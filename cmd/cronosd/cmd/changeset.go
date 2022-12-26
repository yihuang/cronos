package cmd

import (
	"bufio"
	"bytes"
	"compress/zlib"
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/cosmos/cosmos-sdk/client/flags"
	"github.com/cosmos/cosmos-sdk/server"
	"github.com/cosmos/iavl"
	"github.com/golang/protobuf/jsonpb"
	"github.com/linxGnu/grocksdb"
	"github.com/spf13/cast"
	"github.com/spf13/cobra"
	dbm "github.com/tendermint/tm-db"
	"github.com/tidwall/btree"
	"google.golang.org/protobuf/proto"
)

const (
	TimestampSize = 8

	flagStartVersion     = "start-version"
	flagEndVersion       = "end-version"
	flagOutput           = "output"
	flagConcurrency      = "concurrency"
	flagNoParseChangeset = "no-parse-changeset"
	flagChunkSize        = "chunk-size"
	flagZlibLevel        = "zlib-level"
	flagMoveFiles        = "move-files"

	DefaultChunkSize = 1000000

	CompressedFileSuffix = ".zz"
)

func ChangeSetGroupCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "changeset",
		Short: "dump changeset from iavl versions or manage changeset files",
	}
	cmd.AddCommand(
		DumpFileChangeSetCmd(),
		PrintPlainFileCmd(),
		ConvertPlainToSSTTSCmd(),
		IngestSSTCmd(),
	)
	return cmd
}

func DumpFileChangeSetCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "dump-file [store]",
		Short: "Extract changeset from iavl versions, and save to plain file format",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := server.GetServerContextFromCmd(cmd)
			if err := ctx.Viper.BindPFlags(cmd.Flags()); err != nil {
				return err
			}

			db, err := openDBReadOnly(ctx.Viper.GetString(flags.FlagHome), server.GetAppDBBackend(ctx.Viper))
			if err != nil {
				return err
			}
			store := args[0]
			prefix := []byte(fmt.Sprintf("s/k:%s/", store))
			db = dbm.NewPrefixDB(db, prefix)

			cacheSize := cast.ToInt(ctx.Viper.Get(server.FlagIAVLCacheSize))

			startVersion, err := cmd.Flags().GetInt64(flagStartVersion)
			if err != nil {
				return err
			}
			endVersion, err := cmd.Flags().GetInt64(flagEndVersion)
			if err != nil {
				return err
			}
			concurrency, err := cmd.Flags().GetInt(flagConcurrency)
			if err != nil {
				return err
			}
			chunkSize, err := cmd.Flags().GetInt(flagChunkSize)
			if err != nil {
				return err
			}
			zlibLevel, err := cmd.Flags().GetInt(flagZlibLevel)
			if err != nil {
				return err
			}

			outDir, err := cmd.Flags().GetString(flagOutput)
			if err != nil {
				return err
			}
			outDir = filepath.Join(outDir, store)
			if err := os.MkdirAll(outDir, os.ModePerm); err != nil {
				return err
			}

			if endVersion == 0 {
				tree, err := iavl.NewMutableTree(db, 0, true)
				if err != nil {
					return err
				}
				latestVersion, err := tree.LazyLoadVersion(0)
				if err != nil {
					return err
				}
				endVersion = latestVersion + 1
			}

			tasksChan := make(chan *dumpTask, 2^32)
			for i := 0; i < concurrency; i++ {
				go func() {
					// use separate iavl node cache for each task to improve parallel performance.
					tree := iavl.NewImmutableTree(db, cacheSize, true)
					for task := range tasksChan {
						task.run(tree, outDir)
					}
				}()
			}

			// split chunks
			var chunks []chunk
			for i := startVersion; i < endVersion; i += int64(chunkSize) {
				end := i + int64(chunkSize)
				if end > endVersion {
					end = endVersion
				}
				var tasks []*dumpTask
				for _, work := range splitWorkLoad(concurrency, Range{Start: i, End: end}) {
					task := newDumpTask(work)
					tasksChan <- task
					tasks = append(tasks, task)
				}
				chunks = append(chunks, chunk{beginVersion: i, tasks: tasks})
			}

			for _, chunk := range chunks {
				if err := chunk.collect(outDir, zlibLevel); err != nil {
					return err
				}
			}
			return nil
		},
	}
	cmd.Flags().Int64(flagStartVersion, 0, "The start version")
	cmd.Flags().Int64(flagEndVersion, 0, "The end version, exclusive, default to latestVersion+1")
	cmd.Flags().String(flagOutput, "-", "Output file, default to stdout")
	cmd.Flags().Int(flagConcurrency, runtime.NumCPU(), "Number concurrent goroutines to parallelize the work")
	cmd.Flags().Int(server.FlagIAVLCacheSize, 781250, "size of the iavl tree cache")
	cmd.Flags().Int(flagChunkSize, DefaultChunkSize, "size of the block chunk")
	cmd.Flags().Int(flagZlibLevel, 6, "level of zlib compression, 0: plain data, 1: fast, 9: best, default: 6, if not 0 the output file name will have .zz extension")
	return cmd
}

func IngestSSTCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "ingest-sst db-path file1.sst file2.sst ...",
		Short: "Ingest sst files into database",
		Args:  cobra.MinimumNArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			dbPath := args[0]
			moveFiles, err := cmd.Flags().GetBool(flagMoveFiles)
			if err != nil {
				return err
			}
			opts := newRocksDBOptions(true)
			opts.SetCreateIfMissing(true)
			db, err := grocksdb.OpenDb(opts, dbPath)
			if err != nil {
				return err
			}
			ingestOpts := grocksdb.NewDefaultIngestExternalFileOptions()
			ingestOpts.SetMoveFiles(moveFiles)
			return db.IngestExternalFile(args[1:], ingestOpts)
		},
	}
	cmd.Flags().Bool(flagMoveFiles, false, "move sst files instead of copy them")
	return cmd
}

func ConvertPlainToSSTTSCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "plain-to-sst-ts plain-input sst-output",
		Short: "Convert plain file format to rocksdb sst file, using user timestamp feature of rocksdb",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			plainFile := args[0]
			sstFile := args[1]

			sorted := btree.NewBTreeGOptions(btreeItemLess, btree.Options{
				NoLocks: true,
			})

			if err := withPlainInput(plainFile, func(reader io.Reader) error {
				offset, err := readPlainFile(reader, func(version int64, changeSet *iavl.ChangeSet) error {
					var ts [TimestampSize]byte
					binary.LittleEndian.PutUint64(ts[:], uint64(version))
					for _, pair := range changeSet.Pairs {
						sorted.Set(btreeItem{
							ts:    ts,
							key:   pair.Key,
							value: pair.Value,
						})
					}
					return nil
				}, true)

				if err == io.ErrUnexpectedEOF {
					// incomplete end of file, we'll output a warning and process the completed versions.
					fmt.Fprintf(os.Stderr, "file incomplete, the completed versions are processed, the last completed file offset: %d\n", offset)
				} else if err != nil {
					return err
				}
				return nil
			}); err != nil {
				return err
			}

			if sorted.Len() > 0 {
				w := newSSTFileWriter(true)
				defer w.Destroy()

				if err := w.Open(sstFile); err != nil {
					return err
				}

				sorted.Scan(func(item btreeItem) bool {
					if err := w.PutWithTS(item.key, item.ts[:], item.value); err != nil {
						fmt.Fprintf(os.Stderr, "sst writer fail: %w", err)
						return false
					}
					return true
				})

				return w.Finish()
			}
			return nil
		},
	}
	return cmd
}

func PrintPlainFileCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "print-plain [plain-file]",
		Short: "Pretty-print content of plain changeset file",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			noParseChangeset, err := cmd.Flags().GetBool(flagNoParseChangeset)
			if err != nil {
				return err
			}

			marshaler := jsonpb.Marshaler{}
			return withPlainInput(args[0], func(reader io.Reader) error {
				offset, err := readPlainFile(reader, func(version int64, changeSet *iavl.ChangeSet) error {
					fmt.Printf("version: %d\n", version)
					for _, pair := range changeSet.Pairs {
						js, err := marshaler.MarshalToString(pair)
						if err != nil {
							return err
						}
						fmt.Println(js)
					}
					return nil
				}, !noParseChangeset)
				if err == io.ErrUnexpectedEOF {
					// incomplete end of file, we'll output a warning and process the completed versions.
					fmt.Fprintf(os.Stderr, "file incomplete, the completed versions are processed, the last completed file offset: %d\n", offset)
				} else if err != nil {
					return err
				}
				return nil
			})
		},
	}
	cmd.Flags().Bool(flagNoParseChangeset, false, "if parse and output the change set content, otherwise only version numbers are outputted")
	return cmd
}

func dumpRangeBlocksWorker(dir string, tree *iavl.ImmutableTree, startVersion, endVersion int64) (*os.File, error) {
	fp, err := ioutil.TempFile(dir, "tmp-*")
	if err != nil {
		return nil, err
	}
	writer := bufio.NewWriter(fp)
	defer writer.Flush()

	if err := dumpRangeBlocks(writer, tree, startVersion, endVersion); err != nil {
		os.Remove(fp.Name())
		return nil, err
	}
	return fp, nil
}

func dumpRangeBlocks(writer io.Writer, tree *iavl.ImmutableTree, startVersion, endVersion int64) error {
	var versionHeader [16]byte
	return tree.TraverseStateChanges(startVersion, endVersion, func(version int64, changeSet *iavl.ChangeSet) error {
		bz, err := proto.Marshal(changeSet)
		if err != nil {
			return err
		}

		binary.LittleEndian.PutUint64(versionHeader[:8], uint64(version))
		binary.LittleEndian.PutUint64(versionHeader[8:16], uint64(len(bz)))

		writer.Write(versionHeader[:])
		writer.Write(bz)
		return nil
	})
}

func withPlainInput(plainFile string, fn func(io.Reader) error) error {
	var reader io.Reader
	if plainFile == "-" {
		reader = os.Stdin
	} else {
		fp, err := os.Open(plainFile)
		if err != nil {
			return err
		}
		defer fp.Close()
		reader = fp
		if strings.HasSuffix(plainFile, CompressedFileSuffix) {
			zreader, err := zlib.NewReader(fp)
			if err != nil {
				return err
			}
			reader = zreader
		}
	}
	return fn(reader)
}

func readPlainFile(input io.Reader, fn func(version int64, changeSet *iavl.ChangeSet) error, parseChangeset bool) (int, error) {
	var (
		err             error
		versionHeader   [16]byte
		lastValidOffset int
	)

	lastValidOffset = 0
	for {
		if _, err = io.ReadFull(input, versionHeader[:]); err != nil {
			break
		}
		version := binary.LittleEndian.Uint64(versionHeader[:8])
		size := int(binary.LittleEndian.Uint64(versionHeader[8:16]))
		var changeSet iavl.ChangeSet
		if size > 0 {
			if parseChangeset {
				buf := make([]byte, size)
				if _, err = io.ReadFull(input, buf[:]); err != nil {
					break
				}

				if err = proto.Unmarshal(buf[:], &changeSet); err != nil {
					return lastValidOffset, err
				}
			} else {
				if written, err := io.CopyN(ioutil.Discard, input, int64(size)); err != nil {
					if err == io.EOF && written < int64(size) {
						err = io.ErrUnexpectedEOF
					}
					break
				}
			}
		}

		if err = fn(int64(version), &changeSet); err != nil {
			return lastValidOffset, err
		}

		lastValidOffset += size + 16
	}

	if err != io.EOF {
		return lastValidOffset, err
	}

	return lastValidOffset, nil
}

func openDBReadOnly(home string, backendType dbm.BackendType) (dbm.DB, error) {
	dataDir := filepath.Join(home, "data")
	if backendType == dbm.RocksDBBackend {
		dbDir := filepath.Join(dataDir, "application.db")
		opts := grocksdb.NewDefaultOptions()
		raw, err := grocksdb.OpenDbForReadOnly(opts, dbDir, false)
		if err != nil {
			return nil, err
		}
		return dbm.NewRocksDBWithRawDB(raw), nil
	} else {
		return dbm.NewDB("application", backendType, dataDir)
	}
}

func newRocksDBOptions(enableUserTS bool) *grocksdb.Options {
	opts := grocksdb.NewDefaultOptions()
	opts.SetCompression(grocksdb.ZSTDCompression)
	if enableUserTS {
		opts.SetComparator(createTSComparator())
	}

	blkOpts := grocksdb.NewDefaultBlockBasedTableOptions()
	blkOpts.SetBlockSize(32 * 1024)
	blkOpts.SetFilterPolicy(grocksdb.NewRibbonFilterPolicy(9.9))
	blkOpts.SetIndexType(grocksdb.KTwoLevelIndexSearchIndexType)
	blkOpts.SetPartitionFilters(true)
	blkOpts.SetDataBlockIndexType(grocksdb.KDataBlockIndexTypeBinarySearchAndHash)
	opts.SetBlockBasedTableFactory(blkOpts)
	opts.SetOptimizeFiltersForHits(true)

	compressOpts := grocksdb.NewDefaultCompressionOptions()
	compressOpts.MaxDictBytes = 112640 // 110k
	compressOpts.Level = 12
	opts.SetCompressionOptions(compressOpts)
	opts.SetCompressionOptionsZstdMaxTrainBytes(compressOpts.MaxDictBytes * 100)
	opts.SetCompressionOptionsZstdDictTrainer(true)
	return opts
}

func newSSTFileWriter(enableUserTS bool) *grocksdb.SSTFileWriter {
	envOpts := grocksdb.NewDefaultEnvOptions()
	return grocksdb.NewSSTFileWriter(envOpts, newRocksDBOptions(enableUserTS))
}

func compareTS(bz1 []byte, bz2 []byte) int {
	ts1 := binary.LittleEndian.Uint64(bz1)
	ts2 := binary.LittleEndian.Uint64(bz2)
	switch {
	case ts1 < ts2:
		return -1
	case ts1 > ts2:
		return 1
	default:
		return 0
	}
}

func compare(a []byte, b []byte) int {
	ret := compareWithoutTS(a, true, b, true)
	if ret != 0 {
		return ret
	}
	// Compare timestamp.
	// For the same user key with different timestamps, larger (newer) timestamp
	// comes first.
	return -compareTS(a[len(a)-TimestampSize:], b[len(b)-TimestampSize:])
}

func compareWithoutTS(a []byte, aHasTs bool, b []byte, bHasTs bool) int {
	if aHasTs {
		a = a[:len(a)-TimestampSize]
	}
	if bHasTs {
		b = b[:len(b)-TimestampSize]
	}
	return bytes.Compare(a, b)
}

// createTSComparator should be compatible with builtin timestamp comparator.
func createTSComparator() *grocksdb.Comparator {
	return grocksdb.NewComparatorWithTimestamp(
		"leveldb.BytewiseComparator.u64ts", TimestampSize, compare, compareTS, compareWithoutTS,
	)
}

type Range struct {
	Start, End int64
}

func splitWorkLoad(workers int, full Range) []Range {
	var chunks []Range
	chunkSize := (full.End - full.Start + int64(workers) - 1) / int64(workers)
	for i := full.Start; i < full.End; i += chunkSize {
		end := i + chunkSize
		if end > full.End {
			end = full.End
		}
		chunks = append(chunks, Range{Start: i, End: end})
	}
	return chunks
}

type btreeItem struct {
	ts    [8]byte
	key   []byte
	value []byte
}

func btreeItemLess(i, j btreeItem) bool {
	switch bytes.Compare(i.key, j.key) {
	case -1:
		return true
	case 1:
		return false
	default:
		// Compare timestamp.
		// For the same user key with different timestamps, larger (newer) timestamp
		// comes first.
		return binary.LittleEndian.Uint64(j.ts[:]) < binary.LittleEndian.Uint64(i.ts[:])
	}
}

type dumpTask struct {
	work       Range
	resultChan chan *os.File
}

func newDumpTask(work Range) *dumpTask {
	return &dumpTask{
		work:       work,
		resultChan: make(chan *os.File, 1),
	}
}

// run the task, write a result to the result channel if successful.
func (dt *dumpTask) run(tree *iavl.ImmutableTree, tmpDir string) {
	defer close(dt.resultChan)
	tmpFile, err := dumpRangeBlocksWorker(tmpDir, tree, dt.work.Start, dt.work.End)
	if err != nil {
		fmt.Fprintf(os.Stderr, "worker failed: start: %d, end: %d, err: %e", dt.work.Start, dt.work.End, err)
		return
	}
	// seek to begining, prepare to read
	if _, err := tmpFile.Seek(0, 0); err != nil {
		fmt.Fprintf(os.Stderr, "seek failed: %e", err)
		os.Remove(tmpFile.Name())
		return
	}
	dt.resultChan <- tmpFile
}

type chunk struct {
	beginVersion int64
	tasks        []*dumpTask
}

func (c *chunk) collect(outDir string, zlibLevel int) error {
	output := filepath.Join(outDir, fmt.Sprintf("block-%d", c.beginVersion))
	if zlibLevel > 0 {
		output += CompressedFileSuffix
	}

	fp, err := os.OpenFile(output, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	defer fp.Close()
	writer := io.Writer(fp)

	if zlibLevel > 0 {
		zwriter, err := zlib.NewWriterLevel(writer, zlibLevel)
		if err != nil {
			return err
		}
		defer zwriter.Close()

		writer = zwriter
	}

	for i, task := range c.tasks {
		tmpFile, ok := <-task.resultChan
		if !ok {
			return fmt.Errorf("worker failed: chunk: %d, worker: %d", c.beginVersion, i)
		}
		defer func() {
			tmpFile.Close()
			os.Remove(tmpFile.Name())
		}()

		if _, err := io.Copy(writer, tmpFile); err != nil {
			return err
		}
	}

	return nil
}
