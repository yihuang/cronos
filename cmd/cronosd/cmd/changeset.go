package cmd

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"runtime"

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

	flagStartVersion = "start-version"
	flagEndVersion   = "end-version"
	flagOutput       = "output"
	flagConcurrency  = "concurrency"
)

func ChangeSetGroupCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "changeset",
		Short: "dump changeset from iavl versions or manage changeset files",
	}
	cmd.AddCommand(
		DumpFileChangeSetCmd(),
		DumpSSTChangeSetCmd(),
		IngestSSTCmd(),
		ConvertPlainToSSTCmd(),
		ConvertPlainToSSTTSCmd(),
		PrintPlainFileCmd(),
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
			prefix := []byte(fmt.Sprintf("s/k:%s/", args[0]))
			db = dbm.NewPrefixDB(db, prefix)

			cacheSize := cast.ToInt(ctx.Viper.Get(server.FlagIAVLCacheSize))

			startVersion, err := cmd.Flags().GetInt(flagStartVersion)
			if err != nil {
				return err
			}
			endVersion, err := cmd.Flags().GetInt(flagEndVersion)
			if err != nil {
				return err
			}
			concurrency, err := cmd.Flags().GetInt(flagConcurrency)
			if err != nil {
				return err
			}

			output, err := cmd.Flags().GetString(flagOutput)
			if err != nil {
				return err
			}

			var tmpDir string
			var writer io.Writer
			if output == "-" {
				writer = os.Stdout

				tmpDir = "/tmp"
			} else {
				fp, err := os.OpenFile(output, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
				if err != nil {
					return err
				}
				defer fp.Close()
				writer = fp

				tmpDir = filepath.Dir(output)
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
				endVersion = int(latestVersion) + 1
			}

			works := splitWorkLoad(concurrency, Range{Start: startVersion, End: endVersion})

			chs := make([]chan *os.File, len(works))
			for i := 0; i < len(works); i++ {
				chs[i] = make(chan *os.File, 1)
				go func(i int) {
					defer close(chs[i])
					work := works[i]

					tree := iavl.NewImmutableTree(db, cacheSize, true)
					tmpFile, err := dumpRangeBlocksWorker(tmpDir, tree, int64(work.Start), int64(work.End))
					if err != nil {
						fmt.Fprintf(os.Stderr, "worker failed: start: %d, end: %d, err: %e", work.Start, work.End, err)
						return
					}
					// seek to begining, prepare to read
					if _, err := tmpFile.Seek(0, 0); err != nil {
						fmt.Fprintf(os.Stderr, "seek failed: %e", err)
						os.Remove(tmpFile.Name())
						return
					}
					chs[i] <- tmpFile
				}(i)
			}

			for i, ch := range chs {
				tmpFile, ok := <-ch
				if !ok {
					return fmt.Errorf("worker failed: %d", i)
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
		},
	}
	cmd.Flags().Int(flagStartVersion, 1, "The start version")
	cmd.Flags().Int(flagEndVersion, 0, "The end version, exclusive")
	cmd.Flags().String(flagOutput, "-", "Output file, default to stdout")
	cmd.Flags().Int(flagConcurrency, runtime.NumCPU(), "Number concurrent goroutines to parallelize the work")
	cmd.Flags().Int(server.FlagIAVLCacheSize, 781250, "size of the iavl tree cache")
	return cmd
}

func DumpSSTChangeSetCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "dump-sst [store]",
		Short: "Extract changeset from iavl versions and save to rocksdb sst file",
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
			prefix := []byte(fmt.Sprintf("s/k:%s/", args[0]))
			db = dbm.NewPrefixDB(db, prefix)

			cacheSize := cast.ToInt(ctx.Viper.Get(server.FlagIAVLCacheSize))

			startVersion, err := cmd.Flags().GetInt(flagStartVersion)
			if err != nil {
				return err
			}
			endVersion, err := cmd.Flags().GetInt(flagEndVersion)
			if err != nil {
				return err
			}

			output, err := cmd.Flags().GetString(flagOutput)
			if err != nil {
				return err
			}
			if len(output) == 0 {
				return errors.New("output file is not specified")
			}

			w := newSSTFileWriter(false)
			defer w.Destroy()

			if err := w.Open(output); err != nil {
				return err
			}

			tree := iavl.NewImmutableTree(db, cacheSize, true)
			if err := tree.TraverseStateChanges(int64(startVersion), int64(endVersion), func(version int64, changeSet *iavl.ChangeSet) error {
				return writeChangeSetToSST(w, version, changeSet)
			}); err != nil {
				return err
			}

			return w.Finish()
		},
	}
	cmd.Flags().Int(flagStartVersion, 1, "The start version")
	cmd.Flags().Int(flagEndVersion, 0, "The end version, exclusive")
	cmd.Flags().String(flagOutput, "", "Output file, default to stdout")
	return cmd
}

func IngestSSTCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "ingest-sst db-path file1.sst file2.sst ...",
		Short: "Ingest sst files into database",
		Args:  cobra.MinimumNArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			dbPath := args[0]
			opts := grocksdb.NewDefaultOptions()
			opts.SetCreateIfMissing(true)
			db, err := grocksdb.OpenDb(opts, dbPath)
			if err != nil {
				return err
			}
			ingestOpts := grocksdb.NewDefaultIngestExternalFileOptions()
			return db.IngestExternalFile(args[1:], ingestOpts)
		},
	}
	return cmd
}

func ConvertPlainToSSTCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "plain-to-sst plain-input sst-output",
		Short: "Convert plain file format to rocksdb sst file",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			plainFile := args[0]
			sstFile := args[1]

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
			}

			w := newSSTFileWriter(false)
			defer w.Destroy()

			if err := w.Open(sstFile); err != nil {
				return err
			}

			offset, err := readPlainFile(reader, func(version int64, changeSet *iavl.ChangeSet) error {
				return writeChangeSetToSST(w, version, changeSet)
			})
			if err == io.ErrUnexpectedEOF {
				// incomplete end of file, we'll output a warning and process the completed versions.
				fmt.Fprintf(os.Stderr, "file incomplete, the completed versions are processed, the last completed file offset: %d\n", offset)
			} else if err != nil {
				return err
			}

			return w.Finish()
		},
	}
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
			}

			w := newSSTFileWriter(true)
			defer w.Destroy()

			sorted := btree.NewBTreeGOptions(btreeItemLess, btree.Options{
				NoLocks: true,
			})

			offset, err := readPlainFile(reader, func(version int64, changeSet *iavl.ChangeSet) error {
				var ts [TimestampSize]byte
				binary.BigEndian.PutUint64(ts[:], uint64(version))
				for _, pair := range changeSet.Pairs {
					sorted.Set(btreeItem{
						ts:    ts,
						key:   pair.Key,
						value: pair.Value,
					})
				}
				return nil
			})

			if err == io.ErrUnexpectedEOF {
				// incomplete end of file, we'll output a warning and process the completed versions.
				fmt.Fprintf(os.Stderr, "file incomplete, the completed versions are processed, the last completed file offset: %d\n", offset)
			} else if err != nil {
				return err
			}

			fmt.Fprintln(os.Stderr, "start writing sst file", sstFile)
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
			var (
				err    error
				reader io.Reader
			)
			if args[0] == "-" {
				reader = os.Stdin
			} else {
				reader, err = os.Open(args[0])
				if err != nil {
					return err
				}
			}
			marshaler := jsonpb.Marshaler{}
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
			})
			if err == io.ErrUnexpectedEOF {
				// incomplete end of file, we'll output a warning and process the completed versions.
				fmt.Fprintf(os.Stderr, "file incomplete, the completed versions are processed, the last completed file offset: %d\n", offset)
			} else if err != nil {
				return err
			}
			return nil
		},
	}
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
	return tree.TraverseStateChanges(int64(startVersion), int64(endVersion), func(version int64, changeSet *iavl.ChangeSet) error {
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

func readPlainFile(input io.Reader, fn func(version int64, changeSet *iavl.ChangeSet) error) (int, error) {
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
			buf := make([]byte, size)
			if _, err = io.ReadFull(input, buf[:]); err != nil {
				break
			}

			if err = proto.Unmarshal(buf[:], &changeSet); err != nil {
				return lastValidOffset, err
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

func writeChangeSetToSST(w *grocksdb.SSTFileWriter, version int64, changeSet *iavl.ChangeSet) error {
	versionBuf := make([]byte, 8)
	binary.BigEndian.PutUint64(versionBuf[:], uint64(version))
	for _, pair := range changeSet.Pairs {
		key := make([]byte, 8+len(pair.Key))
		copy(key[:8], versionBuf)
		copy(key[8:], pair.Key)
		// deletion will have empty values
		w.Add(key, pair.Value)
	}

	return nil
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

func newSSTFileWriter(enableUserTS bool) *grocksdb.SSTFileWriter {
	envOpts := grocksdb.NewDefaultEnvOptions()
	opts := grocksdb.NewDefaultOptions()
	opts.SetCompression(grocksdb.ZSTDCompression)
	if enableUserTS {
		opts.SetComparator(grocksdb.NewComparatorWithTimestamp(
			"default-ts", TimestampSize, bytes.Compare, bytes.Compare, func(a []byte, aHasTs bool, b []byte, bHasTs bool) int {
				if aHasTs {
					a = a[:len(a)-TimestampSize]
				}
				if bHasTs {
					b = b[:len(b)-TimestampSize]
				}
				return bytes.Compare(a, b)
			},
		))
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
	return grocksdb.NewSSTFileWriter(envOpts, opts)
}

type Range struct {
	Start, End int
}

func splitWorkLoad(workers int, full Range) []Range {
	var chunks []Range
	chunkSize := (full.End - full.Start + workers - 1) / workers
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
		return binary.BigEndian.Uint64(i.ts[:]) < binary.BigEndian.Uint64(j.ts[:])
	}
}
