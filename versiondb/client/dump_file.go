package client

import (
	"bufio"
	"compress/zlib"
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"runtime"

	"github.com/cosmos/iavl"
	"github.com/spf13/cast"
	"github.com/spf13/cobra"
	dbm "github.com/tendermint/tm-db"
	"google.golang.org/protobuf/proto"

	"github.com/cosmos/cosmos-sdk/client/flags"
	"github.com/cosmos/cosmos-sdk/server"

	"github.com/crypto-org-chain/cronos/versiondb/tsrocksdb"
)

const DefaultChunkSize = 1000000

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

			db, err := openDB(ctx.Viper.GetString(flags.FlagHome), server.GetAppDBBackend(ctx.Viper))
			if err != nil {
				return err
			}
			store := args[0]
			prefix := []byte(fmt.Sprintf(tsrocksdb.StorePrefixTmp, store))
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

			tasksChan := make(chan *dumpTask, 2048)
			for i := 0; i < concurrency; i++ {
				go func() {
					// use separate iavl node cache for each task to improve parallel performance.
					tree := iavl.NewImmutableTree(db, cacheSize, true)
					for task := range tasksChan {
						task.run(tree, outDir)
					}
				}()
			}

			// split block chunks
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

func openDB(home string, backendType dbm.BackendType) (dbm.DB, error) {
	dataDir := filepath.Join(home, "data")
	return dbm.NewDB("application", backendType, dataDir)
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
