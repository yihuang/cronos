package cmd

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/edsrzf/mmap-go"

	"github.com/cosmos/cosmos-sdk/client/flags"
	"github.com/cosmos/cosmos-sdk/server"
	"github.com/cosmos/gorocksdb"
	"github.com/cosmos/iavl"
	"github.com/spf13/cast"
	"github.com/spf13/cobra"
	dbm "github.com/tendermint/tm-db"
	"google.golang.org/protobuf/proto"
)

const (
	flagStartVersion = "start-version"
	flagEndVersion   = "end-version"
	flagOutput       = "output"
)

var VersionDBMagic = []byte("VERDB000")

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

			cacheSize := cast.ToInt(ctx.Viper.Get("iavl-cache-size"))

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

			var writer io.Writer
			if output == "-" {
				writer = os.Stdout
			} else {
				fp, err := os.OpenFile(output, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
				if err != nil {
					return err
				}
				defer fp.Close()
				bufWriter := bufio.NewWriter(fp)
				defer bufWriter.Flush()

				writer = bufWriter
				if _, err = writer.Write(VersionDBMagic); err != nil {
					return err
				}
			}

			var sizeBuf [binary.MaxVarintLen64]byte
			tree := iavl.NewImmutableTree(db, cacheSize, true)
			return tree.TraverseStateChanges(int64(startVersion), int64(endVersion), func(version int64, changeSet *iavl.ChangeSet) error {
				bz, err := proto.Marshal(changeSet)
				if err != nil {
					return err
				}

				n := binary.PutUvarint(sizeBuf[:], uint64(version))
				writer.Write(sizeBuf[:n])

				n = binary.PutUvarint(sizeBuf[:], uint64(len(bz)))
				writer.Write(sizeBuf[:n])

				writer.Write(bz)
				return nil
			})
		},
	}
	cmd.Flags().Int(flagStartVersion, 1, "The start version")
	cmd.Flags().Int(flagEndVersion, 0, "The end version, exclusive")
	cmd.Flags().String(flagOutput, "-", "Output file, default to stdout")
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

			cacheSize := cast.ToInt(ctx.Viper.Get("iavl-cache-size"))

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

			w := newSSTFileWriter()
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
			opts := gorocksdb.NewDefaultOptions()
			opts.SetCreateIfMissing(true)
			db, err := gorocksdb.OpenDb(opts, dbPath)
			if err != nil {
				return err
			}
			ingestOpts := gorocksdb.NewDefaultIngestExternalFileOptions()
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

			fp, err := os.Open(plainFile)
			if err != nil {
				return err
			}
			defer fp.Close()

			data, err := mmap.Map(fp, mmap.RDONLY, 0)
			if err != nil {
				return err
			}
			defer data.Unmap()

			w := newSSTFileWriter()
			defer w.Destroy()

			if err := w.Open(sstFile); err != nil {
				return err
			}

			offset, err := readPlainFile(data, func(version int64, changeSet *iavl.ChangeSet) error {
				return writeChangeSetToSST(w, version, changeSet)
			})
			if err != nil {
				return err
			}

			if offset != len(data) {
				fmt.Println("file not complete, last completed offset:", offset)
			}

			return w.Finish()
		},
	}
	return cmd
}

func readPlainFile(data []byte, fn func(version int64, changeSet *iavl.ChangeSet) error) (int, error) {
	if bytes.Compare(data[:8], VersionDBMagic) != 0 {
		return 0, errors.New("invalid file magic header")
	}

	offset := 8
	tmp := offset

	for offset < len(data) {
		version, n := binary.Uvarint(data[tmp : tmp+binary.MaxVarintLen64])
		if n <= 0 {
			return offset, fmt.Errorf("read version fail: %d", n)
		}
		tmp += n

		usize, n := binary.Uvarint(data[tmp : tmp+binary.MaxVarintLen64])
		if n <= 0 {
			return offset, fmt.Errorf("read size fail: %d", n)
		}
		tmp += n

		size := int(usize)
		if tmp+size > len(data) {
			break
		}
		var changeSet iavl.ChangeSet
		if err := proto.Unmarshal(data[tmp:tmp+size], &changeSet); err != nil {
			return offset, err
		}

		if err := fn(int64(version), &changeSet); err != nil {
			return offset, err
		}

		tmp += size
		offset = tmp
	}
	return offset, nil
}

func writeChangeSetToSST(w *gorocksdb.SSTFileWriter, version int64, changeSet *iavl.ChangeSet) error {
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
	if backendType != dbm.RocksDBBackend {
		return nil, errors.New("only support rocksdb backend")
	}
	dataDir := filepath.Join(home, "data", "application.db")
	opts := gorocksdb.NewDefaultOptions()
	raw, err := gorocksdb.OpenDbForReadOnly(opts, dataDir, false)
	if err != nil {
		return nil, err
	}
	return dbm.NewRocksDBWithRawDB(raw), nil
}

func newSSTFileWriter() *gorocksdb.SSTFileWriter {
	envOpts := gorocksdb.NewDefaultEnvOptions()
	opts := gorocksdb.NewDefaultOptions()
	opts.SetCompression(gorocksdb.LZ4HCCompression)
	compressOpts := gorocksdb.NewDefaultCompressionOptions()
	compressOpts.MaxDictBytes = 112640 // 110k
	opts.SetCompressionOptions(compressOpts)
	zstdOpts := gorocksdb.NewZSTDCompressionOptions(1, compressOpts.MaxDictBytes*100)
	opts.SetZSTDCompressionOptions(zstdOpts)
	return gorocksdb.NewSSTFileWriter(envOpts, opts)
}
