package client

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/cosmos/iavl"
	"github.com/linxGnu/grocksdb"
	"github.com/spf13/cobra"

	"github.com/crypto-org-chain/cronos/versiondb/extsort"
	"github.com/crypto-org-chain/cronos/versiondb/tsrocksdb"
)

const (
	SSTFileExtension       = ".sst"
	DefaultSSTFileSize     = 128 * 1024 * 1024
	DefaultSorterChunkSize = 128 * 1024 * 1024
)

func ConvertPlainToSSTTSCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "plain-to-sst-ts plain-input sst-output",
		Short: "Convert plain file format to rocksdb sst file, using user timestamp feature of rocksdb",
		Args:  cobra.MinimumNArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			sstFile := args[0]
			plainFiles := args[1:]

			sstFileSize, err := cmd.Flags().GetUint64(flagSSTFileSize)
			if err != nil {
				return err
			}

			sorterChunkSize, err := cmd.Flags().GetInt64(flagSorterChunkSize)
			if err != nil {
				return err
			}
			store, err := cmd.Flags().GetString(flagStore)
			if err != nil {
				return err
			}

			var prefix []byte
			if len(store) > 0 {
				prefix = []byte(fmt.Sprintf(tsrocksdb.StorePrefixTpl, store))
			}

			sorter := extsort.New(filepath.Dir(sstFile), sorterChunkSize, compareSorterItem)
			defer sorter.Close()
			for _, plainFile := range plainFiles {
				if err := withPlainInput(plainFile, func(reader io.Reader) error {
					offset, err := readPlainFile(reader, func(version int64, changeSet *iavl.ChangeSet) (bool, error) {
						for _, pair := range changeSet.Pairs {
							item := encodeSorterItem(uint64(version), pair)
							if err := sorter.Feed(item); err != nil {
								return false, err
							}
						}
						return true, nil
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
			}

			mergedReader, err := sorter.Finalize()
			if err != nil {
				return err
			}

			sstWriter := newSSTFileWriter()
			defer sstWriter.Destroy()
			sstSeq := 0
			if err := sstWriter.Open(sstFileName(sstFile, sstSeq)); err != nil {
				return err
			}
			sstSeq++
			for {
				item, err := mergedReader.Next()
				if err != nil {
					return err
				}
				if item == nil {
					break
				}
				pair := decodeSorterItem(item)
				key := cloneAppend(prefix, pair.Key)
				sstWriter.PutWithTS(key, item[:8], pair.Value)
				if sstWriter.FileSize() >= sstFileSize {
					if err := sstWriter.Finish(); err != nil {
						return err
					}
					if err := sstWriter.Open(sstFileName(sstFile, sstSeq)); err != nil {
						return err
					}
					sstSeq++
				}
			}
			return sstWriter.Finish()
		},
	}
	cmd.Flags().Uint64(flagSSTFileSize, DefaultSSTFileSize, "the sst file size target")
	cmd.Flags().String(flagStore, "", "store name, the keys are prefixed with \"s/k:{store}/\"")
	cmd.Flags().Int64(flagSorterChunkSize, DefaultSorterChunkSize, "uncompressed chunk size for external sorter, it decides the peak ram usage, on disk it'll be snappy compressed")
	return cmd
}

// sstFileName inserts the seq integer into the base file name
func sstFileName(fileName string, seq int) string {
	stem := fileName[:len(fileName)-len(SSTFileExtension)]
	return stem + fmt.Sprintf("-%d", seq) + SSTFileExtension
}

func newSSTFileWriter() *grocksdb.SSTFileWriter {
	envOpts := grocksdb.NewDefaultEnvOptions()
	return grocksdb.NewSSTFileWriter(envOpts, tsrocksdb.NewVersionDBOpts())
}

// encodeSorterItem encode kv-pair for use in external sorter.
// format: version(8) + key length(2) + key + value
func encodeSorterItem(version uint64, pair iavl.KVPair) []byte {
	item := make([]byte, 10+len(pair.Key)+len(pair.Value))
	binary.LittleEndian.PutUint64(item[:], version)
	binary.LittleEndian.PutUint16(item[8:], uint16(len(pair.Key)))
	copy(item[10:], pair.Key)
	copy(item[10+len(pair.Key):], pair.Value)
	return item

}

// decodeSorterItem decode the kv-pair from external sorter.
func decodeSorterItem(item []byte) iavl.KVPair {
	keyLen := binary.LittleEndian.Uint16(item[8:])
	key := item[10 : 10+keyLen]
	value := item[10+keyLen:]
	return iavl.KVPair{
		Delete: len(value) == 0,
		Key:    key,
		Value:  value,
	}
}

// compareSorterItem compare encoded kv-pairs return if a < b.
func compareSorterItem(a, b []byte) bool {
	// decode key and version
	aKeyLen := binary.LittleEndian.Uint16(a[8:])
	bKeyLen := binary.LittleEndian.Uint16(b[8:])
	ret := bytes.Compare(a[10:10+aKeyLen], b[10:10+bKeyLen])
	if ret != 0 {
		return ret == -1
	}

	aVersion := binary.LittleEndian.Uint64(a)
	bVersion := binary.LittleEndian.Uint64(b)
	// Compare version.
	// For the same user key with different timestamps, larger (newer) timestamp
	// comes first.
	return aVersion >= bVersion
}
