package client

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/cosmos/iavl"
	"github.com/crypto-org-chain/cronos/versiondb/tsrocksdb"
	"github.com/linxGnu/grocksdb"
	"github.com/spf13/cobra"
	"github.com/tidwall/btree"
)

const (
	SSTFileExtension = ".sst"
)

func ConvertPlainToSSTTSCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "plain-to-sst-ts plain-input sst-output",
		Short: "Convert plain file format to rocksdb sst file, using user timestamp feature of rocksdb",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			plainFile := args[0]
			sstFile := args[1]

			batchSize, err := cmd.Flags().GetInt(flagBatchSize)
			if err != nil {
				return err
			}

			store, err := cmd.Flags().GetString(flagStore)
			if err != nil {
				return err
			}

			var prefix []byte
			if len(store) > 0 {
				prefix = []byte(fmt.Sprintf(tsrocksdb.StorePrefixTmp, store))
			}

			sstBatchWriter, err := newTSSSTWriter(batchSize, sstFile, prefix)
			if err != nil {
				return err
			}

			return withPlainInput(plainFile, func(reader io.Reader) error {
				offset, err := readPlainFile(reader, sstBatchWriter.AddChangeSet, true)
				if err := sstBatchWriter.Finalize(); err != nil {
					return err
				}

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
	cmd.Flags().Int(flagBatchSize, 0, "split the input into batches, output separate sst files for each batch, default: 0 (disable batching).")
	cmd.Flags().String(flagStore, "", "store name, the keys are prefixed with \"s/k:{store}/\"")
	return cmd
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

type tsSSTWriter struct {
	batchSize int
	fileName  string
	batch     *btree.BTreeG[btreeItem]
	prefix    []byte

	batchSeq int
}

func newBTree() *btree.BTreeG[btreeItem] {
	return btree.NewBTreeGOptions(
		btreeItemLess, btree.Options{
			NoLocks: true,
		},
	)
}

func newTSSSTWriter(batchSize int, fileName string, prefix []byte) (tsSSTWriter, error) {
	if !strings.HasSuffix(fileName, SSTFileExtension) {
		return tsSSTWriter{}, errors.New("invalid sst filename")
	}
	return tsSSTWriter{
		batchSize: batchSize,
		fileName:  fileName,
		batch:     newBTree(),
	}, nil
}

func (w tsSSTWriter) batchFileName() string {
	stem := w.fileName[:len(w.fileName)-len(SSTFileExtension)]
	return stem + fmt.Sprintf("-%d", w.batchSeq) + SSTFileExtension
}

func (w tsSSTWriter) AddChangeSet(version int64, changeSet *iavl.ChangeSet) error {
	if len(changeSet.Pairs) == 0 {
		return nil
	}
	var ts [tsrocksdb.TimestampSize]byte
	binary.LittleEndian.PutUint64(ts[:], uint64(version))
	for _, pair := range changeSet.Pairs {
		w.batch.Set(btreeItem{
			ts:    ts,
			key:   pair.Key,
			value: pair.Value,
		})
	}
	if w.batchSize > 0 && w.batch.Len() > w.batchSize {
		if err := w.writeBatch(w.batchFileName()); err != nil {
			return err
		}
		w.batch = newBTree()
		w.batchSeq++
	}
	return nil
}

func newSSTFileWriter() *grocksdb.SSTFileWriter {
	envOpts := grocksdb.NewDefaultEnvOptions()
	return grocksdb.NewSSTFileWriter(envOpts, tsrocksdb.NewVersionDBOpts())
}
func (w tsSSTWriter) writeBatch(sstFile string) error {
	// write out a batch
	sstWriter := newSSTFileWriter()
	defer sstWriter.Destroy()

	if err := sstWriter.Open(sstFile); err != nil {
		return err
	}

	w.batch.Scan(func(item btreeItem) bool {
		key := item.key
		if len(w.prefix) > 0 {
			key = cloneAppend(w.prefix, item.key)
		}
		if err := sstWriter.PutWithTS(key, item.ts[:], item.value); err != nil {
			fmt.Fprintf(os.Stderr, "sst writer fail: %w", err)
			return false
		}
		return true
	})

	return sstWriter.Finish()
}

func (w tsSSTWriter) Finalize() error {
	if w.batch.Len() == 0 {
		return nil
	}

	if w.batchSize == 0 {
		// write to normal filename
		return w.writeBatch(w.fileName)
	}

	// write the final batch
	return w.writeBatch(w.batchFileName())
}

func cloneAppend(bz []byte, tail []byte) (res []byte) {
	res = make([]byte, len(bz)+len(tail))
	copy(res, bz)
	copy(res[len(bz):], tail)
	return
}
