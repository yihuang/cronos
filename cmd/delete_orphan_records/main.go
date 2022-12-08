package main

import (
	"bytes"
	"fmt"
	"os"

	"github.com/cosmos/gorocksdb"
)

var _ gorocksdb.CompactionFilter = RangeCompactionFilter{}

type RangeCompactionFilter struct {
	startKey []byte
	endKey   []byte
}

func (rcf RangeCompactionFilter) Filter(level int, key, val []byte) (remove bool, newVal []byte) {
	if bytes.Compare(key, rcf.startKey) >= 0 && bytes.Compare(key, rcf.endKey) <= 0 {
		// Delete the key.
		return true, nil
	}
	// Keep the key.
	return false, nil
}

func (rcf RangeCompactionFilter) Name() string {
	return "range"
}

// PrefixEndBytes returns the []byte that would end a
// range query for all []byte with a certain prefix
// Deals with last byte of prefix being FF without overflowing
func PrefixEndBytes(prefix []byte) []byte {
	if len(prefix) == 0 {
		return nil
	}

	end := make([]byte, len(prefix))
	copy(end, prefix)

	for {
		if end[len(end)-1] != byte(255) {
			end[len(end)-1]++
			break
		}

		end = end[:len(end)-1]

		if len(end) == 0 {
			end = nil
			break
		}
	}

	return end
}

func DeleteOrphanNodes(dbpath string, store string) {
	prefix := []byte(fmt.Sprintf("s/k:%s/o", store))
	endKey := PrefixEndBytes(prefix)

	filter := RangeCompactionFilter{
		startKey: prefix,
		endKey:   endKey,
	}

	// Open a RocksDB database.
	options := gorocksdb.NewDefaultOptions()
	options.SetCompactionFilter(filter)
	db, err := gorocksdb.OpenDb(options, dbpath)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	// Perform a manual compaction to apply the filter.
	db.CompactRange(gorocksdb.Range{Start: filter.startKey, Limit: filter.endKey})
}

func main() {
	DeleteOrphanNodes(os.Args[1], os.Args[2])
}
