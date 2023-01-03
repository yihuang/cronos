package tsrocksdb

import (
	"bytes"
	"encoding/binary"

	"github.com/linxGnu/grocksdb"
)

// CreateTSComparator should be compatible with builtin timestamp comparator.
func CreateTSComparator() *grocksdb.Comparator {
	return grocksdb.NewComparatorWithTimestamp(
		"leveldb.BytewiseComparator.u64ts", TimestampSize, compare, compareTS, compareWithoutTS,
	)
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
