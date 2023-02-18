package memiavl

import (
	"bytes"
	"encoding/binary"
	"math/rand"
	"testing"

	"github.com/tidwall/btree"
)

type itemT struct {
	key, value []byte
}

func lessG(a, b itemT) bool {
	return bytes.Compare(a.key, b.key) == -1
}

func int64ToItemT(n uint64) itemT {
	var key, value [8]byte
	binary.BigEndian.PutUint64(key[:], n)
	binary.LittleEndian.PutUint64(value[:], n)
	return itemT{
		key:   key[:],
		value: value[:],
	}
}

func genRandItems(n int) []itemT {
	items := make([]itemT, n)
	itemsM := make(map[uint64]bool)
	for i := 0; i < n; i++ {
		for {
			key := uint64(rand.Int63n(10000000000000000))
			if !itemsM[key] {
				itemsM[key] = true
				items[i] = int64ToItemT(key)
				break
			}
		}
	}
	return items
}

func BenchmarkRandomSet(b *testing.B) {
	items := genRandItems(1000000)

	b.ResetTimer()
	b.Run("memiavl", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			tree := New()
			for _, item := range items {
				tree.Set(item.key, item.value)
			}
		}
	})
	b.Run("btree-degree-1", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			bt := btree.NewBTreeGOptions(lessG, btree.Options{
				NoLocks: true,
				Degree:  1,
			})
			for _, item := range items {
				bt.Set(item)
			}
		}
	})
	b.Run("btree-degree-32", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			bt := btree.NewBTreeGOptions(lessG, btree.Options{
				NoLocks: true,
				Degree:  32,
			})
			for _, item := range items {
				bt.Set(item)
			}
		}
	})
}
