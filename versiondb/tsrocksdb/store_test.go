package tsrocksdb

import (
	"encoding/binary"
	"testing"

	"github.com/crypto-org-chain/cronos/versiondb"
	"github.com/linxGnu/grocksdb"
	"github.com/stretchr/testify/require"
)

func TestTSVersionDB(t *testing.T) {
	versiondb.Run(t, func() versiondb.VersionStore {
		store, err := NewStore(t.TempDir())
		require.NoError(t, err)
		return store
	})
}

func TestDebug(t *testing.T) {
	store, err := NewStore(t.TempDir())
	require.NoError(t, err)

	var ts [8]byte
	binary.LittleEndian.PutUint64(ts[:], 1000)

	err = store.db.PutCFWithTS(grocksdb.NewDefaultWriteOptions(), store.cfHandle, []byte("hello"), ts[:], []byte{1})
	require.NoError(t, err)

	v := int64(999)
	bz, err := store.db.GetCF(newTSReadOptions(&v), store.cfHandle, []byte("hello"))
	defer bz.Free()
	require.NoError(t, err)
	require.False(t, bz.Exists())

	bz, err = store.db.GetCF(newTSReadOptions(nil), store.cfHandle, []byte("hello"))
	defer bz.Free()
	require.NoError(t, err)
	require.Equal(t, []byte{1}, bz.Data())

	v = int64(1000)
	it := store.db.NewIteratorCF(newTSReadOptions(&v), store.cfHandle)
	it.SeekToFirst()
	require.True(t, it.Valid())
	require.Equal(t, []byte("hello"), it.Key().Data())

	binary.LittleEndian.PutUint64(ts[:], 1002)
	err = store.db.PutCFWithTS(grocksdb.NewDefaultWriteOptions(), store.cfHandle, []byte("hella"), ts[:], []byte{2})
	require.NoError(t, err)

	v = int64(1002)
	it = store.db.NewIteratorCF(newTSReadOptions(&v), store.cfHandle)
	it.SeekToFirst()
	require.True(t, it.Valid())
	require.Equal(t, []byte("hella"), it.Key().Data())
}
