package tmdb

import (
	"bytes"
	"testing"

	"github.com/cosmos/cosmos-sdk/codec"
	codectypes "github.com/cosmos/cosmos-sdk/codec/types"
	"github.com/crypto-org-chain/cronos/file"
	"github.com/crypto-org-chain/cronos/versiondb"
	"github.com/crypto-org-chain/cronos/x/cronos/types"
	"github.com/stretchr/testify/require"
	dbm "github.com/tendermint/tm-db"
)

func storeCreator() versiondb.VersionStore {
	return NewStore(dbm.NewMemDB(), dbm.NewMemDB(), dbm.NewMemDB())
}

func TestTMDB(t *testing.T) {
	versiondb.Run(t, storeCreator)
}

func TestFeed(t *testing.T) {
	registry := codectypes.NewInterfaceRegistry()
	types.RegisterInterfaces(registry)
	cdc := codec.NewProtoCodec(registry)
	for i := 0; i < 4; i++ {
		buf := new(bytes.Buffer)
		v := int64(i)
		storePairs := versiondb.MockStoreKVPairs(v)
		for i := range storePairs {
			bz, err := cdc.MarshalLengthPrefixed(&storePairs[i])
			require.NoError(t, err)
			_, err = buf.Write(bz)
			require.NoError(t, err)
		}
		pairs, err := file.DecodeData(buf.Bytes())
		require.NoError(t, err)
		require.NotEmpty(t, pairs)
		store := storeCreator()
		require.NoError(t, store.PutAtVersion(v, pairs))
		for _, pair := range pairs {
			value, err := store.GetAtVersion(pair.StoreKey, pair.Key, &v)
			if pair.Delete {
				require.Nil(t, value)
			} else {
				require.NotNil(t, value)
			}
			require.NoError(t, err)
		}
	}
}