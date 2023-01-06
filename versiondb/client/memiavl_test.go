package client

import (
	"fmt"
	"testing"

	"github.com/cosmos/iavl"
	"github.com/stretchr/testify/require"
	db "github.com/tendermint/tm-db"
)

type Change struct {
	Delete     bool
	Key, Value []byte
}

var ChangeSets [][]Change

func init() {
	var changes []Change
	ChangeSets = append(ChangeSets,
		[]Change{{Key: []byte("hello"), Value: []byte("world")}},
		[]Change{{Key: []byte("hello"), Value: []byte("world1")}, {Key: []byte("hello1"), Value: []byte("world1")}},
		[]Change{{Key: []byte("hello2"), Value: []byte("world1")}, {Key: []byte("hello3"), Value: []byte("world1")}},
	)

	changes = nil
	for i := 0; i < 1; i++ {
		changes = append(changes, Change{Key: []byte(fmt.Sprintf("hello%02d", i)), Value: []byte("world1")})
	}

	ChangeSets = append(ChangeSets, changes)
	ChangeSets = append(ChangeSets, []Change{{Key: []byte("hello"), Delete: true}, {Key: []byte("hello19"), Delete: true}})

	changes = nil
	for i := 0; i < 21; i++ {
		changes = append(changes, Change{Key: []byte(fmt.Sprintf("aello%02d", i)), Value: []byte("world1")})
	}
	ChangeSets = append(ChangeSets, changes)

	changes = nil
	for i := 0; i < 21; i++ {
		changes = append(changes, Change{Key: []byte(fmt.Sprintf("aello%02d", i)), Delete: true})
	}
	for i := 0; i < 19; i++ {
		changes = append(changes, Change{Key: []byte(fmt.Sprintf("hello%02d", i)), Delete: true})
	}
	ChangeSets = append(ChangeSets, changes)
}

func applyChangeSet(t *Tree, changes []Change) {
	for _, change := range changes {
		if change.Delete {
			t.Remove(change.Key)
		} else {
			t.Set(change.Key, change.Value)
		}
	}
}

func applyChangeSetRef(t *iavl.MutableTree, changes []Change) error {
	for _, change := range changes {
		if change.Delete {
			if _, _, err := t.Remove(change.Key); err != nil {
				return err
			}
		} else {
			if _, err := t.Set(change.Key, change.Value); err != nil {
				return err
			}
		}
	}
	return nil
}

func TestRootHashes(t *testing.T) {
	tree := NewEmptyTree(0)

	// ref impl
	d := db.NewMemDB()
	refTree, err := iavl.NewMutableTree(d, 0, true)
	require.NoError(t, err)

	for _, changes := range ChangeSets {
		applyChangeSet(tree, changes)
		require.NoError(t, applyChangeSetRef(refTree, changes))

		hash, v, err := tree.SaveVersion(true)
		require.NoError(t, err)

		refHash, refV, err := refTree.SaveVersion()
		require.NoError(t, err)

		require.Equal(t, refV, v)
		require.Equal(t, refHash, hash)
	}
}
