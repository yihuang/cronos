package memiavl

import (
	"path/filepath"
)

// verify change sets by replay them to rebuild iavl tree and verify the root hashes
type Tree struct {
	version int64
	root    Node
}

func NewEmptyTree(version int64) *Tree {
	return &Tree{version: version}
}

// LoadTreeFromSnapshot mmap the blob files and create the root node.
func LoadTreeFromSnapshot(snapshotDir string) (*Tree, *PersistedBlobs, error) {
	blobs, version, rootIndex, err := loadSnapshot(snapshotDir)
	if err != nil {
		return nil, nil, err
	}

	if blobs == nil {
		return NewEmptyTree(int64(version)), nil, err
	}

	return &Tree{
		version: int64(version),
		root:    blobs.Node(rootIndex),
	}, blobs, nil
}

func (t *Tree) Set(key, value []byte) {
	t.root, _ = setRecursive(t.root, key, value, t.version+1)
}

func (t *Tree) Remove(key []byte) {
	_, t.root, _ = removeRecursive(t.root, key, t.version+1)
}

// SaveVersion returns current root hash and increase version number
func (t *Tree) SaveVersion(updateHash bool) ([]byte, int64, error) {
	var hash []byte
	if updateHash {
		hash = t.root.Hash()
	}
	t.version++
	return hash, t.version, nil
}

func (t *Tree) Version() int64 {
	return t.version
}

// RootHash updates the hashes and return the current root hash
func (t *Tree) RootHash() []byte {
	return t.root.Hash()
}

func (t *Tree) WriteSnapshot(snapshotDir string) error {
	return WriteSnapshot(
		t.root,
		t.version,
		filepath.Join(snapshotDir, "nodes"),
		filepath.Join(snapshotDir, "keys"),
		filepath.Join(snapshotDir, "values"),
		filepath.Join(snapshotDir, "metadata"),
	)
}
