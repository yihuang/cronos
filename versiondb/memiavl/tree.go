package memiavl

import (
	"encoding/binary"
	"os"
	"path/filepath"

	"github.com/edsrzf/mmap-go"
	"github.com/pkg/errors"
)

// verify change sets by replay them to rebuild iavl tree and verify the root hashes
type Tree struct {
	version int64
	root    Node
}

func NewEmptyTree(version int64) *Tree {
	return &Tree{version: version}
}

// LoadSnapshot mmap the blob files and create the root node.
func LoadSnapshot(snapshotDir string) (*Tree, error) {
	nodesFile := filepath.Join(snapshotDir, "nodes")
	keysFile := filepath.Join(snapshotDir, "keys")
	valuesFile := filepath.Join(snapshotDir, "values")
	metadataFile := filepath.Join(snapshotDir, "metadata")

	bz, err := os.ReadFile(metadataFile)
	if err != nil {
		return nil, err
	}
	version := binary.LittleEndian.Uint64(bz[:])
	rootIndex := binary.LittleEndian.Uint64(bz[8:])

	fpNodes, err := os.Open(nodesFile)
	if err != nil {
		return nil, err
	}

	nodes, err := mmap.Map(fpNodes, mmap.RDONLY, 0)
	if err != nil {
		fpNodes.Close()
		return nil, err
	}

	if len(nodes) == 0 {
		// empty tree
		fpNodes.Close()
		return nil, nil
	}

	if uint64(len(nodes)) != (rootIndex+1)*SizeNode {
		fpNodes.Close()
		return nil, errors.Errorf("nodes file size %d don't match root node index %d", len(nodes), rootIndex)
	}

	fpKeys, err := os.Open(keysFile)
	if err != nil {
		fpNodes.Close()
		return nil, err
	}
	keys, err := mmap.Map(fpKeys, mmap.RDONLY, 0)
	if err != nil {
		fpNodes.Close()
		fpKeys.Close()
		return nil, err
	}
	fpValues, err := os.Open(valuesFile)
	if err != nil {
		fpNodes.Close()
		fpKeys.Close()
		return nil, err
	}
	values, err := mmap.Map(fpValues, mmap.RDONLY, 0)
	if err != nil {
		fpNodes.Close()
		fpKeys.Close()
		fpValues.Close()
		return nil, err
	}

	blobs := &PersistedBlobs{
		nodesFile:  fpNodes,
		keysFile:   fpKeys,
		valuesFile: fpValues,

		nodes:  nodes,
		keys:   keys,
		values: values,
	}

	// the last node is the root node
	root := blobs.Node(rootIndex * SizeNode)

	return &Tree{
		version: int64(version),
		root:    root,
	}, nil
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
