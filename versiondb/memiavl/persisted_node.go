package memiavl

import (
	"encoding/binary"
	"os"

	"github.com/edsrzf/mmap-go"
	"github.com/pkg/errors"
)

const (
	OffsetHeight  = 0
	OffsetVersion = OffsetHeight + 4
	OffsetSize    = OffsetVersion + 4
	OffsetKey     = OffsetSize + 8
	OffsetRight   = OffsetKey + 4
	OffsetLeft    = OffsetRight + 4
	OffsetValue   = OffsetKey + 8
	OffsetHash    = OffsetValue + 8

	SizeHash = 32
	SizeNode = OffsetHash + SizeHash
)

type PersistedBlobs struct {
	nodesFile  *os.File
	keysFile   *os.File
	valuesFile *os.File

	nodes  []byte
	keys   []byte
	values []byte
}

// OpenIAVLSnapshot mmap the blob files and the root node.
func OpenIAVLSnapshot(nodesFile, keysFile, valuesFile string) (*PersistedBlobs, PersistedNode, error) {
	fpNodes, err := os.Open(nodesFile)
	if err != nil {
		return nil, PersistedNode{}, err
	}

	nodes, err := mmap.Map(fpNodes, mmap.RDONLY, 0)
	if err != nil {
		fpNodes.Close()
		return nil, PersistedNode{}, err
	}

	if len(nodes) == 0 {
		// empty tree
		fpNodes.Close()
		return &PersistedBlobs{}, PersistedNode{}, nil
	}

	if len(nodes)%SizeNode != 0 {
		fpNodes.Close()
		return nil, PersistedNode{}, errors.Errorf("snapshot file size %d is not multiples of node size %d", len(nodes), SizeNode)
	}

	fpKeys, err := os.Open(keysFile)
	if err != nil {
		fpNodes.Close()
		return nil, PersistedNode{}, err
	}
	keys, err := mmap.Map(fpKeys, mmap.RDONLY, 0)
	if err != nil {
		fpNodes.Close()
		fpKeys.Close()
		return nil, PersistedNode{}, err
	}
	fpValues, err := os.Open(valuesFile)
	if err != nil {
		fpNodes.Close()
		fpKeys.Close()
		return nil, PersistedNode{}, err
	}
	values, err := mmap.Map(fpValues, mmap.RDONLY, 0)
	if err != nil {
		fpNodes.Close()
		fpKeys.Close()
		fpValues.Close()
		return nil, PersistedNode{}, err
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
	root := blobs.Node(uint64((len(nodes) - SizeNode) / SizeNode))

	return blobs, root, nil
}

func (blobs *PersistedBlobs) Node(offset uint64) PersistedNode {
	return PersistedNode{
		blobs:  blobs,
		offset: offset,
	}
}

func (blobs *PersistedBlobs) Close() error {
	err1 := blobs.nodesFile.Close()
	err2 := blobs.keysFile.Close()
	err3 := blobs.valuesFile.Close()
	*blobs = PersistedBlobs{}
	if err1 != nil {
		return err1
	}
	if err2 != nil {
		return err2
	}
	return err3
}

// PersistedNode is backed by serialized byte array, usually mmap-ed from disk file.
// Encoding format (all integers are encoded in little endian):
// - height  : int8          // padded to 4bytes
// - version : int32
// - size    : int64
// - key     : int64
// - left    : int32         // inner node only
// - right   : int32         // inner node only
// - value   : int64 offset  // leaf node only
// - hash    : [32]byte
type PersistedNode struct {
	blobs  *PersistedBlobs
	offset uint64
}

var _ Node = PersistedNode{}

func (node PersistedNode) Height() int8 {
	return int8(node.blobs.nodes[node.offset+OffsetHeight])
}

func (node PersistedNode) Version() int64 {
	offset := node.offset + OffsetVersion
	return int64(binary.LittleEndian.Uint32(node.blobs.nodes[offset : offset+4]))
}

func (node PersistedNode) Size() int64 {
	offset := node.offset + OffsetSize
	return int64(binary.LittleEndian.Uint64(node.blobs.nodes[offset : offset+8]))
}

func (node PersistedNode) Key() []byte {
	offset := node.offset + OffsetKey
	keyOffset := binary.LittleEndian.Uint64(node.blobs.nodes[offset : offset+8])
	keyLen := uint64(binary.LittleEndian.Uint16(node.blobs.keys[keyOffset : keyOffset+2]))
	keyOffset += 2
	return node.blobs.keys[keyOffset : keyOffset+keyLen]
}

// Value result is not defined for non-leaf node.
func (node PersistedNode) Value() []byte {
	offset := node.offset + OffsetValue
	valueOffset := binary.LittleEndian.Uint64(node.blobs.nodes[offset : offset+8])
	valueLen := uint64(binary.LittleEndian.Uint16(node.blobs.values[valueOffset : valueOffset+2]))
	valueOffset += 2
	return node.blobs.values[valueOffset : valueOffset+valueLen]
}

// Left result is not defined for leaf nodes.
func (node PersistedNode) Left() Node {
	offset := node.offset + OffsetLeft
	nodeIndex := binary.LittleEndian.Uint32(node.blobs.nodes[offset : offset+4])
	return PersistedNode{blobs: node.blobs, offset: uint64(nodeIndex) * SizeNode}
}

// Right result is not defined for leaf nodes.
func (node PersistedNode) Right() Node {
	offset := node.offset + OffsetRight
	nodeIndex := binary.LittleEndian.Uint32(node.blobs.nodes[offset : offset+4])
	return PersistedNode{blobs: node.blobs, offset: uint64(nodeIndex) * SizeNode}
}

func (node PersistedNode) Hash() []byte {
	offset := node.offset + OffsetHash
	return node.blobs.nodes[offset : offset+32]
}

func (node PersistedNode) Mutate(version int64) *MemNode {
	mnode := &MemNode{
		height:  node.Height(),
		size:    node.Size(),
		version: version,
		key:     node.Key(),
		hash:    node.Hash(),
	}
	if mnode.isLeaf() {
		mnode.value = node.Value()
	} else {
		mnode.left = node.Left()
		mnode.right = node.Right()
	}
	return mnode
}
