package memiavl

import (
	"encoding/binary"
	"os"

	"github.com/ledgerwatch/erigon-lib/mmap"
)

const (
	OffsetHeight  = 0
	OffsetVersion = OffsetHeight + 4
	OffsetSize    = OffsetVersion + 4
	OffsetKey     = OffsetSize + 8
	OffsetRight   = OffsetKey + 8
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

	// mmap handle for windows (this is used to close mmap)
	nodesHandle  *[mmap.MaxMapSize]byte
	keysHandle   *[mmap.MaxMapSize]byte
	valuesHandle *[mmap.MaxMapSize]byte
}

func (blobs *PersistedBlobs) Node(index uint64) PersistedNode {
	return PersistedNode{
		blobs:  blobs,
		offset: index * SizeNode,
	}
}

func (blobs *PersistedBlobs) Close() error {
	_ = mmap.Munmap(blobs.nodes, blobs.nodesHandle)
	_ = mmap.Munmap(blobs.keys, blobs.keysHandle)
	_ = mmap.Munmap(blobs.values, blobs.valuesHandle)

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
	return int64(binary.LittleEndian.Uint32(node.blobs.nodes[node.offset+OffsetVersion:]))
}

func (node PersistedNode) Size() int64 {
	return int64(binary.LittleEndian.Uint64(node.blobs.nodes[node.offset+OffsetSize:]))
}

func (node PersistedNode) Key() []byte {
	keyOffset := binary.LittleEndian.Uint64(node.blobs.nodes[node.offset+OffsetKey:])
	keyLen := uint64(binary.LittleEndian.Uint16(node.blobs.keys[keyOffset:]))
	keyOffset += 2
	return node.blobs.keys[keyOffset : keyOffset+keyLen]
}

// Value result is not defined for non-leaf node.
func (node PersistedNode) Value() []byte {
	valueOffset := binary.LittleEndian.Uint64(node.blobs.nodes[node.offset+OffsetValue:])
	valueLen := uint64(binary.LittleEndian.Uint16(node.blobs.values[valueOffset : valueOffset+2]))
	valueOffset += 2
	return node.blobs.values[valueOffset : valueOffset+valueLen]
}

// Left result is not defined for leaf nodes.
func (node PersistedNode) Left() Node {
	nodeIndex := binary.LittleEndian.Uint32(node.blobs.nodes[node.offset+OffsetLeft:])
	return PersistedNode{blobs: node.blobs, offset: uint64(nodeIndex) * SizeNode}
}

// Right result is not defined for leaf nodes.
func (node PersistedNode) Right() Node {
	nodeIndex := binary.LittleEndian.Uint32(node.blobs.nodes[node.offset+OffsetRight:])
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
	}
	if mnode.isLeaf() {
		mnode.value = node.Value()
	} else {
		mnode.left = node.Left()
		mnode.right = node.Right()
	}
	return mnode
}
