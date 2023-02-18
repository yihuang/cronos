package memiavl

import (
	"encoding/binary"
)

const (
	// encoding key/value length as 4 bytes with little endianness.
	SizeKeyLen   = 4
	SizeValueLen = 4
)

// PersistedNode is backed by serialized byte array, usually mmap-ed from disk file.
// Encoding format (all integers are encoded in little endian):
// - height  : int8          // padded to 4bytes
// - version : int32
// - size    : int64
// - key     : int64
// - left    : int32         // node index, inner node only
// - right   : int32         // node index, inner node only
// - value   : int64 offset  // leaf node only
// - hash    : [32]byte
type PersistedNode struct {
	snapshot *Snapshot
	offset   uint64
}

var _ Node = PersistedNode{}

func (node PersistedNode) Height() int8 {
	return GetHeight(node.snapshot.nodes, node.offset)
}

func (node PersistedNode) Version() int64 {
	return GetVersion(node.snapshot.nodes, node.offset)
}

func (node PersistedNode) Size() int64 {
	return GetSize(node.snapshot.nodes, node.offset)
}

func (node PersistedNode) Key() []byte {
	keyOffset := GetKeyOffset(node.snapshot.nodes, node.offset)
	keyLen := uint64(binary.LittleEndian.Uint32(node.snapshot.keys[keyOffset:]))
	keyOffset += SizeKeyLen
	return node.snapshot.keys[keyOffset : keyOffset+keyLen]
}

// Value result is not defined for non-leaf node.
func (node PersistedNode) Value() []byte {
	valueOffset := GetValueOffset(node.snapshot.nodes, node.offset)
	valueLen := uint64(binary.LittleEndian.Uint32(node.snapshot.values[valueOffset:]))
	valueOffset += SizeValueLen
	return node.snapshot.values[valueOffset : valueOffset+valueLen]
}

// Left result is not defined for leaf nodes.
func (node PersistedNode) Left() Node {
	nodeIndex := GetLeftIndex(node.snapshot.nodes, node.offset)
	return PersistedNode{snapshot: node.snapshot, offset: uint64(nodeIndex) * SizeNode}
}

// Right result is not defined for leaf nodes.
func (node PersistedNode) Right() Node {
	nodeIndex := GetRightIndex(node.snapshot.nodes, node.offset)
	return PersistedNode{snapshot: node.snapshot, offset: uint64(nodeIndex) * SizeNode}
}

func (node PersistedNode) Hash() []byte {
	return GetHash(node.snapshot.nodes, node.offset)
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
