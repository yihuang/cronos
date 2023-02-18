package memiavl

import (
	"bytes"
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

func (node PersistedNode) data() []byte {
	return node.snapshot.nodes[node.offset : node.offset+SizeNode]
}

func (node PersistedNode) Height() int8 {
	return GetHeight(node.data())
}

func (node PersistedNode) isLeaf() bool {
	return node.Height() == 0
}

func (node PersistedNode) Version() int64 {
	return GetVersion(node.data())
}

func (node PersistedNode) Size() int64 {
	return GetSize(node.data())
}

func (node PersistedNode) Key() []byte {
	return node.snapshot.Key(GetKeyOffset(node.data()))
}

// Value result is not defined for non-leaf node.
func (node PersistedNode) Value() []byte {
	return node.snapshot.Value(GetValueOffset(node.data()))
}

// Left result is not defined for leaf nodes.
func (node PersistedNode) Left() Node {
	nodeIndex := GetLeftIndex(node.data())
	return PersistedNode{snapshot: node.snapshot, offset: uint64(nodeIndex) * SizeNode}
}

// Right result is not defined for leaf nodes.
func (node PersistedNode) Right() Node {
	nodeIndex := GetRightIndex(node.data())
	return PersistedNode{snapshot: node.snapshot, offset: uint64(nodeIndex) * SizeNode}
}

func (node PersistedNode) Hash() []byte {
	return GetHash(node.data())
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

func (node PersistedNode) Get(key []byte) []byte {
	return getPersistedNode(node.snapshot, node.offset, key)
}

func getPersistedNode(snapshot *Snapshot, offset uint64, key []byte) []byte {
	buf := snapshot.nodes[offset : offset+SizeNode]
	height := GetHeight(buf)
	nodeKey := snapshot.Key(GetKeyOffset(buf))
	if height == 0 {
		if bytes.Equal(key, nodeKey) {
			return snapshot.Value(GetValueOffset(buf))
		}
		return nil
	}

	if bytes.Compare(key, nodeKey) == -1 {
		return getPersistedNode(snapshot, uint64(GetLeftIndex(buf))*SizeNode, key)
	}
	return getPersistedNode(snapshot, uint64(GetRightIndex(buf))*SizeNode, key)
}
