package memiavl

import "encoding/binary"

const (
	/** serialized node format, all integers are encoded in little endian.
	height  : int8                  // padded to 4bytes
	version : int32
	size    : int64
	key     : int64
	left    : int32                 // inner node only
	right   : int32                 // inner node only
	value   : int64 offset          // leaf node only
	hash    : [32]byte
	*/
	OffsetHeight  = 0
	OffsetVersion = OffsetHeight + 4
	OffsetSize    = OffsetVersion + 4
	OffsetKey     = OffsetSize + 8
	OffsetLeft    = OffsetKey + 8
	OffsetRight   = OffsetLeft + 4
	OffsetValue   = OffsetKey + 8
	OffsetHash    = OffsetValue + 8

	SizeHash = 32
	SizeNode = OffsetHash + SizeHash
)

type PersistedBlobs struct {
	nodes  []byte
	keys   []byte
	values []byte
}

// PersistedNode is backed by serialized bytes
// - height: int8  padded to 4 bytes
// - version: int32
// - size: int64
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
	nodeOffset := binary.LittleEndian.Uint32(node.blobs.nodes[offset : offset+4])
	return PersistedNode{blobs: node.blobs, offset: uint64(nodeOffset)}
}

// Right result is not defined for leaf nodes.
func (node PersistedNode) Right() Node {
	offset := node.offset + OffsetRight
	nodeOffset := binary.LittleEndian.Uint32(node.blobs.nodes[offset : offset+4])
	return PersistedNode{blobs: node.blobs, offset: uint64(nodeOffset)}
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
