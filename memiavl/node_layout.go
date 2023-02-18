package memiavl

import (
	"crypto/sha256"
	"encoding/binary"
)

// Node buffer layout (all integers are encoded in little endian):
// - height  : int8
// - flags   : int8          // left level: 2, right level: 2, key level: 2
// - _       : int16         // padding
// - version : int32
// - size    : int64
// - key     : int64
// - left    : int32         // node index, inner node only
// - right   : int32         // node index, inner node only
// - value   : int64 offset  // leaf node only
// - hash    : [32]byte
const (
	OffsetHeight  = 0
	OffsetFlags   = 1
	OffsetVersion = OffsetHeight + 4
	OffsetSize    = OffsetVersion + 4
	OffsetKey     = OffsetSize + 8
	OffsetRight   = OffsetKey + 8
	OffsetLeft    = OffsetRight + 4
	OffsetValue   = OffsetKey + 8
	OffsetHash    = OffsetValue + 8

	ShiftLeftLevel  = 0
	ShiftRightLevel = 2
	ShiftKeyLevel   = 4
	MaskLevel       = 0b11

	SizeHash            = sha256.Size
	SizeNodeWithoutHash = OffsetHash
	SizeNode            = SizeNodeWithoutHash + SizeHash
)

// KeyRef represents a key offset in a level
type KeyRef struct {
	Level  int8
	Offset uint64
}

// NodeRef represents a node index in a level
type NodeRef struct {
	Level int8
	Index uint32
}

func GetHeight(nodes []byte, offset uint64) int8 {
	return int8(nodes[offset])
}

func GetVersion(nodes []byte, offset uint64) int64 {
	return int64(binary.LittleEndian.Uint32(nodes[offset+OffsetVersion : offset+OffsetVersion+4]))
}

func GetSize(nodes []byte, offset uint64) int64 {
	return int64(binary.LittleEndian.Uint64(nodes[offset+OffsetSize : offset+OffsetSize+8]))
}

func GetKeyOffset(nodes []byte, offset uint64) uint64 {
	return binary.LittleEndian.Uint64(nodes[offset+OffsetKey : offset+OffsetKey+8])
}

func GetKeyRef(nodes []byte, offset uint64) KeyRef {
	return KeyRef{
		Level:  (int8(nodes[offset+OffsetFlags]) >> ShiftKeyLevel) & MaskLevel,
		Offset: GetKeyOffset(nodes, offset),
	}
}

func GetValueOffset(nodes []byte, offset uint64) uint64 {
	return binary.LittleEndian.Uint64(nodes[offset+OffsetValue : offset+OffsetValue+8])
}

func GetLeftIndex(nodes []byte, offset uint64) uint32 {
	return binary.LittleEndian.Uint32(nodes[offset+OffsetLeft : offset+OffsetLeft+4])
}

func GetLeftRef(nodes []byte, offset uint64) NodeRef {
	return NodeRef{
		Level: (int8(nodes[offset+OffsetFlags]) >> ShiftLeftLevel) & MaskLevel,
		Index: GetLeftIndex(nodes, offset),
	}
}

func GetRightIndex(nodes []byte, offset uint64) uint32 {
	return binary.LittleEndian.Uint32(nodes[offset+OffsetRight : offset+OffsetRight+4])
}

func GetRightRef(nodes []byte, offset uint64) NodeRef {
	return NodeRef{
		Level: (int8(nodes[offset+OffsetFlags]) >> ShiftRightLevel) & MaskLevel,
		Index: GetRightIndex(nodes, offset),
	}
}

func GetHash(nodes []byte, offset uint64) []byte {
	offset += OffsetHash
	return nodes[offset : offset+SizeHash]
}
