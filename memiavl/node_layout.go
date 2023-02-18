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

func GetHeight(buf []byte) int8 {
	return int8(buf[0])
}

func GetVersion(buf []byte) int64 {
	return int64(binary.LittleEndian.Uint32(buf[OffsetVersion : OffsetVersion+4]))
}

func GetSize(buf []byte) int64 {
	return int64(binary.LittleEndian.Uint64(buf[OffsetSize : OffsetSize+8]))
}

func GetKeyOffset(buf []byte) uint64 {
	return binary.LittleEndian.Uint64(buf[OffsetKey : OffsetKey+8])
}

func GetKeyRef(buf []byte) KeyRef {
	return KeyRef{
		Level:  (int8(buf[OffsetFlags]) >> ShiftKeyLevel) & MaskLevel,
		Offset: GetKeyOffset(buf),
	}
}

func GetValueOffset(buf []byte) uint64 {
	return binary.LittleEndian.Uint64(buf[OffsetValue : OffsetValue+8])
}

func GetLeftIndex(buf []byte) uint32 {
	return binary.LittleEndian.Uint32(buf[OffsetLeft : OffsetLeft+4])
}

func GetLeftRef(buf []byte) NodeRef {
	return NodeRef{
		Level: (int8(buf[OffsetFlags]) >> ShiftLeftLevel) & MaskLevel,
		Index: GetLeftIndex(buf),
	}
}

func GetRightIndex(buf []byte) uint32 {
	return binary.LittleEndian.Uint32(buf[OffsetRight : OffsetRight+4])
}

func GetRightRef(buf []byte) NodeRef {
	return NodeRef{
		Level: (int8(buf[OffsetFlags]) >> ShiftRightLevel) & MaskLevel,
		Index: GetRightIndex(buf),
	}
}

func GetHash(buf []byte) []byte {
	return buf[OffsetHash : OffsetHash+SizeHash]
}
