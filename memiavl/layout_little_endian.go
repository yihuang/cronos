//go:build !nativebyteorder
// +build !nativebyteorder

package memiavl

import (
	"encoding/binary"
)

// Nodes is a continuously stored IAVL nodes
type Nodes struct {
	data []byte
}

func NewNodes(data []byte) (Nodes, error) {
	return Nodes{data}, nil
}

func (nodes Nodes) Node(i uint32) *NodeLayout {
	return &NodeLayout{data: nodes.data[i*SizeNode:]}
}

// see comment of `PersistedNode`
type NodeLayout struct {
	data []byte
}

func (node *NodeLayout) Height() uint8 {
	return node.data[OffsetHeight]
}

func (node *NodeLayout) Version() uint32 {
	return binary.LittleEndian.Uint32(node.data[OffsetVersion : OffsetVersion+4])
}

func (node *NodeLayout) Size() uint32 {
	return binary.LittleEndian.Uint32(node.data[OffsetSize : OffsetSize+4])
}

func (node *NodeLayout) KeyOffset() uint64 {
	return binary.LittleEndian.Uint64(node.data[OffsetKeyOffset : OffsetKeyOffset+8])
}

func (node *NodeLayout) KeySlice() (uint64, uint32) {
	_ = node.data[SizeNode-1]
	l := uint32(node.data[OffsetKeyLen]) | uint32(node.data[OffsetKeyLen+1])<<8 | uint32(node.data[OffsetKeyLen+2])<<16
	return node.KeyOffset(), l
}

func (node *NodeLayout) KeyNode() uint32 {
	return binary.LittleEndian.Uint32(node.data[OffsetKeyNode : OffsetKeyNode+4])
}

func (node *NodeLayout) LeafIndex() uint32 {
	return binary.LittleEndian.Uint32(node.data[OffsetLeafIndex : OffsetLeafIndex+4])
}

func (node *NodeLayout) Hash() []byte {
	return node.data[OffsetHash : OffsetHash+SizeHash]
}
