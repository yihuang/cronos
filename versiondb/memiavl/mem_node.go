package memiavl

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

type MemNode struct {
	height  int8
	size    int64
	version int64
	key     []byte
	value   []byte
	left    Node
	right   Node

	hash []byte
}

var _ Node = (*MemNode)(nil)

func newLeafNode(key, value []byte, version int64) *MemNode {
	return &MemNode{
		key: key, value: value, version: version, size: 1,
	}
}

func (node *MemNode) isLeaf() bool {
	return node.height == 0
}

func (node *MemNode) Height() int8 {
	return node.height
}

func (node *MemNode) Size() int64 {
	return node.size
}

func (node *MemNode) Version() int64 {
	return node.version
}

func (node *MemNode) Key() []byte {
	return node.key
}

func (node *MemNode) Value() []byte {
	return node.value
}

func (node *MemNode) Left() Node {
	return node.left
}

func (node *MemNode) Right() Node {
	return node.right
}

// Mutate clears hash and update version field to prepare for further modifications.
func (node *MemNode) Mutate(version int64) *MemNode {
	node.version = version
	node.hash = nil
	return node
}

// Writes the node's hash to the given `io.Writer`. This function recursively calls
// children to update hashes.
func (node *MemNode) writeHashBytes(w io.Writer) error {
	var (
		n   int
		buf [binary.MaxVarintLen64]byte
	)

	n = binary.PutVarint(buf[:], int64(node.height))
	if _, err := w.Write(buf[0:n]); err != nil {
		return fmt.Errorf("writing height, %w", err)
	}
	n = binary.PutVarint(buf[:], node.size)
	if _, err := w.Write(buf[0:n]); err != nil {
		return fmt.Errorf("writing size, %w", err)
	}
	n = binary.PutVarint(buf[:], node.version)
	if _, err := w.Write(buf[0:n]); err != nil {
		return fmt.Errorf("writing version, %w", err)
	}

	// Key is not written for inner nodes, unlike writeBytes.

	if node.isLeaf() {
		if err := EncodeBytes(w, node.key, buf[:]); err != nil {
			return fmt.Errorf("writing key, %w", err)
		}

		// Indirection needed to provide proofs without values.
		// (e.g. ProofLeafNode.ValueHash)
		valueHash := sha256.Sum256(node.value)

		if err := EncodeBytes(w, valueHash[:], buf[:]); err != nil {
			return fmt.Errorf("writing value, %w", err)
		}
	} else {
		if node.left == nil || node.right == nil {
			return errors.New("empty child")
		}
		if err := EncodeBytes(w, node.left.Hash(), buf[:]); err != nil {
			return fmt.Errorf("writing left hash, %w", err)
		}
		if err := EncodeBytes(w, node.right.Hash(), buf[:]); err != nil {
			return fmt.Errorf("writing right hash, %w", err)
		}
	}

	return nil
}

// Computes the hash of the node without computing its descendants. Must be
// called on nodes which have descendant node hashes already computed.
func (node *MemNode) Hash() []byte {
	if node == nil {
		return nil
	}
	if node.hash != nil {
		return node.hash
	}

	h := sha256.New()
	if err := node.writeHashBytes(h); err != nil {
		panic(err)
	}
	node.hash = h.Sum(nil)
	return node.hash
}

func (node *MemNode) updateHeightSize() {
	node.height = maxInt8(node.left.Height(), node.right.Height()) + 1
	node.size = node.left.Size() + node.right.Size()
}

func (node *MemNode) calcBalance() int {
	return int(node.left.Height()) - int(node.right.Height())
}

func calcBalance(node Node) int {
	return int(node.Left().Height()) - int(node.Right().Height())
}

// Invariant: node is returned by `Mutate(version)`.
//
//      S               L
//     / \      =>     / \
//    L                   S
//   / \                 / \
//     LR               LR
func (node *MemNode) rotateRight(version int64) *MemNode {
	newSelf := node.left.Mutate(version)
	node.left = node.left.Right()
	newSelf.right = node
	node.updateHeightSize()
	newSelf.updateHeightSize()
	return newSelf
}

// Invariant: node is returned by `Mutate(version)`.
//
//   S              R
//  / \     =>     / \
//      R         S
//     / \       / \
//   RL             RL
func (node *MemNode) rotateLeft(version int64) *MemNode {
	newSelf := node.right.Mutate(version)
	node.right = node.right.Left()
	newSelf.left = node
	node.updateHeightSize()
	newSelf.updateHeightSize()
	return newSelf
}

// Invariant: node is returned by `Mutate(version)`.
func (node *MemNode) reBalance(version int64) *MemNode {
	balance := node.calcBalance()
	if balance > 1 {
		leftBalance := calcBalance(node.left)
		if leftBalance >= 0 {
			// left left
			return node.rotateRight(version)
		} else {
			// left right
			node.left = node.left.Mutate(version).rotateLeft(version)
			return node.rotateRight(version)
		}
	} else if balance < -1 {
		rightBalance := calcBalance(node.right)
		if rightBalance <= 0 {
			// right right
			return node.rotateLeft(version)
		} else {
			// right left
			node.right = node.right.Mutate(version).rotateRight(version)
			return node.rotateLeft(version)
		}
	} else {
		// nothing changed
		return node
	}
}

// EncodeBytes writes a varint length-prefixed byte slice to the writer.
func EncodeBytes(w io.Writer, bz []byte, buf []byte) error {
	n := binary.PutUvarint(buf, uint64(len(bz)))
	if _, err := w.Write(buf[0:n]); err != nil {
		return err
	}
	_, err := w.Write(bz)
	return err
}

func maxInt8(a, b int8) int8 {
	if a > b {
		return a
	}
	return b
}
