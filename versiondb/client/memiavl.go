package client

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// verify change sets by replay them to rebuild iavl tree and verify the root hashes
type Tree struct {
	version int64
	root    *Node
}

func NewEmptyTree(version int64) *Tree {
	return &Tree{version: version}
}

func (t *Tree) Set(key, value []byte) {
	t.root, _ = t.root.setRecursive(key, value, t.version+1)
}

func (t *Tree) Remove(key []byte) {
	_, t.root = t.root.removeRecursive(key, t.version+1)
}

// SaveVersion returns current root hash and increase version number
func (t *Tree) SaveVersion(updateHash bool) ([]byte, int64, error) {
	var (
		err  error
		hash []byte
	)
	if updateHash {
		hash, err = t.root._hash()
		if err != nil {
			return nil, 0, err
		}
	}
	t.version++
	return hash, t.version, nil
}

// RootHash updates the hashes and return the current root hash
func (t *Tree) RootHash() ([]byte, error) {
	return t.root._hash()
}

type Node struct {
	height  int8
	size    int64
	version int64
	key     []byte
	value   []byte
	left    *Node
	right   *Node

	hash []byte
}

func newLeafNode(key, value []byte, version int64) *Node {
	return &Node{
		key: key, value: value, version: version, size: 1,
	}
}

func (node *Node) isLeaf() bool {
	return node.height == 0
}

// Writes the node's hash to the given io.Writer. This function expects
// child hashes to be already set.
func (node *Node) writeHashBytes(w io.Writer) error {
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
		leftHash, err := node.left._hash()
		if err != nil {
			return err
		}
		rightHash, err := node.right._hash()
		if err != nil {
			return err
		}
		if err := EncodeBytes(w, leftHash, buf[:]); err != nil {
			return fmt.Errorf("writing left hash, %w", err)
		}
		if err := EncodeBytes(w, rightHash, buf[:]); err != nil {
			return fmt.Errorf("writing right hash, %w", err)
		}
	}

	return nil
}

// Computes the hash of the node without computing its descendants. Must be
// called on nodes which have descendant node hashes already computed.
func (node *Node) _hash() ([]byte, error) {
	if node == nil {
		return nil, nil
	}
	if node.hash != nil {
		return node.hash, nil
	}

	h := sha256.New()
	if err := node.writeHashBytes(h); err != nil {
		return nil, err
	}
	node.hash = h.Sum(nil)
	return node.hash, nil
}

func (node *Node) updateHeightSize() {
	node.height = maxInt8(node.left.height, node.right.height) + 1
	node.size = node.left.size + node.right.size
}

func (node *Node) calcBalance() int {
	return int(node.left.height) - int(node.right.height)
}

// mutate clear hash and update version to prepare for direct modification.
func (node *Node) mutate(version int64) *Node {
	node.version = version
	node.hash = nil
	return node
}

//      S               L
//     / \      =>     / \
//    L                   S
//   / \                 / \
//     LR               LR
func (node *Node) rotateRight(version int64) *Node {
	newSelf := node.left.mutate(version)
	node.mutate(version).left = node.left.right
	newSelf.right = node
	node.updateHeightSize()
	newSelf.updateHeightSize()
	return newSelf
}

//   S              R
//  / \     =>     / \
//      R         S
//     / \       / \
//   RL             RL
func (node *Node) rotateLeft(version int64) *Node {
	newSelf := node.right.mutate(version)
	node.mutate(version).right = node.right.left
	newSelf.left = node
	node.updateHeightSize()
	newSelf.updateHeightSize()
	return newSelf
}

func (node *Node) reBalance(version int64) *Node {
	balance := node.calcBalance()
	if balance > 1 {
		leftBalance := node.left.calcBalance()
		if leftBalance >= 0 {
			// left left
			return node.rotateRight(version)
		} else {
			// left right
			node.left = node.left.rotateLeft(version)
			return node.rotateRight(version)
		}
	} else if balance < -1 {
		rightBalance := node.right.calcBalance()
		if rightBalance <= 0 {
			// right right
			return node.rotateLeft(version)
		} else {
			// right left
			node.right = node.right.rotateRight(version)
			return node.rotateLeft(version)
		}
	} else {
		// nothing changed
		return node
	}
}

// setRecursive do set operation.
// it always do modification and return new node, even if the value is the same.
// also returns if it's update or insertion, in update case, the tree height and balance not changed.
func (node *Node) setRecursive(key, value []byte, version int64) (*Node, bool) {
	if node == nil {
		return newLeafNode(key, value, version), true
	}

	if node.isLeaf() {
		switch bytes.Compare(key, node.key) {
		case -1:
			return &Node{
				height:  1,
				size:    2,
				version: version,
				key:     node.key,
				left:    newLeafNode(key, value, version),
				right:   node,
			}, false
		case 1:
			return &Node{
				height:  1,
				size:    2,
				version: version,
				key:     key,
				left:    node,
				right:   newLeafNode(key, value, version),
			}, false
		default:
			node.mutate(version).value = value
			return node, true
		}
	} else {
		var (
			newChild, newNode *Node
			updated           bool
		)
		if bytes.Compare(key, node.key) == -1 {
			newChild, updated = node.left.setRecursive(key, value, version)
			newNode = node.mutate(version)
			newNode.left = newChild
		} else {
			newChild, updated = node.right.setRecursive(key, value, version)
			newNode = node.mutate(version)
			newNode.right = newChild
		}

		if !updated {
			newNode.updateHeightSize()
			newNode = newNode.reBalance(version)
		}

		return newNode, updated
	}
}

// removeRecursive returns:
// - (nil, _) -> nothing changed in subtree
// - (value, nil) -> leaf node is removed
// - (value, new node) -> subtree changed
func (node *Node) removeRecursive(key []byte, version int64) ([]byte, *Node) {
	if node.isLeaf() {
		if bytes.Equal(node.key, key) {
			return node.value, nil
		} else {
			return nil, node
		}
	} else {
		if bytes.Compare(key, node.key) == -1 {
			value, newLeft := node.left.removeRecursive(key, version)
			if value == nil {
				return nil, node
			}
			if newLeft == nil {
				return value, node.right
			}
			node.mutate(version).left = newLeft
			node.updateHeightSize()
			return value, node.reBalance(version)
		} else {
			value, newRight := node.right.removeRecursive(key, version)
			if value == nil {
				return nil, node
			}
			if newRight == nil {
				return value, node.left
			}
			node.mutate(version).right = newRight
			node.updateHeightSize()
			return value, node.reBalance(version)
		}
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
