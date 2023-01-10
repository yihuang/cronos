package memiavl

import "bytes"

// Node interface encapsulate the interface of both PersistedNode and MemNode.
type Node interface {
	Height() int8
	Size() int64
	Version() int64
	Key() []byte
	Value() []byte
	Left() Node
	Right() Node
	Hash() []byte

	// PersistedNode clone a new node, MemNode modify in place
	Mutate(version int64) *MemNode
}

func isLeaf(node Node) bool {
	return node.Height() == 0
}

// setRecursive do set operation.
// it always do modification and return new `MemNode`, even if the value is the same.
// also returns if it's an update or insertion, if update, the tree height and balance is not changed.
func setRecursive(node Node, key, value []byte, version int64) (*MemNode, bool) {
	if node == nil {
		return newLeafNode(key, value, version), true
	}

	nodeKey := node.Key()
	if isLeaf(node) {
		switch bytes.Compare(key, nodeKey) {
		case -1:
			return &MemNode{
				height:  1,
				size:    2,
				version: version,
				key:     nodeKey,
				left:    newLeafNode(key, value, version),
				right:   node,
			}, false
		case 1:
			return &MemNode{
				height:  1,
				size:    2,
				version: version,
				key:     key,
				left:    node,
				right:   newLeafNode(key, value, version),
			}, false
		default:
			newNode := node.Mutate(version)
			newNode.value = value
			return newNode, true
		}
	} else {
		var (
			newChild, newNode *MemNode
			updated           bool
		)
		if bytes.Compare(key, nodeKey) == -1 {
			newChild, updated = setRecursive(node.Left(), key, value, version)
			newNode = node.Mutate(version)
			newNode.left = newChild
		} else {
			newChild, updated = setRecursive(node.Right(), key, value, version)
			newNode = node.Mutate(version)
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
// - (nil, origNode, nil) -> nothing changed in subtree
// - (value, nil, newKey) -> leaf node is removed
// - (value, new node, newKey) -> subtree changed
func removeRecursive(node Node, key []byte, version int64) ([]byte, Node, []byte) {
	if node == nil {
		return nil, nil, nil
	}

	if isLeaf(node) {
		if bytes.Equal(node.Key(), key) {
			return node.Value(), nil, nil
		} else {
			return nil, node, nil
		}
	} else {
		if bytes.Compare(key, node.Key()) == -1 {
			value, newLeft, newKey := removeRecursive(node.Left(), key, version)
			if value == nil {
				return nil, node, nil
			}
			if newLeft == nil {
				return value, node.Right(), node.Key()
			}
			newNode := node.Mutate(version)
			newNode.left = newLeft
			newNode.updateHeightSize()
			return value, newNode.reBalance(version), newKey
		} else {
			value, newRight, newKey := removeRecursive(node.Right(), key, version)
			if value == nil {
				return nil, node, nil
			}
			if newRight == nil {
				return value, node.Left(), nil
			}
			newNode := node.Mutate(version)
			newNode.right = newRight
			if newKey != nil {
				newNode.key = newKey
			}
			newNode.updateHeightSize()
			return value, newNode.reBalance(version), nil
		}
	}
}
