package memiavl

import (
	"bytes"
)

const DefaultPreallocate = 1024 * 1024

type Node2 struct {
	height  int8
	size    int64
	version int64
	key     []byte
	value   []byte
	left    int64 // also used to link the free list
	right   int64

	hash []byte
}

func newLeaf(key, value []byte, version int64) Node2 {
	return Node2{
		key: key, value: value, version: version, size: 1,
	}
}

func (node *Node2) Clear() {
	*node = Node2{}
}

func (node *Node2) IsLeaf() bool {
	return node.height == 0
}

func (node *Node2) mutate(version int64) {
	node.version = version
	node.hash = nil
}

// setValue set value field on leaf node
func (node *Node2) setValue(value []byte, version int64) {
	node.mutate(version)
	node.value = value
}

// setLeft set left child on branch node
func (node *Node2) setLeft(i int64, version int64) {
	node.mutate(version)
	node.left = i
}

// setRight set left child on branch node
func (node *Node2) setRight(i int64, version int64) {
	node.mutate(version)
	node.right = i
}

type Tree2 struct {
	arena []Node2
	size  int64

	// head of freelist, -1 means no free slot
	head int64

	// root node of current tree, -1 means empty tree
	root    int64
	version int64
}

func NewTree2() *Tree2 {
	return NewTree2WithCapacity(DefaultPreallocate)
}

func NewTree2WithCapacity(capacity int64) *Tree2 {
	return &Tree2{root: -1, head: -1, arena: make([]Node2, 0, capacity)}
}

func (t *Tree2) node(i int64) *Node2 {
	return &t.arena[i]
}

func (t *Tree2) reBalance(index int64, node *Node2, version int64) int64 {
	left := t.node(node.left)
	right := t.node(node.right)

	balance := int(left.height - right.height)
	switch {
	case balance > 1:
		left.mutate(version)
		leftL := t.node(left.left)
		leftR := t.node(left.right)
		if leftL.height >= leftR.height {
			// left left
			root := node.left

			// update references
			node.left = left.right
			left.right = index

			// update height/size
			node.height = maxInt8(leftR.height, right.height) + 1
			node.size = leftR.size + right.size
			left.height = maxInt8(leftL.height, node.height) + 1
			left.size = leftL.size + node.size
			return root
		}
		// left right
		leftRL := t.node(leftR.left)
		leftRR := t.node(leftR.right)
		root := left.right

		leftR.mutate(version)
		// update references
		left.right = leftR.left
		leftR.left = node.left
		node.left = leftR.right
		leftR.right = index

		// update height/size
		node.height = maxInt8(leftRR.height, right.height) + 1
		node.size = leftRR.size + right.size
		left.height = maxInt8(leftL.height, leftRL.height) + 1
		left.size = leftL.size + leftRL.size
		leftR.height = maxInt8(left.height, node.height) + 1
		leftR.size = left.size + node.size
		return root
	case balance < -1:
		right.mutate(version)
		rightL := t.node(right.left)
		rightR := t.node(right.right)
		if rightL.height <= rightR.height {
			// right right
			root := node.right

			// update references
			node.right = right.left
			right.left = index

			// update height/size
			node.height = maxInt8(left.height, rightL.height) + 1
			node.size = left.size + rightL.size
			right.height = maxInt8(node.height, rightR.height) + 1
			right.size = node.size + rightR.size
			return root
		}
		// right left
		rightLL := t.node(rightL.left)
		rightLR := t.node(rightL.right)
		root := right.left

		rightL.mutate(version)
		// update references
		right.left = rightL.right
		rightL.right = node.right
		node.right = rightL.left
		rightL.left = index

		// update height/size
		node.height = maxInt8(left.height, rightLL.height) + 1
		node.size = left.size + rightLL.size
		right.height = maxInt8(rightLR.height, rightR.height) + 1
		right.size = rightLR.size + rightR.size
		rightL.height = maxInt8(node.height, right.height) + 1
		rightL.size = node.size + right.size
		return root
	default:
		// nothing changed
		return index
	}
}

func (t *Tree2) AddNode(node Node2) int64 {
	if t.head >= 0 {
		var i int64
		p := t.node(t.head)
		i, t.head = t.head, p.left
		*p = node
		return i
	}
	t.arena = append(t.arena, node)
	return int64(len(t.arena)) - 1
}

func (t *Tree2) FreeNode(i int64) {
	t.node(i).left, t.head = t.head, i
}

// Get value for the key, return nil if not found.
func (t *Tree2) Get(key []byte) []byte {
	if t.root < 0 {
		return nil
	}

	return t.getRecursive(t.root, key)
}

func (t *Tree2) getRecursive(i int64, key []byte) []byte {
	for {
		node := t.node(i)
		if node.height == 0 {
			if bytes.Equal(key, node.key) {
				return node.value
			}
			return nil
		}

		if bytes.Compare(key, node.key) == -1 {
			i = node.left
		} else {
			i = node.right
		}
	}
}

func (t *Tree2) Set(key, value []byte) {
	if t.root < 0 {
		t.root = t.AddNode(newLeaf(key, value, t.version+1))
		return
	}
	t.root, _ = t.setRecursive(t.root, key, value, t.version+1)
}

func (t *Tree2) setRecursive(i int64, key, value []byte, version int64) (int64, bool) {
	node := t.node(i)
	if node.height == 0 {
		switch bytes.Compare(key, node.key) {
		case -1:
			i1 := t.AddNode(newLeaf(key, value, version))
			i2 := t.AddNode(Node2{
				height: 1, size: 2, version: version,
				key:   node.key,
				left:  i1,
				right: i,
			})
			return i2, false
		case 1:
			i1 := t.AddNode(newLeaf(key, value, version))
			i2 := t.AddNode(Node2{
				height: 1, size: 2, version: version,
				key:   key,
				left:  i,
				right: i1,
			})
			return i2, false
		default:
			node.setValue(value, version)
			return i, true
		}
	}
	var (
		j       int64
		updated bool
	)
	if bytes.Compare(key, node.key) == -1 {
		j, updated = t.setRecursive(node.left, key, value, version)
		node.setLeft(j, version)
	} else {
		j, updated = t.setRecursive(node.right, key, value, version)
		node.setRight(j, version)
	}

	if !updated {
		i = t.reBalance(i, node, version)
	}

	return i, updated
}
