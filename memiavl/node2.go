package memiavl

import (
	"bytes"
)

const ArenaChunkSize = 1024 * 1024

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

type ArenaChunk [ArenaChunkSize]Node2

type Tree2 struct {
	arena []*ArenaChunk
	size  int64

	// head of freelist, -1 means no free slot
	head int64

	// root node of current tree, -1 means empty tree
	root    int64
	version int64
}

func NewTree2() *Tree2 {
	return &Tree2{root: -1, head: -1, arena: []*ArenaChunk{new(ArenaChunk)}}
}

func (t *Tree2) node(i int64) *Node2 {
	a, b := i/ArenaChunkSize, i%ArenaChunkSize
	return &t.arena[a][b]
}

func (t *Tree2) updateHeightSize(node *Node2) {
	left := t.node(node.left)
	right := t.node(node.right)
	node.height = maxInt8(left.height, right.height) + 1
	node.size = left.size + right.size
}

func (t *Tree2) calcBalance(node *Node2) int {
	left := t.node(node.left)
	right := t.node(node.right)
	return int(left.height - right.height)
}

func (t *Tree2) reBalance(i int64, version int64) int64 {
	node := t.node(i)

	// inline t.updateHeightSize(node)
	left := t.node(node.left)
	right := t.node(node.right)
	node.mutate(version)
	node.height = maxInt8(left.height, right.height) + 1
	node.size = left.size + right.size

	// inline t.calcBalance(node)
	balance := int(left.height - right.height)

	switch {
	case balance > 1:
		// inline t.calcBalance(left)
		leftLeft := t.node(left.left)
		leftRight := t.node(left.right)
		leftBalance := int(leftLeft.height - leftRight.height)
		if leftBalance >= 0 {
			// left left
			// inline t.rotateRight(i, version)
			result := node.left
			node.left = left.right
			left.setRight(i, version)

			// inline t.updateHeightSize(node)
			node.height = maxInt8(leftRight.height, right.height) + 1
			node.size = leftRight.size + right.size
			// inline t.updateHeightSize(left)
			left.height = maxInt8(leftLeft.height, node.height) + 1
			left.size = leftLeft.size + node.size
			return result
		}
		// left right
		leftRightLeft := t.node(leftRight.left)
		leftRightRight := t.node(leftRight.right)
		node.left = left.right
		// inline t.rotateLeft(node.left, version)
		left.setRight(leftRight.left, version)
		leftRight.setLeft(node.left, version)
		// inline t.updateHeightSize(left)
		left.height = maxInt8(leftLeft.height, leftRightLeft.height) + 1
		left.size = leftLeft.size + leftRightLeft.size
		// inline t.updateHeightSize(leftRight)
		leftRight.height = maxInt8(left.height, leftRightRight.height) + 1
		leftRight.size = left.size + leftRightRight.size

		left = leftRight
		leftRight = leftRightRight
		leftLeft = leftRightLeft
		// inline t.rotateRight(i, version)
		result := node.left
		node.left = left.right
		left.right = i

		// inline t.updateHeightSize(node)
		node.height = maxInt8(leftRight.height, right.height) + 1
		node.size = leftRight.size + right.size
		// inline t.updateHeightSize(left)
		left.height = maxInt8(leftLeft.height, node.height) + 1
		left.size = leftLeft.size + node.size
		return result
	case balance < -1:
		rightBalance := t.calcBalance(right)
		if rightBalance <= 0 {
			// right right
			return t.rotateLeft(i, version)
		}
		// right left
		node.right = t.rotateRight(node.right, version)
		return t.rotateLeft(i, version)
	default:
		// nothing changed
		return i
	}
}

func (t *Tree2) rotateRight(i int64, version int64) int64 {
	node := t.node(i)
	lefti := node.left
	left := t.node(lefti)

	node.setLeft(left.right, version)
	left.setRight(i, version)

	t.updateHeightSize(node)
	t.updateHeightSize(left)
	return lefti
}

func (t *Tree2) rotateLeft(i int64, version int64) int64 {
	node := t.node(i)
	righti := node.right
	right := t.node(righti)

	node.setRight(right.left, version)
	right.setLeft(i, version)

	t.updateHeightSize(node)
	t.updateHeightSize(right)
	return righti
}

func (t *Tree2) AddNode(node Node2) int64 {
	if t.head >= 0 {
		var i int64
		p := t.node(t.head)
		i, t.head = t.head, p.left
		*p = node
		return i
	}
	a, b := t.size/ArenaChunkSize, t.size%ArenaChunkSize
	for a >= int64(len(t.arena)) {
		t.arena = append(t.arena, new(ArenaChunk))
	}
	t.arena[a][b] = node
	t.size++
	return t.size - 1
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
	if node.IsLeaf() {
		switch bytes.Compare(key, node.key) {
		case -1:
			i1 := t.AddNode(newLeaf(key, value, version))
			i2 := t.AddNode(Node2{
				height:  1,
				size:    2,
				version: version,
				key:     node.key,
				left:    i1,
				right:   i,
			})
			return i2, false
		case 1:
			i1 := t.AddNode(newLeaf(key, value, version))
			i2 := t.AddNode(Node2{
				height:  1,
				size:    2,
				version: version,
				key:     key,
				left:    i,
				right:   i1,
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
		i = t.reBalance(i, version)
	}

	return i, updated
}
