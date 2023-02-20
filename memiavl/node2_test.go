package memiavl

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNode2(t *testing.T) {
	tree := NewTree2()
	tree.Set([]byte("hello"), []byte("world"))
	tree.Set([]byte("hello5"), []byte("world"))
	tree.Set([]byte("hello2"), []byte("world"))
	tree.Set([]byte("hello1"), []byte("world"))
	tree.Set([]byte("hello7"), []byte("world"))
	require.Equal(t, []byte("world"), tree.Get([]byte("hello")))
}
