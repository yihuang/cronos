package outersort

import (
	"bytes"
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

const (
	ItemSize = 20
	ItemTpl  = "testkey-%08d"
)

func doTestOuterSorter(t *testing.T, chunkSize int64, inputCount int) {
	sorter := NewOuterSorter("/tmp", chunkSize, func(a, b []byte) bool {
		return bytes.Compare(a, b) == -1
	})
	defer func() {
		require.NoError(t, sorter.Close())
	}()

	var expItems [][]byte
	for i := 0; i < inputCount; i++ {
		item := []byte(fmt.Sprintf(ItemTpl, i))
		expItems = append(expItems, item)
	}

	randItems := make([][]byte, len(expItems))
	copy(randItems, expItems)
	rand.Seed(0)
	rand.Shuffle(len(randItems), func(i, j int) { randItems[i], randItems[j] = randItems[j], randItems[i] })

	// feed in random order
	for _, item := range randItems {
		err := sorter.Feed(item)
		require.NoError(t, err)
	}
	reader, err := sorter.Finalize()
	require.NoError(t, err)

	var result [][]byte
	for {
		item, err := reader.Next()
		require.NoError(t, err)
		if item == nil {
			break
		}
		result = append(result, item)
	}

	require.Equal(t, expItems, result)
}

func TestOuterSorter(t *testing.T) {
	doTestOuterSorter(t, ItemSize*100, 100)
	doTestOuterSorter(t, ItemSize*100, 1550)
	doTestOuterSorter(t, ItemSize*100, 155000)
}
