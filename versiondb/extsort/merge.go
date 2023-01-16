package extsort

import (
	"container/heap"
)

type heapEntry struct {
	stream NextFunc
	value  []byte
}

func newHeapEntry(stream NextFunc) (*heapEntry, error) {
	value, err := stream()
	if err != nil {
		return nil, err
	}
	if value == nil {
		return nil, nil
	}
	return &heapEntry{
		stream: stream,
		value:  value,
	}, nil
}

func (entry *heapEntry) update() (err error) {
	entry.value, err = entry.stream()
	return
}

type MultiWayMerge struct {
	entries    []*heapEntry
	lesserFunc LesserFunc
}

var _ heap.Interface = (*MultiWayMerge)(nil)

func NewMultiWayMerge(streams []NextFunc, lesserFunc LesserFunc) (*MultiWayMerge, error) {
	var entries []*heapEntry
	for _, stream := range streams {
		entry, err := newHeapEntry(stream)
		if err != nil {
			return nil, err
		}
		if entry != nil {
			entries = append(entries, entry)
		}
	}

	merge := &MultiWayMerge{
		entries:    entries,
		lesserFunc: lesserFunc,
	}

	heap.Init(merge)

	return merge, nil
}

func (merge *MultiWayMerge) Len() int {
	return len(merge.entries)
}

func (merge *MultiWayMerge) Swap(i, j int) {
	merge.entries[i], merge.entries[j] = merge.entries[j], merge.entries[i]
}

func (merge *MultiWayMerge) Less(i, j int) bool {
	return merge.lesserFunc(merge.entries[i].value, merge.entries[j].value)
}

func (merge *MultiWayMerge) Push(x interface{}) {
	entry := x.(*heapEntry)
	merge.entries = append(merge.entries, entry)
}

func (merge *MultiWayMerge) Pop() interface{} {
	l := merge.Len()
	item := merge.entries[l-1]
	merge.entries = merge.entries[:l-1]
	return item
}

func (merge *MultiWayMerge) Next() ([]byte, error) {
	if merge.Len() == 0 {
		return nil, nil
	}
	minEntry := merge.entries[0]
	result := minEntry.value
	if err := minEntry.update(); err != nil {
		return nil, err
	}
	if minEntry.value == nil {
		heap.Remove(merge, 0)
	} else {
		heap.Fix(merge, 0)
	}
	return result, nil
}
