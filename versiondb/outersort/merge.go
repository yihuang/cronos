package outersort

import "container/heap"

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

type MultiMerge2 struct {
	entries    []*heapEntry
	lesserFunc LesserFunc
}

var _ heap.Interface = (*MultiMerge2)(nil)

func NewMultiMerge2(streams []NextFunc, lesserFunc LesserFunc) (*MultiMerge2, error) {
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

	return &MultiMerge2{
		entries:    entries,
		lesserFunc: lesserFunc,
	}, nil
}

func (merge *MultiMerge2) Len() int {
	return len(merge.entries)
}

func (merge *MultiMerge2) Swap(i, j int) {
	merge.entries[i], merge.entries[j] = merge.entries[j], merge.entries[i]
}

func (merge *MultiMerge2) Less(i, j int) bool {
	return merge.lesserFunc(merge.entries[i].value, merge.entries[j].value)
}

func (merge *MultiMerge2) Push(x interface{}) {
	entry := x.(*heapEntry)
	merge.entries = append(merge.entries, entry)
}

func (merge *MultiMerge2) Pop() interface{} {
	l := merge.Len()
	item := merge.entries[l-1]
	merge.entries = merge.entries[:l-1]
	return item
}

func (merge *MultiMerge2) Next() ([]byte, error) {
	if merge.Len() == 0 {
		return nil, nil
	}
	min := merge.entries[0]
	result := min.value
	min.update()
	if min.value == nil {
		heap.Remove(merge, 0)
	} else {
		heap.Fix(merge, 0)
	}
	return result, nil
}
