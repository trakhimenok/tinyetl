package etl

import "io"

type SliceIterator struct {
	i     int
	items []WorkItem
}

var _ Iterator = (*SliceIterator)(nil)

func NewSliceIterator(items ...WorkItem) *SliceIterator {
	return &SliceIterator{items: items, i: -1}
}

func (iterator SliceIterator) Add(item WorkItem) {
	iterator.items = append(iterator.items, item)
}

func (iterator *SliceIterator) Next() error {
	if iterator.i+1 >= len(iterator.items) {
		return io.EOF
	}
	iterator.i++
	return nil
}

func (iterator SliceIterator) CurrentItem() WorkItem {
	return iterator.items[iterator.i]
}
