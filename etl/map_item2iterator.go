package etl

import (
	"context"
	"io"
)

type ItemToIteratorMapper interface {
	MapItemToIterator(c context.Context, item WorkItem) (output Iterator, err error)
}

type itemToIteratorMapper struct {
	c              context.Context
	items          Iterator
	worker         ItemToIteratorMapper
	outputIterator Iterator
	err            error
}

func (iterator *itemToIteratorMapper) Next() error {
	if iterator.err != nil { // Done
		return iterator.err
	}

	// On 1st Next or when previous outputIterator ended
	if iterator.outputIterator == nil {
		if iterator.nextInputItem(); iterator.err != nil {
			return iterator.err
		}
	}

	if iterator.err = iterator.outputIterator.Next(); iterator.err != nil {
		if iterator.err == io.EOF {
			iterator.err = nil
			iterator.outputIterator = nil
			return iterator.Next()
		}
	}

	return iterator.err
}

func (iterator *itemToIteratorMapper) nextInputItem() (err error) {
	if iterator.err = iterator.items.Next(); iterator.err != nil {
		return iterator.err
	}
	currentInputItem := iterator.items.CurrentItem()
	iterator.outputIterator, iterator.err = iterator.worker.MapItemToIterator(iterator.c, currentInputItem)
	return iterator.err
}

func (iterator itemToIteratorMapper) CurrentItem() WorkItem {
	return iterator.outputIterator.CurrentItem()
}
