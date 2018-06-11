package workers

import (
	"context"
	"github.com/astec/tinyetl/etl"
)

type filterWorker struct {
	condition FilterCondition
}

var (
	_ etl.IteratorToIteratorMapper = (*filterWorker)(nil)
)

type FilterCondition func(c context.Context, item etl.WorkItem) (include bool, err error)

func NewFilter(condition FilterCondition) filterWorker {
	return filterWorker{condition: condition}
}

func (filterWorker) Name() string {
	return "filterWorker"
}

func (worker filterWorker) MapIteratorToIterator(c context.Context, iterator etl.Iterator) (output etl.Iterator, err error) {
	output = filter{iterator: iterator, condition: worker.condition}
	return
}

type filter struct {
	c         context.Context
	iterator  etl.Iterator
	condition FilterCondition
}

func (filter filter) Next() (err error) {
	for {
		if err = filter.iterator.Next(); err != nil {
			return
		}
		var include bool
		if include, err = filter.condition(filter.c, filter.iterator.CurrentItem()); include {
			// Stop on good items, skip bad ones.
			break
		}
	}
	return
}

func (filter filter) CurrentItem() etl.WorkItem {
	return filter.iterator.CurrentItem()
}
