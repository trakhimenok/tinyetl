package etl

import (
	"context"
	"io"
)

type repeaterWorker struct { // Keep inside ETL package as is used in tests
	repeat int
}

func newRepeater(repeat int) repeaterWorker {
	if repeat <= 0 {
		panic("repeat <= 0")
	}
	return repeaterWorker{repeat: repeat}
}

func (repeaterWorker) Name() string {
	return "repeaterWorker"
}

//var _ etl.ItemToIteratorMapper = (*repeaterWorker)(nil)

func (worker repeaterWorker) MapItemToIterator(c context.Context, item WorkItem) (output Iterator, err error) {
	return &repeater{worker: worker, item: item}, nil
}

type repeater struct {
	worker    repeaterWorker
	item      WorkItem
	iteration int
}

func (r *repeater) Next() error {
	r.iteration++
	if r.iteration > r.worker.repeat {
		return io.EOF
	}
	return nil
}

func (r *repeater) CurrentItem() WorkItem {
	return r.item
}
