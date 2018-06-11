package workers

import (
	"context"
	"github.com/astec/tinyetl/etl"
	"io"
	"sort"
)

var (
	_ etl.IteratorToIteratorMapper = (*sorterWorker)(nil)
)

type sorterWorker struct {
	f SortLessFunc
}

type SortLessFunc func(v1, v2 interface{}) bool

func NewSorter(f SortLessFunc) sorterWorker {
	return sorterWorker{f: f}
}

func (sorterWorker) Name() string {
	return "sorterWorker"
}

func (worker sorterWorker) MapIteratorToIterator(c context.Context, iterator etl.Iterator) (output etl.Iterator, err error) {
	var items []etl.WorkItem
	for {
		if err = iterator.Next(); err != nil {
			if err == io.EOF {
				err = nil
				break
			}
			return
		}
		items = append(items, iterator.CurrentItem())
	}
	sort.Sort(sorter{f: worker.f, items: items})
	output = etl.NewSliceIterator(items...)
	return
}

type sorter struct {
	f     SortLessFunc
	items []etl.WorkItem
}

func (sorter sorter) Len() int {
	return len(sorter.items)
}

func (sorter sorter) Less(i, j int) bool {
	return sorter.f(sorter.items[i].Data, sorter.items[j].Data)
}

func (sorter sorter) Swap(i, j int) {
	iData := sorter.items[i].Data
	sorter.items[i].Data = sorter.items[j].Data
	sorter.items[j].Data = iData
}
