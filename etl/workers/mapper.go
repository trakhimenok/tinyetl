package workers

import (
	"context"
	"github.com/astec/tinyetl/etl"
)

type mapperWorker struct {
	name string
	f    MapperFunc
}

var (
	_ etl.IteratorToIteratorMapper = (*mapperWorker)(nil)
)

type MapperFunc func(c context.Context, input etl.WorkItem) (output interface{}, err error)

func NewMapper(name string, f MapperFunc) mapperWorker {
	return mapperWorker{name: name, f: f}
}

func (worker mapperWorker) Name() string {
	return worker.name
}

func (worker mapperWorker) MapIteratorToIterator(c context.Context, iterator etl.Iterator) (output etl.Iterator, err error) {
	output = &mapper{worker: worker, iterator: iterator, f: worker.f}
	return
}

type mapper struct {
	worker      mapperWorker
	c           context.Context
	iterator    etl.Iterator
	f           MapperFunc
	currentItem etl.WorkItem
}

func (mapper *mapper) Next() (err error) {
	if err = mapper.iterator.Next(); err != nil {
		return
	}
	var v interface{}
	if v, err = mapper.f(mapper.c, mapper.iterator.CurrentItem()); err != nil {
		return
	}
	mapper.currentItem = etl.NewWorkItem(mapper.worker, v)
	return
}

func (mapper mapper) CurrentItem() etl.WorkItem {
	return mapper.currentItem
}
