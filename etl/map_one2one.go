package etl

import "context"

type OneToOneMapper interface {
	MapItemToItem(c context.Context, item WorkItem) (output WorkItem, err error)
}

type oneToOneMapper struct {
	c           context.Context
	worker      OneToOneMapper
	items       Iterator
	currentItem WorkItem
}

func newOneToOneMapper(c context.Context, items Iterator, worker OneToOneMapper) *oneToOneMapper {
	if items == nil {
		panic("items == nil")
	}
	return &oneToOneMapper{c: c, items: items, worker: worker}
}

func (iterator *oneToOneMapper) Next() (err error) {
	if err = iterator.items.Next(); err != nil {
		return
	}
	inputItem := iterator.items.CurrentItem()
	iterator.currentItem, err = iterator.worker.MapItemToItem(iterator.c, inputItem)
	return
}

func (iterator oneToOneMapper) CurrentItem() WorkItem {
	return iterator.currentItem
}
