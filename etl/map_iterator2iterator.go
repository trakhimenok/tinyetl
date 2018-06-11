package etl

import "context"

type IteratorToIteratorMapper interface {
	MapIteratorToIterator(c context.Context, items Iterator) (output Iterator, err error)
}
