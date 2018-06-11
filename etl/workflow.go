package etl

import (
	"context"
	"fmt"
	"io"
)

type Workflow struct {
	workers []Worker
}

func NewWorkflow(workers []Worker) Workflow {
	return Workflow{workers: workers}
}

func (workflow Workflow) Execute(c context.Context, items Iterator) (err error) {

	// Create chain of iterators
	for _, worker := range workflow.workers {
		items, err = workflow.executeWorker(c, worker, items)
		if err != nil {
			return
		} else if items == nil {
			err = fmt.Errorf("worker %v have not returned output iterator", worker.Name())
			return
		}
	}

	// Consume final items from last worker to drive the whole workflow stream processing
	for err == nil {
		err = items.Next()
	}

	if err == io.EOF { // EndOfFile is an expected flag for end of workflow stream
		err = nil
	}

	return
}

func (workflow Workflow) executeWorker(c context.Context, worker Worker, items Iterator) (output Iterator, err error) {
	switch w := worker.(type) {
	case OneToOneMapper:
		output = newOneToOneMapper(c, items, w)
		return
	case ItemToIteratorMapper:
		output = &itemToIteratorMapper{c: c, items: items, worker: w}
		return
	case IteratorToIteratorMapper:
		return w.MapIteratorToIterator(c, items)
	default:
		panic(fmt.Sprintf("worker has unknown type %T, name: %v", worker, worker.Name()))
	}
}
