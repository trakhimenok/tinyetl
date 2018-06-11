package etl

import "fmt"

// WorkItem contains data to be processed by a Worker
type WorkItem struct {
	lastWorker string
	Data       interface{}
}

func NewWorkItem(lastWorker Worker, data interface{}) WorkItem {
	var s string
	if lastWorker == nil {
		s = "nil"
	} else {
		s = fmt.Sprintf("%T:%v", lastWorker, lastWorker.Name())
	}
	return WorkItem{lastWorker: s, Data: data}
}

type Worker interface {
	Name() string
}

type Iterator interface {
	CurrentItem() WorkItem
	Next() error
}
