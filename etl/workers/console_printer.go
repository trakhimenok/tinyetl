package workers

import (
	"context"
	"fmt"
	"github.com/astec/tinyetl/etl"
)

type DataToConsolePrinter struct {
}

var _ etl.OneToOneMapper = (*DataToConsolePrinter)(nil)

func (DataToConsolePrinter) Name() string {
	return "DataToConsolePrinter"
}

func (worker DataToConsolePrinter) MapItemToItem(c context.Context, item etl.WorkItem) (output etl.WorkItem, err error) {
	var data interface{}
	switch item.Data.(type) {
	case []byte:
		data = string(item.Data.([]byte))
	default:
		data = item.Data
	}
	if _, err = fmt.Println(data); err != nil {
		return
	}
	output = item
	return
}
