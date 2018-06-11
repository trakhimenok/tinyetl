package workers

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/asterus/tinyetl/etl"
)

type jsonItemParser struct {
	createItemData func() interface{}
}

func NewJsonItemParser(itemDataCreator func() interface{}) etl.Worker {
	if itemDataCreator == nil {
		itemDataCreator = func() interface{} {
			v := make(map[string]interface{})
			return &v
		}
	}
	return jsonItemParser{createItemData: itemDataCreator}
}

var _ etl.OneToOneMapper = (*jsonItemParser)(nil)

func (jsonItemParser) Name() string {
	return "JsonParser"
}

func (worker jsonItemParser) MapItemToItem(c context.Context, item etl.WorkItem) (output etl.WorkItem, err error) {
	var data []byte
	switch item.Data.(type) {
	case []byte:
		data = item.Data.([]byte)
	case string:
		data = []byte(item.Data.(string))
	default:
		panic(fmt.Sprintf("worker jsonItemParser expected input data to be of type []byte or string, got %T", item.Data))
	}
	v := worker.createItemData()
	if err = json.Unmarshal(data, v); err != nil {
		return
	}
	output = etl.NewWorkItem(worker, v)
	return
}
