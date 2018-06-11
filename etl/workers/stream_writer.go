package workers

import (
	"context"
	"fmt"
	"github.com/asterus/tinyetl/etl"
	"io"
)

type streamWriter struct {
	io.Writer
}

var _ etl.OneToOneMapper = (*streamWriter)(nil)

func (streamWriter) Name() string {
	return "streamWriter"
}

func NewStreamWriter(w io.Writer) streamWriter {
	return streamWriter{Writer: w}
}

func (worker streamWriter) MapItemToItem(c context.Context, item etl.WorkItem) (output etl.WorkItem, err error) {
	fmt.Println(item)
	output = item
	return
}
