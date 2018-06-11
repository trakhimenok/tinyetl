package workers

import (
	"bufio"
	"context"
	"fmt"
	"github.com/asterus/tinyetl/etl"
	"io"
)

func (StreamSplitter) getScanner(item etl.WorkItem) *bufio.Scanner {
	reader, isReader := item.Data.(io.Reader)
	if !isReader {
		panic(fmt.Sprintf("ioSplitter expects item.Data to be io.Reader, got: %T", item.Data))
	}
	return bufio.NewScanner(reader)
}

type StreamSplitter struct {
	streamSplitter
}

func (StreamSplitter) Name() string {
	return "SyncStreamSplitter"
}

var _ etl.ItemToIteratorMapper = (*StreamSplitter)(nil)

type streamSplitter struct {
	done        bool
	scanner     *bufio.Scanner
	currentItem etl.WorkItem
}

func (streamSplitter) Name() string {
	return "streamSplitter"
}

func (iterator *streamSplitter) Next() (err error) {
	if iterator.done {
		return iterator.scanner.Err()
	}

	for !iterator.done {
		iterator.done = !iterator.scanner.Scan()
		if err = iterator.scanner.Err(); err != nil {
			return
		}
		data := iterator.scanner.Bytes()
		if len(data) > 0 { // Skip empty lines
			iterator.currentItem = etl.NewWorkItem(iterator, data)
			break
		}
	}

	if iterator.done {
		return io.EOF
	}
	return nil
}

func (iterator *streamSplitter) CurrentItem() etl.WorkItem {
	return iterator.currentItem
}

func (worker StreamSplitter) MapItemToIterator(c context.Context, item etl.WorkItem) (output etl.Iterator, err error) {
	output = &streamSplitter{scanner: worker.getScanner(item)}
	return
}
