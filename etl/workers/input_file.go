package workers

import (
	"context"
	"fmt"
	"github.com/asterus/tinyetl/etl"
	"github.com/pkg/errors"
	"os"
)

// FileInput opens files in read-only mode for processing
type FileInput struct {
	ContinueOnFileOpenError bool
}

func (FileInput) Name() string {
	return "FileInput"
}

// Make sure FileInput satisfies Worker interface
var _ etl.OneToOneMapper = (*FileInput)(nil)

// Process takes list of file names and returns list of corresponding io.Reader's
func (worker FileInput) MapItemToItem(c context.Context, item etl.WorkItem) (output etl.WorkItem, err error) {
	fileName, ok := item.Data.(string)
	if !ok {
		err = fmt.Errorf("worker FileInput expects input data to be a string, got %T", item.Data)
		return
	}
	if fileName == "" {
		err = errors.New("etl.worker.FileInput: file name is required")
		return
	}

	file, err := os.Open(fileName)
	if err != nil {
		return output, err
	}
	output = etl.NewWorkItem(worker, file)
	return
}
