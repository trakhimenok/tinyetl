package customerscli

import (
	"github.com/astec/tinyetl/etl"
	"io"
	"testing"
)

func TestGetSortBy(t *testing.T) {
	if sortBy := GetSortBy("ort=name"); sortBy != "name" {
		t.Errorf("kingpin bug not fixed")
	}
}

func TestGetFileNamesIterator(t *testing.T) {
	result := GetFileNamesIterator("")
	var items []etl.WorkItem
	var err error
	for {
		if err = result.Next(); err != nil {
			break
		}
		items = append(items, result.CurrentItem())
	}
	if err != io.EOF {
		t.Fatalf("unexpected error: %v", err)
	}
	if i := len(items); i != 1 {
		t.Fatalf("unexpected len: %d", i)
	}
	if v := items[0].Data.(string); v != "customers.txt" {
		t.Fatalf("unexpected value: %v", v)
	}
}
