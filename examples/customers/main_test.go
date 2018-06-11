package main

import (
	"testing"
	"github.com/asterus/tinyetl/etl"
	"io"
)

func TestGetFileNamesIterator(t *testing.T) {
	result := getFileNamesIterator("")
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
