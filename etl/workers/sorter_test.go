package workers

import (
	"testing"
	"context"
	"github.com/asterus/tinyetl/etl"
)

func TestSorter(t *testing.T) {
	sorter := NewSorter(func(v1, v2 interface{}) bool {
		return v1.(int) < v2.(int)
	})

	iterator, err := sorter.MapIteratorToIterator(context.Background(), etl.NewSliceIterator(
		etl.NewWorkItem(nil, 3),
		etl.NewWorkItem(nil, 2),
		etl.NewWorkItem(nil, 5),
		etl.NewWorkItem(nil, 1),
	))

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var actual []int
	for {
		if err = iterator.Next(); err != nil {
			break
		}
		actual = append(actual, iterator.CurrentItem().Data.(int))
	}

	expects := []int {1, 2, 3, 5}
	if len(actual) != len(expects) {
		t.Fatalf("unexpected result: %v", actual)
	}
	for i := 0; i < len(actual); i++ {
		if actual[i] != expects[i] {
			t.Errorf("unexpected actual[%d]: %v", i, actual[i])
		}
	}
}
