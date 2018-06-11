package etl

import (
	"testing"
)

func TestItemToIteratorMapper(t *testing.T) {
	const repeatCount = 3

	inputWorkItems := []WorkItem{
		{Data: "First item"},
		{Data: "Second item"},
		{Data: "Third item"},
	}
	inputIterator := NewSliceIterator(inputWorkItems...)
	mapper := itemToIteratorMapper{worker: newRepeater(repeatCount), items: inputIterator}

	outputItems := make([]WorkItem, 0, len(inputWorkItems)*repeatCount)
	var err error
	for {
		if err = mapper.Next(); err != nil {
			break
		}
		currentItem := mapper.CurrentItem()
		outputItems = append(outputItems, currentItem)
	}
	if expected := len(inputWorkItems) * repeatCount; expected != len(outputItems) {
		t.Fatalf("Expected %v items, got %v: %v", expected, len(outputItems), outputItems)
	}
	for i, inputItem := range inputWorkItems {
		for j := 1; j <= repeatCount; j++ {
			k := i*repeatCount + j - 1
			if outputItems[k].Data != inputItem.Data {
				t.Errorf("Output[%v] expected to be equal to input[%v]\n\tExpected: %v\n\tActual: %v", k, i, inputItem.Data, outputItems[k].Data)
			}
		}
	}
}
