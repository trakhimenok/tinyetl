package main

import (
	"context"
	"fmt"
	"gopkg.in/alecthomas/kingpin.v2"
	"github.com/astec/tinyetl/examples/customers/customerscli"
)

var (
	input   = kingpin.Flag("input", "Input file or URL").Short('i').String()
	sorting = kingpin.Flag("sort", "Specifies how to sort customers: id, name. Prepend '-' for descending order.").Short('s').String()
)

func main() {
	c := context.Background()
	_ = kingpin.Parse()

	// Creates workflow defined by chain of workers.
	// Initialization is in separate package to support unit-testing.
	workflow := customerscli.CreateWorkflow(*sorting)

	// Create initial work items as list of files to process.
	fileNamesIterator := customerscli.GetFileNamesIterator(*input)

	err := workflow.Execute(c, fileNamesIterator) // Run processing

	if err != nil { // Verify for error and report if needed
		fmt.Println("ERROR:", err)
	}
}

