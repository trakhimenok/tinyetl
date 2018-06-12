package main

import (
	"context"
	"fmt"
	"github.com/astec/tinyetl/examples/customers/customerscli"
	"gopkg.in/alecthomas/kingpin.v2"
)

func main() {
	var (
		input   = kingpin.Flag("input", "Input file or URL").Short('i').String()
		sorting = kingpin.Flag("sort", "Specifies how to sort customers: id, name. Prepend '-' for descending order.").Short('s').String()
	)
	_ = kingpin.Parse() // Parses CLI input arguments

	// Creates workflow defined by chain of workers.
	// Initialization is in separate package to support unit-testing.
	// Quick link in case if you browse not in IDE:
	// https://github.com/astec/tinyetl/blob/master/examples/customers/customerscli/etl_workflow.go
	workflow := customerscli.CreateWorkflow(nil, *sorting)

	// Create initial work items as list of files to process.
	fileNamesIterator := customerscli.GetFileNamesIterator(*input)

	c := context.Background()
	err := workflow.Execute(c, fileNamesIterator) // Run processing

	if err != nil { // Verify for error and report if needed
		fmt.Println("ERROR:", err)
	}
}
