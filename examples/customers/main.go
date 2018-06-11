package main

import (
	"context"
	"fmt"
	"github.com/astec/tinyetl/etl"
	etlWorkers "github.com/astec/tinyetl/etl/workers"
	"github.com/astec/tinyetl/examples/customers/geo"
	"github.com/astec/tinyetl/examples/customers/models"
	"gopkg.in/alecthomas/kingpin.v2"
	"strings"
)

var (
	input   = kingpin.Flag("input", "Input file or URL").Short('i').String()
	sorting = kingpin.Flag("sort", "Specifies how to sort customers: id, name. Prepend '-' for descending order.").Short('s').String()
)

func main() {
	c := context.Background()
	_ = kingpin.Parse()

	//command = kingpin.Parse()
	//args := os.Args
	//fmt.Println("Args:", args)
	//fmt.Println("Command: " + command)

	workers := createWorkflowWorker() // Define what to do

	workflow := etl.NewWorkflow(workers) // Create workflow to orchestrate work defined by workers

	fileNamesIterator := getFileNamesIterator(*input) // Create initial work items as list of files to process

	err := workflow.Execute(c, fileNamesIterator) // Run processing

	if err != nil { // Verify for error and report if needed
		fmt.Println("ERROR:", err)
	}
}

func getFileNamesIterator(fileNamesArg string) etl.Iterator {
	if fileNamesArg == "" { // Default file name, useful in DEV mode
		fileNamesArg = "customers.txt"
	} else if i := strings.Index(fileNamesArg, "="); i >= 0 { // workaround for kingpin bug
		fileNamesArg = fileNamesArg[i+1:]
	}

	fileNames := strings.Split(fileNamesArg, ",")

	fileNameWorkItems := make([]etl.WorkItem, len(fileNames))
	for i, fileName := range fileNames {
		fileNameWorkItems[i] = etl.NewWorkItem(nil, fileName)
	}
	return etl.NewSliceIterator(fileNameWorkItems...)
}

func createWorkflowWorker() (workers []etl.Worker) {
	const kilometers = 1000

	intercomOffice := geo.Point{Latitude: 53.339428, Longitude: -6.257664}
	workers = []etl.Worker{
		//etlWorkers.DataToConsolePrinter{},
		etlWorkers.FileInput{},
		etlWorkers.StreamSplitter{},
		//etlWorkers.DataToConsolePrinter{},
		etlWorkers.NewJsonItemParser(func() interface{} {
			return &models.CustomerExtended{}
		}),
		etlWorkers.NewFilter(func(c context.Context, item etl.WorkItem) (ok bool, err error) {
			customer := item.Data.(*models.CustomerExtended)
			var customerLocation geo.Point
			if customerLocation, err = customer.GeoPoint(); err != nil {
				return
			}
			return geo.Distance(customerLocation, intercomOffice) < 100*kilometers, nil
		}),
		// Minimise memory footprint as next sorting step requires loading all items to memory
		etlWorkers.NewMapper("CustomerExtended=>CustomerShot", func(c context.Context, input etl.WorkItem) (output interface{}, err error) {
			customer := input.Data.(*models.CustomerExtended)
			return customer.CustomerShort, nil
		}),
	}
	sortBy := *sorting
	if i := strings.Index(sortBy, "="); i >= 0 { // workaround for kingpin bug
		sortBy = sortBy[i+1:]
	}
	switch sortBy {
	case "":
		// No sorting
	case "id", "-id": // Default sorting
		workers = append(workers, etlWorkers.NewSorter(func(v1, v2 interface{}) bool {
			c1 := v1.(models.CustomerShort)
			c2 := v2.(models.CustomerShort)
			if strings.HasPrefix(sortBy, "-") {
				return c1.UserID > c2.UserID
			}
			return c1.UserID < c2.UserID
		}))
	case "name", "-name":
		workers = append(workers, etlWorkers.NewSorter(func(v1, v2 interface{}) bool {
			c1 := v1.(models.CustomerShort)
			c2 := v2.(models.CustomerShort)
			if strings.HasPrefix(sortBy, "-") {
				return c1.Name > c2.Name
			}
			return c1.Name < c2.Name
		}))
	default:
		fmt.Println("Unknown value of parameter --sort: " + sortBy)
		return []etl.Worker{}
	}
	workers = append(workers, etlWorkers.NewMapper("Customer => CSV", func(c context.Context, input etl.WorkItem) (output interface{}, err error) {
		customer := input.Data.(models.CustomerShort)
		output = fmt.Sprintf("%d,%v", customer.UserID, customer.Name)
		return
	}))

	// Output result to console as CSV as output target&format were not specified
	workers = append(workers, etlWorkers.DataToConsolePrinter{})
	return
}
