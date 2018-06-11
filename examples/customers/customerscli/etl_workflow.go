package customerscli

import (
	"github.com/astec/tinyetl/etl"
	"github.com/astec/tinyetl/examples/customers/models"
	"strings"
	"fmt"
	"github.com/astec/tinyetl/examples/customers/geo"
	etlWorkers "github.com/astec/tinyetl/etl/workers"
	"context"
)

func CreateWorkflow(sorting string) (workflow etl.Workflow) {
	workers := createWorkflowWorkers(sorting)
	return etl.NewWorkflow(workers)
}

func createWorkflowWorkers(sorting string) (workers []etl.Worker) {
	const kilometers = 1000

	intercomOffice := geo.Point{Latitude: 53.339428, Longitude: -6.257664}
	workers = []etl.Worker{
		//etlWorkers.DataToConsolePrinter{},
		etlWorkers.FileInput{},      // Read input from files. Can nbe replaced with HTTP client in future.
		etlWorkers.StreamSplitter{}, // Split stream into chunks by line break
		//etlWorkers.DataToConsolePrinter{},
		etlWorkers.NewJsonItemParser(func() interface{} { // Parses chunk of stream data as JSON into provider struct
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

	sortBy := GetSortBy(sorting)
	workers = addSorterIfRequested(workers, sortBy)

	workers = append(workers, etlWorkers.NewMapper("Customer => CSV", func(c context.Context, input etl.WorkItem) (output interface{}, err error) {
		customer := input.Data.(models.CustomerShort)
		output = fmt.Sprintf("%d,%v", customer.UserID, customer.Name)
		return
	}))

	// Output result to console as CSV as output target&format were not specified
	workers = append(workers, etlWorkers.DataToConsolePrinter{})
	return
}

func addSorterIfRequested(workers []etl.Worker, sortBy string) []etl.Worker {
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
	return workers
}
