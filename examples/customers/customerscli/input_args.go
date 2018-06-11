package customerscli

import (
	"github.com/astec/tinyetl/etl"
	"strings"
)

func GetFileNamesIterator(fileNamesArg string) etl.Iterator {
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

func GetSortBy(sorting string) (sortBy string) {
	sortBy = sorting
	if i := strings.Index(sortBy, "="); i >= 0 { // workaround for kingpin bug
		sortBy = sortBy[i+1:]
	}
	return
}
