package utils

import (
	"log"
)

type DatasetInput struct {
	Data []string
}
type DatasetOutput struct {
	Message    string
	SortedData []string
}

func CheckError(err error) {
	// if an error is returned, print it to the console and exit
	if err != nil {
		log.Fatal(err)
	}
}
