package main

import (
	"files_sorter/file_processors"
	"fmt"
	"math/rand"
	"os"
	"testing"
)

/*
5GB is - 10000 files X 40000
*/
const (
	WORKER_POOL = 100
	FILE_SIZE   = 4000000
)

func MakeDataFile(processChan <-chan int, resultChan chan<- bool) {
	min := -1000000000000
	max := 1000000000000
	for num := range processChan {
		fileWriter := file_processors.NewFileWriter(fmt.Sprintf("test_data/%d.txt", num+1))
		// 40k lines for 5 Kib file size
		for i := 0; i < FILE_SIZE; i++ {
			fileWriter.AppendToBuffer(rand.Intn(max-min+1) + min)
		}
		// Write remainder to file
		fileWriter.WriteToFile()
		fileWriter.File.Close()
		resultChan <- true
	}
}

func prepareTestData(numOfFiles int) {
	err := os.Mkdir("test_data", os.ModePerm)
	if err != nil {
		fmt.Errorf("Can't create folder for test data: %v", err)
	}
	// Make pool of workers for data creation
	processChan := make(chan int)
	resultChan := make(chan bool)
	// setup workers
	for i := 0; i < WORKER_POOL; i++ {
		go MakeDataFile(processChan, resultChan)
	}
	go func() {
		for i := 0; i < numOfFiles; i++ {
			processChan <- i
		}
	}()
	// block until we generate all data
	for i := 0; i < numOfFiles; i++ {
		<-resultChan
	}
}

func init() {
	prepareTestData(100)
}

func BenchmarkMergeSortedFiles(b *testing.B) {
	main()
}
