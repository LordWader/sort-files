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
benchmark - 30 sec for 66000000 lines
*/
const (
	WORKER_POOL = 100
)

func MakeDataFile(processChan <-chan int, resultChan chan<- bool, fileSize int) {
	min := -1000000000000
	max := 1000000000000
	for num := range processChan {
		fileWriter := file_processors.NewFileWriter(fmt.Sprintf("test_data/%d.txt", num+1))
		// 40k lines for 5 Kib file size
		for i := 0; i < fileSize; i++ {
			fileWriter.AppendToBuffer(rand.Intn(max-min+1) + min)
		}
		// Write remainder to file
		fileWriter.WriteToFile()
		fileWriter.File.Close()
		resultChan <- true
	}
}

func prepareTestData(numOfFiles int, fileSize int) {
	err := os.Mkdir("test_data", os.ModePerm)
	if err != nil {
		fmt.Errorf("Can't create folder for test data: %v", err)
	}
	// Make pool of workers for data creation
	processChan := make(chan int)
	resultChan := make(chan bool)
	// setup workers
	for i := 0; i < WORKER_POOL; i++ {
		go MakeDataFile(processChan, resultChan, fileSize)
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
	defer close(processChan)
	defer close(resultChan)
}

func init() {
	//prepareTestData(10, 10)
	//prepareTestData(10, 1000)
	//prepareTestData(1000, 40000)
	prepareTestData(499_999, 400)
	//prepareTestData(10_000, 40_000)
	prepareTestData(1, 200_000_000)
}

func BenchmarkMergeSortedFiles(b *testing.B) {
	main()
}
