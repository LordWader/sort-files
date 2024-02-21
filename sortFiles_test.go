package main

import (
	"files_sorter/worker"
	"fmt"
	"math/rand"
	"os"
	"testing"
)

const (
	WORKER_POOL = 100
	FILE_SIZE   = 40000
)

func MakeDataFile(processChan <-chan int, resultChan chan<- bool) {
	min := -1000000000000
	max := 1000000000000
	for num := range processChan {
		file, err := os.Create(fmt.Sprintf("test_data/%d.txt", num+1))
		if err != nil {
			panic(err)
		}
		fileWriter := worker.NewFileWriter(file)
		// 40k lines for 5 Kib file size
		for i := 0; i < FILE_SIZE; i++ {
			fileWriter.AppendToBuffer(rand.Intn(max-min+1) + min)
		}
		// Write remainder to file
		fileWriter.WriteToFile()
		err = file.Close()
		if err != nil {
			panic(err)
		}
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
	prepareTestData(1000)
}

func BenchmarkMergeSortedFiles(b *testing.B) {
	main()
}
