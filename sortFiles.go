package main

import (
	"files_sorter/bfs_walker"
	"files_sorter/worker"
	"fmt"
	"math"
	_ "net/http/pprof"
	"os"
	"runtime"
	"time"
)

var workerPool int

func TrackMemoryUsage() {
	for {
		select {
		case <-time.Tick(time.Second * 5):
			var m runtime.MemStats
			runtime.ReadMemStats(&m)
			fmt.Printf("Alloc = %v MiB", m.Alloc/1024/1024)
			fmt.Printf("\tTotalAlloc = %v MiB", m.TotalAlloc/1024/1024)
			fmt.Printf("\tSys = %v MiB", m.Sys/1024/1024)
			fmt.Printf("\tNumGC = %v\n", m.NumGC)
		}
	}
}

/*
TODO - нужно считывать больше данных и сортировать их уже в ram!
TODO - поиграться с кодировками
*/

func main() {
	// start goroutime with memory tracker
	go TrackMemoryUsage()
	files, err := os.ReadDir("test_data")
	if err != nil {
		panic(err)
	}
	workerPool = int(math.Min(float64(len(files)), 50))
	toProcess := make(chan string, workerPool)
	resultChan := make(chan string, workerPool)
	defer close(resultChan)
	err = os.Mkdir("tmp", os.ModePerm)
	if err != nil {
		fmt.Errorf("Can't create folder for temporary sorted files: %v", err)
	}
	// Setup workers to parse and sort files
	for i := 0; i < workerPool; i++ {
		go worker.SortInitialFiles(toProcess, i+1, resultChan)
	}
	fmt.Println("Start sorting files")
	startTime := time.Now()
	go func() {
		for _, file := range files {
			toProcess <- file.Name()
		}
		defer close(toProcess)
	}()
	for i := 0; i < len(files); i++ {
		<-resultChan
	}
	fmt.Printf("Done sorting, took %s. now going to merge files\n", time.Now().Sub(startTime))
	startTime = time.Now()
	bfs_walker.MergeAllFiles("tmp")
	fmt.Printf("Done merging, took %s! Take a look at results!\n", time.Now().Sub(startTime))
}
