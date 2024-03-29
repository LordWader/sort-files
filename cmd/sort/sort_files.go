package main

import (
	"files_sorter/file_merger"
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
		case <-time.Tick(time.Second * 2):
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
TODO - Сделать ram-cache при записи в файл
TODO - поиграться с кодировками
*/

func main() {
	// start goroutime with memory tracker
	go TrackMemoryUsage()
	files, err := os.ReadDir("test_data")
	if err != nil {
		panic(err)
	}
	workerPool = int(math.Min(float64(len(files)), float64(runtime.NumCPU()-1)))
	toProcess := make(chan string, workerPool)
	remainderChan := make(chan bool, workerPool)
	err = os.Mkdir("tmp", os.ModePerm)
	if err != nil {
		fmt.Errorf("Can't create folder for temporary sorted files: %v", err)
	}
	// Setup workers to parse and sort files
	workers := make([]*file_merger.Sorter, workerPool)
	for i := 0; i < workerPool; i++ {
		workers[i] = file_merger.NewSorter()
		go workers[i].SortInitialFiles(toProcess, remainderChan)
	}
	fmt.Println("Start sorting files")
	startTime := time.Now()
	go func() {
		for _, file := range files {
			toProcess <- file.Name()
		}
		defer close(toProcess)
	}()
	for i := 0; i < workerPool; i++ {
		<-remainderChan
	}
	close(remainderChan)
	fmt.Printf("Done sorting, took %s. now going to merge files\n", time.Now().Sub(startTime))
	startTime = time.Now()
	file_merger.MergeAllFiles()
	fmt.Printf("Done merging, took %s! Take a look at results!\n", time.Now().Sub(startTime))
}
