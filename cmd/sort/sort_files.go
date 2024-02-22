package main

import (
	"files_sorter/file_merger"
	"fmt"
	"math"
	_ "net/http/pprof"
	"os"
	"runtime"
	"sync"
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
TODO - Кажется, что можно больше считывать файлов и их мержить, с тем чтобы уменьшить их начальное кол-во
TODO - поиграться с кодировками
*/

func main() {
	// start goroutime with memory tracker
	go TrackMemoryUsage()
	files, err := os.ReadDir("test_data")
	if err != nil {
		panic(err)
	}
	workerPool = int(math.Min(float64(len(files)), 20))
	toProcess := make(chan string, workerPool)
	resultChan := make(chan string, workerPool)
	nums := make(chan int)
	defer close(resultChan)
	err = os.Mkdir("tmp", os.ModePerm)
	if err != nil {
		fmt.Errorf("Can't create folder for temporary sorted files: %v", err)
	}
	// Setup workers to parse and sort files
	var wg sync.WaitGroup
	sorter := file_merger.NewSorter(nums)
	wg.Add(1)
	go sorter.ProcessNumbers(&wg)
	for i := 0; i < workerPool; i++ {
		go file_merger.SortInitialFiles(toProcess, sorter, resultChan)
	}
	fmt.Println("Start sorting files")
	startTime := time.Now()
	go func() {
		for _, file := range files {
			toProcess <- file.Name()
		}
		defer close(toProcess)
	}()
	go func() {
		for i := 0; i < len(files); i++ {
			<-resultChan
		}
		defer close(nums)
	}()
	wg.Wait()
	fmt.Printf("Done sorting, took %s. now going to merge files\n", time.Now().Sub(startTime))
	startTime = time.Now()
	file_merger.MergeAllFiles("tmp")
	fmt.Printf("Done merging, took %s! Take a look at results!\n", time.Now().Sub(startTime))
}
