package main

import (
	"files_sorter/bfs_walker"
	"files_sorter/worker"
	"fmt"
	"os"
)

const WORKER_POOL int = 200

func main() {
	// debug.SetMemoryLimit(40 / 1000000 * 1 << 20) // 40 MB
	toProcess := make(chan string, WORKER_POOL)
	resultChan := make(chan string, WORKER_POOL)
	err := os.Mkdir("tmp", os.ModePerm)
	if err != nil {
		fmt.Errorf("Can't create folder for temporary sorted files: %v", err)
	}
	// Setup workers to parse and sort files
	for i := 0; i < WORKER_POOL; i++ {
		go worker.SortInitialFiles(toProcess, i+1, resultChan)
	}
	files, err := os.ReadDir("test_data")
	if err != nil {
		panic(err)
	}
	go func() {
		for _, file := range files {
			toProcess <- file.Name()
		}
		defer close(toProcess)
	}()
	for i := 0; i < len(files); i++ {
		<-resultChan
	}
	//fmt.Println("Done sorting, now going to merge files")
	bfs_walker.MergeAllFiles("tmp")
	//fmt.Println("Done merging! Take a look at result")
}
