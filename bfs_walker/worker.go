package bfs_walker

import (
	"files_sorter/worker"
	"os"
)

const WORKER_POOL int = 200

func MergeAllFiles(filesDir string) {
	toProcess := make(chan worker.TwoFiles, WORKER_POOL)
	resultChan := make(chan string, WORKER_POOL)
	// start pool of process
	for i := 0; i < WORKER_POOL; i++ {
		go worker.MergeTwoFiles(toProcess, resultChan)
	}
	deque := make([]string, 0)
	files, err := os.ReadDir("tmp")
	if err != nil {
		panic(err)
	}
	for _, file := range files {
		deque = append(deque, file.Name())
	}
	i := 0
	// TODO - протестировать вариант с большим кол-вом одновременно сжимаемых файлов (bfs)
	for len(deque) > 1 {
		toProcess <- worker.TwoFiles{
			First:  deque[i],
			Second: deque[i+1],
		}
		deque = deque[2:]
		newFile := <-resultChan
		deque = append(deque, newFile)
	}
}
