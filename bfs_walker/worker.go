package bfs_walker

import (
	"files_sorter/worker"
	"math"
	"os"
)

const WORKER_POOL int = 50
const K_FILE_MERGE_PARAM int = 50

func MergeAllFiles(filesDir string) {
	toProcess := make(chan worker.KFiles, WORKER_POOL)
	resultChan := make(chan string, WORKER_POOL)
	// start pool of process
	for i := 0; i < WORKER_POOL; i++ {
		processor := worker.NewKFiles()
		go processor.MergeKFiles(toProcess, resultChan)
	}
	deque := make([]string, 0)
	files, err := os.ReadDir("tmp")
	if err != nil {
		panic(err)
	}
	for _, file := range files {
		deque = append(deque, file.Name())
	}
	// TODO - протестировать вариант с большим кол-вом одновременно сжимаемых файлов (bfs)
	for len(deque) > 1 {
		chunkSize := int(math.Min(float64(K_FILE_MERGE_PARAM), float64(len(deque))))
		toProcess <- worker.KFiles{
			Files: deque[:chunkSize],
		}
		deque = deque[chunkSize:]
		newFile := <-resultChan
		deque = append(deque, newFile)
	}
}
