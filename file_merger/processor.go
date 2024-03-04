package file_merger

import (
	"fmt"
	"math"
	"os"
	"runtime"
)

// const WORKER_POOL int = 20
const K_FILE_MERGE_PARAM int = 30

func MergeAllFiles() {
	WORKER_POOL := runtime.NumCPU()
	toProcess := make(chan KFiles, WORKER_POOL)
	resultChan := make(chan string, WORKER_POOL)
	// start pool of process
	for i := 0; i < WORKER_POOL; i++ {
		processor := NewKFiles()
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
		toProcess <- KFiles{
			Files: deque[:chunkSize],
		}
		deque = deque[chunkSize:]
		newFile := <-resultChan
		deque = append(deque, newFile)
	}
	// Rename final results file
	oldName := "tmp/" + deque[0]
	err = os.Rename(oldName, fmt.Sprintf("tmp/%s", "res.txt"))
	if err != nil {
		fmt.Printf("Error in renaming file: %v", err)
	}
}
