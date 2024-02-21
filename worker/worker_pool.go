package worker

import (
	"container/heap"
	"files_sorter/file_processors"
	"files_sorter/utils"
	"fmt"
	"os"
	"strings"
)

type TwoFiles struct {
	First, Second string
}

func SortInitialFiles(jobs <-chan string, fileId int, result chan<- string) {
	for job := range jobs {
		pq := &utils.IntHeap{}
		heap.Init(pq)
		chunkNum := 0
		fileReader := file_processors.NewFileReader(fmt.Sprintf("test_data/%s", job))
		fileNum := strings.Split(job, ".")[0]
		fileWriter := file_processors.NewFileWriter(fmt.Sprintf("tmp/tmp_%s_chunk_%d.txt", fileNum, chunkNum))
		for {
			num, err := fileReader.GetNextNum()
			if err != nil {
				break
			}
			heap.Push(pq, num)
			// separate data in chunks
			// TODO - profile, maybe we can make larger chunks
			if pq.Len() > 700000 {
				fileWriter.WriteToBuffer(pq)
				pq := &utils.IntHeap{}
				heap.Init(pq)
				chunkNum++
				fileWriter = file_processors.NewFileWriter(fmt.Sprintf("tmp/tmp_%s_chunk_%d.txt", fileNum, chunkNum))
				// result <- fmt.Sprintf("%d_chunk_%d", fileId, chunkNum-1)
			}
		}
		fileWriter.WriteToBuffer(pq)
		result <- job
	}
}

/*
Здесь нужно мержить и записывать сразу в файл! Убрать очередь и порефачить
*/
func MergeTwoFiles(toProcess <-chan TwoFiles, resultChan chan<- string) {
	for tf := range toProcess {
		first := tf.First
		second := tf.Second
		fr1 := file_processors.NewFileReader(fmt.Sprintf("tmp/%s", first))
		fr2 := file_processors.NewFileReader(fmt.Sprintf("tmp/%s", second))
		// create tmp file for merged data
		newFileName := fmt.Sprintf("tmp/tmp_%s", first)
		fileWriter := file_processors.NewFileWriter(newFileName)
		num1, err := fr1.GetNextNum()
		if err != nil {
			panic(err)
		}
		num2, err := fr2.GetNextNum()
		if err != nil {
			panic(err)
		}
		var stop1, stop2 bool
		for {
			if stop1 && stop2 {
				break
			}
			for (num1 <= num2 || stop2) && !stop1 {
				fileWriter.AppendToBuffer(num1)
				num1, err = fr1.GetNextNum()
				if err != nil {
					stop1 = true
					break
				}
			}
			for (num2 <= num1 || stop1) && !stop2 {
				fileWriter.AppendToBuffer(num2)
				num2, err = fr2.GetNextNum()
				if err != nil {
					stop2 = true
					break
				}
			}
		}
		// write from buffer to file
		fileWriter.WriteToFile()
		fileWriter.File.Close()
		// clean tmp dir from merged files
		err = os.Remove(fmt.Sprintf("tmp/%s", first))
		if err != nil {
			panic(err)
		}
		err = os.Remove(fmt.Sprintf("tmp/%s", second))
		if err != nil {
			panic(err)
		}
		err = os.Rename(newFileName, fmt.Sprintf("tmp/%s", first))
		if err != nil {
			fmt.Printf("Error in renaming file: %v", err)
		}
		resultChan <- first
	}
}
