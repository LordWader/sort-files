package file_merger

import (
	"container/heap"
	"files_sorter/file_processors"
	"files_sorter/utils"
	"fmt"
	"math/rand"
	"os"
	"strconv"
)

type Sorter struct {
	chunkNum int
	pq       *utils.IntHeap
}

func NewSorter() *Sorter {
	pq := &utils.IntHeap{}
	heap.Init(pq)
	return &Sorter{
		chunkNum: 0,
		pq:       pq,
	}
}

func (s *Sorter) SortInitialFiles(jobs <-chan string, remainderChan chan<- bool) {
	prefix := strconv.Itoa(rand.Int())
	defer func() {
		if s.pq.Len() > 0 {
			fileWriter := file_processors.NewFileWriter(fmt.Sprintf("tmp/%s_chunk_%d.txt", prefix, s.chunkNum))
			fileWriter.WriteToBuffer(s.pq)
			remainderChan <- true
		} else {
			remainderChan <- true
		}
	}()
	for job := range jobs {
		fileReader := file_processors.NewFileReader(fmt.Sprintf("test_data/%s", job))
		for {
			num, err := fileReader.GetNextNum()
			if err != nil {
				break
			}
			heap.Push(s.pq, num)
			if s.pq.Len() > 3_000_000 {
				fileWriter := file_processors.NewFileWriter(fmt.Sprintf("tmp/%s_chunk_%d.txt", prefix, s.chunkNum))
				fileWriter.WriteToBuffer(s.pq)
				s.pq = &utils.IntHeap{}
				heap.Init(s.pq)
				s.chunkNum++
			}
		}
	}
}

type KFiles struct {
	Files        []string
	FilePointers map[string]*file_processors.FileReader
}

func NewKFiles() *KFiles {
	return &KFiles{
		Files:        make([]string, 0),
		FilePointers: make(map[string]*file_processors.FileReader),
	}
}

/*
Здесь нужно мержить и записывать сразу в файл! Убрать очередь и порефачить
*/
func (kf *KFiles) MergeKFiles(toProcess <-chan KFiles, resultChan chan<- string) {
	for tf := range toProcess {
		first := tf.Files[0]
		pq := &utils.PriorityQueue{}
		heap.Init(pq)
		for _, file := range tf.Files {
			kf.FilePointers[file] = file_processors.NewFileReader(fmt.Sprintf("tmp/%s", file))
			num, err := kf.FilePointers[file].GetNextNum()
			if err != nil {
				panic(err)
			}
			heap.Push(pq, &utils.NumFromFile{
				FileName: file,
				Num:      num,
			})
		}
		// create tmp file for merged data
		newFileName := fmt.Sprintf("tmp/tmp_%s", first)
		fileWriter := file_processors.NewFileWriter(newFileName)
		for pq.Len() > 0 {
			topHeap := heap.Pop(pq).(*utils.NumFromFile)
			fileWriter.AppendToBuffer(topHeap.Num)
			nextNum, err := kf.FilePointers[topHeap.FileName].GetNextNum()
			if err != nil {
				continue
			}
			heap.Push(pq, &utils.NumFromFile{
				FileName: topHeap.FileName,
				Num:      nextNum,
			})
		}
		// write from buffer to file
		fileWriter.WriteToFile()
		fileWriter.File.Close()
		// clean tmp dir from merged files
		for _, file := range tf.Files {
			err := os.Remove(fmt.Sprintf("tmp/%s", file))
			if err != nil {
				panic(err)
			}
		}
		err := os.Rename(newFileName, fmt.Sprintf("tmp/%s", first))
		if err != nil {
			fmt.Printf("Error in renaming file: %v", err)
		}
		resultChan <- first
	}
}
