package file_processors

import (
	"container/heap"
	"files_sorter/utils"
	"fmt"
	"os"
	"strconv"
)

type FileWriter struct {
	buffer []byte
	File   *os.File
}

func NewFileWriter(filePath string) *FileWriter {
	file, err := os.Create(filePath)
	if err != nil {
		panic(err)
	}
	return &FileWriter{
		buffer: make([]byte, 0),
		File:   file,
	}
}

// WriteToBuffer - used for sorting large files and split them into smaller chunks/*
func (fw *FileWriter) WriteToBuffer(pq *utils.IntHeap) {
	for pq.Len() > 0 {
		fw.buffer = append(fw.buffer, []byte(strconv.Itoa(heap.Pop(pq).(int))+"\n")...)
	}
	fw.WriteToFile()
	fw.File.Close()
}

func (fw *FileWriter) WriteToFile() {
	_, err := fw.File.Write(fw.buffer)
	if err != nil {
		fmt.Printf("Cant write integer to File: %v", err)
	}
	fw.buffer = make([]byte, 0)
}

func (fw *FileWriter) AppendToBuffer(num int) {
	fw.buffer = append(fw.buffer, []byte(strconv.Itoa(num)+"\n")...)
	if len(fw.buffer) > 100000 {
		fw.WriteToFile()
	}
}
