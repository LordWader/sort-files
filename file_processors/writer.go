package file_processors

import (
	"bytes"
	"container/heap"
	"files_sorter/utils"
	"fmt"
	"os"
	"strconv"
)

type FileWriter struct {
	buffer bytes.Buffer
	File   *os.File
}

func NewFileWriter(filePath string) *FileWriter {
	file, err := os.Create(filePath)
	if err != nil {
		panic(err)
	}
	bf := bytes.Buffer{}
	return &FileWriter{
		buffer: bf,
		File:   file,
	}
}

// WriteToBuffer - used for sorting large files and split them into smaller chunks/*
func (fw *FileWriter) WriteToBuffer(pq *utils.IntHeap) {
	for pq.Len() > 0 {
		fw.buffer.WriteString(strconv.Itoa(heap.Pop(pq).(int)))
		fw.buffer.WriteRune('\n')
	}
	fw.WriteToFile()
	fw.File.Close()
}

func (fw *FileWriter) WriteToFile() {
	_, err := fw.buffer.WriteTo(fw.File)
	if err != nil {
		fmt.Printf("Cant write integer to File: %v", err)
	}
	fw.buffer = bytes.Buffer{}
}

func (fw *FileWriter) AppendToBuffer(num int) {
	fw.buffer.WriteString(strconv.Itoa(num))
	fw.buffer.WriteRune('\n')
	if fw.buffer.Len() > 100000 {
		fw.WriteToFile()
	}
}
