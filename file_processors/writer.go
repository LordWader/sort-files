package file_processors

import (
	"bytes"
	"container/heap"
	"files_sorter/utils"
	"fmt"
	"os"
	"strconv"
	"sync"
)

var Pool = sync.Pool{
	New: func() interface{} {
		b := &bytes.Buffer{}
		b.Grow(5_000_000)
		return b
	},
}

type FileWriter struct {
	Buffer *bytes.Buffer
	File   *os.File
}

func NewFileWriter(filePath string) *FileWriter {
	file, err := os.Create(filePath)
	if err != nil {
		panic(err)
	}
	return &FileWriter{
		Buffer: Pool.Get().(*bytes.Buffer),
		File:   file,
	}
}

// WriteToBuffer - used for sorting large files and split them into smaller chunks/*
func (fw *FileWriter) WriteToBuffer(pq *utils.IntHeap) {
	for pq.Len() > 0 {
		fw.Buffer.WriteString(strconv.Itoa(heap.Pop(pq).(int)))
		fw.Buffer.WriteRune('\n')
	}
	fw.WriteToFile()
	err := fw.File.Close()
	if err != nil {
		fmt.Errorf("can't close file: %w", err)
	}
	Pool.Put(fw.Buffer)
}

func (fw *FileWriter) WriteToFile() {
	_, err := fw.Buffer.WriteTo(fw.File)
	if err != nil {
		fmt.Printf("Cant write integer to File: %v", err)
	}
	fw.Buffer.Reset()
}

func (fw *FileWriter) AppendToBuffer(num int) {
	fw.Buffer.WriteString(strconv.Itoa(num))
	fw.Buffer.WriteRune('\n')
	if fw.Buffer.Len() > 100000 {
		fw.WriteToFile()
	}
}
