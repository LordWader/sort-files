package worker

import (
	"bufio"
	"container/heap"
	"files_sorter/utils"
	"fmt"
	"os"
	"strconv"
)

type TwoFiles struct {
	First, Second string
}

type FileReader struct {
	file    *os.File
	scanner *bufio.Scanner
}

type NumFromFile struct {
	fileNum, num int
}

func NewFileReader(pathToFile string) *FileReader {
	f, err := os.Open(pathToFile)
	if err != nil {
		panic(err)
	}
	return &FileReader{
		file:    f,
		scanner: bufio.NewScanner(f),
	}
}

func (fr *FileReader) CanScan() bool {
	canScan := fr.scanner.Scan()
	if !canScan {
		return false
	}
	return true
}

func (fr *FileReader) GetNextNum() (int, error) {
	if !fr.CanScan() {
		fr.file.Close()
		return -1, fmt.Errorf("can't get new numbers from file")
	}
	txt := fr.scanner.Text()
	i, err := strconv.Atoi(txt)
	if err != nil {
		panic(err)
	}
	return i, nil
}

func WriteDataToFile(pq *utils.IntHeap, fileName string) {
	file, err := os.Create(fmt.Sprintf("tmp/%s", fileName))
	defer file.Close()
	if err != nil {
		panic(err)
	}
	for pq.Len() > 0 {
		_, err = file.Write([]byte(strconv.Itoa(heap.Pop(pq).(int)) + "\n"))
		if err != nil {
			fmt.Printf("Cant write integer to file: %v", err)
		}
	}
}

func WriteNumToFile(file *os.File, num int) {
	_, err := file.Write([]byte(strconv.Itoa(num) + "\n"))
	if err != nil {
		fmt.Printf("Cant write integer to file: %v", err)
	}
}

func SortInitialFiles(jobs <-chan string, fileId int, result chan<- string) {
	for job := range jobs {
		pq := &utils.IntHeap{}
		heap.Init(pq)
		fr := NewFileReader(fmt.Sprintf("test_data/%d.txt", fileId))
		for {
			num, err := fr.GetNextNum()
			if err != nil {
				break
			}
			heap.Push(pq, num)
		}
		WriteDataToFile(pq, fmt.Sprintf("tmp_%s", job))
		result <- job
	}
}

//type FileWriter struct {
//	buffer []byte
//	file   *os.File
//}
//
//func NewFileWriter(file *os.File) *FileWriter {
//	return &FileWriter{
//		buffer: make([]byte, 0),
//		file:   file,
//	}
//}
//
//func (fw *FileWriter) WriteToFile() {
//	_, err := fw.file.Write(fw.buffer)
//	if err != nil {
//		fmt.Printf("Cant write integer to file: %v", err)
//	}
//	fw.buffer = make([]byte, 0)
//}
//
//func (fw *FileWriter) AppendToBuffer(num int) {
//	fw.buffer = append(fw.buffer, []byte(strconv.Itoa(num)+"\n")...)
//	if len(fw.buffer) > 100 {
//		fw.WriteToFile()
//	}
//}

/*
ЗДесь нужно мержить и записывать сразу в файл! Убрать очередь и порефачить
*/
func MergeTwoFiles(toProcess <-chan TwoFiles, resultChan chan<- string) {
	for tf := range toProcess {
		first := tf.First
		second := tf.Second
		fr1 := NewFileReader(fmt.Sprintf("tmp/%s", first))
		fr2 := NewFileReader(fmt.Sprintf("tmp/%s", second))
		// create tmp file for merged data
		newFileName := fmt.Sprintf("tmp/tmp_%s", first)
		file, err := os.Create(newFileName)
		defer file.Close()
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
				WriteNumToFile(file, num1)
				num1, err = fr1.GetNextNum()
				if err != nil {
					stop1 = true
					break
				}
			}
			for (num2 <= num1 || stop1) && !stop2 {
				WriteNumToFile(file, num2)
				num2, err = fr2.GetNextNum()
				if err != nil {
					stop2 = true
					break
				}
			}
		}
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
