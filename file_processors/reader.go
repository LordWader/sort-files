package file_processors

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
)

type FileReader struct {
	file    *os.File
	scanner *bufio.Scanner
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
		return -1, fmt.Errorf("can't get new numbers from File")
	}
	txt := string(fr.scanner.Bytes())
	i, err := strconv.Atoi(txt)
	if err != nil {
		panic(err)
	}
	return i, nil
}
