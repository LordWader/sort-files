package file_processors

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"sync"
)

var (
	bytesPool = sync.Pool{New: func() any {
		buf := make([]byte, 4096)
		return buf
	},
	}
)

type FileReader struct {
	file    *os.File
	scanner *bufio.Scanner
	buf     []byte
}

func NewFileReader(pathToFile string) *FileReader {
	f, err := os.Open(pathToFile)
	if err != nil {
		panic(err)
	}
	buf := bytesPool.Get().([]byte)

	scanner := bufio.NewScanner(f)
	scanner.Buffer(buf, cap(buf))
	return &FileReader{
		file:    f,
		scanner: bufio.NewScanner(f),
		buf:     buf,
	}
}

func (fr *FileReader) CanScan() bool {
	canScan := fr.scanner.Scan()
	if !canScan {
		bytesPool.Put(fr.buf)
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
