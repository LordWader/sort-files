package main

import (
	"fmt"
	"math/rand"
	"os"
	"strconv"
)

func prepare() {
	var (
		f   *os.File
		num int
		err error
	)
	defer f.Close()
	err = os.Mkdir("test_data", os.ModePerm)
	if err != nil {
		fmt.Errorf("Can't create folder for test data: %v", err)
	}
	for i := 0; i < 1000000; i++ {
		if i%10 == 0 {
			if i > 0 {
				err := f.Close()
				if err != nil {
					fmt.Printf("error during closing file: %v", err)
				}
			}
			f, err = os.Create(fmt.Sprintf("test_data/%d.txt", num+1))
			if err != nil {
				panic(err)
			}
			num++
		}
		_, err = f.Write([]byte(strconv.Itoa(rand.Int()) + "\n"))
		if err != nil {
			fmt.Printf("Cant write integer to file: %v", err)
		}
	}
}
