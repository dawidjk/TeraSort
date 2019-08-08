package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"os"
	"sync"
	"time"
)

func main() {
	start := time.Now()

	oneGig := 134217728
	readSize := oneGig
	bytesInInt64 := 8
	totalFileSize := oneGig * 8

	fileName := "/Users/dave07747/Development/Terabyte-Sort/eightGB.bin"

	var _, err = os.Stat(fileName)

	if os.IsNotExist(err) {
		log.Fatal("File does not exist")
		return
	}

	file, err := os.OpenFile(fileName, os.O_RDONLY, os.ModeAppend)

	uint64count := readSize / bytesInInt64

	for i := 0; i < totalFileSize/readSize; i++ {
		toBeSorted := make([]uint64, uint64count)
		readBytes := readNextSector(file, readSize)
		// var wg sync.WaitGroup
		// wg.Add(uint64count)

		for j := 0; j < uint64count; j++ {
			// go func(i int) {
			// 	readBytes := readNextSector(file, readSize)
			// 	lowerBoundary := 8 * i
			// 	upperBoundary := 8 * (i + 1)
			// 	toBeSorted[i] = bytesToInt64(readBytes[lowerBoundary:upperBoundary])
			// }(i)
			lowerBoundary := 8 * j
			upperBoundary := 8 * (j + 1)
			toBeSorted[j] = bytesToInt64(readBytes[lowerBoundary:upperBoundary])
		}
		println("Starting sorting")

		parallelMergeSort(toBeSorted)

		writeSorted(toBeSorted, i)
	}

	end := time.Now()
	elapsed := end.Sub(start)

	log.Println("Total time: ")
	log.Println(elapsed / 1000)
}

func writeSorted(sorted []uint64, index int) {
	tempFiles := "/Users/dave07747/Development/Terabyte-Sort/%d.bin"
	writtenFileName := fmt.Sprintf(tempFiles, index)

	var file, err = os.Create(writtenFileName)

	if err != nil {
		return
	}

	defer file.Close()

	if err != nil {
		log.Fatal(err)
	}

	var binaryBuffer bytes.Buffer
	for i := 0; i < len(sorted); i++ {
		binary.Write(&binaryBuffer, binary.BigEndian, sorted[i])
	}

	_, writeErr := file.Write(binaryBuffer.Bytes())

	if writeErr != nil {
		log.Fatal(err)
	}
}

// https://hackernoon.com/parallel-merge-sort-in-go-fe14c1bc006
func parallelMergeSort(toBeSorted []uint64) {
	length := len(toBeSorted)

	if length > 1 {
		midpoint := length / 2
		var wg sync.WaitGroup
		wg.Add(2)

		go func() {
			defer wg.Done()
			parallelMergeSort(toBeSorted[:midpoint])
		}()

		go func() {
			defer wg.Done()
			parallelMergeSort(toBeSorted[midpoint:])
		}()

		wg.Wait()
		merge(toBeSorted, midpoint)
	}
}

func merge(toBeMerged []uint64, middle int) {
	temp := make([]uint64, len(toBeMerged))
	copy(temp, toBeMerged)

	tempLeft := 0
	tempRight := middle
	current := 0
	high := len(toBeMerged) - 1

	for tempLeft <= middle-1 && tempRight <= high {
		if temp[tempLeft] <= temp[tempRight] {
			toBeMerged[current] = temp[tempLeft]
			tempLeft++
		} else {
			toBeMerged[current] = temp[tempRight]
			tempRight++
		}

		current++
	}

	for tempLeft <= middle-1 {
		toBeMerged[current] = temp[tempLeft]
		current++
		tempLeft++
	}
}

func bytesToInt64(data []byte) uint64 {
	return binary.BigEndian.Uint64(data)
}

func readNextSector(file *os.File, number int) []byte {
	bytes := make([]byte, number)
	_, err := file.Read(bytes)

	if err != nil {
		if err == io.EOF {
			return nil
		}

		log.Fatal(err)
	}

	return bytes
}
