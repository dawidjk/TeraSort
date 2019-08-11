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

	oneGig := 1073741824
	maxRAMUsage := oneGig
	bytesInInt64 := 8
	totalFileSize := oneGig * 8

	fileName := "/Users/dave07747/Development/Terabyte-Sort/eightGB.bin"

	var _, err = os.Stat(fileName)

	if os.IsNotExist(err) {
		log.Fatal("File does not exist")
		return
	}

	file, err := os.OpenFile(fileName, os.O_RDONLY, os.ModeAppend)

	uint64count := maxRAMUsage / bytesInInt64

	for i := 0; i < totalFileSize/maxRAMUsage; i++ {
		toBeSorted := make([]uint64, uint64count)
		readBytes := readNextSector(file, maxRAMUsage)

		for j := 0; j < uint64count; j++ {
			lowerBoundary := 8 * j
			upperBoundary := 8 * (j + 1)
			toBeSorted[j] = bytesToInt64(readBytes[lowerBoundary:upperBoundary])
		}
		println("Starting sorting")
		roundStart := time.Now()

		mergeSortConcurrent(toBeSorted)

		end := time.Now()
		elapsed := end.Sub(roundStart)

		log.Println("Elapsed time: ")
		log.Println(elapsed / 1000)

		writeSorted(toBeSorted, i)
	}

	// fileMerge(totalFileSize/maxRAMUsage, maxRAMUsage)

	end := time.Now()
	elapsed := end.Sub(start)

	log.Println("Total time: ")
	log.Println(elapsed / 1000)
}

var mergeSortWorkers = make(chan struct{}, 8)

func mergeSortConcurrent(s []uint64) []uint64 {
	if len(s) <= 1 {
		return s
	}

	wg := sync.WaitGroup{}
	wg.Add(2)

	midPoint := len(s) / 2

	var left []uint64
	var right []uint64

	select {
	case mergeSortWorkers <- struct{}{}:
		go func() {
			left = mergeSortConcurrent(s[:midPoint])
			<-mergeSortWorkers
			wg.Done()
		}()
	default:
		left = mergeSort(s[:midPoint])
		wg.Done()
	}

	select {
	case mergeSortWorkers <- struct{}{}:
		go func() {
			right = mergeSortConcurrent(s[midPoint:])
			<-mergeSortWorkers
			wg.Done()
		}()

	default:
		right = mergeSort(s[midPoint:])
		wg.Done()
	}

	wg.Wait()
	return merge(left, right)
}

func mergeSort(s []uint64) []uint64 {
	if len(s) <= 1 {
		return s
	}

	midPoint := len(s) / 2

	var left []uint64
	var right []uint64

	left = mergeSort(s[:midPoint])
	right = mergeSort(s[midPoint:])

	return merge(left, right)
}

func merge(left, right []uint64) []uint64 {
	sorted := make([]uint64, 0, len(left)+len(right))

	for len(right) > 0 || len(left) > 0 {
		if len(right) == 0 {
			return append(sorted, left...)
		}

		if len(left) == 0 {
			return append(sorted, right...)
		}

		if right[0] >= left[0] {
			sorted = append(sorted, left[0])
			left = left[1:]
		} else {
			sorted = append(sorted, right[0])
			right = right[1:]
		}
	}

	return sorted
}

// func fileMerge(fileCount int, maxRAMUsage int) {
// 	filesToBeMerged := fileCount
// 	nextFileName := fileCount
// 	currentFileName := 0

// 	for filesToBeMerged != 1 {

// 	}
// }

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
