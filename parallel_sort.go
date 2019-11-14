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

	fileMerge(totalFileSize/maxRAMUsage, maxRAMUsage)

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

func fileMerge(fileCount int, maxRAMUsage int) {
	bytesInInt64 := 8
	filesReadConcurrent := 2
	filesToBeMerged := fileCount
	nextFileName := fileCount
	currentFileName := 0
	fileTemplate := "/Users/dave07747/Development/Terabyte-Sort/%d.bin"

	for filesToBeMerged != 1 {
		println(fmt.Sprintf("Writing file: %d", nextFileName))
		file1, err1 := os.Open(fmt.Sprintf(fileTemplate, currentFileName))
		file2, err2 := os.Open(fmt.Sprintf(fileTemplate, currentFileName+1))
		fi1, _ := file1.Stat()
		fi2, _ := file2.Stat()

		if err1 != nil || err2 != nil || fi1.Size() != fi2.Size() {
			log.Fatal(err1)
		}

		fileSectorCount := int(fi1.Size() / int64(maxRAMUsage/filesReadConcurrent))
		fileSectorLength := int(fi1.Size() / int64(fileSectorCount))
		println(fileSectorCount)
		println(int64(fileSectorLength))

		for i := 0; i < int(fileSectorCount); i++ {
			toBeMerged1 := make([]uint64, fileSectorLength/bytesInInt64)
			readBytes := readNextSector(file1, fileSectorLength/bytesInInt64)

			// Can optimize space complexity of this by truncating readBytes every time read
			for j := 0; j < (fileSectorLength/bytesInInt64)/bytesInInt64; j++ {
				lowerBoundary := bytesInInt64 * j
				upperBoundary := bytesInInt64 * (j + 1)
				toBeMerged1[j] = bytesToInt64(readBytes[lowerBoundary:upperBoundary])
			}

			toBeMerged2 := make([]uint64, fileSectorLength/bytesInInt64)
			readBytes = readNextSector(file1, fileSectorLength/bytesInInt64)

			for j := 0; j < (fileSectorLength/bytesInInt64)/bytesInInt64; j++ {
				lowerBoundary := bytesInInt64 * j
				upperBoundary := bytesInInt64 * (j + 1)
				toBeMerged2[j] = bytesToInt64(readBytes[lowerBoundary:upperBoundary])
			}

			println("Merging")
			merged := merge(toBeMerged1, toBeMerged2)
			println("Saving Merged")
			writeSorted(merged, nextFileName)
		}

		nextFileName++
		filesToBeMerged--
		os.Remove(fmt.Sprintf(fileTemplate, currentFileName))
		os.Remove(fmt.Sprintf(fileTemplate, currentFileName+1))
		currentFileName += 2
	}
}

func writeSorted(sorted []uint64, index int) {
	tempFiles := "/Users/dave07747/Development/Terabyte-Sort/%d.bin"
	writtenFileName := fmt.Sprintf(tempFiles, index)

	var _, err = os.Stat(writtenFileName)

	// create file if not exists
	if os.IsNotExist(err) {
		var file, err = os.Create(writtenFileName)

		if err != nil {
			return
		}

		defer file.Close()
	}

	file, err := os.OpenFile(writtenFileName, os.O_WRONLY, os.ModeAppend)
	defer file.Close()

	if err != nil {
		log.Fatal(err)
	}

	var binaryBuffer bytes.Buffer
	println(len(sorted))
	for i := 0; i < len(sorted); i++ {
		binary.Write(&binaryBuffer, binary.BigEndian, sorted[i])
	}
	println(len(binaryBuffer.Bytes()))
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
