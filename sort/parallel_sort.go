package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"os"
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

		toBeSorted = mergeSort(toBeSorted)

		for j := 1; j < len(toBeSorted); j++ {
			if toBeSorted[j-1] > toBeSorted[j] {
				println(toBeSorted[j-1], toBeSorted[j], j)
				log.Fatal("Failed to sort")
				return
			}
		}

		end := time.Now()
		elapsed := end.Sub(roundStart)

		log.Println("Elapsed time: ", elapsed/1000)
		return
		// writeSorted(toBeSorted, i)
	}

	fileMerge(totalFileSize/maxRAMUsage, maxRAMUsage)

	end := time.Now()
	elapsed := end.Sub(start)

	log.Println("Total time: ")
	log.Println(elapsed / 1000)
}

func mergeSort(s []uint64) []uint64 {
	if len(s) <= 1 {
		return s
	}

	midPoint := len(s) / 2

	left := mergeSort(s[:midPoint])
	right := mergeSort(s[midPoint:])

	s = nil

	return merge(left, right)
}

func merge(left, right []uint64) []uint64 {
	length := len(left) + len(right)
	sorted := make([]uint64, length, length)
	i, j, k := 0, 0, 0
	for i < len(left) && j < len(left) {
		if right[j] >= left[i] {
			sorted[k] = left[i]
			i++
		} else {
			sorted[k] = right[j]
			j++
		}
		k++
	}

	for i < len(left) {
		sorted[k] = left[i]
		i++
		k++
	}

	for j < len(right) {
		sorted[k] = right[j]
		j++
		k++
	}

	left = nil
	right = nil

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
