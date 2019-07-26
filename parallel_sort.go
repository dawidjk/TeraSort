package terasort

import (
	"encoding/binary"
	"log"
	"os"
	"sync"
)

func main() {
	oneGig := 134217728
	totalFileSize := oneGig * 8
	totalMemoryGb := 4
	readSize := oneGig
	bytesInInt64 := 8

	fileName := "/Users/dave07747/Development/Terabyte-Sort/eightGB.bin"

	var _, err = os.Stat(fileName)

	if os.IsNotExist(err) {
		log.Fatal("File does not exist")
		return
	}

	file, err := os.OpenFile(fileName, os.O_RDONLY, os.ModeAppend)
	readBytes := readNextSector(file, readSize)

	uint64count := readSize / bytesInInt64
	toBeSorted := make([]uint64, uint64count)

	var wg sync.WaitGroup
	wg.Add(uint64count)

	for i := 0; i < uint64count; i++ {
		go func(i int) {
			lowerBoundary := 8 * i
			upperBoundary := 8 * (i + 1)
			toBeSorted[i] = bytesToInt64(readBytes[lowerBoundary:upperBoundary])
		}(i)
	}
}

func bytesToInt64(data []byte) uint64 {
	return binary.BigEndian.Uint64(data)
}

func readNextSector(file *os.File, number int) []byte {
	bytes := make([]byte, number)
	_, err := file.Read(bytes)

	if err != nil {
		log.Fatal(err)
	}

	return bytes
}
