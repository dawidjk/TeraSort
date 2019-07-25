package main

import (
	"bytes"
	"encoding/binary"
	"log"
	"math/rand"
	"os"
	"time"
)

func main() {
	fileSize := 1073741824

	seed := int64(29)
	writeSize := 1000000

	var _, err = os.Stat(fileName)

	// create file if not exists
	if os.IsNotExist(err) {
		var file, err = os.Create(fileName)

		if err != nil {
			return
		}

		defer file.Close()
	}

	file, err := os.OpenFile(fileName, os.O_WRONLY, os.ModeAppend)
	defer file.Close()

	if err != nil {
		log.Fatal(err)
	}

	random := rand.New(rand.NewSource(seed))

	start := time.Now()

	for written := 0; written < fileSize; written += writeSize {
		bitsToBeWritten := writeSize

		if written+writeSize > fileSize {
			bitsToBeWritten = fileSize - written
		}

		var binaryBuffer bytes.Buffer
		for i := 0; i < bitsToBeWritten; i++ {
			binary.Write(&binaryBuffer, binary.BigEndian, random.Uint64())
		}

		_, err := file.Write(binaryBuffer.Bytes())

		if err != nil {
			log.Fatal(err)
		}
	}

	end := time.Now()
	elapsed := end.Sub(start)

	log.Println("Total time: ")
	log.Println(elapsed / 1000)
}
