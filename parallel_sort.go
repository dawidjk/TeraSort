package terasort

import (
	"log"
	"os"
)

func main() {
	oneGig := 134217728
	totalFileSize := oneGig * 8
	totalMemoryGb := 4
	readSize := oneGig

	fileName := "/Users/dave07747/Development/Terabyte-Sort/eightGB.bin"

	var _, err = os.Stat(fileName)

	if os.IsNotExist(err) {
		log.Fatal("File does not exist")
		return
	}

	file, err := os.OpenFile(fileName, os.O_RDONLY, os.ModeAppend)

}

func readNextSector(file *os.File, number length) {
	bytes := make([]byte, number)
	_, err := file.Read(bytes)

	if err != nil {
		log.Fatal(err)
	}

	return bytes
}
