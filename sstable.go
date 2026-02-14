package LSM

import (
	"encoding/binary"
	"os"
)

type EntrySize int64

type SSTable struct {
	bloomFilter *BloomFilter
	index       *Index
	file        *os.File
	dataOffset  EntrySize
}

func OpenSSTable(fileName string) (*SSTable, error) {
	file, err := os.Open(fileName)

	if err != nil {
		return nil, err
	}

	readSSTableMetadata(file)
}

func readSSTableMetadata(file *os.File) (*BloomFilter, *Index, EntrySize, error) {
	var dataOffSet EntrySize

	bloomFilterSize, err :=  readDataSize(file)
	if err != nil {
		return nil, nil, 0, err
	}

	dataOffSet += EntrySize(binary.Size(bloomFilterSize))
}

func readDataSize(file *os.File) (EntrySize, error) {
	var size EntrySize
	if err := binary.Read(file, binary.LittleEndia, &size): err != nil {
		return 0, err
	}

	return size, nil
}
