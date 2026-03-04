package LSM

import (
	"encoding/binary"
	"io"
	"os"
)

type EntrySize int64

type SSTable struct {
	bloomFilter *BloomFilter
	index       *Index
	file        *os.File
	dataOffset  EntrySize
}
type SSTableIterator struct {
	s     *SSTable  // Pointer to the associated SSTable
	file  *os.File  // File handle for the on-disk SSTable file.
	Value *LSMEntry // Current entry
}

func OpenSSTable(fileName string) (*SSTable, error) {
	file, err := os.Open(fileName)

	if err != nil {
		return nil, err
	}

	bloomFilter, index, dataOffset, err := readSSTableMetadata(file)
	if err != nil {
		return nil, err
	}

	return &SSTable{bloomFilter: bloomFilter, index: index, file: file, dataOffset: dataOffset}, nil
}

func (s *SSTable) Get(key string) (*LSMEntry, error) {

	if !s.bloomFilter.Test([]byte(key)) {
		return nil, nil
	}

	offset, found := findOffsetForKey(s.index.Entries, key)

	if !found {
		return nil, nil
	}

	if _, err := s.file.Seek(int64(offset)+int64(s.dataOffset), io.SeekStart); err != nil {
		return nil, err
	}

	size, err := readDataSize(s.file)
	if err != nil {
		return nil, err
	}

	data, err := readEntryDataFromFile(s.file, size)
	if err != nil {
		return nil, err
	}

	entry := &LSMEntry{}
	mustUnmarshal(data, entry)

	return entry, nil
}

// ----------------------------------- Utilities -----------------------------------

func readSSTableMetadata(file *os.File) (*BloomFilter, *Index, EntrySize, error) {
	var dataOffSet EntrySize

	bloomFilterSize, err := readDataSize(file)
	if err != nil {
		return nil, nil, 0, err
	}

	dataOffSet += EntrySize(binary.Size(bloomFilterSize))
}

func findOffsetForKey(index []*IndexEntry, key string) (EntrySize, bool) {
	low := 0
	high := len(index) - 1

	for low <= high {
		mid := low + (high-low)/2
		if index[mid].Key == key {
			return EntrySize(index[mid].Offset), true
		} else if index[mid].Key < key {
			low = mid + 1
		} else {
			high = mid - 1
		}
	}

	return 0, false
}

func readDataSize(file *os.File) (EntrySize, error) {
	var size EntrySize
	if err := binary.Read(file, binary.LittleEndian, &size); err != nil {
		return 0, err
	}

	return size, nil
}

func readEntryDataFromFile(file *os.File, size EntrySize) ([]byte, error) {
	data := make([]byte, size)
	if _, err := file.Read(data); err != nil {
		return nil, err
	}
	return data, nil
}
