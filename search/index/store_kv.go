package index

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"log"
	"os"

	"github.com/edsrzf/mmap-go"
)

type KVStoreWriter struct {
	dataFile    *os.File
	dataWriter  *bufio.Writer
	indexFile   *os.File
	indexWriter *bufio.Writer
	offset      uint64
}

func newKVStoreWriter(basename string) (*KVStoreWriter, error) {
	dataFile, err := os.OpenFile(basename+".data", os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0600)
	if err != nil {
		return nil, err
	}
	indexFile, err := os.OpenFile(basename+".index", os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0600)
	if err != nil {
		dataFile.Close()
		return nil, err
	}

	return &KVStoreWriter{
		dataFile:    dataFile,
		dataWriter:  bufio.NewWriter(dataFile),
		indexFile:   indexFile,
		indexWriter: bufio.NewWriter(indexFile),
		offset:      0,
	}, nil
}

// Caller is responsible to check that keys are inserted in order
func (w *KVStoreWriter) Append(key []byte, values ...[]byte) error {
	keyLength := uint32(len(key))

	var valueLength uint32

	for _, value := range values {
		valueLength += uint32(len(value))
	}

	totalLength := keyLength + valueLength + 8 // 8 bytes for both lengths

	buffer := make([]byte, 0, 8+keyLength+valueLength)
	buffer = binary.BigEndian.AppendUint32(buffer, keyLength)
	buffer = binary.BigEndian.AppendUint32(buffer, valueLength)

	buffer = append(buffer, key...)
	for _, value := range values {
		buffer = append(buffer, value...)
	}

	if _, err := w.dataWriter.Write(buffer); err != nil {
		return err
	}

	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, w.offset)

	if _, err := w.indexWriter.Write(b); err != nil {
		return err
	}

	w.offset += uint64(totalLength)

	return nil
}

func (w *KVStoreWriter) Close() error {
	if err := w.dataWriter.Flush(); err != nil {
		return err
	}

	if err := w.dataFile.Close(); err != nil {
		return err
	}

	if err := w.indexWriter.Flush(); err != nil {
		return err
	}

	if err := w.indexFile.Close(); err != nil {
		return err
	}

	return nil
}

// type cursor struct {
// 	// offset in data file
// 	offset int64

// 	// index of the item
// 	index int64

// 	keyLength   int64
// 	valueLength int64
// 	key         []byte
// }

type KVStoreReader struct {
	data      mmap.MMap
	dataFile  *os.File
	index     mmap.MMap
	indexFile *os.File
	// cursor    cursor
}

func newKVStoreReader(basename string) (*KVStoreReader, error) {
	dataFile, err := os.Open(basename + ".data")
	if err != nil {
		return nil, err
	}

	data, err := mmap.Map(dataFile, mmap.RDONLY, 0)
	if err != nil {
		_ = dataFile.Close()
		return nil, err
	}

	indexFile, err := os.Open(basename + ".index")
	if err != nil {
		_ = dataFile.Close()
		return nil, err
	}

	index, err := mmap.Map(indexFile, mmap.RDONLY, 0)
	if err != nil {
		_ = dataFile.Close()
		_ = indexFile.Close()
		return nil, err
	}

	return &KVStoreReader{
		data:      data,
		dataFile:  dataFile,
		index:     index,
		indexFile: indexFile,
	}, nil
}

// func (kv *KVStoreReader) Start() {
// 	kv.index = 0
// 	kv.offset = 0
// }

// numItems = 4
// 0, 1, 2, 3

// numItems = 5
// 0, 1, 2, 3, 4

func (kv *KVStoreReader) Get(key []byte) []byte {
	numItems := len(kv.index) / 8
	if numItems == 0 {
		return nil
	}

	var leftIndex int64 = 0
	var rightIndex int64 = int64(numItems) - 1

	for leftIndex <= rightIndex {
		index := leftIndex + ((rightIndex - leftIndex) / 2)

		offset := binary.BigEndian.Uint64(kv.index[index*8 : (index*8)+8])
		keyLength := binary.BigEndian.Uint32(kv.data[offset : offset+4])
		currentKey := kv.data[offset+8 : offset+8+uint64(keyLength)]

		keyCompareResult := bytes.Compare(currentKey, key)

		switch keyCompareResult {
		case -1:
			// currentKey < key, go right

			leftIndex = index + 1
		case 0:
			// We found it
			valueLength := binary.BigEndian.Uint32(kv.data[offset+4 : offset+8])
			return kv.data[offset+8+uint64(keyLength) : offset+8+uint64(keyLength)+uint64(valueLength)]

		case 1:
			// currentKey > key, go left

			rightIndex = index - 1

		default:
			log.Fatalf("compare is not -1, 0 or 1: %d\n", keyCompareResult)
		}
	}

	return nil
}

func (kv *KVStoreReader) Close() error {
	if err := kv.dataFile.Close(); err != nil {
		_ = kv.indexFile.Close()
		return err
	}

	if err := kv.indexFile.Close(); err != nil {
		return err
	}

	return nil
}
