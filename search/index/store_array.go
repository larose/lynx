package index

import (
	"os"

	"github.com/edsrzf/mmap-go"
)

type ArrayStoreWriter struct {
	file *os.File
}

func newArrayStoreWriter(filename string) (*ArrayStoreWriter, error) {
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0600)
	if err != nil {
		return nil, err
	}

	return &ArrayStoreWriter{
		file: file,
	}, nil
}

func (writer *ArrayStoreWriter) Append(value []byte) error {
	_, err := writer.file.Write(value)
	return err
}

type ArrayStoreReader struct {
	data             mmap.MMap
	elementValueSize uint32
	file             *os.File
}

func newArrayStoreReader(filename string, elementValueSize uint32) (*ArrayStoreReader, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}

	data, err := mmap.Map(file, mmap.RDONLY, 0)
	if err != nil {
		_ = file.Close()
		return nil, err
	}

	return &ArrayStoreReader{
		data:             data,
		elementValueSize: elementValueSize,
		file:             file,
	}, nil
}

func (reader *ArrayStoreReader) Get(position uint32) []byte {
	return reader.data[position*reader.elementValueSize : (position*reader.elementValueSize)+reader.elementValueSize]
}
