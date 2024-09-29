package index

import (
	"os"

	"github.com/edsrzf/mmap-go"
)

func createFile(filename string) (*os.File, error) {
	return os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0600)
}

// type FileWriter struct {
// 	offset uint64
// 	file   *os.File
// }

// func newFileWriter(filename string) (*FileWriter, error) {
// 	file, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0600)

// 	if err != nil {
// 		return nil, err
// 	}

// 	return &FileWriter{
// 		file: file,
// 	}, nil
// }

// func (writer *FileWriter) Write(data []byte) (uint64, uint64, error) {
// 	n, err := writer.file.Write(data)
// 	if err != nil {
// 		return 0, 0, err
// 	}

// 	startOffset := writer.offset
// 	writer.offset += uint64(n)

// 	return startOffset, writer.offset, nil
// }

type FileReader struct {
	data mmap.MMap
	file *os.File
}

func newFileReader(filename string) (*FileReader, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}

	data, err := mmap.Map(file, mmap.RDONLY, 0)
	if err != nil {
		_ = file.Close()
		return nil, err
	}

	return &FileReader{
		data: data,
		file: file,
	}, nil
}

func (reader *FileReader) Slice(start, end uint64) []byte {
	return reader.data[start:end]
}
