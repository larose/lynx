package search

import (
	"os"
)

type FileWriter struct {
	file   *os.File
	offset int64
}

func newFileWriter(filename string) (*FileWriter, error) {
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_RDWR|os.O_EXCL, 0600)
	if err != nil {
		return nil, err
	}

	return &FileWriter{
		file: file,
	}, nil
}

func (writer *FileWriter) Close() error {
	return writer.file.Close()
}

func (writer *FileWriter) Offset() int64 {
	return writer.offset
}

func (writer *FileWriter) Write(data []byte) (int, error) {
	bytesWritten, err := writer.file.Write(data)
	writer.offset += int64(bytesWritten)
	return bytesWritten, err
}
