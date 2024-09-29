package index

import (
	"encoding/binary"
	"os"
	"path/filepath"
)

type FieldStatsWriter struct {
	file *os.File
}

func newFieldStatsWriter(directory, segment, fieldName string) (*FieldStatsWriter, error) {
	file, err := createFile(filepath.Join(directory, "segment."+segment+"."+fieldName+".stats"))
	if err != nil {
		return nil, err
	}

	return &FieldStatsWriter{
		file: file,
	}, nil
}

func (writer *FieldStatsWriter) Write(docCount uint32, sumTermFreq uint64) error {
	buffer := make([]byte, 12)

	binary.BigEndian.PutUint32(buffer, docCount)
	binary.BigEndian.PutUint64(buffer[4:], sumTermFreq)

	_, err := writer.file.Write(buffer)
	return err
}

type FieldStatsReader struct {
	file *os.File
}

func newFieldStatsReader(directory, segment, fieldName string) (*FieldStatsReader, error) {
	file, err := os.Open(filepath.Join(directory, "segment."+segment+"."+fieldName+".stats"))
	if err != nil {
		return nil, err
	}

	return &FieldStatsReader{
		file: file,
	}, nil
}

func (reader *FieldStatsReader) Read() (uint32, uint64, error) {
	buffer := make([]byte, 12)

	_, err := reader.file.Read(buffer)
	if err != nil {
		return 0, 0, err
	}

	docCount := binary.BigEndian.Uint32(buffer)
	sumTermFreq := binary.BigEndian.Uint64(buffer[4:])
	return docCount, sumTermFreq, nil
}
