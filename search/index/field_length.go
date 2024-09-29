package index

import (
	"fmt"
	"path/filepath"
)

type FieldLengthReader struct {
	arrayStoreReader *ArrayStoreReader
	docCount         uint32
}

func (reader *FieldLengthReader) GetId(docId DocumentId) (byte, error) {
	value := reader.arrayStoreReader.Get(uint32(docId))

	if value == nil {
		return 0, fmt.Errorf("document %d not found", docId)
	}

	return value[0], nil
}

type DocFieldLengthReader struct {
	directory         string
	arrayStoreReaders map[string]*ArrayStoreReader
	segmentId         string
}

func newDocFieldLengthReader(directory, segmentId string) *DocFieldLengthReader {
	return &DocFieldLengthReader{
		directory:         directory,
		arrayStoreReaders: make(map[string]*ArrayStoreReader, 100),
		segmentId:         segmentId,
	}
}

func (reader *DocFieldLengthReader) FieldLengthReader(fieldName string) (*FieldLengthReader, error) {
	arrayStoreReader, exists := reader.arrayStoreReaders[fieldName]
	if !exists {
		var err error
		arrayStoreReader, err = newArrayStoreReader(filepath.Join(reader.directory, "segment."+reader.segmentId+"."+fieldName+".lengths"), 1)
		if err != nil {
			return nil, err
		}

		reader.arrayStoreReaders[fieldName] = arrayStoreReader
	}

	docCount := uint32(len(arrayStoreReader.data))

	return &FieldLengthReader{
		arrayStoreReader: arrayStoreReader,
		docCount:         docCount,
	}, nil
}
