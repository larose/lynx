package index

import (
	"path/filepath"
	"slices"

	"github.com/larose/lynx/search/utils"
)

type StoreWriter struct {
	currentDocId DocumentId
	values       map[string]map[DocumentId][]byte
}

func newStoreWriter() *StoreWriter {
	return &StoreWriter{
		values: make(map[string]map[DocumentId][]byte, 10),
	}
}

func (writer *StoreWriter) Doc(docId DocumentId) {
	writer.currentDocId = docId
}

func (writer *StoreWriter) Field(fieldName string, value []byte) {
	fieldValues, exists := writer.values[fieldName]

	if !exists {
		fieldValues = make(map[DocumentId][]byte, 100)
		writer.values[fieldName] = fieldValues
	}

	fieldValues[writer.currentDocId] = value
}

func (writer *StoreWriter) EndField() {
}

func (writer *StoreWriter) Term(term []byte) {
}

func (writer *StoreWriter) Write(directory, segmentId string) error {
	for fieldName, values := range writer.values {
		kvStoreWriter, err := newKVStoreWriter(filepath.Join(directory, "segment."+segmentId+"."+fieldName+".store"))
		if err != nil {
			return err
		}

		sortedDocIds := make([]DocumentId, 0, len(values))
		for docId := range values {
			sortedDocIds = append(sortedDocIds, docId)
		}

		slices.Sort(sortedDocIds)

		for _, docId := range sortedDocIds {
			value := values[docId]
			kvStoreWriter.Append(utils.Uint32ToBytes(uint32(docId)), value)
		}

		if err := kvStoreWriter.Close(); err != nil {
			return err
		}
	}

	return nil
}

type FieldStoreReader struct {
	kvStoreReader *KVStoreReader
}

func newFieldStoreReader(directory string, segmentId string, fieldName string) (*FieldStoreReader, error) {
	kvStoreReader, err := newKVStoreReader(filepath.Join(directory, "segment."+segmentId+"."+fieldName+".store"))
	if err != nil {
		return nil, err
	}

	return &FieldStoreReader{kvStoreReader: kvStoreReader}, nil
}

func (reader *FieldStoreReader) Value(docId DocumentId) []byte {
	value := reader.kvStoreReader.Get(utils.Uint32ToBytes(uint32(docId)))
	return value
}

type StoreReader struct {
	directory         string
	fieldStoreReaders map[string]*FieldStoreReader
	segmentId         string
}

func newStoreReader(directory, segmentId string) *StoreReader {
	return &StoreReader{
		directory:         directory,
		segmentId:         segmentId,
		fieldStoreReaders: make(map[string]*FieldStoreReader, 10),
	}
}

func (reader *StoreReader) GetFieldStoreReader(fieldName string) (*FieldStoreReader, error) {
	fieldStoreReaders, exists := reader.fieldStoreReaders[fieldName]
	if !exists {
		var err error
		fieldStoreReaders, err = newFieldStoreReader(reader.directory, reader.segmentId, fieldName)
		if err != nil {
			return nil, err
		}

		reader.fieldStoreReaders[fieldName] = fieldStoreReaders
	}

	return fieldStoreReaders, nil
}
