package index

import (
	"encoding/binary"
	"path/filepath"
)

type TermInfo struct {
	DocFreq              uint32
	FreqsFileStartOffset uint64
	FreqsFileEndOffset   uint64
}

type DictionaryWriter struct {
	buffer   []byte
	kvWriter *KVStoreWriter
}

func newDictionaryWriter(directory, segmentId, fieldName string) (*DictionaryWriter, error) {
	writer, err := newKVStoreWriter(filepath.Join(directory, "segment."+segmentId+"."+fieldName+".dictionary"))
	if err != nil {
		return nil, err
	}

	return &DictionaryWriter{buffer: make([]byte, 20), kvWriter: writer}, err
}

func (writer *DictionaryWriter) Write(term []byte, termInfo *TermInfo) error {
	binary.BigEndian.PutUint32(writer.buffer, termInfo.DocFreq)
	binary.BigEndian.PutUint64(writer.buffer[4:], termInfo.FreqsFileStartOffset)
	binary.BigEndian.PutUint64(writer.buffer[12:], termInfo.FreqsFileEndOffset)
	return writer.kvWriter.Append(term, writer.buffer)
}

func (writer *DictionaryWriter) Close() error {
	return writer.kvWriter.Close()
}

type DictionaryReader struct {
	kvReader *KVStoreReader
}

func newDictionaryReader(directory, segmentId, fieldName string) (*DictionaryReader, error) {
	kvReader, err := newKVStoreReader(filepath.Join(directory, "segment."+segmentId+"."+fieldName+".dictionary"))
	if err != nil {
		return nil, err
	}

	return &DictionaryReader{kvReader: kvReader}, nil
}

func (reader *DictionaryReader) Get(term []byte) *TermInfo {
	value := reader.kvReader.Get(term)

	if value == nil {
		return nil
	}

	docFreq := binary.BigEndian.Uint32(value)
	freqsStartOffset := binary.BigEndian.Uint64(value[4:])
	freqsEndOffset := binary.BigEndian.Uint64(value[12:])

	return &TermInfo{
		DocFreq:              docFreq,
		FreqsFileStartOffset: freqsStartOffset,
		FreqsFileEndOffset:   freqsEndOffset,
	}
}
