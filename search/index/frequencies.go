package index

import (
	"bufio"
	"encoding/binary"
	"os"
	"path/filepath"
)

type FieldFreqsWriter struct {
	file   *os.File
	offset int64
	writer *bufio.Writer
}

func newFieldFreqsWriter(directory, segment, fieldName string) (*FieldFreqsWriter, error) {
	file, err := createFile(filepath.Join(directory, "segment."+segment+"."+fieldName+".frequencies"))
	if err != nil {
		return nil, err
	}

	writer := bufio.NewWriter(file)

	return &FieldFreqsWriter{
		file:   file,
		writer: writer,
	}, nil
}

/*
Block:
  - Header:
	- [0] num docs (byte)
	- [1] first doc id (uint32)
	- [5] last doc id (uint32)
	- [9] max term freq (uint64)
	- [17] min field length id (byte)
	- [18] length bytes (uint32)
  - Doc ids block
  - Term freq block
*/
const headerSize = 22

func (writer *FieldFreqsWriter) WriteBlock(docIds []uint32, termFreqs []uint64, minFieldLengthId byte) (uint64, uint64, error) {
	blockStartOffset := writer.offset

	buffer := make([]byte, 0, len(docIds)*4*2+headerSize)

	buffer = append(buffer, byte(len(docIds)))
	buffer = binary.BigEndian.AppendUint32(buffer, docIds[0])
	buffer = binary.BigEndian.AppendUint32(buffer, docIds[len(docIds)-1])

	// TODO: we can probably skip them
	buffer = binary.BigEndian.AppendUint64(buffer, 0) // maxFreq
	buffer = append(buffer, 0)                        // min field length
	buffer = binary.BigEndian.AppendUint32(buffer, 0) // skip byte

	docIdDeltas := make([]uint32, len(docIds))
	docIdDeltas[0] = docIds[0]

	for i := 1; i < len(docIds); i++ {
		docIdDeltas[i] = docIds[i] - docIds[i-1]
	}

	for _, docIdDelta := range docIdDeltas {
		buffer = binary.AppendUvarint(buffer, uint64(docIdDelta))
	}

	maxFreq := uint64(0)

	for _, termFreq := range termFreqs {
		buffer = binary.AppendUvarint(buffer, termFreq)

		if termFreq > maxFreq {
			maxFreq = termFreq
		}
	}

	writer.offset = blockStartOffset + int64(len(buffer))

	// Write header

	binary.BigEndian.PutUint64(buffer[9:], maxFreq)
	buffer[17] = minFieldLengthId
	binary.BigEndian.PutUint32(buffer[18:], uint32(len(buffer)))

	_, err := writer.writer.Write(buffer)
	if err != nil {
		return 0, 0, err
	}

	return uint64(blockStartOffset), uint64(writer.offset), nil
}

func (w *FieldFreqsWriter) Close() error {
	if err := w.writer.Flush(); err != nil {
		return err
	}

	return w.file.Close()
}

type FieldFreqsReader struct {
	fileReader FileReader
}

func newFieldFreqsReader(directory, segment, fieldName string) (*FieldFreqsReader, error) {
	fileReader, err := newFileReader(filepath.Join(directory, "segment."+segment+"."+fieldName+".frequencies"))
	if err != nil {
		return nil, err
	}

	return &FieldFreqsReader{
		fileReader: *fileReader,
	}, nil
}

func (reader *FieldFreqsReader) TermFreqsIterator(termInfo *TermInfo) *TermFreqsIterator {
	return newTermFreqsIterator(reader.fileReader, termInfo)
}
