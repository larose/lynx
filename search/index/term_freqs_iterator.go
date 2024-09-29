package index

import (
	"bytes"
	"encoding/binary"
	"io"
	"log"
)

type TermFreqsIterator struct {
	reader *bytes.Reader

	// Block header
	blockHeaderDecoded bool
	numDocs            byte
	firstDocId         DocumentId
	LastDocId          DocumentId
	maxFreq            uint64
	minLengthId        byte
	length             uint32
	nextBlockOffset    int64

	// Block data
	blockDataDecoded bool
	indexInBlockId   int
	blockDocIds      []DocumentId
	blockFreqs       []uint64
}

func newTermFreqsIterator(fileReader FileReader, termInfo *TermInfo) *TermFreqsIterator {
	data := fileReader.Slice(termInfo.FreqsFileStartOffset, termInfo.FreqsFileEndOffset)
	reader := bytes.NewReader(data)

	return &TermFreqsIterator{
		indexInBlockId: -1,
		blockDocIds:    make([]DocumentId, 0, 128),
		blockFreqs:     make([]uint64, 0, 128),
		reader:         reader,
	}
}

func (it *TermFreqsIterator) Next(docId DocumentId) bool {
	if !it.NextShallow(docId) {
		return false
	}

	if !it.blockDataDecoded {
		it.blockDocIds = it.blockDocIds[:it.numDocs]
		it.blockFreqs = it.blockFreqs[:it.numDocs]

		for i := 0; i < int(it.numDocs); i++ {
			value, err := binary.ReadUvarint(it.reader)
			if err != nil {
				log.Fatal(err)
			}

			if i == 0 {
				it.blockDocIds[i] = DocumentId(value)
			} else {
				it.blockDocIds[i] = it.blockDocIds[i-1] + DocumentId(value)
			}
		}

		for i := 0; i < int(it.numDocs); i++ {
			value, err := binary.ReadUvarint(it.reader)
			if err != nil {
				log.Fatal(err)
			}

			it.blockFreqs[i] = value
		}

		it.indexInBlockId = 0
		it.blockDataDecoded = true
	}

	for ; it.indexInBlockId < len(it.blockDocIds); it.indexInBlockId++ {
		_docId := it.blockDocIds[it.indexInBlockId]

		if docId <= _docId {
			break
		}
	}

	if it.indexInBlockId < len(it.blockDocIds) {
		return true
	}

	return it.NextShallow(it.LastDocId + 1)
}

func (it *TermFreqsIterator) NextShallow(docId DocumentId) bool {
	decodeHeader := func() {
		start, err := it.reader.Seek(0, io.SeekCurrent)
		if err != nil {
			log.Fatal(err)
		}

		binary.Read(it.reader, binary.BigEndian, &it.numDocs)
		binary.Read(it.reader, binary.BigEndian, &it.firstDocId)
		binary.Read(it.reader, binary.BigEndian, &it.LastDocId)
		binary.Read(it.reader, binary.BigEndian, &it.maxFreq)
		binary.Read(it.reader, binary.BigEndian, &it.minLengthId)
		binary.Read(it.reader, binary.BigEndian, &it.length)
		it.nextBlockOffset = start + int64(it.length)
		it.blockDataDecoded = false
	}

	for {
		if !it.blockHeaderDecoded {
			decodeHeader()
			it.blockHeaderDecoded = true
		}

		if docId <= it.LastDocId {
			return true
		}

		if it.reader.Len() == 0 {
			return false
		}

		_, err := it.reader.Seek(it.nextBlockOffset, io.SeekStart)
		if err != nil {
			log.Fatal(err)
		}

		decodeHeader()
	}
}

func (it *TermFreqsIterator) BlockMaxFreqMinLengthId() (uint64, byte) {
	return it.maxFreq, it.minLengthId
}

func (it *TermFreqsIterator) DocId() DocumentId {
	if it.indexInBlockId != -1 {
		return it.blockDocIds[it.indexInBlockId]
	}

	return it.firstDocId
}

func (it *TermFreqsIterator) TermFreq() uint64 {
	return it.blockFreqs[it.indexInBlockId]
}
