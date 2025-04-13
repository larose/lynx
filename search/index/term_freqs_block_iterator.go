package index

import (
	"bytes"
	"encoding/binary"
	"io"
	"log"
)

// TermFreqsBlockIterator provides block-level iteration over term frequency data.
// It handles reading block headers and navigating between blocks.
type TermFreqsBlockIterator struct {
	reader *bytes.Reader

	// Block header
	blockHeaderDecoded bool
	numDocs            byte
	firstDocId         DocumentId
	lastDocId          DocumentId
	maxFreq            uint64
	minLengthId        byte
	length             uint32
	nextBlockOffset    int64
	blockStartOffset   int64
}

// newTermFreqsBlockIterator creates a new block iterator for the given term.
func newTermFreqsBlockIterator(fileReader FileReader, termInfo *TermInfo) *TermFreqsBlockIterator {
	data := fileReader.Slice(termInfo.FreqsFileStartOffset, termInfo.FreqsFileEndOffset)
	reader := bytes.NewReader(data)

	return &TermFreqsBlockIterator{
		reader: reader,
	}
}

// SeekBlock advances to the block that may contain documents with ID >= docId.
// Returns false if there are no more blocks.
func (it *TermFreqsBlockIterator) SeekBlock(docId DocumentId) bool {
	for {
		if !it.blockHeaderDecoded {
			it.decodeHeader()
			it.blockHeaderDecoded = true
		}

		if docId <= it.lastDocId {
			return true
		}

		if it.reader.Len() == 0 {
			return false
		}

		_, err := it.reader.Seek(it.nextBlockOffset, io.SeekStart)
		if err != nil {
			log.Fatal(err)
		}

		it.blockHeaderDecoded = false
	}
}

// decodeHeader reads the block header from the current position in the reader.
func (it *TermFreqsBlockIterator) decodeHeader() {
	var err error

	it.blockStartOffset, err = it.reader.Seek(0, io.SeekCurrent)
	if err != nil {
		log.Fatal(err)
	}

	binary.Read(it.reader, binary.BigEndian, &it.numDocs)
	binary.Read(it.reader, binary.BigEndian, &it.firstDocId)
	binary.Read(it.reader, binary.BigEndian, &it.lastDocId)
	binary.Read(it.reader, binary.BigEndian, &it.maxFreq)
	binary.Read(it.reader, binary.BigEndian, &it.minLengthId)
	binary.Read(it.reader, binary.BigEndian, &it.length)

	it.nextBlockOffset = it.blockStartOffset + int64(it.length)
}

// NumDocs returns the number of documents in the current block.
func (it *TermFreqsBlockIterator) NumDocs() byte {
	return it.numDocs
}

// FirstDocId returns the ID of the first document in the current block.
func (it *TermFreqsBlockIterator) FirstDocId() DocumentId {
	return it.firstDocId
}

// LastDocId returns the ID of the last document in the current block.
func (it *TermFreqsBlockIterator) LastDocId() DocumentId {
	return it.lastDocId
}

// MaxFreq returns the maximum term frequency in the current block.
func (it *TermFreqsBlockIterator) MaxFreq() uint64 {
	return it.maxFreq
}

// MinLengthId returns the minimum field length ID in the current block.
func (it *TermFreqsBlockIterator) MinLengthId() byte {
	return it.minLengthId
}

// BlockMaxFreqMinLengthId returns the maximum term frequency and minimum field length ID
// in the current block. This is used for computing score bounds.
func (it *TermFreqsBlockIterator) BlockMaxFreqMinLengthId() (uint64, byte) {
	return it.maxFreq, it.minLengthId
}

// CreateDocIterator creates a document iterator for the current block.
func (it *TermFreqsBlockIterator) CreateDocIterator() *TermFreqsDocIterator {
	// Get current position (after header)
	currentPos, err := it.reader.Seek(0, io.SeekCurrent)
	if err != nil {
		log.Fatal(err)
	}

	// Save current position for later restoration
	savedPos := currentPos

	// Calculate remaining data size for this block
	dataSize := it.nextBlockOffset - currentPos

	// Create a reader just for this block's data
	blockData := make([]byte, dataSize)
	_, err = io.ReadFull(it.reader, blockData)
	if err != nil {
		log.Fatal(err)
	}

	// Restore position for subsequent operations
	_, err = it.reader.Seek(savedPos, io.SeekStart)
	if err != nil {
		log.Fatal(err)
	}

	// Create the document iterator
	docIterator := &TermFreqsDocIterator{
		reader:       bytes.NewReader(blockData),
		numDocs:      it.numDocs,
		firstDocId:   it.firstDocId,
		blockDocIds:  make([]DocumentId, it.numDocs),
		blockFreqs:   make([]uint64, it.numDocs),
		dataDecoded:  false,
		currentIndex: -1,
	}

	return docIterator
}
