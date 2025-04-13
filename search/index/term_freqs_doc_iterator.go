package index

import (
	"encoding/binary"
	"io"
	"log"
)

// TermFreqsDocIterator provides document-level iteration within a block
// of term frequency data.
type TermFreqsDocIterator struct {
	reader       io.Reader
	numDocs      byte
	firstDocId   DocumentId
	blockDocIds  []DocumentId
	blockFreqs   []uint64
	dataDecoded  bool
	currentIndex int
}

// SeekDoc advances to the document with ID >= docId within the current block.
// Returns false if there are no more documents in this block that match the criteria.
func (it *TermFreqsDocIterator) SeekDoc(docId DocumentId) bool {
	if !it.dataDecoded {
		it.decodeBlockData()
	}

	// Find the first document with ID >= docId
	for it.currentIndex < len(it.blockDocIds)-1 {
		it.currentIndex++
		if it.blockDocIds[it.currentIndex] >= docId {
			return true
		}
	}

	return false
}

// decodeBlockData decodes all document IDs and frequencies in the block.
func (it *TermFreqsDocIterator) decodeBlockData() {
	// Read the document IDs
	for i := 0; i < int(it.numDocs); i++ {
		value, err := binary.ReadUvarint(it.reader.(io.ByteReader))
		if err != nil {
			log.Fatal(err)
		}

		if i == 0 {
			it.blockDocIds[i] = DocumentId(value)
		} else {
			it.blockDocIds[i] = it.blockDocIds[i-1] + DocumentId(value)
		}
	}

	// Read the term frequencies
	for i := 0; i < int(it.numDocs); i++ {
		value, err := binary.ReadUvarint(it.reader.(io.ByteReader))
		if err != nil {
			log.Fatal(err)
		}

		it.blockFreqs[i] = value
	}

	it.dataDecoded = true
	it.currentIndex = -1 // Reset the index so Next() will start from the beginning
}

// DocId returns the ID of the current document.
func (it *TermFreqsDocIterator) DocId() DocumentId {
	if it.currentIndex >= 0 && it.currentIndex < len(it.blockDocIds) {
		return it.blockDocIds[it.currentIndex]
	}

	return 0
}

// TermFreq returns the term frequency for the current document.
func (it *TermFreqsDocIterator) TermFreq() uint64 {
	if it.currentIndex >= 0 && it.currentIndex < len(it.blockFreqs) {
		return it.blockFreqs[it.currentIndex]
	}

	return 0
}

// HasNext returns true if there are more documents in this block.
func (it *TermFreqsDocIterator) HasNext() bool {
	return it.currentIndex < len(it.blockDocIds)-1
}
