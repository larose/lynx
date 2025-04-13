package index

// TermFreqsIterator provides a unified interface for iterating through term frequencies
// while using the decoupled block and document iterators internally.
type TermFreqsIterator struct {
	blockIterator *TermFreqsBlockIterator
	docIterator   *TermFreqsDocIterator
	currentDocId  DocumentId
	lastDocId     DocumentId // Made private with getter method
}

// newTermFreqsIterator creates a new TermFreqsIterator for the given term.
func newTermFreqsIterator(fileReader FileReader, termInfo *TermInfo) *TermFreqsIterator {
	blockIterator := newTermFreqsBlockIterator(fileReader, termInfo)

	return &TermFreqsIterator{
		blockIterator: blockIterator,
		docIterator:   nil,
		currentDocId:  0,
	}
}

// SeekDoc advances to the document with ID >= docId.
// Returns false if there are no more documents.
func (it *TermFreqsIterator) SeekDoc(docId DocumentId) bool {
	// If we've exhausted the current doc iterator or don't have one yet
	if it.docIterator == nil || !it.docIterator.SeekDoc(docId) {
		// Try to advance to a block that may contain docId
		for {
			if !it.SeekBlock(docId) {
				return false
			}

			// Create a new doc iterator for this block
			it.docIterator = it.blockIterator.CreateDocIterator()

			// Try to find a document with ID >= docId in this block
			if it.docIterator.SeekDoc(docId) {
				it.currentDocId = it.docIterator.DocId()
				return true
			}

			// If not found, try the next block
			docId = it.blockIterator.LastDocId() + 1
		}
	}

	it.currentDocId = it.docIterator.DocId()
	return true
}

// SeekBlock advances to the block that may contain documents with ID >= docId.
// Returns false if there are no more blocks.
func (it *TermFreqsIterator) SeekBlock(docId DocumentId) bool {
	result := it.blockIterator.SeekBlock(docId)
	if result {
		it.lastDocId = it.blockIterator.LastDocId()
	}
	return result
}

// LastDocId returns the ID of the last document in the current block.
func (it *TermFreqsIterator) LastDocId() DocumentId {
	return it.lastDocId
}

// BlockMaxFreqMinLengthId returns the maximum term frequency and minimum field length ID
// in the current block. This is used for computing score bounds.
func (it *TermFreqsIterator) BlockMaxFreqMinLengthId() (uint64, byte) {
	return it.blockIterator.BlockMaxFreqMinLengthId()
}

// DocId returns the ID of the current document.
func (it *TermFreqsIterator) DocId() DocumentId {
	if it.docIterator != nil {
		return it.docIterator.DocId()
	}
	return it.blockIterator.FirstDocId()
}

// TermFreq returns the term frequency for the current document.
func (it *TermFreqsIterator) TermFreq() uint64 {
	if it.docIterator != nil {
		return it.docIterator.TermFreq()
	}
	return 0
}
