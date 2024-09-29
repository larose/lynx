package index

import (
	"math"
	"path/filepath"
	"slices"

	"github.com/RoaringBitmap/roaring/v2"
)

type Posting struct {
	docId    DocumentId
	position uint64
}

type InvertedIndexWriter struct {
	docId     DocumentId
	fieldName string
	fieldId   int
	position  uint64

	fieldIds   map[string]int
	fieldNames []string
	// postings[fieldId][term]
	postings []map[string][]*Posting

	// fieldLengtths[fieldName][docId]
	fieldLengths map[string][]uint64
}

func newInvertedIndexWriter() *InvertedIndexWriter {
	return &InvertedIndexWriter{
		fieldIds:     make(map[string]int),
		fieldNames:   make([]string, 0, 5),
		postings:     make([]map[string][]*Posting, 0, 5),
		fieldLengths: make(map[string][]uint64),
	}
}

func (writer *InvertedIndexWriter) Doc(docId DocumentId) {
	writer.docId = docId
}

func (w *InvertedIndexWriter) Field(fieldName string, value []byte) {
	w.fieldName = fieldName
	w.position = 0

	fieldId, exists := w.fieldIds[fieldName]
	if !exists {
		fieldId = len(w.fieldIds)
		w.fieldNames = append(w.fieldNames, fieldName)
		w.fieldIds[fieldName] = fieldId
		w.postings = append(w.postings, make(map[string][]*Posting))
	}

	w.fieldId = fieldId
}

func (w *InvertedIndexWriter) EndField() {
	fieldLengths, exists := w.fieldLengths[w.fieldName]
	if !exists {
		fieldLengths = make([]uint64, 0)
		w.fieldLengths[w.fieldName] = fieldLengths
	}

	w.fieldLengths[w.fieldName] = append(fieldLengths, w.position)
}

func (w *InvertedIndexWriter) Term(term []byte) {
	termString := string(term)
	posting := &Posting{
		docId:    w.docId,
		position: w.position,
	}

	w.postings[w.fieldId][termString] = append(w.postings[w.fieldId][termString], posting)
	w.position++
}

func (w *InvertedIndexWriter) Write(directory, segmentId string) error {
	var fieldName string
	var term string

	var fieldSumTermFreq uint64
	fieldDocIds := roaring.NewBitmap()
	var fieldStatsWriter *FieldStatsWriter
	var fieldFreqsWriter *FieldFreqsWriter
	var dictWriter *DictionaryWriter
	var arrayStoreWriter *ArrayStoreWriter

	termDocIds := make([]uint32, 0, 100)
	termFreqs := make([]uint64, 0, 100)
	var termFreq uint64
	termInfo := &TermInfo{}

	var fieldLengths []uint64

	startField := func(fieldName string) error {
		fieldSumTermFreq = 0
		fieldDocIds.Clear()

		var err error

		fieldFreqsWriter, err = newFieldFreqsWriter(directory, segmentId, fieldName)
		if err != nil {
			return err
		}

		fieldStatsWriter, err = newFieldStatsWriter(directory, segmentId, fieldName)
		if err != nil {
			return err
		}

		dictWriter, err = newDictionaryWriter(directory, segmentId, fieldName)
		if err != nil {
			return err
		}

		arrayStoreWriter, err = newArrayStoreWriter(filepath.Join(directory, "segment."+segmentId+"."+fieldName+".lengths"))
		if err != nil {
			return err
		}

		fieldLengths = w.fieldLengths[fieldName]

		return nil
	}

	endField := func() error {
		if err := fieldStatsWriter.Write(uint32(fieldDocIds.GetCardinality()), fieldSumTermFreq); err != nil {
			return err
		}

		buffer := make([]byte, len(fieldLengths))

		for docId, length := range fieldLengths {
			buffer[docId] = fieldLengthToId(length)
		}

		arrayStoreWriter.Append(buffer)

		if err := fieldFreqsWriter.Close(); err != nil {
			return err
		}

		if err := dictWriter.Close(); err != nil {
			return err
		}

		return nil
	}

	startTerm := func() error {
		termDocIds = termDocIds[:0]
		termFreqs = termFreqs[:0]

		return nil
	}

	endTerm := func() error {
		firstOffset := uint64(0)
		firstOffsetSet := false
		endOffset := uint64(0)

		for i := 0; i < len(termDocIds); i += 128 {
			end := i + 128
			if end > len(termDocIds) {
				end = len(termDocIds)
			}

			docIdsInBatch := termDocIds[i:end]
			termFreqsInBatch := termFreqs[i:end]

			var minFieldLength uint64
			minFieldLength = math.MaxUint64

			for _, docId := range docIdsInBatch {
				fieldLength := fieldLengths[docId]
				if fieldLength < minFieldLength {
					minFieldLength = fieldLength
				}
			}

			startOffset, _endOffset, err := fieldFreqsWriter.WriteBlock(docIdsInBatch, termFreqsInBatch, fieldLengthToId(minFieldLength))
			if err != nil {
				return err
			}

			if !firstOffsetSet {
				firstOffset = startOffset
				firstOffsetSet = true
			}

			endOffset = _endOffset
		}

		termInfo.DocFreq = uint32(len(termDocIds))
		termInfo.FreqsFileStartOffset = firstOffset
		termInfo.FreqsFileEndOffset = endOffset

		return dictWriter.Write([]byte(term), termInfo)
	}

	startDoc := func(docId DocumentId) error {
		fieldDocIds.Add(uint32(docId))
		termDocIds = append(termDocIds, uint32(docId))
		termFreq = 0
		return nil
	}

	endDoc := func() error {
		termFreqs = append(termFreqs, termFreq)
		return nil
	}

	startPosition := func() error {
		fieldSumTermFreq++
		termFreq++
		return nil
	}

	for fieldId, fieldPostings := range w.postings {
		fieldName = w.fieldNames[fieldId]
		if err := startField(fieldName); err != nil {
			return err
		}

		sortedTerms := make([]string, 0, len(fieldPostings))
		for term := range fieldPostings {
			sortedTerms = append(sortedTerms, term)
		}
		slices.Sort(sortedTerms)

		for _, _term := range sortedTerms {
			term = _term
			termPostings := fieldPostings[term]

			if err := startTerm(); err != nil {
				return err
			}

			docId := termPostings[0].docId

			if err := startDoc(docId); err != nil {
				return err
			}

			for _, posting := range termPostings {
				if posting.docId != docId {
					if err := endDoc(); err != nil {
						return err
					}

					docId = posting.docId

					if err := startDoc(posting.docId); err != nil {
						return err
					}
				}

				if err := startPosition(); err != nil {
					return err
				}
			}

			if err := endDoc(); err != nil {
				return err
			}

			if err := endTerm(); err != nil {
				return err
			}
		}

		if err := endField(); err != nil {
			return err
		}
	}

	return nil
}
