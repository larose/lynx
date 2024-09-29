package query

import (
	"math"

	"github.com/larose/lynx/search/index"
)

type ExecutionContext struct {
	// fieldFreqsReaders[segmentIndex][fieldIndex]
	fieldFreqsReaders [][]*index.FieldFreqsReader

	// FieldLengthReaders[segmentIndex][fieldIndex]
	FieldLengthReaders [][]*index.FieldLengthReader

	//PrecomputedFieldNorms[fieldIndex][lengthId]
	PrecomputedFieldNorms [][]float32

	// segmentReaders[segmentIndex]
	segmentReaders []*index.SegmentReader

	// termIdfs[fieldIndex][termIndex]
	termIdfs [][]float32

	// termInfos[segmentIndex][fieldIndex][termIndex]
	termInfos [][][]*index.TermInfo
}

type FieldStats struct {
	fieldName   string
	docCount    uint64
	sumTermFreq uint64
}

func GenerateExecutionContext(queryContext *QueryContext, segmentReaders []*index.SegmentReader) (*ExecutionContext, error) {
	fieldFreqsReaders := make([][]*index.FieldFreqsReader, len(segmentReaders))
	fieldLengthReaders := make([][]*index.FieldLengthReader, len(segmentReaders))
	fieldStats := make([]*FieldStats, len(queryContext.Fields))
	for i, field := range queryContext.Fields {
		fieldStats[i] = &FieldStats{fieldName: field.name}
	}
	termInfos := make([][][]*index.TermInfo, len(segmentReaders))

	for i, segmentReader := range segmentReaders {
		fieldFreqsReadersForSegment := make([]*index.FieldFreqsReader, len(queryContext.Fields))
		fieldFreqsReaders[i] = fieldFreqsReadersForSegment

		fieldLengthReadersForSegment := make([]*index.FieldLengthReader, len(queryContext.Fields))
		fieldLengthReaders[i] = fieldLengthReadersForSegment

		termsInfosByFieldAndTerm := make([][]*index.TermInfo, len(queryContext.Fields))
		termInfos[i] = termsInfosByFieldAndTerm

		for j, field := range queryContext.Fields {

			fieldFreqsReader, err := segmentReader.FieldFreqsReader(field.name)
			if err != nil {
				return nil, err
			}
			fieldFreqsReadersForSegment[j] = fieldFreqsReader

			fieldLengthReader, err := segmentReader.DocLengthReader.FieldLengthReader(field.name)
			if err != nil {
				return nil, err
			}

			fieldLengthReadersForSegment[j] = fieldLengthReader

			segmentDocCount, segmentSumTermFreq, err := segmentReader.DocCountAndSumTermFreqForField(field.name)
			if err != nil {
				return nil, err
			}

			fieldStatsForCurrentField := fieldStats[j]
			fieldStatsForCurrentField.docCount += uint64(segmentDocCount)
			fieldStatsForCurrentField.sumTermFreq += segmentSumTermFreq

			dictionaryReader, err := segmentReader.DictionaryReader(field.name)
			if err != nil {
				return nil, err
			}

			termsInfosByTerm := make([]*index.TermInfo, len(field.terms))
			for k, term := range field.terms {
				termInfo := dictionaryReader.Get(term)
				termsInfosByTerm[k] = termInfo
			}

			termsInfosByFieldAndTerm[j] = termsInfosByTerm
		}
	}

	// Step 2: Compute IDFs for each term
	termIdfs := make([][]float32, len(queryContext.Fields))
	for i, field := range queryContext.Fields {
		fieldTermIdfs := make([]float32, len(field.terms))
		termIdfs[i] = fieldTermIdfs

		docCount := fieldStats[i].docCount

		for j := range field.terms {
			docFreq := uint64(0)

			for k := range segmentReaders {
				termInfo := termInfos[k][i][j]

				if termInfo == nil {
					continue
				}

				docFreq += uint64(termInfo.DocFreq)
			}

			fieldTermIdfs[j] = float32(math.Log(float64(1 + (float32(docCount)-float32(docFreq)+0.5)/(float32(docFreq)+0.5))))
		}
	}

	precomputedFieldNorms := make([][]float32, len(fieldStats))

	for i, fieldStatsForField := range fieldStats {
		precomputedFieldNormsForField := make([]float32, index.FieldLengthSize)
		precomputedFieldNorms[i] = precomputedFieldNormsForField

		averageFieldLength := float32(fieldStatsForField.sumTermFreq) / float32(fieldStatsForField.docCount)

		for id, length := range index.FieldLengthTable {
			norm := index.Bm25K1 * (1 - index.Bm25B + index.Bm25B*(float32(length)/averageFieldLength))
			precomputedFieldNormsForField[id] = norm
		}
	}

	return &ExecutionContext{
		fieldFreqsReaders:     fieldFreqsReaders,
		FieldLengthReaders:    fieldLengthReaders,
		PrecomputedFieldNorms: precomputedFieldNorms,
		segmentReaders:        segmentReaders,
		termIdfs:              termIdfs,
		termInfos:             termInfos,
	}, nil
}
