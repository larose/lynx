package query

import (
	"github.com/larose/lynx/search/index"
)

// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
// Node
// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

type TermNode struct {
	FieldName string
	Term      []byte
}

func (t *TermNode) CreateRootNode(context *QueryContext) (RootNode, error) {
	fieldIndex, termIndex := context.RegisterTerm(t.FieldName, t.Term)
	return &RootTermNode{fieldIndex: fieldIndex, termIndex: termIndex}, nil
}

func (t *TermNode) CreateChildNode(context *QueryContext) (ChildNode, error) {
	fieldIndex, termIndex := context.RegisterTerm(t.FieldName, t.Term)
	return &ChildTermNode{fieldIndex: fieldIndex, termIndex: termIndex}, nil
}

// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
// RootTermNode
// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

type RootTermNode struct {
	// field index in query context
	fieldIndex int
	// term index in query context
	termIndex int
}

func (t *RootTermNode) CreateRootDocIterator(context *ExecutionContext, segmentIndex int) RootDocIterator {
	termInfo := context.termInfos[segmentIndex][t.fieldIndex][t.termIndex]

	if termInfo == nil {
		return nil
	}

	return newRootTermDocIterator(t.fieldIndex, context.fieldFreqsReaders[segmentIndex][t.fieldIndex].TermFreqsIterator(termInfo), context.PrecomputedFieldNorms[t.fieldIndex], context.termIdfs[t.fieldIndex][t.termIndex])
}

// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
// RootTermDocIterator
// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

type RootTermDocIterator struct {
	blockLastDocId              index.DocumentId
	blockUpperBoundCache        float32
	docId                       index.DocumentId
	fieldIndex                  int
	freqsIterator               *index.TermFreqsIterator
	precomputedFieldLengthNorms []float32
	termIdf                     float32
}

func newRootTermDocIterator(fieldIndex int, freqsIterator *index.TermFreqsIterator, precomputedFieldLengthNorms []float32, termIdf float32) *RootTermDocIterator {
	return &RootTermDocIterator{
		fieldIndex:                  fieldIndex,
		freqsIterator:               freqsIterator,
		precomputedFieldLengthNorms: precomputedFieldLengthNorms,
		termIdf:                     termIdf,
	}
}

func (t *RootTermDocIterator) Next(fieldLengthNorms *index.FieldLengthNorms, lowerBound float32) (index.DocumentId, float32, bool) {
	for {
		exists := t.freqsIterator.NextShallow(t.docId)
		if !exists {
			return 0, 0, false
		}

		if t.freqsIterator.LastDocId != t.blockLastDocId {
			upperBound := t.blockUpperBound()

			if upperBound < lowerBound {
				// fmt.Print("skipped block")
				t.docId = t.freqsIterator.LastDocId + 1
				continue
			}

			t.blockLastDocId = t.freqsIterator.LastDocId
			t.blockUpperBoundCache = upperBound
		}

		exists = t.freqsIterator.Next(t.docId)
		if !exists {
			return 0, 0, false
		}

		docId := t.freqsIterator.DocId()
		t.docId = docId + 1

		// TODO: we don't need a FieldLengthNorms since we only have one field
		fieldLengthNorms.SetDocId(docId)

		score := t.score(fieldLengthNorms)

		return docId, score, true
	}
}

func (t *RootTermDocIterator) blockUpperBound() float32 {
	_maxFreq, minLengthId := t.freqsIterator.BlockMaxFreqMinLengthId()
	return t.computeScoreForUpperBound(_maxFreq, minLengthId)
}

func (t *RootTermDocIterator) computeScoreForUpperBound(freq uint64, lengthId byte) float32 {
	_freq := float32(freq)
	lengthNorm := t.precomputedFieldLengthNorms[lengthId]
	termFreqFactor := (_freq * (index.Bm25K1 + 1)) / (_freq + lengthNorm)
	return t.termIdf * float32(termFreqFactor)
}

// Copy paste
func (t *RootTermDocIterator) score(fieldLengthNorms *index.FieldLengthNorms) float32 {
	termFreq := float32(t.freqsIterator.TermFreq())
	lengthNorm := fieldLengthNorms.Get(t.fieldIndex)
	termFreqFactor := (termFreq * (index.Bm25K1 + 1)) / (termFreq + lengthNorm)
	return t.termIdf * float32(termFreqFactor)
}

// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
// ChildTermNode
// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

type ChildTermNode struct {
	// field index in query context
	fieldIndex int
	// term index in query context
	termIndex int
}

func (t *ChildTermNode) CreateChildDocIterator(context *ExecutionContext, segmentIndex int) ChildDocIterator {
	termInfo := context.termInfos[segmentIndex][t.fieldIndex][t.termIndex]

	if termInfo == nil {
		return nil
	}

	return newChildTermDocIterator(t.fieldIndex, context.fieldFreqsReaders[segmentIndex][t.fieldIndex].TermFreqsIterator(termInfo), context.PrecomputedFieldNorms[t.fieldIndex], context.termIdfs[t.fieldIndex][t.termIndex])
}

// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
// ChildTermDocIterator
// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

type ChildTermDocIterator struct {
	fieldIndex                  int
	freqsIterator               *index.TermFreqsIterator
	globalUpperBound            float32
	precomputedFieldLengthNorms []float32
	termIdf                     float32
}

func newChildTermDocIterator(fieldIndex int, freqsIterator *index.TermFreqsIterator, precomputedFieldLengthNorms []float32, termIdf float32) *ChildTermDocIterator {
	return &ChildTermDocIterator{
		fieldIndex:                  fieldIndex,
		freqsIterator:               freqsIterator,
		globalUpperBound:            termIdf * (index.Bm25K1 + 1),
		precomputedFieldLengthNorms: precomputedFieldLengthNorms,
		termIdf:                     termIdf,
	}
}

func (t *ChildTermDocIterator) computeScoreForUpperBound(freq uint64, lengthId byte) float32 {
	_freq := float32(freq)
	lengthNorm := t.precomputedFieldLengthNorms[lengthId]
	termFreqFactor := (_freq * (index.Bm25K1 + 1)) / (_freq + lengthNorm)
	return t.termIdf * float32(termFreqFactor)
}

func (t *ChildTermDocIterator) BlockMaxDocId() index.DocumentId {
	return t.freqsIterator.LastDocId
}

func (t *ChildTermDocIterator) BlockUpperBound() float32 {
	_maxFreq, minLengthId := t.freqsIterator.BlockMaxFreqMinLengthId()
	return t.computeScoreForUpperBound(_maxFreq, minLengthId)
}

func (t *ChildTermDocIterator) DocId() index.DocumentId {
	return t.freqsIterator.DocId()
}

func (t *ChildTermDocIterator) GlobalUpperBound() float32 {
	return t.globalUpperBound
}

func (t *ChildTermDocIterator) IDF() float32 {
	return t.termIdf
}

func (t *ChildTermDocIterator) Next(docId index.DocumentId) bool {
	return t.freqsIterator.Next(docId)
}

func (t *ChildTermDocIterator) NextShallow(docId index.DocumentId) bool {
	return t.freqsIterator.NextShallow(docId)
}

func (t *ChildTermDocIterator) Score(fieldLengthNorms *index.FieldLengthNorms) float32 {
	termFreq := float32(t.freqsIterator.TermFreq())
	lengthNorm := fieldLengthNorms.Get(t.fieldIndex)
	termFreqFactor := (termFreq * (index.Bm25K1 + 1)) / (termFreq + lengthNorm)
	return t.termIdf * float32(termFreqFactor)
}
