package index

import "log"

const Bm25K1 = 1.2
const Bm25B = 0.75

// For a specific doc
type FieldLengthNorms struct {
	docId              DocumentId
	computed           []bool
	fieldLengthReaders []*FieldLengthReader
	lengthNorms        []float32
	// precomputedLengthNorms[fieldIndex][lengthId]
	precomputedLengthNorms [][]float32
}

func NewFieldLengthNorms(fieldLengthReaders []*FieldLengthReader, precomputedLengthNorms [][]float32) *FieldLengthNorms {
	numFields := len(fieldLengthReaders)

	return &FieldLengthNorms{
		computed:               make([]bool, numFields),
		fieldLengthReaders:     fieldLengthReaders,
		lengthNorms:            make([]float32, numFields),
		precomputedLengthNorms: precomputedLengthNorms,
	}
}

func (ctx *FieldLengthNorms) Get(fieldIndex int) float32 {
	if ctx.computed[fieldIndex] {
		return ctx.lengthNorms[fieldIndex]
	}

	fieldLengthId, err := ctx.fieldLengthReaders[fieldIndex].GetId(ctx.docId)

	// TODO: should we return the error?
	if err != nil {
		log.Fatal(err)
	}

	lengthNorm := ctx.precomputedLengthNorms[fieldIndex][fieldLengthId]

	ctx.computed[fieldIndex] = true
	ctx.lengthNorms[fieldIndex] = lengthNorm

	return lengthNorm
}

func (ctx *FieldLengthNorms) SetDocId(newDocId DocumentId) {
	for i := range len(ctx.computed) {
		ctx.computed[i] = false
	}
	ctx.docId = newDocId
}
