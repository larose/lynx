package query

import "github.com/larose/lynx/search/index"

// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
// Node
// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

type Node interface {
	CreateRootNode(context *QueryContext) (RootNode, error)
	CreateChildNode(context *QueryContext) (ChildNode, error)
}

// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
// Root
// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

type RootNode interface {
	CreateRootDocIterator(context *ExecutionContext, segmentIndex int) RootDocIterator
}

type RootDocIterator interface {
	Next(documentContext *index.FieldLengthNorms, lowerBound float32) (index.DocumentId, float32, bool)
}

// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
// Child
// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

type ChildNode interface {
	CreateChildDocIterator(context *ExecutionContext, segmentIndex int) ChildDocIterator
}

type ChildDocIterator interface {
	BlockMaxDocId() index.DocumentId
	BlockUpperBound() float32
	DocId() index.DocumentId
	GlobalUpperBound() float32
	IDF() float32
	Next(docId index.DocumentId) bool
	NextShallow(docId index.DocumentId) bool
	Score(documentContext *index.FieldLengthNorms) float32
}

// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
// Empty child doc iterator
// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

type EmptyChildDocIterator struct {
}

func (e *EmptyChildDocIterator) DocId() index.DocumentId {
	return 0
}

func (e *EmptyChildDocIterator) Next(docId index.DocumentId) bool {
	return false
}

func (e *EmptyChildDocIterator) NextShallow(docId index.DocumentId) bool {
	return false
}

func (e *EmptyChildDocIterator) Score(documentContext *index.FieldLengthNorms) float32 {
	return 0
}

func (e *EmptyChildDocIterator) UpperBound() float32 {
	return 0
}
