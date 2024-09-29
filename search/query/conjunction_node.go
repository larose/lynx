package query

import (
	"cmp"
	"slices"

	"github.com/larose/lynx/search/index"
)

// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
// ConjunctionRootNode
// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

type ConjunctionRootNode struct {
	childNodes []ChildNode
}

func (d *ConjunctionRootNode) CreateRootDocIterator(context *ExecutionContext, segmentIndex int) RootDocIterator {
	childDocIterators := make([]ChildDocIterator, 0, len(d.childNodes))

	for _, childCompiledNode := range d.childNodes {
		childDocIterator := childCompiledNode.CreateChildDocIterator(context, segmentIndex)
		if childDocIterator == nil {
			return nil
		}

		childDocIterators = append(childDocIterators, childDocIterator)
	}

	return NewConjunctionDocIterator(childDocIterators)
}

// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
// RootConjunctionDocIterator
// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

type RootConjunctionDocIterator struct {
	childIterators []ChildDocIterator
}

func NewConjunctionDocIterator(childIterators []ChildDocIterator) *RootConjunctionDocIterator {
	for i := 0; i < len(childIterators); i++ {
		childIterators[i].NextShallow(0)
	}

	return &RootConjunctionDocIterator{
		childIterators: childIterators,
	}
}

func (d *RootConjunctionDocIterator) Next(fieldLengthNorms *index.FieldLengthNorms, lowerBound float32) (index.DocumentId, float32, bool) {
	for {
		if len(d.childIterators) == 0 {
			return 0, 0, false
		}

		// Sort children by doc id IN DESCENDING order
		slices.SortFunc(d.childIterators, func(a, b ChildDocIterator) int {
			return cmp.Compare(b.DocId(), a.DocId())
		})

		maxDocId := d.childIterators[0].DocId()

		allAtMaxDocId := true
		childIndexesToRemove := make([]int, 0)
		for i, child := range d.childIterators {
			hasNext := child.Next(maxDocId)

			if !hasNext {
				childIndexesToRemove = append(childIndexesToRemove, i)
				continue
			}

			allAtMaxDocId = child.DocId() == maxDocId

			if !allAtMaxDocId {
				break
			}
		}

		for i := len(childIndexesToRemove) - 1; i >= 0; i-- {
			d.childIterators = removeElement(d.childIterators, childIndexesToRemove[i])
		}

		if !allAtMaxDocId {
			continue
		}

		fieldLengthNorms.SetDocId(maxDocId)

		childIndexesToRemove = make([]int, 0)
		score := float32(0)
		for i, child := range d.childIterators {
			score += child.Score(fieldLengthNorms)
			hasNext := child.Next(maxDocId + 1)

			if !hasNext {
				childIndexesToRemove = append(childIndexesToRemove, i)
			}
		}

		for i := len(childIndexesToRemove) - 1; i >= 0; i-- {
			d.childIterators = removeElement(d.childIterators, childIndexesToRemove[i])
		}

		return maxDocId, score, true
	}
}
