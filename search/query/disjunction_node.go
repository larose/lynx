package query

import (
	"cmp"
	"log"
	"slices"

	"github.com/larose/lynx/search/index"
)

// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
// DisjunctionRootNode
// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

type DisjunctionRootNode struct {
	childNodes []ChildNode
}

func (d *DisjunctionRootNode) CreateRootDocIterator(context *ExecutionContext, segmentIndex int) RootDocIterator {
	childDocIterators := make([]ChildDocIterator, 0, len(d.childNodes))

	for _, childCompiledNode := range d.childNodes {
		childDocIterator := childCompiledNode.CreateChildDocIterator(context, segmentIndex)
		if childDocIterator != nil {
			childDocIterators = append(childDocIterators, childDocIterator)
		}
	}

	return NewDisjunctionDocIterator(childDocIterators)
}

// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
// RootDisjunctionDocIterator
// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

type RootDisjunctionDocIterator struct {
	childIterators []ChildDocIterator
}

func NewDisjunctionDocIterator(childIterators []ChildDocIterator) *RootDisjunctionDocIterator {
	for i := 0; i < len(childIterators); i++ {
		childIterators[i].NextShallow(0)
	}

	return &RootDisjunctionDocIterator{
		childIterators: childIterators,
	}
}

func removeElement(childIterators []ChildDocIterator, index int) []ChildDocIterator {
	if index != len(childIterators)-1 {
		childIterators[index] = childIterators[len(childIterators)-1]
	}

	return childIterators[:len(childIterators)-1]
}

// Implements block-max WAND
// Reference: Shuai Ding and Torsten Suel. 2011. Faster top-k document retrieval using block-max indexes. In Proceedings of the 34th international ACM SIGIR conference on Research and development in Information Retrieval (SIGIR '11). Association for Computing Machinery, New York, NY, USA, 993–1002.
func (d *RootDisjunctionDocIterator) Next(documentContext *index.FieldLengthNorms, lowerBound float32) (index.DocumentId, float32, bool) {
	for {
		if len(d.childIterators) == 0 {
			return 0, 0, false
		}

		// Sort children by doc id
		slices.SortFunc(d.childIterators, func(a, b ChildDocIterator) int {
			return cmp.Compare(a.DocId(), b.DocId())
		})

		globalUpperBound := float32(0)
		childPivotIndex := -1
		for i, child := range d.childIterators {
			globalUpperBound += child.GlobalUpperBound()
			if globalUpperBound > lowerBound {
				childPivotIndex = i
				break
			}
		}

		if childPivotIndex == -1 {
			return 0, 0, false
		}

		pivotDocId := d.childIterators[childPivotIndex].DocId()

		for i := 0; i < childPivotIndex; {
			hasDocs := d.childIterators[i].NextShallow(pivotDocId)
			if hasDocs {
				i++
				continue
			}

			d.childIterators = append(d.childIterators[:i], d.childIterators[i+1:]...)
			childPivotIndex--
		}

		if len(d.childIterators) == 0 {
			return 0, 0, false
		}

		upperBound := float32(0)
		for i := 0; i <= childPivotIndex; i++ {
			upperBound += d.childIterators[i].BlockUpperBound()
		}

		// If current blocks cannot make it
		if upperBound <= lowerBound {
			maxIdf := float32(0)
			bestChildrenIndex := 0
			maxDocId := index.DocumentId(0)
			for i, it := range d.childIterators {
				if i <= childPivotIndex {
					idf := it.IDF()
					if idf > maxIdf {
						maxIdf = idf
						bestChildrenIndex = i
					}
				}

				blockMaxDocId := it.BlockMaxDocId()
				if blockMaxDocId > maxDocId {
					maxDocId = blockMaxDocId
				}
			}

			if !d.childIterators[bestChildrenIndex].Next(maxDocId + 1) {
				d.childIterators = removeElement(d.childIterators, bestChildrenIndex)
			}
			continue
		}

		if d.childIterators[0].DocId() != pivotDocId {
			bestChildrenIndex := 0
			maxIdf := float32(0)

			for i, it := range d.childIterators[:childPivotIndex] {
				idf := it.IDF()
				if idf > maxIdf {
					maxIdf = idf
					bestChildrenIndex = i
				}

			}

			if !d.childIterators[bestChildrenIndex].Next(pivotDocId + 1) {
				d.childIterators = removeElement(d.childIterators, bestChildrenIndex)
			}
			continue
		}

		documentContext.SetDocId(pivotDocId)

		score := float32(0)
		// TODO: speed up with evaluatePartial
		childIndexesToRemove := make([]int, 0)
		for i, it := range d.childIterators {
			if !it.Next(pivotDocId) {
				log.Fatal("Next(pivotDocId) returned false")
			}

			if it.DocId() == pivotDocId {
				score += it.Score(documentContext)

				if !it.Next(pivotDocId + 1) {
					childIndexesToRemove = append(childIndexesToRemove, i)
				}
			}
		}

		for i := len(childIndexesToRemove) - 1; i >= 0; i-- {
			d.childIterators = removeElement(d.childIterators, childIndexesToRemove[i])
		}

		return pivotDocId, score, true
	}
}
