package search

import (
	"github.com/larose/lynx/search/index"
	"github.com/larose/lynx/search/query"
)

func Search(_query query.Node, indexReader *index.IndexReader, collector query.Collector) error {
	queryContext := &query.QueryContext{
		Fields: make([]*query.QueryField, 0, 10),
	}

	compiledQueryNode, err := _query.CreateRootNode(queryContext)
	if err != nil {
		return err
	}

	executionContext, err := query.GenerateExecutionContext(queryContext, indexReader.SegmentReaders)
	if err != nil {
		return err
	}

	for i, segmentReader := range indexReader.SegmentReaders {
		documentContext := index.NewFieldLengthNorms(
			executionContext.FieldLengthReaders[i],
			executionContext.PrecomputedFieldNorms,
		)

		docIterator := compiledQueryNode.CreateRootDocIterator(executionContext, i)
		if docIterator == nil {
			continue
		}

		for {
			lowerBound := collector.LowerBound()
			localDocId, score, exists := docIterator.Next(documentContext, lowerBound)
			if !exists {
				break
			}

			if score < lowerBound {
				continue
			}

			// TODO: Prevent the score computation for a deleted document.
			// We should to back to next where it says there's a document to score
			// And then we call it for scoring
			if segmentReader.DeletedDocIds.Contains(uint32(localDocId)) {
				continue
			}

			docId := index.ToGlobalDocId(segmentReader.Id, uint32(localDocId))

			collector.Collect(docId, score)
		}
	}

	return nil
}
