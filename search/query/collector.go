package query

import (
	"container/heap"
)

type Collector interface {
	Collect(docId uint64, score float32)
	LowerBound() float32
}

type DocScore struct {
	DocId uint64
	Score float32
}

type TopNCollector struct {
	topN    int
	minHeap *Heap
}

func NewTopNCollector(topN int) *TopNCollector {
	minHeap := NewMinHeap()

	return &TopNCollector{
		topN:    topN,
		minHeap: minHeap,
	}
}

func (c *TopNCollector) Collect(docId uint64, score float32) {
	if c.minHeap.Len() < c.topN {
		heap.Push(
			c.minHeap,
			&KeyValuePair{
				Key: score,
				Value: &DocScore{
					DocId: docId,
					Score: score,
				},
			})
		return
	}

	minScoreInHeap := c.minHeap.items[0].Value.(*DocScore).Score

	if score > minScoreInHeap {
		heap.Pop(c.minHeap)
		heap.Push(c.minHeap, &KeyValuePair{
			Key:   score,
			Value: &DocScore{DocId: docId, Score: score},
		})
	}
}

func (c *TopNCollector) Get() []*DocScore {
	results := make([]*DocScore, c.minHeap.Len())

	for i := len(results) - 1; i >= 0; i-- {
		results[i] = heap.Pop(c.minHeap).(*KeyValuePair).Value.(*DocScore)
	}

	return results
}

func (c *TopNCollector) LowerBound() float32 {
	if len(c.minHeap.items) < c.topN {
		return 0
	}

	return c.minHeap.items[0].Key
}
