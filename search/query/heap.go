package query

type KeyValuePair struct {
	Key   float32
	Value interface{}
}

type Heap struct {
	items    []*KeyValuePair
	lessFunc func(a, b float32) bool
}

func NewMinHeap() *Heap {
	h := &Heap{
		lessFunc: func(a, b float32) bool {
			return a < b
		},
	}

	return h
}

func NewMaxHeap() *Heap {
	h := &Heap{
		lessFunc: func(a, b float32) bool {
			return a > b
		},
	}
	return h
}

func (h *Heap) Len() int { return len(h.items) }

func (h Heap) Less(i, j int) bool {
	return h.lessFunc(h.items[i].Key, h.items[j].Key)
}

func (h Heap) Swap(i, j int) {
	h.items[i], h.items[j] = h.items[j], h.items[i]
}

func (h *Heap) Push(item any) {
	h.items = append(h.items, item.(*KeyValuePair))
}

func (h *Heap) Pop() any {
	old := h.items
	n := len(old)
	x := old[n-1]
	h.items = old[0 : n-1]
	return x
}
