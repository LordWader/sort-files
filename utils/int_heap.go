package utils

type IntHeap []int

func (pq IntHeap) Len() int { return len(pq) }

func (pq IntHeap) Less(i, j int) bool {
	return pq[i] < pq[j]
}

func (pq IntHeap) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
}

func (pq *IntHeap) Push(x any) {
	item := x.(int)
	*pq = append(*pq, item)
}

func (pq *IntHeap) Pop() any {
	old := *pq
	n := len(old)
	item := old[n-1]
	*pq = old[0 : n-1]
	return item
}
