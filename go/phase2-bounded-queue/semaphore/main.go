package main

import (
	"fmt"
	"os"
	"sync"
)

type BoundedQueue struct {
	capacity   int
	buf        []int
	head, tail int
	mu         sync.Mutex
	emptySlots chan struct{}
	fullSlots  chan struct{}
}

func NewBoundedQueue(capacity int) *BoundedQueue {
	q := &BoundedQueue{
		capacity:   capacity,
		buf:        make([]int, capacity),
		emptySlots: make(chan struct{}, capacity),
		fullSlots:  make(chan struct{}, capacity),
	}
	for i := 0; i < capacity; i++ {
		q.emptySlots <- struct{}{}
	}
	return q
}

func (q *BoundedQueue) Push(item int) {
	<-q.emptySlots

	q.mu.Lock()
	q.buf[q.tail] = item
	q.tail = (q.tail + 1) % q.capacity
	q.mu.Unlock()

	q.fullSlots <- struct{}{}
}

func (q *BoundedQueue) Pop() int {
	<-q.fullSlots

	q.mu.Lock()
	item := q.buf[q.head]
	q.head = (q.head + 1) % q.capacity
	q.mu.Unlock()

	q.emptySlots <- struct{}{}
	return item
}

const (
	capacity         = 16
	numProducers     = 8
	itemsPerProducer = 2000
	numConsumers     = 8
	poisonPill       = -1
)

func runCompletenessCheck() bool {
	q := NewBoundedQueue(capacity)
	totalItems := numProducers * itemsPerProducer

	var producers sync.WaitGroup
	producers.Add(numProducers)
	for p := 0; p < numProducers; p++ {
		p := p
		go func() {
			defer producers.Done()
			base := p * itemsPerProducer
			for i := 0; i < itemsPerProducer; i++ {
				q.Push(base + i)
			}
		}()
	}

	var mu sync.Mutex
	consumed := make([]int, 0, totalItems)

	var consumers sync.WaitGroup
	consumers.Add(numConsumers)
	for c := 0; c < numConsumers; c++ {
		go func() {
			defer consumers.Done()
			for {
				item := q.Pop()
				if item == poisonPill {
					return
				}
				mu.Lock()
				consumed = append(consumed, item)
				mu.Unlock()
			}
		}()
	}

	producers.Wait()
	for c := 0; c < numConsumers; c++ {
		q.Push(poisonPill)
	}
	consumers.Wait()

	if len(consumed) != totalItems {
		fmt.Printf("FAILED (completeness): expected %d items, got %d\n", totalItems, len(consumed))
		return false
	}

	seen := make([]bool, totalItems)
	for _, v := range consumed {
		if v < 0 || v >= totalItems || seen[v] {
			fmt.Printf("FAILED (completeness): bad or duplicate value %d\n", v)
			return false
		}
		seen[v] = true
	}
	return true
}

func runOrderCheck() bool {
	q := NewBoundedQueue(capacity)
	totalItems := numProducers * itemsPerProducer

	var producers sync.WaitGroup
	producers.Add(numProducers)
	for p := 0; p < numProducers; p++ {
		p := p
		go func() {
			defer producers.Done()
			base := p * itemsPerProducer
			for i := 0; i < itemsPerProducer; i++ {
				q.Push(base + i)
			}
		}()
	}

	consumed := make([]int, 0, totalItems)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for len(consumed) < totalItems {
			consumed = append(consumed, q.Pop())
		}
	}()

	producers.Wait()
	<-done

	lastValuePerProducer := make(map[int]int)
	for _, v := range consumed {
		producer := v / itemsPerProducer
		if prev, ok := lastValuePerProducer[producer]; ok && v <= prev {
			fmt.Printf("FAILED (order): producer %d saw %d after %d\n", producer, v, prev)
			return false
		}
		lastValuePerProducer[producer] = v
	}
	return true
}

func main() {
	if !runCompletenessCheck() {
		os.Exit(1)
	}
	if !runOrderCheck() {
		os.Exit(1)
	}
	fmt.Printf("OK: %d items, completeness + per-producer FIFO order both hold\n", numProducers*itemsPerProducer)
}
