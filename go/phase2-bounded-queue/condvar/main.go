package main

import (
	"fmt"
	"os"
	"sync"
)

type BoundedQueue struct {
	capacity          int
	buf               []int
	head, tail, count int
	mu                sync.Mutex
	notFull, notEmpty *sync.Cond
}

func NewBoundedQueue(capacity int) *BoundedQueue {
	q := &BoundedQueue{capacity: capacity, buf: make([]int, capacity)}
	q.notFull = sync.NewCond(&q.mu)
	q.notEmpty = sync.NewCond(&q.mu)
	return q
}

func (q *BoundedQueue) Push(item int) {
	q.mu.Lock()
	for q.count == q.capacity {
		q.notFull.Wait()
	}
	if q.count < 0 || q.count > q.capacity {
		panic(fmt.Sprintf("invariant violated: count=%d capacity=%d", q.count, q.capacity))
	}
	q.buf[q.tail] = item
	q.tail = (q.tail + 1) % q.capacity
	q.count++
	q.mu.Unlock()

	q.notEmpty.Signal()
}

func (q *BoundedQueue) Pop() int {
	q.mu.Lock()
	for q.count == 0 {
		q.notEmpty.Wait()
	}
	if q.count < 0 || q.count > q.capacity {
		panic(fmt.Sprintf("invariant violated: count=%d capacity=%d", q.count, q.capacity))
	}
	item := q.buf[q.head]
	q.head = (q.head + 1) % q.capacity
	q.count--
	q.mu.Unlock()

	q.notFull.Signal()
	return item
}

const (
	capacity         = 16
	numProducers     = 8
	itemsPerProducer = 2000
	numConsumers     = 8
	poisonPill       = -1
)

// Multiple producers AND multiple consumers. The order in which consumer
// goroutines record what they popped is not guaranteed to match true
// dequeue order (a goroutine can be descheduled between Pop() returning
// and it recording the value), so this phase only checks completeness:
// every produced item consumed exactly once, none lost or duplicated.
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

// Multiple producers, a SINGLE consumer. With one consumer there's no race
// on the recording step, so the log's order faithfully reflects true
// dequeue order — this phase checks that each producer's items come out
// in the same relative order they went in (global FIFO across producers
// isn't required, only per-producer order).
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
