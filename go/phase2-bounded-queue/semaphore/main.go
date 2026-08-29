package main

import (
	"fmt"
	"os"
	"sync"
	"time"
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

// PushTimeout / PopTimeout: unlike the Cond version, channels support
// timeouts natively via select — no manual timer-plus-Broadcast dance
// needed. This is one of the few places the channel-based design is
// straightforwardly simpler than Mutex+Cond.
func (q *BoundedQueue) PushTimeout(item int, d time.Duration) bool {
	select {
	case <-q.emptySlots:
	case <-time.After(d):
		return false
	}

	q.mu.Lock()
	q.buf[q.tail] = item
	q.tail = (q.tail + 1) % q.capacity
	q.mu.Unlock()

	q.fullSlots <- struct{}{}
	return true
}

func (q *BoundedQueue) PopTimeout(d time.Duration) (int, bool) {
	select {
	case <-q.fullSlots:
	case <-time.After(d):
		return 0, false
	}

	q.mu.Lock()
	item := q.buf[q.head]
	q.head = (q.head + 1) % q.capacity
	q.mu.Unlock()

	q.emptySlots <- struct{}{}
	return item, true
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

type popResult struct {
	val int
	ok  bool
}

func runTimeoutCheck() bool {
	q := NewBoundedQueue(2)

	start := time.Now()
	if _, ok := q.PopTimeout(30 * time.Millisecond); ok {
		fmt.Println("FAILED (timeout): PopTimeout on empty queue unexpectedly succeeded")
		return false
	}
	if elapsed := time.Since(start); elapsed < 25*time.Millisecond || elapsed > 200*time.Millisecond {
		fmt.Printf("FAILED (timeout): PopTimeout elapsed %v, expected ~30ms\n", elapsed)
		return false
	}

	if !q.PushTimeout(1, 10*time.Millisecond) || !q.PushTimeout(2, 10*time.Millisecond) {
		fmt.Println("FAILED (timeout): PushTimeout on non-full queue unexpectedly failed")
		return false
	}
	start = time.Now()
	if q.PushTimeout(3, 30*time.Millisecond) {
		fmt.Println("FAILED (timeout): PushTimeout on full queue unexpectedly succeeded")
		return false
	}
	if elapsed := time.Since(start); elapsed < 25*time.Millisecond || elapsed > 200*time.Millisecond {
		fmt.Printf("FAILED (timeout): PushTimeout elapsed %v, expected ~30ms\n", elapsed)
		return false
	}

	q2 := NewBoundedQueue(1)
	resultCh := make(chan popResult, 1)
	go func() {
		v, ok := q2.PopTimeout(500 * time.Millisecond)
		resultCh <- popResult{v, ok}
	}()
	time.Sleep(20 * time.Millisecond)
	pushStart := time.Now()
	q2.Push(42)
	res := <-resultCh
	if !res.ok || res.val != 42 {
		fmt.Printf("FAILED (timeout): expected interrupted PopTimeout to get 42, got val=%d ok=%v\n", res.val, res.ok)
		return false
	}
	if elapsed := time.Since(pushStart); elapsed > 100*time.Millisecond {
		fmt.Printf("FAILED (timeout): PopTimeout took %v to notice a real push, should be near-instant\n", elapsed)
		return false
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
	if !runTimeoutCheck() {
		os.Exit(1)
	}
	fmt.Printf("OK: %d items, completeness + per-producer FIFO order + timeout variants all hold\n", numProducers*itemsPerProducer)
}
