package main

import (
	"fmt"
	"os"
	"sync"
	"time"
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

// PushTimeout and PopTimeout give up after d instead of blocking forever.
// sync.Cond has no native timed wait, so a timer is armed to Broadcast()
// after the deadline, forcing every waiter on that Cond to wake and
// recheck — same as any other wakeup, the loop re-verifies both the
// predicate AND the remaining time, since a real Push/Pop and a timeout
// can race to wake the same waiter in the same window.
func (q *BoundedQueue) PushTimeout(item int, d time.Duration) bool {
	deadline := time.Now().Add(d)
	q.mu.Lock()
	for q.count == q.capacity {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			q.mu.Unlock()
			return false
		}
		timer := time.AfterFunc(remaining, func() {
			q.mu.Lock()
			q.notFull.Broadcast()
			q.mu.Unlock()
		})
		q.notFull.Wait()
		timer.Stop()
	}
	q.buf[q.tail] = item
	q.tail = (q.tail + 1) % q.capacity
	q.count++
	q.mu.Unlock()

	q.notEmpty.Signal()
	return true
}

func (q *BoundedQueue) PopTimeout(d time.Duration) (item int, ok bool) {
	deadline := time.Now().Add(d)
	q.mu.Lock()
	for q.count == 0 {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			q.mu.Unlock()
			return 0, false
		}
		timer := time.AfterFunc(remaining, func() {
			q.mu.Lock()
			q.notEmpty.Broadcast()
			q.mu.Unlock()
		})
		q.notEmpty.Wait()
		timer.Stop()
	}
	item = q.buf[q.head]
	q.head = (q.head + 1) % q.capacity
	q.count--
	q.mu.Unlock()

	q.notFull.Signal()
	return item, true
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

	// A blocked PopTimeout should wake almost immediately on a real Push,
	// not sit until its (much longer) deadline.
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
