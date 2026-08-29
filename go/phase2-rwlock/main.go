package main

import (
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

type RWLock interface {
	RLock()
	RUnlock()
	Lock()
	Unlock()
}

// ---------- Reader-preference ----------
//
// New readers only check whether a writer is CURRENTLY ACTIVE, never
// whether one is merely waiting. So a continuous stream of readers can
// keep the "readers > 0" condition true forever, and a waiting writer's
// predicate (readers == 0) never becomes true. Simple, but starves
// writers under sustained read load — by design, not by bug.
type RWLockReaderPref struct {
	mu           sync.Mutex
	cond         *sync.Cond
	readers      int
	writerActive bool
}

func NewRWLockReaderPref() *RWLockReaderPref {
	l := &RWLockReaderPref{}
	l.cond = sync.NewCond(&l.mu)
	return l
}

func (l *RWLockReaderPref) RLock() {
	l.mu.Lock()
	for l.writerActive {
		l.cond.Wait()
	}
	l.readers++
	l.mu.Unlock()
}

func (l *RWLockReaderPref) RUnlock() {
	l.mu.Lock()
	l.readers--
	last := l.readers == 0
	l.mu.Unlock()
	if last {
		l.cond.Broadcast()
	}
}

func (l *RWLockReaderPref) Lock() {
	l.mu.Lock()
	for l.writerActive || l.readers > 0 {
		l.cond.Wait()
	}
	l.writerActive = true
	l.mu.Unlock()
}

func (l *RWLockReaderPref) Unlock() {
	l.mu.Lock()
	l.writerActive = false
	l.mu.Unlock()
	l.cond.Broadcast()
}

// ---------- Writer-preference ----------
//
// A waiting writer increments waitingWriters BEFORE it starts waiting.
// New readers check waitingWriters too, not just writerActive, so once a
// writer is queued, no new reader is admitted until it's had its turn.
// Fixes writer starvation, but a continuous stream of writers can now
// starve readers the same way readers used to starve writers above.
type RWLockWriterPref struct {
	mu             sync.Mutex
	cond           *sync.Cond
	readers        int
	writerActive   bool
	waitingWriters int
}

func NewRWLockWriterPref() *RWLockWriterPref {
	l := &RWLockWriterPref{}
	l.cond = sync.NewCond(&l.mu)
	return l
}

func (l *RWLockWriterPref) RLock() {
	l.mu.Lock()
	for l.writerActive || l.waitingWriters > 0 {
		l.cond.Wait()
	}
	l.readers++
	l.mu.Unlock()
}

func (l *RWLockWriterPref) RUnlock() {
	l.mu.Lock()
	l.readers--
	last := l.readers == 0
	l.mu.Unlock()
	if last {
		l.cond.Broadcast()
	}
}

func (l *RWLockWriterPref) Lock() {
	l.mu.Lock()
	l.waitingWriters++
	for l.writerActive || l.readers > 0 {
		l.cond.Wait()
	}
	l.waitingWriters--
	l.writerActive = true
	l.mu.Unlock()
}

func (l *RWLockWriterPref) Unlock() {
	l.mu.Lock()
	l.writerActive = false
	l.mu.Unlock()
	l.cond.Broadcast()
}

// ---------- Fair / FIFO ----------
//
// Every RLock/Lock call takes a ticket and joins a FIFO queue. A writer
// may only proceed when its ticket is at the front of the queue (i.e.
// every earlier-arrived request, of either kind, has already been
// served). A reader may proceed once every entry in the queue AHEAD of
// its own ticket is also a reader — so a contiguous run of readers that
// arrived before any writer can all proceed, but a reader that arrived
// after a still-pending writer must wait behind it. Neither role can
// starve the other: arrival order is the only thing that decides turn.
type ticket struct {
	writer bool
}

type RWLockFair struct {
	mu           sync.Mutex
	cond         *sync.Cond
	queue        []*ticket
	readers      int
	writerActive bool
}

func NewRWLockFair() *RWLockFair {
	l := &RWLockFair{}
	l.cond = sync.NewCond(&l.mu)
	return l
}

func (l *RWLockFair) removeTicket(t *ticket) {
	for i, q := range l.queue {
		if q == t {
			l.queue = append(l.queue[:i], l.queue[i+1:]...)
			return
		}
	}
}

func (l *RWLockFair) readerCanProceed(t *ticket) bool {
	for _, q := range l.queue {
		if q == t {
			return true
		}
		if q.writer {
			return false
		}
	}
	return false
}

func (l *RWLockFair) RLock() {
	l.mu.Lock()
	t := &ticket{writer: false}
	l.queue = append(l.queue, t)
	for !(l.readerCanProceed(t) && !l.writerActive) {
		l.cond.Wait()
	}
	l.removeTicket(t)
	l.readers++
	l.mu.Unlock()
	l.cond.Broadcast()
}

func (l *RWLockFair) RUnlock() {
	l.mu.Lock()
	l.readers--
	l.mu.Unlock()
	l.cond.Broadcast()
}

func (l *RWLockFair) Lock() {
	l.mu.Lock()
	t := &ticket{writer: true}
	l.queue = append(l.queue, t)
	for !(len(l.queue) > 0 && l.queue[0] == t && !l.writerActive && l.readers == 0) {
		l.cond.Wait()
	}
	l.removeTicket(t)
	l.writerActive = true
	l.mu.Unlock()
	l.cond.Broadcast()
}

func (l *RWLockFair) Unlock() {
	l.mu.Lock()
	l.writerActive = false
	l.mu.Unlock()
	l.cond.Broadcast()
}

// ---------- Tests ----------

type mutexCheck struct {
	writerActive atomic.Bool
	readerCount  atomic.Int64
	violated     atomic.Bool
}

func runMutualExclusionCheck(name string, lock RWLock) bool {
	var s mutexCheck
	const numReaders, numWriters = 16, 4
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(numReaders + numWriters)

	for i := 0; i < numReaders; i++ {
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				lock.RLock()
				s.readerCount.Add(1)
				if s.writerActive.Load() {
					s.violated.Store(true)
				}
				s.readerCount.Add(-1)
				lock.RUnlock()
			}
		}()
	}
	for i := 0; i < numWriters; i++ {
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				lock.Lock()
				if s.writerActive.Swap(true) {
					s.violated.Store(true)
				}
				if s.readerCount.Load() != 0 {
					s.violated.Store(true)
				}
				s.writerActive.Store(false)
				lock.Unlock()
			}
		}()
	}

	time.Sleep(100 * time.Millisecond)
	close(stop)
	wg.Wait()

	if s.violated.Load() {
		fmt.Printf("FAILED (%s): mutual exclusion violated\n", name)
		return false
	}
	return true
}

func measureWriterWaitUnderReadLoad(lock RWLock) time.Duration {
	const numReaders = 8
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(numReaders)
	for i := 0; i < numReaders; i++ {
		i := i
		go func() {
			defer wg.Done()
			time.Sleep(time.Duration(i) * time.Millisecond)
			for {
				select {
				case <-stop:
					return
				default:
				}
				lock.RLock()
				time.Sleep(2 * time.Millisecond)
				lock.RUnlock()
			}
		}()
	}

	time.Sleep(10 * time.Millisecond) // let read load ramp up
	start := time.Now()
	lock.Lock()
	elapsed := time.Since(start)
	lock.Unlock()

	close(stop)
	wg.Wait()
	return elapsed
}

func measureReaderWaitUnderWriteLoad(lock RWLock) time.Duration {
	const numWriters = 4
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(numWriters)
	for i := 0; i < numWriters; i++ {
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				lock.Lock()
				time.Sleep(2 * time.Millisecond)
				lock.Unlock()
			}
		}()
	}

	time.Sleep(10 * time.Millisecond)
	start := time.Now()
	lock.RLock()
	elapsed := time.Since(start)
	lock.RUnlock()

	close(stop)
	wg.Wait()
	return elapsed
}

func main() {
	if !runMutualExclusionCheck("reader-preference", NewRWLockReaderPref()) {
		os.Exit(1)
	}
	if !runMutualExclusionCheck("writer-preference", NewRWLockWriterPref()) {
		os.Exit(1)
	}
	if !runMutualExclusionCheck("fair", NewRWLockFair()) {
		os.Exit(1)
	}

	rpWait := measureWriterWaitUnderReadLoad(NewRWLockReaderPref())
	if rpWait < 100*time.Millisecond {
		fmt.Printf("FAILED: expected reader-preference to starve the writer, got wait %v\n", rpWait)
		os.Exit(1)
	}

	wpWriterWait := measureWriterWaitUnderReadLoad(NewRWLockWriterPref())
	if wpWriterWait > 50*time.Millisecond {
		fmt.Printf("FAILED: expected writer-preference to NOT starve the writer, got wait %v\n", wpWriterWait)
		os.Exit(1)
	}
	wpReaderWait := measureReaderWaitUnderWriteLoad(NewRWLockWriterPref())
	if wpReaderWait < 100*time.Millisecond {
		fmt.Printf("FAILED: expected writer-preference to starve the reader under write load, got wait %v\n", wpReaderWait)
		os.Exit(1)
	}

	fairWriterWait := measureWriterWaitUnderReadLoad(NewRWLockFair())
	if fairWriterWait > 50*time.Millisecond {
		fmt.Printf("FAILED: expected fair lock to NOT starve the writer, got wait %v\n", fairWriterWait)
		os.Exit(1)
	}
	fairReaderWait := measureReaderWaitUnderWriteLoad(NewRWLockFair())
	if fairReaderWait > 50*time.Millisecond {
		fmt.Printf("FAILED: expected fair lock to NOT starve the reader, got wait %v\n", fairReaderWait)
		os.Exit(1)
	}

	fmt.Printf("OK: mutual exclusion holds for all 3 variants\n")
	fmt.Printf("  reader-preference: writer waited %v under sustained reads (starved, as expected)\n", rpWait)
	fmt.Printf("  writer-preference: writer waited %v (not starved); reader waited %v under sustained writes (starved, as expected)\n", wpWriterWait, wpReaderWait)
	fmt.Printf("  fair:              writer waited %v, reader waited %v (neither starved)\n", fairWriterWait, fairReaderWait)
}
