package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// boundedQueue is the same Mutex+Cond circular buffer as phase2-bounded-queue,
// generic so the thread pool can queue *job values instead of ints.
type boundedQueue[T any] struct {
	capacity          int
	buf               []T
	head, tail, count int
	mu                sync.Mutex
	notFull, notEmpty *sync.Cond
}

func newBoundedQueue[T any](capacity int) *boundedQueue[T] {
	q := &boundedQueue[T]{capacity: capacity, buf: make([]T, capacity)}
	q.notFull = sync.NewCond(&q.mu)
	q.notEmpty = sync.NewCond(&q.mu)
	return q
}

func (q *boundedQueue[T]) Push(item T) {
	q.mu.Lock()
	for q.count == q.capacity {
		q.notFull.Wait()
	}
	q.buf[q.tail] = item
	q.tail = (q.tail + 1) % q.capacity
	q.count++
	q.mu.Unlock()
	q.notEmpty.Signal()
}

func (q *boundedQueue[T]) Pop() T {
	q.mu.Lock()
	for q.count == 0 {
		q.notEmpty.Wait()
	}
	item := q.buf[q.head]
	var zero T
	q.buf[q.head] = zero
	q.head = (q.head + 1) % q.capacity
	q.count--
	q.mu.Unlock()
	q.notFull.Signal()
	return item
}

// PopTimeout is Pop with a deadline. sync.Cond has no native timed wait, so
// a timer is armed to Broadcast() after the deadline, forcing every waiter
// to wake and recheck — same "while loop rule" tolerance for spurious
// wakeups covers this being a spurious-looking wakeup too.
func (q *boundedQueue[T]) PopTimeout(d time.Duration) (item T, ok bool) {
	deadline := time.Now().Add(d)
	q.mu.Lock()
	for q.count == 0 {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			q.mu.Unlock()
			return item, false
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
	var zero T
	q.buf[q.head] = zero
	q.head = (q.head + 1) % q.capacity
	q.count--
	q.mu.Unlock()
	q.notFull.Signal()
	return item, true
}

type Task func(ctx context.Context) (any, error)

type Future struct {
	mu     sync.Mutex
	result any
	err    error
	done   chan struct{}
}

func newFuture() *Future {
	return &Future{done: make(chan struct{})}
}

func (f *Future) complete(result any, err error) {
	f.mu.Lock()
	f.result, f.err = result, err
	f.mu.Unlock()
	close(f.done)
}

func (f *Future) Get() (any, error) {
	<-f.done
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.result, f.err
}

type job struct {
	task   Task
	future *Future
	ctx    context.Context
	cancel context.CancelFunc
}

type ThreadPool struct {
	queue     *boundedQueue[*job]
	core, max int
	keepAlive time.Duration
	live      atomic.Int64
	workers   sync.WaitGroup
	closed    atomic.Bool
}

func NewThreadPool(core, max, queueCapacity int, keepAlive time.Duration) *ThreadPool {
	p := &ThreadPool{
		queue:     newBoundedQueue[*job](queueCapacity),
		core:      core,
		max:       max,
		keepAlive: keepAlive,
	}
	for i := 0; i < core; i++ {
		p.live.Add(1)
		p.workers.Add(1)
		go p.runWorker(false)
	}
	return p
}

func (p *ThreadPool) Live() int { return int(p.live.Load()) }

// runWorker(reclaimable=false) is a permanent core worker: blocks forever
// on Pop(). runWorker(reclaimable=true) is a transient worker spawned to
// absorb a burst: it uses PopTimeout, and exits (reclaiming itself) after
// keepAlive with nothing to do.
func (p *ThreadPool) runWorker(reclaimable bool) {
	defer func() {
		p.live.Add(-1)
		p.workers.Done()
	}()
	for {
		var j *job
		if reclaimable {
			var ok bool
			j, ok = p.queue.PopTimeout(p.keepAlive)
			if !ok {
				return
			}
		} else {
			j = p.queue.Pop()
		}
		if j == nil { // poison pill
			return
		}
		p.runTask(j)
	}
}

func (p *ThreadPool) runTask(j *job) {
	defer j.cancel()
	defer func() {
		if r := recover(); r != nil {
			j.future.complete(nil, fmt.Errorf("task panicked: %v", r))
		}
	}()
	result, err := j.task(j.ctx)
	j.future.complete(result, err)
}

// tryGrow atomically claims one more worker slot, capped at max.
func (p *ThreadPool) tryGrow() bool {
	for {
		cur := p.live.Load()
		if cur >= int64(p.max) {
			return false
		}
		if p.live.CompareAndSwap(cur, cur+1) {
			return true
		}
	}
}

var ErrPoolClosed = errors.New("thread pool is shut down")

func (p *ThreadPool) submit(task Task, ctx context.Context, cancel context.CancelFunc) (*Future, error) {
	if p.closed.Load() {
		cancel()
		return nil, ErrPoolClosed
	}
	if p.tryGrow() {
		p.workers.Add(1)
		go p.runWorker(true)
	}
	f := newFuture()
	p.queue.Push(&job{task: task, future: f, ctx: ctx, cancel: cancel})
	return f, nil
}

func (p *ThreadPool) Submit(task Task) (*Future, error) {
	return p.submit(task, context.Background(), func() {})
}

// SubmitWithTimeout only bounds how long the CALLER waits before the task
// is told to stop via ctx.Done() — it cannot forcibly kill a task that
// doesn't check ctx. Cancellation in Go is cooperative, not preemptive.
func (p *ThreadPool) SubmitWithTimeout(task Task, timeout time.Duration) (*Future, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	return p.submit(task, ctx, cancel)
}

// Shutdown pushes max poison pills — an upper bound on however many
// workers (core + any still-live transient ones) could possibly be
// running, since live is capped at max. Any pills beyond the actual live
// count just sit unconsumed in the queue, harmlessly, once every worker
// has exited.
func (p *ThreadPool) Shutdown() {
	p.closed.Store(true)
	for i := 0; i < p.max; i++ {
		p.queue.Push(nil)
	}
	p.workers.Wait()
}

const (
	core          = 4
	max           = 16
	queueCapacity = 32
	keepAlive     = 30 * time.Millisecond
	numTasks      = 5000
	numSubmitters = 4
)

func main() {
	pool := NewThreadPool(core, max, queueCapacity, keepAlive)

	futures := make([]*Future, numTasks)
	var submitters sync.WaitGroup
	submitters.Add(numSubmitters)
	chunk := numTasks / numSubmitters
	for s := 0; s < numSubmitters; s++ {
		s := s
		go func() {
			defer submitters.Done()
			start := s * chunk
			end := start + chunk
			for i := start; i < end; i++ {
				i := i
				f, err := pool.Submit(func(ctx context.Context) (any, error) {
					switch i % 5 {
					case 0:
						panic(fmt.Sprintf("intentional failure for task %d", i))
					case 1:
						return nil, fmt.Errorf("explicit error for task %d", i)
					default:
						return i * i, nil
					}
				})
				if err != nil {
					fmt.Printf("FAILED: unexpected submit error: %v\n", err)
					os.Exit(1)
				}
				futures[i] = f
			}
		}()
	}
	submitters.Wait()

	// Concurrent Get() from multiple goroutines on the same Future: proves
	// close(done) (not a send) is what lets every caller unblock.
	var multiGetWG sync.WaitGroup
	multiGetResults := make([]any, 10)
	multiGetWG.Add(10)
	for g := 0; g < 10; g++ {
		g := g
		go func() {
			defer multiGetWG.Done()
			result, _ := futures[2].Get() // task 2: 2%5==2, success case
			multiGetResults[g] = result
		}()
	}
	multiGetWG.Wait()
	for g, r := range multiGetResults {
		if r != 4 {
			fmt.Printf("FAILED: concurrent Get() #%d got %v, want 4\n", g, r)
			os.Exit(1)
		}
	}

	for i := 0; i < numTasks; i++ {
		result, err := futures[i].Get()
		switch i % 5 {
		case 0:
			if err == nil || !strings.Contains(err.Error(), "task panicked") {
				fmt.Printf("FAILED: task %d expected panic-derived error, got result=%v err=%v\n", i, result, err)
				os.Exit(1)
			}
		case 1:
			if err == nil || !strings.Contains(err.Error(), "explicit error") {
				fmt.Printf("FAILED: task %d expected explicit error, got result=%v err=%v\n", i, result, err)
				os.Exit(1)
			}
		default:
			if err != nil || result != i*i {
				fmt.Printf("FAILED: task %d expected result=%d err=nil, got result=%v err=%v\n", i, i*i, result, err)
				os.Exit(1)
			}
		}
	}

	// Idle worker reclamation: after the burst drains and nothing new is
	// submitted, transient workers above `core` should self-exit within
	// roughly keepAlive. Poll with a generous ceiling to absorb scheduler
	// jitter without the test itself becoming a source of flakiness.
	reclaimed := false
	for i := 0; i < 200; i++ {
		if pool.Live() == core {
			reclaimed = true
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !reclaimed {
		fmt.Printf("FAILED: idle reclamation never brought live workers back to core=%d, stuck at %d\n", core, pool.Live())
		os.Exit(1)
	}

	// Task timeout, cooperative case: task checks ctx.Done() and returns
	// promptly once the deadline fires.
	cooperativeStart := time.Now()
	coopFuture, _ := pool.SubmitWithTimeout(func(ctx context.Context) (any, error) {
		for {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(2 * time.Millisecond):
			}
		}
	}, 20*time.Millisecond)
	_, coopErr := coopFuture.Get()
	coopElapsed := time.Since(cooperativeStart)
	if !errors.Is(coopErr, context.DeadlineExceeded) {
		fmt.Printf("FAILED: cooperative task expected context.DeadlineExceeded, got %v\n", coopErr)
		os.Exit(1)
	}
	if coopElapsed > 100*time.Millisecond {
		fmt.Printf("FAILED: cooperative task took %v, expected to stop near its 20ms deadline\n", coopElapsed)
		os.Exit(1)
	}

	// Task timeout, UNCOOPERATIVE case: task never checks ctx, so the
	// deadline can't force it to stop early — Get() only returns once the
	// task's own sleep finishes, proving cancellation is cooperative, not
	// preemptive.
	uncoopStart := time.Now()
	uncoopFuture, _ := pool.SubmitWithTimeout(func(ctx context.Context) (any, error) {
		time.Sleep(60 * time.Millisecond)
		return "done despite timeout", nil
	}, 5*time.Millisecond)
	uncoopResult, uncoopErr := uncoopFuture.Get()
	uncoopElapsed := time.Since(uncoopStart)
	if uncoopErr != nil || uncoopResult != "done despite timeout" {
		fmt.Printf("FAILED: uncooperative task expected to finish normally, got result=%v err=%v\n", uncoopResult, uncoopErr)
		os.Exit(1)
	}
	if uncoopElapsed < 50*time.Millisecond {
		fmt.Printf("FAILED: uncooperative task returned in %v, suspiciously fast for a 60ms sleep — was it force-killed?\n", uncoopElapsed)
		os.Exit(1)
	}

	pool.Shutdown()

	if _, err := pool.Submit(func(ctx context.Context) (any, error) { return nil, nil }); !errors.Is(err, ErrPoolClosed) {
		fmt.Printf("FAILED: expected ErrPoolClosed after shutdown, got %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("OK: %d tasks resolved; idle reclamation, cooperative timeout, and shutdown all verified\n", numTasks)
}
