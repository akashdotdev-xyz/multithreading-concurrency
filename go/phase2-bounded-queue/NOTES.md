# 2.1 — Bounded Blocking Queue

**Task:** generic FIFO queue, fixed capacity `n`, concurrent `Push`/`Pop` from **multiple**
producers and **multiple** consumers. `Push` blocks while full, `Pop` blocks while empty.
Built from scratch — no channel used as the queue itself (that would trivialize the exercise;
see [PRIMITIVES.md](../PRIMITIVES.md)'s Go warning).

**Invariant:** `0 <= count <= capacity` at all times; FIFO order preserved.

---

## Solution 1 — Mutex + two Conds (`condvar/main.go`)

```go
type BoundedQueue struct {
    capacity          int
    buf               []int
    head, tail, count int
    mu                sync.Mutex
    notFull, notEmpty *sync.Cond
}

func (q *BoundedQueue) Push(item int) {
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
// Pop mirrors Push: waits on notEmpty, signals notFull
```

Classic circular buffer (`head`/`tail`/`count`) plus the two-condition pattern from
[PHASE2-CONCEPTS.md](../PHASE2-CONCEPTS.md) §1: a `Push` only ever needs to wake something
waiting for "not empty," a `Pop` only ever needs to wake something waiting for "not full" —
never both from one call.

**`Signal()`, not `Broadcast()` — different from Print in Order / FooBar, and worth being able
to say why.** In those problems, multiple waiters shared *one* `Cond` with *different*
predicates, so `Signal()` risked waking the wrong one. Here, each `Cond` has a **homogeneous**
waiter set — every goroutine parked on `notFull` is checking the exact same predicate
(`count == capacity`), every goroutine parked on `notEmpty` is checking the exact same one
(`count == 0`). A single `Push` only ever frees up *one* additional slot, so waking exactly one
`notEmpty` waiter is always sufficient — nothing is left unwoken, and `Signal()` avoids the
thundering-herd cost of waking every waiter just to have all but one recheck and go back to
sleep.

---

## Solution 2 — semaphores (`semaphore/main.go`)

```go
type BoundedQueue struct {
    capacity   int
    buf        []int
    head, tail int
    mu         sync.Mutex      // protects buf/head/tail only
    emptySlots chan struct{}   // permits = capacity initially
    fullSlots  chan struct{}   // permits = 0 initially
}

func (q *BoundedQueue) Push(item int) {
    <-q.emptySlots              // acquire a permit to write (blocks if buffer full)
    q.mu.Lock()
    q.buf[q.tail] = item
    q.tail = (q.tail + 1) % q.capacity
    q.mu.Unlock()
    q.fullSlots <- struct{}{}   // release a permit to read
}
// Pop mirrors Push: acquires fullSlots, releases emptySlots
```

The Little Book of Semaphores' classic producer-consumer shape: `emptySlots` counts available
capacity, `fullSlots` counts items ready to read. Two counting semaphores (buffered channels,
per the standard Go substitute — no native semaphore type) handle **admission control**; a
separate plain `Mutex` still guards the actual index mutation, since the semaphores only bound
*how many* goroutines may be mid-`Push`/`Pop` at once, not the correctness of `head`/`tail`
arithmetic itself if two producers raced on it simultaneously (`emptySlots` permits multiple
producers through concurrently when there's room — the mutex is what keeps their writes from
corrupting each other).

---

## Testing gotcha worth remembering

First version of the stress harness had multiple producers **and** multiple consumers, with
each consumer appending what it popped to a shared log for later verification. It failed
intermittently with what looked like an ordering bug — but the queue was actually correct.
**The bug was in the test, not the code under test:** with multiple consumers, the order in
which goroutines append to an external log is not guaranteed to match the true dequeue order —
a goroutine can be descheduled between `Pop()` returning and it executing the append, letting a
later-popping goroutine record first. Fixed by splitting verification into two phases:

- **Completeness** (multi-producer, multi-consumer): every produced item consumed exactly
  once, none lost or duplicated. This is observable correctly even with concurrent consumers.
- **Order** (multi-producer, **single** consumer): with only one consumer there's no race on
  the recording step, so the log faithfully reflects true dequeue order — checks that each
  producer's items come out in the same relative order they went in.

General lesson: strict ordering claims about a concurrent system are often only *externally
verifiable* under a reduced/simplified harness (fewer consumers), even when the property holds
internally under full concurrency. Don't mistake a broken assertion for a broken implementation
without checking whether the assertion itself assumed something the concurrency model doesn't
guarantee.

---

## Verified

Both variants: 200 runs via `stress.sh` at 8 producers × 8 consumers × 2000 items/producer
(16,000 items/run), 0 failures. 10 runs under `-race`, 0 races.

## Follow-ups not yet implemented (per the main plan)

- `offer(item, timeout)` / `poll(timeout)` — no timed wait for `Cond` in Go; needs a
  channel+timer pattern instead (see PHASE2-CONCEPTS.md §1).
- Bounded by total bytes rather than item count.
- `size()` correct without holding the lock for the whole call.
