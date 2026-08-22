# LC 1114 — Print in Order

**Invariant:** `Second()` may not begin until `First()` has completed; `Third()` may not
begin until `Second()` has completed. Pure ordering constraint — no shared resource being
fought over, so this is a signaling problem, not a mutual-exclusion problem.

Three goroutines call `First()`, `Second()`, `Third()` in arbitrary launch order (the
harness in each `main.go` shuffles launch order and re-verifies 300× via `stress.sh` to
make sure "worked once" isn't mistaken for "correct").

---

## Solution 1 — channels (`channel/main.go`)

```go
type Foo struct {
    secondCh chan struct{}
    thirdCh  chan struct{}
}

func (f *Foo) First(printFirst func()) {
    printFirst()
    f.secondCh <- struct{}{}
}

func (f *Foo) Second(printSecond func()) {
    <-f.secondCh
    printSecond()
    f.thirdCh <- struct{}{}
}

func (f *Foo) Third(printThird func()) {
    <-f.thirdCh
    printThird()
}
```

Two channels, each buffered to capacity 1 — structurally a binary semaphore, one per
ordering gate. `Second` blocks on `<-secondCh` until `First` sends; `Third` blocks on
`<-thirdCh` until `Second` sends. No shared mutable state at all — "share memory by
communicating." Buffered(1) so the sender never blocks waiting for a receiver.

---

## Solution 2 — Mutex + Cond (`condvar/main.go`)

```go
type Foo struct {
    mu    sync.Mutex
    cond  *sync.Cond
    stage int
}

func (f *Foo) First(printFirst func()) {
    f.mu.Lock()
    printFirst()
    f.stage = 1
    f.mu.Unlock()
    f.cond.Broadcast()
}

func (f *Foo) Second(printSecond func()) {
    f.mu.Lock()
    for f.stage != 1 {
        f.cond.Wait()
    }
    printSecond()
    f.stage = 2
    f.mu.Unlock()
    f.cond.Broadcast()
}
// Third mirrors Second, waiting for stage == 2, no further Broadcast needed.
```

One shared `stage` int (0 → 1 → 2) instead of two channels. Two details here are load-bearing,
not style:

- **`for f.stage != 1`, never `if`.** `Cond.Wait()` can return without the predicate being
  true — most likely here because it was *another* waiter's turn, not this one's. Always
  re-check after waking.
- **`Broadcast()`, not `Signal()`.** Two goroutines block on the same `cond` with two
  *different* predicates (`stage != 1` vs `stage != 2`). `Signal()` wakes one arbitrary
  waiter — if it's the wrong one, it rechecks, finds its predicate still false, and goes
  back to sleep, and the goroutine that actually needed waking never gets woken. Confirmed
  by swapping in `Signal()` and re-running the stress script — intermittent deadlocks.

### Why the lock is mandatory (not optional)

Tested by stripping `mu.Lock()`/`Unlock()` from a copy of this file: it crashes immediately,
every run, with `fatal error: sync: unlock of unlocked mutex`. Two independent reasons:

1. **Mechanical.** `Cond.Wait()` is implemented as `c.L.Unlock()` → park → `c.L.Lock()` on
   wake. That unlock/lock pair is what lets "release the mutex and go to sleep" happen as
   one atomic-seeming step, closing the window where a wakeup could be missed. If `c.L`
   was never locked, the internal `Unlock()` call panics.
2. **Memory model.** Even ignoring `Cond`'s mechanics, an unsynchronized `int` written by
   one goroutine and read by another has no happens-before relationship — the compiler/CPU
   are free to cache, reorder, or never re-read it. Correctness on any given run is
   coincidence, not guarantee. Same bug family as "unsynchronized read of a flag" (bug
   catalog #6 in the main plan).

---

## Dead end worth remembering — a single shared `sync.WaitGroup` does NOT work

Attempted version: one `WaitGroup`, with `First` calling `Done()` and `Second`/`Third` each
calling `Add(1)` then `Wait()` on themselves before proceeding.

**Why it's broken:** `WaitGroup` is one counter with one "reached zero" event — it has no
concept of stages or identity. This design asks it to play two incompatible roles on the
same integer: the thing `First` decrements to signal completion, *and* the thing
`Second`/`Third` increment on themselves to create something to wait on. Second's own
`Add(1)` pollutes the exact value Third is also gating on. Traced interleaving that
reproduces it 100% of the time on this machine:

```
Second: Add(1) -> counter=1, Wait() blocks
Third:  Add(1) -> counter=2, Wait() blocks
First:  prints, defer Done() -> counter=1 (not 0)
-> counter stuck at 1 forever, Second and Third deadlocked
fatal error: all goroutines are asleep - deadlock!
```

(A different interleaving — `First` finishing before either `Add` runs — instead panics
with `sync: negative WaitGroup counter`. Same root cause, different symptom.)

**Where WaitGroup *would* work:** two separate one-shot `WaitGroup`s, each `Add(1)`'d once
upfront in the constructor (before any goroutine starts), one per gate — `firstDone` for
First→Second, `secondDone` for Second→Third. That's structurally identical to two Java
`CountDownLatch(1)`s and avoids the self-Add problem entirely. Not implemented here since
the channel and Cond versions already cover the two required styles, but worth knowing as
a third valid shape if `Add`-before-`Wait` ordering is guaranteed by construction rather than
by racing goroutines.

---

## Verified

Both `channel/main.go` and `condvar/main.go`: 300 runs via `stress.sh` with launch order
shuffled each run (0 failures), plus 10 runs under `-race` (0 races detected).
