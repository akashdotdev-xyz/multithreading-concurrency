# LC 1115 — Print FooBar Alternately

**Invariant:** on each of `n` rounds, `Foo` prints before `Bar`, and `Bar` doesn't print again
until the *next* round's `Foo` has printed. Unlike Print in Order (one-shot sequence), this is
**cyclic** — the same two gates get reused `n` times.

---

## Solution 1 — Mutex + Cond (`condvar/main.go`)

```go
type FooBar struct {
    n     int
    mu    sync.Mutex
    cond  *sync.Cond
    stage int64
}

func (fb *FooBar) Foo(printFoo func()) {
    for i := 0; i < fb.n; i++ {
        fb.mu.Lock()
        for fb.stage%2 != 0 {
            fb.cond.Wait()
        }
        printFoo()
        fb.stage += 1
        fb.mu.Unlock()
        fb.cond.Broadcast()
    }
}
// Bar mirrors Foo, waiting for stage%2 == 1
```

**The parity trick:** a monotonically increasing `stage`, checked mod 2, instead of a bool
that flips each round. Functionally the same as a toggle, but generalizes cleanly to N-way
rotation (`%N`) — relevant for Zero-Even-Odd next.

**`Broadcast()` called after `Unlock()`, not before.** Deliberately outside the critical
section — Go's docs explicitly allow this. Avoids waking a goroutine only to have it
immediately block again on a lock the broadcaster is still holding. Still race-free: the
state mutation (`stage += 1`) happens *before* `Unlock()`, and any goroutine about to `Wait()`
is holding the lock while it checks the predicate, so it cannot be mid-check at the exact
moment the other side mutates and broadcasts.

---

## Solution 2 — channels (`channel/main.go`)

```go
type FooBar struct {
    n      int
    evenCh chan struct{}
    oddCh  chan struct{}
}

func NewFooBar(n int) *FooBar {
    f := &FooBar{n: n, evenCh: make(chan struct{}, 1), oddCh: make(chan struct{}, 1)}
    f.evenCh <- struct{}{} // seed: Foo goes first
    return f
}

func (fb *FooBar) Foo(printFoo func()) {
    for i := 0; i < fb.n; i++ {
        <-fb.evenCh
        printFoo()
        fb.oddCh <- struct{}{}
    }
}
// Bar mirrors Foo, draining oddCh and refilling evenCh
```

Baton-passing: `evenCh` pre-loaded with one token so `Foo` can start immediately. Each round,
whoever's turn it is drains their channel, does the work, refills the *other* channel. Same
size-1-buffered-channel-as-binary-semaphore idea as Print in Order, but the token is passed
back and forth `n` times instead of fired once — that's the structural difference between a
one-shot ordering constraint and a cyclic one. No mutex, no shared int — cleaner fit than
`Cond` for this specific problem shape.

---

## Verified

Both variants: 300 runs at n=2000 via `stress.sh` (0 failures), 10 runs under `-race`
(0 races).
