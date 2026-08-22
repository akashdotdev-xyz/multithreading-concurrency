# Go Concurrency Primitives — Interview Cheat Sheet

What each primitive actually is, when to reach for it, and the specific things that trip
people up in interviews. Grounded in what we've actually hit so far: [day0-racedemo](day0-racedemo/main.go),
[phase1-print-in-order](phase1-print-in-order/NOTES.md).

---

## `chan` (channel)

**What it is:** a typed, built-in conduit for passing values between goroutines. Two flavors:
- **Unbuffered** (`make(chan T)`) — a send blocks until a receiver is ready. A rendezvous point.
- **Buffered** (`make(chan T, N)`) — a send only blocks once N un-received values are queued.

**Why / where:** Go's answer to "share memory by communicating" instead of "communicate by
sharing memory" — pass *ownership* of data through a channel rather than letting multiple
goroutines touch the same variable. Used for:
- Signaling / gating (our Print in Order solution — `chan struct{}` of size 1 per ordering gate)
- Producer-consumer pipelines (stage A sends, stage B receives)
- Fan-out / fan-in
- Timeouts and cancellation (`select` + `time.After`, or `context.Context`, which wraps a channel)

**Remember for interviews:**
- Send on a closed channel **panics**. Close an already-closed channel **panics**. Close a
  `nil` channel **panics**.
- Receive from a closed channel returns the zero value **immediately**, never blocks — check
  with `v, ok := <-ch`; `ok == false` means closed-and-drained.
- **Only the sender closes a channel, never the receiver** — closing from the receive side is
  a classic bug (a second, later send would then panic).
- A `nil` channel blocks forever on send *and* receive — useful deliberately, to disable a
  `select` case at runtime by nil-ing out the channel variable.
- A buffered channel of size N *is* a counting semaphore with N permits — if asked to
  implement a semaphore in Go with no external packages, `make(chan struct{}, N)` is the
  idiomatic answer (acquire = send, release = receive).
- `select` among multiple ready cases picks **pseudo-randomly**, not in source order — matters
  for fairness questions.
- No built-in semaphore/counting type in the standard library before you reach for
  `golang.org/x/sync` — the buffered-channel trick above is what you're expected to know cold.

---

## `sync.Mutex`

**What it is:** binary mutual-exclusion lock. `Lock()` / `Unlock()`.

**Why / where:** protect a critical section touched by multiple goroutines — the general tool,
reach for it when the critical section is more than a single primitive operation, or touches
several related variables that must change together consistently.

**Remember for interviews:**
- Zero value is ready to use — `var mu sync.Mutex`, no constructor.
- **Not reentrant.** Calling `Lock()` twice from the same goroutine deadlocks itself. This is
  the single biggest trap for people coming from Java (`synchronized` and `ReentrantLock` are
  both reentrant). Go has no recursive mutex in the standard library at all.
- Never copy a `Mutex` after first use — copying a struct that embeds one copies the lock
  state too, silently splitting one lock into two. `go vet`'s `copylocks` check catches this.
- Always `defer mu.Unlock()` immediately after `Lock()` — exception/panic-safe by construction.
  Forgetting this is bug catalog item #11 from the main plan (lock released non-exception-safely).
- `sync.RWMutex` is the reader-writer variant — `RLock`/`RUnlock` for readers, `Lock`/`Unlock`
  for the writer. Comes up directly in Phase 2.
- **Mutex vs. atomic:** if the critical section is literally one word being updated
  (a counter, a flag, a pointer), `sync/atomic` is faster and lock-free — proven directly in
  [abc.go](day0-racedemo/abc.go), where `mu.Lock(); counter += 1; mu.Unlock()` works but
  `counter.Add(1)` via `atomic.Int64` is the better answer for that exact shape.
- Holding a lock across a blocking call (I/O, a channel receive, another lock) is a classic
  anti-pattern — bug catalog item #9. Keep critical sections short and non-blocking.

---

## `sync.Cond`

**What it is:** a condition variable, always paired with a `Locker` (typically `*sync.Mutex`).
Three methods: `Wait()` (atomically unlock, park, re-lock on wake), `Signal()` (wake exactly
one waiter), `Broadcast()` (wake all waiters).

**Why / where:** "block until some predicate over shared state becomes true," for cases
channels don't fit naturally — especially when several *different* predicates share the same
underlying state (our `stage` int in [condvar/main.go](phase1-print-in-order/condvar/main.go)),
or the classic producer-consumer bounded buffer (`notFull` / `notEmpty`, coming in Phase 2).

**Remember for interviews:**
- Go's own docs call `Cond` a **last-resort** primitive — idiomatic Go reaches for channels
  first. Know it because interviewers ask for it directly (it maps cleanly onto Java's
  `wait`/`notify`), not because it's what you'd pick unprompted.
- `Wait()` **requires the lock to already be held** — it works by calling `c.L.Unlock()`
  internally, then re-`Lock()`ing before returning. Skip the lock and it crashes immediately
  with `sync: unlock of unlocked mutex` — proven directly, every run, when we tried it.
- **Always loop on the predicate**, never `if`: `for !ready { c.Wait() }`. A wakeup doesn't
  guarantee your specific condition is now true — it might have been meant for a different
  waiter. This is the Phase 1 "while loop rule" and bug catalog item #1, same thing.
- **`Signal()` vs `Broadcast()`:** `Signal()` wakes one arbitrary waiter — only safe when every
  waiter is blocked on the *same* predicate, so it genuinely doesn't matter who wakes.
  `Broadcast()` wakes everyone, each re-checks its own predicate; required when different
  waiters are waiting for different conditions on the same `Cond` (our `stage != 1` vs.
  `stage != 2`). Using `Signal()` there caused reproducible deadlocks.
- No built-in timeout. Unlike Java's `Condition.awaitNanos()`, `Cond.Wait()` has no deadline
  parameter — a timed wait needs a different pattern entirely (typically a channel + timer
  instead of `Cond`). Worth naming as a Go-specific gap if asked.
- Construct with `sync.NewCond(&mu)`, not a zero value — `Cond` needs its `Locker` set at
  creation.

---

## `sync.WaitGroup`

**What it is:** a decrement-only counter. `Add(n)` increments, `Done()` is `Add(-1)`, `Wait()`
blocks until the counter hits zero.

**Why / where:** "wait for N independent goroutines to finish" — fan-out/fan-in. It is **not**
a signaling/ordering tool between goroutines; it has no identity, only a count.

**Remember for interviews:**
- **The happens-before contract:** every `Add` with a positive delta must complete *before*
  the matching `Wait` observes it. In practice: call `Add` from the spawning goroutine,
  *before* launching each worker — never from inside the worker itself. We broke this
  deliberately (`Add` inside the child goroutine) and got two different real failures
  depending on timing: a **deadlock** (`all goroutines are asleep`) or a **panic**
  (`sync: negative WaitGroup counter`) — see [NOTES.md](phase1-print-in-order/NOTES.md) for
  the exact traced interleaving.
- It tells you *the count is currently zero* — nothing more. Don't try to make one shared
  `WaitGroup` represent multiple distinct ordering gates (e.g. "First is done" vs. "Second is
  done"); the counter can't distinguish why it reached a given value. If you need N separate
  ordering gates, use N separate one-shot `WaitGroup`s (each `Add(1)`'d once upfront) — that's
  structurally identical to N Java `CountDownLatch(1)`s.
- Never copy a `WaitGroup` after use — same `copylocks` rule as `Mutex`.
- It's reusable — after `Wait()` returns you can `Add()` again for a new "wave" — but only
  once you're certain no goroutine is still inside `Wait()` from the previous wave. Most code
  just constructs a fresh `WaitGroup` per wave instead of reusing one.
- No result channel built in — unlike Java's `ExecutorService.invokeAll`, a `WaitGroup` is
  purely a barrier. If you need each goroutine's result, pair it with a channel or a
  pre-sized slice indexed by goroutine number.

---

## Java equivalence table

Useful since the plan covers both languages — same underlying concepts, different names and
different edge-case behavior:

| Go | Closest Java equivalent | Where it differs |
|---|---|---|
| `chan` (buffered, size N) | `Semaphore(N)` | channel also carries a value, not just a permit |
| `chan struct{}` (size 1), used as a one-shot signal | `CountDownLatch(1)` | — |
| `sync.Mutex` | `synchronized` / `ReentrantLock` | **Go's is not reentrant**; Java's both are |
| `sync.RWMutex` | `ReentrantReadWriteLock` | — |
| `sync.Cond` | `Condition` (via `Lock.newCondition()`) / `Object.wait()`/`notify()` | Go's has no timed wait |
| `sync.WaitGroup` | `CountDownLatch` (roughly) | Go's is reusable via re-`Add`; `CountDownLatch` is strictly one-shot |
| `sync/atomic` types | `java.util.concurrent.atomic.*` | broadly the same idea |

---

## Recurring judgment call

Given a shared-state coordination problem, the fastest useful triage:

1. **Am I just passing ownership of data / signaling an event?** → `chan`
2. **Is the critical section a single primitive op (counter, flag, pointer)?** → `sync/atomic`
3. **Is the critical section more than one op, or touches multiple related variables?** → `sync.Mutex` (+ `sync.RWMutex` if reads vastly outnumber writes)
4. **Do I need to block until a predicate over shared state becomes true, especially with multiple distinct predicates on the same state?** → `sync.Mutex` + `sync.Cond`
5. **Am I just waiting for N independent goroutines to finish, no ordering between them?** → `sync.WaitGroup`
