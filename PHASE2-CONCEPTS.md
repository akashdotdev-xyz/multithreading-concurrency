# Phase 2 — Building Blocks: Concepts

Theory to internalize before writing code for: bounded blocking queue, thread pool, reader-writer
lock, thread-safe containers (stack/LRU/hash map), fixed-buffer FIFO. Language-agnostic —
implementation specifics (Go vs Java) are called out inline where they diverge.

---

## 1. Producer-Consumer / Bounded Buffer

**The shape:** N producers push items into a fixed-capacity buffer, M consumers pop them out.
Two invariants, not one:
- A consumer must block if the buffer is **empty** (nothing to take).
- A producer must block if the buffer is **full** (capacity `n`, already holding `n` items).

This is the single most-reused shape in the whole plan — it's the engine underneath the
bounded queue (2.1), the thread pool's task queue (2.2), and the connection pool (Phase 4).

### Two-condition pattern vs. one-condition + `notifyAll`/`Broadcast`

Two ways to signal the two invariants above:

**Two conditions** (`notFull`, `notEmpty`) — a producer that just added an item only wakes
consumers (`notEmpty.signal()`), never producers; a consumer that just removed an item only
wakes producers (`notFull.signal()`). Precise: only the goroutines/threads that could
possibly have a newly-true predicate get woken.

**One condition + broadcast-to-all** — a single condition variable, and every producer/consumer
wakes *everyone* on every state change, each re-checking its own predicate (`while` loop rule
applies exactly as in Phase 1).

**Both are correct.** The difference is purely performance: with one condition, a producer that
adds an item wakes up other *producers* too (who were waiting on "not full") even though
nothing about "not full" changed for them beyond one slot — they wake, recheck, find it still
false (buffer might still be full if consumers are slow), and go back to sleep. Wasted
wakeups, worse under contention. This is exactly the tradeoff your notes call out:
*"why one condition + notifyAll is correct-but-slower."* Two conditions is the answer to give
first; know you can name the perf cost of the simpler version as a deliberate simplification if
asked to justify it.

**Go specifics:** `sync.Cond` only gives you *one* condition per `Cond` — for two independent
conditions on the same lock you need **two separate `sync.Cond`s sharing the same `*sync.Mutex`**
(`notFull := sync.NewCond(&mu)`, `notEmpty := sync.NewCond(&mu)`). This maps directly onto
Java's `lock.newCondition()` called twice on the same `ReentrantLock`.

### `offer(item, timeout)` / `poll(timeout)`

Timed variants of push/pop that give up and return failure after a deadline instead of
blocking forever. The subtlety: a spurious/misdirected wakeup can happen *and* the timeout can
expire in the same window — the loop has to re-check the predicate **and** re-check remaining
time on every wake, not just one or the other. In Go this is `cond.Wait()` doesn't support
timeouts at all (noted in [PRIMITIVES.md](PRIMITIVES.md)) — the idiomatic timed-wait pattern
uses a channel + `time.After` + `select` instead, or a helper goroutine that broadcasts after a
timer fires. Worth sketching this gap explicitly if asked to implement `poll(timeout)` in Go
with a `Cond`-based queue — it's a real structural limitation, not just extra ceremony.

### Bounded by bytes, not item count

Same two-condition machinery, but capacity is checked against `sum(len(item) for item in buffer)`
instead of `len(buffer)`. The only real wrinkle: a single item larger than the *entire* buffer
capacity can never be admitted — decide up front whether that's a hard error or the queue special-
cases "buffer is empty AND this item still doesn't fit" as an allowed admission (rare in practice,
but an interviewer may probe it).

### `size()` without holding the lock for the whole call

If `size()` just reads a single already-synchronized counter (e.g., an `atomic.Int64` you
maintain alongside the buffer, incremented/decremented under the same lock as push/pop), you
can read it with a single atomic load — no need to hold the main lock for the duration of the
call, since you're not touching the buffer itself, just one already-consistent number. This is
a good "why is this safe" follow-up: the reason a bare atomic read suffices here (vs. needing
the full lock) is that `size()` doesn't need to observe a value *consistent with the rest of the
buffer's state at that instant* — a caller has no way to use a `size()` result atomically with
a subsequent operation anyway (it could be stale the instant it returns), so a relaxed,
lock-free read costs nothing in correctness and saves contention.

---

## 2. Thread Pool Anatomy

**The shape:** a fixed (or dynamic) set of worker goroutines/threads, each running a loop:
pull a task off a shared queue, run it, repeat. The queue is exactly the bounded buffer from
§1 — this is why your notes chain bounded queue (round 1) into thread pool (round 2) at Rubrik;
it's genuinely the same primitive with a worker loop bolted on.

### Exception isolation

**The failure mode to avoid:** an unhandled panic/exception inside a task kills the worker
goroutine/thread entirely — the pool silently shrinks by one every time a task misbehaves, and
eventually has zero live workers with tasks still queued, and nothing tells you why. The fix:
wrap the task execution itself in a `recover()` (Go) / `try-catch` (Java) **inside the worker
loop**, not around the whole loop — record the failure (into whatever `Future`-equivalent the
caller is holding), then **keep looping**. The worker must survive its task's failure.

### Task timeout

Two different things people conflate: (a) a task that's *allowed* to run for at most T before
being cancelled, vs. (b) a caller's `get()` on a `Future` giving up waiting after T even though
the task keeps running in the background. (b) is easy (a timed wait on the per-task
completion signal). (a) is hard in general — you can't forcibly kill an arbitrary running
goroutine in Go (no `Thread.stop()` even in Java, deliberately removed as unsafe); the task
itself has to cooperatively check a cancellation signal (a `context.Context` in Go, an
`interrupt` check in Java) at safe points.

### Idle thread reclamation

Dynamic pool sizing: workers beyond some "core" count that sit idle for longer than a
keep-alive window terminate themselves. Implemented as: worker's "pull a task" step becomes a
*timed* wait on the queue instead of an unbounded one; if the timeout fires with no task
available and this worker is above the core count, it exits its loop instead of retrying.

### Graceful `shutdown()` vs. `shutdownNow()` — poison pill

- **Graceful:** stop accepting new tasks, let already-queued tasks drain, then stop. Classic
  implementation: push a sentinel "poison pill" value per worker onto the queue after the last
  real task; a worker that pulls a poison pill exits instead of executing it. Guarantees every
  queued task runs first, since the pill is FIFO-ordered behind them.
- **Immediate (`shutdownNow`):** drop everything still queued, signal all workers to stop
  after their *current* task (can't interrupt mid-task without cooperative cancellation, same
  caveat as task timeout above), return the dropped tasks to the caller.

### `Future` / blocking `get()`

Each submitted task gets a small per-task object holding: a result slot, an error slot, and a
completion signal — the completion signal is a one-shot latch (in Go, a `chan struct{}` that
gets `close()`d on completion — closing rather than sending is the idiom specifically *because*
`close()` lets **any number of `get()` callers** all unblock via receive-from-closed-channel,
whereas a single send only ever unblocks one receiver). `get()` blocks on that signal, then
reads the result/error slots — safe to read without further locking because the happens-before
edge from the completion signal (mutex unlock, or channel close) already guarantees the result
write is visible.

### Rejection policies (queue full, pool at capacity)

- **Abort** — reject immediately, caller gets an error/exception.
- **Caller-runs** — the calling goroutine/thread executes the task itself, synchronously. Self-
  throttling: it naturally slows down whoever's submitting work faster than the pool can drain.
- **Discard** — silently drop the task (rarely the right default; know it exists, wouldn't
  reach for it unprompted).

---

## 3. Reader-Writer Locks

**The invariant:** any number of concurrent readers, OR exactly one writer, never both, never
multiple writers.

### The three variants — build all three, know the tradeoff of each

1. **Reader-preference.** A writer waits until *all current readers* finish, but new readers
   arriving while a writer waits are still admitted ahead of it. Simple to implement (a reader
   count + a lock), but **starves writers** under sustained read load — say this out loud
   unprompted, it's exactly the kind of volunteered failure-mode your notes flag as the
   strongest signal.
2. **Writer-preference.** Once a writer is waiting, new readers block behind it (existing
   in-progress readers still finish). Fixes writer starvation, but now a steady stream of
   writers can starve readers instead — you've moved the unfairness, not removed it.
3. **Fair / FIFO.** A single waiting queue for both readers and writers; whoever arrived first
   goes first (readers arriving together can still batch-proceed together). No starvation
   either direction, at the cost of extra bookkeeping (a queue, not just a counter) and readers
   no longer maximally parallel (a reader behind a waiting writer in FIFO order blocks even
   though no writer is *currently* active).

### Follow-ups to have ready

- **Upgrading a read lock to a write lock is a deadlock trap.** If two readers both hold the
  read lock and both try to upgrade to a write lock, each is waiting for the other to release
  its read lock first — neither ever will, since both are blocked trying to acquire the write
  lock while still holding a read lock. The safe pattern is to fully release the read lock,
  then acquire the write lock fresh (accepting that another writer could sneak in between,
  so you must re-validate whatever you originally read).
- **Reentrancy.** Can the same thread that holds a read lock acquire it again (nested reads)?
  Can a thread holding a write lock also acquire a read lock (write implies read access)?
  Java's `ReentrantReadWriteLock` supports both with specific rules; a from-scratch
  implementation needs to explicitly track owner identity per lock, not just a count, if you
  want reentrancy — plain counters can't distinguish "one thread reentering" from "two threads
  both holding."
- **When an RW lock is slower than a plain mutex.** RW locks have real overhead (tracking
  reader counts, managing two wait sets) beyond a mutex's single bit. For a **short** critical
  section, or a **write-heavy** workload, that overhead isn't amortized by any actual read
  parallelism gained — a plain `Mutex` wins. RW locks pay off specifically when critical
  sections are long-ish *and* reads dominate writes by a wide margin.

---

## 4. Lock Granularity

**Coarse-grained:** one lock protects a large structure (e.g., one mutex for an entire hash
map). Simple, easy to reason about, but every operation serializes against every other,
including ones touching unrelated parts of the structure.

**Fine-grained / lock striping:** split the structure into independent segments, each with its
own lock (e.g., a hash map with 16 stripes, each stripe's lock protecting only the buckets that
hash into it). Operations on different stripes proceed fully in parallel; only operations that
happen to collide on the *same* stripe serialize. This is exactly how a from-scratch concurrent
hash map differs from wrapping a plain map in one mutex (`synchronizedMap` in Java is
coarse-grained — know this distinction cold, it's specifically called out in your notes as
*"why synchronizedMap is not the same thing"* as a real concurrent hash map).

**The tradeoff to name unprompted:** more stripes = more parallelism, but also more memory
overhead (N locks instead of 1) and more complexity for any operation that needs to span
multiple stripes atomically (e.g., a global `size()` or an iteration that must see a consistent
snapshot — striping makes both harder, sometimes requiring you to lock all stripes at once,
which temporarily degrades back to coarse-grained behavior).

---

## 5. `volatile` / Memory Visibility

**The bug:** a plain flag, written by one thread, read in a loop by another — `while (!ready) {}`
— can loop **forever**, even though "obviously" the other thread set `ready = true`. Not a
hypothetical: without a synchronization primitive establishing a happens-before edge, the
compiler is free to cache the read in a register and never re-read memory at all, and the CPU
is free to reorder the write. This is the exact bug shape we proved out empirically earlier
this session (see the `stage` field in
[phase1-print-in-order/condvar](go/phase1-print-in-order/condvar/main.go)) — that discussion's
"reason 2" (memory-model visibility) is this concept, generalized. Bug catalog item #6.

**The fix is never "add more locking around just the read"** in isolation — it's using a
primitive that the memory model actually guarantees visibility through:
- Java: mark the field `volatile` (guarantees visibility + prevents reordering around it, but
  is **not** a substitute for atomicity of compound operations like `count++` — `volatile` and
  `AtomicInteger` solve different problems and get conflated constantly).
- Go: there is no `volatile` keyword at all. Visibility only comes from actual synchronization
  — a channel send/receive, a mutex lock/unlock, or `sync/atomic`. A plain `bool` field with no
  synchronization is *always* a race in Go, full stop; there's no lightweight "just make reads
  visible" escape hatch the way `volatile` offers in Java.

**Double-checked locking without `volatile`** (bug catalog item #7) is the sharpest version of
this bug: the classic "check flag, lock, check flag again, initialize, set flag" lazy-init
pattern is broken if the flag isn't `volatile`/atomic, because another thread can observe the
flag as `true` **before** the object it guards is fully constructed — the write to the flag and
the writes that construct the object can be reordered relative to each other from a second
thread's point of view. Worth being able to draw this one on a whiteboard from memory.

---

## 6. CAS, the ABA Problem, and Optimistic Concurrency

**Compare-and-swap (CAS):** a single hardware instruction — `CAS(address, expected, new)` —
that atomically checks whether the current value at `address` equals `expected`, and if so
sets it to `new`, all as one indivisible step; returns whether it succeeded. This is the
primitive underneath every `atomic.*` type in both Go and Java.

**Optimistic vs. pessimistic concurrency**, the framing that CAS enables:
- **Pessimistic** (locks): assume conflict is likely — acquire exclusive access *before* touching
  shared state, so no one else can interfere while you work.
- **Optimistic** (CAS loops): assume conflict is rare — read the current state, compute the new
  state *without* holding anything, then CAS it in; if the CAS fails (someone else changed it
  first), retry the whole read-compute-CAS cycle. No thread ever blocks another; failure just
  means redo the work.

Canonical optimistic pattern (this is what "lock-free stack with CAS" actually is):
```
loop:
    old := head.Load()
    newNode.next = old
    if head.CompareAndSwap(old, newNode) { break }   // else: someone else pushed first, retry
```

**The ABA problem:** CAS only checks "is the value still equal to what I last read" — not "has
this value definitely been untouched since I read it." If a value changes from A to B and back
to A between your read and your CAS, the CAS succeeds (it sees A, as expected) even though the
underlying state was mutated in between and your assumptions about *why* it's A may no longer
hold. Concretely for a lock-free stack: thread 1 reads `head == NodeA` (next: NodeB). Thread 2
pops NodeA, pops NodeB, then pushes NodeA back onto an empty stack — `head` is `NodeA` again,
satisfying thread 1's CAS, but NodeA's `next` pointer no longer points at NodeB (which may have
been freed/reused) — thread 1's CAS "succeeds" and corrupts the stack, pointing `head.next` at
stale memory.

**The fix:** don't compare the pointer alone — pair it with a **version counter** (a "stamped"
or "tagged" pointer/reference) that increments on every modification, and CAS the `(pointer,
version)` pair together. Even if the pointer coincidentally returns to the same value, the
version won't match, so the CAS correctly fails and forces a retry with fresh state. Java's
`AtomicStampedReference` exists specifically for this; Go has no built-in equivalent — you'd
hand-roll a struct combining a pointer and a counter, updated together via a single
`atomic.Value` or `atomic.Pointer` swap of the whole pair, since Go's `CompareAndSwap` only
operates on one word at a time.

---

## 7. Fixed-Buffer FIFO Queue (senior-level)

**The constraint that makes this hard:** no dynamic allocation — a fixed-size array (or
pre-allocated byte buffer), used as a **circular buffer**. Two classic arithmetic approaches:

- **Size counter alongside head/tail indices.** `head`, `tail`, and an explicit `count`. Full
  when `count == capacity`, empty when `count == 0`. Simplest to reason about; the counter
  itself needs to be updated atomically alongside head/tail under whatever lock protects the
  buffer.
- **One-slot-empty trick.** No separate counter — reserve one slot that's never used, so
  `head == tail` unambiguously means empty, and `(tail + 1) % capacity == head` means full.
  Saves a field, costs one slot of usable capacity, and is a bit more subtle to get the modular
  arithmetic right on (easy to off-by-one the full/empty check when first implementing it).

**The follow-up — two logical queues sharing one buffer, each keeping its own FIFO order:**
this is a genuinely different problem from a single circular buffer, not just "do it twice."
You need a **movable boundary** between the two queues' regions within the same backing array,
rebalanced as usage skews — sketch the data layout (which region belongs to which queue,
where the boundary pointer lives, what happens when one queue wants to grow into space
currently owned by the other) on paper *before* writing any code. This is exactly the kind of
question where jumping straight into arithmetic without a diagram is how candidates get stuck
mid-interview — draw the array as a strip, mark head/tail for each queue, and reason about the
boundary move as its own atomic step before deciding how to implement it.

---

## Quick self-check before moving to problems

You should be able to answer each of these out loud, in under a minute, before starting
implementation:

1. Why does two-condition beat one-condition-plus-broadcast, given both are correct?
2. Why can't `Cond.Wait()` support a timeout the way Java's `Condition.awaitNanos()` does —
   what would you build instead in Go?
3. Trace the exact interleaving that makes reader-preference starve a writer.
4. Why is upgrading a read lock to a write lock a deadlock trap?
5. Why is `synchronizedMap` not the same thing as a real concurrent hash map?
6. Draw the double-checked-locking bug from memory — where exactly does it break without
   `volatile`/atomic?
7. Walk through the ABA problem on a lock-free stack with a concrete 3-step interleaving.
8. Why does closing a channel (not sending on it) let multiple `Future.get()`-style callers
   all unblock?
