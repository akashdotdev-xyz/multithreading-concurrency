# Concurrency & Multithreading Interview Prep — Holistic Plan

A 6-week, problem-driven plan covering the LeetCode Concurrency set plus the questions
candidates actually reported being asked (Rubrik, Confluent, Salesforce, LinkedIn, Netflix,
Databricks, MongoDB, Pure Storage).

**Core principle: concepts are learned *through* problems, not before them.** Each phase
introduces exactly the primitives its problems require. Don't read ahead — you'll retain
almost nothing until a problem forces you to use it.

---

## 0. Setup (Day 0, ~2 hours)

### Pick one language and commit

| Language | Best if | Primitives you'll live in |
|---|---|---|
| **Java** | Interviewing at Rubrik / Confluent / Salesforce / LinkedIn in India | `synchronized`, `ReentrantLock`, `Condition`, `Semaphore`, `CountDownLatch`, `CyclicBarrier`, `AtomicInteger`, `ExecutorService` |
| **C++** | Systems / storage / infra roles | `std::mutex`, `unique_lock`, `condition_variable`, `atomic`, `counting_semaphore` (C++20), `jthread` |
| **Python** | You're fastest here and the role isn't perf-critical | `threading.Lock`, `RLock`, `Condition`, `Semaphore`, `Event`, `Barrier`, `queue.Queue` |
| **Go** | The company is Go-first | goroutines, channels, `sync.Mutex`, `sync.WaitGroup`, `sync.Cond`, `select` |

> **Warning on Python:** the GIL means your code won't demonstrate real parallelism, and
> some interviewers will probe this. Know the answer: the GIL protects interpreter state,
> not *your* invariants — you still need locks for correctness. Threads still help for
> I/O-bound work.
>
> **Warning on Go:** channels make many classic problems trivial, which can *reduce* signal.
> Several reported rounds explicitly banned library helpers. Be ready to solve with
> `sync.Mutex` + `sync.Cond` too.

### Environment

Set up a scratch project where you can run multithreaded code **and** run it 1000× in a loop.
A concurrency bug that appears 1 in 200 runs is the norm, not the exception.

```bash
# Java
for i in $(seq 1 500); do java Main || echo "FAILED at $i"; done

# Python
for i in $(seq 1 500); do python main.py || echo "FAILED at $i"; done
```

Add stress helpers to your template from day one: random `sleep`s at critical points,
many more threads than cores, and an assertion on the invariant after every run.

### Ground rules for the whole plan

1. **Write runnable code, not pseudocode.** Multiple reported rounds required executing code
   in a shared IDE (CoderPad). Pseudocode reads as "hasn't actually done this."
2. **Solve single-threaded first, then add concurrency.** This is the most repeated piece of
   Rubrik-specific advice. Jumping straight to locks reads as pattern-matching.
3. **Every problem gets solved twice** — once with lock + condition variable, once with
   semaphores. The "now do it without X" follow-up is standard.
4. **State your invariant out loud before you write a lock.** "At most N items in the buffer,"
   "no two groups inside simultaneously." The lock exists to protect the invariant; if you
   can't name it you can't defend the lock.

---

## Phase 1 — Ordering & Signaling (Week 1)

**Goal:** stop fearing `wait`/`notify`. Build the reflex that a thread blocks until a
*predicate* is true, not until it is "notified."

### Concepts (learn while solving)

- Race condition, critical section, mutual exclusion, atomicity
- Mutex vs. binary semaphore — the ownership difference
- Condition variables: `wait` / `signal` / `broadcast`
- **The `while` loop rule** — why `if (!ready) wait();` is a bug
- Spurious wakeups and lost wakeups
- Counting semaphores as permits

### Problems

| # | Problem | What it teaches | Do it twice with |
|---|---|---|---|
| LC 1114 | Print in Order | Happens-before, simple gating | Semaphore / CountDownLatch |
| LC 1115 | Print FooBar Alternately | Two-thread ping-pong | 2 binary semaphores / lock+condition |
| LC 1116 | Print Zero Even Odd | 3-way turn-taking with shared state | 3 semaphores / single condition + state var |
| LC 1195 | Fizz Buzz Multithreaded | 4 threads on one counter | Semaphores / condition + predicate |
| LC 1117 | Building H2O | **Barrier / rendezvous** — group N before release | Semaphores + barrier / CyclicBarrier |

### Must be able to answer

- Why is the `while` loop around `wait()` mandatory? Give a concrete failure trace with `if`.
- `notify()` vs `notifyAll()` — when does `notify()` cause a **lost wakeup deadlock**?
- In Building H2O, after two H and one O are released, what stops a *fourth* H from racing
  ahead into the next molecule?
- What is a lost wakeup, and how does a semaphore's internal count prevent it where a bare
  condition variable does not?

### Phase 1 exit criteria

You can write a correct alternating-print from scratch in under 6 minutes, in two different
styles, and explain the failure mode of each shortcut.

---

## Phase 2 — Building Blocks (Weeks 2–3) ⭐ **Highest ROI phase**

This is where the majority of reported interview questions live. Rubrik chained these across
consecutive rounds: bounded queue in round 1, thread pool built on it in round 2.

### Concepts

- Producer–consumer, bounded buffer
- Two-condition pattern (`notFull` / `notEmpty`) and why one condition + `notifyAll` is
  correct-but-slower
- Thread pool anatomy: worker loop, task queue, shutdown, exception isolation
- Reader-writer locks: reader preference vs. writer preference vs. fair
- Writer starvation
- Lock granularity; coarse vs. fine-grained locking
- `volatile` / memory visibility — why a plain flag can loop forever

### 2.1 Bounded Blocking Queue

> **LC 1188** (Premium) — and reported directly at Rubrik: *"Implement a bounded thread-safe queue."*

Build with `ReentrantLock` + two `Condition`s. **Do not use `LinkedBlockingQueue`.**

**Follow-ups to prepare:**
- Multiple producers *and* multiple consumers (single-condition versions break here — know why)
- `offer(item, timeout)` and `poll(timeout)` — implement with `awaitNanos`, and handle the
  spurious-wakeup-plus-timeout interaction correctly
- Bounded by total *bytes* rather than item count
- Make `size()` correct without holding the lock for the whole call

### 2.2 Thread Pool / Task Executor

> Reported at Rubrik as the round-2 follow-on: *"Implement a thread pool task executor"*,
> **with `ExecutorService` explicitly banned.**

Build: N worker threads pulling from your Phase-2.1 queue.

**The follow-ups they actually asked:**
- **How do you handle an exception thrown inside a task?** (Worker must not die. Catch at the
  worker loop boundary, record into the future, keep looping.)
- **How do you implement a timeout on a task?**
- **How do you reclaim / retire an idle thread?** (Dynamic pool sizing, keep-alive time)
- Graceful `shutdown()` vs. `shutdownNow()` — the poison-pill pattern
- Return a `Future` — implement `get()` blocking on a per-task latch
- What happens when the queue is full? (Rejection policies: abort, caller-runs, discard)

### 2.3 Reader-Writer Lock

> Reported at Rubrik: *"Implement reader writer lock. Various cases and how to handle that."*
> One candidate failed this round and had to redo the loop — treat it as high-stakes.

**Build all three variants:**
1. Reader-preference (simple, **starves writers** — say this out loud unprompted)
2. Writer-preference (blocks new readers once a writer waits)
3. Fair / FIFO (queue-based)

**Follow-ups:** upgrade a read lock to a write lock (why is this a deadlock trap?),
reentrancy, and when a RW lock is actually *slower* than a plain mutex (short critical
sections, write-heavy workloads).

### 2.4 Thread-Safe Containers

> Reported at Rubrik: *"Implement a stack using linked list; make it thread-safe. Push and
> pop O(1)"* — then *"now implement it with CAS."* Also: LRU cache appears on the
> commonly-asked list.

- Thread-safe stack (linked list, O(1))
- **Lock-free stack with CAS** — this is where a candidate got stuck; don't be that candidate
- Thread-safe LRU cache (HashMap + doubly linked list under a lock; then discuss sharding)
- Concurrent hash map — bucket/stripe locking, why `synchronizedMap` is not the same thing

**Concepts to pick up here:** compare-and-swap, the **ABA problem**, atomic references,
optimistic vs. pessimistic concurrency, lock striping.

### 2.5 Fixed-Buffer FIFO Queue *(senior-level, Rubrik)*

> *"Implement a FIFO queue over a fixed-size int buffer. No dynamic memory allocation."*
> Follow-ups: multi-thread access → **two logical queues sharing one buffer**, each keeping
> its own FIFO order, buffer 100MB–1GB+, dynamically rebalanced by usage, minimize waste.

Circular buffer arithmetic, head/tail with a size counter (or one-slot-empty trick), then
a split-buffer design with a movable boundary. This is a genuinely hard design question —
sketch the data layout before writing any code.

### Phase 2 exit criteria

From a blank file, no libraries: bounded queue in ~15 min, thread pool on top in ~20 min,
reader-writer lock in ~15 min, and you can name the starvation failure of each.

---

## Phase 3 — Schedulers & Task Graphs (Week 4) ⭐ **Most-repeated question**

Reported at Rubrik ×3 and Confluent ×2. If you only prepare one thing, prepare this.

### Concepts

- Topological sort / dependency DAG under concurrency
- Delay queue / min-heap keyed by execution time
- Timer thread + condition variable with timed wait
- Fan-out / fan-in, `CountDownLatch` and `CyclicBarrier`
- Cycle and deadlock detection in a task graph

### 3.1 Concurrent Job Scheduler with Dependencies

> Rubrik L4, 2025: *"Design a job scheduler that runs all available jobs in an optimized
> manner, considering job dependencies. Assume `get_next_jobs(finished_jobs)` returns all
> jobs whose dependencies are fully satisfied. A job runs only after all its parents complete."*
>
> Rubrik entry-level (near-identical): given an API returning a task's dependencies and
> another that runs a task, execute concurrently and **discuss trade-offs, race conditions,
> and deadlock.**
>
> Confluent: *"design and code a problem to schedule jobs"* — twice, independently reported.

**Build it:** worker pool + ready queue + atomic in-degree counters. When a job finishes,
decrement each dependent's counter; the thread that drives it to zero enqueues it.

**Nail these:**
- Where is the race? (Two parents finishing simultaneously both seeing in-degree 1 → the job
  runs twice, or never. Fix with an atomic decrement-and-test.)
- Termination: how do workers know *everything* is done vs. merely momentarily empty?
- A cycle exists — how do you detect it rather than hang?
- One job fails: fail-fast, skip descendants, or retry?
- Priorities, or a max-parallelism cap per job type

### 3.2 ScheduledExecutorService

> Rubrik senior screen: *implement `schedule(Runnable, delay, unit)` and
> `scheduleAtFixedRate(Runnable, initialDelay, period, unit)`.*

Min-heap by next-run-time + a single scheduler thread doing a **timed wait on a condition**,
handing due tasks to a worker pool.

**Nail these:** why `wait(timeout)` on the head's delay rather than a busy poll or
`sleep(1)`; what happens when a new earlier task arrives while the scheduler is waiting
(you must signal to re-evaluate); `scheduleAtFixedRate` vs. `scheduleWithFixedDelay`; a task
that runs longer than its period.

### 3.3 Multithreaded Web Crawler

> **LC 1242** (Premium). Reported at Rubrik as a DSA round with a multithreading follow-up,
> and it's on the commonly-asked list.

Concurrent visited-set (`putIfAbsent` as the atomic claim), work queue, hostname filtering,
and — the real difficulty — **knowing when to stop.** Track in-flight tasks, not just an
empty queue.

---

## Phase 4 — Group Mutual Exclusion & Starvation (Week 5)

A whole family of reported questions that are the *same problem* wearing different costumes.
Recognize the shape and you've solved all of them.

**The shape:** a shared resource, capacity N, and members of different groups may not mix.
Follow-up is *always* "now prevent starvation."

### The variants (all reported)

| Disguise | Source | Constraint |
|---|---|---|
| Political bathroom (Democrats/Republicans) | Rubrik "mostly asked" list | Capacity 3, no mixed groups |
| Playground | Rubrik L4 2025 | One team at a time, max 10 players |
| Uber ride | Educative (recruiter-recommended by Confluent) | 4 seats, no group in minority |
| Building H2O | LC 1117 | Exactly 2 H + 1 O per group |
| Unisex bathroom | Little Book of Semaphores | Classic form |

Solve **one** carefully with the lightswitch/turnstile pattern, then map the rest onto it.

### Also in this phase

- **Dining Philosophers (LC 1226)** — deadlock avoidance: resource ordering, arbitrator,
  limit to N−1 diners. Know all three and their trade-offs.
- **Thread-safe parking allocator** *(Rubrik)*: cars take 1 unit, trucks take 2, in a 1-D
  array. Contiguous allocation + fragmentation under concurrency.
- **Logical memory unit / page cache** *(Rubrik)*: CPU requests page by ID; return from
  logical memory if present, else fetch from physical. This is a **thread-safe cache with
  expensive misses** — the real question is preventing N threads from all fetching the same
  missing page (per-key locking, or a "loading" placeholder future).
- **Connection pool** *(Salesforce LMTS)*: limited connections, many requests. Blocking queue
  for the pool, default timeouts, and the follow-up about separate open/closed queues when
  connection counts get large.
- **Ticket booking / BookMyShow** *(Rubrik G5, LinkedIn)*: two users, one seat. Optimistic
  locking, seat holds with expiry, idempotency.

### Concepts

Starvation, livelock, fairness, the lightswitch pattern, turnstiles, priority inversion,
deadlock's four Coffman conditions and how each fix breaks one.

---

## Phase 5 — Bug Squash, Memory Model & Theory (Week 6)

Reported as its own round format: you're handed broken multithreaded code and asked to find
the bugs.

> Rubrik: C++ producer/consumer where threads contend over a critical section — find the bug.
> Rubrik CPD: a banking app with parallel threads — find bugs, propose fixes. The successful
> answer included a **read-write lock and a concurrent hash map** for performance.

### Build a bug catalog — be able to spot each in under 60 seconds

1. `if` instead of `while` around `wait()`
2. Check-then-act (TOCTOU) — `if (!map.containsKey(k)) map.put(k, v)`
3. Non-atomic compound ops — `count++`
4. Inconsistent lock ordering → deadlock
5. Publishing `this` from a constructor
6. Unsynchronized read of a flag → infinite loop (missing `volatile`)
7. Double-checked locking without `volatile`
8. `notify()` where `notifyAll()` is required
9. Lock held across a blocking I/O call
10. Mutating a collection while another thread iterates it
11. Releasing a lock in a non-exception-safe way (missing `finally` / RAII)
12. Sharing a non-thread-safe object (`SimpleDateFormat`, `Random`) across threads

### Memory model concepts (senior signal)

`volatile` / `atomic` semantics, happens-before, instruction reordering, cache coherence,
memory barriers and fences, sequential consistency vs. relaxed ordering, false sharing.
Blind reports these come up specifically for senior candidates.

### Theory rapid-fire

Process vs. thread; user vs. kernel threads; context switch cost; thread lifecycle;
mutex vs. semaphore vs. monitor; `synchronized` vs. `ReentrantLock`; when concurrency
*hurts*; Amdahl's law; I/O-bound vs. CPU-bound sizing; thread-per-request vs. event loop.

---

## Phase 6 — Simulation (ongoing, from Week 3)

Once a week from Week 3, run a timed 45-minute mock:

- Pick a problem from Phases 2–4 you haven't done in a week
- Talk out loud the whole time
- Write **running** code, plus a `main` that stresses it with 50 threads
- Have someone (or an AI) throw two follow-ups at you in the last 10 minutes

### The 45-minute round structure

| Minutes | Do this |
|---|---|
| 0–5 | Clarify: how many threads? blocking or non-blocking? fairness needed? bounded? |
| 5–8 | **State the invariant.** Sketch the single-threaded structure. |
| 8–12 | Say where the races are *before* writing the lock. Name your primitives and why. |
| 12–35 | Code it. Narrate. Run it. |
| 35–45 | Stress test. Then volunteer: starvation risk, fairness, lock granularity, trade-offs. |

**Volunteering the failure modes unprompted is the strongest signal you can send.**
"This version starves writers — if you want writer-preference I'd add a waiting-writer
counter" earns more than a silently perfect solution.

---

## Recurring Patterns Cheat Sheet

Almost everything above reduces to one of these six:

| Pattern | Primitive | Appears in |
|---|---|---|
| **Gate / ordering** | Semaphore(0), latch | Print in Order, task dependencies |
| **Turn-taking** | N semaphores in a cycle, or condition + state | FooBar, Zero-Even-Odd, FizzBuzz |
| **Bounded buffer** | Lock + `notFull`/`notEmpty` | Blocking queue, thread pool, connection pool |
| **Barrier / rendezvous** | Counter + broadcast, CyclicBarrier | H2O, fan-in, molecule grouping |
| **Lightswitch** | Counter + mutex, first-in-locks / last-out-unlocks | RW lock, bathroom, playground, Uber ride |
| **Counter to zero → fire** | Atomic decrement-and-test | Job scheduler DAG, latches |

---

## Priority Order (if you're short on time)

Ranked by reported frequency across all sources:

1. **Job scheduler with dependencies** — Rubrik ×3, Confluent ×2
2. **Bounded blocking queue → thread pool** (no library primitives)
3. **Reader-writer lock**, all three fairness variants
4. **Thread-safe container** with an O(1) constraint + CAS follow-up
5. **Group mutual exclusion** + starvation (bathroom / playground / Uber ride)
6. **Bug squash** on supplied multithreaded code
7. **Multithreaded web crawler**
8. LC 1114/1115/1116/1117/1195/1226 as warm-ups

---

## Progress Tracker

Mark a problem done only when: solved from scratch, **two** ways, code ran clean 500×, and
you answered every follow-up out loud.

### Phase 1 — Ordering & Signaling
- [ ] LC 1114 Print in Order
- [ ] LC 1115 Print FooBar Alternately
- [ ] LC 1116 Print Zero Even Odd
- [ ] LC 1195 Fizz Buzz Multithreaded
- [ ] LC 1117 Building H2O

### Phase 2 — Building Blocks
- [ ] Bounded blocking queue (LC 1188 + Rubrik)
- [ ] Bounded queue: timeout variants
- [ ] Thread pool executor (no `ExecutorService`)
- [ ] Thread pool: exception handling, timeout, thread reclamation
- [ ] Thread pool: `Future` / `get()`
- [ ] Reader-writer lock — reader preference
- [ ] Reader-writer lock — writer preference
- [ ] Reader-writer lock — fair / FIFO
- [ ] Thread-safe stack, O(1)
- [ ] Lock-free stack with CAS
- [ ] Thread-safe LRU cache
- [ ] Concurrent hash map with lock striping
- [ ] Fixed-buffer FIFO queue
- [ ] Two queues sharing one fixed buffer (senior)

### Phase 3 — Schedulers & Task Graphs
- [ ] Concurrent job scheduler with dependencies
- [ ] Job scheduler: cycle detection
- [ ] Job scheduler: failure handling & termination
- [ ] `schedule()` + `scheduleAtFixedRate()`
- [ ] Multithreaded web crawler (LC 1242)

### Phase 4 — Group Mutual Exclusion
- [ ] Political bathroom / unisex bathroom
- [ ] Playground (one team, max 10)
- [ ] Uber ride seating
- [ ] Dining Philosophers (LC 1226) — all three solutions
- [ ] Car & truck parking allocator
- [ ] Logical memory unit / page cache
- [ ] Connection pool
- [ ] Ticket booking / seat contention

### Phase 5 — Bugs & Theory
- [ ] Bug catalog: all 12 spotted in <60s
- [ ] Producer/consumer bug squash
- [ ] Banking app bug squash
- [ ] Memory model rapid-fire
- [ ] Theory rapid-fire

### Phase 6 — Mocks
- [ ] Mock 1 · [ ] Mock 2 · [ ] Mock 3 · [ ] Mock 4

---

## Resources

**Primary**
- *The Little Book of Semaphores* (Downey) — free PDF. Multiple candidates cite it as the
  single best source; searcher-inserter-deleter, unisex bathroom, and the group-exclusion
  family all come from here.
- LeetCode Concurrency problem set — `leetcode.com/problemset/concurrency/`
- *Java Concurrency in Practice* (Goetz) — if you're going the Java route
- LeetCode Discuss: filter interview experiences by company tag (Rubrik, Confluent) for the
  most current questions

**Supplementary**
- Educative's concurrency blog posts — notably, a Confluent recruiter reportedly sent this to
  a candidate as official prep material
- OS textbook slides (Silberschatz) — Part 2 covers everything needed
- YouTube: DefogTech (Java concurrency), Jenkov's tutorials

**Company-specific reconnaissance**

Before any loop, search LeetCode Discuss for `<company> interview experience concurrency`
filtered to the last 6 months. These questions rotate, and the discussion threads are far
more current than any static list — including this one.

---

## A note on scope

Companies with dedicated concurrency rounds: **Rubrik** (2–3 "System Coding" rounds),
**Confluent**, **Databricks**, **Netflix**, **LinkedIn**, **MongoDB**, **Pure Storage**,
**Salesforce**, **Coreweave**, **Anthropic**. Blind reports these are near-guaranteed on
systems/infrastructure tracks and common at Meta, Amazon, and Microsoft for infra teams,
but uncommon for general product SWE roles.

Calibrate your investment accordingly — and ask your recruiter directly whether the round is
a knowledge discussion, a LeetCode-style problem, or a build-it-live exercise. Candidates who
asked reported very different formats.