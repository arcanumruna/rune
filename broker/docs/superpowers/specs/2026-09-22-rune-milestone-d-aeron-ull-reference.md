# Milestone D — Aeron/Agrona Low-Latency Reference (techniques Rune mirrors)

> **STATUS: REFERENCE.** Source-read of Aeron at `/Users/vradu/Projects/aeron`
> (+ Agrona 2.6.0 sources; C ports under `aeron-client/src/main/c/`). Per the
> "Aeron is the bible" rule, Rune mirrors these techniques exactly. Two parts:
> (1) the low-latency **primitives** (hot path + control plane); (2) the **Aeron
> Cluster consensus/apply model**, which is the template for our metadata state
> machine. Companion to the D design spec and the Kafka parity doc.

## Part 1 — Low-latency primitives (the decision-#8 toolbox)

### 1. Lock-free ring buffers (cross-thread handoff)
`aeron-client/src/main/c/concurrent/aeron_rb.h`, `aeron_mpsc_rb.c`, `aeron_spsc_rb.c`;
Java `Agrona ManyToOneRingBuffer`/`RingBufferDescriptor`/`RecordDescriptor`.
- Power-of-2 data region + a trailer whose control fields (tail, headCache, head)
  are each on **their own 2-cache-line (128 B) slot** (`aeron_rb.h:26-40`;
  `RingBufferDescriptor.java:57-77`).
- Record header = 8 bytes (`int32 length` @0, `int32 msgTypeId` @4), 8-byte aligned.
- MPSC producer: read a **cached head** to avoid touching the consumer's line on
  the common path; only re-read the true head when apparently full; then a single
  64-bit **CAS** on tail (`aeron_mpsc_rb_claim_capacity`). SPSC needs no CAS.
- **`length` field carries the commit handshake:** write `-len` (claimed) →
  release-store → copy payload → release-store `+len` (committed). A **padding
  record** (`msgTypeId = -1`) fills to the buffer end so records never straddle
  the wrap. `tryClaim`/`commit`/`abort` = zero-copy publish.
- Consumer reads `length` with **acquire**; `≤0` ⇒ nothing committed ⇒ stop;
  **zeroes consumed bytes**, advances head with a **release store**.
- *Rune uses this for* every SPSC cleaner/append swap ring; the MSM→bridge handoff
  and the balancer→apply handoff follow the same shape.

### 2. Memory-ordering discipline
`aeron-client/src/main/c/concurrent/aeron_atomic64_gcc_x86_64.h`; `UnsafeBuffer`.
- Publish = **release store** (`AERON_SET_RELEASE`), observe = **acquire load**
  (`AERON_GET_ACQUIRE`) — on x86 these are plain MOV + compiler fence. RMW only via
  `lock`-prefixed CAS (`cmpxchgq`) / fetch-add (`xaddq`). Never a plain load for a
  cross-thread flag. C++: `std::atomic` with `memory_order_acquire/release`.

### 3. False-sharing avoidance
`BitUtil.CACHE_LINE_LENGTH = 64`; `AERON_CACHE_LINE_LENGTH`.
- Every concurrently-written field gets its own line, **padded to 2× (128 B)** to
  defeat the hardware adjacent-line prefetcher (ring trailer, counters).
- Pad mutable per-object state with dead `pN` fields on both sides
  (`BackoffIdleStrategy` = 256 B).
- Branchless `align(v,a) = (v + (a-1)) & -a`; power-of-2 capacities + mask (AND,
  not modulo).

### 4. Agent duty-cycle & idle strategy
`Agent.doWork()`, `AgentRunner` (`workCount = doWork(); idle(workCount)`);
`AgentInvoker` (inline). Idle progression **spin → yield → park** (`BackoffIdleStrategy`
defaults `maxSpins=10, maxYields=5, minPark=1µs, maxPark=1ms`; `BusySpin` for the
tightest loops). *Rune's* LogCleanerAgent / append agent already follow
`doWork()→idle(workCount)`; the MSM apply loop and the controller/balancer loop
adopt the same, single-threaded (no locks on hot state).

### 5. Open-addressing collections (control plane, no boxing)
Agrona `Int2ObjectHashMap`/`Long2ObjectHashMap`/`Object2IntHashMap`/`IntArrayList`
(C `aeron_int64_counter_map`). Primitive key arrays, **linear probing**,
sentinel-not-null for primitive values, **0.65 load factor**, power-of-2 + mask.
No per-op allocation, L1-friendly (no pointer chasing). *Rune:* the routing map,
the `node→partitions-led` reverse index, and the assignment tables use this shape,
not `std::unordered_map` node-chaining.

### 6. Status counters (lock-free cross-process signaling)
`aeron_counters_manager.*`; `aeronc.h:851-871`. Each counter on a **128 B (2 cache
line) slot** in a memory-mapped file: 8-byte value + ids + padding. Write with a
single **release store** (`aeron_counter_set_release`), read with **acquire**;
monotonic positions via **`propose_max_release`** (never a decreasing store).
*Rune's* role, commit-position, and append-position signaling is exactly this — the
apply loop reads the commit counter, never calls into the controller thread.

### 7. Pre-allocated rotating buffers + MTU-bounded claims
Aeron log = **3 term partitions** (fill N+1 while N drains, N-1 cleans → no
term-boundary stall); `tryClaim` bounded to `MTU - header` (one frame), fragment
above; positions are monotonic 64-bit counters; reusable `BufferClaim` (no runtime
allocation on the append path). *Rune's* append path is already zero-alloc via
pre-provisioned segments (the p99 append fix); D adds nothing to it.

### ULL checklist (diff against any D component on the hot path)
- [ ] Cross-thread handoff = power-of-2 ring buffer; SPSC (no CAS) or MPSC (1 CAS);
      cached opposite-end pointer; length-as-commit-flag; tryClaim/commit/abort;
      padding at wrap; zero-on-consume.
- [ ] release-store / acquire-load for every cross-thread field; RMW only via CAS/xadd.
- [ ] 128 B padding on every concurrently-written field; power-of-2 + mask indexing.
- [ ] `doWork()→idle(workCount)` single-thread agents; spin→yield→park backoff.
- [ ] Open-addressing, no-boxing, 0.65-load collections for all control-plane maps.
- [ ] Role/position via 128 B mmap counters, release/acquire, propose-max monotonic.
- [ ] Pre-allocated/rotating buffers; zero allocation on produce/append.

## Part 2 — Aeron Cluster = the metadata-state-machine template

The load-bearing insight: **the replicated log IS the state; the state machine is a
deterministic fold over the committed log.** `decide → log → apply.`

### Single-writer sequencing
`ConsensusModuleAgent` is one thread; only the leader for the current
`leadershipTermId` may append, and only when `termId == current && role == LEADER`
(`onIngressMessage:835`). All partition-assignment / membership / heartbeat-expiry
decisions become log records produced by that one thread — order = call order.
→ *Rune's controller* is the metadata-consensus leader; it is the sole writer of
assignment records; term-check every inbound admin message.

### Commit = durable quorum, not mere replication
`updateLeaderPosition` sets leader `logPosition = appendPosition` (Archive
`RecordingPos` = fsync'd bytes); `ClusterMember.quorumPosition` = the (n/2+1)-th
highest **persisted** position; `commitPosition.proposeMaxRelease(quorum)`;
followers apply only up to `min(notifiedCommit, ownPersisted)`. Inactive members
(past heartbeat timeout) are dropped from the quorum calc so one dead node can't
stall commit.
→ *Rune:* advance commit to `min(leaderPersisted, quorumMin(persisted))`; apply
only committed records.

### Deterministic apply (the `ClusteredService` contract)
The service is a **separate agent** whose `doWork` polls `logAdapter.poll(commitPosition.get())`
— it can never observe past the commit counter. Every callback sets its `logPosition`
and `clusterTime` **from the log record**, never the wall clock. A
`checkForValidInvocation` guard trips if apply-code tries to emit commands, schedule
timers, or offer during a lifecycle callback — enforcing determinism. `doBackgroundWork`
is the *only* non-deterministic lane and is barred from mutating state.
→ *Rune's MSM apply* = pure `apply(record, logPosition, clusterTime)`; the
partition-leadership *decision* is a logged record, applied identically on leader,
every follower, and every recovering node. Fence non-deterministic side-work into a
clearly-separated background lane.

### Term fencing
`candidateTermId` monotonic and **persisted before use**; vote granted only if the
candidate's `(termId, logPosition)` ≥ the voter's (`compareLog`); a term-boundary
marker is written into the log at leader-ready; higher-term message ⇒ step down.
→ stops a partitioned old controller from assigning leadership after supersession.

### Snapshot + replay recovery
Snapshot taken at a **logged** SNAPSHOT action (deterministic position), framed
BEGIN/END markers carrying `(logPosition, leadershipTermId, appVersion)`; persisted
durably (block until recorded); recovery = load snapshot → set applied position =
snapshot position → replay committed tail from exactly there (contiguity asserted).
→ *Rune's MSM* snapshots the assignment map + membership at a committed position and
replays the tail; snapshot trigger is a logged action, never a local timer.

### Counter-based role/position (Rune already does this)
Role (`CLUSTER_NODE_ROLE_TYPE_ID`), election state, commit position, append position
all exposed as Aeron counters — release-store on write, acquire on read. The apply
loop and external observers read them lock-free.

### Deterministic timers (critical for our balancer + heartbeat-expiry)
`Cluster.scheduleTimer` does NOT schedule locally — it asks the consensus module;
the **leader** holds the authoritative `PriorityHeapTimerService`, polls it against
**cluster/log time**, and on expiry **appends a TimerEvent to the log**, which every
replica applies deterministically via `onTimerEvent`. Pending timers are snapshotted
and re-armed against cluster time on election-complete.
→ *Rune:* the periodic **Balancer tick** and **heartbeat-expiry** must be **logged
records**, not local wall-clock callbacks. Heartbeats update last-seen time in
*applied state*; a logged periodic tick checks expiry deterministically and emits
the logged "member down → reassign partitions" decision — so leadership
reassignment is itself a committed, replayable entry, identical everywhere and
across recovery. (This is a correction to the naive "controller runs a timer"
model — see the design spec's Deterministic-control section.)

### Backpressure
Log back-pressure ⇒ re-offer the fragment (ABORT), never drop/reorder; bounded
per-poll batches (`SERVICE_MESSAGE_LIMIT=20`, `TimerService.POLL_LIMIT=20`); the
apply agent simply polls less log per cycle if it falls behind — it never blocks
consensus (separate process).
