# Phase 2 — Aeron Cluster Raft: correctness + low-latency techniques (binding notes)

Derived from studying Aeron Cluster's Java consensus module (`aeron-cluster/src/main/java/io/aeron/cluster/`: `Election.java`, `ElectionState.java`, `ClusterMember.java`, `ConsensusModuleAgent.java`, `ConsensusPublisher.java`). Rune's C++ Raft MUST match the correctness rules and adopt these techniques.

## A. Correctness rules (validated against Aeron)

1. **Quorum threshold** = `(n >> 1) + 1` (Aeron `ClusterMember.quorumThreshold`). Our `ClusterConfig::majority()` matches.
2. **Up-to-date comparison — `compareLog`** (Aeron `ClusterMember.compareLog`): lexicographic on `(leadershipTermId, logPosition)`. Rune equivalent: `(lastLogTerm, lastLogIndex)`. A vote is granted only if the candidate's `(term, index)` is ≥ the voter's — Leader Completeness.
3. **RequestVote decision** (Aeron `Election.onRequestVote`), in order:
   - ignore if self or uninitialized;
   - if `candidateTerm ≤ alreadySeenCandidateTerm` → **reject** (one vote per term);
   - else if voter's log is strictly more up-to-date (`compareLog(mine, theirs) > 0`) → **reject**;
   - else → **persist** the candidate term (durable `votedFor`/term via PersistentState) *before replying*, **grant**, and step to follower-ballot.
4. **Vote persistence before reply** — currentTerm/votedFor are fsync'd before a vote response is sent (our `PersistentState` already does write+fsync+rename). Never grant two votes in one term across a crash.
5. **AppendEntries** (Raft §5.3, mirrored by Aeron's log-replication states): reject on term < currentTerm or prevLog mismatch; truncate conflicts; append; `commitIndex = min(leaderCommit, lastNewIndex)`; a leader advances commit only to an entry **of its own term** with a majority `matchIndex` (Aeron `quorumPosition` over ranked member positions) — never commit a prior-term entry by count alone (Leader Completeness).
6. **Election phases**: Aeron uses INIT→CANVASS→NOMINATE→CANDIDATE_BALLOT/FOLLOWER_BALLOT→(leader/follower log replication/replay/catchup)→READY. Canvass/nominate gossip member `(termId, logPosition)` so the most-up-to-date member nominates, cutting dueling elections. **Rune core**: use randomized election timeouts (provably-safe vanilla Raft ballot) for P2-c/P2-d; keep a pre-vote/canvass-lite as a later optimization (documented, not required for correctness).

## B. Low-latency techniques (mandatory — "same techniques")

1. **Single-writer agent.** The Raft node is driven by ONE thread via a `doWork(nowNs)` loop (Aeron `ConsensusModuleAgent implements Agent`). No locks, no shared mutable state across threads; all node state is single-writer. `doWork` polls inbound, drives timers, emits outbound, and returns a `workCount`.
2. **IdleStrategy.** The runtime loop calls `idleStrategy.idle(workCount)` — busy-spin when busy, back off when idle. (Runtime/P2-h; the pure core just returns workCount.)
3. **Injected monotonic nanosecond clock.** All time is a `nowNs` parameter passed into `doWork`/handlers (Aeron injects `NanoClock`/`epochClock`). No `std::chrono::system_clock` on the hot path; tests inject a manual clock.
4. **Zero-copy publication.** Encode straight into the Aeron term buffer with `Publication::tryClaim` (Aeron `ConsensusPublisher` holds one reusable `BufferClaim` + one reusable SBE encoder **per message type as fields**, reused every send). No per-message heap allocation. (P2-g codecs + P2-h transport.)
5. **Flat, fixed-layout SBE-style messages.** Raft/metadata messages are flat structs with fixed offsets (like rune-protocol's record-batch), decodable as views over the received buffer with zero copy. Reuse decoder views.
6. **No hot-path allocation.** Pre-allocate reusable buffers and temp arrays (Aeron `quorumPosition(members, rankedPositions[], ...)` takes a caller-owned temp array "to avoid allocation"). Rune: the node writes outbound into a caller-provided sink / reusable buffer rather than allocating a fresh container each tick; commit-quorum computation uses a pre-sized scratch array.

## C. Consequences for the Rune design (applied to P2-c…P2-h)

- **P2-c `RaftNode`**: model as a single-writer agent — `int doWork(int64 nowNs, Inbox&, Outbox&)` returning workCount; injected nano clock (nowNs param); randomized election timeout via seeded RNG; role transitions reset volatile state per Raft. Outbound written to a caller-owned `Outbox` (reusable), not freshly allocated. Up-to-date via `(lastLogTerm, lastLogIndex)` compareLog.
- **P2-d/P2-e**: RequestVote/AppendEntries decisions exactly per §A.3/§A.5; persist term+vote before replying; commit only own-term entries at majority match.
- **P2-g codecs**: flat fixed-offset layout, encode via reusable encoder into a provided buffer (tryClaim-ready), CRC-32C + varint from rune-protocol; decode as zero-copy views.
- **P2-h transport**: Aeron `tryClaim` zero-copy send + `IdleStrategy` agent loop + embedded media driver; feasibility spike first.
- **P2-i harness**: single-writer nodes stepped by a manual nano clock over an in-memory bus; assert Election Safety, Leader Completeness, State Machine Safety.
