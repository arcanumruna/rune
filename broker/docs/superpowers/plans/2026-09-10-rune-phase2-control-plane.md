# Rune Phase 2 — Control Plane (C++ Raft metadata) Plan

**Goal:** A correct, well-tested C++ Raft consensus core + the metadata state machine it replicates, built pure and TDD-first (no networking), then wired over Aeron and exercised by a multi-node cluster harness. Implements design spec §5 (control plane) and §10 (phasing).

**Spec:** `docs/specs/2026-09-10-rune-broker-design.md` (§5 metadata Raft quorum, §8 fencing via leader-epoch, §6.6 leader-epoch/producer-state).

**Reference:** Raft paper (Ongaro & Ousterhout, "In Search of an Understandable Consensus Algorithm"), Figure 2 (state, RPCs, rules). Aeron Cluster `Election`/KRaft `QuorumState` as design blueprints.

**Where:** a new `librune_consensus` library inside `rune-broker` (`libs/consensus/`), namespace `rune::consensus`, mirroring `librune_storage` conventions (C++23, CMake, gtest unit target `rune_consensus_tests`, integration target `rune_consensus_integration_tests`). Consumes rune-protocol (varint/CRC) via the existing include path.

**Live protocol:** per `CLAUDE.md` — each unit gets a Planka card (Backlog→In Progress→In Review→Done), a `plan` chat note at start, and a pushed `test`/`benchmark` result with a stable suite id (`rune-broker/consensus`, `rune-broker/consensus-integration`).

## Global constraints
- C++23; cmake not on PATH (use Conan's cmake); rune-protocol via `${RUNE_PROTOCOL_INCLUDE}`; `aeron/1.41.4`; NEVER `conan create/export/upload`.
- Raft correctness is the priority. Every safety rule from Raft Figure 2 is a test.
- **Determinism:** all time comes from an INJECTED clock (`std::function<int64_t()>` nanos or a `Clock` interface); all randomness (election timeout jitter) from an injected/seeded RNG. No wall-clock, no real threads in unit tests.
- Log is **1-indexed** (index 1 = first real entry; index 0 = "empty" sentinel with term 0), matching the Raft paper.
- `NodeId = int32_t`; `Term = int64_t`; `Index = int64_t`.

## Units (each = one Planka card, TDD, own commit(s))

### P2-a — Scaffold `libs/consensus` + Raft persistent state
- `libs/consensus/CMakeLists.txt` (lib `rune_consensus` + `rune_consensus_tests` under BUILD_TESTING), wired into root CMake (`add_subdirectory(libs/consensus)`), `RUNE_PROTOCOL_INCLUDE` on its includes.
- `include/rune/consensus/Types.h` — `NodeId/Term/Index` aliases, `LogEntry{Term term; std::string command;}`, `RaftRole{Follower,Candidate,Leader}`, `ClusterConfig{NodeId self; std::vector<NodeId> members; int majority() const;}`.
- `PersistentState` — `currentTerm()`, `votedFor()` (`-1`=none), `setCurrentTerm`, `setVotedFor`, durable to a file (fsync on change) + reload on construction. Tests: defaults (term 0, votedFor -1), persists across reopen, votedFor reset semantics.

### P2-b — RaftLog
- `RaftLog` — `append(Term, command)` returns new lastIndex; `Index lastIndex()`, `Term lastTerm()`; `Term termAt(Index)` (0 for index 0 or beyond); `const LogEntry& at(Index)`; `entriesFrom(Index)`; `truncateFrom(Index)` (drop index..end, for conflict resolution); `commitIndex()`/`setCommitIndex` (monotonic, ≤ lastIndex). 1-indexed. Tests: append/termAt/last, truncate mid-log, commit monotonic + clamp, out-of-range termAt returns 0.

### P2-c — Raft role/quorum state machine (timeouts)
- `RaftNode` skeleton owning `PersistentState`, `RaftLog`, `RaftRole`, injected `nowNanos` + seeded RNG + `ClusterConfig`. `tick(nowNanos)` drives timeouts: follower/candidate election-timeout (randomized in [T,2T]) → become candidate + start election; leader heartbeat-interval → emit AppendEntries (empty) to peers. `becomeFollower(term)`, `becomeCandidate()`, `becomeLeader()` with correct term/votedFor/volatile-state resets. Output = a list of outbound messages (pure; caller delivers). Tests: election timeout fires → candidate + term+1 + self-vote; leader emits heartbeats each interval; higher-term observation → step down to follower.

### P2-d — Election / RequestVote (Raft Fig 2 rules)
- `RequestVote{term, candidateId, lastLogIndex, lastLogTerm}` + `RequestVoteResult{term, voteGranted}`. `handleRequestVote()`: reply false if term<currentTerm; if term>currentTerm step down; grant iff (votedFor==none||candidate) AND candidate log is at-least-as-up-to-date (lastLogTerm/Index rule). Candidate collects results → majority ⇒ becomeLeader; higher term ⇒ step down. Tests: term rules, up-to-date rejection, one-vote-per-term, split vote no-majority, majority→leader.

### P2-e — Replication / AppendEntries
- `AppendEntries{term, leaderId, prevLogIndex, prevLogTerm, entries[], leaderCommit}` + `AppendEntriesResult{term, success, matchIndex}`. `handleAppendEntries()`: reject if term<currentTerm or prevLog mismatch; delete conflicting suffix + append new; advance commitIndex=min(leaderCommit,lastNewIndex); reset election timer; step down if term≥currentTerm and candidate/leader. Leader side: per-peer `nextIndex`/`matchIndex`, decrement-on-reject, advance-on-success; commit advances when a majority `matchIndex ≥ N` and `log[N].term==currentTerm`. Tests: consistency check, conflict truncation, commit advancement, majority-match commit, leader-completeness (never commits older-term entry via count alone).

### P2-f — Metadata state machine
- `MetadataStateMachine` applying committed `LogEntry.command` (decoded metadata commands): `CreateTopic`, `DeleteTopic`, `AssignPartitionLeader(topic,part,leader,epoch)`, `UpdateIsr(topic,part,isr[],epoch)`, `RegisterNode`/`ExpireNode`. State: topics→partitions→{leader, replicas, isr, leaderEpoch}, node liveness. `apply(index, command)` idempotent/ordered; `snapshot()`/`restore()`. Leader-epoch is monotonic per partition (fencing). Tests: each command mutates correctly; epoch monotonicity; snapshot/restore round-trip == replay.

### P2-g — Raft + metadata message codecs
- Compact binary encode/decode (varint + CRC-32C from rune-protocol) for RequestVote(+Result), AppendEntries(+Result, incl. entries), and the metadata command union. Tests: round-trip all message types (incl. multi-entry AppendEntries, empty entries), corruption→decode error, byte-layout stability.

### P2-h — Aeron transport integration (RISK-gated)
- Embedded Aeron media driver (C++) + per-node ingress/egress publications/subscriptions carrying the P2-g wire messages; a `RaftTransport` that maps outbound messages → Aeron offers and inbound fragments → `RaftNode` handlers. **Feasibility spike FIRST:** confirm the C++ Aeron client + an embedded media driver actually start in this environment; if not, document the blocker and keep the in-memory bus (P2-i) as the Phase-2 deliverable, deferring Aeron wiring. Push a `thought` note with the finding.

### P2-i — Multi-node in-process cluster harness + integration tests
- `InMemoryBus` delivering messages between N in-process `RaftNode`s (with controllable partition/drop/delay), driven by a manual clock. Integration tests: 3- and 5-node clusters elect exactly one leader; a client metadata command replicates to a majority and applies on all; kill the leader → new leader elected, committed state preserved; network partition (minority isolated) → minority cannot commit, heals without split-brain; restart a node → recovers currentTerm/votedFor and re-syncs log. Assert the Raft safety properties (Election Safety, Leader Completeness, State Machine Safety) via an in-test history checker.

## Execution & exit
Per unit: card→In Progress + `plan` note → implementer subagent (TDD) → quality review → fix loop → commit → card→Done → push `test` result (suite `rune-broker/consensus`). Integration units push `rune-broker/consensus-integration`. Phase-2 exit: single-leader election + metadata replication + failover + partition-safety all green across 3/5-node in-memory clusters; Aeron transport wired if feasible, else documented + deferred with the in-memory bus as the proven core.
