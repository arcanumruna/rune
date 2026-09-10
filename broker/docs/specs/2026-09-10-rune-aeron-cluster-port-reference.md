# Rune: Aeron Cluster → C++ Faithful-Port Reference

**Date:** 2026-09-10  
**Sources:** `aeron-cluster/src/main/java/io/aeron/cluster/` (read-only)  
**Purpose:** Exact blueprint for porting the Aeron Cluster consensus module to `rune::consensus` in C++.

---

## 1. ElectionState Machine

### 1.1 State Table

All 18 states from `ElectionState.java:26–115`. Codes are stored as an `AtomicCounter` (`ELECTION_STATE_TYPE_ID`).

| Code | Name | Handler job | Outgoing transitions (condition → next state) |
|------|------|-------------|-----------------------------------------------|
| 0 | **INIT** | Consolidate local state, reset in-flight resources (replication, catchup, log subscription, replay). Read `nodeStateFile.candidateTerm().candidateTermId()` → update `candidateTermId`. | Single-node cluster: `candidateTermId++`, persist, → **LEADER_LOG_REPLICATION**. Multi-node: → **CANVASS**. `Election.java:683–696` |
| 1 | **CANVASS** | Broadcast `CanvassPosition` to all peers on `electionStatusIntervalNs` cadence. Waits for peers to advertise their log state. Computes whether this node is a valid candidate. | `isUnanimousCandidate` → **NOMINATE** (immediate); OR deadline expired (`startupCanvassTimeoutNs` on startup, else `leaderHeartbeatTimeoutNs`) AND `isQuorumCandidate` → **NOMINATE**. `Election.java:700–729`. Entry: resets all member state (`ClusterMember.reset()`), role=FOLLOWER. |
| 2 | **NOMINATE** | Random backoff delay (`nominationDeadlineNs = nowNs + rand * electionTimeoutNs/2`). Continues publishing `CanvassPosition`. | `nowNs >= nominationDeadlineNs` → persist `candidateTermId+1` via `proposeMaxCandidateTermId`, call `becomeCandidate`, → **CANDIDATE_BALLOT**. `Election.java:732–751`. Entry: role=CANDIDATE. |
| 3 | **CANDIDATE_BALLOT** | Send `RequestVote` to every member that hasn't received a ballot yet (loops `isBallotSent`). | `isUnanimousLeader` → set `leaderMember=thisMember`, `leadershipTermId=candidateTermId`, → **LEADER_LOG_REPLICATION**. Timeout (`electionTimeoutNs`) + `isQuorumLeader` → same. Timeout without quorum → **CANVASS**. `Election.java:754–793`. |
| 4 | **FOLLOWER_BALLOT** | Voted for a candidate; waits for result. No active sending. | Timeout (`electionTimeoutNs`) → **CANVASS** `Election.java:796–807`. Receives `onNewLeadershipTerm` matching `candidateTermId` → **FOLLOWER_REPLAY** or **FOLLOWER_LOG_REPLICATION** (see §1.2). |
| 5 | **LEADER_LOG_REPLICATION** | New leader waits for followers to replicate to `appendPosition`. Publishes `NewLeadershipTerm` + `CommitPosition` on `leaderHeartbeatIntervalNs`. Sets `thisMember.logPosition=appendPosition`. Creates log publication (`addLogPublication`). | `quorumPosition >= appendPosition` → **LEADER_REPLAY**. `Election.java:809–825`. Entry: role=LEADER, `logSessionId=addLogPublication(appendPosition)`. |
| 6 | **LEADER_REPLAY** | Replay log from `logPosition` to `appendPosition` via `LogReplay`. Continues publishing NLT+commit. | `logPosition == appendPosition` (nothing to replay): → **LEADER_INIT** immediately. Replay done → `logPosition=appendPosition` → **LEADER_INIT**. `Election.java:828–865`. |
| 7 | **LEADER_INIT** | One-shot: calls `joinLogAsLeader(leadershipTermId, logPosition, logSessionId, isLeaderStartup)`, updates `RecordingLog`. | Unconditionally → **LEADER_READY**. `Election.java:868–874`. |
| 8 | **LEADER_READY** | Publishes `NewLeadershipTerm` on interval. Checks `hasQuorumAtPosition(clusterMembers, leadershipTermId, logPosition)`. Calls `updateLeaderPosition`. | Quorum at `logPosition` AND `appendNewLeadershipTermEvent` succeeds → calls `electionComplete`, → **CLOSED**. `Election.java:877–894`. |
| 9 | **FOLLOWER_LOG_REPLICATION** | Follower replicates missing log entries from leader's Archive via `RecordingReplication`. Publishes `AppendPosition` to leader on interval. | Replication done AND `notifiedCommitPosition >= appendPosition` → update RecordingLog → **CANVASS**. Replication not started AND `appendPosition >= replicationStopPosition` → skip replication, update RecordingLog → **CANVASS**. Timeout (`replicationDeadlineNs`) → `TimeoutException`. `Election.java:897–957`. Entry: role=FOLLOWER. |
| 10 | **FOLLOWER_REPLAY** | Follower replays local log from `logPosition` up to `min(appendPosition, notifiedCommitPosition)`. | `logPosition == appendPosition` (nothing to replay): `catchupJoinPosition != NULL_POSITION` → **FOLLOWER_CATCHUP_INIT**, else → **FOLLOWER_LOG_INIT**. Replay done and `logPosition == appendPosition` → same fork. Replay done but `logPosition < appendPosition` (gap) → **CANVASS**. `Election.java:960–1017`. Entry: role=FOLLOWER. |
| 11 | **FOLLOWER_CATCHUP_INIT** | Adds log subscription + catchup MDC destination. Resolves dynamic port if `:0`. Sends `CatchupPosition` to leader. | Send succeeds → **FOLLOWER_CATCHUP_AWAIT**. Timeout (`leaderHeartbeatTimeoutNs`) without send → `TimeoutException`. `Election.java:1020–1055`. |
| 12 | **FOLLOWER_CATCHUP_AWAIT** | Waits for leader replay image to appear on `logSessionId`. | Image present AND `tryJoinLogAsFollower` succeeds → **FOLLOWER_CATCHUP**. Timeout → `TimeoutException`. `Election.java:1058–1086`. |
| 13 | **FOLLOWER_CATCHUP** | Polls catchup replay via `catchupPoll(notifiedCommitPosition, nowNs)`. Adds live log destination when near-live. | `commitPositionCounter >= catchupJoinPosition` AND `>= notifiedCommitPosition` AND catchup destination removed → `appendPosition=logPosition=position` → **FOLLOWER_LOG_INIT**. `Election.java:1089–1112`. |
| 14 | **FOLLOWER_LOG_INIT** | If `logSubscription == null` (no prior catchup): adds follower subscription + live log MDC destination → **FOLLOWER_LOG_AWAIT**. Else (post-catchup, subscription exists): → **FOLLOWER_READY**. `Election.java:1115–1131`. |
| 15 | **FOLLOWER_LOG_AWAIT** | Waits for live log image by `logSessionId`. | Image + `tryJoinLogAsFollower` → update RecordingLog → **FOLLOWER_READY**. Channel ERRORED → `ClusterException`. Timeout → `TimeoutException`. `Election.java:1134–1163`. |
| 16 | **FOLLOWER_READY** | Publishes `AppendPosition(leadershipTermId, logPosition, thisMemberId, FLAG_NONE)` to leader once. | Send succeeds → calls `electionComplete` → **CLOSED**. Timeout → `TimeoutException`. `Election.java:1166–1179`. |
| 17 | **CLOSED** | Terminal: election complete, no doWork activity. `Election.java:255`. | — |

### 1.2 Compact State Diagram

```
             +-------------------------------------------------------+
             |              onNewLeadershipTerm received              |
             v                                                        |
    [INIT] --> [CANVASS] --> [NOMINATE] --> [CANDIDATE_BALLOT]       |
       |           ^              |              |        |           |
       | (single)  | (timeout/no  |              |        |           |
       |           | quorum)      v              v (quorum/unanimous) |
       v           +-------[FOLLOWER_BALLOT]  [LEADER_LOG_REPLICATION]
  [LEADER_LOG_REPLICATION]       |                    |
       |                         | (timeout)          v
       v                         +--->[CANVASS] [LEADER_REPLAY]
  [LEADER_REPLAY]                                     |
       |                                              v
       v                                        [LEADER_INIT]
  [LEADER_INIT]                                      |
       |                                              v
       v                                        [LEADER_READY]
  [LEADER_READY] ---(quorum+appendEvent)-----------> [CLOSED]

  Follower path (from onNewLeadershipTerm):
  [FOLLOWER_BALLOT/CANVASS] --> [FOLLOWER_LOG_REPLICATION] --> [CANVASS]
                            --> [FOLLOWER_REPLAY] --> [FOLLOWER_CATCHUP_INIT]
                                                  --> [FOLLOWER_LOG_INIT]
                            [FOLLOWER_CATCHUP_INIT] --> [FOLLOWER_CATCHUP_AWAIT]
                            [FOLLOWER_CATCHUP_AWAIT] --> [FOLLOWER_CATCHUP]
                            [FOLLOWER_CATCHUP] --> [FOLLOWER_LOG_INIT]
                            [FOLLOWER_LOG_INIT] --> [FOLLOWER_LOG_AWAIT] --> [FOLLOWER_READY]
                                               --> [FOLLOWER_READY]
                            [FOLLOWER_READY] --> [CLOSED]
```

---

## 2. Core Model

### 2.1 Term/Position Variables

All live in `Election.java:85–108` and `ConsensusModuleAgent.java:131`.

| Variable | Owner | Meaning |
|----------|-------|---------|
| `leadershipTermId` | Election, CMA | The term this node recognises as active/new. Advances when a node wins an election or accepts a NLT. Initialized from last RecordingLog term entry. |
| `candidateTermId` | Election | The term ID a candidate proposes. Always `>= leadershipTermId`. Durably persisted via `NodeStateFile`. Monotonically increased by `proposeMaxCandidateTermId`. |
| `logLeadershipTermId` | Election | The term ID of the most recent entry this node has physically appended. May lag `leadershipTermId` while replicating. Initialized from last log entry. |
| `logPosition` | Election | The byte position up to which the log has been **committed** / replayed. Updated on replay completion. |
| `appendPosition` | Election | The byte position up to which the local Archive has **physically appended** (i.e., the highest durable write). May exceed `logPosition` (uncommitted). |
| `notifiedCommitPosition` | Election | Highest `commitPosition` received from the leader via `CommitPosition` or `NewLeadershipTerm` messages. |
| `commitPosition` (Counter) | CMA | Shared `io.aeron.Counter` representing the quorum-committed position. Readable by services without locks. `ConsensusModuleAgent.java:152`. |

**Relationship:** `logPosition <= notifiedCommitPosition <= appendPosition`. A new leader's goal is to bring all followers' `appendPosition` up to its own before declaring `LEADER_READY`.

### 2.2 `compareLog` — Exact Algorithm

`ClusterMember.java:1173–1197`:

```java
public static int compareLog(
    long lhsLogLeadershipTermId, long lhsLogPosition,
    long rhsLogLeadershipTermId, long rhsLogPosition)
{
    if (lhsLogLeadershipTermId > rhsLogLeadershipTermId)  return 1;
    if (lhsLogLeadershipTermId < rhsLogLeadershipTermId)  return -1;
    if (lhsLogPosition > rhsLogPosition)                  return 1;
    if (lhsLogPosition < rhsLogPosition)                  return -1;
    return 0;
}
```

**Rule:** Primary key = `logLeadershipTermId` (higher is more recent). Secondary key = `logPosition` (higher is more recent). Used in:
- `onRequestVote` (`Election.java:355`) — candidate's log must be >= voter's log
- `willVoteFor` (`ClusterMember.java:1297`) — member votes for candidate only if `compareLog(this, candidate) <= 0`

### 2.3 Quorum Helpers (Exact)

**`quorumThreshold`** (`ClusterMember.java:853–856`):
```java
public static int quorumThreshold(final int memberCount) {
    return (memberCount >> 1) + 1;  // majority = floor(N/2) + 1
}
```

**`quorumPosition`** (`ClusterMember.java:867–895`): Insertion-sort into `rankedPositions[]` (length = `quorumThreshold(N)`). For each active member (active = `timeOfLastAppendPositionNs + timeoutNs > nowNs`), insert `logPosition` into the sorted array keeping the top K values. Returns `rankedPositions[length-1]` = the K-th highest position = quorum position.
- Array is pre-allocated (no per-call allocation): `rankedPositions = new long[quorumThreshold(activeMembers.length)]` at `ConsensusModuleAgent.java:242`.

**`hasActiveQuorum`** (`ClusterMember.java:828–845`): Counts active members; returns true when count reaches `quorumThreshold`.

**`hasQuorumAtPosition`** (`ClusterMember.java:907–925`): Counts members where `isActive && leadershipTermId == this.leadershipTermId && logPosition >= position`. Returns true when count >= `quorumThreshold`.

**`isUnanimousLeader` / `isQuorumLeader`** (`ClusterMember.java:981–1036`): Both check `candidateTermId == member.candidateTermId` and `vote == TRUE`. Unanimous requires all non-closed-leader members; quorum requires majority with no FALSE votes.

---

## 3. Consensus Message Protocol

Schema: `aeron-cluster/src/main/resources/cluster/aeron-cluster-codecs.xml`, schema ID=111, version=17, little-endian SBE.

SBE message header (8 bytes): `blockLength:uint16, templateId:uint16, schemaId:uint16, version:uint16`.

### 3.1 Core Election/Consensus Messages (IDs 50–57, 75–76)

| Msg | Template ID | Fields (type) | Sender state(s) | Receiver handling |
|-----|-------------|---------------|-----------------|-------------------|
| **CanvassPosition** | 50 | `logLeadershipTermId:int64`, `logPosition:int64`, `leadershipTermId:int64`, `followerMemberId:int32`, `protocolVersion:version_t` | CANVASS, NOMINATE (periodic) | `onCanvassPosition` → update member log position; if existing leader, send NLT back. `ConsensusPublisher.java:53–89`, `Election.java:284–332`. |
| **RequestVote** | 51 | `logLeadershipTermId:int64`, `logPosition:int64`, `candidateTermId:int64`, `candidateMemberId:int32`, `protocolVersion:version_t` | CANDIDATE_BALLOT (per peer not yet sent) | `onRequestVote` → `placeVote(true/false)` based on `compareLog` and `candidateTermId` check. `ConsensusPublisher.java:91–129`, `Election.java:334–380`. |
| **Vote** | 52 | `candidateTermId:int64`, `logLeadershipTermId:int64`, `logPosition:int64`, `candidateMemberId:int32`, `followerMemberId:int32`, `vote:BooleanType` | Any (response to RequestVote) | `onVote` → if CANDIDATE_BALLOT and matching termId+candidateId: record vote and log position on member. `ConsensusPublisher.java:131–170`, `Election.java:383–409`. |
| **NewLeadershipTerm** | 53 | `logLeadershipTermId:int64`, `nextLeadershipTermId:int64`, `nextTermBaseLogPosition:int64`, `nextLogPosition:int64`, `leadershipTermId:int64`, `termBaseLogPosition:int64`, `logPosition:int64`, `leaderRecordingId:int64`, `timestamp:time_t`, `leaderMemberId:int32`, `logSessionId:int32`, `appVersion:version_t`, `isStartup:BooleanType`, `commitPosition:int64` (v15+) | LEADER_LOG_REPLICATION, LEADER_REPLAY, LEADER_READY (on interval) | `onNewLeadershipTerm` → complex dispatch: FOLLOWER_BALLOT/CANDIDATE_BALLOT/CANVASS → set leaderMember, decide FOLLOWER_LOG_REPLICATION or FOLLOWER_REPLAY based on appendPosition vs termBaseLogPosition. `ConsensusPublisher.java:172–232`, `Election.java:411–533`. |
| **AppendPosition** | 54 | `leadershipTermId:int64`, `logPosition:int64`, `followerMemberId:int32`, `flags:uint8` | FOLLOWER_LOG_REPLICATION, FOLLOWER_REPLAY, FOLLOWER_READY | `onAppendPosition` → update member logPosition; `trackCatchupCompletion`. Flags: 0=NONE, 1=CATCHUP. `ConsensusPublisher.java:234–271`, `Election.java:535–554`. |
| **CommitPosition** | 55 | `leadershipTermId:int64`, `logPosition:int64`, `leaderMemberId:int32` | LEADER_LOG_REPLICATION, LEADER_REPLAY (on interval) | `onCommitPosition` → update `notifiedCommitPosition`; extend `replicationDeadlineNs`; if LEADER_READY and higher termId → enter new election. `ConsensusPublisher.java:273–306`, `Election.java:557–587`. |
| **CatchupPosition** | 56 | `leadershipTermId:int64`, `logPosition:int64`, `followerMemberId:int32`, `catchupEndpoint:varAsciiEncoding` | FOLLOWER_CATCHUP_INIT | `onCatchupPosition` → leader starts archive replay to follower's catchup endpoint. `ConsensusPublisher.java:308–349`, `ConsensusModuleAgent.java:1147`. |
| **StopCatchup** | 57 | `leadershipTermId:int64`, `followerMemberId:int32` | Leader (when catchup completes) | `onStopCatchup` → follower tears down catchup replay. `ConsensusPublisher.java:351–381`. |
| **TerminationPosition** | 75 | `leadershipTermId:int64`, `logPosition:int64` | Leader (graceful shutdown) | `onTerminationPosition` → follower schedules termination. `ConsensusPublisher.java:383–415`. |
| **TerminationAck** | 76 | `leadershipTermId:int64`, `logPosition:int64`, `memberId:int32` | Follower (after reaching termination position) | `onTerminationAck` → leader collects acks before terminating. `ConsensusPublisher.java:417–450`. |

Note: comments `<!-- keep this under 32 - 8 = 24 bytes for efficiency -->` at IDs 54 and 55 — these messages are deliberately kept to 24 bytes of payload to fit in a single cache line alongside the 8-byte SBE header.

### 3.2 Log-Encoded Messages (written into Aeron log, not consensus channel)

| Msg | ID | Key fields | Purpose |
|-----|----|-----------|---------|
| NewLeadershipTermEvent | 24 | `leadershipTermId`, `logPosition`, `timestamp`, `termBaseLogPosition`, `leaderMemberId`, `logSessionId`, `timeUnit`, `appVersion` | Written to replicated log to mark term start; read back during replay. |
| SessionMessageHeader | 1 | `leadershipTermId`, `clusterSessionId`, `timestamp` | Wraps application messages in the replicated log. |
| TimerEvent | 20 | `leadershipTermId`, `correlationId`, `timestamp` | Timer trigger persisted in log. |
| SessionOpenEvent | 21 | `leadershipTermId`, `correlationId`, `clusterSessionId`, `timestamp`, `responseStreamId`, `responseChannel`, `encodedPrincipal` | Session open persisted in log. |
| SessionCloseEvent | 22 | `leadershipTermId`, `clusterSessionId`, `timestamp`, `closeReason` | Session close persisted in log. |
| MembershipChangeEvent | 25 | `leadershipTermId`, `logPosition`, `timestamp`, `leaderMemberId`, `clusterSize`, `changeType`, `memberId`, `clusterMembers` | Membership changes persisted in log. |

### 3.3 Auxiliary Consensus Messages (not election-critical for Rune Phase 2)

`BackupQuery` (77), `BackupResponse` (78), `HeartbeatRequest` (79), `HeartbeatResponse` (80), `StandbySnapshot` (81) — used by `ClusterBackupAgent` and standby node functionality. Archive-dependent. → Phase 3.

---

## 4. Single-Writer Agent + Threading

### 4.1 Threading Model

`ConsensusModuleAgent implements Agent` (`ConsensusModuleAgent.java:122`). The entire consensus state machine runs on **a single dedicated thread** driven by `AgentRunner` (or `AgentInvoker` for embedded use).

- **`AgentRunner`** wraps the agent: `ConsensusModule.java:299–300`:
  ```java
  conductorRunner = new AgentRunner(ctx.idleStrategy(), ctx.errorHandler(), ctx.errorCounter(), conductor);
  AgentRunner.startOnThread(consensusModule.conductorRunner, ctx.threadFactory());  // line 343
  ```
- No locking anywhere in the consensus hot path. Single-writer discipline enforced by the thread model.
- `aeronClientInvoker` is called from within `doWork` (`ConsensusModuleAgent.java:2387`) to interleave Aeron media driver I/O on the same thread when `useConductorAgentInvoker=true`.

### 4.2 `doWork()` Structure

`ConsensusModuleAgent.java:403–476`:

```
doWork(nowNs):
  1. clusterClock.time() + convertToNanos() — get timestamp + ns
  2. dutyCycleTracker.measureAndUpdate(nowNs)
  3. if (nowNs >= slowTickDeadlineNs):
       slowTickWork(nowNs)   — aeronClientInvoker.invoke(), pollArchiveEvents(),
                               sessionManager.sendRedirects/Rejections, checkControlToggle,
                               processAllPendingSessions/checkSessions, checkNodeControlToggle,
                               processPendingStandbySnapshotNotifications, recordingLogValidator
       slowTickDeadlineNs = nowNs + SLOW_TICK_INTERVAL_NS (10ms) if no work, else nowNs+1
  4. consensusAdapter.poll()  — receive consensus messages, dispatch to onXxx handlers (up to 10 fragments)
  5. if (election != null):
       election.doWork(nowNs)  — run election state machine
     else:
       consensusWork(timestamp, nowNs)  — leader/follower normal operation:
         timerService.poll, PendingServiceMessageTracker.poll,
         ingressAdapter.poll, updateLeaderPosition / updateFollowerPosition,
         consensusModuleAdapter.poll, pollStandbySnapshotReplication
  6. consensusModuleExtension.doWork(nowNs) (if present)
  7. clusterTimeConsumer.accept(timestamp)
  return workCount
```

### 4.3 `IdleStrategy`

`ConsensusModuleAgent` itself `implements IdleStrategy` (`line 122`) and delegates to `ctx.idleStrategy()`. The `AgentRunner` calls `idle(workCount)` after each `doWork`. When `workCount==0`, `idleStrategy.idle()` is called; when `workCount>0`, `idleStrategy.reset()`. Default configurable; `BackoffIdleStrategy` typical for production.

`ConsensusModuleAgent.java:524–561`:
```java
public IdleStrategy idleStrategy() { return this; }
public void idle() { idleStrategy.idle(); }
public void idle(int workCount) {
    idleStrategy.idle(workCount);
    if (0 == workCount) { ... }  // mark file update
}
public void reset() { idleStrategy.reset(); }
```

### 4.4 Election Driving

`Election.doWork(nowNs)` is called directly from `ConsensusModuleAgent.doWork()` on the same thread. No separate election thread. The election receives messages via `ConsensusAdapter.onFragment` → `consensusModuleAgent.onXxx(...)` → `election.onXxx(...)` all on the same thread, all synchronous.

### 4.5 `ConsensusAdapter` Poll

`ConsensusAdapter.java:35`: `FRAGMENT_LIMIT = 10`. Uses `FragmentAssembler` (handles fragmented SBE messages). Each `poll()` call processes up to 10 fragments from the consensus subscription.

---

## 5. Persistence

### 5.1 NodeStateFile (`NodeStateFile.java`)

File: `node-state.dat` in cluster directory. Memory-mapped (`MappedByteBuffer` via `IoUtil.mapNewFile`). `UnsafeBuffer` wraps the mapped region.

**Layout** (SBE-framed entries, 8-byte aligned):
```
[NodeStateHeader: version:int32, padding:int32]
[MessageHeader(8B)] [candidateTerm body]
[MessageHeader(8B)] [ClusterMembers body]
[MessageHeader(8B)] [nodeStateFooter (empty)]
```

**`candidateTerm` SBE message** (ID=301, schema `aeron-cluster-node-state-codecs.xml:37–41`):

| Field | Type | Meaning |
|-------|------|---------|
| `candidateTermId` | int64 | Highest candidate term ID seen by this node. Written via `putLongVolatile` for visibility. |
| `timestamp` | time_t (int64) | Epoch ms when candidateTermId was last updated. |
| `logPosition` | int64 | Log position when candidateTermId was updated. |

**Key operations:**
- `proposeMaxCandidateTermId(candidateTermId, logPosition, timestampMs)` (`NodeStateFile.java:274–285`): only writes if `candidateTermId > existing`. Returns the max. Write order: `logPosition` first, `timestamp` second, then `candidateTermIdOffset` via `putLongVolatile` (release semantics). `syncFile()` (MappedByteBuffer.force()) if `fileSyncLevel > 0`. `NodeStateFile.java:258–264`.
- `candidateTerm().candidateTermId()` reads via `buffer.getLongVolatile(candidateTermIdOffset)`. `NodeStateFile.java:362–365`.
- On INIT: `candidateTermId = max(nodeStateFile.candidateTerm().candidateTermId(), leadershipTermId)`. `Election.java:681`.

### 5.2 RecordingLog (`RecordingLog.java`)

File: `recording.log` in cluster directory (raw binary, not SBE). Fixed-size records, each 72+ bytes.

**Entry layout** (from `RecordingLog.java:58–92`):

| Field | Java type | Bytes | Meaning |
|-------|-----------|-------|---------|
| `recordingId` | long | 8 | Aeron Archive recording identifier |
| `leadershipTermId` | long | 8 | Leadership term this entry covers |
| `termBaseLogPosition` | long | 8 | Accumulated byte offset at start of this term |
| `logPosition` | long | 8 | Byte position when entry was created/committed (NULL_POSITION for open term) |
| `timestamp` | long | 8 | Cluster clock timestamp |
| `serviceId` | int | 4 | For snapshots: which service; for terms: `SERVICE_ID` sentinel |
| `type` | int | 4 | `ENTRY_TYPE_TERM` or `ENTRY_TYPE_SNAPSHOT` (high bit = invalid flag) |
| `archiveEndpoint` | variable | 4+N | For standby snapshots: remote archive endpoint |

**Entry class:** `RecordingLog.Entry` (`RecordingLog.java:100–200`). Fields are `public final`.

**Note:** RecordingLog is backed by Aeron Archive for actual data storage; the log file only records term metadata. This is Archive-dependent → **Phase 3** for Rune. For Phase 2, Rune needs equivalent durable term-metadata storage.

---

## 6. Low-Level Optimizations Checklist

Each is a **MUST** for the Rune C++ port.

### 6.1 Single-Writer / Agent Pattern
- **Aeron class:** `org.agrona.concurrent.Agent`, `AgentRunner`, `AgentInvoker`
- **What:** Entire consensus state machine (election + normal operation) runs on one dedicated thread. No locks on any hot path. Role transitions, member updates, timer ticks — all single-threaded.
- **Rune MUST:** `rune::consensus::ConsensusAgent` as a single-threaded duty-cycle loop, no mutexes inside.

### 6.2 `BufferClaim` / `tryClaim` Zero-Copy Send
- **Aeron class:** `io.aeron.logbuffer.BufferClaim`, `ExclusivePublication.tryClaim(length, claim)`
- **What:** Instead of copying a message to a buffer and then `offer()`-ing it, `tryClaim` directly reserves space in the Aeron term buffer and returns a `BufferClaim` view. The encoder writes directly into the term buffer; `commit()` makes it visible. `ConsensusPublisher.java:70–88` (all fixed-size messages use this path).
- **Rune MUST:** Use Aeron C++ `ExclusivePublication::tryClaim` for all fixed-size consensus messages (CanvassPosition, RequestVote, Vote, AppendPosition, CommitPosition, StopCatchup). Fall back to `offer(buffer, length)` for variable-length (CatchupPosition, BackupQuery).

### 6.3 Reusable SBE Encoders/Decoders (No Per-Message Allocation)
- **Aeron class:** Generated SBE encoder/decoder classes (e.g., `CanvassPositionEncoder`, `NewLeadershipTermDecoder`)
- **What:** All encoders/decoders are **fields of the publisher/adapter** (`ConsensusPublisher.java:36–51`, `ConsensusAdapter.java:37–52`). They are instantiated once at construction and reused for every message. SBE `wrapAndApplyHeader(buffer, offset, header)` re-wraps the encoder on the existing buffer.
- **Rune MUST:** C++ SBE generated codecs as members of the publisher/adapter classes. No per-message `new`.

### 6.4 `ExpandableArrayBuffer` for Variable-Length Messages
- **Aeron class:** `org.agrona.ExpandableArrayBuffer`
- **What:** `ConsensusPublisher` holds a single `ExpandableArrayBuffer buffer` (field, line 33) used for variable-length messages (BackupQuery, BackupResponse, etc.) that cannot use `tryClaim`. The buffer grows if needed but is reused across calls.
- **Rune MUST:** Single `ExpandableArrayBuffer`-equivalent per publisher for non-claim paths.

### 6.5 `UnsafeBuffer` for Memory-Mapped I/O
- **Aeron class:** `org.agrona.concurrent.UnsafeBuffer`
- **What:** `NodeStateFile` wraps its `MappedByteBuffer` in an `UnsafeBuffer` for direct memory access without bounds checking overhead. `putLongVolatile` / `getLongVolatile` used for `candidateTermId` to ensure visibility across threads (even though single-writer, the counter is observed by other threads). `NodeStateFile.java:84, 124`.
- **Rune MUST:** Use `mmap` + raw pointer writes with `std::atomic<int64_t>` for `candidateTermId` in the node state file. Call `msync(MS_SYNC)` equivalent when `fileSyncLevel > 0`.

### 6.6 Pre-Allocated `rankedPositions[]` for Quorum Computation
- **Aeron class:** primitive `long[]` array
- **What:** `quorumPosition(members, rankedPositions, nowNs, timeoutNs)` takes a caller-supplied array to avoid allocation during the quorum calculation. Array sized to `quorumThreshold(N)`. `ConsensusModuleAgent.java:242`: `rankedPositions = new long[ClusterMember.quorumThreshold(activeMembers.length)]`.
- **Rune MUST:** Pre-allocate `std::array<int64_t, K>` at startup; pass by reference to `quorum_position()`.

### 6.7 `ExclusivePublication` per Peer (No Contention)
- **Aeron class:** `io.aeron.ExclusivePublication`
- **What:** Each `ClusterMember` holds its own `ExclusivePublication` (`ClusterMember.java:56`). `ExclusivePublication` has no thread-safety overhead (vs. `Publication` which uses CAS). Since the consensus agent is single-writer, `ExclusivePublication` is safe and faster.
- **Rune MUST:** One `aeron::ExclusivePublication` per peer, created at startup.

### 6.8 `NanoClock` / `ClusterClock` for All Timing
- **Aeron class:** `org.agrona.concurrent.NanoClock`, `io.aeron.cluster.service.ClusterClock` (interface: `NanosecondClusterClock`, `MillisecondClusterClock`)
- **What:** All election timeouts (`electionTimeoutNs`, `leaderHeartbeatTimeoutNs`, `startupCanvassTimeoutNs`) are in nanoseconds. `clusterClock.timeNanos()` called once per `doWork` cycle; the result is passed through as `nowNs`. `ConsensusModuleAgent.java:405–406`.
- **Rune MUST:** Single `clock_gettime(CLOCK_MONOTONIC_RAW)` call per duty cycle. All timeout comparisons in nanoseconds.

### 6.9 `AtomicCounter` for State Visibility
- **Aeron class:** `org.agrona.concurrent.status.AtomicCounter`
- **What:** `electionStateCounter.setRelease(newState.code())` on every state transition (`Election.java:1433`). The election state is visible to external observers (tools, monitoring) via a counter in Aeron's Counters map. Also `clusterRoleCounter` for LEADER/FOLLOWER/CANDIDATE.
- **Rune MUST:** Expose election state and role via Aeron counters or equivalent atomic variables.

### 6.10 `FragmentAssembler` for Reassembly
- **Aeron class:** `io.aeron.FragmentAssembler`
- **What:** `ConsensusAdapter` wraps `this` (a `FragmentHandler`) with a `FragmentAssembler` (`ConsensusAdapter.java:54`). This transparently reassembles fragmented SBE messages before dispatching. Necessary because `NewLeadershipTerm` is large (14 fields).
- **Rune MUST:** Use Aeron C++ `FragmentAssembler` on the consensus subscription.

---

## 7. Rune Port Mapping

| Aeron Java Class/Concept | Rune C++ Unit | Notes |
|--------------------------|---------------|-------|
| `ElectionState` enum (18 states) | `rune::consensus::ElectionState` enum class | Exact 1:1 mapping, same ordinal codes |
| `Election` class | `rune::consensus::Election` | All fields, all state handlers, all `onXxx` methods |
| `ClusterMember` | `rune::consensus::ClusterMember` | `compareLog`, `quorumThreshold`, `quorumPosition`, `hasQuorumAtPosition`, `isUnanimousLeader`, `isQuorumLeader`, `willVoteFor` |
| `ConsensusModuleAgent` | `rune::consensus::ConsensusAgent` | `doWork()`, role transitions, all `onXxx` dispatch, `AgentRunner` → `std::thread` + duty-cycle loop |
| `ConsensusPublisher` | `rune::consensus::ConsensusPublisher` | One `ExclusivePublication` per peer; reusable SBE encoders; `tryClaim` for fixed-size |
| `ConsensusAdapter` | `rune::consensus::ConsensusAdapter` | `FragmentAssembler` + SBE decoders; dispatches to `ConsensusAgent::onXxx` |
| SBE codecs (aeron-cluster-codecs.xml) | Generated C++ SBE from same XML | Same schema ID=111, same template IDs 50–57, 75–76 |
| `NodeStateFile` | `rune::consensus::NodeStateFile` | `mmap`-backed, `std::atomic<int64_t>` for `candidateTermId`, `msync` on commit |
| `RecordingLog` (term metadata) | `rune::consensus::TermLog` | Phase 2: implement term-metadata file without Archive dependency. Phase 3: integrate with Rune's own durable log. |
| `RecordingLog.Entry` | `rune::consensus::TermEntry` | Fields: `recordingId`, `leadershipTermId`, `termBaseLogPosition`, `logPosition`, `timestamp`, `serviceId`, `type`, `isValid` |
| `RecordingReplication` (Archive replication) | → **Phase 3** | Aeron Archive-backed. Rune uses its own log replication. |
| `LogReplay` (Archive replay) | → **Phase 3** | Archive-backed. Phase 2: `LogReplay` is a stub/no-op or uses local Rune log. |
| `AgentRunner` / `AgentInvoker` | `rune::AgentRunner` (already in Rune) | Same pattern: single thread, `doWork()` duty cycle, `IdleStrategy` |
| `IdleStrategy` | `rune::BackoffIdleStrategy` / `rune::BusySpinIdleStrategy` | Match Agrona semantics: `idle(workCount)`, `reset()` |
| `ExclusivePublication.tryClaim` | `aeron::ExclusivePublication::tryClaim` (C++ Aeron client) | Same API in C++ Aeron |
| `BufferClaim` | `aeron::concurrent::logbuffer::BufferClaim` (C++ Aeron) | Same |
| `ExpandableArrayBuffer` | `rune::ExpandableArrayBuffer` or `std::vector<uint8_t>` | Single instance per publisher |
| `UnsafeBuffer` | `rune::UnsafeBuffer` (already in Rune/Agrona C++) | Same `org.agrona.concurrent.UnsafeBuffer` semantics |
| `AtomicCounter` (Aeron Counters) | `aeron::Counter` (C++ Aeron) | For `electionStateCounter`, `clusterRoleCounter`, `commitPosition` |
| `ClusterClock` / `NanoClock` | `clock_gettime(CLOCK_MONOTONIC_RAW)` | Once per `doWork`; propagate `nowNs` everywhere |
| `timerService` (`WheelTimerService`) | `rune::consensus::TimerService` | For Rune Phase 2; timer events are consensus-module internal |
| `quorumPosition` pre-alloc array | `std::array<int64_t, K>` member of `ConsensusAgent` | Sized at startup: `quorumThreshold(memberCount)` |
| `Cluster.Role` enum (FOLLOWER=0, CANDIDATE=1, LEADER=2) | `rune::consensus::Role` | Same ordinal codes for `clusterRoleCounter` |

### Phase 3 (Archive-dependent) items deferred:
- `RecordingReplication` (log replication via Archive)
- `LogReplay` (replay via Archive recording)
- `AeronArchive` integration for `logRecordingId` tracking
- `RecordingLog` write path (currently writes term entries to Archive)
- `BackupQuery/Response`, `StandbySnapshot`

---

## File:Line Index (Key References)

| Concept | File:Line |
|---------|-----------|
| ElectionState enum | `ElectionState.java:26–115` |
| Election fields | `Election.java:71–108` |
| Election.doWork dispatch | `Election.java:181–260` |
| init() handler | `Election.java:653–698` |
| canvass() handler | `Election.java:700–729` |
| nominate() handler | `Election.java:732–751` |
| candidateBallot() handler | `Election.java:754–793` |
| followerBallot() handler | `Election.java:796–807` |
| leaderLogReplication() handler | `Election.java:809–825` |
| leaderReplay() handler | `Election.java:828–865` |
| leaderInit() handler | `Election.java:868–874` |
| leaderReady() handler | `Election.java:877–894` |
| followerLogReplication() handler | `Election.java:897–957` |
| followerReplay() handler | `Election.java:960–1017` |
| followerCatchupInit() handler | `Election.java:1020–1055` |
| followerCatchupAwait() handler | `Election.java:1058–1086` |
| followerCatchup() handler | `Election.java:1089–1112` |
| followerLogInit() handler | `Election.java:1115–1131` |
| followerLogAwait() handler | `Election.java:1134–1163` |
| followerReady() handler | `Election.java:1166–1179` |
| onCanvassPosition | `Election.java:284–332` |
| onRequestVote | `Election.java:334–380` |
| onVote | `Election.java:383–409` |
| onNewLeadershipTerm | `Election.java:411–533` |
| onAppendPosition | `Election.java:535–554` |
| onCommitPosition | `Election.java:557–587` |
| state() transition method | `Election.java:1385–1438` |
| compareLog (exact) | `ClusterMember.java:1173–1197` |
| quorumThreshold | `ClusterMember.java:853–856` |
| quorumPosition | `ClusterMember.java:867–895` |
| hasQuorumAtPosition | `ClusterMember.java:907–925` |
| isUnanimousLeader | `ClusterMember.java:981–1002` |
| isQuorumLeader | `ClusterMember.java:1015–1036` |
| willVoteFor | `ClusterMember.java:1292–1298` |
| ClusterMember fields | `ClusterMember.java:44–66` |
| CMA fields | `ConsensusModuleAgent.java:128–208` |
| CMA.doWork() | `ConsensusModuleAgent.java:403–476` |
| CMA.onCanvassPosition | `ConsensusModuleAgent.java:902–960` |
| CMA.onRequestVote | `ConsensusModuleAgent.java:962–980` |
| CMA.onVote | `ConsensusModuleAgent.java:982–997` |
| CMA.onNewLeadershipTerm | `ConsensusModuleAgent.java:999–1076` |
| CMA.onAppendPosition | `ConsensusModuleAgent.java:1078–1098` |
| CMA.onCommitPosition | `ConsensusModuleAgent.java:1108–1145` |
| AgentRunner setup | `ConsensusModule.java:292–343` |
| IdleStrategy | `ConsensusModuleAgent.java:524–561` |
| rankedPositions pre-alloc | `ConsensusModuleAgent.java:242` |
| ConsensusPublisher encoders | `ConsensusPublisher.java:33–51` |
| tryClaim pattern | `ConsensusPublisher.java:70–88` |
| ConsensusAdapter decoders | `ConsensusAdapter.java:37–52` |
| FRAGMENT_LIMIT=10 | `ConsensusAdapter.java:35` |
| NodeStateFile layout | `NodeStateFile.java:39–69` |
| proposeMaxCandidateTermId | `NodeStateFile.java:274–285` |
| updateCandidateTermId (volatile write) | `NodeStateFile.java:258–264` |
| candidateTermId read (volatile) | `NodeStateFile.java:362–365` |
| CandidateTerm SBE schema | `aeron-cluster-node-state-codecs.xml:37–41` |
| RecordingLog.Entry fields | `RecordingLog.java:100–200` |
| RecordingLog on-disk layout | `RecordingLog.java:58–92` |
| SBE schema consensus section | `aeron-cluster-codecs.xml:443–593` |
| AppendPosition efficiency comment | `aeron-cluster-codecs.xml:494,504` |
