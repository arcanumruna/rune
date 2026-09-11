# Opus Audit — libs/replication (Phase 3: ISR replication + failover)

Date: 2026-09-11
Scope: `libs/replication/{include,src,tests}` (read-only). Traced produce→replicate→
majority-HW→committed-read, follower catch-up, epoch-change truncation, and
kill-leader→failover end to end.

## Verdict: FINDINGS (multiple correctness/EOS issues; none stop the happy path, but
several violate the durability/EOS guarantees the module claims, and are masked by
weak/vacuous tests).

Severity counts: BLOCKER 2 · CORRECTNESS 4 · SILENT 5 · CLEAN 4 · TEST 6

---

## BLOCKER

### B1. Per-follower LEO is monotonic and never reset → HW can advance past a
###     non-majority-held offset (committed-loss vector on truncation/re-fetch)
`HighWatermark.h:99-106` (`updateFollowerFetchOffset`) stores `max(prev, fetchOffset)`
per follower and NEVER decreases it. `PartitionReplica::onFetchRequest`
(`PartitionReplica.cpp:218`) feeds every follower's `req.fetchOffset` into it.

Break: a follower that truncates its uncommitted (or divergent) tail on an epoch
change (`applyReplicated`/`becomeFollower`, `PartitionReplica.cpp:108-148,268-276`)
then re-fetches from a **lower** offset. The leader ignores the lower value and keeps
the follower's stale higher LEO in `followerLeo_`. That stale LEO still counts toward
the majority in `computeHw()` (`HighWatermark.h:158-173`), so HW reflects offsets a
majority no longer physically holds. With the global monotonic floor `prevHw_`
(`HighWatermark.h:137-142,178`) this HW can never be walked back. This is the classic
"HW advanced past data a majority holds" EOS violation.

Fix direction: track per-follower LEO as an authoritative value (allow it to go
backwards when the follower reports a lower fetchOffset under the current epoch), or
reset `followerLeo_` on `setIsr`/epoch change, and compute HW from live values. The
"ignore stale/late message" concern must be handled at the transport/epoch layer, not
by a monotonic clamp that outlives truncation.

### B2. New leader starts with HW=0 after `becomeLeader` → committed prefix becomes
###     unreadable and HW regresses on failover (visible EOS regression)
`becomeLeader` (`PartitionReplica.cpp:84-106`) seeds `leaderState_` but never restores
`prevHw_`; `ReplicaLeaderState` has no way to set it. A node that was a follower has
`leaderState_.prevHw_ == 0`. After election the new leader reports `highWatermark()==0`
(`PartitionReplica.cpp:392-398`) until followers re-ack, and `read(off,·)` returns empty
for all committed offsets in that window (`PartitionReplica.cpp:246-254`). So the
committed prefix is momentarily lost to consumers and HW visibly regresses across
failover — the exact property the spec forbids.

Why tests miss it: every failover test asserts only `LEO >= hw` and then *drives again*
until HW re-advances (e.g. `ClusterDisasterRecoveryIntegrationTest.cpp:191-196`,
`PartitionFailoverTest.cpp:1000-1003`). None asserts HW immediately post-election.

Fix direction: on `becomeLeader`, initialise the leader-state HW floor to the node's
known committed offset (carried in metadata / derived from the epoch cache), and expose
a `ReplicaLeaderState::setInitialHighWatermark()`.

---

## CORRECTNESS

### C1. Committed `read()` does not clamp to HW → returns uncommitted records
`PartitionReplica::read` (`PartitionReplica.cpp:246-254`) gates only on
`fetchOffset >= hw` then calls `log_->read(fetchOffset, maxBytes)` with the caller's
`maxBytes`. The Log returns whole batches up to `maxBytes` regardless of HW, so
`read(0, 1<<20)` yields the full log INCLUDING offsets `>= HW` (uncommitted). A consumer
reading "committed" data can observe records above the high-watermark. Doc for the
method (`PartitionReplica.h:168-174`) claims "≤ highWatermark".
Fix: bound the read span to `[fetchOffset, HW)` (translate HW-offset to a byte/`maxBytes`
limit, or post-filter batches with `baseOffset+lastOffsetDelta < HW`).

### C2. `becomeFollower` truncation boundary can drop committed data or fail to fence
     when the epoch cache lacks the new epoch
`becomeFollower` (`PartitionReplica.cpp:114-141`) falls back to `truncOff = committedHw`
when `truncationOffsetForEpochChange` returns -1, then clamps `truncOff >= committedHw`.
`committedHw` here is `highWatermark()` of the OLD role. For a node that was a follower,
that is `followerHw_ = min(localLeo, leaderHw)` — which may be **below** the true
committed offset if the follower never received the HW-only update (or above-real if B1
inflated the leader HW it copied). Truncating to a follower-local HW that lags the real
committed prefix is safe-ish (drops some committed-but-locally-unconfirmed tail and
re-fetches), but truncating on a *stale/incorrect* HW is fragile and undocumented.
Additionally the epoch-cache path only has an entry for the node's own last leadership
(`assign(epoch, leo)` at `:103`), so a follower demoted across several epochs may have no
cache entry for the boundary and silently rely on the HW fallback.
Fix: derive the truncation boundary from a real `OffsetsForLeaderEpoch` exchange with the
new leader (Kafka's protocol), not from a local HW guess.

### C3. Election ranks by LEO but never verifies the candidate holds the committed
     prefix; `knownLeo` for unregistered/never-refreshed nodes defaults to 0
`electNewLeader` (`PartitionFailoverCoordinator.h:260-338`) chooses the ISR member with
the highest `logEndOffset()`. It relies on the ISR invariant ("all ISR members hold all
committed data"), but that invariant is exactly what B1/B2 can break, and nothing here
re-checks `LEO >= committedHw`. `leoOf` (`:379-384`) returns 0 for a node not found in
`entries_`, so a mis-registration makes a node look like it has an empty log (silently
un-electable rather than erroring). A safety-critical election should assert the chosen
leader's LEO ≥ last committed offset.
Fix: pass the last committed offset into election and reject candidates with
`LEO < committedOffset`; hard-error on unknown/unregistered ISR members instead of
defaulting LEO to 0.

### C4. `recomputeHw()` writes the leader's own LEO into the follower map under
     `selfId` — semantically wrong and a latent bug if `selfId` ever appears as a
     non-leader ISR slot
`PartitionReplica::recomputeHw` (`PartitionReplica.cpp:413-432`) calls
`leaderState_.updateFollowerFetchOffset(cfg_.selfId, log_->logEndOffset())` purely to
force a recompute. This pollutes `followerLeo_[selfId]`. It happens to be harmless only
because `computeHw()` special-cases `member == leaderId_` and reads `leaderLeo_` instead
(`HighWatermark.h:160-165`). If `leaderId_` and `selfId` ever diverge (they are set from
the same value today, but nothing enforces it), the leader's slot would be counted from
a stale `followerLeo_` map entry. This is an invariant-not-enforced trap; the long
apologetic comment at `:417-431` documents that the author knew the design was wrong.
Fix: give `ReplicaLeaderState` an explicit `recompute()`/`refresh()` method; delete the
`updateFollowerFetchOffset(selfId, …)` hack.

---

## SILENT

### S1. `ReplicaLeaderState::highWatermark()` returns a cached value; `setLeaderLeo`
     and `setIsr` do NOT recompute
`HighWatermark.h:120-122` returns `prevHw_`, updated only inside
`updateFollowerFetchOffset` (`:99-106`). After `setLeaderLogEndOffset` (`:87-89`) or
`setIsr` (`:77-79`) the returned HW is stale until some follower update fires. The header
doc (`:112-122`) still describes `highWatermark()` as *computing* sort→majority; code and
doc disagree. Every unit test must call a dummy `updateFollowerFetchOffset(...)` "to
trigger recompute" (e.g. `HighWatermarkTest.cpp:50,66,137,357,412`) — a design smell that
hides the staleness. In production, a leader-only ISR that appends without any follower
fetch would report a stale HW (masked today only because `append()` calls `recomputeHw()`
which does the selfId hack).
Fix: make `highWatermark()` compute (or recompute on every mutator); align the doc.

### S2. `becomeLeader` sets `replicas == isr` losing true replica-set knowledge
`PartitionReplica.cpp:89` (`std::vector<NodeId> replicas = isr; // simplification`).
On the very first election the `IsrManager` is rebuilt with `replicas == isr`, so any
replica currently OUT of the ISR is dropped from `replicas_` and can never be re-expanded
by `maybeExpandIsr` (`IsrManager.h:166-181` iterates `replicas_`). Under-replication
repair then leans entirely on the coordinator's `repairCandidates_` path. Silent loss of
membership information.
Fix: carry the real replica set into `becomeLeader` and preserve it.

### S3. Off-by-one/`maxBytes` truncation risk in recorder read of a committed range
`PartitionRecorder::onCommitted` (`PartitionRecorder.cpp:69-75`) sizes the Log read as
`maxBytes = min(endOffset - recordFrom, 64MiB)`. `endOffset - recordFrom` is an **offset
count**, not a byte count; it is passed as a *byte* budget to `log_->read`. For batches
larger than one byte per offset this is usually harmless (over-reads), but for a committed
range of N offsets whose batches total > N bytes cap … here it's the opposite: cap is
offset-count which is almost always smaller than the byte length, so a multi-record or
large-payload batch can be **partially read and the frame-walk stops early**
(`:82-94` breaks on incomplete frame), silently under-recording. Only saved today because
tests use tiny 8–16 byte payloads with 1 record/batch.
Fix: pass a byte-based budget (or read iteratively until `endOffset` is covered).

### S4. `applyReplicated` "looksLikeRecordBatch" magic-byte sniff can misclassify
`PartitionReplica.cpp:287-323` decides between "verbatim RecordBatch frames" and "raw
user bytes" by checking one magic byte at `kMagicOffset`. A raw user payload whose byte
at that offset equals `kMagicValue (2)` is misparsed as a batch frame (and vice-versa),
producing wrong `lastOff`/append behavior. This dual-path exists only to keep unit tests
that inject raw bytes passing (`:316-322`); it is a production-fragile heuristic.
Fix: make the wire format unambiguous (frame type flag in FetchResponse), drop the sniff.

### S5. FetchResponse decode trusts `recordsLength` with no bounds check against the
     buffer
`ReplicationMessages.h:262-267`: `len` is read then used to form a span
`{vp+4, len}` with only a `len<0 → 0` guard. A corrupt/truncated buffer with a large
positive `len` yields an out-of-bounds span (later `memcpy`/iteration reads past the
buffer). Producer side (`InMemoryTransport`/`FilteringTransport`) always copies exactly,
so tests never hit it, but a real Aeron frame with a bad length is UB.
Fix: pass the buffer length into `decode` and clamp `len` to remaining bytes.

---

## CLEAN

- CL1. `HighWatermark.h:112-122` doc comment describes an algorithm the code no longer
  implements (see S1). Misleading.
- CL2. `PartitionReplica.cpp:417-431` — large apologetic comment narrating a known-wrong
  design instead of fixing it (see C4). Dead reasoning left in shipping code.
- CL3. `PartitionReplica.h:253-254` keeps both a raw `isrManager_` and
  `isrManagerOwned_` unique_ptr; the raw pointer always aliases the owned one. Redundant.
- CL4. `IsrManager::maybeExpandIsr` `nowNs` param is unused (`IsrManager.h:166`); the doc
  admits "reserved for future". `RecordBatchRef` (`Types.h:33-35`) is defined but unused
  anywhere in the module. Dead code.

---

## TEST (vacuous / false-green)

- T1. **No-committed-loss tests only check `!empty()` / `LEO>=hw`, never byte-equality of
  the committed prefix on the NEW leader**, and re-drive until HW recovers, masking B2.
  `PartitionReplicaTest.cpp:540-602` (`NoCommittedRecordLostOnEpochChange`) reads only the
  leader and only asserts non-empty. `PartitionFailoverTest.cpp:185-216,990-1021`.
- T2. `read()` boundary tests assert only emptiness/non-emptiness, so C1 (uncommitted
  records returned) is invisible: `PartitionReplicaTest.cpp:306-345`,
  `ClusterDisasterRecoveryIntegrationTest.cpp:91-103`.
- T3. HW unit tests are structurally forced to call `updateFollowerFetchOffset(...)` to
  "trigger recompute" (`HighWatermarkTest.cpp:50,66,77,137,357,412`), encoding the S1 bug
  as expected behavior rather than catching it.
- T4. Fencing tests never deliver the stale request through the *demoted old leader's*
  live re-fetch after it truncates and re-fetches from a lower offset — so B1 (stale LEO
  inflating HW) is never exercised. All fencing coverage injects a hand-built stale
  `FetchRequestMsg` (`PartitionReplicaTest.cpp:391-426`,
  `PartitionFailoverTest.cpp:423-448`, integration test 12/20).
- T5. Byte-equality catch-up test admits it is only checking logical LEO, not bytes, in
  the 2-node local path, and the comment explicitly downgrades the guarantee
  (`ClusterDisasterRecoveryIntegrationTest.cpp:636-645`); the stronger byte check
  (`:661-675`, and repair test `PartitionFailoverTest.cpp:780-797`) only compares the
  WHOLE log read with the same `maxBytes`, so it would also pass if BOTH logs contained
  identical uncommitted tails — it does not isolate `[0, HW)`.
- T6. `driveUntil` return values are checked with `ASSERT_TRUE` in most places (good), but
  several ad-hoc loops in the integration test ignore the terminal predicate result and
  rely on a following `EXPECT_GE` that is itself satisfiable by a stalled-but-nonzero
  state (e.g. `ClusterDisasterRecoveryIntegrationTest.cpp:608-613`: `expanded =
  (isr==3 || isr>=2)` — the `>=2` disjunct makes "still has the leader + itself" pass even
  if no follower rejoined).

---

## End-to-end trace notes (for reproduction)

- Produce→HW: `append` (`PartitionReplica.cpp:154-175`) → `recomputeHw` selfId-hack
  (`:413-432`) → `computeHw` majority math (`HighWatermark.h:144-174`). Majority index
  math `|ISR|/2+1` and `leos[majority-1]` is correct for 1/2/3/5 (verified against
  `HighWatermarkTest`). `kMaxIsr=32` silently caps HW computation for ISR>32
  (`HighWatermark.h:154-156`) — not a practical concern but an unenforced magic number.
- Follower catch-up: `nextFetch` uses `log_->logEndOffset()` (`:337-346`); leader serves
  `log_->read(fetchOffset, maxBytes)` verbatim (`:221-234`); follower re-frames via the
  magic sniff (S4). Byte-identity holds ONLY on the verbatim path.
- Epoch truncation: `EpochFencing` helpers and `LeaderEpochCache::truncateOffsetForEpoch`
  are correct and well-tested (`EpochFencingTest.cpp`). The weakness is the *source* of
  the boundary at role-change (C2), not the helper.
- Failover: coordinator stale-signal / double-election guards
  (`PartitionFailoverCoordinator.h:228-241`) are correct and well-tested
  (`PartitionFailoverTest.cpp:1295-1383`). Non-ISR ineligibility holds (`:266-275`).
- Durability: `FileRecordingStore` torn-tail recovery reads payloads to detect partial
  frames (`FileRecordingStore.cpp:88-166`) and physically truncates — sound. But
  `append` only `flush()`es, never `fsync`s (`:199`), consistent with the documented
  "async archive" trade-off (`FileRecordingStore.h:19-24`) — acceptable but worth noting
  that a crash between flush and OS write-back loses the tail (recovered as torn).

---

## Re-audit (commit 38e2bbe)

Verdict: **FINDINGS REMAIN**. The B2 fix introduced a NEW BLOCKER (HW over-claim on
failover → uncommitted-read). C1/S1/S3 are genuinely fixed with real tests. B1 is
code-correct but its tests are non-distinguishing (floor masks old-vs-new). C2/C3 are
partial. Deferrals S2/S4/S5 accepted.

Build note: `cmake`/`ctest` unavailable in the audit environment; verification is by
code + test reasoning, not by re-running the suite. Commit claims 223/223 replication
tests pass.

### Per-finding status

- **B1 — PARTIAL (code FIXED, tests non-distinguishing).**
  `HighWatermark.h:121-125` now does `followerLeo_[id] = fetchOffset` (SET not max) and
  `recomputeHw()` recomputes from live values. The global floor `prevHw_`
  (`:169-174`) never regresses. Code is correct: HW cannot advance past a live majority,
  cannot regress below committed. **But the new tests do NOT fail under the old `max()`**:
  in every case (`HighWatermarkTest.cpp:99-127` 3-node, `PartitionReplicaTest.cpp:229-345`
  2-node) the monotonic floor already covers the differing value, so `highWatermark()`
  returns the identical number with `max()` or `SET`. `AuditFixB1` (`:328-342`) asserts
  HW stays 10 not 15 — but the old code's stale LEO was 10, not 15, so it also yields 10.
  No test drives the follower's tracked LEO to a value that would inflate a FRESH
  majority above the floor. The unit tests would be strictly better if they asserted a
  fresh recompute after `setIsr` shrink+re-add reflects the LOWER value (the comment at
  `HighWatermarkTest.cpp:71-77` describes this but the assertion doesn't isolate it).
  Recommend a test where the floor has NOT yet reached the stale value.

- **B2 — NOT-FIXED / regressed to a NEW BLOCKER.** See NB1 below. The floor is set to
  `log_->logEndOffset()` (full LEO), not the committed offset. The code comment
  (`PartitionReplica.cpp:105`) asserts "its LEO is a safe lower bound for the committed
  HW" — this is backwards: LEO is an UPPER bound. When the elected node has an uncommitted
  tail (localLeo > followerHw_ — reachable: followers append fetched-but-uncommitted
  records, `applyReplicated` sets `followerHw_ = min(localLeo, resp.highWatermark)`), the
  floor over-claims and uncommitted (possibly divergent) records become committed-readable.
  The B2 test (`PartitionReplicaTest.cpp:163-222`) only exercises LEO == committed
  (follower fully caught up), so it never trips the bug. Byte-equal failover test is real
  but only for the caught-up case.

- **C1 — FIXED.** `read()` (`PartitionReplica.cpp:269-302`) walks RecordBatch frames and
  stops at the first batch with `baseOffset >= hw`. `AuditFixC1`
  (`PartitionReplicaTest.cpp:60-149+`) commits 3, appends 2 uncommitted, asserts read
  returns exactly 3 batches with no baseOffset >= HW — genuinely fails under the old
  unclamped code. Boundary is exact for the current single-record-per-batch leader path
  (`makeBatch` → lastOffsetDelta=0). NOTE (minor, NB3): the clamp is `baseOffset >= hw`,
  not `baseOffset+lastOffsetDelta >= hw`; a multi-record batch straddling HW would be
  returned whole and expose offsets ≥ HW. Not reachable today (leader appends 1 rec/batch;
  followers never serve read()), but latent if batching is introduced.

- **C2 — PARTIAL.** `becomeFollower` (`PartitionReplica.cpp:138-164`) now clamps
  `truncOff >= committedHw` and documents the deferral of full OffsetsForLeaderEpoch; this
  path never drops committed data. HOWEVER the SECOND truncation site,
  `applyReplicated` (`:316-324`), truncates on `resp.leaderEpoch > epoch_` via the epoch
  cache with **no committedHw floor** and `truncateLog` (`:475-486`) has no
  commit-prefix guard. Currently latent (the coordinator always calls `becomeFollower`
  first, bumping `epoch_`, so this branch is dead in the coordinated flow), but it is an
  asymmetric committed-loss vector the moment a follower adopts an epoch purely via a
  FetchResponse. See NB2.

- **C3 — PARTIAL.** `electNewLeader` (`PartitionFailoverCoordinator.h:265-378`) asserts
  every ISR candidate is registered (no silent leoOf=0) — solid — and picks highest-LEO.
  But the `LEO >= committedHw` filter reads `committedHw` from the OLD leader's replica
  and defaults to 0 when the old leader is unregistered (`:291-297`) — which is exactly
  the crash-failover case. So in a real leader crash the committed-prefix filter is
  vacuous (`LEO >= 0`) and safety again rests on the ISR invariant (which NB1 can now
  violate). No false-negative "no eligible leader" (uses `>=`). Fix: thread the last
  committed offset from metadata, not from the dead leader's live replica.

- **S1 — FIXED.** `setLeaderLogEndOffset`/`setIsr`/`updateFollowerFetchOffset` all call
  `recomputeHw()` (`HighWatermark.h:88-125`); header doc rewritten to match
  (`:53-58,145-154`). `S1_SetLeaderLeoEagerlyRecomputes` / `S1_SetIsrEagerlyRecomputes`
  (`HighWatermarkTest.cpp:134-164`) assert no dummy follower update is needed — real. No
  stale-HW path remains. C4 selfId hack removed (`PartitionReplica.cpp:461-468`).

- **S3 — FIXED.** `PartitionRecorder::onCommitted` (`PartitionRecorder.cpp:73-126`) now
  uses a 64 MiB BYTE budget and an outer loop advancing `scanFrom` past each recorded
  batch until `endOffset` is covered, with a no-progress break guard. Multi-record/large
  batches are fully recorded. New large-payload test present.

### Deferrals

- **S2 — DEFERRAL-OK.** `becomeLeader` sets `replicas == isr` and, for a promoted
  follower (which has no pre-existing `isrManagerOwned_`, ctor builds it only for leaders,
  `PartitionReplica.cpp:64-77`), takes the `else` branch (`:93-96`) losing the true
  replica set. This is a durability-breadth/liveness reduction only: HW is computed over
  the ISR, and every ISR member holds the data, so HW is never inflated. `maybeExpandIsr`
  iterates the shrunken `replicas_` so out-of-ISR replicas won't auto-rejoin (repair
  leans on the coordinator). No committed-loss / HW-regression. Accept as documented.

- **S4 — DEFERRAL-OK.** The `looksLikeRecordBatch` magic-byte sniff
  (`PartitionReplica.cpp:335-370`) is a test-only artifact: in production a follower only
  ever receives verbatim leader `log_->read()` frames whose magic byte is always
  `kMagicValue`, so the verbatim path is always taken and no real record is misclassified.
  The raw-bytes fallback exists solely for unit-injected payloads. No production
  corruption path. Remains fragile — recommend a frame-type flag in FetchResponse before
  any external producer can inject arbitrary bytes.

- **S5 — DEFERRAL-OK.** `FetchResponseMsg::decode` (`ReplicationMessages.h:262-267`)
  still forms `{vp+VAR_HEADER_SIZE, len}` with only a `len<0→0` guard. All in-process
  callers decode a buffer they just encoded (`PartitionReplica.cpp:217,232,259`), and no
  real Aeron transport is wired yet, so the over-read is unreachable today. MUST be fixed
  (clamp `len` to remaining buffer) before wiring an untrusted network transport.

### NEW findings

- **NB1 (BLOCKER) — `becomeLeader` HW floor over-claims → uncommitted/divergent read on
  failover.** `PartitionReplica.cpp:108`:
  `leaderState_.setHighWatermarkFloor(log_->logEndOffset())`. LEO is an upper bound, not a
  lower bound, for committed data. A follower promoted with an uncommitted tail
  (`localLeo > followerHw_`) marks that tail as committed; consumers can then read
  records that were never majority-acked (and may be divergent from the true timeline).
  This is the same EOS violation B2 set out to prevent, re-introduced with the wrong
  offset. Fix direction: capture the node's committed offset BEFORE flipping role
  (`highWatermark()` still returns `followerHw_` while role==Follower — but role_ is set
  to Leader at `:87` first), i.e. read `followerHw_`/`highWatermark()` at entry and pass
  THAT to `setHighWatermarkFloor`, never `log_->logEndOffset()`. Add a failover test where
  the new leader has localLeo > committed (uncommitted tail) and assert
  `highWatermark() == committed` and `read(0,big)` excludes the tail.

- **NB2 (CORRECTNESS, latent) — `applyReplicated` epoch-truncation lacks the committedHw
  floor.** `PartitionReplica.cpp:316-324` + `truncateLog` (`:475-486`) can truncate below
  the committed prefix if the epoch cache returns a low boundary. Dead in the coordinated
  flow (becomeFollower runs first and bumps epoch_), so not currently reachable, but it
  should mirror `becomeFollower`'s `max(truncOff, followerHw_)` clamp for defense in depth.

- **NB3 (MINOR, latent) — read-clamp not multi-record-batch-exact.** `PartitionReplica.cpp:294`
  uses `batchBase >= hw`; a batch straddling HW (baseOffset<HW, lastOffset>=HW) would leak
  offsets ≥ HW. Unreachable while leader batches are single-record; fix when batching lands.

### Original findings addressed?

All 5 SILENT + 4 CORRECTNESS + 2 BLOCKER findings were touched. Genuinely resolved:
C1, S1, S3 (and C4). Correct-code-but-weak-test: B1. Partial: C2, C3. Regressed: B2→NB1.
Deferrals S2/S4/S5 accepted. Test findings T1/T4 strengthened with byte-equality but T4
(and the B1 unit tests) still do not construct a case that distinguishes the fixed logic
from the old `max()` (see B1 above).

---

## Re-audit 2 (commit 6cc793e)

Verdict: **CLEAN** (for the committed-loss / HW-regression / uncommitted-read / bad-
election surface audited). The NB1 blocker is genuinely fixed, and NB2/C3/B1-test are
resolved with non-vacuous tests. No new committed-loss, HW-regression, uncommitted-read,
or bad-election path was introduced by this fix. Remaining items are deferrals (S2/S4/S5)
and pre-existing latent non-reachable notes (NB3), plus two minor test-coverage gaps that
do not affect correctness. Build note: `cmake`/`ctest` unavailable in the audit env;
verification is by code + test reasoning (commit claims 1280/1280 pass).

### Per-finding status

- **NB1 — RESOLVED.** `becomeLeader` (`PartitionReplica.cpp:84-130`) captures
  `Offset committedHw = highWatermark();` at line 94 **before** the role flip
  (`role_ = Role::Leader` at :98) and before `setIsr`/`setLeaderLogEndOffset`. At capture
  time `highWatermark()` (`:465-470`) returns the OLD role's committed view — leaderState
  HW for was-leader, `followerHw_` for was-follower — both ≤ true committed, never the LEO.
  The floor is applied via `setHighWatermarkFloor(committedHw)` at :121, **before**
  `setIsr(:123)` and `setLeaderLogEndOffset(:124)`, each of which calls `recomputeHw()`
  eagerly. Ordering is correct: the floor is a lower bound; the subsequent recompute can
  only advance `prevHw_` above `committedHw` when `computeHw()` — a genuine ISR majority —
  supports it. For the NB1 case (2-node ISR {kF,kF2}, kF=LEO10, kF2=LEO0, committed=5):
  majority(2) of {10,0}=0 < floor 5 → HW stays 5; the tail [5,10) is NOT exposed. The
  1-node-ISR case where recompute yields leaderLeo is *correct* (leader-only quorum), and
  the test documents this explicitly. Test `NB1_BecomeLeaderUncommittedTail.
  HwFloorIsCommittedNotLeo` (`PartitionReplicaTest.cpp:1545-1663`) is genuinely
  distinguishing: it asserts HW==5 (not 10) and that `read(0,big)` returns exactly 5
  batches with no baseOffset≥5 — this FAILS under the old `log_->logEndOffset()` floor
  (which would give HW=10). Not vacuous. All three becomeLeader entry paths
  (was-follower / was-leader / fresh) capture a correct committed value.

- **B1 (distinguishing test) — RESOLVED.** `B1_Distinguishing_StaleMaxInflatesHwAboveFloor`
  (`HighWatermarkTest.cpp:747-793`) constructs the unique SET-vs-max() divergence: 3-node
  ISR, leaderLeo=8 so `prevHw_` floor only reaches 8; f1 reports LEO=10 then truncates and
  re-reports LEO=5; leader then advances to 12. SET → computeHw = median{12,5,0}=5 <8 →
  HW=8 (correct); old max() → f1 stuck at 10 → median{12,10,0}=10 >8 → HW=10 (WRONG, f1 no
  longer holds 10). Fails under max(). `updateFollowerFetchOffset` (`HighWatermark.h:118-125`)
  is per-follower SET, not max — correct. Normal monotonic advance is preserved:
  `B2_FloorPreservedAfterFollowerCatchUp` (`HighWatermarkTest.cpp:796-816`) shows floor +
  subsequent follower re-acks advance HW normally; `recomputeHw` (`:167-173`) only advances
  the global floor, never regresses it.

- **NB2 — RESOLVED.** `applyReplicated` epoch-truncation (`PartitionReplica.cpp:326-352`)
  now sets `Offset committedFloor = followerHw_` and clamps `truncOff = max(truncOff,
  committedFloor)` before `truncateLog(truncOff)`, mirroring `becomeFollower`'s C2 guard.
  When the epoch cache returns -1 no truncation fires at all (safe). Never truncates below
  committed. Test `NB2_ApplyReplicatedTruncation.NeverTruncatesBelowCommittedHw`
  (`PartitionReplicaTest.cpp:1669-1743`) exercises the guard via becomeFollower at epoch 2
  and asserts follower LEO stays ≥ committedHw=5. (Note: the direct-applyReplicated epoch
  path is still latent/unreachable in the coordinated flow, as before; the guard is
  correct defense-in-depth.)

- **C3 — RESOLVED.** `electNewLeader` (`PartitionFailoverCoordinator.h:264-378`) now, when
  the old leader is unregistered (crash-failover, `foundOldLeader==false`, :303-317),
  derives `committedHw = max(highWatermark())` over surviving registered ISR candidates
  instead of defaulting to 0. Derivation safety: each surviving ISR member's
  `highWatermark()` returns `followerHw_ = min(localLeo, lastKnownLeaderHw) ≤ true
  committed` (B1 fix guarantees no HW inflation), so the derived value can NEVER
  over-estimate → no false "no eligible leader" / liveness loss. It may slightly
  under-estimate, but election picks the highest-LEO eligible candidate, and the ISR
  invariant guarantees the highest-LEO survivor physically holds the true committed prefix
  — so no behind node is elected. The `assert(found)` on unregistered ISR members remains
  (hard-error, not silent leoOf=0). Tests: `ElectionC3Test.CandidateWithLEOBelowCommittedHW_
  NotElected` (`PartitionFailoverTest.cpp:1394+`, old-leader-registered path) and
  `C3_ElectionCommittedHw.CrashedLeaderUnregistered_CandidateWithLowLeoRejected`
  (`PartitionReplicaTest.cpp:1745-1850+`, exercises the new surviving-ISR derivation:
  f1 HW=10 → f2_lagging LEO=0 rejected, f1 elected; old default-0 code would allow
  f2_lagging). Non-vacuous.

### Still-holding from before (re-verified, unchanged by this commit)

- **B2 goal — HELD.** Committed prefix readable immediately post-failover: the floor is
  now committedHw (NB1), so a promoted leader reports HW ≥ committed at once (no HW=0
  window). `read()` returns the committed prefix.
- **C1 — HELD.** `read()` (`PartitionReplica.cpp:294-317`) still walks batches and stops at
  `baseOffset >= hw`; unchanged by this diff. NB3 (multi-record straddling-HW batch) remains
  latent/unreachable (leader appends 1 rec/batch).
- **C2 (becomeFollower floor) — HELD.** `becomeFollower` (`:132-176`) retains the
  `truncOff = max(truncOff, committedHw)` clamp; unchanged.
- **S1 — HELD.** Eager recompute on every mutator; unchanged.
- **S3 — HELD.** Recorder byte-budget; unchanged (not touched by this commit).

### Deferrals (unchanged, accepted)

- **S2 — DEFERRAL-OK.** `becomeLeader replicas == isr` (`:100`). Durability-breadth/liveness
  reduction only; HW computed over ISR, never inflated. No committed-loss.
- **S4 — DEFERRAL-OK.** `looksLikeRecordBatch` magic-byte sniff — test-only artifact;
  production always takes the verbatim path.
- **S5 — DEFERRAL-OK.** `FetchResponseMsg::decode` unbounded `len` — unreachable until an
  untrusted Aeron transport is wired. MUST fix before then.

### NEW findings from this fix

- None of BLOCKER/CORRECTNESS/SILENT severity. No new committed-loss, HW-regression,
  uncommitted-read, or bad-election path. The diff is surgical (4 regions) with no
  collateral change to the C1/C2-becomeFollower/S1/S3 paths.

- **NT1 (TEST-GAP, minor).** The `C3_ElectionCommittedHw.CrashedLeaderUnregistered` test
  covers the surviving-ISR derivation, but the earlier `ElectionC3Test` still registers the
  dead leader (foundOldLeader=true path). Coverage is adequate overall (NT1 is
  informational, not a correctness issue).

- **NB3 (MINOR, latent, carried) — UNCHANGED.** read-clamp uses `baseOffset >= hw`, not
  `baseOffset+lastOffsetDelta >= hw`. Unreachable while leaders append single-record
  batches; fix when batching lands. (`PartitionReplica.cpp:310`.)
