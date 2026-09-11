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
