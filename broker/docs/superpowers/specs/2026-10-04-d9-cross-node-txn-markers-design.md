# D9 — cross-node transaction markers (design)

Date: 2026-10-04.  Scope: Milestone D Phase 3 (transaction coordinator ↔ distributed partition leaders); pre-approved
for autonomous execution.  Owner repo: rune-broker (replication message schema lives where AlterIsrMsg lives).

## 1. Problem

On EndTxn the TransactionCoordinationService (TCS, runtime thread of the COORDINATOR node) writes one COMMIT/ABORT
control marker per partition of the transaction via `ClientGateway::appendControlRecord`, which queues it on the
OWNING worker's op ring.  With distributed partition leadership a partition can be led by ANOTHER node: today the
marker is refused (NotLeader) and retried with backoff until leadership happens to return — the transaction stays
Prepare* (EndTxn deferred, the producer's next transaction refused ConcurrentTransactions) for as long as the
partition is led elsewhere: forever in steady state.  `WriteTxnMarkersRequestMsg` exists in TransactionMessages.h
but is not wired.

Kafka: the coordinator sends WriteTxnMarkers to each partition's leader broker; the leader appends the marker and
answers per partition; the coordinator retries on NOT_LEADER (after a metadata refresh) until every marker landed.

## 2. Design

Forward in the GATEWAY (it already owns marker routing, the per-partition routing image and the marker completion
callbacks into the TCS) — the TCS is unchanged.

Coordinator node (gateway `appendControlRecord`):
- Resolve the partition's leader from the routing image.  Local leader → today's path (worker op ring).
- Remote leader → allocate a marker token (same `markersInFlight_` table, entry flagged `remoteNode = <id>`), send a
  `WriteTxnMarkerMsg` over the cluster-plane transport (`AeronPartitionTransport`, runtime thread, function-pointer
  handler like `alterIsrHandler_`; no virtual / std::function), return `kControlRecordAsync`.
- `WriteTxnMarkerResponseMsg` (token, errorCode, landed offset) → the SAME completion code as a local marker:
  landed → `txnService_->onControlMarkerLanded`, refused → `onControlMarkerRefused` (TCS keeps its existing
  retry/backoff and re-dispatch, which re-resolves the leader).
- Leadership change of a partition with remote markers in flight (routing-image update: leader no longer that node)
  → each such marker completes as refused `NotLeader` immediately → TCS retry → re-routed to the new leader.  This is
  event-driven convergence (no timeout constant decides correctness).  A marker whose request or answer is lost while
  leadership does NOT change is re-sent by the TCS retry (`kMarkerRetryMaxNs` backoff) — see §3 idempotence.

Leader node (cluster-plane handler):
- `WriteTxnMarkerMsg` → `ClientGateway::appendRemoteControlRecord(originNode, originToken, topic, partition, marker
  bytes, producerId, commit, coordinatorEpoch)`: same worker op-ring path as a local marker; the in-flight entry
  records the origin.  On `ControlRecordAppended` / refusal of a remote-origin entry the gateway sends
  `WriteTxnMarkerResponseMsg` to the origin node instead of calling its own TCS.
- Not the leader here (stale routing at the coordinator) → answer NotLeader at once.

Wire messages (replication schema, next free template ids; SBE-style like AlterIsrMsg, little-endian):
- `WriteTxnMarkerMsg { coordinatorNodeId:i32, coordinatorEpoch:i32, token:i64, topicId:i32, partition:i32,
  producerId:i64, commit:u8, markerLen:u32, marker bytes }` (marker = the encoded control batch, ≤ 128 B).
- `WriteTxnMarkerResponseMsg { token:i64, topicId:i32, partition:i32, producerId:i64, coordinatorEpoch:i32,
  errorCode:i16, offset:i64 }`.

### §2 addendum (fix rounds 1–2, 2026-10-04)

- **Target leader epoch.** `WriteTxnMarkerMsg` also carries `leaderEpoch:i32` at block offset +40: the leader
  epoch of the TARGET partition in the routing image the coordinator routed the marker under.  The marker bytes
  move to +44.  The leader's owning worker refuses the marker with NotLeader (retriable, so the TCS re-routes) when
  that epoch is older than the replica's current leader epoch.  This closes the case where a copy is held across
  two leadership changes and then delivered to a node that leads again, which the coordinator-epoch fence cannot
  catch because the copy carries the same coordinator epoch.  Local markers are dispatched as soon as they are
  routed and are not checked.
- **Leader-side idempotence.**
  - Per (topic, partition, producer), the leader remembers the last remote marker it took and its outcome:
    (origin node, coordinator epoch, token).
  - From the same coordinator load (same origin, epoch and token incarnation):
    - a copy of the same token is re-answered with the original outcome and is not appended again (or is
      skipped while its answer is still owed);
    - a copy of an older token is never appended.
  - This index is kept in memory only. After a leader change, a stale copy is refused by the target leader epoch
    above.
- **Completion is event-driven only.**
  - A remote marker completes on its answer or on a leadership change. No timer completes it.
  - A copy is re-sent with the same token when the plane reports that the link to its leader came back. The plane
    reports this in three ways:
    - the leader's publication goes not-connected → connected; this is edge-detected for every peer on every poll;
    - the hold router's connection generation is bumped;
    - a new inbound image appears from that peer's host.
  - Several peers on one host are all reported; an unknown host reports every peer.
- **Markers wait for the high watermark.** A marker counts as landed only once the partition HW passes it with the
  ISR ≥ minIsr (Kafka WriteTxnMarkers, acks=-1). The answer waits with no deadline; a role change gives NotLeader.
- **Rolling upgrade.** A node running a build without D9 silently drops template ids 11 and 12. A transaction that
  needs a marker on such a leader waits until that node is upgraded and restarted. Its new image fires the
  re-send.
- **Template ids 11 and 12** are reserved, as a comment, in rune-protocol `schema/rune-replication.xml`.

## 3. Invariants

- Idempotence: a duplicate marker is harmless — `RcState::onMarker` → `LsoTracker::onControlMarker` is a no-op when
  the producer has no open transaction, and on the ProducerStateManager a control batch is only an epoch raise
  (fence).  So re-sends (lost answer, leader change after the append) never corrupt state (Kafka parity).
- Epoch fencing: a response whose coordinatorEpoch is not the current load is ignored (TCS `currentEpoch_` already
  does this on both callbacks).
- Ordering: a remote marker lands after every produce the leader appended before it (same op-ring ordering as local).
- Never dropped: a full outbound ring keeps the marker in `markerSendQueue_` (existing FIFO retry), like a full op ring.
- Hot path: cold control traffic only; no allocation on the produce path; no atomics/mutex (runtime-thread owned).

## 4. Tests (RED-first, `*Regression*` / integration)

- Unit: message encode/decode round-trip incl. max marker length; a truncated frame is rejected.
- Gateway unit: remote-leader marker → a WriteTxnMarkerMsg is emitted (not the op ring); its response drives
  onControlMarkerLanded / Refused; routing-image leader change completes in-flight remote markers as NotLeader;
  remote-origin entry answers the origin instead of the local TCS.
- Integration (in-process 3-node, real cluster-plane transport): a transaction spanning partitions led by two
  different nodes, coordinator on a third → EndTxn(commit) completes; read_committed consumers on each leader see the
  data; abort variant hides it.  Leader kill/move mid-EndTxn → the marker lands on the new leader, transaction
  completes, no duplicate visible effect.
- E3 dockerized tx_* suite (tx_atomicity / tx_eos / tx_read_committed) green on k3s-node-01; D4 repro rides there.
- "Stuck markers" gauge: count of markers awaiting retry > N s exposed via the existing metrics/log path (warn once
  per partition at the max backoff — exists; add the gauge counter).
