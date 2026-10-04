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
