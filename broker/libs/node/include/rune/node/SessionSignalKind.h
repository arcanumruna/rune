#pragma once
// SessionSignalKind.h — WorkerReturn::Type::SessionSignal kinds (carried in
// WorkerReturn::lastOffset).  Own header so SessionAckSink.h reports them
// without pulling in WorkerOp.h.  Append only — never renumber (the values ride
// the in-process return ring).
#include <cstdint>

namespace rune::node {

enum class SessionSignalKind : int64_t {
    Failed   = 0,   ///< its ack publication can never deliver → the runtime closes the session
    Held     = 1,   ///< the worker holds acks for it → the runtime pauses its gateway produce intake
    Released = 2,   ///< no longer held on this worker
    Lost     = 3,   ///< D8b: its ack publication reports NOT_CONNECTED (acks stay queued)
    Found    = 4,   ///< D8b: a later claim saw the publication connected again
    Progress = 5,   ///< D8b: a held session's acks are being claimed (rate-limited by passes)
    /// D8c: WorkerReturn::baseOffset gateway-routed produce acks of the session
    /// were CLAIMED on its ack publication since the last Credit report — the
    /// runtime returns that many produce credits (coalesced: <= one per session
    /// per worker pass).
    Credit   = 6,
    /// D8d: the worker's direct (worker-ingress) budget is full and this
    /// session — held here with the largest direct share — is to be EXPIRED: the
    /// runtime closes it everywhere and tells the client SessionExpired (it
    /// reconnects and resends; idempotent producers dedupe).  WorkerReturn::
    /// baseOffset = its direct share (diagnostic).
    Expire   = 7,
    /// 2026-10-05 (record-egress lifetime): the worker executed the release op
    /// (an Unsubscribe carrying egressPub != 0) — it dropped every subscription
    /// writing through that record-egress publication and will never touch it
    /// again.  WorkerReturn::baseOffset = the runtime's retire token; the
    /// runtime destroys the publication on this acknowledgement (retire-on-ack).
    EgressReleased = 8,
    /// Authentication Task 16: NOT a session signal - a worker reports the cumulative number of inter-broker frames
    /// its replication plane dropped (no established link / did not open): WorkerReturn::baseOffset = the count,
    /// ::partition = the worker, ::sessionId unused.  Paced (<= one per second, only when it changed).  The runtime
    /// folds the per-worker counts into counter 1'000'235 - the worker owns its crypto, nothing is shared.
    PeerFrameDrops = 9,
    /// Issues #109 / #196: NOT a session signal - the worker executed an enforcement fence: every op queued before
    /// it on that ring is done.  WorkerReturn::baseOffset = the fence sequence number, ::partition = the worker,
    /// ::sessionId = the ring (0 = op ring, 1 = session-lifecycle ring).  Never dropped (enqueueSignal_).
    FenceAck = 10,
};

} // namespace rune::node
