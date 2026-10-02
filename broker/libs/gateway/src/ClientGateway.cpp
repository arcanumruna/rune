// ClientGateway.cpp — implementation of the client-facing session gateway (BR-4 / BR-5 / BR-7a / GR-4 / GR-5).

#include <rune/gateway/ClientGateway.h>
#include <cassert>
#include <rune_fr/FlightRecorder.h>
#include <rune/gateway/TopicRegistry.h>
#include <rune/gateway/GatewayReplicaView.h>
#include <rune/node/ReplicaRegistry.h>
#include <rune/transaction/ReadCommittedView.h>
#include <rune/metadata/RoutingImage.h>
#include <rune/metadata/Gpid.h>
// GroupCoordinationService.h is NOT included here (CG-4 fix: break rune_gateway→rune_node
// symbol cycle).  ClientGateway dispatches group messages through IGroupRequestHandler
// (defined in this lib); the concrete type lives in rune_node.
#include <rune/replication/PartitionReplica.h>
#include <rune/replication/IsrManager.h>

// E2 inc2a: SPSC worker routing (included via RUNE_NODE_INCLUDE raw path to
// avoid a circular CMake target dep; rune_gateway does NOT link rune_node).
#include <rune/node/WorkerPool.h>
#include <rune/node/WorkerOp.h>
#include <rune/node/SessionAckSink.h>
// E5.1.2d: shared produce-batch helper now in rune_storage (leaf lib, no circular dep).
#include <rune/storage/ProducePath.h>

#include <rune/protocol/client/Dispatch.h>
#include <rune/protocol/client/ControlMessages.h>
#include <rune/protocol/client/DataMessages.h>
#include <rune/protocol/client/MetadataMessages.h>
#include <rune/protocol/client/PingMessages.h>
#include <rune/protocol/client/GroupMessages.h>
#include <rune/protocol/client/OffsetMessages.h>
#include <rune/protocol/client/Protocol.h>
#include <rune/protocol/client/SchemaMessages.h>
#include <rune/protocol/RecordBatch.h>

#include <spdlog/spdlog.h>

static_assert(rune::gateway::ClientGateway::kShardMaxSessions ==
                  rune::node::SessionAckSink::kMaxSessions - 1,
              "Task 13: the runtime's shard session cap is a worker ack table's capacity");

#include <cstring>

#include <algorithm>
#include <deque>
#include <chrono>


namespace rune::gateway {

namespace pc = rune::protocol::client;

// ---------------------------------------------------------------------------
// Startup-only synchronous resolve budget: the ingress subscription in the
// constructor.  Every duty-cycle resolve (Connect / redirect egress) is async
// via pendingConnects_ — the blocking resolvePublication() was deleted (Task 11:
// a duty-cycle sleep-poll would stall worker 0's loop under fused placement).
// ---------------------------------------------------------------------------
static constexpr int kResolveMaxMs = 5000; // 5 s

// ---------------------------------------------------------------------------
// ClientGateway constructor
// ---------------------------------------------------------------------------
ClientGateway::ClientGateway(std::shared_ptr<aeron::Aeron>   aeron,
                             std::string                     clientChannel,
                             int32_t                         clientStreamId,
                             int32_t                         thisNodeId,
                             int64_t                         sessionTimeoutNs,
                             std::function<LeaderView()>     leaderView,
                             int64_t                         connectResolveTimeoutNs,
                             rune::aeronutil::AeronPump      pump)
    : aeron_(std::move(aeron))
    , clientChannel_(std::move(clientChannel))
    , clientStreamId_(clientStreamId)
    , thisNodeId_(thisNodeId)
    , sessionTimeoutNs_(sessionTimeoutNs)
    , connectResolveTimeoutNs_(connectResolveTimeoutNs)
    , leaderView_(std::move(leaderView))
{
    // Pre-allocate the draining-release vector (bounded by kMaxPendingConnects).
    // pendingConnects_ is a deque — no reserve() needed; O(1) front push/pop.
    drainingRegistrations_.reserve(kMaxPendingConnects);

    // GR-5: Pre-allocate the pending-ack queue to avoid heap allocs on the hot path.
    // The vector is used as a shrinking queue (erase-by-swap-and-pop); capacity
    // stays at kMaxPendingAcks after the initial reserve.
    pendingAcks_.reserve(kMaxPendingAcks);

    // Register the ingress subscription.
    int64_t regId = aeron_->addSubscription(clientChannel_, clientStreamId_);

    // Bounded resolve — same pattern as AeronIngress (spec §3.6: an invoker
    // driver + conductor is pumped by this thread between probes).
    sub_ = pump.resolve([&] { return aeron_->findSubscription(regId); },
                        std::chrono::milliseconds{kResolveMaxMs});
    if (!sub_) {
        throw std::runtime_error(
            "ClientGateway: ingress subscription not resolved within timeout");
    }
}

// ---------------------------------------------------------------------------
// G0.1: Test-only constructor — skips Aeron setup entirely.
// ---------------------------------------------------------------------------
ClientGateway::ClientGateway(TestConstructTag,
                             TestEgressFn  egressFn,
                             int64_t       currentSessionId)
    : aeron_(nullptr)
    , clientChannel_()
    , clientStreamId_(0)
    , thisNodeId_(0)
    , sessionTimeoutNs_(kDefaultSessionTimeoutNs)
    , connectResolveTimeoutNs_(kDefaultConnectResolveTimeoutNs)
    , leaderView_(nullptr)
    , testEgressFn_(std::move(egressFn))
    , currentProduceSessionId_(currentSessionId)
{
    pendingAcks_.reserve(kMaxPendingAcks);
    drainingRegistrations_.reserve(kMaxPendingConnects);
}

// G1.1: Test-only constructor — also wires leaderView_.
ClientGateway::ClientGateway(TestConstructTag,
                             TestEgressFn                egressFn,
                             std::function<LeaderView()> leaderView,
                             int64_t                     currentSessionId)
    : aeron_(nullptr)
    , clientChannel_()
    , clientStreamId_(0)
    , thisNodeId_(0)
    , sessionTimeoutNs_(kDefaultSessionTimeoutNs)
    , connectResolveTimeoutNs_(kDefaultConnectResolveTimeoutNs)
    , leaderView_(std::move(leaderView))
    , testEgressFn_(std::move(egressFn))
    , currentProduceSessionId_(currentSessionId)
{
    pendingAcks_.reserve(kMaxPendingAcks);
    drainingRegistrations_.reserve(kMaxPendingConnects);
}

// ---------------------------------------------------------------------------
// G0.1: makeForTest — static factory for test-only (no Aeron) construction.
// ---------------------------------------------------------------------------
ClientGateway ClientGateway::makeForTest(TestEgressFn egressFn,
                                         int64_t      currentProduceSessionId)
{
    return ClientGateway(TestConstructTag{},
                         std::move(egressFn),
                         currentProduceSessionId);
}

// G1.1: makeForTestWithLeaderView — like makeForTest but also wires leaderView_.
// ---------------------------------------------------------------------------
ClientGateway ClientGateway::makeForTestWithLeaderView(TestEgressFn             egressFn,
                                                       std::function<LeaderView()> leaderView,
                                                       int64_t currentProduceSessionId)
{
    return ClientGateway(TestConstructTag{},
                         std::move(egressFn),
                         std::move(leaderView),
                         currentProduceSessionId);
}

// ---------------------------------------------------------------------------
// G0.1: offerBytes_ — try to deliver encoded bytes via the egress.
//
// Uses testEgressFn_ when set (non-null only in tests), otherwise calls
// sendTo on the provided session pointer.
// Returns > 0 on success, <= 0 on back-pressure / failure.
// ---------------------------------------------------------------------------
int64_t ClientGateway::offerBytes_(Session* session,
                                   const uint8_t* buf,
                                   std::size_t    len)
{
    if (testEgressFn_) {
        return testEgressFn_(buf, len);
    }
    if (!session) return -1;
    // sendTo returns bool: true = delivered (may have used tryClaim or offer),
    // false = back-pressure / not-connected.  Map to int64_t for a uniform API.
    return sendTo(*session, buf, len) ? 1 : -1;
}

// ---------------------------------------------------------------------------
// G0.1: enqueueOrBestEffort_ — THE single reliable-egress primitive.
//
// EVERY produce-path ack / redirect / error response routes through this
// function (directly or via sendRespReliable_).  It:
//   1. Tries immediate delivery via offerBytes_(&session).
//   2. On back-pressure (result <= 0): copies the bytes onto the ring.
//   3. On ring-full or oversized frame: best-effort sendTo + warn (bounded,
//      logged last resort — never a silent drop).
//
// The bytes must already be encoded (caller owns encoding into any buffer).
// len > PendingResp::kMaxBytes → cannot fit a ring slot → best-effort.
//
// Single-writer: duty-cycle thread only.
// ---------------------------------------------------------------------------
void ClientGateway::enqueueOrBestEffort_(Session&       session,
                                         const uint8_t* buf,
                                         std::size_t    len)
{
    // Try immediate delivery.
    const int64_t result = offerBytes_(&session, buf, len);
    if (result > 0) return;   // delivered

    // Oversized for the fixed ring slot — best-effort (rare long redirect URLs).
    if (len > PendingResp::kMaxBytes) {
        spdlog::warn("ClientGateway: egress frame {} bytes > ring slot {}; "
                     "best-effort send (session={})",
                     len, PendingResp::kMaxBytes, session.sessionId);
        sendTo(session, buf, len);
        return;
    }

    // Ring full — best-effort (should never fire; back-pressure clears in cycles).
    if (pendingRespCount_ >= kMaxPendingResponses) {
        spdlog::warn("ClientGateway: pendingResponses_ ring full ({} entries); "
                     "best-effort send (session={})",
                     kMaxPendingResponses, session.sessionId);
        sendTo(session, buf, len);
        return;
    }

    // Enqueue for drainPendingResponses().
    const std::size_t slot = (pendingRespHead_ + pendingRespCount_) % kMaxPendingResponses;
    PendingResp& e = pendingResponses_[slot];
    std::memcpy(e.bytes.data(), buf, len);
    e.len       = len;
    e.sessionId = session.sessionId;
    ++pendingRespCount_;
    spdlog::debug("ClientGateway: egress back-pressured; enqueued session={} "
                  "len={} (depth={})", session.sessionId, len, pendingRespCount_);
}

// ---------------------------------------------------------------------------
// F19: pushRecords_ — send one Records push frame on the session's egress.
// Real egress → rune::node::sendRecordsFrame (tryClaim fast path for one-MTU
// frames, unchanged; vectored [fixed | batch] offer above it; TooLarge above
// maxMessageLength).  Null egress (injectTestSession) → the test capture hook
// (treated as sent); no hook → Retry.
// ---------------------------------------------------------------------------
rune::node::RecordsSendResult ClientGateway::pushRecords_(Session& session, const pc::Records& rec)
{
    if (session.egress) [[likely]]
        return rune::node::sendRecordsFrame(*session.egress, rec, scratch_.data(), scratch_.size());
    if (!onEgressFrame_) return {rune::node::RecordsSendKind::Retry, -1};
    captureTestRecordsFrame_(session, rec);
    return {rune::node::RecordsSendKind::Sent, 1};
}

void ClientGateway::captureTestRecordsFrame_(Session& session, const pc::Records& rec)
{
    const std::size_t frameLen = rec.encodedSize();
    if (frameLen <= scratch_.size()) {
        onEgressFrame_(session.sessionId, scratch_.data(), rec.encode(scratch_.data()));
        return;
    }
    std::vector<uint8_t> frame(frameLen);   // test-only: frame larger than scratch_
    onEgressFrame_(session.sessionId, frame.data(), rec.encode(frame.data()));
}

// ---------------------------------------------------------------------------
// F19: failPushSubscriptionTooLarge_ — the next stored batch of `sub` can never
// be delivered on this session's egress publication (frame > maxMessageLength).
// Fail the subscription ONCE, explicitly, and park it AT the batch — nextOffset
// is NOT advanced, so nothing is lost silently; pushSubscriptions() skips it
// (no spin) until the client unsubscribes.  The error (F15 S3 m-f convention:
// correlationId 0, subscription named in the message) is owed until sent.
// ---------------------------------------------------------------------------
void ClientGateway::failPushSubscriptionTooLarge_(Subscription& sub, Session& session,
                                                  std::size_t frameLen)
{
    if (sub.failed) return;
    sub.failed         = true;
    sub.errorOwed      = true;
    sub.failedCause    = rune::node::PushTooLargeCause::EgressFrame;
    sub.failedFrameLen = frameLen;
    spdlog::error("ClientGateway: push sub={} (topicId={} partition={}) failed at offset={}: "
                  "Records frame {} B exceeds the egress maxMessageLength {} B — "
                  "MessageTooLarge, subscription parked (cursor not advanced)",
                  sub.subscriptionId, sub.topicId, sub.partition, sub.nextOffset, frameLen,
                  session.egress ? session.egress->maxMessageLength() : 0);
    trySendOwedPushError_(sub, session);
}

void ClientGateway::trySendOwedPushError_(Subscription& sub, Session& session)
{
    static_assert(rune::node::kPushTooLargeErrorMaxBytes <= kScratchSize);
    const std::size_t n = rune::node::encodePushTooLargeError(
        scratch_.data(), sub.sessionId, sub.subscriptionId, sub.topicId, sub.partition,
        sub.nextOffset, sub.failedFrameLen,
        session.egress ? session.egress->maxMessageLength() : 0, sub.failedCause);
    if (offerBytes_(&session, scratch_.data(), n) > 0) sub.errorOwed = false;
}

// ---------------------------------------------------------------------------
// G0.1: sendRespReliable_ — encode a ProduceResponse into scratch_ and route it
// through enqueueOrBestEffort_.  Returns false only on an encode error.
// ---------------------------------------------------------------------------
bool ClientGateway::sendRespReliable_(Session&          session,
                                      pc::ErrorCode     ec,
                                      int32_t           partition,
                                      int64_t           baseOffset,
                                      int64_t           correlationId)
{
    static_assert(pc::ProduceResponse::ENCODED_SIZE <= PendingResp::kMaxBytes,
                  "ProduceResponse::ENCODED_SIZE exceeds PendingResp::kMaxBytes");
    static_assert(pc::ProduceResponse::ENCODED_SIZE <= kScratchSize,
                  "ProduceResponse::ENCODED_SIZE exceeds kScratchSize");

    pc::ProduceResponse resp;
    resp.sessionId     = session.sessionId;
    resp.correlationId = correlationId;
    resp.errorCode     = static_cast<int16_t>(ec);
    resp.partition     = partition;
    resp.baseOffset    = baseOffset;
    const std::size_t encoded = resp.encode(scratch_.data());
    if (encoded == 0 || encoded > PendingResp::kMaxBytes) {
        spdlog::error("ClientGateway::sendRespReliable_: encode error ({} bytes)", encoded);
        return false;
    }

    enqueueOrBestEffort_(session, scratch_.data(), encoded);
    return true;
}

// ---------------------------------------------------------------------------
// G0.1: sendRespReliable — public 4-param test-seam (targets
// currentProduceSessionId_; set by handleProduce or makeForTest).
//
// Production path: look up the session and delegate to sendRespReliable_().
// Test path (testEgressFn_ set): the ring/offer logic is exercised by wrapping
// a synthetic Session whose id is currentProduceSessionId_ — offerBytes_ ignores
// the session pointer entirely when testEgressFn_ is set, so this is safe.
// ---------------------------------------------------------------------------
bool ClientGateway::sendRespReliable(rune::protocol::client::ErrorCode ec,
                                     int32_t    partition,
                                     int64_t    baseOffset,
                                     int64_t    correlationId)
{
    if (testEgressFn_) {
        // No real Session in the test harness — build a synthetic one whose id
        // matches currentProduceSessionId_.  offerBytes_ routes through
        // testEgressFn_ and never dereferences the session pointer in test mode.
        Session synthetic;
        synthetic.sessionId = currentProduceSessionId_;
        return sendRespReliable_(synthetic, ec, partition, baseOffset, correlationId);
    }

    Session* session = sessions_.find(currentProduceSessionId_);
    if (!session) {
        spdlog::debug("ClientGateway::sendRespReliable: session {} not found",
                      currentProduceSessionId_);
        return false;
    }
    return sendRespReliable_(*session, ec, partition, baseOffset, correlationId);
}

// ---------------------------------------------------------------------------
// G0.1: drainPendingResponses — re-offer queued egress frames from the ring.
// Stops at the first back-pressure (preserves FIFO).
// Returns number of frames delivered.
//
// nowNs is currently unused (no per-entry aging) — kept in the signature for
// the doWork() call convention and future deadline/timeout support.
// ---------------------------------------------------------------------------
int ClientGateway::drainPendingResponses(int64_t /*nowNs*/)
{
    int delivered = 0;
    while (pendingRespCount_ > 0) {
        PendingResp& e  = pendingResponses_[pendingRespHead_];
        Session* session = testEgressFn_ ? nullptr : sessions_.find(e.sessionId);
        // Session reaped (production, non-test) → cannot deliver → discard the
        // entry (no session to ack) rather than blocking the queue forever.
        if (!testEgressFn_ && !session) {
            pendingRespHead_ = (pendingRespHead_ + 1) % kMaxPendingResponses;
            --pendingRespCount_;
            continue;
        }
        const int64_t result = offerBytes_(session, e.bytes.data(), e.len);
        if (result <= 0) break;   // still back-pressured — stop (preserve FIFO)
        pendingRespHead_ = (pendingRespHead_ + 1) % kMaxPendingResponses;
        --pendingRespCount_;
        ++delivered;
    }
    return delivered;
}

// ---------------------------------------------------------------------------
// doWork — poll ingress; poll pending-connects; reap expired sessions.
// Returns fragment count + pending-connect resolves.
// ---------------------------------------------------------------------------
int ClientGateway::doWork(int64_t nowNs) {
    nowNs_ = nowNs;

    // G1.1: Snapshot minIsr_ once per duty-cycle cycle so handleProduce() and
    // drainPendingAcks() use a plain int load instead of a per-record atomic load.
    // Safe: cachedMinIsr_ is only accessed on this single-writer thread.
    cachedMinIsr_ = minIsr_.load(std::memory_order_relaxed);

    // G0.1: drain any back-pressured egress frames FIRST — before ingress poll,
    // before worker returns, before everything else.  This ensures a buffered
    // produce ack is sent as soon as the publication clears back-pressure,
    // preserving the "no silent drop on back-pressure" invariant.
    drainPendingResponses(nowNs);

    // E2 inc2a: drain the workers' return rings FIRST — send responses from the
    // previous cycle's produce-appends and push-reads before processing any new
    // ingress.  This keeps produce responses strictly ordered before push-Records
    // (same session, different op types) and ensures deferred acks=All are
    // checked with the latest HW from the previous cycle.
    //
    // Guard: only active when a WorkerPool is wired in.  Falls back gracefully
    // to the pre-E2 inline-execution path when workerPool_ is null.
    if (workerPool_) {
        drainWorkerReturns_();
    }

    // TX-2b: Detect Follower→Leader transition and rebuild the PSM from the
    // partition Logs.  Mirrors TransactionCoordinationService::doWork().
    //
    // leaderView_ is set only when replication is configured; when null (single-
    // node passthrough) we are always "leader" — the gateway was never a follower,
    // so no rebuild is needed on the very first doWork() call (wasLeader_ starts
    // true in the no-leaderView path to suppress the unnecessary cold rebuild).
    //
    // With leaderView_: rebuild fires exactly once on the first duty cycle after
    // the node becomes leader (wasLeader_ flips false→true). Losing leadership
    // resets wasLeader_ so the next election fires another rebuild.
    if (localLeader_ >= 0 || leaderView_) {
        // Task 11 fix 1: the runtime pushes this cycle's leadership as a value
        // (no per-cycle std::function call); gateway-only tests use leaderView_.
        const bool leader = localLeader_ >= 0 ? localLeader_ == 1 : leaderView_().isLeader;
        if (leader && !wasLeader_) {
            // Follower→Leader: rebuild PSM from all partition Logs.
            rebuildProducerStateFromLogs();
            wasLeader_ = true;
        } else if (!leader && wasLeader_) {
            // Leader→Follower: reset so the next election triggers another rebuild.
            wasLeader_ = false;
        }
    }

    // G0.3: fragment limit is configurable (default 256, was hard-coded 10).
    // Sourced from NodeConfig::gatewayFragmentLimit via setFragmentLimit().
    // Task 10 round 4: retry worker-bound ops a full ring refused earlier
    // (control markers, subscription lifecycle) BEFORE new ingress, in order.
    int work = 0;
    if (workerPool_) work += pumpMarkerQueue_() + pumpSubOps_();
    if (shardMode_) {
        // Task 13 fix round 2 (finding 1, G-A6 on the gateway-routed path): a
        // Produce frame of a session some worker holds acks for is ABORTed — it
        // stays on ITS image (one client publication) and the poll moves on to
        // the other images; the client's offers back-pressure until it reads
        // its acks (Released).  One branch when nothing is held.
        // ABORT stops the WHOLE image: every later frame of that client —
        // Keepalive, Metadata, Disconnect, … — waits behind the aborted Produce
        // until Released, a close (SessionFailed), or the lease reap.  No
        // deadlock (releasing a held ack never needs a gateway frame); while
        // paused, the client's lease is refreshed only by the post-ack
        // notifications of the acks it does read, and a client that drops its
        // egress before Disconnect leaves through the lease reap (fix round 3).
        work += sub_->controlledPoll(
            [this](aeron::concurrent::AtomicBuffer& b, aeron::util::index_t o, aeron::util::index_t l,
                   aeron::Header& h) {
                return controlledAssembler_.onFragment(
                    [this](aeron::concurrent::AtomicBuffer& buffer, aeron::util::index_t offset,
                           aeron::util::index_t length, aeron::Header& header) {
                        if (heldSessions_ != 0 &&
                            produceFrameHeld_(reinterpret_cast<const uint8_t*>(buffer.buffer() + offset),
                                              static_cast<std::size_t>(length))) [[unlikely]] {
                            ++heldProduceAborts_;
                            return aeron::ControlledPollAction::ABORT;
                        }
                        onFragment(buffer, offset, length, header);
                        return aeron::ControlledPollAction::CONTINUE;
                    },
                    b, o, l, h);
            },
            fragmentLimit_);
        controlledImageWatch_.afterPoll(*sub_, controlledAssembler_);
    } else {
        // Task 11 fix 2 (B3): templated assembler + handler — no std::function.
        work += sub_->poll(
            [this](aeron::concurrent::AtomicBuffer& b, aeron::util::index_t o, aeron::util::index_t l,
                   aeron::Header& h) {
                assembler_.onFragment(
                    [this](aeron::concurrent::AtomicBuffer& buffer, aeron::util::index_t offset,
                           aeron::util::index_t length, aeron::Header& header) {
                        onFragment(buffer, offset, length, header);
                    },
                    b, o, l, h);
            },
            fragmentLimit_);
        imageWatch_.afterPoll(*sub_, assembler_);   // free buffers of lost client images (m-c)
    }
    pollPendingConnects(nowNs);
    // Task 13: SessionClose ops a full worker ring refused earlier — never dropped.
    if (shardMode_ && workerPool_) work += sessionAnnouncer_.retryCloses(*workerPool_);
    pollPendingRecordEgress_(nowNs);  // hybrid egress: resolve unicast pubs + drain deferred subscribes
    reapExpired(nowNs);
    // GB-5p2: group-commit flush — time-based cadence (kGroupCommitIntervalNs).
    // Fsyncs all dirty partition Logs (appendNoSync'd since last flush) at most
    // once per interval.  Produces between flushes land in page cache at ~1-10µs.
    // Must run AFTER the ingress poll; placed before pushSubscriptions so push
    // reads fsync-durable data.  No busy-spin; no-op when interval not elapsed.
    flushDirtyLogs();
    // GR-5: drain deferred acks=All pending-ack queue.
    // Called AFTER ingress poll (appends already done for this cycle) and AFTER
    // the replica's HW may have advanced (replication doWork is driven by the
    // BrokerNode agent loop BEFORE gateway.doWork in the same duty-cycle thread,
    // so any follower FetchResponse processed this cycle already updated HW).
    // Must run BEFORE pushSubscriptions so the client gets the ProduceResponse
    // before any Records push (strictly ordered on the same egress publication).
    drainPendingAcks();
    // GB-3: push new Records to all active subscribers (egress-paced, single-writer).
    pushSubscriptions();
    return work;
}

// ---------------------------------------------------------------------------
// rebuildProducerStateFromLogs — node-level Follower→Leader edge.
//
// Called once on the first doWork() cycle after the node becomes leader.
//
// Producer state (inline path): NOT rebuilt here any more.  The former rebuild
// scanned only to the committed HW (TX-2b, to keep a never-committed tail out
// of the dedup window) — but a new leader KEEPS its tail between HW and LEO,
// so a resend of the first tail batch (seq == lastSequence + 1) was accepted
// and appended twice (a silent duplicate), and later tail batches answered OOS
// ("NOT appended" — the client then re-sends them).  It also ran only on this
// node-level edge, missing every per-partition promotion / truncation.  Now the
// partition's proof is invalidated here and proveInlinePsm_ rebuilds it from
// logStart to LEO on the next idempotent batch — and on every change of the
// replica's log / leadership generation, which is what makes the LEO bound
// safe: a later KIP-101 truncation of the tail bumps the log generation and
// forces another rebuild before any answer.
//
// read_committed state (TX-7, no worker pool only): rebuilt from [0, HW) as
// before.
//
// No-op when topicRegistry_ is null (gateway not yet wired).
// Cold path: allocations acceptable.  Single-writer: duty-cycle thread only.
// ---------------------------------------------------------------------------
void ClientGateway::rebuildProducerStateFromLogs()
{
    if (!topicRegistry_) return;

    const int32_t topicCount = topicRegistry_->topicCount();
    for (int32_t topicId = 0; topicId < topicCount; ++topicId) {
        const rune::node::TopicSpec* spec = topicRegistry_->topic(topicId);
        if (!spec) continue;

        for (int32_t partition = 0; partition < spec->partitionCount; ++partition) {
            rune::storage::Log* log = topicRegistry_->log(topicId, partition);
            if (!log) continue;

            // The read_committed rebuild below is bounded by the committed HW
            // (LEO when no replication is wired: passthrough, HW == LEO).
            int64_t scanTo = log->logEndOffset(); // fallback: passthrough/test mode
            if (replicaRegistry_) {
                rune::replication::PartitionReplica* replica =
                    replicaRegistry_->lookup(topicId, partition);
                if (replica) {
                    scanTo = replica->gatewayHighWatermark();
                }
            }

            // F15 S5 (§2.5, Q3): the owning worker rebuilds the producer state of
            // every partition it owns (its RC build) — the gateway keeps state
            // only for partitions it appends to itself (no worker pool, or an
            // internal partition in shard mode), and never reads a worker-owned
            // log from this thread.
            if (routesToWorker_(topicId)) continue;
            // PSM <-> log consistency (review of b4594f8, finding 1): the
            // inline producer state is NOT rebuilt here to HW — a new leader
            // keeps its tail between HW and LEO, and a resend of the first tail
            // batch (seq == lastSequence + 1) was then appended twice.  The
            // state is invalidated; the next idempotent batch of the partition
            // rebuilds it from logStart to LEO (proveInlinePsm_), which also
            // covers every per-partition promotion / truncation this node-level
            // edge never sees.
            inlinePsm_.erase(inlinePsmKey_(topicId, partition));

            // TX-7: Rebuild per-partition read_committed state (LsoTracker +
            // AbortedTxnIndex) from the same committed log range [0, scanTo).
            //
            // On a Follower→Leader transition the partitionTxnStates_ map is
            // EMPTY: it is only populated incrementally via onProduceBatch /
            // onControlMarker during normal produce handling.  An empty tracker
            // returns lso(hw) == hw, which would allow a read_committed consumer
            // to see records belonging to OPEN transactions (violating KIP-98 §8:
            // LSO must pin at the first open-txn offset).  Similarly, an empty
            // abortedIndex would deliver ABORTED data batches that are already
            // below HW in the log (spec §8: aborted batches must be filtered).
            //
            // ReadCommittedView::rebuild scans [0, hw) from the log and
            // populates both LsoTracker and AbortedTxnIndex — exactly the same
            // structures that pushSubscriptions() reads via getTxnStateRO().
            //
            // We must CLEAR any stale state first (a partition that briefly had
            // incremental state before the node lost leadership could carry
            // stale entries; a clean rebuild from the committed log is the
            // canonical source of truth).
            //
            // scanTo == hw — bounded to committed records, never seeding from
            // the uncommitted ahead-tail.
            //
            // F15 S4 (Option B): with a worker pool (every production mode) the
            // OWNING worker builds and serves the read_committed state (§2.3) —
            // the gateway holds none for those partitions.  Only the no-pool
            // inline path (unit rigs; the control thread is the single reader
            // and writer there) keeps this gateway-side state.
            if (!workerPool_) {
                PartitionTxnState& ts = getTxnState(topicId, partition);
                ts.lsoTracker.clear();
                ts.abortedIndex.clear();
                rune::transaction::ReadCommittedView view(
                    &ts.lsoTracker, &ts.abortedIndex);
                view.rebuild(log, scanTo);
            }
        }
    }

    spdlog::info("ClientGateway: inline producer state invalidated + read_committed state rebuilt "
                 "from {} topic(s) on leader transition", topicCount);
}

// ---------------------------------------------------------------------------
// proveInlinePsm_ — PSM <-> log consistency gate of the inline path (see the
// InlinePsmState comment in ClientGateway.h).  Hot path (Proven, unchanged
// facts): one hash lookup + a compare.  Cold: the rebuild.
// ---------------------------------------------------------------------------
ClientGateway::InlinePsmGate
ClientGateway::proveInlinePsm_(int32_t topicId, int32_t partition, rune::storage::Log& log,
                               const rune::replication::PartitionReplica* replica, uint32_t incarnation)
{
    const uint64_t key = inlinePsmKey_(topicId, partition);
    InlinePsmProven facts;
    facts.log         = &log;
    facts.incarnation = incarnation;
    if (replica != nullptr) {
        // Only a COMPLETE leader's log is stable below its LEO: a follower
        // takes replicated appends, a deficient leader may still reconcile-
        // append old-term batches (the worker path's RC gate, I2).
        if (replica->role() != rune::replication::Role::Leader || !replica->isLeaderComplete()) [[unlikely]] {
            if (auto it = inlinePsm_.find(key); it != inlinePsm_.end()) it->second = InlinePsmUnproven{};
            return InlinePsmGate::NotLeader;
        }
        facts.logGen  = replica->logGeneration();
        facts.leadGen = replica->leadershipGeneration();
    }
    auto it = inlinePsm_.find(key);
    if (it != inlinePsm_.end()) {
        const auto* proven = std::get_if<InlinePsmProven>(&it->second);
        if (proven != nullptr && *proven == facts) [[likely]] return InlinePsmGate::Proven;
    }
    // Cold: rebuild from logStart to LEO — every stored batch (the kept tail
    // above HW included; Kafka loads producer state to the log end).  A later
    // truncation of that tail bumps logGeneration and is caught above.
    bool complete = false;
    const char* why = "it stopped short of the log end (an unreadable batch)";
    try {
        complete = producerStateManager_.rebuildFromLog(topicId, log, log.logEndOffset(), partition);
    } catch (const std::exception& ex) {
        why = ex.what();
    }
    if (!complete) [[unlikely]] {
        producerStateManager_.clearPartition(topicId, partition);
        if (it != inlinePsm_.end()) it->second = InlinePsmUnproven{};
        spdlog::error("ClientGateway: producer-state rebuild failed for topicId={} partition={}: {} — "
                      "idempotent produces answered Internal (outcome unknown) until it succeeds",
                      topicId, partition, why);
        return InlinePsmGate::Unprovable;
    }
    if (it != inlinePsm_.end()) it->second = facts;
    else inlinePsm_.emplace(key, facts);
    return InlinePsmGate::Proven;
}

bool ClientGateway::inlineWaiterStillValid_(const rune::replication::PartitionReplica& rep,
                                            PendingAck& entry) noexcept
{
    if (rep.role() != rune::replication::Role::Leader) return false;
    const uint64_t g = rep.logGeneration();
    if (g == entry.logGen) [[likely]] return true;
    if (g != entry.logGen + 1 || entry.lastOffset >= rep.lastTruncationOffset()) return false;
    entry.logGen = g;   // exactly one cut, strictly above the waiter: its data is untouched
    return true;
}


// ---------------------------------------------------------------------------
// reapExpired — remove sessions idle beyond sessionTimeoutNs_.
//
// Uses injected nowNs ONLY (no wall-clock).  Bounded O(n) scan.
// Two-phase: collect expired IDs into reapScratch_ (pre-allocated vector),
// then erase them — avoids iterator invalidation.
// ---------------------------------------------------------------------------
void ClientGateway::reapExpired(int64_t nowNs) {
    reapScratch_.clear();

    sessions_.forEach([&](Session& s) {
        if (nowNs - s.lastSeenNs > sessionTimeoutNs_) {
            reapScratch_.push_back(s.sessionId);
        }
    });

    // Fix round 2 (finding 4): ONE close path (closeSession_) for reap,
    // Disconnect and SessionFailed — table, workers, record-egress resolves,
    // subscriptions, group-member tracking, held state.
    for (int64_t sid : reapScratch_) (void)closeSession_(sid);
}

// ---------------------------------------------------------------------------
// onFragment — called by FragmentAssembler for each reassembled frame.
// Drop-safe: any exception or decode failure is caught and silently dropped.
// ---------------------------------------------------------------------------
void ClientGateway::onFragment(aeron::concurrent::AtomicBuffer& buffer,
                               aeron::util::index_t             offset,
                               aeron::util::index_t             length,
                               aeron::Header&                   /*header*/)
{
    const auto* buf = reinterpret_cast<const uint8_t*>(buffer.buffer() + offset);
    const auto  len = static_cast<std::size_t>(length);

    // Step 1: validate frame with checkFrame — reject anything non-conformant.
    const auto fc = pc::checkFrame(buf, len);
    if (fc.status != pc::FrameStatus::Ok) {
        // Drop silently — bad/unknown frame never crashes the gateway.
        return;
    }

    // Step 2: dispatch on templateId.
    const auto tid = static_cast<pc::TemplateId>(fc.templateId);

    // F23: every client->broker request refreshes its session's liveness, not
    // only Keepalive/Ping (Kafka: a connection is idle only when it sends no
    // requests).  Produce refreshes inside handleProduce (it already looks the
    // session up); Connect has no session yet; Keepalive/Ping touch in their
    // handlers.  Cold request types pay one session lookup here.
    switch (tid) {
        case pc::TemplateId::MetadataRequest:
        case pc::TemplateId::Subscribe:
        case pc::TemplateId::Unsubscribe:
        case pc::TemplateId::JoinGroup:
        case pc::TemplateId::SyncGroup:
        case pc::TemplateId::Heartbeat:
        case pc::TemplateId::LeaveGroup:
        case pc::TemplateId::OffsetCommit:
        case pc::TemplateId::OffsetFetch:
        case pc::TemplateId::InitProducerId:
        case pc::TemplateId::AddPartitionsToTxn:
        case pc::TemplateId::AddOffsetsToTxn:
        case pc::TemplateId::TxnOffsetCommit:
        case pc::TemplateId::EndTxn:
        case pc::TemplateId::DefineTopic:
        case pc::TemplateId::GetTopicSchema:
            if (const auto hdr = pc::MessageHeader::decode(buf, len)) {
                sessions_.touch(hdr->sessionId, nowNs_);
            }
            break;
        default:
            break;
    }

    switch (tid) {
        case pc::TemplateId::Connect:
            handleConnect(buf, len, nowNs_);
            break;
        case pc::TemplateId::Keepalive:
            handleKeepalive(buf, len, nowNs_);
            break;
        case pc::TemplateId::Ping:
            handlePing(buf, len, nowNs_);
            break;
        case pc::TemplateId::Disconnect:
            handleDisconnect(buf, len);
            break;
        // MD1 Task 10: Metadata request — answerable by any node.
        case pc::TemplateId::MetadataRequest:
            handleMetadata(buf, len);
            break;
        case pc::TemplateId::Produce:
            handleProduce(buf, len);
            break;
        case pc::TemplateId::Subscribe:
            handleSubscribe(buf, len);
            break;
        case pc::TemplateId::Unsubscribe:
            handleUnsubscribe(buf, len);
            break;
        // CG-3: consumer-group range (32–43); Assign (44) is broker→client, drop.
        case pc::TemplateId::JoinGroup:
        case pc::TemplateId::SyncGroup:
        case pc::TemplateId::Heartbeat:
        case pc::TemplateId::LeaveGroup:
        case pc::TemplateId::OffsetCommit:
        case pc::TemplateId::OffsetFetch:
            handleGroupMessage(buf, len, nowNs_, fc.templateId);
            break;
        // TX-3: txn request range (45,47,49,51,53); responses (46,48,50,52,54) are broker→client.
        case pc::TemplateId::InitProducerId:
        case pc::TemplateId::AddPartitionsToTxn:
        case pc::TemplateId::AddOffsetsToTxn:
        case pc::TemplateId::TxnOffsetCommit:
        case pc::TemplateId::EndTxn:
            handleTxnMessage(buf, len, nowNs_, fc.templateId);
            break;
        // Task 8a: schema DDL — route to the admin sink (ring hand-off).
        case pc::TemplateId::DefineTopic:
            handleDefineTopic(buf, len);
            break;
        case pc::TemplateId::GetTopicSchema:
            handleGetTopicSchema(buf, len);
            break;
        default:
            // ConnectResponse, ErrorResponse, Pong, MetadataResponse (broker→client),
            // ProduceResponse, SubscribeResponse, Records, Assign,
            // InitProducerIdResponse, AddPartitionsToTxnResponse,
            // AddOffsetsToTxnResponse, TxnOffsetCommitResponse, EndTxnResponse
            // — server→client or other direction; drop.
            break;
    }
}

// ---------------------------------------------------------------------------
// handleConnect (BR-7a async path)
//
// BR-5 leader-awareness:
//   - Query leaderView_ (if set).
//   - If NOT leader AND leaderClientEndpoint non-empty → send ErrorResponse
//     {code=Redirect, message=leaderClientEndpoint} — over the same async
//     pending-connect machinery (DR-BUG-2; no synchronous resolve exists).
//   - If leader (or hook unset/leader-unknown) → ASYNC: register
//     addExclusivePublication and enqueue a PendingConnect.  The publication
//     is polled across doWork() cycles via pollPendingConnects().
// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// evictOldestPendingConnect_ — the kMaxPendingConnects cap.
// The evicted registration must be released: attempt a final
// findExclusivePublication — see handleConnect.
// ---------------------------------------------------------------------------
void ClientGateway::evictOldestPendingConnect_()
{
    spdlog::warn("ClientGateway: pending-connect cap reached; dropping oldest entry");
    const int64_t evictedRegId      = pendingConnects_.front().registrationId;
    // DR-BUG-2 FIX: if the oldest entry has a stashed resolvedPub, the
    // shared_ptr destructor (run by pop_front()) is sufficient to release the
    // registration.  Do NOT call findExclusivePublication again — that would
    // touch a conductor entry that is already being released, and if it
    // returns nullptr we would push a stale regId to drainingRegistrations_
    // that never drains.  Only push to drain when resolvedPub==nullptr (pub
    // not yet resolved — the conductor still holds it in AWAITING state).
    const bool evictedAlreadyResolved = (pendingConnects_.front().resolvedPub != nullptr);
    // Fix round 1 (I2): an evicted entry whose announcement began holds
    // worker ack-table slots — close it on every worker (never leaked).
    closeDroppedPendingConnect_(pendingConnects_.front());
    pendingConnects_.pop_front(); // O(1); drops resolvedPub shared_ptr if non-null
    if (!evictedAlreadyResolved && aeron_) {
        try {
            // Attempt immediate release by resolving and dropping the shared_ptr.
            auto pub = aeron_->findExclusivePublication(evictedRegId);
            if (!pub) {
                // Still AWAITING_MEDIA_DRIVER — keep draining until it resolves.
                drainingRegistrations_.push_back(evictedRegId);
            }
            // If pub non-null: drops here → ~ExclusivePublication() releases registration.
        } catch (...) {
            // ERRORED_MEDIA_DRIVER: conductor erased the entry and threw — no leak.
        }
    }
    // If evictedAlreadyResolved: the drop of resolvedPub in pop_front() already
    // releases the registration — no findExclusivePublication needed, no drain entry.
}

// ---------------------------------------------------------------------------
// Task 13 fix round 1 — shard-mode session lifecycle helpers.
// ---------------------------------------------------------------------------
std::size_t ClientGateway::shardSessionSlotsInUse_() const noexcept
{
    std::size_t begun = 0;
    for (const auto& p : pendingConnects_) begun += p.announceBegun ? 1 : 0;   // <= kMaxPendingConnects
    return sessions_.size() + sessionAnnouncer_.pendingCloses() + begun;
}

void ClientGateway::closeDroppedPendingConnect_(const PendingConnect& pending)
{
    if (shardMode_ && workerPool_ && pending.announceBegun)
        sessionAnnouncer_.close(*workerPool_, pending.sessionId);
}

bool ClientGateway::closeSession_(int64_t sid, bool notifyExpired)
{
    Session* s = sessions_.find(sid);
    if (!s) return false;                     // m4: unknown → no worker traffic
    // Fix round 2 (finding 2): tell the client before its publication goes —
    // the rune client reconnects only on SessionExpired / UnknownSession.
    if (notifyExpired) sendSessionExpired_(nullptr, s, sid);
    if (s->heldByWorkers != 0) --heldSessions_;
    sessions_.remove(sid);                    // releases its egress publication
    // Task 13: the workers forget it too (its queued acks drop, its
    // publications are released) — retried until every worker has it.
    if (shardMode_ && workerPool_) sessionAnnouncer_.close(*workerPool_, sid);
    // Hybrid egress: drop any in-flight record-egress resolves for this session
    // (all workers — its buffered subscribes die with the session).
    for (auto pit = pendingRecordEgress_.begin(); pit != pendingRecordEgress_.end(); ) {
        pit = (pit->first.first == sid) ? pendingRecordEgress_.erase(pit) : std::next(pit);
    }

    // GB-3: drop all subscriptions belonging to this session.
    auto it = subscriptions_.begin();
    while (it != subscriptions_.end()) {
        if (it->second.sessionId == sid) {
            it = subscriptions_.erase(it);
        } else {
            ++it;
        }
    }

    // CG-4 fix (#2): erase the group-member tracking entry so groupMemberSubs_
    // does not grow unboundedly under connect/disconnect churn.
    groupMemberSubs_.erase(sid);
    return true;
}

bool ClientGateway::produceFrameHeld_(const uint8_t* buf, std::size_t len)
{
    if (len < pc::MessageHeader::ENCODED_SIZE) return false;
    const uint16_t templateId = static_cast<uint16_t>(buf[2] | (buf[3] << 8));
    if (templateId != pc::Produce::TEMPLATE_ID) return false;   // only produce intake is paused
    uint64_t sid = 0;
    for (int i = 7; i >= 0; --i) sid = (sid << 8) | buf[8 + i];
    const Session* s = sessions_.find(static_cast<int64_t>(sid));
    return s && s->heldByWorkers != 0;
}

void ClientGateway::sendSessionExpired_(aeron::ExclusivePublication* pub, Session* session, int64_t sid)
{
    pc::ErrorResponse e;
    e.sessionId     = sid;
    e.correlationId = 0;
    e.code          = static_cast<int16_t>(pc::ErrorCode::SessionExpired);
    const std::size_t encoded = e.encode(scratch_.data());
    if (session) (void)sendTo(*session, scratch_.data(), encoded);   // best effort
    else if (pub) (void)sendOnPub(*pub, scratch_.data(), encoded);
}

void ClientGateway::onSessionSignal_(const rune::node::WorkerReturn& ret)
{
    using K = rune::node::SessionSignalKind;
    const auto kind = static_cast<K>(ret.lastOffset);
    if (kind == K::Held || kind == K::Released) {
        Session* s = sessions_.find(ret.sessionId);
        if (!s) return;                                               // closed meanwhile
        // Unreachable: NodeConfig refuses > kMaxHeldMaskWorkers shard workers.
        assert(ret.partition >= 0 && ret.partition < kMaxHeldMaskWorkers);
        if (ret.partition < 0 || ret.partition >= kMaxHeldMaskWorkers) [[unlikely]] return;
        const uint64_t bit = uint64_t{1} << ret.partition;
        const bool was = s->heldByWorkers != 0;
        if (kind == K::Held) s->heldByWorkers |= bit; else s->heldByWorkers &= ~bit;
        const bool now = s->heldByWorkers != 0;
        if (now && !was) ++heldSessions_;
        if (was && !now) --heldSessions_;
        return;
    }
    // Failed (C2): close the session everywhere and tell the client, so it
    // reconnects; its later notifications find no session.  State-driven.
    if (closeSession_(ret.sessionId, /*notifyExpired=*/true)) {
        spdlog::warn("ClientGateway: session {} closed — its ack publication on worker {} can never "
                     "deliver (the client was told SessionExpired)", ret.sessionId, ret.partition);
        return;
    }
    // Not accepted yet (ConnectResponse still retrying): drop the pending
    // connect — never accept a session no worker can ack.
    for (auto it = pendingConnects_.begin(); it != pendingConnects_.end(); ++it) {
        if (it->sessionId != ret.sessionId) continue;
        spdlog::warn("ClientGateway: pending session {} dropped — its ack publication on worker {} can "
                     "never deliver", ret.sessionId, ret.partition);
        if (it->resolvedPub) sendSessionExpired_(it->resolvedPub.get(), nullptr, ret.sessionId);
        closeDroppedPendingConnect_(*it);
        pendingConnects_.erase(it);   // resolvedPub released with the entry
        break;
    }
}

void ClientGateway::handleConnect(const uint8_t* buf, std::size_t len, int64_t nowNs)
{
    auto reqOpt = pc::ConnectRequest::decode(buf, len);
    if (!reqOpt) return; // truncated — drop

    const auto& req = *reqOpt;
    // Task 13 fix round 1 (I2): a worker's ack table stores the reply channel in
    // a fixed slot — refuse a longer one HERE (never accept a session no worker
    // can ack; the worker treats a refused announcement as an invariant halt).
    if (shardMode_ && req.egressChannel.size() > rune::node::SessionAckSink::kMaxChannelBytes) [[unlikely]] {
        spdlog::warn("ClientGateway: egress channel of {} B exceeds the shard limit {} B; dropping Connect",
                     req.egressChannel.size(), rune::node::SessionAckSink::kMaxChannelBytes);
        return;
    }

    // -----------------------------------------------------------------------
    // BR-5 / MD1 Fix 6: check leadership before creating a session.
    //
    // MD1 Fix 6 (per-partition routing session gate):
    //   When routingImage_ is set (per-partition routing is active — either from
    //   StaticBalancer during the bootstrap window, or from committedMap in steady
    //   state), this node can accept sessions regardless of Raft leadership.
    //   The produce / subscribe handlers route each op to the correct partition
    //   leader via Path A (routingImage_ lookup), so the session host does NOT
    //   need to be the Raft leader.
    //
    //   Rationale: without this gate, Raft-follower nodes that are partition
    //   leaders (e.g. rune-1 = node 0 holds events:0) could never accept client
    //   sessions, because their leaderView_.isLeader == false caused
    //   handleConnect to enqueue an ErrorResponse{Redirect} back to the Raft
    //   leader.  Combined with Fix 5 (StaticBalancer immediate routingImage_ on
    //   all nodes), this caused a redirect loop:
    //     rune-2 (Raft leader) → Path A → Redirect produce to rune-1
    //     rune-1 (Raft follower) → handleConnect → Redirect session to rune-2
    //     → kMaxRedirects exhausted.
    //
    //   When routingImage_ is null (not yet set), fall back to the original
    //   Raft-leader gate (Path B / pre-MD1 behaviour).
    // -----------------------------------------------------------------------
    bool capturedIsLeader = true;
    std::string capturedLeaderEndpoint;
    if (leaderView_ && !routingImage_) {
        // No per-partition routing image yet: use the Raft-leader gate (Path B /
        // pre-MD1 bootstrap).  Raft followers redirect sessions to the Raft leader.
        const LeaderView lv = leaderView_();
        capturedIsLeader        = lv.isLeader;
        capturedLeaderEndpoint  = lv.leaderClientEndpoint;

        // NOTE: the REDIRECT case (!isLeader && !leaderClientEndpoint.empty()) is
        // handled by the SAME async pending-connect machinery as the leader-accept
        // path below (see DR-BUG-2 fix).  We do NOT resolve the egress publication
        // synchronously here.
        //
        // DR-BUG-2 (redirect-reconnect storm) ROOT CAUSE + FIX:
        //   Previously this path called the BLOCKING resolvePublication(), which
        //   sleeps up to kResolveMaxMs (5 s) in 1 ms slices ON THE GATEWAY
        //   DUTY-CYCLE THREAD.  When N clients hit a follower at once (the DR-3
        //   reconnect storm), each ConnectRequest was dispatched serially inside a
        //   single sub_->poll() and each blocked in resolvePublication before the
        //   next client's addExclusivePublication was even issued.  One slow egress
        //   warm-up (media-driver contention while N new publications warm up on
        //   BOTH follower and leader simultaneously) head-of-line-blocked the whole
        //   follower gateway — no ingress poll, no reaping, and the LAST client's
        //   redirect was delayed enough that its subsequent leader connect blew the
        //   app-side connect deadline (the intermittent 1-of-5 connect() timeout).
        //   The synchronous resolve ALSO serialised the follower's own doWork loop.
        //
        //   Fix: enqueue the redirect as an async PendingConnect (non-blocking
        //   addExclusivePublication) — pollPendingConnects() resolves ALL pending
        //   entries fairly per cycle (no head-of-line block), stashes the resolved
        //   pub, and retries the ErrorResponse{Redirect} until the egress link
        //   warms up.  Bounded by kMaxPendingConnects + connectResolveTimeoutNs_,
        //   single-writer on the gateway agent thread, no busy-spin.
        //
        // else (leader-unknown, empty endpoint) → degrade to local accept below,
        // ALSO via the same async machinery (isLeader captured true / redirect
        // endpoint empty ⇒ pollPendingConnects sends ConnectResponse{OK}).
    }
    // else: routingImage_ is set → capturedIsLeader remains true (accept session
    //       on any node; per-partition routing in the produce/subscribe handlers
    //       routes each op to the correct partition leader).  This is the MD1
    //       steady-state and bootstrap-window path (Fix 5 + Fix 6 together).

    // -----------------------------------------------------------------------
    // Async accept path (BR-7a): never block the duty-cycle thread.
    // -----------------------------------------------------------------------

    // Cap: evict the oldest pending entry if we are at capacity.
    // The evicted registration must be released: attempt a final
    // findExclusivePublication — if it resolves (shared_ptr non-null) let it
    // drop immediately (destructor calls
    // ClientConductor::releaseExclusivePublication, see ExclusivePublication.cpp
    // ~ExclusivePublication()); if it throws (ERRORED), conductor already cleaned
    // up; if still AWAITING (nullptr returned), enqueue in drainingRegistrations_
    // and keep polling until it resolves.
    if (pendingConnects_.size() >= kMaxPendingConnects) evictOldestPendingConnect_();

    // Pre-allocate sessionId now so it is stable across cycles.
    const int64_t sessionId = sessions_.nextSessionId();

    // Kick off the egress publication registration (returns immediately).
    int64_t regId = -1;
    try {
        regId = aeron_->addExclusivePublication(
            std::string(req.egressChannel), req.egressStreamId);
    } catch (...) {
        // Bad channel — drop (never crash).
        spdlog::warn("ClientGateway: addExclusivePublication threw for egress channel '{}'; dropping Connect",
                     std::string(req.egressChannel));
        return;
    }

    PendingConnect pc_entry;
    pc_entry.registrationId        = regId;
    pc_entry.sessionId             = sessionId;
    pc_entry.correlationId         = req.correlationId;
    pc_entry.requestedAtNs         = nowNs;
    pc_entry.isLeader              = capturedIsLeader;
    pc_entry.leaderClientEndpoint  = std::move(capturedLeaderEndpoint);
    pc_entry.egressChannel         = std::string(req.egressChannel);
    pc_entry.egressStreamId        = req.egressStreamId;
    pendingConnects_.push_back(std::move(pc_entry));
}

// ---------------------------------------------------------------------------
// pollPendingConnects — called from doWork() each cycle.
//
// For each pending-connect entry:
//   - Try findExclusivePublication(registrationId).
//   - If resolved → promote to SessionTable + send ConnectResponse (OK).
//   - If timeout exceeded → drop + log.
//   - Otherwise → leave in pending for next cycle.
// Never blocks.
// ---------------------------------------------------------------------------
void ClientGateway::pollPendingConnects(int64_t nowNs)
{
    // Drive the draining-release table first (bounded, non-blocking).
    drainPendingReleases();

    if (pendingConnects_.empty()) return;

    namespace pc = rune::protocol::client;

    // Hand-rolled forward-erase loop (not std::remove_if — we need per-element
    // control to handle three distinct outcomes: resolve, timeout, and error).
    auto it = pendingConnects_.begin();
    while (it != pendingConnects_.end()) {
        PendingConnect& pending = *it;

        // Check timeout first.
        if (nowNs - pending.requestedAtNs > connectResolveTimeoutNs_) {
            spdlog::warn("ClientGateway: pending-connect session={} timed out after {}ms; dropped",
                         pending.sessionId,
                         (nowNs - pending.requestedAtNs) / 1'000'000LL);

            // DR-BUG-2 FIX: if the publication was already resolved and stashed
            // in pending.resolvedPub, the shared_ptr release happens naturally
            // when the PendingConnect entry is erased (destructor drops the
            // shared_ptr → ~ExclusivePublication() releases the registration).
            // We must NOT call findExclusivePublication again in that case — the
            // conductor entry is already being released, and a redundant find
            // would return nullptr and push a stale regId into drainingRegistrations_
            // that can never drain (use-after-release).
            //
            // Only call findExclusivePublication / push to drainingRegistrations_
            // when the publication was NOT yet resolved (resolvedPub == nullptr):
            // in that case the conductor still holds the registration in AWAITING
            // state and we must poll it until it resolves so we can drop it.
            const int64_t timedOutRegId = pending.registrationId;
            const bool alreadyResolved  = (pending.resolvedPub != nullptr);
            // Task 13: a session announced (fully or partly) but never accepted
            // is closed on every worker — the client never learned its id.
            closeDroppedPendingConnect_(pending);
            it = pendingConnects_.erase(it); // drops resolvedPub shared_ptr if non-null
            if (!alreadyResolved) {
                // Publication not yet resolved: enqueue in drain table.
                // If resolved (non-null shared_ptr): drop immediately.
                // If throws (ERRORED): conductor already cleaned up.
                // If nullptr (still AWAITING): enqueue to keep polling until resolved.
                try {
                    auto pub = aeron_->findExclusivePublication(timedOutRegId);
                    if (!pub) {
                        drainingRegistrations_.push_back(timedOutRegId);
                    }
                    // pub non-null: drops here → destructor releases registration.
                } catch (...) {
                    // ERRORED_MEDIA_DRIVER: conductor erased the entry — no leak.
                }
            }
            continue;
        }

        // Try to find the publication (non-blocking).
        std::shared_ptr<aeron::ExclusivePublication> pub;
        try {
            pub = aeron_->findExclusivePublication(pending.registrationId);
        } catch (...) {
            // ERRORED_MEDIA_DRIVER: conductor signalled an error (bad channel, etc.).
            // The conductor erased its own entry when throwing — no registration leak.
            spdlog::warn("ClientGateway: findExclusivePublication threw for session={}; dropped",
                         pending.sessionId);
            closeDroppedPendingConnect_(pending);   // fix round 1 (I2): every drop site
            it = pendingConnects_.erase(it);
            continue;
        }

        if (!pub) {
            // Not yet resolved — but we may already have a stashed resolved pub
            // from a previous cycle (DR-BUG-2 fix: stash-and-retry path).
            if (!pending.resolvedPub) {
                // Still waiting for the conductor to register the publication.
                ++it;
                continue;
            }
            // Use the already-resolved pub from a previous cycle.
            pub = pending.resolvedPub;
        }

        // Resolved (either this cycle or previously stashed).
        //
        // DR-BUG-2 FIX: stash the resolved pub in the PendingConnect entry so
        // that if the ConnectResponse send fails (publication not yet Aeron-
        // connected — the UDP image between the broker and the client egress
        // endpoint may not be established yet when 5 clients start
        // simultaneously), we keep retrying on subsequent doWork() cycles
        // instead of dropping the entry.  The resolve-timeout still applies:
        // if connectResolveTimeoutNs_ elapses before the send succeeds, the
        // entry is dropped and the client times out and re-Connects.
        //
        // This fixes the ~33% failure rate for one-of-five concurrent clients
        // under load: previously a not-yet-connected pub caused an immediate
        // drop and the client waited the full 15 s connectTimeoutMs.
        if (!pending.resolvedPub) {
            pending.resolvedPub = pub;
        }

        // DR-BUG-2 (redirect-reconnect storm): a follower entry (captured
        // !isLeader with a known leader endpoint) sends ErrorResponse{Redirect}
        // on its resolved egress and creates NO session — the client will
        // retarget to the leader and reconnect there.  This shares the fair,
        // non-blocking poll loop with the leader-accept path (no synchronous
        // resolvePublication head-of-line block on the duty-cycle thread).
        const bool isRedirect = (!pending.isLeader && !pending.leaderClientEndpoint.empty());

        std::size_t encoded = 0;
        if (isRedirect) {
            pc::ErrorResponse err;
            err.sessionId     = 0;
            err.correlationId = pending.correlationId;
            err.code          = static_cast<int16_t>(pc::ErrorCode::Redirect);
            err.message       = pending.leaderClientEndpoint;

            // Guard: message must fit in scratch (encode would overflow otherwise).
            const std::size_t needed = pc::MessageHeader::ENCODED_SIZE +
                                       pc::ErrorResponse::BODY_FIXED + 2 +
                                       pending.leaderClientEndpoint.size();
            if (needed > scratch_.size()) {
                // Pathological endpoint length — drop (cannot encode).  The
                // resolvedPub shared_ptr release on erase() frees the egress reg.
                spdlog::warn("ClientGateway: redirect endpoint too large ({}B); dropping",
                             pending.leaderClientEndpoint.size());
                it = pendingConnects_.erase(it);
                continue;
            }
            encoded = err.encode(scratch_.data());
        } else {
            // Task 13 (spec §3.4) announce-before-accept: every worker must know
            // this session's reply destination before the client can learn its
            // session id (its first Produce may reach a worker before any later
            // runtime message).  A full worker ring defers the accept to the next
            // cycle (bounded by the connect timeout, never a wait); workers
            // already reached are not re-sent.  The worker ack table's capacity
            // bounds concurrent shard sessions (pending closes still hold slots).
            if (shardMode_ && workerPool_ && !pending.announced) {
                // Fix round 1 (I2): count every slot a worker table may hold —
                // including announced-but-not-accepted connects (this one is
                // counted once its announcement begins).
                if (!pending.announceBegun &&
                    shardSessionSlotsInUse_() + 1 > kShardMaxSessions) [[unlikely]] {
                    spdlog::warn("ClientGateway: shard session cap {} reached; session={} held until the "
                                 "connect timeout", kShardMaxSessions, pending.sessionId);
                    ++it;
                    continue;
                }
                pending.announceBegun = true;
                if (!sessionAnnouncer_.announce(*workerPool_, pending.sessionId,
                                                pending.egressChannel, pending.egressStreamId)) {
                    ++it;
                    continue;   // a worker ring is full — retry next cycle
                }
                pending.announced = true;
            }
            // Invariant (FIX A): a session must NEVER be added to the table unless
            // the client received a ConnectResponse for it.  Only promote to a
            // session after a successful send.
            pc::ConnectResponse resp;
            resp.sessionId         = pending.sessionId;
            resp.correlationId     = pending.correlationId;
            resp.code              = static_cast<int16_t>(pc::ErrorCode::OK);
            resp.leaderMemberId    = thisNodeId_;
            resp.assignedSessionId = pending.sessionId;
            encoded = resp.encode(scratch_.data());
        }

        if (!sendOnPub(*pub, scratch_.data(), encoded)) {
            // Publication not yet Aeron-connected (UDP image not yet up) or
            // transient back-pressure.  Keep the PendingConnect alive
            // (resolvedPub is now stashed); retry next doWork() cycle.
            // The resolve-timeout still bounds this: if it expires before the
            // link warms up, we drop + the client re-Connects.
            spdlog::debug("ClientGateway: {} not-connected/back-pressured for "
                          "session={}; retrying next cycle (pub stashed)",
                          isRedirect ? "Redirect" : "ConnectResponse",
                          pending.sessionId);
            ++it;
            continue;
        }

        // Send succeeded.  Redirect entries create NO session (the resolvedPub
        // shared_ptr release on erase frees the transient egress registration).
        if (!isRedirect) {
            // Only now is it safe to add the session (leader-accept path).
            // Carry the client's egress endpoint so a per-consumer unicast Records
            // publication can be lazily created on the first subscribe.
            sessions_.add(pending.sessionId, std::move(pub), nowNs,
                          pending.egressChannel, pending.egressStreamId);
        }

        it = pendingConnects_.erase(it);
    }
}

// ---------------------------------------------------------------------------
// drainPendingReleases — poll registrations that were logically dropped before
// their ExclusivePublication resolved, to complete the release.
//
// Release mechanism (Aeron C++ 1.41.4, ExclusivePublication.cpp line 62-65):
//   ~ExclusivePublication() { m_conductor.releaseExclusivePublication(m_registrationId); }
// So: obtaining a shared_ptr via findExclusivePublication and letting it drop
// is the ONLY correct release path for a registration that is still in
// AWAITING_MEDIA_DRIVER state.  Once the conductor transitions it to
// REGISTERED_MEDIA_DRIVER, findExclusivePublication returns a non-null
// shared_ptr; dropping it triggers the destructor which releases the
// registration (and driver-side log buffers).
// ---------------------------------------------------------------------------
void ClientGateway::drainPendingReleases()
{
    if (drainingRegistrations_.empty()) return;

    // Drain-table growth warning (rate-limited: emit once per threshold-crossing).
    // The table is expected-bounded — an Aeron ExclusivePublication resolves within
    // a few driver cycles — so sustained growth indicates a stuck/never-resolving
    // driver entry that warrants operator attention.  We do NOT hard-cap (evicting
    // an unresolved entry re-introduces the registration leak this table prevents).
    {
        static constexpr std::size_t kDrainWarnThreshold = 4 * kMaxPendingConnects;
        static bool sWarned = false;
        const std::size_t drainSz = drainingRegistrations_.size();
        if (drainSz > kDrainWarnThreshold && !sWarned) {
            spdlog::warn("ClientGateway: drainingRegistrations_ size {} exceeds {}×kMaxPendingConnects={}; "
                         "possible stuck driver entry — investigate Aeron media driver state",
                         drainSz, 4u, kMaxPendingConnects);
            sWarned = true;
        } else if (drainSz <= kDrainWarnThreshold) {
            sWarned = false; // reset so the warning fires again if it grows again
        }
    }

    auto it = drainingRegistrations_.begin();
    while (it != drainingRegistrations_.end()) {
        const int64_t regId = *it;
        try {
            auto pub = aeron_->findExclusivePublication(regId);
            if (pub) {
                // Resolved: drop the shared_ptr immediately.
                // ~ExclusivePublication() calls
                // m_conductor.releaseExclusivePublication(registrationId).
                pub.reset();
                it = drainingRegistrations_.erase(it);
            } else {
                // Still AWAITING — try again next doWork cycle.
                ++it;
            }
        } catch (...) {
            // ERRORED_MEDIA_DRIVER: conductor erased its internal entry and threw.
            // The registration is already cleaned up — remove from drain list.
            it = drainingRegistrations_.erase(it);
        }
    }
}

// ---------------------------------------------------------------------------
// handleKeepalive
// ---------------------------------------------------------------------------
void ClientGateway::handleKeepalive(const uint8_t* buf, std::size_t len, int64_t nowNs)
{
    auto msgOpt = pc::Keepalive::decode(buf, len);
    if (!msgOpt) return;
    sessions_.touch(msgOpt->sessionId, nowNs);
}

// ---------------------------------------------------------------------------
// handlePing
// ---------------------------------------------------------------------------
void ClientGateway::handlePing(const uint8_t* buf, std::size_t len, int64_t nowNs)
{
    auto pingOpt = pc::Ping::decode(buf, len);
    if (!pingOpt) return;

    Session* session = sessions_.find(pingOpt->sessionId);
    if (!session) {
        // Unknown session — drop (no egress to reply on).
        return;
    }

    // Refresh session liveness: every inbound message (including Ping) keeps
    // the session alive.  Touch before sending the Pong so a Ping-only client
    // (no explicit Keepalive) is never wrongly reaped mid-activity.
    sessions_.touch(pingOpt->sessionId, nowNs);

    // Encode Pong with verbatim echo.
    pc::Pong pong;
    pong.sessionId     = pingOpt->sessionId;
    pong.correlationId = pingOpt->correlationId;
    pong.echo          = pingOpt->echo;

    // Guard: echo payload must fit in the scratch buffer.
    const std::size_t needed = pc::MessageHeader::ENCODED_SIZE + 2 + pingOpt->echo.size();
    if (needed > scratch_.size()) return; // oversized echo — drop

    const std::size_t encoded = pong.encode(scratch_.data());
    sendTo(*session, scratch_.data(), encoded);
}

// ---------------------------------------------------------------------------
// handleDisconnect
// ---------------------------------------------------------------------------
void ClientGateway::handleDisconnect(const uint8_t* buf, std::size_t len)
{
    auto msgOpt = pc::Disconnect::decode(buf, len);
    if (!msgOpt) return;
    (void)closeSession_(msgOpt->sessionId);
}

// ---------------------------------------------------------------------------
// sendTo — tryClaim first, fallback to offer (mirrors AeronMessageSink).
// ---------------------------------------------------------------------------
bool ClientGateway::sendTo(Session& session, const uint8_t* buf, std::size_t len)
{
    // Guard against null egress (test sessions injected via injectTestSession
    // have no real ExclusivePublication).
    if (!session.egress) {
        // CG-4 test hook: when a null-egress callback is installed, fire it and
        // return true so callers (pushSubscriptions) advance nextOffset — this lets
        // tests drive the real push path and capture Records without a live Aeron driver.
        if (onEgressFrame_) {
            onEgressFrame_(session.sessionId, buf, len);
            return true;
        }
        return false;
    }
    return sendOnPub(*session.egress, buf, len);
}

// ---------------------------------------------------------------------------
// sendOnPub — send on a raw ExclusivePublication (used by redirect path).
// ---------------------------------------------------------------------------
bool ClientGateway::sendOnPub(aeron::ExclusivePublication& pub,
                              const uint8_t*               buf,
                              std::size_t                  len)
{
    if (!pub.isConnected()) return false;

    // AtomicBuffer takes a non-const pointer, but Aeron only reads (copies out)
    // the data — it never writes back through the pointer.  The cast is safe.
    auto* mutableBuf = const_cast<uint8_t*>(buf);
    aeron::concurrent::AtomicBuffer atomicBuf(
        mutableBuf, static_cast<aeron::util::index_t>(len));

    if (len <= static_cast<std::size_t>(pub.maxPayloadLength())) {
        aeron::concurrent::logbuffer::BufferClaim claim;
        const int64_t pos = pub.tryClaim(
            static_cast<aeron::util::index_t>(len), claim);
        if (pos > 0) {
            claim.buffer().putBytes(claim.offset(), atomicBuf, 0,
                                    static_cast<aeron::util::index_t>(len));
            claim.commit();
            return true;
        }
        // BACK_PRESSURED / ADMIN_ACTION / NOT_CONNECTED — fall through to offer.
    }

    const int64_t result = pub.offer(atomicBuf, 0,
                                     static_cast<aeron::util::index_t>(len));
    return result > 0;
}

// ---------------------------------------------------------------------------
// handleProduce (GB-2) — leader-local append with broker-assigned offsets.
//
// Flow:
//   1. Decode Produce (tid16) — drop on truncation.
//   2. Require known session — drop (UnknownSession) if not found.
//   GB-4: Check leadership BEFORE touching the engine.
//     - NOT leader + known leaderClientEndpoint → ErrorResponse{Redirect, endpoint},
//       engine untouched.
//     - NOT leader + unknown endpoint → ErrorResponse{NotLeader}, engine untouched.
//     - Leader → serve (existing path).
//   3. Resolve partition: explicit when partitionKnown==1, else partitionForKey().
//   4. Lookup Log* from registry; reply error on unknown topic / bad partition.
//   5. Assign offsets (CRC-safe restamp):
//        base = log->logEndOffset()
//        copy recordBatch bytes into restampScratch_
//        write base as LE64 at kBaseOffsetOffset (excluded from CRC region)
//        read recordCount as LE32 at kRecordCountOffset
//        guard recordCount >= 1 (0-record batch → drop / InvalidBatch response)
//        lastOffset = base + recordCount - 1
//        log->append(restampScratch_[0..batchLen], lastOffset)
//   6. Reply ProduceResponse{OK, partition, baseOffset=base}.
//
// Acks semantics (M-B leader-local):
//   Leader and None → ack after local append (only leader log exists here).
//   All (ISR-majority HW) → same as Leader for now — replication-ack wiring
//   is deferred.
//   // TODO(Milestone B GB-4/replication): true acks=All waits for ISR HW.
//   (Produce in M-B does not carry an `acks` field; always ack after append.)
//
// Drop-safe: never throws; all engine calls wrapped in try/catch.
// Single-writer: called only from onFragment → doWork duty-cycle thread.
// Zero heap allocation on hot path: restampScratch_ is a reusable member array.
// ---------------------------------------------------------------------------
void ClientGateway::setWorkerPool(rune::node::WorkerPool* pool) noexcept {
    workerPool_ = pool;
    if (!pool) return;
    for (int32_t i = 0; i < pool->workerCount(); ++i)
        maxProduceBatchBytes_ = std::min(maxProduceBatchBytes_, pool->worker(i).maxIntakeBatchBytes());
}

void ClientGateway::sendErrorResponse_(Session& session, int64_t sessionId, int64_t correlationId,
                                       rune::protocol::client::ErrorCode code)
{
    namespace pc = rune::protocol::client;
    pc::ErrorResponse errResp;
    errResp.sessionId     = sessionId;
    errResp.correlationId = correlationId;
    errResp.code          = static_cast<int16_t>(code);
    const std::size_t needed = pc::MessageHeader::ENCODED_SIZE + pc::ErrorResponse::BODY_FIXED + 2;
    if (needed <= scratch_.size()) {
        const std::size_t encoded = errResp.encode(scratch_.data());
        // G0.1: reliable delivery (retry on back-pressure).
        enqueueOrBestEffort_(session, scratch_.data(), encoded);
    }
}

void ClientGateway::handleProduce(const uint8_t* buf, std::size_t len)
{
    // 1. Decode Produce.
    auto msgOpt = pc::Produce::decode(buf, len);
    if (!msgOpt) {
        spdlog::debug("ClientGateway: Produce decode failed (truncated?) — dropped");
        return;
    }
    const pc::Produce& msg = *msgOpt;
    RUNE_FR_SPAN_PRODUCE(rune_fr::Site::GatewayHandle,
                         static_cast<uint32_t>(msg.correlationId));

    // 2. Require known session.
    Session* session = sessions_.find(msg.sessionId);
    if (!session) {
        spdlog::debug("ClientGateway: Produce for unknown sessionId={} — dropped", msg.sessionId);
        return;
    }
    // F23: any request is liveness (Kafka: idle == no requests).  Reuses the
    // lookup above — no extra cost on the produce hot path.
    session->lastSeenNs = nowNs_;
    // G0.1: capture session id so the public sendRespReliable() test-seam knows
    // which session to target for the duration of this handleProduce() call.
    currentProduceSessionId_ = msg.sessionId;

    // GB-4 / MD1-9: Leadership check — data ops MUST reach the partition leader.
    //
    // Two-path routing decision:
    //   (A) Per-partition routing (MD1 Task 9): when routingImage_ is set, we
    //       resolve the partition first, then look up the routing image to determine
    //       whether this node is the partition leader.  If not → REDIRECT to that
    //       partition's leader endpoint.  This replaces the cluster-level boolean
    //       check for the per-partition case.
    //
    //   (B) Cluster-level fallback: when routingImage_ is null (no metadata service
    //       wired in yet, or single-node bypass), fall back to the original
    //       leaderView_-based check for backward compatibility.
    //
    // Helper: send a redirect/notleader error response (used by both paths).
    // G0.1: also routes through the retry queue on back-pressure.
    auto sendRedirect = [&](const std::string& endpoint, int64_t corrId) {
        pc::ErrorResponse errResp;
        errResp.sessionId     = msg.sessionId;
        errResp.correlationId = corrId;
        if (!endpoint.empty()) {
            errResp.code    = static_cast<int16_t>(pc::ErrorCode::Redirect);
            errResp.message = endpoint;
            spdlog::debug("ClientGateway: Produce per-partition redirect sessionId={} → {}",
                          msg.sessionId, endpoint);
        } else {
            errResp.code    = static_cast<int16_t>(pc::ErrorCode::NotLeader);
            spdlog::debug("ClientGateway: Produce per-partition NotLeader (endpoint unknown) sessionId={}",
                          msg.sessionId);
        }
        const std::size_t needed = pc::MessageHeader::ENCODED_SIZE +
                                   pc::ErrorResponse::BODY_FIXED + 2 +
                                   errResp.message.size();
        if (needed > scratch_.size()) return;   // oversized — drop (same as before)
        const std::size_t encoded = errResp.encode(scratch_.data());
        // G0.1: route through the single reliable-egress primitive (retries on
        // back-pressure; best-effort fallback for oversized redirect URLs).
        enqueueOrBestEffort_(*session, scratch_.data(), encoded);
    };

    // Cluster-level fallback (path B): only when routingImage_ is null.
    if (!routingImage_ && leaderView_) {
        const LeaderView lv = leaderView_();
        if (!lv.isLeader) {
            sendRedirect(lv.leaderClientEndpoint, msg.correlationId);
            return;  // engine untouched
        }
    }

    // G0.1: Helper: send ProduceResponse reliably (retry on back-pressure).
    // Delegates to sendRespReliable_ which encodes once and enqueues on
    // BACK_PRESSURED / ADMIN_ACTION instead of silently dropping the ack.
    auto sendResp = [&](pc::ErrorCode ec, int32_t partition, int64_t baseOffset) {
        sendRespReliable_(*session, ec, partition, baseOffset, msg.correlationId);
    };

    // 3. Resolve partition.
    int32_t partition = msg.partition;
    if (msg.partitionKnown == 0) {
        // Key-based: resolve via TopicRegistry (FNV-1a 64 % partitionCount).
        if (!topicRegistry_) {
            spdlog::warn("ClientGateway: Produce key-partition but no TopicRegistry — UnknownTopic");
            sendResp(pc::ErrorCode::UnknownTopic, 0, 0);
            return;
        }
        partition = topicRegistry_->partitionForKey(msg.topicId, msg.key);
        if (partition < 0) {
            spdlog::debug("ClientGateway: Produce partitionForKey → unknown topicId={}", msg.topicId);
            sendResp(pc::ErrorCode::UnknownTopic, 0, 0);
            return;
        }
    }

    // 3b. MD1 Task 9: Per-partition routing image check (path A).
    //
    // When routingImage_ is set, look up the global partition id for this
    // (topicId, partition) to determine the authoritative leader for this
    // partition.  The GPID scheme: gpid = gpidOf(topicId, partition).
    // This must match the scheme used by BrokerNode/MetadataService when building
    // the RoutingImage.
    //
    // Capture-once discipline (MD1-8 ruling):
    //   routingImage_ is a shared_ptr captured ONCE at the point we check it here.
    //   All lookup() calls within this handler use `img` (the raw pointer from the
    //   captured shared_ptr), never calling routingImage_ again.
    //   This avoids per-produce atomic_load (Apple libc++ global lock / Linux CAS).
    //
    // BOOTSTRAP FIX — defense-in-depth (MD1-13):
    //   Even after the primary fix (NodeRuntimeAgent gates setRoutingImageSnapshot
    //   on a non-empty committedMap), a convergence window exists where routingImage_
    //   is non-null but contains NO_LEADER for a partition that is legitimately not
    //   yet assigned (e.g. rolling assignment commit, or a topic added mid-flight).
    //   When that happens, fall back to Path B (cluster-leader check) rather than
    //   returning NotLeader — this avoids redirect-exhaustion for genuinely
    //   bootstrap-window produces.  A steady-state unassigned partition (where the
    //   Raft leader cannot serve it either) will still reject via Path B.
    if (routingImage_) {
        // Capture the snapshot once (the shared_ptr was already stored in routingImage_
        // on the duty-cycle thread — no cross-thread atomic_load needed here).
        const rune::metadata::RoutingImage* img = routingImage_.get();
        const int32_t gpid = rune::metadata::gpidOf(msg.topicId, partition);
        const auto& entry = img->lookup(gpid);

        if (entry.leaderNodeId == rune::metadata::RoutingImage::NO_LEADER_NODE_ID) {
            // Partition not yet assigned in the routing image — bootstrap window or
            // genuinely unassigned.  Fall back to Path B (cluster-leader fallback)
            // instead of returning NotLeader directly, so the Raft leader can serve
            // the produce if it is the cluster leader and has the log locally.
            // Path B is applied immediately below (outside this if-block) when
            // leaderView_ is set; if leaderView_ is null, we fall through to the
            // log lookup (single-node passthrough, always-leader).
            spdlog::debug("ClientGateway: Produce partition not in routing image "
                          "(topicId={} part={}) — falling back to cluster-leader path, sessionId={}",
                          msg.topicId, partition, msg.sessionId);
            if (leaderView_) {
                const LeaderView lv = leaderView_();
                if (!lv.isLeader) {
                    sendRedirect(lv.leaderClientEndpoint, msg.correlationId);
                    return;  // engine untouched
                }
            }
            // leaderView_ says we ARE the leader (or is null) — serve locally (fall through).
        } else if (entry.leaderNodeId != thisNodeId_) {
            // This partition is led by a different node — REDIRECT.
            const std::string& redirectEndpoint = [&]() -> const std::string& {
                auto it = nodeEndpointMap_.find(entry.leaderNodeId);
                if (it != nodeEndpointMap_.end()) return it->second;
                static const std::string kEmpty;
                return kEmpty;
            }();
            spdlog::debug("ClientGateway: Produce per-partition redirect "
                          "(topicId={} part={} leader={} epoch={}) sessionId={} → {}",
                          msg.topicId, partition, entry.leaderNodeId, entry.leaderEpoch,
                          msg.sessionId, redirectEndpoint);
            sendRedirect(redirectEndpoint, msg.correlationId);
            return;  // engine untouched
        }
        // entry.leaderNodeId == thisNodeId_ → serve locally (fall through).
    }

    // 4. Lookup Log.
    if (!topicRegistry_) {
        spdlog::warn("ClientGateway: Produce but no TopicRegistry — UnknownTopic");
        sendResp(pc::ErrorCode::UnknownTopic, partition, 0);
        return;
    }

    // topic() returns nullptr for unknown topicId.
    const rune::node::TopicSpec* spec = topicRegistry_->topic(msg.topicId);
    if (!spec) {
        spdlog::debug("ClientGateway: Produce unknown topicId={}", msg.topicId);
        sendResp(pc::ErrorCode::UnknownTopic, partition, 0);
        return;
    }

    rune::storage::Log* log = topicRegistry_->log(msg.topicId, partition);
    if (!log) {
        spdlog::debug("ClientGateway: Produce bad partition={} for topicId={}", partition, msg.topicId);
        sendResp(pc::ErrorCode::UnknownPartition, partition, 0);
        return;
    }

    // GR-4 / F15 §3.1 (I1): per-partition NotLeader.
    //
    // A worker-owned replica's role is the OWNING worker's state: reading
    // role() here (control thread) was a data race and a check-then-act race —
    // the role can change between this check and the worker's append.  The
    // owning worker is the authority (role gate in executeProduceWire_ /
    // handleWorkerIngressFrame_).  The gateway holds only a GatewayReplicaView
    // (no role()) and consults the control-owned intended-role HINT the bridge
    // records when it routes a role op:
    //   - Follower → fast NotLeader (nothing routed);
    //   - Leader or absent → route; W decides.
    // The hint may lead or lag W; both are safe (W is authoritative, both
    // answers are retriable).  The bootstrap window (committedMetadataReady_
    // false, replicas still Follower) is answered NotLeader by W (Q4).
    // The INLINE path below (no worker pool / gateway-owned internal partition)
    // is the replica's owner thread and keeps its own role check there.
    //
    // O(1) lookup (unordered_map): zero per-op alloc, single-writer.
    GatewayReplicaView replica;
    if (replicaRegistry_) {
        replica = GatewayReplicaView{replicaRegistry_->lookupHandle(msg.topicId, partition)};
        if (replica.intendedRole() == GatewayReplicaView::IntendedRole::Follower) [[unlikely]] {
            spdlog::debug("ClientGateway: Produce per-partition NotLeader (intended-role hint) "
                          "(topicId={} partition={}) sessionId={}",
                          msg.topicId, partition, msg.sessionId);
            sendErrorResponse_(*session, msg.sessionId, msg.correlationId, pc::ErrorCode::NotLeader);
            return;  // engine untouched — no append, nothing routed
        }
    }

    // 5. Assign offsets (CRC-safe restamp).

    // Guard: recordBatch must be present (non-empty).
    const std::size_t batchLen = msg.recordBatch.size();
    if (batchLen == 0) {
        spdlog::debug("ClientGateway: Produce empty recordBatch — dropped");
        // Reply InvalidBatch (no valid errorCode for InvalidBatch in client::ErrorCode;
        // use Internal as the closest sentinel for a malformed produce).
        sendResp(pc::ErrorCode::Internal, partition, 0);
        return;
    }

    // Guard: reject an oversized batch LOUDLY (explicit MessageTooLarge), never
    // silently drop.  The zero-copy append path has no fixed scratch cap anymore;
    // this bound only protects the worker ingress ring / log segment from an
    // absurd or malformed length.  The client caps its batch wire-size below this.
    // F15 S3 (review m4 / I-1): the gateway and the worker share these bounds.
    static_assert(kMaxProduceBatchBytes == rune::node::Worker::kMaxProduceBatchBytes,
                  "gateway and shard-ingress max produce batch must match");
    static_assert(kMaxChunkBytes == rune::node::Worker::kMaxPushReadChunkBytes,
                  "gateway and worker PushRead chunk budget must match");
    if (batchLen > maxProduceBatchBytes_) [[unlikely]] {
        // F3 round 6, item 2: rate-limited (first, then every 1024th) —
        // control-thread-owned counter (single-writer).
        if (oversizeProduceRejects_++ % 1024 == 0) {
            ++oversizeProduceRejectLogs_;
            spdlog::warn("ClientGateway: Produce batch too large ({} > {}); rejected "
                         "(MessageTooLarge, rejects={})",
                         batchLen, maxProduceBatchBytes_, oversizeProduceRejects_);
        }
        sendResp(pc::ErrorCode::MessageTooLarge, partition, 0);
        return;
    }

    // Guard: batch must be at least large enough to hold the fixed header fields
    // we will read (kRecordCountOffset + 4 bytes).
    // M1 fix: also need kLastOffsetDeltaOffset + 4 bytes (kLastOffsetDeltaOffset=23 < 57).
    constexpr std::size_t kMinBatchSize =
        static_cast<std::size_t>(rune::protocol::batch::kRecordCountOffset) + 4u;
    if (batchLen < kMinBatchSize) {
        spdlog::debug("ClientGateway: Produce batch too short ({} < {}); dropped",
                      batchLen, kMinBatchSize);
        sendResp(pc::ErrorCode::Internal, partition, 0);
        return;
    }

    // Zero-copy: the recordBatch is validated + appended directly from the
    // read-only Aeron fragment (msg.recordBatch) — no restamp scratch copy.  The
    // base offset is patched into the page cache by Log::appendNoSyncRestamp
    // (inline path) or by the worker's appendNoSyncRestamp (worker path).
    const uint8_t* rbData = msg.recordBatch.data();

    // ---------------------------------------------------------------------------
    // acks=All gateway-policy checks (bootstrap window + ISR gate).
    //
    // These checks live in the gateway (not the helper) because they use gateway
    // state (committedMetadataReady_, replica->isr(), cachedMinIsr_) and may
    // return non-ProduceResponse error types (ErrorResponse{NotLeader}).
    //
    // BOOTSTRAP WINDOW — acks=All DURABILITY CONTRACT (MD1-13 Fix Round 2):
    //   When committedMetadataReady_ is false (committedMap still empty — no
    //   majority-committed assignment yet), the replica is still a Follower
    //   (bridge Step B has not fired).  In this state:
    //     - The replica's ISR is empty (Follower role) → min-ISR gate would always
    //       reject with MinIsrViolation.
    //     - onLeaderLogAppended() is a no-op on Followers → HW never advances →
    //       the deferred-ack queue would stall until timeout.
    //   SILENTLY downgrading acks=All to acks=Leader (immediate ack after local
    //   append) violates the diamond-durability contract: the caller requested
    //   majority-committed durability and the broker would ack without it.
    //
    //   Correct behaviour: return NotLeader (a retriable error code) so the client
    //   retries the produce after bootstrap completes.  NotLeader is retriable by
    //   the client SDK; once committedMetadataReady_ becomes true the retry lands
    //   on the steady-state path where acks=All is honored with real majority-
    //   commit semantics (deferred-ack queue + ISR HW gate).
    //
    //   acks=Leader and acks=0 in the bootstrap window are fine to serve locally
    //   — their contract explicitly does NOT guarantee majority durability.
    //
    //   The log is untouched (no append, no LEO advance) for the retriable-error
    //   path, preserving the "NEVER falsely ack" invariant.
    const bool acksAll = (msg.acks == 1);
    if (acksAll && !committedMetadataReady_) {
        // Bootstrap window: acks=All cannot be served with majority durability yet.
        // Return NotLeader (retriable) so the client retries after bootstrap.
        spdlog::debug("ClientGateway: acks=All produce rejected (bootstrap: committedMetadataReady_=false) "
                      "sessionId={} topicId={} partition={} — returning NotLeader (retriable)",
                      msg.sessionId, msg.topicId, partition);
        pc::ErrorResponse errResp;
        errResp.sessionId     = msg.sessionId;
        errResp.correlationId = msg.correlationId;
        errResp.code          = static_cast<int16_t>(pc::ErrorCode::NotLeader);
        const std::size_t needed = pc::MessageHeader::ENCODED_SIZE +
                                   pc::ErrorResponse::BODY_FIXED + 2;
        if (needed <= scratch_.size()) {
            const std::size_t encoded = errResp.encode(scratch_.data());
            // G0.1: reliable delivery (retry on back-pressure).
            enqueueOrBestEffort_(*session, scratch_.data(), encoded);
        }
        return;  // log untouched — no append, no LEO advance
    }

    // E2 inc2a: When a WorkerPool is wired, route the append to the owning worker
    // via the SPSC ingress ring.  The worker executes appendNoSync +
    // onLeaderLogAppended; the response (or pending-ack) is completed in
    // drainWorkerReturns_() at the top of the NEXT doWork() cycle.
    //
    // When no WorkerPool is wired (null), fall through to the inline path below.
    // Route the produce to the OWNING WORKER via the ingress ring whenever a
    // WorkerPool exists AND either:
    //   - per-worker ingress is off (legacy: workers own all partitions), OR
    //   - per-worker ingress is on AND this is a USER partition (topicId >= 0),
    //     which is worker-owned.
    //
    // #13 (HW-visibility race / read tail-stall) FIX: when per-worker ingress is
    // active, a USER-partition log is worker-owned — the worker is its SOLE
    // appender AND the sole writer of its replica high-watermark (via
    // onLeaderLogAppended), AND the sole reader of that HW (pushOwnedSubscriptions_).
    // The gateway must therefore NEVER inline-append to a worker-owned log: doing so
    // wrote the HW on the gateway thread while the worker read it on its own thread
    // with no synchronization — on a weak memory model the worker could read a stale
    // HW that never included the final produce, so the last records were never pushed
    // (consumer stalls on the tail).  Routing to the worker keeps append + HW write +
    // HW read all on the one worker thread — shared-nothing, race-free.
    //
    // No double-processing: a single produce reaches EITHER the worker's own Aeron
    // ingress stream (client routed per-partition) OR the gateway's stream (client
    // fell back) — never both — so routing gateway-received produce to the worker
    // appends it exactly once.  Internal partitions (topicId < 0) are gateway-owned
    // and stay on the inline path below.
    const bool routeToWorker = routesToWorker_(msg.topicId);   // F15 K3: shared with markers
    if (routeToWorker) {
        // ---------------------------------------------------------------------------
        // WORKER PATH: Parse + validate the batch inline (same logic as before) so
        // that the WorkerOp carries the pre-validated fields (lastOffsetDelta,
        // recordCount) and ISR gate is enforced on the gateway thread.
        //
        // M1 fix: derive lastOffset from the CRC-covered lastOffsetDelta field
        // (kLastOffsetDeltaOffset=23, LE32) rather than trusting client recordCount.
        //
        // The base offset is NOT assigned here — the worker's appendNoSyncRestamp
        // assigns it authoritatively when it appends (worker is the single writer).
        // We validate the batch on the READ-ONLY fragment and forward it by view.

        // Read lastOffsetDelta (LE32 at kLastOffsetDeltaOffset=23).
        int32_t lastOffsetDelta = 0;
        {
            const uint8_t* p = rbData + rune::protocol::batch::kLastOffsetDeltaOffset;
            lastOffsetDelta = static_cast<int32_t>(
                static_cast<uint32_t>(p[0]) |
                (static_cast<uint32_t>(p[1]) << 8) |
                (static_cast<uint32_t>(p[2]) << 16) |
                (static_cast<uint32_t>(p[3]) << 24));
        }

        // Read recordCount (LE32 at kRecordCountOffset=57) for consistency validation only.
        int32_t recordCount = 0;
        {
            const uint8_t* p = rbData + rune::protocol::batch::kRecordCountOffset;
            recordCount = static_cast<int32_t>(
                static_cast<uint32_t>(p[0]) |
                (static_cast<uint32_t>(p[1]) << 8) |
                (static_cast<uint32_t>(p[2]) << 16) |
                (static_cast<uint32_t>(p[3]) << 24));
        }

        // lastOffsetDelta must be >= 0 (single-record batch: delta=0 → count=1).
        if (lastOffsetDelta < 0) {
            spdlog::debug("ClientGateway: Produce lastOffsetDelta={} < 0; rejected (Internal)",
                          lastOffsetDelta);
            sendResp(pc::ErrorCode::Internal, partition, 0);
            return;
        }

        // recordCount must equal lastOffsetDelta + 1.
        if (recordCount != lastOffsetDelta + 1) {
            spdlog::warn("ClientGateway: Produce recordCount={} != lastOffsetDelta+1={}; "
                         "batch internally inconsistent → rejected (Internal)",
                         recordCount, lastOffsetDelta + 1);
            sendResp(pc::ErrorCode::Internal, partition, 0);
            return;
        }

        // CRC validation: reject corrupt batches before they touch the log.
        {
            rune::protocol::RecordBatchReader reader(rbData, batchLen);
            if (!reader.ensureValid()) {
                spdlog::warn("ClientGateway: Produce CRC mismatch (corrupt batch) → rejected (Internal)");
                sendResp(pc::ErrorCode::Internal, partition, 0);
                return;
            }
        }

        // F15 S5 (§2.5, I6 / Q3): NO idempotent-producer gate here, in any mode.
        // The OWNING worker's psm_ is the single producer-state authority for
        // every partition it owns (validate → append → update in its produce
        // core, in program order with every marker's fence) — a gateway mirror
        // would be a second authority for one pid (a check-then-act race, and a
        // one-pass window for seq k+1 before seq k's result drained).

        // GR-5 (Task 10 round 5): NO min-ISR gate here.  The replica's ISR is
        // worker-owned state; reading replica->isr() on this control thread was a
        // data race (and could segfault while becomeLeader was assigning the ISR
        // manager).  The OWNING worker applies the gate (executeProduceWire_ /
        // the shard ingress path) and answers MinIsrViolation in the ProduceResult.

        // P1 (Task 10 round 4): a control marker for this partition is still
        // waiting for worker-ring space — no later batch may overtake it.
        if (!markerSendQueue_.empty() && markerQueuedFor_(msg.topicId, partition)) [[unlikely]] {
            sendResp(pc::ErrorCode::NotLeader, partition, 0);
            return;
        }

        // Build the op envelope.
        rune::node::WorkerOp op;
        op.type         = rune::node::WorkerOp::Type::Produce;
        op.topicId      = msg.topicId;
        op.partition    = partition;
        replica.stampRouting(op);
        op.logPtr       = log;
        op.sessionId    = msg.sessionId;
        op.correlationId = msg.correlationId;
        op.acks          = msg.acks;
        op.producerId    = msg.producerId;
        op.producerEpoch = msg.producerEpoch;
        op.baseSequence  = msg.baseSequence;
        op.lastOffsetDelta = lastOffsetDelta;
        op.recordCount   = recordCount;
        op.deferAck      = acksAll && static_cast<bool>(replica) && committedMetadataReady_;
        op.minIsr        = cachedMinIsr_;   // the admission policy; the worker gates on its own ISR
        op.enqueuedAtNs  = nowNs_;
        // E3 Task 5: plumb the topic's async-durability tier so the worker
        // knows whether to defer the ack until fsync completion (Strict=2).
        // Majority (1) is the default — keeps existing behavior unchanged.
        op.durabilityTier = replica.durabilityTier();

        // Capture batch attributes for LSO tracking (forwarded in return).
        constexpr std::size_t kAttrOffset =
            static_cast<std::size_t>(rune::protocol::batch::kAttributesOffset);
        if (batchLen >= kAttrOffset + 2u) {
            op.batchAttrs = static_cast<uint16_t>(
                static_cast<uint16_t>(rbData[kAttrOffset]) |
                (static_cast<uint16_t>(rbData[kAttrOffset + 1u]) << 8));
        }

        // Zero-copy: forward the batch by READ-ONLY view (aliases the Aeron
        // fragment, valid for the synchronous tryEnqueueOp call below).  putTail
        // copies it ONCE into the ring slot — no restampScratch_, no owning heap
        // vector.  The worker's appendNoSyncRestamp assigns + patches the base
        // offset (CRC-safe: base-offset field [0..7] precedes the CRC region).
        op.recordBatchView = msg.recordBatch;
        // F15 S5 fix round 1 (M-3): these exact bytes (rbData == msg.recordBatch,
        // unmodified since the ensureValid() above, copied once into the ring by
        // enqueueOp below) passed the CRC32C check — the worker may skip its
        // repeat pass.  INVARIANT: set ONLY here, only after ensureValid().
        op.crcValidated    = true;

        // Enqueue to the worker.  tryEnqueueOp returns false on back-pressure
        // (ring full, or the worker's SetOwnership still undelivered).  M5 (Task
        // 10 round 4): answer the RETRIABLE NotLeader — the client re-sends the
        // same batch (same producer sequence) — never Internal: nothing was
        // appended, so a retry is always safe.  (The client frame aliases the
        // Aeron fragment, so the gateway cannot hold it for its own retry.)
        const rune::node::EnqueueResult er =
            workerPool_->enqueueOp(msg.topicId, partition, std::move(op));
        if (er == rune::node::EnqueueResult::TooLarge) [[unlikely]] {
            // F18 (m-1): the batch can NEVER fit one worker-ingress frame — not
            // back-pressure.  NotLeader would make the client retry it forever;
            // answer the non-retriable MessageTooLarge instead (loud).  Unreachable
            // while maxProduceBatchBytes_ <= the intake limit.
            spdlog::error("ClientGateway: produce batch {} B for topicId={} partition={} exceeds "
                          "the worker ingress frame; MessageTooLarge",
                          batchLen, msg.topicId, partition);
            sendResp(pc::ErrorCode::MessageTooLarge, partition, 0);
        } else if (er != rune::node::EnqueueResult::Enqueued) {
            ++produceBackPressured_;
            if (produceBackPressured_ == 1 || (produceBackPressured_ & 1023u) == 0)
                spdlog::warn("ClientGateway: worker back-pressure for topicId={} partition={}; "
                             "ProduceResponse NotLeader (retriable; total={})",
                             msg.topicId, partition, produceBackPressured_);
            sendResp(pc::ErrorCode::NotLeader, partition, 0);
        }
        // Response (or pending-ack) will be processed in drainWorkerReturns_().
        return;
    }

    // ---------------------------------------------------------------------------
    // INLINE path (pre-E2 / no WorkerPool):
    //
    // E5.1.2b: Delegate to the shared handleProduceBatch helper.  The helper
    // performs: base-offset restamp in restampScratch_, lastOffsetDelta/recordCount
    // parse + consistency check, CRC validation, idempotent-dedup (PSM validate +
    // update), min-ISR gate (acks=All + isrSize vs minIsr), appendNoSync, and
    // Strict-tier sync().  Returns a ProduceOutcome.
    //
    // The gateway retains ownership of (this NO-WORKER-POOL inline path only):
    // LSO tracking (getTxnState — F15 S4: never used with a worker pool, where
    // the owning worker holds the read_committed state), dirty-log set,
    // replica->onLeaderLogAppended(), and deferred-ack queue management.
    //
    // Durability contract — acks=Leader (acks=1):
    //   Records are written to the OS page cache and visible to Log::read()
    //   immediately.  They are NOT fsync-durable until flushDirtyLogs() runs
    //   at the end of this doWork() cycle.  A leader crash in the window between
    //   appendNoSync() and sync() may lose the most-recent un-flushed acked
    //   records.  True crash-durability requires acks=All + ISR replication
    //   (GR-5/Milestone C).
    // F15 §3.1: the inline path runs on the replica's OWNER thread (no worker
    // pool, or a gateway-owned internal partition) — its live state is ours.
    rune::replication::PartitionReplica* const ownedReplica = replica.controlOwned();
    if (ownedReplica && committedMetadataReady_ &&
        ownedReplica->role() != rune::replication::Role::Leader) {
        spdlog::debug("ClientGateway: Produce per-partition NotLeader (inline owner) "
                      "(topicId={} partition={}) sessionId={}",
                      msg.topicId, partition, msg.sessionId);
        sendErrorResponse_(*session, msg.sessionId, msg.correlationId, pc::ErrorCode::NotLeader);
        return;  // engine untouched — no append
    }
    const uint8_t effectiveTier = replica.durabilityTier();
    // ISR size for min-ISR gate: only applies in steady state (committedMetadataReady_
    // true) and when a replica is wired.  Zero causes the helper to skip the gate
    // (passthrough — single-node / bootstrap).
    // Note: bootstrap-window acks=All is already rejected above before we reach here.
    const int isrSizeForHelper = (acksAll && ownedReplica && committedMetadataReady_)
                                 ? static_cast<int>(ownedReplica->isr().size()) : 0;

    // PSM <-> log consistency (findings 1 + 2): an idempotent batch is validated
    // only against a producer state PROVEN consistent with this log — rebuilt
    // here (cold) when the log / leadership it was built for changed.  An
    // unprovable state never answers OOS (the client's "NOT appended" proof)
    // nor accepts a possibly-stored batch: retriable NotLeader while this is not
    // a complete leader, Internal (outcome unknown) if the rebuild failed.
    if (msg.producerId >= 0) {
        const InlinePsmGate gate =
            proveInlinePsm_(msg.topicId, partition, *log, ownedReplica, replica.incarnation());
        if (gate != InlinePsmGate::Proven) [[unlikely]] {
            sendResp(gate == InlinePsmGate::NotLeader ? pc::ErrorCode::NotLeader : pc::ErrorCode::Internal,
                     partition, 0);
            return;   // log untouched
        }
    }

    const auto outcome = rune::node::handleProduceBatch(
        rbData, batchLen,
        msg.topicId, partition,
        msg.producerId, msg.producerEpoch, msg.baseSequence,
        *log, producerStateManager_,
        acksAll,
        isrSizeForHelper,
        cachedMinIsr_,
        effectiveTier);

    // Handle non-OK / duplicate outcomes (no append occurred).
    if (!outcome.isOK()) {
        if (outcome.identityMismatch) [[unlikely]] {
            // EOS-DUP defense: frame identity != CRC-covered batch header.
            if (identityMismatchRejects_++ % 1024 == 0)
                spdlog::warn("ClientGateway: produce rejected — frame producer identity (pid={} epoch={} seq={}) "
                             "!= RecordBatch header; topicId={} partition={} (SchemaValidationFailed, rejects={})",
                             msg.producerId, msg.producerEpoch, msg.baseSequence,
                             msg.topicId, partition, identityMismatchRejects_);
        }
        sendResp(outcome.error, partition, 0);
        return;
    }
    // Dedup path (log untouched).  F15 S5 parity on the inline path (Kafka
    // DelayedProduce): a duplicate is answered only under its original's ack
    // condition — acks=all parks below on the HW past outcome.lastOffset (the
    // original's range); otherwise it is answered at once.
    // The producer state is proven consistent with this log (the gate above),
    // so a KNOWN-OLD duplicate (baseOffset -1: entirely <= lastSequence, out of
    // the window) is stored: OK with an unknown offset (Kafka
    // DUPLICATE_SEQUENCE_NUMBER parity, as on the worker path) under its ack
    // condition (acks=all parks on outcome.lastOffset below).
    if (outcome.isDuplicate && !(acksAll && ownedReplica && committedMetadataReady_)) {
        sendResp(pc::ErrorCode::OK, partition, outcome.baseOffset);
        return;
    }

    if (!outcome.isDuplicate) {
    // Successful append — apply post-append gateway-owned side-effects.

    // TX-4 Task 9: Feed LsoTracker for transactional data batches.
    //
    // A transactional batch has producerId >= 0 AND the TRANSACTIONAL bit set in
    // the batch attributes field (bit 4, kAttributesOffset=21).  The helper
    // returns batchAttrs so we can check this bit without re-reading the scratch.
    if (msg.producerId >= 0) {
        constexpr uint16_t kTransactionalBit = 0x10u;
        if (outcome.batchAttrs & kTransactionalBit) {
            getTxnState(msg.topicId, partition)
                .lsoTracker.onProduceBatch(msg.producerId, outcome.baseOffset);
        }
    }

    // E3 Task 9: Strict-tier sync() was already done by the helper (outcome.syncDone).
    // For any other tier, mark the Log dirty so flushDirtyLogs() group-commits it.
    //
    // This path is only reached when workerPool_ is null — test fixtures that bypass
    // BrokerNode/WorkerPool construction or (theoretically) workerCount==0.
    // Internal partitions (topicId < 0) always use durabilityTier=1 (Majority).
    if (outcome.syncDone) {
        // Strict-tier: log was sync()'d by the helper; remove from dirtyLogs_ so
        // flushDirtyLogs() doesn't double-sync it at end of cycle.
        dirtyLogs_.erase(log);
    } else {
        // Non-strict: mark dirty for group-commit at end of cycle.
        dirtyLogs_.insert(log);
    }

    // GR-4: Reconcile the leader replica's tracked LEO/HW (Mode A seam).
    //
    // The gateway appended DIRECTLY to the Log (bypassing PartitionReplica::append)
    // so the leader replica's ReplicaLeaderState still holds the pre-append LEO.
    // onLeaderLogAppended() reads log_->logEndOffset(), refreshes per-follower LEO,
    // and recomputes HW — this is EXACTLY the Mode-A seam described in the spec.
    //
    // No double-append: the Log was written once by the helper's appendNoSync().
    // onLeaderLogAppended() only reads the Log's LEO and updates in-memory state.
    if (ownedReplica) {
        ownedReplica->onLeaderLogAppended();
    }
    } // !outcome.isDuplicate

    // 6. Reply ProduceResponse or enqueue deferred ack.
    //
    // acks=Leader / None: ack immediately after page-cache write.
    // acks=All:           enqueue a PendingAck; drainPendingAcks() will send the
    //                     response once replica->highWatermark() >= lastOffset.
    //
    // acks=All during the bootstrap window (!committedMetadataReady_) is rejected
    // ABOVE before reaching here; by this point either acks != All or
    // committedMetadataReady_ is true.
    const bool deferAck = acksAll && ownedReplica && committedMetadataReady_;
    if (!deferAck) {
        // Immediate ack: acks=Leader or no replica wired (passthrough).
        sendResp(pc::ErrorCode::OK, partition, outcome.baseOffset);
    } else {
        // acks=All deferred ack path (steady state: committedMetadataReady_ true).
        // Cap check: if the queue is full, reject rather than silently overflowing.
        if (pendingAcks_.size() >= kMaxPendingAcks) {
            spdlog::warn("ClientGateway: pendingAcks_ full ({} entries); acks=All produce rejected "
                         "with MinIsrViolation sessionId={} topicId={} partition={}",
                         kMaxPendingAcks, msg.sessionId, msg.topicId, partition);
            sendResp(pc::ErrorCode::MinIsrViolation, partition, 0);
            return;
        }

        PendingAck entry;
        entry.sessionId     = msg.sessionId;
        entry.correlationId = msg.correlationId;
        entry.partition     = partition;
        entry.baseOffset    = outcome.baseOffset;
        entry.lastOffset    = outcome.lastOffset;
        entry.enqueuedAtNs  = nowNs_;
        entry.topicId       = msg.topicId;
        entry.incarnation   = replica.incarnation();
        entry.logGen        = ownedReplica->logGeneration();   // deferAck ⇒ ownedReplica

        pendingAcks_.push_back(entry);  // no heap alloc (capacity pre-reserved)

        spdlog::debug("ClientGateway: acks=All produce enqueued pending-ack "
                      "sessionId={} partition={} baseOffset={} lastOffset={}",
                      msg.sessionId, partition, outcome.baseOffset, outcome.lastOffset);
    }
}

// ---------------------------------------------------------------------------
// handleSubscribe (GB-3, tid18) — register a per-session subscription.
//
// Flow:
//   1. Decode Subscribe — drop on truncation.
//   2. Require known session — drop on unknown session (no egress to reply on).
//   GB-4: Check leadership BEFORE touching the engine.
//     - NOT leader + known leaderClientEndpoint → ErrorResponse{Redirect, endpoint},
//       engine untouched, no subscription registered.
//     - NOT leader + unknown endpoint → ErrorResponse{NotLeader}, engine untouched.
//     - Leader → serve (existing path).
//   3. Resolve partition: explicit (partitionKnown=1) or partitionForKey().
//   4. Validate topic + partition → SubscribeResponse{error} on failure.
//   5. Clamp fromOffset:
//        fromOffset < 0  → treat as "from Latest" (LEO).
//        fromOffset > LEO → clamp to LEO.
//        0 ≤ fromOffset ≤ LEO → use as-is.
//   6. Allocate subscriptionId; register Subscription; reply SubscribeResponse{OK}.
//
// No heap allocation on the hot path (scratch_ is pre-allocated).
// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// read-rework T3/T4 (I3b): route subscription lifecycle to the owning worker.
// ---------------------------------------------------------------------------
// enqueueSubscribeOp_ — build a Subscribe WorkerOp with the resolved per-consumer
// egress pub pointer and route it to the owning worker.  egressPub may be null
// (no session / test fixture / resolve timeout) → the worker falls back to MDC.
void ClientGateway::enqueueSubscribeOp_(const BufferedSubscribe& s, void* egressPub)
{
    rune::storage::Log* log = topicRegistry_ ? topicRegistry_->log(s.topicId, s.partition) : nullptr;
    const rune::replication::PartitionReplica* rep =
        replicaRegistry_ ? replicaRegistry_->lookup(s.topicId, s.partition) : nullptr;

    rune::node::WorkerOp op;
    op.type                 = rune::node::WorkerOp::Type::Subscribe;
    op.subscriptionId       = s.subId;
    op.sessionId            = s.sessionId;
    op.topicId              = s.topicId;
    op.partition            = s.partition;
    op.nextOffset           = s.nextOffset;
    op.isolation            = s.isolation;
    op.isGroup              = s.isGroup;
    op.groupCarrierReceived = s.groupCarrierReceived;
    op.logPtr               = log;
    op.replica              = const_cast<rune::replication::PartitionReplica*>(rep);
    op.egressPub            = egressPub;

    // M5: never dropped — a refused Subscribe (or one behind a queued lifecycle
    // op for the same worker) is queued and retried every doWork, in order.
    const auto w = static_cast<std::size_t>(workerPool_->ownerWorkerId(s.topicId, s.partition));
    const bool behind = w < pendingSubOps_.size() && !pendingSubOps_[w].empty();
    if (behind || !workerPool_->tryEnqueueOp(s.topicId, s.partition, std::move(op))) {
        PendingSubOp p;
        p.sub = s;
        p.mdc = egressPub == nullptr;
        queueSubOp_(std::move(p));
    }
}

void ClientGateway::queueSubOp_(PendingSubOp p)
{
    const auto w = static_cast<std::size_t>(workerPool_->ownerWorkerId(p.sub.topicId, p.sub.partition));
    if (pendingSubOps_.size() <= w) pendingSubOps_.resize(static_cast<std::size_t>(workerPool_->workerCount()));
    pendingSubOps_[w].push_back(std::move(p));
    const std::size_t n = pendingSubOpsForTest();
    if (n == 1 || (n & (n - 1)) == 0)   // rate limit: first + each power of two
        spdlog::warn("ClientGateway: {} subscription op(s) waiting for worker ring space — retried every cycle", n);
}

bool ClientGateway::trySubOp_(const PendingSubOp& p)
{
    rune::node::WorkerOp op;
    if (p.unsubscribe) {
        op.type           = rune::node::WorkerOp::Type::Unsubscribe;
        op.subscriptionId = p.sub.subId;
        op.topicId        = p.sub.topicId;
        op.partition      = p.sub.partition;
        return workerPool_->tryEnqueueOp(p.sub.topicId, p.sub.partition, std::move(op));
    }
    // Re-resolve the egress publication NOW (never a pointer held across cycles).
    void* egressPub = nullptr;
    if (!p.mdc) {
        rune::gateway::Session* sess = sessions_.find(p.sub.sessionId);
        if (!sess) return true;   // session gone: the Subscribe is moot (dropped)
        const int32_t wid = workerPool_->ownerWorkerId(p.sub.topicId, p.sub.partition);
        auto it = sess->recordEgressByWorker.find(wid);
        if (it == sess->recordEgressByWorker.end() || !it->second) return true;   // moot
        egressPub = it->second.get();
    }
    const auto& s = p.sub;
    op.type                 = rune::node::WorkerOp::Type::Subscribe;
    op.subscriptionId       = s.subId;
    op.sessionId            = s.sessionId;
    op.topicId              = s.topicId;
    op.partition            = s.partition;
    op.nextOffset           = s.nextOffset;
    op.isolation            = s.isolation;
    op.isGroup              = s.isGroup;
    op.groupCarrierReceived = s.groupCarrierReceived;
    op.logPtr               = topicRegistry_ ? topicRegistry_->log(s.topicId, s.partition) : nullptr;
    op.egressPub            = egressPub;
    return workerPool_->tryEnqueueOp(s.topicId, s.partition, std::move(op));
}

int ClientGateway::pumpSubOps_()
{
    int done = 0;
    for (auto& q : pendingSubOps_) {
        while (!q.empty()) {
            if (!trySubOp_(q.front())) break;   // this worker's ring still full; others proceed
            q.pop_front();
            ++done;
        }
    }
    return done;
}

void ClientGateway::forwardSubscribeToWorker_(int64_t subId, int64_t sessionId,
                                              int32_t topicId, int32_t partition,
                                              int64_t nextOffset, uint8_t isolation,
                                              bool isGroup, bool groupCarrierReceived)
{
    const BufferedSubscribe s{subId, sessionId, topicId, partition,
                              nextOffset, isolation, isGroup, groupCarrierReceived};

    rune::gateway::Session* sess = sessions_.find(sessionId);

    // Hybrid egress: forward with this consumer's per-(session,worker) UNICAST
    // Records pub (independent per-consumer offsets, not MDC broadcast).  ONE pub
    // per owning worker — an ExclusivePublication is single-writer, so partitions
    // owned by different workers MUST use different pubs.
    //   - No session / no egress endpoint (test fixtures) → forward with MDC (null).
    //   - Pub for this worker already resolved on the Session → forward immediately.
    //   - Otherwise → kick off a NON-BLOCKING resolve and DEFER the Subscribe until
    //     the pub is ready (drained by pollPendingRecordEgress_).  Never blocks the
    //     control thread.
    if (!sess || sess->egressChannel.empty() || !aeron_ || !workerPool_) {
        enqueueSubscribeOp_(s, nullptr);
        return;
    }
    const int32_t wid = workerPool_->ownerWorkerId(topicId, partition);
    if (auto it = sess->recordEgressByWorker.find(wid);
        it != sess->recordEgressByWorker.end() && it->second) {
        enqueueSubscribeOp_(s, it->second.get());
        return;
    }

    // Ensure a pending resolve is in flight for (session, worker), then buffer the
    // subscribe until it resolves.  Each worker gets its OWN ExclusivePublication
    // to the client's egress endpoint (distinct Aeron session → distinct image;
    // single-writer preserved).
    auto& pend = pendingRecordEgress_[{sessionId, wid}];
    if (pend.regId < 0) {
        try {
            pend.regId         = aeron_->addExclusivePublication(sess->egressChannel,
                                                                 sess->egressStreamId);
            pend.requestedAtNs = nowNs_;
        } catch (...) {
            spdlog::warn("ClientGateway: record-egress pub create threw for session {} "
                         "worker {} (channel '{}'); Subscribe uses MDC fallback",
                         sessionId, wid, sess->egressChannel);
            pendingRecordEgress_.erase({sessionId, wid});
            enqueueSubscribeOp_(s, nullptr);
            return;
        }
    }
    pend.waiters.push_back(s);
}

// pollPendingRecordEgress_ — resolve in-flight per-session record-egress pubs
// (non-blocking) and drain each session's buffered Subscribes once its pub is
// ready.  Called from doWork().  A resolve that exceeds the connect timeout drops
// to the MDC fallback so a subscribe is never wedged forever.
void ClientGateway::pollPendingRecordEgress_(int64_t nowNs)
{
    if (pendingRecordEgress_.empty()) return;
    for (auto it = pendingRecordEgress_.begin(); it != pendingRecordEgress_.end(); ) {
        const int64_t sessionId = it->first.first;
        const int32_t workerId  = it->first.second;
        PendingRecordEgress& pend = it->second;

        std::shared_ptr<aeron::ExclusivePublication> pub;
        try {
            if (pend.regId >= 0) pub = aeron_->findExclusivePublication(pend.regId);
        } catch (...) {
            pub = nullptr;  // ERRORED — treat as failed resolve below
            pend.regId = -1;
        }

        const bool timedOut =
            (nowNs - pend.requestedAtNs) > connectResolveTimeoutNs_;

        if (pub) {
            // Resolved: cache on the Session under its worker id and drain waiters.
            void* raw = pub.get();
            if (rune::gateway::Session* sess = sessions_.find(sessionId)) {
                sess->recordEgressByWorker[workerId] = std::move(pub);
                raw = sess->recordEgressByWorker[workerId].get();
            }
            for (const auto& w : pend.waiters) enqueueSubscribeOp_(w, raw);
            it = pendingRecordEgress_.erase(it);
            continue;
        }
        if (timedOut || pend.regId < 0) {
            spdlog::warn("ClientGateway: record-egress pub for session {} worker {} did not "
                         "resolve ({} waiters) — falling back to MDC",
                         sessionId, workerId, pend.waiters.size());
            for (const auto& w : pend.waiters) enqueueSubscribeOp_(w, nullptr);
            it = pendingRecordEgress_.erase(it);
            continue;
        }
        ++it;  // still resolving — retry next cycle
    }
}

void ClientGateway::forwardUnsubscribeToWorker_(int64_t subId, int32_t topicId, int32_t partition)
{
    // M5: never dropped on a full ring — queued (behind any queued Subscribe of
    // the same worker, so the pair keeps its order) and retried every doWork.
    PendingSubOp p;
    p.unsubscribe   = true;
    p.sub.subId     = subId;
    p.sub.topicId   = topicId;
    p.sub.partition = partition;
    const auto w = static_cast<std::size_t>(workerPool_->ownerWorkerId(topicId, partition));
    const bool behind = w < pendingSubOps_.size() && !pendingSubOps_[w].empty();
    if (behind || !trySubOp_(p)) queueSubOp_(std::move(p));
}

void ClientGateway::handleSubscribe(const uint8_t* buf, std::size_t len)
{
    // 1. Decode.
    auto msgOpt = pc::Subscribe::decode(buf, len);
    if (!msgOpt) {
        spdlog::debug("ClientGateway: Subscribe decode failed (truncated?) — dropped");
        return;
    }
    const pc::Subscribe& msg = *msgOpt;

    // 2. Require known session.
    Session* session = sessions_.find(msg.sessionId);
    if (!session) {
        spdlog::debug("ClientGateway: Subscribe for unknown sessionId={} — dropped", msg.sessionId);
        return;
    }

    // GB-4 / MD1-9: Leadership check — data ops MUST reach the partition leader.
    //
    // Helper: send redirect/notleader error response for subscribe.
    auto sendSubRedirect = [&](const std::string& endpoint, int64_t corrId) {
        pc::ErrorResponse errResp;
        errResp.sessionId     = msg.sessionId;
        errResp.correlationId = corrId;
        if (!endpoint.empty()) {
            errResp.code    = static_cast<int16_t>(pc::ErrorCode::Redirect);
            errResp.message = endpoint;
            spdlog::debug("ClientGateway: Subscribe per-partition redirect sessionId={} → {}",
                          msg.sessionId, endpoint);
        } else {
            errResp.code    = static_cast<int16_t>(pc::ErrorCode::NotLeader);
            spdlog::debug("ClientGateway: Subscribe per-partition NotLeader (endpoint unknown) sessionId={}",
                          msg.sessionId);
        }
        const std::size_t needed = pc::MessageHeader::ENCODED_SIZE +
                                   pc::ErrorResponse::BODY_FIXED + 2 +
                                   errResp.message.size();
        if (needed <= scratch_.size()) {
            const std::size_t encoded = errResp.encode(scratch_.data());
            sendTo(*session, scratch_.data(), encoded);
        }
    };

    // Cluster-level fallback: only when routingImage_ is null.
    if (!routingImage_ && leaderView_) {
        const LeaderView lv = leaderView_();
        if (!lv.isLeader) {
            sendSubRedirect(lv.leaderClientEndpoint, msg.correlationId);
            return;  // engine untouched, no subscription registered
        }
    }

    // Helper: send SubscribeResponse on the session egress.
    auto sendResp = [&](pc::ErrorCode ec, int32_t partition, int64_t subId) {
        pc::SubscribeResponse resp;
        resp.sessionId      = msg.sessionId;
        resp.correlationId  = msg.correlationId;
        resp.errorCode      = static_cast<int16_t>(ec);
        resp.partition      = partition;
        resp.subscriptionId = subId;
        static_assert(pc::SubscribeResponse::ENCODED_SIZE <= kScratchSize,
                      "scratch too small for SubscribeResponse");
        const std::size_t encoded = resp.encode(scratch_.data());
        sendTo(*session, scratch_.data(), encoded);
    };

    // 3. Resolve partition.
    int32_t partition = msg.partition;
    if (msg.partitionKnown == 0) {
        if (!topicRegistry_) {
            spdlog::warn("ClientGateway: Subscribe key-partition but no TopicRegistry — UnknownTopic");
            sendResp(pc::ErrorCode::UnknownTopic, 0, 0);
            return;
        }
        partition = topicRegistry_->partitionForKey(msg.topicId, msg.key);
        if (partition < 0) {
            spdlog::debug("ClientGateway: Subscribe partitionForKey → unknown topicId={}", msg.topicId);
            sendResp(pc::ErrorCode::UnknownTopic, 0, 0);
            return;
        }
    }

    // 3b. MD1 Task 9: Per-partition routing image check.
    // Mirrors the produce path: capture-once discipline (routingImage_.get()).
    //
    // BOOTSTRAP FIX — defense-in-depth (MD1-13): same as handleProduce.
    // When the image is set but this partition is NO_LEADER (not yet assigned),
    // fall back to Path B (cluster-leader) rather than returning NotLeader.
    if (routingImage_) {
        const rune::metadata::RoutingImage* img = routingImage_.get();
        const int32_t gpid = rune::metadata::gpidOf(msg.topicId, partition);
        const auto& entry = img->lookup(gpid);

        if (entry.leaderNodeId == rune::metadata::RoutingImage::NO_LEADER_NODE_ID) {
            // Partition not yet assigned — fall back to cluster-leader path.
            spdlog::debug("ClientGateway: Subscribe partition not in routing image "
                          "(topicId={} part={}) — falling back to cluster-leader path, sessionId={}",
                          msg.topicId, partition, msg.sessionId);
            if (leaderView_) {
                const LeaderView lv = leaderView_();
                if (!lv.isLeader) {
                    sendSubRedirect(lv.leaderClientEndpoint, msg.correlationId);
                    return;  // engine untouched, no subscription registered
                }
            }
            // leaderView_ says we ARE the leader (or is null) — serve locally (fall through).
        } else if (entry.leaderNodeId != thisNodeId_) {
            const std::string& redirectEndpoint = [&]() -> const std::string& {
                auto it = nodeEndpointMap_.find(entry.leaderNodeId);
                if (it != nodeEndpointMap_.end()) return it->second;
                static const std::string kEmpty;
                return kEmpty;
            }();
            spdlog::debug("ClientGateway: Subscribe per-partition redirect "
                          "(topicId={} part={} leader={} epoch={}) sessionId={} → {}",
                          msg.topicId, partition, entry.leaderNodeId, entry.leaderEpoch,
                          msg.sessionId, redirectEndpoint);
            sendSubRedirect(redirectEndpoint, msg.correlationId);
            return;  // engine untouched, no subscription registered
        }
        // entry.leaderNodeId == thisNodeId_ → serve locally (fall through).
    }

    // 4. Validate topic + partition.
    if (!topicRegistry_) {
        spdlog::warn("ClientGateway: Subscribe but no TopicRegistry — UnknownTopic");
        sendResp(pc::ErrorCode::UnknownTopic, partition, 0);
        return;
    }

    const rune::node::TopicSpec* spec = topicRegistry_->topic(msg.topicId);
    if (!spec) {
        spdlog::debug("ClientGateway: Subscribe unknown topicId={}", msg.topicId);
        sendResp(pc::ErrorCode::UnknownTopic, partition, 0);
        return;
    }

    rune::storage::Log* log = topicRegistry_->log(msg.topicId, partition);
    if (!log) {
        spdlog::debug("ClientGateway: Subscribe bad partition={} for topicId={}", partition, msg.topicId);
        sendResp(pc::ErrorCode::UnknownPartition, partition, 0);
        return;
    }

    // 5. Clamp fromOffset to [0, HW] (GR-6) — or [0, LEO] for non-replicated topics.
    //    fromOffset < 0 → "from Latest"; fromOffset > clamp-limit → clamped.
    //    Convention: negative fromOffset is the "Latest" sentinel documented in the brief.
    //
    // GB-5 Fix #5 — Sub-batch over-delivery (Kafka-consistent whole-batch semantics):
    //    If fromOffset lands mid-batch (i.e., between the batch's baseOffset and its
    //    last record offset), pushSubscriptions() will deliver the ENTIRE containing
    //    batch starting at baseOffset — not just the records from fromOffset onward.
    //    This is intentional (Kafka-compatible whole-batch delivery): the broker never
    //    splits batches, so the client must filter sub-batch records client-side.
    //    Example: batch [baseOffset=0, recordCount=5], fromOffset=3 → the whole batch
    //    (offsets 0..4) is delivered; client drops records 0..2 and consumes 3..4.
    // CB-5: honour the client's From sentinels distinctly (see client
    // From::toWire()):  Earliest = -2 → offset 0 (deliver the whole log);
    // Latest = -1 → HW (only new committed records).  Previously ALL negatives mapped
    // to LEO, which silently dropped Earliest subscriptions (0/N delivered).
    //
    // GR-6: "Latest" and "beyond-clamp" both use HW (not LEO) when a replica is
    // wired.  A subscriber starting at "Latest" should NOT sit above HW — that
    // would mean it would only receive new records AFTER HW advances past its
    // starting point, which is correct behavior since it should only see committed
    // data.  Clamping to HW also prevents the caller from providing a fromOffset
    // above HW (e.g. from a stale client cache reflecting LEO of a previous leader
    // that lost uncommitted records on failover — clamping to HW avoids a permanent
    // "nothing to deliver" stall on a reconnecting subscriber).
    static constexpr int64_t kFromEarliest = -2;
    static constexpr int64_t kFromLatest   = -1;
    const int64_t leo = log->logEndOffset();
    // GR-6: use HW as the clamp limit when a replica is available.
    // For non-replicated / single-node topics, HW == LEO — no behavioral change.
    int64_t clampLimit;
    if (replicaRegistry_) {
        const rune::replication::PartitionReplica* replica =
            replicaRegistry_->lookup(msg.topicId, partition);
        clampLimit = replica ? replica->gatewayHighWatermark() : leo;
    } else {
        clampLimit = leo;
    }
    // CP-5.2 FIX (Earliest-below-logStart starvation): the oldest offset that is
    // still on disk.  Retention/compaction can delete the earliest segment(s),
    // advancing the first segment base past 0.  Log::read(offset) for any offset
    // BELOW the first segment base returns {} forever (upper_bound lands on
    // begin()), so an Earliest subscriber pinned to 0 gets 0 records for the life
    // of the subscription.  Resolve Earliest — and clamp any explicit fromOffset
    // that points into a deleted region — UP to the live log start instead.
    const int64_t logStart = log->logStartOffset();
    int64_t nextOffset;
    if (msg.fromOffset == kFromEarliest) {
        // Earliest → the oldest offset still readable (NOT hardcoded 0).
        nextOffset = logStart;
    } else if (msg.fromOffset < 0) {
        // Latest (-1) or any other negative sentinel → subscribe from HW (GR-6).
        (void)kFromLatest;
        nextOffset = clampLimit;
    } else if (msg.fromOffset > clampLimit) {
        // Beyond HW → clamp to HW (GR-6: don't skip ahead of committed data).
        nextOffset = clampLimit;
    } else if (msg.fromOffset < logStart) {
        // Explicit offset points into a retention/compaction-deleted region →
        // clamp up to the oldest surviving offset (avoids the same 0-delivered
        // starvation for a stale client cursor after aggressive retention).
        nextOffset = logStart;
    } else {
        nextOffset = msg.fromOffset;
    }

    // 5b. Group-consumer isolation carrier (TX-5/TX-6):
    //     A group member conveys its isolation level via a Subscribe frame AFTER
    //     the SyncGroup assignment has already created the internal group
    //     subscription (isGroup=true) through addGroupSubscription().  Treat this
    //     Subscribe as an UPDATE of that existing group subscription's isolation
    //     rather than a second, independent push subscription.
    //
    //     Without this, the same (session, topicId, partition) would carry TWO
    //     subscriptions — the group one (created read_uncommitted by
    //     GroupCoordinationService::handleSyncGroup) and this per-session one —
    //     both driven by pushSubscriptions().  That produced (a) 2× delivery of
    //     every record and (b) an isolation leak: the group subscription stayed
    //     read_uncommitted and delivered aborted/open transactional records even
    //     when the client asked for read_committed.  We must NOT reset the group
    //     subscription's nextOffset (seeded from the committed group offset) — we
    //     only carry the isolation across.
    {
        const auto memIt = groupMemberSubs_.find(msg.sessionId);
        if (memIt != groupMemberSubs_.end()) {
            for (const int64_t existingSubId : memIt->second) {
                auto subIt = subscriptions_.find(existingSubId);
                if (subIt == subscriptions_.end()) continue;
                Subscription& gsub = subIt->second;
                if (gsub.isGroup && gsub.topicId == msg.topicId &&
                    gsub.partition == partition) {
                    // TX-6b: Mark carrier received and update isolation.
                    // The push loop stalls pre-carrier group subs at any transactional
                    // batch that would be abort-filtered (so nextOffset has NOT advanced
                    // past aborted records yet).  Marking groupCarrierReceived=true here
                    // unblocks the push loop on the next duty cycle with the correct
                    // isolation level: isolation=0 will deliver aborted records;
                    // isolation=1 will skip them — both correctly, without duplicates or
                    // permanent record loss.
                    gsub.isolation            = msg.isolation;
                    gsub.groupCarrierReceived = true;
                    gsub.parkedAtHw           = -1;   // F15 R-3: a new isolation → read again
                    spdlog::debug("ClientGateway: Subscribe carries isolation={} to group "
                                  "sub sessionId={} subId={} topic={} partition={} "
                                  "(no duplicate subscription)",
                                  msg.isolation, msg.sessionId, existingSubId,
                                  msg.topicId, partition);
                    sendResp(pc::ErrorCode::OK, partition, existingSubId);
                    return;
                }
            }
        }
    }

    // 6. Allocate subscriptionId and register subscription.
    const int64_t subId = ++nextSubscriptionId_;

    Subscription sub;
    sub.subscriptionId = subId;
    sub.sessionId      = msg.sessionId;
    sub.topicId        = msg.topicId;
    sub.partition      = partition;
    sub.nextOffset     = nextOffset;
    sub.isolation      = msg.isolation;

    subscriptions_.emplace(subId, std::move(sub));

    // read-rework T3/T4 (I3b): when per-worker egress is active, route the push to
    // the OWNING worker — it owns the cursor and pushes on its own dynamic-MDC
    // egress.  User partitions only (internal topicId < 0 stay gateway-pushed).
    // The gateway keeps its subscriptions_ record for setup/teardown; its
    // pushSubscriptions() skips these subs (see the per-worker-egress guard there).
    if (perWorkerEgressActive_ && workerPool_ && msg.topicId >= 0) {
        forwardSubscribeToWorker_(subId, msg.sessionId, msg.topicId, partition,
                                  nextOffset, msg.isolation,
                                  /*isGroup=*/false, /*groupCarrierReceived=*/false);
    }

    // Reply SubscribeResponse{OK, partition, subscriptionId}.
    sendResp(pc::ErrorCode::OK, partition, subId);

    spdlog::debug("ClientGateway: Subscribe sessionId={} subId={} topic={} partition={} nextOffset={} isolation={}",
                  msg.sessionId, subId, msg.topicId, partition, nextOffset, msg.isolation);
}

// ---------------------------------------------------------------------------
// handleUnsubscribe (GB-3, tid21) — remove a subscription.
//
// No response required (brief: "just remove").
// Drop-safe: unknown subscriptionId is silently ignored.
// ---------------------------------------------------------------------------
void ClientGateway::handleUnsubscribe(const uint8_t* buf, std::size_t len)
{
    auto msgOpt = pc::Unsubscribe::decode(buf, len);
    if (!msgOpt) {
        spdlog::debug("ClientGateway: Unsubscribe decode failed — dropped");
        return;
    }
    // read-rework T3/T4 (I3b): tell the owning worker to drop the cursor too.
    if (perWorkerEgressActive_ && workerPool_) {
        auto it = subscriptions_.find(msgOpt->subscriptionId);
        if (it != subscriptions_.end() && it->second.topicId >= 0) {
            forwardUnsubscribeToWorker_(msgOpt->subscriptionId,
                                        it->second.topicId, it->second.partition);
        }
    }

    const std::size_t removed = subscriptions_.erase(msgOpt->subscriptionId);
    spdlog::debug("ClientGateway: Unsubscribe subId={} removed={}",
                  msgOpt->subscriptionId, removed);
}

// ---------------------------------------------------------------------------
// pushSubscriptions (GB-3 / GR-6) — push new Records from the Log to each subscriber.
//
// Called from doWork() AFTER the ingress poll (single-writer, duty-cycle thread).
//
// For each active subscription:
//   - Compute isolation bound (GR-6):
//       When replicaRegistry_ is wired, look up the PartitionReplica.  If found,
//       use replica->highWatermark() as the delivery bound — this is the
//       majority-committed offset; no record above HW can ever be delivered to a
//       subscriber regardless of isolation setting.  A record above HW has been
//       written to the leader's log (LEO advanced) but has NOT been acknowledged
//       by a majority of replicas.  If the leader fails before majority replication,
//       the incoming leader truncates those records; a subscriber that already
//       received them would hold data that is no longer on any replica — a
//       correctness violation.  HW-bounding prevents this.
//
//       Isolation semantics post-GR-6:
//         isolation=0 (read_uncommitted): still bounded by HW.  The "uncommitted"
//           in the Kafka sense refers to open TRANSACTIONS (no-op in Milestone B/C
//           scope here) — it does NOT mean "allow delivery of truncatable records".
//           We default ALL subscribers to HW-bounded to be safe.
//         isolation=1 (read_committed): additionally bounded by the LSO and
//           aborted batches are filtered.  F15 S4 (Option B, I-B): with a worker
//           pool (every production mode) the gateway HW snapshot only bounds the
//           PushRead REQUEST — the owning worker decides read_committed from its
//           own RC state (executePushReadWire_).  Only the no-pool inline path
//           below keeps a gateway-side LSO (unit rigs; deletion scheduled T16).
//
//       If no replica is found (topic not replicated, or replicaRegistry_ null),
//       fall back to logEndOffset() — single-node / non-replicated path where HW
//       equals LEO by definition (no followers to lag).
//
//   - If nextOffset >= bound → skip (nothing committed to push yet).
//   - Otherwise: Log::read(nextOffset, kMaxChunkBytes) → iterate whole batches:
//       For each batch (identified via kLengthOffset = 12 + LE32@8):
//         Encode Records{subscriptionId, topicId, partition, baseOffset, batchBytes}.
//         tryClaim on session egress.
//         SUCCESS → advance nextOffset = baseOffset + recordCount.
//         BACK_PRESSURED / not-connected → STOP for this subscription this cycle.
//           Do NOT advance nextOffset (retry next cycle from same offset).
//   - Per-cycle fairness cap: at most kMaxBatchesPerSubPerCycle batches per subscription.
//
// Zero-alloc hot path: scratch_ is the pre-allocated encode buffer; Log::read
// returns a vector (unavoidable OS read) but we do not allocate per-batch.
// Replica lookup is O(1) (unordered_map pointer; no alloc).
//
// Correctness guarantee: nextOffset is NEVER advanced past a batch that was not
// successfully pushed (tryClaim > 0).  This ensures exactly-once delivery in order,
// even under back-pressure.
// ---------------------------------------------------------------------------
void ClientGateway::pushSubscriptions()
{
    using namespace rune::protocol::batch;

    ++pushCycle_;   // F15 S4 fix round 1: the backoff clock (cycles, never wall time)
    if (subscriptions_.empty()) return;

    for (auto& [subId, sub] : subscriptions_) {
        // read-rework T3/T4 (I3b): when per-worker egress is active the OWNING worker
        // reads+encodes+pushes user partitions on its own dynamic-MDC egress — the
        // gateway must NOT also push them (that would double-deliver).  Internal
        // partitions (topicId < 0) are not worker-owned, so keep gateway-pushing them.
        if (perWorkerEgressActive_ && sub.topicId >= 0) continue;
        // Find the owning session.
        Session* session = sessions_.find(sub.sessionId);
        if (sub.failed) [[unlikely]] {   // F19: parked at an undeliverable batch
            if (sub.errorOwed && session) trySendOwedPushError_(sub, *session);
            continue;
        }
        if (!session) {
            // Session gone — subscription will be reaped; skip this cycle.
            continue;
        }

        // Compute isolation bound (GR-6: HW-bounded delivery).
        // See block comment above for the full rationale.
        rune::storage::Log* log = topicRegistry_ ?
            topicRegistry_->log(sub.topicId, sub.partition) : nullptr;
        if (!log) continue; // registry removed or not set — skip

        // GR-6: Look up the PartitionReplica for this (topicId, partition).
        // If found → bound = replica->highWatermark() (majority-committed).
        // If not found (non-replicated / registry not wired) → bound = LEO (pass-through).
        // O(1), no alloc, lock-free (single-writer duty-cycle thread).
        int64_t hw;
        if (replicaRegistry_) {
            const rune::replication::PartitionReplica* replica =
                replicaRegistry_->lookup(sub.topicId, sub.partition);
            hw = replica ? replica->gatewayHighWatermark() : log->logEndOffset();
        } else {
            // No replication wired — single-node pass-through: HW == LEO.
            hw = log->logEndOffset();
        }

        // TX-4 Task 9: Apply isolation-level bound.
        //
        // read_uncommitted (isolation=0): bound = HW (deliver everything committed).
        // read_committed  (isolation=1): bound = LSO = min(firstOffset of open txns)
        //                                       or HW if no open transactions.
        //
        // Control batches (COMMIT/ABORT markers) are NEVER delivered to clients
        // in EITHER isolation mode (Kafka KIP-98 invariant 5; they carry
        // internal coordinator data, not application records).
        //
        // No heap alloc: getTxnStateRO is O(unordered_map lookup) = O(1) amortised.
        //
        // F15 S4 (I-B): with a worker pool the gateway makes NO read_committed
        // decision.  Its HW snapshot only bounds (loosens) the PushRead request;
        // the owning worker clamps the answer to its own LSO and strips aborted
        // and control batches (executePushReadWire_).  Only the no-pool inline
        // path below keeps a gateway-side LSO.
        int64_t bound = hw;
        if (!workerPool_ &&
            sub.isolation == static_cast<uint8_t>(rune::transaction::IsolationLevel::ReadCommitted)) {
            if (const PartitionTxnState* ts = getTxnStateRO(sub.topicId, sub.partition)) {
                bound = ts->lsoTracker.lso(hw);
            }
            // If no txn state exists yet, no transactional batches written → bound = hw.
        }

        if (sub.nextOffset >= bound) continue; // nothing committed to push yet

        // E2 inc2a: When a WorkerPool is wired, route the log read through the
        // owning worker's SPSC ingress ring.  The chunk is returned via the
        // return ring and processed in drainWorkerReturns_() next cycle.
        //
        // Skip this subscription if it already has an in-flight PushRead (the
        // previous cycle's read has not yet returned).  This preserves the
        // strictly-ordered delivery guarantee: we never issue a second read
        // before the first one's response has been sent.
        if (workerPool_) {
            if (pendingWorkerReads_.count(subId) > 0) {
                // In-flight — skip until the return is processed.
                continue;
            }
            // F15 addendum R-3: the owning worker answered "nothing below its
            // bound" (an open txn pins the LSO, a pre-carrier cut, the HW) for
            // this cursor at HW snapshot parkedAtHw — re-issue only once the HW
            // snapshot moved past it (any LSO advance needs a marker append,
            // which moves the HW).  No empty-PushRead spin.
            if (sub.parkedAtHw >= 0 && hw <= sub.parkedAtHw) continue;
            // Hold-capacity fallback (the worker could not hold a not-Ready
            // read_committed read): back off in push cycles, never a spin.
            if (sub.retryAtCycle > pushCycle_) continue;

            // Look up the owning replica (for the log accessor in the worker).
            const rune::replication::PartitionReplica* repForRead = nullptr;
            if (replicaRegistry_) {
                repForRead = replicaRegistry_->lookup(sub.topicId, sub.partition);
            }

            rune::node::WorkerOp op;
            op.type                = rune::node::WorkerOp::Type::PushRead;
            op.topicId             = sub.topicId;
            op.partition           = sub.partition;
            op.replica             = const_cast<rune::replication::PartitionReplica*>(repForRead);
            op.logPtr              = log;
            op.subscriptionId      = subId;
            op.sessionId           = sub.sessionId;
            op.nextOffset          = sub.nextOffset;
            op.hwBound             = bound;
            op.isolation           = sub.isolation;
            op.isGroup             = sub.isGroup;
            op.groupCarrierReceived = sub.groupCarrierReceived;

            if (!workerPool_->tryEnqueueOp(sub.topicId, sub.partition, std::move(op))) {
                spdlog::warn("ClientGateway: worker ring full for push sub={}; retrying next cycle",
                             subId);
                // Ring full — skip this cycle; pendingWorkerReads_ not set so
                // we retry next cycle.
            } else {
                // Successfully enqueued — mark as in-flight.
                pendingWorkerReads_.insert(subId);
                sub.issuedNextOffset = sub.nextOffset;   // R-1: the cursor this read answers
                sub.issuedHw         = bound;
                sub.parkedAtHw       = -1;
            }
            continue; // Gateway will process the return in the next cycle.
        }

        // INLINE path (pre-E2 / no WorkerPool):
        // Zero-copy: map a bounded span of the log starting at nextOffset — no heap
        // allocation, no pread copy.  The span aliases the segment's read-only mmap
        // (shared page cache with the writer), valid until the next readView() on
        // this log.  We fully consume (encode + push) it within this loop iteration.
        rune::storage::LogReadView view;
        try {
            view = log->readView(sub.nextOffset, kMaxChunkBytes);
        } catch (const std::exception& ex) {
            spdlog::warn("ClientGateway: Log::readView threw for sub={}: {}", subId, ex.what());
            continue;
        } catch (...) {
            spdlog::warn("ClientGateway: Log::readView threw unknown exception for sub={}", subId);
            continue;
        }

        if (view.empty()) continue; // nothing returned (nextOffset not yet visible)

        // Iterate whole record batches in the chunk (span into the read mmap).
        // Batch layout: kBaseOffsetOffset=0 (LE64), kLengthOffset=8 (LE32 = bytes after this field),
        //               total batch size = 12 + LE32@8; kRecordCountOffset=57 (LE32).
        const uint8_t* const chunk = view.data;
        std::size_t chunkPos  = 0;
        const std::size_t chunkSize = view.len;
        int batchesPushed = 0;
        bool backPressured = false;

        while (chunkPos < chunkSize && batchesPushed < maxBatchesPerSubPerCycle_ && !backPressured) {
            // Need at least kRecordCountOffset + 4 bytes to read header fields.
            constexpr std::size_t kMinBatchHdr =
                static_cast<std::size_t>(kRecordCountOffset) + 4u;
            if (chunkPos + kMinBatchHdr > chunkSize) break; // partial header at end of chunk

            const uint8_t* batchPtr = chunk + chunkPos;

            // Read baseOffset (LE64@0).
            int64_t baseOffset = 0;
            {
                for (int i = 0; i < 8; ++i)
                    baseOffset |= (static_cast<int64_t>(batchPtr[i]) << (i * 8));
            }

            // TX-4 Task 9: Per-batch isolation bound check.
            //
            // Stop delivering if this batch's baseOffset has reached the bound.
            //   read_uncommitted (isolation=0): bound = HW.
            //   read_committed  (isolation=1): bound = LSO.
            //
            // We STOP (break) rather than skip-and-continue, because the log is
            // ordered by baseOffset: all subsequent batches in the chunk also
            // have baseOffset >= bound (log is sequential).  Breaking also avoids
            // advancing nextOffset past `bound`, which would violate exactly-once
            // delivery once the LSO advances (those batches should be re-delivered
            // next cycle after the LSO moves forward).
            if (baseOffset >= bound) break;

            // Read length field (LE32@8) → batchSize = 12 + length.
            uint32_t lengthField = 0;
            {
                const uint8_t* p = batchPtr + kLengthOffset;
                lengthField = static_cast<uint32_t>(p[0]) |
                              (static_cast<uint32_t>(p[1]) << 8) |
                              (static_cast<uint32_t>(p[2]) << 16) |
                              (static_cast<uint32_t>(p[3]) << 24);
            }
            const std::size_t batchSize = 12u + static_cast<std::size_t>(lengthField);

            // Guard: the batch must fit within the remaining chunk.
            if (chunkPos + batchSize > chunkSize) break; // incomplete batch at end

            // Read attributes (LE16@kAttributesOffset=21).
            // Bits: bit4=TRANSACTIONAL, bit5=CONTROL.
            // Used for:
            //   (a) Control-batch skip (both isolation modes).
            //   (b) Aborted-range filter (read_committed only).
            uint16_t batchAttrs = 0;
            {
                constexpr std::size_t kAttrOff =
                    static_cast<std::size_t>(kAttributesOffset);
                if (chunkPos + kAttrOff + 2u <= chunkSize) {
                    batchAttrs = static_cast<uint16_t>(
                        static_cast<uint16_t>(batchPtr[kAttrOff]) |
                        (static_cast<uint16_t>(batchPtr[kAttrOff + 1u]) << 8));
                }
            }
            constexpr uint16_t kControlBit      = 0x20u;  // bit 5
            constexpr uint16_t kTransactionalBit = 0x10u;  // bit 4

            // TX-4: Skip control batches (COMMIT/ABORT markers) unconditionally.
            // Control batches must NEVER be delivered to applications in either
            // isolation mode (Kafka KIP-98; they are coordinator metadata only).
            if (batchAttrs & kControlBit) {
                // Advance past this control batch without delivering it.
                // We still need to read recordCount to compute the nextOffset advance.
                // Control batches have recordCount=1 (one ControlRecord), so we can
                // safely advance by 1.  But if recordCount field is available use it
                // to be correct for any future multi-record control batch format.
                const uint8_t* prc = batchPtr + kRecordCountOffset;
                int32_t ctlRecordCount = static_cast<int32_t>(
                    static_cast<uint32_t>(prc[0]) |
                    (static_cast<uint32_t>(prc[1]) << 8) |
                    (static_cast<uint32_t>(prc[2]) << 16) |
                    (static_cast<uint32_t>(prc[3]) << 24));
                if (ctlRecordCount < 1) ctlRecordCount = 1; // safety fallback
                chunkPos += batchSize;
                sub.nextOffset = baseOffset + static_cast<int64_t>(ctlRecordCount);
                continue;
            }

            // Read recordCount (LE32@57).
            int32_t recordCount = 0;
            {
                const uint8_t* p = batchPtr + kRecordCountOffset;
                recordCount = static_cast<int32_t>(
                    static_cast<uint32_t>(p[0]) |
                    (static_cast<uint32_t>(p[1]) << 8) |
                    (static_cast<uint32_t>(p[2]) << 16) |
                    (static_cast<uint32_t>(p[3]) << 24));
            }

            // Guard: recordCount must be positive.
            if (recordCount < 1) {
                // M2 fix: corrupt batch — skip it and advance BOTH chunkPos AND
                // sub.nextOffset so the subscription makes forward progress.
                //
                // Without advancing nextOffset, the next pushSubscriptions() cycle
                // re-reads from the same offset, re-encounters the same corrupt
                // batch, and stalls permanently (the subscription can never advance
                // past it).  (F19: an oversize batch is NOT skipped like this —
                // it is undeliverable, not corrupt: the subscription is failed
                // explicitly and parked at it.)
                //
                // A corrupt batch in a recovered log should be unreachable
                // (recover() drops torn tails), but if it occurs (e.g. a bug
                // writes recordCount=0 to the log), this ensures no permanent stall.
                //
                // Note: this is NOT the back-pressure path.  Back-pressure (pos≤0)
                // must NOT advance nextOffset — the back-pressured batch will be
                // retried next cycle from the same offset (see the else-branch below).
                spdlog::warn("ClientGateway: push sub={} corrupt batch at chunkPos={} "
                             "recordCount={} baseOffset={} — skipping, advancing nextOffset",
                             subId, chunkPos, recordCount, baseOffset);
                chunkPos += batchSize;
                // Use baseOffset+1 as the best-effort next offset when recordCount
                // is zero/negative (we cannot compute lastOffset = base + recordCount - 1).
                sub.nextOffset = baseOffset + 1;
                continue;
            }

            // TX-4 Task 9: Aborted-txn filter for read_committed.
            //
            // For read_committed (isolation=1): if this is a transactional data
            // batch (TRANSACTIONAL bit set) and its producerId falls within a
            // recorded aborted range, skip it (advance past it) without delivering.
            //
            // For read_uncommitted (isolation=0): no abort filtering — deliver all
            // non-control batches that are below HW (already bounded by `bound`).
            //
            // No alloc: AbortedTxnIndex::isAborted is O(ranges) = O(tiny) with a
            // vector scan; in practice at most a few concurrent aborted txns.
            //
            // TX-6b pre-carrier guard: group subscriptions are created with the
            // conservative default isolation=1 (read_committed) before the client's
            // Subscribe carrier frame arrives carrying the TRUE isolation level.
            // If we advance nextOffset past an aborted batch under isolation=1 and
            // the client then sends a carrier with isolation=0 (read_uncommitted),
            // those aborted records are permanently lost (nextOffset is already past
            // them).  To avoid this, if the subscription is a group sub whose carrier
            // hasn't arrived yet (!groupCarrierReceived), we STOP pushing at the first
            // transactional batch that would be isolation-filtered: break out of this
            // subscription's push loop without advancing nextOffset.  On the next
            // duty cycle — either after the carrier arrived (groupCarrierReceived=true,
            // correct isolation applied) or while still waiting (same stall, no harm)
            // — the batch is re-evaluated under the correct semantics.
            //
            // This stall is bounded: the carrier arrives within one client duty cycle
            // after SyncGroup and is processed on the SAME broker duty-cycle thread
            // (no cross-thread race).  The pre-carrier stall never affects non-group
            // subscriptions (isGroup=false never sets groupCarrierReceived=false in any
            // ambiguous way) and does not affect already-confirmed group subs.
            if (sub.isolation == static_cast<uint8_t>(rune::transaction::IsolationLevel::ReadCommitted)
                && (batchAttrs & kTransactionalBit)) {
                // Read producerId (LE64 at kProducerIdOffset=43).
                constexpr std::size_t kPidOff =
                    static_cast<std::size_t>(kProducerIdOffset);
                if (chunkPos + kPidOff + 8u <= chunkSize) {
                    int64_t batchPid = 0;
                    for (int bi = 0; bi < 8; ++bi)
                        batchPid |= (static_cast<int64_t>(batchPtr[kPidOff + bi]) << (bi * 8));

                    if (const PartitionTxnState* ts = getTxnStateRO(sub.topicId, sub.partition)) {
                        if (ts->abortedIndex.isAborted(batchPid, baseOffset)) {
                            // TX-6b: pre-carrier group sub: do NOT advance past this
                            // aborted batch.  Break so we re-evaluate after the carrier
                            // arrives with the true isolation level.
                            if (sub.isGroup && !sub.groupCarrierReceived) {
                                spdlog::debug("ClientGateway: push sub={} pre-carrier group "
                                              "sub stalling at aborted batch offset={} "
                                              "(waiting for isolation carrier)",
                                              subId, baseOffset);
                                break;
                            }
                            // Skip aborted batch — advance past it.
                            chunkPos += batchSize;
                            sub.nextOffset = baseOffset + static_cast<int64_t>(recordCount);
                            continue;
                        }
                    }
                }
            }

            // Records{subscriptionId, topicId, partition, baseOffset, batchBytes}:
            // the batch bytes are a zero-copy span into the chunk.
            //
            // F19: no size guard here — the Records var field has a u32 length and
            // pushRecords_ delivers any frame up to the egress maxMessageLength
            // (tryClaim fast path / vectored offer).  A larger frame is answered
            // with an explicit error and the subscription parks AT the batch —
            // never skipped silently.
            pc::Records rec;
            rec.sessionId      = sub.sessionId;
            rec.correlationId  = 0;  // push; no correlation
            rec.subscriptionId = sub.subscriptionId;
            rec.topicId        = sub.topicId;
            rec.partition      = sub.partition;
            rec.baseOffset     = baseOffset;
            rec.recordBatch    = std::span<const uint8_t>(batchPtr, batchSize);

            const rune::node::RecordsSendResult sent = pushRecords_(*session, rec);
            if (sent.kind == rune::node::RecordsSendKind::TooLarge) [[unlikely]] {
                failPushSubscriptionTooLarge_(sub, *session, rec.encodedSize());
                break;
            }
            const int64_t pos = sent.position;

            if (pos > 0) {
                // SUCCESS: advance nextOffset exactly past this batch.
                sub.nextOffset = baseOffset + static_cast<int64_t>(recordCount);
                chunkPos += batchSize;
                ++batchesPushed;
            } else {
                // BACK_PRESSURED or not-connected: STOP for this subscription this cycle.
                // Do NOT advance nextOffset — retry from the same offset next cycle.
                backPressured = true;
                spdlog::debug("ClientGateway: push sub={} back-pressured at offset={} (pos={})",
                              subId, baseOffset, pos);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// drainWorkerReturns_ (E2 inc2a) — collect results from per-worker SPSC rings
// and complete the corresponding in-flight gateway operations.
//
// Called at the top of doWork() (before ingress poll) when workerPool_ is set.
// This ensures that results from the PREVIOUS cycle's produce-appends and
// push-reads are delivered before processing new ingress — preserving the
// session-level ordering guarantee.
//
// ProduceResult handling:
//   1. If errorCode != 0: find session, send ProduceResponse{Internal, partition, 0}.
//   2. If success: send ProduceResponse{OK, partition, baseOffset} — every
//      worker result is FINAL (F15 I-A).  No mirror is fed (F15 S4/S5: the
//      owning worker holds the read_committed and producer state and syncs
//      its own logs).  A deferAck result is a shard-mode post-ack liveness
//      notification: the session is touched, nothing is sent.
//
// PushReadResult handling:
//   1. Remove subscriptionId from pendingWorkerReads_.
//   2. If chunk is empty → done (nothing to push this cycle).
//   3. Iterate whole record batches in the chunk (same loop as inline path).
//   4. Encode + tryClaim Records frames; advance sub.nextOffset on success only.
//      Back-pressure: stop for this subscription (same invariant as inline path).
//
// Single-writer: duty-cycle thread only.
// ---------------------------------------------------------------------------
bool ClientGateway::trySendMarker_(int64_t token)
{
    auto it = markersInFlight_.find(token);
    if (it == markersInFlight_.end()) return true;   // already resolved
    if (!workerPool_) return false;
    const MarkerInFlight& m = it->second;
    rune::node::WorkerOp op;
    op.type          = rune::node::WorkerOp::Type::AppendControlRecord;
    op.topicId       = m.topicId;
    op.partition     = m.partition;
    op.correlationId = token;
    op.producerId    = m.producerId;
    op.logPtr        = topicRegistry_ ? topicRegistry_->log(m.topicId, m.partition) : nullptr;
    op.recordBatchView = std::span<const uint8_t>(m.bytes);   // copied once into the ring slot
    // Replica identity is stamped by WorkerPool::tryEnqueueOp from the registry.
    return workerPool_->tryEnqueueOp(m.topicId, m.partition, std::move(op));
}

int ClientGateway::pumpMarkerQueue_()
{
    int sent = 0;
    while (!markerSendQueue_.empty()) {
        if (!trySendMarker_(markerSendQueue_.front())) break;   // still back-pressured
        markerSendQueue_.pop_front();
        ++sent;
    }
    return sent;
}

bool ClientGateway::markerQueuedFor_(int32_t topicId, int32_t partition) const noexcept
{
    for (const int64_t t : markerSendQueue_) {
        auto it = markersInFlight_.find(t);
        if (it != markersInFlight_.end() && it->second.topicId == topicId && it->second.partition == partition)
            return true;
    }
    return false;
}

void ClientGateway::onControlRecordAppended_(const rune::node::WorkerReturn& ret)
{
    auto it = markersInFlight_.find(ret.correlationId);
    if (it == markersInFlight_.end()) {
        spdlog::warn("ClientGateway: ControlRecordAppended for unknown marker token={} topic={} partition={}",
                     ret.correlationId, ret.topicId, ret.partition);
        return;
    }
    const MarkerInFlight m = std::move(it->second);
    markersInFlight_.erase(it);
    if (ret.errorCode != 0) {
        spdlog::error("ClientGateway: control marker for topic={} partition={} producerId={} FAILED "
                      "to append (error={}) — surfaced to the txn coordinator",
                      m.topicId, m.partition, m.producerId, ret.errorCode);
        if (txnService_) txnService_->onControlMarkerRefused(m.topicId, m.partition, m.producerId, ret.errorCode,
                                                             m.coordinatorEpoch);
        return;
    }
    // F15 S4 (Option B): the owning worker applied the marker to its own RC
    // state in the same invocation as the append — the gateway holds none.
    // Round 5: only now may the coordinator count this partition's marker as
    // written (the transaction completes when the last one lands).
    if (txnService_) txnService_->onControlMarkerLanded(m.topicId, m.partition, m.producerId, m.coordinatorEpoch);
}

void ClientGateway::onControlRecordRefused_(const rune::node::WorkerReturn& ret)
{
    ++controlRecordsRefused_;
    const bool registered = replicaRegistry_ && replicaRegistry_->lookup(ret.topicId, ret.partition);
    auto it = markersInFlight_.find(ret.correlationId);
    if (it == markersInFlight_.end()) {
        // Not a marker this gateway issued.  Still on this node → an invariant
        // violation (halt); gone → surface NotLeader to the coordinator.
        if (registered) {
            const std::string what = "control marker refused (unknown token " + std::to_string(ret.correlationId) +
                                     ") while topic=" + std::to_string(ret.topicId) + " partition=" +
                                     std::to_string(ret.partition) + " is registered on this node";
            spdlog::critical("ClientGateway: INVARIANT VIOLATION — {}", what);
            if (invariantViolationHandler_) invariantViolationHandler_(what);
            return;
        }
        if (txnService_) txnService_->onControlMarkerRefused(ret.topicId, ret.partition, ret.producerId, ret.errorCode,
                                                             ITxnRequestHandler::kAnyCoordinatorEpoch);
        return;
    }
    MarkerInFlight& m = it->second;
    if (registered) {
        // Task 10 round 4 (P1): re-routing is SAFE now — no offset was ever
        // pre-computed or reported; the offset comes from the append that
        // finally executes.  WorkerPool re-stamps the CURRENT registry identity.
        // Bounded: after (a)-(c) of round 3 a registered partition's marker can
        // only be refused transiently, so repeated refusals = a broken invariant.
        if (++m.reroutes > kMaxMarkerReroutes) {
            const std::string what = "control marker for topic=" + std::to_string(m.topicId) + " partition=" +
                                     std::to_string(m.partition) + " refused " + std::to_string(m.reroutes) +
                                     " times while registered on this node";
            spdlog::critical("ClientGateway: INVARIANT VIOLATION — {}", what);
            if (invariantViolationHandler_) invariantViolationHandler_(what);
            return;
        }
        spdlog::warn("ClientGateway: control marker for topic={} partition={} refused by the worker "
                     "(stale routing) — re-routed (attempt {})", m.topicId, m.partition, m.reroutes);
        if (!markerSendQueue_.empty() || !trySendMarker_(ret.correlationId))
            markerSendQueue_.push_front(ret.correlationId);   // ahead of later markers
        return;
    }
    spdlog::error("ClientGateway: control marker for topic={} partition={} producerId={} REFUSED "
                  "(partition not on this node) — surfaced to the txn coordinator as NotLeader",
                  m.topicId, m.partition, m.producerId);
    if (txnService_) txnService_->onControlMarkerRefused(m.topicId, m.partition, m.producerId, ret.errorCode,
                                                         m.coordinatorEpoch);
    markersInFlight_.erase(it);
}

void ClientGateway::raiseStorageFailure_(const std::string& logDir, int err)
{
    spdlog::critical("ClientGateway: STORAGE FAILURE on {} (errno={}) — fsync failed; "
                     "halting the broker (fail-stop)", logDir, err);
    if (storageFailureRaised_) return;
    storageFailureRaised_ = true;
    if (storageFailureHandler_) storageFailureHandler_(logDir, err);
}

// F15 S4 addendum R-3 + review I-1 / I-2: what a PushReadResult tells the
// scheduler.  Park the subscription (re-read only once the gateway HW snapshot
// moves past the one the read was issued at) ONLY on an explicit answer: a
// bound >= 0, ready, and the cursor at / past it.  An unanswered result (a
// refusal, no log: bound -1) parks nothing — the next push cycle re-issues.  A
// notReady result (the worker's hold capacity was exhausted) backs off
// 1, 2, 4 … 1024 push cycles, reset by the next ready answer.
void ClientGateway::noteWorkerReadAnswer_(Subscription& sub, int64_t effectiveBound, bool notReady) noexcept
{
    if (notReady) {
        sub.notReadyBackoff = sub.notReadyBackoff == 0 ? 1 : std::min<int64_t>(sub.notReadyBackoff * 2, 1024);
        sub.retryAtCycle    = pushCycle_ + sub.notReadyBackoff;
        return;
    }
    sub.notReadyBackoff = 0;
    sub.retryAtCycle    = 0;
    if (effectiveBound >= 0 && sub.nextOffset >= effectiveBound) sub.parkedAtHw = sub.issuedHw;
}

void ClientGateway::drainWorkerReturns_()
{
    using namespace rune::protocol::batch;

    workerPool_->drainAllReturns([this](rune::node::WorkerReturn ret) {
        handleWorkerReturn_(std::move(ret));
    });
}

void ClientGateway::handleWorkerReturnForTest(const rune::node::WorkerReturn& ret)
{
    handleWorkerReturn_(ret);
}

void ClientGateway::handleWorkerReturn_(rune::node::WorkerReturn ret)
{
    using namespace rune::protocol::batch;

        // ===================================================================
        // StorageFailure (fsyncgate fail-stop) — halt the broker.
        // ===================================================================
        if (ret.type == rune::node::WorkerReturn::Type::StorageFailure) [[unlikely]] {
            raiseStorageFailure_(std::string(ret.chunk.begin(), ret.chunk.end()), ret.errorCode);
            return;
        }

        // Task 13 fix round 1 (C2): a worker's ack publication for this session
        // can never deliver — close the session everywhere (table, workers,
        // subscriptions) so the client reconnects; its later notifications find
        // no session (no liveness refresh).  State-driven, never a timer.
        if (ret.type == rune::node::WorkerReturn::Type::SessionSignal) [[unlikely]] {
            onSessionSignal_(ret);
            return;
        }

        // Task 10 fix 3: a worker applied SetOwnership(gen) — lets the runtime
        // free replicas removed at or below the minimum acked generation.
        if (ret.type == rune::node::WorkerReturn::Type::OwnershipAck) {
            workerPool_->onOwnershipAck(ret.partition, static_cast<std::uint64_t>(ret.lastOffset));
            return;
        }

        // Task 10 fix 3 / round 4: a control marker refused by the worker
        // ownership guard.  Re-routed with the CURRENT routing if the partition
        // is still on this node (safe: its offset is never pre-computed);
        // otherwise NotLeader to the txn coordinator — never a silent drop.
        if (ret.type == rune::node::WorkerReturn::Type::ControlRecordRefused) [[unlikely]] {
            onControlRecordRefused_(ret);
            return;
        }
        // Task 10 round 4 (P1): the marker's AUTHORITATIVE offset.
        if (ret.type == rune::node::WorkerReturn::Type::ControlRecordAppended) {
            onControlRecordAppended_(ret);
            return;
        }

        // ===================================================================
        // ProduceResult
        // ===================================================================
        if (ret.type == rune::node::WorkerReturn::Type::ProduceResult) {
            // Mark the instant the gateway first sees the worker's produce result.
            // The gap between WorkerAppend (worker thread) and this mark is the
            // worker→gateway return-ring + duty-cycle latency (same correlationId).
            RUNE_FR_MARK_PRODUCE(rune_fr::Site::WorkerProcessOp,
                                 static_cast<uint32_t>(ret.correlationId));
            // Task 13 (spec §3.4): with worker-side acks (shard mode) a
            // deferAck ProduceResult is the post-ack notification of an ack the
            // worker already sent — and the only produce traffic the runtime
            // sees for the session (F23 liveness, ack time >= request time:
            // never early).  One lookup; nothing is sent.  F15 S5: it carries no
            // facts — the owning worker's psm_ is the sole producer-state
            // authority (the facts-only return and the gateway mirror are gone).
            assert(!(ret.deferAck && ret.errorCode != 0) &&
                   "worker invariant: an error ProduceResult never carries deferAck");
            if (ret.deferAck) {
                sessions_.touch(ret.sessionId, nowNs_);
                return;
            }
            Session* session = sessions_.find(ret.sessionId);
            if (!session) {
                // Session gone — drop result silently.
                return;
            }
            // F23: per-worker ingress (forced on in shard mode) delivers Produce
            // straight to the worker, bypassing onFragment, and every such
            // produce (acks=0 included) returns here — refresh liveness on the
            // lookup we already do (ack time >= request time: never early).
            session->lastSeenNs = nowNs_;

            // Error path: worker failed to append (e.g. Log threw).
            if (ret.errorCode != 0) {
                pc::ProduceResponse resp;
                resp.sessionId     = ret.sessionId;
                resp.correlationId = ret.correlationId;
                resp.errorCode     = ret.errorCode;
                resp.partition     = ret.partition;
                resp.baseOffset    = 0;
                static_assert(pc::ProduceResponse::ENCODED_SIZE <= kScratchSize,
                              "scratch too small for ProduceResponse");
                const std::size_t encoded = resp.encode(scratch_.data());
                // G0.1: reliable delivery (retry on back-pressure).
                enqueueOrBestEffort_(*session, scratch_.data(), encoded);
                return;
            }

            // F15 I-A: every ProduceResult from a worker is FINAL.  The owning
            // worker answers acks=all itself — parked on W until the HW passes
            // with |ISR| >= minIsr and the §3.2 role / ownership / log-generation
            // re-validation holds — so the gateway never parks a worker-owned
            // ack (its pendingAcks_ serves only the inline, control-owned path).
            {
                RUNE_FR_SPAN_PRODUCE(rune_fr::Site::ProduceAckSend,
                                     static_cast<uint32_t>(ret.correlationId));
                pc::ProduceResponse okResp;
                okResp.sessionId     = ret.sessionId;
                okResp.correlationId = ret.correlationId;
                okResp.errorCode     = static_cast<int16_t>(pc::ErrorCode::OK);
                okResp.partition     = ret.partition;
                okResp.baseOffset    = ret.baseOffset;
                const std::size_t encoded = okResp.encode(scratch_.data());
                // G0.1: reliable delivery (retry on back-pressure).
                enqueueOrBestEffort_(*session, scratch_.data(), encoded);
            }
            return;
        }

        // ===================================================================
        // PushReadResult
        // ===================================================================
        if (ret.type == rune::node::WorkerReturn::Type::PushReadResult) {
            // Always clear the in-flight marker regardless of outcome.
            pendingWorkerReads_.erase(ret.subscriptionId);

            // F15 S3 re-review 1 I-4: the next stored batch is larger than the
            // worker's return ring can carry — fail this subscription once,
            // non-retriably, and park it (never re-issue the read: no spin).
            // Merged with F19 (integration wave 2): the SAME failure state and the
            // same owed error (encodePushTooLargeError: MessageTooLarge,
            // correlationId 0, subscription named — re-review 2 m-f), re-offered
            // each push cycle until sent (errorOwed) — never best-effort.
            if (ret.errorCode != 0) [[unlikely]] {
                auto it = subscriptions_.find(ret.subscriptionId);
                if (it == subscriptions_.end() || it->second.failed) return;
                Subscription& sub = it->second;
                sub.failed         = true;
                sub.errorOwed      = true;
                sub.failedCause    = rune::node::PushTooLargeCause::ReturnRing;
                sub.failedFrameLen = 0;
                spdlog::error("ClientGateway: subscription {} (topicId={} partition={} offset={}) failed: "
                              "worker answered error {} (a stored batch exceeds the worker return ring) — "
                              "MessageTooLarge, subscription parked (cursor not advanced)",
                              ret.subscriptionId, ret.readTopicId, ret.readPartition, ret.newNextOffset,
                              ret.errorCode);
                if (Session* session = sessions_.find(sub.sessionId))
                    trySendOwedPushError_(sub, *session);
                return;
            }

            // Find the subscription (it may have been removed if the client
            // unsubscribed while the read was in-flight).
            auto subIt = subscriptions_.find(ret.subscriptionId);
            if (subIt == subscriptions_.end()) return;
            Subscription& sub = subIt->second;
            if (sub.failed) [[unlikely]] return;   // F19 ⇄ F15 S3: parked — never push past the failed batch
            // A result for a cursor the subscription no longer holds (rewound /
            // re-seeded meanwhile) carries nothing to adopt.
            const bool current = sub.nextOffset == sub.issuedNextOffset;

            if (ret.chunk.empty()) {
                // F15 addendum R-1: the worker may have consumed batches without
                // returning any (a trailing marker, an aborted tail stripped on
                // the worker) — adopt its cursor, else the partition is re-read
                // every cycle.  R-3: a cursor at the worker's effective bound
                // parks until the gateway's HW snapshot moves.
                if (current) {
                    if (ret.newNextOffset > sub.nextOffset) sub.nextOffset = ret.newNextOffset;
                    noteWorkerReadAnswer_(sub, ret.pushEffectiveBound(), ret.pushRcNotReady());
                }
                return;
            }

            // Find the owning session.
            Session* session = sessions_.find(ret.sessionId);
            if (!session) return;
            if (!current) return;   // stale cursor: the next push cycle re-reads

            // Iterate whole record batches in the chunk — mirrors the inline
            // pushSubscriptions() loop exactly.
            const std::vector<uint8_t>& chunk = ret.chunk;
            std::size_t chunkPos  = 0;
            const std::size_t chunkSize = chunk.size();
            int batchesPushed = 0;
            bool backPressured = false;

            while (chunkPos < chunkSize
                   && batchesPushed < maxBatchesPerSubPerCycle_
                   && !backPressured)
            {
                constexpr std::size_t kMinBatchHdr =
                    static_cast<std::size_t>(kRecordCountOffset) + 4u;
                if (chunkPos + kMinBatchHdr > chunkSize) break;

                const uint8_t* batchPtr = chunk.data() + chunkPos;

                // Read baseOffset (LE64@0).
                int64_t baseOffset = 0;
                for (int i = 0; i < 8; ++i)
                    baseOffset |= (static_cast<int64_t>(batchPtr[i]) << (i * 8));

                // Isolation bound check (bound was captured at enqueue time).
                if (baseOffset >= ret.hwBound) break;

                // Read length field (LE32@8) → batchSize = 12 + length.
                uint32_t lengthField = 0;
                {
                    const uint8_t* p = batchPtr + kLengthOffset;
                    lengthField = static_cast<uint32_t>(p[0]) |
                                  (static_cast<uint32_t>(p[1]) << 8) |
                                  (static_cast<uint32_t>(p[2]) << 16) |
                                  (static_cast<uint32_t>(p[3]) << 24);
                }
                const std::size_t batchSize =
                    12u + static_cast<std::size_t>(lengthField);

                if (chunkPos + batchSize > chunkSize) break;

                // Read attributes (LE16@kAttributesOffset=21).
                uint16_t batchAttrs = 0;
                {
                    constexpr std::size_t kAttrOff =
                        static_cast<std::size_t>(kAttributesOffset);
                    if (chunkPos + kAttrOff + 2u <= chunkSize) {
                        batchAttrs = static_cast<uint16_t>(
                            static_cast<uint16_t>(batchPtr[kAttrOff]) |
                            (static_cast<uint16_t>(batchPtr[kAttrOff + 1u]) << 8));
                    }
                }
                constexpr uint16_t kControlBit      = 0x20u;
                constexpr uint16_t kTransactionalBit = 0x10u;

                // Skip control batches unconditionally.
                if (batchAttrs & kControlBit) {
                    const uint8_t* prc = batchPtr + kRecordCountOffset;
                    int32_t ctlRc = static_cast<int32_t>(
                        static_cast<uint32_t>(prc[0]) |
                        (static_cast<uint32_t>(prc[1]) << 8) |
                        (static_cast<uint32_t>(prc[2]) << 16) |
                        (static_cast<uint32_t>(prc[3]) << 24));
                    if (ctlRc < 1) ctlRc = 1;
                    chunkPos += batchSize;
                    sub.nextOffset = baseOffset + static_cast<int64_t>(ctlRc);
                    continue;
                }

                // Read recordCount (LE32@57).
                int32_t recordCount = 0;
                {
                    const uint8_t* p = batchPtr + kRecordCountOffset;
                    recordCount = static_cast<int32_t>(
                        static_cast<uint32_t>(p[0]) |
                        (static_cast<uint32_t>(p[1]) << 8) |
                        (static_cast<uint32_t>(p[2]) << 16) |
                        (static_cast<uint32_t>(p[3]) << 24));
                }

                if (recordCount < 1) {
                    spdlog::warn(
                        "ClientGateway: push (worker) sub={} corrupt batch at "
                        "chunkPos={} recordCount={} baseOffset={} — skipping",
                        ret.subscriptionId, chunkPos, recordCount, baseOffset);
                    chunkPos += batchSize;
                    sub.nextOffset = baseOffset + 1;
                    continue;
                }

                // F15 S4 (I-B): aborted transactional batches were stripped by
                // the owning worker (and, before a group's isolation carrier,
                // the chunk was CUT at the first one — addendum R-2): the
                // gateway makes no read_committed decision.

                // Records frame (zero-copy batch span).  F19: any size up to the
                // egress maxMessageLength is delivered; above it the subscription
                // is failed explicitly and parked AT the batch (no silent skip).
                pc::Records rec;
                rec.sessionId      = ret.sessionId;
                rec.correlationId  = 0;
                rec.subscriptionId = ret.subscriptionId;
                rec.topicId        = ret.readTopicId;
                rec.partition      = ret.readPartition;
                rec.baseOffset     = baseOffset;
                rec.recordBatch    =
                    std::span<const uint8_t>(batchPtr, batchSize);

                const rune::node::RecordsSendResult sent = pushRecords_(*session, rec);
                if (sent.kind == rune::node::RecordsSendKind::TooLarge) [[unlikely]] {
                    failPushSubscriptionTooLarge_(sub, *session, rec.encodedSize());
                    break;
                }
                const int64_t pos = sent.position;

                if (pos > 0) {
                    sub.nextOffset =
                        baseOffset + static_cast<int64_t>(recordCount);
                    chunkPos += batchSize;
                    ++batchesPushed;
                } else {
                    backPressured = true;
                    spdlog::debug(
                        "ClientGateway: push (worker) sub={} back-pressured at "
                        "offset={} (pos={})",
                        ret.subscriptionId, baseOffset, pos);
                }
            }
            // F15 addendum R-1 / R-3: every batch of the chunk was delivered →
            // adopt the worker's cursor (it is past the batches it stripped
            // after the last one returned) and park at its effective bound.
            if (!backPressured && chunkPos >= chunkSize) {
                if (ret.newNextOffset > sub.nextOffset) sub.nextOffset = ret.newNextOffset;
                noteWorkerReadAnswer_(sub, ret.pushEffectiveBound(), ret.pushRcNotReady());
            }
            return;
        }

}

// ---------------------------------------------------------------------------
// drainPendingAcks (GR-5) — drain the deferred acks=All pending-ack queue.
//
// Called from doWork() each cycle, after the replica's HW has been advanced
// by any FetchResponse processing (replication doWork is ordered BEFORE gateway
// doWork on the duty-cycle thread in BrokerNode::doWork()).
//
// For each pending entry:
//   - If replica->highWatermark() > lastOffset AND replica->isr().size() >=
//     minIsr_ → HW covers the WHOLE batch AND the ISR is still healthy at
//     DRAIN time → send ProduceResponse{OK, partition, baseOffset} and remove.
//     (HW is EXCLUSIVE — count of committed records — so the last record at
//     offset `lastOffset` is majority-durable only once HW > lastOffset.)
//     (DR-BUG-3: the ISR>=minIsr check at drain closes the enqueue→drain
//     shrink-to-minority TOCTOU; a minority leader never falsely acks OK.)
//   - If nowNs_ - enqueuedAtNs >= kPendingAckTimeoutNs → stuck → send
//     ProduceResponse{Internal} (followers not advancing) and remove the entry.
//   - Otherwise → leave in queue for next cycle.
//
// Correctness:
//   - An acks=All ack ONLY fires when HW > lastOffset (majority-durable).
//     HW is EXCLUSIVE (committed-record count); the last record at lastOffset is
//     durable IFF HW > lastOffset (== HW >= lastOffset + 1).
//   - An acks=All ack is NEVER sent with errorCode=OK if HW <= lastOffset.
//   - Timeout path: sends Internal (error) — NEVER an erroneous OK.
//   - Produce-order is NOT relied on: swap-and-pop (erase-by-swap-back-pop) can
//     reorder the vector on removal, so positional order is not guaranteed after
//     the first eviction.  This is correct because each entry carries its own
//     lastOffset and the HW check is purely per-entry (HW > lastOffset).
//     Every entry resolves independently; a stuck entry does NOT block later
//     entries from acking.  This is safe because HW is monotonically
//     non-decreasing: once HW > lastOffset[n], the same HW automatically
//     satisfies all earlier batches in the same partition sequence.
//     Future maintainers: do NOT rely on positional vector order for
//     per-session or per-partition sequencing — the swap-and-pop is intentional.
//
// Zero-alloc: no heap allocation on any code path.  Erase-by-swap-back-pop
// ensures amortised O(1) removal at arbitrary positions.
//
// Single-writer: called only on the duty-cycle thread.
// ---------------------------------------------------------------------------
void ClientGateway::drainPendingAcks()
{
    if (pendingAcks_.empty()) return;

    // Iterate with index (not iterator) because we swap-and-pop, which invalidates
    // iterators.  Traversal is front-to-back; positional order is not maintained
    // across removals (swap-and-pop reorders entries).  Each entry resolves
    // independently via its own HW check — no positional ordering is required.
    for (std::size_t i = 0; i < pendingAcks_.size(); ) {
        PendingAck& entry = pendingAcks_[i];

        bool remove = false;

        // HW check: has a majority replicated the WHOLE batch?
        //
        // INVARIANT (GR-9): highWatermark() is EXCLUSIVE — it is the count of
        // majority-committed records (== the next offset to be committed), i.e.
        // HW == leos[majority-1] where each follower/leader LEO is the NEXT slot
        // to write.  A record at offset X is majority-durable IFF HW > X
        // (equivalently HW >= X + 1).  entry.lastOffset is the INCLUSIVE offset
        // of the LAST record in the batch (base + lastOffsetDelta).  Therefore
        // the batch is fully majority-durable IFF  HW > lastOffset.
        //
        // Do NOT use >= here: with an exclusive HW that acks a single-record
        // batch (lastOffset=0) at HW=0 — BEFORE any follower has replicated it —
        // silently defeating acks=All durability (records lost on leader kill).
        // Do NOT over-correct to > lastOffset+1 either: once a majority's LEO
        // reaches N (all N records fetched) HW=N and HW > lastOffset=N-1 is true,
        // so the last record acks promptly (no never-ack stall).
        // DR-BUG-3: drain-time min-ISR COMMIT gate (Kafka min.insync.replicas).
        //
        // The enqueue-time gate (handleProduce) only proves the ISR was healthy
        // at ADMISSION.  The ISR can shrink below minIsr in the window between
        // enqueue and this drain (leader stranded in a minority after followers
        // die).  An acks=All entry may be acked OK ONLY when BOTH:
        //   (a) HW covers the whole batch (HW > lastOffset), AND
        //   (b) the ISR is still >= minIsr at DRAIN time.
        //
        // If the ISR is below minIsr we do NOT ack (hold the entry); it stays in
        // the queue and either (i) acks later if the ISR recovers and HW covers
        // it, or (ii) hits the existing timeout path below → error (never OK).
        // This closes the shrink-to-minority TOCTOU: a minority leader NEVER
        // returns a false majority-durable ack.
        //
        // Defense in depth: PartitionReplica::highWatermark() also clamps the
        // exposed HW to the last majority-committed offset while |ISR| < minIsr
        // (so HW > lastOffset would already be false for the stranded tail); this
        // explicit gate makes the commit contract unambiguous at the ack site and
        // is robust to any future HW-exposure change.
        // G1.1: use per-cycle snapshot (plain int) instead of per-iteration atomic load.
        const int drainMinIsr = cachedMinIsr_;
        // Task 10 round 4 (Important 3): resolve the replica through the
        // registry on EVERY check — never a pointer held across cycles.  Gone
        // or re-created (different incarnation) → stale → fail RETRIABLY.
        const rune::replication::PartitionReplica* rep = nullptr;
        if (replicaRegistry_) {
            rep = entry.incarnation != 0
                ? replicaRegistry_->resolve(entry.topicId, entry.partition, entry.incarnation)
                : replicaRegistry_->lookup(entry.topicId, entry.partition);
        }
        if (rep == nullptr) [[unlikely]] {
            if (Session* session = sessions_.find(entry.sessionId)) {
                pc::ProduceResponse resp;
                resp.sessionId     = entry.sessionId;
                resp.correlationId = entry.correlationId;
                resp.errorCode     = static_cast<int16_t>(pc::ErrorCode::NotLeader);
                resp.partition     = entry.partition;
                resp.baseOffset    = 0;
                const std::size_t encoded = resp.encode(scratch_.data());
                enqueueOrBestEffort_(*session, scratch_.data(), encoded);
            }
            spdlog::debug("ClientGateway: acks=All pending ack for topic={} partition={} is stale "
                          "(partition removed / re-created) → NotLeader", entry.topicId, entry.partition);
            if (i + 1 < pendingAcks_.size()) pendingAcks_[i] = std::move(pendingAcks_.back());
            pendingAcks_.pop_back();
            continue;
        }
        // Truncation (KIP-101) / demotion: the worker path's §3.2 rule — a
        // waiter whose data a truncation or reconcile may have replaced never
        // fires OK (its offsets may now hold another leader's data); it fails
        // RETRIABLY (the resend is deduplicated or appended by the proven
        // producer state).  The replica is control-owned on this inline path.
        if (!inlineWaiterStillValid_(*rep, entry)) [[unlikely]] {
            if (Session* session = sessions_.find(entry.sessionId)) {
                pc::ProduceResponse resp;
                resp.sessionId     = entry.sessionId;
                resp.correlationId = entry.correlationId;
                resp.errorCode     = static_cast<int16_t>(pc::ErrorCode::NotLeader);
                resp.partition     = entry.partition;
                resp.baseOffset    = 0;
                const std::size_t encoded = resp.encode(scratch_.data());
                enqueueOrBestEffort_(*session, scratch_.data(), encoded);
            }
            if (i + 1 < pendingAcks_.size()) pendingAcks_[i] = std::move(pendingAcks_.back());
            pendingAcks_.pop_back();
            continue;
        }
        // Task #13: use gatewayIsrSize() (published-snapshot acquire-load)
        // instead of the live isr().size() — closes the ISR vector race.
        // (Round 4: pre-existing atomic snapshot — not extended; F9 removes it.)
        const bool isrHealthy = rep->gatewayIsrSize() >= drainMinIsr;

        // Task #13: use gatewayHighWatermark() (published-snapshot acquire-load)
        // instead of the live highWatermark() — closes TSan R5/R6b.
        const int64_t entryHw = rep->gatewayHighWatermark();

        if (isrHealthy && entryHw > entry.lastOffset) {
            // HW advanced past this batch AND ISR is healthy — ack OK.
            Session* session = sessions_.find(entry.sessionId);
            if (session) {
                pc::ProduceResponse resp;
                resp.sessionId     = entry.sessionId;
                resp.correlationId = entry.correlationId;
                resp.errorCode     = static_cast<int16_t>(pc::ErrorCode::OK);
                resp.partition     = entry.partition;
                resp.baseOffset    = entry.baseOffset;
                static_assert(pc::ProduceResponse::ENCODED_SIZE <= kScratchSize,
                              "scratch too small for ProduceResponse");
                const std::size_t encoded = resp.encode(scratch_.data());
                // G0.1: reliable delivery (retry on back-pressure) — this is the
                // highest-durability acks=All OK ack; a silent drop here is the
                // exact defect #3 the retry queue exists to prevent.
                enqueueOrBestEffort_(*session, scratch_.data(), encoded);
                spdlog::debug("ClientGateway: acks=All deferred ack sent OK "
                              "sessionId={} partition={} baseOffset={} lastOffset={} hw={}",
                              entry.sessionId, entry.partition, entry.baseOffset,
                              entry.lastOffset, entryHw);
            }
            // Session gone (reaped): entry is silently discarded (no way to ack).
            remove = true;
        } else if (nowNs_ - entry.enqueuedAtNs >= kPendingAckTimeoutNs) {
            // Timeout: HW never advanced to cover this batch within the timeout
            // (follower stuck/crashed and ISR not updated yet).  Send an error
            // response so the client can retry rather than waiting forever.
            // NEVER send OK here — that would falsely claim majority durability.
            Session* session = sessions_.find(entry.sessionId);
            if (session) {
                pc::ProduceResponse resp;
                resp.sessionId     = entry.sessionId;
                resp.correlationId = entry.correlationId;
                resp.errorCode     = static_cast<int16_t>(pc::ErrorCode::Internal);
                resp.partition     = entry.partition;
                resp.baseOffset    = entry.baseOffset;
                const std::size_t encoded = resp.encode(scratch_.data());
                // G0.1: reliable delivery (retry on back-pressure).
                enqueueOrBestEffort_(*session, scratch_.data(), encoded);
                spdlog::warn("ClientGateway: acks=All deferred ack timed out "
                             "sessionId={} partition={} baseOffset={} lastOffset={} "
                             "hw={} elapsed_ms={}",
                             entry.sessionId, entry.partition, entry.baseOffset,
                             entry.lastOffset, entryHw,
                             (nowNs_ - entry.enqueuedAtNs) / 1'000'000LL);
            }
            remove = true;
        }

        if (remove) {
            // Swap-and-pop: move last element to position i, shrink by 1.
            // If this IS the last element, swap is a no-op and pop reduces size by 1.
            if (i + 1 < pendingAcks_.size()) {
                pendingAcks_[i] = std::move(pendingAcks_.back());
            }
            pendingAcks_.pop_back();
            // Do NOT increment i — the swapped-in entry at position i needs checking.
        } else {
            ++i;
        }
    }
}

// ---------------------------------------------------------------------------
// flushDirtyLogs (GB-5p2) — group-commit fsync for the produce hot path.
//
// Called from doWork() every cycle, but only performs the actual fsync when
// kGroupCommitIntervalNs nanoseconds have elapsed since the last flush.
// This is the time-based group-commit cadence:
//
//   - Produces BETWEEN flushes: appendNoSync() writes to page cache at ~1-10µs.
//     ProduceResponse is sent immediately (acks=Leader, page-cache-durable).
//     Data is visible to Log::read() before the fsync.
//   - At flush time: one fdatasync per dirty partition Log amortises the fsync
//     cost over every produce since the last flush.
//   - Under idle (no produces): dirtyLogs_ is empty; the timer still fires but
//     no syscalls are made → zero overhead.
//
// Durability contract — acks=Leader (acks=1):
//   Data written since the last flush is in the OS page cache and visible to
//   Log::read(), but NOT fsync-durable.  A leader crash before the next flush
//   may lose the most-recent un-flushed acked records.  The maximum durability
//   window is kGroupCommitIntervalNs (10 ms by default).  True fsync-durability
//   requires acks=All + ISR replication (Milestone C).
//
// TODO(Milestone C): when acks=All is wired, the ProduceResponse must be deferred
//   until ISR HWM advances past lastOffset on all in-sync replicas.  Today we ack
//   immediately after page-cache write.
//
// Single-writer: called only on the duty-cycle thread → no locking.
// Zero per-produce heap: dirtyLogs_ is a pre-allocated unordered_set<Log*>.
// ---------------------------------------------------------------------------
void ClientGateway::flushDirtyLogs()
{
    // Time-based cadence: skip if the flush interval has not elapsed yet.
    // This is the key that keeps fsync off the per-produce hot path.
    // Lazy init: lastFlushNs_ == 0 means "not yet set"; initialise to nowNs_
    // so the first flush fires after kGroupCommitIntervalNs, not immediately.
    if (lastFlushNs_ == 0) {
        lastFlushNs_ = nowNs_;
        return;
    }
    if (nowNs_ - lastFlushNs_ < kGroupCommitIntervalNs) return;

    if (dirtyLogs_.empty()) {
        // No produces since last flush — just advance the timer.
        lastFlushNs_ = nowNs_;
        return;
    }

    // Flush each dirty partition Log exactly once per interval.
    // MC6 fix: if sync() throws, keep the Log in dirtyLogs_ so the next cycle
    // retries (don't silently mark it clean on fsync failure).  An fsync failure
    // means the data is NOT durable; falsely clearing dirtyLogs_ would ack
    // durability that was never achieved.  We log the error but do NOT crash —
    // fsync failure is surfaced here and the dirty state is retained.
    // Note: on Linux, fsync failure may be unrecoverable (EIO/fsyncgate semantics);
    // at Milestone C this path should escalate to a leader step-down.
    //
    // E5.1.3 (d) R7 fix: skip any log in workerOwnedLogs_ — those are exclusively
    // owned by a worker thread (which drives its own sync via syncOwnedLogs_ in
    // doWork).  Calling Log::sync from the gateway thread while the worker thread
    // appends is a TSan-confirmed data race (R7).  Internal partitions (topicId<0)
    // are never added to workerOwnedLogs_ and always take this gateway sync path.
    std::unordered_set<rune::storage::Log*> stillDirty;
    for (rune::storage::Log* log : dirtyLogs_) {
        // R7 gate: skip worker-owned logs (they sync themselves on the worker thread).
        if (!workerOwnedLogs_.empty() && workerOwnedLogs_.count(log)) {
            // Worker-owned: remove from dirtyLogs_ (the worker handles sync).
            continue;
        }
        bool synced = false;
        try {
            log->sync();
            synced = true;
        } catch (const std::exception& ex) {
            spdlog::error("ClientGateway: flushDirtyLogs log->sync() threw: {}; "
                          "log stays dirty (durability NOT achieved)", ex.what());
        } catch (...) {
            spdlog::error("ClientGateway: flushDirtyLogs log->sync() threw unknown exception; "
                          "log stays dirty (durability NOT achieved)");
        }
        // fsyncgate: an fsync failure is never retried — the log is permanently
        // failed (LogSegment::sync) and the broker halts.
        if (!synced && log->storageFailed()) raiseStorageFailure_(log->dir(), log->storageErrno());
        if (!synced) stillDirty.insert(log);
    }
    dirtyLogs_ = std::move(stillDirty);
    lastFlushNs_ = nowNs_;
}

// ---------------------------------------------------------------------------
// handleGroupMessage (CG-3) — route consumer-group frames to the
// GroupCoordinationService.
//
// Flow:
//   1. Require a known session (drop on unknown session — no egress).
//   2. Delegate decode + act + encode to groupService_ (null-safe: drop if
//      service not wired in).
//   3. Send the encoded response on the session egress (same send path as all
//      other handlers — sendTo / scratch_).
//
// Reused response buffer: responses are encoded into scratch_ which is already
// 68 KiB — sufficient for any group response (max SyncGroupResponse with 64
// partitions ≈ 24 + 2 + 2 + 64*8 = 540 B; OffsetFetchResponse with 1024
// entries ≈ 24 + 2 + 1024*18 = 18458 B).  Well within kScratchSize.
//
// Single-writer: called only on the duty-cycle thread.
// ---------------------------------------------------------------------------
void ClientGateway::handleGroupMessage(const uint8_t* buf, std::size_t len,
                                       int64_t nowNs, uint16_t tid)
{
    if (!groupService_) {
        // Group service not wired in — drop silently (configuration choice).
        spdlog::debug("ClientGateway: group message tid={} received but groupService_ not set — dropped", tid);
        return;
    }

    // Decode just the session/correlationId from the MessageHeader to find the
    // session egress.  Every group request carries sessionId in the MessageHeader
    // (positions [8..15] LE64).  We don't need the full decode here — just the
    // session lookup.
    auto hdrOpt = pc::MessageHeader::decode(buf, len);
    if (!hdrOpt) {
        spdlog::debug("ClientGateway: group message tid={} header decode failed — dropped", tid);
        return;
    }

    Session* session = sessions_.find(hdrOpt->sessionId);
    if (!session) {
        spdlog::debug("ClientGateway: group message tid={} for unknown sessionId={} — dropped",
                      tid, hdrOpt->sessionId);
        return;
    }

    // Dispatch to the service.  The service encodes the response into scratch_.
    // Capacity is passed so handlers bound-check BEFORE writing each element;
    // the post-write guard below is a final safety net only.
    std::size_t encoded = 0;
    const std::size_t cap = scratch_.size();
    const auto templateId = static_cast<pc::TemplateId>(tid);
    switch (templateId) {
        case pc::TemplateId::JoinGroup:
            encoded = groupService_->handleJoinGroup(buf, len, nowNs, scratch_.data(), cap);
            break;
        case pc::TemplateId::SyncGroup:
            encoded = groupService_->handleSyncGroup(buf, len, nowNs, scratch_.data(), cap);
            break;
        case pc::TemplateId::Heartbeat:
            encoded = groupService_->handleHeartbeat(buf, len, nowNs, scratch_.data(), cap);
            break;
        case pc::TemplateId::LeaveGroup:
            encoded = groupService_->handleLeaveGroup(buf, len, nowNs, scratch_.data(), cap);
            break;
        case pc::TemplateId::OffsetCommit:
            encoded = groupService_->handleOffsetCommit(buf, len, nowNs, scratch_.data(), cap);
            break;
        case pc::TemplateId::OffsetFetch:
            encoded = groupService_->handleOffsetFetch(buf, len, nowNs, scratch_.data(), cap);
            break;
        default:
            return; // unknown group template — drop
    }

    if (encoded == 0) return; // handler signalled an error (e.g. decode failure)
    if (encoded > scratch_.size()) {
        // Should never happen: handlers bound-check before writing.
        spdlog::error("ClientGateway: group response tid={} encoded size {} > scratch {} — BUG; dropped",
                      tid, encoded, scratch_.size());
        return;
    }

    sendTo(*session, scratch_.data(), encoded);
}

// ---------------------------------------------------------------------------
// handleTxnMessage (TX-3) — route transaction frames to the
// TransactionCoordinationService.
//
// Mirrors handleGroupMessage exactly: decode session from MessageHeader,
// dispatch to txnService_, send the encoded response on the session egress.
//
// Single-writer: called only on the duty-cycle thread.
// ---------------------------------------------------------------------------
void ClientGateway::handleTxnMessage(const uint8_t* buf, std::size_t len,
                                     int64_t nowNs, uint16_t tid)
{
    if (!txnService_) {
        spdlog::debug("ClientGateway: txn message tid={} received but txnService_ not set — dropped", tid);
        return;
    }

    auto hdrOpt = pc::MessageHeader::decode(buf, len);
    if (!hdrOpt) {
        spdlog::debug("ClientGateway: txn message tid={} header decode failed — dropped", tid);
        return;
    }

    Session* session = sessions_.find(hdrOpt->sessionId);
    if (!session) {
        spdlog::debug("ClientGateway: txn message tid={} for unknown sessionId={} — dropped",
                      tid, hdrOpt->sessionId);
        return;
    }

    std::size_t encoded = 0;
    const std::size_t cap = scratch_.size();
    const auto templateId = static_cast<pc::TemplateId>(tid);
    switch (templateId) {
        case pc::TemplateId::InitProducerId:
            encoded = txnService_->handleInitProducerId(buf, len, nowNs, scratch_.data(), cap);
            break;
        case pc::TemplateId::AddPartitionsToTxn:
            encoded = txnService_->handleAddPartitions(buf, len, nowNs, scratch_.data(), cap);
            break;
        case pc::TemplateId::AddOffsetsToTxn:
            encoded = txnService_->handleAddOffsets(buf, len, nowNs, scratch_.data(), cap);
            break;
        case pc::TemplateId::TxnOffsetCommit:
            encoded = txnService_->handleTxnOffsetCommit(buf, len, nowNs, scratch_.data(), cap);
            break;
        case pc::TemplateId::EndTxn:
            encoded = txnService_->handleEndTxn(buf, len, nowNs, scratch_.data(), cap);
            break;
        default:
            return; // unknown txn template — drop
    }

    if (encoded == 0) return;
    if (encoded > scratch_.size()) {
        spdlog::error("ClientGateway: txn response tid={} encoded size {} > scratch {} — BUG; dropped",
                      tid, encoded, scratch_.size());
        return;
    }

    sendTo(*session, scratch_.data(), encoded);
}

// ---------------------------------------------------------------------------
// Task 8a: handleDefineTopic (RULING-2 F) — route DefineTopic (64) to the admin
// sink via a ring hand-off.  Validate + require known session + decode, then
// copy the fields into the sink and RETURN immediately.  NO registry access, NO
// blocking on the gateway hot path — a stalled AdminWorker never delays produce.
// ---------------------------------------------------------------------------
void ClientGateway::handleDefineTopic(const uint8_t* buf, std::size_t len)
{
    if (!adminSink_) {
        spdlog::debug("ClientGateway: DefineTopic received but adminSink_ not set — dropped");
        return;
    }
    auto msgOpt = pc::DefineTopic::decode(buf, len);
    if (!msgOpt) {
        spdlog::debug("ClientGateway: DefineTopic decode failed — dropped");
        return;
    }
    const pc::DefineTopic& m = *msgOpt;

    // Require a known session (mirror handleProduce).
    Session* session = sessions_.find(m.sessionId);
    if (!session) {
        spdlog::debug("ClientGateway: DefineTopic for unknown sessionId={} — dropped", m.sessionId);
        return;
    }

    // Copy-and-return: the sink OWNS the copied bytes (name/keyField/blob views
    // point into the ingress buffer and are copied by the sink before return).
    (void)adminSink_->submitDefineTopic(m.sessionId, m.correlationId,
                                        m.topicName, m.partitionCount,
                                        m.keyField, m.validate,
                                        m.descriptorBlob, m.descriptorLen);
    // Return immediately — the AdminWorker produces the DefineTopicResponse
    // asynchronously on the session egress (via the admin→control egress ring).
}

// ---------------------------------------------------------------------------
// Task 8a: handleGetTopicSchema (RULING-2 F) — route GetTopicSchema (66) to the
// admin sink via a ring hand-off (same discipline as handleDefineTopic).
// ---------------------------------------------------------------------------
void ClientGateway::handleGetTopicSchema(const uint8_t* buf, std::size_t len)
{
    if (!adminSink_) {
        spdlog::debug("ClientGateway: GetTopicSchema received but adminSink_ not set — dropped");
        return;
    }
    auto msgOpt = pc::GetTopicSchema::decode(buf, len);
    if (!msgOpt) {
        spdlog::debug("ClientGateway: GetTopicSchema decode failed — dropped");
        return;
    }
    const pc::GetTopicSchema& m = *msgOpt;

    Session* session = sessions_.find(m.sessionId);
    if (!session) {
        spdlog::debug("ClientGateway: GetTopicSchema for unknown sessionId={} — dropped", m.sessionId);
        return;
    }
    (void)adminSink_->submitGetSchema(m.sessionId, m.correlationId, m.topicName);
}

// ---------------------------------------------------------------------------
// CG-4: addGroupSubscription — create an internal push subscription for a
// group member.  Seeds nextOffset from fromOffset (the committed offset from
// the ReplicatedOffsetStore, or log-start/HW for auto.offset.reset).
//
// Reuses the SAME Subscription struct and pushSubscriptions() machinery;
// marks isGroup=true so revokeGroupSubscriptions() can identify it.
// Tracks the subscriptionId in groupMemberSubs_[memberSessionId].
//
// Returns subscriptionId > 0 on success, -1 if session is unknown or topic
// is unreachable (topicRegistry_ null / bad topicId / bad partition).
//
// Single-writer: must be called on the duty-cycle thread.
// Rebalance-time cold path: not on the per-cycle push hot path.
// ---------------------------------------------------------------------------
int64_t ClientGateway::addGroupSubscription(int64_t memberSessionId,
                                            int32_t topicId,
                                            int32_t partition,
                                            int64_t fromOffset,
                                            uint8_t isolation,
                                            bool    pendingCarrier)
{
    // Require a known session.
    Session* session = sessions_.find(memberSessionId);
    if (!session) {
        spdlog::warn("ClientGateway::addGroupSubscription: unknown session={}", memberSessionId);
        return -1;
    }

    // Validate the topic and partition via the registry.
    if (!topicRegistry_) {
        spdlog::warn("ClientGateway::addGroupSubscription: topicRegistry_ not set");
        return -1;
    }

    rune::storage::Log* log = topicRegistry_->log(topicId, partition);
    if (!log) {
        spdlog::warn("ClientGateway::addGroupSubscription: unknown topicId={} partition={}",
                     topicId, partition);
        return -1;
    }

    // Clamp fromOffset to [0, bound] where bound is HW when a replica is
    // wired (GR-6), else LEO.  Negative fromOffset is clamped to 0 (log start).
    const int64_t leo = log->logEndOffset();
    int64_t bound;
    if (replicaRegistry_) {
        const rune::replication::PartitionReplica* replica =
            replicaRegistry_->lookup(topicId, partition);
        bound = replica ? replica->gatewayHighWatermark() : leo;
    } else {
        bound = leo;
    }
    const int64_t nextOffset = (fromOffset < 0)  ? int64_t{0}
                             : (fromOffset > bound) ? bound
                             : fromOffset;

    // Allocate subscriptionId and register the group subscription.
    const int64_t subId = ++nextSubscriptionId_;

    Subscription sub;
    sub.subscriptionId       = subId;
    sub.sessionId            = memberSessionId;
    sub.topicId              = topicId;
    sub.partition            = partition;
    sub.nextOffset           = nextOffset;
    sub.isolation            = isolation;
    sub.isGroup              = true;
    // TX-6b: stall pre-carrier ONLY when the caller signals that a Subscribe
    // carrier frame is still in flight (pendingCarrier=true).  This is set
    // exclusively by GroupCoordinationService, which seeds the sub with the
    // conservative default isolation=1 and expects the client to send a carrier
    // with the true isolation level.  All other callers (explicit isolation,
    // tests) pass pendingCarrier=false and behave normally from the start.
    sub.groupCarrierReceived = !pendingCarrier;

    subscriptions_.emplace(subId, std::move(sub));

    // Track the subscriptionId under this member session for eager revoke.
    groupMemberSubs_[memberSessionId].push_back(subId);

    // read-rework I5: route group subscriptions to the owning worker too (the
    // worker pushes on its own dynamic-MDC egress).  Without this, pushSubscriptions
    // skips user partitions under per-worker egress and group reads get nothing.
    if (perWorkerEgressActive_ && workerPool_ && topicId >= 0) {
        forwardSubscribeToWorker_(subId, memberSessionId, topicId, partition,
                                  nextOffset, isolation,
                                  /*isGroup=*/true, /*groupCarrierReceived=*/!pendingCarrier);
    }

    spdlog::debug("ClientGateway::addGroupSubscription: sessionId={} subId={} "
                  "topic={} partition={} nextOffset={}",
                  memberSessionId, subId, topicId, partition, nextOffset);
    return subId;
}

// ---------------------------------------------------------------------------
// CG-4: revokeGroupSubscriptions — eagerly remove all push subscriptions
// that were created via addGroupSubscription() for the given member session.
//
// Normal client-initiated subscriptions (isGroup==false) are NOT touched.
// O(n_group_subs) where n_group_subs is the number of partitions assigned
// to this member — typically very small.
//
// Single-writer: must be called on the duty-cycle thread.
// ---------------------------------------------------------------------------
void ClientGateway::revokeGroupSubscriptions(int64_t memberSessionId)
{
    auto it = groupMemberSubs_.find(memberSessionId);
    if (it == groupMemberSubs_.end()) return;  // no group subs for this session

    for (int64_t subId : it->second) {
        // read-rework I5: tell the owning worker to drop the cursor too (mirror of
        // addGroupSubscription's forward), before erasing the gateway record.
        if (perWorkerEgressActive_ && workerPool_) {
            auto sIt = subscriptions_.find(subId);
            if (sIt != subscriptions_.end() && sIt->second.topicId >= 0) {
                forwardUnsubscribeToWorker_(subId, sIt->second.topicId, sIt->second.partition);
            }
        }
        const std::size_t removed = subscriptions_.erase(subId);
        spdlog::debug("ClientGateway::revokeGroupSubscriptions: sessionId={} subId={} removed={}",
                      memberSessionId, subId, removed);
    }
    groupMemberSubs_.erase(it);
}

// ---------------------------------------------------------------------------
// CG-4: sendControlFrame — send an already-encoded frame (e.g. Assign) on
// a session's egress.  Reuses the same sendTo path as all other responses.
//
// Returns true if the frame was accepted by the egress publication.
// Does NOT modify scratch_ (the caller encodes the frame into its own buffer).
//
// Single-writer: must be called on the duty-cycle thread.
// ---------------------------------------------------------------------------
bool ClientGateway::sendControlFrame(int64_t sessionId,
                                     const uint8_t* buf,
                                     std::size_t len)
{
    // Invoke the optional test capture callback BEFORE the egress send.
    // This allows test code to capture Assign frames without a live Aeron publication.
    if (onControlFrame_) {
        onControlFrame_(sessionId, buf, len);
    }

    Session* session = sessions_.find(sessionId);
    if (!session) {
        spdlog::debug("ClientGateway::sendControlFrame: unknown sessionId={} — dropped", sessionId);
        return false;
    }
    return sendTo(*session, buf, len);
}

// ---------------------------------------------------------------------------
// TX-4 Task 8: appendControlRecord
//
// Appends an already-framed control-marker batch (COMMIT or ABORT) to the
// log of the given (topicId, partition).  This is the "gateway writes marker"
// path used by TransactionCoordinationService::doWork() when it drives marker
// writes for PrepareCommit/PrepareAbort transactions.
//
// Key contract (KIP-98):
//   - Control batches are appended with acks=All (majority-durable) via
//     PartitionReplica::append(marker, acksAll=true).
//   - If the replica is not available (no replicaRegistry, or partition
//     unknown), falls back to a direct Log::appendNoSync (for single-node /
//     test environments where acks=All is trivially satisfied).
//   - After appending, calls replica->onLeaderLogAppended() to reconcile
//     leader LEO/HW.
//
// Returns the base offset of the appended batch, or -1 on failure.
//
// Single-writer: must be called on the duty-cycle thread.
// ---------------------------------------------------------------------------
std::size_t ClientGateway::purgeQueuedMarkers(int32_t olderThan)
{
    std::size_t purged = 0;
    for (auto q = markerSendQueue_.begin(); q != markerSendQueue_.end();) {
        auto it = markersInFlight_.find(*q);
        if (it != markersInFlight_.end() && it->second.coordinatorEpoch < olderThan) {
            markersInFlight_.erase(it);
            q = markerSendQueue_.erase(q);
            ++purged;
        } else {
            ++q;
        }
    }
    if (purged > 0)
        spdlog::info("ClientGateway: purged {} queued control marker(s) of coordinator epochs < {}", purged, olderThan);
    return purged;
}

int64_t ClientGateway::appendControlRecord(int32_t topicId, int32_t partition,
                                            std::span<const uint8_t> marker,
                                            int64_t producerId, bool commit,
                                            int32_t coordinatorEpoch)
{
    if (!topicRegistry_) {
        spdlog::warn("ClientGateway::appendControlRecord: no TopicRegistry — dropped");
        return -1;
    }

    rune::storage::Log* log = topicRegistry_->log(topicId, partition);
    if (!log) {
        spdlog::debug("ClientGateway::appendControlRecord: unknown topicId={} partition={} — dropped",
                      topicId, partition);
        return -1;
    }

    int64_t base = -1;

    // Find the replica (if replication is wired in).
    rune::replication::PartitionReplica* replica = nullptr;
    if (replicaRegistry_) {
        replica = replicaRegistry_->lookup(topicId, partition);
    }

    // Task #13 / F15 K3: the marker is appended by the OWNING worker under
    // EXACTLY the produce-routing predicate (routesToWorker_), in every mode —
    // including the default (Legacy) config, where produces already go to the
    // worker: appending the marker inline here was a write-write race with the
    // worker, and the marker overtook produces still queued on its op ring.
    // Queued on the same op ring, the marker lands after every produce routed
    // before it (L4).
    //
    // Task 10 round 4 (P1): the marker's offset is NOT pre-computed here; the
    // worker returns where it ACTUALLY landed (ControlRecordAppended) and
    // the owning worker applied it to its own RC state in the same invocation
    // (F15 S4; no gateway-side LSO with a worker pool).  The caller
    // gets kControlRecordAsync.  A full ring never drops the marker: it waits in
    // markerSendQueue_ and is retried every doWork, in order.  An unregistered
    // partition is stamped stale by WorkerPool and answered through the normal
    // refusal path.
    //
    // The inline branch below remains only without a worker pool (unit rigs) and
    // for gateway-owned internal partitions in shard mode (never markers).
    if (routesToWorker_(topicId)) {
        if (marker.empty()) return -1;
        if (producerId < 0 && marker.size() >= 43u + 8u) {
            producerId = 0;
            for (int i = 0; i < 8; ++i) producerId |= static_cast<int64_t>(marker[43u + i]) << (i * 8);
        }
        const int64_t token = nextMarkerToken_++;
        MarkerInFlight m;
        m.topicId    = topicId;
        m.partition  = partition;
        m.producerId = producerId;
        m.commit     = commit;
        m.coordinatorEpoch = coordinatorEpoch;
        m.bytes.assign(marker.begin(), marker.end());
        markersInFlight_.emplace(token, std::move(m));
        if (!markerSendQueue_.empty() || !trySendMarker_(token)) {
            markerSendQueue_.push_back(token);   // FIFO behind earlier markers
            spdlog::warn("ClientGateway::appendControlRecord: worker back-pressure for topicId={} "
                         "partition={} — marker queued ({} waiting), retried every cycle",
                         topicId, partition, markerSendQueue_.size());
        }
        return kControlRecordAsync;
    }

    if (replica) {
        // Durable path (F15 N2): the marker lands AS a control batch, restamped
        // in place, role- and min-ISR-gated (acks=All); negative = refused.
        base = replica->appendControlBatch(marker, /*acksAll=*/true);
        if (base < 0) {
            spdlog::warn("ClientGateway::appendControlRecord: appendControlBatch returned {} "
                         "topicId={} partition={}", base, topicId, partition);
            return -1;
        }
    } else {
        // No-replica path: single-node or test — direct log append.
        // In production this branch is only taken when replicaRegistry_ is null
        // (no replication wired); minIsr=1 is trivially satisfied locally.
        const int64_t batchLen = static_cast<int64_t>(marker.size());
        if (batchLen == 0) return -1;

        // Zero-copy restamp: read lastOffsetDelta (LE32@23) from the read-only
        // marker span (we must not mutate the caller's span), then let
        // appendNoSyncRestamp assign + patch the base offset via pwritev — no copy.
        int32_t lod = 0;
        {
            constexpr std::size_t kLodOff =
                static_cast<std::size_t>(rune::protocol::batch::kLastOffsetDeltaOffset);
            if (marker.size() >= kLodOff + 4u) {
                const uint8_t* p = marker.data() + kLodOff;
                lod = static_cast<int32_t>(
                    static_cast<uint32_t>(p[0]) |
                    (static_cast<uint32_t>(p[1]) << 8) |
                    (static_cast<uint32_t>(p[2]) << 16) |
                    (static_cast<uint32_t>(p[3]) << 24));
            }
        }

        try {
            log->appendNoSyncRestamp(marker.data(), marker.size(),
                                     lod >= 0 ? lod : 0, base);
        } catch (const std::exception& ex) {
            spdlog::error("ClientGateway::appendControlRecord: appendNoSyncRestamp threw: {}", ex.what());
            return -1;
        } catch (...) {
            spdlog::error("ClientGateway::appendControlRecord: appendNoSyncRestamp threw unknown exception");
            return -1;
        }

        dirtyLogs_.insert(log);
    }

    // Reconcile leader HW after the append.
    if (replica) {
        replica->onLeaderLogAppended();
    }

    // Markers / epoch fencing (PSM <-> log consistency): the rebuild applies
    // every control batch as an epoch raise (ProducerStateManager::rebuildFromLog);
    // the live inline state must too, or it differs from its own log (a zombie
    // of the fenced epoch would be appended).  Raise-only; cold (per marker).
    if (marker.size() >= static_cast<std::size_t>(rune::protocol::batch::kRecordBatchOverhead)) {
        const rune::protocol::RecordBatchReader mr(marker.data(), marker.size());
        producerStateManager_.fenceEpoch(topicId, mr.producerId(), partition, mr.producerEpoch());
    }

    // Synchronous (no-pool inline) path: the caller invokes notifyControlMarker()
    // to update the inline path's LsoTracker + AbortedTxnIndex.
    return base;
}

// ---------------------------------------------------------------------------
// TX-4 Task 9: notifyTransactionalBatch — test/integration hook.
//
// Feeds the LsoTracker for a transactional data batch that was appended via
// a path other than handleProduce (e.g. directly by a test fixture).
// In production this is called from handleProduce when the TRANSACTIONAL
// bit is set; this hook allows test fixtures to set up LSO state without
// going through the full produce pipeline.
//
// Single-writer: duty-cycle thread only.
// ---------------------------------------------------------------------------
void ClientGateway::notifyTransactionalBatch(int32_t topicId, int32_t partition,
                                              int64_t producerId, int64_t baseOffset) noexcept
{
    if (producerId < 0) return;  // non-transactional; ignore
    getTxnState(topicId, partition).lsoTracker.onProduceBatch(producerId, baseOffset);
}

// ---------------------------------------------------------------------------
// TX-4 Task 9: notifyControlMarker — feeds LsoTracker + AbortedTxnIndex.
//
// Called when a COMMIT or ABORT control marker is appended for a producer
// on (topicId, partition).  Updates the LsoTracker (removes the producer from
// open transactions → LSO can advance) and, for ABORT markers, records the
// aborted range in AbortedTxnIndex so pushSubscriptions() can filter it.
//
// In production this is called from appendControlRecord (or the doWork path
// that drives marker appends).  In tests the fixture calls it directly after
// appending test marker batches to set up the expected LSO/aborted state.
//
// Single-writer: duty-cycle thread only.
// ---------------------------------------------------------------------------
void ClientGateway::notifyControlMarker(int32_t topicId, int32_t partition,
                                         int64_t producerId, int64_t markerOffset,
                                         bool commit) noexcept
{
    if (producerId < 0) return;  // non-transactional marker; ignore
    PartitionTxnState& ts = getTxnState(topicId, partition);
    const int64_t firstOffset = ts.lsoTracker.onControlMarker(producerId, markerOffset, commit);
    if (!commit && firstOffset >= 0) {
        // ABORT: record the aborted range so pushSubscriptions() can filter it.
        ts.abortedIndex.onAbortMarker(producerId, firstOffset, markerOffset);
    }
}

// ---------------------------------------------------------------------------
// handleMetadata (MD1 Task 10) — answer a MetadataRequest from the routing image.
//
// ANSWERABLE BY ANY NODE — not just the leader.  Cluster metadata is
// cluster-wide information; any node can build a MetadataResponse from the
// RoutingImage (which is published by MetadataService to all nodes via the
// RoutingImageHolder and then installed into the gateway by
// setRoutingImageSnapshot()).
//
// Response contents:
//   - brokers[]: one entry per nodeId in nodeEndpointMap_ (nodeId + "host:port"
//     parsed from the Aeron channel string).  The port is extracted from the
//     endpoint part of "aeron:udp?endpoint=host:port"; nodeId 0 is the local node
//     (thisNodeId_) with its own channel.  Empty map → empty brokers list.
//   - partitions[]: one entry per slot in the routing image that is not the
//     NO_LEADER sentinel.  The (topicId, partition) pair is recovered from the
//     GPID via the inverse of the gpid = topicId * kMaxGpidPartitions + partition
//     scheme.  If topicRegistry_ is wired, only partitions belonging to known topics
//     are included; otherwise all non-sentinel slots are included.
//
// When routingImage_ is null (no metadata published yet) the response carries
// empty partitions but still includes the broker list (from nodeEndpointMap_).
//
// The response is sent back on the session's egress — it must be decoded by the
// client just like any other broker→client frame.
//
// Single-writer: duty-cycle thread only (same as all other handle* methods).
// ---------------------------------------------------------------------------
void ClientGateway::handleMetadata(const uint8_t* buf, std::size_t len)
{
    auto reqOpt = pc::MetadataRequest::decode(buf, len);
    if (!reqOpt) return;  // truncated or bad — drop

    const pc::MetadataRequest& req = *reqOpt;

    Session* session = sessions_.find(req.sessionId);
    if (!session) return;  // unknown session — drop

    // -----------------------------------------------------------------------
    // Build BrokerEntry list from nodeEndpointMap_.
    //
    // Each value in nodeEndpointMap_ is an Aeron channel string of the form
    //   "aeron:udp?endpoint=host:port"
    // We parse out "host" and "port" for the wire encoding.
    // A missing or unparseable entry is included as port=0 + empty host
    // (best-effort; the client can still route via the endpoint string).
    // -----------------------------------------------------------------------
    constexpr std::size_t kMaxBrokers = 32;
    static_assert(kMaxBrokers <= 256);
    std::vector<pc::BrokerEntry> brokers;
    brokers.reserve(nodeEndpointMap_.size());

    // We keep the parsed host strings alive for the encode pass.
    std::vector<std::string> hostStore;
    hostStore.reserve(nodeEndpointMap_.size());

    for (const auto& [nodeId, channel] : nodeEndpointMap_) {
        pc::BrokerEntry b;
        b.nodeId = nodeId;

        // Parse "aeron:udp?endpoint=host:port" → host, port.
        // We look for "endpoint=" and then split on the last ':'.
        int32_t port = 0;
        std::string host;
        const auto epPos = channel.find("endpoint=");
        if (epPos != std::string::npos) {
            const std::string_view ep(channel.c_str() + epPos + 9,
                                       channel.size() - epPos - 9);
            const auto colonPos = ep.rfind(':');
            if (colonPos != std::string_view::npos) {
                host = std::string(ep.substr(0, colonPos));
                try {
                    port = std::stoi(std::string(ep.substr(colonPos + 1)));
                } catch (...) {
                    port = 0;
                }
            } else {
                host = std::string(ep);
            }
        } else {
            host = channel;  // store whole channel string as fallback
        }
        b.port = port;
        hostStore.push_back(std::move(host));
        b.host = hostStore.back();  // string_view into hostStore
        brokers.push_back(b);
    }

    // -----------------------------------------------------------------------
    // Build PartitionEntry list from routingImage_.
    //
    // The GPID scheme used by BrokerNode/MetadataService:
    //   gpid = gpidOf(topicId, partition)  [= topicId * kMaxGpidPartitions + partition]
    // We iterate all capacity slots and invert:
    //   topicId   = gpid / kMaxGpidPartitions
    //   partition = gpid % kMaxGpidPartitions
    //
    // If topicRegistry_ is set, we only include partitions whose topicId is in
    // range [0, topicRegistry_->topicCount()).  This prevents ghost partitions
    // from a stale routing image that refers to topics no longer registered.
    //
    // If the request carries specific topicIds (topicIds non-empty), filter
    // to only those topics.
    // -----------------------------------------------------------------------
    using rune::metadata::kMaxGpidPartitions;
    // Stable storage for the per-partition ingressEndpoint views (deque: element
    // addresses never move).  Control path (Metadata), not the per-record path.
    std::deque<std::string> endpointStore;
    std::vector<pc::PartitionEntry> parts;

    if (routingImage_) {
        const rune::metadata::RoutingImage* img = routingImage_.get();
        const int32_t cap = img->capacity();
        parts.reserve(std::min(cap, 4096));

        // Build a quick lookup set of requested topicIds (empty = all).
        // After the MISALIGNMENT FIX in MetadataMessages.h (decode() now copies
        // each int32 via getLE32 into an owned, naturally-aligned vector),
        // req.topicIds elements are already correct native int32 values — no
        // reinterpret_cast workaround needed here.
        const bool filterTopics = !req.topicIds.empty();
        std::unordered_set<int32_t> requestedTopics;
        if (filterTopics) {
            for (const int32_t topicId : req.topicIds) {
                requestedTopics.insert(topicId);
            }
        }

        for (int32_t gpid = 0; gpid < cap; ++gpid) {
            const auto& entry = img->lookup(gpid);
            if (entry.leaderNodeId == rune::metadata::RoutingImage::NO_LEADER_NODE_ID) {
                continue;  // unassigned — skip
            }
            const int32_t topicId   = gpid / kMaxGpidPartitions;
            const int32_t partition = gpid % kMaxGpidPartitions;

            // Filter: known topics only (when topicRegistry_ is wired).
            if (topicRegistry_ && topicId >= topicRegistry_->topicCount()) {
                continue;
            }

            // Filter: requested topics only (when non-empty topic list in request).
            if (filterTopics && requestedTopics.find(topicId) == requestedTopics.end()) {
                continue;
            }

            pc::PartitionEntry p;
            p.topicId     = topicId;
            p.partition   = partition;
            p.leaderId    = entry.leaderNodeId;
            p.leaderEpoch = entry.leaderEpoch;
            // E5.1: per-worker ingress routing — ingressStreamId = base + workerForPartition.
            // CRITICAL: only advertise a per-worker stream when per-worker ingress is
            // ACTUALLY ACTIVE (perWorkerIngressActive_). Otherwise the broker has NO
            // subscriber on the per-worker stream (only the gateway on clientStreamId),
            // so a client routing there hits NOT_CONNECTED and can't send consistently.
            //
            // When per-worker ingress is INACTIVE (gateway-fanout mode), advertise the
            // gateway's OWN client ingress stream (clientStreamId_) — NOT 0.  Every
            // node's gateway subscribes on clientStreamId_, so a client that opens a
            // per-partition session to the true leader endpoint on this stream lands
            // directly on that leader's gateway.  Advertising 0 here disabled client
            // per-partition routing entirely (the client's route builder skips
            // ingressStreamId==0), which — once partition leadership legitimately
            // SPREADS across nodes after failover — forced the client onto a single
            // shared transport that ping-ponged between two partition leaders on
            // consecutive records and exhausted its redirect budget (dr3 rolling
            // leader-kill: produced 44/50).  clientStreamId_ has a live subscriber on
            // every node, so it is always a safe, consistent routing target.
            //
            // F25 — shard mode (spec §2/§3.3): advertise clientStreamId_.  The
            // node endpoint's gateway subscribes it (→ worker op ring →
            // executeProduceWire_) AND every shard's WorkerIngress subscribes it
            // on its own port, so BOTH routes a client can derive are live: an
            // ingressEndpoint-aware client sends (endpoint, stream) straight to
            // the owning worker; one that ignores the trailer sends (node
            // endpoint, stream) to the gateway.  base + owner was subscribed only
            // on the worker port → the latter hit NOT_CONNECTED forever.
            p.ingressStreamId = shardMode_ ? clientStreamId_
                              : perWorkerIngressActive_
                              ? (workerIngressBaseStreamId_
                                 + (workerPool_
                                    ? workerPool_->ownerWorkerId(topicId, partition)
                                    : 0))
                              : clientStreamId_;
            // read-rework T3/T4 (I4): advertise the per-worker dynamic-MDC egress
            // endpoint so the client subscribes to the OWNING worker's control
            // address + stream to receive this partition's pushed Records.  Control
            // host = the leader BrokerEntry.host (client-side).  0 when inactive →
            // client falls back to the legacy per-session egress.
            if (perWorkerEgressActive_ && workerPool_) {
                const int32_t wid = workerPool_->ownerWorkerId(topicId, partition);
                p.egressControlPort = workerEgressControlBasePort_ + wid;
                p.egressStreamId    = workerEgressBaseStreamId_ + wid;
            }
            // Shard mode (spec §3.3): advertise the LEADER's owning-worker ingress
            // endpoint from explicit configuration (worker index is symmetric across
            // nodes); no entry / unadvertised base → empty → node endpoint.
            if (!shardIngressEndpoints_.empty() && workerPool_) {
                for (const auto& ep : shardIngressEndpoints_) {
                    if (ep.nodeId != entry.leaderNodeId) continue;
                    if (ep.basePort > 0 && !ep.host.empty()) {
                        endpointStore.push_back(ep.host + ":" +
                            std::to_string(ep.basePort + workerPool_->ownerWorkerId(topicId, partition)));
                        p.ingressEndpoint = endpointStore.back();
                    }
                    break;
                }
            }
            parts.push_back(p);
        }
    }

    // -----------------------------------------------------------------------
    // Encode MetadataResponse into scratch buffer.
    // -----------------------------------------------------------------------
    pc::MetadataResponse resp;
    resp.sessionId     = req.sessionId;
    resp.correlationId = req.correlationId;
    resp.brokers       = std::span<const pc::BrokerEntry>(brokers.data(), brokers.size());
    resp.partitions    = std::span<const pc::PartitionEntry>(parts.data(), parts.size());

    const std::size_t needed = resp.encodedSize();
    if (needed <= scratch_.size()) {
        const std::size_t encoded = resp.encode(scratch_.data());
        sendTo(*session, scratch_.data(), encoded);
    } else {
        spdlog::warn("ClientGateway: MetadataResponse too large ({} bytes); scratch={} — dropped",
                     needed, scratch_.size());
    }
}

} // namespace rune::gateway
