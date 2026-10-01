// AeronPartitionTransport.cpp — GR-1 implementation.
//
// See AeronPartitionTransport.h for design notes.
// Single-writer. No locks. No hot-path alloc.

#include <rune/replication/AeronPartitionTransport.h>
#include <rune/replication/ReplicationMessages.h>
#include <rune_fr/FlightRecorder.h>
#include <rune_protocol_internal_sbe/MessageHeader.h>
#include <rune_protocol_internal_sbe/OutboundFrame.h>

// E2 inc2a: WorkerPool + WorkerOp for SPSC ApplyReplicated routing.
// Included here (not in the header) to avoid pulling consensus/spdlog headers
// into consumers of rune_replication_aeron that don't need them.
// rune_replication_aeron has RUNE_NODE_INCLUDE set in its CMakeLists.txt.
#include <rune/node/WorkerPool.h>
#include <rune/node/WorkerOp.h>

#include <cstdlib>
#include <spdlog/spdlog.h>

#include <chrono>
#include <stdexcept>
#include <thread>
#include <cstring>

namespace rune::replication {

namespace isbe = ::rune::protocol::internal::sbe;

// kOutboundEnvelopeOverhead (header): the records tail must be capped so the
// whole envelope fits the outbound ring's maxMsgLength.

namespace {
// F3 round 5: the ONE formula for this transport's records ceiling, shared by
// the config-only helper (BrokerNode's derivation), the live instance, and
// poll()'s FetchRequest clamp.
int32_t fetchCeilingFrom(int64_t ringMaxMsgLength, int64_t termMaxMessageLength) noexcept {
    const int64_t framing = static_cast<int64_t>(FetchResponseMsg::encodedSize(0));
    const int64_t ringBound =
        ringMaxMsgLength - static_cast<int64_t>(AeronPartitionTransport::kOutboundEnvelopeOverhead) - framing;
    const int64_t termBound = termMaxMessageLength - framing;
    int64_t c = std::min(ringBound, termBound);
    if (c < 0) c = 0;
    if (c > INT32_MAX) c = INT32_MAX;
    return static_cast<int32_t>(c);
}
} // namespace

int32_t AeronPartitionTransport::maxFetchResponseBytesFor(int outboundRingBytes,
                                                          int32_t termBufferBytes) noexcept
{
    // IngressRing(payloadCapacity) → M2ORingBuffer capacity = payloadCapacity,
    // maxMsgLength = capacity / 8 (agrona rule).  Aeron pub maxMessageLength =
    // FrameDescriptor::computeMaxMessageLength(termLength).
    const int64_t ringMax = static_cast<int64_t>(outboundRingBytes) / 8;
    const int64_t termMax = termBufferBytes > 0
        ? aeron::concurrent::logbuffer::FrameDescriptor::computeMaxMessageLength(termBufferBytes)
        : 0;
    return fetchCeilingFrom(ringMax, termMax);
}

// ---------------------------------------------------------------------------
// Constructor
// ---------------------------------------------------------------------------
AeronPartitionTransport::AeronPartitionTransport(
        std::shared_ptr<aeron::Aeron> aeron,
        const std::string&             ingressChannel,
        int32_t                        ingressStreamId,
        NodeId                         selfId,
        int32_t                        termBufferBytes,
        int                            outboundRingBytes)
    : aeron_(std::move(aeron))
    , assembler_([this](aeron::concurrent::AtomicBuffer& buf,
                        aeron::util::index_t             offset,
                        aeron::util::index_t             length,
                        aeron::Header&                   hdr) {
          return onFragment(buf, offset, length, hdr);
      })
    , selfId_(selfId)
    , outboundRing_(outboundRingBytes)  // NodeConfig-configurable ring size
    , termMaxMessageLength_(termBufferBytes > 0
          ? aeron::concurrent::logbuffer::FrameDescriptor::computeMaxMessageLength(termBufferBytes)
          : 0)
{
    // Register ingress subscription.  One bounded synchronous wait at
    // construction — never called on the duty-cycle hot path.
    static constexpr int kResolveMaxMs  = 5000;
    static constexpr int kResolveSliceMs = 1;

    int64_t regId = aeron_->addSubscription(ingressChannel, ingressStreamId);

    using clk = std::chrono::steady_clock;
    auto deadline = clk::now() + std::chrono::milliseconds{kResolveMaxMs};
    while (clk::now() < deadline) {
        sub_ = aeron_->findSubscription(regId);
        if (sub_) break;
        std::this_thread::sleep_for(std::chrono::milliseconds{kResolveSliceMs});
    }
    if (!sub_) {
        throw std::runtime_error(
            "AeronPartitionTransport: ingress subscription not resolved within 5 s");
    }
}

// ---------------------------------------------------------------------------
// addPeer — register egress channel for a remote node (non-blocking).
// ---------------------------------------------------------------------------
void AeronPartitionTransport::addPeer(NodeId             peerId,
                                       const std::string& egressChannel,
                                       int32_t            egressStreamId)
{
    if (peers_.count(peerId)) {
        spdlog::warn("AeronPartitionTransport: addPeer called twice for peerId={}", peerId);
        return;
    }

    PeerPub p;
    p.peerId = peerId;
    try {
        p.registrationId = aeron_->addExclusivePublication(egressChannel, egressStreamId);
        p.registered     = true;
    } catch (const std::exception& ex) {
        spdlog::warn("AeronPartitionTransport: addExclusivePublication failed for "
                     "peerId={}: {}", peerId, ex.what());
        // Leave registered=false; peer will be silently dropped on send.
    }
    peers_.emplace(peerId, std::move(p));
}

// ---------------------------------------------------------------------------
// registerReplica — add a ((topicId, partitionId) → PartitionReplica*) entry.
// DR-BUG-5: composite key prevents collision when multiple topics share the
// same partition index.
// ---------------------------------------------------------------------------
void AeronPartitionTransport::registerReplica(int32_t           topicId,
                                               PartitionId       partitionId,
                                               PartitionReplica* replica)
{
    replicas_[ReplicaKey{topicId, partitionId}] = replica;
}

// ---------------------------------------------------------------------------
// sendFetchRequest — (Follower → Leader)  non-blocking
// ---------------------------------------------------------------------------
void AeronPartitionTransport::sendFetchRequest(const FetchRequestMsg& req)
{
    // Shared-nothing: encode into a per-call STACK buffer (no shared state),
    // then marshal to the control thread via the outbound ring. FetchRequest is
    // a broadcast (leader NodeId not on the wire) → kBroadcast target.
    uint8_t wire[kMaxWireFrame];
    const std::size_t len = FetchRequestMsg::encode(
        wire, 0,
        req.leaderEpoch,
        req.partition,
        req.followerId,
        req.fetchOffset,
        req.maxBytes,
        req.topicId);
    enqueueOutbound(kBroadcast, wire, len);
}

// ---------------------------------------------------------------------------
// sendFetchResponse — (Leader → Follower)  non-blocking
// ---------------------------------------------------------------------------
void AeronPartitionTransport::sendFetchResponse(NodeId                toFollower,
                                                 const FetchResponseMsg& resp)
{
    RUNE_FR_SPAN_REPLICATION(rune_fr::Site::ReplSendFetchResp, 0);
    // #2b zero-copy: encode the FetchResponse straight into the claimed outbound-ring
    // slot's OutboundFrame payload — one copy (records: log mmap → ring) instead of
    // the old encode-into-`wire` + putPayload-into-ring double copy.  Shared-nothing:
    // the worker only writes the ring; peers_/pubs stay control-owned (drainOutbound
    // offers).  Back-pressure (full ring) → false; the follower re-fetches.
    enqueueFetchResponse(toFollower, resp, /*reconcile=*/false);
}

// ---------------------------------------------------------------------------
// DR-BUG-6: OffsetsForLeaderEpoch request  (Follower → new Leader)
// ---------------------------------------------------------------------------
void AeronPartitionTransport::sendOffsetsForLeaderEpochRequest(
        NodeId toLeader, const OffsetsForLeaderEpochReqMsg& req)
{
    uint8_t wire[kMaxWireFrame];
    const std::size_t len = OffsetsForLeaderEpochReqMsg::encode(
        wire, 0,
        req.currentEpoch, req.partition, req.followerId,
        req.requestedEpoch, req.followerLeo, req.topicId);
    enqueueOutbound(toLeader, wire, len);
}

// ---------------------------------------------------------------------------
// DR-BUG-6: OffsetsForLeaderEpoch response  (Leader → Follower)
// ---------------------------------------------------------------------------
void AeronPartitionTransport::sendOffsetsForLeaderEpochResponse(
        NodeId toFollower, const OffsetsForLeaderEpochRespMsg& resp)
{
    uint8_t wire[kMaxWireFrame];
    const std::size_t len = OffsetsForLeaderEpochRespMsg::encode(
        wire, 0,
        resp.leaderEpoch, resp.partition, resp.responseEpoch,
        resp.endOffset, resp.errorCode, resp.topicId);
    enqueueOutbound(toFollower, wire, len);
}

// ---------------------------------------------------------------------------
// DR-BUG-6: reconcile request  (deficient Leader → ahead survivor)
// Reuses the FetchRequest layout, re-stamped with the reconcile templateId.
// ---------------------------------------------------------------------------
void AeronPartitionTransport::sendReconcileRequest(
        NodeId toPeer, const FetchRequestMsg& req)
{
    uint8_t wire[kMaxWireFrame];
    const std::size_t len = FetchRequestMsg::encode(
        wire, 0,
        req.leaderEpoch, req.partition, req.followerId,
        req.fetchOffset, req.maxBytes, req.topicId);
    stampTemplateId(wire, 0, kReconcileFetchReqTemplateId);
    enqueueOutbound(toPeer, wire, len);
}

// ---------------------------------------------------------------------------
// DR-BUG-6: reconcile response  (ahead survivor → deficient Leader)
// Reuses the FetchResponse layout, re-stamped with the reconcile templateId.
// ---------------------------------------------------------------------------
void AeronPartitionTransport::sendReconcileResponse(
        NodeId toPeer, const FetchResponseMsg& resp)
{
    // #2b zero-copy: encode the FetchResponse straight into the outbound-ring slot,
    // then re-stamp the reconcile templateId in place (see enqueueFetchResponse).
    enqueueFetchResponse(toPeer, resp, /*reconcile=*/true);
}

int32_t AeronPartitionTransport::maxFetchResponseBytes() const noexcept
{
    // F3 round 5, item I3: min(outbound-ring bound, Aeron term bound) — both
    // fixed at construction, so this is safe from any thread (worker or
    // control).  The term bound is what sendOnPub() enforces (pub
    // maxMessageLength = term/8); before round 5 only the ring bound was
    // used, so a term smaller than the ring could make this claim
    // "sendable" for a frame sendOnPub() then silently dropped.
    return fetchCeilingFrom(static_cast<int64_t>(outboundRing_.maxMsgLength()),
                            static_cast<int64_t>(termMaxMessageLength_));
}

// FR trace correlation via per-endpoint MONOTONIC SEQUENCE (not offset — offsets
// repeat across fast-acks and collide).  Because each ring is FIFO, the k-th
// enqueued frame is the k-th drained → ReplOutEnqueue(seq=k) pairs 1:1 with
// ReplOutDrain(seq=k); likewise ReplFragRecv(recvSeq=k) with ReplWorkerOpRun(k).
// One transport per node → file-static counters are per-process.
//   g_outEnqSeq  : atomic (enqueue* may be called from >1 worker thread).
//   g_outDrainSeq: control-thread only.   g_inRecvSeq: control-thread only.
static std::atomic<uint32_t> g_outEnqSeq{0};
static uint32_t              g_outDrainSeq = 0;
static uint32_t              g_inRecvSeq   = 0;

// ---------------------------------------------------------------------------
// enqueueFetchResponse — WORKER-thread side, ZERO-COPY (#2b).
// Claims the outbound-ring slot, then encodes the FetchResponse SBE message
// DIRECTLY into the OutboundFrame's var-data payload region — records are copied
// exactly once (log mmap → ring, via FetchResponse::putRecords), eliminating the
// intermediate `wire` buffer + the putPayload memcpy of enqueueOutbound(wire,len).
//
// Var-data layout is [uint32 LE length][payload bytes]; we write the length prefix
// exactly as OutboundFrame::putPayload would (same LE encoding), then hand the
// payload pointer to FetchResponseMsg::encode.  Uses only the codec's public
// buffer()/sbePosition() API + the documented 4-byte varDataEncoding header.
// ---------------------------------------------------------------------------
bool AeronPartitionTransport::enqueueFetchResponse(int32_t                 targetNodeId,
                                                    const FetchResponseMsg& resp,
                                                    bool                    reconcile)
{
    const std::size_t payloadLen = FetchResponseMsg::encodedSize(resp.records.size());
    const std::size_t frameLen =
        isbe::MessageHeader::encodedLength() + isbe::OutboundFrame::computeLength(payloadLen);
    // Oversized guard (identical policy to enqueueOutbound): a frame beyond the
    // ring's maxMsgLength is a caller cap bug — reject as retriable back-pressure.
    if (frameLen > outboundRing_.maxMsgLength()) {
        spdlog::error(
            "AeronPartitionTransport::enqueueFetchResponse: framed message {} B exceeds "
            "outbound ring maxMsgLength {} B (target={}); dropping (caller must cap)",
            frameLen, outboundRing_.maxMsgLength(), targetNodeId);
        ++outboundRingFullCount_;
        return false;
    }
    const aeron::util::index_t idx = outboundRing_.tryClaim(frameLen);
    if (idx < 0) {                          // full ring → back-pressure, nothing committed
        ++outboundRingFullCount_;
        return false;
    }
    char* slot = reinterpret_cast<char*>(outboundRing_.base() + idx);
    isbe::OutboundFrame enc;
    enc.wrapAndApplyHeader(slot, 0, frameLen);
    enc.targetNodeId(targetNodeId);
    // Var-data payload header + in-place FetchResponse encode.  sbePosition() sits at
    // the var-data length field once the fixed block is written.
    const std::uint64_t lenPos = enc.sbePosition();
    detail::write_le<std::uint32_t>(reinterpret_cast<uint8_t*>(slot) + lenPos,
                                    static_cast<std::uint32_t>(payloadLen));
    uint8_t* payload = reinterpret_cast<uint8_t*>(slot) + lenPos + isbe::OutboundFrame::payloadHeaderLength();
    FetchResponseMsg::encode(payload, 0,
                             resp.leaderEpoch, resp.partition, resp.highWatermark,
                             resp.logStartOffset, resp.errorCode, resp.records, resp.topicId);
    if (reconcile) {
        // DR reverse-catch-up: re-stamp the FetchResponse templateId in place so it
        // routes to the reconcile handlers (matches sendReconcileResponse's wire).
        stampTemplateId(payload, 0, kReconcileFetchRespTemplateId);
    }
    // Advance the flyweight position so the frame is well-formed (commit uses frameLen).
    enc.sbePosition(lenPos + isbe::OutboundFrame::payloadHeaderLength() + payloadLen);
    outboundRing_.commit(idx);
    RUNE_FR_MARK_REPLICATION(rune_fr::Site::ReplOutEnqueue,
                             g_outEnqSeq.fetch_add(1, std::memory_order_relaxed));
    return true;
}

// ---------------------------------------------------------------------------
// enqueueOutbound — WORKER-thread side of the shared-nothing send path.
// Frames [int32 targetNodeId][wire bytes] into a stack buffer and writes it to
// the outbound ring (agrona ManyToOneRingBuffer). No peers_/pubs access here.
// Back-pressure: on a full ring, bump the counter and return false — the send is
// skipped (replication is retriable; caller re-drives next cycle). Never blocks.
// ---------------------------------------------------------------------------

// Per-second transport rate diagnostic (control-thread only; RUNE_FS_LOG).
static bool     g_txRateLog     = [] { const char* e = std::getenv("RUNE_FS_LOG");
                                       return e == nullptr || e[0] != '0'; }();
static int64_t  g_txRateLastNs  = 0;
static int64_t  g_txPolls       = 0;   // poll() calls
static int64_t  g_txFragsRecv   = 0;   // onFragment invocations (fragments received)
static int64_t  g_txOutDrained  = 0;   // outbound messages offered to Aeron

bool AeronPartitionTransport::enqueueOutbound(int32_t        targetNodeId,
                                              const uint8_t* wire,
                                              std::size_t    len)
{
    // The outbound-ring frame is an SBE OutboundFrame envelope {targetNodeId,
    // payload=wire}, encoded WRAP-IN-PLACE straight into the claimed ring slot — no
    // intermediate buffer, no hand-rolled header/offsets, no raw memcpy (the
    // generated flyweight owns the layout; putPayload copies the already-encoded
    // replication message once).
    const std::size_t frameLen =
        isbe::MessageHeader::encodedLength() + isbe::OutboundFrame::computeLength(len);
    // Oversized guard: tryClaim rejects a message larger than the ring's maxMsgLength.
    // Callers cap the variable-length payload (the FetchResponse records tail) below
    // this bound; an oversized frame here is a caller bug — reject it (retriable).
    if (frameLen > outboundRing_.maxMsgLength()) {
        spdlog::error(
            "AeronPartitionTransport::enqueueOutbound: framed message {} B exceeds "
            "outbound ring maxMsgLength {} B (target={}); dropping (caller must cap)",
            frameLen, outboundRing_.maxMsgLength(), targetNodeId);
        ++outboundRingFullCount_;
        return false;
    }
    // tryClaim returns a NEGATIVE index on a full ring → BACK-PRESSURE: bump the
    // counter and return false WITHOUT committing (nothing claimed, no torn record).
    // The caller treats the skipped send as retriable (the follower re-fetches).
    const aeron::util::index_t idx = outboundRing_.tryClaim(frameLen);
    if (idx < 0) {
        ++outboundRingFullCount_;
        return false;
    }
    isbe::OutboundFrame enc;
    enc.wrapAndApplyHeader(reinterpret_cast<char*>(outboundRing_.base() + idx), 0, frameLen);
    enc.targetNodeId(targetNodeId);
    enc.putPayload(reinterpret_cast<const char*>(wire), static_cast<std::uint32_t>(len));
    outboundRing_.commit(idx);
    // Worker-clock: frame handed to the outbound ring (→ control thread drains it).
    // corr = monotonic enqueue seq; pairs 1:1 with ReplOutDrain (FIFO ring).
    RUNE_FR_MARK_REPLICATION(rune_fr::Site::ReplOutEnqueue,
                             g_outEnqSeq.fetch_add(1, std::memory_order_relaxed));
    return true;
}

// ---------------------------------------------------------------------------
// drainOutbound — CONTROL-thread side. Reads framed outbound messages and does
// the real peer offers. ONLY this path (control thread) touches peers_/pubs on
// the send side, preserving shared-nothing.
// ---------------------------------------------------------------------------
int AeronPartitionTransport::drainOutbound()
{
    RUNE_FR_SPAN_REPLICATION(rune_fr::Site::ReplOutboundDrain, 0);
    auto handler = [this](std::int32_t /*msgTypeId*/,
                          aeron::concurrent::AtomicBuffer& buf,
                          aeron::util::index_t offset,
                          aeron::util::index_t length) {
        // Decode the SBE OutboundFrame envelope: targetNodeId + a ZERO-COPY payload
        // view into the ring frame (no memcpy, no hand-rolled offsets).
        char* fbase = reinterpret_cast<char*>(buf.buffer()) + offset;
        isbe::MessageHeader mh;
        mh.wrap(fbase, 0, 0, static_cast<std::uint64_t>(length));
        isbe::OutboundFrame dec;
        dec.wrapForDecode(fbase, isbe::MessageHeader::encodedLength(),
                          mh.blockLength(), mh.version(),
                          static_cast<std::uint64_t>(length));
        const int32_t target = dec.targetNodeId();
        const std::string_view pv = dec.getPayloadAsStringView();
        const uint8_t* wire = reinterpret_cast<const uint8_t*>(pv.data());
        const std::size_t wlen = pv.size();
        // Control-clock: frame drained from the outbound ring + offered to peer(s).
        // corr = monotonic drain seq; k-th drain == k-th enqueue (FIFO) → the gap
        // ReplOutEnqueue(k)→ReplOutDrain(k) is the EXACT worker→control handoff.
        RUNE_FR_MARK_REPLICATION(rune_fr::Site::ReplOutDrain, g_outDrainSeq++);
        ++g_txOutDrained;

        if (target == kBroadcast) {
            for (auto& [nodeId, peer] : peers_) {
                if (peer.pub && peer.pub->isConnected())
                    sendOnPub(*peer.pub, wire, wlen);
            }
        } else {
            PeerPub* peer = findPeer(target);
            if (peer && peer->pub && peer->pub->isConnected())
                sendOnPub(*peer->pub, wire, wlen);
        }
    };
    // Mirror the ConsensusAgent pattern: return the count of ring messages
    // drained so poll()/doWork() folds it into workCount and the AgentRunner's
    // IdleStrategy idles only when there was genuinely no work.
    return outboundRing_.drain(handler, /*limit=*/kFragmentLimit);
}

// ---------------------------------------------------------------------------
// poll — duty-cycle step: resolve pubs + drain ingress subscription.
// ---------------------------------------------------------------------------
int AeronPartitionTransport::poll(int64_t nowNs)
{
    RUNE_FR_SPAN_REPLICATION(rune_fr::Site::ReplIngressPoll, 0);
    nowNs_ = nowNs;

    // Non-blocking probe: try to resolve pending publications.
    resolvePendingPubs();

    // Shared-nothing: drain worker-marshalled outbound sends and do the real
    // peer offers here on the control thread (the only thread that touches
    // peers_/pubs). ([[rune-shared-nothing-law]]) Count folds into work so the
    // AgentRunner's IdleStrategy sees outbound activity as work (no idle).
    int work = drainOutbound();

    // Drain ingress subscription with a CONTROLLED poll so onFragment can apply
    // back-pressure: a fragment whose target worker ring is full returns ABORT and
    // is re-delivered next cycle (not dropped).  See onFragment.
    const int fragged = sub_->controlledPoll(assembler_.handler(), kFragmentLimit);
    work += fragged;

    if (g_txRateLog) {
        ++g_txPolls;
        if (g_txRateLastNs == 0) g_txRateLastNs = nowNs;
        else if (nowNs - g_txRateLastNs >= 1'000'000'000LL) {
            spdlog::info("TXRATE[node={}] polls/s={} fragsRecv/s={} outDrained/s={}",
                         selfId_, g_txPolls, g_txFragsRecv, g_txOutDrained);
            g_txRateLastNs = nowNs; g_txPolls = 0; g_txFragsRecv = 0; g_txOutDrained = 0;
        }
    }
    return work;
}

// ---------------------------------------------------------------------------
// onFragment — called by FragmentAssembler for each reassembled frame.
// ---------------------------------------------------------------------------
aeron::ControlledPollAction AeronPartitionTransport::onFragment(
    aeron::concurrent::AtomicBuffer& buffer,
    aeron::util::index_t             offset,
    aeron::util::index_t             length,
    aeron::Header&                   /*header*/)
{
    using Action = aeron::ControlledPollAction;
    ++g_txFragsRecv;
    RUNE_FR_SPAN_REPLICATION(rune_fr::Site::ReplOnFragment, 0);
    // CONTINUE = consume this fragment and keep polling (success / skip malformed).
    // ABORT    = leave this fragment UNCONSUMED and stop the poll — Aeron re-delivers
    //            it next cycle.  Returned ONLY when a target worker ring is full, so
    //            the frame is never dropped: back-pressure propagates to the sender.
    const auto* buf = reinterpret_cast<const uint8_t*>(buffer.buffer() + offset);
    const auto  len = static_cast<std::size_t>(length);

    // Minimum frame: 8-byte SBE header.
    if (len < ReplicationMessageHeader::ENCODED_SIZE) {
        spdlog::trace("AeronPartitionTransport: short frame ({} bytes) dropped", len);
        return Action::CONTINUE;
    }

    // Validate schema id.
    const auto hdr = ReplicationMessageHeader::decode(buf, 0);
    if (hdr.schemaId != kSchemaId) {
        spdlog::trace("AeronPartitionTransport: unknown schemaId={} dropped", hdr.schemaId);
        return Action::CONTINUE;
    }

    const uint16_t tid = hdr.templateId;

    if (tid == FetchRequestMsg::TEMPLATE_ID) {
        // Minimum size check.
        if (len < FetchRequestMsg::TOTAL_SIZE) {
            spdlog::trace("AeronPartitionTransport: FetchRequest truncated ({} bytes)", len);
            return Action::CONTINUE;
        }
        FetchRequestMsg req = FetchRequestMsg::decode(buf, 0);

        // Route to the replica owning (topicId, partition).
        // DR-BUG-5: composite key prevents collision across topics that share
        // the same partition index.
        auto it = replicas_.find(ReplicaKey{req.topicId, req.partition});
        if (it == replicas_.end() || it->second == nullptr) {
            spdlog::trace("AeronPartitionTransport: FetchRequest for unknown (topicId={}, partition={})",
                          req.topicId, req.partition);
            return Action::CONTINUE;
        }

        // GR-1 fix: cap req.maxBytes to the deliverable ceiling for this follower.
        //
        // The leader reads up to req.maxBytes from the log and encodes them into
        // a single FetchResponse.  Aeron's offer() throws IllegalArgumentException
        // if the message exceeds pub.maxMessageLength() = termLength/8.  With the
        // production 64 KiB term this is 8192 bytes; uncapped responses of up to
        // 1 MiB would throw on every send and livelock the follower at LEO=0.
        //
        // kFetchResponseOverhead = the always-present framing (SBE header + fixed
        // block + records length-prefix), from the GENERATED codec — the remaining
        // budget is for records.  encodedSize(0) = the zero-records frame size.
        const int32_t kFetchResponseOverhead =
            static_cast<int32_t>(FetchResponseMsg::encodedSize(0));

        // F3 round 5: the unconditional base IS maxFetchResponseBytes() (ring
        // AND term bound) — the same function BrokerNode's derivation uses.
        // The records tail must survive TWO framing hops without exceeding either
        // transport bound:
        //   1. enqueueOutbound writes [int32 targetNodeId][SBE frame] into the
        //      worker→control outbound ring, whose agrona maxMsgLength (= capacity/8)
        //      THROWS on an oversized write — it is NOT retriable back-pressure.
        //   2. drainOutbound offers the SBE frame on the follower publication, whose
        //      maxMessageLength (= termLength/8) rejects oversized offers.
        // The outbound-ring bound is ALWAYS known (independent of pub resolution), so
        // cap to it UNCONDITIONALLY, then tighten with the pub bound once resolved.
        // Previously the cap was gated on followerPeer->pub AND ignored the ring bound
        // + the 4-byte outbound header, so an early/large fetch slipped through and
        // threw inside the worker's onFetchRequest — the fetch was lost.
        int32_t ceiling = maxFetchResponseBytes();
        auto* followerPeer = findPeer(req.followerId);
        if (followerPeer && followerPeer->pub) {
            const int32_t pubCeiling =
                static_cast<int32_t>(followerPeer->pub->maxMessageLength())
                - kFetchResponseOverhead;
            if (pubCeiling < ceiling) ceiling = pubCeiling;
        }
        if (ceiling < 0) ceiling = 0;
        if (req.maxBytes > ceiling) req.maxBytes = ceiling;

        // E5.1.2c: route leader-side FetchRequest to the owning worker so
        // leaderState_/isrManager_ is single-owner (worker thread only).
        // Internal partitions (topicId<0) remain inline (node-thread-owned).
        if (workerPool_ && req.topicId >= 0) {
            rune::node::WorkerOp op;
            op.type                              = rune::node::WorkerOp::Type::FetchRequest;
            op.topicId                           = req.topicId;
            op.partition                         = req.partition;
            op.replica                           = it->second;
            op.logPtr                            = nullptr;
            op.fetchRequestPayload.leaderEpoch   = req.leaderEpoch;
            op.fetchRequestPayload.partition     = req.partition;
            op.fetchRequestPayload.followerId    = req.followerId;
            op.fetchRequestPayload.fetchOffset   = req.fetchOffset;
            op.fetchRequestPayload.maxBytes      = req.maxBytes;
            op.fetchRequestPayload.topicId       = req.topicId;
            op.fetchRequestPayload.nowNs         = nowNs_;
            if (!workerPool_->tryEnqueueOp(req.topicId, req.partition, std::move(op))) {
                // Worker ring full → back-pressure: leave the fragment unconsumed and
                // retry next cycle (the follower's fetch is served once the worker
                // drains).  No drop.
                return Action::ABORT;
            }
            // Control-clock: fetch op enqueued to the worker ring (corr=recvSeq).
            // Pairs 1:1 (FIFO) with ReplWorkerOpRun when the worker dequeues it →
            // the gap is the EXACT control→worker handoff latency on this node.
            RUNE_FR_MARK_REPLICATION(rune_fr::Site::ReplFragRecv, g_inRecvSeq++);
        } else {
            it->second->onFetchRequest(req, nowNs_);
        }

    } else if (tid == FetchResponseMsg::TEMPLATE_ID) {
        // Minimum valid frame = the zero-records framing (SBE header + block +
        // records length-prefix), from the generated codec.
        const std::size_t kMinResponseSize = FetchResponseMsg::encodedSize(0);
        if (len < kMinResponseSize) {
            spdlog::trace("AeronPartitionTransport: FetchResponse truncated ({} bytes)", len);
            return Action::CONTINUE;
        }
        // Zero-copy decode: FetchResponseMsg::records is a span into `buf`
        // which lives in the Aeron term buffer for the duration of this callback.
        // We call applyReplicated() synchronously within the callback, so the
        // span is valid.  applyReplicated() copies the bytes into the local Log.
        // Pass `len` so decode() can clamp the records span against the actual
        // fragment size (defence against malformed length prefix).
        const FetchResponseMsg resp = FetchResponseMsg::decode(buf, 0, len);

        // Route to the replica owning (topicId, partition).
        // DR-BUG-5: composite key prevents collision across topics.
        auto it = replicas_.find(ReplicaKey{resp.topicId, resp.partition});
        if (it == replicas_.end() || it->second == nullptr) {
            spdlog::trace("AeronPartitionTransport: FetchResponse for unknown (topicId={}, partition={})",
                          resp.topicId, resp.partition);
            return Action::CONTINUE;
        }
        // E2 inc2a: when a WorkerPool is wired, route the applyReplicated call
        // through the owning worker's SPSC ingress ring instead of calling it
        // inline within the Aeron fragment callback.
        //
        // BUFFER LIFETIME: FetchResponseMsg::records is a span into the Aeron
        // term buffer, which is ONLY valid inside this callback.  We COPY the
        // record bytes into a std::vector<uint8_t> owned by the WorkerOp at
        // enqueue time (before we return from the callback) — this is the
        // mandated approach from the inc2a brief.
        // Shared-nothing control/data split ([[rune-shared-nothing-law]]):
        // route inbound to the owning WORKER only for USER partitions (topicId>=0);
        // INTERNAL partitions (topicId<0) are control-thread-driven, so apply
        // inline here (routing them to a worker would race the control thread's
        // NodeRuntimeAgent doWork on the same internal replica).
        if (workerPool_ && resp.topicId >= 0) {
            // F3 round 5, item I1: records that can never fit the owning
            // worker's ingress ring must be DROPPED (the follower re-fetches;
            // BrokerNode caps fetch maxBytes to the worker ring), never
            // ABORT-retried forever and never thrown out of tryClaim().
            if (!workerPool_->fitsIngressTail(resp.topicId, resp.partition,
                                              resp.records.size())) [[unlikely]] {
                if (unfittableApplyDrops_++ % 1024 == 0) {
                    spdlog::error("AeronPartitionTransport: FetchResponse records {} B do not fit "
                                  "the worker ingress ring (topicId={} partition={}); dropped "
                                  "(drops={})", resp.records.size(), resp.topicId,
                                  resp.partition, unfittableApplyDrops_);
                }
                return Action::CONTINUE;
            }
            rune::node::WorkerOp op;
            op.type                              = rune::node::WorkerOp::Type::ApplyReplicated;
            op.topicId                           = resp.topicId;
            op.partition                         = resp.partition;
            op.replica                           = it->second;
            op.logPtr                            = nullptr; // not needed for ApplyReplicated
            op.replicatedPayload.leaderEpoch     = resp.leaderEpoch;
            op.replicatedPayload.partition       = resp.partition;
            op.replicatedPayload.highWatermark   = resp.highWatermark;
            op.replicatedPayload.logStartOffset  = resp.logStartOffset;
            op.replicatedPayload.errorCode       = resp.errorCode;
            op.replicatedPayload.topicId         = resp.topicId;
            // Copy records span bytes into an owned vector (buffer lifetime).
            if (!resp.records.empty()) {
                op.replicatedPayload.records.assign(
                    resp.records.data(),
                    resp.records.data() + resp.records.size());
            }
            if (!workerPool_->tryEnqueueOp(resp.topicId, resp.partition,
                                           std::move(op))) {
                // Worker ring full → back-pressure: leave unconsumed, retry next cycle.
                // The records span still lives in the term buffer (fragment not
                // consumed), so the re-delivered fragment re-copies correctly. No drop.
                return Action::ABORT;
            }
            // Control-clock: apply op enqueued to the worker ring (corr=recvSeq).
            RUNE_FR_MARK_REPLICATION(rune_fr::Site::ReplFragRecv, g_inRecvSeq++);
        } else {
            // INLINE path (no WorkerPool): call applyReplicated synchronously.
            it->second->applyReplicated(resp);
        }

    } else if (tid == OffsetsForLeaderEpochReqMsg::TEMPLATE_ID) {
        // DR-BUG-6: KIP-101 epoch-boundary query → leader answers.
        if (len < OffsetsForLeaderEpochReqMsg::TOTAL_SIZE) return Action::CONTINUE;
        auto req = OffsetsForLeaderEpochReqMsg::decode(buf, 0);
        auto it = replicas_.find(ReplicaKey{req.topicId, req.partition});
        if (it == replicas_.end() || it->second == nullptr) return Action::CONTINUE;
        // E5.1.2c: route to owning worker (touches leaderState_/isrManager_).
        if (workerPool_ && req.topicId >= 0) {
            rune::node::WorkerOp op;
            op.type                              = rune::node::WorkerOp::Type::OffsetsForLeaderEpochReq;
            op.topicId                           = req.topicId;
            op.partition                         = req.partition;
            op.replica                           = it->second;
            op.logPtr                            = nullptr;
            op.offsEpochReqPayload.currentEpoch   = req.currentEpoch;
            op.offsEpochReqPayload.partition      = req.partition;
            op.offsEpochReqPayload.followerId     = req.followerId;
            op.offsEpochReqPayload.requestedEpoch = req.requestedEpoch;
            op.offsEpochReqPayload.followerLeo    = req.followerLeo;
            op.offsEpochReqPayload.topicId        = req.topicId;
            if (!workerPool_->tryEnqueueOp(req.topicId, req.partition, std::move(op))) {
                // Worker ring full → back-pressure: leave unconsumed, retry next cycle.
                return Action::ABORT;
            }
        } else {
            it->second->onOffsetsForLeaderEpochRequest(req);
        }

    } else if (tid == OffsetsForLeaderEpochRespMsg::TEMPLATE_ID) {
        // DR-BUG-6: KIP-101 epoch-boundary answer → follower truncates.
        if (len < OffsetsForLeaderEpochRespMsg::TOTAL_SIZE) return Action::CONTINUE;
        auto resp = OffsetsForLeaderEpochRespMsg::decode(buf, 0);
        auto it = replicas_.find(ReplicaKey{resp.topicId, resp.partition});
        if (it == replicas_.end() || it->second == nullptr) return Action::CONTINUE;
        it->second->applyOffsetsForLeaderEpochResponse(resp);

    } else if (tid == kReconcileFetchReqTemplateId) {
        // DR-BUG-6: reverse pull request (deficient leader → survivor).  Reuses
        // the FetchRequest layout.  Cap maxBytes to the requesting peer's
        // deliverable ceiling (same rationale as the normal FetchRequest path).
        if (len < FetchRequestMsg::TOTAL_SIZE) return Action::CONTINUE;
        FetchRequestMsg req = FetchRequestMsg::decode(buf, 0);
        auto it = replicas_.find(ReplicaKey{req.topicId, req.partition});
        if (it == replicas_.end() || it->second == nullptr) return Action::CONTINUE;
        // Cap to the outbound-ring bound UNCONDITIONALLY (same two-hop framing
        // rationale as the normal FetchRequest path), tightened by the pub bound when
        // resolved, minus the outbound header — so the reconcile response never
        // overflows the outbound ring and throws inside the worker.
        const int32_t kOverhead = static_cast<int32_t>(FetchResponseMsg::encodedSize(0));
        int32_t ceiling = maxFetchResponseBytes();   // F3 r5: ring AND term bound
        auto* peer = findPeer(req.followerId);
        if (peer && peer->pub) {
            const int32_t pubCeiling =
                static_cast<int32_t>(peer->pub->maxMessageLength()) - kOverhead;
            if (pubCeiling < ceiling) ceiling = pubCeiling;
        }
        if (ceiling < 0) ceiling = 0;
        if (req.maxBytes > ceiling) req.maxBytes = ceiling;
        it->second->onReconcileRequest(req);

    } else if (tid == kReconcileFetchRespTemplateId) {
        // DR-BUG-6: reverse pull response (survivor → deficient leader).  Reuses
        // the FetchResponse layout.
        const std::size_t kMinResp = FetchResponseMsg::encodedSize(0);
        if (len < kMinResp) return Action::CONTINUE;
        const FetchResponseMsg resp = FetchResponseMsg::decode(buf, 0, len);
        auto it = replicas_.find(ReplicaKey{resp.topicId, resp.partition});
        if (it == replicas_.end() || it->second == nullptr) return Action::CONTINUE;
        // E5.1.2c: route to owning worker (applyReconcileResponse touches
        // leaderState_.setLeaderLogEndOffset + recomputeHw).
        // Copy records bytes before returning from the Aeron callback (buffer lifetime).
        if (workerPool_ && resp.topicId >= 0) {
            if (!workerPool_->fitsIngressTail(resp.topicId, resp.partition,
                                              resp.records.size())) [[unlikely]] {
                if (unfittableApplyDrops_++ % 1024 == 0) {
                    spdlog::error("AeronPartitionTransport: reconcile response records {} B do "
                                  "not fit the worker ingress ring (topicId={} partition={}); "
                                  "dropped (drops={})", resp.records.size(), resp.topicId,
                                  resp.partition, unfittableApplyDrops_);
                }
                return Action::CONTINUE;
            }
            rune::node::WorkerOp op;
            op.type                              = rune::node::WorkerOp::Type::ReconcileFetchResp;
            op.topicId                           = resp.topicId;
            op.partition                         = resp.partition;
            op.replica                           = it->second;
            op.logPtr                            = nullptr;
            op.replicatedPayload.leaderEpoch     = resp.leaderEpoch;
            op.replicatedPayload.partition       = resp.partition;
            op.replicatedPayload.highWatermark   = resp.highWatermark;
            op.replicatedPayload.logStartOffset  = resp.logStartOffset;
            op.replicatedPayload.errorCode       = resp.errorCode;
            op.replicatedPayload.topicId         = resp.topicId;
            if (!resp.records.empty()) {
                op.replicatedPayload.records.assign(
                    resp.records.data(),
                    resp.records.data() + resp.records.size());
            }
            if (!workerPool_->tryEnqueueOp(resp.topicId, resp.partition, std::move(op))) {
                // Worker ring full → back-pressure: leave unconsumed, retry next cycle.
                return Action::ABORT;
            }
        } else {
            it->second->applyReconcileResponse(resp);
        }

    } else {
        spdlog::trace("AeronPartitionTransport: unknown templateId={} dropped", tid);
    }
    return Action::CONTINUE;
}

// ---------------------------------------------------------------------------
// resolvePendingPubs — non-blocking: one findExclusivePublication per unresolved peer.
// ---------------------------------------------------------------------------
void AeronPartitionTransport::resolvePendingPubs()
{
    for (auto& [nodeId, peer] : peers_) {
        if (peer.pub) continue;          // already resolved
        if (!peer.registered) continue;  // registration failed at addPeer time

        try {
            auto pub = aeron_->findExclusivePublication(peer.registrationId);
            if (pub) {
                peer.pub = std::move(pub);
                spdlog::debug("AeronPartitionTransport: pub resolved for peerId={}", peer.peerId);
            }
        } catch (const std::exception& ex) {
            // Conductor signalled an error (bad channel, closed, etc.).
            // Mark as unregistered so we don't keep probing a dead registration.
            spdlog::warn("AeronPartitionTransport: findExclusivePublication threw for "
                         "peerId={}: {}; dropping peer", peer.peerId, ex.what());
            peer.registered = false;
        }
    }
}

// ---------------------------------------------------------------------------
// sendOnPub — non-blocking zero-copy send (tryClaim → commit; fallback offer).
// ---------------------------------------------------------------------------
bool AeronPartitionTransport::sendOnPub(aeron::ExclusivePublication& pub,
                                         const uint8_t*               buf,
                                         std::size_t                  len)
{
    // Defence-in-depth: NEVER let offer() throw IllegalArgumentException on an
    // oversized frame.  After fixes (1)+(3) this branch must never be reached in
    // normal operation — req.maxBytes is capped to the deliverable ceiling before
    // onFetchRequest() reads the log, and the whole-batch alignment in
    // onFetchRequest() ensures the encoded response stays within that ceiling.
    // If somehow it fires, DROP + warn (the follower will retry on next fetch).
    //
    // Note: a single very large RecordBatch (e.g. value field close to INT16_MAX)
    // could still exceed maxMessageLength with the production 64 KiB term
    // (maxMessageLength = 8192).  In that case the replication term must be sized
    // to accommodate the largest possible batch.  The guard here prevents a crash
    // regardless — the batch simply cannot be replicated until the term is resized.
    if (len > static_cast<std::size_t>(pub.maxMessageLength())) {
        spdlog::warn("AeronPartitionTransport::sendOnPub: drop oversized frame "
                     "len={} maxMessageLength={} — fetch ceiling not applied correctly",
                     len, pub.maxMessageLength());
        return false;
    }

    auto* mutableBuf = const_cast<uint8_t*>(buf);
    aeron::concurrent::AtomicBuffer atomicBuf(
        mutableBuf, static_cast<aeron::util::index_t>(len));

    // Prefer tryClaim for zero-copy (works when len <= MTU).
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

    // Fallback: offer (copies into term buffer).
    const int64_t result = pub.offer(atomicBuf, 0,
                                      static_cast<aeron::util::index_t>(len));
    return result > 0;
}

// ---------------------------------------------------------------------------
// findPeer
// ---------------------------------------------------------------------------
AeronPartitionTransport::PeerPub* AeronPartitionTransport::findPeer(NodeId id)
{
    auto it = peers_.find(id);
    if (it == peers_.end()) return nullptr;
    return &it->second;
}

// ---------------------------------------------------------------------------
// unresolvedPeerCount
// ---------------------------------------------------------------------------
int AeronPartitionTransport::unresolvedPeerCount() const noexcept
{
    int count = 0;
    for (const auto& [id, peer] : peers_) {
        if (!peer.pub) ++count;
    }
    return count;
}

} // namespace rune::replication
