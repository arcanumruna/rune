// AeronPartitionTransport.cpp — GR-1 implementation.
//
// See AeronPartitionTransport.h for design notes.
// Single-writer. No locks. No hot-path alloc.

#include <rune/replication/AeronPartitionTransport.h>
#include <rune/replication/ReplicationMessages.h>
#include <rune/replication/ReplicationFrameLimits.h>
#include <rune/protocol/RecordBatch.h>
#include <rune_fr/FlightRecorder.h>
#include <rune_protocol_internal_sbe/MessageHeader.h>
#include <rune_protocol_internal_sbe/OutboundFrame.h>

// E2 inc2a: WorkerPool + WorkerOp for SPSC ApplyReplicated routing.
// Included here (not in the header) to avoid pulling consensus/spdlog headers
// into consumers of rune_replication_aeron that don't need them.
// rune_replication_aeron has RUNE_NODE_INCLUDE set in its CMakeLists.txt.
#include <rune/node/ReplicaRegistry.h>
#include <rune/node/WorkerPool.h>
#include <rune/node/WorkerOp.h>

#include <cassert>
#include <cstdlib>
#include <spdlog/spdlog.h>

#include <chrono>
#include <stdexcept>
#include <thread>
#include <cstring>

namespace rune::replication {

namespace isbe = ::rune::protocol::internal::sbe;

// kOutboundEnvelopeOverhead (ReplicationFrameLimits.h): the OutboundFrame envelope
// overhead, from the generated codec — the records tail must be capped so the whole
// envelope fits the outbound ring's maxMsgLength.  Shared with the produce intake
// limit (NodeConfig::produceIntakeLimit) and maxFetchRecordsBytes().

// ---------------------------------------------------------------------------
// Constructor
// ---------------------------------------------------------------------------
AeronPartitionTransport::AeronPartitionTransport(
        std::shared_ptr<aeron::Aeron> aeron,
        const std::string&             ingressChannel,
        int32_t                        ingressStreamId,
        NodeId                         selfId,
        int                            outboundRingBytes,
        rune::aeronutil::AeronPump     pump)
    : aeron_(std::move(aeron))
    , selfId_(selfId)
    , outboundRing_(outboundRingBytes)  // NodeConfig-configurable ring size
{
    // Register ingress subscription.  One bounded synchronous wait at
    // construction — never called on the duty-cycle hot path.
    static constexpr int kResolveMaxMs = 5000;

    int64_t regId = aeron_->addSubscription(ingressChannel, ingressStreamId);
    sub_ = pump.resolve([&] { return aeron_->findSubscription(regId); },
                        std::chrono::milliseconds{kResolveMaxMs});
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
    // F27 F1: the peer's held FIFO, sized to one full outbound ring (>= 8 max-size
    // frames), allocated once here (cold) and never reallocated.
    // R3: an empty FIFO must hold any ring frame (capacity >= maxMsgLength).
    assert(outboundRing_.capacity() >= outboundRing_.maxMsgLength() + 8);
    holdRouter_.addPeer(peerId, outboundRing_.capacity());
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
    if (registry_) return;   // the registry is the source of truth (use-time resolve)
    replicas_[ReplicaKey{topicId, partitionId}] = replica;
}

void AeronPartitionTransport::unregisterReplica(int32_t topicId, PartitionId partitionId)
{
    replicas_.erase(ReplicaKey{topicId, partitionId});
}

PartitionReplica* AeronPartitionTransport::resolveReplica_(int32_t topicId,
                                                           PartitionId partitionId) const noexcept
{
    if (registry_) {
        if (topicId >= 0 && !registryUserPartitions_) return nullptr;   // on per-worker planes
        return registry_->lookup(topicId, static_cast<int32_t>(partitionId));
    }
    auto it = replicas_.find(ReplicaKey{topicId, partitionId});
    return it == replicas_.end() ? nullptr : it->second;
}

bool AeronPartitionTransport::deliverFrameForTest(const uint8_t* frame, std::size_t len, int64_t nowNs)
{
    nowNs_ = nowNs;
    return handleFrame_(frame, len) != aeron::ControlledPollAction::ABORT;
}

// ---------------------------------------------------------------------------
// maxFetchRecordsBytes — F18.  WORKER-SAFE: reads only the outbound ring's
// maxMsgLength (fixed at construction) and pubMaxMessageBytes_ (set once before
// any worker runs) — never the control-owned peers_/pubs.
// ---------------------------------------------------------------------------
int64_t AeronPartitionTransport::maxFetchRecordsBytes() const noexcept
{
    const int64_t kFrameFixed = static_cast<int64_t>(FetchResponseMsg::encodedSize(0));
    int64_t cap = static_cast<int64_t>(outboundRing_.maxMsgLength())
                - static_cast<int64_t>(kOutboundEnvelopeOverhead) - kFrameFixed;
    if (pubMaxMessageBytes_ > 0) {
        cap = std::min<int64_t>(cap, pubMaxMessageBytes_ - kFrameFixed);
    }
    return std::max<int64_t>(0, cap);
}

// ---------------------------------------------------------------------------
// sendFetchRequest — (Follower → Leader)  non-blocking
// ---------------------------------------------------------------------------
bool AeronPartitionTransport::sendFetchRequest(const FetchRequestMsg& req)
{
    // Shared-nothing: encode into a per-call STACK buffer (no shared state),
    // then marshal to the control thread via the outbound ring. FetchRequest is
    // a broadcast (leader NodeId not on the wire) → kBroadcast target.
    // F18 (B-1): never ask for more than this node's worker can apply in ONE op
    // (user partitions are applied via the worker ingress ring; immutable cap).
    int32_t maxBytes = req.maxBytes;
    if (applyRecordsCap_ > 0 && req.topicId >= 0 && maxBytes > applyRecordsCap_)
        maxBytes = static_cast<int32_t>(applyRecordsCap_);
    uint8_t wire[kMaxWireFrame];
    const std::size_t len = FetchRequestMsg::encode(
        wire, 0,
        req.leaderEpoch,
        req.partition,
        req.followerId,
        req.fetchOffset,
        maxBytes,
        req.topicId);
    return enqueueOutbound(kBroadcast, wire, len);
}

// ---------------------------------------------------------------------------
// sendFetchResponse — (Leader → Follower)  non-blocking
// ---------------------------------------------------------------------------
bool AeronPartitionTransport::sendFetchResponse(NodeId                toFollower,
                                                 const FetchResponseMsg& resp)
{
    RUNE_FR_SPAN_REPLICATION(rune_fr::Site::ReplSendFetchResp, 0);
    // #2b zero-copy: encode the FetchResponse straight into the claimed outbound-ring
    // slot's OutboundFrame payload — one copy (records: log mmap → ring) instead of
    // the old encode-into-`wire` + putPayload-into-ring double copy.  Shared-nothing:
    // the worker only writes the ring; peers_/pubs stay control-owned (drainOutbound
    // offers).  Back-pressure (full ring) → false; the follower re-fetches.
    return enqueueFetchResponse(toFollower, resp, /*reconcile=*/false);
}

// ---------------------------------------------------------------------------
// DR-BUG-6: OffsetsForLeaderEpoch request  (Follower → new Leader)
// ---------------------------------------------------------------------------
bool AeronPartitionTransport::sendOffsetsForLeaderEpochRequest(
        NodeId toLeader, const OffsetsForLeaderEpochReqMsg& req)
{
    uint8_t wire[kMaxWireFrame];
    const std::size_t len = OffsetsForLeaderEpochReqMsg::encode(
        wire, 0,
        req.currentEpoch, req.partition, req.followerId,
        req.requestedEpoch, req.followerLeo, req.topicId);
    return enqueueOutbound(toLeader, wire, len);
}

// ---------------------------------------------------------------------------
// DR-BUG-6: OffsetsForLeaderEpoch response  (Leader → Follower)
// ---------------------------------------------------------------------------
bool AeronPartitionTransport::sendOffsetsForLeaderEpochResponse(
        NodeId toFollower, const OffsetsForLeaderEpochRespMsg& resp)
{
    uint8_t wire[kMaxWireFrame];
    const std::size_t len = OffsetsForLeaderEpochRespMsg::encode(
        wire, 0,
        resp.leaderEpoch, resp.partition, resp.responseEpoch,
        resp.endOffset, resp.errorCode, resp.topicId);
    return enqueueOutbound(toFollower, wire, len);
}

// ---------------------------------------------------------------------------
// DR-BUG-6: reconcile request  (deficient Leader → ahead survivor)
// Reuses the FetchRequest layout, re-stamped with the reconcile templateId.
// ---------------------------------------------------------------------------
bool AeronPartitionTransport::sendReconcileRequest(
        NodeId toPeer, const FetchRequestMsg& req)
{
    // F18 (B-3): the reverse pull lands on THIS node's worker ingress ring exactly
    // like a follower fetch reply — cap it to our own apply frame (B-1), else the
    // survivor's multi-batch reply (bounded only by ITS frame) is refused on every
    // retry and reconcile never completes.
    int32_t maxBytes = req.maxBytes;
    if (applyRecordsCap_ > 0 && req.topicId >= 0 && maxBytes > applyRecordsCap_)
        maxBytes = static_cast<int32_t>(applyRecordsCap_);
    uint8_t wire[kMaxWireFrame];
    const std::size_t len = FetchRequestMsg::encode(
        wire, 0,
        req.leaderEpoch, req.partition, req.followerId,
        req.fetchOffset, maxBytes, req.topicId);
    stampTemplateId(wire, 0, kReconcileFetchReqTemplateId);
    return enqueueOutbound(toPeer, wire, len);
}

// ---------------------------------------------------------------------------
// DR-BUG-6: reconcile response  (ahead survivor → deficient Leader)
// Reuses the FetchResponse layout, re-stamped with the reconcile templateId.
// ---------------------------------------------------------------------------
bool AeronPartitionTransport::sendReconcileResponse(
        NodeId toPeer, const FetchResponseMsg& resp)
{
    // #2b zero-copy: encode the FetchResponse straight into the outbound-ring slot,
    // then re-stamp the reconcile templateId in place (see enqueueFetchResponse).
    return enqueueFetchResponse(toPeer, resp, /*reconcile=*/true);
}

// FR trace correlation (F9a: no file-static / atomic sequence counters).
//   Outbound (worker → control, many-to-one ring): ReplOutEnqueue and
//   ReplOutDrain are both marked with the record's RING SLOT OFFSET (the
//   tryClaim() index == the offset drain() hands the handler).  A slot is reused
//   only after the consumer passed it, so an in-flight enqueue/drain pair is
//   unique by slot — exact pairing even with several producer workers (the old
//   shared fetch_add counter needed an atomic and could interleave with claims).
//   Inbound (control → worker): ReplFragRecv uses inRecvSeq_, a per-transport
//   control-thread-owned counter (was a file static shared by every in-process
//   node's control thread); pairs FIFO with the worker's ReplWorkerOpRun seq.

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
    // F3 round 6: unreachable by construction — every FetchResponse/push is
    // capped at maxFetchRecordsBytes() (which includes this envelope) before it
    // gets here.  This runs on ANY worker thread (multi-producer), so it keeps
    // no counter (no shared state); the log is the backstop.
    if (frameLen > outboundRing_.maxMsgLength()) {
        spdlog::error(
            "AeronPartitionTransport::enqueueFetchResponse: framed message {} B exceeds "
            "outbound ring maxMsgLength {} B (target={}); dropping (caller must cap)",
            frameLen, outboundRing_.maxMsgLength(), targetNodeId);
        return false;
    }
    const aeron::util::index_t idx = outboundRing_.tryClaim(frameLen);
    if (idx < 0) {                          // full ring → back-pressure, nothing committed
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
    // FR mark BEFORE commit: once committed the consumer may drain + free the slot
    // and a later claim may reuse it, so marking after would mis-pair the slot.
    RUNE_FR_MARK_REPLICATION(rune_fr::Site::ReplOutEnqueue, static_cast<uint32_t>(idx));
    outboundRing_.commit(idx);
    return true;
}

// ---------------------------------------------------------------------------
// enqueueOutbound — WORKER-thread side of the shared-nothing send path.
// Frames [int32 targetNodeId][wire bytes] into a stack buffer and writes it to
// the outbound ring (agrona ManyToOneRingBuffer). No peers_/pubs access here.
// Back-pressure: on a full ring, bump the counter and return false — the send is
// skipped (replication is retriable; caller re-drives next cycle). Never blocks.
// ---------------------------------------------------------------------------

// Per-second transport rate diagnostic (RUNE_FS_LOG).  The enable flag is
// immutable after static init; the counters are per-transport control-thread
// members (F9a: they were file statics shared by every in-process node).
static const bool g_txRateLog = [] { const char* e = std::getenv("RUNE_FS_LOG");
                                     return e == nullptr || e[0] != '0'; }();

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
        return false;
    }
    // tryClaim returns a NEGATIVE index on a full ring → BACK-PRESSURE: return
    // false WITHOUT committing (nothing claimed, no torn record).
    // The caller treats the skipped send as retriable (the follower re-fetches).
    const aeron::util::index_t idx = outboundRing_.tryClaim(frameLen);
    if (idx < 0) {
        return false;
    }
    isbe::OutboundFrame enc;
    enc.wrapAndApplyHeader(reinterpret_cast<char*>(outboundRing_.base() + idx), 0, frameLen);
    enc.targetNodeId(targetNodeId);
    enc.putPayload(reinterpret_cast<const char*>(wire), static_cast<std::uint32_t>(len));
    // Worker-clock: frame handed to the outbound ring (→ control thread drains it).
    // corr = ring slot offset; pairs 1:1 with ReplOutDrain (same slot).  Marked
    // BEFORE commit (after commit the slot may already be drained and reused).
    RUNE_FR_MARK_REPLICATION(rune_fr::Site::ReplOutEnqueue, static_cast<uint32_t>(idx));
    outboundRing_.commit(idx);
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
    // F27 F1: NOTHING the ring accepted is ever dropped here.  Phase A flushes
    // each peer's held frames in order (a refusal stops that peer only); phase B
    // routes ring frames through the hold router under a CONTROLLED read — a
    // frame a full FIFO cannot take stays in the ring (ABORT) for the next drain.
    PubOffer offer{this};
    int work = holdRouter_.flushHeld(offer);

    auto handler = [this, &offer](std::int32_t /*msgTypeId*/,
                                  aeron::concurrent::AtomicBuffer& buf,
                                  aeron::util::index_t offset,
                                  aeron::util::index_t length) -> bool {
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
        const bool consumed = holdRouter_.route(offer, target, wire, wlen);
        if (consumed) {
            // Control-clock: frame drained from the outbound ring (offered or held).
            // corr = ring slot offset == the enqueue's tryClaim index.
            RUNE_FR_MARK_REPLICATION(rune_fr::Site::ReplOutDrain, static_cast<uint32_t>(offset));
            ++txOutDrained_;
        }
        return consumed;
    };
    // Work = ring frames consumed + held frames flushed (a FIFO that keeps being
    // refused is NOT work, so the IdleStrategy can back off).
    work += outboundRing_.controlledDrain(handler, /*limit=*/kFragmentLimit);
    // R3: a held-FIFO push can only fail if the FIFO >= ring-maxMsgLength
    // invariant is broken — never silent: counted by the router, logged here.
    if (const uint64_t pf = holdRouter_.stats().pushFailed; pf != lastPushFailed_) [[unlikely]] {
        lastPushFailed_ = pf;
        spdlog::error("AeronPartitionTransport[node={}]: held-FIFO push failed ({} total) — "
                      "FIFO smaller than a ring frame; frame DROPPED (invariant breach)",
                      selfId_, pf);
    }
    return work;
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
    // Task 11 fix 2 (B3): rune templated controlled assembler — no std::function.
    const int fragged = sub_->controlledPoll(
        [this](aeron::concurrent::AtomicBuffer& b, aeron::util::index_t o, aeron::util::index_t l,
               aeron::Header& h) {
            return assembler_.onFragment(
                [this](aeron::concurrent::AtomicBuffer& buf, aeron::util::index_t offset,
                       aeron::util::index_t length, aeron::Header& hdr) {
                    return onFragment(buf, offset, length, hdr);
                },
                b, o, l, h);
        },
        kFragmentLimit);
    imageWatch_.afterPoll(*sub_, assembler_);   // free buffers of lost images (m-c)
    work += fragged;

    if (g_txRateLog) {
        ++txPolls_;
        if (txRateLastNs_ == 0) txRateLastNs_ = nowNs;
        else if (nowNs - txRateLastNs_ >= 1'000'000'000LL) {
            spdlog::info("TXRATE[node={}] polls/s={} fragsRecv/s={} outDrained/s={}",
                         selfId_, txPolls_, txFragsRecv_, txOutDrained_);
            txRateLastNs_ = nowNs; txPolls_ = 0; txFragsRecv_ = 0; txOutDrained_ = 0;
        }
    }
    return work;
}

AeronPartitionTransport::TxStats AeronPartitionTransport::txStatsForTest() const noexcept {
    return TxStats{txPolls_, txFragsRecv_, txOutDrained_};
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
    ++txFragsRecv_;
    RUNE_FR_SPAN_REPLICATION(rune_fr::Site::ReplOnFragment, 0);
    return handleFrame_(reinterpret_cast<const uint8_t*>(buffer.buffer() + offset),
                        static_cast<std::size_t>(length));
}

aeron::ControlledPollAction AeronPartitionTransport::handleFrame_(const uint8_t* buf, std::size_t len)
{
    using Action = aeron::ControlledPollAction;
    // CONTINUE = consume this fragment and keep polling (success / skip malformed).
    // ABORT    = leave this fragment UNCONSUMED and stop the poll — Aeron re-delivers
    //            it next cycle.  Returned ONLY when a target worker ring is full, so
    //            the frame is never dropped: back-pressure propagates to the sender.

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
        PartitionReplica* const rep = resolveReplica_(req.topicId, req.partition);
        if (rep == nullptr) {
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

        // F3 round 5 ⇄ F18: the unconditional base IS maxFetchRecordsBytes() (ring
        // AND publication bound) — the transport's ONE records ceiling.
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
        int32_t ceiling = maxFetchResponseBytes();   // = clamp(maxFetchRecordsBytes())
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
            op.replica                           = rep;
            op.logPtr                            = nullptr;
            op.fetchRequestPayload.leaderEpoch   = req.leaderEpoch;
            op.fetchRequestPayload.partition     = req.partition;
            op.fetchRequestPayload.followerId    = req.followerId;
            op.fetchRequestPayload.fetchOffset   = req.fetchOffset;
            op.fetchRequestPayload.maxBytes      = req.maxBytes;
            op.fetchRequestPayload.topicId       = req.topicId;
            op.fetchRequestPayload.nowNs         = nowNs_;
            op.fetchRequestPayload.connGen       = peerConnGen_(req.followerId);   // R1
            if (!workerPool_->tryEnqueueOp(req.topicId, req.partition, std::move(op))) {
                // Worker ring full → back-pressure: leave the fragment unconsumed and
                // retry next cycle (the follower's fetch is served once the worker
                // drains).  No drop.
                return Action::ABORT;
            }
            // Control-clock: fetch op enqueued to the worker ring (corr=recvSeq).
            // Pairs 1:1 (FIFO) with ReplWorkerOpRun when the worker dequeues it →
            // the gap is the EXACT control→worker handoff latency on this node.
            RUNE_FR_MARK_REPLICATION(rune_fr::Site::ReplFragRecv, inRecvSeq_++);
        } else {
            rep->onFetchRequest(req, nowNs_, peerConnGen_(req.followerId));   // R1
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
        PartitionReplica* const rep = resolveReplica_(resp.topicId, resp.partition);
        if (rep == nullptr) {
            spdlog::trace("AeronPartitionTransport: FetchResponse for unknown (topicId={}, partition={})",
                          resp.topicId, resp.partition);
            return Action::CONTINUE;
        }
        if (workerPool_ && resp.topicId >= 0 &&
            !workerPool_->opTailFits(resp.topicId, resp.partition, resp.records.size())) [[unlikely]] {
            // F18: records that cannot fit ONE frame of the owning worker's ingress
            // ring would THROW in tryClaim (terminating the process), and ABORT would
            // redeliver them forever.  Look at the FRONT batch only (B-1):
            //  - it fits on its own → a multi-batch prefix / push / duplicate that
            //    merely exceeds the frame in total: DROP (counted), never wedge.  The
            //    follower's fetch is capped to this frame (setApplyRecordsCap), so it
            //    re-fetches what it needs one frame at a time.
            //  - it does not fit on its own → tell the follower replica (on its
            //    owning worker) via a small record-less kErrRecordTooLarge op carrying
            //    that batch's baseOffset; the replica wedges only if that is the batch
            //    at its LEO (I-1: stops fetching → leaves the ISR visibly).
            const std::size_t kOverhead =
                static_cast<std::size_t>(rune::protocol::batch::kRecordBatchOverhead);
            std::size_t frontLen  = resp.records.size();
            Offset      frontBase = -1;
            if (resp.records.size() >= kOverhead) {
                rune::protocol::RecordBatchReader r(resp.records.data(), resp.records.size());
                if (r.totalLength() > 0) {
                    frontLen  = std::min<std::size_t>(static_cast<std::size_t>(r.totalLength()),
                                                      resp.records.size());
                    frontBase = r.baseOffset();
                }
            }
            if (frontBase < 0 || workerPool_->opTailFits(resp.topicId, resp.partition, frontLen)) {
                if (oversizedApplyDropped_++ == 0 || (oversizedApplyDropped_ & 1023u) == 0) {
                    spdlog::warn("AeronPartitionTransport: FetchResponse for (topicId={}, partition={}) "
                                 "carries {} B — above the worker ingress frame in total; dropped "
                                 "(each batch fits; the capped fetch recovers) (count={})",
                                 resp.topicId, resp.partition, resp.records.size(),
                                 oversizedApplyDropped_);
                }
                return Action::CONTINUE;
            }
            if (oversizedApplyRefused_++ == 0 || (oversizedApplyRefused_ & 1023u) == 0) {
                spdlog::error("AeronPartitionTransport: batch at offset {} for (topicId={}, partition={}) "
                              "is {} B — exceeds this node's worker ingress frame on its own (raise "
                              "RUNE_WORKER_INGRESS_RING_BYTES); follower told (count={})",
                              frontBase, resp.topicId, resp.partition, frontLen,
                              oversizedApplyRefused_);
            }
            rune::node::WorkerOp op;
            op.type                              = rune::node::WorkerOp::Type::ApplyReplicated;
            op.topicId                           = resp.topicId;
            op.partition                         = resp.partition;
            op.replica                           = rep;
            op.nextOffset                        = frontBase;   // the unapplicable batch
            op.replicatedPayload.leaderEpoch     = resp.leaderEpoch;
            op.replicatedPayload.partition       = resp.partition;
            op.replicatedPayload.highWatermark   = resp.highWatermark;
            op.replicatedPayload.logStartOffset  = resp.logStartOffset;
            op.replicatedPayload.errorCode       = kErrRecordTooLarge;
            op.replicatedPayload.topicId         = resp.topicId;
            if (!workerPool_->tryEnqueueOp(resp.topicId, resp.partition, std::move(op))) {
                --oversizedApplyRefused_;   // re-counted on redelivery
                return Action::ABORT;       // ring full (small op): genuine back-pressure
            }
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
            // (F3 round 5 item I1 — unfittable records dropped, never ABORT-retried
            //  or thrown — is F18's opTailFits handling just above: one mechanism.)
            rune::node::WorkerOp op;
            op.type                              = rune::node::WorkerOp::Type::ApplyReplicated;
            op.topicId                           = resp.topicId;
            op.partition                         = resp.partition;
            op.replica                           = rep;
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
            const auto er = workerPool_->enqueueOp(resp.topicId, resp.partition,
                                                   std::move(op));
            if (er == rune::node::EnqueueResult::TooLarge) [[unlikely]]
                return Action::CONTINUE;   // never fits: drop (F3 r6 item 5; pre-checked above)
            if (er != rune::node::EnqueueResult::Enqueued) {
                // Worker ring full → back-pressure: leave unconsumed, retry next cycle.
                // The records span still lives in the term buffer (fragment not
                // consumed), so the re-delivered fragment re-copies correctly. No drop.
                return Action::ABORT;
            }
            // Control-clock: apply op enqueued to the worker ring (corr=recvSeq).
            RUNE_FR_MARK_REPLICATION(rune_fr::Site::ReplFragRecv, inRecvSeq_++);
        } else {
            // INLINE path (no WorkerPool): call applyReplicated synchronously.
            rep->applyReplicated(resp);
        }

    } else if (tid == OffsetsForLeaderEpochReqMsg::TEMPLATE_ID) {
        // DR-BUG-6: KIP-101 epoch-boundary query → leader answers.
        if (len < OffsetsForLeaderEpochReqMsg::TOTAL_SIZE) return Action::CONTINUE;
        auto req = OffsetsForLeaderEpochReqMsg::decode(buf, 0);
        PartitionReplica* const rep = resolveReplica_(req.topicId, req.partition);
        if (rep == nullptr) return Action::CONTINUE;
        // E5.1.2c: route to owning worker (touches leaderState_/isrManager_).
        if (workerPool_ && req.topicId >= 0) {
            rune::node::WorkerOp op;
            op.type                              = rune::node::WorkerOp::Type::OffsetsForLeaderEpochReq;
            op.topicId                           = req.topicId;
            op.partition                         = req.partition;
            op.replica                           = rep;
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
            rep->onOffsetsForLeaderEpochRequest(req);
        }

    } else if (tid == OffsetsForLeaderEpochRespMsg::TEMPLATE_ID) {
        // DR-BUG-6: KIP-101 epoch-boundary answer → follower truncates.
        if (len < OffsetsForLeaderEpochRespMsg::TOTAL_SIZE) return Action::CONTINUE;
        auto resp = OffsetsForLeaderEpochRespMsg::decode(buf, 0);
        PartitionReplica* const rep = resolveReplica_(resp.topicId, resp.partition);
        if (rep == nullptr) return Action::CONTINUE;
        // Task 10 round 4: a USER replica's follower state / log is owned by
        // its worker — apply there (was inline on the control thread: a race).
        if (workerPool_ && resp.topicId >= 0) {
            rune::node::WorkerOp op;
            op.type                  = rune::node::WorkerOp::Type::OffsetsForLeaderEpochResp;
            op.topicId               = resp.topicId;
            op.partition             = resp.partition;
            op.offsEpochReqPayload.currentEpoch   = resp.leaderEpoch;
            op.offsEpochReqPayload.requestedEpoch = resp.responseEpoch;
            op.offsEpochReqPayload.followerLeo    = resp.endOffset;
            op.offsEpochReqPayload.partition      = resp.partition;
            op.offsEpochReqPayload.topicId        = resp.topicId;
            op.replicatedPayload.errorCode        = resp.errorCode;
            if (!workerPool_->tryEnqueueOp(resp.topicId, resp.partition, std::move(op)))
                return Action::ABORT;   // worker ring full → re-delivered next cycle
        } else {
            rep->applyOffsetsForLeaderEpochResponse(resp);
        }

    } else if (tid == kReconcileFetchReqTemplateId) {
        // DR-BUG-6: reverse pull request (deficient leader → survivor).  Reuses
        // the FetchRequest layout.  Cap maxBytes to the requesting peer's
        // deliverable ceiling (same rationale as the normal FetchRequest path).
        if (len < FetchRequestMsg::TOTAL_SIZE) return Action::CONTINUE;
        FetchRequestMsg req = FetchRequestMsg::decode(buf, 0);
        PartitionReplica* const rep = resolveReplica_(req.topicId, req.partition);
        if (rep == nullptr) return Action::CONTINUE;
        // Cap to the outbound-ring bound UNCONDITIONALLY (same two-hop framing
        // rationale as the normal FetchRequest path), tightened by the pub bound when
        // resolved, minus the outbound header — so the reconcile response never
        // overflows the outbound ring and throws inside the worker.
        const int32_t kOverhead = static_cast<int32_t>(FetchResponseMsg::encodedSize(0));
        int32_t ceiling = maxFetchResponseBytes();   // F3 r5 ⇄ F18: ring AND publication bound
        auto* peer = findPeer(req.followerId);
        if (peer && peer->pub) {
            const int32_t pubCeiling =
                static_cast<int32_t>(peer->pub->maxMessageLength()) - kOverhead;
            if (pubCeiling < ceiling) ceiling = pubCeiling;
        }
        if (ceiling < 0) ceiling = 0;
        if (req.maxBytes > ceiling) req.maxBytes = ceiling;
        // Task 10 round 4: serve a USER replica's reconcile pull on its owning
        // worker (reads the worker-owned log; was inline on the control thread).
        if (workerPool_ && req.topicId >= 0) {
            rune::node::WorkerOp op;
            op.type                            = rune::node::WorkerOp::Type::ReconcileFetchReq;
            op.topicId                         = req.topicId;
            op.partition                       = req.partition;
            op.fetchRequestPayload.leaderEpoch = req.leaderEpoch;
            op.fetchRequestPayload.partition   = req.partition;
            op.fetchRequestPayload.followerId  = req.followerId;
            op.fetchRequestPayload.fetchOffset = req.fetchOffset;
            op.fetchRequestPayload.maxBytes    = req.maxBytes;
            op.fetchRequestPayload.topicId     = req.topicId;
            if (!workerPool_->tryEnqueueOp(req.topicId, req.partition, std::move(op)))
                return Action::ABORT;
        } else {
            rep->onReconcileRequest(req);
        }

    } else if (tid == kReconcileFetchRespTemplateId) {
        // DR-BUG-6: reverse pull response (survivor → deficient leader).  Reuses
        // the FetchResponse layout.
        const std::size_t kMinResp = FetchResponseMsg::encodedSize(0);
        if (len < kMinResp) return Action::CONTINUE;
        const FetchResponseMsg resp = FetchResponseMsg::decode(buf, 0, len);
        PartitionReplica* const rep = resolveReplica_(resp.topicId, resp.partition);
        if (rep == nullptr) return Action::CONTINUE;
        if (workerPool_ && resp.topicId >= 0 &&
            !workerPool_->opTailFits(resp.topicId, resp.partition, resp.records.size())) [[unlikely]] {
            // F18: records that cannot fit ONE frame of the owning worker's ingress
            // ring would THROW in tryClaim (terminating the process), and treating it
            // as back-pressure (ABORT) would redeliver it forever and stall every
            // partition on this subscription.  Unreachable when intake is bounded by
            // produceIntakeLimit(); refuse it as a replication error (counted, logged),
            // consume the fragment — the follower stays at its offset and re-fetches.
            ++oversizedApplyRefused_;
            spdlog::error("AeronPartitionTransport: ReconcileFetchResponse for (topicId={}, partition={}) "
                          "carries {} B of records — exceeds the worker ingress frame; refused",
                          resp.topicId, resp.partition, resp.records.size());
            return Action::CONTINUE;
        }
        // E5.1.2c: route to owning worker (applyReconcileResponse touches
        // leaderState_.setLeaderLogEndOffset + recomputeHw).
        // Copy records bytes before returning from the Aeron callback (buffer lifetime).
        if (workerPool_ && resp.topicId >= 0) {
            rune::node::WorkerOp op;
            op.type                              = rune::node::WorkerOp::Type::ReconcileFetchResp;
            op.topicId                           = resp.topicId;
            op.partition                         = resp.partition;
            op.replica                           = rep;
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
            const auto er = workerPool_->enqueueOp(resp.topicId, resp.partition, std::move(op));
            if (er == rune::node::EnqueueResult::TooLarge) [[unlikely]]
                return Action::CONTINUE;   // never fits: drop (F3 r6 item 5; pre-checked above)
            if (er != rune::node::EnqueueResult::Enqueued) {
                // Worker ring full → back-pressure: leave unconsumed, retry next cycle.
                return Action::ABORT;
            }
        } else {
            rep->applyReconcileResponse(resp);
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
int64_t AeronPartitionTransport::sendOnPub(aeron::ExclusivePublication& pub,
                                         const uint8_t*               buf,
                                         std::size_t                  len)
{
    // Defence-in-depth: NEVER let offer() throw IllegalArgumentException on an
    // oversized frame.  This branch must never be reached in normal operation:
    // later batches are bounded by req.maxBytes (capped to the deliverable
    // ceiling), and an oversized FIRST batch (KIP-74, served whole) that exceeds
    // the frame is answered kErrRecordTooLarge by the replica instead of sent
    // (F18).  If it fires anyway, DROP + warn.
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
        ++oversizedFrameDropped_;   // F18: an oversize, not back-pressure
        return kOfferOversized;     // permanent: never held
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
            return pos;
        }
        // BACK_PRESSURED / ADMIN_ACTION / NOT_CONNECTED — fall through to offer.
    }

    // Fallback: offer (copies into term buffer).
    const int64_t result = pub.offer(atomicBuf, 0,
                                      static_cast<aeron::util::index_t>(len));
    return result;
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
