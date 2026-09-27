// AeronPartitionTransport.cpp — GR-1 implementation.
//
// See AeronPartitionTransport.h for design notes.
// Single-writer. No locks. No hot-path alloc.

#include <rune/replication/AeronPartitionTransport.h>
#include <rune/replication/ReplicationMessages.h>

// E2 inc2a: WorkerPool + WorkerOp for SPSC ApplyReplicated routing.
// Included here (not in the header) to avoid pulling consensus/spdlog headers
// into consumers of rune_replication_aeron that don't need them.
// rune_replication_aeron has RUNE_NODE_INCLUDE set in its CMakeLists.txt.
#include <rune/node/WorkerPool.h>
#include <rune/node/WorkerOp.h>

#include <spdlog/spdlog.h>

#include <chrono>
#include <stdexcept>
#include <thread>
#include <cstring>

namespace rune::replication {

// ---------------------------------------------------------------------------
// Constructor
// ---------------------------------------------------------------------------
AeronPartitionTransport::AeronPartitionTransport(
        std::shared_ptr<aeron::Aeron> aeron,
        const std::string&             ingressChannel,
        int32_t                        ingressStreamId,
        NodeId                         selfId,
        int                            outboundRingBytes)
    : aeron_(std::move(aeron))
    , assembler_([this](aeron::concurrent::AtomicBuffer& buf,
                        aeron::util::index_t             offset,
                        aeron::util::index_t             length,
                        aeron::Header&                   hdr) {
          onFragment(buf, offset, length, hdr);
      })
    , selfId_(selfId)
    , outboundRing_(outboundRingBytes)  // NodeConfig-configurable ring size
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
    // Shared-nothing: encode into a stack buffer, marshal to control thread via
    // the outbound ring (peers_/pubs are control-owned; drainOutbound offers).
    // Size the buffer by the GENERATED codec's encodedSize() — stack fast-path for
    // the common (≤128KB) case, heap fallback for a large replicated batch.  No
    // fixed-cap overflow (was: fixed uint8_t wire[64KB] → __chk_fail on a 64KB+ batch).
    const std::size_t frame = FetchResponseMsg::encodedSize(resp.records.size());
    if (frame <= kMaxWireFrame) {
        uint8_t wire[kMaxWireFrame];
        FetchResponseMsg::encode(wire, 0, resp.leaderEpoch, resp.partition,
                                 resp.highWatermark, resp.logStartOffset,
                                 resp.errorCode, resp.records, resp.topicId);
        enqueueOutbound(toFollower, wire, frame);
    } else {
        std::vector<uint8_t> wire(frame);
        FetchResponseMsg::encode(wire.data(), 0, resp.leaderEpoch, resp.partition,
                                 resp.highWatermark, resp.logStartOffset,
                                 resp.errorCode, resp.records, resp.topicId);
        enqueueOutbound(toFollower, wire.data(), frame);
    }
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
    // Reuses the FetchResponse codec, re-stamped with the reconcile templateId.
    // Same encodedSize()-based sizing (stack fast-path + heap fallback) as
    // sendFetchResponse so a large reconcile batch cannot overflow a fixed buffer.
    const std::size_t frame = FetchResponseMsg::encodedSize(resp.records.size());
    if (frame <= kMaxWireFrame) {
        uint8_t wire[kMaxWireFrame];
        FetchResponseMsg::encode(wire, 0, resp.leaderEpoch, resp.partition,
                                 resp.highWatermark, resp.logStartOffset,
                                 resp.errorCode, resp.records, resp.topicId);
        stampTemplateId(wire, 0, kReconcileFetchRespTemplateId);
        enqueueOutbound(toPeer, wire, frame);
    } else {
        std::vector<uint8_t> wire(frame);
        FetchResponseMsg::encode(wire.data(), 0, resp.leaderEpoch, resp.partition,
                                 resp.highWatermark, resp.logStartOffset,
                                 resp.errorCode, resp.records, resp.topicId);
        stampTemplateId(wire.data(), 0, kReconcileFetchRespTemplateId);
        enqueueOutbound(toPeer, wire.data(), frame);
    }
}

// ---------------------------------------------------------------------------
// enqueueOutbound — WORKER-thread side of the shared-nothing send path.
// Frames [int32 targetNodeId][wire bytes] into a stack buffer and writes it to
// the outbound ring (agrona ManyToOneRingBuffer). No peers_/pubs access here.
// Back-pressure: on a full ring, bump the counter and return false — the send is
// skipped (replication is retriable; caller re-drives next cycle). Never blocks.
// ---------------------------------------------------------------------------
bool AeronPartitionTransport::enqueueOutbound(int32_t        targetNodeId,
                                              const uint8_t* wire,
                                              std::size_t    len)
{
    const std::size_t total = kOutboundHeaderSize + len;
    // Defensive: agrona's ManyToOneRingBuffer::write() THROWS (not returns false)
    // when a message exceeds maxMsgLength — only insufficient SPACE returns false.
    // Callers cap the only variable-length payload (the FetchResponse records tail)
    // below this bound; if an oversized frame still arrives it is a caller bug —
    // reject it here (retriable: the follower re-fetches and the leader's cap bounds
    // the re-fetch) rather than letting the throw unwind through the worker duty
    // cycle and abort the fetch.
    if (total > outboundRing_.maxMsgLength()) {
        spdlog::error(
            "AeronPartitionTransport::enqueueOutbound: framed message {} B exceeds "
            "outbound ring maxMsgLength {} B (target={}); dropping (caller must cap)",
            total, outboundRing_.maxMsgLength(), targetNodeId);
        ++outboundRingFullCount_;
        return false;
    }
    // Stack fast-path for the common case; heap fallback for a large frame.  A full
    // ring returns false below and is treated as back-pressure (retriable).
    if (total <= kMaxWireFrame) {
        uint8_t framed[kMaxWireFrame];
        std::memcpy(framed, &targetNodeId, kOutboundHeaderSize);
        std::memcpy(framed + kOutboundHeaderSize, wire, len);
        if (!outboundRing_.write(framed, total)) {
            ++outboundRingFullCount_;  // back-pressure: skipped (retriable)
            return false;
        }
        return true;
    }
    std::vector<uint8_t> framed(total);
    std::memcpy(framed.data(), &targetNodeId, kOutboundHeaderSize);
    std::memcpy(framed.data() + kOutboundHeaderSize, wire, len);
    if (!outboundRing_.write(framed.data(), total)) {
        ++outboundRingFullCount_;  // back-pressure or > ring maxMsgLength (retriable)
        return false;
    }
    return true;
}

// ---------------------------------------------------------------------------
// drainOutbound — CONTROL-thread side. Reads framed outbound messages and does
// the real peer offers. ONLY this path (control thread) touches peers_/pubs on
// the send side, preserving shared-nothing.
// ---------------------------------------------------------------------------
int AeronPartitionTransport::drainOutbound()
{
    auto handler = [this](std::int32_t /*msgTypeId*/,
                          aeron::concurrent::AtomicBuffer& buf,
                          aeron::util::index_t offset,
                          aeron::util::index_t length) {
        const uint8_t* p = buf.buffer() + offset;
        int32_t target = 0;
        std::memcpy(&target, p, kOutboundHeaderSize);
        const uint8_t* wire = p + kOutboundHeaderSize;
        const std::size_t wlen = static_cast<std::size_t>(length) - kOutboundHeaderSize;

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
    nowNs_ = nowNs;

    // Non-blocking probe: try to resolve pending publications.
    resolvePendingPubs();

    // Shared-nothing: drain worker-marshalled outbound sends and do the real
    // peer offers here on the control thread (the only thread that touches
    // peers_/pubs). ([[rune-shared-nothing-law]]) Count folds into work so the
    // AgentRunner's IdleStrategy sees outbound activity as work (no idle).
    int work = drainOutbound();

    // Drain ingress subscription.
    work += sub_->poll(assembler_.handler(), kFragmentLimit);
    return work;
}

// ---------------------------------------------------------------------------
// onFragment — called by FragmentAssembler for each reassembled frame.
// ---------------------------------------------------------------------------
void AeronPartitionTransport::onFragment(aeron::concurrent::AtomicBuffer& buffer,
                                          aeron::util::index_t             offset,
                                          aeron::util::index_t             length,
                                          aeron::Header&                   /*header*/)
{
    const auto* buf = reinterpret_cast<const uint8_t*>(buffer.buffer() + offset);
    const auto  len = static_cast<std::size_t>(length);

    // Minimum frame: 8-byte SBE header.
    if (len < ReplicationMessageHeader::ENCODED_SIZE) {
        spdlog::trace("AeronPartitionTransport: short frame ({} bytes) dropped", len);
        return;
    }

    // Validate schema id.
    const auto hdr = ReplicationMessageHeader::decode(buf, 0);
    if (hdr.schemaId != kSchemaId) {
        spdlog::trace("AeronPartitionTransport: unknown schemaId={} dropped", hdr.schemaId);
        return;
    }

    const uint16_t tid = hdr.templateId;

    if (tid == FetchRequestMsg::TEMPLATE_ID) {
        // Minimum size check.
        if (len < FetchRequestMsg::TOTAL_SIZE) {
            spdlog::trace("AeronPartitionTransport: FetchRequest truncated ({} bytes)", len);
            return;
        }
        FetchRequestMsg req = FetchRequestMsg::decode(buf, 0);

        // Route to the replica owning (topicId, partition).
        // DR-BUG-5: composite key prevents collision across topics that share
        // the same partition index.
        auto it = replicas_.find(ReplicaKey{req.topicId, req.partition});
        if (it == replicas_.end() || it->second == nullptr) {
            spdlog::trace("AeronPartitionTransport: FetchRequest for unknown (topicId={}, partition={})",
                          req.topicId, req.partition);
            return;
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
        int32_t ceiling =
            static_cast<int32_t>(outboundRing_.maxMsgLength())
            - static_cast<int32_t>(kOutboundHeaderSize)
            - kFetchResponseOverhead;
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
                spdlog::warn(
                    "AeronPartitionTransport: worker ring full for "
                    "FetchRequest (topicId={}, partition={}); dropping frame",
                    req.topicId, req.partition);
            }
        } else {
            it->second->onFetchRequest(req, nowNs_);
        }

    } else if (tid == FetchResponseMsg::TEMPLATE_ID) {
        // Minimum valid frame = the zero-records framing (SBE header + block +
        // records length-prefix), from the generated codec.
        const std::size_t kMinResponseSize = FetchResponseMsg::encodedSize(0);
        if (len < kMinResponseSize) {
            spdlog::trace("AeronPartitionTransport: FetchResponse truncated ({} bytes)", len);
            return;
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
            return;
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
                spdlog::warn(
                    "AeronPartitionTransport: worker ring full for "
                    "ApplyReplicated (topicId={}, partition={}); dropping frame",
                    resp.topicId, resp.partition);
            }
        } else {
            // INLINE path (no WorkerPool): call applyReplicated synchronously.
            it->second->applyReplicated(resp);
        }

    } else if (tid == OffsetsForLeaderEpochReqMsg::TEMPLATE_ID) {
        // DR-BUG-6: KIP-101 epoch-boundary query → leader answers.
        if (len < OffsetsForLeaderEpochReqMsg::TOTAL_SIZE) return;
        auto req = OffsetsForLeaderEpochReqMsg::decode(buf, 0);
        auto it = replicas_.find(ReplicaKey{req.topicId, req.partition});
        if (it == replicas_.end() || it->second == nullptr) return;
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
                spdlog::warn(
                    "AeronPartitionTransport: worker ring full for "
                    "OffsetsForLeaderEpochReq (topicId={}, partition={}); dropping frame",
                    req.topicId, req.partition);
            }
        } else {
            it->second->onOffsetsForLeaderEpochRequest(req);
        }

    } else if (tid == OffsetsForLeaderEpochRespMsg::TEMPLATE_ID) {
        // DR-BUG-6: KIP-101 epoch-boundary answer → follower truncates.
        if (len < OffsetsForLeaderEpochRespMsg::TOTAL_SIZE) return;
        auto resp = OffsetsForLeaderEpochRespMsg::decode(buf, 0);
        auto it = replicas_.find(ReplicaKey{resp.topicId, resp.partition});
        if (it == replicas_.end() || it->second == nullptr) return;
        it->second->applyOffsetsForLeaderEpochResponse(resp);

    } else if (tid == kReconcileFetchReqTemplateId) {
        // DR-BUG-6: reverse pull request (deficient leader → survivor).  Reuses
        // the FetchRequest layout.  Cap maxBytes to the requesting peer's
        // deliverable ceiling (same rationale as the normal FetchRequest path).
        if (len < FetchRequestMsg::TOTAL_SIZE) return;
        FetchRequestMsg req = FetchRequestMsg::decode(buf, 0);
        auto it = replicas_.find(ReplicaKey{req.topicId, req.partition});
        if (it == replicas_.end() || it->second == nullptr) return;
        // Cap to the outbound-ring bound UNCONDITIONALLY (same two-hop framing
        // rationale as the normal FetchRequest path), tightened by the pub bound when
        // resolved, minus the outbound header — so the reconcile response never
        // overflows the outbound ring and throws inside the worker.
        const int32_t kOverhead = static_cast<int32_t>(FetchResponseMsg::encodedSize(0));
        int32_t ceiling =
            static_cast<int32_t>(outboundRing_.maxMsgLength())
            - static_cast<int32_t>(kOutboundHeaderSize)
            - kOverhead;
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
        if (len < kMinResp) return;
        const FetchResponseMsg resp = FetchResponseMsg::decode(buf, 0, len);
        auto it = replicas_.find(ReplicaKey{resp.topicId, resp.partition});
        if (it == replicas_.end() || it->second == nullptr) return;
        // E5.1.2c: route to owning worker (applyReconcileResponse touches
        // leaderState_.setLeaderLogEndOffset + recomputeHw).
        // Copy records bytes before returning from the Aeron callback (buffer lifetime).
        if (workerPool_ && resp.topicId >= 0) {
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
                spdlog::warn(
                    "AeronPartitionTransport: worker ring full for "
                    "ReconcileFetchResp (topicId={}, partition={}); dropping frame",
                    resp.topicId, resp.partition);
            }
        } else {
            it->second->applyReconcileResponse(resp);
        }

    } else {
        spdlog::trace("AeronPartitionTransport: unknown templateId={} dropped", tid);
    }
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
