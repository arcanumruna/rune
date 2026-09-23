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
        NodeId                         selfId)
    : aeron_(std::move(aeron))
    , assembler_([this](aeron::concurrent::AtomicBuffer& buf,
                        aeron::util::index_t             offset,
                        aeron::util::index_t             length,
                        aeron::Header&                   hdr) {
          onFragment(buf, offset, length, hdr);
      })
    , selfId_(selfId)
{
    encodeScratch_.resize(kScratchSize);

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
    // Encode into scratch buffer (zero alloc).
    // DR-BUG-5: pass req.topicId so the leader can route to the correct
    // replica.  The default topicId=0 caused multi-topic FetchRequests to be
    // misrouted to topic-0 replicas, starving multi-topic ISRs and blocking
    // all acks=All produces to any topic with id != 0.
    const std::size_t len = FetchRequestMsg::encode(
        encodeScratch_.data(), 0,
        req.leaderEpoch,
        req.partition,
        req.followerId,
        req.fetchOffset,
        req.maxBytes,
        req.topicId);

    // The leader is identified by the replica's leaderId field; the follower
    // knows the leader's NodeId from its PartitionConfig (set via becomeFollower).
    // The FetchRequest itself doesn't carry the leader NodeId on the wire — the
    // transport must know which peer is the leader.  For GR-1 we route the
    // FetchRequest to ALL known peers whose pub is resolved and connected — the
    // leader will handle it (others will reject with kErrNotLeader).  This is a
    // simple broadcast; GR-3 will narrow it to the specific leader NodeId once
    // the metadata bridge (GR-2) is in place.
    //
    // NOTE: FetchRequestMsg.followerId == selfId_ (set by PartitionReplica::nextFetch).
    // The leader uses that to track per-follower LEO.
    for (auto& [nodeId, peer] : peers_) {
        if (!peer.pub) continue;           // not yet resolved — skip
        if (!peer.pub->isConnected()) continue;
        sendOnPub(*peer.pub, encodeScratch_.data(), len);
    }
}

// ---------------------------------------------------------------------------
// sendFetchResponse — (Leader → Follower)  non-blocking
// ---------------------------------------------------------------------------
void AeronPartitionTransport::sendFetchResponse(NodeId                toFollower,
                                                 const FetchResponseMsg& resp)
{
    PeerPub* peer = findPeer(toFollower);
    if (!peer || !peer->pub) {
        // Publication not yet resolved — drop (follower will retry on next fetch).
        spdlog::trace("AeronPartitionTransport: sendFetchResponse drop (pub not ready) "
                      "follower={}", toFollower);
        return;
    }
    if (!peer->pub->isConnected()) {
        spdlog::trace("AeronPartitionTransport: sendFetchResponse drop (not connected) "
                      "follower={}", toFollower);
        return;
    }

    // Encode FetchResponse + records into scratch buffer.
    // DR-BUG-5: pass resp.topicId so the follower can route to the correct
    // replica.  The default topicId=0 caused multi-topic FetchResponses to be
    // misrouted to topic-0 replicas on the follower side, so multi-topic
    // replicas never applied replicated data.
    const std::size_t len = FetchResponseMsg::encode(
        encodeScratch_.data(), 0,
        resp.leaderEpoch,
        resp.partition,
        resp.highWatermark,
        resp.logStartOffset,
        resp.errorCode,
        resp.records,
        resp.topicId);

    sendOnPub(*peer->pub, encodeScratch_.data(), len);
}

// ---------------------------------------------------------------------------
// DR-BUG-6: OffsetsForLeaderEpoch request  (Follower → new Leader)
// ---------------------------------------------------------------------------
void AeronPartitionTransport::sendOffsetsForLeaderEpochRequest(
        NodeId toLeader, const OffsetsForLeaderEpochReqMsg& req)
{
    PeerPub* peer = findPeer(toLeader);
    if (!peer || !peer->pub || !peer->pub->isConnected()) return;
    const std::size_t len = OffsetsForLeaderEpochReqMsg::encode(
        encodeScratch_.data(), 0,
        req.currentEpoch, req.partition, req.followerId,
        req.requestedEpoch, req.followerLeo, req.topicId);
    sendOnPub(*peer->pub, encodeScratch_.data(), len);
}

// ---------------------------------------------------------------------------
// DR-BUG-6: OffsetsForLeaderEpoch response  (Leader → Follower)
// ---------------------------------------------------------------------------
void AeronPartitionTransport::sendOffsetsForLeaderEpochResponse(
        NodeId toFollower, const OffsetsForLeaderEpochRespMsg& resp)
{
    PeerPub* peer = findPeer(toFollower);
    if (!peer || !peer->pub || !peer->pub->isConnected()) return;
    const std::size_t len = OffsetsForLeaderEpochRespMsg::encode(
        encodeScratch_.data(), 0,
        resp.leaderEpoch, resp.partition, resp.responseEpoch,
        resp.endOffset, resp.errorCode, resp.topicId);
    sendOnPub(*peer->pub, encodeScratch_.data(), len);
}

// ---------------------------------------------------------------------------
// DR-BUG-6: reconcile request  (deficient Leader → ahead survivor)
// Reuses the FetchRequest layout, re-stamped with the reconcile templateId.
// ---------------------------------------------------------------------------
void AeronPartitionTransport::sendReconcileRequest(
        NodeId toPeer, const FetchRequestMsg& req)
{
    PeerPub* peer = findPeer(toPeer);
    if (!peer || !peer->pub || !peer->pub->isConnected()) return;
    const std::size_t len = FetchRequestMsg::encode(
        encodeScratch_.data(), 0,
        req.leaderEpoch, req.partition, req.followerId,
        req.fetchOffset, req.maxBytes, req.topicId);
    stampTemplateId(encodeScratch_.data(), 0, kReconcileFetchReqTemplateId);
    sendOnPub(*peer->pub, encodeScratch_.data(), len);
}

// ---------------------------------------------------------------------------
// DR-BUG-6: reconcile response  (ahead survivor → deficient Leader)
// Reuses the FetchResponse layout, re-stamped with the reconcile templateId.
// ---------------------------------------------------------------------------
void AeronPartitionTransport::sendReconcileResponse(
        NodeId toPeer, const FetchResponseMsg& resp)
{
    PeerPub* peer = findPeer(toPeer);
    if (!peer || !peer->pub || !peer->pub->isConnected()) return;
    const std::size_t len = FetchResponseMsg::encode(
        encodeScratch_.data(), 0,
        resp.leaderEpoch, resp.partition, resp.highWatermark,
        resp.logStartOffset, resp.errorCode, resp.records, resp.topicId);
    stampTemplateId(encodeScratch_.data(), 0, kReconcileFetchRespTemplateId);
    sendOnPub(*peer->pub, encodeScratch_.data(), len);
}

// ---------------------------------------------------------------------------
// poll — duty-cycle step: resolve pubs + drain ingress subscription.
// ---------------------------------------------------------------------------
int AeronPartitionTransport::poll(int64_t nowNs)
{
    nowNs_ = nowNs;

    // Non-blocking probe: try to resolve pending publications.
    resolvePendingPubs();

    // Drain ingress subscription.
    int work = sub_->poll(assembler_.handler(), kFragmentLimit);
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
        // kFetchResponseOverhead = SBE header(8) + fixed block(30) + len-prefix(4) = 42
        // bytes of framing that are always present; the remaining budget is for records.
        static constexpr int32_t kFetchResponseOverhead =
            static_cast<int32_t>(ReplicationMessageHeader::ENCODED_SIZE) +
            static_cast<int32_t>(FetchResponseMsg::BLOCK_LENGTH_FIXED) +
            static_cast<int32_t>(FetchResponseMsg::VAR_HEADER_SIZE);

        auto* followerPeer = findPeer(req.followerId);
        if (followerPeer && followerPeer->pub) {
            int32_t ceiling = static_cast<int32_t>(followerPeer->pub->maxMessageLength())
                              - kFetchResponseOverhead;
            if (ceiling < 0) ceiling = 0;
            if (req.maxBytes > ceiling) {
                req.maxBytes = ceiling;
            }
        }

        it->second->onFetchRequest(req, nowNs_);

    } else if (tid == FetchResponseMsg::TEMPLATE_ID) {
        // Minimum fixed size check (header + fixed block + var length prefix).
        static constexpr std::size_t kMinResponseSize =
            ReplicationMessageHeader::ENCODED_SIZE +
            FetchResponseMsg::BLOCK_LENGTH_FIXED +
            FetchResponseMsg::VAR_HEADER_SIZE;
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
        if (workerPool_) {
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
        it->second->onOffsetsForLeaderEpochRequest(req);

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
        static constexpr int32_t kOverhead =
            static_cast<int32_t>(ReplicationMessageHeader::ENCODED_SIZE) +
            static_cast<int32_t>(FetchResponseMsg::BLOCK_LENGTH_FIXED) +
            static_cast<int32_t>(FetchResponseMsg::VAR_HEADER_SIZE);
        auto* peer = findPeer(req.followerId);
        if (peer && peer->pub) {
            int32_t ceiling = static_cast<int32_t>(peer->pub->maxMessageLength()) - kOverhead;
            if (ceiling < 0) ceiling = 0;
            if (req.maxBytes > ceiling) req.maxBytes = ceiling;
        }
        it->second->onReconcileRequest(req);

    } else if (tid == kReconcileFetchRespTemplateId) {
        // DR-BUG-6: reverse pull response (survivor → deficient leader).  Reuses
        // the FetchResponse layout.
        static constexpr std::size_t kMinResp =
            ReplicationMessageHeader::ENCODED_SIZE +
            FetchResponseMsg::BLOCK_LENGTH_FIXED +
            FetchResponseMsg::VAR_HEADER_SIZE;
        if (len < kMinResp) return;
        const FetchResponseMsg resp = FetchResponseMsg::decode(buf, 0, len);
        auto it = replicas_.find(ReplicaKey{resp.topicId, resp.partition});
        if (it == replicas_.end() || it->second == nullptr) return;
        it->second->applyReconcileResponse(resp);

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
