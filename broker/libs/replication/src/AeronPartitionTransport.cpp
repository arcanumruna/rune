// AeronPartitionTransport.cpp — GR-1 implementation.
//
// See AeronPartitionTransport.h for design notes.
// Single-writer. No locks. No hot-path alloc.

#include <rune/replication/AeronPartitionTransport.h>
#include <rune/replication/ReplicationMessages.h>

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
// registerReplica — add a (partitionId → PartitionReplica*) entry.
// ---------------------------------------------------------------------------
void AeronPartitionTransport::registerReplica(PartitionId       partitionId,
                                               PartitionReplica* replica)
{
    replicas_[partitionId] = replica;
}

// ---------------------------------------------------------------------------
// sendFetchRequest — (Follower → Leader)  non-blocking
// ---------------------------------------------------------------------------
void AeronPartitionTransport::sendFetchRequest(const FetchRequestMsg& req)
{
    // Encode into scratch buffer (zero alloc).
    const std::size_t len = FetchRequestMsg::encode(
        encodeScratch_.data(), 0,
        req.leaderEpoch,
        req.partition,
        req.followerId,
        req.fetchOffset,
        req.maxBytes);

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
    const std::size_t len = FetchResponseMsg::encode(
        encodeScratch_.data(), 0,
        resp.leaderEpoch,
        resp.partition,
        resp.highWatermark,
        resp.logStartOffset,
        resp.errorCode,
        resp.records);

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
        const FetchRequestMsg req = FetchRequestMsg::decode(buf, 0);

        // Route to the replica owning this partition.
        auto it = replicas_.find(req.partition);
        if (it == replicas_.end() || it->second == nullptr) {
            spdlog::trace("AeronPartitionTransport: FetchRequest for unknown partition={}", req.partition);
            return;
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
        const FetchResponseMsg resp = FetchResponseMsg::decode(buf, 0);

        auto it = replicas_.find(resp.partition);
        if (it == replicas_.end() || it->second == nullptr) {
            spdlog::trace("AeronPartitionTransport: FetchResponse for unknown partition={}", resp.partition);
            return;
        }
        it->second->applyReplicated(resp);

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
