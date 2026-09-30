#pragma once
// ---------------------------------------------------------------------------
// WorkerReplicationTransport.h — per-worker Aeron replication transport
//                                (send path, Task 3 of E5 per-worker replication).
//
// ## Role
//   Each worker thread owns ONE WorkerReplicationTransport.  It binds a
//   dedicated Aeron subscription for inbound replication traffic on stream
//   (replBase + workerId) and ONE ExclusivePublication per peer node on the
//   same stream offset on the peer's side.
//
//   This transport implements rune::replication::PartitionTransport so it
//   drops in as a direct replacement for AeronPartitionTransport for
//   the per-worker replication plane (E5.2).
//
// ## Wire format
//   RAW SBE replication messages (NO OutboundFrame envelope).  The legacy
//   AeronPartitionTransport wraps frames in an OutboundFrame for the
//   worker→control ring; here the worker IS the Aeron thread so we encode
//   directly into the tryClaim BufferClaim buffer (zero extra copy).
//
// ## Send path
//   For frames <= pub.maxPayloadLength():
//     - tryClaim the Aeron term buffer slot
//     - encode the SBE message DIRECTLY into the claimed buffer (zero-copy)
//     - commit
//   FetchResponse (spec §3.5a.2): only the 46-byte SBE prefix is encoded (on the
//   stack); the records body is a zero-copy log view handed to sendGatherToPeer,
//   which copies it ONCE, log → term buffer:
//     - len <= pub.maxPayloadLength(): tryClaim + two memcpys + commit
//     - larger: 2-buffer gather pub.offer() (Aeron fragments it) — no scratch copy
//
// ## Routing
//   sendFetchResponse(toFollower, ...)  → single peer pub (by NodeId)
//   sendOffsetsForLeaderEpochRequest(toLeader, ...) → single peer pub
//   sendOffsetsForLeaderEpochResponse(toFollower,...) → single peer pub
//   sendReconcileRequest(toPeer, ...)   → single peer pub
//   sendReconcileResponse(toPeer, ...)  → single peer pub
//   sendFetchRequest(req)               → ALL peer pubs (broadcast)
//     Rationale: sendFetchRequest has no target on the wire; only the current
//     leader responds; non-leaders drop it via their role guard.  Mirrors
//     AeronPartitionTransport::sendFetchRequest → kBroadcast.
//   sendFetchRequestTo(leader, req)     → single peer pub (spec §3.5.2: the
//     follower's fetches / fast-acks go to its KNOWN leader only); falls back to
//     the broadcast when the leader is not a registered peer.
//
// ## Shared-nothing
//   This object is OWNED by one worker thread and NEVER touched by another.
//   No atomics, no locks, no cross-thread state.
//
// ## Construction
//   Non-throwing.  Mirrors WorkerIngress (subscription) + WorkerEgress
//   (ExclusivePublication) bounded-resolve loops.  Check resolved() after
//   construction; if false the transport is inert (sends are no-ops).
//
// ## Test seam
//   The class is templated on PubRegistry, a policy that owns the per-peer
//   pub handles and provides:
//     int64_t sendToPeer(NodeId, const uint8_t* buf, std::size_t len)
//     void    broadcastToPeers(const uint8_t* buf, std::size_t len)
//     int64_t sendGatherToPeer(NodeId, const uint8_t* prefix, std::size_t prefixLen,
//                              std::span<const uint8_t> body)
//   The default AeronPubRegistry resolves real ExclusivePublications and calls
//   tryClaim/offer.  Tests inject FakePubRegistry (records offers per NodeId,
//   no Aeron driver needed).  No virtual calls on any path.
//
// ## Receive path (Task 4)
//   poll(fragmentLimit, dispatch) drains up to fragmentLimit reassembled SBE
//   frames from the inbound subscription and invokes dispatch zero-copy inside
//   each fragment callback.  In test mode it drains the in-process inbound
//   queue fed by injectFrameForTest().  FetchRequest.maxBytes is capped to
//   maxFetchResponseBytes() before dispatch (GR-1 carry-forward).
// ---------------------------------------------------------------------------

#include <Aeron.h>
#include <ExclusivePublication.h>
#include <FragmentAssembler.h>
#include <concurrent/AtomicBuffer.h>
#include <concurrent/logbuffer/BufferClaim.h>
#include <util/Index.h>

#include <rune/aeronutil/AeronPump.h>          // spec §3.6 invoker-aware resolve
#include <rune/replication/FollowerSlots.h>      // kMaxReplicas (flat peer array bound)
#include <rune/replication/PartitionReplica.h>   // PartitionTransport base
#include <rune/replication/ReplicationMessages.h> // SBE codecs + stampTemplateId
#include <rune_fr/FlightRecorder.h>                // #4 deep instrumentation

#include <spdlog/spdlog.h>

#include <algorithm>
#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <deque>
#include <limits>
#include <memory>
#include <span>
#include <string>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

namespace rune::replication {

// ===========================================================================
// AeronPubRegistry — production PubRegistry policy.
//
// Owns a FLAT array of (NodeId, shared_ptr<ExclusivePublication>) — at most
// FollowerSlots::kMaxReplicas peers; a linear scan beats unordered_map::find on
// every send (spec §3.5a.2 fix on sight).
// Resolves publications via bounded addExclusivePublication + findExclusive-
// Publication resolve via AeronPump (kResolveMaxMs=5000; 1 ms sleep with the
// default pump, invoker pumping otherwise), same as
// WorkerEgress.  Single-owner / single-thread.
// ===========================================================================
class AeronPubRegistry {
public:
    static constexpr int kResolveMaxMs   = 5000;

    // -----------------------------------------------------------------------
    // Construction: for each (peerId, peerChannel) pair, register an
    // ExclusivePublication on peerStreamId and resolve it.
    // -----------------------------------------------------------------------
    AeronPubRegistry(std::shared_ptr<aeron::Aeron>                                       aeron,
                     const std::vector<std::pair<rune::replication::NodeId, std::string>>& peerChannels,
                     int32_t peerStreamId,
                     rune::aeronutil::AeronPump pump = {}) noexcept
    {
        if (!aeron) return;  // no driver → all unresolved

        struct Pending {
            rune::replication::NodeId peerId;
            int64_t                   regId;
        };
        std::vector<Pending> pending;
        pending.reserve(peerChannels.size());

        // Peers whose registration failed (add or find threw): absent from pubs_
        // (sends to them are no-ops) and they keep allResolved() false.
        std::size_t failedPeers = 0;
        for (const auto& [peerId, channel] : peerChannels) {
            try {
                int64_t regId = aeron->addExclusivePublication(channel, peerStreamId);
                pending.push_back({peerId, regId});
            } catch (const std::exception& e) {
                spdlog::error("AeronPubRegistry: peer {} addExclusivePublication {} failed: {}",
                              peerId, channel, e.what());
                ++failedPeers;
            }
        }

        // Bounded resolve (spec §3.6): default pump sleeps 1 ms between probes
        // (legacy WorkerEgress behaviour); an invoker pump drives this shard's
        // own driver + conductor instead.
        // A peer whose registration ERRORED (find throws: bad channel, ...) is
        // dropped from `pending` and counted in `failedPeers` — the others keep
        // resolving; the ctor is noexcept so nothing may escape.
        (void)pump.resolve([&] {
            for (auto it = pending.begin(); it != pending.end(); ) {
                std::shared_ptr<aeron::ExclusivePublication> pub;
                try {
                    pub = aeron->findExclusivePublication(it->regId);
                } catch (const std::exception& e) {
                    spdlog::error("AeronPubRegistry: peer {} publication failed: {}", it->peerId, e.what());
                    ++failedPeers;
                    it = pending.erase(it);
                    continue;
                }
                if (pub) {
                    if (pubCount_ < kMaxPeers) pubs_[pubCount_++] = PeerPub{it->peerId, std::move(pub)};
                    it = pending.erase(it);
                } else {
                    ++it;
                }
            }
            return pending.empty();
        }, std::chrono::milliseconds{kResolveMaxMs});
        // Any still-pending entries were not resolved within the window;
        // they remain absent from pubs_ → sends to them are no-ops.
        allResolved_ = pending.empty() && failedPeers == 0 && pubCount_ > 0 &&
                       peerChannels.size() <= kMaxPeers;
    }

    // Non-copyable, non-movable.
    AeronPubRegistry(const AeronPubRegistry&)            = delete;
    AeronPubRegistry& operator=(const AeronPubRegistry&) = delete;
    AeronPubRegistry(AeronPubRegistry&&)                 = delete;
    AeronPubRegistry& operator=(AeronPubRegistry&&)      = delete;

    // -----------------------------------------------------------------------
    // allResolved — true when every peer pub resolved AND there is ≥1 peer.
    // -----------------------------------------------------------------------
    [[nodiscard]] bool allResolved() const noexcept { return allResolved_; }

    [[nodiscard]] bool hasPeer(rune::replication::NodeId peer) const noexcept
    {
        return findPub_(peer) != nullptr;
    }

    // -----------------------------------------------------------------------
    // dropPeer — forget `peer`'s publication (cold path: shard plane refusal).
    // Afterwards every send to it is a no-op and broadcasts skip it.
    // -----------------------------------------------------------------------
    void dropPeer(rune::replication::NodeId peer) noexcept
    {
        for (std::size_t i = 0; i < pubCount_; ++i) {
            if (pubs_[i].id != peer) continue;
            for (std::size_t j = i + 1; j < pubCount_; ++j) pubs_[j - 1] = std::move(pubs_[j]);
            pubs_[--pubCount_] = PeerPub{};
            return;
        }
    }

    // -----------------------------------------------------------------------
    // maxMessageLength — minimum maxMessageLength across all resolved peer
    // publications.  Used to cap FetchRequest.maxBytes so the leader never
    // builds a FetchResponse that exceeds the Aeron frame limit.
    // Returns 0 if no pubs are resolved yet (safe: ceiling computation will
    // clamp to 0 which disables non-empty fetches until pubs resolve).
    // -----------------------------------------------------------------------
    [[nodiscard]] int32_t maxMessageLength() const noexcept
    {
        int32_t minLen = std::numeric_limits<int32_t>::max();
        for (std::size_t i = 0; i < pubCount_; ++i) {
            if (const auto& pub = pubs_[i].pub) {
                const int32_t ml = static_cast<int32_t>(pub->maxMessageLength());
                if (ml < minLen) minLen = ml;
            }
        }
        return (minLen == std::numeric_limits<int32_t>::max()) ? 0 : minLen;
    }

    // -----------------------------------------------------------------------
    // sendToPeer — look up the peer pub by NodeId and send (tryClaim / offer).
    // Returns the Aeron result (>0 accepted; ≤0 back-pressure / not found).
    // -----------------------------------------------------------------------
    int64_t sendToPeer(rune::replication::NodeId peer,
                       const uint8_t* buf, std::size_t len) noexcept
    {
        aeron::ExclusivePublication* pub = findPub_(peer);
        if (!pub) return -1;
        return sendOnPub_(*pub, buf, len);
    }

    // -----------------------------------------------------------------------
    // broadcastToPeers — send to ALL registered peer pubs (best-effort;
    // individual back-pressure does not abort the broadcast).
    // -----------------------------------------------------------------------
    void broadcastToPeers(const uint8_t* buf, std::size_t len) noexcept
    {
        RUNE_FR_SPAN_REPLICATION(rune_fr::Site::ReplWorkerBroadcast, 0);
        for (std::size_t i = 0; i < pubCount_; ++i) {
            if (pubs_[i].pub) sendOnPub_(*pubs_[i].pub, buf, len);
        }
    }

    // -----------------------------------------------------------------------
    // sendGatherToPeer — zero-copy send of [prefix | body]: body bytes are
    // copied exactly once, from the log view into the term buffer.  tryClaim
    // for <= maxPayloadLength, else a 2-buffer gather offer() (Aeron fragments
    // it) — no intermediate scratch copy.
    // -----------------------------------------------------------------------
    int64_t sendGatherToPeer(rune::replication::NodeId peer,
                             const uint8_t* prefix, std::size_t prefixLen,
                             std::span<const uint8_t> body) noexcept
    {
        aeron::ExclusivePublication* pub = findPub_(peer);
        if (!pub) return -1;
        const std::size_t len = prefixLen + body.size();
        RUNE_FR_SPAN_REPLICATION(rune_fr::Site::ReplWorkerSendOnPub,
                                 static_cast<uint32_t>(len));
        if (len > static_cast<std::size_t>(pub->maxMessageLength())) {
            spdlog::error("WorkerReplicationTransport: drop oversized frame len={} max={}",
                          len, pub->maxMessageLength());
            return -1;
        }
        if (len <= static_cast<std::size_t>(pub->maxPayloadLength())) {
            aeron::concurrent::logbuffer::BufferClaim claim;
            const int64_t pos = pub->tryClaim(static_cast<aeron::util::index_t>(len), claim);
            if (pos > 0) {
                uint8_t* dst = claim.buffer().buffer() + claim.offset();
                std::memcpy(dst, prefix, prefixLen);
                if (!body.empty()) std::memcpy(dst + prefixLen, body.data(), body.size());
                claim.commit();
            }
            return pos;
        }
        const std::array<aeron::concurrent::AtomicBuffer, 2> parts{
            aeron::concurrent::AtomicBuffer(const_cast<uint8_t*>(prefix),
                                            static_cast<aeron::util::index_t>(prefixLen)),
            aeron::concurrent::AtomicBuffer(const_cast<uint8_t*>(body.data()),
                                            static_cast<aeron::util::index_t>(body.size()))};
        return pub->offer(parts);
    }

private:
    // Zero-copy tryClaim for ≤ maxPayloadLength; offer() for larger frames.
    // Mirrors Worker::sendEncodedToPub_ (Worker.h:1064-1085) plus the
    // defence-in-depth maxMessageLength guard from AeronPartitionTransport::sendOnPub.
    static int64_t sendOnPub_(aeron::ExclusivePublication& pub,
                              const uint8_t* buf, std::size_t len) noexcept
    {
        RUNE_FR_SPAN_REPLICATION(rune_fr::Site::ReplWorkerSendOnPub,
                                 static_cast<uint32_t>(len));
        // Guard: offer() THROWS IllegalArgumentException if len > maxMessageLength.
        // Drop and log — the follower will retry on the next fetch cycle.
        if (len > static_cast<std::size_t>(pub.maxMessageLength())) {
            spdlog::error("WorkerReplicationTransport::sendOnPub_: drop oversized frame "
                          "len={} maxMessageLength={} — replication term too small",
                          len, pub.maxMessageLength());
            return -1;
        }

        auto* mutableBuf = const_cast<uint8_t*>(buf);
        aeron::concurrent::AtomicBuffer atomicBuf(
            mutableBuf, static_cast<aeron::util::index_t>(len));

        if (len <= static_cast<std::size_t>(pub.maxPayloadLength())) {
            aeron::concurrent::logbuffer::BufferClaim claim;
            const int64_t pos =
                pub.tryClaim(static_cast<aeron::util::index_t>(len), claim);
            if (pos > 0) {
                claim.buffer().putBytes(
                    claim.offset(), atomicBuf, 0,
                    static_cast<aeron::util::index_t>(len));
                claim.commit();
            }
            return pos;
        }
        return pub.offer(atomicBuf, 0, static_cast<aeron::util::index_t>(len));
    }

    struct PeerPub {
        rune::replication::NodeId                    id = -1;
        std::shared_ptr<aeron::ExclusivePublication> pub;
    };
    static constexpr std::size_t kMaxPeers = FollowerSlots::kMaxReplicas;

    [[nodiscard]] aeron::ExclusivePublication* findPub_(rune::replication::NodeId id) const noexcept
    {
        for (std::size_t i = 0; i < pubCount_; ++i) if (pubs_[i].id == id) return pubs_[i].pub.get();
        return nullptr;
    }

    bool                           allResolved_ = false;
    std::array<PeerPub, kMaxPeers> pubs_{};
    std::size_t                    pubCount_    = 0;
};

// ===========================================================================
// WorkerReplicationTransportImpl<PubRegistry>
//
// Implements PartitionTransport (6 virtuals).
// Template parameter PubRegistry is the policy for peer pub management and
// sending (see AeronPubRegistry above; FakePubRegistry used in tests).
// ===========================================================================
template<typename PubRegistry = AeronPubRegistry>
class WorkerReplicationTransportImpl final
    : public rune::replication::PartitionTransport
{
public:
    // -----------------------------------------------------------------------
    // Construction (production path) — resolves the inbound subscription and
    // all per-peer publications.  Non-throwing; check resolved() after ctor.
    //
    // Parameters:
    //   aeron          — Aeron client (shared; embedded driver or external)
    //   workerId       — worker index, used for diagnostic purposes
    //   inboundChannel — aeron channel the worker subscribes on
    //   inboundStreamId— stream id = replBase + workerId
    //   peerChannels   — (peerNodeId, peerSendChannel) per remote node
    //   peerStreamId   — stream id on the peer's side (replBase + workerId)
    // -----------------------------------------------------------------------
    WorkerReplicationTransportImpl(
            std::shared_ptr<aeron::Aeron>                                           aeron,
            int32_t                                                                  workerId,
            std::string                                                              inboundChannel,
            int32_t                                                                  inboundStreamId,
            std::vector<std::pair<rune::replication::NodeId, std::string>>           peerChannels,
            int32_t                                                                  peerStreamId,
            rune::aeronutil::AeronPump                                               pump = {}) noexcept
        : workerId_(workerId)
        , inboundChannel_(std::move(inboundChannel))
        , inboundStreamId_(inboundStreamId)
        , assembler_([this](aeron::concurrent::AtomicBuffer& buffer,
                            aeron::util::index_t             offset,
                            aeron::util::index_t             length,
                            aeron::Header&                   header) {
              trampoline_(ctx_,
                          reinterpret_cast<const uint8_t*>(buffer.buffer() + offset),
                          static_cast<std::size_t>(length),
                          header);
          })
        , pubReg_(aeron, peerChannels, peerStreamId, pump)
    {
        if (!aeron) return;  // null aeron → unresolved (test or misconfiguration)

        // Resolve inbound subscription — mirrors WorkerIngress ctor exactly.
        // The registry got its own copy of `pump`; this copy drives the
        // subscription resolve (default: 1 ms sleep; invoker: pump, no sleep).
        // Registration errors (bad channel, ...) throw from add/find; the ctor is
        // noexcept, so they become a logged resolve failure (resolved()==false).
        bool failed = false;
        try {
            const int64_t regId = aeron->addSubscription(inboundChannel_, inboundStreamId_);
            sub_ = pump.resolve([&]() -> std::shared_ptr<aeron::Subscription> {
                try {
                    return aeron->findSubscription(regId);
                } catch (const std::exception& e) {
                    spdlog::error("WorkerReplicationTransport[{}]: inbound {} stream {} failed: {}",
                                  workerId_, inboundChannel_, inboundStreamId_, e.what());
                    failed = true;
                    return nullptr;
                }
            }, std::chrono::milliseconds{AeronPubRegistry::kResolveMaxMs}, &failed);
        } catch (const std::exception& e) {
            spdlog::error("WorkerReplicationTransport[{}]: addSubscription {} stream {} failed: {}",
                          workerId_, inboundChannel_, inboundStreamId_, e.what());
        }
        // sub_ == nullptr → resolved() returns false.
    }

    // -----------------------------------------------------------------------
    // Test-seam constructor — injects a pre-built PubRegistry (e.g.
    // FakePubRegistry) and bypasses Aeron entirely (sub_ left null but
    // resolved() treats it as already done when pubReg_.allResolved()).
    // Used by makeTestTransport only.
    // -----------------------------------------------------------------------
    explicit WorkerReplicationTransportImpl(int32_t workerId, PubRegistry pubReg) noexcept
        : workerId_(workerId)
        , inboundStreamId_(0)
        , assembler_([this](aeron::concurrent::AtomicBuffer& buffer,
                            aeron::util::index_t             offset,
                            aeron::util::index_t             length,
                            aeron::Header&                   header) {
              trampoline_(ctx_,
                          reinterpret_cast<const uint8_t*>(buffer.buffer() + offset),
                          static_cast<std::size_t>(length),
                          header);
          })
        , pubReg_(std::move(pubReg))
        , testMode_(true)
    {}

    // Non-copyable, non-movable (ExclusivePublication is single-owner).
    WorkerReplicationTransportImpl(const WorkerReplicationTransportImpl&)            = delete;
    WorkerReplicationTransportImpl& operator=(const WorkerReplicationTransportImpl&) = delete;
    WorkerReplicationTransportImpl(WorkerReplicationTransportImpl&&)                 = delete;
    WorkerReplicationTransportImpl& operator=(WorkerReplicationTransportImpl&&)      = delete;

    ~WorkerReplicationTransportImpl() override = default;

    // -----------------------------------------------------------------------
    // resolved — true iff inbound sub AND every peer pub are live.
    // -----------------------------------------------------------------------
    [[nodiscard]] bool resolved() const noexcept
    {
        if (testMode_) return pubReg_.allResolved();
        return sub_ != nullptr && pubReg_.allResolved();
    }

    // Construction-time classification (spec §3.5a.1): the production registry is
    // the replica's concrete, de-virtualised arm; any test registry stays Generic.
    [[nodiscard]] rune::replication::TransportKind kind() const noexcept override
    {
        return std::is_same_v<PubRegistry, AeronPubRegistry>
                   ? rune::replication::TransportKind::PerWorker
                   : rune::replication::TransportKind::Generic;
    }

    // -----------------------------------------------------------------------
    // workerId — diagnostic accessor.
    // -----------------------------------------------------------------------
    [[nodiscard]] int32_t workerId() const noexcept { return workerId_; }

    // -----------------------------------------------------------------------
    // poll — inbound fragment dispatch (Task 4).
    //
    // Drain up to fragmentLimit reassembled replication frames from the inbound
    // subscription (or the test-mode inbound queue) and invoke dispatch for each.
    //
    // dispatch is called INSIDE the Aeron fragment callback (or inline in test
    // mode) with a zero-copy view into the term buffer (valid ONLY for the
    // duration of the call).  The caller MUST NOT store the frame pointer.
    //
    // dispatch signature:
    //   void(int32_t topicId, int32_t partition, uint16_t templateId,
    //        const uint8_t* frame, std::size_t len)
    //
    // For FetchRequest (templateId 1) and ReconcileFetchReq (templateId 5):
    //   req.maxBytes is capped to maxFetchResponseBytes() BEFORE dispatch so
    //   the leader never builds a FetchResponse that exceeds the Aeron frame
    //   limit on the send side.  The dispatch callback receives the capped
    //   value.  A local copy of the decoded fields is built for this.
    //
    // Returns the number of fragments handled; 0 if not resolved().
    //
    // No std::function of our own — the only type-erasure is Aeron's own
    // fragment_handler_t at the Subscription::poll boundary (mirrors
    // WorkerIngress::poll exactly).
    // -----------------------------------------------------------------------
    template <class Dispatch>
    int poll(int fragmentLimit, Dispatch&& dispatch)
    {
        RUNE_FR_SPAN_REPLICATION(rune_fr::Site::ReplWorkerPoll, 0);
        if (!resolved()) [[unlikely]] return 0;

        using D = std::remove_reference_t<Dispatch>;

        if (testMode_) {
            // Test-mode: drain the in-process inbound queues (no real Aeron sub).
            // Two queues are drained in FIFO order across both:
            //   inboundQueue_ — frames stored as owned heap vectors (copy-inject path).
            //   viewQueue_    — frames stored as raw (ptr, len) views into caller-owned
            //                   backing buffers (zero-copy-inject path, mirrors Aeron
            //                   term-buffer dispatch for aliasing regression tests).
            // Honour fragmentLimit across both queues combined.
            int count = 0;
            while (count < fragmentLimit &&
                   (!inboundQueue_.empty() || !viewQueue_.empty())) {
                if (!inboundQueue_.empty()) {
                    auto [sessionId, frame] = std::move(inboundQueue_.front());
                    inboundQueue_.pop_front();
                    dispatchFrame_(frame.data(), frame.size(), sessionId, dispatch);
                } else {
                    const auto [ptr, len] = viewQueue_.front();
                    viewQueue_.pop_front();
                    // dispatch ptr DIRECTLY (zero-copy: pointer into caller's buffer).
                    // Valid ONLY for the duration of this dispatchFrame_ call, exactly
                    // as the production path is valid only inside the Aeron callback.
                    dispatchFrame_(ptr, len, /*sessionId=*/0, dispatch);
                }
                ++count;
            }
            return count;
        }

        // Production path: FragmentAssembler + trampoline, mirroring
        // WorkerIngress::poll exactly.
        //
        // ctx_ points to a PollCtx on this call's stack frame — valid for the
        // synchronous duration of sub_->poll() (single-threaded, no reentry).
        // The trampoline is a plain function pointer (no capture, no heap).
        struct PollCtx {
            WorkerReplicationTransportImpl* self;
            D*                              dispatch;
        };
        PollCtx pctx{ this, std::addressof(dispatch) };
        ctx_ = static_cast<void*>(&pctx);
        trampoline_ = [](void*          ctx,
                         const uint8_t* data,
                         std::size_t    len,
                         aeron::Header& hdr) {
            auto* pc = static_cast<PollCtx*>(ctx);
            pc->self->dispatchFrame_(data, len, hdr.sessionId(), *pc->dispatch);
        };

        int result = sub_->poll(assembler_.handler(), fragmentLimit);

        ctx_        = nullptr;
        trampoline_ = nullptr;
        return result;
    }

    // -----------------------------------------------------------------------
    // injectFrameForTest — feed an encoded SBE frame into the test-mode inbound
    // queue.  Must only be called when testMode_ is true (i.e. the test-seam
    // constructor was used).  poll() drains this queue instead of the real
    // Aeron subscription.
    //
    // This COPIES the bytes into a private heap vector (one vector per injected
    // frame) — safe for all tests that do not need to verify term-buffer aliasing
    // behaviour.
    // -----------------------------------------------------------------------
    void injectFrameForTest(const uint8_t* buf, std::size_t len, int32_t sessionId = 0)
    {
        inboundQueue_.emplace_back(sessionId, std::vector<uint8_t>(buf, buf + len));
    }

    // -----------------------------------------------------------------------
    // injectFrameViewForTest — inject a ZERO-COPY view into a caller-owned
    // backing buffer without copying bytes.
    //
    // The transport stores ONLY the raw pointer and length; it does NOT take
    // ownership of the memory.  poll() dispatches the raw pointer directly,
    // exactly as the production Aeron path dispatches a pointer into the term
    // buffer.
    //
    // This is the test seam for aliasing / lifetime regression tests:
    //   1. Caller fills `backingBuf` with frame1.
    //   2. `injectFrameViewForTest(backingBuf.data(), backingBuf.size())`
    //   3. poll(1, cb) — cb receives a pointer INTO backingBuf.  The contract:
    //      all data must be consumed INSIDE cb; the pointer is invalid after
    //      poll() returns.
    //   4. Caller overwrites `backingBuf` with frame2 bytes.
    //   5. `injectFrameViewForTest(backingBuf.data(), backingBuf.size())`
    //   6. poll(1, cb) — pointer still points to backingBuf (now frame2).
    //
    // IF a broken dispatch implementation had stored the raw pointer from step 3
    // and deferred reading it until after step 4, it would observe frame2 bytes
    // instead of frame1 bytes — causing the assertion on the captured value to
    // FAIL.  This is the real regression guard.
    //
    // The caller MUST keep the backing buffer alive and unmodified for the
    // duration of the poll() call that consumes the view.
    // -----------------------------------------------------------------------
    void injectFrameViewForTest(const uint8_t* buf, std::size_t len)
    {
        viewQueue_.emplace_back(buf, len);
    }

    // -----------------------------------------------------------------------
    // maxFetchResponseBytes — the maximum records payload that a FetchResponse
    // may carry without exceeding the Aeron frame limit on this transport's
    // publications.
    //
    // = pubReg_.maxMessageLength() - kFetchResponseOverhead
    //
    // The poll path caps FetchRequest.maxBytes to this value before dispatching
    // to onFetchRequest so the leader never builds an oversized response.
    // -----------------------------------------------------------------------
    [[nodiscard]] int32_t maxFetchResponseBytes() const noexcept
    {
        const int32_t kOverhead =
            static_cast<int32_t>(rune::replication::FetchResponseMsg::encodedSize(0));
        const int32_t ml = pubReg_.maxMessageLength();
        return (ml > kOverhead) ? (ml - kOverhead) : 0;
    }

    // =======================================================================
    // Shard plane handshake (spec §3.3 symmetric worker count; Task 6 ruling I2).
    //
    // Worker-count symmetry cannot be checked at config-parse time (a peer's
    // RUNE_WORKERS is not in RUNE_PEERS), so in shard mode every plane exchanges
    // a PlaneHello{nodeId, workerId, workerCount}.  Per peer (flat, <= kMaxPeers):
    //   Unverified — no hello yet: driveHandshake() sends ours (reply requested)
    //                every kHelloRetryNs of the injected clock until one arrives.
    //   Verified   — hello matched (same worker count, same plane index); holds
    //                the peer's image session id.  A reply-requested hello (peer
    //                restarted) is answered at once; no timer, no race.
    //   Refused    — hello mismatched: logged CRITICAL once, the peer's pub is
    //                dropped (nothing more is sent to it) and every frame from its
    //                image session is discarded before dispatch.  Terminal for the
    //                life of this transport (restart the node once fixed) — the
    //                planes are never split silently.
    // Frames from a not-yet-verified peer still flow (a mismatched peer's frames
    // for partitions this worker does not own are dropped by the worker anyway),
    // so the handshake adds no startup latency to a healthy cluster.
    // Worker-thread only (driven from the worker's replication poll).  Legacy /
    // flag-off: never enabled → driveHandshake() is one predicted branch.
    // =======================================================================
    static constexpr int64_t kHelloRetryNs = 10'000'000;   // 10 ms (injected clock)

    enum class PeerPlaneState : uint8_t { Unverified, Verified, Refused };

    /// Startup only (before the worker runner starts).  `peers` = the remote
    /// node ids on this plane (at most kMaxHandshakePeers; extra ids ignored).
    void enablePlaneHandshake(rune::replication::NodeId selfNodeId, int32_t selfWorkerCount,
                              const std::vector<rune::replication::NodeId>& peers) noexcept
    {
        hsEnabled_       = true;
        selfNodeId_      = selfNodeId;
        selfWorkerCount_ = selfWorkerCount;
        hsCount_         = 0;
        for (auto id : peers) {
            if (hsCount_ == kMaxHandshakePeers) break;
            hs_[hsCount_++] = PeerHandshake{id, PeerPlaneState::Unverified, 0, 0};
        }
        unverified_  = static_cast<uint32_t>(hsCount_);
        nextHelloNs_ = 0;   // first pass sends immediately
    }

    /// One call per worker pass (cold unless a peer is still unverified).
    int driveHandshake(int64_t nowNs) noexcept
    {
        if (!hsEnabled_ || unverified_ == 0) [[likely]] return 0;
        if (nowNs < nextHelloNs_) return 0;
        nextHelloNs_ = nowNs + kHelloRetryNs;
        int sent = 0;
        for (std::size_t i = 0; i < hsCount_; ++i) {
            if (hs_[i].state != PeerPlaneState::Unverified) continue;
            if (sendHello_(hs_[i].id, rune::replication::PlaneHelloMsg::kReplyRequested) > 0) ++sent;
        }
        return sent;
    }

    [[nodiscard]] bool     planeHandshakeEnabled() const noexcept { return hsEnabled_; }
    [[nodiscard]] bool     planeRefused() const noexcept { return refusedCount_ != 0; }
    [[nodiscard]] uint64_t planeRefusals() const noexcept { return refusals_; }
    [[nodiscard]] PeerPlaneState peerPlaneState(rune::replication::NodeId peer) const noexcept
    {
        for (std::size_t i = 0; i < hsCount_; ++i)
            if (hs_[i].id == peer) return hs_[i].state;
        return PeerPlaneState::Unverified;
    }

    // =======================================================================
    // PartitionTransport virtuals — SEND PATH
    //
    // Fixed-size messages encode on the stack and go out via sendToPeer /
    // broadcastToPeers (tryClaim).  FetchResponse / ReconcileResponse encode only
    // their SBE prefix on the stack and gather-send the records view
    // (sendGatherToPeer: tryClaim <= maxPayloadLength, else 2-buffer offer()).
    // On back-pressure (tryClaim/offer <= 0 / peer not found): drop.
    //      Replication is retriable; the follower re-issues its fetch.
    // =======================================================================

    // -----------------------------------------------------------------------
    // sendFetchRequest — BROADCAST to all peers.
    // FetchRequest has no explicit target on the wire; the current leader
    // responds; non-leaders drop via the role guard.
    // -----------------------------------------------------------------------
    void sendFetchRequest(const rune::replication::FetchRequestMsg& req) override
    {
        RUNE_FR_SPAN_REPLICATION(rune_fr::Site::ReplWorkerFetchReqSend,
                                 static_cast<uint32_t>(req.partition));
        constexpr std::size_t kSize = rune::replication::FetchRequestMsg::TOTAL_SIZE;
        alignas(8) uint8_t claimBuf[kSize];
        rune::replication::FetchRequestMsg::encode(
            claimBuf, 0,
            req.leaderEpoch, req.partition, req.followerId,
            req.fetchOffset, req.maxBytes, req.topicId);
        pubReg_.broadcastToPeers(claimBuf, kSize);
    }

    // -----------------------------------------------------------------------
    // sendFetchRequestTo — spec §3.5.2: UNICAST to the replica's known leader.
    // A leader that is not (yet) a registered peer falls back to the broadcast.
    // -----------------------------------------------------------------------
    void sendFetchRequestTo(rune::replication::NodeId leader,
                            const rune::replication::FetchRequestMsg& req) override
    {
        RUNE_FR_SPAN_REPLICATION(rune_fr::Site::ReplWorkerFetchReqSend,
                                 static_cast<uint32_t>(req.partition));
        constexpr std::size_t kSize = rune::replication::FetchRequestMsg::TOTAL_SIZE;
        alignas(8) uint8_t claimBuf[kSize];
        rune::replication::FetchRequestMsg::encode(
            claimBuf, 0, req.leaderEpoch, req.partition, req.followerId,
            req.fetchOffset, req.maxBytes, req.topicId);
        if (pubReg_.hasPeer(leader)) pubReg_.sendToPeer(leader, claimBuf, kSize);
        else                         pubReg_.broadcastToPeers(claimBuf, kSize);
    }

    // -----------------------------------------------------------------------
    // sendFetchResponse — direct-encode to toFollower's pub.
    // Variable-length (records tail) → size computed; fast path uses tryClaim
    // with in-claim encode (zero-copy for ≤ MTU frames).
    // -----------------------------------------------------------------------
    void sendFetchResponse(rune::replication::NodeId toFollower,
                           const rune::replication::FetchResponseMsg& resp) override
    {
        RUNE_FR_SPAN_REPLICATION(rune_fr::Site::ReplWorkerSend,
                                 static_cast<uint32_t>(resp.records.size()));
        sendFetchResponseImpl_(toFollower, resp, /*reconcile=*/false);
    }

    // -----------------------------------------------------------------------
    // sendOffsetsForLeaderEpochRequest — fixed-size, single peer.
    // -----------------------------------------------------------------------
    void sendOffsetsForLeaderEpochRequest(
            rune::replication::NodeId toLeader,
            const rune::replication::OffsetsForLeaderEpochReqMsg& req) override
    {
        constexpr std::size_t kSize = rune::replication::OffsetsForLeaderEpochReqMsg::TOTAL_SIZE;
        alignas(8) uint8_t claimBuf[kSize];
        rune::replication::OffsetsForLeaderEpochReqMsg::encode(
            claimBuf, 0,
            req.currentEpoch, req.partition, req.followerId,
            req.requestedEpoch, req.followerLeo, req.topicId);
        pubReg_.sendToPeer(toLeader, claimBuf, kSize);
    }

    // -----------------------------------------------------------------------
    // sendOffsetsForLeaderEpochResponse — fixed-size, single peer.
    // -----------------------------------------------------------------------
    void sendOffsetsForLeaderEpochResponse(
            rune::replication::NodeId toFollower,
            const rune::replication::OffsetsForLeaderEpochRespMsg& resp) override
    {
        constexpr std::size_t kSize = rune::replication::OffsetsForLeaderEpochRespMsg::TOTAL_SIZE;
        alignas(8) uint8_t claimBuf[kSize];
        rune::replication::OffsetsForLeaderEpochRespMsg::encode(
            claimBuf, 0,
            resp.leaderEpoch, resp.partition, resp.responseEpoch,
            resp.endOffset, resp.errorCode, resp.topicId);
        pubReg_.sendToPeer(toFollower, claimBuf, kSize);
    }

    // -----------------------------------------------------------------------
    // sendReconcileRequest — FetchRequest re-stamped as ReconcileFetchReq.
    // -----------------------------------------------------------------------
    void sendReconcileRequest(rune::replication::NodeId toPeer,
                              const rune::replication::FetchRequestMsg& req) override
    {
        constexpr std::size_t kSize = rune::replication::FetchRequestMsg::TOTAL_SIZE;
        alignas(8) uint8_t claimBuf[kSize];
        rune::replication::FetchRequestMsg::encode(
            claimBuf, 0,
            req.leaderEpoch, req.partition, req.followerId,
            req.fetchOffset, req.maxBytes, req.topicId);
        rune::replication::stampTemplateId(
            claimBuf, 0, rune::replication::kReconcileFetchReqTemplateId);
        pubReg_.sendToPeer(toPeer, claimBuf, kSize);
    }

    // -----------------------------------------------------------------------
    // sendReconcileResponse — FetchResponse re-stamped as ReconcileFetchResp.
    // -----------------------------------------------------------------------
    void sendReconcileResponse(rune::replication::NodeId toPeer,
                               const rune::replication::FetchResponseMsg& resp) override
    {
        sendFetchResponseImpl_(toPeer, resp, /*reconcile=*/true);
    }

private:
    // -----------------------------------------------------------------------
    // dispatchFrame_ — decode the SBE header and fixed fields, apply maxBytes
    // cap for FetchRequest variants, then invoke dispatch.
    //
    // Called from both the production trampoline (term-buffer pointer, valid
    // only for the call) and the test-mode queue drain (heap buffer, but same
    // zero-copy contract enforced: dispatch MUST NOT store the pointer).
    //
    // Template on Dispatch to keep the call inlined and avoid an extra virtual.
    // -----------------------------------------------------------------------
    template <class Dispatch>
    void dispatchFrame_(const uint8_t* frame, std::size_t len, int32_t sessionId, Dispatch& dispatch)
    {
        RUNE_FR_SPAN_REPLICATION(rune_fr::Site::ReplWorkerDispatch,
                                 static_cast<uint32_t>(len));
        if (len < rune::replication::ReplicationMessageHeader::ENCODED_SIZE)
            [[unlikely]] return;  // too short to have a valid header

        // Shard plane handshake: nothing from a refused peer's image is applied.
        // refusedCount_ is 0 unless a mismatched peer was seen (branch predicted).
        if (refusedCount_ != 0) [[unlikely]] {
            if (isRefusedSession_(sessionId)) return;
        }

        const uint16_t templateId =
            rune::replication::peekTemplateId(frame, 0);
        if (templateId == rune::replication::kPlaneHelloTemplateId) [[unlikely]] {
            onPlaneHello_(frame, len, sessionId);
            return;
        }

        // Extract (topicId, partition) from the fixed block (all message types
        // carry both fields at the same offsets relative to the block start):
        //
        //   FetchRequest (1) / ReconcileFetchReq (5): TOTAL_SIZE=40
        //     partition @ hdr+8, topicId @ hdr+28
        //   FetchResponse (2) / ReconcileFetchResp (6): variable
        //     partition @ hdr+8, topicId @ hdr+32
        //   OffsetsForLeaderEpochReq (3): TOTAL_SIZE=44
        //     partition @ hdr+8, topicId @ hdr+32
        //   OffsetsForLeaderEpochResp (4): TOTAL_SIZE=44
        //     partition @ hdr+8, topicId @ hdr+32
        //
        // We always read partition from hdr+8 (offset 10 in frame).
        // topicId location depends on the message type (28 vs 32 into block).

        namespace rr = rune::replication;
        constexpr std::size_t kHdrSize = rr::ReplicationMessageHeader::ENCODED_SIZE;

        // Minimum frame size check (need at least the header + partition field).
        if (len < kHdrSize + 12) [[unlikely]] return;

        const int32_t partition =
            rr::detail::read_le<int32_t>(frame + kHdrSize + 8);

        int32_t topicId = 0;

        switch (templateId) {
            case static_cast<uint16_t>(rr::ReplicationTemplateId::FetchRequest):
            case rr::kReconcileFetchReqTemplateId:
            {
                if (len < rr::FetchRequestMsg::TOTAL_SIZE) [[unlikely]] return;
                topicId = rr::detail::read_le<int32_t>(frame + kHdrSize + 28);

                // GR-1 carry-forward: cap maxBytes to the deliverable ceiling so the
                // leader never builds a FetchResponse that exceeds this transport's
                // Aeron frame limit.  Mirror AeronPartitionTransport::onFragment.
                //
                // We decode the full FetchRequest into a local copy, apply the cap,
                // then re-encode the capped frame into a scratch buffer and dispatch
                // the scratch pointer.  This keeps the zero-copy contract on the
                // OUTBOUND records path (the records span in the FetchResponse is the
                // zero-copy object; the capped FetchRequest is fixed-size and cheap).
                const int32_t ceiling = maxFetchResponseBytes();
                const int32_t rawMaxBytes =
                    rr::detail::read_le<int32_t>(frame + kHdrSize + 24);
                if (ceiling > 0 && rawMaxBytes > ceiling) {
                    // Build a capped copy and dispatch it instead of the original.
                    alignas(8) uint8_t cappedBuf[rr::FetchRequestMsg::TOTAL_SIZE];
                    std::memcpy(cappedBuf, frame, rr::FetchRequestMsg::TOTAL_SIZE);
                    rr::detail::write_le<int32_t>(
                        cappedBuf + kHdrSize + 24, ceiling);
                    dispatch(topicId, partition, templateId,
                             cappedBuf, rr::FetchRequestMsg::TOTAL_SIZE);
                    return;
                }
                break;
            }
            case static_cast<uint16_t>(rr::ReplicationTemplateId::FetchResponse):
            case rr::kReconcileFetchRespTemplateId:
            {
                if (len < rr::FetchResponseMsg::encodedSize(0)) [[unlikely]] return;
                // Generated SBE: topicId @ m_offset+30 (errorCode+2=@28, topicId@30;
                // no padding; sbeBlockLength=34).
                topicId = rr::detail::read_le<int32_t>(frame + kHdrSize + 30);
                break;
            }
            case static_cast<uint16_t>(rr::ReplicationTemplateId::OffsetsForLeaderEpochReq):
            {
                if (len < rr::OffsetsForLeaderEpochReqMsg::TOTAL_SIZE) [[unlikely]] return;
                // Generated SBE: topicId @ m_offset+32 (followerLeo@24+8=32).
                topicId = rr::detail::read_le<int32_t>(frame + kHdrSize + 32);
                break;
            }
            case static_cast<uint16_t>(rr::ReplicationTemplateId::OffsetsForLeaderEpochResp):
            {
                if (len < rr::OffsetsForLeaderEpochRespMsg::TOTAL_SIZE) [[unlikely]] return;
                // Hand-rolled encode (ReplicationMessages.h): errorCode@+28 (2 bytes),
                // alignment pad@+30 (2 bytes), topicId@+32 (4 bytes).
                // Note: the generated SBE places topicId@+30 (no pad), but the
                // wire frames are produced by the hand-rolled encode which inserts
                // a 2-byte pad, so we must read from +32 to match the actual wire layout.
                topicId = rr::detail::read_le<int32_t>(frame + kHdrSize + 32);
                break;
            }
            default:
                // Unknown templateId — log and drop.
                spdlog::trace("WorkerReplicationTransport worker={}: unknown templateId={} len={}",
                              workerId_, templateId, len);
                return;
        }

        dispatch(topicId, partition, templateId, frame, len);
    }

    // -----------------------------------------------------------------------
    // Plane handshake internals (cold path; worker thread only).
    // -----------------------------------------------------------------------
    struct PeerHandshake {
        rune::replication::NodeId id              = -1;
        PeerPlaneState            state           = PeerPlaneState::Unverified;
        int32_t                   sessionId       = 0;   // Verified / Refused: the peer's image session
        int32_t                   peerWorkerCount = 0;   // Refused: what the peer advertised
    };
    static constexpr std::size_t kMaxHandshakePeers = FollowerSlots::kMaxReplicas;

    int64_t sendHello_(rune::replication::NodeId peer, int32_t flags) noexcept
    {
        rune::replication::PlaneHelloMsg m;
        m.nodeId      = static_cast<int32_t>(selfNodeId_);
        m.workerId    = workerId_;
        m.workerCount = selfWorkerCount_;
        m.flags       = flags;
        alignas(8) uint8_t buf[rune::replication::PlaneHelloMsg::TOTAL_SIZE];
        rune::replication::PlaneHelloMsg::encode(buf, m);
        return pubReg_.sendToPeer(peer, buf, sizeof(buf));
    }

    [[nodiscard]] bool isRefusedSession_(int32_t sessionId) const noexcept
    {
        for (std::size_t i = 0; i < hsCount_; ++i)
            if (hs_[i].state == PeerPlaneState::Refused && hs_[i].sessionId == sessionId) return true;
        return false;
    }

    void onPlaneHello_(const uint8_t* frame, std::size_t len, int32_t sessionId) noexcept
    {
        namespace rr = rune::replication;
        rr::PlaneHelloMsg h;
        if (!hsEnabled_ || !rr::PlaneHelloMsg::decode(frame, len, h)) return;
        PeerHandshake* p = nullptr;
        for (std::size_t i = 0; i < hsCount_; ++i)
            if (hs_[i].id == static_cast<rr::NodeId>(h.nodeId)) { p = &hs_[i]; break; }
        if (!p) {
            spdlog::warn("WorkerReplicationTransport[{}]: PlaneHello from unknown node {} ignored",
                         workerId_, h.nodeId);
            return;
        }
        if (p->state == PeerPlaneState::Refused) return;   // terminal
        if (h.workerCount != selfWorkerCount_ || h.workerId != workerId_) {
            spdlog::critical(
                "WorkerReplicationTransport[{}]: shard mode REQUIRES a symmetric worker count — "
                "node {} runs RUNE_WORKERS={} (its plane {}) but this node {} runs {} (plane {}). "
                "REFUSING to replicate with node {} on this plane (never split planes silently); "
                "fix RUNE_WORKERS and restart", workerId_, h.nodeId, h.workerCount, h.workerId,
                selfNodeId_, selfWorkerCount_, workerId_, h.nodeId);
            if (p->state == PeerPlaneState::Unverified) --unverified_;
            p->state           = PeerPlaneState::Refused;
            p->sessionId       = sessionId;
            p->peerWorkerCount = h.workerCount;
            ++refusedCount_;
            ++refusals_;
            pubReg_.dropPeer(p->id);
            return;
        }
        if (p->state == PeerPlaneState::Unverified) {
            --unverified_;
            p->state = PeerPlaneState::Verified;
            spdlog::info("WorkerReplicationTransport[{}]: plane verified with node {} ({} workers)",
                         workerId_, h.nodeId, h.workerCount);
        }
        p->sessionId = sessionId;
        if (h.flags & rr::PlaneHelloMsg::kReplyRequested) (void)sendHello_(p->id, 0);
    }

    // -----------------------------------------------------------------------
    // sendFetchResponseImpl_ — shared implementation for sendFetchResponse and
    // sendReconcileResponse (reconcile=true re-stamps the templateId).
    //
    // Zero-copy (spec §3.5a.2): only the fixed SBE prefix (header + fixed block +
    // records length) is encoded, on the stack; resp.records (a view into the
    // log) is handed to sendGatherToPeer, which copies it ONCE into the term
    // buffer.  No scratch buffer, no allocation.
    // -----------------------------------------------------------------------
    static constexpr std::size_t kFetchResponsePrefixMax = 64;   // encodedSize(0) == 46

    void sendFetchResponseImpl_(rune::replication::NodeId peer,
                                const rune::replication::FetchResponseMsg& resp,
                                bool reconcile) noexcept
    {
        namespace rr = rune::replication;
        static const uint8_t kNoRecords = 0;
        alignas(8) uint8_t prefix[kFetchResponsePrefixMax];
        const std::size_t prefixLen = rr::FetchResponseMsg::encodedSize(0);
        // Encode header + fixed block + a zero-length records prefix, then patch the
        // real length: the body is handed over separately (never copied here).
        rr::FetchResponseMsg::encode(prefix, 0, resp.leaderEpoch, resp.partition,
                                     resp.highWatermark, resp.logStartOffset,
                                     resp.errorCode,
                                     std::span<const uint8_t>(&kNoRecords, 0),
                                     resp.topicId);
        rr::detail::write_le<uint32_t>(prefix + prefixLen - rr::FetchResponseMsg::VAR_HEADER_SIZE,
                                       static_cast<uint32_t>(resp.records.size()));
        if (reconcile) rr::stampTemplateId(prefix, 0, rr::kReconcileFetchRespTemplateId);
        pubReg_.sendGatherToPeer(peer, prefix, prefixLen, resp.records);
    }

    // -----------------------------------------------------------------------
    // Members
    // -----------------------------------------------------------------------
    int32_t     workerId_;
    std::string inboundChannel_;
    int32_t     inboundStreamId_;

    std::shared_ptr<aeron::Subscription> sub_;  // inbound (resolved by ctor)

    // -----------------------------------------------------------------------
    // Fragment assembler + trampoline — mirrors WorkerIngress EXACTLY.
    //
    // assembler_ is a member (lambda captures `this`) so it outlives any
    // poll() call.  trampoline_/ctx_ are set for the duration of each poll()
    // call and cleared immediately after.  No std::function of our own on the
    // hot path — Aeron's fragment_handler_t is the ONLY type-erasure boundary.
    // -----------------------------------------------------------------------
    using TrampolineFn = void(*)(void*              ctx,
                                 const uint8_t*     data,
                                 std::size_t        len,
                                 aeron::Header&     hdr);

    void*          ctx_        = nullptr;
    TrampolineFn   trampoline_ = nullptr;

    aeron::FragmentAssembler assembler_;

    PubRegistry pubReg_;  // per-peer pub management + send policy

    // Test-mode inbound queue: feed frames via injectFrameForTest().
    // poll() drains this instead of the real Aeron subscription in testMode_.
    // Each entry is an OWNED copy of the encoded frame bytes.
    std::deque<std::pair<int32_t, std::vector<uint8_t>>> inboundQueue_;   // (sessionId, frame)

    // Test-mode zero-copy view queue: feed raw pointer views via
    // injectFrameViewForTest().  Entries are (ptr, len) pairs into
    // caller-owned backing buffers — NOT owned by this queue.
    // Simulates the Aeron term-buffer dispatch for aliasing regression tests.
    std::deque<std::pair<const uint8_t*, std::size_t>> viewQueue_;

    // True when constructed via the test-seam constructor (no real Aeron sub).
    bool testMode_ = false;

    // Shard plane handshake (see enablePlaneHandshake).  Worker-thread only.
    std::array<PeerHandshake, kMaxHandshakePeers> hs_{};
    std::size_t               hsCount_         = 0;
    uint32_t                  unverified_      = 0;
    uint32_t                  refusedCount_    = 0;
    uint64_t                  refusals_        = 0;
    int64_t                   nextHelloNs_     = 0;
    rune::replication::NodeId selfNodeId_      = -1;
    int32_t                   selfWorkerCount_ = 0;
    bool                      hsEnabled_       = false;
};

// ---------------------------------------------------------------------------
// WorkerReplicationTransport — production alias (AeronPubRegistry).
// ---------------------------------------------------------------------------
using WorkerReplicationTransport = WorkerReplicationTransportImpl<AeronPubRegistry>;

} // namespace rune::replication
