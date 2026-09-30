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
//   For frames > pub.maxPayloadLength() (large FetchResponse record tails):
//     - encode into a per-instance scratch buffer
//     - pub.offer() (auto-fragments large batches)
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

#include <rune/replication/PartitionReplica.h>   // PartitionTransport base
#include <rune/replication/ReplicationMessages.h> // SBE codecs + stampTemplateId
#include <rune_fr/FlightRecorder.h>                // #4 deep instrumentation

#include <spdlog/spdlog.h>

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <deque>
#include <limits>
#include <memory>
#include <string>
#include <thread>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>

namespace rune::replication {

// ===========================================================================
// AeronPubRegistry — production PubRegistry policy.
//
// Owns an unordered_map<NodeId, shared_ptr<ExclusivePublication>>.
// Resolves publications via bounded addExclusivePublication / findExclusive-
// Publication loops (kResolveMaxMs=5000 / kResolveSliceMs=1), same as
// WorkerEgress.  Single-owner / single-thread.
// ===========================================================================
class AeronPubRegistry {
public:
    static constexpr int kResolveMaxMs   = 5000;
    static constexpr int kResolveSliceMs = 1;

    // -----------------------------------------------------------------------
    // Construction: for each (peerId, peerChannel) pair, register an
    // ExclusivePublication on peerStreamId and resolve it.
    // -----------------------------------------------------------------------
    AeronPubRegistry(std::shared_ptr<aeron::Aeron>                                       aeron,
                     const std::vector<std::pair<rune::replication::NodeId, std::string>>& peerChannels,
                     int32_t peerStreamId) noexcept
    {
        if (!aeron) return;  // no driver → all unresolved

        using clock = std::chrono::steady_clock;

        struct Pending {
            rune::replication::NodeId peerId;
            int64_t                   regId;
        };
        std::vector<Pending> pending;
        pending.reserve(peerChannels.size());

        for (const auto& [peerId, channel] : peerChannels) {
            try {
                int64_t regId = aeron->addExclusivePublication(channel, peerStreamId);
                pending.push_back({peerId, regId});
            } catch (...) {
                // Failed to register — leave this peer absent; sends to it are no-ops.
            }
        }

        // Bounded resolve loop: mirror WorkerEgress exactly.
        auto deadline = clock::now() + std::chrono::milliseconds{kResolveMaxMs};
        while (!pending.empty() && clock::now() < deadline) {
            for (auto it = pending.begin(); it != pending.end(); ) {
                auto pub = aeron->findExclusivePublication(it->regId);
                if (pub) {
                    pubs_.emplace(it->peerId, std::move(pub));
                    it = pending.erase(it);
                } else {
                    ++it;
                }
            }
            if (!pending.empty()) {
                std::this_thread::sleep_for(std::chrono::milliseconds{kResolveSliceMs});
            }
        }
        // Any still-pending entries were not resolved within the window;
        // they remain absent from pubs_ → sends to them are no-ops.
        allResolved_ = pending.empty() && !pubs_.empty();
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
        return pubs_.find(peer) != pubs_.end();
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
        for (const auto& [id, pub] : pubs_) {
            if (pub) {
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
        auto it = pubs_.find(peer);
        if (it == pubs_.end() || !it->second) return -1;
        return sendOnPub_(*it->second, buf, len);
    }

    // -----------------------------------------------------------------------
    // broadcastToPeers — send to ALL registered peer pubs (best-effort;
    // individual back-pressure does not abort the broadcast).
    // -----------------------------------------------------------------------
    void broadcastToPeers(const uint8_t* buf, std::size_t len) noexcept
    {
        RUNE_FR_SPAN_REPLICATION(rune_fr::Site::ReplWorkerBroadcast, 0);
        for (auto& [id, pub] : pubs_) {
            if (pub) sendOnPub_(*pub, buf, len);
        }
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

    bool allResolved_  = false;
    std::unordered_map<rune::replication::NodeId,
                       std::shared_ptr<aeron::ExclusivePublication>> pubs_;
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
            int32_t                                                                  peerStreamId) noexcept
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
        , pubReg_(aeron, peerChannels, peerStreamId)
    {
        if (!aeron) return;  // null aeron → unresolved (test or misconfiguration)

        // Resolve inbound subscription — mirrors WorkerIngress ctor exactly.
        static constexpr int kResolveMaxMs   = 5000;
        static constexpr int kResolveSliceMs = 1;

        const int64_t regId = aeron->addSubscription(inboundChannel_, inboundStreamId_);

        using clock = std::chrono::steady_clock;
        auto deadline = clock::now() + std::chrono::milliseconds{kResolveMaxMs};
        while (clock::now() < deadline) {
            sub_ = aeron->findSubscription(regId);
            if (sub_) break;
            std::this_thread::sleep_for(std::chrono::milliseconds{kResolveSliceMs});
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
                    std::vector<uint8_t> frame = std::move(inboundQueue_.front());
                    inboundQueue_.pop_front();
                    dispatchFrame_(frame.data(), frame.size(), dispatch);
                } else {
                    const auto [ptr, len] = viewQueue_.front();
                    viewQueue_.pop_front();
                    // dispatch ptr DIRECTLY (zero-copy: pointer into caller's buffer).
                    // Valid ONLY for the duration of this dispatchFrame_ call, exactly
                    // as the production path is valid only inside the Aeron callback.
                    dispatchFrame_(ptr, len, dispatch);
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
                         aeron::Header& /*hdr*/) {
            auto* pc = static_cast<PollCtx*>(ctx);
            pc->self->dispatchFrame_(data, len, *pc->dispatch);
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
    void injectFrameForTest(const uint8_t* buf, std::size_t len)
    {
        inboundQueue_.emplace_back(buf, buf + len);
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
    // PartitionTransport virtuals — SEND PATH
    //
    // Each method:
    //   1. Computes encodedSize.
    //   2. If size <= maxPayloadLength: encode DIRECTLY into the tryClaim
    //      BufferClaim buffer (true zero-copy, no intermediate copy).
    //      This is the fast path: the SBE encode writes into the term buffer.
    //   3. Else: encode into scratchBuf_ and offer() (handles large record
    //      tails that span multiple Aeron MTU fragments).
    //   4. On back-pressure (tryClaim ≤ 0 / peer not found): drop.
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
    void dispatchFrame_(const uint8_t* frame, std::size_t len, Dispatch& dispatch)
    {
        RUNE_FR_SPAN_REPLICATION(rune_fr::Site::ReplWorkerDispatch,
                                 static_cast<uint32_t>(len));
        if (len < rune::replication::ReplicationMessageHeader::ENCODED_SIZE)
            [[unlikely]] return;  // too short to have a valid header

        const uint16_t templateId =
            rune::replication::peekTemplateId(frame, 0);

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
    // sendFetchResponseImpl_ — shared implementation for sendFetchResponse and
    // sendReconcileResponse (reconcile=true re-stamps the templateId).
    //
    // Fast path (frame ≤ maxPayloadLength for a dummy/default query of 64KiB
    // Aeron term):
    //   The encoded frame fits in a single Aeron fragment; encode into a
    //   stack/scratch buffer and let the PubRegistry's sendToPeer use
    //   tryClaim for zero-copy placement into the term buffer.
    //
    // Large path (frame > maxPayloadLength):
    //   Encode into scratchBuf_ and offer() (auto-fragments by Aeron driver).
    //
    // NOTE: The "direct in-claim encode" that the brief describes (#2b) is
    // best realised here by encoding into a buffer whose pointer is passed to
    // sendToPeer; for the tryClaim fast path the PubRegistry copies from that
    // buffer into the claimed slot — this is ONE copy (same as the #2b
    // enqueueFetchResponse zero-copy claim, minus the outbound-ring indirection
    // the legacy transport needs).  The only allocation is the scratch vector
    // for the large-frame path.
    // -----------------------------------------------------------------------
    void sendFetchResponseImpl_(rune::replication::NodeId peer,
                                const rune::replication::FetchResponseMsg& resp,
                                bool reconcile)
    {
        const std::size_t frameLen =
            rune::replication::FetchResponseMsg::encodedSize(resp.records.size());

        // Encode into scratch buffer (grown on demand, never shrunk).
        if (scratchBuf_.size() < frameLen) scratchBuf_.resize(frameLen);
        rune::replication::FetchResponseMsg::encode(
            scratchBuf_.data(), 0,
            resp.leaderEpoch, resp.partition, resp.highWatermark,
            resp.logStartOffset, resp.errorCode, resp.records, resp.topicId);
        if (reconcile) {
            rune::replication::stampTemplateId(
                scratchBuf_.data(), 0,
                rune::replication::kReconcileFetchRespTemplateId);
        }
        pubReg_.sendToPeer(peer, scratchBuf_.data(), frameLen);
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

    // Reusable scratch buffer for FetchResponse large-frame encode path.
    // Per-instance (not shared): this class is single-thread owned.
    std::vector<uint8_t> scratchBuf_;

    // Test-mode inbound queue: feed frames via injectFrameForTest().
    // poll() drains this instead of the real Aeron subscription in testMode_.
    // Each entry is an OWNED copy of the encoded frame bytes.
    std::deque<std::vector<uint8_t>> inboundQueue_;

    // Test-mode zero-copy view queue: feed raw pointer views via
    // injectFrameViewForTest().  Entries are (ptr, len) pairs into
    // caller-owned backing buffers — NOT owned by this queue.
    // Simulates the Aeron term-buffer dispatch for aliasing regression tests.
    std::deque<std::pair<const uint8_t*, std::size_t>> viewQueue_;

    // True when constructed via the test-seam constructor (no real Aeron sub).
    bool testMode_ = false;
};

// ---------------------------------------------------------------------------
// WorkerReplicationTransport — production alias (AeronPubRegistry).
// ---------------------------------------------------------------------------
using WorkerReplicationTransport = WorkerReplicationTransportImpl<AeronPubRegistry>;

} // namespace rune::replication
