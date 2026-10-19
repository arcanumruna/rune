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
//     bool    broadcastToPeers(const uint8_t* buf, std::size_t len)   // true iff EVERY peer accepted
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
#include <rune/aeronutil/FragmentAssembler.h>
#include <concurrent/AtomicBuffer.h>
#include <concurrent/logbuffer/BufferClaim.h>
#include <concurrent/logbuffer/FrameDescriptor.h>
#include <util/Index.h>

#include <rune/aeronutil/AeronPump.h>          // spec §3.6 invoker-aware resolve
#include <rune/replication/FollowerSlots.h>      // kMaxReplicas (flat peer array bound)
#include <rune/replication/PartitionReplica.h>   // PartitionTransport base
#include <rune/replication/PubAddBroker.h>       // legacy-plane control-thread re-adds
#include <rune/consensus/PeerResolution.h>       // off-thread resolution events
#include <rune/replication/ReplicationFrameLimits.h> // F18 frame limits (one source)
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
#include <variant>
#include <vector>

namespace rune::replication {

// ===========================================================================
// AeronPubRegistry — production PubRegistry policy.
//
// Owns a FLAT array of (NodeId, shared_ptr<ExclusivePublication>) — at most
// FollowerSlots::kMaxReplicas peers; a linear scan beats unordered_map::find on
// every send (spec §3.5a.2 fix on sight).
// Resolves publications via bounded addExclusivePublication + findExclusive-
// Publication resolve via AeronPump (until Aeron answers — AeronPump::kUntilAnswered, bounded by Aeron's driverTimeoutMs; 1 ms sleep with the
// default pump, invoker pumping otherwise), same as
// WorkerEgress.  Single-owner / single-thread.
// ===========================================================================
class AeronPubRegistry {
public:

    // -----------------------------------------------------------------------
    // Construction: for each (peerId, peerChannel) pair, register an
    // ExclusivePublication on peerStreamId and resolve it.
    // -----------------------------------------------------------------------
    AeronPubRegistry(std::shared_ptr<aeron::Aeron>                                       aeron,
                     const std::vector<std::pair<rune::replication::NodeId, std::string>>& peerChannels,
                     int32_t peerStreamId,
                     rune::aeronutil::AeronPump pump = {}) noexcept
    {
        attach(std::move(aeron), peerChannels, peerStreamId, pump);
    }

    /// Unattached registry (no pubs; allResolved()==false; every send a no-op)
    /// until attach() — lets a shard plane exist before its client does.
    AeronPubRegistry() noexcept = default;

    /// DR-15 fix round 1: the resolution events of this registry's owner thread
    /// (the off-thread PeerResolverHub).  Cold; before attach().
    /// Drains the prefilled ring at once (startup thread, before the owner runs)
    /// so startup adds already go out on the NUMERIC address (DR-4).
    void attachResolution(rune::consensus::PeerResolutionSource src) {
        inbox_.attach(src);
        (void)inbox_.poll([this](int idx) { onHostResolved_(idx); });
    }

    /// LEGACY planes (they ride the node client): every re-add after startup is
    /// made by the CONTROL thread through `broker` (review MEDIUM-2).  Before
    /// the owner thread runs.
    void setAddBroker(PubAddBroker* broker) noexcept { broker_ = broker; }

    /// Register every peer pub and resolve what Aeron answers at startup (the
    /// former ctor body).  Once only, on the startup thread.
    ///
    /// DR-15 (2026-10-05): a peer whose registration ERRORS — its hostname does
    /// not resolve yet (the peer pod/container is itself restarting, DNS lag) —
    /// is NOT fatal and NOT dropped: it stays Resolving (pub == nullptr: every
    /// data send to it is refused exactly like a not-connected peer).  Fix round
    /// 1: it is re-added ONLY on its host's resolution event (inbox_, fed by the
    /// cold resolver thread) — never on a pass count, and no duty thread ever
    /// resolves a name (the driver answers peer hosts from its off-thread cache).
    /// From then on it is a peer that connected late (plane hello, ISR / catch-up).
    void attach(std::shared_ptr<aeron::Aeron>                                       aeron,
                const std::vector<std::pair<rune::replication::NodeId, std::string>>& peerChannels,
                int32_t peerStreamId,
                rune::aeronutil::AeronPump pump = {}) noexcept
    {
        if (!aeron) return;  // no driver → all unresolved
        aeron_      = aeron;
        streamId_   = peerStreamId;
        // Local configuration validity (fatal at the caller): >= 1 peer and no
        // more than the flat array holds.  Peer RESOLUTION never decides it.
        configured_ = !peerChannels.empty() && peerChannels.size() <= kMaxPeers;
        try {
            resolving_.reserve(peerChannels.size());
        } catch (...) {}
        PubAddBroker* const broker = broker_;
        broker_ = nullptr;   // startup adds: direct, on this (startup) thread
        for (const auto& [peerId, channel] : peerChannels) {
            if (pubCount_ == kMaxPeers) break;
            pubs_[pubCount_] = PeerPub{peerId, nullptr, false};
            try {
                cold_[pubCount_] = PeerCold{channel, inbox_.hostIndexOfChannel(channel)};
                resolving_.push_back(Resolving{pubCount_, channel, inbox_.hostIndexOfChannel(channel)});
            } catch (...) {
                configured_ = false;   // allocation failure at startup: refuse (caller throws)
                break;
            }
            ++pubCount_;
            addNow_(resolving_.back(), /*retry=*/false);
        }

        // Startup resolve (spec §3.6): until Aeron ANSWERS every registration
        // (ready or error) — default pump sleeps 1 ms between probes, an invoker
        // pump drives this shard's own driver + conductor instead.  An errored
        // peer waits for its host's resolution event (never fatal).
        (void)pump.resolve([&] {
            (void)inbox_.poll([this](int idx) { onHostResolved_(idx); });
            bool anyPending = false;
            for (std::size_t i = 0; i < resolving_.size(); ) {
                Resolving& r = resolving_[i];
                if (r.regId >= 0) {
                    const int st = probe_(r);
                    if (st > 0) { eraseResolving_(i); continue; }
                    if (st == 0 || r.regId >= 0) anyPending = true;
                }
                ++i;
            }
            return !anyPending;
        }, rune::aeronutil::AeronPump::kUntilAnswered);   // D8: until Aeron answers (errors stop it)
        broker_      = broker;
        allResolved_ = resolving_.empty() && configured_;
    }

    /// One call per owner-thread duty pass: drains the resolution events (an
    /// empty ring is a few loads) and — only while a peer is Resolving — re-adds
    /// a peer whose host just resolved and probes in-flight registrations.
    /// Never resolves a name, never on a pass count.  Returns work done.
    int drivePeerResolve() noexcept
    {
        int work = inbox_.poll([this](int idx) { onHostResolved_(idx); });
        if (resolving_.empty()) [[likely]] return work;
        if (broker_ != nullptr)
            work += broker_->pollResults([this](rune::replication::NodeId peer, PubAddBroker::PubPtr pub) {
                onBrokerResult_(peer, std::move(pub));
            });
        for (std::size_t i = 0; i < resolving_.size(); ) {
            Resolving& r = resolving_[i];
            if (r.regId < 0) { ++i; continue; }   // waiting for an event / at the broker
            const int st = probe_(r);
            if (st > 0) { ++work; eraseResolving_(i); continue; }
            if (st < 0) ++work;
            ++i;
        }
        allResolved_ = resolving_.empty() && configured_;
        return work;
    }

    /// Local configuration valid (>= 1 peer, <= kMaxPeers) — independent of
    /// whether every peer has resolved yet.
    [[nodiscard]] bool configured() const noexcept { return configured_; }

    /// Peers still Resolving (pub not yet created).
    [[nodiscard]] std::size_t resolvingPeerCount() const noexcept { return resolving_.size(); }

    /// Total publication re-adds issued after a registration error.
    [[nodiscard]] uint64_t peerReAdds() const noexcept { return reAdds_; }

    // Non-copyable, non-movable.
    AeronPubRegistry(const AeronPubRegistry&)            = delete;
    AeronPubRegistry& operator=(const AeronPubRegistry&) = delete;
    AeronPubRegistry(AeronPubRegistry&&)                 = delete;
    AeronPubRegistry& operator=(AeronPubRegistry&&)      = delete;

    // -----------------------------------------------------------------------
    // allResolved — true when every peer pub resolved AND there is ≥1 peer.
    // -----------------------------------------------------------------------
    [[nodiscard]] bool allResolved() const noexcept { return allResolved_; }

    /// F3 round 6: frames dropped because they exceeded a pub's maxMessageLength.
    [[nodiscard]] uint64_t oversizeDrops() const noexcept { return oversizeDrops_; }

    [[nodiscard]] bool hasPeer(rune::replication::NodeId peer) const noexcept
    {
        return findPub_(peer) != nullptr;
    }

    // -----------------------------------------------------------------------
    // setPeerBlocked — shard plane refusal (cold path).  A blocked peer is
    // invisible to every DATA send (sendToPeer / sendGatherToPeer / broadcast /
    // hasPeer); control frames still reach it via sendControlToPeer, so a peer
    // that is fixed can heal the plane.  Unblocking restores data sends.
    // -----------------------------------------------------------------------
    void setPeerBlocked(rune::replication::NodeId peer, bool blocked) noexcept
    {
        for (std::size_t i = 0; i < pubCount_; ++i)
            if (pubs_[i].id == peer) pubs_[i].blocked = blocked;
    }

    /// Control frame (PlaneHello) — ignores the data block.
    int64_t sendControlToPeer(rune::replication::NodeId peer,
                              const uint8_t* buf, std::size_t len) noexcept
    {
        for (std::size_t i = 0; i < pubCount_; ++i)
            if (pubs_[i].id == peer && pubs_[i].pub) return sendOnPub_(*pubs_[i].pub, buf, len);
        return -1;
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
    // broadcastToPeers — send to ALL registered peer pubs (individual back-
    // pressure does not abort the broadcast).  F27: true iff at least one
    // peer exists and EVERY data peer accepted.  Deliberately all-or-nothing
    // (review F5): the broadcast is only used for a FetchRequest whose leader is
    // unknown/unresolved, so we cannot tell WHICH peer must get it — a partial
    // success re-sends next pass to every peer.  The duplicates are tiny and
    // idempotent at the leader (non-leaders drop them by role), and a refused
    // follower send is not counted as work (F4), so this never spins a core.
    // -----------------------------------------------------------------------
    bool broadcastToPeers(const uint8_t* buf, std::size_t len) noexcept
    {
        RUNE_FR_SPAN_REPLICATION(rune_fr::Site::ReplWorkerBroadcast, 0);
        bool all = false;
        bool anyRefused = false;
        for (std::size_t i = 0; i < pubCount_; ++i) {
            if (pubs_[i].pub && !pubs_[i].blocked) {
                all = true;
                if (sendOnPub_(*pubs_[i].pub, buf, len) <= 0) anyRefused = true;
            }
        }
        return all && !anyRefused;
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
        if (len > static_cast<std::size_t>(pub->maxMessageLength())) [[unlikely]] {
            // F3 round 6: counted + rate-limited (first, then every 1024th).
            if (oversizeDrops_++ % 1024 == 0) {
                spdlog::error("WorkerReplicationTransport: drop oversized frame len={} max={} "
                              "(drops={})", len, pub->maxMessageLength(), oversizeDrops_);
            }
            return -1;
        }
        if (len <= static_cast<std::size_t>(pub->maxPayloadLength())) {
            aeron::concurrent::logbuffer::BufferClaim claim;
            int64_t pos = pub->tryClaim(static_cast<aeron::util::index_t>(len), claim);
            if (pos == aeron::ADMIN_ACTION) [[unlikely]]   // A9: term rotated — retry now (Aeron contract)
                pos = pub->tryClaim(static_cast<aeron::util::index_t>(len), claim);
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
        const int64_t r = pub->offer(parts);
        return r == aeron::ADMIN_ACTION ? pub->offer(parts) : r;   // A9: term rotated — retry now
    }

private:
    // Zero-copy tryClaim for ≤ maxPayloadLength; offer() for larger frames.
    // Mirrors Worker::sendEncodedToPub_ (Worker.h:1064-1085) plus the
    // defence-in-depth maxMessageLength guard from AeronPartitionTransport::sendOnPub.
    int64_t sendOnPub_(aeron::ExclusivePublication& pub,
                       const uint8_t* buf, std::size_t len) noexcept
    {
        RUNE_FR_SPAN_REPLICATION(rune_fr::Site::ReplWorkerSendOnPub,
                                 static_cast<uint32_t>(len));
        // Guard: offer() THROWS IllegalArgumentException if len > maxMessageLength.
        // Drop and log (rate-limited, F3 r6) — the follower retries on its next fetch.
        if (len > static_cast<std::size_t>(pub.maxMessageLength())) [[unlikely]] {
            if (oversizeDrops_++ % 1024 == 0) {
                spdlog::error("WorkerReplicationTransport::sendOnPub_: drop oversized frame "
                              "len={} maxMessageLength={} — replication term too small "
                              "(drops={})", len, pub.maxMessageLength(), oversizeDrops_);
            }
            return -1;
        }

        auto* mutableBuf = const_cast<uint8_t*>(buf);
        aeron::concurrent::AtomicBuffer atomicBuf(
            mutableBuf, static_cast<aeron::util::index_t>(len));

        if (len <= static_cast<std::size_t>(pub.maxPayloadLength())) {
            aeron::concurrent::logbuffer::BufferClaim claim;
            int64_t pos = pub.tryClaim(static_cast<aeron::util::index_t>(len), claim);
            if (pos == aeron::ADMIN_ACTION) [[unlikely]]   // A9: term rotated — retry now (Aeron contract)
                pos = pub.tryClaim(static_cast<aeron::util::index_t>(len), claim);
            if (pos > 0) {
                claim.buffer().putBytes(
                    claim.offset(), atomicBuf, 0,
                    static_cast<aeron::util::index_t>(len));
                claim.commit();
            }
            return pos;
        }
        const int64_t r = pub.offer(atomicBuf, 0, static_cast<aeron::util::index_t>(len));
        return r == aeron::ADMIN_ACTION ? pub.offer(atomicBuf, 0, static_cast<aeron::util::index_t>(len)) : r;
    }

    struct PeerPub {
        rune::replication::NodeId                    id = -1;
        std::shared_ptr<aeron::ExclusivePublication> pub;
        bool                                         blocked = false;   // shard plane refusal
    };
    static constexpr std::size_t kMaxPeers = FollowerSlots::kMaxReplicas;
    // LOW-7: the outstanding broker requests per peer — at most one kRelease +
    // one kAdd (D24(b) IP-change re-add) — always fit the request ring
    // (ControlRing<1024>: 1024 / 24-byte records = 42), so requestAdd never
    // fails for capacity (a full ring on release keeps the stale pub; retried).
    static_assert(2 * kMaxPeers <= 1024 / 24, "PubAddBroker request ring must hold two requests per peer");

    [[nodiscard]] aeron::ExclusivePublication* findPub_(rune::replication::NodeId id) const noexcept
    {
        for (std::size_t i = 0; i < pubCount_; ++i)
            if (pubs_[i].id == id) return pubs_[i].blocked ? nullptr : pubs_[i].pub.get();
        return nullptr;
    }

    // -----------------------------------------------------------------------
    // Resolving peer (cold; owner thread only).  regId >= 0: a registration is
    // in flight; kWaiting: waiting for its host's resolution event; kBrokered:
    // an add request is at the control thread (legacy planes).
    // -----------------------------------------------------------------------
    static constexpr int64_t  kWaiting  = -1;
    static constexpr int64_t  kBrokered = -2;
    static constexpr uint64_t kLogEvery = 1024;   // first + every 1024th failure
    struct Resolving {
        std::size_t slot    = 0;      // index into pubs_
        std::string channel;
        int         hostIdx = -1;     // managed peer host (-1: unmanaged → no re-add)
        int64_t     regId   = kWaiting;
        uint32_t    addGen  = 0;      // host resolution generation at the last add
        uint32_t    failures = 0;
    };

    void addNow_(Resolving& r, bool retry) noexcept
    {
        if (retry) ++reAdds_;
        r.addGen = inbox_.generation(r.hostIdx);
        cold_[r.slot].boundKnown = inbox_.addressOf(r.hostIdx, cold_[r.slot].boundIp);
        if (broker_ != nullptr) {   // D24(b): on the numeric address this inbox holds
            r.regId = broker_->requestAdd(pubs_[r.slot].id, cold_[r.slot].boundIp, cold_[r.slot].boundKnown)
                          ? kBrokered : kWaiting;
            return;
        }
        const auto aeron = aeron_.lock();
        if (!aeron) { r.regId = kWaiting; return; }   // client closed (teardown)
        try {
            r.regId = aeron->addExclusivePublication(inbox_.addChannel(r.channel, r.hostIdx), streamId_);
        } catch (const std::exception& e) {
            onFailed_(r, e.what());
        }
    }

    /// The registration failed: re-add at once if the host resolved (again)
    /// since that add was issued, else wait for the next resolution event.
    void onFailed_(Resolving& r, const char* why) noexcept
    {
        ++r.failures;
        if ((failures_++ % kLogEvery) == 0)
            spdlog::warn("AeronPubRegistry: peer {} publication {} unresolved: {} — peer stays Resolving "
                         "until its host resolves ({} failure(s) so far)",
                         pubs_[r.slot].id, r.channel, why, failures_);
        r.regId = kWaiting;
        if (r.hostIdx >= 0 && inbox_.generation(r.hostIdx) != r.addGen) addNow_(r, /*retry=*/true);
    }

    void onHostResolved_(int idx) noexcept
    {
        for (auto& r : resolving_)
            if (r.hostIdx == idx && r.regId == kWaiting) addNow_(r, /*retry=*/true);
        // DR-4 (2026-10-07): a RESOLVED peer whose host now resolves to a
        // different address (a restarted container / pod got a new IP — two
        // restarted peers can swap theirs).  Its publication still sends to the
        // old address, where ANOTHER member's inbound (same port + stream)
        // accepts the frames and answers status messages, so Aeron never
        // re-resolves it.  Drop it and re-add on the new (numeric) address.
        // An add still in flight is never superseded (the Aeron client releases
        // only a publication it has FOUND): once it completes on a stale
        // address, the hub's next confirmation (~1 s) re-adds it here.  Legacy
        // planes (broker_, D24(b)): the stale pub is handed to the control
        // thread to drop (releasing it here would take the node client's admin
        // lock on the worker) and the re-add is made there too.  Cold path.
        for (std::size_t i = 0; i < pubCount_; ++i) {
            PeerCold& c = cold_[i];
            if (c.hostIdx != idx || !pubs_[i].pub) continue;
            std::uint32_t ip = 0;
            if (!inbox_.addressOf(idx, ip)) continue;
            if (!c.boundKnown) { c.boundIp = ip; c.boundKnown = true; continue; }
            if (ip == c.boundIp) continue;   // a confirmation of the same address
            spdlog::warn("AeronPubRegistry: peer {} host of {} changed address — re-adding its publication",
                         pubs_[i].id, c.channel);
            try {
                resolving_.push_back(Resolving{i, c.channel, idx});
            } catch (...) {
                continue;   // allocation failure: keep the (stale) pub; retried on the next confirmation
            }
            if (broker_ != nullptr) {
                if (!broker_->release(pubs_[i].pub)) {   // ring full: keep the stale pub, retried next confirmation
                    resolving_.pop_back();
                    continue;
                }
            } else {
                pubs_[i].pub.reset();
            }
            allResolved_ = false;
            addNow_(resolving_.back(), /*retry=*/true);
        }
    }

    void onBrokerResult_(rune::replication::NodeId peer, PubAddBroker::PubPtr pub) noexcept
    {
        for (std::size_t i = 0; i < resolving_.size(); ++i) {
            Resolving& r = resolving_[i];
            if (pubs_[r.slot].id != peer || r.regId != kBrokered) continue;
            if (pub) {
                pubs_[r.slot].pub = std::move(pub);
                eraseResolving_(i);
            } else {
                onFailed_(r, "registration error (via control thread)");
            }
            return;
        }
    }

    /// 1 = resolved (pub installed), 0 = not answered yet, -1 = errored.
    int probe_(Resolving& r) noexcept
    {
        std::shared_ptr<aeron::ExclusivePublication> pub;
        const auto aeron = aeron_.lock();
        if (!aeron) return 0;
        try {
            pub = aeron->findExclusivePublication(r.regId);
        } catch (const std::exception& e) {
            onFailed_(r, e.what());
            return -1;
        }
        if (!pub) return 0;
        if (r.failures > 0)
            spdlog::info("AeronPubRegistry: peer {} publication {} resolved after {} failed attempt(s)",
                         pubs_[r.slot].id, r.channel, r.failures);
        pubs_[r.slot].pub = std::move(pub);
        return 1;
    }

    void eraseResolving_(std::size_t i) noexcept
    {
        if (i + 1 != resolving_.size()) resolving_[i] = std::move(resolving_.back());
        resolving_.pop_back();
    }

    bool                           allResolved_ = false;
    bool                           configured_  = false;
    uint64_t                       oversizeDrops_ = 0;   // F3 r6: owner-worker-thread only
    std::array<PeerPub, kMaxPeers> pubs_{};
    std::size_t                    pubCount_    = 0;
    /// Cold per-peer data (kept off the hot pubs_ array): re-add target + the
    /// address the live publication was added on (DR-4 IP-change re-add).
    struct PeerCold {
        std::string   channel;
        int           hostIdx    = -1;
        std::uint32_t boundIp    = 0;
        bool          boundKnown = false;
    };
    std::array<PeerCold, kMaxPeers> cold_{};
    // Non-owning (never extends the client's life past its owner's teardown).
    std::weak_ptr<aeron::Aeron>    aeron_;               // for re-adds (owner thread)
    int32_t                        streamId_    = 0;
    std::vector<Resolving>         resolving_;           // cold: peers without a pub yet
    rune::consensus::PeerResolutionInbox inbox_;         // owner thread: resolution events
    PubAddBroker*                  broker_      = nullptr;   // legacy planes: control-thread adds
    uint64_t                       failures_    = 0;
    uint64_t                       reAdds_      = 0;
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
        : WorkerReplicationTransportImpl(workerId, std::move(inboundChannel), inboundStreamId, DeferredAttach{})
    {
        (void)attach(std::move(aeron), peerChannels, peerStreamId, pump);
    }

    // -----------------------------------------------------------------------
    // Deferred construction (shard mode, Task 9 fix round 1 I2): the plane
    // object exists (replicas bind their concrete transport pointer to it at
    // build time) but has NO client yet: resolved()==false, poll() is a no-op,
    // every send a no-op.  attach() later registers + resolves on the shard's
    // own client, right before the shard runner starts (no unpumped gap).
    // -----------------------------------------------------------------------
    struct DeferredAttach {};
    WorkerReplicationTransportImpl(int32_t workerId, std::string inboundChannel,
                                   int32_t inboundStreamId, DeferredAttach) noexcept
        : workerId_(workerId)
        , inboundChannel_(std::move(inboundChannel))
        , inboundStreamId_(inboundStreamId)
    {}

    /// Register + resolve the inbound sub and every peer pub on `aeron`.  Once
    /// only (startup thread, before the worker runner).  Returns resolved() —
    /// false only for a LOCAL failure; unresolved peers stay Resolving.
    bool attach(std::shared_ptr<aeron::Aeron> aeron,
                const std::vector<std::pair<rune::replication::NodeId, std::string>>& peerChannels,
                int32_t peerStreamId, rune::aeronutil::AeronPump pump = {}) noexcept
    {
        pubReg_.attach(aeron, peerChannels, peerStreamId, pump);
        attachInbound_(aeron, pump);
        return resolved();
    }

private:
    void attachInbound_(const std::shared_ptr<aeron::Aeron>& aeron, rune::aeronutil::AeronPump& pump) noexcept
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
            }, rune::aeronutil::AeronPump::kUntilAnswered, &failed);   // D8
        } catch (const std::exception& e) {
            spdlog::error("WorkerReplicationTransport[{}]: addSubscription {} stream {} failed: {}",
                          workerId_, inboundChannel_, inboundStreamId_, e.what());
        }
        // sub_ == nullptr → resolved() returns false.
    }


public:
    // -----------------------------------------------------------------------
    // Test-seam constructor — injects a pre-built PubRegistry (e.g.
    // FakePubRegistry) and bypasses Aeron entirely (sub_ left null but
    // resolved() treats it as already done when pubReg_.allResolved()).
    // Used by makeTestTransport only.
    // -----------------------------------------------------------------------
    explicit WorkerReplicationTransportImpl(int32_t workerId, PubRegistry pubReg) noexcept
        : workerId_(workerId)
        , inboundStreamId_(0)
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
    // resolved — the plane is USABLE: its inbound sub is live and its peer set
    // is validly configured.  2026-10-05 (DR-15): peer publications that have
    // not resolved yet (peer hostname unresolvable while it restarts) do NOT
    // make the plane unusable — they stay Resolving and are re-added by
    // drivePeerResolve(); allPeersResolved() reports them.  Only genuinely
    // local failures (own bind / bad config) leave resolved() false.
    // -----------------------------------------------------------------------
    [[nodiscard]] bool resolved() const noexcept
    {
        if (testMode_) return pubReg_.allResolved();
        if constexpr (std::is_same_v<PubRegistry, AeronPubRegistry>)
            return sub_ != nullptr && pubReg_.configured();
        else
            return sub_ != nullptr && pubReg_.allResolved();
    }

    /// Every peer publication resolved (no peer still Resolving).
    [[nodiscard]] bool allPeersResolved() const noexcept { return pubReg_.allResolved(); }

    /// One call per worker pass: drain resolution events (a few loads when
    /// empty); re-add a Resolving peer on its host's event and probe in-flight
    /// registrations.  Never resolves a name.  Owner thread only.
    int drivePeerResolve() noexcept
    {
        if constexpr (requires(PubRegistry& r) { r.drivePeerResolve(); })
            return pubReg_.drivePeerResolve();
        else
            return 0;
    }

    /// DR-15 fix round 1 (production registry): resolution events of the
    /// owner thread, and — legacy planes — the control-thread add broker.
    /// Both before attach().
    void attachResolution(rune::consensus::PeerResolutionSource src) { pubReg_.attachResolution(src); }
    void setAddBroker(PubAddBroker* broker) noexcept { pubReg_.setAddBroker(broker); }
    [[nodiscard]] std::size_t resolvingPeerCount() const noexcept { return pubReg_.resolvingPeerCount(); }
    [[nodiscard]] uint64_t peerReAddsForTest() const noexcept { return pubReg_.peerReAdds(); }

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
    // No std::function, no trampoline: rune templated assembler inlined into
    // Aeron's templated poll (mirrors WorkerIngress::poll exactly).
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

        // Production path (Task 11 fix 2, B3): rune templated assembler + the
        // concrete dispatch, inlined into Aeron's templated poll — no
        // std::function, no trampoline.
        const int n = sub_->poll(
            [this, &dispatch](aeron::concurrent::AtomicBuffer& b, aeron::util::index_t o,
                              aeron::util::index_t l, aeron::Header& h) {
                assembler_.onFragment(
                    [this, &dispatch](aeron::concurrent::AtomicBuffer& buffer, aeron::util::index_t offset,
                                      aeron::util::index_t length, aeron::Header& header) {
                        dispatchFrame_(reinterpret_cast<const uint8_t*>(buffer.buffer() + offset),
                                       static_cast<std::size_t>(length), header.sessionId(), dispatch);
                    },
                    b, o, l, h);
            },
            fragmentLimit);
        imageWatch_.afterPoll(*sub_, assembler_);   // free buffers of lost images (m-c)
        return n;
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
    //
    // F3 fix-round-2, item A: also the PartitionTransport virtual override —
    // onFetchRequest()/pushToFollowers() call this (via onTransport()) to
    // decide whether an oversized-relative-to-maxBytes first batch can still
    // be sent alone.  Safe to call from this worker's own thread: this whole
    // transport instance is owned by exactly one worker (no cross-thread
    // state read here — pubReg_'s per-peer pubs are this worker's own).
    // -----------------------------------------------------------------------
    /// F18: PartitionTransport::maxFetchRecordsBytes — the per-worker plane's
    /// frame ceiling (its pubs are owned by this worker: safe to read here).
    [[nodiscard]] int64_t maxFetchRecordsBytes() const noexcept override {
        return maxFetchResponseBytes();
    }

    [[nodiscard]] int32_t maxFetchResponseBytes() const noexcept
    {
        const int32_t kOverhead =
            static_cast<int32_t>(rune::replication::FetchResponseMsg::encodedSize(0));
        const int32_t ml = pubReg_.maxMessageLength();
        return (ml > kOverhead) ? (ml - kOverhead) : 0;
    }

    // F3 round 5 (config-only view): the same ceiling before any pub exists —
    // the peer pubs are UDP ExclusivePublications on the embedded driver
    // (maxMessageLength = min(term/8, 16 MiB)), minus FetchResponse framing:
    // ReplicationFrameLimits.h's publication term, the one THE batch-limit
    // formula uses.
    [[nodiscard]] static int32_t maxFetchResponseBytesFor(int32_t termBufferBytes) noexcept
    {
        if (termBufferBytes <= 0) return 0;
        const int64_t c = rune::replication::maxFetchRecordsViaPublication(termBufferBytes);
        return static_cast<int32_t>(c < 0 ? 0 : (c > INT32_MAX ? INT32_MAX : c));
    }

    // =======================================================================
    // Shard plane handshake (spec §3.3 symmetric worker count; Task 6 ruling I2,
    // Task 9 fix round 1).
    //
    // Worker-count symmetry cannot be checked at config-parse time (a peer's
    // RUNE_WORKERS is not in RUNE_PEERS), so every shard plane exchanges
    // PlaneHello{nodeId, workerId, workerCount}.  A worker-count-mismatched peer is
    // treated EXACTLY like an unreachable peer on this plane — nothing more (the
    // ISR/minIsr logic gives clients the right answer: the peer drops out of the
    // ISR; acks=all completes with the rest or fails with MinIsrViolation).
    //
    // Per-peer rich-state machine (spec §1a.4), transitions ONLY on a hello:
    //   Unverified{lastHelloSentNs}         -- matching hello -->  Verified{sessionId}
    //   Unverified / Verified / Refused     -- mismatched hello -> Refused{sessionId, peerWorkerCount, peerPlane, lastHelloSentNs}
    //   Refused / Verified                  -- matching hello -->  Verified{sessionId}   (heals; new session recorded)
    // An UNRESOLVED peer (DR-15: its publication is still Resolving) is treated
    // like an unreachable one, with one asymmetry (Kafka's one-way partition):
    // its hellos still reach this node, so this side can go Verified (and record
    // its fetches) while the peer stays Unverified until our publication to it
    // resolves; it stays in the ISR only while caught up, then lag-shrinks.
    // Every hello is (re)evaluated, including from a refused peer: a restarted
    // peer presents a NEW image session, so a peer that was fixed and restarted
    // heals both ways without restarting this node (review M3).
    //   - Dispatch is session-verified: a frame is applied ONLY if its image
    //     session presented a MATCHING hello (unverified / refused sessions are
    //     dropped and counted).  A frame racing ahead of its hello is simply
    //     re-sent by the fetch/push retry path (time-independent), and the
    //     reply-requested hello makes verification immediate.
    //   - Unverified and Refused peers get a reply-requested hello every
    //     kHelloRetryNs of the injected clock (per peer); Verified peers none.
    //   - Refused peers get NO data frames (pub blocked); hellos still go out.
    //   - Critical log on refusal is rate-limited (first + every 1024th).
    // Worker-thread only (driven from the worker's replication poll).  Legacy /
    // flag-off never enables it: driveHandshake() is one predicted branch and
    // dispatch never checks sessions.
    // =======================================================================
    static constexpr int64_t kHelloRetryNs = 10'000'000;   // 10 ms (injected clock)

    enum class PeerPlaneState : uint8_t { Unverified = 0, Verified = 1, Refused = 2 };   // == variant index

    /// Startup only (before the worker runner starts).  `peers` = the remote
    /// node ids on this plane (at most kMaxHandshakePeers; extra ids ignored).
    /// F3 round 6, item 6: also advertises — and requires every peer to match —
    /// this node's effective max produce batch and worker ingress ring size
    /// (the max-batch constraint is cluster-wide; a mismatch is treated exactly
    /// like a worker-count mismatch: Refused).
    void enablePlaneHandshake(rune::replication::NodeId selfNodeId, int32_t selfWorkerCount,
                              const std::vector<rune::replication::NodeId>& peers,
                              int64_t selfMaxProduceBatchBytes = 0,
                              int32_t selfWorkerIngressRingBytes = 0) noexcept
    {
        hsEnabled_       = true;
        selfNodeId_      = selfNodeId;
        selfWorkerCount_ = selfWorkerCount;
        selfMaxProduceBatchBytes_   = selfMaxProduceBatchBytes;
        selfWorkerIngressRingBytes_ = selfWorkerIngressRingBytes;
        hsCount_         = 0;
        for (auto id : peers) {
            if (hsCount_ == kMaxHandshakePeers) break;
            hs_[hsCount_++] = PeerPlane{id, Unverified{}};
        }
        needHello_ = static_cast<uint32_t>(hsCount_);
    }

    /// One call per worker pass (cold unless a peer is unverified / refused).
    int driveHandshake(int64_t nowNs) noexcept
    {
        if (!hsEnabled_ || needHello_ == 0) [[likely]] return 0;
        int sent = 0;
        for (std::size_t i = 0; i < hsCount_; ++i) {
            int64_t* last = nullptr;
            if (auto* u = std::get_if<Unverified>(&hs_[i].st))    last = &u->lastHelloSentNs;
            else if (auto* r = std::get_if<Refused>(&hs_[i].st))  last = &r->lastHelloSentNs;
            if (!last || (*last != kNeverNs && nowNs - *last < kHelloRetryNs)) continue;
            *last = nowNs;
            if (sendHello_(hs_[i].id, rune::replication::PlaneHelloMsg::kReplyRequested) > 0) ++sent;
        }
        return sent;
    }

    [[nodiscard]] bool     planeHandshakeEnabled() const noexcept { return hsEnabled_; }
    [[nodiscard]] bool     planeRefused() const noexcept
    {
        for (std::size_t i = 0; i < hsCount_; ++i)
            if (std::holds_alternative<Refused>(hs_[i].st)) return true;
        return false;
    }
    [[nodiscard]] uint64_t planeRefusals() const noexcept { return refusals_; }
    [[nodiscard]] uint64_t planeFramesDroppedUnverified() const noexcept { return droppedUnverified_; }
    [[nodiscard]] PeerPlaneState peerPlaneState(rune::replication::NodeId peer) const noexcept
    {
        for (std::size_t i = 0; i < hsCount_; ++i)
            if (hs_[i].id == peer) return static_cast<PeerPlaneState>(hs_[i].st.index());
        return PeerPlaneState::Unverified;
    }

    // =======================================================================
    // PartitionTransport virtuals — SEND PATH
    //
    // Fixed-size messages encode on the stack and go out via sendToPeer /
    // broadcastToPeers (tryClaim).  FetchResponse / ReconcileResponse encode only
    // their SBE prefix on the stack and gather-send the records view
    // (sendGatherToPeer: tryClaim <= maxPayloadLength, else 2-buffer offer()).
    // F27: every send returns whether the frame was ACCEPTED (Aeron position
    // > 0).  BACK_PRESSURED (-2) / ADMIN_ACTION (-3) / NOT_CONNECTED (-1) /
    // no pub / oversized → false, and the replica retries on its next pass
    // from its own cursor (never a resend timer).  Once accepted, Aeron is a
    // reliable, ordered stream — the frame reaches the peer's image unless the
    // image itself is lost (peer restart), which the follower's fetch covers.
    // =======================================================================

    // -----------------------------------------------------------------------
    // sendFetchRequest — BROADCAST to all peers.
    // FetchRequest has no explicit target on the wire; the current leader
    // responds; non-leaders drop via the role guard.
    // -----------------------------------------------------------------------
    bool sendFetchRequest(const rune::replication::FetchRequestMsg& req) override
    {
        RUNE_FR_SPAN_REPLICATION(rune_fr::Site::ReplWorkerFetchReqSend,
                                 static_cast<uint32_t>(req.partition));
        constexpr std::size_t kSize = rune::replication::FetchRequestMsg::TOTAL_SIZE;
        alignas(8) uint8_t claimBuf[kSize];
        rune::replication::FetchRequestMsg::encode(
            claimBuf, 0,
            req.leaderEpoch, req.partition, req.followerId,
            req.fetchOffset, req.maxBytes, req.topicId, req.lastFetchedEpoch, req.incarnation,
            req.storageId, req.lastEpochStartOffset);
        return pubReg_.broadcastToPeers(claimBuf, kSize);
    }

    // -----------------------------------------------------------------------
    // sendFetchRequestTo — spec §3.5.2: UNICAST to the replica's known leader.
    // A leader that is not (yet) a registered peer falls back to the broadcast.
    // -----------------------------------------------------------------------
    bool sendFetchRequestTo(rune::replication::NodeId leader,
                            const rune::replication::FetchRequestMsg& req) override
    {
        RUNE_FR_SPAN_REPLICATION(rune_fr::Site::ReplWorkerFetchReqSend,
                                 static_cast<uint32_t>(req.partition));
        constexpr std::size_t kSize = rune::replication::FetchRequestMsg::TOTAL_SIZE;
        alignas(8) uint8_t claimBuf[kSize];
        rune::replication::FetchRequestMsg::encode(
            claimBuf, 0, req.leaderEpoch, req.partition, req.followerId,
            req.fetchOffset, req.maxBytes, req.topicId, req.lastFetchedEpoch, req.incarnation,
            req.storageId, req.lastEpochStartOffset);
        if (pubReg_.hasPeer(leader)) return pubReg_.sendToPeer(leader, claimBuf, kSize) > 0;
        return pubReg_.broadcastToPeers(claimBuf, kSize);
    }

    // -----------------------------------------------------------------------
    // sendFetchResponse — direct-encode to toFollower's pub.
    // Variable-length (records tail) → size computed; fast path uses tryClaim
    // with in-claim encode (zero-copy for ≤ MTU frames).
    // -----------------------------------------------------------------------
    bool sendFetchResponse(rune::replication::NodeId toFollower,
                           const rune::replication::FetchResponseMsg& resp) override
    {
        RUNE_FR_SPAN_REPLICATION(rune_fr::Site::ReplWorkerSend,
                                 static_cast<uint32_t>(resp.records.size()));
        return sendFetchResponseImpl_(toFollower, resp, /*reconcile=*/false);
    }

    // -----------------------------------------------------------------------
    // sendOffsetsForLeaderEpochRequest — fixed-size, single peer.
    // -----------------------------------------------------------------------
    bool sendOffsetsForLeaderEpochRequest(
            rune::replication::NodeId toLeader,
            const rune::replication::OffsetsForLeaderEpochReqMsg& req) override
    {
        constexpr std::size_t kSize = rune::replication::OffsetsForLeaderEpochReqMsg::TOTAL_SIZE;
        alignas(8) uint8_t claimBuf[kSize];
        rune::replication::OffsetsForLeaderEpochReqMsg::encode(
            claimBuf, 0,
            req.currentEpoch, req.partition, req.followerId,
            req.requestedEpoch, req.followerLeo, req.topicId);
        return pubReg_.sendToPeer(toLeader, claimBuf, kSize) > 0;
    }

    // -----------------------------------------------------------------------
    // sendOffsetsForLeaderEpochResponse — fixed-size, single peer.
    // -----------------------------------------------------------------------
    bool sendOffsetsForLeaderEpochResponse(
            rune::replication::NodeId toFollower,
            const rune::replication::OffsetsForLeaderEpochRespMsg& resp) override
    {
        constexpr std::size_t kSize = rune::replication::OffsetsForLeaderEpochRespMsg::TOTAL_SIZE;
        alignas(8) uint8_t claimBuf[kSize];
        rune::replication::OffsetsForLeaderEpochRespMsg::encode(
            claimBuf, 0,
            resp.leaderEpoch, resp.partition, resp.responseEpoch,
            resp.endOffset, resp.errorCode, resp.topicId);
        return pubReg_.sendToPeer(toFollower, claimBuf, kSize) > 0;
    }

    // -----------------------------------------------------------------------
    // sendReconcileRequest — FetchRequest re-stamped as ReconcileFetchReq.
    // -----------------------------------------------------------------------
    bool sendReconcileRequest(rune::replication::NodeId toPeer,
                              const rune::replication::FetchRequestMsg& req) override
    {
        constexpr std::size_t kSize = rune::replication::FetchRequestMsg::TOTAL_SIZE;
        alignas(8) uint8_t claimBuf[kSize];
        rune::replication::FetchRequestMsg::encode(
            claimBuf, 0,
            req.leaderEpoch, req.partition, req.followerId,
            req.fetchOffset, req.maxBytes, req.topicId, req.lastFetchedEpoch, req.incarnation,
            req.storageId, req.lastEpochStartOffset);
        rune::replication::stampTemplateId(
            claimBuf, 0, rune::replication::kReconcileFetchReqTemplateId);
        return pubReg_.sendToPeer(toPeer, claimBuf, kSize) > 0;
    }

    // -----------------------------------------------------------------------
    // sendReconcileResponse — FetchResponse re-stamped as ReconcileFetchResp.
    // -----------------------------------------------------------------------
    bool sendReconcileResponse(rune::replication::NodeId toPeer,
                               const rune::replication::FetchResponseMsg& resp) override
    {
        return sendFetchResponseImpl_(toPeer, resp, /*reconcile=*/true);
    }

private:
    // -----------------------------------------------------------------------
    // dispatchFrame_ — decode the SBE header and fixed fields, apply maxBytes
    // cap for FetchRequest variants, then invoke dispatch.
    //
    // Called from both the production poll (term-buffer pointer, valid
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

        const uint16_t templateId =
            rune::replication::peekTemplateId(frame, 0);
        if (templateId == rune::replication::kPlaneHelloTemplateId) [[unlikely]] {
            onPlaneHello_(frame, len, sessionId);
            return;
        }
        // Shard plane: apply ONLY frames from a session that presented a matching
        // hello.  Legacy planes (handshake off) never check.
        if (hsEnabled_ && !sessionVerified_(sessionId)) {
            ++droppedUnverified_;
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
                if (len < rr::FetchRequestMsg::MIN_SIZE) [[unlikely]] return;
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
                    // v2 (32-byte block) or v3 (36) frame: copy what arrived.
                    const std::size_t reqLen = len < rr::FetchRequestMsg::TOTAL_SIZE
                        ? len : rr::FetchRequestMsg::TOTAL_SIZE;
                    alignas(8) uint8_t cappedBuf[rr::FetchRequestMsg::TOTAL_SIZE];
                    std::memcpy(cappedBuf, frame, reqLen);
                    rr::detail::write_le<int32_t>(
                        cappedBuf + kHdrSize + 24, ceiling);
                    dispatch(topicId, partition, templateId, cappedBuf, reqLen);
                    return;
                }
                break;
            }
            case static_cast<uint16_t>(rr::ReplicationTemplateId::FetchResponse):
            case rr::kReconcileFetchRespTemplateId:
            {
                if (len < rr::FetchResponseMsg::MIN_SIZE) [[unlikely]] return;
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
    // Plane handshake internals (cold path except sessionVerified_; worker
    // thread only).  Each state holds exactly its own variables.
    // -----------------------------------------------------------------------
    static constexpr int64_t kNeverNs = INT64_MIN;
    struct Unverified { int64_t lastHelloSentNs = kNeverNs; };
    struct Verified   { int32_t sessionId = 0; };
    struct Refused    { int32_t sessionId = 0; int32_t peerWorkerCount = 0; int32_t peerPlane = -1;
                        int64_t lastHelloSentNs = kNeverNs;
                        int64_t peerMaxProduceBatchBytes = 0;     // F3 r6 item 6
                        int32_t peerWorkerIngressRingBytes = 0; };
    using PlaneState = std::variant<Unverified, Verified, Refused>;   // index == PeerPlaneState
    struct PeerPlane {
        rune::replication::NodeId id = -1;
        PlaneState                st{};
    };
    static constexpr std::size_t kMaxHandshakePeers = FollowerSlots::kMaxReplicas;
    static constexpr uint64_t    kLogEvery          = 1024;   // rate limit: first + every 1024th

    int64_t sendHello_(rune::replication::NodeId peer, int32_t flags) noexcept
    {
        rune::replication::PlaneHelloMsg m;
        m.nodeId      = static_cast<int32_t>(selfNodeId_);
        m.workerId    = workerId_;
        m.workerCount = selfWorkerCount_;
        m.flags       = flags;
        m.maxProduceBatchBytes   = selfMaxProduceBatchBytes_;
        m.workerIngressRingBytes = selfWorkerIngressRingBytes_;
        alignas(8) uint8_t buf[rune::replication::PlaneHelloMsg::TOTAL_SIZE];
        rune::replication::PlaneHelloMsg::encode(buf, m);
        return pubReg_.sendControlToPeer(peer, buf, sizeof(buf));
    }

    // Per data frame in shard mode: a flat scan of <= kMaxHandshakePeers entries.
    [[nodiscard]] bool sessionVerified_(int32_t sessionId) const noexcept
    {
        for (std::size_t i = 0; i < hsCount_; ++i) {
            const auto* v = std::get_if<Verified>(&hs_[i].st);
            if (v && v->sessionId == sessionId) return true;
        }
        return false;
    }

    void onPlaneHello_(const uint8_t* frame, std::size_t len, int32_t sessionId) noexcept
    {
        namespace rr = rune::replication;
        rr::PlaneHelloMsg h;
        if (!hsEnabled_ || !rr::PlaneHelloMsg::decode(frame, len, h)) return;
        PeerPlane* p = nullptr;
        for (std::size_t i = 0; i < hsCount_; ++i)
            if (hs_[i].id == static_cast<rr::NodeId>(h.nodeId)) { p = &hs_[i]; break; }
        if (!p) {
            if ((unknownHellos_++ % kLogEvery) == 0)
                spdlog::warn("WorkerReplicationTransport[{}]: PlaneHello from unknown node {} ignored "
                             "({} so far)", workerId_, h.nodeId, unknownHellos_);
            return;
        }
        const bool wasHelloTarget = !std::holds_alternative<Verified>(p->st);
        const bool countMismatch = h.workerCount != selfWorkerCount_ || h.workerId != workerId_;
        const bool batchMismatch = h.maxProduceBatchBytes != selfMaxProduceBatchBytes_ ||
                                   h.workerIngressRingBytes != selfWorkerIngressRingBytes_;
        if (countMismatch || batchMismatch) {
            // Keep the retry clock across Refused -> Refused (hellos keep flowing).
            int64_t last = kNeverNs;
            if (auto* u = std::get_if<Unverified>(&p->st)) last = u->lastHelloSentNs;
            else if (auto* r = std::get_if<Refused>(&p->st)) last = r->lastHelloSentNs;
            if ((refusals_ % kLogEvery) == 0 && !countMismatch)
                spdlog::critical(
                    "WorkerReplicationTransport[{}]: the max produce batch is a CLUSTER-WIDE "
                    "constraint — node {} advertises effective max produce batch {} B / worker "
                    "ingress ring {} B but this node {} has {} B / {} B; a batch one node accepts "
                    "may be unreplicable to the other.  Treating node {} as UNREACHABLE on this "
                    "plane; make the term/ring/max-batch config homogeneous and restart it — the "
                    "plane heals on its next matching hello ({} refusal(s) so far)", workerId_,
                    h.nodeId, h.maxProduceBatchBytes, h.workerIngressRingBytes, selfNodeId_,
                    selfMaxProduceBatchBytes_, selfWorkerIngressRingBytes_, h.nodeId, refusals_ + 1);
            else if ((refusals_ % kLogEvery) == 0)
                spdlog::critical(
                    "WorkerReplicationTransport[{}]: shard mode REQUIRES a symmetric worker count — "
                    "node {} runs RUNE_WORKERS={} (its plane {}) but this node {} runs {} (plane {}). "
                    "Treating node {} as UNREACHABLE on this plane (never split planes silently); "
                    "fix RUNE_WORKERS and restart it — the plane heals on its next matching hello "
                    "({} refusal(s) so far)", workerId_, h.nodeId, h.workerCount, h.workerId,
                    selfNodeId_, selfWorkerCount_, workerId_, h.nodeId, refusals_ + 1);
            ++refusals_;
            p->st = Refused{sessionId, h.workerCount, h.workerId, last,
                            h.maxProduceBatchBytes, h.workerIngressRingBytes};
            if (!wasHelloTarget) ++needHello_;
            pubReg_.setPeerBlocked(p->id, true);
        } else {
            if (!std::holds_alternative<Verified>(p->st))
                spdlog::info("WorkerReplicationTransport[{}]: peer {} verified on plane {} ({} workers)",
                             workerId_, h.nodeId, workerId_, h.workerCount);
            p->st = Verified{sessionId};
            if (wasHelloTarget) --needHello_;
            pubReg_.setPeerBlocked(p->id, false);
        }
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

    bool sendFetchResponseImpl_(rune::replication::NodeId peer,
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
        return pubReg_.sendGatherToPeer(peer, prefix, prefixLen, resp.records) > 0;
    }

    // -----------------------------------------------------------------------
    // Members
    // -----------------------------------------------------------------------
    int32_t     workerId_;
    std::string inboundChannel_;
    int32_t     inboundStreamId_;

    std::shared_ptr<aeron::Subscription> sub_;  // inbound (resolved by ctor)

    // Rune templated fragment assembler (Task 11 fix 2, B3) — no std::function.
    rune::aeronutil::FragmentAssembler assembler_;
    rune::aeronutil::ImageLossWatch    imageWatch_;

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
    std::array<PeerPlane, kMaxHandshakePeers> hs_{};
    std::size_t               hsCount_           = 0;
    uint32_t                  needHello_         = 0;   // peers in Unverified or Refused
    uint64_t                  refusals_          = 0;
    uint64_t                  unknownHellos_     = 0;
    uint64_t                  droppedUnverified_ = 0;
    rune::replication::NodeId selfNodeId_        = -1;
    int32_t                   selfWorkerCount_   = 0;
    int64_t                   selfMaxProduceBatchBytes_   = 0;   // F3 r6 item 6
    int32_t                   selfWorkerIngressRingBytes_ = 0;
    bool                      hsEnabled_         = false;
};

// ---------------------------------------------------------------------------
// WorkerReplicationTransport — production alias (AeronPubRegistry).
// ---------------------------------------------------------------------------
using WorkerReplicationTransport = WorkerReplicationTransportImpl<AeronPubRegistry>;

} // namespace rune::replication
