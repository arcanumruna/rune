#pragma once

#include <cstdio>
#include <cstdlib>
// SessionTable.h — Per-session egress tracking for the ClientGateway (BR-4).
//
// Design:
//   - Monotonically-increasing sessionId generator (starts at 1).
//   - Flat unordered_map: sessionId → Session.
//   - All operations are called from the single doWork() thread — no locks.

#include <Aeron.h>
#include <ExclusivePublication.h>

// client<->broker TLS/AEAD (Task 7): Session's own CryptoState holds the two
// directional AeadCiphers once a server-role TLS handshake completes
// (ClientGateway::handleHandshakeContinue_).  NOTE (build/linkage trap,
// task-7-interfaces.md): this header is already part of rune_gateway's
// PUBLIC interface (it includes <Aeron.h>) — pulling rune/crypto/Aead.h in
// here means every downstream target that compiles a TU including
// SessionTable.h now needs rune_crypto's include dir transitively.  rune_crypto
// + OpenSSL must be linked PUBLIC on rune_gateway (mirroring rune_fr / the
// client's Task-5 fix-round-1), not PRIVATE as the brief's literal text says.
//
// CryptoState itself (Task 8) lives in its own aeron-free header —
// rune/gateway/CryptoState.h — so the data-plane codec (CryptoFrame.h) that
// seals/opens frames under it can be unit-tested standalone without ever
// touching <Aeron.h>.
#include <rune/gateway/CryptoState.h>

#include <array>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>

namespace rune::gateway {

// ---------------------------------------------------------------------------
// CryptoState — see rune/gateway/CryptoState.h (extracted there in Task 8 so
// the aeron-free data-plane codec can use it without pulling in <Aeron.h>).
// ---------------------------------------------------------------------------
// Session — one connected client.
// ---------------------------------------------------------------------------
struct Session {
    int64_t                                        sessionId   = 0;
    std::shared_ptr<aeron::ExclusivePublication>   egress;        // acks/responses (gateway-written)
    int64_t                                        lastSeenNs  = 0;

    // Hybrid egress: the client's egress endpoint (captured at Connect) and, per
    // OWNING WORKER, a dedicated publication to it for Records pushed by that
    // worker (per-consumer UNICAST → independent per-consumer offsets).  ONE pub
    // per (session, worker): an ExclusivePublication is single-writer, so a
    // consumer subscribing to partitions owned by several workers gets one pub per
    // worker (distinct Aeron images on the client's single egress subscription,
    // demuxed by its per-session FragmentAssembler).  Lazily created on first
    // subscribe, written ONLY by the owning worker thread.  Dropped with the session.
    std::string                                    egressChannel;
    int32_t                                        egressStreamId = 0;
    std::unordered_map<int32_t, std::shared_ptr<aeron::ExclusivePublication>>
                                                   recordEgressByWorker;
    // Task 13 fix round 2 (shard mode, G-A6 on the gateway-routed path): bit w
    // set = worker w holds acks for this session (SessionSignal Held/Released).
    // Non-zero → the gateway ABORTs this session's Produce frames.
    uint64_t                                       heldByWorkers = 0;
    // D6: ConnectRequest.clientVersion — gates behaviour the client must
    // understand (UnknownProducerId is answered OutOfOrderSequence below
    // protocol::client::kClientVersionUnknownProducerId).  0 = unknown (oldest).
    int32_t                                        clientVersion = 0;
    // D8b round 2: bit w set = worker w reports this session's ack publication
    // Lost (NOT_CONNECTED); lostSinceNs = when the mask last became non-zero, restarted by a Found
    // from one worker while another is still Lost (D8b-L: a successful claim is proof of life).
    // Non-zero → reapExpired uses the normal lease measured from lostSinceNs.
    uint64_t                                       lostByWorkers = 0;
    int64_t                                        lostSinceNs   = 0;
    // D8c: produce ops forwarded to the workers whose acks no worker has
    // claimed yet (SessionSignal Credit).  At ClientGateway::sessionProduceCredits()
    // the gateway ABORTs this session's Produce frames.  Dies with the session.
    uint32_t                                       produceInFlight = 0;
    // D8c fix round 1: produceInFlight split by the owning worker it was
    // forwarded to (Credit signals name their worker) — kept so a close returns
    // the session's share of every worker's routed cap.  Cold (close / credit).
    static constexpr int                           kMaxWorkers = 64;   // == ClientGateway::kMaxHeldMaskWorkers
    std::array<uint16_t, kMaxWorkers>              produceInFlightOn{};   // <= the per-session budget (4096)
    // D8c fix round 2: tag of the worker's cap-full scan window (a fresh tag per
    // kCapEvictAfterDeferredPolls deferred polls without a victim — lows batch C,
    // as D8d) during which this session last returned a Credit from that worker
    // (a reader — never evicted in that window).  uint16 tags wrap after 65536
    // windows (harmless: at worst one reading session is considered for
    // eviction once more).
    std::array<uint16_t, kMaxWorkers>              creditTagOn{};
    // Subscribe dedupe (2026-10-05; last — review L-5: off the produce-path fields' lines): a Subscribe is identified by (session,
    // correlationId, topicId, resolved partition) — the client binds exactly one
    // instance per correlationId, so a re-sent Subscribe (redirect / reconnect
    // retry paths) re-answers this subscriptionId instead of registering a
    // second, leaked stream.  The (topic, partition) part keeps clients that do
    // not tag per-partition Subscribes (group isolation carriers: correlationId
    // 0 for every assigned partition) one subscription per partition.  Cold
    // (control path: Subscribe / Unsubscribe only).  Dropped with the session,
    // so the same correlationId on a NEW session is a new subscription.
    struct SubKey {
        int64_t correlationId = 0;
        int32_t topicId       = 0;
        int32_t partition     = 0;
        bool operator==(const SubKey&) const = default;
    };
    struct SubKeyHash {
        std::size_t operator()(const SubKey& k) const noexcept {
            const uint64_t tp = (static_cast<uint64_t>(static_cast<uint32_t>(k.topicId)) << 32) |
                                static_cast<uint32_t>(k.partition);
            return std::hash<uint64_t>{}(static_cast<uint64_t>(k.correlationId) ^ (tp * 0x9E3779B97F4A7C15ULL));
        }
    };
    std::unordered_map<SubKey, int64_t, SubKeyHash> subIdByKey;

    // client<->broker TLS/AEAD (Task 7): this session's crypto state.
    // `crypto.encrypted` is false (default CryptoState) for a plaintext
    // session — byte-identical to every pre-Task-7 Session.
    CryptoState crypto;
};

// ---------------------------------------------------------------------------
// SessionTable
// ---------------------------------------------------------------------------
class SessionTable {
public:
    SessionTable() = default;

    // No copies — owns ExclusivePublications via shared_ptr (but the table
    // itself is not meant to be copied).
    SessionTable(const SessionTable&)            = delete;
    SessionTable& operator=(const SessionTable&) = delete;

    // -----------------------------------------------------------------------
    // nextSessionId() — monotonically-increasing, starts at 1, OR-ed with the
    // cluster id prefix (setIdPrefix): ((nodeId + 1) << 40) | counter makes a
    // session id unique across the cluster's nodes (a client holds one session
    // per node and demultiplexes by id).  Always positive.
    // -----------------------------------------------------------------------
    [[nodiscard]] int64_t nextSessionId() noexcept {
        // LOW-2: the counter must never carry into the node prefix.
        if (++nextId_ >= (int64_t{1} << 40)) [[unlikely]] {
            std::fprintf(stderr, "SessionTable: session id space exhausted (2^40) — aborting\n");
            std::abort();
        }
        return idPrefix_ | nextId_;
    }
    /// Cluster-unique ids: the prefix OR-ed into every id (0 = none, tests).
    void setIdPrefix(int64_t prefix) noexcept { idPrefix_ = prefix; }

    // -----------------------------------------------------------------------
    // add() — insert a new session.  Returns a reference valid until remove().
    // -----------------------------------------------------------------------
    Session& add(int64_t                                      sessionId,
                 std::shared_ptr<aeron::ExclusivePublication> egress,
                 int64_t                                      nowNs,
                 std::string                                  egressChannel = {},
                 int32_t                                      egressStreamId = 0,
                 int32_t                                      clientVersion = 0) {
        Session s;
        s.sessionId      = sessionId;
        s.egress         = std::move(egress);
        s.lastSeenNs     = nowNs;
        s.egressChannel  = std::move(egressChannel);
        s.egressStreamId = egressStreamId;
        s.clientVersion  = clientVersion;
        auto [it, _] = sessions_.emplace(sessionId, std::move(s));
        return it->second;
    }

    // -----------------------------------------------------------------------
    // find() — returns nullptr if the session is absent.
    // -----------------------------------------------------------------------
    [[nodiscard]] Session* find(int64_t sessionId) noexcept {
        auto it = sessions_.find(sessionId);
        if (it == sessions_.end()) return nullptr;
        return &it->second;
    }

    // -----------------------------------------------------------------------
    // remove() — returns true if the session existed and was removed.
    // -----------------------------------------------------------------------
    bool remove(int64_t sessionId) noexcept {
        return sessions_.erase(sessionId) > 0;
    }

    // -----------------------------------------------------------------------
    // touch() — update lastSeenNs (no-op if absent; silently ignored).
    // -----------------------------------------------------------------------
    void touch(int64_t sessionId, int64_t nowNs) noexcept {
        auto it = sessions_.find(sessionId);
        if (it != sessions_.end()) {
            it->second.lastSeenNs = nowNs;
        }
    }

    // -----------------------------------------------------------------------
    // size() — number of active sessions.
    // -----------------------------------------------------------------------
    [[nodiscard]] std::size_t size() const noexcept {
        return sessions_.size();
    }

    // -----------------------------------------------------------------------
    // forEach() — iterate for BR-5 timeout reaping.  Callback: (Session&) → void.
    // -----------------------------------------------------------------------
    template <typename Fn>
    void forEach(Fn&& fn) {
        for (auto& [id, session] : sessions_) {
            fn(session);
        }
    }

private:
    int64_t                              nextId_   = 0;
    int64_t                              idPrefix_ = 0;
    std::unordered_map<int64_t, Session> sessions_;
};

} // namespace rune::gateway
