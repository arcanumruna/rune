// include/rune/client/lease/LeaseSession.h - LeaseSession<Capacity, OnLost, OnAtRisk>: keeps leases alive and guards writes (Task 15).
//
// A session owns up to Capacity leases (each: a lease topic, a key, the version granted, its ttl).  doWork(nowNs) - on the thread that
// owns the session, with nowNs from Client::nowNs() - sends LeaseKeepAlive every ttl/3, batched per (topic, partition) and phase-
// preserving, tracks each lease's margin, and runs the callbacks.  A lease whose margin M passes, whose key the broker no longer has, or
// whose watch ended is LOST: the producers bound to it are failed and blocked FIRST (the produce guard, run by the client's duty thread),
// then onLost(ref, reason) fires once, then a best-effort fenced revoke goes out (for the reasons where the lease may still be live).
// A lease that is merely at risk (no answered keepalive for atRiskNum/atRiskDen of its ttl) trips the guard the same way and fires
// onAtRisk(ref, Entered); a later answered keepalive clears it (onAtRisk(ref, Cleared)) and writes go out again.
//
// Threading: not thread-safe; every method and every callback runs on the thread that calls doWork().  Callbacks must not call doWork().
// Memory: all storage is fixed members sized from Capacity plus the session's own Completions queue; doWork allocates nothing.
// The callbacks are functor members (no std::function, no virtual call).
//
// Guarded producers: bindProducer(ref, producer) BEFORE the producer's first write to the lease's data, on the thread that writes (the
// bind travels the producer's ring, FIFO with its writes; a write submitted before the bind is not guarded).  At most 8 producers per
// lease.  A producer whose lease left the session (lost, removed, revoked, ended) stays blocked for good: bind a new producer to a new
// lease.  Blocked writes complete DeliveryFailed with errorCode ClientErrorCode::LeaseAtRisk.
#pragma once

#include <rune/client/Client.h>
#include <rune/client/Completions.h>
#include <rune/client/lease/LeaseTopic.h>
#include <rune/client/producer/Producer.h>

#include <array>
#include <cstdint>
#include <span>
#include <stdexcept>
#include <utility>
#include <variant>

namespace rune::client {

enum class LostReason : uint8_t {
    MarginExpired,
    NotFound,
    TopicDropped,
    WatchedRevoked,
    WatchedExpired,
    WatchedVanished,
    SessionRefused,
};

enum class AtRiskEdge : uint8_t { Entered, Cleared };

/// A lease of a session: a slot and the generation of its occupant.  A stale ref (the slot was freed since) is refused by every call.
struct LeaseRef {
    uint32_t slot = 0xFFFFFFFFu;
    uint32_t gen  = 0;
    [[nodiscard]] bool operator==(const LeaseRef&) const noexcept = default;
};

struct LeaseSessionOptions {
    uint32_t maxDelayMs = 100;   ///< d: the longest the application may take to act on a callback; ttl/2 + d must stay below ttl - ttl/50
    uint32_t atRiskNum  = 1;     ///< a lease is at risk atRiskNum/atRiskDen of its ttl after the last answered keepalive was SENT
    uint32_t atRiskDen  = 2;     ///< 0 < atRiskNum < atRiskDen
};

enum class LeaseEntryState : uint8_t { Free, Held, AtRisk, Losing, Revoking };

namespace detail {

/// The non-template body of a LeaseSession (src/lease/LeaseSession.cpp).
class LeaseSessionCore {
public:
    static constexpr uint32_t kMaxRequests = 64;    ///< in-flight keepalive requests
    static constexpr uint32_t kMaxGroups   = 64;    ///< distinct (topicId, partition) pairs
    static constexpr uint32_t kMaxKeepAliveEntries = rune::protocol::client::kLeaseKeepAliveMaxEntries;
    static constexpr uint32_t kPoolExtra   = 8;

    struct Callbacks {
        void* ctx = nullptr;
        void (*lost)(void* ctx, LeaseRef ref, LostReason reason) = nullptr;
        void (*atRisk)(void* ctx, LeaseRef ref, AtRiskEdge edge) = nullptr;
    };

    // ---- the entry's rich state (each arm holds exactly the fields it needs) ----
    struct Timing {
        int64_t lastOkSentNs = 0;
        int64_t marginNs     = 0;    // M
        int64_t atRiskNs     = 0;
        int64_t phaseNs      = 0;    // the next keepalive instant of the entry's own cadence
        int64_t nextSendNs   = 0;
        int32_t inFlightReq  = -1;
    };
    struct Free {};
    struct Held { Timing t; };
    struct AtRiskS { Timing t; int64_t sinceNs = 0; };
    struct Losing { LostReason reason = LostReason::MarginExpired; bool closeAfter = false; };
    struct Revoking { bool sent = false; };
    using State = std::variant<Free, Held, AtRiskS, Losing, Revoking>;

    struct Entry {
        uint32_t gen = 1;
        State    st{};
        LeaseTopicCore* topic = nullptr;
        int32_t  partition = 0;
        int32_t  group = -1;
        uint8_t  keyLen = 0;
        uint8_t  key[rune::protocol::kMaxLeaseKeyBytes]{};
        int64_t  version = 0;
        int64_t  ttlNs = 0;
        uint32_t armSeq = 0;
        uint8_t  nProducers = 0;
        uint8_t  cmdPending = 0;   // bit 0: a GuardArm to repost, bit 1: a GuardEnd to repost
    };

    LeaseSessionCore(Client& client, Entry* entries, uint32_t* cand, uint32_t capacity, Callbacks cb, const LeaseSessionOptions& opts);
    ~LeaseSessionCore();
    LeaseSessionCore(const LeaseSessionCore&)            = delete;
    LeaseSessionCore& operator=(const LeaseSessionCore&) = delete;

    [[nodiscard]] LeaseRef add(LeaseTopicCore& topic, std::span<const uint8_t> key, int64_t version, uint64_t ttlMs, int64_t grantSentNs,
                               OpResult* err);
    [[nodiscard]] OpResult remove(LeaseRef ref);
    [[nodiscard]] OpResult revoke(LeaseRef ref);
    [[nodiscard]] OpResult update(LeaseRef ref, std::span<const uint8_t> row, CompletionTarget target, Token token);
    [[nodiscard]] OpResult bindProducer(LeaseRef ref, Producer& producer);
    void close();
    [[nodiscard]] bool isClosed() const noexcept { return closed_ && live_ == 0; }
    void onWatchedEnd(int32_t topicId, int32_t partition, std::span<const uint8_t> key, int64_t version, LostReason reason);
    int  doWork(int64_t nowNs);

    // ---- observers (tests) ----
    [[nodiscard]] uint32_t liveCount() const noexcept { return live_; }
    [[nodiscard]] LeaseEntryState stateOf(LeaseRef ref) const noexcept;
    [[nodiscard]] uint32_t sessionId() const noexcept { return sid_; }
    [[nodiscard]] bool refused() const noexcept { return refused_; }
    [[nodiscard]] uint64_t requestsSent() const noexcept { return requestsSent_; }
    [[nodiscard]] uint64_t okAnswers() const noexcept { return okAnswers_; }   ///< keepalive entries answered Ok so far
    [[nodiscard]] Completions& queue() noexcept { return cq_; }

private:
    struct Group { int32_t topicId = 0; int32_t partition = 0; uint32_t refs = 0; };
    struct Request {
        bool     used = false;
        uint32_t gen = 1;
        int64_t  sendNs = 0;
        uint16_t count = 0;
        uint16_t slot[kMaxKeepAliveEntries]{};
    };

    static CompletionsConfig queueConfig_(uint32_t capacity);
    [[nodiscard]] static Timing* timing_(Entry& e) noexcept;
    [[nodiscard]] static const Timing* timing_(const Entry& e) noexcept;
    [[nodiscard]] Entry* resolve_(LeaseRef ref) noexcept;
    [[nodiscard]] LeaseRef refOf_(uint32_t slot) const noexcept { return LeaseRef{slot, entries_[slot].gen}; }
    [[nodiscard]] int64_t margin_(const Entry& e, int64_t sentNs) const noexcept;
    [[nodiscard]] int64_t atRiskAt_(const Entry& e, int64_t sentNs) const noexcept;
    [[nodiscard]] bool postGuard_(detail::HubCommand::Op op, uint32_t slot, uint64_t gen, int64_t b, int64_t c);
    [[nodiscard]] bool postArm_(uint32_t slot, Entry& e, int64_t atRiskNs);
    void   postEnd_(uint32_t slot, Entry& e);
    void   freeEntry_(uint32_t slot);
    void   toLosing_(uint32_t slot, LostReason why);
    void   noteWork_(int64_t t) noexcept { if (t < nextWorkNs_) nextWorkNs_ = t; }
    int    drain_(int64_t nowNs);
    void   onCompletion_(const Completion& c, int64_t nowNs);
    void   onGuardEvent_(const Completion& c, int64_t nowNs);
    void   onKeepAlive_(const Completion& c, int64_t nowNs);
    void   onRevoked_(const Completion& c);
    void   onEnded_(uint32_t slot);
    void   setPending_(Entry& e, uint8_t bit) noexcept { e.cmdPending |= bit; anyPending_ = true; }
    bool   sendRevoke_(uint32_t slot, bool closeKind);
    void   bestEffortRevoke_(LeaseTopicCore* topic, const uint8_t* key, uint8_t keyLen, int64_t version, uint32_t slot, uint32_t gen);
    int    retryPending_();
    int    timeWork_(int64_t nowNs);
    int    sendDue_(int64_t nowNs);
    bool   sendRequest_(int32_t group, const uint32_t* slots, uint32_t n, int64_t nowNs);
    void   releasePool_() noexcept;

    Client&      client_;
    Entry*       entries_;
    uint32_t*    cand_;
    uint32_t     capacity_;
    Callbacks    cb_;
    int64_t      dNs_;
    uint32_t     num_, den_;
    Completions  cq_;
    uint32_t     sid_ = 0;
    uint32_t     pool_ = 0;
    uint32_t     poolHeld_ = 0;        // reserve(48) calls that succeeded (returned in the destructor)
    uint64_t     consumed_ = 0;        // guard events consumed so far
    uint64_t     acked_    = 0;        // ... as last reported with GuardAck
    uint32_t     live_ = 0;
    int64_t      nextWorkNs_ = 0;
    bool         closed_ = false;
    bool         refused_ = false;
    bool         guardReady_ = false;
    bool         anyPending_ = false;  // some entry has a guard command (or an unsent revoke) to repost
    bool         posted_ = false;      // a guard command / revoke went out this pass: drain again
    uint64_t     requestsSent_ = 0;
    uint64_t     okAnswers_    = 0;
    uint32_t     groupsUsed_ = 0;
    std::array<Group, kMaxGroups>     groups_{};
    std::array<Request, kMaxRequests> requests_{};
    std::array<rune::protocol::client::LeaseKeepAlive::Entry, kMaxKeepAliveEntries> kaEntries_{};
};

}  // namespace detail

template <uint32_t Capacity, class OnLost, class OnAtRisk>
class LeaseSession {
    static_assert(Capacity >= 1 && Capacity <= 4095, "LeaseSession: Capacity must be 1..4095 (the slot rides in a 12-bit token field)");

public:
    LeaseSession(Client& client, OnLost onLost, OnAtRisk onAtRisk, const LeaseSessionOptions& opts = {})
        : onLost_(std::move(onLost))
        , onAtRisk_(std::move(onAtRisk))
        , core_(client, entries_.data(), cand_.data(), Capacity,
                detail::LeaseSessionCore::Callbacks{this, &LeaseSession::lostThunk_, &LeaseSession::atRiskThunk_}, opts) {}
    LeaseSession(const LeaseSession&)            = delete;
    LeaseSession& operator=(const LeaseSession&) = delete;

    /// Starts keeping the lease of `keyRow.key()` alive.  `grantSentNs` is the client-clock time at which the caller SENT the grant (or
    /// the update that produced `version`); `ttlMs` the lease's ttl.  An invalid ref (and *err set) when: the key is outside 1..128
    /// bytes, ttlMs is 0 or above the topic's max, ttl/2 + d >= ttl - ttl/50, no free slot, 64 groups in use, a topic of another
    /// client, a closed or refused session.
    template <class T>
    [[nodiscard]] LeaseRef add(LeaseTopic<T>& topic, const T& keyRow, int64_t version, uint64_t ttlMs, int64_t grantSentNs,
                               OpResult* err = nullptr) {
        uint8_t key[rune::protocol::kMaxLeaseKeyBytes];
        std::size_t n = 0;
        if (!topic.keyBytes(keyRow, key, n)) {
            if (err != nullptr) *err = OpResult::InvalidArgument;
            return LeaseRef{};
        }
        return core_.add(topic.core(), std::span<const uint8_t>(key, n), version, ttlMs, grantSentNs, err);
    }
    [[nodiscard]] LeaseRef add(detail::LeaseTopicCore& topic, std::span<const uint8_t> key, int64_t version, uint64_t ttlMs,
                               int64_t grantSentNs, OpResult* err = nullptr) {
        return core_.add(topic, key, version, ttlMs, grantSentNs, err);
    }
    [[nodiscard]] OpResult remove(LeaseRef ref) { return core_.remove(ref); }
    [[nodiscard]] OpResult revoke(LeaseRef ref) { return core_.revoke(ref); }
    [[nodiscard]] OpResult update(LeaseRef ref, std::span<const uint8_t> row, CompletionTarget target, Token token) {
        return core_.update(ref, row, target, token);
    }
    [[nodiscard]] OpResult bindProducer(LeaseRef ref, Producer& producer) { return core_.bindProducer(ref, producer); }
    void close() { core_.close(); }
    [[nodiscard]] bool isClosed() const noexcept { return core_.isClosed(); }
    void onWatchedEnd(int32_t topicId, int32_t partition, std::span<const uint8_t> key, int64_t version, LostReason reason) {
        core_.onWatchedEnd(topicId, partition, key, version, reason);
    }
    int doWork(int64_t nowNs) { return core_.doWork(nowNs); }

    [[nodiscard]] uint32_t liveCount() const noexcept { return core_.liveCount(); }
    [[nodiscard]] LeaseEntryState stateOf(LeaseRef ref) const noexcept { return core_.stateOf(ref); }
    [[nodiscard]] detail::LeaseSessionCore& core() noexcept { return core_; }

private:
    static void lostThunk_(void* ctx, LeaseRef ref, LostReason r) { static_cast<LeaseSession*>(ctx)->onLost_(ref, r); }
    static void atRiskThunk_(void* ctx, LeaseRef ref, AtRiskEdge e) { static_cast<LeaseSession*>(ctx)->onAtRisk_(ref, e); }

    OnLost   onLost_;
    OnAtRisk onAtRisk_;
    std::array<detail::LeaseSessionCore::Entry, Capacity> entries_{};
    std::array<uint32_t, Capacity>                        cand_{};
    detail::LeaseSessionCore                              core_;
};

}  // namespace rune::client
