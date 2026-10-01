#pragma once
// SessionAckSink.h — worker-side produce acks (spec §3.4, Task 13).
//
// In shard mode the worker encodes each ProduceResponse itself and claims it on
// ITS OWN exclusive publication to the client session (single writer, zero-copy
// tryClaim/BufferClaim).  Sessions are announced by the runtime over the
// runtime→worker ring BEFORE the client learns its session id (announce-before-
// accept); the publication is added on the first flush after the announcement
// and resolved without ever blocking the loop.
//
// Rich state (spec §1a.4) — each state holds exactly what it needs:
//   per session  PubState = Announced(chan, len, stream)        add() pending
//                         → Resolving(regId, deadlineNs)         find() pending
//                         → Ready(pub, stalledSinceNs, blockedPass)
//                         | Failed(sinceNs)                      acks dropped (client gone)
//   whole sink   Backlog  = Clear | Backlogged(pending, sinceNs) — G-A6
//
// Coalescing: every ack due in a pass is claimed in one flush, before the shard's
// next driver duty cycle, so the sender ships each session's acks together (one
// datagram up to the MTU).  The client protocol, frames and correlation ids are
// unchanged (resolution of the §3.4 "one frame" wording).
//
// Never-drop (G-A6): an ack that cannot be claimed (back-pressured, or the
// client's image not yet connected) stays queued in order (per-session order is
// kept: once a session blocks in a pass, its later acks in that pass wait too).
// Acks are dropped ONLY when the client can never receive them: the session was
// closed by the runtime (Disconnect / reap / connect timeout), its publication
// failed (driver error, CLOSED, resolve deadline), or it stayed unclaimable for
// kStallDropNs (a client that vanished without Disconnect — its session is
// reaped by the runtime later; this bounds what it can hold here).  The worker
// sizes the queue (kAckCapacity) against the same static bound as its return
// FIFO and gates intake on its free slots, so enqueue never fails in steady
// state; a full queue is an invariant violation for the caller.
//
// Zero allocation per pass: fixed arrays, open addressing (backward-shift
// delete), no node containers.  std::string only inside the policy's add()
// (announcement, cold path).  Owner: the worker thread only.
#include <rune/protocol/client/DataMessages.h>

#include <Aeron.h>
#include <ExclusivePublication.h>
#include <concurrent/logbuffer/BufferClaim.h>

#include <array>
#include <bit>
#include <cstdint>
#include <cstring>
#include <memory>
#include <string>
#include <string_view>
#include <variant>

namespace rune::node {

struct AckEntry {
    int64_t sessionId     = 0;
    int64_t correlationId = 0;
    int64_t baseOffset    = 0;
    int32_t partition     = 0;
    int16_t errorCode     = 0;
};

enum class PubFind : uint8_t { Pending, Ready, Error };

template <class PubPolicy>
class SessionAckSinkT {
public:
    using PubHandle = typename PubPolicy::PubHandle;

    static constexpr std::size_t kMaxSessions      = 4096;             // power of two (one slot stays empty)
    static constexpr std::size_t kMaxChannelSlots  = 256;              // announcements awaiting add()
    static constexpr std::size_t kAckCapacity      = 16384;            // >= Worker::kRetFifoBound (static_assert there)
    static constexpr std::size_t kMaxChannelBytes  = 255;
    static constexpr int64_t     kResolveTimeoutNs = 5'000'000'000LL;
    static constexpr int64_t     kStallDropNs      = 5'000'000'000LL;
    /// A claim result meaning "this publication can never deliver" (Aeron CLOSED
    /// / MAX_POSITION_EXCEEDED, mapped by the policy) → the session fails now.
    static constexpr int64_t     kClaimFatal       = -4;

    struct PubAnnounced { uint16_t chan = 0; uint16_t chanLen = 0; int32_t streamId = 0; };
    struct PubResolving { int64_t regId = -1; int64_t deadlineNs = 0; };
    struct PubReady     { PubHandle pub{}; int64_t stalledSinceNs = kNotStalled; uint64_t blockedPass = 0; };
    struct PubFailed    { int64_t sinceNs = 0; };
    using PubState = std::variant<PubAnnounced, PubResolving, PubReady, PubFailed>;

    struct Clear {};
    struct Backlogged { uint32_t pending = 0; int64_t sinceNs = 0; };
    using Backlog = std::variant<Clear, Backlogged>;

    explicit SessionAckSinkT(PubPolicy policy) noexcept : policy_(std::move(policy)) {
        for (std::size_t i = 0; i < kMaxChannelSlots; ++i) freeChan_[i] = static_cast<uint16_t>(i);
        freeChanCount_ = kMaxChannelSlots;
    }
    SessionAckSinkT(const SessionAckSinkT&)            = delete;
    SessionAckSinkT& operator=(const SessionAckSinkT&) = delete;

    /// Runtime announcement (ring op, cold).  Re-announcing a known session
    /// restarts it on a fresh publication.  False = refused (session id <= 0,
    /// table full, channel too long) — the session's acks are then dropped.
    bool announce(int64_t sessionId, std::string_view channel, int32_t streamId) noexcept {
        if (sessionId <= 0 || channel.size() > kMaxChannelBytes) return false;
        std::size_t idx = find_(sessionId);
        if (idx == kNone) idx = insert_(sessionId);
        if (idx == kNone) return false;                                   // table full
        releaseChan_(states_[idx]);
        if (unresolvedCount_ == kMaxSessions) [[unlikely]] {              // unreachable (one entry per session)
            states_[idx] = PubFailed{0};
            return false;
        }
        if (freeChanCount_ > 0) [[likely]] {
            const uint16_t c = freeChan_[--freeChanCount_];
            std::memcpy(chanPool_[c].data(), channel.data(), channel.size());
            states_[idx] = PubAnnounced{c, static_cast<uint16_t>(channel.size()), streamId};
        } else {
            // Connect burst beyond the channel pool: register now (the channel
            // view is still valid here) instead of refusing the session.
            states_[idx] = addNow_(channel, streamId, lastNowNs_);
            if (std::holds_alternative<PubFailed>(states_[idx])) return false;
        }
        unresolved_[unresolvedCount_++] = sessionId;
        return true;
    }

    /// Runtime close (Disconnect / reap / connect timeout): forget the session;
    /// its queued acks drop at the next flush; its publication is released.
    void close(int64_t sessionId) noexcept {
        const std::size_t idx = find_(sessionId);
        if (idx == kNone) return;
        releaseChan_(states_[idx]);
        erase_(idx);
    }

    /// Queue one ack for this pass's flush.  False only when the queue is full
    /// (the caller's intake gate makes that an invariant violation).
    bool enqueue(const AckEntry& a) noexcept {
        if (ackCount_ == kAckCapacity) [[unlikely]] return false;
        acks_[ackCount_++] = a;
        return true;
    }

    /// One pass: advance announcements, then claim every queued ack on its
    /// session publication in order.  Returns work done.
    int flush(int64_t nowNs) noexcept {
        lastNowNs_ = nowNs;
        ++pass_;
        int work = unresolvedCount_ ? resolvePending_(nowNs) : 0;
        if (ackCount_ == 0) [[likely]] {
            backlog_ = Clear{};
            return work;
        }
        std::size_t keep = 0;
        for (std::size_t i = 0; i < ackCount_; ++i) {
            const AckEntry a = acks_[i];
            const std::size_t idx = find_(a.sessionId);
            if (idx == kNone) { ++dropped_; continue; }             // closed / never announced
            PubState& st = states_[idx];
            if (auto* r = std::get_if<PubReady>(&st)) [[likely]] {
                if (r->blockedPass != pass_) {                      // per-session order kept
                    const int64_t pos = policy_.claim(r->pub, kAckBytes, [&](uint8_t* dst) { encode_(a, dst); });
                    if (pos > 0) [[likely]] {
                        r->stalledSinceNs = kNotStalled;
                        ++work;
                        continue;
                    }
                    if (pos == kClaimFatal) [[unlikely]] {           // can never deliver
                        st = PubFailed{nowNs};
                        ++dropped_;
                        ++work;
                        continue;
                    }
                    r->blockedPass = pass_;                         // back-pressured / not yet connected
                    if (r->stalledSinceNs == kNotStalled) r->stalledSinceNs = nowNs;
                }
                if (nowNs - r->stalledSinceNs >= kStallDropNs) [[unlikely]] {   // client gone
                    st = PubFailed{nowNs};
                    ++dropped_;
                    ++work;
                    continue;
                }
            } else if (std::holds_alternative<PubFailed>(st)) {
                ++dropped_;
                continue;
            }
            acks_[keep++] = a;                                       // Announced / Resolving / blocked
        }
        ackCount_ = keep;
        if (keep == 0) {
            backlog_ = Clear{};
        } else if (auto* b = std::get_if<Backlogged>(&backlog_)) {
            b->pending = static_cast<uint32_t>(keep);
        } else {
            backlog_ = Backlogged{static_cast<uint32_t>(keep), nowNs};
        }
        return work;
    }

    /// No ack is waiting (nothing held after the last flush).
    [[nodiscard]] bool        intakeAllowed() const noexcept { return std::holds_alternative<Clear>(backlog_); }
    [[nodiscard]] std::size_t freeAckSlots() const noexcept { return kAckCapacity - ackCount_; }
    [[nodiscard]] std::size_t pending() const noexcept { return ackCount_; }
    [[nodiscard]] std::size_t sessionCount() const noexcept { return count_; }
    [[nodiscard]] std::size_t pendingForTest() const noexcept { return ackCount_; }
    [[nodiscard]] uint64_t    droppedForTest() const noexcept { return dropped_; }
    [[nodiscard]] const PubState* stateForTest(int64_t sessionId) const noexcept {
        const std::size_t idx = find_(sessionId);
        return idx == kNone ? nullptr : &states_[idx];
    }

private:
    static constexpr int64_t     kNotStalled = INT64_MIN;
    static constexpr std::size_t kNone     = static_cast<std::size_t>(-1);
    static constexpr std::size_t kMask     = kMaxSessions - 1;
    static constexpr int         kBits     = std::countr_zero(kMaxSessions);
    static constexpr std::size_t kAckBytes = rune::protocol::client::ProduceResponse::ENCODED_SIZE;
    static_assert(std::has_single_bit(kMaxSessions), "kMaxSessions must be a power of two");
    static_assert(kMaxChannelSlots <= 65536, "channel slot index is uint16");

    static std::size_t hash_(int64_t sid) noexcept {
        return static_cast<std::size_t>((static_cast<uint64_t>(sid) * 0x9E3779B97F4A7C15ULL) >> (64 - kBits));
    }
    std::size_t find_(int64_t sid) const noexcept {
        std::size_t i = hash_(sid);
        for (std::size_t n = 0; n < kMaxSessions; ++n, i = (i + 1) & kMask) {
            if (keys_[i] == 0)   return kNone;
            if (keys_[i] == sid) return i;
        }
        return kNone;
    }
    std::size_t insert_(int64_t sid) noexcept {
        if (count_ == kMaxSessions - 1) return kNone;               // one empty slot ends every probe
        std::size_t i = hash_(sid);
        while (keys_[i] != 0) i = (i + 1) & kMask;
        keys_[i] = sid;
        states_[i] = PubFailed{0};
        ++count_;
        return i;
    }
    void erase_(std::size_t i) noexcept {                            // backward-shift deletion
        keys_[i] = 0;
        states_[i] = PubFailed{0};                                   // releases a Ready publication
        --count_;
        std::size_t j = i;
        for (;;) {
            j = (j + 1) & kMask;
            if (keys_[j] == 0) return;
            const std::size_t home = hash_(keys_[j]);
            const bool stays = (i <= j) ? (i < home && home <= j) : (i < home || home <= j);
            if (stays) continue;
            keys_[i] = keys_[j];
            states_[i] = std::move(states_[j]);
            keys_[j] = 0;
            states_[j] = PubFailed{0};
            i = j;
        }
    }
    void releaseChan_(const PubState& st) noexcept {
        if (const auto* a = std::get_if<PubAnnounced>(&st); a && freeChanCount_ < kMaxChannelSlots)
            freeChan_[freeChanCount_++] = a->chan;
    }
    PubState addNow_(std::string_view channel, int32_t streamId, int64_t nowNs) noexcept {
        const int64_t reg = policy_.add(channel, streamId);
        if (reg < 0) return PubFailed{nowNs};
        return PubResolving{reg, nowNs + kResolveTimeoutNs};
    }
    int resolvePending_(int64_t nowNs) noexcept {
        int work = 0;
        std::size_t keep = 0;
        for (std::size_t k = 0; k < unresolvedCount_; ++k) {
            const int64_t sid = unresolved_[k];
            const std::size_t idx = find_(sid);
            if (idx == kNone) continue;                              // closed meanwhile
            PubState& st = states_[idx];
            if (const auto* a = std::get_if<PubAnnounced>(&st)) {
                const std::string_view ch(chanPool_[a->chan].data(), a->chanLen);
                const int32_t stream = a->streamId;
                releaseChan_(st);
                st = addNow_(ch, stream, nowNs);
                ++work;
                if (std::holds_alternative<PubResolving>(st)) unresolved_[keep++] = sid;
                continue;
            }
            if (auto* r = std::get_if<PubResolving>(&st)) {
                PubHandle pub{};
                const PubFind f = policy_.find(r->regId, pub);
                if (f == PubFind::Ready) { st = PubReady{std::move(pub), kNotStalled, 0}; ++work; continue; }
                if (f == PubFind::Error || nowNs >= r->deadlineNs) { st = PubFailed{nowNs}; ++work; continue; }
                unresolved_[keep++] = sid;
            }
        }
        unresolvedCount_ = keep;
        return work;
    }
    static void encode_(const AckEntry& a, uint8_t* dst) noexcept {
        rune::protocol::client::ProduceResponse r;
        r.sessionId     = a.sessionId;
        r.correlationId = a.correlationId;
        r.errorCode     = a.errorCode;
        r.partition     = a.partition;
        r.baseOffset    = a.baseOffset;
        r.encode(dst);
    }

    PubPolicy                                                        policy_;
    std::array<int64_t, kMaxSessions>                                keys_{};      // 0 = empty
    std::array<PubState, kMaxSessions>                               states_{};
    std::size_t                                                      count_ = 0;
    std::array<int64_t, kMaxSessions>                                unresolved_{};
    std::size_t                                                      unresolvedCount_ = 0;
    std::array<std::array<char, kMaxChannelBytes>, kMaxChannelSlots> chanPool_{};
    std::array<uint16_t, kMaxChannelSlots>                           freeChan_{};
    std::size_t                                                      freeChanCount_ = 0;
    std::array<AckEntry, kAckCapacity>                               acks_{};
    std::size_t                                                      ackCount_ = 0;
    Backlog                                                          backlog_{Clear{}};
    uint64_t                                                         pass_    = 0;
    uint64_t                                                         dropped_ = 0;
    int64_t                                                          lastNowNs_ = 0;
};

/// Production policy: the worker's own invoker-mode Aeron client (spec §3.1).
struct AeronSessionPubPolicy {
    using PubHandle = std::shared_ptr<aeron::ExclusivePublication>;
    aeron::Aeron* client = nullptr;

    int64_t add(std::string_view channel, int32_t streamId) noexcept {
        try { return client->addExclusivePublication(std::string(channel), streamId); }
        catch (...) { return -1; }
    }
    PubFind find(int64_t regId, PubHandle& out) noexcept {
        try {
            out = client->findExclusivePublication(regId);
            return out ? PubFind::Ready : PubFind::Pending;
        } catch (...) {
            return PubFind::Error;                                   // ERRORED_MEDIA_DRIVER (bad channel, ...)
        }
    }
    template <class Encode>
    int64_t claim(PubHandle& pub, std::size_t len, Encode&& encode) noexcept {
        aeron::concurrent::logbuffer::BufferClaim c;
        const int64_t pos = pub->tryClaim(static_cast<aeron::util::index_t>(len), c);
        if (pos > 0) [[likely]] {
            encode(c.buffer().buffer() + c.offset());               // zero-copy: encode in the claim
            c.commit();
            return pos;
        }
        if (pos == aeron::PUBLICATION_CLOSED || pos == aeron::MAX_POSITION_EXCEEDED) return -4;
        return pos;                                                  // NOT_CONNECTED / BACK_PRESSURED / ADMIN_ACTION
    }
};
static_assert(SessionAckSinkT<AeronSessionPubPolicy>::kClaimFatal == -4);

using SessionAckSink = SessionAckSinkT<AeronSessionPubPolicy>;

} // namespace rune::node
