#pragma once
// SessionAckSink.h — worker-side produce acks (spec §3.4, Task 13).
//
// In shard mode the worker encodes each ProduceResponse itself and claims it on
// ITS OWN exclusive publication to the client session (single writer, zero-copy
// tryClaim/BufferClaim).  Sessions are announced by the runtime over the
// worker's session-lifecycle ring BEFORE the client learns its session id
// (announce-before-accept); the publication is added on the first flush after
// the announcement and resolved without ever blocking the loop.
//
// Rich state (spec §1a.4) — each state holds exactly what it needs:
//   per session  PubState = Announced(chan, len, stream)        add() pending
//                         → Resolving(regId)                     find() pending
//                         → Ready(pub, blockedPass)
//                         | Failed(reported)                     can never deliver
//   per ack      sessionPos — the lifecycle-ring producer position an ack of a
//                not-yet-known session is held against (fix round 1, I1).
//   per session  Edges — what the runtime was told (reportedHeld, reportedLost),
//                the current Lost verdict, a pending Progress and the pass of
//                the last Held / Progress report (D8b round 2).
//
// Coalescing: every ack due in a pass is claimed in one flush, before the shard's
// next driver duty cycle, so the sender ships each session's acks together (one
// datagram up to the MTU).  The client protocol, frames and correlation ids are
// unchanged (resolution of the §3.4 "one frame" wording).
//
// Never-drop, time-independent (G-A6, §1a.9 — fix round 1, C1/C2).  An ack that
// cannot be claimed (BACK_PRESSURED, NOT_CONNECTED, ADMIN_ACTION, or the
// publication not yet resolved) stays queued in order — the sink has NO timer: a
// slow client drains at its own pace while the worker pauses ONLY that session's
// intake (holding() → the worker ABORTs its frames in a controlled poll).  The
// sink only REPORTS per-session edges (reportSessionEdges); every time bound
// lives in the runtime's session leases (ClientGateway::reapExpired):
//   * Held / Released — the session has acks queued after a flush / no longer;
//   * Lost / Found (D8b round 2) — a claim returned NOT_CONNECTED (Aeron: no
//     status message from the client's driver for the publication-connection
//     timeout, 5 s by default — a dead client, an unreachable egress endpoint,
//     a client that stopped pumping its Aeron runtime (invoker mode), or a
//     network blip; also a publication that never connected) / a later claim
//     saw it connected (success, BACK_PRESSURED or ADMIN_ACTION — Aeron returns
//     those only while connected).  Acks are NEVER dropped for Lost; the runtime
//     reaps a session that stays Lost for its normal lease, which then drops
//     them through the close below;
//   * Progress — acks of a session the runtime knows as Held were claimed (the
//     client reads); at most once per kProgressPassInterval passes per session
//     (a pass count, never a clock), so the runtime refreshes its lease.
// Credits (D8c, reportCredits): every CLAIMED ack of a gateway-routed produce
// (AckEntry::credit) returns one produce credit to the runtime, coalesced per
// session per pass.  The runtime forwards at most its per-session budget of
// unclaimed gateway-routed produces, so the gateway pauses a slow client BEFORE
// this queue can fill — independent of burst size / MTU / socket buffers.
// Acks are dropped only on STATE events:
//   * the session was closed by the runtime (Disconnect / lease reap / connect
//     timeout / SessionFailed) — the client is gone;
//   * its publication can never deliver (add / find error, CLOSED,
//     MAX_POSITION_EXCEEDED) → Failed(reported = false); the worker reports it
//     (reportFailures → WorkerReturn::SessionFailed) and the runtime closes the
//     session everywhere, so the client reconnects — never a silent black hole;
//   * the ack's session is unknown AND the lifecycle ring's consumer position
//     reached the producer position sampled at the flush that first saw it: every
//     announcement enqueued before its frame existed has been applied, so the
//     session is truly unknown (closed).  Until then the ack is held (a frame
//     can be visible before its announcement is drained: aeron:ipc commits
//     straight into the log buffer).
// Direct budget (D8d — D8c review LOW-R2): every DIRECT (worker-ingress)
// produce is admitted by admitDirect and counted from that moment (owed —
// parked acks=all / Strict — or queued) until its ack LEAVES this queue
// (claimed, or dropped for a closed / failed session).  Per session at most
// directSessionBudget_; per worker at most directCap_ (Worker::kDirectAckBudget:
// the own-intake half minus the held-PushRead cap and one full op drain), so no
// number of stalled direct sessions can take the room the op drain (role /
// replication / gateway-routed ops) needs.  In the cap's last quarter only LIGHT
// sessions (< kDirectLightInFlight) are admitted: a stalled session — at its
// whole share — runs out first.  A frame refused here is ABORTed by the worker's
// controlled poll (it stays on ITS image; the client's offers back-pressure).
// When the cap is full, the session HELD here with the largest direct share is
// EXPIRED (reportDirectExpiry → the runtime closes it with SessionExpired) —
// the policy and thrash guards of D8c's gateway expiry: only a held session,
// never one whose acks were claimed during the episode, only after
// kDirectExpireAfterDeferredPasses deferred passes, at most one per episode and
// one in flight until the runtime's close arrives.
// Bound: the worker gates every intake step (op drain, client poll) on free
// slots minus owed acks, so the queue never overflows (Worker.h, kAckCapacity).
//
// Zero allocation per pass: fixed arrays, open addressing (backward-shift
// delete), no node containers.  std::string only inside the policy's add()
// (announcement, cold path).  Owner: the worker thread only.
#include <rune/node/WorkerWriteGrants.h>
#include <rune/node/AeronChannel.h>
#include <rune/node/RecordsSeal.h>
#include <rune/node/SessionSignalKind.h>
#include <rune/protocol/client/DataMessages.h>
#include <flight_recorder/FlightRecorder.h>

#include <Aeron.h>
#include <ExclusivePublication.h>
#include <concurrent/logbuffer/BufferClaim.h>

#include <algorithm>
#include <array>
#include <cassert>
#include <bit>
#include <cstdint>
#include <limits>
#include <cstring>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <variant>

namespace rune::node {

inline constexpr uint32_t kNoVerdictRun = 0xFFFF'FFFFu;

struct AckEntry {
    int64_t sessionId     = 0;
    int64_t correlationId = 0;
    int64_t baseOffset    = 0;
    int32_t partition     = 0;
    int16_t errorCode     = 0;
    /// D8c: 1 = the answer of a GATEWAY-routed produce (an op-ring produce):
    /// the runtime holds one produce credit for it until it is CLAIMED here
    /// (reportCredits).  0 = a direct (worker-ingress) produce — no credit.
    uint8_t credit        = 0;
    /// D8d: a DIRECT (worker-ingress) produce's answer, counted against the
    /// direct budget since admitDirect: kDirectTotalOnly (the worker total) or
    /// kDirectCounted (also its session's share); 0 = not a direct answer.
    uint8_t direct        = 0;
    /// Unknown-session hold (I1): the lifecycle-ring producer position sampled
    /// at the first flush that saw this ack with no session; -1 = not held.
    int64_t sessionPos    = -1;
    /// D6: with errorCode == UnknownProducerId, the partition's log start
    /// (ProduceResponse.logStartOffset); -1 = not carried.
    int64_t logStartOffset = -1;
    /// Client 17 (checked writes): the key a key-0 create was assigned (0 = none), and the verdict trailer of a View ack
    /// (verdictCount entries at verdictRun in the worker's arena; kNoVerdictRun = none).  A session below 17 never sees them.
    uint64_t assignedBase  = 0;
    uint32_t verdictRun    = kNoVerdictRun;
    uint16_t verdictCount  = 0;
    uint8_t  hasVerdicts   = 0;
};

enum class PubFind : uint8_t { Pending, Ready, Error };

template <class PubPolicy>
class SessionAckSinkT {
public:
    using PubHandle = typename PubPolicy::PubHandle;

    static constexpr std::size_t kMaxSessions      = 4096;             // power of two (one slot stays empty)
    static constexpr std::size_t kMaxChannelSlots  = 256;              // announcements awaiting add()
    /// Bound: Worker.h (static_asserts there).  D8c fix round 1: 2 x the
    /// worker's reserve (owed ceiling + drain + poll = 16384) — the other half is
    /// the gateway-routed share (Worker::kRoutedAckBudget).  48 B each (D6 logStartOffset): 1.5 MiB,
    /// heap (setShardClient).
    static constexpr std::size_t kAckCapacity      = 32768;
    static constexpr std::size_t kMaxChannelBytes  = 255;
    /// A claim result meaning "this publication can never deliver" (Aeron CLOSED
    /// / MAX_POSITION_EXCEEDED, mapped by the policy) → the session fails now.
    static constexpr int64_t     kClaimFatal       = -4;
    /// Aeron NOT_CONNECTED (static_assert at the Aeron policy): the session is
    /// marked Lost — its acks stay queued in order; any other non-fatal claim
    /// result (or a success) clears it.  A suspicion, never a verdict: the
    /// runtime decides on its lease (D8b round 2, re-review MAJOR-A).
    static constexpr int64_t     kClaimNotConnected = -1;
    /// Progress rate limit, in PASSES (the sink has no clock): a held session that
    /// drains reports Progress at most once per this many flushes, counted from its
    /// Held edge / previous Progress.  A worker under backoff idle passes at
    /// >= ~5k/s (200 µs max park) and far faster when busy, so 1024 passes is
    /// <= ~200 ms — orders of magnitude inside the runtime's held bound (10 min).
    /// The signal rate is pass-rate / 1024 per held session: ~5/s idle, ~10/s at
    /// 10k passes/s, ~1k/s for a busy worker at ~1M passes/s (D8b-L: the rate
    /// scales with the pass rate — it is not a fixed ~10/s).
    static constexpr uint64_t    kProgressPassInterval = 1024;
    /// D8d: admitDirect results (AckEntry::direct).
    static constexpr uint8_t     kDirectRefused   = 0;   ///< ABORT the frame (held / no budget / band / full)
    static constexpr uint8_t     kDirectTotalOnly = 1;   ///< admitted; session not announced yet: worker total only
    static constexpr uint8_t     kDirectCounted   = 2;   ///< admitted; worker total + the session's share
    /// Review MEDIUM-2: the session was CLOSED here (expired / reaped / failed /
    /// disconnected): CONSUME the frame without appending or answering — the
    /// gateway path's UnknownSession drop — so a client that resends after
    /// SessionExpired cannot duplicate its buffered backlog.
    static constexpr uint8_t     kDirectDropClosed = 3;
    /// Task 6: the frame came from an image other than the response-mode session's bound one (or the session awaits
    /// its bind): CONSUME without append or answer.  Never counted against any budget.
    static constexpr uint8_t     kDirectDropSpoofed = 4;
    /// Review MEDIUM-2: recently closed session ids remembered (FIFO ring +
    /// open-addressing set: O(1) per frame of an unknown session).  Lows batch C
    /// (was 1024): kMaxSessions — a mass close of EVERY session this worker can
    /// know (one gateway reap cycle, e.g. after a partition) keeps every
    /// tombstone.  A tombstone is evicted only after kMaxSessions NEWER closes;
    /// each needs a session announced first (a client Connect round trip), while
    /// the closed client's image — at most its term window — drains in a few
    /// passes (its frames are consumed, never appended).  Memory: 32 KiB ring +
    /// 64 KiB set per worker (the sink is heap-allocated once).
    static constexpr std::size_t kClosedTombstones = kMaxSessions;
    /// D8d: default per-session direct budget — the owed-return cap default
    /// (Worker::kMaxOwedReturns): one session may keep a full pipeline of
    /// parked acks=all on a worker; a stalled one holds at most this.
    static constexpr uint32_t    kDefaultDirectSessionBudget = 4096;
    /// D8d: in the cap's last quarter only sessions holding fewer than this
    /// many direct acks are admitted (ClientGateway::kLightSessionInFlight).
    static constexpr uint32_t    kDirectLightInFlight = 64;
    /// D8d: deferred passes of a full direct cap before an expiry is considered
    /// (and between scans) — ClientGateway::kCapEvictAfterDeferredPolls.
    static constexpr uint32_t    kDirectExpireAfterDeferredPasses = 64;

    struct PubAnnounced { uint16_t chan = 0; uint16_t chanLen = 0; int32_t streamId = 0; };
    /// A10: a Resolving session keeps its channel slot (kNoChan = registered without one, the pool-exhausted
    /// path) so a transient driver error can re-add on the same channel — bounded by kMaxPubAddAttempts.
    static constexpr uint16_t kNoChan = 0xFFFF;
    static constexpr uint8_t  kMaxPubAddAttempts = 8;
    /// regId < 0: a re-add is due at pass `retryPass` (A10 review M2: pass-count backoff 2, 4, ... passes —
    /// spaced, never a timer).
    struct PubResolving {
        int64_t regId = -1; uint16_t chan = kNoChan; uint16_t chanLen = 0; int32_t streamId = 0; uint8_t attempts = 1;
        uint64_t retryPass = 0;
    };
    struct PubReady     { PubHandle pub{}; uint64_t blockedPass = 0; };
    /// reported == false: the runtime has not yet been told (reportFailures).
    struct PubFailed    { bool reported = true; };
    /// NAT-safe egress (Task 6): a response-mode session waits here for its client's WorkerBind.  No channel yet:
    /// the response publication is added once the bind names the client's ingress image.  Acks are HELD like a
    /// Resolving session's.  `token` is the gateway-issued 16 B secret (constant-time compare in bind()).
    struct PubAwaitingBind { std::array<uint8_t, 16> token{}; int32_t streamId = 0; };
    using PubState = std::variant<PubAnnounced, PubResolving, PubReady, PubFailed, PubAwaitingBind>;
    /// AlreadyBound (I2): a repeated bind of a session already bound with the SAME token (the client resends until it
    /// sees the worker's WorkerBindAck): no state change, the worker only re-queues the ack.  The image is compared by
    /// bind() / the worker (checkBind has no image).
    enum class BindResult : uint8_t { Bound, UnknownSession, BadToken, NotAwaiting, AlreadyBound };
    /// I5: boundImage_ is ONE word per session: 0 = not response-mode, kAwaitingBind = response-mode awaiting its
    /// bind, otherwise imageKey_(image) (bit 32 set — never equal to kAwaitingBind, asserted below).  The produce
    /// admission reads this single word; states_ stays authoritative.
    static constexpr int64_t kAwaitingBind = std::numeric_limits<int64_t>::min();
    [[nodiscard]] static constexpr bool isBound_(int64_t v) noexcept { return v != 0 && v != kAwaitingBind; }

    explicit SessionAckSinkT(PubPolicy policy) noexcept : policy_(std::move(policy)) {
        for (std::size_t i = 0; i < kMaxChannelSlots; ++i) freeChan_[i] = static_cast<uint16_t>(i);
        freeChanCount_ = kMaxChannelSlots;
    }
    SessionAckSinkT(const SessionAckSinkT&)            = delete;
    SessionAckSinkT& operator=(const SessionAckSinkT&) = delete;

    /// Runtime announcement (ring op, cold).  Re-announcing a known session
    /// restarts it on a fresh publication.  False = refused (session id <= 0,
    /// table full, channel too long) — unreachable by construction: the runtime
    /// caps sessions at kMaxSessions − 1 and refuses longer channels at Connect
    /// (the worker treats a refusal as an invariant violation).
    /// D6: `clientVersion` (ConnectRequest.clientVersion) gates the
    /// UnknownProducerId answer for this session (OOS below version 2).
    /// Authentication Task 13 (D30): `grantAll` (the announce's mode bit: 1 in dev / in-process modes, 0 when
    /// authentication is enabled) seeds a NEW session's write grants; a re-announce of a known session (the second,
    /// crypto-key wave) never touches the grants it already holds.  Default false = deny until permissions arrive.
    bool announce(int64_t sessionId, std::string_view channel, int32_t streamId,
                  int32_t clientVersion = 0, bool grantAll = false) noexcept {   // D6 review: default = oldest (never 28 by omission)
        if (sessionId <= 0 || channel.size() > kMaxChannelBytes) return false;
        std::size_t idx = find_(sessionId);
        const bool fresh = (idx == kNone);
        if (fresh) idx = insert_(sessionId);
        if (idx == kNone) return false;                                   // table full
        if (fresh) grants_[idx].table = grantAll ? allowAllWriteTable() : denyWriteTable();
        clientVersion_[idx] = clientVersion;
        boundImage_[idx] = 0;                                              // Task 6: a plain announce is never response-mode
        PubState& st = states_[idx];
        if (edges_[idx].lost) { edges_[idx].lost = 0; markDirty_(idx); }   // a fresh publication: not lost (yet)
        // m3: an Announced / Resolving session is already in unresolved_.
        const bool listed = std::holds_alternative<PubAnnounced>(st) || std::holds_alternative<PubResolving>(st);
        leaveState_(st);
        if (freeChanCount_ > 0) [[likely]] {
            const uint16_t c = freeChan_[--freeChanCount_];
            std::memcpy(chanPool_[c].data(), channel.data(), channel.size());
            st = PubAnnounced{c, static_cast<uint16_t>(channel.size()), streamId};
        } else {
            // Connect burst beyond the channel pool: register now (the channel
            // view is still valid here) instead of refusing the session.
            addNow_(st, channel, streamId);
        }
        if (!listed && !std::holds_alternative<PubFailed>(st)) unresolved_[unresolvedCount_++] = sessionId;
        return true;
    }

    /// NAT-safe egress (Task 6, cold): announce a RESPONSE-MODE session.  Like announce(), but no channel and no
    /// channel slot: the session enters PubAwaitingBind and is NOT listed in unresolved_ (nothing to add yet).
    /// Idempotent for the second (crypto-key) announce wave: a session already awaiting its bind keeps its state
    /// (token refreshed), and one already BOUND keeps its publication and image — a late wave must never undo a bind.
    bool announceResponse(int64_t sessionId, const std::array<uint8_t, 16>& token, int32_t streamId,
                          int32_t clientVersion = 0, bool grantAll = false) noexcept {
        if (sessionId <= 0 || isZeroToken_(token)) return false;          // a zero token authorizes nothing: refused, no state
        std::size_t idx = find_(sessionId);
        const bool fresh = (idx == kNone);
        if (fresh) idx = insert_(sessionId);
        if (idx == kNone) return false;                                   // table full
        if (fresh) grants_[idx].table = grantAll ? allowAllWriteTable() : denyWriteTable();
        clientVersion_[idx] = clientVersion;
        PubState& st = states_[idx];
        if (!fresh) {
            // I2: a failure the runtime has not been told yet must still be reported: never turn it into a new
            // AwaitingBind (a re-announce after the failure was REPORTED is a fresh start and is allowed below).
            if (const auto* f = std::get_if<PubFailed>(&st); f && !f->reported) return true;
            if (auto* w = std::get_if<PubAwaitingBind>(&st)) { w->token = token; w->streamId = streamId; return true; }
            if (isBound_(boundImage_[idx]) && !std::holds_alternative<PubFailed>(st)) return true;   // bound: keep it
        }
        if (edges_[idx].lost) { edges_[idx].lost = 0; markDirty_(idx); }
        leaveState_(st);
        boundImage_[idx] = kAwaitingBind;
        st = PubAwaitingBind{token, streamId};
        return true;
    }

    /// An all-zero token is the "no token" marker (and what a failed key derivation yields): it must never authorize a
    /// bind, so every entry point refuses it before any compare.
    [[nodiscard]] static bool isZeroToken_(const std::array<uint8_t, 16>& t) noexcept {
        uint8_t any = 0;
        for (const uint8_t b : t) any = static_cast<uint8_t>(any | b);
        return any == 0;
    }
    /// Constant-time token compare: 0 = equal.
    [[nodiscard]] static uint8_t tokenDiff_(const std::array<uint8_t, 16>& a, const std::array<uint8_t, 16>& b) noexcept {
        uint8_t diff = 0;                                            // constant-time: fold every byte
        for (std::size_t i = 0; i < a.size(); ++i) diff = static_cast<uint8_t>(diff | (a[i] ^ b[i]));
        return diff;
    }
    /// I4: the bind's authorization half, with NO allocation and no side effect: Bound = the session awaits its bind
    /// and the token matches (constant-time), i.e. bind() would be admitted.  The worker calls it FIRST so an
    /// unauthenticated WorkerBind (any sender can aim one at a worker) costs one probe and one compare — never an
    /// image lookup or a channel string.
    [[nodiscard]] BindResult checkBind(int64_t sessionId, const std::array<uint8_t, 16>& token) const noexcept {
        if (isZeroToken_(token)) return BindResult::BadToken;   // before any lookup or compare
        const std::size_t idx = find_(sessionId);
        if (idx == kNone) return BindResult::UnknownSession;
        const auto* w = std::get_if<PubAwaitingBind>(&states_[idx]);
        if (!w) {
            // I2: bound (and not failed) with the SAME token = a repeated bind; anything else is NotAwaiting.
            if (!isBound_(boundImage_[idx]) || std::holds_alternative<PubFailed>(states_[idx]))
                return BindResult::NotAwaiting;
            return tokenDiff_(bindToken_[idx], token) != 0 ? BindResult::NotAwaiting : BindResult::AlreadyBound;
        }
        return tokenDiff_(w->token, token) != 0 ? BindResult::BadToken : BindResult::Bound;
    }

    /// NAT-safe egress (Task 6, cold): the client's WorkerBind named `imageSessionId` (the Aeron session id of ITS
    /// ingress image) and proved the token.  Bound: the response publication is added on `responseChannel` (copied,
    /// exactly like announce) and the session is pinned to that image.  Only PubAwaitingBind can bind: a second bind —
    /// even with the right token — never rebinds (a changed NAT mapping ends the session, spec D2).
    BindResult bind(int64_t sessionId, const std::array<uint8_t, 16>& token, int32_t imageSessionId,
                    std::string_view responseChannel) noexcept {
        const BindResult chk = checkBind(sessionId, token);   // defence: the worker already checked, never trust it
        const std::size_t idx = find_(sessionId);
        if (chk == BindResult::AlreadyBound)                          // I2: the same image again is a no-op; another is refused
            return boundImage_[idx] == imageKey_(imageSessionId) ? BindResult::AlreadyBound : BindResult::NotAwaiting;
        if (chk != BindResult::Bound) return chk;
        PubState& st = states_[idx];
        auto* w = std::get_if<PubAwaitingBind>(&st);
        if (responseChannel.size() > kMaxChannelBytes) return BindResult::BadToken;   // unreachable: the worker builds it
        const int32_t streamId = w->streamId;
        if (freeChanCount_ > 0) [[likely]] {
            const uint16_t c = freeChan_[--freeChanCount_];
            std::memcpy(chanPool_[c].data(), responseChannel.data(), responseChannel.size());
            st = PubAnnounced{c, static_cast<uint16_t>(responseChannel.size()), streamId};
        } else {
            addNow_(st, responseChannel, streamId);
        }
        boundImage_[idx] = imageKey_(imageSessionId);
        bindToken_[idx]  = token;
        if (!std::holds_alternative<PubFailed>(st)) unresolved_[unresolvedCount_++] = sessionId;
        return BindResult::Bound;
    }

    /// I2: the session is unknown (closed) or Failed: nothing will ever be sent on its publication.
    [[nodiscard]] bool pubGoneOrFailed(int64_t sessionId) const noexcept {
        const std::size_t idx = find_(sessionId);
        return idx == kNone || std::holds_alternative<PubFailed>(states_[idx]);
    }

    /// The Aeron session id of the ingress image `sessionId` is bound to (nullopt: not response-mode / not bound).
    [[nodiscard]] std::optional<int32_t> boundImage(int64_t sessionId) const noexcept {
        const std::size_t idx = find_(sessionId);
        if (idx == kNone || !isBound_(boundImage_[idx])) return std::nullopt;
        return static_cast<int32_t>(static_cast<uint32_t>(boundImage_[idx]));
    }

    /// The session's Ready publication (null otherwise).  Zero allocation; the pointer is valid until the next call
    /// that can release the publication (close, unbindImage, announce) — callers look it up per pass.
    template <class H = PubHandle>
    [[nodiscard]] typename H::element_type* readyPub(int64_t sessionId) const noexcept {   // dependent: only publication types with element_type
        // (the real policies) instantiate it; a bench's bump policy need not provide one
        const std::size_t idx = find_(sessionId);
        if (idx == kNone) return nullptr;
        auto* r = std::get_if<PubReady>(&states_[idx]);
        return r ? r->pub.get() : nullptr;
    }

    /// Cold: `f(int32_t imageSessionId)` for every session bound to an ingress image (Worker: the unavailable-image
    /// ring overflowed, so the images that still exist are re-derived from the sink).
    template <class F>
    void forEachBoundImage(F&& f) const {
        for (std::size_t i = 0; i < kMaxSessions; ++i)
            if (keys_[i] != 0 && isBound_(boundImage_[i]) && !std::holds_alternative<PubFailed>(states_[i]))
                f(static_cast<int32_t>(static_cast<uint32_t>(boundImage_[i])));
    }

    /// The client ingress image `imageSessionId` went away (Aeron unavailable-image): every session bound to it can
    /// never be answered again -> Failed(reported = false), so the runtime closes it (cold, linear scan).
    int unbindImage(int32_t imageSessionId) noexcept {
        int n = 0;
        for (std::size_t i = 0; i < kMaxSessions; ++i) {
            if (keys_[i] == 0 || boundImage_[i] != imageKey_(imageSessionId)) continue;
            PubState& st = states_[i];
            if (std::holds_alternative<PubFailed>(st)) continue;                       // already gone
            fail_(st);
            ++n;
        }
        return n;
    }

    /// Runtime close (Disconnect / reap / connect timeout / SessionFailed):
    /// forget the session; its queued acks drop at the next flush (once the
    /// lifecycle ring is consumed past them); its publication is released.
    void close(int64_t sessionId) noexcept {
        const std::size_t idx = find_(sessionId);
        if (idx == kNone) return;
        erase_(idx);
        tombstone_(sessionId);                                       // MEDIUM-2 (evicts the oldest when full)
    }

    /// Queue one ack for this pass's flush.  False only when the queue is full
    /// (the worker's intake gate makes that an invariant violation).
    bool enqueue(const AckEntry& a) noexcept {
        if (ackCount_ == kAckCapacity) [[unlikely]] return false;
        acks_[ackCount_++] = a;
        return true;
    }

    /// Task 23: every session is plaintext unless `lookupCrypto` says
    /// otherwise — the default keeps flush()'s old 2-arg signature and
    /// behavior byte-identical for every pre-Task-23 caller (this header's
    /// own 1000+-line test file calls `s.flush(a, b)` throughout; a defaulted
    /// template parameter means none of those call sites change).
    struct NoSealLookup {
        const RecordsSealCtx* operator()(int64_t /*sessionId*/) const noexcept { return nullptr; }
    };

    /// One pass: advance announcements, then claim every queued ack on its
    /// session publication in order.  `ringProducerPos` / `ringConsumerPos`:
    /// the worker's session-lifecycle ring, sampled now (after every frame of
    /// this pass was observed) — they decide when an unknown session's ack is
    /// truly orphaned (I1).
    /// Task 23: `lookupCrypto(sessionId) -> const RecordsSealCtx*` — null for
    /// a plaintext session (the default, and every existing caller); non-null
    /// means "seal this session's ack under the worker's OWN per-sender b2c
    /// cipher and sendSeq" (the SAME ctx/sendSeq Records sealing uses for that
    /// session — see SessionAckSink.h's file header and flushAcks_'s seal
    /// branch). The sink has no sessionId<->crypto table of its own (Rule 1:
    /// shared-nothing — the worker already owns that table, sessionCrypto_);
    /// this is the seam that lets the worker hand it over at flush time
    /// without SessionAckSink depending on Worker.h's types.
    /// Returns work done.
    template <class CryptoLookup = NoSealLookup>
    int flush(int64_t ringProducerPos, int64_t ringConsumerPos,
              CryptoLookup&& lookupCrypto = CryptoLookup{}) noexcept {
        const int work = flushAcks_(ringProducerPos, ringConsumerPos, lookupCrypto);
        if ((directDeferredThisPass_ | directEp_.tag) != 0) [[unlikely]] directEpisode_();
        return work;
    }

    /// G-A6 per-client pause: `sessionId` still holds acks after the last flush
    /// (its publication back-pressured / not connected / not yet resolved).  O(1) when nothing
    /// is held (the hot case) — one hash probe otherwise.
    [[nodiscard]] bool holding(int64_t sessionId) const noexcept {
        if (heldAfterFlush_ == 0) [[likely]] return false;
        const std::size_t idx = find_(sessionId);
        return idx != kNone && heldPass_[idx] == pass_;
    }

private:
    template <class CryptoLookup>
    int flushAcks_(int64_t ringProducerPos, int64_t ringConsumerPos, CryptoLookup& lookupCrypto) noexcept {
        ++pass_;
        int work = unresolvedCount_ ? resolvePending_() : 0;
        if (drainCount_) [[unlikely]] work += drainRegistrations_();
        heldNowCount_ = 0;
        if (ackCount_ == 0) [[likely]] {
            heldAfterFlush_ = 0;
            if (heldPrevCount_) [[unlikely]] noteHoldEdges_();
            return work;
        }
        std::size_t keep = 0;
        for (std::size_t i = 0; i < ackCount_; ++i) {
            AckEntry a = acks_[i];
            const std::size_t idx = find_(a.sessionId);
            if (idx == kNone) [[unlikely]] {
                // Unknown: hold until every announcement enqueued before its
                // frame existed is applied, then it is orphaned (closed session).
                if (a.sessionPos < 0) a.sessionPos = ringProducerPos;
                if (ringConsumerPos >= a.sessionPos) {
                    if (a.direct) directDone_(a.direct, kNone, false);
                    ++dropped_;
                    continue;
                }
                acks_[keep++] = a;
                continue;
            }
            PubState& st = states_[idx];
            if (auto* r = std::get_if<PubReady>(&st)) [[likely]] {
                if (r->blockedPass != pass_) {                      // per-session order kept
                    const int32_t ver = clientVersion_[idx];
                    // D6 review M4: the pre-D6 38 B frame below clientVersion 2.
                    const RecordsSealCtx* sealCtx = lookupCrypto(a.sessionId);
                    int64_t pos;
                    if (sealCtx) [[unlikely]] {
                        // Task 23: this session is ENCRYPTED — seal BEFORE
                        // claiming (mirrors sendRecordsFrame's ordering
                        // exactly: RecordsPush.h's file header explains why
                        // the zero-copy in-claim encode cannot survive AEAD
                        // sealing). `sealCtx->sendSeq` is the worker's ONE
                        // shared per-session counter (the SAME one Records
                        // sealing increments for this session, Worker.h Task
                        // 19/23) — ++*sendSeq happens inside sealRecordsFrame,
                        // so acks and Records draw from one monotonic
                        // sequence under the one b2c key, never two.
                        const std::size_t plainLen = rune::protocol::client::ProduceResponse::encodedSizeFor(ver, a.hasVerdicts != 0, a.verdictCount);
                        encode_(a, ver, ackPlainScratch_.data());
                        const std::size_t n = sealRecordsFrame(*sealCtx, ackPlainScratch_.data(), plainLen,
                                                               ackSealedScratch_.data(), ackSealedScratch_.size());
                        if (n == 0) [[unlikely]] {
                            // Fail-closed (G-A6 never-drop + Task 23 ★★): no
                            // send key installed yet, or an AEAD seal error —
                            // NEVER fall back to a plaintext claim. Hold this
                            // ack exactly like a back-pressured one (retried
                            // next pass, in order) without touching Lost/
                            // blockedPass — a missing key is not an Aeron
                            // connectivity signal.
                            if (heldPass_[idx] != pass_) {
                                heldPass_[idx] = pass_;
                                heldNow_[heldNowCount_++] = a.sessionId;
                            }
                            acks_[keep++] = a;
                            continue;
                        }
                        pos = policy_.claim(r->pub, n,
                                            [&](uint8_t* dst) { std::memcpy(dst, ackSealedScratch_.data(), n); });
                    } else {
                        pos = policy_.claim(r->pub, rune::protocol::client::ProduceResponse::encodedSizeFor(ver, a.hasVerdicts != 0, a.verdictCount),
                                            [&](uint8_t* dst) { encode_(a, ver, dst); });
                    }
                    if (pos > 0) [[likely]] {
                        if (const Edges& e = edges_[idx];
                            ((e.lost | e.reportedHeld) != 0) | ((e.dflags & (kDfDelivered | kDfExpiring)) != kDfDelivered))
                            [[unlikely]] claimedWatched_(idx);
                        if (a.credit) noteCredit_(idx);              // D8c: gateway-routed — return its credit
                        else if (a.direct) directDone_(a.direct, idx, true);   // D8d: its direct share
                        // Hop chain: the ack left on the client's session publication.
                        FLIGHT_RECORDER_MARK_VAL_REPLICATION(flight_recorder::Site::ShAckClaimed,
                                                             flight_recorder::foldCorr(a.correlationId),
                                                             static_cast<double>(a.baseOffset));
                        ++work;
                        continue;
                    }
                    // FR: refused (arg = -position: 1 NOT_CONNECTED, 2 BACK_PRESSURED,
                    // 3 ADMIN_ACTION, 4 fatal) — throttled, it retries every pass.
                    FLIGHT_RECORDER_THROTTLED_REPLICATION(frClaimBlocked_, flight_recorder::Site::ShAckClaimBlocked,
                                                          flight_recorder::foldCorr(a.correlationId),
                                                          static_cast<uint16_t>(pos < 0 ? -pos : 0));
                    if (pos == kClaimFatal) [[unlikely]] {           // can never deliver: CLOSED / MAX_POSITION
                        fail_(st);
                        if (a.direct) directDone_(a.direct, idx, false);
                        ++dropped_;
                        ++work;
                        continue;
                    }
                    setLost_(idx, pos == kClaimNotConnected);         // D8b round 2: suspicion, acks stay queued
                    r->blockedPass = pass_;                         // back-pressured / not connected
                }
            } else if (std::holds_alternative<PubFailed>(st)) {
                if (a.direct) directDone_(a.direct, idx, false);
                ++dropped_;                                          // reported → the runtime closes it
                continue;
            }
            // Announced / Resolving / AwaitingBind (Task 6: awaiting the client's WorkerBind) and a Ready
            // publication that refused the claim: the ack is HELD, in order.
            if (heldPass_[idx] != pass_) {                           // first held ack of this session
                heldPass_[idx] = pass_;
                heldNow_[heldNowCount_++] = a.sessionId;
            }
            acks_[keep++] = a;
        }
        ackCount_ = keep;
        heldAfterFlush_ = keep;
        if (heldPrevCount_ | heldNowCount_) [[unlikely]] noteHoldEdges_();
        return work;
    }

public:

    /// Report every failed session not yet reported: `tryReport(sessionId)`
    /// returns false when it cannot deliver now (return ring full) — the
    /// failure stays recorded in its state and is retried next pass (never
    /// dropped, never through an unbounded queue).  Cold: scans only while an
    /// unreported failure exists.  Returns how many were reported.
    template <class TryReport>
    int reportFailures(TryReport&& tryReport) noexcept {
        if (unreported_ == 0) [[likely]] return 0;
        int n = 0;
        for (std::size_t i = 0; i < kMaxSessions && unreported_ != 0; ++i) {
            if (keys_[i] == 0) continue;
            auto* f = std::get_if<PubFailed>(&states_[i]);
            if (!f || f->reported) continue;
            if (!tryReport(keys_[i])) break;
            f->reported = true;
            --unreported_;
            ++n;
        }
        return n;
    }

    /// Report every per-session edge the runtime has not heard yet, as
    /// `tryReport(sessionId, SessionSignalKind)` — in this order per session:
    ///   Lost / Found      the Lost verdict changed (D8b round 2);
    ///   Held / Released   the held state changed (fix round 2, finding 1);
    ///   Progress          acks of a reported-Held session were claimed, at most
    ///                     once per kProgressPassInterval passes since its Held
    ///                     edge / previous Progress (a pending one waits for the
    ///                     interval — never lost; dropped by Released).
    /// One signal per transition (no repeats while a state is stable).
    /// tryReport returns false when it cannot deliver now (return ring full):
    /// what was reported is recorded, the rest is retried next pass — never lost,
    /// never through an unbounded queue.  A closed session needs no report: the
    /// runtime's close clears its state.  Returns how many were reported.
    template <class TryReport>
    int reportSessionEdges(TryReport&& tryReport) noexcept {
        if (dirtyCount_ == 0) [[likely]] return 0;
        int n = 0;
        std::size_t keep = 0, k = 0;
        for (; k < dirtyCount_; ++k) {
            const int64_t sid = dirty_[k];
            const std::size_t idx = find_(sid);
            if (idx == kNone) continue;                              // closed
            Edges& e = edges_[idx];
            e.dirty = 0;
            if (e.lost != e.reportedLost) {
                if (!tryReport(sid, e.lost ? SessionSignalKind::Lost : SessionSignalKind::Found)) { e.dirty = 1; break; }
                e.reportedLost = e.lost;
                ++n;
            }
            const uint8_t held = (heldPass_[idx] == pass_ && heldAfterFlush_ != 0) ? 1 : 0;
            if (held != e.reportedHeld) {
                if (!tryReport(sid, held ? SessionSignalKind::Held : SessionSignalKind::Released)) { e.dirty = 1; break; }
                e.reportedHeld = held;
                e.progressed   = 0;                                  // Released: moot; Held: counted from now
                e.progressPass = pass_;
                ++n;
            }
            if (e.progressed) {
                if (pass_ - e.progressPass < kProgressPassInterval) {   // rate limit: wait, stay listed
                    e.dirty = 1;
                    dirty_[keep++] = sid;
                    continue;
                }
                if (!tryReport(sid, SessionSignalKind::Progress)) { e.dirty = 1; break; }
                e.progressed   = 0;
                e.progressPass = pass_;
                ++n;
            }
        }
        for (; k < dirtyCount_; ++k) dirty_[keep++] = dirty_[k];      // the unreported tail
        dirtyCount_ = keep;
        return n;
    }

    /// D8c — produce credits of the GATEWAY-routed path.  Every claimed ack with
    /// AckEntry::credit set adds one to its session's count; this reports each
    /// non-zero count as `tryReport(sessionId, n)` (n > 0, one call per session
    /// per pass at most — coalesced) and zeroes it.  An ack is counted when it
    /// is CLAIMED, never at enqueue: the runtime's per-session credit budget
    /// therefore bounds the gateway-routed acks this sink can hold for one
    /// session, whatever the client's read rate (G-A6, pipeline-depth
    /// independent).  Dropped acks (closed / failed session) return nothing —
    /// the runtime's close discards the session's whole budget.  tryReport
    /// false (return ring full) = the count stays and is retried next pass:
    /// never lost, never double-counted, never through an unbounded queue.
    /// Returns how many were reported.
    template <class TryReport>
    int reportCredits(TryReport&& tryReport) noexcept {
        if (creditDirtyCount_ == 0) [[likely]] return 0;
        int n = 0;
        std::size_t keep = 0, k = 0;
        for (; k < creditDirtyCount_; ++k) {
            const int64_t sid = creditDirty_[k];
            const std::size_t idx = find_(sid);
            if (idx == kNone) continue;                              // closed: its budget died with it
            Edges& e = edges_[idx];
            if (!tryReport(sid, e.credits)) break;                   // the rest stays listed
            e.credits = 0;
            e.creditListed = 0;
            ++n;
        }
        for (; k < creditDirtyCount_; ++k) creditDirty_[keep++] = creditDirty_[k];   // the unreported tail
        creditDirtyCount_ = keep;
        return n;
    }
    // -----------------------------------------------------------------------
    // D8d — DIRECT (worker-ingress) produce budget.  See the header comment.
    // -----------------------------------------------------------------------
    /// Per-session budget and worker cap (cold: setup / tests).  The cap is
    /// clamped to [4 x kDirectLightInFlight, kAckCapacity], the budget to
    /// [1, cap]; the congested band is the cap's last quarter.
    void setDirectBudgets(uint32_t perSession, uint32_t workerCap) noexcept {
        workerCap  = std::clamp<uint32_t>(workerCap, 4 * kDirectLightInFlight, static_cast<uint32_t>(kAckCapacity));
        perSession = std::clamp<uint32_t>(perSession, 1u, workerCap);
        directCap_           = workerCap;
        directBandStart_     = workerCap - workerCap / 4;
        directSessionBudget_ = perSession;
    }
    /// Admit one direct produce frame of `sessionId` (the worker's controlled
    /// poll, per frame — one hash probe): kDirectRefused = ABORT it; otherwise it
    /// is counted (AckEntry::direct = the result) until its ack leaves the queue.
    [[nodiscard]] uint8_t admitDirect(int64_t sessionId) noexcept { return admitDirect_<false>(sessionId, 0); }
    /// NAT-safe egress (Task 6): the same single probe also checks WHERE the frame came from.  `imageSessionId` is
    /// the Aeron session id of the image the frame arrived on.  A response-mode session accepts produce only from
    /// the image it is bound to: a frame from another image, or for a session still awaiting its bind, is
    /// kDirectDropSpoofed (CONSUME it: no append, no reply — the sender may be an attacker; session ids are
    /// sequential).  A session that is not response-mode takes today's path (one extra load + compare).
    [[nodiscard]] uint8_t admitDirect(int64_t sessionId, int32_t imageSessionId) noexcept {
        return admitDirect_<true>(sessionId, imageSessionId);
    }
    /// Frames consumed by the image check (test seam).
    [[nodiscard]] uint64_t directDroppedSpoofedForTest() const noexcept { return directDroppedSpoofed_; }
private:
    template <bool kCheckImage>
    [[nodiscard]] uint8_t admitDirect_(int64_t sessionId, int32_t imageSessionId) noexcept {
        const std::size_t idx = find_(sessionId);
        uint32_t mine = 0;
        if (idx != kNone) [[likely]] {
            if constexpr (kCheckImage) {
                // I5: ONE load + compare.  0 = plain (admit); kAwaitingBind = awaiting its bind (never equals an
                // image key: dropped); else the bound image's key.
                const int64_t bound = boundImage_[idx];
                if (bound != 0 && bound != imageKey_(imageSessionId)) [[unlikely]] {
                    ++directDroppedSpoofed_;
                    return kDirectDropSpoofed;
                }
            }
            if (heldAfterFlush_ != 0 && heldPass_[idx] == pass_) [[unlikely]] {   // G-A6: it holds acks
                ++directRefusedHeld_;
                return kDirectRefused;
            }
            mine = edges_[idx].direct;
            if (mine >= directSessionBudget_) [[unlikely]] { ++directRefusedBudget_; return kDirectRefused; }
        }
        else if (closedRecently_(sessionId)) [[unlikely]] {         // MEDIUM-2: never append a closed session's backlog
            ++directDroppedClosed_;
            return kDirectDropClosed;
        }
        if (directInFlight_ >= directBandStart_) [[unlikely]] {
            if (directInFlight_ >= directCap_) {
                ++directRefusedFull_;
                directDeferredThisPass_ = 1;                          // a deferred pass (expiry pacing)
                return kDirectRefused;
            }
            if (mine >= kDirectLightInFlight) {
                ++directRefusedBand_;
                // Review MEDIUM-1: a (non-held) session deferred by the band is a
                // deferred pass too — held sessions parking the band without
                // filling it are expired like a full share's.
                directDeferredThisPass_ = 1;
                return kDirectRefused;
            }
        }
        ++directInFlight_;
        if (idx == kNone) [[unlikely]] return kDirectTotalOnly;     // announcement not applied yet (I1)
        ++edges_[idx].direct;
        return kDirectCounted;
    }
public:
    /// Give back an admitted direct frame that produced no answer (undecodable).
    void releaseDirect(int64_t sessionId, uint8_t tag) noexcept {
        if (tag == kDirectRefused) return;
        directDone_(tag, find_(sessionId), false);
    }
    /// Report the pending direct expiry as `tryReport(sessionId)` (false = the
    /// return ring is full: kept and retried next pass, never lost).  The session
    /// stays marked until the runtime's close erases it.  Returns 1 if reported.
    template <class TryReport>
    int reportDirectExpiry(TryReport&& tryReport) noexcept {
        if (expireUnreported_ == 0) [[likely]] return 0;
        const int64_t sid = expireUnreported_;
        const std::size_t idx = find_(sid);
        if (idx == kNone) { expireUnreported_ = 0; return 0; }    // closed meanwhile (erase_ clears it too)
        Edges& e = edges_[idx];
        // Review LOW-1: re-checked when the signal is actually sent — a victim that
        // is no longer held, or claimed an ack since it was chosen, is reading.
        if ((e.dflags & kDfReadSinceChosen) != 0 || !expiryEligible_(idx)) {
            e.dflags = static_cast<uint8_t>(e.dflags & ~(kDfExpiring | kDfReadSinceChosen));
            --expiringCount_;
            expireUnreported_ = 0;
            ++directExpiryCancels_;
            return 0;
        }
        if (!tryReport(sid, e.direct)) return 0;
        expireUnreported_ = 0;
        return 1;
    }
    [[nodiscard]] std::size_t directInFlight() const noexcept { return directInFlight_; }
    [[nodiscard]] uint32_t directInFlightOf(int64_t sessionId) const noexcept {
        const std::size_t idx = find_(sessionId);
        return idx == kNone ? 0u : edges_[idx].direct;
    }
    [[nodiscard]] uint32_t directCap() const noexcept { return directCap_; }
    [[nodiscard]] uint32_t directSessionBudget() const noexcept { return directSessionBudget_; }
    [[nodiscard]] uint64_t directExpiries() const noexcept { return directExpiries_; }
    [[nodiscard]] int64_t  lastDirectExpiredForTest() const noexcept { return lastDirectExpired_; }
    [[nodiscard]] uint64_t directRefusedHeldForTest() const noexcept { return directRefusedHeld_; }
    [[nodiscard]] uint64_t directRefusedBudgetForTest() const noexcept { return directRefusedBudget_; }
    [[nodiscard]] uint64_t directRefusedBandForTest() const noexcept { return directRefusedBand_; }
    [[nodiscard]] uint64_t directRefusedFullForTest() const noexcept { return directRefusedFull_; }
    [[nodiscard]] uint64_t directUnderflowsForTest() const noexcept { return directUnderflows_; }
    [[nodiscard]] uint64_t directDroppedClosed() const noexcept { return directDroppedClosed_; }
    [[nodiscard]] uint64_t directExpiryCancelsForTest() const noexcept { return directExpiryCancels_; }

    /// Claimed credits of `sessionId` not yet reported (test seam).
    [[nodiscard]] uint32_t unreportedCreditsForTest(int64_t sessionId) const noexcept {
        const std::size_t idx = find_(sessionId);
        return idx == kNone ? 0u : edges_[idx].credits;
    }

    /// Fix round 2 (finding 8): registrations of sessions closed / re-announced
    /// while Resolving, still being released (find → drop).
    [[nodiscard]] std::size_t drainingRegistrations() const noexcept { return drainCount_; }
    /// A10: ack-publication re-adds after a (transient) driver error.
    [[nodiscard]] uint64_t pubAddRetries() const noexcept { return pubAddRetries_; }
    /// A10 review M3: channel slots currently free (all of them once no session is Announced / Resolving).
    [[nodiscard]] std::size_t freeChannelSlotsForTest() const noexcept { return freeChanCount_; }
    [[nodiscard]] uint64_t    drainOverflowForTest() const noexcept { return drainOverflow_; }
    [[nodiscard]] std::size_t freeAckSlots() const noexcept { return kAckCapacity - ackCount_; }
    [[nodiscard]] std::size_t pending() const noexcept { return ackCount_; }
    /// Acks still queued after the last flush (held for their sessions).
    [[nodiscard]] std::size_t heldAcks() const noexcept { return heldAfterFlush_; }
    [[nodiscard]] std::size_t sessionCount() const noexcept { return count_; }

    /// Authentication Task 13: the session's WRITE grants (nullptr = no such session).  O(1) average: one find_.
    [[nodiscard]] const WriteGrantSlot* writeGrants(int64_t sessionId) const noexcept {
        const std::size_t idx = find_(sessionId);
        return idx == kNone ? nullptr : &grants_[idx];
    }
    /// Issue #259: swap `words` into TABLE `tableId` (the old buffer comes back in `words`): no allocation.  Every
    /// session bound to it sees the new bits at once.  False = an id beyond the store (never sent by the runtime).
    bool installWriteGrants(int64_t tableId, std::vector<uint64_t>& words, bool grantAll, uint64_t /*epoch*/,
                            uint64_t /*owner*/) noexcept {
        if (static_cast<uint64_t>(tableId) >= kMaxWritePermTables) return false;
        WritePermTable& t = permTables_[static_cast<std::size_t>(tableId)];
        t.bits.swap(words);
        t.grantAll = grantAll;
        return true;
    }
    /// Words [first, first + n) of table `tableId`, the table resized to `total` words (a created / dropped object).
    /// Cold (DDL rate); a grown table allocates here, on the lifecycle ring, never on the produce path.
    bool patchWriteTable(int64_t tableId, uint64_t first, uint64_t total, const uint8_t* words, std::size_t n) {
        if (static_cast<uint64_t>(tableId) >= kMaxWritePermTables || first > total || n > total - first) return false;
        WritePermTable& t = permTables_[static_cast<std::size_t>(tableId)];
        if (t.bits.size() != total) t.bits.resize(static_cast<std::size_t>(total), 0);
        if (n != 0) std::memcpy(t.bits.data() + first, words, n * 8);   // little-endian hosts only
        return true;
    }
    /// Session `sessionId` now uses table `tableId` (-2: suspended ReadOnly, other < 0: deny everything).  False =
    /// unknown session / bad id.
    bool bindWriteGrants(int64_t sessionId, int64_t tableId, uint64_t epoch, uint64_t owner) noexcept {
        const std::size_t idx = find_(sessionId);
        if (idx == kNone || tableId >= static_cast<int64_t>(kMaxWritePermTables)) return false;
        WriteGrantSlot& g = grants_[idx];
        g.table    = tableId == -2 ? suspendedWriteTable()
                   : tableId < 0   ? denyWriteTable()
                                   : &permTables_[static_cast<std::size_t>(tableId)];
        g.epoch    = epoch;
        g.ownerTag = rune::protocol::pidOwnerTag(owner);
        return true;
    }
    /// Table `tableId` is freed: every slot still pointing at it is reset to deny (a closing session's), its bits
    /// cleared (capacity persists).  O(kMaxSessions), cold (a grant set's last holder left).
    bool dropWriteTable(int64_t tableId) noexcept {
        if (static_cast<uint64_t>(tableId) >= kMaxWritePermTables) return false;
        WritePermTable* t = &permTables_[static_cast<std::size_t>(tableId)];
        for (std::size_t i = 0; i < kMaxSessions; ++i)
            if (grants_[i].table == t) grants_[i].table = denyWriteTable();
        t->bits.clear();
        t->grantAll = false;
        return true;
    }
    /// Test seam: slots examined by the last find_ of `sessionId` (probe chain length).
    [[nodiscard]] std::size_t probeChainForTest(int64_t sessionId) const noexcept {
        std::size_t i = hash_(sessionId);
        std::size_t n = 1;
        for (; keys_[i] != 0 && keys_[i] != sessionId && n <= kMaxSessions; i = (i + 1) & kMask) ++n;
        return n;
    }
    [[nodiscard]] std::size_t unreportedFailures() const noexcept { return unreported_; }
    [[nodiscard]] std::size_t pendingForTest() const noexcept { return ackCount_; }
    [[nodiscard]] uint64_t    droppedForTest() const noexcept { return dropped_; }
    [[nodiscard]] const PubState* stateForTest(int64_t sessionId) const noexcept {
        const std::size_t idx = find_(sessionId);
        return idx == kNone ? nullptr : &states_[idx];
    }

private:
    static constexpr std::size_t kNone     = static_cast<std::size_t>(-1);
    static constexpr std::size_t kMask     = kMaxSessions - 1;
    static constexpr int         kBits     = std::countr_zero(kMaxSessions);
    static_assert(std::has_single_bit(kMaxSessions), "kMaxSessions must be a power of two");
    static_assert(kMaxChannelSlots <= 65536, "channel slot index is uint16");

    /// A bound image as a non-zero key (Aeron session ids are any int32, 0 included): bit 32 marks "bound".
    static constexpr int64_t imageKey_(int32_t imageSessionId) noexcept {
        return (int64_t{1} << 32) | static_cast<int64_t>(static_cast<uint32_t>(imageSessionId));
    }
    static_assert(kAwaitingBind != 0 && imageKey_(0) > 0 && imageKey_(-1) > 0 && imageKey_(INT32_MIN) > 0,
                  "an image key (bit 32 set, bits 33+ clear) is positive: never 0 and never kAwaitingBind");
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
    static void resetGrants_(WriteGrantSlot& g) noexcept { g.table = denyWriteTable(); g.epoch = 0; g.ownerTag = 0; }
    std::size_t insert_(int64_t sid) noexcept {
        if (count_ == kMaxSessions - 1) return kNone;               // one empty slot ends every probe
        std::size_t i = hash_(sid);
        while (keys_[i] != 0) i = (i + 1) & kMask;
        keys_[i] = sid;
        states_[i] = PubFailed{};                                    // neutral (reported) placeholder
        heldPass_[i] = 0; edges_[i] = Edges{}; clientVersion_[i] = 0; boundImage_[i] = 0; bindToken_[i] = {};
        resetGrants_(grants_[i]);
        ++count_;
        return i;
    }
    void erase_(std::size_t i) noexcept {                            // backward-shift deletion
        leaveState_(states_[i]);
        if (edges_[i].dflags & kDfExpiring) {                        // D8d: the runtime's close arrived
            --expiringCount_;
            if (expireUnreported_ == keys_[i]) expireUnreported_ = 0;
        }
        keys_[i] = 0;
        states_[i] = PubFailed{};                                    // releases a Ready publication
        heldPass_[i] = 0; edges_[i] = Edges{}; clientVersion_[i] = 0; boundImage_[i] = 0; bindToken_[i] = {};
        resetGrants_(grants_[i]);
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
            heldPass_[i] = heldPass_[j];
            edges_[i] = edges_[j];
            clientVersion_[i] = clientVersion_[j];
            boundImage_[i] = boundImage_[j];
            bindToken_[i] = bindToken_[j];
            std::swap(grants_[i], grants_[j]);                       // the buffer travels with its key; j's old one is reset below
            keys_[j] = 0;
            states_[j] = PubFailed{};
            heldPass_[j] = 0; edges_[j] = Edges{}; clientVersion_[j] = 0; boundImage_[j] = 0; bindToken_[j] = {};
            resetGrants_(grants_[j]);
            i = j;
        }
    }
    /// Bookkeeping when a state is replaced: return its channel slot; forget an
    /// unreported failure (closed / re-announced: nothing left to report).
    void leaveState_(const PubState& st) noexcept {
        if (const auto* a = std::get_if<PubAnnounced>(&st); a && freeChanCount_ < kMaxChannelSlots)
            freeChan_[freeChanCount_++] = a->chan;
        else if (const auto* f = std::get_if<PubFailed>(&st); f && !f->reported)
            --unreported_;
        else if (std::holds_alternative<PubAwaitingBind>(st)) { /* Task 6: no channel slot, not listed, nothing registered */ }
        else if (const auto* r = std::get_if<PubResolving>(&st)) {
            freeChanSlot_(r->chan);                                  // A10: the slot it kept for a re-add
            if (r->regId < 0) return;                                // a re-add was pending: nothing registered
            // finding 8: never leaked.  At most one Resolving registration per
            // live session is ever pending here (errored ones are not queued),
            // so the list cannot fill; if it ever did, count it — never silent.
            if (drainCount_ < kMaxSessions) [[likely]] draining_[drainCount_++] = r->regId;
            else ++drainOverflow_;
        }
    }
    /// Hold edges of this flush: sessions held now that were not reported held,
    /// and sessions held at the previous flush that no longer are → dirty.
    void noteHoldEdges_() noexcept {
        for (std::size_t k = 0; k < heldPrevCount_; ++k) {
            const std::size_t idx = find_(heldPrev_[k]);
            if (idx != kNone && heldPass_[idx] != pass_) {                 // released
                FLIGHT_RECORDER_MARK_ARG_VAL_PRODUCE(flight_recorder::Site::ShAckHold,
                                                     static_cast<uint32_t>(heldPrev_[k]), 0, 0.0);
                markDirty_(idx);
            }
        }
        for (std::size_t k = 0; k < heldNowCount_; ++k) {
            const std::size_t idx = find_(heldNow_[k]);
            if (idx == kNone) continue;
#if FLIGHT_RECORDER && !defined(FLIGHT_RECORDER_NO_PRODUCE)
            if (flight_recorder::enabled(flight_recorder::Site::ShAckHold)) [[unlikely]] {   // FR edge: newly held
                bool wasHeld = false;
                for (std::size_t j = 0; j < heldPrevCount_ && !wasHeld; ++j) wasHeld = heldPrev_[j] == heldNow_[k];
                if (!wasHeld) flight_recorder::markArgVal(flight_recorder::Site::ShAckHold,
                                                          static_cast<uint32_t>(heldNow_[k]), 1,
                                                          static_cast<double>(heldAfterFlush_));
            }
#endif
            if (edges_[idx].reportedHeld == 0) markDirty_(idx);                  // newly held
        }
        for (std::size_t k = 0; k < heldNowCount_; ++k) heldPrev_[k] = heldNow_[k];   // only the live prefix
        heldPrevCount_ = heldNowCount_;
        heldNowCount_ = 0;
    }
    /// D8b round 2: a claim on a Lost or reported-Held session succeeded — it is
    /// connected (Found) and, if the runtime knows it Held, it progressed.
    void claimedWatched_(std::size_t idx) noexcept {
        Edges& e = edges_[idx];
        // D8d review LOW-1 / LOW-2: it delivered (once is enough), and a chosen
        // expiry victim that claims is reading — cancelled at report time.
        e.dflags = static_cast<uint8_t>(e.dflags | kDfDelivered | ((e.dflags & kDfExpiring) ? kDfReadSinceChosen : 0));
        if (e.lost) { e.lost = 0; markDirty_(idx); }
        if (e.reportedHeld && !e.progressed) { e.progressed = 1; markDirty_(idx); }
    }
    /// D8b round 2: a failed (non-fatal) claim's verdict — NOT_CONNECTED = Lost;
    /// BACK_PRESSURED / ADMIN_ACTION = connected (Aeron returns them only then).
    void setLost_(std::size_t idx, bool lost) noexcept {
        Edges& e = edges_[idx];
        if (e.lost == static_cast<uint8_t>(lost)) [[likely]] return;
        e.lost = lost ? 1 : 0;
        markDirty_(idx);
    }
    /// D8c: one gateway-routed ack of this session was claimed.
    void noteCredit_(std::size_t idx) noexcept {
        Edges& e = edges_[idx];
        ++e.credits;
        if (e.creditListed) [[likely]] return;
        if (creditDirtyCount_ == kMaxSessions) [[unlikely]] {        // closed sessions' entries: compact
            std::size_t keep = 0;
            for (std::size_t k = 0; k < creditDirtyCount_; ++k)
                if (const std::size_t j = find_(creditDirty_[k]); j != kNone && edges_[j].creditListed)
                    creditDirty_[keep++] = creditDirty_[k];
            creditDirtyCount_ = keep;                                // < kMaxSessions live sessions
        }
        e.creditListed = 1;
        creditDirty_[creditDirtyCount_++] = keys_[idx];              // <= one entry per live session
    }
    /// D8d: one counted direct answer left the queue (claimed: the session is
    /// reading — marked for the current congestion episode) or was dropped, or an
    /// admitted frame produced no answer.  `idx` kNone = the session is gone: its
    /// share died with it (erase_), only the worker total is returned.
    void directDone_(uint8_t tag, std::size_t idx, bool claimed) noexcept {
        if (directInFlight_ == 0) [[unlikely]] { ++directUnderflows_; return; }   // unreachable: counted once each
        --directInFlight_;
        if (idx == kNone) return;
        Edges& e = edges_[idx];
        if (tag == kDirectCounted && e.direct != 0) --e.direct;
        if (claimed && directEp_.tag != 0) [[unlikely]] e.directTag = directEp_.tag;
    }
    /// D8d: once per pass while a frame was deferred this pass (full share, or —
    /// fix round MEDIUM-1 — the band deferring a non-held session) or an episode
    /// is open.  Episode: starts with such a deferral; ends with its single
    /// expiry, or when the total drops below the congested band.  A session is a
    /// reader (never blamed) if one of its direct acks was claimed in the current
    /// scan window (kDirectExpireAfterDeferredPasses deferred passes; a fresh tag
    /// per window).  While an expiry awaits the
    /// runtime's close nothing is counted (one in flight).  Cold: the scan runs
    /// at most once per kDirectExpireAfterDeferredPasses deferred passes.
    [[gnu::cold]] void directEpisode_() noexcept {
        const bool full = directDeferredThisPass_ != 0;
        directDeferredThisPass_ = 0;
        if (!full) {
            if (directInFlight_ < directBandStart_) directEp_ = DirectEpisode{};
            return;
        }
        if (expiringCount_ != 0) return;                             // one expiry in flight
        if (directEp_.tag == 0) {                                    // a new episode
            if (++directEpTags_ == 0) ++directEpTags_;               // tags are never 0
            directEp_ = DirectEpisode{directEpTags_, 0};
        }
        if (++directEp_.deferredPasses % kDirectExpireAfterDeferredPasses != 0) return;
        std::size_t victim = kNone;
        uint16_t share = 0;
        for (std::size_t i = 0; i < kMaxSessions; ++i) {
            if (keys_[i] == 0 || !expiryEligible_(i)) continue;      // not held / never delivered: not to blame
            const Edges& e = edges_[i];
            if ((e.dflags & kDfExpiring) || e.directTag == directEp_.tag) continue;   // claimed in this episode: a (slow) reader
            if (e.direct > share) { share = e.direct; victim = i; }
        }
        if (victim == kNone) {                                       // no held session to blame: keep deferring
            // Fix round (MEDIUM-1): reader marks count claims of the LAST scan
            // window only — a fresh tag per window.  A session that read while it
            // was being admitted (or before it stalled) in a long band episode is
            // judged by what it does now, not marked a reader forever.
            if (++directEpTags_ == 0) ++directEpTags_;
            directEp_.tag = directEpTags_;
            return;
        }
        directEp_ = DirectEpisode{};                                 // the episode ends with its one expiry
        edges_[victim].dflags = static_cast<uint8_t>((edges_[victim].dflags | kDfExpiring) & ~kDfReadSinceChosen);
        ++expiringCount_;
        expireUnreported_ = keys_[victim];
        lastDirectExpired_ = keys_[victim];
        ++directExpiries_;
    }
    /// D8d: may this session be expired?  HELD after this flush (acks queued its
    /// publication refuses) AND its publication is Ready and has DELIVERED before
    /// (review LOW-2: a session whose publication is still being added /
    /// resolved / connecting — a brand-new client — is never blamed; one that
    /// never connects is D8b Lost and reaped on its lease).
    [[nodiscard]] bool expiryEligible_(std::size_t i) const noexcept {
        return heldAfterFlush_ != 0 && heldPass_[i] == pass_ && (edges_[i].dflags & kDfDelivered) != 0 &&
               std::holds_alternative<PubReady>(states_[i]);
    }
    /// MEDIUM-2: was `sid` closed here recently?  Consulted only for a frame
    /// whose session is not in the table — one O(1) probe (lows batch C: was a
    /// scan of every tombstone per frame, ~1k compares for each frame of a
    /// closed client's backlog or of a not-yet-announced session).
    [[nodiscard]] bool closedRecently_(int64_t sid) const noexcept { return tombFind_(sid) != kNone; }
    static std::size_t tombHash_(int64_t sid) noexcept {
        return static_cast<std::size_t>((static_cast<uint64_t>(sid) * 0x9E3779B97F4A7C15ULL) >> (64 - kTombBits));
    }
    std::size_t tombFind_(int64_t sid) const noexcept {
        for (std::size_t i = tombHash_(sid);; i = (i + 1) & kTombMask) {   // load <= 1/2: always an empty slot
            if (tombSet_[i] == 0)   return kNone;
            if (tombSet_[i] == sid) return i;
        }
    }
    /// Remember a closed session (FIFO): when full, the oldest tombstone is
    /// evicted from the set first.  A session closed again (re-announced in
    /// between) keeps its older tombstone.
    void tombstone_(int64_t sid) noexcept {
        if (tombFind_(sid) != kNone) return;
        if (tombCount_ == kClosedTombstones) {
            tombErase_(tombFind_(closed_[tombHead_]));
            tombHead_ = (tombHead_ + 1) & (kClosedTombstones - 1);
            --tombCount_;
        }
        closed_[(tombHead_ + tombCount_) & (kClosedTombstones - 1)] = sid;
        ++tombCount_;
        std::size_t i = tombHash_(sid);
        while (tombSet_[i] != 0) i = (i + 1) & kTombMask;
        tombSet_[i] = sid;
    }
    void tombErase_(std::size_t i) noexcept {                        // backward-shift deletion
        if (i == kNone) [[unlikely]] return;                         // unreachable: ring and set agree
        tombSet_[i] = 0;
        for (std::size_t j = i;;) {
            j = (j + 1) & kTombMask;
            if (tombSet_[j] == 0) return;
            const std::size_t home = tombHash_(tombSet_[j]);
            const bool stays = (i <= j) ? (i < home && home <= j) : (i < home || home <= j);
            if (stays) continue;
            tombSet_[i] = tombSet_[j];
            tombSet_[j] = 0;
            i = j;
        }
    }
    void markDirty_(std::size_t idx) noexcept {
        if (edges_[idx].dirty) return;
        if (dirtyCount_ == kMaxSessions) [[unlikely]] {              // closed sessions' entries: compact
            std::size_t keep = 0;
            for (std::size_t k = 0; k < dirtyCount_; ++k)
                if (const std::size_t j = find_(dirty_[k]); j != kNone && edges_[j].dirty) dirty_[keep++] = dirty_[k];
            dirtyCount_ = keep;                                      // < kMaxSessions live sessions
        }
        edges_[idx].dirty = 1;
        dirty_[dirtyCount_++] = keys_[idx];                          // <= one entry per live session
    }
    /// Release registrations whose session went away while they resolved.
    int drainRegistrations_() noexcept {
        std::size_t keep = 0;
        for (std::size_t k = 0; k < drainCount_; ++k) {
            PubHandle pub{};
            if (policy_.find(draining_[k], pub) == PubFind::Pending) draining_[keep++] = draining_[k];
            // Ready: `pub` drops here (released); Error: the driver already dropped it.
        }
        const int done = static_cast<int>(drainCount_ - keep);
        drainCount_ = keep;
        return done;
    }
    void fail_(PubState& st) noexcept {
        leaveState_(st);
        st = PubFailed{false};
        ++unreported_;
    }
    void freeChanSlot_(uint16_t c) noexcept {
        if (c != kNoChan && freeChanCount_ < kMaxChannelSlots) freeChan_[freeChanCount_++] = c;
    }
    void addNow_(PubState& st, std::string_view channel, int32_t streamId) noexcept {
        const int64_t reg = policy_.add(channel, streamId);
        if (reg < 0) { st = PubFailed{false}; ++unreported_; return; }
        st = PubResolving{reg};
    }
    int resolvePending_() noexcept {
        int work = 0;
        std::size_t keep = 0;
        for (std::size_t k = 0; k < unresolvedCount_; ++k) {
            const int64_t sid = unresolved_[k];
            const std::size_t idx = find_(sid);
            if (idx == kNone) continue;                              // closed meanwhile
            PubState& st = states_[idx];
            if (const auto* a = std::get_if<PubAnnounced>(&st)) {
                const PubAnnounced ann = *a;                         // A10: the slot moves to Resolving (re-add)
                const int64_t reg = policy_.add(std::string_view(chanPool_[ann.chan].data(), ann.chanLen), ann.streamId);
                ++work;
                if (reg < 0) { freeChanSlot_(ann.chan); st = PubFailed{false}; ++unreported_; continue; }
                st = PubResolving{reg, ann.chan, ann.chanLen, ann.streamId, 1};
                unresolved_[keep++] = sid;
                continue;
            }
            if (auto* r = std::get_if<PubResolving>(&st)) {
                if (r->regId < 0) {                                  // A10: a backed-off re-add
                    if (pass_ < r->retryPass) { unresolved_[keep++] = sid; continue; }
                    const int64_t reg = policy_.add(std::string_view(chanPool_[r->chan].data(), r->chanLen),
                                                    r->streamId);
                    ++work;
                    if (reg < 0) { freeChanSlot_(r->chan); st = PubFailed{false}; ++unreported_; continue; }
                    r->regId = reg;
                    ++r->attempts;
                    ++pubAddRetries_;
                    unresolved_[keep++] = sid;
                    continue;
                }
                PubHandle pub{};
                const PubFind f = policy_.find(r->regId, pub);
                if (f == PubFind::Ready) {
                    freeChanSlot_(r->chan);
                    st = PubReady{std::move(pub), 0};
                    ++work;
                    continue;
                }
                if (f == PubFind::Error) {
                    // The client conductor erased the registration before
                    // reporting the error (on Aeron 1.51 a later find of it
                    // THROWS "Unknown registration id", reported as PubFind::Error): nothing to release — NOT via leaveState_'s
                    // drain push (fix round 3).
                    ++work;
                    // A10: a driver error can be transient (EINTR: tmpfs fallocate interrupted by pending
                    // io_uring task work on this worker thread) — re-add on the same channel, bounded; only a
                    // persistent error (bad channel) fails the session.
                    if (r->chan != kNoChan && r->attempts < kMaxPubAddAttempts) {
                        r->regId     = -1;                           // errored regs are never drained (fix round 3)
                        r->retryPass = pass_ + (uint64_t{1} << r->attempts);   // 2, 4, ... 128 passes
                        unresolved_[keep++] = sid;
                        continue;
                    }
                    freeChanSlot_(r->chan);
                    st = PubFailed{false};
                    ++unreported_;
                    continue;
                }
                unresolved_[keep++] = sid;                           // driver answers in a pass or two
            }
        }
        unresolvedCount_ = keep;
        return work;
    }
    static void encode_(const AckEntry& a, int32_t clientVersion, uint8_t* dst) noexcept {
        rune::protocol::client::ProduceResponse r;
        r.sessionId      = a.sessionId;
        r.correlationId  = a.correlationId;
        r.errorCode      = a.errorCode;
        r.partition      = a.partition;
        r.baseOffset     = a.baseOffset;
        r.logStartOffset = a.logStartOffset;
        // Client 17: encodeFor writes these only for a session at 17 or later.  The verdict arena is Part 04's.
        assert(a.verdictCount == 0);
        r.assignedBase   = a.assignedBase;
        r.hasVerdicts    = a.hasVerdicts != 0;
        r.verdictCount   = a.verdictCount;
        // Task 14 (D28): a Throttled retry-after only to a v14+ client (encodeFor answers it InvalidRequest), as the
        // gateway path does.
        if (a.errorCode == static_cast<int16_t>(rune::protocol::client::ErrorCode::Throttled) &&
            clientVersion < rune::protocol::client::kClientVersionNamespaces)
            r.logStartOffset = -1;
        // D6: UnknownProducerId only to clients that understand it (else OOS —
        // the pre-D6 answer), and (review M4) the pre-D6 38 B frame to them.
        r.encodeFor(dst, clientVersion);
    }

    PubPolicy                                                        policy_;
    std::array<int64_t, kMaxSessions>                                keys_{};      // 0 = empty
    std::array<PubState, kMaxSessions>                               states_{};
    std::array<uint64_t, kMaxSessions>                               heldPass_{};  // pass that last held acks
    /// Per-session report state (D8b round 2).  Reset in insert_ / erase_, moved
    /// with its key by erase_'s backward shift — like heldPass_.
    struct Edges {
        uint64_t progressPass = 0;   ///< pass of the last Held / Progress report (rate limit)
        uint8_t  reportedHeld = 0;   ///< what the runtime was told: Held
        uint8_t  dirty        = 0;   ///< listed in dirty_
        uint8_t  lost         = 0;   ///< the last non-fatal claim result was NOT_CONNECTED
        uint8_t  reportedLost = 0;   ///< what the runtime was told: Lost
        uint8_t  progressed   = 0;   ///< acks claimed while reportedHeld, Progress not yet reported
        uint8_t  creditListed = 0;   ///< D8c: listed in creditDirty_
        uint32_t credits      = 0;   ///< D8c: claimed gateway-routed acks not yet reported (<= kAckCapacity per pass)
        uint16_t direct       = 0;   ///< D8d: counted direct produces (admitted, ack not yet out) — <= kAckCapacity / 2
        uint16_t directTag    = 0;   ///< D8d: congestion episode in which one of its direct acks was claimed
        uint8_t  dflags       = 0;   ///< D8d: kDf* — delivered / chosen for expiry / read since chosen
    };
    static constexpr uint8_t kDfDelivered       = 1;   ///< a claim on its publication succeeded once (LOW-2)
    static constexpr uint8_t kDfExpiring        = 2;   ///< chosen for expiry; the runtime's close is awaited
    static constexpr uint8_t kDfReadSinceChosen = 4;   ///< claimed an ack after it was chosen (LOW-1: cancel)
    static_assert(std::has_single_bit(kClosedTombstones));
    static constexpr std::size_t kTombSlots = 2 * kClosedTombstones;   // load factor <= 1/2
    static constexpr std::size_t kTombMask  = kTombSlots - 1;
    static constexpr int         kTombBits  = std::countr_zero(kTombSlots);
    std::array<Edges, kMaxSessions>                                  edges_{};
    std::array<int32_t, kMaxSessions>                                clientVersion_{}; // D6: per session
    std::array<std::array<uint8_t, 16>, kMaxSessions>                bindToken_{};     // I2: the token a bound session proved (repeat binds compare against it); moved like boundImage_
    std::array<int64_t, kMaxSessions>                                boundImage_{};    // Task 6: imageKey_ of a bound response-mode session's ingress image (0 = none); moved like clientVersion_
    std::array<WriteGrantSlot, kMaxSessions>                         grants_{};        // authentication Task 13: moved with its key like clientVersion_
    std::unique_ptr<WritePermTable[]> permTables_ = std::make_unique<WritePermTable[]>(kMaxWritePermTables);   // #259: stable addresses
    std::array<int64_t, kMaxSessions>                                dirty_{};         // edges to report
    std::size_t                                                      dirtyCount_ = 0;
    std::array<int64_t, kMaxSessions>                                creditDirty_{};   // D8c: credits to report
    std::size_t                                                      creditDirtyCount_ = 0;
    std::array<int64_t, kMaxSessions>                                heldNow_{};       // held this flush
    std::array<int64_t, kMaxSessions>                                heldPrev_{};      // held last flush
    std::size_t                                                      heldNowCount_ = 0, heldPrevCount_ = 0;
    std::array<int64_t, kMaxSessions>                                draining_{};      // finding 8
    std::size_t                                                      drainCount_ = 0;
    uint64_t                                                         drainOverflow_ = 0;
    uint64_t                                                         pubAddRetries_ = 0;   ///< A10: transient re-adds
    std::size_t                                                      count_ = 0;
    std::array<int64_t, kMaxSessions>                                unresolved_{};
    std::size_t                                                      unresolvedCount_ = 0;
    std::array<std::array<char, kMaxChannelBytes>, kMaxChannelSlots> chanPool_{};
    std::array<uint16_t, kMaxChannelSlots>                           freeChan_{};
    std::size_t                                                      freeChanCount_ = 0;
    std::array<AckEntry, kAckCapacity>                               acks_{};
    std::size_t                                                      ackCount_ = 0;
    // Task 23: fixed scratch for the ENCRYPTED ack-seal path (flushAcks_'s
    // seal branch). Unlike Records (RecordsSealScratch, a GROW-ONCE vector —
    // batches are unbounded), a ProduceResponse has a compile-time-bounded
    // max size (kMaxEncodedSize: the 54 B client-17 frame plus a 4096-entry verdict trailer), so a fixed
    // std::array is strictly simpler than grow-once here and still Rule-4
    // zero-allocation (no vector, no resize, ever). One instance per sink
    // (one per worker — shared-nothing); reused for every sealed ack, one at
    // a time (the seal result is consumed synchronously before the next ack
    // is processed, never held across iterations).
    std::array<uint8_t, rune::protocol::client::ProduceResponse::kMaxEncodedSize>                  ackPlainScratch_{};
    std::array<uint8_t, rune::protocol::client::ProduceResponse::kMaxEncodedSize +
                             rune::protocol::client::kCryptoOverhead>                               ackSealedScratch_{};
    std::size_t                                                      heldAfterFlush_ = 0;
    std::size_t                                                      unreported_ = 0;
    uint64_t                                                         pass_    = 0;
    uint64_t                                                         dropped_ = 0;
    [[no_unique_address]] flight_recorder::Throttle                  frClaimBlocked_{};   // FR only
    // D8d — direct budget (rich state: the congestion episode holds its tag + pacing count).
    struct DirectEpisode { uint16_t tag = 0; uint32_t deferredPasses = 0; };   ///< tag 0 = no episode
    std::size_t                                                      directInFlight_ = 0;
    uint32_t directCap_           = static_cast<uint32_t>(kAckCapacity);        // Worker sets kDirectAckBudget
    uint32_t directBandStart_     = static_cast<uint32_t>(kAckCapacity - kAckCapacity / 4);
    uint32_t directSessionBudget_ = kDefaultDirectSessionBudget;
    DirectEpisode                                                    directEp_{};
    uint16_t                                                         directEpTags_ = 0;
    uint8_t                                                          directDeferredThisPass_ = 0;
    std::size_t                                                      expiringCount_ = 0;
    int64_t                                                          expireUnreported_ = 0;
    int64_t                                                          lastDirectExpired_ = 0;
    uint64_t                                                         directExpiries_ = 0;
    uint64_t directRefusedHeld_ = 0, directRefusedBudget_ = 0, directRefusedBand_ = 0, directRefusedFull_ = 0;
    uint64_t                                                         directUnderflows_ = 0;
    uint64_t                                                         directDroppedClosed_ = 0;
    uint64_t                                                         directDroppedSpoofed_ = 0;
    uint64_t                                                         directExpiryCancels_ = 0;
    std::array<int64_t, kClosedTombstones>                           closed_{};        // MEDIUM-2 tombstones (FIFO)
    std::size_t                                                      tombHead_ = 0, tombCount_ = 0;
    std::array<int64_t, kTombSlots>                                  tombSet_{};       // the same ids, O(1) lookup
};

/// Production policy: the worker's own invoker-mode Aeron client (spec §3.1).
struct AeronSessionPubPolicy {
    using PubHandle = std::shared_ptr<aeron::ExclusivePublication>;
    aeron::Aeron* client = nullptr;
    int32_t controlTermBytes = 0;   ///< per-publication term for the ack stream (0 = driver default)

    int64_t add(std::string_view channel, int32_t streamId) noexcept {
        // A10: sparse terms — created on a worker thread with io_uring work pending (see withSparseTerms).
        try {
            return client->addExclusivePublication(
                rune::node::withSparseTerms(rune::node::withTermLength(channel, controlTermBytes)), streamId);
        }
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
        int64_t pos = pub->tryClaim(static_cast<aeron::util::index_t>(len), c);
        if (pos == aeron::ADMIN_ACTION) [[unlikely]]   // A9: term rotated — retry now (Aeron contract)
            pos = pub->tryClaim(static_cast<aeron::util::index_t>(len), c);
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
static_assert(SessionAckSinkT<AeronSessionPubPolicy>::kClaimNotConnected == aeron::NOT_CONNECTED,
              "the policy passes NOT_CONNECTED through unchanged");

using SessionAckSink = SessionAckSinkT<AeronSessionPubPolicy>;

} // namespace rune::node
