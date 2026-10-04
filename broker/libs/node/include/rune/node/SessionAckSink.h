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
#include <rune/node/AeronChannel.h>
#include <rune/node/SessionSignalKind.h>
#include <rune/protocol/client/DataMessages.h>

#include <Aeron.h>
#include <ExclusivePublication.h>
#include <concurrent/logbuffer/BufferClaim.h>

#include <algorithm>
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
    /// <= ~200 ms (~100 ms at ~10k passes/s) — orders of magnitude inside the
    /// runtime's held bound (10 min), at <= ~10 signals/s per held session.
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
    /// Review MEDIUM-2: recently closed session ids remembered (FIFO ring) —
    /// sized for every close burst between a close and the drain of the closed
    /// client's image (a client's term window drains in a few passes).
    static constexpr std::size_t kClosedTombstones = 1024;
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
    struct PubResolving { int64_t regId = -1; };
    struct PubReady     { PubHandle pub{}; uint64_t blockedPass = 0; };
    /// reported == false: the runtime has not yet been told (reportFailures).
    struct PubFailed    { bool reported = true; };
    using PubState = std::variant<PubAnnounced, PubResolving, PubReady, PubFailed>;

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
    bool announce(int64_t sessionId, std::string_view channel, int32_t streamId,
                  int32_t clientVersion = 0) noexcept {   // D6 review: default = oldest (never 28 by omission)
        if (sessionId <= 0 || channel.size() > kMaxChannelBytes) return false;
        std::size_t idx = find_(sessionId);
        if (idx == kNone) idx = insert_(sessionId);
        if (idx == kNone) return false;                                   // table full
        clientVersion_[idx] = clientVersion;
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

    /// Runtime close (Disconnect / reap / connect timeout / SessionFailed):
    /// forget the session; its queued acks drop at the next flush (once the
    /// lifecycle ring is consumed past them); its publication is released.
    void close(int64_t sessionId) noexcept {
        const std::size_t idx = find_(sessionId);
        if (idx == kNone) return;
        erase_(idx);
        closed_[closedNext_++ & (kClosedTombstones - 1)] = sessionId;   // MEDIUM-2 tombstone (overwrites the oldest)
    }

    /// Queue one ack for this pass's flush.  False only when the queue is full
    /// (the worker's intake gate makes that an invariant violation).
    bool enqueue(const AckEntry& a) noexcept {
        if (ackCount_ == kAckCapacity) [[unlikely]] return false;
        acks_[ackCount_++] = a;
        return true;
    }

    /// One pass: advance announcements, then claim every queued ack on its
    /// session publication in order.  `ringProducerPos` / `ringConsumerPos`:
    /// the worker's session-lifecycle ring, sampled now (after every frame of
    /// this pass was observed) — they decide when an unknown session's ack is
    /// truly orphaned (I1).  Returns work done.
    int flush(int64_t ringProducerPos, int64_t ringConsumerPos) noexcept {
        const int work = flushAcks_(ringProducerPos, ringConsumerPos);
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
    int flushAcks_(int64_t ringProducerPos, int64_t ringConsumerPos) noexcept {
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
                    const int64_t pos = policy_.claim(r->pub, rune::protocol::client::ProduceResponse::encodedSizeFor(ver),
                                                      [&](uint8_t* dst) { encode_(a, ver, dst); });
                    if (pos > 0) [[likely]] {
                        if (const Edges& e = edges_[idx];
                            ((e.lost | e.reportedHeld) != 0) | ((e.dflags & (kDfDelivered | kDfExpiring)) != kDfDelivered))
                            [[unlikely]] claimedWatched_(idx);
                        if (a.credit) noteCredit_(idx);              // D8c: gateway-routed — return its credit
                        else if (a.direct) directDone_(a.direct, idx, true);   // D8d: its direct share
                        ++work;
                        continue;
                    }
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
    [[nodiscard]] uint8_t admitDirect(int64_t sessionId) noexcept {
        const std::size_t idx = find_(sessionId);
        uint32_t mine = 0;
        if (idx != kNone) [[likely]] {
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
    [[nodiscard]] uint64_t    drainOverflowForTest() const noexcept { return drainOverflow_; }
    [[nodiscard]] std::size_t freeAckSlots() const noexcept { return kAckCapacity - ackCount_; }
    [[nodiscard]] std::size_t pending() const noexcept { return ackCount_; }
    /// Acks still queued after the last flush (held for their sessions).
    [[nodiscard]] std::size_t heldAcks() const noexcept { return heldAfterFlush_; }
    [[nodiscard]] std::size_t sessionCount() const noexcept { return count_; }
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
        states_[i] = PubFailed{};                                    // neutral (reported) placeholder
        heldPass_[i] = 0; edges_[i] = Edges{}; clientVersion_[i] = 0;
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
        heldPass_[i] = 0; edges_[i] = Edges{}; clientVersion_[i] = 0;
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
            keys_[j] = 0;
            states_[j] = PubFailed{};
            heldPass_[j] = 0; edges_[j] = Edges{}; clientVersion_[j] = 0;
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
        else if (const auto* r = std::get_if<PubResolving>(&st)) {
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
            if (idx != kNone && heldPass_[idx] != pass_) markDirty_(idx);   // released
        }
        for (std::size_t k = 0; k < heldNowCount_; ++k) {
            const std::size_t idx = find_(heldNow_[k]);
            if (idx != kNone && edges_[idx].reportedHeld == 0) markDirty_(idx);   // newly held
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
    /// MEDIUM-2: was `sid` closed here recently?  Cold: consulted only for a
    /// frame whose session is not in the table.
    [[nodiscard]] bool closedRecently_(int64_t sid) const noexcept {
        const std::size_t n = closedNext_ < kClosedTombstones ? closedNext_ : kClosedTombstones;
        for (std::size_t k = 0; k < n; ++k) if (closed_[k] == sid) return true;
        return false;
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
                const std::string_view ch(chanPool_[a->chan].data(), a->chanLen);
                const int32_t stream = a->streamId;
                leaveState_(st);                                     // slot freed; ch still readable
                addNow_(st, ch, stream);
                ++work;
                if (std::holds_alternative<PubResolving>(st)) unresolved_[keep++] = sid;
                continue;
            }
            if (auto* r = std::get_if<PubResolving>(&st)) {
                PubHandle pub{};
                const PubFind f = policy_.find(r->regId, pub);
                if (f == PubFind::Ready) { st = PubReady{std::move(pub), 0}; ++work; continue; }
                if (f == PubFind::Error) {
                    // The client conductor erased the registration before
                    // reporting the error (later finds of it say "not yet"
                    // forever): nothing to release — NOT via leaveState_'s
                    // drain push (fix round 3).
                    st = PubFailed{false};
                    ++unreported_;
                    ++work;
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
    std::array<Edges, kMaxSessions>                                  edges_{};
    std::array<int32_t, kMaxSessions>                                clientVersion_{}; // D6: per session
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
    std::size_t                                                      count_ = 0;
    std::array<int64_t, kMaxSessions>                                unresolved_{};
    std::size_t                                                      unresolvedCount_ = 0;
    std::array<std::array<char, kMaxChannelBytes>, kMaxChannelSlots> chanPool_{};
    std::array<uint16_t, kMaxChannelSlots>                           freeChan_{};
    std::size_t                                                      freeChanCount_ = 0;
    std::array<AckEntry, kAckCapacity>                               acks_{};
    std::size_t                                                      ackCount_ = 0;
    std::size_t                                                      heldAfterFlush_ = 0;
    std::size_t                                                      unreported_ = 0;
    uint64_t                                                         pass_    = 0;
    uint64_t                                                         dropped_ = 0;
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
    uint64_t                                                         directExpiryCancels_ = 0;
    std::array<int64_t, kClosedTombstones>                           closed_{};        // MEDIUM-2 tombstones
    std::size_t                                                      closedNext_ = 0;
};

/// Production policy: the worker's own invoker-mode Aeron client (spec §3.1).
struct AeronSessionPubPolicy {
    using PubHandle = std::shared_ptr<aeron::ExclusivePublication>;
    aeron::Aeron* client = nullptr;
    int32_t controlTermBytes = 0;   ///< per-publication term for the ack stream (0 = driver default)

    int64_t add(std::string_view channel, int32_t streamId) noexcept {
        try { return client->addExclusivePublication(rune::node::withTermLength(channel, controlTermBytes), streamId); }
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
