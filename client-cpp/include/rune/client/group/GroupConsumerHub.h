// include/rune/client/group/GroupConsumerHub.h
//
// CG-5 — GroupConsumerHub: manages all registered GroupConsumerImpl instances.
//
// F8a — shared-nothing: the hub (its consumer list and every duty-side
// GroupConsumerImpl field) is OWNED by the duty thread.  No COW snapshot, no
// mutex, no atomics: app threads reach it only through HubCommands drained from
// the agent command ring (applyCommand), and learn "finished" only through each
// impl's status ring (closed when the consumer is closed or the Client closes).
//
// Duty-cycle entry points (duty thread):
//   applyCommand(cmd)               — GroupAdd / GroupCommit / GroupCommitSync / GroupClose
//   doWork(nowNs, sessionId, sendFn) — heartbeat + autocommit + rejoin timers
//   onRecords(rv)                   — route Records frame to assigned consumer
//   onJoinGroupResponse(...)        — state machine: Joining → Syncing
//   onSyncGroupResponse(...)        — state machine: Syncing → Stable
//   onHeartbeatResponse(...)        — state machine: Stable → Rejoining (on error)
//   onOffsetCommitResponse(...)     — complete the pending commitSync (F8c C4)
//   onAssign(...)                   — inline assignment update (Assign frame 44)
//   closeAll()                      — deactivate all consumers on Client::close()

#pragma once

#include <rune/client/group/GroupConsumer.h>
#include <rune/client/codec/DataCodec.h>
#include <rune/client/detail/HubCommand.h>

#include <rune/protocol/client/GroupMessages.h>
#include <rune/protocol/client/DataMessages.h>
#include <rune/protocol/client/OffsetMessages.h>

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <string_view>
#include <vector>

namespace rune::client::group {

// The duty-thread-owned consumer list.
using ImplList = std::vector<GroupConsumerImpl*>;  // F8c C7: not owning (see owned_)

// SendFn: (ingressStreamId, buf, len) -> bool.
// ingressStreamId=0 means "use the default ingress stream".
// Matches ClientAgent::SendFn so Task 1.3 can route group frames to the
// coordinator session without an adapter at every call site.
using SendFn = std::function<bool(int32_t, const uint8_t*, std::size_t)>;

class GroupConsumerHub {
public:
    /// #438: a commitSync answered UnstableOffsetCommit is re-sent after this backoff (doubling, capped).
    static constexpr int64_t  kCommitRetryMinNs = 20'000'000;
    static constexpr int64_t  kCommitRetryMaxNs = 1'000'000'000;
    GroupConsumerHub() = default;

    // Non-copyable / non-movable.
    GroupConsumerHub(const GroupConsumerHub&) = delete;
    GroupConsumerHub& operator=(const GroupConsumerHub&) = delete;
    GroupConsumerHub(GroupConsumerHub&&) = delete;
    GroupConsumerHub& operator=(GroupConsumerHub&&) = delete;

    // ---- Registration + commands (duty thread) --------------------------------

    /// Register a GroupConsumerImpl (duty thread; Client reaches it via
    /// HubCommand{GroupAdd}).  Cold path.
    void add(GroupConsumerImpl* impl);

    /// F8c C7: the handle released `impl` (its HandleRelease was applied and
    /// the consumer closed): once nothing references it any more (no callback
    /// of it running, no LeaveGroup being retried, no dirty snapshot), forget
    /// it and return true — the caller detaches the duty side's share (and
    /// frees the impl if the handle already detached).  Call only between
    /// duty cycles' hub walks (never from inside a hub iteration).
    [[nodiscard]] bool release(GroupConsumerImpl* impl) noexcept;

    /// Client close (after closeAll): every impl this hub still owns is
    /// handed back through `detach(impl)`; the hub holds nothing afterwards.
    template <class Detach>
    void handBackAll(Detach&& detach) noexcept {
        std::vector<GroupConsumerImpl*> owned;
        owned.swap(owned_);
        impls_.clear();
        for (GroupConsumerImpl* impl : owned) detach(impl);
    }

    /// Finish `impl` locally (inactive, a pending commitSync Closed, status
    /// closed — deferred past its running callback) with no LeaveGroup sent.
    void finishLocal(GroupConsumerImpl& impl) { finish_(impl); }

    /// F8c C7 F1: visit every impl this hub owns (cold: an orphan scan).
    template <class F>
    void forEachOwned(F&& f) {
        for (std::size_t i = 0; i < owned_.size(); ++i) f(*owned_[i]);
    }

    /// Impls this hub owns (adopted, not yet released and forgotten).
    [[nodiscard]] std::size_t ownedCount() const noexcept { return owned_.size(); }

    /// Apply one group HubCommand drained from the agent ring.  Non-group ops
    /// are ignored.  A command naming an impl this hub does not hold is
    /// ignored (a commitSync it carries completes Closed, never dropped).
    /// `nowNs`: the duty clock (a commitSync's deadline starts here).
    void applyCommand(const detail::HubCommand& cmd, int64_t nowNs = 0);

    /// fix3 I3: the same command applied DIRECTLY on the duty thread by a
    /// caller whose handle holds the impl (cmd.handle is dereferenced): acts on
    /// the impl even before its GroupAdd (still in the ring) is adopted.
    void applyCommandDirect(const detail::HubCommand& cmd, int64_t nowNs = 0);

    /// F8c C4: where commitSync completions go (the Client's registry).  Set
    /// before start; without one a handler target is called directly.
    void setCompletionSink(detail::CompletionSink sink) noexcept { completions_ = sink; }

    /// F8c C4: complete every commitSync whose deadline (duty clock) passed
    /// with OpStatus::Timeout.  Returns how many.
    int sweepCommitSyncTimeouts(int64_t nowNs);

    /// fix3 M4: LeaveGroup frames the transport refused (GroupConsumer close),
    /// retried every duty cycle; dropped on a session change.
    [[nodiscard]] std::size_t pendingLeaves() const noexcept { return pendingLeaves_; }
    int retryPendingLeaves(int64_t currentSessionId);

    // ---- Agent-thread duty cycle --------------------------------------------

    /// Drive heartbeat, autocommit, and rejoin timers for all consumers.
    /// Also drains any pending commit() requests.
    /// Returns work count (frames sent).
    int doWork(int64_t nowNs,
               int64_t sessionId,
               const SendFn& sendFn);

    // ---- Egress frame routing (agent thread) --------------------------------

    /// Route a Records(20) frame: if (topicId, partition) is in any consumer's
    /// current assignment, deliver to that consumer and advance its position.
    /// Also tries the existing subscriberHub path first (via the caller).
    /// C1: a routed partition accepts a batch only from its CURRENT
    /// subscription instance of the current generation: the broker stamps every
    /// push with the correlationId of the Subscribe that created it, so the
    /// batch must carry this instance's correlationId and broker session id
    /// (its first batch binds the subscriptionId if the SubscribeResponse has
    /// not arrived; from then on the id must match too).  No other local
    /// subscription — another consumer's, a plain one, a retired one — can
    /// ever match.  Positions are seeded only by OffsetFetch / the reset
    /// policy.  A batch entirely below the position is a duplicate (skipped);
    /// otherwise the position advances to the batch's last offset + 1 — gaps
    /// are legitimate (transaction markers, aborted batches, compaction holes).
    void onRecords(const codec::RecordsView& rv);

    /// Phase 2c: a broker position note (an empty Records frame, baseOffset = how far a FILTERED stream has
    /// scanned) for a filtered group's partition: the position follows it forward (the commit then moves past
    /// the filtered-out span).  Same instance checks as onRecords; unfiltered groups ignore notes.  No allocation.
    void onPositionNote(const codec::RecordsView& rv);

    /// Called when JoinGroupResponse (tid 33) arrives.
    void onJoinGroupResponse(const uint8_t* buf, std::size_t len,
                             int64_t sessionId, const SendFn& sendFn);

    /// Called when SyncGroupResponse (tid 35) arrives.
    void onSyncGroupResponse(const uint8_t* buf, std::size_t len,
                             int64_t sessionId, const SendFn& sendFn);

    /// Called when HeartbeatResponse (tid 37) arrives.
    void onHeartbeatResponse(const uint8_t* buf, std::size_t len,
                             int64_t sessionId, const SendFn& sendFn);

    /// Called when OffsetCommitResponse (tid 41) arrives.
    void onOffsetCommitResponse(const uint8_t* buf, std::size_t len);

    /// Called when OffsetFetchResponse (tid 43) arrives.
    void onOffsetFetchResponse(const uint8_t* buf, std::size_t len);

    /// Called when Assign (tid 44) arrives for a specific groupId member.
    void onAssign(const uint8_t* buf, std::size_t len,
                  int64_t sessionId, const SendFn& sendFn);

    // ---- Called on connect (runner thread) ----------------------------------

    /// Trigger all Unjoined / Rejoining consumers to send JoinGroup now
    /// (called when a new ConnectResponse{OK} arrives).
    void onConnected(int64_t sessionId, const SendFn& sendFn);
    /// D4: the coordinator is on another session now (the Client's coordinator
    /// route moved): every joined / joining member re-joins there (at once when
    /// it is connected, else on its connect).
    void onCoordinatorMoved();
    /// #290 (PR #513 review): the coordinator node's session was Rejected (its
    /// user revoked there, its login refused): no member can join or commit
    /// there, so every active one is refused with that code at once
    /// (46 SessionRevoked, 45 NotAuthorized, else RefusedByBroker) — never left
    /// cycling seed -> NotCoordinator -> that node until a budget runs out.
    void onCoordinatorRejected(int16_t code);

    // ---- Shutdown -----------------------------------------------------------

    /// Deactivate all consumers (send LeaveGroup where possible + fire onRevoked).
    void closeAll(int64_t sessionId, const SendFn& sendFn);

    /// F8a fix2: duty-thread direct send on the default ingress publication for
    /// OffsetCommit frames (plain fn pointer + ctx, set once before start).
    using DirectSendFn = detail::SendResult (*)(void* ctx, const uint8_t* buf, std::size_t len);
    void setDirectSend(DirectSendFn fn, void* ctx) noexcept { directSend_ = fn; directSendCtx_ = ctx; }

    /// F8a fix2: send every requested commit now (duty cycle; a refused offer
    /// keeps commitRequested set and is retried next cycle).  Requires the
    /// direct send to be wired.
    int flushRequestedCommits(int64_t sessionId);

    /// F8a fix2 (IM-2): the session went down (Connected -> anything else):
    /// complete every pending commitSync SessionLost now — progress-driven.
    void onSessionDown();

    /// F8a fix3 I2 (level-triggered session state, duty thread).  While not
    /// connected: a commitSync applied is failed at once, and commits are not
    /// offered.  Default true (hub-level tests have no session).
    /// fix4 M5: called AT THE TRANSITION (the Client's ConnectResponse /
    /// ErrorResponse handlers, before applying any hub command, and once per
    /// duty cycle).  Leaving a connected (sessionId, episode) — going down, or
    /// replaced by a new session / connect episode without ever being seen
    /// down — fails every pending commitSync with SessionLostError.
    void setSessionConnected(bool connected, int64_t sessionId = 0, int64_t episode = 0) noexcept;
    [[nodiscard]] bool sessionConnected() const noexcept { return sessionConnected_; }
    [[nodiscard]] bool hasPendingCommitSync() const noexcept;

    /// fix1 I4: consumers whose generation snapshot a near-full status ring
    /// refused; the duty cycle calls republishDirty() while non-zero.
    [[nodiscard]] std::size_t dirtyCount() const noexcept { return dirtyCount_; }
    int republishDirty();

    /// True if there are no registered consumers (duty thread).
    [[nodiscard]] bool empty() const noexcept { return impls_.empty(); }

    // ---- Fetch routing (distributed partition leadership) --------------------
    //
    // A broker answers a Subscribe for a partition it does not lead with
    // ErrorResponse{Redirect | NotLeader} and registers nothing.  So each
    // assigned partition's Subscribe goes to the partition's LEADER session
    // (stamped with that session's id), and converges on Redirect / NotLeader /
    // a leader change / a session change: reconcileFetch() compares every
    // assigned partition's subscription with where its leader is now and moves
    // it (Unsubscribe on the old node, Subscribe on the new one from the
    // member's position).  The group protocol itself (Join / Sync / Heartbeat /
    // OffsetCommit / OffsetFetch / LeaveGroup) stays on the seed session = the
    // coordinator's.  Records arrive on the shared egress and are matched by
    // (topicId, partition) in onRecords(), whichever node pushed them.
    //
    // Without a router (hub-level tests) the historical single-session path is
    // used: every Subscribe goes through the SendFn with correlationId 0.

    /// How a Subscribe reaches a partition's leader (plain function pointers +
    /// context, wired once before start by the Client to its ClientAgent).
    struct FetchRouter {
        void* ctx = nullptr;
        /// Session index leading (topicId, partition) now (0 = the seed); may
        /// lazily open that node's session.
        uint32_t (*leaderSession)(void* ctx, int32_t topicId, int32_t partition) = nullptr;
        /// That session's broker session id while Connected, else 0 (stamping).
        int64_t (*sessionId)(void* ctx, uint32_t session) = nullptr;
        /// That session's connect epoch while Connected, else 0 (liveness:
        /// MEDIUM-1 — a reconnect that got the same broker id is a new epoch).
        uint64_t (*epoch)(void* ctx, uint32_t session) = nullptr;
        /// Offer a control frame on that session's node ingress.
        detail::SendResult (*offer)(void* ctx, uint32_t session, const uint8_t* buf, std::size_t len) = nullptr;
        /// MEDIUM-1: re-drive the connect of a node session that is terminally
        /// Disconnected (optional; no-op otherwise).
        void (*revive)(void* ctx, uint32_t session) = nullptr;
        /// A-MEDIUM-3: that session's node broker version (optional; nullptr =
        /// current).  A partition led by a node below
        /// kBrokerVersionExplicitGroupSubscriptions is parked, logged.
        int32_t (*brokerVersion)(void* ctx, uint32_t session) = nullptr;
        /// #290: that node session's reject code (46 / 44 ...) when it is
        /// Rejected, else 0 (optional).  A fetch it would carry can never be
        /// sent: the consumer is refused with that code at once.
        int16_t (*rejectCode)(void* ctx, uint32_t session) = nullptr;
    };
    void setFetchRouter(const FetchRouter& r) noexcept { router_ = r; }

    /// Group fetch Subscribes / Unsubscribes carry correlationIds tagged with
    /// bit 61 (bit 62 clear, positive) — disjoint from producer batch ids (bit
    /// 62), connect ids (session * 1e12), keepalive ids (negative) and the
    /// Client's counter.
    static constexpr int64_t kFetchCorrTag = int64_t{1} << 61;

    static constexpr int64_t  kOffsetFetchCorrTag      = int64_t{1} << 60;
    [[nodiscard]] static constexpr bool isFetchCorrId(int64_t c) noexcept {
        return c > 0 && (c & (int64_t{3} << 61)) == kFetchCorrTag;
    }
    /// Consecutive Redirects of one partition (no bind in between) after which
    /// it is parked until fresh metadata (no ping-pong storm).
    static constexpr uint8_t kMaxFetchRedirects = 8;
    /// PR #441 F4: a partition fetch answered RecordEgressUnavailable (the broker cannot build this session's record
    /// stream) this many times since its last bind ends the consumer explicitly (RetryBudgetExhausted).  Only that
    /// cause counts: NotLeader / Redirect (a failover, however long) re-route without a bound, as before.
    static constexpr uint8_t kMaxEgressRefusalsWithoutBind = 8;

    /// Bring every Stable consumer's subscriptions in line with the current
    /// leaders and sessions (cold: group / routing events).  Also re-offers
    /// refused Unsubscribes.  Returns the Subscribe frames sent.
    int reconcileFetch();
    /// MEDIUM-2 (round 3/4): re-send the still-unanswered fetch Subscribes (no
    /// SubscribeResponse, no push yet) sent on `session` — called on that
    /// session's events, never on a timer.  Returns the Subscribe frames sent.
    int resendUnanswered(uint32_t session);
    /// Fresh metadata: refused / parked partitions may retry.
    void unparkFetch() noexcept;
    /// A partition was parked (refused, gap loop): fresh metadata is wanted.
    /// Taken (and cleared) by the caller, which arms a metadata refresh.
    [[nodiscard]] bool takeMetadataWanted() noexcept {
        const bool w = metadataWanted_;
        metadataWanted_ = false;
        return w;
    }
    /// A SubscribeResponse whose correlationId isFetchCorrId(): binds it (an
    /// error parks the partition).  False when not a group fetch Subscribe.
    bool onFetchSubscribeResponse(int64_t corrId, int16_t errorCode, int64_t subscriptionId);
    /// An ErrorResponse{Redirect | NotLeader} to a group fetch Subscribe: the
    /// partition is unsubscribed locally (parked when `park` or after
    /// kMaxFetchRedirects).  True + its (topicId, partition) when it is a live
    /// one — the caller re-routes it and calls reconcileFetch().
    bool onFetchSubscribeRefused(int64_t corrId, bool park, int32_t& topicId, int32_t& partition);
    /// Authentication (Task 7): an ErrorResponse{NotAuthorized | SessionRevoked | ...} on a group fetch
    /// Subscribe's correlationId (unanswered, or a BOUND instance — a revoked READ): its consumer is finished
    /// with the matching GroupFailReason, never re-armed.  False when not a live group fetch.
    bool onFetchAuthRefused(int64_t corrId, int16_t errorCode);
    /// Phase 2c: the broker's InvalidRequest on a FILTERED group's fetch Subscribe (unanswered, or its bound
    /// instance: the stream it can no longer serve): the consumer is finished RefusedByBroker, never re-armed.
    /// False when not a live filtered group fetch.
    bool onFetchInvalidRequest(int64_t corrId);
    /// Authentication (D26): the session was revoked — every active consumer is finished SessionRevoked.
    int revokeAll();
    /// M2: Unsubscribes the transport refused, re-offered by the duty cycle
    /// (one branch when zero) until sent or moot (their session's epoch ended).
    [[nodiscard]] std::size_t pendingFetchUnsubscribes() const noexcept { return pendingUnsubs_.size(); }
    int retryFetchUnsubscribes();
    /// MEDIUM-2: re-send OffsetFetch for every Stable consumer with an assigned
    /// partition whose start offset is still unknown (refused send, errored
    /// entry, capped answer).  Paced by a doubling backoff on the duty clock.
    int retryOffsetFetch(int64_t nowNs, int64_t sessionId, const SendFn& sendFn);
    /// MAJOR-2 (round 3): the seed broker's ConnectResponse.brokerVersion.  A
    /// GroupConsumer runs only against >= kBrokerVersionExplicitGroupSubscriptions;
    /// otherwise onConnected / joinIfConnected finish it (GroupFailReason::
    /// BrokerTooOld).  Hub-level tests (no broker) keep the default (current).
    void setBrokerVersion(int32_t v) noexcept { brokerVersion_ = v; }

    /// MEDIUM-7: a consumer adopted while the session is already Connected joins
    /// now (onConnected already ran).  No-op otherwise.
    void joinIfConnected(int64_t sessionId, const SendFn& sendFn);
    /// M1: does any Stable consumer's partition subscribe through `session`?
    [[nodiscard]] bool fetchUses(uint32_t session) const noexcept;
    /// MEDIUM-1 (re-review): an assigned partition waits for a leader session
    /// that is not Connected (the caller keeps a metadata refresh armed).
    [[nodiscard]] bool fetchWaitsForSession() const noexcept;

    /// Test/observer: Subscribes abandoned before their answer, awaiting it.
    [[nodiscard]] std::size_t lateFetchUnsubscribes() const noexcept { return late_.size(); }
    /// Test/observer: batches dropped by the identity / contiguity rules (C1).
    [[nodiscard]] uint64_t droppedRecordBatches() const noexcept { return droppedBatches_; }
    /// 8-byte notes (lineage / catalog epoch) that reached the group path: ignored.
    [[nodiscard]] uint64_t notesIgnored() const noexcept { return notesIgnored_; }
    /// Task 13 fix round 3 (M2-r2): frames with a CRC-bad / torn batch (delivery and
    /// the position stopped at the valid prefix).
    [[nodiscard]] uint64_t malformedBatches() const noexcept { return malformedBatches_; }

    /// fix1 C1: onAssigned/onRevoked/onRecords run inside the noexcept
    /// ClientAgent::doWork chain (or, for closeAll's final onRevoked, inside
    /// Client::closeImpl_); a throwing callback is caught, counted here, and
    /// logged instead of terminating the process. PLAIN counter (fix2
    /// minor-5 — shared-nothing rule, no atomics): written only on the
    /// runner/duty-cycle thread, or from closeAll()'s thread strictly after
    /// the runner is joined. See Client::testCallbackExceptionCount()'s doc
    /// for the exact read-side contract this implies.
    [[nodiscard]] uint64_t testCallbackExceptionCount() const noexcept {
        return callbackExceptions_;
    }

private:
    // ---- Registered consumers (duty-thread owned) ------------------------------
    // impls_: the consumers the duty cycle drives (active at adoption).  Index
    // loops capture the size first (a handler may adopt one); an entry is
    // removed only by release(), which never runs inside a hub walk.
    ImplList impls_;
    // F8c C7: every impl whose GroupAdd this hub adopted (also one closed
    // before its adoption), until release() forgets it or Client close hands
    // it back.
    std::vector<GroupConsumerImpl*> owned_;

    // fix1 C1 / fix2 minor-5: plain counter (see testCallbackExceptionCount()).
    uint64_t callbackExceptions_ = 0;

    // F8a fix1 C1: the consumer whose callback is running (null otherwise).
    GroupConsumerImpl* dispatching_ = nullptr;
    // F8a fix1 I6: correlationIds for commitSync's OffsetCommit (0 = untracked).
    int64_t nextCommitCorrId_ = 1;
    // F8a fix1 I4.
    std::size_t dirtyCount_ = 0;
    // F8a fix3 I2 / M4.
    bool sessionConnected_ = true;
    int64_t currentSessionId_ = 0;
    int64_t sessionEpisode_   = 0;
    std::size_t pendingLeaves_ = 0;
    void applyTo_(const detail::HubCommand& cmd, GroupConsumerImpl* impl, int64_t nowNs);
    static std::size_t encodeLeave_(GroupConsumerImpl& impl, int64_t sessionId);
    void sendLeave_(GroupConsumerImpl& impl);
    // F8a fix2.
    DirectSendFn directSend_    = nullptr;
    void*        directSendCtx_ = nullptr;

    int32_t brokerVersion_ = rune::protocol::client::kBrokerVersion;
    void refuse_(GroupConsumerImpl& impl, GroupFailReason why);
    bool filterNeedsRouting_(GroupConsumerImpl& impl);   // Phase 2c: refuses (FilterMismatch) a filtered group on a non-routed hub

    // Fetch routing (see FetchRouter).
    FetchRouter router_{};
    int64_t     nextFetchCorr_ = 1;
    bool        metadataWanted_ = false;
    uint64_t    droppedBatches_ = 0;
    uint64_t    notesIgnored_   = 0;
    uint64_t    malformedBatches_ = 0;
    void noteMalformed_() noexcept;
    struct LateSub {  // a Subscribe dropped before its answer
        int64_t        corrId;
        uint32_t       session;
        uint64_t       epoch;
        TopicPartition tp;
        int64_t        brokerSid;  // LOW-2 (round 4): a push reveals its id only on that session
    };
    std::vector<LateSub> late_;
    struct OldNodeLog { uint32_t session; uint64_t epoch; };
    std::vector<OldNodeLog> oldNodeLogged_;  // R4-MEDIUM-1: one log per pre-v3 node episode
    struct SubRef {   // a subscription instance
        uint32_t       session;
        uint64_t       epoch;
        int64_t        subscriptionId;
        TopicPartition tp;
    };
    std::vector<SubRef> pendingUnsubs_;  // M2: refused Unsubscribes
    std::vector<SubRef> unsubScratch_;   // LOW-2: double buffer (no per-pass allocation)
    [[nodiscard]] bool routed_() const noexcept { return router_.offer != nullptr; }
    [[nodiscard]] int64_t nextFetchCorr_tagged_() noexcept {
        return kFetchCorrTag | (nextFetchCorr_++ & (kFetchCorrTag - 1));
    }
    /// An abandoned (unanswered) instance revealed its id by a Records frame:
    /// unsubscribe it.  Cold (only for batches no consumer accepts).
    void learnLateFromRecords_(const codec::RecordsView& rv);
    void pruneFetch_();
    int  reconcileImpl_(GroupConsumerImpl& impl);
    /// Forget fs's subscription: its instance is retired (its records dropped)
    /// and unsubscribed on its node (an unanswered one once its answer arrives).
    void dropFetch_(const TopicPartition& tp, GroupConsumerImpl::FetchSub& fs);
    void dropAllFetch_(GroupConsumerImpl& impl);
    void unsubscribe_(uint32_t session, uint64_t epoch, int64_t subscriptionId, const TopicPartition& tp);
    /// True when the transport took it (phase Sent); false = refused / nothing to fetch (phase Idle).
    bool sendOffsetFetchFor_(GroupConsumerImpl& impl, int64_t sessionId, const SendFn& sendFn, bool unknownOnly);

    /// RAII: marks impl as "callback running"; on exit, if the callback
    /// finished the consumer, closes its status ring (now that no callback runs).
    struct DispatchScope {
        GroupConsumerHub&  hub;
        GroupConsumerImpl& impl;
        GroupConsumerImpl* prev;
        DispatchScope(GroupConsumerHub& h, GroupConsumerImpl& i) noexcept;
        ~DispatchScope();
        DispatchScope(const DispatchScope&) = delete;
        DispatchScope& operator=(const DispatchScope&) = delete;
    };

    enum class CommitSend : uint8_t { Nothing, Sent, SendFailed, Fatal };

    /// F8c C4: complete impl's pending commitSync (if any) exactly once.
    void completeCommitSync_(GroupConsumerImpl& impl, OpStatus st, uint16_t errorCode = 0);
    /// Task 10: complete impl's pending rewind (if any) exactly once (cleared BEFORE delivery).
    void completeRewind_(GroupConsumerImpl& impl, OpStatus st, std::string_view text = {});
    /// Task 10: apply GroupSeekToCommitted / GroupSeek (duty thread).
    void applyRewind_(const detail::HubCommand& cmd, GroupConsumerImpl* impl, int64_t nowNs);
    /// Task 10: move a pending rewind forward (AwaitFetch -> Resubscribe -> Ok) or end it
    /// (Rejoined: the generation changed / the group left Stable; Timeout).  Once per duty cycle.
    void advanceRewind_(GroupConsumerImpl& impl, int64_t nowNs);
    /// #494: a rewind that has STARTED (OffsetFetch / resubscribe sent) and so dies with its session; a parked
    /// RewindAwaitSession (issued while the session was down) is not in flight.
    [[nodiscard]] static bool rewindInFlight_(const GroupConsumerImpl& impl) noexcept;
    /// The checks a rewind passes before starting (at issue, and again when a parked one is applied).
    [[nodiscard]] bool rewindAdmissible_(const GroupConsumerImpl& impl, bool isSeek, const TopicPartition& seekTp,
                                         OpStatus& st, std::string_view& text) const;
    /// Start an admitted rewind: supersede the pending one, restart the partitions, arm AwaitFetch / Resubscribe.
    void startRewind_(GroupConsumerImpl* impl, OpKind kind, const detail::OpTarget& target, Token token,
                      const TopicPartition& seekTp, int64_t seekOffset, int64_t deadlineNs);
    detail::CompletionSink completions_{};
    int drainCommit_(GroupConsumerImpl& impl, int64_t sessionId, const SendFn& sendFn);
    static void closeStatus_(GroupConsumerImpl& impl);
    void toRejoining_(GroupConsumerImpl& impl);
    /// No-hang fix round 2 (C1): an error that is neither a known rebalance
    /// signal nor non-retriable (Internal, NotLeader, unknown codes): rejoin
    /// after an exponential backoff (20 ms doubling, cap 1 s); after
    /// kMaxBackoffRejoins consecutive such failures the consumer is refused.
    void rejoinWithBackoff_(GroupConsumerImpl& impl);
    static constexpr uint32_t kMaxBackoffRejoins       = 8;
    /// I2: consecutive OffsetFetch answers with a retriable per-entry error
    /// before the consumer is refused (each re-send is paced by retryOffsetFetch).
    static constexpr uint32_t kMaxOffsetFetchAttempts  = 8;

    /// Fix round 4 (item 2): an OffsetFetch answered NotCoordinator (or another
    /// rebalance-set code) re-joins (paced, 20 ms doubling to 1 s); after
    /// kMaxOffsetFetchRejoins consecutive such answers (no fetch served in
    /// between) the consumer is refused RetryBudgetExhausted.
    void rejoinForOffsetFetch_(GroupConsumerImpl& impl);
    static constexpr uint32_t kMaxOffsetFetchRejoins   = 8;
    /// I2: OffsetFetch correlationIds (bit 60 tag; disjoint from fetch ids).
    int64_t nextOffsetFetchCorr_ = 1;
    void publishGeneration_(GroupConsumerImpl& impl);
    /// The status snapshot of `impl` (generation, `reserved`, member id: #125).
    static GroupConsumerStatus statusOf_(const GroupConsumerImpl& impl, int32_t reserved) noexcept;

    // ---- Internal helpers (runner thread only) ------------------------------

    /// Send JoinGroup for impl; transitions to Joining.
    static void sendJoinGroup(GroupConsumerImpl& impl,
                              int64_t sessionId,
                              const SendFn& sendFn);

    /// Send SyncGroup for impl; transitions to Syncing.
    static void sendSyncGroup(GroupConsumerImpl& impl,
                              int64_t sessionId,
                              const SendFn& sendFn);

    /// Send Heartbeat for impl.
    static void sendHeartbeat(GroupConsumerImpl& impl,
                              int64_t sessionId,
                              const SendFn& sendFn);

    /// Send OffsetCommit for impl's current positions (skips unseeded sentinel
    /// entries) carrying `correlationId` (0 = no commitSync waits on it).
    CommitSend sendOffsetCommit(GroupConsumerImpl& impl,
                                       int64_t sessionId,
                                       const SendFn& sendFn,
                                       int64_t correlationId);

    /// Send OffsetFetch for all currently-assigned partitions to seed positions
    /// from the broker's committed offsets.  Called after entering Stable.
    void sendOffsetFetch(GroupConsumerImpl& impl,
                                int64_t sessionId,
                                const SendFn& sendFn);

    /// TX-5 (fix-round 1, CRITICAL 2): Send a Subscribe frame for each assigned
    /// partition, carrying cfg_.isolation so the broker applies the correct
    /// read isolation level when delivering Records to this group consumer.
    /// Called after entering Stable (SyncGroupResponse) and after Assign.
    static void sendSubscribeForPartitions(GroupConsumerImpl& impl,
                                           int64_t sessionId,
                                           const SendFn& sendFn);

    /// The registered impl whose address is `handle`, or nullptr.
    GroupConsumerImpl* find_(uint64_t handle) const noexcept;

    /// Deactivate impl: inactive, complete a pending commitSync Closed, close
    /// its status ring (deferred to the end of its running callback, if any).
    void finish_(GroupConsumerImpl& impl);

    /// Apply a new assignment to impl: fire revoked+assigned callbacks. Not
    /// static (fix1 C1): catches/counts a throwing callback via callbackExceptions_.
    void applyAssignment(GroupConsumerImpl& impl,
                         std::vector<TopicPartition> newAssignment);
};

} // namespace rune::client::group
