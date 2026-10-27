// src/group/GroupConsumerHub.cpp
//
// CG-5 — GroupConsumerHub: state machine + records routing (runner thread).
//
// State machine transitions:
//   Unjoined   → [on connect]       → Joining  (sendJoinGroup)
//   Joining    → [JoinGroupResponse OK]  → Syncing  (sendSyncGroup)
//   Joining    → [JoinGroupResponse err] → Rejoining
//   Syncing    → [SyncGroupResponse OK]  → Stable   (fire onAssigned; arm timers)
//   Syncing    → [SyncGroupResponse err] → Rejoining
//   Stable     → [HeartbeatResponse OK]  → Stable   (update nextHeartbeat)
//   Stable     → [HeartbeatResponse RebalanceInProgress] → Rejoining
//   Stable     → [HeartbeatResponse NotCoordinator]      → Rejoining
//   Stable     → [Assign frame]          → Stable   (fire revoked+assigned)
//   Rejoining  → [doWork timer]    → Joining  (sendJoinGroup)
//
// Records routing (hot path):
//   onRecords(): for each registered consumer in Stable state,
//   check if (topicId, partition) is in its assignment map.
//   If found: fire cbRecords, advance position.
//
// F8a shared-nothing contract:
//   impls_ and every duty-side impl field are owned by the duty thread.  App
//   threads reach the hub only via applyCommand() (HubCommands drained from the
//   agent ring).  No locks, no atomics.
//   Re-entrancy (F8c fix round 1, I-3): user code runs inline inside the hub
//   loops (callbacks, and since C4 commitSync completion handlers), and may
//   adopt a consumer on this thread (impls_.push_back).  Every loop that can
//   run user code iterates by index over the size captured before it and
//   holds the impl by raw pointer (impls are never removed) — never a vector
//   iterator / element reference across user code.

#include <rune/client/group/GroupConsumerHub.h>
#include <rune/client/producer/TxnTypes.h>
#include <rune/client/codec/DataCodec.h>
#include <rune/protocol/client/GroupMessages.h>
#include <rune/protocol/client/DataMessages.h>
#include <rune/protocol/client/OffsetMessages.h>
#include <rune/protocol/client/Protocol.h>
#include <rune/protocol/RecordBatch.h>  // RecordBatchReader (recordCount)

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <exception>
#include <string>
#include <memory>
#include <stdexcept>

namespace rune::client::group {

namespace rpc = rune::protocol::client;
using TP      = rune::client::TopicPartition;

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

static int64_t nowNsToMs(int64_t nowNs) noexcept {
    return nowNs / 1'000'000LL;
}

static std::chrono::steady_clock::time_point tpNow() noexcept {
    return std::chrono::steady_clock::now();
}

// Build a byte span from a std::string (for encoding groupId / memberId).
static std::span<const uint8_t> strSpan(const std::string& s) noexcept {
    return {reinterpret_cast<const uint8_t*>(s.data()), s.size()};
}

// Ensure sendBuf is at least `needed` bytes.
static void ensureBuf(GroupConsumerImpl& impl, std::size_t needed) {
    if (impl.sendBuf.size() < needed) {
        impl.sendBuf.resize(needed + 256);
    }
}

// ---------------------------------------------------------------------------
// add() / applyCommand() — duty thread (cold path)
// ---------------------------------------------------------------------------

void GroupConsumerHub::add(GroupConsumerImpl* impl) {
    if (impl == nullptr) return;
    // F8c C7: the duty side's share is held from adoption on (even if closed).
    owned_.push_back(impl);
    // #431: a hub-unique 24-bit control id (never 0) tags this consumer's Join / Sync / Heartbeat correlation ids.
    impl->ctlId = (++nextCtlId_ & 0xFFFFFFu);
    if (impl->ctlId == 0) impl->ctlId = (++nextCtlId_ & 0xFFFFFFu);
    // fix3 I3: closed (directly, on this thread) before its adoption: honour
    // it — never driven, never joins.
    if (impl->active) impls_.push_back(impl);
}

GroupConsumerImpl* GroupConsumerHub::find_(uint64_t handle) const noexcept {
    for (GroupConsumerImpl* impl : impls_) {
        if (reinterpret_cast<uint64_t>(impl) == handle) return impl;
    }
    return nullptr;
}

bool GroupConsumerHub::release(GroupConsumerImpl* impl) noexcept {
    if (impl == dispatching_ || impl->active || impl->leavePending || impl->statusDirty) return false;
    for (std::size_t i = 0; i < impls_.size(); ++i) {
        if (impls_[i] == impl) {
            impls_.erase(impls_.begin() + static_cast<std::ptrdiff_t>(i));  // keeps the drive order
            break;
        }
    }
    for (std::size_t i = 0; i < owned_.size(); ++i) {
        if (owned_[i] == impl) {
            owned_[i] = owned_.back();
            owned_.pop_back();
            break;
        }
    }
    return true;
}

void GroupConsumerHub::completeCommitSync_(GroupConsumerImpl& impl, OpStatus st, uint16_t errorCode) {
    if (!impl.hasCommitSync) return;
    // Cleared BEFORE delivery: a handler may issue a new commitSync (applied
    // directly on this thread) — it must find no pending one to supersede.
    const GroupConsumerImpl::PendingCommitSync p = impl.commitSync;
    impl.hasCommitSync    = false;
    impl.commitSync       = {};
    impl.commitSyncCorrId = 0;
    Completion c;
    c.token     = p.token;
    c.op        = OpKind::CommitSync;
    c.status    = st;
    c.errorCode = errorCode;
    completions_.deliver(p.target, c);
}

void GroupConsumerHub::completeRewind_(GroupConsumerImpl& impl, OpStatus st, std::string_view text) {
    using R = GroupConsumerImpl;
    OpKind kind{};
    detail::OpTarget target{};
    Token token = 0;
    if (const auto* a = std::get_if<R::RewindAwaitFetch>(&impl.rewind)) { kind = a->kind; target = a->target; token = a->token; }
    else if (const auto* b = std::get_if<R::RewindResubscribe>(&impl.rewind)) { kind = b->kind; target = b->target; token = b->token; }
    else if (const auto* c = std::get_if<R::RewindAwaitSession>(&impl.rewind)) { kind = c->kind; target = c->target; token = c->token; }
    else return;
    // Cleared BEFORE delivery: a handler may issue a new rewind (applied directly on this
    // thread) — it must find none pending to supersede.
    impl.rewind = R::NoRewind{};
    for (auto& [tp, fs] : impl.fetch) fs.rewound = false;
    Completion c;
    c.token = token;
    c.op    = kind;
    c.status = st;
    completions_.deliver(target, c,
        std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(text.data()), text.size()));
}

/*static*/
bool GroupConsumerHub::rewindInFlight_(const GroupConsumerImpl& impl) noexcept {
    using R = GroupConsumerImpl;
    // A parked rewind (RewindAwaitSession) is waiting for a session: nothing of it was sent, so nothing is lost
    // when a session goes away.
    return !std::holds_alternative<R::NoRewind>(impl.rewind) && !std::holds_alternative<R::RewindAwaitSession>(impl.rewind);
}

// The one set of checks a rewind passes before it starts (at issue, and again when a parked rewind is applied).
// Returns false and fills `st` / `text` when it is refused.
bool GroupConsumerHub::rewindAdmissible_(const GroupConsumerImpl& impl, bool isSeek, const TopicPartition& seekTp,
                                         OpStatus& st, std::string_view& text) const {
    if (!impl.active) { st = OpStatus::Closed; return false; }
    if (isSeek) {
        if (impl.state != GroupConsumerState::Stable || impl.assignment.find(seekTp) == impl.assignment.end()) {
            st = OpStatus::InvalidArgument; text = "partition not assigned"; return false;
        }
    } else if (impl.state != GroupConsumerState::Stable) {
        // The (re)join re-seeds every position from the committed offsets anyway.
        st = OpStatus::Rejoined; return false;
    }
    if (!routed_()) { st = OpStatus::InvalidArgument; text = "fetch routing unavailable"; return false; }
    return true;
}

void GroupConsumerHub::advanceRewind_(GroupConsumerImpl& impl, int64_t nowNs) {
    using R = GroupConsumerImpl;
    if (std::holds_alternative<R::NoRewind>(impl.rewind)) return;
    if (const auto* parked = std::get_if<R::RewindAwaitSession>(&impl.rewind)) {
        // #494: issued while the coordinator session was down.  Never started, so the generation / Stable check of
        // the started states does not apply: it ends at its deadline, or is applied once a session is back.
        if (nowNs >= parked->deadlineNs) { completeRewind_(impl, OpStatus::Timeout); return; }
        if (!sessionConnected_) return;   // keep waiting
        const R::RewindAwaitSession p = *parked;
        impl.rewind = R::NoRewind{};
        const bool isSeek = p.kind == OpKind::Seek;
        OpStatus st{};
        std::string_view text;
        if (!rewindAdmissible_(impl, isSeek, p.seekTp, st, text)) {
            Completion c;
            c.token  = p.token;
            c.op     = p.kind;
            c.status = st;
            completions_.deliver(p.target, c,
                std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(text.data()), text.size()));
            return;
        }
        startRewind_(&impl, p.kind, p.target, p.token, p.seekTp, p.seekOffset, p.deadlineNs);
        return;
    }
    auto deadline = [&] {
        if (const auto* a = std::get_if<R::RewindAwaitFetch>(&impl.rewind)) return a->deadlineNs;
        return std::get<R::RewindResubscribe>(impl.rewind).deadlineNs;
    }();
    auto generation = [&] {
        if (const auto* a = std::get_if<R::RewindAwaitFetch>(&impl.rewind)) return a->generation;
        return std::get<R::RewindResubscribe>(impl.rewind).generation;
    }();
    if (impl.state != GroupConsumerState::Stable || impl.generationId != generation) {
        completeRewind_(impl, OpStatus::Rejoined);
        return;
    }
    if (nowNs >= deadline) {
        completeRewind_(impl, OpStatus::Timeout);
        return;
    }
    // A rewound partition is re-bound once its new subscription is answered / fed.
    bool anyUnknown = false, anyUnbound = false;
    for (auto& [tp, fs] : impl.fetch) {
        if (!fs.rewound) continue;
        if (!fs.offsetKnown) { anyUnknown = true; continue; }
        if (fs.epoch != 0 && fs.subscriptionId != 0) fs.rewound = false;
        else anyUnbound = true;
    }
    if (auto* a = std::get_if<R::RewindAwaitFetch>(&impl.rewind)) {
        if (anyUnknown) return;
        impl.rewind = R::RewindResubscribe{a->kind, a->target, a->token, a->deadlineNs, a->generation};
    }
    if (!anyUnknown && !anyUnbound) completeRewind_(impl, OpStatus::Ok);
}

void GroupConsumerHub::applyRewind_(const detail::HubCommand& cmd, GroupConsumerImpl* impl, int64_t nowNs) {
    using R = GroupConsumerImpl;
    const bool isSeek = cmd.op == detail::HubCommand::Op::GroupSeek;
    const OpKind kind = isSeek ? OpKind::Seek : OpKind::SeekToCommitted;
    const detail::OpTarget target = detail::decodeTarget(cmd);
    const Token token = static_cast<Token>(cmd.a);
    auto reply = [&](OpStatus st, std::string_view text = {}) {
        Completion c;
        c.token  = token;
        c.op     = kind;
        c.status = st;
        completions_.deliver(target, c,
            std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(text.data()), text.size()));
    };
    if (impl == nullptr || !impl->active) { reply(OpStatus::Closed); return; }
    TopicPartition seekTp{};
    // A negative offset is never a position (-1 is the unseeded sentinel: the partition would stall).
    if (isSeek && cmd.d < 0) { reply(OpStatus::InvalidArgument, "offset must be >= 0"); return; }
    if (isSeek) {
        seekTp = TopicPartition{static_cast<int32_t>(static_cast<uint64_t>(cmd.e) >> 32),
                                static_cast<int32_t>(static_cast<uint32_t>(cmd.e))};
    }
    OpStatus refusal{};
    std::string_view refusalText;
    if (!rewindAdmissible_(*impl, isSeek, seekTp, refusal, refusalText)) { reply(refusal, refusalText); return; }
    // The deadline is fixed ONCE, at issue, and carried through a wait for the session.
    const int64_t deadlineNs = nowNs + static_cast<int64_t>(impl->cfg.commitSyncTimeoutMs) * 1'000'000LL;
    if (!sessionConnected_) {   // #494: wait for the coordinator session, bounded by the op's own deadline
        // A rewind already pending is superseded (exactly once) and replaced.
        completeRewind_(*impl, OpStatus::Superseded);
        if (!impl->active) { reply(OpStatus::Closed); return; }   // its handler closed the consumer
        impl->rewind = R::RewindAwaitSession{kind, target, token, deadlineNs, seekTp, cmd.d};
        return;
    }
    startRewind_(impl, kind, target, token, seekTp, cmd.d, deadlineNs);
}

void GroupConsumerHub::startRewind_(GroupConsumerImpl* impl, OpKind kind, const detail::OpTarget& target, Token token,
                                    const TopicPartition& seekTp, int64_t seekOffset, int64_t deadlineNs) {
    using R = GroupConsumerImpl;
    const bool isSeek = kind == OpKind::Seek;
    auto reply = [&](OpStatus st) {
        Completion c;
        c.token  = token;
        c.op     = kind;
        c.status = st;
        completions_.deliver(target, c, {});
    };
    // A rewind already pending is superseded (exactly once) and replaced.
    completeRewind_(*impl, OpStatus::Superseded);
    if (!impl->active) { reply(OpStatus::Closed); return; }   // its handler closed the consumer

    auto restart = [&](const TopicPartition& tp, int64_t& pos, int64_t newPos, bool known) {
        GroupConsumerImpl::FetchSub& fs = impl->fetch[tp];
        dropFetch_(tp, fs);        // pushes of the old instance are ignored (instance identity)
        pos             = newPos;  // kUnseededSentinel: auto-commit can never commit a stale position
        fs.offsetKnown  = known;
        fs.rewound      = true;
        ++fs.rewindEpoch;          // a callback-applied rewind survives the post-callback advance
        fs.parked       = false;
        fs.redirects    = 0;
    };
    if (isSeek) {
        restart(seekTp, impl->assignment.find(seekTp)->second, seekOffset, /*known=*/true);
        impl->rewind = R::RewindResubscribe{kind, target, token, deadlineNs, impl->generationId};
    } else {
        if (impl->assignment.empty()) { reply(OpStatus::Ok); return; }
        for (auto& [tp, pos] : impl->assignment) restart(tp, pos, GroupConsumerImpl::kUnseededSentinel, /*known=*/false);
        impl->rewind = R::RewindAwaitFetch{kind, target, token, deadlineNs, impl->generationId};
    }
    // An OffsetFetch in flight may predate the rewind (its committed offsets could be older than
    // a commit already sent): its answer is ignored; the retry pass sends a fresh one for the
    // partitions whose start offset is unknown.
    impl->offsetFetchCorrId     = 0;
    impl->offsetFetchPhase      = GroupConsumerImpl::OffsetFetchPhase::Idle;
    impl->offsetFetchDueNs      = 0;
    impl->offsetFetchBackoffNs  = 0;
    if (isSeek) (void)reconcileImpl_(*impl);   // the start offset is known: resubscribe now
}

int GroupConsumerHub::sweepCommitSyncTimeouts(int64_t nowNs) {
    int n = 0;
    for (std::size_t ix = 0, nx = impls_.size(); ix < nx; ++ix) {  // re-entrancy safe (I-3)
        GroupConsumerImpl* const impl = impls_[ix];
        if (!std::holds_alternative<GroupConsumerImpl::NoRewind>(impl->rewind)) {
            advanceRewind_(*impl, nowNs);   // Task 10: bound / fetched / rejoined / timed out
            ++n;
        }
        if (impl->hasCommitSync && nowNs >= impl->commitSync.deadlineNs) {
            completeCommitSync_(*impl, OpStatus::Timeout);
            ++n;
        }
        if (impl->hasCommitSync && impl->commitSync.retryDueNs >= 0) {   // #438: an UnstableOffsetCommit retry
            GroupConsumerImpl::PendingCommitSync& p = impl->commitSync;
            if (p.retryDueNs == 0) {
                p.retryDueNs = nowNs + p.retryBackoffNs;
            } else if (nowNs >= p.retryDueNs) {
                p.retryDueNs = -1;
                impl->commitRequested = true;   // drainCommit_ re-sends it (a fresh correlationId)
                ++n;
            }
        }
    }
    return n;
}

void GroupConsumerHub::setSessionConnected(bool connected, int64_t sessionId, int64_t episode) noexcept {
    // fix4 M5: the transition is (connected, sessionId, connect episode), not
    // the connected bit alone — a reconnect inside one duty cycle (never
    // observed "down") still strands every commitSync of the old session.
    if (sessionConnected_ &&
        (!connected || sessionId != currentSessionId_ || episode != sessionEpisode_)) {
        // I-3: a completion handler may adopt a consumer (impls_ grows): index
        // loop over the size captured first (removal: only release(), never
        // inside a hub walk).
        for (std::size_t ix = 0, nx = impls_.size(); ix < nx; ++ix) {
            completeCommitSync_(*impls_[ix], OpStatus::SessionLost);
            if (rewindInFlight_(*impls_[ix])) completeRewind_(*impls_[ix], OpStatus::SessionLost);   // #494
        }
    }
    sessionConnected_ = connected;
    currentSessionId_ = sessionId;
    sessionEpisode_   = episode;
}

/*static*/
void GroupConsumerHub::closeStatus_(GroupConsumerImpl& impl) {
    if (impl.status.producerClosed()) return;
    // Final snapshot (the reserve guarantees it fits), THEN close.
    (void)impl.status.publishFinal(statusOf_(impl, static_cast<int32_t>(impl.failReason)));
    impl.status.close();
}

void GroupConsumerHub::finish_(GroupConsumerImpl& impl) {
    impl.active = false;
    completeCommitSync_(impl, OpStatus::Closed);
    completeRewind_(impl, OpStatus::Closed);
    if (impl.statusDirty) { impl.statusDirty = false; --dirtyCount_; }
    // A callback of this consumer is running (it closed itself): the ring is
    // closed when the callback returns (DispatchScope), so finished() never
    // turns true while a callback still executes.
    if (&impl == dispatching_) return;
    closeStatus_(impl);
}

namespace {
// No-hang fix round 2 (C1): the coordinator's error codes, classified
// EXPLICITLY (the old "else" branch rejoined at once on anything — a tight
// JoinGroup <-> Internal storm — and a Heartbeat error outside the rebalance
// set left the member Stable while the broker had evicted it).
enum class GroupVerdict : uint8_t { Ok, Rejoin, Backoff, Refuse };
GroupVerdict classifyGroupAnswer_(rpc::ErrorCode ec) noexcept {
    switch (ec) {
    case rpc::ErrorCode::OK:                  return GroupVerdict::Ok;
    case rpc::ErrorCode::NotCoordinator:
    case rpc::ErrorCode::RebalanceInProgress:
    case rpc::ErrorCode::UnknownMember:
    case rpc::ErrorCode::IllegalGeneration:
    case rpc::ErrorCode::StaleGeneration:     return GroupVerdict::Rejoin;    // the rebalance protocol
    case rpc::ErrorCode::ProtocolVersion:
    case rpc::ErrorCode::InvalidRequest:
    // Authentication (client version 9): a grant / a login is an admin's act, not a transient condition —
    // re-joining cannot change the answer (it used to fall into Backoff and loop until the retry budget).
    case rpc::ErrorCode::AuthenticationFailed:
    case rpc::ErrorCode::NotAuthorized:
    case rpc::ErrorCode::SessionRevoked:
    case rpc::ErrorCode::PasswordChangeRequired: return GroupVerdict::Refuse;    // re-sending fails the same way
    default:                                  return GroupVerdict::Backoff;   // Internal, NotLeader, unknown, ...
    }
}
/// Why a Refuse verdict ended the consumer.
GroupFailReason refusalFor_(rpc::ErrorCode ec) noexcept {
    switch (ec) {
    case rpc::ErrorCode::NotAuthorized:  return GroupFailReason::NotAuthorized;
    case rpc::ErrorCode::SessionRevoked: return GroupFailReason::SessionRevoked;
    default:                             return GroupFailReason::RefusedByBroker;
    }
}
} // namespace

void GroupConsumerHub::rejoinWithBackoff_(GroupConsumerImpl& impl) {
    // Fix round 3: only a Heartbeat OK (a proven-healthy Stable period) resets
    // this budget — a coordinator answering every Heartbeat Internal can no
    // longer cycle Stable → rejoin → Stable forever.
    if (++impl.backoffRejoins > kMaxBackoffRejoins) {
        refuse_(impl, GroupFailReason::RetryBudgetExhausted);
        return;
    }
    // 20 ms doubling, capped at 1 s: the whole budget spends ~3.3 s.
    const auto shift = std::min<uint32_t>(impl.backoffRejoins - 1, 6u);
    const auto delay = std::chrono::milliseconds(std::min<int64_t>(int64_t{20} << shift, 1000));
    impl.rejoinNotBefore = tpNow() + delay;
    toRejoining_(impl);
}

void GroupConsumerHub::rejoinForOffsetFetch_(GroupConsumerImpl& impl) {
    // Fix round 4 (item 2): an OffsetFetch answered NotCoordinator (or another
    // rebalance-set code) means THIS node no longer serves the group — re-join
    // (the Join path rediscovers the coordinator: NotCoordinator → D4 redirect).
    // Not charged to the Internal backoff budget (a failover is not an error),
    // but capped on its own: kMaxOffsetFetchRejoins consecutive such answers
    // with no fetch served in between end in an explicit RetryBudgetExhausted
    // — a node that keeps answering Heartbeat OK but OffsetFetch NotCoordinator
    // can no longer keep the member Stable-and-unsubscribed forever.
    if (++impl.offsetFetchRejoins > kMaxOffsetFetchRejoins) {
        refuse_(impl, GroupFailReason::RetryBudgetExhausted);
        return;
    }
    // 20 ms doubling, capped at 1 s (as rejoinWithBackoff_).
    const auto shift = std::min<uint32_t>(impl.offsetFetchRejoins - 1, 6u);
    impl.rejoinNotBefore = tpNow() + std::chrono::milliseconds(std::min<int64_t>(int64_t{20} << shift, 1000));
    toRejoining_(impl);
}

void GroupConsumerHub::toRejoining_(GroupConsumerImpl& impl) {
    // C1: the old generation's subscriptions are dead at once — their records
    // are dropped (retired) and ours unsubscribed.
    if (routed_()) dropAllFetch_(impl);
    impl.state = GroupConsumerState::Rejoining;
    completeCommitSync_(impl, OpStatus::Rejoined);
    completeRewind_(impl, OpStatus::Rejoined);
}

/*static*/
GroupConsumerStatus GroupConsumerHub::statusOf_(const GroupConsumerImpl& impl, int32_t reserved) noexcept {
    GroupConsumerStatus st;
    st.generationId = impl.generationId;
    st.reserved     = reserved;
    // #125: a member id longer than a snapshot carries is published empty (groupMetadata() then names no member and
    // the coordinator answers UnknownMember) - never truncated into another id.
    if (impl.memberId.size() <= kMaxGroupMemberIdBytes) {
        st.memberIdLen = static_cast<uint16_t>(impl.memberId.size());
        std::memcpy(st.memberId, impl.memberId.data(), impl.memberId.size());
    }
    return st;
}

void GroupConsumerHub::publishGeneration_(GroupConsumerImpl& impl) {
    if (impl.status.tryPublish(statusOf_(impl, 0))) {
        if (impl.statusDirty) { impl.statusDirty = false; --dirtyCount_; }
    } else if (!impl.status.closed() && !impl.statusDirty) {
        impl.statusDirty = true;
        ++dirtyCount_;
    }
}

int GroupConsumerHub::republishDirty() {
    int n = 0;
    for (std::size_t ix = 0, nx = impls_.size(); ix < nx; ++ix) {  // re-entrancy safe (I-3)
        GroupConsumerImpl* const impl = impls_[ix];
        if (!impl->statusDirty) continue;
        const std::size_t before = dirtyCount_;
        publishGeneration_(*impl);
        if (dirtyCount_ < before) ++n;
        else if (impl->status.closed()) { impl->statusDirty = false; --dirtyCount_; }
    }
    return n;
}

GroupConsumerHub::DispatchScope::DispatchScope(GroupConsumerHub& h, GroupConsumerImpl& i) noexcept
    : hub(h), impl(i), prev(h.dispatching_) {
    hub.dispatching_ = &impl;
}

GroupConsumerHub::DispatchScope::~DispatchScope() {
    hub.dispatching_ = prev;
    if (!impl.active && prev != &impl) closeStatus_(impl);
}

void GroupConsumerHub::applyCommand(const detail::HubCommand& cmd, int64_t nowNs) {
    // Ring path: commands are FIFO behind their GroupAdd — resolve the
    // identity against the hub's own list (never dereference an unknown one).
    applyTo_(cmd, cmd.op == detail::HubCommand::Op::GroupAdd ? nullptr : find_(cmd.handle), nowNs);
}

void GroupConsumerHub::applyCommandDirect(const detail::HubCommand& cmd, int64_t nowNs) {
    // fix3 I3: direct path (duty thread, the caller's handle keeps the impl
    // alive): act on the IMPL itself, even if its GroupAdd is still queued in
    // the ring — add() honours what was done (closed => never added).
    applyTo_(cmd, cmd.op == detail::HubCommand::Op::GroupAdd
                      ? nullptr : reinterpret_cast<GroupConsumerImpl*>(cmd.handle), nowNs);
}

void GroupConsumerHub::applyTo_(const detail::HubCommand& cmd, GroupConsumerImpl* impl, int64_t nowNs) {
    using Op = detail::HubCommand::Op;
    switch (cmd.op) {
    case Op::GroupAdd: {
        // F8c C7: handle = the impl itself (no shared_ptr holder): adopted.
        add(reinterpret_cast<GroupConsumerImpl*>(cmd.handle));
        break;
    }
    case Op::GroupCommit: {
        if (impl && impl->active) impl->commitRequested = true;
        break;
    }
    case Op::GroupCommitSync: {
        // F8c C4: an accepted commitSync completes exactly once.
        GroupConsumerImpl::PendingCommitSync p;
        p.target = detail::decodeTarget(cmd);
        p.token  = static_cast<Token>(cmd.a);
        if (!impl || !impl->active) {
            Completion c;
            c.token  = p.token;
            c.op     = OpKind::CommitSync;
            c.status = OpStatus::Closed;
            completions_.deliver(p.target, c);
            break;
        }
        // Already one in flight — it is superseded (exactly once) and replaced.
        completeCommitSync_(*impl, OpStatus::Superseded);
        p.deadlineNs          = nowNs + static_cast<int64_t>(impl->cfg.commitSyncTimeoutMs) * 1'000'000LL;
        impl->commitSync      = p;
        impl->hasCommitSync   = true;
        impl->commitSyncCorrId = 0;
        impl->commitRequested = true;  // send the commit promptly
        // fix3 I2 (level-triggered): applied while the session is down — it can
        // never be acknowledged; complete it now.
        if (!sessionConnected_) completeCommitSync_(*impl, OpStatus::SessionLost);
        break;
    }
    case Op::GroupSeekToCommitted:
    case Op::GroupSeek:
        applyRewind_(cmd, impl, nowNs);   // Task 10: a token op, completed exactly once
        break;
    case Op::GroupClose: {
        if (impl && impl->active) {
            if (routed_()) dropAllFetch_(*impl);  // its leader-node subscriptions go too
            finish_(*impl);
            sendLeave_(*impl);  // fix3 M4: direct, retried per cycle
        }
        break;
    }
    case Op::GroupSetCallback: {
        const auto raw = static_cast<uintptr_t>(cmd.b);
        if (cmd.a == 2) {
            std::unique_ptr<GroupConsumer::RecordsCallback> fn(
                reinterpret_cast<GroupConsumer::RecordsCallback*>(raw));
            if (impl && fn) impl->cbRecords = std::move(*fn);
        } else {
            std::unique_ptr<GroupConsumer::AssignmentCallback> fn(
                reinterpret_cast<GroupConsumer::AssignmentCallback*>(raw));
            if (impl && fn) (cmd.a == 0 ? impl->cbAssigned : impl->cbRevoked) = std::move(*fn);
        }
        break;
    }
    default:
        break;  // not a group op
    }
}

std::size_t GroupConsumerHub::encodeLeave_(GroupConsumerImpl& impl, int64_t sessionId) {
    rpc::LeaveGroup msg;
    msg.sessionId     = sessionId;
    msg.correlationId = 0;
    msg.groupId       = strSpan(impl.groupId);
    msg.memberId      = strSpan(impl.memberId);
    ensureBuf(impl, msg.encodedSize());
    return msg.encode(impl.sendBuf.data());
}

void GroupConsumerHub::sendLeave_(GroupConsumerImpl& impl) {
    // A member that never joined has nothing to leave; while the session is
    // down the broker expires the member with the session.
    if (impl.memberId.empty() || !sessionConnected_ || directSend_ == nullptr) return;
    impl.leaveSessionId = currentSessionId_;
    const std::size_t len = encodeLeave_(impl, impl.leaveSessionId);
    if (len == 0) return;
    // fix4 M1: only a transient refusal is retried; Fatal = never deliverable,
    // dropped (the broker expires the member with its session).
    if (directSend_(directSendCtx_, impl.sendBuf.data(), len) == detail::SendResult::Retry &&
        !impl.leavePending) {
        impl.leavePending = true;
        ++pendingLeaves_;
    }
}

int GroupConsumerHub::retryPendingLeaves(int64_t currentSessionId) {
    int sent = 0;
    for (std::size_t ix = 0, nx = impls_.size(); ix < nx; ++ix) {  // re-entrancy safe (I-3)
        GroupConsumerImpl* const impl = impls_[ix];
        if (!impl->leavePending) continue;
        bool done = true;
        if (impl->leaveSessionId != currentSessionId) {
            // The session is gone: the broker expires the member with it.
        } else if (!sessionConnected_ || directSend_ == nullptr) {
            done = false;
        } else {
            const std::size_t len = encodeLeave_(*impl, impl->leaveSessionId);
            const detail::SendResult r = len == 0 ? detail::SendResult::Accepted
                                                  : directSend_(directSendCtx_, impl->sendBuf.data(), len);
            done = r != detail::SendResult::Retry;  // Fatal (fix4 M1): dropped
            if (r == detail::SendResult::Accepted) ++sent;
        }
        if (done) { impl->leavePending = false; --pendingLeaves_; }
    }
    return sent;
}

static_assert(GroupConsumerHub::kGroupCtlCorrTag == producer::coordcorr::kGroupCtlTag);

// #431: tag | ctlId << 32 | seq.  The sequence skips 0 on wrap.
/*static*/
int64_t GroupConsumerHub::nextCtlCorr_(GroupConsumerImpl& impl) noexcept
{
    if (++impl.ctlSeq == 0) impl.ctlSeq = 1;
    return kGroupCtlCorrTag | (static_cast<int64_t>(impl.ctlId) << 32) | static_cast<int64_t>(impl.ctlSeq);
}

// ---------------------------------------------------------------------------
// sendJoinGroup — runner thread
// ---------------------------------------------------------------------------

/*static*/
void GroupConsumerHub::sendJoinGroup(GroupConsumerImpl& impl,
                                     int64_t sessionId,
                                     const SendFn& sendFn)
{
    const auto grpSpan = strSpan(impl.groupId);
    const auto mbrSpan = strSpan(impl.memberId);  // empty = new member

    rpc::JoinGroup msg;
    msg.sessionId        = sessionId;
    msg.correlationId    = impl.ctlCorrId = nextCtlCorr_(impl);   // #431: the answer echoes it
    msg.sessionTimeoutMs = impl.cfg.sessionTimeoutMs;
    msg.groupId          = grpSpan;
    msg.memberId         = mbrSpan;
    msg.filter           = impl.cfg.filterBytes.span();   // Phase 2c: empty = no trailer, the frame is byte-identical to version 9

    ensureBuf(impl, msg.encodedSize(impl.topicIds.size()));   // reads msg.filter itself

    const std::size_t len = msg.encode(impl.sendBuf.data(),
                                        std::span<const int32_t>(impl.topicIds));
    if (len == 0) return;

    impl.joinSessionId = sendFn(0, impl.sendBuf.data(), len) ? sessionId : 0;  // D4

    impl.state = GroupConsumerState::Joining;
    impl.joinDeadline = tpNow() + std::chrono::milliseconds(impl.cfg.sessionTimeoutMs);
}

// ---------------------------------------------------------------------------
// sendSyncGroup — runner thread
// ---------------------------------------------------------------------------

/*static*/
void GroupConsumerHub::sendSyncGroup(GroupConsumerImpl& impl,
                                      int64_t sessionId,
                                      const SendFn& sendFn)
{
    const auto grpSpan = strSpan(impl.groupId);
    const auto mbrSpan = strSpan(impl.memberId);

    const std::size_t needed = rpc::SyncGroup{}.encodedSize()
        + grpSpan.size() + mbrSpan.size();  // rough upper bound

    // SyncGroup::encodedSize computes exact size but we need the object set up.
    rpc::SyncGroup msg;
    msg.sessionId     = sessionId;
    msg.correlationId = impl.ctlCorrId = nextCtlCorr_(impl);   // #431
    msg.generationId  = impl.generationId;
    msg.groupId       = grpSpan;
    msg.memberId      = mbrSpan;

    const std::size_t exact = msg.encodedSize();
    ensureBuf(impl, exact);

    const std::size_t len = msg.encode(impl.sendBuf.data());
    if (len == 0) return;

    impl.joinSessionId = sendFn(0, impl.sendBuf.data(), len) ? sessionId : 0;  // D4

    impl.state = GroupConsumerState::Syncing;
}

// ---------------------------------------------------------------------------
// sendHeartbeat — runner thread
// ---------------------------------------------------------------------------

/*static*/
void GroupConsumerHub::sendHeartbeat(GroupConsumerImpl& impl,
                                      int64_t sessionId,
                                      const SendFn& sendFn)
{
    const auto grpSpan = strSpan(impl.groupId);
    const auto mbrSpan = strSpan(impl.memberId);

    rpc::Heartbeat msg;
    msg.sessionId     = sessionId;
    msg.correlationId = nextCtlCorr_(impl);   // #431
    msg.generationId  = impl.generationId;
    msg.groupId       = grpSpan;
    msg.memberId      = mbrSpan;

    const std::size_t exact = msg.encodedSize();
    ensureBuf(impl, exact);

    const std::size_t len = msg.encode(impl.sendBuf.data());
    if (len == 0) return;

    sendFn(0, impl.sendBuf.data(), len);
}

// ---------------------------------------------------------------------------
// sendOffsetCommit — runner thread
// ---------------------------------------------------------------------------

GroupConsumerHub::CommitSend GroupConsumerHub::sendOffsetCommit(GroupConsumerImpl& impl,
                                                                int64_t sessionId,
                                                                const SendFn& sendFn,
                                                                int64_t correlationId)
{
    if (impl.assignment.empty()) return CommitSend::Nothing;

    // Reuse the scratch vector to avoid per-commit heap allocation (#5).
    // Skip any partition whose position is still the unseeded sentinel (-1):
    // committing a sentinel would overwrite the broker's committed offset
    // with 0 (or a garbage value), breaking at-least-once guarantees (#1b).
    impl.commitEntriesScratch.clear();
    for (const auto& [tp, pos] : impl.assignment) {
        if (pos == GroupConsumerImpl::kUnseededSentinel) {
            continue;  // not yet seeded from OffsetFetch — never commit
        }
        rpc::OffsetCommitEntry e;
        e.topicId   = tp.topicId;
        e.partition = tp.partition;
        e.offset    = pos;  // next offset to consume = committed position
        // metadata: empty (zero-copy empty span — default constructed)
        impl.commitEntriesScratch.push_back(e);
    }

    if (impl.commitEntriesScratch.empty()) return CommitSend::Nothing;  // all still unseeded

    const auto grpSpan = strSpan(impl.groupId);
    const auto mbrSpan = strSpan(impl.memberId);
    const std::size_t needed = rpc::OffsetCommit::encodedSizeFor(
        grpSpan, mbrSpan, impl.commitEntriesScratch);
    ensureBuf(impl, needed);

    rpc::OffsetCommit msg;
    msg.sessionId     = sessionId;
    msg.correlationId = correlationId;  // echoed by the broker (commitSync matching)
    msg.generationId  = impl.generationId;
    msg.groupId       = grpSpan;
    msg.memberId      = mbrSpan;

    const std::size_t len = msg.encode(impl.sendBuf.data(), impl.commitEntriesScratch);
    if (len == 0) return CommitSend::Nothing;

    // F8a fix2: straight to the transport from the duty thread when wired (the
    // caller keeps commitRequested set and retries every duty cycle on refusal).
    if (directSend_ == nullptr) {
        return sendFn(0, impl.sendBuf.data(), len) ? CommitSend::Sent : CommitSend::SendFailed;
    }
    switch (directSend_(directSendCtx_, impl.sendBuf.data(), len)) {
    case detail::SendResult::Accepted: return CommitSend::Sent;
    case detail::SendResult::Retry:    return CommitSend::SendFailed;
    case detail::SendResult::Fatal:    break;
    }
    return CommitSend::Fatal;  // fix4 M1: never deliverable
}

// ---------------------------------------------------------------------------
// sendOffsetFetch — runner thread
// Send OffsetFetch for all assigned partitions to seed positions from the
// broker's committed offsets.  Called after entering Stable (onSyncGroupResponse)
// and after applyAssignment adds new partitions.
// ---------------------------------------------------------------------------

void GroupConsumerHub::sendOffsetFetch(GroupConsumerImpl& impl,
                                        int64_t sessionId,
                                        const SendFn& sendFn)
{
    (void)sendOffsetFetchFor_(impl, sessionId, sendFn, /*unknownOnly=*/false);
}

bool GroupConsumerHub::sendOffsetFetchFor_(GroupConsumerImpl& impl, int64_t sessionId,
                                           const SendFn& sendFn, bool unknownOnly)
{
    // unknownOnly (MEDIUM-2 retry): only the partitions whose start offset is
    // unknown; otherwise the whole assignment.  Encoded from a scratch whose
    // capacity persists (zero steady-state allocation).
    auto& entries = impl.fetchEntriesScratch;
    entries.clear();
    for (const auto& [tp, _] : impl.assignment) {
        if (unknownOnly) {
            const auto f = impl.fetch.find(tp);
            if (f != impl.fetch.end() && f->second.offsetKnown) continue;
        }
        rpc::OffsetFetchEntry e;
        e.topicId   = tp.topicId;
        e.partition = tp.partition;
        entries.push_back(e);
    }
    if (entries.empty()) return false;
    const auto grpSpan = strSpan(impl.groupId);
    ensureBuf(impl, rpc::OffsetFetch::encodedSizeFor(grpSpan, entries.size()));
    rpc::OffsetFetch msg;
    msg.sessionId     = sessionId;
    msg.correlationId = kOffsetFetchCorrTag | nextOffsetFetchCorr_++;   // I2: only ITS answer seeds
    msg.groupId       = grpSpan;
    impl.offsetFetchCorrId = msg.correlationId;
    const std::size_t len = msg.encode(impl.sendBuf.data(), entries);
    const bool sent = len > 0 && sendFn && sendFn(0, impl.sendBuf.data(), len);
    // Refused: Idle (the next retry pass re-offers it, paced).  Sent: awaiting
    // its answer — the retry pass never duplicates it while it is in flight.
    impl.offsetFetchPhase = sent ? GroupConsumerImpl::OffsetFetchPhase::Sent
                                 : GroupConsumerImpl::OffsetFetchPhase::Idle;
    return sent;
}

// ---------------------------------------------------------------------------
// sendSubscribeForPartitions — runner thread (TX-5 CRITICAL 2)
//
// After entering Stable state, send a Subscribe frame for each assigned
// partition so the broker knows the isolation level to apply when delivering
// Records.  Uses fromOffset=0 (the broker ignores fromOffset for group
// consumers; position seeding happens via OffsetFetch) and partitionKnown=1.
//
// Isolation wire value: static_cast<uint8_t>(impl.cfg.isolation).
// ---------------------------------------------------------------------------

/*static*/
void GroupConsumerHub::sendSubscribeForPartitions(GroupConsumerImpl& impl,
                                                   int64_t sessionId,
                                                   const SendFn& sendFn)
{
    if (impl.assignment.empty()) return;
    // Phase 2c: this path reaches the broker's group CARRIER Subscribe, which refuses a filter, and it cannot
    // apply one.  A filtered group is refused before it joins when the hub is not routed (filterNeedsRouting_);
    // never fall through to an unfiltered fetch.
    if (!impl.cfg.filterBytes.empty()) return;

    const uint8_t isolation = static_cast<uint8_t>(impl.cfg.isolation);

    for (const auto& [tp, _] : impl.assignment) {
        // Each Subscribe frame is 44 bytes (no key — partitionKnown=1).
        constexpr std::size_t kSubscribeFrameSize = 44u;
        ensureBuf(impl, kSubscribeFrameSize);

        const std::size_t len = codec::encodeSubscribe(
            impl.sendBuf.data(),
            impl.sendBuf.size(),
            tp.topicId,
            tp.partition,
            /*partitionKnown=*/1,
            /*fromOffset=*/0,
            isolation,
            /*key=*/{},
            sessionId,
            /*correlationId=*/0);

        if (len > 0) {
            sendFn(0, impl.sendBuf.data(), len);
        }
    }
}

// ---------------------------------------------------------------------------
// Fetch routing — duty thread, cold path (group / routing events)
// ---------------------------------------------------------------------------

void GroupConsumerHub::learnLateFromRecords_(const codec::RecordsView& rv)
{
    for (std::size_t i = 0; i < late_.size(); ++i) {
        const LateSub l = late_[i];
        if (l.corrId != rv.correlationId || l.brokerSid != rv.sessionId) continue;  // another client's push
        late_[i] = late_.back();
        late_.pop_back();
        unsubscribe_(l.session, l.epoch, rv.subscriptionId, l.tp);
        return;
    }
}

void GroupConsumerHub::unsubscribe_(uint32_t session, uint64_t epoch, int64_t subscriptionId, const TopicPartition& tp)
{
    // Only while the subscription's session episode is the current one: a new
    // episode is a new broker session (the old one's subscriptions died with it).
    if (epoch == 0 || router_.epoch(router_.ctx, session) != epoch) return;
    uint8_t buf[64];
    const std::size_t len = codec::encodeUnsubscribe(buf, sizeof buf, subscriptionId,
                                                     router_.sessionId(router_.ctx, session),
                                                     nextFetchCorr_tagged_());
    if (len == 0) return;
    switch (router_.offer(router_.ctx, session, buf, len)) {
    case detail::SendResult::Accepted:
        break;
    case detail::SendResult::Retry:
        pendingUnsubs_.push_back(SubRef{session, epoch, subscriptionId, tp});  // M2: re-offered per pass
        break;
    case detail::SendResult::Fatal:
        // LOW-3: never deliverable; its records are dropped anyway (they carry
        // its own correlationId, never the current instance's) until its
        // session episode ends.
        std::fprintf(stderr,
            "rune-client: group fetch Unsubscribe (subscription %lld, topic %d partition %d) can never be "
            "sent — dropped; the subscription ends with its session\n",
            static_cast<long long>(subscriptionId), tp.topicId, tp.partition);
        break;
    }
}

int GroupConsumerHub::retryFetchUnsubscribes()
{
    if (!routed_() || pendingUnsubs_.empty()) return 0;
    unsubScratch_.clear();
    unsubScratch_.swap(pendingUnsubs_);  // LOW-2: both keep their capacity
    int sent = 0;
    for (const SubRef& u : unsubScratch_) {
        const std::size_t before = pendingUnsubs_.size();
        unsubscribe_(u.session, u.epoch, u.subscriptionId, u.tp);  // moot (episode over) => dropped
        if (pendingUnsubs_.size() == before && router_.epoch(router_.ctx, u.session) == u.epoch) ++sent;
    }
    unsubScratch_.clear();
    return sent;
}

void GroupConsumerHub::dropFetch_(const TopicPartition& tp, GroupConsumerImpl::FetchSub& fs)
{
    if (fs.epoch != 0) {
        if (fs.subscriptionId != 0) {
            unsubscribe_(fs.session, fs.epoch, fs.subscriptionId, tp);
        } else if (fs.corrId != 0) {
            late_.push_back(LateSub{fs.corrId, fs.session, fs.epoch, tp, fs.brokerSid});
        }
    }
    fs.epoch          = 0;
    fs.brokerSid      = 0;
    fs.instanceCorr   = 0;
    fs.corrId         = 0;
    fs.subscriptionId = 0;
}

void GroupConsumerHub::dropAllFetch_(GroupConsumerImpl& impl)
{
    for (auto& [tp, fs] : impl.fetch) dropFetch_(tp, fs);
    impl.fetch.clear();
}

void GroupConsumerHub::pruneFetch_()
{
    // Instances / abandoned Subscribes of an ended session episode: moot.
    auto ended = [this](uint32_t session, uint64_t epoch) {
        return router_.epoch(router_.ctx, session) != epoch;
    };
    for (std::size_t i = 0; i < late_.size();) {
        if (ended(late_[i].session, late_[i].epoch)) { late_[i] = late_.back(); late_.pop_back(); }
        else ++i;
    }
    for (std::size_t i = 0; i < oldNodeLogged_.size();) {
        if (ended(oldNodeLogged_[i].session, oldNodeLogged_[i].epoch)) {
            oldNodeLogged_[i] = oldNodeLogged_.back();
            oldNodeLogged_.pop_back();
        } else {
            ++i;
        }
    }
}

int GroupConsumerHub::reconcileImpl_(GroupConsumerImpl& impl)
{
    // Partitions no longer assigned: their subscriptions go.
    for (auto it = impl.fetch.begin(); it != impl.fetch.end();) {
        if (impl.assignment.find(it->first) == impl.assignment.end()) {
            dropFetch_(it->first, it->second);
            it = impl.fetch.erase(it);
        } else {
            ++it;
        }
    }
    const uint8_t isolation = static_cast<uint8_t>(impl.cfg.isolation);
    int sent = 0;
    for (const auto& [tp, pos] : impl.assignment) {
        GroupConsumerImpl::FetchSub& fs = impl.fetch[tp];
        const uint32_t target = router_.leaderSession(router_.ctx, tp.topicId, tp.partition);
        if (fs.epoch != 0) {
            // Live where the leader is, in the same session episode: nothing to do.
            if (fs.session == target && router_.epoch(router_.ctx, fs.session) == fs.epoch) continue;
            dropFetch_(tp, fs);  // the leader moved, or that session's episode ended
        }
        if (fs.parked) continue;
        // C1: subscribed only once the start position is known (OffsetFetch /
        // the reset policy) — no record can arrive before that.
        if (!fs.offsetKnown || pos == GroupConsumerImpl::kUnseededSentinel) continue;
        const uint64_t ep  = router_.epoch(router_.ctx, target);
        const int64_t  sid = router_.sessionId(router_.ctx, target);
        if (ep == 0 || sid == 0) {
            // #290: a Rejected node session (its user revoked there, its login
            // refused) never carries this fetch: refused now with its code, as
            // the fetch's own 46 / 44 answer would — never left to wait.
            if (target != 0 && router_.rejectCode != nullptr) {
                if (const int16_t code = router_.rejectCode(router_.ctx, target); code != 0) {
                    refuse_(impl, refusalFor_(static_cast<rpc::ErrorCode>(static_cast<uint16_t>(code))));
                    return sent;
                }
            }
            // Not Connected: its ConnectResponse re-drives.  A terminally
            // Disconnected one (however it got there) is re-driven now.
            if (router_.revive != nullptr) router_.revive(router_.ctx, target);
            continue;
        }
        // A-MEDIUM-3 / R4-MEDIUM-1: a node that predates explicit group
        // subscriptions pushes without the Subscribe's correlationId — every
        // batch would be dropped (a silent stall).  The partition is skipped
        // (not parked, no metadata request: nothing to ask — the node itself
        // must change) and logged ONCE per that session's connect episode.  It
        // resumes on the event that exists: the upgraded node's session
        // reconnects (a new episode advertising >= 3) and its accepted connect
        // re-drives reconcile.
        //
        // Phase 2c: a FILTERED group raises the bar to kBrokerVersionFilter — a node below it would ignore the
        // filter and push everything, so the partition is skipped, never fetched unfiltered.  This point is
        // reached only for a Connected node (ep != 0 above), so the version judged is the live session's: a node
        // mid-reconnect defers instead of failing, and the upgraded node's connect re-drives reconcile.
        const int32_t minVersion = impl.cfg.filterBytes.empty()
            ? rune::protocol::client::kBrokerVersionExplicitGroupSubscriptions
            : rune::protocol::client::kBrokerVersionFilter;
        // I1: the seed session (target 0) is gated like any other — it may have reconnected onto an older node
        // while the coordinator lives elsewhere, and a filtered fetch must never go to a node that ignores it.
        if (router_.brokerVersion != nullptr && router_.brokerVersion(router_.ctx, target) < minVersion) {
            bool logged = false;
            for (const OldNodeLog& o : oldNodeLogged_) {
                if (o.session == target && o.epoch == ep) { logged = true; break; }
            }
            if (!logged) {
                oldNodeLogged_.push_back(OldNodeLog{target, ep});  // cold: once per episode
                std::fprintf(stderr,
                    "rune-client: group '%s': a partition leader node (session %u) runs broker version %d "
                    "(< %d, %s) — its partitions (first: topic %d partition %d) are "
                    "not consumed until the node is upgraded and reconnects\n",
                    impl.groupId.c_str(), target, router_.brokerVersion(router_.ctx, target), minVersion,
                    impl.cfg.filterBytes.empty() ? "explicit group subscriptions" : "subscription filters",
                    tp.topicId, tp.partition);
            }
            continue;
        }
        const int64_t corr = nextFetchCorr_tagged_();
        constexpr std::size_t kSubscribeFrameSize = 44u;
        ensureBuf(impl, kSubscribeFrameSize + impl.cfg.filterBytes.len + 2u);
        // Phase 2c: a plain Subscribe at the leader carries the group's filter (empty = byte-identical to before).
        const std::size_t len = codec::encodeSubscribe(impl.sendBuf.data(), impl.sendBuf.size(), tp.topicId,
                                                       tp.partition, /*partitionKnown=*/1, pos, isolation,
                                                       /*key=*/{}, sid, corr, impl.cfg.filterBytes.span());
        if (len == 0) continue;
        switch (router_.offer(router_.ctx, target, impl.sendBuf.data(), len)) {
        case detail::SendResult::Accepted:
            fs.session        = target;
            fs.epoch          = ep;
            fs.brokerSid      = sid;
            fs.instanceCorr   = corr;
            fs.corrId         = corr;
            fs.subscriptionId = 0;
            ++sent;
            break;
        case detail::SendResult::Retry:
            break;  // transiently refused: the next routing event / pass re-offers it
        case detail::SendResult::Fatal:
            fs.parked       = true;  // never deliverable as is: retried after fresh metadata
            metadataWanted_ = true;
            break;
        }
    }
    return sent;
}

int GroupConsumerHub::resendUnanswered(uint32_t session)
{
    if (!routed_()) return 0;
    // MEDIUM-2 (round 3/4): a Subscribe that got neither its answer nor a push
    // is re-sent on THAT session's events (it (re)connected, was lost, ...) and
    // each new generation (never on a clock): the old instance is abandoned
    // (late_: unsubscribed if its id ever shows up — its pushes carry its own
    // correlationId, never the new one's).  Other sessions' in-flight
    // Subscribes are left alone (A-MEDIUM-2: their answers are queued by the
    // broker, never lost on a live session).
    for (std::size_t ix = 0, nx = impls_.size(); ix < nx; ++ix) {
        GroupConsumerImpl* const impl = impls_[ix];
        if (!impl->active || impl->state != GroupConsumerState::Stable) continue;
        for (auto& [tp, fs] : impl->fetch) {
            if (fs.epoch != 0 && fs.corrId != 0 && fs.session == session) dropFetch_(tp, fs);
        }
    }
    return reconcileFetch();
}

int GroupConsumerHub::reconcileFetch()
{
    if (!routed_()) return 0;
    pruneFetch_();
    (void)retryFetchUnsubscribes();
    int sent = 0;
    for (std::size_t ix = 0, nx = impls_.size(); ix < nx; ++ix) {  // re-entrancy safe (I-3)
        GroupConsumerImpl* const impl = impls_[ix];
        if (impl->active && impl->state == GroupConsumerState::Stable) sent += reconcileImpl_(*impl);
    }
    return sent;
}

void GroupConsumerHub::unparkFetch() noexcept
{
    for (GroupConsumerImpl* impl : impls_) {
        for (auto& [tp, fs] : impl->fetch) {
            fs.parked    = false;
            fs.redirects = 0;
        }
    }
}

bool GroupConsumerHub::fetchUses(uint32_t session) const noexcept
{
    if (!routed_()) return false;
    for (const GroupConsumerImpl* impl : impls_) {
        if (!impl->active || impl->state != GroupConsumerState::Stable) continue;
        for (const auto& [tp, pos] : impl->assignment) {
            const auto f = impl->fetch.find(tp);
            if (f != impl->fetch.end() && f->second.epoch != 0 && f->second.session == session) return true;
            // Not subscribed (yet): does its route point at that session?
            if (router_.leaderSession(router_.ctx, tp.topicId, tp.partition) == session) return true;
        }
    }
    return false;
}

bool GroupConsumerHub::fetchWaitsForSession() const noexcept
{
    if (!routed_()) return false;
    for (const GroupConsumerImpl* impl : impls_) {
        if (!impl->active || impl->state != GroupConsumerState::Stable) continue;
        for (const auto& [tp, fs] : impl->fetch) {
            if (fs.epoch != 0 || !fs.offsetKnown) continue;
            const uint32_t target = router_.leaderSession(router_.ctx, tp.topicId, tp.partition);
            if (router_.epoch(router_.ctx, target) == 0) return true;
        }
    }
    return false;
}

bool GroupConsumerHub::onFetchSubscribeResponse(int64_t corrId, int16_t errorCode, int64_t subscriptionId)
{
    if (!isFetchCorrId(corrId)) return false;
    for (GroupConsumerImpl* impl : impls_) {
        for (auto& [tp, fs] : impl->fetch) {
            if (fs.corrId != corrId) continue;
            fs.corrId = 0;
            if (errorCode == static_cast<int16_t>(rpc::ErrorCode::NotAuthorized) ||
                errorCode == static_cast<int16_t>(rpc::ErrorCode::SessionRevoked)) {
                // Authentication: not retriable — never parked for a metadata retry loop.
                refuse_(*impl, refusalFor_(static_cast<rpc::ErrorCode>(static_cast<uint16_t>(errorCode))));
                return true;
            }
            if (errorCode == static_cast<int16_t>(rpc::ErrorCode::InvalidRequest) && !impl->cfg.filterBytes.empty()) {
                // Phase 2c (spec §9): the leader refused this group's FILTER (untyped topic, over its capacity,
                // unknown tag).  The same bytes would be refused again — never parked for a metadata retry
                // loop; the consumer ends (Client.cpp passes the refusal detail to onError).
                refuse_(*impl, GroupFailReason::RefusedByBroker);
                return true;
            }
            // PR #441 F4: the broker cannot build this session's record stream (RecordEgressUnavailable): retried
            // after fresh metadata like any refusal, but within a small budget (reset by a bind), then the consumer
            // ends explicitly - never an endless re-subscribe.  Routing refusals (NotLeader / Redirect) stay unbounded.
            if (errorCode == static_cast<int16_t>(rpc::ErrorCode::RecordEgressUnavailable) &&
                ++fs.egressRefusals > kMaxEgressRefusalsWithoutBind) {
                refuse_(*impl, GroupFailReason::RetryBudgetExhausted);
                return true;
            }
            if (errorCode == 0) {
                fs.subscriptionId = subscriptionId;
                fs.redirects      = 0;
                fs.egressRefusals = 0;   // F4: bound
            } else {
                fs.epoch        = 0;     // nothing registered (UnknownTopic / UnknownPartition / ...)
                fs.brokerSid    = 0;
                fs.instanceCorr = 0;
                fs.parked       = true;  // retried after fresh metadata
                metadataWanted_ = true;
            }
            return true;
        }
    }
    for (std::size_t i = 0; i < late_.size(); ++i) {
        if (late_[i].corrId != corrId) continue;
        const LateSub l = late_[i];
        late_[i] = late_.back();
        late_.pop_back();
        if (errorCode == 0) unsubscribe_(l.session, l.epoch, subscriptionId, l.tp);  // its records never matched
        break;
    }
    return true;  // a group fetch answer (maybe stale): never the SubscriberHub's
}

bool GroupConsumerHub::onFetchAuthRefused(int64_t corrId, int16_t errorCode)
{
    if (!isFetchCorrId(corrId)) return false;
    for (GroupConsumerImpl* impl : impls_) {
        if (!impl->active) continue;
        for (auto& [tp, fs] : impl->fetch) {
            // Unanswered (corrId), or a BOUND instance (its Subscribe id: a revoked READ ends it explicitly).
            if (fs.corrId != corrId && !(fs.epoch != 0 && fs.instanceCorr == corrId)) continue;
            refuse_(*impl, refusalFor_(static_cast<rpc::ErrorCode>(static_cast<uint16_t>(errorCode))));
            return true;
        }
    }
    return false;
}

bool GroupConsumerHub::onFetchInvalidRequest(int64_t corrId)
{
    if (!isFetchCorrId(corrId)) return false;
    for (GroupConsumerImpl* impl : impls_) {
        if (!impl->active || impl->cfg.filterBytes.empty()) continue;
        for (auto& [tp, fs] : impl->fetch) {
            if (fs.corrId != corrId && !(fs.epoch != 0 && fs.instanceCorr == corrId)) continue;
            refuse_(*impl, GroupFailReason::RefusedByBroker);
            return true;
        }
    }
    return false;
}

int GroupConsumerHub::revokeAll()
{
    int n = 0;
    for (std::size_t ix = 0, nx = impls_.size(); ix < nx; ++ix) {   // re-entrancy safe (I-3)
        GroupConsumerImpl* const impl = impls_[ix];
        if (!impl->active) continue;
        refuse_(*impl, GroupFailReason::SessionRevoked);
        ++n;
    }
    return n;
}

bool GroupConsumerHub::onFetchSubscribeRefused(int64_t corrId, bool park, int32_t& topicId, int32_t& partition)
{
    if (!isFetchCorrId(corrId)) return false;
    for (GroupConsumerImpl* impl : impls_) {
        for (auto& [tp, fs] : impl->fetch) {
            // Unanswered (corrId), or — Rune E2 Task 13 — a BOUND instance its
            // node stopped serving (it answers the instance's Subscribe id
            // NotLeader and drops it after a demotion): re-routed the same way.
            if (fs.corrId != corrId && !(fs.epoch != 0 && fs.instanceCorr == corrId)) continue;
            // A BOUND instance (fix round 2, M4): release it on its node too, as
            // the plain path does.  The node that answered NotLeader already
            // dropped it (an Unsubscribe of an unknown id is ignored); should
            // the answer have been a stray one, no second stream leaks.
            if (fs.corrId != corrId && fs.subscriptionId != 0)
                unsubscribe_(fs.session, fs.epoch, fs.subscriptionId, tp);
            fs.corrId         = 0;
            fs.epoch          = 0;  // registered nothing
            fs.brokerSid      = 0;
            fs.instanceCorr   = 0;
            fs.subscriptionId = 0;
            if (park || ++fs.redirects > kMaxFetchRedirects) {
                fs.parked       = true;
                metadataWanted_ = true;
            }
            topicId   = tp.topicId;
            partition = tp.partition;
            return true;
        }
    }
    for (std::size_t i = 0; i < late_.size(); ++i) {
        if (late_[i].corrId == corrId) {  // abandoned and refused: nothing to undo
            late_[i] = late_.back();
            late_.pop_back();
            break;
        }
    }
    return false;
}

int GroupConsumerHub::retryOffsetFetch(int64_t nowNs, int64_t sessionId, const SendFn& sendFn)
{
    if (!routed_() || !sessionConnected_) return 0;
    int sent = 0;
    for (std::size_t ix = 0, nx = impls_.size(); ix < nx; ++ix) {  // re-entrancy safe (I-3)
        GroupConsumerImpl* const impl = impls_[ix];
        if (!impl->active || impl->state != GroupConsumerState::Stable) continue;
        bool missing = false;
        for (const auto& [tp, pos] : impl->assignment) {
            const auto f = impl->fetch.find(tp);
            if (f == impl->fetch.end() || !f->second.offsetKnown) { missing = true; break; }
        }
        if (!missing) {
            impl->offsetFetchBackoffNs = 0;
            continue;
        }
        // Answer deadline of a fetch in flight: a lost answer is re-sent, a
        // slow one never duplicated (the coordinator evicts a member silent
        // for sessionTimeoutMs anyway).
        const int64_t answerTimeoutNs = int64_t{impl->cfg.sessionTimeoutMs} * 1'000'000LL;
        using Phase = GroupConsumerImpl::OffsetFetchPhase;
        switch (impl->offsetFetchPhase) {
        case Phase::Sent:       // the eager Stable-entry / Assign fetch: arm its deadline
            impl->offsetFetchPhase      = Phase::Awaiting;
            impl->offsetFetchAnswerByNs = nowNs + answerTimeoutNs;
            continue;
        case Phase::Awaiting:
            if (nowNs < impl->offsetFetchAnswerByNs) continue;   // still in flight
            break;                                               // unanswered: re-send
        case Phase::Idle:
            if (nowNs < impl->offsetFetchDueNs) continue;        // paced
            break;
        }
        if (sendOffsetFetchFor_(*impl, sessionId, sendFn, /*unknownOnly=*/true)) {
            impl->offsetFetchPhase      = Phase::Awaiting;
            impl->offsetFetchAnswerByNs = nowNs + answerTimeoutNs;
        }
        // Pacing only (the answer, not this clock, decides): 100 ms doubling to 2 s.
        impl->offsetFetchBackoffNs = impl->offsetFetchBackoffNs == 0
            ? 100'000'000LL : std::min<int64_t>(impl->offsetFetchBackoffNs * 2, 2'000'000'000LL);
        impl->offsetFetchDueNs = nowNs + impl->offsetFetchBackoffNs;
        ++sent;
    }
    return sent;
}

// #440: what the coordinator's version allows.  A coordinator below the needed version while a newer node was seen is a
// rolling upgrade in flight: the join waits for a coordinator move / reconnect (no timer); with no newer node ever
// seen the cluster cannot serve this consumer and it ends BrokerTooOld.
GroupConsumerHub::JoinGate GroupConsumerHub::joinGate_(const GroupConsumerImpl& impl) const noexcept
{
    namespace pc = rune::protocol::client;
    const int32_t need = impl.cfg.filterBytes.empty()
        ? pc::kBrokerVersionExplicitGroupSubscriptions
        : std::max(pc::kBrokerVersionExplicitGroupSubscriptions, pc::kBrokerVersionFilter);
    if (brokerVersion_ >= need) return JoinGate::Ok;
    return maxBrokerVersionSeen_ >= need ? JoinGate::Defer : JoinGate::Refuse;
}

bool GroupConsumerHub::gateAllows_(GroupConsumerImpl& impl)
{
    switch (joinGate_(impl)) {
    case JoinGate::Ok:
        return true;
    case JoinGate::Refuse:
        refuse_(impl, GroupFailReason::BrokerTooOld);   // Phase 2c: a filtered group needs >= kBrokerVersionFilter
        return false;
    case JoinGate::Defer:
        if (!impl.joinDeferredLogged) {
            impl.joinDeferredLogged = true;   // once per deferral episode
            const int32_t need = impl.cfg.filterBytes.empty()
                ? rune::protocol::client::kBrokerVersionExplicitGroupSubscriptions
                : std::max(rune::protocol::client::kBrokerVersionExplicitGroupSubscriptions,
                           rune::protocol::client::kBrokerVersionFilter);
            std::fprintf(stderr,
                "rune-client: group '%s': coordinator broker version %d is below %d (a newer node was seen): "
                "join deferred until the coordinator moves or reconnects\n",
                impl.groupId.c_str(), brokerVersion_, need);
        }
        return false;
    }
    return false;
}

bool GroupConsumerHub::tryJoin_(GroupConsumerImpl& impl, int64_t sessionId, const SendFn& sendFn)
{
    if (!gateAllows_(impl)) return false;
    impl.joinDeferredLogged = false;
    sendJoinGroup(impl, sessionId, sendFn);
    return true;
}

void GroupConsumerHub::joinIfConnected(int64_t sessionId, const SendFn& sendFn)
{
    if (!sessionConnected_ || sessionId == 0) return;
    for (std::size_t ix = 0, nx = impls_.size(); ix < nx; ++ix) {  // re-entrancy safe (I-3)
        GroupConsumerImpl* const impl = impls_[ix];
        if (!impl->active || impl->state != GroupConsumerState::Unjoined) continue;
        if (!gateAllows_(*impl)) continue;   // #440: Refuse ends it, Defer waits
        if (filterNeedsRouting_(*impl)) continue;
        (void)tryJoin_(*impl, sessionId, sendFn);
    }
}

// ---------------------------------------------------------------------------
// applyAssignment — runner thread
// Fire revoked + assigned callbacks, update assignment map.
// ---------------------------------------------------------------------------

void GroupConsumerHub::applyAssignment(GroupConsumerImpl& impl,
                                        std::vector<TP> newAssignment)
{
    // Compute revoked = old \ new and newly added = new \ old.
    std::vector<TP> revoked;
    std::vector<TP> added;

    for (const auto& [tp, _] : impl.assignment) {
        bool inNew = false;
        for (const auto& ntp : newAssignment) {
            if (ntp == tp) { inNew = true; break; }
        }
        if (!inNew) revoked.push_back(tp);
    }
    for (const auto& ntp : newAssignment) {
        if (impl.assignment.find(ntp) == impl.assignment.end()) {
            added.push_back(ntp);
        }
    }

    // Fire revoked. fix1 C1: this runs inside the noexcept ClientAgent::doWork
    // chain — a throwing callback must never escape it.
    if (!revoked.empty() && impl.cbRevoked) {
        DispatchScope scope(*this, impl);
        try {
            impl.cbRevoked(revoked);
        } catch (const std::exception& e) {
            ++callbackExceptions_;
            std::fprintf(stderr,
                "rune-client: group onRevoked threw: %s (caught; the duty cycle continues)\n",
                e.what());
        } catch (...) {
            ++callbackExceptions_;
            std::fprintf(stderr,
                "rune-client: group onRevoked threw a non-std::exception "
                "(caught; the duty cycle continues)\n");
        }
    }

    // Remove revoked from assignment map.
    for (const auto& tp : revoked) {
        impl.assignment.erase(tp);
    }

    // Add new partitions with sentinel -1 (position unknown until OffsetFetch
    // response seeds it).  We must NOT commit sentinel positions (#1b).
    // Caller (onSyncGroupResponse / onAssign) sends OffsetFetch after this.
    for (const auto& tp : added) {
        impl.assignment[tp] = GroupConsumerImpl::kUnseededSentinel;
    }

    // Fire assigned.
    if (!added.empty() && impl.cbAssigned) {
        DispatchScope scope(*this, impl);
        try {
            impl.cbAssigned(added);
        } catch (const std::exception& e) {
            ++callbackExceptions_;
            std::fprintf(stderr,
                "rune-client: group onAssigned threw: %s (caught; the duty cycle continues)\n",
                e.what());
        } catch (...) {
            ++callbackExceptions_;
            std::fprintf(stderr,
                "rune-client: group onAssigned threw a non-std::exception "
                "(caught; the duty cycle continues)\n");
        }
    }
}

// ---------------------------------------------------------------------------
// doWork — runner thread
// ---------------------------------------------------------------------------

int GroupConsumerHub::doWork(int64_t nowNs,
                              int64_t sessionId,
                              const SendFn& sendFn)
{
    int work = 0;
    const auto now = std::chrono::steady_clock::now();

    for (std::size_t ix = 0, nx = impls_.size(); ix < nx; ++ix) {  // re-entrancy safe (I-3)
        GroupConsumerImpl* const impl = impls_[ix];
        if (!impl->active) continue;

        // ---- Drain pending commit() / commitSync() requests --------------------
        work += drainCommit_(*impl, sessionId, sendFn);

        switch (impl->state) {

        case GroupConsumerState::Unjoined:
            // Not yet joined — wait for onConnected() to trigger.
            break;

        case GroupConsumerState::Joining:
            // Awaiting JoinGroupResponse. Guard against stale join (timeout).
            // D4: a Join the coordinator session refused (back-pressure) is
            // re-offered on the next pass once that session is connected.
            if (now >= impl->joinDeadline || (impl->joinSessionId == 0 && sessionConnected_ && sessionId != 0)) {
                // Re-send JoinGroup (rejoin guard expired).
                if (tryJoin_(*impl, sessionId, sendFn)) ++work;   // #440: a deferred join sends nothing
            }
            break;

        case GroupConsumerState::Syncing:
            // Awaiting SyncGroupResponse. Guard with join deadline (D4: or the
            // Sync was refused — re-join at once once connected).
            if (now >= impl->joinDeadline || (impl->joinSessionId == 0 && sessionConnected_ && sessionId != 0)) {
                toRejoining_(*impl);
            }
            break;

        case GroupConsumerState::Stable: {
            // ---- Heartbeat timer --------------------------------------------
            if (now >= impl->nextHeartbeat) {
                sendHeartbeat(*impl, sessionId, sendFn);
                const int32_t intervalMs = impl->cfg.sessionTimeoutMs / 3;
                impl->nextHeartbeat = now + std::chrono::milliseconds(intervalMs);
                ++work;
            }

            // ---- Auto-commit timer ------------------------------------------
            if (impl->cfg.autoCommit && now >= impl->nextAutoCommit) {
                (void)sendOffsetCommit(*impl, sessionId, sendFn, /*correlationId=*/0);
                impl->nextAutoCommit =
                    now + std::chrono::milliseconds(impl->cfg.autoCommitIntervalMs);
                ++work;
            }
            break;
        }

        case GroupConsumerState::Rejoining:
            // Re-send JoinGroup — at once for a rebalance signal, after the
            // backoff for an Internal / unclassified error (C1).
            if (now < impl->rejoinNotBefore) break;
            if (tryJoin_(*impl, sessionId, sendFn)) ++work;   // #440
            break;
        }
    }

    return work;
}

// ---------------------------------------------------------------------------
// drainCommit_ / flushRequestedCommits / onSessionDown — duty thread (fix2)
// ---------------------------------------------------------------------------

int GroupConsumerHub::drainCommit_(GroupConsumerImpl& impl, int64_t sessionId, const SendFn& sendFn)
{
    // Kept pending until Stable and Connected (a commitSync waits for it; a
    // rejoin or session loss fails it) and until the transport accepts it.
    if (!impl.commitRequested || impl.state != GroupConsumerState::Stable || !sessionConnected_) return 0;
    impl.commitRequested = false;
    const bool forSync = impl.hasCommitSync && impl.commitSyncCorrId == 0;
    const int64_t corr = forSync ? nextCommitCorrId_++ : 0;
    const CommitSend r = sendOffsetCommit(impl, sessionId, sendFn, corr);
    if (r == CommitSend::SendFailed) {
        impl.commitRequested = true;  // retried next duty cycle (never silently lost)
        return 0;
    }
    if (r == CommitSend::Fatal) {
        // fix4 M1: the transport can never carry it — dropped (not re-offered
        // forever); a commitSync waiting on it completes now (NotConnected:
        // the transport cannot reach the broker with it).
        completeCommitSync_(impl, OpStatus::NotConnected);
        return 1;
    }
    if (forSync) {
        if (r == CommitSend::Sent) {
            impl.commitSyncCorrId = corr;  // resolved by the matching response
        } else {
            // Nothing to commit (no seeded position): done.
            completeCommitSync_(impl, OpStatus::Ok);
        }
    }
    return 1;
}

int GroupConsumerHub::flushRequestedCommits(int64_t sessionId)
{
    int work = 0;
    for (std::size_t ix = 0, nx = impls_.size(); ix < nx; ++ix) {  // re-entrancy safe (I-3)
        GroupConsumerImpl* const impl = impls_[ix];
        if (impl->active && impl->commitRequested) work += drainCommit_(*impl, sessionId, SendFn{});
    }
    return work;
}

bool GroupConsumerHub::hasPendingCommitSync() const noexcept
{
    for (const auto& impl : impls_) {
        if (impl->hasCommitSync || rewindInFlight_(*impl)) return true;   // #494: a parked rewind does not count
    }
    return false;
}

void GroupConsumerHub::onSessionDown()
{
    for (std::size_t ix = 0, nx = impls_.size(); ix < nx; ++ix) {  // re-entrancy safe (I-3)
        GroupConsumerImpl* const impl = impls_[ix];
        if (impl->active) {
            completeCommitSync_(*impl, OpStatus::SessionLost);
            if (rewindInFlight_(*impl)) completeRewind_(*impl, OpStatus::SessionLost);   // #494
        }
    }
}

// ---------------------------------------------------------------------------
// onRecords — runner thread (hot path)
// ---------------------------------------------------------------------------

void GroupConsumerHub::onRecords(const codec::RecordsView& rv)
{
    // Task 13 fix round 2 (whole class): an 8-byte recordBatch is a NOTE (the
    // transient lineage note, a catalog epoch note / marker), never a batch — it
    // must not move a member's position (the empty position note never reaches
    // here: Client.cpp routes it to plain subscriptions only).  A frame of
    // several whole batches spans to its last batch (RecordsView fields).
    if (rv.recordBatch.size() == 8) [[unlikely]] { ++notesIgnored_; return; }
    const TP key{rv.topicId, rv.partition};
    for (std::size_t ix = 0, nx = impls_.size(); ix < nx; ++ix) {  // re-entrancy safe (I-3)
        GroupConsumerImpl* const impl = impls_[ix];
        if (!impl->active) continue;
        if (impl->state != GroupConsumerState::Stable) continue;

        // Check if this (topicId, partition) is in the assignment.
        auto it = impl->assignment.find(key);
        if (it == impl->assignment.end()) continue;

        if (routed_()) {
            // C1: only this consumer's CURRENT subscription instance — the push
            // names the Subscribe that created it (correlationId) and its
            // session; once bound, its (cluster-unique) id as well.  Another
            // consumer (another group) may own the batch: keep looking (LOW-5).
            const auto f = impl->fetch.find(key);
            if (f == impl->fetch.end()) continue;
            GroupConsumerImpl::FetchSub& fs = f->second;
            if (fs.epoch == 0 || rv.correlationId != fs.instanceCorr || rv.sessionId != fs.brokerSid) continue;
            if (router_.epoch(router_.ctx, fs.session) != fs.epoch) continue;
            if (fs.subscriptionId == 0) {
                fs.subscriptionId = rv.subscriptionId;  // bound by its first push
                fs.corrId         = 0;
                fs.redirects      = 0;
                fs.egressRefusals = 0;   // F4: bound
            } else if (rv.subscriptionId != fs.subscriptionId) {
                continue;
            }
        }

        // The position is seeded only by OffsetFetch / the reset policy (C1).
        const int64_t pos = it->second;
        if (pos == GroupConsumerImpl::kUnseededSentinel) { ++droppedBatches_; return; }
        // Task 13 fix round 3 (M2-r2): validate every batch (CRC) up front.  The
        // position never moves past a record that was not handed to the callback
        // unless that was counted and logged: delivery and the position stop at
        // the valid prefix.  Stub views (shorter than a batch header) keep the
        // header-derived fields (test helpers).  No allocation: one stack copy.
        namespace pb = rune::protocol::batch;
        std::size_t validLen = 0;
        int64_t     validEnd = -1;
        const bool  validate = rv.recordBatch.size() >= static_cast<std::size_t>(pb::kRecordBatchOverhead);
        if (validate) {
            std::span<const uint8_t> rest = rv.recordBatch;
            bool bad = false;
            while (rest.size() >= static_cast<std::size_t>(pb::kRecordBatchOverhead)) {
                rune::protocol::RecordBatchReader r(rest.data(), rest.size());
                if (!r.ensureValid()) { bad = true; break; }
                validEnd = r.baseOffset() + static_cast<int64_t>(r.lastOffsetDelta()) + 1;
                validLen += static_cast<std::size_t>(r.totalLength());
                rest = rest.subspan(static_cast<std::size_t>(r.totalLength()));
            }
            if (!rest.empty() && !bad) bad = true;   // a torn tail shorter than a batch header
            if (bad) noteMalformed_();
            if (validLen == 0) { ++droppedBatches_; return; }   // nothing valid: nothing delivered, the position stays
        }
        // LOW-1: last offset + 1 (compaction holes inside a batch), never less
        // than the record count (hand-built views without a batch header).
        const int64_t span = std::max<int64_t>(static_cast<int64_t>(rv.lastOffsetDelta) + 1, rv.recordCount);
        const int64_t end  = validate ? validEnd : rv.baseOffset + span;
        if (end <= pos) { ++droppedBatches_; return; }  // entirely below: a duplicate (MEDIUM-3)
        // Otherwise delivered and the position moves to the batch's end — a
        // batch above the position is legitimate (transaction markers, aborted
        // batches, compaction holes are never pushed).  E2 Task 13 fix round 2:
        // one STRADDLING it (a resume after a leader move, a mid-batch
        // committed offset — brokers push whole batches) carries records
        // already consumed: the view says so (deliverFrom = the position; a
        // stack copy, no allocation) and its iteration skips them.
        // One stack copy carries both the straddle floor and the validated prefix.
        codec::RecordsView straddle;
        const codec::RecordsView* view = &rv;
        if (rv.baseOffset < pos || validate) {
            straddle = rv;
            if (rv.baseOffset < pos) [[unlikely]] straddle.deliverFrom = pos;
            if (validate) straddle.validBatchBytes = static_cast<int32_t>(validLen);
            view = &straddle;
        }

        // Deliver to consumer. fix1 C1: noexcept doWork chain — catch/count/log.
        uint32_t rewindEpoch0 = 0;
        if (routed_()) {
            const auto f0 = impl->fetch.find(key);
            if (f0 != impl->fetch.end()) rewindEpoch0 = f0->second.rewindEpoch;
        }
        if (impl->cbRecords) {
            DispatchScope scope(*this, *impl);
            try {
                impl->cbRecords(*view);
            } catch (const std::exception& e) {
                ++callbackExceptions_;
                std::fprintf(stderr,
                    "rune-client: group onRecords threw: %s (caught; the duty cycle continues)\n",
                    e.what());
            } catch (...) {
                ++callbackExceptions_;
                std::fprintf(stderr,
                    "rune-client: group onRecords threw a non-std::exception "
                    "(caught; the duty cycle continues)\n");
            }
        }

        // (#2) newPos = baseOffset + recordCount (== lastOffset + 1); recordCount
        // falls back to 1 for empty/stub batches (test helpers).  The callback
        // may have rebalanced this consumer: look the partition up again.
        const auto again = impl->assignment.find(key);
        if (routed_()) {
            // A seek / seekToCommitted applied INSIDE the callback owns the position: the advance
            // would overwrite it (a seek to this batch's start must redeliver the batch).
            const auto f1 = impl->fetch.find(key);
            if (f1 != impl->fetch.end() && f1->second.rewindEpoch != rewindEpoch0) return;
        }
        if (again != impl->assignment.end() && again->second == pos) again->second = end;
        return;
    }
    ++droppedBatches_;
    // An abandoned, never-answered instance just revealed its id: clean it up.
    if (routed_() && !late_.empty() && rv.correlationId != 0) learnLateFromRecords_(rv);
}

// ---------------------------------------------------------------------------
// onPositionNote — runner thread (hot path: one per idle/filtered frame; no allocation)
//
// Phase 2c: a filtered stream delivers fewer records than the log holds; the broker says how far it has
// scanned with an EMPTY Records frame whose baseOffset is the next offset it will consider.  A filtered group
// follows it so its commit moves past the filtered-out span.  The instance checks are exactly onRecords'.
// Unfiltered groups ignore notes (a position past records never delivered would lose them).
// ---------------------------------------------------------------------------

void GroupConsumerHub::onPositionNote(const codec::RecordsView& rv)
{
    if (!routed_()) return;
    const TP key{rv.topicId, rv.partition};
    for (std::size_t ix = 0, nx = impls_.size(); ix < nx; ++ix) {
        GroupConsumerImpl* const impl = impls_[ix];
        if (!impl->active || impl->cfg.filterBytes.empty()) continue;
        if (impl->state != GroupConsumerState::Stable) continue;
        const auto it = impl->assignment.find(key);
        if (it == impl->assignment.end()) continue;
        const auto f = impl->fetch.find(key);
        if (f == impl->fetch.end()) continue;
        GroupConsumerImpl::FetchSub& fs = f->second;
        if (fs.epoch == 0 || rv.correlationId != fs.instanceCorr || rv.sessionId != fs.brokerSid) continue;
        if (router_.epoch(router_.ctx, fs.session) != fs.epoch) continue;
        if (fs.subscriptionId == 0) {
            fs.subscriptionId = rv.subscriptionId;  // bound by its first push
            fs.corrId         = 0;
            fs.redirects      = 0;
        } else if (rv.subscriptionId != fs.subscriptionId) {
            continue;
        }
        // Only forward, and only once seeded (a sentinel position is OffsetFetch's to set).
        if (it->second != GroupConsumerImpl::kUnseededSentinel && rv.baseOffset > it->second)
            it->second = rv.baseOffset;
        return;
    }
}

// Task 13 fix round 3 (M2-r2): a Records frame whose batch failed its CRC /
// length check on the group path — counted, logged at most once per 1024.
void GroupConsumerHub::noteMalformed_() noexcept
{
    if (malformedBatches_++ % 1024 == 0)
        std::fprintf(stderr, "rune-client: a group Records frame carried a malformed batch — delivered up to it, "
                             "the position stops there (malformed so far: %llu)\n",
                     static_cast<unsigned long long>(malformedBatches_));
}

// ---------------------------------------------------------------------------
// onJoinGroupResponse — runner thread
// ---------------------------------------------------------------------------

void GroupConsumerHub::onJoinGroupResponse(const uint8_t* buf, std::size_t len,
                                            int64_t sessionId, const SendFn& sendFn)
{
    auto opt = rpc::JoinGroupResponse::decode(buf, len);
    if (!opt) return;
    if (!isGroupCtlCorrId(opt->correlationId)) return;   // #431: not an answer to one of ours

    for (std::size_t ix = 0, nx = impls_.size(); ix < nx; ++ix) {  // re-entrancy safe (I-3)
        GroupConsumerImpl* const impl = impls_[ix];
        if (!impl->active) continue;
        // #431: only the answer to THIS consumer's join in flight (a stale or foreign one is dropped).
        if (impl->state != GroupConsumerState::Joining || impl->ctlCorrId != opt->correlationId) continue;

        const auto ec = static_cast<rpc::ErrorCode>(opt->errorCode);
        // Phase 2c (E10): the group runs with another filter (the first member fixed it).  Re-joining re-sends
        // the same filter — never retried; the consumer ends FilterMismatch.  Any other InvalidRequest keeps the
        // existing classification.  (Client.cpp hands the detail text to the error callback.)
        if (ec == rpc::ErrorCode::InvalidRequest && opt->detail.starts_with(rpc::kGroupFilterMismatchPrefix)) {
            refuse_(*impl, GroupFailReason::FilterMismatch);
            break;
        }
        const GroupVerdict v = classifyGroupAnswer_(ec);

        if (v == GroupVerdict::Ok) {
            // Store memberId and generationId.
            impl->memberId    = std::string(
                reinterpret_cast<const char*>(opt->memberId.data()),
                opt->memberId.size());
            impl->generationId = opt->generationId;
            publishGeneration_(*impl);

            sendSyncGroup(*impl, sessionId, sendFn);
        } else if (v == GroupVerdict::Refuse) {
            // MAJOR-2 (round 3): ProtocolVersion — this client is too old for the
            // broker.  No-hang wave: InvalidRequest (the broker could not decode
            // the JoinGroup) — re-joining re-sends the same bytes (a join storm).
            refuse_(*impl, refusalFor_(ec));
        } else if (v == GroupVerdict::Rejoin) {
            toRejoining_(*impl);
        } else {
            rejoinWithBackoff_(*impl);   // C1: Internal / unclassified — paced + bounded
        }
        break;  // only one Joining consumer handled per response
    }
}

// ---------------------------------------------------------------------------
// onSyncGroupResponse — runner thread
// ---------------------------------------------------------------------------

void GroupConsumerHub::onSyncGroupResponse(const uint8_t* buf, std::size_t len,
                                            int64_t sessionId, const SendFn& sendFn)
{
    auto opt = rpc::SyncGroupResponse::decode(buf, len);
    if (!opt) return;
    if (!isGroupCtlCorrId(opt->correlationId)) return;   // #431

    for (std::size_t ix = 0, nx = impls_.size(); ix < nx; ++ix) {  // re-entrancy safe (I-3)
        GroupConsumerImpl* const impl = impls_[ix];
        if (!impl->active) continue;
        if (impl->state != GroupConsumerState::Syncing || impl->ctlCorrId != opt->correlationId) continue;   // #431

        const auto ec = static_cast<rpc::ErrorCode>(opt->errorCode);
        const GroupVerdict v = classifyGroupAnswer_(ec);

        if (v == GroupVerdict::Ok) {
            impl->offsetFetchAttempts = 0;   // fix round 3: a new generation starts a fresh fetch budget
            // A new generation starts fresh OffsetFetch pacing (never a clock
            // left over from an earlier generation) with nothing in flight.
            impl->offsetFetchBackoffNs = 0;
            impl->offsetFetchDueNs     = 0;
            impl->offsetFetchPhase     = GroupConsumerImpl::OffsetFetchPhase::Idle;
            // Build new assignment from SyncGroupResponse.
            std::vector<TP> newAsgn;
            for (const auto& tp : opt->assignment) {
                newAsgn.push_back({tp.topicId, tp.partition});
            }

            applyAssignment(*impl, std::move(newAsgn));

            // (#1a) On every Syncing→Stable transition (fresh join or rejoin),
            // reset ALL assigned partitions to the unseeded sentinel.
            // This ensures that even kept partitions (same partition re-assigned
            // after a rejoin) are re-seeded from the broker's committed offset,
            // which is the authoritative source after any gap in connectivity.
            for (auto& [tp, pos] : impl->assignment) {
                pos = GroupConsumerImpl::kUnseededSentinel;
            }

            // Arm heartbeat and auto-commit timers.
            const auto now  = tpNow();
            const int32_t hbMs = impl->cfg.sessionTimeoutMs / 3;
            impl->nextHeartbeat  = now + std::chrono::milliseconds(hbMs);
            impl->nextAutoCommit =
                now + std::chrono::milliseconds(impl->cfg.autoCommitIntervalMs);

            // #431: heartbeat answers of earlier episodes (sent before this Sync) are no longer applied.
            impl->hbSeqLo = impl->ctlSeq + 1;
            if (impl->hbSeqLo == 0) impl->hbSeqLo = 1;
            impl->state = GroupConsumerState::Stable;

            // (TX-5 CRITICAL 2) Send Subscribe per assigned partition so the broker
            // applies the correct isolation level when delivering Records.
            if (routed_()) {
                // A new generation re-seeds every position from the committed
                // offsets (above): every subscription restarts — a leader-node
                // one is unsubscribed and re-made from the committed offset once
                // the OffsetFetch answers (records it pushed while this member
                // was not Stable were dropped, never consumed), the coordinator's
                // group subscriptions (possibly re-created) get their carrier.
                dropAllFetch_(*impl);
                (void)reconcileImpl_(*impl);
            } else {
                sendSubscribeForPartitions(*impl, sessionId, sendFn);
            }

            // (#1a) Seed positions from broker-committed offsets immediately.
            // All assigned partitions have sentinel -1 until this response arrives.
            sendOffsetFetch(*impl, sessionId, sendFn);
        } else if (v == GroupVerdict::Refuse) {
            refuse_(*impl, refusalFor_(ec));   // C1: never a SyncGroup loop
        } else if (v == GroupVerdict::Rejoin) {
            toRejoining_(*impl);
        } else {
            rejoinWithBackoff_(*impl);
        }
        break;
    }
}

// ---------------------------------------------------------------------------
// onHeartbeatResponse — runner thread
// ---------------------------------------------------------------------------

void GroupConsumerHub::onHeartbeatResponse(const uint8_t* buf, std::size_t len,
                                            int64_t sessionId, const SendFn& sendFn)
{
    auto opt = rpc::HeartbeatResponse::decode(buf, len);
    if (!opt) return;
    const int64_t corr = opt->correlationId;
    if (!isGroupCtlCorrId(corr)) return;   // #431

    for (std::size_t ix = 0, nx = impls_.size(); ix < nx; ++ix) {  // re-entrancy safe (I-3)
        GroupConsumerImpl* const impl = impls_[ix];
        if (!impl->active) continue;
        // #431: this consumer's id, and a heartbeat of the current Stable episode (any still in flight counts).
        if (impl->state != GroupConsumerState::Stable) continue;
        if (impl->ctlId != static_cast<uint32_t>((corr >> 32) & 0xFFFFFF)) continue;
        if (static_cast<int32_t>(static_cast<uint32_t>(corr) - impl->hbSeqLo) < 0) continue;

        const auto ec = static_cast<rpc::ErrorCode>(
            static_cast<uint16_t>(opt->errorCode));

        switch (classifyGroupAnswer_(ec)) {
        case GroupVerdict::Ok:      impl->backoffRejoins = 0; break;   // fix round 3: proven healthy — budget reset
        case GroupVerdict::Rejoin:  toRejoining_(*impl); break;
        case GroupVerdict::Refuse:  refuse_(*impl, refusalFor_(ec)); break;
        // C1: never stay Stable on an error the broker could not serve — the
        // broker may have evicted this member (split-brain): rejoin, paced.
        case GroupVerdict::Backoff: rejoinWithBackoff_(*impl); break;
        }
        break;
    }

    (void)sessionId; (void)sendFn;
}

// ---------------------------------------------------------------------------
// onOffsetCommitResponse — runner thread
// ---------------------------------------------------------------------------

void GroupConsumerHub::onOffsetCommitResponse(const uint8_t* buf, std::size_t len)
{
    auto opt = rpc::OffsetCommitResponse::decode(buf, len);
    if (!opt) return;

    for (std::size_t ix = 0, nx = impls_.size(); ix < nx; ++ix) {  // re-entrancy safe (I-3)
        GroupConsumerImpl* const impl = impls_[ix];
        if (!impl->active) continue;
        // fix1 I6: only the response to THIS commitSync's OffsetCommit (echoed
        // correlationId) resolves it — never an auto-commit / plain commit
        // response (correlationId 0) or a stale one.
        if (!impl->hasCommitSync || impl->commitSyncCorrId == 0 ||
            opt->correlationId != impl->commitSyncCorrId) {
            continue;
        }
        int16_t err = 0;
        bool    unstable = false;
        for (std::size_t i = 0; i < opt->results.size(); ++i) {
            const auto r = opt->results[i];
            if (r.errorCode == static_cast<int16_t>(rpc::ErrorCode::UnstableOffsetCommit)) { unstable = true; continue; }
            if (r.errorCode != 0) { err = r.errorCode; break; }
        }
        if (err == 0 && unstable) {
            // #438: a transaction holds a key of this commit - re-sent after a backoff (20 ms doubling to 1 s, armed
            // on the duty clock by sweepCommitSyncTimeouts), never surfaced; the commitSync deadline still bounds it.
            impl->commitSyncCorrId = 0;
            impl->commitSync.retryBackoffNs = impl->commitSync.retryBackoffNs == 0
                ? kCommitRetryMinNs : std::min<int64_t>(impl->commitSync.retryBackoffNs * 2, kCommitRetryMaxNs);
            impl->commitSync.retryDueNs = 0;   // armed by the next sweep
            break;
        }
        if (err != 0) {
            completeCommitSync_(*impl, OpStatus::BrokerError, static_cast<uint16_t>(err));
        } else {
            completeCommitSync_(*impl, OpStatus::Ok);
        }
        break;
    }
}

// ---------------------------------------------------------------------------
// onOffsetFetchResponse — runner thread
// Seeds positions after join from broker-committed offsets.
// ---------------------------------------------------------------------------

void GroupConsumerHub::onOffsetFetchResponse(const uint8_t* buf, std::size_t len)
{
    auto opt = rpc::OffsetFetchResponse::decode(buf, len);
    if (!opt) return;

    for (std::size_t ix = 0, nx = impls_.size(); ix < nx; ++ix) {  // re-entrancy safe (I-3)
        GroupConsumerImpl* const impl = impls_[ix];
        if (!impl->active) continue;
        if (impl->state != GroupConsumerState::Stable) continue;
        // No-hang fix round 2 (I2): only the answer to THIS consumer's latest
        // OffsetFetch (echoed correlationId) — never the first Stable consumer's
        // by accident, never a stale answer.
        if (impl->offsetFetchCorrId == 0 || opt->correlationId != impl->offsetFetchCorrId) continue;
        impl->offsetFetchPhase = GroupConsumerImpl::OffsetFetchPhase::Idle;   // answered: none in flight

        bool refused = false;
        GroupFailReason refusedWhy = GroupFailReason::RefusedByBroker;
        bool rejoin = false;
        bool retriableError = false;
        bool unstable = false;   // #438: an unfinished transaction holds the key - waited out, never a budget
        // (#1a) Seed positions from committed offsets.
        // We seed unconditionally for partitions that are still at the sentinel
        // (-1 = never seeded) OR that haven't received any Records yet (still
        // at the committed value from a prior OffsetFetch). We do NOT seed
        // partitions that already have a position advanced past the committed
        // offset by delivered Records — that would regress the position.
        for (const auto& r : opt->results) {
            if (r.errorCode != 0) {
                // I2: a whole-request rejection ({-1, -1, code}: the broker could
                // not decode / serve the OffsetFetch) or a non-retriable code
                // finishes the consumer; any other per-entry error is re-sent
                // (paced by retryOffsetFetch) within kMaxOffsetFetchAttempts.
                const auto ec = static_cast<rpc::ErrorCode>(static_cast<uint16_t>(r.errorCode));
                const GroupVerdict v = classifyGroupAnswer_(ec);
                // #438 (client version 10): the committed offset is about to change (a transaction holds the
                // key until it completes) - re-fetched after the usual pacing; it ends on its own (the
                // transaction completes or times out), so it is not charged to kMaxOffsetFetchAttempts.
                if (ec == rpc::ErrorCode::UnstableOffsetCommit && r.topicId >= 0) {
                    unstable = true;
                    continue;
                }
                if (r.topicId < 0 || v == GroupVerdict::Refuse) {
                    refused = true;
                    if (v == GroupVerdict::Refuse) refusedWhy = refusalFor_(ec);
                }
                // Fix round 4: NotCoordinator (& the rebalance set) = the coordinator
                // moved / the group is rebalancing — the member REDISCOVERS the
                // coordinator by re-joining (paced, capped: rejoinForOffsetFetch_),
                // never by re-fetching from the same node while Stable.
                else if (v == GroupVerdict::Rejoin) rejoin = true;
                else retriableError = true;
                continue;
            }
            const TP tp{r.topicId, r.partition};
            auto it = impl->assignment.find(tp);
            if (it == impl->assignment.end()) continue;
            if (routed_()) {
                // Task 10: a seek() positioned this partition explicitly — an answer never moves it.
                const auto f = impl->fetch.find(tp);
                if (f != impl->fetch.end() && f->second.rewound && f->second.offsetKnown) continue;
            }
            if (it->second == GroupConsumerImpl::kUnseededSentinel ||
                it->second < r.offset) {
                // Seed: either never seeded, or the committed offset is ahead
                // of our current position (reconnect resume scenario).
                it->second = r.offset;
            }
            if (it->second == GroupConsumerImpl::kUnseededSentinel) {
                // No committed offset: the LOG START (there is no
                // auto-offset-reset setting, #170; the broker clamps 0 up to the oldest held record; the
                // first batch then simply starts above 0, a legitimate gap).
                // E2 N6: for a transient partition whose lineage changed this
                // reads every refilled record — never Latest (silent loss).
                it->second = 0;
            }
            if (routed_()) impl->fetch[tp].offsetKnown = true;
        }
        if (refused) {
            refuse_(*impl, refusedWhy);
            break;
        }
        if (rejoin) {
            rejoinForOffsetFetch_(*impl);
            break;
        }
        // The coordinator served this fetch: the NotCoordinator streak ends.
        impl->offsetFetchRejoins = 0;
        (void)unstable;   // its partitions stay offsetKnown = false: retryOffsetFetch re-sends them, paced
        if (retriableError && impl->offsetFetchCountedCorr != opt->correlationId) {
            // Fix round 3: one budget attempt per OffsetFetch (a duplicate answer
            // of the same fetch is not counted twice; seeding stays idempotent).
            // Fix round 4: the budget is per generation (reset by SyncGroup OK
            // only) — no answer resets it, so no answer mix can loop forever.
            impl->offsetFetchCountedCorr = opt->correlationId;
            if (++impl->offsetFetchAttempts > kMaxOffsetFetchAttempts) {
                refuse_(*impl, GroupFailReason::RetryBudgetExhausted);
                break;
            }
        }
        // Leader-node subscriptions waiting for their start offset go now.
        if (routed_()) (void)reconcileImpl_(*impl);
        break;
    }
}

// ---------------------------------------------------------------------------
// onAssign (Assign frame 44) — runner thread
// ---------------------------------------------------------------------------

void GroupConsumerHub::onAssign(const uint8_t* buf, std::size_t len,
                                 int64_t sessionId, const SendFn& sendFn)
{
    auto opt = rpc::Assign::decode(buf, len);
    if (!opt) return;

    for (std::size_t ix = 0, nx = impls_.size(); ix < nx; ++ix) {  // re-entrancy safe (I-3)
        GroupConsumerImpl* const impl = impls_[ix];
        if (!impl->active) continue;
        if (impl->state != GroupConsumerState::Stable) continue;

        // Update generationId if the Assign carries a newer one.
        impl->generationId = opt->generationId;
        publishGeneration_(*impl);

        // Build new assignment from Assign frame.
        std::vector<TP> newAsgn;
        for (const auto& tp : opt->assignment) {
            newAsgn.push_back({tp.topicId, tp.partition});
        }

        applyAssignment(*impl, std::move(newAsgn));

        // (TX-5 CRITICAL 2) Re-send Subscribe frames for newly assigned partitions
        // so the broker applies the correct isolation level when delivering Records.
        if (routed_()) {
            // Kept subscriptions stay (Stable throughout); revoked ones are
            // unsubscribed, added ones subscribed once their offset is known.
                (void)reconcileImpl_(*impl);
        } else {
            sendSubscribeForPartitions(*impl, sessionId, sendFn);
        }

        // (#1a) Seed positions for any newly added partitions from broker offsets.
        sendOffsetFetch(*impl, sessionId, sendFn);
        break;
    }
}

// ---------------------------------------------------------------------------
// onConnected — called when ConnectResponse{OK} arrives
// ---------------------------------------------------------------------------

// Phase 2c: a filtered group's fetches are plain Subscribes at the partition leader (routed fetch).  The legacy
// non-routed path reaches the broker's group carrier Subscribe, which refuses a filter — the group ends
// FilterMismatch rather than ever delivering unfiltered records.
bool GroupConsumerHub::filterNeedsRouting_(GroupConsumerImpl& impl)
{
    if (impl.cfg.filterBytes.empty() || routed_()) return false;
    refuse_(impl, GroupFailReason::FilterMismatch);
    return true;
}

void GroupConsumerHub::refuse_(GroupConsumerImpl& impl, GroupFailReason why)
{
    if (!impl.active) return;
    std::fprintf(stderr,
        "rune-client: group '%s' cannot run: %s — the consumer is finished\n",
        impl.groupId.c_str(),
        why == GroupFailReason::BrokerTooOld
            ? "the broker does not advertise explicit group subscriptions (client and broker must both be "
              "version >= 3)"
        : why == GroupFailReason::RetryBudgetExhausted
            ? "a retriable coordinator failure persisted past its retry budget (transient: recreate the consumer)"
        : why == GroupFailReason::NotAuthorized
            ? "NotAuthorized: the principal holds no READ on the group / topic (ask an admin for the grant)"
        : why == GroupFailReason::SessionRevoked
            ? "SessionRevoked: the session's user was dropped or disabled"
        : why == GroupFailReason::FilterMismatch
            ? "FilterMismatch: the group runs with another filter, or this client cannot apply one (a filtered "
              "group needs the Client's routed fetch)"
            : "the broker coordinator refused this consumer's request (non-retriable answer)");
    if (routed_()) dropAllFetch_(impl);
    impl.failReason = why;
    finish_(impl);
}

void GroupConsumerHub::onCoordinatorRejected(int16_t code)
{
    const GroupFailReason why = refusalFor_(static_cast<rpc::ErrorCode>(static_cast<uint16_t>(code)));
    for (std::size_t ix = 0, nx = impls_.size(); ix < nx; ++ix) {  // re-entrancy safe (I-3)
        GroupConsumerImpl* const impl = impls_[ix];
        if (impl->active) refuse_(*impl, why);
    }
}

void GroupConsumerHub::onConnected(int64_t sessionId, const SendFn& sendFn)
{
    for (std::size_t ix = 0, nx = impls_.size(); ix < nx; ++ix) {  // re-entrancy safe (I-3)
        GroupConsumerImpl* const impl = impls_[ix];
        if (!impl->active) continue;
        // MAJOR-2 (round 3): never against a broker that predates explicit
        // group subscriptions (it would auto-subscribe and lose records).
        if (!gateAllows_(*impl)) continue;   // #440: Refuse ends it, Defer waits
        if (filterNeedsRouting_(*impl)) continue;

        // fix1 I6: a NEW session (reconnect) means the old one was lost — a
        // commitSync whose OffsetCommit went out on it can never be answered.
        if (impl->commitSyncCorrId != 0) completeCommitSync_(*impl, OpStatus::SessionLost);

        if (impl->state == GroupConsumerState::Unjoined  ||
            impl->state == GroupConsumerState::Rejoining) {
            (void)tryJoin_(*impl, sessionId, sendFn);
        } else if ((impl->state == GroupConsumerState::Joining ||
                    impl->state == GroupConsumerState::Syncing) &&
                   impl->joinSessionId != sessionId) {
            // D4 (first review MAJOR-1): a Join / Sync in flight on ANOTHER
            // session (a coordinator move, a reconnect mid-join, or held — never
            // sent — while this session connected) is never answered here:
            // re-join on this one now instead of idling until the join deadline.
            // One that went out on THIS session (a resume that kept its id) is
            // left alone: the coordinator may hold it (a rebalance), and a
            // second JoinGroup with an empty memberId would register a ghost
            // member (review MEDIUM-3).  After a session id change the held
            // join's answer is addressed to the old session (lost, or taken by
            // the re-join); a ghost from the old join costs one rebalance after
            // sessionTimeoutMs — the same as waiting out the join deadline (the
            // broker reaps members by sessionTimeoutMs, not with the client
            // session; KIP-394 MEMBER_ID_REQUIRED is the real fix — ticket).
            toRejoining_(*impl);
            (void)tryJoin_(*impl, sessionId, sendFn);
        }
    }
}

// ---------------------------------------------------------------------------
// onCoordinatorMoved — D4: the coordinator is now on another session (the
// client's coordinator route moved); the seed session never moves for it
// ---------------------------------------------------------------------------

void GroupConsumerHub::onCoordinatorMoved()
{
    for (std::size_t ix = 0, nx = impls_.size(); ix < nx; ++ix) {  // re-entrancy safe (I-3)
        GroupConsumerImpl* const impl = impls_[ix];
        if (!impl->active) continue;
        if (impl->state == GroupConsumerState::Unjoined || impl->state == GroupConsumerState::Rejoining) continue;
        toRejoining_(*impl);   // Joining / Syncing / Stable: re-join on the coordinator session (doWork / onConnected)
    }
}

// ---------------------------------------------------------------------------
// closeAll — called on Client::close()
// ---------------------------------------------------------------------------

void GroupConsumerHub::closeAll(int64_t sessionId, const SendFn& sendFn)
{
    for (std::size_t ix = 0, nx = impls_.size(); ix < nx; ++ix) {  // re-entrancy safe (I-3)
        GroupConsumerImpl* const impl = impls_[ix];
        if (!impl->active) continue;

        // Fire revoked for everything. fix1 C1: catch/count/log.
        if (!impl->assignment.empty() && impl->cbRevoked) {
            std::vector<TP> all;
            for (const auto& [tp, _] : impl->assignment) {
                all.push_back(tp);
            }
            DispatchScope scope(*this, *impl);
            try {
                impl->cbRevoked(all);
            } catch (const std::exception& e) {
                ++callbackExceptions_;
                std::fprintf(stderr,
                    "rune-client: group onRevoked (close) threw: %s (caught)\n", e.what());
            } catch (...) {
                ++callbackExceptions_;
                std::fprintf(stderr,
                    "rune-client: group onRevoked (close) threw a non-std::exception (caught)\n");
            }
        }
        // fix4 M4: the callback may have closed this consumer (its close sent
        // the LeaveGroup and finished it) — never a second LeaveGroup.
        if (!impl->active) continue;

        // Send LeaveGroup (best-effort; may fail if transport is down).
        if (!impl->memberId.empty()) {
            const auto grpSpan = strSpan(impl->groupId);
            const auto mbrSpan = strSpan(impl->memberId);

            rpc::LeaveGroup msg;
            msg.sessionId     = sessionId;
            msg.correlationId = 0;
            msg.groupId       = grpSpan;
            msg.memberId      = mbrSpan;

            const std::size_t needed = msg.encodedSize();
            ensureBuf(*impl, needed);
            const std::size_t len = msg.encode(impl->sendBuf.data());
            if (len > 0) sendFn(0, impl->sendBuf.data(), len);
        }

        // Inactive + complete any pending commitSync Closed + close the status ring.
        finish_(*impl);
    }

    // Consumers already closed by their handle still get their ring closed
    // (idempotent) — every handle must learn the Client is gone.
    for (std::size_t ix = 0, nx = impls_.size(); ix < nx; ++ix) closeStatus_(*impls_[ix]);

    (void)sessionId; (void)sendFn;
}

} // namespace rune::client::group
