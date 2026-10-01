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

#include <rune/client/group/GroupConsumerHub.h>
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
#include <future>
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

void GroupConsumerHub::add(std::shared_ptr<GroupConsumerImpl> impl) {
    if (impl) impls_.push_back(std::move(impl));
}

GroupConsumerImpl* GroupConsumerHub::find_(uint64_t handle) const noexcept {
    for (const auto& impl : impls_) {
        if (reinterpret_cast<uint64_t>(impl.get()) == handle) return impl.get();
    }
    return nullptr;
}

/*static*/
void GroupConsumerHub::failCommitSync_(GroupConsumerImpl& impl, const char* why) {
    if (!impl.commitSyncPromise) return;
    try {
        impl.commitSyncPromise->set_exception(std::make_exception_ptr(std::runtime_error(why)));
    } catch (...) {}
    impl.commitSyncPromise.reset();
    impl.commitSyncCorrId = 0;
}

/*static*/
void GroupConsumerHub::closeStatus_(GroupConsumerImpl& impl) {
    if (impl.status.producerClosed()) return;
    // Final snapshot (the reserve guarantees it fits), THEN close.
    (void)impl.status.publishFinal(GroupConsumerStatus{impl.generationId, 0});
    impl.status.close();
}

void GroupConsumerHub::finish_(GroupConsumerImpl& impl, const char* why) {
    impl.active = false;
    failCommitSync_(impl, why);
    if (impl.statusDirty) { impl.statusDirty = false; --dirtyCount_; }
    // A callback of this consumer is running (it closed itself): the ring is
    // closed when the callback returns (DispatchScope), so finished() never
    // turns true while a callback still executes.
    if (&impl == dispatching_) return;
    closeStatus_(impl);
}

void GroupConsumerHub::toRejoining_(GroupConsumerImpl& impl) {
    impl.state = GroupConsumerState::Rejoining;
    failCommitSync_(impl, "commitSync() aborted: the group is rebalancing (rejoin)");
}

void GroupConsumerHub::publishGeneration_(GroupConsumerImpl& impl) {
    if (impl.status.tryPublish(GroupConsumerStatus{impl.generationId, 0})) {
        if (impl.statusDirty) { impl.statusDirty = false; --dirtyCount_; }
    } else if (!impl.status.closed() && !impl.statusDirty) {
        impl.statusDirty = true;
        ++dirtyCount_;
    }
}

int GroupConsumerHub::republishDirty() {
    int n = 0;
    for (auto& impl : impls_) {
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

void GroupConsumerHub::applyCommand(const detail::HubCommand& cmd) {
    using Op = detail::HubCommand::Op;
    switch (cmd.op) {
    case Op::GroupAdd: {
        auto* holder = reinterpret_cast<std::shared_ptr<GroupConsumerImpl>*>(cmd.handle);
        if (holder) {
            add(std::move(*holder));
            delete holder;
        }
        break;
    }
    case Op::GroupCommit: {
        GroupConsumerImpl* impl = find_(cmd.handle);
        if (impl && impl->active) impl->commitRequested = true;
        break;
    }
    case Op::GroupCommitSync: {
        std::unique_ptr<std::promise<void>> prom(
            reinterpret_cast<std::promise<void>*>(static_cast<uintptr_t>(cmd.a)));
        GroupConsumerImpl* impl = find_(cmd.handle);
        if (!impl || !impl->active) {
            if (prom) {
                try {
                    prom->set_exception(std::make_exception_ptr(
                        std::runtime_error("GroupConsumer closed")));
                } catch (...) {}
            }
            break;
        }
        // Already one in flight — fail the old one and replace.
        failCommitSync_(*impl, "commitSync() superseded by a newer call");
        impl->commitSyncPromise = std::move(prom);
        impl->commitRequested   = true;  // send the commit promptly
        break;
    }
    case Op::GroupClose: {
        GroupConsumerImpl* impl = find_(cmd.handle);
        if (impl && impl->active) finish_(*impl, "GroupConsumer closed");
        break;
    }
    case Op::GroupSetCallback: {
        GroupConsumerImpl* impl = find_(cmd.handle);
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

    const std::size_t needed = rpc::JoinGroup::encodedSizeFor(
        grpSpan, mbrSpan, impl.topicIds.size());

    ensureBuf(impl, needed);

    rpc::JoinGroup msg;
    msg.sessionId        = sessionId;
    msg.correlationId    = 0;  // broker ignores; matched by state
    msg.sessionTimeoutMs = impl.cfg.sessionTimeoutMs;
    msg.groupId          = grpSpan;
    msg.memberId         = mbrSpan;

    const std::size_t len = msg.encode(impl.sendBuf.data(),
                                        std::span<const int32_t>(impl.topicIds));
    if (len == 0) return;

    sendFn(0, impl.sendBuf.data(), len);

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
    msg.correlationId = 0;
    msg.generationId  = impl.generationId;
    msg.groupId       = grpSpan;
    msg.memberId      = mbrSpan;

    const std::size_t exact = msg.encodedSize();
    ensureBuf(impl, exact);

    const std::size_t len = msg.encode(impl.sendBuf.data());
    if (len == 0) return;

    sendFn(0, impl.sendBuf.data(), len);

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
    msg.correlationId = 0;
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

/*static*/
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

    return sendFn(0, impl.sendBuf.data(), len) ? CommitSend::Sent : CommitSend::SendFailed;
}

// ---------------------------------------------------------------------------
// sendOffsetFetch — runner thread
// Send OffsetFetch for all assigned partitions to seed positions from the
// broker's committed offsets.  Called after entering Stable (onSyncGroupResponse)
// and after applyAssignment adds new partitions.
// ---------------------------------------------------------------------------

/*static*/
void GroupConsumerHub::sendOffsetFetch(GroupConsumerImpl& impl,
                                        int64_t sessionId,
                                        const SendFn& sendFn)
{
    if (impl.assignment.empty()) return;

    // Build entry list from current assignment.
    std::vector<rpc::OffsetFetchEntry> entries;
    entries.reserve(impl.assignment.size());
    for (const auto& [tp, _] : impl.assignment) {
        rpc::OffsetFetchEntry e;
        e.topicId   = tp.topicId;
        e.partition = tp.partition;
        entries.push_back(e);
    }

    const auto grpSpan = strSpan(impl.groupId);
    const std::size_t needed = rpc::OffsetFetch::encodedSizeFor(grpSpan, entries.size());
    ensureBuf(impl, needed);

    rpc::OffsetFetch msg;
    msg.sessionId     = sessionId;
    msg.correlationId = 0;
    msg.groupId       = grpSpan;

    const std::size_t len = msg.encode(impl.sendBuf.data(), entries);
    if (len == 0) return;

    sendFn(0, impl.sendBuf.data(), len);
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

    for (auto& impl : impls_) {
        if (!impl->active) continue;

        // ---- Drain pending commit() / commitSync() requests --------------------
        // Kept pending until Stable (a commitSync waits for it; a rejoin fails it).
        if (impl->commitRequested && impl->state == GroupConsumerState::Stable) {
            impl->commitRequested = false;
            const bool forSync = impl->commitSyncPromise && impl->commitSyncCorrId == 0;
            const int64_t corr = forSync ? nextCommitCorrId_++ : 0;
            const CommitSend r = sendOffsetCommit(*impl, sessionId, sendFn, corr);
            if (r == CommitSend::SendFailed) {
                impl->commitRequested = true;  // retry next cycle (never silently lost)
            } else if (forSync) {
                if (r == CommitSend::Sent) {
                    impl->commitSyncCorrId = corr;  // resolved by the matching response
                } else {
                    // Nothing to commit (no seeded position): done.
                    try { impl->commitSyncPromise->set_value(); } catch (...) {}
                    impl->commitSyncPromise.reset();
                }
            }
            ++work;
        }

        switch (impl->state) {

        case GroupConsumerState::Unjoined:
            // Not yet joined — wait for onConnected() to trigger.
            break;

        case GroupConsumerState::Joining:
            // Awaiting JoinGroupResponse. Guard against stale join (timeout).
            if (now >= impl->joinDeadline) {
                // Re-send JoinGroup (rejoin guard expired).
                sendJoinGroup(*impl, sessionId, sendFn);
                ++work;
            }
            break;

        case GroupConsumerState::Syncing:
            // Awaiting SyncGroupResponse. Guard with join deadline.
            if (now >= impl->joinDeadline) {
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
            // Re-send JoinGroup immediately.
            sendJoinGroup(*impl, sessionId, sendFn);
            ++work;
            break;
        }
    }

    return work;
}

// ---------------------------------------------------------------------------
// onRecords — runner thread (hot path)
// ---------------------------------------------------------------------------

void GroupConsumerHub::onRecords(const codec::RecordsView& rv)
{
    for (auto& impl : impls_) {
        if (!impl->active) continue;
        if (impl->state != GroupConsumerState::Stable) continue;

        // Check if this (topicId, partition) is in the assignment.
        const TP key{rv.topicId, rv.partition};
        auto it = impl->assignment.find(key);
        if (it == impl->assignment.end()) continue;

        // Deliver to consumer. fix1 C1: noexcept doWork chain — catch/count/log.
        if (impl->cbRecords) {
            DispatchScope scope(*this, *impl);
            try {
                impl->cbRecords(rv);
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

        // (#2) Advance position by the actual batch record count:
        //   newPos = baseOffset + recordCount  (== baseOffset + lastOffsetDelta + 1)
        // Using +1 was wrong for multi-record batches and caused re-delivery of
        // records B+1..B+N-1 after any rebalance or reconnect.
        // rv.recordCount is parsed from the RecordBatch header in decodeRecords();
        // falls back to 1 for empty/stub batches (test helpers, no real batch bytes).
        // Also handles seeding: if the partition was at the sentinel, the first
        // Records delivery itself acts as a position seed — safe because the broker
        // sent us a real record, so we can safely advance from there.
        const int64_t newPos = rv.baseOffset + static_cast<int64_t>(rv.recordCount);
        if (newPos > it->second || it->second == GroupConsumerImpl::kUnseededSentinel) {
            it->second = newPos;
        }

        // Only deliver to ONE consumer per Records frame (partitions are
        // exclusively assigned within a group).
        break;
    }
}

// ---------------------------------------------------------------------------
// onJoinGroupResponse — runner thread
// ---------------------------------------------------------------------------

void GroupConsumerHub::onJoinGroupResponse(const uint8_t* buf, std::size_t len,
                                            int64_t sessionId, const SendFn& sendFn)
{
    auto opt = rpc::JoinGroupResponse::decode(buf, len);
    if (!opt) return;

    for (auto& impl : impls_) {
        if (!impl->active) continue;
        if (impl->state != GroupConsumerState::Joining) continue;

        // Match by groupId is implicit: only one consumer per hub is in
        // Joining state for a given connection. (Multi-group: match by corrId
        // once JoinGroup carries a per-consumer correlationId — for now we
        // support one group per connection, matching Kafka client convention.)
        const auto ec = static_cast<rpc::ErrorCode>(opt->errorCode);

        if (ec == rpc::ErrorCode::OK) {
            // Store memberId and generationId.
            impl->memberId    = std::string(
                reinterpret_cast<const char*>(opt->memberId.data()),
                opt->memberId.size());
            impl->generationId = opt->generationId;
            publishGeneration_(*impl);

            sendSyncGroup(*impl, sessionId, sendFn);
        } else if (ec == rpc::ErrorCode::NotCoordinator ||
                   ec == rpc::ErrorCode::RebalanceInProgress) {
            toRejoining_(*impl);
        } else {
            // Other error (UnknownMember, GroupDead, etc.) — rejoin.
            toRejoining_(*impl);
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

    for (auto& impl : impls_) {
        if (!impl->active) continue;
        if (impl->state != GroupConsumerState::Syncing) continue;

        const auto ec = static_cast<rpc::ErrorCode>(opt->errorCode);

        if (ec == rpc::ErrorCode::OK) {
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

            impl->state = GroupConsumerState::Stable;

            // (TX-5 CRITICAL 2) Send Subscribe per assigned partition so the broker
            // applies the correct isolation level when delivering Records.
            sendSubscribeForPartitions(*impl, sessionId, sendFn);

            // (#1a) Seed positions from broker-committed offsets immediately.
            // All assigned partitions have sentinel -1 until this response arrives.
            sendOffsetFetch(*impl, sessionId, sendFn);
        } else {
            toRejoining_(*impl);
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

    for (auto& impl : impls_) {
        if (!impl->active) continue;
        if (impl->state != GroupConsumerState::Stable) continue;

        const auto ec = static_cast<rpc::ErrorCode>(
            static_cast<uint16_t>(opt->errorCode));

        if (ec == rpc::ErrorCode::OK) {
            // Nothing to do — next heartbeat already armed by doWork.
        } else if (ec == rpc::ErrorCode::RebalanceInProgress ||
                   ec == rpc::ErrorCode::NotCoordinator       ||
                   ec == rpc::ErrorCode::IllegalGeneration     ||
                   ec == rpc::ErrorCode::StaleGeneration       ||
                   ec == rpc::ErrorCode::UnknownMember) {
            // Trigger rejoin.
            toRejoining_(*impl);
        }
        // Other errors: stay Stable, let next heartbeat cycle handle it.
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

    for (auto& impl : impls_) {
        if (!impl->active) continue;
        // fix1 I6: only the response to THIS commitSync's OffsetCommit (echoed
        // correlationId) resolves it — never an auto-commit / plain commit
        // response (correlationId 0) or a stale one.
        if (!impl->commitSyncPromise || impl->commitSyncCorrId == 0 ||
            opt->correlationId != impl->commitSyncCorrId) {
            continue;
        }
        int16_t err = 0;
        for (std::size_t i = 0; i < opt->results.size(); ++i) {
            const auto r = opt->results[i];
            if (r.errorCode != 0) { err = r.errorCode; break; }
        }
        if (err != 0) {
            const std::string why = "commitSync() failed: OffsetCommit errorCode " + std::to_string(err);
            failCommitSync_(*impl, why.c_str());
        } else {
            try { impl->commitSyncPromise->set_value(); } catch (...) {}
            impl->commitSyncPromise.reset();
            impl->commitSyncCorrId = 0;
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

    for (auto& impl : impls_) {
        if (!impl->active) continue;
        if (impl->state != GroupConsumerState::Stable) continue;

        // (#1a) Seed positions from committed offsets.
        // We seed unconditionally for partitions that are still at the sentinel
        // (-1 = never seeded) OR that haven't received any Records yet (still
        // at the committed value from a prior OffsetFetch). We do NOT seed
        // partitions that already have a position advanced past the committed
        // offset by delivered Records — that would regress the position.
        for (const auto& r : opt->results) {
            if (r.errorCode != 0) continue;  // skip errored entries
            const TP tp{r.topicId, r.partition};
            auto it = impl->assignment.find(tp);
            if (it == impl->assignment.end()) continue;
            if (it->second == GroupConsumerImpl::kUnseededSentinel ||
                it->second < r.offset) {
                // Seed: either never seeded, or the committed offset is ahead
                // of our current position (reconnect resume scenario).
                it->second = r.offset;
            }
        }
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

    for (auto& impl : impls_) {
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
        sendSubscribeForPartitions(*impl, sessionId, sendFn);

        // (#1a) Seed positions for any newly added partitions from broker offsets.
        sendOffsetFetch(*impl, sessionId, sendFn);
        break;
    }
}

// ---------------------------------------------------------------------------
// onConnected — called when ConnectResponse{OK} arrives
// ---------------------------------------------------------------------------

void GroupConsumerHub::onConnected(int64_t sessionId, const SendFn& sendFn)
{
    for (auto& impl : impls_) {
        if (!impl->active) continue;

        // fix1 I6: a NEW session (reconnect) means the old one was lost — a
        // commitSync whose OffsetCommit went out on it can never be answered.
        if (impl->commitSyncCorrId != 0) {
            failCommitSync_(*impl, "commitSync() aborted: the session was lost before the commit was acknowledged");
        }

        if (impl->state == GroupConsumerState::Unjoined  ||
            impl->state == GroupConsumerState::Rejoining) {
            sendJoinGroup(*impl, sessionId, sendFn);
        }
    }
}

// ---------------------------------------------------------------------------
// closeAll — called on Client::close()
// ---------------------------------------------------------------------------

void GroupConsumerHub::closeAll(int64_t sessionId, const SendFn& sendFn)
{
    for (auto& impl : impls_) {
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

        // Inactive + fail any pending commitSync + close the status ring.
        finish_(*impl, "Client closed");
    }

    // Consumers already closed by their handle still get their ring closed
    // (idempotent) — every handle must learn the Client is gone.
    for (auto& impl : impls_) closeStatus_(*impl);

    (void)sessionId; (void)sendFn;
}

} // namespace rune::client::group
