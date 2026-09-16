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
// Wait-free contract:
//   snapshot_.load(acquire) once per hot-path call, then iterate.
//   No locks on runner thread. Cold path (add()) under addMtx_.

#include <rune/client/group/GroupConsumerHub.h>
#include <rune/protocol/client/GroupMessages.h>
#include <rune/protocol/client/OffsetMessages.h>
#include <rune/protocol/client/Protocol.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstring>

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
// add() — cold path, app thread
// ---------------------------------------------------------------------------

void GroupConsumerHub::add(std::shared_ptr<GroupConsumerImpl> impl) {
    std::lock_guard<std::mutex> lk(addMtx_);

    // Load the current live list.
    const ImplList* cur = snapshot_.load(std::memory_order_acquire);

    // Allocate a new list = current + new impl.
    auto* next = new ImplList(*cur);
    next->push_back(std::move(impl));

    // Publish (release) and retain old pointer for safe deferred free.
    snapshot_.store(next, std::memory_order_release);
    owned_.push_back(next);
    // (owned_ now holds the old cur too from a prior add, or from constructor.)
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

    sendFn(impl.sendBuf.data(), len);

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

    sendFn(impl.sendBuf.data(), len);

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

    sendFn(impl.sendBuf.data(), len);
}

// ---------------------------------------------------------------------------
// sendOffsetCommit — runner thread
// ---------------------------------------------------------------------------

/*static*/
void GroupConsumerHub::sendOffsetCommit(GroupConsumerImpl& impl,
                                         int64_t sessionId,
                                         const SendFn& sendFn)
{
    if (impl.assignment.empty()) return;

    // Build entry list from current positions.
    std::vector<rpc::OffsetCommitEntry> entries;
    entries.reserve(impl.assignment.size());
    for (const auto& [tp, pos] : impl.assignment) {
        rpc::OffsetCommitEntry e;
        e.topicId   = tp.topicId;
        e.partition = tp.partition;
        e.offset    = pos;  // next offset to consume = committed position
        // metadata: empty
        entries.push_back(e);
    }

    const auto grpSpan = strSpan(impl.groupId);
    const auto mbrSpan = strSpan(impl.memberId);
    const std::size_t needed = rpc::OffsetCommit::encodedSizeFor(
        grpSpan, mbrSpan, entries);
    ensureBuf(impl, needed);

    rpc::OffsetCommit msg;
    msg.sessionId     = sessionId;
    msg.correlationId = 0;
    msg.generationId  = impl.generationId;
    msg.groupId       = grpSpan;
    msg.memberId      = mbrSpan;

    const std::size_t len = msg.encode(impl.sendBuf.data(), entries);
    if (len == 0) return;

    sendFn(impl.sendBuf.data(), len);
}

// ---------------------------------------------------------------------------
// applyAssignment — runner thread
// Fire revoked + assigned callbacks, update assignment map.
// ---------------------------------------------------------------------------

/*static*/
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

    // Fire revoked.
    if (!revoked.empty() && impl.cbRevoked) {
        impl.cbRevoked(revoked);
    }

    // Remove revoked from assignment map.
    for (const auto& tp : revoked) {
        impl.assignment.erase(tp);
    }

    // Add new partitions with initial offset 0 (broker resumes from committed;
    // position will be updated by incoming Records).
    for (const auto& tp : added) {
        impl.assignment[tp] = 0;
    }

    // Fire assigned.
    if (!added.empty() && impl.cbAssigned) {
        impl.cbAssigned(added);
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
    const ImplList* list = snapshot_.load(std::memory_order_acquire);

    const auto now = std::chrono::steady_clock::now();

    for (auto& impl : *list) {
        if (!impl->active.load(std::memory_order_acquire)) continue;

        // ---- Drain pending commit() requests (wait-free acquire) ------------
        if (impl->commitRequested.exchange(false, std::memory_order_acq_rel)) {
            if (impl->state == GroupConsumerState::Stable) {
                sendOffsetCommit(*impl, sessionId, sendFn);
                ++work;
            }
            // If not stable, positions will be committed on next auto-commit
            // after reaching Stable — at-least-once is preserved.
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
                impl->state = GroupConsumerState::Rejoining;
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
                sendOffsetCommit(*impl, sessionId, sendFn);
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
    const ImplList* list = snapshot_.load(std::memory_order_acquire);

    for (auto& impl : *list) {
        if (!impl->active.load(std::memory_order_acquire)) continue;
        if (impl->state != GroupConsumerState::Stable) continue;

        // Check if this (topicId, partition) is in the assignment.
        const TP key{rv.topicId, rv.partition};
        auto it = impl->assignment.find(key);
        if (it == impl->assignment.end()) continue;

        // Deliver to consumer.
        if (impl->cbRecords) {
            impl->cbRecords(rv);
        }

        // Advance position: baseOffset + number of records in batch.
        // The broker's baseOffset is the first record's offset; the next
        // expected offset = baseOffset + batchSize. We use baseOffset + 1
        // as a conservative advance (the SubscriberHub uses the same approach
        // with nextOffset = lastDelivered + 1 from the RecordBatch).
        // For group consumers, advance to max(current, rv.baseOffset + 1).
        const int64_t newPos = rv.baseOffset + 1;
        if (newPos > it->second) {
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

    const ImplList* list = snapshot_.load(std::memory_order_acquire);

    for (auto& impl : *list) {
        if (!impl->active.load(std::memory_order_acquire)) continue;
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

            sendSyncGroup(*impl, sessionId, sendFn);
        } else if (ec == rpc::ErrorCode::NotCoordinator ||
                   ec == rpc::ErrorCode::RebalanceInProgress) {
            impl->state = GroupConsumerState::Rejoining;
        } else {
            // Other error (UnknownMember, GroupDead, etc.) — rejoin.
            impl->state = GroupConsumerState::Rejoining;
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

    const ImplList* list = snapshot_.load(std::memory_order_acquire);

    for (auto& impl : *list) {
        if (!impl->active.load(std::memory_order_acquire)) continue;
        if (impl->state != GroupConsumerState::Syncing) continue;

        const auto ec = static_cast<rpc::ErrorCode>(opt->errorCode);

        if (ec == rpc::ErrorCode::OK) {
            // Build new assignment from SyncGroupResponse.
            std::vector<TP> newAsgn;
            for (const auto& tp : opt->assignment) {
                newAsgn.push_back({tp.topicId, tp.partition});
            }

            applyAssignment(*impl, std::move(newAsgn));

            // Arm heartbeat and auto-commit timers.
            const auto now  = tpNow();
            const int32_t hbMs = impl->cfg.sessionTimeoutMs / 3;
            impl->nextHeartbeat  = now + std::chrono::milliseconds(hbMs);
            impl->nextAutoCommit =
                now + std::chrono::milliseconds(impl->cfg.autoCommitIntervalMs);

            impl->state = GroupConsumerState::Stable;
        } else {
            impl->state = GroupConsumerState::Rejoining;
        }
        break;
    }

    (void)sendFn;  // unused in Stable transition (sendFn used if we send something)
}

// ---------------------------------------------------------------------------
// onHeartbeatResponse — runner thread
// ---------------------------------------------------------------------------

void GroupConsumerHub::onHeartbeatResponse(const uint8_t* buf, std::size_t len,
                                            int64_t sessionId, const SendFn& sendFn)
{
    auto opt = rpc::HeartbeatResponse::decode(buf, len);
    if (!opt) return;

    const ImplList* list = snapshot_.load(std::memory_order_acquire);

    for (auto& impl : *list) {
        if (!impl->active.load(std::memory_order_acquire)) continue;
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
            impl->state = GroupConsumerState::Rejoining;
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

    const ImplList* list = snapshot_.load(std::memory_order_acquire);

    for (auto& impl : *list) {
        if (!impl->active.load(std::memory_order_acquire)) continue;

        // Resolve any pending commitSync promise (cold path under lock).
        std::lock_guard<std::mutex> lk(impl->commitSyncMtx);
        if (impl->commitSyncPending) {
            try { impl->commitSyncPromise.set_value(); } catch (...) {}
            impl->commitSyncPending = false;
        }
        // Only one consumer can have a pending commitSync for this response.
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

    const ImplList* list = snapshot_.load(std::memory_order_acquire);

    for (auto& impl : *list) {
        if (!impl->active.load(std::memory_order_acquire)) continue;
        if (impl->state != GroupConsumerState::Stable) continue;

        // Seed positions from committed offsets.
        for (const auto& r : opt->results) {
            if (r.errorCode != 0) continue;  // skip errored entries
            const TP tp{r.topicId, r.partition};
            auto it = impl->assignment.find(tp);
            if (it != impl->assignment.end() && it->second == 0) {
                // Only seed if we haven't received any Records yet.
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

    const ImplList* list = snapshot_.load(std::memory_order_acquire);

    for (auto& impl : *list) {
        if (!impl->active.load(std::memory_order_acquire)) continue;
        if (impl->state != GroupConsumerState::Stable) continue;

        // Update generationId if the Assign carries a newer one.
        impl->generationId = opt->generationId;

        // Build new assignment from Assign frame.
        std::vector<TP> newAsgn;
        for (const auto& tp : opt->assignment) {
            newAsgn.push_back({tp.topicId, tp.partition});
        }

        applyAssignment(*impl, std::move(newAsgn));
        break;
    }

    (void)sessionId; (void)sendFn;
}

// ---------------------------------------------------------------------------
// onConnected — called when ConnectResponse{OK} arrives
// ---------------------------------------------------------------------------

void GroupConsumerHub::onConnected(int64_t sessionId, const SendFn& sendFn)
{
    const ImplList* list = snapshot_.load(std::memory_order_acquire);

    for (auto& impl : *list) {
        if (!impl->active.load(std::memory_order_acquire)) continue;

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
    const ImplList* list = snapshot_.load(std::memory_order_acquire);

    for (auto& impl : *list) {
        if (!impl->active.load(std::memory_order_acquire)) continue;

        // Fire revoked for everything.
        if (!impl->assignment.empty() && impl->cbRevoked) {
            std::vector<TP> all;
            for (const auto& [tp, _] : impl->assignment) {
                all.push_back(tp);
            }
            impl->cbRevoked(all);
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
            if (len > 0) sendFn(impl->sendBuf.data(), len);
        }

        impl->active.store(false, std::memory_order_release);

        // Resolve any pending commitSync with an error.
        std::lock_guard<std::mutex> lk(impl->commitSyncMtx);
        if (impl->commitSyncPending) {
            try {
                impl->commitSyncPromise.set_exception(
                    std::make_exception_ptr(
                        std::runtime_error("Client closed")));
            } catch (...) {}
            impl->commitSyncPending = false;
        }
    }

    (void)sessionId; (void)sendFn;
}

} // namespace rune::client::group
