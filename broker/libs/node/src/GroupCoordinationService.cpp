// ---------------------------------------------------------------------------
// GroupCoordinationService.cpp — CG-3 implementation.
//
// Single-writer. No locks. No per-cycle allocation in steady state.
// See GroupCoordinationService.h for design notes.
// ---------------------------------------------------------------------------

#include <rune/node/GroupCoordinationService.h>

#include <rune/gateway/ClientGateway.h>  // CG-4: addGroupSubscription / revokeGroupSubscriptions / sendControlFrame

#include <rune/protocol/client/GroupMessages.h>
#include <rune/protocol/client/OffsetMessages.h>
#include <rune/protocol/client/Protocol.h>
#include <rune/node/NodeConfig.h>  // TopicSpec (for TopicRegistry lookup)

#include <spdlog/spdlog.h>

#include <algorithm>
#include <cstring>

namespace rune::node {

namespace pc = rune::protocol::client;
namespace coord = rune::coordination;

// ---------------------------------------------------------------------------
// Helper: convert coordinator error codes → protocol ErrorCode (int16_t)
// ---------------------------------------------------------------------------
static int16_t joinErrorToProto(int coordErr) noexcept {
    switch (coordErr) {
        case coord::JoinResult::kErrDead: return static_cast<int16_t>(pc::ErrorCode::GroupDead);
        default:                          return static_cast<int16_t>(pc::ErrorCode::OK);
    }
}

static int16_t heartbeatErrorToProto(int coordErr) noexcept {
    switch (coordErr) {
        case coord::HeartbeatResult::kOk:                   return static_cast<int16_t>(pc::ErrorCode::OK);
        case coord::HeartbeatResult::kErrUnknownMember:     return static_cast<int16_t>(pc::ErrorCode::UnknownMember);
        case coord::HeartbeatResult::kErrStaleGeneration:   return static_cast<int16_t>(pc::ErrorCode::StaleGeneration);
        case coord::HeartbeatResult::kErrRebalanceInProgress: return static_cast<int16_t>(pc::ErrorCode::RebalanceInProgress);
        default:                                            return static_cast<int16_t>(pc::ErrorCode::OK);
    }
}

static int16_t syncErrorToProto(int coordErr) noexcept {
    switch (coordErr) {
        case coord::SyncResult::kOk:                   return static_cast<int16_t>(pc::ErrorCode::OK);
        case coord::SyncResult::kErrStaleGeneration:   return static_cast<int16_t>(pc::ErrorCode::StaleGeneration);
        case coord::SyncResult::kErrUnknownMember:     return static_cast<int16_t>(pc::ErrorCode::UnknownMember);
        case coord::SyncResult::kErrRebalanceInProgress: return static_cast<int16_t>(pc::ErrorCode::RebalanceInProgress);
        case coord::SyncResult::kErrWrongState:        return static_cast<int16_t>(pc::ErrorCode::RebalanceInProgress);
        default:                                       return static_cast<int16_t>(pc::ErrorCode::OK);
    }
}

// ---------------------------------------------------------------------------
// Construction
// ---------------------------------------------------------------------------
GroupCoordinationService::GroupCoordinationService(
    coord::ReplicatedOffsetStore* offsets,
    rune::gateway::TopicRegistry* topics,
    std::function<bool()>         isLeader,
    std::function<NodeId()>       leaderId)
    : offsets_(offsets)
    , topics_(topics)
    , isLeaderFn_(std::move(isLeader))
    , leaderIdFn_(std::move(leaderId))
{
    // Pre-reserve scratch vectors to avoid alloc in steady state.
    // 64 partitions / entries covers common real-world scenarios;
    // vectors will grow if needed (cold path, amortised).
    assignScratch_.reserve(64);
    commitResultScratch_.reserve(64);
    fetchResultScratch_.reserve(64);
    topicIdToNameScratch_.reserve(32);
    joinTopicsScratch_.reserve(32);
    assignFrameTpScratch_.reserve(64);  // CG-4: Assign frame TopicPartition32 scratch
}

// ---------------------------------------------------------------------------
// isLocalCoordinator
// ---------------------------------------------------------------------------
bool GroupCoordinationService::isLocalCoordinator() const {
    return isLeaderFn_ ? isLeaderFn_() : true;
}

// ---------------------------------------------------------------------------
// coordinatorFor — v1 seam: cluster leader owns all groups.
// ---------------------------------------------------------------------------
NodeId GroupCoordinationService::coordinatorFor(const GroupId& /*groupId*/) const {
    return leaderIdFn_ ? leaderIdFn_() : NodeId{0};
}

// ---------------------------------------------------------------------------
// coordinator — lazy get-or-create.
// ---------------------------------------------------------------------------
coord::GroupCoordinator& GroupCoordinationService::coordinator(const GroupId& groupId) {
    auto it = coordinators_.find(groupId);
    if (it != coordinators_.end()) {
        return it->second;
    }
    // Cold path: insert new GroupCoordinator.  One allocation; never in steady state.
    auto [inserted, ok] = coordinators_.emplace(
        std::piecewise_construct,
        std::forward_as_tuple(groupId),
        std::forward_as_tuple(groupId, /*nowNs=*/0));
    (void)ok;
    return inserted->second;
}

// ---------------------------------------------------------------------------
// buildTopicPartitionCounts — cold path; called only on join-timeout transition.
// ---------------------------------------------------------------------------
coord::TopicPartitionCounts GroupCoordinationService::buildTopicPartitionCounts() const {
    coord::TopicPartitionCounts counts;
    if (!topics_) return counts;

    const int32_t n = topics_->topicCount();
    for (int32_t tid = 0; tid < n; ++tid) {
        const rune::node::TopicSpec* spec = topics_->topic(tid);
        if (spec) {
            counts[spec->name] = spec->partitionCount;
        }
    }
    return counts;
}

// ---------------------------------------------------------------------------
// doWork — drive all coordinators; detect CompletingRebalance and auto-assign.
// Returns total work count (0 in steady state when all groups are Stable/Empty).
// ---------------------------------------------------------------------------
int GroupCoordinationService::doWork(int64_t nowNs) {
    if (coordinators_.empty()) return 0;

    int work = 0;
    for (auto& [groupId, coord] : coordinators_) {
        const coord::GroupState stateBefore = coord.state();

        const int w = coord.doWork(nowNs);
        work += w;

        // CG-4: detect PreparingRebalance entry (membership change) and eagerly
        // revoke all group push subscriptions + send empty Assign to each member.
        // This fires when:
        //   (a) stateBefore == Stable and stateAfter == PreparingRebalance
        //       (member join/leave triggered a new rebalance)
        //   (b) stateBefore == CompletingRebalance and a member joined causing
        //       another rebalance — but typically handled by the join logic.
        // Guard: only trigger once per entry by checking stateBefore != PreparingRebalance
        // and stateAfter == PreparingRebalance (avoid repeated revoke calls).
        const coord::GroupState stateAfter = coord.state();
        if (stateAfter == coord::GroupState::PreparingRebalance &&
            stateBefore != coord::GroupState::PreparingRebalance) {
            onEnterPreparingRebalance(groupId, coord);
        }

        // Check for CompletingRebalance → autoAssignAndSync.
        //
        // The group can reach CompletingRebalance via two paths:
        //   (a) doWork join-timeout: PreparingRebalance → CompletingRebalance
        //       (stateBefore != CompletingRebalance, stateAfter == CompletingRebalance)
        //   (b) all expected members re-joined inside handleJoinGroup before the
        //       timeout fired: GroupCoordinator::join() calls
        //       transitionToCompletingRebalance() directly.  In this case the
        //       coordinator arrives in doWork ALREADY in CompletingRebalance
        //       (both stateBefore and stateAfter == CompletingRebalance, w == 0).
        //
        // In both cases we need to call autoAssignAndSync to compute assignments
        // and drive the group into Stable.  We use !leaderSynced() as the guard
        // so we don't call it twice if doWork is already past it.
        if (stateAfter == coord::GroupState::CompletingRebalance && !coord.leaderSynced()) {
            // Auto-assign with RangeAssignor; transitions to Stable.
            const coord::TopicPartitionCounts partCounts = buildTopicPartitionCounts();
            const coord::SyncResult sr =
                coord.autoAssignAndSync(partCounts, coord::AssignorType::Range, nowNs);
            if (sr.ok()) {
                spdlog::debug("GroupCoordinationService: group='{}' auto-assigned, Stable",
                              groupId);
                // CG-4: on reaching Stable, create push subscriptions and send Assign frames.
                if (coord.state() == coord::GroupState::Stable) {
                    onReachedStable(groupId, coord, nowNs);
                }
            } else {
                spdlog::warn("GroupCoordinationService: group='{}' autoAssignAndSync failed: {}",
                             groupId, sr.errorCode);
            }
            ++work;
        }
    }
    return work;
}

// ---------------------------------------------------------------------------
// Lookup topicId → topic name via TopicRegistry.
// Returns empty string if topics_ is null or topicId is out of range.
// ---------------------------------------------------------------------------
static std::string topicNameForId(rune::gateway::TopicRegistry* topics, int32_t topicId) {
    if (!topics) return {};
    const rune::node::TopicSpec* spec = topics->topic(topicId);
    if (!spec) return {};
    return spec->name;
}

// ===========================================================================
// Handlers
// ===========================================================================

// ---------------------------------------------------------------------------
// handleJoinGroup (templateId 32)
// ---------------------------------------------------------------------------
size_t GroupCoordinationService::handleJoinGroup(
    const uint8_t* buf, size_t len, int64_t nowNs, uint8_t* out, size_t /*outCap*/)
{
    auto msgOpt = pc::JoinGroup::decode(buf, len);
    if (!msgOpt) {
        spdlog::debug("GroupCoordinationService: JoinGroup decode failed — dropped");
        return 0;
    }
    const pc::JoinGroup& msg = *msgOpt;

    const std::string groupIdStr(
        reinterpret_cast<const char*>(msg.groupId.data()), msg.groupId.size());
    const std::string memberIdStr(
        reinterpret_cast<const char*>(msg.memberId.data()), msg.memberId.size());

    // NotCoordinator fence.
    if (!isLocalCoordinator()) {
        pc::JoinGroupResponse resp;
        resp.sessionId     = msg.sessionId;
        resp.correlationId = msg.correlationId;
        resp.errorCode     = static_cast<int16_t>(pc::ErrorCode::NotCoordinator);
        resp.generationId  = 0;
        resp.isLeader      = 0;
        resp.memberId      = std::span<const uint8_t>();
        return resp.encode(out);
    }

    // Collect subscribedTopicIds → topic names.
    // joinTopicsScratch_ is a reused member; clear + refill to avoid per-call
    // vector allocation.  Short topic names benefit from SSO; the vector itself
    // is amortized (only grows on first large-subscribe, cold path).
    joinTopicsScratch_.clear();
    for (const int32_t tid : msg.subscribedTopicIds) {
        std::string name = topicNameForId(topics_, tid);
        if (!name.empty()) {
            joinTopicsScratch_.push_back(std::move(name));
        }
        // Unknown topicId silently ignored (Assignor ignores topics not in partCounts).
    }

    coord::GroupCoordinator& gc = coordinator(groupIdStr);

    // Set rebalance timeout from the member's sessionTimeout (or a sane default).
    gc.setRebalanceTimeoutMs(
        (msg.sessionTimeoutMs > 0) ? msg.sessionTimeoutMs : 30'000);

    coord::JoinResult jr = gc.join(memberIdStr,
                                   msg.sessionTimeoutMs > 0 ? msg.sessionTimeoutMs : 30'000,
                                   joinTopicsScratch_,
                                   nowNs);

    // CG-4: track member → sessionId so doWork can send Assign frames and create
    // push subscriptions without carrying session context through the coordinator.
    // Key format: "<groupId>:<memberId>" (both non-empty in steady state).
    // Store/overwrite on every JoinGroup (covers re-join with the same memberId
    // but from a new TCP session with a different sessionId).
    if (!jr.memberId.empty() && jr.errorCode != coord::JoinResult::kErrDead) {
        const std::string memberKey = groupIdStr + ":" + jr.memberId;
        memberSessionMap_[memberKey] = msg.sessionId;
        spdlog::debug("GroupCoordinationService: JoinGroup tracking member '{}' sessionId={}",
                      memberKey, msg.sessionId);
    }

    // Build response.
    const std::string& assignedMemberId = jr.memberId;
    pc::JoinGroupResponse resp;
    resp.sessionId     = msg.sessionId;
    resp.correlationId = msg.correlationId;
    resp.generationId  = jr.generationId;
    resp.isLeader      = jr.isLeader ? uint8_t{1} : uint8_t{0};
    resp.errorCode     = joinErrorToProto(jr.errorCode);
    resp.memberId      = std::span<const uint8_t>(
        reinterpret_cast<const uint8_t*>(assignedMemberId.data()),
        assignedMemberId.size());

    return resp.encode(out);
}

// ---------------------------------------------------------------------------
// handleSyncGroup (templateId 34)
// ---------------------------------------------------------------------------
size_t GroupCoordinationService::handleSyncGroup(
    const uint8_t* buf, size_t len, int64_t nowNs, uint8_t* out, size_t /*outCap*/)
{
    auto msgOpt = pc::SyncGroup::decode(buf, len);
    if (!msgOpt) {
        spdlog::debug("GroupCoordinationService: SyncGroup decode failed — dropped");
        return 0;
    }
    const pc::SyncGroup& msg = *msgOpt;

    const std::string groupIdStr(
        reinterpret_cast<const char*>(msg.groupId.data()), msg.groupId.size());
    const std::string memberIdStr(
        reinterpret_cast<const char*>(msg.memberId.data()), msg.memberId.size());

    // NotCoordinator fence.
    if (!isLocalCoordinator()) {
        pc::SyncGroupResponse resp;
        resp.sessionId     = msg.sessionId;
        resp.correlationId = msg.correlationId;
        resp.errorCode     = static_cast<int16_t>(pc::ErrorCode::NotCoordinator);
        return resp.encode(out, {});
    }

    // Fetch the coordinator (if it doesn't exist the member is unknown).
    auto it = coordinators_.find(groupIdStr);
    if (it == coordinators_.end()) {
        pc::SyncGroupResponse resp;
        resp.sessionId     = msg.sessionId;
        resp.correlationId = msg.correlationId;
        resp.errorCode     = static_cast<int16_t>(pc::ErrorCode::UnknownMember);
        return resp.encode(out, {});
    }
    coord::GroupCoordinator& gc = it->second;

    // Generation fence.
    if (msg.generationId != gc.generationId()) {
        pc::SyncGroupResponse resp;
        resp.sessionId     = msg.sessionId;
        resp.correlationId = msg.correlationId;
        resp.errorCode     = static_cast<int16_t>(pc::ErrorCode::StaleGeneration);
        return resp.encode(out, {});
    }

    // Followers call syncGroup with empty assignment map; they get their assignment
    // from the coordinator once Stable.
    const coord::Assignment emptyAssignment;
    const coord::SyncResult sr = gc.syncGroup(memberIdStr, msg.generationId, emptyAssignment, nowNs);

    if (!sr.ok()) {
        pc::SyncGroupResponse resp;
        resp.sessionId     = msg.sessionId;
        resp.correlationId = msg.correlationId;
        resp.errorCode     = syncErrorToProto(sr.errorCode);
        return resp.encode(out, {});
    }

    // Stable: return the member's assignment.
    const std::vector<coord::TopicPartition> assignment = gc.assignmentFor(memberIdStr);

    // Build TopicPartition32 array for the response.
    // assignScratch_ is pre-reserved; may grow if needed (rare, cold).
    assignScratch_.clear();
    assignScratch_.reserve(assignment.size());
    for (const auto& tp : assignment) {
        // Resolve topic name → topicId via TopicRegistry.
        int32_t topicId = -1;
        if (topics_) {
            const auto opt = topics_->topicIdForName(tp.topic);
            if (opt) topicId = *opt;
        }
        if (topicId >= 0) {
            assignScratch_.push_back(pc::TopicPartition32{topicId, tp.partition});
        }
        // If topicId unknown (topics_ null or not registered), skip.
    }

    pc::SyncGroupResponse resp;
    resp.sessionId     = msg.sessionId;
    resp.correlationId = msg.correlationId;
    resp.errorCode     = static_cast<int16_t>(pc::ErrorCode::OK);
    return resp.encode(out, std::span<const pc::TopicPartition32>(assignScratch_));
}

// ---------------------------------------------------------------------------
// handleHeartbeat (templateId 36)
// ---------------------------------------------------------------------------
size_t GroupCoordinationService::handleHeartbeat(
    const uint8_t* buf, size_t len, int64_t nowNs, uint8_t* out, size_t /*outCap*/)
{
    auto msgOpt = pc::Heartbeat::decode(buf, len);
    if (!msgOpt) {
        spdlog::debug("GroupCoordinationService: Heartbeat decode failed — dropped");
        return 0;
    }
    const pc::Heartbeat& msg = *msgOpt;

    const std::string groupIdStr(
        reinterpret_cast<const char*>(msg.groupId.data()), msg.groupId.size());
    const std::string memberIdStr(
        reinterpret_cast<const char*>(msg.memberId.data()), msg.memberId.size());

    // NotCoordinator fence.
    if (!isLocalCoordinator()) {
        pc::HeartbeatResponse resp;
        resp.sessionId     = msg.sessionId;
        resp.correlationId = msg.correlationId;
        resp.errorCode     = static_cast<int16_t>(pc::ErrorCode::NotCoordinator);
        return resp.encode(out);
    }

    auto it = coordinators_.find(groupIdStr);
    if (it == coordinators_.end()) {
        pc::HeartbeatResponse resp;
        resp.sessionId     = msg.sessionId;
        resp.correlationId = msg.correlationId;
        resp.errorCode     = static_cast<int16_t>(pc::ErrorCode::UnknownMember);
        return resp.encode(out);
    }

    const coord::HeartbeatResult hr =
        it->second.heartbeat(memberIdStr, msg.generationId, nowNs);

    pc::HeartbeatResponse resp;
    resp.sessionId     = msg.sessionId;
    resp.correlationId = msg.correlationId;
    resp.errorCode     = heartbeatErrorToProto(hr.errorCode);
    return resp.encode(out);
}

// ---------------------------------------------------------------------------
// handleLeaveGroup (templateId 38)
// ---------------------------------------------------------------------------
size_t GroupCoordinationService::handleLeaveGroup(
    const uint8_t* buf, size_t len, int64_t nowNs, uint8_t* out, size_t /*outCap*/)
{
    auto msgOpt = pc::LeaveGroup::decode(buf, len);
    if (!msgOpt) {
        spdlog::debug("GroupCoordinationService: LeaveGroup decode failed — dropped");
        return 0;
    }
    const pc::LeaveGroup& msg = *msgOpt;

    const std::string groupIdStr(
        reinterpret_cast<const char*>(msg.groupId.data()), msg.groupId.size());
    const std::string memberIdStr(
        reinterpret_cast<const char*>(msg.memberId.data()), msg.memberId.size());

    // NotCoordinator fence.
    if (!isLocalCoordinator()) {
        pc::LeaveGroupResponse resp;
        resp.sessionId     = msg.sessionId;
        resp.correlationId = msg.correlationId;
        resp.errorCode     = static_cast<int16_t>(pc::ErrorCode::NotCoordinator);
        return resp.encode(out);
    }

    auto it = coordinators_.find(groupIdStr);
    if (it != coordinators_.end()) {
        // Leave is generation-fenced inside GroupCoordinator::leave.
        // We need the member's last-known generationId; since the protocol
        // frame doesn't carry it for LeaveGroup (templateId 38), we pass
        // the current coordinator generationId (faithful Kafka: leave is
        // fenced; unknown/stale leaves are no-ops).
        it->second.leave(memberIdStr, it->second.generationId(), nowNs);
    }
    // If group doesn't exist: no-op (treat as already left).

    // CG-4: drop member → session mapping on Leave.
    if (!memberIdStr.empty()) {
        const std::string memberKey = groupIdStr + ":" + memberIdStr;
        memberSessionMap_.erase(memberKey);
    }

    pc::LeaveGroupResponse resp;
    resp.sessionId     = msg.sessionId;
    resp.correlationId = msg.correlationId;
    resp.errorCode     = static_cast<int16_t>(pc::ErrorCode::OK);
    return resp.encode(out);
}

// ---------------------------------------------------------------------------
// handleOffsetCommit (templateId 40)
// ---------------------------------------------------------------------------
size_t GroupCoordinationService::handleOffsetCommit(
    const uint8_t* buf, size_t len, int64_t nowNs, uint8_t* out, size_t outCap)
{
    auto msgOpt = pc::OffsetCommit::decode(buf, len);
    if (!msgOpt) {
        spdlog::debug("GroupCoordinationService: OffsetCommit decode failed — dropped");
        return 0;
    }
    const pc::OffsetCommit& msg = *msgOpt;

    const std::string groupIdStr(
        reinterpret_cast<const char*>(msg.groupId.data()), msg.groupId.size());

    // NotCoordinator fence — fires for both empty and non-empty entry lists.
    // For empty entries: encode a sentinel single result with NotCoordinator so
    // the client receives an unambiguous error signal (not a silent empty-OK frame).
    if (!isLocalCoordinator()) {
        commitResultScratch_.clear();
        for (const auto& e : msg.entries) {
            // Bound-check: stop if adding this result would overrun outCap.
            const std::size_t needed =
                pc::OffsetCommitResponse::encodedSizeFor(commitResultScratch_.size() + 1);
            if (needed > outCap) break;
            pc::OffsetCommitResult r;
            r.topicId   = e.topicId;
            r.partition = e.partition;
            r.errorCode = static_cast<int16_t>(pc::ErrorCode::NotCoordinator);
            commitResultScratch_.push_back(r);
        }
        // If entries were empty, add a single sentinel so the caller can detect
        // NotCoordinator even without per-entry results.
        if (commitResultScratch_.empty()) {
            const std::size_t needed = pc::OffsetCommitResponse::encodedSizeFor(1);
            if (needed <= outCap) {
                pc::OffsetCommitResult sentinel;
                sentinel.topicId   = -1;
                sentinel.partition = -1;
                sentinel.errorCode = static_cast<int16_t>(pc::ErrorCode::NotCoordinator);
                commitResultScratch_.push_back(sentinel);
            }
        }
        pc::OffsetCommitResponse resp;
        resp.sessionId     = msg.sessionId;
        resp.correlationId = msg.correlationId;
        return resp.encode(out, std::span<const pc::OffsetCommitResult>(commitResultScratch_));
    }

    // Generation fence: verify the member is in the group at the claimed generation.
    auto it = coordinators_.find(groupIdStr);
    const bool groupExists = (it != coordinators_.end());
    const bool genOk = groupExists && (msg.generationId == it->second.generationId());

    commitResultScratch_.clear();

    // Iterate entries defensively (CG-1 note: count can overstate; stop when
    // the view iterator ends, which happens when the frame bytes run out).
    // Also bound-check BEFORE each push so the final encode never overruns outCap.
    for (const auto& e : msg.entries) {
        // Stop if adding this result would overrun the output buffer.
        const std::size_t needed =
            pc::OffsetCommitResponse::encodedSizeFor(commitResultScratch_.size() + 1);
        if (needed > outCap) {
            spdlog::warn("GroupCoordinationService: OffsetCommit response capped at {} entries "
                         "(outCap={} would be exceeded)", commitResultScratch_.size(), outCap);
            break;
        }

        pc::OffsetCommitResult r;
        r.topicId   = e.topicId;
        r.partition = e.partition;

        if (!genOk) {
            r.errorCode = static_cast<int16_t>(
                groupExists ? pc::ErrorCode::IllegalGeneration
                            : pc::ErrorCode::UnknownMember);
            commitResultScratch_.push_back(r);
            continue;
        }

        // Resolve topicId → topic name.
        const std::string topicName = topicNameForId(topics_, e.topicId);
        if (topicName.empty()) {
            // Unknown topic — no commit; report UnknownTopic per entry.
            r.errorCode = static_cast<int16_t>(pc::ErrorCode::UnknownTopic);
            commitResultScratch_.push_back(r);
            continue;
        }

        const coord::TopicPartition tp{topicName, e.partition};
        const std::string_view meta(
            reinterpret_cast<const char*>(e.metadata.data()), e.metadata.size());

        const bool committed = offsets_->commit(groupIdStr, tp, e.offset, meta, nowNs);
        r.errorCode = committed ? static_cast<int16_t>(pc::ErrorCode::OK)
                                : static_cast<int16_t>(pc::ErrorCode::MinIsrViolation);
        commitResultScratch_.push_back(r);
    }

    pc::OffsetCommitResponse resp;
    resp.sessionId     = msg.sessionId;
    resp.correlationId = msg.correlationId;
    return resp.encode(out, std::span<const pc::OffsetCommitResult>(commitResultScratch_));
}

// ---------------------------------------------------------------------------
// handleOffsetFetch (templateId 42)
// ---------------------------------------------------------------------------
size_t GroupCoordinationService::handleOffsetFetch(
    const uint8_t* buf, size_t len, int64_t nowNs, uint8_t* out, size_t outCap)
{
    (void)nowNs;

    auto msgOpt = pc::OffsetFetch::decode(buf, len);
    if (!msgOpt) {
        spdlog::debug("GroupCoordinationService: OffsetFetch decode failed — dropped");
        return 0;
    }
    const pc::OffsetFetch& msg = *msgOpt;

    const std::string groupIdStr(
        reinterpret_cast<const char*>(msg.groupId.data()), msg.groupId.size());

    // NotCoordinator fence — fires for both empty and non-empty entry lists.
    // For empty entries: encode a sentinel result so the caller can detect the
    // error; a purely empty OK frame would be indistinguishable from "committed
    // nothing".
    if (!isLocalCoordinator()) {
        fetchResultScratch_.clear();
        for (const auto& e : msg.entries) {
            // Bound-check before each push.
            const std::size_t needed =
                pc::OffsetFetchResponse::encodedSizeFor(fetchResultScratch_.size() + 1);
            if (needed > outCap) break;
            pc::OffsetFetchResult r;
            r.topicId   = e.topicId;
            r.partition = e.partition;
            r.offset    = -1;
            r.errorCode = static_cast<int16_t>(pc::ErrorCode::NotCoordinator);
            fetchResultScratch_.push_back(r);
        }
        // Sentinel for empty-entries case.
        if (fetchResultScratch_.empty()) {
            const std::size_t needed = pc::OffsetFetchResponse::encodedSizeFor(1);
            if (needed <= outCap) {
                pc::OffsetFetchResult sentinel;
                sentinel.topicId   = -1;
                sentinel.partition = -1;
                sentinel.offset    = -1;
                sentinel.errorCode = static_cast<int16_t>(pc::ErrorCode::NotCoordinator);
                fetchResultScratch_.push_back(sentinel);
            }
        }
        pc::OffsetFetchResponse resp;
        resp.sessionId     = msg.sessionId;
        resp.correlationId = msg.correlationId;
        return resp.encode(out, std::span<const pc::OffsetFetchResult>(fetchResultScratch_));
    }

    fetchResultScratch_.clear();

    if (msg.entries.empty()) {
        // Fetch all committed offsets for the group.
        // Bound-check BEFORE writing each result so a large fetch-all never
        // overruns a fixed scratch buffer.  Results that would exceed outCap
        // are silently dropped (the response is capped but never overruns).
        const auto all = offsets_->fetchAll(groupIdStr);
        for (const auto& [tp, co] : all) {
            // Bound-check before each push.
            const std::size_t needed =
                pc::OffsetFetchResponse::encodedSizeFor(fetchResultScratch_.size() + 1);
            if (needed > outCap) {
                spdlog::warn("GroupCoordinationService: OffsetFetch fetch-all capped at {} entries "
                             "(outCap={} would be exceeded)", fetchResultScratch_.size(), outCap);
                break;
            }

            // Resolve topic name → topicId.
            int32_t topicId = -1;
            if (topics_) {
                const auto opt = topics_->topicIdForName(tp.topic);
                if (opt) topicId = *opt;
            }
            if (topicId < 0) continue; // unknown topic — skip

            pc::OffsetFetchResult r;
            r.topicId   = topicId;
            r.partition = tp.partition;
            r.offset    = co.offset;
            r.errorCode = static_cast<int16_t>(pc::ErrorCode::OK);
            fetchResultScratch_.push_back(r);
        }
    } else {
        // Fetch specific (topicId, partition) entries.
        // Bound-check before each push.
        for (const auto& e : msg.entries) {
            const std::size_t needed =
                pc::OffsetFetchResponse::encodedSizeFor(fetchResultScratch_.size() + 1);
            if (needed > outCap) {
                spdlog::warn("GroupCoordinationService: OffsetFetch capped at {} entries "
                             "(outCap={} would be exceeded)", fetchResultScratch_.size(), outCap);
                break;
            }

            const std::string topicName = topicNameForId(topics_, e.topicId);

            pc::OffsetFetchResult r;
            r.topicId   = e.topicId;
            r.partition = e.partition;
            r.errorCode = static_cast<int16_t>(pc::ErrorCode::OK);

            if (topicName.empty()) {
                r.offset = -1; // no-offset sentinel
            } else {
                const coord::TopicPartition tp{topicName, e.partition};
                const auto co = offsets_->fetch(groupIdStr, tp);
                r.offset = co.has_value() ? co->offset : int64_t{-1};
            }
            fetchResultScratch_.push_back(r);
        }
    }

    pc::OffsetFetchResponse resp;
    resp.sessionId     = msg.sessionId;
    resp.correlationId = msg.correlationId;
    return resp.encode(out, std::span<const pc::OffsetFetchResult>(fetchResultScratch_));
}

// ===========================================================================
// CG-4 helpers
// ===========================================================================

// ---------------------------------------------------------------------------
// onEnterPreparingRebalance — called from doWork when a group transitions into
// PreparingRebalance (e.g. a new member joined while Stable, or a member left).
//
// For every current member:
//   1. revokeGroupSubscriptions(memberSession) — stop all group push deliveries.
//   2. sendControlFrame with an Assign{gen, {}} (empty = revoke all).
//
// This is the "eager stop-the-world" revoke: no member receives records for
// partitions from the previous generation once rebalance begins.
//
// No-op when gateway_ is null (tests without a real gateway).
// ---------------------------------------------------------------------------
void GroupCoordinationService::onEnterPreparingRebalance(
    const GroupId& groupId,
    rune::coordination::GroupCoordinator& coord)
{
    if (!gateway_) return;  // no push path wired

    const int32_t gen = coord.generationId();

    for (const auto& member : coord.members()) {
        const std::string memberKey = groupId + ":" + member.memberId;
        const auto it = memberSessionMap_.find(memberKey);
        if (it == memberSessionMap_.end()) continue;  // no session tracked yet (early join)

        const int64_t sessionId = it->second;

        // 1. Eagerly revoke all group push subscriptions for this member.
        gateway_->revokeGroupSubscriptions(sessionId);

        // 2. Send empty Assign{gen, {}} (revoke).
        // Encode into assignFrameScratch_ (pre-allocated, no heap alloc).
        const std::size_t needed = pc::Assign::encodedSizeFor(0);
        if (needed > kAssignFrameScratchSize) {
            spdlog::error("GroupCoordinationService::onEnterPreparingRebalance: "
                          "Assign frame size {} > scratch {}; skipping member '{}'",
                          needed, kAssignFrameScratchSize, member.memberId);
            continue;
        }

        pc::Assign revokeFrame;
        revokeFrame.sessionId     = sessionId;
        revokeFrame.correlationId = 0;
        revokeFrame.generationId  = gen;
        const std::size_t encoded = revokeFrame.encode(
            assignFrameScratch_.data(), std::span<const pc::TopicPartition32>{});

        const bool sent = gateway_->sendControlFrame(
            sessionId, assignFrameScratch_.data(), encoded);
        spdlog::debug("GroupCoordinationService: revoke Assign(gen={}) sent={} "
                      "to member='{}' sessionId={}",
                      gen, sent, member.memberId, sessionId);
    }
}

// ---------------------------------------------------------------------------
// onReachedStable — called from doWork after autoAssignAndSync succeeds and
// the group transitions to Stable.
//
// For every member:
//   1. For each assigned (topicId, partition):
//      - fromOffset = offsets_->fetch(group, tp) → committed, or 0 (earliest)
//        if not found.  (Milestone-D will support auto.offset.reset=latest via
//        replicaRegistry lookup — for now, "no commit" → earliest = 0.)
//      - addGroupSubscription(memberSession, topicId, partition, fromOffset, 0)
//   2. Build Assign{gen, assignments} and send via sendControlFrame.
//      Bounded by assignFrameScratch_ capacity (outCap-bounded encode).
//
// Resolves the CG-3 carried item: Assign frame encoding is outCap-bounded
// (entries that exceed the scratch capacity are silently capped, not written).
//
// No-op when gateway_ is null (tests without a real gateway).
// ---------------------------------------------------------------------------
void GroupCoordinationService::onReachedStable(
    const GroupId& groupId,
    rune::coordination::GroupCoordinator& coord,
    int64_t nowNs)
{
    (void)nowNs;
    if (!gateway_) return;  // no push path wired

    const int32_t gen = coord.generationId();

    for (const auto& member : coord.members()) {
        const std::string memberKey = groupId + ":" + member.memberId;
        const auto it = memberSessionMap_.find(memberKey);
        if (it == memberSessionMap_.end()) {
            spdlog::warn("GroupCoordinationService::onReachedStable: "
                         "no sessionId for member '{}' in group '{}' — skipping",
                         member.memberId, groupId);
            continue;
        }
        const int64_t sessionId = it->second;

        // Build assignment and create push subscriptions.
        assignFrameTpScratch_.clear();

        for (const auto& tp : member.assignment) {
            // Resolve topic name → topicId via TopicRegistry.
            int32_t topicId = -1;
            if (topics_) {
                const auto opt = topics_->topicIdForName(tp.topic);
                if (opt) topicId = *opt;
            }
            if (topicId < 0) {
                spdlog::warn("GroupCoordinationService::onReachedStable: "
                             "unknown topic '{}' for member '{}' — skipping partition",
                             tp.topic, member.memberId);
                continue;
            }

            // fromOffset: use committed offset if available; else 0 (earliest).
            int64_t fromOffset = 0;
            {
                const auto committed = offsets_->fetch(groupId, tp);
                if (committed.has_value()) {
                    fromOffset = committed->offset;
                }
                // auto.offset.reset=latest would use replica->highWatermark() here;
                // deferred to Milestone-D.  For now: no commit → earliest (0).
            }

            // Create the push subscription (resumed from fromOffset).
            const int64_t subId = gateway_->addGroupSubscription(
                sessionId, topicId, tp.partition, fromOffset, /*isolation=*/0);
            if (subId < 0) {
                spdlog::warn("GroupCoordinationService::onReachedStable: "
                             "addGroupSubscription failed for member '{}' topic={} partition={}",
                             member.memberId, topicId, tp.partition);
                continue;
            }

            // Accumulate for the Assign frame (outCap-bounded below).
            // Check if this entry would overflow the scratch before pushing.
            const std::size_t nextCount = assignFrameTpScratch_.size() + 1;
            const std::size_t neededForNext = pc::Assign::encodedSizeFor(nextCount);
            if (neededForNext > kAssignFrameScratchSize) {
                spdlog::warn("GroupCoordinationService::onReachedStable: "
                             "Assign frame capacity reached at {} entries (scratch={}B); "
                             "remaining partitions pushed but not in Assign frame",
                             assignFrameTpScratch_.size(), kAssignFrameScratchSize);
                break;  // push subscriptions already created above; frame is capped
            }
            assignFrameTpScratch_.push_back(pc::TopicPartition32{topicId, tp.partition});
        }

        // Encode and send Assign{gen, assignment}.
        const std::size_t needed = pc::Assign::encodedSizeFor(assignFrameTpScratch_.size());
        if (needed > kAssignFrameScratchSize) {
            // Defensive — should never happen given the outCap-bounded loop above.
            spdlog::error("GroupCoordinationService::onReachedStable: "
                          "Assign frame {} > scratch {} — BUG; skipping send",
                          needed, kAssignFrameScratchSize);
            continue;
        }

        pc::Assign assignFrame;
        assignFrame.sessionId     = sessionId;
        assignFrame.correlationId = 0;
        assignFrame.generationId  = gen;
        const std::size_t encoded = assignFrame.encode(
            assignFrameScratch_.data(),
            std::span<const pc::TopicPartition32>(assignFrameTpScratch_));

        const bool sent = gateway_->sendControlFrame(
            sessionId, assignFrameScratch_.data(), encoded);
        spdlog::debug("GroupCoordinationService::onReachedStable: Assign(gen={}, {} partitions) "
                      "sent={} to member='{}' sessionId={}",
                      gen, assignFrameTpScratch_.size(), sent, member.memberId, sessionId);
    }
}

} // namespace rune::node
