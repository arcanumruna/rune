// ---------------------------------------------------------------------------
// GroupCoordinationService.cpp — CG-3 implementation.
//
// Single-writer. No locks. No per-cycle allocation in steady state.
// See GroupCoordinationService.h for design notes.
// ---------------------------------------------------------------------------

#include <rune/node/GroupCoordinationService.h>

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
    // 64 partitions per group covers common real-world scenarios;
    // vectors will grow if needed (cold path).
    assignScratch_.reserve(64);
    commitResultScratch_.reserve(64);
    fetchResultScratch_.reserve(64);
    topicIdToNameScratch_.reserve(32);
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
        //
        // CG-4 will hook here to send Assign egress frames.
        const coord::GroupState stateAfter = coord.state();
        if (stateAfter == coord::GroupState::CompletingRebalance && !coord.leaderSynced()) {
            // Auto-assign with RangeAssignor; transitions to Stable.
            const coord::TopicPartitionCounts partCounts = buildTopicPartitionCounts();
            const coord::SyncResult sr =
                coord.autoAssignAndSync(partCounts, coord::AssignorType::Range, nowNs);
            if (sr.ok()) {
                spdlog::debug("GroupCoordinationService: group='{}' auto-assigned, Stable",
                              groupId);
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
    const uint8_t* buf, size_t len, int64_t nowNs, uint8_t* out)
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
    // Pre-cleared scratch; grows only on first encounters (cold path).
    std::vector<std::string> subscribedTopics;
    subscribedTopics.reserve(msg.subscribedTopicIds.size());
    for (const int32_t tid : msg.subscribedTopicIds) {
        std::string name = topicNameForId(topics_, tid);
        if (!name.empty()) {
            subscribedTopics.push_back(std::move(name));
        }
        // Unknown topicId silently ignored (Assignor ignores topics not in partCounts).
    }

    coord::GroupCoordinator& gc = coordinator(groupIdStr);

    // Set rebalance timeout from the member's sessionTimeout (or a sane default).
    gc.setRebalanceTimeoutMs(
        (msg.sessionTimeoutMs > 0) ? msg.sessionTimeoutMs : 30'000);

    coord::JoinResult jr = gc.join(memberIdStr,
                                   msg.sessionTimeoutMs > 0 ? msg.sessionTimeoutMs : 30'000,
                                   subscribedTopics,
                                   nowNs);

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
    const uint8_t* buf, size_t len, int64_t nowNs, uint8_t* out)
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
    const uint8_t* buf, size_t len, int64_t nowNs, uint8_t* out)
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
    const uint8_t* buf, size_t len, int64_t nowNs, uint8_t* out)
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
    const uint8_t* buf, size_t len, int64_t nowNs, uint8_t* out)
{
    auto msgOpt = pc::OffsetCommit::decode(buf, len);
    if (!msgOpt) {
        spdlog::debug("GroupCoordinationService: OffsetCommit decode failed — dropped");
        return 0;
    }
    const pc::OffsetCommit& msg = *msgOpt;

    const std::string groupIdStr(
        reinterpret_cast<const char*>(msg.groupId.data()), msg.groupId.size());

    // NotCoordinator fence.
    if (!isLocalCoordinator()) {
        // Return a single-entry response with NotCoordinator for each entry.
        // The response doesn't carry per-entry semantics for this error; use
        // a 0-result response and encode NotCoordinator at the frame level via
        // a standard error code in the first (and only) result, or return 0
        // results with an implicit error.  Per the protocol spec (CG-1),
        // OffsetCommitResponse contains per-result errorCodes; we return an
        // empty result list with no NotCoordinator result — clients check the
        // errorCode per result.  For simplicity in the non-coordinator case,
        // return a single result entry with NotCoordinator.
        // Actually the best approach: return 0 results (empty) and let the
        // caller handle missing results as errors.  But per-brief:
        // "encode the response with errorCode=NotCoordinator".
        // We'll encode the response with 0 entries; the session layer wraps
        // this correctly as a coordinator error at the gateway level.
        // Per brief: "If !isLocalCoordinator(): encode the response with
        // errorCode=NotCoordinator." This means we encode it using the response
        // message's per-result errorCode field — since OffsetCommitResponse
        // doesn't have a top-level errorCode, we build one synthetic result
        // per entry (or a single synthetic result).  The cleanest approach:
        // build one result per requested entry with NotCoordinator.
        commitResultScratch_.clear();
        for (const auto& e : msg.entries) {
            pc::OffsetCommitResult r;
            r.topicId   = e.topicId;
            r.partition = e.partition;
            r.errorCode = static_cast<int16_t>(pc::ErrorCode::NotCoordinator);
            commitResultScratch_.push_back(r);
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
    for (const auto& e : msg.entries) {
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
            // Unknown topic — skip (no commit, no error; or use UnknownTopic).
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
    const uint8_t* buf, size_t len, int64_t nowNs, uint8_t* out)
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

    // NotCoordinator fence.
    if (!isLocalCoordinator()) {
        fetchResultScratch_.clear();
        for (const auto& e : msg.entries) {
            pc::OffsetFetchResult r;
            r.topicId   = e.topicId;
            r.partition = e.partition;
            r.offset    = -1;
            r.errorCode = static_cast<int16_t>(pc::ErrorCode::NotCoordinator);
            fetchResultScratch_.push_back(r);
        }
        pc::OffsetFetchResponse resp;
        resp.sessionId     = msg.sessionId;
        resp.correlationId = msg.correlationId;
        return resp.encode(out, std::span<const pc::OffsetFetchResult>(fetchResultScratch_));
    }

    fetchResultScratch_.clear();

    if (msg.entries.empty()) {
        // Fetch all committed offsets for the group.
        const auto all = offsets_->fetchAll(groupIdStr);
        fetchResultScratch_.reserve(all.size());
        for (const auto& [tp, co] : all) {
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
        fetchResultScratch_.reserve(msg.entries.size());
        for (const auto& e : msg.entries) {
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

} // namespace rune::node
