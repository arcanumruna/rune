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

    // CG-7 zero-alloc: pre-reserve per-handler string scratch buffers.
    // 128 bytes covers groupId + memberId + composite key for any realistic name.
    // After the first long-string call the capacity stabilises — no per-call alloc.
    groupIdScratch_.reserve(64);
    memberIdScratch_.reserve(64);
    memberKeyScratch_.reserve(128);
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
    // CG-6 failover: rebuild the in-memory offset index on the first duty cycle
    // after becoming coordinator (Follower → Leader transition).  This ensures
    // committed offsets that were replicated to this node while it was a follower
    // are visible before serving any JoinGroup / SyncGroup requests.
    const bool amLeader = isLocalCoordinator();
    if (amLeader && !wasLeader_) {
        spdlog::info("GroupCoordinationService::doWork: became coordinator — rebuilding offset index");
        if (offsets_) {
            offsets_->rebuildFromLog();
        }
        wasLeader_ = true;
    } else if (!amLeader && wasLeader_) {
        // Lost leadership — mark so the next election triggers another rebuild.
        wasLeader_ = false;
    }

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

    // CG-7 zero-alloc: assign() into pre-reserved scratch strings; no heap alloc
    // when capacity >= size (capacity grows on first long-string call, then stable).
    groupIdScratch_.assign(
        reinterpret_cast<const char*>(msg.groupId.data()), msg.groupId.size());
    memberIdScratch_.assign(
        reinterpret_cast<const char*>(msg.memberId.data()), msg.memberId.size());
    const std::string& groupIdStr  = groupIdScratch_;
    const std::string& memberIdStr = memberIdScratch_;

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

    // CG-4: capture state BEFORE join so we can detect a Stable→PreparingRebalance
    // transition triggered directly by gc.join() (new member arriving while Stable).
    // The doWork() transition detector only fires when the transition happens inside
    // coord.doWork(); for join-triggered rebalances the state is already
    // PreparingRebalance by the time doWork() next runs — so we must fire here.
    const coord::GroupState stateBefore = gc.state();

    coord::JoinResult jr = gc.join(memberIdStr,
                                   msg.sessionTimeoutMs > 0 ? msg.sessionTimeoutMs : 30'000,
                                   joinTopicsScratch_,
                                   nowNs);

    // CG-4: if the join triggered a Stable→PreparingRebalance transition, eagerly
    // revoke all group push subscriptions and send empty Assign (revoke) frames.
    // This mirrors the doWork() detection path for timer-triggered rebalances.
    if (gateway_ &&
        gc.state() == coord::GroupState::PreparingRebalance &&
        stateBefore != coord::GroupState::PreparingRebalance) {
        onEnterPreparingRebalance(groupIdStr, gc);
    }

    // CG-4: track member → sessionId so doWork can send Assign frames and create
    // push subscriptions without carrying session context through the coordinator.
    // Key format: "<groupId>:<memberId>" (both non-empty in steady state).
    // Store/overwrite on every JoinGroup (covers re-join with the same memberId
    // but from a new TCP session with a different sessionId).
    if (!jr.memberId.empty() && jr.errorCode != coord::JoinResult::kErrDead) {
        // CG-7 zero-alloc: build composite key into pre-reserved scratch.
        memberKeyScratch_.clear();
        memberKeyScratch_.append(groupIdStr);
        memberKeyScratch_ += ':';
        memberKeyScratch_.append(jr.memberId);
        memberSessionMap_[memberKeyScratch_] = msg.sessionId;
        spdlog::debug("GroupCoordinationService: JoinGroup tracking member '{}' sessionId={}",
                      memberKeyScratch_, msg.sessionId);
    }

    // CG-6 fix: if the group just reached CompletingRebalance (all expected
    // members joined), auto-assign NOW instead of waiting for the next doWork()
    // cycle.  Without this, the client receives JoinGroupResponse-OK and
    // immediately sends SyncGroup — but leaderSynced_ is still false so
    // syncGroup() returns RebalanceInProgress, the client enters Rejoining state,
    // and the next JoinGroup re-triggers a rebalance → infinite revoke/assign loop.
    //
    // By calling autoAssignAndSync() synchronously here (before returning the
    // JoinGroupResponse), the assignment is committed + subscriptions are wired
    // + the Assign frame is sent — all before the client even sees
    // JoinGroupResponse.  When the client's SyncGroup arrives, leaderSynced_=true
    // and state=Stable → syncGroup() returns OK with the assignment in-hand.
    //
    // Mirrors the doWork() detection path for timer-triggered completions; the
    // guard (!gc.leaderSynced()) ensures this fires at most once per generation.
    if (gc.state() == coord::GroupState::CompletingRebalance && !gc.leaderSynced()) {
        const coord::TopicPartitionCounts partCounts = buildTopicPartitionCounts();
        const coord::SyncResult sr =
            gc.autoAssignAndSync(partCounts, coord::AssignorType::Range, nowNs);
        if (sr.ok()) {
            spdlog::debug("GroupCoordinationService: group='{}' early auto-assign (join-triggered)",
                          groupIdStr);
            if (gc.state() == coord::GroupState::Stable) {
                onReachedStable(groupIdStr, gc, nowNs);
            }
        } else {
            spdlog::warn("GroupCoordinationService: group='{}' early autoAssignAndSync failed: {}",
                         groupIdStr, sr.errorCode);
        }
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

    // CG-7 zero-alloc: assign() into pre-reserved scratch strings.
    groupIdScratch_.assign(
        reinterpret_cast<const char*>(msg.groupId.data()), msg.groupId.size());
    memberIdScratch_.assign(
        reinterpret_cast<const char*>(msg.memberId.data()), msg.memberId.size());
    const std::string& groupIdStr  = groupIdScratch_;
    const std::string& memberIdStr = memberIdScratch_;

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

    // CG-6 push-subscription seam: create (or reset) the member's group push
    // subscriptions here — at the moment the client confirms it is ready to
    // receive records — rather than eagerly in onReachedStable.
    //
    // Rationale: onReachedStable fires while the client is still in Joining or
    // Syncing state (it just sent JoinGroup and is waiting for the SyncGroup
    // round-trip).  If subscriptions are created there, pushSubscriptions() may
    // deliver records BEFORE the client reaches Stable, causing them to be
    // silently dropped by GroupConsumerHub::onRecords (state != Stable guard).
    // By the time the client enters Stable the subscription's nextOffset has
    // already advanced past all pre-existing records → 0 records consumed.
    //
    // Creating subscriptions HERE (on the SyncGroup OK path) guarantees the
    // push starts at the committed offset EXACTLY when the client is about to
    // transition to Stable (the SyncGroupResponse triggers the transition).
    // Any records pushed between now and the SyncGroupResponse arriving at the
    // client are at most one network-RTT worth — a negligible window and safe
    // because the subscription always starts from the committed offset, which
    // is what the client will OffsetFetch next anyway.
    //
    // Fix #4: skip the revoke+recreate cycle when this member session already
    // has the correct group subscriptions active (idempotent SyncGroup for an
    // unchanged assignment — Stable→Stable re-sync).  Revoking a live sub and
    // re-creating it from the committed offset would move the push frontier
    // backwards (re-delivery), which is unnecessary bounded duplication when
    // the assignment did not change.  Guard: only recreate when the session has
    // NO live group subs (first SyncGroup in this generation) or the count
    // differs from the assignment (partition set did change).
    if (gateway_) {
        const std::size_t existingSubs = gateway_->groupMemberSubCount(msg.sessionId);
        const bool needRecreate = (existingSubs != assignment.size());
        if (!needRecreate) {
            // Assignment matches existing live subs — skip revoke+recreate.
            spdlog::debug("GroupCoordinationService::handleSyncGroup: "
                          "skipping sub recreate for member='{}' sessionId={} "
                          "(already have {} subs = assignment size)",
                          memberIdStr, msg.sessionId, existingSubs);
        } else {
        gateway_->revokeGroupSubscriptions(msg.sessionId);
        for (const auto& tp : assignment) {
            int32_t topicId = -1;
            if (topics_) {
                const auto opt = topics_->topicIdForName(tp.topic);
                if (opt) topicId = *opt;
            }
            if (topicId < 0) {
                spdlog::warn("GroupCoordinationService::handleSyncGroup: "
                             "unknown topic '{}' for member '{}' — skipping partition",
                             tp.topic, memberIdStr);
                continue;
            }
            // Seed at the committed offset only.  Cross-generation re-delivery
            // of [committed, delivered) is legitimate bounded duplication (the
            // group tolerates it via assert_group_bounded_duplicates).  We do NOT
            // import the previous generation's wire-send position: that position is
            // the broker's PUSH frontier (nextOffset in Subscription), which may be
            // ahead of any records actually consumed+committed by the client.
            // Seeding at max(committed, wirePosition) would silently skip records
            // that were pushed to the old member but never consumed — an at-least-
            // once violation (silent loss).
            int64_t fromOffset = 0;
            {
                const auto committed = offsets_->fetch(groupIdStr, tp);
                if (committed.has_value()) fromOffset = committed->offset;
            }
            // Conservative default isolation = read_committed (1).  The member
            // conveys its true isolation via a Subscribe carrier frame AFTER it
            // enters Stable (TX-5 sendSubscribeForPartitions), which the gateway
            // applies to THIS same group subscription.  Defaulting to
            // read_committed closes the race window between this sub becoming
            // push-eligible and the carrier arriving: during that window we must
            // never leak aborted/open transactional records to a read_committed
            // consumer.  A read_uncommitted member is simply capped at the LSO
            // for the few ms until its carrier downgrades this sub to isolation=0
            // — a harmless, self-correcting delay, never a correctness violation.
            const int64_t subId = gateway_->addGroupSubscription(
                msg.sessionId, topicId, tp.partition, fromOffset, /*isolation=*/1,
                /*pendingCarrier=*/true);
            if (subId < 0) {
                spdlog::warn("GroupCoordinationService::handleSyncGroup: "
                             "addGroupSubscription failed for member '{}' topic={} partition={}",
                             memberIdStr, topicId, tp.partition);
            } else {
                spdlog::debug("GroupCoordinationService::handleSyncGroup: "
                              "group sub for member='{}' sessionId={} topic={} partition={} fromOffset={}",
                              memberIdStr, msg.sessionId, topicId, tp.partition, fromOffset);
            }
        }
        } // end else (needRecreate)
    } // end if (gateway_)

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

    // CG-7 zero-alloc: assign() into pre-reserved scratch strings; no heap alloc
    // once capacity >= size.  Heartbeat is the hottest handler path.
    groupIdScratch_.assign(
        reinterpret_cast<const char*>(msg.groupId.data()), msg.groupId.size());
    memberIdScratch_.assign(
        reinterpret_cast<const char*>(msg.memberId.data()), msg.memberId.size());
    const std::string& groupIdStr  = groupIdScratch_;
    const std::string& memberIdStr = memberIdScratch_;

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

    // CG-7 zero-alloc: assign() into pre-reserved scratch strings.
    groupIdScratch_.assign(
        reinterpret_cast<const char*>(msg.groupId.data()), msg.groupId.size());
    memberIdScratch_.assign(
        reinterpret_cast<const char*>(msg.memberId.data()), msg.memberId.size());
    const std::string& groupIdStr  = groupIdScratch_;
    const std::string& memberIdStr = memberIdScratch_;

    // NotCoordinator fence.
    if (!isLocalCoordinator()) {
        pc::LeaveGroupResponse resp;
        resp.sessionId     = msg.sessionId;
        resp.correlationId = msg.correlationId;
        resp.errorCode     = static_cast<int16_t>(pc::ErrorCode::NotCoordinator);
        return resp.encode(out);
    }

    // CG-4 fix (#3): revoke the leaving member's group push subscriptions BEFORE
    // erasing the session mapping.  This prevents dual-owner: the next
    // PreparingRebalance iterates coord.members() which no longer contains the
    // departed member — so without this explicit revoke the member keeps receiving
    // records for its old partitions until session reap, which can overlap with
    // the reassigned owner (dup delivery, single-owner violation).
    if (gateway_ && !memberIdStr.empty()) {
        memberKeyScratch_.clear();
        memberKeyScratch_.append(groupIdStr);
        memberKeyScratch_ += ':';
        memberKeyScratch_.append(memberIdStr);
        const auto sessIt = memberSessionMap_.find(memberKeyScratch_);
        if (sessIt != memberSessionMap_.end()) {
            const int64_t leavingSid = sessIt->second;
            gateway_->revokeGroupSubscriptions(leavingSid);
            spdlog::debug("GroupCoordinationService: LeaveGroup revoking subs for "
                          "member='{}' sessionId={}", memberIdStr, leavingSid);
        }
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
        // memberKeyScratch_ was already built above; rebuild in case the
        // gateway block was skipped (empty gateway_).
        memberKeyScratch_.clear();
        memberKeyScratch_.append(groupIdStr);
        memberKeyScratch_ += ':';
        memberKeyScratch_.append(memberIdStr);
        memberSessionMap_.erase(memberKeyScratch_);
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

    // CG-7 zero-alloc: assign() into pre-reserved scratch; no heap alloc once stable.
    groupIdScratch_.assign(
        reinterpret_cast<const char*>(msg.groupId.data()), msg.groupId.size());
    const std::string& groupIdStr = groupIdScratch_;

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

    // CG-7 zero-alloc: assign() into pre-reserved scratch; no heap alloc once stable.
    groupIdScratch_.assign(
        reinterpret_cast<const char*>(msg.groupId.data()), msg.groupId.size());
    const std::string& groupIdStr = groupIdScratch_;

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
        // CG-7 zero-alloc: build memberKey into pre-reserved scratch string.
        memberKeyScratch_.clear();
        memberKeyScratch_.append(groupId);
        memberKeyScratch_ += ':';
        memberKeyScratch_.append(member.memberId);
        const auto it = memberSessionMap_.find(memberKeyScratch_);
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
// onReachedStable — called from doWork/handleJoinGroup after autoAssignAndSync
// succeeds and the group transitions to Stable.
//
// Sends an Assign{gen, assignment} control frame to each member so they can
// update their local assignment maps.  Group push subscriptions are NOT created
// here; they are created in handleSyncGroup when the member's SyncGroup arrives
// with the correct generation (the moment the client is about to enter Stable).
//
// Separation of concerns:
//   - onReachedStable  → Assign frame only (best-effort delivery; may be
//                        silently dropped if the client is in Joining/Syncing
//                        state, which is fine — SyncGroupResponse carries the
//                        assignment authoritatively).
//   - handleSyncGroup  → push subscriptions created here (correct committed
//                        offset seed; see comment block there for full rationale).
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
        // CG-7 zero-alloc: build memberKey into pre-reserved scratch string.
        memberKeyScratch_.clear();
        memberKeyScratch_.append(groupId);
        memberKeyScratch_ += ':';
        memberKeyScratch_.append(member.memberId);
        const auto it = memberSessionMap_.find(memberKeyScratch_);
        if (it == memberSessionMap_.end()) {
            spdlog::warn("GroupCoordinationService::onReachedStable: "
                         "no sessionId for member '{}' in group '{}' — skipping",
                         member.memberId, groupId);
            continue;
        }
        const int64_t sessionId = it->second;

        // Build assignment for the Assign frame only (no subscription creation).
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

            // Accumulate for the Assign frame (outCap-bounded below).
            const std::size_t nextCount = assignFrameTpScratch_.size() + 1;
            const std::size_t neededForNext = pc::Assign::encodedSizeFor(nextCount);
            if (neededForNext > kAssignFrameScratchSize) {
                spdlog::warn("GroupCoordinationService::onReachedStable: "
                             "Assign frame capacity reached at {} entries (scratch={}B); "
                             "remaining partitions not in Assign frame",
                             assignFrameTpScratch_.size(), kAssignFrameScratchSize);
                break;
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
