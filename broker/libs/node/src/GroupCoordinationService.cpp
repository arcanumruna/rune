// ---------------------------------------------------------------------------
// GroupCoordinationService.cpp — CG-3 implementation.
//
// Single-writer. No locks. No per-cycle allocation in steady state.
// See GroupCoordinationService.h for design notes.
// ---------------------------------------------------------------------------

#include <rune/node/GroupCoordinationService.h>
#include <rune/node/GroupErrorMapping.h>
#include <rune/gateway/RequestRejection.h>

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

// Coordinator result → protocol ErrorCode: GroupErrorMapping.h (fix round 3).
using rune::node::joinErrorToProto;
using rune::node::heartbeatErrorToProto;
using rune::node::syncErrorToProto;

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
    if (localLeader_ >= 0) return localLeader_ == 1;
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
        // C1: a runtime topic awaiting its DDL commit is not assignable.
        if (spec && !topics_->awaitingCommit(tid)) {
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
    // The offset index is valid only for the leadership AND log content it was
    // rebuilt from, and only while the __consumer_offsets replica leads.  A new
    // leadership or a log-content change (truncation) re-arms the rebuild.
    const rune::replication::PartitionReplica* const offRep =
        offsets_ ? offsets_->replica() : nullptr;
    const bool offsetsReady = offRep == nullptr || offRep->acceptsProduce();
    const bool amLeader = isLocalCoordinator() && offsetsReady;
    if (offRep != nullptr) {
        const uint64_t gen = (offRep->leadershipGeneration() << 32) ^ offRep->logGeneration();
        if (gen != offsetsGen_) { offsetsGen_ = gen; offsetsRebuilt_ = false; }
    }
    if (amLeader) {
        if (!wasLeader_) {
            // First duty cycle after becoming coordinator — attempt the rebuild.
            wasLeader_ = true;
            offsetsRebuilt_ = false; // arm the retry for this leadership term
        }
        // CP-5.2 cold-restart fix: RETRY the offset-index rebuild every duty cycle
        // until it is EFFECTIVE (hw>0).  On a cold cluster restart the
        // __consumer_offsets HW is briefly gated at 0 while the ISR re-expands, so
        // a one-shot rebuild recovers nothing and consumer groups resume from 0.
        // rebuildFromLog() now returns false while hw<=0 (and does NOT clear the
        // index in that case), so retrying is safe and idempotent; once it runs
        // with hw>0 we latch offsetsRebuilt_ and stop retrying.  This retry window
        // is before any group resumes committing, so re-running the rebuild cannot
        // clobber a live in-memory commit.
        if (!offsetsRebuilt_ && offsets_) {
            if (offsets_->rebuildFromLog()) {
                offsetsRebuilt_ = true;
                spdlog::info("GroupCoordinationService::doWork: became coordinator "
                             "— offset index rebuilt from __consumer_offsets");
            }
        }
    } else if (!amLeader && wasLeader_) {
        // Lost leadership — mark so the next election triggers another rebuild.
        wasLeader_ = false;
        offsetsRebuilt_ = false;
    }

    if (coordinators_.empty()) return 0;

    sweepSupersededLineages_(nowNs);   // R1 (round 5)

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
// E2 residual round 5 (R1): superseded-lineage rebalances.
//
// The coordinator can have answered a member a transient offset while the
// lineage still looked valid (e.g. the partition's only replica had just died,
// before the controller detected it).  Once a NEW lineage is minted for that
// partition the offset points into data that did not survive.  Closing this
// only at OffsetFetch leaves the member consuming on from its old position and
// committing it under the new lineage (silent loss).  So, end to end:
//   - every group holding a committed offset of a superseded lineage is forced
//     to rebalance (generation bump): members revoke, re-join, re-fetch (-1:
//     the log start) — swept here on every lineage change;
//   - a commit at the old generation is fenced by the generation check
//     (IllegalGeneration, explicit); a commit that reaches us BEFORE the sweep
//     saw the change forces the rebalance itself and is refused (see
//     handleOffsetCommit).
// ---------------------------------------------------------------------------
bool GroupCoordinationService::rebalanceForSupersededLineage_(const GroupId& groupId, coord::GroupCoordinator& coord,
                                                              int32_t topicId, int32_t partition, uint64_t now,
                                                              int64_t nowNs) {
    auto& marks = supersededHandled_[groupId];
    for (const auto& m : marks)
        if (m.topicId == topicId && m.partition == partition && m.lineage == now) return false;
    marks.push_back(SupersededMark{topicId, partition, now});
    const coord::GroupState before = coord.state();
    if (!coord.forceRebalance(nowNs)) return false;
    ++supersededRebalances_;
    spdlog::info("GroupCoordinationService: group '{}' consumed / committed transient topicId={} partition={} "
                 "under a superseded data lineage (now {:#x}) — forced rebalance to generation {} (members "
                 "re-fetch: the log start)", groupId, topicId, partition, now, coord.generationId());
    if (before != coord::GroupState::PreparingRebalance) onEnterPreparingRebalance(groupId, coord);
    return true;
}

GroupCoordinationService::GenLineage* GroupCoordinationService::genLineageOf_(const std::string& groupId,
                                                                             int32_t topicId, int32_t partition) {
    const auto it = genLineages_.find(groupId);
    if (it == genLineages_.end()) return nullptr;
    for (auto& g : it->second)
        if (g.topicId == topicId && g.partition == partition) return &g;
    return nullptr;
}

void GroupCoordinationService::recordGenLineages_(const GroupId& groupId, const coord::GroupCoordinator& coord) {
    if (lineageFn_ == nullptr || topics_ == nullptr) return;
    auto& v = genLineages_[groupId];   // cold: once per rebalance
    v.clear();
    for (const auto& m : coord.members()) {
        for (const auto& tp : m.assignment) {
            const auto id = topics_->topicIdForName(tp.topic);
            if (!id) continue;
            const uint64_t l = lineageOf_(*id, tp.partition);
            if (l == 0) continue;   // a disk partition
            bool dup = false;
            for (const auto& g : v) dup = dup || (g.topicId == *id && g.partition == tp.partition);
            if (!dup) v.push_back(GenLineage{*id, tp.partition, l});
        }
    }
}

void GroupCoordinationService::pruneGroupLineageState_(const std::string& groupId) {
    supersededHandled_.erase(groupId);
    genLineages_.erase(groupId);
    std::erase_if(pendingChecks_, [&](const PendingCheck& p) { return p.groupId == groupId; });
}

bool GroupCoordinationService::checkSuperseded_(const std::string& groupId, int32_t topicId, int32_t partition,
                                                uint64_t ref, bool genEntry, int64_t nowNs) {
    const uint64_t now = lineageOf_(topicId, partition);
    if (now == kLineagePending) return false;
    if (now == 0) return true;
    if (genEntry) {
        GenLineage* g = genLineageOf_(groupId, topicId, partition);
        if (g == nullptr) return true;   // re-recorded / pruned since
        if (g->lineage == kLineagePending) { g->lineage = now; return true; }   // the first confirmed: consumed
        ref = g->lineage;
    }
    if (ref == now) return true;
    const auto it = coordinators_.find(groupId);
    if (it != coordinators_.end() && !it->second.isEmpty())
        (void)rebalanceForSupersededLineage_(it->first, it->second, topicId, partition, now, nowNs);
    return true;
}

void GroupCoordinationService::retryPendingChecks_(int64_t nowNs) {
    if (pendingChecks_.empty() || nowNs < nextLineageSweepNs_) return;
    std::size_t kept = 0;
    for (std::size_t i = 0; i < pendingChecks_.size(); ++i) {
        PendingCheck& p = pendingChecks_[i];
        if (!checkSuperseded_(p.groupId, p.topicId, p.partition, p.ref, p.genEntry, nowNs)) {
            if (kept != i) pendingChecks_[kept] = std::move(p);
            ++kept;
        }
    }
    pendingChecks_.resize(kept);   // shrinks only: no allocation
    nextLineageSweepNs_ = nowNs + 100'000'000;
}

void GroupCoordinationService::sweepSupersededLineages_(int64_t nowNs) {
    if (lineageFn_ == nullptr || offsets_ == nullptr || !isLocalCoordinator() || !offsetsLoaded_()) return;
    const uint64_t v = lineageVersionFn_ != nullptr ? lineageVersionFn_(lineageVersionCtx_) : 0;
    if (v == lineageSweptVersion_) {   // no lineage change: only the undecided ones (F4: no fetchAll)
        retryPendingChecks_(nowNs);
        return;
    }
    // A lineage changed (an event: a restart / a leadership move) — cold.
    pendingChecks_.clear();
    for (auto& [groupId, coord] : coordinators_) {
        if (coord.isEmpty()) { pruneGroupLineageState_(groupId); continue; }   // F4: nothing live to fence
        // (a) the partitions its current generation consumes (F1: no commit needed)
        if (auto gl = genLineages_.find(groupId); gl != genLineages_.end()) {
            for (const auto& g : gl->second)
                if (!checkSuperseded_(groupId, g.topicId, g.partition, 0, /*genEntry=*/true, nowNs))
                    pendingChecks_.push_back(PendingCheck{groupId, g.topicId, g.partition, 0, true});
        }
        // (b) its committed offsets (R1)
        for (const auto& [tp, co] : offsets_->fetchAll(groupId)) {
            if (co.lineage == 0 || topics_ == nullptr) continue;
            const auto id = topics_->topicIdForName(tp.topic);
            if (!id) continue;
            if (!checkSuperseded_(groupId, *id, tp.partition, co.lineage, /*genEntry=*/false, nowNs))
                pendingChecks_.push_back(PendingCheck{groupId, *id, tp.partition, co.lineage, false});
        }
    }
    lineageSweptVersion_ = v;
    nextLineageSweepNs_  = nowNs + 100'000'000;
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
    const uint8_t* buf, size_t len, int64_t nowNs, uint8_t* out, size_t outCap)
{
    auto msgOpt = pc::JoinGroup::decode(buf, len);
    if (!msgOpt) {
        // No-hang (R2): answered JoinGroupResponse{InvalidRequest}, never dropped.
        spdlog::debug("GroupCoordinationService: JoinGroup decode failed — answered InvalidRequest");
        return rune::gateway::encodeRejectionFor(
            buf, len, static_cast<uint16_t>(pc::ErrorCode::InvalidRequest), out, outCap);
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

    // CG routing round 3 (MAJOR-2): a member below client version 3 relies on
    // the coordinator-created subscriptions this broker no longer makes (and
    // demultiplexes Records without the creating Subscribe's correlationId) —
    // refuse it, non-retriably, instead of serving it duplicates / losses.
    if (gateway_) {
        const int32_t v = gateway_->sessionClientVersion(msg.sessionId);
        if (v >= 0 && v < pc::kClientVersionExplicitGroupSubscriptions) {
            // LOW-3: an old client re-joins forever — log the first refusal of a
            // session and then every power of two (bounded log volume).
            if (oldClientRefusals_.size() >= 4096) oldClientRefusals_.clear();  // bounded (log only)
            const uint64_t n = ++oldClientRefusals_[msg.sessionId];
            if ((n & (n - 1)) == 0) spdlog::warn("GroupCoordinationService: JoinGroup from session {} refused — client version {} < {} "
                         "(explicit group subscriptions required); upgrade the client",
                         msg.sessionId, v, pc::kClientVersionExplicitGroupSubscriptions);
            pc::JoinGroupResponse resp;
            resp.sessionId     = msg.sessionId;
            resp.correlationId = msg.correlationId;
            resp.errorCode     = static_cast<int16_t>(pc::ErrorCode::ProtocolVersion);
            resp.generationId  = 0;
            resp.isLeader      = 0;
            resp.memberId      = std::span<const uint8_t>();
            return resp.encode(out);
        }
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
    const uint8_t* buf, size_t len, int64_t nowNs, uint8_t* out, size_t outCap)
{
    auto msgOpt = pc::SyncGroup::decode(buf, len);
    if (!msgOpt) {
        // No-hang (R2): answered SyncGroupResponse{InvalidRequest}, never dropped.
        spdlog::debug("GroupCoordinationService: SyncGroup decode failed — answered InvalidRequest");
        return rune::gateway::encodeRejectionFor(
            buf, len, static_cast<uint16_t>(pc::ErrorCode::InvalidRequest), out, outCap);
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

    // CG routing redesign (2026-10-04): the coordinator NO LONGER creates the
    // member's push subscriptions at SyncGroup.  With distributed partition
    // leadership a partition's records come from its LEADER node; the member
    // subscribes there explicitly (after its OffsetFetch answer, from the
    // committed offset, or the log start when there is none).  A coordinator-created
    // subscription pushed from the SyncGroup moment, before the member knew its
    // position — those records were dropped and the subscription's cursor ran
    // ahead of the member (stalled / lost partitions).  The eager revoke at
    // PreparingRebalance (onEnterPreparingRebalance) and revokeGroupSubscriptions
    // stay: they are no-ops for members without coordinator-created subs.

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
    const uint8_t* buf, size_t len, int64_t nowNs, uint8_t* out, size_t outCap)
{
    auto msgOpt = pc::Heartbeat::decode(buf, len);
    if (!msgOpt) {
        // No-hang (R2): answered HeartbeatResponse{InvalidRequest}, never dropped.
        spdlog::debug("GroupCoordinationService: Heartbeat decode failed — answered InvalidRequest");
        return rune::gateway::encodeRejectionFor(
            buf, len, static_cast<uint16_t>(pc::ErrorCode::InvalidRequest), out, outCap);
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
    const uint8_t* buf, size_t len, int64_t nowNs, uint8_t* out, size_t outCap)
{
    auto msgOpt = pc::LeaveGroup::decode(buf, len);
    if (!msgOpt) {
        // No-hang (R2): answered LeaveGroupResponse{InvalidRequest}, never dropped.
        spdlog::debug("GroupCoordinationService: LeaveGroup decode failed — answered InvalidRequest");
        return rune::gateway::encodeRejectionFor(
            buf, len, static_cast<uint16_t>(pc::ErrorCode::InvalidRequest), out, outCap);
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
// evictSessionMembers — no-hang fix round 4 (item 8, pre-v5 clients).
//
// A v4 client stays Stable on a Heartbeat answer outside the rebalance set
// (Internal / InvalidRequest): the gateway answers it UnknownMember instead and
// calls this, so the answer is true — the member is gone (as on LeaveGroup),
// the v4 member re-joins, and any split-brain ends with that paced re-join.
// Cold (an error answer to an old client only).
// ---------------------------------------------------------------------------
void GroupCoordinationService::evictMember(std::string_view groupId, std::string_view memberId,
                                           int64_t sessionId, int64_t nowNs)
{
    // No-hang fix round 5 (N23): scoped to the request's group (and member) —
    // a failing group A on a session never evicts that session's member of a
    // healthy group B (which would keep B rebalancing for as long as A fails).
    // Subscriptions are not revoked here: group members subscribe explicitly
    // (CG routing redesign) and drop their own subscriptions on re-join;
    // revokeGroupSubscriptions is session-wide and would hit group B.
    auto cit = coordinators_.find(groupId);
    if (cit == coordinators_.end()) return;
    coord::GroupCoordinator& coord = cit->second;
    evictScratch_.clear();
    for (const auto& member : coord.members()) {
        if (!memberId.empty() && member.memberId != memberId) continue;
        memberKeyScratch_.clear();
        memberKeyScratch_.append(groupId);
        memberKeyScratch_ += ':';
        memberKeyScratch_.append(member.memberId);
        const auto it = memberSessionMap_.find(memberKeyScratch_);
        if (it != memberSessionMap_.end() && it->second == sessionId) evictScratch_.push_back(member.memberId);
    }
    for (const auto& m : evictScratch_) {
        spdlog::warn("GroupCoordinationService: old client (session {}) member '{}' of group '{}' evicted — "
                     "answered UnknownMember instead of an error it would ignore", sessionId, m, groupId);
        coord.leave(m, coord.generationId(), nowNs);
        memberKeyScratch_.clear();
        memberKeyScratch_.append(groupId);
        memberKeyScratch_ += ':';
        memberKeyScratch_.append(m);
        memberSessionMap_.erase(memberKeyScratch_);
    }
}

void GroupCoordinationService::evictSessionMembers(int64_t sessionId, int64_t nowNs)
{
    for (auto& [groupId, coord] : coordinators_) {
        evictScratch_.clear();
        for (const auto& member : coord.members()) {
            memberKeyScratch_.clear();
            memberKeyScratch_.append(groupId);
            memberKeyScratch_ += ':';
            memberKeyScratch_.append(member.memberId);
            const auto it = memberSessionMap_.find(memberKeyScratch_);
            if (it != memberSessionMap_.end() && it->second == sessionId) evictScratch_.push_back(member.memberId);
        }
        if (evictScratch_.empty()) continue;
        if (gateway_) gateway_->revokeGroupSubscriptions(sessionId);
        for (const auto& memberId : evictScratch_) {
            spdlog::warn("GroupCoordinationService: old client (session {}) member '{}' of group '{}' evicted — "
                         "answered UnknownMember instead of an error it would ignore", sessionId, memberId, groupId);
            coord.leave(memberId, coord.generationId(), nowNs);
            memberKeyScratch_.clear();
            memberKeyScratch_.append(groupId);
            memberKeyScratch_ += ':';
            memberKeyScratch_.append(memberId);
            memberSessionMap_.erase(memberKeyScratch_);
        }
    }
}

// ---------------------------------------------------------------------------
// E2 final fix wave (C1): a transient partition's committed offset is a
// position of ONE data lineage — answered only while that lineage lives.
// ---------------------------------------------------------------------------
bool GroupCoordinationService::staleLineage_(const rune::coordination::CommittedOffset& co, int32_t topicId,
                                             int32_t partition)
{
    const uint64_t now = lineageOf_(topicId, partition);
    if (now == 0 || now == kLineagePending || co.lineage == now) return false;   // not transient / pending / same
    if ((staleLineageOffsets_++ & 1023u) == 0)
        spdlog::info("GroupCoordinationService: committed offset {} of transient topicId={} partition={} "
                     "belongs to data that did not survive (lineage {:#x}, now {:#x}) — answered as no "
                     "committed offset; the member starts at the log start ({} so far)",
                     co.offset, topicId, partition, co.lineage, now, staleLineageOffsets_);
    return true;
}

// ---------------------------------------------------------------------------
// handleOffsetCommit (templateId 40)
// ---------------------------------------------------------------------------
size_t GroupCoordinationService::handleOffsetCommit(
    const uint8_t* buf, size_t len, int64_t nowNs, uint8_t* out, size_t outCap)
{
    auto msgOpt = pc::OffsetCommit::decode(buf, len);
    if (!msgOpt) {
        // No-hang (R2): answered OffsetCommitResponse{InvalidRequest}, never dropped.
        spdlog::debug("GroupCoordinationService: OffsetCommit decode failed — answered InvalidRequest");
        return rune::gateway::encodeRejectionFor(
            buf, len, static_cast<uint16_t>(pc::ErrorCode::InvalidRequest), out, outCap);
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
    bool genOk = groupExists && (msg.generationId == it->second.generationId());   // R1: cleared by a forced rebalance

    commitResultScratch_.clear();
    const bool loaded = offsetsLoaded_();   // E2 residual round 4

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

        if (!loaded) [[unlikely]] {
            // Round 4: the offsets are not loaded yet — a commit now would be
            // wiped by the rebuild (clear + replay): refused retriably.
            ++offsetsNotLoadedAnswers_;
            r.errorCode = static_cast<int16_t>(pc::ErrorCode::NotLeader);
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

        const uint64_t lineage = lineageOf_(e.topicId, e.partition);   // C1
        if (lineage == kLineagePending) [[unlikely]] {
            // The partition's current leadership has not confirmed its data
            // lineage yet: no stamp could be right — refused retriably.
            ++pendingLineageAnswers_;
            r.errorCode = static_cast<int16_t>(pc::ErrorCode::NotLeader);
            commitResultScratch_.push_back(r);
            continue;
        }
        if (lineage != 0) {
            // R1 (round 5): the group's committed position for this partition is
            // of a SUPERSEDED lineage and the group was not rebalanced for the new
            // one yet — this member's position may come from the old data: the
            // group is rebalanced now and the commit refused (explicit; the
            // member re-joins and re-fetches).  After that rebalance an old-
            // generation commit is fenced by genOk above.
            const auto prev = offsets_->fetch(groupIdStr, tp);
            // Round 6 (F1): or this generation consumed the partition under a
            // lineage that has been superseded since — even with NO committed
            // offset (an in-memory position): the generation predates the mint.
            GenLineage* gl = genLineageOf_(groupIdStr, e.topicId, e.partition);
            if (gl != nullptr && gl->lineage == kLineagePending) gl->lineage = lineage;   // first confirmed
            const bool genSuperseded = gl != nullptr && gl->lineage != lineage;
            const bool prevSuperseded = prev.has_value() && prev->lineage != 0 && prev->lineage != lineage;
            if ((genSuperseded || prevSuperseded) &&
                rebalanceForSupersededLineage_(groupIdStr, it->second, e.topicId, e.partition, lineage, nowNs)) {
                ++supersededCommitsFenced_;
                genOk = false;   // the rest of this request is at the old generation now
                r.errorCode = static_cast<int16_t>(pc::ErrorCode::IllegalGeneration);
                commitResultScratch_.push_back(r);
                continue;
            }
        }
        const bool committed = offsets_->commit(groupIdStr, tp, e.offset, meta, nowNs, lineage);
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
        // No-hang (R2): answered OffsetFetchResponse{InvalidRequest}, never dropped.
        spdlog::debug("GroupCoordinationService: OffsetFetch decode failed — answered InvalidRequest");
        return rune::gateway::encodeRejectionFor(
            buf, len, static_cast<uint16_t>(pc::ErrorCode::InvalidRequest), out, outCap);
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

    if (!offsetsLoaded_()) [[unlikely]] {
        // E2 residual round 4: the offsets of this coordinator's
        // __consumer_offsets partition are not loaded yet (a restart / a new
        // leadership; the rebuild waits for the whole log).  Neither an offset
        // nor "none" can be answered — every entry (or one sentinel for a
        // fetch-all) gets the retriable NotLeader; the client retries.
        auto refuse = [&](int32_t t, int32_t p) {
            if (pc::OffsetFetchResponse::encodedSizeFor(fetchResultScratch_.size() + 1) > outCap) return;
            pc::OffsetFetchResult r;
            r.topicId   = t;
            r.partition = p;
            r.offset    = -1;
            r.errorCode = static_cast<int16_t>(pc::ErrorCode::NotLeader);
            ++offsetsNotLoadedAnswers_;
            fetchResultScratch_.push_back(r);
        };
        for (const auto& e : msg.entries) refuse(e.topicId, e.partition);
        if (fetchResultScratch_.empty()) refuse(-1, -1);
        pc::OffsetFetchResponse resp;
        resp.sessionId     = msg.sessionId;
        resp.correlationId = msg.correlationId;
        return resp.encode(out, std::span<const pc::OffsetFetchResult>(fetchResultScratch_));
    }

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
            const uint64_t lineage = lineageOf_(topicId, tp.partition);   // C1
            if (lineage != kLineagePending && staleLineage_(co, topicId, tp.partition)) continue;   // as if never committed

            pc::OffsetFetchResult r;
            r.topicId   = topicId;
            r.partition = tp.partition;
            r.offset    = co.offset;
            r.errorCode = static_cast<int16_t>(pc::ErrorCode::OK);
            if (lineage == kLineagePending) [[unlikely]] {   // not answerable yet: retriable
                ++pendingLineageAnswers_;
                r.offset    = -1;
                r.errorCode = static_cast<int16_t>(pc::ErrorCode::NotLeader);
            }
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
                if (lineageOf_(e.topicId, e.partition) == kLineagePending) [[unlikely]] {
                    // C1: the current leadership has not confirmed the lineage
                    // yet — neither the offset nor "none" can be answered.
                    ++pendingLineageAnswers_;
                    r.offset    = -1;
                    r.errorCode = static_cast<int16_t>(pc::ErrorCode::NotLeader);
                } else {
                    const auto co = offsets_->fetch(groupIdStr, tp);
                    r.offset = co.has_value() && !staleLineage_(*co, e.topicId, e.partition) ? co->offset
                                                                                             : int64_t{-1};
                }
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
    // Fix round 5: an Assign frame (sent reliably through sendControlFrame)
    // always fits the gateway spill's staging buffer.
    static_assert(kAssignFrameScratchSize <= rune::gateway::ClientGateway::spillSizing().stage,
                  "Assign frames must be spillable");
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
    recordGenLineages_(groupId, coord);   // round 6 (F1): the lineages this generation consumes
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
