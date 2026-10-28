// ---------------------------------------------------------------------------
// GroupCoordinationService.cpp — CG-3 implementation.
//
// Single-writer. No locks. No per-cycle allocation in steady state.
// See GroupCoordinationService.h for design notes.
// ---------------------------------------------------------------------------

#include <rune/node/GroupCoordinationService.h>
#include <rune/node/GroupErrorMapping.h>
#include <rune/gateway/RequestRejection.h>
#include <rune/gateway/NsResolve.h>

#include <rune/gateway/ClientGateway.h>  // CG-4: addGroupSubscription / revokeGroupSubscriptions / sendControlFrame
#include <rune/crypto/Kdf.h>             // issues #123 / #124: the random half of member ids

#include <rune/codec/FilterWire.h>
#include <rune/protocol/client/GroupMessages.h>
#include <rune/protocol/client/OffsetMessages.h>
#include <rune/protocol/client/Protocol.h>
#include <rune/node/NodeConfig.h>  // TopicSpec (for TopicRegistry lookup)

#include <spdlog/spdlog.h>

#include <algorithm>
#include <charconv>
#include <cstring>
#include <span>

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
    parkedResults_.resize(kParkedResultsCapacity);   // #433: the parked answers' results ring (once)
}

// ---------------------------------------------------------------------------
// isLocalCoordinator
// ---------------------------------------------------------------------------
bool GroupCoordinationService::isLocalCoordinator() const {
    if (localLeader_ >= 0) return localLeader_ == 1;
    return isLeaderFn_ ? isLeaderFn_() : true;
}

// ---------------------------------------------------------------------------
// tombstoneNamespace (Phase 2 Task 7)
// ---------------------------------------------------------------------------
ReapStatus GroupCoordinationService::tombstoneNamespace(uint32_t nsId, uint32_t batch, int64_t nowNs) {
    if (offsets_ == nullptr) return ReapStatus::Done;   // no offset store (a rig): nothing to remove
    if (!isLocalCoordinator() || !offsetsLoaded_()) return ReapStatus::Refused;
    rune::replication::PartitionReplica* r = offsets_->replica();
    const rune::replication::Offset leo0 = r->logEndOffset();
    const bool none = offsets_->tombstoneNamespace(nsId, batch, nowNs);
    if (!none) return r->logEndOffset() > leo0 ? ReapStatus::More : ReapStatus::Refused;
    return r->highWatermark() >= r->logEndOffset() ? ReapStatus::Done : ReapStatus::More;   // all committed
}

std::optional<std::size_t> GroupCoordinationService::liveKeysOfNamespaceForTest(uint32_t nsId) const {
    if (offsets_ == nullptr || !isLocalCoordinator() || !offsetsLoaded_()) return std::nullopt;   // unknown: not loaded / not the coordinator
    return offsets_->liveKeysOfNamespaceForTest(nsId);
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
    if (coordinators_.empty()) groupsTerm_ = currentTerm_();   // #570: the first group fixes the groups' term
    auto [inserted, ok] = coordinators_.emplace(
        std::piecewise_construct,
        std::forward_as_tuple(groupId),
        std::forward_as_tuple(groupId, /*nowNs=*/0));
    (void)ok;
    inserted->second.setMemberIdEntropy(
        [](uint8_t* out, std::size_t n) noexcept { return rune::crypto::randomBytes(std::span<uint8_t>(out, n)); });
    return inserted->second;
}

// ---------------------------------------------------------------------------
// Issues #123 / #124 / #169: member ownership (see memberSessionMap_).
// ---------------------------------------------------------------------------
void GroupCoordinationService::buildMemberKey_(std::string_view groupId, std::string_view memberId) {
    // PR #423 review F2: "<group length>:<group><member>" - unambiguous for any bytes in either id (a ':' separator
    // alone let group "x" + member "y:m" alias group "x:y" + member "m").  No allocation once the capacity is reached.
    char len[8];
    const auto [end, ec] = std::to_chars(len, len + sizeof len, groupId.size());
    (void)ec;   // a u16-prefixed wire field: at most 5 digits
    memberKeyScratch_.clear();
    memberKeyScratch_.append(len, static_cast<std::size_t>(end - len));
    memberKeyScratch_ += ':';
    memberKeyScratch_.append(groupId);
    memberKeyScratch_.append(memberId);
}

const GroupCoordinationService::MemberBinding* GroupCoordinationService::bindingOf_(std::string_view groupId,
                                                                                    std::string_view memberId) {
    if (memberId.empty()) return nullptr;
    buildMemberKey_(groupId, memberId);
    const auto it = memberSessionMap_.find(memberKeyScratch_);
    return it == memberSessionMap_.end() ? nullptr : &it->second;
}

uint64_t GroupCoordinationService::principalOf_(int64_t sessionId) const {
    if (principalFn_ != nullptr) return principalFn_(principalCtx_, sessionId);
    return gateway_ != nullptr ? gateway_->sessionPrincipalId(sessionId) : 0;
}

void GroupCoordinationService::drainDeparted_(std::string_view groupId, coord::GroupCoordinator& gc) {
    if (!gc.hasDeparted()) return;
    gc.takeDeparted(departedScratch_);
    for (const std::string& m : departedScratch_) {
        if (gc.hasMember(m)) continue;   // re-joined since (same id, its owner): keep the binding
        buildMemberKey_(groupId, m);
        memberSessionMap_.erase(memberKeyScratch_);
    }
    departedScratch_.clear();
}

// ---------------------------------------------------------------------------
// buildTopicPartitionCounts — cold path; called only on join-timeout transition.
// ---------------------------------------------------------------------------
bool GroupCoordinationService::topicIdAssignable_(int32_t tid) const {
    // C1 (catalog history Task 9 fix round 1): a catalog topic applied but not PLACED yet is not assignable (the
    // gateway would refuse its partitions NotPlaced) — it joins the next rebalance once placed.
    return topics_ != nullptr && topics_->topic(tid) != nullptr && !topics_->isViewSink(tid) &&
           (gateway_ == nullptr || gateway_->topicAssignable(tid));
}

coord::TopicPartitionCounts GroupCoordinationService::buildTopicPartitionCounts(uint32_t nsId) const {
    coord::TopicPartitionCounts counts;
    if (!topics_) return counts;

    const int32_t n = topics_->topicCount();
    for (int32_t tid = 0; tid < n; ++tid) {
        if (!topicIdAssignable_(tid)) continue;
        const rune::node::TopicSpec* spec = topics_->topic(tid);
        if (spec->nsId != nsId) continue;   // D19: a group assigns only its own namespace's topics (same local name in two namespaces)
        counts[spec->name] = spec->partitionCount;
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

    int work = parkedCount_ != 0 ? releaseParkedCommits_() : 0;   // #433

    // #565: a node that is not the coordinator holds no group: its in-memory groups are stale (heartbeats now go to
    // the new coordinator) and ageing them would expire members, bump generations and send revoke frames.  Unload
    // them, Kafka-style; a regained role rebuilds each group by re-join (group membership is never persisted).
    work += syncTerm_();   // #570: a role lost and regained between two cycles still changed the term
    if (!coordinators_.empty() && !isLocalCoordinator()) return work + unloadGroups_("not the group coordinator");

    if (coordinators_.empty()) return work;

    sweepSupersededLineages_(nowNs);   // R1 (round 5)

    for (auto& [groupId, coord] : coordinators_) {
        const coord::GroupState stateBefore = coord.state();

        refreshRebalanceTimeout_(coord);   // #499: a departed long-timeout member stops counting
        const int w = coord.doWork(nowNs);
        work += w;
        drainDeparted_(groupId, coord);   // #124: expired / timed-out members lose their binding

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
            const coord::TopicPartitionCounts partCounts = buildTopicPartitionCounts(rune::coordination::parseScopedId(groupId).nsId);
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

        // #549: a topic a JoinGroup named before it was assignable here (catalog create not applied yet, or not
        // placed) was left out of the member's subscription.  Once it is assignable and no member holds it, force
        // one rebalance: the members re-join and resolve it.  State-driven, no timer (rule 16).
        // PR #564 review: only the coordinator rebalances (a stale group on an ex-coordinator must never revoke its
        // members), an emptied / Dead group forgets its ids, an id that arrives as a view sink rebalances too (the
        // re-join gets #19's refusal, never a silently empty assignment), and an id already held is resolved.
        if (!unassignedJoinTopics_.empty()) {
            if (const auto it = unassignedJoinTopics_.find(groupId); it != unassignedJoinTopics_.end()) {
                if (coord.isEmpty() || coord.state() == coord::GroupState::Dead) {
                    unassignedJoinTopics_.erase(it);
                } else if (coord.state() == coord::GroupState::Stable && isLocalCoordinator()) {
                    bool ready = false;
                    std::erase_if(it->second, [&](int32_t tid) {
                        if (topics_ != nullptr && topics_->isViewSink(tid)) { ready = true; return true; }
                        if (!topicIdAssignable_(tid)) return false;   // still waiting
                        const std::string& name = topics_->topic(tid)->name;
                        bool held = false;
                        for (const auto& m : coord.members())
                            for (const auto& tp : m.assignment) held = held || tp.topic == name;
                        ready = ready || !held;
                        return true;   // assignable: resolved by this check either way
                    });
                    if (ready || it->second.empty()) unassignedJoinTopics_.erase(it);   // re-joins re-record
                    if (ready && coord.forceRebalance(nowNs)) {
                        onEnterPreparingRebalance(groupId, coord);
                        spdlog::info("GroupCoordinationService: group '{}' — a topic its members joined for became "
                                     "assignable; rebalancing (generation {}) (#549)", groupId, coord.generationId());
                        ++work;
                    }
                }
            }
        }
    }

    // #214: an Empty group holds nothing worth keeping (membership is never persisted; its committed offsets live in
    // the offsets store; its departed members' bindings were drained above): drop it, with its per-group state.  A
    // departed member's late request is answered UnknownMember (no group), exactly as for a member-less one.  Cold.
    for (auto it = coordinators_.begin(); it != coordinators_.end();) {
        const coord::GroupCoordinator& gc = it->second;
        if (gc.isEmpty() && gc.state() == coord::GroupState::Empty && !gc.hasDeparted()) {
            pruneGroupLineageState_(it->first);
            if (!filterMismatchRefusals_.empty()) {
                const std::string& g = it->first;
                std::erase_if(filterMismatchRefusals_, [&](const auto& e) {
                    return e.first.size() > g.size() && e.first.compare(0, g.size(), g) == 0 && e.first[g.size()] == '\x1f';
                });
            }
            it = coordinators_.erase(it);
            ++groupsPruned_;
            ++work;
        } else {
            ++it;
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

std::size_t GroupCoordinationService::onTopicDropped(std::string_view topicName, int64_t nowNs, uint32_t nsId) {
    if (!isLocalCoordinator()) return 0;
    std::size_t n = 0;
    for (auto& [groupId, coord] : coordinators_) {
        if (rune::coordination::parseScopedId(groupId).nsId != nsId) continue;   // D19: only that namespace's groups
        bool holds = false;
        for (const auto& m : coord.members())
            for (const auto& tp : m.assignment) holds = holds || tp.topic == topicName;
        // Already rebalancing: its next assignment is computed from the
        // registry, which no longer lists the topic (idempotent).
        if (!holds || coord.state() == coord::GroupState::PreparingRebalance) continue;
        if (!coord.forceRebalance(nowNs)) continue;
        onEnterPreparingRebalance(groupId, coord);
        spdlog::info("GroupCoordinationService: topic '{}' dropped — group '{}' rebalances it away (generation {})",
                     topicName, groupId, coord.generationId());
        ++n;
    }
    return n;
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
            const auto id = topics_->topicIdForName(rune::coordination::parseScopedId(groupId).nsId, tp.topic);
            if (!id) continue;
            const uint64_t l = lineageOf_(*id, tp.partition);
            if (l == 0) continue;   // a disk partition
            bool dup = false;
            for (const auto& g : v) dup = dup || (g.topicId == *id && g.partition == tp.partition);
            if (!dup) v.push_back(GenLineage{*id, tp.partition, l});
        }
    }
}

// #565: unload every group on a node that is not the coordinator.  The members' group push subscriptions end (no
// stale deliveries); no Assign frame is sent - a member learns NotCoordinator on its next request and re-discovers
// the coordinator.  Parked commits, transactional offsets in flight and the offsets store are left alone (they keep
// their own NotCoordinator handling).  Cold: once per lost role.
// #499: the rebalance window is the largest session timeout among the CURRENT members (each already clamped into the
// coordinator's bounds).  A member-less group keeps its value (nothing to wait for).  O(members), no allocation.
void GroupCoordinationService::refreshRebalanceTimeout_(coord::GroupCoordinator& gc) noexcept {
    int32_t maxMs = 0;
    for (const auto& m : gc.members()) maxMs = std::max(maxMs, m.sessionTimeoutMs);
    if (maxMs > 0) gc.setRebalanceTimeoutMs(maxMs);
}

// #570: the groups belong to the term they were loaded under.  A role lost and regained between two duty cycles is
// never sampled as lost, but the term moved: unload exactly as on a lost role (members re-join the new term).
int GroupCoordinationService::syncTerm_() {
    if (!coordinators_.empty() && currentTerm_() != groupsTerm_)
        return unloadGroups_("the coordinator term changed since the groups were loaded");
    return 0;
}

int GroupCoordinationService::unloadGroups_(const char* why) {
    if (gateway_) {
        int64_t last = -1;
        for (const auto& [key, binding] : memberSessionMap_) {
            if (binding.sessionId == last) continue;   // session-wide and idempotent
            gateway_->revokeGroupSubscriptions(binding.sessionId);
            last = binding.sessionId;
        }
    }
    memberSessionMap_.clear();
    for (const auto& [groupId, coord] : coordinators_) pruneGroupLineageState_(groupId);
    const auto n = coordinators_.size();
    coordinators_.clear();
    spdlog::info("GroupCoordinationService: {} — unloaded {} group(s) (#565 / #570)", why, n);
    return static_cast<int>(n);
}

void GroupCoordinationService::pruneGroupLineageState_(const std::string& groupId) {
    supersededHandled_.erase(groupId);
    unassignedJoinTopics_.erase(groupId);   // #549: an empty group has nothing to assign
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
            const auto id = topics_->topicIdForName(rune::coordination::parseScopedId(groupId).nsId, tp.topic);
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
// Returns empty string if topics_ is null, topicId is out of range, or the topic belongs to another namespace than
// `nsId` (Review U2: a cross-namespace reference is "does not exist", for everyone).
// ---------------------------------------------------------------------------
static std::string topicNameForId(rune::gateway::TopicRegistry* topics, uint32_t nsId, int32_t topicId) {
    if (!topics) return {};
    const rune::node::TopicSpec* spec = topics->topic(topicId);
    if (!spec || spec->nsId != nsId) return {};
    return spec->name;
}

// ===========================================================================
// Handlers
// ===========================================================================

namespace {

// The old-client rule of ClientGateway::answerRejection_: a client below 5 re-joins forever on InvalidRequest.
pc::ErrorCode rejectionCodeFor(int32_t clientVersion) noexcept {
    return pc::clientUnderstandsInvalidRequest(clientVersion) ? pc::ErrorCode::InvalidRequest : pc::ErrorCode::ProtocolVersion;
}

// "group filter mismatch: the group uses filter <hex>" (at most 128 bytes shown, then "... (<n> bytes)"), "none" when empty.
std::string groupFilterMismatchDetail(std::span<const uint8_t> f) {
    std::string s(pc::kGroupFilterMismatchPrefix);
    s += ": the group uses filter ";
    if (f.empty()) { s += "none"; return s; }
    static constexpr char kHex[] = "0123456789abcdef";
    const std::size_t shown = std::min<std::size_t>(f.size(), 128);
    for (std::size_t i = 0; i < shown; ++i) { s += kHex[f[i] >> 4]; s += kHex[f[i] & 0xF]; }
    if (shown < f.size()) s += "... (" + std::to_string(f.size()) + " bytes)";
    return s;
}

}  // namespace

// Log rate-limit for filter-mismatch refusals: first per (group, session), then every power of two (bounded, cold).
void GroupCoordinationService::noteFilterMismatch_(const std::string& groupId, int64_t sessionId) {
    if (filterMismatchRefusals_.size() >= 4096) filterMismatchRefusals_.clear();
    std::string key = groupId;
    key += '\x1f';
    key += std::to_string(sessionId);
    const uint64_t n = ++filterMismatchRefusals_[key];
    if ((n & (n - 1)) == 0)
        spdlog::warn("GroupCoordinationService: JoinGroup from session {} refused — group '{}' uses a different filter "
                     "(refusal #{})", sessionId, groupId, n);
}

// ---------------------------------------------------------------------------
// handleJoinGroup (templateId 32)
// ---------------------------------------------------------------------------
size_t GroupCoordinationService::handleJoinGroup(
    const uint8_t* buf, size_t len, int32_t clientVersion, int64_t nowNs, uint8_t* out, size_t outCap,
    const rune::coordination::NsRequestContext& ctx)
{
    auto msgOpt = pc::JoinGroup::decode(buf, len, clientVersion);
    if (!msgOpt) {
        // No-hang (R2): answered JoinGroupResponse{InvalidRequest}, never dropped.
        spdlog::debug("GroupCoordinationService: JoinGroup decode failed — answered InvalidRequest");
        return rune::gateway::encodeRejectionFor(
            buf, len, static_cast<uint16_t>(pc::ErrorCode::InvalidRequest), out, outCap);
    }
    const pc::JoinGroup& msg = *msgOpt;
    if (rune::coordination::reservedClientId(idView_(msg.groupId))) [[unlikely]]   // D19: the scoped-id prefix is reserved
        return rune::gateway::encodeRejectionFor(
            buf, len, static_cast<uint16_t>(pc::ErrorCode::InvalidRequest), out, outCap);

    // CG-7 zero-alloc: assign() into pre-reserved scratch strings; no heap alloc
    // when capacity >= size (capacity grows on first long-string call, then stable).
    rune::gateway::scopeClientIdInto(ctx, idView_(msg.groupId), groupIdScratch_);   // D19: namespace-scoped id
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
    (void)syncTerm_();   // #570

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

    // Phase 2c (spec 13.3): one filter per group, fixed by the first member, changeable once the group is empty.
    // The session's version is the one the frame was decoded with: an older member carries no filter.
    const int32_t ver = clientVersion;
    const std::span<const uint8_t> filter = pc::clientUnderstandsFilters(ver) ? msg.filter : std::span<const uint8_t>{};
    // A refused join: never admitted, no state change.  Old clients get the rule's ProtocolVersion; the detail
    // (cold; reserved member string) goes to a session >= 11 only.
    auto refuseJoin = [&](std::string detail, bool nsLimit = false) {
        pc::JoinGroupResponse resp;
        resp.sessionId     = msg.sessionId;
        resp.correlationId = msg.correlationId;
        // Namespaces Phase 2 Task 9: a namespace limit is NamespaceLimitExceeded (52) for a v14+ member, else the old rule.
        resp.errorCode     = static_cast<int16_t>(nsLimit && ver >= pc::kClientVersionNamespaces ? pc::ErrorCode::NamespaceLimitExceeded
                                                                                                  : rejectionCodeFor(ver));
        resp.generationId  = 0;
        resp.isLeader      = 0;
        resp.memberId      = std::span<const uint8_t>();
        if (pc::clientUnderstandsFilters(ver)) {
            filterDetailScratch_ = std::move(detail);
            resp.detail = filterDetailScratch_;
        }
        // encodeFor writes the detail only to a session >= 11 (encode() never does)
        const std::size_t n = resp.encodeFor(out, ver, outCap);
        return n != 0 ? n : resp.encode(out);
    };

    // The same cap the Subscribe filter path enforces (CompiledFilter::compile): no group is fixed to bytes every
    // Subscribe would refuse.  #214 review: checked before the group is created - a refused join never holds a
    // group slot (not even until the next duty cycle).
    if (filter.size() > rune::codec::kMaxFilterBytes) {
        return refuseJoin("group filter too large: " + std::to_string(filter.size()) + " bytes, at most " +
                          std::to_string(rune::codec::kMaxFilterBytes));
    }

    // Collect subscribedTopicIds → topic names.
    // joinTopicsScratch_ is a reused member; clear + refill to avoid per-call
    // vector allocation.  Short topic names benefit from SSO; the vector itself
    // is amortized (only grows on first large-subscribe, cold path).
    joinTopicsScratch_.clear();
    for (const int32_t tid : msg.subscribedTopicIds) {
        // #19: a view sink is in the registry but holds no partitions to consume: refuse, never an empty assignment
        // (#214 review: before the group is created).
        if (topics_ != nullptr && topics_->isViewSink(tid)) {
            return refuseJoin("topic id " + std::to_string(tid) +
                              " is a view sink: it holds no records to consume; read the view with the view read API");
        }
        std::string name = topicNameForId(topics_, rune::coordination::parseScopedId(groupIdStr).nsId, tid);
        if (!name.empty()) {
            joinTopicsScratch_.push_back(std::move(name));
        }
        // Unknown topicId silently ignored (Assignor ignores topics not in partCounts).
    }

    // Namespaces Phase 2 Task 9 (D23): maxGroups of the group's namespace, at the join that would CREATE a group (count of
    // distinct group ids of that nsId in coordinators_; count >= limit refuses; 0 = unlimited).  The limit is read from the
    // gateway's namespace mirror the request carries (ctx.reg).  Cold: only a new group's first join scans.
    if (ctx.reg != nullptr && coordinators_.find(groupIdStr) == coordinators_.end()) {
        const uint32_t gns = rune::coordination::parseScopedId(groupIdStr).nsId;
        if (const auto* nsr = ctx.reg->byId(gns); nsr != nullptr && nsr->limits.maxGroups != 0) {
            uint64_t have = 0;
            for (const auto& [id, c] : coordinators_) if (rune::coordination::parseScopedId(id).nsId == gns) ++have;
            if (have >= nsr->limits.maxGroups)
                return refuseJoin("namespace limit exceeded: groups (" + std::to_string(have) + " of " +
                                  std::to_string(nsr->limits.maxGroups) + ")", true);
        }
    }
    // #214: the number of coordinated groups is bounded.  Only a join that would CREATE a group is refused (never one
    // to an existing group); empty groups are dropped by doWork, so the room comes back as groups empty.
    if (coordinators_.size() >= static_cast<std::size_t>(maxGroups_) &&
        coordinators_.find(groupIdStr) == coordinators_.end()) [[unlikely]] {
        const uint64_t n = ++joinsRefusedGroupCap_;
        if ((n & (n - 1)) == 0)
            spdlog::warn("GroupCoordinationService: JoinGroup for new group '{}' refused - this broker coordinates its "
                         "maximum of {} consumer groups (RUNE_GROUP_MAX_GROUPS) (refusal #{})", groupIdStr, maxGroups_, n);
        return refuseJoin("this broker coordinates its maximum of " + std::to_string(maxGroups_) +
                          " consumer groups; retry after a group empties or raise RUNE_GROUP_MAX_GROUPS");
    }
    coord::GroupCoordinator& gc = coordinator(groupIdStr);   // only creates the group
    if (!gc.admitFilter(filter)) {
        noteFilterMismatch_(groupIdStr, msg.sessionId);
        return refuseJoin(groupFilterMismatchDetail(gc.filterBytes()));
    }

    // Issues #123 / #124: a non-empty member id is a RE-JOIN only when it is bound to this session's principal (the
    // same session, or a reconnect of the same principal).  Any other id - another principal's member, a member that
    // no longer exists, an id the client made up - joins as a NEW member with a fresh id: the answer looks the same
    // whether or not the id names someone (nothing to enumerate), and an id is never adopted from the client.
    // PR #423 review F2: and only for a CURRENT member of THIS group (a binding never stands in for membership).
    const uint64_t principal = principalOf_(msg.sessionId);
    if (!memberIdScratch_.empty()) {
        const MemberBinding* b = bindingOf_(groupIdStr, memberIdStr);
        if (b == nullptr || b->principal != principal || !gc.hasMember(memberIdStr)) memberIdScratch_.clear();
    }

    // The member's session timeout (or a sane default), CLAMPED into [min, max] (Kafka: group.min / group.max
    // .session.timeout.ms).  #213: an unbounded one would let one member stall every rebalance and outlive its death
    // for weeks.  #499: a tiny one is raised to the minimum, and the group's rebalance timeout is the LARGEST current
    // member's (refreshRebalanceTimeout_ after the join), never the last joiner's - one 1 ms joiner no longer
    // collapses the window and evicts every other member.  The 30 s default for an absent (<= 0) value is clamped too.
    // A minimum above the maximum: the maximum is the bound (NodeConfig refuses that configuration).
    const int32_t sessionLo = std::min(minSessionTimeoutMs_, maxSessionTimeoutMs_);
    const int32_t sessionMs = std::clamp(msg.sessionTimeoutMs > 0 ? msg.sessionTimeoutMs : 30'000, sessionLo,
                                         maxSessionTimeoutMs_);
    if (msg.sessionTimeoutMs > 0 && msg.sessionTimeoutMs != sessionMs) [[unlikely]]
        spdlog::debug("GroupCoordinationService: JoinGroup sessionTimeoutMs {} clamped to {} (group bounds [{}, {}])",
                      msg.sessionTimeoutMs, sessionMs, sessionLo, maxSessionTimeoutMs_);

    // CG-4: capture state BEFORE join so we can detect a Stable→PreparingRebalance
    // transition triggered directly by gc.join() (new member arriving while Stable).
    // The doWork() transition detector only fires when the transition happens inside
    // coord.doWork(); for join-triggered rebalances the state is already
    // PreparingRebalance by the time doWork() next runs — so we must fire here.
    const coord::GroupState stateBefore = gc.state();

    // #549: remember the ids this join named that are not assignable now; doWork rebalances once one becomes so.
    bool pendingCapHit = false;
    for (const int32_t tid : msg.subscribedTopicIds) {
        if (topicIdAssignable_(tid)) continue;
        auto& pending = unassignedJoinTopics_[groupIdStr];
        if (std::find(pending.begin(), pending.end(), tid) != pending.end()) continue;
        if (pending.size() >= kMaxUnassignedJoinTopics) { pendingCapHit = true; continue; }
        pending.push_back(tid);
    }
    if (pendingCapHit)   // once per join, never per id
        spdlog::warn("GroupCoordinationService: group '{}' names more than {} not-yet-assignable topics; the rest "
                     "are not tracked (#549)", groupIdStr, kMaxUnassignedJoinTopics);

    coord::JoinResult jr = gc.join(memberIdStr,
                                   sessionMs,
                                   joinTopicsScratch_,
                                   nowNs);
    refreshRebalanceTimeout_(gc);   // #499: the largest current member's session timeout

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
        buildMemberKey_(groupIdStr, jr.memberId);
        memberSessionMap_[memberKeyScratch_] = MemberBinding{msg.sessionId, principal};
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
        const coord::TopicPartitionCounts partCounts = buildTopicPartitionCounts(rune::coordination::parseScopedId(groupIdStr).nsId);
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
    const uint8_t* buf, size_t len, int64_t nowNs, uint8_t* out, size_t outCap,
    const rune::coordination::NsRequestContext& ctx)
{
    auto msgOpt = pc::SyncGroup::decode(buf, len);
    if (!msgOpt) {
        // No-hang (R2): answered SyncGroupResponse{InvalidRequest}, never dropped.
        spdlog::debug("GroupCoordinationService: SyncGroup decode failed — answered InvalidRequest");
        return rune::gateway::encodeRejectionFor(
            buf, len, static_cast<uint16_t>(pc::ErrorCode::InvalidRequest), out, outCap);
    }
    const pc::SyncGroup& msg = *msgOpt;
    if (rune::coordination::reservedClientId(idView_(msg.groupId))) [[unlikely]]   // D19: the scoped-id prefix is reserved
        return rune::gateway::encodeRejectionFor(
            buf, len, static_cast<uint16_t>(pc::ErrorCode::InvalidRequest), out, outCap);

    // CG-7 zero-alloc: assign() into pre-reserved scratch strings.
    rune::gateway::scopeClientIdInto(ctx, idView_(msg.groupId), groupIdScratch_);   // D19: namespace-scoped id
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
    (void)syncTerm_();   // #570

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

    // Issues #124: another session's member is answered as a member that does not exist (an empty id is never a
    // member): its assignment is never handed out.
    if (!ownsMember_(groupIdStr, memberIdStr, msg.sessionId)) memberIdScratch_.clear();

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
            const auto opt = topics_->topicIdForName(rune::coordination::parseScopedId(groupIdStr).nsId, tp.topic);
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
    const uint8_t* buf, size_t len, int64_t nowNs, uint8_t* out, size_t outCap,
    const rune::coordination::NsRequestContext& ctx)
{
    auto msgOpt = pc::Heartbeat::decode(buf, len);
    if (!msgOpt) {
        // No-hang (R2): answered HeartbeatResponse{InvalidRequest}, never dropped.
        spdlog::debug("GroupCoordinationService: Heartbeat decode failed — answered InvalidRequest");
        return rune::gateway::encodeRejectionFor(
            buf, len, static_cast<uint16_t>(pc::ErrorCode::InvalidRequest), out, outCap);
    }
    const pc::Heartbeat& msg = *msgOpt;
    if (rune::coordination::reservedClientId(idView_(msg.groupId))) [[unlikely]]   // D19: the scoped-id prefix is reserved
        return rune::gateway::encodeRejectionFor(
            buf, len, static_cast<uint16_t>(pc::ErrorCode::InvalidRequest), out, outCap);

    // CG-7 zero-alloc: assign() into pre-reserved scratch strings; no heap alloc
    // once capacity >= size.  Heartbeat is the hottest handler path.
    rune::gateway::scopeClientIdInto(ctx, idView_(msg.groupId), groupIdScratch_);   // D19: namespace-scoped id
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
    (void)syncTerm_();   // #570

    auto it = coordinators_.find(groupIdStr);
    if (it == coordinators_.end()) {
        pc::HeartbeatResponse resp;
        resp.sessionId     = msg.sessionId;
        resp.correlationId = msg.correlationId;
        resp.errorCode     = static_cast<int16_t>(pc::ErrorCode::UnknownMember);
        return resp.encode(out);
    }

    // Issue #124: another session's member is answered as a member that does not exist and is not refreshed (a
    // foreign heartbeat never keeps a dead member alive).
    if (!ownsMember_(groupIdStr, memberIdStr, msg.sessionId)) memberIdScratch_.clear();
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
    const uint8_t* buf, size_t len, int64_t nowNs, uint8_t* out, size_t outCap,
    const rune::coordination::NsRequestContext& ctx)
{
    auto msgOpt = pc::LeaveGroup::decode(buf, len);
    if (!msgOpt) {
        // No-hang (R2): answered LeaveGroupResponse{InvalidRequest}, never dropped.
        spdlog::debug("GroupCoordinationService: LeaveGroup decode failed — answered InvalidRequest");
        return rune::gateway::encodeRejectionFor(
            buf, len, static_cast<uint16_t>(pc::ErrorCode::InvalidRequest), out, outCap);
    }
    const pc::LeaveGroup& msg = *msgOpt;
    if (rune::coordination::reservedClientId(idView_(msg.groupId))) [[unlikely]]   // D19: the scoped-id prefix is reserved
        return rune::gateway::encodeRejectionFor(
            buf, len, static_cast<uint16_t>(pc::ErrorCode::InvalidRequest), out, outCap);

    // CG-7 zero-alloc: assign() into pre-reserved scratch strings.
    rune::gateway::scopeClientIdInto(ctx, idView_(msg.groupId), groupIdScratch_);   // D19: namespace-scoped id
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
    (void)syncTerm_();   // #570

    pc::LeaveGroupResponse resp;
    resp.sessionId     = msg.sessionId;
    resp.correlationId = msg.correlationId;

    // Issue #123: a member leaves only on the session it is bound to.  Another session's member - and a member that
    // does not exist, with the same answer - is UnknownMember: nothing is revoked, nothing rebalances.
    auto it = coordinators_.find(groupIdStr);
    if (it == coordinators_.end() || !ownsMember_(groupIdStr, memberIdStr, msg.sessionId) ||
        !it->second.hasMember(memberIdStr)) {
        resp.errorCode = static_cast<int16_t>(pc::ErrorCode::UnknownMember);
        return resp.encode(out);
    }

    // CG-4 fix (#3): revoke the leaving member's group push subscriptions BEFORE
    // erasing the session mapping.  This prevents dual-owner: the next
    // PreparingRebalance iterates coord.members() which no longer contains the
    // departed member — so without this explicit revoke the member keeps receiving
    // records for its old partitions until session reap, which can overlap with
    // the reassigned owner (dup delivery, single-owner violation).
    if (gateway_) {
        gateway_->revokeGroupSubscriptions(msg.sessionId);
        spdlog::debug("GroupCoordinationService: LeaveGroup revoking subs for "
                      "member='{}' sessionId={}", memberIdStr, msg.sessionId);
    }

    // Leave is generation-fenced inside GroupCoordinator::leave.
    // We need the member's last-known generationId; since the protocol
    // frame doesn't carry it for LeaveGroup (templateId 38), we pass
    // the current coordinator generationId (faithful Kafka: leave is
    // fenced; unknown/stale leaves are no-ops).
    it->second.leave(memberIdStr, it->second.generationId(), nowNs);
    drainDeparted_(groupIdStr, it->second);   // CG-4: drop member → session mapping on Leave

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
        buildMemberKey_(groupId, member.memberId);
        const auto it = memberSessionMap_.find(memberKeyScratch_);
        if (it != memberSessionMap_.end() && it->second.sessionId == sessionId) evictScratch_.push_back(member.memberId);
    }
    for (const auto& m : evictScratch_) {
        spdlog::warn("GroupCoordinationService: old client (session {}) member '{}' of group '{}' evicted — "
                     "answered UnknownMember instead of an error it would ignore", sessionId, m, groupId);
        coord.leave(m, coord.generationId(), nowNs);
        buildMemberKey_(groupId, m);
        memberSessionMap_.erase(memberKeyScratch_);
    }
}

int GroupCoordinationService::removeUnauthorizedMembers(int64_t sessionId, int64_t nowNs, GroupReadCheck check)
{
    if (check.mayRead == nullptr) return 0;
    int removed = 0;
    for (auto& [groupId, coord] : coordinators_) {
        // Issue #212: a group whose READ holds is not skipped wholesale - a member of it subscribed to a topic the
        // session may no longer read leaves too.
        const bool groupOk = check.mayRead(check.ctx, rune::coordination::parseScopedId(groupId).local);   // D19: authorize the LOCAL name
        if (groupOk && check.mayReadTopic == nullptr) continue;
        evictScratch_.clear();
        for (const auto& member : coord.members()) {
            buildMemberKey_(groupId, member.memberId);
            const auto it = memberSessionMap_.find(memberKeyScratch_);
            if (it == memberSessionMap_.end() || it->second.sessionId != sessionId) continue;
            bool topicLost = false;
            if (check.mayReadTopic != nullptr)
                for (const std::string& topic : member.subscribedTopics)
                    if (!check.mayReadTopic(check.ctx, topic)) { topicLost = true; break; }
            if (!groupOk || topicLost) evictScratch_.push_back(member.memberId);
        }
        if (evictScratch_.empty()) continue;
        if (gateway_) gateway_->revokeGroupSubscriptions(sessionId);   // as LeaveGroup: before the mapping goes
        for (const auto& memberId : evictScratch_) {
            if (!groupOk)
                spdlog::warn("GroupCoordinationService: session {} lost READ on group '{}' - member '{}' removed (its "
                             "next request is answered NotAuthorized)", sessionId, groupId, memberId);
            else
                spdlog::warn("GroupCoordinationService: session {} lost READ on a topic member '{}' of group '{}' is "
                             "subscribed to - member removed (the group rebalances; a re-join is refused)",
                             sessionId, memberId, groupId);
            coord.leave(memberId, coord.generationId(), nowNs);
            buildMemberKey_(groupId, memberId);
            memberSessionMap_.erase(memberKeyScratch_);
            ++removed;
        }
    }
    return removed;
}

void GroupCoordinationService::evictSessionMembers(int64_t sessionId, int64_t nowNs)
{
    evictSessionMembers_(sessionId, nowNs, "old client - answered UnknownMember instead of an error it would ignore");
}

void GroupCoordinationService::removeSessionMembers(int64_t sessionId, int64_t nowNs)
{
    evictSessionMembers_(sessionId, nowNs, "session revoked - its members leave their groups");
}

void GroupCoordinationService::evictSessionMembers_(int64_t sessionId, int64_t nowNs, const char* why)
{
    for (auto& [groupId, coord] : coordinators_) {
        evictScratch_.clear();
        for (const auto& member : coord.members()) {
            buildMemberKey_(groupId, member.memberId);
            const auto it = memberSessionMap_.find(memberKeyScratch_);
            if (it != memberSessionMap_.end() && it->second.sessionId == sessionId) evictScratch_.push_back(member.memberId);
        }
        if (evictScratch_.empty()) continue;
        if (gateway_) gateway_->revokeGroupSubscriptions(sessionId);
        for (const auto& memberId : evictScratch_) {
            spdlog::warn("GroupCoordinationService: session {} member '{}' of group '{}' evicted: {}", sessionId,
                         memberId, groupId, why);
            coord.leave(memberId, coord.generationId(), nowNs);
            buildMemberKey_(groupId, memberId);
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
// applyTransactionalOffset - an offset of a committed transaction (see the header).
// ---------------------------------------------------------------------------
coord::TxnOffsetApply GroupCoordinationService::applyTransactionalOffset(const std::string& group,
                                                                         const coord::TopicPartition& tp,
                                                                         int64_t offset, int64_t nowNs,
                                                                         coord::TxnOffsetToken token)
{
    using R = coord::TxnOffsetApply;
    using Why = coord::TxnOffsetRefusal;
    if (offsets_ == nullptr || !isLocalCoordinator()) return refuse_(Why::NotCoordinator);
    if (!offsetsLoaded_()) return refuse_(Why::NotLoaded);
    uint64_t lineage = 0;
    if (topics_ != nullptr) {
        if (const auto id = topics_->topicIdForName(rune::coordination::parseScopedId(group).nsId, tp.topic)) {
            lineage = lineageOf_(*id, tp.partition);   // C1: 0 for a disk partition
            if (lineage == kLineagePending) return refuse_(Why::LineagePending);
        }
    }
    const rune::replication::PartitionReplica* r = offsets_->replica();
    if (r == nullptr) return offsets_->commit(group, tp, offset, {}, nowNs, lineage) ? R::Applied : refuse_(Why::AppendRefused);
    // #358: Applied only once a record carrying this offset or a NEWER one of the key is COMMITTED (see the header).
    const uint64_t leadGen = r->leadershipGeneration(), logGen = r->logGeneration();
    const int64_t  hw = r->highWatermark();
    const TxnOffsetKey key{group, tp.topic, tp.partition};
    auto slot = txnOffsetsInFlight_.find(key);
    if (slot != txnOffsetsInFlight_.end()) {
        TxnOffsetKeyState& k = slot->second;
        // A newest record seen committed survives every later change of leadership or log.
        if (k.lastRecord >= 0 && k.lastLeadershipGen == leadGen && k.lastLogGen == logGen && hw > k.lastRecord)
            k.lastCommitted = true;
        for (std::size_t i = 0; i < k.waiters.size(); ++i) {
            TxnOffsetWaiter& w = k.waiters[i];
            if (!(w.token == token) || w.offset != offset) continue;
            // An anonymous caller (a default token: rigs) cannot tell two transactions apart; it is matched only
            // as the key's newest waiter (an older anonymous entry is dropped when a newer record is appended).
            if (token == coord::TxnOffsetToken{} && i + 1 != k.waiters.size()) continue;
            const auto leave = [&] {
                k.waiters.erase(k.waiters.begin() + static_cast<std::ptrdiff_t>(i));
                --txnOffsetsWaiting_;
                if (k.waiters.empty()) txnOffsetsInFlight_.erase(slot);
                return R::Applied;
            };
            if (k.lastCommitted) return leave();   // its record or a newer one of the key is committed
            const bool sameGen = k.lastLeadershipGen == leadGen && k.lastLogGen == logGen;
            if (sameGen) return R::Replicating;    // the newest record of the key is still replicating
            // Leadership or log changed and the newest record may be gone.  Only the newest waiter appends again;
            // an older one waits for it (a newer value is never overwritten by an older one).  A plain commit that
            // was the newest and may be gone hands that role back to the newest waiter.
            if (i + 1 != k.waiters.size()) return R::Replicating;
            const int64_t at = offsets_->commitAt(group, tp, offset, {}, nowNs, lineage);
            if (at < 0) return refuse_(Why::AppendRefused);
            w.recordOffset = at; w.leadershipGen = leadGen; w.logGen = logGen;
            k.lastRecord = at; k.lastLeadershipGen = leadGen; k.lastLogGen = logGen;
            k.lastCommitted = r->highWatermark() > at; k.plainIsLast = false;
            return k.lastCommitted ? leave() : R::Replicating;
        }
        if (k.waiters.size() >= kMaxTxnOffsetWaiters) return refuse_(Why::TooManyWaiters);   // never evict: retried
    }
    const int64_t at = offsets_->commitAt(group, tp, offset, {}, nowNs, lineage);
    if (at < 0) return refuse_(Why::AppendRefused);
    if (slot == txnOffsetsInFlight_.end()) slot = txnOffsetsInFlight_.try_emplace(key).first;
    TxnOffsetKeyState& k = slot->second;
    {   // older anonymous entries can never be asked for again (see above)
        const std::size_t before = k.waiters.size();
        std::erase_if(k.waiters, [](const TxnOffsetWaiter& w) { return w.token == coord::TxnOffsetToken{}; });
        txnOffsetsWaiting_ -= before - k.waiters.size();
    }
    k.lastRecord = at; k.lastLeadershipGen = leadGen; k.lastLogGen = logGen;
    k.lastCommitted = r->highWatermark() > at; k.plainIsLast = false;
    if (k.lastCommitted) {   // committed on the append (a single-replica offsets partition): nothing waits
        if (k.waiters.empty()) txnOffsetsInFlight_.erase(slot);
        return R::Applied;
    }
    k.waiters.push_back(TxnOffsetWaiter{token, offset, at, leadGen, logGen});
    ++txnOffsetsWaiting_;
    return R::Replicating;
}

void GroupCoordinationService::releaseTransactionalOffsets(const coord::TxnOffsetToken& token)
{
    if (txnOffsetsWaiting_ == 0) return;
    for (auto it = txnOffsetsInFlight_.begin(); it != txnOffsetsInFlight_.end();) {
        auto& ws = it->second.waiters;
        const std::size_t before = ws.size();
        std::erase_if(ws, [&](const TxnOffsetWaiter& w) { return w.token == token; });
        txnOffsetsWaiting_ -= before - ws.size();
        it = ws.empty() ? txnOffsetsInFlight_.erase(it) : std::next(it);
    }
}

void GroupCoordinationService::notePlainOffsetCommit_(const std::string& group, const coord::TopicPartition& tp,
                                                      int64_t at)
{
    if (txnOffsetsWaiting_ == 0 || offsets_ == nullptr || offsets_->replica() == nullptr) return;
    const auto it = txnOffsetsInFlight_.find(TxnOffsetKey{group, tp.topic, tp.partition});
    if (it == txnOffsetsInFlight_.end()) return;
    const rune::replication::PartitionReplica* r = offsets_->replica();
    TxnOffsetKeyState& k = it->second;
    k.lastRecord = at; k.lastLeadershipGen = r->leadershipGeneration(); k.lastLogGen = r->logGeneration();
    k.lastCommitted = r->highWatermark() > at; k.plainIsLast = true;
}

// ---------------------------------------------------------------------------
// handleOffsetCommit (templateId 40)
// ---------------------------------------------------------------------------
size_t GroupCoordinationService::handleOffsetCommit(
    const uint8_t* buf, size_t len, int64_t nowNs, uint8_t* out, size_t outCap,
    const rune::coordination::NsRequestContext& ctx)
{
    auto msgOpt = pc::OffsetCommit::decode(buf, len);
    if (!msgOpt) {
        // No-hang (R2): answered OffsetCommitResponse{InvalidRequest}, never dropped.
        spdlog::debug("GroupCoordinationService: OffsetCommit decode failed — answered InvalidRequest");
        return rune::gateway::encodeRejectionFor(
            buf, len, static_cast<uint16_t>(pc::ErrorCode::InvalidRequest), out, outCap);
    }
    const pc::OffsetCommit& msg = *msgOpt;
    if (rune::coordination::reservedClientId(idView_(msg.groupId))) [[unlikely]]   // D19: the scoped-id prefix is reserved
        return rune::gateway::encodeRejectionFor(
            buf, len, static_cast<uint16_t>(pc::ErrorCode::InvalidRequest), out, outCap);

    // CG-7 zero-alloc: assign() into pre-reserved scratch; no heap alloc once stable.
    rune::gateway::scopeClientIdInto(ctx, idView_(msg.groupId), groupIdScratch_);   // D19: namespace-scoped id
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
    (void)syncTerm_();   // #570

    // Member + generation fence (issues #124 / #169, Kafka parity): the commit is honoured only for a CURRENT member
    // of the group, bound to THIS session, at the group's generation.  Generations restart with the coordinator, so
    // the generation alone let a pre-restart member's late commit (or another session's) move the group's position.
    // Another session's member is answered exactly as a member that does not exist.
    auto it = coordinators_.find(groupIdStr);
    const bool groupExists = (it != coordinators_.end());
    memberIdScratch_.assign(reinterpret_cast<const char*>(msg.memberId.data()), msg.memberId.size());
    const bool memberOk = groupExists && ownsMember_(groupIdStr, memberIdScratch_, msg.sessionId) &&
                          it->second.hasMember(memberIdScratch_);
    bool genOk = memberOk && (msg.generationId == it->second.generationId());   // R1: cleared by a forced rebalance

    commitResultScratch_.clear();
    const bool loaded = offsetsLoaded_();   // E2 residual round 4

    // #433: a commit whose records may need to wait for the high watermark is parked; it is accepted only when the
    // park has room for its whole answer - otherwise it is refused NotCoordinator (retriable) with NOTHING appended.
    const bool mayPark = offsets_ != nullptr && offsets_->replica() != nullptr;
    const std::size_t maxResults = mayPark ? kMaxParkedResultsPerCommit : SIZE_MAX;
    const bool parkFull = mayPark && genOk && loaded &&
                          !parkHasRoomFor_(std::min<std::size_t>(msg.entries.size(), maxResults));
    if (parkFull) [[unlikely]] {
        if ((parkRefusals_++ & 1023u) == 0)
            spdlog::warn("GroupCoordinationService: OffsetCommit refused NotCoordinator (retriable) - {} answers are "
                         "already waiting for their records to commit ({} refusals so far)", parkedCount_,
                         parkRefusals_);
    }
    int64_t maxRecordAt = -1;   // #433: the newest record this request appended

    // Iterate entries defensively (CG-1 note: count can overstate; stop when
    // the view iterator ends, which happens when the frame bytes run out).
    // Also bound-check BEFORE each push so the final encode never overruns outCap.
    for (const auto& e : msg.entries) {
        // Stop if adding this result would overrun the output buffer.
        const std::size_t needed =
            pc::OffsetCommitResponse::encodedSizeFor(commitResultScratch_.size() + 1);
        if (needed > outCap || commitResultScratch_.size() >= maxResults) {
            spdlog::warn("GroupCoordinationService: OffsetCommit response capped at {} entries "
                         "(outCap={} would be exceeded)", commitResultScratch_.size(), outCap);
            break;
        }

        pc::OffsetCommitResult r;
        r.topicId   = e.topicId;
        r.partition = e.partition;

        if (parkFull) [[unlikely]] {
            r.errorCode = static_cast<int16_t>(pc::ErrorCode::NotCoordinator);
            commitResultScratch_.push_back(r);
            continue;
        }

        if (!genOk) {
            r.errorCode = static_cast<int16_t>(
                memberOk ? pc::ErrorCode::IllegalGeneration
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
        const std::string topicName = topicNameForId(topics_, rune::coordination::parseScopedId(groupIdStr).nsId, e.topicId);
        if (topicName.empty()) {
            // Unknown topic — no commit; report UnknownTopic per entry.
            r.errorCode = static_cast<int16_t>(pc::ErrorCode::UnknownTopic);
            commitResultScratch_.push_back(r);
            continue;
        }

        const coord::TopicPartition tp{topicName, e.partition};
        const std::string_view meta(
            reinterpret_cast<const char*>(e.metadata.data()), e.metadata.size());

        // #438: an unfinished transaction holds this key (or the transaction coordinator is not loaded here:
        // unknown = held) - a plain record never races its pending transactional one.  Retriable; nothing appended.
        // (A deviation from Kafka, which lets a plain commit through: here it would be overwritten - or rewind
        // the key - when the transaction completes.)
        if (holdOf_(groupIdStr, tp) != coord::TxnOffsetHold::Free) [[unlikely]] {
            r.errorCode = unstableFor_(msg.sessionId);
            commitResultScratch_.push_back(r);
            continue;
        }

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
        const int64_t at = offsets_->commitAt(groupIdStr, tp, e.offset, meta, nowNs, lineage);
        const bool committed = at >= 0;
        if (committed) {
            notePlainOffsetCommit_(groupIdStr, tp, at);   // the newest record of the key (F2)
            if (at > maxRecordAt) maxRecordAt = at;
        }
        r.errorCode = committed ? static_cast<int16_t>(pc::ErrorCode::OK)
                                : static_cast<int16_t>(pc::ErrorCode::MinIsrViolation);
        commitResultScratch_.push_back(r);
    }

    // #433: OK only once every appended record is committed.  A commit behind parked ones is parked too (answers
    // leave in request order).  A single-replica offsets partition commits on the append: answered inline.
    if (mayPark && maxRecordAt >= 0 &&
        (parkedCount_ != 0 || offsets_->replica()->highWatermark() <= maxRecordAt)) {
        parkCommit_(msg.sessionId, msg.correlationId, maxRecordAt);
        return 0;   // sent by releaseParkedCommits_ (the gateway treats a 0 OffsetCommit answer as deferred)
    }

    pc::OffsetCommitResponse resp;
    resp.sessionId     = msg.sessionId;
    resp.correlationId = msg.correlationId;
    return resp.encode(out, std::span<const pc::OffsetCommitResult>(commitResultScratch_));
}

// ---------------------------------------------------------------------------
// #433: parked OffsetCommit answers (see the header).
// ---------------------------------------------------------------------------
void GroupCoordinationService::parkCommit_(int64_t sessionId, int64_t correlationId, int64_t maxRecordAt)
{
    // Room was checked before anything was appended (parkFull); the results are the response's, capped alike.
    const rune::replication::PartitionReplica* r = offsets_->replica();
    ParkedCommit& p = parked_[(parkedHead_ + parkedCount_) % kMaxParkedCommits];
    p.sessionId     = sessionId;
    p.correlationId = correlationId;
    p.maxRecordAt   = maxRecordAt;
    p.leadershipGen = r->leadershipGeneration();
    p.logGen        = r->logGeneration();
    p.resultsAt     = static_cast<uint32_t>((resultsHead_ + resultsUsed_) % kParkedResultsCapacity);
    p.count         = static_cast<uint32_t>(commitResultScratch_.size());
    for (std::size_t i = 0; i < commitResultScratch_.size(); ++i)
        parkedResults_[(p.resultsAt + i) % kParkedResultsCapacity] = commitResultScratch_[i];
    resultsUsed_ += p.count;
    ++parkedCount_;
}

int GroupCoordinationService::releaseParkedCommits_()
{
    // Sent reliably through sendControlFrame like an Assign frame: it always fits the gateway spill's staging buffer.
    static_assert(kParkedFrameBytes <= rune::gateway::ClientGateway::spillSizing().stage,
                  "parked OffsetCommit answers must be spillable");
    const rune::replication::PartitionReplica* r = offsets_ != nullptr ? offsets_->replica() : nullptr;
    const bool coordinating = isLocalCoordinator();
    int n = 0;
    while (parkedCount_ != 0) {
        ParkedCommit& p = parked_[parkedHead_];
        const bool sameGen = r != nullptr && r->leadershipGeneration() == p.leadershipGen &&
                             r->logGeneration() == p.logGen;
        const bool lost = !sameGen || !coordinating || !r->acceptsProduce();
        if (!lost && r->highWatermark() <= p.maxRecordAt) break;   // still replicating (later ones are newer)
        commitResultScratch_.clear();
        for (uint32_t i = 0; i < p.count; ++i) {
            pc::OffsetCommitResult res = parkedResults_[(p.resultsAt + i) % kParkedResultsCapacity];
            // The record may be gone with the old leadership / log: never an OK for it.  Retriable (idempotent).
            if (lost && res.errorCode == static_cast<int16_t>(pc::ErrorCode::OK))
                res.errorCode = static_cast<int16_t>(pc::ErrorCode::NotCoordinator);
            commitResultScratch_.push_back(res);
        }
        if (lost)
            spdlog::info("GroupCoordinationService: a parked OffsetCommit (session {}, corr {}) answered "
                         "NotCoordinator - the offsets partition changed before its record {} committed",
                         p.sessionId, p.correlationId, p.maxRecordAt);
        pc::OffsetCommitResponse resp;
        resp.sessionId     = p.sessionId;
        resp.correlationId = p.correlationId;
        const std::size_t len = resp.encode(parkedFrame_.data(),
                                            std::span<const pc::OffsetCommitResult>(commitResultScratch_));
        sendDeferred_(p.sessionId, parkedFrame_.data(), len);
        resultsHead_ = (resultsHead_ + p.count) % kParkedResultsCapacity;
        resultsUsed_ -= p.count;
        parkedHead_ = (parkedHead_ + 1) % kMaxParkedCommits;
        --parkedCount_;
        ++n;
    }
    return n;
}

void GroupCoordinationService::sendDeferred_(int64_t sessionId, const uint8_t* frame, std::size_t n)
{
    if (gateway_ != nullptr) (void)gateway_->sendControlFrame(sessionId, frame, n);   // queued, never dropped
    else if (answerSink_ != nullptr) answerSink_(answerSinkCtx_, sessionId, frame, n);
}

int16_t GroupCoordinationService::unstableFor_(int64_t sessionId)
{
    ++unstableAnswers_;
    const int32_t v = clientVersionFn_ != nullptr ? clientVersionFn_(clientVersionCtx_, sessionId)
                    : gateway_ != nullptr        ? gateway_->sessionClientVersion(sessionId)
                                                 : -1;
    return static_cast<int16_t>(pc::clientUnderstandsUnstableOffsetCommit(v) ? pc::ErrorCode::UnstableOffsetCommit
                                                                            : pc::ErrorCode::NotCoordinator);
}

// ---------------------------------------------------------------------------
// handleOffsetFetch (templateId 42)
// ---------------------------------------------------------------------------
size_t GroupCoordinationService::handleOffsetFetch(
    const uint8_t* buf, size_t len, int64_t nowNs, uint8_t* out, size_t outCap,
    const rune::coordination::NsRequestContext& ctx)
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
    if (rune::coordination::reservedClientId(idView_(msg.groupId))) [[unlikely]]   // D19: the scoped-id prefix is reserved
        return rune::gateway::encodeRejectionFor(
            buf, len, static_cast<uint16_t>(pc::ErrorCode::InvalidRequest), out, outCap);

    // CG-7 zero-alloc: assign() into pre-reserved scratch; no heap alloc once stable.
    rune::gateway::scopeClientIdInto(ctx, idView_(msg.groupId), groupIdScratch_);   // D19: namespace-scoped id
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

    // #471: a key whose __consumer_offsets record is not committed yet (above the replica's high watermark) must
    // not be served: the record may still be lost on a leader change.
    const rune::replication::PartitionReplica* const offsetsReplica = offsets_ != nullptr ? offsets_->replica() : nullptr;
    auto isUnstable = [&](const coord::CommittedOffset& co) noexcept {
        return co.recordAt >= 0 && offsetsReplica != nullptr && co.recordAt >= offsetsReplica->highWatermark();
    };
    auto answerRetriableSentinel = [&]() {
        fetchResultScratch_.clear();
        if (pc::OffsetFetchResponse::encodedSizeFor(1) <= outCap) {
            pc::OffsetFetchResult sentinel;
            sentinel.topicId   = -1;
            sentinel.partition = -1;
            sentinel.offset    = -1;
            sentinel.errorCode = unstableFor_(msg.sessionId);
            fetchResultScratch_.push_back(sentinel);
        }
        pc::OffsetFetchResponse resp;
        resp.sessionId     = msg.sessionId;
        resp.correlationId = msg.correlationId;
        return resp.encode(out, std::span<const pc::OffsetFetchResult>(fetchResultScratch_));
    };

    if (msg.entries.empty()) {
        // #438: a fetch-all cannot list a held key that has no committed offset yet: while ANY key of the group is
        // held (or the hold state is unknown) the whole fetch is answered the retriable sentinel.
        if (holdOf_(groupIdStr, coord::TopicPartition{std::string{}, -1}) != coord::TxnOffsetHold::Free) [[unlikely]] {
            return answerRetriableSentinel();
        }
        // Fetch all committed offsets for the group.
        // Bound-check BEFORE writing each result so a large fetch-all never
        // overruns a fixed scratch buffer.  Results that would exceed outCap
        // are silently dropped (the response is capped but never overruns).
        // #498: an unstable key (#471: its record is above the high watermark) is listed with the retriable code and
        // no offset - exactly what a specific fetch answers - and every stable key with its offset; steady commits on
        // one key never starve the listing of the others.  An unstable value is never served.
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
                const auto opt = topics_->topicIdForName(rune::coordination::parseScopedId(groupIdStr).nsId, tp.topic);
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
            } else if (isUnstable(co)) [[unlikely]] {   // #471 / #498: not committed yet - retriable, never its value
                r.offset    = -1;
                r.errorCode = unstableFor_(msg.sessionId);
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

            const std::string topicName = topicNameForId(topics_, rune::coordination::parseScopedId(groupIdStr).nsId, e.topicId);

            pc::OffsetFetchResult r;
            r.topicId   = e.topicId;
            r.partition = e.partition;
            r.errorCode = static_cast<int16_t>(pc::ErrorCode::OK);

            if (topicName.empty()) {
                r.offset = -1; // no-offset sentinel
            } else {
                const coord::TopicPartition tp{topicName, e.partition};
                if (holdOf_(groupIdStr, tp) != coord::TxnOffsetHold::Free) [[unlikely]] {
                    // #438: an unfinished transaction holds the key (or unknown = held): its committed offset is
                    // about to change - neither it nor "none" is answered; retriable.
                    r.offset    = -1;
                    r.errorCode = unstableFor_(msg.sessionId);
                } else if (lineageOf_(e.topicId, e.partition) == kLineagePending) [[unlikely]] {
                    // C1: the current leadership has not confirmed the lineage
                    // yet — neither the offset nor "none" can be answered.
                    ++pendingLineageAnswers_;
                    r.offset    = -1;
                    r.errorCode = static_cast<int16_t>(pc::ErrorCode::NotLeader);
                } else {
                    const auto co = offsets_->fetch(groupIdStr, tp);
                    if (co.has_value() && isUnstable(*co)) [[unlikely]] {
                        // #471: the record that set this value is not committed yet; retriable.
                        r.offset    = -1;
                        r.errorCode = unstableFor_(msg.sessionId);
                    } else {
                        r.offset = co.has_value() && !staleLineage_(*co, e.topicId, e.partition) ? co->offset
                                                                                                 : int64_t{-1};
                    }
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
        buildMemberKey_(groupId, member.memberId);
        const auto it = memberSessionMap_.find(memberKeyScratch_);
        if (it == memberSessionMap_.end()) continue;  // no session tracked yet (early join)

        const int64_t sessionId = it->second.sessionId;

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
        buildMemberKey_(groupId, member.memberId);
        const auto it = memberSessionMap_.find(memberKeyScratch_);
        if (it == memberSessionMap_.end()) {
            spdlog::warn("GroupCoordinationService::onReachedStable: "
                         "no sessionId for member '{}' in group '{}' — skipping",
                         member.memberId, groupId);
            continue;
        }
        const int64_t sessionId = it->second.sessionId;

        // Build assignment for the Assign frame only (no subscription creation).
        assignFrameTpScratch_.clear();

        for (const auto& tp : member.assignment) {
            // Resolve topic name → topicId via TopicRegistry.
            int32_t topicId = -1;
            if (topics_) {
                const auto opt = topics_->topicIdForName(rune::coordination::parseScopedId(groupId).nsId, tp.topic);
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

namespace rune::node {
// ---------------------------------------------------------------------------
// #125 (KIP-447): the member fence of a TxnOffsetCommit - handleOffsetCommit's member + generation rules.
// ---------------------------------------------------------------------------
rune::protocol::client::ErrorCode GroupCoordinationService::checkTxnOffsetMember(const std::string& group,
                                                                                const std::string& memberId,
                                                                                int32_t generationId, int64_t sessionId)
{
    using EC = rune::protocol::client::ErrorCode;
    if (!isLocalCoordinator()) return EC::NotCoordinator;
    (void)syncTerm_();   // #570
    const auto it = coordinators_.find(group);
    const bool memberOk = it != coordinators_.end() && ownsMember_(group, memberId, sessionId) &&
                          it->second.hasMember(memberId);
    if (!memberOk) return EC::UnknownMember;   // another session's member: exactly as one that does not exist
    return generationId == it->second.generationId() ? EC::OK : EC::IllegalGeneration;
}
}  // namespace rune::node
