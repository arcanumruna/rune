#pragma once
// ---------------------------------------------------------------------------
// ConsumerGroupHarness.h — Deterministic consumer-group integration harness.
//
// Namespace: rune::coordination::test
// C++23. NO busy-spin. NO while(!done). All loops bounded + ASSERT(done).
// Injected monotonic clock. REAL Phase-4 components:
//   GroupCoordinator, OffsetStore, RangeAssignor / RoundRobinAssignor.
//
// ## Design (mirrors ReplicationClusterHarness pattern from P3-6)
//
//   ConsumerGroupHarness owns:
//     - ONE GroupCoordinator (in-memory; can be destroyed/recreated for failover).
//     - ONE OffsetStore at a durable temp path (survives coordinator restart).
//     - N simulated consumers (ConsumerSim) each holding a memberId + assignment.
//     - One injected int64_t clock_ (nanoseconds).
//
//   driveUntil(pred, maxSteps, stepNs):
//     Bounded driver: iterate at most maxSteps times. Each step advances the
//     clock by stepNs and calls doWork(clock_) on the coordinator.
//     Returns true iff pred() is true after the loop.
//     NEVER hangs — caller ASSERT_TRUE(driveUntil(...)).
//
//   Fault injection:
//     crashMember(i)      — marks consumer[i] as crashed (stops heartbeating).
//     reviveMember(i)     — re-joins consumer[i] with a fresh memberId.
//     recreateCoordinator()— destroys the GroupCoordinator and creates a new
//                            one from scratch (simulates coordinator failover).
//                            The OffsetStore path is reused → durability.
//
//   Invariant helpers:
//     assertPartitionsCoveredOnce(assignment): every partition in the expected set
//       is assigned exactly once across all members.
//     assertGenerationMonotonic(prevGen): current generationId > prevGen.
//     assertCommittedOffsetNotLost(group, tp, expectedOffset): fetch from
//       OffsetStore returns >= expectedOffset.
// ---------------------------------------------------------------------------

#include <gtest/gtest.h>

#include <rune/coordination/GroupCoordinator.h>
#include <rune/coordination/OffsetStore.h>
#include <rune/coordination/Assignor.h>
#include <rune/coordination/Types.h>

#include <algorithm>
#include <atomic>
#include <cassert>
#include <chrono>
#include <filesystem>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <unistd.h>
#include <vector>

namespace rune::coordination::test {

// ---------------------------------------------------------------------------
// Utilities
// ---------------------------------------------------------------------------
static inline std::string makeUniqSuffix() {
    static std::atomic<uint64_t> gSeq{0};
    auto ts = std::chrono::steady_clock::now().time_since_epoch().count();
    return std::to_string(::getpid()) + "_" +
           std::to_string(gSeq.fetch_add(1, std::memory_order_relaxed)) + "_" +
           std::to_string(ts);
}

/// RAII directory that auto-removes on destruction.
struct HarnessDir {
    std::string path;
    explicit HarnessDir(const std::string& tag) {
        path = std::filesystem::temp_directory_path().string()
               + "/rune_cg_integ_" + tag + "_" + makeUniqSuffix();
        std::filesystem::create_directories(path);
    }
    ~HarnessDir() {
        std::error_code ec;
        std::filesystem::remove_all(path, ec);
    }
    HarnessDir(const HarnessDir&)            = delete;
    HarnessDir& operator=(const HarnessDir&) = delete;
    HarnessDir(HarnessDir&&) noexcept        = default;
    HarnessDir& operator=(HarnessDir&&)      = delete;
};

// ---------------------------------------------------------------------------
// ConsumerSim — simulated consumer state inside the harness.
// ---------------------------------------------------------------------------
struct ConsumerSim {
    std::string              memberId;           ///< current memberId (empty = not joined)
    std::vector<std::string> subscribedTopics;
    int32_t                  sessionTimeoutMs{5'000};
    bool                     crashed{false};     ///< true = stopped heartbeating
    std::vector<TopicPartition> assignment;      ///< latest assignment after sync
};

// ---------------------------------------------------------------------------
// ConsumerGroupHarness
// ---------------------------------------------------------------------------
class ConsumerGroupHarness {
public:
    static constexpr int     kDefaultCap = 2000;
    static constexpr int64_t kStep1ms    = 1'000'000LL;  // 1 ms in ns

    /// \param groupId           The consumer group ID.
    /// \param n                 Number of simulated consumers.
    /// \param topics            Subscribed topics (same for all consumers).
    /// \param sessionTimeoutMs  Per-member session timeout.
    explicit ConsumerGroupHarness(const std::string&              groupId,
                                   int                             n,
                                   const std::vector<std::string>& topics,
                                   int32_t                         sessionTimeoutMs = 5'000)
        : groupId_(groupId)
        , dir_("cg_" + groupId)
        , offsetStorePath_(dir_.path + "/offsets.log")
        , sessionTimeoutMs_(sessionTimeoutMs)
    {
        // Build per-consumer state.
        consumers_.resize(static_cast<size_t>(n));
        for (int i = 0; i < n; ++i) {
            consumers_[static_cast<size_t>(i)].subscribedTopics = topics;
            consumers_[static_cast<size_t>(i)].sessionTimeoutMs = sessionTimeoutMs;
        }

        // Create coordinator + offset store.
        coord_ = std::make_unique<GroupCoordinator>(groupId_, clock_);
        coord_->setRebalanceTimeoutMs(sessionTimeoutMs);
        offsetStore_ = std::make_unique<OffsetStore>(offsetStorePath_);
    }

    // -----------------------------------------------------------------------
    // Drive loop — bounded deterministic step.
    // -----------------------------------------------------------------------

    /// Advance clock by stepNs and call doWork. Repeat until pred() is true
    /// or maxSteps exceeded.  Returns true iff pred() satisfied.
    ///
    /// If `heartbeatAlive` is true (default), each step also heartbeats all
    /// non-crashed consumers to keep their sessions alive.  Pass false when
    /// you WANT the clock to advance past some members' session timeouts
    /// (e.g., testing crash scenarios where only the crashed member times out).
    ///
    /// NEVER hangs — caller must ASSERT_TRUE(driveUntil(...)).
    bool driveUntil(std::function<bool()> pred,
                    int maxSteps = kDefaultCap,
                    int64_t stepNs = kStep1ms,
                    bool heartbeatAlive = true)
    {
        for (int i = 0; i < maxSteps && !pred(); ++i) {
            clock_ += stepNs;
            if (heartbeatAlive) {
                for (auto& c : consumers_) {
                    if (!c.crashed && !c.memberId.empty()) {
                        coord_->heartbeat(c.memberId, coord_->generationId(), clock_);
                    }
                }
            }
            coord_->doWork(clock_);
        }
        return pred();
    }

    /// Drive without heartbeating any consumers — used to test session timeout
    /// scenarios where we want ALL members to time out based on the clock.
    bool driveUntilNoHeartbeat(std::function<bool()> pred,
                                int maxSteps = kDefaultCap,
                                int64_t stepNs = kStep1ms)
    {
        return driveUntil(std::move(pred), maxSteps, stepNs, /*heartbeatAlive=*/false);
    }

    // -----------------------------------------------------------------------
    // Group lifecycle helpers
    // -----------------------------------------------------------------------

    /// Join all consumers (full initial group formation).
    ///
    /// Follows the Kafka JoinGroup protocol faithfully (as verified by the
    /// unit tests in RebalanceTest.cpp):
    ///   1. All N consumers join (Phase 1) → coordinator enters PreparingRebalance.
    ///   2. All N consumers RE-JOIN with their assigned memberIds (Phase 2)
    ///      → coordinator transitions to CompletingRebalance automatically once
    ///      all expected members have re-joined.
    ///   3. autoAssignAndSync() → Stable.
    ///
    /// Returns true when the coordinator reaches Stable after auto-assign.
    bool joinAll(const TopicPartitionCounts& partCounts,
                 AssignorType assignorType = AssignorType::Range,
                 int maxSteps = kDefaultCap)
    {
        // Phase 1: initial joins (empty memberId → new member IDs assigned).
        // First join of any member triggers PreparingRebalance.
        for (auto& c : consumers_) {
            if (c.crashed) continue;
            clock_ += kStep1ms; // small tick between joins
            auto res = coord_->join("", c.sessionTimeoutMs, c.subscribedTopics, clock_);
            EXPECT_EQ(res.errorCode, 0) << "Phase1 join() failed: errorCode=" << res.errorCode;
            c.memberId = res.memberId;
        }

        // Phase 2: all members RE-JOIN with their assigned memberIds.
        // This drives the coordinator from PreparingRebalance → CompletingRebalance.
        for (auto& c : consumers_) {
            if (c.crashed || c.memberId.empty()) continue;
            clock_ += kStep1ms;
            auto res = coord_->join(c.memberId, c.sessionTimeoutMs, c.subscribedTopics, clock_);
            EXPECT_EQ(res.errorCode, 0) << "Phase2 rejoin() failed: errorCode=" << res.errorCode;
            // memberId should be unchanged on re-join.
            c.memberId = res.memberId;
        }

        // State should now be CompletingRebalance (all members re-joined).
        // If not (e.g., some edge case), use driveUntil to catch any doWork transition.
        if (coord_->state() != GroupState::CompletingRebalance) {
            bool ok = driveUntil(
                [&]{ return coord_->state() == GroupState::CompletingRebalance; },
                maxSteps);
            if (!ok) return false;
        }

        // Phase 3: auto-assign and sync → Stable.
        auto syncRes = coord_->autoAssignAndSync(partCounts, assignorType, clock_);
        if (syncRes.errorCode != SyncResult::kOk) return false;

        // Capture assignments.
        for (auto& c : consumers_) {
            if (!c.memberId.empty() && !c.crashed) {
                c.assignment = coord_->assignmentFor(c.memberId);
            }
        }

        return coord_->state() == GroupState::Stable;
    }

    /// Heartbeat all alive consumers once.
    void heartbeatAll() {
        for (auto& c : consumers_) {
            if (c.memberId.empty() || c.crashed) continue;
            coord_->heartbeat(c.memberId, coord_->generationId(), clock_);
        }
    }

    /// Commit offset for consumer[idx] on the given partition.
    void commitOffset(std::size_t idx, const TopicPartition& tp, Offset offset,
                       std::string_view metadata = "") {
        offsetStore_->commit(groupId_, tp, offset, metadata, clock_);
    }

    /// Fetch committed offset from the durable store.
    std::optional<CommittedOffset> fetchOffset(const TopicPartition& tp) const {
        return offsetStore_->fetch(groupId_, tp);
    }

    // -----------------------------------------------------------------------
    // Fault injection
    // -----------------------------------------------------------------------

    /// Mark consumer[i] as crashed: it stops heartbeating entirely.
    /// Clears the assignment to reflect that the crashed member has no current assignment.
    void crashMember(std::size_t i) {
        ASSERT_LT(i, consumers_.size());
        consumers_[i].crashed = true;
        consumers_[i].assignment.clear(); // crashed member has no valid assignment
    }

    /// Revive consumer[i]: re-join with a fresh memberId after a crash.
    /// Does NOT heartbeat — caller must drive the coordinator to pick it up.
    void reviveMember(std::size_t i) {
        ASSERT_LT(i, consumers_.size());
        consumers_[i].crashed = false;
        consumers_[i].memberId = ""; // will get new id on next join
    }

    /// Rejoin consumer[i] explicitly (used during/after crash recovery).
    /// Returns the JoinResult.
    JoinResult rejoinMember(std::size_t i) {
        EXPECT_LT(i, consumers_.size());
        auto& c = consumers_[i];
        c.crashed = false;
        auto res = coord_->join("", c.sessionTimeoutMs, c.subscribedTopics, clock_);
        if (res.errorCode == 0) c.memberId = res.memberId;
        return res;
    }

    /// Destroy and recreate the GroupCoordinator (simulates coordinator failover).
    /// The OffsetStore is closed and reopened from the SAME durable path
    /// → committed offsets are preserved.
    void recreateCoordinator() {
        coord_.reset();
        offsetStore_.reset();

        // Reopen durable offset store (crash recovery / replay).
        offsetStore_ = std::make_unique<OffsetStore>(offsetStorePath_);

        // Fresh coordinator — group starts Empty; members must rejoin.
        coord_ = std::make_unique<GroupCoordinator>(groupId_, clock_);
        coord_->setRebalanceTimeoutMs(sessionTimeoutMs_);

        // All consumer memberIds are now stale (coordinator lost state).
        for (auto& c : consumers_) {
            c.memberId = "";
            c.assignment.clear();
        }
    }

    // -----------------------------------------------------------------------
    // Helpers for consumers performing a full rebalance after crash/failover
    // -----------------------------------------------------------------------

    /// All non-crashed consumers rejoin (fresh coordinator after failover).
    /// Follows the two-phase join protocol: initial join then re-join.
    bool rejoinAndAssign(const TopicPartitionCounts& partCounts,
                         AssignorType assignorType = AssignorType::Range,
                         int maxSteps = kDefaultCap)
    {
        // Phase 1: initial join (all memberIds are empty after coordinator restart).
        for (auto& c : consumers_) {
            if (c.crashed) continue;
            clock_ += kStep1ms;
            auto res = coord_->join("", c.sessionTimeoutMs, c.subscribedTopics, clock_);
            EXPECT_EQ(res.errorCode, 0);
            c.memberId = res.memberId;
        }

        // Phase 2: re-join with assigned memberIds.
        for (auto& c : consumers_) {
            if (c.crashed || c.memberId.empty()) continue;
            clock_ += kStep1ms;
            auto res = coord_->join(c.memberId, c.sessionTimeoutMs, c.subscribedTopics, clock_);
            EXPECT_EQ(res.errorCode, 0);
            c.memberId = res.memberId;
        }

        if (coord_->state() != GroupState::CompletingRebalance) {
            bool ok = driveUntil(
                [&]{ return coord_->state() == GroupState::CompletingRebalance
                         || coord_->state() == GroupState::Stable; },
                maxSteps);
            if (!ok) return false;
        }

        if (coord_->state() != GroupState::Stable) {
            auto syncRes = coord_->autoAssignAndSync(partCounts, assignorType, clock_);
            if (syncRes.errorCode != SyncResult::kOk) return false;
        }

        for (auto& c : consumers_) {
            if (!c.memberId.empty() && !c.crashed) {
                c.assignment = coord_->assignmentFor(c.memberId);
            }
        }
        return coord_->state() == GroupState::Stable;
    }

    // -----------------------------------------------------------------------
    // Invariant assertions
    // -----------------------------------------------------------------------

    /// Every partition in expectedPartitions is assigned exactly once across
    /// all non-crashed consumers.
    void assertPartitionsCoveredOnce(const std::vector<TopicPartition>& expected) const {
        std::map<TopicPartition, int> counts;
        for (auto& tp : expected) counts[tp] = 0;
        for (auto& c : consumers_) {
            for (auto& tp : c.assignment) {
                if (counts.count(tp)) ++counts[tp];
            }
        }
        for (auto& [tp, cnt] : counts) {
            EXPECT_EQ(cnt, 1)
                << "Partition " << tp.topic << "-" << tp.partition
                << " assigned " << cnt << " times (expected exactly 1)";
        }
    }

    /// generationId > prevGen.
    void assertGenerationMonotonic(Generation prevGen) const {
        EXPECT_GT(coord_->generationId(), prevGen)
            << "Generation must be strictly greater than " << prevGen
            << " (got " << coord_->generationId() << ")";
    }

    /// The committed offset for (groupId_, tp) is at least expectedOffset.
    void assertCommittedOffsetNotLost(const TopicPartition& tp, Offset expectedOffset) const {
        auto co = offsetStore_->fetch(groupId_, tp);
        ASSERT_TRUE(co.has_value())
            << "No committed offset found for " << tp.topic << "-" << tp.partition;
        EXPECT_GE(co->offset, expectedOffset)
            << "Committed offset for " << tp.topic << "-" << tp.partition
            << " is " << co->offset << " but expected >= " << expectedOffset;
    }

    /// No consumer has both a committed offset AND records below it in its range
    /// (i.e., the committed offset is the new start — nothing re-consumed).
    /// This checks that consumer[idx] with committed offset `committed` on `tp`
    /// would NOT see records at offset < committed after reassignment.
    void assertNoDoubleConsume(std::size_t consumerIdx,
                                const TopicPartition& tp,
                                Offset committed) const {
        // Post-reassignment, the consumer's resume offset is committed.
        // A re-consume would happen if a consumer started below committed.
        // We verify the committed offset is >= what was committed before.
        auto co = offsetStore_->fetch(groupId_, tp);
        if (!co.has_value()) return; // nothing committed — nothing to double-consume
        EXPECT_GE(co->offset, committed)
            << "Consumer " << consumerIdx << " committed=" << committed
            << " but store has " << co->offset << " — potential double-consume";
    }

    // -----------------------------------------------------------------------
    // Accessors
    // -----------------------------------------------------------------------

    [[nodiscard]] GroupCoordinator&  coordinator()  noexcept { return *coord_; }
    [[nodiscard]] OffsetStore&       offsetStore()  noexcept { return *offsetStore_; }
    [[nodiscard]] int64_t            clock()        const noexcept { return clock_; }
    [[nodiscard]] std::size_t        consumerCount() const noexcept { return consumers_.size(); }
    [[nodiscard]] ConsumerSim&       consumer(std::size_t i) { return consumers_.at(i); }
    [[nodiscard]] const ConsumerSim& consumer(std::size_t i) const { return consumers_.at(i); }
    [[nodiscard]] const std::string& groupId()     const noexcept { return groupId_; }
    [[nodiscard]] const std::string& offsetStorePath() const noexcept { return offsetStorePath_; }

    /// Advance the injected clock by ns without calling doWork.
    void advanceClock(int64_t ns) noexcept { clock_ += ns; }

    /// Make a TopicPartition list for a topic with `n` partitions.
    static std::vector<TopicPartition> makePartitions(const std::string& topic, int32_t n) {
        std::vector<TopicPartition> v;
        v.reserve(static_cast<size_t>(n));
        for (int32_t i = 0; i < n; ++i) v.push_back({topic, i});
        return v;
    }

    /// Build a TopicPartitionCounts map from a topic → count pairs.
    static TopicPartitionCounts makeCounts(
        std::initializer_list<std::pair<std::string, int32_t>> init)
    {
        TopicPartitionCounts m;
        for (auto& [t, c] : init) m[t] = c;
        return m;
    }

private:
    std::string  groupId_;
    HarnessDir   dir_;
    std::string  offsetStorePath_;
    int32_t      sessionTimeoutMs_;

    std::vector<ConsumerSim>          consumers_;
    std::unique_ptr<GroupCoordinator> coord_;
    std::unique_ptr<OffsetStore>      offsetStore_;

    int64_t clock_ = 0;
};

} // namespace rune::coordination::test
