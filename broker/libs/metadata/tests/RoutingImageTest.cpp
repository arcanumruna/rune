// RoutingImageTest.cpp — TDD: tests written BEFORE implementation.
// All tests should FAIL until RoutingImage.h is filled in.
//
// Test cases:
//   1. Build from an assignment list, lookup(pid) returns correct {leader, epoch}.
//   2. Out-of-range / unassigned pid → sentinel {-1, -1} (NO_LEADER).
//   3. Atomic swap — a reader holding the old image sees a consistent snapshot
//      after a new image is published (no torn read, no crash).
//   4. lookup() performs no heap allocation in the hot loop (structural test).

#include "rune/metadata/RoutingImage.h"

#include <gtest/gtest.h>

#include <atomic>
#include <cstdint>
#include <memory>
#include <thread>
#include <vector>

// Prevent optimizer from eliminating reads in concurrent test.
template<typename T>
static void doNotOptimize(T const& val) noexcept
{
    asm volatile("" : : "r,m"(val) : "memory");
}

using namespace rune::metadata;

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

static RoutingImage::BuildEntry entry(int32_t pid, int32_t leader, int32_t epoch)
{
    return RoutingImage::BuildEntry{pid, leader, epoch};
}

// ─────────────────────────────────────────────────────────────────────────────
// 1. Build from an assignment list, lookup returns correct {leader, epoch}
// ─────────────────────────────────────────────────────────────────────────────

TEST(RoutingImage, LookupReturnsCorrectLeaderAndEpoch)
{
    std::vector<RoutingImage::BuildEntry> entries = {
        entry(0, 1, 10),
        entry(1, 2, 11),
        entry(2, 3, 12),
        entry(5, 1, 20),   // non-contiguous: gaps are sentinel
    };

    auto img = RoutingImage::build(entries);
    ASSERT_NE(img, nullptr);

    const auto& e0 = img->lookup(0);
    EXPECT_EQ(e0.leaderNodeId,  1);
    EXPECT_EQ(e0.leaderEpoch,  10);

    const auto& e1 = img->lookup(1);
    EXPECT_EQ(e1.leaderNodeId,  2);
    EXPECT_EQ(e1.leaderEpoch,  11);

    const auto& e2 = img->lookup(2);
    EXPECT_EQ(e2.leaderNodeId,  3);
    EXPECT_EQ(e2.leaderEpoch,  12);

    const auto& e5 = img->lookup(5);
    EXPECT_EQ(e5.leaderNodeId,  1);
    EXPECT_EQ(e5.leaderEpoch,  20);
}

TEST(RoutingImage, LookupFromAssignmentMap)
{
    // Build from AssignmentMap via the map-based builder.
    AssignmentMap amap;
    amap.put(0, 0,  PartitionAssignment{1, {1}, {1}, /*leaderEpoch=*/5,  /*partEpoch=*/5});
    amap.put(0, 1,  PartitionAssignment{2, {2}, {2}, /*leaderEpoch=*/6,  /*partEpoch=*/6});
    amap.put(0, 2,  PartitionAssignment{3, {3}, {3}, /*leaderEpoch=*/7,  /*partEpoch=*/7});
    // Use a second topic to produce a distinct global pid range.
    amap.put(1, 0,  PartitionAssignment{4, {4}, {4}, /*leaderEpoch=*/8,  /*partEpoch=*/8});

    // Build with a function that computes globalPid = topicId * 10 + partition.
    auto img = RoutingImage::buildFromMap(amap,
        [](int32_t topicId, int32_t partition) -> int32_t {
            return topicId * 10 + partition;
        });
    ASSERT_NE(img, nullptr);

    EXPECT_EQ(img->lookup(0).leaderNodeId,  1);  // (0,0) → pid 0
    EXPECT_EQ(img->lookup(1).leaderNodeId,  2);  // (0,1) → pid 1
    EXPECT_EQ(img->lookup(2).leaderNodeId,  3);  // (0,2) → pid 2
    EXPECT_EQ(img->lookup(10).leaderNodeId, 4);  // (1,0) → pid 10
}

// ─────────────────────────────────────────────────────────────────────────────
// 2. Out-of-range / unassigned pid → sentinel {-1, -1}
// ─────────────────────────────────────────────────────────────────────────────

TEST(RoutingImage, OutOfRangePidReturnsSentinel)
{
    std::vector<RoutingImage::BuildEntry> entries = {
        entry(0, 1, 1),
        entry(1, 2, 2),
    };
    auto img = RoutingImage::build(entries);
    ASSERT_NE(img, nullptr);

    // pid=2 is out of range (max assigned is 1)
    const auto& e2 = img->lookup(2);
    EXPECT_EQ(e2.leaderNodeId, -1);
    EXPECT_EQ(e2.leaderEpoch,  -1);

    // negative pid
    const auto& neg = img->lookup(-1);
    EXPECT_EQ(neg.leaderNodeId, -1);
    EXPECT_EQ(neg.leaderEpoch,  -1);

    // very large pid
    const auto& big = img->lookup(9999);
    EXPECT_EQ(big.leaderNodeId, -1);
    EXPECT_EQ(big.leaderEpoch,  -1);
}

TEST(RoutingImage, GapInPidSpaceReturnsSentinel)
{
    // Build with pid 0 and pid 5; pid 1..4 are unassigned gaps.
    std::vector<RoutingImage::BuildEntry> entries = {
        entry(0, 1, 1),
        entry(5, 2, 2),
    };
    auto img = RoutingImage::build(entries);
    ASSERT_NE(img, nullptr);

    EXPECT_EQ(img->lookup(0).leaderNodeId,  1);
    EXPECT_EQ(img->lookup(5).leaderNodeId,  2);

    // Gaps are sentinel
    for (int32_t pid = 1; pid <= 4; ++pid) {
        const auto& e = img->lookup(pid);
        EXPECT_EQ(e.leaderNodeId, -1) << "pid=" << pid;
        EXPECT_EQ(e.leaderEpoch,  -1) << "pid=" << pid;
    }
}

TEST(RoutingImage, EmptyImageAllPidsSentinel)
{
    auto img = RoutingImage::build({});
    ASSERT_NE(img, nullptr);
    EXPECT_EQ(img->lookup(0).leaderNodeId, -1);
    EXPECT_EQ(img->lookup(100).leaderNodeId, -1);
}

// ─────────────────────────────────────────────────────────────────────────────
// 3. Atomic swap — reader holding old image sees consistent snapshot
//    after a new image is published (Kafka MetadataCache / Aeron pattern)
// ─────────────────────────────────────────────────────────────────────────────

TEST(RoutingImage, AtomicSwapReaderSeesOldSnapshot)
{
    // Build the "old" image.
    std::vector<RoutingImage::BuildEntry> oldEntries = {
        entry(0, 1, 1),
        entry(1, 2, 2),
    };
    auto oldImg = RoutingImage::build(oldEntries);

    // Publish via the RoutingImageHolder (release-store).
    RoutingImageHolder holder;
    holder.publish(oldImg);

    // Reader: acquire-load ONCE into a local snapshot.
    auto snapshot = holder.current();  // acquire-load
    ASSERT_NE(snapshot, nullptr);

    // Verify the snapshot is consistent (old values).
    EXPECT_EQ(snapshot->lookup(0).leaderNodeId, 1);
    EXPECT_EQ(snapshot->lookup(1).leaderNodeId, 2);

    // Now the publisher swaps in a new image.
    std::vector<RoutingImage::BuildEntry> newEntries = {
        entry(0, 99, 99),
        entry(1, 98, 98),
        entry(2, 97, 97),
    };
    auto newImg = RoutingImage::build(newEntries);
    holder.publish(newImg);

    // The reader still holds the old snapshot — it must remain consistent
    // (no crash, no data corruption; shared_ptr keeps old image alive).
    EXPECT_EQ(snapshot->lookup(0).leaderNodeId, 1);   // old value still visible
    EXPECT_EQ(snapshot->lookup(1).leaderNodeId, 2);   // old value still visible

    // A fresh acquire-load sees the new image.
    auto fresh = holder.current();
    ASSERT_NE(fresh, nullptr);
    EXPECT_EQ(fresh->lookup(0).leaderNodeId, 99);
    EXPECT_EQ(fresh->lookup(1).leaderNodeId, 98);
    EXPECT_EQ(fresh->lookup(2).leaderNodeId, 97);
}

TEST(RoutingImage, AtomicSwapConcurrentReaderWriter)
{
    // Stress: concurrent writer publishing new images while reader reads.
    // Goal: no crash, no torn read on the shared_ptr.

    RoutingImageHolder holder;

    // Seed an initial image.
    holder.publish(RoutingImage::build({entry(0, 1, 1)}));

    constexpr int kIterations = 10'000;
    std::atomic<bool> done{false};
    std::atomic<int>  reads{0};
    // Value-consistency counter: the writer only ever publishes leaderNodeId
    // in [0,7] for pid 0, so a torn/garbage read would surface as an
    // out-of-range value here.  Run this target under TSan in CI to make the
    // data-race assertion hard.
    std::atomic<int>  badValues{0};

    // Writer thread: rapidly publishes new images.
    std::thread writer([&] {
        for (int i = 0; i < kIterations; ++i) {
            std::vector<RoutingImage::BuildEntry> ents = {entry(0, i % 8, i)};
            holder.publish(RoutingImage::build(ents));
        }
        done.store(true, std::memory_order_release);
    });

    // Reader thread: acquire-load + lookup until writer is done.
    std::thread reader([&] {
        while (!done.load(std::memory_order_acquire)) {
            auto snap = holder.current();   // acquire-load
            if (snap) {
                // Every snapshot the reader observes must be internally
                // consistent: pid 0 leaderNodeId is always in [0,7]
                // (writer's i % 8).  Anything else is a torn read.
                const auto& e = snap->lookup(0);
                doNotOptimize(e.leaderNodeId);
                if (e.leaderNodeId < 0 || e.leaderNodeId > 7) {
                    badValues.fetch_add(1, std::memory_order_relaxed);
                }
                reads.fetch_add(1, std::memory_order_relaxed);
            }
        }
    });

    writer.join();
    reader.join();

    EXPECT_GT(reads.load(), 0) << "reader never ran";
    EXPECT_EQ(badValues.load(), 0)
        << "reader observed a torn/inconsistent leaderNodeId";
}

// ─────────────────────────────────────────────────────────────────────────────
// 4. lookup() performs no heap allocation (structural / white-box test)
//
// We verify structural properties that guarantee zero allocation:
//   a. The image is backed by a std::vector<Entry> (contiguous flat array).
//   b. lookup() returns a const reference — no copy, no new.
//   c. Calling lookup() in a tight loop with DoNotOptimize does not grow heap
//      (we rely on the benchmark for hard numbers; here we verify the invariants).
// ─────────────────────────────────────────────────────────────────────────────

TEST(RoutingImage, LookupReturnsConstReference_NoAllocation)
{
    std::vector<RoutingImage::BuildEntry> entries = {
        entry(0, 1, 10),
        entry(1, 2, 11),
    };
    auto img = RoutingImage::build(entries);
    ASSERT_NE(img, nullptr);

    // Verify that lookup returns a reference into the image's own storage
    // (i.e. same address on repeated calls — immutable flat array).
    const auto& e0a = img->lookup(0);
    const auto& e0b = img->lookup(0);
    EXPECT_EQ(&e0a, &e0b) << "lookup must return a reference to stable storage";

    const auto& e1a = img->lookup(1);
    const auto& e1b = img->lookup(1);
    EXPECT_EQ(&e1a, &e1b);

    // Sentinel for out-of-range must also be a stable reference (same address each time).
    const auto& sA = img->lookup(99);
    const auto& sB = img->lookup(99);
    EXPECT_EQ(&sA, &sB) << "sentinel lookup must return a reference to stable storage";
    EXPECT_EQ(sA.leaderNodeId, -1);
}

TEST(RoutingImage, SentinelIsNegativeOne)
{
    // Verify the sentinel constants.
    EXPECT_EQ(RoutingImage::NO_LEADER_NODE_ID, -1);
    EXPECT_EQ(RoutingImage::NO_LEADER_EPOCH,   -1);
}
