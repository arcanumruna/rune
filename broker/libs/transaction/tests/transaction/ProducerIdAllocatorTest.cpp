// ---------------------------------------------------------------------------
// ProducerIdAllocatorTest.cpp — Unit tests for ProducerIdAllocator.
//
// TDD: monotonic, unique, deterministic, block boundaries.
// Bounded drivers with hard caps + ASSERT. No spin. No wall-clock waits.
// ---------------------------------------------------------------------------

#include <rune/transaction/ProducerIdAllocator.h>

#include <gtest/gtest.h>

#include <set>
#include <vector>

using namespace rune::transaction;

namespace {

// ---------------------------------------------------------------------------
// Basic allocation
// ---------------------------------------------------------------------------

TEST(ProducerIdAllocatorTest, FirstAllocationIsZero)
{
    ProducerIdAllocator alloc;
    EXPECT_EQ(alloc.allocate(), 0);
}

TEST(ProducerIdAllocatorTest, MonotonicallyIncreasing)
{
    ProducerIdAllocator alloc;
    ProducerId prev = alloc.allocate();
    constexpr int N = 50;
    for (int i = 0; i < N; ++i) {
        ProducerId next = alloc.allocate();
        EXPECT_GT(next, prev) << "id=" << next << " was not > prev=" << prev;
        prev = next;
    }
}

TEST(ProducerIdAllocatorTest, AllUnique)
{
    ProducerIdAllocator alloc;
    std::set<ProducerId> seen;
    constexpr int N = 200;
    for (int i = 0; i < N; ++i) {
        ProducerId pid = alloc.allocate();
        auto [it, inserted] = seen.insert(pid);
        EXPECT_TRUE(inserted) << "Duplicate producerId: " << pid;
    }
    EXPECT_EQ(static_cast<int>(seen.size()), N);
}

// ---------------------------------------------------------------------------
// Custom starting base
// ---------------------------------------------------------------------------

TEST(ProducerIdAllocatorTest, StartsAtRecoveredWatermark)
{
    // Simulate recovering from a crash at watermark 5000.
    ProducerIdAllocator alloc(5000);
    EXPECT_EQ(alloc.allocate(), 5000);
    EXPECT_EQ(alloc.allocate(), 5001);
    EXPECT_EQ(alloc.allocate(), 5002);
}

TEST(ProducerIdAllocatorTest, MonotonicallyIncreasingFromBase)
{
    ProducerIdAllocator alloc(1'000'000);
    ProducerId prev = alloc.allocate();
    constexpr int N = 100;
    for (int i = 0; i < N; ++i) {
        ProducerId next = alloc.allocate();
        EXPECT_GT(next, prev);
        prev = next;
    }
}

// ---------------------------------------------------------------------------
// Block boundaries
// ---------------------------------------------------------------------------

TEST(ProducerIdAllocatorTest, BlockBoundaryContinuesMonotonically)
{
    // Allocate across a full block to verify no id is skipped at the boundary.
    ProducerIdAllocator alloc;
    std::vector<ProducerId> ids;
    ids.reserve(ProducerIdAllocator::kBlockSize + 10);
    constexpr int N = static_cast<int>(ProducerIdAllocator::kBlockSize + 10);
    for (int i = 0; i < N; ++i) {
        ids.push_back(alloc.allocate());
    }

    // Check monotonic and sequential.
    for (int i = 1; i < N; ++i) {
        EXPECT_EQ(ids[i], ids[i-1] + 1)
            << "Gap at i=" << i << ": " << ids[i-1] << " -> " << ids[i];
    }
}

TEST(ProducerIdAllocatorTest, MultipleBlocksAllUnique)
{
    ProducerIdAllocator alloc;
    const int N = static_cast<int>(ProducerIdAllocator::kBlockSize * 3 + 7);
    std::set<ProducerId> seen;
    for (int i = 0; i < N; ++i) {
        auto [it, inserted] = seen.insert(alloc.allocate());
        EXPECT_TRUE(inserted) << "Duplicate at iteration " << i;
    }
}

// ---------------------------------------------------------------------------
// Accessors
// ---------------------------------------------------------------------------

TEST(ProducerIdAllocatorTest, NextIdMatchesExpected)
{
    ProducerIdAllocator alloc;
    EXPECT_EQ(alloc.nextId(), 0);
    (void)alloc.allocate();
    EXPECT_EQ(alloc.nextId(), 1);
    (void)alloc.allocate();
    EXPECT_EQ(alloc.nextId(), 2);
}

TEST(ProducerIdAllocatorTest, BlockCeilingIsMultipleOfBlockSize)
{
    ProducerIdAllocator alloc;
    // First allocation forces the block to be replenished.
    (void)alloc.allocate();
    // blockCeiling_ should be kBlockSize after the first block is opened.
    EXPECT_EQ(alloc.blockCeiling(), ProducerIdAllocator::kBlockSize);
}

TEST(ProducerIdAllocatorTest, BlockCeilingAdvancesOnNewBlock)
{
    ProducerIdAllocator alloc;
    // Exhaust first block.
    for (int i = 0; i < static_cast<int>(ProducerIdAllocator::kBlockSize); ++i) {
        (void)alloc.allocate();
    }
    ProducerId ceilingBefore = alloc.blockCeiling();
    // This allocation crosses into the second block.
    (void)alloc.allocate();
    EXPECT_GT(alloc.blockCeiling(), ceilingBefore);
}

// ---------------------------------------------------------------------------
// Independence of two allocators (no shared state)
// ---------------------------------------------------------------------------

TEST(ProducerIdAllocatorTest, TwoAllocatorsAreIndependent)
{
    ProducerIdAllocator a1;
    ProducerIdAllocator a2(10'000);

    // They should give different id ranges.
    ProducerId p1 = a1.allocate();
    ProducerId p2 = a2.allocate();
    EXPECT_NE(p1, p2);
    EXPECT_EQ(p1, 0);
    EXPECT_EQ(p2, 10'000);
}

} // anonymous namespace
