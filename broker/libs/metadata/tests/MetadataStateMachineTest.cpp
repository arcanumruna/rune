// MetadataStateMachineTest.cpp — TDD exhaustive tests for:
//   AssignmentMap, MetadataStateMachine apply/committed-only, deriveLocalDelta,
//   snapshot/restore round-trip + prune.
//
// Written BEFORE implementation — all tests FAIL until implementation is complete.

#include "rune/metadata/AssignmentMap.h"
#include "rune/metadata/MetadataStateMachine.h"
#include "rune/protocol/MetadataRecords.h"

#include <gtest/gtest.h>
#include <array>
#include <cstring>
#include <vector>
#include <functional>

using namespace rune::metadata;
using namespace rune::protocol::metadata;

// ─────────────────────────────────────────────────────────────────────────────
// Helpers: wire encoding + snapshot I/O
// ─────────────────────────────────────────────────────────────────────────────

// Encode a CreateTopicRecord into a fixed buffer and return the byte count.
static std::pair<std::array<uint8_t, 256>, std::size_t> encodeCreate(
    int32_t topicId, int32_t partitionCount, int16_t rf,
    std::string_view name = "t")
{
    CreateTopicRecord rec;
    rec.topicId        = topicId;
    rec.partitionCount = partitionCount;
    rec.rf             = rf;
    rec.name           = name;
    rec.partitionKey   = "";
    rec.placementMode  = 0;
    rec.coPartitionGroup = "";

    std::array<uint8_t, 256> buf{};
    std::size_t sz = rec.encode(buf.data(), buf.size());
    return {buf, sz};
}

// Encode an UpdatePartitionAssignmentRecord.
struct UpdateArgs {
    int32_t topicId;
    int32_t partition;
    int32_t leaderNode;
    std::vector<int32_t> isr;
    std::vector<int32_t> replicas;
    int32_t leaderEpoch    = 0;
    int32_t partitionEpoch = 0;
};

static std::pair<std::array<uint8_t, 512>, std::size_t> encodeUpdate(const UpdateArgs& a)
{
    // Build wire-format arrays manually using encode path
    // We need Int32ArrayView backed by actual storage
    std::array<uint8_t, 512> buf{};

    // Encode the whole record via UpdatePartitionAssignmentRecord
    // Int32ArrayView needs raw LE bytes. Build temporary LE arrays.
    std::vector<uint8_t> isrBytes(a.isr.size() * 4u);
    for (std::size_t i = 0; i < a.isr.size(); ++i) {
        uint32_t v = static_cast<uint32_t>(a.isr[i]);
        isrBytes[i*4+0] = static_cast<uint8_t>(v);
        isrBytes[i*4+1] = static_cast<uint8_t>(v >> 8);
        isrBytes[i*4+2] = static_cast<uint8_t>(v >> 16);
        isrBytes[i*4+3] = static_cast<uint8_t>(v >> 24);
    }
    std::vector<uint8_t> repBytes(a.replicas.size() * 4u);
    for (std::size_t i = 0; i < a.replicas.size(); ++i) {
        uint32_t v = static_cast<uint32_t>(a.replicas[i]);
        repBytes[i*4+0] = static_cast<uint8_t>(v);
        repBytes[i*4+1] = static_cast<uint8_t>(v >> 8);
        repBytes[i*4+2] = static_cast<uint8_t>(v >> 16);
        repBytes[i*4+3] = static_cast<uint8_t>(v >> 24);
    }

    UpdatePartitionAssignmentRecord rec;
    rec.topicId        = a.topicId;
    rec.partition      = a.partition;
    rec.leaderNode     = a.leaderNode;
    rec.leaderEpoch    = a.leaderEpoch;
    rec.partitionEpoch = a.partitionEpoch;
    rec.isr     = Int32ArrayView(reinterpret_cast<const int32_t*>(isrBytes.data()),
                                 static_cast<uint16_t>(a.isr.size()));
    rec.replicas = Int32ArrayView(reinterpret_cast<const int32_t*>(repBytes.data()),
                                  static_cast<uint16_t>(a.replicas.size()));

    std::size_t sz = rec.encode(buf.data(), buf.size());
    return {buf, sz};
}

// Simple byte-vector snapshot writer/reader to avoid separate I/O interfaces
// being needed in the test. The MSM snapshot/restore API accepts these.

class VecSnapshotWriter {
public:
    void writeInt32(int32_t v) {
        uint32_t u = static_cast<uint32_t>(v);
        data_.push_back(static_cast<uint8_t>(u));
        data_.push_back(static_cast<uint8_t>(u >> 8));
        data_.push_back(static_cast<uint8_t>(u >> 16));
        data_.push_back(static_cast<uint8_t>(u >> 24));
    }
    void writeInt64(int64_t v) {
        uint64_t u = static_cast<uint64_t>(v);
        for (int i = 0; i < 8; ++i)
            data_.push_back(static_cast<uint8_t>(u >> (i * 8)));
    }
    const std::vector<uint8_t>& data() const noexcept { return data_; }
private:
    std::vector<uint8_t> data_;
};

class VecSnapshotReader {
public:
    explicit VecSnapshotReader(const std::vector<uint8_t>& d) : data_(d), pos_(0) {}

    int32_t readInt32() {
        uint32_t v = uint32_t(data_[pos_])
                   | (uint32_t(data_[pos_+1]) << 8)
                   | (uint32_t(data_[pos_+2]) << 16)
                   | (uint32_t(data_[pos_+3]) << 24);
        pos_ += 4;
        return static_cast<int32_t>(v);
    }
    int64_t readInt64() {
        uint64_t v = 0;
        for (int i = 0; i < 8; ++i)
            v |= static_cast<uint64_t>(data_[pos_+i]) << (i*8);
        pos_ += 8;
        return static_cast<int64_t>(v);
    }
    bool done() const noexcept { return pos_ >= data_.size(); }
private:
    const std::vector<uint8_t>& data_;
    std::size_t pos_;
};

// Hash a map by XOR-ing all (key, assignment) fields in canonical sorted order.
// Used to assert deterministic equality between two MSM states.
static uint64_t hashMap(const MetadataStateMachine& msm);

// ─────────────────────────────────────────────────────────────────────────────
// 1. AssignmentMap — basic put/get/capacity
// ─────────────────────────────────────────────────────────────────────────────

TEST(AssignmentMap, PutAndGet)
{
    AssignmentMap m;
    PartitionAssignment pa{1, {1,2}, {1,2,3}, 1, 2};
    m.put(10, 0, pa);

    const PartitionAssignment* p = m.get(10, 0);
    ASSERT_NE(p, nullptr);
    EXPECT_EQ(p->leaderNode,      1);
    EXPECT_EQ(p->leaderEpoch,     1);
    EXPECT_EQ(p->partitionEpoch,  2);
    EXPECT_EQ(p->isr,             (std::vector<int32_t>{1,2}));
    EXPECT_EQ(p->replicas,        (std::vector<int32_t>{1,2,3}));
}

TEST(AssignmentMap, GetMissingReturnsNull)
{
    AssignmentMap m;
    EXPECT_EQ(m.get(99, 0), nullptr);
}

TEST(AssignmentMap, PutOverwritesExisting)
{
    AssignmentMap m;
    m.put(1, 0, PartitionAssignment{1, {1}, {1}, 0, 0});
    m.put(1, 0, PartitionAssignment{2, {2}, {2}, 1, 1});
    const auto* p = m.get(1, 0);
    ASSERT_NE(p, nullptr);
    EXPECT_EQ(p->leaderNode, 2);
}

TEST(AssignmentMap, MultipleKeys)
{
    AssignmentMap m;
    for (int32_t t = 0; t < 4; ++t)
        for (int32_t p = 0; p < 8; ++p)
            m.put(t, p, PartitionAssignment{t + p, {}, {}, 0, t*8+p});

    for (int32_t t = 0; t < 4; ++t)
        for (int32_t p = 0; p < 8; ++p) {
            const auto* a = m.get(t, p);
            ASSERT_NE(a, nullptr) << "t=" << t << " p=" << p;
            EXPECT_EQ(a->leaderNode,     t + p);
            EXPECT_EQ(a->partitionEpoch, t*8+p);
        }
}

// ─────────────────────────────────────────────────────────────────────────────
// 2. Apply stream → expected assignment
// ─────────────────────────────────────────────────────────────────────────────

TEST(MetadataStateMachine, ApplyCreateAndUpdate)
{
    MetadataStateMachine msm;

    // Create topic 1 with 2 partitions
    auto [cbuf, csz] = encodeCreate(1, 2, 1);
    msm.apply(cbuf.data(), csz, /*pos=*/0);
    msm.commit(0);

    // Assign p0 → leader=5, epoch 1/1
    auto [ubuf0, usz0] = encodeUpdate({1, 0, 5, {5}, {5,6}, 1, 1});
    msm.apply(ubuf0.data(), usz0, /*pos=*/1);
    msm.commit(1);

    // Assign p1 → leader=6, epoch 1/1
    auto [ubuf1, usz1] = encodeUpdate({1, 1, 6, {6,5}, {5,6}, 1, 1});
    msm.apply(ubuf1.data(), usz1, /*pos=*/2);
    msm.commit(2);

    const auto* a0 = msm.assignment(1, 0);
    ASSERT_NE(a0, nullptr);
    EXPECT_EQ(a0->leaderNode,      5);
    EXPECT_EQ(a0->leaderEpoch,     1);
    EXPECT_EQ(a0->partitionEpoch,  1);

    const auto* a1 = msm.assignment(1, 1);
    ASSERT_NE(a1, nullptr);
    EXPECT_EQ(a1->leaderNode,      6);
}

TEST(MetadataStateMachine, UnknownTopicPartitionReturnsNull)
{
    MetadataStateMachine msm;
    EXPECT_EQ(msm.assignment(99, 0), nullptr);
}

// ─────────────────────────────────────────────────────────────────────────────
// 3. Deterministic replay — apply same stream twice from empty → identical map
// ─────────────────────────────────────────────────────────────────────────────

TEST(MetadataStateMachine, DeterministicReplay)
{
    // Build a stream of records
    auto [cbuf, csz] = encodeCreate(1, 2, 2);
    auto [ubuf0, usz0] = encodeUpdate({1, 0, 10, {10,11}, {10,11}, 1, 1});
    auto [ubuf1, usz1] = encodeUpdate({1, 1, 11, {11,10}, {10,11}, 1, 1});
    // Second update to p0
    auto [ubuf2, usz2] = encodeUpdate({1, 0, 11, {11,10}, {10,11}, 2, 2});

    auto applyAll = [&](MetadataStateMachine& m) {
        m.apply(cbuf.data(),  csz,  0); m.commit(0);
        m.apply(ubuf0.data(), usz0, 1); m.commit(1);
        m.apply(ubuf1.data(), usz1, 2); m.commit(2);
        m.apply(ubuf2.data(), usz2, 3); m.commit(3);
    };

    MetadataStateMachine m1, m2;
    applyAll(m1);
    applyAll(m2);

    // Both must have the same content
    const auto* a1p0 = m1.assignment(1, 0);
    const auto* a2p0 = m2.assignment(1, 0);
    ASSERT_NE(a1p0, nullptr);
    ASSERT_NE(a2p0, nullptr);
    EXPECT_EQ(a1p0->leaderNode,      a2p0->leaderNode);
    EXPECT_EQ(a1p0->leaderEpoch,     a2p0->leaderEpoch);
    EXPECT_EQ(a1p0->partitionEpoch,  a2p0->partitionEpoch);

    const auto* a1p1 = m1.assignment(1, 1);
    const auto* a2p1 = m2.assignment(1, 1);
    ASSERT_NE(a1p1, nullptr);
    ASSERT_NE(a2p1, nullptr);
    EXPECT_EQ(a1p1->leaderNode, a2p1->leaderNode);
}

// ─────────────────────────────────────────────────────────────────────────────
// 4. Apply-committed-only:
//    Records at pos <= committedPos are visible; records above are NOT.
// ─────────────────────────────────────────────────────────────────────────────

TEST(MetadataStateMachine, ApplyCommittedOnly)
{
    MetadataStateMachine msm;
    // committedPos starts at -1 (nothing committed)

    auto [cbuf, csz] = encodeCreate(1, 1, 1);
    msm.apply(cbuf.data(), csz, /*pos=*/0);

    auto [ubuf, usz] = encodeUpdate({1, 0, 7, {7}, {7}, 1, 1});
    msm.apply(ubuf.data(), usz, /*pos=*/5);

    // With committedPos = 3, pos=5 is above it → not visible
    EXPECT_EQ(msm.assignment(1, 0), nullptr);

    // Advance committedPos to 5 → now it is visible
    msm.commit(5);
    ASSERT_NE(msm.assignment(1, 0), nullptr);
    EXPECT_EQ(msm.assignment(1, 0)->leaderNode, 7);
}

TEST(MetadataStateMachine, CommittedRecordStaysVisible)
{
    MetadataStateMachine msm;

    auto [cbuf, csz] = encodeCreate(2, 1, 1);
    msm.apply(cbuf.data(), csz, /*pos=*/0);
    msm.commit(0);

    auto [ubuf, usz] = encodeUpdate({2, 0, 3, {3}, {3}, 1, 1});
    msm.apply(ubuf.data(), usz, /*pos=*/1);
    msm.commit(1);

    // Still visible after further operations
    ASSERT_NE(msm.assignment(2, 0), nullptr);
    EXPECT_EQ(msm.assignment(2, 0)->leaderNode, 3);
}

// ─────────────────────────────────────────────────────────────────────────────
// 5. deriveLocalDelta — 4 cases + no-change
// ─────────────────────────────────────────────────────────────────────────────

// Helper: build an AssignmentMap from a single entry.
static AssignmentMap singleEntry(int32_t topicId, int32_t partition,
                                  const PartitionAssignment& pa)
{
    AssignmentMap m;
    m.put(topicId, partition, pa);
    return m;
}

// Case A: self becomes leader with a new leaderEpoch → electedLeaders
TEST(LocalRoleDelta, ElectedLeader)
{
    // prev: node 1 is leader, le=1, pe=1
    AssignmentMap prev = singleEntry(1, 0, PartitionAssignment{1, {1,2}, {1,2}, 1, 1});

    // current MSM: node 1 still leader but leaderEpoch bumped (replica removed)
    MetadataStateMachine msm;
    auto [cbuf, csz] = encodeCreate(1, 1, 2);
    msm.apply(cbuf.data(), csz, 0);
    msm.commit(0);
    // Install: leader=1, le=2, pe=2 (leaderEpoch bumped)
    auto [ubuf, usz] = encodeUpdate({1, 0, 1, {1}, {1}, 2, 2});
    msm.apply(ubuf.data(), usz, 1);
    msm.commit(1);

    LocalRoleDelta delta = msm.deriveLocalDelta(/*selfNode=*/1, prev);

    EXPECT_EQ(delta.electedLeaders.size(), 1u);
    EXPECT_TRUE(delta.leaderUpdates.empty());
    EXPECT_TRUE(delta.followers.empty());
    EXPECT_TRUE(delta.deletes.empty());

    EXPECT_EQ(delta.electedLeaders[0].topicId,   1);
    EXPECT_EQ(delta.electedLeaders[0].partition, 0);
}

// Case B: self stays leader but partitionEpoch changed (ISR change) → leaderUpdates
TEST(LocalRoleDelta, LeaderUpdate)
{
    // prev: node 5 is leader, le=1, pe=1
    AssignmentMap prev = singleEntry(1, 0, PartitionAssignment{5, {5,6}, {5,6}, 1, 1});

    // current: node 5 still leader, leaderEpoch unchanged (1), partitionEpoch bumped (2)
    MetadataStateMachine msm;
    auto [cbuf, csz] = encodeCreate(1, 1, 2);
    msm.apply(cbuf.data(), csz, 0);
    msm.commit(0);
    auto [ubuf, usz] = encodeUpdate({1, 0, 5, {5}, {5,6}, 1, 2}); // le=1, pe=2
    msm.apply(ubuf.data(), usz, 1);
    msm.commit(1);

    LocalRoleDelta delta = msm.deriveLocalDelta(/*selfNode=*/5, prev);

    EXPECT_TRUE(delta.electedLeaders.empty());
    EXPECT_EQ(delta.leaderUpdates.size(), 1u);
    EXPECT_TRUE(delta.followers.empty());
    EXPECT_TRUE(delta.deletes.empty());

    EXPECT_EQ(delta.leaderUpdates[0].topicId,   1);
    EXPECT_EQ(delta.leaderUpdates[0].partition, 0);
}

// Case C: self not leader but partitionEpoch changed → followers
TEST(LocalRoleDelta, Follower)
{
    // prev: node 5 is leader, node 6 is follower, pe=1
    AssignmentMap prev = singleEntry(1, 0, PartitionAssignment{5, {5,6}, {5,6}, 1, 1});

    // current: still same leader, pe bumped to 2
    MetadataStateMachine msm;
    auto [cbuf, csz] = encodeCreate(1, 1, 2);
    msm.apply(cbuf.data(), csz, 0);
    msm.commit(0);
    auto [ubuf, usz] = encodeUpdate({1, 0, 5, {5,6}, {5,6}, 1, 2}); // le=1, pe=2
    msm.apply(ubuf.data(), usz, 1);
    msm.commit(1);

    LocalRoleDelta delta = msm.deriveLocalDelta(/*selfNode=*/6, prev);

    EXPECT_TRUE(delta.electedLeaders.empty());
    EXPECT_TRUE(delta.leaderUpdates.empty());
    EXPECT_EQ(delta.followers.size(), 1u);
    EXPECT_TRUE(delta.deletes.empty());
}

// Case D: self was a replica, now absent → deletes
TEST(LocalRoleDelta, Delete)
{
    // prev: node 7 was in replicas
    AssignmentMap prev = singleEntry(1, 0, PartitionAssignment{5, {5,6}, {5,6,7}, 1, 1});

    // current: node 7 removed from replicas (replica removed → le bumped too)
    MetadataStateMachine msm;
    auto [cbuf, csz] = encodeCreate(1, 1, 2);
    msm.apply(cbuf.data(), csz, 0);
    msm.commit(0);
    auto [ubuf, usz] = encodeUpdate({1, 0, 5, {5,6}, {5,6}, 2, 2}); // 7 removed → le=2, pe=2
    msm.apply(ubuf.data(), usz, 1);
    msm.commit(1);

    LocalRoleDelta delta = msm.deriveLocalDelta(/*selfNode=*/7, prev);

    EXPECT_TRUE(delta.electedLeaders.empty());
    EXPECT_TRUE(delta.leaderUpdates.empty());
    EXPECT_TRUE(delta.followers.empty());
    EXPECT_EQ(delta.deletes.size(), 1u);

    EXPECT_EQ(delta.deletes[0].topicId,   1);
    EXPECT_EQ(delta.deletes[0].partition, 0);
}

// Case E: no change → appears in none
TEST(LocalRoleDelta, NoChange_AppearsInNone)
{
    // prev and current are identical
    PartitionAssignment pa{5, {5,6}, {5,6}, 1, 1};
    AssignmentMap prev = singleEntry(1, 0, pa);

    MetadataStateMachine msm;
    auto [cbuf, csz] = encodeCreate(1, 1, 2);
    msm.apply(cbuf.data(), csz, 0);
    msm.commit(0);
    auto [ubuf, usz] = encodeUpdate({1, 0, 5, {5,6}, {5,6}, 1, 1}); // identical
    msm.apply(ubuf.data(), usz, 1);
    msm.commit(1);

    // selfNode=5 (the leader): no epoch change → no delta
    LocalRoleDelta delta5 = msm.deriveLocalDelta(5, prev);
    EXPECT_TRUE(delta5.electedLeaders.empty());
    EXPECT_TRUE(delta5.leaderUpdates.empty());
    EXPECT_TRUE(delta5.followers.empty());
    EXPECT_TRUE(delta5.deletes.empty());

    // selfNode=6 (a follower): no epoch change → no delta
    LocalRoleDelta delta6 = msm.deriveLocalDelta(6, prev);
    EXPECT_TRUE(delta6.electedLeaders.empty());
    EXPECT_TRUE(delta6.leaderUpdates.empty());
    EXPECT_TRUE(delta6.followers.empty());
    EXPECT_TRUE(delta6.deletes.empty());
}

// Case F: partition is new in current, absent in prev → if self is leader → electedLeaders
TEST(LocalRoleDelta, NewPartition_ElectedLeader)
{
    AssignmentMap prev; // empty — no prior knowledge of partition

    MetadataStateMachine msm;
    auto [cbuf, csz] = encodeCreate(3, 1, 1);
    msm.apply(cbuf.data(), csz, 0);
    msm.commit(0);
    auto [ubuf, usz] = encodeUpdate({3, 0, 9, {9}, {9}, 1, 1});
    msm.apply(ubuf.data(), usz, 1);
    msm.commit(1);

    LocalRoleDelta delta = msm.deriveLocalDelta(/*selfNode=*/9, prev);

    // selfNode=9 is the new leader — this is an election
    EXPECT_EQ(delta.electedLeaders.size(), 1u);
    EXPECT_TRUE(delta.leaderUpdates.empty());
    EXPECT_TRUE(delta.followers.empty());
    EXPECT_TRUE(delta.deletes.empty());
}

// ─────────────────────────────────────────────────────────────────────────────
// 6. Snapshot → restore round-trip: identical map (hash equality)
// ─────────────────────────────────────────────────────────────────────────────

TEST(MetadataStateMachine, SnapshotRestoreRoundTrip)
{
    MetadataStateMachine msm;

    auto [cbuf, csz] = encodeCreate(1, 2, 2);
    msm.apply(cbuf.data(), csz, 0);
    msm.commit(0);
    auto [u0buf, u0sz] = encodeUpdate({1, 0, 10, {10,11}, {10,11}, 1, 1});
    msm.apply(u0buf.data(), u0sz, 1);
    msm.commit(1);
    auto [u1buf, u1sz] = encodeUpdate({1, 1, 11, {11,10}, {10,11}, 1, 1});
    msm.apply(u1buf.data(), u1sz, 2);
    msm.commit(2);

    VecSnapshotWriter w;
    msm.snapshot(w);

    MetadataStateMachine restored;
    VecSnapshotReader r(w.data());
    restored.restore(r);

    // Both should have the same assignments
    const auto* orig0  = msm.assignment(1, 0);
    const auto* rest0  = restored.assignment(1, 0);
    ASSERT_NE(orig0,  nullptr);
    ASSERT_NE(rest0,  nullptr);
    EXPECT_EQ(orig0->leaderNode,     rest0->leaderNode);
    EXPECT_EQ(orig0->leaderEpoch,    rest0->leaderEpoch);
    EXPECT_EQ(orig0->partitionEpoch, rest0->partitionEpoch);
    EXPECT_EQ(orig0->isr,            rest0->isr);
    EXPECT_EQ(orig0->replicas,       rest0->replicas);

    const auto* orig1  = msm.assignment(1, 1);
    const auto* rest1  = restored.assignment(1, 1);
    ASSERT_NE(orig1,  nullptr);
    ASSERT_NE(rest1,  nullptr);
    EXPECT_EQ(orig1->leaderNode,     rest1->leaderNode);
    EXPECT_EQ(orig1->leaderEpoch,    rest1->leaderEpoch);
    EXPECT_EQ(orig1->partitionEpoch, rest1->partitionEpoch);
}

// ─────────────────────────────────────────────────────────────────────────────
// 7. Restore prunes: create A+B, snapshot with only A → restore ⇒ B gone
// ─────────────────────────────────────────────────────────────────────────────

TEST(MetadataStateMachine, RestorePrunesAbsentAssignments)
{
    // Create MSM with two partition assignments
    MetadataStateMachine original;
    auto [cbuf, csz] = encodeCreate(1, 2, 1);
    original.apply(cbuf.data(), csz, 0);
    original.commit(0);

    auto [ua, uasz] = encodeUpdate({1, 0, 1, {1}, {1}, 1, 1});
    original.apply(ua.data(), uasz, 1);
    original.commit(1);

    auto [ub, ubsz] = encodeUpdate({1, 1, 2, {2}, {2}, 1, 1});
    original.apply(ub.data(), ubsz, 2);
    original.commit(2);

    ASSERT_NE(original.assignment(1, 0), nullptr);
    ASSERT_NE(original.assignment(1, 1), nullptr);

    // Build a snapshot that only contains partition 0 (simulate a pruned state)
    // We do this by building a fresh MSM with only partition 0, then snapshotting it.
    MetadataStateMachine partialSource;
    {
        auto [cb, cbs] = encodeCreate(1, 1, 1);
        partialSource.apply(cb.data(), cbs, 0);
        partialSource.commit(0);
        auto [upa, upas] = encodeUpdate({1, 0, 1, {1}, {1}, 1, 1});
        partialSource.apply(upa.data(), upas, 1);
        partialSource.commit(1);
    }

    VecSnapshotWriter w;
    partialSource.snapshot(w);

    // Restore into original — must reset and only A survives
    VecSnapshotReader r(w.data());
    original.restore(r);

    ASSERT_NE(original.assignment(1, 0), nullptr);  // A survives
    EXPECT_EQ(original.assignment(1, 1), nullptr);  // B pruned
}

// ─────────────────────────────────────────────────────────────────────────────
// 8. Structural sharing: applying one topic's change does not affect another
// ─────────────────────────────────────────────────────────────────────────────

TEST(MetadataStateMachine, StructuralSharing_OtherTopicUnaffected)
{
    MetadataStateMachine msm;

    // Create two topics
    auto [cb1, cs1] = encodeCreate(1, 1, 1);
    msm.apply(cb1.data(), cs1, 0);
    msm.commit(0);
    auto [cb2, cs2] = encodeCreate(2, 1, 1);
    msm.apply(cb2.data(), cs2, 1);
    msm.commit(1);

    // Assign both
    auto [ub1, us1] = encodeUpdate({1, 0, 10, {10}, {10}, 1, 1});
    msm.apply(ub1.data(), us1, 2);
    msm.commit(2);

    auto [ub2, us2] = encodeUpdate({2, 0, 20, {20}, {20}, 1, 1});
    msm.apply(ub2.data(), us2, 3);
    msm.commit(3);

    const PartitionAssignment* before = msm.assignment(1, 0);
    ASSERT_NE(before, nullptr);
    int32_t leaderBefore = before->leaderNode;
    int32_t peBefore     = before->partitionEpoch;

    // Update topic 2's partition
    auto [ub2b, us2b] = encodeUpdate({2, 0, 21, {21}, {20,21}, 2, 2});
    msm.apply(ub2b.data(), us2b, 4);
    msm.commit(4);

    // Topic 1 assignment must be unchanged
    const PartitionAssignment* after = msm.assignment(1, 0);
    ASSERT_NE(after, nullptr);
    EXPECT_EQ(after->leaderNode,     leaderBefore);
    EXPECT_EQ(after->partitionEpoch, peBefore);
}

// ─────────────────────────────────────────────────────────────────────────────
// 9. Unknown record version → skipped deterministically (no crash, no state change)
// ─────────────────────────────────────────────────────────────────────────────

TEST(MetadataStateMachine, UnknownRecordVersion_Skipped)
{
    MetadataStateMachine msm;

    // Encode a valid CreateTopicRecord
    auto [cbuf, csz] = encodeCreate(1, 1, 1);
    msm.apply(cbuf.data(), csz, 0);
    msm.commit(0);

    // Corrupt the version byte (index 2) to an unknown version
    auto [ubuf, usz] = encodeUpdate({1, 0, 5, {5}, {5}, 1, 1});
    ubuf[2] = 0xFF; // unknown version
    msm.apply(ubuf.data(), usz, 1); // must not crash
    msm.commit(1);

    // assignment should remain absent (record was skipped)
    EXPECT_EQ(msm.assignment(1, 0), nullptr);
}

// ─────────────────────────────────────────────────────────────────────────────
// 10. NodeJoin/NodeLeave records are applied without crashing (no partition state)
// ─────────────────────────────────────────────────────────────────────────────

TEST(MetadataStateMachine, NodeJoinLeave_NoPartitionStateChange)
{
    MetadataStateMachine msm;

    NodeJoinRecord njr;
    njr.nodeId   = 42;
    njr.endpoint = "127.0.0.1:9000";
    std::array<uint8_t, 64> njbuf{};
    std::size_t njsz = njr.encode(njbuf.data(), njbuf.size());
    msm.apply(njbuf.data(), njsz, 0);
    msm.commit(0);

    NodeLeaveRecord nlr;
    nlr.nodeId = 42;
    std::array<uint8_t, 16> nlbuf{};
    std::size_t nlsz = nlr.encode(nlbuf.data(), nlbuf.size());
    msm.apply(nlbuf.data(), nlsz, 1);
    msm.commit(1);

    // No partition state — all null
    EXPECT_EQ(msm.assignment(42, 0), nullptr);
}

// ─────────────────────────────────────────────────────────────────────────────
// 11. CommittedPos boundary: record at exactly committedPos IS visible
// ─────────────────────────────────────────────────────────────────────────────

TEST(MetadataStateMachine, ExactBoundary_CommittedPos)
{
    MetadataStateMachine msm;
    auto [cbuf, csz] = encodeCreate(5, 1, 1);
    msm.apply(cbuf.data(), csz, 100);
    msm.commit(100);

    auto [ubuf, usz] = encodeUpdate({5, 0, 3, {3}, {3}, 1, 1});
    msm.apply(ubuf.data(), usz, 200);
    // Not yet committed
    EXPECT_EQ(msm.assignment(5, 0), nullptr);
    // Commit at exactly 200
    msm.commit(200);
    ASSERT_NE(msm.assignment(5, 0), nullptr);
    EXPECT_EQ(msm.assignment(5, 0)->leaderNode, 3);
}

// ─────────────────────────────────────────────────────────────────────────────
// 12. Multiple updates to same partition — last committed wins
// ─────────────────────────────────────────────────────────────────────────────

TEST(MetadataStateMachine, MultipleUpdates_LastCommittedWins)
{
    MetadataStateMachine msm;
    auto [cbuf, csz] = encodeCreate(1, 1, 2);
    msm.apply(cbuf.data(), csz, 0);
    msm.commit(0);

    auto [u1, s1] = encodeUpdate({1, 0, 10, {10,11}, {10,11}, 1, 1});
    msm.apply(u1.data(), s1, 1);
    msm.commit(1);

    auto [u2, s2] = encodeUpdate({1, 0, 11, {11,10}, {10,11}, 2, 2});
    msm.apply(u2.data(), s2, 2);
    msm.commit(2);

    auto [u3, s3] = encodeUpdate({1, 0, 11, {11}, {10,11}, 2, 3});
    msm.apply(u3.data(), s3, 3);
    msm.commit(3);

    const auto* a = msm.assignment(1, 0);
    ASSERT_NE(a, nullptr);
    EXPECT_EQ(a->leaderNode,     11);
    EXPECT_EQ(a->leaderEpoch,    2);
    EXPECT_EQ(a->partitionEpoch, 3);
}

// ─────────────────────────────────────────────────────────────────────────────
// 13. AssignmentMap — remove
// ─────────────────────────────────────────────────────────────────────────────

TEST(AssignmentMap, RemoveExistingKey)
{
    AssignmentMap m;
    m.put(1, 0, PartitionAssignment{1, {1}, {1}, 0, 0});
    m.remove(1, 0);
    EXPECT_EQ(m.get(1, 0), nullptr);
}

TEST(AssignmentMap, RemoveNonExistentNoOp)
{
    AssignmentMap m;
    // Should not crash
    m.remove(99, 99);
    EXPECT_EQ(m.get(99, 99), nullptr);
}

// ─────────────────────────────────────────────────────────────────────────────
// 14. Large load (stress the power-of-2 rehashing)
// ─────────────────────────────────────────────────────────────────────────────

TEST(AssignmentMap, LargeLoad)
{
    AssignmentMap m;
    const int N = 1024;
    for (int32_t i = 0; i < N; ++i)
        m.put(i / 32, i % 32, PartitionAssignment{i, {}, {}, 0, i});

    for (int32_t i = 0; i < N; ++i) {
        const auto* a = m.get(i / 32, i % 32);
        ASSERT_NE(a, nullptr) << "missing i=" << i;
        EXPECT_EQ(a->leaderNode,     i);
        EXPECT_EQ(a->partitionEpoch, i);
    }
}
