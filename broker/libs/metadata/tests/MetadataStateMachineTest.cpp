// MetadataStateMachineTest.cpp — TDD exhaustive tests for:
//   AssignmentMap, MetadataStateMachine apply/committed-only, deriveLocalDelta,
//   snapshot/restore round-trip + prune.
//
// Written BEFORE implementation — all tests FAIL until implementation is complete.

#include "rune/metadata/AssignmentMap.h"
#include "rune/metadata/MetadataStateMachine.h"
#include "rune/protocol/MetadataRecords.h"

#include <gtest/gtest.h>
#include <algorithm>
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

// Hash a map by folding ALL (key, assignment) fields in canonical sorted order.
// Entries are sorted by (topicId, partition) for a stable traversal order
// independent of hash-table slot layout, then each field is mixed into the
// running hash with FNV-1a-style multiplications.  Used to assert deterministic
// equality between two MSM states (whole-map hash, not just 2 partitions).
static uint64_t hashMap(const MetadataStateMachine& msm)
{
    // Collect all entries into a sortable vector
    struct Entry {
        int32_t topicId;
        int32_t partition;
        int32_t leaderNode;
        int32_t leaderEpoch;
        int32_t partitionEpoch;
        std::vector<int32_t> isr;     // stored as-is (caller's sort order)
        std::vector<int32_t> replicas;
    };
    std::vector<Entry> entries;
    msm.committedMap().forEach([&](int32_t t, int32_t p, const PartitionAssignment& pa) {
        Entry e;
        e.topicId        = t;
        e.partition      = p;
        e.leaderNode     = pa.leaderNode;
        e.leaderEpoch    = pa.leaderEpoch;
        e.partitionEpoch = pa.partitionEpoch;
        e.isr            = pa.isr;
        e.replicas       = pa.replicas;
        entries.push_back(std::move(e));
    });

    // Sort by (topicId, partition) for a canonical, slot-independent order
    std::sort(entries.begin(), entries.end(), [](const Entry& a, const Entry& b) {
        if (a.topicId != b.topicId) return a.topicId < b.topicId;
        return a.partition < b.partition;
    });

    // Mix every field into a 64-bit hash
    auto mix = [](uint64_t h, int32_t v) -> uint64_t {
        h ^= static_cast<uint64_t>(static_cast<uint32_t>(v));
        h *= UINT64_C(11400714819323198485); // Fibonacci/Knuth multiplier
        h ^= h >> 33;
        return h;
    };

    uint64_t h = UINT64_C(0xcbf29ce484222325); // FNV offset basis
    for (const Entry& e : entries) {
        h = mix(h, e.topicId);
        h = mix(h, e.partition);
        h = mix(h, e.leaderNode);
        h = mix(h, e.leaderEpoch);
        h = mix(h, e.partitionEpoch);
        h = mix(h, static_cast<int32_t>(e.isr.size()));
        for (int32_t v : e.isr)     h = mix(h, v);
        h = mix(h, static_cast<int32_t>(e.replicas.size()));
        for (int32_t v : e.replicas) h = mix(h, v);
    }
    return h;
}

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
//    Proved by whole-map hash equality (hashMap(m1) == hashMap(m2)).
// ─────────────────────────────────────────────────────────────────────────────

TEST(MetadataStateMachine, DeterministicReplay)
{
    // Build a stream of records covering 2 topics, 3 partitions, multiple updates
    auto [cbuf,  csz]  = encodeCreate(1, 2, 2);
    auto [cb2,   cs2]  = encodeCreate(2, 1, 1);
    auto [ubuf0, usz0] = encodeUpdate({1, 0, 10, {10,11}, {10,11}, 1, 1});
    auto [ubuf1, usz1] = encodeUpdate({1, 1, 11, {11,10}, {10,11}, 1, 1});
    auto [ubuf2, usz2] = encodeUpdate({1, 0, 11, {11,10}, {10,11}, 2, 2}); // update p0
    auto [ubuf3, usz3] = encodeUpdate({2, 0, 20, {20},    {20},    1, 1});

    auto applyAll = [&](MetadataStateMachine& m) {
        m.apply(cbuf.data(),  csz,  0); m.commit(0);
        m.apply(cb2.data(),   cs2,  1); m.commit(1);
        m.apply(ubuf0.data(), usz0, 2); m.commit(2);
        m.apply(ubuf1.data(), usz1, 3); m.commit(3);
        m.apply(ubuf2.data(), usz2, 4); m.commit(4);
        m.apply(ubuf3.data(), usz3, 5); m.commit(5);
    };

    MetadataStateMachine m1, m2;
    applyAll(m1);
    applyAll(m2);

    // Whole-map hash equality proves determinism across ALL fields of ALL entries
    EXPECT_EQ(hashMap(m1), hashMap(m2));

    // Spot-check a couple of entries for clarity
    const auto* a1p0 = m1.assignment(1, 0);
    const auto* a2p0 = m2.assignment(1, 0);
    ASSERT_NE(a1p0, nullptr);
    ASSERT_NE(a2p0, nullptr);
    EXPECT_EQ(a1p0->leaderNode,      a2p0->leaderNode);
    EXPECT_EQ(a1p0->leaderEpoch,     a2p0->leaderEpoch);
    EXPECT_EQ(a1p0->partitionEpoch,  a2p0->partitionEpoch);
}

// ─────────────────────────────────────────────────────────────────────────────
// 3b. FlushPending with >1 entries queued: commit advances past multiple pending
//     entries in one shot.  The multi-entry sort path in flushPending() is the
//     determinism-critical path; assert hash equality across two machines that
//     receive the same records in the same order but only commit once at the end.
// ─────────────────────────────────────────────────────────────────────────────

TEST(MetadataStateMachine, FlushPendingMultiEntry_Deterministic)
{
    auto [cbuf,  csz]  = encodeCreate(1, 3, 2);
    auto [ubuf0, usz0] = encodeUpdate({1, 0, 10, {10,11}, {10,11}, 1, 1});
    auto [ubuf1, usz1] = encodeUpdate({1, 1, 11, {11,10}, {10,11}, 1, 1});
    auto [ubuf2, usz2] = encodeUpdate({1, 2, 12, {12},    {10,11,12}, 1, 1});

    // Apply all records WITHOUT committing — they all queue into pending_.
    // Then commit once at pos=3, flushing all 3 pending entries in one shot.
    // This exercises flushPending()'s multi-entry sort on the determinism path.
    auto applyAllThenCommit = [&](MetadataStateMachine& m) {
        // CreateTopicRecord at pos=0: commit it so topic exists
        m.apply(cbuf.data(),  csz,  0);
        m.commit(0);
        // Queue 3 assignment records without committing each individually
        m.apply(ubuf0.data(), usz0, 1);
        m.apply(ubuf1.data(), usz1, 2);
        m.apply(ubuf2.data(), usz2, 3);
        // Single commit flushes all 3 pending entries at once
        m.commit(3);
    };

    MetadataStateMachine m1, m2;
    applyAllThenCommit(m1);
    applyAllThenCommit(m2);

    // Whole-map hash equality: same result regardless of internal slot order
    EXPECT_EQ(hashMap(m1), hashMap(m2));

    // Verify all 3 partitions are visible
    ASSERT_NE(m1.assignment(1, 0), nullptr);
    ASSERT_NE(m1.assignment(1, 1), nullptr);
    ASSERT_NE(m1.assignment(1, 2), nullptr);
    EXPECT_EQ(m1.assignment(1, 0)->leaderNode, 10);
    EXPECT_EQ(m1.assignment(1, 1)->leaderNode, 11);
    EXPECT_EQ(m1.assignment(1, 2)->leaderNode, 12);
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

// Case A2: electedLeader detected when ONLY leaderEpoch bumped, partitionEpoch flat.
// This validates the MINOR fix: the early-return gate must be
// (leaderEpochBumped || partitionEpochBumped), not just partitionEpochBumped.
// A controller may bump leaderEpoch without bumping partitionEpoch (e.g. leader
// re-registered at the same partition epoch after a node restart).
TEST(LocalRoleDelta, ElectedLeader_OnlyLeaderEpochBumped)
{
    // prev: node 1 is leader, le=1, pe=1
    AssignmentMap prev = singleEntry(1, 0, PartitionAssignment{1, {1,2}, {1,2}, 1, 1});

    // current: leader=1, le=2 (bumped), pe=1 (UNCHANGED)
    MetadataStateMachine msm;
    auto [cbuf, csz] = encodeCreate(1, 1, 2);
    msm.apply(cbuf.data(), csz, 0);
    msm.commit(0);
    auto [ubuf, usz] = encodeUpdate({1, 0, 1, {1,2}, {1,2}, 2, 1}); // le=2, pe=1
    msm.apply(ubuf.data(), usz, 1);
    msm.commit(1);

    LocalRoleDelta delta = msm.deriveLocalDelta(/*selfNode=*/1, prev);

    // leaderEpoch advanced → electedLeaders even though partitionEpoch did not change
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
// 8. Structural sharing: applying one topic's change does not affect another.
//
//    We assert POINTER IDENTITY — the PartitionAssignment* for topic 1 must
//    remain the same object after a topic-2 update.  A full-rebuild-every-apply
//    implementation would return a different pointer; our open-addressing map
//    returns a stable slot pointer as long as no rehash occurs.
//
//    Note: this test assumes no rehash of the committed_ map occurs within the
//    window between capturing the pointer and the final assertion.  With only
//    2 topics (2 partition entries) in an INITIAL_CAP=16 table, load factor
//    stays well below 0.60, so no rehash will occur.
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

    // Capture POINTER to topic-1's PartitionAssignment slot BEFORE topic-2 update.
    // A full-rebuild-every-apply impl would return a different pointer after any
    // put(); pointer stability proves no rebuild or rehash affected topic 1's slot.
    const PartitionAssignment* slotBefore = msm.assignment(1, 0);
    ASSERT_NE(slotBefore, nullptr);

    // Update topic 2's partition — must not touch topic 1's slot
    auto [ub2b, us2b] = encodeUpdate({2, 0, 21, {21}, {20,21}, 2, 2});
    msm.apply(ub2b.data(), us2b, 4);
    msm.commit(4);

    // Topic 1's pointer must be the SAME object (stable slot identity)
    const PartitionAssignment* slotAfter = msm.assignment(1, 0);
    ASSERT_NE(slotAfter, nullptr);
    EXPECT_EQ(slotAfter, slotBefore); // pointer identity: no rebuild/rehash in window
    // Field values must also be unchanged
    EXPECT_EQ(slotAfter->leaderNode,     10);
    EXPECT_EQ(slotAfter->partitionEpoch, 1);
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

// ─────────────────────────────────────────────────────────────────────────────
// 15. MVCC revert — discard uncommitted speculation, committed read unchanged
// ─────────────────────────────────────────────────────────────────────────────

TEST(MetadataStateMachine, RevertDiscardsUncommittedPending) {
    MetadataStateMachine msm;

    // Apply two UpdatePartitionAssignmentRecords at pos 0 and pos 1.
    // Only pos 0 is committed.
    auto [u0buf, u0sz] = encodeUpdate({10, 0, 5, {5}, {5}, 1, 1});
    auto [u1buf, u1sz] = encodeUpdate({10, 1, 6, {6}, {6}, 1, 1});

    msm.apply(u0buf.data(), u0sz, /*pos=*/0);
    msm.apply(u1buf.data(), u1sz, /*pos=*/1);

    // Commit only pos 0 — pos 1 is uncommitted (pending)
    msm.commit(0);

    // partition 0 must be committed and visible
    ASSERT_NE(msm.assignment(10, 0), nullptr);
    EXPECT_EQ(msm.assignment(10, 0)->leaderNode, 5);

    // partition 1 must NOT be visible (uncommitted speculation)
    EXPECT_EQ(msm.assignment(10, 1), nullptr);

    // revert(0): discard pending records with pos > 0
    msm.revert(0);

    // committed partition 0 must remain untouched
    ASSERT_NE(msm.assignment(10, 0), nullptr);
    EXPECT_EQ(msm.assignment(10, 0)->leaderNode, 5);

    // partition 1 must still be absent (reverted)
    EXPECT_EQ(msm.assignment(10, 1), nullptr);

    // a later commit(1) must NOT resurrect the reverted pending entry
    msm.commit(1);
    EXPECT_EQ(msm.assignment(10, 1), nullptr)
        << "reverted pending record must not be resurrected by a later commit";
}

// ─────────────────────────────────────────────────────────────────────────────
// 16. handleLoss — unknown record version increments lossCount, does not crash
// ─────────────────────────────────────────────────────────────────────────────

TEST(MetadataStateMachine, UnknownRecordVersionIsHandleLossNotCrash) {
    MetadataStateMachine msm;
    uint8_t buf[32]{}; // recordType=2 (UpdatePartitionAssignment), version=99
    buf[0]=2; buf[1]=0; buf[2]=99;
    msm.apply(buf, sizeof buf, /*pos=*/0);
    msm.commit(0);
    EXPECT_EQ(msm.lossCount(), 1u);
    EXPECT_EQ(msm.committedMap().size(), 0);
}

// Task 11: the runtime rebuilds the RoutingImage only when committedVersion()
// moves — so it must move on EVERY committed-map change and never otherwise.
TEST(MetadataStateMachineRegression, CommittedVersionMovesOnlyOnCommittedMapChange)
{
    MetadataStateMachine msm;
    const uint64_t v0 = msm.committedVersion();
    auto [cbuf, csz] = encodeCreate(1, 2, 1);
    msm.apply(cbuf.data(), csz, /*pos=*/0);
    auto [ubuf, usz] = encodeUpdate({1, 0, 7, {7}, {7}, 1, 1});
    msm.apply(ubuf.data(), usz, /*pos=*/5);
    EXPECT_EQ(msm.committedVersion(), v0) << "pending (uncommitted) records change nothing";
    msm.commit(5);
    const uint64_t v1 = msm.committedVersion();
    EXPECT_GT(v1, v0) << "commit installed an assignment";
    msm.commit(5);
    EXPECT_EQ(msm.committedVersion(), v1) << "idempotent commit: no change";
    auto [u2, u2sz] = encodeUpdate({1, 1, 3, {3}, {3}, 1, 1});
    msm.apply(u2.data(), u2sz, /*pos=*/3);                    // at/below committedPos: installs directly
    const uint64_t v2 = msm.committedVersion();
    EXPECT_GT(v2, v1);
    VecSnapshotWriter w;
    msm.snapshot(w);
    VecSnapshotReader r(w.data());
    msm.restore(r);
    EXPECT_GT(msm.committedVersion(), v2) << "restore replaces the committed map";
}

// Epoch-restart fix round 2 (N4): the MSM never installs an assignment record
// that moves a partition BACK — a lower leaderEpoch, the same leaderEpoch with
// a different leader, or a lower partitionEpoch — whether it arrives committed
// at once, through the pending queue, or in a replay of the log.
TEST(MetadataStateMachineRegression, EpochRegressionRecordsIgnoredInApplyAndReplayRegression)
{
    const std::vector<UpdateArgs> log = {
        {1, 0, 5, {5, 6}, {5, 6}, 3, 4},   // installed
        {1, 0, 6, {6, 5}, {5, 6}, 2, 9},   // lower leaderEpoch            → ignored
        {1, 0, 6, {6},    {5, 6}, 3, 9},   // same epoch, other leader     → ignored
        {1, 0, 5, {5},    {5, 6}, 3, 3},   // lower partitionEpoch         → ignored
        {1, 0, 5, {5},    {5, 6}, 3, 5},   // ISR change (pe + 1)          → installed
        {1, 0, 6, {6, 5}, {5, 6}, 4, 6},   // new election (le + 1)        → installed
        {1, 0, 5, {5, 6}, {5, 6}, 0, 0},   // a static epoch-0 re-emit      → ignored
    };
    auto check = [](const MetadataStateMachine& msm, const char* how) {
        const auto* a = msm.assignment(1, 0);
        ASSERT_NE(a, nullptr) << how;
        EXPECT_EQ(a->leaderNode, 6) << how;
        EXPECT_EQ(a->leaderEpoch, 4) << how;
        EXPECT_EQ(a->partitionEpoch, 6) << how;
        EXPECT_EQ(a->isr, (std::vector<int32_t>{6, 5})) << how;
        EXPECT_EQ(msm.regressionsRejected(), 4u) << how;
    };
    {   // committed one by one
        MetadataStateMachine msm;
        for (std::size_t i = 0; i < log.size(); ++i) {
            auto [b, n] = encodeUpdate(log[i]);
            msm.apply(b.data(), n, static_cast<int64_t>(i));
            msm.commit(static_cast<int64_t>(i));
        }
        check(msm, "applied + committed one by one");
    }
    {   // replay: everything pending, committed in one step
        MetadataStateMachine msm;
        for (std::size_t i = 0; i < log.size(); ++i) {
            auto [b, n] = encodeUpdate(log[i]);
            msm.apply(b.data(), n, static_cast<int64_t>(i));
        }
        msm.commit(static_cast<int64_t>(log.size()));
        check(msm, "replayed (pending, one commit)");
    }
}
