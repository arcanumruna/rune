#include <gtest/gtest.h>
#include <rune/node/NamespacePaths.h>
#include <rune/gateway/TopicRegistry.h>
#include <rune/node/NodeConfig.h>
#include <rune/protocol/RecordBatch.h>

#include <atomic>
#include <cstdint>
#include <filesystem>
#include <span>
#include <string_view>
#include <unistd.h>
#include <vector>

using rune::gateway::TopicRegistry;
using rune::node::TopicSpec;
using rune::storage::Log;

// ---------------------------------------------------------------------------
// Unique temp-dir helper (getpid() + atomic counter)
// ---------------------------------------------------------------------------
static std::filesystem::path makeTempDir(const char* stem)
{
    static std::atomic<uint64_t> g_seq{0};
    auto id = std::to_string(::getpid()) + "_" +
              std::to_string(g_seq.fetch_add(1, std::memory_order_relaxed));
    // A reused pid could meet a previous run's dir (these tests never remove
    // theirs): start from an empty one, so a recovery assertion never reads stale data.
    auto p = std::filesystem::temp_directory_path() / (std::string("rune_tr_") + stem + "_" + id);
    std::error_code ec;
    std::filesystem::remove_all(p, ec);
    return p;
}

// Helper: build a TopicSpec.
static TopicSpec makeSpec(std::string name, int32_t pc, int32_t id) {
    TopicSpec ts;
    ts.name           = std::move(name);
    ts.partitionCount = pc;
    ts.topicId        = id;
    return ts;
}

// Helper: a minimal one-record RecordBatch.
static std::vector<uint8_t> makeBatch(int64_t baseOffset = 0) {
    rune::protocol::RecordBatchBuilder b(baseOffset, 1000);
    b.append("key", "val", 1000);
    return b.build();
}

// ---------------------------------------------------------------------------
// Construction tests
// ---------------------------------------------------------------------------

TEST(TopicRegistry, EmptyTopicsConstructsOk) {
    auto dir = makeTempDir("empty");
    std::filesystem::create_directories(dir);
    TopicRegistry reg({}, dir.string());
    EXPECT_EQ(reg.topicCount(), 0);
}

TEST(TopicRegistry, SingleTopicSinglePartition) {
    auto dir = makeTempDir("single");
    std::filesystem::create_directories(dir);

    std::vector<TopicSpec> specs = {makeSpec("chat", 1, 0)};
    TopicRegistry reg(specs, dir.string());

    EXPECT_EQ(reg.topicCount(), 1);

    // topicIdForName
    auto id = reg.topicIdForName("chat");
    ASSERT_TRUE(id.has_value());
    EXPECT_EQ(*id, 0);

    // topic spec
    const TopicSpec* ts = reg.topic(0);
    ASSERT_NE(ts, nullptr);
    EXPECT_EQ(ts->name,           "chat");
    EXPECT_EQ(ts->partitionCount, 1);
    EXPECT_EQ(ts->topicId,        0);

    // log for partition 0
    Log* log = reg.log(0, 0);
    ASSERT_NE(log, nullptr);

    // log dir exists on disk
    auto expectedDir = rune::node::topicsRoot(dir, 0) / "chat" / "0";
    EXPECT_TRUE(std::filesystem::exists(expectedDir));
}

TEST(TopicRegistry, MultiTopicMultiPartition) {
    auto dir = makeTempDir("multi");
    std::filesystem::create_directories(dir);

    std::vector<TopicSpec> specs = {
        makeSpec("inventory", 8, 0),
        makeSpec("entities",  4, 1),
        makeSpec("chat",      1, 2),
    };
    TopicRegistry reg(specs, dir.string());

    EXPECT_EQ(reg.topicCount(), 3);

    // inventory: 8 partitions
    for (int32_t p = 0; p < 8; ++p) {
        Log* l = reg.log(0, p);
        ASSERT_NE(l, nullptr) << "partition " << p;
        auto d = rune::node::topicsRoot(dir, 0) / "inventory" / std::to_string(p);
        EXPECT_TRUE(std::filesystem::exists(d)) << "missing dir for partition " << p;
    }

    // entities: 4 partitions
    for (int32_t p = 0; p < 4; ++p) {
        EXPECT_NE(reg.log(1, p), nullptr) << "entities partition " << p;
    }

    // chat: 1 partition
    EXPECT_NE(reg.log(2, 0), nullptr);
}

// ---------------------------------------------------------------------------
// topicIdForName lookups
// ---------------------------------------------------------------------------

TEST(TopicRegistry, TopicIdForNameFound) {
    auto dir = makeTempDir("find");
    std::filesystem::create_directories(dir);
    std::vector<TopicSpec> specs = {makeSpec("events", 4, 0)};
    TopicRegistry reg(specs, dir.string());

    EXPECT_EQ(reg.topicIdForName("events"), std::optional<int32_t>{0});
}

TEST(TopicRegistry, TopicIdForNameNotFound) {
    auto dir = makeTempDir("notfound");
    std::filesystem::create_directories(dir);
    TopicRegistry reg({}, dir.string());

    EXPECT_FALSE(reg.topicIdForName("missing").has_value());
}

// ---------------------------------------------------------------------------
// Bounds-checked lookups
// ---------------------------------------------------------------------------

TEST(TopicRegistry, LogOutOfRangeTopicIdReturnsNull) {
    auto dir = makeTempDir("oob_tid");
    std::filesystem::create_directories(dir);
    std::vector<TopicSpec> specs = {makeSpec("chat", 1, 0)};
    TopicRegistry reg(specs, dir.string());

    EXPECT_EQ(reg.log(-1, 0), nullptr);
    EXPECT_EQ(reg.log(1, 0),  nullptr);  // topicId == count
    EXPECT_EQ(reg.log(99, 0), nullptr);
}

TEST(TopicRegistry, LogOutOfRangePartitionReturnsNull) {
    auto dir = makeTempDir("oob_part");
    std::filesystem::create_directories(dir);
    std::vector<TopicSpec> specs = {makeSpec("chat", 2, 0)};
    TopicRegistry reg(specs, dir.string());

    EXPECT_EQ(reg.log(0, -1), nullptr);
    EXPECT_EQ(reg.log(0, 2),  nullptr);  // partition == count
    EXPECT_EQ(reg.log(0, 99), nullptr);
}

TEST(TopicRegistry, TopicOutOfRangeReturnsNull) {
    auto dir = makeTempDir("oob_topic");
    std::filesystem::create_directories(dir);
    TopicRegistry reg({}, dir.string());

    EXPECT_EQ(reg.topic(-1), nullptr);
    EXPECT_EQ(reg.topic(0),  nullptr);  // empty registry
}

// ---------------------------------------------------------------------------
// partitionForKey
// ---------------------------------------------------------------------------

TEST(TopicRegistry, PartitionForKeyInRange) {
    auto dir = makeTempDir("pfk");
    std::filesystem::create_directories(dir);
    std::vector<TopicSpec> specs = {makeSpec("events", 8, 0)};
    TopicRegistry reg(specs, dir.string());

    // "key" FNV-1a = 0x3dc94a19365b10ec, % 8 = 4
    std::string_view k = "key";
    std::span<const uint8_t> key{
        reinterpret_cast<const uint8_t*>(k.data()), k.size()};

    int32_t p = reg.partitionForKey(0, key);
    EXPECT_EQ(p, 4);
    EXPECT_GE(p, 0);
    EXPECT_LT(p, 8);
}

TEST(TopicRegistry, PartitionForKeySinglePartitionAlwaysZero) {
    auto dir = makeTempDir("pfk1");
    std::filesystem::create_directories(dir);
    std::vector<TopicSpec> specs = {makeSpec("chat", 1, 0)};
    TopicRegistry reg(specs, dir.string());

    for (std::string_view sv : {"key", "inventory", "hello", "abc", "rune"}) {
        std::span<const uint8_t> key{
            reinterpret_cast<const uint8_t*>(sv.data()), sv.size()};
        EXPECT_EQ(reg.partitionForKey(0, key), 0) << "key=" << sv;
    }
}

TEST(TopicRegistry, PartitionForKeyStable) {
    auto dir = makeTempDir("pfk_stable");
    std::filesystem::create_directories(dir);
    std::vector<TopicSpec> specs = {makeSpec("events", 4, 0)};
    TopicRegistry reg(specs, dir.string());

    std::string_view k = "stable_key";
    std::span<const uint8_t> key{
        reinterpret_cast<const uint8_t*>(k.data()), k.size()};

    int32_t first = reg.partitionForKey(0, key);
    EXPECT_GE(first, 0);
    EXPECT_LT(first, 4);
    for (int i = 0; i < 50; ++i) {
        EXPECT_EQ(reg.partitionForKey(0, key), first);
    }
}

TEST(TopicRegistry, PartitionForKeyUnknownTopicIdReturnsNegOne) {
    auto dir = makeTempDir("pfk_unk");
    std::filesystem::create_directories(dir);
    TopicRegistry reg({}, dir.string());

    std::string_view k = "key";
    std::span<const uint8_t> key{
        reinterpret_cast<const uint8_t*>(k.data()), k.size()};

    EXPECT_EQ(reg.partitionForKey(0, key), -1);
    EXPECT_EQ(reg.partitionForKey(-1, key), -1);
}

TEST(TopicRegistry, PartitionForKeyUniformSanity) {
    // Over 10000 integer keys on 8 partitions, no partition should be empty.
    auto dir = makeTempDir("pfk_uni");
    std::filesystem::create_directories(dir);
    constexpr int kPart = 8;
    std::vector<TopicSpec> specs = {makeSpec("uniform", kPart, 0)};
    TopicRegistry reg(specs, dir.string());

    std::array<int, kPart> counts{};
    for (int i = 0; i < 10000; ++i) {
        uint8_t buf[4];
        buf[0] = static_cast<uint8_t>(i & 0xFF);
        buf[1] = static_cast<uint8_t>((i >> 8) & 0xFF);
        buf[2] = static_cast<uint8_t>((i >> 16) & 0xFF);
        buf[3] = static_cast<uint8_t>((i >> 24) & 0xFF);
        int32_t p = reg.partitionForKey(0, {buf, 4});
        ASSERT_GE(p, 0); ASSERT_LT(p, kPart);
        ++counts[p];
    }
    for (int i = 0; i < kPart; ++i) {
        EXPECT_GE(counts[i], 100) << "partition " << i << " too few keys: " << counts[i];
    }
}

// ---------------------------------------------------------------------------
// Recover-across-restart
// ---------------------------------------------------------------------------

TEST(TopicRegistry, RecoverAcrossRestart) {
    // Append a batch via the registry, destroy + rebuild on the same dataDir,
    // verify logEndOffset() > 0 (LEO persisted across restart).
    auto dir = makeTempDir("recover");
    std::filesystem::create_directories(dir);

    std::vector<TopicSpec> specs = {makeSpec("events", 1, 0)};

    int64_t leoAfterAppend = 0;
    {
        TopicRegistry reg(specs, dir.string());
        Log* l = reg.log(0, 0);
        ASSERT_NE(l, nullptr);

        // LEO starts at 0 on a fresh log.
        EXPECT_EQ(l->logEndOffset(), 0);

        // Append one batch (baseOffset=0, lastOffset=0).
        auto batch = makeBatch(0);
        int64_t nextOff = l->append(batch, 0);
        EXPECT_GT(nextOff, 0);
        leoAfterAppend = l->logEndOffset();
        EXPECT_GT(leoAfterAppend, 0);
    }
    // reg destroyed — files flushed to disk.

    // Rebuild on the same dataDir.
    {
        TopicRegistry reg2(specs, dir.string());
        Log* l = reg2.log(0, 0);
        ASSERT_NE(l, nullptr);

        // LEO must be restored to the value before destruction.
        EXPECT_EQ(l->logEndOffset(), leoAfterAppend)
            << "logEndOffset not recovered across restart";
    }
}

TEST(TopicRegistry, RecoverAcrossRestartMultiPartition) {
    // Multi-partition recovery: append to partition 3 of 8, verify only that
    // partition has a non-zero LEO after rebuild.
    auto dir = makeTempDir("recover_multi");
    std::filesystem::create_directories(dir);

    constexpr int kPart = 8;
    constexpr int kTargetPart = 3;
    std::vector<TopicSpec> specs = {makeSpec("events", kPart, 0)};

    int64_t leoAfterAppend = 0;
    {
        TopicRegistry reg(specs, dir.string());
        Log* l = reg.log(0, kTargetPart);
        ASSERT_NE(l, nullptr);

        auto batch = makeBatch(0);
        l->append(batch, 0);
        leoAfterAppend = l->logEndOffset();
        EXPECT_GT(leoAfterAppend, 0);
    }

    {
        TopicRegistry reg2(specs, dir.string());

        // Only partition kTargetPart should have non-zero LEO.
        for (int p = 0; p < kPart; ++p) {
            Log* l = reg2.log(0, p);
            ASSERT_NE(l, nullptr) << "partition " << p;
            if (p == kTargetPart) {
                EXPECT_EQ(l->logEndOffset(), leoAfterAppend)
                    << "partition " << p << " LEO not recovered";
            } else {
                EXPECT_EQ(l->logEndOffset(), 0)
                    << "partition " << p << " unexpected non-zero LEO";
            }
        }
    }
}

// ---------------------------------------------------------------------------
// CP-1: cleanupPolicyOf / dirtyRatioThresholdOf / deleteRetentionMsOf
// ---------------------------------------------------------------------------

// Helper: build a TopicSpec with full retention fields.
static TopicSpec makeCompactSpec(std::string name, int32_t pc, int32_t id) {
    TopicSpec ts;
    ts.name                   = std::move(name);
    ts.partitionCount         = pc;
    ts.topicId                = id;
    ts.cleanupPolicy          = TopicSpec::CleanupPolicy::Compact;
    ts.retentionMs            = 3'600'000LL;
    ts.retentionBytes         = -1LL;
    ts.deleteRetentionMs      = 43'200'000LL;
    ts.minCleanableDirtyRatio = 0.3;
    return ts;
}

TEST(TopicRegistry, CP1_DefaultPolicyIsDelete) {
    // A topic registered with the default TopicSpec must report Delete policy.
    auto dir = makeTempDir("cp1_default");
    std::filesystem::create_directories(dir);

    std::vector<TopicSpec> specs = {makeSpec("chat", 1, 0)};
    TopicRegistry reg(specs, dir.string());

    EXPECT_EQ(reg.cleanupPolicyOf(0), TopicSpec::CleanupPolicy::Delete);
    EXPECT_DOUBLE_EQ(reg.dirtyRatioThresholdOf(0), 0.5);
    EXPECT_EQ(reg.deleteRetentionMsOf(0), 86'400'000LL);
}

TEST(TopicRegistry, CP1_CompactPolicyRoundTrips) {
    // A topic registered with Compact policy must return Compact from cleanupPolicyOf.
    auto dir = makeTempDir("cp1_compact");
    std::filesystem::create_directories(dir);

    std::vector<TopicSpec> specs = {makeCompactSpec("events", 4, 0)};
    TopicRegistry reg(specs, dir.string());

    EXPECT_EQ(reg.cleanupPolicyOf(0), TopicSpec::CleanupPolicy::Compact);
    EXPECT_DOUBLE_EQ(reg.dirtyRatioThresholdOf(0), 0.3);
    EXPECT_EQ(reg.deleteRetentionMsOf(0), 43'200'000LL);
}

TEST(TopicRegistry, CP1_MultiTopicIndependentPolicies) {
    // Two topics: one Delete, one Compact — policies must be independent.
    auto dir = makeTempDir("cp1_multi");
    std::filesystem::create_directories(dir);

    std::vector<TopicSpec> specs = {
        makeSpec("chat", 1, 0),           // Delete (default)
        makeCompactSpec("events", 4, 1),  // Compact
    };
    TopicRegistry reg(specs, dir.string());

    EXPECT_EQ(reg.cleanupPolicyOf(0), TopicSpec::CleanupPolicy::Delete);
    EXPECT_EQ(reg.cleanupPolicyOf(1), TopicSpec::CleanupPolicy::Compact);
    EXPECT_DOUBLE_EQ(reg.dirtyRatioThresholdOf(0), 0.5);
    EXPECT_DOUBLE_EQ(reg.dirtyRatioThresholdOf(1), 0.3);
    EXPECT_EQ(reg.deleteRetentionMsOf(0), 86'400'000LL);
    EXPECT_EQ(reg.deleteRetentionMsOf(1), 43'200'000LL);
}

TEST(TopicRegistry, CP1_OutOfRangeTopicIdReturnsSafeDefaults) {
    // Out-of-range topicId must return safe defaults, not crash.
    auto dir = makeTempDir("cp1_oor");
    std::filesystem::create_directories(dir);

    TopicRegistry reg({}, dir.string()); // empty registry

    EXPECT_EQ(reg.cleanupPolicyOf(0),   TopicSpec::CleanupPolicy::Delete);
    EXPECT_EQ(reg.cleanupPolicyOf(-1),  TopicSpec::CleanupPolicy::Delete);
    EXPECT_EQ(reg.cleanupPolicyOf(99),  TopicSpec::CleanupPolicy::Delete);
    EXPECT_DOUBLE_EQ(reg.dirtyRatioThresholdOf(0),  0.5);
    EXPECT_EQ(reg.deleteRetentionMsOf(0), 86'400'000LL);
}

// ---------------------------------------------------------------------------
// Phase 2b Part C regression: the broker resolves an EMPTY key (a request with
// partitionKnown=0 — produce / subscribe / GetViewRow) to partition 0, the
// same partition the client's Key{}.partition(n) picks.  Before, the broker
// hashed fnv1a64("") % 8 = 5; a client-only rule split a Key{} producer from a
// Key{} subscriber.  Both now use rune::protocol::partitionForKey's rule.
// ---------------------------------------------------------------------------
TEST(TopicRegistryRegression, EmptyKeyPartitionZero) {
    auto dir = makeTempDir("pfk_empty");
    std::filesystem::create_directories(dir);
    std::vector<TopicSpec> specs = {makeSpec("p1", 1, 0), makeSpec("p2", 2, 1),
                                    makeSpec("p8", 8, 2), makeSpec("p64", 64, 3)};
    TopicRegistry reg(specs, dir.string());

    const std::span<const uint8_t> empty{};
    for (int32_t id = 0; id < 4; ++id) {
        EXPECT_EQ(reg.partitionForKey(id, empty), 0) << "topicId=" << id;
    }
}
