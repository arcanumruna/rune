#include <gtest/gtest.h>
#include <rune/gateway/TopicRegistry.h>
#include <rune/node/NodeConfig.h>
#include <rune/protocol/RecordBatch.h>

#include <atomic>
#include <cstdint>
#include <filesystem>
#include <unistd.h>
#include <vector>

namespace fs = std::filesystem;
using rune::gateway::TopicRegistry;
using rune::node::TopicSpec;
using rune::storage::Log;

// ---------------------------------------------------------------------------
// Unique temp-dir helper (same pattern as TopicRegistryTest.cpp)
// ---------------------------------------------------------------------------
static fs::path makeTempDir(const char* stem)
{
    static std::atomic<uint64_t> g_seq{0};
    auto id = std::to_string(::getpid()) + "_" +
              std::to_string(g_seq.fetch_add(1, std::memory_order_relaxed));
    // A reused pid could meet a previous run's dir (these tests never remove
    // theirs): start from an empty one, so a recovery assertion never reads stale data.
    auto p = fs::temp_directory_path() / (std::string("rune_tr_rt_") + stem + "_" + id);
    std::error_code ec;
    fs::remove_all(p, ec);
    return p;
}

// ---------------------------------------------------------------------------
// TopicRegistryRuntime — topics created after boot.  Catalog history Task 9:
// the runtime createTopic (sequential ids) is gone; every post-boot topic is
// built at its CATALOG id (createTopicAt).  Same scenarios, catalog ids.
// ---------------------------------------------------------------------------

// Task 5: runtime topic creation creates partition dirs and a recoverable log.
TEST(TopicRegistryRuntime, CreateTopicMakesProducibleTopic) {
    fs::path dir = makeTempDir("create");
    fs::create_directories(dir);

    // Start with an empty registry (no boot-time topics).
    TopicRegistry reg(std::vector<TopicSpec>{}, dir.string());

    TopicSpec spec;
    spec.name           = "characters";
    spec.topicId        = 0x40000000;  // caller hint; registry assigns sequentially
    spec.partitionCount = 2;

    const int32_t id = reg.createTopicAt(0, spec);
    // The topic lives at exactly its catalog id (spec.topicId is ignored).
    EXPECT_EQ(id, 0);

    // Partition log directories now exist under dataDir/topics/<name>/<partition>/
    EXPECT_TRUE(fs::exists(dir / "topics" / "characters" / "0"));
    EXPECT_TRUE(fs::exists(dir / "topics" / "characters" / "1"));

    // The topic is accessible via standard accessors.
    EXPECT_TRUE(reg.topicIdForName("characters").has_value());
    EXPECT_EQ(reg.topicIdForName("characters").value(), 0);
    EXPECT_NE(reg.log(0, 0), nullptr);
    EXPECT_NE(reg.log(0, 1), nullptr);
    EXPECT_EQ(reg.topicCount(), 1);

    // Idempotent: creating again returns the same id, no throw, no double-create.
    EXPECT_EQ(reg.createTopicAt(0, spec), 0);
    EXPECT_EQ(reg.topicCount(), 1);
}

// Task 5: a catalog topic after the boot-time (static) topics, at its catalog id.
TEST(TopicRegistryRuntime, CreateTopicAfterBootTopicsAssignsSequentialId) {
    fs::path dir = makeTempDir("seq");
    fs::create_directories(dir);

    TopicSpec boot;
    boot.name           = "events";
    boot.topicId        = 0;  // boot-time id (parsed by NodeConfig)
    boot.partitionCount = 4;

    TopicRegistry reg(std::vector<TopicSpec>{boot}, dir.string());
    EXPECT_EQ(reg.topicCount(), 1);

    TopicSpec runtime;
    runtime.name           = "characters";
    runtime.partitionCount = 2;
    runtime.topicId        = 0x40000000;  // hint ignored by registry

    EXPECT_EQ(reg.createTopicAt(0, runtime), -1) << "a static topic's id is never a catalog topic's";
    const int32_t id = reg.createTopicAt(1, runtime);
    EXPECT_EQ(id, 1);  // second topic → id 1
    EXPECT_EQ(reg.topicCount(), 2);

    // Both topics accessible.
    EXPECT_EQ(reg.topicIdForName("events").value(),     0);
    EXPECT_EQ(reg.topicIdForName("characters").value(), 1);
    EXPECT_NE(reg.log(1, 0), nullptr);
    EXPECT_NE(reg.log(1, 1), nullptr);

    // Partition dirs for the runtime topic exist.
    EXPECT_TRUE(fs::exists(dir / "topics" / "characters" / "0"));
    EXPECT_TRUE(fs::exists(dir / "topics" / "characters" / "1"));
}

// Task 5: createTopicAt is idempotent for the same id + name; the name at another
// live id is refused.
TEST(TopicRegistryRuntime, CreateTopicIdempotentByName) {
    fs::path dir = makeTempDir("idem");
    fs::create_directories(dir);

    TopicRegistry reg(std::vector<TopicSpec>{}, dir.string());

    TopicSpec spec;
    spec.name           = "orders";
    spec.partitionCount = 1;

    const int32_t first  = reg.createTopicAt(0, spec);
    const int32_t second = reg.createTopicAt(0, spec);
    const int32_t third  = reg.createTopicAt(1, spec);

    EXPECT_EQ(first, 0);
    EXPECT_EQ(second, 0);
    EXPECT_EQ(third, -1) << "the name is held by live id 0";
    EXPECT_EQ(reg.topicCount(), 1);
}

// Task 5: multiple distinct catalog topics, each at its own id (a gap below an id
// is a Gap slot: unknown here, fillable).
TEST(TopicRegistryRuntime, CreateMultipleTopicsUniqueIds) {
    fs::path dir = makeTempDir("multi");
    fs::create_directories(dir);

    TopicRegistry reg(std::vector<TopicSpec>{}, dir.string());

    auto makeSpec = [](std::string name, int32_t pc) {
        TopicSpec ts;
        ts.name           = std::move(name);
        ts.partitionCount = pc;
        return ts;
    };

    const int32_t id0 = reg.createTopicAt(0, makeSpec("inventory", 8));
    const int32_t id2 = reg.createTopicAt(2, makeSpec("entities",  4));
    EXPECT_EQ(reg.state(1), TopicRegistry::TopicState::Unknown) << "a Gap: never seen here";
    const int32_t id1 = reg.createTopicAt(1, makeSpec("chat",      1));

    EXPECT_EQ(id0, 0);
    EXPECT_EQ(id1, 1) << "a Gap is fillable";
    EXPECT_EQ(id2, 2);
    EXPECT_EQ(reg.topicCount(), 3);

    // All 8 inventory partitions exist.
    for (int p = 0; p < 8; ++p) {
        EXPECT_TRUE(fs::exists(dir / "topics" / "inventory" / std::to_string(p)))
            << "missing partition " << p;
    }
}

// Task 5 Regression: boot-time constructor still works after the createTopic refactor.
// This is the same coverage as TopicRegistryTest::SingleTopicSinglePartition
// but explicitly tagged *Regression* to run in the regression suite.
TEST(TopicRegistryRuntimeRegression, BootPathUnchangedAfterRefactor) {
    fs::path dir = makeTempDir("boot_reg");
    fs::create_directories(dir);

    TopicSpec ts;
    ts.name           = "chat";
    ts.partitionCount = 1;
    ts.topicId        = 0;

    TopicRegistry reg({ts}, dir.string());
    EXPECT_EQ(reg.topicCount(), 1);
    EXPECT_TRUE(reg.topicIdForName("chat").has_value());
    EXPECT_EQ(reg.topicIdForName("chat").value(), 0);
    EXPECT_NE(reg.log(0, 0), nullptr);
    EXPECT_TRUE(fs::exists(dir / "topics" / "chat" / "0"));
}

// Task 5 Regression: createTopic recover — log end-offset persists across a
// rebuild of the registry on the same dataDir.
TEST(TopicRegistryRuntimeRegression, CreateTopicLogRecoverAcrossRebuild) {
    fs::path dir = makeTempDir("recover_rt");
    fs::create_directories(dir);

    TopicSpec ts;
    ts.name           = "stream";
    ts.partitionCount = 1;

    int64_t leoAfterAppend = 0;
    {
        TopicRegistry reg({}, dir.string());
        const int32_t id = reg.createTopicAt(0, ts);
        ASSERT_EQ(id, 0);

        Log* l = reg.log(0, 0);
        ASSERT_NE(l, nullptr);

        rune::protocol::RecordBatchBuilder b(0, 1000);
        b.append("k", "v", 1000);
        auto batch = b.build();
        l->append(batch, 0);
        leoAfterAppend = l->logEndOffset();
        EXPECT_GT(leoAfterAppend, 0);
    }

    // Rebuild on the same dir — createTopicAt must recover the existing log
    // (a restart re-runs the effect: an existing dir is recovered, never wiped).
    {
        TopicRegistry reg({}, dir.string());
        const int32_t id = reg.createTopicAt(0, ts);
        ASSERT_EQ(id, 0);

        Log* l = reg.log(0, 0);
        ASSERT_NE(l, nullptr);
        EXPECT_EQ(l->logEndOffset(), leoAfterAppend)
            << "logEndOffset not recovered across a createTopicAt rebuild";
    }
}

// ---------------------------------------------------------------------------
// Catalog history Task 9 (fix-on-sight, Task 8 N1, hard rule 3): a slot is ONE
// state holding exactly its variables — Static / Serving {spec, partitions},
// Pending {name}, Gap {}, Dropped {spec, held logs} — never a tag over one struct.
// Each state answers its own way; a transition rebuilds the slot.
// ---------------------------------------------------------------------------
TEST(TopicRegistrySlotRegression, EverySlotStateAnswersExactlyItsOwnWayRegression) {
    fs::path dir = makeTempDir("slots");
    fs::create_directories(dir);
    TopicSpec boot;
    boot.name = "static"; boot.partitionCount = 1;
    TopicRegistry reg({boot}, dir.string());
    using St = TopicRegistry::TopicState;

    // Static: serves, not placement-gated.
    EXPECT_EQ(reg.state(0), St::Serving);
    EXPECT_FALSE(reg.placementGated(0));
    EXPECT_NE(reg.log(0, 0), nullptr);

    // Pending {name}: no spec, no logs, not served — and only its own name fills it.
    ASSERT_TRUE(reg.reserveTopicAt(2, "waiting"));
    EXPECT_EQ(reg.state(2), St::Pending);
    EXPECT_EQ(reg.topic(2), nullptr);
    EXPECT_EQ(reg.log(2, 0), nullptr);
    EXPECT_EQ(reg.state(1), St::Unknown) << "the id below is a Gap";
    TopicSpec other; other.name = "other"; other.partitionCount = 1;
    EXPECT_EQ(reg.createTopicAt(2, other), -1) << "reserved for 'waiting'";
    TopicSpec waiting; waiting.name = "waiting"; waiting.partitionCount = 2;
    waiting.cleanupPolicy = rune::node::TopicSpec::CleanupPolicy::Compact;
    ASSERT_EQ(reg.createTopicAt(2, waiting), 2);
    EXPECT_EQ(reg.state(2), St::Serving);
    EXPECT_TRUE(reg.placementGated(2)) << "a catalog topic serves once placed";
    EXPECT_EQ(reg.heldLogCountForTest(2), 2u);

    // Dropped {spec, logs}: serves nothing, its config still answers, never refilled.
    ASSERT_TRUE(reg.dropTopic(2));
    EXPECT_EQ(reg.state(2), St::Dropped);
    EXPECT_EQ(reg.topic(2), nullptr);
    EXPECT_EQ(reg.log(2, 0), nullptr);
    EXPECT_FALSE(reg.topicIdForName("waiting").has_value()) << "the name is free again";
    EXPECT_EQ(reg.cleanupPolicyOf(2), rune::node::TopicSpec::CleanupPolicy::Compact);
    EXPECT_EQ(reg.heldLogCountForTest(2), 2u) << "logs held until released";
    EXPECT_TRUE(reg.releaseDroppedLogs(2));
    EXPECT_EQ(reg.heldLogCountForTest(2), 0u);
    EXPECT_EQ(reg.createTopicAt(2, waiting), -1) << "a dropped catalog id is never re-created";

    // A static topic's id is never taken by a catalog create, even of its own name.
    EXPECT_EQ(reg.createTopicAt(0, boot), -1);
    // The Gap at 1 is fillable; a dropped Gap / Pending becomes Dropped.
    ASSERT_TRUE(reg.dropTopic(1));
    EXPECT_EQ(reg.state(1), St::Dropped);
}

// Task 11 fix round 3 (a): a broker whose catalog apply halted BEFORE a topic's
// create (e.g. mid boot replay) never builds it in its registry — yet the data
// is on its disk.  The topic dir names its catalog id, so the broker reports
// itself a HOLDER (never removed) instead of "no local data".  RED (round 2):
// such a broker was reported unbuildable and could be removed as the sole
// in-sync replica, losing the committed data.
TEST(TopicRegistryRuntimeRegression, OnDiskTopicDataIsFoundWithoutTheCatalogHavingAppliedItRegression) {
    const fs::path dir = makeTempDir("holder");
    {
        TopicRegistry reg({}, dir.string());
        TopicSpec s; s.name = "orders"; s.partitionCount = 2;
        ASSERT_EQ(reg.createTopicAt(7, s), 7);
    }
    EXPECT_TRUE(fs::exists(dir / "topics" / "orders" / TopicRegistry::kTopicIdMarker));
    // A fresh process whose catalog never applied 'orders': the registry knows nothing …
    TopicRegistry fresh({}, dir.string());
    EXPECT_EQ(fresh.state(7), TopicRegistry::TopicState::Unknown);
    // … the disk does.
    const auto disk = TopicRegistry::scanLocalTopicData(dir.string());
    EXPECT_TRUE(disk.holds(7, 0));
    EXPECT_TRUE(disk.holds(7, 1));
    EXPECT_FALSE(disk.holds(7, 2)) << "no such partition dir";
    EXPECT_FALSE(disk.holds(8, 0)) << "another topic: no local data";
    EXPECT_FALSE(disk.unmarkedDirs);
    // A topic dir without a readable marker: conservatively a holder of anything.
    fs::create_directories(dir / "topics" / "legacy" / "0");
    const auto conservative = TopicRegistry::scanLocalTopicData(dir.string());
    EXPECT_TRUE(conservative.unmarkedDirs);
    EXPECT_TRUE(conservative.holds(8, 0)) << "never 'no data' when unsure";
    std::error_code ec;
    fs::remove_all(dir, ec);
}
