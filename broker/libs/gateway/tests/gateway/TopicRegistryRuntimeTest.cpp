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
    return fs::temp_directory_path() /
           (std::string("rune_tr_rt_") + stem + "_" + id);
}

// ---------------------------------------------------------------------------
// TopicRegistryRuntime — createTopic tests
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

    const int32_t id = reg.createTopic(spec);
    // The registry assigns the next sequential id (0 — first topic in empty registry).
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
    EXPECT_EQ(reg.createTopic(spec), 0);
    EXPECT_EQ(reg.topicCount(), 1);
}

// Task 5: createTopic after boot-time topics assigns the next sequential id.
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

    const int32_t id = reg.createTopic(runtime);
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

// Task 5: createTopic is idempotent by name — repeated calls return the same id.
TEST(TopicRegistryRuntime, CreateTopicIdempotentByName) {
    fs::path dir = makeTempDir("idem");
    fs::create_directories(dir);

    TopicRegistry reg(std::vector<TopicSpec>{}, dir.string());

    TopicSpec spec;
    spec.name           = "orders";
    spec.partitionCount = 1;

    const int32_t first  = reg.createTopic(spec);
    const int32_t second = reg.createTopic(spec);
    const int32_t third  = reg.createTopic(spec);

    EXPECT_EQ(first, 0);
    EXPECT_EQ(second, 0);
    EXPECT_EQ(third, 0);
    EXPECT_EQ(reg.topicCount(), 1);
}

// Task 5: multiple distinct runtime topics each get unique sequential ids.
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

    const int32_t id0 = reg.createTopic(makeSpec("inventory", 8));
    const int32_t id1 = reg.createTopic(makeSpec("chat",      1));
    const int32_t id2 = reg.createTopic(makeSpec("entities",  4));

    EXPECT_EQ(id0, 0);
    EXPECT_EQ(id1, 1);
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
        const int32_t id = reg.createTopic(ts);
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

    // Rebuild on the same dir — createTopic must recover the existing log.
    {
        TopicRegistry reg({}, dir.string());
        const int32_t id = reg.createTopic(ts);
        ASSERT_EQ(id, 0);

        Log* l = reg.log(0, 0);
        ASSERT_NE(l, nullptr);
        EXPECT_EQ(l->logEndOffset(), leoAfterAppend)
            << "logEndOffset not recovered across createTopic rebuild";
    }
}
