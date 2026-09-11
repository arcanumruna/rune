#include <gtest/gtest.h>
#include <rune/storage/PartitionApi.h>
#include <filesystem>
#include <string>
#include <vector>
#include <atomic>
#include <chrono>
#include <unistd.h>
using namespace rune::storage;

// ---------------------------------------------------------------------------
// Helper: unique temp dir path per test
// ---------------------------------------------------------------------------
static std::filesystem::path makeTempDir(const char* stem) {
    static std::atomic<uint64_t> g_tempSeq{0};
    auto ts = std::chrono::steady_clock::now().time_since_epoch().count();
    auto uniq = std::to_string(::getpid()) + "_" +
                std::to_string(g_tempSeq.fetch_add(1, std::memory_order_relaxed)) + "_" +
                std::to_string(ts);
    return std::filesystem::temp_directory_path() / (std::string(stem) + "_" + uniq);
}

// ---- Test 1 (brief's canonical test): produce/consume + restart durability ----
TEST(PartitionApi, ProduceConsumeAndSurviveRestart) {
    auto dir = makeTempDir("rune_api_t1");
    std::filesystem::remove_all(dir); std::filesystem::create_directories(dir);
    {
        PartitionApi api(dir.string());
        EXPECT_EQ(api.produce("a", "1", 1000), 1);
        EXPECT_EQ(api.produce("b", "2", 1001), 2);
        EXPECT_EQ(api.produce("a", "3", 1002), 3);
        EXPECT_EQ(api.highWatermark(), 3);
        auto recs = api.consume(0);
        ASSERT_EQ(recs.size(), 3u);
        EXPECT_EQ(recs[0].value, "1"); EXPECT_EQ(recs[0].offset, 0);
        EXPECT_EQ(recs[1].value, "2"); EXPECT_EQ(recs[1].offset, 1);
        EXPECT_EQ(recs[2].value, "3"); EXPECT_EQ(recs[2].offset, 2);
        auto tail = api.consume(2);
        ASSERT_EQ(tail.size(), 1u); EXPECT_EQ(tail[0].value, "3");
    }
    { // reopen: data persisted
        PartitionApi api(dir.string());
        EXPECT_EQ(api.highWatermark(), 3);
        EXPECT_EQ(api.consume(0).size(), 3u);
    }
    std::filesystem::remove_all(dir);
}

// ---- Test 2: empty partition ----
TEST(PartitionApi, EmptyPartition) {
    auto dir = makeTempDir("rune_api_t2");
    std::filesystem::remove_all(dir); std::filesystem::create_directories(dir);
    {
        PartitionApi api(dir.string());
        EXPECT_EQ(api.highWatermark(), 0);
        auto recs = api.consume(0);
        EXPECT_TRUE(recs.empty());
    }
    std::filesystem::remove_all(dir);
}

// ---- Test 3: ordering & offsets for N=20 records ----
TEST(PartitionApi, OrderingAndOffsets) {
    auto dir = makeTempDir("rune_api_t3");
    std::filesystem::remove_all(dir); std::filesystem::create_directories(dir);
    {
        constexpr int N = 20;
        PartitionApi api(dir.string());
        for (int i = 0; i < N; ++i) {
            std::string key = "k" + std::to_string(i);
            std::string val = "val" + std::to_string(i);
            api.produce(key, val, 3000 + i);
        }
        EXPECT_EQ(api.highWatermark(), N);
        auto recs = api.consume(0);
        ASSERT_EQ(recs.size(), static_cast<size_t>(N));
        for (int i = 0; i < N; ++i) {
            EXPECT_EQ(recs[i].offset, i) << "offset mismatch at i=" << i;
            EXPECT_EQ(recs[i].value, "val" + std::to_string(i)) << "value mismatch at i=" << i;
            EXPECT_EQ(recs[i].key, "k" + std::to_string(i)) << "key mismatch at i=" << i;
            EXPECT_EQ(recs[i].timestamp, 3000 + i) << "timestamp mismatch at i=" << i;
        }
    }
    std::filesystem::remove_all(dir);
}

// ---- Test 4: consume from arbitrary offsets ----
TEST(PartitionApi, ConsumeFromArbitraryOffsets) {
    auto dir = makeTempDir("rune_api_t4");
    std::filesystem::remove_all(dir); std::filesystem::create_directories(dir);
    {
        constexpr int N = 10;
        PartitionApi api(dir.string());
        for (int i = 0; i < N; ++i)
            api.produce("k", "v" + std::to_string(i), 4000 + i);
        EXPECT_EQ(api.highWatermark(), N);

        // consume(0) returns all N
        EXPECT_EQ(api.consume(0).size(), static_cast<size_t>(N));

        // consume(k) returns N-k records with correct starting offset
        for (int k : {3, 7, 9}) {
            auto recs = api.consume(k);
            ASSERT_EQ(recs.size(), static_cast<size_t>(N - k)) << "k=" << k;
            EXPECT_EQ(recs[0].offset, k) << "k=" << k;
        }

        // consume(N) returns empty
        EXPECT_TRUE(api.consume(N).empty());

        // consume(N+5) returns empty
        EXPECT_TRUE(api.consume(N + 5).empty());
    }
    std::filesystem::remove_all(dir);
}

// ---- Test 5: values round-trip exactly (empty value, embedded NUL/high bytes) ----
TEST(PartitionApi, ValuesRoundTripExactly) {
    auto dir = makeTempDir("rune_api_t5");
    std::filesystem::remove_all(dir); std::filesystem::create_directories(dir);
    {
        PartitionApi api(dir.string());

        // Record 0: empty value
        api.produce("k0", "", 5000);

        // Record 1: value with embedded NUL and high bytes
        std::string tricky;
        tricky += '\x00';
        tricky += '\xDE';
        tricky += '\xAD';
        tricky += '\x00';
        tricky += '\xFF';
        api.produce("k1", tricky, 5001);

        // Record 2: normal value for sanity
        api.produce("k2", "hello", 5002);

        EXPECT_EQ(api.highWatermark(), 3);
        auto recs = api.consume(0);
        ASSERT_EQ(recs.size(), 3u);

        // empty value
        EXPECT_EQ(recs[0].value.size(), 0u);
        EXPECT_EQ(recs[0].key, "k0");

        // tricky value: size and content must match exactly
        ASSERT_EQ(recs[1].value.size(), tricky.size());
        EXPECT_EQ(recs[1].value, tricky);
        EXPECT_EQ(recs[1].key, "k1");

        // normal value
        EXPECT_EQ(recs[2].value, "hello");
    }
    std::filesystem::remove_all(dir);
}

// ---- Test 7: consume across many small maxBytes chunks (regression for return-vs-break bug) ----
TEST(PartitionApi, ConsumeAcrossManyReadChunks) {
    auto dir = makeTempDir("rune_api_t7");
    std::filesystem::remove_all(dir); std::filesystem::create_directories(dir);
    {
        constexpr int N = 500;
        PartitionApi api(dir.string());
        // Produce N records with deterministic, unique values.
        for (int i = 0; i < N; ++i) {
            std::string key = "key" + std::to_string(i);
            std::string val = "value_" + std::to_string(i) + "_x" + std::string(16, static_cast<char>('a' + (i % 26)));
            api.produce(key, val, 7000 + i);
        }
        EXPECT_EQ(api.highWatermark(), static_cast<int64_t>(N));

        // Rebuild expected values in the same order.
        std::vector<std::string> expectedKeys, expectedVals;
        expectedKeys.reserve(N);
        expectedVals.reserve(N);
        for (int i = 0; i < N; ++i) {
            expectedKeys.push_back("key" + std::to_string(i));
            expectedVals.push_back("value_" + std::to_string(i) + "_x" + std::string(16, static_cast<char>('a' + (i % 26))));
        }

        // Use maxBytes=4096 — equal to the default index interval — to stress both the
        // consume() multi-chunk loop and LogSegment::read()'s grow-and-retry window.
        // Before the read() fix, read(250, 4096) returned {} when the sparse index
        // pointed to a position more than 4096 bytes before offset 250's batch.
        constexpr int32_t kSmallMax = 4096;
        auto recs = api.consume(0, kSmallMax);
        ASSERT_EQ(static_cast<int>(recs.size()), N)
            << "consume(0, " << kSmallMax << ") returned " << recs.size() << " instead of " << N;
        for (int i = 0; i < N; ++i) {
            EXPECT_EQ(recs[static_cast<size_t>(i)].offset, static_cast<int64_t>(i))
                << "offset mismatch at i=" << i;
            EXPECT_EQ(recs[static_cast<size_t>(i)].key, expectedKeys[static_cast<size_t>(i)])
                << "key mismatch at i=" << i;
            EXPECT_EQ(recs[static_cast<size_t>(i)].value, expectedVals[static_cast<size_t>(i)])
                << "value mismatch at i=" << i;
        }

        // consume(250, kSmallMax) must return exactly records 250..499.
        auto tail = api.consume(250, kSmallMax);
        ASSERT_EQ(static_cast<int>(tail.size()), N - 250)
            << "consume(250, " << kSmallMax << ") returned " << tail.size() << " instead of " << (N - 250);
        for (int i = 0; i < N - 250; ++i) {
            int absIdx = 250 + i;
            EXPECT_EQ(tail[static_cast<size_t>(i)].offset, static_cast<int64_t>(absIdx))
                << "offset mismatch at absIdx=" << absIdx;
            EXPECT_EQ(tail[static_cast<size_t>(i)].value, expectedVals[static_cast<size_t>(absIdx)])
                << "value mismatch at absIdx=" << absIdx;
        }
    }
    std::filesystem::remove_all(dir);
}

// ---- Test 6: restart mid-stream (proves recover() sets logEndOffset correctly) ----
TEST(PartitionApi, RestartMidStream) {
    auto dir = makeTempDir("rune_api_t6");
    std::filesystem::remove_all(dir); std::filesystem::create_directories(dir);
    {
        PartitionApi api(dir.string());
        for (int i = 0; i < 10; ++i)
            api.produce("k", "v" + std::to_string(i), 6000 + i);
        EXPECT_EQ(api.highWatermark(), 10);
    }
    {
        // Reopen and produce 5 more
        PartitionApi api(dir.string());
        EXPECT_EQ(api.highWatermark(), 10);
        for (int i = 10; i < 15; ++i)
            api.produce("k", "v" + std::to_string(i), 6000 + i);
        EXPECT_EQ(api.highWatermark(), 15);

        auto recs = api.consume(0);
        ASSERT_EQ(recs.size(), 15u);
        // Contiguous offsets 0..14
        for (int i = 0; i < 15; ++i) {
            EXPECT_EQ(recs[i].offset, i) << "offset mismatch at i=" << i;
            EXPECT_EQ(recs[i].value, "v" + std::to_string(i)) << "value mismatch at i=" << i;
        }
    }
    std::filesystem::remove_all(dir);
}

// ---------------------------------------------------------------------------
// Part B — edge-case tests (full re-audit 2026-09-11)
// ---------------------------------------------------------------------------

// B18: consume() with fromOffset == highWatermark returns empty; fromOffset
// beyond highWatermark also returns empty without crashing.
TEST(PartitionApi, ConsumeAtOrPastHWMReturnsEmpty) {
    auto dir = makeTempDir("rune_api_b18");
    std::filesystem::remove_all(dir); std::filesystem::create_directories(dir);

    PartitionApi api(dir.string());
    api.produce("k", "v1", 1000);
    api.produce("k", "v2", 1001);
    EXPECT_EQ(api.highWatermark(), 2);

    // consume(2) = at HWM → empty.
    auto atHwm = api.consume(2);
    EXPECT_TRUE(atHwm.empty()) << "consume at HWM must return empty";

    // consume(100) = beyond HWM → empty.
    auto pastHwm = api.consume(100);
    EXPECT_TRUE(pastHwm.empty()) << "consume past HWM must return empty";

    std::filesystem::remove_all(dir);
}
