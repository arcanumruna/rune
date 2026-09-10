#include <gtest/gtest.h>
#include <rune/storage/PartitionApi.h>
#include <filesystem>
#include <string>
#include <vector>
using namespace rune::storage;

// ---- Test 1 (brief's canonical test): produce/consume + restart durability ----
TEST(PartitionApi, ProduceConsumeAndSurviveRestart) {
    auto dir = std::filesystem::temp_directory_path() / "rune_api_t1";
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
    auto dir = std::filesystem::temp_directory_path() / "rune_api_t2";
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
    auto dir = std::filesystem::temp_directory_path() / "rune_api_t3";
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
        }
    }
    std::filesystem::remove_all(dir);
}

// ---- Test 4: consume from arbitrary offsets ----
TEST(PartitionApi, ConsumeFromArbitraryOffsets) {
    auto dir = std::filesystem::temp_directory_path() / "rune_api_t4";
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
    auto dir = std::filesystem::temp_directory_path() / "rune_api_t5";
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

// ---- Test 6: restart mid-stream (proves recover() sets logEndOffset correctly) ----
TEST(PartitionApi, RestartMidStream) {
    auto dir = std::filesystem::temp_directory_path() / "rune_api_t6";
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
