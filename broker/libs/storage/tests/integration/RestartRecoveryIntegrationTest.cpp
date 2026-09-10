// ============================================================================
// RestartRecoveryIntegrationTest.cpp
//
// Integration tests: destroy and reopen PartitionApi over the same directory
// to exercise Log::recover() and verify durability across restarts.
// ============================================================================

#include "IntegrationTestBase.h"
#include <rune/storage/PartitionApi.h>

using namespace rune::storage;

// ---------------------------------------------------------------------------
// 1. Produce N, destroy, reopen: HWM == N, consumeAll returns all N byte-exact.
// ---------------------------------------------------------------------------
TEST_F(IntegrationTestBase, ProduceDestroyReopenHwmAndValues) {
    constexpr int N = 300;
    std::vector<ProducedRecord> produced;
    {
        PartitionApi api(testDir_.string());
        produced = produceN(api, N, 32);
        EXPECT_EQ(api.highWatermark(), static_cast<int64_t>(N));
    }
    {
        PartitionApi api(testDir_.string()); // reopen — calls recover() in ctor
        EXPECT_EQ(api.highWatermark(), static_cast<int64_t>(N));
        auto recs = consumeAll(api);
        verifyContiguous(recs, produced, N, "after first restart");
    }
}

// ---------------------------------------------------------------------------
// 2. Five restart cycles, each adding more records; assert contiguous
//    offsets 0..total-1 after every restart.
// ---------------------------------------------------------------------------
TEST_F(IntegrationTestBase, FiveRestartCyclesContiguousOffsets) {
    constexpr int BATCH = 50;
    std::vector<ProducedRecord> allProduced;
    int totalProduced = 0;

    for (int cycle = 0; cycle < 5; ++cycle) {
        {
            PartitionApi api(testDir_.string());
            EXPECT_EQ(api.highWatermark(), static_cast<int64_t>(totalProduced))
                << "cycle " << cycle << ": HWM on reopen wrong";
            // Produce a fresh batch (offset them so makeKey/makeValue are unique).
            for (int i = 0; i < BATCH; ++i) {
                int absIdx = totalProduced + i;
                ProducedRecord rec;
                rec.key       = "cyc" + std::to_string(cycle) + "-" + std::to_string(i);
                rec.value     = makeValue(absIdx, 24);
                rec.timestamp = static_cast<int64_t>(2000 + absIdx);
                api.produce(rec.key, rec.value, rec.timestamp);
                allProduced.push_back(std::move(rec));
            }
            totalProduced += BATCH;
            EXPECT_EQ(api.highWatermark(), static_cast<int64_t>(totalProduced))
                << "cycle " << cycle << ": HWM after produce wrong";
        }
        // Reopen and verify
        {
            PartitionApi api(testDir_.string());
            EXPECT_EQ(api.highWatermark(), static_cast<int64_t>(totalProduced))
                << "cycle " << cycle << ": HWM after reopen wrong";
            auto recs = consumeAll(api);
            ASSERT_EQ(static_cast<int>(recs.size()), totalProduced)
                << "cycle " << cycle << ": consumeAll count wrong";
            for (int i = 0; i < totalProduced; ++i) {
                EXPECT_EQ(recs[static_cast<size_t>(i)].offset,
                          static_cast<int64_t>(i))
                    << "cycle " << cycle << ": offset mismatch at i=" << i;
                EXPECT_EQ(recs[static_cast<size_t>(i)].value,
                          allProduced[static_cast<size_t>(i)].value)
                    << "cycle " << cycle << ": value mismatch at i=" << i;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// 3. Produce-after-restart continues contiguously (new records don't repeat
//    or skip offsets).
// ---------------------------------------------------------------------------
TEST_F(IntegrationTestBase, ProduceAfterRestartContinuesContiguously) {
    constexpr int FIRST = 100;
    constexpr int SECOND = 75;

    std::vector<ProducedRecord> allProduced;
    {
        PartitionApi api(testDir_.string());
        auto batch1 = produceN(api, FIRST, 16);
        allProduced.insert(allProduced.end(), batch1.begin(), batch1.end());
        EXPECT_EQ(api.highWatermark(), static_cast<int64_t>(FIRST));
    }
    {
        PartitionApi api(testDir_.string());
        EXPECT_EQ(api.highWatermark(), static_cast<int64_t>(FIRST));
        // Produce more; makeKey/makeValue indices start at FIRST to avoid collision.
        for (int i = FIRST; i < FIRST + SECOND; ++i) {
            ProducedRecord rec;
            rec.key       = makeKey(i);
            rec.value     = makeValue(i, 16);
            rec.timestamp = static_cast<int64_t>(1000 + i);
            api.produce(rec.key, rec.value, rec.timestamp);
            allProduced.push_back(rec);
        }
        EXPECT_EQ(api.highWatermark(), static_cast<int64_t>(FIRST + SECOND));

        auto recs = consumeAll(api);
        ASSERT_EQ(static_cast<int>(recs.size()), FIRST + SECOND);
        for (int i = 0; i < FIRST + SECOND; ++i) {
            EXPECT_EQ(recs[static_cast<size_t>(i)].offset, static_cast<int64_t>(i))
                << "offset mismatch at i=" << i;
            EXPECT_EQ(recs[static_cast<size_t>(i)].value,
                      allProduced[static_cast<size_t>(i)].value)
                << "value mismatch at i=" << i;
        }
    }
}

// ---------------------------------------------------------------------------
// 4. Reopen an empty directory → HWM == 0, consume returns empty.
// ---------------------------------------------------------------------------
TEST_F(IntegrationTestBase, ReopenEmptyDirectoryHwmZero) {
    {
        PartitionApi api(testDir_.string());
        EXPECT_EQ(api.highWatermark(), 0);
        EXPECT_TRUE(consumeAll(api).empty());
    }
    {
        PartitionApi api(testDir_.string()); // second open of empty dir
        EXPECT_EQ(api.highWatermark(), 0);
        EXPECT_TRUE(consumeAll(api).empty());
    }
}

// ---------------------------------------------------------------------------
// 5. Produce single record; restart; HWM == 1 and value exact.
// ---------------------------------------------------------------------------
TEST_F(IntegrationTestBase, SingleRecordSurvivesRestart) {
    std::string val;
    val += '\xDE'; val += '\xAD'; val += '\xBE'; val += '\xEF';
    {
        PartitionApi api(testDir_.string());
        api.produce("single", val, 42);
        EXPECT_EQ(api.highWatermark(), 1);
    }
    {
        PartitionApi api(testDir_.string());
        EXPECT_EQ(api.highWatermark(), 1);
        auto recs = consumeAll(api);
        ASSERT_EQ(recs.size(), 1u);
        EXPECT_EQ(recs[0].offset, 0);
        EXPECT_EQ(recs[0].key, "single");
        EXPECT_EQ(recs[0].value, val);
        EXPECT_EQ(recs[0].timestamp, 42);
    }
}

// ---------------------------------------------------------------------------
// 6. Many restarts on larger dataset — stress that recover() is idempotent.
// ---------------------------------------------------------------------------
TEST_F(IntegrationTestBase, RecoverIdempotentAfterManyReopens) {
    constexpr int N = 150;
    std::vector<ProducedRecord> produced;
    {
        PartitionApi api(testDir_.string());
        produced = produceN(api, N, 20);
    }
    // Reopen 4 times without producing — HWM must stay N.
    for (int pass = 0; pass < 4; ++pass) {
        PartitionApi api(testDir_.string());
        EXPECT_EQ(api.highWatermark(), static_cast<int64_t>(N))
            << "pass " << pass;
        auto recs = consumeAll(api);
        verifyContiguous(recs, produced, N, ("pass " + std::to_string(pass)).c_str());
    }
}
