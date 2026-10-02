// F27OwedMemoSwapTest.cpp — re-review R2: the owed-answer walk memo must never
// be reused across a compaction swap that rewrites the bytes below an UNCHANGED
// LEO (a torn batch / wrong dataServedThrough otherwise).  Injected clock only.
#include <gtest/gtest.h>

#include <rune/protocol/RecordBatch.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/storage/Log.h>

#include <filesystem>
#include <fstream>
#include <memory>
#include <string>
#include <vector>
#include <unistd.h>

using namespace rune::replication;
namespace fs = std::filesystem;

namespace {

struct RefusingTransport final : PartitionTransport {
    bool refuse3 = true;
    std::vector<std::vector<uint8_t>> to3;   // accepted records payloads to follower 3
    bool sendFetchRequest(const FetchRequestMsg&) override { return true; }
    bool sendFetchResponse(NodeId to, const FetchResponseMsg& m) override {
        if (to != 3) return true;
        if (refuse3) return false;                       // BACK_PRESSURED
        to3.emplace_back(m.records.begin(), m.records.end());
        return true;
    }
};

std::vector<uint8_t> batchAt(Offset base, std::size_t valueBytes) {
    rune::protocol::RecordBatchBuilder b(base, 1000);
    b.append("k", std::string(valueBytes, 'x'), 1000);
    return b.build();
}

} // namespace

TEST(F27OwedMemoRegression, OwedAnswerMemoRebuiltAfterCompactionSwapAtSameLeoRegression) {
    const fs::path dir = fs::temp_directory_path() /
                         ("rune_f27_memo_" + std::to_string(::getpid()));
    fs::remove_all(dir);
    fs::create_directories(dir);
    {
        rune::storage::Log log(dir.string(), /*segmentRollBytes=*/1000);
        log.recover();
        for (Offset o = 0; o < 3; ++o) log.append(batchAt(o, 400), o);   // segment 0: offsets 0..2
        log.append(batchAt(3, 400), 3);                                  // rolls → segment 3
        ASSERT_EQ(log.logEndOffset(), 4);
        ASSERT_GT(log.nextSegmentBaseAfter(0), 0) << "precondition: segment 0 is sealed";

        PartitionConfig cfg;
        cfg.partitionId = 0; cfg.selfId = 1; cfg.minIsr = 1;
        cfg.replicaLagMaxMs = 60'000; cfg.replicaLagMaxOffset = 1'000'000;
        cfg.fetchIntervalNs = 1'000'000; cfg.isrCheckIntervalNs = 1'000'000'000;
        cfg.maxFetchBytes = 1 << 20; cfg.pushOnAppend = false;
        RefusingTransport t;
        PartitionReplica leader(cfg, Role::Leader, 1, {1, 2, 3}, {1, 2, 3}, &log, &t);

        const std::size_t oneBatch = batchAt(0, 400).size();
        constexpr int64_t kNow = 1'000'000'000;
        // Fetch from 0, room for exactly ONE old batch → owed (refused).
        (void)leader.onFetchRequest(FetchRequestMsg{1, 0, 3, 0, static_cast<int32_t>(oneBatch + 10), 0}, kNow);
        (void)leader.doWork(kNow);                       // retry: walk built (one old batch), refused
        ASSERT_EQ(leader.owedAnswerRebuildsForTest(), 1u);

        // Compaction: segment 0 rewritten (rename-into-place, like the cleaner):
        // offset 0 removed, offsets 1 and 2 now SMALLER batches.  LEO unchanged.
        const std::vector<uint8_t> b1 = batchAt(1, 129), b2 = batchAt(2, 209);
        ASSERT_LT(b1.size(), oneBatch);
        ASSERT_GT(b1.size() + b2.size(), oneBatch) << "the old length would cut b2 mid-batch";
        ASSERT_LE(b1.size() + b2.size(), oneBatch + 10) << "both new batches fit the fetch's maxBytes";
        const std::string seg0 = (dir / "00000000000000000000.log").string();
        {
            std::ofstream out(seg0 + ".new", std::ios::binary | std::ios::trunc);
            out.write(reinterpret_cast<const char*>(b1.data()), static_cast<std::streamsize>(b1.size()));
            out.write(reinterpret_cast<const char*>(b2.data()), static_cast<std::streamsize>(b2.size()));
        }
        fs::rename(seg0 + ".new", seg0);
        for (auto& e : fs::directory_iterator(dir))
            if (e.path().stem() == "00000000000000000000" && e.path().extension() != ".log")
                fs::remove(e.path());                    // stale index: rebuilt by recover()
        log.reloadSegment(0);
        ASSERT_EQ(log.logEndOffset(), 4) << "same LEO across the swap";

        t.refuse3 = false;
        (void)leader.doWork(kNow);                       // retry after the swap
        ASSERT_EQ(t.to3.size(), 1u) << "the owed answer is delivered";
        const auto& rec = t.to3.front();
        std::size_t pos = 0;
        int batches = 0;
        while (pos < rec.size()) {
            rune::protocol::RecordBatchReader r(rec.data() + pos, rec.size() - pos);
            const int32_t total = r.totalLength();
            ASSERT_GT(total, 0);
            ASSERT_LE(pos + static_cast<std::size_t>(total), rec.size())
                << "TORN batch: the stale pre-swap length cut a post-swap batch";
            pos += static_cast<std::size_t>(total);
            ++batches;
        }
        EXPECT_EQ(pos, rec.size());
        EXPECT_GE(batches, 1);
        EXPECT_EQ(rune::protocol::RecordBatchReader(rec.data(), rec.size()).baseOffset(), 1);
        EXPECT_GE(leader.owedAnswerRebuildsForTest(), 2u) << "the swap forced a rebuild";
    }
    fs::remove_all(dir);
}
