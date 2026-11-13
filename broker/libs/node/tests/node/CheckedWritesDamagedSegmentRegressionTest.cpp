// #874: a view and the verdict judge fail explicitly at a damaged log segment instead of waiting for bytes that
// will never come.
#include "CheckedWritesRig.h"

#include <rune/storage/testing/DamageSegmentForTest.h>

#include <filesystem>

using namespace rune::node;
using namespace rune::node::cw;

namespace {

constexpr int32_t kProj = 0x40000511, kRej = 0x40000611;
constexpr uint64_t kObj = 91;
constexpr int64_t kRoll = 400;   // a few one-record batches per segment

TestRejectRule tagRule(uint8_t tag) {
    TestRejectRule r;
    r.mode = TestRejectRule::Mode::TagEquals;
    r.tag = tag;
    return r;
}

std::size_t segmentFiles(const std::string& dir) {
    std::size_t n = 0;
    for (const auto& e : std::filesystem::directory_iterator(dir)) n += e.path().extension() == ".log" ? 1 : 0;
    return n;
}

/// A single-node source of one-record batches over several sealed segments, the second one damaged.
struct DamagedSource {
    VerdictRig r{false, kRoll};
    int64_t damaged = -1;   ///< the first offset of the damaged batch

    /// `records` produced (none judged yet when the judge is held), then segment 1 is damaged and re-detected.
    void build(int records) {
        r.log->setDeferSealSync(false);   // inline seals: the recovery point is persisted per roll
        for (int i = 1; i <= records; ++i) ASSERT_EQ(r.produce({{static_cast<uint64_t>(i), 10 * i, 0}}), 0);
        r.log->sync();
        ASSERT_GE(segmentFiles(r.log->dir()), 3u) << "the damage needs a sealed segment with a successor";
        damaged = rune::storage::testing::damageSecondBatchOfSegment(r.log->dir(), 1);
        ASSERT_GT(damaged, 0);
        ASSERT_TRUE(r.log->reloadSegments());
        ASSERT_EQ(r.log->damagedSegmentCount(), 1u);
    }
    [[nodiscard]] uint64_t corruptReads() const { return r.worker->corruptReadsForTest(); }
    VerdictRig::ViewRead readRow(uint64_t id) {
        std::vector<uint8_t> key(8);
        for (int i = 0; i < 8; ++i) key[static_cast<std::size_t>(i)] = static_cast<uint8_t>(id >> (8 * i));
        return r.viewRead(WorkerOp::Type::GetViewRowK, kProj, key);
    }
};

}  // namespace

// A view folding a damaged source pauses at the damaged offset: reads answer StorageError, never NotReady and never a
// row, and the paused view does not read the log again every pass.
TEST(CheckedWritesDamagedSegment, ViewAtADamagedSegmentPausesAndAnswersStorageErrorRegression) {
    DamagedSource s;
    s.r.typed();
    s.build(30);
    s.r.installProjection(kProj);
    s.r.passes(60);
    EXPECT_EQ(s.r.vs->viewDamagedPausesForTest(), 1u);
    EXPECT_EQ(s.r.vs->foldCursorForTest(kProj, 0), s.damaged) << "the fold stopped at the damaged batch";
    const auto read = s.readRow(1);
    EXPECT_EQ(read.code, static_cast<int16_t>(rune::protocol::client::ErrorCode::StorageError));
    EXPECT_TRUE(read.row.empty());
    const uint64_t before = s.corruptReads();
    for (int i = 0; i < 500; ++i) s.r.pass(0);   // the clock does not advance: no probe is due
    EXPECT_EQ(s.corruptReads(), before) << "a paused view reads the damaged segment again every pass";
    EXPECT_EQ(s.r.vs->viewDamagedPausesForTest(), 1u);
}

// The paused view probes its offset once per second of the clock, no more.
TEST(CheckedWritesDamagedSegment, DamagedViewProbesOncePerSecondRegression) {
    DamagedSource s;
    s.r.typed();
    s.build(30);
    s.r.installProjection(kProj);
    s.r.passes(60);
    ASSERT_EQ(s.r.vs->viewDamagedPausesForTest(), 1u);
    const uint64_t base = s.corruptReads();
    for (uint64_t step = 1; step <= 5; ++step) {
        s.r.pass(1'000'000'000);
        for (int i = 0; i < 20; ++i) s.r.pass(0);
        EXPECT_EQ(s.corruptReads(), base + step) << "step " << step << ": one probe per second, no more";
    }
    EXPECT_EQ(s.r.vs->viewDamagedPausesForTest(), 1u) << "a probe that still reads damaged does not pause again";
}

// A judge that must read through a damaged segment stalls with an explicit reason, stops reading, and a new write to
// its partition is refused with StorageError (nothing appended) instead of waiting for a verdict that never comes.
TEST(CheckedWritesDamagedSegment, JudgeAtADamagedSegmentStallsAndProducesAreRefusedRegression) {
    DamagedSource s;
    s.r.typed();
    s.r.installRejectView(kRej, kObj, tagRule(9));
    s.r.holdJudge(true);   // nothing is judged while the log is written: the judge must read all of it later
    s.build(30);
    s.r.holdJudge(false);
    s.r.passes(100);
    const VerdictSource* judge = s.r.vs->verdictSource(VerdictRig::kSrc, 0);
    ASSERT_NE(judge, nullptr);
    EXPECT_TRUE(judge->damaged());
    ASSERT_NE(s.r.stats(), nullptr);
    EXPECT_EQ(s.r.stats()->verdictStalls, 1u);
    const uint64_t before = s.corruptReads();
    const int64_t leo = s.r.leo();
    s.r.passes(300);
    EXPECT_EQ(s.corruptReads(), before) << "a stalled judge reads the damaged segment again";
    EXPECT_EQ(s.r.produce({{1000, 5, 0}}), static_cast<int16_t>(rune::protocol::client::ErrorCode::StorageError));
    EXPECT_EQ(s.r.leo(), leo) << "a refused produce appended nothing";
    EXPECT_EQ(s.r.worker->checkedDamagedRefusalsForTest(), 1u);
}
