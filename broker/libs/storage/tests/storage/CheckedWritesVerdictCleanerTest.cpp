// Part 04 Task 8: the cleaner on a verdict source - VerdictCleanState, the bound at W, the rewrite rules.
#include <gtest/gtest.h>

#include <atomic>
#include <filesystem>
#include <limits>
#include <map>
#include <set>
#include <string>
#include <unistd.h>
#include <vector>

#include <rune/protocol/RecordBatch.h>
#include <rune/storage/ControlRecordLayout.h>
#include <rune/storage/Log.h>
#include <rune/storage/LogCleaner.h>
#include <rune/storage/SkimpyOffsetMap.h>
#include <rune/storage/VerdictCleanState.h>

namespace cr = rune::storage::control_record;
using rune::storage::Log;
using rune::storage::LogCleaner;
using rune::storage::SkimpyOffsetMap;
using rune::storage::VerdictCleanState;
using Entry = rune::storage::RejectedOffsetIndex::Entry;
constexpr int64_t kMax = std::numeric_limits<int64_t>::max();

namespace {
std::filesystem::path tempDir() {
    static std::atomic<int> n{0};
    auto d = std::filesystem::temp_directory_path() / ("rune_cwvc_" + std::to_string(::getpid()) + "_" + std::to_string(n++));
    std::filesystem::remove_all(d);
    std::filesystem::create_directories(d);
    return d;
}
void stampBase(std::vector<uint8_t>& b, int64_t off) {
    for (int i = 0; i < 8; ++i) b[std::size_t(i)] = uint8_t(uint64_t(off) >> (8 * i));
}
void data(Log& log, int64_t off, const std::string& key, const std::string& val) {
    rune::protocol::RecordBatchBuilder b(off, 2000 + off);
    b.append(key, val, 2000 + off);
    log.append(b.build(), off);
}
// A verdict marker over [from, ft) listing `rejected`.
void marker(Log& log, int64_t off, int64_t from, int64_t ft, std::vector<int64_t> rejected) {
    std::vector<cr::MarkerEntry> es;
    for (const int64_t r : rejected) es.push_back(cr::MarkerEntry{uint32_t(r - from), 55, 1});
    std::vector<uint8_t> b(cr::markerBatchBytes(es.size()));
    (void)cr::buildVerdictMarker(b, cr::MarkerHeader{from, ft, 1, uint32_t(es.size())}, es);
    stampBase(b, off);
    log.append(b, off);
}
void change(Log& log, int64_t off, bool nonEmpty) {
    std::vector<cr::ChangeMember> ms;
    if (nonEmpty) ms.push_back(cr::ChangeMember{7, 3, 99});
    std::vector<uint8_t> b(cr::changeBatchBytes(ms.size()));
    (void)cr::buildConstraintSetChange(b, 1, ms);
    stampBase(b, off);
    log.append(b, off);
}

enum class Path { Range, RangeCoalesce };

// One pass over the sealed region.  Returns the pass end.
int64_t clean(Log& log, Path path, int64_t verdictBound, int64_t markerEnd, int64_t markerRemovalBound = kMax, int64_t nowMs = 1'000'000) {
    auto bases = log.segmentBaseOffsets();
    const int64_t active = bases.back();
    LogCleaner cleaner;
    cleaner.setMarkerRemovalBound(markerRemovalBound);
    cleaner.setVerdictBounds(verdictBound, markerEnd);
    bases.pop_back();
    std::vector<int64_t> sizes(bases.size(), 1);
    SkimpyOffsetMap map(1024);
    const auto res = cleaner.produceCleanedRangeBounded(log.dir(), bases, sizes, active, 0, 0, 1000, map, path == Path::RangeCoalesce, 1 << 20, nullptr, nowMs);
    log.reloadSegments();
    return res.passEnd;
}

std::set<int64_t> dataOffsets(Log& log) {
    std::set<int64_t> out;
    for (int64_t cur = log.logStartOffset(); cur < log.logEndOffset();) {
        auto bytes = log.read(cur, 1 << 20);
        if (bytes.empty()) {   // a cleaned gap: continue at the next segment
            const auto bases = log.segmentBaseOffsets();
            const auto next = std::upper_bound(bases.begin(), bases.end(), cur);
            if (next == bases.end()) break;
            cur = *next;
            continue;
        }
        rune::protocol::RecordBatchReader r(bytes.data(), bytes.size());
        if (!r.ensureValid()) break;
        if ((r.attributes() & rune::protocol::batch::kAttrControl) == 0)
            r.forEachRecord([&](std::string_view, std::string_view, int64_t o, int64_t) { out.insert(o); });
        cur = std::max(cur + 1, r.baseOffset() + r.lastOffsetDelta() + 1);
    }
    return out;
}
bool hasBatchAt(Log& log, int64_t off) {
    auto bytes = log.read(off, 1 << 20);
    if (bytes.empty()) return false;
    rune::protocol::RecordBatchReader r(bytes.data(), bytes.size());
    return r.baseOffset() == off;
}
}  // namespace

// ---- VerdictCleanState ------------------------------------------------------------------------------------

TEST(CheckedWritesVerdictCleaner, CoverageRules) {
    VerdictCleanState s;
    s.finish(100, kMax, kMax);
    EXPECT_EQ(s.cleanLimit(), kMax) << "never a change: not a verdict source";
    s.clear();
    s.onConstraintSetChange(4, true);
    s.finish(100, kMax, kMax);
    EXPECT_EQ(s.cleanLimit(), 5) << "a change and no marker: cover just past it";
    s.clear();
    s.onConstraintSetChange(4, true);
    s.onVerdictMarker(10, 5, 30, {});
    s.finish(100, kMax, kMax);
    EXPECT_EQ(s.cleanLimit(), 30);
    s.onConstraintSetChange(40, false);
    s.clear();
    s.onConstraintSetChange(4, true);
    s.onVerdictMarker(10, 5, 41, {});
    s.onConstraintSetChange(40, false);
    s.finish(100, kMax, kMax);
    EXPECT_EQ(s.cleanLimit(), kMax) << "an emptied set whose cover reached past the change: no verdict owed";
    s.clear();
    s.onConstraintSetChange(4, true);
    s.onVerdictMarker(10, 5, 41, {});
    s.onConstraintSetChange(40, false);
    s.onConstraintSetChange(50, true);
    s.finish(100, kMax, kMax);
    EXPECT_EQ(s.cleanLimit(), 51) << "re-activated at 50: the verdicts owed start again, cover just past the new change";
}

TEST(CheckedWritesVerdictCleanerRegression, MissingChangeRecordNeverUncapsRegression) {
    VerdictCleanState s;
    s.onVerdictMarker(10, 5, 30, {});
    s.finish(100, kMax, kMax);
    EXPECT_EQ(s.cleanLimit(), 30) << "markers without any change record: capped at what they cover, never uncapped";
    s.clear();
    s.finish(100, kMax, kMax);
    EXPECT_EQ(s.cleanLimit(), kMax) << "neither a change nor a marker: not a verdict source";
}

TEST(CheckedWritesVerdictCleaner, StaleBoundOnlyCleansLess) {
    VerdictCleanState s;
    s.onConstraintSetChange(0, true);
    s.onVerdictMarker(31, 1, 30, {});
    s.finish(100, /*publishedBound=*/50, kMax);
    EXPECT_EQ(s.cleanLimit(), 30);
    s.clear();
    s.onConstraintSetChange(0, true);
    s.onVerdictMarker(31, 1, 30, {});
    s.finish(100, /*publishedBound=*/20, kMax);
    EXPECT_EQ(s.cleanLimit(), 20);
}

TEST(CheckedWritesVerdictCleaner, MarkerRemovalNeedsASupersedingMarkerAndTheBounds) {
    VerdictCleanState s;
    s.onVerdictMarker(10, 0, 10, std::vector<Entry>{{7, 55, 1}});
    s.onVerdictMarker(20, 10, 20, {});
    s.finish(100, kMax, /*markerRemovalBound=*/10);
    s.markRemovalsBelow(100);
    EXPECT_FALSE(s.markerRemovable(10)) << "a replica has not fetched it";
    s.clear();
    s.onVerdictMarker(10, 0, 10, std::vector<Entry>{{7, 55, 1}});
    s.onVerdictMarker(20, 10, 20, {});
    s.finish(100, kMax, /*markerRemovalBound=*/11);
    EXPECT_FALSE(s.markerRemovable(10)) << "no pass boundary marked yet";
    s.markRemovalsBelow(7);
    EXPECT_FALSE(s.markerRemovable(10)) << "the rejected record is not below the pass boundary yet";
    s.markRemovalsBelow(8);
    EXPECT_TRUE(s.markerRemovable(10));
    s.markRemovalsBelow(100);
    EXPECT_FALSE(s.markerRemovable(20)) << "the last marker is the coverage";
    s.markRemovalsBelow(8);
    EXPECT_FALSE(s.keepsControlRecord(10, cr::kVerdictMarker));
    EXPECT_TRUE(s.keepsControlRecord(10, cr::kConstraintSetChange));
    EXPECT_TRUE(s.keepsControlRecord(10, cr::kTxnJudge));
}

// ---- the cleaner on a Log ---------------------------------------------------------------------------------

TEST(CheckedWritesVerdictCleaner, RejectedLatestValueNeverWins) {
    for (const Path path : {Path::Range, Path::RangeCoalesce}) {
        const auto dir = tempDir();
        {
            Log log(dir.string(), /*segmentBytes=*/1);
            log.recover();
            change(log, 0, true);
            for (int64_t o = 1; o < 3; ++o) data(log, o, "pad" + std::to_string(o), "p");
            data(log, 3, "5", "accepted");
            for (int64_t o = 4; o < 7; ++o) data(log, o, "pad" + std::to_string(o), "p");
            data(log, 7, "5", "rejected");
            data(log, 8, "pad8", "p");
            marker(log, 9, 1, 9, {7});
            for (int64_t o = 10; o < 13; ++o) data(log, o, "tail" + std::to_string(o), "t");
            clean(log, path, /*verdictBound=*/10, /*markerEnd=*/10);
            const auto offs = dataOffsets(log);
            EXPECT_TRUE(offs.count(3)) << "the accepted value stays";
            EXPECT_FALSE(offs.count(7)) << "the rejected one is dropped";
        }
        std::filesystem::remove_all(dir);
    }
}

// The two single-pass rewrite engines (one segment, one merged group) drop a rejected record as the range engine does.
TEST(CheckedWritesVerdictCleaner, RejectedRecordsAreDroppedInBothRewriteEngines) {
    for (const bool group : {false, true}) {
        const auto dir = tempDir();
        {
            Log log(dir.string(), /*segmentBytes=*/1);
            log.recover();
            change(log, 0, true);
            for (int64_t o = 1; o < 3; ++o) data(log, o, "pad" + std::to_string(o), "p");
            data(log, 3, "5", "accepted");
            for (int64_t o = 4; o < 7; ++o) data(log, o, "pad" + std::to_string(o), "p");
            data(log, 7, "5", "rejected");
            data(log, 8, "pad8", "p");
            marker(log, 9, 1, 9, {7});
            for (int64_t o = 10; o < 13; ++o) data(log, o, "tail" + std::to_string(o), "t");
            auto bases = log.segmentBaseOffsets();
            const int64_t active = bases.back();
            bases.pop_back();
            LogCleaner cleaner;
            cleaner.setVerdictBounds(10, 10);
            if (group) {
                rune::storage::SegmentGroup g;
                g.bases = {7, 8};
                g.combinedBytes = 1 << 20;
                (void)cleaner.produceCleanedGroupBounded(log.dir(), bases, g, active, 0, 0, 1000, 1 << 20, nullptr, 1'000'000);
            } else {
                (void)cleaner.produceCleanedFileBounded(log.dir(), bases, /*segBase=*/7, active, 0, 0, 1000, 1 << 20, nullptr, 1'000'000);
            }
            log.reloadSegments();
            const auto offs = dataOffsets(log);
            EXPECT_TRUE(offs.count(3)) << (group ? "group" : "file") << ": the accepted value stays";
            EXPECT_FALSE(offs.count(7)) << (group ? "group" : "file") << ": the rejected one is dropped";
        }
        std::filesystem::remove_all(dir);
    }
}

TEST(CheckedWritesVerdictCleanerRegression, NeverCleansAtOrAboveWRegression) {
    const auto dir = tempDir();
    {
        Log log(dir.string(), 1);
        log.recover();
        change(log, 0, true);
        for (int64_t o = 1; o < 20; ++o) data(log, o, "k", "v" + std::to_string(o));   // every older copy is superseded
        marker(log, 20, 1, 20, {});
        for (int64_t o = 21; o < 23; ++o) data(log, o, "tail" + std::to_string(o), "t");
        clean(log, Path::Range, /*verdictBound=*/6, /*markerEnd=*/21);
        const auto offs = dataOffsets(log);
        for (int64_t o = 6; o < 20; ++o) EXPECT_TRUE(offs.count(o)) << "offset " << o << " at or above W was rewritten away";
    }
    std::filesystem::remove_all(dir);
}

TEST(CheckedWritesVerdictCleaner, MarkerAboveToOffsetIsReadUpToMarkerEnd) {
    const auto dir = tempDir();
    {
        Log log(dir.string(), 1);
        log.recover();
        change(log, 0, true);
        for (int64_t o = 1; o < 7; ++o) data(log, o, "pad" + std::to_string(o), "p");
        data(log, 7, "k", "rejected");
        for (int64_t o = 8; o < 15; ++o) data(log, o, "pad" + std::to_string(o), "p");
        marker(log, 15, 1, 15, {7});
        data(log, 16, "tail", "t");
        const auto bases = log.segmentBaseOffsets();
        for (const int64_t markerEnd : {int64_t(16), int64_t(10)}) {
            LogCleaner cleaner;
            cleaner.setVerdictBounds(kMax, markerEnd);
            SkimpyOffsetMap map(1024);
            cleaner.buildOffsetMapFromBases(log.dir(), bases, 0, /*toOffset=*/10, map);   // the map filled below the marker
            EXPECT_EQ(map.verdictState().isRejected(7), markerEnd == 16) << "markerEnd " << markerEnd;
            if (markerEnd == 10) EXPECT_LT(map.verdictState().cleanLimit(), 8) << "the marker was not read: offset 7 has no verdict yet";
            else EXPECT_GE(map.verdictState().cleanLimit(), 8) << "the marker covers offset 7";
        }
    }
    std::filesystem::remove_all(dir);
}

TEST(CheckedWritesVerdictCleaner, ChangeRecordsAreAlwaysKept) {
    const auto dir = tempDir();
    {
        Log log(dir.string(), 1);
        log.recover();
        change(log, 0, true);
        for (int64_t o = 1; o < 4; ++o) data(log, o, "pad" + std::to_string(o), "p");
        change(log, 4, false);
        marker(log, 5, 1, 5, {});
        for (int64_t o = 6; o < 9; ++o) data(log, o, "pad" + std::to_string(o), "p");
        for (int pass = 0; pass < 4; ++pass) clean(log, Path::Range, kMax, 9, kMax, 1'000'000 + pass * 2000);
        EXPECT_TRUE(hasBatchAt(log, 0));
        EXPECT_TRUE(hasBatchAt(log, 4));
    }
    std::filesystem::remove_all(dir);
}

TEST(CheckedWritesVerdictCleanerRegression, MarkerKeptUntilEveryReplicaFetchedItRegression) {
    const auto dir = tempDir();
    {
        Log log(dir.string(), 1);
        log.recover();
        change(log, 0, true);
        data(log, 1, "pad1", "p");
        data(log, 2, "k", "rejected");
        marker(log, 3, 1, 3, {2});
        data(log, 4, "pad4", "p");
        marker(log, 5, 3, 5, {});
        for (int64_t o = 6; o < 9; ++o) data(log, o, "pad" + std::to_string(o), "p");
        const int64_t t0 = 1'000'000;
        clean(log, Path::Range, kMax, 9, /*markerRemovalBound=*/3, t0);
        EXPECT_TRUE(hasBatchAt(log, 3)) << "a replica has not fetched the marker: kept";
        clean(log, Path::Range, kMax, 9, /*markerRemovalBound=*/4, t0);
        ASSERT_TRUE(hasBatchAt(log, 3)) << "stamped, not dropped";
        clean(log, Path::Range, kMax, 9, /*markerRemovalBound=*/4, t0 + 5000);
        EXPECT_FALSE(hasBatchAt(log, 3)) << "dropped at the horizon";
        EXPECT_TRUE(hasBatchAt(log, 5)) << "the last marker is the coverage";
    }
    std::filesystem::remove_all(dir);
}
