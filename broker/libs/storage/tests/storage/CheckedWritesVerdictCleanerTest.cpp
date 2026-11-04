// Part 04 Task 8: the cleaner on a verdict source - VerdictCleanState, the bound at W, the rewrite rules.
#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <fstream>
#include <filesystem>
#include <limits>
#include <map>
#include <optional>
#include <iomanip>
#include <iostream>
#include <set>
#include <sstream>
#include <string>
#include <unistd.h>
#include <vector>

#include <rune/protocol/BatchRebuild.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/storage/ControlRecordLayout.h>
#include <rune/storage/Log.h>
#include <rune/storage/LogCleaner.h>
#include <rune/storage/SkimpyOffsetMap.h>
#include <rune/storage/VerdictCleanState.h>
#include <rune/transaction/ControlMarker.h>

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
// offset -> attributes byte of every data record in the log
std::map<int64_t, uint8_t> recordAttributes(Log& log) {
    std::map<int64_t, uint8_t> out;
    for (int64_t cur = log.logStartOffset(); cur < log.logEndOffset();) {
        auto bytes = log.read(cur, 1 << 20);
        if (bytes.empty()) {
            const auto bases = log.segmentBaseOffsets();
            const auto next = std::upper_bound(bases.begin(), bases.end(), cur);
            if (next == bases.end()) break;
            cur = *next;
            continue;
        }
        for (std::size_t pos = 0; pos < bytes.size();) {
            rune::protocol::RecordBatchReader r(bytes.data() + pos, bytes.size() - pos);
            if (!r.ensureValid()) return out;
            if ((r.attributes() & rune::protocol::batch::kAttrControl) == 0)
                (void)rune::protocol::forEachRecordSpan(bytes.data() + pos, bytes.size() - pos, [&](const rune::protocol::RecordSpan& rec) { out[rec.offset] = rec.attributes; });
            cur = std::max(cur + 1, r.baseOffset() + r.lastOffsetDelta() + 1);
            pos += static_cast<std::size_t>(r.totalLength());
        }
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
    s.finish(0, kMax, kMax);
    EXPECT_EQ(s.cleanLimit(), kMax) << "never a change: not a verdict source";
    s.clear();
    s.onConstraintSetChange(4, true);
    s.finish(0, kMax, kMax);
    EXPECT_EQ(s.cleanLimit(), 5) << "a change and no marker: cover just past it";
    s.clear();
    s.onConstraintSetChange(4, true);
    s.onVerdictMarker(10, 5, 30, {});
    s.finish(0, kMax, kMax);
    EXPECT_EQ(s.cleanLimit(), 30);
    s.onConstraintSetChange(40, false);
    s.clear();
    s.onConstraintSetChange(4, true);
    s.onVerdictMarker(10, 5, 41, {});
    s.onConstraintSetChange(40, false);
    s.finish(0, kMax, kMax);
    EXPECT_EQ(s.cleanLimit(), kMax) << "an emptied set whose cover reached past the change: no verdict owed";
    s.clear();
    s.onConstraintSetChange(4, true);
    s.onVerdictMarker(10, 5, 41, {});
    s.onConstraintSetChange(40, false);
    s.onConstraintSetChange(50, true);
    s.finish(0, kMax, kMax);
    EXPECT_EQ(s.cleanLimit(), 51) << "re-activated at 50: the verdicts owed start again, cover just past the new change";
}

TEST(CheckedWritesVerdictCleanerRegression, MissingChangeRecordNeverUncapsRegression) {
    VerdictCleanState s;
    s.onVerdictMarker(10, 5, 30, {});
    s.finish(0, kMax, kMax);
    EXPECT_EQ(s.cleanLimit(), 30) << "markers without any change record: capped at what they cover, never uncapped";
    s.clear();
    s.finish(0, kMax, kMax);
    EXPECT_EQ(s.cleanLimit(), kMax) << "neither a change nor a marker: not a verdict source";
}

TEST(CheckedWritesVerdictCleaner, StaleBoundOnlyCleansLess) {
    VerdictCleanState s;
    s.onConstraintSetChange(0, true);
    s.onVerdictMarker(31, 1, 30, {});
    s.finish(0, /*publishedBound=*/50, kMax);
    EXPECT_EQ(s.cleanLimit(), 30);
    s.clear();
    s.onConstraintSetChange(0, true);
    s.onVerdictMarker(31, 1, 30, {});
    s.finish(0, /*publishedBound=*/20, kMax);
    EXPECT_EQ(s.cleanLimit(), 20);
}

TEST(CheckedWritesVerdictCleaner, MarkerRemovalNeedsASupersedingMarkerAndTheBounds) {
    VerdictCleanState s;
    s.onVerdictMarker(10, 0, 10, std::vector<Entry>{{7, 55, 1}});
    s.onVerdictMarker(20, 10, 20, {});
    s.finish(0, kMax, /*markerRemovalBound=*/10);
    s.markRemovalsBelow(100);
    EXPECT_FALSE(s.markerRemovable(10)) << "a replica has not fetched it";
    s.clear();
    s.onVerdictMarker(10, 0, 10, std::vector<Entry>{{7, 55, 1}});
    s.onVerdictMarker(20, 10, 20, {});
    s.finish(0, kMax, /*markerRemovalBound=*/11);
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

TEST(CheckedWritesVerdictCleanerRegression, RejectionsBelowTheDirtyStartAreNoWorkRegression) {
    VerdictCleanState s;
    s.onConstraintSetChange(0, true);
    s.onVerdictMarker(31, 1, 30, std::vector<Entry>{{7, 55, 1}});
    s.finish(/*dirtyStart=*/8, kMax, kMax);
    EXPECT_FALSE(s.hasWork()) << "the only rejected offset was compacted by an earlier pass";
    s.clear();
    s.onConstraintSetChange(0, true);
    s.onVerdictMarker(31, 1, 30, std::vector<Entry>{{7, 55, 1}});
    s.finish(/*dirtyStart=*/7, kMax, kMax);
    EXPECT_TRUE(s.hasWork()) << "a rejected offset inside [dirtyStart, cleanLimit)";
    s.clear();
    s.onConstraintSetChange(0, true);
    s.onVerdictMarker(31, 1, 30, std::vector<Entry>{{7, 55, 1}});
    s.finish(/*dirtyStart=*/0, /*publishedBound=*/7, kMax);
    EXPECT_FALSE(s.hasWork()) << "a rejected offset at or above the clean limit is not this pass's work";
    s.clear();
    s.onVerdictMarker(10, 0, 10, std::vector<Entry>{{7, 55, 1}});
    s.onVerdictMarker(20, 10, 20, {});
    s.finish(/*dirtyStart=*/8, kMax, /*markerRemovalBound=*/11);
    EXPECT_TRUE(s.hasWork()) << "a superseded marker is removable work";
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

TEST(CheckedWritesVerdictCleanerRegression, RebuildKeepsRecordAttributesRegression) {
    for (const Path path : {Path::Range, Path::RangeCoalesce}) {
        SCOPED_TRACE(path == Path::Range ? "range" : "coalesce");
        const auto dir = tempDir();
        {
            Log log(dir.string(), /*segmentBytes=*/1);
            log.recover();
            // (#839: the lastOffset passed to append is the batch's real last offset - a lower one rolled the next segment at 1,
            // INSIDE this batch, which the header bound rightly calls damage.)
            rune::protocol::RecordBatchBuilder a(0, 2000);   // offset 0 is superseded: the batch is rebuilt without it
            a.append("k1", "old", 2000, 0);
            a.append("k2", "kept", 2001, 2);
            log.append(a.build(), 1);
            rune::protocol::RecordBatchBuilder b(2, 2002);
            b.append("k1", "new", 2002, 1);
            b.append("k3", "also", 2003, 3);
            log.append(b.build(), 3);
            data(log, 4, "tail", "t");
            clean(log, path, /*verdictBound=*/kMax, /*markerEnd=*/kMax);
            const auto attrs = recordAttributes(log);
            EXPECT_EQ(attrs.count(0), 0u) << "the superseded record is gone";
            ASSERT_EQ(attrs.count(1), 1u);
            EXPECT_EQ(attrs.at(1), 2) << "kept record's intent bits survive the rebuild";
            EXPECT_EQ(attrs.at(2), 1);
            EXPECT_EQ(attrs.at(3), 3);
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

namespace {
// Flips the last byte of the batch whose base offset is `off`, in whichever segment file holds it (its CRC no longer matches).
bool flipLastByteOfBatch(const std::string& dir, int64_t off) {
    for (const auto& e : std::filesystem::directory_iterator(dir)) {
        if (e.path().extension() != ".log") continue;
        std::vector<uint8_t> buf(std::filesystem::file_size(e.path()));
        {
            std::ifstream in(e.path(), std::ios::binary);
            in.read(reinterpret_cast<char*>(buf.data()), static_cast<std::streamsize>(buf.size()));
        }
        for (std::size_t pos = 0; pos + rune::protocol::batch::kRecordBatchOverhead <= buf.size();) {
            rune::protocol::RecordBatchReader r(buf.data() + pos, buf.size() - pos);
            const std::size_t total = static_cast<std::size_t>(r.totalLength());
            if (total < rune::protocol::batch::kRecordBatchOverhead || pos + total > buf.size()) break;
            if (r.baseOffset() == off) {
                std::fstream f(e.path(), std::ios::binary | std::ios::in | std::ios::out);
                f.seekp(static_cast<std::streamoff>(pos + total - 1));
                const char flipped = static_cast<char>(buf[pos + total - 1] ^ 0xFF);
                f.write(&flipped, 1);
                return true;
            }
            pos += total;
        }
    }
    return false;
}
}  // namespace

// #804: the cleaner's walks validate a control record once, in parseControl, and a data batch with ensureValid.  Neither may be
// weakened: a damaged marker is not read, and a damaged data batch stops the walk before the marker behind it.  (There is no CRC
// counter seam, so the single validation is held by the code shape; this guards the refusals that must survive it.)
TEST(CheckedWritesVerdictCleanerRegression, CleanerWalkRefusesADamagedMarkerAndADamagedDataBatchRegression) {
    const auto dir = tempDir();
    {
        Log log(dir.string(), /*segmentBytes=*/1 << 20);   // one segment: a damaged batch stops the walk of everything behind it
        log.recover();
        change(log, 0, true);
        for (int64_t o = 1; o < 7; ++o) data(log, o, "pad" + std::to_string(o), "p");
        data(log, 7, "k", "rejected");
        for (int64_t o = 8; o < 15; ++o) data(log, o, "pad" + std::to_string(o), "p");
        marker(log, 15, 1, 15, {7});
        data(log, 16, "tail", "t");
        const auto bases = log.segmentBaseOffsets();
        const auto scan = [&](bool& rejected, int64_t& limit) {
            LogCleaner cleaner;
            cleaner.setVerdictBounds(kMax, 16);
            SkimpyOffsetMap map(1024);
            cleaner.buildOffsetMapFromBases(log.dir(), bases, 0, /*toOffset=*/10, map);
            rejected = map.verdictState().isRejected(7);
            limit    = map.verdictState().cleanLimit();
        };
        bool rejected = false;
        int64_t limit = 0;
        scan(rejected, limit);
        ASSERT_TRUE(rejected) << "intact: the marker is read";
        ASSERT_GE(limit, 8);

        ASSERT_TRUE(flipLastByteOfBatch(log.dir(), 15));   // the marker
        scan(rejected, limit);
        EXPECT_FALSE(rejected) << "a damaged marker is refused";
        EXPECT_LT(limit, 8);
        ASSERT_TRUE(flipLastByteOfBatch(log.dir(), 15));   // restored

        ASSERT_TRUE(flipLastByteOfBatch(log.dir(), 3));    // a data batch before the marker
        scan(rejected, limit);
        EXPECT_FALSE(rejected) << "a damaged data batch stops the walk before the marker behind it";
        ASSERT_TRUE(flipLastByteOfBatch(log.dir(), 3));
        scan(rejected, limit);
        EXPECT_TRUE(rejected) << "restored: read again";
    }
    std::filesystem::remove_all(dir);
}

// ===========================================================================
// Batch 1 adversarial pass: the cleaner over a damaged or truncated verdict marker, at every byte offset.
// ===========================================================================
namespace {

// Every data record offset on disk, file by file: a damaged batch is skipped by its length (a bad length ends that file).
// Independent of the Log's index, so it shows what the cleaner kept, not what a reader of a damaged log can reach.
std::set<int64_t> diskDataOffsets(const std::string& dir) {
    std::set<int64_t> out;
    for (const auto& e : std::filesystem::directory_iterator(dir)) {
        if (e.path().extension() != ".log") continue;
        std::vector<uint8_t> buf(std::filesystem::file_size(e.path()));
        {
            std::ifstream in(e.path(), std::ios::binary);
            in.read(reinterpret_cast<char*>(buf.data()), static_cast<std::streamsize>(buf.size()));
        }
        for (std::size_t pos = 0; pos + rune::protocol::batch::kRecordBatchOverhead <= buf.size();) {
            rune::protocol::RecordBatchReader r(buf.data() + pos, buf.size() - pos);
            const std::size_t total = static_cast<std::size_t>(r.totalLength());
            if (total < rune::protocol::batch::kRecordBatchOverhead || pos + total > buf.size()) break;
            if (r.ensureValid() && (r.attributes() & rune::protocol::batch::kAttrControl) == 0)
                r.forEachRecord([&](std::string_view, std::string_view, int64_t o, int64_t) { out.insert(o); });
            pos += total;
        }
    }
    return out;
}

// One range pass over the sealed segments, file work only: no Log reload afterwards (a reload's recovery truncates a segment at
// a damaged batch, which would hide what the cleaner itself wrote).
void cleanFilesOnly(const std::string& dir, const std::vector<int64_t>& allBases, Path path, int64_t verdictBound, int64_t markerEnd) {
    auto bases = allBases;
    const int64_t active = bases.back();
    bases.pop_back();
    LogCleaner cleaner;
    cleaner.setVerdictBounds(verdictBound, markerEnd);
    std::vector<int64_t> sizes(bases.size(), 1);
    SkimpyOffsetMap map(1024);
    (void)cleaner.produceCleanedRangeBounded(dir, bases, sizes, active, 0, 0, 1000, map, path == Path::RangeCoalesce, 1 << 20, nullptr, 1'000'000);
}

struct BatchPos { std::filesystem::path file; std::size_t pos; std::size_t total; };

// Where the batch whose base offset is `off` sits on disk.
std::optional<BatchPos> findBatch(const std::string& dir, int64_t off) {
    for (const auto& e : std::filesystem::directory_iterator(dir)) {
        if (e.path().extension() != ".log") continue;
        std::vector<uint8_t> buf(std::filesystem::file_size(e.path()));
        {
            std::ifstream in(e.path(), std::ios::binary);
            in.read(reinterpret_cast<char*>(buf.data()), static_cast<std::streamsize>(buf.size()));
        }
        for (std::size_t pos = 0; pos + rune::protocol::batch::kRecordBatchOverhead <= buf.size();) {
            rune::protocol::RecordBatchReader r(buf.data() + pos, buf.size() - pos);
            const std::size_t total = static_cast<std::size_t>(r.totalLength());
            if (total < rune::protocol::batch::kRecordBatchOverhead || pos + total > buf.size()) break;
            if (r.baseOffset() == off) return BatchPos{e.path(), pos, total};
            pos += total;
        }
    }
    return std::nullopt;
}

void xorByteAt(const std::filesystem::path& f, std::size_t at, uint8_t mask) {
    std::fstream io(f, std::ios::binary | std::ios::in | std::ios::out);
    io.seekg(static_cast<std::streamoff>(at));
    char c = 0;
    io.read(&c, 1);
    c = static_cast<char>(static_cast<uint8_t>(c) ^ mask);
    io.seekp(static_cast<std::streamoff>(at));
    io.write(&c, 1);
}

// The log every damage case starts from.  Offsets:
//   0 change (non-empty set), 1 dup=old, 2..4 pads, 5 dup=new, 6 "rej" (listed by the marker), 7 marker over [1, 7) {6},
//   8..11 data, then pads up to the roll, later segments with more data; the active segment last.
// Returns every data offset written.
std::set<int64_t> buildDamageLog(Log& log) {
    std::set<int64_t> offs;
    change(log, 0, true);
    data(log, 1, "dup", "old"); offs.insert(1);
    for (int64_t o = 2; o < 5; ++o) { data(log, o, "pad" + std::to_string(o), "p"); offs.insert(o); }
    data(log, 5, "dup", "new"); offs.insert(5);
    data(log, 6, "rej", "r");   offs.insert(6);
    marker(log, 7, 1, 7, {6});
    for (int64_t o = 8; o < 40; ++o) { data(log, o, "after" + std::to_string(o), "a"); offs.insert(o); }
    return offs;
}

}  // namespace

// Flip every byte of a sealed verdict marker in turn (and, separately, cut the segment inside it at every length), then run a
// full cleaner pass on both range engines.  Whatever the damage: no data record but the superseded `dup` (1) and — only when
// the damaged byte lies outside the CRC and the framing checks, so the marker is still intact — the rejected record (6) may
// disappear.  Every record after the marker, in its own segment and in later ones, survives.
TEST(CheckedWritesVerdictCleanerRegression, DamagedMarkerAtEveryByteNeverDropsDataAdversarialRegression) {
    // Find the marker's size once.
    std::size_t markerTotal = 0;
    {
        const auto dir = tempDir();
        {
            Log log(dir.string(), /*segmentBytes=*/1 << 20);
            log.recover();
            (void)buildDamageLog(log);
            const auto at = findBatch(log.dir(), 7);
            ASSERT_TRUE(at.has_value());
            markerTotal = at->total;
        }
        std::filesystem::remove_all(dir);
    }
    ASSERT_GT(markerTotal, static_cast<std::size_t>(rune::protocol::batch::kRecordBatchOverhead));
    int cases = 0, rewrote = 0;
    for (const Path path : {Path::Range, Path::RangeCoalesce}) {
        // 0: xor byte i with 0x5A; 1: cut the segment at marker start + i; 2: xor byte i with 0x01 (a one-bit flip: an entry's
        // offset delta 5 becomes 4, so a marker trusted without its CRC would reject the live record 5 instead of 6).
        for (int mode = 0; mode < 3; ++mode) {
            for (std::size_t i = (mode == 1 ? 1 : 0); i < markerTotal; ++i) {
                SCOPED_TRACE(std::string(path == Path::Range ? "range" : "coalesce") + (mode == 1 ? " cut " : " xor ") + std::to_string(mode) + " at " + std::to_string(i));
                const auto dir = tempDir();
                {
                    // ~900 B segments: the marker shares its segment with data before and after it.
                    Log log(dir.string(), /*segmentBytes=*/900);
                    log.recover();
                    const std::set<int64_t> written = buildDamageLog(log);
                    const auto bases = log.segmentBaseOffsets();
                    ASSERT_GE(bases.size(), 3u);
                    const auto at = findBatch(log.dir(), 7);
                    ASSERT_TRUE(at.has_value());
                    if (mode != 1) {
                        xorByteAt(at->file, at->pos + i, mode == 0 ? 0x5A : 0x01);
                    } else {
                        std::filesystem::resize_file(at->file, at->pos + i);   // a torn segment tail inside the marker
                    }
                    const std::set<int64_t> before = diskDataOffsets(log.dir());
                    ASSERT_TRUE(before.count(39)) << "the later segments are intact";
                    if (mode != 1 && (i < 8 || i >= 12)) ASSERT_TRUE(before.count(8)) << "the record right behind the damaged marker is on disk";
                    cleanFilesOnly(log.dir(), bases, path, /*verdictBound=*/kMax, /*markerEnd=*/8);
                    const std::set<int64_t> after = diskDataOffsets(log.dir());
                    // A flip outside the CRC-covered region and the checked framing fields leaves the marker's content intact:
                    // baseOffset [0, 8) and partitionLeaderEpoch [12, 16) are CRC-exempt by the batch format.
                    const bool stillIntact = mode != 1 && (i < 8 || (i >= 12 && i < 16));
                    for (const int64_t o : before) {
                        if (o == 1) continue;                    // superseded by 5: may be compacted
                        if (o == 6 && stillIntact) continue;     // the intact marker rejects it
                        EXPECT_TRUE(after.count(o)) << "data offset " << o << " was on disk before the pass and is gone after it";
                    }
                    if (after.count(1) == 0) ++rewrote;
                    ++cases;
                }
                std::filesystem::remove_all(dir);
                if (::testing::Test::HasFailure()) return;
            }
        }
    }
    EXPECT_EQ(cases, static_cast<int>(2 * (3 * markerTotal - 1)));
}

// The cleaner's walk stops at the first damaged batch (A3).  When an earlier record of the same segment IS cleaned away (no
// verdict source: plain key compaction), the rewritten segment must still carry every intact batch behind the damage —
// a rewrite that keeps only the walked prefix silently drops them.  Flip each byte of a mid-segment data batch in turn.
// URGENT break found by the batch 1 adversarial pass (silent loss of intact records): every engine's walk `break`s at the
// damaged batch and installs the prefix it walked.  Fixed: a damaged walk installs nothing.
TEST(CheckedWritesVerdictCleanerRegression, DamagedBatchMidSegmentNeverTruncatesTheRewriteAdversarialRegression) {
    std::size_t victimTotal = 0;
    constexpr int64_t kVictim = 6;
    auto build = [](Log& log) {
        data(log, 0, "dup", "old");
        for (int64_t o = 1; o < 4; ++o) data(log, o, "pad" + std::to_string(o), "p");
        data(log, 4, "dup", "new");
        data(log, 5, "x5", "v");
        data(log, kVictim, "victim", "v");
        for (int64_t o = 7; o < 40; ++o) data(log, o, "after" + std::to_string(o), "a");
    };
    {
        const auto dir = tempDir();
        {
            Log log(dir.string(), 1 << 20);
            log.recover();
            build(log);
            victimTotal = findBatch(log.dir(), kVictim)->total;
        }
        std::filesystem::remove_all(dir);
    }
    int cases = 0, rewrote = 0;
    std::array<int, 4> lostCases{};
    std::array<std::string, 4> firstLost{};
    // Engines: 0 range, 1 range + coalesce, 2 one file (produceCleanedFileBounded), 3 one group (produceCleanedGroupBounded).
    for (int engine = 0; engine < 4; ++engine) {
        // i == victimTotal is the CONTROL: no flip.  Since #839 a flip of the un-CRC-covered header bytes (baseOffset, magic) is
        // damage too, so every flip leaves the log uncleanable; the control keeps the "the pass really rewrites" guard non-vacuous.
        for (std::size_t i = 0; i <= victimTotal; ++i) {
            SCOPED_TRACE("engine " + std::to_string(engine) + " flip " + std::to_string(i));
            const auto dir = tempDir();
            {
                Log log(dir.string(), /*segmentBytes=*/900);
                log.recover();
                build(log);
                const auto at = findBatch(log.dir(), kVictim);
                ASSERT_TRUE(at.has_value());
                ASSERT_EQ(std::stoll(at->file.stem().string()), 0) << "the victim shares the first segment with the dup pair";
                if (i < victimTotal) xorByteAt(at->file, at->pos + i, 0x5A);
                const std::set<int64_t> before = diskDataOffsets(log.dir());
                if (i < 8 || i >= 12) ASSERT_TRUE(before.count(kVictim + 1)) << "the record right behind the damaged batch is on disk";
                if (engine < 2) {
                    cleanFilesOnly(log.dir(), log.segmentBaseOffsets(), engine == 0 ? Path::Range : Path::RangeCoalesce, kMax, kMax);
                } else {
                    auto bases = log.segmentBaseOffsets();
                    const int64_t active = bases.back();
                    bases.pop_back();
                    ASSERT_GE(bases.size(), 2u);
                    LogCleaner cleaner;
                    if (engine == 3) {
                        rune::storage::SegmentGroup g;
                        g.bases = {bases[0], bases[1]};
                        g.combinedBytes = 1 << 20;
                        (void)cleaner.produceCleanedGroupBounded(log.dir(), bases, g, active, 0, 0, 1000, 1 << 20, nullptr, 1'000'000);
                    } else {
                        (void)cleaner.produceCleanedFileBounded(log.dir(), bases, bases[0], active, 0, 0, 1000, 1 << 20, nullptr, 1'000'000);
                    }
                }
                const std::set<int64_t> after = diskDataOffsets(log.dir());
                for (const int64_t o : before) {
                    if (o == 0 || after.count(o) != 0) continue;   // 0: superseded by 4
                    ++lostCases[engine];
                    if (firstLost[engine].empty())
                        firstLost[engine] = "flip at byte " + std::to_string(i) + ": offset " + std::to_string(o) + " (and every later one of the segment)";
                    break;
                }
                if (after.count(0) == 0) ++rewrote;
                ++cases;
            }
            std::filesystem::remove_all(dir);
            if (::testing::Test::HasFatalFailure()) return;
        }
    }
    EXPECT_EQ(cases, static_cast<int>(4 * (victimTotal + 1)));
    EXPECT_GT(rewrote, 0) << "the pass must really rewrite the damaged segment";
    for (int e = 0; e < 4; ++e)
        EXPECT_EQ(lostCases[e], 0) << "engine " << e << ": " << lostCases[e] << " of " << victimTotal << " damage cases lost intact data; first: " << firstLost[e];
}

// A damaged segment makes the log uncleanable until restart (Kafka: a corrupt segment is never truncated by the cleaner).
// After the first pass that meets the damage, the same cleaner reads and changes nothing, even for a healthy segment that a
// healthy cleaner would compact; a fresh cleaner (restart) compacts that segment and meets the damage again.
TEST(CheckedWritesVerdictCleanerRegression, ADamagedSegmentMakesTheLogUncleanableUntilRestartRegression) {
    const auto dir = tempDir();
    {
        Log log(dir.string(), /*segmentBytes=*/900);
        log.recover();
        for (int64_t o = 0; o < 40; ++o) data(log, o, "k" + std::to_string(o), "v");
        data(log, 40, "zz", "old");
        data(log, 41, "zz", "new");
        for (int64_t o = 42; o < 90; ++o) data(log, o, "f" + std::to_string(o), "v");
        const auto victim = findBatch(log.dir(), 6);
        const auto dup    = findBatch(log.dir(), 40);
        ASSERT_TRUE(victim.has_value() && dup.has_value());
        ASSERT_NE(victim->file, dup->file) << "the duplicate pair lives in a healthy segment";
        xorByteAt(victim->file, victim->pos + 20, 0x5A);
        const int64_t victimBase = std::stoll(victim->file.stem().string());
        const int64_t dupBase    = std::stoll(dup->file.stem().string());
        auto bases = log.segmentBaseOffsets();
        const int64_t active = bases.back();
        bases.pop_back();
        ASSERT_LT(dupBase, active);
        auto bytes = [](const std::filesystem::path& f) {
            std::ifstream in(f, std::ios::binary);
            return std::vector<char>((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
        };
        const auto victimBytes = bytes(victim->file);
        const auto dupBytes    = bytes(dup->file);
        auto pass = [&](LogCleaner& c, int64_t base) {
            return c.produceCleanedFileBounded(log.dir(), bases, base, active, 0, 0, 1000, 1 << 20, nullptr, 1'000'000);
        };

        LogCleaner cleaner;
        EXPECT_FALSE(cleaner.uncleanable());
        EXPECT_FALSE(cleaner.damage().has_value());
        EXPECT_FALSE(pass(cleaner, victimBase).compacted);
        ASSERT_TRUE(cleaner.uncleanable());
        ASSERT_TRUE(cleaner.damage().has_value());
        EXPECT_EQ(cleaner.damage()->segBase, victimBase);
        EXPECT_GE(cleaner.damage()->byteOffset, victim->pos) << "the walk stopped at or before the damaged batch's end";
        EXPECT_LT(cleaner.damage()->byteOffset, victim->pos + victim->total + 1);
        EXPECT_EQ(bytes(victim->file), victimBytes) << "the damaged segment is never rewritten";

        // Second pass, healthy segment: the sticky state reads and changes nothing.
        EXPECT_FALSE(pass(cleaner, dupBase).compacted);
        EXPECT_EQ(bytes(dup->file), dupBytes);
        EXPECT_TRUE(cleaner.uncleanable());

        // Restart: a fresh cleaner's pre-pass walks the whole dirty range, meets the damage and refuses the whole pass
        // (even for a healthy target segment: it never cleans against partial txn/verdict state).
        LogCleaner restarted;
        EXPECT_FALSE(restarted.uncleanable());
        EXPECT_FALSE(pass(restarted, dupBase).compacted) << "the pass over a log with a damaged segment is refused whole";
        EXPECT_EQ(bytes(dup->file), dupBytes);
        EXPECT_TRUE(restarted.uncleanable());
        EXPECT_EQ(restarted.damage()->segBase, victimBase);
        EXPECT_FALSE(pass(restarted, victimBase).compacted);
        EXPECT_EQ(bytes(victim->file), victimBytes);
    }
    std::filesystem::remove_all(dir);
}

// ===========================================================================
// Batch 1 fix-round re-attack: the pre-pass (transaction / verdict scan) and the offset-map build stop at a damaged batch
// and the pass goes on.  A record or marker the cleaner must keep is judged by state those scans build: whatever lies
// behind the damage (a COMMIT/ABORT marker, a verdict marker, a txn data batch) is missing from it.
// ===========================================================================
namespace {
constexpr int64_t kTxnPid = 4242;
void txnData(Log& log, int64_t off, const std::string& key, const std::string& val, int32_t seq) {
    rune::protocol::RecordBatchBuilder b(off, 2000 + off);
    b.setProducerId(kTxnPid);
    b.setProducerEpoch(3);
    b.setBaseSequence(seq);
    b.setTransactional(true);
    b.append(key, val, 2000 + off);
    log.append(b.build(), off);
}
void txnMarker(Log& log, int64_t off, bool abort) {
    auto b = rune::transaction::ControlMarker::build(kTxnPid, 3, abort ? rune::transaction::ControlType::Abort
                                                                        : rune::transaction::ControlType::Commit, 0);
    stampBase(b, off);
    log.append(b, off);
}
int64_t baseOf(const std::filesystem::path& f) { return std::stoll(f.stem().string()); }

// What the log holds once the pass's outputs are installed: data record offsets, and control batch offsets with whether
// they carry a delete horizon.  Deferred-install outputs replace (or, with no output, remove) their members' files.
struct DiskView {
    std::set<int64_t> data;
    std::map<int64_t, bool> control;   // offset -> horizon stamped
};
void scanFile(const std::filesystem::path& p, DiskView& v) {
    std::vector<uint8_t> buf(std::filesystem::file_size(p));
    {
        std::ifstream in(p, std::ios::binary);
        in.read(reinterpret_cast<char*>(buf.data()), static_cast<std::streamsize>(buf.size()));
    }
    for (std::size_t pos = 0; pos + rune::protocol::batch::kRecordBatchOverhead <= buf.size();) {
        rune::protocol::RecordBatchReader r(buf.data() + pos, buf.size() - pos);
        const std::size_t total = static_cast<std::size_t>(r.totalLength());
        if (total < rune::protocol::batch::kRecordBatchOverhead || pos + total > buf.size()) break;
        if ((r.attributes() & rune::protocol::batch::kAttrControl) != 0) {
            v.control[r.baseOffset()] = r.hasDeleteHorizon();
        } else if (r.ensureValid()) {
            r.forEachRecord([&](std::string_view, std::string_view, int64_t o, int64_t) { v.data.insert(o); });
        }
        pos += total;
    }
}
DiskView diskView(const std::string& dir, const std::vector<rune::storage::CleanedGroup>& deferred = {}) {
    std::map<int64_t, std::filesystem::path> replace;
    std::set<int64_t> removed;
    for (const auto& g : deferred) {
        for (std::size_t i = 0; i < g.bases.size(); ++i) {
            if (i == 0 && g.hasOutput) replace[g.bases[0]] = Log::cleanedLogPath(dir, g.bases[0], g.seq);
            else removed.insert(g.bases[i]);
        }
    }
    DiskView v;
    for (const auto& e : std::filesystem::directory_iterator(dir)) {
        if (e.path().extension() != ".log") continue;
        const int64_t b = baseOf(e.path());
        if (removed.count(b)) continue;
        scanFile(replace.count(b) ? std::filesystem::path(replace[b]) : e.path(), v);
    }
    return v;
}

// Engines: 0 range, 1 range + coalesce (one group per segment), 2 range in deferred-install mode, 3 compact (the sync
// path), 4 one file (produceCleanedFileBounded at `target`), 5 one group (produceCleanedGroupBounded {target}).
constexpr int kEngines = 6;
const char* engineName(int e) {
    static const char* n[] = {"range", "range+coalesce", "range deferred", "compact", "one file", "one group"};
    return n[e];
}
DiskView runEngine(int engine, Log& log, int64_t target, int64_t nowMs, int64_t verdictBound, int64_t markerEnd) {
    auto bases = log.segmentBaseOffsets();
    const int64_t active = bases.back();
    bases.pop_back();
    LogCleaner cleaner;
    cleaner.setMarkerRemovalBound(kMax);
    cleaner.setVerdictBounds(verdictBound, markerEnd);
    uint64_t seq = 1;
    std::vector<rune::storage::CleanedGroup> deferred;
    if (engine <= 2) {
        if (engine == 2) cleaner.setDeferredInstall(&seq);
        std::vector<int64_t> sizes;
        for (const int64_t b : bases) sizes.push_back(int64_t(std::filesystem::file_size(log.dir() + "/" + [&] {
            std::ostringstream os; os << std::setw(20) << std::setfill('0') << b << ".log"; return os.str(); }())));
        SkimpyOffsetMap map(1024);
        auto res = cleaner.produceCleanedRangeBounded(log.dir(), bases, sizes, active, 0, 0, 1000, map, engine == 1,
                                                      /*groupRollBytes=*/1, nullptr, nowMs);
        deferred = std::move(res.groups);
    } else if (engine == 3) {
        cleaner.clean(log, 0, active, 0, 1000, nowMs);
    } else if (engine == 4) {
        (void)cleaner.produceCleanedFileBounded(log.dir(), bases, target, active, 0, 0, 1000, 1 << 20, nullptr, nowMs);
    } else {
        rune::storage::SegmentGroup g;
        g.bases = {target};
        g.combinedBytes = 1 << 20;
        (void)cleaner.produceCleanedGroupBounded(log.dir(), bases, g, active, 0, 0, 1000, 1 << 20, nullptr, nowMs);
    }
    return diskView(log.dir(), deferred);
}
}  // namespace

// The ABORT marker of a transaction sits behind a damaged batch; the producer later commits another transaction.  The
// txn pre-pass never sees the ABORT, so it merges both transactions into one COMMITTED one: the aborted value of `k` is
// indexed as the latest and the committed value of `k` in an earlier, healthy segment is dropped.
// URGENT break found by the batch 1 fix-round re-attack (silent loss of a committed record): scanTxnState and
// buildOffsetMapFromBases stop at a damaged batch without refusing the pass.
TEST(CheckedWritesVerdictCleanerRegression, DamagedBatchBeforeAnAbortMarkerNeverDropsTheCommittedValueAdversarialRegression) {
    // Run 0..5: intact (the control: nothing is lost), 6..11: the victim CRC-damaged.
    for (int run = 0; run < 2 * kEngines; ++run) {
        const int engine = run % kEngines;
        const bool damaged = run >= kEngines;
        SCOPED_TRACE(std::string(engineName(engine)) + (damaged ? " damaged" : " intact"));
        const auto dir = tempDir();
        {
            Log log(dir.string(), /*segmentBytes=*/900);
            log.recover();
            data(log, 0, "k", "committed");
            for (int64_t o = 1; o < 13; ++o) data(log, o, "a" + std::to_string(o), "v");
            txnData(log, 13, "k", "aborted", 0);
            data(log, 14, "victim", "v");
            txnMarker(log, 15, /*abort=*/true);
            for (int64_t o = 16; o < 27; ++o) data(log, o, "b" + std::to_string(o), "v");
            txnData(log, 27, "q", "committed", 1);
            txnMarker(log, 28, /*abort=*/false);
            for (int64_t o = 29; o < 66; ++o) data(log, o, "c" + std::to_string(o), "v");
            const auto k0 = findBatch(log.dir(), 0), k12 = findBatch(log.dir(), 13), v = findBatch(log.dir(), 14),
                       ab = findBatch(log.dir(), 15), cm = findBatch(log.dir(), 28);
            ASSERT_TRUE(k0 && k12 && v && ab && cm);
            ASSERT_NE(k0->file, k12->file) << "the committed value lives in an earlier segment";
            ASSERT_EQ(v->file, ab->file) << "the damage and the ABORT share a segment";
            ASSERT_NE(ab->file, cm->file);
            ASSERT_LT(baseOf(cm->file), log.segmentBaseOffsets().back()) << "the COMMIT is sealed";
            if (damaged) xorByteAt(v->file, v->pos + v->total - 1, 0x5A);   // CRC-damage the victim
            const DiskView after = runEngine(engine, log, baseOf(k0->file), 1'000'000, kMax, kMax);
            EXPECT_TRUE(after.data.count(0)) << "the committed value of k is gone; only its aborted value remains";
        }
        std::filesystem::remove_all(dir);
    }
}

// The same through the verdict pre-pass: the verdict marker that rejects k@13 sits behind a damaged batch, a later marker
// covers past it.  The scan never learns k@13 is rejected, indexes it as k's latest and drops the accepted k@0.
// URGENT break found by the batch 1 fix-round re-attack (silent loss of an accepted record), same root cause.
TEST(CheckedWritesVerdictCleanerRegression, DamagedBatchBeforeAVerdictMarkerNeverDropsTheAcceptedValueAdversarialRegression) {
    // Run 0..5: intact (the control: nothing is lost), 6..11: the victim CRC-damaged.
    for (int run = 0; run < 2 * kEngines; ++run) {
        const int engine = run % kEngines;
        const bool damaged = run >= kEngines;
        SCOPED_TRACE(std::string(engineName(engine)) + (damaged ? " damaged" : " intact"));
        const auto dir = tempDir();
        {
            Log log(dir.string(), /*segmentBytes=*/900);
            log.recover();
            data(log, 0, "k", "accepted");
            for (int64_t o = 1; o < 13; ++o) data(log, o, "a" + std::to_string(o), "v");
            data(log, 13, "k", "rejected");
            data(log, 14, "victim", "v");
            marker(log, 15, 0, 16, {13});
            for (int64_t o = 16; o < 27; ++o) data(log, o, "b" + std::to_string(o), "v");
            marker(log, 27, 16, 28, {});
            for (int64_t o = 28; o < 66; ++o) data(log, o, "c" + std::to_string(o), "v");
            const auto k0 = findBatch(log.dir(), 0), k12 = findBatch(log.dir(), 13), v = findBatch(log.dir(), 14),
                       m1 = findBatch(log.dir(), 15), m2 = findBatch(log.dir(), 27);
            ASSERT_TRUE(k0 && k12 && v && m1 && m2);
            ASSERT_NE(k0->file, k12->file);
            ASSERT_EQ(v->file, m1->file);
            ASSERT_NE(m1->file, m2->file);
            ASSERT_LT(baseOf(m2->file), log.segmentBaseOffsets().back());
            if (damaged) xorByteAt(v->file, v->pos + v->total - 1, 0x5A);
            const DiskView after = runEngine(engine, log, baseOf(k0->file), 1'000'000, kMax, kMax);
            EXPECT_TRUE(after.data.count(0)) << "the accepted value of k is gone; only its rejected value remains";
        }
        std::filesystem::remove_all(dir);
    }
}

// A transaction's data batch sits behind a damaged batch; its ABORT marker is in the next segment.  The pre-pass sees a
// marker with no data and makes it removable: a pass over the marker's segment stamps it, a later one drops it, while the
// aborted data it judges is still on disk (it then reads as an open, or committed, transaction).
// The range and compact engines rewrite the damaged segment first and stop there; the one-file / one-group engines
// target the marker's segment directly.
// URGENT break found by the batch 1 fix-round re-attack (an aborted txn's marker removed while its data stays), same root
// cause.
TEST(CheckedWritesVerdictCleanerRegression, DamagedTxnDataBeforeItsMarkerNeverRemovesTheMarkerAdversarialRegression) {
    // Run 0..5: intact (the control: nothing is lost), 6..11: the victim CRC-damaged.
    for (int run = 0; run < 2 * kEngines; ++run) {
        const int engine = run % kEngines;
        const bool damaged = run >= kEngines;
        if (engine == 3) continue;   // compact reloads the log, whose recovery truncates at the damage
        SCOPED_TRACE(std::string(engineName(engine)) + (damaged ? " damaged" : " intact"));
        const auto dir = tempDir();
        {
            Log log(dir.string(), /*segmentBytes=*/900);
            log.recover();
            for (int64_t o = 0; o < 13; ++o) data(log, o, "a" + std::to_string(o), "v");
            data(log, 13, "victim", "v");
            txnData(log, 14, "t", "aborted", 0);
            for (int64_t o = 15; o < 27; ++o) data(log, o, "b" + std::to_string(o), "v");
            txnMarker(log, 27, /*abort=*/true);
            for (int64_t o = 28; o < 34; ++o) data(log, o, "c" + std::to_string(o), "v");
            txnData(log, 34, "q", "committed", 1);
            txnMarker(log, 35, /*abort=*/false);
            for (int64_t o = 36; o < 72; ++o) data(log, o, "d" + std::to_string(o), "v");
            const auto v = findBatch(log.dir(), 13), d = findBatch(log.dir(), 14), ab = findBatch(log.dir(), 27),
                       cm = findBatch(log.dir(), 35);
            ASSERT_TRUE(v && d && ab && cm);
            ASSERT_EQ(v->file, d->file);
            ASSERT_NE(d->file, ab->file);
            ASSERT_LT(baseOf(cm->file), log.segmentBaseOffsets().back());
            if (damaged) xorByteAt(v->file, v->pos + v->total - 1, 0x5A);
            (void)runEngine(engine, log, baseOf(ab->file), 1'000'000, kMax, kMax);           // pass 1: stamp
            const DiskView after = runEngine(engine, log, baseOf(ab->file), 1'000'000 + 5000, kMax, kMax);   // pass 2: drop
            if (damaged) ASSERT_TRUE(after.data.count(14)) << "the damaged segment is never rewritten: the aborted data stays";
            if (after.data.count(14) != 0) {   // (intact: the cleaner discarded the aborted data, then the marker may go)
                EXPECT_TRUE(after.control.count(27)) << "the ABORT marker of data still on disk was removed";
                if (after.control.count(27)) EXPECT_FALSE(after.control.at(27)) << "the ABORT marker of data still on disk was stamped for removal";
            }
        }
        std::filesystem::remove_all(dir);
    }
}

// A range pass that stops at a damaged segment must not report the range past it as cleaned: the agent advances (and
// persists) the clean watermark to passEnd, so the dirty records of the damaged segment and of every segment after it
// would never be indexed again.  The agent builds a fresh LogCleaner every pass (no sticky state reaches it), so passEnd is
// all it has.  Deferred-install mode, as in production.
// Non-urgent break (under-cleaning, no loss) found by the batch 1 fix-round re-attack: #840.
TEST(CheckedWritesVerdictCleanerRegression, ARangePassStoppedByDamageNeverReportsThePassEndPastItAdversarialRegression) {
    const auto dir = tempDir();
    {
        Log log(dir.string(), /*segmentBytes=*/900);
        log.recover();
        for (int64_t o = 0; o < 60; ++o) data(log, o, "k" + std::to_string(o % 7), "v" + std::to_string(o));
        const auto v = findBatch(log.dir(), 20);
        ASSERT_TRUE(v.has_value());
        const int64_t damagedBase = baseOf(v->file);
        ASSERT_GT(damagedBase, 0);
        xorByteAt(v->file, v->pos + v->total - 1, 0x5A);
        auto bases = log.segmentBaseOffsets();
        const int64_t active = bases.back();
        bases.pop_back();
        std::vector<int64_t> sizes(bases.size(), 1);
        for (int pass = 0; pass < 2; ++pass) {   // pass 1 = the next agent cycle: a fresh cleaner again
            SCOPED_TRACE("pass " + std::to_string(pass));
            LogCleaner cleaner;
            uint64_t seq = 1;
            cleaner.setDeferredInstall(&seq);
            SkimpyOffsetMap map(1024);
            const auto res = cleaner.produceCleanedRangeBounded(log.dir(), bases, sizes, active, 0, 0, 1000, map, false, 1 << 20, nullptr, 1'000'000);
            for (const auto& g : res.groups)
                if (g.hasOutput) std::filesystem::remove(Log::cleanedLogPath(log.dir(), g.bases.front(), g.seq));
            EXPECT_TRUE(cleaner.uncleanable());
            EXPECT_LE(res.passEnd, damagedBase) << "the watermark would skip the damaged segment's dirty records";
        }
    }
    std::filesystem::remove_all(dir);
}

namespace {
// offset -> key of every intact data record once the deferred outputs are installed.
std::map<int64_t, std::string> diskKeys(const std::string& dir, const std::vector<rune::storage::CleanedGroup>& deferred = {}) {
    std::map<int64_t, std::filesystem::path> replace;
    std::set<int64_t> removed;
    for (const auto& g : deferred)
        for (std::size_t i = 0; i < g.bases.size(); ++i) {
            if (i == 0 && g.hasOutput) replace[g.bases[0]] = Log::cleanedLogPath(dir, g.bases[0], g.seq);
            else removed.insert(g.bases[i]);
        }
    std::map<int64_t, std::string> out;
    for (const auto& e : std::filesystem::directory_iterator(dir)) {
        if (e.path().extension() != ".log") continue;
        const int64_t b = baseOf(e.path());
        if (removed.count(b)) continue;
        const auto p = replace.count(b) ? std::filesystem::path(replace[b]) : e.path();
        std::vector<uint8_t> buf(std::filesystem::file_size(p));
        {
            std::ifstream in(p, std::ios::binary);
            in.read(reinterpret_cast<char*>(buf.data()), static_cast<std::streamsize>(buf.size()));
        }
        for (std::size_t pos = 0; pos + rune::protocol::batch::kRecordBatchOverhead <= buf.size();) {
            rune::protocol::RecordBatchReader r(buf.data() + pos, buf.size() - pos);
            const std::size_t total = static_cast<std::size_t>(r.totalLength());
            if (total < rune::protocol::batch::kRecordBatchOverhead || pos + total > buf.size()) break;
            if (r.ensureValid() && (r.attributes() & rune::protocol::batch::kAttrControl) == 0)
                r.forEachRecord([&](std::string_view k, std::string_view, int64_t o, int64_t) { out[o] = std::string(k); });
            pos += total;
        }
    }
    return out;
}
}  // namespace

// Plain key compaction over a log with one damaged segment: damage in its first, middle or last batch (a CRC flip), or
// leftover bytes shorter than a batch at its end; the damaged segment first, in the middle or last of the sealed region;
// every engine, two passes with a fresh cleaner each (the agent; a restart).  Invariant: every record that was on disk is
// either still there or superseded by a LATER record of its key that is still there (outputs installed before the pass
// met the damage are consistent with the map), and the damaged segment's file is byte-identical.
TEST(CheckedWritesVerdictCleanerRegression, DamageAtEveryPlaceKeepsEveryUnsupersededRecordOnEveryEngineAdversarialRegression) {
    int cases = 0, droppedSomewhere = 0;
    for (int where = 0; where < 3; ++where) {            // damaged segment: first / middle / last sealed
        for (int kind = 0; kind < 4; ++kind) {           // 0 first batch, 1 middle batch, 2 last batch, 3 short leftover
            for (int engine = 0; engine < kEngines; ++engine) {
                if (engine == 3) continue;               // compact reloads the log: its recovery truncates at the damage
                SCOPED_TRACE("where " + std::to_string(where) + " kind " + std::to_string(kind) + " " + engineName(engine));
                const auto dir = tempDir();
                {
                    Log log(dir.string(), /*segmentBytes=*/900);
                    log.recover();
                    uint32_t rng = 7u + static_cast<uint32_t>(where * 31 + kind * 7 + engine);
                    for (int64_t o = 0; o < 80; ++o) {
                        rng = rng * 1664525u + 1013904223u;
                        data(log, o, "k" + std::to_string((rng >> 8) % 9), "v" + std::to_string(o));
                    }
                    auto bases = log.segmentBaseOffsets();
                    ASSERT_GE(bases.size(), 5u);
                    const int64_t active = bases.back();
                    const std::size_t sealed = bases.size() - 1;
                    const int64_t segBase = bases[where == 0 ? 0 : where == 1 ? sealed / 2 : sealed - 1];
                    const int64_t segEnd  = *std::upper_bound(bases.begin(), bases.end(), segBase);
                    const int64_t victim  = kind == 0 ? segBase : kind == 1 ? (segBase + segEnd) / 2 : segEnd - 1;
                    std::ostringstream os;
                    os << log.dir() << "/" << std::setw(20) << std::setfill('0') << segBase << ".log";
                    const std::filesystem::path file = os.str();
                    if (kind < 3) {
                        const auto v = findBatch(log.dir(), victim);
                        ASSERT_TRUE(v.has_value());
                        ASSERT_EQ(v->file, file);
                        xorByteAt(v->file, v->pos + v->total - 1, 0x5A);
                    } else {
                        std::ofstream app(file, std::ios::binary | std::ios::app);
                        const char junk[17] = "leftover-bytes!!";
                        app.write(junk, 17);   // shorter than one batch's overhead
                    }
                    auto readAll = [](const std::filesystem::path& f) {
                        std::ifstream in(f, std::ios::binary);
                        return std::vector<char>((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
                    };
                    const auto damagedBytes = readAll(file);
                    const auto before = diskKeys(log.dir());
                    for (int pass = 0; pass < 2; ++pass) {
                        bases = log.segmentBaseOffsets();
                        bases.pop_back();
                        LogCleaner cleaner;
                        cleaner.setMarkerRemovalBound(kMax);
                        std::vector<rune::storage::CleanedGroup> deferred;
                        uint64_t seq = 1 + static_cast<uint64_t>(pass) * 100;
                        if (engine <= 2) {
                            if (engine == 2) cleaner.setDeferredInstall(&seq);
                            std::vector<int64_t> sizes(bases.size(), 1);
                            SkimpyOffsetMap map(1024);
                            auto res = cleaner.produceCleanedRangeBounded(log.dir(), bases, sizes, active, 0, 0, 1000, map,
                                                                          engine == 1, 1 << 20, nullptr, 1'000'000);
                            deferred = std::move(res.groups);
                        } else {
                            for (const int64_t b : bases) {   // every sealed segment, the way a per-segment caller would
                                LogCleaner c;
                                c.setMarkerRemovalBound(kMax);
                                if (engine == 4) {
                                    (void)c.produceCleanedFileBounded(log.dir(), bases, b, active, 0, 0, 1000, 1 << 20, nullptr, 1'000'000);
                                } else {
                                    rune::storage::SegmentGroup g;
                                    g.bases = {b};
                                    g.combinedBytes = 1 << 20;
                                    (void)c.produceCleanedGroupBounded(log.dir(), bases, g, active, 0, 0, 1000, 1 << 20, nullptr, 1'000'000);
                                }
                            }
                        }
                        const auto after = diskKeys(log.dir(), deferred);
                        // Install the deferred outputs the way the owner would, so the next pass sees them.
                        for (const auto& g : deferred) {
                            for (std::size_t i = 0; i < g.bases.size(); ++i) {
                                std::ostringstream p;
                                p << log.dir() << "/" << std::setw(20) << std::setfill('0') << g.bases[i] << ".log";
                                if (i == 0 && g.hasOutput) std::filesystem::rename(Log::cleanedLogPath(log.dir(), g.bases[0], g.seq), p.str());
                                else std::filesystem::remove(p.str());
                            }
                        }
                        for (const auto& [o, k] : before) {
                            if (after.count(o)) continue;
                            ++droppedSomewhere;
                            bool superseded = false;
                            for (const auto& [o2, k2] : after) if (o2 > o && k2 == k) { superseded = true; break; }
                            EXPECT_TRUE(superseded) << "pass " << pass << ": offset " << o << " (key " << k
                                                    << ") was dropped with no later record of its key left";
                        }
                        EXPECT_EQ(readAll(file), damagedBytes) << "pass " << pass << ": the damaged segment was rewritten";
                    }
                    ++cases;
                }
                std::filesystem::remove_all(dir);
                if (::testing::Test::HasFailure()) return;
            }
        }
    }
    EXPECT_EQ(cases, 3 * 4 * (kEngines - 1));
    // A pre-pass walk that meets damage refuses the whole pass: nothing is dropped anywhere (healthy segments wait).
    EXPECT_EQ(droppedSomewhere, 0) << "a pass over a damaged log must be refused whole";
    // Control: the same data with no damage IS compacted, so the zero above is the refusal, not an inert workload.
    {
        const auto dir = tempDir();
        Log log(dir.string(), /*segmentBytes=*/900);
        log.recover();
        uint32_t rng = 7u;
        for (int64_t o = 0; o < 80; ++o) {
            rng = rng * 1664525u + 1013904223u;
            data(log, o, "k" + std::to_string((rng >> 8) % 9), "v" + std::to_string(o));
        }
        auto bases = log.segmentBaseOffsets();
        const int64_t active = bases.back();
        bases.pop_back();
        LogCleaner cleaner;
        std::vector<int64_t> sizes(bases.size(), 1);
        SkimpyOffsetMap map(1024);
        const auto res = cleaner.produceCleanedRangeBounded(log.dir(), bases, sizes, active, 0, 0, 1000, map, false, 1 << 20, nullptr, 1'000'000);
        EXPECT_TRUE(res.compacted) << "the intact control must compact";
        EXPECT_FALSE(cleaner.uncleanable());
        std::filesystem::remove_all(dir);
    }
}

// PR #851 review re-attack (edf94a73, #840): a middle sealed segment cut at EVERY byte length from one past its first
// batch to its full size.  A cut inside a batch is damage: the range pass (the agent's engine, deferred-install mode) is
// refused whole (uncleanable, passEnd == globalDirtyStart, no group, no output file).  A cut exactly at a batch boundary is
// a shorter, intact segment and must NOT mark the log uncleanable (a false positive would stop compaction of a healthy
// log until restart): the pass compacts.
TEST(CheckedWritesVerdictCleanerRegression, ASegmentCutAtEveryLengthRefusesOnlyTornBatchesAdversarialRegression) {
    const auto dir = tempDir();
    {
        Log log(dir.string(), /*segmentBytes=*/900);
        log.recover();
        for (int64_t o = 0; o < 80; ++o) data(log, o, "k" + std::to_string(o % 5), "v" + std::to_string(o));
        auto bases = log.segmentBaseOffsets();
        ASSERT_GE(bases.size(), 5u);
        const int64_t active = bases.back();
        bases.pop_back();
        const int64_t victimBase = bases[bases.size() / 2];
        const auto first = findBatch(log.dir(), victimBase);
        ASSERT_TRUE(first.has_value());
        const std::filesystem::path file = first->file;
        std::vector<char> whole;
        {
            std::ifstream in(file, std::ios::binary);
            whole.assign(std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>());
        }
        // Batch boundaries of the victim segment.
        std::set<std::size_t> boundaries;
        for (std::size_t pos = 0; pos + rune::protocol::batch::kRecordBatchOverhead <= whole.size();) {
            boundaries.insert(pos);
            rune::protocol::RecordBatchReader r(reinterpret_cast<const uint8_t*>(whole.data()) + pos, whole.size() - pos);
            pos += static_cast<std::size_t>(r.totalLength());
        }
        boundaries.insert(whole.size());
        ASSERT_GE(boundaries.size(), 3u) << "the victim segment needs several batches";
        int torn = 0, intact = 0;
        for (std::size_t cut = first->total + 1; cut <= whole.size(); ++cut) {
            SCOPED_TRACE("cut " + std::to_string(cut) + " of " + std::to_string(whole.size()));
            {
                std::ofstream out(file, std::ios::binary | std::ios::trunc);
                out.write(whole.data(), static_cast<std::streamsize>(cut));
            }
            LogCleaner cleaner;
            uint64_t seq = 1;
            cleaner.setDeferredInstall(&seq);
            std::vector<int64_t> sizes(bases.size(), 1);
            SkimpyOffsetMap map(1024);
            auto res = cleaner.produceCleanedRangeBounded(log.dir(), bases, sizes, active, 0, 0, 1000, map, false, 1 << 20,
                                                          nullptr, 1'000'000);
            const bool atBoundary = boundaries.count(cut) != 0;
            if (atBoundary) {
                ++intact;
                EXPECT_FALSE(cleaner.uncleanable()) << "a segment ending at a batch boundary was taken for damage";
                EXPECT_GT(res.passEnd, 0) << "the intact pass did not advance";
            } else {
                ++torn;
                EXPECT_TRUE(cleaner.uncleanable()) << "a torn batch was not noticed";
                ASSERT_TRUE(cleaner.damage().has_value());
                EXPECT_EQ(cleaner.damage()->segBase, victimBase);
                EXPECT_EQ(res.passEnd, 0) << "a refused pass reported progress";
                EXPECT_TRUE(res.groups.empty()) << "a refused pass produced output";
            }
            for (const auto& g : res.groups)
                if (g.hasOutput) std::filesystem::remove(Log::cleanedLogPath(log.dir(), g.bases.front(), g.seq));
            if (::testing::Test::HasFailure()) break;
        }
        EXPECT_GT(torn, 50);
        EXPECT_GE(intact, 2);
        std::ofstream restore(file, std::ios::binary | std::ios::trunc);
        restore.write(whole.data(), static_cast<std::streamsize>(whole.size()));
    }
    std::filesystem::remove_all(dir);
}

// F5 (#839): baseOffset and magic are not covered by the batch CRC.  The cleaner's walk treats a bad one as damage
// (the log is uncleanable, the segment is never rewritten), exactly like a CRC failure.
TEST(CheckedWritesVerdictCleanerRegression, UnCrcCoveredHeaderFlipMakesTheLogUncleanableRegression) {
    for (int c = 0; c < 2; ++c) {
        SCOPED_TRACE(c == 0 ? "baseOffset" : "magic");
        const auto dir = tempDir();
        {
            Log log(dir.string(), /*segmentBytes=*/900);
            log.recover();
            for (int64_t o = 0; o < 40; ++o) data(log, o, "k" + std::to_string(o % 3), "v");   // plenty of dead duplicates
            const auto victim = findBatch(log.dir(), 6);
            ASSERT_TRUE(victim.has_value());
            std::ifstream in(victim->file, std::ios::binary);
            std::vector<char> raw((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
            in.close();
            const std::size_t at = victim->pos + (c == 0 ? 0 : 16);
            // baseOffset 6: clear its lowest set bit (lowers it below the expected offset); magic: xor it.
            const uint8_t b = static_cast<uint8_t>(raw.at(at));
            ASSERT_NE(b, 0);
            xorByteAt(victim->file, at, c == 1 ? 0x5A : static_cast<uint8_t>(b & -b));
            const int64_t victimBase = std::stoll(victim->file.stem().string());
            auto bases = log.segmentBaseOffsets();
            const int64_t active = bases.back();
            bases.pop_back();
            auto bytes = [](const std::filesystem::path& f) {
                std::ifstream i2(f, std::ios::binary);
                return std::vector<char>((std::istreambuf_iterator<char>(i2)), std::istreambuf_iterator<char>());
            };
            const auto before = bytes(victim->file);
            LogCleaner cleaner;
            EXPECT_FALSE(cleaner.produceCleanedFileBounded(log.dir(), bases, victimBase, active, 0, 0, 1000, 1 << 20, nullptr, 1'000'000).compacted);
            EXPECT_TRUE(cleaner.uncleanable());
            EXPECT_EQ(bytes(victim->file), before) << "the damaged segment is never rewritten";
        }
        std::filesystem::remove_all(dir);
    }
}

// Coverage of the cleaner's scanEnd cut: no batch-2 mutation reddens this test.  With the BatchHeaderCursor upper bound
// the rotted batch now also ends the cleaner's valid prefix (it is refused like a CRC failure), so the offset map never
// sees the inflated offset.
// ADVERSARIAL (batch-2 union review, #839 F5 gap): a baseOffset that rots UPWARD (+2^40, byte 5) still verifies and passes
// BatchHeaderCursor (it only refuses a base that went down).  Mid-segment, the NEXT batch then looks like damage; but on the
// LAST batch of a sealed segment nothing follows it, so the cleaner's offset map sees that key at offset 2^40+o - "newer"
// than every real record of the key - and the pass deletes the genuinely newest one.  Every old duplicate 0..36 is rotted
// in turn (fresh log each); the newest k0 / k1 / k2 (39 / 37 / 38) must always survive.
TEST(CheckedWritesVerdictCleanerRegression, UpwardBaseOffsetRotNeverDropsTheNewestRecordAdversarialRegression) {
    int lastInSegmentCases = 0;
    for (int64_t victimOff = 0; victimOff <= 36; ++victimOff) {
        SCOPED_TRACE("rotted offset " + std::to_string(victimOff));
        const auto dir = tempDir();
        {
            Log log(dir.string(), /*segmentBytes=*/900);
            log.recover();
            for (int64_t o = 0; o < 40; ++o) data(log, o, "k" + std::to_string(o % 3), "v" + std::to_string(o));
            for (int64_t o = 40; o < 80; ++o) data(log, o, "u" + std::to_string(o), "pad");   // the newest k* are sealed
            log.sync();
            const auto bases = log.segmentBaseOffsets();
            ASSERT_GE(bases.size(), 3u);
            const auto victim = findBatch(log.dir(), victimOff);
            ASSERT_TRUE(victim.has_value());
            if (victim->pos + victim->total == std::filesystem::file_size(victim->file)) ++lastInSegmentCases;
            const auto newest = findBatch(log.dir(), 39);
            ASSERT_TRUE(newest.has_value());
            ASSERT_LT(std::stoll(newest->file.stem().string()), bases.back()) << "premise: k0@39 is in a sealed segment";
            xorByteAt(victim->file, victim->pos + 5, 0x01);
            cleanFilesOnly(log.dir(), bases, Path::Range, kMax, kMax);
            const std::set<int64_t> after = diskDataOffsets(log.dir());
            const int64_t newestOfKey = victimOff % 3 == 0 ? 39 : victimOff % 3 == 1 ? 37 : 38;
            EXPECT_TRUE(after.count(newestOfKey)) << "the newest record of k" << victimOff % 3 << " (offset " << newestOfKey
                                                  << ") was deleted: a rotted batch claimed a higher offset";
        }
        std::filesystem::remove_all(dir);
    }
    EXPECT_GT(lastInSegmentCases, 0) << "premise: some victim is the last batch of its segment";
}
