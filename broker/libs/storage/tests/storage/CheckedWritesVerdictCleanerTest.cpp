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
#include <set>
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
            rune::protocol::RecordBatchBuilder a(0, 2000);   // offset 0 is superseded: the batch is rebuilt without it
            a.append("k1", "old", 2000, 0);
            a.append("k2", "kept", 2001, 2);
            log.append(a.build(), 0);
            rune::protocol::RecordBatchBuilder b(2, 2002);
            b.append("k1", "new", 2002, 1);
            b.append("k3", "also", 2003, 3);
            log.append(b.build(), 2);
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
        for (std::size_t i = 0; i < victimTotal; ++i) {
            SCOPED_TRACE("engine " + std::to_string(engine) + " flip " + std::to_string(i));
            const auto dir = tempDir();
            {
                Log log(dir.string(), /*segmentBytes=*/900);
                log.recover();
                build(log);
                const auto at = findBatch(log.dir(), kVictim);
                ASSERT_TRUE(at.has_value());
                ASSERT_EQ(std::stoll(at->file.stem().string()), 0) << "the victim shares the first segment with the dup pair";
                xorByteAt(at->file, at->pos + i, 0x5A);
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
    EXPECT_EQ(cases, static_cast<int>(4 * victimTotal));
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

        // Restart: a fresh cleaner compacts the healthy segment, then meets the damage again.
        LogCleaner restarted;
        EXPECT_FALSE(restarted.uncleanable());
        EXPECT_TRUE(pass(restarted, dupBase).compacted) << "a healthy cleaner drops the superseded duplicate";
        EXPECT_NE(bytes(dup->file), dupBytes);
        EXPECT_FALSE(restarted.uncleanable()) << "the healthy segment alone is no damage";
        EXPECT_FALSE(pass(restarted, victimBase).compacted);
        EXPECT_TRUE(restarted.uncleanable());
        EXPECT_EQ(restarted.damage()->segBase, victimBase);
        EXPECT_EQ(bytes(victim->file), victimBytes);
    }
    std::filesystem::remove_all(dir);
}
