// Task 8: the cleaner's lease mode - the live grant, the live version's checkpoint, the lease tombstones.
#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <cstring>
#include <filesystem>
#include <limits>
#include <optional>
#include <set>
#include <string>
#include <unistd.h>
#include <vector>

#include <rune/protocol/BatchRebuild.h>
#include <rune/protocol/LeaseRecord.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/storage/ControlRecordLayout.h>
#include <rune/storage/Log.h>
#include <rune/storage/LogCleaner.h>
#include <rune/storage/OffsetMap.h>
#include <rune/storage/SkimpyOffsetMap.h>

namespace cr = rune::storage::control_record;
using rune::storage::CleanerTopicMode;
using rune::storage::Log;
using rune::storage::LogCleaner;
using rune::storage::SkimpyOffsetMap;
constexpr int64_t kMax = std::numeric_limits<int64_t>::max();

namespace {
std::filesystem::path tempDir() {
    static std::atomic<int> n{0};
    auto d = std::filesystem::temp_directory_path() / ("rune_lcm_" + std::to_string(::getpid()) + "_" + std::to_string(n++));
    std::filesystem::remove_all(d);
    std::filesystem::create_directories(d);
    return d;
}
void stampBase(std::vector<uint8_t>& b, int64_t off) {
    for (int i = 0; i < 8; ++i) b[std::size_t(i)] = uint8_t(uint64_t(off) >> (8 * i));
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

// Record attributes (intent bits 0-1, lease op bits 2-3).
constexpr uint8_t kGrant = 1, kUpdate = 2, kRevoke = 2 | (1 << 2), kExpire = 2 << 2, kCheckpoint = 3 << 2;

// ONE record batch at `off`: value [u16 56][u16 schemaVer][version][ttl][holder=1][remaining] + 24 user bytes (+ `extra`).
void lrec(Log& log, int64_t off, const std::string& key, uint8_t attrs, int64_t version, uint64_t ttl, uint64_t remaining,
          uint16_t schemaVer = 1, std::size_t extra = 0, int64_t ts = -1) {
    if (ts < 0) ts = 2000 + off;
    std::string v(4 + 32 + 24 + extra, '\0');
    const auto put16 = [&](std::size_t at, uint16_t x) { std::memcpy(v.data() + at, &x, 2); };
    const auto put64 = [&](std::size_t at, uint64_t x) { std::memcpy(v.data() + at, &x, 8); };
    put16(0, 56); put16(2, schemaVer);
    put64(4, static_cast<uint64_t>(version)); put64(12, ttl); put64(20, 1); put64(28, remaining);
    rune::protocol::RecordBatchBuilder b(off, ts);
    b.append(key, v, ts, attrs);
    log.append(b.build(), off);
}
void pad(Log& log, int64_t off) {   // a record of its own key
    rune::protocol::RecordBatchBuilder b(off, 2000 + off);
    b.append("pad" + std::to_string(off), "p", 2000 + off);
    log.append(b.build(), off);
}

enum class Engine { Range, RangeCoalesce, File, Group };
constexpr Engine kEngines[] = {Engine::Range, Engine::RangeCoalesce, Engine::File, Engine::Group};
const char* name(Engine e) {
    switch (e) { case Engine::Range: return "range"; case Engine::RangeCoalesce: return "coalesce"; case Engine::File: return "file"; default: return "group"; }
}

// One pass over the whole sealed region with `engine`.
void clean(Log& log, Engine engine, int64_t nowMs = 1'000'000, CleanerTopicMode mode = CleanerTopicMode::Lease,
           int64_t verdictBound = kMax, int64_t markerEnd = kMax, int64_t dirtyStart = 0) {
    auto bases = log.segmentBaseOffsets();
    const int64_t active = bases.back();
    bases.pop_back();
    LogCleaner cleaner;
    cleaner.setTopicMode(mode);
    cleaner.setVerdictBounds(verdictBound, markerEnd);
    switch (engine) {
        case Engine::Range:
        case Engine::RangeCoalesce: {
            std::vector<int64_t> sizes(bases.size(), 1);
            SkimpyOffsetMap map(1024);
            (void)cleaner.produceCleanedRangeBounded(log.dir(), bases, sizes, active, dirtyStart, 0, 1000, map, engine == Engine::RangeCoalesce,
                                                     1 << 20, nullptr, nowMs);
            break;
        }
        case Engine::File:
            for (const int64_t b : bases) (void)cleaner.produceCleanedFileBounded(log.dir(), bases, b, active, dirtyStart, 0, 1000, 1 << 20, nullptr, nowMs);
            break;
        case Engine::Group: {
            rune::storage::SegmentGroup g;
            g.bases = bases;
            g.combinedBytes = 1 << 20;
            (void)cleaner.produceCleanedGroupBounded(log.dir(), bases, g, active, dirtyStart, 0, 1000, 1 << 20, nullptr, nowMs);
            break;
        }
    }
    log.reloadSegments();
}

// The surviving offsets among `candidates` (fillers and markers are not asked about).
std::set<int64_t> kept(Log& log, std::set<int64_t> candidates) {
    std::set<int64_t> out;
    for (const int64_t o : dataOffsets(log)) if (candidates.count(o)) out.insert(o);
    return out;
}
using Set = std::set<int64_t>;
}  // namespace

// Runs `build` on a fresh log per engine, finishes it with two filler records, cleans once and compares.
template <class Build>
void expectKept(Build build, int64_t fillerAt, Set candidates, Set expected, CleanerTopicMode mode = CleanerTopicMode::Lease,
                int64_t nowMs = 1'000'000) {
    for (const Engine e : kEngines) {
        SCOPED_TRACE(name(e));
        const auto dir = tempDir();
        {
            Log log(dir.string(), /*segmentBytes=*/1);
            log.recover();
            build(log);
            pad(log, fillerAt);
            pad(log, fillerAt + 1);
            clean(log, e, nowMs, mode);
            EXPECT_EQ(kept(log, candidates), expected);
        }
        std::filesystem::remove_all(dir);
    }
}

TEST(LeaseCleanerRegression, KeepsGrantOfLiveVersionRegression) {
    const auto build = [](Log& log) {
        lrec(log, 0, "k", kGrant, 0, 5000, 5000);
        for (int64_t i = 1; i <= 5; ++i) lrec(log, i, "k", kUpdate, 0, 7000 + static_cast<uint64_t>(i), 4000);
    };
    expectKept(build, 6, {0, 1, 2, 3, 4, 5}, {0, 5});
    expectKept(build, 6, {0, 1, 2, 3, 4, 5}, {5}, CleanerTopicMode::Plain);
}

TEST(LeaseCleanerRegression, KeepsRowAndSameVersionCheckpointRegression) {
    expectKept([](Log& log) {
        lrec(log, 0, "k", kGrant, 0, 5000, 5000);
        lrec(log, 1, "k", kUpdate, 0, 5000, 4000);
        lrec(log, 2, "k", kCheckpoint, 0, 5000, 900);
        lrec(log, 3, "k", kCheckpoint, 0, 5000, 800);
    }, 4, {0, 1, 2, 3}, {0, 1, 3});
}

TEST(LeaseCleanerRegression, DropsCheckpointOfEndedLeaseRegression) {
    for (const uint8_t end : {kRevoke, kExpire}) {
        SCOPED_TRACE(end == kRevoke ? "revoke" : "expire");
        for (const Engine e : kEngines) {
            SCOPED_TRACE(name(e));
            const auto dir = tempDir();
            {
                Log log(dir.string(), 1);
                log.recover();
                lrec(log, 0, "k", kGrant, 0, 5000, 5000);
                lrec(log, 1, "k", kCheckpoint, 0, 5000, 900);
                lrec(log, 2, "k", end, 0, 5000, 0);
                pad(log, 3);
                pad(log, 4);
                clean(log, e, 1'000'000);
                EXPECT_EQ(kept(log, {0, 1, 2}), (Set{2})) << "the tombstone is kept with a fresh horizon; the grant and the checkpoint go";
                clean(log, e, 1'000'000 + 1000);
                EXPECT_EQ(kept(log, {0, 1, 2}), Set{}) << "the tombstone is past its horizon";
            }
            std::filesystem::remove_all(dir);
        }
    }
}

TEST(LeaseCleanerRegression, DropsCheckpointOfRegrantedLeaseRegression) {
    expectKept([](Log& log) {
        lrec(log, 0, "k", kGrant, 0, 5000, 5000);
        lrec(log, 1, "k", kCheckpoint, 0, 5000, 900);
        lrec(log, 2, "k", kRevoke, 0, 5000, 0);
        lrec(log, 3, "k", kGrant, 3, 6000, 6000);
        lrec(log, 4, "k", kUpdate, 3, 6000, 5000);
    }, 5, {0, 1, 2, 3, 4}, {3, 4});
}

TEST(LeaseCleanerRegression, CheckpointNeverSupersedesRowRegression) {
    expectKept([](Log& log) {
        lrec(log, 0, "k", kGrant, 0, 5000, 5000);
        lrec(log, 1, "k", kUpdate, 0, 5000, 4000);
        lrec(log, 2, "k", kCheckpoint, 0, 5000, 900);
    }, 3, {0, 1, 2}, {0, 1, 2});
    // A checkpoint alone in the dirty range above a clean-prefix row.
    for (const Engine e : kEngines) {
        SCOPED_TRACE(name(e));
        const auto dir = tempDir();
        {
            Log log(dir.string(), 1);
            log.recover();
            lrec(log, 0, "k", kGrant, 0, 5000, 5000);
            lrec(log, 1, "k", kUpdate, 0, 5000, 4000);
            pad(log, 2);
            pad(log, 3);
            clean(log, e);
            EXPECT_EQ(kept(log, {0, 1}), (Set{0, 1}));
            lrec(log, 4, "k", kCheckpoint, 0, 5000, 900);
            pad(log, 5);
            pad(log, 6);
            clean(log, e, 1'000'000, CleanerTopicMode::Lease, kMax, kMax, /*dirtyStart=*/4);   // the map holds only the checkpoint (kNoRecord)
            EXPECT_EQ(kept(log, {0, 1, 4}), (Set{0, 1, 4}));
        }
        std::filesystem::remove_all(dir);
    }
}

TEST(LeaseCleanerRegression, VersionOffsetAcrossSchemaVersionsRegression) {
    expectKept([](Log& log) {
        lrec(log, 0, "k", kGrant, 0, 5000, 5000, /*schemaVer=*/1, 0);
        lrec(log, 1, "k", kUpdate, 0, 5000, 4000, 2, 8);
        lrec(log, 2, "k", kUpdate, 0, 5000, 3000, 3, 16);
    }, 3, {0, 1, 2}, {0, 2});
}

TEST(LeaseCleanerRegression, TombstoneHorizonIsCleanerStampRegression) {
    for (const Engine e : kEngines) {
        SCOPED_TRACE(name(e));
        const auto dir = tempDir();
        {
            Log log(dir.string(), 1);
            log.recover();
            lrec(log, 0, "k", kGrant, 0, 5000, 5000, 1, 0, /*ts=*/0);
            lrec(log, 1, "k", kExpire, 0, 5000, 0, 1, 0, /*ts=*/0);
            pad(log, 2);
            pad(log, 3);
            clean(log, e, 5'000'000);
            EXPECT_EQ(kept(log, {0, 1}), (Set{1})) << "stamped, not dropped by its timestamp 0";
            clean(log, e, 5'000'999);
            EXPECT_EQ(kept(log, {0, 1}), (Set{1}));
            clean(log, e, 5'001'000);
            EXPECT_EQ(kept(log, {0, 1}), Set{});
        }
        std::filesystem::remove_all(dir);
    }
}

TEST(LeaseCleanerRegression, RejectedLeaseRecordIsDroppedRegression) {
    for (const Engine e : kEngines) {
        SCOPED_TRACE(name(e));
        const auto dir = tempDir();
        {
            Log log(dir.string(), 1);
            log.recover();
            change(log, 0, true);
            lrec(log, 1, "k", kGrant, 1, 5000, 5000);
            pad(log, 2);
            lrec(log, 3, "k", kUpdate, 1, 5000, 4000);
            pad(log, 4);
            lrec(log, 5, "k", kUpdate, 1, 5000, 3000);   // rejected
            pad(log, 6);
            marker(log, 7, 1, 7, {5});
            pad(log, 8);
            pad(log, 9);
            clean(log, e, 1'000'000, CleanerTopicMode::Lease, /*verdictBound=*/8, /*markerEnd=*/8);
            EXPECT_EQ(kept(log, {1, 3, 5}), (Set{1, 3}));
        }
        std::filesystem::remove_all(dir);
    }
}

TEST(LeaseCleanerRegression, PlainModeIgnoresLeaseBitsRegression) {
    for (const Engine e : kEngines) {
        SCOPED_TRACE(name(e));
        const auto dir = tempDir();
        {
            Log log(dir.string(), 1);
            log.recover();
            lrec(log, 0, "k", kGrant, 0, 5000, 5000);
            lrec(log, 1, "k", kCheckpoint, 0, 5000, 900);
            lrec(log, 2, "k", kRevoke, 0, 5000, 0);
            rune::protocol::RecordBatchBuilder b(3, 2003);
            b.append("j", "", 2003, 0);   // an empty value: the only Plain tombstone
            log.append(b.build(), 3);
            pad(log, 4);
            pad(log, 5);
            clean(log, e, 1'000'000, CleanerTopicMode::Plain);
            EXPECT_EQ(kept(log, {0, 1, 2, 3}), (Set{2, 3})) << "latest only; the tombstone is stamped and kept";
            clean(log, e, 1'001'000, CleanerTopicMode::Plain);
            EXPECT_EQ(kept(log, {0, 1, 2, 3}), (Set{2})) << "the empty value goes at its horizon; the revoke (non-empty) never does";
        }
        std::filesystem::remove_all(dir);
    }
}

// ---- Clean-prefix cases: part A is cleaned with dirtyStart 0, part B is appended, then a clean with dirtyStart = first offset of B.
namespace {
struct PrefixCheck { Set candidates; Set expected; };
template <class A, class B>
void prefixCase(A partA, B partB, int64_t firstB, int64_t fillerAt, const std::vector<PrefixCheck>& checks,
                const std::vector<PrefixCheck>* afterHorizon = nullptr) {
    for (const Engine e : kEngines) {
        SCOPED_TRACE(name(e));
        const auto dir = tempDir();
        {
            Log log(dir.string(), 1);
            log.recover();
            partA(log);
            clean(log, e);
            partB(log);
            pad(log, fillerAt);
            pad(log, fillerAt + 1);
            clean(log, e, 1'000'000, CleanerTopicMode::Lease, kMax, kMax, firstB);
            for (const auto& c : checks) EXPECT_EQ(kept(log, c.candidates), c.expected);
            if (afterHorizon != nullptr) {
                clean(log, e, 1'001'000, CleanerTopicMode::Lease, kMax, kMax, firstB);
                for (const auto& c : *afterHorizon) EXPECT_EQ(kept(log, c.candidates), c.expected);
            }
        }
        std::filesystem::remove_all(dir);
    }
}
}  // namespace

TEST(LeaseCleanerRegression, CleanPrefixUpdateKeepsCheckpointRegression) {
    prefixCase([](Log& l) { lrec(l, 0, "k", kGrant, 0, 5000, 5000); lrec(l, 1, "k", kUpdate, 0, 5000, 4000); lrec(l, 2, "k", kCheckpoint, 0, 5000, 900); },
               [](Log& l) { lrec(l, 3, "k", kUpdate, 0, 5000, 3000); }, 3, 4, {{{0, 1, 2, 3}, {0, 2, 3}}});
}
TEST(LeaseCleanerRegression, CleanPrefixCheckpointOnlyRegression) {
    prefixCase([](Log& l) { lrec(l, 0, "k", kGrant, 0, 5000, 5000); lrec(l, 1, "k", kUpdate, 0, 5000, 4000); },
               [](Log& l) { lrec(l, 2, "k", kCheckpoint, 0, 5000, 900); }, 2, 3, {{{0, 1, 2}, {0, 1, 2}}});
}
TEST(LeaseCleanerRegression, CleanPrefixRevokeThenHorizonRegression) {
    const std::vector<PrefixCheck> later{{{0, 1, 2, 3}, Set{}}};
    prefixCase([](Log& l) { lrec(l, 0, "k", kGrant, 0, 5000, 5000); lrec(l, 1, "k", kUpdate, 0, 5000, 4000); lrec(l, 2, "k", kCheckpoint, 0, 5000, 900); },
               [](Log& l) { lrec(l, 3, "k", kRevoke, 0, 5000, 0); }, 3, 4, {{{0, 1, 2, 3}, {3}}}, &later);
}
TEST(LeaseCleanerRegression, CleanPrefixNewerCheckpointRegression) {
    prefixCase([](Log& l) { lrec(l, 0, "k", kGrant, 0, 5000, 5000); lrec(l, 1, "k", kCheckpoint, 0, 5000, 900); },
               [](Log& l) { lrec(l, 2, "k", kCheckpoint, 0, 5000, 800); }, 2, 3, {{{0, 1, 2}, {0, 2}}});
}
TEST(LeaseCleanerRegression, CleanPrefixRegrantInDirtyRegression) {
    prefixCase([](Log& l) { lrec(l, 0, "k", kGrant, 0, 5000, 5000); lrec(l, 1, "k", kCheckpoint, 0, 5000, 900); },
               [](Log& l) {
                   lrec(l, 2, "k", kRevoke, 0, 5000, 0);
                   lrec(l, 3, "k", kGrant, 3, 6000, 6000);
                   lrec(l, 4, "k", kUpdate, 3, 6000, 5000);
               }, 2, 5, {{{0, 1, 2, 3, 4}, {3, 4}}});
}
TEST(LeaseCleanerRegression, CleanPrefixQuietKeyKeepsCheckpointRegression) {
    prefixCase([](Log& l) { lrec(l, 0, "Q", kGrant, 0, 5000, 5000); lrec(l, 1, "Q", kCheckpoint, 0, 5000, 900); lrec(l, 2, "K", kGrant, 2, 5000, 5000); },
               [](Log& l) { lrec(l, 3, "K", kUpdate, 2, 5000, 4000); }, 3, 4, {{{0, 1}, {0, 1}}, {{2, 3}, {2, 3}}});
}

TEST(LeaseCleanerRegression, CheckpointBeforeLatestUpdateKeptRegression) {
    expectKept([](Log& log) {
        lrec(log, 0, "k", kGrant, 0, 5000, 5000);
        lrec(log, 1, "k", kCheckpoint, 0, 5000, 900);
        lrec(log, 2, "k", kUpdate, 0, 5000, 4000);
    }, 3, {0, 1, 2}, {0, 1, 2});
}

TEST(LeaseCleanerRegression, OffsetMapPathsRefuseLeaseModeRegression) {
    const auto dir = tempDir();
    {
        Log log(dir.string(), 1);
        log.recover();
        lrec(log, 0, "k", kGrant, 0, 5000, 5000);
        for (int64_t i = 1; i <= 5; ++i) lrec(log, i, "k", kUpdate, 0, 7000 + static_cast<uint64_t>(i), 4000);
        pad(log, 6);
        pad(log, 7);
        const Set all{0, 1, 2, 3, 4, 5};
        const auto bases = log.segmentBaseOffsets();
        const int64_t active = bases.back();
        auto sealed = bases;
        sealed.pop_back();
        LogCleaner cleaner;
        cleaner.setTopicMode(CleanerTopicMode::Lease);
        rune::storage::OffsetMap built;
        cleaner.buildOffsetMapFromBases(log.dir(), sealed, 0, active, built);
        EXPECT_EQ(built.size(), 0u) << "nothing is mapped in lease mode";
        // rewriteSegment(OffsetMap) is private: it is reached through produceCleanedFile (build + rewrite).
        for (const int64_t b : sealed) EXPECT_FALSE(cleaner.produceCleanedFile(log.dir(), sealed, b, active, 0, 0, 1000));
        log.reloadSegments();
        EXPECT_EQ(kept(log, all), all) << "produceCleanedFile cleaned nothing";
        rune::storage::OffsetMap map;   // what a Plain build would have produced: k -> 5
        map.put("k", 5);
        for (const int64_t b : sealed)   // the private rewrite on its own, with a map that would drop offsets 0-4 in Plain mode
            EXPECT_FALSE(cleaner.rewriteSegmentForTest(log.dir(), b, map, 0, active, 0, 1000, 1'000'000));
        log.reloadSegments();
        EXPECT_EQ(kept(log, all), all) << "rewriteSegment(OffsetMap) rewrote nothing";
        cleaner.compact(log, map, 0, active, 0, 1000, 1'000'000);
        log.reloadSegments();
        EXPECT_EQ(kept(log, all), all) << "the log is untouched";
    }
    std::filesystem::remove_all(dir);
}
