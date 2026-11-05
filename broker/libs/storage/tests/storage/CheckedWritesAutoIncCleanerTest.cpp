// Part 06 Task 10: the cleaner on an autoincrement partition - the latest reservation, the last 32 batches of a live producer kept
// whole, and the tombstones of their keys pinned.
#include <gtest/gtest.h>

#include <algorithm>
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
#include <rune/storage/ProducerBatchWindow.h>
#include <rune/storage/ProducerTails.h>
#include <rune/storage/SkimpyOffsetMap.h>

namespace cr = rune::storage::control_record;
using rune::storage::Log;
using rune::storage::LogCleaner;
using rune::storage::SkimpyOffsetMap;
constexpr int64_t kMax = std::numeric_limits<int64_t>::max();

namespace {
std::filesystem::path tempDir() {
    static std::atomic<int> n{0};
    auto d = std::filesystem::temp_directory_path() / ("rune_cwai_" + std::to_string(::getpid()) + "_" + std::to_string(n++));
    std::filesystem::remove_all(d);
    std::filesystem::create_directories(d);
    return d;
}
void stampBase(std::vector<uint8_t>& b, int64_t off) {
    for (int i = 0; i < 8; ++i) b[std::size_t(i)] = uint8_t(uint64_t(off) >> (8 * i));
}
std::string idKey(uint64_t id) {
    std::string k(8, '\0');
    for (int i = 0; i < 8; ++i) k[std::size_t(i)] = static_cast<char>(id >> (8 * i));
    return k;
}
/// A batch of `ids` from `off` (timestamp `ts`, default 2000 + off): idempotent when pid >= 0, tombstones when `val` is empty.
void recs(Log& log, int64_t off, int64_t pid, int32_t seq, const std::vector<uint64_t>& ids, const std::string& val, int64_t ts = -1) {
    if (ts < 0) ts = 2000 + off;
    rune::protocol::RecordBatchBuilder b(off, ts);
    if (pid >= 0) { b.setProducerId(pid); b.setProducerEpoch(0); b.setBaseSequence(seq); }
    for (const uint64_t id : ids) b.append(idKey(id), val, ts);
    log.append(b.build(), off + static_cast<int64_t>(ids.size()) - 1);
}
void rec(Log& log, int64_t off, int64_t pid, int32_t seq, uint64_t id, const std::string& val, int64_t ts = -1) {
    recs(log, off, pid, seq, {id}, val, ts);
}
void pad(Log& log, int64_t off) { rec(log, off, -1, -1, 1'000'000 + uint64_t(off), "pad"); }
void reservation(Log& log, int64_t off, uint64_t n) {
    std::vector<uint8_t> b(cr::reservationBatchBytes());
    (void)cr::buildReservation(b, n);
    stampBase(b, off);
    log.append(b, off);
}
void change(Log& log, int64_t off) {
    std::vector<cr::ChangeMember> ms{cr::ChangeMember{7, 3, 99}};
    std::vector<uint8_t> b(cr::changeBatchBytes(ms.size()));
    (void)cr::buildConstraintSetChange(b, 1, ms);
    stampBase(b, off);
    log.append(b, off);
}
void marker(Log& log, int64_t off, int64_t from, int64_t ft, std::vector<int64_t> rejected) {
    std::vector<cr::MarkerEntry> es;
    for (const int64_t r : rejected) es.push_back(cr::MarkerEntry{uint32_t(r - from), 55, 1});
    std::vector<uint8_t> b(cr::markerBatchBytes(es.size()));
    (void)cr::buildVerdictMarker(b, cr::MarkerHeader{from, ft, 1, uint32_t(es.size())}, es);
    stampBase(b, off);
    log.append(b, off);
}

struct Pass {
    int depth = 32;
    int64_t nowMs = 1'000'000;
    int64_t retentionMs = 1000;
    bool coalesce = false;
    int64_t verdictBound = kMax;
    std::size_t mapSlots = 1024;
    int64_t* dirty = nullptr;   ///< the clean watermark the pass starts from and advances, as LogCleanerAgent does; null = 0
    int64_t* progress = nullptr;   ///< with `dirty`: where the last pass ended (PartitionReplica::cleanerProgress); null = none
};
/// One range pass over the sealed region; true = the cleaner built a ProducerBatchWindow.
bool clean(Log& log, const Pass& p) {
    auto bases = log.segmentBaseOffsets();
    const int64_t active = bases.back();
    bases.pop_back();
    LogCleaner cleaner;
    cleaner.setMarkerRemovalBound(kMax);
    cleaner.setVerdictBounds(p.verdictBound, p.verdictBound);
    cleaner.setProducerTailDepth(p.depth);
    if (p.progress) cleaner.setPreviousPassEnd(*p.progress);
    std::vector<int64_t> sizes(bases.size(), 1);
    SkimpyOffsetMap map(p.mapSlots);
    const auto res = cleaner.produceCleanedRangeBounded(log.dir(), bases, sizes, active, p.dirty ? *p.dirty : 0, 0, p.retentionMs, map, p.coalesce,
                                                        1 << 20, nullptr, p.nowMs);
    if (p.dirty && res.passEnd > *p.dirty) {
        *p.dirty = res.passEnd;
        if (p.progress) *p.progress = res.passEnd;
    }
    if (p.progress && res.cleanedThrough > std::max(res.passEnd, *p.dirty)) *p.progress = res.cleanedThrough;
    log.reloadSegments();
    return cleaner.producerBatchWindowForTest() != nullptr;
}

/// Every batch in the log by base offset, as its bytes.
std::map<int64_t, std::vector<uint8_t>> batches(Log& log) {
    std::map<int64_t, std::vector<uint8_t>> out;
    for (int64_t cur = log.logStartOffset(); cur < log.logEndOffset();) {
        auto bytes = log.read(cur, 1 << 20);
        if (bytes.empty()) {   // a cleaned gap: continue at the next segment
            const auto bases = log.segmentBaseOffsets();
            const auto next = std::upper_bound(bases.begin(), bases.end(), cur);
            if (next == bases.end()) break;
            cur = *next;
            continue;
        }
        for (std::size_t pos = 0; pos + rune::protocol::batch::kRecordBatchOverhead <= bytes.size();) {
            rune::protocol::RecordBatchReader r(bytes.data() + pos, bytes.size() - pos);
            const auto total = static_cast<std::size_t>(r.totalLength());
            if (!r.ensureValid() || pos + total > bytes.size()) return out;
            out[r.baseOffset()].assign(bytes.begin() + std::ptrdiff_t(pos), bytes.begin() + std::ptrdiff_t(pos + total));
            cur = std::max(cur + 1, r.baseOffset() + r.lastOffsetDelta() + 1);
            pos += total;
        }
    }
    return out;
}
std::set<int64_t> dataOffsets(Log& log) {
    std::set<int64_t> out;
    for (const auto& [base, b] : batches(log)) {
        rune::protocol::RecordBatchReader r(b.data(), b.size());
        if ((r.attributes() & rune::protocol::batch::kAttrControl) == 0)
            r.forEachRecord([&](std::string_view, std::string_view, int64_t o, int64_t) { out.insert(o); });
    }
    return out;
}
/// The reservation records as offset -> reserved-through n.
std::map<int64_t, uint64_t> reservations(Log& log) {
    std::map<int64_t, uint64_t> out;
    for (const auto& [base, b] : batches(log))
        if (const auto n = cr::parseReservation(b.data(), b.size())) out[base] = *n;
    return out;
}

/// Producer 7 inserts ids 1-40 (offsets 0-39), producer 8 updates each (40-79), a pad seals them (80).
void insertsThenUpdates(Log& log) {
    for (int i = 0; i < 40; ++i) rec(log, i, 7, i, uint64_t(i) + 1, "insert");
    for (int i = 0; i < 40; ++i) rec(log, 40 + i, 8, i, uint64_t(i) + 1, "update");
    pad(log, 80);
}
/// Producer 7 inserts id 5 (0), a plain tombstone of 5 follows (1), a pad seals them (2).
void protectedInsertThenTombstone(Log& log) {
    rec(log, 0, 7, 0, 5, "insert");
    rec(log, 1, -1, -1, 5, "");
    pad(log, 2);
}
/// A verdict source: producer 7's batches 1-10 (ids 1-10), a marker rejecting 9, a marker superseding it, a pad.
void rejectedInAProtectedBatch(Log& log) {
    change(log, 0);
    for (int i = 1; i <= 10; ++i) rec(log, i, 7, i - 1, uint64_t(i), "insert");
    marker(log, 11, 1, 11, {9});
    marker(log, 12, 11, 12, {});
    pad(log, 13);
}
const char* name(bool coalesce) { return coalesce ? "coalesce" : "range"; }
}  // namespace

TEST(CheckedWritesAutoIncCleaner, OnlyTheLatestReservationSurvives) {
    for (const bool coalesce : {false, true}) {
        SCOPED_TRACE(name(coalesce));
        const auto dir = tempDir();
        {
            Log log(dir.string(), /*segmentBytes=*/1);
            log.recover();
            int32_t seq = 0;
            reservation(log, 0, 4096);
            for (int64_t o = 1; o < 20; ++o) rec(log, o, 7, seq++, uint64_t(o), "v");
            reservation(log, 20, 8192);
            for (int64_t o = 21; o < 40; ++o) rec(log, o, 7, seq++, uint64_t(o), "v");
            reservation(log, 40, 12288);
            for (int64_t o = 41; o < 45; ++o) rec(log, o, 7, seq++, uint64_t(o), "v");
            pad(log, 45);
            ASSERT_EQ(reservations(log).size(), 3u);
            (void)clean(log, Pass{.coalesce = coalesce});
            (void)clean(log, Pass{.nowMs = 1'000'000 + 2000, .coalesce = coalesce});   // past the delete horizon
            EXPECT_EQ(reservations(log), (std::map<int64_t, uint64_t>{{40, 12288}}));
        }
        std::filesystem::remove_all(dir);
    }
}

// Fix round 1: a type-16 record whose payload is no reservation (the leader's scan skips it) never outranks the latest one.  RED:
// the pre-pass counted it as the highest reservation, so the real latest went past its horizon and a restart reused its ids.
TEST(CheckedWritesAutoIncCleanerRegression, MalformedReservationDoesNotOutrankTheLatestRegression) {
    for (const bool coalesce : {false, true}) {
        SCOPED_TRACE(name(coalesce));
        const auto dir = tempDir();
        {
            Log log(dir.string(), /*segmentBytes=*/1);
            log.recover();
            reservation(log, 0, 4096);
            rec(log, 1, 7, 0, 1, "v");
            std::vector<uint8_t> junk(cr::detail::batchBytes(1));
            std::size_t pos = cr::detail::beginBatch(junk.data(), cr::kReservation, 1);
            junk[pos++] = cr::kPayloadVersion;   // no reserved-through field
            cr::detail::sealBatch(junk.data(), pos);
            stampBase(junk, 2);
            log.append(junk, 2);
            pad(log, 3);
            (void)clean(log, Pass{.coalesce = coalesce});
            (void)clean(log, Pass{.nowMs = 1'000'000 + 2000, .coalesce = coalesce});   // past the delete horizon
            EXPECT_EQ(reservations(log), (std::map<int64_t, uint64_t>{{0, 4096}}));
        }
        std::filesystem::remove_all(dir);
    }
}

TEST(CheckedWritesAutoIncCleaner, LastThirtyTwoBatchesOfAProducerAreKeptWhole) {
    for (const bool coalesce : {false, true}) {
        SCOPED_TRACE(name(coalesce));
        const auto dir = tempDir();
        {
            Log log(dir.string(), /*segmentBytes=*/1);
            log.recover();
            insertsThenUpdates(log);
            const auto before = batches(log);
            (void)clean(log, Pass{.coalesce = coalesce});
            const auto after = batches(log);
            const auto offs = dataOffsets(log);
            for (int64_t o = 8; o < 40; ++o) {
                ASSERT_TRUE(after.count(o)) << "insert at " << o;
                EXPECT_EQ(after.at(o), before.at(o)) << "insert at " << o << " is kept byte for byte";
            }
            for (int64_t o = 0; o < 8; ++o) EXPECT_FALSE(offs.count(o)) << "the superseded insert at " << o << " is gone";
            for (int64_t o = 40; o < 80; ++o) EXPECT_TRUE(offs.count(o)) << "update at " << o;
        }
        std::filesystem::remove_all(dir);
    }
}

TEST(CheckedWritesAutoIncCleaner, ExpiredProducerIsNotProtected) {
    for (const bool coalesce : {false, true}) {
        SCOPED_TRACE(name(coalesce));
        const auto dir = tempDir();
        {
            Log log(dir.string(), /*segmentBytes=*/1);
            log.recover();
            insertsThenUpdates(log);
            (void)clean(log, Pass{.nowMs = 1'000'000 + rune::storage::ProducerTails::kDefaultExpirationMs, .coalesce = coalesce});
            const auto offs = dataOffsets(log);
            for (int64_t o = 0; o < 40; ++o) EXPECT_FALSE(offs.count(o)) << "the superseded insert at " << o << " is gone";
            for (int64_t o = 40; o < 80; ++o) EXPECT_TRUE(offs.count(o)) << "update at " << o;
        }
        std::filesystem::remove_all(dir);
    }
}

TEST(CheckedWritesAutoIncCleaner, PinnedTombstoneOutlivesDeleteRetention) {
    for (const bool coalesce : {false, true}) {
        SCOPED_TRACE(name(coalesce));
        const auto dir = tempDir();
        {
            Log log(dir.string(), /*segmentBytes=*/1);
            log.recover();
            protectedInsertThenTombstone(log);
            (void)clean(log, Pass{.retentionMs = 0, .coalesce = coalesce});                   // stamps the horizon
            (void)clean(log, Pass{.nowMs = 1'000'001, .retentionMs = 0, .coalesce = coalesce});   // past it
            auto offs = dataOffsets(log);
            EXPECT_TRUE(offs.count(0)) << "the protected insert stays";
            EXPECT_TRUE(offs.count(1)) << "its key's tombstone is pinned past the horizon";
            for (int i = 0; i < 32; ++i) rec(log, 3 + i, 7, 1 + i, 100 + uint64_t(i), "later");
            pad(log, 35);
            (void)clean(log, Pass{.nowMs = 1'000'002, .retentionMs = 0, .coalesce = coalesce});
            offs = dataOffsets(log);
            EXPECT_FALSE(offs.count(0)) << "32 later batches unprotect the insert: superseded";
            EXPECT_FALSE(offs.count(1)) << "and the tombstone is no longer pinned";
        }
        std::filesystem::remove_all(dir);
    }
}

TEST(CheckedWritesAutoIncCleaner, RejectedRecordInAProtectedBatchIsKept) {
    for (const bool coalesce : {false, true}) {
        SCOPED_TRACE(name(coalesce));
        const auto dir = tempDir();
        {
            Log log(dir.string(), /*segmentBytes=*/1);
            log.recover();
            rejectedInAProtectedBatch(log);
            (void)clean(log, Pass{.coalesce = coalesce, .verdictBound = 13});
            (void)clean(log, Pass{.nowMs = 1'000'000 + 2000, .coalesce = coalesce, .verdictBound = 13});
            EXPECT_TRUE(dataOffsets(log).count(9)) << "the rejected record of a protected batch stays";
            const auto all = batches(log);
            ASSERT_TRUE(all.count(11)) << "the marker listing it is not removable";
            EXPECT_FALSE(rune::protocol::RecordBatchReader(all.at(11).data(), all.at(11).size()).hasDeleteHorizon()) << "nor stamped";
        }
        std::filesystem::remove_all(dir);
    }
}

TEST(CheckedWritesAutoIncCleanerRegression, DepthOneTakesTodaysPathRegression) {
    for (const bool coalesce : {false, true}) {
        SCOPED_TRACE(name(coalesce));
        {
            const auto dir = tempDir();
            {
                Log log(dir.string(), 1);
                log.recover();
                insertsThenUpdates(log);
                EXPECT_FALSE(clean(log, Pass{.depth = 1, .coalesce = coalesce})) << "depth 1 never builds a ProducerBatchWindow";
                const auto offs = dataOffsets(log);
                for (int64_t o = 0; o < 40; ++o) EXPECT_FALSE(offs.count(o)) << "the superseded insert at " << o << " is gone";
                const auto all = batches(log);
                ASSERT_TRUE(all.count(39));
                rune::protocol::RecordBatchReader tail(all.at(39).data(), all.at(39).size());
                EXPECT_EQ(tail.recordCount(), 0) << "producer 7's last batch keeps its metadata only (ProducerTails)";
                EXPECT_EQ(tail.producerId(), 7);
            }
            std::filesystem::remove_all(dir);
        }
        {
            const auto dir = tempDir();
            {
                Log log(dir.string(), 1);
                log.recover();
                protectedInsertThenTombstone(log);
                EXPECT_FALSE(clean(log, Pass{.depth = 1, .retentionMs = 0, .coalesce = coalesce}));
                EXPECT_FALSE(clean(log, Pass{.depth = 1, .nowMs = 1'000'001, .retentionMs = 0, .coalesce = coalesce}));
                const auto offs = dataOffsets(log);
                EXPECT_FALSE(offs.count(0));
                EXPECT_FALSE(offs.count(1)) << "no pin at depth 1: the tombstone expires";
            }
            std::filesystem::remove_all(dir);
        }
        {
            const auto dir = tempDir();
            {
                Log log(dir.string(), 1);
                log.recover();
                rejectedInAProtectedBatch(log);
                EXPECT_FALSE(clean(log, Pass{.depth = 1, .coalesce = coalesce, .verdictBound = 13}));
                EXPECT_FALSE(dataOffsets(log).count(9)) << "the rejected record is dropped at depth 1";
            }
            std::filesystem::remove_all(dir);
        }
    }
    LogCleaner cleaner;
    EXPECT_EQ(cleaner.producerBatchWindowForTest(), nullptr) << "the default depth is 1";
    cleaner.setProducerTailDepth(32);
    ASSERT_NE(cleaner.producerBatchWindowForTest(), nullptr);
    EXPECT_EQ(cleaner.producerBatchWindowForTest()->depth(), 32);
    cleaner.setProducerTailDepth(1);
    EXPECT_EQ(cleaner.producerBatchWindowForTest(), nullptr);
}

// Fix round 1 (finding 1): the passes start from the watermark the previous pass returned, as LogCleanerAgent's do.  RED: the pass
// that pinned the tombstone advanced the watermark past the protected insert; once 32 later batches released it, the next map no
// longer held key 5, so the insert stayed (a key absent from the map is kept) while its expired tombstone went - id 5 came back.
TEST(CheckedWritesAutoIncCleanerRegression, IncrementalPassesNeverResurrectADeletedIdRegression) {
    for (const bool coalesce : {false, true}) {
        SCOPED_TRACE(name(coalesce));
        const auto dir = tempDir();
        {
            Log log(dir.string(), /*segmentBytes=*/1);
            log.recover();
            protectedInsertThenTombstone(log);
            int64_t dirty = 0;
            const auto noInsertWithoutItsTombstone = [&](const char* when) {
                const auto offs = dataOffsets(log);
                EXPECT_TRUE(!offs.count(0) || offs.count(1)) << when << ": the insert of id 5 is live without its tombstone";
            };
            (void)clean(log, Pass{.retentionMs = 0, .coalesce = coalesce, .dirty = &dirty});
            noInsertWithoutItsTombstone("the pass that stamps the horizon");
            (void)clean(log, Pass{.nowMs = 1'000'001, .retentionMs = 0, .coalesce = coalesce, .dirty = &dirty});
            noInsertWithoutItsTombstone("a pass past the horizon, the insert protected");
            for (int i = 0; i < 32; ++i) rec(log, 3 + i, 7, 1 + i, 100 + uint64_t(i), "later");
            pad(log, 35);
            for (int i = 0; i < 3; ++i) {
                (void)clean(log, Pass{.nowMs = 1'000'002 + i, .retentionMs = 0, .coalesce = coalesce, .dirty = &dirty});
                noInsertWithoutItsTombstone("a pass after 32 later batches");
            }
            const auto offs = dataOffsets(log);
            EXPECT_FALSE(offs.count(0)) << "the released insert is superseded";
            EXPECT_FALSE(offs.count(1)) << "and its tombstone expires with it";
        }
        std::filesystem::remove_all(dir);
    }
}

// Fix round 1 (finding 1): once the window moves on, the inserts it kept are compacted like any other.  RED: the first pass moved
// the watermark past them, so no later map held their keys and they were never dropped.
TEST(CheckedWritesAutoIncCleanerRegression, IncrementalPassesCompactWhatTheWindowReleasedRegression) {
    for (const bool coalesce : {false, true}) {
        SCOPED_TRACE(name(coalesce));
        const auto dir = tempDir();
        {
            Log log(dir.string(), /*segmentBytes=*/1);
            log.recover();
            insertsThenUpdates(log);
            int64_t dirty = 0;
            (void)clean(log, Pass{.coalesce = coalesce, .dirty = &dirty});
            for (int64_t o = 8; o < 40; ++o) ASSERT_TRUE(dataOffsets(log).count(o)) << "protected insert at " << o;
            for (int i = 0; i < 32; ++i) rec(log, 81 + i, 7, 40 + i, 1001 + uint64_t(i), "later");
            for (int i = 0; i < 32; ++i) rec(log, 113 + i, 8, 40 + i, 2001 + uint64_t(i), "later");
            pad(log, 145);
            (void)clean(log, Pass{.coalesce = coalesce, .dirty = &dirty});
            std::map<std::string, int> perKey;
            for (const auto& [base, b] : batches(log)) {
                rune::protocol::RecordBatchReader r(b.data(), b.size());
                if ((r.attributes() & rune::protocol::batch::kAttrControl) == 0)
                    r.forEachRecord([&](std::string_view k, std::string_view, int64_t, int64_t) { ++perKey[std::string(k)]; });
            }
            for (uint64_t id = 1; id <= 40; ++id) EXPECT_EQ(perKey[idKey(id)], 1) << "id " << id << " converged to its update";
            const auto offs = dataOffsets(log);
            for (int64_t o = 40; o < 80; ++o) EXPECT_TRUE(offs.count(o)) << "update at " << o;
        }
        std::filesystem::remove_all(dir);
    }
}

// Fix round 1 (finding 2): the map fills inside a protected batch, so the pass boundary splits it.  RED: the batch was protected
// only when it ended below the boundary, so the record rules ran on it and dropped its superseded first record.
TEST(CheckedWritesAutoIncCleanerRegression, ProtectedBatchSplitByAFullMapIsKeptWholeRegression) {
    for (const bool coalesce : {false, true}) {
        SCOPED_TRACE(name(coalesce));
        const auto dir = tempDir();
        {
            Log log(dir.string(), /*segmentBytes=*/1);
            log.recover();
            pad(log, 0);
            recs(log, 1, 7, 0, {1, 1, 2, 3}, "v");   // offsets 1-4; a three-key map fills at key 3 (offset 4)
            pad(log, 5);
            const auto before = batches(log);
            ASSERT_TRUE(before.count(1));
            (void)clean(log, Pass{.coalesce = coalesce, .mapSlots = 3});
            const auto after = batches(log);
            EXPECT_TRUE(after.count(1) && after.at(1) == before.at(1)) << "the protected batch is kept byte for byte";
        }
        std::filesystem::remove_all(dir);
    }
}

// Fix round 1 (finding 4): past its budget the window records no more producers, and every batch is then protected whatever its own
// timestamp.  RED: an unrecorded producer's batch expired on its own maxTimestamp although the producer's last batch was recent.
TEST(CheckedWritesAutoIncCleanerRegression, OverflowedWindowProtectsAnUnrecordedProducerRegression) {
    using rune::storage::ProducerBatchWindow;
    const int64_t exp = rune::storage::ProducerTails::kDefaultExpirationMs;
    const int64_t now = 10'000 + exp;
    ProducerBatchWindow w(32);
    w.begin(ProducerBatchWindow::kBytesPerProducer + 64, now, exp);   // producer 7 and its batch fit, producer 8 does not
    w.noteBatch(7, 0, 0, now, 0, 0);
    w.noteBatch(8, 1, 1, 2000, 1, 0);   // this batch alone has expired
    w.noteBatch(8, 2, 2, now, 2, 0);
    w.finish([](int64_t, std::size_t, std::vector<uint64_t>& keys) { keys.push_back(1); return true; });
    ASSERT_TRUE(w.overflowed());
    EXPECT_TRUE(w.protects(1)) << "the unrecorded producer's superseded insert is kept";
    EXPECT_TRUE(w.pinsTombstone(idKey(5))) << "and its key's tombstone is pinned";
    EXPECT_EQ(w.lowestProtectedBase(), std::numeric_limits<int64_t>::min()) << "and the watermark is held";
}

// Fix round 1 (finding 3): the window costs its budget at most - rings grow per batch, an expired producer is dropped at finish(),
// and each kept key is held once.  RED (before): every new producer allocated all 32 ring slots, its batches' keys were copied
// twice, and expired producers stayed.
TEST(CheckedWritesAutoIncCleanerRegression, WindowStaysWithinItsBudgetRegression) {
    using rune::storage::ProducerBatchWindow;
    const int64_t exp = rune::storage::ProducerTails::kDefaultExpirationMs;
    const int64_t now = 10'000 + exp;
    const auto keysOf = [](int64_t, std::size_t pos, std::vector<uint64_t>& keys) {
        keys.push_back(pos);
        keys.push_back(pos);   // a key twice in one batch
        return true;
    };
    {
        ProducerBatchWindow w(32);
        w.begin(std::numeric_limits<std::size_t>::max(), now, exp);
        w.noteBatch(7, 0, 0, now, 0, 0);
        const std::size_t one = w.bytes();
        w.noteBatch(7, 1, 1, now, 1, 1);
        EXPECT_EQ(w.bytes() - one, one - ProducerBatchWindow::kBytesPerProducer) << "a batch costs one slot: the ring grows lazily";
        w.noteBatch(9, 2, 2, 2000, 2, 2);   // producer 9 has expired
        w.finish(keysOf);
        EXPECT_TRUE(w.protects(0) && w.protects(1));
        EXPECT_FALSE(w.protects(2)) << "the expired producer is dropped";
        EXPECT_TRUE(w.pinsTombstone(idKey(0)));
        EXPECT_FALSE(w.pinsTombstone(idKey(2)));
        EXPECT_EQ(w.bytes(), 2 * (one - ProducerBatchWindow::kBytesPerProducer) + 2 * sizeof(uint64_t))
            << "two batches and two keys, each key held once; no producer is kept past finish()";
    }
    {
        const std::size_t budget = 64 * 1024;
        ProducerBatchWindow w(32);
        w.begin(budget, now, exp);
        int64_t off = 0;
        for (int64_t pid = 0; pid < 4096 && !w.overflowed(); ++pid)
            for (int i = 0; i < 32; ++i, ++off) w.noteBatch(pid, off, off, now, off, 0);
        EXPECT_TRUE(w.overflowed()) << "4096 producers of 32 batches do not fit 64 KiB";
        EXPECT_LE(w.bytes(), budget);
        w.finish(keysOf);
        EXPECT_TRUE(w.protects(off - 1)) << "overflowed: conservative";
    }
}

// Fix round 2 (c): a watermark held by an idle producer whose map fills below the last pass's end maps from that end instead, so
// later passes compact above the held offset.  RED: every pass refilled the map from the held offset and stopped at the same
// place - the inserts superseded above it stayed.
TEST(CheckedWritesAutoIncCleanerRegression, HeldWatermarkWithAFullMapStillCompactsAboveItRegression) {
    for (const bool coalesce : {false, true}) {
        SCOPED_TRACE(name(coalesce));
        const auto dir = tempDir();
        {
            Log log(dir.string(), /*segmentBytes=*/1);
            log.recover();
            rec(log, 0, 9, 0, 900, "insert");   // an idle producer: holds the watermark at 0
            for (int i = 0; i < 20; ++i) rec(log, 1 + i, -1, -1, 10 + uint64_t(i), "a");
            for (int i = 0; i < 20; ++i) rec(log, 21 + i, -1, -1, 10 + uint64_t(i), "b");
            pad(log, 41);
            int64_t dirty = 0, progress = 0;
            for (int i = 0; i < 20; ++i) (void)clean(log, Pass{.coalesce = coalesce, .mapSlots = 4, .dirty = &dirty, .progress = &progress});
            EXPECT_EQ(dirty, 0) << "the idle producer still holds the watermark";
            const auto offs = dataOffsets(log);
            EXPECT_TRUE(offs.count(0));
            for (int64_t o = 1; o <= 20; ++o) EXPECT_FALSE(offs.count(o)) << "superseded insert at " << o;
            for (int64_t o = 21; o <= 40; ++o) EXPECT_TRUE(offs.count(o)) << "update at " << o;
        }
        std::filesystem::remove_all(dir);
    }
}

// Fix round 2 (c): a pass mapping from above the held watermark lacks the keys below its map, so a tombstone there whose insert the
// window just released is kept until a pass whose map holds the key.  RED: the tombstone went past its horizon, and the released
// insert, absent from that map, stayed live without it.
TEST(CheckedWritesAutoIncCleanerRegression, PassFromThePreviousEndKeepsTheTombstoneOfAReleasedInsertRegression) {
    for (const bool coalesce : {false, true}) {
        SCOPED_TRACE(name(coalesce));
        const auto dir = tempDir();
        {
            Log log(dir.string(), /*segmentBytes=*/1);
            log.recover();
            rec(log, 0, 9, 0, 900, "insert");   // an idle producer: holds the watermark at 0
            rec(log, 1, 7, 0, 5, "insert");
            rec(log, 2, -1, -1, 5, "");
            for (int i = 0; i < 10; ++i) rec(log, 3 + i, -1, -1, 10 + uint64_t(i), "a");
            pad(log, 13);
            int64_t dirty = 0, progress = 0;
            const auto noInsertWithoutItsTombstone = [&](const char* when) {
                const auto offs = dataOffsets(log);
                EXPECT_TRUE(!offs.count(1) || offs.count(2)) << when << ": the insert of id 5 is live without its tombstone";
            };
            (void)clean(log, Pass{.retentionMs = 0, .coalesce = coalesce, .mapSlots = 4, .dirty = &dirty, .progress = &progress});
            noInsertWithoutItsTombstone("the pass that stamps the horizon");
            ASSERT_GT(progress, 3);
            for (int i = 0; i < 32; ++i) rec(log, 14 + i, 7, 1 + i, 100 + uint64_t(i), "later");   // releases producer 7's insert
            pad(log, 46);
            for (int i = 0; i < 6; ++i) {
                (void)clean(log, Pass{.nowMs = 1'000'001 + i, .retentionMs = 0, .coalesce = coalesce, .mapSlots = 4, .dirty = &dirty,
                                      .progress = &progress});
                noInsertWithoutItsTombstone("a pass after the release");
            }
            EXPECT_EQ(dirty, 0);
        }
        std::filesystem::remove_all(dir);
    }
}

// Fix round 2 (N1): producers that expired cost the window only their ids, so a log of many of them still compacts and its
// watermark moves.  RED: past the budget the window overflowed - every batch protected and the watermark frozen.
TEST(CheckedWritesAutoIncCleanerRegression, ManyExpiredProducersDoNotOverflowTheWindowRegression) {
    const int64_t exp = rune::storage::ProducerTails::kDefaultExpirationMs;
    const int64_t now = 10'000 + exp;
    {
        using rune::storage::ProducerBatchWindow;
        ProducerBatchWindow w(32);
        w.begin(64 * 1024, now, exp);
        for (int64_t pid = 0; pid < 4096; ++pid) w.noteBatch(1000 + pid, pid, pid, 2000, pid, 0);   // expired
        w.noteBatch(7, 4096, 4096, now, 4096, 0);
        w.finish([](int64_t, std::size_t, std::vector<uint64_t>& keys) { keys.push_back(1); return true; });
        EXPECT_FALSE(w.overflowed());
        EXPECT_TRUE(w.protects(4096));
        EXPECT_FALSE(w.protects(0));
        EXPECT_LE(w.bytes(), 64u * 1024);
    }
    {
        using rune::storage::ProducerBatchWindow;
        ProducerBatchWindow w(32);
        w.begin(ProducerBatchWindow::kBytesPerProducer + 64, now, exp);
        w.noteBatch(8, 0, 0, 2000, 0, 0);   // expired
        w.noteBatch(7, 1, 1, now, 1, 0);    // evicts 8
        w.noteBatch(8, 2, 2, now, 2, 0);    // 8 again: it may be live
        EXPECT_TRUE(w.overflowed()) << "an evicted producer seen again overflows";
    }
    for (const bool coalesce : {false, true}) {
        SCOPED_TRACE(name(coalesce));
        const auto dir = tempDir();
        {
            Log log(dir.string(), /*segmentBytes=*/1);
            log.recover();
            constexpr int kExpired = 800;   // 800 producers of one batch: past the 64 KiB window
            for (int i = 0; i < kExpired; ++i) rec(log, i, 1000 + i, 0, uint64_t(i), "insert");
            for (int i = 0; i < 10; ++i) rec(log, kExpired + i, -1, -1, uint64_t(i), "update");
            rec(log, kExpired + 10, 7, 0, 999'999, "live", now);
            pad(log, kExpired + 11);
            int64_t dirty = 0, progress = 0;
            ASSERT_TRUE(clean(log, Pass{.nowMs = now, .coalesce = coalesce, .mapSlots = 4096, .dirty = &dirty, .progress = &progress}));
            EXPECT_EQ(dirty, kExpired + 10) << "the watermark moves up to the live producer's batch";
            const auto offs = dataOffsets(log);
            for (int64_t o = 0; o < 10; ++o) EXPECT_FALSE(offs.count(o)) << "superseded insert at " << o;
            EXPECT_TRUE(offs.count(kExpired + 10));
        }
        std::filesystem::remove_all(dir);
    }
}
