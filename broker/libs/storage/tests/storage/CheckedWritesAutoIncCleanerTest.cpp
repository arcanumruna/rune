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
/// A one-record batch of `id` at `off`: idempotent when pid >= 0, a tombstone when `val` is empty.
void rec(Log& log, int64_t off, int64_t pid, int32_t seq, uint64_t id, const std::string& val) {
    rune::protocol::RecordBatchBuilder b(off, 2000 + off);
    if (pid >= 0) { b.setProducerId(pid); b.setProducerEpoch(0); b.setBaseSequence(seq); }
    b.append(idKey(id), val, 2000 + off);
    log.append(b.build(), off);
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
    std::vector<int64_t> sizes(bases.size(), 1);
    SkimpyOffsetMap map(1024);
    (void)cleaner.produceCleanedRangeBounded(log.dir(), bases, sizes, active, 0, 0, p.retentionMs, map, p.coalesce, 1 << 20, nullptr, p.nowMs);
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
