// FilterGroupTest.cpp — Phase 2c Task 7: FilterGroup, the pure filter engine (member state machine, shared scan,
// rebuild delivery, position notes), driven by FakeSource / FakeSink.  No Aeron, no sleeps: time is nowNs.
#include "rune/node/filter/FilterGroup.h"

#include "support/AllocWindow.h"
#include "support/FilterFakes.h"

#include <gtest/gtest.h>
#include <spdlog/sinks/ostream_sink.h>
#include <spdlog/spdlog.h>

#include <cstdint>
#include <random>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

using namespace rune::node::filter;
using namespace rune::node::filter::fakes;
using rune::Match;
using rune::anyOf;
using rune::codec::CompiledFilter;
using rune::codec::FilterBytes;
using rune::codec::FilterSchema;
using rune::codec::FilterWriter;
using rune::codec::PrimType;

namespace {

constexpr uint64_t kSrc = 0x5EED0001;
constexpr int64_t  kT0  = 1'000'000'000'000;   // 1000 s: a clock that is never near 0

template <class Build>
CompiledFilter mk(const FilterSchema& s, Build&& build) {
    FilterBytes fb;
    FilterWriter w(std::span<uint8_t>(fb.bytes.data(), fb.bytes.size()));
    build(w);
    auto n = w.finish();
    if (!n || *n == 0) throw std::runtime_error("filter encode failed");
    auto c = CompiledFilter::compile(std::span<const uint8_t>(fb.bytes.data(), *n), s);
    if (!c) throw std::runtime_error("filter compile failed");
    return std::move(*c);
}
CompiledFilter worldEq(const FilterSchema& s, uint32_t w) {
    return mk(s, [&](FilterWriter& f) { f.term(1, PrimType::UInt32, 4, Match<uint32_t>(w)); });
}
CompiledFilter channelIn(const FilterSchema& s, std::vector<int32_t> v) {
    return mk(s, [&](FilterWriter& f) { f.term(2, PrimType::Int32, 4, Match<int32_t>(anyOf(std::span<const int32_t>(v)))); });
}
CompiledFilter channelEq(const FilterSchema& s, int32_t c) {
    return mk(s, [&](FilterWriter& f) { f.term(2, PrimType::Int32, 4, Match<int32_t>(c)); });
}
CompiledFilter worldAndChannel(const FilterSchema& s, uint32_t w, int32_t c) {
    return mk(s, [&](FilterWriter& f) {
        f.term(1, PrimType::UInt32, 4, Match<uint32_t>(w));
        f.term(2, PrimType::Int32, 4, Match<int32_t>(c));
    });
}
CompiledFilter worldAndHpIn(const FilterSchema& s, uint32_t w, std::vector<int64_t> hp) {
    return mk(s, [&](FilterWriter& f) {
        f.term(1, PrimType::UInt32, 4, Match<uint32_t>(w));
        f.term(6, PrimType::Int64, 8, Match<int64_t>(anyOf(std::span<const int64_t>(hp))));
    });
}

std::vector<PlayerRow> worlds(std::initializer_list<uint32_t> ws) {
    std::vector<PlayerRow> v;
    for (uint32_t w : ws) v.push_back(PlayerRow{.worldId = w});
    return v;
}
std::vector<int64_t> offs(std::initializer_list<int64_t> o) { return o; }

struct Rig {
    FilterConfig cfg;
    FilterGroup  g;
    FakeSource   src;
    FakeSink     sink;
    FilterScratch sc;
    FilterSchema schema;
    int64_t now = kT0;

    explicit Rig(FilterConfig c = {}, uint16_t schemaVersion = 3) : cfg(c), schema(playerSchema(schemaVersion)) {
        g.init(1, 0, cfg, kSrc);
        sc.reserve(cfg, 256);
    }
    uint32_t add(uint32_t sub, CompiledFilter f) { return g.add(sub, std::move(f), schema, kSrc); }
    void publish(const std::vector<uint8_t>& b) { src.append(b); src.publishAll(); }
    int pass(bool fresh = true) {
        std::size_t budget = std::size_t{1} << 30;
        return g.pass(src, sink, schema, sc, now, budget, fresh);
    }
    int passWith(std::size_t& budget, bool fresh) { return g.pass(src, sink, schema, sc, now, budget, fresh); }
    /// Passes until a pass does no work (hard cap 200: a hang fails instead of spinning).
    void drain() {
        for (int i = 0; i < 200; ++i) if (pass() == 0) return;
        FAIL() << "drain: still working after 200 passes";
    }
    bool inSync(uint32_t m) const { return std::holds_alternative<InSync>(g.stateOf(m)); }
};

struct LogCapture {
    std::ostringstream os;
    std::shared_ptr<spdlog::logger> old, mine;
    LogCapture() {
        mine = std::make_shared<spdlog::logger>("cap", std::make_shared<spdlog::sinks::ostream_sink_mt>(os));
        mine->set_level(spdlog::level::info);
        mine->flush_on(spdlog::level::info);
        old = spdlog::default_logger();
        spdlog::set_default_logger(mine);
    }
    ~LogCapture() { spdlog::set_default_logger(old); }
    std::size_t count(const std::string& needle) const {
        std::size_t n = 0, at = 0;
        const std::string s = os.str();
        while ((at = s.find(needle, at)) != std::string::npos) { ++n; at += needle.size(); }
        return n;
    }
};

}  // namespace

// ---------------------------------------------------------------------------------------------------------------
// The fixture itself: the test support must produce valid batches the engine's codec walks.
// ---------------------------------------------------------------------------------------------------------------
TEST(FilterGroup, FakeBatchesAreValidRecordBatches) {
    const auto b = makePlayerBatch(10, 3, worlds({1, 2, 3}));
    rune::protocol::RecordBatchReader r(b.data(), b.size());
    EXPECT_TRUE(r.ensureValid());
    EXPECT_EQ(r.baseOffset(), 10);
    EXPECT_EQ(r.lastOffsetDelta(), 2);
    EXPECT_EQ(r.recordCount(), 3);
    const auto c = makePlayerBatch(0, 3, worlds({1}), {.control = true});
    EXPECT_TRUE(rune::protocol::RecordBatchReader(c.data(), c.size()).isControl());
}

TEST(FilterGroup, NewMemberCatchesUpThenJoinsTheScan) {
    Rig r;
    r.src.append(makePlayerBatch(0, 3, worlds({7, 8, 7})));
    r.src.append(makePlayerBatch(3, 3, worlds({8, 7, 8})));
    r.src.append(makePlayerBatch(6, 3, worlds({7, 7, 8})));
    r.src.publishAll();
    const uint32_t m = r.add(0, worldEq(r.schema, 7));
    ASSERT_NE(m, kNil);
    EXPECT_TRUE(std::holds_alternative<Behind>(r.g.stateOf(m)));
    EXPECT_GT(r.pass(), 0);
    EXPECT_TRUE(r.inSync(m));
    EXPECT_EQ(r.sink.clientView(0), offs({0, 2, 4, 6, 7}));
    EXPECT_EQ(r.g.cursorOf(m, r.sink.cursor(0)), 9);
    r.publish(makePlayerBatch(9, 3, worlds({7, 8, 8})));
    r.drain();
    EXPECT_EQ(r.sink.clientView(0), offs({0, 2, 4, 6, 7, 9}));
    EXPECT_EQ(r.sink.offsetsFor(0), offs({0, 2, 4, 6, 7, 9})) << "no duplicates";
    EXPECT_EQ(r.g.cursorOf(m, 0), 12);
}

TEST(FilterGroup, AllMatchingBatchIsTheOriginalZeroCopy) {
    Rig r;
    const uint32_t m = r.add(0, worldEq(r.schema, 7));
    r.pass();
    ASSERT_TRUE(r.inSync(m));
    const auto b = makePlayerBatch(0, 3, worlds({7, 7, 7}));
    r.publish(b);
    r.pass();
    const auto frames = r.sink.dataFor(0);
    ASSERT_EQ(frames.size(), 1u);
    EXPECT_EQ(frames[0]->bytes, b) << "the original bytes, untouched";
    EXPECT_EQ(frames[0]->base, 0);
    EXPECT_EQ(r.g.groupCounters().zeroCopy, 1u);
    EXPECT_EQ(r.g.groupCounters().rebuilt, 0u);
    EXPECT_EQ(r.g.countersOf(m).zeroCopy, 1u);
}

TEST(FilterGroup, PartlyMatchingBatchIsRebuiltWithOriginalOffsets) {
    Rig r;
    const uint32_t m = r.add(0, worldEq(r.schema, 7));
    r.pass();
    ASSERT_TRUE(r.inSync(m));
    r.publish(makePlayerBatch(100, 3, worlds({7, 8, 7, 8, 7})));
    r.pass();
    ASSERT_EQ(r.sink.dataFor(0).size(), 1u);
    const Frame& f = *r.sink.dataFor(0)[0];
    rune::protocol::RecordBatchReader rd(f.bytes.data(), f.bytes.size());
    EXPECT_TRUE(rd.ensureValid());
    EXPECT_EQ(rd.baseOffset(), 100);
    EXPECT_EQ(rd.lastOffsetDelta(), 4) << "still spans the source range";
    EXPECT_EQ(rd.recordCount(), 3);
    EXPECT_EQ(r.sink.offsetsFor(0), offs({100, 102, 104}));
    EXPECT_EQ(r.g.groupCounters().rebuilt, 1u);
}

TEST(FilterGroup, NonMatchingBatchSendsNothingThenANote) {
    FilterConfig cfg;
    cfg.noteIntervalNs = 1'000'000;
    Rig r(cfg);
    const uint32_t m = r.add(0, worldEq(r.schema, 7));
    r.pass();
    ASSERT_TRUE(r.inSync(m));
    r.publish(makePlayerBatch(0, 3, worlds({1, 2, 3, 4})));
    r.pass();
    EXPECT_TRUE(r.sink.dataFor(0).empty());
    EXPECT_TRUE(r.sink.notesFor(0).empty()) << "interval not yet elapsed";
    r.now += 2'000'000;
    r.pass();
    EXPECT_TRUE(r.sink.dataFor(0).empty());
    EXPECT_EQ(r.sink.notesFor(0), (std::vector<int64_t>{4})) << "one note at the log end";
    EXPECT_EQ(r.g.countersOf(m).notes, 1u);
    r.now += 4'000'000;
    r.pass();
    EXPECT_EQ(r.sink.notesFor(0).size(), 1u) << "nothing skipped since: no second note";
}

TEST(FilterGroup, NoteByCountBeforeTheInterval) {
    FilterConfig cfg;
    cfg.noteRecords = 100;   // interval stays 50 ms
    Rig r(cfg);
    const uint32_t m = r.add(0, worldEq(r.schema, 7));
    r.pass();
    ASSERT_TRUE(r.inSync(m));
    r.publish(makePlayerBatch(0, 3, std::vector<PlayerRow>(150, PlayerRow{.worldId = 1})));
    r.pass();
    EXPECT_EQ(r.sink.notesFor(0), (std::vector<int64_t>{150})) << "150 skipped records: a note at once";
}

TEST(FilterGroup, FiveHundredMembersOneMatchEachTouchesOnlyTheMatched) {
    FilterConfig cfg;
    cfg.noteIntervalNs = int64_t{1} << 60;   // no sweep inside this test
    cfg.noteRecords = uint64_t{1} << 60;
    Rig r(cfg);
    for (uint32_t i = 0; i < 500; ++i) ASSERT_NE(r.add(i, worldEq(r.schema, i)), kNil);
    r.pass();
    EXPECT_EQ(r.g.inSyncCount(), 500u);
    const uint64_t before = r.g.slotsTouchedForTest();
    r.publish(makePlayerBatch(0, 3, worlds({3, 7})));
    r.pass();
    EXPECT_EQ(r.sink.dataFrames, 2u);
    EXPECT_EQ(r.g.groupCounters().zeroCopy + r.g.groupCounters().rebuilt, 2u);
    EXPECT_EQ(r.sink.offsetsFor(3), offs({0}));
    EXPECT_EQ(r.sink.offsetsFor(7), offs({1}));
    EXPECT_LE(r.g.slotsTouchedForTest() - before, 4u) << "2 index hits + 2 deliveries, never the other 498";
}

TEST(FilterGroup, InIndexTermRegistersEveryValueOnce) {
    Rig r(FilterConfig{}, 2);   // v1/v2 records still have channel
    const uint32_t m = r.add(0, channelIn(r.schema, {1, 2, 3}));
    r.pass();
    ASSERT_TRUE(r.inSync(m));
    r.publish(makePlayerBatch(0, 2, {PlayerRow{.channel = 9}, PlayerRow{.channel = 2}, PlayerRow{.channel = 9}}));
    r.publish(makePlayerBatch(3, 2, {PlayerRow{.channel = 1}, PlayerRow{.channel = 3}}));
    r.pass();
    EXPECT_EQ(r.sink.offsetsFor(0), offs({1, 3, 4})) << "each matching record exactly once";
}

TEST(FilterGroup, TwoIndexFieldsInOneGroup) {   // Op MixedIndexFields
    Rig r(FilterConfig{}, 2);
    const uint32_t a = r.add(0, worldEq(r.schema, 5));
    const uint32_t b = r.add(1, channelEq(r.schema, 2));
    const uint32_t c = r.add(2, worldAndChannel(r.schema, 5, 2));
    r.pass();
    ASSERT_TRUE(r.inSync(a) && r.inSync(b) && r.inSync(c));
    r.publish(makePlayerBatch(0, 2, {PlayerRow{.worldId = 5, .channel = 2}, PlayerRow{.worldId = 5, .channel = 1},
                                     PlayerRow{.worldId = 6, .channel = 2}, PlayerRow{.worldId = 7, .channel = 7}}));
    r.pass();
    EXPECT_EQ(r.sink.offsetsFor(0), offs({0, 1}));
    EXPECT_EQ(r.sink.offsetsFor(1), offs({0, 2}));
    EXPECT_EQ(r.sink.offsetsFor(2), offs({0})) << "index term world, residual channel";
}

TEST(FilterGroupRegression, BackPressuredMemberFallsBehindWithoutStallingOthersRegression) {
    Rig r;
    const uint32_t a = r.add(0, worldEq(r.schema, 7));
    const uint32_t b = r.add(1, worldEq(r.schema, 7));
    r.pass();
    ASSERT_TRUE(r.inSync(a) && r.inSync(b));
    r.sink.refuse(0, 3);
    std::vector<int64_t> all;
    for (int i = 0; i < 6; ++i) {
        r.now += 1000;
        r.publish(makePlayerBatch(i * 2, 3, worlds({7, 7})));
        all.push_back(i * 2);
        all.push_back(i * 2 + 1);
        r.pass();
        EXPECT_EQ(r.sink.offsetsFor(1), all) << "B gets every batch in the pass that published it (i=" << i << ")";
        if (i == 0) {
            EXPECT_TRUE(std::holds_alternative<Behind>(r.g.stateOf(a)));
            EXPECT_EQ(r.sink.cursor(0), 0) << "the member's own cursor was written back";
            EXPECT_EQ(r.g.countersOf(a).fellBehind, 1u);
        }
    }
    r.drain();
    EXPECT_EQ(r.sink.offsetsFor(0), all) << "A: every batch exactly once, in order";
    EXPECT_TRUE(r.inSync(a));
    EXPECT_EQ(r.g.countersOf(a).rejoined, 1u);
    EXPECT_EQ(r.g.countersOf(a).fellBehind, 1u);
    EXPECT_EQ(r.g.countersOf(b).fellBehind, 0u);
}

TEST(FilterGroupRegression, TooLargeAndUndeliverableFallBehindLikeRetryRegression) {
    Rig r;
    const uint32_t a = r.add(0, worldEq(r.schema, 7));
    const uint32_t b = r.add(1, worldEq(r.schema, 7));
    const uint32_t c = r.add(2, worldEq(r.schema, 7));
    r.pass();
    r.sink.setTooLarge(0, true);
    r.sink.setDeliverable(1, false);
    r.publish(makePlayerBatch(10, 3, worlds({7})));
    r.pass();
    EXPECT_FALSE(r.inSync(a));
    EXPECT_FALSE(r.inSync(b));
    EXPECT_TRUE(r.inSync(c));
    EXPECT_EQ(r.sink.cursor(0), 10);
    EXPECT_EQ(r.sink.cursor(1), 10);
    EXPECT_EQ(r.sink.offsetsFor(2), offs({10}));
    EXPECT_TRUE(r.sink.dataFor(0).empty());
    EXPECT_TRUE(r.sink.dataFor(1).empty());
    r.sink.setTooLarge(0, false);
    r.sink.setDeliverable(1, true);
    r.drain();
    EXPECT_EQ(r.sink.offsetsFor(0), offs({10}));
    EXPECT_EQ(r.sink.offsetsFor(1), offs({10}));
    EXPECT_TRUE(r.inSync(a) && r.inSync(b));
}

TEST(FilterGroupRegression, UnknownVersionStallsTheScanThenExcludesAfterTheWaitRegression) {
    // Instance 1: the schema is extended during the wait.
    {
        Rig r(FilterConfig{}, 1);
        const uint32_t m = r.add(0, worldEq(r.schema, 7));
        r.pass();
        ASSERT_TRUE(r.inSync(m));
        r.publish(makePlayerBatch(0, 2, worlds({7})));   // a v2 record, schema knows v1
        r.publish(makePlayerBatch(1, 1, worlds({7})));
        r.pass();
        EXPECT_TRUE(std::holds_alternative<ScanStall>(r.g.scanState()));
        EXPECT_EQ(std::get<ScanStall>(r.g.scanState()).version, 2);
        r.now += 1'000'000'000;   // 1 s < 5 s
        r.pass();
        EXPECT_TRUE(r.sink.dataFor(0).empty()) << "nothing past the stalled batch is delivered";
        EXPECT_TRUE(std::holds_alternative<ScanStall>(r.g.scanState()));
        EXPECT_EQ(r.g.cursorOf(m, 0), 0);
        ASSERT_EQ(r.schema.extendFrom(playerSchema(2)), FilterSchema::ExtendResult::Extended);
        r.pass();
        EXPECT_EQ(r.sink.offsetsFor(0), offs({0, 1}));
        EXPECT_TRUE(std::holds_alternative<Scanning>(r.g.scanState()));
        EXPECT_EQ(r.g.groupCounters().unknownVersionExcluded, 0u);
    }
    // Instance 2: the schema never arrives.
    {
        Rig r(FilterConfig{}, 1);
        const uint32_t m = r.add(0, worldEq(r.schema, 7));
        r.pass();
        ASSERT_TRUE(r.inSync(m));
        r.publish(makePlayerBatch(0, 2, worlds({7})));
        r.publish(makePlayerBatch(1, 1, worlds({7})));
        r.pass();
        r.now += 4'999'000'000;
        r.pass();
        EXPECT_TRUE(std::holds_alternative<ScanStall>(r.g.scanState()));
        r.now += 2'000'000;   // past the 5 s wait, measured from the stall start
        r.pass();
        EXPECT_EQ(r.g.groupCounters().unknownVersionExcluded, 1u);
        EXPECT_EQ(r.sink.offsetsFor(0), offs({1})) << "the unknown record is excluded, the scan moves on";
        EXPECT_TRUE(std::holds_alternative<Scanning>(r.g.scanState()));
    }
}

TEST(FilterGroupRegression, BehindMemberStallsOnUnknownVersionThenResumesRegression) {
    Rig r(FilterConfig{}, 1);
    r.src.append(makePlayerBatch(0, 1, worlds({7})));
    r.src.append(makePlayerBatch(1, 2, worlds({7})));
    r.src.append(makePlayerBatch(2, 1, worlds({7})));
    r.src.publishAll();
    const uint32_t m = r.add(0, worldEq(r.schema, 7));
    r.pass();
    ASSERT_TRUE(std::holds_alternative<VersionStall>(r.g.stateOf(m)));
    EXPECT_EQ(std::get<VersionStall>(r.g.stateOf(m)).version, 2);
    EXPECT_EQ(std::get<VersionStall>(r.g.stateOf(m)).sinceNs, kT0);
    EXPECT_EQ(r.sink.offsetsFor(0), offs({0})) << "the batch before the stall was delivered";
    EXPECT_EQ(r.sink.cursor(0), 1);
    r.now += 1'000'000'000;
    r.pass();
    EXPECT_TRUE(std::holds_alternative<VersionStall>(r.g.stateOf(m)));
    EXPECT_EQ(std::get<VersionStall>(r.g.stateOf(m)).sinceNs, kT0) << "the stall start does not move";
    ASSERT_EQ(r.schema.extendFrom(playerSchema(2)), FilterSchema::ExtendResult::Extended);
    r.drain();
    EXPECT_EQ(r.sink.offsetsFor(0), offs({0, 1, 2}));
    EXPECT_TRUE(r.inSync(m));
}

TEST(FilterGroupRegression, BehindMemberExcludesAfterTheWaitRegression) {
    Rig r(FilterConfig{}, 1);
    r.src.append(makePlayerBatch(0, 2, worlds({7, 7})));
    r.src.append(makePlayerBatch(2, 1, worlds({7})));
    r.src.publishAll();
    const uint32_t m = r.add(0, worldEq(r.schema, 7));
    r.pass();
    ASSERT_TRUE(std::holds_alternative<VersionStall>(r.g.stateOf(m)));
    r.now += 5'000'000'000;
    r.drain();
    EXPECT_EQ(r.g.countersOf(m).unknownVersionExcluded, 2u);
    EXPECT_EQ(r.sink.offsetsFor(0), offs({2}));
    EXPECT_TRUE(r.inSync(m));
}

TEST(FilterGroupRegression, AbsentIndexFieldIsCountedPerMemberRegression) {
    LogCapture log;
    Rig r;   // schema v3: channel dropped
    const uint32_t m = r.add(0, channelIn(r.schema, {1, 2}));
    const uint32_t other = r.add(1, worldEq(r.schema, 7));
    r.pass();
    ASSERT_TRUE(r.inSync(m));
    r.publish(makePlayerBatch(0, 3, {PlayerRow{.worldId = 7}, PlayerRow{.worldId = 7}, PlayerRow{.worldId = 7}}));
    r.publish(makePlayerBatch(3, 3, {PlayerRow{.worldId = 7}, PlayerRow{.worldId = 8}}));
    r.pass();
    EXPECT_EQ(r.g.countersOf(m).absentExcluded, 5u) << "one per record";
    EXPECT_EQ(r.g.countersOf(other).absentExcluded, 0u);
    EXPECT_TRUE(r.sink.dataFor(0).empty());
    EXPECT_EQ(r.sink.offsetsFor(1), offs({0, 1, 2, 3}));
    EXPECT_EQ(log.count("filter: filtered field absent: sub 0 topic 1 partition 0 tag 2 schema version 3 "
                        "(records of this version never match)"), 1u)
        << "logged once per member and version";
    // Counters survive leaving the scan (folded on the way out).
    r.sink.refuse(1, 1);
    r.publish(makePlayerBatch(5, 3, worlds({7})));
    r.pass();
    EXPECT_FALSE(r.inSync(other));
    EXPECT_EQ(r.g.countersOf(m).absentExcluded, 6u);
}

TEST(FilterGroupRegression, AbsentResidualFieldIsCountedAndLoggedOnceRegression) {
    LogCapture log;
    Rig r;   // v3: channel dropped; the filter's residual term is on channel
    const uint32_t m = r.add(0, worldAndChannel(r.schema, 5, 2));
    r.pass();
    ASSERT_TRUE(r.inSync(m));
    r.publish(makePlayerBatch(0, 3, worlds({5, 5, 6, 5})));
    r.pass();
    EXPECT_EQ(r.g.countersOf(m).absentExcluded, 3u) << "the three world-5 records reach the residual term";
    EXPECT_TRUE(r.sink.dataFor(0).empty());
    EXPECT_EQ(log.count("filter: filtered field absent: sub 0 topic 1 partition 0 tag 2 schema version 3"), 1u);
}

TEST(FilterGroupRegression, ReadCommittedSkipsAbortedAndStopsAtTheLsoRegression) {
    Rig r;
    r.src.setReadCommitted(true);
    const uint32_t early = r.add(0, worldEq(r.schema, 7));   // joins the scan before any data
    r.pass();
    ASSERT_TRUE(r.inSync(early));
    r.src.append(makePlayerBatch(0, 3, worlds({7})));                                          // plain
    r.src.append(makePlayerBatch(1, 3, worlds({7}), {.transactional = true, .producerId = 9}));   // aborted
    r.src.append(makePlayerBatch(2, 3, worlds({7}), {.transactional = true, .producerId = 10}));  // committed
    r.src.append(makePlayerBatch(3, 3, worlds({7}), {.control = true, .producerId = 10}));       // marker
    r.src.append(makePlayerBatch(4, 3, worlds({7})));                                          // beyond the LSO
    r.src.abort(9, 1);
    r.src.setBound(4);   // the LSO: batch at 4 is not yet servable
    const uint32_t late = r.add(1, worldEq(r.schema, 7));   // catches up on its own cursor
    r.drain();
    EXPECT_EQ(r.sink.offsetsFor(0), offs({0, 2})) << "scan path";
    EXPECT_EQ(r.sink.offsetsFor(1), offs({0, 2})) << "catch-up path";
    EXPECT_TRUE(r.inSync(late));
    EXPECT_EQ(r.g.cursorOf(early, 0), 4);
    r.src.setBound(5);
    r.drain();
    EXPECT_EQ(r.sink.offsetsFor(0), offs({0, 2, 4}));
    EXPECT_EQ(r.sink.offsetsFor(1), offs({0, 2, 4}));
}

TEST(FilterGroupRegression, MatchListOverflowServesTheBatchPerMemberRegression) {
    auto run = [](uint32_t matchScratch, bool expectOverflow) {
        FilterConfig cfg;
        cfg.matchScratch = matchScratch;
        Rig r(cfg);
        for (uint32_t i = 0; i < 3; ++i) r.add(i, worldEq(r.schema, 7));
        r.pass();
        r.publish(makePlayerBatch(0, 3, worlds({7, 8, 7, 7})));
        r.publish(makePlayerBatch(4, 3, worlds({8, 7, 8})));
        r.pass();
        EXPECT_EQ(r.g.groupCounters().overflowBatches, expectOverflow ? 1u : 0u);   // 9 matches > 4, then 3 <= 4
        std::vector<std::vector<uint8_t>> frames;
        for (uint32_t i = 0; i < 3; ++i)
            for (const Frame* f : r.sink.dataFor(i)) frames.push_back(f->bytes);
        return frames;
    };
    const auto normal = run(65'536, false);
    const auto overflowed = run(4, true);
    EXPECT_EQ(normal.size(), 6u);
    EXPECT_EQ(overflowed, normal) << "same deliveries as without overflow";
}

TEST(FilterGroupRegression, RemovingAMemberUnregistersEveryIndexValueRegression) {
    FilterConfig cfg;
    cfg.maxIndexValuesPerPartition = 3;
    Rig r(cfg, 2);
    const uint32_t a = r.add(0, channelIn(r.schema, {1, 2, 3}));
    ASSERT_NE(a, kNil);
    EXPECT_EQ(r.add(1, channelEq(r.schema, 4)), kNil) << "a 4th distinct value is over the cap";
    EXPECT_EQ(r.g.groupCounters().overCap, 1u);
    r.pass();
    r.g.remove(a);
    EXPECT_TRUE(r.g.empty());
    const uint32_t b = r.add(1, channelEq(r.schema, 4));
    ASSERT_NE(b, kNil) << "every value of the removed member was freed";
    EXPECT_EQ(b, a) << "the slot is reused";
    r.pass();
    r.publish(makePlayerBatch(0, 2, {PlayerRow{.channel = 2}, PlayerRow{.channel = 4}}));
    r.pass();
    EXPECT_TRUE(r.sink.dataFor(0).empty());
    EXPECT_EQ(r.sink.offsetsFor(1), offs({1})) << "the removed member's values no longer route to the new one";
    r.g.remove(b);
    EXPECT_TRUE(std::holds_alternative<ScanIdle>(r.g.scanState())) << "no InSync member left";
}

TEST(FilterGroupRegression, MemberCapReturnsNilNeverAllocatesRegression) {
    FilterConfig cfg;
    cfg.maxSubsPerPartition = 2;
    Rig r(cfg);
    EXPECT_NE(r.add(0, worldEq(r.schema, 1)), kNil);
    EXPECT_NE(r.add(1, worldEq(r.schema, 2)), kNil);
    EXPECT_EQ(r.add(2, worldEq(r.schema, 3)), kNil);
    EXPECT_EQ(r.g.groupCounters().overCap, 1u);
    EXPECT_EQ(r.g.memberCount(), 2u);
}

TEST(FilterGroupRegression, NinthDistinctIndexFieldIsRefusedRegression) {
    using namespace rune::codec;
    DescriptorBuilder db("wide");
    for (uint16_t t = 0; t < 9; ++t) db.addTagged("f" + std::to_string(t), t, PrimType::UInt32, 1, 0xFFFF, t == 0, {});
    const FilterSchema schema = FilterSchema::build(db.build(), 1);
    FilterGroup g;
    g.init(1, 0, FilterConfig{}, kSrc);
    for (uint16_t t = 0; t < 9; ++t) {
        auto f = mk(schema, [&](FilterWriter& w) { w.term(t, PrimType::UInt32, 4, Match<uint32_t>(1u)); });
        const uint32_t m = g.add(t, std::move(f), schema, kSrc);
        if (t < kMaxIndexFields) EXPECT_NE(m, kNil) << t; else EXPECT_EQ(m, kNil) << "9th distinct field";
    }
    EXPECT_EQ(g.groupCounters().overCap, 1u);
    // A second member on an existing field still fits.
    auto again = mk(schema, [&](FilterWriter& w) { w.term(3, PrimType::UInt32, 4, Match<uint32_t>(2u)); });
    EXPECT_NE(g.add(20, std::move(again), schema, kSrc), kNil);
}

TEST(FilterGroupRegression, TombstonesAreExcludedAndTheScanAdvancesRegression) {
    Rig r;
    const uint32_t m = r.add(0, worldEq(r.schema, 7));
    r.pass();
    ASSERT_TRUE(r.inSync(m));
    r.publish(makePlayerBatch(0, 3, {PlayerRow{.tombstone = true}, PlayerRow{.worldId = 7}, PlayerRow{.tombstone = true},
                                     PlayerRow{.worldId = 7}, PlayerRow{.shortValue = 2}}));
    r.pass();
    EXPECT_EQ(r.sink.offsetsFor(0), offs({1, 3}));
    EXPECT_EQ(r.g.groupCounters().nullValueExcluded, 2u);
    EXPECT_EQ(r.g.groupCounters().malformed, 1u);
    EXPECT_EQ(r.g.cursorOf(m, 0), 5);
    // The same on the catch-up path.
    Rig c;
    c.src.append(makePlayerBatch(0, 3, {PlayerRow{.tombstone = true}, PlayerRow{.worldId = 7}, PlayerRow{.shortValue = 1}}));
    c.src.publishAll();
    const uint32_t cm = c.add(0, worldEq(c.schema, 7));
    c.pass();
    EXPECT_EQ(c.sink.offsetsFor(0), offs({1}));
    EXPECT_EQ(c.g.countersOf(cm).nullValueExcluded, 1u);
    EXPECT_EQ(c.g.countersOf(cm).malformed, 1u);
}

TEST(FilterGroupRegression, StraddlingStartDeliversTheWholeBatchOnceRegression) {
    Rig r;
    r.src.append(makePlayerBatch(10, 3, worlds({7, 7, 7, 7, 7})));   // 10..14
    r.src.append(makePlayerBatch(15, 3, worlds({7, 8})));            // 15..16
    r.src.publishAll();
    r.sink.cursor(0) = 12;   // inside the first batch
    const uint32_t m = r.add(0, worldEq(r.schema, 7));
    r.drain();
    EXPECT_EQ(r.sink.dataFor(0).size(), 2u) << "each source batch once";
    EXPECT_EQ(r.sink.offsetsFor(0), offs({10, 11, 12, 13, 14, 15}));
    EXPECT_EQ(r.sink.clientView(0, 12), offs({12, 13, 14, 15})) << "the model client drops 10 and 11";
    EXPECT_TRUE(r.inSync(m));
}

TEST(FilterGroupRegression, EvictedCursorResumesAtTheRingStartRegression) {
    Rig r;
    r.src.append(makePlayerBatch(0, 3, worlds({7, 7})));
    r.src.append(makePlayerBatch(2, 3, worlds({7, 7})));
    r.src.append(makePlayerBatch(4, 3, worlds({7, 7})));
    r.src.publishAll();
    r.src.setRingStart(2);
    const uint32_t m = r.add(0, worldEq(r.schema, 7));
    r.drain();
    EXPECT_EQ(r.sink.offsetsFor(0), offs({2, 3, 4, 5})) << "the evicted batch is skipped, not stalled on";
    EXPECT_GT(r.src.skipEvictedCalls, 0u);
    EXPECT_TRUE(r.inSync(m));
    // The scan itself resumes at the ring start too.
    r.publish(makePlayerBatch(6, 3, worlds({7})));
    r.publish(makePlayerBatch(7, 3, worlds({7})));
    r.src.setRingStart(7);
    r.drain();
    EXPECT_EQ(r.sink.offsetsFor(0), offs({2, 3, 4, 5, 7}));
}

TEST(FilterGroupRegression, FreshBudgetOnlyLetsTheFirstBatchOfAFreshPassExceedItRegression) {
    // Scan path.
    auto scanRig = [](Rig& r) {
        r.add(0, worldEq(r.schema, 7));
        r.pass();
        r.publish(makePlayerBatch(0, 3, std::vector<PlayerRow>(20, PlayerRow{.worldId = 7})));   // ~1 KB
    };
    {
        Rig r;
        scanRig(r);
        std::size_t budget = 10;
        r.passWith(budget, /*fresh=*/true);
        EXPECT_EQ(r.sink.dataFrames, 1u) << "an oversize first batch of a fresh pass is delivered";
        EXPECT_EQ(budget, 0u);
    }
    {
        Rig r;
        scanRig(r);
        std::size_t budget = 10;
        r.passWith(budget, /*fresh=*/false);
        EXPECT_EQ(r.sink.dataFrames, 0u) << "the shared budget is already spent: it waits";
        EXPECT_EQ(budget, 10u);
        r.pass(true);
        EXPECT_EQ(r.sink.dataFrames, 1u);
    }
    // Catch-up path.
    auto behindRig = [](Rig& r) {
        r.src.append(makePlayerBatch(0, 3, std::vector<PlayerRow>(20, PlayerRow{.worldId = 7})));
        r.src.publishAll();
        r.add(0, worldEq(r.schema, 7));
    };
    {
        Rig r;
        behindRig(r);
        std::size_t budget = 10;
        r.passWith(budget, true);
        EXPECT_EQ(r.sink.dataFrames, 1u);
    }
    {
        Rig r;
        behindRig(r);
        std::size_t budget = 10;
        r.passWith(budget, false);
        EXPECT_EQ(r.sink.dataFrames, 0u);
    }
}

TEST(FilterGroupRegression, BudgetStopsTheScanAtTheBatchThatWouldExceedItRegression) {
    Rig r;
    r.add(0, worldEq(r.schema, 7));
    r.pass();
    const auto b0 = makePlayerBatch(0, 3, std::vector<PlayerRow>(8, PlayerRow{.worldId = 7}));
    const auto b1 = makePlayerBatch(8, 3, std::vector<PlayerRow>(8, PlayerRow{.worldId = 7}));
    r.src.append(b0);
    r.src.append(b1);
    r.src.publishAll();
    // Read charge + send charge for b0 = 2 * size; leave room for exactly that.
    std::size_t budget = 2 * b0.size() + 5;
    r.passWith(budget, true);
    EXPECT_EQ(r.sink.offsetsFor(0).size(), 8u) << "only the first batch";
    EXPECT_EQ(budget, 5u);
    r.drain();
    EXPECT_EQ(r.sink.offsetsFor(0).size(), 16u);
}

TEST(FilterGroupRegression, ScanSourceIdentityIsEnforcedRegression) {
    // Debug: add() asserts on a foreign scan source.  Release: add() is addOrRefuse(); that path is exercised in
    // every build type through the public addOrRefuse() hook (a TU compiled with NDEBUG would break the ODR of the
    // header-only class).
    ::testing::FLAGS_gtest_death_test_style = "threadsafe";
    Rig r;
    EXPECT_DEBUG_DEATH({ (void)r.g.add(0, worldEq(r.schema, 1), r.schema, kSrc + 1); }, "scan source");
    const uint32_t id = r.g.addOrRefuse(0, worldEq(r.schema, 1), r.schema, kSrc + 1);
    EXPECT_EQ(id, kNil);
    EXPECT_EQ(r.g.groupCounters().sourceRefused, 1u);
    EXPECT_TRUE(r.g.empty());
    EXPECT_NE(r.add(0, worldEq(r.schema, 1)), kNil) << "the group's own source is accepted";
    EXPECT_EQ(r.g.groupCounters().sourceRefused, 1u);
}

constexpr std::size_t kRealBudget = 128 * 1024;   // Worker kPushPassBudgetBytes

TEST(FilterGroupRegression, BackPressuredBehindMemberDoesNotStarveTheScanUnderTheRealBudgetRegression) {
    Rig r;
    const uint32_t live = r.add(0, worldEq(r.schema, 7));
    r.pass();
    ASSERT_TRUE(r.inSync(live));
    // ~80 KiB batches: a read plus a send crosses the 128 KiB pass budget.
    const std::vector<PlayerRow> big(1500, PlayerRow{.worldId = 7});
    for (int i = 0; i < 6; ++i) r.src.append(makePlayerBatch(i * 1500, 3, big));
    r.src.publishAll();
    const uint32_t stuck = r.add(1, worldEq(r.schema, 7));   // cursor 0: Behind
    r.sink.refuse(1, 1'000'000);
    int passes = 0;
    while (r.sink.dataFor(0).size() < 6 && passes < 20) {
        ++passes;
        r.now += 1000;
        std::size_t budget = kRealBudget;
        r.passWith(budget, true);
    }
    EXPECT_EQ(r.sink.dataFor(0).size(), 6u) << "the InSync member got every batch";
    EXPECT_LE(passes, 8) << "one batch per pass, bounded";
    EXPECT_TRUE(r.sink.dataFor(1).empty());
    EXPECT_FALSE(r.inSync(stuck));
}

TEST(FilterGroupRegression, LargeCatchUpDoesNotStarveTheLiveTailUnderALimitedBudgetRegression) {
    Rig r;
    const std::vector<PlayerRow> chunk(300, PlayerRow{.worldId = 7});   // ~16 KiB
    for (int i = 0; i < 40; ++i) r.src.append(makePlayerBatch(i * 300, 3, chunk));
    r.src.publishAll();
    r.sink.cursor(0) = 40 * 300;   // the live member subscribes at the tail
    const uint32_t live = r.add(0, worldEq(r.schema, 7));
    const uint32_t late = r.add(1, worldEq(r.schema, 7));   // from offset 0: a 640 KiB backlog
    int64_t next = 40 * 300;
    uint64_t liveBatches = 0;
    for (int i = 0; i < 60; ++i) {
        if (i > 0) {
            r.publish(makePlayerBatch(next, 3, chunk));
            next += 300;
        }
        r.now += 1000;
        std::size_t budget = kRealBudget;
        r.passWith(budget, true);
        if (i > 0) {
            ++liveBatches;
            EXPECT_EQ(r.sink.dataFor(0).size(), liveBatches) << "live delivery makes progress in pass " << i;
        }
    }
    EXPECT_TRUE(r.inSync(live));
    // The backlog drains too, in order and exactly once.
    for (int i = 0; i < 100 && !r.inSync(late); ++i) { std::size_t b = kRealBudget; r.passWith(b, true); }
    EXPECT_TRUE(r.inSync(late));
    const auto got = r.sink.offsetsFor(1);
    ASSERT_FALSE(got.empty());
    for (std::size_t i = 0; i < got.size(); ++i) ASSERT_EQ(got[i], static_cast<int64_t>(i)) << i;
    EXPECT_EQ(got.size(), static_cast<std::size_t>(next));
}

TEST(FilterGroupRegression, RefusedCatchUpSendRefundsTheReadAndTheFreshAllowanceRegression) {
    Rig r;
    r.src.append(makePlayerBatch(0, 3, std::vector<PlayerRow>(20, PlayerRow{.worldId = 7})));   // ~1 KiB
    r.src.publishAll();
    r.add(0, worldEq(r.schema, 7));
    r.add(1, worldEq(r.schema, 7));
    r.sink.refuse(0, 1);
    std::size_t budget = 10;
    r.passWith(budget, true);
    EXPECT_EQ(r.sink.offsetsFor(1).size(), 20u)
        << "the refused member gave back the fresh-pass allowance: the next member's oversize batch goes out";
    EXPECT_TRUE(r.sink.dataFor(0).empty());
    // And a pass with only a refused member spends nothing.
    Rig q;
    q.src.append(makePlayerBatch(0, 3, std::vector<PlayerRow>(20, PlayerRow{.worldId = 7})));
    q.src.publishAll();
    q.add(0, worldEq(q.schema, 7));
    q.sink.refuse(0, 1);
    std::size_t b2 = 10;
    q.passWith(b2, true);
    EXPECT_EQ(b2, 10u) << "a refused send refunds the read charge";
    q.passWith(b2, true);
    EXPECT_EQ(q.sink.dataFor(0).size(), 1u);
}

TEST(FilterGroupRegression, ExpiredVersionWaitDoesNotRestartOnTheNextBatchRegression) {
    // Scan path: v2 batches interleaved with v1; the schema never arrives.
    {
        Rig r(FilterConfig{}, 1);
        const uint32_t m = r.add(0, worldEq(r.schema, 7));
        r.pass();
        ASSERT_TRUE(r.inSync(m));
        for (int i = 0; i < 6; ++i) r.src.append(makePlayerBatch(i, i % 2 == 0 ? 2 : 1, worlds({7})));
        r.src.publishAll();
        r.pass();
        ASSERT_TRUE(std::holds_alternative<ScanStall>(r.g.scanState()));
        r.now += 5'000'000'000;   // the one wait
        r.pass();
        EXPECT_TRUE(std::holds_alternative<Scanning>(r.g.scanState()));
        EXPECT_EQ(r.g.cursorOf(m, 0), 6) << "all six batches in ONE pass after ONE wait";
        EXPECT_EQ(r.sink.offsetsFor(0), offs({1, 3, 5}));
        EXPECT_EQ(r.g.groupCounters().unknownVersionExcluded, 3u);
        r.publish(makePlayerBatch(6, 2, worlds({7})));   // a later batch of the expired version: no new wait
        r.pass();
        EXPECT_EQ(r.g.cursorOf(m, 0), 7);
        EXPECT_EQ(r.g.groupCounters().unknownVersionExcluded, 4u);
    }
    // Catch-up path.
    {
        Rig r(FilterConfig{}, 1);
        for (int i = 0; i < 6; ++i) r.src.append(makePlayerBatch(i, i % 2 == 0 ? 2 : 1, worlds({7})));
        r.src.publishAll();
        const uint32_t m = r.add(0, worldEq(r.schema, 7));
        r.pass();
        ASSERT_TRUE(std::holds_alternative<VersionStall>(r.g.stateOf(m)));
        r.now += 5'000'000'000;
        r.pass();
        EXPECT_TRUE(r.inSync(m)) << "one wait, then the member moves through the rest";
        EXPECT_EQ(r.sink.offsetsFor(0), offs({1, 3, 5}));
        EXPECT_EQ(r.g.countersOf(m).unknownVersionExcluded, 3u);
    }
}

TEST(FilterGroupRegression, MixedAbsentVersionsLogOncePerMemberAndVersionRegression) {
    LogCapture log;
    Rig r(FilterConfig{}, 4);   // v3 and v4 both lack channel
    const uint32_t a = r.add(0, channelIn(r.schema, {1, 2}));
    const uint32_t b = r.add(1, channelEq(r.schema, 3));
    const uint32_t c = r.add(2, worldAndChannel(r.schema, 5, 2));   // residual absent
    r.pass();
    ASSERT_TRUE(r.inSync(a) && r.inSync(b) && r.inSync(c));
    for (int i = 0; i < 12; ++i) r.publish(makePlayerBatch(i, i % 2 == 0 ? 3 : 4, worlds({5})));
    r.pass();
    EXPECT_EQ(r.g.countersOf(a).absentExcluded, 12u);
    EXPECT_EQ(log.count("filter: filtered field absent: sub 0 "), 2u) << "v3 once, v4 once";
    EXPECT_EQ(log.count("filter: filtered field absent: sub 1 "), 2u);
    EXPECT_EQ(log.count("filter: filtered field absent: sub 2 "), 2u);
    EXPECT_EQ(log.count("schema version 3 "), 3u);
    EXPECT_EQ(log.count("schema version 4 "), 3u);
}

TEST(FilterGroupRegression, CatchUpUnknownVersionWarnsOncePerVersionRegression) {
    LogCapture log;
    Rig r(FilterConfig{}, 1);
    r.src.append(makePlayerBatch(0, 2, worlds({7, 7, 7, 7})));
    r.src.append(makePlayerBatch(4, 2, worlds({7, 7})));
    r.src.publishAll();
    const uint32_t m = r.add(0, worldEq(r.schema, 7));
    r.pass();
    r.now += 5'000'000'000;
    r.drain();
    EXPECT_EQ(r.g.countersOf(m).unknownVersionExcluded, 6u);
    EXPECT_EQ(log.count("unknown record schema version: sub 0 "), 1u) << "once per member and version, not per record";
}

TEST(FilterGroupRegression, IndexFieldSlotIsReusedAfterItsLastMemberLeavesRegression) {
    using namespace rune::codec;
    DescriptorBuilder db("wide");
    for (uint16_t t = 0; t < 12; ++t) db.addTagged("f" + std::to_string(t), t, PrimType::UInt32, 1, 0xFFFF, t == 0, {});
    const FilterSchema schema = FilterSchema::build(db.build(), 1);
    FilterGroup g;
    g.init(1, 0, FilterConfig{}, kSrc);
    auto on = [&](uint16_t t, uint32_t v) {
        return mk(schema, [&](FilterWriter& w) { w.term(t, PrimType::UInt32, 4, Match<uint32_t>(v)); });
    };
    // 12 distinct fields one after another, one member at a time.
    for (uint16_t t = 0; t < 12; ++t) {
        const uint32_t m = g.add(t, on(t, 1), schema, kSrc);
        ASSERT_NE(m, kNil) << "field " << t;
        g.remove(m);
    }
    EXPECT_EQ(g.groupCounters().overCap, 0u);
    // 8 fields live, one leaves, a 9th is accepted; with all 8 live it is refused.
    std::vector<uint32_t> ms;
    for (uint16_t t = 0; t < 8; ++t) ms.push_back(g.add(t, on(t, 1), schema, kSrc));
    EXPECT_EQ(g.add(20, on(9, 1), schema, kSrc), kNil);
    g.remove(ms[3]);
    EXPECT_NE(g.add(21, on(9, 1), schema, kSrc), kNil) << "the freed slot serves the new field";
    EXPECT_NE(g.add(22, on(9, 2), schema, kSrc), kNil) << "and takes more members";
    EXPECT_EQ(g.add(23, on(3, 2), schema, kSrc), kNil) << "all 8 slots hold live fields again: field 3 is now the 9th";
}

TEST(FilterGroup, SlotsTouchedByAnIdlePassIsZero) {   // PR1: no per-member scan
    FilterConfig cfg;
    cfg.noteIntervalNs = 1'000'000;
    Rig r(cfg);
    for (uint32_t i = 0; i < 500; ++i) ASSERT_NE(r.add(i, worldEq(r.schema, i)), kNil);
    r.pass();
    ASSERT_EQ(r.g.inSyncCount(), 500u);
    uint64_t before = r.g.slotsTouchedForTest();
    for (int k = 0; k < 5; ++k) {
        r.now += 10'000'000;
        r.pass();
    }
    EXPECT_EQ(r.g.slotsTouchedForTest() - before, 0u) << "500 InSync members, nothing to scan or note";
    // A scanned batch nobody matches owes every member a note; after the notes, idle again.
    r.publish(makePlayerBatch(0, 3, worlds({9999})));
    r.pass();
    r.now += 10'000'000;
    r.pass();
    EXPECT_EQ(r.sink.noteFrames, 500u);
    before = r.g.slotsTouchedForTest();
    for (int k = 0; k < 5; ++k) {
        r.now += 10'000'000;
        r.pass();
    }
    EXPECT_EQ(r.g.slotsTouchedForTest() - before, 0u) << "every note sent: back to zero";
    EXPECT_EQ(r.sink.noteFrames, 500u);
}

TEST(FilterGroup, MembersMoveAndAreFound) {
    Rig r;
    EXPECT_TRUE(r.g.empty());
    EXPECT_EQ(r.g.anySub(), kNil);
    const uint32_t a = r.add(5, worldEq(r.schema, 7));
    EXPECT_FALSE(r.g.empty());
    EXPECT_EQ(r.g.anySub(), 5u);
    r.pass();
    EXPECT_EQ(r.g.anySub(), 5u) << "also once InSync";
    r.g.moveSub(a, 9);
    EXPECT_EQ(r.g.anySub(), 9u);
    r.publish(makePlayerBatch(0, 3, worlds({7})));
    r.pass();
    EXPECT_EQ(r.sink.offsetsFor(9), offs({0})) << "deliveries follow the moved sub";
    EXPECT_TRUE(r.sink.dataFor(5).empty());
}

// ---------------------------------------------------------------------------------------------------------------
// Allocation: the record path, with the shared AllocWindow (and a positive control for it).
// ---------------------------------------------------------------------------------------------------------------
namespace { volatile void* gEscape = nullptr; }

TEST(AllocWindow, CountsARealAllocationAndOnlyOnItsOwnThread) {
    uint64_t own = 0;
    {
        rune::testing::AllocWindow w;
        int* p = new int[64];
        gEscape = p;   // the pointer escapes: the allocation cannot be elided
        delete[] p;
        std::vector<int> v;
        v.reserve(100);
        gEscape = v.data();
        own = w.count();
    }
    EXPECT_GE(own, 2u) << "the window counts real allocations";
    uint64_t quiet = 0, busy = 0;
    {
        rune::testing::AllocWindow w;
        std::thread t([] {});
        t.join();
        quiet = w.count();   // includes whatever creating a thread allocates here
    }
    {
        rune::testing::AllocWindow w;
        std::thread t([] {
            for (int i = 0; i < 100; ++i) { int* p = new int[8]; gEscape = p; delete[] p; }
        });
        t.join();
        busy = w.count();
    }
    EXPECT_EQ(busy, quiet) << "another thread's 100 allocations are not counted";
}

TEST(FilterGroup, RecordPathAllocatesNothing) {
    constexpr uint32_t kMembers = 50, kBatches = 1100, kWarm = 100;
    FilterConfig cfg;
    cfg.noteIntervalNs = 1'000'000;
    Rig r(cfg);
    r.sink.recordFrames = false;
    r.sink.reserveSubs(kMembers);
    for (uint32_t i = 0; i < kMembers; ++i) ASSERT_NE(r.add(i, i % 5 == 0 ? worldAndHpIn(r.schema, i, {0, 1, 2}) : worldEq(r.schema, i)), kNil);
    r.pass();
    ASSERT_EQ(r.g.inSyncCount(), kMembers);
    // 8 records per batch, worldId in [0, 1000): 50 worlds are subscribed -> 5% of records have a candidate.
    for (uint32_t b = 0; b < kBatches; ++b) {
        std::vector<PlayerRow> rows;
        for (uint32_t j = 0; j < 8; ++j) rows.push_back(PlayerRow{.worldId = (b * 8 + j) * 37 % 1000, .hp = static_cast<int64_t>(j % 4)});
        r.src.append(makePlayerBatch(static_cast<int64_t>(b) * 8, 3, rows));
    }
    auto step = [&](uint32_t b) {
        r.src.setBound(r.src.entries()[b].lastOffset + 1);
        r.now += 100'000;
        r.pass();
    };
    for (uint32_t b = 0; b < kWarm; ++b) step(b);   // warm-up: the rebuild buffer reaches its size
    ASSERT_GT(r.sink.dataFrames, 0u);
    const uint64_t framesBefore = r.sink.dataFrames;
    rune::testing::AllocWindow window;
    for (uint32_t b = kWarm; b < kWarm + 1000; ++b) step(b);
    const uint64_t allocs = window.count();
    EXPECT_EQ(allocs, 0u);
    EXPECT_GT(r.sink.dataFrames, framesBefore + 20) << "the window did real work: deliveries";
    EXPECT_GT(r.sink.noteFrames, 0u) << "and notes";
    EXPECT_GT(r.g.groupCounters().rebuilt, 0u) << "and rebuilds";
}

// ---------------------------------------------------------------------------------------------------------------
// Seeded model check.  Random members (Eq / In / residual filters), batches (versions, control, transactional and
// aborted under read_committed), back-pressure, undeliverable subs, random budgets (fresh or not), remove, moveSub,
// eviction, a lagging bound and a schema that arrives late (version stalls).  Per member, over every frame it ever
// received, in order: the offsets strictly increase (a duplicate or a reorder fails), every delivered offset at or
// after its start is a matching record, and every matching record at or after max(start, final ring start) arrived.
// ---------------------------------------------------------------------------------------------------------------
class FilterGroupModel : public ::testing::TestWithParam<uint32_t> {};

TEST_P(FilterGroupModel, DeliveriesEqualTheMatchingRecords) {
    const uint32_t seed = GetParam();
    SCOPED_TRACE("seed " + std::to_string(seed));
    std::mt19937_64 rng(seed);
    FilterConfig cfg;
    cfg.noteIntervalNs = 100'000;
    cfg.matchScratch = rng() % 3 == 0 ? 6 : 65'536;   // some seeds overflow constantly
    cfg.versionWaitNs = int64_t{1} << 60;             // a stall ends only when the schema arrives
    cfg.catchUpSharePercent = static_cast<uint32_t>(10 + rng() % 90);
    const uint16_t startVer = static_cast<uint16_t>(1 + rng() % 2);
    Rig r(cfg, startVer);
    const bool rc = rng() % 3 == 0;
    r.src.setReadCommitted(rc);

    std::vector<PlayerRow> rows;      // by offset
    std::vector<uint8_t> okRec;       // 1 = a record of a normal (non-control, non-aborted) batch
    std::vector<int64_t> batchEnds;   // end offset of every published batch
    int64_t bound = 0, ringStart = 0;
    struct M { uint32_t id; std::vector<uint32_t> subs; int64_t start; int kind; uint32_t w; std::vector<int32_t> ch;
               std::vector<int64_t> hp; bool removed = false; };
    std::vector<M> ms;
    uint32_t nextSub = 0;
    auto matches = [&](const M& m, const PlayerRow& row) {
        switch (m.kind) {
            case 0: return row.worldId == m.w;
            case 1: return std::find(m.ch.begin(), m.ch.end(), row.channel) != m.ch.end();
            case 2: return row.worldId == m.w && std::find(m.hp.begin(), m.hp.end(), row.hp) != m.hp.end();
            default: return row.worldId == m.w && row.channel == m.ch[0];
        }
    };
    auto addMember = [&](int64_t start) {
        M m;
        m.kind = static_cast<int>(rng() % 4);
        m.w = static_cast<uint32_t>(rng() % 6);
        m.start = start;
        switch (m.kind) {
            case 1: { std::set<int32_t> s; for (uint64_t k = 1 + rng() % 3; k > 0; --k) s.insert(static_cast<int32_t>(rng() % 4)); m.ch.assign(s.begin(), s.end()); break; }
            case 2: { std::set<int64_t> s; for (uint64_t k = 1 + rng() % 3; k > 0; --k) s.insert(static_cast<int64_t>(rng() % 3)); m.hp.assign(s.begin(), s.end()); break; }
            case 3: m.ch = {static_cast<int32_t>(rng() % 4)}; break;
            default: break;
        }
        CompiledFilter f = m.kind == 0 ? worldEq(r.schema, m.w) : m.kind == 1 ? channelIn(r.schema, m.ch)
                         : m.kind == 2 ? worldAndHpIn(r.schema, m.w, m.hp) : worldAndChannel(r.schema, m.w, m.ch[0]);
        const uint32_t sub = nextSub++;
        r.sink.cursor(sub) = start;
        m.subs.push_back(sub);
        m.id = r.add(sub, std::move(f));
        ASSERT_NE(m.id, kNil);
        ms.push_back(std::move(m));
    };
    auto liveIdx = [&]() {
        std::vector<std::size_t> v;
        for (std::size_t i = 0; i < ms.size(); ++i) if (!ms[i].removed) v.push_back(i);
        return v;
    };
    bool extended = startVer >= 2;
    const std::size_t budgets[] = {64, 600, 5'000, std::size_t{1} << 20};
    for (int step = 0; step < 90; ++step) {
        const auto live = liveIdx();
        switch (rng() % 10) {
            case 0: case 1: if (ms.size() < 14) addMember(static_cast<int64_t>(rng() % (rows.size() + 1))); break;
            case 2: if (!live.empty()) r.sink.refuse(ms[live[rng() % live.size()]].subs.back(), 1 + static_cast<uint32_t>(rng() % 3)); break;
            case 3: if (!live.empty()) { const auto sub = ms[live[rng() % live.size()]].subs.back(); r.sink.setDeliverable(sub, rng() % 2 == 0); } break;
            case 4: if (!live.empty() && rng() % 2 == 0) { M& m = ms[live[rng() % live.size()]]; r.g.remove(m.id); m.removed = true; } break;
            case 5: if (!live.empty()) {
                M& m = ms[live[rng() % live.size()]];
                const uint32_t old = m.subs.back(), nw = nextSub++;
                r.sink.cursor(nw) = r.g.cursorOf(m.id, r.sink.cursor(old));
                r.g.moveSub(m.id, nw);
                m.subs.push_back(nw);
            } break;
            case 6: if (!batchEnds.empty() && rng() % 2 == 0) ringStart = std::max(ringStart, batchEnds[rng() % batchEnds.size()]); break;
            case 7: if (!extended && rng() % 2 == 0) { ASSERT_EQ(r.schema.extendFrom(playerSchema(2)), FilterSchema::ExtendResult::Extended); extended = true; } break;
            default: {   // publish a batch
                std::vector<PlayerRow> batch;
                for (uint64_t k = 1 + rng() % 6; k > 0; --k)
                    batch.push_back(PlayerRow{.worldId = static_cast<uint32_t>(rng() % 6), .channel = static_cast<int32_t>(rng() % 4),
                                              .hp = static_cast<int64_t>(rng() % 3)});
                BatchOpts opts;
                bool ok = true;
                const uint64_t kind = rng() % 12;
                if (kind == 0) { opts.control = true; opts.producerId = 5; ok = false; batch.resize(1); }
                else if (kind <= 2) {
                    opts.transactional = true;
                    opts.producerId = 9;
                    if (rng() % 2 == 0) { if (rc) { r.src.abort(9, static_cast<int64_t>(rows.size())); ok = false; } }
                }
                const uint16_t ver = static_cast<uint16_t>(1 + rng() % 2);
                r.src.append(makePlayerBatch(static_cast<int64_t>(rows.size()), ver, batch, opts));
                rows.insert(rows.end(), batch.begin(), batch.end());
                okRec.insert(okRec.end(), batch.size(), ok ? 1 : 0);
                batchEnds.push_back(static_cast<int64_t>(rows.size()));
            }
        }
        // The bound only moves forward, to a batch end (HW / LSO).
        if (!batchEnds.empty() && rng() % 3 != 0) {
            const int64_t cand = batchEnds[rng() % batchEnds.size()];
            bound = std::max(bound, cand);
        }
        r.src.setBound(bound);
        r.src.setRingStart(ringStart);
        r.now += static_cast<int64_t>(rng() % 200'000);
        std::size_t budget = budgets[rng() % 4];
        r.passWith(budget, rng() % 2 == 0);
    }
    // Settle: everything deliverable, no refusals, the schema known, the bound at the end, a big fresh budget.
    if (!extended) ASSERT_EQ(r.schema.extendFrom(playerSchema(2)), FilterSchema::ExtendResult::Extended);
    for (uint32_t sub = 0; sub < nextSub; ++sub) { r.sink.refuse(sub, 0); r.sink.setDeliverable(sub, true); }
    bound = static_cast<int64_t>(rows.size());
    r.src.setBound(bound);
    for (int i = 0; i < 600; ++i) {
        r.now += 1'000'000;
        if (r.pass() == 0 && i > 5) break;
    }
    for (const M& m : ms) {
        std::vector<int64_t> raw;
        for (const uint32_t sub : m.subs) {
            const auto o = r.sink.offsetsFor(sub);
            raw.insert(raw.end(), o.begin(), o.end());
        }
        for (std::size_t i = 1; i < raw.size(); ++i)
            ASSERT_GT(raw[i], raw[i - 1]) << "duplicate or reordered delivery, member " << m.id << " kind " << m.kind;
        std::set<int64_t> expect;
        for (int64_t o = m.start; o < static_cast<int64_t>(rows.size()); ++o)
            if (okRec[static_cast<std::size_t>(o)] != 0 && matches(m, rows[static_cast<std::size_t>(o)])) expect.insert(o);
        std::set<int64_t> got;
        for (const int64_t o : raw) {
            if (o < m.start) continue;   // the straddling first batch
            got.insert(o);
            EXPECT_TRUE(expect.count(o) != 0) << "member " << m.id << " got a non-matching record " << o;
        }
        if (m.removed) continue;
        for (const int64_t e : expect)
            if (e >= ringStart) EXPECT_TRUE(got.count(e) != 0) << "member " << m.id << " kind " << m.kind << " start " << m.start
                                                                << " missed " << e << " (ring start " << ringStart << ")";
    }
}
INSTANTIATE_TEST_SUITE_P(Seeds, FilterGroupModel, ::testing::Range(1u, 61u));
