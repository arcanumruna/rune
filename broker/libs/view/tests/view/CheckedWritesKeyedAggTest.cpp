// Part 07 Task 11: composite group keys for aggregate views.  A group's key (the view key) is interned to a dense id; the
// contributor rows, links, accumulators and value blocks use that id exactly as a legacy group value.
#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <cstring>
#include <map>
#include <random>
#include <set>
#include <string>
#include <vector>

#include <rune/codec/Descriptor.h>
#include <rune/codec/Evolve.h>
#include <rune/codec/UnicodeLower.h>
#include <rune/protocol/BatchRebuild.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/view/AggFold.h>
#include <rune/view/PlanCompiler.h>
#include <rune/view/TxnOverlay.h>
#include <rune/view/ViewCheckpoint.h>

#include "LegacyFoldReference.h"

namespace {

using rune::codec::PrimType;
using namespace rune::view;

constexpr int32_t kViewTopic = 0x40000009;

// eventId (key) | userId | deviceFp | model | platform | region | v2: shard, lastSeen, bytes.  A v1 record has no shard (no
// group for a (u64, u64) key) and no lastSeen / bytes (nulls).
rune::codec::Descriptor deviceSource() {
    return rune::codec::DescriptorBuilder("device", 2)
        .add("eventId", PrimType::UInt64, 1, true).add("userId", PrimType::UInt64).addFixedStr("deviceFp", 32).addFixedStr("model", 16)
        .add("platform", PrimType::Enum).add("region", PrimType::UInt16).add("shard", PrimType::UInt64, 2)
        .add("lastSeen", PrimType::Int64, 2).add("bytes", PrimType::UInt64, 2).build();
}

struct Ev {
    uint64_t id = 0;
    uint64_t user = 0;
    std::string fp;
    std::string model;
    uint32_t platform = 0;
    uint16_t region = 0;
    uint64_t shard = 0;
    int64_t lastSeen = 0;
    uint64_t bytes = 0;
    int version = 2;
    bool tombstone = false;
};

std::string wireKey(uint64_t id) {
    std::string k(8, '\0');
    for (int i = 0; i < 8; ++i) k[size_t(i)] = char(id >> (8 * i));
    return k;
}

std::string valueOf(const rune::codec::Descriptor& evolving, const Ev& e) {
    const auto d = rune::codec::materializeAsOf(evolving, uint16_t(e.version));
    std::string rec(4 + d.blockLength, '\0');
    rec[0] = char(d.blockLength); rec[1] = char(d.blockLength >> 8);
    rec[2] = char(d.version);     rec[3] = char(d.version >> 8);
    const auto num = [&](const char* name, uint64_t v) {
        const auto* f = d.find(name);
        if (f == nullptr) return;
        for (uint32_t i = 0; i < rune::codec::fieldWidth(*f); ++i) rec[4 + f->offset + i] = char(v >> (8 * i));
    };
    const auto str = [&](const char* name, const std::string& s) {
        const auto* f = d.find(name);
        for (size_t i = 0; i < s.size(); ++i) rec[4 + f->offset + i] = s[i];
    };
    num("eventId", e.id); num("userId", e.user); num("platform", e.platform); num("region", e.region);
    num("shard", e.shard); num("lastSeen", uint64_t(e.lastSeen)); num("bytes", e.bytes);
    str("deviceFp", e.fp); str("model", e.model);
    return rec;
}

// A source log that grows one batch per append.
struct Log {
    rune::codec::Descriptor src = deviceSource();
    std::vector<uint8_t> bytes;
    int64_t next = 0;
    size_t keyBytes = 8;   // the wire key width of the source key
    void batch(const std::vector<Ev>& evs) {
        rune::protocol::RecordBatchBuilder b(next, 0);
        for (const Ev& e : evs) b.append(wireKey(e.id).substr(0, keyBytes), e.tombstone ? std::string() : valueOf(src, e), 0);
        const auto out = b.build();
        bytes.insert(bytes.end(), out.begin(), out.end());
        next += int64_t(evs.size());
    }
    [[nodiscard]] rune::storage::LogReadView view() const { return rune::storage::LogReadView{bytes.data(), bytes.size(), 0, next}; }
};

ViewPlan aggregateOf(std::vector<KeyComponent> key, std::vector<AuthoredField> output, const char* sourceKey = "userId") {
    ViewPlan v;
    v.viewName = "device_history";
    v.sources = {{5, sourceKey}};
    for (const auto& k : key) {
        if (k.keyFunction) v.unicodeVersion = rune::codec::kUnicodeVersion;
    }
    v.groupBy = key;
    v.keyComponents = std::move(key);
    v.output = std::move(output);
    return v;
}

// count(*) as uses, sum(bytes) as total, min(lastSeen) as firstSeen, max(lastSeen) as lastSeen, last(platform) as platform
std::vector<AuthoredField> historyColumns() {
    return {{"uses", PrimType::Int64, 0, "", MergeRule::CountAll}, {"total", PrimType::UInt64, 0, "bytes", MergeRule::Sum},
            {"firstSeen", PrimType::Int64, 0, "lastSeen", MergeRule::Min}, {"lastSeen", PrimType::Int64, 0, "lastSeen", MergeRule::Max},
            {"platform", PrimType::Enum, 0, "platform", MergeRule::Last}};
}

// Every rule once, so the model run covers the accumulators, links and value blocks.
std::vector<AuthoredField> allRules() {
    return {{"uses", PrimType::Int64, 0, "", MergeRule::CountAll}, {"nBytes", PrimType::Int64, 0, "bytes", MergeRule::Count},
            {"total", PrimType::UInt64, 0, "bytes", MergeRule::Sum}, {"avgSeen", PrimType::Float64, 0, "lastSeen", MergeRule::Avg},
            {"firstSeen", PrimType::Int64, 0, "lastSeen", MergeRule::Min}, {"lastSeen", PrimType::Int64, 0, "lastSeen", MergeRule::Max},
            {"lastPlatform", PrimType::Enum, 0, "platform", MergeRule::Last}, {"lastBytes", PrimType::UInt64, 0, "bytes", MergeRule::LastNonNull}};
}

constexpr uint64_t kNoSeed = 0;

// The v2 checkpoint record of the fixed log in SingleEightByteGroupIsByteIdenticalRegression, captured from the unchanged legacy fold.
const std::vector<uint8_t> kLegacyGroupGolden = {
    0x32, 0x43, 0x4b, 0x56, 0x06, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03, 0x00, 0x19, 0x00, 0x00, 0x00, 0x03, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x07, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x07, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x07, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x05, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x09, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x09, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x37, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x11, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x07, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x09, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x19, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x03, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x09, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x1e, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x02, 0x38, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x07, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x07, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x05, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x09,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x37, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x5c, 0xda, 0x48, 0x4f};

// The tables of a keyed aggregate view, the fold's inputs and a prospect (what WorkerViewSet holds per instance).
struct Rig {
    rune::codec::Descriptor src;
    CompileResult comp;
    AggLayout agg;
    KeyedFoldSpec spec;
    VersionDescriptorCache vdc{src};
    ViewTable view, contributors, groups, blocks, groupIds;
    uint64_t next = 0;
    Prospect p;

    static CompileResult compile(const ViewPlan& v, const rune::codec::Descriptor& s) {
        const rune::codec::Descriptor* srcs[] = {&s};
        auto r = compileView(v, srcs, kViewTopic);
        EXPECT_TRUE(r.has_value()) << (r ? "" : r.error().message);
        return std::move(*r);
    }
    static RowLayout idRow() { RowLayout r; r.rowLen = 8; return r; }

    explicit Rig(const ViewPlan& v, uint64_t seed = kNoSeed, rune::codec::Descriptor source = deviceSource())
        : src(std::move(source)), comp(compile(v, src)), agg(buildAggLayout(comp.plan, src).value()), spec(buildKeyedFoldSpec(comp.plan, src).value()),
          view(comp.rowLayout, 256, 1u << 16, ViewTableKeys{uint8_t(spec.keyWidth), uint8_t(spec.prefixWidth), seed}),
          contributors(agg.contributorRow, 256, 1u << 16, ViewTableKeys{8, 0, seed}), groups(agg.groupRow, 256, 1u << 16, ViewTableKeys{8, 0, seed}),
          blocks(agg.blockRow, 256, 1u << 16, ViewTableKeys{8, 0, seed}), groupIds(idRow(), 256, 1u << 16, ViewTableKeys{uint8_t(spec.keyWidth), 0, seed}) {
        p.reserve(comp.rowLayout, &agg);
    }
    [[nodiscard]] AggregateFold fold() {
        return {comp.plan, comp.rowLayout, agg, vdc, view, contributors, groups, agg.numBlockCols > 0 ? &blocks : nullptr, &spec, &groupIds, &next};
    }
    // Folds all of `log` in passes of at most `perPass` records (0 = one pass).
    FoldStats foldLog(const Log& log, int64_t perPass = 0, std::mt19937* rng = nullptr) {
        FoldStats total{};
        int64_t cursor = 0;
        while (cursor < log.next) {
            const int64_t step = rng != nullptr ? 1 + int64_t((*rng)() % 7) : perPass;
            const int64_t bound = step == 0 ? log.next : std::min(log.next, cursor + step);
            auto f = fold();
            const FoldStats st = foldKeyedAggregateBatch(f, p, log.view(), bound, cursor, kNoSkip);
            total.applied += st.applied; total.deleted += st.deleted; total.skippedNoGroup += st.skippedNoGroup;
            total.skippedMalformed += st.skippedMalformed; total.aggRescans += st.aggRescans; total.aggInconsistent += st.aggInconsistent;
        }
        return total;
    }
    [[nodiscard]] std::vector<uint8_t> rowOf(const ViewKey& k) const {
        const uint8_t* r = view.get(k);
        return r == nullptr ? std::vector<uint8_t>{} : std::vector<uint8_t>(r, r + comp.rowLayout.rowLen);
    }
    [[nodiscard]] uint16_t slot(const char* name) const {
        for (size_t i = 0; i < comp.outputDescriptor.fields.size(); ++i)
            if (comp.outputDescriptor.fields[i].name == name) return uint16_t(i);
        ADD_FAILURE() << "no column " << name;
        return 0;
    }
    [[nodiscard]] rune::codec::FieldValue col(const ViewKey& k, const char* name) const {
        const uint8_t* r = view.get(k);
        EXPECT_NE(r, nullptr);
        return r == nullptr ? rune::codec::FieldValue{} : readSlot(r, comp.rowLayout, slot(name));
    }
};

ViewKey keyOf(std::initializer_list<uint64_t> numbers, const std::string& fp = "", size_t fpWidth = 32) {
    ViewKey k;
    for (uint64_t n : numbers) {
        for (int i = 7; i >= 0; --i) k.bytes[k.len++] = uint8_t(n >> (8 * i));
    }
    if (fpWidth != 0) {
        for (size_t i = 0; i < fpWidth; ++i) k.bytes[k.len++] = i < fp.size() ? uint8_t(fp[i]) : uint8_t{0};
    }
    return k;
}
ViewKey userFp(uint64_t user, const std::string& fp) { return keyOf({user}, fp); }

ViewPlan deviceHistory() { return aggregateOf({{"userId", 0, "userId"}, {"deviceFp", 0, "deviceFp"}}, historyColumns()); }

Ev ev(uint64_t id, uint64_t user, const std::string& fp, int64_t seen, uint64_t bytes, uint32_t platform = 1) {
    Ev e;
    e.id = id; e.user = user; e.fp = fp; e.lastSeen = seen; e.bytes = bytes; e.platform = platform;
    return e;
}
Ev gone(uint64_t id) { Ev e; e.id = id; e.tombstone = true; return e; }

size_t prefixCount(const Rig& r, const ViewKey& prefix, size_t prefixLen) {
    size_t n = 0;
    std::array<uint8_t, kScanTokenBytes> token{};
    EXPECT_EQ(r.view.scanPrefix({prefix.bytes.data(), prefixLen}, {}, token, [&](std::span<const uint8_t>, const uint8_t*) { ++n; return true; }), PrefixScan::End);
    return n;
}

}  // namespace

TEST(CheckedWritesKeyedAgg, TwoColumnGroupsAggregate) {
    Rig r(deviceHistory());
    EXPECT_TRUE(r.agg.keyed);
    EXPECT_EQ(r.agg.groupKeyWidth, 40u);
    Log log;
    log.batch({ev(1, 1, "fpA", 100, 10, 1), ev(2, 1, "fpA", 300, 20, 2), ev(3, 1, "fpA", 200, 30, 3), ev(4, 1, "fpB", 50, 5, 1),
               ev(5, 2, "fpA", 70, 1, 1), ev(6, 2, "fpA", 60, 2, 2)});
    const FoldStats st = r.foldLog(log);
    EXPECT_EQ(st.applied, 6u);
    EXPECT_EQ(r.view.size(), 3u);
    EXPECT_EQ(r.groupIds.size(), 3u);
    EXPECT_EQ(r.next, 3u);

    const ViewKey a = userFp(1, "fpA");
    EXPECT_EQ(r.col(a, "uses").i, 3);
    EXPECT_EQ(r.col(a, "total").u, 60u);
    EXPECT_EQ(r.col(a, "firstSeen").i, 100);
    EXPECT_EQ(r.col(a, "lastSeen").i, 300);
    EXPECT_EQ(r.col(a, "platform").u, 3u);   // the last write
    EXPECT_EQ(r.col(a, "userId").u, 1u);
    EXPECT_EQ(std::string(r.col(a, "deviceFp").bytes.substr(0, 3)), "fpA");
    EXPECT_EQ(r.col(userFp(1, "fpB"), "uses").i, 1);
    EXPECT_EQ(r.col(userFp(2, "fpA"), "total").u, 3u);

    EXPECT_EQ(prefixCount(r, a, 8), 2u);
    EXPECT_EQ(prefixCount(r, keyOf({2}), 8), 1u);
    EXPECT_EQ(prefixCount(r, keyOf({3}), 8), 0u);
}

TEST(CheckedWritesKeyedAgg, UpdateMovesAContributorBetweenGroups) {
    Rig r(deviceHistory());
    Log log;
    log.batch({ev(1, 1, "fpA", 100, 10), ev(2, 1, "fpA", 900, 20), ev(3, 1, "fpB", 500, 5)});
    r.foldLog(log);
    EXPECT_EQ(r.col(userFp(1, "fpA"), "lastSeen").i, 900);
    const uint64_t gidA = loadU64(r.groupIds.get(userFp(1, "fpA")));

    log.batch({ev(2, 1, "fpB", 900, 20)});   // the max of fpA moves to fpB
    const FoldStats st = r.foldLog(log);
    EXPECT_GE(st.aggRescans, 1u);
    EXPECT_EQ(st.aggInconsistent, 0u);
    const ViewKey a = userFp(1, "fpA"), b = userFp(1, "fpB");
    EXPECT_EQ(r.col(a, "uses").i, 1);
    EXPECT_EQ(r.col(b, "uses").i, 2);
    EXPECT_EQ(r.col(a, "lastSeen").i, 100);
    EXPECT_EQ(r.col(a, "firstSeen").i, 100);
    EXPECT_EQ(r.col(b, "lastSeen").i, 900);
    EXPECT_EQ(r.col(b, "firstSeen").i, 500);
    EXPECT_EQ(r.col(a, "total").u, 10u);
    EXPECT_EQ(r.col(b, "total").u, 25u);
    EXPECT_EQ(loadU64(r.groupIds.get(a)), gidA);   // the group keeps its id
}

TEST(CheckedWritesKeyedAgg, TombstoneDropsTheLastContributorsGroup) {
    Rig r(deviceHistory());
    Log log;
    log.batch({ev(1, 1, "fpA", 100, 10), ev(2, 2, "fpZ", 400, 7, 3)});
    r.foldLog(log);
    ASSERT_EQ(r.groups.size(), 2u);
    ASSERT_EQ(r.groupIds.size(), 2u);
    const uint64_t oldGid = loadU64(r.groupIds.get(userFp(2, "fpZ")));

    log.batch({gone(2)});
    r.foldLog(log);
    EXPECT_EQ(r.view.get(userFp(2, "fpZ")), nullptr);
    EXPECT_EQ(r.groupIds.get(userFp(2, "fpZ")), nullptr);
    EXPECT_EQ(r.groups.get(oldGid), nullptr);
    EXPECT_EQ(r.contributors.get(2), nullptr);
    EXPECT_EQ(r.view.size(), 1u);
    EXPECT_EQ(r.groups.size(), 1u);
    EXPECT_EQ(r.groupIds.size(), 1u);
    EXPECT_EQ(prefixCount(r, keyOf({2}), 8), 0u);

    log.batch({ev(2, 2, "fpZ", 400, 7, 3)});   // re-created: a new gid, the same row as a fresh fold
    r.foldLog(log);
    EXPECT_NE(loadU64(r.groupIds.get(userFp(2, "fpZ"))), oldGid);
    Log fresh;
    fresh.batch({ev(1, 1, "fpA", 100, 10), ev(2, 2, "fpZ", 400, 7, 3)});
    Rig f(deviceHistory());
    f.foldLog(fresh);
    EXPECT_EQ(r.rowOf(userFp(2, "fpZ")), f.rowOf(userFp(2, "fpZ")));
}

TEST(CheckedWritesKeyedAgg, LowerGroupKey) {
    Rig r(aggregateOf({{"userId", 0, "userId"}, {"deviceFp", 1, "fpKey"}}, historyColumns()));
    Log log;
    log.batch({ev(1, 1, "FpA", 100, 10), ev(2, 1, "fpa", 200, 20), ev(3, 1, "FPB", 300, 30)});
    r.foldLog(log);
    EXPECT_EQ(r.view.size(), 2u);
    ASSERT_EQ(r.spec.keyWidth, 8u + rune::codec::maxLoweredWidth(32));   // the key holds the lowered bytes
    const ViewKey fpa = keyOf({1}, "fpa", rune::codec::maxLoweredWidth(32));
    EXPECT_EQ(r.col(fpa, "uses").i, 2);
    EXPECT_EQ(r.col(keyOf({1}, "fpb", rune::codec::maxLoweredWidth(32)), "uses").i, 1);
    const auto key = r.col(fpa, "fpKey");
    EXPECT_EQ(std::string(key.bytes.substr(0, 3)), "fpa");
    EXPECT_EQ(key.bytes.size(), size_t(rune::codec::maxLoweredWidth(32)));
    EXPECT_TRUE(std::all_of(key.bytes.begin() + 3, key.bytes.end(), [](char c) { return c == 0; }));
}

TEST(CheckedWritesKeyedAggRegression, NarrowIntegerSourceKeyFoldsRegression) {
    const auto narrow = rune::codec::DescriptorBuilder("device", 2)
        .add("eventId", PrimType::UInt32, 1, true).add("userId", PrimType::UInt64).addFixedStr("deviceFp", 32).addFixedStr("model", 16)
        .add("platform", PrimType::Enum).add("region", PrimType::UInt16).add("shard", PrimType::UInt64, 2)
        .add("lastSeen", PrimType::Int64, 2).add("bytes", PrimType::UInt64, 2).build();
    Rig r(deviceHistory(), kNoSeed, narrow);
    Log log;
    log.src = narrow;
    log.keyBytes = 4;
    log.batch({ev(1, 1, "fpA", 100, 10), ev(0xFFFFFFFFu, 1, "fpA", 300, 20), ev(3, 2, "fpB", 50, 5)});
    const FoldStats st = r.foldLog(log);
    EXPECT_EQ(st.skippedMalformed, 0u);
    EXPECT_EQ(st.applied, 3u);
    EXPECT_EQ(r.view.size(), 2u);
    EXPECT_EQ(r.col(userFp(1, "fpA"), "uses").i, 2);
    EXPECT_EQ(r.col(userFp(1, "fpA"), "total").u, 30u);

    log.batch({gone(0xFFFFFFFFu)});
    r.foldLog(log);
    EXPECT_EQ(r.col(userFp(1, "fpA"), "uses").i, 1);
}

TEST(CheckedWritesKeyedAgg, EnumAndNarrowGroupKeys) {
    const std::vector<AuthoredField> cols = {{"uses", PrimType::Int64, 0, "", MergeRule::CountAll}};
    Rig en(aggregateOf({{"platform", 0, "platform"}}, cols, "platform"));
    EXPECT_EQ(en.spec.keyWidth, 4u);
    Log log;
    Ev a = ev(1, 1, "x", 1, 1, 2), b = ev(2, 1, "x", 1, 1, 2), c = ev(3, 1, "x", 1, 1, 7);
    log.batch({a, b, c});
    en.foldLog(log);
    ASSERT_EQ(en.view.size(), 2u);
    const auto enumKey = [](uint32_t v) { ViewKey k; for (int i = 3; i >= 0; --i) k.bytes[k.len++] = uint8_t(v >> (8 * i)); return k; };
    EXPECT_EQ(en.col(enumKey(2), "uses").i, 2);
    EXPECT_EQ(en.col(enumKey(7), "uses").i, 1);
    EXPECT_EQ(en.col(enumKey(2), "platform").u, 2u);

    Rig u16(aggregateOf({{"region", 0, "region"}}, cols, "region"));
    EXPECT_EQ(u16.spec.keyWidth, 2u);
    Ev r1 = ev(1, 1, "x", 1, 1), r2 = ev(2, 1, "x", 1, 1), r3 = ev(3, 1, "x", 1, 1);
    r1.region = 0x0102; r2.region = 0x0102; r3.region = 3;
    Log log2;
    log2.batch({r1, r2, r3});
    u16.foldLog(log2);
    ASSERT_EQ(u16.view.size(), 2u);
    ViewKey k1; k1.bytes[0] = 0x01; k1.bytes[1] = 0x02; k1.len = 2;
    ViewKey k3; k3.bytes[0] = 0x00; k3.bytes[1] = 0x03; k3.len = 2;
    EXPECT_EQ(u16.col(k1, "uses").i, 2);
    EXPECT_EQ(u16.col(k3, "uses").i, 1);
    EXPECT_EQ(u16.col(k1, "region").u, 0x0102u);
}

namespace {

// ---- a random log: inserts, group-moving updates, tombstones, v1 writers (nulls, no shard) ------------------------

struct Shape {
    const char* name;
    std::vector<KeyComponent> key;
    const char* sourceKey;
};

std::vector<Shape> shapes() {
    return {{"u64,u64", {{"userId", 0, "userId"}, {"shard", 0, "shard"}}, "userId"},
            {"u64,string32", {{"userId", 0, "userId"}, {"deviceFp", 0, "deviceFp"}}, "userId"},
            {"u64,lower(string16)", {{"userId", 0, "userId"}, {"model", 1, "modelKey"}}, "userId"},
            {"enum", {{"platform", 0, "platform"}}, "platform"}};
}

Ev randomEvent(std::mt19937& rng, uint64_t id) {
    static const char* const fps[] = {"fpA", "fpB", "FpA", "dev-1234567890", "z"};
    static const char* const models[] = {"Pixel", "pixel", "PIXEL", "iPhone", "Ünï", "x"};
    Ev e;
    e.id = id;
    if (rng() % 6 == 0) { e.tombstone = true; return e; }
    e.user = 1 + rng() % 3;
    e.fp = fps[rng() % 5];
    e.model = models[rng() % 6];
    e.platform = uint32_t(rng() % 4);
    e.shard = 1 + rng() % 3;
    e.lastSeen = int64_t(rng() % 50) - 10;
    e.bytes = rng() % 100;
    e.version = rng() % 5 == 0 ? 1 : 2;
    return e;
}

Log randomLog(std::mt19937& rng) {
    Log log;
    const int batches = 1 + int(rng() % 20);
    for (int b = 0; b < batches; ++b) {
        std::vector<Ev> evs;
        const int n = 1 + int(rng() % 12);
        for (int i = 0; i < n; ++i) evs.push_back(randomEvent(rng, 1 + rng() % 40));
        log.batch(evs);
    }
    return log;
}

// Every group key the log can produce (a record with a group, in any writer version), as view keys.
std::set<std::vector<uint8_t>> groupKeysOf(const Rig& r, const Log& log) {
    std::set<std::vector<uint8_t>> keys;
    rune::protocol::forEachRecordSpan(log.bytes.data(), log.bytes.size(), [&](const rune::protocol::RecordSpan& s) {
        if (s.value.empty()) return;
        const uint16_t version = uint16_t(uint8_t(s.value[2]) | (uint8_t(s.value[3]) << 8));
        const auto d = rune::codec::materializeAsOf(r.src, version);
        ViewKey k;
        if (deriveViewKey(r.spec, d, {reinterpret_cast<const uint8_t*>(s.value.data()), s.value.size()}, k) == DeriveKey::Ok)
            keys.insert(std::vector<uint8_t>(k.bytes.begin(), k.bytes.begin() + k.len));
    });
    return keys;
}

// One v3 record per pass: every dirty row of the five tables, as WorkerViewSet encodes it.
struct Stream {
    std::vector<std::vector<uint8_t>> records;
    void capture(Rig& r, int64_t O) {
        struct Part { ViewTable* t; uint8_t id, keyLen; uint32_t rowLen; };
        std::vector<Part> parts = {{&r.view, kKeyedTableView, uint8_t(r.spec.keyWidth), r.comp.rowLayout.rowLen},
                                   {&r.contributors, kKeyedTableContributors, 8, r.agg.contributorRow.rowLen},
                                   {&r.groups, kKeyedTableGroups, 8, r.agg.groupRow.rowLen}};
        if (r.agg.numBlockCols > 0) parts.push_back({&r.blocks, kKeyedTableBlocks, 8, r.agg.blockRow.rowLen});
        parts.push_back({&r.groupIds, kKeyedTableGroupIds, uint8_t(r.spec.keyWidth), 8});
        std::vector<std::vector<KeyedDirtyEntry>> entries(parts.size());
        std::vector<std::unique_ptr<std::vector<uint8_t>>> keep;
        for (size_t t = 0; t < parts.size(); ++t) {
            parts[t].t->drainDirtyKeyed([&](uint32_t rid, const uint8_t* row, bool deleted, std::span<const uint8_t> key) {
                keep.push_back(std::make_unique<std::vector<uint8_t>>(key.begin(), key.end()));
                const auto* k = keep.back().get();
                const uint8_t* rowCopy = nullptr;
                if (!deleted) {
                    keep.push_back(std::make_unique<std::vector<uint8_t>>(row, row + parts[t].rowLen));
                    rowCopy = keep.back()->data();
                }
                entries[t].push_back({rid, deleted, std::span<const uint8_t>(k->data(), k->size()), rowCopy});
            });
        }
        std::vector<KeyedTableDelta> tds;
        for (size_t t = 0; t < parts.size(); ++t) tds.push_back({parts[t].id, parts[t].keyLen, parts[t].rowLen, entries[t]});
        records.emplace_back();
        encodeKeyedDelta(tds, O, records.back());
    }
};

struct Replayed {
    ViewTable view, contributors, groups, blocks, groupIds;
    explicit Replayed(const Rig& r)
        : view(r.comp.rowLayout, 256, 1u << 16, ViewTableKeys{uint8_t(r.spec.keyWidth), uint8_t(r.spec.prefixWidth), 99}),
          contributors(r.agg.contributorRow, 256, 1u << 16, ViewTableKeys{8, 0, 99}), groups(r.agg.groupRow, 256, 1u << 16, ViewTableKeys{8, 0, 99}),
          blocks(r.agg.blockRow, 256, 1u << 16, ViewTableKeys{8, 0, 99}), groupIds(Rig::idRow(), 256, 1u << 16, ViewTableKeys{uint8_t(r.spec.keyWidth), 0, 99}) {}
};

}  // namespace

TEST(CheckedWritesKeyedAggRegression, GroupIdsAreInternalRegression) {
    for (uint64_t seed = 1; seed <= 20; ++seed) {
        std::mt19937 rng{uint32_t(seed)};
        const Log log = randomLog(rng);
        const ViewPlan plan = aggregateOf({{"userId", 0, "userId"}, {"deviceFp", 0, "deviceFp"}}, allRules());

        Rig one(plan);
        one.foldLog(log);

        // Another history: noise groups created and dropped first (the ids move on), then one record per batch with random pass bounds.
        Rig other(plan);
        Log noise;
        for (uint64_t i = 0; i < 5; ++i) noise.batch({ev(1000 + i, 90 + i, "noise", 1, 1)});
        other.foldLog(noise);
        for (uint64_t i = 0; i < 5; ++i) noise.batch({gone(1000 + i)});
        other.foldLog(noise);
        ASSERT_EQ(other.next, 5u);
        std::mt19937 bounds{uint32_t(seed) + 1000};
        other.foldLog(log, 0, &bounds);

        const auto keys = groupKeysOf(one, log);
        bool idsDiffer = false;
        for (const auto& kb : keys) {
            const ViewKey k = ViewKey::fromBytes(kb);
            EXPECT_EQ(one.rowOf(k), other.rowOf(k)) << "seed " << seed;
            const uint8_t* a = one.groupIds.get(k);
            const uint8_t* b = other.groupIds.get(k);
            ASSERT_EQ(a == nullptr, b == nullptr);
            if (a != nullptr && loadU64(a) != loadU64(b)) idsDiffer = true;
        }
        EXPECT_EQ(one.view.size(), other.view.size());
        if (!keys.empty() && one.groupIds.size() != 0) EXPECT_TRUE(idsDiffer) << "seed " << seed;
    }
}

TEST(CheckedWritesKeyedAggRegression, SingleEightByteGroupIsByteIdenticalRegression) {
    // count(*), sum(bytes) group by userId: the LegacyU64 path -- no keyed fold, no group key bytes, no groupIds.
    const auto src = deviceSource();
    const ViewPlan plan = aggregateOf({{"userId", 0, "userId"}},
                                      {{"uses", PrimType::Int64, 0, "", MergeRule::CountAll}, {"total", PrimType::UInt64, 0, "bytes", MergeRule::Sum}});
    const rune::codec::Descriptor* srcs[] = {&src};
    const auto comp = compileView(plan, srcs, kViewTopic).value();
    ASSERT_EQ(keyModeOf(comp.plan), ViewKeyMode::LegacyU64);
    const AggLayout agg = buildAggLayout(comp.plan, src).value();
    EXPECT_FALSE(agg.keyed);
    EXPECT_EQ(agg.groupRow.rowLen, 8u + 16u * 2 + 8u * 2);   // [count][2 accumulators][2 non-null counts]: no key bytes
    EXPECT_EQ(aggDirtyBoundPerRecord(agg).groupIdRows, 0u);

    Log log;
    log.batch({ev(1, 7, "a", 1, 10), ev(2, 7, "b", 2, 20), ev(3, 9, "a", 3, 30)});
    log.batch({ev(2, 9, "b", 2, 25), gone(1), ev(4, 7, "c", 4, 5)});

    const auto stream = [&](auto&& foldFn) {
        VersionDescriptorCache vdc{src};
        ViewTable view(comp.rowLayout, 256, 1u << 16), contributors(agg.contributorRow, 256, 1u << 16), groups(agg.groupRow, 256, 1u << 16);
        int64_t cursor = 0;
        foldFn(vdc, view, contributors, groups, cursor);
        std::vector<DirtyEntry> entries[3];
        std::vector<std::vector<uint8_t>> rows;
        rows.reserve(64);
        ViewTable* tables[3] = {&view, &contributors, &groups};
        const uint32_t lens[3] = {comp.rowLayout.rowLen, agg.contributorRow.rowLen, agg.groupRow.rowLen};
        for (int t = 0; t < 3; ++t) {
            tables[t]->drainDirty([&](uint32_t rid, const uint8_t* row, bool deleted, uint64_t key) {
                rows.emplace_back(row, row + (deleted ? 0 : lens[t]));
                entries[t].push_back({rid, deleted, key, deleted ? nullptr : rows.back().data()});
            });
        }
        const TableDelta tds[3] = {{kAggTableView, lens[0], entries[0]}, {kAggTableContributors, lens[1], entries[1]}, {kAggTableGroups, lens[2], entries[2]}};
        std::vector<uint8_t> out;
        encodeMultiDelta(tds, log.next, out);
        return out;
    };
    const auto current = stream([&](VersionDescriptorCache& vdc, ViewTable& v, ViewTable& c, ViewTable& g, int64_t& cursor) {
        (void)foldAggregateBatch(comp.plan, comp.rowLayout, agg, vdc, v, c, g, log.view(), log.next, cursor, kNoSkip);
    });
    const auto reference = stream([&](VersionDescriptorCache& vdc, ViewTable& v, ViewTable& c, ViewTable& g, int64_t& cursor) {
        (void)legacy_fold::foldAggregateBatch(comp.plan, comp.rowLayout, agg, vdc, v, c, g, nullptr, log.view(), log.next, cursor, kNoSkip);
    });
    EXPECT_EQ(current, reference);
    EXPECT_EQ(current, kLegacyGroupGolden);
}

TEST(CheckedWritesKeyedAggModelRegression, RandomLogsFoldToTheFreshRefold) {
    FoldStats seen{};   // the runs must reach the retraction paths, not only inserts
    size_t groupsFolded = 0, groupsDropped = 0;
    for (const Shape& shape : shapes()) {
        for (uint64_t seed = 1; seed <= 300; ++seed) {
            std::mt19937 rng{uint32_t(seed * 7919 + shape.name[0])};
            const Log log = randomLog(rng);
            const ViewPlan plan = aggregateOf(shape.key, allRules(), shape.sourceKey);

            Rig inc(plan);
            Stream stream;
            {
                std::mt19937 bounds{uint32_t(seed)};
                int64_t cursor = 0;
                while (cursor < log.next) {
                    const int64_t bound = std::min(log.next, cursor + 1 + int64_t(bounds() % 9));
                    auto f = inc.fold();
                    const FoldStats st = foldKeyedAggregateBatch(f, inc.p, log.view(), bound, cursor, kNoSkip);
                    seen.applied += st.applied; seen.deleted += st.deleted; seen.aggRescans += st.aggRescans;
                    seen.skippedNoGroup += st.skippedNoGroup; seen.aggInconsistent += st.aggInconsistent;
                    stream.capture(inc, cursor);
                }
            }
            Rig fresh(plan);
            fresh.foldLog(log);

            Replayed rep(inc);
            ViewTable* tables[6] = {&rep.view, nullptr, &rep.contributors, &rep.groups, inc.agg.numBlockCols > 0 ? &rep.blocks : nullptr, &rep.groupIds};
            const uint32_t lens[6] = {inc.comp.rowLayout.rowLen, 0, inc.agg.contributorRow.rowLen, inc.agg.groupRow.rowLen, inc.agg.blockRow.rowLen, 8};
            const uint8_t keyLens[6] = {uint8_t(inc.spec.keyWidth), 0, 8, 8, 8, uint8_t(inc.spec.keyWidth)};
            for (const auto& rec : stream.records) ASSERT_TRUE(applyKeyedDelta(tables, lens, keyLens, rec).has_value()) << shape.name << " seed " << seed;

            const auto keys = groupKeysOf(inc, log);
            for (const auto& kb : keys) {
                const ViewKey k = ViewKey::fromBytes(kb);
                const auto want = fresh.rowOf(k);
                EXPECT_EQ(inc.rowOf(k), want) << shape.name << " seed " << seed;
                const uint8_t* rr = rep.view.get(k);
                EXPECT_EQ(rr == nullptr ? std::vector<uint8_t>{} : std::vector<uint8_t>(rr, rr + lens[0]), want) << "replay " << shape.name << " seed " << seed;
                EXPECT_EQ(inc.groupIds.get(k) == nullptr, want.empty());
                EXPECT_EQ(rep.groupIds.get(k) == nullptr, want.empty());
                if (!want.empty()) EXPECT_EQ(loadU64(rep.groupIds.get(k)), loadU64(inc.groupIds.get(k)));
            }
            EXPECT_EQ(inc.view.size(), fresh.view.size()) << shape.name << " seed " << seed;
            groupsFolded += keys.size();
            groupsDropped += keys.size() - inc.view.size();
            EXPECT_EQ(rep.view.size(), fresh.view.size());
            EXPECT_EQ(inc.groups.size(), fresh.groups.size());
            EXPECT_EQ(inc.groupIds.size(), inc.groups.size());
            EXPECT_EQ(rep.groups.size(), inc.groups.size());
            EXPECT_EQ(rep.contributors.size(), inc.contributors.size());
            EXPECT_EQ(inc.agg.numBlockCols > 0 ? rep.blocks.size() : 0u, inc.agg.numBlockCols > 0 ? inc.blocks.size() : 0u);
        }
    }
    EXPECT_EQ(seen.aggInconsistent, 0u);
    EXPECT_GT(seen.applied, 20000u);
    EXPECT_GT(seen.deleted, 2000u);
    EXPECT_GT(seen.aggRescans, 500u);
    EXPECT_GT(seen.skippedNoGroup, 100u);
    EXPECT_GT(groupsFolded, 3000u);
    EXPECT_GT(groupsDropped, 500u);
}

// Part 11 Task 3: a keyed view's chained prepares see exactly what sequential prepares do, burn gids, and leave the tables alone.
TEST(CheckedWritesKeyedAgg, ChainedPreparesEqualSequentialProspectsRegression) {
    for (uint64_t seed = 1; seed <= 100; ++seed) {
        std::mt19937_64 rng(seed);
        const ViewPlan plan = aggregateOf({{"userId", 0, "userId"}, {"deviceFp", 0, "deviceFp"}}, allRules());
        Rig chained(plan), sequential(plan);
        TxnOverlay chain;
        chain.reserve(512, 4096);
        const char* fps[] = {"a", "b", "c"};
        for (size_t i = 0, n = 1 + rng() % 40; i < n; ++i) {
            const uint64_t id = rng() % 10;
            const std::string key = wireKey(id);
            const std::string value = rng() % 4 == 0 ? std::string() : valueOf(chained.src, ev(id, 1 + rng() % 3, fps[rng() % 3], int64_t(rng() % 20), rng() % 50));
            const rune::view::SourceRecord rec{key, value, int64_t(i), 0, nullptr};
            auto cf = chained.fold();
            auto sf = sequential.fold();
            ASSERT_EQ(prepareAggregate(cf, rec, chained.p, &chain), PrepareStatus::Ok) << "seed " << seed;
            ASSERT_EQ(prepareAggregate(sf, rec, sequential.p), PrepareStatus::Ok);
            const Prospect &c = chained.p, &s = sequential.p;
            ASSERT_EQ(c.op, s.op) << "seed " << seed << " record " << i;
            ASSERT_EQ(c.rowCount, s.rowCount);
            for (uint8_t j = 0; j < s.rowCount; ++j) {
                EXPECT_TRUE(c.rows[j].key == s.rows[j].key);
                EXPECT_EQ(c.rows[j].removed, s.rows[j].removed);
                EXPECT_EQ(c.rows[j].existed, s.rows[j].existed);
                if (s.rows[j].image != nullptr) EXPECT_EQ(std::memcmp(c.rows[j].image, s.rows[j].image, chained.comp.rowLayout.rowLen), 0);
            }
            applyAggregate(sf, rec, sequential.p);
        }
        EXPECT_EQ(chained.view.size() + chained.groups.size() + chained.contributors.size() + chained.groupIds.size(), 0u);
        EXPECT_EQ(chained.next == 0, chain.rows() == 0);
    }
}
