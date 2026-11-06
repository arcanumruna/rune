// Part 07 Task 7: the checkpoint file splitter knows v3 records.
#include <gtest/gtest.h>

#include <array>
#include <filesystem>
#include <initializer_list>
#include <stdexcept>
#include <string>
#include <vector>

#include <unistd.h>

#include <rune/node/AdminChannels.h>
#include <rune/node/ViewCheckpointScan.h>
#include <rune/node/ViewSnapshotChannel.h>
#include <rune/node/WorkerViewSet.h>
#include <rune/protocol/client/DataMessages.h>
#include <rune/view/ViewCheckpoint.h>

#include "CheckedWritesKeyedAggFixture.h"
#include "CheckedWritesKeyedViewFixture.h"

using namespace rune::view;

namespace {

std::vector<uint8_t> v1Record(int64_t O) {
    const uint8_t row[8] = {1, 2, 3, 4, 5, 6, 7, 8};
    const DirtyEntry e{0, false, 5, row};
    std::vector<uint8_t> out;
    encodeDelta({&e, 1}, O, 8, out);
    return out;
}

std::vector<uint8_t> v3Record(int64_t O) {
    const uint8_t row[8] = {1, 2, 3, 4, 5, 6, 7, 8};
    const std::array<uint8_t, 24> key{7};
    const KeyedDirtyEntry up{0, false, key, row};
    const KeyedDirtyEntry del{1, true, key, nullptr};
    const KeyedDirtyEntry es[] = {del, up};
    const KeyedTableDelta tds[] = {{0, 24, 8, es}, {1, 24, 8, {}}};
    std::vector<uint8_t> out;
    encodeKeyedDelta(tds, O, out);
    return out;
}

}  // namespace

TEST(CheckedWritesViewSet, SplitterAcceptsV3) {
    const auto a = v1Record(1), b = v3Record(2), c = v3Record(3);
    std::vector<uint8_t> file;
    file.insert(file.end(), a.begin(), a.end());
    file.insert(file.end(), b.begin(), b.end());
    file.insert(file.end(), c.begin(), c.end() - 3);   // the last v3 record is torn

    const auto recs = rune::node::splitViewCheckpointRecords(file);
    ASSERT_EQ(recs.size(), 2u);
    EXPECT_EQ(recs[0].size(), a.size());
    EXPECT_EQ(recs[1].size(), b.size());
    EXPECT_TRUE(std::equal(recs[1].begin(), recs[1].end(), b.begin()));

    const auto scan = rune::node::scanViewCheckpoint(file);
    EXPECT_TRUE(std::holds_alternative<rune::node::ViewCheckpointTornTail>(scan.tail));
}

namespace {

using namespace cwkeyed;
using Status = rune::protocol::client::ViewRowResponse;
using rune::node::WorkerViewSet;

std::string rigDir() {
    return (std::filesystem::temp_directory_path() / ("rune-cw-keyed-viewset-" + std::to_string(::getpid()))).string();
}

struct KeyedRig {
    rune::codec::Descriptor src = characterSource();
    rune::node::ViewSnapshotChannel ch;
    WorkerViewSet vs;
    Log log{src};
    std::vector<uint8_t> out = std::vector<uint8_t>(rune::node::kMaxScanPageBytes);

    explicit KeyedRig(uint64_t seed = 0) : vs(ch, rigDir(), seed) {}
    void install(const rune::view::CompileResult& c, bool persisted = false) { vs.installView(c.plan, c.rowLayout, src, persisted); }
    void fold() {
        vs.driveFolds(kSrcTopic, 0, [&](int64_t f, int64_t t) { return log.read(f, t); }, log.end(), rune::view::kNoSkip, 0);
    }
    WorkerViewSet::ScanPage scan(const std::vector<uint8_t>& prefix, std::span<const uint8_t> token = {}, uint16_t limit = 0,
                                 int32_t view = kViewTopic) {
        return vs.scanPage(view, 0, prefix, token, limit, out);
    }
};

std::vector<Rec> userRows(uint64_t user, uint64_t firstChar, int n) {
    std::vector<Rec> rs;
    for (int i = 0; i < n; ++i) rs.push_back(Rec{firstChar + static_cast<uint64_t>(i), user, "n", static_cast<uint16_t>(i)});
    return rs;
}

}  // namespace

TEST(CheckedWritesViewSet, KeyedInstanceFoldsAndServes) {
    KeyedRig r;
    const auto c = accountCharacters(r.src);
    r.install(c);
    r.log.append({{1, 42, "a", 10}});
    r.fold();
    r.log.append({{2, 42, "b", 20}});
    r.fold();
    r.log.append({{3, 43, "c", 30}});
    r.fold();

    const rune::view::RowLayout* layout = nullptr;
    WorkerViewSet::RowStatus st{};
    const auto key = beKey({42, 2});
    const uint8_t* row = r.vs.getRowK(kViewTopic, 0, key, layout, &st);
    ASSERT_NE(row, nullptr);
    EXPECT_EQ(st, WorkerViewSet::RowStatus::Ok);
    EXPECT_EQ(rune::view::readSlot(row, *layout, 2).u, 20u);

    EXPECT_EQ(r.vs.getRowK(kViewTopic, 0, beKey({42, 3}), layout, &st), nullptr);
    EXPECT_EQ(st, WorkerViewSet::RowStatus::NotFound);

    EXPECT_EQ(r.vs.getRow(kViewTopic, 0, 42, layout, &st), nullptr);
    EXPECT_EQ(st, WorkerViewSet::RowStatus::BadKey);
}

TEST(CheckedWritesViewSet, LegacyViewServesBothWays) {
    KeyedRig r;
    rune::view::ViewPlan v;
    v.viewName = "by_character";
    v.key      = "characterId";
    v.sources  = {{kSrcTopic, "characterId"}};
    v.output   = {{"characterId", PrimType::UInt64, 0, "characterId", rune::view::MergeRule::Replace},
                  {"gender", PrimType::UInt16, 0, "gender", rune::view::MergeRule::Replace}};
    const rune::codec::Descriptor* srcs[] = {&r.src};
    const auto c = rune::view::compileView(v, srcs, kViewTopic).value();
    r.install(c);
    r.log.append({{42, 7, "a", 5}});
    r.fold();

    const rune::view::RowLayout* l1 = nullptr;
    const rune::view::RowLayout* l2 = nullptr;
    const uint8_t* a = r.vs.getRow(kViewTopic, 0, 42, l1);
    const uint8_t* b = r.vs.getRowK(kViewTopic, 0, beKey({42}), l2);
    ASSERT_NE(a, nullptr);
    EXPECT_EQ(a, b);

    // A narrow u16 key: the u64 read cannot route it, the keyed read can.
    const auto coded = rune::codec::DescriptorBuilder("coded", 1).add("provider", PrimType::UInt16, 1, true).add("gender", PrimType::UInt16).build();
    rune::view::ViewPlan nv;
    nv.viewName = "by_provider";
    nv.key      = "provider";
    nv.sources  = {{6, "provider"}};
    nv.output   = {{"provider", PrimType::UInt16, 0, "provider", rune::view::MergeRule::Replace},
                   {"gender", PrimType::UInt16, 0, "gender", rune::view::MergeRule::Replace}};
    const rune::codec::Descriptor* nsrcs[] = {&coded};
    const auto nc = rune::view::compileView(nv, nsrcs, 0x40000008).value();
    r.vs.installView(nc.plan, nc.rowLayout, coded, false);
    std::string rec(4 + coded.blockLength, 0);
    rec[0] = static_cast<char>(coded.blockLength);
    rec[2] = static_cast<char>(coded.version);
    rec[4 + coded.find("provider")->offset] = 9;
    rune::protocol::RecordBatchBuilder bb(0, 0);
    bb.append(std::string("\x09\x00", 2), rec, 0);
    const auto bytes = bb.build();
    r.vs.driveFolds(6, 0, [&](int64_t, int64_t) { return rune::storage::LogReadView{bytes.data(), bytes.size(), 0, 1}; }, 1, rune::view::kNoSkip, 0);
    const uint8_t narrowKey[] = {0x00, 0x09};
    WorkerViewSet::RowStatus st{};
    EXPECT_NE(r.vs.getRowK(0x40000008, 0, narrowKey, l2, &st), nullptr);
    EXPECT_EQ(st, WorkerViewSet::RowStatus::Ok);
    EXPECT_EQ(r.vs.getRow(0x40000008, 0, 9, l2, &st), nullptr);
    EXPECT_EQ(st, WorkerViewSet::RowStatus::BadKey);
}

TEST(CheckedWritesViewSet, ScanPageFillsAndResumes) {
    KeyedRig r;
    r.install(accountCharacters(r.src));
    r.log.append(userRows(42, 1, 300));
    r.fold();

    const auto prefix = beKey({42});
    uint32_t total = 0;
    std::array<uint8_t, rune::view::kScanTokenBytes> token{};
    uint8_t tokenLen = 0;
    for (int page = 0; page < 3; ++page) {
        const auto p = r.scan(prefix, std::span<const uint8_t>(token.data(), tokenLen), 100);
        ASSERT_EQ(p.status, Status::kOk);
        EXPECT_EQ(p.rows, 100u);
        total += p.rows;
        tokenLen = p.nextTokenLen;
        token    = p.nextToken;
        EXPECT_EQ(tokenLen, page < 2 ? rune::view::kScanTokenBytes : 0u);
    }
    EXPECT_EQ(total, 300u);
    const auto* counters = r.vs.foldCounters(kViewTopic, 0);
    ASSERT_NE(counters, nullptr);
    EXPECT_EQ(counters->scanRows, 300u);
    EXPECT_EQ(counters->scanPages, 3u);
}

TEST(CheckedWritesViewSet, ScanPageRespectsTheByteBudget) {
    auto b = rune::codec::DescriptorBuilder("wide", 1).add("characterId", PrimType::UInt64, 1, true).add("userId", PrimType::UInt64);
    std::vector<std::string> select;
    for (int i = 0; i < 15; ++i) {
        select.push_back("s" + std::to_string(i));
        b.addFixedStr(select.back(), 255);
    }
    const auto wide = b.build();
    rune::view::ViewPlan v;
    v.viewName = "wide_view";
    v.sources  = {{kSrcTopic, "characterId"}};
    v.keyComponents = {{"userId", 0, "userId"}, {"characterId", 0, "characterId"}};
    for (const auto& s : select) v.output.push_back({s, PrimType::FixedStr, 0, s, rune::view::MergeRule::Replace});
    const rune::codec::Descriptor* srcs[] = {&wide};
    const auto c = rune::view::compileView(v, srcs, kViewTopic).value();

    rune::node::ViewSnapshotChannel ch;
    WorkerViewSet vs(ch, rigDir());
    vs.installView(c.plan, c.rowLayout, wide, false);
    rune::protocol::RecordBatchBuilder bb(0, 0);
    for (uint64_t id = 1; id <= 40; ++id) {
        std::string rec(4 + wide.blockLength, 0);
        rec[0] = static_cast<char>(wide.blockLength);
        rec[1] = static_cast<char>(wide.blockLength >> 8);
        rec[2] = static_cast<char>(wide.version);
        rec[4 + wide.find("characterId")->offset] = static_cast<char>(id);
        rec[4 + wide.find("userId")->offset]      = 42;
        bb.append(wireKey(id), rec, 0);
    }
    const auto bytes = bb.build();
    vs.driveFolds(kSrcTopic, 0, [&](int64_t, int64_t) { return rune::storage::LogReadView{bytes.data(), bytes.size(), 0, 40}; }, 40, rune::view::kNoSkip, 0);

    ASSERT_GE(c.rowLayout.rowLen, 3800u);
    std::vector<uint8_t> out(rune::node::kMaxScanPageBytes);
    const auto p = vs.scanPage(kViewTopic, 0, beKey({42}), {}, 0, out);
    ASSERT_EQ(p.status, Status::kOk);
    EXPECT_EQ(p.rows, rune::node::kMaxScanPageBytes / (c.rowLayout.rowLen + 4));
    EXPECT_EQ(p.rows, 15u);
    EXPECT_EQ(p.bytes, p.rows * (c.rowLayout.rowLen + 4));
    EXPECT_EQ(p.nextTokenLen, rune::view::kScanTokenBytes);
}

TEST(CheckedWritesViewSet, ScanStatuses) {
    KeyedRig r;
    r.install(accountCharacters(r.src));
    r.log.append(userRows(42, 1, 10));
    r.fold();
    const auto prefix = beKey({42});

    EXPECT_EQ(r.scan(beKey({42, 1})).status, Status::kInvalidRequest) << "a prefix of the wrong length";
    EXPECT_EQ(r.scan({}).status, Status::kInvalidRequest);
    const uint8_t shortToken[5] = {};
    EXPECT_EQ(r.scan(prefix, shortToken).status, Status::kInvalidRequest) << "a token of the wrong length";
    EXPECT_EQ(r.scan(prefix, {}, 0, 0x7777).status, Status::kUnknownView);

    const auto first = r.scan(prefix, {}, 4);
    ASSERT_EQ(first.status, Status::kOk);
    ASSERT_EQ(first.nextTokenLen, rune::view::kScanTokenBytes);
    const std::vector<uint8_t> token(first.nextToken.begin(), first.nextToken.end());
    EXPECT_EQ(r.scan(prefix, token, 4).status, Status::kOk);
    r.log.append({{99, 42, "z", 1}});   // an insert under the prefix
    r.fold();
    EXPECT_EQ(r.scan(prefix, token, 4).status, Status::kCursorInvalidated);
    EXPECT_EQ(r.scan(beKey({77}), token, 4).status, Status::kCursorInvalidated) << "a token for a prefix with no rows";

    ASSERT_TRUE(r.vs.beginDiscardForTest(kViewTopic, 0, rune::node::ViewDiscardReason::Unreadable));
    EXPECT_EQ(r.scan(prefix).status, Status::kNotReady);

    // A LegacyU64 view has no prefix index.
    KeyedRig legacy;
    rune::view::ViewPlan v;
    v.viewName = "by_character";
    v.key      = "characterId";
    v.sources  = {{kSrcTopic, "characterId"}};
    v.output   = {{"characterId", PrimType::UInt64, 0, "characterId", rune::view::MergeRule::Replace}};
    const rune::codec::Descriptor* srcs[] = {&legacy.src};
    legacy.install(rune::view::compileView(v, srcs, kViewTopic).value());
    EXPECT_EQ(legacy.scan(beKey({1})).status, Status::kInvalidRequest);
}

TEST(CheckedWritesViewSetRegression, ScanOfAnInstanceNotHostedIsNotAnEmptyPageRegression) {
    KeyedRig r;
    r.install(accountCharacters(r.src));
    r.log.append(userRows(42, 1, 3));
    r.fold();
    const rune::view::RowLayout* layout = nullptr;
    WorkerViewSet::RowStatus st{};
    EXPECT_EQ(r.vs.getRowK(kViewTopic, 7, beKey({42, 1}), layout, &st), nullptr);
    EXPECT_EQ(st, WorkerViewSet::RowStatus::NotFound);
    EXPECT_EQ(r.vs.scanPage(kViewTopic, 7, beKey({42}), {}, 0, r.out).status, Status::kAbsentKey);
    const std::vector<uint8_t> token(rune::view::kScanTokenBytes, 0);
    EXPECT_EQ(r.vs.scanPage(kViewTopic, 7, beKey({42}), token, 0, r.out).status, Status::kAbsentKey);
    EXPECT_EQ(r.scan(beKey({42})).status, Status::kOk);
}

TEST(CheckedWritesViewSetRegression, FixedSeedHookReachesEveryTableRegression) {
    KeyedRig r(7);
    EXPECT_EQ(r.vs.hashSeedForTest(), 7u);
    r.install(accountCharacters(r.src));
    r.log.append({{1, 42, "a", 1}});
    r.fold();
    const auto* inst = r.vs.instanceForTest(kViewTopic, 0);
    ASSERT_NE(inst, nullptr);
    ASSERT_TRUE(inst->owners.has_value());
    EXPECT_EQ(inst->table.hashSeedForTest(), 7u);
    EXPECT_EQ(inst->owners->hashSeedForTest(), 7u);

    KeyedRig plain;
    EXPECT_EQ(plain.vs.hashSeedForTest(), 0u);
}

TEST(CheckedWritesViewSetRegression, UnsupportedUnicodePlanIsNotInstalledRegression) {
    KeyedRig r;
    auto c = namedCharacters(r.src);
    c.plan.unicodeVersion = rune::codec::packUnicodeVersion(17, 0, 0);
    rune::node::ViewPlanUpdateMsg m;
    m.viewTopicId = c.plan.viewTopicId;
    m.planBlob    = rune::view::serialize(c.plan);
    m.outDescBlob = rune::codec::serialize(c.outputDescriptor);
    m.srcDescBlob = rune::codec::serialize(r.src);
    ASSERT_TRUE(rune::node::encodeViewPlanUpdate(r.ch.install, rune::node::ViewSnapshotChannel::kInstallMsgType, m));
    (void)r.vs.drainInstalls();
    EXPECT_EQ(r.vs.installRefusals(), 1u);
    EXPECT_EQ(r.vs.viewCount(), 0u);
    const rune::view::RowLayout* layout = nullptr;
    EXPECT_EQ(r.vs.getRowK(c.plan.viewTopicId, 0, nameKey("foo", 1), layout), nullptr);
    EXPECT_EQ(layout, nullptr);

    // The supported version installs.
    c.plan.unicodeVersion = rune::codec::kUnicodeVersion;
    m.planBlob = rune::view::serialize(c.plan);
    ASSERT_TRUE(rune::node::encodeViewPlanUpdate(r.ch.install, rune::node::ViewSnapshotChannel::kInstallMsgType, m));
    (void)r.vs.drainInstalls();
    EXPECT_EQ(r.vs.installRefusals(), 1u);
    EXPECT_EQ(r.vs.viewCount(), 1u);
}

TEST(CheckedWritesViewSet, MaxRecordsPerPassBoundsAV3Delta) {
    auto b = rune::codec::DescriptorBuilder("wide", 1).add("characterId", PrimType::UInt64, 1, true).addFixedStr("tag", 80);
    for (int i = 0; i < 3; ++i) b.addFixedStr("s" + std::to_string(i), 100);
    const auto wide = b.build();
    rune::view::ViewPlan v;
    v.viewName = "wide_view";
    v.sources  = {{kSrcTopic, "characterId"}};
    v.unicodeVersion = rune::codec::kUnicodeVersion;
    v.keyComponents  = {{"tag", 1, "tagKey"}, {"characterId", 0, "characterId"}};
    for (int i = 0; i < 3; ++i) v.output.push_back({"s" + std::to_string(i), PrimType::FixedStr, 0, "s" + std::to_string(i), rune::view::MergeRule::Replace});
    const rune::codec::Descriptor* srcs[] = {&wide};
    const auto c = rune::view::compileView(v, srcs, kViewTopic).value();
    constexpr int64_t K = 128, W = 8;
    const int64_t rowLen = c.rowLayout.rowLen;
    ASSERT_GE(rowLen, 300);

    rune::node::ViewSnapshotChannel ch;
    WorkerViewSet vs(ch, rigDir());
    vs.installView(c.plan, c.rowLayout, wide, true);

    const int64_t fixed = rune::node::ViewSnapshotChannel::kDeltaHeaderBytes + rune::view::kCheckpointCrcBytes + rune::view::kCheckpointV3HeaderBytes +
                          2 * rune::view::kCheckpointV3TableHeaderBytes;
    const int64_t perMove = (5 + K) + (5 + K + rowLen) + (5 + W + K);
    const int64_t maxMsg = ch.deltas.maxMsgLength();

    auto write = [&](rune::protocol::RecordBatchBuilder& bb, uint64_t id, char tag) {
        std::string rec(4 + wide.blockLength, 0);
        rec[0] = static_cast<char>(wide.blockLength);
        rec[1] = static_cast<char>(wide.blockLength >> 8);
        rec[2] = static_cast<char>(wide.version);
        rec[4 + wide.find("characterId")->offset] = static_cast<char>(id);
        rec[4 + wide.find("characterId")->offset + 1] = static_cast<char>(id >> 8);
        rec[4 + wide.find("tag")->offset] = tag;
        bb.append(wireKey(id), rec, 0);
    };
    // A drive with nothing committed creates the instance, which tells how many records one pass may cover.
    vs.driveFolds(kSrcTopic, 0, [&](int64_t, int64_t) { return rune::storage::LogReadView{}; }, 0, rune::view::kNoSkip, 0);
    const int64_t perPass = vs.maxRecordsPerPassForTest(kViewTopic, 0);
    ASSERT_GE(perPass, 1);
    EXPECT_EQ(perPass, (maxMsg - fixed) / perMove);

    // Pass 1 creates perPass rows, pass 2 renames every one of them: the worst case, every record a move.
    rune::protocol::RecordBatchBuilder bb(0, 0);
    for (int64_t i = 0; i < perPass; ++i) write(bb, static_cast<uint64_t>(i + 1), 'a');
    for (int64_t i = 0; i < perPass; ++i) write(bb, static_cast<uint64_t>(i + 1), 'b');
    const auto bytes = bb.build();
    auto read = [&](int64_t, int64_t) { return rune::storage::LogReadView{bytes.data(), bytes.size(), 0, 2 * perPass}; };
    vs.driveFolds(kSrcTopic, 0, read, 2 * perPass, rune::view::kNoSkip, 0);
    vs.driveFolds(kSrcTopic, 0, read, 2 * perPass, rune::view::kNoSkip, 0);
    std::vector<std::size_t> frames;
    ch.deltas.drain([&](int32_t, aeron::concurrent::AtomicBuffer&, rune::node::OneToOneByteRing::index_t, rune::node::OneToOneByteRing::index_t len) {
        frames.push_back(static_cast<std::size_t>(len));
    });
    ASSERT_EQ(frames.size(), 2u);
    EXPECT_EQ(static_cast<int64_t>(frames[1]), fixed + perPass * perMove);
    EXPECT_LE(static_cast<int64_t>(frames[1]), maxMsg);
}

namespace {

struct AggRig {
    rune::codec::Descriptor src = cwagg::deviceSource();
    rune::view::CompileResult comp = cwagg::deviceHistory(src);
    rune::node::ViewSnapshotChannel ch;
    WorkerViewSet vs;
    cwagg::Log log{src};
    std::vector<uint8_t> out = std::vector<uint8_t>(rune::node::kMaxScanPageBytes);

    explicit AggRig(uint64_t seed = 0) : vs(ch, rigDir(), seed) { vs.installView(comp.plan, comp.rowLayout, src, /*persisted=*/false); }
    uint16_t slot(const char* name) const {
        for (size_t i = 0; i < comp.outputDescriptor.fields.size(); ++i)
            if (comp.outputDescriptor.fields[i].name == name) return static_cast<uint16_t>(i);
        throw std::runtime_error(name);
    }
    void fold() {
        vs.driveFolds(cwagg::kSrcTopic, 0, [&](int64_t f, int64_t t) { return log.read(f, t); }, log.end(), rune::view::kNoSkip, 0);
    }
};

}  // namespace

TEST(CheckedWritesViewSet, KeyedAggregateServesPrefixReads) {
    AggRig r;
    std::vector<cwagg::Ev> evs;
    for (uint64_t i = 0; i < 12; ++i) evs.push_back({i + 1, 42, "fp" + std::string(1, static_cast<char>('A' + i)), 1, static_cast<int64_t>(100 + i), 10});
    evs.push_back({13, 42, "fpA", 2, 500, 5});   // a second contributor of the first device
    evs.push_back({14, 43, "fpA", 1, 100, 1});   // another user's device
    r.log.append(evs);
    r.fold();

    const auto p = r.vs.scanPage(cwagg::kViewTopic, 0, cwkeyed::beKey({42}), {}, 0, r.out);
    ASSERT_EQ(p.status, Status::kOk);
    EXPECT_EQ(p.rows, 12u);
    EXPECT_EQ(p.nextTokenLen, 0u);

    const rune::view::RowLayout* layout = nullptr;
    WorkerViewSet::RowStatus st{};
    const uint8_t* row = r.vs.getRowK(cwagg::kViewTopic, 0, cwagg::groupKey(42, "fpA"), layout, &st);
    ASSERT_NE(row, nullptr);
    EXPECT_EQ(st, WorkerViewSet::RowStatus::Ok);
    EXPECT_EQ(rune::view::readSlot(row, *layout, r.slot("uses")).i, 2);
    EXPECT_EQ(rune::view::readSlot(row, *layout, r.slot("total")).u, 15u);
    EXPECT_EQ(r.vs.getRowK(cwagg::kViewTopic, 0, cwagg::groupKey(44, "fpA"), layout, &st), nullptr);
}

TEST(CheckedWritesViewSetRegression, FixedSeedHookReachesEveryAggregateTableRegression) {
    AggRig r(7);
    r.log.append({{1, 42, "fpA", 1, 100, 10}});
    r.fold();
    const auto* inst = r.vs.instanceForTest(cwagg::kViewTopic, 0);
    ASSERT_NE(inst, nullptr);
    ASSERT_TRUE(inst->agg.has_value());
    ASSERT_TRUE(inst->agg->blocks.has_value());
    ASSERT_TRUE(inst->agg->groupIds.has_value());
    EXPECT_EQ(inst->table.hashSeedForTest(), 7u);
    EXPECT_EQ(inst->agg->contributors.hashSeedForTest(), 7u);
    EXPECT_EQ(inst->agg->groups.hashSeedForTest(), 7u);
    EXPECT_EQ(inst->agg->blocks->hashSeedForTest(), 7u);
    EXPECT_EQ(inst->agg->groupIds->hashSeedForTest(), 7u);
}

TEST(CheckedWritesViewSet, ConstrainedTemplatesCarryKindObjectIdAndFingerprint) {
    KeyedRig r;
    auto uniq = namedCharacters(r.src);
    uniq.plan.unique       = true;
    uniq.plan.viewObjectId = 7;
    auto chk = accountCharacters(r.src);
    chk.plan.viewTopicId  = kViewTopic + 1;
    chk.plan.viewObjectId = 8;
    chk.plan.constraints  = {{1, "c", {}}};
    auto plain = accountCharacters(r.src);
    plain.plan.viewTopicId = kViewTopic + 2;

    const auto install = [&](const rune::view::CompileResult& c) {
        rune::node::ViewPlanUpdateMsg m;
        m.viewTopicId = c.plan.viewTopicId;
        m.planBlob    = rune::view::serialize(c.plan);
        m.outDescBlob = rune::codec::serialize(c.outputDescriptor);
        m.srcDescBlob = rune::codec::serialize(r.src);
        EXPECT_TRUE(rune::node::encodeViewPlanUpdate(r.ch.install, rune::node::ViewSnapshotChannel::kInstallMsgType, m));
        (void)r.vs.drainInstalls();
        return m.planBlob;
    };
    const auto uniqBlob = install(uniq);
    const auto chkBlob  = install(chk);
    (void)install(plain);
    ASSERT_EQ(r.vs.viewCount(), 3u);

    const auto u = r.vs.verdictIdentityForTest(uniq.plan.viewTopicId);
    ASSERT_TRUE(u.has_value());
    EXPECT_EQ(u->kind, VerdictViewKind::Unique);
    EXPECT_EQ(u->objectId, 7u);
    EXPECT_EQ(u->fingerprint, verdictPlanFingerprint(uniqBlob));
    const auto c = r.vs.verdictIdentityForTest(chk.plan.viewTopicId);
    ASSERT_TRUE(c.has_value());
    EXPECT_EQ(c->kind, VerdictViewKind::Check);
    EXPECT_EQ(c->objectId, 8u);
    EXPECT_EQ(c->fingerprint, verdictPlanFingerprint(chkBlob));
    EXPECT_FALSE(r.vs.verdictIdentityForTest(plain.plan.viewTopicId).has_value());

    uniq.plan.viewTopicId = kViewTopic + 3;
    (void)install(uniq);
    const auto replaced = r.vs.verdictIdentityForTest(kViewTopic + 3);
    ASSERT_TRUE(replaced.has_value());
    EXPECT_NE(replaced->fingerprint, u->fingerprint);
}
