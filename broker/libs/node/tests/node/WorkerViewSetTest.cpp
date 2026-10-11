// libs/node/tests/node/WorkerViewSetTest.cpp
//
// Task 15: WorkerViewSet — per-worker view-fold driver.  Deterministic,
// in-process: a fake committed-range readRange (no real Log, no Aeron, no
// clock) feeds foldBatch; the gate test proves a committed range folds into
// the in-memory table and getRow() serves the latest row.
#include <gtest/gtest.h>

#include <array>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <limits>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include <rune/codec/Descriptor.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/storage/LogReadView.h>
#include <rune/view/PlanCompiler.h>
#include <rune/view/ViewRow.h>
#include <rune/view/ViewCheckpoint.h>

#include <rune/node/ViewSnapshotChannel.h>
#include <rune/node/WorkerViewSet.h>

#include <spdlog/sinks/ringbuffer_sink.h>
#include <spdlog/spdlog.h>

namespace {

// v1 "character" source: {characterId: Int64 key (tag 0), hp: Int32 (tag 1)}.
rune::codec::Descriptor charSourceV1() {
    return rune::codec::DescriptorBuilder("character", 1)
        .add("characterId", rune::codec::PrimType::Int64, 1, /*isKey*/ true)
        .add("hp", rune::codec::PrimType::Int32)
        .build();
}

// char_hp(characterId, hp) <- source topicId 5 "characterId"/"hp".
rune::view::ViewPlan hpView(uint8_t persistence = 0) {
    rune::view::ViewPlan v;
    v.viewName    = "char_hp";
    v.key         = "characterId";
    v.sources     = {{5, "characterId"}};
    v.output      = {{"characterId", rune::codec::PrimType::Int64, 0, "characterId",
                     rune::view::MergeRule::Replace},
                   {"hp", rune::codec::PrimType::Int32, 0, "hp", rune::view::MergeRule::Replace}};
    v.persistence = persistence;
    return v;
}

std::vector<uint8_t> keyBytes(int64_t id) {
    std::vector<uint8_t> b(8);
    for (int i = 0; i < 8; ++i) b[static_cast<size_t>(i)] = static_cast<uint8_t>(uint64_t(id) >> (8 * i));
    return b;
}
std::string_view asSv(const std::vector<uint8_t>& b) {
    return std::string_view(reinterpret_cast<const char*>(b.data()), b.size());
}

// [blockLength u16][version u16][id i64][hp i32] — a v1 "character" value.
std::vector<uint8_t> encodeCharacter(int64_t id, int32_t hp, uint16_t version) {
    std::vector<uint8_t> b;
    auto le16 = [&](uint16_t v) { b.push_back(uint8_t(v)); b.push_back(uint8_t(v >> 8)); };
    auto le32 = [&](int32_t v) { for (int i = 0; i < 4; ++i) b.push_back(uint8_t(uint32_t(v) >> (8 * i))); };
    auto le64 = [&](int64_t v) { for (int i = 0; i < 8; ++i) b.push_back(uint8_t(uint64_t(v) >> (8 * i))); };
    le16(8 + 4);
    le16(version);
    le64(id);
    le32(hp);
    return b;
}

// Two upserts for the SAME wire key 7 (hp 10 then hp 20) in one committed
// batch at offsets [0,2).
std::vector<uint8_t> twoUpsertsKey7() {
    rune::protocol::RecordBatchBuilder b(/*baseOffset*/ 0, /*baseTimestamp*/ 0);
    const auto k  = keyBytes(7);
    const auto v0 = encodeCharacter(7, 10, 1);
    const auto v1 = encodeCharacter(7, 20, 1);
    b.append(asSv(k), asSv(v0), 0);
    b.append(asSv(k), asSv(v1), 0);
    return b.build();
}

// One upsert (key, hp) in a single-record batch at offset [0,1).
std::vector<uint8_t> oneUpsert(int64_t key, int32_t hp) {
    rune::protocol::RecordBatchBuilder b(/*baseOffset*/ 0, /*baseTimestamp*/ 0);
    const auto k = keyBytes(key);
    const auto v = encodeCharacter(key, hp, 1);
    b.append(asSv(k), asSv(v), 0);
    return b.build();
}

} // namespace

// Gate test (brief Step 1): a committed range of two upserts for key 7 folds,
// and getRow serves the LATEST row (hp=20).
TEST(WorkerViewSet, FoldsCommittedRangeAndServesRow) {
    auto src = charSourceV1();
    const rune::codec::Descriptor* srcs[] = {&src};
    auto comp = rune::view::compileView(hpView(), srcs, 0x40000007).value();

    rune::node::ViewSnapshotChannel channel;
    rune::node::WorkerViewSet vs(channel, "/tmp/rune-views-test");
    vs.installView(comp.plan, comp.rowLayout, src, /*persisted*/ true);

    const auto batch = twoUpsertsKey7();
    auto readRange = [&](int64_t /*from*/, int64_t /*to*/) -> rune::storage::LogReadView {
        return rune::storage::LogReadView{batch.data(), batch.size(), 0, 2};
    };
    vs.driveFolds(/*srcTopicId*/ 5, /*partition*/ 0, readRange, /*bound*/ 2, /*nowNs*/ 0);

    const rune::view::RowLayout* L = nullptr;
    const uint8_t* row = vs.getRow(0x40000007, /*partition*/ 0, 7, L);
    ASSERT_NE(row, nullptr);
    ASSERT_NE(L, nullptr);
    EXPECT_EQ(rune::view::readSlot(row, *L, 1).i, 20);
}

// A persisted view pushes exactly one checkpoint delta frame onto the
// channel's deltas ring, routed [viewTopicId i32][partition i32][delta...].
TEST(WorkerViewSet, PersistedViewPushesCheckpointDelta) {
    auto src = charSourceV1();
    const rune::codec::Descriptor* srcs[] = {&src};
    auto comp = rune::view::compileView(hpView(0), srcs, 0x40000007).value();

    rune::node::ViewSnapshotChannel channel;
    rune::node::WorkerViewSet vs(channel, "/tmp/rune-views-test");
    vs.installView(comp.plan, comp.rowLayout, src, /*persisted*/ true);

    const auto batch = twoUpsertsKey7();
    auto readRange = [&](int64_t, int64_t) -> rune::storage::LogReadView {
        return rune::storage::LogReadView{batch.data(), batch.size(), 0, 2};
    };
    vs.driveFolds(5, 0, readRange, 2, 0);

    int frames = 0;
    int32_t gotView = 0, gotPart = -1;
    channel.deltas.drain([&](int32_t msgType, aeron::concurrent::AtomicBuffer& buf,
                             rune::node::OneToOneByteRing::index_t off,
                             rune::node::OneToOneByteRing::index_t len) {
        ++frames;
        EXPECT_EQ(msgType, rune::node::ViewSnapshotChannel::kDeltaMsgType);
        ASSERT_GE(len, static_cast<rune::node::OneToOneByteRing::index_t>(
                           rune::node::ViewSnapshotChannel::kDeltaHeaderBytes));
        const uint8_t* p = buf.buffer() + off;
        std::memcpy(&gotView, p, 4);
        std::memcpy(&gotPart, p + 4, 4);
    });
    EXPECT_EQ(frames, 1);
    EXPECT_EQ(gotView, 0x40000007);
    EXPECT_EQ(gotPart, 0);
}

// A transient view folds + serves rows but NEVER emits a checkpoint delta.
TEST(WorkerViewSet, TransientViewSkipsCheckpoint) {
    auto src = charSourceV1();
    const rune::codec::Descriptor* srcs[] = {&src};
    auto comp = rune::view::compileView(hpView(1), srcs, 0x40000007).value();

    rune::node::ViewSnapshotChannel channel;
    rune::node::WorkerViewSet vs(channel, "/tmp/rune-views-test");
    vs.installView(comp.plan, comp.rowLayout, src, /*persisted*/ false);

    const auto batch = twoUpsertsKey7();
    auto readRange = [&](int64_t, int64_t) -> rune::storage::LogReadView {
        return rune::storage::LogReadView{batch.data(), batch.size(), 0, 2};
    };
    vs.driveFolds(5, 0, readRange, 2, 0);

    const rune::view::RowLayout* L = nullptr;
    const uint8_t* row = vs.getRow(0x40000007, /*partition*/ 0, 7, L);
    ASSERT_NE(row, nullptr);
    EXPECT_EQ(rune::view::readSlot(row, *L, 1).i, 20);

    int frames = 0;
    channel.deltas.drain([&](int32_t, aeron::concurrent::AtomicBuffer&,
                             rune::node::OneToOneByteRing::index_t,
                             rune::node::OneToOneByteRing::index_t) { ++frames; });
    EXPECT_EQ(frames, 0) << "transient view must not emit a checkpoint delta";
}

// driveFolds for a source topic with no installed view is a no-op (no crash,
// no fold, no push).
TEST(WorkerViewSet, UninterestedSourceIsNoOp) {
    rune::node::ViewSnapshotChannel channel;
    rune::node::WorkerViewSet vs(channel, "/tmp/rune-views-test");
    bool called = false;
    auto readRange = [&](int64_t, int64_t) -> rune::storage::LogReadView {
        called = true;
        return rune::storage::LogReadView{};
    };
    vs.driveFolds(/*srcTopicId*/ 99, /*partition*/ 0, readRange, /*bound*/ 5, /*nowNs*/ 0);
    EXPECT_FALSE(called);
}

// *Regression* (review IMPORTANT 1): a single WorkerViewSet drives partition 0
// and partition 1 of the SAME source topic into ONE view.  Offsets are
// partition-local, so each (view, partition) MUST carry its own foldCursor +
// its own ViewTable.  Pre-fix (one ViewInstance per view, keyed by viewTopicId
// alone): the p1 drive resumes at p0's advanced cursor and folds p1's records
// into p0's shared table — key 8 bleeds into partition 0, and the two
// partitions' rows collide in one table.  Post-fix: each partition's table
// reflects ONLY its own partition's records; cursors are independent.
TEST(WorkerViewSetRegression, PerPartitionFoldCursorsAreIndependent) {
    auto src = charSourceV1();
    const rune::codec::Descriptor* srcs[] = {&src};
    auto comp = rune::view::compileView(hpView(0), srcs, 0x40000007).value();

    rune::node::ViewSnapshotChannel channel;
    rune::node::WorkerViewSet vs(channel, "/tmp/rune-views-test");
    vs.installView(comp.plan, comp.rowLayout, src, /*persisted*/ true);

    // DIFFERENT committed ranges per partition: p0 => key 7 hp=10 (1 record);
    // p1 => key 8 hp=99 (1 record).  p0 is driven first so its cursor advances.
    const auto p0 = oneUpsert(7, 10);
    const auto p1 = oneUpsert(8, 99);
    auto readP0 = [&](int64_t, int64_t) -> rune::storage::LogReadView {
        return rune::storage::LogReadView{p0.data(), p0.size(), 0, 1};
    };
    auto readP1 = [&](int64_t, int64_t) -> rune::storage::LogReadView {
        return rune::storage::LogReadView{p1.data(), p1.size(), 0, 1};
    };
    vs.driveFolds(/*srcTopicId*/ 5, /*partition*/ 0, readP0, /*bound*/ 1, 0);
    vs.driveFolds(/*srcTopicId*/ 5, /*partition*/ 1, readP1, /*bound*/ 1, 0);

    EXPECT_EQ(vs.instanceCount(), 2u);  // one ViewInstance per (view, partition)

    const rune::view::RowLayout* L = nullptr;

    // Partition 0's table: key 7 (hp=10) only — NOT key 8.
    const uint8_t* p0k7 = vs.getRow(0x40000007, /*partition*/ 0, 7, L);
    ASSERT_NE(p0k7, nullptr);
    EXPECT_EQ(rune::view::readSlot(p0k7, *L, 1).i, 10);
    EXPECT_EQ(vs.getRow(0x40000007, /*partition*/ 0, 8, L), nullptr)
        << "partition 1's key 8 must NOT bleed into partition 0's table";

    // Partition 1's table: key 8 (hp=99) only — NOT key 7.
    const uint8_t* p1k8 = vs.getRow(0x40000007, /*partition*/ 1, 8, L);
    ASSERT_NE(p1k8, nullptr);
    EXPECT_EQ(rune::view::readSlot(p1k8, *L, 1).i, 99);
    EXPECT_EQ(vs.getRow(0x40000007, /*partition*/ 1, 7, L), nullptr)
        << "partition 0's key 7 must NOT bleed into partition 1's table";
}

// REGRESSION (restart-recovery serve-guard). A view installed FOR RECOVERY
// (underRecovery) whose per-partition instance has NOT been materialized yet —
// e.g. the owned source replica is still being re-assigned post-restart, so
// driveViews_ has not yet run materializeRecoveryInstancesFor for it — must
// answer a RETRIABLE NotReady, never a terminal AbsentKey/NotFound. The live
// docker e2e caught this: a restarted broker reported a durable row "absent"
// before it was reloaded, and the client (waiting for recovery) gave up early.
// The existing serve-guard only covered an EXISTING RebuildingViews instance;
// with no instance at all it was bypassed and getRow fell through to NotFound.
TEST(WorkerViewSetRegression, UnderRecoveryViewNoInstanceServesNotReady) {
    auto src = charSourceV1();
    const rune::codec::Descriptor* srcs[] = {&src};
    auto comp = rune::view::compileView(hpView(/*persistence*/ 0), srcs, 0x40000007).value();

    rune::node::ViewSnapshotChannel channel;
    rune::node::WorkerViewSet vs(channel, "/tmp/rune-views-recovery-notready");
    // Install FOR RECOVERY; deliberately do NOT materialize the instance.
    vs.installView(comp.plan, comp.rowLayout, src, /*persisted*/ true, /*underRecovery*/ true);

    const rune::view::RowLayout* layout = nullptr;
    auto rs = rune::node::WorkerViewSet::RowStatus::Ok;
    const uint8_t* row = vs.getRow(comp.plan.viewTopicId, /*partition*/ 0, /*key*/ 4242, layout, &rs);
    EXPECT_EQ(row, nullptr);
    EXPECT_EQ(rs, rune::node::WorkerViewSet::RowStatus::NotReady)
        << "under-recovery view with no materialized instance must be NotReady, not AbsentKey";
}

// A PLAIN (non-recovery) view with no instance keeps the terminal NotFound
// answer: its instance is created serve-ready on the first driveFolds, so a
// missing instance means the key is genuinely absent (AbsentKey), not recovery.
TEST(WorkerViewSetRegression, PlainViewNoInstanceServesNotFound) {
    auto src = charSourceV1();
    const rune::codec::Descriptor* srcs[] = {&src};
    auto comp = rune::view::compileView(hpView(/*persistence*/ 0), srcs, 0x40000007).value();

    rune::node::ViewSnapshotChannel channel;
    rune::node::WorkerViewSet vs(channel, "/tmp/rune-views-plain-notfound");
    vs.installView(comp.plan, comp.rowLayout, src, /*persisted*/ false);  // underRecovery defaults false

    const rune::view::RowLayout* layout = nullptr;
    auto rs = rune::node::WorkerViewSet::RowStatus::Ok;
    const uint8_t* row = vs.getRow(comp.plan.viewTopicId, /*partition*/ 0, /*key*/ 4242, layout, &rs);
    EXPECT_EQ(row, nullptr);
    EXPECT_EQ(rs, rune::node::WorkerViewSet::RowStatus::NotFound)
        << "plain view with no instance is a genuine miss (AbsentKey), not NotReady";
}

TEST(WorkerViewSetRegression, SplitterReturnsV1AndV2RecordsInFileOrder) {
    uint8_t row[8] = {};
    const rune::view::DirtyEntry e{0, false, 1, row};
    std::vector<uint8_t> file;
    rune::view::encodeDelta(std::span<const rune::view::DirtyEntry>(&e, 1), 1, 8, file);
    const size_t v1Len = file.size();
    const rune::view::TableDelta tds[] = {{0, 8, {&e, 1}}};
    rune::view::encodeMultiDelta(tds, 2, file);
    rune::view::encodeDelta(std::span<const rune::view::DirtyEntry>(&e, 1), 3, 8, file);

    const auto recs = rune::node::splitViewCheckpointRecords(file);
    ASSERT_EQ(recs.size(), 3u);
    EXPECT_EQ(recs[0].size(), v1Len);
    uint32_t magic; std::memcpy(&magic, recs[1].data(), 4);
    EXPECT_EQ(magic, rune::view::kViewCheckpointMagicV2);

    auto torn = file; torn.resize(torn.size() - 2);
    EXPECT_EQ(rune::node::splitViewCheckpointRecords(torn).size(), 2u) << "torn trailing record dropped";
}

namespace {
// "order": {orderId Int64 (key), userId Int64, amount Int64} — the e2e shape.
rune::codec::Descriptor orderSrc() {
    return rune::codec::DescriptorBuilder("order", 1)
        .add("orderId", rune::codec::PrimType::Int64, 1, true)
        .add("userId", rune::codec::PrimType::Int64)
        .add("amount", rune::codec::PrimType::Int64)
        .build();
}
rune::view::ViewPlan userTotalsPlan(uint8_t persistence) {
    using P = rune::codec::PrimType; using R = rune::view::MergeRule;
    rune::view::ViewPlan v;
    v.viewName = "user_totals"; v.key = "userId"; v.sources = {{6, "userId"}};
    v.output = {{"userId", P::Int64, 0, "userId", R::Replace},
                {"n", P::Int64, 0, "", R::CountAll},
                {"total", P::Int64, 0, "amount", R::Sum}};
    v.persistence = persistence;
    return v;
}
std::vector<uint8_t> orderValue(int64_t orderId, int64_t userId, int64_t amount) {
    std::vector<uint8_t> b;
    auto le16 = [&](uint16_t v) { b.push_back(uint8_t(v)); b.push_back(uint8_t(v >> 8)); };
    auto le64 = [&](int64_t v) { for (int i = 0; i < 8; ++i) b.push_back(uint8_t(uint64_t(v) >> (8 * i))); };
    le16(24); le16(1); le64(orderId); le64(userId); le64(amount);
    return b;
}
// One batch of (orderId, userId, amount) at `base`.
std::vector<uint8_t> orderBatch(int64_t base, std::vector<std::array<int64_t, 3>> recs) {
    rune::protocol::RecordBatchBuilder b(base, 0);
    for (const auto& r : recs) {
        const auto k = keyBytes(r[0]);
        const auto v = orderValue(r[0], r[1], r[2]);
        b.append(asSv(k), asSv(v), 0);
    }
    return b.build();
}
// Drains every delta frame on `ch` into payload byte vectors (header stripped).
std::vector<std::vector<uint8_t>> drainDeltas(rune::node::ViewSnapshotChannel& ch) {
    std::vector<std::vector<uint8_t>> deltas;
    ch.deltas.drain([&](int32_t, aeron::concurrent::AtomicBuffer& buf,
                        rune::node::OneToOneByteRing::index_t off,
                        rune::node::OneToOneByteRing::index_t len) {
        const uint8_t* p = buf.buffer() + off + rune::node::ViewSnapshotChannel::kDeltaHeaderBytes;
        deltas.emplace_back(p, p + (len - rune::node::ViewSnapshotChannel::kDeltaHeaderBytes));
    });
    return deltas;
}
// A fresh, empty per-test view data directory.
std::string freshDir(const std::string& name) {
    const std::string d = "/tmp/" + name;
    std::filesystem::remove_all(d);
    std::filesystem::create_directories(d);
    return d;
}
} // namespace

TEST(WorkerViewSet, AggregateViewFoldsAndServesGroupRow) {
    const auto src = orderSrc();
    const rune::codec::Descriptor* srcs[] = {&src};
    auto comp = rune::view::compileView(userTotalsPlan(1), srcs, 0x40000009).value();
    rune::node::ViewSnapshotChannel channel;
    rune::node::WorkerViewSet vs(channel, "/tmp/rune-views-agg");
    vs.installView(comp.plan, comp.rowLayout, src, /*persisted*/ false);

    const auto batch = orderBatch(0, {{1, 100, 10}, {2, 100, 30}, {3, 200, 5}});
    auto rr = [&](int64_t, int64_t) { return rune::storage::LogReadView{batch.data(), batch.size(), 0, 3}; };
    vs.driveFolds(6, 0, rr, 3, 0);

    const rune::view::RowLayout* L = nullptr;
    const uint8_t* row = vs.getRow(0x40000009, 0, 100, L);
    ASSERT_NE(row, nullptr);
    EXPECT_EQ(rune::view::readSlot(row, *L, 1).i, 2);    // n
    EXPECT_EQ(rune::view::readSlot(row, *L, 2).i, 40);   // total
}

// REGRESSION (Review Focus #4): after recovery, an UPDATE to a pre-restart
// contributor must retract its pre-restart contribution — only possible if the
// contributor map (not just the output rows) was checkpointed and restored.
TEST(WorkerViewSetRegression, AggregateRecoveryRestoresContributorMap) {
    const auto src = orderSrc();
    const rune::codec::Descriptor* srcs[] = {&src};
    auto comp = rune::view::compileView(userTotalsPlan(0), srcs, 0x40000009).value();

    // 1. Fold on a persisted view and capture the checkpoint delta it pushes.
    rune::node::ViewSnapshotChannel ch1;
    rune::node::WorkerViewSet vs1(ch1, "/tmp/rune-views-agg-rec");
    vs1.installView(comp.plan, comp.rowLayout, src, /*persisted*/ true);
    const auto b1 = orderBatch(0, {{1, 100, 10}, {2, 100, 30}});
    auto rr1 = [&](int64_t, int64_t) { return rune::storage::LogReadView{b1.data(), b1.size(), 0, 2}; };
    vs1.driveFolds(6, 0, rr1, 2, 0);

    const auto deltas = drainDeltas(ch1);
    ASSERT_EQ(deltas.size(), 1u);
    uint32_t magic; std::memcpy(&magic, deltas[0].data(), 4);
    EXPECT_EQ(magic, rune::view::kViewCheckpointMagicV2) << "aggregate views write v2";

    // 2. Recover into a fresh worker view set from that checkpoint.
    rune::node::ViewSnapshotChannel ch2;
    rune::node::WorkerViewSet vs2(ch2, "/tmp/rune-views-agg-rec");
    const std::span<const uint8_t> recs[] = {deltas[0]};
    vs2.installViewForRecovery(comp.plan, comp.rowLayout, src, /*persisted*/ true, /*partition*/ 0,
                               /*base*/ {}, recs, /*committedTail*/ 2);
    const rune::view::RowLayout* L = nullptr;
    const uint8_t* row = vs2.getRow(0x40000009, 0, 100, L);
    ASSERT_NE(row, nullptr);
    EXPECT_EQ(rune::view::readSlot(row, *L, 2).i, 40);

    // 3. Post-recovery update of contributor 1: 10 -> 25. Correct: total 55, n 2.
    //    A lost contributor map would give n 3, total 65.
    const auto b2 = orderBatch(2, {{1, 100, 25}});
    auto rr2 = [&](int64_t, int64_t) { return rune::storage::LogReadView{b2.data(), b2.size(), 2, 3}; };
    vs2.driveFolds(6, 0, rr2, 3, 0);
    row = vs2.getRow(0x40000009, 0, 100, L);
    ASSERT_NE(row, nullptr);
    EXPECT_EQ(rune::view::readSlot(row, *L, 1).i, 2);
    EXPECT_EQ(rune::view::readSlot(row, *L, 2).i, 55);
}

// REGRESSION: the LAZY recovery path (getOrCreate_ -> loadRecoveryCheckpoint_,
// reading the on-disk delta file) must also restore all three aggregate tables
// from a v2 record — not just installViewForRecovery's explicit-span path.
TEST(WorkerViewSetRegression, AggregateLazyRecoveryLoadsV2CheckpointFile) {
    const auto src = orderSrc();
    const rune::codec::Descriptor* srcs[] = {&src};
    auto comp = rune::view::compileView(userTotalsPlan(0), srcs, 0x40000009).value();
    const std::string dir = freshDir("rune-views-agg-lazy-rec");

    rune::node::ViewSnapshotChannel ch1;
    rune::node::WorkerViewSet vs1(ch1, dir);
    vs1.installView(comp.plan, comp.rowLayout, src, /*persisted*/ true);
    const auto b1 = orderBatch(0, {{1, 100, 10}, {2, 100, 30}});
    auto rr1 = [&](int64_t, int64_t) { return rune::storage::LogReadView{b1.data(), b1.size(), 0, 2}; };
    vs1.driveFolds(6, 0, rr1, 2, 0);
    const auto deltas = drainDeltas(ch1);
    ASSERT_EQ(deltas.size(), 1u);
    {   // Exactly what the snapshot IO agent appends (same file name scheme).
        std::ofstream f(dir + "/view-" + std::to_string(0x40000009u) + "-p0.delta", std::ios::binary);
        f.write(reinterpret_cast<const char*>(deltas[0].data()),
                static_cast<std::streamsize>(deltas[0].size()));
    }

    // Restart: template installed for recovery; the instance is created lazily
    // by driveFolds, which loads the file (O=2) then folds [2,3).
    rune::node::ViewSnapshotChannel ch2;
    rune::node::WorkerViewSet vs2(ch2, dir);
    vs2.installView(comp.plan, comp.rowLayout, src, /*persisted*/ true, /*underRecovery*/ true);
    const auto b2 = orderBatch(2, {{1, 100, 25}});
    auto rr2 = [&](int64_t, int64_t) { return rune::storage::LogReadView{b2.data(), b2.size(), 2, 3}; };
    vs2.driveFolds(6, 0, rr2, 3, 0);

    const rune::view::RowLayout* L = nullptr;
    const uint8_t* row = vs2.getRow(0x40000009, 0, 100, L);
    ASSERT_NE(row, nullptr);
    EXPECT_EQ(rune::view::readSlot(row, *L, 1).i, 2);
    EXPECT_EQ(rune::view::readSlot(row, *L, 2).i, 55);
}

// §9.7: a TRANSIENT aggregate view re-folds from the retained source on
// restart, starting from EMPTY aux tables (no checkpoint file exists).
TEST(WorkerViewSet, TransientAggregateRefoldsFromEmptyOnRestart) {
    const auto src = orderSrc();
    const rune::codec::Descriptor* srcs[] = {&src};
    auto comp = rune::view::compileView(userTotalsPlan(1), srcs, 0x40000009).value();
    const std::string dir = freshDir("rune-views-agg-transient-refold");

    rune::node::ViewSnapshotChannel ch;
    rune::node::WorkerViewSet vs(ch, dir);
    vs.installView(comp.plan, comp.rowLayout, src, /*persisted*/ false, /*underRecovery*/ true);
    const auto b = orderBatch(0, {{1, 100, 10}, {2, 100, 30}, {1, 100, 25}});
    auto rr = [&](int64_t, int64_t) { return rune::storage::LogReadView{b.data(), b.size(), 0, 3}; };
    vs.driveFolds(6, 0, rr, 3, 0);

    const rune::view::RowLayout* L = nullptr;
    const uint8_t* row = vs.getRow(0x40000009, 0, 100, L);
    ASSERT_NE(row, nullptr);
    EXPECT_EQ(rune::view::readSlot(row, *L, 1).i, 2);
    EXPECT_EQ(rune::view::readSlot(row, *L, 2).i, 55);
    EXPECT_TRUE(drainDeltas(ch).empty()) << "transient views never checkpoint";
}

// Phase 1 Replace views are unchanged: still a v1 checkpoint record.
TEST(WorkerViewSet, ReplaceViewStillWritesV1Checkpoint) {
    auto src = charSourceV1();
    const rune::codec::Descriptor* srcs[] = {&src};
    auto comp = rune::view::compileView(hpView(0), srcs, 0x40000007).value();
    rune::node::ViewSnapshotChannel ch;
    rune::node::WorkerViewSet vs(ch, "/tmp/rune-views-test");
    vs.installView(comp.plan, comp.rowLayout, src, /*persisted*/ true);
    const auto batch = twoUpsertsKey7();
    auto rr = [&](int64_t, int64_t) { return rune::storage::LogReadView{batch.data(), batch.size(), 0, 2}; };
    vs.driveFolds(5, 0, rr, 2, 0);
    const auto deltas = drainDeltas(ch);
    ASSERT_EQ(deltas.size(), 1u);
    uint32_t magic; std::memcpy(&magic, deltas[0].data(), 4);
    EXPECT_EQ(magic, rune::view::kViewCheckpointMagic);
}

// REGRESSION (zero-alloc steady state): a TRANSIENT view never checkpoints, so
// nothing drained its tables' dirty tracking — every erase appended to the
// deleted list and every erase+reinsert re-pushed a rid onto the dirty list,
// growing both without bound (an aggregate erases contributors on tombstones
// and empty groups on moves, so it churns far more than a Replace view).  Each
// pass must discard the dirty state of every table of a transient instance.
TEST(WorkerViewSetRegression, TransientViewDiscardsDirtyStateEachPass) {
    {   // Replace view.
        auto src = charSourceV1();
        const rune::codec::Descriptor* srcs[] = {&src};
        auto comp = rune::view::compileView(hpView(1), srcs, 0x40000007).value();
        rune::node::ViewSnapshotChannel ch;
        rune::node::WorkerViewSet vs(ch, "/tmp/rune-views-test");
        vs.installView(comp.plan, comp.rowLayout, src, /*persisted*/ false);
        const auto batch = twoUpsertsKey7();
        auto rr = [&](int64_t, int64_t) { return rune::storage::LogReadView{batch.data(), batch.size(), 0, 2}; };
        vs.driveFolds(5, 0, rr, 2, 0);
        EXPECT_EQ(vs.dirtyBacklog(0x40000007, 0), 0u);
        // Tombstone then re-upsert of key 7, one record per pass: each erase
        // appends to the table's pending-delete list, which only a drain clears.
        for (int64_t off = 2; off < 34; ++off) {
            rune::protocol::RecordBatchBuilder bb(off, 0);
            const auto k = keyBytes(7);
            const auto v = encodeCharacter(7, 30, 1);
            bb.append(asSv(k), (off % 2) ? asSv(v) : std::string_view{}, 0);
            const auto b = bb.build();
            auto rr2 = [&](int64_t, int64_t) { return rune::storage::LogReadView{b.data(), b.size(), off, off + 1}; };
            vs.driveFolds(5, 0, rr2, off + 1, 0);
            ASSERT_EQ(vs.dirtyBacklog(0x40000007, 0), 0u) << "pass " << off;
        }
    }
    {   // Aggregate view: contributor 1 hops between groups 100 and 200, so a
        // group empties (erase) and is recreated (reinsert) every pass.
        const auto src = orderSrc();
        const rune::codec::Descriptor* srcs[] = {&src};
        auto comp = rune::view::compileView(userTotalsPlan(1), srcs, 0x40000009).value();
        rune::node::ViewSnapshotChannel ch;
        rune::node::WorkerViewSet vs(ch, "/tmp/rune-views-agg");
        vs.installView(comp.plan, comp.rowLayout, src, /*persisted*/ false);
        for (int64_t off = 0; off < 64; ++off) {
            const auto b = orderBatch(off, {{1, (off % 2) ? 200 : 100, 10}});
            auto rr = [&](int64_t, int64_t) { return rune::storage::LogReadView{b.data(), b.size(), off, off + 1}; };
            vs.driveFolds(6, 0, rr, off + 1, 0);
            ASSERT_EQ(vs.dirtyBacklog(0x40000009, 0), 0u) << "pass " << off;
        }
        const rune::view::RowLayout* L = nullptr;
        const uint8_t* row = vs.getRow(0x40000009, 0, 200, L);   // off 63 -> group 200
        ASSERT_NE(row, nullptr);
        EXPECT_EQ(rune::view::readSlot(row, *L, 2).i, 10);
        EXPECT_EQ(vs.getRow(0x40000009, 0, 100, L), nullptr);
    }
}

namespace {
// Drives (src, partition 0) until caught up to `bound`, draining the delta ring
// between passes (the IO agent's role) and collecting every delta payload.
template <class RR>
void driveToEnd(rune::node::WorkerViewSet& vs, rune::node::ViewSnapshotChannel& ch, int32_t src,
                RR& rr, int64_t bound, std::vector<std::vector<uint8_t>>& out) {
    for (int pass = 0; pass < 10000; ++pass) {
        const int w = vs.driveFolds(src, 0, rr, bound, 0);
        for (;;) {
            auto d = drainDeltas(ch);
            if (d.empty()) break;
            for (auto& x : d) out.push_back(std::move(x));
        }
        if (w == 0) return;
    }
    FAIL() << "never caught up";
}
} // namespace

// REGRESSION (Task 7 fix round 1, Important): one fold pass over a large
// committed range used to encode ONE unbounded checkpoint delta; above the
// delta ring's max message length (capacity/8 = 128 KiB) tryClaim THROWS on the
// worker thread.  An aggregate dirties up to 5 rows per record, so ~3K
// contributors crashed the worker.  Each persisted pass is now capped so its
// delta always fits; the view still reaches the end with correct aggregates.
TEST(WorkerViewSetRegression, PersistedAggregateLargeRangeDeltasFitTheRing) {
    const auto src = orderSrc();
    const rune::codec::Descriptor* srcs[] = {&src};
    auto comp = rune::view::compileView(userTotalsPlan(0), srcs, 0x40000009).value();
    constexpr int64_t kN = 5000;
    std::vector<std::array<int64_t, 3>> recs;
    for (int64_t i = 1; i <= kN; ++i) recs.push_back({i, i % 10, i});
    const auto batch = orderBatch(0, recs);
    auto rr = [&](int64_t, int64_t) { return rune::storage::LogReadView{batch.data(), batch.size(), 0, kN}; };

    rune::node::ViewSnapshotChannel ch;
    rune::node::WorkerViewSet vs(ch, "/tmp/rune-views-agg-large");
    vs.installView(comp.plan, comp.rowLayout, src, /*persisted*/ true);
    std::vector<std::vector<uint8_t>> deltas;
    ASSERT_NO_THROW(driveToEnd(vs, ch, 6, rr, kN, deltas));
    EXPECT_GT(deltas.size(), 1u) << "the range must have been split across capped passes";

    const rune::view::RowLayout* L = nullptr;
    for (int64_t g = 0; g < 10; ++g) {
        int64_t total = 0;
        for (int64_t i = 1; i <= kN; ++i) if (i % 10 == g) total += i;
        const uint8_t* row = vs.getRow(0x40000009, 0, uint64_t(g), L);
        ASSERT_NE(row, nullptr) << "group " << g;
        EXPECT_EQ(rune::view::readSlot(row, *L, 1).i, kN / 10) << "group " << g;
        EXPECT_EQ(rune::view::readSlot(row, *L, 2).i, total) << "group " << g;
    }
}

// REGRESSION (Task 7 fix round 1): recovery used to leave every restored row
// dirty, so the first post-recovery checkpoint re-sent the WHOLE contributor
// map (oversized → throw).  Restored rows equal the durable checkpoint, so the
// first delta after recovery carries only what that pass changed.
TEST(WorkerViewSetRegression, AggregateRecoveryOfLargeCheckpointDoesNotResendRows) {
    const auto src = orderSrc();
    const rune::codec::Descriptor* srcs[] = {&src};
    auto comp = rune::view::compileView(userTotalsPlan(0), srcs, 0x40000009).value();
    constexpr int64_t kN = 5000;
    std::vector<std::array<int64_t, 3>> recs;
    for (int64_t i = 1; i <= kN; ++i) recs.push_back({i, i % 10, i});
    const auto batch = orderBatch(0, recs);
    auto rr = [&](int64_t, int64_t) { return rune::storage::LogReadView{batch.data(), batch.size(), 0, kN}; };

    rune::node::ViewSnapshotChannel ch1;
    rune::node::WorkerViewSet vs1(ch1, "/tmp/rune-views-agg-large-rec");
    vs1.installView(comp.plan, comp.rowLayout, src, /*persisted*/ true);
    std::vector<std::vector<uint8_t>> deltas;
    driveToEnd(vs1, ch1, 6, rr, kN, deltas);
    std::vector<std::span<const uint8_t>> spans(deltas.begin(), deltas.end());

    rune::node::ViewSnapshotChannel ch2;
    rune::node::WorkerViewSet vs2(ch2, "/tmp/rune-views-agg-large-rec");
    vs2.installViewForRecovery(comp.plan, comp.rowLayout, src, /*persisted*/ true, 0, {},
                               std::span<const std::span<const uint8_t>>(spans.data(), spans.size()),
                               /*committedTail*/ kN);
    EXPECT_EQ(vs2.dirtyBacklog(0x40000009, 0), 0u) << "restored rows must not be dirty";

    // One post-recovery pass: contributor 1 (group 1, amount 1) -> amount 1001.
    const auto b2 = orderBatch(kN, {{1, 1, 1001}});
    auto rr2 = [&](int64_t, int64_t) { return rune::storage::LogReadView{b2.data(), b2.size(), kN, kN + 1}; };
    ASSERT_NO_THROW(vs2.driveFolds(6, 0, rr2, kN + 1, 0));
    const auto post = drainDeltas(ch2);
    ASSERT_EQ(post.size(), 1u);
    EXPECT_LT(post[0].size(), 512u) << "first post-recovery delta must carry only this pass's rows";

    int64_t total = 1000;
    for (int64_t i = 1; i <= kN; ++i) if (i % 10 == 1) total += i;
    const rune::view::RowLayout* L = nullptr;
    const uint8_t* row = vs2.getRow(0x40000009, 0, 1, L);
    ASSERT_NE(row, nullptr);
    EXPECT_EQ(rune::view::readSlot(row, *L, 1).i, kN / 10);
    EXPECT_EQ(rune::view::readSlot(row, *L, 2).i, total);
}

// REGRESSION (Task 7 fix round 1): the same unbounded-delta throw existed for a
// Phase 1 persisted Replace view (one dirty row per distinct key).
TEST(WorkerViewSetRegression, PersistedReplaceLargeRangeDeltasFitTheRing) {
    auto src = charSourceV1();
    const rune::codec::Descriptor* srcs[] = {&src};
    auto comp = rune::view::compileView(hpView(0), srcs, 0x40000007).value();
    // ~5K keys of this narrow row only just fit one 128 KiB message; 20K keys
    // (≈4x over) make the unbounded single delta throw.
    constexpr int64_t kN = 20000;
    rune::protocol::RecordBatchBuilder bb(0, 0);
    for (int64_t i = 1; i <= kN; ++i) {
        const auto k = keyBytes(i);
        const auto v = encodeCharacter(i, static_cast<int32_t>(i), 1);
        bb.append(asSv(k), asSv(v), 0);
    }
    const auto batch = bb.build();
    auto rr = [&](int64_t, int64_t) { return rune::storage::LogReadView{batch.data(), batch.size(), 0, kN}; };

    rune::node::ViewSnapshotChannel ch;
    rune::node::WorkerViewSet vs(ch, "/tmp/rune-views-replace-large");
    vs.installView(comp.plan, comp.rowLayout, src, /*persisted*/ true);
    std::vector<std::vector<uint8_t>> deltas;
    ASSERT_NO_THROW(driveToEnd(vs, ch, 5, rr, kN, deltas));
    uint32_t magic; std::memcpy(&magic, deltas.front().data(), 4);
    EXPECT_EQ(magic, rune::view::kViewCheckpointMagic) << "Replace views still write v1";

    const rune::view::RowLayout* L = nullptr;
    for (int64_t i : {int64_t{1}, kN / 2, kN}) {
        const uint8_t* row = vs.getRow(0x40000007, 0, uint64_t(i), L);
        ASSERT_NE(row, nullptr) << i;
        EXPECT_EQ(rune::view::readSlot(row, *L, 1).i, i);
    }
}

namespace {
// Captures spdlog output into a ring for the scope's lifetime (restores the
// previous default logger on exit).
struct LogCapture {
    std::shared_ptr<spdlog::sinks::ringbuffer_sink_mt> sink =
        std::make_shared<spdlog::sinks::ringbuffer_sink_mt>(256);
    std::shared_ptr<spdlog::logger> prev = spdlog::default_logger();
    LogCapture() {
        auto lg = std::make_shared<spdlog::logger>("capture", sink);
        lg->set_level(spdlog::level::trace);
        lg->set_pattern("%v");
        spdlog::set_default_logger(lg);
    }
    ~LogCapture() { spdlog::set_default_logger(prev); }
    std::vector<std::string> lines() const { return sink->last_formatted(); }
    std::vector<std::string> grep(const std::string& needle) const {
        std::vector<std::string> out;
        for (auto& l : lines()) if (l.find(needle) != std::string::npos) out.push_back(l);
        return out;
    }
};
} // namespace

// REGRESSION (final review I2): a successful recovery replay logs ONE stable,
// greppable VIEW-RECOVERED line (view, partition, recovered O, records applied,
// checkpoint format) so the e2e can PROVE a v2-checkpoint recovery happened
// (and was not just a cold re-fold of the source).  Both recovery entry
// points: the explicit-span installViewForRecovery and the lazy file load.
TEST(WorkerViewSetRegression, RecoveryLogsViewRecoveredLineRegression) {
    const auto src = orderSrc();
    const rune::codec::Descriptor* srcs[] = {&src};
    auto comp = rune::view::compileView(userTotalsPlan(0), srcs, 0x40000009).value();
    const std::string dir = freshDir("rune-views-recovered-log");

    rune::node::ViewSnapshotChannel ch1;
    rune::node::WorkerViewSet vs1(ch1, dir);
    vs1.installView(comp.plan, comp.rowLayout, src, /*persisted*/ true);
    const auto b1 = orderBatch(0, {{1, 100, 10}, {2, 100, 30}});
    auto rr1 = [&](int64_t, int64_t) { return rune::storage::LogReadView{b1.data(), b1.size(), 0, 2}; };
    vs1.driveFolds(6, 0, rr1, 2, 0);
    const auto deltas = drainDeltas(ch1);
    ASSERT_EQ(deltas.size(), 1u);

    {   // explicit-span path
        LogCapture cap;
        rune::node::ViewSnapshotChannel ch2;
        rune::node::WorkerViewSet vs2(ch2, dir);
        const std::span<const uint8_t> recs[] = {deltas[0]};
        vs2.installViewForRecovery(comp.plan, comp.rowLayout, src, true, 0, {}, recs, 2);
        const auto hits = cap.grep("VIEW-RECOVERED ");
        ASSERT_EQ(hits.size(), 1u);
        EXPECT_NE(hits[0].find("view=" + std::to_string(0x40000009)), std::string::npos) << hits[0];
        EXPECT_NE(hits[0].find("partition=0"), std::string::npos) << hits[0];
        EXPECT_NE(hits[0].find("O=2"), std::string::npos) << hits[0];
        EXPECT_NE(hits[0].find("records=1"), std::string::npos) << hits[0];
        EXPECT_NE(hits[0].find("format=v2"), std::string::npos) << hits[0];
    }
    {   // lazy on-disk path (getOrCreate_ -> loadRecoveryCheckpoint_)
        {
            std::ofstream f(dir + "/view-" + std::to_string(0x40000009u) + "-p0.delta", std::ios::binary);
            f.write(reinterpret_cast<const char*>(deltas[0].data()),
                    static_cast<std::streamsize>(deltas[0].size()));
        }
        LogCapture cap;
        rune::node::ViewSnapshotChannel ch3;
        rune::node::WorkerViewSet vs3(ch3, dir);
        vs3.installView(comp.plan, comp.rowLayout, src, true, /*underRecovery*/ true);
        vs3.driveFolds(6, 0, rr1, 2, 0);
        const auto hits = cap.grep("VIEW-RECOVERED ");
        ASSERT_EQ(hits.size(), 1u);
        EXPECT_NE(hits[0].find("O=2 records=1 format=v2"), std::string::npos) << hits[0];
    }
    {   // a cold start (nothing to replay) is NOT a recovery: no line
        LogCapture cap;
        rune::node::ViewSnapshotChannel ch4;
        rune::node::WorkerViewSet vs4(ch4, freshDir("rune-views-recovered-log-cold"));
        vs4.installViewForRecovery(comp.plan, comp.rowLayout, src, true, 0, {}, {}, 2);
        EXPECT_TRUE(cap.grep("VIEW-RECOVERED ").empty());
    }
    {   // a Replace view's v1 recovery reports format=v1
        auto csrc = charSourceV1();
        const rune::codec::Descriptor* csrcs[] = {&csrc};
        auto rcomp = rune::view::compileView(hpView(0), csrcs, 0x40000007).value();
        rune::node::ViewSnapshotChannel ch5;
        rune::node::WorkerViewSet vs5(ch5, "/tmp/rune-views-recovered-log-v1");
        vs5.installView(rcomp.plan, rcomp.rowLayout, csrc, true);
        const auto b = oneUpsert(7, 10);
        auto rr = [&](int64_t, int64_t) { return rune::storage::LogReadView{b.data(), b.size(), 0, 1}; };
        vs5.driveFolds(5, 0, rr, 1, 0);
        const auto d = drainDeltas(ch5);
        ASSERT_EQ(d.size(), 1u);
        LogCapture cap;
        rune::node::ViewSnapshotChannel ch6;
        rune::node::WorkerViewSet vs6(ch6, "/tmp/rune-views-recovered-log-v1");
        const std::span<const uint8_t> recs[] = {d[0]};
        vs6.installViewForRecovery(rcomp.plan, rcomp.rowLayout, csrc, true, 0, {}, recs, 1);
        const auto hits = cap.grep("VIEW-RECOVERED ");
        ASSERT_EQ(hits.size(), 1u);
        EXPECT_NE(hits[0].find("O=1 records=1 format=v1"), std::string::npos) << hits[0];
    }
}

namespace {
// "order_f": {orderId Int64 (key), userId Int64, amount Int64, price Float64}.
rune::codec::Descriptor orderFSrc() {
    return rune::codec::DescriptorBuilder("order_f", 1)
        .add("orderId", rune::codec::PrimType::Int64, 1, true)
        .add("userId", rune::codec::PrimType::Int64)
        .add("amount", rune::codec::PrimType::Int64)
        .add("price", rune::codec::PrimType::Float64)
        .build();
}
rune::view::ViewPlan userSumsPlan() {
    using P = rune::codec::PrimType; using R = rune::view::MergeRule;
    rune::view::ViewPlan v;
    v.viewName = "user_sums"; v.key = "userId"; v.sources = {{8, "userId"}};
    v.output = {{"userId", P::Int64, 0, "userId", R::Replace},
                {"total", P::Int64, 0, "amount", R::Sum},
                {"ptotal", P::Float64, 0, "price", R::Sum}};
    v.persistence = 0;
    return v;
}
std::vector<uint8_t> orderFValue(int64_t orderId, int64_t userId, int64_t amount, double price) {
    std::vector<uint8_t> b;
    auto le16 = [&](uint16_t v) { b.push_back(uint8_t(v)); b.push_back(uint8_t(v >> 8)); };
    auto le64 = [&](uint64_t v) { for (int i = 0; i < 8; ++i) b.push_back(uint8_t(v >> (8 * i))); };
    uint64_t pbits; std::memcpy(&pbits, &price, 8);
    le16(32); le16(1); le64(uint64_t(orderId)); le64(uint64_t(userId)); le64(uint64_t(amount)); le64(pbits);
    return b;
}
} // namespace

// REGRESSION (final review I3): driveView_ used to discard every FoldStats
// counter beyond the work count, so a Sum overflow, a non-finite Float input or
// a malformed wire key was invisible.  They now accumulate per (view,
// partition) and are readable via foldCounters().
TEST(WorkerViewSetRegression, FoldCountersAccumulatePerInstanceRegression) {
    const auto src = orderFSrc();
    const rune::codec::Descriptor* srcs[] = {&src};
    auto comp = rune::view::compileView(userSumsPlan(), srcs, 0x4000000B).value();
    rune::node::ViewSnapshotChannel ch;
    rune::node::WorkerViewSet vs(ch, "/tmp/rune-views-fold-counters");
    vs.installView(comp.plan, comp.rowLayout, src, /*persisted*/ false);
    EXPECT_EQ(vs.foldCounters(0x4000000B, 0), nullptr) << "no instance hosted yet";

    const double nan = std::numeric_limits<double>::quiet_NaN();
    rune::protocol::RecordBatchBuilder bb(0, 0);
    const int64_t big = std::numeric_limits<int64_t>::max();
    const std::array<std::vector<uint8_t>, 3> vals = {orderFValue(1, 100, big, 1.5),
                                                      orderFValue(2, 100, big, nan),
                                                      orderFValue(3, 200, 5, 2.0)};
    for (int i = 0; i < 3; ++i) {
        const auto k = keyBytes(i + 1);
        bb.append(asSv(k), asSv(vals[size_t(i)]), 0);
    }
    const std::vector<uint8_t> shortKey = {1, 2, 3, 4};           // 4-byte wire key
    bb.append(asSv(shortKey), asSv(vals[2]), 0);
    const auto batch = bb.build();
    auto rr = [&](int64_t, int64_t) { return rune::storage::LogReadView{batch.data(), batch.size(), 0, 4}; };
    vs.driveFolds(8, 0, rr, 4, 0);

    const rune::node::ViewFoldCounters* c = vs.foldCounters(0x4000000B, 0);
    ASSERT_NE(c, nullptr);
    EXPECT_GE(c->aggOverflow, 1u) << "INT64_MAX + INT64_MAX overflowed the Int64 Sum";
    EXPECT_EQ(c->aggNonFinite, 1u) << "one NaN Float input";
    EXPECT_EQ(c->skippedMalformed, 1u) << "one 4-byte wire key";
    EXPECT_EQ(c->skippedNoGroup, 0u);
    EXPECT_EQ(c->aggInconsistent, 0u);

    // Counters ACCUMULATE across passes (not overwritten by the last pass).
    const auto b2 = [&] {
        rune::protocol::RecordBatchBuilder b(4, 0);
        const auto v = orderFValue(9, 300, 1, std::numeric_limits<double>::infinity());
        const auto k = keyBytes(9);
        b.append(asSv(k), asSv(v), 0);
        return b.build();
    }();
    auto rr2 = [&](int64_t, int64_t) { return rune::storage::LogReadView{b2.data(), b2.size(), 4, 5}; };
    vs.driveFolds(8, 0, rr2, 5, 0);
    c = vs.foldCounters(0x4000000B, 0);
    ASSERT_NE(c, nullptr);
    EXPECT_EQ(c->aggNonFinite, 2u);
    EXPECT_EQ(c->skippedMalformed, 1u);
    EXPECT_EQ(vs.foldCounters(0x4000000B, 1), nullptr) << "per partition";
}

// REGRESSION (final review I4): recovery from a checkpoint whose O lies in the
// MIDDLE of a source batch (a capped pass ended mid-batch), then continued
// folding over that SAME whole batch, must skip every offset below O — exact
// final totals, no double counting.
TEST(WorkerViewSetRegression, RecoveryFromMidBatchOffsetThenContinuedFoldIsExactRegression) {
    const auto src = orderSrc();
    const rune::codec::Descriptor* srcs[] = {&src};
    auto comp = rune::view::compileView(userTotalsPlan(0), srcs, 0x40000009).value();
    constexpr int64_t kN = 5000;
    std::vector<std::array<int64_t, 3>> recs;
    for (int64_t i = 1; i <= kN; ++i) recs.push_back({i, i % 10, i});
    const auto batch = orderBatch(0, recs);   // ONE batch [0, kN)
    auto rr = [&](int64_t, int64_t) { return rune::storage::LogReadView{batch.data(), batch.size(), 0, kN}; };

    rune::node::ViewSnapshotChannel ch1;
    rune::node::WorkerViewSet vs1(ch1, "/tmp/rune-views-agg-midbatch");
    vs1.installView(comp.plan, comp.rowLayout, src, /*persisted*/ true);
    std::vector<std::vector<uint8_t>> deltas;
    driveToEnd(vs1, ch1, 6, rr, kN, deltas);
    ASSERT_GE(deltas.size(), 3u) << "the single batch must be split across capped passes";

    // Recover from a PREFIX only: its last O is strictly inside the batch.
    const std::size_t prefix = deltas.size() / 2;
    std::vector<std::span<const uint8_t>> spans(deltas.begin(), deltas.begin() + std::ptrdiff_t(prefix));
    uint64_t prefixO = 0;
    std::memcpy(&prefixO, deltas[prefix - 1].data() + 4, 8);   // v2 header: magic u32, O i64, tableCount u8
    ASSERT_GT(int64_t(prefixO), 0);
    ASSERT_LT(int64_t(prefixO), kN) << "recovered O must be mid-batch";

    rune::node::ViewSnapshotChannel ch2;
    rune::node::WorkerViewSet vs2(ch2, "/tmp/rune-views-agg-midbatch");
    vs2.installViewForRecovery(comp.plan, comp.rowLayout, src, /*persisted*/ true, 0, {},
                               std::span<const std::span<const uint8_t>>(spans.data(), spans.size()),
                               /*committedTail*/ kN);
    EXPECT_EQ(vs2.readiness(0x40000009, 0), rune::node::ViewReadiness::RebuildingViews);
    std::vector<std::vector<uint8_t>> more;
    driveToEnd(vs2, ch2, 6, rr, kN, more);   // the reader hands back the WHOLE batch each pass
    EXPECT_EQ(vs2.readiness(0x40000009, 0), rune::node::ViewReadiness::Ready);

    const rune::view::RowLayout* L = nullptr;
    for (int64_t g = 0; g < 10; ++g) {
        int64_t total = 0;
        for (int64_t i = 1; i <= kN; ++i) if (i % 10 == g) total += i;
        const uint8_t* row = vs2.getRow(0x40000009, 0, uint64_t(g), L);
        ASSERT_NE(row, nullptr) << "group " << g;
        EXPECT_EQ(rune::view::readSlot(row, *L, 1).i, kN / 10) << "group " << g;
        EXPECT_EQ(rune::view::readSlot(row, *L, 2).i, total) << "group " << g;
    }
}
