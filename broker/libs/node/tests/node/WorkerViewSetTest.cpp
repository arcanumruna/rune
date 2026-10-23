// libs/node/tests/node/WorkerViewSetTest.cpp
//
// Task 15: WorkerViewSet — per-worker view-fold driver.  Deterministic,
// in-process: a fake committed-range readRange (no real Log, no Aeron, no
// clock) feeds foldBatch; the gate test proves a committed range folds into
// the in-memory table and getRow() serves the latest row.
#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <climits>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <initializer_list>
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

#include <rune/node/AdminChannels.h>
#include <rune/node/ViewSnapshotChannel.h>
#include <rune/node/WorkerViewSet.h>

#include <spdlog/sinks/ringbuffer_sink.h>
#include <spdlog/spdlog.h>

#include <unistd.h>

namespace {
/// The slot of a view row.  getRow is called FIRST (it sets the layout): `readSlot(getRow(.., L), *L, s)` left the
/// order to the compiler, and GCC evaluates `*L` before getRow assigns L (a null dereference on Linux).
template <class VS>
rune::codec::FieldValue slotOf(VS& vs, int32_t view, int32_t partition, uint64_t key, uint16_t slot) {
    const rune::view::RowLayout* layout = nullptr;
    const uint8_t* row = vs.getRow(view, partition, key, layout);
    if (row == nullptr || layout == nullptr) {
        ADD_FAILURE() << "no row for key " << key;
        return {};
    }
    return rune::view::readSlot(row, *layout, slot);
}
}  // namespace

namespace {

// Every view data dir is per PROCESS: "<temp>/<name>-<pid>".  ctest runs each
// test as its own process (-j6) and other checkouts on this machine run the same
// suite concurrently; with a fixed /tmp/<name> one process's freshDir remove_all
// wiped another's checkpoint file mid-test (the
// WorkerViewSetRegression.AggregateLazyRecoveryLoadsV2CheckpointFile flake).
// Every dir handed out is removed when the process's tests finish.
std::vector<std::string>& createdTmpDirs() {
    static std::vector<std::string> dirs;
    return dirs;
}
std::string tmpDir(const std::string& name) {
    std::string d = (std::filesystem::temp_directory_path() /
                     (name + "-" + std::to_string(::getpid()))).string();
    createdTmpDirs().push_back(d);
    return d;
}
class TmpDirCleanup : public ::testing::Environment {
public:
    void TearDown() override {
        std::error_code ec;
        for (const std::string& d : createdTmpDirs()) std::filesystem::remove_all(d, ec);
    }
};
[[maybe_unused]] ::testing::Environment* const kTmpDirCleanup =
    ::testing::AddGlobalTestEnvironment(new TmpDirCleanup);

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
    rune::node::WorkerViewSet vs(channel, tmpDir("rune-views-test"));
    vs.installView(comp.plan, comp.rowLayout, src, /*persisted*/ true);

    const auto batch = twoUpsertsKey7();
    auto readRange = [&](int64_t /*from*/, int64_t /*to*/) -> rune::storage::LogReadView {
        return rune::storage::LogReadView{batch.data(), batch.size(), 0, 2};
    };
    vs.driveFolds(/*srcTopicId*/ 5, /*partition*/ 0, readRange, /*bound*/ 2, rune::view::kNoAbortedTxns, /*nowNs*/ 0);

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
    rune::node::WorkerViewSet vs(channel, tmpDir("rune-views-test"));
    vs.installView(comp.plan, comp.rowLayout, src, /*persisted*/ true);

    const auto batch = twoUpsertsKey7();
    auto readRange = [&](int64_t, int64_t) -> rune::storage::LogReadView {
        return rune::storage::LogReadView{batch.data(), batch.size(), 0, 2};
    };
    vs.driveFolds(5, 0, readRange, 2, rune::view::kNoAbortedTxns, 0);

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
    rune::node::WorkerViewSet vs(channel, tmpDir("rune-views-test"));
    vs.installView(comp.plan, comp.rowLayout, src, /*persisted*/ false);

    const auto batch = twoUpsertsKey7();
    auto readRange = [&](int64_t, int64_t) -> rune::storage::LogReadView {
        return rune::storage::LogReadView{batch.data(), batch.size(), 0, 2};
    };
    vs.driveFolds(5, 0, readRange, 2, rune::view::kNoAbortedTxns, 0);

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
    rune::node::WorkerViewSet vs(channel, tmpDir("rune-views-test"));
    bool called = false;
    auto readRange = [&](int64_t, int64_t) -> rune::storage::LogReadView {
        called = true;
        return rune::storage::LogReadView{};
    };
    vs.driveFolds(/*srcTopicId*/ 99, /*partition*/ 0, readRange, /*bound*/ 5, rune::view::kNoAbortedTxns, /*nowNs*/ 0);
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
    rune::node::WorkerViewSet vs(channel, tmpDir("rune-views-test"));
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
    vs.driveFolds(/*srcTopicId*/ 5, /*partition*/ 0, readP0, /*bound*/ 1, rune::view::kNoAbortedTxns, 0);
    vs.driveFolds(/*srcTopicId*/ 5, /*partition*/ 1, readP1, /*bound*/ 1, rune::view::kNoAbortedTxns, 0);

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
    rune::node::WorkerViewSet vs(channel, tmpDir("rune-views-recovery-notready"));
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
    rune::node::WorkerViewSet vs(channel, tmpDir("rune-views-plain-notfound"));
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
    const std::string d = tmpDir(name);
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
    rune::node::WorkerViewSet vs(channel, tmpDir("rune-views-agg"));
    vs.installView(comp.plan, comp.rowLayout, src, /*persisted*/ false);

    const auto batch = orderBatch(0, {{1, 100, 10}, {2, 100, 30}, {3, 200, 5}});
    auto rr = [&](int64_t, int64_t) { return rune::storage::LogReadView{batch.data(), batch.size(), 0, 3}; };
    vs.driveFolds(6, 0, rr, 3, rune::view::kNoAbortedTxns, 0);

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
    rune::node::WorkerViewSet vs1(ch1, tmpDir("rune-views-agg-rec"));
    vs1.installView(comp.plan, comp.rowLayout, src, /*persisted*/ true);
    const auto b1 = orderBatch(0, {{1, 100, 10}, {2, 100, 30}});
    auto rr1 = [&](int64_t, int64_t) { return rune::storage::LogReadView{b1.data(), b1.size(), 0, 2}; };
    vs1.driveFolds(6, 0, rr1, 2, rune::view::kNoAbortedTxns, 0);

    const auto deltas = drainDeltas(ch1);
    ASSERT_EQ(deltas.size(), 1u);
    uint32_t magic; std::memcpy(&magic, deltas[0].data(), 4);
    EXPECT_EQ(magic, rune::view::kViewCheckpointMagicV2) << "aggregate views write v2";

    // 2. Recover into a fresh worker view set from that checkpoint.
    rune::node::ViewSnapshotChannel ch2;
    rune::node::WorkerViewSet vs2(ch2, tmpDir("rune-views-agg-rec"));
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
    vs2.driveFolds(6, 0, rr2, 3, rune::view::kNoAbortedTxns, 0);
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
    vs1.driveFolds(6, 0, rr1, 2, rune::view::kNoAbortedTxns, 0);
    const auto deltas = drainDeltas(ch1);
    ASSERT_EQ(deltas.size(), 1u);
    {   // Exactly what the snapshot IO agent appends (same file name scheme).
        std::ofstream f(rune::node::viewCheckpointFilePath(dir, static_cast<int32_t>(0x40000009u), 0), std::ios::binary);
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
    vs2.driveFolds(6, 0, rr2, 3, rune::view::kNoAbortedTxns, 0);

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
    vs.driveFolds(6, 0, rr, 3, rune::view::kNoAbortedTxns, 0);

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
    rune::node::WorkerViewSet vs(ch, tmpDir("rune-views-test"));
    vs.installView(comp.plan, comp.rowLayout, src, /*persisted*/ true);
    const auto batch = twoUpsertsKey7();
    auto rr = [&](int64_t, int64_t) { return rune::storage::LogReadView{batch.data(), batch.size(), 0, 2}; };
    vs.driveFolds(5, 0, rr, 2, rune::view::kNoAbortedTxns, 0);
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
        rune::node::WorkerViewSet vs(ch, tmpDir("rune-views-test"));
        vs.installView(comp.plan, comp.rowLayout, src, /*persisted*/ false);
        const auto batch = twoUpsertsKey7();
        auto rr = [&](int64_t, int64_t) { return rune::storage::LogReadView{batch.data(), batch.size(), 0, 2}; };
        vs.driveFolds(5, 0, rr, 2, rune::view::kNoAbortedTxns, 0);
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
            vs.driveFolds(5, 0, rr2, off + 1, rune::view::kNoAbortedTxns, 0);
            ASSERT_EQ(vs.dirtyBacklog(0x40000007, 0), 0u) << "pass " << off;
        }
    }
    {   // Aggregate view: contributor 1 hops between groups 100 and 200, so a
        // group empties (erase) and is recreated (reinsert) every pass.
        const auto src = orderSrc();
        const rune::codec::Descriptor* srcs[] = {&src};
        auto comp = rune::view::compileView(userTotalsPlan(1), srcs, 0x40000009).value();
        rune::node::ViewSnapshotChannel ch;
        rune::node::WorkerViewSet vs(ch, tmpDir("rune-views-agg"));
        vs.installView(comp.plan, comp.rowLayout, src, /*persisted*/ false);
        for (int64_t off = 0; off < 64; ++off) {
            const auto b = orderBatch(off, {{1, (off % 2) ? 200 : 100, 10}});
            auto rr = [&](int64_t, int64_t) { return rune::storage::LogReadView{b.data(), b.size(), off, off + 1}; };
            vs.driveFolds(6, 0, rr, off + 1, rune::view::kNoAbortedTxns, 0);
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
        const int w = vs.driveFolds(src, 0, rr, bound, rune::view::kNoAbortedTxns, 0);
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
    rune::node::WorkerViewSet vs(ch, tmpDir("rune-views-agg-large"));
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
    rune::node::WorkerViewSet vs1(ch1, tmpDir("rune-views-agg-large-rec"));
    vs1.installView(comp.plan, comp.rowLayout, src, /*persisted*/ true);
    std::vector<std::vector<uint8_t>> deltas;
    driveToEnd(vs1, ch1, 6, rr, kN, deltas);
    std::vector<std::span<const uint8_t>> spans(deltas.begin(), deltas.end());

    rune::node::ViewSnapshotChannel ch2;
    rune::node::WorkerViewSet vs2(ch2, tmpDir("rune-views-agg-large-rec"));
    vs2.installViewForRecovery(comp.plan, comp.rowLayout, src, /*persisted*/ true, 0, {},
                               std::span<const std::span<const uint8_t>>(spans.data(), spans.size()),
                               /*committedTail*/ kN);
    EXPECT_EQ(vs2.dirtyBacklog(0x40000009, 0), 0u) << "restored rows must not be dirty";

    // One post-recovery pass: contributor 1 (group 1, amount 1) -> amount 1001.
    const auto b2 = orderBatch(kN, {{1, 1, 1001}});
    auto rr2 = [&](int64_t, int64_t) { return rune::storage::LogReadView{b2.data(), b2.size(), kN, kN + 1}; };
    ASSERT_NO_THROW(vs2.driveFolds(6, 0, rr2, kN + 1, rune::view::kNoAbortedTxns, 0));
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
    rune::node::WorkerViewSet vs(ch, tmpDir("rune-views-replace-large"));
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
    vs1.driveFolds(6, 0, rr1, 2, rune::view::kNoAbortedTxns, 0);
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
            std::ofstream f(rune::node::viewCheckpointFilePath(dir, static_cast<int32_t>(0x40000009u), 0), std::ios::binary);
            f.write(reinterpret_cast<const char*>(deltas[0].data()),
                    static_cast<std::streamsize>(deltas[0].size()));
        }
        LogCapture cap;
        rune::node::ViewSnapshotChannel ch3;
        rune::node::WorkerViewSet vs3(ch3, dir);
        vs3.installView(comp.plan, comp.rowLayout, src, true, /*underRecovery*/ true);
        vs3.driveFolds(6, 0, rr1, 2, rune::view::kNoAbortedTxns, 0);
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
        rune::node::WorkerViewSet vs5(ch5, tmpDir("rune-views-recovered-log-v1"));
        vs5.installView(rcomp.plan, rcomp.rowLayout, csrc, true);
        const auto b = oneUpsert(7, 10);
        auto rr = [&](int64_t, int64_t) { return rune::storage::LogReadView{b.data(), b.size(), 0, 1}; };
        vs5.driveFolds(5, 0, rr, 1, rune::view::kNoAbortedTxns, 0);
        const auto d = drainDeltas(ch5);
        ASSERT_EQ(d.size(), 1u);
        LogCapture cap;
        rune::node::ViewSnapshotChannel ch6;
        rune::node::WorkerViewSet vs6(ch6, tmpDir("rune-views-recovered-log-v1"));
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
    rune::node::WorkerViewSet vs(ch, tmpDir("rune-views-fold-counters"));
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
    vs.driveFolds(8, 0, rr, 4, rune::view::kNoAbortedTxns, 0);

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
    vs.driveFolds(8, 0, rr2, 5, rune::view::kNoAbortedTxns, 0);
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
    rune::node::WorkerViewSet vs1(ch1, tmpDir("rune-views-agg-midbatch"));
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
    rune::node::WorkerViewSet vs2(ch2, tmpDir("rune-views-agg-midbatch"));
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

// ---------------------------------------------------------------------------
// Phase 2b E: views over a TRANSIENT source.
// ---------------------------------------------------------------------------
namespace {
std::vector<uint8_t> upsertAt(int64_t base, int64_t key, int32_t hp) {
    rune::protocol::RecordBatchBuilder b(base, 0);
    const auto k = keyBytes(key);
    const auto v = encodeCharacter(key, hp, 1);
    b.append(asSv(k), asSv(v), 0);
    return b.build();
}
} // namespace

namespace {
// n upserts (keys base..base+n-1, hp = key) in ONE batch at offsets [base,base+n).
std::vector<uint8_t> upsertRun(int64_t base, int n) {
    rune::protocol::RecordBatchBuilder b(base, 0);
    for (int i = 0; i < n; ++i) {
        const auto k = keyBytes(base + i);
        const auto v = encodeCharacter(base + i, static_cast<int32_t>(base + i), 1);
        b.append(asSv(k), asSv(v), 0);
    }
    return b.build();
}
} // namespace

// Invariant breach: a view over a transient topic is itself transient, so its
// cursor can never sit above the source end.  If it does (the source restarted
// EMPTY and reuses offsets 0.. below a live cursor) the rows are kept, one warn
// is logged, and every post-restart record must fold (without the reset the
// cursor 2 would silently skip the new record at offset 0).
TEST(WorkerViewSetRegression, TransientSourceCursorAboveLogEndIsInvariantBreachRegression) {
    auto src = charSourceV1();
    const rune::codec::Descriptor* srcs[] = {&src};
    auto comp = rune::view::compileView(hpView(1), srcs, 0x40000031).value();
    rune::node::ViewSnapshotChannel ch;
    rune::node::WorkerViewSet vs(ch, tmpDir("rune-views-transient-reset"));
    vs.installView(comp.plan, comp.rowLayout, src, /*persisted*/ false);

    const auto before = twoUpsertsKey7();                                    // offsets [0,2)
    auto rr0 = [&](int64_t, int64_t) { return rune::storage::LogReadView{before.data(), before.size(), 0, 2}; };
    const rune::node::TransientBounds tb0{0, 2};
    vs.driveFolds(5, 0, rr0, 2, rune::view::kNoAbortedTxns, 0, &tb0);
    const rune::view::RowLayout* L = nullptr;
    ASSERT_NE(vs.getRow(0x40000031, 0, 7, L), nullptr);

    // Source restarted empty: logStart 0, logEnd 0 < cursor 2.
    const auto after = upsertAt(0, 9, 77);                                   // reused offset 0
    auto rr1 = [&](int64_t, int64_t) { return rune::storage::LogReadView{after.data(), after.size(), 0, 1}; };
    const rune::node::TransientBounds tb1{0, 0};
    vs.driveFolds(5, 0, rr1, 0, rune::view::kNoAbortedTxns, 0, &tb1);                                    // breach: cursor -> 0
    const rune::node::TransientBounds tb2{0, 1};
    vs.driveFolds(5, 0, rr1, 1, rune::view::kNoAbortedTxns, 0, &tb2);
    const uint8_t* row9 = vs.getRow(0x40000031, 0, 9, L);
    ASSERT_NE(row9, nullptr) << "a post-restart record at a reused offset must fold";
    EXPECT_EQ(rune::view::readSlot(row9, *L, 1).i, 77);
    EXPECT_NE(vs.getRow(0x40000031, 0, 7, L), nullptr) << "pre-restart rows are kept";
    EXPECT_EQ(vs.foldCounters(0x40000031, 0)->transientRecordsEvicted, 0u);
}

// The ring evicted records the view had not folded yet: skip to logStart and
// count the loss -- never stall on an offset the source no longer has.
TEST(WorkerViewSetRegression, TransientSourceEvictionAheadOfCursorSkipsAndCountsRegression) {
    auto src = charSourceV1();
    const rune::codec::Descriptor* srcs[] = {&src};
    auto comp = rune::view::compileView(hpView(1), srcs, 0x40000032).value();
    rune::node::ViewSnapshotChannel ch;
    rune::node::WorkerViewSet vs(ch, tmpDir("rune-views-transient-evict"));
    vs.installView(comp.plan, comp.rowLayout, src, /*persisted*/ false);

    auto empty = [](int64_t, int64_t) { return rune::storage::LogReadView{}; };
    const rune::node::TransientBounds tb0{0, 0};
    vs.driveFolds(5, 0, empty, 0, rune::view::kNoAbortedTxns, 0, &tb0);                 // instance exists, Live at cursor 0
    ASSERT_NE(vs.foldCounters(0x40000032, 0), nullptr);

    const auto b = upsertAt(3, 11, 5);                      // the surviving record at offset 3
    auto rr = [&](int64_t from, int64_t) {
        EXPECT_EQ(from, 3) << "the fold resumes at logStart, not at the evicted cursor";
        return rune::storage::LogReadView{b.data(), b.size(), 3, 4};
    };
    const rune::node::TransientBounds tb1{3, 4};
    vs.driveFolds(5, 0, rr, 4, rune::view::kNoAbortedTxns, 0, &tb1);
    EXPECT_EQ(vs.foldCounters(0x40000032, 0)->transientRecordsEvicted, 3u);
    const rune::view::RowLayout* L = nullptr;
    ASSERT_NE(vs.getRow(0x40000032, 0, 11, L), nullptr);
}

// M1: an instance created AFTER the ring already evicted records starts at
// logStart in the SAME pass (no empty pass) and does not count the earlier
// eviction as a loss -- the view never had those records to lose.
TEST(WorkerViewSetRegression, TransientFreshInstanceStartsAtLogStartWithoutCountingRegression) {
    auto src = charSourceV1();
    const rune::codec::Descriptor* srcs[] = {&src};
    auto comp = rune::view::compileView(hpView(1), srcs, 0x40000034).value();
    rune::node::ViewSnapshotChannel ch;
    rune::node::WorkerViewSet vs(ch, tmpDir("rune-views-transient-fresh"));
    vs.installView(comp.plan, comp.rowLayout, src, /*persisted*/ false);

    const auto b = upsertAt(5, 12, 9);
    auto rr = [&](int64_t from, int64_t) {
        EXPECT_EQ(from, 5);
        return rune::storage::LogReadView{b.data(), b.size(), 5, 6};
    };
    const rune::node::TransientBounds tb{5, 6};
    EXPECT_GT(vs.driveFolds(5, 0, rr, 6, rune::view::kNoAbortedTxns, 0, &tb), 0);
    const rune::view::RowLayout* L = nullptr;
    ASSERT_NE(vs.getRow(0x40000034, 0, 12, L), nullptr) << "folded on the creating pass";
    EXPECT_EQ(vs.foldCounters(0x40000034, 0)->transientRecordsEvicted, 0u);
}

// I1: a RECOVERED cursor (checkpoint of an earlier source incarnation) is
// meaningless once the source restarted and reuses offsets -- even when the new
// incarnation has already caught up PAST the cursor (logEnd >= O), where a
// plain "cursor > logEnd" check would miss it and skip [logStart, O).
TEST(WorkerViewSetRegression, TransientRecoveredCursorIsReconciledEvenWhenSourceCaughtUpRegression) {
    auto src = charSourceV1();
    const rune::codec::Descriptor* srcs[] = {&src};
    auto comp = rune::view::compileView(hpView(0), srcs, 0x40000033).value();

    rune::node::ViewSnapshotChannel ch1;
    rune::node::WorkerViewSet vs1(ch1, tmpDir("rune-views-transient-rec1"));
    vs1.installView(comp.plan, comp.rowLayout, src, /*persisted*/ true);
    const auto b1 = twoUpsertsKey7();                                        // checkpoint O = 2
    auto rr1 = [&](int64_t, int64_t) { return rune::storage::LogReadView{b1.data(), b1.size(), 0, 2}; };
    vs1.driveFolds(5, 0, rr1, 2, rune::view::kNoAbortedTxns, 0);
    const auto deltas = drainDeltas(ch1);
    ASSERT_EQ(deltas.size(), 1u);

    rune::node::ViewSnapshotChannel ch2;
    rune::node::WorkerViewSet vs2(ch2, tmpDir("rune-views-transient-rec2"));
    const std::span<const uint8_t> recs[] = {deltas[0]};
    vs2.installViewForRecovery(comp.plan, comp.rowLayout, src, /*persisted*/ true, 0, {}, recs,
                               /*committedTail*/ 5);

    // The restarted source already holds [0,5) of NEW data (logEnd 5 >= O 2).
    const auto fresh = upsertRun(0, 5);                                      // keys 0..4
    auto rr2 = [&](int64_t from, int64_t) {
        EXPECT_EQ(from, 0) << "recovered cursor must restart at logStart";
        return rune::storage::LogReadView{fresh.data(), fresh.size(), 0, 5};
    };
    const rune::node::TransientBounds tb{0, 5};
    vs2.driveRebuild(5, 0, rr2, 5, rune::view::kNoAbortedTxns, 0, &tb);
    vs2.driveFolds(5, 0, rr2, 5, rune::view::kNoAbortedTxns, 0, &tb);
    const rune::view::RowLayout* L = nullptr;
    ASSERT_NE(vs2.getRow(0x40000033, 0, 0, L), nullptr) << "offset 0 (< recovered O) must fold";
    ASSERT_NE(vs2.getRow(0x40000033, 0, 1, L), nullptr) << "offset 1 (< recovered O) must fold";
    ASSERT_NE(vs2.getRow(0x40000033, 0, 4, L), nullptr);
    EXPECT_NE(vs2.getRow(0x40000033, 0, 7, L), nullptr) << "checkpoint rows are kept";
}

// REGRESSION: view data dirs are per process (no cross-process temp-dir collision
// under ctest -j / concurrent checkouts).
TEST(WorkerViewSetRegression, ViewDataDirsArePerProcess) {
    const std::string d = freshDir("rune-views-per-process");
    EXPECT_EQ(d, (std::filesystem::temp_directory_path() /
                  ("rune-views-per-process-" + std::to_string(::getpid()))).string());
    EXPECT_TRUE(std::filesystem::is_directory(d));
    EXPECT_EQ(tmpDir("rune-views-test"), tmpDir("rune-views-test")) << "stable within a process";
}

// ---------------------------------------------------------------------------
// Phase 2b E2 (Part E Task 7 carry-over): a Live cursor is live against ONE
// source ring identity — (replica incarnation, logGeneration).
// ---------------------------------------------------------------------------
namespace {
std::vector<uint8_t> concatBatches(std::initializer_list<std::vector<uint8_t>> parts) {
    std::vector<uint8_t> out;
    for (const auto& p : parts) out.insert(out.end(), p.begin(), p.end());
    return out;
}
} // namespace

// The source ring was RECREATED (a new replica for the same partition on this
// node) and its range [0,3) happens to cover the old cursor 2: before the fix
// the instance stayed Live at 2 — keys 7 and 8 kept their stale values.
TEST(WorkerViewSetRegression, TransientSourceRecreatedRefoldsFromRingStartRegression) {
    auto src = charSourceV1();
    const rune::codec::Descriptor* srcs[] = {&src};
    auto comp = rune::view::compileView(hpView(1), srcs, 0x40000035).value();
    rune::node::ViewSnapshotChannel ch;
    rune::node::WorkerViewSet vs(ch, tmpDir("rune-views-transient-recreated"));
    vs.installView(comp.plan, comp.rowLayout, src, /*persisted*/ false);

    const auto inc1 = concatBatches({upsertAt(0, 7, 10), upsertAt(1, 8, 20)});
    auto rr1 = [&](int64_t, int64_t) { return rune::storage::LogReadView{inc1.data(), inc1.size(), 0, 2}; };
    rune::node::TransientBounds tb1{0, 2};
    tb1.incarnation = 1;
    vs.driveFolds(5, 0, rr1, 2, rune::view::kNoAbortedTxns, 0, &tb1);
    const rune::view::RowLayout* L = nullptr;
    ASSERT_NE(vs.getRow(0x40000035, 0, 7, L), nullptr);
    ASSERT_EQ(slotOf(vs, 0x40000035, 0, 7, 1).i, 10);

    const auto inc2 = concatBatches({upsertAt(0, 7, 99), upsertAt(1, 8, 98), upsertAt(2, 9, 97)});
    auto rr2 = [&](int64_t, int64_t) { return rune::storage::LogReadView{inc2.data(), inc2.size(), 0, 3}; };
    rune::node::TransientBounds tb2{0, 3};
    tb2.incarnation = 2;
    vs.driveFolds(5, 0, rr2, 3, rune::view::kNoAbortedTxns, 0, &tb2);
    EXPECT_EQ(slotOf(vs, 0x40000035, 0, 7, 1).i, 99) << "a stale row survived";
    EXPECT_EQ(slotOf(vs, 0x40000035, 0, 8, 1).i, 98) << "a stale row survived";
    ASSERT_NE(vs.getRow(0x40000035, 0, 9, L), nullptr);
    EXPECT_EQ(vs.foldCounters(0x40000035, 0)->transientSourceResets, 1u);
    EXPECT_EQ(vs.foldCounters(0x40000035, 0)->transientRecordsEvicted, 0u) << "a recreation is not an eviction";
}

// The ring's content changed IN PLACE (logGeneration moved: a truncation or a
// follower ring reset) with the cut BELOW the cursor: re-fold from the cut.  A
// generation change whose cut is at/above the cursor leaves it alone.
TEST(WorkerViewSetRegression, TransientSourceCutBelowCursorRefoldsFromTheCutRegression) {
    auto src = charSourceV1();
    const rune::codec::Descriptor* srcs[] = {&src};
    auto comp = rune::view::compileView(hpView(1), srcs, 0x40000036).value();
    rune::node::ViewSnapshotChannel ch;
    rune::node::WorkerViewSet vs(ch, tmpDir("rune-views-transient-cut"));
    vs.installView(comp.plan, comp.rowLayout, src, /*persisted*/ false);

    const auto gen0 = concatBatches({upsertAt(0, 7, 1), upsertAt(1, 8, 2), upsertAt(2, 9, 3)});
    auto rr0 = [&](int64_t, int64_t) { return rune::storage::LogReadView{gen0.data(), gen0.size(), 0, 3}; };
    rune::node::TransientBounds tb0{0, 3};
    vs.driveFolds(5, 0, rr0, 3, rune::view::kNoAbortedTxns, 0, &tb0);   // Live, cursor 3, generation 0

    // Generation 1, cut at 1: offsets >= 1 changed — 8 and 9 now carry new values.
    const auto gen1 = concatBatches({upsertAt(0, 7, 1), upsertAt(1, 8, 22), upsertAt(2, 9, 33)});
    auto rr1 = [&](int64_t from, int64_t) {
        EXPECT_EQ(from, 1) << "re-fold from the cut, not from the ring start or the old cursor";
        return rune::storage::LogReadView{gen1.data(), gen1.size(), 0, 3};
    };
    rune::node::TransientBounds tb1{0, 3};
    tb1.logGeneration = 1;
    tb1.cutOffset     = 1;
    vs.driveFolds(5, 0, rr1, 3, rune::view::kNoAbortedTxns, 0, &tb1);
    const rune::view::RowLayout* L = nullptr;
    EXPECT_EQ(slotOf(vs, 0x40000036, 0, 8, 1).i, 22);
    EXPECT_EQ(slotOf(vs, 0x40000036, 0, 9, 1).i, 33);
    EXPECT_EQ(vs.foldCounters(0x40000036, 0)->transientSourceResets, 1u);

    // Generation 2, cut at 5 (above the cursor 3): nothing the view folded changed.
    auto rrNone = [&](int64_t, int64_t) { return rune::storage::LogReadView{}; };
    rune::node::TransientBounds tb2{0, 3};
    tb2.logGeneration = 2;
    tb2.cutOffset     = 5;
    vs.driveFolds(5, 0, rrNone, 3, rune::view::kNoAbortedTxns, 0, &tb2);
    EXPECT_EQ(vs.foldCounters(0x40000036, 0)->transientSourceResets, 1u) << "a cut above the cursor is no reset";
}

// The follower ring-reset shape (PartitionReplica::resetTransientLogTo_): the
// generation moves with the cut at the OLD ring start and the ring restarts
// empty ABOVE the cursor.  The view re-folds from the oldest held record (the
// new logStart), counts one reset, and the jump is not counted as an eviction.
TEST(WorkerViewSetRegression, TransientSourceRingResetRefoldsFromOldestHeldRecordRegression) {
    auto src = charSourceV1();
    const rune::codec::Descriptor* srcs[] = {&src};
    auto comp = rune::view::compileView(hpView(1), srcs, 0x40000037).value();
    rune::node::ViewSnapshotChannel ch;
    rune::node::WorkerViewSet vs(ch, tmpDir("rune-views-transient-ring-reset"));
    vs.installView(comp.plan, comp.rowLayout, src, /*persisted*/ false);

    const auto gen0 = concatBatches({upsertAt(0, 7, 1), upsertAt(1, 8, 2)});
    auto rr0 = [&](int64_t, int64_t) { return rune::storage::LogReadView{gen0.data(), gen0.size(), 0, 2}; };
    rune::node::TransientBounds tb0{0, 2};
    vs.driveFolds(5, 0, rr0, 2, rune::view::kNoAbortedTxns, 0, &tb0);   // Live, cursor 2, generation 0

    const auto reset = upsertAt(10, 9, 3);  // ring restarted at the leader's start 10
    auto rr1 = [&](int64_t from, int64_t) {
        EXPECT_EQ(from, 10) << "re-fold from the oldest held record";
        return rune::storage::LogReadView{reset.data(), reset.size(), 10, 11};
    };
    rune::node::TransientBounds tb1{10, 11};
    tb1.logGeneration = 1;
    tb1.cutOffset     = 0;                  // cut at the OLD ring start
    vs.driveFolds(5, 0, rr1, 11, rune::view::kNoAbortedTxns, 0, &tb1);
    const rune::view::RowLayout* L = nullptr;
    ASSERT_NE(vs.getRow(0x40000037, 0, 9, L), nullptr);
    EXPECT_NE(vs.getRow(0x40000037, 0, 7, L), nullptr) << "rows are kept";
    EXPECT_EQ(vs.foldCounters(0x40000037, 0)->transientSourceResets, 1u);
    EXPECT_EQ(vs.foldCounters(0x40000037, 0)->transientRecordsEvicted, 0u) << "a reset is not an eviction";
}

// Fix round 1: PartitionReplica keeps only the LAST cut.  Two cuts between view
// passes (truncate at 2, then at 5; generation 0 -> 2) with the cursor at 8:
// trusting the last cut (5) would keep the stale rows folded from [2,5).  A
// generation jump of more than one re-folds from the ring start.
TEST(WorkerViewSetRegression, TransientSourceGenerationJumpRefoldsFromRingStartRegression) {
    auto src = charSourceV1();
    const rune::codec::Descriptor* srcs[] = {&src};
    auto comp = rune::view::compileView(hpView(1), srcs, 0x40000038).value();
    rune::node::ViewSnapshotChannel ch;
    rune::node::WorkerViewSet vs(ch, tmpDir("rune-views-transient-gen-jump"));
    vs.installView(comp.plan, comp.rowLayout, src, /*persisted*/ false);

    // Generation 0: keys 100..107 at offsets [0,8), hp = key.
    const auto g0 = concatBatches({upsertAt(0, 100, 0), upsertAt(1, 101, 1), upsertAt(2, 102, 2),
                                   upsertAt(3, 103, 3), upsertAt(4, 104, 4), upsertAt(5, 105, 5),
                                   upsertAt(6, 106, 6), upsertAt(7, 107, 7)});
    auto rrG0 = [&](int64_t, int64_t) { return rune::storage::LogReadView{g0.data(), g0.size(), 0, 8}; };
    rune::node::TransientBounds tb0{0, 8};
    vs.driveFolds(5, 0, rrG0, 8, rune::view::kNoAbortedTxns, 0, &tb0);   // Live, cursor 8, generation 0
    const rune::view::RowLayout* L = nullptr;
    ASSERT_EQ(slotOf(vs, 0x40000038, 0, 103, 1).i, 3);

    // Truncated at 2 (gen 1), refilled, truncated at 5 (gen 2), refilled: the
    // ring now holds new values for offsets [2,8).  Only the last cut (5) is kept.
    const auto g2 = concatBatches({upsertAt(0, 100, 0), upsertAt(1, 101, 1), upsertAt(2, 102, 52),
                                   upsertAt(3, 103, 53), upsertAt(4, 104, 54), upsertAt(5, 105, 55),
                                   upsertAt(6, 106, 56), upsertAt(7, 107, 57)});
    auto rrG2 = [&](int64_t from, int64_t) {
        EXPECT_EQ(from, 0) << "an unknown earlier cut: re-fold from the ring start";
        return rune::storage::LogReadView{g2.data(), g2.size(), 0, 8};
    };
    rune::node::TransientBounds tb2{0, 8};
    tb2.logGeneration = 2;
    tb2.cutOffset     = 5;
    vs.driveFolds(5, 0, rrG2, 8, rune::view::kNoAbortedTxns, 0, &tb2);
    for (int64_t k = 102; k <= 107; ++k)
        EXPECT_EQ(slotOf(vs, 0x40000038, 0, static_cast<uint64_t>(k), 1).i, k - 50)
            << "stale row for key " << k;
    EXPECT_EQ(vs.foldCounters(0x40000038, 0)->transientSourceResets, 1u);
    EXPECT_EQ(vs.foldCounters(0x40000038, 0)->transientRecordsEvicted, 0u);
}

// ---------------------------------------------------------------------------
// Catalog history Task 8: a DropView / AlterView retired the view — the worker
// drops its plan template, every instance (table, dirty state) and its reactor
// index on its own thread; nothing folds into it any more.  RED: uninstall a
// no-op (the dropped view keeps folding and serving rows, its memory never freed).
// ---------------------------------------------------------------------------
TEST(WorkerViewSetRegression, UninstallViewStopsFoldingAndFreesItsInstances) {
    auto src = charSourceV1();
    const rune::codec::Descriptor* srcs[] = {&src};
    auto comp  = rune::view::compileView(hpView(), srcs, 0x40000007).value();
    auto other = rune::view::compileView(hpView(), srcs, 0x40000008).value();   // a 2nd view over the source

    rune::node::ViewSnapshotChannel channel;
    rune::node::WorkerViewSet vs(channel, tmpDir("rune-views-uninstall"));
    vs.installView(comp.plan, comp.rowLayout, src, /*persisted*/ false);
    vs.installView(other.plan, other.rowLayout, src, /*persisted*/ false);

    const auto batch = twoUpsertsKey7();
    auto readRange = [&](int64_t, int64_t) -> rune::storage::LogReadView {
        return rune::storage::LogReadView{batch.data(), batch.size(), 0, 2};
    };
    vs.driveFolds(5, 0, readRange, 2, rune::view::kNoAbortedTxns, 0);
    vs.driveFolds(5, 1, readRange, 2, rune::view::kNoAbortedTxns, 0);
    ASSERT_EQ(vs.viewCount(), 2u);
    ASSERT_EQ(vs.instanceCount(), 4u);

    EXPECT_TRUE(vs.uninstallView(0x40000007));
    EXPECT_EQ(vs.viewCount(), 1u);
    EXPECT_EQ(vs.instanceCount(), 2u) << "both partitions' instances of the dropped view are freed";
    const rune::view::RowLayout* L = nullptr;
    rune::node::WorkerViewSet::RowStatus st = rune::node::WorkerViewSet::RowStatus::Ok;
    EXPECT_EQ(vs.getRow(0x40000007, 0, 7, L, &st), nullptr);
    EXPECT_EQ(st, rune::node::WorkerViewSet::RowStatus::NotFound);
    uint64_t fp = 0;
    EXPECT_FALSE(vs.viewOutputFingerprint(0x40000007, fp)) << "its plan template is gone";

    // Folding the source again creates nothing for the dropped view; the other view still serves.
    vs.driveFolds(5, 0, readRange, 2, rune::view::kNoAbortedTxns, 0);
    EXPECT_EQ(vs.instanceCount(), 2u) << "a dropped view never folds again (no lazy re-create)";
    EXPECT_NE(vs.getRow(0x40000008, 0, 7, L), nullptr);
    EXPECT_TRUE(vs.interestedIn(5)) << "the other view keeps the source's reactor entry";

    EXPECT_TRUE(vs.uninstallView(0x40000008));
    EXPECT_FALSE(vs.interestedIn(5)) << "no view left: the source's reactor entry is removed";
    EXPECT_EQ(vs.instanceCount(), 0u);
}

TEST(WorkerViewSetRegression, UninstallUnknownViewIsANoOp) {
    auto src = charSourceV1();
    const rune::codec::Descriptor* srcs[] = {&src};
    auto comp = rune::view::compileView(hpView(), srcs, 0x40000007).value();
    rune::node::ViewSnapshotChannel channel;
    rune::node::WorkerViewSet vs(channel, tmpDir("rune-views-uninstall-unknown"));
    vs.installView(comp.plan, comp.rowLayout, src, /*persisted*/ false);
    EXPECT_FALSE(vs.uninstallView(0x7777));
    EXPECT_EQ(vs.viewCount(), 1u) << "an unknown id touches nothing";
    EXPECT_TRUE(vs.uninstallView(0x40000007));
    EXPECT_FALSE(vs.uninstallView(0x40000007)) << "the same uninstall again is a no-op (idempotent)";
    EXPECT_EQ(vs.viewCount(), 0u);
}

// The production delivery: the control thread fans the uninstall out on each
// worker's install ring (ViewPlanUpdateMsg.uninstall, op byte 2).
TEST(WorkerViewSetRegression, UninstallArrivesOnTheInstallRing) {
    auto src = charSourceV1();
    const rune::codec::Descriptor* srcs[] = {&src};
    auto comp = rune::view::compileView(hpView(), srcs, 0x40000007).value();
    rune::node::ViewSnapshotChannel channel;
    rune::node::WorkerViewSet vs(channel, tmpDir("rune-views-uninstall-ring"));
    vs.installView(comp.plan, comp.rowLayout, src, /*persisted*/ false);
    rune::node::ViewPlanUpdateMsg m;
    m.viewTopicId = 0x40000007;
    m.uninstall   = true;
    ASSERT_TRUE(rune::node::encodeViewPlanUpdate(channel.install, rune::node::ViewSnapshotChannel::kInstallMsgType, m));
    (void)vs.drainInstalls();
    EXPECT_EQ(vs.viewCount(), 0u);
    // Round trip of the op byte: uninstall / recovery / install never alias.
    std::vector<uint8_t> buf(rune::node::viewPlanUpdateWireLen(m));
    rune::node::writeViewPlanUpdate(buf.data(), m.viewTopicId, 0, rune::node::kViewPlanUninstallByte, {}, {}, {});
    const auto d = rune::node::decodeViewPlanUpdate(buf.data(), buf.size());
    EXPECT_TRUE(d.uninstall);
    EXPECT_FALSE(d.recovery);
    rune::node::writeViewPlanUpdate(buf.data(), m.viewTopicId, 0, 1, {}, {}, {});
    const auto r = rune::node::decodeViewPlanUpdate(buf.data(), buf.size());
    EXPECT_TRUE(r.recovery);
    EXPECT_FALSE(r.uninstall);
}

namespace {
// user_max = GROUP BY userId: Count(*), Max(amount), Last(amount) over "order"
// (topic 6) — a LINKED aggregate view (Phase 2b).
rune::view::ViewPlan userMaxPlan(uint8_t persistence) {
    using P = rune::codec::PrimType; using R = rune::view::MergeRule;
    rune::view::ViewPlan v;
    v.viewName = "user_max"; v.key = "userId"; v.sources = {{6, "userId"}};
    v.output = {{"userId", P::Int64, 0, "userId", R::Replace},
                {"n", P::Int64, 0, "", R::CountAll},
                {"max", P::Int64, 0, "amount", R::Max},
                {"last", P::Int64, 0, "amount", R::Last}};
    v.persistence = persistence;
    return v;
}
} // namespace

// REGRESSION (Review Focus #2): after recovery, retracting the cached Max must
// rescan the RECOVERED value block (table 3 of the v2 checkpoint; links, head,
// cache, block id and positions came back in the contributor and group rows)
// and surface the exact next extreme.
TEST(WorkerViewSetRegression, RestartThenRetractingTheMaxIsExactRegression) {
    const auto src = orderSrc();
    const rune::codec::Descriptor* srcs[] = {&src};
    auto comp = rune::view::compileView(userMaxPlan(0), srcs, 0x4000000A).value();

    rune::node::ViewSnapshotChannel ch1;
    rune::node::WorkerViewSet vs1(ch1, tmpDir("rune-views-max-rec"));
    vs1.installView(comp.plan, comp.rowLayout, src, /*persisted*/ true);
    const auto b1 = orderBatch(0, {{1, 100, 10}, {2, 100, 50}, {3, 100, 30}});
    auto rr1 = [&](int64_t, int64_t) { return rune::storage::LogReadView{b1.data(), b1.size(), 0, 3}; };
    vs1.driveFolds(6, 0, rr1, 3, rune::view::kNoAbortedTxns, 0);
    const auto deltas = drainDeltas(ch1);
    ASSERT_EQ(deltas.size(), 1u);
    uint32_t magic; std::memcpy(&magic, deltas[0].data(), 4);
    EXPECT_EQ(magic, rune::view::kViewCheckpointMagicV2) << "no new format: still the v2 record";
    EXPECT_EQ(deltas[0][rune::view::kCheckpointV2HeaderBytes - 1], 4u) << "a Min/Max view's record carries four tables (value blocks = id 3)";

    rune::node::ViewSnapshotChannel ch2;
    rune::node::WorkerViewSet vs2(ch2, tmpDir("rune-views-max-rec"));
    const std::span<const uint8_t> recs[] = {deltas[0]};
    vs2.installViewForRecovery(comp.plan, comp.rowLayout, src, /*persisted*/ true, /*partition*/ 0,
                               /*base*/ {}, recs, /*committedTail*/ 3);
    const rune::view::RowLayout* L = nullptr;
    const uint8_t* row = vs2.getRow(0x4000000A, 0, 100, L);
    ASSERT_NE(row, nullptr);
    EXPECT_EQ(rune::view::readSlot(row, *L, 2).i, 50);
    EXPECT_EQ(rune::view::readSlot(row, *L, 3).i, 30);

    // Post-restart: the max holder (contributor 2) drops to 5.
    const auto b2 = orderBatch(3, {{2, 100, 5}});
    auto rr2 = [&](int64_t, int64_t) { return rune::storage::LogReadView{b2.data(), b2.size(), 3, 4}; };
    vs2.driveFolds(6, 0, rr2, 4, rune::view::kNoAbortedTxns, 0);
    row = vs2.getRow(0x4000000A, 0, 100, L);
    ASSERT_NE(row, nullptr);
    EXPECT_EQ(rune::view::readSlot(row, *L, 1).i, 3);
    EXPECT_EQ(rune::view::readSlot(row, *L, 2).i, 30) << "next extreme found by scanning the recovered value block";
    EXPECT_EQ(rune::view::readSlot(row, *L, 3).i, 5) << "the re-write is now the head";
    const rune::node::ViewFoldCounters* fc = vs2.foldCounters(0x4000000A, 0);
    ASSERT_NE(fc, nullptr);
    EXPECT_EQ(fc->aggInconsistent, 0u);
    EXPECT_EQ(fc->aggRescans, 1u) << "the post-restart retraction rescanned once (counter surfaced, Task 5 review item 2)";
}

// REGRESSION (Review Focus #3, fix round 1): the TRUE worst-case move stream.
// Each of kG source groups S is built to 65 entries (3 chunks), 16 of them are
// moved out to a sink group (49 left: chunk 2 is the spare, tail holds 17), the
// list is reordered so contributor 1 (position 0) sits between 2 and 3 while the
// last-position entry 49 is NOT its neighbour, and the target group T already
// has a head.  The final move 1: S -> T then dirties 5 contributor rows (1, its
// two neighbours, T's old head, the swapped-in 49), 2 group rows, 2 view rows and
// 3 block rows (S's hole chunk, S's FREED spare, T's chunk) = exactly the
// {5,2,2,3} bound.  Moves are last in the stream and touch distinct groups, so
// every pass of moves is full-size.  Under an undercounted block term ({5,2,2,2})
// the per-pass cap lets a full pass of these moves overflow the ring max message
// and the worker THROWS; the assertions below prove the worst case is present.
TEST(WorkerViewSetRegression, PersistedRetractableMovesDeltasFitTheRingRegression) {
    const auto src = orderSrc();
    const rune::codec::Descriptor* srcs[] = {&src};
    auto comp = rune::view::compileView(userMaxPlan(0), srcs, 0x4000000C).value();
    const auto agg = rune::view::buildAggLayout(comp.plan, src).value();
    constexpr int64_t kG = 130;
    auto cid = [](int64_t g, int64_t k) { return (g + 1) * 1000 + k; };
    std::vector<std::array<int64_t, 3>> recs;
    for (int64_t g = 0; g < kG; ++g) recs.push_back({500000 + g, kG + g, 7});          // T's head
    for (int64_t g = 0; g < kG; ++g) {
        for (int64_t k = 1; k <= 65; ++k) recs.push_back({cid(g, k), g, k});            // S: 3 chunks
        for (int64_t k = 65; k >= 50; --k) recs.push_back({cid(g, k), 2 * kG + g, k});  // out to the sink
        recs.push_back({cid(g, 3), g, 3});                                              // S list: 2 -> 1 -> 3 -> 49 ...
        recs.push_back({cid(g, 1), g, 1});
        recs.push_back({cid(g, 2), g, 2});
    }
    for (int64_t g = 0; g < kG; ++g) recs.push_back({cid(g, 1), kG + g, 50});          // the worst-case move
    const int64_t total = static_cast<int64_t>(recs.size());
    const auto batch = orderBatch(0, recs);
    auto rr = [&](int64_t, int64_t) {
        return rune::storage::LogReadView{batch.data(), batch.size(), 0, total};
    };

    rune::node::ViewSnapshotChannel ch;
    rune::node::WorkerViewSet vs(ch, tmpDir("rune-views-max-large"));
    vs.installView(comp.plan, comp.rowLayout, src, /*persisted*/ true);
    std::vector<std::vector<uint8_t>> deltas;
    ASSERT_NO_THROW(driveToEnd(vs, ch, 6, rr, total, deltas));

    // Largest delta vs the ring budget and vs the undercounted {5,2,2,2} estimate.
    using namespace rune::view;
    const auto b = aggDirtyBoundPerRecord(agg);
    ASSERT_EQ(b.blockRows, 3u);
    auto perRecord = [&](uint32_t blockRows) {
        return b.contributorRows * (kCheckpointEntryHeaderBytes + agg.contributorRow.rowLen) +
               b.groupRows * (kCheckpointEntryHeaderBytes + agg.groupRow.rowLen) +
               b.viewRows * (kCheckpointEntryHeaderBytes + comp.rowLayout.rowLen) +
               blockRows * (kCheckpointEntryHeaderBytes + agg.blockRow.rowLen);
    };
    const std::size_t recFixed = kCheckpointV2HeaderBytes + 4 * kCheckpointV2TableHeaderBytes + kCheckpointCrcBytes;
    const std::size_t budget = static_cast<std::size_t>(ch.deltas.maxMsgLength()) -
                               rune::node::ViewSnapshotChannel::kDeltaHeaderBytes;
    const std::size_t perReal = perRecord(b.blockRows);
    const std::size_t recsPerPass = (budget - recFixed) / perReal;
    std::size_t maxDelta = 0;
    for (const auto& d : deltas) maxDelta = std::max(maxDelta, d.size());
    std::printf("[ worst-case ] largest delta %zu B of %zu B budget; %zu recs/pass; measured %zu B/record (bound %zu, undercounted %zu)\n",
                maxDelta, budget, recsPerPass, (maxDelta - recFixed) / recsPerPass, perReal, perRecord(b.blockRows - 1));
    EXPECT_LE(maxDelta, budget) << "every delta fits the ring max message";
    EXPECT_GT(maxDelta, recFixed + recsPerPass * perRecord(b.blockRows - 1))
        << "the stream really reaches the block term: a 2-block-row bound could not hold this pass";
    // The freed spare is an ERASE entry (header only, no row payload), so the
    // worst real record is 5/2/2 rows + 2 chunk upserts + 1 chunk erase: the
    // bound's third block row is a conservative byte over-count by one payload.
    EXPECT_EQ((maxDelta - recFixed) / recsPerPass, perRecord(b.blockRows - 1) + kCheckpointEntryHeaderBytes)
        << "full passes carry 5 contributor + 2 group + 2 view rows, 2 chunk puts and the spare's erase";

    const rune::view::RowLayout* L = nullptr;
    for (int64_t g = 0; g < kG; g += 13) {
        const uint8_t* s = vs.getRow(0x4000000C, 0, uint64_t(g), L);
        ASSERT_NE(s, nullptr) << "group " << g;
        EXPECT_EQ(rune::view::readSlot(s, *L, 1).i, 48) << "group " << g;
        EXPECT_EQ(rune::view::readSlot(s, *L, 2).i, 49) << "group " << g;
        EXPECT_EQ(rune::view::readSlot(s, *L, 3).i, 2) << "group " << g << ": newest remaining";
        const uint8_t* t = vs.getRow(0x4000000C, 0, uint64_t(kG + g), L);
        ASSERT_NE(t, nullptr) << "group " << kG + g;
        EXPECT_EQ(rune::view::readSlot(t, *L, 1).i, 2);
        EXPECT_EQ(rune::view::readSlot(t, *L, 2).i, 50);
        EXPECT_EQ(rune::view::readSlot(t, *L, 3).i, 50) << "the moved contributor is the newest";
    }
    const rune::node::ViewFoldCounters* fc = vs.foldCounters(0x4000000C, 0);
    ASSERT_NE(fc, nullptr);
    EXPECT_EQ(fc->aggInconsistent, 0u);
}
