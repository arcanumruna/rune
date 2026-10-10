// libs/node/tests/node/WorkerViewSetTest.cpp
//
// Task 15: WorkerViewSet — per-worker view-fold driver.  Deterministic,
// in-process: a fake committed-range readRange (no real Log, no Aeron, no
// clock) feeds foldBatch; the gate test proves a committed range folds into
// the in-memory table and getRow() serves the latest row.
#include <gtest/gtest.h>

#include <cstdint>
#include <cstring>
#include <string_view>
#include <vector>

#include <rune/codec/Descriptor.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/storage/LogReadView.h>
#include <rune/view/PlanCompiler.h>
#include <rune/view/ViewRow.h>

#include <rune/node/ViewSnapshotChannel.h>
#include <rune/node/WorkerViewSet.h>

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
    const uint8_t* row = vs.getRow(0x40000007, 7, L);
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
    const uint8_t* row = vs.getRow(0x40000007, 7, L);
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
