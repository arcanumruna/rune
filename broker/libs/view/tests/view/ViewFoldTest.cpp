// libs/view/tests/view/ViewFoldTest.cpp
#include <gtest/gtest.h>
#include <cstring>

#include <rune/codec/Descriptor.h>
#include <rune/codec/Record.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/view/PlanCompiler.h>
#include <rune/view/ViewFold.h>

namespace {

// --- Test fixtures -----------------------------------------------------

// v1 "character" source: {characterId: Int64 key (tag 0), hp: Int32 (tag 1)}.
rune::codec::Descriptor charSourceV1() {
    return rune::codec::DescriptorBuilder("character", 1)
        .add("characterId", rune::codec::PrimType::Int64, 1, /*isKey*/ true)
        .add("hp", rune::codec::PrimType::Int32)
        .build();
}

// Evolving "character" source whose current version is 2: characterId
// (tag 0) stays live forever; hp (tag 1) is live only for v1 (untilVersion=2
// -- DROPPED at v2). Built with explicit tags so untilVersion can be set
// (DescriptorBuilder::add() always leaves untilVersion at 0xFFFF).
rune::codec::Descriptor charEvolvingDropsHpAtV2() {
    return rune::codec::DescriptorBuilder("character", 2)
        .addTagged("characterId", 0, rune::codec::PrimType::Int64,
                   /*since*/ 1, /*until*/ 0xFFFF, /*isKey*/ true, {})
        .addTagged("hp", 1, rune::codec::PrimType::Int32,
                   /*since*/ 1, /*until*/ 2, /*isKey*/ false, {})
        .build();
}

// The view plan: char_hp(characterId, hp) <- source 0 "characterId"/"hp".
rune::view::ViewPlan hpView() {
    rune::view::ViewPlan v;
    v.viewName = "char_hp";
    v.key      = "characterId";
    v.sources  = {{5, "characterId"}};
    v.output   = {{"characterId", rune::codec::PrimType::Int64, 0, "characterId",
                  rune::view::MergeRule::Replace},
                {"hp", rune::codec::PrimType::Int32, 0, "hp", rune::view::MergeRule::Replace}};
    return v;
}

// --- Test-only encode/batch helpers --------------------------------------

// 8-byte little-endian encoding of an Int64 key -- the wire ROUTING key a
// view-source producer stamps for an integer-keyed topic (see ViewFold.cpp's
// decodeWireKey doc: this is what a tombstone's key is decoded from, since a
// tombstone's VALUE is empty).
std::vector<uint8_t> keyBytes(int64_t id) {
    std::vector<uint8_t> b(8);
    for (int i = 0; i < 8; ++i) b[static_cast<size_t>(i)] = static_cast<uint8_t>(uint64_t(id) >> (8 * i));
    return b;
}

std::string_view asSv(const std::vector<uint8_t>& b) {
    return std::string_view(reinterpret_cast<const char*>(b.data()), b.size());
}

// Hand-frames a codec record VALUE: [blockLength u16][version u16][id i64][hp i32]
// -- a v1 "character" record with both fields live.
std::vector<uint8_t> encodeCharacter(int64_t id, int32_t hp, uint16_t version) {
    std::vector<uint8_t> b;
    auto le16 = [&](uint16_t v) { b.push_back(uint8_t(v)); b.push_back(uint8_t(v >> 8)); };
    auto le32 = [&](int32_t v) { for (int i = 0; i < 4; ++i) b.push_back(uint8_t(uint32_t(v) >> (8 * i))); };
    auto le64 = [&](int64_t v) { for (int i = 0; i < 8; ++i) b.push_back(uint8_t(uint64_t(v) >> (8 * i))); };
    const uint16_t block = 8 + 4; // id(8) + hp(4)
    le16(block);
    le16(version);
    le64(id);
    le32(hp);
    return b;
}

// Hand-frames a v2 "character" record where hp has been DROPPED: just
// [blockLength=8][version=2][id i64].
std::vector<uint8_t> encodeCharacterV2DroppedHp(int64_t id) {
    std::vector<uint8_t> b;
    auto le16 = [&](uint16_t v) { b.push_back(uint8_t(v)); b.push_back(uint8_t(v >> 8)); };
    auto le64 = [&](int64_t v) { for (int i = 0; i < 8; ++i) b.push_back(uint8_t(uint64_t(v) >> (8 * i))); };
    le16(8);
    le16(2);
    le64(id);
    return b;
}

// Builds a single-record RecordBatch (valid, CRC-stamped) via
// RecordBatchBuilder. `value` empty => a tombstone (per RecordBatch.h's own
// contract: "a tombstone is value.empty()"); `tombstone` is accepted for
// documentation symmetry with the task brief but is not separately checked.
std::vector<uint8_t> makeBatch(int64_t baseOffset, std::string_view key,
                                std::string_view value, bool /*tombstone*/) {
    rune::protocol::RecordBatchBuilder b(baseOffset, /*baseTimestamp*/ 0);
    b.append(key, value, /*timestamp*/ 0);
    return b.build();
}

} // namespace

// ---------------------------------------------------------------------------

TEST(ViewFold, UpsertAndTombstone) {
    auto src = charSourceV1();
    const rune::codec::Descriptor* srcs[] = {&src};
    auto comp = rune::view::compileView(hpView(), srcs, 0x40000007).value();

    rune::view::ViewTable t(comp.rowLayout);
    rune::view::VersionDescriptorCache vdc(src);

    const auto kb = keyBytes(1001);

    // record 0: id=1001 hp=50
    const auto val0 = encodeCharacter(1001, 50, 1);
    const auto b0 = makeBatch(0, asSv(kb), asSv(val0), /*tomb*/ false);
    rune::storage::LogReadView v0{b0.data(), b0.size(), 0, 1};
    int64_t cur = 0;
    auto s0 = rune::view::foldBatch(comp.plan, comp.rowLayout, vdc, t, v0, /*bound*/ 1, cur);
    EXPECT_EQ(s0.applied, 1u);
    ASSERT_NE(t.get(1001), nullptr);
    EXPECT_EQ(rune::view::readSlot(t.get(1001), comp.rowLayout, 1).i, 50);
    EXPECT_EQ(cur, 1);

    // record 1: tombstone for id=1001
    const auto b1 = makeBatch(1, asSv(kb), std::string_view{}, /*tomb*/ true);
    rune::storage::LogReadView v1{b1.data(), b1.size(), 1, 2};
    auto s1 = rune::view::foldBatch(comp.plan, comp.rowLayout, vdc, t, v1, 2, cur);
    EXPECT_EQ(s1.deleted, 1u);
    EXPECT_EQ(t.get(1001), nullptr);
    EXPECT_EQ(cur, 2);
}

// Review-focus #1: a v2 record that dropped `hp` must leave the view's hp
// column NULL, not garbage and not a stale carry-over.
TEST(ViewFoldDroppedFieldRegression, FoldDroppedSourceFieldGoesNull) {
    auto src = charEvolvingDropsHpAtV2(); // v1 has {id,hp}; v2 drops hp (untilVersion=2)
    const rune::codec::Descriptor* srcs[] = {&src};
    auto comp = rune::view::compileView(hpView(), srcs, 1).value();
    const uint32_t rowLenBefore = comp.rowLayout.rowLen;

    rune::view::ViewTable t(comp.rowLayout);
    rune::view::VersionDescriptorCache vdc(src);

    const auto kb = keyBytes(1001);
    const auto val = encodeCharacterV2DroppedHp(1001);
    const auto b = makeBatch(0, asSv(kb), asSv(val), /*tomb*/ false);
    rune::storage::LogReadView v{b.data(), b.size(), 0, 1};
    int64_t cur = 0;
    auto stats = rune::view::foldBatch(comp.plan, comp.rowLayout, vdc, t, v, 1, cur);

    EXPECT_EQ(stats.applied, 1u);
    ASSERT_NE(t.get(1001), nullptr);
    EXPECT_TRUE(rune::view::isNull(t.get(1001), comp.rowLayout, 1)); // hp column null
    EXPECT_EQ(comp.rowLayout.rowLen, rowLenBefore);                  // rowLen unchanged
    // The key column itself must still be live and correct.
    EXPECT_FALSE(rune::view::isNull(t.get(1001), comp.rowLayout, 0));
    EXPECT_EQ(rune::view::readSlot(t.get(1001), comp.rowLayout, 0).i, 1001);
}

// Determinism (#16): the same committed prefix folds to a byte-identical
// view table whether the 3 records arrive as one 3-record batch or as 3
// separate 1-record batches walked within the same foldBatch() call.
TEST(ViewFold, DeterministicAcrossBatchBoundaries) {
    auto src = charSourceV1();
    const rune::codec::Descriptor* srcs[] = {&src};
    auto comp = rune::view::compileView(hpView(), srcs, 0x40000007).value();

    // Case A: one batch containing all 3 records.
    rune::protocol::RecordBatchBuilder bA(0, 0);
    std::vector<std::vector<uint8_t>> keepAliveA; // keys/values must outlive append()
    for (int64_t id = 1; id <= 3; ++id) {
        auto k = keyBytes(id);
        auto val = encodeCharacter(id, static_cast<int32_t>(id * 10), 1);
        bA.append(asSv(k), asSv(val), 0);
        keepAliveA.push_back(std::move(k));
        keepAliveA.push_back(std::move(val));
    }
    const auto dataA = bA.build();
    rune::storage::LogReadView vA{dataA.data(), dataA.size(), 0, 3};
    rune::view::ViewTable tableA(comp.rowLayout);
    rune::view::VersionDescriptorCache vdcA(src);
    int64_t curA = 0;
    auto statsA = rune::view::foldBatch(comp.plan, comp.rowLayout, vdcA, tableA, vA, 3, curA);

    // Case B: 3 separate single-record batches, concatenated into one view.
    std::vector<uint8_t> dataB;
    for (int64_t id = 1; id <= 3; ++id) {
        rune::protocol::RecordBatchBuilder bB(id - 1, 0);
        auto k = keyBytes(id);
        auto val = encodeCharacter(id, static_cast<int32_t>(id * 10), 1);
        bB.append(asSv(k), asSv(val), 0);
        auto one = bB.build();
        dataB.insert(dataB.end(), one.begin(), one.end());
    }
    rune::storage::LogReadView vB{dataB.data(), dataB.size(), 0, 3};
    rune::view::ViewTable tableB(comp.rowLayout);
    rune::view::VersionDescriptorCache vdcB(src);
    int64_t curB = 0;
    auto statsB = rune::view::foldBatch(comp.plan, comp.rowLayout, vdcB, tableB, vB, 3, curB);

    EXPECT_EQ(statsA.applied, statsB.applied);
    EXPECT_EQ(curA, curB);
    for (int64_t id = 1; id <= 3; ++id) {
        const uint8_t* rowA = tableA.get(id);
        const uint8_t* rowB = tableB.get(id);
        ASSERT_NE(rowA, nullptr);
        ASSERT_NE(rowB, nullptr);
        EXPECT_EQ(std::memcmp(rowA, rowB, comp.rowLayout.rowLen), 0);
    }
}

// Post-review ruling: the merge/table key MUST be the record's WIRE
// (compaction) key, never the value's codec key-field -- those are two
// different key spaces that only agree under an unenforced producer
// contract. This proves the unification: an upsert whose VALUE key-field
// (9999) deliberately diverges from its WIRE key (2002) is still addressed
// -- and later erased -- by the WIRE key. Pre-fix (value-field key for
// upsert, wire key for tombstone), the upsert would have landed at row 9999
// instead of 2002, so `t.get(2002)` would already be null right after the
// upsert -- this test fails loudly under the old code instead of silently
// leaking a row.
TEST(ViewFoldKeyUnificationRegression, TombstoneErasesByWireKeyEvenWhenValueKeyFieldDiffers) {
    auto src = charSourceV1();
    const rune::codec::Descriptor* srcs[] = {&src};
    auto comp = rune::view::compileView(hpView(), srcs, 0x40000007).value();

    rune::view::ViewTable t(comp.rowLayout);
    rune::view::VersionDescriptorCache vdc(src);

    // Wire (compaction) key = 2002; the VALUE's key-field claims a DIFFERENT
    // id (9999) -- this divergence must NOT matter for merge identity.
    const auto wireKey = keyBytes(2002);
    const auto val = encodeCharacter(/*value key-field*/ 9999, /*hp*/ 77, 1);
    const auto b0 = makeBatch(0, asSv(wireKey), asSv(val), /*tomb*/ false);
    rune::storage::LogReadView v0{b0.data(), b0.size(), 0, 1};
    int64_t cur = 0;
    auto s0 = rune::view::foldBatch(comp.plan, comp.rowLayout, vdc, t, v0, 1, cur);
    EXPECT_EQ(s0.applied, 1u);

    // The row must be addressable by the WIRE key -- not by the value's
    // key-field, which is only a display/projection column.
    ASSERT_NE(t.get(2002), nullptr) << "row must be keyed by the wire/compaction key";
    EXPECT_EQ(rune::view::readSlot(t.get(2002), comp.rowLayout, 1).i, 77);
    // Its key OUTPUT column still reflects the value's key-field (a plain
    // projection), even though it differs from the merge key.
    EXPECT_EQ(rune::view::readSlot(t.get(2002), comp.rowLayout, 0).i, 9999);
    EXPECT_EQ(t.get(9999), nullptr) << "must not have created a second row at the value key-field";

    // A tombstone keyed by the SAME wire key must erase exactly that row.
    const auto b1 = makeBatch(1, asSv(wireKey), std::string_view{}, /*tomb*/ true);
    rune::storage::LogReadView v1{b1.data(), b1.size(), 1, 2};
    auto s1 = rune::view::foldBatch(comp.plan, comp.rowLayout, vdc, t, v1, 2, cur);
    EXPECT_EQ(s1.deleted, 1u);
    EXPECT_EQ(t.get(2002), nullptr) << "tombstone keyed by the wire key must erase the row it keys";
    EXPECT_EQ(t.get(9999), nullptr);
}

// A wire key that is not exactly the view key column's byte width (here: 4
// bytes for an Int64 key, which needs 8) must be rejected -- never silently
// zero-padded, which could otherwise mis-key a row under a truncated/corrupt
// producer-side key.
TEST(ViewFoldMalformedKeyRegression, WrongWidthWireKeySkippedNotZeroPadded) {
    auto src = charSourceV1();
    const rune::codec::Descriptor* srcs[] = {&src};
    auto comp = rune::view::compileView(hpView(), srcs, 0x40000007).value();

    rune::view::ViewTable t(comp.rowLayout);
    rune::view::VersionDescriptorCache vdc(src);

    const std::vector<uint8_t> shortKey(4, 0x07); // Int64 key needs 8 bytes, not 4
    const auto val = encodeCharacter(1001, 50, 1);
    const auto b = makeBatch(0, std::string_view(reinterpret_cast<const char*>(shortKey.data()), shortKey.size()),
                              asSv(val), /*tomb*/ false);
    rune::storage::LogReadView v{b.data(), b.size(), 0, 1};
    int64_t cur = 0;
    auto stats = rune::view::foldBatch(comp.plan, comp.rowLayout, vdc, t, v, 1, cur);

    EXPECT_EQ(stats.skippedMalformed, 1u);
    EXPECT_EQ(stats.applied, 0u);
    EXPECT_EQ(t.size(), 0u);
}

// The LogReadView contract is WHOLE batches only. If the walk breaks early
// because a later batch is torn/corrupt (fails ensureValid()), the cursor
// must stop at the last FULLY folded batch -- never jump ahead to
// min(view.nextOffset, bound), which would silently skip the unfolded
// (corrupt) batch's records on the next fold.
TEST(ViewFoldCorruptBreakRegression, CursorNeverAdvancesPastAnUnfoldedCorruptBatch) {
    auto src = charSourceV1();
    const rune::codec::Descriptor* srcs[] = {&src};
    auto comp = rune::view::compileView(hpView(), srcs, 0x40000007).value();

    rune::view::ViewTable t(comp.rowLayout);
    rune::view::VersionDescriptorCache vdc(src);

    // Batch 0: one valid record at offset 0.
    const auto kb0 = keyBytes(1001);
    const auto val0 = encodeCharacter(1001, 50, 1);
    auto batch0 = makeBatch(0, asSv(kb0), asSv(val0), /*tomb*/ false);

    // Batch 1: an otherwise-valid batch whose bytes are corrupted (flipping
    // the last byte breaks only the stored CRC, not the size/length checks)
    // -- simulates a torn/corrupt tail the reader must stop at.
    const auto kb1 = keyBytes(1002);
    const auto val1 = encodeCharacter(1002, 60, 1);
    auto batch1 = makeBatch(1, asSv(kb1), asSv(val1), /*tomb*/ false);
    batch1.back() ^= 0xFF;

    std::vector<uint8_t> data;
    data.insert(data.end(), batch0.begin(), batch0.end());
    data.insert(data.end(), batch1.begin(), batch1.end());

    // The view claims committed data through offset 5, as if a caller's LSO
    // tracker believed more batches existed past the corrupt one -- the bug
    // this guards against is trusting that claim and skipping straight to it.
    rune::storage::LogReadView v{data.data(), data.size(), 0, 5};
    int64_t cur = 0;
    auto stats = rune::view::foldBatch(comp.plan, comp.rowLayout, vdc, t, v, /*bound*/ 5, cur);

    EXPECT_EQ(stats.applied, 1u);     // only batch 0's record folded
    ASSERT_NE(t.get(1001), nullptr);
    EXPECT_EQ(t.get(1002), nullptr);  // batch 1 never folded (corrupt)
    EXPECT_EQ(cur, 1) << "cursor must stop at the last FULLY folded batch, "
                          "not jump to min(nextOffset, bound)";
}
