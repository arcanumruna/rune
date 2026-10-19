// tests/MixedVersionReadTest.cpp
//
// GC1: deep mixed-version-read lineages + drop-then-readd tag distinctness +
// rename-chain, across a realistic 5-version lineage built with applyOps:
//
//   v1: {id:int64(key, tag1), age:int32(tag2)}
//   v2: + score:int32(tag3), + name:string(tag4)
//   v3: DROP age(tag2); RENAME name->nickname (tag4 unchanged)         [one ALTER, two ops]
//   v4: RENAME nickname->fullName (tag4 unchanged)
//   v5: + age:int32  -- a FRESH tag (NOT tag2), distinct from v1's age
//
// For each version V a record is hand-framed (header [blockLength u16][version
// u16] + fixed block + var section) against materializeAsOf(evolving, V), the
// real per-version layout, exactly mirroring how a producer bound to vV would
// write it. Every test below then decodes records -- scanning mixes of
// versions -- using readField() by durable tag, never by name/position.
#include <gtest/gtest.h>
#include <algorithm>
#include <cstring>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>
#include <rune/codec/AlterOps.h>
#include <rune/codec/Descriptor.h>
#include <rune/codec/Evolve.h>
#include <rune/codec/Record.h>

using namespace rune::codec;

namespace {

void putU16(std::vector<uint8_t>& b, size_t off, uint16_t v) {
    b[off]     = uint8_t(v);
    b[off + 1] = uint8_t(v >> 8);
}
void putU32(std::vector<uint8_t>& b, size_t off, uint32_t v) {
    for (int k = 0; k < 4; ++k) b[off + k] = uint8_t(v >> (8 * k));
}
void putI64(std::vector<uint8_t>& b, size_t off, int64_t v) {
    uint64_t u = uint64_t(v);
    for (int k = 0; k < 8; ++k) b[off + k] = uint8_t(u >> (8 * k));
}
void putI32(std::vector<uint8_t>& b, size_t off, int32_t v) {
    uint32_t u = uint32_t(v);
    for (int k = 0; k < 4; ++k) b[off + k] = uint8_t(u >> (8 * k));
}

// Hand-frame one record stamped `version`, laid out exactly per `view` (==
// materializeAsOf(evolving, version)): header, then fixed-block values from
// `fixedVals` (by tag), then var-section values from `varVals` (by tag),
// emitted in the view's var-index order. Every tag passed MUST be live in
// `view` (mirrors a real encoder, which only ever writes a version's live
// fields) -- an omission or typo here throws rather than silently miswriting.
std::vector<uint8_t> encodeRecord(const Descriptor& view, uint16_t version,
                                   const std::vector<std::pair<uint16_t, int64_t>>& fixedVals,
                                   const std::vector<std::pair<uint16_t, std::string>>& varVals) {
    std::vector<uint8_t> rec(4 + view.blockLength, 0);
    putU16(rec, 0, uint16_t(view.blockLength));
    putU16(rec, 2, version);

    for (auto& [tag, val] : fixedVals) {
        const FieldDescriptor* fd = nullptr;
        for (auto& f : view.fields) {
            if (f.id == tag) { fd = &f; break; }
        }
        if (!fd) throw std::runtime_error("encodeRecord: fixed tag not live in view");
        size_t off = 4 + fd->offset;
        if (fd->type == PrimType::Int64)      putI64(rec, off, val);
        else if (fd->type == PrimType::Int32) putI32(rec, off, int32_t(val));
        else throw std::runtime_error("encodeRecord: unsupported fixed type in test helper");
    }

    std::vector<const FieldDescriptor*> varFields;
    for (auto& f : view.fields) {
        if (isVarLen(f.type)) varFields.push_back(&f);
    }
    std::sort(varFields.begin(), varFields.end(),
              [](const FieldDescriptor* a, const FieldDescriptor* b) { return a->offset < b->offset; });
    for (auto* fd : varFields) {
        const std::string* value = nullptr;
        for (auto& [tag, s] : varVals) {
            if (tag == fd->id) { value = &s; break; }
        }
        if (!value) throw std::runtime_error("encodeRecord: missing var value for live field");
        size_t base = rec.size();
        rec.resize(base + 4 + value->size());
        putU32(rec, base, uint32_t(value->size()));
        std::memcpy(rec.data() + base + 4, value->data(), value->size());
    }
    return rec;
}

struct Fixture {
    Descriptor evolving;            // cumulative lineage descriptor through v5
    Descriptor v1, v2, v3, v4, v5;  // materializeAsOf(evolving, K)
    std::vector<uint8_t> rec1, rec2, rec3, rec4, rec5;
    uint16_t tagId        = 0;
    uint16_t tagAgeOrig   = 0;  // v1's "age" durable tag
    uint16_t tagScore     = 0;
    uint16_t tagNameChain = 0;  // name -> nickname -> fullName, same tag throughout
    uint16_t tagAgeFresh  = 0;  // v5's re-added "age" durable tag (MUST differ from tagAgeOrig)
};

Fixture buildFixture() {
    Fixture fx;

    DescriptorBuilder b("lineage", 1);
    b.addTagged("id",  1, PrimType::Int64, 1, 0xFFFF, /*isKey=*/true,  {});
    b.addTagged("age", 2, PrimType::Int32, 1, 0xFFFF, /*isKey=*/false, {});
    Descriptor cur = b.build();
    uint16_t nextTag = 3;

    // v2: ADD score, ADD name
    cur = applyOps(cur,
                    {{AlterOpType::Add, "score", "", PrimType::Int32, 0, {}},
                     {AlterOpType::Add, "name",  "", PrimType::String, 0, {}}},
                    nextTag, 2)
              .value();

    // v3: one ALTER statement, two ops -- DROP age, RENAME name->nickname.
    cur = applyOps(cur,
                    {{AlterOpType::Drop,   "age",  "",         {}, 0, {}},
                     {AlterOpType::Rename, "name", "nickname", {}, 0, {}}},
                    nextTag, 3)
              .value();

    // v4: RENAME nickname->fullName (second hop of the rename chain).
    cur = applyOps(cur, {{AlterOpType::Rename, "nickname", "fullName", {}, 0, {}}}, nextTag, 4)
              .value();

    // v5: re-ADD age. The name "age" was freed by the v3 DROP, so this is a
    // brand-new field with a FRESH tag -- NOT tag2.
    cur = applyOps(cur, {{AlterOpType::Add, "age", "", PrimType::Int32, 0, {}}}, nextTag, 5)
              .value();

    fx.evolving = cur;
    fx.v1 = materializeAsOf(cur, 1);
    fx.v2 = materializeAsOf(cur, 2);
    fx.v3 = materializeAsOf(cur, 3);
    fx.v4 = materializeAsOf(cur, 4);
    fx.v5 = materializeAsOf(cur, 5);

    fx.tagId        = fx.v1.find("id")->id;
    fx.tagAgeOrig   = fx.v1.find("age")->id;
    fx.tagScore     = fx.v2.find("score")->id;
    // NOTE: RENAME mutates the field's name IN PLACE on the cumulative
    // `evolving` descriptor -- it does not keep a per-version history of
    // names. So every materializeAsOf() snapshot of this field (v2..v5)
    // carries today's CURRENT name, "fullName", never the historical
    // "name"/"nickname" spellings -- find() by an old name on any view
    // returns nullptr. This is exactly the "reader's current name" contract
    // the rename-chain assertion is about: look the tag up once by its
    // current name, then read every record (old and new) by that tag.
    fx.tagNameChain = fx.v5.find("fullName")->id;
    fx.tagAgeFresh  = fx.v5.find("age")->id;

    // v1: id=1001, age=30
    fx.rec1 = encodeRecord(fx.v1, 1,
                            {{fx.tagId, 1001}, {fx.tagAgeOrig, 30}}, {});
    // v2: id=1002, age=31, score=500, name="Alice"
    fx.rec2 = encodeRecord(fx.v2, 2,
                            {{fx.tagId, 1002}, {fx.tagAgeOrig, 31}, {fx.tagScore, 500}},
                            {{fx.tagNameChain, "Alice"}});
    // v3: id=1003, score=501, nickname="Bob" (age dropped)
    fx.rec3 = encodeRecord(fx.v3, 3,
                            {{fx.tagId, 1003}, {fx.tagScore, 501}},
                            {{fx.tagNameChain, "Bob"}});
    // v4: id=1004, score=502, fullName="Carol"
    fx.rec4 = encodeRecord(fx.v4, 4,
                            {{fx.tagId, 1004}, {fx.tagScore, 502}},
                            {{fx.tagNameChain, "Carol"}});
    // v5: id=1005, score=503, fullName="Dave", age(fresh)=99
    fx.rec5 = encodeRecord(fx.v5, 5,
                            {{fx.tagId, 1005}, {fx.tagScore, 503}, {fx.tagAgeFresh, 99}},
                            {{fx.tagNameChain, "Dave"}});
    return fx;
}

} // namespace

// ---------------------------------------------------------------------------
// Lineage sanity: tags are durable, distinct, and the rename chain keeps one.
// ---------------------------------------------------------------------------
TEST(MixedVersionRead, LineageTagsAreStableAndDistinct) {
    Fixture fx = buildFixture();
    EXPECT_NE(fx.tagAgeOrig, fx.tagAgeFresh)
        << "v1's age and v5's re-added age must be different durable tags";
    // Every snapshot shares the cumulative descriptor's one current name
    // ("fullName") for this field -- see the comment in buildFixture().
    EXPECT_EQ(fx.v2.find("fullName")->id, fx.tagNameChain);
    EXPECT_EQ(fx.v3.find("fullName")->id, fx.tagNameChain);
    EXPECT_EQ(fx.v4.find("fullName")->id, fx.tagNameChain);
    EXPECT_EQ(fx.v5.find("fullName")->id, fx.tagNameChain);
    // Retired tag2 is gone from v5's view entirely -- not merely absent-by-version.
    EXPECT_EQ(fx.v5.find("age")->id, fx.tagAgeFresh);
}

// ---------------------------------------------------------------------------
// Required assertion 1: drop-then-readd tag distinctness.
// ---------------------------------------------------------------------------
TEST(MixedVersionRead, DropThenReaddNeverCrossReadsBetweenV1AndV5Age) {
    Fixture fx = buildFixture();

    // v1 record's age reads via the v1 (original) tag with the v1 value.
    auto v1Age = readField(fx.v1, fx.rec1, fx.tagAgeOrig);
    EXPECT_TRUE(v1Age.present);
    EXPECT_EQ(v1Age.i, 30);

    // v5 record's age reads via the v5 (fresh) tag with the v5 value.
    auto v5Age = readField(fx.v5, fx.rec5, fx.tagAgeFresh);
    EXPECT_TRUE(v5Age.present);
    EXPECT_EQ(v5Age.i, 99);

    // Decoding a mixed {v1,v5} set never cross-reads one tag's value into the
    // other: the v1 record has no v5 tag, and v5's view has no v1 tag at all.
    EXPECT_FALSE(readField(fx.v1, fx.rec1, fx.tagAgeFresh).present);
    EXPECT_FALSE(readField(fx.v5, fx.rec5, fx.tagAgeOrig).present);
}

// ---------------------------------------------------------------------------
// Required assertion 2: rename-chain keeps the SAME tag; records from before
// and after the rename decode under the reader's CURRENT name.
// ---------------------------------------------------------------------------
TEST(MixedVersionRead, RenameChainResolvesSameTagUnderCurrentName) {
    Fixture fx = buildFixture();

    // Same tag (fx.tagNameChain) resolves to the correct per-record value at
    // every stop along the chain -- name(v2)="Alice", nickname(v3)="Bob",
    // fullName(v4)="Carol", fullName(v5)="Dave". (Each view's find() can only
    // look the field up by its CURRENT name, "fullName" -- see the comment in
    // buildFixture() -- so the lookups below all key off the stable tag.)
    EXPECT_EQ(readField(fx.v2, fx.rec2, fx.tagNameChain).bytes, "Alice");
    EXPECT_EQ(readField(fx.v3, fx.rec3, fx.tagNameChain).bytes, "Bob");
    EXPECT_EQ(readField(fx.v4, fx.rec4, fx.tagNameChain).bytes, "Carol");
    EXPECT_EQ(readField(fx.v5, fx.rec5, fx.tagNameChain).bytes, "Dave");

    // The var-data section is framed from each record's OWN header
    // blockLength (self-describing), and this lineage never has more than
    // one live var field at a time, so the CURRENT reader (v5, which only
    // knows the name "fullName") can decode the tag out of every record
    // across the whole rename chain, old and new alike -- a consistent
    // logical field under the reader's current name.
    EXPECT_FALSE(readField(fx.v5, fx.rec1, fx.tagNameChain).present);  // v1 predates the field entirely
    EXPECT_EQ(readField(fx.v5, fx.rec2, fx.tagNameChain).bytes, "Alice");
    EXPECT_EQ(readField(fx.v5, fx.rec3, fx.tagNameChain).bytes, "Bob");
    EXPECT_EQ(readField(fx.v5, fx.rec4, fx.tagNameChain).bytes, "Carol");
    EXPECT_EQ(readField(fx.v5, fx.rec5, fx.tagNameChain).bytes, "Dave");
}

// ---------------------------------------------------------------------------
// Required assertion 3: dropped field (age, tag2) -- present at v1/v2,
// absent at v3/v4/v5 -- correct present/absent/value in one scan over
// {v1..v5}, each record decoded via its OWN matching version's view.
// ---------------------------------------------------------------------------
TEST(MixedVersionRead, DroppedFieldPresentAbsentAcrossSingleScan) {
    Fixture fx = buildFixture();

    struct Row { const Descriptor* view; const std::vector<uint8_t>* rec; bool present; int64_t value; };
    std::vector<Row> rows = {
        {&fx.v1, &fx.rec1, true,  30},
        {&fx.v2, &fx.rec2, true,  31},
        {&fx.v3, &fx.rec3, false, 0},
        {&fx.v4, &fx.rec4, false, 0},
        {&fx.v5, &fx.rec5, false, 0},
    };
    for (size_t i = 0; i < rows.size(); ++i) {
        SCOPED_TRACE(testing::Message() << "row v" << (i + 1));
        auto fv = readField(*rows[i].view, *rows[i].rec, fx.tagAgeOrig);
        EXPECT_EQ(fv.present, rows[i].present);
        if (rows[i].present) EXPECT_EQ(fv.i, rows[i].value);
    }
}

// ---------------------------------------------------------------------------
// Required assertion 4 (forward): a descriptor materialized at vK reading a
// v(K+n) record ignores tags added after K (absent); a tag whose offset is
// stable across the span (never shifted by an intervening DROP) still
// decodes correctly even though the reader predates the record.
// ---------------------------------------------------------------------------
TEST(MixedVersionRead, ForwardReadOlderReaderIgnoresTagsAddedLater) {
    Fixture fx = buildFixture();

    // Reader@v1 (knows only id, age) reads a v2 record: id and age (both
    // live continuously from v1 into v2, nothing dropped in between, so
    // their fixed offsets are unchanged) decode correctly; score (added at
    // v2) isn't even in the v1 view -- absent.
    EXPECT_EQ(readField(fx.v1, fx.rec2, fx.tagId).i, 1002);
    EXPECT_EQ(readField(fx.v1, fx.rec2, fx.tagAgeOrig).i, 31);
    EXPECT_FALSE(readField(fx.v1, fx.rec2, fx.tagScore).present);

    // Reader@v2 reads a v5 record: id is stable at offset 0 across the whole
    // lineage (nothing is ever inserted or dropped ahead of it) and decodes
    // correctly; the fresh age tag (added at v5) isn't in the v2 view at
    // all -- absent. (score is deliberately NOT exercised here: it shifts
    // offset at v3's DROP of age, so a v2-bound reader against a v5 record
    // is outside the documented contract -- see Evolve.h's materializeAsOf
    // comment, which pairs a view with ITS OWN record version.)
    EXPECT_EQ(readField(fx.v2, fx.rec5, fx.tagId).i, 1005);
    EXPECT_FALSE(readField(fx.v2, fx.rec5, fx.tagAgeFresh).present);
}

// ---------------------------------------------------------------------------
// Required assertion 4 (backward): a descriptor materialized at vK reading a
// v(K-n) record still decodes the fields that existed at the record's
// version; fields added after the record's version correctly gate absent via
// readField's sinceVersion check (not merely "tag not found").
// ---------------------------------------------------------------------------
TEST(MixedVersionRead, BackwardReadNewerReaderDecodesFieldsLiveAtRecordVersion) {
    Fixture fx = buildFixture();

    // Reader@v2 reads a v1 record: id, age existed at v1 -> decode correctly.
    // score and name both have sinceVersion=2 > the record's version(1), so
    // readField's sinceVersion gate reports them absent even though the v2
    // view itself contains those tags.
    EXPECT_EQ(readField(fx.v2, fx.rec1, fx.tagId).i, 1001);
    EXPECT_EQ(readField(fx.v2, fx.rec1, fx.tagAgeOrig).i, 30);
    EXPECT_FALSE(readField(fx.v2, fx.rec1, fx.tagScore).present);
    EXPECT_FALSE(readField(fx.v2, fx.rec1, fx.tagNameChain).present);

    // Reader@v5 (a long jump back) reads the same v1 record: id still
    // decodes (its offset never moved); score/fullName/age-fresh are all
    // gated absent by sinceVersion > 1.
    EXPECT_EQ(readField(fx.v5, fx.rec1, fx.tagId).i, 1001);
    EXPECT_FALSE(readField(fx.v5, fx.rec1, fx.tagScore).present);
    EXPECT_FALSE(readField(fx.v5, fx.rec1, fx.tagNameChain).present);
    EXPECT_FALSE(readField(fx.v5, fx.rec1, fx.tagAgeFresh).present);
}

// ---------------------------------------------------------------------------
// Consolidated scan: all five versions, fed to the reader OUT of chronological
// order, each decoded via its OWN matching materialized view -- the realistic
// "consumer reading a mixed log" shape. Confirms id/age/score/name-chain are
// each correct, record by record, regardless of scan order.
// ---------------------------------------------------------------------------
TEST(MixedVersionRead, FullMixedScanAllFiveVersionsOutOfOrder) {
    Fixture fx = buildFixture();

    struct Expect {
        const Descriptor* view;
        const std::vector<uint8_t>* rec;
        uint16_t version;
        int64_t id;
        bool ageOrigPresent; int64_t ageOrig;
        bool scorePresent;   int64_t score;
        bool ageFreshPresent; int64_t ageFresh;
        std::string nameChain;  // empty => absent
    };
    std::vector<Expect> scan = {
        {&fx.v3, &fx.rec3, 3, 1003, false, 0, true, 501, false, 0, "Bob"},
        {&fx.v1, &fx.rec1, 1, 1001, true,  30, false, 0, false, 0, ""},
        {&fx.v5, &fx.rec5, 5, 1005, false, 0, true, 503, true, 99, "Dave"},
        {&fx.v2, &fx.rec2, 2, 1002, true,  31, true, 500, false, 0, "Alice"},
        {&fx.v4, &fx.rec4, 4, 1004, false, 0, true, 502, false, 0, "Carol"},
    };

    for (auto& e : scan) {
        SCOPED_TRACE(testing::Message() << "record version=" << e.version);
        EXPECT_EQ(readField(*e.view, *e.rec, fx.tagId).i, e.id);

        auto age = readField(*e.view, *e.rec, fx.tagAgeOrig);
        EXPECT_EQ(age.present, e.ageOrigPresent);
        if (e.ageOrigPresent) EXPECT_EQ(age.i, e.ageOrig);

        auto score = readField(*e.view, *e.rec, fx.tagScore);
        EXPECT_EQ(score.present, e.scorePresent);
        if (e.scorePresent) EXPECT_EQ(score.i, e.score);

        auto ageFresh = readField(*e.view, *e.rec, fx.tagAgeFresh);
        EXPECT_EQ(ageFresh.present, e.ageFreshPresent);
        if (e.ageFreshPresent) EXPECT_EQ(ageFresh.i, e.ageFresh);

        auto nameChain = readField(*e.view, *e.rec, fx.tagNameChain);
        if (e.nameChain.empty()) {
            EXPECT_FALSE(nameChain.present);
        } else {
            EXPECT_TRUE(nameChain.present);
            EXPECT_EQ(nameChain.bytes, e.nameChain);
        }
    }
}
