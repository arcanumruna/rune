// tests/DecodeBoundaryMatrixTest.cpp
//
// GC2: decode boundary matrix (test-matrix spec scenarios J2, J4-J18, J37,
// J41-46). A single 7-version lineage, built directly via addTagged /
// addFixedStrTagged (the per-field sinceVersion/untilVersion knobs), is
// materialized at every version 1..7 and a record is hand-framed against
// each materialized view -- exactly mirroring how a producer bound to that
// version would write it. Every assertion below decodes via readField() by
// durable tag against the record's OWN matching materialized view, per the
// documented materializeAsOf() contract.
//
// Lineage (declaration order; tag = durable id):
//   id            Int64     tag1  since=1 until=0xFFFF  (key)   -- anchor
//   lateAdd       Int32     tag2  since=4 until=0xFFFF          -- sinceVersion boundary (K=4)
//   earlyDrop     Int32     tag3  since=1 until=6               -- untilVersion boundary (M=6)
//   neighborBefore Int32    tag4  since=1 until=3               -- untilVersion boundary (M=3);
//                                                                    FixedStr "code" neighbour
//   code          FixedStr(6) tag5 since=1 until=0xFFFF         -- width constant, offset shifts
//   neighborAfter Int32     tag6  since=5 until=0xFFFF          -- FixedStr "code" neighbour
//   status        Enum      tag7  since=2 until=5  {1,2,3}      -- enum whitelist, version-scoped
//   varA          String    tag8  since=1 until=5               -- var-field drop
//   varB          String    tag9  since=3 until=0xFFFF          -- var-index reassigned when varA drops
//
// Per-version live fixed-block layout (byte offset, width constant throughout
// per field) and live var-index assignment -- computed by computeOffsets()
// from the live set alone, which is exactly what this file verifies:
//
//   v1: id@0(8) earlyDrop@8(4) neighborBefore@12(4) code@16(6)                      | varA@0
//   v2: id@0(8) earlyDrop@8(4) neighborBefore@12(4) code@16(6) status@22(4)         | varA@0
//   v3: id@0(8) earlyDrop@8(4) code@12(6) status@18(4)                             | varA@0 varB@1
//   v4: id@0(8) lateAdd@8(4) earlyDrop@12(4) code@16(6) status@22(4)                | varA@0 varB@1
//   v5: id@0(8) lateAdd@8(4) earlyDrop@12(4) code@16(6) neighborAfter@22(4)         | varB@0 (varA dropped)
//   v6: id@0(8) lateAdd@8(4) code@12(6) neighborAfter@18(4)                        | varB@0 (earlyDrop dropped)
//   v7: same shape as v6                                                           | varB@0
#include <gtest/gtest.h>
#include <algorithm>
#include <cstring>
#include <string>
#include <stdexcept>
#include <vector>
#include <rune/codec/Descriptor.h>
#include <rune/codec/Evolve.h>
#include <rune/codec/Record.h>
#include <rune/codec/Validator.h>
#include <rune/codec/AlterOps.h>

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

const FieldDescriptor* findByTag(const Descriptor& d, uint16_t tag) {
    for (const auto& f : d.fields) {
        if (f.id == tag) return &f;
    }
    return nullptr;
}

// Hand-frame one record stamped `version`, laid out per `view` (==
// materializeAsOf(evolving, version)): header, fixed block (ints + FixedStr
// blobs by tag), then var section (by tag, emitted in the view's var-index
// order). Mirrors tests/MixedVersionReadTest.cpp's encodeRecord, extended
// with FixedStr and Enum support for this boundary matrix.
std::vector<uint8_t> encodeRecord(
    const Descriptor& view, uint16_t version,
    const std::vector<std::pair<uint16_t, int64_t>>& fixedInts,
    const std::vector<std::pair<uint16_t, std::string>>& fixedStrs,
    const std::vector<std::pair<uint16_t, std::string>>& varVals) {
    std::vector<uint8_t> rec(4 + view.blockLength, 0);
    putU16(rec, 0, uint16_t(view.blockLength));
    putU16(rec, 2, version);

    for (auto& [tag, val] : fixedInts) {
        const FieldDescriptor* fd = findByTag(view, tag);
        if (!fd) throw std::runtime_error("encodeRecord: fixed-int tag not live in view");
        size_t off = 4 + fd->offset;
        if (fd->type == PrimType::Int64) putI64(rec, off, val);
        else if (fd->type == PrimType::Int32 || fd->type == PrimType::Enum)
            putI32(rec, off, int32_t(val));
        else throw std::runtime_error("encodeRecord: unsupported fixed-int type in test helper");
    }

    for (auto& [tag, val] : fixedStrs) {
        const FieldDescriptor* fd = findByTag(view, tag);
        if (!fd) throw std::runtime_error("encodeRecord: FixedStr tag not live in view");
        if (fd->type != PrimType::FixedStr)
            throw std::runtime_error("encodeRecord: tag is not FixedStr");
        if (val.size() > fd->fixedLen)
            throw std::runtime_error("encodeRecord: FixedStr value wider than fixedLen");
        size_t off = 4 + fd->offset;
        std::memcpy(rec.data() + off, val.data(), val.size());
        // remaining bytes (if val shorter than fixedLen) stay zero-filled.
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
    Descriptor evolving;
    Descriptor v1, v2, v3, v4, v5, v6, v7;
    std::vector<uint8_t> rec1, rec2, rec3, rec3BadEnum, rec4, rec5, rec6, rec7;

    uint16_t tagId             = 1;
    uint16_t tagLateAdd        = 2;
    uint16_t tagEarlyDrop      = 3;
    uint16_t tagNeighborBefore = 4;
    uint16_t tagCode           = 5;
    uint16_t tagNeighborAfter  = 6;
    uint16_t tagStatus         = 7;
    uint16_t tagVarA           = 8;
    uint16_t tagVarB           = 9;
};

Fixture buildFixture() {
    Fixture fx;

    DescriptorBuilder b("boundary", 1);
    b.addTagged("id",             fx.tagId,             PrimType::Int64, 1, 0xFFFF, /*isKey=*/true,  {});
    b.addTagged("lateAdd",        fx.tagLateAdd,         PrimType::Int32, 4, 0xFFFF, false, {});
    b.addTagged("earlyDrop",      fx.tagEarlyDrop,       PrimType::Int32, 1, 6,      false, {});
    b.addTagged("neighborBefore", fx.tagNeighborBefore,  PrimType::Int32, 1, 3,      false, {});
    b.addFixedStrTagged("code",   fx.tagCode, 6,                          1, 0xFFFF, false);
    b.addTagged("neighborAfter",  fx.tagNeighborAfter,   PrimType::Int32, 5, 0xFFFF, false, {});
    b.addTagged("status",         fx.tagStatus,          PrimType::Enum,  2, 5,      false, {1, 2, 3});
    b.addTagged("varA",            fx.tagVarA,           PrimType::String,1, 5,      false, {});
    b.addTagged("varB",            fx.tagVarB,           PrimType::String,3, 0xFFFF, false, {});
    fx.evolving = b.build();

    fx.v1 = materializeAsOf(fx.evolving, 1);
    fx.v2 = materializeAsOf(fx.evolving, 2);
    fx.v3 = materializeAsOf(fx.evolving, 3);
    fx.v4 = materializeAsOf(fx.evolving, 4);
    fx.v5 = materializeAsOf(fx.evolving, 5);
    fx.v6 = materializeAsOf(fx.evolving, 6);
    fx.v7 = materializeAsOf(fx.evolving, 7);

    fx.rec1 = encodeRecord(fx.v1, 1,
        {{fx.tagId, 2001}, {fx.tagEarlyDrop, 301}, {fx.tagNeighborBefore, 778}},
        {{fx.tagCode, "CODE01"}},
        {{fx.tagVarA, "vA1"}});

    fx.rec2 = encodeRecord(fx.v2, 2,
        {{fx.tagId, 2002}, {fx.tagEarlyDrop, 302}, {fx.tagNeighborBefore, 779}, {fx.tagStatus, 1}},
        {{fx.tagCode, "CODE02"}},
        {{fx.tagVarA, "vA2"}});

    fx.rec3 = encodeRecord(fx.v3, 3,
        {{fx.tagId, 2003}, {fx.tagEarlyDrop, 303}, {fx.tagStatus, 2}},
        {{fx.tagCode, "CODE03"}},
        {{fx.tagVarA, "vA3"}, {fx.tagVarB, "vB3"}});

    fx.rec3BadEnum = encodeRecord(fx.v3, 3,
        {{fx.tagId, 2003}, {fx.tagEarlyDrop, 303}, {fx.tagStatus, 99}},  // 99 not in {1,2,3}
        {{fx.tagCode, "CODE03"}},
        {{fx.tagVarA, "vA3"}, {fx.tagVarB, "vB3"}});

    fx.rec4 = encodeRecord(fx.v4, 4,
        {{fx.tagId, 2004}, {fx.tagLateAdd, 404}, {fx.tagEarlyDrop, 304}, {fx.tagStatus, 3}},
        {{fx.tagCode, "CODE04"}},
        {{fx.tagVarA, "vA4"}, {fx.tagVarB, "vB4"}});

    fx.rec5 = encodeRecord(fx.v5, 5,
        {{fx.tagId, 2005}, {fx.tagLateAdd, 405}, {fx.tagEarlyDrop, 305}, {fx.tagNeighborAfter, 905}},
        {{fx.tagCode, "CODE05"}},
        {{fx.tagVarB, "vB5"}});

    fx.rec6 = encodeRecord(fx.v6, 6,
        {{fx.tagId, 2006}, {fx.tagLateAdd, 406}, {fx.tagNeighborAfter, 906}},
        {{fx.tagCode, "CODE06"}},
        {{fx.tagVarB, "vB6"}});

    fx.rec7 = encodeRecord(fx.v7, 7,
        {{fx.tagId, 2007}, {fx.tagLateAdd, 407}, {fx.tagNeighborAfter, 907}},
        {{fx.tagCode, "CODE07"}},
        {{fx.tagVarB, "vB7"}});

    return fx;
}

} // namespace

// ---------------------------------------------------------------------------
// 1. sinceVersion boundary: lateAdd (tag2, K=4) -- absent at K-1=3,
//    present at K=4 and K+1=5.
// ---------------------------------------------------------------------------
TEST(DecodeBoundaryRegression, SinceVersionBoundary_AbsentBeforePresentAtAndAfter) {
    Fixture fx = buildFixture();

    EXPECT_FALSE(isLiveAt(*findByTag(fx.evolving, fx.tagLateAdd), 3));
    EXPECT_TRUE (isLiveAt(*findByTag(fx.evolving, fx.tagLateAdd), 4));
    EXPECT_TRUE (isLiveAt(*findByTag(fx.evolving, fx.tagLateAdd), 5));

    EXPECT_EQ(fx.v3.find("lateAdd"), nullptr);
    ASSERT_NE(fx.v4.find("lateAdd"), nullptr);
    ASSERT_NE(fx.v5.find("lateAdd"), nullptr);

    EXPECT_FALSE(readField(fx.v3, fx.rec3, fx.tagLateAdd).present);
    EXPECT_EQ(readField(fx.v4, fx.rec4, fx.tagLateAdd).i, 404);
    EXPECT_EQ(readField(fx.v5, fx.rec5, fx.tagLateAdd).i, 405);
}

// ---------------------------------------------------------------------------
// 2. untilVersion EXCLUSIVE boundary: earlyDrop (tag3, M=6) -- present at
//    M-1=5, absent at M=6 and M+1=7; isLiveAt false at EXACTLY M.
//    Second instance: neighborBefore (tag4, M=3) -- same shape, smaller M.
// ---------------------------------------------------------------------------
TEST(DecodeBoundaryRegression, UntilVersionBoundaryExclusive_EarlyDropAtM6) {
    Fixture fx = buildFixture();

    EXPECT_TRUE (isLiveAt(*findByTag(fx.evolving, fx.tagEarlyDrop), 5));
    EXPECT_FALSE(isLiveAt(*findByTag(fx.evolving, fx.tagEarlyDrop), 6));  // exactly M
    EXPECT_FALSE(isLiveAt(*findByTag(fx.evolving, fx.tagEarlyDrop), 7));

    ASSERT_NE(fx.v5.find("earlyDrop"), nullptr);
    EXPECT_EQ(fx.v6.find("earlyDrop"), nullptr);
    EXPECT_EQ(fx.v7.find("earlyDrop"), nullptr);

    EXPECT_EQ(readField(fx.v5, fx.rec5, fx.tagEarlyDrop).i, 305);
    EXPECT_FALSE(readField(fx.v6, fx.rec6, fx.tagEarlyDrop).present);
    EXPECT_FALSE(readField(fx.v7, fx.rec7, fx.tagEarlyDrop).present);
}

TEST(DecodeBoundaryRegression, UntilVersionBoundaryExclusive_NeighborBeforeAtM3) {
    Fixture fx = buildFixture();

    EXPECT_TRUE (isLiveAt(*findByTag(fx.evolving, fx.tagNeighborBefore), 2));
    EXPECT_FALSE(isLiveAt(*findByTag(fx.evolving, fx.tagNeighborBefore), 3));  // exactly M
    EXPECT_FALSE(isLiveAt(*findByTag(fx.evolving, fx.tagNeighborBefore), 4));

    ASSERT_NE(fx.v2.find("neighborBefore"), nullptr);
    EXPECT_EQ(fx.v3.find("neighborBefore"), nullptr);
    EXPECT_EQ(fx.v4.find("neighborBefore"), nullptr);

    EXPECT_EQ(readField(fx.v2, fx.rec2, fx.tagNeighborBefore).i, 779);
    EXPECT_FALSE(readField(fx.v3, fx.rec3, fx.tagNeighborBefore).present);
    EXPECT_FALSE(readField(fx.v4, fx.rec4, fx.tagNeighborBefore).present);
}

// ---------------------------------------------------------------------------
// 3. var-field drop -> var-index reassignment: varA (tag8) is live v1-v4 at
//    var-index 0; varB (tag9) joins at v3 at var-index 1 (behind varA).
//    At v5 varA drops and varB is reassigned down to var-index 0. Every
//    record's var section still decodes correctly per its own version.
// ---------------------------------------------------------------------------
TEST(DecodeBoundaryRegression, VarFieldDropReindexesLaterVarField) {
    Fixture fx = buildFixture();

    for (const Descriptor* v : {&fx.v1, &fx.v2, &fx.v3, &fx.v4}) {
        ASSERT_NE(v->find("varA"), nullptr);
        EXPECT_EQ(v->find("varA")->offset, 0u);
    }
    EXPECT_EQ(fx.v1.find("varB"), nullptr);
    EXPECT_EQ(fx.v2.find("varB"), nullptr);
    ASSERT_NE(fx.v3.find("varB"), nullptr);
    EXPECT_EQ(fx.v3.find("varB")->offset, 1u);   // behind varA
    ASSERT_NE(fx.v4.find("varB"), nullptr);
    EXPECT_EQ(fx.v4.find("varB")->offset, 1u);

    EXPECT_EQ(fx.v5.find("varA"), nullptr);      // dropped at v5
    for (const Descriptor* v : {&fx.v5, &fx.v6, &fx.v7}) {
        ASSERT_NE(v->find("varB"), nullptr);
        EXPECT_EQ(v->find("varB")->offset, 0u);  // reindexed after varA's removal
    }

    EXPECT_EQ(readField(fx.v1, fx.rec1, fx.tagVarA).bytes, "vA1");
    EXPECT_EQ(readField(fx.v3, fx.rec3, fx.tagVarA).bytes, "vA3");
    EXPECT_EQ(readField(fx.v3, fx.rec3, fx.tagVarB).bytes, "vB3");
    EXPECT_EQ(readField(fx.v4, fx.rec4, fx.tagVarA).bytes, "vA4");
    EXPECT_EQ(readField(fx.v4, fx.rec4, fx.tagVarB).bytes, "vB4");
    EXPECT_FALSE(readField(fx.v5, fx.rec5, fx.tagVarA).present);  // tag not even in v5's view
    EXPECT_EQ(readField(fx.v5, fx.rec5, fx.tagVarB).bytes, "vB5");
    EXPECT_EQ(readField(fx.v6, fx.rec6, fx.tagVarB).bytes, "vB6");
    EXPECT_EQ(readField(fx.v7, fx.rec7, fx.tagVarB).bytes, "vB7");
}

// ---------------------------------------------------------------------------
// 4. FixedStr(6) "code": fixed-block neighbours (neighborBefore dropped@3,
//    lateAdd added@4, earlyDrop dropped@6, neighborAfter added@5) keep its
//    width constant (6) but shift its inline offset per version; each
//    record decodes the correct bytes for its own version regardless.
// ---------------------------------------------------------------------------
TEST(DecodeBoundaryRegression, FixedStrWidthConstantOffsetShiftsPerVersion) {
    Fixture fx = buildFixture();

    struct Row { const Descriptor* view; const std::vector<uint8_t>* rec; uint32_t offset; const char* bytes; };
    std::vector<Row> rows = {
        {&fx.v1, &fx.rec1, 16, "CODE01"},
        {&fx.v2, &fx.rec2, 16, "CODE02"},
        {&fx.v3, &fx.rec3, 12, "CODE03"},
        {&fx.v4, &fx.rec4, 16, "CODE04"},
        {&fx.v5, &fx.rec5, 16, "CODE05"},
        {&fx.v6, &fx.rec6, 12, "CODE06"},
        {&fx.v7, &fx.rec7, 12, "CODE07"},
    };
    for (size_t i = 0; i < rows.size(); ++i) {
        SCOPED_TRACE(testing::Message() << "version v" << (i + 1));
        const FieldDescriptor* code = rows[i].view->find("code");
        ASSERT_NE(code, nullptr);
        EXPECT_EQ(code->fixedLen, 6u);              // width constant across the whole lineage
        EXPECT_EQ(code->offset, rows[i].offset);     // offset shifts with neighbours
        auto fv = readField(*rows[i].view, *rows[i].rec, fx.tagCode);
        EXPECT_TRUE(fv.present);
        EXPECT_EQ(fv.bytes, rows[i].bytes);
    }
}

// ---------------------------------------------------------------------------
// 5. Enum whitelist: a value in {1,2,3} validates and decodes at a version
//    where "status" is live (v3); an out-of-list value is rejected by the
//    Validator at that SAME version (readField itself does not gate on the
//    whitelist -- it is the zero-copy hot path and trusts a pre-validated
//    record, per Record.h's documented precondition); a record stamped at a
//    version where the field was dropped (v5) reads absent, and the
//    Validator for that version's view doesn't even know the field exists.
// ---------------------------------------------------------------------------
TEST(DecodeBoundaryRegression, EnumWhitelistValidatesInRangeRejectsOutOfRangePerVersion) {
    Fixture fx = buildFixture();

    EXPECT_EQ(readField(fx.v3, fx.rec3, fx.tagStatus).u, 2u);

    Validator val3(fx.v3);
    auto okResult = val3.validate(fx.rec3);
    EXPECT_TRUE(okResult.ok);

    auto badResult = val3.validate(fx.rec3BadEnum);
    EXPECT_FALSE(badResult.ok);
    EXPECT_EQ(badResult.fieldId, fx.tagStatus);
    ASSERT_NE(badResult.reason, nullptr);
    EXPECT_NE(std::string(badResult.reason).find("whitelist"), std::string::npos);

    // readField trusts the record (hot path, no whitelist check) -- it
    // decodes the same out-of-range raw value the Validator rejects.
    EXPECT_EQ(readField(fx.v3, fx.rec3BadEnum, fx.tagStatus).u, 99u);

    // v5: "status" was dropped at v5 (untilVersion=5) -- absent, not merely
    // "decodes to default"; the view doesn't carry the tag at all.
    EXPECT_EQ(fx.v5.find("status"), nullptr);
    EXPECT_FALSE(readField(fx.v5, fx.rec5, fx.tagStatus).present);
    Validator val5(fx.v5);
    EXPECT_TRUE(val5.validate(fx.rec5).ok);  // nothing to check -- the field isn't in this version
}

// ---------------------------------------------------------------------------
// 6a. sinceVersion == untilVersion: a field declared this way is NEVER live
//     at any version. DescriptorBuilder::build() does NOT reject this (no
//     such check exists); production behaviour is an empty live-set, not a
//     rejection -- isLiveAt() is false for every version and
//     materializeAsOf() excludes the field at every version, silently and
//     safely (not a crash, not a special case in the decode path).
// ---------------------------------------------------------------------------
TEST(DecodeBoundaryRegression, SinceEqualsUntil_NeverLive_EmptyLiveSetNotRejected) {
    DescriptorBuilder b("deadfield", 1);
    b.addTagged("anchor", 1, PrimType::Int64, 1, 0xFFFF, /*isKey=*/true, {});
    b.addTagged("dead",   2, PrimType::Int32, 3, 3,      /*isKey=*/false, {});  // since==until==3

    Descriptor d;
    EXPECT_NO_THROW(d = b.build());

    for (uint16_t v = 0; v <= 20; ++v) {
        SCOPED_TRACE(testing::Message() << "version " << v);
        EXPECT_FALSE(isLiveAt(*findByTag(d, 2), v));
        EXPECT_EQ(materializeAsOf(d, v).find("dead"), nullptr);
    }
}

// ---------------------------------------------------------------------------
// 6b. untilVersion < sinceVersion: an invalid range. build() does NOT reject
//     this either; v>=sinceVersion && v<untilVersion can never both hold
//     when untilVersion<sinceVersion, so this also collapses to an empty
//     live-set (isLiveAt always false) -- the SAME production behaviour as
//     6a, not a distinct rejection path.
// ---------------------------------------------------------------------------
TEST(DecodeBoundaryRegression, UntilLessThanSince_InvalidRange_EmptyLiveSetNotRejected) {
    DescriptorBuilder b("deadfield2", 1);
    b.addTagged("anchor", 1, PrimType::Int64, 1, 0xFFFF, /*isKey=*/true, {});
    b.addTagged("dead",   2, PrimType::Int32, 5, 2,      /*isKey=*/false, {});  // until(2) < since(5)

    Descriptor d;
    EXPECT_NO_THROW(d = b.build());

    for (uint16_t v = 0; v <= 20; ++v) {
        SCOPED_TRACE(testing::Message() << "version " << v);
        EXPECT_FALSE(isLiveAt(*findByTag(d, 2), v));
        EXPECT_EQ(materializeAsOf(d, v).find("dead"), nullptr);
    }
}

// ---------------------------------------------------------------------------
// 6c. The SAME invalid-range shape, reached via the applyOps path: applyOps's
//     DROP sets untilVersion=newVersion with no check that newVersion is
//     greater than the field's own sinceVersion. A caller that (incorrectly)
//     passes a newVersion <= the field's sinceVersion produces the identical
//     until<since empty live-set documented in 6b -- applyOps does not
//     reject it. This is a real footgun if a caller's version sequencing
//     were ever non-monotonic (out of scope to fix here: tests only).
// ---------------------------------------------------------------------------
TEST(DecodeBoundaryRegression, ApplyOpsNonMonotonicDropAlsoProducesEmptyLiveSet) {
    DescriptorBuilder b("deadfield3");
    b.addTagged("anchor", 1, PrimType::Int64, 1, 0xFFFF, true, {});
    b.addTagged("age",    2, PrimType::Int32, 1, 0xFFFF, false, {});  // sinceVersion=1
    auto cur = b.build();
    uint16_t nextTag = 3;

    // DROP with newVersion=0 < age's sinceVersion(1) -- applyOps has no
    // monotonicity check on newVersion and accepts this.
    auto dropped = applyOps(cur, {{AlterOpType::Drop, "age", "", {}, 0, {}}}, nextTag, /*newVersion=*/0);
    ASSERT_TRUE(dropped.has_value());

    const FieldDescriptor* age = findByTag(*dropped, 2);
    ASSERT_NE(age, nullptr);
    EXPECT_EQ(age->sinceVersion, 1);
    EXPECT_EQ(age->untilVersion, 0);  // until(0) < since(1): the invalid range

    for (uint16_t v = 0; v <= 10; ++v) {
        SCOPED_TRACE(testing::Message() << "version " << v);
        EXPECT_FALSE(isLiveAt(*age, v));  // retroactively dead at every version, including v1 where it used to be live
        EXPECT_EQ(materializeAsOf(*dropped, v).find("age"), nullptr);
    }
}
