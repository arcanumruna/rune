// tests/LineageDecodeTest.cpp — #418: the generated decode() and Decoder read every record by its OWN version.
//
// The evo_mix / evo_fix lineages (LineageFixtures.h) move fields at every step: a mid-block fixed DROP, a var DROP
// that re-indexes the var fields after it, ADDs, a RENAME, and an ADD that reuses a dropped field's name under a new
// tag.  One generated type per version (from that version's evolving descriptor, as `rune schema generate` fetches
// it).  Every (writer version w) x (reader version r) pair, both directions:
//   - w <= r (new reads old): decode() alone places every field (the compile-time kLineage rows);
//   - w >  r (old reads new): decode() alone reports false (the version is unknown to it) and leaves every member
//     at its default; with the topic's run-time Lineage it places every field;
// and each reader field is either the writer's value (its tag is live at w) or absent (default) - never another
// field's bytes.  The oracle is readField(materializeAsOf(evolving, w), record, tag).
#include <gtest/gtest.h>

#include "LineageFixtures.h"
#include "generated/evo_fix_v1.gen.h"
#include "generated/evo_fix_v2.gen.h"
#include "generated/evo_fix_v3.gen.h"
#include "generated/evo_fix_v4.gen.h"
#include "generated/evo_mix_v1.gen.h"
#include "generated/evo_mix_v2.gen.h"
#include "generated/evo_mix_v3.gen.h"
#include "generated/evo_mix_v4.gen.h"

#include <rune/codec/Evolve.h>
#include <rune/codec/Generator.h>
#include <rune/codec/Lineage.h>
#include <rune/codec/Record.h>

#include <cstring>
#include <map>
#include <optional>
#include <string>
#include <vector>

using rune::codec::Descriptor;
using rune::codec::FieldDescriptor;
using rune::codec::PrimType;
using rune::codec::testing::Lineage;
using rune::codec::testing::evolvingAt;
using namespace rune::gen;

namespace {

/// A field value, comparable across types (ints widen to i, string / bytes / string(N) to s).
struct Val {
    int64_t     i = 0;
    double      f = 0;
    std::string s;
    bool operator==(const Val&) const = default;
};
std::ostream& operator<<(std::ostream& o, const Val& v) { return o << "{i=" << v.i << " f=" << v.f << " s='" << v.s << "'}"; }

using Fields = std::map<uint16_t, Val>;   // tag -> value; a missing tag reads as default

const Descriptor& lineageOf(Lineage l) {
    static const Descriptor mix = evolvingAt(Lineage::Mix, 4, "evo_mix");
    static const Descriptor fix = evolvingAt(Lineage::Fix, 4, "evo_fix");
    return l == Lineage::Mix ? mix : fix;
}
const FieldDescriptor& fieldOf(Lineage l, uint16_t tag) {
    for (const auto& f : lineageOf(l).fields) if (f.id == tag) return f;
    throw std::logic_error("no tag");
}

/// The value every writer of version w puts in field `tag` (distinct per tag and version).
Val valueFor(Lineage l, uint16_t tag, uint16_t w) {
    const FieldDescriptor& f = fieldOf(l, tag);
    Val v;
    switch (f.type) {
        case PrimType::Bool:     v.i = 1; break;   // never the default
        case PrimType::Float64:  v.f = tag + w / 8.0; break;
        case PrimType::UInt16:   v.i = 100 * tag + w; break;
        case PrimType::String:   v.s = "t" + std::to_string(tag) + "w" + std::to_string(w); break;
        case PrimType::FixedStr: v.s = "c" + std::to_string(tag) + "w" + std::to_string(w); break;   // exactly 4 bytes
        default:                 v.i = 100'000LL * tag + 100 * w + 1; break;   // fits Int32, never 0
    }
    return v;
}

/// The record a writer of version `w` puts on the topic, from the version's layout (independent of the generated
/// code: what any client encodes).
std::vector<uint8_t> recordAt(Lineage l, uint16_t w) {
    const Descriptor d = rune::codec::materializeAsOf(lineageOf(l), w);
    std::vector<uint8_t> rec(4 + d.blockLength, 0);
    rec[0] = static_cast<uint8_t>(d.blockLength); rec[1] = static_cast<uint8_t>(d.blockLength >> 8);
    rec[2] = static_cast<uint8_t>(w);             rec[3] = static_cast<uint8_t>(w >> 8);
    std::vector<const FieldDescriptor*> vars;
    for (const auto& f : d.fields) {
        const Val v = valueFor(l, f.id, w);
        uint8_t* p = rec.data() + 4 + f.offset;
        switch (f.type) {
            case PrimType::Int32:   { const int32_t x = static_cast<int32_t>(v.i); std::memcpy(p, &x, 4); break; }
            case PrimType::Int64:   std::memcpy(p, &v.i, 8); break;
            case PrimType::UInt16:  { const uint16_t x = static_cast<uint16_t>(v.i); std::memcpy(p, &x, 2); break; }
            case PrimType::Float64: std::memcpy(p, &v.f, 8); break;
            case PrimType::Bool:    *p = static_cast<uint8_t>(v.i); break;
            case PrimType::FixedStr: std::memcpy(p, v.s.data(), f.fixedLen); break;
            case PrimType::String:  vars.push_back(&f); break;
            default: throw std::logic_error("type");
        }
    }
    std::sort(vars.begin(), vars.end(), [](auto* a, auto* b) { return a->offset < b->offset; });
    for (const FieldDescriptor* f : vars) {
        const std::string s = valueFor(l, f->id, w).s;
        const uint32_t n = static_cast<uint32_t>(s.size());
        rec.insert(rec.end(), {static_cast<uint8_t>(n), static_cast<uint8_t>(n >> 8), static_cast<uint8_t>(n >> 16),
                               static_cast<uint8_t>(n >> 24)});
        rec.insert(rec.end(), s.begin(), s.end());
    }
    return rec;
}

/// What a correct reader with fields `tags` sees in a record of version w: the writer's value where the tag is live
/// at w, nothing (default) elsewhere.  Cross-checked against readField over the record's own version.
Fields expected(Lineage l, std::span<const uint16_t> tags, uint16_t w, const std::vector<uint8_t>& rec) {
    Fields out;
    const Descriptor at = rune::codec::materializeAsOf(lineageOf(l), w);
    for (const uint16_t t : tags) {
        const bool live = rune::codec::isLiveAt(fieldOf(l, t), w);
        const auto fv = rune::codec::readField(at, rec, t);
        EXPECT_EQ(fv.present, live) << "oracle: tag " << t << " at v" << w;
        if (live) out[t] = valueFor(l, t, w);
    }
    return out;
}

Val sv(std::string_view s) { Val v; v.s = std::string(s); return v; }
Val iv(int64_t i) { Val v; v.i = i; return v; }
Val fv(double f) { Val v; v.f = f; return v; }
template <std::size_t N> Val av(const std::array<char, N>& a) { return sv(std::string_view(a.data(), N)); }

/// Every reader field as a Val, defaults dropped (absent == default: the writers never write a default).
Fields nonDefault(Fields all) {
    for (auto it = all.begin(); it != all.end();) it = (it->second == Val{} || it->second == sv(std::string(4, '\0'))) ? all.erase(it) : std::next(it);
    return all;
}

Fields collect(const EvoMixV1& s) { return nonDefault({{0, iv(s.id)}, {1, iv(s.age)}, {2, iv(s.score)}, {3, sv(s.nick)}, {4, sv(s.bio)}, {5, iv(s.flag)}}); }
Fields collect(const EvoMixV2& s) { return nonDefault({{0, iv(s.id)}, {2, iv(s.score)}, {4, sv(s.bio)}, {5, iv(s.flag)}}); }
Fields collect(const EvoMixV3& s) { return nonDefault({{0, iv(s.id)}, {2, iv(s.score)}, {4, sv(s.bio)}, {5, iv(s.flag)}, {6, iv(s.level)}, {7, sv(s.motto)}}); }
Fields collect(const EvoMixV4& s) { return nonDefault({{0, iv(s.id)}, {2, iv(s.points)}, {5, iv(s.flag)}, {6, iv(s.level)}, {7, sv(s.motto)}, {8, iv(s.age)}}); }
Fields collect(const EvoFixV1& s) { return nonDefault({{0, iv(s.id)}, {1, iv(s.age)}, {2, iv(s.score)}, {3, fv(s.x)}, {4, av(s.code)}}); }
Fields collect(const EvoFixV2& s) { return nonDefault({{0, iv(s.id)}, {2, iv(s.score)}, {3, fv(s.x)}, {4, av(s.code)}}); }
Fields collect(const EvoFixV3& s) { return nonDefault({{0, iv(s.id)}, {2, iv(s.score)}, {3, fv(s.x)}, {4, av(s.code)}, {5, iv(s.level)}}); }
Fields collect(const EvoFixV4& s) { return nonDefault({{0, iv(s.id)}, {3, fv(s.y)}, {4, av(s.code)}, {5, iv(s.level)}, {6, iv(s.age)}}); }

Fields collect(const EvoFixV1Decoder& d) { return nonDefault({{0, iv(d.id())}, {1, iv(d.age())}, {2, iv(d.score())}, {3, fv(d.x())}, {4, sv(d.code())}}); }
Fields collect(const EvoFixV2Decoder& d) { return nonDefault({{0, iv(d.id())}, {2, iv(d.score())}, {3, fv(d.x())}, {4, sv(d.code())}}); }
Fields collect(const EvoFixV3Decoder& d) { return nonDefault({{0, iv(d.id())}, {2, iv(d.score())}, {3, fv(d.x())}, {4, sv(d.code())}, {5, iv(d.level())}}); }
Fields collect(const EvoFixV4Decoder& d) { return nonDefault({{0, iv(d.id())}, {3, fv(d.y())}, {4, sv(d.code())}, {5, iv(d.level())}, {6, iv(d.age())}}); }

/// The topic's run-time lineage as reader T gets it (the broker serves the evolving descriptor under T's name).
template <class T> typename T::Lineage runtimeLineage(Lineage l, uint16_t upTo = 4) {
    return T::lineage(evolvingAt(l, upTo, std::string(T::kSchemaName)));
}

template <class T>
void checkStruct(Lineage l, uint16_t w) {
    SCOPED_TRACE(std::string(T::kSchemaName) + " reading a v" + std::to_string(w) + " record");
    const std::vector<uint8_t> rec = recordAt(l, w);
    const Fields want = expected(l, std::span<const uint16_t>(T::kFieldTags, T::kFieldCount), w, rec);
    T s;
    if (w <= T::kVersion) {
        EXPECT_TRUE(s.decode(rec)) << "a version up to kVersion needs no lineage";
        EXPECT_EQ(collect(s), want);
    } else {
        EXPECT_FALSE(s.decode(rec)) << "a newer version is reported, never decoded at the wrong offsets";
        EXPECT_EQ(collect(s), Fields{}) << "every member at its default";
    }
    const auto lin = runtimeLineage<T>(l);
    T t;
    EXPECT_TRUE(t.decode(rec, lin));
    EXPECT_EQ(collect(t), want) << "with the topic's lineage every version reads right";
    // A lineage that ends before the record's version does not know it either.
    if (w > T::kVersion) {
        const auto older = runtimeLineage<T>(l, static_cast<uint16_t>(w - 1));
        T u;
        EXPECT_FALSE(u.decode(rec, older));
        EXPECT_EQ(collect(u), Fields{});
    }
}

template <class T>
void checkDecoder(Lineage l, uint16_t w) {
    using D = typename T::Decoder;
    SCOPED_TRACE(std::string(T::kSchemaName) + "Decoder reading a v" + std::to_string(w) + " record");
    const std::vector<uint8_t> rec = recordAt(l, w);
    const Fields want = expected(l, std::span<const uint16_t>(T::kFieldTags, T::kFieldCount), w, rec);
    const D plain(rec);
    EXPECT_EQ(plain.version(), w);
    if (w <= T::kVersion) {
        EXPECT_TRUE(plain.known());
        EXPECT_EQ(collect(plain), want);
    } else {
        EXPECT_FALSE(plain.known());
        EXPECT_EQ(collect(plain), Fields{});
    }
    const auto lin = runtimeLineage<T>(l);
    const D withLineage(rec, lin);
    EXPECT_TRUE(withLineage.known());
    EXPECT_EQ(collect(withLineage), want);
    EXPECT_EQ(withLineage.key(), want.at(0).i) << "the key field is placed like any other";
}

template <class T> void everyWriter(Lineage l, bool decoder = false) {
    for (uint16_t w = 1; w <= 4; ++w) {
        checkStruct<T>(l, w);
        if constexpr (requires { typename T::Decoder; }) if (decoder) checkDecoder<T>(l, w);
    }
}

}  // namespace

// ── The matrix: 4 writer versions x 4 reader versions x {decode, decode+lineage, Decoder, Decoder+lineage} ──

TEST(LineageDecodeRegression, MixedLineageV1ReaderReadsEveryVersion) { everyWriter<EvoMixV1>(Lineage::Mix); }
TEST(LineageDecodeRegression, MixedLineageV2ReaderReadsEveryVersion) { everyWriter<EvoMixV2>(Lineage::Mix); }
TEST(LineageDecodeRegression, MixedLineageV3ReaderReadsEveryVersion) { everyWriter<EvoMixV3>(Lineage::Mix); }
TEST(LineageDecodeRegression, MixedLineageV4ReaderReadsEveryVersion) { everyWriter<EvoMixV4>(Lineage::Mix); }
TEST(LineageDecodeRegression, FixedLineageV1ReaderReadsEveryVersion) { everyWriter<EvoFixV1>(Lineage::Fix, true); }
TEST(LineageDecodeRegression, FixedLineageV2ReaderReadsEveryVersion) { everyWriter<EvoFixV2>(Lineage::Fix, true); }
TEST(LineageDecodeRegression, FixedLineageV3ReaderReadsEveryVersion) { everyWriter<EvoFixV3>(Lineage::Fix, true); }
TEST(LineageDecodeRegression, FixedLineageV4ReaderReadsEveryVersion) { everyWriter<EvoFixV4>(Lineage::Fix, true); }

// ── The issue's repro, pinned by value ──

TEST(LineageDecodeRegression, AV2TypeReadsAV1RecordWrittenBeforeAMidBlockDrop) {
    // v1 {id, age, score, ...}; v2 DROP age: score moved 12 -> 8.  The v1 record's score is at 12.
    const auto rec = recordAt(Lineage::Mix, 1);
    EvoMixV2 s;
    ASSERT_TRUE(s.decode(rec));
    EXPECT_EQ(s.score, valueFor(Lineage::Mix, 2, 1).i) << "score, not age's bytes";
    EXPECT_EQ(s.bio, valueFor(Lineage::Mix, 4, 1).s) << "bio is var index 1 at v1 (nick was index 0)";
}

TEST(LineageDecodeRegression, AV1TypeNeverMisreadsAV2RecordWrittenAfterAMidBlockDrop) {
    const auto rec = recordAt(Lineage::Mix, 2);
    EvoMixV1 s;
    EXPECT_FALSE(s.decode(rec)) << "without the lineage a v1 type cannot place a v2 record";
    EXPECT_EQ(s.age, 0) << "never score's bytes";
    const auto lin = runtimeLineage<EvoMixV1>(Lineage::Mix);
    ASSERT_TRUE(s.decode(rec, lin));
    EXPECT_EQ(s.age, 0) << "age is dropped at v2: absent";
    EXPECT_EQ(s.score, valueFor(Lineage::Mix, 2, 2).i);
    EXPECT_TRUE(s.nick.empty()) << "nick is dropped at v2: absent";
    EXPECT_EQ(s.bio, valueFor(Lineage::Mix, 4, 2).s) << "bio is var index 0 at v2";
}

TEST(LineageDecodeRegression, AReusedNameIsANewFieldNotTheDroppedOne) {
    // v4 ADDs `age` again (Int64, tag 8): an old reader's age (tag 1, dropped at v2) stays absent.
    const auto rec = recordAt(Lineage::Mix, 4);
    EvoMixV1 old;
    ASSERT_TRUE(old.decode(rec, runtimeLineage<EvoMixV1>(Lineage::Mix)));
    EXPECT_EQ(old.age, 0);
    EvoMixV4 now;
    ASSERT_TRUE(now.decode(rec));
    EXPECT_EQ(now.age, valueFor(Lineage::Mix, 8, 4).i);
}

// ── The generated encode writes exactly the layout every reader places ──

TEST(LineageDecodeRegression, EachVersionsEncodeWritesItsVersionsLayout) {
    {
        EvoMixV2 s; s.id = valueFor(Lineage::Mix, 0, 2).i; s.score = static_cast<int32_t>(valueFor(Lineage::Mix, 2, 2).i);
        const std::string bio = valueFor(Lineage::Mix, 4, 2).s; s.bio = bio; s.flag = valueFor(Lineage::Mix, 5, 2).i != 0;
        std::vector<uint8_t> out(s.encodedSize());
        out.resize(s.encode(out.data()));
        EXPECT_EQ(out, recordAt(Lineage::Mix, 2));
    }
    {
        EvoFixV4 s; s.id = valueFor(Lineage::Fix, 0, 4).i; s.y = valueFor(Lineage::Fix, 3, 4).f;
        std::memcpy(s.code.data(), valueFor(Lineage::Fix, 4, 4).s.data(), 4);
        s.level = static_cast<int32_t>(valueFor(Lineage::Fix, 5, 4).i); s.age = static_cast<uint16_t>(valueFor(Lineage::Fix, 6, 4).i);
        std::vector<uint8_t> out(s.encodedSize());
        out.resize(s.encode(out.data()));
        EXPECT_EQ(out, recordAt(Lineage::Fix, 4));
        std::vector<uint8_t> claimed(EvoFixV4::Encoder::kEncodedLength, 0xEE);
        EvoFixV4::Encoder(claimed).id(s.id).y(s.y).code(valueFor(Lineage::Fix, 4, 4).s).level(s.level).age(s.age);
        EXPECT_EQ(claimed, out);
    }
}

// ── Hostile and edge input ──

TEST(LineageDecodeRegression, UnknownVersionsAndShortRecordsAreReportedNeverDecoded) {
    auto rec = recordAt(Lineage::Mix, 2);
    const auto lin = runtimeLineage<EvoMixV4>(Lineage::Mix);
    for (const uint16_t ver : {uint16_t{0}, uint16_t{5}, uint16_t{999}, uint16_t{0xFFFF}}) {
        rec[2] = static_cast<uint8_t>(ver); rec[3] = static_cast<uint8_t>(ver >> 8);
        EvoMixV4 s;
        EXPECT_FALSE(s.decode(rec)) << ver;
        EXPECT_FALSE(s.decode(rec, lin)) << ver;
        const auto fixLin = runtimeLineage<EvoFixV4>(Lineage::Fix);
        const EvoFixV4Decoder d(rec, fixLin);
        EXPECT_FALSE(d.known()) << ver;
        EXPECT_EQ(d.id(), 0) << ver;
    }
    for (std::size_t n = 0; n < 4; ++n) {
        EvoMixV2 s;
        EXPECT_FALSE(s.decode(std::span<const uint8_t>(rec.data(), n)));
        const EvoFixV2Decoder d(std::span<const uint8_t>(rec.data(), n));
        EXPECT_FALSE(d.known());
    }
}

TEST(LineageDecodeRegression, ATruncatedRecordReadsTheFieldsItHoldsAndNothingPastItsEnd) {
    const auto full = recordAt(Lineage::Mix, 3);
    for (std::size_t n = 4; n <= full.size(); ++n) {
        std::vector<uint8_t> cut(full.begin(), full.begin() + static_cast<std::ptrdiff_t>(n));
        EvoMixV3 s;
        ASSERT_TRUE(s.decode(cut));
        const Fields got = collect(s);
        const Fields want = expected(Lineage::Mix, std::span<const uint16_t>(EvoMixV3::kFieldTags, EvoMixV3::kFieldCount), 3, full);
        for (const auto& [tag, v] : got) EXPECT_EQ(v, want.at(tag)) << "tag " << tag << " cut at " << n << ": right or absent";
    }
}

TEST(LineageDecodeRegression, DecodeResetsEveryMemberSoNoStaleValueSurvives) {
    EvoMixV3 s;
    ASSERT_TRUE(s.decode(recordAt(Lineage::Mix, 3)));
    ASSERT_NE(s.level, 0);
    ASSERT_FALSE(s.motto.empty());
    ASSERT_TRUE(s.decode(recordAt(Lineage::Mix, 2)));   // level / motto are not part of v2
    EXPECT_EQ(s.level, 0);
    EXPECT_TRUE(s.motto.empty());
}

TEST(LineageDecodeRegression, ALineageOfAnotherSchemaIsRefused) {
    EXPECT_THROW((void)EvoMixV2::lineage(evolvingAt(Lineage::Fix, 4, "evo_mix_v2")), std::invalid_argument);
    EXPECT_THROW((void)EvoMixV3::lineage(evolvingAt(Lineage::Mix, 2, "evo_mix_v3")), std::invalid_argument)
        << "a lineage that ends before the type's version";
    EXPECT_NO_THROW((void)EvoMixV3::lineage(evolvingAt(Lineage::Mix, 3, "evo_mix_v3")));
}

// ── The table itself ──

TEST(LineageDecodeRegression, TheCompileTimeRowsAreTheRunTimeRows) {
    const auto lin = runtimeLineage<EvoMixV3>(Lineage::Mix);
    for (uint16_t v = 1; v <= EvoMixV3::kVersion; ++v) {
        const uint32_t* r = lin.row(v);
        ASSERT_NE(r, nullptr);
        for (std::size_t i = 0; i < EvoMixV3::kFieldCount; ++i) EXPECT_EQ(r[i], EvoMixV3::kLineage[v - 1][i]) << v << "/" << i;
    }
    EXPECT_EQ(lin.row(0), nullptr);
    EXPECT_EQ(lin.row(5), nullptr);
    EXPECT_EQ(lin.maxVersion(), 4);
    // score (member 1 of v3) sits at 12 in v1 and at 8 from v2 on; bio (member 2) is var 1 at v1, var 0 after.
    EXPECT_EQ(EvoMixV3::kLineage[0][1], 12u);
    EXPECT_EQ(EvoMixV3::kLineage[1][1], 8u);
    EXPECT_EQ(EvoMixV3::kLineage[0][2], 1u);
    EXPECT_EQ(EvoMixV3::kLineage[1][2], 0u);
    EXPECT_EQ(EvoMixV3::kLineage[1][4], rune::codec::kAbsentField) << "level is not part of v2";
}

TEST(LineageDecodeRegression, TheGeneratedTypeIsTheVersionNotTheWholeLineage) {
    // The v4 evolving descriptor still carries the retired fields; the v2 type of it has only v2's members.
    const std::string hdr = rune::codec::generateHeader(evolvingAt(Lineage::Mix, 4, "evo_mix"), 2);
    EXPECT_NE(hdr.find("int32_t points = {};"), std::string::npos) << "names are the lineage's (a RENAME is not versioned)";
    EXPECT_EQ(hdr.find("int32_t age = {};"), std::string::npos) << "age is retired at v2";
    EXPECT_EQ(hdr.find("level"), std::string::npos) << "level comes at v3";
    EXPECT_NE(hdr.find("kVersion     = 2u"), std::string::npos);
    EXPECT_THROW((void)rune::codec::generateHeader(evolvingAt(Lineage::Mix, 2), 0), std::invalid_argument);
    EXPECT_THROW((void)rune::codec::generateHeader(evolvingAt(Lineage::Mix, 2), 3), std::invalid_argument);
}

// ── Adversarial review of PR #434 (#418): hostile headers against every reader, every writer version ──
//
// Every string_view a decode hands out must lie inside the record span (or be the default empty view): the record
// is placed at the front of a large zeroed backing buffer, so a read past the span is observable as a view that
// points outside it (and stays inside the allocation: no crash, a deterministic failure).

namespace {

std::vector<std::string_view> views(const EvoMixV1& s) { return {s.nick, s.bio}; }
std::vector<std::string_view> views(const EvoMixV2& s) { return {s.bio}; }
std::vector<std::string_view> views(const EvoMixV3& s) { return {s.bio, s.motto}; }
std::vector<std::string_view> views(const EvoMixV4& s) { return {s.motto}; }

bool inside(std::string_view v, std::span<const uint8_t> rec) {
    if (v.data() == nullptr) return v.empty();
    const auto* b = reinterpret_cast<const char*>(rec.data());
    return v.data() >= b && v.data() + v.size() <= b + rec.size();
}

template <class T>
void sweepBlockLength(Lineage l, uint16_t w, uint16_t verOverride) {
    const std::vector<uint8_t> rec = recordAt(l, w);
    std::vector<uint8_t> backing(rec.size() + 0x10000 + 64, 0);
    std::copy(rec.begin(), rec.end(), backing.begin());
    backing[2] = static_cast<uint8_t>(verOverride); backing[3] = static_cast<uint8_t>(verOverride >> 8);
    const std::span<const uint8_t> span(backing.data(), rec.size());
    const auto lin = runtimeLineage<T>(l);
    for (uint32_t bl = 0; bl <= 0xFFFF; ++bl) {
        backing[0] = static_cast<uint8_t>(bl); backing[1] = static_cast<uint8_t>(bl >> 8);
        T s;
        (void)s.decode(span);
        for (const auto v : views(s))
            ASSERT_TRUE(inside(v, span)) << T::kSchemaName << " v" << w << " record, ver " << verOverride << ", bl " << bl
                                         << ": a var field read past the record";
        T t;
        (void)t.decode(span, lin);
        for (const auto v : views(t))
            ASSERT_TRUE(inside(v, span)) << T::kSchemaName << "+lineage v" << w << " record, ver " << verOverride
                                         << ", bl " << bl << ": a var field read past the record";
    }
}

template <class T> void sweepEveryWriter(Lineage l) {
    for (uint16_t w = 1; w <= 4; ++w) {
        sweepBlockLength<T>(l, w, w);              // the record's own version, a hostile block length
        sweepBlockLength<T>(l, w, T::kVersion);    // claims the reader's own version: the inlined fast path
    }
}

}  // namespace

TEST(LineageDecodeRegression, AHostileBlockLengthNeverReadsAVarFieldPastTheRecordV1AdversarialRegression) {
    sweepEveryWriter<EvoMixV1>(Lineage::Mix);
}
TEST(LineageDecodeRegression, AHostileBlockLengthNeverReadsAVarFieldPastTheRecordV2AdversarialRegression) {
    sweepEveryWriter<EvoMixV2>(Lineage::Mix);
}
TEST(LineageDecodeRegression, AHostileBlockLengthNeverReadsAVarFieldPastTheRecordV3AdversarialRegression) {
    sweepEveryWriter<EvoMixV3>(Lineage::Mix);
}
TEST(LineageDecodeRegression, AHostileBlockLengthNeverReadsAVarFieldPastTheRecordV4AdversarialRegression) {
    sweepEveryWriter<EvoMixV4>(Lineage::Mix);
}

TEST(LineageDecodeRegression, AHostileVarLengthReadsThatFieldAndEveryLaterOneEmptyAdversarialRegression) {
    // v3 record read by the v3 type (fast path) and by the v4 type (row path): the first var length is hostile.
    for (const uint32_t bad : {0xFFFFFFFFu, 0xFFFFFFFCu, 0x80000000u, 0x10000u}) {
        std::vector<uint8_t> rec = recordAt(Lineage::Mix, 3);
        const uint16_t bl = static_cast<uint16_t>(rec[0] | (rec[1] << 8));
        std::memcpy(rec.data() + 4 + bl, &bad, 4);
        EvoMixV3 s;
        ASSERT_TRUE(s.decode(rec));
        EXPECT_TRUE(s.bio.empty()) << bad;
        EXPECT_TRUE(s.motto.empty()) << bad << ": never read from inside the bad field's bytes";
        EXPECT_EQ(s.level, valueFor(Lineage::Mix, 6, 3).i) << "the fixed block is still read";
        EvoMixV4 t;
        ASSERT_TRUE(t.decode(rec));
        EXPECT_TRUE(t.motto.empty()) << bad;
    }
}

TEST(LineageDecodeRegression, EveryHeaderAndTruncationOfAFixedRecordReadsInsideItAdversarialRegression) {
    // The zero-copy Decoder and the struct, every (version, block length, size) of a v3 evo_fix record: the
    // string(4) field's view stays inside the record, and a version the reader does not know reads nothing.
    const std::vector<uint8_t> full = recordAt(Lineage::Fix, 3);
    std::vector<uint8_t> backing(full.size() + 0x10000 + 64, 0);
    std::copy(full.begin(), full.end(), backing.begin());
    for (const uint16_t ver : {uint16_t{0}, uint16_t{1}, uint16_t{2}, uint16_t{3}, uint16_t{4}, uint16_t{5}, uint16_t{0xFFFF}}) {
        backing[2] = static_cast<uint8_t>(ver); backing[3] = static_cast<uint8_t>(ver >> 8);
        for (const uint32_t bl : {0u, 1u, 8u, 15u, 16u, 23u, 24u, 27u, 28u, 29u, 0x7FFFu, 0xFFFFu}) {
            backing[0] = static_cast<uint8_t>(bl); backing[1] = static_cast<uint8_t>(bl >> 8);
            for (std::size_t n = 0; n <= full.size(); ++n) {
                const std::span<const uint8_t> span(backing.data(), n);
                const EvoFixV2Decoder d(span);
                ASSERT_TRUE(inside(d.code(), span)) << "ver " << ver << " bl " << bl << " n " << n;
                if (ver > EvoFixV2::kVersion || ver == 0) {
                    EXPECT_FALSE(d.known());
                    EXPECT_EQ(d.id(), 0);
                }
                EvoFixV2 s;
                const bool ok = s.decode(span);
                if (ver > EvoFixV2::kVersion || ver == 0 || n < 4) {
                    EXPECT_FALSE(ok) << "ver " << ver << " bl " << bl << " n " << n;
                    EXPECT_EQ(s.id, 0);
                }
            }
        }
    }
}
