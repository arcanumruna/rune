// Namespaces Task 2 (plan 2026-10-26-rune-namespaces.md): catalog formatVer 2 layout (u32 nsId after kind),
// kinds 32-35, the explicit kind table, NsName.h and CatalogFingerprintV2.h.
// Task 3 appends the NamespaceRegistry / ApplyV2 tests to this file.
#include <rune/catalog/CatalogEvent.h>
#include <rune/catalog/CatalogFile.h>
#include <rune/catalog/CatalogFingerprintV2.h>
#include <rune/catalog/CatalogReplayer.h>
#include <rune/catalog/NsName.h>
#include <gtest/gtest.h>

#include <fstream>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "CatalogGoldenPins.h"

namespace rc = rune::catalog;
namespace rp = rune::protocol;

namespace {
std::string toHex(const std::vector<uint8_t>& b) {
    static constexpr char d[] = "0123456789abcdef";
    std::string s;
    for (uint8_t x : b) { s += d[x >> 4]; s += d[x & 15]; }
    return s;
}

/// The v1 golden DropTopic event of CatalogEventTest (goldenEvent(1)), spelled again here.
rc::CatalogEvent dropTopicEvent(uint16_t formatVer, uint32_t nsId) {
    rc::CatalogEvent e;
    e.formatVer = formatVer;
    e.nsId = nsId;
    e.ops = rc::DropTopicOps{.topicId = 3};
    e.kind = rc::EventKind::DropTopic;
    e.objectId = 0x0102030405060708ull;
    e.objectName = "t";
    e.prevVersion = 0x0A0B0C0D;
    e.newVersion = 0x0A0B0C0E;
    e.statement = "s;";
    e.principal = rc::Principal{.sessionId = -2, .clientName = "c", .sourceAddress = "a", .authenticated = "u",
                                .requestId = {.hi = 0x0123456789ABCDEFull, .lo = 0xFEDCBA9876543210ull},
                                .requestFingerprint = 0x5151515151515151ull};
    e.replayedFrom = rc::ReplayedFrom{.clusterId = "x", .offset = -3, .principal = "p"};
    e.timestampMs = -5;
    e.fingerprint = 0xF0E1D2C3B4A59687ull;
    return e;
}

rp::NamespaceLimits distinctLimits() {
    rp::NamespaceLimits l;
    l.maxTopics = 11; l.maxViews = 12; l.maxUsers = 13; l.maxDiskBytes = 14; l.produceBytesPerSec = 15;
    l.maxConnections = 16; l.maxViewMemoryBytes = 17; l.lowestMinInsync = 18; l.featureBits = 0x81;
    return l;
}

/// One richly populated Ops per namespace kind, in kind order 32..35.
std::vector<rc::Ops> namespaceOps() {
    return {rc::NamespaceCreatedOps{.name = "arena-7", .limits = distinctLimits()},
            rc::NamespaceLimitsSetOps{.limits = distinctLimits()},
            rc::NamespaceStateChangedOps{.to = rc::NsStateTag::SuspendedRejectAll},
            rc::NamespaceDeletedOps{}};
}
rc::CatalogEvent namespaceEvent(std::size_t i, uint32_t nsId = 5) {
    rc::CatalogEvent e;
    e.formatVer = rc::kFormatV2;
    e.nsId = nsId;
    e.ops = namespaceOps().at(i);
    e.kind = rc::kindOf(e.ops);
    e.objectId = 0;
    e.objectName = "arena-7";
    e.prevVersion = 1;
    e.newVersion = 2;
    e.statement = "CREATE NAMESPACE arena-7";
    e.principal.sessionId = 9;
    e.timestampMs = 1234;
    e.fingerprint = 0xABCDEF;
    return e;
}
} // namespace

// ── the v2 header ─────────────────────────────────────────────────────────────
TEST(CatalogV2Regression, HeaderLayoutRegression) {
    const rc::CatalogEvent v2 = dropTopicEvent(rc::kFormatV2, 7);
    const auto enc = rc::encode(v2);
    ASSERT_TRUE(enc.has_value()) << rc::encodeErrorName(enc.error());
    // Pinned: the v1 golden DropTopic bytes with formatVer 2 and u32 nsId = 7 spliced in after the kind.
    EXPECT_EQ(toHex(*enc),
              "020002000700000008070605040302010d0c0b0a0e0c0b0afbffffffffffffff8796a5b4c3d2e1f00100740200000073"
              "3bfeffffffffffffff010063010061010075efcdab89674523011032547698badcfe515151515151515101010078fdff"
              "ffffffffffff010070050000000203000000");
    ASSERT_GE(enc->size(), 8u);
    EXPECT_EQ((*enc)[4], 7); EXPECT_EQ((*enc)[5], 0); EXPECT_EQ((*enc)[6], 0); EXPECT_EQ((*enc)[7], 0);
    // ... and everything else equals the v1 encoding shifted by 4.
    const auto v1 = rc::encode(dropTopicEvent(rc::kFormatV1, 0));
    ASSERT_TRUE(v1.has_value());
    ASSERT_EQ(enc->size(), v1->size() + 4);
    EXPECT_TRUE(std::equal(v1->begin() + 4, v1->end(), enc->begin() + 8));
    auto d = rc::decode(*enc);
    ASSERT_TRUE(d.has_value());
    EXPECT_EQ(*d, v2);
    EXPECT_EQ(d->nsId, 7u);
}

TEST(CatalogV2Regression, CurrentFormatVerIsTwoRegression) {
    EXPECT_EQ(rc::kCurrentFormatVer, 2);
    EXPECT_EQ(rc::CatalogEvent{}.formatVer, rc::kFormatV2);
}

TEST(CatalogV2Regression, V1EventsDecodeWithNsIdZeroAndCannotCarryOneRegression) {
    const auto v1 = rc::encode(dropTopicEvent(rc::kFormatV1, 0));
    ASSERT_TRUE(v1.has_value());
    auto d = rc::decode(*v1);
    ASSERT_TRUE(d.has_value());
    EXPECT_EQ(d->nsId, 0u);
    EXPECT_EQ(d->formatVer, rc::kFormatV1);
    // formatVer 1 has no nsId field: an event that carries one is not encodable.
    auto bad = rc::encode(dropTopicEvent(rc::kFormatV1, 3));
    ASSERT_FALSE(bad.has_value());
    EXPECT_EQ(bad.error(), rc::EncodeError::BadEnum);
}

// ── the kind table ────────────────────────────────────────────────────────────
TEST(CatalogV2Regression, KindTableBijectiveRegression) {
    static_assert(std::variant_size_v<rc::Ops> == 13);
    for (std::size_t i = 0; i < rc::kKindOfAlt.size(); ++i)
        EXPECT_EQ(rc::altOf(rc::kKindOfAlt[i]), static_cast<int>(i)) << i;
    EXPECT_EQ(rc::altOf(static_cast<rc::EventKind>(10)), -1);
    EXPECT_EQ(rc::altOf(static_cast<rc::EventKind>(36)), -1);
    EXPECT_EQ(rc::altOf(static_cast<rc::EventKind>(0)), -1);
    EXPECT_EQ(rc::altOf(static_cast<rc::EventKind>(31)), -1);
    EXPECT_EQ(rc::kKindOfAlt[8], rc::EventKind::DropView);
    EXPECT_EQ(rc::kKindOfAlt[9], rc::EventKind::NamespaceCreated);
    EXPECT_EQ(rc::kKindOfAlt[12], rc::EventKind::NamespaceDeleted);
    // kindOf agrees with the table for every namespace alternative.
    const auto ops = namespaceOps();
    for (std::size_t i = 0; i < ops.size(); ++i)
        EXPECT_EQ(static_cast<uint16_t>(rc::kindOf(ops[i])), 32 + i);
}

TEST(CatalogV2Regression, NamespaceBodiesRoundTripRegression) {
    for (std::size_t i = 0; i < 4; ++i) {
        const rc::CatalogEvent e = namespaceEvent(i);
        const auto enc = rc::encode(e);
        ASSERT_TRUE(enc.has_value()) << i << " " << rc::encodeErrorName(enc.error());
        auto d = rc::decode(*enc);
        ASSERT_TRUE(d.has_value()) << i << " " << rc::decodeErrorName(d.error());
        EXPECT_EQ(*d, e) << rc::kindName(e.kind);
        EXPECT_EQ(d->nsId, 5u);
    }
    EXPECT_EQ(rc::kindName(rc::EventKind::NamespaceCreated), "NamespaceCreated");
    EXPECT_EQ(rc::kindName(rc::EventKind::NamespaceLimitsSet), "NamespaceLimitsSet");
    EXPECT_EQ(rc::kindName(rc::EventKind::NamespaceStateChanged), "NamespaceStateChanged");
    EXPECT_EQ(rc::kindName(rc::EventKind::NamespaceDeleted), "NamespaceDeleted");
}

TEST(CatalogV2Regression, NamespaceCreatedBodyLayoutRegression) {
    // body = str16 name | NamespaceLimits (256 bytes) ; ops tag byte == kind (32).
    const rc::CatalogEvent e = namespaceEvent(0);
    const auto enc = rc::encode(e).value();
    // the ops blob is the tail: u32 opsLen then tag 32, u16 len 7, "arena-7", 256 limit bytes.
    const std::size_t bodyLen = 1 + 2 + 7 + rp::kNamespaceLimitsBytes;
    ASSERT_GT(enc.size(), bodyLen + 4);
    const std::size_t opsAt = enc.size() - bodyLen;
    EXPECT_EQ(enc[opsAt - 4], bodyLen & 0xff);
    EXPECT_EQ(enc[opsAt], 32);
    EXPECT_EQ(enc[opsAt + 1], 7); EXPECT_EQ(enc[opsAt + 2], 0);
    EXPECT_EQ(std::string(enc.begin() + opsAt + 3, enc.begin() + opsAt + 10), "arena-7");
    rp::NamespaceLimits back;
    ASSERT_TRUE(rp::decodeLimits(enc.data() + opsAt + 10, rp::kNamespaceLimitsBytes, back));
    EXPECT_EQ(back, distinctLimits());
}

TEST(CatalogV2Regression, TruncatedAtEveryOffsetRegression) {
    std::vector<rc::CatalogEvent> all;
    for (std::size_t i = 0; i < 4; ++i) all.push_back(namespaceEvent(i));
    all.push_back(dropTopicEvent(rc::kFormatV2, 9));
    for (const rc::CatalogEvent& e : all) {
        const auto b = rc::encode(e).value();
        for (std::size_t n = 0; n < b.size(); ++n) {
            auto d = rc::decode(std::span<const uint8_t>(b.data(), n));
            ASSERT_FALSE(d.has_value()) << rc::kindName(e.kind) << " prefix " << n << " of " << b.size();
            EXPECT_EQ(d.error(), rc::DecodeError::Truncated) << rc::kindName(e.kind) << " prefix " << n;
        }
    }
}

TEST(CatalogV2Regression, KindRangeByFormatVerRegression) {
    // v2 accepts 32..35 and refuses 36..63 (ReservedKind); a kind outside both stays UnknownKind.
    for (uint16_t k = 36; k <= 63; ++k) {
        auto b = rc::encode(namespaceEvent(3)).value();
        b[2] = static_cast<uint8_t>(k); b[3] = 0;
        auto d = rc::decode(b);
        ASSERT_FALSE(d.has_value()) << k;
        EXPECT_EQ(d.error(), rc::DecodeError::ReservedKind) << k;
        rc::CatalogEvent e = namespaceEvent(3);
        e.kind = static_cast<rc::EventKind>(k);
        auto r = rc::encode(e);
        ASSERT_FALSE(r.has_value()) << k;
        EXPECT_EQ(r.error(), rc::EncodeError::ReservedKind) << k;
    }
    for (uint16_t k : {0, 10, 31, 64}) {
        auto b = rc::encode(namespaceEvent(3)).value();
        b[2] = static_cast<uint8_t>(k); b[3] = 0;
        auto d = rc::decode(b);
        ASSERT_FALSE(d.has_value()) << k;
        EXPECT_EQ(d.error(), rc::DecodeError::UnknownKind) << k;
    }
    // a bad NsStateTag byte is a BadEnum, both ways
    auto b = rc::encode(namespaceEvent(2)).value();
    b.back() = 4;
    auto d = rc::decode(b);
    ASSERT_FALSE(d.has_value());
    EXPECT_EQ(d.error(), rc::DecodeError::BadEnum);
    rc::CatalogEvent bad = namespaceEvent(2);
    std::get<rc::NamespaceStateChangedOps>(bad.ops).to = static_cast<rc::NsStateTag>(4);
    auto r = rc::encode(bad);
    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error(), rc::EncodeError::BadEnum);
}

// ── formatVer 1 stays frozen ──────────────────────────────────────────────────
TEST(CatalogV1FrozenRegression, RefusesNamespaceKindsRegression) {
    // A formatVer-1 frame with kind 32..35 is refused by checkKind (ReservedKind), by decode and by encode.
    for (uint16_t k = 32; k <= 35; ++k) {
        auto b = rc::encode(dropTopicEvent(rc::kFormatV1, 0)).value();
        b[2] = static_cast<uint8_t>(k);
        auto d = rc::decode(b);
        ASSERT_FALSE(d.has_value()) << k;
        EXPECT_EQ(d.error(), rc::DecodeError::ReservedKind) << k;
    }
    rc::CatalogEvent e = namespaceEvent(0, 0);
    e.formatVer = rc::kFormatV1;
    auto r = rc::encode(e);
    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error(), rc::EncodeError::ReservedKind);
}

TEST(CatalogV1FrozenRegression, V1GoldenStillReplaysRegression) {
    std::ifstream in(RUNE_CATALOG_GOLDEN_DIR "/v1/basic.rcat", std::ios::binary);
    ASSERT_TRUE(in) << "missing golden log";
    auto records = rc::readRcat(in);
    ASSERT_TRUE(records.has_value()) << records.error();
    ASSERT_EQ(records->size(), std::size(rc::golden::kBasicV1Digests));
    rc::CatalogReplayer r;
    for (std::size_t i = 0; i < records->size(); ++i) {
        auto e = rc::decode((*records)[i].second);
        ASSERT_TRUE(e.has_value()) << i;
        EXPECT_EQ(e->formatVer, rc::kFormatV1) << i;
        EXPECT_EQ(e->nsId, 0u) << i;
        auto a = r.feedDecoded((*records)[i].first, *e);
        ASSERT_TRUE(a.has_value()) << rc::describe(a.error());
        EXPECT_EQ(r.state().digest(), rc::golden::kBasicV1Digests[i]) << "offset " << i;
    }
}

// ── fingerprints ──────────────────────────────────────────────────────────────
TEST(CatalogV2Regression, FingerprintV2Regression) {
    const uint64_t v1 = 0x0123456789abcdefull;
    EXPECT_NE(rc::objectFingerprintV2(v1, 0), rc::objectFingerprintV2(v1, 1));
    EXPECT_NE(rc::objectFingerprintV2(v1, 1), rc::objectFingerprintV2(v1 + 1, 1));
    EXPECT_EQ(rc::objectFingerprintV2(v1, 7), rc::objectFingerprintV2(v1, 7));
    const rp::NamespaceLimits l = distinctLimits();
    const uint64_t base = rc::nsFingerprint(1, "a", l, rc::NsStateTag::Active);
    EXPECT_NE(base, rc::nsFingerprint(2, "a", l, rc::NsStateTag::Active));
    EXPECT_NE(base, rc::nsFingerprint(1, "b", l, rc::NsStateTag::Active));
    EXPECT_NE(base, rc::nsFingerprint(1, "a", l, rc::NsStateTag::Deleting));
    rp::NamespaceLimits l2 = l; l2.maxTopics += 1;
    EXPECT_NE(base, rc::nsFingerprint(1, "a", l2, rc::NsStateTag::Active));
    rp::NamespaceLimits l3 = l; l3.reserved3 = 1;
    EXPECT_NE(base, rc::nsFingerprint(1, "a", l3, rc::NsStateTag::Active));
}

// ── NsName.h ──────────────────────────────────────────────────────────────────
TEST(NsNameRegression, ParseQualifiedTableRegression) {
    struct Row { const char* in; bool ok; const char* ns; const char* local; };
    const Row rows[] = {
        {"t", true, "", "t"},                 {"a/t", true, "a", "t"},          {"default/t", true, "default", "t"},
        {"arena-7/players", true, "arena-7", "players"},                         {"x/__catalog", true, "x", "__catalog"},
        {"/t", false, "", ""},                {"a/", false, "", ""},            {"a/b/c", false, "", ""},
        {"/", false, "", ""},                 {"", false, "", ""},              {"//", false, "", ""},
        {"a//b", false, "", ""},              {"a/b/", false, "", ""},          {"/a/b", false, "", ""},
        {"players.v2", true, "", "players.v2"}, {"A/t", true, "A", "t"},        {"a/T", true, "a", "T"},
        {"a b/t", true, "a b", "t"},          {"a/b c", true, "a", "b c"},      {"0/0", true, "0", "0"},
        {"a/b/c/d", false, "", ""},           {"ns/", false, "", ""},           {"/ns", false, "", ""},
        {"d/e", true, "d", "e"},
    };
    ASSERT_EQ(std::size(rows), 24u);
    for (const Row& r : rows) {
        const auto q = rc::parseQualified(r.in);
        if (!r.ok) {
            ASSERT_FALSE(q.has_value()) << "'" << r.in << "'";
            EXPECT_EQ(q.error(), rc::QualifyError::Malformed) << "'" << r.in << "'";
            continue;
        }
        ASSERT_TRUE(q.has_value()) << "'" << r.in << "'";
        EXPECT_EQ(q->ns, r.ns) << "'" << r.in << "'";
        EXPECT_EQ(q->local, r.local) << "'" << r.in << "'";
    }
}

TEST(NsNameRegression, ValidNamespaceNameTableRegression) {
    const std::string n63(63, 'a'), n64(64, 'a');
    struct Row { std::string in; bool ok; };
    const Row rows[] = {
        {"a", true}, {"0", true}, {"a-b", true}, {n63, true}, {"a0-9z", true}, {"9", true}, {"abc", true},
        {"x-", true}, {"default1", true}, {"my-ns", true}, {"defaul", true},
        {"-a", false}, {"A", false}, {"a_b", false}, {n64, false}, {"default", false}, {"", false}, {"__x", false},
        {"a/b", false}, {"a.b", false}, {"aB", false}, {" a", false},
    };
    ASSERT_GE(std::size(rows), 20u);
    for (const Row& r : rows) EXPECT_EQ(rc::validNamespaceName(r.in), r.ok) << "'" << r.in << "'";
    EXPECT_EQ(rc::kDefaultNsId, 0u);
    EXPECT_EQ(rc::kDefaultNsName, "default");
}

// ════════════════════════════════════════════════════════════════════════════════════════════════════════════
// Task 3: NamespaceRegistry, (nsId, name) keys, ApplyV2
// ════════════════════════════════════════════════════════════════════════════════════════════════════════════
#include <rune/catalog/CatalogFingerprint.h>
#include <rune/catalog/CatalogState.h>
#include <rune/catalog/NamespaceRegistry.h>
#include <rune/catalog/testing/GoldenScript.h>
#include <rune/codec/Descriptor.h>
#include <rune/codec/Serialize.h>

#include <variant>

namespace cd = rune::codec;

namespace {

using Res = std::expected<rc::Applied, rc::ApplyError>;

/// The leader's way of building an event (stamp the previewed fingerprint, then apply), over formatVer 2.
struct Rig {
    rc::CatalogState s;
    int64_t          off = 0;

    Res go(rc::CatalogEvent e) {
        auto fp = rc::previewFingerprint(s, e);
        if (!fp) return std::unexpected(fp.error());
        e.fingerprint = *fp;
        auto a = rc::apply(s, e, off);
        if (a) ++off;
        return a;
    }
    static rc::CatalogEvent base(rc::Ops ops, std::string name, uint32_t nsId, uint64_t objectId, uint32_t prev) {
        rc::CatalogEvent e;
        e.formatVer = rc::kFormatV2;
        e.nsId = nsId;
        e.kind = rc::kindOf(ops);
        e.ops = std::move(ops);
        e.objectName = std::move(name);
        e.objectId = objectId;
        e.prevVersion = prev;
        e.newVersion = prev + 1;
        return e;
    }
    // ── namespace events ─────────────────────────────────────────────────────────────────────────────────────
    Res nsCreateAs(uint32_t nsId, const std::string& name, rp::NamespaceLimits l = {}) {
        return go(base(rc::NamespaceCreatedOps{name, l}, name, nsId, 0, 0));
    }
    Res nsCreate(const std::string& name, rp::NamespaceLimits l = {}) {
        return nsCreateAs(s.namespaces().nextNsId(), name, l);
    }
    Res nsLimits(uint32_t nsId, const rp::NamespaceLimits& l) {
        const rc::NamespaceRecord* r = s.namespaces().byId(nsId);
        return go(base(rc::NamespaceLimitsSetOps{l}, r ? r->name : "?", nsId, 0, r ? r->version : 0));
    }
    Res nsState(uint32_t nsId, rc::NsStateTag to) {
        const rc::NamespaceRecord* r = s.namespaces().byId(nsId);
        return go(base(rc::NamespaceStateChangedOps{to}, r ? r->name : "?", nsId, 0, r ? r->version : 0));
    }
    Res nsDelete(uint32_t nsId) {
        const rc::NamespaceRecord* r = s.namespaces().byId(nsId);
        return go(base(rc::NamespaceDeletedOps{}, r ? r->name : "?", nsId, 0, r ? r->version : 0));
    }
    // ── object events ────────────────────────────────────────────────────────────────────────────────────────
    Res topic(uint32_t nsId, const std::string& name, int32_t partitions = 1) {
        rc::CreateTopicOps o;
        o.topicId = s.nextTopicId();
        o.partitions = partitions;
        return go(base(std::move(o), name, nsId, s.nextObjectId(), 0));
    }
    Res typedTopic(uint32_t nsId, const std::string& name) {
        rc::CreateTopicOps o;
        o.topicId = s.nextTopicId();
        cd::DescriptorBuilder d(name);
        d.add("id", cd::PrimType::Int64, 1, true).add("hp", cd::PrimType::Int32);
        o.schema = rc::SchemaDef{.keyField = "id", .validate = true, .descriptorBlob = cd::serialize(d.build()), .nextTag = 2};
        return go(base(std::move(o), name, nsId, s.nextObjectId(), 0));
    }
    Res dropTopic(uint32_t nsId, const std::string& name) {
        const rc::TopicObject* t = s.topicByName(nsId, name);
        return go(base(rc::DropTopicOps{t ? t->topicId : -1}, name, nsId, t ? t->objectId : 0, t ? t->version : 0));
    }
    Res view(uint32_t nsId, const std::string& name, std::vector<std::string> sources, int32_t sinkPartitions = 1) {
        cd::DescriptorBuilder out(name);
        out.add("id", cd::PrimType::Int64, 1, true);
        rc::ViewDef v;
        v.viewTopicId = s.nextTopicId();
        v.sinkPartitions = sinkPartitions;
        v.keyField = "id";
        v.sourceNames = std::move(sources);
        v.normalizedPlanBlob = {1, 2, 3};
        v.compiledPlanBlob = {9};
        v.outputDescriptorBlob = cd::serialize(out.build());
        return go(base(rc::CreateViewOps{std::move(v)}, name, nsId, s.nextObjectId(), 0));
    }
    Res createSchemaOn(uint32_t evNsId, const rc::TopicObject& t) {   // evNsId may disagree with t's namespace
        cd::DescriptorBuilder d(t.name);
        d.add("id", cd::PrimType::Int64, 1, true);
        return go(base(rc::CreateSchemaOps{rc::SchemaDef{.keyField = "id", .validate = false,
                                                         .descriptorBlob = cd::serialize(d.build()), .nextTag = 1}},
                       t.name, evNsId, t.objectId, t.version));
    }
};

rp::NamespaceLimits limitsWith(uint64_t maxTopics) {
    rp::NamespaceLimits l;
    l.maxTopics = maxTopics;
    l.maxViews = maxTopics * 2;
    l.featureBits = 0x3;
    return l;
}
void expectRefused(const Res& r, rc::ApplyError::Code code, const std::string& what) {
    ASSERT_FALSE(r.has_value()) << what << ": expected a refusal";
    EXPECT_EQ(r.error().code, code) << what << ": " << r.error().detail;
}
} // namespace

TEST(NamespaceCatalogV2Regression, DefaultPredefinedRegression) {
    rc::CatalogState s;
    const rc::NamespaceRecord* d = s.namespaces().byId(0);
    ASSERT_NE(d, nullptr);
    EXPECT_EQ(d->name, "default");
    EXPECT_TRUE(std::holds_alternative<rc::NsActive>(d->state));
    EXPECT_EQ(s.namespaces().byName("default"), d);
    EXPECT_EQ(s.namespaces().nextNsId(), 1u);
    EXPECT_EQ(s.namespaces().liveCount(), 1u);
    EXPECT_EQ(s.namespaces().byId(1), nullptr);
    // the boot-resolved cluster default limits are carried by the predefined record
    const rc::CatalogState withDefaults{limitsWith(77)};
    EXPECT_EQ(withDefaults.namespaces().byId(0)->limits.maxTopics, 77u);
    // the fresh state's digest does not depend on the default namespace record (v1 digests stay valid)
    EXPECT_EQ(rc::CatalogState{}.digest(), rc::CatalogState{limitsWith(5)}.digest());
}

TEST(NamespaceCatalogV2Regression, CreateAssignsNextIdRegression) {
    Rig r;
    ASSERT_TRUE(r.nsCreateAs(1, "a").has_value());
    EXPECT_EQ(r.s.namespaces().nextNsId(), 2u);
    expectRefused(r.nsCreateAs(3, "b"), rc::ApplyError::Code::BadNsId, "Created(3, b) skips an id");
    expectRefused(r.nsCreateAs(2, "a"), rc::ApplyError::Code::Precondition, "Created(2, a) reuses a name");
    expectRefused(r.nsCreateAs(1, "b"), rc::ApplyError::Code::BadNsId, "Created(1, b) repeats an id");
    expectRefused(r.nsCreateAs(0, "b"), rc::ApplyError::Code::BadNsId, "Created(0, b) is the default's id");
    ASSERT_TRUE(r.nsCreateAs(2, "b").has_value());
    EXPECT_EQ(r.s.namespaces().liveCount(), 3u);
    EXPECT_EQ(r.s.namespaces().byName("b")->nsId, 2u);
    EXPECT_EQ(r.s.namespaces().byId(1)->version, 1u);
}

TEST(NamespaceCatalogV2Regression, CreateDefaultRefusedRegression) {
    Rig r;
    expectRefused(r.nsCreateAs(1, "default"), rc::ApplyError::Code::Precondition, "Created(1, default)");
    expectRefused(r.nsCreateAs(0, "default"), rc::ApplyError::Code::BadNsId, "Created(0, default)");
    for (const char* bad : {"", "A", "-x", "a/b", "a_b", "__x"})
        expectRefused(r.nsCreateAs(1, bad), rc::ApplyError::Code::Precondition, std::string("invalid name '") + bad + "'");
    EXPECT_EQ(r.s.namespaces().liveCount(), 1u);
    EXPECT_EQ(r.s.namespaces().nextNsId(), 1u);
}

TEST(NamespaceCatalogV2Regression, NsIdNeverReusedRegression) {
    Rig r;
    ASSERT_TRUE(r.nsCreate("one").has_value());   // 1
    ASSERT_TRUE(r.nsCreate("two").has_value());   // 2
    ASSERT_TRUE(r.nsState(2, rc::NsStateTag::Deleting).has_value());
    ASSERT_TRUE(r.nsDelete(2).has_value());
    EXPECT_EQ(r.s.namespaces().nextNsId(), 3u) << "survives Deleted";
    EXPECT_EQ(r.s.namespaces().byId(2), nullptr);
    EXPECT_EQ(r.s.namespaces().byName("two"), nullptr);
    EXPECT_EQ(r.s.namespaces().liveCount(), 2u);   // default + one
    expectRefused(r.nsCreateAs(2, "c"), rc::ApplyError::Code::BadNsId, "Created(2, c) reuses a deleted id");
    ASSERT_TRUE(r.nsCreateAs(3, "two").has_value()) << "the NAME is free again, the id is not";
    EXPECT_EQ(r.s.namespaces().byName("two")->nsId, 3u);
    EXPECT_EQ(r.s.namespaces().nextNsId(), 4u);
}

TEST(NamespaceCatalogV2Regression, StateEdgesTableRegression) {
    using T = rc::NsStateTag;
    struct Row { T from; T to; bool ok; };
    const T states[] = {T::Active, T::SuspendedReadOnly, T::SuspendedRejectAll, T::Deleting};
    // allowed (D5): ACTIVE <-> SUSPENDED(mode), ACTIVE | SUSPENDED -> DELETING
    auto allowed = [](T from, T to) {
        if (from == T::Deleting) return false;
        if (from == T::Active) return to != T::Active;
        return to == T::Active || to == T::Deleting;
    };
    int rows = 0;
    for (const T from : states) {
        for (const T to : states) {
            Rig r;
            ASSERT_TRUE(r.nsCreate("n").has_value());
            if (from != T::Active) ASSERT_TRUE(r.nsState(1, from).has_value());
            const uint64_t before = r.s.digest();
            const auto res = r.nsState(1, to);
            const std::string what = "state " + std::to_string(static_cast<int>(from)) + " -> " + std::to_string(static_cast<int>(to));
            if (allowed(from, to)) {
                EXPECT_TRUE(res.has_value()) << what;
            } else {
                expectRefused(res, rc::ApplyError::Code::Precondition, what);
                EXPECT_EQ(r.s.digest(), before) << what << ": a refusal changes nothing";
            }
            ++rows;
        }
        // Deleted from `from`: only a DELETING namespace may be deleted
        Rig r;
        ASSERT_TRUE(r.nsCreate("n").has_value());
        if (from != T::Active) ASSERT_TRUE(r.nsState(1, from).has_value());
        const auto res = r.nsDelete(1);
        if (from == T::Deleting) {
            EXPECT_TRUE(res.has_value()) << "Deleted from Deleting";
        } else {
            expectRefused(res, rc::ApplyError::Code::Precondition, "Deleted from state " + std::to_string(static_cast<int>(from)));
            EXPECT_NE(r.s.namespaces().byId(1), nullptr);
        }
        ++rows;
    }
    EXPECT_EQ(rows, 20);
    // nsId 0 is never deleted (and never enters DELETING, so it can never be deleted)
    Rig r;
    expectRefused(r.nsDelete(0), rc::ApplyError::Code::Precondition, "Deleted(0)");
    expectRefused(r.nsState(0, T::Deleting), rc::ApplyError::Code::Precondition, "StateChanged(0, Deleting)");
    // a state change or delete of a namespace that does not exist
    expectRefused(r.nsState(9, T::SuspendedReadOnly), rc::ApplyError::Code::BadNsId, "StateChanged(9)");
    expectRefused(r.nsDelete(9), rc::ApplyError::Code::BadNsId, "Deleted(9)");
}

TEST(NamespaceCatalogV2Regression, LimitsSetRegression) {
    Rig r;
    ASSERT_TRUE(r.nsCreate("a", limitsWith(5)).has_value());
    const uint64_t fpBefore = r.s.namespaces().byId(1)->fingerprint;
    ASSERT_TRUE(r.nsLimits(1, limitsWith(9)).has_value());
    const rc::NamespaceRecord* rec = r.s.namespaces().byId(1);
    EXPECT_EQ(rec->limits, limitsWith(9)) << "the FULL record is replaced";
    EXPECT_EQ(rec->version, 2u) << "the version bumps";
    EXPECT_NE(rec->fingerprint, fpBefore);
    EXPECT_EQ(rec->fingerprint, rc::nsFingerprint(1, "a", limitsWith(9), rc::NsStateTag::Active));
    expectRefused(r.nsLimits(5, limitsWith(1)), rc::ApplyError::Code::BadNsId, "LimitsSet on a missing namespace");
    // the default namespace's limits can be changed (D1) and then the digest reflects it
    const uint64_t d0 = r.s.digest();
    ASSERT_TRUE(r.nsLimits(0, limitsWith(3)).has_value());
    EXPECT_EQ(r.s.namespaces().byId(0)->limits, limitsWith(3));
    EXPECT_NE(r.s.digest(), d0);
    // a stale version chain is refused
    auto stale = Rig::base(rc::NamespaceLimitsSetOps{limitsWith(4)}, "a", 1, 0, 1);
    stale.fingerprint = 0;
    EXPECT_FALSE(rc::previewFingerprint(r.s, stale).has_value());
}

TEST(NamespaceCatalogV2Regression, SameNameTwoNamespacesRegression) {
    Rig r;
    ASSERT_TRUE(r.nsCreate("a").has_value());
    ASSERT_TRUE(r.typedTopic(0, "t").has_value());
    ASSERT_TRUE(r.typedTopic(1, "t").has_value());
    ASSERT_TRUE(r.topic(0, "u").has_value());
    ASSERT_TRUE(r.topic(1, "u").has_value());
    ASSERT_TRUE(r.createSchemaOn(0, *r.s.topicByName(0, "u")).has_value());   // a schema "s" on u, in both
    ASSERT_TRUE(r.createSchemaOn(1, *r.s.topicByName(1, "u")).has_value());
    ASSERT_TRUE(r.view(0, "v", {"t"}).has_value());
    ASSERT_TRUE(r.view(1, "v", {"t"}).has_value());
    const rc::TopicObject* t0 = r.s.topicByName(0, "t");
    const rc::TopicObject* t1 = r.s.topicByName(1, "t");
    ASSERT_NE(t0, nullptr); ASSERT_NE(t1, nullptr);
    EXPECT_NE(t0, t1);
    EXPECT_NE(t0->objectId, t1->objectId);
    EXPECT_NE(t0->topicId, t1->topicId);
    EXPECT_EQ(t0->nsId, 0u); EXPECT_EQ(t1->nsId, 1u);
    EXPECT_TRUE(r.s.nameTaken(0, "v")); EXPECT_TRUE(r.s.nameTaken(1, "v"));
    EXPECT_NE(r.s.viewByName(0, "v"), r.s.viewByName(1, "v"));
    EXPECT_EQ(r.s.viewByName(1, "v")->nsId, 1u);
    // same definition, different namespace: the v2 object fingerprints differ
    EXPECT_EQ(rc::fingerprintOf(*t0), rc::fingerprintOf(*t1));
    EXPECT_NE(rc::objectFingerprintV2(rc::fingerprintOf(*t0), t0->nsId), rc::objectFingerprintV2(rc::fingerprintOf(*t1), t1->nsId));
    // a view reads ITS namespace's source, and a topic is read only by its own namespace's views
    EXPECT_EQ(r.s.viewReading(1, "t"), r.s.viewByName(1, "v"));
    EXPECT_EQ(r.s.viewReading(0, "t"), r.s.viewByName(0, "v"));
    expectRefused(r.dropTopic(1, "t"), rc::ApplyError::Code::Precondition, "ns-1 t is read by ns-1 v");
    ASSERT_NE(r.s.topicByName(1, "t"), nullptr);
    // dropping ns 1's v then t leaves ns 0's untouched
    {
        const rc::ViewObject* v1 = r.s.viewByName(1, "v");
        ASSERT_TRUE(r.go(Rig::base(rc::DropViewOps{v1->def.viewTopicId}, "v", 1, v1->objectId, v1->version)).has_value());
    }
    ASSERT_TRUE(r.dropTopic(1, "t").has_value());
    EXPECT_EQ(r.s.topicByName(1, "t"), nullptr);
    EXPECT_NE(r.s.topicByName(0, "t"), nullptr);
    EXPECT_NE(r.s.viewByName(0, "v"), nullptr);
    // the freed name can be taken again in ns 1, and is a different object
    ASSERT_TRUE(r.topic(1, "t").has_value());
    // a name that case-folds onto a live name is a conflict only within ONE namespace
    EXPECT_TRUE(r.topic(1, "T").has_value() == false);
    ASSERT_TRUE(r.nsCreate("b").has_value());
    EXPECT_TRUE(r.topic(2, "T").has_value()) << "ns 2 has no t";
    // the tombstone remembers its namespace
    ASSERT_FALSE(r.s.tombstones().empty());
    EXPECT_EQ(r.s.tombstones().back().nsId, 1u);
}

TEST(NamespaceCatalogV2Regression, ObjectInMissingOrDeletingNsRefusedRegression) {
    Rig r;
    expectRefused(r.topic(9, "t"), rc::ApplyError::Code::BadNsId, "CreateTopic in a missing namespace");
    expectRefused(r.view(9, "v", {"t"}), rc::ApplyError::Code::BadNsId, "CreateView in a missing namespace");
    ASSERT_TRUE(r.nsCreate("a").has_value());
    ASSERT_TRUE(r.topic(1, "t").has_value());
    ASSERT_TRUE(r.nsState(1, rc::NsStateTag::SuspendedReadOnly).has_value());
    expectRefused(r.topic(1, "u"), rc::ApplyError::Code::Precondition, "CreateTopic in a suspended namespace");
    expectRefused(r.dropTopic(1, "t"), rc::ApplyError::Code::Precondition, "DropTopic in a suspended namespace");
    ASSERT_TRUE(r.nsState(1, rc::NsStateTag::Active).has_value());
    ASSERT_TRUE(r.topic(1, "u").has_value());
    ASSERT_TRUE(r.nsState(1, rc::NsStateTag::Deleting).has_value());
    expectRefused(r.topic(1, "w"), rc::ApplyError::Code::Precondition, "CreateTopic in a deleting namespace");
    expectRefused(r.dropTopic(1, "u"), rc::ApplyError::Code::Precondition, "DropTopic in a deleting namespace");
    ASSERT_TRUE(r.nsDelete(1).has_value());
    expectRefused(r.topic(1, "w"), rc::ApplyError::Code::BadNsId, "CreateTopic in a deleted namespace");
    EXPECT_TRUE(r.topic(0, "w").has_value()) << "the default namespace is unaffected";
}

TEST(NamespaceCatalogV2Regression, CrossNsReferenceRefusedRegression) {
    Rig r;
    ASSERT_TRUE(r.nsCreate("a").has_value());
    ASSERT_TRUE(r.topic(0, "src0").has_value());
    ASSERT_TRUE(r.topic(1, "src1").has_value());
    // a view in ns 1 over a name that exists only in ns 0
    expectRefused(r.view(1, "bad", {"src0"}), rc::ApplyError::Code::Precondition, "foreign source");
    expectRefused(r.view(1, "bad", {"src1", "src0"}), rc::ApplyError::Code::Precondition, "foreign second source");
    EXPECT_EQ(r.s.viewByName(1, "bad"), nullptr);
    // the same with the source in ns 1
    ASSERT_TRUE(r.view(1, "good", {"src1"}).has_value());
    // and the reverse direction
    expectRefused(r.view(0, "bad", {"src1"}), rc::ApplyError::Code::Precondition, "ns 0 view over an ns-1 source");
    // a schema / drop / alter event of ns 1 naming an ns-0 object (by its global objectId) is refused
    const rc::TopicObject* foreign = r.s.topicByName(0, "src0");
    ASSERT_NE(foreign, nullptr);
    expectRefused(r.createSchemaOn(1, *foreign), rc::ApplyError::Code::Precondition, "CreateSchema on a foreign topic");
    expectRefused(r.go(Rig::base(rc::DropTopicOps{foreign->topicId}, "src0", 1, foreign->objectId, foreign->version)),
                  rc::ApplyError::Code::Precondition, "DropTopic of a foreign topic");
    expectRefused(r.go(Rig::base(rc::AlterTopicConfigOps{{{rc::ConfigKey::RetentionMs, 5}}}, "src0", 1, foreign->objectId,
                                 foreign->version)),
                  rc::ApplyError::Code::Precondition, "AlterTopicConfig of a foreign topic");
    EXPECT_NE(r.s.topicByName(0, "src0"), nullptr);
    EXPECT_EQ(r.s.topicByName(0, "src0")->version, 1u);
}

TEST(NamespaceCatalogV2Regression, V1KindsApplyIdenticallyRegression) {
    std::ifstream in(RUNE_CATALOG_GOLDEN_DIR "/v1/basic.rcat", std::ios::binary);
    ASSERT_TRUE(in);
    auto records = rc::readRcat(in);
    ASSERT_TRUE(records.has_value());
    rc::CatalogState v1, v2;
    int64_t off = 0;
    for (const auto& [o, bytes] : *records) {
        auto e1 = rc::decode(bytes);
        ASSERT_TRUE(e1.has_value());
        ASSERT_TRUE(rc::apply(v1, *e1, o).has_value()) << "v1 offset " << o;
        // the same event re-encoded as formatVer 2 with nsId 0 (the fingerprint is the v2 one)
        rc::CatalogEvent e2 = *e1;
        e2.formatVer = rc::kFormatV2;
        e2.nsId = 0;
        e2.fingerprint = rc::objectFingerprintV2(e1->fingerprint, 0);
        auto round = rc::decode(rc::encode(e2).value());
        ASSERT_TRUE(round.has_value());
        ASSERT_EQ(*round, e2);
        auto a2 = rc::apply(v2, *round, o);
        ASSERT_TRUE(a2.has_value()) << "v2 offset " << o << ": " << a2.error().detail;
        EXPECT_EQ(v2.digest(), v1.digest()) << "offset " << o;
        off = o;
    }
    EXPECT_EQ(off, 22);
    // the object sets are equal, object for object
    ASSERT_EQ(v1.topics().size(), v2.topics().size());
    for (std::size_t i = 0; i < v1.topics().size(); ++i) EXPECT_EQ(*v1.topics()[i], *v2.topics()[i]);
    ASSERT_EQ(v1.views().size(), v2.views().size());
    for (std::size_t i = 0; i < v1.views().size(); ++i) EXPECT_EQ(*v1.views()[i], *v2.views()[i]);
    EXPECT_EQ(v1.tombstones(), v2.tombstones());
    EXPECT_EQ(v1.nextObjectId(), v2.nextObjectId());
    EXPECT_EQ(v1.nextTopicId(), v2.nextTopicId());
}

TEST(NamespaceCatalogV2Regression, PreviewFingerprintV2Regression) {
    Rig r;
    // every namespace kind and some object kinds: preview == what apply accepts, and preview never mutates
    auto check = [&](rc::CatalogEvent e, const std::string& what) {
        const uint64_t before = r.s.digest();
        auto p = rc::previewFingerprint(r.s, e);
        ASSERT_TRUE(p.has_value()) << what << ": " << p.error().detail;
        EXPECT_EQ(r.s.digest(), before) << what << ": preview mutated the state";
        EXPECT_EQ(*p, rc::previewFingerprint(r.s, e).value()) << what << ": repeatable";
        e.fingerprint = *p ^ 1;
        auto bad = rc::apply(r.s, e, r.off);
        ASSERT_FALSE(bad.has_value()) << what;
        EXPECT_EQ(bad.error().code, rc::ApplyError::Code::FingerprintMismatch) << what;
        EXPECT_EQ(bad.error().expected, *p ^ 1);
        EXPECT_EQ(bad.error().actual, *p) << what << ": apply recomputes what preview said";
        e.fingerprint = *p;
        ASSERT_TRUE(rc::apply(r.s, e, r.off++).has_value()) << what;
    };
    check(Rig::base(rc::NamespaceCreatedOps{"a", limitsWith(2)}, "a", 1, 0, 0), "Created");
    check(Rig::base(rc::NamespaceLimitsSetOps{limitsWith(3)}, "a", 1, 0, 1), "LimitsSet");
    check(Rig::base(rc::NamespaceStateChangedOps{rc::NsStateTag::SuspendedRejectAll}, "a", 1, 0, 2), "StateChanged");
    check(Rig::base(rc::NamespaceStateChangedOps{rc::NsStateTag::Deleting}, "a", 1, 0, 3), "StateChanged->Deleting");
    check(Rig::base(rc::NamespaceDeletedOps{}, "a", 1, 0, 4), "Deleted");
    rc::CreateTopicOps o;
    o.topicId = 0;
    check(Rig::base(o, "t", 0, 1, 0), "CreateTopic ns 0");
    // preview refuses where apply refuses
    EXPECT_FALSE(rc::previewFingerprint(r.s, Rig::base(rc::NamespaceCreatedOps{"a", {}}, "a", 9, 0, 0)).has_value());
    EXPECT_FALSE(rc::previewFingerprint(r.s, Rig::base(rc::NamespaceDeletedOps{}, "a", 1, 0, 0)).has_value());
    // an event naming a namespace the v1 rules cannot express is refused under formatVer 1
    rc::CatalogEvent v1 = Rig::base(o, "u", 0, 2, 0);
    v1.formatVer = rc::kFormatV1;
    v1.nsId = 3;
    EXPECT_FALSE(rc::previewFingerprint(r.s, v1).has_value());
}

TEST(NamespaceCatalogV2Regression, DigestTracksNamespacesRegression) {
    Rig a, b;
    ASSERT_TRUE(a.nsCreate("x").has_value());
    ASSERT_TRUE(b.nsCreate("y").has_value());
    EXPECT_NE(a.s.digest(), b.s.digest());
    Rig c, d;
    ASSERT_TRUE(c.nsCreate("x", limitsWith(1)).has_value());
    ASSERT_TRUE(d.nsCreate("x", limitsWith(2)).has_value());
    EXPECT_NE(c.s.digest(), d.s.digest());
    // create then delete leaves a trace (the id is never reused) in the digest
    Rig e;
    const uint64_t empty = e.s.digest();
    ASSERT_TRUE(e.nsCreate("x").has_value());
    ASSERT_TRUE(e.nsState(1, rc::NsStateTag::Deleting).has_value());
    ASSERT_TRUE(e.nsDelete(1).has_value());
    EXPECT_NE(e.s.digest(), empty);
    // same topic name in two namespaces is a different state from two topics in one
    Rig f, g;
    ASSERT_TRUE(f.nsCreate("x").has_value()); ASSERT_TRUE(g.nsCreate("x").has_value());
    ASSERT_TRUE(f.topic(0, "t").has_value()); ASSERT_TRUE(g.topic(1, "t").has_value());
    EXPECT_NE(f.s.digest(), g.s.digest());
}

TEST(NamespaceCatalogV2Regression, RequestIdDedupeSpansNamespaceKindsRegression) {
    Rig r;
    auto e = Rig::base(rc::NamespaceCreatedOps{"a", {}}, "a", 1, 0, 0);
    e.principal.requestId = rc::RequestId{7, 8};
    e.principal.requestFingerprint = 99;
    ASSERT_TRUE(r.go(e).has_value());
    const auto rec = r.s.request(rc::RequestId{7, 8});
    ASSERT_TRUE(rec.has_value());
    EXPECT_EQ(rec->kind, rc::EventKind::NamespaceCreated);
    EXPECT_EQ(rec->requestFingerprint, 99u);
    auto again = Rig::base(rc::NamespaceCreatedOps{"b", {}}, "b", 2, 0, 0);
    again.principal.requestId = rc::RequestId{7, 8};
    expectRefused(r.go(again), rc::ApplyError::Code::Precondition, "a request id applied twice");
}

TEST(CatalogV2GoldenRegression, NamespacesGoldenPinnedRegression) {
    std::ifstream in(RUNE_CATALOG_GOLDEN_DIR "/v2/namespaces.rcat", std::ios::binary);
    ASSERT_TRUE(in) << "missing golden log";
    auto records = rc::readRcat(in);
    ASSERT_TRUE(records.has_value()) << records.error();
    ASSERT_EQ(records->size(), std::size(rc::golden::kNamespacesV2Digests));
    rc::CatalogReplayer r;
    for (std::size_t i = 0; i < records->size(); ++i) {
        auto a = r.feed((*records)[i].first, (*records)[i].second);
        ASSERT_TRUE(a.has_value()) << rc::describe(a.error());
        EXPECT_EQ(r.state().digest(), rc::golden::kNamespacesV2Digests[i]) << "offset " << i;
        if (i == 4) {   // both `t`s exist, in different namespaces
            const auto& s = r.state();
            ASSERT_EQ(s.namespaces().liveCount(), 4u) << "default + 3 namespaces";
            ASSERT_NE(s.topicByName(1, "t"), nullptr);
            ASSERT_NE(s.topicByName(2, "t"), nullptr);
            EXPECT_NE(s.topicByName(1, "t"), s.topicByName(2, "t"));
        }
    }
    const auto& s = r.state();
    EXPECT_EQ(s.namespaces().liveCount(), 3u) << "default, alpha, beta (gamma was deleted)";
    EXPECT_EQ(s.namespaces().nextNsId(), 4u);
    EXPECT_EQ(s.namespaces().byName("beta")->limits.maxTopics, 40u) << "the limits change";
    EXPECT_EQ(s.namespaces().byName("beta")->version, 2u);
}

TEST(CatalogV2GoldenRegression, TheV2GoldenScriptStillProducesTheCheckedInBytesRegression) {
    std::ifstream in(RUNE_CATALOG_GOLDEN_DIR "/v2/namespaces.rcat", std::ios::binary);
    auto records = rc::readRcat(in);
    ASSERT_TRUE(records.has_value());
    const auto script = rc::testing::goldenScriptV2();
    ASSERT_EQ(records->size(), script.size());
    for (std::size_t i = 0; i < script.size(); ++i) {
        EXPECT_EQ((*records)[i].second, rc::encode(script[i]).value()) << "offset " << i;
        auto e = rc::decode((*records)[i].second);
        ASSERT_TRUE(e.has_value());
        EXPECT_EQ(*e, script[i]) << "offset " << i;
        EXPECT_EQ(e->formatVer, rc::kFormatV2);
    }
}

// Checkpoint A F1: applyErrorName had no arm for BadNsId, so every BadNsId refusal logged as "?".
TEST(CatalogV2Regression, ApplyErrorNameCoversEveryCodeRegression) {
    using C = rc::ApplyError::Code;
    EXPECT_EQ(rc::applyErrorName(C::UnsupportedFormat), "UnsupportedFormat");
    EXPECT_EQ(rc::applyErrorName(C::Precondition), "Precondition");
    EXPECT_EQ(rc::applyErrorName(C::FingerprintMismatch), "FingerprintMismatch");
    EXPECT_EQ(rc::applyErrorName(C::BadOps), "BadOps");
    EXPECT_EQ(rc::applyErrorName(C::BadNsId), "BadNsId");
}

// Checkpoint A F6: a deleted namespace must leave a trace in the digest.  Two histories that create and delete a
// DIFFERENT name at the same nsId reach the same live state and id counter; before the tombstone fold their digests
// were equal, so a boot replay over a divergent log passed the checkpoint check.  Mutation: drop the fold line in
// CatalogState::digest() → the first EXPECT_NE turns red.
TEST(CatalogV2Regression, DeletedNamespaceNameIsInTheDigestRegression) {
    auto run = [](const std::string& name) {
        Rig r;
        EXPECT_TRUE(r.nsCreate(name).has_value());
        EXPECT_TRUE(r.nsState(1, rc::NsStateTag::Deleting).has_value());
        EXPECT_TRUE(r.nsDelete(1).has_value());
        EXPECT_EQ(r.s.namespaces().byId(1), nullptr);
        EXPECT_EQ(r.s.namespaces().nextNsId(), 2u);
        return r.s.digest();
    };
    EXPECT_NE(run("alpha"), run("beta"));
    EXPECT_EQ(run("alpha"), run("alpha"));   // deterministic: the same history always gives the same digest
}
