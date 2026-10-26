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
