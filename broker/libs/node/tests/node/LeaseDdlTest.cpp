// LeaseDdlTest.cpp — lease topics Task 3a: the admin build of a lease topic (DefineTopic -> CreateTopic event with a
// lease), against the REAL AdminWorker over a real __catalog replica (CatalogDdlRig).
//
// Interaction matrix (rows = rule, columns = the entity/timing it is crossed with):
//   | rule                           | crossed with                               | test
//   |--------------------------------|--------------------------------------------|-----
//   | every lease refusal (1,2,3.1-9)| lease-only rules vs the same non-lease     | LeaseDdlRegression.RefusalTableRegression
//   | existing name                  | topic/view/lease view name                 | RefusalTableRegression, LeaseObjectDdlRegression
//   | key width                      | 16/17/128/129, lease vs non-lease          | KeyWidthRegression
//   | system columns                 | ids, offsets, nextTag                      | SystemColumnsFirstRegression
//   | stored lease / lease view      | formatVer                                  | LeaseStoredRegression
//   | deleteRetention default        | default vs explicit                        | DeleteRetentionDefaultRegression
//   | feature level                  | 5 vs 6, lease vs non-lease                 | FeatureLevelGateRegression
//   | namespace counts               | maxTopics / maxViews                       | NamespaceCountsRegression
//   | request fingerprint            | lease / key features / none                | DedupeFingerprintRegression
//   | admin wire                     | with/without lease, bad byte, every cut    | AdminWireRoundTripRegression
//   | lease objects                  | every DDL naming the lease topic or view   | LeaseObjectDdlRegression
//   | lease view name                | too long / topic / view / case-fold       | LeaseViewNameRegression
//   | rule order                     | two broken rules per row                   | RuleOrderRegression
//   | tag exhaustion                 | user tag 0xFFFA / 0xFFFB                   | TagExhaustionRegression
#include <rune/catalog/CatalogEvent.h>
#include <rune/codec/Serialize.h>
#include <rune/protocol/LeaseSpec.h>
#include <rune/protocol/NamespaceLimits.h>

#include "support/CatalogDdlRig.h"

#include <gtest/gtest.h>

#include <string>
#include <vector>

using namespace rune::node;
using namespace rune::node::test;
using rune::codec::PrimType;
namespace rp  = rune::protocol;
namespace cat = rune::catalog;

namespace {

constexpr uint16_t kOk = 0, kTopicExists = 25, kInvalidRequest = 39;
constexpr uint16_t kInternal = 6;
constexpr uint16_t kInvalidSchema = 23, kNamespaceLimitExceeded = 52, kInvalidTopicName = 40;

rp::LeaseSpec validSpec() {
    rp::LeaseSpec s;
    s.ttlMs = 10'000; s.minTtlMs = 1'000; s.maxTtlMs = 60'000; s.maxKeys = 1; s.expiryBatch = 64;
    s.checkpoint = 0; s.checkpointIntervalMs = 1'000; s.checkpointRate = 1;
    return s;
}

/// {<keyName> key, note String} (+ extra Int64 fields).
std::vector<uint8_t> blobOf(PrimType keyType = PrimType::UInt64, uint16_t width = 0, const std::string& keyName = "id",
                            const std::vector<std::string>& extra = {}) {
    rune::codec::DescriptorBuilder b("t", 1);
    if (keyType == PrimType::FixedStr) b.addFixedStr(keyName, width, 1, true);
    else b.add(keyName, keyType, 1, true);
    b.add("note", PrimType::String);
    for (const auto& e : extra) b.add(e, PrimType::Int64);
    return rune::codec::serialize(b.build());
}

rp::TopicConfig leaseConfig() {
    rp::TopicConfig c;
    c.cleanupPolicy = 1; c.retentionMs = -1; c.retentionBytes = -1;
    return c;
}

DefineTopicMsg leaseMsg(CatalogDdlRig& rig, const std::string& name, uint32_t nsId = 0) {
    DefineTopicMsg m;
    m.session = rig.sessionId(); m.corr = rig.nextCorr(); m.name = name; m.keyField = "id"; m.blob = blobOf();
    m.config = leaseConfig(); m.lease = validSpec(); m.nsId = nsId;
    m.meta.requestId = rig.freshRequestId();
    return m;
}

CatalogDdlRig::Answer submit(CatalogDdlRig& rig, DefineTopicMsg m) { return rig.submitRaw(DdlRequest{std::move(m)}); }

void serve(CatalogDdlRig& rig, int16_t level = 6) {
    rig.becomeServingLeader();
    rig.admin().setClusterFeatureLevel(level);
}

const cat::TopicObject* topicOf(CatalogDdlRig& rig, const std::string& name, uint32_t nsId = 0) {
    return rig.admin().applier().state().topicByName(nsId, name);
}
const cat::ViewObject* viewOf(CatalogDdlRig& rig, const std::string& name, uint32_t nsId = 0) {
    return rig.admin().applier().state().viewByName(nsId, name);
}
cat::CatalogEvent lastEvent(CatalogDdlRig& rig) { return rig.recordedLog().back().event; }

}  // namespace

TEST(LeaseDdlRegression, ValidSpecIsValidRegression) {
    EXPECT_FALSE(rp::validateLeaseSpec(validSpec()).has_value());
}

// ── the refusal table ────────────────────────────────────────────────────────
namespace {

struct Row {
    const char* name;
    int16_t     level;
    uint16_t    code;
    const char* detail;
    void (*mutate)(DefineTopicMsg&);
    bool        nonLeaseAccepted;   // the same message with the lease removed is accepted
};

const Row kRows[] = {
    {"Level5", 5, kInvalidRequest, "cluster below lease feature level", [](DefineTopicMsg&) {}, true},
    {"SpecMinGtMax", 6, kInvalidRequest, "lease.min_ttl must be between 1000 and lease.max_ttl ms",
     [](DefineTopicMsg& m) { m.lease->minTtlMs = 70'000; }, true},
    {"Untyped", 6, kInvalidRequest, "a lease topic needs a typed, keyed topic",
     [](DefineTopicMsg& m) { m.blob.clear(); m.keyField.clear(); }, true},
    {"Keyless", 6, kInvalidRequest, "a lease topic needs a typed, keyed topic", [](DefineTopicMsg& m) { m.keyField.clear(); }, true},
    {"KeyNotInDescriptor", 6, kInvalidRequest, "a lease topic needs a typed, keyed topic",
     [](DefineTopicMsg& m) { m.keyField = "nope"; }, false},
    {"AutoIncrement", 6, kInvalidRequest, "a lease topic cannot use autoincrement",
     [](DefineTopicMsg& m) { m.keyFeatures.autoIncrement = true; }, false},
    {"KeyIndex", 6, kInvalidRequest, "a lease topic's key index is its lease view",
     [](DefineTopicMsg& m) { m.keyFeatures.keyIndex = true; }, false},
    {"ModuloPartitioner", 6, kInvalidRequest, "the modulo partitioner needs autoincrement",
     [](DefineTopicMsg& m) { m.keyFeatures.partitioner = rp::Partitioner::Modulo; }, false},
    {"LeftoverAutoIncMax", 6, kInvalidRequest, "autoincrement.max and autoincrement.reserve need autoincrement",
     [](DefineTopicMsg& m) { m.keyFeatures.autoIncrementMax = 5; }, false},
    {"LeftoverAutoIncReserve", 6, kInvalidRequest, "autoincrement.max and autoincrement.reserve need autoincrement",
     [](DefineTopicMsg& m) { m.keyFeatures.autoIncrementReserve = 7; }, false},
    {"Transient", 6, kInvalidRequest, "transient lease topics are not supported yet", [](DefineTopicMsg& m) { m.persistence = 1; }, true},
    {"CleanupDelete", 6, kInvalidRequest, "a lease topic is compacted and keeps every live lease",
     [](DefineTopicMsg& m) { m.config.cleanupPolicy = 0; }, true},
    {"RetentionMs", 6, kInvalidRequest, "a lease topic is compacted and keeps every live lease",
     [](DefineTopicMsg& m) { m.config.retentionMs = 1000; }, true},
    {"RetentionBytes", 6, kInvalidRequest, "a lease topic is compacted and keeps every live lease",
     [](DefineTopicMsg& m) { m.config.retentionBytes = 1000; }, true},
    {"ReservedFieldName", 6, kInvalidRequest, "lease_ field names are reserved",
     [](DefineTopicMsg& m) { m.blob = blobOf(PrimType::UInt64, 0, "id", {"lease_x"}); }, true},
};

}  // namespace

class LeaseDdlRefusal : public ::testing::TestWithParam<int> {};

TEST_P(LeaseDdlRefusal, RefusalTableRegression) {
    const Row& row = kRows[GetParam()];
    CatalogDdlRig rig("leaseref");
    serve(rig, row.level);
    DefineTopicMsg m = leaseMsg(rig, "inst");
    row.mutate(m);
    const auto a = submit(rig, m);
    EXPECT_EQ(a.code, row.code) << a.detail;
    EXPECT_EQ(a.detail, row.detail);
    EXPECT_EQ(topicOf(rig, "inst"), nullptr);
    EXPECT_EQ(viewOf(rig, "inst.leases"), nullptr);

    // The lease-only rules do not touch a topic without a lease.
    if (row.nonLeaseAccepted) {
        CatalogDdlRig rig2("leaseref2");
        serve(rig2, row.level);
        DefineTopicMsg n = leaseMsg(rig2, "plain");
        row.mutate(n);
        n.lease.reset();
        const auto b = submit(rig2, n);
        EXPECT_EQ(b.code, kOk) << row.name << ": " << b.detail;
    }
}

INSTANTIATE_TEST_SUITE_P(LeaseDdl, LeaseDdlRefusal, ::testing::Range(0, static_cast<int>(sizeof kRows / sizeof kRows[0])),
                         [](const ::testing::TestParamInfo<int>& i) { return std::string(kRows[i.param].name); });

TEST(LeaseDdlRegression, ExistingNameAnswersTopicExistsRegression) {
    CatalogDdlRig rig("leaseexists");
    serve(rig);
    ASSERT_EQ(submit(rig, leaseMsg(rig, "inst")).code, kOk);
    // A second lease define of the same name (even one that breaks a lease rule) answers TopicExists.
    DefineTopicMsg again = leaseMsg(rig, "inst");
    again.config.retentionMs = 5;
    const auto a = submit(rig, again);
    EXPECT_EQ(a.code, kTopicExists);
    EXPECT_NE(a.detail, "a lease topic is compacted and keeps every live lease");
    // A plain topic's name: lease define answers TopicExists too (typed topic).
    CatalogDdlRig::DefineArgs plain; plain.name = "plain"; plain.typed = true;
    ASSERT_EQ(rig.define(plain).code, kOk);
    EXPECT_EQ(submit(rig, leaseMsg(rig, "plain")).code, kTopicExists);
    // An UNTYPED existing topic: a lease define refuses adoption.
    CatalogDdlRig::DefineArgs untyped; untyped.name = "bare";
    ASSERT_EQ(rig.define(untyped).code, kOk);
    const auto adopt = submit(rig, leaseMsg(rig, "bare"));
    EXPECT_EQ(adopt.code, kInvalidRequest);
    EXPECT_EQ(adopt.detail, "a lease topic needs a new topic");
}

// ── key width ────────────────────────────────────────────────────────────────
TEST(LeaseDdlRegression, KeyWidthRegression) {
    CatalogDdlRig rig("leasekey");
    serve(rig);
    for (const uint16_t w : {uint16_t{17}, uint16_t{128}}) {
        DefineTopicMsg m = leaseMsg(rig, "k" + std::to_string(w));
        m.blob = blobOf(PrimType::FixedStr, w);
        const auto a = submit(rig, m);
        EXPECT_EQ(a.code, kOk) << w << ": " << a.detail;
    }
    DefineTopicMsg over = leaseMsg(rig, "k129");
    over.blob = blobOf(PrimType::FixedStr, 129);
    const auto a = submit(rig, over);
    EXPECT_EQ(a.code, kInvalidSchema);
    EXPECT_EQ(a.detail, "key field 'id' is string(129): a lease topic key is an integer or string(N) with N <= 128");

    // A NON-lease topic keeps the old width.
    DefineTopicMsg plain = leaseMsg(rig, "plain17");
    plain.lease.reset();
    plain.config = {};
    plain.blob   = blobOf(PrimType::FixedStr, 17);
    const auto b = submit(rig, plain);
    EXPECT_EQ(b.code, kInvalidSchema);
    EXPECT_EQ(b.detail, "key field 'id' is string(17): a topic key is an integer or string(N) with N <= 16");
}

// ── system columns ───────────────────────────────────────────────────────────
TEST(LeaseDdlRegression, SystemColumnsFirstRegression) {
    CatalogDdlRig rig("leasecols");
    serve(rig);
    ASSERT_EQ(submit(rig, leaseMsg(rig, "inst")).code, kOk);
    const cat::TopicObject* t = topicOf(rig, "inst");
    ASSERT_NE(t, nullptr);
    ASSERT_TRUE(t->schema);
    const rune::codec::Descriptor d = rune::codec::deserialize(t->schema->evolvingBlob);
    ASSERT_EQ(d.fields.size(), 6u);
    const char* names[] = {"lease_version", "lease_ttl_ms", "lease_holder", "lease_remaining_ms"};
    const PrimType types[] = {PrimType::Int64, PrimType::UInt64, PrimType::UInt64, PrimType::UInt64};
    const uint32_t offsets[] = {0, 8, 16, 24};
    for (uint16_t i = 0; i < 4; ++i) {
        EXPECT_EQ(d.fields[i].name, names[i]);
        EXPECT_EQ(d.fields[i].type, types[i]);
        EXPECT_EQ(d.fields[i].id, i);
        EXPECT_EQ(d.fields[i].offset, offsets[i]);
        EXPECT_FALSE(d.fields[i].isKey);
        EXPECT_EQ(d.fields[i].sinceVersion, 1);
        EXPECT_EQ(d.fields[i].untilVersion, 0xFFFF);
    }
    EXPECT_EQ(d.fields[4].name, "id");
    EXPECT_EQ(d.fields[4].id, 4);
    EXPECT_EQ(d.fields[4].offset, 32u);
    EXPECT_TRUE(d.fields[4].isKey);
    EXPECT_EQ(d.fields[5].name, "note");
    EXPECT_EQ(d.fields[5].id, 5);
    EXPECT_EQ(t->schema->nextTag, 6);
    EXPECT_EQ(t->schema->keyField, "id");
}

// ── stored lease, lease view, formatVer ──────────────────────────────────────
TEST(LeaseDdlRegression, LeaseStoredRegression) {
    CatalogDdlRig rig("leasestored");
    serve(rig);
    DefineTopicMsg m = leaseMsg(rig, "inst");
    m.lease->maxKeys = 77;
    ASSERT_EQ(submit(rig, m).code, kOk);
    const cat::TopicObject* t = topicOf(rig, "inst");
    ASSERT_NE(t, nullptr);
    ASSERT_TRUE(t->lease.has_value());
    EXPECT_EQ(*t->lease, *m.lease);
    const cat::ViewObject* v = viewOf(rig, "inst.leases");
    ASSERT_NE(v, nullptr);
    EXPECT_EQ(v->kind, cat::ViewKind::LeaseIndex);
    // The event the admin appended (rig.recordedLog(): every event the catalog replica holds, in order).
    const cat::CatalogEvent e = lastEvent(rig);
    EXPECT_EQ(e.formatVer, cat::kFormatV4);
    const auto* op = std::get_if<cat::CreateTopicOps>(&e.ops);
    ASSERT_NE(op, nullptr);
    ASSERT_TRUE(op->lease.has_value());
    EXPECT_EQ(*op->lease, *m.lease);
}

TEST(LeaseDdlRegression, DeleteRetentionDefaultRegression) {
    CatalogDdlRig rig("leasedel");
    serve(rig);
    ASSERT_EQ(submit(rig, leaseMsg(rig, "dflt")).code, kOk);
    EXPECT_EQ(topicOf(rig, "dflt")->config.deleteRetentionMs, 3'600'000);
    DefineTopicMsg ex = leaseMsg(rig, "expl");
    ex.config.deleteRetentionMs = 5'000;
    ASSERT_EQ(submit(rig, ex).code, kOk);
    EXPECT_EQ(topicOf(rig, "expl")->config.deleteRetentionMs, 5'000);
    // A non-lease topic keeps the 24 h default.
    DefineTopicMsg plain = leaseMsg(rig, "plain");
    plain.lease.reset();
    plain.config = {};
    ASSERT_EQ(submit(rig, plain).code, kOk);
    EXPECT_EQ(topicOf(rig, "plain")->config.deleteRetentionMs, rp::TopicConfig{}.deleteRetentionMs);
}

TEST(LeaseDdlRegression, FeatureLevelGateRegression) {
    CatalogDdlRig rig("leaselevel");
    serve(rig, 5);
    const auto refused = submit(rig, leaseMsg(rig, "inst"));
    EXPECT_EQ(refused.code, kInvalidRequest);
    EXPECT_EQ(refused.detail, "cluster below lease feature level");
    DefineTopicMsg p5 = leaseMsg(rig, "plain5");
    p5.lease.reset(); p5.config = {};
    ASSERT_EQ(submit(rig, p5).code, kOk);
    EXPECT_EQ(lastEvent(rig).formatVer, cat::kFormatV3);

    rig.admin().setClusterFeatureLevel(6);
    ASSERT_EQ(submit(rig, leaseMsg(rig, "inst")).code, kOk);
    EXPECT_EQ(lastEvent(rig).formatVer, cat::kFormatV4);
    DefineTopicMsg p6 = leaseMsg(rig, "plain6");
    p6.lease.reset(); p6.config = {};
    ASSERT_EQ(submit(rig, p6).code, kOk);
    EXPECT_EQ(lastEvent(rig).formatVer, cat::kFormatV4);

    // The same non-lease define built at level 5 (V3) and at level 6 (V4, fresh rig, same ids) has the same fingerprints.
    CatalogDdlRig rigA("leasefp5");
    serve(rigA, 5);
    DefineTopicMsg a = leaseMsg(rigA, "plainfp"); a.lease.reset(); a.config = {}; a.meta.requestId = {1, 2};
    ASSERT_EQ(submit(rigA, a).code, kOk);
    EXPECT_EQ(lastEvent(rigA).formatVer, cat::kFormatV3);
    CatalogDdlRig rigB("leasefp6");
    serve(rigB, 6);
    DefineTopicMsg b = leaseMsg(rigB, "plainfp"); b.lease.reset(); b.config = {}; b.meta.requestId = {1, 2};
    ASSERT_EQ(submit(rigB, b).code, kOk);
    EXPECT_EQ(lastEvent(rigB).formatVer, cat::kFormatV4);
    EXPECT_EQ(lastEvent(rigA).fingerprint, lastEvent(rigB).fingerprint);
    EXPECT_EQ(lastEvent(rigA).principal.requestFingerprint, lastEvent(rigB).principal.requestFingerprint);
    EXPECT_NE(lastEvent(rigA).fingerprint, 0u);
}

// ── namespace accounting ─────────────────────────────────────────────────────
namespace {
std::array<uint8_t, 4> maskOf(std::size_t field) {
    std::array<uint8_t, 4> m{};
    m[field >> 3] = static_cast<uint8_t>(1u << (field & 7));
    return m;
}
rp::NamespaceLimits limitsWith(std::size_t field, uint64_t v) {
    uint8_t b[rp::kNamespaceLimitsBytes] = {};
    rp::detail::putLimitU64(b + 8 * field, v);
    rp::NamespaceLimits l;
    (void)rp::decodeLimits(b, sizeof(b), l);
    return l;
}
constexpr std::size_t kFieldMaxTopics = 0, kFieldMaxViews = 3;
}  // namespace

TEST(LeaseDdlRegression, NamespaceCountsRegression) {
    {   // maxTopics = 1: a lease topic counts once; the second is refused.
        CatalogDdlRig rig("leasenst");
        serve(rig);
        const auto ns = rig.createNamespace("a", maskOf(kFieldMaxTopics), limitsWith(kFieldMaxTopics, 1));
        ASSERT_EQ(ns.code, kOk) << ns.detail;
        const uint32_t id = static_cast<uint32_t>(ns.objectId);
        ASSERT_EQ(submit(rig, leaseMsg(rig, "one", id)).code, kOk);
        EXPECT_NE(viewOf(rig, "one.leases", id), nullptr);
        EXPECT_EQ(submit(rig, leaseMsg(rig, "two", id)).code, kNamespaceLimitExceeded);
    }
    {   // maxViews = 1: the lease views do not count - two lease topics, then one plain view, are all accepted.
        CatalogDdlRig rig("leasensv");
        serve(rig);
        const auto ns = rig.createNamespace("a", maskOf(kFieldMaxViews), limitsWith(kFieldMaxViews, 1));
        ASSERT_EQ(ns.code, kOk) << ns.detail;
        const uint32_t id = static_cast<uint32_t>(ns.objectId);
        ASSERT_EQ(submit(rig, leaseMsg(rig, "one", id)).code, kOk);
        ASSERT_EQ(submit(rig, leaseMsg(rig, "two", id)).code, kOk);
        CatalogDdlRig::DefineArgs src; src.name = "src"; src.typed = true; src.nsId = id;
        ASSERT_EQ(rig.define(src).code, kOk);
        const auto v = rig.createView("pv", "src", false, 0, id);
        EXPECT_EQ(v.code, kOk) << v.detail;
        // The plain view is the namespace's one counted view: a second is refused.
        EXPECT_EQ(rig.createView("pv2", "src", false, 0, id).code, kNamespaceLimitExceeded);
    }
}

// ── dedupe fingerprint ───────────────────────────────────────────────────────
TEST(LeaseDdlRegression, DedupeFingerprintRegression) {
    DefineTopicMsg base;
    base.session = 1; base.corr = 2; base.name = "t"; base.keyField = "id"; base.blob = blobOf();
    base.meta.statement = "s";
    DefineTopicMsg withLease = base;
    withLease.lease = validSpec();
    DefineTopicMsg otherLease = withLease;
    otherLease.lease->ttlMs = 20'000;
    DefineTopicMsg withKey = base;
    withKey.keyFeatures.keyIndex = true;
    const uint64_t fBase = requestFingerprint(DdlRequest{base});
    EXPECT_NE(fBase, requestFingerprint(DdlRequest{withLease}));
    EXPECT_NE(requestFingerprint(DdlRequest{withLease}), requestFingerprint(DdlRequest{otherLease}));
    EXPECT_NE(fBase, requestFingerprint(DdlRequest{withKey}));
    EXPECT_NE(requestFingerprint(DdlRequest{withLease}), requestFingerprint(DdlRequest{withKey}));
    // Pinned at the parent commit: a plain, default-features request keeps its fingerprint.
    EXPECT_EQ(fBase, 3452249952166456580ull);   // computed with the parent commit's requestFingerprint
}

// ── admin wire ───────────────────────────────────────────────────────────────
namespace {
std::vector<uint8_t> frameOf(const DefineTopicMsg& m) {
    OneToOneByteRing ring{kAdminRequestRingBytes};
    EXPECT_TRUE(encodeDdl(ring, m));
    std::vector<uint8_t> out;
    ring.drain([&](int32_t, aeron::concurrent::AtomicBuffer& b, aeron::util::index_t off, aeron::util::index_t len) {
        const auto* p = reinterpret_cast<const uint8_t*>(b.buffer() + off);
        out.assign(p, p + len);
    });
    return out;
}
}  // namespace

TEST(LeaseDdlRegression, AdminWireRoundTripRegression) {
    DefineTopicMsg m;
    m.session = 7; m.corr = 8; m.name = "p"; m.keyField = "id"; m.blob = {1, 2}; m.config = leaseConfig();
    for (const bool lease : {false, true}) {
        if (lease) m.lease = validSpec();
        const std::vector<uint8_t> f = frameOf(m);
        ASSERT_FALSE(f.empty());
        const DdlRequest r = decodeDefineTopic(f.data(), f.size());
        const auto* got = std::get_if<DefineTopicMsg>(&r);
        ASSERT_NE(got, nullptr) << lease;
        EXPECT_EQ(got->lease.has_value(), lease);
        if (lease) EXPECT_EQ(*got->lease, *m.lease);
        EXPECT_EQ(got->name, "p");
        // Strict: one byte more or less is malformed.
        EXPECT_TRUE(std::holds_alternative<MalformedDdl>(decodeDefineTopic(f.data(), f.size() - 1)));
        std::vector<uint8_t> longer = f; longer.push_back(0);
        EXPECT_TRUE(std::holds_alternative<MalformedDdl>(decodeDefineTopic(longer.data(), longer.size())));
    }
    // hasLease byte 2 is malformed (the byte is at n - 40 with a lease, n - 1 without).
    {
        DefineTopicMsg n = m; n.lease.reset();
        std::vector<uint8_t> f = frameOf(n);
        f[f.size() - 1] = 2;
        EXPECT_TRUE(std::holds_alternative<MalformedDdl>(decodeDefineTopic(f.data(), f.size())));
    }
    // A lease block cut at every length 0..38 (the hasLease byte says 1, the block is short): malformed.
    {
        std::vector<uint8_t> f = frameOf(m);   // m holds the lease now
        const std::size_t base = f.size() - rp::kLeaseSpecBytes;   // the first byte of the block
        for (std::size_t cut = 0; cut < rp::kLeaseSpecBytes; ++cut) {
            const DdlRequest r = decodeDefineTopic(f.data(), base + cut);
            const auto* mal = std::get_if<MalformedDdl>(&r);
            ASSERT_NE(mal, nullptr) << cut;
            EXPECT_EQ(mal->corr, 8);
        }
    }
}

// ── the lease view's name (L15) ──────────────────────────────────────────────
TEST(LeaseDdlRegression, LeaseViewNameRegression) {
    {   // 249 bytes with the suffix is the longest accepted; one more is too long.
        CatalogDdlRig rig("leasename");
        serve(rig);
        const std::string fits(242, 'a'), over(243, 'b');
        const auto ok = submit(rig, leaseMsg(rig, fits));
        EXPECT_EQ(ok.code, kOk) << ok.detail;
        const auto a = submit(rig, leaseMsg(rig, over));
        EXPECT_EQ(a.code, kInvalidTopicName);
        EXPECT_EQ(a.detail, "the lease view name '" + over + ".leases' is too long");
        EXPECT_EQ(topicOf(rig, over), nullptr);
        // A non-lease topic of the long name is fine.
        DefineTopicMsg plain = leaseMsg(rig, over); plain.lease.reset(); plain.config = {};
        EXPECT_EQ(submit(rig, plain).code, kOk);
    }
    {   // a TOPIC holds '<name>.leases'
        CatalogDdlRig rig("leasenametopic");
        serve(rig);
        CatalogDdlRig::DefineArgs held; held.name = "inst.leases"; held.typed = true;
        const auto h = rig.define(held);
        ASSERT_EQ(h.code, kOk);
        const auto a = submit(rig, leaseMsg(rig, "inst"));
        EXPECT_EQ(a.code, kTopicExists);
        EXPECT_EQ(a.topicId, h.topicId);
        EXPECT_EQ(a.version, h.version);
        EXPECT_EQ(a.detail, "");
        EXPECT_EQ(topicOf(rig, "inst"), nullptr);
    }
    {   // a VIEW holds '<name>.leases'
        CatalogDdlRig rig("leasenameview");
        serve(rig);
        CatalogDdlRig::DefineArgs src; src.name = "src"; src.typed = true;
        ASSERT_EQ(rig.define(src).code, kOk);
        const auto v = rig.createView("inst.leases", "src");
        ASSERT_EQ(v.code, kOk) << v.detail;
        const cat::ViewObject* vo = viewOf(rig, "inst.leases");
        ASSERT_NE(vo, nullptr);
        const auto a = submit(rig, leaseMsg(rig, "inst"));
        EXPECT_EQ(a.code, kTopicExists);
        EXPECT_EQ(a.topicId, vo->def.viewTopicId);
        EXPECT_EQ(a.version, 1u);   // the view topic's registry version (the createView answer carries 0)
        EXPECT_EQ(a.detail, "");
        EXPECT_EQ(topicOf(rig, "inst"), nullptr);
    }
    {   // a name that differs only in case from '<name>.leases' (the fold answer: TopicExists with the
        // holder's id; a DefineTopic response frame carries no detail for it)
        CatalogDdlRig rig("leasenamefold");
        serve(rig);
        CatalogDdlRig::DefineArgs held; held.name = "Inst.Leases"; held.typed = true;
        const auto h = rig.define(held);
        ASSERT_EQ(h.code, kOk);
        const auto a = submit(rig, leaseMsg(rig, "inst"));
        EXPECT_EQ(a.code, kTopicExists);
        EXPECT_EQ(a.topicId, h.topicId);
        EXPECT_EQ(a.version, 0u);
        EXPECT_EQ(a.detail, "");
        EXPECT_EQ(topicOf(rig, "inst"), nullptr);
    }
}

// ── rule order: two broken rules, the documented winner ──────────────────────
TEST(LeaseDdlRegression, RuleOrderRegression) {
    struct Case {
        const char* name; int16_t level; uint16_t code; std::string detail;
        void (*mutate)(CatalogDdlRig&, DefineTopicMsg&);
    };
    const Case cases[] = {
        {"LevelBeforeSpec", 5, kInvalidRequest, "cluster below lease feature level",
         [](CatalogDdlRig&, DefineTopicMsg& m) { m.lease->minTtlMs = 70'000; }},
        {"TransientBeforeRetention", 6, kInvalidRequest, "transient lease topics are not supported yet",
         [](CatalogDdlRig&, DefineTopicMsg& m) { m.persistence = 1; m.config.retentionMs = 1000; }},
        {"AutoIncrementBeforeReservedName", 6, kInvalidRequest, "a lease topic cannot use autoincrement",
         [](CatalogDdlRig&, DefineTopicMsg& m) {
             m.blob = blobOf(PrimType::UInt64, 0, "id", {"lease_x"}); m.keyFeatures.autoIncrement = true;
         }},
        {"KeyWidthBeforeLevel", 5, kInvalidSchema,
         "key field 'id' is string(129): a lease topic key is an integer or string(N) with N <= 128",
         [](CatalogDdlRig&, DefineTopicMsg& m) { m.blob = blobOf(PrimType::FixedStr, 129); }},
    };
    for (const Case& c : cases) {
        CatalogDdlRig rig("leaseorder");
        serve(rig, c.level);
        DefineTopicMsg m = leaseMsg(rig, "inst");
        c.mutate(rig, m);
        const auto a = submit(rig, m);
        EXPECT_EQ(a.code, c.code) << c.name << ": " << a.detail;
        EXPECT_EQ(a.detail, c.detail) << c.name;
        EXPECT_EQ(topicOf(rig, "inst"), nullptr) << c.name;
    }
    {   // an existing name at level 5: TopicExists, not the level text
        CatalogDdlRig rig("leaseorderexists");
        serve(rig, 5);
        CatalogDdlRig::DefineArgs plain; plain.name = "inst"; plain.typed = true;
        const auto p = rig.define(plain);
        ASSERT_EQ(p.code, kOk);
        const auto a = submit(rig, leaseMsg(rig, "inst"));
        EXPECT_EQ(a.code, kTopicExists);
        EXPECT_EQ(a.topicId, p.topicId);
        EXPECT_EQ(a.detail, "");
    }
}

// ── tag exhaustion (the four system columns shift every user tag by 4) ───────
TEST(LeaseDdlRegression, TagExhaustionRegression) {
    auto blobWithTag = [](uint16_t tag) {
        rune::codec::DescriptorBuilder b("t", 1);
        b.add("id", PrimType::UInt64, 1, true);
        b.add("note", PrimType::String);
        rune::codec::Descriptor d = b.build();
        d.fields[1].id = tag;
        return rune::codec::serialize(d);
    };
    CatalogDdlRig rig("leasetags");
    serve(rig);
    DefineTopicMsg over = leaseMsg(rig, "over");
    over.blob = blobWithTag(0xFFFB);
    const auto a = submit(rig, over);
    EXPECT_EQ(a.code, kInvalidSchema);
    EXPECT_EQ(a.detail, "field tags are exhausted after the 4 lease system columns");
    EXPECT_EQ(topicOf(rig, "over"), nullptr);
    DefineTopicMsg fits = leaseMsg(rig, "fits");
    fits.blob = blobWithTag(0xFFFA);
    const auto b = submit(rig, fits);
    EXPECT_EQ(b.code, kOk) << b.detail;
    ASSERT_NE(topicOf(rig, "fits"), nullptr);
    EXPECT_EQ(topicOf(rig, "fits")->schema->nextTag, 0xFFFF);
}

// ── the lease objects through every DDL ──────────────────────────────────────
TEST(LeaseDdlRegression, LeaseObjectDdlRegression) {
    CatalogDdlRig rig("leaseobj");
    serve(rig);
    ASSERT_EQ(submit(rig, leaseMsg(rig, "inst")).code, kOk);
    CatalogDdlRig::DefineArgs plain; plain.name = "plain"; plain.typed = true;
    ASSERT_EQ(rig.define(plain).code, kOk);

    const auto dropView = rig.dropView("inst.leases");
    const auto dropLeaseTopic = rig.dropTopic("inst.leases");
    const auto alterView = rig.alterView("inst.leases", "plain", false);
    const auto createView = rig.createView("inst.leases", "plain");
    DefineTopicMsg defLease = leaseMsg(rig, "inst.leases"); defLease.lease.reset(); defLease.config = {};
    const auto defineView = submit(rig, defLease);
    const auto retention = rig.alterConfig("inst", {{rp::ConfigKey::RetentionMs, 1000}});
    const auto badTtl = rig.alterConfig("inst", {{rp::ConfigKey::LeaseTtlMs, 20'000}});
    const auto plainTtl = rig.alterConfig("plain", {{rp::ConfigKey::LeaseTtlMs, 20'000}});
    const auto alterAdd = rig.alterAdd("inst", "lease_x");

    constexpr uint16_t kAlterViewCode = 38, kViewExists = 32, kInvalidSchemaCode = 23;
    EXPECT_EQ(dropView.code, kInvalidRequest);
    EXPECT_EQ(dropView.detail, "'inst.leases' is a lease view; drop its lease topic");
    EXPECT_EQ(dropLeaseTopic.code, kInvalidRequest);
    EXPECT_EQ(dropLeaseTopic.detail, "'inst.leases' is a lease view; drop its lease topic");
    EXPECT_EQ(alterView.code, kAlterViewCode);
    EXPECT_EQ(alterView.detail, "");
    EXPECT_EQ(createView.code, kViewExists);
    EXPECT_EQ(createView.detail, "");
    EXPECT_EQ(defineView.code, kTopicExists);
    EXPECT_EQ(defineView.detail, "");
    EXPECT_EQ(retention.code, kInvalidRequest);
    EXPECT_EQ(retention.detail, "AlterTopicConfig: a lease topic is compacted and keeps every live lease");
    EXPECT_EQ(plainTtl.code, kInvalidRequest);
    EXPECT_EQ(plainTtl.detail, "AlterTopicConfig: lease.* keys need a lease topic");
    EXPECT_EQ(alterAdd.code, kInvalidSchemaCode);
    EXPECT_EQ(alterAdd.detail, "AlterSchema: lease_ field names are reserved");
    EXPECT_EQ(badTtl.detail, "");
    EXPECT_EQ(badTtl.code, kOk) << badTtl.detail;
    EXPECT_EQ(topicOf(rig, "inst")->lease->ttlMs, 20'000u);
    EXPECT_EQ(topicOf(rig, "inst")->config.retentionMs, -1);

    // At level 5 the lease.* key is refused (the level never lowers on a live admin: restart replays the catalog at the
    // base level, then the level is raised to 5).
    rig.restart();
    rig.becomeServingLeader();
    rig.admin().setClusterFeatureLevel(5);
    ASSERT_NE(topicOf(rig, "inst"), nullptr);
    const auto lvl5 = rig.alterConfig("inst", {{rp::ConfigKey::LeaseTtlMs, 30'000}});
    EXPECT_EQ(lvl5.code, kInvalidRequest);
    EXPECT_EQ(lvl5.detail, "lease.* keys need every broker at feature level 6");
    EXPECT_EQ(topicOf(rig, "inst")->lease->ttlMs, 20'000u);
    // A retention.ms-only AlterTopicConfig on a plain topic at level 5 is still accepted.
    CatalogDdlRig::DefineArgs p5; p5.name = "plain5"; p5.typed = true;
    ASSERT_EQ(rig.define(p5).code, kOk);
    EXPECT_EQ(rig.alterConfig("plain5", {{rp::ConfigKey::RetentionMs, 60'000}}).code, kOk);
    rig.admin().setClusterFeatureLevel(6);

    // Dropping the topic drops its lease view.
    const auto dropInst = rig.dropTopic("inst");
    EXPECT_EQ(dropInst.code, kOk);
    EXPECT_EQ(dropInst.detail, "");
    EXPECT_EQ(topicOf(rig, "inst"), nullptr);
    EXPECT_EQ(viewOf(rig, "inst.leases"), nullptr);
}
