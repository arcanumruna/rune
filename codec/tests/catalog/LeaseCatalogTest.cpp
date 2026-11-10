// Lease topics Task 2: catalog formatVer 4 — the LeaseSpec trailer of a CreateTopic, the lease.* AlterTopicConfig keys,
// the lease view that one CreateTopic creates beside its topic, the state-driven lease rules and the v4 golden log.
#include <rune/catalog/CatalogEvent.h>
#include <rune/catalog/CatalogFile.h>
#include <rune/catalog/CatalogQueries.h>
#include <rune/catalog/CatalogReplayer.h>
#include <rune/catalog/CatalogState.h>
#include <rune/catalog/testing/GoldenScript.h>
#include <rune/codec/Descriptor.h>
#include <rune/codec/PrimType.h>
#include <rune/codec/Serialize.h>
#include <rune/protocol/CatalogTypes.h>
#include <rune/protocol/LeaseSpec.h>
#include <gtest/gtest.h>

#include <cstdint>
#include <fstream>
#include <functional>
#include <iterator>
#include <set>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "CatalogGoldenPins.h"

namespace rc = rune::catalog;
namespace rp = rune::protocol;
namespace cd = rune::codec;

namespace {

using Code = rc::ApplyError::Code;

std::vector<uint8_t> leaseBlob(cd::PrimType keyType = cd::PrimType::FixedStr) {
    cd::DescriptorBuilder b("inst");
    b.add("lease_version", cd::PrimType::Int64).add("lease_ttl_ms", cd::PrimType::UInt64)
        .add("lease_holder", cd::PrimType::UInt64).add("lease_remaining_ms", cd::PrimType::UInt64);
    if (keyType == cd::PrimType::FixedStr) b.addFixedStr("id", 64, 1, /*isKey=*/true);
    else b.add("id", keyType, 1, /*isKey=*/true);
    return cd::serialize(b.build());
}
/// A lease schema blob with the given system columns (name, type) in order, then `id` (the key), then `extra` fields.
std::vector<uint8_t> columnsBlob(const std::vector<std::pair<const char*, cd::PrimType>>& cols,
                                 const std::vector<const char*>& extra = {}, const char* keyName = "id") {
    cd::DescriptorBuilder b("inst");
    for (const auto& [n, t] : cols) b.add(n, t, 1, std::string_view(n) == keyName);
    if (std::string_view(keyName) == "id") b.addFixedStr("id", 64, 1, /*isKey=*/true);
    else b.addFixedStr("id", 64);
    for (const char* e : extra) b.add(e, cd::PrimType::Int32);
    return cd::serialize(b.build());
}
using Cols = std::vector<std::pair<const char*, cd::PrimType>>;
const Cols kSysCols = {{"lease_version", cd::PrimType::Int64}, {"lease_ttl_ms", cd::PrimType::UInt64},
                       {"lease_holder", cd::PrimType::UInt64}, {"lease_remaining_ms", cd::PrimType::UInt64}};
rc::CreateTopicOps leaseOps(int32_t topicId, const rp::LeaseSpec& spec = {}) {
    rc::CreateTopicOps o;
    o.topicId = topicId; o.partitions = 2; o.config.cleanupPolicy = 1;
    o.schema = rc::SchemaDef{.keyField = "id", .validate = true, .descriptorBlob = leaseBlob(), .nextTag = 5};
    o.lease = spec;
    return o;
}
rc::CatalogEvent event(uint16_t formatVer, rc::Ops ops, std::string name, uint64_t objectId, uint32_t prev, uint32_t nsId = 0) {
    rc::CatalogEvent e;
    e.formatVer = formatVer; e.nsId = nsId;
    e.kind = rc::kindOf(ops); e.ops = std::move(ops); e.objectName = std::move(name);
    e.objectId = objectId; e.prevVersion = prev; e.newVersion = prev + 1;
    return e;
}
rc::CatalogEvent v4(rc::Ops ops, std::string name, uint64_t objectId, uint32_t prev, uint32_t nsId = 0) {
    return event(rc::kFormatV4, std::move(ops), std::move(name), objectId, prev, nsId);
}
/// The leader's way: preview the fingerprint, stamp it, apply at the next offset.
std::expected<rc::Applied, rc::ApplyError> put(rc::CatalogState& s, rc::CatalogEvent e) {
    auto fp = rc::previewFingerprint(s, e);
    if (!fp) return std::unexpected(fp.error());
    e.fingerprint = *fp;
    return rc::apply(s, e, s.appliedOffset() + 1);
}
/// A lease topic `inst` (objectId 1, topicId 0) plus its view (2, 1) on a fresh state.
void leaseState(rc::CatalogState& s, const rp::LeaseSpec& spec = {}) {
    auto a = put(s, v4(leaseOps(0, spec), "inst", 1, 0));
    ASSERT_TRUE(a.has_value()) << a.error().detail;
}
rc::AlterTopicConfigOps cfg(std::vector<rc::ConfigChange> c) { return rc::AlterTopicConfigOps{std::move(c)}; }
rc::ResolvedAlterOp op(cd::AlterOpType t, uint16_t tag, std::string name = {}) {
    rc::ResolvedAlterOp r;
    r.type = t; r.tag = tag; r.name = std::move(name); r.primType = cd::PrimType::Int32;
    return r;
}
bool has(const std::string& hay, const char* needle) { return hay.find(needle) != std::string::npos; }

const rp::LeaseSpec kCustom{.ttlMs = 12'345, .minTtlMs = 2'000, .maxTtlMs = 99'999, .maxKeys = 777, .expiryBatch = 33,
                            .checkpoint = 0, .checkpointIntervalMs = 4'321, .checkpointRate = 55};

std::vector<uint8_t> leaseBytes(const rp::LeaseSpec& s) {
    std::vector<uint8_t> b(rp::kLeaseSpecBytes);
    rp::encodeLeaseSpec(s, b.data());
    return b;
}
/// The ops blob is the event's last field, behind its own u32 length: find that length (the LE u32 that equals the
/// number of bytes after it).
std::size_t opsLenOffset(const std::vector<uint8_t>& b) {
    for (std::size_t at = b.size() - 4; at-- > 0;) {
        uint32_t v = 0;
        for (int i = 0; i < 4; ++i) v |= static_cast<uint32_t>(b[at + i]) << (8 * i);
        if (v == b.size() - at - 4) return at;
    }
    ADD_FAILURE() << "no ops length found";
    return 0;
}
void patchOpsLen(std::vector<uint8_t>& b, std::size_t at, uint32_t len) {
    for (int i = 0; i < 4; ++i) b[at + i] = static_cast<uint8_t>(len >> (8 * i));
}

} // namespace

// 1 ─────────────────────────────────────────────────────────────────────────────────────────────────────────────────
TEST(LeaseCatalogRegression, LeaseTrailerRoundTripsInV4Regression) {
    const auto e = v4(leaseOps(0, kCustom), "inst", 1, 0);
    const auto bytes = rc::encode(e);
    ASSERT_TRUE(bytes.has_value());
    const std::size_t at = bytes->size() - (1 + rp::kLeaseSpecBytes);
    EXPECT_EQ((*bytes)[at], 0x01) << "the byte after the schema";
    EXPECT_EQ(std::vector<uint8_t>(bytes->begin() + at + 1, bytes->end()), leaseBytes(kCustom));
    const auto d = rc::decode(*bytes);
    ASSERT_TRUE(d.has_value());
    EXPECT_EQ(*d, e);
    EXPECT_EQ(std::get<rc::CreateTopicOps>(d->ops).lease, kCustom);

    auto plain = leaseOps(0);
    plain.lease.reset();
    const auto pb = rc::encode(v4(plain, "t", 1, 0));
    ASSERT_TRUE(pb.has_value());
    EXPECT_EQ(pb->back(), 0x00) << "a v4 CreateTopic without a lease writes a trailing 0";
    EXPECT_EQ(rc::decode(*pb).value(), v4(plain, "t", 1, 0));

    // formatVer 3 writes neither the byte nor the block
    auto v3 = leaseOps(0);
    v3.lease.reset();
    EXPECT_EQ(rc::encode(event(rc::kFormatV3, v3, "t", 1, 0)).value().size() + 1, pb->size());
}

// 2 ─────────────────────────────────────────────────────────────────────────────────────────────────────────────────
TEST(LeaseCatalogRegression, TrailerTruncationRegression) {
    const auto full = rc::encode(v4(leaseOps(0, kCustom), "inst", 1, 0)).value();
    const std::size_t lenAt = opsLenOffset(full);
    const uint32_t opsLen = static_cast<uint32_t>(full.size() - lenAt - 4);
    // cut at EVERY length from the hasLease byte to full - 1, the ops length patched to match so the cut reaches the body
    for (std::size_t cut = 1; cut <= 1 + rp::kLeaseSpecBytes; ++cut) {
        auto b = std::vector<uint8_t>(full.begin(), full.end() - static_cast<std::ptrdiff_t>(cut));
        patchOpsLen(b, lenAt, opsLen - static_cast<uint32_t>(cut));
        const auto d = rc::decode(b);
        ASSERT_FALSE(d.has_value()) << "cut " << cut;
        // cut == 40: no hasLease byte at all; 1..39: the byte is 1 and the block is short
        EXPECT_EQ(d.error(), rc::DecodeError::Truncated) << "cut " << cut;
    }
    // the same cuts without patching the length: the ops blob is short
    for (std::size_t cut = 1; cut <= 1 + rp::kLeaseSpecBytes; ++cut)
        EXPECT_EQ(rc::decode(std::span<const uint8_t>(full.data(), full.size() - cut)).error(), rc::DecodeError::Truncated) << cut;

    auto bad = full;
    bad[full.size() - (1 + rp::kLeaseSpecBytes)] = 2;   // hasLease = 2
    EXPECT_EQ(rc::decode(bad).error(), rc::DecodeError::BadEnum);
    auto ck = full;
    ck[full.size() - rp::kLeaseSpecBytes + 30] = 2;     // checkpoint byte = 2
    EXPECT_EQ(rc::decode(ck).error(), rc::DecodeError::BadEnum);
    // a trailing byte after the block
    auto extra = full;
    extra.push_back(0);
    patchOpsLen(extra, lenAt, opsLen + 1);
    EXPECT_EQ(rc::decode(extra).error(), rc::DecodeError::TrailingBytes);
}

// 3 ─────────────────────────────────────────────────────────────────────────────────────────────────────────────────
TEST(LeaseCatalogRegression, OlderFormatsRefuseLeaseRegression) {
    for (const uint16_t f : {rc::kFormatV1, rc::kFormatV2, rc::kFormatV3}) {
        const auto r = rc::encode(event(f, leaseOps(0), "inst", 1, 0));
        ASSERT_FALSE(r.has_value()) << f;
        EXPECT_EQ(r.error(), rc::EncodeError::NeedsFormatV4) << f;
        for (const rc::ConfigKey k : {rc::ConfigKey::LeaseTtlMs, rc::ConfigKey::LeaseMinTtlMs, rc::ConfigKey::LeaseMaxTtlMs,
                                      rc::ConfigKey::LeaseCheckpoint, rc::ConfigKey::LeaseCheckpointIntervalMs,
                                      rc::ConfigKey::LeaseCheckpointRate}) {
            const auto a = rc::encode(event(f, cfg({{k, 5000}}), "inst", 1, 1));
            ASSERT_FALSE(a.has_value()) << f << " " << static_cast<int>(k);
            EXPECT_EQ(a.error(), rc::EncodeError::NeedsFormatV4) << f << " " << static_cast<int>(k);
        }
        EXPECT_TRUE(rc::encode(event(f, cfg({{rc::ConfigKey::MaxCompactionLagMs, 5}}), "inst", 1, 1)).has_value()) << f;
    }
    EXPECT_STREQ(std::string(rc::encodeErrorName(rc::EncodeError::NeedsFormatV4)).c_str(), "NeedsFormatV4");

    rp::LeaseSpec bad;
    bad.minTtlMs = 999;
    const auto r = rc::encode(v4(leaseOps(0, bad), "inst", 1, 0));
    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error(), rc::EncodeError::BadEnum);

    // decode of the same invalid spec in bytes (a valid event with the block patched): BadEnum
    auto good = rc::encode(v4(leaseOps(0), "inst", 1, 0)).value();
    const auto blk = good.size() - rp::kLeaseSpecBytes;
    for (int i = 0; i < 8; ++i) good[blk + 8 + i] = static_cast<uint8_t>(999 >> (8 * i));   // min_ttl = 999
    EXPECT_EQ(rc::decode(good).error(), rc::DecodeError::BadEnum);

    // the lease keys are in range for formatVer 4
    const auto ok = rc::encode(v4(cfg({{rc::ConfigKey::LeaseCheckpointRate, 10}}), "inst", 1, 1));
    ASSERT_TRUE(ok.has_value());
    EXPECT_EQ(rc::decode(*ok).value(), v4(cfg({{rc::ConfigKey::LeaseCheckpointRate, 10}}), "inst", 1, 1));
    // a key beyond the last
    const auto beyond = rc::encode(v4(cfg({{static_cast<rc::ConfigKey>(13), 10}}), "inst", 1, 1));
    ASSERT_FALSE(beyond.has_value());
    EXPECT_EQ(beyond.error(), rc::EncodeError::BadEnum);
}

// 4 ─────────────────────────────────────────────────────────────────────────────────────────────────────────────────
TEST(LeaseCatalogRegression, OneEventCreatesTopicAndViewRegression) {
    rc::CatalogState s;
    auto a = put(s, v4(leaseOps(0, kCustom), "inst", 1, 0));
    ASSERT_TRUE(a.has_value()) << a.error().detail;
    const auto* t = s.topicByName(rc::kDefaultNsId, "inst");
    ASSERT_NE(t, nullptr);
    EXPECT_EQ(t->lease, kCustom);
    const auto* v = s.viewByName(rc::kDefaultNsId, "inst.leases");
    ASSERT_NE(v, nullptr);
    EXPECT_EQ(v->kind, rc::ViewKind::LeaseIndex);
    EXPECT_EQ(v->objectId, t->objectId + 1);
    EXPECT_EQ(v->def.viewTopicId, t->topicId + 1);
    EXPECT_EQ(v->def.sinkPartitions, t->partitions);
    EXPECT_EQ(v->def.persistence, t->persistence);
    EXPECT_TRUE(v->def.sourceNames.empty());
    EXPECT_EQ(v->version, 1u);
    EXPECT_EQ(v->leaseTopicObjectId, t->objectId);
    EXPECT_EQ(a->leaseViewObjectId, 2u);
    EXPECT_EQ(a->leaseViewTopicId, 1);
    EXPECT_EQ(a->createdTopicId, 0);
    EXPECT_EQ(s.nextObjectId(), 3u);
    EXPECT_EQ(s.nextTopicId(), 2);
    EXPECT_EQ(s.leaseViewOf(t->objectId), v);
    EXPECT_EQ(s.viewByViewTopicId(1), v);
    EXPECT_EQ(s.viewReading(rc::kDefaultNsId, "inst"), nullptr);

    // a plain topic: no lease, no view, one id each
    rc::CatalogState p;
    auto plain = leaseOps(0);
    plain.lease.reset();
    auto pa = put(p, v4(plain, "inst", 1, 0));
    ASSERT_TRUE(pa.has_value()) << pa.error().detail;
    EXPECT_EQ(pa->leaseViewObjectId, 0u);
    EXPECT_EQ(pa->leaseViewTopicId, -1);
    EXPECT_EQ(p.nextObjectId(), 2u);
    EXPECT_EQ(p.nextTopicId(), 1);
    EXPECT_EQ(p.leaseViewOf(1), nullptr);
    EXPECT_NE(p.digest(), s.digest());

    // the next object takes the ids after the pair
    auto next = put(s, v4(rc::CreateTopicOps{.topicId = 2}, "other", 3, 0));
    EXPECT_TRUE(next.has_value()) << next.error().detail;
}

// 5 ─────────────────────────────────────────────────────────────────────────────────────────────────────────────────
TEST(LeaseCatalogRegression, DropTopicDropsViewRegression) {
    rc::CatalogState s;
    leaseState(s);
    auto a = put(s, v4(rc::DropTopicOps{0}, "inst", 1, 1));
    ASSERT_TRUE(a.has_value()) << a.error().detail;
    EXPECT_EQ(s.topicByName(rc::kDefaultNsId, "inst"), nullptr);
    EXPECT_EQ(s.viewByName(rc::kDefaultNsId, "inst.leases"), nullptr);
    EXPECT_TRUE(s.topics().empty());
    EXPECT_TRUE(s.views().empty());
    EXPECT_EQ(s.viewByViewTopicId(1), nullptr);
    ASSERT_EQ(s.tombstones().size(), 2u);
    EXPECT_EQ(s.tombstones()[0], (rc::Tombstone{1, "inst", 2, false, 0}));
    EXPECT_EQ(s.tombstones()[1], (rc::Tombstone{2, "inst.leases", 2, true, 0}));
    EXPECT_EQ(a->leaseViewObjectId, 2u);
    EXPECT_EQ(a->leaseViewTopicId, 1);
    EXPECT_EQ(a->droppedTopicId, 0);
    // the name is free again
    auto reuse = put(s, v4(rc::CreateTopicOps{.topicId = 2}, "inst.leases", 3, 0));
    EXPECT_TRUE(reuse.has_value()) << reuse.error().detail;
    // and a plain DropTopic of a non-lease topic carries no view ids
    auto b = put(s, v4(rc::DropTopicOps{2}, "inst.leases", 3, 1));
    ASSERT_TRUE(b.has_value());
    EXPECT_EQ(b->leaseViewObjectId, 0u);
    EXPECT_EQ(b->leaseViewTopicId, -1);
}

// 6 ─────────────────────────────────────────────────────────────────────────────────────────────────────────────────
TEST(LeaseCatalogRegression, DropLeaseViewRefusedRegression) {
    rc::CatalogState s;
    leaseState(s);
    const uint64_t before = s.digest();
    const auto drop = rc::apply(s, [&] { auto e = v4(rc::DropViewOps{1}, "inst.leases", 2, 1); e.fingerprint = 1; return e; }(), 1);
    ASSERT_FALSE(drop.has_value());
    EXPECT_EQ(drop.error().code, Code::Precondition);
    EXPECT_TRUE(has(drop.error().detail, "drop the lease topic")) << drop.error().detail;

    rc::ViewDef def;
    def.viewTopicId = 2; def.sinkPartitions = 2; def.sourceNames = {"inst"};
    const auto alter = put(s, v4(rc::AlterViewOps{def}, "inst.leases", 2, 1));
    ASSERT_FALSE(alter.has_value());
    EXPECT_EQ(alter.error().code, Code::Precondition);
    EXPECT_TRUE(has(alter.error().detail, "a lease view cannot be altered")) << alter.error().detail;
    EXPECT_EQ(s.digest(), before);
    EXPECT_NE(s.viewByName(rc::kDefaultNsId, "inst.leases"), nullptr);

    // the same refusal under an older formatVer, and through preview
    const auto prev = rc::previewFingerprint(s, event(rc::kFormatV3, rc::DropViewOps{1}, "inst.leases", 2, 1));
    ASSERT_FALSE(prev.has_value());
    EXPECT_TRUE(has(prev.error().detail, "drop the lease topic"));
}

// 7 ─────────────────────────────────────────────────────────────────────────────────────────────────────────────────
TEST(LeaseCatalogRegression, LeaseViewNameTakenRegression) {
    {   // (a) the lease topic is live: its view's name is taken for a topic and for a view
        rc::CatalogState s;
        leaseState(s);
        const auto t = put(s, v4(rc::CreateTopicOps{.topicId = 2}, "inst.leases", 3, 0));
        ASSERT_FALSE(t.has_value());
        EXPECT_EQ(t.error().code, Code::Precondition);
        EXPECT_TRUE(has(t.error().detail, "is taken")) << t.error().detail;
        rc::ViewDef def;
        def.viewTopicId = 2; def.sinkPartitions = 2; def.sourceNames = {"inst"};
        def.normalizedPlanBlob = {1}; def.outputDescriptorBlob = leaseBlob();
        const auto v = put(s, v4(rc::CreateViewOps{def}, "inst.leases", 3, 0));
        ASSERT_FALSE(v.has_value());
        EXPECT_EQ(v.error().code, Code::Precondition);
        EXPECT_TRUE(has(v.error().detail, "is taken")) << v.error().detail;
    }
    {   // (b) a plain topic owns the name
        rc::CatalogState s;
        ASSERT_TRUE(put(s, v4(rc::CreateTopicOps{.topicId = 0}, "inst.leases", 1, 0)).has_value());
        const auto r = put(s, v4(leaseOps(1), "inst", 2, 0));
        ASSERT_FALSE(r.has_value());
        EXPECT_EQ(r.error().code, Code::Precondition);
        EXPECT_TRUE(has(r.error().detail, "'inst.leases' is taken")) << r.error().detail;
        EXPECT_EQ(s.topics().size(), 1u);
    }
    {   // (c) a name that only case-folds onto it
        rc::CatalogState s;
        ASSERT_TRUE(put(s, v4(rc::CreateTopicOps{.topicId = 0}, "INST.LEASES", 1, 0)).has_value());
        const auto r = put(s, v4(leaseOps(1), "inst", 2, 0));
        ASSERT_FALSE(r.has_value());
        EXPECT_EQ(r.error().code, Code::Precondition);
        EXPECT_TRUE(has(r.error().detail, "'inst.leases' is taken")) << r.error().detail;
        // and the other direction: the lease view is live, a topic that folds onto its name is refused
        rc::CatalogState t;
        leaseState(t);
        const auto f = put(t, v4(rc::CreateTopicOps{.topicId = 2}, "INST.LEASES", 3, 0));
        ASSERT_FALSE(f.has_value());
        EXPECT_EQ(f.error().code, Code::Precondition);
    }
    {   // (d) another namespace is independent
        rc::CatalogState s;
        ASSERT_TRUE(put(s, v4(rc::NamespaceCreatedOps{"ns1", {}}, "ns1", 0, 0, 1)).has_value());
        leaseState(s);
        const auto other = put(s, v4(leaseOps(2), "inst", 3, 0, 1));
        EXPECT_TRUE(other.has_value()) << other.error().detail;
        const auto plain = put(s, v4(rc::CreateTopicOps{.topicId = 4}, "inst.leases", 5, 0, 1));
        EXPECT_FALSE(plain.has_value()) << "ns 1's own lease view owns the name there";
        EXPECT_NE(s.viewByName(1, "inst.leases"), nullptr);
        EXPECT_NE(s.viewByName(0, "inst.leases"), nullptr);
        rc::CatalogState u;
        ASSERT_TRUE(put(u, v4(rc::NamespaceCreatedOps{"ns1", {}}, "ns1", 0, 0, 1)).has_value());
        ASSERT_TRUE(put(u, v4(rc::CreateTopicOps{.topicId = 0}, "inst.leases", 1, 0, 1)).has_value());
        const auto fine = put(u, v4(leaseOps(1), "inst", 2, 0));   // default namespace: the name is free there
        EXPECT_TRUE(fine.has_value()) << fine.error().detail;
    }
}

// 8 ─────────────────────────────────────────────────────────────────────────────────────────────────────────────────
struct CreateRule { const char* name; std::function<void(rc::CreateTopicOps&, std::string&)> mutate; const char* text; };

class LeaseCatalogCreateRules : public ::testing::TestWithParam<CreateRule> {};

TEST_P(LeaseCatalogCreateRules, LeaseCreateTopicRule) {
    rc::CatalogState s;
    auto o = leaseOps(0);
    std::string name = "inst";
    GetParam().mutate(o, name);
    const auto r = put(s, v4(o, name, 1, 0));
    ASSERT_FALSE(r.has_value()) << GetParam().name;
    EXPECT_EQ(r.error().code, Code::Precondition) << r.error().detail;
    EXPECT_TRUE(has(r.error().detail, GetParam().text)) << GetParam().name << ": " << r.error().detail;
    EXPECT_TRUE(s.topics().empty());
    EXPECT_TRUE(s.views().empty());
}

INSTANTIATE_TEST_SUITE_P(LeaseCatalogRegression, LeaseCatalogCreateRules, ::testing::Values(
    CreateRule{"NoSchema", [](rc::CreateTopicOps& o, std::string&) { o.schema.reset(); }, "a lease topic needs a typed, keyed topic"},
    CreateRule{"EmptyKey", [](rc::CreateTopicOps& o, std::string&) { o.schema->keyField.clear(); }, "a lease topic needs a typed, keyed topic"},
    CreateRule{"KeyIndexOn", [](rc::CreateTopicOps& o, std::string&) { o.schema->keyFeatures.keyIndex = true; },
               "a lease topic's key index is its lease view"},
    CreateRule{"AutoIncrementOn", [](rc::CreateTopicOps& o, std::string&) {
                   o.schema->descriptorBlob = leaseBlob(cd::PrimType::UInt64);   // a key the formatVer-3 rules accept
                   o.schema->keyFeatures = {.keyIndex = true, .autoIncrement = true, .partitioner = rp::Partitioner::Modulo};
               }, "a lease topic's key index is its lease view"},
    CreateRule{"CleanupDelete", [](rc::CreateTopicOps& o, std::string&) { o.config.cleanupPolicy = 0; },
               "a lease topic is compacted and keeps every live lease"},
    CreateRule{"RetentionMs", [](rc::CreateTopicOps& o, std::string&) { o.config.retentionMs = 1000; },
               "a lease topic is compacted and keeps every live lease"},
    CreateRule{"RetentionBytes", [](rc::CreateTopicOps& o, std::string&) { o.config.retentionBytes = 1000; },
               "a lease topic is compacted and keeps every live lease"},
    CreateRule{"NameTooLong", [](rc::CreateTopicOps&, std::string& n) { n = std::string(243, 'a'); }, "is too long"},
    CreateRule{"SystemColumnsMissing", [](rc::CreateTopicOps& o, std::string&) { o.schema->descriptorBlob = columnsBlob({}); },
               "a lease topic starts with the four lease system columns"},
    CreateRule{"SystemColumnsWrongOrder", [](rc::CreateTopicOps& o, std::string&) {
                   o.schema->descriptorBlob = columnsBlob({kSysCols[0], kSysCols[2], kSysCols[1], kSysCols[3]});
               }, "a lease topic starts with the four lease system columns"},
    CreateRule{"TtlColumnInt64", [](rc::CreateTopicOps& o, std::string&) {
                   auto c = kSysCols;
                   c[1].second = cd::PrimType::Int64;
                   o.schema->descriptorBlob = columnsBlob(c);
               }, "a lease topic starts with the four lease system columns"},
    CreateRule{"ExtraLeaseFoo", [](rc::CreateTopicOps& o, std::string&) {
                   o.schema->descriptorBlob = columnsBlob(kSysCols, {"lease_foo"});
                   o.schema->nextTag = 6;
               }, "lease_ field names are reserved"},
    CreateRule{"KeyOnLeaseVersion", [](rc::CreateTopicOps& o, std::string&) {
                   o.schema->descriptorBlob = columnsBlob(kSysCols, {}, "lease_version");
                   o.schema->keyField = "lease_version";
               }, "the key of a lease topic is a user field"}),
    [](const auto& i) { return std::string(i.param.name); });

TEST(LeaseCatalogRegression, CreateRulesTableRegression) {
    {   // 242 bytes: the view name is 249, the longest a name may be
        rc::CatalogState s;
        const auto r = put(s, v4(leaseOps(0), std::string(242, 'a'), 1, 0));
        EXPECT_TRUE(r.has_value()) << r.error().detail;
        EXPECT_NE(s.viewByName(rc::kDefaultNsId, std::string(242, 'a') + ".leases"), nullptr);
    }
    {   // owner ruling: a transient lease topic applies (the broker refuses it at build for now)
        rc::CatalogState s;
        auto o = leaseOps(0);
        o.persistence = rc::Persistence::Transient;
        const auto r = put(s, v4(o, "inst", 1, 0));
        ASSERT_TRUE(r.has_value()) << r.error().detail;
        EXPECT_EQ(s.viewByName(rc::kDefaultNsId, "inst.leases")->def.persistence, rc::Persistence::Transient);
    }
    {   // the usual id rules still apply to a lease CreateTopic
        rc::CatalogState s;
        const auto r = put(s, v4(leaseOps(0), "inst", 7, 0));
        ASSERT_FALSE(r.has_value());
        EXPECT_TRUE(has(r.error().detail, "objectId"));
    }
}

// 8b ────────────────────────────────────────────────────────────────────────────────────────────────────────────────
TEST(LeaseCatalogRegression, ModuloAndAutoIncrementOnLeaseCreateRefusedByV3Regression) {
    auto refusal = [](const std::function<void(rc::CreateTopicOps&)>& mutate) {
        rc::CatalogState s;
        auto o = leaseOps(0);
        mutate(o);
        const auto r = put(s, v4(o, "inst", 1, 0));
        EXPECT_FALSE(r.has_value());
        EXPECT_TRUE(s.topics().empty());
        return r.has_value() ? std::string{} : r.error().detail;
    };
    // the modulo partitioner alone: ApplyV3's text, before any lease rule
    EXPECT_TRUE(has(refusal([](rc::CreateTopicOps& o) { o.schema->keyFeatures.partitioner = rp::Partitioner::Modulo; }),
                    "the modulo partitioner needs autoincrement"));
    // autoincrement without key.index (a key type it accepts)
    const std::string noIndex = refusal([](rc::CreateTopicOps& o) {
        o.schema->descriptorBlob = leaseBlob(cd::PrimType::UInt64);
        o.schema->keyFeatures = {.keyIndex = false, .autoIncrement = true, .partitioner = rp::Partitioner::Modulo};
    });
    EXPECT_TRUE(has(noIndex, "autoincrement needs key.index")) << noIndex;
    // autoincrement over a key type it refuses
    EXPECT_TRUE(has(refusal([](rc::CreateTopicOps& o) {
                        o.schema->keyFeatures = {.keyIndex = true, .autoIncrement = true, .partitioner = rp::Partitioner::Modulo};
                    }),
                    "autoincrement needs a uint64, int64 or uint32 key"));
}

// 9 ─────────────────────────────────────────────────────────────────────────────────────────────────────────────────
TEST(LeaseCatalogRegression, AlterKeysRegression) {
    struct Row { rc::ConfigKey key; int64_t value; std::function<void(rp::LeaseSpec&)> expect; };
    const std::vector<Row> rows = {
        {rc::ConfigKey::LeaseTtlMs, 5'000, [](rp::LeaseSpec& l) { l.ttlMs = 5'000; }},
        {rc::ConfigKey::LeaseMinTtlMs, 2'000, [](rp::LeaseSpec& l) { l.minTtlMs = 2'000; }},
        {rc::ConfigKey::LeaseMaxTtlMs, 100'000, [](rp::LeaseSpec& l) { l.maxTtlMs = 100'000; }},
        {rc::ConfigKey::LeaseCheckpoint, 0, [](rp::LeaseSpec& l) { l.checkpoint = 0; }},
        {rc::ConfigKey::LeaseCheckpointIntervalMs, 5'000, [](rp::LeaseSpec& l) { l.checkpointIntervalMs = 5'000; }},
        {rc::ConfigKey::LeaseCheckpointRate, 50, [](rp::LeaseSpec& l) { l.checkpointRate = 50; }},
    };
    for (const Row& row : rows) {
        rc::CatalogState s;
        leaseState(s);
        const rc::TopicConfig configBefore = s.topicByObjectId(1)->config;
        auto a = put(s, v4(cfg({{row.key, row.value}}), "inst", 1, 1));
        ASSERT_TRUE(a.has_value()) << static_cast<int>(row.key) << ": " << a.error().detail;
        rp::LeaseSpec want;
        row.expect(want);
        const auto* t = s.topicByObjectId(1);
        EXPECT_EQ(t->lease, want) << static_cast<int>(row.key);
        EXPECT_EQ(t->version, 2u);
        EXPECT_EQ(t->config, configBefore) << "a lease key changes no TopicConfig field";
        EXPECT_EQ(a->createdTopicId, -1);
    }
    auto refused = [](const std::vector<rc::ConfigChange>& changes, const char* text, const char* what) {
        rc::CatalogState s;
        leaseState(s);
        const uint64_t before = s.digest();
        const auto r = put(s, v4(cfg(changes), "inst", 1, 1));
        ASSERT_FALSE(r.has_value()) << what;
        EXPECT_EQ(r.error().code, Code::Precondition) << what;
        EXPECT_TRUE(has(r.error().detail, text)) << what << ": " << r.error().detail;
        EXPECT_EQ(s.digest(), before) << what;
    };
    refused({{rc::ConfigKey::LeaseMinTtlMs, 999}}, "lease.min_ttl must be between 1000", "min_ttl 999");
    refused({{rc::ConfigKey::LeaseMinTtlMs, 3000}, {rc::ConfigKey::LeaseMaxTtlMs, 2000}}, "lease.min_ttl must be between 1000 and lease.max_ttl", "min > max");
    refused({{rc::ConfigKey::LeaseTtlMs, 500}}, "lease.ttl must be between", "ttl below min");
    refused({{rc::ConfigKey::LeaseTtlMs, -1}}, "lease.ttl", "negative ttl");
    refused({{rc::ConfigKey::LeaseCheckpoint, 2}}, "lease.checkpoint must be 0 or 1", "checkpoint 2");
    refused({{rc::ConfigKey::LeaseCheckpoint, 256}}, "lease.checkpoint must be 0 or 1", "checkpoint 256");
    refused({{rc::ConfigKey::LeaseCheckpointRate, int64_t{1} << 32}}, "lease.checkpoint_rate", "rate beyond u32");
    refused({{rc::ConfigKey::LeaseCheckpointIntervalMs, 999}}, "lease.checkpoint_interval must be between", "interval 999");
    refused({{rc::ConfigKey::LeaseTtlMs, 5000}, {rc::ConfigKey::LeaseTtlMs, 6000}}, "set twice", "a lease key twice");
    refused({{rc::ConfigKey::RetentionMs, 1000}}, "a lease topic is compacted and keeps every live lease", "retention.ms");
    refused({{rc::ConfigKey::RetentionBytes, 1000}}, "a lease topic is compacted and keeps every live lease", "retention.bytes");
    refused({{rc::ConfigKey::LeaseTtlMs, 5000}, {rc::ConfigKey::RetentionMs, 1000}}, "a lease topic is compacted", "mixed with retention");

    {   // lease keys on a plain topic
        rc::CatalogState s;
        auto plain = leaseOps(0);
        plain.lease.reset();
        ASSERT_TRUE(put(s, v4(plain, "plain", 1, 0)).has_value());
        const auto r = put(s, v4(cfg({{rc::ConfigKey::LeaseTtlMs, 5000}}), "plain", 1, 1));
        ASSERT_FALSE(r.has_value());
        EXPECT_EQ(r.error().code, Code::Precondition);
        EXPECT_TRUE(has(r.error().detail, "lease.* keys need a lease topic")) << r.error().detail;
        // an untyped plain topic too
        ASSERT_TRUE(put(s, v4(rc::CreateTopicOps{.topicId = 1}, "raw", 2, 0)).has_value());
        EXPECT_FALSE(put(s, v4(cfg({{rc::ConfigKey::LeaseCheckpoint, 1}}), "raw", 2, 1)).has_value());
    }
    {   // a mixed list applies both
        rc::CatalogState s;
        leaseState(s);
        auto r = put(s, v4(cfg({{rc::ConfigKey::DeleteRetentionMs, 777}, {rc::ConfigKey::LeaseTtlMs, 9000}}), "inst", 1, 1));
        ASSERT_TRUE(r.has_value()) << r.error().detail;
        EXPECT_EQ(s.topicByObjectId(1)->config.deleteRetentionMs, 777);
        EXPECT_EQ(s.topicByObjectId(1)->lease->ttlMs, 9000u);
        EXPECT_EQ(s.topicByObjectId(1)->version, 2u);
    }
    {   // a non-lease key alone (formatVer 3 and 4 both)
        for (const uint16_t f : {rc::kFormatV3, rc::kFormatV4}) {
            rc::CatalogState s;
            leaseState(s);
            auto r = put(s, event(f, cfg({{rc::ConfigKey::DeleteRetentionMs, 5}}), "inst", 1, 1));
            ASSERT_TRUE(r.has_value()) << f << r.error().detail;
            EXPECT_EQ(s.topicByObjectId(1)->lease, rp::LeaseSpec{}) << "the lease is carried";
        }
    }
    {   // an empty change list stays refused
        rc::CatalogState s;
        leaseState(s);
        EXPECT_FALSE(put(s, v4(cfg({}), "inst", 1, 1)).has_value());
    }
    {   // the version chain and the liveness gates still apply to a lease-only change
        rc::CatalogState s;
        leaseState(s);
        EXPECT_FALSE(put(s, v4(cfg({{rc::ConfigKey::LeaseTtlMs, 5000}}), "inst", 1, 5)).has_value());
        EXPECT_FALSE(put(s, v4(cfg({{rc::ConfigKey::LeaseTtlMs, 5000}}), "inst", 9, 1)).has_value());
        EXPECT_FALSE(put(s, v4(cfg({{rc::ConfigKey::LeaseTtlMs, 5000}}), "nope", 1, 1)).has_value());
    }
}

// 10 ────────────────────────────────────────────────────────────────────────────────────────────────────────────────
// Spec A.2: a transient lease topic's max_ttl can only be raised (a live lease keeps its TTL across an alter); a persisted
// lease topic's can go down.
TEST(LeaseCatalogRegression, TransientMaxTtlOnlyRisesRegression) {
    const rp::LeaseSpec base{.ttlMs = 10'000, .minTtlMs = 1'000, .maxTtlMs = 60'000, .maxKeys = 100, .expiryBatch = 64,
                             .checkpoint = 0, .checkpointIntervalMs = 1'000, .checkpointRate = 1};
    const auto make = [&](rc::CatalogState& s, rc::Persistence p) {
        auto o = leaseOps(0, base);
        o.persistence = p;
        auto a = put(s, v4(o, "inst", 1, 0));
        ASSERT_TRUE(a.has_value()) << a.error().detail;
        ASSERT_EQ(s.topicByObjectId(1)->persistence, p);
    };
    {   // transient: up is applied
        rc::CatalogState s;
        make(s, rc::Persistence::Transient);
        auto a = put(s, v4(cfg({{rc::ConfigKey::LeaseMaxTtlMs, 120'000}}), "inst", 1, 1));
        ASSERT_TRUE(a.has_value()) << a.error().detail;
        EXPECT_EQ(s.topicByObjectId(1)->lease->maxTtlMs, 120'000);
        // the same value again (not lower) is applied too
        auto same = put(s, v4(cfg({{rc::ConfigKey::LeaseMaxTtlMs, 120'000}}), "inst", 1, 2));
        EXPECT_TRUE(same.has_value());
    }
    {   // transient: down is refused, state untouched
        rc::CatalogState s;
        make(s, rc::Persistence::Transient);
        const uint64_t before = s.digest();
        const auto r = put(s, v4(cfg({{rc::ConfigKey::LeaseMaxTtlMs, 30'000}}), "inst", 1, 1));
        ASSERT_FALSE(r.has_value());
        EXPECT_EQ(r.error().code, Code::Precondition);
        EXPECT_TRUE(has(r.error().detail, "a transient lease topic's max_ttl can only be raised")) << r.error().detail;
        EXPECT_EQ(s.digest(), before);
        EXPECT_EQ(s.topicByObjectId(1)->lease->maxTtlMs, 60'000);
    }
    {   // persisted: down is still applied
        rc::CatalogState s;
        make(s, rc::Persistence::Disk);
        auto a = put(s, v4(cfg({{rc::ConfigKey::LeaseMaxTtlMs, 30'000}}), "inst", 1, 1));
        ASSERT_TRUE(a.has_value()) << a.error().detail;
        EXPECT_EQ(s.topicByObjectId(1)->lease->maxTtlMs, 30'000);
    }
}

TEST(LeaseCatalogRegression, AlterSchemaGuardsRegression) {
    auto refuses = [](rc::ResolvedAlterOp o, const char* text) {
        rc::CatalogState s;
        leaseState(s);
        const uint64_t before = s.digest();
        const auto r = put(s, v4(rc::AlterSchemaOps{{o}}, "inst", 1, 1));
        ASSERT_FALSE(r.has_value());
        EXPECT_EQ(r.error().code, Code::Precondition);
        EXPECT_TRUE(has(r.error().detail, text)) << r.error().detail;
        EXPECT_EQ(s.digest(), before);
    };
    const char* kSys = "the lease system columns cannot be dropped or renamed";
    for (uint16_t tag = 0; tag < 4; ++tag) {
        refuses(op(cd::AlterOpType::Drop, tag), kSys);
        refuses(op(cd::AlterOpType::Rename, tag, "x"), kSys);
    }
    {
        auto add = op(cd::AlterOpType::Add, 5, "lease_x");
        refuses(add, "lease_ field names are reserved");
    }
    refuses(op(cd::AlterOpType::Rename, 4, "lease_x"), "lease_ field names are reserved");   // a rename's NEW name is reserved too
    rc::CatalogState s;
    leaseState(s);
    auto add = op(cd::AlterOpType::Add, 5, "note");
    const auto r = put(s, v4(rc::AlterSchemaOps{{add}}, "inst", 1, 1));
    ASSERT_TRUE(r.has_value()) << r.error().detail;
    const auto* t = s.topicByObjectId(1);
    EXPECT_EQ(t->schema->schemaVersion, 2);
    EXPECT_EQ(t->lease, rp::LeaseSpec{}) << "carried across the AlterSchema";
    EXPECT_NE(s.viewByName(rc::kDefaultNsId, "inst.leases"), nullptr);
    {   // renaming the user field `note` (tag 5): to lease_x is refused, to memo applies
        const uint64_t before = s.digest();
        const auto bad = put(s, v4(rc::AlterSchemaOps{{op(cd::AlterOpType::Rename, 5, "lease_x")}}, "inst", 1, 2));
        ASSERT_FALSE(bad.has_value());
        EXPECT_TRUE(has(bad.error().detail, "lease_ field names are reserved")) << bad.error().detail;
        EXPECT_EQ(s.digest(), before);
        const auto good = put(s, v4(rc::AlterSchemaOps{{op(cd::AlterOpType::Rename, 5, "memo")}}, "inst", 1, 2));
        EXPECT_TRUE(good.has_value()) << good.error().detail;
    }
    // a plain typed topic is not guarded: it may add lease_ fields and drop tag 2
    rc::CatalogState p;
    auto plain = leaseOps(0);
    plain.lease.reset();
    ASSERT_TRUE(put(p, v4(plain, "plain", 1, 0)).has_value());
    EXPECT_TRUE(put(p, v4(rc::AlterSchemaOps{{op(cd::AlterOpType::Add, 5, "lease_x")}}, "plain", 1, 1)).has_value());
    EXPECT_TRUE(put(p, v4(rc::AlterSchemaOps{{op(cd::AlterOpType::Drop, 2)}}, "plain", 1, 2)).has_value());
}

// 11 ────────────────────────────────────────────────────────────────────────────────────────────────────────────────
TEST(LeaseCatalogRegression, NamespaceDeleteRegression) {
    rc::CatalogState s;
    ASSERT_TRUE(put(s, v4(rc::NamespaceCreatedOps{"ns1", {}}, "ns1", 0, 0, 1)).has_value());
    ASSERT_TRUE(put(s, v4(leaseOps(0), "inst", 1, 0, 1)).has_value());   // topic 1 / 0, lease view 2 / 1
    rc::ViewDef def;
    def.viewTopicId = 2; def.sinkPartitions = 2; def.sourceNames = {"inst"};
    def.normalizedPlanBlob = {1}; def.outputDescriptorBlob = leaseBlob();
    auto cv = put(s, v4(rc::CreateViewOps{def}, "uv", 3, 0, 1));
    ASSERT_TRUE(cv.has_value()) << cv.error().detail;
    ASSERT_NE(s.viewByName(1, "inst.leases"), nullptr);

    auto del = put(s, v4(rc::NamespaceDeletedOps{}, "ns1", 0, 1, 1));
    ASSERT_TRUE(del.has_value()) << del.error().detail;
    const auto* d = std::get_if<rc::NsDeleting>(&s.namespaces().byId(1)->state);
    ASSERT_NE(d, nullptr);
    EXPECT_EQ(d->pendingViews, (std::set<uint64_t>{3})) << "only the user view; the lease view goes with its topic";
    EXPECT_EQ(d->pendingTopics, (std::set<uint64_t>{1}));

    // the lease view cannot be dropped on its own while DELETING either
    const auto direct = put(s, v4(rc::DropViewOps{1}, "inst.leases", 2, 1, 1));
    ASSERT_FALSE(direct.has_value());
    EXPECT_TRUE(has(direct.error().detail, "drop the lease topic"));

    ASSERT_TRUE(put(s, v4(rc::DropViewOps{2}, "uv", 3, 1, 1)).has_value());
    auto dt = put(s, v4(rc::DropTopicOps{0}, "inst", 1, 1, 1));
    ASSERT_TRUE(dt.has_value()) << dt.error().detail;
    EXPECT_EQ(dt->leaseViewObjectId, 2u);
    EXPECT_TRUE(s.views().empty());
    EXPECT_FALSE(dt->nsErased) << "the four steps are still to come";
    uint32_t version = 2;
    for (const uint8_t step : {rc::TxnsAborted, rc::GroupsTombstoned, rc::TxnStateTombstoned, rc::SecurityPurged}) {
        auto st = put(s, v4(rc::NamespaceDeleteStepOps{step}, "ns1", 0, version++, 1));
        ASSERT_TRUE(st.has_value()) << st.error().detail;
        EXPECT_EQ(st->nsErased, step == rc::SecurityPurged);
    }
    EXPECT_EQ(s.namespaces().byId(1), nullptr);
    EXPECT_TRUE(s.topics().empty());
    EXPECT_TRUE(s.views().empty());
    EXPECT_EQ(s.viewByName(1, "inst.leases"), nullptr);

    // dropping the topic FIRST also completes (the order of the drops does not matter)
    rc::CatalogState t;
    ASSERT_TRUE(put(t, v4(rc::NamespaceCreatedOps{"ns1", {}}, "ns1", 0, 0, 1)).has_value());
    ASSERT_TRUE(put(t, v4(leaseOps(0), "inst", 1, 0, 1)).has_value());
    ASSERT_TRUE(put(t, v4(rc::NamespaceDeletedOps{}, "ns1", 0, 1, 1)).has_value());
    ASSERT_TRUE(put(t, v4(rc::DropTopicOps{0}, "inst", 1, 1, 1)).has_value());
    version = 2;
    for (const uint8_t step : {rc::TxnsAborted, rc::GroupsTombstoned, rc::TxnStateTombstoned, rc::SecurityPurged})
        ASSERT_TRUE(put(t, v4(rc::NamespaceDeleteStepOps{step}, "ns1", 0, version++, 1)).has_value());
    EXPECT_EQ(t.namespaces().byId(1), nullptr);
}

// 12 ────────────────────────────────────────────────────────────────────────────────────────────────────────────────
namespace {
void replayMatches(const char* path, const uint64_t* pins, std::size_t n) {
    std::ifstream in(std::string(RUNE_CATALOG_GOLDEN_DIR) + "/" + path, std::ios::binary);
    ASSERT_TRUE(in) << path;
    auto records = rc::readRcat(in);
    ASSERT_TRUE(records.has_value()) << records.error();
    ASSERT_EQ(records->size(), n) << path;
    rc::CatalogReplayer r;
    for (std::size_t i = 0; i < n; ++i) {
        auto a = r.feed((*records)[i].first, (*records)[i].second);
        ASSERT_TRUE(a.has_value()) << path << ": " << rc::describe(a.error());
        EXPECT_EQ(r.state().digest(), pins[i]) << path << " offset " << i;
    }
}
} // namespace

TEST(LeaseCatalogRegression, NonLeaseFingerprintUnchangedRegression) {
    replayMatches("v1/basic.rcat", rc::golden::kBasicV1Digests, std::size(rc::golden::kBasicV1Digests));
    replayMatches("v2/namespaces.rcat", rc::golden::kNamespacesV2Digests, std::size(rc::golden::kNamespacesV2Digests));
    replayMatches("v3/basic.rcat", rc::golden::kBasicV3Digests, std::size(rc::golden::kBasicV3Digests));
    replayMatches("v3/autoinc.rcat", rc::golden::kAutoIncV3Digests, std::size(rc::golden::kAutoIncV3Digests));
    // a formatVer-4 topic WITHOUT a lease hashes exactly like the same topic under formatVer 3
    rc::CatalogState a, b;
    auto plain = leaseOps(0);
    plain.lease.reset();
    ASSERT_TRUE(put(a, event(rc::kFormatV3, plain, "t", 1, 0)).has_value());
    ASSERT_TRUE(put(b, v4(plain, "t", 1, 0)).has_value());
    EXPECT_EQ(a.digest(), b.digest());
}

// 13 ────────────────────────────────────────────────────────────────────────────────────────────────────────────────
TEST(LeaseCatalogRegression, LeaseFingerprintRegression) {
    rc::CatalogState s;
    rp::LeaseSpec other;
    other.checkpointRate = 2'000;
    const auto fa = rc::previewFingerprint(s, v4(leaseOps(0), "inst", 1, 0));
    const auto fb = rc::previewFingerprint(s, v4(leaseOps(0, other), "inst", 1, 0));
    ASSERT_TRUE(fa.has_value() && fb.has_value());
    EXPECT_NE(*fa, *fb);
    auto plain = leaseOps(0);
    plain.lease.reset();
    EXPECT_NE(*rc::previewFingerprint(s, v4(plain, "inst", 1, 0)), *fa) << "a lease changes the topic's fingerprint";

    // an alter that sets lease.ttl to its current value applies; the result equals the same topic reached another way
    rc::CatalogState a, c;
    leaseState(a);
    auto same = put(a, v4(cfg({{rc::ConfigKey::LeaseTtlMs, 30'000}}), "inst", 1, 1));
    ASSERT_TRUE(same.has_value()) << same.error().detail;
    rp::LeaseSpec tenSeconds;
    tenSeconds.ttlMs = 10'000;
    leaseState(c, tenSeconds);
    auto back = put(c, v4(cfg({{rc::ConfigKey::LeaseTtlMs, 30'000}}), "inst", 1, 1));
    ASSERT_TRUE(back.has_value()) << back.error().detail;
    EXPECT_EQ(same->fingerprint, back->fingerprint);
    EXPECT_EQ(a.digest(), c.digest());
    EXPECT_EQ(a.topicByObjectId(1)->lease, rp::LeaseSpec{});
    rc::CatalogState fresh;
    leaseState(fresh);
    EXPECT_NE(fresh.digest(), a.digest()) << "the version moved";
    rc::CatalogState empty;
    EXPECT_NE(same->fingerprint, rc::previewFingerprint(empty, v4(leaseOps(0), "inst", 1, 0)).value());

    // the lease view's contribution: the same state without a lease view hashes differently
    rc::CatalogState noLease;
    ASSERT_TRUE(put(noLease, v4(plain, "inst", 1, 0)).has_value());
    EXPECT_NE(noLease.digest(), fresh.digest());
}

// 14 ────────────────────────────────────────────────────────────────────────────────────────────────────────────────
TEST(LeaseCatalogRegression, ReplayDeterminismRegression) {
    const auto script = rc::testing::goldenScriptV4Lease();
    rc::CatalogReplayer one, two, bytes;
    for (std::size_t i = 0; i < script.size(); ++i) {
        const auto off = static_cast<int64_t>(i);
        ASSERT_TRUE(one.feedDecoded(off, script[i]).has_value()) << i;
        ASSERT_TRUE(two.feedDecoded(off, script[i]).has_value()) << i;
        const auto enc = rc::encode(script[i]);
        ASSERT_TRUE(enc.has_value()) << i;
        ASSERT_TRUE(bytes.feed(off, *enc).has_value()) << i;
        EXPECT_EQ(one.state().digest(), two.state().digest()) << "offset " << i;
        EXPECT_EQ(one.state().digest(), bytes.state().digest()) << "feed == feedDecoded, offset " << i;
    }
}

// 15 ────────────────────────────────────────────────────────────────────────────────────────────────────────────────
TEST(CatalogV4LeaseGoldenRegression, LogReplaysToPinnedFingerprints) {
    std::ifstream in(RUNE_CATALOG_GOLDEN_DIR "/v4/lease.rcat", std::ios::binary);
    ASSERT_TRUE(in) << "missing golden log";
    auto records = rc::readRcat(in);
    ASSERT_TRUE(records.has_value()) << records.error();
    ASSERT_EQ(records->size(), std::size(rc::golden::kLeaseV4Digests));
    rc::CatalogReplayer r;
    for (std::size_t i = 0; i < records->size(); ++i) {
        auto a = r.feed((*records)[i].first, (*records)[i].second);
        ASSERT_TRUE(a.has_value()) << rc::describe(a.error());
        EXPECT_EQ(r.state().digest(), rc::golden::kLeaseV4Digests[i]) << "offset " << i;
    }
    const auto& s = r.state();
    const auto* inst = s.topicByName(rc::kDefaultNsId, "inst");
    ASSERT_NE(inst, nullptr);
    EXPECT_EQ(inst->lease, rp::LeaseSpec{}) << "the second inst is a fresh default";
    EXPECT_EQ(inst->version, 1u);
    const auto* view = s.viewByName(rc::kDefaultNsId, "inst.leases");
    ASSERT_NE(view, nullptr);
    EXPECT_EQ(view->kind, rc::ViewKind::LeaseIndex);
    EXPECT_EQ(view->leaseTopicObjectId, inst->objectId);
    EXPECT_EQ(s.viewByName(rc::kDefaultNsId, "inst_ids"), nullptr);
    EXPECT_EQ(s.tombstones().size(), 3u) << "inst_ids, then inst and inst.leases";
}

TEST(CatalogV4LeaseGoldenRegression, TheGoldenScriptStillProducesTheCheckedInBytesRegression) {
    std::ifstream in(RUNE_CATALOG_GOLDEN_DIR "/v4/lease.rcat", std::ios::binary);
    auto records = rc::readRcat(in);
    ASSERT_TRUE(records.has_value());
    const auto script = rc::testing::goldenScriptV4Lease();
    ASSERT_EQ(records->size(), script.size());
    for (std::size_t i = 0; i < script.size(); ++i)
        EXPECT_EQ((*records)[i].second, rc::encode(script[i]).value()) << "offset " << i;
}

TEST(CatalogV4LeaseGoldenRegression, TheGoldenLogDecodesToTheScriptEventByEvent) {
    std::ifstream in(RUNE_CATALOG_GOLDEN_DIR "/v4/lease.rcat", std::ios::binary);
    auto records = rc::readRcat(in);
    ASSERT_TRUE(records.has_value());
    const auto script = rc::testing::goldenScriptV4Lease();
    ASSERT_EQ(records->size(), script.size());
    for (std::size_t i = 0; i < script.size(); ++i) {
        auto e = rc::decode((*records)[i].second);
        ASSERT_TRUE(e.has_value()) << "offset " << i;
        EXPECT_EQ(*e, script[i]) << "offset " << i;
        EXPECT_EQ(e->formatVer, rc::kFormatV4);
    }
}

// 16 ────────────────────────────────────────────────────────────────────────────────────────────────────────────────
TEST(LeaseCatalogRegression, KeyWidthRegression) {
    using cd::PrimType;
    EXPECT_TRUE(cd::isLeaseTopicKeyType(PrimType::FixedStr, 128));
    EXPECT_FALSE(cd::isLeaseTopicKeyType(PrimType::FixedStr, 129));
    EXPECT_TRUE(cd::isLeaseTopicKeyType(PrimType::FixedStr, 17));
    EXPECT_FALSE(cd::isTopicKeyType(PrimType::FixedStr, 17)) << "isTopicKeyType is unchanged";
    EXPECT_TRUE(cd::isTopicKeyType(PrimType::FixedStr, 16));
    EXPECT_EQ(cd::isLeaseTopicKeyType(PrimType::FixedStr, 0), cd::isTopicKeyType(PrimType::FixedStr, 0));
    EXPECT_FALSE(cd::isLeaseTopicKeyType(PrimType::FixedStr, 0));
    for (std::uint8_t t = 0; t <= static_cast<std::uint8_t>(PrimType::FixedStr); ++t) {
        const auto p = static_cast<PrimType>(t);
        if (p == PrimType::FixedStr) continue;
        EXPECT_EQ(cd::isLeaseTopicKeyType(p, 0), cd::isTopicKeyType(p, 0)) << static_cast<int>(t);
        EXPECT_EQ(cd::isLeaseTopicKeyType(p, 8), cd::isTopicKeyType(p, 8)) << static_cast<int>(t);
    }
    static_assert(cd::kMaxLeaseKeyBytes == rp::kMaxLeaseKeyBytes);
}

// 17 ────────────────────────────────────────────────────────────────────────────────────────────────────────────────
TEST(LeaseCatalogRegression, OldFormatLeaseKeysDecodeBadEnumRegression) {
    for (const uint16_t f : {rc::kFormatV1, rc::kFormatV2, rc::kFormatV3}) {
        for (uint8_t key = 7; key <= 12; ++key) {
            auto bytes = rc::encode(v4(cfg({{static_cast<rc::ConfigKey>(key), 5000}}), "inst", 1, 1)).value();
            // the body is the same for formatVer 1..4 (an AlterTopicConfig has no key block or lease trailer); only the
            // header differs: formatVer 1 has no nsId after the kind
            bytes[0] = static_cast<uint8_t>(f);
            bytes[1] = 0;
            if (f == rc::kFormatV1) bytes.erase(bytes.begin() + 4, bytes.begin() + 8);
            const auto d = rc::decode(bytes);
            ASSERT_FALSE(d.has_value()) << "formatVer " << f << " key " << int{key};
            EXPECT_EQ(d.error(), rc::DecodeError::BadEnum) << "formatVer " << f << " key " << int{key};
        }
    }
    // control: key 6 decodes at formatVer 1 through the same patching
    auto ok = rc::encode(v4(cfg({{rc::ConfigKey::MaxCompactionLagMs, 5}}), "inst", 1, 1)).value();
    ok[0] = static_cast<uint8_t>(rc::kFormatV1);
    ok.erase(ok.begin() + 4, ok.begin() + 8);
    EXPECT_TRUE(rc::decode(ok).has_value());
}

// 18 ────────────────────────────────────────────────────────────────────────────────────────────────────────────────
TEST(LeaseCatalogRegression, CatalogDiffSeesLeaseRegression) {
    rc::CatalogState a, b;
    rp::LeaseSpec other;
    other.checkpointRate = 2'000;
    leaseState(a);
    leaseState(b, other);
    const auto d = rc::diff(a, b);
    ASSERT_EQ(d.size(), 1u) << "only the topic differs; the lease view is the same";
    EXPECT_EQ(d[0].type, rc::ObjectType::Topic);
    EXPECT_EQ(d[0].name, "inst");
    EXPECT_EQ(d[0].change, rc::DiffEntry::Change::Changed);
    EXPECT_NE(d[0].fingerprintA, d[0].fingerprintB);
    EXPECT_EQ(d[0].fields, (std::vector<std::string>{"lease"}));
    EXPECT_TRUE(rc::diff(a, a).empty());

    // a lease view against a plan view of the same definition differs in the kind
    rc::ViewObject lv = *a.viewByName(rc::kDefaultNsId, "inst.leases");
    rc::ViewObject pv = lv;
    pv.kind = rc::ViewKind::Plan;
    EXPECT_EQ(rc::changedFields(lv, pv), (std::vector<std::string>{"view.kind"}));
    EXPECT_NE(rc::layeredFingerprint(lv), rc::layeredFingerprint(pv));
}

// 19 ────────────────────────────────────────────────────────────────────────────────────────────────────────────────
TEST(LeaseCatalogRegression, StateRulesAtOldFormatsRegression) {
    for (const uint16_t f : {rc::kFormatV2, rc::kFormatV3}) {
        auto fresh = [&](rc::CatalogState& s) { leaseState(s); };
        {   // DropView of the lease view
            rc::CatalogState s;
            fresh(s);
            const auto r = put(s, event(f, rc::DropViewOps{1}, "inst.leases", 2, 1));
            ASSERT_FALSE(r.has_value()) << f;
            EXPECT_TRUE(has(r.error().detail, "drop the lease topic")) << r.error().detail;
        }
        {   // AlterView of the lease view
            rc::CatalogState s;
            fresh(s);
            rc::ViewDef def;
            def.viewTopicId = 2; def.sinkPartitions = 2; def.sourceNames = {"inst"};
            const auto r = put(s, event(f, rc::AlterViewOps{def}, "inst.leases", 2, 1));
            ASSERT_FALSE(r.has_value()) << f;
            EXPECT_TRUE(has(r.error().detail, "a lease view cannot be altered")) << r.error().detail;
        }
        {   // DropTopic takes the view along
            rc::CatalogState s;
            fresh(s);
            const auto r = put(s, event(f, rc::DropTopicOps{0}, "inst", 1, 1));
            ASSERT_TRUE(r.has_value()) << f << r.error().detail;
            EXPECT_TRUE(s.topics().empty());
            EXPECT_TRUE(s.views().empty());
            EXPECT_EQ(r->leaseViewObjectId, 2u);
        }
        {   // retention.ms on a lease topic
            rc::CatalogState s;
            fresh(s);
            const auto r = put(s, event(f, cfg({{rc::ConfigKey::RetentionMs, 1000}}), "inst", 1, 1));
            ASSERT_FALSE(r.has_value()) << f;
            EXPECT_TRUE(has(r.error().detail, "a lease topic is compacted and keeps every live lease")) << r.error().detail;
        }
        {   // AlterSchema adding lease_x
            rc::CatalogState s;
            fresh(s);
            const auto r = put(s, event(f, rc::AlterSchemaOps{{op(cd::AlterOpType::Add, 5, "lease_x")}}, "inst", 1, 1));
            ASSERT_FALSE(r.has_value()) << f;
            EXPECT_TRUE(has(r.error().detail, "lease_ field names are reserved")) << r.error().detail;
        }
        {   // NamespaceDeleted: pendingViews excludes the lease view
            rc::CatalogState s;
            ASSERT_TRUE(put(s, event(f, rc::NamespaceCreatedOps{"ns1", {}}, "ns1", 0, 0, 1)).has_value());
            ASSERT_TRUE(put(s, v4(leaseOps(0), "inst", 1, 0, 1)).has_value());
            const auto r = put(s, event(f, rc::NamespaceDeletedOps{}, "ns1", 0, 1, 1));
            ASSERT_TRUE(r.has_value()) << f << r.error().detail;
            const auto* d = std::get_if<rc::NsDeleting>(&s.namespaces().byId(1)->state);
            ASSERT_NE(d, nullptr);
            EXPECT_TRUE(d->pendingViews.empty()) << f;
            EXPECT_EQ(d->pendingTopics, (std::set<uint64_t>{1})) << f;
        }
    }
}

// 20 ────────────────────────────────────────────────────────────────────────────────────────────────────────────────
TEST(LeaseCatalogRegression, LeaseDropCompletesNamespaceRegression) {
    rc::CatalogState s;
    ASSERT_TRUE(put(s, v4(rc::NamespaceCreatedOps{"ns1", {}}, "ns1", 0, 0, 1)).has_value());
    ASSERT_TRUE(put(s, v4(leaseOps(0), "inst", 1, 0, 1)).has_value());
    ASSERT_TRUE(put(s, v4(rc::NamespaceDeletedOps{}, "ns1", 0, 1, 1)).has_value());
    uint32_t version = 2;
    for (const uint8_t step : {rc::TxnsAborted, rc::GroupsTombstoned, rc::TxnStateTombstoned, rc::SecurityPurged}) {
        auto st = put(s, v4(rc::NamespaceDeleteStepOps{step}, "ns1", 0, version++, 1));
        ASSERT_TRUE(st.has_value()) << st.error().detail;
        EXPECT_FALSE(st->nsErased) << "the lease topic is still to be dropped";
    }
    ASSERT_NE(s.namespaces().byId(1), nullptr);
    ASSERT_NE(s.viewByName(1, "inst.leases"), nullptr);
    const auto dt = put(s, v4(rc::DropTopicOps{0}, "inst", 1, 1, 1));
    ASSERT_TRUE(dt.has_value()) << dt.error().detail;
    EXPECT_TRUE(dt->nsErased);
    EXPECT_EQ(dt->leaseViewObjectId, 2u);
    EXPECT_EQ(s.namespaces().byId(1), nullptr);
    EXPECT_TRUE(s.views().empty());
    EXPECT_TRUE(s.topics().empty());
    EXPECT_EQ(s.viewByName(1, "inst.leases"), nullptr);
}

// 21 ────────────────────────────────────────────────────────────────────────────────────────────────────────────────
TEST(LeaseCatalogRegression, LeaseOnlyAlterRequestIdRegression) {
    auto alter = [](uint32_t prev, uint64_t lo, uint32_t nsId = 0) {
        auto e = v4(cfg({{rc::ConfigKey::LeaseTtlMs, 5000}}), "inst", 1, prev, nsId);
        e.principal.requestId = rp::RequestId{7, lo};
        e.principal.requestFingerprint = 0xABCDEF0123ull;
        return e;
    };
    {
        rc::CatalogState s;
        leaseState(s);
        const int64_t at = s.appliedOffset() + 1;
        ASSERT_TRUE(put(s, alter(1, 1)).has_value());
        const auto rec = s.request(rp::RequestId{7, 1});
        ASSERT_TRUE(rec.has_value());
        EXPECT_EQ(rec->offset, at);
        EXPECT_EQ(rec->kind, rc::EventKind::AlterTopicConfig);
        EXPECT_EQ(rec->objectId, 1u);
        EXPECT_EQ(rec->newVersion, 2u);
        EXPECT_EQ(rec->requestFingerprint, 0xABCDEF0123ull);
        // a second event with the same requestId
        const auto again = put(s, alter(2, 1));
        ASSERT_FALSE(again.has_value());
        EXPECT_TRUE(has(again.error().detail, "already applied")) << again.error().detail;
        EXPECT_EQ(s.topicByObjectId(1)->version, 2u);
    }
    {   // a DELETING namespace refuses it; a missing one is BadNsId
        rc::CatalogState s;
        ASSERT_TRUE(put(s, v4(rc::NamespaceCreatedOps{"ns1", {}}, "ns1", 0, 0, 1)).has_value());
        ASSERT_TRUE(put(s, v4(leaseOps(0), "inst", 1, 0, 1)).has_value());
        const auto missing = put(s, alter(1, 2, 9));
        ASSERT_FALSE(missing.has_value());
        EXPECT_EQ(missing.error().code, Code::BadNsId);
        ASSERT_TRUE(put(s, v4(rc::NamespaceDeletedOps{}, "ns1", 0, 1, 1)).has_value());
        const auto deleting = put(s, alter(1, 3, 1));
        ASSERT_FALSE(deleting.has_value());
        EXPECT_EQ(deleting.error().code, Code::Precondition);
        EXPECT_TRUE(has(deleting.error().detail, "DELETING")) << deleting.error().detail;
        EXPECT_FALSE(s.request(rp::RequestId{7, 3}).has_value());
    }
}
