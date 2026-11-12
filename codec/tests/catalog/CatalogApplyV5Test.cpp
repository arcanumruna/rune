// Schema retire (#419): catalog formatVer 5 — the RetireSchemaVersions kind (event layout, apply rules, the retire floor
// in the state digest and across later ALTERs, replay).
#include <rune/catalog/CatalogEvent.h>
#include <rune/catalog/CatalogFile.h>
#include <rune/catalog/CatalogReplayer.h>
#include <rune/catalog/CatalogState.h>
#include <rune/catalog/testing/GoldenScript.h>
#include <rune/codec/Descriptor.h>
#include <rune/codec/PrimType.h>
#include <rune/codec/Serialize.h>
#include <rune/protocol/LeaseSpec.h>
#include <gtest/gtest.h>

#include <cstdint>
#include <fstream>
#include <span>
#include <string>
#include <utility>
#include <vector>

#include "CatalogGoldenPins.h"

namespace rc = rune::catalog;
namespace cd = rune::codec;

namespace {

using Code = rc::ApplyError::Code;

std::vector<uint8_t> keyedBlob() {
    cd::DescriptorBuilder b("t");
    b.add("id", cd::PrimType::Int64, 1, /*isKey=*/true).add("v", cd::PrimType::Int32);
    return cd::serialize(b.build());
}
rc::CreateTopicOps typedCreate(int32_t topicId = 0) {
    rc::CreateTopicOps o;
    o.topicId = topicId; o.partitions = 2;
    o.schema = rc::SchemaDef{.keyField = "id", .validate = true, .descriptorBlob = keyedBlob(), .nextTag = 2};
    return o;
}
rc::CatalogEvent event(uint16_t formatVer, rc::Ops ops, std::string name, uint64_t objectId, uint32_t prev, uint32_t nsId = 0) {
    rc::CatalogEvent e;
    e.formatVer = formatVer; e.nsId = nsId;
    e.kind = rc::kindOf(ops); e.ops = std::move(ops); e.objectName = std::move(name);
    e.objectId = objectId; e.prevVersion = prev; e.newVersion = prev + 1;
    return e;
}
/// The leader's way: preview the fingerprint, stamp it, apply at the next offset.
std::expected<rc::Applied, rc::ApplyError> put(rc::CatalogState& s, rc::CatalogEvent e) {
    auto fp = rc::previewFingerprint(s, e);
    if (!fp) return std::unexpected(fp.error());
    e.fingerprint = *fp;
    return rc::apply(s, e, s.appliedOffset() + 1);
}
rc::AlterSchemaOps addField(uint16_t tag) {
    rc::AlterSchemaOps o;
    o.ops.push_back({.type = cd::AlterOpType::Add, .tag = tag, .name = "f" + std::to_string(tag), .primType = cd::PrimType::Int32});
    return o;
}
const rc::TopicObject& topic(const rc::CatalogState& s) { return *s.topicByName(rc::kDefaultNsId, "t"); }
uint32_t catalogVersion(const rc::CatalogState& s) { return topic(s).version; }

rc::CatalogEvent retire(const rc::CatalogState& s, uint16_t below, uint16_t formatVer = rc::kFormatV5) {
    return event(formatVer, rc::RetireSchemaVersionsOps{.below = below}, "t", 1, catalogVersion(s));
}
rc::CatalogEvent alter(const rc::CatalogState& s, uint16_t tag, uint16_t formatVer = rc::kFormatV5) {
    return event(formatVer, addField(tag), "t", 1, catalogVersion(s));
}
/// Typed topic "t" (objectId 1) at schemaVersion `versions` (versions - 1 ALTERs), all in formatVer 5.
void typedState(rc::CatalogState& s, uint16_t versions) {
    ASSERT_TRUE(put(s, event(rc::kFormatV5, typedCreate(), "t", 1, 0)).has_value());
    for (uint16_t v = 2; v <= versions; ++v) {
        const auto a = put(s, alter(s, static_cast<uint16_t>(v)));   // tags 2.. are free: nextTag starts at 2
        ASSERT_TRUE(a.has_value()) << a.error().detail;
    }
    ASSERT_EQ(topic(s).schema->schemaVersion, versions);
}
bool has(const std::string& hay, const char* needle) { return hay.find(needle) != std::string::npos; }

} // namespace

// ── the event layout ──────────────────────────────────────────────────────────
TEST(CatalogApplyRegression, RetireEventRoundTripsAndEveryTruncationIsRefusedRegression) {
    auto e = event(rc::kFormatV5, rc::RetireSchemaVersionsOps{.below = 0x0A0B}, "t", 1, 3);
    e.fingerprint = 0x1122334455667788ull;
    ASSERT_EQ(e.kind, static_cast<rc::EventKind>(10));
    EXPECT_EQ(rc::kindName(e.kind), "RetireSchemaVersions");
    const auto bytes = rc::encode(e);
    ASSERT_TRUE(bytes.has_value());
    // the ops blob is the last field: u8 opsTag (= kind) + u16 below, little-endian
    ASSERT_GE(bytes->size(), 7u);
    EXPECT_EQ(std::vector<uint8_t>(bytes->end() - 7, bytes->end()), (std::vector<uint8_t>{3, 0, 0, 0, 10, 0x0B, 0x0A}));
    const auto d = rc::decode(*bytes);
    ASSERT_TRUE(d.has_value());
    EXPECT_EQ(*d, e);
    EXPECT_EQ(std::get<rc::RetireSchemaVersionsOps>(d->ops).below, 0x0A0B);
    for (std::size_t n = 0; n < bytes->size(); ++n) {
        const auto cut = rc::decode(std::span<const uint8_t>(bytes->data(), n));
        ASSERT_FALSE(cut.has_value()) << "prefix " << n << " decoded";
        EXPECT_EQ(cut.error(), rc::DecodeError::Truncated) << "prefix " << n;
    }
    auto trailing = *bytes;
    trailing.push_back(0);
    EXPECT_FALSE(rc::decode(trailing).has_value());
}

TEST(CatalogApplyRegression, RetireKindIsRefusedUnderFormatOneToFourRegression) {
    for (const uint16_t fv : {rc::kFormatV1, rc::kFormatV2, rc::kFormatV3, rc::kFormatV4}) {
        auto e = event(fv, rc::RetireSchemaVersionsOps{.below = 2}, "t", 1, 1);
        std::vector<uint8_t> buf{1};
        const auto enc = rc::encodeInto(e, buf);
        ASSERT_FALSE(enc.has_value()) << fv;
        EXPECT_EQ(enc.error(), rc::EncodeError::UnknownKind) << fv;
        EXPECT_TRUE(buf.empty());
        // the same bytes under formatVer 5, relabelled to fv: decode refuses the kind as unknown
        auto ok = rc::encode(event(rc::kFormatV5, rc::RetireSchemaVersionsOps{.below = 2}, "t", 1, 1));
        ASSERT_TRUE(ok.has_value());
        (*ok)[0] = static_cast<uint8_t>(fv & 0xFF);
        (*ok)[1] = static_cast<uint8_t>(fv >> 8);
        if (fv == rc::kFormatV1) continue;   // v1 has no nsId field: the layout differs, covered by the encode refusal
        const auto d = rc::decode(*ok);
        ASSERT_FALSE(d.has_value()) << fv;
        EXPECT_EQ(d.error(), rc::DecodeError::UnknownKind) << fv;
    }
    // formatVer 6 does not exist
    auto bytes = rc::encode(event(rc::kFormatV5, rc::RetireSchemaVersionsOps{.below = 2}, "t", 1, 1)).value();
    bytes[0] = 6;
    EXPECT_EQ(rc::decode(bytes).error(), rc::DecodeError::UnknownFormatVer);
}

TEST(CatalogApplyRegression, FormatFiveDecodesEveryFormatFourBodyTheSameRegression) {
    rc::CatalogState s;
    ASSERT_TRUE(put(s, event(rc::kFormatV4, typedCreate(), "t", 1, 0)).has_value());
    const std::vector<rc::Ops> bodies = {typedCreate(1), rc::DropTopicOps{.topicId = 1},
                                         rc::AlterTopicConfigOps{.changes = {{rc::ConfigKey::RetentionMs, 1000}}},
                                         addField(2), rc::SetCompatibilityOps{.mode = cd::CompatMode::Forward}};
    for (const rc::Ops& ops : bodies) {
        auto v4 = event(rc::kFormatV4, ops, "t", 1, 1);
        auto bytes = rc::encode(v4).value();
        auto v5 = v4;
        v5.formatVer = rc::kFormatV5;
        const auto bytes5 = rc::encode(v5).value();
        ASSERT_EQ(bytes.size(), bytes5.size());
        EXPECT_EQ(std::vector<uint8_t>(bytes.begin() + 2, bytes.end()), std::vector<uint8_t>(bytes5.begin() + 2, bytes5.end()))
            << rc::kindName(v4.kind) << ": the body bytes are the same";
        bytes[0] = 5;
        const auto d = rc::decode(bytes);
        ASSERT_TRUE(d.has_value()) << rc::kindName(v4.kind);
        EXPECT_EQ(*d, v5);
    }
}

// ── apply rules ───────────────────────────────────────────────────────────────
TEST(CatalogApplyRegression, RetireOfAnUntypedOrDroppedOrUnknownTopicIsRefusedRegression) {
    rc::CatalogState s;
    ASSERT_TRUE(put(s, event(rc::kFormatV5, rc::CreateTopicOps{.topicId = 0}, "t", 1, 0)).has_value());
    auto r = put(s, retire(s, 2));
    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error().code, Code::Precondition);
    EXPECT_TRUE(has(r.error().detail, "not a typed topic")) << r.error().detail;

    rc::CatalogState d;
    typedState(d, 2);
    ASSERT_TRUE(put(d, event(rc::kFormatV5, rc::DropTopicOps{.topicId = 0}, "t", 1, catalogVersion(d))).has_value());
    auto gone = put(d, event(rc::kFormatV5, rc::RetireSchemaVersionsOps{.below = 2}, "t", 1, 3));
    ASSERT_FALSE(gone.has_value());
    EXPECT_EQ(gone.error().code, Code::Precondition);

    rc::CatalogState e;
    auto nsGone = put(e, event(rc::kFormatV5, rc::RetireSchemaVersionsOps{.below = 2}, "t", 1, 1, /*nsId=*/9));
    ASSERT_FALSE(nsGone.has_value());
    EXPECT_EQ(nsGone.error().code, Code::BadNsId);
}

TEST(CatalogApplyRegression, RetireWithAWrongVersionChainOrNameIsRefusedRegression) {
    rc::CatalogState s;
    typedState(s, 3);
    auto e = retire(s, 2);
    e.prevVersion += 1; e.newVersion += 1;
    auto r = put(s, e);
    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error().code, Code::Precondition);
    auto named = retire(s, 2);
    named.objectName = "other";
    EXPECT_FALSE(put(s, named).has_value());
    EXPECT_EQ(topic(s).schema->retiredBelow, 1) << "a refused retire leaves the floor";
}

TEST(CatalogApplyRegression, RetireFloorRulesRegression) {
    struct Case { const char* name; uint16_t versions; uint16_t firstBelow; uint16_t below; bool ok; const char* why; };
    const Case cases[] = {
        {"N == 1 (the floor is 1): refused", 3, 0, 1, false, "already retired"},
        {"N == 0: refused", 3, 0, 0, false, "already retired"},
        {"N == 2 of 3: ok", 3, 0, 2, true, nullptr},
        {"N == current: ok", 3, 0, 3, true, nullptr},
        {"N == current + 1: refused", 3, 0, 4, false, "above the current version"},
        {"N far above: refused", 3, 0, 60000, false, "above the current version"},
        {"a one-version topic has nothing to retire (N == 1): refused", 1, 0, 1, false, "already retired"},
        {"a one-version topic, N == 2: refused (the current version)", 1, 0, 2, false, "above the current version"},
        {"N == floor: refused", 4, 3, 3, false, "already retired"},
        {"N < floor: refused", 4, 3, 2, false, "already retired"},
        {"N > floor: ok", 4, 2, 4, true, nullptr},
    };
    for (const Case& c : cases) {
        rc::CatalogState s;
        typedState(s, c.versions);
        if (c.firstBelow != 0) {
            const auto first = put(s, retire(s, c.firstBelow));
            ASSERT_TRUE(first.has_value()) << c.name << ": " << first.error().detail;
        }
        const auto r = put(s, retire(s, c.below));
        if (c.ok) {
            ASSERT_TRUE(r.has_value()) << c.name << ": " << r.error().detail;
            EXPECT_EQ(topic(s).schema->retiredBelow, c.below) << c.name;
        } else {
            ASSERT_FALSE(r.has_value()) << c.name;
            EXPECT_EQ(r.error().code, Code::Precondition) << c.name;
            EXPECT_TRUE(has(r.error().detail, c.why)) << c.name << ": " << r.error().detail;
        }
    }
}

TEST(CatalogApplyRegression, RetireOnlyMovesTheFloorAndTheCatalogVersionRegression) {
    rc::CatalogState s;
    typedState(s, 3);
    const rc::TopicObject before = topic(s);
    ASSERT_EQ(before.schema->retiredBelow, 1);
    const auto a = put(s, retire(s, 3));
    ASSERT_TRUE(a.has_value()) << a.error().detail;
    EXPECT_EQ(a->kind, static_cast<rc::EventKind>(10));
    rc::TopicObject expect = before;
    expect.schema->retiredBelow = 3;
    expect.version = before.version + 1;
    EXPECT_EQ(topic(s), expect) << "schemaVersion, blob, nextTag, compat, key features, config and lease are unchanged";
}

TEST(CatalogApplyRegression, TwoRetiresInSequenceAndAlterAfterRetireKeepTheFloorRegression) {
    rc::CatalogState s;
    typedState(s, 4);
    ASSERT_TRUE(put(s, retire(s, 2)).has_value());
    EXPECT_EQ(topic(s).schema->retiredBelow, 2);
    ASSERT_TRUE(put(s, retire(s, 3)).has_value());
    EXPECT_EQ(topic(s).schema->retiredBelow, 3);
    // an ALTER after the retire (under the v5 and the v4 rules) keeps the floor and moves the schema version
    ASSERT_TRUE(put(s, alter(s, 5)).has_value());
    EXPECT_EQ(topic(s).schema->schemaVersion, 5);
    EXPECT_EQ(topic(s).schema->retiredBelow, 3);
    ASSERT_TRUE(put(s, alter(s, 6, rc::kFormatV4)).has_value());
    EXPECT_EQ(topic(s).schema->schemaVersion, 6);
    EXPECT_EQ(topic(s).schema->retiredBelow, 3);
    // so does a SET COMPATIBILITY and a config change
    ASSERT_TRUE(put(s, event(rc::kFormatV5, rc::SetCompatibilityOps{.mode = cd::CompatMode::Forward}, "t", 1, catalogVersion(s))).has_value());
    EXPECT_EQ(topic(s).schema->schemaVersion, 7);
    EXPECT_EQ(topic(s).schema->retiredBelow, 3);
    ASSERT_TRUE(put(s, event(rc::kFormatV5, rc::AlterTopicConfigOps{.changes = {{rc::ConfigKey::RetentionMs, 5000}}}, "t", 1,
                              catalogVersion(s))).has_value());
    EXPECT_EQ(topic(s).schema->retiredBelow, 3);
    // the floor never passes the current version, which moved on: a retire up to it is fine
    ASSERT_TRUE(put(s, retire(s, 7)).has_value());
    EXPECT_EQ(topic(s).schema->retiredBelow, 7);
}

TEST(CatalogApplyRegression, RetireAfterAlterIsAllowedRegression) {
    rc::CatalogState s;
    typedState(s, 2);
    ASSERT_TRUE(put(s, alter(s, 3)).has_value());
    ASSERT_TRUE(put(s, retire(s, 3)).has_value());
    EXPECT_EQ(topic(s).schema->retiredBelow, 3);
    EXPECT_EQ(topic(s).schema->schemaVersion, 3);
}

TEST(CatalogApplyRegression, RetireReplaysToTheSameStateAndDigestAndTheFloorIsInTheDigestRegression) {
    // one log, applied twice (the leader's apply and a replica's replay)
    std::vector<rc::CatalogEvent> log;
    rc::CatalogState leader;
    auto step = [&](rc::CatalogEvent e) {
        auto fp = rc::previewFingerprint(leader, e);
        ASSERT_TRUE(fp.has_value()) << fp.error().detail;
        e.fingerprint = *fp;
        ASSERT_TRUE(rc::apply(leader, e, leader.appliedOffset() + 1).has_value());
        log.push_back(e);
    };
    step(event(rc::kFormatV5, typedCreate(), "t", 1, 0));
    step(alter(leader, 2));
    step(alter(leader, 3));
    const uint64_t unretired = leader.digest();
    step(retire(leader, 3));
    EXPECT_NE(leader.digest(), unretired);

    rc::CatalogState replica;
    for (std::size_t i = 0; i < log.size(); ++i) {
        const auto bytes = rc::encode(log[i]);
        ASSERT_TRUE(bytes.has_value());
        const auto decoded = rc::decode(*bytes);
        ASSERT_TRUE(decoded.has_value());
        ASSERT_TRUE(rc::apply(replica, *decoded, static_cast<int64_t>(i)).has_value()) << i;
    }
    EXPECT_EQ(replica.digest(), leader.digest());
    EXPECT_EQ(topic(replica), topic(leader));
    EXPECT_EQ(topic(replica).schema->retiredBelow, 3);

    // the same state with the floor left out (every event but the retire) digests differently, and so does a
    // fingerprint that disagrees: an event stamped without the floor is refused as a mismatch
    rc::CatalogState noRetire;
    for (std::size_t i = 0; i + 1 < log.size(); ++i) ASSERT_TRUE(rc::apply(noRetire, log[i], static_cast<int64_t>(i)).has_value());
    EXPECT_EQ(noRetire.digest(), unretired);
    EXPECT_NE(noRetire.digest(), replica.digest());

    // a floor of 2 and a floor of 3 over the same history are two different states
    rc::CatalogState other;
    for (std::size_t i = 0; i + 1 < log.size(); ++i) ASSERT_TRUE(rc::apply(other, log[i], static_cast<int64_t>(i)).has_value());
    ASSERT_TRUE(put(other, retire(other, 2)).has_value());
    EXPECT_NE(other.digest(), replica.digest());
}

TEST(CatalogApplyRegression, AStateWithEveryFloorAtOneKeepsTodaysDigestsRegression) {
    // the checked-in v3 and v4 golden logs hold no retire: their pinned digests must not move, and every typed
    // topic reads a floor of 1
    struct Log { const char* path; const std::uint64_t* pins; std::size_t n; };
    const Log logs[] = {{RUNE_CATALOG_GOLDEN_DIR "/v3/basic.rcat", rc::golden::kBasicV3Digests, std::size(rc::golden::kBasicV3Digests)},
                        {RUNE_CATALOG_GOLDEN_DIR "/v4/lease.rcat", rc::golden::kLeaseV4Digests, std::size(rc::golden::kLeaseV4Digests)}};
    for (const Log& l : logs) {
        std::ifstream in(l.path, std::ios::binary);
        ASSERT_TRUE(in) << l.path;
        auto records = rc::readRcat(in);
        ASSERT_TRUE(records.has_value()) << l.path;
        ASSERT_EQ(records->size(), l.n);
        rc::CatalogReplayer r;
        for (std::size_t i = 0; i < records->size(); ++i) {
            ASSERT_TRUE(r.feed((*records)[i].first, (*records)[i].second).has_value()) << l.path << " " << i;
            EXPECT_EQ(r.state().digest(), l.pins[i]) << l.path << " offset " << i;
        }
        std::size_t typed = 0;
        for (const rc::TopicObject* t : r.state().topics())
            if (t->schema) { ++typed; EXPECT_EQ(t->schema->retiredBelow, 1) << t->name; }
        EXPECT_GT(typed, 0u) << l.path;
    }
}

TEST(CatalogApplyRegression, RetireOnALeaseTopicIsAllowedRegression) {
    // the lease rules look at the system columns, never at old versions: a lease topic retires like any typed topic
    cd::DescriptorBuilder b("inst");
    b.add("lease_version", cd::PrimType::Int64).add("lease_ttl_ms", cd::PrimType::UInt64)
        .add("lease_holder", cd::PrimType::UInt64).add("lease_remaining_ms", cd::PrimType::UInt64)
        .addFixedStr("id", 64, 1, /*isKey=*/true);
    rc::CreateTopicOps o;
    o.topicId = 0; o.partitions = 2; o.config.cleanupPolicy = 1;
    o.schema = rc::SchemaDef{.keyField = "id", .validate = true, .descriptorBlob = cd::serialize(b.build()), .nextTag = 5};
    o.lease = rune::protocol::LeaseSpec{};
    rc::CatalogState s;
    const auto c = put(s, event(rc::kFormatV5, o, "inst", 1, 0));
    ASSERT_TRUE(c.has_value()) << c.error().detail;
    ASSERT_TRUE(put(s, event(rc::kFormatV5, addField(5), "inst", 1, 1)).has_value());
    const auto r = put(s, event(rc::kFormatV5, rc::RetireSchemaVersionsOps{.below = 2}, "inst", 1, 2));
    ASSERT_TRUE(r.has_value()) << r.error().detail;
    EXPECT_EQ(s.topicByName(rc::kDefaultNsId, "inst")->schema->retiredBelow, 2);
    EXPECT_TRUE(s.topicByName(rc::kDefaultNsId, "inst")->lease.has_value());
}
