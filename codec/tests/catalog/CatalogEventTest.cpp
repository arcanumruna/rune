// Catalog history (spec §4): the frozen formatVer-1 event encoding.
#include <rune/catalog/CatalogEvent.h>
#include <gtest/gtest.h>

#include <cstdlib>
#include <functional>
#include <new>
#include <string>

// ── allocation probe (test binary only): the largest single allocation while armed ──
namespace {
bool        gProbeArmed = false;
std::size_t gProbePeak  = 0;
} // namespace
void* operator new(std::size_t n) {
    if (gProbeArmed && n > gProbePeak) gProbePeak = n;
    if (void* p = std::malloc(n ? n : 1)) return p;
    throw std::bad_alloc();
}
void operator delete(void* p) noexcept { std::free(p); }
void operator delete(void* p, std::size_t) noexcept { std::free(p); }

namespace rc = rune::catalog;

namespace {
rc::CatalogEvent baseEvent(rc::Ops ops) {
    rc::CatalogEvent e;
    e.kind = rc::kindOf(ops);
    e.objectId = 7;
    e.objectName = "players";
    e.prevVersion = 0;
    e.newVersion = 1;
    e.statement = "topic players { id int64 } partitions 3 key id;";
    e.ops = std::move(ops);
    e.principal = rc::Principal{.sessionId = 42, .clientName = "cli", .sourceAddress = "aeron:udp?endpoint=10.0.0.5:40500",
                                .authenticated = "", .requestId = {.hi = 0x0123456789ABCDEFull, .lo = 0xABCDEFull},
                                .requestFingerprint = 0x5151515151515151ull};
    e.timestampMs = 1'760'000'000'000;
    e.fingerprint = 0x1122334455667788ull;
    return e;
}
rc::CreateTopicOps typedCreate() {
    rc::CreateTopicOps o;
    o.topicId = 3; o.partitions = 3; o.replicationFactor = 3; o.persistence = rc::Persistence::Disk;
    o.config.retentionMs = 86'400'000;
    o.schema = rc::SchemaDef{.keyField = "id", .validate = true, .descriptorBlob = {1, 2, 3, 4}, .nextTag = 1};
    return o;
}
rc::ViewDef viewDef() {
    rc::ViewDef v;
    v.viewTopicId = 9; v.sinkPartitions = 1; v.persistence = rc::Persistence::Transient; v.keyField = "id";
    v.sourceNames = {"players"};
    v.normalizedPlanBlob = {9, 9}; v.compiledPlanBlob = {8, 8, 8}; v.outputDescriptorBlob = {7};
    return v;
}
std::vector<rc::Ops> everyKind() {
    return {typedCreate(), rc::DropTopicOps{.topicId = 3},
            rc::AlterTopicConfigOps{.changes = {{rc::ConfigKey::RetentionMs, 1000}, {rc::ConfigKey::RetentionBytes, -1}}},
            rc::CreateSchemaOps{.schema = {.keyField = "id", .validate = false, .descriptorBlob = {5, 6}, .nextTag = 2}},
            rc::AlterSchemaOps{.ops = {{.type = rune::codec::AlterOpType::Add, .tag = 2, .name = "mana",
                                        .primType = rune::codec::PrimType::Int32},
                                       {.type = rune::codec::AlterOpType::Rename, .tag = 1, .name = "health"},
                                       {.type = rune::codec::AlterOpType::Drop, .tag = 0}}},
            rc::SetCompatibilityOps{.mode = rune::codec::CompatMode::Forward},
            rc::CreateViewOps{.view = viewDef()}, rc::AlterViewOps{.view = viewDef()}, rc::DropViewOps{.viewTopicId = 9}};
}
} // namespace

TEST(CatalogEventRegression, EveryKindRoundTripsByteExactly) {
    for (const rc::Ops& ops : everyKind()) {
        rc::CatalogEvent e = baseEvent(ops);
        e.replayedFrom = rc::ReplayedFrom{.clusterId = "src-cluster", .offset = 12, .principal = "cli@10.0.0.9"};
        const std::vector<uint8_t> bytes = rc::encode(e).value();
        auto d = rc::decode(bytes);
        ASSERT_TRUE(d.has_value()) << rc::kindName(e.kind) << ": " << rc::decodeErrorName(d.error());
        EXPECT_EQ(rc::encode(*d).value(), bytes) << rc::kindName(e.kind);
        EXPECT_EQ(d->kind, e.kind);
        EXPECT_EQ(d->ops.index(), e.ops.index());
        EXPECT_EQ(d->principal.requestId, (rc::RequestId{0x0123456789ABCDEFull, 0xABCDEFull})) << "128-bit request id (F14)";
        EXPECT_EQ(d->principal.requestFingerprint, 0x5151515151515151ull);
        ASSERT_TRUE(d->replayedFrom.has_value());
        EXPECT_EQ(d->replayedFrom->offset, 12);
        if (const auto* a = std::get_if<rc::AlterSchemaOps>(&d->ops)) {   // F20: durable tags, never names
            ASSERT_EQ(a->ops.size(), 3u);
            EXPECT_EQ(a->ops[0].tag, 2); EXPECT_EQ(a->ops[1].tag, 1); EXPECT_EQ(a->ops[1].name, "health");
            EXPECT_EQ(a->ops[2].type, rune::codec::AlterOpType::Drop); EXPECT_TRUE(a->ops[2].name.empty());
        }
    }
}

TEST(CatalogEventRegression, HeaderLayoutIsFrozen) {
    // formatVer u16 LE, kind u16 LE, objectId u64 LE, prevVersion u32, newVersion u32,
    // timestampMs i64, fingerprint u64 — the first 36 bytes, in this order, forever.
    const auto b = rc::encode(baseEvent(rc::DropTopicOps{.topicId = 3})).value();
    ASSERT_GE(b.size(), 36u);
    EXPECT_EQ(b[0], 1); EXPECT_EQ(b[1], 0);                 // formatVer 1
    EXPECT_EQ(b[2], 2); EXPECT_EQ(b[3], 0);                 // DropTopic
    EXPECT_EQ(b[4], 7);                                      // objectId low byte
    EXPECT_EQ(b[12], 0); EXPECT_EQ(b[16], 1);                // prev 0, new 1
    EXPECT_EQ(b[28], 0x88); EXPECT_EQ(b[35], 0x11);          // fingerprint LE
}

TEST(CatalogEventRegression, EveryTruncationIsRefusedNeverMisread) {
    for (const rc::Ops& ops : everyKind()) {
        const auto b = rc::encode(baseEvent(ops)).value();
        for (std::size_t n = 0; n < b.size(); ++n) {
            auto d = rc::decode(std::span<const uint8_t>(b.data(), n));
            ASSERT_FALSE(d.has_value()) << "prefix " << n << " of " << b.size() << " decoded";
            EXPECT_EQ(d.error(), rc::DecodeError::Truncated);
        }
    }
}

TEST(CatalogEventRegression, TrailingBytesAreRefused) {
    auto b = rc::encode(baseEvent(rc::DropViewOps{.viewTopicId = 1})).value();
    b.push_back(0);
    auto d = rc::decode(b);
    ASSERT_FALSE(d.has_value());
    EXPECT_EQ(d.error(), rc::DecodeError::TrailingBytes);
}

TEST(CatalogEventRegression, UnknownFormatVersionIsAnErrorNotASkip) {
    auto b = rc::encode(baseEvent(rc::DropTopicOps{.topicId = 1})).value();
    b[0] = 2;   // formatVer 2 does not exist yet
    auto d = rc::decode(b);
    ASSERT_FALSE(d.has_value());
    EXPECT_EQ(d.error(), rc::DecodeError::UnknownFormatVer);
}

TEST(CatalogEventRegression, ReservedTenantAndAclKindsAreRefusedByV1) {
    for (uint16_t k : {uint16_t{32}, uint16_t{40}, uint16_t{63}}) {
        auto b = rc::encode(baseEvent(rc::DropTopicOps{.topicId = 1})).value();
        b[2] = static_cast<uint8_t>(k & 0xff); b[3] = static_cast<uint8_t>(k >> 8);
        auto d = rc::decode(b);
        ASSERT_FALSE(d.has_value());
        EXPECT_EQ(d.error(), rc::DecodeError::ReservedKind) << k;
    }
    auto b = rc::encode(baseEvent(rc::DropTopicOps{.topicId = 1})).value();
    b[2] = 10; b[3] = 0;
    EXPECT_EQ(rc::decode(b).error(), rc::DecodeError::UnknownKind);
}

TEST(CatalogEventRegression, KindDisagreeingWithOpsIsRefused) {
    auto b = rc::encode(baseEvent(rc::DropTopicOps{.topicId = 1})).value();
    b[2] = 9;   // claims DropView; the ops blob is a DropTopic (same 4-byte shape) — the ops tag catches it
    EXPECT_EQ(rc::decode(b).error(), rc::DecodeError::KindOpsMismatch);
}

TEST(CatalogEventRegression, BadEnumBytesAreRefused) {
    rc::CreateTopicOps o = typedCreate();
    auto b = rc::encode(baseEvent(o)).value();
    // The CreateTopic body is the event's tail; persistence follows topicId(4) partitions(4) rf(2).
    const std::size_t bodyStart = b.size() - rc::detail_testing::createTopicOpsSize(o);
    b[bodyStart + 10] = 7;
    EXPECT_EQ(rc::decode(b).error(), rc::DecodeError::BadEnum);
}

TEST(CatalogEventRegression, EncodeIntoReusesCapacity) {
    std::vector<uint8_t> buf;
    buf.reserve(4096);
    const auto* data = buf.data();
    ASSERT_TRUE(rc::encodeInto(baseEvent(typedCreate()), buf).has_value());
    EXPECT_EQ(buf.data(), data) << "encodeInto must not reallocate a buffer that fits";
}

// ── Fix round 0: encoding never truncates a length or a count ────────────────
namespace {
struct OversizeCase {
    const char*                                   name;
    rc::EncodeError                               expected;
    std::size_t                                   max;
    std::function<rc::CatalogEvent(std::size_t)>  make;   // an event whose field has length / count n
};
rc::CatalogEvent withReplay(rc::Ops ops) {
    rc::CatalogEvent e = baseEvent(std::move(ops));
    e.replayedFrom = rc::ReplayedFrom{.clusterId = "c", .offset = 1, .principal = "p"};
    return e;
}
std::vector<OversizeCase> oversizeCases() {
    using E = rc::EncodeError;
    const std::size_t S = rc::kMaxStr16Bytes, L = rc::kMaxListCount;
    auto str = [](std::size_t n) { return std::string(n, 'x'); };
    return {
        {"objectName", E::FieldTooLong, S, [=](std::size_t n) { auto e = baseEvent(rc::DropTopicOps{1}); e.objectName = str(n); return e; }},
        {"principal.clientName", E::FieldTooLong, S, [=](std::size_t n) { auto e = baseEvent(rc::DropTopicOps{1}); e.principal.clientName = str(n); return e; }},
        {"principal.sourceAddress", E::FieldTooLong, S, [=](std::size_t n) { auto e = baseEvent(rc::DropTopicOps{1}); e.principal.sourceAddress = str(n); return e; }},
        {"principal.authenticated", E::FieldTooLong, S, [=](std::size_t n) { auto e = baseEvent(rc::DropTopicOps{1}); e.principal.authenticated = str(n); return e; }},
        {"replayedFrom.clusterId", E::FieldTooLong, S, [=](std::size_t n) { auto e = withReplay(rc::DropTopicOps{1}); e.replayedFrom->clusterId = str(n); return e; }},
        {"replayedFrom.principal", E::FieldTooLong, S, [=](std::size_t n) { auto e = withReplay(rc::DropTopicOps{1}); e.replayedFrom->principal = str(n); return e; }},
        {"config.coPartitionGroup", E::FieldTooLong, S, [=](std::size_t n) { auto o = typedCreate(); o.config.coPartitionGroup = str(n); return baseEvent(o); }},
        {"config.partitionKey", E::FieldTooLong, S, [=](std::size_t n) { auto o = typedCreate(); o.config.partitionKey = str(n); return baseEvent(o); }},
        {"createTopic.schema.keyField", E::FieldTooLong, S, [=](std::size_t n) { auto o = typedCreate(); o.schema->keyField = str(n); return baseEvent(o); }},
        {"createSchema.keyField", E::FieldTooLong, S, [=](std::size_t n) { rc::CreateSchemaOps o; o.schema.keyField = str(n); return baseEvent(o); }},
        {"alterSchema.op.name", E::FieldTooLong, S, [=](std::size_t n) {
             return baseEvent(rc::AlterSchemaOps{.ops = {{.type = rune::codec::AlterOpType::Rename, .tag = 1, .name = str(n)}}}); }},
        {"view.keyField", E::FieldTooLong, S, [=](std::size_t n) { auto v = viewDef(); v.keyField = str(n); return baseEvent(rc::CreateViewOps{v}); }},
        {"view.sourceName", E::FieldTooLong, S, [=](std::size_t n) { auto v = viewDef(); v.sourceNames = {str(n)}; return baseEvent(rc::AlterViewOps{v}); }},
        {"alterTopicConfig.changes", E::TooManyElements, L, [=](std::size_t n) {
             return baseEvent(rc::AlterTopicConfigOps{.changes = std::vector<rc::ConfigChange>(n, {rc::ConfigKey::RetentionMs, 1})}); }},
        {"alterSchema.ops", E::TooManyElements, L, [=](std::size_t n) {
             return baseEvent(rc::AlterSchemaOps{.ops = std::vector<rc::ResolvedAlterOp>(n, rc::ResolvedAlterOp{.tag = 1})}); }},
        {"view.sourceNames", E::TooManyElements, L, [=](std::size_t n) {
             auto v = viewDef(); v.sourceNames.assign(n, "s"); return baseEvent(rc::CreateViewOps{v}); }},
    };
}
} // namespace

// Every str16 field and every u16-counted list: the wire maximum round-trips; one more is an
// explicit EncodeError and leaves the buffer empty — never a silently truncated prefix.
// (str32 / bytes32 fields go through the same length check at 4 GiB; not materialized here.)
TEST(CatalogEventRegression, OversizeFieldOrListIsAnEncodeErrorNeverAnUndecodableEvent) {
    for (const OversizeCase& c : oversizeCases()) {
        const rc::CatalogEvent atMax = c.make(c.max);
        auto ok = rc::encode(atMax);
        ASSERT_TRUE(ok.has_value()) << c.name << " at its wire maximum must encode";
        auto d = rc::decode(*ok);
        ASSERT_TRUE(d.has_value()) << c.name << ": " << rc::decodeErrorName(d.error());
        EXPECT_EQ(rc::encode(*d).value(), *ok) << c.name;

        std::vector<uint8_t> buf{1, 2, 3};
        auto bad = rc::encodeInto(c.make(c.max + 1), buf);
        ASSERT_FALSE(bad.has_value()) << c.name << " one over its wire width encoded";
        EXPECT_EQ(bad.error(), c.expected) << c.name << ": " << rc::encodeErrorName(bad.error());
        EXPECT_TRUE(buf.empty()) << c.name << ": a refused event leaves no bytes behind";
        EXPECT_FALSE(rc::encode(c.make(c.max + 1)).has_value()) << c.name;
    }
}

// ── Fix round 0: a corrupt list count never sizes an allocation ──────────────
TEST(CatalogEventRegression, HugeListCountInAShortBufferIsTruncatedWithoutAllocating) {
    struct Case { const char* name; rc::CatalogEvent event; std::size_t countFromEnd; };
    rc::ViewDef emptyView;   // empty keyField / sources / blobs: the count is followed by 3 empty bytes32 (12 bytes)
    const Case cases[] = {
        {"alterTopicConfig.changes", baseEvent(rc::AlterTopicConfigOps{}), 2},
        {"alterSchema.ops", baseEvent(rc::AlterSchemaOps{}), 2},
        {"view.sourceNames", baseEvent(rc::CreateViewOps{emptyView}), 14},
    };
    for (const Case& c : cases) {
        auto b = rc::encode(c.event).value();
        const std::size_t at = b.size() - c.countFromEnd;
        ASSERT_EQ(b[at], 0); ASSERT_EQ(b[at + 1], 0);   // sanity: we are on the (zero) count
        b[at] = 0xFF; b[at + 1] = 0xFF;                   // 65535 elements claimed, a handful of bytes present
        gProbePeak = 0; gProbeArmed = true;
        auto d = rc::decode(b);
        gProbeArmed = false;
        ASSERT_FALSE(d.has_value()) << c.name;
        EXPECT_EQ(d.error(), rc::DecodeError::Truncated) << c.name;
        EXPECT_LT(gProbePeak, 4096u) << c.name << ": decode sized an allocation from an untrusted count";
    }
}
