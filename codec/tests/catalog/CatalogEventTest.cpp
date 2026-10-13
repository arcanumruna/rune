// Catalog history (spec §4): the frozen formatVer-1 event encoding.
#include <rune/catalog/CatalogEvent.h>
#include <gtest/gtest.h>

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
        const std::vector<uint8_t> bytes = rc::encode(e);
        auto d = rc::decode(bytes);
        ASSERT_TRUE(d.has_value()) << rc::kindName(e.kind) << ": " << rc::decodeErrorName(d.error());
        EXPECT_EQ(rc::encode(*d), bytes) << rc::kindName(e.kind);
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
    const auto b = rc::encode(baseEvent(rc::DropTopicOps{.topicId = 3}));
    ASSERT_GE(b.size(), 36u);
    EXPECT_EQ(b[0], 1); EXPECT_EQ(b[1], 0);                 // formatVer 1
    EXPECT_EQ(b[2], 2); EXPECT_EQ(b[3], 0);                 // DropTopic
    EXPECT_EQ(b[4], 7);                                      // objectId low byte
    EXPECT_EQ(b[12], 0); EXPECT_EQ(b[16], 1);                // prev 0, new 1
    EXPECT_EQ(b[28], 0x88); EXPECT_EQ(b[35], 0x11);          // fingerprint LE
}

TEST(CatalogEventRegression, EveryTruncationIsRefusedNeverMisread) {
    for (const rc::Ops& ops : everyKind()) {
        const auto b = rc::encode(baseEvent(ops));
        for (std::size_t n = 0; n < b.size(); ++n) {
            auto d = rc::decode(std::span<const uint8_t>(b.data(), n));
            ASSERT_FALSE(d.has_value()) << "prefix " << n << " of " << b.size() << " decoded";
            EXPECT_EQ(d.error(), rc::DecodeError::Truncated);
        }
    }
}

TEST(CatalogEventRegression, TrailingBytesAreRefused) {
    auto b = rc::encode(baseEvent(rc::DropViewOps{.viewTopicId = 1}));
    b.push_back(0);
    auto d = rc::decode(b);
    ASSERT_FALSE(d.has_value());
    EXPECT_EQ(d.error(), rc::DecodeError::TrailingBytes);
}

TEST(CatalogEventRegression, UnknownFormatVersionIsAnErrorNotASkip) {
    auto b = rc::encode(baseEvent(rc::DropTopicOps{.topicId = 1}));
    b[0] = 2;   // formatVer 2 does not exist yet
    auto d = rc::decode(b);
    ASSERT_FALSE(d.has_value());
    EXPECT_EQ(d.error(), rc::DecodeError::UnknownFormatVer);
}

TEST(CatalogEventRegression, ReservedTenantAndAclKindsAreRefusedByV1) {
    for (uint16_t k : {uint16_t{32}, uint16_t{40}, uint16_t{63}}) {
        auto b = rc::encode(baseEvent(rc::DropTopicOps{.topicId = 1}));
        b[2] = static_cast<uint8_t>(k & 0xff); b[3] = static_cast<uint8_t>(k >> 8);
        auto d = rc::decode(b);
        ASSERT_FALSE(d.has_value());
        EXPECT_EQ(d.error(), rc::DecodeError::ReservedKind) << k;
    }
    auto b = rc::encode(baseEvent(rc::DropTopicOps{.topicId = 1}));
    b[2] = 10; b[3] = 0;
    EXPECT_EQ(rc::decode(b).error(), rc::DecodeError::UnknownKind);
}

TEST(CatalogEventRegression, KindDisagreeingWithOpsIsRefused) {
    auto b = rc::encode(baseEvent(rc::DropTopicOps{.topicId = 1}));
    b[2] = 9;   // claims DropView; the ops blob is a DropTopic (same 4-byte shape) — the ops tag catches it
    EXPECT_EQ(rc::decode(b).error(), rc::DecodeError::KindOpsMismatch);
}

TEST(CatalogEventRegression, BadEnumBytesAreRefused) {
    rc::CreateTopicOps o = typedCreate();
    auto b = rc::encode(baseEvent(o));
    // The CreateTopic body is the event's tail; persistence follows topicId(4) partitions(4) rf(2).
    const std::size_t bodyStart = b.size() - rc::detail_testing::createTopicOpsSize(o);
    b[bodyStart + 10] = 7;
    EXPECT_EQ(rc::decode(b).error(), rc::DecodeError::BadEnum);
}

TEST(CatalogEventRegression, EncodeIntoReusesCapacity) {
    std::vector<uint8_t> buf;
    buf.reserve(4096);
    const auto* data = buf.data();
    rc::encodeInto(baseEvent(typedCreate()), buf);
    EXPECT_EQ(buf.data(), data) << "encodeInto must not reallocate a buffer that fits";
}
