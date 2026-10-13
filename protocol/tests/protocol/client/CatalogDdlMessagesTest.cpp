// tests/protocol/client/CatalogDdlMessagesTest.cpp
//
// Catalog history (client version 8): DDL templates 74–78, the DDL extension
// trailer, DefineTopic replicationFactor + TopicConfigWire, kCatalogTopicId.
// Whole-class rule: every new frame's decode refuses every truncation, trailing
// bytes and out-of-range enum bytes; every encode refuses (returns 0) a field
// that does not fit its wire width instead of truncating it.

#include <rune/protocol/CatalogTypes.h>
#include <rune/protocol/client/Dispatch.h>
#include <rune/protocol/client/Protocol.h>
#include <rune/protocol/client/SchemaMessages.h>
#include <algorithm>
#include <array>
#include <cstdio>
#include <string>
#include <type_traits>
#include <vector>
#include <gtest/gtest.h>

#ifndef RUNE_CONFORMANCE_DIR
#  error "RUNE_CONFORMANCE_DIR must be defined by CMakeLists.txt"
#endif

namespace pc = rune::protocol::client;

TEST(CatalogDdlWireRegression, ConstantsArePinned) {
    EXPECT_EQ(static_cast<uint16_t>(pc::TemplateId::DropTopic), 74);
    EXPECT_EQ(static_cast<uint16_t>(pc::TemplateId::AlterTopicConfig), 75);
    EXPECT_EQ(static_cast<uint16_t>(pc::TemplateId::DropView), 76);
    EXPECT_EQ(static_cast<uint16_t>(pc::TemplateId::AlterView), 77);
    EXPECT_EQ(static_cast<uint16_t>(pc::TemplateId::DdlResponse), 78);
    EXPECT_EQ(pc::kClientVersionCatalog, 8);
    EXPECT_GE(pc::kClientVersion, pc::kClientVersionCatalog);
    EXPECT_EQ(pc::kCatalogTopicId, -6);
    for (uint16_t t = 74; t <= 78; ++t) EXPECT_TRUE(pc::isKnownTemplate(t)) << t << " (F1: else checkFrame drops it)";
    EXPECT_FALSE(pc::isKnownTemplate(79));
    EXPECT_FALSE(pc::clientUnderstandsCatalogDdl(7));
    EXPECT_TRUE(pc::clientUnderstandsCatalogDdl(8));
}

TEST(CatalogDdlWireRegression, TopicConfigWireIsTheOneTopicConfigDefinition) {
    // F32: the wire view, the owned config (catalog / client / rune-ql) and their defaults are one definition.
    static_assert(std::is_same_v<pc::TopicConfigWire, rune::protocol::BasicTopicConfig<std::string_view>>);
    const rune::protocol::TopicConfig owned{};
    EXPECT_EQ(rune::protocol::convertTopicConfig<pc::TopicConfigWire>(owned), pc::TopicConfigWire{});
    rune::protocol::TopicConfig c; c.retentionMs = 5; c.partitionKey = "k";
    EXPECT_EQ(rune::protocol::convertTopicConfig<rune::protocol::TopicConfig>(
                  rune::protocol::convertTopicConfig<pc::TopicConfigWire>(c)), c);
}

TEST(CatalogDdlWireRegression, DefineTopicV8RoundTripsRfConfigAndExtension) {
    pc::DefineTopic m;
    m.sessionId = 1; m.correlationId = 2; m.topicName = "events"; m.partitionCount = 4;
    m.persistence = 0; m.replicationFactor = 3;
    m.config.retentionMs = 60'000; m.config.cleanupPolicy = 1; m.config.partitionKey = "k";
    m.hasExt = true; m.ext.statement = "topic events partitions 4 replicas 3;"; m.ext.clientName = "cli";
    m.ext.requestId = {.hi = 0xC11E, .lo = 77}; m.ext.hasReplayedFrom = 1; m.ext.replayedClusterId = "src"; m.ext.replayedOffset = 5;
    std::array<uint8_t, 1024> buf{};
    const std::size_t n = m.encode(buf.data());
    ASSERT_EQ(n, m.encodedSize());
    auto d = pc::DefineTopic::decode(buf.data(), n);
    ASSERT_TRUE(d.has_value());
    EXPECT_EQ(d->replicationFactor, 3);
    EXPECT_EQ(d->config.retentionMs, 60'000);
    EXPECT_EQ(d->config.cleanupPolicy, 1);
    EXPECT_EQ(d->config.partitionKey, "k");
    EXPECT_TRUE(d->hasExt);
    EXPECT_EQ(d->ext.requestId, (rune::protocol::RequestId{0xC11E, 77})) << "all 128 bits (F14)";
    EXPECT_EQ(d->ext.replayedOffset, 5);
    EXPECT_EQ(d->descriptorLen, 0u) << "an empty descriptor is an untyped topic";
}

TEST(CatalogDdlWireRegression, PreV8DefineTopicStillDecodes) {
    pc::DefineTopic m; m.topicName = "t"; m.partitionCount = 1; m.persistence = 1;   // v7 frame: no ext
    std::array<uint8_t, 256> buf{};
    const std::size_t n = m.encode(buf.data());
    auto d = pc::DefineTopic::decode(buf.data(), n);
    ASSERT_TRUE(d.has_value());
    EXPECT_FALSE(d->hasExt);
    EXPECT_EQ(d->replicationFactor, 0);
    EXPECT_EQ(d->persistence, 1);
}

TEST(CatalogDdlWireRegression, EveryTruncatedTrailerIsRefused) {
    pc::DropTopic m; m.topicName = "t"; m.ext.statement = "drop topic t;"; m.ext.requestId = {.hi = 1, .lo = 9};
    std::array<uint8_t, 256> buf{};
    const std::size_t n = m.encode(buf.data());
    for (std::size_t k = 0; k < n; ++k) EXPECT_FALSE(pc::DropTopic::decode(buf.data(), k).has_value()) << k;
    EXPECT_TRUE(pc::DropTopic::decode(buf.data(), n).has_value());
}

TEST(CatalogDdlWireRegression, AlterTopicConfigRefusesAnUnknownKey) {
    std::array<uint8_t, 9> change{7, 0, 0, 0, 0, 0, 0, 0, 0};   // key 7 does not exist
    std::array<uint8_t, 11> changes{1, 0};
    std::copy(change.begin(), change.end(), changes.begin() + 2);
    pc::AlterTopicConfig m; m.topicName = "t"; m.changes = changes;
    std::array<uint8_t, 256> buf{};
    const std::size_t n = m.encode(buf.data());
    EXPECT_FALSE(pc::AlterTopicConfig::decode(buf.data(), n).has_value());
}

TEST(CatalogDdlWireRegression, DdlResponseRoundTrips) {
    pc::DdlResponse r; r.errorCode = 39; r.objectId = 12; r.newVersion = 3; r.gateTopicId = 5;
    r.detail = "replicationFactor 5 > broker count 3";
    std::array<uint8_t, 256> buf{};
    const std::size_t n = r.encode(buf.data());
    auto d = pc::DdlResponse::decode(buf.data(), n);
    ASSERT_TRUE(d.has_value());
    EXPECT_EQ(d->detail, r.detail);
    EXPECT_EQ(d->gateTopicId, 5);
}

// ---------------------------------------------------------------------------
// Whole-class coverage: every frame, every truncation, trailing bytes, enums,
// oversize fields.
// ---------------------------------------------------------------------------
namespace {

constexpr int64_t kSessionId     = static_cast<int64_t>(0x1122334455667788ULL);
constexpr int64_t kCorrelationId = static_cast<int64_t>(0x0A0B0C0D0E0F1011ULL);
constexpr std::array<uint8_t, 4> kPlan{0xDE, 0xAD, 0xBE, 0xEF};

pc::DdlExtension fullExt(uint64_t lo) {
    pc::DdlExtension e;
    e.statement = "stmt;"; e.clientName = "cli"; e.requestId = {.hi = 0xC11E, .lo = lo};
    e.hasReplayedFrom = 1; e.replayedClusterId = "src"; e.replayedOffset = 5; e.replayedPrincipal = "alice";
    return e;
}

// Golden-vector field values (conformance/gen_vectors.cpp builds the same frames).
pc::DefineTopic vecDefineTopicV8() {
    pc::DefineTopic m;
    m.sessionId = kSessionId; m.correlationId = kCorrelationId; m.topicName = "events"; m.partitionCount = 4;
    m.persistence = 0; m.replicationFactor = 3;
    m.config.retentionMs = 60'000; m.config.cleanupPolicy = 1; m.config.partitionKey = "k";
    m.hasExt = true; m.ext.statement = "topic events partitions 4 replicas 3;"; m.ext.clientName = "cli";
    m.ext.requestId = {.hi = 0xC11E, .lo = 77}; m.ext.hasReplayedFrom = 1; m.ext.replayedClusterId = "src";
    m.ext.replayedOffset = 5;
    return m;
}
pc::DropTopic vecDropTopic() {
    pc::DropTopic m; m.sessionId = kSessionId; m.correlationId = kCorrelationId; m.topicName = "t";
    m.ext.statement = "drop topic t;"; m.ext.clientName = "cli"; m.ext.requestId = {.hi = 1, .lo = 9};
    return m;
}
constexpr std::array<pc::ConfigChangeWire, 1> kVecChanges{{{1, 60'000}}};
pc::AlterTopicConfig vecAlterTopicConfig(std::span<const uint8_t> changes) {
    pc::AlterTopicConfig m; m.sessionId = kSessionId; m.correlationId = kCorrelationId;
    m.topicName = "events"; m.expectedVersion = 2; m.changes = changes;
    m.ext.statement = "ALTER TOPIC events SET (retention.ms=60000)"; m.ext.clientName = "cli";
    m.ext.requestId = {.hi = 1, .lo = 10};
    return m;
}
pc::DropView vecDropView() {
    pc::DropView m; m.sessionId = kSessionId; m.correlationId = kCorrelationId; m.viewName = "v";
    m.ext.statement = "drop view v;"; m.ext.clientName = "cli"; m.ext.requestId = {.hi = 1, .lo = 11};
    return m;
}
pc::AlterView vecAlterView() {
    pc::AlterView m; m.sessionId = kSessionId; m.correlationId = kCorrelationId;
    m.viewName = "v"; m.keyField = "id"; m.persistence = 0; m.expectedVersion = 1;
    m.planBlob = kPlan.data(); m.planLen = static_cast<uint32_t>(kPlan.size());
    m.ext.statement = "alter view v;"; m.ext.clientName = "cli"; m.ext.requestId = {.hi = 1, .lo = 12};
    return m;
}
pc::DdlResponse vecDdlResponse() {
    pc::DdlResponse r; r.sessionId = kSessionId; r.correlationId = kCorrelationId;
    r.errorCode = 39; r.objectId = 12; r.newVersion = 3; r.gateTopicId = 5;
    r.detail = "replicationFactor 5 > broker count 3";
    return r;
}

template <class M>
std::vector<uint8_t> encodeToVec(const M& m) {
    std::vector<uint8_t> v(m.encodedSize() + 16, 0xCC);
    const std::size_t n = m.encode(v.data());
    EXPECT_EQ(n, m.encodedSize());
    v.resize(n);
    return v;
}

// Every strict prefix fails unless it is one of `okCuts` (a pre-v8 frame).
template <class M>
void expectEveryPrefixRefused(const std::vector<uint8_t>& bytes, std::initializer_list<std::size_t> okCuts = {}) {
    for (std::size_t k = 0; k < bytes.size(); ++k) {
        std::vector<uint8_t> exact(bytes.begin(), bytes.begin() + static_cast<std::ptrdiff_t>(k));   // ASAN: exact size
        const bool ok = M::decode(exact.data(), k).has_value();
        const bool allowed = std::find(okCuts.begin(), okCuts.end(), k) != okCuts.end();
        EXPECT_EQ(ok, allowed) << "prefix " << k << " of " << bytes.size();
    }
    std::vector<uint8_t> trailing(bytes); trailing.push_back(0);
    EXPECT_FALSE(M::decode(trailing.data(), trailing.size()).has_value()) << "trailing byte";
    EXPECT_TRUE(M::decode(bytes.data(), bytes.size()).has_value());
}

std::vector<uint8_t> vecChangesBlock() {
    std::vector<uint8_t> b(pc::configChangesEncodedSize(kVecChanges.size()));
    EXPECT_EQ(pc::putConfigChanges(b.data(), kVecChanges), b.size());
    return b;
}

const std::string kOversize16(0x10000, 'x');   // one byte over a u16 length

} // namespace

TEST(CatalogDdlWireRegression, EveryNewFrameRefusesEveryTruncationAndTrailingBytes) {
    expectEveryPrefixRefused<pc::DropTopic>(encodeToVec(vecDropTopic()));
    expectEveryPrefixRefused<pc::DropView>(encodeToVec(vecDropView()));
    expectEveryPrefixRefused<pc::AlterView>(encodeToVec(vecAlterView()));
    expectEveryPrefixRefused<pc::DdlResponse>(encodeToVec(vecDdlResponse()));
    const auto changes = vecChangesBlock();
    expectEveryPrefixRefused<pc::AlterTopicConfig>(encodeToVec(vecAlterTopicConfig(changes)));
}

TEST(CatalogDdlWireRegression, V8TrailersRefuseEveryTruncationPastThePreV8Body) {
    // DefineTopic: the only accepted strict prefixes end the pre-v8 body (with or
    // without the persistence byte) — they decode as pre-v8, hasExt false.
    pc::DefineTopic dt = vecDefineTopicV8();
    const auto dtBytes = encodeToVec(dt);
    const std::size_t dtBase = pc::MessageHeader::ENCODED_SIZE + pc::DefineTopic::BODY_FIXED
                             + 2 + dt.topicName.size() + 2 + dt.keyField.size() + 4 + dt.descriptorLen;
    expectEveryPrefixRefused<pc::DefineTopic>(dtBytes, {dtBase, dtBase + 1});
    EXPECT_FALSE(pc::DefineTopic::decode(dtBytes.data(), dtBase + 1)->hasExt);

    pc::AlterTopic at; at.topicName = "events"; at.expectedVersion = 2;
    at.opsBlob = kPlan.data(); at.opsLen = 4; at.hasExt = true; at.ext = fullExt(20);
    const auto atBytes = encodeToVec(at);
    const std::size_t atBase = pc::MessageHeader::ENCODED_SIZE + pc::AlterTopic::BODY_FIXED + 2 + 6 + 4 + 4;
    expectEveryPrefixRefused<pc::AlterTopic>(atBytes, {atBase});
    auto atD = pc::AlterTopic::decode(atBytes.data(), atBytes.size());
    ASSERT_TRUE(atD.has_value());
    EXPECT_TRUE(atD->hasExt);
    EXPECT_EQ(atD->ext, at.ext);
    EXPECT_EQ(atD->opsLen, 4u);

    pc::CreateView cv; cv.viewName = "v"; cv.keyField = "id"; cv.persistence = 1;
    cv.planBlob = kPlan.data(); cv.planLen = 4; cv.hasExt = true; cv.ext = fullExt(21);
    const auto cvBytes = encodeToVec(cv);
    const std::size_t cvBase = pc::MessageHeader::ENCODED_SIZE + pc::CreateView::BODY_FIXED + 2 + 1 + 2 + 2 + 4 + 4;
    expectEveryPrefixRefused<pc::CreateView>(cvBytes, {cvBase});
    auto cvD = pc::CreateView::decode(cvBytes.data(), cvBytes.size());
    ASSERT_TRUE(cvD.has_value());
    EXPECT_TRUE(cvD->hasExt);
    EXPECT_EQ(cvD->ext, cv.ext);
    EXPECT_EQ(cvD->persistence, 1);
}

TEST(CatalogDdlWireRegression, PreV8AlterTopicAndCreateViewAreByteIdenticalAndDecode) {
    pc::AlterTopic at; at.topicName = "t"; at.expectedVersion = 1;
    const auto atBytes = encodeToVec(at);
    EXPECT_EQ(atBytes.size(), pc::MessageHeader::ENCODED_SIZE + 2u + 3u + 4u) << "no trailer without hasExt";
    auto atD = pc::AlterTopic::decode(atBytes.data(), atBytes.size());
    ASSERT_TRUE(atD.has_value());
    EXPECT_FALSE(atD->hasExt);

    pc::CreateView cv; cv.viewName = "v";
    const auto cvBytes = encodeToVec(cv);
    EXPECT_EQ(cvBytes.size(), pc::MessageHeader::ENCODED_SIZE + 1u + 3u + 2u + 4u);
    auto cvD = pc::CreateView::decode(cvBytes.data(), cvBytes.size());
    ASSERT_TRUE(cvD.has_value());
    EXPECT_FALSE(cvD->hasExt);
}

TEST(CatalogDdlWireRegression, DefineTopicV8WritesThePersistenceByteAndFullConfigRoundTrips) {
    pc::DefineTopic m; m.topicName = "t"; m.partitionCount = 2; m.persistence = 1; m.replicationFactor = -1;
    m.hasExt = true; m.ext = fullExt(30);
    m.config.cleanupPolicy = 2; m.config.retentionMs = 1; m.config.retentionBytes = 2; m.config.deleteRetentionMs = 3;
    m.config.minCleanableDirtyRatioPpm = 4; m.config.segmentRollBytes = 5; m.config.minCompactionLagMs = 6;
    m.config.maxCompactionLagMs = 7; m.config.retentionCheckMs = 8; m.config.fileDeleteDelayMs = 9;
    m.config.durability = 1; m.config.durabilityTier = 2; m.config.coPartitionGroup = "grp"; m.config.partitionKey = "pk";
    const auto bytes = encodeToVec(m);
    auto d = pc::DefineTopic::decode(bytes.data(), bytes.size());
    ASSERT_TRUE(d.has_value());
    EXPECT_EQ(d->config, m.config);
    EXPECT_EQ(d->ext, m.ext);
    EXPECT_EQ(d->replicationFactor, -1) << "i16 on the wire (F31); bounds are the leader's check";
    EXPECT_EQ(d->persistence, 1);

    pc::DefineTopic disk = m; disk.persistence = 0;
    const auto diskBytes = encodeToVec(disk);
    EXPECT_EQ(diskBytes.size(), bytes.size()) << "the persistence byte is always written with the trailer";
    auto dd = pc::DefineTopic::decode(diskBytes.data(), diskBytes.size());
    ASSERT_TRUE(dd.has_value());
    EXPECT_TRUE(dd->hasExt);
    EXPECT_EQ(dd->persistence, 0);
}

TEST(CatalogDdlWireRegression, ExtensionWithoutReplayedFromLeavesItsDefaults) {
    pc::DropView m = vecDropView();
    const auto bytes = encodeToVec(m);
    EXPECT_EQ(bytes.size(), pc::MessageHeader::ENCODED_SIZE + 2u + 1u + 4u + 12u + 2u + 3u + 16u + 1u);
    auto d = pc::DropView::decode(bytes.data(), bytes.size());
    ASSERT_TRUE(d.has_value());
    EXPECT_EQ(d->ext.hasReplayedFrom, 0);
    EXPECT_EQ(d->ext.replayedOffset, -1);
    EXPECT_TRUE(d->ext.replayedClusterId.empty());
    EXPECT_EQ(d->ext, m.ext);
}

TEST(CatalogDdlWireRegression, OutOfRangeEnumBytesAreRefused) {
    // hasReplayedFrom: the byte right after requestId (DropTopic "t", no replayedFrom => last byte).
    auto drop = encodeToVec(vecDropTopic());
    drop.back() = 2;
    EXPECT_FALSE(pc::DropTopic::decode(drop.data(), drop.size()).has_value());

    // AlterView persistence 2.
    auto av = encodeToVec(vecAlterView());
    const std::size_t persOff = pc::MessageHeader::ENCODED_SIZE + 2 + 1 + 2 + 2;
    ASSERT_EQ(av[persOff], 0);
    av[persOff] = 2;
    EXPECT_FALSE(pc::AlterView::decode(av.data(), av.size()).has_value());

    // DefineTopic config enum bytes: cleanupPolicy 3, durability 2, durabilityTier 3.
    const auto dt = encodeToVec(vecDefineTopicV8());
    const std::size_t cfgOff = pc::MessageHeader::ENCODED_SIZE + 5 + 2 + 6 + 2 + 4 + 1 + 2;
    ASSERT_EQ(dt[cfgOff], 1) << "cleanupPolicy";
    for (auto [off, bad] : {std::pair<std::size_t, uint8_t>{cfgOff, 3}, {cfgOff + 69, 2}, {cfgOff + 70, 3}}) {
        auto bytes = dt; bytes[off] = bad;
        EXPECT_FALSE(pc::DefineTopic::decode(bytes.data(), bytes.size()).has_value()) << off;
    }

    // ... and encode refuses them too.
    pc::DefineTopic m = vecDefineTopicV8(); m.config.durabilityTier = 3;
    std::array<uint8_t, 1024> buf{};
    EXPECT_EQ(m.encode(buf.data()), 0u);
    pc::DropTopic d = vecDropTopic(); d.ext.hasReplayedFrom = 2;
    EXPECT_EQ(d.encode(buf.data()), 0u);
    pc::AlterView v = vecAlterView(); v.persistence = 2;
    EXPECT_EQ(v.encode(buf.data()), 0u);
}

TEST(CatalogDdlWireRegression, OversizeFieldsFailEncodeInsteadOfTruncating) {
    std::vector<uint8_t> buf(0x30000);
    { pc::DropTopic m = vecDropTopic(); m.topicName = kOversize16; EXPECT_FALSE(m.fits()); EXPECT_EQ(m.encode(buf.data()), 0u); }
    { pc::DropTopic m = vecDropTopic(); m.ext.clientName = kOversize16; EXPECT_EQ(m.encode(buf.data()), 0u); }
    { pc::DropTopic m = vecDropTopic(); m.ext.hasReplayedFrom = 1; m.ext.replayedClusterId = kOversize16;
      EXPECT_EQ(m.encode(buf.data()), 0u); }
    { pc::DropTopic m = vecDropTopic(); m.ext.hasReplayedFrom = 1; m.ext.replayedPrincipal = kOversize16;
      EXPECT_EQ(m.encode(buf.data()), 0u); }
    { pc::DropView m = vecDropView(); m.viewName = kOversize16; EXPECT_EQ(m.encode(buf.data()), 0u); }
    { pc::AlterView m = vecAlterView(); m.keyField = kOversize16; EXPECT_EQ(m.encode(buf.data()), 0u); }
    { pc::DdlResponse m = vecDdlResponse(); m.detail = kOversize16; EXPECT_EQ(m.encode(buf.data()), 0u); }
    { const auto ch = vecChangesBlock();
      pc::AlterTopicConfig m = vecAlterTopicConfig(ch); m.topicName = kOversize16; EXPECT_EQ(m.encode(buf.data()), 0u); }
    { pc::DefineTopic m = vecDefineTopicV8(); m.config.coPartitionGroup = kOversize16; EXPECT_EQ(m.encode(buf.data()), 0u); }
    { pc::DefineTopic m = vecDefineTopicV8(); m.config.partitionKey = kOversize16; EXPECT_EQ(m.encode(buf.data()), 0u); }
    // A statement longer than 64 KiB is legal (u32 length) and round-trips intact.
    const std::string longStmt(0x10001, 's');
    pc::DropTopic m = vecDropTopic(); m.ext.statement = longStmt;
    const std::size_t n = m.encode(buf.data());
    ASSERT_EQ(n, m.encodedSize());
    auto d = pc::DropTopic::decode(buf.data(), n);
    ASSERT_TRUE(d.has_value());
    EXPECT_EQ(d->ext.statement.size(), longStmt.size());
}

TEST(CatalogDdlWireRegression, DefineTopicV8FieldsWithoutTheExtensionFailInsteadOfBeingDropped) {
    std::array<uint8_t, 512> buf{};
    pc::DefineTopic rf; rf.topicName = "t"; rf.replicationFactor = 3;   // hasExt false
    EXPECT_EQ(rf.encode(buf.data()), 0u);
    pc::DefineTopic cfg; cfg.topicName = "t"; cfg.config.retentionMs = 1;
    EXPECT_EQ(cfg.encode(buf.data()), 0u);
}

TEST(CatalogDdlWireRegression, AlterTopicConfigChangesRoundTripAndAMalformedBlockFailsEncode) {
    const std::array<pc::ConfigChangeWire, 6> all{{{1, 60'000}, {2, int64_t{1} << 30}, {3, 0},
                                                   {4, 250'000}, {5, 7}, {6, -1}}};
    std::vector<uint8_t> block(pc::configChangesEncodedSize(all.size()));
    ASSERT_EQ(pc::putConfigChanges(block.data(), all), block.size());
    pc::AlterTopicConfig m = vecAlterTopicConfig(block);
    m.ext = fullExt(40);
    const auto bytes = encodeToVec(m);
    auto d = pc::AlterTopicConfig::decode(bytes.data(), bytes.size());
    ASSERT_TRUE(d.has_value());
    EXPECT_EQ(d->expectedVersion, 2u);
    ASSERT_EQ(pc::configChangeCount(d->changes), all.size());
    for (std::size_t i = 0; i < all.size(); ++i) EXPECT_EQ(pc::configChangeAt(d->changes, i), all[i]) << i;
    EXPECT_EQ(d->ext, m.ext);

    std::array<uint8_t, 512> buf{};
    std::vector<uint8_t> shortBlock(block.begin(), block.end() - 1);   // count says 6, 5.9 changes present
    EXPECT_EQ(vecAlterTopicConfig(shortBlock).encode(buf.data()), 0u);
    EXPECT_EQ(vecAlterTopicConfig({}).encode(buf.data()), 0u) << "no count";
    // An empty change list is well-formed on the wire (the leader refuses it with detail).
    const std::array<uint8_t, 2> none{0, 0};
    const auto emptyBytes = encodeToVec(vecAlterTopicConfig(none));
    ASSERT_TRUE(pc::AlterTopicConfig::decode(emptyBytes.data(), emptyBytes.size()).has_value());
    // Key 0 is refused like key 7.
    auto zeroKey = encodeToVec(vecAlterTopicConfig(vecChangesBlock()));
    const std::size_t keyOff = pc::MessageHeader::ENCODED_SIZE + 2 + 6 + 4 + 2;
    ASSERT_EQ(zeroKey[keyOff], 1);
    zeroKey[keyOff] = 0;
    EXPECT_FALSE(pc::AlterTopicConfig::decode(zeroKey.data(), zeroKey.size()).has_value());
}

TEST(CatalogDdlWireRegression, AlterViewRoundTrips) {
    const pc::AlterView m = vecAlterView();
    const auto bytes = encodeToVec(m);
    auto d = pc::AlterView::decode(bytes.data(), bytes.size());
    ASSERT_TRUE(d.has_value());
    EXPECT_EQ(d->viewName, "v");
    EXPECT_EQ(d->keyField, "id");
    EXPECT_EQ(d->expectedVersion, 1u);
    ASSERT_EQ(d->planLen, kPlan.size());
    EXPECT_TRUE(std::equal(kPlan.begin(), kPlan.end(), d->planBlob));
    EXPECT_EQ(d->ext, m.ext);
}

TEST(CatalogDdlWireRegression, CheckFrameAcceptsEveryNewFrameAndDecodersRefuseOtherTemplates) {
    const auto changes = vecChangesBlock();
    const std::array<std::pair<std::vector<uint8_t>, uint16_t>, 6> frames{{
        {encodeToVec(vecDefineTopicV8()), 64}, {encodeToVec(vecDropTopic()), 74},
        {encodeToVec(vecAlterTopicConfig(changes)), 75}, {encodeToVec(vecDropView()), 76},
        {encodeToVec(vecAlterView()), 77}, {encodeToVec(vecDdlResponse()), 78}}};
    for (const auto& [bytes, tid] : frames) {
        const auto fc = pc::checkFrame(bytes.data(), bytes.size());
        EXPECT_EQ(fc.status, pc::FrameStatus::Ok) << tid;
        EXPECT_EQ(fc.templateId, tid);
    }
    // A DropView frame is not a DropTopic (same body layout, different template).
    const auto& dv = frames[3].first;
    EXPECT_FALSE(pc::DropTopic::decode(dv.data(), dv.size()).has_value());
    EXPECT_FALSE(pc::DdlResponse::decode(dv.data(), dv.size()).has_value());
}

// Fix-on-sight (whole-class rule): the pre-existing DDL encoders truncated an
// oversize name/key field to its low 16 bits (a corrupt frame); they now fail.
TEST(SchemaMessagesOversizeRegression, PreExistingDdlEncodersFailInsteadOfTruncatingRegression) {
    std::vector<uint8_t> buf(0x30000);
    { pc::DefineTopic m; m.topicName = kOversize16; EXPECT_EQ(m.encode(buf.data()), 0u); }
    { pc::DefineTopic m; m.topicName = "t"; m.keyField = kOversize16; EXPECT_EQ(m.encode(buf.data()), 0u); }
    { pc::DefineTopic m; m.topicName = "t"; m.persistence = 2; EXPECT_EQ(m.encode(buf.data()), 0u); }
    { pc::AlterTopic m; m.topicName = kOversize16; EXPECT_EQ(m.encode(buf.data()), 0u); }
    { pc::CreateView m; m.viewName = kOversize16; EXPECT_EQ(m.encode(buf.data()), 0u); }
    { pc::CreateView m; m.viewName = "v"; m.keyField = kOversize16; EXPECT_EQ(m.encode(buf.data()), 0u); }
    { pc::GetTopicSchema m; m.topicName = kOversize16; EXPECT_EQ(m.encode(buf.data()), 0u); }
    { pc::AlterTopic m; m.topicName = "t"; m.hasExt = true; m.ext.clientName = kOversize16;
      EXPECT_EQ(m.encode(buf.data()), 0u); }
    // The largest legal name still encodes.
    const std::string maxName(0xFFFF, 'n');
    pc::GetTopicSchema ok; ok.topicName = maxName;
    EXPECT_EQ(ok.encode(buf.data()), ok.encodedSize());
}

// ---------------------------------------------------------------------------
// Conformance goldens: conformance/vectors/{define_topic_v8,drop_topic,
// alter_topic_config,drop_view,alter_view,ddl_response}.bin — decode-golden and
// encode-golden (byte-identical).
// ---------------------------------------------------------------------------
namespace {
std::vector<uint8_t> readVector(const char* name) {
    const std::string path = std::string(RUNE_CONFORMANCE_DIR) + "/vectors/" + name;
    FILE* f = std::fopen(path.c_str(), "rb");
    EXPECT_NE(f, nullptr) << "Cannot open golden vector: " << path;
    if (!f) return {};
    std::vector<uint8_t> out;
    std::array<uint8_t, 4096> chunk{};
    std::size_t got = 0;
    while ((got = std::fread(chunk.data(), 1, chunk.size(), f)) > 0) out.insert(out.end(), chunk.begin(), chunk.begin() + got);
    std::fclose(f);
    return out;
}
} // namespace

TEST(CatalogDdlConformance, DefineTopicV8Golden) {
    const auto golden = readVector("define_topic_v8.bin");
    EXPECT_EQ(encodeToVec(vecDefineTopicV8()), golden);
    auto d = pc::DefineTopic::decode(golden.data(), golden.size());
    ASSERT_TRUE(d.has_value());
    const auto want = vecDefineTopicV8();
    EXPECT_EQ(d->sessionId, kSessionId);
    EXPECT_EQ(d->correlationId, kCorrelationId);
    EXPECT_EQ(d->topicName, want.topicName);
    EXPECT_EQ(d->partitionCount, want.partitionCount);
    EXPECT_EQ(d->replicationFactor, want.replicationFactor);
    EXPECT_EQ(d->config, want.config);
    EXPECT_TRUE(d->hasExt);
    EXPECT_EQ(d->ext, want.ext);
}

TEST(CatalogDdlConformance, DropTopicGolden) {
    const auto golden = readVector("drop_topic.bin");
    EXPECT_EQ(encodeToVec(vecDropTopic()), golden);
    auto d = pc::DropTopic::decode(golden.data(), golden.size());
    ASSERT_TRUE(d.has_value());
    EXPECT_EQ(d->correlationId, kCorrelationId);
    EXPECT_EQ(d->topicName, "t");
    EXPECT_EQ(d->ext, vecDropTopic().ext);
}

TEST(CatalogDdlConformance, AlterTopicConfigGolden) {
    const auto golden = readVector("alter_topic_config.bin");
    const auto changes = vecChangesBlock();
    EXPECT_EQ(encodeToVec(vecAlterTopicConfig(changes)), golden);
    auto d = pc::AlterTopicConfig::decode(golden.data(), golden.size());
    ASSERT_TRUE(d.has_value());
    EXPECT_EQ(d->topicName, "events");
    EXPECT_EQ(d->expectedVersion, 2u);
    ASSERT_EQ(pc::configChangeCount(d->changes), 1u);
    EXPECT_EQ(pc::configChangeAt(d->changes, 0), kVecChanges[0]);
    EXPECT_EQ(d->ext, vecAlterTopicConfig(changes).ext);
}

TEST(CatalogDdlConformance, DropViewGolden) {
    const auto golden = readVector("drop_view.bin");
    EXPECT_EQ(encodeToVec(vecDropView()), golden);
    auto d = pc::DropView::decode(golden.data(), golden.size());
    ASSERT_TRUE(d.has_value());
    EXPECT_EQ(d->viewName, "v");
    EXPECT_EQ(d->ext, vecDropView().ext);
}

TEST(CatalogDdlConformance, AlterViewGolden) {
    const auto golden = readVector("alter_view.bin");
    EXPECT_EQ(encodeToVec(vecAlterView()), golden);
    auto d = pc::AlterView::decode(golden.data(), golden.size());
    ASSERT_TRUE(d.has_value());
    EXPECT_EQ(d->viewName, "v");
    EXPECT_EQ(d->keyField, "id");
    EXPECT_EQ(d->expectedVersion, 1u);
    EXPECT_EQ(d->ext, vecAlterView().ext);
}

TEST(CatalogDdlConformance, DdlResponseGolden) {
    const auto golden = readVector("ddl_response.bin");
    EXPECT_EQ(encodeToVec(vecDdlResponse()), golden);
    auto d = pc::DdlResponse::decode(golden.data(), golden.size());
    ASSERT_TRUE(d.has_value());
    EXPECT_EQ(d->errorCode, 39u);
    EXPECT_EQ(d->objectId, 12u);
    EXPECT_EQ(d->newVersion, 3u);
    EXPECT_EQ(d->gateTopicId, 5);
    EXPECT_EQ(d->detail, "replicationFactor 5 > broker count 3");
}
