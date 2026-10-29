#include <rune/protocol/NamespaceLimits.h>
#include <rune/protocol/SecurityTypes.h>
#include <rune/protocol/client/Dispatch.h>
#include <rune/protocol/client/NamespaceMessages.h>
#include <rune/protocol/client/NamespaceUsageMessages.h>
#include <rune/protocol/client/ControlMessages.h>
#include <gtest/gtest.h>
#include <cstring>
#include <string>
#include <vector>

namespace pc = rune::protocol::client;
namespace rp = rune::protocol;

namespace {

// Field i (declaration order) = i * 0x0101010101010101 so any reordering of the wire shows in a byte compare.
rp::NamespaceLimits distinctLimits() {
    rp::NamespaceLimits l;
    constexpr uint64_t k = 0x0101010101010101ULL;
    l.maxTopics = 0 * k;            l.maxPartitionsPerTopic = 1 * k; l.maxPartitionsTotal = 2 * k;
    l.maxViews = 3 * k;             l.maxSchemas = 4 * k;            l.maxVersionsPerSchema = 5 * k;
    l.maxGroups = 6 * k;            l.maxUsers = 7 * k;
    l.maxDiskBytes = 8 * k;         l.maxRetentionMs = 9 * k;        l.maxRetentionBytes = 10 * k;
    l.maxMessageBytes = 11 * k;     l.maxBatchBytes = 12 * k;
    l.produceBytesPerSec = 13 * k;  l.produceRecordsPerSec = 14 * k; l.fetchBytesPerSec = 15 * k;
    l.requestOpsPerSec = 16 * k;    l.queryCyclesPerSec = 17 * k;
    l.maxConnections = 18 * k;      l.maxConnectionsPerUser = 19 * k; l.maxSessionsPerUser = 20 * k;
    l.maxOpenTxns = 21 * k;         l.maxTxnTimeMs = 22 * k;
    l.maxViewMemoryBytes = 23 * k;
    l.minReplicationFactor = 24 * k; l.maxReplicationFactor = 25 * k; l.lowestAcks = 26 * k; l.lowestMinInsync = 27 * k;
    l.featureBits = 0xA1B2C3D4u;    l.reserved0 = 0x0A0B0C0Du;
    l.reserved1 = 0x1112131415161718ULL; l.reserved2 = 0x2122232425262728ULL; l.reserved3 = 0x3132333435363738ULL;
    return l;
}

pc::DdlExtension ext() {
    pc::DdlExtension e;
    e.statement = "CREATE NAMESPACE acme";
    e.clientName = "cli";
    e.requestId = {0xC11E, 77};
    return e;
}

std::vector<uint8_t> gWire;   // decoded DdlExtension string_views point into the wire buffer: keep it alive

std::vector<uint8_t> header(uint16_t tid, int64_t sess, int64_t corr, uint16_t blockLength) {
    std::vector<uint8_t> b(pc::MessageHeader::ENCODED_SIZE);
    pc::MessageHeader h;
    h.blockLength = blockLength; h.templateId = tid; h.schemaId = pc::kSchemaId; h.version = pc::kProtocolVersion;
    h.sessionId = sess; h.correlationId = corr;
    h.encode(b.data());
    return b;
}

} // namespace

TEST(NamespaceMessagesRegression, LimitsRoundTripRegression) {
    const auto l = distinctLimits();
    std::vector<uint8_t> buf(rp::kNamespaceLimitsBytes);
    rp::encodeLimits(l, buf.data());
    // Golden prefix: field 0 = 0x00.. then field 1 = 0x01.. (little-endian, byte-wise, declaration order).
    static constexpr uint8_t kGoldenPrefix[16] = {0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1, 1, 1};
    EXPECT_EQ(0, std::memcmp(buf.data(), kGoldenPrefix, sizeof kGoldenPrefix));
    // featureBits @224 and reserved0 @228 are u32 LE; reserved1 @232 is u64 LE.
    EXPECT_EQ((std::vector<uint8_t>{0xD4, 0xC3, 0xB2, 0xA1, 0x0D, 0x0C, 0x0B, 0x0A}),
              (std::vector<uint8_t>(buf.begin() + 224, buf.begin() + 232)));
    EXPECT_EQ(buf[232], 0x18); EXPECT_EQ(buf[239], 0x11);
    rp::NamespaceLimits d;
    ASSERT_TRUE(rp::decodeLimits(buf.data(), buf.size(), d));
    EXPECT_EQ(d, l);
    EXPECT_FALSE(rp::decodeLimits(buf.data(), buf.size() - 1, d));
    EXPECT_FALSE(rp::decodeLimits(buf.data(), 0, d));
}

TEST(NamespaceMessagesRegression, LimitsSizePinnedRegression) {
    EXPECT_EQ(rp::kNamespaceLimitsBytes, 256u);
    EXPECT_EQ(sizeof(rp::NamespaceLimits), 256u);
    EXPECT_EQ(rp::kNamespaceLimitFieldCount, 29u);
}

TEST(NamespaceMessagesRegression, DdlRoundTripRegression) {
    struct Case { pc::NamespaceDdlKind kind; std::array<uint8_t, 4> mask; };
    const Case cases[] = {
        {pc::NamespaceDdlKind::Create,    {0x01, 0x00, 0x00, 0x00}},   // maxTopics only
        {pc::NamespaceDdlKind::SetLimits, {0x00, 0x80, 0x00, 0x10}},   // field 15 and field 28 (featureBits)
        {pc::NamespaceDdlKind::SetLimits, {0xFF, 0xFF, 0xFF, 0x1F}},   // every field
        {pc::NamespaceDdlKind::Create,    {0x00, 0x00, 0x00, 0x00}},   // none
    };
    for (const auto& c : cases) {
        pc::NamespaceDdlRequest m;
        m.sessionId = 7; m.correlationId = 9; m.ext = ext(); m.kind = c.kind; m.name = "acme-prod";
        m.presentMask = c.mask; m.limits = distinctLimits();
        gWire.assign(m.encodedSize() + 8, 0);
        const std::size_t n = m.encode(gWire.data());
        ASSERT_EQ(n, m.encodedSize());
        gWire.resize(n);
        auto d = pc::NamespaceDdlRequest::decode(gWire.data(), gWire.size());
        ASSERT_TRUE(d);
        EXPECT_EQ(d->sessionId, 7); EXPECT_EQ(d->correlationId, 9);
        EXPECT_EQ(d->ext, m.ext); EXPECT_EQ(d->ext.requestId.hi, 0xC11Eu); EXPECT_EQ(d->ext.requestId.lo, 77u);
        EXPECT_EQ(d->kind, c.kind); EXPECT_EQ(d->name, "acme-prod");
        EXPECT_EQ(d->presentMask, c.mask); EXPECT_EQ(d->limits, m.limits);
    }
    EXPECT_TRUE(pc::namespaceMaskHas({0x00, 0x80, 0x00, 0x10}, 15));
    EXPECT_TRUE(pc::namespaceMaskHas({0x00, 0x80, 0x00, 0x10}, 28));
    EXPECT_FALSE(pc::namespaceMaskHas({0x00, 0x80, 0x00, 0x10}, 14));
    EXPECT_FALSE(pc::namespaceMaskHas({0xFF, 0xFF, 0xFF, 0xFF}, 29));

    // Encoder refusals: out-of-range kind, reserved mask bits.
    pc::NamespaceDdlRequest bad;
    bad.ext = ext(); bad.name = "a";
    std::vector<uint8_t> out(bad.encodedSize() + 8);
    bad.kind = static_cast<pc::NamespaceDdlKind>(6);
    EXPECT_EQ(bad.encode(out.data()), 0u);
    bad.kind = static_cast<pc::NamespaceDdlKind>(0);
    EXPECT_EQ(bad.encode(out.data()), 0u);
    bad.kind = pc::NamespaceDdlKind::Create; bad.presentMask = {0, 0, 0, 0x20};
    EXPECT_EQ(bad.encode(out.data()), 0u);
}

TEST(NamespaceMessagesRegression, DdlTruncatedAtEveryOffsetRegression) {
    pc::NamespaceDdlRequest m;
    m.sessionId = 1; m.correlationId = 2; m.ext = ext(); m.kind = pc::NamespaceDdlKind::SetLimits; m.name = "acme";
    m.presentMask = {0x03, 0, 0, 0}; m.limits = distinctLimits();
    std::vector<uint8_t> full(m.encodedSize());
    ASSERT_EQ(m.encode(full.data()), full.size());
    ASSERT_TRUE(pc::NamespaceDdlRequest::decode(full.data(), full.size()));
    for (std::size_t cut = 0; cut < full.size(); ++cut) {
        // Exact-size heap copy so ASan flags any read past `cut`.
        std::vector<uint8_t> part(full.begin(), full.begin() + static_cast<std::ptrdiff_t>(cut));
        EXPECT_FALSE(pc::NamespaceDdlRequest::decode(part.data(), part.size())) << "cut " << cut;
    }
    // Trailing garbage, unknown kind and reserved mask bits are refused too.
    auto extra = full; extra.push_back(0);
    EXPECT_FALSE(pc::NamespaceDdlRequest::decode(extra.data(), extra.size()));
    // kind byte sits right after the ext.
    const std::size_t kindOff = pc::MessageHeader::ENCODED_SIZE + pc::extEncodedSize(m.ext);
    auto k = full; k[kindOff] = 3;
    EXPECT_FALSE(pc::NamespaceDdlRequest::decode(k.data(), k.size()));
    k[kindOff] = 0;
    EXPECT_FALSE(pc::NamespaceDdlRequest::decode(k.data(), k.size()));
    auto r = full; r[kindOff + 1 + 2 + m.name.size() + 3] = 0x20;
    EXPECT_FALSE(pc::NamespaceDdlRequest::decode(r.data(), r.size()));
    // ListNamespaces / ListNamespacesResponse: same exhaustive truncation.
    pc::ListNamespaces ln; ln.afterNsId = 5; ln.max = 100;
    std::vector<uint8_t> lb(ln.encodedSize());
    ASSERT_EQ(ln.encode(lb.data()), lb.size());
    for (std::size_t cut = 0; cut < lb.size(); ++cut) {
        std::vector<uint8_t> part(lb.begin(), lb.begin() + static_cast<std::ptrdiff_t>(cut));
        EXPECT_FALSE(pc::ListNamespaces::decode(part.data(), part.size())) << "cut " << cut;
    }
    pc::ListNamespacesResponse lr;
    lr.namespaces = {{1, "a", 0, distinctLimits()}, {2, "bb", 1, rp::NamespaceLimits{}}};
    lr.nextAfter = 2;
    std::vector<uint8_t> rb(lr.encodedSize());
    ASSERT_EQ(lr.encode(rb.data()), rb.size());
    for (std::size_t cut = 0; cut < rb.size(); ++cut) {
        std::vector<uint8_t> part(rb.begin(), rb.begin() + static_cast<std::ptrdiff_t>(cut));
        EXPECT_FALSE(pc::ListNamespacesResponse::decode(part.data(), part.size())) << "cut " << cut;
    }
    // A count claiming more entries than the bytes hold is refused without a huge reserve.
    auto lie = header(83, 1, 2, 2);
    lie.push_back(0xFF); lie.push_back(0xFF);
    lie.insert(lie.end(), 4, 0);
    EXPECT_FALSE(pc::ListNamespacesResponse::decode(lie.data(), lie.size()));
}

TEST(NamespaceMessagesRegression, ListResponseRoundTripRegression) {
    for (const std::size_t n : {std::size_t{0}, std::size_t{1}, std::size_t{300}}) {
        pc::ListNamespacesResponse m;
        m.sessionId = 3; m.correlationId = 4; m.nextAfter = n == 0 ? 0u : static_cast<uint32_t>(n + 1000);
        for (std::size_t i = 0; i < n; ++i) {
            auto l = distinctLimits(); l.maxTopics = i;
            m.namespaces.push_back({static_cast<uint32_t>(i + 1), "ns-" + std::to_string(i),
                                    static_cast<uint8_t>(i % 3), l});
        }
        std::vector<uint8_t> buf(m.encodedSize() + 8, 0);
        const std::size_t w = m.encode(buf.data());
        ASSERT_EQ(w, m.encodedSize());
        buf.resize(w);
        auto d = pc::ListNamespacesResponse::decode(buf.data(), buf.size());
        ASSERT_TRUE(d) << n;
        EXPECT_EQ(d->sessionId, 3); EXPECT_EQ(d->correlationId, 4);
        EXPECT_EQ(d->namespaces, m.namespaces);
        EXPECT_EQ(d->nextAfter, m.nextAfter);
    }
    pc::ListNamespaces q; q.sessionId = 5; q.correlationId = 6; q.afterNsId = 0xDEADBEEFu; q.max = 0xFFFF;
    std::vector<uint8_t> qb(q.encodedSize());
    ASSERT_EQ(q.encode(qb.data()), qb.size());
    auto dq = pc::ListNamespaces::decode(qb.data(), qb.size());
    ASSERT_TRUE(dq);
    EXPECT_EQ(dq->afterNsId, 0xDEADBEEFu); EXPECT_EQ(dq->max, 0xFFFFu);
    EXPECT_EQ(dq->sessionId, 5); EXPECT_EQ(dq->correlationId, 6);
}

TEST(NamespaceMessagesRegression, SecurityEnumsRegression) {
    EXPECT_EQ(static_cast<uint8_t>(rp::SecurityOperation::Truncate), 7);
    EXPECT_EQ(static_cast<uint8_t>(rp::SecurityResourceType::Namespace), 5);
    EXPECT_TRUE(rp::validSecurityOperation(7));
    EXPECT_FALSE(rp::validSecurityOperation(8));
    EXPECT_TRUE(rp::validSecurityOperation(6));
    EXPECT_TRUE(rp::validSecurityResourceType(5));
    EXPECT_FALSE(rp::validSecurityResourceType(6));
    EXPECT_TRUE(rp::validSecurityResourceType(4));
}

TEST(NamespaceMessagesRegression, TemplateDispatchRegression) {
    for (const uint16_t tid : {uint16_t{81}, uint16_t{82}, uint16_t{83}}) {
        auto b = header(tid, 1, 2, 0);
        const auto fc = pc::checkFrame(b.data(), b.size());
        EXPECT_EQ(fc.status, pc::FrameStatus::Ok) << tid;
        EXPECT_EQ(fc.templateId, tid);
    }
    for (const uint16_t tid : {uint16_t{87}, uint16_t{95}}) {
        auto b = header(tid, 1, 2, 0);
        EXPECT_EQ(pc::checkFrame(b.data(), b.size()).status, pc::FrameStatus::UnknownTemplate) << tid;
    }
    EXPECT_EQ(static_cast<uint16_t>(pc::TemplateId::NamespaceDdl), 81);
    EXPECT_EQ(static_cast<uint16_t>(pc::TemplateId::ListNamespaces), 82);
    EXPECT_EQ(static_cast<uint16_t>(pc::TemplateId::ListNamespacesResponse), 83);
    EXPECT_EQ(pc::kProtocolVersion, 3);
    EXPECT_EQ(pc::kClientVersion, 16);   // v16: NAT-safe response egress; v15: RecordEgressUnavailable (#450)
    EXPECT_EQ(pc::kClientVersionNamespaces, 14);
    EXPECT_EQ(pc::kBrokerVersion, 9);
}

TEST(NamespaceMessagesRegression, ErrorCodeValuesRegression) {
    EXPECT_EQ(static_cast<uint16_t>(pc::ErrorCode::NamespaceSuspended), 51);
    EXPECT_EQ(static_cast<uint16_t>(pc::ErrorCode::NamespaceLimitExceeded), 52);
    EXPECT_EQ(static_cast<uint16_t>(pc::ErrorCode::Throttled), 53);
    EXPECT_EQ(static_cast<uint16_t>(pc::ErrorCode::ViewPaused), 54);
}

namespace {
pc::NamespaceDdlRequest ddl(pc::NamespaceDdlKind k) {
    pc::NamespaceDdlRequest m;
    m.sessionId = 1; m.correlationId = 2; m.ext = ext(); m.kind = k; m.name = "acme";
    m.presentMask = {0x03, 0, 0, 0}; m.limits = distinctLimits();
    return m;
}
std::vector<uint8_t> enc(const pc::NamespaceDdlRequest& m) {
    std::vector<uint8_t> b(m.encodedSize() + 8, 0);
    const std::size_t n = m.encode(b.data());
    b.resize(n);
    return b;
}
} // namespace

TEST(NamespaceDdlRegression, SuspendCarriesModeByteRegression) {
    for (const auto mode : {pc::NsSuspendMode::ReadOnly, pc::NsSuspendMode::RejectAll}) {
        auto m = ddl(pc::NamespaceDdlKind::Suspend); m.mode = mode;
        gWire = enc(m);
        ASSERT_EQ(gWire.size(), m.encodedSize());
        ASSERT_EQ(gWire.size(), pc::MessageHeader::ENCODED_SIZE + pc::extEncodedSize(m.ext) + 1u + 2u + m.name.size() + 1u);
        auto d = pc::NamespaceDdlRequest::decode(gWire.data(), gWire.size());
        ASSERT_TRUE(d);
        EXPECT_EQ(d->name, "acme"); EXPECT_EQ(d->kind, pc::NamespaceDdlKind::Suspend); EXPECT_EQ(d->mode, mode);
        for (const uint8_t bad : {uint8_t{0}, uint8_t{3}}) {
            auto p = gWire; p.back() = bad;
            EXPECT_FALSE(pc::NamespaceDdlRequest::decode(p.data(), p.size())) << int(bad);
        }
    }
    auto bad = ddl(pc::NamespaceDdlKind::Suspend); bad.mode = static_cast<pc::NsSuspendMode>(3);
    std::vector<uint8_t> out(bad.encodedSize() + 8);
    EXPECT_EQ(bad.encode(out.data()), 0u);
}

TEST(NamespaceDdlRegression, ResumeAndDeleteCarryNameOnlyRegression) {
    for (const auto k : {pc::NamespaceDdlKind::Resume, pc::NamespaceDdlKind::Delete}) {
        auto m = ddl(k);
        gWire = enc(m);
        EXPECT_EQ(gWire.size(), 24u + pc::extEncodedSize(m.ext) + 1u + 2u + m.name.size());
        auto d = pc::NamespaceDdlRequest::decode(gWire.data(), gWire.size());
        ASSERT_TRUE(d);
        EXPECT_EQ(d->kind, k); EXPECT_EQ(d->name, "acme");
        auto withLimits = gWire;
        withLimits.insert(withLimits.end(), pc::kNamespacePresentMaskBytes + rp::kNamespaceLimitsBytes, 0);
        EXPECT_FALSE(pc::NamespaceDdlRequest::decode(withLimits.data(), withLimits.size()));
        auto extra = gWire; extra.push_back(0);
        EXPECT_FALSE(pc::NamespaceDdlRequest::decode(extra.data(), extra.size()));
    }
}

TEST(NamespaceDdlRegression, TruncatedAtEveryOffsetRegression) {
    for (uint8_t k = 1; k <= 5; ++k) {
        auto m = ddl(static_cast<pc::NamespaceDdlKind>(k));
        const auto full = enc(m);
        ASSERT_FALSE(full.empty()) << int(k);
        EXPECT_TRUE(pc::NamespaceDdlRequest::decode(full.data(), full.size())) << int(k);
        for (std::size_t len = 0; len < full.size(); ++len) {
            std::vector<uint8_t> part(full.begin(), full.begin() + static_cast<std::ptrdiff_t>(len));
            EXPECT_FALSE(pc::NamespaceDdlRequest::decode(part.data(), part.size())) << "kind " << int(k) << " len " << len;
        }
    }
}

TEST(NamespaceDdlRegression, KindOutOfRangeRefusedRegression) {
    auto m = ddl(pc::NamespaceDdlKind::Resume);
    auto b = enc(m);
    const std::size_t kindOff = pc::MessageHeader::ENCODED_SIZE + pc::extEncodedSize(m.ext);
    for (const uint8_t k : {uint8_t{0}, uint8_t{6}}) {
        auto p = b; p[kindOff] = k;
        EXPECT_FALSE(pc::NamespaceDdlRequest::decode(p.data(), p.size())) << int(k);
    }
}

TEST(NamespaceUsageRegression, RoundTripRegression) {
    pc::NamespaceUsageRequest q; q.sessionId = 5; q.correlationId = 6; q.name = "acme";
    std::vector<uint8_t> qb(q.encodedSize());
    ASSERT_EQ(q.encode(qb.data()), qb.size());
    auto dq = pc::NamespaceUsageRequest::decode(qb.data(), qb.size());
    ASSERT_TRUE(dq);
    EXPECT_EQ(dq->name, "acme"); EXPECT_EQ(dq->sessionId, 5); EXPECT_EQ(dq->correlationId, 6);
    q.name.clear();
    EXPECT_EQ(q.encode(qb.data()), 0u);

    pc::NamespaceUsageResponse r;
    r.sessionId = 7; r.correlationId = 8; r.code = 0; r.state = 2; r.limits = distinctLimits();
    for (std::size_t i = 0; i < r.usage.size(); ++i) r.usage[i] = 1000 + i;
    std::vector<uint8_t> rb(pc::NamespaceUsageResponse::encodedSize());
    ASSERT_EQ(r.encode(rb.data(), rb.size()), rb.size());
    auto d = pc::NamespaceUsageResponse::decode(rb.data(), rb.size());
    ASSERT_TRUE(d);
    EXPECT_EQ(d->sessionId, 7); EXPECT_EQ(d->correlationId, 8); EXPECT_EQ(d->code, 0); EXPECT_EQ(d->state, 2);
    EXPECT_EQ(d->limits, r.limits);
    for (std::size_t i = 0; i < r.usage.size(); ++i) EXPECT_EQ(d->usage[i], 1000u + i) << i;
    for (std::size_t len = 0; len < rb.size(); ++len) {
        std::vector<uint8_t> part(rb.begin(), rb.begin() + static_cast<std::ptrdiff_t>(len));
        EXPECT_FALSE(pc::NamespaceUsageResponse::decode(part.data(), part.size())) << len;
    }
    auto extra = rb; extra.push_back(0);
    EXPECT_FALSE(pc::NamespaceUsageResponse::decode(extra.data(), extra.size()));
}

TEST(NamespaceUsageRegression, ShortCapReturnsZeroRegression) {
    pc::NamespaceUsageResponse r;
    const std::size_t sz = pc::NamespaceUsageResponse::encodedSize();
    EXPECT_EQ(sz, 24u + 3u + rp::kNamespaceLimitsBytes + 8u * 29u);
    std::vector<uint8_t> b(sz);
    EXPECT_EQ(r.encode(b.data(), sz - 1), 0u);
    EXPECT_EQ(r.encode(b.data(), sz), sz);
}

TEST(ThrottledRegression, RetryAfterRoundTripRegression) {
    pc::ErrorResponse e;
    e.sessionId = 1; e.correlationId = 2; e.code = static_cast<int16_t>(pc::ErrorCode::Throttled);
    e.message = "slow down"; e.retryAfterNs = 123456789012ULL;
    std::vector<uint8_t> b(28 + e.message.size() + 8);
    ASSERT_EQ(e.encode(b.data()), b.size());
    auto d = pc::ErrorResponse::decode(b.data(), b.size());
    ASSERT_TRUE(d);
    EXPECT_EQ(d->retryAfterNs, 123456789012ULL); EXPECT_EQ(d->message, "slow down");
    std::vector<uint8_t> t(b.begin(), b.end() - 8);
    auto d2 = pc::ErrorResponse::decode(t.data(), t.size());
    ASSERT_TRUE(d2);
    EXPECT_EQ(d2->retryAfterNs, 0u);
    e.code = static_cast<int16_t>(pc::ErrorCode::InvalidRequest);
    std::vector<uint8_t> o(64);
    EXPECT_EQ(e.encode(o.data()), 28 + e.message.size());
}

TEST(DispatchRegression, Templates84And85RoutedRegression) {
    EXPECT_TRUE(pc::isKnownTemplate(84));
    EXPECT_TRUE(pc::isKnownTemplate(85));
    EXPECT_FALSE(pc::isKnownTemplate(87));
    EXPECT_FALSE(pc::isKnownTemplate(95));
}
