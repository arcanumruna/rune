// tests/protocol/client/MetadataMessagesTest.cpp
//
// MD1 Task 10: Protocol codec tests for MetadataRequest and MetadataResponse.
//
// Coverage:
//   MetadataRequest:
//     MR-1: round-trip empty topic list (all topics)
//     MR-2: round-trip with specific topic ids
//     MR-3: LE bytes correct (hand-verify)
//     MR-4: truncation reject (too few bytes)
//     MR-5: unknown-version reject (version > kProtocolVersion)
//     MR-6: templateId 8 is in the control range — Dispatch recognises it
//
//   MetadataResponse:
//     MRR-1: round-trip empty brokers + empty partitions
//     MRR-2: round-trip with multiple brokers + partitions
//     MRR-3: LE bytes correct (hand-verify first partition entry)
//     MRR-4: truncation reject (truncate after broker count, after partition count,
//             in the middle of a partition entry)
//     MRR-5: unknown-version reject
//     MRR-6: templateId 9 is in the control range — Dispatch recognises it

#include <gtest/gtest.h>

#include <rune/protocol/client/MetadataMessages.h>
#include <rune/protocol/client/Dispatch.h>
#include <rune/protocol/client/MessageHeader.h>
#include <rune/protocol/client/Protocol.h>

#include <array>
#include <cstdint>
#include <cstring>
#include <vector>

namespace rpc = rune::protocol::client;

// ===========================================================================
// Helpers
// ===========================================================================

// Build a raw MetadataRequest buffer for round-trip tests.
static std::vector<uint8_t> buildReqBuf(int64_t sid, int64_t corr,
                                         std::vector<int32_t> tids)
{
    rpc::MetadataRequest req;
    req.sessionId     = sid;
    req.correlationId = corr;
    req.topicIds      = std::span<const int32_t>(tids.data(), tids.size());
    std::vector<uint8_t> buf(req.encodedSize());
    req.encode(buf.data());
    return buf;
}

// Build a raw MetadataResponse buffer for round-trip tests.
static std::vector<uint8_t> buildRespBuf(
    int64_t sid, int64_t corr,
    std::vector<rpc::BrokerEntry> brokers,
    std::vector<rpc::PartitionEntry> parts)
{
    rpc::MetadataResponse resp;
    resp.sessionId     = sid;
    resp.correlationId = corr;
    resp.brokers       = std::span<const rpc::BrokerEntry>(brokers.data(), brokers.size());
    resp.partitions    = std::span<const rpc::PartitionEntry>(parts.data(), parts.size());
    std::vector<uint8_t> buf(resp.encodedSize());
    resp.encode(buf.data());
    return buf;
}

// ===========================================================================
// MetadataRequest tests
// ===========================================================================

// MR-1: round-trip empty topic list.
TEST(MetadataMessages, RequestRoundTripEmpty)
{
    const int64_t sid  = 0xAABBCCDDEEFF0011LL;
    const int64_t corr = 0x1122334455667788LL;
    auto buf = buildReqBuf(sid, corr, {});

    auto opt = rpc::MetadataRequest::decode(buf.data(), buf.size());
    ASSERT_TRUE(opt.has_value());
    EXPECT_EQ(opt->sessionId,     sid);
    EXPECT_EQ(opt->correlationId, corr);
    EXPECT_TRUE(opt->topicIds.empty());
}

// MR-2: round-trip with specific topic ids.
TEST(MetadataMessages, RequestRoundTripWithTopics)
{
    const int64_t sid  = 0x0102030405060708LL;
    const int64_t corr = 0x0A0B0C0D0E0F1011LL;
    std::vector<int32_t> tids = {1, 2, 3, 100, 999};
    auto buf = buildReqBuf(sid, corr, tids);

    auto opt = rpc::MetadataRequest::decode(buf.data(), buf.size());
    ASSERT_TRUE(opt.has_value());
    EXPECT_EQ(opt->sessionId,     sid);
    EXPECT_EQ(opt->correlationId, corr);
    ASSERT_EQ(opt->topicIds.size(), tids.size());
    // MISALIGNMENT FIX: decode() now copies each int32 via getLE32() into an
    // owned, naturally-aligned std::vector<int32_t> (ownedTopicIds_) and points
    // topicIds at it.  So the span can be dereferenced directly as int32_t
    // WITHOUT the previous reinterpret_cast at a 4-byte-misaligned wire offset.
    // Assert the backing storage is 4-byte aligned (proves we're not aliasing
    // the misaligned wire buffer) and that the values decode correctly.
    EXPECT_EQ(reinterpret_cast<std::uintptr_t>(opt->topicIds.data()) % alignof(int32_t), 0u)
        << "decoded topicIds must be naturally aligned (owned vector, not wire buffer)";
    EXPECT_EQ(opt->topicIds.data(), opt->ownedTopicIds_.data())
        << "topicIds must view the owned vector after decode";
    for (std::size_t i = 0; i < tids.size(); ++i) {
        EXPECT_EQ(opt->topicIds[i], tids[i]) << "mismatch at index " << i;
    }
}

// MR-3: LE bytes — verify the count and first topic id byte-by-byte.
TEST(MetadataMessages, RequestLEBytes)
{
    // Two topics: topicId=0x00000001, topicId=0x00000200 (to see byte ordering).
    std::vector<int32_t> tids = {1, 0x200};
    auto buf = buildReqBuf(0, 0, tids);

    // Header is 24 bytes; BODY_FIXED=0; then count (u16 LE).
    constexpr std::size_t countOff = rpc::MessageHeader::ENCODED_SIZE;
    EXPECT_EQ(buf[countOff],     2);   // count=2, low byte
    EXPECT_EQ(buf[countOff + 1], 0);   // count=2, high byte

    // First int32 at countOff+2: value=1 in LE = {01, 00, 00, 00}
    EXPECT_EQ(buf[countOff + 2], 0x01);
    EXPECT_EQ(buf[countOff + 3], 0x00);
    EXPECT_EQ(buf[countOff + 4], 0x00);
    EXPECT_EQ(buf[countOff + 5], 0x00);

    // Second int32 at countOff+6: value=0x200 in LE = {00, 02, 00, 00}
    EXPECT_EQ(buf[countOff + 6], 0x00);
    EXPECT_EQ(buf[countOff + 7], 0x02);
    EXPECT_EQ(buf[countOff + 8], 0x00);
    EXPECT_EQ(buf[countOff + 9], 0x00);
}

// MR-4: truncation reject.
TEST(MetadataMessages, RequestTruncationReject)
{
    std::vector<int32_t> tids = {42};
    auto buf = buildReqBuf(0, 0, tids);

    // Truncate: only the header (missing count field).
    EXPECT_FALSE(rpc::MetadataRequest::decode(buf.data(),
                 rpc::MessageHeader::ENCODED_SIZE).has_value())
        << "should reject: missing count u16";

    // Truncate: count present (2 bytes) but only 2 of the 4 topic-id bytes.
    const std::size_t partial = rpc::MessageHeader::ENCODED_SIZE + 2u + 2u;
    if (partial < buf.size()) {
        EXPECT_FALSE(rpc::MetadataRequest::decode(buf.data(), partial).has_value())
            << "should reject: partial topic id bytes";
    }

    // Zero bytes: always reject.
    EXPECT_FALSE(rpc::MetadataRequest::decode(buf.data(), 0).has_value());
}

// MR-5: unknown-version reject (version > kProtocolVersion).
TEST(MetadataMessages, RequestUnknownVersionReject)
{
    std::vector<int32_t> tids = {};
    auto buf = buildReqBuf(0, 0, tids);

    // Patch the version field in the header to an unsupported value.
    // MessageHeader layout: blockLength(2)+templateId(2)+schemaId(2)+version(2)+sid(8)+corr(8) = 24
    // version is at offset 6.
    buf[6] = 0xFF;
    buf[7] = 0x00;  // version = 255 > kProtocolVersion(1)

    // checkFrame should return UnsupportedVersion.
    const auto fc = rpc::checkFrame(buf.data(), buf.size());
    EXPECT_EQ(fc.status, rpc::FrameStatus::UnsupportedVersion);

    // MetadataRequest::decode should also reject (version check inside header decode).
    // Actually MessageHeader::decode does not check version — checkFrame does.
    // So Dispatch-level rejection is the right path.  The codec itself will
    // decode successfully (it checks schemaId and templateId only) unless we
    // verify via Dispatch first, which is the standard gateway pattern.
    // The test confirms Dispatch catches it.
}

// MR-6: templateId 8 is recognised by Dispatch.
TEST(MetadataMessages, RequestTemplateIdKnown)
{
    std::vector<int32_t> tids = {};
    auto buf = buildReqBuf(0, 0, tids);

    const auto fc = rpc::checkFrame(buf.data(), buf.size());
    EXPECT_EQ(fc.status,     rpc::FrameStatus::Ok);
    EXPECT_EQ(fc.templateId, static_cast<uint16_t>(rpc::TemplateId::MetadataRequest));

    EXPECT_EQ(rpc::peekTemplateId(buf.data(), buf.size()),
              std::optional<uint16_t>(static_cast<uint16_t>(rpc::TemplateId::MetadataRequest)));
}

// ===========================================================================
// MetadataResponse tests
// ===========================================================================

// MRR-1: round-trip empty brokers and partitions.
TEST(MetadataMessages, ResponseRoundTripEmpty)
{
    const int64_t sid  = 0xCAFEBABEDEADBEEFLL;
    const int64_t corr = 0x0102030405060708LL;
    auto buf = buildRespBuf(sid, corr, {}, {});

    auto opt = rpc::MetadataResponse::decode(buf.data(), buf.size());
    ASSERT_TRUE(opt.has_value());
    EXPECT_EQ(opt->sessionId,     sid);
    EXPECT_EQ(opt->correlationId, corr);
    EXPECT_TRUE(opt->brokers.empty());
    EXPECT_TRUE(opt->partitions.empty());
}

// MRR-2: round-trip multiple brokers and partitions.
TEST(MetadataMessages, ResponseRoundTripFull)
{
    const int64_t sid  = 0x1122334455667788LL;
    const int64_t corr = 0x0A0B0C0D0E0F1011LL;

    rpc::BrokerEntry b0; b0.nodeId = 1; b0.port = 40200; b0.host = "node1.internal";
    rpc::BrokerEntry b1; b1.nodeId = 2; b1.port = 40201; b1.host = "node2.internal";

    rpc::PartitionEntry p0; p0.topicId = 10; p0.partition = 0; p0.leaderId = 1; p0.leaderEpoch = 3;
    rpc::PartitionEntry p1; p1.topicId = 10; p1.partition = 1; p1.leaderId = 2; p1.leaderEpoch = 3;
    rpc::PartitionEntry p2; p2.topicId = 20; p2.partition = 0; p2.leaderId = 1; p2.leaderEpoch = 5;

    auto buf = buildRespBuf(sid, corr, {b0, b1}, {p0, p1, p2});

    auto opt = rpc::MetadataResponse::decode(buf.data(), buf.size());
    ASSERT_TRUE(opt.has_value());
    EXPECT_EQ(opt->sessionId,     sid);
    EXPECT_EQ(opt->correlationId, corr);

    // Check brokers.
    ASSERT_EQ(opt->brokers.size(), 2u);
    EXPECT_EQ(opt->brokers[0].nodeId, 1);
    EXPECT_EQ(opt->brokers[0].port,   40200);
    EXPECT_EQ(opt->brokers[0].host,   "node1.internal");
    EXPECT_EQ(opt->brokers[1].nodeId, 2);
    EXPECT_EQ(opt->brokers[1].port,   40201);
    EXPECT_EQ(opt->brokers[1].host,   "node2.internal");

    // Check partitions.
    ASSERT_EQ(opt->partitions.size(), 3u);
    EXPECT_EQ(opt->partitions[0].topicId,     10);
    EXPECT_EQ(opt->partitions[0].partition,   0);
    EXPECT_EQ(opt->partitions[0].leaderId,    1);
    EXPECT_EQ(opt->partitions[0].leaderEpoch, 3);
    EXPECT_EQ(opt->partitions[1].topicId,     10);
    EXPECT_EQ(opt->partitions[1].partition,   1);
    EXPECT_EQ(opt->partitions[1].leaderId,    2);
    EXPECT_EQ(opt->partitions[1].leaderEpoch, 3);
    EXPECT_EQ(opt->partitions[2].topicId,     20);
    EXPECT_EQ(opt->partitions[2].partition,   0);
    EXPECT_EQ(opt->partitions[2].leaderId,    1);
    EXPECT_EQ(opt->partitions[2].leaderEpoch, 5);
}

// MRR-3: LE bytes — hand-verify the first partition entry bytes.
TEST(MetadataMessages, ResponseLEBytesPartition)
{
    // Single partition: topicId=0x01020304, partition=0x0A0B0C0D,
    //                   leaderId=0x10203040, leaderEpoch=0x00000001
    rpc::PartitionEntry p;
    p.topicId     = 0x01020304;
    p.partition   = 0x0A0B0C0D;
    p.leaderId    = 0x10203040;
    p.leaderEpoch = 0x00000001;

    auto buf = buildRespBuf(0, 0, {}, {p});

    // Layout after header(24) + brokerCount(2) + partitionCount(2) = offset 28.
    // Note: there are no brokers, so the broker array is just the count (2 bytes).
    constexpr std::size_t partBase = rpc::MessageHeader::ENCODED_SIZE
                                   + 2u  // brokerCount = 0
                                   + 2u; // partitionCount = 1
    EXPECT_EQ(buf[partBase + 0],  0x04);  // topicId LE low byte
    EXPECT_EQ(buf[partBase + 1],  0x03);
    EXPECT_EQ(buf[partBase + 2],  0x02);
    EXPECT_EQ(buf[partBase + 3],  0x01);
    EXPECT_EQ(buf[partBase + 4],  0x0D);  // partition LE low byte
    EXPECT_EQ(buf[partBase + 5],  0x0C);
    EXPECT_EQ(buf[partBase + 6],  0x0B);
    EXPECT_EQ(buf[partBase + 7],  0x0A);
    EXPECT_EQ(buf[partBase + 8],  0x40);  // leaderId LE low byte
    EXPECT_EQ(buf[partBase + 9],  0x30);
    EXPECT_EQ(buf[partBase + 10], 0x20);
    EXPECT_EQ(buf[partBase + 11], 0x10);
    EXPECT_EQ(buf[partBase + 12], 0x01);  // leaderEpoch=1 LE
    EXPECT_EQ(buf[partBase + 13], 0x00);
    EXPECT_EQ(buf[partBase + 14], 0x00);
    EXPECT_EQ(buf[partBase + 15], 0x00);
    // ingressStreamId=0 (default) → 4 zero bytes at offset +16..+19
    EXPECT_EQ(buf[partBase + 16], 0x00);
    EXPECT_EQ(buf[partBase + 17], 0x00);
    EXPECT_EQ(buf[partBase + 18], 0x00);
    EXPECT_EQ(buf[partBase + 19], 0x00);
}

// MRR-4: truncation reject at various offsets.
TEST(MetadataMessages, ResponseTruncationReject)
{
    rpc::BrokerEntry b; b.nodeId = 1; b.port = 40200; b.host = "n1";
    rpc::PartitionEntry p; p.topicId = 1; p.partition = 0; p.leaderId = 1; p.leaderEpoch = 1;
    auto buf = buildRespBuf(0, 0, {b}, {p});

    // Only header (missing brokerCount).
    EXPECT_FALSE(rpc::MetadataResponse::decode(buf.data(),
                 rpc::MessageHeader::ENCODED_SIZE).has_value())
        << "missing brokerCount";

    // Header + brokerCount but no broker data.
    EXPECT_FALSE(rpc::MetadataResponse::decode(buf.data(),
                 rpc::MessageHeader::ENCODED_SIZE + 2u).has_value())
        << "missing broker nodeId";

    // Zero bytes.
    EXPECT_FALSE(rpc::MetadataResponse::decode(buf.data(), 0).has_value());

    // Just 1 byte short of full.
    if (buf.size() > 1) {
        EXPECT_FALSE(rpc::MetadataResponse::decode(buf.data(), buf.size() - 1).has_value())
            << "1 byte short of full";
    }
}

// MRR-5: unknown-version reject via Dispatch.
TEST(MetadataMessages, ResponseUnknownVersionReject)
{
    auto buf = buildRespBuf(0, 0, {}, {});

    // Patch version to unsupported value.
    // kProtocolVersion is now 3 (read-rework bump); use 4 to be unsupported.
    buf[6] = 0x04;  // version=4 > kProtocolVersion(3)
    buf[7] = 0x00;

    const auto fc = rpc::checkFrame(buf.data(), buf.size());
    EXPECT_EQ(fc.status, rpc::FrameStatus::UnsupportedVersion);
}

// MRR-6: templateId 9 is recognised by Dispatch.
TEST(MetadataMessages, ResponseTemplateIdKnown)
{
    auto buf = buildRespBuf(0, 0, {}, {});

    const auto fc = rpc::checkFrame(buf.data(), buf.size());
    EXPECT_EQ(fc.status,     rpc::FrameStatus::Ok);
    EXPECT_EQ(fc.templateId, static_cast<uint16_t>(rpc::TemplateId::MetadataResponse));

    EXPECT_EQ(rpc::peekTemplateId(buf.data(), buf.size()),
              std::optional<uint16_t>(static_cast<uint16_t>(rpc::TemplateId::MetadataResponse)));
}

// MRR-7: E5.1 — ingressStreamId round-trips through encode/decode (new 5th field).
// Verifies encode stride (20 bytes/entry) and decode reads at offset +16.
TEST(MetadataMessages, ResponseIngressStreamIdRoundTrip)
{
    const int64_t sid  = 0xDEADC0DE00000001LL;
    const int64_t corr = 0x0000000100000002LL;

    rpc::PartitionEntry p0;
    p0.topicId        = 1;
    p0.partition      = 0;
    p0.leaderId       = 5;
    p0.leaderEpoch    = 7;
    p0.ingressStreamId = 1001;
    p0.egressControlPort = 40300;
    p0.egressStreamId  = 2001;

    rpc::PartitionEntry p1;
    p1.topicId        = 1;
    p1.partition      = 1;
    p1.leaderId       = 6;
    p1.leaderEpoch    = 7;
    p1.ingressStreamId = 1002;
    p1.egressControlPort = 40301;
    p1.egressStreamId  = 2002;

    rpc::PartitionEntry p2;
    p2.topicId        = 2;
    p2.partition      = 0;
    p2.leaderId       = 5;
    p2.leaderEpoch    = 3;
    p2.ingressStreamId = 0;  // default — exercises zero-value path
    p2.egressControlPort = 0;
    p2.egressStreamId  = 0;

    auto buf = buildRespBuf(sid, corr, {}, {p0, p1, p2});

    // encodedSize must reflect 3 * 28 = 84 bytes for partitions.
    constexpr std::size_t hdrBrokerPart = rpc::MessageHeader::ENCODED_SIZE
                                        + 2u   // brokerCount
                                        + 2u;  // partitionCount
    EXPECT_EQ(buf.size(), hdrBrokerPart + 3u * 28u) << "stride must be 28 bytes/entry";

    auto opt = rpc::MetadataResponse::decode(buf.data(), buf.size());
    ASSERT_TRUE(opt.has_value()) << "decode must succeed";
    EXPECT_EQ(opt->sessionId,     sid);
    EXPECT_EQ(opt->correlationId, corr);
    ASSERT_EQ(opt->partitions.size(), 3u);

    EXPECT_EQ(opt->partitions[0].topicId,         1);
    EXPECT_EQ(opt->partitions[0].partition,        0);
    EXPECT_EQ(opt->partitions[0].leaderId,         5);
    EXPECT_EQ(opt->partitions[0].leaderEpoch,      7);
    EXPECT_EQ(opt->partitions[0].ingressStreamId,  1001);

    EXPECT_EQ(opt->partitions[1].topicId,         1);
    EXPECT_EQ(opt->partitions[1].partition,        1);
    EXPECT_EQ(opt->partitions[1].leaderId,         6);
    EXPECT_EQ(opt->partitions[1].leaderEpoch,      7);
    EXPECT_EQ(opt->partitions[1].ingressStreamId,  1002);

    EXPECT_EQ(opt->partitions[2].topicId,         2);
    EXPECT_EQ(opt->partitions[2].partition,        0);
    EXPECT_EQ(opt->partitions[2].leaderId,         5);
    EXPECT_EQ(opt->partitions[2].leaderEpoch,      3);
    EXPECT_EQ(opt->partitions[2].ingressStreamId,  0);

    // read-rework: egress control port + stream id round-trip (offsets +20, +24).
    EXPECT_EQ(opt->partitions[0].egressControlPort, 40300);
    EXPECT_EQ(opt->partitions[0].egressStreamId,    2001);
    EXPECT_EQ(opt->partitions[1].egressControlPort, 40301);
    EXPECT_EQ(opt->partitions[1].egressStreamId,    2002);
    EXPECT_EQ(opt->partitions[2].egressControlPort, 0);
    EXPECT_EQ(opt->partitions[2].egressStreamId,    0);

    // LE byte check for ingressStreamId=1001 (0x000003E9) at partition[0]+16.
    // partBase = hdrBrokerPart (no brokers).
    constexpr std::size_t partBase = hdrBrokerPart;
    EXPECT_EQ(buf[partBase + 16], 0xE9);  // 1001 LE: low byte
    EXPECT_EQ(buf[partBase + 17], 0x03);
    EXPECT_EQ(buf[partBase + 18], 0x00);
    EXPECT_EQ(buf[partBase + 19], 0x00);
}

// ===========================================================================
// isKnownTemplate boundary: 8 and 9 must now be known; 10-15 still unknown.
// ===========================================================================
TEST(MetadataMessages, IsKnownTemplateBoundaries)
{
    EXPECT_TRUE(rpc::isKnownTemplate(8))   << "MetadataRequest=8 must be known";
    EXPECT_TRUE(rpc::isKnownTemplate(9))   << "MetadataResponse=9 must be known";
    EXPECT_FALSE(rpc::isKnownTemplate(10)) << "10 is still unassigned";
    EXPECT_FALSE(rpc::isKnownTemplate(15)) << "15 is still unassigned";
}

// ===========================================================================
// 2026-09-30 self-contained worker shards: per-partition ingressEndpoint trailer.
// ===========================================================================
namespace {
std::vector<uint8_t> encodeResp(const std::vector<rpc::BrokerEntry>& brokers,
                                const std::vector<rpc::PartitionEntry>& parts) {
    rpc::MetadataResponse resp;
    resp.sessionId = 5; resp.correlationId = 6;
    resp.brokers    = std::span<const rpc::BrokerEntry>(brokers.data(), brokers.size());
    resp.partitions = std::span<const rpc::PartitionEntry>(parts.data(), parts.size());
    std::vector<uint8_t> buf(resp.encodedSize());
    EXPECT_EQ(resp.encode(buf.data()), buf.size());
    return buf;
}
} // namespace

TEST(MetadataMessagesIngressEndpoint, RoundTripsPerPartitionEndpoints) {
    rpc::BrokerEntry b; b.nodeId = 1; b.port = 40200; b.host = "10.0.0.1";
    rpc::PartitionEntry p0; p0.topicId = 3; p0.partition = 0; p0.leaderId = 1; p0.ingressEndpoint = "10.0.0.1:40500";
    rpc::PartitionEntry p1; p1.topicId = 3; p1.partition = 1; p1.leaderId = 1; p1.ingressEndpoint = "10.0.0.1:40501";
    auto buf = encodeResp({b}, {p0, p1});
    auto dec = rpc::MetadataResponse::decode(buf.data(), buf.size());
    ASSERT_TRUE(dec.has_value());
    ASSERT_EQ(dec->partitions.size(), 2u);
    EXPECT_EQ(dec->partitions[0].ingressEndpoint, "10.0.0.1:40500");
    EXPECT_EQ(dec->partitions[1].ingressEndpoint, "10.0.0.1:40501");
    EXPECT_EQ(dec->partitions[1].partition, 1);
}

TEST(MetadataMessagesIngressEndpoint, EmptyEndpointsAreByteIdenticalToLegacyLayout) {
    rpc::BrokerEntry b; b.nodeId = 1; b.port = 40200; b.host = "h";
    rpc::PartitionEntry p; p.topicId = 1; p.leaderId = 1;
    auto buf = encodeResp({b}, {p, p});
    // header + brokerCount + (nodeId+port+hostLen+1) + partitionCount + 2*28, no trailer.
    EXPECT_EQ(buf.size(), rpc::MessageHeader::ENCODED_SIZE + 2u + (4u + 4u + 2u + 1u) + 2u + 2u * 28u);
    auto dec = rpc::MetadataResponse::decode(buf.data(), buf.size());
    ASSERT_TRUE(dec.has_value());
    EXPECT_TRUE(dec->partitions[0].ingressEndpoint.empty());
}

TEST(MetadataMessagesIngressEndpoint, OlderDecoderIgnoresTrailer) {
    rpc::PartitionEntry p; p.topicId = 9; p.partition = 2; p.leaderId = 4; p.ingressEndpoint = "h:1";
    auto buf = encodeResp({}, {p});
    // A pre-trailer decoder consumes exactly header + 2 + 2 + 28 bytes; decoding that
    // prefix alone must succeed and yield identical fixed fields.
    const std::size_t legacyLen = rpc::MessageHeader::ENCODED_SIZE + 2u + 2u + 28u;
    ASSERT_GT(buf.size(), legacyLen);
    auto dec = rpc::MetadataResponse::decode(buf.data(), legacyLen);
    ASSERT_TRUE(dec.has_value());
    EXPECT_EQ(dec->partitions[0].topicId, 9);
    EXPECT_EQ(dec->partitions[0].partition, 2);
    EXPECT_EQ(dec->partitions[0].leaderId, 4);
    EXPECT_TRUE(dec->partitions[0].ingressEndpoint.empty());
}

TEST(MetadataMessagesIngressEndpointRegression, TornTrailerRejected) {
    rpc::PartitionEntry p; p.ingressEndpoint = "host:40500";
    auto buf = encodeResp({}, {p});
    for (std::size_t cut = 1; cut <= 12; ++cut) {
        EXPECT_FALSE(rpc::MetadataResponse::decode(buf.data(), buf.size() - cut).has_value())
            << "cut=" << cut;
    }
}

TEST(MetadataMessagesIngressEndpointRegression, TrailerCountMismatchRejected) {
    rpc::PartitionEntry p; p.ingressEndpoint = "host:1";
    auto buf = encodeResp({}, {p});
    const std::size_t trailerPos = rpc::MessageHeader::ENCODED_SIZE + 2u + 2u + 28u;
    buf[trailerPos] = 2;   // claims 2 endpoints for 1 partition
    EXPECT_FALSE(rpc::MetadataResponse::decode(buf.data(), buf.size()).has_value());
}
