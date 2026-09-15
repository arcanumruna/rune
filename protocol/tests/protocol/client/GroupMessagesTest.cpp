// GroupMessagesTest.cpp — CG-1 tests for consumer-group codecs (templateIds 32–39, 44).
//
// Coverage per message:
//   (1) encode→decode round-trip asserting all fields (incl. arrays, var fields
//       with/without content, empty memberId, multi-element arrays)
//   (2) truncation: decode(buf, buf.size()-1) → nullopt
//   (3) wrong-template/schema: decode → nullopt
// Plus: conformance vectors (fixed byte array + expected decode) for JoinGroup,
//       SyncGroupResponse (with assignment), and Assign (empty = revoke).

#include <gtest/gtest.h>
#include <rune/protocol/client/GroupMessages.h>

#include <array>
#include <climits>
#include <cstdint>
#include <cstring>
#include <vector>

namespace rpc = rune::protocol::client;

// ---------------------------------------------------------------------------
// Test-local LE readers
// ---------------------------------------------------------------------------
static uint8_t  leU8 (const uint8_t* buf, int off) { return buf[off]; }
static uint16_t leU16(const uint8_t* buf, int off) {
    return static_cast<uint16_t>(uint16_t(buf[off]) | (uint16_t(buf[off + 1]) << 8));
}
static int16_t  leS16(const uint8_t* buf, int off) {
    return static_cast<int16_t>(leU16(buf, off));
}
static int32_t  leS32(const uint8_t* buf, int off) {
    uint32_t v = uint32_t(buf[off])
               | (uint32_t(buf[off + 1]) << 8)
               | (uint32_t(buf[off + 2]) << 16)
               | (uint32_t(buf[off + 3]) << 24);
    return static_cast<int32_t>(v);
}
static int64_t  leS64(const uint8_t* buf, int off) {
    uint64_t v = 0;
    for (int i = 0; i < 8; ++i)
        v |= static_cast<uint64_t>(buf[off + i]) << (i * 8);
    return static_cast<int64_t>(v);
}

// Canonical field values shared across tests
static constexpr int64_t kSession     = static_cast<int64_t>(0x1122334455667788ULL);
static constexpr int64_t kCorrelation = static_cast<int64_t>(0x0A0B0C0D0E0F1011ULL);

static const uint8_t kGroupId[]  = {'g', 'r', 'p', '1'};
static const uint8_t kMemberId[] = {'m', 'b', 'r', 'A', 'B', 'C'};

// ===========================================================================
// JoinGroup (templateId 32, BODY_FIXED = 4)
// ===========================================================================

// ---------------------------------------------------------------------------
// JG-1. Round-trip: non-empty groupId, non-empty memberId, two subscribed topics.
// ---------------------------------------------------------------------------
TEST(JoinGroupCodec, RoundTripFull) {
    rpc::JoinGroup msg;
    msg.sessionId        = kSession;
    msg.correlationId    = kCorrelation;
    msg.sessionTimeoutMs = 30000;
    msg.groupId          = std::span<const uint8_t>(kGroupId, sizeof(kGroupId));
    msg.memberId         = std::span<const uint8_t>(kMemberId, sizeof(kMemberId));
    msg.subscribedTopicIds = {10, 20};

    // Total = 24+4 + 2+4 + 2+6 + 2+4+4 = 24+4+6+8+10 = 52
    const std::size_t expected = 24u + 4u + 2u + sizeof(kGroupId) + 2u + sizeof(kMemberId)
                               + 2u + 4u * 2u;
    ASSERT_EQ(msg.encodedSize(), expected);

    std::vector<uint8_t> buf(expected, 0u);
    const std::size_t written = msg.encode(buf.data());
    ASSERT_EQ(written, expected);

    // Header checks
    EXPECT_EQ(leU16(buf.data(), 0), rpc::JoinGroup::BODY_FIXED);   // blockLength = 4
    EXPECT_EQ(leU16(buf.data(), 2), rpc::JoinGroup::TEMPLATE_ID);  // templateId = 32
    EXPECT_EQ(leU16(buf.data(), 4), rpc::kSchemaId);               // 120
    EXPECT_EQ(leU16(buf.data(), 6), rpc::kProtocolVersion);        // 1
    EXPECT_EQ(leS64(buf.data(), 8),  kSession);
    EXPECT_EQ(leS64(buf.data(), 16), kCorrelation);

    // Fixed body
    EXPECT_EQ(leS32(buf.data(), 24), 30000);  // sessionTimeoutMs

    // groupId var field at offset 28
    EXPECT_EQ(leU16(buf.data(), 28), static_cast<uint16_t>(sizeof(kGroupId)));
    EXPECT_EQ(std::memcmp(buf.data() + 30, kGroupId, sizeof(kGroupId)), 0);

    // memberId var field
    int memberIdOff = 30 + static_cast<int>(sizeof(kGroupId));
    EXPECT_EQ(leU16(buf.data(), memberIdOff), static_cast<uint16_t>(sizeof(kMemberId)));
    EXPECT_EQ(std::memcmp(buf.data() + memberIdOff + 2, kMemberId, sizeof(kMemberId)), 0);

    // topics count
    int topicsOff = memberIdOff + 2 + static_cast<int>(sizeof(kMemberId));
    EXPECT_EQ(leU16(buf.data(), topicsOff), 2u);
    EXPECT_EQ(leS32(buf.data(), topicsOff + 2), 10);
    EXPECT_EQ(leS32(buf.data(), topicsOff + 6), 20);

    // Round-trip decode
    auto decoded = rpc::JoinGroup::decode(buf.data(), written);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded->sessionId,        kSession);
    EXPECT_EQ(decoded->correlationId,    kCorrelation);
    EXPECT_EQ(decoded->sessionTimeoutMs, 30000);
    ASSERT_EQ(decoded->groupId.size(),   sizeof(kGroupId));
    EXPECT_EQ(std::memcmp(decoded->groupId.data(), kGroupId, sizeof(kGroupId)), 0);
    ASSERT_EQ(decoded->memberId.size(),  sizeof(kMemberId));
    EXPECT_EQ(std::memcmp(decoded->memberId.data(), kMemberId, sizeof(kMemberId)), 0);
    ASSERT_EQ(decoded->subscribedTopicIds.size(), 2u);
    EXPECT_EQ(decoded->subscribedTopicIds[0], 10);
    EXPECT_EQ(decoded->subscribedTopicIds[1], 20);
}

// ---------------------------------------------------------------------------
// JG-2. Round-trip: empty memberId (new member), single topic.
// ---------------------------------------------------------------------------
TEST(JoinGroupCodec, EmptyMemberId) {
    rpc::JoinGroup msg;
    msg.sessionId        = kSession;
    msg.correlationId    = kCorrelation;
    msg.sessionTimeoutMs = 5000;
    msg.groupId          = std::span<const uint8_t>(kGroupId, sizeof(kGroupId));
    msg.memberId         = std::span<const uint8_t>{};  // empty = new member
    msg.subscribedTopicIds = {99};

    const std::size_t total = msg.encodedSize();
    std::vector<uint8_t> buf(total, 0u);
    const std::size_t written = msg.encode(buf.data());
    ASSERT_EQ(written, total);

    auto decoded = rpc::JoinGroup::decode(buf.data(), written);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded->sessionTimeoutMs,             5000);
    EXPECT_EQ(decoded->memberId.size(),              0u);
    ASSERT_EQ(decoded->subscribedTopicIds.size(),    1u);
    EXPECT_EQ(decoded->subscribedTopicIds[0],        99);
}

// ---------------------------------------------------------------------------
// JG-3. Round-trip: empty subscribedTopicIds list.
// ---------------------------------------------------------------------------
TEST(JoinGroupCodec, EmptyTopics) {
    rpc::JoinGroup msg;
    msg.sessionTimeoutMs = 1000;
    msg.groupId          = std::span<const uint8_t>(kGroupId, sizeof(kGroupId));
    msg.memberId         = std::span<const uint8_t>{};
    msg.subscribedTopicIds = {};

    const std::size_t total = msg.encodedSize();
    std::vector<uint8_t> buf(total, 0u);
    msg.encode(buf.data());

    auto decoded = rpc::JoinGroup::decode(buf.data(), total);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded->subscribedTopicIds.size(), 0u);
}

// ---------------------------------------------------------------------------
// JG-4. Truncation: decode(buf, total-1) → nullopt.
// ---------------------------------------------------------------------------
TEST(JoinGroupCodec, Truncation) {
    rpc::JoinGroup msg;
    msg.sessionTimeoutMs = 1000;
    msg.groupId          = std::span<const uint8_t>(kGroupId, sizeof(kGroupId));
    msg.memberId         = std::span<const uint8_t>(kMemberId, sizeof(kMemberId));
    msg.subscribedTopicIds = {1, 2, 3};

    const std::size_t total = msg.encodedSize();
    std::vector<uint8_t> buf(total, 0u);
    msg.encode(buf.data());

    EXPECT_FALSE(rpc::JoinGroup::decode(buf.data(), total - 1u).has_value());
    EXPECT_TRUE (rpc::JoinGroup::decode(buf.data(), total).has_value());
}

// ---------------------------------------------------------------------------
// JG-5. Wrong template → nullopt; wrong schema → nullopt.
// ---------------------------------------------------------------------------
TEST(JoinGroupCodec, WrongTemplateAndSchema) {
    rpc::JoinGroup msg;
    msg.sessionTimeoutMs = 1000;
    msg.groupId = std::span<const uint8_t>(kGroupId, sizeof(kGroupId));
    msg.memberId = std::span<const uint8_t>{};
    msg.subscribedTopicIds = {};

    const std::size_t total = msg.encodedSize();
    std::vector<uint8_t> buf(total, 0u);
    msg.encode(buf.data());

    // Wrong templateId
    std::vector<uint8_t> buf2 = buf;
    buf2[2] = 33;  // JoinGroupResponse
    buf2[3] = 0;
    EXPECT_FALSE(rpc::JoinGroup::decode(buf2.data(), total).has_value());

    // Wrong schemaId
    std::vector<uint8_t> buf3 = buf;
    buf3[4] = 0;
    buf3[5] = 0;
    EXPECT_FALSE(rpc::JoinGroup::decode(buf3.data(), total).has_value());
}

// ===========================================================================
// JoinGroupResponse (templateId 33, BODY_FIXED = 7)
// ===========================================================================

TEST(JoinGroupResponseCodec, RoundTrip) {
    rpc::JoinGroupResponse msg;
    msg.sessionId     = kSession;
    msg.correlationId = kCorrelation;
    msg.memberId      = std::span<const uint8_t>(kMemberId, sizeof(kMemberId));
    msg.generationId  = 42;
    msg.isLeader      = 1;
    msg.errorCode     = 0;

    const std::size_t total = msg.encodedSize();
    std::vector<uint8_t> buf(total, 0u);
    const std::size_t written = msg.encode(buf.data());
    ASSERT_EQ(written, total);

    auto decoded = rpc::JoinGroupResponse::decode(buf.data(), written);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded->sessionId,     kSession);
    EXPECT_EQ(decoded->correlationId, kCorrelation);
    EXPECT_EQ(decoded->generationId,  42);
    EXPECT_EQ(decoded->isLeader,      1u);
    EXPECT_EQ(decoded->errorCode,     0);
    ASSERT_EQ(decoded->memberId.size(), sizeof(kMemberId));
    EXPECT_EQ(std::memcmp(decoded->memberId.data(), kMemberId, sizeof(kMemberId)), 0);
}

TEST(JoinGroupResponseCodec, ErrorCodeNonZero) {
    rpc::JoinGroupResponse msg;
    msg.generationId = 0;
    msg.isLeader     = 0;
    msg.errorCode    = static_cast<int16_t>(rpc::ErrorCode::RebalanceInProgress);
    msg.memberId     = std::span<const uint8_t>{};

    const std::size_t total = msg.encodedSize();
    std::vector<uint8_t> buf(total, 0u);
    msg.encode(buf.data());

    auto decoded = rpc::JoinGroupResponse::decode(buf.data(), total);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded->errorCode, static_cast<int16_t>(rpc::ErrorCode::RebalanceInProgress));
}

TEST(JoinGroupResponseCodec, Truncation) {
    rpc::JoinGroupResponse msg;
    msg.generationId = 1;
    msg.isLeader     = 0;
    msg.errorCode    = 0;
    msg.memberId     = std::span<const uint8_t>(kMemberId, sizeof(kMemberId));

    const std::size_t total = msg.encodedSize();
    std::vector<uint8_t> buf(total, 0u);
    msg.encode(buf.data());

    EXPECT_FALSE(rpc::JoinGroupResponse::decode(buf.data(), total - 1u).has_value());
    EXPECT_TRUE (rpc::JoinGroupResponse::decode(buf.data(), total).has_value());
}

TEST(JoinGroupResponseCodec, WrongTemplate) {
    rpc::JoinGroupResponse msg;
    msg.generationId = 1;
    msg.isLeader     = 0;
    msg.errorCode    = 0;
    msg.memberId     = std::span<const uint8_t>{};

    const std::size_t total = msg.encodedSize();
    std::vector<uint8_t> buf(total, 0u);
    msg.encode(buf.data());
    buf[2] = 32;  // JoinGroup
    buf[3] = 0;

    EXPECT_FALSE(rpc::JoinGroupResponse::decode(buf.data(), total).has_value());
}

// ===========================================================================
// SyncGroup (templateId 34, BODY_FIXED = 4)
// ===========================================================================

TEST(SyncGroupCodec, RoundTrip) {
    rpc::SyncGroup msg;
    msg.sessionId     = kSession;
    msg.correlationId = kCorrelation;
    msg.groupId       = std::span<const uint8_t>(kGroupId, sizeof(kGroupId));
    msg.memberId      = std::span<const uint8_t>(kMemberId, sizeof(kMemberId));
    msg.generationId  = 7;

    const std::size_t total = msg.encodedSize();
    std::vector<uint8_t> buf(total, 0u);
    const std::size_t written = msg.encode(buf.data());
    ASSERT_EQ(written, total);

    auto decoded = rpc::SyncGroup::decode(buf.data(), written);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded->sessionId,     kSession);
    EXPECT_EQ(decoded->correlationId, kCorrelation);
    EXPECT_EQ(decoded->generationId,  7);
    ASSERT_EQ(decoded->groupId.size(), sizeof(kGroupId));
    EXPECT_EQ(std::memcmp(decoded->groupId.data(), kGroupId, sizeof(kGroupId)), 0);
    ASSERT_EQ(decoded->memberId.size(), sizeof(kMemberId));
    EXPECT_EQ(std::memcmp(decoded->memberId.data(), kMemberId, sizeof(kMemberId)), 0);
}

TEST(SyncGroupCodec, Truncation) {
    rpc::SyncGroup msg;
    msg.groupId      = std::span<const uint8_t>(kGroupId, sizeof(kGroupId));
    msg.memberId     = std::span<const uint8_t>(kMemberId, sizeof(kMemberId));
    msg.generationId = 1;

    const std::size_t total = msg.encodedSize();
    std::vector<uint8_t> buf(total, 0u);
    msg.encode(buf.data());

    EXPECT_FALSE(rpc::SyncGroup::decode(buf.data(), total - 1u).has_value());
    EXPECT_TRUE (rpc::SyncGroup::decode(buf.data(), total).has_value());
}

TEST(SyncGroupCodec, WrongTemplate) {
    rpc::SyncGroup msg;
    msg.groupId      = std::span<const uint8_t>(kGroupId, sizeof(kGroupId));
    msg.memberId     = std::span<const uint8_t>{};
    msg.generationId = 1;

    const std::size_t total = msg.encodedSize();
    std::vector<uint8_t> buf(total, 0u);
    msg.encode(buf.data());
    buf[2] = 35;  // SyncGroupResponse
    buf[3] = 0;

    EXPECT_FALSE(rpc::SyncGroup::decode(buf.data(), total).has_value());
}

// ===========================================================================
// SyncGroupResponse (templateId 35, BODY_FIXED = 2)
// ===========================================================================

TEST(SyncGroupResponseCodec, RoundTripWithAssignment) {
    rpc::SyncGroupResponse msg;
    msg.sessionId     = kSession;
    msg.correlationId = kCorrelation;
    msg.errorCode     = 0;
    msg.assignment    = { {10, 0}, {10, 1}, {20, 3} };

    const std::size_t total = msg.encodedSize();
    // 24 + 2 + 2 + 3*8 = 52
    EXPECT_EQ(total, 24u + 2u + 2u + 3u * 8u);

    std::vector<uint8_t> buf(total, 0u);
    const std::size_t written = msg.encode(buf.data());
    ASSERT_EQ(written, total);

    auto decoded = rpc::SyncGroupResponse::decode(buf.data(), written);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded->sessionId,         kSession);
    EXPECT_EQ(decoded->correlationId,     kCorrelation);
    EXPECT_EQ(decoded->errorCode,         0);
    ASSERT_EQ(decoded->assignment.size(), 3u);
    EXPECT_EQ(decoded->assignment[0].topicId,   10);
    EXPECT_EQ(decoded->assignment[0].partition, 0);
    EXPECT_EQ(decoded->assignment[1].topicId,   10);
    EXPECT_EQ(decoded->assignment[1].partition, 1);
    EXPECT_EQ(decoded->assignment[2].topicId,   20);
    EXPECT_EQ(decoded->assignment[2].partition, 3);
}

TEST(SyncGroupResponseCodec, EmptyAssignment) {
    rpc::SyncGroupResponse msg;
    msg.errorCode  = static_cast<int16_t>(rpc::ErrorCode::NotCoordinator);
    msg.assignment = {};

    const std::size_t total = msg.encodedSize();
    std::vector<uint8_t> buf(total, 0u);
    msg.encode(buf.data());

    auto decoded = rpc::SyncGroupResponse::decode(buf.data(), total);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded->errorCode,          static_cast<int16_t>(rpc::ErrorCode::NotCoordinator));
    EXPECT_EQ(decoded->assignment.size(),  0u);
}

TEST(SyncGroupResponseCodec, Truncation) {
    rpc::SyncGroupResponse msg;
    msg.errorCode  = 0;
    msg.assignment = { {1, 0}, {2, 1} };

    const std::size_t total = msg.encodedSize();
    std::vector<uint8_t> buf(total, 0u);
    msg.encode(buf.data());

    EXPECT_FALSE(rpc::SyncGroupResponse::decode(buf.data(), total - 1u).has_value());
    EXPECT_TRUE (rpc::SyncGroupResponse::decode(buf.data(), total).has_value());
}

TEST(SyncGroupResponseCodec, WrongTemplate) {
    rpc::SyncGroupResponse msg;
    msg.errorCode  = 0;
    msg.assignment = {};

    const std::size_t total = msg.encodedSize();
    std::vector<uint8_t> buf(total, 0u);
    msg.encode(buf.data());
    buf[2] = 34;  // SyncGroup
    buf[3] = 0;

    EXPECT_FALSE(rpc::SyncGroupResponse::decode(buf.data(), total).has_value());
}

// ===========================================================================
// Heartbeat (templateId 36, BODY_FIXED = 4)
// ===========================================================================

TEST(HeartbeatCodec, RoundTrip) {
    rpc::Heartbeat msg;
    msg.sessionId     = kSession;
    msg.correlationId = kCorrelation;
    msg.groupId       = std::span<const uint8_t>(kGroupId, sizeof(kGroupId));
    msg.memberId      = std::span<const uint8_t>(kMemberId, sizeof(kMemberId));
    msg.generationId  = 5;

    const std::size_t total = msg.encodedSize();
    std::vector<uint8_t> buf(total, 0u);
    const std::size_t written = msg.encode(buf.data());
    ASSERT_EQ(written, total);

    auto decoded = rpc::Heartbeat::decode(buf.data(), written);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded->sessionId,     kSession);
    EXPECT_EQ(decoded->correlationId, kCorrelation);
    EXPECT_EQ(decoded->generationId,  5);
    ASSERT_EQ(decoded->groupId.size(), sizeof(kGroupId));
    EXPECT_EQ(std::memcmp(decoded->groupId.data(), kGroupId, sizeof(kGroupId)), 0);
    ASSERT_EQ(decoded->memberId.size(), sizeof(kMemberId));
    EXPECT_EQ(std::memcmp(decoded->memberId.data(), kMemberId, sizeof(kMemberId)), 0);
}

TEST(HeartbeatCodec, Truncation) {
    rpc::Heartbeat msg;
    msg.groupId      = std::span<const uint8_t>(kGroupId, sizeof(kGroupId));
    msg.memberId     = std::span<const uint8_t>(kMemberId, sizeof(kMemberId));
    msg.generationId = 1;

    const std::size_t total = msg.encodedSize();
    std::vector<uint8_t> buf(total, 0u);
    msg.encode(buf.data());

    EXPECT_FALSE(rpc::Heartbeat::decode(buf.data(), total - 1u).has_value());
    EXPECT_TRUE (rpc::Heartbeat::decode(buf.data(), total).has_value());
}

TEST(HeartbeatCodec, WrongTemplate) {
    rpc::Heartbeat msg;
    msg.groupId      = std::span<const uint8_t>(kGroupId, sizeof(kGroupId));
    msg.memberId     = std::span<const uint8_t>{};
    msg.generationId = 1;

    const std::size_t total = msg.encodedSize();
    std::vector<uint8_t> buf(total, 0u);
    msg.encode(buf.data());
    buf[2] = 37;  // HeartbeatResponse
    buf[3] = 0;

    EXPECT_FALSE(rpc::Heartbeat::decode(buf.data(), total).has_value());
}

// ===========================================================================
// HeartbeatResponse (templateId 37, BODY_FIXED = 2, ENCODED_SIZE = 26)
// ===========================================================================

TEST(HeartbeatResponseCodec, RoundTripOk) {
    rpc::HeartbeatResponse msg;
    msg.sessionId     = kSession;
    msg.correlationId = kCorrelation;
    msg.errorCode     = 0;

    std::array<uint8_t, 32> buf{};
    const std::size_t n = msg.encode(buf.data());
    ASSERT_EQ(n, rpc::HeartbeatResponse::ENCODED_SIZE); // 26

    auto decoded = rpc::HeartbeatResponse::decode(buf.data(), n);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded->sessionId,     kSession);
    EXPECT_EQ(decoded->correlationId, kCorrelation);
    EXPECT_EQ(decoded->errorCode,     0);
}

TEST(HeartbeatResponseCodec, AllGroupErrorCodes) {
    auto roundTrip = [](rpc::ErrorCode ec) -> bool {
        rpc::HeartbeatResponse msg;
        msg.errorCode = static_cast<int16_t>(ec);
        std::array<uint8_t, 32> buf{};
        msg.encode(buf.data());
        auto d = rpc::HeartbeatResponse::decode(buf.data(), rpc::HeartbeatResponse::ENCODED_SIZE);
        return d.has_value() && d->errorCode == static_cast<int16_t>(ec);
    };

    EXPECT_TRUE(roundTrip(rpc::ErrorCode::StaleGeneration));
    EXPECT_TRUE(roundTrip(rpc::ErrorCode::UnknownMember));
    EXPECT_TRUE(roundTrip(rpc::ErrorCode::RebalanceInProgress));
    EXPECT_TRUE(roundTrip(rpc::ErrorCode::NotCoordinator));
    EXPECT_TRUE(roundTrip(rpc::ErrorCode::IllegalGeneration));
    EXPECT_TRUE(roundTrip(rpc::ErrorCode::GroupDead));
}

TEST(HeartbeatResponseCodec, Truncation) {
    rpc::HeartbeatResponse msg;
    msg.errorCode = 0;

    std::array<uint8_t, 32> buf{};
    msg.encode(buf.data());

    for (std::size_t a = 0; a < rpc::HeartbeatResponse::ENCODED_SIZE; ++a) {
        EXPECT_FALSE(rpc::HeartbeatResponse::decode(buf.data(), a).has_value())
            << "avail=" << a;
    }
    EXPECT_TRUE(rpc::HeartbeatResponse::decode(buf.data(), rpc::HeartbeatResponse::ENCODED_SIZE).has_value());
}

TEST(HeartbeatResponseCodec, WrongTemplate) {
    rpc::HeartbeatResponse msg;
    msg.errorCode = 0;
    std::array<uint8_t, 32> buf{};
    msg.encode(buf.data());
    buf[2] = 36;  // Heartbeat
    buf[3] = 0;
    EXPECT_FALSE(rpc::HeartbeatResponse::decode(buf.data(), rpc::HeartbeatResponse::ENCODED_SIZE).has_value());
}

// ===========================================================================
// LeaveGroup (templateId 38, BODY_FIXED = 0)
// ===========================================================================

TEST(LeaveGroupCodec, RoundTrip) {
    rpc::LeaveGroup msg;
    msg.sessionId     = kSession;
    msg.correlationId = kCorrelation;
    msg.groupId       = std::span<const uint8_t>(kGroupId, sizeof(kGroupId));
    msg.memberId      = std::span<const uint8_t>(kMemberId, sizeof(kMemberId));

    const std::size_t total = msg.encodedSize();
    std::vector<uint8_t> buf(total, 0u);
    const std::size_t written = msg.encode(buf.data());
    ASSERT_EQ(written, total);

    auto decoded = rpc::LeaveGroup::decode(buf.data(), written);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded->sessionId,     kSession);
    EXPECT_EQ(decoded->correlationId, kCorrelation);
    ASSERT_EQ(decoded->groupId.size(), sizeof(kGroupId));
    EXPECT_EQ(std::memcmp(decoded->groupId.data(), kGroupId, sizeof(kGroupId)), 0);
    ASSERT_EQ(decoded->memberId.size(), sizeof(kMemberId));
    EXPECT_EQ(std::memcmp(decoded->memberId.data(), kMemberId, sizeof(kMemberId)), 0);
}

TEST(LeaveGroupCodec, Truncation) {
    rpc::LeaveGroup msg;
    msg.groupId  = std::span<const uint8_t>(kGroupId, sizeof(kGroupId));
    msg.memberId = std::span<const uint8_t>(kMemberId, sizeof(kMemberId));

    const std::size_t total = msg.encodedSize();
    std::vector<uint8_t> buf(total, 0u);
    msg.encode(buf.data());

    EXPECT_FALSE(rpc::LeaveGroup::decode(buf.data(), total - 1u).has_value());
    EXPECT_TRUE (rpc::LeaveGroup::decode(buf.data(), total).has_value());
}

TEST(LeaveGroupCodec, WrongTemplate) {
    rpc::LeaveGroup msg;
    msg.groupId  = std::span<const uint8_t>(kGroupId, sizeof(kGroupId));
    msg.memberId = std::span<const uint8_t>{};

    const std::size_t total = msg.encodedSize();
    std::vector<uint8_t> buf(total, 0u);
    msg.encode(buf.data());
    buf[2] = 39;  // LeaveGroupResponse
    buf[3] = 0;

    EXPECT_FALSE(rpc::LeaveGroup::decode(buf.data(), total).has_value());
}

// ===========================================================================
// LeaveGroupResponse (templateId 39, BODY_FIXED = 2, ENCODED_SIZE = 26)
// ===========================================================================

TEST(LeaveGroupResponseCodec, RoundTrip) {
    rpc::LeaveGroupResponse msg;
    msg.sessionId     = kSession;
    msg.correlationId = kCorrelation;
    msg.errorCode     = static_cast<int16_t>(rpc::ErrorCode::OK);

    std::array<uint8_t, 32> buf{};
    const std::size_t n = msg.encode(buf.data());
    ASSERT_EQ(n, rpc::LeaveGroupResponse::ENCODED_SIZE); // 26

    auto decoded = rpc::LeaveGroupResponse::decode(buf.data(), n);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded->sessionId,     kSession);
    EXPECT_EQ(decoded->correlationId, kCorrelation);
    EXPECT_EQ(decoded->errorCode,     static_cast<int16_t>(rpc::ErrorCode::OK));
}

TEST(LeaveGroupResponseCodec, Truncation) {
    rpc::LeaveGroupResponse msg;
    msg.errorCode = 0;

    std::array<uint8_t, 32> buf{};
    msg.encode(buf.data());

    EXPECT_FALSE(rpc::LeaveGroupResponse::decode(buf.data(), rpc::LeaveGroupResponse::ENCODED_SIZE - 1u).has_value());
    EXPECT_TRUE (rpc::LeaveGroupResponse::decode(buf.data(), rpc::LeaveGroupResponse::ENCODED_SIZE).has_value());
}

TEST(LeaveGroupResponseCodec, WrongTemplate) {
    rpc::LeaveGroupResponse msg;
    msg.errorCode = 0;
    std::array<uint8_t, 32> buf{};
    msg.encode(buf.data());
    buf[2] = 38;  // LeaveGroup
    buf[3] = 0;
    EXPECT_FALSE(rpc::LeaveGroupResponse::decode(buf.data(), rpc::LeaveGroupResponse::ENCODED_SIZE).has_value());
}

// ===========================================================================
// Assign (templateId 44, BODY_FIXED = 4)
// ===========================================================================

TEST(AssignCodec, RoundTripWithAssignment) {
    rpc::Assign msg;
    msg.sessionId     = kSession;
    msg.correlationId = kCorrelation;
    msg.generationId  = 3;
    msg.assignment    = { {10, 0}, {10, 1} };

    const std::size_t total = msg.encodedSize();
    // 24 + 4 + 2 + 2*8 = 46
    EXPECT_EQ(total, 24u + 4u + 2u + 2u * 8u);

    std::vector<uint8_t> buf(total, 0u);
    const std::size_t written = msg.encode(buf.data());
    ASSERT_EQ(written, total);

    auto decoded = rpc::Assign::decode(buf.data(), written);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded->sessionId,         kSession);
    EXPECT_EQ(decoded->correlationId,     kCorrelation);
    EXPECT_EQ(decoded->generationId,      3);
    ASSERT_EQ(decoded->assignment.size(), 2u);
    EXPECT_EQ(decoded->assignment[0].topicId,   10);
    EXPECT_EQ(decoded->assignment[0].partition, 0);
    EXPECT_EQ(decoded->assignment[1].topicId,   10);
    EXPECT_EQ(decoded->assignment[1].partition, 1);
}

TEST(AssignCodec, EmptyAssignmentIsRevoke) {
    rpc::Assign msg;
    msg.sessionId     = kSession;
    msg.correlationId = kCorrelation;
    msg.generationId  = 7;
    msg.assignment    = {};  // empty = revoke all

    const std::size_t total = msg.encodedSize();
    // 24 + 4 + 2 + 0 = 30
    EXPECT_EQ(total, 30u);

    std::vector<uint8_t> buf(total, 0u);
    const std::size_t written = msg.encode(buf.data());
    ASSERT_EQ(written, total);

    auto decoded = rpc::Assign::decode(buf.data(), written);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded->generationId,      7);
    EXPECT_EQ(decoded->assignment.size(), 0u);
}

TEST(AssignCodec, Truncation) {
    rpc::Assign msg;
    msg.generationId = 1;
    msg.assignment   = { {5, 2}, {6, 3} };

    const std::size_t total = msg.encodedSize();
    std::vector<uint8_t> buf(total, 0u);
    msg.encode(buf.data());

    EXPECT_FALSE(rpc::Assign::decode(buf.data(), total - 1u).has_value());
    EXPECT_TRUE (rpc::Assign::decode(buf.data(), total).has_value());
}

TEST(AssignCodec, WrongTemplate) {
    rpc::Assign msg;
    msg.generationId = 1;
    msg.assignment   = {};

    const std::size_t total = msg.encodedSize();
    std::vector<uint8_t> buf(total, 0u);
    msg.encode(buf.data());
    buf[2] = 35;  // SyncGroupResponse
    buf[3] = 0;

    EXPECT_FALSE(rpc::Assign::decode(buf.data(), total).has_value());
}

TEST(AssignCodec, WrongSchema) {
    rpc::Assign msg;
    msg.generationId = 1;
    msg.assignment   = {};

    const std::size_t total = msg.encodedSize();
    std::vector<uint8_t> buf(total, 0u);
    msg.encode(buf.data());
    buf[4] = 0;
    buf[5] = 0;  // schemaId = 0

    EXPECT_FALSE(rpc::Assign::decode(buf.data(), total).has_value());
}

// ===========================================================================
// CG-1 Conformance vectors for group messages
// ===========================================================================

// Canonical golden values
static constexpr int64_t kGoldSession     = static_cast<int64_t>(0x1122334455667788ULL);
static constexpr int64_t kGoldCorrelation = static_cast<int64_t>(0x0A0B0C0D0E0F1011ULL);
static const uint8_t kGoldGroupId[]  = {'r', 'u', 'n', 'e', '-', 'g', 'r', 'p'};
static const uint8_t kGoldMemberId[] = {'m', '-', '0', '0', '1'};

// ---------------------------------------------------------------------------
// CV-GRP-1: JoinGroup golden
//   sessionTimeoutMs=30000, groupId="rune-grp", memberId="" (new), topics=[100,200]
//   Frame layout:
//     [0..23]  header: blockLen=4, tid=32, schema=120, ver=1, session, correlation
//     [24..27] sessionTimeoutMs = 30000 = 0x00007530 → 30 75 00 00 (LE)
//     [28..29] groupId len = 8 → 08 00
//     [30..37] "rune-grp"
//     [38..39] memberId len = 0 → 00 00
//     [40..41] topicCount = 2 → 02 00
//     [42..45] 100 = 0x64 → 64 00 00 00
//     [46..49] 200 = 0xC8 → C8 00 00 00
//   Total = 50 bytes
// ---------------------------------------------------------------------------
TEST(GroupConformance, JoinGroupGolden) {
    rpc::JoinGroup msg;
    msg.sessionId        = kGoldSession;
    msg.correlationId    = kGoldCorrelation;
    msg.sessionTimeoutMs = 30000;
    msg.groupId          = std::span<const uint8_t>(kGoldGroupId, sizeof(kGoldGroupId));
    msg.memberId         = std::span<const uint8_t>{};  // new member
    msg.subscribedTopicIds = {100, 200};

    const std::size_t total = msg.encodedSize();
    ASSERT_EQ(total, 50u);

    std::vector<uint8_t> buf(total, 0u);
    const std::size_t n = msg.encode(buf.data());
    ASSERT_EQ(n, 50u);

    // Header
    EXPECT_EQ(leU16(buf.data(),  0), 4u);    // blockLength = BODY_FIXED
    EXPECT_EQ(leU16(buf.data(),  2), 32u);   // templateId = JoinGroup
    EXPECT_EQ(leU16(buf.data(),  4), 120u);  // schemaId
    EXPECT_EQ(leU16(buf.data(),  6), 1u);    // version
    EXPECT_EQ(leS64(buf.data(),  8), kGoldSession);
    EXPECT_EQ(leS64(buf.data(), 16), kGoldCorrelation);

    // Fixed body
    EXPECT_EQ(leS32(buf.data(), 24), 30000);

    // groupId = "rune-grp"
    EXPECT_EQ(leU16(buf.data(), 28), 8u);
    EXPECT_EQ(std::memcmp(buf.data() + 30, kGoldGroupId, 8u), 0);

    // memberId = empty
    EXPECT_EQ(leU16(buf.data(), 38), 0u);

    // topics
    EXPECT_EQ(leU16(buf.data(), 40), 2u);
    EXPECT_EQ(leS32(buf.data(), 42), 100);
    EXPECT_EQ(leS32(buf.data(), 46), 200);

    // Decode-golden
    auto decoded = rpc::JoinGroup::decode(buf.data(), n);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded->sessionId,                   kGoldSession);
    EXPECT_EQ(decoded->correlationId,               kGoldCorrelation);
    EXPECT_EQ(decoded->sessionTimeoutMs,            30000);
    ASSERT_EQ(decoded->groupId.size(),              8u);
    EXPECT_EQ(std::memcmp(decoded->groupId.data(), kGoldGroupId, 8u), 0);
    EXPECT_EQ(decoded->memberId.size(),             0u);
    ASSERT_EQ(decoded->subscribedTopicIds.size(),   2u);
    EXPECT_EQ(decoded->subscribedTopicIds[0],       100);
    EXPECT_EQ(decoded->subscribedTopicIds[1],       200);
}

// ---------------------------------------------------------------------------
// CV-GRP-2: SyncGroupResponse golden with assignment
//   errorCode=0, assignment=[ (10,0), (10,1), (20,3) ]
//   Total = 24 + 2 + 2 + 3*8 = 52 bytes
// ---------------------------------------------------------------------------
TEST(GroupConformance, SyncGroupResponseGolden) {
    rpc::SyncGroupResponse msg;
    msg.sessionId     = kGoldSession;
    msg.correlationId = kGoldCorrelation;
    msg.errorCode     = 0;
    msg.assignment    = { {10, 0}, {10, 1}, {20, 3} };

    const std::size_t total = msg.encodedSize();
    ASSERT_EQ(total, 52u);

    std::vector<uint8_t> buf(total, 0u);
    const std::size_t n = msg.encode(buf.data());
    ASSERT_EQ(n, 52u);

    // Header
    EXPECT_EQ(leU16(buf.data(),  2), 35u);  // templateId = SyncGroupResponse
    EXPECT_EQ(leU16(buf.data(),  4), 120u); // schemaId

    // errorCode
    EXPECT_EQ(leS16(buf.data(), 24), 0);

    // assignment count = 3
    EXPECT_EQ(leU16(buf.data(), 26), 3u);
    EXPECT_EQ(leS32(buf.data(), 28), 10);  EXPECT_EQ(leS32(buf.data(), 32), 0);
    EXPECT_EQ(leS32(buf.data(), 36), 10);  EXPECT_EQ(leS32(buf.data(), 40), 1);
    EXPECT_EQ(leS32(buf.data(), 44), 20);  EXPECT_EQ(leS32(buf.data(), 48), 3);

    // Decode-golden
    auto decoded = rpc::SyncGroupResponse::decode(buf.data(), n);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded->sessionId,         kGoldSession);
    EXPECT_EQ(decoded->correlationId,     kGoldCorrelation);
    EXPECT_EQ(decoded->errorCode,         0);
    ASSERT_EQ(decoded->assignment.size(), 3u);
    EXPECT_EQ(decoded->assignment[0].topicId,   10);
    EXPECT_EQ(decoded->assignment[0].partition, 0);
    EXPECT_EQ(decoded->assignment[2].topicId,   20);
    EXPECT_EQ(decoded->assignment[2].partition, 3);
}

// ---------------------------------------------------------------------------
// CV-GRP-3: Assign empty = revoke golden
//   generationId=7, assignment=[] (revoke)
//   Total = 24 + 4 + 2 = 30 bytes
// ---------------------------------------------------------------------------
TEST(GroupConformance, AssignEmptyRevokeGolden) {
    rpc::Assign msg;
    msg.sessionId     = kGoldSession;
    msg.correlationId = kGoldCorrelation;
    msg.generationId  = 7;
    msg.assignment    = {};

    const std::size_t total = msg.encodedSize();
    ASSERT_EQ(total, 30u);

    std::vector<uint8_t> buf(total, 0u);
    const std::size_t n = msg.encode(buf.data());
    ASSERT_EQ(n, 30u);

    // Header
    EXPECT_EQ(leU16(buf.data(),  2), 44u);   // templateId = Assign
    EXPECT_EQ(leU16(buf.data(),  4), 120u);  // schemaId

    // generationId
    EXPECT_EQ(leS32(buf.data(), 24), 7);

    // assignment count = 0 (revoke)
    EXPECT_EQ(leU16(buf.data(), 28), 0u);

    // Decode-golden
    auto decoded = rpc::Assign::decode(buf.data(), n);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded->sessionId,         kGoldSession);
    EXPECT_EQ(decoded->correlationId,     kGoldCorrelation);
    EXPECT_EQ(decoded->generationId,      7);
    EXPECT_EQ(decoded->assignment.size(), 0u);
}
