// GroupMessagesTest.cpp — CG-1 tests for consumer-group codecs (templateIds 32–39, 44).
//
// Coverage per message:
//   (1) encode→decode round-trip asserting all fields (incl. arrays, var fields
//       with/without content, empty memberId, multi-element arrays)
//   (2) truncation: decode(buf, total-1) → nullopt
//   (3) wrong-template/schema: ASSERT_TRUE(decode(good_buf,total)) baseline first,
//       then assert !has_value() for the mutated buffer.
//
// NOTE: After CG-1 fix round, JoinGroup/SyncGroupResponse/Assign encode() takes
// the array as a second argument (std::span<const T>) — the struct fields for
// array data are zero-copy views on the decode side only.

#include <gtest/gtest.h>
#include <rune/protocol/client/GroupMessages.h>

#include <array>
#include <climits>
#include <cstdint>
#include <cstring>
#include <string>
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
    const std::span<const uint8_t> grp(kGroupId, sizeof(kGroupId));
    const std::span<const uint8_t> mbr(kMemberId, sizeof(kMemberId));
    const int32_t topics[] = {10, 20};
    const std::span<const int32_t> topicSpan(topics, 2);

    rpc::JoinGroup msg;
    msg.sessionId        = kSession;
    msg.correlationId    = kCorrelation;
    msg.sessionTimeoutMs = 30000;
    msg.groupId          = grp;
    msg.memberId         = mbr;

    // Total = 24+4 + 2+4 + 2+6 + 2+4+4 = 52
    const std::size_t expected = rpc::JoinGroup::encodedSizeFor(grp, mbr, 2u);
    ASSERT_EQ(expected, 24u + 4u + 2u + sizeof(kGroupId) + 2u + sizeof(kMemberId) + 2u + 8u);

    std::vector<uint8_t> buf(expected, 0u);
    const std::size_t written = msg.encode(buf.data(), topicSpan);
    ASSERT_EQ(written, expected);

    // Header checks
    EXPECT_EQ(leU16(buf.data(), 0), rpc::JoinGroup::BODY_FIXED);   // blockLength = 4
    EXPECT_EQ(leU16(buf.data(), 2), rpc::JoinGroup::TEMPLATE_ID);  // templateId = 32
    EXPECT_EQ(leU16(buf.data(), 4), rpc::kSchemaId);               // 120
    EXPECT_EQ(leU16(buf.data(), 6), rpc::kProtocolVersion);        // 2 (E5.1 bump)
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
    const std::span<const uint8_t> grp(kGroupId, sizeof(kGroupId));
    const std::span<const uint8_t> mbr{};
    const int32_t topics[] = {99};
    const std::span<const int32_t> topicSpan(topics, 1);

    rpc::JoinGroup msg;
    msg.sessionId        = kSession;
    msg.correlationId    = kCorrelation;
    msg.sessionTimeoutMs = 5000;
    msg.groupId          = grp;
    msg.memberId         = mbr;

    const std::size_t total = rpc::JoinGroup::encodedSizeFor(grp, mbr, 1u);
    std::vector<uint8_t> buf(total, 0u);
    const std::size_t written = msg.encode(buf.data(), topicSpan);
    ASSERT_EQ(written, total);

    auto decoded = rpc::JoinGroup::decode(buf.data(), written);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded->sessionTimeoutMs,              5000);
    EXPECT_EQ(decoded->memberId.size(),               0u);
    ASSERT_EQ(decoded->subscribedTopicIds.size(),     1u);
    EXPECT_EQ(decoded->subscribedTopicIds[0],         99);
}

// ---------------------------------------------------------------------------
// JG-3. Round-trip: empty subscribedTopicIds list.
// ---------------------------------------------------------------------------
TEST(JoinGroupCodec, EmptyTopics) {
    const std::span<const uint8_t> grp(kGroupId, sizeof(kGroupId));
    const std::span<const uint8_t> mbr{};
    const std::span<const int32_t> emptyTopics{};

    rpc::JoinGroup msg;
    msg.sessionTimeoutMs = 1000;
    msg.groupId          = grp;
    msg.memberId         = mbr;

    const std::size_t total = rpc::JoinGroup::encodedSizeFor(grp, mbr, 0u);
    std::vector<uint8_t> buf(total, 0u);
    msg.encode(buf.data(), emptyTopics);

    auto decoded = rpc::JoinGroup::decode(buf.data(), total);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded->subscribedTopicIds.size(), 0u);
}

// ---------------------------------------------------------------------------
// JG-4. Truncation: decode(buf, total-1) → nullopt.
// ---------------------------------------------------------------------------
TEST(JoinGroupCodec, Truncation) {
    const std::span<const uint8_t> grp(kGroupId, sizeof(kGroupId));
    const std::span<const uint8_t> mbr(kMemberId, sizeof(kMemberId));
    const int32_t topics[] = {1, 2, 3};
    const std::span<const int32_t> topicSpan(topics, 3);

    rpc::JoinGroup msg;
    msg.sessionTimeoutMs = 1000;
    msg.groupId          = grp;
    msg.memberId         = mbr;

    const std::size_t total = rpc::JoinGroup::encodedSizeFor(grp, mbr, 3u);
    std::vector<uint8_t> buf(total, 0u);
    msg.encode(buf.data(), topicSpan);

    ASSERT_TRUE(rpc::JoinGroup::decode(buf.data(), total).has_value());
    EXPECT_FALSE(rpc::JoinGroup::decode(buf.data(), total - 1u).has_value());
}

// ---------------------------------------------------------------------------
// JG-5. Wrong template → nullopt; wrong schema → nullopt.
// ---------------------------------------------------------------------------
TEST(JoinGroupCodec, WrongTemplateAndSchema) {
    const std::span<const uint8_t> grp(kGroupId, sizeof(kGroupId));
    const std::span<const uint8_t> mbr{};
    const std::span<const int32_t> emptyTopics{};

    rpc::JoinGroup msg;
    msg.sessionTimeoutMs = 1000;
    msg.groupId = grp;
    msg.memberId = mbr;

    const std::size_t total = rpc::JoinGroup::encodedSizeFor(grp, mbr, 0u);
    std::vector<uint8_t> buf(total, 0u);
    msg.encode(buf.data(), emptyTopics);

    // Baseline: valid decode must succeed before we mutate
    ASSERT_TRUE(rpc::JoinGroup::decode(buf.data(), total).has_value());

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

    ASSERT_TRUE(rpc::JoinGroupResponse::decode(buf.data(), total).has_value());
    EXPECT_FALSE(rpc::JoinGroupResponse::decode(buf.data(), total - 1u).has_value());
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
    // Baseline
    ASSERT_TRUE(rpc::JoinGroupResponse::decode(buf.data(), total).has_value());

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

    ASSERT_TRUE(rpc::SyncGroup::decode(buf.data(), total).has_value());
    EXPECT_FALSE(rpc::SyncGroup::decode(buf.data(), total - 1u).has_value());
}

TEST(SyncGroupCodec, WrongTemplate) {
    rpc::SyncGroup msg;
    msg.groupId      = std::span<const uint8_t>(kGroupId, sizeof(kGroupId));
    msg.memberId     = std::span<const uint8_t>{};
    msg.generationId = 1;

    const std::size_t total = msg.encodedSize();
    std::vector<uint8_t> buf(total, 0u);
    msg.encode(buf.data());
    ASSERT_TRUE(rpc::SyncGroup::decode(buf.data(), total).has_value());

    buf[2] = 35;  // SyncGroupResponse
    buf[3] = 0;
    EXPECT_FALSE(rpc::SyncGroup::decode(buf.data(), total).has_value());
}

// ===========================================================================
// SyncGroupResponse (templateId 35, BODY_FIXED = 2)
// ===========================================================================

TEST(SyncGroupResponseCodec, RoundTripWithAssignment) {
    const rpc::TopicPartition32 asgn[] = { {10, 0}, {10, 1}, {20, 3} };
    const std::span<const rpc::TopicPartition32> asgnSpan(asgn, 3);

    rpc::SyncGroupResponse msg;
    msg.sessionId     = kSession;
    msg.correlationId = kCorrelation;
    msg.errorCode     = 0;

    const std::size_t total = rpc::SyncGroupResponse::encodedSizeFor(3u);
    // 24 + 2 + 2 + 3*8 = 52
    EXPECT_EQ(total, 52u);

    std::vector<uint8_t> buf(total, 0u);
    const std::size_t written = msg.encode(buf.data(), asgnSpan);
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
    const std::span<const rpc::TopicPartition32> empty{};

    rpc::SyncGroupResponse msg;
    msg.errorCode = static_cast<int16_t>(rpc::ErrorCode::NotCoordinator);

    const std::size_t total = rpc::SyncGroupResponse::encodedSizeFor(0u);
    std::vector<uint8_t> buf(total, 0u);
    msg.encode(buf.data(), empty);

    auto decoded = rpc::SyncGroupResponse::decode(buf.data(), total);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded->errorCode,          static_cast<int16_t>(rpc::ErrorCode::NotCoordinator));
    EXPECT_EQ(decoded->assignment.size(),  0u);
}

TEST(SyncGroupResponseCodec, Truncation) {
    const rpc::TopicPartition32 asgn[] = { {1, 0}, {2, 1} };
    const std::span<const rpc::TopicPartition32> asgnSpan(asgn, 2);

    rpc::SyncGroupResponse msg;
    msg.errorCode  = 0;

    const std::size_t total = rpc::SyncGroupResponse::encodedSizeFor(2u);
    std::vector<uint8_t> buf(total, 0u);
    msg.encode(buf.data(), asgnSpan);

    ASSERT_TRUE(rpc::SyncGroupResponse::decode(buf.data(), total).has_value());
    EXPECT_FALSE(rpc::SyncGroupResponse::decode(buf.data(), total - 1u).has_value());
}

TEST(SyncGroupResponseCodec, WrongTemplate) {
    const std::span<const rpc::TopicPartition32> empty{};

    rpc::SyncGroupResponse msg;
    msg.errorCode  = 0;

    const std::size_t total = rpc::SyncGroupResponse::encodedSizeFor(0u);
    std::vector<uint8_t> buf(total, 0u);
    msg.encode(buf.data(), empty);
    ASSERT_TRUE(rpc::SyncGroupResponse::decode(buf.data(), total).has_value());

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

    ASSERT_TRUE(rpc::Heartbeat::decode(buf.data(), total).has_value());
    EXPECT_FALSE(rpc::Heartbeat::decode(buf.data(), total - 1u).has_value());
}

TEST(HeartbeatCodec, WrongTemplate) {
    rpc::Heartbeat msg;
    msg.groupId      = std::span<const uint8_t>(kGroupId, sizeof(kGroupId));
    msg.memberId     = std::span<const uint8_t>{};
    msg.generationId = 1;

    const std::size_t total = msg.encodedSize();
    std::vector<uint8_t> buf(total, 0u);
    msg.encode(buf.data());
    ASSERT_TRUE(rpc::Heartbeat::decode(buf.data(), total).has_value());

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
    ASSERT_TRUE(rpc::HeartbeatResponse::decode(buf.data(), rpc::HeartbeatResponse::ENCODED_SIZE).has_value());

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

    ASSERT_TRUE(rpc::LeaveGroup::decode(buf.data(), total).has_value());
    EXPECT_FALSE(rpc::LeaveGroup::decode(buf.data(), total - 1u).has_value());
}

TEST(LeaveGroupCodec, WrongTemplate) {
    rpc::LeaveGroup msg;
    msg.groupId  = std::span<const uint8_t>(kGroupId, sizeof(kGroupId));
    msg.memberId = std::span<const uint8_t>{};

    const std::size_t total = msg.encodedSize();
    std::vector<uint8_t> buf(total, 0u);
    msg.encode(buf.data());
    ASSERT_TRUE(rpc::LeaveGroup::decode(buf.data(), total).has_value());

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

    ASSERT_TRUE(rpc::LeaveGroupResponse::decode(buf.data(), rpc::LeaveGroupResponse::ENCODED_SIZE).has_value());
    EXPECT_FALSE(rpc::LeaveGroupResponse::decode(buf.data(), rpc::LeaveGroupResponse::ENCODED_SIZE - 1u).has_value());
}

TEST(LeaveGroupResponseCodec, WrongTemplate) {
    rpc::LeaveGroupResponse msg;
    msg.errorCode = 0;
    std::array<uint8_t, 32> buf{};
    msg.encode(buf.data());
    ASSERT_TRUE(rpc::LeaveGroupResponse::decode(buf.data(), rpc::LeaveGroupResponse::ENCODED_SIZE).has_value());

    buf[2] = 38;  // LeaveGroup
    buf[3] = 0;
    EXPECT_FALSE(rpc::LeaveGroupResponse::decode(buf.data(), rpc::LeaveGroupResponse::ENCODED_SIZE).has_value());
}

// ===========================================================================
// Assign (templateId 44, BODY_FIXED = 4)
// ===========================================================================

TEST(AssignCodec, RoundTripWithAssignment) {
    const rpc::TopicPartition32 asgn[] = { {10, 0}, {10, 1} };
    const std::span<const rpc::TopicPartition32> asgnSpan(asgn, 2);

    rpc::Assign msg;
    msg.sessionId     = kSession;
    msg.correlationId = kCorrelation;
    msg.generationId  = 3;

    const std::size_t total = rpc::Assign::encodedSizeFor(2u);
    // 24 + 4 + 2 + 2*8 = 46
    EXPECT_EQ(total, 46u);

    std::vector<uint8_t> buf(total, 0u);
    const std::size_t written = msg.encode(buf.data(), asgnSpan);
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
    const std::span<const rpc::TopicPartition32> empty{};

    rpc::Assign msg;
    msg.sessionId     = kSession;
    msg.correlationId = kCorrelation;
    msg.generationId  = 7;

    const std::size_t total = rpc::Assign::encodedSizeFor(0u);
    // 24 + 4 + 2 + 0 = 30
    EXPECT_EQ(total, 30u);

    std::vector<uint8_t> buf(total, 0u);
    const std::size_t written = msg.encode(buf.data(), empty);
    ASSERT_EQ(written, total);

    auto decoded = rpc::Assign::decode(buf.data(), written);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded->generationId,      7);
    EXPECT_EQ(decoded->assignment.size(), 0u);
}

TEST(AssignCodec, Truncation) {
    const rpc::TopicPartition32 asgn[] = { {5, 2}, {6, 3} };
    const std::span<const rpc::TopicPartition32> asgnSpan(asgn, 2);

    rpc::Assign msg;
    msg.generationId = 1;

    const std::size_t total = rpc::Assign::encodedSizeFor(2u);
    std::vector<uint8_t> buf(total, 0u);
    msg.encode(buf.data(), asgnSpan);

    ASSERT_TRUE(rpc::Assign::decode(buf.data(), total).has_value());
    EXPECT_FALSE(rpc::Assign::decode(buf.data(), total - 1u).has_value());
}

TEST(AssignCodec, WrongTemplate) {
    const std::span<const rpc::TopicPartition32> empty{};

    rpc::Assign msg;
    msg.generationId = 1;

    const std::size_t total = rpc::Assign::encodedSizeFor(0u);
    std::vector<uint8_t> buf(total, 0u);
    msg.encode(buf.data(), empty);
    ASSERT_TRUE(rpc::Assign::decode(buf.data(), total).has_value());

    buf[2] = 35;  // SyncGroupResponse
    buf[3] = 0;
    EXPECT_FALSE(rpc::Assign::decode(buf.data(), total).has_value());
}

TEST(AssignCodec, WrongSchema) {
    const std::span<const rpc::TopicPartition32> empty{};

    rpc::Assign msg;
    msg.generationId = 1;

    const std::size_t total = rpc::Assign::encodedSizeFor(0u);
    std::vector<uint8_t> buf(total, 0u);
    msg.encode(buf.data(), empty);
    ASSERT_TRUE(rpc::Assign::decode(buf.data(), total).has_value());

    buf[4] = 0;
    buf[5] = 0;  // schemaId = 0
    EXPECT_FALSE(rpc::Assign::decode(buf.data(), total).has_value());
}

// ===========================================================================
// CG-1 Conformance vectors for group messages
// (Self-referential but field-pinning tests; real file-backed golden tests
//  are in ConformanceTest.cpp via cg1_join_group.bin etc.)
// ===========================================================================

// Canonical golden values
static constexpr int64_t kGoldSession     = static_cast<int64_t>(0x1122334455667788ULL);
static constexpr int64_t kGoldCorrelation = static_cast<int64_t>(0x0A0B0C0D0E0F1011ULL);
static const uint8_t kGoldGroupId[]  = {'r', 'u', 'n', 'e', '-', 'g', 'r', 'p'};
static const uint8_t kGoldMemberId[] = {'m', '-', '0', '0', '1'};

// ---------------------------------------------------------------------------
// CV-GRP-1: JoinGroup golden (field-pinning only)
//   sessionTimeoutMs=30000, groupId="rune-grp", memberId="" (new), topics=[100,200]
//   Total = 50 bytes
// ---------------------------------------------------------------------------
TEST(GroupConformance, JoinGroupGolden) {
    const std::span<const uint8_t> grp(kGoldGroupId, sizeof(kGoldGroupId));
    const std::span<const uint8_t> mbr{};
    const int32_t topics[] = {100, 200};
    const std::span<const int32_t> topicSpan(topics, 2);

    rpc::JoinGroup msg;
    msg.sessionId        = kGoldSession;
    msg.correlationId    = kGoldCorrelation;
    msg.sessionTimeoutMs = 30000;
    msg.groupId          = grp;
    msg.memberId         = mbr;

    const std::size_t total = rpc::JoinGroup::encodedSizeFor(grp, mbr, 2u);
    ASSERT_EQ(total, 50u);

    std::vector<uint8_t> buf(total, 0u);
    const std::size_t n = msg.encode(buf.data(), topicSpan);
    ASSERT_EQ(n, 50u);

    // Header
    EXPECT_EQ(leU16(buf.data(),  0), 4u);    // blockLength = BODY_FIXED
    EXPECT_EQ(leU16(buf.data(),  2), 32u);   // templateId = JoinGroup
    EXPECT_EQ(leU16(buf.data(),  4), 120u);  // schemaId
    EXPECT_EQ(leU16(buf.data(),  6), rpc::kProtocolVersion);    // version (E5.1: now 2)
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
//   Total = 52 bytes
// ---------------------------------------------------------------------------
TEST(GroupConformance, SyncGroupResponseGolden) {
    const rpc::TopicPartition32 asgn[] = { {10, 0}, {10, 1}, {20, 3} };
    const std::span<const rpc::TopicPartition32> asgnSpan(asgn, 3);

    rpc::SyncGroupResponse msg;
    msg.sessionId     = kGoldSession;
    msg.correlationId = kGoldCorrelation;
    msg.errorCode     = 0;

    const std::size_t total = rpc::SyncGroupResponse::encodedSizeFor(3u);
    ASSERT_EQ(total, 52u);

    std::vector<uint8_t> buf(total, 0u);
    const std::size_t n = msg.encode(buf.data(), asgnSpan);
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
//   Total = 30 bytes
// ---------------------------------------------------------------------------
TEST(GroupConformance, AssignEmptyRevokeGolden) {
    const std::span<const rpc::TopicPartition32> empty{};

    rpc::Assign msg;
    msg.sessionId     = kGoldSession;
    msg.correlationId = kGoldCorrelation;
    msg.generationId  = 7;

    const std::size_t total = rpc::Assign::encodedSizeFor(0u);
    ASSERT_EQ(total, 30u);

    std::vector<uint8_t> buf(total, 0u);
    const std::size_t n = msg.encode(buf.data(), empty);
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

// ===========================================================================
// Phase 2c (client v10): JoinGroup filter trailer, JoinGroupResponse detail
// ===========================================================================
namespace {
rpc::JoinGroup fixedJoin(std::span<const uint8_t> grp, std::span<const uint8_t> mbr) {
    rpc::JoinGroup msg;
    msg.sessionId = 11; msg.correlationId = 12; msg.sessionTimeoutMs = 30000;
    msg.groupId = grp; msg.memberId = mbr;
    return msg;
}
const std::array<uint8_t, 3> kGrp{'g', 'r', 'p'};
const std::array<uint8_t, 2> kMbr{'m', '1'};
const std::array<int32_t, 2> kTopics{7, 9};
} // namespace

TEST(JoinGroupFilterRegression, UnfilteredJoinIsByteIdenticalRegression) {
    rpc::JoinGroup msg = fixedJoin(kGrp, kMbr);
    const std::size_t v9Size = 24 + 4 + (2 + 3) + (2 + 2) + (2 + 8);   // = 47
    ASSERT_EQ(rpc::JoinGroup::encodedSizeFor(kGrp, kMbr, 2), v9Size);
    std::array<uint8_t, 128> buf{};
    ASSERT_EQ(msg.encode(buf.data(), kTopics), v9Size);
    auto d = rpc::JoinGroup::decode(buf.data(), v9Size);
    ASSERT_TRUE(d.has_value());
    EXPECT_TRUE(d->filter.empty());
    EXPECT_EQ(d->subscribedTopicIds.size(), 2u);
}

TEST(JoinGroupFilterRegression, FilterRoundTripsRegression) {
    rpc::JoinGroup msg = fixedJoin(kGrp, kMbr);
    const std::array<uint8_t, 11> f{1, 1, 5, 0, 4, 1, 1, 0xAA, 0xBB, 0xCC, 0xDD};
    msg.filter = f;
    const std::size_t n = 47u + 2u + 11u;
    ASSERT_EQ(rpc::JoinGroup::encodedSizeFor(kGrp, kMbr, 2, f.size()), n);
    std::array<uint8_t, 128> buf{};
    ASSERT_EQ(msg.encode(buf.data(), kTopics), n);
    auto d = rpc::JoinGroup::decode(buf.data(), n);
    ASSERT_TRUE(d.has_value());
    ASSERT_EQ(d->filter.size(), 11u);
    EXPECT_EQ(std::memcmp(d->filter.data(), f.data(), 11), 0);
    EXPECT_EQ(d->subscribedTopicIds.size(), 2u);
    // Pre-v10 session: trailer ignored, everything else intact.
    auto d9 = rpc::JoinGroup::decode(buf.data(), n, rpc::kClientVersionAuth);
    ASSERT_TRUE(d9.has_value());
    EXPECT_TRUE(d9->filter.empty());
    EXPECT_EQ(d9->subscribedTopicIds.size(), 2u);
    EXPECT_TRUE(rpc::JoinGroup::decode(buf.data(), 48, rpc::kClientVersionAuth).has_value());
}

TEST(JoinGroupFilterRegression, TruncatedTrailerIsUndecodableRegression) {
    rpc::JoinGroup msg = fixedJoin(kGrp, kMbr);
    const std::array<uint8_t, 11> f{};
    msg.filter = f;
    std::array<uint8_t, 128> buf{};
    const std::size_t n = msg.encode(buf.data(), kTopics);
    for (std::size_t cut = 48; cut < n; ++cut) {   // 48 = half a length prefix
        EXPECT_FALSE(rpc::JoinGroup::decode(buf.data(), cut).has_value()) << "cut " << cut;
    }
    EXPECT_TRUE(rpc::JoinGroup::decode(buf.data(), 47).has_value());
}

TEST(JoinGroupFilterRegression, MaxFilterIsAcceptedRegression) {
    rpc::JoinGroup msg = fixedJoin(kGrp, kMbr);
    std::vector<uint8_t> f(8234, 0x5A);
    msg.filter = f;
    std::vector<uint8_t> buf(rpc::JoinGroup::encodedSizeFor(kGrp, kMbr, 2, f.size()));
    ASSERT_EQ(msg.encode(buf.data(), kTopics), buf.size());
    auto d = rpc::JoinGroup::decode(buf.data(), buf.size());
    ASSERT_TRUE(d.has_value());
    EXPECT_EQ(d->filter.size(), 8234u);
}

TEST(JoinGroupResponseDetailRegression, DetailRoundTripsRegression) {
    rpc::JoinGroupResponse r;
    r.sessionId = 1; r.correlationId = 2; r.generationId = 3; r.isLeader = 0; r.errorCode = 39;
    r.memberId = kMbr;
    r.detail = "group filter mismatch: the group uses filter 0101";
    std::array<uint8_t, 128> buf{};
    ASSERT_EQ(r.encodedSize(), 24u + 7u + 2u + 2u);
    ASSERT_EQ(r.encodedSizeWithDetail(), r.encodedSize() + 2u + r.detail.size());
    const std::size_t n = r.encode(buf.data(), rpc::kClientVersionFilter);
    ASSERT_EQ(n, r.encodedSizeWithDetail());
    auto d = rpc::JoinGroupResponse::decode(buf.data(), n);
    ASSERT_TRUE(d.has_value());
    EXPECT_EQ(d->detail, r.detail);
    EXPECT_EQ(d->memberId.size(), 2u);
    EXPECT_EQ(d->errorCode, 39);
    // prefix decodes alone; truncated trailer fails cleanly
    auto dp = rpc::JoinGroupResponse::decode(buf.data(), r.encodedSize());
    ASSERT_TRUE(dp.has_value());
    EXPECT_TRUE(dp->detail.empty());
    EXPECT_FALSE(rpc::JoinGroupResponse::decode(buf.data(), r.encodedSize() + 1).has_value());
    EXPECT_FALSE(rpc::JoinGroupResponse::decode(buf.data(), n - 1).has_value());
}

TEST(JoinGroupResponseDetailRegression, DetailOnlyForV10AndEmptyOmittedAndMaxRoundTripsRegression) {
    rpc::JoinGroupResponse r;
    r.errorCode = 39; r.memberId = kMbr; r.detail = "x";
    std::array<uint8_t, 128> buf{};
    EXPECT_EQ(r.encode(buf.data(), rpc::kClientVersionAuth), r.encodedSize());   // old client: no trailer
    EXPECT_GT(r.encode(buf.data(), rpc::kClientVersionFilter), r.encodedSize());
    r.detail = {};
    EXPECT_EQ(r.encodedSizeWithDetail(), r.encodedSize());
    EXPECT_EQ(r.encode(buf.data(), rpc::kClientVersionFilter), r.encodedSize());

    const std::string big(65535, 'z');
    r.detail = big;
    std::vector<uint8_t> big_buf(r.encodedSizeWithDetail());
    ASSERT_EQ(r.encode(big_buf.data()), big_buf.size());
    auto d = rpc::JoinGroupResponse::decode(big_buf.data(), big_buf.size());
    ASSERT_TRUE(d.has_value());
    EXPECT_EQ(d->detail.size(), 65535u);
}

TEST(JoinGroupFilterRegression, MismatchPrefixIsExactRegression) {
    EXPECT_EQ(rpc::kGroupFilterMismatchPrefix, std::string_view("group filter mismatch"));
}
