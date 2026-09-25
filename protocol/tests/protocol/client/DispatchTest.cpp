// tests/protocol/client/DispatchTest.cpp
//
// Exhaustive tests for rune::protocol::client::checkFrame / peekTemplateId.
//
// Coverage:
//   - Ok for all 28 known template ids: MA control 1..7, MB produce/subscribe 16..21,
//     MC consumer-groups 32..44
//   - TooShort: avail = 0, 1, 23 (no over-read); exactly 24 is NOT TooShort
//   - WrongSchema: schemaId = 0, 111, 119, 121, 65535
//   - UnsupportedVersion: version = 2, 3, 65535 (> 1 rejected);
//                         version = 1 (equal) and version = 0 (older) accepted;
//                         data template (tid=16) at bad version → UnsupportedVersion
//   - UnknownTemplate: templateId = 0, 8, 15, 22, 45, 64, 99, 65535
//     (8 and 15 = control-range gap; 22 = first unassigned produce/consume gap;
//      45 = first unassigned groups/txn gap after Assign=44)
//   - Ordering precedence: TooShort wins over WrongSchema; WrongSchema wins over
//                          UnsupportedVersion
//   - peekTemplateId: returns nullopt for every non-Ok case; returns id for Ok
//   - No over-read: exact-size (24-byte) buffers used throughout

#include <gtest/gtest.h>

#include <rune/protocol/client/ControlMessages.h>
#include <rune/protocol/client/DataMessages.h>
#include <rune/protocol/client/Dispatch.h>
#include <rune/protocol/client/GroupMessages.h>
#include <rune/protocol/client/MessageHeader.h>
#include <rune/protocol/client/OffsetMessages.h>
#include <rune/protocol/client/PingMessages.h>
#include <rune/protocol/client/Protocol.h>

#include <array>
#include <cstdint>
#include <cstring>

namespace rpc = rune::protocol::client;

// ===========================================================================
// Helpers
// ===========================================================================

// Build a minimal 24-byte header buffer with specified field values.
static std::array<uint8_t, 24> makeHeaderBuf(uint16_t templateId,
                                              uint16_t schemaId,
                                              uint16_t version,
                                              int64_t  sessionId     = 0,
                                              int64_t  correlationId = 0) noexcept {
    rpc::MessageHeader hdr;
    hdr.blockLength   = 0;
    hdr.templateId    = templateId;
    hdr.schemaId      = schemaId;
    hdr.version       = version;
    hdr.sessionId     = sessionId;
    hdr.correlationId = correlationId;

    std::array<uint8_t, 24> buf{};
    hdr.encode(buf.data());
    return buf;
}

// Shorthand: build a valid header (correct schema + version) with given templateId.
static std::array<uint8_t, 24> makeValidHeaderBuf(uint16_t templateId,
                                                   uint16_t version = rpc::kProtocolVersion) noexcept {
    return makeHeaderBuf(templateId, rpc::kSchemaId, version);
}

// ===========================================================================
// 1. Ok — all 15 known template ids (MA control 1..7, MB produce/subscribe 16..21)
//
// For each template id we build a minimal valid encoded frame (or a plain 24-byte
// header for the header-only messages) and assert Ok + correct templateId.
// ===========================================================================

TEST(DispatchCheckFrame, OkConnect) {
    rpc::ConnectRequest req;
    req.sessionId      = 0x1122334455667788LL;
    req.correlationId  = 0x0A0B0C0D0E0F1011LL;
    req.egressStreamId = 42;
    req.clientVersion  = 1;
    req.egressChannel  = "aeron:udp?endpoint=127.0.0.1:40000";

    std::array<uint8_t, 128> buf{};
    const std::size_t len = req.encode(buf.data());
    ASSERT_GE(len, rpc::MessageHeader::ENCODED_SIZE);

    const auto fc = rpc::checkFrame(buf.data(), len);
    EXPECT_EQ(fc.status,     rpc::FrameStatus::Ok);
    EXPECT_EQ(fc.templateId, static_cast<uint16_t>(rpc::TemplateId::Connect));  // 1
    EXPECT_EQ(fc.version,    rpc::kProtocolVersion);

    EXPECT_EQ(rpc::peekTemplateId(buf.data(), len),
              std::optional<uint16_t>(static_cast<uint16_t>(rpc::TemplateId::Connect)));
}

TEST(DispatchCheckFrame, OkConnectResponse) {
    rpc::ConnectResponse resp;
    resp.sessionId         = 0x1122334455667788LL;
    resp.correlationId     = 0x0A0B0C0D0E0F1011LL;
    resp.code              = 0;
    resp.leaderMemberId    = 1;
    resp.assignedSessionId = 0;

    std::array<uint8_t, 64> buf{};
    const std::size_t len = resp.encode(buf.data());

    const auto fc = rpc::checkFrame(buf.data(), len);
    EXPECT_EQ(fc.status,     rpc::FrameStatus::Ok);
    EXPECT_EQ(fc.templateId, static_cast<uint16_t>(rpc::TemplateId::ConnectResponse));  // 2

    EXPECT_EQ(rpc::peekTemplateId(buf.data(), len),
              std::optional<uint16_t>(static_cast<uint16_t>(rpc::TemplateId::ConnectResponse)));
}

TEST(DispatchCheckFrame, OkKeepalive) {
    rpc::Keepalive ka;
    ka.sessionId     = 0xABCDEF0123456789LL;
    ka.correlationId = 0x0102030405060708LL;

    std::array<uint8_t, 32> buf{};
    const std::size_t len = ka.encode(buf.data());

    const auto fc = rpc::checkFrame(buf.data(), len);
    EXPECT_EQ(fc.status,     rpc::FrameStatus::Ok);
    EXPECT_EQ(fc.templateId, static_cast<uint16_t>(rpc::TemplateId::Keepalive));  // 3

    EXPECT_EQ(rpc::peekTemplateId(buf.data(), len),
              std::optional<uint16_t>(static_cast<uint16_t>(rpc::TemplateId::Keepalive)));
}

TEST(DispatchCheckFrame, OkDisconnect) {
    rpc::Disconnect dis;
    dis.sessionId     = 0xDEADBEEFCAFEBABELL;
    dis.correlationId = 0x1122334455667788LL;
    dis.reason        = -1;

    std::array<uint8_t, 32> buf{};
    const std::size_t len = dis.encode(buf.data());

    const auto fc = rpc::checkFrame(buf.data(), len);
    EXPECT_EQ(fc.status,     rpc::FrameStatus::Ok);
    EXPECT_EQ(fc.templateId, static_cast<uint16_t>(rpc::TemplateId::Disconnect));  // 4

    EXPECT_EQ(rpc::peekTemplateId(buf.data(), len),
              std::optional<uint16_t>(static_cast<uint16_t>(rpc::TemplateId::Disconnect)));
}

TEST(DispatchCheckFrame, OkErrorResponse) {
    rpc::ErrorResponse err;
    err.sessionId     = 0x0102030405060708LL;
    err.correlationId = 0x0A0B0C0D0E0F1011LL;
    err.code          = static_cast<int16_t>(rpc::ErrorCode::SessionExpired);
    err.message       = "test error";

    std::array<uint8_t, 128> buf{};
    const std::size_t len = err.encode(buf.data());

    const auto fc = rpc::checkFrame(buf.data(), len);
    EXPECT_EQ(fc.status,     rpc::FrameStatus::Ok);
    EXPECT_EQ(fc.templateId, static_cast<uint16_t>(rpc::TemplateId::ErrorResponse));  // 5

    EXPECT_EQ(rpc::peekTemplateId(buf.data(), len),
              std::optional<uint16_t>(static_cast<uint16_t>(rpc::TemplateId::ErrorResponse)));
}

TEST(DispatchCheckFrame, OkPing) {
    rpc::Ping ping;
    ping.sessionId     = 0xDEADBEEFCAFEBABELL;
    ping.correlationId = 0x0102030405060708LL;
    ping.echo          = "pong-echo";

    std::array<uint8_t, 64> buf{};
    const std::size_t len = ping.encode(buf.data());

    const auto fc = rpc::checkFrame(buf.data(), len);
    EXPECT_EQ(fc.status,     rpc::FrameStatus::Ok);
    EXPECT_EQ(fc.templateId, static_cast<uint16_t>(rpc::TemplateId::Ping));  // 6

    EXPECT_EQ(rpc::peekTemplateId(buf.data(), len),
              std::optional<uint16_t>(static_cast<uint16_t>(rpc::TemplateId::Ping)));
}

TEST(DispatchCheckFrame, OkPong) {
    rpc::Pong pong;
    pong.sessionId     = 0xDEADBEEFCAFEBABELL;
    pong.correlationId = 0x0102030405060708LL;
    pong.echo          = "pong-echo";

    std::array<uint8_t, 64> buf{};
    const std::size_t len = pong.encode(buf.data());

    const auto fc = rpc::checkFrame(buf.data(), len);
    EXPECT_EQ(fc.status,     rpc::FrameStatus::Ok);
    EXPECT_EQ(fc.templateId, static_cast<uint16_t>(rpc::TemplateId::Pong));  // 7

    EXPECT_EQ(rpc::peekTemplateId(buf.data(), len),
              std::optional<uint16_t>(static_cast<uint16_t>(rpc::TemplateId::Pong)));
}

// PB-1: Produce (tid 16) and ProduceResponse (tid 17) are now known.
TEST(DispatchCheckFrame, OkProduce) {
    static constexpr uint8_t kRb[] = {0xAA, 0xBB};
    rpc::Produce prod;
    prod.sessionId      = 0x1122334455667788LL;
    prod.correlationId  = 0x0A0B0C0D0E0F1011LL;
    prod.topicId        = 1;
    prod.partitionKnown = 1;
    prod.key            = std::span<const uint8_t>{};
    prod.recordBatch    = std::span<const uint8_t>(kRb, sizeof(kRb));

    std::array<uint8_t, 64> buf{};
    const std::size_t len = prod.encode(buf.data());

    const auto fc = rpc::checkFrame(buf.data(), len);
    EXPECT_EQ(fc.status,     rpc::FrameStatus::Ok);
    EXPECT_EQ(fc.templateId, static_cast<uint16_t>(rpc::TemplateId::Produce));  // 16

    EXPECT_EQ(rpc::peekTemplateId(buf.data(), len),
              std::optional<uint16_t>(static_cast<uint16_t>(rpc::TemplateId::Produce)));
}

TEST(DispatchCheckFrame, OkProduceResponse) {
    rpc::ProduceResponse resp;
    resp.sessionId     = 0x1122334455667788LL;
    resp.correlationId = 0x0A0B0C0D0E0F1011LL;
    resp.errorCode     = 0;
    resp.partition     = 3;
    resp.baseOffset    = 999LL;

    std::array<uint8_t, 64> buf{};
    const std::size_t len = resp.encode(buf.data());

    const auto fc = rpc::checkFrame(buf.data(), len);
    EXPECT_EQ(fc.status,     rpc::FrameStatus::Ok);
    EXPECT_EQ(fc.templateId, static_cast<uint16_t>(rpc::TemplateId::ProduceResponse));  // 17

    EXPECT_EQ(rpc::peekTemplateId(buf.data(), len),
              std::optional<uint16_t>(static_cast<uint16_t>(rpc::TemplateId::ProduceResponse)));
}

// PB-3: Subscribe (tid 18), SubscribeResponse (tid 19), Records (tid 20), Unsubscribe (tid 21).
TEST(DispatchCheckFrame, OkSubscribe) {
    rpc::Subscribe sub;
    sub.sessionId      = 0x1122334455667788LL;
    sub.correlationId  = 0x0A0B0C0D0E0F1011LL;
    sub.topicId        = 3001;
    sub.partition      = 5;
    sub.partitionKnown = 1;
    sub.fromOffset     = 9999LL;
    sub.isolation      = 1;
    sub.key            = std::span<const uint8_t>{};

    std::array<uint8_t, 64> buf{};
    const std::size_t len = sub.encode(buf.data());

    const auto fc = rpc::checkFrame(buf.data(), len);
    EXPECT_EQ(fc.status,     rpc::FrameStatus::Ok);
    EXPECT_EQ(fc.templateId, static_cast<uint16_t>(rpc::TemplateId::Subscribe));  // 18

    EXPECT_EQ(rpc::peekTemplateId(buf.data(), len),
              std::optional<uint16_t>(static_cast<uint16_t>(rpc::TemplateId::Subscribe)));
}

TEST(DispatchCheckFrame, OkSubscribeResponse) {
    rpc::SubscribeResponse sresp;
    sresp.sessionId      = 0x1122334455667788LL;
    sresp.correlationId  = 0x0A0B0C0D0E0F1011LL;
    sresp.errorCode      = 0;
    sresp.partition      = 5;
    sresp.subscriptionId = 0x0102030405060708LL;

    std::array<uint8_t, 64> buf{};
    const std::size_t len = sresp.encode(buf.data());

    const auto fc = rpc::checkFrame(buf.data(), len);
    EXPECT_EQ(fc.status,     rpc::FrameStatus::Ok);
    EXPECT_EQ(fc.templateId, static_cast<uint16_t>(rpc::TemplateId::SubscribeResponse));  // 19

    EXPECT_EQ(rpc::peekTemplateId(buf.data(), len),
              std::optional<uint16_t>(static_cast<uint16_t>(rpc::TemplateId::SubscribeResponse)));
}

TEST(DispatchCheckFrame, OkRecords) {
    static constexpr uint8_t kRb[] = {0xDE, 0xAD, 0xBE, 0xEF};
    rpc::Records rec;
    rec.sessionId      = 0x1122334455667788LL;
    rec.correlationId  = 0x0A0B0C0D0E0F1011LL;
    rec.subscriptionId = 0x0102030405060708LL;
    rec.topicId        = 3001;
    rec.partition      = 5;
    rec.baseOffset     = 100000LL;
    rec.recordBatch    = std::span<const uint8_t>(kRb, sizeof(kRb));

    std::array<uint8_t, 128> buf{};
    const std::size_t len = rec.encode(buf.data());

    const auto fc = rpc::checkFrame(buf.data(), len);
    EXPECT_EQ(fc.status,     rpc::FrameStatus::Ok);
    EXPECT_EQ(fc.templateId, static_cast<uint16_t>(rpc::TemplateId::Records));  // 20

    EXPECT_EQ(rpc::peekTemplateId(buf.data(), len),
              std::optional<uint16_t>(static_cast<uint16_t>(rpc::TemplateId::Records)));
}

TEST(DispatchCheckFrame, OkUnsubscribe) {
    rpc::Unsubscribe unsub;
    unsub.sessionId      = 0x1122334455667788LL;
    unsub.correlationId  = 0x0A0B0C0D0E0F1011LL;
    unsub.subscriptionId = 0x0102030405060708LL;

    std::array<uint8_t, 64> buf{};
    const std::size_t len = unsub.encode(buf.data());

    const auto fc = rpc::checkFrame(buf.data(), len);
    EXPECT_EQ(fc.status,     rpc::FrameStatus::Ok);
    EXPECT_EQ(fc.templateId, static_cast<uint16_t>(rpc::TemplateId::Unsubscribe));  // 21

    EXPECT_EQ(rpc::peekTemplateId(buf.data(), len),
              std::optional<uint16_t>(static_cast<uint16_t>(rpc::TemplateId::Unsubscribe)));
}

// CG-1: Consumer-group range 32..44 — all must be known.
// We test each by building a minimal valid header (24 bytes) with the correct
// templateId and verifying checkFrame returns Ok (not UnknownTemplate).
TEST(DispatchCheckFrame, OkJoinGroup) {
    const uint8_t kGrp[] = {'g'};
    rpc::JoinGroup msg;
    msg.sessionId        = 0x1122334455667788LL;
    msg.correlationId    = 0x0A0B0C0D0E0F1011LL;
    msg.sessionTimeoutMs = 30000;
    msg.groupId          = std::span<const uint8_t>(kGrp, 1u);
    msg.memberId         = std::span<const uint8_t>{};

    std::array<uint8_t, 64> buf{};
    const std::span<const int32_t> noTopics{};
    const std::size_t len = msg.encode(buf.data(), noTopics);

    const auto fc = rpc::checkFrame(buf.data(), len);
    EXPECT_EQ(fc.status,     rpc::FrameStatus::Ok);
    EXPECT_EQ(fc.templateId, static_cast<uint16_t>(rpc::TemplateId::JoinGroup));  // 32
}

TEST(DispatchCheckFrame, OkJoinGroupResponse) {
    rpc::JoinGroupResponse msg;
    msg.generationId = 1;
    msg.isLeader     = 0;
    msg.errorCode    = 0;
    msg.memberId     = std::span<const uint8_t>{};

    std::array<uint8_t, 64> buf{};
    const std::size_t len = msg.encode(buf.data());

    const auto fc = rpc::checkFrame(buf.data(), len);
    EXPECT_EQ(fc.status,     rpc::FrameStatus::Ok);
    EXPECT_EQ(fc.templateId, static_cast<uint16_t>(rpc::TemplateId::JoinGroupResponse));  // 33
}

TEST(DispatchCheckFrame, OkSyncGroup) {
    const uint8_t kGrp[] = {'g'};
    rpc::SyncGroup msg;
    msg.groupId      = std::span<const uint8_t>(kGrp, 1u);
    msg.memberId     = std::span<const uint8_t>{};
    msg.generationId = 1;

    std::array<uint8_t, 64> buf{};
    const std::size_t len = msg.encode(buf.data());

    const auto fc = rpc::checkFrame(buf.data(), len);
    EXPECT_EQ(fc.status,     rpc::FrameStatus::Ok);
    EXPECT_EQ(fc.templateId, static_cast<uint16_t>(rpc::TemplateId::SyncGroup));  // 34
}

TEST(DispatchCheckFrame, OkSyncGroupResponse) {
    rpc::SyncGroupResponse msg;
    msg.errorCode  = 0;

    std::array<uint8_t, 64> buf{};
    const std::span<const rpc::TopicPartition32> noAsgn{};
    const std::size_t len = msg.encode(buf.data(), noAsgn);

    const auto fc = rpc::checkFrame(buf.data(), len);
    EXPECT_EQ(fc.status,     rpc::FrameStatus::Ok);
    EXPECT_EQ(fc.templateId, static_cast<uint16_t>(rpc::TemplateId::SyncGroupResponse));  // 35
}

TEST(DispatchCheckFrame, OkHeartbeat) {
    const uint8_t kGrp[] = {'g'};
    rpc::Heartbeat msg;
    msg.groupId      = std::span<const uint8_t>(kGrp, 1u);
    msg.memberId     = std::span<const uint8_t>{};
    msg.generationId = 1;

    std::array<uint8_t, 64> buf{};
    const std::size_t len = msg.encode(buf.data());

    const auto fc = rpc::checkFrame(buf.data(), len);
    EXPECT_EQ(fc.status,     rpc::FrameStatus::Ok);
    EXPECT_EQ(fc.templateId, static_cast<uint16_t>(rpc::TemplateId::Heartbeat));  // 36
}

TEST(DispatchCheckFrame, OkHeartbeatResponse) {
    rpc::HeartbeatResponse msg;
    msg.errorCode = 0;

    std::array<uint8_t, 32> buf{};
    const std::size_t len = msg.encode(buf.data());

    const auto fc = rpc::checkFrame(buf.data(), len);
    EXPECT_EQ(fc.status,     rpc::FrameStatus::Ok);
    EXPECT_EQ(fc.templateId, static_cast<uint16_t>(rpc::TemplateId::HeartbeatResponse));  // 37
}

TEST(DispatchCheckFrame, OkLeaveGroup) {
    const uint8_t kGrp[] = {'g'};
    rpc::LeaveGroup msg;
    msg.groupId  = std::span<const uint8_t>(kGrp, 1u);
    msg.memberId = std::span<const uint8_t>{};

    std::array<uint8_t, 64> buf{};
    const std::size_t len = msg.encode(buf.data());

    const auto fc = rpc::checkFrame(buf.data(), len);
    EXPECT_EQ(fc.status,     rpc::FrameStatus::Ok);
    EXPECT_EQ(fc.templateId, static_cast<uint16_t>(rpc::TemplateId::LeaveGroup));  // 38
}

TEST(DispatchCheckFrame, OkLeaveGroupResponse) {
    rpc::LeaveGroupResponse msg;
    msg.errorCode = 0;

    std::array<uint8_t, 32> buf{};
    const std::size_t len = msg.encode(buf.data());

    const auto fc = rpc::checkFrame(buf.data(), len);
    EXPECT_EQ(fc.status,     rpc::FrameStatus::Ok);
    EXPECT_EQ(fc.templateId, static_cast<uint16_t>(rpc::TemplateId::LeaveGroupResponse));  // 39
}

TEST(DispatchCheckFrame, OkOffsetCommit) {
    const uint8_t kGrp[] = {'g'};
    rpc::OffsetCommit msg;
    msg.generationId = 1;
    msg.groupId      = std::span<const uint8_t>(kGrp, 1u);
    msg.memberId     = std::span<const uint8_t>{};

    std::array<uint8_t, 64> buf{};
    const std::span<const rpc::OffsetCommitEntry> noEntries{};
    const std::size_t len = msg.encode(buf.data(), noEntries);

    const auto fc = rpc::checkFrame(buf.data(), len);
    EXPECT_EQ(fc.status,     rpc::FrameStatus::Ok);
    EXPECT_EQ(fc.templateId, static_cast<uint16_t>(rpc::TemplateId::OffsetCommit));  // 40
}

TEST(DispatchCheckFrame, OkOffsetCommitResponse) {
    rpc::OffsetCommitResponse msg;

    std::array<uint8_t, 64> buf{};
    const std::span<const rpc::OffsetCommitResult> noResults{};
    const std::size_t len = msg.encode(buf.data(), noResults);

    const auto fc = rpc::checkFrame(buf.data(), len);
    EXPECT_EQ(fc.status,     rpc::FrameStatus::Ok);
    EXPECT_EQ(fc.templateId, static_cast<uint16_t>(rpc::TemplateId::OffsetCommitResponse));  // 41
}

TEST(DispatchCheckFrame, OkOffsetFetch) {
    const uint8_t kGrp[] = {'g'};
    rpc::OffsetFetch msg;
    msg.groupId = std::span<const uint8_t>(kGrp, 1u);

    std::array<uint8_t, 64> buf{};
    const std::span<const rpc::OffsetFetchEntry> noEntries{};
    const std::size_t len = msg.encode(buf.data(), noEntries);

    const auto fc = rpc::checkFrame(buf.data(), len);
    EXPECT_EQ(fc.status,     rpc::FrameStatus::Ok);
    EXPECT_EQ(fc.templateId, static_cast<uint16_t>(rpc::TemplateId::OffsetFetch));  // 42
}

TEST(DispatchCheckFrame, OkOffsetFetchResponse) {
    rpc::OffsetFetchResponse msg;

    std::array<uint8_t, 64> buf{};
    const std::span<const rpc::OffsetFetchResult> noResults{};
    const std::size_t len = msg.encode(buf.data(), noResults);

    const auto fc = rpc::checkFrame(buf.data(), len);
    EXPECT_EQ(fc.status,     rpc::FrameStatus::Ok);
    EXPECT_EQ(fc.templateId, static_cast<uint16_t>(rpc::TemplateId::OffsetFetchResponse));  // 43
}

TEST(DispatchCheckFrame, OkAssign) {
    rpc::Assign msg;
    msg.generationId = 1;

    std::array<uint8_t, 64> buf{};
    const std::span<const rpc::TopicPartition32> noAsgn{};
    const std::size_t len = msg.encode(buf.data(), noAsgn);

    const auto fc = rpc::checkFrame(buf.data(), len);
    EXPECT_EQ(fc.status,     rpc::FrameStatus::Ok);
    EXPECT_EQ(fc.templateId, static_cast<uint16_t>(rpc::TemplateId::Assign));  // 44

    EXPECT_EQ(rpc::peekTemplateId(buf.data(), len),
              std::optional<uint16_t>(static_cast<uint16_t>(rpc::TemplateId::Assign)));
}

// ===========================================================================
// 2. TooShort — avail = 0, 1, 23 → TooShort; avail = 24 (exact) → NOT TooShort
// ===========================================================================

TEST(DispatchCheckFrame, TooShortZero) {
    // Zero bytes — nothing to read, must not crash.
    const uint8_t* buf = nullptr;  // will not be dereferenced
    std::array<uint8_t, 1> dummy{};
    const auto fc = rpc::checkFrame(dummy.data(), 0);
    EXPECT_EQ(fc.status, rpc::FrameStatus::TooShort);
    EXPECT_EQ(rpc::peekTemplateId(dummy.data(), 0), std::nullopt);
}

TEST(DispatchCheckFrame, TooShortOne) {
    std::array<uint8_t, 1> buf{0xFF};
    const auto fc = rpc::checkFrame(buf.data(), 1);
    EXPECT_EQ(fc.status, rpc::FrameStatus::TooShort);
    EXPECT_EQ(rpc::peekTemplateId(buf.data(), 1), std::nullopt);
}

TEST(DispatchCheckFrame, TooShortTwentyThree) {
    // 23 bytes — one byte short of a full header.
    auto buf = makeValidHeaderBuf(static_cast<uint16_t>(rpc::TemplateId::Keepalive));
    const auto fc = rpc::checkFrame(buf.data(), 23);
    EXPECT_EQ(fc.status, rpc::FrameStatus::TooShort);
    EXPECT_EQ(rpc::peekTemplateId(buf.data(), 23), std::nullopt);
}

TEST(DispatchCheckFrame, ExactlyTwentyFourNotTooShort) {
    // 24 bytes with valid header — Keepalive is exactly 24 bytes, so this is Ok.
    rpc::Keepalive ka;
    ka.sessionId     = 0;
    ka.correlationId = 0;
    std::array<uint8_t, 24> buf{};
    ka.encode(buf.data());

    const auto fc = rpc::checkFrame(buf.data(), 24);
    // Must NOT be TooShort.
    EXPECT_NE(fc.status, rpc::FrameStatus::TooShort);
    // With valid schema + version + known templateId (Keepalive=3) → Ok.
    EXPECT_EQ(fc.status, rpc::FrameStatus::Ok);
}

// ===========================================================================
// 3. WrongSchema — schemaId = 0, 111, 119, 121, 65535
// ===========================================================================

TEST(DispatchCheckFrame, WrongSchemaZero) {
    auto buf = makeHeaderBuf(static_cast<uint16_t>(rpc::TemplateId::Keepalive), 0, rpc::kProtocolVersion);
    const auto fc = rpc::checkFrame(buf.data(), buf.size());
    EXPECT_EQ(fc.status, rpc::FrameStatus::WrongSchema);
    EXPECT_EQ(rpc::peekTemplateId(buf.data(), buf.size()), std::nullopt);
}

TEST(DispatchCheckFrame, WrongSchema111) {
    auto buf = makeHeaderBuf(static_cast<uint16_t>(rpc::TemplateId::Connect), 111, rpc::kProtocolVersion);
    const auto fc = rpc::checkFrame(buf.data(), buf.size());
    EXPECT_EQ(fc.status, rpc::FrameStatus::WrongSchema);
}

TEST(DispatchCheckFrame, WrongSchema119) {
    // 119 = kSchemaId(120) - 1
    auto buf = makeHeaderBuf(static_cast<uint16_t>(rpc::TemplateId::Connect), 119, rpc::kProtocolVersion);
    const auto fc = rpc::checkFrame(buf.data(), buf.size());
    EXPECT_EQ(fc.status, rpc::FrameStatus::WrongSchema);
}

TEST(DispatchCheckFrame, WrongSchema121) {
    // 121 = kSchemaId(120) + 1
    auto buf = makeHeaderBuf(static_cast<uint16_t>(rpc::TemplateId::Connect), 121, rpc::kProtocolVersion);
    const auto fc = rpc::checkFrame(buf.data(), buf.size());
    EXPECT_EQ(fc.status, rpc::FrameStatus::WrongSchema);
}

TEST(DispatchCheckFrame, WrongSchema65535) {
    auto buf = makeHeaderBuf(static_cast<uint16_t>(rpc::TemplateId::Keepalive), 65535, rpc::kProtocolVersion);
    const auto fc = rpc::checkFrame(buf.data(), buf.size());
    EXPECT_EQ(fc.status, rpc::FrameStatus::WrongSchema);
    EXPECT_EQ(rpc::peekTemplateId(buf.data(), buf.size()), std::nullopt);
}

// ===========================================================================
// 4. UnsupportedVersion — > kProtocolVersion(2) rejected; 0, 1, 2 accepted
// ===========================================================================

TEST(DispatchCheckFrame, UnsupportedVersionTwo) {
    // version=2 is now kProtocolVersion — must be ACCEPTED (E5.1 bump).
    auto buf = makeValidHeaderBuf(static_cast<uint16_t>(rpc::TemplateId::Keepalive), 2);
    const auto fc = rpc::checkFrame(buf.data(), buf.size());
    EXPECT_NE(fc.status, rpc::FrameStatus::UnsupportedVersion);
    EXPECT_EQ(fc.status, rpc::FrameStatus::Ok);
    EXPECT_EQ(fc.version, 2u);
    // peekTemplateId must return the template (not nullopt) for a valid frame.
    EXPECT_EQ(rpc::peekTemplateId(buf.data(), buf.size()),
              std::optional<uint16_t>(static_cast<uint16_t>(rpc::TemplateId::Keepalive)));
}

TEST(DispatchCheckFrame, UnsupportedVersionThree) {
    // version=3 is now kProtocolVersion (E5.1 bump) — use version=4 (truly unsupported).
    auto buf = makeValidHeaderBuf(static_cast<uint16_t>(rpc::TemplateId::Connect), 4);
    const auto fc = rpc::checkFrame(buf.data(), buf.size());
    EXPECT_EQ(fc.status, rpc::FrameStatus::UnsupportedVersion);
    EXPECT_EQ(fc.version, 4u);
}

TEST(DispatchCheckFrame, UnsupportedVersion65535) {
    auto buf = makeValidHeaderBuf(static_cast<uint16_t>(rpc::TemplateId::Ping), 65535);
    const auto fc = rpc::checkFrame(buf.data(), buf.size());
    EXPECT_EQ(fc.status, rpc::FrameStatus::UnsupportedVersion);
    EXPECT_EQ(fc.version, 65535u);
    EXPECT_EQ(rpc::peekTemplateId(buf.data(), buf.size()), std::nullopt);
}

TEST(DispatchCheckFrame, VersionOneAccepted) {
    // version=1 (older than kProtocolVersion=2) — must be accepted (boundary: > not >=).
    auto buf = makeValidHeaderBuf(static_cast<uint16_t>(rpc::TemplateId::Keepalive), 1);
    const auto fc = rpc::checkFrame(buf.data(), buf.size());
    EXPECT_NE(fc.status, rpc::FrameStatus::UnsupportedVersion);
    EXPECT_EQ(fc.status, rpc::FrameStatus::Ok);
}

TEST(DispatchCheckFrame, VersionZeroAccepted) {
    // version == 0 (older) — must be accepted.
    auto buf = makeValidHeaderBuf(static_cast<uint16_t>(rpc::TemplateId::Keepalive), 0);
    const auto fc = rpc::checkFrame(buf.data(), buf.size());
    EXPECT_NE(fc.status, rpc::FrameStatus::UnsupportedVersion);
    // templateId 3 is known, schema correct, version 0 <= 2 → Ok.
    EXPECT_EQ(fc.status, rpc::FrameStatus::Ok);
    EXPECT_EQ(fc.version, 0u);
}

TEST(DispatchCheckFrame, UnsupportedVersionDataTemplate) {
    // Data template (tid=16 / Produce) at version > kProtocolVersion(3) → UnsupportedVersion.
    // Use version=4 since version=3 is now kProtocolVersion (E5.1 bump).
    // Version guard fires before template check — no mis-parse possible.
    auto buf = makeValidHeaderBuf(static_cast<uint16_t>(rpc::TemplateId::Produce), 4);
    const auto fc = rpc::checkFrame(buf.data(), buf.size());
    EXPECT_EQ(fc.status,  rpc::FrameStatus::UnsupportedVersion);
    EXPECT_EQ(fc.version, 4u);
    EXPECT_EQ(rpc::peekTemplateId(buf.data(), buf.size()), std::nullopt);
}

// ===========================================================================
// 5. UnknownTemplate — ids 0, 8, 15, 22, 45, 64, 99, 65535
//    (valid schema + version but unknown id)
//    Note: 16-21 (Produce..Unsubscribe) are KNOWN (PB-1/PB-2/PB-3).
//          32-44 (JoinGroup..Assign) are KNOWN (CG-1).
//    8 and 15 = unassigned control-range gap; 22 = first unassigned produce/consume gap;
//    45 = first unassigned groups/txn gap (Assign=44 is last assigned).
// ===========================================================================

TEST(DispatchCheckFrame, UnknownTemplateZero) {
    auto buf = makeValidHeaderBuf(0);
    const auto fc = rpc::checkFrame(buf.data(), buf.size());
    EXPECT_EQ(fc.status, rpc::FrameStatus::UnknownTemplate);
    EXPECT_EQ(fc.templateId, 0u);
    EXPECT_EQ(rpc::peekTemplateId(buf.data(), buf.size()), std::nullopt);
}

TEST(DispatchCheckFrame, KnownTemplateEight) {
    // 8 = MetadataRequest — now assigned by MD1 Task 10; must be Ok.
    auto buf = makeValidHeaderBuf(8);
    const auto fc = rpc::checkFrame(buf.data(), buf.size());
    EXPECT_EQ(fc.status, rpc::FrameStatus::Ok);
    EXPECT_EQ(fc.templateId, 8u);
}

TEST(DispatchCheckFrame, KnownTemplateNine) {
    // 9 = MetadataResponse — now assigned by MD1 Task 10; must be Ok.
    auto buf = makeValidHeaderBuf(9);
    const auto fc = rpc::checkFrame(buf.data(), buf.size());
    EXPECT_EQ(fc.status, rpc::FrameStatus::Ok);
    EXPECT_EQ(fc.templateId, 9u);
}

TEST(DispatchCheckFrame, UnknownTemplateTen) {
    // 10 = still unassigned in control range
    auto buf = makeValidHeaderBuf(10);
    const auto fc = rpc::checkFrame(buf.data(), buf.size());
    EXPECT_EQ(fc.status, rpc::FrameStatus::UnknownTemplate);
    EXPECT_EQ(fc.templateId, 10u);
}

TEST(DispatchCheckFrame, UnknownTemplateFifteen) {
    // 15 = last in control range (unassigned)
    auto buf = makeValidHeaderBuf(15);
    const auto fc = rpc::checkFrame(buf.data(), buf.size());
    EXPECT_EQ(fc.status, rpc::FrameStatus::UnknownTemplate);
    EXPECT_EQ(fc.templateId, 15u);
}

TEST(DispatchCheckFrame, UnknownTemplateTwentyTwo) {
    // 22 = first unassigned id in produce/consume range (16-21 are now all known in PB-3)
    auto buf = makeValidHeaderBuf(22);
    const auto fc = rpc::checkFrame(buf.data(), buf.size());
    EXPECT_EQ(fc.status, rpc::FrameStatus::UnknownTemplate);
    EXPECT_EQ(fc.templateId, 22u);
}

TEST(DispatchCheckFrame, UnknownTemplateFiftyFive) {
    // 55 = first unassigned id in groups/txn range after EndTxnResponse=54 (TX-1)
    // 45-54 are now all assigned to txn messages.
    auto buf = makeValidHeaderBuf(55);
    const auto fc = rpc::checkFrame(buf.data(), buf.size());
    EXPECT_EQ(fc.status, rpc::FrameStatus::UnknownTemplate);
    EXPECT_EQ(fc.templateId, 55u);
}

TEST(DispatchCheckFrame, UnknownTemplateSixtyFour) {
    // 64 = first in view range (unassigned)
    auto buf = makeValidHeaderBuf(64);
    const auto fc = rpc::checkFrame(buf.data(), buf.size());
    EXPECT_EQ(fc.status, rpc::FrameStatus::UnknownTemplate);
}

TEST(DispatchCheckFrame, UnknownTemplateNinetyNine) {
    auto buf = makeValidHeaderBuf(99);
    const auto fc = rpc::checkFrame(buf.data(), buf.size());
    EXPECT_EQ(fc.status, rpc::FrameStatus::UnknownTemplate);
    EXPECT_EQ(rpc::peekTemplateId(buf.data(), buf.size()), std::nullopt);
}

TEST(DispatchCheckFrame, UnknownTemplate65535) {
    auto buf = makeValidHeaderBuf(65535);
    const auto fc = rpc::checkFrame(buf.data(), buf.size());
    EXPECT_EQ(fc.status, rpc::FrameStatus::UnknownTemplate);
    EXPECT_EQ(fc.templateId, 65535u);
    EXPECT_EQ(rpc::peekTemplateId(buf.data(), buf.size()), std::nullopt);
}

// ===========================================================================
// 6. Ordering precedence
//    a) TooShort beats WrongSchema   — length is checked before any field read.
//    b) WrongSchema beats UnsupportedVersion — schema checked before version.
// ===========================================================================

TEST(DispatchCheckFrame, OrderingTooShortBeatsWrongSchema) {
    // Buffer has wrong schema bytes but is only 23 bytes → must return TooShort,
    // not WrongSchema (length is checked before any bytes are read from the header).
    auto full = makeHeaderBuf(static_cast<uint16_t>(rpc::TemplateId::Keepalive),
                              /*schemaId=*/0, rpc::kProtocolVersion);
    // Provide only 23 bytes of the 24-byte buffer.
    const auto fc = rpc::checkFrame(full.data(), 23);
    EXPECT_EQ(fc.status, rpc::FrameStatus::TooShort);
}

TEST(DispatchCheckFrame, OrderingWrongSchemaBeatsUnsupportedVersion) {
    // Header has both wrong schema AND version > 1 → WrongSchema wins.
    auto buf = makeHeaderBuf(static_cast<uint16_t>(rpc::TemplateId::Keepalive),
                             /*schemaId=*/0, /*version=*/99);
    const auto fc = rpc::checkFrame(buf.data(), buf.size());
    EXPECT_EQ(fc.status, rpc::FrameStatus::WrongSchema);
}

// ===========================================================================
// 7. peekTemplateId — nullopt for every non-Ok status
// ===========================================================================

TEST(DispatchPeekTemplateId, NulloptForTooShort) {
    std::array<uint8_t, 24> buf{};
    EXPECT_EQ(rpc::peekTemplateId(buf.data(), 0),  std::nullopt);
    EXPECT_EQ(rpc::peekTemplateId(buf.data(), 1),  std::nullopt);
    EXPECT_EQ(rpc::peekTemplateId(buf.data(), 23), std::nullopt);
}

TEST(DispatchPeekTemplateId, NulloptForWrongSchema) {
    auto buf = makeHeaderBuf(static_cast<uint16_t>(rpc::TemplateId::Keepalive), 0, 1);
    EXPECT_EQ(rpc::peekTemplateId(buf.data(), buf.size()), std::nullopt);
}

TEST(DispatchPeekTemplateId, NulloptForUnsupportedVersion) {
    // version=3 is now kProtocolVersion (E5.1 bump) — use version=4 (truly unsupported).
    auto buf = makeValidHeaderBuf(static_cast<uint16_t>(rpc::TemplateId::Keepalive), 4);
    EXPECT_EQ(rpc::peekTemplateId(buf.data(), buf.size()), std::nullopt);
}

TEST(DispatchPeekTemplateId, NulloptForUnknownTemplate) {
    auto buf = makeValidHeaderBuf(0);
    EXPECT_EQ(rpc::peekTemplateId(buf.data(), buf.size()), std::nullopt);
    auto buf2 = makeValidHeaderBuf(99);
    EXPECT_EQ(rpc::peekTemplateId(buf2.data(), buf2.size()), std::nullopt);
}

TEST(DispatchPeekTemplateId, ReturnsIdForOkFrame) {
    // Verify peekTemplateId returns the correct id for all known templates:
    // Milestone A control (1..7), Milestone B produce/subscribe (16..21),
    // Milestone C consumer-groups (32..44).
    for (uint16_t tid = 1; tid <= 9; ++tid) {
        auto buf = makeValidHeaderBuf(tid);
        const auto opt = rpc::peekTemplateId(buf.data(), buf.size());
        ASSERT_TRUE(opt.has_value()) << "Expected Some for templateId=" << tid;
        EXPECT_EQ(*opt, tid) << "templateId mismatch for " << tid;
    }
    // PB-1/PB-2/PB-3: tids 16..21 must all return a value.
    for (uint16_t tid = 16; tid <= 21; ++tid) {
        auto buf = makeValidHeaderBuf(tid);
        const auto opt = rpc::peekTemplateId(buf.data(), buf.size());
        ASSERT_TRUE(opt.has_value()) << "Expected Some for templateId=" << tid;
        EXPECT_EQ(*opt, tid) << "templateId mismatch for " << tid;
    }
    // CG-1: tids 32..44 must all return a value.
    for (uint16_t tid = 32; tid <= 44; ++tid) {
        auto buf = makeValidHeaderBuf(tid);
        const auto opt = rpc::peekTemplateId(buf.data(), buf.size());
        ASSERT_TRUE(opt.has_value()) << "Expected Some for templateId=" << tid;
        EXPECT_EQ(*opt, tid) << "templateId mismatch for " << tid;
    }
}

// ===========================================================================
// 8. No over-read: exact-size 24-byte buffers — every valid header field is
//    exercised at the edge without reading past the buffer end.
// ===========================================================================

TEST(DispatchNoOverread, ExactSizeBufferAllKnownTemplates) {
    // For each known templateId, a 24-byte buffer holds exactly one header.
    // checkFrame must not read past byte 23.  Under ASAN this catches any
    // over-read immediately; without ASAN the exact-size array acts as a
    // canary via std::array bounds in debug mode.
    //
    // Milestone A control (1..7) + Milestone B produce/subscribe (16..21)
    // + Milestone C consumer-groups (32..44).
    constexpr uint16_t kKnown[] = {
        1, 2, 3, 4, 5, 6, 7,
        8, 9,  // MD1 Task 10: MetadataRequest + MetadataResponse
        16, 17, 18, 19, 20, 21,
        32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44
    };
    for (uint16_t tid : kKnown) {
        const auto buf = makeValidHeaderBuf(tid);
        static_assert(sizeof(buf) == 24);

        const auto fc = rpc::checkFrame(buf.data(), 24);
        EXPECT_EQ(fc.status,     rpc::FrameStatus::Ok)  << "tid=" << tid;
        EXPECT_EQ(fc.templateId, tid)                    << "tid=" << tid;
        EXPECT_EQ(fc.version,    rpc::kProtocolVersion)  << "tid=" << tid;
    }
}

TEST(DispatchNoOverread, ExactSizeBufferWrongSchema) {
    // Wrong schema — still must not over-read the 24 bytes.
    const auto buf = makeHeaderBuf(1, 0, 1);
    static_assert(sizeof(buf) == 24);
    const auto fc = rpc::checkFrame(buf.data(), 24);
    EXPECT_EQ(fc.status, rpc::FrameStatus::WrongSchema);
}

// ===========================================================================
// 9. isKnownTemplate boundary
// ===========================================================================

TEST(DispatchIsKnownTemplate, KnownBoundaries) {
    // Milestone A control range: 1..7 inclusive must be known.
    for (uint16_t tid = 1; tid <= 7; ++tid) {
        EXPECT_TRUE(rpc::isKnownTemplate(tid)) << "tid=" << tid;
    }
    // Milestone B produce/subscribe range: 16..21 must all be known (PB-1/PB-2/PB-3).
    for (uint16_t tid = 16; tid <= 21; ++tid) {
        EXPECT_TRUE(rpc::isKnownTemplate(tid)) << "tid=" << tid;
    }
    // Milestone C consumer-group range: 32..44 must all be known (CG-1).
    for (uint16_t tid = 32; tid <= 44; ++tid) {
        EXPECT_TRUE(rpc::isKnownTemplate(tid)) << "tid=" << tid;
    }
    // Milestone C-B txn range: 45..54 must all be known (TX-1).
    for (uint16_t tid = 45; tid <= 54; ++tid) {
        EXPECT_TRUE(rpc::isKnownTemplate(tid)) << "tid=" << tid;
    }

    // MD1 Task 10: MetadataRequest=8, MetadataResponse=9 are now known.
    EXPECT_TRUE(rpc::isKnownTemplate(8));
    EXPECT_TRUE(rpc::isKnownTemplate(9));

    // Not known: 0, 10..15 (unassigned control gap), 22..31 (unassigned produce/consume gap),
    //            55..63 (unassigned groups/txn gap after TX-1), 64+ (view range), 65535.
    EXPECT_FALSE(rpc::isKnownTemplate(0));
    EXPECT_FALSE(rpc::isKnownTemplate(10));
    EXPECT_FALSE(rpc::isKnownTemplate(15));
    EXPECT_FALSE(rpc::isKnownTemplate(22));
    EXPECT_FALSE(rpc::isKnownTemplate(31));
    EXPECT_FALSE(rpc::isKnownTemplate(55));
    EXPECT_FALSE(rpc::isKnownTemplate(64));
    EXPECT_FALSE(rpc::isKnownTemplate(65535));
}
