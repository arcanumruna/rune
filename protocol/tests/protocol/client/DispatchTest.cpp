// tests/protocol/client/DispatchTest.cpp
//
// Exhaustive tests for rune::protocol::client::checkFrame / peekTemplateId.
//
// Coverage:
//   - Ok for all 7 known template ids (built with CP-1..4 encoders)
//   - TooShort: avail = 0, 1, 23 (no over-read); exactly 24 is NOT TooShort
//   - WrongSchema: schemaId = 0, 111, 119, 121, 65535
//   - UnsupportedVersion: version = 2, 3, 65535 (> 1 rejected);
//                         version = 1 (equal) and version = 0 (older) accepted
//   - UnknownTemplate: templateId = 0, 8, 15, 16, 32, 64, 99, 65535
//   - Ordering precedence: TooShort wins over WrongSchema; WrongSchema wins over
//                          UnsupportedVersion
//   - peekTemplateId: returns nullopt for every non-Ok case; returns id for Ok
//   - No over-read: exact-size (24-byte) buffers used throughout

#include <gtest/gtest.h>

#include <rune/protocol/client/ControlMessages.h>
#include <rune/protocol/client/Dispatch.h>
#include <rune/protocol/client/MessageHeader.h>
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
// 1. Ok — all 7 known template ids
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
// 4. UnsupportedVersion — > kProtocolVersion(1) rejected; 0 and 1 accepted
// ===========================================================================

TEST(DispatchCheckFrame, UnsupportedVersionTwo) {
    auto buf = makeValidHeaderBuf(static_cast<uint16_t>(rpc::TemplateId::Keepalive), 2);
    const auto fc = rpc::checkFrame(buf.data(), buf.size());
    EXPECT_EQ(fc.status, rpc::FrameStatus::UnsupportedVersion);
    EXPECT_EQ(fc.version, 2u);
    EXPECT_EQ(rpc::peekTemplateId(buf.data(), buf.size()), std::nullopt);
}

TEST(DispatchCheckFrame, UnsupportedVersionThree) {
    auto buf = makeValidHeaderBuf(static_cast<uint16_t>(rpc::TemplateId::Connect), 3);
    const auto fc = rpc::checkFrame(buf.data(), buf.size());
    EXPECT_EQ(fc.status, rpc::FrameStatus::UnsupportedVersion);
    EXPECT_EQ(fc.version, 3u);
}

TEST(DispatchCheckFrame, UnsupportedVersion65535) {
    auto buf = makeValidHeaderBuf(static_cast<uint16_t>(rpc::TemplateId::Ping), 65535);
    const auto fc = rpc::checkFrame(buf.data(), buf.size());
    EXPECT_EQ(fc.status, rpc::FrameStatus::UnsupportedVersion);
    EXPECT_EQ(fc.version, 65535u);
    EXPECT_EQ(rpc::peekTemplateId(buf.data(), buf.size()), std::nullopt);
}

TEST(DispatchCheckFrame, VersionOneAccepted) {
    // version == kProtocolVersion (1) — must be accepted (boundary: > not >=).
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
    // templateId 3 is known, schema correct, version 0 <= 1 → Ok.
    EXPECT_EQ(fc.status, rpc::FrameStatus::Ok);
    EXPECT_EQ(fc.version, 0u);
}

// ===========================================================================
// 5. UnknownTemplate — ids 0, 8, 15, 16, 32, 64, 99, 65535
//    (valid schema + version but unknown id)
// ===========================================================================

TEST(DispatchCheckFrame, UnknownTemplateZero) {
    auto buf = makeValidHeaderBuf(0);
    const auto fc = rpc::checkFrame(buf.data(), buf.size());
    EXPECT_EQ(fc.status, rpc::FrameStatus::UnknownTemplate);
    EXPECT_EQ(fc.templateId, 0u);
    EXPECT_EQ(rpc::peekTemplateId(buf.data(), buf.size()), std::nullopt);
}

TEST(DispatchCheckFrame, UnknownTemplateEight) {
    // 8 = first reserved-but-unassigned in control range
    auto buf = makeValidHeaderBuf(8);
    const auto fc = rpc::checkFrame(buf.data(), buf.size());
    EXPECT_EQ(fc.status, rpc::FrameStatus::UnknownTemplate);
    EXPECT_EQ(fc.templateId, 8u);
}

TEST(DispatchCheckFrame, UnknownTemplateFifteen) {
    // 15 = last in control range (unassigned)
    auto buf = makeValidHeaderBuf(15);
    const auto fc = rpc::checkFrame(buf.data(), buf.size());
    EXPECT_EQ(fc.status, rpc::FrameStatus::UnknownTemplate);
    EXPECT_EQ(fc.templateId, 15u);
}

TEST(DispatchCheckFrame, UnknownTemplateSixteen) {
    // 16 = first in produce/consume range
    auto buf = makeValidHeaderBuf(16);
    const auto fc = rpc::checkFrame(buf.data(), buf.size());
    EXPECT_EQ(fc.status, rpc::FrameStatus::UnknownTemplate);
}

TEST(DispatchCheckFrame, UnknownTemplateThirtyTwo) {
    // 32 = first in groups/txn range
    auto buf = makeValidHeaderBuf(32);
    const auto fc = rpc::checkFrame(buf.data(), buf.size());
    EXPECT_EQ(fc.status, rpc::FrameStatus::UnknownTemplate);
}

TEST(DispatchCheckFrame, UnknownTemplateSixtyFour) {
    // 64 = first in view range
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
    auto buf = makeValidHeaderBuf(static_cast<uint16_t>(rpc::TemplateId::Keepalive), 2);
    EXPECT_EQ(rpc::peekTemplateId(buf.data(), buf.size()), std::nullopt);
}

TEST(DispatchPeekTemplateId, NulloptForUnknownTemplate) {
    auto buf = makeValidHeaderBuf(0);
    EXPECT_EQ(rpc::peekTemplateId(buf.data(), buf.size()), std::nullopt);
    auto buf2 = makeValidHeaderBuf(99);
    EXPECT_EQ(rpc::peekTemplateId(buf2.data(), buf2.size()), std::nullopt);
}

TEST(DispatchPeekTemplateId, ReturnsIdForOkFrame) {
    // Verify peekTemplateId returns the correct id for each of the 7 known templates.
    for (uint16_t tid = 1; tid <= 7; ++tid) {
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
    for (uint16_t tid = 1; tid <= 7; ++tid) {
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
    // 1..7 inclusive must be known.
    for (uint16_t tid = 1; tid <= 7; ++tid) {
        EXPECT_TRUE(rpc::isKnownTemplate(tid)) << "tid=" << tid;
    }
    // 0 and 8 are NOT known.
    EXPECT_FALSE(rpc::isKnownTemplate(0));
    EXPECT_FALSE(rpc::isKnownTemplate(8));
    EXPECT_FALSE(rpc::isKnownTemplate(65535));
}
