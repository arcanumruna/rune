// UnknownProducerIdTest.cpp — D6: UNKNOWN_PRODUCER_ID (Kafka 59) wire support.
//
//   * ErrorCode::UnknownProducerId == 28.
//   * ProduceResponse carries the partition's logStartOffset (int64, Kafka
//     PartitionProduceResponse.logStartOffset) as an ADDITIVE fixed field:
//     BODY_FIXED 14 -> 22, blockLength-versioned.  -1 = not carried.
//   * Compat (both directions, *Regression*):
//       - a new decoder reads an old 38-byte frame (blockLength 14) with
//         logStartOffset == -1;
//       - an OLD decoder (the pre-D6 fixed-offset decode, reproduced verbatim
//         below) reads a new frame's fields unchanged;
//       - the header version is NOT bumped (an old checkFrame would reject a
//         newer version as UnsupportedVersion).
//   * Client version gating constants (ConnectRequest.clientVersion).

#include <gtest/gtest.h>
#include <rune/protocol/client/ControlMessages.h>
#include <rune/protocol/client/DataMessages.h>
#include <rune/protocol/client/Dispatch.h>
#include <rune/protocol/client/Protocol.h>

#include <array>
#include <cstdint>
#include <cstring>
#include <optional>

namespace rpc = rune::protocol::client;

namespace {

// The pre-D6 ProduceResponse::decode, verbatim (fixed offsets, avail >= 38,
// blockLength ignored) — what every already-shipped client runs.
struct OldProduceResponse {
    int64_t sessionId = 0, correlationId = 0;
    int16_t errorCode = 0;
    int32_t partition = 0;
    int64_t baseOffset = 0;
};
std::optional<OldProduceResponse> oldDecode(const uint8_t* buf, std::size_t avail) {
    constexpr std::size_t kOldEncodedSize = 38;
    if (avail < kOldEncodedSize) return std::nullopt;
    auto hdr = rpc::MessageHeader::decode(buf, avail);
    if (!hdr || hdr->schemaId != rpc::kSchemaId || hdr->templateId != 17) return std::nullopt;
    OldProduceResponse m;
    m.sessionId     = hdr->sessionId;
    m.correlationId = hdr->correlationId;
    m.errorCode     = rpc::detail::getLE16s(buf, 24);
    m.partition     = rpc::detail::getLE32(buf, 26);
    m.baseOffset    = rpc::detail::getLE64(buf, 30);
    return m;
}

// An old broker's 38-byte frame (blockLength 14), built by hand.
std::array<uint8_t, 38> oldFrame(int16_t ec, int32_t partition, int64_t baseOffset) {
    std::array<uint8_t, 38> b{};
    rpc::MessageHeader h;
    h.blockLength   = 14;
    h.templateId    = 17;
    h.schemaId      = rpc::kSchemaId;
    h.version       = rpc::kProtocolVersion;
    h.sessionId     = 77;
    h.correlationId = 88;
    h.encode(b.data());
    rpc::detail::putLE16s(b.data(), 24, ec);
    rpc::detail::putLE32(b.data(), 26, partition);
    rpc::detail::putLE64(b.data(), 30, baseOffset);
    return b;
}

}  // namespace

TEST(UnknownProducerIdWire, ErrorCodeIs28) {
    EXPECT_EQ(static_cast<uint16_t>(rpc::ErrorCode::UnknownProducerId), 28u);
}

TEST(UnknownProducerIdWire, ClientVersionGateConstants) {
    // Clients at or above this version understand UnknownProducerId (28) and
    // ProduceResponse.logStartOffset; the broker answers older ones OOS.
    EXPECT_EQ(rpc::kClientVersionUnknownProducerId, 2);
    EXPECT_GE(rpc::kClientVersion, rpc::kClientVersionUnknownProducerId);
    EXPECT_TRUE(rpc::clientUnderstandsUnknownProducerId(rpc::kClientVersion));
    EXPECT_FALSE(rpc::clientUnderstandsUnknownProducerId(1));
    EXPECT_FALSE(rpc::clientUnderstandsUnknownProducerId(0));
}

TEST(UnknownProducerIdWire, ProduceResponseCarriesLogStartOffset) {
    rpc::ProduceResponse msg;
    msg.sessionId      = 5;
    msg.correlationId  = 6;
    msg.errorCode      = static_cast<int16_t>(rpc::ErrorCode::UnknownProducerId);
    msg.partition      = 3;
    msg.baseOffset     = 0;
    msg.logStartOffset = 0x0102030405060708LL;

    std::array<uint8_t, 64> buf{};
    const std::size_t n = msg.encode(buf.data());
    ASSERT_EQ(n, rpc::ProduceResponse::ENCODED_SIZE);
    ASSERT_EQ(n, 46u);
    EXPECT_EQ(rpc::ProduceResponse::BODY_FIXED, 22u);
    EXPECT_EQ(rpc::detail::getLE16(buf.data(), 0), 22u);                     // blockLength
    EXPECT_EQ(rpc::detail::getLE64(buf.data(), 38), 0x0102030405060708LL);  // logStartOffset @ body+14

    auto d = rpc::ProduceResponse::decode(buf.data(), n);
    ASSERT_TRUE(d.has_value());
    EXPECT_EQ(d->errorCode, static_cast<int16_t>(rpc::ErrorCode::UnknownProducerId));
    EXPECT_EQ(d->partition, 3);
    EXPECT_EQ(d->logStartOffset, 0x0102030405060708LL);
}

TEST(UnknownProducerIdWire, LogStartOffsetDefaultsToMinusOne) {
    rpc::ProduceResponse msg;
    EXPECT_EQ(msg.logStartOffset, -1);
    std::array<uint8_t, 64> buf{};
    const std::size_t n = msg.encode(buf.data());
    auto d = rpc::ProduceResponse::decode(buf.data(), n);
    ASSERT_TRUE(d.has_value());
    EXPECT_EQ(d->logStartOffset, -1);
}

// A new client talking to an OLD broker: the 38-byte frame still decodes and
// reports logStartOffset as not carried (-1).
TEST(UnknownProducerIdWireCompatRegression, NewDecoderReadsOldFrameRegression) {
    const auto f = oldFrame(0, 9, 123456);
    auto d = rpc::ProduceResponse::decode(f.data(), f.size());
    ASSERT_TRUE(d.has_value());
    EXPECT_EQ(d->sessionId, 77);
    EXPECT_EQ(d->correlationId, 88);
    EXPECT_EQ(d->errorCode, 0);
    EXPECT_EQ(d->partition, 9);
    EXPECT_EQ(d->baseOffset, 123456);
    EXPECT_EQ(d->logStartOffset, -1);
}

// An OLD client talking to a NEW broker: its fixed-offset decode reads every
// field it knows unchanged (the new field is a trailing addition).
TEST(UnknownProducerIdWireCompatRegression, OldDecoderReadsNewFrameRegression) {
    rpc::ProduceResponse msg;
    msg.sessionId      = 11;
    msg.correlationId  = 12;
    msg.errorCode      = static_cast<int16_t>(rpc::ErrorCode::OutOfOrderSequence);
    msg.partition      = 4;
    msg.baseOffset     = 999;
    msg.logStartOffset = 500;
    std::array<uint8_t, 64> buf{};
    const std::size_t n = msg.encode(buf.data());
    auto o = oldDecode(buf.data(), n);
    ASSERT_TRUE(o.has_value());
    EXPECT_EQ(o->sessionId, 11);
    EXPECT_EQ(o->correlationId, 12);
    EXPECT_EQ(o->errorCode, static_cast<int16_t>(rpc::ErrorCode::OutOfOrderSequence));
    EXPECT_EQ(o->partition, 4);
    EXPECT_EQ(o->baseOffset, 999);
    // ...and an old checkFrame accepts it (no version bump).
    EXPECT_EQ(rpc::checkFrame(buf.data(), n).status, rpc::FrameStatus::Ok);
}

// Truncation: header + blockLength bytes are required; a frame whose declared
// block is shorter than the v0 block is rejected.
TEST(UnknownProducerIdWireCompatRegression, DecodeHonoursBlockLengthRegression) {
    rpc::ProduceResponse msg;
    msg.logStartOffset = 7;
    std::array<uint8_t, 64> buf{};
    const std::size_t n = msg.encode(buf.data());
    for (std::size_t a = 0; a < n; ++a)
        EXPECT_FALSE(rpc::ProduceResponse::decode(buf.data(), a).has_value()) << "avail=" << a;
    EXPECT_TRUE(rpc::ProduceResponse::decode(buf.data(), n).has_value());

    auto f = oldFrame(0, 1, 2);
    for (std::size_t a = 0; a < f.size(); ++a)
        EXPECT_FALSE(rpc::ProduceResponse::decode(f.data(), a).has_value()) << "v0 avail=" << a;
    rpc::detail::putLE16(f.data(), 0, 13);   // declared block below v0
    EXPECT_FALSE(rpc::ProduceResponse::decode(f.data(), f.size()).has_value());

    // A future longer block (forward-compat): known fields still decode.
    std::array<uint8_t, 64> big{};
    std::memcpy(big.data(), buf.data(), n);
    rpc::detail::putLE16(big.data(), 0, 30);
    auto d = rpc::ProduceResponse::decode(big.data(), 24 + 30);
    ASSERT_TRUE(d.has_value());
    EXPECT_EQ(d->logStartOffset, 7);
}

// ---------------------------------------------------------------------------
// Review M4: the 46 B frame must not reach a session the version gate treats as
// old.  A STRICT pre-D6 decoder (exact length 38, exact blockLength 14 — what a
// non-C++ client may hard-code) must decode every answer the broker gives a
// session below kClientVersionUnknownProducerId.  encodeFor() is the single
// per-session encoder: v0 frame + UnknownProducerId -> OOS for old sessions;
// the 46 B frame (28 + logStartOffset) only for version >= 2.
// ---------------------------------------------------------------------------
namespace {
std::optional<OldProduceResponse> strictOldDecode(const uint8_t* buf, std::size_t len) {
    if (len != 38) return std::nullopt;
    if (rpc::detail::getLE16(buf, 0) != 14) return std::nullopt;   // blockLength
    return oldDecode(buf, len);
}
}  // namespace

TEST(UnknownProducerIdWireCompatRegression, EncodeForOldClientIsStrictV0FrameRegression) {
    for (int32_t ver : {0, 1}) {
        for (auto ec : {rpc::ErrorCode::OK, rpc::ErrorCode::UnknownProducerId, rpc::ErrorCode::NotLeader}) {
            rpc::ProduceResponse msg;
            msg.sessionId      = 21;
            msg.correlationId  = 22;
            msg.errorCode      = static_cast<int16_t>(ec);
            msg.partition      = 6;
            msg.baseOffset     = ec == rpc::ErrorCode::OK ? 4242 : 0;
            msg.logStartOffset = ec == rpc::ErrorCode::UnknownProducerId ? 100 : -1;
            std::array<uint8_t, 64> buf{};
            const std::size_t n = msg.encodeFor(buf.data(), ver);
            ASSERT_EQ(n, rpc::ProduceResponse::ENCODED_SIZE_V0) << "ver=" << ver;
            ASSERT_EQ(n, rpc::ProduceResponse::encodedSizeFor(ver));
            auto o = strictOldDecode(buf.data(), n);
            ASSERT_TRUE(o.has_value()) << "ver=" << ver << " ec=" << static_cast<int>(ec);
            EXPECT_EQ(o->sessionId, 21);
            EXPECT_EQ(o->correlationId, 22);
            EXPECT_EQ(o->partition, 6);
            EXPECT_EQ(o->baseOffset, msg.baseOffset);
            const auto want = ec == rpc::ErrorCode::UnknownProducerId ? rpc::ErrorCode::OutOfOrderSequence : ec;
            EXPECT_EQ(o->errorCode, static_cast<int16_t>(want));
            EXPECT_EQ(rpc::checkFrame(buf.data(), n).status, rpc::FrameStatus::Ok);
            auto d = rpc::ProduceResponse::decode(buf.data(), n);   // the new decoder too
            ASSERT_TRUE(d.has_value());
            EXPECT_EQ(d->logStartOffset, -1);
        }
    }
}

TEST(UnknownProducerIdWireCompatRegression, EncodeForNewClientIsFullFrameRegression) {
    rpc::ProduceResponse msg;
    msg.sessionId      = 31;
    msg.correlationId  = 32;
    msg.errorCode      = static_cast<int16_t>(rpc::ErrorCode::UnknownProducerId);
    msg.partition      = 2;
    msg.logStartOffset = 77;
    std::array<uint8_t, 64> buf{};
    const std::size_t n = msg.encodeFor(buf.data(), rpc::kClientVersionUnknownProducerId);
    ASSERT_EQ(n, rpc::ProduceResponse::ENCODED_SIZE);
    ASSERT_EQ(n, rpc::ProduceResponse::encodedSizeFor(rpc::kClientVersion));
    EXPECT_FALSE(strictOldDecode(buf.data(), n).has_value());   // a v2 frame by design
    auto d = rpc::ProduceResponse::decode(buf.data(), n);
    ASSERT_TRUE(d.has_value());
    EXPECT_EQ(d->errorCode, static_cast<int16_t>(rpc::ErrorCode::UnknownProducerId));
    EXPECT_EQ(d->logStartOffset, 77);
    // The gate helper (one definition for every broker answer site, review L4).
    EXPECT_EQ(rpc::produceErrorForClient(rpc::ErrorCode::UnknownProducerId, 1), rpc::ErrorCode::OutOfOrderSequence);
    EXPECT_EQ(rpc::produceErrorForClient(rpc::ErrorCode::UnknownProducerId, 2), rpc::ErrorCode::UnknownProducerId);
    EXPECT_EQ(rpc::produceErrorForClient(rpc::ErrorCode::NotLeader, 0), rpc::ErrorCode::NotLeader);
}
