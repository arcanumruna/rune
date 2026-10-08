#include <gtest/gtest.h>
#include "rune/protocol/client/CryptoEnvelope.h"

using namespace rune::protocol::client;

TEST(CryptoEnvelope, SubHeaderRoundTrip) {
    uint8_t buf[64] = {0};
    CryptoSubHeader h;
    h.version = 1;
    h.cipher  = 1;
    h.epoch   = 7;
    h.seq     = 0x0102030405060708ull;
    h.encode(buf + 24);

    auto d = CryptoSubHeader::decode(buf + 24, 12);
    ASSERT_TRUE(d.has_value());
    EXPECT_EQ(d->version, 1);
    EXPECT_EQ(d->cipher, 1);
    EXPECT_EQ(d->epoch, 7);
    EXPECT_EQ(d->seq, 0x0102030405060708ull);
}

TEST(CryptoEnvelope, SubHeaderDecodeTruncatedReturnsNullopt) {
    uint8_t buf[64] = {0};
    CryptoSubHeader h;
    h.version = 1;
    h.cipher  = 2;
    h.epoch   = 9;
    h.seq     = 42;
    h.encode(buf + 24);

    for (std::size_t avail = 0; avail < CryptoSubHeader::ENCODED_SIZE; ++avail) {
        EXPECT_FALSE(CryptoSubHeader::decode(buf + 24, avail).has_value()) << "avail=" << avail;
    }
    EXPECT_TRUE(CryptoSubHeader::decode(buf + 24, CryptoSubHeader::ENCODED_SIZE).has_value());
}

TEST(CryptoEnvelope, OverheadConstants) {
    EXPECT_EQ(kCryptoHeaderBytes, 36u);
    EXPECT_EQ(kCryptoOverhead, 28u);
    EXPECT_EQ(kCryptoSchemaId, 121);
}

// --- senderId in the epoch high byte (Task 18) --------------------------

TEST(CryptoEnvelope, SubHeaderSenderIdRoundTrip) {
    uint8_t buf[64] = {0};
    CryptoSubHeader h;
    h.version  = 1;
    h.cipher   = 1;
    h.epoch    = 0;
    h.senderId = 7;
    h.seq      = 0x0102030405060708ull;
    h.encode(buf + 24);

    // Wire-level: low byte (offset 2) is epoch, high byte (offset 3) is senderId.
    EXPECT_EQ(buf[24 + 2], 0);
    EXPECT_EQ(buf[24 + 3], 7);

    auto d = CryptoSubHeader::decode(buf + 24, 12);
    ASSERT_TRUE(d.has_value());
    EXPECT_EQ(d->version, 1);
    EXPECT_EQ(d->cipher, 1);
    EXPECT_EQ(d->epoch, 0);
    EXPECT_EQ(d->senderId, 7);
    EXPECT_EQ(d->seq, 0x0102030405060708ull);
}

TEST(CryptoEnvelope, SubHeaderSenderIdMaxRoundTrips) {
    uint8_t buf[64] = {0};
    CryptoSubHeader h;
    h.version  = 1;
    h.cipher   = 0;
    h.epoch    = 0;
    h.senderId = 255;
    h.seq      = 99;
    h.encode(buf + 24);

    EXPECT_EQ(buf[24 + 3], 255);

    auto d = CryptoSubHeader::decode(buf + 24, 12);
    ASSERT_TRUE(d.has_value());
    EXPECT_EQ(d->senderId, 255);
    EXPECT_EQ(d->epoch, 0);
}

TEST(CryptoEnvelope, SubHeaderSenderIdZeroEpochZeroByteIdenticalToPreTask18) {
    // Pre-Task-18 encoding of {version=1, cipher=1, epoch=0, seq=0x0102030405060708}:
    // bytes [0]=1 [1]=1 [2..3]=0,0 (LE u16 epoch=0) [4..11]=seq LE.
    static constexpr uint8_t kExpected[12] = {
        1, 1, 0, 0,
        0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01
    };

    uint8_t buf[12] = {0};
    CryptoSubHeader h;
    h.version  = 1;
    h.cipher   = 1;
    h.epoch    = 0;
    h.senderId = 0;
    h.seq      = 0x0102030405060708ull;
    h.encode(buf);

    for (int i = 0; i < 12; ++i) {
        EXPECT_EQ(buf[i], kExpected[i]) << "byte " << i;
    }
}

TEST(CryptoEnvelope, SubHeaderDecodeHighByteSenderId) {
    // Simulate a frame whose wire u16@2 has high byte 42 set directly (not via encode()),
    // confirming decode() reads senderId from the high byte independent of encode().
    uint8_t buf[12] = {0};
    buf[0] = 1;              // version
    buf[1] = 0;               // cipher
    buf[2] = 3;                // epoch low byte
    buf[3] = 42;               // senderId high byte
    // seq left at 0.

    auto d = CryptoSubHeader::decode(buf, 12);
    ASSERT_TRUE(d.has_value());
    EXPECT_EQ(d->epoch, 3);
    EXPECT_EQ(d->senderId, 42);
}
