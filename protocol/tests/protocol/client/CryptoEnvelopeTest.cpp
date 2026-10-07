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
