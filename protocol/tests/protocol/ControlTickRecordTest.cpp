// ControlTickRecordTest.cpp — TDD tests for ControlTickRecord codec.
//
// ControlTickRecord (recordType = 5) is the deterministic control-plane tick
// logged by the LEADER to the MSM log so all replicas apply decisions
// identically (heartbeat-expiry, reassignment trigger, etc.).
//
// Wire layout (all little-endian):
//   [0..1]   recordType     uint16 LE = 5
//   [2]      recordVersion  uint8  = 0
//   [3..10]  tickTimeNs     int64 LE
//   [11..18] tickSeq        int64 LE

#include <rune/protocol/MetadataRecords.h>
#include <gtest/gtest.h>
using namespace rune::protocol::metadata;

// ---------------------------------------------------------------------------
// Required by brief verbatim
// ---------------------------------------------------------------------------
TEST(ControlTickRecord, RoundTrips) {
    ControlTickRecord in; in.tickTimeNs = 123456789; in.tickSeq = 42;
    uint8_t buf[64]; auto n = in.encode(buf, sizeof buf); ASSERT_GT(n, 0u);
    auto out = ControlTickRecord::decode(buf, n); ASSERT_TRUE(out.has_value());
    EXPECT_EQ(out->tickTimeNs, 123456789);
    EXPECT_EQ(out->tickSeq, 42);
}

// ---------------------------------------------------------------------------
// Additional: wire byte-order / layout
// ---------------------------------------------------------------------------
TEST(ControlTickRecord, LittleEndianByteOrder) {
    ControlTickRecord in;
    in.tickTimeNs = static_cast<int64_t>(0x0102030405060708LL);
    in.tickSeq    = static_cast<int64_t>(0x090A0B0C0D0E0F10LL);

    uint8_t buf[64] = {};
    auto n = in.encode(buf, sizeof buf);
    ASSERT_GT(n, 0u);

    // [0..1] recordType = 5, LE
    EXPECT_EQ(buf[0], 0x05u);
    EXPECT_EQ(buf[1], 0x00u);
    // [2] recordVersion = 0
    EXPECT_EQ(buf[2], 0x00u);
    // [3..10] tickTimeNs LE: 0x0102030405060708 → 08 07 06 05 04 03 02 01
    EXPECT_EQ(buf[3],  0x08u);
    EXPECT_EQ(buf[4],  0x07u);
    EXPECT_EQ(buf[5],  0x06u);
    EXPECT_EQ(buf[6],  0x05u);
    EXPECT_EQ(buf[7],  0x04u);
    EXPECT_EQ(buf[8],  0x03u);
    EXPECT_EQ(buf[9],  0x02u);
    EXPECT_EQ(buf[10], 0x01u);
    // [11..18] tickSeq LE: 0x090A0B0C0D0E0F10 → 10 0F 0E 0D 0C 0B 0A 09
    EXPECT_EQ(buf[11], 0x10u);
    EXPECT_EQ(buf[12], 0x0Fu);
    EXPECT_EQ(buf[13], 0x0Eu);
    EXPECT_EQ(buf[14], 0x0Du);
    EXPECT_EQ(buf[15], 0x0Cu);
    EXPECT_EQ(buf[16], 0x0Bu);
    EXPECT_EQ(buf[17], 0x0Au);
    EXPECT_EQ(buf[18], 0x09u);
}

TEST(ControlTickRecord, TruncatedBufferReturnsNullopt) {
    ControlTickRecord in; in.tickTimeNs = 1; in.tickSeq = 2;
    uint8_t buf[64] = {};
    auto full = in.encode(buf, sizeof buf);
    ASSERT_GT(full, 0u);

    EXPECT_FALSE(ControlTickRecord::decode(buf, 0).has_value());
    EXPECT_FALSE(ControlTickRecord::decode(buf, 2).has_value());  // only type, no version
    EXPECT_FALSE(ControlTickRecord::decode(buf, full - 1).has_value());
    EXPECT_TRUE (ControlTickRecord::decode(buf, full).has_value());
}

TEST(ControlTickRecord, UnknownVersionReturnsNullopt) {
    ControlTickRecord in; in.tickTimeNs = 1; in.tickSeq = 2;
    uint8_t buf[64] = {};
    auto full = in.encode(buf, sizeof buf);
    ASSERT_GT(full, 0u);

    buf[2] = 99; // corrupt recordVersion
    EXPECT_FALSE(ControlTickRecord::decode(buf, full).has_value());
}

TEST(ControlTickRecord, WrongRecordTypeReturnsNullopt) {
    // Write a NodeLeaveRecord and try to decode as ControlTickRecord
    NodeLeaveRecord leave; leave.nodeId = 7;
    uint8_t buf[64] = {};
    auto full = leave.encode(buf, sizeof buf);
    ASSERT_GT(full, 0u);

    EXPECT_FALSE(ControlTickRecord::decode(buf, full).has_value());
}

TEST(ControlTickRecord, PeekReturnsFive) {
    ControlTickRecord in; in.tickTimeNs = 0; in.tickSeq = 0;
    uint8_t buf[64] = {};
    auto n = in.encode(buf, sizeof buf);
    ASSERT_GT(n, 0u);

    auto peeked = peekRecordType(buf, n);
    ASSERT_TRUE(peeked.has_value());
    EXPECT_EQ(*peeked, MetadataRecordType::ControlTick);
    EXPECT_EQ(static_cast<uint16_t>(*peeked), 5u);
}

TEST(ControlTickRecord, NegativeValuesRoundTrip) {
    ControlTickRecord in;
    in.tickTimeNs = -1LL;
    in.tickSeq    = INT64_MIN;
    uint8_t buf[64] = {};
    auto n = in.encode(buf, sizeof buf);
    ASSERT_GT(n, 0u);
    auto out = ControlTickRecord::decode(buf, n);
    ASSERT_TRUE(out.has_value());
    EXPECT_EQ(out->tickTimeNs, -1LL);
    EXPECT_EQ(out->tickSeq, INT64_MIN);
}

TEST(ControlTickRecord, EncodedSize) {
    // prefix(3) + tickTimeNs(8) + tickSeq(8) = 19 bytes
    ControlTickRecord in; in.tickTimeNs = 0; in.tickSeq = 0;
    uint8_t buf[64] = {};
    auto n = in.encode(buf, sizeof buf);
    EXPECT_EQ(n, 19u);
}
