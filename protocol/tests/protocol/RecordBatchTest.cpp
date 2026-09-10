#include <gtest/gtest.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/protocol/Crc32c.h>
namespace b = rune::protocol::batch;

static uint32_t beU32(const std::vector<uint8_t>& b, int off) {
    return (b[off]<<24)|(b[off+1]<<16)|(b[off+2]<<8)|b[off+3];
}

TEST(RecordBatchLayout, FieldOffsetsMatchKafkaV2) {
    EXPECT_EQ(b::kBaseOffsetOffset, 0);
    EXPECT_EQ(b::kLengthOffset, 8);
    EXPECT_EQ(b::kPartitionLeaderEpochOffset, 12);
    EXPECT_EQ(b::kMagicOffset, 16);
    EXPECT_EQ(b::kCrcOffset, 17);
    EXPECT_EQ(b::kAttributesOffset, 21);
    EXPECT_EQ(b::kLastOffsetDeltaOffset, 23);
    EXPECT_EQ(b::kBaseTimestampOffset, 27);
    EXPECT_EQ(b::kMaxTimestampOffset, 35);
    EXPECT_EQ(b::kProducerIdOffset, 43);
    EXPECT_EQ(b::kProducerEpochOffset, 51);
    EXPECT_EQ(b::kBaseSequenceOffset, 53);
    EXPECT_EQ(b::kRecordCountOffset, 57);
    EXPECT_EQ(b::kRecordBatchOverhead, 61);
    EXPECT_EQ(b::kMagicValue, 2);
}

TEST(RecordBatchBuilder, EncodesHeaderAndCrc) {
    using namespace rune::protocol;
    RecordBatchBuilder rb(100 /*baseOffset*/, 5000 /*baseTs*/);
    rb.append("k1", "v1", 5000);
    rb.append("k2", "v2", 5001);
    auto bytes = rb.build();

    EXPECT_EQ(bytes[batch::kMagicOffset], batch::kMagicValue);
    EXPECT_EQ(beU32(bytes, batch::kRecordCountOffset), 2u);
    EXPECT_EQ(beU32(bytes, batch::kLastOffsetDeltaOffset), 1u); // 2 records -> delta 1
    // LENGTH = total - (baseOffset(8) + lengthField(4))
    EXPECT_EQ(beU32(bytes, batch::kLengthOffset), bytes.size() - 12);
    // CRC covers [attributes .. end]
    uint32_t expect = crc32c(bytes.data() + batch::kAttributesOffset,
                             bytes.size() - batch::kAttributesOffset);
    EXPECT_EQ(beU32(bytes, batch::kCrcOffset), expect);
}
