#include <gtest/gtest.h>
#include <rune/protocol/RecordBatch.h>
namespace b = rune::protocol::batch;

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
