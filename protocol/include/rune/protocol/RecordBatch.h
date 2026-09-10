#pragma once
#include <cstddef>
#include <cstdint>

namespace rune::protocol::batch {
inline constexpr int kBaseOffsetOffset = 0;
inline constexpr int kLengthOffset = 8;               // int32: bytes after this field
inline constexpr int kPartitionLeaderEpochOffset = 12; // int32, EXCLUDED from CRC
inline constexpr int kMagicOffset = 16;                // int8
inline constexpr int kCrcOffset = 17;                  // uint32 CRC-32C
inline constexpr int kAttributesOffset = 21;           // int16; CRC starts here
inline constexpr int kLastOffsetDeltaOffset = 23;      // int32
inline constexpr int kBaseTimestampOffset = 27;        // int64
inline constexpr int kMaxTimestampOffset = 35;         // int64
inline constexpr int kProducerIdOffset = 43;           // int64
inline constexpr int kProducerEpochOffset = 51;        // int16
inline constexpr int kBaseSequenceOffset = 53;         // int32
inline constexpr int kRecordCountOffset = 57;          // int32
inline constexpr int kRecordBatchOverhead = 61;        // records start here
inline constexpr int8_t kMagicValue = 2;
} // namespace rune::protocol::batch
