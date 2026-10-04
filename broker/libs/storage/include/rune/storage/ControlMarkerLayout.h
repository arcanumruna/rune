#pragma once
// ---------------------------------------------------------------------------
// ControlMarkerLayout.h — the ONE definition of the end-txn marker wire layout
// (D3b fix round 2).
//
// rune::transaction::ControlMarker::build() writes it (its ENCODED_SIZE /
// BATCH_HEADER_SIZE are these constants, and build() static_asserts the record
// offsets below), and the log cleaner (rune_storage, which cannot depend on
// rune_transaction) parses it with controlType() — no hand-copied offsets.
//
// Layout (all-LE RecordBatch, 75 bytes): the 61-byte batch header, then ONE
// record WITHOUT a record-length varint (unlike Kafka record framing):
//   [61] attributes = 0   [62] tsDelta = 0   [63] offsetDelta = 0
//   [64] keyLength = zig-zag(2) = 0x04   [65..66] key = int16 LE controlType
//   [67] valueLength = zig-zag(6) = 0x0C [68..73] version int16 + coordinatorEpoch int32
//   [74] headerCount = 0
// ---------------------------------------------------------------------------
#include <cstddef>
#include <cstdint>

namespace rune::storage::control_marker {

inline constexpr std::size_t kEncodedSize      = 75;
inline constexpr std::size_t kBatchHeaderSize   = 61;
inline constexpr std::size_t kKeyLengthOffset   = 64;
inline constexpr std::size_t kKeyOffset         = 65;
inline constexpr std::size_t kValueLengthOffset = 67;
inline constexpr uint8_t     kKeyLengthByte     = 0x04;   // zig-zag varint 2
inline constexpr uint8_t     kValueLengthByte   = 0x0C;   // zig-zag varint 6

inline constexpr int kCommit       = 0;
inline constexpr int kAbort        = 1;
inline constexpr int kUnparseable  = -1;

/// controlType of a marker in the layout above: kCommit / kAbort, or
/// kUnparseable when `batch` is not exactly that layout with a known type.
[[nodiscard]] inline int controlType(const uint8_t* batch, std::size_t len) noexcept {
    if (batch == nullptr || len != kEncodedSize) return kUnparseable;
    if (batch[kBatchHeaderSize] != 0 || batch[kBatchHeaderSize + 1] != 0 || batch[kBatchHeaderSize + 2] != 0 ||
        batch[kKeyLengthOffset] != kKeyLengthByte || batch[kValueLengthOffset] != kValueLengthByte)
        return kUnparseable;
    const int t = static_cast<int>(static_cast<uint16_t>(batch[kKeyOffset]) |
                                   static_cast<uint16_t>(batch[kKeyOffset + 1]) << 8);
    return (t == kCommit || t == kAbort) ? t : kUnparseable;
}

/// coordinatorEpoch of a marker in the layout above (value bytes [70..73]), or
/// -1 when `batch` is not that layout (D9 review MAJOR-2: the coordinator fence
/// is rebuilt from the log's markers).
inline constexpr std::size_t kCoordinatorEpochOffset = kValueLengthOffset + 3;   // after valueLength + int16 version
[[nodiscard]] inline int32_t coordinatorEpoch(const uint8_t* batch, std::size_t len) noexcept {
    if (controlType(batch, len) == kUnparseable) return -1;
    const uint8_t* p = batch + kCoordinatorEpochOffset;
    return static_cast<int32_t>(static_cast<uint32_t>(p[0]) | static_cast<uint32_t>(p[1]) << 8 |
                                static_cast<uint32_t>(p[2]) << 16 | static_cast<uint32_t>(p[3]) << 24);
}

} // namespace rune::storage::control_marker
