#pragma once
// ViewCheckpointScan.h -- classify an on-disk view checkpoint file (C7).
//
// The file a ViewSnapshotIoAgent writes is a BARE concatenation of rune::view::encodeDelta() wire records (the 8-byte
// routing header is stripped before the bytes hit the file).  Each record is self-describing:
//   v1: [magic u32][O i64][rowLen u32][count u32] count x {rid u32, flags u8, key u64, [rowLen bytes iff flags==0]} [crc u32]
//   v2: [magic u32][O i64][tableCount u8] { [id u8][rowLen u32][count u32] entries }* [crc u32]
// A crash between grouped fdatasyncs can leave a strict prefix of the last record (or a zero-filled extent) at the end.
// The scan sorts every file into exactly one class and never silently drops a tail:
//   Clean               every byte belongs to a CRC-valid record;
//   TornTail{cleanLen}  CRC-valid records up to cleanLen, then bytes holding NO complete CRC-valid record (a strict prefix of
//                       one record, or zeros).  Truncating the file to cleanLen loses nothing recovery could ever decode;
//   Corrupt{cleanLen}   anything else (a complete-length record whose CRC fails, an unknown non-zero magic, a complete valid
//                       record after bytes that do not verify).  Not provably lossless: backed up and rebuilt, never truncated.
// Bounds-safe (never trusts a length/count to over-read).  Cold: the IO agent runs it once per file open, the worker once per
// instance recovery.
#include <cstddef>
#include <cstdint>
#include <span>
#include <variant>
#include <vector>

namespace rune::node {

struct ViewCheckpointClean {};
struct ViewCheckpointTornTail { std::size_t cleanLen; };
struct ViewCheckpointCorrupt  { std::size_t cleanLen; };
using ViewCheckpointTail = std::variant<ViewCheckpointClean, ViewCheckpointTornTail, ViewCheckpointCorrupt>;

struct ViewCheckpointScan {
    std::vector<std::span<const uint8_t>> records;   ///< the CRC-valid records before the tail; spans point INTO `bytes`
    ViewCheckpointTail                    tail;
};

[[nodiscard]] ViewCheckpointScan scanViewCheckpoint(std::span<const uint8_t> bytes);

/// The records of scanViewCheckpoint(bytes) only (the tail class is dropped): for callers that do not repair the file.
[[nodiscard]] std::vector<std::span<const uint8_t>> splitViewCheckpointRecords(std::span<const uint8_t> bytes);

}  // namespace rune::node
