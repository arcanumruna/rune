#pragma once
// rune/catalog/RecordFile.h — the record-file container shared by .rcat (a recorded __catalog log)
// and .rsec (a recorded __security log): the magic differs, the layout does not.
//
// Layout (all little-endian, byte-wise — host-endian independent):
//   magic (4 bytes) | u16 file version (= 2)
//   per record: i64 offset | u32 len | u32 crc32c | len bytes (one encoded event, any formatVer)
// crc32c (Castagnoli, rune::protocol::crc32c) covers the WHOLE encoded event, so a flipped bit in a
// field the fingerprint does not cover (principal, statement, timestamp) is caught too.  Offsets
// strictly increase.  The file version is the CONTAINER's; each record carries its own event
// formatVer, judged by the replayer.
#include <cstddef>
#include <cstdint>
#include <expected>
#include <iosfwd>
#include <span>
#include <string>
#include <utility>
#include <vector>

namespace rune::catalog {

inline constexpr uint16_t    kRecordFileVersion           = 2;
inline constexpr std::size_t kRecordFileHeaderBytes       = 4 + 2;
inline constexpr std::size_t kRecordFileRecordHeaderBytes = 8 + 4 + 4;   // offset, len, crc32c

void writeRecordFileHeader(std::ostream& out, const char (&magic)[4]);
/// A record longer than a u32 can say is never truncated: it sets out's failbit and writes nothing.
void writeRecordFileRecord(std::ostream& out, int64_t offset, std::span<const uint8_t> bytes);
/// Every record, in file order.  Refused (a message naming the byte position, prefixed with the
/// lower-cased magic): a bad magic, an unknown file version, a truncated header or record, an offset
/// that does not strictly increase, a record whose CRC32C does not match (naming the record index).
/// A declared length never allocates past the bytes actually present.
[[nodiscard]] std::expected<std::vector<std::pair<int64_t, std::vector<uint8_t>>>, std::string>
    readRecordFile(std::istream& in, const char (&magic)[4]);

} // namespace rune::catalog
