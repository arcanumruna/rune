#pragma once
// rune/catalog/CatalogFile.h — the .rcat file: a recorded __catalog log, as written by
// `rune catalog export` and read by `rune catalog replay` / the golden-log tests.
//
// Layout (all little-endian, byte-wise — host-endian independent):
//   "RCAT" (4 bytes) | u16 file version (= 2)
//   per record: i64 offset | u32 len | u32 crc32c | len bytes (one encoded CatalogEvent, any formatVer)
// crc32c (Castagnoli, rune::protocol::crc32c) covers the WHOLE encoded event, so a flipped bit
// in a field the fingerprint does not cover (principal, statement, timestamp) is caught too.
// Version 1 (no CRC) was never released and is refused.  Offsets strictly increase.  The file version is the CONTAINER's; each record carries
// its own event formatVer, judged by the replayer.
#include <cstddef>
#include <cstdint>
#include <expected>
#include <iosfwd>
#include <span>
#include <string>
#include <utility>
#include <vector>

namespace rune::catalog {

inline constexpr char     kRcatMagic[4]    = {'R', 'C', 'A', 'T'};
inline constexpr uint16_t kRcatFileVersion = 2;
inline constexpr std::size_t kRcatHeaderBytes       = 4 + 2;
inline constexpr std::size_t kRcatRecordHeaderBytes = 8 + 4 + 4;   // offset, len, crc32c

void writeRcatHeader(std::ostream& out);
/// A record longer than a u32 can say is never truncated: it sets out's failbit and writes nothing.
void writeRcatRecord(std::ostream& out, int64_t offset, std::span<const uint8_t> bytes);
/// Every record, in file order.  Refused (a message naming the byte position): a bad magic, an
/// unknown file version, a truncated header or record, an offset that does not strictly
/// increase, a record whose CRC32C does not match (naming the record index).  A declared length never allocates past the bytes actually present.
[[nodiscard]] std::expected<std::vector<std::pair<int64_t, std::vector<uint8_t>>>, std::string>
    readRcat(std::istream& in);

} // namespace rune::catalog
