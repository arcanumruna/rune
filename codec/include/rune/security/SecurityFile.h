#pragma once
// rune/security/SecurityFile.h — the .rsec file: a recorded __security log, written by the golden-log tool
// (make_security_golden) and read by the golden-log tests.  The RecordFile.h container (plan D38)
// with magic "RSEC": file version 2, per-record CRC32C, strictly increasing offsets.
// No `rune` command writes this file yet.  A future writer must create it with mode 0600 and O_EXCL: the file holds
// every user's verifier (StoredKey / ServerKey).
#include <rune/catalog/RecordFile.h>

#include <cstdint>
#include <expected>
#include <iosfwd>
#include <span>
#include <string>
#include <utility>
#include <vector>

namespace rune::security {

inline constexpr char kRsecMagic[4] = {'R', 'S', 'E', 'C'};

void writeRsecHeader(std::ostream&);
void writeRsecRecord(std::ostream&, int64_t offset, std::span<const uint8_t> bytes);
[[nodiscard]] std::expected<std::vector<std::pair<int64_t, std::vector<uint8_t>>>, std::string> readRsec(std::istream&);

} // namespace rune::security
