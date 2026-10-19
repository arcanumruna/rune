// src/security/SecurityFile.cpp — the .rsec functions: RecordFile with magic "RSEC".
#include <rune/security/SecurityFile.h>

namespace rune::security {

void writeRsecHeader(std::ostream& out) { rune::catalog::writeRecordFileHeader(out, kRsecMagic); }

void writeRsecRecord(std::ostream& out, int64_t offset, std::span<const uint8_t> bytes) {
    rune::catalog::writeRecordFileRecord(out, offset, bytes);
}

std::expected<std::vector<std::pair<int64_t, std::vector<uint8_t>>>, std::string> readRsec(std::istream& in) {
    return rune::catalog::readRecordFile(in, kRsecMagic);
}

} // namespace rune::security
