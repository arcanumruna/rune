// src/catalog/CatalogFile.cpp — the .rcat functions: RecordFile.cpp with magic "RCAT" (bytes unchanged).
#include <rune/catalog/CatalogFile.h>

namespace rune::catalog {

void writeRcatHeader(std::ostream& out) { writeRecordFileHeader(out, kRcatMagic); }

void writeRcatRecord(std::ostream& out, int64_t offset, std::span<const uint8_t> bytes) {
    writeRecordFileRecord(out, offset, bytes);
}

std::expected<std::vector<std::pair<int64_t, std::vector<uint8_t>>>, std::string> readRcat(std::istream& in) {
    return readRecordFile(in, kRcatMagic);
}

} // namespace rune::catalog
