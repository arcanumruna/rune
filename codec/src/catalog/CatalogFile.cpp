// src/catalog/CatalogFile.cpp — see CatalogFile.h for the frozen .rcat layout.
#include <rune/catalog/CatalogFile.h>
#include <rune/protocol/Crc32c.h>

#include <algorithm>
#include <cstdio>
#include <array>
#include <istream>
#include <limits>
#include <ostream>

namespace rune::catalog {

namespace {
template <std::size_t N>
void putLe(std::ostream& out, uint64_t v) {
    std::array<char, N> b{};
    for (std::size_t i = 0; i < N; ++i) b[i] = static_cast<char>(static_cast<uint8_t>(v >> (8 * i)));
    out.write(b.data(), N);
}
uint64_t getLe(const uint8_t* p, std::size_t n) noexcept {
    uint64_t v = 0;
    for (std::size_t i = 0; i < n; ++i) v |= static_cast<uint64_t>(p[i]) << (8 * i);
    return v;
}
/// Read up to n bytes; returns how many were read (fewer only at end of stream).
std::size_t readSome(std::istream& in, uint8_t* p, std::size_t n) {
    in.read(reinterpret_cast<char*>(p), static_cast<std::streamsize>(n));
    return static_cast<std::size_t>(in.gcount());
}
std::string at(uint64_t pos) { return " at byte " + std::to_string(pos); }
std::string hex32(uint32_t v) {
    char buf[2 + 8 + 1];
    std::snprintf(buf, sizeof buf, "0x%08x", v);
    return buf;
}

/// Body bytes are read in chunks, so a corrupt (huge) length allocates no more than the
/// stream actually holds before it is found truncated.
constexpr std::size_t kChunk = 64 * 1024;
} // namespace

void writeRcatHeader(std::ostream& out) {
    out.write(kRcatMagic, sizeof kRcatMagic);
    putLe<2>(out, kRcatFileVersion);
}

void writeRcatRecord(std::ostream& out, int64_t offset, std::span<const uint8_t> bytes) {
    if (bytes.size() > std::numeric_limits<uint32_t>::max()) {
        out.setstate(std::ios::failbit);
        return;
    }
    putLe<8>(out, static_cast<uint64_t>(offset));
    putLe<4>(out, static_cast<uint32_t>(bytes.size()));
    putLe<4>(out, rune::protocol::crc32c(bytes.data(), bytes.size()));
    out.write(reinterpret_cast<const char*>(bytes.data()), static_cast<std::streamsize>(bytes.size()));
}

std::expected<std::vector<std::pair<int64_t, std::vector<uint8_t>>>, std::string> readRcat(std::istream& in) {
    using Out = std::vector<std::pair<int64_t, std::vector<uint8_t>>>;
    std::array<uint8_t, kRcatRecordHeaderBytes> h{};
    uint64_t pos = 0;

    const std::size_t got = readSome(in, h.data(), kRcatHeaderBytes);
    if (got < kRcatHeaderBytes)
        return std::unexpected("rcat: truncated file header" + at(0) + " (" + std::to_string(got) + " of " +
                               std::to_string(kRcatHeaderBytes) + " bytes)");
    if (!std::equal(h.begin(), h.begin() + 4, reinterpret_cast<const uint8_t*>(kRcatMagic)))
        return std::unexpected("rcat: bad magic" + at(0) + " (not an .rcat file)");
    const auto version = static_cast<uint16_t>(getLe(h.data() + 4, 2));
    if (version != kRcatFileVersion)
        return std::unexpected("rcat: unknown file version " + std::to_string(version) + at(4));
    pos = kRcatHeaderBytes;

    Out out;
    for (;;) {
        const uint64_t recordAt = pos;
        const std::size_t n = readSome(in, h.data(), kRcatRecordHeaderBytes);
        if (n == 0) break;   // clean end: between records
        if (n < kRcatRecordHeaderBytes)
            return std::unexpected("rcat: truncated record header" + at(recordAt) + " (" + std::to_string(n) + " of " +
                                   std::to_string(kRcatRecordHeaderBytes) + " bytes)");
        const auto offset = static_cast<int64_t>(getLe(h.data(), 8));
        const auto len = static_cast<uint32_t>(getLe(h.data() + 8, 4));
        const auto crc = static_cast<uint32_t>(getLe(h.data() + 12, 4));
        if (!out.empty() && offset <= out.back().first)
            return std::unexpected("rcat: offset " + std::to_string(offset) + " does not follow offset " +
                                   std::to_string(out.back().first) + at(recordAt));
        pos += kRcatRecordHeaderBytes;
        std::vector<uint8_t> body;
        while (body.size() < len) {
            const std::size_t want = std::min<std::size_t>(kChunk, len - body.size());
            const std::size_t have = body.size();
            body.resize(have + want);
            const std::size_t r = readSome(in, body.data() + have, want);
            if (r < want)
                return std::unexpected("rcat: truncated record body of offset " + std::to_string(offset) +
                                       at(pos + have + r) + " (" + std::to_string(have + r) + " of " +
                                       std::to_string(len) + " bytes)");
        }
        if (const uint32_t actual = rune::protocol::crc32c(body.data(), body.size()); actual != crc)
            return std::unexpected("rcat: CRC32C mismatch in record " + std::to_string(out.size()) + " (offset " +
                                   std::to_string(offset) + ")" + at(recordAt) + ": stored=" + hex32(crc) +
                                   " computed=" + hex32(actual));
        pos += len;
        out.emplace_back(offset, std::move(body));
    }
    if (in.bad()) return std::unexpected("rcat: read error" + at(pos));
    return out;
}

} // namespace rune::catalog
