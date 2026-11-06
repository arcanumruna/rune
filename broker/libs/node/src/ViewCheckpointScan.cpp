// ViewCheckpointScan.cpp -- see the header for the contract and the three classes.
#include <rune/node/ViewCheckpointScan.h>

#include <rune/protocol/Crc32c.h>
#include <rune/view/ViewCheckpoint.h>

namespace rune::node {

namespace {

inline uint32_t readU32LE(const uint8_t* p) noexcept {
    return static_cast<uint32_t>(p[0]) | (static_cast<uint32_t>(p[1]) << 8) |
           (static_cast<uint32_t>(p[2]) << 16) | (static_cast<uint32_t>(p[3]) << 24);
}

constexpr std::size_t kEntryHdr = 4 + 1 + 8;  // rid u32 + flags u8 + key u64
constexpr std::size_t kCrc      = 4;

// End offset of the record starting at `pos` (magic already known to be v1, v2 or v3), or 0 when the walk runs out of bytes.
std::size_t recordEnd(std::span<const uint8_t> bytes, std::size_t pos, uint32_t magic) noexcept {
    const std::size_t total = bytes.size();
    // Walks `count` entries of `rowLen` from `p`; 0 on truncation, else the end.
    auto walkEntries = [&](std::size_t p, uint32_t count, uint32_t rowLen, std::size_t entryHdr = kEntryHdr) -> std::size_t {
        for (uint32_t i = 0; i < count; ++i) {
            if (p + entryHdr > total) return 0;
            const uint8_t flags = bytes[p + 4];
            p += entryHdr;
            if (flags == 0) {  // upsert carries rowLen payload bytes; a delete does not
                if (p + rowLen > total) return 0;
                p += rowLen;
            }
        }
        return p;
    };
    if (magic == rune::view::kViewCheckpointMagic) {
        if (pos + 20 > total) return 0;
        const std::size_t p = walkEntries(pos + 20, readU32LE(bytes.data() + pos + 16), readU32LE(bytes.data() + pos + 12));
        return (p == 0 || p + kCrc > total) ? 0 : p + kCrc;
    }
    if (pos + 13 > total) return 0;
    const uint8_t tableCount = bytes[pos + 12];
    std::size_t p = pos + 13;
    if (magic == rune::view::kViewCheckpointMagicV3) {
        for (uint8_t t = 0; t < tableCount; ++t) {
            if (p + rune::view::kCheckpointV3TableHeaderBytes > total) return 0;
            const uint8_t keyLen = bytes[p + 1];
            p = walkEntries(p + rune::view::kCheckpointV3TableHeaderBytes, readU32LE(bytes.data() + p + 6), readU32LE(bytes.data() + p + 2),
                            rune::view::checkpointV3EntryHeaderBytes(keyLen));
            if (p == 0) return 0;
        }
        return p + kCrc > total ? 0 : p + kCrc;
    }
    for (uint8_t t = 0; t < tableCount; ++t) {
        if (p + 9 > total) return 0;
        const uint32_t rowLen = readU32LE(bytes.data() + p + 1);
        const uint32_t count  = readU32LE(bytes.data() + p + 5);
        p = walkEntries(p + 9, count, rowLen);
        if (p == 0) return 0;
    }
    return p + kCrc > total ? 0 : p + kCrc;
}

inline bool knownMagic(uint32_t m) noexcept {
    return m == rune::view::kViewCheckpointMagic || m == rune::view::kViewCheckpointMagicV2 || m == rune::view::kViewCheckpointMagicV3;
}

bool crcOk(std::span<const uint8_t> bytes, std::size_t pos, std::size_t end) noexcept {
    return readU32LE(bytes.data() + end - kCrc) == rune::protocol::crc32c(bytes.data() + pos, end - kCrc - pos);
}

bool allZero(std::span<const uint8_t> b) noexcept {
    for (const uint8_t c : b) if (c != 0) return false;
    return true;
}

// Fewer than 4 bytes left: a torn tail only when they are a prefix of a known magic's little-endian bytes.
bool prefixOfKnownMagic(std::span<const uint8_t> b) noexcept {
    for (const uint32_t magic : {rune::view::kViewCheckpointMagic, rune::view::kViewCheckpointMagicV2, rune::view::kViewCheckpointMagicV3}) {
        bool ok = true;
        for (std::size_t i = 0; i < b.size(); ++i) ok = ok && b[i] == static_cast<uint8_t>(magic >> (8 * i));
        if (ok) return true;
    }
    return false;
}

// True when a complete CRC-valid record starts anywhere in (pos, total): then the bytes at `pos` are not a record prefix
// that the writer simply never finished, and truncating there would drop that record.
bool hasValidRecordAfter(std::span<const uint8_t> bytes, std::size_t pos) noexcept {
    const std::size_t total = bytes.size();
    for (std::size_t q = pos + 1; q + 4 <= total; ++q) {
        const uint32_t m = readU32LE(bytes.data() + q);
        if (!knownMagic(m)) continue;
        const std::size_t end = recordEnd(bytes, q, m);
        if (end != 0 && crcOk(bytes, q, end)) return true;
    }
    return false;
}

}  // namespace

ViewCheckpointScan scanViewCheckpoint(std::span<const uint8_t> bytes) {
    ViewCheckpointScan out;
    const std::size_t total = bytes.size();
    std::size_t pos = 0;
    while (pos < total) {
        const auto rest = bytes.subspan(pos);
        if (allZero(rest)) { out.tail = ViewCheckpointTornTail{pos}; return out; }       // a zero-filled extent
        if (rest.size() < 4) {
            out.tail = prefixOfKnownMagic(rest) ? ViewCheckpointTail{ViewCheckpointTornTail{pos}} : ViewCheckpointTail{ViewCheckpointCorrupt{pos}};
            return out;
        }
        const uint32_t magic = readU32LE(bytes.data() + pos);
        if (!knownMagic(magic)) { out.tail = ViewCheckpointCorrupt{pos}; return out; }
        const std::size_t end = recordEnd(bytes, pos, magic);
        if (end == 0) {   // ran out of bytes: a strict prefix of one record, unless a whole valid record hides behind it
            out.tail = hasValidRecordAfter(bytes, pos) ? ViewCheckpointTail{ViewCheckpointCorrupt{pos}} : ViewCheckpointTail{ViewCheckpointTornTail{pos}};
            return out;
        }
        if (!crcOk(bytes, pos, end)) {
            // #817: size committed, data not. A complete-length record whose every byte after its fixed header, to EOF (not to
            // the record end), is zero is the LAST thing in the file and holds nothing: truncating there loses nothing.
            const std::size_t hdr = magic == rune::view::kViewCheckpointMagic ? 20 : 13;   // v1 / v2 header (recordEnd)
            out.tail = allZero(bytes.subspan(pos + hdr)) ? ViewCheckpointTail{ViewCheckpointTornTail{pos}}
                                                         : ViewCheckpointTail{ViewCheckpointCorrupt{pos}};
            return out;
        }
        out.records.push_back(bytes.subspan(pos, end - pos));
        pos = end;
    }
    out.tail = ViewCheckpointClean{};
    return out;
}

std::vector<std::span<const uint8_t>> splitViewCheckpointRecords(std::span<const uint8_t> bytes) {
    return scanViewCheckpoint(bytes).records;
}

}  // namespace rune::node
