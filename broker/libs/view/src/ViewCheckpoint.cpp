// libs/view/src/ViewCheckpoint.cpp
#include <rune/view/ViewCheckpoint.h>
#include <rune/protocol/Crc32c.h>
#include <cstring>

namespace rune::view {

namespace {

constexpr size_t kHeaderLen = 4 + 8 + 4 + 4; // magic, O, rowLen, count
constexpr size_t kCrcLen    = 4;
constexpr size_t kEntryHeaderLen = 4 + 1 + 8; // rid, flags, key
constexpr uint8_t kFlagUpsert = 0;
constexpr uint8_t kFlagDelete = 1;

inline void appendLE32(std::vector<uint8_t>& b, uint32_t v) {
    for (int i = 0; i < 4; ++i) b.push_back(static_cast<uint8_t>(v >> (i * 8)));
}
inline void appendLE64(std::vector<uint8_t>& b, uint64_t v) {
    for (int i = 0; i < 8; ++i) b.push_back(static_cast<uint8_t>(v >> (i * 8)));
}
inline uint32_t readLE32(const uint8_t* p) noexcept {
    return static_cast<uint32_t>(p[0]) | (static_cast<uint32_t>(p[1]) << 8) |
           (static_cast<uint32_t>(p[2]) << 16) | (static_cast<uint32_t>(p[3]) << 24);
}
inline uint64_t readLE64(const uint8_t* p) noexcept {
    uint64_t v = 0;
    for (int i = 0; i < 8; ++i) v |= static_cast<uint64_t>(p[i]) << (i * 8);
    return v;
}

} // namespace

size_t encodeDelta(std::span<const DirtyEntry> entries, int64_t O, uint32_t rowLen,
                    std::vector<uint8_t>& out) {
    const size_t startSize = out.size();

    appendLE32(out, kViewCheckpointMagic);
    appendLE64(out, static_cast<uint64_t>(O));
    appendLE32(out, rowLen);
    appendLE32(out, static_cast<uint32_t>(entries.size()));

    for (const auto& e : entries) {
        appendLE32(out, e.rid);
        out.push_back(e.deleted ? kFlagDelete : kFlagUpsert);
        appendLE64(out, e.key);
        if (!e.deleted) {
            out.insert(out.end(), e.row, e.row + rowLen);
        }
    }

    const size_t bodyLen = out.size() - startSize;
    const uint32_t crc = rune::protocol::crc32c(out.data() + startSize, bodyLen);
    appendLE32(out, crc);

    return out.size() - startSize;
}

std::optional<AppliedDelta> applyDelta(ViewTable& table, const RowLayout& layout,
                                        std::span<const uint8_t> deltaBytes) {
    const size_t total = deltaBytes.size();
    if (total < kHeaderLen + kCrcLen) return std::nullopt;

    const uint8_t* p = deltaBytes.data();

    const uint32_t magic = readLE32(p + 0);
    if (magic != kViewCheckpointMagic) return std::nullopt;
    const int64_t  O      = static_cast<int64_t>(readLE64(p + 4));
    const uint32_t rowLen = readLE32(p + 12);
    const uint32_t count  = readLE32(p + 16);

    // The record's declared row width must match the table it is being
    // replayed into -- a mismatch means either a wrong-schema checkpoint or
    // a corrupt header, and either way the row bytes below cannot be
    // trusted to be rowLen-sized, so reject before reading any entry.
    if (rowLen != layout.rowLen) return std::nullopt;

    // Pass 1: walk every entry validating bounds against the ACTUAL buffer
    // size at each step (never against the unvalidated `count` alone). A
    // bogus/huge `count` on a short buffer fails on its very first
    // iteration -- no huge allocation, no scan proportional to `count`, no
    // OOB read, ever.
    const size_t bodyEnd = total - kCrcLen; // one past the last entry byte; crc starts here
    size_t pos = kHeaderLen;
    for (uint32_t i = 0; i < count; ++i) {
        if (pos + kEntryHeaderLen > bodyEnd) return std::nullopt; // truncated entry header
        const uint8_t flags = p[pos + 4];
        if (flags != kFlagUpsert && flags != kFlagDelete) return std::nullopt; // malformed flags
        pos += kEntryHeaderLen;
        if (flags == kFlagUpsert) {
            if (pos + rowLen > bodyEnd) return std::nullopt; // truncated row payload
            pos += rowLen;
        }
    }
    if (pos != bodyEnd) return std::nullopt; // trailing junk between last entry and crc, or short body

    const uint32_t computedCrc = rune::protocol::crc32c(p, bodyEnd);
    const uint32_t storedCrc   = readLE32(p + bodyEnd);
    if (computedCrc != storedCrc) return std::nullopt;

    // Pass 2: validated -- now safe to apply every entry to the table.
    pos = kHeaderLen;
    for (uint32_t i = 0; i < count; ++i) {
        const uint8_t flags = p[pos + 4];
        const uint64_t key  = readLE64(p + pos + 5);
        pos += kEntryHeaderLen;
        if (flags == kFlagDelete) {
            table.erase(key);
        } else {
            uint8_t* row = table.put(key);
            std::memcpy(row, p + pos, rowLen);
            pos += rowLen;
        }
    }

    return AppliedDelta{O, count};
}

int64_t replay(ViewTable& table, const RowLayout& layout, std::span<const uint8_t> base,
               std::span<const std::span<const uint8_t>> deltasInOrder) {
    int64_t recovered = -1;

    if (!base.empty()) {
        auto applied = applyDelta(table, layout, base);
        if (!applied) return recovered; // corrupt/malformed base: nothing else can be trusted
        recovered = applied->O;
    }

    for (const auto& delta : deltasInOrder) {
        auto applied = applyDelta(table, layout, delta);
        if (!applied) break; // stop at the first bad/truncated record (append-only log's tail)
        recovered = applied->O;
    }

    return recovered;
}

size_t encodeMultiDelta(std::span<const TableDelta> tables, int64_t O,
                        std::vector<uint8_t>& out) {
    const size_t start = out.size();
    appendLE32(out, kViewCheckpointMagicV2);
    appendLE64(out, static_cast<uint64_t>(O));
    out.push_back(static_cast<uint8_t>(tables.size()));
    for (const TableDelta& t : tables) {
        out.push_back(t.tableId);
        appendLE32(out, t.rowLen);
        appendLE32(out, static_cast<uint32_t>(t.entries.size()));
        for (const DirtyEntry& e : t.entries) {
            appendLE32(out, e.rid);
            out.push_back(e.deleted ? uint8_t{1} : uint8_t{0});
            appendLE64(out, e.key);
            if (!e.deleted) out.insert(out.end(), e.row, e.row + t.rowLen);
        }
    }
    appendLE32(out, rune::protocol::crc32c(out.data() + start, out.size() - start));
    return out.size() - start;
}

std::optional<AppliedDelta> applyMultiDelta(std::span<ViewTable* const> tables,
                                            std::span<const uint32_t> rowLens,
                                            std::span<const uint8_t> b) {
    constexpr size_t kHdr = 4 + 8 + 1, kTbl = 1 + 4 + 4, kEnt = 4 + 1 + 8, kCrc = 4;
    if (tables.size() != rowLens.size() || tables.size() > 64) return std::nullopt;
    if (b.size() < kHdr + kCrc) return std::nullopt;
    const uint8_t* p = b.data();
    if (readLE32(p) != kViewCheckpointMagicV2) return std::nullopt;
    const int64_t O = static_cast<int64_t>(readLE64(p + 4));
    const uint8_t tableCount = p[12];

    // Pass 1: validate, never mutating.
    size_t pos = kHdr;
    uint64_t seen = 0;
    uint32_t total = 0;
    for (uint8_t t = 0; t < tableCount; ++t) {
        if (pos + kTbl > b.size()) return std::nullopt;
        const uint8_t  id     = p[pos];
        const uint32_t rowLen = readLE32(p + pos + 1);
        const uint32_t count  = readLE32(p + pos + 5);
        if (id >= tables.size() || rowLens[id] != rowLen || ((seen >> id) & 1u) != 0) {
            return std::nullopt;
        }
        seen |= uint64_t{1} << id;
        pos += kTbl;
        for (uint32_t i = 0; i < count; ++i) {
            if (pos + kEnt > b.size()) return std::nullopt;
            const uint8_t flags = p[pos + 4];
            if (flags > 1) return std::nullopt;
            pos += kEnt;
            if (flags == 0) {
                if (pos + rowLen > b.size()) return std::nullopt;
                pos += rowLen;
            }
        }
        total += count;
    }
    if (pos + kCrc != b.size()) return std::nullopt;
    if (readLE32(p + pos) != rune::protocol::crc32c(p, pos)) return std::nullopt;

    // Pass 2: apply.
    pos = kHdr;
    for (uint8_t t = 0; t < tableCount; ++t) {
        const uint8_t  id     = p[pos];
        const uint32_t rowLen = readLE32(p + pos + 1);
        const uint32_t count  = readLE32(p + pos + 5);
        pos += kTbl;
        ViewTable& tbl = *tables[id];
        for (uint32_t i = 0; i < count; ++i) {
            const uint8_t  flags = p[pos + 4];
            const uint64_t key   = readLE64(p + pos + 5);
            pos += kEnt;
            if (flags == 1) {
                tbl.erase(key);
            } else {
                std::memcpy(tbl.put(key), p + pos, rowLen);
                pos += rowLen;
            }
        }
    }
    return AppliedDelta{O, total};
}

int64_t replayMulti(std::span<ViewTable* const> tables, std::span<const uint32_t> rowLens,
                    std::span<const std::span<const uint8_t>> recordsInOrder) {
    int64_t last = -1;
    for (const auto& rec : recordsInOrder) {
        const auto applied = applyMultiDelta(tables, rowLens, rec);
        if (!applied) break;
        last = applied->O;
    }
    return last;
}

} // namespace rune::view
