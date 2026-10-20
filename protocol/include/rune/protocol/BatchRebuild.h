#pragma once
// BatchRebuild.h — Phase 2c: rebuild one RecordBatch keeping a subset of its records, offsets unchanged.
// A Kafka-v2 batch may have holes in its offsets (each record carries its own offsetDelta; compaction
// leaves holes too).  A filtered subscription's partly matching batch therefore stays ONE batch: same
// baseOffset / lastOffsetDelta / timestamps / producer fields, matching records copied verbatim.
#include <rune/protocol/Crc32c.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/protocol/Varint.h>

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <span>
#include <string_view>

namespace rune::protocol {

struct RecordSpan {
    std::size_t      pos       = 0;      ///< first byte of the record (its length varint) inside the batch
    std::size_t      len       = 0;      ///< the whole record, length varint included
    std::string_view value;              ///< value bytes; empty when null or empty
    bool             nullValue = false;  ///< valueLen == -1 (a tombstone)
    int64_t          offset    = 0;      ///< baseOffset + offsetDelta
};

namespace detail {
// Bounded varint read: false when the varint runs past `end`.
inline bool readVarintBounded(const uint8_t* d, std::size_t end, std::size_t& pos, int64_t& out) noexcept {
    uint64_t raw = 0; int shift = 0;
    while (pos < end && shift < 64) {
        const uint8_t b = d[pos++];
        raw |= static_cast<uint64_t>(b & 0x7F) << shift;
        if ((b & 0x80) == 0) { out = static_cast<int64_t>((raw >> 1) ^ (~(raw & 1) + 1)); return true; }   // zigzag
        shift += 7;
    }
    return false;
}
}  // namespace detail

/// Visit every record of ONE batch (whose header and CRC the caller trusts): fn(const RecordSpan&).
/// Every read is bounded by the batch's totalLength; returns false at the first record that does not fit.
template <class Fn>
bool forEachRecordSpan(const uint8_t* batch, std::size_t len, Fn&& fn) noexcept {
    if (len < static_cast<std::size_t>(batch::kRecordBatchOverhead)) return false;
    RecordBatchReader r(batch, len);
    const int32_t total = r.totalLength();
    if (total < batch::kRecordBatchOverhead || static_cast<std::size_t>(total) > len) return false;
    const std::size_t end = static_cast<std::size_t>(total);
    const int64_t base = r.baseOffset();
    std::size_t pos = batch::kRecordBatchOverhead;
    for (int32_t i = 0; i < r.recordCount(); ++i) {
        const std::size_t start = pos;
        int64_t bodyLen = 0, tsDelta = 0, offDelta = 0, klen = 0, vlen = 0;
        if (!detail::readVarintBounded(batch, end, pos, bodyLen) || bodyLen < 0) return false;
        if (static_cast<uint64_t>(bodyLen) > end - pos) return false;
        const std::size_t recEnd = pos + static_cast<std::size_t>(bodyLen);
        if (pos >= recEnd) return false;
        ++pos;                                                               // attributes
        if (!detail::readVarintBounded(batch, recEnd, pos, tsDelta)) return false;
        if (!detail::readVarintBounded(batch, recEnd, pos, offDelta)) return false;
        if (!detail::readVarintBounded(batch, recEnd, pos, klen)) return false;
        if (klen > 0) { if (static_cast<uint64_t>(klen) > recEnd - pos) return false; pos += static_cast<std::size_t>(klen); }
        if (!detail::readVarintBounded(batch, recEnd, pos, vlen)) return false;
        RecordSpan s;
        s.pos = start; s.len = recEnd - start; s.offset = base + offDelta; s.nullValue = vlen < 0;
        if (vlen > 0) {
            if (static_cast<uint64_t>(vlen) > recEnd - pos) return false;
            s.value = std::string_view(reinterpret_cast<const char*>(batch + pos), static_cast<std::size_t>(vlen));
        }
        fn(static_cast<const RecordSpan&>(s));
        pos = recEnd;
    }
    return true;
}

/// Rebuild `src` keeping the records `keep` lists (ascending indices into `records`, the spans
/// forEachRecordSpan produced for `src`).  Header fields are copied from `src`; recordCount = keep.size();
/// length and CRC-32C are recomputed; kept records are copied verbatim.
/// Returns the bytes written, or 0 (keep empty, an index out of range, or `out` too small).  No allocation.
inline std::size_t rebuildBatch(std::span<const uint8_t> src, std::span<const RecordSpan> records,
                                std::span<const uint32_t> keep, std::span<uint8_t> out) noexcept {
    if (keep.empty() || src.size() < static_cast<std::size_t>(batch::kRecordBatchOverhead)) return 0;
    std::size_t need = batch::kRecordBatchOverhead;
    for (const uint32_t k : keep) {
        if (k >= records.size() || records[k].pos + records[k].len > src.size()) return 0;
        need += records[k].len;
    }
    if (need > out.size()) return 0;
    std::memcpy(out.data(), src.data(), batch::kRecordBatchOverhead);        // header incl. lastOffsetDelta
    std::size_t pos = batch::kRecordBatchOverhead;
    for (const uint32_t k : keep) {
        std::memcpy(out.data() + pos, src.data() + records[k].pos, records[k].len);
        pos += records[k].len;
    }
    const auto put32 = [&](int off, uint32_t v) {
        for (int i = 0; i < 4; ++i) out[static_cast<std::size_t>(off + i)] = static_cast<uint8_t>(v >> (8 * i));
    };
    put32(batch::kLengthOffset, static_cast<uint32_t>(pos - 12));             // bytes after the length field
    put32(batch::kRecordCountOffset, static_cast<uint32_t>(keep.size()));
    put32(batch::kCrcOffset, crc32c(out.data() + batch::kAttributesOffset, pos - batch::kAttributesOffset));
    return pos;
}

}  // namespace rune::protocol
