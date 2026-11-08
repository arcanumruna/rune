#pragma once
// BatchRebuild.h — Phase 2c: rebuild one RecordBatch keeping a subset of its records, offsets unchanged.
// A Kafka-v2 batch may have holes in its offsets (each record carries its own offsetDelta; compaction
// leaves holes too).  A filtered subscription's partly matching batch therefore stays ONE batch: same
// baseOffset / lastOffsetDelta / timestamps / producer fields, matching records copied verbatim.
#include <rune/protocol/Crc32c.h>
#include <rune/protocol/RecordBatch.h>

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <span>
#include <string_view>

namespace rune::protocol {

struct RecordSpan {
    std::size_t      pos       = 0;      ///< first byte of the record (its length varint) inside the batch
    std::size_t      len       = 0;      ///< the whole record, length varint included
    std::string_view key;                ///< key bytes; empty when null or empty
    std::string_view value;              ///< value bytes; empty when null or empty
    bool             nullValue = false;  ///< valueLen == -1 (a tombstone)
    int64_t          offset    = 0;      ///< baseOffset + offsetDelta (wrapped, never signed overflow: a client's base offset is arbitrary)
    int64_t          offsetDelta = 0;    ///< the record's offsetDelta (0 .. lastOffsetDelta)
    std::size_t      valueLenPos = 0;    ///< first byte of the value-length varint inside the batch
    uint8_t          attributes = 0;     ///< the record's attributes byte (bits 0-1: write intent)
    uint32_t         index     = 0;      ///< the record's ordinal in the batch
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
    if (r.recordCount() < 0) return false;
    const int64_t base = r.baseOffset();
    std::size_t pos = batch::kRecordBatchOverhead;
    for (int32_t i = 0; i < r.recordCount(); ++i) {
        const std::size_t start = pos;
        int64_t bodyLen = 0, tsDelta = 0, offDelta = 0, klen = 0, vlen = 0;
        if (!detail::readVarintBounded(batch, end, pos, bodyLen) || bodyLen < 0) return false;
        if (static_cast<uint64_t>(bodyLen) > end - pos) return false;
        const std::size_t recEnd = pos + static_cast<std::size_t>(bodyLen);
        if (pos >= recEnd) return false;
        const uint8_t attributes = batch[pos++];
        if (!detail::readVarintBounded(batch, recEnd, pos, tsDelta)) return false;
        if (!detail::readVarintBounded(batch, recEnd, pos, offDelta)) return false;
        if (offDelta < 0 || offDelta > r.lastOffsetDelta()) return false;
        if (!detail::readVarintBounded(batch, recEnd, pos, klen)) return false;
        std::string_view key;
        if (klen > 0) {
            if (static_cast<uint64_t>(klen) > recEnd - pos) return false;
            key = std::string_view(reinterpret_cast<const char*>(batch + pos), static_cast<std::size_t>(klen));
            pos += static_cast<std::size_t>(klen);
        }
        const std::size_t vlenPos = pos;
        if (!detail::readVarintBounded(batch, recEnd, pos, vlen)) return false;
        RecordSpan s;
        s.valueLenPos = vlenPos;
        s.key = key;
        s.attributes = attributes; s.index = static_cast<uint32_t>(i);
        s.pos = start; s.len = recEnd - start; s.offsetDelta = offDelta;
        s.offset = static_cast<int64_t>(static_cast<uint64_t>(base) + static_cast<uint64_t>(offDelta)); s.nullValue = vlen < 0;
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
/// Returns the bytes written, or 0: keep empty, an index out of range, `keep` not strictly ascending, or `out`
/// too small.  0 therefore means a refusal OR an empty keep, so callers check keep.empty() first.  No allocation.
inline std::size_t rebuildBatch(std::span<const uint8_t> src, std::span<const RecordSpan> records,
                                std::span<const uint32_t> keep, std::span<uint8_t> out) noexcept {
    if (keep.empty() || src.size() < static_cast<std::size_t>(batch::kRecordBatchOverhead)) return 0;
    std::size_t need = batch::kRecordBatchOverhead;
    int64_t prev = -1;
    for (const uint32_t k : keep) {
        if (static_cast<int64_t>(k) <= prev) return 0;
        prev = k;
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

/// Worst-case bytes of one record's framing around its key and value: the length, timestamp-delta, offset-delta, key-length
/// and value-length varints (10 bytes each), the attributes byte and the header count.
inline constexpr std::size_t kMaxRecordFramingBytes = 5 * 10 + 2;

namespace detail {
inline std::size_t putVarint(uint8_t* out, int64_t v) noexcept {   // zigzag
    uint64_t z = (static_cast<uint64_t>(v) << 1) ^ static_cast<uint64_t>(v >> 63);
    std::size_t n = 0;
    while (z >= 0x80) { out[n++] = static_cast<uint8_t>(z | 0x80); z >>= 7; }
    out[n++] = static_cast<uint8_t>(z);
    return n;
}
inline std::size_t varintSize(int64_t v) noexcept {
    uint64_t z = (static_cast<uint64_t>(v) << 1) ^ static_cast<uint64_t>(v >> 63);
    std::size_t n = 1;
    while (z >= 0x80) { ++n; z >>= 7; }
    return n;
}
}  // namespace detail

/// Rebuild `src` with some records' VALUES replaced (every record kept, offsets unchanged).  `records` are the spans
/// forEachRecordSpan produced for `src`; `replacementFor(i)` (called once per record, in order) returns the new value of record
/// i, or an empty span to copy the record verbatim.  A replaced record gets its value-length and record-length varints
/// re-encoded; everything else (key, attributes, deltas, headers) is copied.  Header fields are copied from `src`; the batch
/// length is recomputed, the record count is unchanged and the CRC-32C is recomputed once.  Only records with a non-null value
/// can be replaced.  Returns the bytes written, or 0 (`records` do not match `src`, or `out` is too small).  No allocation.
template <class ReplacementFn>
std::size_t rebuildBatchReplacingValues(std::span<const uint8_t> src, std::span<const RecordSpan> records, ReplacementFn&& replacementFor,
                                        std::span<uint8_t> out) noexcept {
    if (src.size() < static_cast<std::size_t>(batch::kRecordBatchOverhead) || out.size() < static_cast<std::size_t>(batch::kRecordBatchOverhead)) return 0;
    std::memcpy(out.data(), src.data(), batch::kRecordBatchOverhead);
    std::size_t pos = batch::kRecordBatchOverhead;
    for (std::size_t i = 0; i < records.size(); ++i) {
        const RecordSpan& r = records[i];
        if (r.pos + r.len > src.size()) return 0;
        const std::span<const uint8_t> repl = replacementFor(i);
        if (repl.empty()) {
            if (r.len > out.size() - pos) return 0;
            std::memcpy(out.data() + pos, src.data() + r.pos, r.len);
            pos += r.len;
            continue;
        }
        if (r.value.empty()) return 0;
        const std::size_t valuePos = static_cast<std::size_t>(reinterpret_cast<const uint8_t*>(r.value.data()) - src.data());
        const std::size_t valueEnd = valuePos + r.value.size();
        const std::size_t recEnd   = r.pos + r.len;
        std::size_t bodyStart = r.pos;
        int64_t oldBody = 0;
        if (!detail::readVarintBounded(src.data(), recEnd, bodyStart, oldBody)) return 0;
        if (r.valueLenPos < bodyStart || valuePos < r.valueLenPos || valueEnd > recEnd) return 0;
        const std::size_t head = r.valueLenPos - bodyStart;   // attributes .. key
        const std::size_t tail = recEnd - valueEnd;           // headers
        const int64_t newVlen = static_cast<int64_t>(repl.size());
        const int64_t newBody = static_cast<int64_t>(head + detail::varintSize(newVlen) + repl.size() + tail);
        const std::size_t need = detail::varintSize(newBody) + static_cast<std::size_t>(newBody);
        if (need > out.size() - pos) return 0;
        pos += detail::putVarint(out.data() + pos, newBody);
        std::memcpy(out.data() + pos, src.data() + bodyStart, head);
        pos += head;
        pos += detail::putVarint(out.data() + pos, newVlen);
        std::memcpy(out.data() + pos, repl.data(), repl.size());
        pos += repl.size();
        std::memcpy(out.data() + pos, src.data() + valueEnd, tail);
        pos += tail;
    }
    const auto put32 = [&](int off, uint32_t v) {
        for (int i = 0; i < 4; ++i) out[static_cast<std::size_t>(off + i)] = static_cast<uint8_t>(v >> (8 * i));
    };
    put32(batch::kLengthOffset, static_cast<uint32_t>(pos - 12));
    put32(batch::kCrcOffset, crc32c(out.data() + batch::kAttributesOffset, pos - batch::kAttributesOffset));
    return pos;
}

}  // namespace rune::protocol
