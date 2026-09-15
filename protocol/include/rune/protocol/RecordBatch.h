#pragma once
#include <cstddef>
#include <cstdint>
#include <vector>
#include <string_view>
#include <cstring>
#include <functional>
#include "Crc32c.h"
#include "Varint.h"

namespace rune::protocol::batch {
inline constexpr int kBaseOffsetOffset = 0;
inline constexpr int kLengthOffset = 8;               // int32: bytes after this field
inline constexpr int kPartitionLeaderEpochOffset = 12; // int32, EXCLUDED from CRC
inline constexpr int kMagicOffset = 16;                // int8
inline constexpr int kCrcOffset = 17;                  // uint32 CRC-32C
inline constexpr int kAttributesOffset = 21;           // int16; CRC starts here
inline constexpr int kLastOffsetDeltaOffset = 23;      // int32
inline constexpr int kBaseTimestampOffset = 27;        // int64
inline constexpr int kMaxTimestampOffset = 35;         // int64
inline constexpr int kProducerIdOffset = 43;           // int64
inline constexpr int kProducerEpochOffset = 51;        // int16
inline constexpr int kBaseSequenceOffset = 53;         // int32
inline constexpr int kRecordCountOffset = 57;          // int32
inline constexpr int kRecordBatchOverhead = 61;        // records start here
inline constexpr int8_t kMagicValue = 2;

// Attributes bits (int16, little-endian in header).
inline constexpr int16_t kAttrControl        = 0x0020; // bit 5: isControl batch
inline constexpr int16_t kAttrTransactional  = 0x0010; // bit 4: isTransactional batch
} // namespace rune::protocol::batch

namespace rune::protocol {

// ---------------------------------------------------------------------------
// Little-endian helpers (ALL fixed header fields are now LE).
// Byte offsets are identical to the Kafka v2 layout — only endianness changed.
// ---------------------------------------------------------------------------
namespace detail {
// In-place patch: write v as LE uint32 at b[off].
inline void putLE(std::vector<uint8_t>& b, int off, uint32_t v) {
    b[off]   = static_cast<uint8_t>(v);
    b[off+1] = static_cast<uint8_t>(v >> 8);
    b[off+2] = static_cast<uint8_t>(v >> 16);
    b[off+3] = static_cast<uint8_t>(v >> 24);
}
inline void appendLE64(std::vector<uint8_t>& b, int64_t v) {
    uint64_t u = static_cast<uint64_t>(v);
    for (int i = 0; i < 8; ++i) b.push_back(static_cast<uint8_t>(u >> (i * 8)));
}
inline void appendLE32(std::vector<uint8_t>& b, int32_t v) {
    uint32_t u = static_cast<uint32_t>(v);
    for (int i = 0; i < 4; ++i) b.push_back(static_cast<uint8_t>(u >> (i * 8)));
}
inline void appendLE16(std::vector<uint8_t>& b, int16_t v) {
    uint16_t u = static_cast<uint16_t>(v);
    b.push_back(static_cast<uint8_t>(u));
    b.push_back(static_cast<uint8_t>(u >> 8));
}
inline uint32_t getLE32(const uint8_t* p) {
    return uint32_t(p[0]) | (uint32_t(p[1]) << 8) | (uint32_t(p[2]) << 16) | (uint32_t(p[3]) << 24);
}
inline uint64_t getLE64(const uint8_t* p) {
    uint64_t v = 0;
    for (int i = 0; i < 8; ++i) v |= static_cast<uint64_t>(p[i]) << (i * 8);
    return v;
}
inline uint16_t getLE16(const uint8_t* p) {
    return static_cast<uint16_t>(uint16_t(p[0]) | (uint16_t(p[1]) << 8));
}
} // namespace detail

// ---------------------------------------------------------------------------
// RecordBatchBuilder — little-endian fixed header, varint record bodies
// (varint bodies are byte-streams and are endian-neutral — unchanged).
//
// Optional control-batch mode: pass setControl(true) before build() to set
// the CONTROL attribute bit and stamp producerId/producerEpoch/baseSequence.
// ---------------------------------------------------------------------------
class RecordBatchBuilder {
public:
    RecordBatchBuilder(int64_t baseOffset, int64_t baseTimestamp)
        : baseOffset_(baseOffset), baseTimestamp_(baseTimestamp) {}

    // Enable control-batch mode: sets the CONTROL|TRANSACTIONAL attribute bits
    // and stamps producerId/producerEpoch/baseSequence into the header.
    void setControl(bool ctrl) noexcept { control_ = ctrl; }
    void setProducerId(int64_t pid) noexcept { producerId_ = pid; }
    void setProducerEpoch(int16_t ep) noexcept { producerEpoch_ = ep; }
    void setBaseSequence(int32_t seq) noexcept { baseSequence_ = seq; }

    void append(std::string_view key, std::string_view value, int64_t timestamp) {
        // Kafka v2 record: length(varint) attributes(int8) tsDelta(varlong)
        //   offsetDelta(varint) keyLen(varint) key valueLen(varint) value headerCount(varint=0)
        //
        // Zero per-record heap: we write the record body directly into records_,
        // reserving 10 bytes for the length varint (max varint32 size) at the start,
        // then patch it once we know the body size.
        const std::size_t lenPatchPos = records_.size();
        records_.resize(lenPatchPos + 10); // reserve space for length varint (max 10 bytes)
        const std::size_t bodyStart = lenPatchPos + 10;

        auto appendVarlong = [&](int64_t v) {
            uint8_t tmp[10]; size_t n = writeVarlong(v, tmp);
            records_.insert(records_.end(), tmp, tmp+n);
        };
        auto appendVarint = [&](int32_t v) {
            uint8_t tmp[10]; size_t n = writeVarint(v, tmp);
            records_.insert(records_.end(), tmp, tmp+n);
        };

        records_.push_back(0); // attributes
        appendVarlong(timestamp - baseTimestamp_);          // tsDelta
        appendVarint(recordCount_);                         // offsetDelta
        appendVarint(static_cast<int32_t>(key.size()));    // keyLen
        records_.insert(records_.end(), key.begin(), key.end());
        appendVarint(static_cast<int32_t>(value.size())); // valueLen
        records_.insert(records_.end(), value.begin(), value.end());
        appendVarint(0);                                    // header count

        // Patch length varint: body size is (records_.size() - bodyStart).
        const std::size_t bodySize = records_.size() - bodyStart;
        uint8_t lenbuf[10]; size_t ln = writeVarint(static_cast<int32_t>(bodySize), lenbuf);
        // Shift body bytes right if the actual varint is shorter than the 10-byte reserve.
        // Typical small records: varint is 1-2 bytes, so we trim the reserve.
        const std::size_t reserve = 10;
        if (ln < reserve) {
            // Move body forward to close the gap.
            const std::size_t gap = reserve - ln;
            std::memmove(records_.data() + lenPatchPos + ln,
                         records_.data() + bodyStart, bodySize);
            records_.resize(records_.size() - gap);
        }
        // Write the actual length varint at lenPatchPos.
        std::memcpy(records_.data() + lenPatchPos, lenbuf, ln);

        maxTimestamp_ = std::max(maxTimestamp_, timestamp);
        ++recordCount_;
    }

    /// Build the batch into the caller-supplied buffer (clear + reuse capacity).
    /// Eliminates the per-flush heap allocation on the agent produce hot path.
    void buildInto(std::vector<uint8_t>& out) {
        using namespace batch;
        int16_t attrs = control_
            ? static_cast<int16_t>(kAttrControl | kAttrTransactional)
            : int16_t{0};

        out.clear();
        detail::appendLE64(out, baseOffset_);
        detail::appendLE32(out, 0);                          // length (patched below)
        detail::appendLE32(out, -1);                         // partitionLeaderEpoch
        out.push_back(static_cast<uint8_t>(kMagicValue));   // magic = 2
        detail::appendLE32(out, 0);                          // CRC (patched below)
        detail::appendLE16(out, attrs);                      // attributes
        detail::appendLE32(out, recordCount_ - 1);           // lastOffsetDelta
        detail::appendLE64(out, baseTimestamp_);             // baseTimestamp
        detail::appendLE64(out, maxTimestamp_);              // maxTimestamp
        detail::appendLE64(out, producerId_);                // producerId
        detail::appendLE16(out, producerEpoch_);             // producerEpoch
        detail::appendLE32(out, baseSequence_);              // baseSequence
        detail::appendLE32(out, recordCount_);               // recordCount
        out.insert(out.end(), records_.begin(), records_.end());

        detail::putLE(out, kLengthOffset, static_cast<uint32_t>(out.size() - 12));
        uint32_t crc = crc32c(out.data() + kAttributesOffset, out.size() - kAttributesOffset);
        detail::putLE(out, kCrcOffset, crc);
    }

    std::vector<uint8_t> build() {
        using namespace batch;
        // Attributes: set CONTROL|TRANSACTIONAL bits if in control mode.
        int16_t attrs = control_
            ? static_cast<int16_t>(kAttrControl | kAttrTransactional)
            : int16_t{0};

        std::vector<uint8_t> b;
        detail::appendLE64(b, baseOffset_);                 // 0  (8 bytes)
        detail::appendLE32(b, 0);                           // 8  length (patched below)
        detail::appendLE32(b, -1);                          // 12 partitionLeaderEpoch (unset)
        b.push_back(static_cast<uint8_t>(kMagicValue));     // 16 magic = 2
        detail::appendLE32(b, 0);                           // 17 CRC (patched below)
        detail::appendLE16(b, attrs);                       // 21 attributes (LE int16)
        detail::appendLE32(b, recordCount_ - 1);            // 23 lastOffsetDelta
        detail::appendLE64(b, baseTimestamp_);              // 27 baseTimestamp
        detail::appendLE64(b, maxTimestamp_);               // 35 maxTimestamp
        detail::appendLE64(b, producerId_);                 // 43 producerId
        detail::appendLE16(b, producerEpoch_);              // 51 producerEpoch
        detail::appendLE32(b, baseSequence_);               // 53 baseSequence
        detail::appendLE32(b, recordCount_);                // 57 recordCount
        b.insert(b.end(), records_.begin(), records_.end());// 61 records

        // Patch length = total bytes from offset 12 onwards (i.e. total - 12).
        detail::putLE(b, kLengthOffset, static_cast<uint32_t>(b.size() - 12));
        // CRC32C over [kAttributesOffset .. end].
        uint32_t crc = crc32c(b.data() + kAttributesOffset, b.size() - kAttributesOffset);
        detail::putLE(b, kCrcOffset, crc);
        return b;
    }
    int32_t recordCount() const { return recordCount_; }

private:
    int64_t baseOffset_, baseTimestamp_;
    int64_t maxTimestamp_   = baseTimestamp_;
    int32_t recordCount_    = 0;
    int64_t producerId_     = -1;
    int16_t producerEpoch_  = -1;
    int32_t baseSequence_   = -1;
    bool    control_        = false;
    std::vector<uint8_t> records_;
};

// ---------------------------------------------------------------------------
// RecordBatchReader — reads the little-endian header fields.
// ---------------------------------------------------------------------------
class RecordBatchReader {
public:
    RecordBatchReader(const uint8_t* data, size_t len) : d_(data), len_(len) {}

    int64_t baseOffset()      const { return static_cast<int64_t>(detail::getLE64(d_ + batch::kBaseOffsetOffset)); }
    // partitionLeaderEpoch: the leader-epoch under which this batch was written.
    // Excluded from the CRC; -1 (unset) until a broker stamps it (stampLeaderEpoch).
    int32_t partitionLeaderEpoch() const { return static_cast<int32_t>(detail::getLE32(d_ + batch::kPartitionLeaderEpochOffset)); }
    int32_t lastOffsetDelta() const { return static_cast<int32_t>(detail::getLE32(d_ + batch::kLastOffsetDeltaOffset)); }
    int32_t recordCount()     const { return static_cast<int32_t>(detail::getLE32(d_ + batch::kRecordCountOffset)); }
    int32_t totalLength()     const { return static_cast<int32_t>(detail::getLE32(d_ + batch::kLengthOffset)) + 12; }
    int64_t baseTimestamp()   const { return static_cast<int64_t>(detail::getLE64(d_ + batch::kBaseTimestampOffset)); }
    int64_t producerId()      const { return static_cast<int64_t>(detail::getLE64(d_ + batch::kProducerIdOffset)); }
    int16_t producerEpoch()   const { return static_cast<int16_t>(detail::getLE16(d_ + batch::kProducerEpochOffset)); }
    int16_t attributes()      const { return static_cast<int16_t>(detail::getLE16(d_ + batch::kAttributesOffset)); }

    /// True if this batch has the CONTROL attribute bit set (bit 5 = 0x0020).
    bool isControl() const {
        if (len_ < static_cast<size_t>(batch::kAttributesOffset + 2)) return false;
        return (attributes() & batch::kAttrControl) != 0;
    }

    bool ensureValid() const {
        if (len_ < static_cast<size_t>(batch::kRecordBatchOverhead)) return false;
        int32_t tl = totalLength();
        if (tl <= 0 || static_cast<size_t>(tl) > len_) return false;
        uint32_t stored = detail::getLE32(d_ + batch::kCrcOffset);
        uint32_t actual = crc32c(d_ + batch::kAttributesOffset,
                                 static_cast<size_t>(tl) - batch::kAttributesOffset);
        return stored == actual;
    }

    void forEach(const std::function<void(std::string_view,std::string_view,int64_t,int64_t)>& fn) const {
        size_t pos = batch::kRecordBatchOverhead;
        int64_t base = baseOffset(); int64_t baseTs = baseTimestamp();
        for (int32_t i = 0; i < recordCount(); ++i) {
            (void)readVarint(d_, pos);            // record length
            ++pos;                                // attributes
            int64_t tsDelta  = readVarlong(d_, pos);
            int32_t offDelta = readVarint(d_, pos);
            int32_t klen     = readVarint(d_, pos);
            std::string_view key(reinterpret_cast<const char*>(d_ + pos), klen<0?0:klen);
            if (klen > 0) pos += klen;
            int32_t vlen = readVarint(d_, pos);
            std::string_view val(reinterpret_cast<const char*>(d_ + pos), vlen<0?0:vlen);
            if (vlen > 0) pos += vlen;
            int32_t hcount = readVarint(d_, pos); (void)hcount; // Phase 1: headers unused
            fn(key, val, base + offDelta, baseTs + tsDelta);
        }
    }

    // Stamp partitionLeaderEpoch in place (little-endian).
    // This field is EXCLUDED from the CRC, so the broker sets it on arrival
    // without recomputing the checksum.
    static void stampLeaderEpoch(uint8_t* data, int32_t epoch) {
        uint32_t u = static_cast<uint32_t>(epoch);
        for (int i = 0; i < 4; ++i)
            data[batch::kPartitionLeaderEpochOffset + i] = static_cast<uint8_t>(u >> (i * 8));
    }
private:
    const uint8_t* d_; size_t len_;
};
} // namespace rune::protocol
