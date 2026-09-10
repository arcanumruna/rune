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
} // namespace rune::protocol::batch

namespace rune::protocol {

namespace detail {
inline void putBE(std::vector<uint8_t>& b, int off, uint32_t v) {
    b[off]=v>>24; b[off+1]=v>>16; b[off+2]=v>>8; b[off+3]=v;
}
inline void appendBE64(std::vector<uint8_t>& b, int64_t v) {
    for (int i=7;i>=0;--i) b.push_back(static_cast<uint8_t>(v>>(i*8)));
}
inline void appendBE32(std::vector<uint8_t>& b, int32_t v) {
    for (int i=3;i>=0;--i) b.push_back(static_cast<uint8_t>(v>>(i*8)));
}
inline void appendBE16(std::vector<uint8_t>& b, int16_t v) {
    b.push_back(static_cast<uint8_t>(v>>8)); b.push_back(static_cast<uint8_t>(v));
}
inline uint32_t getBE32(const uint8_t* p){ return (p[0]<<24)|(p[1]<<16)|(p[2]<<8)|p[3]; }
inline uint64_t getBE64(const uint8_t* p){ uint64_t v=0; for(int i=0;i<8;++i) v=(v<<8)|p[i]; return v; }
} // namespace detail

class RecordBatchBuilder {
public:
    RecordBatchBuilder(int64_t baseOffset, int64_t baseTimestamp)
        : baseOffset_(baseOffset), baseTimestamp_(baseTimestamp) {}

    void append(std::string_view key, std::string_view value, int64_t timestamp) {
        // Kafka v2 record: length(varint) attributes(int8) tsDelta(varlong)
        //   offsetDelta(varint) keyLen(varint) key valueLen(varint) value headerCount(varint=0)
        std::vector<uint8_t> body;
        auto put = [&](auto fn){ uint8_t tmp[10]; size_t n = fn(tmp); body.insert(body.end(), tmp, tmp+n); };
        body.push_back(0); // attributes
        put([&](uint8_t* o){ return writeVarlong(timestamp - baseTimestamp_, o); });
        put([&](uint8_t* o){ return writeVarint(recordCount_, o); }); // offsetDelta
        put([&](uint8_t* o){ return writeVarint(static_cast<int32_t>(key.size()), o); });
        body.insert(body.end(), key.begin(), key.end());
        put([&](uint8_t* o){ return writeVarint(static_cast<int32_t>(value.size()), o); });
        body.insert(body.end(), value.begin(), value.end());
        put([&](uint8_t* o){ return writeVarint(0, o); }); // header count

        uint8_t lenbuf[10]; size_t ln = writeVarint(static_cast<int32_t>(body.size()), lenbuf);
        records_.insert(records_.end(), lenbuf, lenbuf+ln);
        records_.insert(records_.end(), body.begin(), body.end());
        maxTimestamp_ = std::max(maxTimestamp_, timestamp);
        ++recordCount_;
    }

    std::vector<uint8_t> build() {
        using namespace batch;
        std::vector<uint8_t> b;
        detail::appendBE64(b, baseOffset_);                 // 0
        detail::appendBE32(b, 0);                           // 8 length (patched)
        detail::appendBE32(b, -1);                          // 12 partitionLeaderEpoch (unset)
        b.push_back(static_cast<uint8_t>(kMagicValue));     // 16
        detail::appendBE32(b, 0);                           // 17 CRC (patched)
        detail::appendBE16(b, 0);                           // 21 attributes
        detail::appendBE32(b, recordCount_ - 1);            // 23 lastOffsetDelta
        detail::appendBE64(b, baseTimestamp_);              // 27
        detail::appendBE64(b, maxTimestamp_);               // 35
        detail::appendBE64(b, -1);                          // 43 producerId
        detail::appendBE16(b, -1);                          // 51 producerEpoch
        detail::appendBE32(b, -1);                          // 53 baseSequence
        detail::appendBE32(b, recordCount_);                // 57 recordCount
        b.insert(b.end(), records_.begin(), records_.end());// 61 records

        detail::putBE(b, kLengthOffset, static_cast<uint32_t>(b.size() - 12));
        uint32_t crc = crc32c(b.data() + kAttributesOffset, b.size() - kAttributesOffset);
        detail::putBE(b, kCrcOffset, crc);
        return b;
    }
    int32_t recordCount() const { return recordCount_; }

private:
    int64_t baseOffset_, baseTimestamp_;
    int64_t maxTimestamp_ = baseTimestamp_;
    int32_t recordCount_ = 0;
    std::vector<uint8_t> records_;
};

class RecordBatchReader {
public:
    RecordBatchReader(const uint8_t* data, size_t len) : d_(data), len_(len) {}
    int64_t baseOffset() const { return static_cast<int64_t>(detail::getBE64(d_ + batch::kBaseOffsetOffset)); }
    int32_t lastOffsetDelta() const { return detail::getBE32(d_ + batch::kLastOffsetDeltaOffset); }
    int32_t recordCount() const { return detail::getBE32(d_ + batch::kRecordCountOffset); }
    int32_t totalLength() const { return static_cast<int32_t>(detail::getBE32(d_ + batch::kLengthOffset)) + 12; }
    int64_t baseTimestamp() const { return static_cast<int64_t>(detail::getBE64(d_ + batch::kBaseTimestampOffset)); }

    bool ensureValid() const {
        if (len_ < static_cast<size_t>(batch::kRecordBatchOverhead)) return false;
        uint32_t stored = detail::getBE32(d_ + batch::kCrcOffset);
        uint32_t actual = crc32c(d_ + batch::kAttributesOffset, totalLength() - batch::kAttributesOffset);
        return stored == actual;
    }

    void forEach(const std::function<void(std::string_view,std::string_view,int64_t,int64_t)>& fn) const {
        size_t pos = batch::kRecordBatchOverhead;
        int64_t base = baseOffset(); int64_t baseTs = baseTimestamp();
        for (int32_t i = 0; i < recordCount(); ++i) {
            (void)readVarint(d_, pos);            // record length
            ++pos;                                // attributes
            int64_t tsDelta = readVarlong(d_, pos);
            int32_t offDelta = readVarint(d_, pos);
            int32_t klen = readVarint(d_, pos);
            std::string_view key(reinterpret_cast<const char*>(d_ + pos), klen<0?0:klen); if (klen>0) pos += klen;
            int32_t vlen = readVarint(d_, pos);
            std::string_view val(reinterpret_cast<const char*>(d_ + pos), vlen<0?0:vlen); if (vlen>0) pos += vlen;
            int32_t hcount = readVarint(d_, pos); (void)hcount; // Phase 1: headers unused
            fn(key, val, base + offDelta, baseTs + tsDelta);
        }
    }

    // Stamp partitionLeaderEpoch in place. This field is EXCLUDED from the CRC,
    // so the broker sets it on arrival without recomputing the checksum.
    static void stampLeaderEpoch(uint8_t* data, int32_t epoch) {
        for (int i = 0; i < 4; ++i)
            data[batch::kPartitionLeaderEpochOffset + i] = static_cast<uint8_t>(epoch >> ((3 - i) * 8));
    }
private:
    const uint8_t* d_; size_t len_;
};
} // namespace rune::protocol
