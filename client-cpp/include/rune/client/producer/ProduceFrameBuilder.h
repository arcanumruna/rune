// include/rune/client/producer/ProduceFrameBuilder.h — the Tier-1 partition
// accumulator that builds the Produce WIRE FRAME in place (backlog B1 + B4).
//
// Layout of frame():
//
//   [0, kProduceHeaderLen)                    Produce header (written by seal())
//   [kProduceHeaderLen, +kRecordBatchOverhead) RecordBatch header (written by seal())
//   [kBatchStart + 61, end)                   records, appended in place
//
// append() writes each record ONCE, straight into the frame; seal() writes only
// the two headers (~110 bytes) and leaves the RecordBatch CRC unset — the
// transport computes it once Aeron accepted the frame (transport/FrameCrc.h).
// A back-pressured offer therefore re-runs seal() (headers only) on the next
// attempt: no record copy, no CRC.  On an accepted acks=Leader/All offer the
// producer SWAPS frame() with the in-flight slot's retained buffer (O(1)) — the
// retained retransmit frame is the encoded frame, never a copy (the old path
// copied the records three times: builder -> encode scratch -> retained frame).
//
// Record encoding is byte-identical to rune::protocol::RecordBatchBuilder
// (Kafka v2 record, zigzag varints) — conformance test:
// tests/producer/ProduceFrameBuilderTest.cpp.  Single-threaded (duty thread).
#pragma once

#include <rune/client/transport/FrameCrc.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/protocol/Varint.h>
#include <rune/protocol/client/DataMessages.h>
#include <rune/protocol/client/MessageHeader.h>

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string_view>
#include <vector>

namespace rune::client::producer {

class ProduceFrameBuilder {
public:
    /// Produce header with an empty key: header + body fixed + u16 key len + u32 batch len.
    static constexpr std::size_t kProduceHeaderLen =
        ::rune::protocol::client::MessageHeader::ENCODED_SIZE +
        ::rune::protocol::client::Produce::BODY_FIXED + 2u + 4u;
    static constexpr std::size_t kBatchStart   = kProduceHeaderLen;
    static constexpr std::size_t kRecordsStart =
        kBatchStart + static_cast<std::size_t>(::rune::protocol::batch::kRecordBatchOverhead);

    /// Upper bound of one record's framing bytes beyond key + value: length
    /// varint (5) + attributes (1) + tsDelta varlong (10) + offsetDelta (5) +
    /// keyLen (5) + valueLen (5) + headerCount (1).
    static constexpr std::size_t kRecordOverheadMax = 32;

    ProduceFrameBuilder(int64_t baseOffset, int64_t baseTimestamp) { reset(baseOffset, baseTimestamp); }

    /// Empty batch; keeps frame() capacity (no per-batch allocation).
    void reset(int64_t baseOffset, int64_t baseTimestamp) noexcept {
        baseOffset_    = baseOffset;
        baseTimestamp_ = baseTimestamp;
        maxTimestamp_  = baseTimestamp;
        recordCount_   = 0;
        producerId_    = -1;
        producerEpoch_ = -1;
        baseSequence_  = -1;
        frame_.resize(kRecordsStart);  // header space only (shrink: no fill)
    }

    void setProducerId(int64_t pid) noexcept { producerId_ = pid; }
    void setProducerEpoch(int16_t ep) noexcept { producerEpoch_ = ep; }
    void setBaseSequence(int32_t seq) noexcept { baseSequence_ = seq; }

    /// Kafka v2 record: length attrs tsDelta offsetDelta keyLen key valueLen value
    /// headerCount(0) — the exact bytes RecordBatchBuilder::append produces, but
    /// written once, without its reserve-and-memmove.
    void append(std::string_view key, std::string_view value, int64_t timestamp) {
        const auto* v = reinterpret_cast<const uint8_t*>(value.data());
        (void)writePrefix_(key, value.size(), timestamp);
        frame_.insert(frame_.end(), v, v + value.size());
        frame_.push_back(0);  // headerCount = varint(0)
        noteRecord_(timestamp);
    }

    /// Phase 2b Part B (Invoker claims): append one record whose `valueLen`
    /// value bytes the caller writes later, in place, at data() + the returned
    /// offset.  Byte-identical to append() once the value is written.  Call
    /// reserveExtra() first when data() must not move (it then cannot throw).
    std::size_t reserveRecord(std::string_view key, std::size_t valueLen, int64_t timestamp) {
        const std::size_t at = writePrefix_(key, valueLen, timestamp);
        frame_.resize(at + valueLen + 1);
        frame_[at + valueLen] = 0;  // headerCount = varint(0)
        noteRecord_(timestamp);
        return at;
    }

    /// Capacity for `n` more bytes: appends / reservations within it never
    /// reallocate (so they never throw and data() never moves).
    void reserveExtra(std::size_t n) { frame_.reserve(frame_.size() + n); }

    /// A point to roll back to (an aborted / abandoned claim).
    struct Mark {
        std::size_t size         = 0;
        int32_t     recordCount  = 0;
        int64_t     maxTimestamp = 0;
    };
    [[nodiscard]] Mark mark() const noexcept { return Mark{frame_.size(), recordCount_, maxTimestamp_}; }
    void rollback(const Mark& m) noexcept {
        frame_.resize(m.size);  // shrink only: never allocates
        recordCount_  = m.recordCount;
        maxTimestamp_ = m.maxTimestamp;
    }

    [[nodiscard]] int32_t recordCount() const noexcept { return recordCount_; }
    /// Encoded record bytes so far (RecordBatchBuilder::bodyBytes parity).
    [[nodiscard]] std::size_t bodyBytes() const noexcept { return frame_.size() - kRecordsStart; }

    /// Writes the Produce header (`msg`, whose key/recordBatch must be empty) and
    /// the RecordBatch header; the batch CRC field is left 0.  Returns the frame
    /// length.  Idempotent: a refused offer re-seals with fresh header fields.
    std::size_t seal(::rune::protocol::client::Produce msg) noexcept {
        namespace rb  = ::rune::protocol::batch;
        namespace rpd = ::rune::protocol::client::detail;
        msg.key         = {};
        msg.recordBatch = {};
        uint8_t* f = frame_.data();
        (void)msg.encode(f);
        const std::size_t batchLen = frame_.size() - kBatchStart;
        rpd::putLE32(f, static_cast<int>(kProduceHeaderLen) - 4, static_cast<int32_t>(batchLen));

        uint8_t* b = f + kBatchStart;
        putLE(b + rb::kBaseOffsetOffset, static_cast<uint64_t>(baseOffset_), 8);
        putLE(b + rb::kLengthOffset, static_cast<uint64_t>(batchLen - 12), 4);
        putLE(b + rb::kPartitionLeaderEpochOffset, 0xFFFFFFFFull, 4);  // -1
        b[rb::kMagicOffset] = static_cast<uint8_t>(rb::kMagicValue);
        putLE(b + rb::kCrcOffset, 0, 4);                                // sealed on accept
        putLE(b + rb::kAttributesOffset, 0, 2);
        putLE(b + rb::kLastOffsetDeltaOffset, static_cast<uint32_t>(recordCount_ - 1), 4);
        putLE(b + rb::kBaseTimestampOffset, static_cast<uint64_t>(baseTimestamp_), 8);
        putLE(b + rb::kMaxTimestampOffset, static_cast<uint64_t>(maxTimestamp_), 8);
        putLE(b + rb::kProducerIdOffset, static_cast<uint64_t>(producerId_), 8);
        putLE(b + rb::kProducerEpochOffset, static_cast<uint16_t>(producerEpoch_), 2);
        putLE(b + rb::kBaseSequenceOffset, static_cast<uint32_t>(baseSequence_), 4);
        putLE(b + rb::kRecordCountOffset, static_cast<uint32_t>(recordCount_), 4);
        return frame_.size();
    }

    /// Where the transport seals the CRC of a seal()ed frame.
    [[nodiscard]] static constexpr transport::FrameCrc frameCrc() noexcept {
        return transport::FrameCrc::forBatchAt(kBatchStart);
    }

    [[nodiscard]] uint8_t* data() noexcept { return frame_.data(); }
    [[nodiscard]] std::size_t size() const noexcept { return frame_.size(); }

    /// Hands the sealed frame to `retained` (an in-flight slot's buffer) and takes
    /// that buffer's storage for the next batch — O(1), no copy.  Leaves the
    /// builder reset (empty batch, same baseOffset / timestamps unchanged until
    /// the caller's reset()).
    void swapFrameInto(std::vector<uint8_t>& retained) {
        frame_.swap(retained);
        // Warm-up only: the slot's previous buffer may be smaller than a batch.
        if (frame_.capacity() < retained.capacity()) frame_.reserve(retained.capacity());
        frame_.resize(kRecordsStart);
        recordCount_ = 0;
    }

private:
    /// [length varint | attrs | tsDelta | offsetDelta | keyLen | key | valueLen]
    /// — the exact bytes RecordBatchBuilder::append produces before the value;
    /// returns the frame offset where the value goes.
    std::size_t writePrefix_(std::string_view key, std::size_t valueLen, int64_t timestamp) {
        using ::rune::protocol::writeVarint;
        using ::rune::protocol::writeVarlong;
        uint8_t pre[48];
        std::size_t n = 0;
        pre[n++] = 0;  // attributes
        n += writeVarlong(timestamp - baseTimestamp_, pre + n);
        n += writeVarint(recordCount_, pre + n);
        n += writeVarint(static_cast<int32_t>(key.size()), pre + n);
        uint8_t vlen[10];
        const std::size_t vn = writeVarint(static_cast<int32_t>(valueLen), vlen);
        const std::size_t bodySize = n + key.size() + vn + valueLen + 1;  // +1 headerCount
        uint8_t blen[10];
        const std::size_t bn = writeVarint(static_cast<int32_t>(bodySize), blen);
        const auto* k = reinterpret_cast<const uint8_t*>(key.data());
        frame_.insert(frame_.end(), blen, blen + bn);
        frame_.insert(frame_.end(), pre, pre + n);
        frame_.insert(frame_.end(), k, k + key.size());
        frame_.insert(frame_.end(), vlen, vlen + vn);
        return frame_.size();
    }
    void noteRecord_(int64_t timestamp) noexcept {
        if (timestamp > maxTimestamp_) maxTimestamp_ = timestamp;
        ++recordCount_;
    }

    static void putLE(uint8_t* p, uint64_t v, int bytes) noexcept {
        for (int i = 0; i < bytes; ++i) p[i] = static_cast<uint8_t>(v >> (8 * i));
    }

    std::vector<uint8_t> frame_;
    int64_t baseOffset_    = 0;
    int64_t baseTimestamp_ = 0;
    int64_t maxTimestamp_  = 0;
    int32_t recordCount_   = 0;
    int64_t producerId_    = -1;
    int16_t producerEpoch_ = -1;
    int32_t baseSequence_  = -1;
};

}  // namespace rune::client::producer
