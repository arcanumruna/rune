// rune/client/codec/DataCodec.h
//
// Thin typed adapter layer over the rune-protocol produce/subscribe codecs.
// Namespace: rune::client::codec
//
// Design contract:
//   - ZERO re-implementation of the wire format.  All encoding/decoding is
//     delegated to the rune-protocol DataMessages headers.
//   - Bounds-checked: every call goes through the underlying codec that already
//     returns std::nullopt on truncation or schema/templateId mismatch.
//   - Zero-alloc-friendly: caller supplies all buffers; no heap allocation here.
//   - Zero-copy views: span fields are views into the caller-supplied buffer;
//     the buffer must outlive any View returned.
//   - Header-only.
//
// CB-1: extends the CC-2 codec adapter pattern (see Codec.h) for the six
// produce/subscribe data messages: templateIds 16-21.

#pragma once

#include <algorithm>
#include <climits>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <string_view>

// Pull in the rune-protocol data codecs (header-only, sibling include path
// injected into the build by CMakeLists.txt).
#include <rune/protocol/client/DataMessages.h>
#include <rune/protocol/client/MessageHeader.h>
#include <rune/protocol/client/Protocol.h>
#include <rune/protocol/RecordBatch.h>  // RecordBatchReader (for recordCount in RecordsView)

namespace rune::client::codec {

namespace rpc = rune::protocol::client;

// ---------------------------------------------------------------------------
// ProduceView — decoded Produce fields
// ---------------------------------------------------------------------------
struct ProduceView {
    int64_t                   sessionId      = 0;
    int64_t                   correlationId  = 0;
    int32_t                   topicId        = 0;
    int32_t                   partition      = 0;
    uint8_t                   partitionKnown = 0;
    uint8_t                   acks           = 0;  // GR-5: 0=Leader/None, 1=All
    int64_t                   producerId     = -1; // frame-level idempotent identity (-1 = none)
    int16_t                   producerEpoch  = 0;
    int32_t                   baseSequence   = -1;
    std::span<const uint8_t>  key;          // zero-copy view into the original buffer
    std::span<const uint8_t>  recordBatch;  // zero-copy view into the original buffer
};

/// Encode a Produce frame into buf[0..cap-1].
/// key and recordBatch are zero-copy spans into caller-owned data.
/// Returns total bytes written, or 0 if cap is insufficient.
inline std::size_t encodeProduce(uint8_t*                  buf,
                                  std::size_t               cap,
                                  int32_t                   topicId,
                                  int32_t                   partition,
                                  uint8_t                   partitionKnown,
                                  std::span<const uint8_t>  key,
                                  std::span<const uint8_t>  recordBatch,
                                  int64_t                   sessionId,
                                  int64_t                   correlationId) noexcept {
    rpc::Produce msg;
    msg.sessionId      = sessionId;
    msg.correlationId  = correlationId;
    msg.topicId        = topicId;
    msg.partition      = partition;
    msg.partitionKnown = partitionKnown;
    msg.key            = key;
    msg.recordBatch    = recordBatch;
    const std::size_t needed = msg.encodedSize();
    if (cap < needed) return 0u;
    return msg.encode(buf);
}

/// Decode a Produce frame from buf[0..len-1].
/// Returns nullopt on truncation, schema mismatch, or templateId mismatch.
/// Returned spans are zero-copy views into buf — buf must outlive the result.
inline std::optional<ProduceView> decodeProduce(const uint8_t* buf,
                                                  std::size_t    len) noexcept {
    auto opt = rpc::Produce::decode(buf, len);
    if (!opt) return std::nullopt;
    ProduceView v;
    v.sessionId      = opt->sessionId;
    v.correlationId  = opt->correlationId;
    v.topicId        = opt->topicId;
    v.partition      = opt->partition;
    v.partitionKnown = opt->partitionKnown;
    v.acks           = opt->acks;
    v.producerId     = opt->producerId;
    v.producerEpoch  = opt->producerEpoch;
    v.baseSequence   = opt->baseSequence;
    v.key            = opt->key;
    v.recordBatch    = opt->recordBatch;
    return v;
}

// ---------------------------------------------------------------------------
// ProduceResponseView — decoded ProduceResponse fields
// ---------------------------------------------------------------------------
struct ProduceResponseView {
    int64_t sessionId      = 0;
    int64_t correlationId  = 0;
    int16_t errorCode      = 0;  // rpc::ErrorCode value
    int32_t partition      = 0;
    int64_t baseOffset     = 0;
    int64_t logStartOffset = -1; // D6: the partition's log start (-1 = not carried / old broker)
};

/// Encode a ProduceResponse frame (rpc::ProduceResponse::ENCODED_SIZE = 46
/// bytes) into buf[0..cap-1].  Returns the size, or 0 if cap is too small.
inline std::size_t encodeProduceResponse(uint8_t*    buf,
                                          std::size_t cap,
                                          int16_t     errorCode,
                                          int32_t     partition,
                                          int64_t     baseOffset,
                                          int64_t     sessionId,
                                          int64_t     correlationId,
                                          int64_t     logStartOffset = -1) noexcept {
    if (cap < rpc::ProduceResponse::ENCODED_SIZE) return 0u;
    rpc::ProduceResponse msg;
    msg.sessionId      = sessionId;
    msg.correlationId  = correlationId;
    msg.errorCode      = errorCode;
    msg.partition      = partition;
    msg.baseOffset     = baseOffset;
    msg.logStartOffset = logStartOffset;
    return msg.encode(buf);
}

/// Decode a ProduceResponse frame from buf[0..len-1].
/// Returns nullopt on truncation, schema mismatch, or templateId mismatch.
inline std::optional<ProduceResponseView> decodeProduceResponse(const uint8_t* buf,
                                                                  std::size_t    len) noexcept {
    auto opt = rpc::ProduceResponse::decode(buf, len);
    if (!opt) return std::nullopt;
    ProduceResponseView v;
    v.sessionId     = opt->sessionId;
    v.correlationId = opt->correlationId;
    v.errorCode     = opt->errorCode;
    v.partition      = opt->partition;
    v.baseOffset     = opt->baseOffset;
    v.logStartOffset = opt->logStartOffset;
    return v;
}

// ---------------------------------------------------------------------------
// SubscribeView — decoded Subscribe fields
// ---------------------------------------------------------------------------
struct SubscribeView {
    int64_t                  sessionId      = 0;
    int64_t                  correlationId  = 0;
    int32_t                  topicId        = 0;
    int32_t                  partition      = 0;
    uint8_t                  partitionKnown = 0;
    int64_t                  fromOffset     = 0;
    uint8_t                  isolation      = 0;
    std::span<const uint8_t> key;  // zero-copy view into the original buffer
};

/// Encode a Subscribe frame into buf[0..cap-1].
/// key is a zero-copy span into caller-owned data.
/// Returns total bytes written, or 0 if cap is insufficient.
inline std::size_t encodeSubscribe(uint8_t*                  buf,
                                    std::size_t               cap,
                                    int32_t                   topicId,
                                    int32_t                   partition,
                                    uint8_t                   partitionKnown,
                                    int64_t                   fromOffset,
                                    uint8_t                   isolation,
                                    std::span<const uint8_t>  key,
                                    int64_t                   sessionId,
                                    int64_t                   correlationId) noexcept {
    rpc::Subscribe msg;
    msg.sessionId      = sessionId;
    msg.correlationId  = correlationId;
    msg.topicId        = topicId;
    msg.partition      = partition;
    msg.partitionKnown = partitionKnown;
    msg.fromOffset     = fromOffset;
    msg.isolation      = isolation;
    msg.key            = key;
    const std::size_t needed = msg.encodedSize();
    if (cap < needed) return 0u;
    return msg.encode(buf);
}

/// Decode a Subscribe frame from buf[0..len-1].
/// Returns nullopt on truncation, schema mismatch, or templateId mismatch.
/// Returned span is a zero-copy view into buf — buf must outlive the result.
inline std::optional<SubscribeView> decodeSubscribe(const uint8_t* buf,
                                                     std::size_t    len) noexcept {
    auto opt = rpc::Subscribe::decode(buf, len, rpc::kClientVersion);
    if (!opt) return std::nullopt;
    SubscribeView v;
    v.sessionId      = opt->sessionId;
    v.correlationId  = opt->correlationId;
    v.topicId        = opt->topicId;
    v.partition      = opt->partition;
    v.partitionKnown = opt->partitionKnown;
    v.fromOffset     = opt->fromOffset;
    v.isolation      = opt->isolation;
    v.key            = opt->key;
    return v;
}

// ---------------------------------------------------------------------------
// SubscribeResponseView — decoded SubscribeResponse fields
// ---------------------------------------------------------------------------
struct SubscribeResponseView {
    int64_t sessionId      = 0;
    int64_t correlationId  = 0;
    int16_t errorCode      = 0;  // rpc::ErrorCode value
    int32_t partition      = 0;
    int64_t subscriptionId = 0;
};

/// Encode a SubscribeResponse frame (38 bytes) into buf[0..cap-1].
/// Returns 38 on success, or 0 if cap < 38.
inline std::size_t encodeSubscribeResponse(uint8_t*    buf,
                                            std::size_t cap,
                                            int16_t     errorCode,
                                            int32_t     partition,
                                            int64_t     subscriptionId,
                                            int64_t     sessionId,
                                            int64_t     correlationId) noexcept {
    if (cap < rpc::SubscribeResponse::ENCODED_SIZE) return 0u;
    rpc::SubscribeResponse msg;
    msg.sessionId      = sessionId;
    msg.correlationId  = correlationId;
    msg.errorCode      = errorCode;
    msg.partition      = partition;
    msg.subscriptionId = subscriptionId;
    return msg.encode(buf);
}

/// Decode a SubscribeResponse frame from buf[0..len-1].
/// Returns nullopt on truncation, schema mismatch, or templateId mismatch.
inline std::optional<SubscribeResponseView> decodeSubscribeResponse(const uint8_t* buf,
                                                                      std::size_t    len) noexcept {
    auto opt = rpc::SubscribeResponse::decode(buf, len);
    if (!opt) return std::nullopt;
    SubscribeResponseView v;
    v.sessionId      = opt->sessionId;
    v.correlationId  = opt->correlationId;
    v.errorCode      = opt->errorCode;
    v.partition      = opt->partition;
    v.subscriptionId = opt->subscriptionId;
    return v;
}

// ---------------------------------------------------------------------------
// RecordsView — decoded Records (push) fields
// ---------------------------------------------------------------------------
struct RecordsView {
    int64_t                  sessionId      = 0;
    int64_t                  correlationId  = 0;
    int64_t                  subscriptionId = 0;
    int32_t                  topicId        = 0;
    int32_t                  partition      = 0;
    int64_t                  baseOffset     = 0;
    // Zero-copy view into the original buffer.  ONE whole batch, or — Task 13 fix
    // round 2, the whole class (DataMessages.h) — SEVERAL whole batches back to
    // back.  Code that walks it itself must walk every batch (forEachRecord does).
    std::span<const uint8_t> recordBatch;

    // Number of records in the frame (every batch): parsed from the batch headers
    // using RecordBatchReader.  Defaults to 1 for backward compatibility when
    // recordBatch is empty/missing (test stubs).
    int32_t                  recordCount    = 1;
    // The frame's last offset minus baseOffset: the last batch's last offset is
    // baseOffset + lastOffsetDelta — compaction can leave holes INSIDE a batch, so
    // the next offset is baseOffset + lastOffsetDelta + 1, not baseOffset +
    // recordCount.  Defaults to recordCount - 1 (no batch header).
    int32_t                  lastOffsetDelta = 0;
    [[nodiscard]] int64_t nextOffset() const noexcept {
        return baseOffset + static_cast<int64_t>(lastOffsetDelta) + 1;
    }
    // Consumer groups (E2 Task 13 fix round 2): brokers push WHOLE batches; a
    // batch that straddles the member's position (a resume after a leader
    // move, a mid-batch committed offset) carries records already consumed.
    // Records with offset < deliverFrom are such re-deliveries (0 = the whole
    // batch is new).  Set by GroupConsumerHub; never on the wire.
    // forEachRecord() skips them for you; only code that walks `recordBatch`
    // itself (raw-batch access) must honour deliverFrom.
    int64_t                  deliverFrom     = 0;
    // Task 13 fix round 3 (M2-r2): -1 = not validated; >= 0 = the valid prefix of
    // recordBatch, this many bytes, already CRC-checked (set by GroupConsumerHub:
    // forEachRecord walks only it, without a second CRC pass).  Never on the wire.
    int32_t                  validBatchBytes = -1;

    /// Visit every NEW record of every batch: fn(key, value, offset, timestamp)
    /// for each record with offset >= deliverFrom — the records a straddling
    /// re-delivery carries below the consumer's position are never handed over
    /// (E2 final fix wave, I4).  A control batch (transaction marker) is never
    /// visited; the walk stops at the first malformed batch (an empty or
    /// malformed frame visits nothing).  Templated (the visitor inlines; no
    /// allocation).  The spans are valid only for the duration of the callback
    /// that received this view.
    template <class Fn>
    void forEachRecord(Fn&& fn) const {
        const int64_t from = deliverFrom;
        const bool validated = validBatchBytes >= 0;
        std::span<const uint8_t> rest =
            validated ? recordBatch.first(std::min<std::size_t>(static_cast<std::size_t>(validBatchBytes), recordBatch.size()))
                      : recordBatch;
        while (rest.size() >= static_cast<std::size_t>(rune::protocol::batch::kRecordBatchOverhead)) {
            rune::protocol::RecordBatchReader reader(rest.data(), rest.size());
            if (validated) {   // CRC-checked already: only the lengths are checked again
                const int32_t tl = reader.totalLength();
                if (tl < rune::protocol::batch::kRecordBatchOverhead || static_cast<std::size_t>(tl) > rest.size()) return;
            } else if (!reader.ensureValid()) {
                return;
            }
            if (!reader.isControl()) {
                reader.forEachRecord([&](std::string_view key, std::string_view value, int64_t offset, int64_t ts) {
                    if (offset < from) [[unlikely]] return;   // already consumed (straddling re-delivery)
                    fn(key, value, offset, ts);
                });
            }
            rest = rest.subspan(static_cast<std::size_t>(reader.totalLength()));
        }
    }
};

/// Encode a Records frame into buf[0..cap-1].
/// recordBatch is a zero-copy span into caller-owned data.
/// Returns total bytes written, or 0 if cap is insufficient.
inline std::size_t encodeRecords(uint8_t*                  buf,
                                  std::size_t               cap,
                                  int64_t                   subscriptionId,
                                  int32_t                   topicId,
                                  int32_t                   partition,
                                  int64_t                   baseOffset,
                                  std::span<const uint8_t>  recordBatch,
                                  int64_t                   sessionId,
                                  int64_t                   correlationId) noexcept {
    rpc::Records msg;
    msg.sessionId      = sessionId;
    msg.correlationId  = correlationId;
    msg.subscriptionId = subscriptionId;
    msg.topicId        = topicId;
    msg.partition      = partition;
    msg.baseOffset     = baseOffset;
    msg.recordBatch    = recordBatch;
    const std::size_t needed = msg.encodedSize();
    if (cap < needed) return 0u;
    return msg.encode(buf);
}

/// Decode a Records frame from buf[0..len-1].
/// Returns nullopt on truncation, schema mismatch, or templateId mismatch.
/// Returned span is a zero-copy view into buf — buf must outlive the result.
/// recordCount is parsed from the recordBatch header (RecordBatchReader.recordCount());
/// if the batch is too short to read (e.g. empty/stub batch in tests), defaults to 1.
inline std::optional<RecordsView> decodeRecords(const uint8_t* buf,
                                                  std::size_t    len) noexcept {
    auto opt = rpc::Records::decode(buf, len);
    if (!opt) return std::nullopt;
    RecordsView v;
    v.sessionId      = opt->sessionId;
    v.correlationId  = opt->correlationId;
    v.subscriptionId = opt->subscriptionId;
    v.topicId        = opt->topicId;
    v.partition      = opt->partition;
    v.baseOffset     = opt->baseOffset;
    v.recordBatch    = opt->recordBatch;
    // Parse the record count / last offset from the batch headers (little-endian;
    // RecordBatchReader requires at least kRecordBatchOverhead (61) bytes).  Every
    // whole batch of the frame counts (Task 13 fix round 2): a frame of several
    // batches spans to its LAST batch's last offset.  The walk stops at a batch
    // whose length does not fit the frame (a stub / torn tail).
    namespace pb = rune::protocol::batch;
    std::span<const uint8_t> rest = v.recordBatch;
    bool any = false;
    int64_t count = 0, lastAbs = 0;
    while (rest.size() >= static_cast<std::size_t>(pb::kRecordBatchOverhead)) {
        rune::protocol::RecordBatchReader reader(rest.data(), rest.size());
        const int32_t cnt = reader.recordCount();
        const int32_t lod = reader.lastOffsetDelta();
        const int64_t c   = cnt > 0 ? cnt : 1;
        const int64_t abs = reader.baseOffset() + std::max<int64_t>(lod, c - 1);
        if (!any) {
            // The first batch: as before (a stub keeps its own header fields).
            v.recordCount     = static_cast<int32_t>(c);
            v.lastOffsetDelta = static_cast<int32_t>(std::max<int64_t>(lod, c - 1));
        }
        const int32_t tl = reader.totalLength();
        if (tl < pb::kRecordBatchOverhead || static_cast<std::size_t>(tl) > rest.size()) break;
        any = true;
        count += c;
        lastAbs = abs;
        rest = rest.subspan(static_cast<std::size_t>(tl));
    }
    if (any && count > v.recordCount) {   // several batches: the frame-wide fields
        v.recordCount     = static_cast<int32_t>(std::min<int64_t>(count, INT32_MAX));
        v.lastOffsetDelta = static_cast<int32_t>(std::clamp<int64_t>(lastAbs - v.baseOffset, 0, INT32_MAX));
    }
    // else: recordCount stays 1 (default — empty/stub batch in tests)
    return v;
}

// ---------------------------------------------------------------------------
// UnsubscribeView — decoded Unsubscribe fields
// ---------------------------------------------------------------------------
struct UnsubscribeView {
    int64_t sessionId      = 0;
    int64_t correlationId  = 0;
    int64_t subscriptionId = 0;
};

/// Encode an Unsubscribe frame (32 bytes) into buf[0..cap-1].
/// Returns 32 on success, or 0 if cap < 32.
inline std::size_t encodeUnsubscribe(uint8_t*    buf,
                                      std::size_t cap,
                                      int64_t     subscriptionId,
                                      int64_t     sessionId,
                                      int64_t     correlationId) noexcept {
    if (cap < rpc::Unsubscribe::ENCODED_SIZE) return 0u;
    rpc::Unsubscribe msg;
    msg.sessionId      = sessionId;
    msg.correlationId  = correlationId;
    msg.subscriptionId = subscriptionId;
    return msg.encode(buf);
}

/// Decode an Unsubscribe frame from buf[0..len-1].
/// Returns nullopt on truncation, schema mismatch, or templateId mismatch.
inline std::optional<UnsubscribeView> decodeUnsubscribe(const uint8_t* buf,
                                                          std::size_t    len) noexcept {
    auto opt = rpc::Unsubscribe::decode(buf, len);
    if (!opt) return std::nullopt;
    UnsubscribeView v;
    v.sessionId      = opt->sessionId;
    v.correlationId  = opt->correlationId;
    v.subscriptionId = opt->subscriptionId;
    return v;
}

} // namespace rune::client::codec
