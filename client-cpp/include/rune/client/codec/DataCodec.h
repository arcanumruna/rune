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

#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>

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
    int64_t sessionId     = 0;
    int64_t correlationId = 0;
    int16_t errorCode     = 0;  // rpc::ErrorCode value
    int32_t partition     = 0;
    int64_t baseOffset    = 0;
};

/// Encode a ProduceResponse frame (38 bytes) into buf[0..cap-1].
/// Returns 38 on success, or 0 if cap < 38.
inline std::size_t encodeProduceResponse(uint8_t*    buf,
                                          std::size_t cap,
                                          int16_t     errorCode,
                                          int32_t     partition,
                                          int64_t     baseOffset,
                                          int64_t     sessionId,
                                          int64_t     correlationId) noexcept {
    if (cap < rpc::ProduceResponse::ENCODED_SIZE) return 0u;
    rpc::ProduceResponse msg;
    msg.sessionId     = sessionId;
    msg.correlationId = correlationId;
    msg.errorCode     = errorCode;
    msg.partition     = partition;
    msg.baseOffset    = baseOffset;
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
    v.partition     = opt->partition;
    v.baseOffset    = opt->baseOffset;
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
    auto opt = rpc::Subscribe::decode(buf, len);
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
    std::span<const uint8_t> recordBatch;  // zero-copy view into the original buffer

    // Number of records in this batch: parsed from recordBatch header using
    // RecordBatchReader.  Correct advance = baseOffset + recordCount.
    // Defaults to 1 for backward compatibility when recordBatch is empty/missing.
    int32_t                  recordCount    = 1;
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
    // Parse record count from RecordBatch header (little-endian at offset 57).
    // RecordBatchReader requires at least kRecordBatchOverhead (61) bytes.
    if (v.recordBatch.size() >= static_cast<std::size_t>(
            rune::protocol::batch::kRecordBatchOverhead)) {
        rune::protocol::RecordBatchReader reader(v.recordBatch.data(),
                                                  v.recordBatch.size());
        const int32_t cnt = reader.recordCount();
        v.recordCount = (cnt > 0) ? cnt : 1;
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
