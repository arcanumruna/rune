#pragma once
// ---------------------------------------------------------------------------
// RecordsPush.h — send one Records push frame (broker → consumer) on an
// ExclusivePublication.  Shared by both push paths: the gateway (inline and
// worker-return) and the per-worker egress (shard mode).
//
// F19: a stored batch of ANY size up to the publication's maxMessageLength is
// deliverable; nothing is ever skipped silently.
//
//   frame <= maxPayloadLength (one MTU fragment) — the hot path, unchanged:
//       encode the whole frame into `scratch`, tryClaim, copy, commit.
//   maxPayloadLength < frame <= maxMessageLength:
//       encode ONLY the fixed part (header + body + u32 batch length, 52 B)
//       into `scratch` and publish [fixed | batch] as a 2-buffer vectored
//       offer — the batch bytes are copied exactly once, from the log view into
//       the term buffer (zero intermediate copy, no scratch-size limit).  Aeron
//       fragments it; the client's FragmentAssembler reassembles.
//   frame > maxMessageLength:
//       TooLarge — undeliverable on this publication.  The caller must fail
//       the subscription explicitly (never advance past the batch).
//
// Sent / Retry carry the Aeron result: Retry (BACK_PRESSURED, NOT_CONNECTED,
// ADMIN_ACTION, ...) keeps the caller's retry-from-the-same-cursor semantics.
//
// No allocation, no locks/atomics, no virtual calls: the vectored offer goes
// through the C++ client's templated iterator overload over a std::array.
// ---------------------------------------------------------------------------
#include <rune/protocol/client/ControlMessages.h>
#include <rune/protocol/client/DataMessages.h>
#include <rune/protocol/client/Protocol.h>

#include <ExclusivePublication.h>
#include <concurrent/AtomicBuffer.h>
#include <concurrent/logbuffer/BufferClaim.h>

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <string_view>

namespace rune::node {

enum class RecordsSendKind : uint8_t { Sent, Retry, TooLarge };

struct RecordsSendResult {
    RecordsSendKind kind     = RecordsSendKind::Retry;
    int64_t         position = 0;   ///< Aeron result for Sent/Retry
};

/// Fixed part of a Records frame: MessageHeader + BODY_FIXED + u32 batch length.
inline constexpr std::size_t kRecordsFixedBytes =
    rune::protocol::client::MessageHeader::ENCODED_SIZE +
    rune::protocol::client::Records::BODY_FIXED + 4u;

/// Encode `rec` (recordBatch = zero-copy span into the log) and send it on
/// `pub`.  `scratch` must hold at least kRecordsFixedBytes; frames up to
/// min(scratchSize, maxPayloadLength) take the single-fragment tryClaim path.
inline RecordsSendResult sendRecordsFrame(aeron::ExclusivePublication&           pub,
                                          const rune::protocol::client::Records& rec,
                                          uint8_t* scratch, std::size_t scratchSize)
{
    namespace pc = rune::protocol::client;
    const std::size_t frameLen = rec.encodedSize();

    if (frameLen <= static_cast<std::size_t>(pub.maxPayloadLength()) && frameLen <= scratchSize)
        [[likely]] {
        const std::size_t encoded = rec.encode(scratch);
        aeron::concurrent::logbuffer::BufferClaim claim;
        int64_t pos = pub.tryClaim(static_cast<aeron::util::index_t>(encoded), claim);
        if (pos == aeron::ADMIN_ACTION) [[unlikely]]   // A9: term rotated — retry now (Aeron contract)
            pos = pub.tryClaim(static_cast<aeron::util::index_t>(encoded), claim);
        if (pos > 0) {
            aeron::concurrent::AtomicBuffer src(scratch, static_cast<aeron::util::index_t>(encoded));
            claim.buffer().putBytes(claim.offset(), src, 0,
                                    static_cast<aeron::util::index_t>(encoded));
            claim.commit();
            return {RecordsSendKind::Sent, pos};
        }
        return {RecordsSendKind::Retry, pos};
    }

    if (frameLen > static_cast<std::size_t>(pub.maxMessageLength())) [[unlikely]]
        return {RecordsSendKind::TooLarge, 0};

    // Fixed part only: encode with an empty batch, then patch the u32 length.
    pc::Records fixed = rec;
    fixed.recordBatch = {};
    const std::size_t fixedLen = fixed.encode(scratch);   // == kRecordsFixedBytes
    pc::detail::putLE32(scratch, static_cast<int>(fixedLen - 4u),
                        static_cast<int32_t>(static_cast<uint32_t>(rec.recordBatch.size())));

    const std::array<aeron::concurrent::AtomicBuffer, 2> parts{
        aeron::concurrent::AtomicBuffer(scratch, static_cast<aeron::util::index_t>(fixedLen)),
        aeron::concurrent::AtomicBuffer(const_cast<uint8_t*>(rec.recordBatch.data()),
                                        static_cast<aeron::util::index_t>(rec.recordBatch.size()))};
    int64_t pos = pub.offer(parts);
    if (pos == aeron::ADMIN_ACTION) [[unlikely]] pos = pub.offer(parts);   // A9: term rotated — retry now
    return {pos > 0 ? RecordsSendKind::Sent : RecordsSendKind::Retry, pos};
}

/// Upper bound of encodePushTooLargeError's output (header + code + message).
inline constexpr std::size_t kPushTooLargeErrorMaxBytes =
    rune::protocol::client::MessageHeader::ENCODED_SIZE +
    rune::protocol::client::ErrorResponse::BODY_FIXED + 2u + 192u;

/// Why a push subscription failed (integration wave 2: F19 ⇄ F15 S3 — one
/// failure state, two error paths, one error encoding).
enum class PushTooLargeCause : uint8_t {
    EgressFrame,   ///< F19: the Records frame exceeds the egress maxMessageLength
    ReturnRing,    ///< F15 S3 I-4: a stored batch exceeds the worker return ring
};

/// Encode the error that fails a push subscription whose next batch at
/// `atOffset` cannot be delivered: EgressFrame — its Records frame (`frameLen`
/// B) exceeds the egress maxMessageLength `maxMessage`; ReturnRing — the owning
/// worker answered its PushRead MessageTooLarge (the stored batch exceeds the
/// worker return ring; `frameLen` / `maxMessage` unused).  F15 S3 convention
/// (re-review 2 m-f): correlationId = 0 — a subscription id shares no namespace
/// with client correlation ids (the client would fail an unrelated pending
/// ping/connect with the same number); the subscription is named in the
/// message "subscription S topic T partition P offset O: ...".  `buf` must hold
/// kPushTooLargeErrorMaxBytes.  No allocation.
inline std::size_t encodePushTooLargeError(uint8_t* buf, int64_t sessionId, int64_t subscriptionId,
                                           int32_t topicId, int32_t partition, int64_t atOffset,
                                           std::size_t frameLen, int64_t maxMessage,
                                           PushTooLargeCause cause = PushTooLargeCause::EgressFrame) noexcept
{
    namespace pc = rune::protocol::client;
    char msg[192];
    const int m = cause == PushTooLargeCause::EgressFrame
        ? std::snprintf(msg, sizeof msg,
              "subscription %lld topic %d partition %d offset %lld: Records frame %zu B exceeds the "
              "egress max message %lld B",
              static_cast<long long>(subscriptionId), topicId, partition,
              static_cast<long long>(atOffset), frameLen, static_cast<long long>(maxMessage))
        : std::snprintf(msg, sizeof msg,
              "subscription %lld topic %d partition %d offset %lld: batch exceeds the broker return ring",
              static_cast<long long>(subscriptionId), topicId, partition,
              static_cast<long long>(atOffset));
    pc::ErrorResponse err;
    err.sessionId     = sessionId;
    err.correlationId = 0;
    err.code          = static_cast<int16_t>(pc::ErrorCode::MessageTooLarge);
    err.message       = std::string_view(
        msg, m > 0 ? static_cast<std::size_t>(m < static_cast<int>(sizeof msg) ? m : sizeof msg - 1) : 0u);
    return err.encode(buf);
}

} // namespace rune::node
