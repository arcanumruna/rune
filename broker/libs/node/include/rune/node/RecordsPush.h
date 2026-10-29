#pragma once
// ---------------------------------------------------------------------------
// RecordsPush.h — send one Records push frame (broker → consumer) on an
// ExclusivePublication.  Shared by both push paths: the gateway (inline and
// worker-return) and the per-worker egress (shard mode).
//
// F19: a stored batch of ANY size up to the publication's maxMessageLength is
// deliverable; nothing is ever skipped silently.
//
// PLAINTEXT session (no RecordsSealCtx passed — byte-identical to pre-Task-8b):
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
// ENCRYPTED session (Task 8b — a non-null RecordsSealCtx* is passed): the
// zero-copy vectored offer above CANNOT survive — AEAD sealing materializes
// the batch ciphertext, so there is nothing left to offer zero-copy. Instead:
//   1. assemble the WHOLE plaintext frame ([24B header][body][u32 batchLen]
//      [batch], i.e. exactly rec.encode()'s normal output) contiguously into
//      `sealScratch->plain` (grow-once, resized on demand up to frameLen);
//   2. seal it via rune::node::sealRecordsFrame (RecordsSeal.h — the Task 8
//      envelope: [24B header][12B CryptoSubHeader][ciphertext][16B tag],
//      AAD = the 36 clear bytes) into `sealScratch->sealed` (grow-once, up
//      to frameLen + kCryptoOverhead); drop (RecordsSendKind::Retry, never
//      transmit) on seal()==0;
//   3. publish the single CONTIGUOUS sealed buffer: tryClaim when it fits
//      one MTU (pub.maxPayloadLength()), else pub.offer() a single buffer —
//      Aeron fragments a >MTU contiguous buffer automatically, the client's
//      FragmentAssembler reassembles it exactly as it does the plaintext
//      vectored offer.
//   TooLarge boundary shrinks by kCryptoOverhead (28): undeliverable when
//   frameLen + 28 > maxMessageLength (checked BEFORE any scratch growth).
//
// Sent / Retry carry the Aeron result: Retry (BACK_PRESSURED, NOT_CONNECTED,
// ADMIN_ACTION, ...) keeps the caller's retry-from-the-same-cursor semantics.
//
// No allocation, no locks/atomics, no virtual calls on the PLAINTEXT path:
// the vectored offer is aeron_exclusive_publication_offerv over two stack
// iovecs (the C++ wrapper's vectored offer() builds a std::vector per call).
// The ENCRYPTED path's two scratch buffers are
// grow-once (never shrink; resize() only grows capacity, and only up to
// maxMessageLength-ish frame sizes ever seen) and caller-owned (shared-
// nothing — each thread that calls sendRecordsFrame owns its own
// RecordsSealScratch; see ClientGateway's recordsSealScratch_).
// ---------------------------------------------------------------------------
#include <rune/node/RecordsSeal.h>
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
#include <vector>

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

/// Task 8b — grow-once scratch for the ENCRYPTED Records path (see the file
/// header comment).  `plain` holds the contiguous plaintext assembly
/// (rec.encode()'s normal output, up to frameLen bytes); `sealed` holds the
/// seal output (up to frameLen + kCryptoOverhead bytes).  Caller-owned,
/// reused across calls (resize() only grows; a bad_alloc in the try/catch at
/// the call site drops the frame rather than throwing).  Shared-nothing: one
/// instance per thread that calls sendRecordsFrame (the gateway's duty-cycle
/// thread; each shard worker's own instance since Task 19 — never shared
/// across threads).
struct RecordsSealScratch {
    std::vector<uint8_t> plain;
    std::vector<uint8_t> sealed;
};

/// Encode `rec` (recordBatch = zero-copy span into the log) and send it on
/// `pub`.  `scratch` must hold at least kRecordsFixedBytes; frames up to
/// min(scratchSize, maxPayloadLength) take the single-fragment tryClaim path.
///
/// `seal` is null for a PLAINTEXT session (the default — byte-identical to
/// pre-Task-8b behavior below). Non-null means "this session is encrypted":
/// `sealScratch` must also be non-null in that case (caller contract — see
/// RecordsSealScratch's doc comment); a non-null `seal` whose `send`/
/// `sendSeq` are themselves null (misconfigured — key not installed) fails
/// closed via sealRecordsFrame()'s own 0-return contract, never falling back
/// to plaintext.
inline RecordsSendResult sendRecordsFrame(aeron::ExclusivePublication&           pub,
                                          const rune::protocol::client::Records& rec,
                                          uint8_t* scratch, std::size_t scratchSize,
                                          const RecordsSealCtx* seal = nullptr,
                                          RecordsSealScratch*   sealScratch = nullptr)
{
    namespace pc = rune::protocol::client;
    const std::size_t frameLen = rec.encodedSize();

    // ---- ENCRYPTED path (Task 8b) --------------------------------------
    // Zero-copy vectored offer cannot survive AEAD sealing (it materializes
    // the batch ciphertext) — assemble the contiguous plaintext, seal it
    // into a grow-once scratch, publish the single contiguous sealed
    // buffer. The PLAINTEXT branches below are untouched by this branch.
    if (seal) [[unlikely]] {
        if (!sealScratch) return {RecordsSendKind::Retry, 0};  // misuse — never transmit plaintext

        const std::size_t sealedLen = frameLen + pc::kCryptoOverhead;
        if (sealedLen > static_cast<std::size_t>(pub.maxMessageLength())) [[unlikely]]
            return {RecordsSendKind::TooLarge, 0};  // boundary shrinks by kCryptoOverhead (28)

        try {
            if (sealScratch->plain.size() < frameLen) sealScratch->plain.resize(frameLen);
            if (sealScratch->sealed.size() < sealedLen) sealScratch->sealed.resize(sealedLen);
        } catch (...) {
            return {RecordsSendKind::Retry, 0};  // scratch exhausted — drop, never transmit
        }

        rec.encode(sealScratch->plain.data());  // contiguous: header|body|u32 batchLen|batch

        const std::size_t n = sealRecordsFrame(
            *seal, sealScratch->plain.data(), frameLen,
            sealScratch->sealed.data(), sealScratch->sealed.size());
        if (n == 0) return {RecordsSendKind::Retry, 0};  // seal failure — never transmit

        if (n <= static_cast<std::size_t>(pub.maxPayloadLength())) {
            aeron::concurrent::logbuffer::BufferClaim claim;
            int64_t pos = pub.tryClaim(static_cast<aeron::util::index_t>(n), claim);
            if (pos == aeron::ADMIN_ACTION) [[unlikely]]   // A9: term rotated — retry now
                pos = pub.tryClaim(static_cast<aeron::util::index_t>(n), claim);
            if (pos > 0) {
                aeron::concurrent::AtomicBuffer src(sealScratch->sealed.data(),
                                                    static_cast<aeron::util::index_t>(n));
                claim.buffer().putBytes(claim.offset(), src, 0,
                                        static_cast<aeron::util::index_t>(n));
                claim.commit();
                return {RecordsSendKind::Sent, pos};
            }
            return {RecordsSendKind::Retry, pos};
        }

        // n > maxPayloadLength but <= maxMessageLength (checked above): a
        // single CONTIGUOUS offer — Aeron fragments it automatically.
        aeron::concurrent::AtomicBuffer sealedAb(sealScratch->sealed.data(),
                                                 static_cast<aeron::util::index_t>(n));
        int64_t pos = pub.offer(sealedAb);
        if (pos == aeron::ADMIN_ACTION) [[unlikely]] pos = pub.offer(sealedAb);   // A9
        return {pos > 0 ? RecordsSendKind::Sent : RecordsSendKind::Retry, pos};
    }

    // ---- PLAINTEXT path: byte-identical to pre-Task-8b -----------------
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

    aeron_iovec_t parts[2] = {{scratch, fixedLen}, {const_cast<uint8_t*>(rec.recordBatch.data()), rec.recordBatch.size()}};
    int64_t pos = aeron_exclusive_publication_offerv(pub.publication(), parts, 2, nullptr, nullptr);
    if (pos == aeron::ADMIN_ACTION) [[unlikely]]   // A9: term rotated — retry now
        pos = aeron_exclusive_publication_offerv(pub.publication(), parts, 2, nullptr, nullptr);
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

/// Issue #109: the terminal frame of a subscription its push owner ENDS (a revoked READ): ErrorResponse{code,
/// correlationId = the client's Subscribe correlation id, `detail` truncated to 192 bytes}.  The worker that pushes the
/// subscription's Records sends it on the same record egress after its last Records, so no Records frame can follow
/// it.  `buf` must hold kPushTooLargeErrorMaxBytes.  No allocation.
inline std::size_t encodeSubscriptionEnd(uint8_t* buf, int64_t sessionId, int64_t subscribeCorrId, int16_t code,
                                         std::string_view detail) noexcept
{
    namespace pc = rune::protocol::client;
    pc::ErrorResponse err;
    err.sessionId     = sessionId;
    err.correlationId = subscribeCorrId;
    err.code          = code;
    err.message       = detail.substr(0, 192);
    return err.encode(buf);
}

} // namespace rune::node
