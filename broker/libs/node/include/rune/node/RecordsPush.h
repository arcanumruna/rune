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
#include <rune/protocol/client/DataMessages.h>

#include <ExclusivePublication.h>
#include <concurrent/AtomicBuffer.h>
#include <concurrent/logbuffer/BufferClaim.h>

#include <array>
#include <cstddef>
#include <cstdint>

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
        const int64_t pos = pub.tryClaim(static_cast<aeron::util::index_t>(encoded), claim);
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
    const int64_t pos = pub.offer(parts);
    return {pos > 0 ? RecordsSendKind::Sent : RecordsSendKind::Retry, pos};
}

} // namespace rune::node
