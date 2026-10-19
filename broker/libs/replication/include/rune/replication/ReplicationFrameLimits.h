#pragma once
// ReplicationFrameLimits.h — F18: how many record bytes ONE replication frame can
// carry.  A FetchResponse crosses two bounded hops, each a hard per-message limit:
//   1. the AeronPartitionTransport worker→control outbound ring (agrona
//      ManyToOne: maxMsgLength = capacity/8), wrapped in an OutboundFrame envelope;
//   2. the replication publication (Aeron maxMessageLength = min(term/8, 16 MiB)).
// Since every serve path sends the first batch WHOLE (KIP-74), the largest batch
// the broker accepts must fit both — NodeConfig::produceIntakeLimit() bounds intake
// (MessageTooLarge above it) by these limits.  The transport's own per-request ceilings use the
// same constants (single source of truth: the generated codecs).
//
// Authentication Task 16 (D34): every inter-broker frame may be sealed (+ kPeerFrameOverhead = 32 bytes: a 16-byte
// header and a 16-byte tag), so EVERY limit below subtracts those 32 bytes in EVERY mode - a limit never changes
// with the crypto mode, and the largest frame a limit admits, sealed, still fits the hop it crosses.
#include <rune/peerlink/PeerFrameCrypto.h>      // kPeerFrameOverhead (authentication Task 16, D34)
#include <rune/replication/ReplicationMessages.h>
#include <rune_protocol_internal_sbe/MessageHeader.h>
#include <rune_protocol_internal_sbe/OutboundFrame.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>

namespace rune::replication {

/// Fixed overhead of the SBE OutboundFrame envelope (SBE header + targetNodeId
/// block + varData length prefix) around a replication message in the outbound ring.
inline constexpr std::size_t kOutboundEnvelopeOverhead =
    ::rune::protocol::internal::sbe::MessageHeader::encodedLength() +
    ::rune::protocol::internal::sbe::OutboundFrame::sbeBlockLength() +
    sizeof(std::uint32_t);

/// Aeron's absolute per-message cap (FrameDescriptor::MAX_MESSAGE_LENGTH).
inline constexpr int64_t kAeronMaxMessageLength = 16 * 1024 * 1024;

/// Aeron publication maxMessageLength for a term length (computeMaxMessageLength).
[[nodiscard]] inline int64_t aeronMaxMessageLength(int64_t termBufferBytes) noexcept {
    return std::min<int64_t>(termBufferBytes / 8, kAeronMaxMessageLength);
}

/// Records bytes one FetchResponse can carry through the outbound ring hop.
[[nodiscard]] inline int64_t maxFetchRecordsViaOutboundRing(int64_t outboundRingBytes) noexcept {
    return std::max<int64_t>(0, outboundRingBytes / 8
                                    - static_cast<int64_t>(kOutboundEnvelopeOverhead)
                                    - static_cast<int64_t>(rune::peerlink::kPeerFrameOverhead)
                                    - static_cast<int64_t>(FetchResponseMsg::encodedSize(0)));
}

/// Records bytes one FetchResponse can carry on a replication publication.
[[nodiscard]] inline int64_t maxFetchRecordsViaPublication(int64_t termBufferBytes) noexcept {
    return std::max<int64_t>(0, aeronMaxMessageLength(termBufferBytes)
                                    - static_cast<int64_t>(rune::peerlink::kPeerFrameOverhead)
                                    - static_cast<int64_t>(FetchResponseMsg::encodedSize(0)));
}

} // namespace rune::replication
