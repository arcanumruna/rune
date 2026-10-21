#pragma once
// GroupMetadata.h — issue #125 (KIP-447): the consumer-group member a transaction's offsets belong to.
//
// GroupConsumer::groupMetadata() returns it; Producer::sendOffsetsToTransaction(offsets, GroupMetadata, ...)
// sends its generation and member id with the TxnOffsetCommit (client version 10), and the group coordinator
// fences it exactly like an OffsetCommit: a stale generation (the group rebalanced) answers IllegalGeneration, a
// member it does not know - or one bound to another session - UnknownMember.  Both fail the transaction (abortable):
// abort it, let the consumer rejoin, and re-process from the committed offsets.  The member is bound to the session
// that joined it, so the producer must run on the SAME Client as the GroupConsumer.

#include <cstdint>
#include <string>

namespace rune::client {

/// The longest member id a GroupMetadata carries (a broker member id is "<group>-<seq>-<32 hex>"; the transactional
/// path caps group ids at 255 bytes).  A longer one completes sendOffsetsToTransaction InvalidArgument.
inline constexpr std::size_t kMaxGroupMemberIdBytes = 512;

struct GroupMetadata {
    std::string groupId;
    int32_t     generationId = -1;   ///< -1: not joined yet (no member block is sent: the legacy, unfenced form)
    std::string memberId;
};

}  // namespace rune::client
