#pragma once
// MessageSink.h — Abstract transport sink for outbound consensus messages.
//
// SbeConsensusPublisher encodes into its reusable buffer and calls send().
// P2P-8 maps this to aeron ExclusivePublication::tryClaim per peer.
// Tests use an in-memory LoopbackSink.

#include <rune/consensus/Types.h>
#include <cstdint>
#include <cstddef>

namespace rune::consensus {

struct MessageSink {
    virtual ~MessageSink() = default;

    /// Send len bytes from buf to the given cluster member.
    virtual void send(NodeId toMember, const uint8_t* buf, std::size_t len) = 0;
};

} // namespace rune::consensus
