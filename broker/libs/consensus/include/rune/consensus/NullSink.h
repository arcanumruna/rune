#pragma once
// NullSink.h — no-op outbound consensus transport (single-node: no peers).
// Final, so SbeConsensusPublisher dispatches to it directly (Task 11 fix 2).
#include <rune/consensus/MessageSink.h>

#include <cstddef>
#include <cstdint>

namespace rune::consensus {

class NullSink final : public MessageSink {
public:
    void send(NodeId, const uint8_t*, std::size_t) override {}
};

} // namespace rune::consensus
