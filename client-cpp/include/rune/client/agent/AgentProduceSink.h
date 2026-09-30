// include/rune/client/agent/AgentProduceSink.h — the production produce sink: a
// plain (inlinable) member call into the agent's partition router.  Replaces the
// std::function SendFn rebuilt every doWork (spec §4.3).
#pragma once
#include <rune/client/agent/ClientAgent.h>

namespace rune::client::agent {
struct AgentProduceSink {
    ClientAgent* agent = nullptr;
    bool operator()(int32_t topicId, int32_t partition, int32_t ingressStreamId,
                    const uint8_t* buf, std::size_t len) const noexcept {
        return agent->offerToPartition(topicId, partition, ingressStreamId, buf, len);
    }
};
} // namespace rune::client::agent
