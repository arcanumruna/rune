#pragma once
// IdleShape.h — A7: the shard-worker backoff shape (ParkUntilWorkIdle.h; derived by
// NodeConfig::shardIdleShape).  Counts are IDLE PASSES: one idle() call follows one duty pass.
#include <cstdint>

namespace rune::node {

struct IdleShape {
    int32_t maxSpins  = 10;
    int32_t maxYields = 20;
    int64_t minParkNs = 1'000;
    int64_t maxParkNs = 200'000;
    friend bool operator==(const IdleShape&, const IdleShape&) = default;
};

} // namespace rune::node
