// include/rune/client/DutyView.h — T15b: what one duty cycle observed about a
// Client's sessions, transport and producers.  The input of the (pure) shard
// state derivation, shard/ShardState.h.  Plain values; no Aeron.
#pragma once

#include <rune/client/session/SessionMachine.h>

#include <cstdint>

namespace rune::client {

struct DutyView {
    session::SessionState seedState          = session::SessionState::Disconnected;
    bool                  seedEverLive       = false;  ///< the seed was Connected at least once
    int16_t               seedRejectCode     = 0;      ///< seed Rejected: the broker's code
    int64_t               seedSessionId      = 0;
    int32_t               seedBackoffAttempt = 0;
    uint32_t              leaderSessions     = 0;  ///< routed leader-node sessions (besides the seed)
    uint64_t              leadersDownMask    = 0;  ///< bit i: leader session i not Connected (first 64)
    bool                  transportFailed    = false;  ///< the Aeron client closed: the Client failed for good
    bool                  producerClosing    = false;  ///< a producer's intake is closed (begin-close / close)
};

} // namespace rune::client
