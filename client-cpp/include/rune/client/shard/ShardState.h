// include/rune/client/shard/ShardState.h — the derived shard lifecycle state
// (T15b; design note docs/superpowers/specs/2026-10-01-t15-clientshard-design.md §4).
//
// The state is DERIVED, not stored: each duty cycle the shard's duty thread
// (Invoker owner / Agent runner) computes it from the Client's DutyView — the
// seed session (Connect + Metadata), every routed leader session, the transport
// and the producer — and publishes it only when it changes.  Pure function.
//
//   Starting      constructed; no duty cycle has run yet
//   Idle          started; never connected (or the first connect failed)
//   Connecting    the seed's first connect is in flight
//   Live          seed Connected AND every routed leader session Connected
//   Degraded      seed Connected, >= 1 routed leader session not Connected
//                 (downMask: which); those partitions wait, the rest flow
//   Reconnecting  the seed was Live and is reconnecting (attempt = its Backoff
//                 attempt); it never gives up (T15a R1).  Batches stay put.
//   Draining      the producer's intake is closed (begin-close): flushing and
//                 awaiting the acks of what it accepted (then teardown)
//   Failed        TransportClosed (the Aeron client closed) | Rejected (a non-
//                 retriable broker answer to the seed, rejectCode).  Every
//                 accepted record has completed Closed; no further I/O.
//   Closed        torn down (close / closeImmediately / destructor)
//
// The state may go back (Live <-> Degraded <-> Reconnecting); only Failed and
// Closed are terminal.
#pragma once

#include <rune/client/DutyView.h>

#include <cstdint>

namespace rune::client::shard {

enum class ShardState : uint8_t { Starting, Idle, Connecting, Live, Degraded, Reconnecting, Draining, Failed, Closed };
enum class ShardFailure : uint8_t { None, TransportClosed, Rejected };

/// One published shard status (trivially copyable: it travels through a StatusRing).
struct ShardStatus {
    int64_t      sessionId  = 0;  ///< the seed session id (0 = none)
    uint64_t     downMask   = 0;  ///< Degraded: routed leader sessions not Connected
    uint32_t     shard      = 0;  ///< the shard index
    int32_t      attempt    = 0;  ///< Reconnecting: the seed's Backoff attempt
    int16_t      rejectCode = 0;  ///< Failed{Rejected}: the broker's code
    ShardState   state      = ShardState::Starting;
    ShardFailure failure    = ShardFailure::None;
    uint8_t      pad[4]{};
};

[[nodiscard]] constexpr const char* toString(ShardState s) noexcept {
    switch (s) {
    case ShardState::Starting:     return "Starting";
    case ShardState::Idle:         return "Idle";
    case ShardState::Connecting:   return "Connecting";
    case ShardState::Live:         return "Live";
    case ShardState::Degraded:     return "Degraded";
    case ShardState::Reconnecting: return "Reconnecting";
    case ShardState::Draining:     return "Draining";
    case ShardState::Failed:       return "Failed";
    case ShardState::Closed:       return "Closed";
    }
    return "?";
}

/// The shard's state from one duty cycle's view (pure).  Precedence: Failed
/// (transport, then a Rejected seed) > Draining > session-derived.
[[nodiscard]] inline ShardStatus deriveShardStatus(const DutyView& v, uint32_t shard) noexcept {
    using SS = session::SessionState;
    ShardStatus s;
    s.shard     = shard;
    s.sessionId = v.seedSessionId;
    if (v.transportFailed) {
        s.state   = ShardState::Failed;
        s.failure = ShardFailure::TransportClosed;
        return s;
    }
    if (v.seedState == SS::Rejected) {
        s.state      = ShardState::Failed;
        s.failure    = ShardFailure::Rejected;
        s.rejectCode = v.seedRejectCode;
        return s;
    }
    if (v.producerClosing) {
        s.state = ShardState::Draining;
        return s;
    }
    if (v.seedState == SS::Connected) {
        s.downMask = v.leadersDownMask;
        s.state    = v.leadersDownMask != 0 ? ShardState::Degraded : ShardState::Live;
        return s;
    }
    if (v.seedEverLive && v.seedState != SS::Closed) {
        s.state   = ShardState::Reconnecting;
        s.attempt = v.seedBackoffAttempt;
        return s;
    }
    s.state = (v.seedState == SS::Connecting || v.seedState == SS::Resuming) ? ShardState::Connecting
                                                                             : ShardState::Idle;
    return s;
}

/// Equal as far as a subscriber cares (publish only on change).
[[nodiscard]] constexpr bool sameStatus(const ShardStatus& a, const ShardStatus& b) noexcept {
    return a.state == b.state && a.failure == b.failure && a.downMask == b.downMask &&
           a.sessionId == b.sessionId && a.attempt == b.attempt && a.rejectCode == b.rejectCode &&
           a.shard == b.shard;
}

} // namespace rune::client::shard
