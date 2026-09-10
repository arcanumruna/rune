#pragma once
// Faithful C++ port of Aeron's ElectionState enum.
// Source: aeron-cluster/src/main/java/io/aeron/cluster/ElectionState.java:26-115
#include <cstdint>
#include <stdexcept>

namespace rune::consensus {

// ElectionState.java:26-115
// All 18 ordinal codes match Java exactly (code == ordinal).
enum class ElectionState : int32_t {
    Init                 = 0,   // ElectionState.java:30
    Canvass              = 1,   // ElectionState.java:35
    Nominate             = 2,   // ElectionState.java:40
    CandidateBallot      = 3,   // ElectionState.java:45
    FollowerBallot       = 4,   // ElectionState.java:50
    LeaderLogReplication = 5,   // ElectionState.java:55
    LeaderReplay         = 6,   // ElectionState.java:60
    LeaderInit           = 7,   // ElectionState.java:65
    LeaderReady          = 8,   // ElectionState.java:70
    FollowerLogReplication = 9, // ElectionState.java:75
    FollowerReplay       = 10,  // ElectionState.java:80
    FollowerCatchupInit  = 11,  // ElectionState.java:85
    FollowerCatchupAwait = 12,  // ElectionState.java:90
    FollowerCatchup      = 13,  // ElectionState.java:95
    FollowerLogInit      = 14,  // ElectionState.java:100
    FollowerLogAwait     = 15,  // ElectionState.java:105
    FollowerReady        = 16,  // ElectionState.java:110
    Closed               = 17,  // ElectionState.java:115
};

// Returns the integer code stored in the counter (== ordinal).
// Mirrors ElectionState.code() / ElectionState.java:136-139.
inline int32_t code(ElectionState s) noexcept {
    return static_cast<int32_t>(s);
}

// Converts a code read from a counter back to ElectionState.
// Mirrors ElectionState.get(long) / ElectionState.java:147-155.
// Throws std::out_of_range for invalid codes.
inline ElectionState fromCode(int32_t c) {
    if (c < 0 || c > 17) {
        throw std::out_of_range("invalid election state counter code: " + std::to_string(c));
    }
    return static_cast<ElectionState>(c);
}

} // namespace rune::consensus
