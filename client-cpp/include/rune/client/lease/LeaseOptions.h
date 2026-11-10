// include/rune/client/lease/LeaseOptions.h - the knobs of a lease write and of a LeaseTopic (Task 14a).
#pragma once

#include <cstdint>

namespace rune::client {

/// Per-call options of a lease write or request.  A zero field takes the topic's default.
struct LeaseOptions {
    uint64_t ttlMs      = 0;   ///< grant only: the lease ttl; 0 = the topic's lease.ttl
    uint64_t holder     = 0;   ///< grant only: the holder id written into the row (0 = none)
    uint32_t opTimeoutMs = 0;  ///< the call's own deadline; 0 = the handle's opTimeout
};

/// Per-handle options of Client::openLeaseTopicAndWait.
struct LeaseTopicConfig {
    /// The deadline of every write and request of the handle that has no LeaseOptions::opTimeoutMs.  0 = lease.ttl / 3
    /// (floor 1 ms).  A write that is not answered within it completes Timeout (a deviation from the 30 s delivery default:
    /// a lease write that is slower than a third of its ttl is already too late to matter).
    uint32_t opTimeoutMs = 0;
};

}  // namespace rune::client
