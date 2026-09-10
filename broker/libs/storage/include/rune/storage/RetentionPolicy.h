#pragma once
#include <cstdint>

namespace rune::storage {

/// Retention policy for log segments.
///
/// Phase 1 (current): Size-based retention via deleteOldSegments().
/// Phase 4 (future): Time-based retention will be added via segment first-batch baseTimestamp.
struct RetentionPolicy {
    int64_t retainBytes;    ///< Keep at least this many bytes of log data (entire segments).
    int64_t retainMillis;   ///< Keep at least this many milliseconds of log data (Phase 4).
};

} // namespace rune::storage
