#pragma once
// ---------------------------------------------------------------------------
// Types.h — Core type aliases for the rune::replication data-plane library.
//
// Namespace: rune::replication
// C++23
// ---------------------------------------------------------------------------

#include <cstdint>

namespace rune::replication {

/// Partition identifier (matches Kafka's int32 partition id).
using PartitionId = int32_t;

/// Absolute log offset (byte position or record offset, monotonically
/// increasing per partition). Matches Kafka's int64 offset semantics.
using Offset = int64_t;

/// Leader epoch — monotonically increasing integer bumped by the control
/// plane each time a new partition leader is elected. Used for fencing:
/// stale leaders (lower epoch) are rejected. Analogous to Kafka's
/// "partitionLeaderEpoch" field in the RecordBatch header (§6.4).
using Epoch = int64_t;

/// Node identifier — identifies a broker node within the cluster.
using NodeId = int32_t;

/// CP-1: per-partition log cleanup policy.  Mirrors the values of
/// rune::node::TopicSpec::CleanupPolicy exactly (kept as a separate,
/// self-contained enum here so the replication layer does not depend on
/// the higher-level node layer).  A future LogCleaner scheduler consults
/// this via PartitionReplica::cleanupPolicy().
enum class CleanupPolicy : uint8_t { Delete = 0, Compact = 1, CompactDelete = 2 };

/// A lightweight reference to a record batch's base offset.
/// The actual byte payload is managed separately (e.g. in-memory ring buffer
/// or Aeron term buffer); this struct is used by the replication layer to
/// track position without owning the bytes.
struct RecordBatchRef {
    Offset baseOffset;  ///< First offset of this batch in the partition log.
};

} // namespace rune::replication
