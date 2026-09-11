#pragma once
// ---------------------------------------------------------------------------
// PartitionReplay.h — Replays recorded batches from a RecordingStore to feed
//                     follower catch-up or cold recovery.
//
// Namespace: rune::replication
// C++23. No locks. No busy-spin. Bounded / deterministic.
//
// ## Overview (spec §6.5, §8, plan P3-4)
//
// PartitionReplay reads committed, recorded batches from a RecordingStore and
// delivers them to a sink in offset order.  It is used for:
//
//   a) Cold follower bootstrap: a follower with an empty log is seeded by
//      PartitionReplay up to the leader's log-start, then transitions to live
//      fetch replication via PartitionReplica::applyReplicated().
//
//   b) Recovery: on restart, a node replays its own Recording to reconstruct
//      the committed log tail.
//
// ## API
//
//   replay(fromOffset, toOffset, sink) — replay batches in [fromOffset, toOffset).
//
// The sink receives each batch's (baseOffset, bytes) in strict order.
// replay() is bounded and deterministic: it reads at most toOffset - fromOffset
// offsets and never blocks.
//
// ## Follower cold catch-up (integration with PartitionReplica)
//
// After replay() seeds a follower's Log up to leaderLogStart, the follower
// transitions to live replication.  The replayed offsets are guaranteed
// contiguous with what the leader currently holds, ensuring no gap.
// ---------------------------------------------------------------------------

#include <rune/replication/RecordingStore.h>
#include <rune/storage/Log.h>

#include <cstdint>
#include <functional>

namespace rune::replication {

// ---------------------------------------------------------------------------
// ReplaySink — callback for each replayed batch.
// Returns false to abort the replay early.
// ---------------------------------------------------------------------------
using ReplaySink = std::function<bool(int64_t baseOffset, std::span<const uint8_t> bytes)>;

// ---------------------------------------------------------------------------
// PartitionReplay
// ---------------------------------------------------------------------------
class PartitionReplay {
public:
    // -----------------------------------------------------------------------
    // Construction
    // -----------------------------------------------------------------------

    /// \param store  Non-owning pointer to the RecordingStore to replay from.
    explicit PartitionReplay(const RecordingStore* store);

    ~PartitionReplay() = default;

    // Non-copyable.
    PartitionReplay(const PartitionReplay&)            = delete;
    PartitionReplay& operator=(const PartitionReplay&) = delete;

    // -----------------------------------------------------------------------
    // Replay API
    // -----------------------------------------------------------------------

    /// Replay recorded batches in [fromOffset, toOffset) to the sink.
    ///
    /// \param fromOffset  First offset to include (inclusive).
    /// \param toOffset    Exclusive end of the range (pass INT64_MAX for all).
    /// \param sink        Callback for each batch.  Return false to stop early.
    ///
    /// \returns Number of batches delivered.
    ///
    /// If no batches fall in the range, returns 0 and the sink is never called.
    /// Byte-exact: the bytes delivered to the sink are the same bytes that were
    /// appended to the RecordingStore.
    int64_t replay(int64_t fromOffset, int64_t toOffset,
                   const ReplaySink& sink) const;

    /// Convenience: replay all recorded batches from the beginning.
    int64_t replayAll(const ReplaySink& sink) const;

    /// Convenience: replay into a Log by calling log->append() for each batch.
    ///
    /// \param log         Target Log (e.g. an empty follower's Log).
    /// \param fromOffset  First offset to replay.
    /// \param toOffset    Exclusive end offset.
    ///
    /// \returns Number of batches appended.
    int64_t replayIntoLog(rune::storage::Log& log,
                          int64_t fromOffset, int64_t toOffset) const;

private:
    const RecordingStore* store_;   ///< non-owning
};

} // namespace rune::replication
