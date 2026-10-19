#pragma once
// ---------------------------------------------------------------------------
// EpochFencing.h — Pure leader-epoch fencing helpers for the replication layer.
//
// Namespace: rune::replication
// C++23, header-only, no allocation.
//
// ## Fencing invariant (spec §8)
//
// Every partition carries a monotonically increasing **leader epoch** that is
// bumped by the control plane each time a new leader is elected.  The epoch is
// stamped into every record batch on arrival (partitionLeaderEpoch in the batch
// header, outside the CRC so the broker can stamp it without recomputing CRC).
//
// This creates a strict timeline:
//   - A batch with epoch E can only have been committed by the leader that held
//     epoch E when the batch was produced.
//   - A newly elected leader starts with epoch E+1 > E; it can NEVER commit
//     batches from a stale leader (epoch E) without truncating them first.
//   - Any replica/leader that observes a request from an epoch LOWER than its
//     own rejects the request (fenced stale leader).
//   - Any replica/leader that observes an epoch HIGHER than its own must
//     step down and adopt the higher epoch (it has been superseded).
//
// ## Truncation on leader change (follower rejoin, spec §8 + §4 recovery)
//
// When a follower learns of a new leader / new epoch, it must truncate its log
// to remove entries that may not be on the new leader's timeline.  These are
// entries written by a *stale* leader whose batches were never acked (they
// exist on this replica but not on the new leader's committed prefix).
//
// The LeaderEpochCache (libs/storage) maps `epoch → startOffset`.
// `truncateOffsetForEpoch(e)` returns the startOffset of the FIRST epoch > e,
// i.e. the boundary where entries from epochs >= (e+1) begin — exactly where
// uncommitted tail entries from stale epochs start.
//
// Given a follower's LeaderEpochCache and a `newLeaderEpoch`:
//   - Entries from epochs < newLeaderEpoch on the follower's timeline are safe
//     (the new leader's history matches up to the old leader's last epoch).
//   - Entries from epochs >= newLeaderEpoch may not be on the new leader's
//     timeline (written by a stale leader that was fenced).  The follower
//     truncates to truncateOffsetForEpoch(newLeaderEpoch - 1), which is the
//     start of any epoch >= newLeaderEpoch on this follower.
//   - If the follower has no epoch >= newLeaderEpoch, truncation is not needed
//     (returns -1 meaning "truncate nothing").
//
// This is the core of Kafka's LEADER_EPOCH_OFFSET_CHECK recovery protocol.
// ---------------------------------------------------------------------------

#include <cstdint>
#include <rune/replication/Types.h>
#include <rune/storage/LeaderEpochCache.h>

namespace rune::replication {

// ---------------------------------------------------------------------------
// Epoch comparison helpers
// ---------------------------------------------------------------------------

/// True if `requestEpoch` is stale relative to the local node's epoch.
///
/// A stale epoch means the request came from a leader/replica that has been
/// superseded.  The receiver MUST reject the request (not apply it).
///
/// \param requestEpoch  Epoch carried in the incoming request.
/// \param localEpoch    Epoch this node currently recognises as authoritative.
/// \returns true → reject (fenced); false → epoch is current or newer.
[[nodiscard]] inline bool isStaleEpoch(Epoch requestEpoch,
                                        Epoch localEpoch) noexcept
{
    return requestEpoch < localEpoch;
}

/// True if the observed epoch is higher than the local epoch, meaning this
/// node has been superseded and must step down.
///
/// When a node receives a message from a peer with a HIGHER epoch, it knows
/// a new leader has been elected without this node's knowledge.  It must:
///   1. Step down from any leader role.
///   2. Adopt `observedEpoch` as the new local epoch.
///   3. Re-sync with the new leader (truncate + fetch).
///
/// \param observedEpoch  Epoch seen in an incoming message from a peer.
/// \param localEpoch     This node's currently held epoch.
/// \returns true → must step down and adopt observedEpoch.
[[nodiscard]] inline bool mustStepDown(Epoch observedEpoch,
                                        Epoch localEpoch) noexcept
{
    return observedEpoch > localEpoch;
}

// ---------------------------------------------------------------------------
// Truncation offset computation
// ---------------------------------------------------------------------------

/// Compute the log truncation offset a follower must apply when it learns of a
/// new leader / new epoch (`newLeaderEpoch`).
///
/// The follower calls this with its own LeaderEpochCache.  It returns the
/// offset at which the follower should truncate its log, removing entries that
/// may not exist on the new leader's timeline.
///
/// Algorithm:
///   - We want the boundary of "entries the follower has that are from epochs
///     >= newLeaderEpoch" because those epochs were not yet committed by any
///     leader the new leader acknowledges.
///   - `LeaderEpochCache::truncateOffsetForEpoch(e)` returns the startOffset
///     of the first epoch strictly greater than `e`, i.e. the first offset that
///     belongs to epochs > e.
///   - We call it with `newLeaderEpoch - 1` to get the startOffset of any
///     epoch >= newLeaderEpoch recorded by the follower.
///   - If no epoch >= newLeaderEpoch exists in the follower's cache, the cache
///     returns -1 meaning "nothing to truncate" (all recorded epochs are safely
///     below the new epoch boundary).
///
/// \param followerEpochs   The calling follower's own LeaderEpochCache.
/// \param newLeaderEpoch   The epoch of the newly elected leader.
/// \returns Offset at which to truncate, or -1 if no truncation is needed.
[[nodiscard]] inline Offset truncationOffsetForEpochChange(
        const rune::storage::LeaderEpochCache& followerEpochs,
        Epoch newLeaderEpoch) noexcept
{
    if (newLeaderEpoch <= 0) {
        // Epoch 0 is the very first epoch; nothing predates it.
        // No records on the follower can be from epochs >= 0 before the new
        // leader that would need truncation based on this call alone.
        // Return -1 (caller interprets as "no truncation needed").
        return -1;
    }
    // truncateOffsetForEpoch(e) → startOffset of first epoch > e.
    // We pass (newLeaderEpoch - 1) → startOffset of first epoch >= newLeaderEpoch.
    return followerEpochs.truncateOffsetForEpoch(
            static_cast<int32_t>(newLeaderEpoch - 1));
}

} // namespace rune::replication
