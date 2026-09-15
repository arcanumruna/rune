#pragma once
#include <cstdint>
#include <map>
#include <utility>
namespace rune::storage {

/// In-memory cache of (leaderEpoch → startOffset) pairs.
///
/// Invariants (A5):
///   • Epochs are monotonically increasing: assign() ignores any call whose
///     epoch ≤ latestEpoch() (non-monotonic epoch is silently dropped).
///   • startOffset must be ≥ the startOffset of the current latest epoch;
///     a non-monotonic startOffset is also silently dropped.
///   • truncateFromEnd(truncateOffset) removes all epoch entries whose
///     startOffset ≥ truncateOffset, pruning the cache in step with
///     Log::truncateTo() so the epoch map never contains epochs that start
///     past the current log-end-offset.
///
/// In-memory only (no persistence).  Must be wired to Log::truncateTo()
/// before Phase 3 replication relies on it.
class LeaderEpochCache {
public:
    /// Assign epoch → startOffset.  A5: silently ignores non-monotonic calls.
    void assign(int32_t epoch, int64_t startOffset) {
        if (!epochs_.empty()) {
            // Drop if epoch is not strictly greater than the current latest.
            if (epoch <= epochs_.rbegin()->first) return;
            // Drop if startOffset is not ≥ the current latest startOffset.
            if (startOffset < epochs_.rbegin()->second) return;
        }
        epochs_[epoch] = startOffset;
    }

    int64_t startOffsetFor(int32_t epoch) const {
        auto it = epochs_.find(epoch);
        return it == epochs_.end() ? -1 : it->second;
    }

    int32_t latestEpoch() const {
        return epochs_.empty() ? -1 : epochs_.rbegin()->first;
    }

    int64_t truncateOffsetForEpoch(int32_t leaderEpoch) const {
        auto it = epochs_.upper_bound(leaderEpoch);
        return it == epochs_.end() ? -1 : it->second;
    }

    /// DR-BUG-6 — the leader-side OffsetsForLeaderEpoch answer (KIP-101).
    ///
    /// A follower asks "for epoch `requestedEpoch` (the follower's latest
    /// epoch), what is your end offset for the last epoch we share?".
    ///
    /// Returns a pair (responseEpoch, endOffset):
    ///   • responseEpoch — the largest cached epoch <= requestedEpoch (the
    ///     epoch the leader and follower actually share at/below the request),
    ///     or -1 if the leader has NO epoch <= requestedEpoch (the follower's
    ///     entire requested-epoch history predates the leader's → divergent).
    ///   • endOffset — the offset at which responseEpoch ENDED on the leader:
    ///       - the start offset of the first cached epoch strictly greater than
    ///         responseEpoch (the boundary where the next epoch began), OR
    ///       - the leader's `logEndOffset` when responseEpoch is the leader's
    ///         LATEST cached epoch (the epoch is still open → its end is the
    ///         current LEO).  This is the crucial KIP-101 case: an open shared
    ///         epoch reports the leader LEO, and the follower interprets a
    ///         shared open epoch as "no divergence → keep my tail" (the leader
    ///         reconciles the tail it is missing).
    ///     When responseEpoch == -1, endOffset is the start of the leader's
    ///     earliest cached epoch (or `logEndOffset` if the cache is empty),
    ///     i.e. the offset up to which the two histories can be trusted.
    ///
    /// Pure: does not mutate the cache.
    std::pair<int32_t, int64_t>
    endOffsetForEpoch(int32_t requestedEpoch, int64_t logEndOffset) const {
        if (epochs_.empty()) {
            return {-1, 0};
        }
        // Largest cached epoch <= requestedEpoch.
        // upper_bound(requestedEpoch) → first epoch > requestedEpoch.
        auto next = epochs_.upper_bound(requestedEpoch);
        if (next == epochs_.begin()) {
            // No cached epoch <= requestedEpoch: the follower's requested-epoch
            // history predates everything the leader knows → divergent.  The
            // shared history ends at the leader's earliest start offset.
            return {-1, epochs_.begin()->second};
        }
        auto shared = std::prev(next);               // largest epoch <= requested
        const int32_t responseEpoch = shared->first;
        if (next == epochs_.end()) {
            // responseEpoch is the leader's latest cached epoch → still open →
            // its end offset is the current log-end-offset.
            return {responseEpoch, logEndOffset};
        }
        // responseEpoch ended where the next cached epoch began.
        return {responseEpoch, next->second};
    }

    /// DR-BUG-6 divergence guard — the leader-epoch that OWNS a given offset.
    ///
    /// Returns the largest cached epoch whose startOffset <= `offset` (the epoch
    /// under which the record at `offset` was written on THIS node's lineage), or
    /// -1 if `offset` precedes the earliest cached epoch (or the cache is empty).
    ///
    /// Used by applyReconcileResponse() to reject a pulled batch whose stamped
    /// leader-epoch does not belong to the receiving node's accepted lineage — a
    /// same-offset batch from an UNSHARED epoch must never be adopted (divergence).
    ///
    /// Pure: does not mutate the cache.
    int32_t epochForOffset(int64_t offset) const {
        // Search by VALUE (startOffset).  epochs_ is keyed by epoch and
        // startOffsets are monotonic in epoch (invariant A5), so a forward scan
        // finds the largest epoch whose startOffset <= offset.
        int32_t owner = -1;
        for (const auto& [ep, start] : epochs_) {
            if (start <= offset) owner = ep;
            else break;
        }
        return owner;
    }

    /// DR-BUG-6 divergence guard — is `epoch` on THIS node's accepted lineage?
    ///
    /// True iff the cache contains an entry for exactly `epoch`.  A pulled batch
    /// stamped with an epoch the receiver never held is off-lineage → reject.
    bool hasEpoch(int32_t epoch) const {
        return epochs_.find(epoch) != epochs_.end();
    }

    /// A5: Prune epoch entries whose startOffset ≥ truncateOffset.
    /// Should be called whenever Log::truncateTo(truncateOffset) is called
    /// so the epoch cache stays consistent with the physical log extent.
    void truncateFromEnd(int64_t truncateOffset) {
        auto it = epochs_.begin();
        while (it != epochs_.end()) {
            if (it->second >= truncateOffset) {
                it = epochs_.erase(it);
            } else {
                ++it;
            }
        }
    }

private:
    std::map<int32_t, int64_t> epochs_;
};

} // namespace rune::storage
