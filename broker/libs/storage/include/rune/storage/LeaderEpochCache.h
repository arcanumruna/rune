#pragma once
#include <cstdint>
#include <map>
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
