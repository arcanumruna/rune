#pragma once
#include <cstdint>
#include <cstring>
#include <fcntl.h>
#include <filesystem>
#include <map>
#include <string>
#include <sys/stat.h>
#include <unistd.h>
#include <utility>
#include <vector>
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

    // -------------------------------------------------------------------------
    // Task 9: persistence — checkpoint/restore across a full node restart.
    //
    // On-disk format (all little-endian):
    //   [0..3]   uint32  magic   = kLecMagic (0x4C454348 "LECH")
    //   [4..7]   uint32  version = 1
    //   [8..11]  int32   count   (number of epoch entries)
    //   [12..15] uint32  _pad = 0
    //   Then count × 12-byte records:
    //     [0..3]   int32  epoch
    //     [4..11]  int64  startOffset
    //
    // Written crash-safely: open(.tmp), write, fsync, rename.
    // -------------------------------------------------------------------------

    /// Return all (epoch, startOffset) entries in ascending epoch order.
    [[nodiscard]] std::vector<std::pair<int32_t, int64_t>> entries() const {
        std::vector<std::pair<int32_t, int64_t>> v;
        v.reserve(epochs_.size());
        for (const auto& [ep, off] : epochs_) v.emplace_back(ep, off);
        return v;
    }

    /// Write all entries to a binary checkpoint at `path`.
    /// Crash-safe: writes to path+".tmp", fsyncs, then renames to path.
    void checkpointToFile(const std::string& path) const {
        static constexpr uint32_t kLecMagic   = 0x4C454348u; // "LECH"
        static constexpr uint32_t kLecVersion = 1u;
        static constexpr std::size_t kHdrSize = 16u;
        static constexpr std::size_t kEntSize = 12u;          // int32 + int64

        const auto count = static_cast<int32_t>(epochs_.size());
        const std::size_t total = kHdrSize + static_cast<std::size_t>(count) * kEntSize;

        std::vector<uint8_t> buf(total, 0u);
        uint8_t* p = buf.data();

        auto wle32 = [](uint8_t* q, uint32_t v) noexcept { std::memcpy(q, &v, 4); };
        auto wle64 = [](uint8_t* q, int64_t v) noexcept  { std::memcpy(q, &v, 8); };
        auto wle32s= [](uint8_t* q, int32_t v) noexcept  { std::memcpy(q, &v, 4); };

        wle32(p,      kLecMagic);
        wle32(p + 4,  kLecVersion);
        wle32s(p + 8, count);
        wle32(p + 12, 0u); // pad
        p += kHdrSize;

        for (const auto& [ep, off] : epochs_) {
            wle32s(p,     ep);
            wle64(p + 4, off);
            p += kEntSize;
        }

        const std::string tmp = path + ".tmp";
        int fd = ::open(tmp.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
        if (fd < 0) return; // best-effort; tolerate — caller will detect missing checkpoint on restore
        (void)::write(fd, buf.data(), total);
        (void)::fsync(fd);
        ::close(fd);
        (void)::rename(tmp.c_str(), path.c_str());
    }

    /// Restore from a checkpoint file.  Returns true on success, false if the
    /// file is absent, truncated, or has an unrecognised magic/version.
    /// On success, replaces all current entries with the loaded state.
    [[nodiscard]] bool restoreFromFile(const std::string& path) {
        static constexpr uint32_t kLecMagic   = 0x4C454348u;
        static constexpr uint32_t kLecVersion = 1u;
        static constexpr std::size_t kHdrSize = 16u;
        static constexpr std::size_t kEntSize = 12u;

        auto rle32 = [](const uint8_t* q) noexcept -> uint32_t { uint32_t v{}; std::memcpy(&v, q, 4); return v; };
        auto rle32s= [](const uint8_t* q) noexcept -> int32_t  { int32_t  v{}; std::memcpy(&v, q, 4); return v; };
        auto rle64s= [](const uint8_t* q) noexcept -> int64_t  { int64_t  v{}; std::memcpy(&v, q, 8); return v; };

        int fd = ::open(path.c_str(), O_RDONLY);
        if (fd < 0) return false; // absent

        struct stat st{};
        if (::fstat(fd, &st) != 0) { ::close(fd); return false; }
        const auto fileSize = static_cast<std::size_t>(st.st_size);
        if (fileSize < kHdrSize) { ::close(fd); return false; } // truncated header

        std::vector<uint8_t> buf(fileSize);
        if (::read(fd, buf.data(), fileSize) != static_cast<ssize_t>(fileSize)) {
            ::close(fd); return false;
        }
        ::close(fd);

        const uint8_t* p = buf.data();
        if (rle32(p) != kLecMagic)   return false; // bad magic
        if (rle32(p + 4) != kLecVersion) return false; // unknown version
        const int32_t count = rle32s(p + 8);
        if (count < 0) return false;

        const std::size_t expected = kHdrSize + static_cast<std::size_t>(count) * kEntSize;
        if (fileSize < expected) return false; // truncated entries

        epochs_.clear();
        p += kHdrSize;
        for (int32_t i = 0; i < count; ++i) {
            int32_t ep  = rle32s(p);
            int64_t off = rle64s(p + 4);
            epochs_[ep] = off;
            p += kEntSize;
        }
        return true;
    }

private:
    std::map<int32_t, int64_t> epochs_;
};

} // namespace rune::storage
