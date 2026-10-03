#pragma once
// ---------------------------------------------------------------------------
// TxnCleanState — what one cleaner pass knows about the transactions in the
// region it may clean (Kafka LogCleaner parity: CleanedTransactionMetadata +
// the LSO bound + the KIP-534 marker removal rule).
//
// Filled by LogCleaner's header pre-pass over every batch below the pass's
// upper bound (`toOffset`, itself <= the committed HW — a marker above it is not
// trusted: it may still be truncated), in offset order, BEFORE the offset map is
// built.  Consumed by the offset-map build and by every rewrite path:
//
//  * firstUnresolvedOffset(): the first offset of the earliest transaction whose
//    marker was not seen below `toOffset` — the LSO as the log itself shows it.
//    The pass never cleans at/after it (cleanLimit()): an open txn may still
//    ABORT, and its data must neither be indexed (it would supersede the
//    committed value) nor rewritten.
//  * isAborted(pid, offset): the transactional data batch at `offset` belongs to
//    a transaction of `pid` that ended with an ABORT marker.  Such data is never
//    indexed and is discarded by the rewrite (its producer metadata survives as a
//    record-less batch when it is the producer's last batch — C2).
//  * markerRemovable(offset): the control batch at `offset` ends a transaction of
//    which NO batch is left (data or record-less tail), it is not its producer's
//    last batch (the PSM rebuild learns the producer's epoch from it, like the C2
//    producer tail) AND every assigned replica has fetched past it (offset <
//    markerRemovalBound — fix round 1: a lagging replica holding the txn's data
//    must still receive its marker; a permanently dead replica therefore keeps
//    every marker — correctness over growth).  Such a marker is stamped with a
//    delete horizon on the first pass that finds it removable and dropped by a
//    pass at/after the horizon (KIP-534).
//
// A marker whose record key is not a 2-byte controlType is treated as COMMIT:
// never discard data on an unparseable marker.
//
// Cleaner thread only (cold; the cleaner is the one sanctioned heap user).
// clear() keeps the containers' capacity for reuse across passes.
// ---------------------------------------------------------------------------
#include <algorithm>
#include <cstdint>
#include <limits>
#include <unordered_map>
#include <utility>
#include <vector>

namespace rune::storage {

class TxnCleanState {
public:
    static constexpr int64_t kNone = std::numeric_limits<int64_t>::max();

    void clear() noexcept {
        scan_.clear();
        aborted_.clear();
        candidates_.clear();
        removable_.clear();
        firstUnresolved_ = kNone;
        cleanLimit_      = kNone;
        abortedBatches_  = 0;
    }
    static constexpr int64_t kNoHorizon = -1;

    // ---- pre-pass feed (offset order) -------------------------------------
    /// A transactional DATA batch (incl. a record-less producer-tail batch,
    /// hasRecords = false: it keeps its txn's marker, but discarding it is no work).
    void onTxnDataBatch(int64_t pid, int64_t baseOffset, int64_t lastOffset, bool hasRecords) {
        Scan& s = scan_[pid];
        if (s.openFirst < 0) s.openFirst = baseOffset;
        ++s.openBatches;
        if (hasRecords) ++s.openRecordBatches;
        s.lastBatch = std::max(s.lastBatch, lastOffset);
    }
    /// Any other batch carrying a producer id (non-transactional idempotent data).
    void onProducerBatch(int64_t pid, int64_t lastOffset) {
        Scan& s = scan_[pid];
        s.lastBatch = std::max(s.lastBatch, lastOffset);
    }
    /// A COMMIT/ABORT marker of `pid` at `offset` (horizonMs = its delete
    /// horizon when already stamped, kNoHorizon otherwise).
    void onMarker(int64_t pid, int64_t offset, bool abort, int64_t horizonMs = kNoHorizon) {
        Scan& s = scan_[pid];
        if (abort && s.openFirst >= 0) {
            aborted_[pid].emplace_back(s.openFirst, offset);
            abortedBatches_ += s.openRecordBatches;
        }
        if (s.openBatches == 0) candidates_.push_back(Candidate{pid, offset, horizonMs});
        s.openFirst         = -1;
        s.openBatches       = 0;
        s.openRecordBatches = 0;
        s.lastBatch         = std::max(s.lastBatch, offset);
    }
    /// End of the pre-pass: resolve the LSO, the clean limit and removable markers
    /// (only below `markerRemovalBound`, the minimum fetch position over every
    /// assigned replica).
    void finish(int64_t toOffset, int64_t markerRemovalBound = 0) {
        firstUnresolved_ = kNone;
        for (const auto& [pid, s] : scan_)
            if (s.openFirst >= 0 && s.openFirst < firstUnresolved_) firstUnresolved_ = s.openFirst;
        cleanLimit_ = std::min(toOffset, firstUnresolved_);
        for (const auto& c : candidates_) {
            if (c.offset >= markerRemovalBound) continue;                  // a replica has not fetched it
            const auto it = scan_.find(c.pid);
            if (it != scan_.end() && it->second.lastBatch == c.offset) continue;   // producer's last batch
            removable_.emplace(c.offset, c.horizonMs);
        }
    }

    // ---- queries ------------------------------------------------------------
    [[nodiscard]] int64_t firstUnresolvedOffset() const noexcept { return firstUnresolved_; }
    /// Exclusive upper bound of what this pass may clean: min(toOffset, LSO).
    [[nodiscard]] int64_t cleanLimit() const noexcept { return cleanLimit_; }
    [[nodiscard]] bool isAborted(int64_t pid, int64_t offset) const noexcept {
        const auto it = aborted_.find(pid);
        if (it == aborted_.end()) return false;
        const auto& v = it->second;   // ascending, disjoint
        auto r = std::upper_bound(v.begin(), v.end(), offset,
                                  [](int64_t o, const std::pair<int64_t, int64_t>& x) { return o < x.first; });
        if (r == v.begin()) return false;
        --r;
        return offset <= r->second;
    }
    [[nodiscard]] bool markerRemovable(int64_t offset) const noexcept { return removable_.count(offset) != 0; }
    /// Is there txn work for a rewrite even when no key was indexed?  Aborted
    /// batches that still hold records, or a removable marker the pass would stamp
    /// (no horizon yet) or drop (horizon reached) — never an already-retained
    /// record-less aborted tail or a stamped marker still inside its horizon.
    [[nodiscard]] bool hasWork(int64_t nowMs) const noexcept {
        if (abortedBatches_ > 0) return true;
        if (nowMs < 0) return false;   // legacy path: markers are never touched
        for (const auto& [off, h] : removable_)
            if (h == kNoHorizon || nowMs >= h) return true;
        return false;
    }

private:
    struct Scan {
        int64_t openFirst   = -1;   ///< first offset of the current (unresolved) txn, -1 = none
        int64_t openBatches = 0;    ///< data batches seen in the current txn
        int64_t openRecordBatches = 0;   ///< ... of which still hold records
        int64_t lastBatch   = -1;   ///< last offset of the producer's last batch (any kind)
    };
    std::unordered_map<int64_t, Scan> scan_;
    std::unordered_map<int64_t, std::vector<std::pair<int64_t, int64_t>>> aborted_;
    struct Candidate { int64_t pid, offset, horizonMs; };
    std::vector<Candidate> candidates_;   ///< markers with no txn data left
    std::unordered_map<int64_t, int64_t> removable_;   ///< marker offset -> its horizon (kNoHorizon)
    int64_t firstUnresolved_ = kNone;
    int64_t cleanLimit_      = kNone;
    int64_t abortedBatches_  = 0;
};

} // namespace rune::storage
