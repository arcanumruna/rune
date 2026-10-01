#pragma once
// RcState.h — F15 S4 (design note §2.1-§2.4, Option B): the read_committed
// state of ONE worker-owned user partition, owned by its worker.
//
//   * LsoTracker + AbortedTxnIndex, updated in the same call as each append
//     (data batch: onTxnBatch; marker: onMarker) — never through a return, an
//     ack or a session (C1, K1, K2: every read_committed decision is made on the
//     owning worker from this state and the live HW, L5).
//   * A rich-state build machine (each state holds only its own variables):
//
//       Unbuilt                                    not serving read_committed
//       Scanning{cursor, target, epoch, gens}      building [logStart, target)
//       Ready{epoch, gens}                         serving; append-time updates live
//
//     gens = (logGeneration, leadershipGeneration) the state was built for.
//     advance() is the per-pass driver (§2.3), state-driven and hook-free:
//       1. not Unbuilt and (not Leader, or a gen changed) → Unbuilt (clears
//          tracker, index, tail list) — BecomeFollower, self-demotion,
//          truncation, reconcile append, a follower period between passes;
//          the `!Unbuilt` guard keeps followers from paying the clear per pass.
//          A leader-epoch bump with unchanged gens keeps the state (exact).
//       2. Unbuilt, Leader and leaderComplete_ (I2) → Scanning{logStart, LEO}.
//       3. Scanning: cursor clamped to logStartOffset (I-C: retention between
//          passes), at most `budget` bytes per pass via the zero-copy readView,
//          an empty view below target skips to the next segment base (a gap),
//          each whole batch fed to RcBuilder::step (N2/N3).  At target the
//          marker tail recorded meanwhile is replayed in order → Ready.
//          Liveness is time-independent: target is fixed at entry, so the scan
//          ends in ceil((target - logStart) bytes / budget) passes whatever the
//          append rate.
//   * The tail list holds MARKERS ONLY: a producerId >= 0 data batch is refused
//     until Ready (the produce gate), non-transactional batches carry no state.
//
// Owner (worker) thread only.  No locks, no atomics, no virtual calls; the hot
// paths (onTxnBatch, lso, isAborted, ready) never allocate once reserve()d.
#include <rune/protocol/RecordBatch.h>
#include <rune/storage/Log.h>
#include <rune/storage/LogReadView.h>
#include <rune/transaction/ReadCommittedView.h>

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <variant>
#include <vector>

namespace rune::node {

/// The replica facts one advance() step reads (owner thread, plain loads).
struct RcFacts {
    bool     leader               = false;
    bool     leaderComplete       = false;
    uint64_t logGeneration        = 0;
    uint64_t leadershipGeneration = 0;
    int32_t  epoch                = 0;
};

class RcState {
public:
    /// Per-pass build budget (bytes read through readView per advance step).
    /// Set from BM_RcBuildStep (cold page cache, p99 per step <= 20 µs target).
    static constexpr int32_t     kDefaultScanBytesPerPass = 64 * 1024;
    static constexpr std::size_t kTailReserve             = 256;

    struct Unbuilt {};
    struct Scanning {
        int64_t  cursor  = 0;
        int64_t  target  = 0;
        int32_t  epoch   = 0;
        uint64_t logGen  = 0;
        uint64_t leadGen = 0;
    };
    struct Ready {
        int32_t  epoch   = 0;
        uint64_t logGen  = 0;
        uint64_t leadGen = 0;
    };
    struct TailEvent {
        int64_t producerId = -1;
        int64_t offset     = 0;
        bool    commit     = false;
    };

    /// Cold (ownership apply, addendum m5): reserve every structure so the
    /// partition's first transactional batch / marker never allocates.
    void reserve(std::size_t openTxnCapacity) {
        tracker_.reserve(openTxnCapacity);
        aborted_.reserve(rune::transaction::AbortedTxnIndex::kDefaultReservedRanges);
        tail_.reserve(kTailReserve);
    }

    [[nodiscard]] bool ready() const noexcept { return std::holds_alternative<Ready>(st_); }
    [[nodiscard]] bool scanning() const noexcept { return std::holds_alternative<Scanning>(st_); }
    /// 0 Unbuilt, 1 Scanning, 2 Ready.
    [[nodiscard]] int statusCode() const noexcept { return static_cast<int>(st_.index()); }

    // ---- §2.1: append-time updates (same invocation as the append) --------
    /// A TRANSACTIONAL data batch appended at `base` (not a duplicate).  Only
    /// reachable while Ready: the produce gate refuses producerId >= 0 batches
    /// until then.
    void onTxnBatch(int64_t producerId, int64_t base) {
        assert(ready() && "F15 §2.3: a producerId >= 0 batch was appended before RC is Ready");
        if (ready()) [[likely]] tracker_.onProduceBatch(producerId, base);
    }
    /// A COMMIT / ABORT control batch appended at `offset`.
    void onMarker(int64_t producerId, int64_t offset, bool commit) {
        if (ready()) {
            applyMarker_(producerId, offset, commit);
        } else if (scanning()) {
            if (tail_.size() == tail_.capacity()) [[unlikely]] ++tailGrowths_;   // cold, counted
            tail_.push_back(TailEvent{producerId, offset, commit});
        }
        // Unbuilt: nothing — the build starts at LEO > offset and scans it.
    }

    // ---- §2.3 step 1 alone: validity against the live replica facts --------
    /// The reset rule of advance() without any build work: cheap enough for
    /// every serve and every gated produce (a role change can land between the
    /// per-pass driver and a read in the same pass — e.g. a self-demotion in a
    /// FetchRequest handled after it).  Returns ready().
    bool revalidate(const RcFacts& f) noexcept {
        if (!std::holds_alternative<Unbuilt>(st_)) {
            const auto [logGen, leadGen] = builtFor_();
            if (!f.leader || f.logGeneration != logGen || f.leadershipGeneration != leadGen) [[unlikely]]
                reset_();
        }
        return ready();
    }

    // ---- §2.3: per-pass driver --------------------------------------------
    /// `log` null = a memory-backed partition (nothing to scan: Ready at once).
    /// Returns the bytes this step read (0: no scan work).
    std::size_t advance(const RcFacts& f, rune::storage::Log* log, int32_t budget) {
        (void)revalidate(f);
        if (Ready* r = std::get_if<Ready>(&st_)) [[likely]] {
            r->epoch = f.epoch;                            // epoch bump, same log: still exact
            if (log != nullptr && !aborted_.ranges().empty() &&
                aborted_.ranges().front().lastOffset < log->logStartOffset()) [[unlikely]]
                aborted_.pruneBelow(log->logStartOffset());   // retention (cold)
            return 0;
        }
        if (std::holds_alternative<Unbuilt>(st_)) {
            if (!f.leader || !f.leaderComplete) return 0;  // I2: wait for leaderComplete_
            if (log == nullptr) {
                st_ = Ready{f.epoch, f.logGeneration, f.leadershipGeneration};
                return 0;
            }
            st_ = Scanning{log->logStartOffset(), log->logEndOffset(), f.epoch, f.logGeneration,
                           f.leadershipGeneration};
        }
        Scanning& s = std::get<Scanning>(st_);
        s.epoch = f.epoch;
        return step_(s, *log, budget);
    }

    // ---- §2.4: serving ------------------------------------------------------
    [[nodiscard]] int64_t lso(int64_t hw) const noexcept { return tracker_.lso(hw); }
    [[nodiscard]] bool isAborted(int64_t producerId, int64_t base) const noexcept {
        return aborted_.isAborted(producerId, base);
    }

    [[nodiscard]] const rune::transaction::LsoTracker&      tracker() const noexcept { return tracker_; }
    [[nodiscard]] const rune::transaction::AbortedTxnIndex& aborted() const noexcept { return aborted_; }
    [[nodiscard]] uint64_t    scanSteps() const noexcept { return scanSteps_; }
    [[nodiscard]] std::size_t maxStepBytes() const noexcept { return maxStepBytes_; }
    [[nodiscard]] uint64_t    tailGrowths() const noexcept { return tailGrowths_; }

private:
    [[nodiscard]] std::pair<uint64_t, uint64_t> builtFor_() const noexcept {
        if (const auto* s = std::get_if<Scanning>(&st_)) return {s->logGen, s->leadGen};
        const auto& r = std::get<Ready>(st_);
        return {r.logGen, r.leadGen};
    }
    void reset_() noexcept {
        tracker_.clear();   // N4: both indexes
        aborted_.clear();
        tail_.clear();
        st_ = Unbuilt{};
    }
    void applyMarker_(int64_t producerId, int64_t offset, bool commit) {
        const int64_t first = tracker_.onControlMarker(producerId, offset, commit);
        if (!commit && first >= 0) aborted_.onAbortMarker(producerId, first, offset);
    }
    std::size_t step_(Scanning& s, rune::storage::Log& log, int32_t budget) {
        using rune::protocol::RecordBatchReader;
        constexpr std::size_t kOverhead = static_cast<std::size_t>(rune::protocol::batch::kRecordBatchOverhead);
        ++scanSteps_;
        // I-C: retention runs on this thread between passes and may delete the
        // segments under the cursor (readView below the first segment is empty
        // forever).  Batches below the new log start are gone, and so is any
        // data they could hide: skipping them is exact.
        s.cursor = std::max(s.cursor, log.logStartOffset());
        std::size_t bytes = 0;
        if (s.cursor < s.target) {
            const rune::storage::LogReadView v = log.readView(s.cursor, budget);
            if (v.empty()) {
                // A gap below target (e.g. compaction emptied a segment): resume
                // at the next segment base (non-allocating lookup).
                const int64_t next = log.nextSegmentBaseAfter(s.cursor);
                s.cursor = next < 0 ? s.target : next;
            } else {
                rune::transaction::RcBuilder builder(&tracker_, &aborted_);
                std::size_t pos = 0;
                while (pos + kOverhead <= v.len) {
                    RecordBatchReader r(v.data + pos, v.len - pos);
                    if (r.baseOffset() >= s.target) { s.cursor = s.target; break; }
                    const auto res = builder.step(v.data + pos, v.len - pos);
                    if (res.outcome == rune::transaction::RcBuilder::Outcome::Invalid) [[unlikely]] {
                        // readView yields whole batches only — never expected; skip
                        // what the view covers so the build cannot stall.
                        s.cursor = std::max(v.nextOffset, s.cursor + 1);
                        break;
                    }
                    pos += static_cast<std::size_t>(res.totalLength);
                    s.cursor = r.baseOffset() + r.lastOffsetDelta() + 1;
                }
                bytes = pos;
            }
        }
        if (bytes > maxStepBytes_) maxStepBytes_ = bytes;
        if (s.cursor >= s.target) finish_(s);
        return bytes;
    }
    void finish_(const Scanning& s) {
        // The tail lies wholly at or above target: replaying it after the scan
        // keeps the FIFO / range order (§2.2).
        for (const TailEvent& e : tail_) applyMarker_(e.producerId, e.offset, e.commit);
        tail_.clear();
        st_ = Ready{s.epoch, s.logGen, s.leadGen};
    }

    rune::transaction::LsoTracker      tracker_;
    rune::transaction::AbortedTxnIndex aborted_;
    std::vector<TailEvent>             tail_;
    std::variant<Unbuilt, Scanning, Ready> st_{Unbuilt{}};
    uint64_t    scanSteps_    = 0;
    std::size_t maxStepBytes_ = 0;
    uint64_t    tailGrowths_  = 0;
};

} // namespace rune::node
