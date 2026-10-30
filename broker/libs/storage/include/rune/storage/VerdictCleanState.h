#pragma once
// VerdictCleanState.h - checked writes Part 04 (8.5): what the cleaner knows about a verdict source's rejected records.
//
// The pre-pass feeds the verdict markers and the constraint-set changes of the log in offset order.  The cleaner then
//   - never indexes or keeps a rejected record below the clean limit (isRejected),
//   - never cleans at or above the offset every verdict is known to (cleanLimit): the coverage of the markers it saw, capped by
//     the bound the owning worker published (the committed watermark W),
//   - drops a verdict marker once it is superseded and every record it lists is gone (markerRemovable).
// Constraint-set changes (type 18) are always kept; reservations (16) and the txn judge (19) are kept until their parts say otherwise.
//
// Cleaner thread only (cold).  clear() keeps the capacity for reuse across passes.
#include "rune/storage/ControlRecordLayout.h"
#include "rune/storage/RejectedOffsetIndex.h"

#include <algorithm>
#include <cstdint>
#include <limits>
#include <span>
#include <vector>

namespace rune::storage {

class VerdictCleanState {
public:
    static constexpr int64_t kNone = std::numeric_limits<int64_t>::max();

    void clear() noexcept {
        rejected_.clear();
        markers_.clear();
        removable_.clear();
        events_.clear();
        cleanLimit_ = kNone;
    }

    // ---- pre-pass feed (offset order) -------------------------------------
    void onVerdictMarker(int64_t markerOffset, int64_t fromOffset, int64_t finalThrough, std::span<const RejectedOffsetIndex::Entry> entries) {
        for (const auto& e : entries)
            if (rejected_.empty() || e.offset > rejected_.back()) rejected_.push_back(e.offset);
            else if (!std::binary_search(rejected_.begin(), rejected_.end(), e.offset)) rejected_.insert(std::lower_bound(rejected_.begin(), rejected_.end(), e.offset), e.offset);
        markers_.push_back(Marker{markerOffset, finalThrough, entries.empty() ? -1 : entries.back().offset});
        events_.push_back(Event{markerOffset, fromOffset, finalThrough, true});
    }
    void onConstraintSetChange(int64_t offset, bool nonEmpty) { events_.push_back(Event{offset, nonEmpty ? 1 : 0, 0, false}); }
    void onReservation(int64_t) noexcept {}   // part 06 gives it meaning

    /// End of the pre-pass: the coverage, and the markers a pass may remove (only below `markerRemovalBound`, the minimum
    /// fetch position over every assigned replica).
    void finish(int64_t toOffset, int64_t publishedBound, int64_t markerRemovalBound) {
        (void)toOffset;
        bool everNonEmpty = false, active = false;
        int64_t lastEmptyAt = -1, cover = 0;
        for (const Event& e : events_) {
            if (!e.marker) {
                const bool nonEmpty = e.a != 0;
                if (nonEmpty && !active) {
                    if (!everNonEmpty || cover > lastEmptyAt) cover = std::max(cover, e.at + 1);
                    active = true;
                    everNonEmpty = true;
                } else if (!nonEmpty && active) {
                    active = false;
                    lastEmptyAt = e.at;
                }
            } else {
                cover = std::max(cover, e.b);
            }
        }
        const int64_t own = !everNonEmpty ? kNone : (!active && cover > lastEmptyAt) ? kNone : cover;
        cleanLimit_ = std::min(own, publishedBound);
        for (std::size_t i = 0; i + 1 < markers_.size(); ++i) {
            const Marker& m = markers_[i];
            if (m.offset >= markerRemovalBound) continue;                  // a replica has not fetched it
            const bool superseded = std::any_of(markers_.begin() + static_cast<std::ptrdiff_t>(i) + 1, markers_.end(),
                                                [&](const Marker& later) { return later.finalThrough > m.finalThrough; });
            if (superseded) removable_.push_back(i);
        }
    }

    // ---- queries ------------------------------------------------------------
    [[nodiscard]] bool isRejected(int64_t offset) const noexcept { return std::binary_search(rejected_.begin(), rejected_.end(), offset); }
    /// Exclusive upper bound of what this pass may clean (kNone: not a verdict source).
    [[nodiscard]] int64_t cleanLimit() const noexcept { return cleanLimit_; }
    /// The pass cleans [.., cleanBoundary): a marker is removable once every record it lists is below it.
    [[nodiscard]] bool markerRemovable(int64_t markerOffset, int64_t cleanBoundary) const noexcept {
        for (const std::size_t i : removable_)
            if (markers_[i].offset == markerOffset) return markers_[i].lastListed < cleanBoundary;
        return false;
    }
    /// A rewrite has work even when no key was indexed: a rejected record to drop or a superseded marker to remove.
    [[nodiscard]] bool hasWork() const noexcept { return !rejected_.empty() || !removable_.empty(); }
    [[nodiscard]] bool keepsReservation(int64_t) const noexcept { return true; }
    /// Type 18 and 19 are always kept; 16 follows keepsReservation; a verdict marker (17) only goes through markerRemovable.
    [[nodiscard]] bool keepsControlRecord(int64_t offset, int16_t type, int64_t cleanBoundary) const noexcept {
        namespace cr = control_record;
        if (type == cr::kReservation) return keepsReservation(offset);
        if (type == cr::kVerdictMarker) return !markerRemovable(offset, cleanBoundary);
        return true;
    }

private:
    struct Marker { int64_t offset, finalThrough, lastListed; };
    struct Event { int64_t at, a, b; bool marker; };   // marker: a = fromOffset, b = finalThrough; change: a = nonEmpty
    std::vector<int64_t> rejected_;                    // ascending
    std::vector<Marker> markers_;
    std::vector<std::size_t> removable_;
    std::vector<Event> events_;
    int64_t cleanLimit_ = kNone;
};

}  // namespace rune::storage
