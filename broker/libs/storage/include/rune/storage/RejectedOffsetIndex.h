#pragma once
// RejectedOffsetIndex.h -- the rejected offsets of one partition, sorted, with the verdict coverage.
//
// One instance per partition replica (PartitionWriteState::rejected), worker-owned.  The leader's judge fills it with
// add(); on every replica the RcState leadership scan merges the committed markers with onMarker().  Reserved at
// creation: add() never allocates, and a full index refuses (the judge then stalls; a full index is never a verdict).
#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <span>
#include <vector>

namespace rune::storage {

class RejectedOffsetIndex {
public:
    struct Entry {
        int64_t  offset;
        uint16_t code;
        uint32_t constraintId;
    };

    void reserve(std::size_t entries) {
        entries_.reserve(entries);
        capacity_ = entries;
    }
    [[nodiscard]] std::size_t capacity() const noexcept { return capacity_; }
    void grow(std::size_t entries) {   // cold
        if (entries > capacity_) reserve(entries);
    }

    /// Appends in ascending offset order.  False: a non-ascending offset (counted in outOfOrder()) or a full index.
    [[nodiscard]] bool add(int64_t offset, uint16_t code, uint32_t constraintId) noexcept {
        if (!entries_.empty() && offset <= entries_.back().offset) {
            ++outOfOrder_;
            return false;
        }
        if (entries_.size() >= capacity_) return false;
        entries_.push_back({offset, code, constraintId});
        return true;
    }

    /// Replica apply and recovery: merges a marker covering [fromOffset, finalThrough).  Cold; idempotent.  An entry
    /// the index holds with another verdict, and an index entry inside the range that the marker lacks, count a
    /// mismatch (a determinism alarm).
    void onMarker(int64_t fromOffset, int64_t finalThrough, std::span<const Entry> entries) {
        for (const Entry& e : entries) {
            auto it = lowerBound(e.offset);
            if (it != entries_.end() && it->offset == e.offset) {
                if (it->code != e.code || it->constraintId != e.constraintId) ++mismatches_;
                continue;
            }
            if (entries_.size() >= capacity_) capacity_ = entries_.size() + 1;
            entries_.insert(it, e);
        }
        for (auto it = lowerBound(fromOffset); it != entries_.end() && it->offset < finalThrough; ++it) {
            const bool listed = std::any_of(entries.begin(), entries.end(), [&](const Entry& e) { return e.offset == it->offset; });
            if (!listed) ++mismatches_;
        }
        covered_ = std::max(covered_, finalThrough);
    }

    [[nodiscard]] bool isRejected(int64_t offset) const noexcept { return find(offset) != nullptr; }
    [[nodiscard]] const Entry* find(int64_t offset) const noexcept {
        const auto it = lowerBound(offset);
        return it != entries_.end() && it->offset == offset ? &*it : nullptr;
    }
    /// fn(const Entry&) for every entry in [from, toExclusive).
    template <class Fn>
    void forEachIn(int64_t from, int64_t toExclusive, Fn&& fn) const noexcept {
        for (auto it = lowerBound(from); it != entries_.end() && it->offset < toExclusive; ++it) fn(*it);
    }
    /// INT64_MAX = none.
    [[nodiscard]] int64_t nextRejectedAtOrAfter(int64_t offset) const noexcept {
        const auto it = lowerBound(offset);
        return it == entries_.end() ? std::numeric_limits<int64_t>::max() : it->offset;
    }
    [[nodiscard]] std::size_t countAtOrAfter(int64_t offset) const noexcept {
        return static_cast<std::size_t>(entries_.end() - lowerBound(offset));
    }

    /// A failover cut the log at `offset`: nothing at or above it is known any more.
    void truncateFrom(int64_t offset) noexcept {
        entries_.erase(lowerBound(offset), entries_.end());
        covered_   = std::min(covered_, offset);
        watermark_ = std::min(watermark_, offset);
    }
    void dropBelow(int64_t offset) noexcept { entries_.erase(entries_.begin(), lowerBound(offset)); }

    /// W: the highest finalThrough of a committed marker.  It bounds the cleaner.
    [[nodiscard]] int64_t watermark() const noexcept { return watermark_; }
    void setWatermark(int64_t w) noexcept { watermark_ = w; }
    /// K: every offset below it has a verdict known on this node.
    [[nodiscard]] int64_t coveredThrough() const noexcept { return covered_; }
    void noteCovered(int64_t offset) noexcept { covered_ = std::max(covered_, offset); }

    [[nodiscard]] std::size_t size() const noexcept { return entries_.size(); }
    [[nodiscard]] uint64_t mismatches() const noexcept { return mismatches_; }
    [[nodiscard]] uint64_t outOfOrder() const noexcept { return outOfOrder_; }

private:
    [[nodiscard]] std::vector<Entry>::const_iterator lowerBound(int64_t offset) const noexcept {
        return std::lower_bound(entries_.begin(), entries_.end(), offset, [](const Entry& e, int64_t o) { return e.offset < o; });
    }
    [[nodiscard]] std::vector<Entry>::iterator lowerBound(int64_t offset) noexcept {
        return std::lower_bound(entries_.begin(), entries_.end(), offset, [](const Entry& e, int64_t o) { return e.offset < o; });
    }

    std::vector<Entry> entries_;
    std::size_t        capacity_  = 0;
    int64_t            watermark_ = 0;
    int64_t            covered_   = 0;
    uint64_t           mismatches_ = 0;
    uint64_t           outOfOrder_ = 0;
};

}  // namespace rune::storage
