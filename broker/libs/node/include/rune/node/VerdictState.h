#pragma once
// VerdictState.h — checked writes Part 04: the per-partition value types of a verdict source (constraint-set timeline,
// marker ledger, stats, the test rule).  Kept apart from VerdictSource.h so PartitionWriteState can hold them by value.
#include <rune/view/Verdict.h>

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <vector>

namespace rune::node {

struct ConstraintSetMember {
    uint64_t                    viewObjectId    = 0;
    rune::view::VerdictViewKind kind{};
    uint64_t                    planFingerprint = 0;
    bool operator==(const ConstraintSetMember&) const = default;
};

inline constexpr std::size_t kMaxVerdictViewsPerSource = 32;

struct ConstraintSet {
    uint32_t id    = 0;
    uint8_t  count = 0;
    std::array<ConstraintSetMember, kMaxVerdictViewsPerSource> members{};

    [[nodiscard]] bool empty() const noexcept { return count == 0; }
    [[nodiscard]] bool sameMembers(const ConstraintSet& o) const noexcept {
        return count == o.count && std::equal(members.begin(), members.begin() + count, o.members.begin());
    }
};

/// The logged constraint sets of one source partition, ascending by the offset of the change record.  The set of a
/// change applies to the offsets above it.
class ConstraintSetTimeline {
public:
    /// Cold.  Idempotent per offset.
    void onChange(int64_t offset, const ConstraintSet& set) {
        auto it = std::lower_bound(changes_.begin(), changes_.end(), offset, [](const Change& c, int64_t o) { return c.offset < o; });
        if (it != changes_.end() && it->offset == offset) { it->set = set; return; }
        changes_.insert(it, Change{offset, set});
    }
    /// The set of the latest change below `offset`; the empty set (id 0) before any.
    [[nodiscard]] const ConstraintSet* setAt(int64_t offset) const noexcept {
        auto it = std::lower_bound(changes_.begin(), changes_.end(), offset, [](const Change& c, int64_t o) { return c.offset < o; });
        return it == changes_.begin() ? &kEmpty : &std::prev(it)->set;
    }
    [[nodiscard]] const ConstraintSet& latest() const noexcept { return changes_.empty() ? kEmpty : changes_.back().set; }
    /// INT64_MAX = none.
    [[nodiscard]] int64_t nextChangeAtOrAfter(int64_t offset) const noexcept {
        auto it = std::lower_bound(changes_.begin(), changes_.end(), offset, [](const Change& c, int64_t o) { return c.offset < o; });
        return it == changes_.end() ? INT64_MAX : it->offset;
    }
    [[nodiscard]] int64_t lastChangeOffset() const noexcept { return changes_.empty() ? -1 : changes_.back().offset; }
    void truncateFrom(int64_t offset) noexcept {
        while (!changes_.empty() && changes_.back().offset >= offset) changes_.pop_back();
    }
    [[nodiscard]] std::size_t size() const noexcept { return changes_.size(); }

private:
    struct Change { int64_t offset; ConstraintSet set; };
    static inline const ConstraintSet kEmpty{};
    std::vector<Change> changes_;
};

/// The verdict markers of the local log that are not yet below the HW (a fixed ring), and what the committed ones cover.
class MarkerLedger {
public:
    static constexpr std::size_t kCapacity = 1024;

    /// False: the ledger is full (emission waits).  A marker at or below an offset already seen is ignored (idempotent).
    [[nodiscard]] bool onMarker(int64_t markerOffset, int64_t finalThrough) noexcept {
        if (markerOffset < committedEnd_ || (count_ != 0 && markerOffset <= at_(count_ - 1).offset)) return true;
        if (count_ == kCapacity) return false;
        at_(count_++) = Slot{markerOffset, finalThrough};
        appended_ = std::max(appended_, finalThrough);
        return true;
    }
    void onHw(int64_t hw) noexcept {
        while (count_ != 0 && at_(0).offset < hw) {
            committed_    = std::max(committed_, at_(0).finalThrough);
            committedEnd_ = at_(0).offset + 1;
            head_         = (head_ + 1) % kCapacity;
            --count_;
        }
    }
    [[nodiscard]] bool full() const noexcept { return count_ == kCapacity; }
    [[nodiscard]] int64_t committedThrough() const noexcept { return committed_; }
    [[nodiscard]] int64_t committedMarkerEnd() const noexcept { return committedEnd_; }
    [[nodiscard]] int64_t appendedThrough() const noexcept { return appended_; }
    void truncateFrom(int64_t offset) noexcept {
        while (count_ != 0 && at_(count_ - 1).offset >= offset) --count_;
        appended_ = committed_;
        for (std::size_t i = 0; i < count_; ++i) appended_ = std::max(appended_, at_(i).finalThrough);
    }

private:
    struct Slot { int64_t offset = 0; int64_t finalThrough = 0; };
    Slot& at_(std::size_t i) noexcept { return ring_[(head_ + i) % kCapacity]; }
    std::array<Slot, kCapacity> ring_{};
    std::size_t head_ = 0, count_ = 0;
    int64_t committed_ = 0, committedEnd_ = 0, appended_ = 0;
};

struct VerdictSourceStats {
    uint64_t rejectionsConstraint = 0, markersWritten = 0, markerAppendRetries = 0, judgeWaitsForInstall = 0, verdictStalls = 0,
             verdictCheckpointsDiscarded = 0, verdictUnknownDuplicates = 0, verdictReplays = 0;
    struct PerConstraint { uint32_t constraintId = 0; uint64_t rejections = 0; };
    std::array<PerConstraint, 256> perConstraint{};
    uint16_t perConstraintCount = 0;
};

struct TestRejectRule {
    enum class Mode : uint8_t { TagEquals, OffsetSet, CapLiveKeys };
    Mode                     mode = Mode::TagEquals;
    uint8_t                  tag  = 0;
    uint32_t                 cap  = 0;
    std::array<int64_t, 16>  offsets{};
    uint8_t                  offsetCount = 0;

    [[nodiscard]] uint64_t fingerprint() const noexcept {
        uint64_t h = 1469598103934665603ull;
        const auto mix = [&h](uint64_t v) { h = (h ^ v) * 1099511628211ull; };
        mix(static_cast<uint64_t>(mode));
        mix(tag);
        mix(cap);
        mix(offsetCount);
        for (uint8_t i = 0; i < offsetCount; ++i) mix(static_cast<uint64_t>(offsets[i]));
        return h;
    }
};

}  // namespace rune::node
