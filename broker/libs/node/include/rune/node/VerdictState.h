#pragma once
// VerdictState.h — checked writes Part 04: the per-partition value types of a verdict source (constraint-set timeline,
// marker ledger, stats, the test rule).  Kept apart from VerdictSource.h so PartitionWriteState can hold them by value.
#include <rune/view/Verdict.h>

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <span>
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
    int64_t  catalogThrough = -1;   ///< the catalog position the set was computed at (#788); not part of the members
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
    /// The offset of the latest change below `offset` (-1 = none): with `offset` the HW, the latest committed change.
    [[nodiscard]] int64_t lastChangeBelow(int64_t offset) const noexcept {
        auto it = std::lower_bound(changes_.begin(), changes_.end(), offset, [](const Change& c, int64_t o) { return c.offset < o; });
        return it == changes_.begin() ? -1 : std::prev(it)->offset;
    }
    void truncateFrom(int64_t offset) noexcept {
        while (!changes_.empty() && changes_.back().offset >= offset) changes_.pop_back();
    }
    [[nodiscard]] std::size_t size() const noexcept { return changes_.size(); }
    /// The offset of the first change after the member's latest join whose set lacks it; INT64_MAX while it is in latest() or never was.
    [[nodiscard]] int64_t leaveOffsetOf(const ConstraintSetMember& member) const noexcept {
        int64_t leave = INT64_MAX;
        bool in = false;
        for (const Change& c : changes_) {
            const bool has = std::find(c.set.members.begin(), c.set.members.begin() + c.set.count, member) != c.set.members.begin() + c.set.count;
            if (has && !in) leave = INT64_MAX;
            else if (!has && in) leave = c.offset;
            in = has;
        }
        return leave;
    }

private:
    struct Change { int64_t offset; ConstraintSet set; };
    static inline const ConstraintSet kEmpty{};
    std::vector<Change> changes_;
};

struct RetainedVerdictPlan {
    uint64_t                    viewObjectId = 0, planFingerprint = 0;
    rune::view::VerdictViewKind kind{};
    int64_t                     offset = -1;
    std::vector<uint8_t>        plan, outputDescriptor, sourceDescriptor;
};

/// The type-20 plan records of the local log (P9-8): what a leader judging under a member that left its catalog installs the plan from.
/// Cold: the leadership scan and activation only.
class RetainedVerdictPlans {
public:
    /// Idempotent per offset.
    void onRecord(int64_t offset, uint64_t viewObjectId, uint64_t planFingerprint, rune::view::VerdictViewKind kind,
                  std::span<const uint8_t> plan, std::span<const uint8_t> out, std::span<const uint8_t> src) {
        auto it = std::lower_bound(plans_.begin(), plans_.end(), offset, [](const RetainedVerdictPlan& p, int64_t o) { return p.offset < o; });
        if (it != plans_.end() && it->offset == offset) return;
        plans_.insert(it, RetainedVerdictPlan{viewObjectId, planFingerprint, kind, offset, {plan.begin(), plan.end()}, {out.begin(), out.end()}, {src.begin(), src.end()}});
    }
    /// The latest record of the member.
    [[nodiscard]] const RetainedVerdictPlan* find(uint64_t viewObjectId, uint64_t planFingerprint) const noexcept {
        const auto it = std::find_if(plans_.rbegin(), plans_.rend(), [&](const RetainedVerdictPlan& p) { return p.viewObjectId == viewObjectId && p.planFingerprint == planFingerprint; });
        return it == plans_.rend() ? nullptr : &*it;
    }
    void truncateFrom(int64_t offset) noexcept {
        while (!plans_.empty() && plans_.back().offset >= offset) plans_.pop_back();
    }
    /// Drops the records of members that left at an offset below `watermark`: nothing judges under them any more.
    void pruneLeftBelow(int64_t watermark, const ConstraintSetTimeline& timeline) noexcept {
        std::erase_if(plans_, [&](const RetainedVerdictPlan& p) {
            return timeline.leaveOffsetOf(ConstraintSetMember{p.viewObjectId, p.kind, p.planFingerprint}) < watermark;
        });
    }
    [[nodiscard]] std::size_t size() const noexcept { return plans_.size(); }

private:
    std::vector<RetainedVerdictPlan> plans_;
};

/// The verdict markers of the local log that are not yet below the HW (a fixed ring), and what the committed ones cover.
class MarkerLedger {
public:
    static constexpr std::size_t kCapacity = 1024;

    /// False: the ledger is full (emission waits) - the marker is in the log all the same, so appendedThrough counts it.  A marker
    /// at or below an offset already seen, a left-out one included, is ignored (idempotent: a scan re-feed never moves left_ back).
    [[nodiscard]] bool onMarker(int64_t markerOffset, int64_t finalThrough) noexcept {
        if (markerOffset < committedEnd_ || markerOffset <= left_.offset || (count_ != 0 && markerOffset <= at_(count_ - 1).offset)) return true;
        appended_ = std::max(appended_, finalThrough);
        if (count_ == kCapacity) {
            left_ = Slot{markerOffset, std::max(left_.finalThrough, finalThrough)};
            return false;
        }
        at_(count_++) = Slot{markerOffset, finalThrough};
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
        if (left_.offset >= offset) left_ = Slot{-1, 0};   // a cut at or below the last marker left out: forget them all (re-marks more)
        appended_ = std::max(committed_, left_.finalThrough);
        for (std::size_t i = 0; i < count_; ++i) appended_ = std::max(appended_, at_(i).finalThrough);
    }

private:
    struct Slot { int64_t offset = 0; int64_t finalThrough = 0; };
    Slot& at_(std::size_t i) noexcept { return ring_[(head_ + i) % kCapacity]; }
    std::array<Slot, kCapacity> ring_{};
    std::size_t head_ = 0, count_ = 0;
    int64_t committed_ = 0, committedEnd_ = 0, appended_ = 0;
    Slot left_{-1, 0};   // the markers a full ring left out: the last one's offset (-1 = none), the highest finalThrough
};

struct VerdictSourceStats {
    uint64_t rejectionsConstraint = 0, markersWritten = 0, markerAppendRetries = 0, judgeWaitsForInstall = 0, verdictStalls = 0,
             verdictCheckpointsDiscarded = 0, verdictUnknownDuplicates = 0, verdictReplays = 0,
             rejectionsKeyExists = 0, rejectionsKeyNotFound = 0, keyIndexRebuilds = 0, keyIndexSkippedMalformed = 0, candidateSwaps = 0, retainedPlanInstalls = 0;
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
