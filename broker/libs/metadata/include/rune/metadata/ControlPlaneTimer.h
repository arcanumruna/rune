#pragma once
// ControlPlaneTimer.h — Deterministic priority-heap timer for the MSM control plane.
//
// DESIGN (mirrors Aeron Cluster):
//   The LEADER polls this timer against CLUSTER/LOG time on each duty-cycle tick.
//   When a timer fires, the caller APPENDS a ControlTickRecord to the MSM log so
//   every replica — including recovering nodes — applies the decision from the
//   same log entry, never from a local wall-clock.  This makes heartbeat-expiry
//   and reassignment decisions deterministic and replayable.
//
// Usage:
//   1. arm(deadlineNs, timerId)   — schedule a timer.
//   2. poll(clusterTimeNs, id)    — returns true + pops earliest timer at or
//                                   before clusterTimeNs.  Caller appends a
//                                   ControlTickRecord.
//   3. snapshotPending(out)       — copy all pending (deadline, id) pairs for snapshot.
//   4. reArm(pending, clusterNow) — clear + re-arm on election-complete.
//                                   Timers whose deadline < clusterNow are kept:
//                                   they fire immediately on the next poll.
//
// Threading: NOT thread-safe.  Must be accessed from a single thread (the
// leader duty-cycle thread).  TSan validates this.
//
// Zero-alloc on poll path: the min-heap uses std::priority_queue backed by a
// pre-allocated std::vector; poll() is O(log n) with no heap allocation.
//
// Namespace: rune::metadata

#include <cstdint>
#include <utility>
#include <vector>
#include <queue>
#include <functional>
#include <rune/protocol/MetadataRecords.h>

namespace rune::metadata {

// ---------------------------------------------------------------------------
// ControlPlaneTimer
//
// Internal representation: min-heap keyed on (deadlineNs, timerId).
// std::priority_queue with std::greater<> gives min-heap semantics.
// ---------------------------------------------------------------------------
class ControlPlaneTimer {
public:
    using Entry = std::pair<int64_t, int32_t>; // (deadlineNs, timerId)

    ControlPlaneTimer() = default;

    // -----------------------------------------------------------------------
    // arm — schedule a timer to fire at or after deadlineNs.
    // -----------------------------------------------------------------------
    void arm(int64_t deadlineNs, int32_t timerId) {
        heap_.push({deadlineNs, timerId});
    }

    // -----------------------------------------------------------------------
    // poll — returns true and pops the earliest timer if its deadline <=
    // clusterTimeNs.  firedTimerId is set to the timer's id.
    // Returns false if no timer is ready.
    //
    // Zero-alloc: priority_queue top/pop are O(log n), no allocations on
    // the poll path (the underlying vector only reallocates on push/arm).
    // -----------------------------------------------------------------------
    [[nodiscard]] bool poll(int64_t clusterTimeNs, int32_t& firedTimerId) noexcept {
        if (heap_.empty()) return false;
        const auto& top = heap_.top();
        if (top.first > clusterTimeNs) return false;
        firedTimerId = top.second;
        heap_.pop();
        return true;
    }

    // -----------------------------------------------------------------------
    // snapshotPending — copy all pending (deadline, id) pairs into out.
    // The heap is NOT modified.  Used before serialising a snapshot or before
    // handing off to a new leader after election.
    // -----------------------------------------------------------------------
    void snapshotPending(std::vector<Entry>& out) const {
        // Copy the heap's underlying container via a temporary.
        // std::priority_queue does not expose iterators, so we drain a copy.
        auto copy = heap_;
        out.clear();
        out.reserve(copy.size());
        while (!copy.empty()) {
            out.push_back(copy.top());
            copy.pop();
        }
        // out is now sorted earliest-deadline-first (min-heap drain order).
    }

    // -----------------------------------------------------------------------
    // reArm — clear all pending timers and re-arm from a snapshot.
    // Called on election-complete so the new leader's timer matches the
    // committed snapshot.
    //
    // Timers whose deadline < clusterNowNs are kept unchanged (deadlines are
    // absolute cluster time — no shifting).  They will fire on the next poll.
    // The clusterNowNs arg is accepted for API symmetry and future use; it is
    // NOT used to drop or shift past-deadline timers.
    // -----------------------------------------------------------------------
    void reArm(const std::vector<Entry>& pending, int64_t /*clusterNowNs*/) {
        // Clear the heap efficiently.
        while (!heap_.empty()) heap_.pop();

        for (const auto& [deadline, id] : pending) {
            heap_.push({deadline, id});
        }
    }

    // -----------------------------------------------------------------------
    // size / empty — for diagnostics / tests.
    // -----------------------------------------------------------------------
    [[nodiscard]] std::size_t size()  const noexcept { return heap_.size(); }
    [[nodiscard]] bool        empty() const noexcept { return heap_.empty(); }

private:
    // Min-heap: smallest deadline fires first.
    std::priority_queue<Entry,
                        std::vector<Entry>,
                        std::greater<Entry>> heap_;
};

} // namespace rune::metadata
