#pragma once
// LeaseTestClock.h - lease topics Task 17 (L18): the injected lease clock of a REAL BrokerNode.
//
// Every worker reads its lease clock (Worker::leaseNow_) from a `const int64_t*` that only its OWN thread may touch.  This helper keeps one
// cell per worker index: install() points the worker at its cell (on the worker thread, through onWorkerThreadForTest), advance() adds to the
// cell on the worker thread.  The test thread never reads or writes a cell, so there is no data race and no atomic (shared-nothing).
//
// Scope: a node whose workers hold an UNBOUNDED view read lease, i.e. a single-node broker.  On a multi-node broker the read lease ends are
// stamped by the election on the real lease clock, so shifting only the workers would lapse every read lease at once; advanceLeaseClock refuses
// (returns false) for more than one node and the multi-node tests run the lease clock in real time with short TTLs.
//
// install() must precede the first lease activity of a node (the lease runtime and its wheels are created on the injected clock); the value
// survives restarts: install() on the new node resumes at the clock's total, so the clock never goes back.
#include <rune/consensus/LeaseClock.h>
#include <rune/node/BrokerNode.h>

#include <algorithm>
#include <cstdint>
#include <deque>

namespace rune::node::test {

class LeaseTestClock {
public:
    explicit LeaseTestClock(int64_t startNs = rune::consensus::leaseClockNowNs()) : totalNs_(startNs) {}

    [[nodiscard]] int64_t nowNs() const noexcept { return totalNs_; }

    /// After a restart the restored lease runtime read the REAL clock before install() could run: resume from the real clock, so the
    /// restored deadlines (stamped on it) and the injected clock agree; the injected time advanced before the restart is not carried over.
    void resync() noexcept { totalNs_ = rune::consensus::leaseClockNowNs(); }

    /// Points every worker of `node` at its cell (value = the clock's total).  The node's workers are running.
    void install(BrokerNode& node) {
        const int32_t n = node.workerCountForTest();
        while (static_cast<int32_t>(cells_.size()) < n) cells_.push_back(0);
        for (int32_t w = 0; w < n; ++w) {
            int64_t* cell = &cells_[static_cast<std::size_t>(w)];
            const int64_t v = totalNs_;
            // Never behind what the worker already read: after a restart it polls the REAL clock until this runs on its thread, which
            // may be later than resync() read it (the lease wheels assert a monotonic clock).
            const int64_t set = node.onWorkerThreadForTest(w, [cell, v](Worker& wk) -> int64_t {
                *cell = std::max(v, wk.leaseNowForTest());
                wk.setLeaseClockForTest(cell);
                return *cell;
            });
            totalNs_ = std::max(totalNs_, set);
        }
    }
    /// Moves the clock forward by `ns` on every worker of `node` (a stopped node: the total only, resumed by the next install()).
    void advance(BrokerNode* node, int64_t ns) {
        totalNs_ += ns;
        if (node == nullptr) return;
        const int32_t n = node->workerCountForTest();
        for (int32_t w = 0; w < n && w < static_cast<int32_t>(cells_.size()); ++w) {
            int64_t* cell = &cells_[static_cast<std::size_t>(w)];
            (void)node->onWorkerThreadForTest(w, [cell, ns](Worker&) { *cell += ns; return 0; });
        }
    }

private:
    int64_t              totalNs_;
    std::deque<int64_t>  cells_;   // stable addresses; element w is touched only by worker w's thread once installed
};

}  // namespace rune::node::test
