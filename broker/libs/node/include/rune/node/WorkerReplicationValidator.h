#pragma once

#include <cstdint>
#include <string>
#include <vector>
#include <utility>

namespace rune::node {

struct WorkerCountCheck {
    bool ok;
    std::string error;
};

/// Pure function: returns ok=true iff every peer's advertised workerCount equals self.
/// A peer with workerCount==0 (unknown/not-yet-advertised) is tolerated (skipped),
/// so a node can start before all peers have advertised; re-checked on membership change.
inline WorkerCountCheck validateSymmetricWorkerCount(
    int32_t selfWorkerCount,
    const std::vector<std::pair<int32_t /*nodeId*/, int32_t /*workerCount*/>>& peers) noexcept
{
    for (auto& [nodeId, wc] : peers) {
        if (wc == 0) continue;                 // not yet advertised — tolerate
        if (wc != selfWorkerCount)
            return { false, "per-worker replication requires equal RUNE_WORKERS: node "
                     + std::to_string(nodeId) + " advertises " + std::to_string(wc)
                     + " but self has " + std::to_string(selfWorkerCount) };
    }
    return { true, {} };
}

} // namespace rune::node
