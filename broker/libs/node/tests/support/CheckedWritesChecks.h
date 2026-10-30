#pragma once
// CheckedWritesChecks.h - the invariants of a verdict source's log, shared by the matrix, the model check and the cluster stress.
#include "CheckedWritesModel.h"

#include <algorithm>
#include <map>
#include <string>
#include <tuple>
#include <vector>

namespace rune::node::cw {

/// Markers may overlap (a new activation re-marks from W while an uncommitted marker sits above it): in an overlap the entries
/// must be identical, and together the markers must cover [firstChange + 1, ...) without a gap.  Returns the problem, or "".
/// `reach` receives the end of the union.
inline std::string markerUnionProblem(const std::vector<VerdictRig::RigMarker>& markers, int64_t firstChange, int64_t* reach = nullptr) {
    std::map<int64_t, std::pair<uint16_t, uint32_t>> seen;
    int64_t end = firstChange + 1;
    for (const auto& m : markers) {
        const int64_t from = m.header.fromOffset, through = m.header.finalThrough;
        if (from > end) return "markers leave a gap at [" + std::to_string(end) + ", " + std::to_string(from) + ")";
        const int64_t overlapTo = std::min(through, end);
        std::map<int64_t, std::pair<uint16_t, uint32_t>> mine;
        for (const auto& [o, code, cid] : m.entries) mine[o] = {code, cid};
        for (auto it = seen.lower_bound(from); it != seen.end() && it->first < overlapTo; ++it) {
            const auto f = mine.find(it->first);
            if (f == mine.end() || f->second != it->second) return "marker at " + std::to_string(m.offset) + " disagrees with an earlier one at offset " + std::to_string(it->first);
        }
        for (const auto& [o, v] : mine) {
            if (o < overlapTo) {
                const auto f = seen.find(o);
                if (f == seen.end() || f->second != v) return "marker at " + std::to_string(m.offset) + " lists offset " + std::to_string(o) + " that an earlier one does not";
            }
            seen[o] = v;
        }
        end = std::max(end, through);
    }
    if (reach != nullptr) *reach = end;
    return {};
}

}  // namespace rune::node::cw
