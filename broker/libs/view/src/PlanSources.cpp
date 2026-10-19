// PlanSources.cpp - see rune/view/PlanSources.h.
#include <rune/view/PlanSources.h>

#include <rune/view/ViewPlan.h>

#include <algorithm>

namespace rune::view {

std::optional<std::vector<int32_t>> planSourceTopicIds(std::span<const uint8_t> planBlob) {
    const std::optional<ViewPlan> plan = deserializeAuthored(planBlob);
    if (!plan) return std::nullopt;
    std::vector<int32_t> ids;
    ids.reserve(plan->sources.size());
    for (const SourceSpec& s : plan->sources)
        if (std::find(ids.begin(), ids.end(), s.topicId) == ids.end()) ids.push_back(s.topicId);
    return ids;
}

} // namespace rune::view
