#pragma once
// PlanSources.h - the source topic ids of a CreateView / AlterView plan blob (authentication D19: a view reads its
// sources, so creating one needs READ on each).  Cold: the gateway decodes the authored blob once per view DDL.
#include <cstdint>
#include <optional>
#include <span>
#include <vector>

namespace rune::view {

/// The authored plan's `sources[].topicId`, in order, duplicates removed.  nullopt = the blob does not decode (the
/// admin thread refuses it with its existing error, nothing is created).
[[nodiscard]] std::optional<std::vector<int32_t>> planSourceTopicIds(std::span<const uint8_t> planBlob);

} // namespace rune::view
