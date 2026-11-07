// include/rune/client/ConstraintNames.h - which view constraint a constraintId (viewObjectId << 16 | ordinal) names.
#pragma once

#include <rune/catalog/CatalogReplayer.h>
#include <rune/catalog/CatalogState.h>
#include <rune/client/WriteResult.h>

#include <cstdint>
#include <optional>
#include <span>

namespace rune::client {

/// Id 0 (the key index) and ids no view definition ever had name nothing.  The live view of that object id answers first;
/// else the newest CreateView / AlterView of it in `events` whose plan has the ordinal, so a replaced or dropped view still
/// resolves.
[[nodiscard]] std::optional<ConstraintName> resolveConstraintName(const rune::catalog::CatalogState& state,
                                                                  std::span<const rune::catalog::RecordedEvent> events,
                                                                  uint32_t constraintId);

}  // namespace rune::client
