// include/rune/client/view/ViewPlanAuthor.h
//
// Client-side authoring of a materialized-view CreateView plan blob (SchemaMessages.h
// wire id 70).  The authored plan and its wire encoding live in the shared codec
// (rune/codec/ViewPlanAuthored.h), so the client and the broker cannot drift; this header
// re-exports them under rune::client::view and keeps the client-only remapSources().
//
// Cold control-plane path (CreateView is a one-time, per-view DDL op): no shared mutable state,
// caller owns the ViewPlan value.
#pragma once

#include <rune/codec/ViewPlanAuthored.h>

#include <cstddef>
#include <cstdint>
#include <span>

namespace rune::client::view {

using MergeRule = rune::view::MergeRule;
using ViewSizing = rune::view::ViewSizing;
using AuthoredField = rune::view::AuthoredField;
using SourceSpec = rune::view::SourceSpec;
using KeyComponent = rune::view::KeyComponent;
using PredicateOp = rune::view::PredicateOp;
using PredicateNode = rune::view::PredicateNode;
using AuthoredConstraint = rune::view::AuthoredConstraint;
using ViewPlan = rune::view::ViewPlan;
using rune::view::serializeAuthored;
using rune::view::deserializeAuthored;

// The v1 version byte: a plan with no v2 fields serializes to it (rune::view::kAuthoredVersion
// is the current, v2 byte).
inline constexpr uint8_t kAuthoredViewPlanVersion = 1;

// Sets source i's topicId to topicIds[i] (a normalized plan names its sources by ordinal,
// ruling 11; a replay maps them onto the TARGET cluster's topic ids).  False — and the plan
// untouched — when the counts differ.
inline bool remapSources(ViewPlan& plan, std::span<const int32_t> topicIds) {
    if (plan.sources.size() != topicIds.size()) return false;
    for (std::size_t i = 0; i < topicIds.size(); ++i) plan.sources[i].topicId = topicIds[i];
    return true;
}

} // namespace rune::client::view
