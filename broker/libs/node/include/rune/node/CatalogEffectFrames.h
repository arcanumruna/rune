#pragma once
// CatalogEffectFrames.h — catalog history Task 7 (fix round 1, C1): the size of
// every admin→control frame an applied catalog event produces, computed from the
// catalog state exactly as the production sink (AdminWorkerAgent) encodes them.
//
// No size that comes from input (a client's DDL, or the bytes a peer replicated)
// may ever abort the broker.  Two defenses use these functions:
//   * the LEADER (Task 9, AdminWorker): catalogEventEffectsRefusal() — a dry run of
//     a candidate event; a refusal is answered InvalidRequest + the detail and the
//     event is NEVER appended;
//   * every FOLLOWER (CatalogApplier): eventEffectFramesError() on the applied
//     state — an oversize effect HALTS apply at that offset (explicit ApplyError +
//     metric; the broker keeps serving what it applied).
// Cold path (control plane): these allocate (descriptor materialization).
#include <rune/catalog/CatalogEvent.h>
#include <rune/catalog/CatalogState.h>

#include <cstddef>
#include <optional>
#include <string>

namespace rune::node {

/// The largest frame the admin→control ring carries: an O2O ring's max message is
/// capacity / 8 (AdminWorkerAgent::failureRing_, kAdminToControlRingBytes).
[[nodiscard]] std::size_t catalogEffectMaxFrame() noexcept;

/// The materialized (as-of its current schema version) descriptor bytes the
/// validator fan-out / a view plan carries for a typed topic.  Throws on a blob
/// that does not deserialize.
[[nodiscard]] std::vector<uint8_t> materializedDescriptorOf(const rune::catalog::TopicObject& t);

/// nullopt = every frame of a topic's final state fits `maxFrame` (TopicApplied +
/// the validator when typed and validated); otherwise the reason.
[[nodiscard]] std::optional<std::string> topicEffectFramesError(const rune::catalog::TopicObject& t,
                                                                std::size_t maxFrame);
/// Same for a view's final state (its sink's TopicApplied + its ViewPlanUpdate,
/// which carries the first source's materialized descriptor from `state`).
[[nodiscard]] std::optional<std::string> viewEffectFramesError(const rune::catalog::ViewObject& v,
                                                               const rune::catalog::CatalogState& state,
                                                               std::size_t maxFrame);
/// Every frame the LIVE effects of `e` (applied as `a`, `after` = the state it
/// produced) post.  nullopt = all fit.
[[nodiscard]] std::optional<std::string> eventEffectFramesError(const rune::catalog::CatalogState& after,
                                                                const rune::catalog::CatalogEvent& e,
                                                                const rune::catalog::Applied& a,
                                                                std::size_t maxFrame);
/// LEADER-side dry run (Task 9 wires it into the AdminWorker): apply a copy of
/// `e` (its fingerprint re-stamped) to a copy of `before` and size every effect
/// frame it would produce — and, for a schema change of a topic, the plan frame of
/// every live view that reads that topic (its source descriptor grows with it).  nullopt = the event's effects fit; otherwise the
/// InvalidRequest detail.  An event the rules refuse is not this function's
/// business: nullopt (the write path reports the apply refusal itself).
[[nodiscard]] std::optional<std::string> catalogEventEffectsRefusal(const rune::catalog::CatalogState& before,
                                                                    const rune::catalog::CatalogEvent& e,
                                                                    std::size_t maxFrame = catalogEffectMaxFrame());

} // namespace rune::node
