#include <rune/client/ConstraintNames.h>

#include <rune/codec/ViewPlanAuthored.h>

#include <variant>

namespace rune::client {

namespace {

std::optional<std::string> constraintOf(std::span<const uint8_t> planBlob, uint16_t ordinal) {
    const auto plan = rune::view::deserializeAuthored(planBlob);
    if (!plan) return std::nullopt;
    if (ordinal == 0) return plan->unique ? std::optional<std::string>("unique") : std::nullopt;
    if (ordinal > plan->constraints.size()) return std::nullopt;
    return plan->constraints[ordinal - 1].name;
}

const rune::catalog::ViewDef* viewDefOf(const rune::catalog::CatalogEvent& e) {
    if (const auto* c = std::get_if<rune::catalog::CreateViewOps>(&e.ops)) return &c->view;
    if (const auto* a = std::get_if<rune::catalog::AlterViewOps>(&e.ops)) return &a->view;
    return nullptr;
}

}  // namespace

std::optional<ConstraintName> resolveConstraintName(const rune::catalog::CatalogState& state,
                                                    std::span<const rune::catalog::RecordedEvent> events, uint32_t constraintId) {
    if (constraintId == 0) return std::nullopt;
    const uint64_t objectId = constraintId >> 16;
    const uint16_t ordinal  = static_cast<uint16_t>(constraintId & 0xFFFF);
    if (const rune::catalog::ViewObject* v = state.viewByObjectId(objectId))
        if (auto name = constraintOf(v->def.normalizedPlanBlob, ordinal)) return ConstraintName{v->name, std::move(*name)};
    for (auto it = events.rbegin(); it != events.rend(); ++it) {
        if (it->event.objectId != objectId) continue;
        const rune::catalog::ViewDef* def = viewDefOf(it->event);
        if (def == nullptr) continue;
        if (auto name = constraintOf(def->normalizedPlanBlob, ordinal)) return ConstraintName{it->event.objectName, std::move(*name)};
    }
    return std::nullopt;
}

}  // namespace rune::client
