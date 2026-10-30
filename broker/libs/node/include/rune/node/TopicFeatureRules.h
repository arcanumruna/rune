#pragma once
// TopicFeatureRules.h — checked writes Part 02: which database features a topic's shape allows, in one table (the
// messages are the user-facing refusals; later parts call featureRefusal from their own DDL).  The rule order is
// untyped, then keyless, then transient.
#include <rune/catalog/CatalogState.h>

#include <cstdint>
#include <optional>
#include <string_view>

namespace rune::node {

enum class DbFeature : uint8_t { View, Filter, KeyIndex, AutoIncrement, Constraint };

struct TopicShape {
    bool typed     = false;
    bool keyed     = false;
    bool transient = false;
};

[[nodiscard]] constexpr TopicShape topicShapeOf(const rune::catalog::TopicObject& t) noexcept {
    const bool typed = t.schema.has_value();
    return {typed, typed && !t.schema->keyField.empty(), t.persistence == rune::catalog::Persistence::Transient};
}

/// nullopt = the feature is allowed on a topic of this shape.
[[nodiscard]] constexpr std::optional<std::string_view> featureRefusal(DbFeature f, const TopicShape& s) noexcept {
    switch (f) {
    case DbFeature::View:
        if (!s.typed) return "view source must be a typed topic";
        if (!s.keyed) return "view source needs a key";
        return std::nullopt;
    case DbFeature::Filter:
        if (!s.typed) return "filter needs a typed topic";
        return std::nullopt;
    case DbFeature::KeyIndex:
        if (!s.typed) return "key.index needs a typed topic";
        if (!s.keyed) return "key.index needs a key";
        if (s.transient) return "key.index needs a persisted topic";
        return std::nullopt;
    case DbFeature::AutoIncrement:
        if (!s.typed) return "autoincrement needs a typed topic";
        if (!s.keyed) return "autoincrement needs a key";
        if (s.transient) return "autoincrement needs a persisted topic";
        return std::nullopt;
    case DbFeature::Constraint:
        if (!s.typed) return "constraints need a typed topic";
        if (!s.keyed) return "constraints need a key";
        if (s.transient) return "constraints need a persisted topic";
        return std::nullopt;
    }
    return std::nullopt;
}

}  // namespace rune::node
