// codec/include/rune/codec/ViewPlanAuthored.h
//
// The authoring form of a view definition (what a `CreateView` statement produces before
// compilation) and its flat little-endian wire encoding - the exact bytes a `CreateView`
// planBlob carries.  Shared by the broker and the client.  v1 is the original layout; v2
// appends the wide-key components, the unique flag, the checked-write constraints, the Unicode
// version and the GROUP BY columns.  A plan that needs none of them still serializes as v1.
#pragma once
#include <rune/codec/PrimType.h>

#include <array>
#include <cstdint>
#include <optional>
#include <span>
#include <string>
#include <vector>

namespace rune::view {

// Wire values are APPEND-ONLY (serialized as u8 in plan blobs): CountAll (= SQL Count(*), counts
// contributors including nulls) was appended at 7 in Phase 2a; LastNonNull (most recent non-null
// contributor value) at 8 in Phase 2b.
enum class MergeRule : uint8_t { Replace, Count, Sum, Avg, Min, Max, Last, CountAll, LastNonNull };

// WITH-clause sizing knobs for a materialized view.  0 fields take the broker's own defaults.
struct ViewSizing {
    uint32_t chunkBytes = 32u << 20;  // ring/table chunk size, bytes
    uint32_t capacity    = 0;          // 0 = default pre-size
    uint64_t maxRows     = 0;          // 0 = unbounded
};

// One output field as authored (names, not resolved ids).  `sourceIdx` indexes into
// ViewPlan::sources; `srcField` is the upstream field name to resolve.
struct AuthoredField {
    std::string name;
    rune::codec::PrimType type;
    uint16_t sourceIdx = 0;
    std::string srcField;
    MergeRule rule = MergeRule::Replace;
};

// One upstream source topic as authored (by topicId plus its key field name, resolved against
// that topic's schema by the compiler).
struct SourceSpec {
    int32_t topicId = -1;
    std::string keyField;
};

// One component of a wide view key (or a GROUP BY column): a source column, an optional key
// function (rune::codec::kKeyFunction*; a raw byte, the compiler refuses unknown values) and the
// name it carries in the view.
struct KeyComponent {
    std::string column;
    uint8_t keyFunction = 0;
    std::string outName;
    bool operator==(const KeyComponent&) const = default;
};

enum class PredicateOp : uint8_t { Column = 1, Const = 2, Eq = 3, Ne = 4, Lt = 5, Le = 6, Gt = 7, Ge = 8, And = 9, Or = 10, Not = 11 };

// One node of a flat predicate tree, root last.  Column reads `column`; Const carries
// `constType`/`constBytes`; the rest reference earlier nodes by index through `a` (and `b`).
struct PredicateNode {
    PredicateOp op;
    uint16_t a = 0, b = 0;
    std::string column;
    rune::codec::PrimType constType{};
    std::array<uint8_t, 16> constBytes{};
    bool operator==(const PredicateNode&) const = default;
};

struct AuthoredConstraint {
    std::string name;
    std::vector<PredicateNode> predicate;
    bool operator==(const AuthoredConstraint&) const = default;
};

// The authoring form of a view definition.  Carried as the wire `planBlob`.
struct ViewPlan {
    std::string viewName;
    std::string key;
    std::vector<SourceSpec> sources;
    std::vector<AuthoredField> output;
    uint8_t persistence = 0;  // 0 = persisted, 1 = transient
    ViewSizing sizing;
    // v2
    std::vector<KeyComponent> keyComponents;
    bool unique = false;
    std::vector<AuthoredConstraint> constraints;
    uint32_t unicodeVersion = 0;
    std::vector<KeyComponent> groupBy;
};

inline constexpr uint8_t kAuthoredVersionV1 = 1, kAuthoredVersion = 2;

// True when the plan cannot be expressed in the v1 layout.
[[nodiscard]] bool needsV2(const ViewPlan& p) noexcept;

// The output type an aggregate rule produces from a source field of type `src` (fixed by rule,
// never cast). nullopt = not aggregatable. Callers have already rejected var-len sources.
[[nodiscard]] std::optional<rune::codec::PrimType> aggregateOutputType(MergeRule rule, rune::codec::PrimType src) noexcept;

// "a,b" of the key components' outNames; `key` for a v1-shaped plan.
[[nodiscard]] std::string keyDisplayName(const ViewPlan& p);

// Both forms lead with a 1-byte VERSION; strings and vectors are length-prefixed with a u32
// count.  Deserialization bounds-checks every read and returns std::nullopt on truncation, an
// unknown version, an out-of-range enum, `unique` > 1 or trailing bytes.
std::vector<uint8_t> serializeAuthored(const ViewPlan& plan);
std::optional<ViewPlan> deserializeAuthored(std::span<const uint8_t> buf);

} // namespace rune::view
