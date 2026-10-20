#pragma once
// FilterSchema.h — Phase 2c: per-tag, per-version field slots for the broker-side filter predicate (spec §6),
// and parseFilter (structural + schema validation of filter bytes).
#include <rune/codec/Descriptor.h>
#include <rune/codec/FilterWire.h>
#include <rune/codec/PrimType.h>

#include <array>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <span>
#include <vector>

namespace rune::codec {

/// Where one field lives in a record VALUE ([blockLength u16][version u16][fixed block]) at one version.
/// offset = 4 + the field's offset in the fixed block.
struct FieldSlot {
    uint16_t offset  = 0;
    uint8_t  width   = 0;
    PrimType type    = PrimType::Int32;
    bool     present = false;
};

class FilterSchema {
public:
    enum class Ineligible : uint8_t { No, VariableLength, Float, FixedStrTooLong, FixedStrEmpty };
    enum class ExtendResult : uint8_t { Extended, Stale, Incompatible };
    struct Field {
        uint16_t tag = 0;
        PrimType type = PrimType::Int32;
        uint16_t fixedLen = 0;
        Ineligible why = Ineligible::No;
        std::vector<FieldSlot> byVersion;   // indexed by version; [0] unused
    };

    /// Cold.  Every field ever declared in `evolving` (live and retired), sorted by tag; slots for v in
    /// 1..currentVersion from materializeAsOf(evolving, v).  Throws std::invalid_argument when currentVersion is 0 or 0xFFFF (the
    /// codec's "never dropped" sentinel); a malformed descriptor throws from materializeAsOf.
    static FilterSchema build(const Descriptor& evolving, uint16_t currentVersion);

    /// Cold.  In place: extend with a newer build of the SAME topic (more versions, more tags); existing field
    /// indices stay valid (but this may reallocate: references from field() are invalidated, so hold indices).
    ///   Extended     - applied.
    ///   Stale        - newer.maxVersion() <= ours and otherwise consistent; *this unchanged (a harmless re-delivery).
    ///   Incompatible - a tag's type or fixedLen differs; *this unchanged (a real conflict).
    [[nodiscard]] ExtendResult extendFrom(const FilterSchema& newer);

    [[nodiscard]] int32_t      indexOfTag(uint16_t tag) const noexcept;   // -1 = unknown
    [[nodiscard]] const Field& field(int32_t idx) const noexcept { return fields_[static_cast<std::size_t>(idx)]; }
    [[nodiscard]] std::size_t  fieldCount() const noexcept { return fields_.size(); }
    [[nodiscard]] uint16_t     maxVersion() const noexcept { return maxVersion_; }
    [[nodiscard]] bool         knowsVersion(uint16_t v) const noexcept { return v >= 1 && v <= maxVersion_; }
    [[nodiscard]] FieldSlot    slot(int32_t idx, uint16_t version) const noexcept {
        const auto& v = fields_[static_cast<std::size_t>(idx)].byVersion;
        return version < v.size() ? v[version] : FieldSlot{};
    }

private:
    std::vector<Field> fields_;     // in first-seen order (sorted by tag at build; extendFrom appends)
    uint16_t maxVersion_ = 0;
};

struct ParsedTerm {
    int32_t  field = -1;
    uint16_t tag = 0;
    FilterOp op = FilterOp::Eq;
    PrimType type = PrimType::Int32;
    uint8_t  width = 0;
    uint8_t  count = 0;
    std::array<FilterKey, kMaxFilterInValues> values{};
};
struct ParsedFilter {
    std::array<ParsedTerm, kMaxFilterTerms> terms{};
    uint8_t termCount = 0;
};

/// Structural + schema validation (spec §6 step 1).  Errors name the term index.  No allocation.
[[nodiscard]] std::expected<ParsedFilter, FilterError> parseFilter(std::span<const uint8_t> bytes,
                                                                   const FilterSchema& schema) noexcept;

}  // namespace rune::codec
