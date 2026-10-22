#pragma once
// FilterBuilder.h — Phase 2c: the runtime, name-based way to build filter bytes (spec §4) for callers that
// only know the topic's descriptor at run time (CLI, probes, dynamic clients).  Cold path: it validates every
// literal and reports the first error from build(); it never allocates.  The bytes equal the generated
// E::Filter's for the same constraints.
#include <rune/codec/Descriptor.h>
#include <rune/codec/FilterWire.h>

#include <algorithm>
#include <array>
#include <concepts>
#include <cstdint>
#include <expected>
#include <initializer_list>
#include <optional>
#include <span>
#include <string_view>
#include <type_traits>
#include <variant>

namespace rune::codec {

using FilterLiteral = std::variant<int64_t, uint64_t, bool, std::string_view>;

class FilterBuilder {
public:
    /// `d` is the topic's CURRENT descriptor (live fields only are addressable); it must outlive the builder.
    static FilterBuilder on(const Descriptor& d);
    /// Starts a term on the named field.  Errors (reported by build()): UnknownField, VariableLength (String /
    /// Bytes), FloatField (Float64), FixedStrTooLong / FixedStrEmpty.
    FilterBuilder& field(std::string_view name);
    /// Sets the current field's constraint (a later eq / in on the same term replaces the earlier one).
    FilterBuilder& eq(FilterLiteral v);
    FilterBuilder& in(std::span<const FilterLiteral> vs);
    FilterBuilder& in(std::initializer_list<FilterLiteral> vs);
    /// Integer conveniences (a plain `42u` or a uint32_t is ambiguous for the variant): converted to int64_t /
    /// uint64_t by signedness; the field's range check is unchanged.
    template <std::integral I> requires(!std::same_as<I, bool>)
    FilterBuilder& eq(I v) { return eq(toLiteral_(v)); }
    template <std::integral I> requires(!std::same_as<I, bool>)
    FilterBuilder& in(std::initializer_list<I> vs) {
        std::array<FilterLiteral, kMaxFilterInValues + 1> a{};   // 65th slot makes "too many" visible
        const std::size_t n = std::min(vs.size(), a.size());
        std::size_t i = 0;
        for (I v : vs) { if (i == n) break; a[i++] = toLiteral_(v); }
        return in(std::span<const FilterLiteral>(a.data(), n));
    }
    /// Terms sorted by tag, In values sorted and de-duplicated; no terms = an empty FilterBytes (no filter).
    [[nodiscard]] std::expected<FilterBytes, FilterError> build() const;

private:
    explicit FilterBuilder(const Descriptor& d) noexcept : d_(&d) {}   // no default constructor: a builder always has its descriptor
    template <std::integral I> static FilterLiteral toLiteral_(I v) noexcept {
        if constexpr (std::is_signed_v<I>) return FilterLiteral{static_cast<int64_t>(v)};
        else return FilterLiteral{static_cast<uint64_t>(v)};
    }
    struct Pending {
        uint16_t tag = 0;
        PrimType type = PrimType::Int32;
        uint8_t  width = 0;
        FilterOp op = FilterOp::Eq;
        uint8_t  count = 0;
        std::array<FilterKey, kMaxFilterInValues> v{};
    };
    bool addValue_(Pending& t, const FilterLiteral& lit, uint8_t termIndex);
    void fail_(FilterErrorKind k, uint8_t termIndex, uint16_t tag) noexcept;

    const Descriptor* d_;
    std::array<Pending, kMaxFilterTerms + 1> terms_{};   // one spare slot: the 9th term is reported, not dropped
    uint8_t n_ = 0;
    std::optional<FilterError> err_;
};

}  // namespace rune::codec
