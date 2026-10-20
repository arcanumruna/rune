#pragma once
// FilterPredicate.h — Phase 2c: a parsed filter compiled for the record path (spec §6).
#include <rune/codec/FilterSchema.h>
#include <rune/codec/FilterWire.h>

#include <algorithm>
#include <array>
#include <cstdint>
#include <expected>
#include <span>
#include <vector>

namespace rune::codec {

enum class FilterVerdict : uint8_t { Match, NoMatch, Absent, UnknownVersion, Malformed, NullValue };

[[nodiscard]] inline uint16_t recordVersion(const uint8_t* value) noexcept { return uint16_t(value[2] | (value[3] << 8)); }

class CompiledFilter {
public:
    struct Term { int32_t field = -1; FilterOp op = FilterOp::Eq; uint8_t width = 0; uint8_t count = 0; uint16_t first = 0;
                  PrimType type = PrimType::Int32; uint16_t tag = 0; };

    /// Cold: parses, then copies the values into one right-sized allocation.
    static std::expected<CompiledFilter, FilterError> compile(std::span<const uint8_t> bytes, const FilterSchema& schema) {
        auto p = parseFilter(bytes, schema);
        if (!p) return std::unexpected(p.error());
        CompiledFilter c;
        std::size_t total = 0;
        for (uint8_t i = 0; i < p->termCount; ++i) total += p->terms[i].count;
        c.values_.reserve(total);
        bool haveEq = false;
        for (uint8_t i = 0; i < p->termCount; ++i) {
            const ParsedTerm& pt = p->terms[i];
            Term& t = c.terms_[i];
            t = Term{pt.field, pt.op, pt.width, pt.count, static_cast<uint16_t>(c.values_.size()), pt.type, pt.tag};
            c.values_.insert(c.values_.end(), pt.values.begin(), pt.values.begin() + pt.count);
            if (pt.op == FilterOp::Eq && !haveEq) { c.index_ = i; haveEq = true; }
        }
        c.n_ = p->termCount;
        if (!haveEq) c.index_ = 0;   // no Eq: the first term is an In (terms are in tag order)
        return c;
    }

    /// Eq-first, in tag order; else the first In term.
    [[nodiscard]] uint8_t indexTerm() const noexcept { return index_; }
    [[nodiscard]] uint8_t termCount() const noexcept { return n_; }
    [[nodiscard]] const Term& term(uint8_t i) const noexcept { return terms_[i]; }
    [[nodiscard]] std::span<const FilterKey> values(uint8_t i) const noexcept {
        return {values_.data() + terms_[i].first, terms_[i].count};
    }

    /// Every term.  `isNull` = the record's value is null (tombstone).  Precedence, first that applies:
    ///   NullValue (isNull); Malformed (value.size() < 4, or a present slot runs past the value);
    ///   UnknownVersion (!s.knowsVersion(version)); Absent (a term's slot is not present in that version, or its
    ///   type differs from the term's); NoMatch (a term differs / is not in its In list); else Match.
    [[nodiscard]] FilterVerdict evaluate(std::span<const uint8_t> value, bool isNull, const FilterSchema& s) const noexcept {
        if (isNull) return FilterVerdict::NullValue;
        if (value.size() < 4) return FilterVerdict::Malformed;
        return run_(value, recordVersion(value.data()), s, n_);
    }

    /// Every term except indexTerm() (the shared scan already matched it).  `version` = recordVersion(value).
    [[nodiscard]] FilterVerdict evaluateResidual(std::span<const uint8_t> value, uint16_t version, const FilterSchema& s) const noexcept {
        if (value.size() < 4) return FilterVerdict::Malformed;
        return run_(value, version, s, index_);
    }

private:
    [[nodiscard]] FilterVerdict run_(std::span<const uint8_t> value, uint16_t version, const FilterSchema& s, uint8_t skip) const noexcept {
        if (!s.knowsVersion(version)) return FilterVerdict::UnknownVersion;
        FilterVerdict worst = FilterVerdict::Match;   // Absent outranks NoMatch outranks Match; Malformed returns at once
        for (uint8_t i = 0; i < n_; ++i) {
            if (i == skip) continue;
            const FilterVerdict v = evalTerm_(terms_[i], values(i), value, version, s);
            if (v == FilterVerdict::Malformed) return v;
            if (v == FilterVerdict::Absent) worst = v;
            else if (v == FilterVerdict::NoMatch && worst == FilterVerdict::Match) worst = v;
        }
        return worst;
    }
    [[nodiscard]] static FilterVerdict evalTerm_(const Term& t, std::span<const FilterKey> vals, std::span<const uint8_t> value,
                                                 uint16_t version, const FilterSchema& s) noexcept {
        const FieldSlot sl = s.slot(t.field, version);
        if (!sl.present || sl.type != t.type) return FilterVerdict::Absent;
        if (static_cast<std::size_t>(sl.offset) + sl.width > value.size()) return FilterVerdict::Malformed;
        const FilterKey k = loadFilterKey(value.data() + sl.offset, sl.width);
        if (t.op == FilterOp::Eq) return k == vals[0] ? FilterVerdict::Match : FilterVerdict::NoMatch;
        if (vals.size() <= 8) { for (const FilterKey& v : vals) if (v == k) return FilterVerdict::Match; return FilterVerdict::NoMatch; }
        return std::binary_search(vals.begin(), vals.end(), k) ? FilterVerdict::Match : FilterVerdict::NoMatch;
    }

    std::array<Term, kMaxFilterTerms> terms_{};
    uint8_t n_ = 0, index_ = 0;
    std::vector<FilterKey> values_;   // allocated once at compile (cold)
};

}  // namespace rune::codec
