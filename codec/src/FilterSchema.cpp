#include <rune/codec/FilterSchema.h>

#include <rune/codec/Evolve.h>

#include <algorithm>
#include <stdexcept>

namespace rune::codec {

namespace {
FilterSchema::Ineligible classify(const FieldDescriptor& f) noexcept {
    switch (f.type) {
        case PrimType::Float64: return FilterSchema::Ineligible::Float;
        case PrimType::String: case PrimType::Bytes: return FilterSchema::Ineligible::VariableLength;
        default: break;
    }
    if (isFilterable(f)) return FilterSchema::Ineligible::No;
    return f.fixedLen == 0 ? FilterSchema::Ineligible::FixedStrEmpty : FilterSchema::Ineligible::FixedStrTooLong;
}
}  // namespace

FilterSchema FilterSchema::build(const Descriptor& evolving, uint16_t currentVersion) {
    if (currentVersion == 0 || currentVersion == 0xFFFF)
        throw std::invalid_argument("FilterSchema::build: currentVersion must be 1..0xFFFE");
    FilterSchema s;
    s.maxVersion_ = currentVersion;
    s.fields_.reserve(evolving.fields.size());
    for (const auto& f : evolving.fields) {
        Field x;
        x.tag = f.id; x.type = f.type; x.fixedLen = f.fixedLen; x.why = classify(f);
        x.byVersion.assign(static_cast<std::size_t>(currentVersion) + 1, FieldSlot{});
        s.fields_.push_back(std::move(x));
    }
    std::sort(s.fields_.begin(), s.fields_.end(), [](const Field& a, const Field& b) { return a.tag < b.tag; });
    for (uint32_t v = 1; v <= currentVersion; ++v) {
        const auto ver = static_cast<uint16_t>(v);
        const Descriptor m = materializeAsOf(evolving, ver);
        for (const auto& f : m.fields) {
            const int32_t i = s.indexOfTag(f.id);
            if (i < 0) continue;
            Field& x = s.fields_[static_cast<std::size_t>(i)];
            if (x.why != Ineligible::No) continue;
            x.byVersion[v] = FieldSlot{static_cast<uint16_t>(4 + f.offset), filterValueWidth(f.type, f.fixedLen), f.type, true};
        }
    }
    return s;
}

FilterSchema::ExtendResult FilterSchema::extendFrom(const FilterSchema& newer) {
    for (const auto& n : newer.fields_) {
        const int32_t i = indexOfTag(n.tag);
        if (i >= 0) {
            const Field& o = fields_[static_cast<std::size_t>(i)];
            if (o.type != n.type || o.fixedLen != n.fixedLen) return ExtendResult::Incompatible;
        }
    }
    if (newer.maxVersion_ <= maxVersion_) return ExtendResult::Stale;
    // Validated: nothing below can refuse.
    for (const auto& n : newer.fields_) {
        int32_t i = indexOfTag(n.tag);
        if (i < 0) {
            Field x;
            x.tag = n.tag; x.type = n.type; x.fixedLen = n.fixedLen; x.why = n.why;
            fields_.push_back(std::move(x));
            i = static_cast<int32_t>(fields_.size()) - 1;
        }
        Field& o = fields_[static_cast<std::size_t>(i)];
        const std::size_t want = static_cast<std::size_t>(newer.maxVersion_) + 1;
        if (o.byVersion.size() < want) o.byVersion.resize(want);
        for (std::size_t v = static_cast<std::size_t>(maxVersion_) + 1; v < want && v < n.byVersion.size(); ++v)
            o.byVersion[v] = n.byVersion[v];
    }
    for (auto& f : fields_)   // fields the newer build lacks keep their old slots; pad to the new length
        f.byVersion.resize(static_cast<std::size_t>(newer.maxVersion_) + 1);
    maxVersion_ = newer.maxVersion_;
    return ExtendResult::Extended;
}

int32_t FilterSchema::indexOfTag(uint16_t tag) const noexcept {
    for (std::size_t i = 0; i < fields_.size(); ++i)
        if (fields_[i].tag == tag) return static_cast<int32_t>(i);
    return -1;
}

std::expected<ParsedFilter, FilterError> parseFilter(std::span<const uint8_t> b, const FilterSchema& schema) noexcept {
    const auto fail = [](FilterErrorKind k, uint8_t term = 0xFF, uint16_t tag = 0) {
        return std::unexpected(FilterError{k, term, tag});
    };
    if (b.size() < kFilterHeaderBytes) return fail(FilterErrorKind::Malformed);
    if (b[0] != kFilterFormatVer) return fail(FilterErrorKind::BadFormatVersion);
    const uint8_t n = b[1];
    if (n < 1 || n > kMaxFilterTerms) return fail(FilterErrorKind::TermCount);

    ParsedFilter out;
    std::size_t pos = kFilterHeaderBytes;
    for (uint8_t t = 0; t < n; ++t) {
        if (b.size() - pos < kFilterTermHeaderBytes) return fail(FilterErrorKind::Malformed, t);
        ParsedTerm& pt = out.terms[t];
        const uint16_t tag = static_cast<uint16_t>(b[pos] | (b[pos + 1] << 8));
        const uint8_t  prim = b[pos + 2], op = b[pos + 3], count = b[pos + 4];
        pos += kFilterTermHeaderBytes;
        if (op != static_cast<uint8_t>(FilterOp::Eq) && op != static_cast<uint8_t>(FilterOp::In))
            return fail(FilterErrorKind::UnknownOp, t, tag);
        if (op == static_cast<uint8_t>(FilterOp::Eq) ? count != 1 : (count < 1 || count > kMaxFilterInValues))
            return fail(FilterErrorKind::ValueCount, t, tag);
        if (t > 0 && tag <= out.terms[t - 1].tag) return fail(FilterErrorKind::NotCanonical, t, tag);
        const int32_t fi = schema.indexOfTag(tag);
        if (fi < 0) return fail(FilterErrorKind::UnknownTag, t, tag);
        const auto& f = schema.field(fi);
        switch (f.why) {
            case FilterSchema::Ineligible::VariableLength: return fail(FilterErrorKind::VariableLength, t, tag);
            case FilterSchema::Ineligible::Float:          return fail(FilterErrorKind::FloatField, t, tag);
            case FilterSchema::Ineligible::FixedStrEmpty:  return fail(FilterErrorKind::FixedStrEmpty, t, tag);
            case FilterSchema::Ineligible::FixedStrTooLong:return fail(FilterErrorKind::FixedStrTooLong, t, tag);
            case FilterSchema::Ineligible::No: break;
        }
        if (prim != static_cast<uint8_t>(f.type)) return fail(FilterErrorKind::TypeMismatch, t, tag);
        const uint8_t width = filterValueWidth(f.type, f.fixedLen);
        if (width == 0) return fail(FilterErrorKind::TypeMismatch, t, tag);
        if ((b.size() - pos) / width < count) return fail(FilterErrorKind::Malformed, t, tag);
        pt.field = fi; pt.tag = tag; pt.op = static_cast<FilterOp>(op); pt.type = f.type; pt.width = width; pt.count = count;
        for (uint8_t i = 0; i < count; ++i) {
            pt.values[i] = loadFilterKey(b.data() + pos, width);
            pos += width;
            if (i > 0 && !(pt.values[i - 1] < pt.values[i])) return fail(FilterErrorKind::NotCanonical, t, tag);
        }
    }
    if (pos != b.size()) return fail(FilterErrorKind::Malformed);
    out.termCount = n;
    return out;
}

}  // namespace rune::codec
