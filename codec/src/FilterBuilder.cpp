// src/FilterBuilder.cpp — Phase 2c runtime filter builder.
#include <rune/codec/Filter.h>
#include <rune/codec/FilterBuilder.h>

#include <cstring>
#include <type_traits>
#include <utility>

namespace rune::codec {

namespace {
template <class T> bool intKey(const FilterLiteral& lit, FilterKey& out, bool& range) {
    int64_t sv = 0; uint64_t uv = 0; bool isSigned = false;
    if (const auto* i = std::get_if<int64_t>(&lit)) { sv = *i; isSigned = true; }
    else if (const auto* u = std::get_if<uint64_t>(&lit)) uv = *u;
    else return false;   // bool / string: type mismatch
    const bool ok = isSigned ? std::in_range<T>(sv) : std::in_range<T>(uv);
    range = ok;
    if (ok) {
        const T v = isSigned ? static_cast<T>(sv) : static_cast<T>(uv);
        out = FilterKey{0, static_cast<uint64_t>(static_cast<std::make_unsigned_t<T>>(v))};
    }
    return true;
}
}  // namespace

FilterBuilder FilterBuilder::on(const Descriptor& d) {
    FilterBuilder b;
    b.d_ = &d;
    return b;
}

void FilterBuilder::fail_(FilterErrorKind k, uint8_t termIndex, uint16_t tag) noexcept {
    if (!err_) err_ = FilterError{k, termIndex, tag};
}

FilterBuilder& FilterBuilder::field(std::string_view name) {
    if (err_) return *this;
    const uint8_t idx = n_;
    const FieldDescriptor* f = nullptr;
    for (const auto& fd : d_->fields)
        if (fd.name == name && fd.untilVersion == 0xFFFF) { f = &fd; break; }
    if (!f) { fail_(FilterErrorKind::UnknownField, idx, 0); return *this; }
    switch (f->type) {
        case PrimType::String: case PrimType::Bytes: fail_(FilterErrorKind::VariableLength, idx, f->id); return *this;
        case PrimType::Float64: fail_(FilterErrorKind::FloatField, idx, f->id); return *this;
        default: break;
    }
    if (f->type == PrimType::FixedStr && f->fixedLen == 0) { fail_(FilterErrorKind::FixedStrEmpty, idx, f->id); return *this; }
    if (f->type == PrimType::FixedStr && f->fixedLen > kMaxFilterFixedStrLen) { fail_(FilterErrorKind::FixedStrTooLong, idx, f->id); return *this; }
    if (n_ == terms_.size()) { fail_(FilterErrorKind::TooManyTerms, 0xFF, f->id); return *this; }
    Pending& t = terms_[n_++];
    t = Pending{};
    t.tag = f->id; t.type = f->type;
    t.width = filterValueWidth(f->type, f->fixedLen);
    return *this;
}

bool FilterBuilder::addValue_(Pending& t, const FilterLiteral& lit, uint8_t termIndex) {
    FilterKey k{};
    bool range = true, typed = true;
    switch (t.type) {
        case PrimType::Bool:
            if (const auto* b = std::get_if<bool>(&lit)) k = FilterKey{0, *b ? 1u : 0u}; else typed = false;
            break;
        case PrimType::Int32:  typed = intKey<int32_t>(lit, k, range); break;
        case PrimType::Enum:   typed = intKey<int32_t>(lit, k, range); break;
        case PrimType::Int64:  typed = intKey<int64_t>(lit, k, range); break;
        case PrimType::UInt16: typed = intKey<uint16_t>(lit, k, range); break;
        case PrimType::UInt32: typed = intKey<uint32_t>(lit, k, range); break;
        case PrimType::UInt64: typed = intKey<uint64_t>(lit, k, range); break;
        case PrimType::FixedStr:
            if (const auto* s = std::get_if<std::string_view>(&lit)) {
                if (s->size() > t.width) range = false;
                else {
                    uint8_t buf[kMaxFilterFixedStrLen] = {};
                    if (!s->empty()) std::memcpy(buf, s->data(), s->size());
                    k = loadFilterKey(buf, t.width);
                }
            } else typed = false;
            break;
        default: typed = false; break;
    }
    if (!typed) { fail_(FilterErrorKind::TypeMismatch, termIndex, t.tag); return false; }
    if (!range) { fail_(FilterErrorKind::ValueOutOfRange, termIndex, t.tag); return false; }
    t.v[t.count++] = k;
    return true;
}

FilterBuilder& FilterBuilder::eq(FilterLiteral v) {
    if (err_) return *this;
    if (n_ == 0) { fail_(FilterErrorKind::Malformed, 0xFF, 0); return *this; }
    Pending& t = terms_[n_ - 1];
    t.count = 0; t.op = FilterOp::Eq;
    addValue_(t, v, static_cast<uint8_t>(n_ - 1));
    return *this;
}

FilterBuilder& FilterBuilder::in(std::span<const FilterLiteral> vs) {
    if (err_) return *this;
    if (n_ == 0) { fail_(FilterErrorKind::Malformed, 0xFF, 0); return *this; }
    Pending& t = terms_[n_ - 1];
    const auto idx = static_cast<uint8_t>(n_ - 1);
    t.count = 0; t.op = FilterOp::In;
    if (vs.size() > kMaxFilterInValues) { fail_(FilterErrorKind::TooManyValues, idx, t.tag); return *this; }
    if (vs.empty()) { fail_(FilterErrorKind::ValueCount, idx, t.tag); return *this; }
    for (const auto& lit : vs)
        if (!addValue_(t, lit, idx)) break;
    return *this;
}

FilterBuilder& FilterBuilder::in(std::initializer_list<FilterLiteral> vs) {
    return in(std::span<const FilterLiteral>(vs.begin(), vs.size()));
}

std::expected<FilterBytes, FilterError> FilterBuilder::build() const {
    if (err_) return std::unexpected(*err_);
    FilterBytes out;
    if (n_ == 0) return out;
    std::array<uint8_t, kMaxFilterTerms + 1> order{};
    for (uint8_t i = 0; i < n_; ++i) order[i] = i;
    for (uint8_t i = 1; i < n_; ++i) {   // stable insertion sort by tag (<= 9 items, no allocation)
        const uint8_t x = order[i];
        uint8_t j = i;
        while (j > 0 && terms_[order[j - 1]].tag > terms_[x].tag) { order[j] = order[j - 1]; --j; }
        order[j] = x;
    }
    FilterWriter w(std::span<uint8_t>(out.bytes.data(), out.bytes.size()));
    for (uint8_t i = 0; i < n_; ++i) {
        const Pending& t = terms_[order[i]];
        w.term(t.tag, t.type, t.width, t.op, std::span<const FilterKey>(t.v.data(), t.count));
    }
    auto r = w.finish();
    if (!r) return std::unexpected(r.error());
    out.len = static_cast<uint16_t>(*r);
    return out;
}

}  // namespace rune::codec
