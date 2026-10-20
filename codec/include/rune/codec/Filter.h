#pragma once
// Filter.h — Phase 2c: the filter-by-example member type (spec §4) and the canonical encoder (spec §5).
#include <rune/codec/FilterWire.h>
#include <rune/codec/PrimType.h>

#include <array>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <limits>
#include <optional>
#include <span>
#include <type_traits>
#include <tuple>
#include <utility>

namespace rune {

template <class U, std::size_t N> struct AnyOf { std::array<U, N> v; };
template <class U> struct AnyOfSpan { std::span<const U> v; };

template <class... U>
    requires(sizeof...(U) >= 1)
constexpr auto anyOf(U... v) {
    static_assert(sizeof...(U) <= codec::kMaxFilterInValues, "anyOf: at most 64 values (spec §4)");
    using C = std::common_type_t<U...>;
    return AnyOf<C, sizeof...(U)>{{static_cast<C>(v)...}};
}
template <class U> constexpr AnyOfSpan<U> anyOf(std::span<const U> v) noexcept { return {v}; }

template <class T, class U>
concept FilterValueFor = std::same_as<T, U> ||
    (std::integral<T> && std::integral<U> && !std::same_as<T, bool> && !std::same_as<U, bool>);

template <class T> struct IsCharArray : std::false_type {};
template <std::size_t N> struct IsCharArray<std::array<char, N>> : std::true_type {};

/// One member of a generated E::Filter: unset (any value), Eq (one value) or In (up to 64 values, inline).
/// Overflow / OutOfRange are terminal error states: once entered, no later assignment leaves them, so a
/// filter built from bad input can never look valid.  FilterWriter turns them into encode errors.
/// (Assigning a whole other Match replaces the object; assign values, not Matches, to keep the guard.)
template <class T>
class Match {
public:
    enum class State : uint8_t { Unset, Eq, In, Overflow, OutOfRange };
    static constexpr std::size_t kMaxValues = codec::kMaxFilterInValues;

    constexpr Match() noexcept = default;
    template <class U> requires FilterValueFor<T, U>
    constexpr Match(U v) noexcept { setEq_(v); }
    template <class U, std::size_t N> requires FilterValueFor<T, U>
    constexpr Match(const AnyOf<U, N>& a) noexcept { setIn_(a.v.data(), N); }
    template <class U> requires FilterValueFor<T, U>
    constexpr Match(AnyOfSpan<U> a) noexcept { setIn_(a.v.data(), a.v.size()); }

    template <class U> requires FilterValueFor<T, U>
    constexpr Match& operator=(U v) noexcept { setEq_(v); return *this; }
    template <class U, std::size_t N> requires FilterValueFor<T, U>
    constexpr Match& operator=(const AnyOf<U, N>& a) noexcept { setIn_(a.v.data(), N); return *this; }
    template <class U> requires FilterValueFor<T, U>
    constexpr Match& operator=(AnyOfSpan<U> a) noexcept { setIn_(a.v.data(), a.v.size()); return *this; }

    [[nodiscard]] constexpr State       state() const noexcept { return s_; }
    [[nodiscard]] constexpr std::size_t size() const noexcept { return n_; }
    [[nodiscard]] constexpr bool        overflow() const noexcept { return s_ == State::Overflow; }
    [[nodiscard]] constexpr bool        outOfRange() const noexcept { return s_ == State::OutOfRange; }
    [[nodiscard]] constexpr const T&    operator[](std::size_t i) const noexcept { return v_[i]; }

private:
    [[nodiscard]] constexpr bool terminal_() const noexcept { return s_ == State::Overflow || s_ == State::OutOfRange; }
    template <class U> constexpr void setEq_(U x) noexcept {
        if (terminal_()) return;
        n_ = 0; s_ = State::Eq;
        push_(x);
    }
    template <class U> constexpr void setIn_(const U* p, std::size_t count) noexcept {
        if (terminal_()) return;
        n_ = 0;
        if (count > kMaxValues) { s_ = State::Overflow; return; }
        s_ = State::In;
        for (std::size_t i = 0; i < count && s_ == State::In; ++i) push_(p[i]);
    }
    template <class U> constexpr void push_(U x) noexcept {
        if constexpr (!std::same_as<T, U>) {
            if (!std::in_range<T>(x)) { s_ = State::OutOfRange; return; }
        }
        v_[n_++] = static_cast<T>(x);
    }
    std::array<T, kMaxValues> v_{};
    uint8_t n_ = 0;
    State   s_ = State::Unset;
};

/// FixedStr members: std::array<char, N> values; literals are zero-padded like fly::storeFixedStr.
template <std::size_t N>
class Match<std::array<char, N>> {
public:
    using V = std::array<char, N>;
    enum class State : uint8_t { Unset, Eq, In, Overflow, OutOfRange };
    static constexpr std::size_t kMaxValues = codec::kMaxFilterInValues;
    constexpr Match() noexcept = default;
    constexpr Match(const V& v) noexcept { setEq_(v); }
    template <std::size_t M> requires(M - 1 <= N)
    constexpr Match(const char (&lit)[M]) noexcept { setEq_(pad_(lit, M - 1)); }
    template <std::size_t K>
    constexpr Match(const AnyOf<const char*, K>& a) noexcept { setInC_(a.v.data(), K); }
    template <std::size_t K>
    constexpr Match(const AnyOf<V, K>& a) noexcept { setInV_(a.v.data(), K); }

    constexpr Match& operator=(const V& v) noexcept { setEq_(v); return *this; }
    template <std::size_t M> requires(M - 1 <= N)
    constexpr Match& operator=(const char (&lit)[M]) noexcept { setEq_(pad_(lit, M - 1)); return *this; }
    template <std::size_t K>
    constexpr Match& operator=(const AnyOf<const char*, K>& a) noexcept { setInC_(a.v.data(), K); return *this; }
    template <std::size_t K>
    constexpr Match& operator=(const AnyOf<V, K>& a) noexcept { setInV_(a.v.data(), K); return *this; }

    [[nodiscard]] constexpr State       state() const noexcept { return s_; }
    [[nodiscard]] constexpr std::size_t size() const noexcept { return n_; }
    [[nodiscard]] constexpr bool        overflow() const noexcept { return s_ == State::Overflow; }
    [[nodiscard]] constexpr bool        outOfRange() const noexcept { return s_ == State::OutOfRange; }
    [[nodiscard]] constexpr const V&    operator[](std::size_t i) const noexcept { return v_[i]; }
private:
    static constexpr V pad_(const char* p, std::size_t len) noexcept { V v{}; for (std::size_t i = 0; i < len && i < N; ++i) v[i] = p[i]; return v; }
    [[nodiscard]] constexpr bool terminal_() const noexcept { return s_ == State::Overflow || s_ == State::OutOfRange; }
    constexpr void setEq_(const V& v) noexcept {
        if (terminal_()) return;
        n_ = 0; s_ = State::Eq; v_[n_++] = v;
    }
    constexpr void setInV_(const V* p, std::size_t count) noexcept {
        if (terminal_()) return;
        n_ = 0;
        if (count > kMaxValues) { s_ = State::Overflow; return; }
        s_ = State::In;
        for (std::size_t i = 0; i < count; ++i) v_[n_++] = p[i];
    }
    constexpr void setInC_(const char* const* p, std::size_t count) noexcept {
        if (terminal_()) return;
        n_ = 0;
        if (count > kMaxValues) { s_ = State::Overflow; return; }
        s_ = State::In;
        for (std::size_t i = 0; i < count && s_ == State::In; ++i) {
            std::size_t len = 0; while (p[i][len] != '\0') ++len;
            if (len > N) { s_ = State::OutOfRange; return; }
            v_[n_++] = pad_(p[i], len);
        }
    }
    std::array<V, kMaxValues> v_{};
    uint8_t n_ = 0;
    State   s_ = State::Unset;
};

}  // namespace rune

namespace rune::codec {

/// Straight stores into a caller buffer.  Callers (generated code, FilterBuilder) pass terms in ascending
/// tag order; In values are sorted (FilterKey order) and de-duplicated in place.  No allocation.
class FilterWriter {
public:
    constexpr explicit FilterWriter(std::span<uint8_t> out) noexcept : out_(out), pos_(kFilterHeaderBytes) {}

    template <class T>
    constexpr void term(uint16_t tag, PrimType type, uint8_t width, const rune::Match<T>& m) noexcept {
        using S = typename rune::Match<T>::State;
        if (err_) return;
        S st = m.state();
        if (st == S::Unset) return;
        const uint8_t idx = terms_;
        if (!widthMatches_<T>(type, width)) { fail_({FilterErrorKind::TypeMismatch, idx, tag}); return; }
        if (terms_ == kMaxFilterTerms) { fail_({FilterErrorKind::TooManyTerms, 0xFF, tag}); return; }
        if (terms_ > 0 && tag <= lastTag_) { fail_({FilterErrorKind::NotCanonical, idx, tag}); return; }
        FilterOp op = FilterOp::Eq;
        switch (st) {
            case S::Overflow:   fail_({FilterErrorKind::TooManyValues, idx, tag}); return;
            case S::OutOfRange: fail_({FilterErrorKind::ValueOutOfRange, idx, tag}); return;
            case S::Eq:         op = FilterOp::Eq; break;
            case S::In:         op = FilterOp::In; break;
            case S::Unset:      return;
        }
        if (m.size() == 0) { fail_({FilterErrorKind::ValueCount, idx, tag}); return; }
        if (pos_ + kFilterTermHeaderBytes + m.size() * width > out_.size()) { fail_({FilterErrorKind::BufferTooSmall, idx, tag}); return; }
        out_[pos_++] = static_cast<uint8_t>(tag); out_[pos_++] = static_cast<uint8_t>(tag >> 8);
        out_[pos_++] = static_cast<uint8_t>(type);
        out_[pos_++] = static_cast<uint8_t>(op);
        const std::size_t countAt = pos_++;
        const std::size_t first = pos_;
        uint8_t count = 0;
        for (std::size_t i = 0; i < m.size(); ++i) {
            storeValue_(m[i], width);
            // insertion into the sorted run [first, pos_ - width): de-duplicate.
            if (!insertSorted_(first, count, width)) pos_ -= width; else ++count;
        }
        out_[countAt] = count;
        lastTag_ = tag;
        ++terms_;
    }

    [[nodiscard]] constexpr std::expected<std::size_t, FilterError> finish() noexcept {
        if (err_) return std::unexpected(*err_);
        if (terms_ == 0) return std::size_t{0};
        out_[0] = kFilterFormatVer;
        out_[1] = terms_;
        return pos_;
    }

private:
    // The caller's width must describe T exactly: no truncation of integers, no read past a char array.
    template <class T> static constexpr bool widthMatches_(PrimType type, uint8_t width) noexcept {
        if (width == 0 || filterValueWidth(type, width) != width) return false;
        if constexpr (rune::IsCharArray<T>::value) return type == PrimType::FixedStr && width <= std::tuple_size_v<T>;
        else return type != PrimType::FixedStr && width == sizeof(T);
    }
    template <class T> constexpr void storeValue_(const T& v, uint8_t width) noexcept {
        if constexpr (rune::IsCharArray<T>::value) {
            for (std::size_t i = 0; i < width; ++i) out_[pos_ + i] = static_cast<uint8_t>(v[i]);
        } else {
            using UT = std::make_unsigned_t<std::conditional_t<std::same_as<T, bool>, uint8_t, T>>;
            const auto u = static_cast<uint64_t>(static_cast<UT>(v));
            for (std::size_t i = 0; i < width; ++i) out_[pos_ + i] = static_cast<uint8_t>(u >> (8 * i));
        }
        pos_ += width;
    }
    // The value just stored at [pos_-width, pos_) moves into sorted position among `count` earlier ones;
    // false when it equals one of them (the caller drops it).
    constexpr bool insertSorted_(std::size_t first, uint8_t count, uint8_t width) noexcept {
        std::size_t at = pos_ - width;
        const FilterKey k = loadFilterKeyFrom_(at, width);
        std::size_t i = count;
        while (i > 0) {
            const FilterKey prev = loadFilterKeyFrom_(first + (i - 1) * width, width);
            if (prev == k) return false;
            if (prev < k) break;
            --i;
        }
        for (std::size_t j = count; j > i; --j)
            for (std::size_t b = 0; b < width; ++b) out_[first + j * width + b] = out_[first + (j - 1) * width + b];
        storeKey_(first + i * width, k, width);
        return true;
    }
    constexpr FilterKey loadFilterKeyFrom_(std::size_t at, uint8_t width) const noexcept { return loadFilterKey(out_.data() + at, width); }
    constexpr void storeKey_(std::size_t at, FilterKey k, uint8_t width) noexcept {
        for (std::size_t b = 0; b < width; ++b)
            out_[at + b] = static_cast<uint8_t>(b < 8 ? (k.lo >> (8 * b)) : (k.hi >> (8 * (b - 8))));
    }
    constexpr void fail_(FilterError e) noexcept { if (!err_) err_ = e; }

    std::span<uint8_t> out_;
    std::size_t pos_ = 0;
    uint8_t terms_ = 0;
    uint16_t lastTag_ = 0;
    std::optional<FilterError> err_;
};

}  // namespace rune::codec
