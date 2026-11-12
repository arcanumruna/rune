#pragma once
// rune::FixedString<N> — the generated C++ type of a schema `string(N)` field (#1010).
//
// Exactly N bytes, zero padded, trivially copyable, standard layout, alignof 1: the wire bytes of a string(N)
// field, so a generated record's layout, fingerprint and wire are unchanged from the old std::array<char, N>.
//   * A string literal is checked at COMPILE time: a literal that fits (<= N chars) converts implicitly
//     (`rec.name = "Ayla"`), a longer one is a compile error (deleted overload — it never falls back to the
//     std::string_view overload).
//   * A runtime string never truncates: an over-long std::string_view throws std::invalid_argument and leaves
//     the object unchanged; tryAssign() is the noexcept form that returns false instead.
#include <array>
#include <compare>
#include <cstddef>
#include <cstring>
#include <stdexcept>
#include <string>
#include <string_view>
#include <type_traits>

namespace rune {

template <std::size_t N>
class FixedString {
    static_assert(N >= 1, "FixedString: N must be at least 1");
public:
    static constexpr std::size_t kCapacity = N;

    constexpr FixedString() noexcept = default;

    /// Literal of at most N characters (an exactly-N literal drops its terminating NUL); copies up to the first
    /// NUL, zero pads the rest.  Deliberately non-explicit: `Character{.name = "Ayla"}`.
    template <std::size_t M> requires(M - 1 <= N)
    constexpr FixedString(const char (&lit)[M]) noexcept {
        for (std::size_t i = 0; i < M - 1 && lit[i] != '\0'; ++i) b_[i] = lit[i];
    }
    /// Literal longer than N characters: compile error (and it must not reach the string_view constructor).
    template <std::size_t M> requires(M - 1 > N)
    FixedString(const char (&)[M]) = delete;

    /// Runtime string: throws std::invalid_argument when longer than N (never truncates).
    explicit FixedString(std::string_view s) {
        if (!tryAssign(s)) throwTooLong_(s.size());
    }

    template <std::size_t M> requires(M - 1 <= N)
    constexpr FixedString& operator=(const char (&lit)[M]) noexcept { return *this = FixedString(lit); }
    template <std::size_t M> requires(M - 1 > N)
    FixedString& operator=(const char (&)[M]) = delete;

    /// Throws std::invalid_argument when s is longer than N; the object is left unchanged.  A shorter s
    /// zero-fills the remaining bytes.
    /// (A template, so `x = {}` stays the plain copy assignment instead of being ambiguous with string_view.)
    template <class S> requires(std::is_convertible_v<const S&, std::string_view> && !std::is_same_v<S, FixedString>)
    FixedString& operator=(const S& str) {
        const std::string_view s = str;
        if (!tryAssign(s)) throwTooLong_(s.size());
        return *this;
    }

    /// noexcept form of operator=(std::string_view): false (object unchanged) when s is longer than N.
    [[nodiscard]] bool tryAssign(std::string_view s) noexcept {
        if (s.size() > N) return false;
        if (!s.empty()) std::memcpy(b_, s.data(), s.size());
        if (s.size() < N) std::memset(b_ + s.size(), 0, N - s.size());
        return true;
    }

    /// The content up to the first NUL (at most N bytes).
    [[nodiscard]] constexpr std::string_view view() const noexcept {
        std::size_t n = 0;
        while (n < N && b_[n] != '\0') ++n;
        return {b_, n};
    }

    [[nodiscard]] constexpr char*       data() noexcept { return b_; }
    [[nodiscard]] constexpr const char* data() const noexcept { return b_; }
    /// The capacity N (like std::array::size), zero padding included.
    [[nodiscard]] static constexpr std::size_t size() noexcept { return N; }

    constexpr char&       operator[](std::size_t i) noexcept { return b_[i]; }
    constexpr const char& operator[](std::size_t i) const noexcept { return b_[i]; }
    constexpr char*       begin() noexcept { return b_; }
    constexpr char*       end() noexcept { return b_ + N; }
    constexpr const char* begin() const noexcept { return b_; }
    constexpr const char* end() const noexcept { return b_ + N; }

    friend constexpr bool operator==(const FixedString&, const FixedString&) = default;
    friend constexpr auto operator<=>(const FixedString&, const FixedString&) = default;
    template <std::size_t M>
    friend constexpr bool operator==(const FixedString& a, const char (&lit)[M]) noexcept {
        return a.view() == std::string_view(lit);
    }
    friend constexpr bool operator==(const FixedString& a, std::string_view s) noexcept { return a.view() == s; }

private:
    [[noreturn]] static void throwTooLong_(std::size_t n) {
        throw std::invalid_argument("FixedString<" + std::to_string(N) + ">: " + std::to_string(n) +
                                    " bytes do not fit (a string(N) field is never truncated)");
    }
    char b_[N]{};
};

static_assert(sizeof(FixedString<8>) == 8 && alignof(FixedString<8>) == 1);
static_assert(std::is_trivially_copyable_v<FixedString<8>> && std::is_standard_layout_v<FixedString<8>>);

template <class T> struct IsFixedString : std::false_type {};
template <std::size_t N> struct IsFixedString<FixedString<N>> : std::true_type {};

}  // namespace rune
