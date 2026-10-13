#pragma once
// Internal little-endian writer/reader for rune_catalog encodings.  Every multi-byte
// integer is stored and loaded BYTE-WISE (shift + mask), so the bytes are little-endian
// on every host, whatever std::endian::native is.  The writer never truncates a length
// or count (it records an error instead); the reader never reads past its span: every
// get returns false on truncation.
#include <rune/catalog/CatalogEvent.h>

#include <cstdint>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <type_traits>
#include <vector>

namespace rune::catalog::wire {

class Writer {
public:
    /// Every length prefix and count is checked against its wire width: an oversize field
    /// records the first error (and stops writing) instead of truncating the prefix.
    using Error = rune::catalog::EncodeError;

    explicit Writer(std::vector<uint8_t>& out) noexcept : out_(out) {}
    void u8(uint8_t v)   { le(v, 1); }
    void u16(uint16_t v) { le(v, 2); }
    void i16(int16_t v)  { le(static_cast<uint16_t>(v), 2); }
    void u32(uint32_t v) { le(v, 4); }
    void i32(int32_t v)  { le(static_cast<uint32_t>(v), 4); }
    void u64(uint64_t v) { le(v, 8); }
    void i64(int64_t v)  { le(static_cast<uint64_t>(v), 8); }
    /// A u8-backed enum, written as its underlying byte.
    template <class E> void e8(E v) {
        static_assert(std::is_enum_v<E> && std::is_same_v<std::underlying_type_t<E>, uint8_t>);
        u8(static_cast<uint8_t>(v));
    }
    void str16(std::string_view s) {
        if (!fits(s.size(), kMaxStr16Bytes, Error::FieldTooLong)) return;
        u16(static_cast<uint16_t>(s.size())); bytes(s.data(), s.size());
    }
    void str32(std::string_view s) {
        if (!fits(s.size(), kMaxStr32Bytes, Error::FieldTooLong)) return;
        u32(static_cast<uint32_t>(s.size())); bytes(s.data(), s.size());
    }
    void bytes32(std::span<const uint8_t> b) {
        if (!fits(b.size(), kMaxStr32Bytes, Error::FieldTooLong)) return;
        u32(static_cast<uint32_t>(b.size())); bytes(b.data(), b.size());
    }
    /// A list's u16 element count.
    void count16(std::size_t n) {
        if (!fits(n, kMaxListCount, Error::TooManyElements)) return;
        u16(static_cast<uint16_t>(n));
    }
    [[nodiscard]] std::size_t size() const noexcept { return out_.size(); }
    /// Back-fill a u32 length written earlier as a placeholder at `at`: the length of
    /// everything written after it.  Over 4 GiB is FieldTooLong.
    void patchLen32(std::size_t at) noexcept {
        if (error_) return;
        const std::size_t n = out_.size() - at - 4;
        if (!fits(n, kMaxStr32Bytes, Error::FieldTooLong)) return;
        for (std::size_t i = 0; i < 4; ++i) out_[at + i] = static_cast<uint8_t>(n >> (8 * i));
    }
    /// The first width violation, if any.  The written bytes are meaningless once set.
    [[nodiscard]] std::optional<Error> error() const noexcept { return error_; }
private:
    bool fits(std::size_t n, std::size_t max, Error e) noexcept {
        if (error_) return false;
        if (n <= max) return true;
        error_ = e;
        return false;
    }
    void le(uint64_t v, int n) {
        if (error_) return;
        for (int i = 0; i < n; ++i) out_.push_back(static_cast<uint8_t>(v >> (8 * i)));
    }
    void bytes(const void* p, std::size_t n) {
        if (error_) return;
        const auto* b = static_cast<const uint8_t*>(p);
        out_.insert(out_.end(), b, b + n);
    }
    std::vector<uint8_t>& out_;
    std::optional<Error>  error_;
};

class Reader {
public:
    explicit Reader(std::span<const uint8_t> in) noexcept : in_(in) {}
    bool u8(uint8_t& v) noexcept   { return get(v); }
    bool u16(uint16_t& v) noexcept { return get(v); }
    bool i16(int16_t& v) noexcept  { return get(v); }
    bool u32(uint32_t& v) noexcept { return get(v); }
    bool i32(int32_t& v) noexcept  { return get(v); }
    bool u64(uint64_t& v) noexcept { return get(v); }
    bool i64(int64_t& v) noexcept  { return get(v); }
    /// A u8-backed enum, loaded as its raw byte.  Well-defined for every byte (fixed
    /// underlying type); the RANGE is judged by the shared validation, never here.
    template <class E> bool e8(E& v) noexcept {
        static_assert(std::is_enum_v<E> && std::is_same_v<std::underlying_type_t<E>, uint8_t>);
        uint8_t b; if (!u8(b)) return false;
        v = static_cast<E>(b); return true;
    }
    bool str16(std::string& s) { uint16_t n; if (!u16(n) || rem() < n) return false; take(s, n); return true; }
    bool str32(std::string& s) { uint32_t n; if (!u32(n) || rem() < n) return false; take(s, n); return true; }
    bool bytes32(std::vector<uint8_t>& b) {
        uint32_t n; if (!u32(n) || rem() < n) return false;
        b.assign(in_.data() + pos_, in_.data() + pos_ + n); pos_ += n; return true;
    }
    /// A sub-reader over the next n bytes (advances past them).
    bool sub(uint32_t n, Reader& out) noexcept {
        if (rem() < n) return false;
        out = Reader(in_.subspan(pos_, n)); pos_ += n; return true;
    }
    [[nodiscard]] std::size_t rem() const noexcept { return in_.size() - pos_; }
    /// Can `n` list elements of at least `minEach` encoded bytes each still be present?
    /// Checked BEFORE sizing a container from an untrusted count.
    [[nodiscard]] bool canHold(std::size_t n, std::size_t minEach) const noexcept { return n <= rem() / minEach; }
private:
    template <class T> bool get(T& v) noexcept {
        static_assert(std::is_integral_v<T>);
        if (rem() < sizeof(T)) return false;
        std::make_unsigned_t<T> u = 0;
        for (std::size_t i = 0; i < sizeof(T); ++i)
            u = static_cast<std::make_unsigned_t<T>>(u | static_cast<std::make_unsigned_t<T>>(in_[pos_ + i]) << (8 * i));
        v = static_cast<T>(u);   // two's complement (C++20) for the signed widths
        pos_ += sizeof(T);
        return true;
    }
    void take(std::string& s, std::size_t n) {
        s.assign(reinterpret_cast<const char*>(in_.data() + pos_), n); pos_ += n;
    }
    std::span<const uint8_t> in_;
    std::size_t pos_ = 0;
};

/// THE formatVer-1 TopicConfig enum bounds — one definition, used by the event validation
/// (validateEvent, encode, decode) AND topicConfigError, so the two can never diverge.
inline constexpr uint8_t kMaxCleanupPolicy  = 2;   // 0 Delete, 1 Compact, 2 CompactDelete
inline constexpr uint8_t kMaxDurability     = 1;   // 0 Majority, 1 AllInSync
inline constexpr uint8_t kMaxDurabilityTier = 2;   // 0 Leader, 1 Majority, 2 Strict

/// THE formatVer-1 value validation (CatalogEvent.cpp) — the one rule set encode and decode
/// run, exported so apply() refuses an in-memory event exactly where encode would
/// (formatVer, kind, kind/ops agreement, every enum range).  nullopt = valid.
[[nodiscard]] std::optional<EncodeError> validateEvent(const CatalogEvent& e) noexcept;

} // namespace rune::catalog::wire
