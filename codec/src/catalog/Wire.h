#pragma once
// Internal little-endian writer/reader for rune_catalog encodings.  The writer
// never truncates a length or count (it records an error instead); the reader
// never reads past its span: every get returns false on truncation.
#include <rune/catalog/CatalogEvent.h>

#include <cstdint>
#include <cstring>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace rune::catalog::wire {

class Writer {
public:
    /// Every length prefix and count is checked against its wire width: an oversize field
    /// records the first error (and stops writing) instead of truncating the prefix.
    using Error = rune::catalog::EncodeError;

    explicit Writer(std::vector<uint8_t>& out) noexcept : out_(out) {}
    void u8(uint8_t v)   { raw(&v, 1); }
    void u16(uint16_t v) { raw(&v, 2); }
    void i16(int16_t v)  { raw(&v, 2); }
    void u32(uint32_t v) { raw(&v, 4); }
    void i32(int32_t v)  { raw(&v, 4); }
    void u64(uint64_t v) { raw(&v, 8); }
    void i64(int64_t v)  { raw(&v, 8); }
    void str16(std::string_view s) {
        if (!fits(s.size(), kMaxStr16Bytes, Error::FieldTooLong)) return;
        u16(static_cast<uint16_t>(s.size())); raw(s.data(), s.size());
    }
    void str32(std::string_view s) {
        if (!fits(s.size(), kMaxStr32Bytes, Error::FieldTooLong)) return;
        u32(static_cast<uint32_t>(s.size())); raw(s.data(), s.size());
    }
    void bytes32(std::span<const uint8_t> b) {
        if (!fits(b.size(), kMaxStr32Bytes, Error::FieldTooLong)) return;
        u32(static_cast<uint32_t>(b.size())); raw(b.data(), b.size());
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
        const std::size_t n = out_.size() - at - 4;
        if (!fits(n, kMaxStr32Bytes, Error::FieldTooLong)) return;
        const auto v = static_cast<uint32_t>(n);
        std::memcpy(out_.data() + at, &v, 4);
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
    void raw(const void* p, std::size_t n) {
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
    bool u8(uint8_t& v) noexcept   { return raw(&v, 1); }
    bool u16(uint16_t& v) noexcept { return raw(&v, 2); }
    bool i16(int16_t& v) noexcept  { return raw(&v, 2); }
    bool u32(uint32_t& v) noexcept { return raw(&v, 4); }
    bool i32(int32_t& v) noexcept  { return raw(&v, 4); }
    bool u64(uint64_t& v) noexcept { return raw(&v, 8); }
    bool i64(int64_t& v) noexcept  { return raw(&v, 8); }
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
    bool raw(void* p, std::size_t n) noexcept {
        if (rem() < n) return false;
        std::memcpy(p, in_.data() + pos_, n); pos_ += n; return true;
    }
    void take(std::string& s, std::size_t n) {
        s.assign(reinterpret_cast<const char*>(in_.data() + pos_), n); pos_ += n;
    }
    std::span<const uint8_t> in_;
    std::size_t pos_ = 0;
};

} // namespace rune::catalog::wire
