#pragma once
// Internal little-endian writer/reader for rune_catalog encodings.  The reader
// never reads past its span: every get returns false on truncation.
#include <cstdint>
#include <cstring>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace rune::catalog::wire {

class Writer {
public:
    explicit Writer(std::vector<uint8_t>& out) noexcept : out_(out) {}
    void u8(uint8_t v)   { out_.push_back(v); }
    void u16(uint16_t v) { raw(&v, 2); }
    void i16(int16_t v)  { raw(&v, 2); }
    void u32(uint32_t v) { raw(&v, 4); }
    void i32(int32_t v)  { raw(&v, 4); }
    void u64(uint64_t v) { raw(&v, 8); }
    void i64(int64_t v)  { raw(&v, 8); }
    void str16(std::string_view s) { u16(static_cast<uint16_t>(s.size())); raw(s.data(), s.size()); }
    void str32(std::string_view s) { u32(static_cast<uint32_t>(s.size())); raw(s.data(), s.size()); }
    void bytes32(std::span<const uint8_t> b) { u32(static_cast<uint32_t>(b.size())); raw(b.data(), b.size()); }
    [[nodiscard]] std::size_t size() const noexcept { return out_.size(); }
    /// Patch a u32 written earlier (length back-fill).
    void patchU32(std::size_t at, uint32_t v) noexcept { std::memcpy(out_.data() + at, &v, 4); }
private:
    void raw(const void* p, std::size_t n) {
        const auto* b = static_cast<const uint8_t*>(p);
        out_.insert(out_.end(), b, b + n);
    }
    std::vector<uint8_t>& out_;
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
