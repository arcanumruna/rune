#pragma once
// Zeroing the optimiser may not elide (password / key material is wiped after use).
#include <cstddef>
#include <cstring>

#include <string>
#include <string_view>
#include <utility>

namespace rune::client::detail {
/// The longest password a Client logs in with.
inline constexpr std::size_t kMaxPasswordBytes = 1024;

inline void secureZero(void* p, std::size_t n) noexcept {
    volatile unsigned char* v = static_cast<volatile unsigned char*>(p);
    while (n-- != 0) *v++ = 0;
}

/// Wipes the whole allocation of `s` (capacity, not only size: a shrunk string keeps old bytes), then clears it.
inline void wipeString(std::string& s) noexcept {
    if (s.capacity() != 0) secureZero(s.data(), s.capacity());
    s.clear();
}

/// A password in a fixed buffer: never allocates, never throws, wiped on every change and on destruction.
/// Not copyable or movable: holders point at one owner (the ClientAgent's) by pointer.
class FixedSecret {
public:
    static constexpr std::size_t kCapacity = kMaxPasswordBytes;
    FixedSecret() noexcept = default;
    FixedSecret(const FixedSecret&) = delete;
    FixedSecret& operator=(const FixedSecret&) = delete;
    ~FixedSecret() { wipe(); }
    /// false (contents unchanged) when `v` is longer than kCapacity: never truncated.  `v` may alias the buffer.
    [[nodiscard]] bool assign(std::string_view v) noexcept {
        if (v.size() > kCapacity) return false;
        if (!v.empty()) std::memmove(buf_, v.data(), v.size());
        secureZero(buf_ + v.size(), kCapacity - v.size());
        len_ = v.size();
        return true;
    }
    void wipe() noexcept { secureZero(buf_, kCapacity); len_ = 0; }
    [[nodiscard]] std::string_view view() const noexcept { return {buf_, len_}; }
    [[nodiscard]] bool empty() const noexcept { return len_ == 0; }
    /// Test seam: the whole buffer (to check that old bytes are gone).
    [[nodiscard]] std::string_view rawForTest() const noexcept { return {buf_, kCapacity}; }
private:
    char        buf_[kCapacity]{};
    std::size_t len_ = 0;
};

/// A password kept only as long as it is needed: movable, never copyable, zeroed on destruction and on being moved
/// from (a moved-from std::string may keep short contents in place).
class SecretString {
public:
    SecretString() = default;
    SecretString(const SecretString&) = delete;
    SecretString& operator=(const SecretString&) = delete;
    SecretString(SecretString&& o) noexcept : s_(std::move(o.s_)) { o.wipe(); }
    SecretString& operator=(SecretString&& o) noexcept {
        if (this != &o) { wipe(); s_ = std::move(o.s_); o.wipe(); }
        return *this;
    }
    ~SecretString() { wipe(); }
    void assign(std::string_view v) { wipe(); s_.assign(v); }
    [[nodiscard]] bool empty() const noexcept { return s_.empty(); }
    [[nodiscard]] std::string_view view() const noexcept { return s_; }
    void wipe() noexcept {
        if (!s_.empty()) secureZero(s_.data(), s_.size());
        s_.clear();
    }
private:
    std::string s_;
};
} // namespace rune::client::detail
