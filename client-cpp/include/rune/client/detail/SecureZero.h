#pragma once
// Zeroing the optimiser may not elide (password / key material is wiped after use).
#include <cstddef>

#include <string>
#include <string_view>
#include <utility>

namespace rune::client::detail {
inline void secureZero(void* p, std::size_t n) noexcept {
    volatile unsigned char* v = static_cast<volatile unsigned char*>(p);
    while (n-- != 0) *v++ = 0;
}

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
