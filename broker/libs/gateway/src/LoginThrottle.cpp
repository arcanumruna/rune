#include <rune/gateway/LoginThrottle.h>

#include <algorithm>

namespace rune::gateway {

namespace {

constexpr uint64_t kPrime = 0x100000001b3ull;
constexpr uint64_t kBasis1 = 0xcbf29ce484222325ull, kBasis2 = 0x84222325cbf29ce4ull;

void mix(uint64_t& h1, uint64_t& h2, std::string_view s) noexcept {
    for (const char c : s) {
        h1 = (h1 ^ static_cast<uint8_t>(c)) * kPrime;
        h2 = (h2 + static_cast<uint8_t>(c) + 0x9e37u) * 0xff51afd7ed558ccdull;
        h2 ^= h2 >> 29;
    }
}

}  // namespace

LoginThrottle::Key LoginThrottle::pairKey(std::string_view user, std::string_view host) noexcept {
    uint64_t h1 = kBasis1, h2 = kBasis2;
    mix(h1, h2, "P");
    mix(h1, h2, user);
    mix(h1, h2, std::string_view("\0", 1));
    mix(h1, h2, host);
    return {h1, h2};
}

LoginThrottle::Key LoginThrottle::hostKey(std::string_view host) noexcept {
    uint64_t h1 = kBasis1, h2 = kBasis2;
    mix(h1, h2, "H");
    mix(h1, h2, host);
    return {h1, h2};
}

const LoginThrottle::Entry* LoginThrottle::find_(const std::vector<Entry>& t, Key k, int64_t nowNs) noexcept {
    const std::size_t start = static_cast<std::size_t>(k.h1) & (kCapacity - 1);
    for (std::size_t i = 0; i < kProbe; ++i) {
        const Entry& e = t[(start + i) & (kCapacity - 1)];
        if (e.failures != 0 && e.h1 == k.h1 && e.h2 == k.h2)
            return nowNs - e.lastFailNs > kForgetAfterNs ? nullptr : &e;   // idle: forgotten
    }
    return nullptr;
}

LoginThrottle::Entry& LoginThrottle::claim_(std::vector<Entry>& t, Key k, int64_t nowNs) noexcept {
    const std::size_t start = static_cast<std::size_t>(k.h1) & (kCapacity - 1);
    Entry* victim = nullptr;
    bool victimReusable = false;   // an empty / idle slot beats any live one
    for (std::size_t i = 0; i < kProbe; ++i) {
        Entry& e = t[(start + i) & (kCapacity - 1)];
        if (e.failures != 0 && e.h1 == k.h1 && e.h2 == k.h2) {
            if (nowNs - e.lastFailNs > kForgetAfterNs) e = Entry{k.h1, k.h2, 0, 0, 0};   // forgotten: start again
            return e;
        }
        const bool reusable = e.failures == 0 || nowNs - e.lastFailNs > kForgetAfterNs;
        if (reusable) {
            if (!victimReusable) { victim = &e; victimReusable = true; }
        } else if (!victimReusable && (victim == nullptr || e.lastFailNs < victim->lastFailNs)) {
            victim = &e;
        }
    }
    *victim = Entry{k.h1, k.h2, 0, 0, 0};   // a free / idle slot, else the OLDEST of the full window
    return *victim;
}

void LoginThrottle::fail_(Entry& e, uint32_t freeFailures, int64_t nowNs) noexcept {
    if (e.failures < 0xFFFF'FFF0u) ++e.failures;
    e.lastFailNs = nowNs;
    if (e.failures <= freeFailures) { e.nextAllowedNs = 0; return; }
    const uint32_t over = e.failures - freeFailures;                    // 1 = the first delayed failure
    const uint32_t shift = std::min<uint32_t>(over - 1, 20);
    e.nextAllowedNs = nowNs + std::min<int64_t>(kBaseDelayNs << shift, kMaxDelayNs);
}

LoginThrottle::Verdict LoginThrottle::check(std::string_view user, std::string_view host, int64_t nowNs) const noexcept {
    int64_t next = 0;
    if (const Entry* e = find_(pairs_, pairKey(user, host), nowNs)) next = std::max(next, e->nextAllowedNs);
    if (const Entry* e = find_(hosts_, hostKey(host), nowNs)) next = std::max(next, e->nextAllowedNs);
    if (nowNs >= next) return {};
    constexpr int64_t ms = 1'000'000;
    return {false, static_cast<uint32_t>((next - nowNs + ms - 1) / ms)};
}

void LoginThrottle::onFailure(std::string_view user, std::string_view host, int64_t nowNs) noexcept {
    fail_(claim_(pairs_, pairKey(user, host), nowNs), kFreePairFailures, nowNs);
    fail_(claim_(hosts_, hostKey(host), nowNs), kFreeHostFailures, nowNs);
}

void LoginThrottle::onSuccess(std::string_view user, std::string_view host) noexcept {
    const Key k = pairKey(user, host);
    const std::size_t start = static_cast<std::size_t>(k.h1) & (kCapacity - 1);
    for (std::size_t i = 0; i < kProbe; ++i) {
        Entry& e = pairs_[(start + i) & (kCapacity - 1)];
        if (e.failures != 0 && e.h1 == k.h1 && e.h2 == k.h2) e = Entry{};
    }
}

} // namespace rune::gateway
