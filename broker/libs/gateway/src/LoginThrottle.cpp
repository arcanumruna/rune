#include <rune/gateway/LoginThrottle.h>

#include <rune/crypto/Kdf.h>

#include <algorithm>
#include <cstring>
#include <random>
#include <span>
#include <string>

namespace rune::gateway {

std::string hostOfSourceIdentity(std::string_view id)
{
    if (id.empty() || id.rfind("aeron:", 0) == 0) return {};
    if (const auto colon = id.rfind(':'); colon != std::string_view::npos) id = id.substr(0, colon);
    if (id.size() > 2 && id.front() == '[' && id.back() == ']') id = id.substr(1, id.size() - 2);
    return std::string(id);
}


namespace {

// SipHash-2-4 (Aumasson & Bernstein), streamed over the key's pieces so no buffer bounds the name or host length.
class SipHash {
public:
    SipHash(const uint8_t* key16) noexcept {
        uint64_t k0 = 0, k1 = 0;
        std::memcpy(&k0, key16, 8);
        std::memcpy(&k1, key16 + 8, 8);
        v0_ = k0 ^ 0x736f6d6570736575ull; v1_ = k1 ^ 0x646f72616e646f6dull;
        v2_ = k0 ^ 0x6c7967656e657261ull; v3_ = k1 ^ 0x7465646279746573ull;
    }
    void update(std::string_view s) noexcept {
        for (const char c : s) {
            tail_ |= static_cast<uint64_t>(static_cast<uint8_t>(c)) << (8 * (len_ & 7));
            if ((++len_ & 7) == 0) { block_(tail_); tail_ = 0; }
        }
    }
    [[nodiscard]] uint64_t final() noexcept {
        block_(tail_ | (static_cast<uint64_t>(len_ & 0xFF) << 56));
        v2_ ^= 0xFF;
        for (int i = 0; i < 4; ++i) round_();
        return v0_ ^ v1_ ^ v2_ ^ v3_;
    }

private:
    static uint64_t rotl(uint64_t x, int b) noexcept { return (x << b) | (x >> (64 - b)); }
    void round_() noexcept {
        v0_ += v1_; v1_ = rotl(v1_, 13); v1_ ^= v0_; v0_ = rotl(v0_, 32);
        v2_ += v3_; v3_ = rotl(v3_, 16); v3_ ^= v2_;
        v0_ += v3_; v3_ = rotl(v3_, 21); v3_ ^= v0_;
        v2_ += v1_; v1_ = rotl(v1_, 17); v1_ ^= v2_; v2_ = rotl(v2_, 32);
    }
    void block_(uint64_t m) noexcept { v3_ ^= m; round_(); round_(); v0_ ^= m; }
    uint64_t v0_, v1_, v2_, v3_, tail_ = 0;
    std::size_t len_ = 0;
};

LoginThrottle::HashKey randomHashKey() {
    LoginThrottle::HashKey k{};
    if (!rune::crypto::randomBytes(std::span<uint8_t>(k.data(), k.size()))) {
        std::random_device rd;   // no OpenSSL randomness: the OS source directly (still unpredictable)
        for (std::size_t i = 0; i < k.size(); i += 4) {
            const uint32_t r = rd();
            std::memcpy(k.data() + i, &r, 4);
        }
    }
    return k;
}

}  // namespace

LoginThrottle::LoginThrottle() : LoginThrottle(randomHashKey()) {}

void LoginThrottle::clear() noexcept {
    for (std::vector<Entry>* t : {&pairs_, &hosts_, &users_, &known_}) std::fill(t->begin(), t->end(), Entry{});
}

LoginThrottle::Key LoginThrottle::hash_(char tag, std::string_view a, std::string_view b) const noexcept {
    // tag | a | NUL | b: the tag separates the tables, the NUL the two parts ("ab"+"c" never equals "a"+"bc").
    const std::string_view t(&tag, 1), nul("\0", 1);
    SipHash x(key_.data()), y(key_.data() + 16);
    for (SipHash* h : {&x, &y}) { h->update(t); h->update(a); h->update(nul); h->update(b); }
    return {x.final(), y.final()};
}

LoginThrottle::Key LoginThrottle::pairKey(std::string_view user, std::string_view host) const noexcept { return hash_('P', user, host); }
LoginThrottle::Key LoginThrottle::hostKey(std::string_view host) const noexcept { return hash_('H', {}, host); }
LoginThrottle::Key LoginThrottle::userKey(std::string_view user) const noexcept { return hash_('U', user, {}); }

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

void LoginThrottle::fail_(Entry& e, uint32_t freeFailures, int64_t maxDelayNs, int64_t nowNs) noexcept {
    if (e.failures < 0xFFFF'FFF0u) ++e.failures;
    e.lastFailNs = nowNs;
    if (e.failures <= freeFailures) { e.nextAllowedNs = 0; return; }
    const uint32_t over = e.failures - freeFailures;                    // 1 = the first delayed failure
    const uint32_t shift = std::min<uint32_t>(over - 1, 20);
    e.nextAllowedNs = nowNs + std::min<int64_t>(kBaseDelayNs << shift, maxDelayNs);
}

LoginThrottle::Verdict LoginThrottle::check(std::string_view user, std::string_view host, int64_t nowNs) const noexcept {
    int64_t next = 0;
    if (const Entry* e = find_(pairs_, pairKey(user, host), nowNs)) next = std::max(next, e->nextAllowedNs);
    if (const Entry* e = find_(hosts_, hostKey(host), nowNs)) next = std::max(next, e->nextAllowedNs);
    // The user component (#131) is skipped for a pair the user has logged in from: a campaign against the user from
    // elsewhere never delays the user at home.  Its pair and host components still apply.
    if (find_(known_, pairKey(user, host), nowNs) == nullptr)
        if (const Entry* e = find_(users_, userKey(user), nowNs)) next = std::max(next, e->nextAllowedNs);
    if (nowNs >= next) return {};
    constexpr int64_t ms = 1'000'000;
    return {false, static_cast<uint32_t>((next - nowNs + ms - 1) / ms)};
}

void LoginThrottle::onFailure(std::string_view user, std::string_view host, int64_t nowNs) noexcept {
    fail_(claim_(pairs_, pairKey(user, host), nowNs), kFreePairFailures, kMaxDelayNs, nowNs);
    fail_(claim_(hosts_, hostKey(host), nowNs), kFreeHostFailures, kMaxDelayNs, nowNs);
    fail_(claim_(users_, userKey(user), nowNs), kFreeUserFailures, kMaxUserDelayNs, nowNs);
}

void LoginThrottle::onSuccess(std::string_view user, std::string_view host, int64_t nowNs) noexcept {
    const Key k = pairKey(user, host);
    const std::size_t start = static_cast<std::size_t>(k.h1) & (kCapacity - 1);
    for (std::size_t i = 0; i < kProbe; ++i) {
        Entry& e = pairs_[(start + i) & (kCapacity - 1)];
        if (e.failures != 0 && e.h1 == k.h1 && e.h2 == k.h2) e = Entry{};
    }
    Entry& known = claim_(known_, k, nowNs);   // refreshed on every success: known for kForgetAfterNs after the last
    known.failures = 1;
    known.lastFailNs = nowNs;
}

} // namespace rune::gateway
