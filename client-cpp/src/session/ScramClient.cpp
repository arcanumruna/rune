// src/session/ScramClient.cpp — see rune/client/session/ScramClient.h.
#include <rune/client/session/ScramClient.h>

#include <algorithm>
#include <cstring>

namespace rune::client::session {

namespace {
/// Zeroing that the optimiser may not elide.
void secureZero(void* p, std::size_t n) noexcept {
    volatile unsigned char* v = static_cast<volatile unsigned char*>(p);
    while (n-- != 0) *v++ = 0;
}
} // namespace

void ScramCache::wipe_(Entry& e) noexcept {
    secureZero(e.sp.bytes.bytes.data(), e.sp.bytes.bytes.size());
    secureZero(e.salt.data(), e.salt.size());
    secureZero(e.pwTag.data(), e.pwTag.size());
    e.sp.bytes.size = 0;
    e.saltLen       = 0;
    e.iterations    = 0;
}

bool ScramCache::tagOf_(std::string_view password, Tag& out) noexcept {
    static constexpr std::string_view kMsg = "rune-scram-cache-tag";
    rune::crypto::DigestBytes d;
    const auto key = std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(password.data()), password.size());
    const auto msg = std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(kMsg.data()), kMsg.size());
    if (!rune::crypto::hmac(rune::crypto::Digest::Sha512, key, msg, d) || d.size != out.size()) return false;
    std::memcpy(out.data(), d.bytes.data(), out.size());
    secureZero(d.bytes.data(), d.bytes.size());
    return true;
}

const rune::crypto::scram::SaltedPassword* ScramCache::find_(const Tag& tag, std::span<const uint8_t> salt,
                                                             uint32_t iterations) const noexcept {
    if (salt.size() > kMaxSalt) return nullptr;
    for (const Entry& e : entries_) {
        if (e.iterations == iterations && e.saltLen == salt.size() &&
            std::memcmp(e.salt.data(), salt.data(), salt.size()) == 0 &&
            std::memcmp(e.pwTag.data(), tag.data(), tag.size()) == 0) {
            return &e.sp;
        }
    }
    return nullptr;
}

void ScramCache::put_(const Tag& tag, std::span<const uint8_t> salt, uint32_t iterations,
                      const rune::crypto::scram::SaltedPassword& sp) noexcept {
    if (salt.size() > kMaxSalt || iterations == 0) return;
    if (find_(tag, salt, iterations) != nullptr) return;
    Entry* victim = nullptr;
    for (Entry& e : entries_) {
        if (e.iterations == 0) { victim = &e; break; }
    }
    if (victim == nullptr) {
        victim = &entries_[next_];
        next_  = (next_ + 1) % kCapacity;
        wipe_(*victim);
    }
    std::memcpy(victim->salt.data(), salt.data(), salt.size());
    victim->saltLen    = static_cast<uint8_t>(salt.size());
    victim->iterations = iterations;
    victim->pwTag      = tag;
    victim->sp         = sp;
}

const rune::crypto::scram::SaltedPassword* ScramCache::find(std::string_view password, std::span<const uint8_t> salt,
                                                            uint32_t iterations) const noexcept {
    Tag tag;
    if (!tagOf_(password, tag)) return nullptr;
    const auto* r = find_(tag, salt, iterations);
    secureZero(tag.data(), tag.size());
    return r;
}

void ScramCache::put(std::string_view password, std::span<const uint8_t> salt, uint32_t iterations,
                     const rune::crypto::scram::SaltedPassword& sp) noexcept {
    Tag tag;
    if (!tagOf_(password, tag)) return;
    put_(tag, salt, iterations, sp);
    secureZero(tag.data(), tag.size());
}

const rune::crypto::scram::SaltedPassword* ScramCache::saltedPassword(std::string_view password,
                                                                      std::span<const uint8_t> salt,
                                                                      uint32_t iterations) noexcept {
    Tag tag;
    const bool tagged = tagOf_(password, tag);
    if (tagged) {
        if (const auto* hit = find_(tag, salt, iterations)) {
            ++hits_;
            secureZero(tag.data(), tag.size());
            return hit;
        }
    }
    auto sp = rune::crypto::scram::saltPassword(rune::crypto::Digest::Sha512, password, salt, iterations);
    if (!sp) { secureZero(tag.data(), tag.size()); return nullptr; }
    ++computations_;
    if (salt.size() > kMaxSalt || !tagged) {   // uncached: held for the caller only
        wipe_(overflow_);
        overflow_.sp = *sp;
        secureZero(sp->bytes.bytes.data(), sp->bytes.bytes.size());   // review B M4: no stray copy
        secureZero(tag.data(), tag.size());
        return &overflow_.sp;
    }
    put_(tag, salt, iterations, *sp);
    secureZero(sp->bytes.bytes.data(), sp->bytes.bytes.size());
    const auto* r = find_(tag, salt, iterations);
    secureZero(tag.data(), tag.size());
    return r;
}

void ScramCache::clear() noexcept {
    for (Entry& e : entries_) wipe_(e);
    wipe_(overflow_);
    next_ = 0;
}

std::size_t ScramCache::size() const noexcept {
    return static_cast<std::size_t>(std::count_if(entries_.begin(), entries_.end(),
                                                  [](const Entry& e) { return e.iterations != 0; }));
}

} // namespace rune::client::session
