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
    e.sp.bytes.size = 0;
    e.saltLen       = 0;
    e.iterations    = 0;
}

const rune::crypto::scram::SaltedPassword* ScramCache::find(std::span<const uint8_t> salt,
                                                            uint32_t iterations) const noexcept {
    if (salt.size() > kMaxSalt) return nullptr;
    for (const Entry& e : entries_) {
        if (e.iterations == iterations && e.saltLen == salt.size() &&
            std::memcmp(e.salt.data(), salt.data(), salt.size()) == 0) {
            return &e.sp;
        }
    }
    return nullptr;
}

void ScramCache::put(std::span<const uint8_t> salt, uint32_t iterations,
                     const rune::crypto::scram::SaltedPassword& sp) noexcept {
    if (salt.size() > kMaxSalt || iterations == 0) return;
    if (find(salt, iterations) != nullptr) return;
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
    victim->sp         = sp;
}

const rune::crypto::scram::SaltedPassword* ScramCache::saltedPassword(std::string_view password,
                                                                      std::span<const uint8_t> salt,
                                                                      uint32_t iterations) noexcept {
    if (const auto* hit = find(salt, iterations)) {
        ++hits_;
        return hit;
    }
    auto sp = rune::crypto::scram::saltPassword(rune::crypto::Digest::Sha512, password, salt, iterations);
    if (!sp) return nullptr;
    ++computations_;
    if (salt.size() > kMaxSalt) {
        wipe_(overflow_);
        overflow_.sp = *sp;
        return &overflow_.sp;
    }
    put(salt, iterations, *sp);
    secureZero(sp->bytes.bytes.data(), sp->bytes.bytes.size());
    return find(salt, iterations);
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
