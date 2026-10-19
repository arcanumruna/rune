#pragma once
// BootstrapProof.h — the cluster-key proof of SecurityBootstrap (authentication plan D4).
//
// A fresh cluster can be bootstrapped exactly once, by whoever holds the cluster key: SecurityBootstrap carries an
// HMAC-SHA-512 under the cluster key (the CURRENT or the NEXT key, so a rotation window works) over
// pc::bootstrapMacInput - ONE definition shared with the CLI.  The gateway verifies it with ITS OWN copy of the
// ClusterKeys before the request is forwarded; both keys are always tried and compared in constant time.
// Control thread; cold (one bootstrap per cluster).

#include <rune/crypto/Kdf.h>
#include <rune/peerlink/ClusterKeys.h>
#include <rune/protocol/client/SecurityMessages.h>

#include <cstdint>
#include <vector>

namespace rune::gateway {

enum class BootstrapProof : uint8_t {
    Ok,         ///< the MAC verifies under the current or the next cluster key
    NoKey,      ///< this broker holds no cluster key (authentication is not enabled)
    Mismatch    ///< no key of this broker verifies it
};

/// `scratch` is the caller's reusable MAC-input buffer (capacity persists).
[[nodiscard]] inline BootstrapProof verifyBootstrapProof(const rune::protocol::client::SecurityBootstrap& b,
                                                        const rune::peerlink::ClusterKeys* keys,
                                                        std::vector<uint8_t>& scratch) {
    if (keys == nullptr) return BootstrapProof::NoKey;
    rune::protocol::client::bootstrapMacInput(b, scratch);
    if (scratch.empty()) return BootstrapProof::Mismatch;   // not encodable
    bool ok = false;
    // Both keys, every time, constant-time compare: the answer's timing never says which key (or none) matched.
    const rune::peerlink::ClusterKey* candidates[2] = {&keys->current, keys->next ? &*keys->next : nullptr};
    for (const rune::peerlink::ClusterKey* k : candidates) {
        if (k == nullptr) continue;
        rune::crypto::DigestBytes mac;
        if (!rune::crypto::hmac(rune::crypto::Digest::Sha512, k->master, scratch, mac) || mac.size != b.mac.size()) continue;
        ok = rune::crypto::constantTimeEqual(mac.view(), std::span<const uint8_t>(b.mac.data(), b.mac.size())) || ok;
    }
    return ok ? BootstrapProof::Ok : BootstrapProof::Mismatch;
}

}  // namespace rune::gateway
