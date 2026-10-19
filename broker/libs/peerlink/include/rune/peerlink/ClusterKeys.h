#pragma once
// rune/peerlink/ClusterKeys.h — the cluster key files (authentication plan D32, spec §8).
//
// Loaded on the STARTUP thread (cold, file I/O); each thread that needs a key gets its own COPY (shared-nothing —
// nothing here is read across threads).  The derivation is the one in rune-crypto (deriveClusterKey), shared with
// the CLI's bootstrap MAC.
#include <rune/crypto/Kdf.h>

#include <cstdint>
#include <expected>
#include <optional>
#include <string>

namespace rune::peerlink {

using ClusterKey = rune::crypto::ClusterKeyMaterial;   // master[64] + keyId

struct ClusterKeys {
    ClusterKey                current;
    std::optional<ClusterKey> next;   // RUNE_CLUSTER_KEY_FILE_NEXT: the key a rolling restart rotates to
    /// current or next by key id; nullptr when this broker holds neither (D35).
    [[nodiscard]] const ClusterKey* byId(uint64_t id) const noexcept {
        if (current.keyId == id) return &current;
        if (next && next->keyId == id) return &*next;
        return nullptr;
    }
};

/// Reads the file(s).  A refusal is a message naming the variable and the path: the file is missing, unreadable, or
/// holds fewer than 32 bytes after trailing whitespace is trimmed.  `next` empty = no rotation key.
[[nodiscard]] std::expected<ClusterKeys, std::string> loadClusterKeys(const std::string& current,
                                                                      const std::string& next);

} // namespace rune::peerlink
