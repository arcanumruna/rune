// ClusterKeys.cpp — loadClusterKeys (authentication plan D32).
#include <rune/peerlink/ClusterKeys.h>

#include <cerrno>
#include <cstring>
#include <fstream>
#include <iterator>
#include <vector>

namespace rune::peerlink {

namespace {

std::expected<ClusterKey, std::string> loadOne(const std::string& path, const char* variable) {
    if (path.empty()) return std::unexpected(std::string{variable} + " is not set — a cluster key file is required");
    std::ifstream in(path, std::ios::binary);
    if (!in) {
        const int e = errno;
        return std::unexpected(std::string{variable} + "=" + path + ": cannot be read (" + std::strerror(e) + ")");
    }
    const std::vector<uint8_t> bytes((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
    if (in.bad()) return std::unexpected(std::string{variable} + "=" + path + ": read error");
    // deriveClusterKey trims trailing whitespace and refuses fewer than 32 bytes (D32): one rule, one place.
    auto k = rune::crypto::deriveClusterKey(bytes);
    if (!k)
        return std::unexpected(std::string{variable} + "=" + path +
                               ": the key file must hold at least 32 bytes (trailing whitespace is ignored)");
    return *k;
}

} // namespace

std::expected<ClusterKeys, std::string> loadClusterKeys(const std::string& current, const std::string& next) {
    auto cur = loadOne(current, "RUNE_CLUSTER_KEY_FILE");
    if (!cur) return std::unexpected(std::move(cur.error()));
    ClusterKeys out;
    out.current = *cur;
    if (!next.empty()) {
        auto nxt = loadOne(next, "RUNE_CLUSTER_KEY_FILE_NEXT");
        if (!nxt) return std::unexpected(std::move(nxt.error()));
        out.next = *nxt;
    }
    return out;
}

} // namespace rune::peerlink
