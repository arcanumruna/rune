#pragma once
// Aeron channel URI helpers shared by the gateway and the shard workers.
#include <algorithm>
#include <cstdint>
#include <string>
#include <string_view>

namespace rune::node {

/// `channel` with a per-publication `term-length` (bytes) appended, unless the channel already sets
/// one (an explicit per-channel value wins) or termBytes <= 0 (driver default).  Low-volume control
/// publications (acks, egress) use a small term: rotating a large term cleans it inside the caller's
/// duty cycle (findings §4.2: 100+ ms stalls at 16 MiB).  Cold path (publication setup).
[[nodiscard]] inline std::string withTermLength(std::string_view channel, int32_t termBytes) {
    std::string ch(channel);
    if (termBytes <= 0 || ch.find("term-length=") != std::string::npos) return ch;
    ch += (ch.find('?') == std::string::npos) ? '?' : '|';
    ch += "term-length=";
    ch += std::to_string(termBytes);
    return ch;
}

/// Smallest term of an ack publication: a 4096-verdict ack (32,824 B) is offered, and Aeron caps one message at term / 8.
inline constexpr int32_t kMinAckTermBytes = 512 * 1024;

/// withTermLength for an ack channel: a `term-length` below kMinAckTermBytes (the channel's own or termBytes) is raised to it.
/// termBytes <= 0 with no explicit value gets the floor.  Cold path (publication setup).
[[nodiscard]] inline std::string withAckTermLength(std::string_view channel, int32_t termBytes, int32_t floor = kMinAckTermBytes) {
    constexpr std::string_view key = "term-length=";
    std::string ch(channel);
    const std::size_t at = ch.find(key);
    if (at == std::string::npos) return withTermLength(ch, std::max(termBytes, floor));
    const std::size_t from = at + key.size();
    std::size_t end = from;
    while (end < ch.size() && ch[end] != '|' && ch[end] != '&') ++end;
    int64_t v = 0;
    std::size_t i = from;
    for (; i < end && ch[i] >= '0' && ch[i] <= '9'; ++i) v = v * 10 + (ch[i] - '0');
    if (i < end) v *= (ch[i] == 'k' || ch[i] == 'K') ? 1024 : (ch[i] == 'm' || ch[i] == 'M') ? 1024 * 1024 : (ch[i] == 'g' || ch[i] == 'G') ? 1024LL * 1024 * 1024 : 1;
    if (v < floor) ch.replace(from, end - from, std::to_string(floor));
    return ch;
}

/// A10: `channel` with `sparse=true` appended unless the channel already sets `sparse` (an explicit value
/// wins).  For the small per-session control publications (acks, egress) created at runtime on a worker
/// thread: a non-sparse term file is fallocate()d on tmpfs, which fails with EINTR whenever the thread has
/// pending notification work (io_uring completions) — Aeron does not retry it and the publication add
/// errors.  A sparse term is ftruncate()d (no EINTR) and needs no pre-faulting at control-stream rates.
/// The data plane keeps the driver's pre-faulted (non-sparse) default.  Cold path (publication setup).
[[nodiscard]] inline std::string withSparseTerms(std::string_view channel) {
    std::string ch(channel);
    if (ch.find("sparse=") != std::string::npos) return ch;
    ch += (ch.find('?') == std::string::npos) ? '?' : '|';
    ch += "sparse=true";
    return ch;
}

} // namespace rune::node
