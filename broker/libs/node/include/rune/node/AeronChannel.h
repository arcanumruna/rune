#pragma once
// Aeron channel URI helpers shared by the gateway and the shard workers.
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
