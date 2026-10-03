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

} // namespace rune::node
