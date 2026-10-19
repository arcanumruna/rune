// include/rune/client/shard/ShardMap.h — pure helpers for client shards
// (T15 design §2; spec §4.1).  Aeron-free, header-only.
//
//   shardFor()               the deterministic partition -> shard map
//   channelWithPortOffset()  shard i's egress channel: the endpoint port + i
//   parseChannelEndpoint()   host + port of a channel's endpoint= param
#pragma once

#include <charconv>
#include <cstdint>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>

namespace rune::client::shard {

/// Deterministic partition -> shard map: (uint32(topicId) + uint32(partition)) % n,
/// and 0 when n <= 1.  Pure and fixed for the life of a group: independent of
/// partition count, metadata and broker worker count.  Consecutive residues mean
/// one topic with P partitions gives every shard < P at least one partition.
[[nodiscard]] constexpr uint32_t shardFor(int32_t topicId, int32_t partition,
                                          uint32_t shardCount) noexcept {
    return shardCount <= 1
               ? 0u
               : (static_cast<uint32_t>(topicId) + static_cast<uint32_t>(partition)) % shardCount;
}

/// host + port of an `endpoint=host:port` channel param.
struct ChannelEndpoint {
    std::string host;
    int32_t     port = 0;
};

namespace detail {

/// [begin, end) of the endpoint param's value, or npos when absent.
struct EndpointSpan {
    std::size_t begin = std::string_view::npos;
    std::size_t end   = std::string_view::npos;
};

[[nodiscard]] inline EndpointSpan findEndpoint(std::string_view channel) noexcept {
    // Params follow '?' and are separated by '|'; match the key exactly.
    std::size_t pos = channel.find('?');
    while (pos != std::string_view::npos) {
        const std::size_t keyStart = pos + 1;
        if (channel.substr(keyStart, 9) == "endpoint=") {
            const std::size_t vb = keyStart + 9;
            std::size_t       ve = channel.find('|', vb);
            if (ve == std::string_view::npos) ve = channel.size();
            return {vb, ve};
        }
        pos = channel.find('|', keyStart);
    }
    return {};
}

/// Splits "host:port" at the LAST ':' and parses the port strictly (digits only,
/// in [1, 65535]).  Throws std::invalid_argument naming the channel.
[[nodiscard]] inline ChannelEndpoint splitEndpoint(std::string_view value, std::string_view channel) {
    const std::size_t colon = value.rfind(':');
    if (colon == std::string_view::npos || colon == 0) {
        throw std::invalid_argument("shard: no endpoint port in channel " + std::string(channel));
    }
    const std::string_view portStr = value.substr(colon + 1);
    int32_t                port    = 0;
    const auto [ptr, ec] = std::from_chars(portStr.data(), portStr.data() + portStr.size(), port, 10);
    if (portStr.empty() || ec != std::errc{} || ptr != portStr.data() + portStr.size() || port <= 0 ||
        port > 65535) {
        throw std::invalid_argument("shard: bad endpoint port in channel " + std::string(channel));
    }
    return ChannelEndpoint{std::string(value.substr(0, colon)), port};
}

} // namespace detail

/// The channel's `endpoint=host:port`, or nullopt when it has none (aeron:ipc).
/// A present but malformed endpoint throws std::invalid_argument.
[[nodiscard]] inline std::optional<ChannelEndpoint> parseChannelEndpoint(std::string_view channel) {
    const detail::EndpointSpan sp = detail::findEndpoint(channel);
    if (sp.begin == std::string_view::npos) return std::nullopt;
    return detail::splitEndpoint(channel.substr(sp.begin, sp.end - sp.begin), channel);
}

/// "aeron:udp?endpoint=h:40400|x=y" + 2 -> "aeron:udp?endpoint=h:40402|x=y".  Only the
/// endpoint port changes.  A channel without an endpoint (aeron:ipc) is returned
/// unchanged.  A missing, non-numeric or overflowing port, or a negative offset,
/// throws std::invalid_argument.
[[nodiscard]] inline std::string channelWithPortOffset(const std::string& channel, int32_t offset) {
    if (offset < 0) {
        throw std::invalid_argument("channelWithPortOffset: negative offset " + std::to_string(offset));
    }
    const detail::EndpointSpan sp = detail::findEndpoint(channel);
    if (sp.begin == std::string_view::npos) return channel;
    const ChannelEndpoint ep =
        detail::splitEndpoint(std::string_view(channel).substr(sp.begin, sp.end - sp.begin), channel);
    const int64_t shifted = static_cast<int64_t>(ep.port) + offset;
    if (shifted > 65535) {
        throw std::invalid_argument("channelWithPortOffset: port " + std::to_string(ep.port) + " + " +
                                    std::to_string(offset) + " exceeds 65535 in " + channel);
    }
    if (offset == 0) return channel;
    return channel.substr(0, sp.begin) + ep.host + ":" + std::to_string(shifted) + channel.substr(sp.end);
}

} // namespace rune::client::shard
