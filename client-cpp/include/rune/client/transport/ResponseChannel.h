// include/rune/client/transport/ResponseChannel.h
//
// NAT-safe egress (spec 2026-10-26-rune-nat-egress-design D2/D7): pure, allocation-light helpers that build the Aeron
// response-channel URIs and split a Redirect target.  No Aeron types, no state: usable from the session layer, the
// agent and the transport alike.  Cold path only (connect / metadata / redirect).
#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>

namespace rune::client::transport {

/// The client's response subscription channel for a request channel:
///   aeron:udp?control=<host>:<port>|control-mode=response
/// where <host> is the host of `requestChannel`'s `endpoint=` (a name, an IPv4 address or a bracketed `[v6]`) and
/// <port> the broker's response-control port.  nullopt when the request channel is not UDP, has no `endpoint=host:port`,
/// or `port` is outside 1..65535 (0 = the broker advertises none).
[[nodiscard]] inline std::optional<std::string> responseControlChannelFor(std::string_view requestChannel,
                                                                          int32_t port) {
    constexpr std::string_view kUdp = "aeron:udp";
    if (requestChannel.substr(0, kUdp.size()) != kUdp) return std::nullopt;
    if (port < 1 || port > 65535) return std::nullopt;
    const auto q = requestChannel.find('?');
    if (q == std::string_view::npos) return std::nullopt;
    std::string_view endpoint;
    std::size_t pos = q + 1;
    bool found = false;
    while (pos <= requestChannel.size()) {
        std::size_t end = requestChannel.find('|', pos);
        if (end == std::string_view::npos) end = requestChannel.size();
        const std::string_view kv = requestChannel.substr(pos, end - pos);
        constexpr std::string_view kKey = "endpoint=";
        if (kv.substr(0, kKey.size()) == kKey) {
            endpoint = kv.substr(kKey.size());
            found = true;
            break;
        }
        pos = end + 1;
    }
    if (!found) return std::nullopt;
    const auto colon = endpoint.rfind(':');
    if (colon == std::string_view::npos || colon == 0) return std::nullopt;
    const std::string_view host = endpoint.substr(0, colon);
    std::string out = "aeron:udp?control=";
    out.append(host);
    out.push_back(':');
    out.append(std::to_string(port));
    out.append("|control-mode=response");
    return out;
}

/// `channel` + the request publication's `response-correlation-id=<regId>` (the response subscription's registration id).
/// A channel with no `?` gets one instead of the `|`.
[[nodiscard]] inline std::string withResponseCorrelation(std::string_view channel, int64_t regId) {
    std::string out(channel);
    out.push_back(channel.find('?') == std::string_view::npos ? '?' : '|');
    out.append("response-correlation-id=");
    out.append(std::to_string(regId));
    return out;
}

/// A Redirect ErrorResponse message to a client >= 16 may end with `#<leader gateway response-control port>`.
struct RedirectTarget {
    std::string_view endpoint;      ///< the text before the LAST '#' (the whole message when there is none)
    int32_t          responsePort;  ///< the digits after it when all digits and 1..65535, else 0
};

[[nodiscard]] inline RedirectTarget splitRedirectTarget(std::string_view msg) noexcept {
    const auto hash = msg.rfind('#');
    if (hash == std::string_view::npos) return {msg, 0};
    const std::string_view digits = msg.substr(hash + 1);
    int32_t port = 0;
    if (!digits.empty() && digits.size() <= 5) {
        int32_t v = 0;
        bool ok = true;
        for (const char c : digits) {
            if (c < '0' || c > '9') { ok = false; break; }
            v = v * 10 + (c - '0');
        }
        if (ok && v >= 1 && v <= 65535) port = v;
    }
    return {msg.substr(0, hash), port};
}

} // namespace rune::client::transport
