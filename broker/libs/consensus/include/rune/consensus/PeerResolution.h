#pragma once
// PeerResolution.h — consumer side of off-thread peer hostname resolution
// (DR-15 fix round 1, 2026-10-05).
//
// No duty thread (worker / runtime / driver conductor) may ever call
// getaddrinfo: a slow resolver (CoreDNS restarting, Docker embedded DNS lag)
// would freeze it for seconds.  One COLD resolver thread (rune::node::
// PeerResolverHub) resolves the configured peer hostnames and posts
// "host #i resolved to <ipv4>" on one ring per consumer:
//   - every embedded driver's conductor (its Aeron name resolver answers peer
//     hosts from this cache ONLY — PeerNameResolver.h), and
//   - every owner of peer publications (shard / legacy planes, the internal
//     partition transport, the consensus sink), which re-add a peer whose
//     registration errored as soon as its host resolves — event-driven, never
//     on a pass / send count.
//
// The host table is IMMUTABLE once built (before any consumer thread exists);
// each ring has exactly one consumer thread; everything else crosses threads
// as a ring message (rune shared-nothing law).

#include <rune/consensus/ControlRing.h>

#include <arpa/inet.h>
#include <netinet/in.h>

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

namespace rune::consensus {

/// The managed peer hostnames (non-numeric peer hosts).  Immutable after
/// construction; shared read-only by every thread.
class PeerHostTable {
public:
    PeerHostTable() = default;
    explicit PeerHostTable(std::vector<std::string> hosts) : hosts_(std::move(hosts)) {}

    [[nodiscard]] int indexOf(std::string_view host) const noexcept {
        for (std::size_t i = 0; i < hosts_.size(); ++i)
            if (hosts_[i] == host) return static_cast<int>(i);
        return -1;
    }
    [[nodiscard]] std::size_t size() const noexcept { return hosts_.size(); }
    [[nodiscard]] const std::string& host(std::size_t i) const noexcept { return hosts_[i]; }

    /// Host part of an Aeron channel's `endpoint=` (empty when absent).
    [[nodiscard]] static std::string_view endpointHost(std::string_view channel) noexcept {
        auto e = channel.find("?endpoint=");
        if (e == std::string_view::npos) e = channel.find("|endpoint=");
        if (e == std::string_view::npos) return {};
        std::string_view hp = channel.substr(e + 10);
        if (const auto bar = hp.find('|'); bar != std::string_view::npos) hp = hp.substr(0, bar);
        if (!hp.empty() && hp.front() == '[') return {};   // IPv6 literal: numeric
        const auto colon = hp.rfind(':');
        return colon == std::string_view::npos ? hp : hp.substr(0, colon);
    }

    /// Numeric IPv4 / IPv6 literal (never needs a name lookup).
    [[nodiscard]] static bool isNumericHost(std::string_view host) {
        std::string h(host);
        if (h.size() >= 2 && h.front() == '[' && h.back() == ']') h = h.substr(1, h.size() - 2);
        in_addr a4{};
        in6_addr a6{};
        return ::inet_pton(AF_INET, h.c_str(), &a4) == 1 || ::inet_pton(AF_INET6, h.c_str(), &a6) == 1;
    }

    /// `channel` with its `endpoint=` host replaced by `host` (unchanged when it
    /// has no endpoint).  Cold (allocates).
    [[nodiscard]] static std::string withEndpointHost(std::string_view channel, std::string_view host) {
        auto e = channel.find("?endpoint=");
        if (e == std::string_view::npos) e = channel.find("|endpoint=");
        if (e == std::string_view::npos) return std::string(channel);
        const std::size_t hs = e + 10;
        std::size_t end = channel.find('|', hs);
        if (end == std::string_view::npos) end = channel.size();
        const std::string_view hp = channel.substr(hs, end - hs);
        const auto colon = hp.rfind(':');
        const std::size_t he = colon == std::string_view::npos ? end : hs + colon;
        std::string out(channel.substr(0, hs));
        out += host;
        out += channel.substr(he);
        return out;
    }

private:
    std::vector<std::string> hosts_;
};

/// One consumer's ring (single consumer thread, the hub thread produces).
using PeerResolutionRing = ControlRing<4096>;
/// {a = host index, b = IPv4, network order}: a first resolution, an address
/// change, AND every later confirmation of the same address (each hub re-check,
/// ~1 s) — so an owner whose add failed for a NON-DNS reason while the host was
/// already cached (fd exhaustion, a transient driver error) is re-added on the
/// next confirmation (fix round 2, MEDIUM-A).
inline constexpr std::int32_t kPeerResolvedV4 = 1;

// ---------------------------------------------------------------------------
// Ad-hoc host resolution (fix round 2, LOW-4): CLIENT-supplied reply-channel
// hostnames are not in the peer table; the gateway asks the hub to resolve
// them off-thread and adds the client's egress publication on the NUMERIC
// endpoint — no driver conductor ever resolves a client name.
//   request  {a = request id, b = (intptr) new std::string(host)} — ownership
//            of the string moves to the hub (it deletes it, also undelivered
//            ones at its destruction);
//   response {a = request id, b = IPv4 (network order)} or kHostUnresolved.
// ---------------------------------------------------------------------------
using HostResolveRing = ControlRing<4096>;
inline constexpr std::int32_t kHostResolveRequest = 1;
inline constexpr std::int32_t kHostResolved       = 2;
inline constexpr std::int32_t kHostUnresolved     = 3;
struct HostResolvePort {
    HostResolveRing* req  = nullptr;   // requester → hub
    HostResolveRing* resp = nullptr;   // hub → requester
    [[nodiscard]] bool valid() const noexcept { return req != nullptr && resp != nullptr; }
};

/// What a consumer is handed by the hub: the shared table + its OWN ring.
struct PeerResolutionSource {
    const PeerHostTable* table = nullptr;
    PeerResolutionRing*  ring  = nullptr;
    [[nodiscard]] bool valid() const noexcept { return table != nullptr && ring != nullptr; }
};

/// Consumer-thread state: last address + resolution generation per host.
/// Owned by exactly one thread (the ring's consumer).
class PeerResolutionInbox {
public:
    PeerResolutionInbox() = default;
    explicit PeerResolutionInbox(PeerResolutionSource s) { attach(s); }

    /// Cold (allocates).  Before the consumer thread starts polling.
    void attach(PeerResolutionSource s) {
        src_ = s;
        entries_.assign(s.table != nullptr ? s.table->size() : 0, Entry{});
    }
    [[nodiscard]] bool attached() const noexcept { return src_.valid(); }
    [[nodiscard]] const PeerHostTable* table() const noexcept { return src_.table; }

    /// Drain the ring; onResolved(hostIndex) per committed update.  An empty
    /// ring costs a few loads (no stores).  Returns updates applied.
    template <typename F>
    int poll(F&& onResolved) noexcept {
        if (src_.ring == nullptr) [[unlikely]] return 0;
        int n = 0;
        src_.ring->poll([&](std::int32_t type, std::int64_t a, std::int64_t b) {
            if (type != kPeerResolvedV4 || a < 0 || static_cast<std::size_t>(a) >= entries_.size()) return;
            Entry& e = entries_[static_cast<std::size_t>(a)];
            e.ipv4 = static_cast<std::uint32_t>(b);
            e.has  = true;
            ++e.gen;
            ++n;
            onResolved(static_cast<int>(a));
        });
        return n;
    }
    int poll() noexcept { return poll([](int) {}); }

    /// Resolution events seen for host `idx` (0 = never resolved).
    [[nodiscard]] std::uint32_t generation(int idx) const noexcept {
        return (idx >= 0 && static_cast<std::size_t>(idx) < entries_.size())
                   ? entries_[static_cast<std::size_t>(idx)].gen : 0;
    }
    /// Last address of host `idx` (network order); false = never resolved.
    [[nodiscard]] bool addressOf(int idx, std::uint32_t& ipv4) const noexcept {
        if (idx < 0 || static_cast<std::size_t>(idx) >= entries_.size()) return false;
        const Entry& e = entries_[static_cast<std::size_t>(idx)];
        ipv4 = e.ipv4;
        return e.has;
    }
    /// The channel to ADD a publication on: `channel` with its managed endpoint
    /// host replaced by the address this inbox last received for it (numeric —
    /// the driver never resolves it), or `channel` unchanged when the host is
    /// unmanaged / not resolved yet.  DR-4 (2026-10-07): a re-add after the
    /// host's address CHANGED must not depend on the driver's own name cache
    /// having applied the change first — a hostname add resolved on the stale
    /// cache kept the publication on the old address (another member).  Cold
    /// (allocates).
    [[nodiscard]] std::string addChannel(const std::string& channel, int hostIdx) const {
        std::uint32_t ip = 0;
        if (!addressOf(hostIdx, ip)) return channel;
        in_addr a{};
        a.s_addr = ip;
        char buf[INET_ADDRSTRLEN] = {};
        if (::inet_ntop(AF_INET, &a, buf, sizeof buf) == nullptr) return channel;
        return PeerHostTable::withEndpointHost(channel, buf);
    }
    /// Index of a channel's endpoint host in the managed table (-1: unmanaged,
    /// numeric, or no table).
    [[nodiscard]] int hostIndexOfChannel(std::string_view channel) const noexcept {
        if (src_.table == nullptr) return -1;
        return src_.table->indexOf(PeerHostTable::endpointHost(channel));
    }

private:
    struct Entry {
        std::uint32_t ipv4 = 0;
        std::uint32_t gen  = 0;
        bool          has  = false;
    };
    PeerResolutionSource src_{};
    std::vector<Entry>   entries_;
};

} // namespace rune::consensus
