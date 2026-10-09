// src/shard/ShardConfig.cpp — client-shard configuration (T15a).  Cold path only:
// everything here runs once, on the control thread, before any shard driver starts.
#include <rune/client/shard/ShardConfig.h>

#include <algorithm>
#include <charconv>
#include <climits>
#include <fstream>
#include <sstream>
#include <stdexcept>
#include <system_error>

#include <arpa/inet.h>
#include <cerrno>
#include <cstdio>
#include <cstring>
#include <ifaddrs.h>
#include <netdb.h>
#include <netinet/in.h>
#include <optional>
#include <sys/socket.h>

#if defined(__linux__)
#include <sched.h>
#elif defined(__APPLE__)
#include <sys/sysctl.h>
#include <sys/types.h>
#endif

namespace rune::client::shard {

namespace {

std::string joinCores(const CpuSet& s) {
    std::string out;
    for (std::size_t i = 0; i < s.size(); ++i) {
        if (i != 0) out += ',';
        out += std::to_string(s[i]);
    }
    return out.empty() ? "<none>" : out;
}

bool contains(const CpuSet& s, int32_t core) {
    return std::binary_search(s.begin(), s.end(), core);
}

/// "localhost", "127.0.0.1" and "::1" all name this host's loopback: compare equal.
std::string normalizeHost(const std::string& h) {
    if (h == "localhost" || h == "127.0.0.1" || h == "::1" || h == "[::1]") return "127.0.0.1";
    return h;
}

std::string stripBrackets(std::string h) {
    if (h.size() >= 2 && h.front() == '[' && h.back() == ']') h = h.substr(1, h.size() - 2);
    return h;
}

bool isWildcard(const std::string& h) { return h == "0.0.0.0" || h == "::" || h == "*"; }

/// Every address `host` names (numeric or resolved); empty when it does not resolve.  Cold path.
std::vector<sockaddr_storage> addressesOf(const std::string& host) {
    std::vector<sockaddr_storage> out;
    addrinfo hints{};
    hints.ai_family   = AF_UNSPEC;
    hints.ai_socktype = SOCK_DGRAM;
    addrinfo* res     = nullptr;
    if (::getaddrinfo(host.c_str(), nullptr, &hints, &res) != 0) return out;
    for (addrinfo* a = res; a != nullptr; a = a->ai_next) {
        if (a->ai_addrlen > sizeof(sockaddr_storage)) continue;
        sockaddr_storage ss{};
        std::memcpy(&ss, a->ai_addr, a->ai_addrlen);
        out.push_back(ss);
    }
    ::freeaddrinfo(res);
    return out;
}

bool sameAddress(const sockaddr_storage& a, const sockaddr_storage& b) {
    if (a.ss_family != b.ss_family) return false;
    if (a.ss_family == AF_INET) {
        return reinterpret_cast<const sockaddr_in&>(a).sin_addr.s_addr ==
               reinterpret_cast<const sockaddr_in&>(b).sin_addr.s_addr;
    }
    if (a.ss_family == AF_INET6) {
        return std::memcmp(&reinterpret_cast<const sockaddr_in6&>(a).sin6_addr,
                           &reinterpret_cast<const sockaddr_in6&>(b).sin6_addr, sizeof(in6_addr)) == 0;
    }
    return false;
}

bool isLoopback(const sockaddr_storage& a) {
    if (a.ss_family == AF_INET) {
        return (ntohl(reinterpret_cast<const sockaddr_in&>(a).sin_addr.s_addr) >> 24) == 127;
    }
    if (a.ss_family == AF_INET6) {
        const in6_addr& v6 = reinterpret_cast<const sockaddr_in6&>(a).sin6_addr;
        return IN6_IS_ADDR_LOOPBACK(&v6) != 0;
    }
    return false;
}

/// An address of one of this host's interfaces (or loopback).
bool isLocal(const sockaddr_storage& a) {
    if (isLoopback(a)) return true;
    ifaddrs* ifs = nullptr;
    if (::getifaddrs(&ifs) != 0) return false;
    bool local = false;
    for (ifaddrs* i = ifs; i != nullptr && !local; i = i->ifa_next) {
        if (i->ifa_addr == nullptr) continue;
        sockaddr_storage ss{};
        const std::size_t len = i->ifa_addr->sa_family == AF_INET    ? sizeof(sockaddr_in)
                              : i->ifa_addr->sa_family == AF_INET6 ? sizeof(sockaddr_in6)
                                                                    : 0;
        if (len == 0) continue;
        std::memcpy(&ss, i->ifa_addr, len);
        local = sameAddress(ss, a);
    }
    ::freeifaddrs(ifs);
    return local;
}

/// Would a local bind of `egressHost` take the same socket as the gateway at `gatewayHost` (same port)?
/// Same name (localhost == 127.0.0.1 == ::1); a wildcard egress (0.0.0.0 / [::]) against a gateway on this
/// host; or two names / an alias resolving to a common address (C-F31 / review m3).
bool sameLocalBind(const std::string& egressHost, const std::string& gatewayHost) {
    const std::string e = stripBrackets(egressHost);
    const std::string g = stripBrackets(gatewayHost);
    if (normalizeHost(e) == normalizeHost(g)) return true;
    const auto gAddrs = addressesOf(g);
    if (isWildcard(e)) {
        for (const auto& ga : gAddrs)
            if (isLocal(ga)) return true;
        return false;
    }
    for (const auto& ea : addressesOf(e))
        for (const auto& ga : gAddrs)
            if (sameAddress(ea, ga)) return true;
    return false;
}

/// The gateway channel's endpoint=, else its MDC control= endpoint, else nullopt (aeron:ipc).
std::optional<ChannelEndpoint> gatewayEndpoint(const std::string& channel) {
    if (auto ep = parseChannelEndpoint(channel)) return ep;
    const std::string_view ch(channel);
    std::size_t pos = ch.find('?');
    while (pos != std::string_view::npos) {
        const std::size_t keyStart = pos + 1;
        if (ch.substr(keyStart, 8) == "control=") {
            const std::size_t vb = keyStart + 8;
            std::size_t       ve = ch.find('|', vb);
            if (ve == std::string_view::npos) ve = ch.size();
            return detail::splitEndpoint(ch.substr(vb, ve - vb), ch);
        }
        pos = ch.find('|', keyStart);
    }
    return std::nullopt;
}

bool parseInt64(std::string_view s, int64_t& out) noexcept {
    const auto [ptr, ec] = std::from_chars(s.data(), s.data() + s.size(), out, 10);
    return !s.empty() && ec == std::errc{} && ptr == s.data() + s.size();
}

std::string_view trim(std::string_view s) noexcept {
    while (!s.empty() && (s.front() == ' ' || s.front() == '\t' || s.front() == '\n' || s.front() == '\r'))
        s.remove_prefix(1);
    while (!s.empty() && (s.back() == ' ' || s.back() == '\t' || s.back() == '\n' || s.back() == '\r'))
        s.remove_suffix(1);
    return s;
}

} // namespace

// ---------------------------------------------------------------------------
// pure
// ---------------------------------------------------------------------------

int32_t quotaCpusFromCfs(int64_t quotaUs, int64_t periodUs) noexcept {
    if (quotaUs <= 0 || periodUs <= 0) return kNoCpuQuota;
    const int64_t cpus = (quotaUs + periodUs - 1) / periodUs;  // ceil: 2.5 CPUs run 3 shards
    return cpus > INT32_MAX ? INT32_MAX : static_cast<int32_t>(cpus);
}

int32_t parseCgroupV2CpuMax(std::string_view text) noexcept {
    text                    = trim(text);
    const std::size_t space = text.find(' ');
    if (space == std::string_view::npos) return kNoCpuQuota;
    const std::string_view quota  = text.substr(0, space);
    const std::string_view period = trim(text.substr(space + 1));
    if (quota == "max") return kNoCpuQuota;
    int64_t q = 0;
    int64_t p = 0;
    if (!parseInt64(quota, q) || !parseInt64(period, p)) return kNoCpuQuota;
    return quotaCpusFromCfs(q, p);
}

int32_t cgroupV2QuotaCpus(const std::string& cgroupRoot, std::string_view procSelfCgroup) {
    // The unified-hierarchy line of /proc/self/cgroup: "0::<path>".
    std::string_view path;
    bool             found = false;
    while (!procSelfCgroup.empty()) {
        const std::size_t nl   = procSelfCgroup.find('\n');
        const std::string_view line = trim(procSelfCgroup.substr(0, nl));
        procSelfCgroup.remove_prefix(nl == std::string_view::npos ? procSelfCgroup.size() : nl + 1);
        if (line.substr(0, 3) == "0::") {
            path  = line.substr(3);
            found = true;
            break;
        }
    }
    if (!found) return kNoCpuQuota;
    while (!path.empty() && path.back() == '/') path.remove_suffix(1);  // "/" -> "" (the root)
    int32_t tightest = kNoCpuQuota;
    for (;;) {
        std::ifstream f(cgroupRoot + std::string(path) + "/cpu.max");
        std::string   line;
        if (f && std::getline(f, line)) {
            const int32_t q = parseCgroupV2CpuMax(line);
            if (q != kNoCpuQuota && (tightest == kNoCpuQuota || q < tightest)) tightest = q;
        }
        if (path.empty()) break;  // the root was read
        const std::size_t slash = path.rfind('/');
        path = slash == std::string_view::npos ? std::string_view() : path.substr(0, slash);
    }
    return tightest;
}

ShardCount resolveShardCount(const ShardGroupConfig& g, const CpuSet& allowed, int32_t quotaCpus) {
    if (g.shardCount != kAutoShards) {
        return {g.shardCount, "shardCount " + std::to_string(g.shardCount) + " (explicit)"};
    }
    if (g.mode == ShardMode::Invoker) {
        throw std::invalid_argument(
            "ShardGroupConfig: shardCount AUTO is not allowed in Invoker mode — each app thread "
            "constructs its own ClientShard(cfg, i), so N must equal the number of driving threads; "
            "set shardCount explicitly");
    }
    if (g.base.topics.empty()) {
        return {1, "shardCount AUTO: no topics known at construction; AUTO → 1 (list the topics in "
                   "base.topics or set shardCount to use more cores)"};
    }
    int64_t cpus = static_cast<int64_t>(allowed.size());
    if (quotaCpus != kNoCpuQuota && quotaCpus < cpus) cpus = quotaCpus;
    const int64_t usable = cpus - static_cast<int64_t>(g.reservedCores);
    int32_t       cap    = 1;
    for (const auto& t : g.base.topics) cap = std::max(cap, t.partitionCount);
    const int64_t n = std::max<int64_t>(1, std::min<int64_t>(usable, cap));

    std::ostringstream why;
    why << "shardCount AUTO → " << n << " (allowed cpus [" << joinCores(allowed) << "] = "
        << allowed.size() << ", cgroup quota "
        << (quotaCpus == kNoCpuQuota ? std::string("none") : std::to_string(quotaCpus))
        << ", reserved " << g.reservedCores << ", usable " << usable
        << ", max partitionCount " << cap << ")";
    return {static_cast<uint32_t>(n), why.str()};
}

std::vector<int32_t> assignShardCores(const ShardGroupConfig& g, uint32_t count, const CpuSet& allowed,
                                      bool pinningSupported) {
    const bool wantsPin = g.pin || g.firstCore >= 0;
    if (wantsPin && !pinningSupported) {
        throw std::invalid_argument(
            "ShardGroupConfig: thread pinning (pin = true or firstCore >= 0) is unsupported on this "
            "platform; rejected rather than silently not pinning");
    }
    if (wantsPin && g.mode == ShardMode::Invoker) {
        throw std::invalid_argument(
            "ShardGroupConfig: pin / firstCore apply to Agent runners; in Invoker mode the app thread "
            "is the owner — pin it yourself");
    }
    std::vector<int32_t> cores(count, -1);
    if (g.firstCore >= 0) {
        for (uint32_t i = 0; i < count; ++i) {
            const int64_t core = static_cast<int64_t>(g.firstCore) + i;
            if (core > INT32_MAX || !contains(allowed, static_cast<int32_t>(core))) {
                throw std::invalid_argument("ShardGroupConfig: shard " + std::to_string(i) + " core " +
                                            std::to_string(core) +
                                            " is not in the calling thread's affinity mask; allowed "
                                            "cores: " + joinCores(allowed));
            }
            cores[i] = static_cast<int32_t>(core);
        }
        return cores;
    }
    if (g.pin) {
        const std::size_t reserved = g.reservedCores;
        if (allowed.size() < reserved || allowed.size() - reserved < count) {
            throw std::invalid_argument(
                "ShardGroupConfig: pin = true needs " + std::to_string(count) +
                " allowed cores after the " + std::to_string(reserved) +
                " reserved ones; allowed cores: " + joinCores(allowed));
        }
        for (uint32_t i = 0; i < count; ++i) cores[i] = allowed[reserved + i];
    }
    return cores;
}

void validateShardGroupConfig(const ShardGroupConfig& g, uint32_t count) {
    if (count == 0) throw std::invalid_argument("ShardGroupConfig: shard count must be >= 1");

    // Egress: shard i listens on base port + i.  Every port must fit, and none may
    // be the gateway's own endpoint on the same host.
    // The gateway's endpoint=, or its MDC control= endpoint (C-F31 / review m3).
    const auto gateway = gatewayEndpoint(g.base.gatewayChannel);
    for (uint32_t i = 0; i < count; ++i) {
        std::string ch;
        try {
            ch = channelWithPortOffset(g.base.egressChannel, static_cast<int32_t>(i));
        } catch (const std::invalid_argument& e) {
            throw std::invalid_argument("ShardGroupConfig: shard " + std::to_string(i) +
                                        " egress channel: " + e.what());
        }
        const auto ep = parseChannelEndpoint(ch);
        if (ep && gateway && ep->port == gateway->port && sameLocalBind(ep->host, gateway->host)) {
            throw std::invalid_argument("ShardGroupConfig: shard " + std::to_string(i) +
                                        " egress endpoint " + ep->host + ":" + std::to_string(ep->port) +
                                        " collides with the gateway endpoint " + g.base.gatewayChannel);
        }
    }

    // fix1 (review m4): every size check below runs on the transport config the
    // driver will REALLY get — after the RUNE_* env overrides (toAgentConfig) —
    // still before any driver starts.  Env overrides are process-wide, so every
    // shard gets the same sizes: shard 0's effective config stands for all.
    const transport::TransportConfig t =
        toAgentConfig(shardClientConfig(g, 0, count, -1)).transport;
    if (auto err = transport::validate(t)) {
        throw std::invalid_argument("ShardGroupConfig: shard 0 transport (after env overrides): " +
                                    *err);
    }

    // Term: the max message is term/8, which must hold the largest frame.
    const std::size_t term = t.termBufferLength;
    if (g.producer.maxFrameBytes > 0 && static_cast<std::size_t>(g.producer.maxFrameBytes) > term / 8) {
        throw std::invalid_argument("ShardGroupConfig: producer.maxFrameBytes (" +
                                    std::to_string(g.producer.maxFrameBytes) +
                                    ") exceeds the shard term's max message (shardTermBufferBytes/8 = " +
                                    std::to_string(term / 8) + ")");
    }
    const std::size_t window = t.rcvInitialWindowLength != 0 ? t.rcvInitialWindowLength
                                                             : transport::kAeronDefaultRcvWindowLength;
    if (window > term / 2) {
        throw std::invalid_argument("ShardGroupConfig: shardTermBufferBytes (" + std::to_string(term) +
                                    ") must be >= 2x the Aeron receive window (" +
                                    std::to_string(window) + ")");
    }
}

std::vector<std::string> shardConfigWarnings(const ShardGroupConfig& g, uint32_t count) {
    std::vector<std::string> w;
    if (g.aeronClientLivenessTimeoutNs < transport::kMaxClientLivenessTimeoutNs) {
        w.push_back("rune-client: shard aeronClientLivenessTimeoutNs = " +
                    std::to_string(g.aeronClientLivenessTimeoutNs) +
                    " ns: the shard's driver and client share one duty thread, so a stall longer than "
                    "this closes the shard's Aeron client for good (default: 1 h, unbounded)");
    }
    if (g.mode == ShardMode::Agent && g.shardCount == kAutoShards &&
        g.base.idleMode == IdleMode::BusySpin && !g.pin && g.firstCore < 0) {
        w.push_back("rune-client: BusySpin with shardCount AUTO (" + std::to_string(count) +
                    " runners) pegs " + std::to_string(count) +
                    " cores unpinned; set pin = true (or firstCore) to give each runner its own core");
    }
    return w;
}

ClientConfig shardClientConfig(const ShardGroupConfig& g, uint32_t index, uint32_t count,
                               int32_t runnerCore) {
    if (count == 0 || index >= count) {
        throw std::invalid_argument("shardClientConfig: shard index " + std::to_string(index) +
                                    " out of range for shard count " + std::to_string(count));
    }
    ClientConfig c                 = g.base;
    c.dataDir                      = g.base.dataDir + "/shard-" + std::to_string(index);
    c.egressChannel                = channelWithPortOffset(g.base.egressChannel, static_cast<int32_t>(index));
    c.runnerMode                   = g.mode == ShardMode::Invoker ? RunnerMode::Invoker : RunnerMode::Thread;
    c.runnerCore                   = g.mode == ShardMode::Agent ? runnerCore : -1;
    c.driverOnDutyThread           = true;  // D1: one thread per shard
    c.reconnectForeverAfterLive    = true;  // R1: never give up once Live
    c.termBufferBytes              = g.shardTermBufferBytes;
    c.aeronClientLivenessTimeoutNs = g.aeronClientLivenessTimeoutNs;
    return c;
}

ClientConfig shardClientConfig(const ShardGroupConfig& g, uint32_t index) {
    if (g.shardCount == kAutoShards) {
        throw std::invalid_argument(
            "shardClientConfig(g, i): shardCount is AUTO — resolve it first (resolveShardCount) "
            "and use shardClientConfig(g, i, count, core)");
    }
    if (index >= g.shardCount) {
        throw std::invalid_argument("shardClientConfig: shard index " + std::to_string(index) +
                                    " out of range for shard count " + std::to_string(g.shardCount));
    }
    // fix1 (review m5): the same core rules as a group — pin / firstCore are
    // honoured or rejected (mask, platform, Invoker), never silently dropped.
    const std::vector<int32_t> cores =
        assignShardCores(g, g.shardCount, platformAllowedCpus(), platformPinningSupported());
    return shardClientConfig(g, index, g.shardCount, cores[index]);
}

ProducerConfig shardProducerConfig(const ShardGroupConfig& g) {
    ProducerConfig p = g.producer;
    if (g.mode == ShardMode::Invoker) p.lingerUs = g.invokerLingerUs;
    return p;
}

// ---------------------------------------------------------------------------
// platform
// ---------------------------------------------------------------------------

CpuSet platformAllowedCpus() {
    CpuSet out;
#if defined(__linux__)
    // C-F31 (review m2): a fixed cpu_set_t holds CPU_SETSIZE (1024) CPUs and sched_getaffinity fails EINVAL
    // on a larger host — size the set dynamically and grow it until the kernel's mask fits.
    for (int ncpus = CPU_SETSIZE; ncpus <= (1 << 20); ncpus *= 2) {
        cpu_set_t* set = CPU_ALLOC(ncpus);
        if (set == nullptr) break;
        const std::size_t bytes = CPU_ALLOC_SIZE(ncpus);
        CPU_ZERO_S(bytes, set);
        if (sched_getaffinity(0, bytes, set) == 0) {
            for (int c = 0; c < ncpus; ++c) {
                if (CPU_ISSET_S(c, bytes, set)) out.push_back(c);
            }
            CPU_FREE(set);
            break;
        }
        const int err = errno;
        CPU_FREE(set);
        if (err != EINVAL) break;
    }
    if (out.empty()) std::fprintf(stderr, "rune-client: sched_getaffinity failed; allowed CPUs fall back to {0}\n");
#elif defined(__APPLE__)
    int    active = 0;
    size_t len    = sizeof(active);
    if (sysctlbyname("hw.activecpu", &active, &len, nullptr, 0) == 0) {
        for (int c = 0; c < active; ++c) out.push_back(c);
    }
#endif
    if (out.empty()) out.push_back(0);
    return out;
}

int32_t platformQuotaCpus() {
#if defined(__linux__)
    // cgroup v2 (the root cpu.max exists only on the unified hierarchy): the process's own cgroup and its
    // ancestors (C-F31 / review m1 — a systemd CPUQuota= service, a container without cgroupns).
    if (std::ifstream("/sys/fs/cgroup/cgroup.controllers")) {
        std::ifstream      self("/proc/self/cgroup");
        std::ostringstream text;
        text << self.rdbuf();
        return cgroupV2QuotaCpus("/sys/fs/cgroup", text.str());
    }
    std::ifstream q("/sys/fs/cgroup/cpu/cpu.cfs_quota_us");
    std::ifstream p("/sys/fs/cgroup/cpu/cpu.cfs_period_us");
    int64_t quota = 0;
    int64_t period = 0;
    if (q >> quota && p >> period) return quotaCpusFromCfs(quota, period);
    return kNoCpuQuota;
#else
    return kNoCpuQuota;
#endif
}

} // namespace rune::client::shard
