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
    const auto gateway = parseChannelEndpoint(g.base.gatewayChannel);
    for (uint32_t i = 0; i < count; ++i) {
        std::string ch;
        try {
            ch = channelWithPortOffset(g.base.egressChannel, static_cast<int32_t>(i));
        } catch (const std::invalid_argument& e) {
            throw std::invalid_argument("ShardGroupConfig: shard " + std::to_string(i) +
                                        " egress channel: " + e.what());
        }
        const auto ep = parseChannelEndpoint(ch);
        if (ep && gateway && ep->port == gateway->port &&
            normalizeHost(ep->host) == normalizeHost(gateway->host)) {
            throw std::invalid_argument("ShardGroupConfig: shard " + std::to_string(i) +
                                        " egress endpoint " + ep->host + ":" + std::to_string(ep->port) +
                                        " collides with the gateway endpoint " + g.base.gatewayChannel);
        }
    }

    // Term: the max message is term/8, which must hold the largest frame.
    const std::size_t term = g.shardTermBufferBytes;
    if (g.producer.maxFrameBytes > 0 && static_cast<std::size_t>(g.producer.maxFrameBytes) > term / 8) {
        throw std::invalid_argument("ShardGroupConfig: producer.maxFrameBytes (" +
                                    std::to_string(g.producer.maxFrameBytes) +
                                    ") exceeds the shard term's max message (shardTermBufferBytes/8 = " +
                                    std::to_string(term / 8) + ")");
    }
    if (transport::kAeronDefaultRcvWindowLength > term / 2) {
        throw std::invalid_argument("ShardGroupConfig: shardTermBufferBytes (" + std::to_string(term) +
                                    ") must be >= 2x the Aeron receive window (" +
                                    std::to_string(transport::kAeronDefaultRcvWindowLength) + ")");
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
    const int32_t core = (g.mode == ShardMode::Agent && g.firstCore >= 0)
                             ? g.firstCore + static_cast<int32_t>(index)
                             : -1;
    return shardClientConfig(g, index, g.shardCount, core);
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
    cpu_set_t set;
    CPU_ZERO(&set);
    if (sched_getaffinity(0, sizeof(set), &set) == 0) {
        for (int c = 0; c < CPU_SETSIZE; ++c) {
            if (CPU_ISSET(c, &set)) out.push_back(c);
        }
    }
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
    {
        std::ifstream v2("/sys/fs/cgroup/cpu.max");
        if (v2) {
            std::string line;
            std::getline(v2, line);
            return parseCgroupV2CpuMax(line);
        }
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
