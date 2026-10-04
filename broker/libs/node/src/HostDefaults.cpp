// HostDefaults.cpp — see HostDefaults.h.  Startup-only (cold) code.
#include <rune/node/HostDefaults.h>
#include "rune/protocol/net/rune_gso_bindings.h"

#include <algorithm>
#include <cerrno>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <string>

#include <arpa/inet.h>
#include <ifaddrs.h>
#include <net/if.h>
#include <netdb.h>
#include <netinet/in.h>
#include <sys/ioctl.h>
#include <sys/socket.h>
#include <unistd.h>
#include <filesystem>
#include <system_error>
#if defined(__linux__)
#include <sched.h>
#include <sys/vfs.h>
#endif

namespace rune::node {

namespace {

constexpr int64_t pow2Floor(int64_t v) noexcept {
    if (v <= 0) return 0;
    int64_t p = 1;
    while (p <= v / 2) p <<= 1;
    return p;
}
constexpr int32_t alignDown(int64_t v, int32_t a) noexcept {
    return static_cast<int32_t>(v - (v % a));
}
void appendWarn(std::string& w, const std::string& msg) {
    if (!w.empty()) w += "; ";
    w += msg;
}
std::string mib(int64_t b) {
    char buf[48];
    if (b % (1024 * 1024) == 0) std::snprintf(buf, sizeof buf, "%lld MiB", static_cast<long long>(b / (1024 * 1024)));
    else std::snprintf(buf, sizeof buf, "%lld B", static_cast<long long>(b));
    return buf;
}

} // namespace

// ---------------------------------------------------------------------------
// Aeron transport (PURE)
// ---------------------------------------------------------------------------
AeronTransportDerived deriveAeronTransport(const HostNetFacts& f, const AeronTransportRequest& req) {
    AeronTransportDerived d;
    const int64_t term = req.termBufferBytes > 0 ? req.termBufferBytes : kTargetSocketBufferBytes;

    // 1. The OS ceiling for one socket's buffers (0 = unknown → assume the target).
    auto known = [](int64_t v) { return v > 0 ? v : static_cast<int64_t>(kTargetSocketBufferBytes); };
    const int64_t osMax = std::min(known(f.rmemMax), known(f.wmemMax));

    // 2. Socket buffer.
    int64_t sndCap = 0;   // the SO_SNDBUF Aeron will validate the MTU against
    int64_t rcvCap = 0;   // the SO_RCVBUF it validates the window against
    if (req.socketBufferBytes == kAeronAuto) {
        const int64_t buf = std::min<int64_t>(kTargetSocketBufferBytes, osMax);
        d.socketBufferBytes  = static_cast<int32_t>(buf);
        d.socketBufferCapped = buf < kTargetSocketBufferBytes;
        if (d.socketBufferCapped)
            appendWarn(d.warnings, "UDP socket buffers capped at " + mib(buf) + " (< 16 MiB) by the OS limit — "
                                   "throughput will suffer; set: " + socketBufferSysctlAdvice(f.platform));
        sndCap = rcvCap = buf;
    } else if (req.socketBufferBytes == 0) {
        d.socketBufferBytes = 0;   // OS default: Aeron leaves SO_*BUF alone
        sndCap = f.defaultSndBuf > 0 ? f.defaultSndBuf : kFallbackMtuBytes;
        rcvCap = f.defaultRcvBuf > 0 ? f.defaultRcvBuf : 128 * 1024;
    } else {
        // An explicit value above the OS limit is CLAMPED to it: Linux would cap it
        // silently, but macOS refuses the setsockopt outright and leaves the 9216
        // default — then aeron_driver_init EINVALs on the MTU.
        d.socketBufferBytes = static_cast<int32_t>(std::min<int64_t>(req.socketBufferBytes, osMax));
        sndCap = rcvCap = d.socketBufferBytes;
        if (req.socketBufferBytes > osMax)
            appendWarn(d.warnings, "explicit socket buffer " + mib(req.socketBufferBytes) +
                                   " exceeds the OS limit " + mib(osMax) + " — clamped to it; set: " +
                                   socketBufferSysctlAdvice(f.platform));
    }

    // 3. Receive window: <= SO_RCVBUF always; derived = pow2(min(4 MiB, kernelBuf/4,
    //    term/2)) where Linux's kernel buffer is 2 x the requested SO_RCVBUF.
    if (req.rcvWindowBytes == kAeronAuto) {
        const int64_t kernelBuf = f.platform == HostPlatform::Linux ? 2 * rcvCap : rcvCap;
        // term/4 (measured config: 256 KiB at the 1 MiB term); Aeron's own cap is term/2.
        const int64_t w = pow2Floor(std::min({static_cast<int64_t>(kMaxDerivedRcvWindowBytes), kernelBuf / 4,
                                              term / 4, rcvCap}));
        d.rcvWindowBytes = static_cast<int32_t>(std::max<int64_t>(w, kMinMtuBytes));
    } else {
        d.rcvWindowBytes = req.rcvWindowBytes;
        if (req.rcvWindowBytes > rcvCap) {
            d.rcvWindowBytes = static_cast<int32_t>(std::max<int64_t>(pow2Floor(rcvCap), kMinMtuBytes));
            appendWarn(d.warnings, "explicit receive window " + mib(req.rcvWindowBytes) + " > SO_RCVBUF " +
                                   mib(rcvCap) + " (Aeron would refuse it) — clamped to " + mib(d.rcvWindowBytes));
        }
    }

    // 4. MTU: <= SO_SNDBUF (aeron_driver_init EINVAL), <= window (receive endpoint
    //    EINVAL), <= 65504, multiple of 32.  Derived also <= window/4 (>= 4
    //    datagrams in flight).
    //    The receiver's effective window is min(window, term/2) (Aeron
    //    aeron_receiver_window_length) — the receive endpoint refuses mtu above it.
    const int64_t hardMtuCap = std::min({sndCap, static_cast<int64_t>(d.rcvWindowBytes), term / 2,
                                         static_cast<int64_t>(kLoopbackMtuBytes)});
    if (req.mtuBytes == kAeronAuto) {
        int64_t mtu = kFallbackMtuBytes;
        if (f.loopback) mtu = kLoopbackMtuBytes;
        else if (f.interfaceMtu > 0)
            mtu = f.interfaceMtu - (f.ipv6 ? kIpv6UdpHeaderBytes : kIpv4UdpHeaderBytes);
        mtu = std::min({mtu, hardMtuCap, std::min<int64_t>(d.rcvWindowBytes, term / 2) / 4});
        d.mtuBytes = std::max(alignDown(mtu, kAeronFrameAlignment), kMinMtuBytes);
    } else {
        d.mtuBytes = req.mtuBytes;
        const int32_t clamped = std::max(alignDown(std::min<int64_t>(req.mtuBytes, hardMtuCap), kAeronFrameAlignment),
                                         kMinMtuBytes);
        if (clamped != req.mtuBytes) {
            appendWarn(d.warnings, "explicit MTU " + std::to_string(req.mtuBytes) +
                                   " is not valid for SO_SNDBUF " + mib(sndCap) + " / window " +
                                   mib(d.rcvWindowBytes) + " / 32-byte alignment (aeron_driver_init would "
                                   "fail EINVAL) — clamped to " + std::to_string(clamped));
            d.mtuBytes = clamped;
        }
    }
    return d;
}

std::string socketBufferSysctlAdvice(HostPlatform platform) {
    if (platform == HostPlatform::MacOS)
        return "sudo sysctl -w kern.ipc.maxsockbuf=33554432";
    return "sysctl -w net.core.rmem_max=16777216 net.core.wmem_max=16777216 "
           "net.core.rmem_default=16777216 net.core.wmem_default=16777216 "
           "net.core.netdev_max_backlog=100000";
}

// ---------------------------------------------------------------------------
// Hosts
// ---------------------------------------------------------------------------
namespace {
std::string_view stripBrackets(std::string_view h) noexcept {
    if (h.size() >= 2 && h.front() == '[' && h.back() == ']') return h.substr(1, h.size() - 2);
    return h;
}
} // namespace

bool isLoopbackHost(std::string_view host) noexcept {
    host = stripBrackets(host);
    if (host == "localhost" || host == "::1") return true;
    in_addr a4{};
    const std::string h(host);
    if (inet_pton(AF_INET, h.c_str(), &a4) == 1) return (ntohl(a4.s_addr) >> 24) == 127;
    in6_addr a6{};
    if (inet_pton(AF_INET6, h.c_str(), &a6) == 1) return IN6_IS_ADDR_LOOPBACK(&a6);
    return false;
}

bool isWildcardHost(std::string_view host) noexcept {
    host = stripBrackets(host);
    return host.empty() || host == "0.0.0.0" || host == "::" || host == "*";
}

std::vector<std::string> nodeEndpointHosts(const NodeConfig& cfg) {
    std::vector<std::string> hosts;
    for (const auto& p : cfg.peers)
        if (std::find(hosts.begin(), hosts.end(), p.host) == hosts.end()) hosts.push_back(p.host);
    // The MTU follows where datagrams GO: the peers, and the client-facing
    // endpoint clients are told to use — the peer host (Metadata advertises
    // peer.host:clientPort / ingressBase), never the bind address.  A wildcard
    // bind therefore adds nothing (k3s regression: 0.0.0.0 + 127.0.0.1 peers
    // derived the NIC's 1472 → 14-18k rec/s instead of 136k at 65504).  A
    // concrete non-wildcard bind host is where this node receives: included.
    const std::string client = cfg.clientEndpoint.channel.empty() ? std::string{} : cfg.clientBindHost();
    if (!isWildcardHost(client) && std::find(hosts.begin(), hosts.end(), client) == hosts.end())
        hosts.push_back(client);
    return hosts;
}

// ---------------------------------------------------------------------------
// Shard ports + affinity (PURE)
// ---------------------------------------------------------------------------
std::optional<ShardBasePorts> deriveShardBasePorts(int32_t nodeId, int32_t workers) noexcept {
    if (workers < 1 || workers > NodeConfig::kMaxShardWorkers || nodeId < 0) return std::nullopt;
    const int64_t base = kDerivedShardPortBase + static_cast<int64_t>(nodeId) * kDerivedShardPortStride;
    if (base + kDerivedShardPortStride - 1 > 65535) return std::nullopt;
    const auto b = static_cast<int32_t>(base);
    return ShardBasePorts{b, b + NodeConfig::kMaxShardWorkers, b + 2 * NodeConfig::kMaxShardWorkers};
}

int32_t deriveWorkerCoreBase(HostPlatform platform, bool allLoopback, int32_t selfIndex, int32_t nodes,
                             int32_t workers, const std::vector<int32_t>& cpus) noexcept {
    if (platform != HostPlatform::Linux || !allLoopback || workers < 1 || nodes < 1) return -1;
    if (selfIndex < 0 || selfIndex >= nodes) return -1;
    const int64_t first = 1 + static_cast<int64_t>(selfIndex) * workers;   // index into the mask
    if (1 + static_cast<int64_t>(nodes) * workers > static_cast<int64_t>(cpus.size())) return -1;
    const int32_t base = cpus[static_cast<std::size_t>(first)];
    for (int32_t w = 1; w < workers; ++w)   // WorkerPool pins base + w: must be consecutive
        if (cpus[static_cast<std::size_t>(first + w)] != base + w) return -1;
    return base;
}

// ---------------------------------------------------------------------------
// applyHostDerivedDefaults (PURE given facts + cores)
// ---------------------------------------------------------------------------
HostDerivation applyHostDerivedDefaults(NodeConfig& cfg, const HostNetFacts& facts, std::string_view ioBackend) {
    HostDerivation out;
    std::string derived;   // names of the derived fields
    auto mark = [&derived](const char* n) { if (!derived.empty()) derived += ','; derived += n; };

    // Aeron transport.
    {
        AeronTransportRequest req;
        req.mtuBytes          = cfg.aeronMtuBytes;
        req.socketBufferBytes = cfg.aeronSocketBufferBytes;
        req.rcvWindowBytes    = cfg.aeronRcvWindowBytes;
        req.termBufferBytes   = cfg.aeronTermBufferBytes;
        const auto t = deriveAeronTransport(facts, req);
        if (cfg.aeronMtuBytes == NodeConfig::kAeronAuto) mark("mtu");
        if (cfg.aeronSocketBufferBytes == NodeConfig::kAeronAuto) mark("socketBuffer");
        if (cfg.aeronRcvWindowBytes == NodeConfig::kAeronAuto) mark("rcvWindow");
        cfg.aeronMtuBytes          = t.mtuBytes;
        cfg.aeronSocketBufferBytes = t.socketBufferBytes;
        cfg.aeronRcvWindowBytes    = t.rcvWindowBytes;
        out.socketBufferCapped     = t.socketBufferCapped;
        if (!t.warnings.empty()) appendWarn(out.warnings, t.warnings);
        // The ack term's receive window is term/2: it must hold at least one full MTU datagram.
        int32_t minCtl = 64 * 1024;
        while (minCtl < 2 * cfg.aeronMtuBytes) minCtl *= 2;
        if (cfg.aeronControlTermBufferBytes < minCtl) {
            appendWarn(out.warnings, "RUNE_AERON_CONTROL_TERM_BYTES=" + std::to_string(cfg.aeronControlTermBufferBytes) +
                       " is below 2 x MTU (" + std::to_string(cfg.aeronMtuBytes) + "): raised to " + std::to_string(minCtl));
            cfg.aeronControlTermBufferBytes = minCtl;
        }
    }

    // Term sparseness (non-sparse only on tmpfs).
    if (!cfg.aeronTermBufferSparse) mark("sparse");
    cfg.aeronTermBufferSparse = deriveTermSparse(cfg.aeronTermBufferSparse, facts.aeronDirTmpfs);
    if (*cfg.aeronTermBufferSparse && !facts.aeronDirTmpfs && cfg.aeronTermBufferBytes >= 16 * 1024 * 1024)
        appendWarn(out.warnings, "Aeron dir is not on tmpfs — term buffers stay sparse (page faults on the "
                                 "hot path; findings §4 measured 73-79k vs 100-105k rec/s): set "
                                 "RUNE_AERON_DIR to a /dev/shm path");

    const int32_t W = cfg.workerCount >= 1 ? cfg.workerCount : 1;

    // Shard mode: the per-worker planes it implies + derived base ports.
    if (cfg.shardMode()) {
        cfg.perWorkerIngressEnabled = true;
        cfg.perWorkerReplication    = true;
        cfg.perWorkerEgressEnabled  = true;
        bool portsDerived = false;
        const auto selfDerived = deriveShardBasePorts(cfg.nodeId, W);
        const PeerConfig* me = nullptr;
        for (const auto& p : cfg.peers) if (p.id == cfg.nodeId) me = &p;
        // Self: an explicit RUNE_WORKER_*_BASE_PORT wins; else the base RUNE_PEERS
        // advertises for self; else the derived one.
        if (!cfg.workerIngressBasePort) {
            cfg.workerIngressBasePort = (me && me->ingressBasePort) ? me->ingressBasePort
                                      : selfDerived ? std::optional<int32_t>(selfDerived->ingress) : std::nullopt;
            portsDerived = true;
        }
        if (!cfg.workerReplBasePort) {
            cfg.workerReplBasePort = (me && me->replBasePort) ? me->replBasePort
                                   : selfDerived ? std::optional<int32_t>(selfDerived->repl) : std::nullopt;
            portsDerived = true;
        }
        if (!cfg.workerEgressControlBasePort) {
            cfg.workerEgressControlBasePort = (me && me->egressControlBasePort) ? me->egressControlBasePort
                                            : selfDerived ? std::optional<int32_t>(selfDerived->egressControl)
                                                          : std::nullopt;
            portsDerived = true;
        }
        // Every peer entry (SELF INCLUDED) that does not advertise a base gets the
        // DERIVED one — exactly what every other node computes for it.  A self base
        // set only in RUNE_WORKER_*_BASE_PORT then disagrees with the self entry and
        // validateShardConfig refuses to start (peers would derive another port).
        // Egress-control base (compat): it was never required in RUNE_PEERS, so an
        // explicit-but-unadvertised SELF base is kept for self (not refused like
        // the ingress / repl bases) — but peers derive another value for this node
        // and advertise THAT to clients asking them: WARN.
        for (auto& p : cfg.peers) {
            if (p.id != cfg.nodeId || p.egressControlBasePort != 0 || !cfg.workerEgressControlBasePort) continue;
            p.egressControlBasePort = static_cast<uint16_t>(*cfg.workerEgressControlBasePort);
            if (!selfDerived || selfDerived->egressControl != *cfg.workerEgressControlBasePort)
                appendWarn(out.warnings, "RUNE_WORKER_EGRESS_CONTROL_BASE_PORT=" +
                           std::to_string(*cfg.workerEgressControlBasePort) + " is not advertised in RUNE_PEERS "
                           "(6th segment): other nodes advertise the derived base for this node to clients");
        }
        for (auto& p : cfg.peers) {
            const auto d = deriveShardBasePorts(p.id, W);
            if (!d && (p.ingressBasePort == 0 || p.replBasePort == 0 || p.egressControlBasePort == 0))
                appendWarn(out.warnings, "node id " + std::to_string(p.id) + " is outside the derived shard "
                           "port range [0, 126] — advertise its replBase:ingressBase:egressControlBase in RUNE_PEERS");
            if (p.ingressBasePort == 0 && d)       { p.ingressBasePort = static_cast<uint16_t>(d->ingress); portsDerived = true; }
            if (p.replBasePort == 0 && d)          { p.replBasePort = static_cast<uint16_t>(d->repl); portsDerived = true; }
            if (p.egressControlBasePort == 0 && d) { p.egressControlBasePort = static_cast<uint16_t>(d->egressControl); portsDerived = true; }
            if (p.id == cfg.nodeId && p.workerCount == 0) p.workerCount = W;
        }
        if (portsDerived) mark("shardPorts");
    }

    // Worker affinity (shard mode only: pinned for cache locality; idle = backoff unless opted in).
    bool allLoopback = !cfg.peers.empty();
    for (const auto& p : cfg.peers) allLoopback = allLoopback && isLoopbackHost(p.host);
    bool coreBaseDerived = false;
    if (cfg.workerCoreBase == NodeConfig::kWorkerCoreBaseAuto) {
        coreBaseDerived = true;
        int32_t selfIndex = 0;
        for (const auto& p : cfg.peers) if (p.id < cfg.nodeId) ++selfIndex;
        const auto nodes = static_cast<int32_t>(std::max<std::size_t>(cfg.peers.size(), 1));
        cfg.workerCoreBase = cfg.shardMode()
            ? deriveWorkerCoreBase(facts.platform, allLoopback, selfIndex, nodes, W, facts.allowedCpus)
            : -1;
        mark("coreBase");
    }
    // io-wq affinity (LAT-1 f): derived ONLY for the layout derived above (co-located, all loopback): every
    // node's workers sit at INDICES [1, 1 + nodes*W) of the allowed-CPU mask -> io-wq gets the other allowed
    // CPUs.  An explicit RUNE_WORKER_CORE_BASE leaves io-wq inheriting (other nodes' cores are unknown) unless
    // RUNE_IOWQ_CORES is set.
    // An explicit RUNE_IOWQ_CORES keeps only the CPUs this process may run on: a CPU outside the mask is
    // refused by newer kernels and silently ignored by older ones (io-wq would keep its default mask).
    if (!cfg.ioWorkerCpus.empty() && !facts.allowedCpus.empty()) {
        std::vector<int32_t> kept;
        for (const int32_t c : cfg.ioWorkerCpus)
            if (std::find(facts.allowedCpus.begin(), facts.allowedCpus.end(), c) != facts.allowedCpus.end())
                kept.push_back(c);
        if (kept.size() != cfg.ioWorkerCpus.size())
            appendWarn(out.warnings, std::string("RUNE_IOWQ_CORES lists CPUs outside this process's allowed set — ") +
                       (kept.empty() ? "none usable, falling back to the default io-wq placement" : "ignoring those"));
        cfg.ioWorkerCpus = std::move(kept);
    }
    // Assumes every node runs W workers (same assumption as deriveWorkerCoreBase).
    if (cfg.ioWorkerCpus.empty() && cfg.shardMode() && cfg.workerCoreBase >= 0 && coreBaseDerived) {
        const auto nodes = static_cast<int64_t>(std::max<std::size_t>(cfg.peers.size(), 1));
        const int64_t hi = 1 + nodes * W;
        for (std::size_t i = 0; i < facts.allowedCpus.size(); ++i)
            if (static_cast<int64_t>(i) < 1 || static_cast<int64_t>(i) >= hi) cfg.ioWorkerCpus.push_back(facts.allowedCpus[i]);
        if (!cfg.ioWorkerCpus.empty()) mark("ioWq");
    }

    const std::string io(ioBackend);
    std::string ioWq;
    for (const int32_t c : cfg.ioWorkerCpus) { if (!ioWq.empty()) ioWq += ','; ioWq += std::to_string(c); }
    if (ioWq.empty()) ioWq = "inherit";
    // A7: the shard-worker idle (ParkUntilWorkIdle; the workers' start line logs the RESOLVED shape).
    std::string idle = "legacy";
    if (cfg.shardMode()) {
        if (cfg.shardWorkersBusySpin()) {
            idle = "busy-spin";
        } else {
            const auto n = [](int32_t v) { return v == NodeConfig::kIdleAuto ? std::string("auto") : std::to_string(v); };
            idle = std::string(cfg.shardIdleWantsWake() ? "wake" : "backoff") + "{spins=" + n(cfg.idleSpins) +
                   " yields=" + n(cfg.idleYields) + " parkUs=1-" + std::to_string(cfg.idleMaxParkUs) + "}";
        }
    }
    char line[1024];
    std::snprintf(line, sizeof line,
        "mode=%s placement=%s workers=%d coreBase=%d(%s) idle=%s io=%s ioWq=%s term=%d ctlTerm=%d fsyncMs{worker=%lld follower=%lld} sparse=%s mtu=%d socketBuffer=%d "
        "rcvWindow=%d gso=%s gro=%s rings{replOut=%d ingress=%d return=%d} ports{ingress=%d repl=%d egressCtl=%d} "
        "host{loopback=%s ifMtu=%d rmemMax=%lld wmemMax=%lld cpus=%d aeronTmpfs=%s} derived={%s}",
        cfg.shardMode() ? "shard" : "legacy", cfg.fused() ? "fused" : "thread", W, cfg.workerCoreBase,
        cfg.shardMode() ? (cfg.shardWorkersBusySpin() ? "busy-spin" : "backoff") : "idle-cfg", idle.c_str(), io.c_str(), ioWq.c_str(),
        cfg.aeronTermBufferBytes, cfg.aeronControlTermBufferBytes,
        static_cast<long long>(cfg.workerSyncIntervalMs), static_cast<long long>(cfg.followerFlushIntervalMs),
        cfg.aeronTermBufferSparse.value_or(false) ? "true" : "false", cfg.aeronMtuBytes,
        cfg.aeronSocketBufferBytes, cfg.aeronRcvWindowBytes,
        // What every driver installs (rune_gso_bindings_create uses the same probe); the drivers' own WARN
        // start line repeats the installed state and their close line any runtime fallback.
        cfg.aeronUdpGso ? (facts.udpGso ? "on" : "unsupported") : "off",
        cfg.aeronUdpGso ? (facts.udpGro ? "on" : "unsupported") : "off", cfg.replicationOutboundRingBytes,
        cfg.workerIngressRingBytes, cfg.workerReturnRingBytes, cfg.workerIngressBasePort.value_or(0),
        cfg.workerReplBasePort.value_or(0), cfg.effectiveWorkerEgressControlBasePort(),
        facts.loopback ? "true" : "false", facts.interfaceMtu, static_cast<long long>(facts.rmemMax),
        static_cast<long long>(facts.wmemMax), static_cast<int>(facts.allowedCpus.size()),
        facts.aeronDirTmpfs ? "true" : "false", derived.c_str());
    out.summary = line;
    return out;
}

// ---------------------------------------------------------------------------
// Probes (IMPURE, never throw)
// ---------------------------------------------------------------------------
namespace {

int64_t readProcLong(const char* path) {
    std::ifstream in(path);
    long long v = 0;
    if (in >> v) return v;
    return 0;
}

/// The largest SO_* value the OS grants (setsockopt + read back), bounded by `want`.
int64_t probeSockCap(int opt, int64_t want) {
    const int fd = ::socket(AF_INET, SOCK_DGRAM, 0);
    if (fd < 0) return 0;
    int64_t got = 0;
    for (int64_t w = want; w >= 64 * 1024 && got == 0; w /= 2) {
        int v = static_cast<int>(w);
        if (::setsockopt(fd, SOL_SOCKET, opt, &v, sizeof v) == 0) {
            int r = 0;
            socklen_t len = sizeof r;
            if (::getsockopt(fd, SOL_SOCKET, opt, &r, &len) == 0) got = r;
        }
    }
    ::close(fd);
    return got;
}

int64_t probeSockDefault(int opt) {
    const int fd = ::socket(AF_INET, SOCK_DGRAM, 0);
    if (fd < 0) return 0;
    int r = 0;
    socklen_t len = sizeof r;
    const int64_t v = ::getsockopt(fd, SOL_SOCKET, opt, &r, &len) == 0 ? r : 0;
    ::close(fd);
    return v;
}

/// MTU of the interface the kernel routes `host` through (0 = unknown).
int32_t egressInterfaceMtu(const std::string& host, bool& ipv6) {
    addrinfo hints{};
    hints.ai_socktype = SOCK_DGRAM;
    hints.ai_family   = AF_UNSPEC;
    addrinfo* res = nullptr;
    const std::string h(stripBrackets(host));
    if (::getaddrinfo(h.c_str(), "9", &hints, &res) != 0 || res == nullptr) return 0;
    int32_t mtu = 0;
    const int family = res->ai_family;
    ipv6 = family == AF_INET6;
    const int fd = ::socket(family, SOCK_DGRAM, 0);
    sockaddr_storage local{};
    socklen_t llen = sizeof local;
    const bool ok = fd >= 0 && ::connect(fd, res->ai_addr, res->ai_addrlen) == 0 &&
                    ::getsockname(fd, reinterpret_cast<sockaddr*>(&local), &llen) == 0;
    if (fd >= 0) ::close(fd);
    ::freeaddrinfo(res);
    if (!ok) return 0;

    ifaddrs* ifs = nullptr;
    if (::getifaddrs(&ifs) != 0) return 0;
    std::string ifname;
    for (ifaddrs* i = ifs; i != nullptr && ifname.empty(); i = i->ifa_next) {
        if (i->ifa_addr == nullptr || i->ifa_addr->sa_family != family) continue;
        if (family == AF_INET) {
            const auto* a = reinterpret_cast<const sockaddr_in*>(i->ifa_addr);
            const auto* b = reinterpret_cast<const sockaddr_in*>(&local);
            if (a->sin_addr.s_addr == b->sin_addr.s_addr) ifname = i->ifa_name;
        } else {
            const auto* a = reinterpret_cast<const sockaddr_in6*>(i->ifa_addr);
            const auto* b = reinterpret_cast<const sockaddr_in6*>(&local);
            if (std::memcmp(&a->sin6_addr, &b->sin6_addr, sizeof(in6_addr)) == 0) ifname = i->ifa_name;
        }
    }
    ::freeifaddrs(ifs);
    if (ifname.empty()) return 0;
    const int q = ::socket(AF_INET, SOCK_DGRAM, 0);
    if (q < 0) return 0;
    ifreq ifr{};
    std::strncpy(ifr.ifr_name, ifname.c_str(), sizeof(ifr.ifr_name) - 1);
    if (::ioctl(q, SIOCGIFMTU, &ifr) == 0) mtu = ifr.ifr_mtu;
    ::close(q);
    return mtu;
}

std::vector<int32_t> allowedCpus() {
    std::vector<int32_t> cpus;
#if defined(__linux__)
    cpu_set_t set;
    CPU_ZERO(&set);
    if (::sched_getaffinity(0, sizeof set, &set) == 0)
        for (int c = 0; c < CPU_SETSIZE; ++c)
            if (CPU_ISSET(c, &set)) cpus.push_back(c);
#endif
    return cpus;   // empty elsewhere: never auto-pin
}

/// The nearest existing ancestor of `path` is on tmpfs (Linux statfs).
bool onTmpfs(const std::string& path) {
#if defined(__linux__)
    std::error_code ec;
    std::filesystem::path p = std::filesystem::absolute(path, ec);
    while (!p.empty() && !std::filesystem::exists(p, ec)) {
        if (p == p.parent_path()) break;
        p = p.parent_path();
    }
    struct statfs st{};
    if (::statfs(p.c_str(), &st) != 0) return false;
    return static_cast<unsigned long>(st.f_type) == 0x01021994UL;   // TMPFS_MAGIC
#else
    (void)path;
    return false;   // no tmpfs Aeron dir on macOS (disk-backed): sparse terms
#endif
}

} // namespace

HostNetFacts probeHostNetFacts(const std::vector<std::string>& endpointHosts, const std::string& aeronDir) {
    HostNetFacts f;
    f.platform = currentHostPlatform();
    f.loopback = true;
    for (const auto& h : endpointHosts) f.loopback = f.loopback && isLoopbackHost(h);
    if (!f.loopback) {
        int32_t minMtu = 0;
        for (const auto& h : endpointHosts) {
            if (isLoopbackHost(h)) continue;
            bool v6 = false;
            // Wildcard: the interface the default route uses (no packet is sent).
            const int32_t m = egressInterfaceMtu(isWildcardHost(h) ? std::string("1.1.1.1") : h, v6);
            f.ipv6 = f.ipv6 || v6;
            if (m > 0 && (minMtu == 0 || m < minMtu)) minMtu = m;
        }
        f.interfaceMtu = minMtu;
    }
    const int udp = rune_gso_platform_supported();   // 0 off Linux
    f.udpGso = (udp & 1) != 0;
    f.udpGro = (udp & 2) != 0;
    if (f.platform == HostPlatform::Linux) {
        f.rmemMax = readProcLong("/proc/sys/net/core/rmem_max");
        f.wmemMax = readProcLong("/proc/sys/net/core/wmem_max");
    }
    // Fallback probe: Linux reports back DOUBLE the granted size.
    const int64_t div = f.platform == HostPlatform::Linux ? 2 : 1;
    if (f.rmemMax <= 0) f.rmemMax = probeSockCap(SO_RCVBUF, 64LL * 1024 * 1024) / div;
    if (f.wmemMax <= 0) f.wmemMax = probeSockCap(SO_SNDBUF, 64LL * 1024 * 1024) / div;
    f.allowedCpus   = allowedCpus();
    f.aeronDirTmpfs = !aeronDir.empty() && onTmpfs(aeronDir);
    f.defaultSndBuf = probeSockDefault(SO_SNDBUF);
    f.defaultRcvBuf = probeSockDefault(SO_RCVBUF);
    return f;
}


} // namespace rune::node
