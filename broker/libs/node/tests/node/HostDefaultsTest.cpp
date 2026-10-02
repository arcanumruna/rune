// HostDefaultsTest.cpp — the findings §4 config as host-derived DEFAULTS
// (HostDefaults.h).  Pure functions: every case is a plain facts → value check.
#include <rune/node/HostDefaults.h>
#include <rune/node/IoBackend.h>
#include <rune/node/NodeConfig.h>

#include <gtest/gtest.h>

#include <map>
#include <set>
#include <string>
#include <system_error>

using namespace rune::node;

namespace {
constexpr int32_t MiB = 1024 * 1024;

HostNetFacts linuxFacts(bool loopback, int32_t ifMtu, int64_t rmem, int64_t wmem) {
    HostNetFacts f;
    f.platform = HostPlatform::Linux;
    f.loopback = loopback;
    f.interfaceMtu = ifMtu;
    f.rmemMax = rmem;
    f.wmemMax = wmem;
    f.defaultSndBuf = 212992;
    f.defaultRcvBuf = 212992;
    return f;
}
HostNetFacts macFacts(int64_t cap) {
    HostNetFacts f;
    f.platform = HostPlatform::MacOS;
    f.loopback = true;
    f.rmemMax = cap;
    f.wmemMax = cap;
    f.defaultSndBuf = 9216;     // net.inet.udp.maxdgram — the old EINVAL source
    f.defaultRcvBuf = 786896;
    return f;
}
AeronTransportRequest autoReq(int32_t term = 16 * MiB) {
    AeronTransportRequest r;
    r.termBufferBytes = term;
    return r;
}
bool isPow2(int64_t v) { return v > 0 && (v & (v - 1)) == 0; }

/// The invariants aeron_driver_init + the receive endpoint enforce (never EINVAL).
void expectDriverValid(const AeronTransportDerived& d, int64_t sndCap, int64_t rcvCap, const std::string& ctx) {
    EXPECT_EQ(d.mtuBytes % 32, 0) << ctx;
    EXPECT_GT(d.mtuBytes, 32) << ctx;
    EXPECT_LE(d.mtuBytes, 65504) << ctx;
    EXPECT_LE(d.mtuBytes, sndCap) << ctx << " mtu > SO_SNDBUF → aeron_driver_init EINVAL";
    EXPECT_LE(d.mtuBytes, d.rcvWindowBytes) << ctx << " mtu > window → receive endpoint EINVAL";
    EXPECT_LE(d.rcvWindowBytes, rcvCap) << ctx << " window > SO_RCVBUF → aeron_driver_init EINVAL";
}
} // namespace

// ---------------------------------------------------------------------------
// deriveAeronTransport — the named host shapes
// ---------------------------------------------------------------------------
TEST(HostDefaults, LoopbackLinuxWithTunedSysctlsIsTheFindingsConfig) {
    const auto d = deriveAeronTransport(linuxFacts(true, 0, 16 * MiB, 16 * MiB), autoReq());
    EXPECT_EQ(d.mtuBytes, 65504);
    EXPECT_EQ(d.socketBufferBytes, 16 * MiB);
    EXPECT_EQ(d.rcvWindowBytes, 4 * MiB);
    EXPECT_FALSE(d.socketBufferCapped);
    EXPECT_TRUE(d.warnings.empty()) << d.warnings;
    expectDriverValid(d, 16 * MiB, 16 * MiB, "loopback linux");
}

TEST(HostDefaults, Nic1500UsesInterfaceMtuMinusIpUdpHeaders) {
    const auto d = deriveAeronTransport(linuxFacts(false, 1500, 16 * MiB, 16 * MiB), autoReq());
    EXPECT_EQ(d.mtuBytes, 1472);            // 1500 - 20 - 8 = 1472 (already 32-aligned)
    EXPECT_EQ(d.socketBufferBytes, 16 * MiB);
    EXPECT_EQ(d.rcvWindowBytes, 4 * MiB);
    expectDriverValid(d, 16 * MiB, 16 * MiB, "1500 NIC");
}

TEST(HostDefaults, Nic1500Ipv6UsesTheLargerHeader) {
    auto f = linuxFacts(false, 1500, 16 * MiB, 16 * MiB);
    f.ipv6 = true;
    EXPECT_EQ(deriveAeronTransport(f, autoReq()).mtuBytes, 1440);   // 1500 - 48 = 1452 → 1440
}

TEST(HostDefaults, Jumbo9001NicAlignsDownTo32) {
    const auto d = deriveAeronTransport(linuxFacts(false, 9001, 16 * MiB, 16 * MiB), autoReq());
    EXPECT_EQ(d.mtuBytes, 8960);            // 9001 - 28 = 8973 → 8960
    expectDriverValid(d, 16 * MiB, 16 * MiB, "jumbo");
}

TEST(HostDefaults, UnknownInterfaceMtuFallsBackToAeronDefault) {
    EXPECT_EQ(deriveAeronTransport(linuxFacts(false, 0, 16 * MiB, 16 * MiB), autoReq()).mtuBytes, 1408);
}

TEST(HostDefaults, TinyRmemMaxCapsBufferWarnsWithExactSysctlsAndStaysValid) {
    // Stock Ubuntu: rmem_max = wmem_max = 212992.
    const auto d = deriveAeronTransport(linuxFacts(true, 0, 212992, 212992), autoReq());
    EXPECT_EQ(d.socketBufferBytes, 212992);
    EXPECT_TRUE(d.socketBufferCapped);
    EXPECT_EQ(d.rcvWindowBytes, 32768);     // pow2floor(min(4 MiB, 212992/4 = 53248, 8 MiB))
    EXPECT_EQ(d.mtuBytes, 8192);            // <= window/4
    for (const char* line : {"net.core.rmem_max=16777216", "net.core.wmem_max=16777216",
                             "net.core.rmem_default=16777216", "net.core.wmem_default=16777216",
                             "net.core.netdev_max_backlog=100000"})
        EXPECT_NE(d.warnings.find(line), std::string::npos) << "WARN must name " << line << ": " << d.warnings;
    expectDriverValid(d, 212992, 212992, "tiny rmem");
}

TEST(HostDefaults, AsymmetricLimitsUseTheSmaller) {
    const auto d = deriveAeronTransport(linuxFacts(true, 0, 16 * MiB, 1 * MiB), autoReq());
    EXPECT_EQ(d.socketBufferBytes, 1 * MiB);
    EXPECT_TRUE(d.socketBufferCapped);
    EXPECT_EQ(d.rcvWindowBytes, 256 * 1024);
    expectDriverValid(d, 1 * MiB, 1 * MiB, "asymmetric");
}

TEST(HostDefaults, MacOsEightMiBCapKeepsJumboMtuBelowSoSndbuf) {
    const auto d = deriveAeronTransport(macFacts(8 * MiB), autoReq());
    EXPECT_EQ(d.socketBufferBytes, 8 * MiB);
    EXPECT_TRUE(d.socketBufferCapped);
    EXPECT_NE(d.warnings.find("kern.ipc.maxsockbuf"), std::string::npos) << d.warnings;
    EXPECT_EQ(d.rcvWindowBytes, 2 * MiB);   // buffer / 4
    EXPECT_EQ(d.mtuBytes, 65504);
    expectDriverValid(d, 8 * MiB, 8 * MiB, "macOS 8 MiB");
}

TEST(HostDefaults, MacOs64KiBCapNeverYieldsMtuAboveSoSndbuf) {
    // The EINVAL lesson: a ~64 KiB SO_SNDBUF cap with a 65504 MTU failed
    // aeron_driver_init.  Derivation must stay below it.
    const auto d = deriveAeronTransport(macFacts(64 * 1024), autoReq());
    EXPECT_EQ(d.socketBufferBytes, 64 * 1024);
    EXPECT_EQ(d.rcvWindowBytes, 16 * 1024);
    EXPECT_EQ(d.mtuBytes, 4096);
    expectDriverValid(d, 64 * 1024, 64 * 1024, "macOS 64 KiB");
}

TEST(HostDefaults, OsDefaultSocketBufferOnMacClampsMtuToTheDefaultSndbuf) {
    // Explicit RUNE_AERON_SOCKET_BUFFER_BYTES=0 (no setsockopt): the driver
    // validates against macOS's default 9216 SO_SNDBUF.
    AeronTransportRequest r = autoReq();
    r.socketBufferBytes = 0;
    const auto d = deriveAeronTransport(macFacts(8 * MiB), r);
    EXPECT_EQ(d.socketBufferBytes, 0);
    EXPECT_LE(d.mtuBytes, 9216);
    expectDriverValid(d, 9216, 786896, "mac OS-default buffer");
    r.mtuBytes = 65504;                                       // explicit jumbo + OS buffer
    const auto e = deriveAeronTransport(macFacts(8 * MiB), r);
    EXPECT_EQ(e.mtuBytes, 9216);
    EXPECT_NE(e.warnings.find("explicit MTU 65504"), std::string::npos) << e.warnings;
    expectDriverValid(e, 9216, 786896, "mac explicit jumbo + OS buffer");
}

TEST(HostDefaults, SmallTermBoundsTheWindowAndTheMtu) {
    const auto d = deriveAeronTransport(linuxFacts(true, 0, 16 * MiB, 16 * MiB), autoReq(64 * 1024));
    EXPECT_EQ(d.rcvWindowBytes, 32 * 1024);   // term / 2
    EXPECT_EQ(d.mtuBytes, 8192);              // window / 4
    expectDriverValid(d, 16 * MiB, 16 * MiB, "64 KiB term");
}

TEST(HostDefaults, UnknownOsLimitsAssumeTheTarget) {
    const auto d = deriveAeronTransport(linuxFacts(true, 0, 0, 0), autoReq());
    EXPECT_EQ(d.socketBufferBytes, 16 * MiB);
    EXPECT_FALSE(d.socketBufferCapped);
}

// ---------------------------------------------------------------------------
// Explicit overrides win (and are only clamped where the driver would EINVAL)
// ---------------------------------------------------------------------------
TEST(HostDefaults, ExplicitValuesWinUnchangedWhenValid) {
    AeronTransportRequest r = autoReq();
    r.mtuBytes = 8192;
    r.socketBufferBytes = 2 * MiB;
    r.rcvWindowBytes = 128 * 1024;
    const auto d = deriveAeronTransport(linuxFacts(true, 0, 16 * MiB, 16 * MiB), r);
    EXPECT_EQ(d.mtuBytes, 8192);
    EXPECT_EQ(d.socketBufferBytes, 2 * MiB);
    EXPECT_EQ(d.rcvWindowBytes, 128 * 1024);
    EXPECT_TRUE(d.warnings.empty()) << d.warnings;
}

TEST(HostDefaults, ExplicitMtuOnlyAndTheRestDerived) {
    AeronTransportRequest r = autoReq();
    r.mtuBytes = 1408;
    const auto d = deriveAeronTransport(linuxFacts(true, 0, 16 * MiB, 16 * MiB), r);
    EXPECT_EQ(d.mtuBytes, 1408);
    EXPECT_EQ(d.socketBufferBytes, 16 * MiB);
    EXPECT_EQ(d.rcvWindowBytes, 4 * MiB);
}

TEST(HostDefaults, ExplicitWindowAboveTheSocketBufferIsClampedLoudly) {
    AeronTransportRequest r = autoReq();
    r.rcvWindowBytes = 16 * MiB;   // the 98 %-drop config, and > an 8 MiB cap
    const auto d = deriveAeronTransport(macFacts(8 * MiB), r);
    EXPECT_EQ(d.rcvWindowBytes, 8 * MiB);
    EXPECT_NE(d.warnings.find("explicit receive window"), std::string::npos) << d.warnings;
    expectDriverValid(d, 8 * MiB, 8 * MiB, "explicit window");
}

TEST(HostDefaults, ExplicitUnalignedMtuIsAlignedDownLoudly) {
    AeronTransportRequest r = autoReq();
    r.mtuBytes = 1500;
    const auto d = deriveAeronTransport(linuxFacts(true, 0, 16 * MiB, 16 * MiB), r);
    EXPECT_EQ(d.mtuBytes, 1472);
    EXPECT_FALSE(d.warnings.empty());
}

TEST(HostDefaults, ExplicitSocketBufferAboveTheSysctlWarns) {
    AeronTransportRequest r = autoReq();
    r.socketBufferBytes = 16 * MiB;
    const auto d = deriveAeronTransport(linuxFacts(true, 0, 212992, 212992), r);
    EXPECT_EQ(d.socketBufferBytes, 16 * MiB);   // kept (the kernel caps it)
    EXPECT_NE(d.warnings.find("net.core.rmem_max=16777216"), std::string::npos) << d.warnings;
    expectDriverValid(d, 212992, 212992, "explicit buffer > sysctl");
}

// Exhaustive sweep: whatever the host and whatever is explicit, the driver
// constraints hold and derived values follow the rules.
TEST(HostDefaults, SweepNeverProducesAnInvalidDriverConfig) {
    const int64_t caps[]  = {0, 9216, 64 * 1024, 212992, 1 * MiB, 8 * MiB, 16 * MiB, 64 * MiB};
    const int32_t mtus[]  = {0, 576, 1500, 9001, 65535};
    const int32_t terms[] = {64 * 1024, 1 * MiB, 4 * MiB, 16 * MiB, 64 * MiB};
    const int32_t exMtu[] = {kAeronAuto, 1408, 4000, 65504};
    const int32_t exBuf[] = {kAeronAuto, 0, 128 * 1024, 16 * MiB};
    const int32_t exWin[] = {kAeronAuto, 64 * 1024, 4 * MiB, 32 * MiB};
    int cases = 0;
    for (auto plat : {HostPlatform::Linux, HostPlatform::MacOS})
    for (bool lo : {true, false})
    for (int64_t cap : caps)
    for (int32_t ifm : mtus)
    for (int32_t term : terms)
    for (int32_t m : exMtu)
    for (int32_t b : exBuf)
    for (int32_t w : exWin) {
        HostNetFacts f = plat == HostPlatform::Linux ? linuxFacts(lo, ifm, cap, cap) : macFacts(cap);
        f.loopback = lo;
        f.interfaceMtu = ifm;
        AeronTransportRequest r;
        r.termBufferBytes = term; r.mtuBytes = m; r.socketBufferBytes = b; r.rcvWindowBytes = w;
        const auto d = deriveAeronTransport(f, r);
        const int64_t osMax = cap > 0 ? cap : 16 * MiB;
        const int64_t sndCap = b == kAeronAuto ? std::min<int64_t>(16 * MiB, osMax)
                             : b == 0 ? f.defaultSndBuf : std::min<int64_t>(b, osMax);
        const int64_t rcvCap = b == kAeronAuto ? std::min<int64_t>(16 * MiB, osMax)
                             : b == 0 ? f.defaultRcvBuf : std::min<int64_t>(b, osMax);
        const std::string ctx = "cap=" + std::to_string(cap) + " ifm=" + std::to_string(ifm) +
                                " term=" + std::to_string(term) + " m=" + std::to_string(m) +
                                " b=" + std::to_string(b) + " w=" + std::to_string(w) + " lo=" + std::to_string(lo);
        expectDriverValid(d, sndCap, rcvCap, ctx);
        if (b == kAeronAuto) EXPECT_LE(d.socketBufferBytes, 16 * MiB) << ctx;
        if (w == kAeronAuto) {
            EXPECT_TRUE(isPow2(d.rcvWindowBytes)) << ctx;
            EXPECT_LE(d.rcvWindowBytes, 4 * MiB) << ctx;
            EXPECT_LE(d.rcvWindowBytes, std::max<int64_t>(term / 2, 64)) << ctx;
            if (b == kAeronAuto) EXPECT_LE(d.rcvWindowBytes, std::max<int64_t>(d.socketBufferBytes / 4, 64)) << ctx;
        }
        if (m == kAeronAuto && w == kAeronAuto) EXPECT_LE(d.mtuBytes, std::max(d.rcvWindowBytes / 4, 64)) << ctx;
        if (::testing::Test::HasFailure()) return;
        ++cases;
    }
    EXPECT_EQ(cases, 2 * 2 * 8 * 5 * 5 * 4 * 4 * 4);
}

// ---------------------------------------------------------------------------
// Hosts
// ---------------------------------------------------------------------------
TEST(HostDefaults, LoopbackAndWildcardHosts) {
    for (const char* h : {"127.0.0.1", "127.1.2.3", "localhost", "::1", "[::1]"}) EXPECT_TRUE(isLoopbackHost(h)) << h;
    for (const char* h : {"10.0.0.1", "178.104.16.102", "0.0.0.0", "::", "", "example.com"}) EXPECT_FALSE(isLoopbackHost(h)) << h;
    for (const char* h : {"0.0.0.0", "::", "", "*"}) EXPECT_TRUE(isWildcardHost(h)) << h;
    EXPECT_FALSE(isWildcardHost("127.0.0.1"));
}

TEST(HostDefaults, EndpointHostsFollowPeersForAWildcardClientBind) {
    NodeConfig c;
    c.nodeId = 0;
    PeerConfig p0; p0.id = 0; p0.host = "127.0.0.1";
    PeerConfig p1; p1.id = 1; p1.host = "127.0.0.1";
    c.peers = {p0, p1};
    c.clientEndpoint.channel = "aeron:udp?endpoint=0.0.0.0:40200";
    EXPECT_EQ(nodeEndpointHosts(c), std::vector<std::string>{"127.0.0.1"});
    c.clientEndpoint.channel = "aeron:udp?endpoint=10.0.0.5:40200";
    EXPECT_EQ(nodeEndpointHosts(c), (std::vector<std::string>{"127.0.0.1", "10.0.0.5"}));
}

TEST(HostDefaults, ProbeOfLoopbackReportsLoopbackAndRealLimits) {
    const auto f = probeHostNetFacts({"127.0.0.1"});
    EXPECT_TRUE(f.loopback);
    EXPECT_EQ(f.platform, currentHostPlatform());
    EXPECT_GT(f.rmemMax, 0);
    EXPECT_GT(f.wmemMax, 0);
    EXPECT_GT(f.defaultSndBuf, 0);
    // And the derived transport from REAL facts is a valid driver config.
    const auto d = deriveAeronTransport(f, AeronTransportRequest{});
    expectDriverValid(d, std::min<int64_t>(16 * MiB, std::min(f.rmemMax, f.wmemMax)),
                      std::min<int64_t>(16 * MiB, std::min(f.rmemMax, f.wmemMax)), "probed host");
}

// ---------------------------------------------------------------------------
// Shard ports + affinity
// ---------------------------------------------------------------------------
TEST(HostDefaults, DerivedShardPortsAreDisjointAcrossCoLocatedNodes) {
    const auto p0 = deriveShardBasePorts(0, 1);
    ASSERT_TRUE(p0);
    EXPECT_EQ(p0->ingress, 41000);
    EXPECT_EQ(p0->repl, 41064);
    EXPECT_EQ(p0->egressControl, 41128);
    std::set<int32_t> used;
    for (int32_t n = 0; n <= 126; ++n) {
        const auto p = deriveShardBasePorts(n, 64);
        ASSERT_TRUE(p) << n;
        for (int32_t w = 0; w < 64; ++w)
            for (int32_t port : {p->ingress + w, p->repl + w, p->egressControl + w}) {
                EXPECT_LE(port, 65535);
                EXPECT_TRUE(used.insert(port).second) << "node " << n << " port " << port << " collides";
            }
    }
    EXPECT_FALSE(deriveShardBasePorts(127, 1)) << "block would pass 65535";
    EXPECT_FALSE(deriveShardBasePorts(-1, 1));
    EXPECT_FALSE(deriveShardBasePorts(0, 65)) << "> kMaxShardWorkers";
}

TEST(HostDefaults, CoreBaseIsTheBenchLayoutOnLinuxOnly) {
    // 3 co-located nodes x 1 worker on 4 cores → cores 1, 2, 3 (cluster.sh RUNE_SHARD=1).
    for (int32_t i = 0; i < 3; ++i)
        EXPECT_EQ(deriveWorkerCoreBase(HostPlatform::Linux, true, i, 3, 1, 4), 1 + i);
    EXPECT_EQ(deriveWorkerCoreBase(HostPlatform::Linux, true, 1, 3, 2, 8), 3);
    EXPECT_EQ(deriveWorkerCoreBase(HostPlatform::Linux, true, 0, 3, 1, 3), -1) << "does not fit";
    EXPECT_EQ(deriveWorkerCoreBase(HostPlatform::Linux, true, 0, 3, 2, 6), -1) << "does not fit";
    EXPECT_EQ(deriveWorkerCoreBase(HostPlatform::Linux, false, 2, 3, 2, 4), 1) << "one node per host";
    EXPECT_EQ(deriveWorkerCoreBase(HostPlatform::Linux, false, 0, 3, 4, 4), -1);
    EXPECT_EQ(deriveWorkerCoreBase(HostPlatform::MacOS, true, 0, 3, 1, 16), -1) << "no affinity on macOS";
    EXPECT_EQ(deriveWorkerCoreBase(HostPlatform::Other, false, 0, 1, 1, 16), -1);
}

// ---------------------------------------------------------------------------
// applyHostDerivedDefaults
// ---------------------------------------------------------------------------
namespace {
NodeConfig threeNodeDefault(int32_t self) {
    NodeConfig c;
    c.nodeId = self;
    for (int i = 0; i < 3; ++i) {
        PeerConfig p;
        p.id = i; p.host = "127.0.0.1";
        p.consensusPort = static_cast<uint16_t>(40110 + i);
        p.clientPort = static_cast<uint16_t>(40200 + i);
        p.replicationPort = static_cast<uint16_t>(40220 + i);
        c.peers.push_back(p);
    }
    c.clientEndpoint.channel = "aeron:udp?endpoint=0.0.0.0:" + std::to_string(40200 + self);
    return c;
}
} // namespace

TEST(HostDefaults, DefaultConfigIsShardFusedWithTheFindingsSizes) {
    const NodeConfig c;
    EXPECT_TRUE(c.shardMode());
    EXPECT_TRUE(c.fused());
    EXPECT_EQ(c.workerCount, 1);
    EXPECT_EQ(c.aeronTermBufferBytes, 16 * MiB);
    EXPECT_FALSE(c.aeronTermBufferSparse);
    EXPECT_EQ(c.replicationOutboundRingBytes, 32 * MiB);
    EXPECT_EQ(c.workerIngressRingBytes, 32 * MiB);
    EXPECT_EQ(c.workerReturnRingBytes, 32 * MiB);
    EXPECT_EQ(c.aeronMtuBytes, NodeConfig::kAeronAuto);
    EXPECT_EQ(c.aeronSocketBufferBytes, NodeConfig::kAeronAuto);
    EXPECT_EQ(c.aeronRcvWindowBytes, NodeConfig::kAeronAuto);
    EXPECT_EQ(c.workerCoreBase, NodeConfig::kWorkerCoreBaseAuto);
}

TEST(HostDefaults, ApplyOnTheK3sBoxGivesExactlyTheTunedConfig) {
    // k3s-node-01: Linux, 4 cores, sysctls at 16 MiB, 3 co-located nodes.
    for (int32_t self = 0; self < 3; ++self) {
        NodeConfig c = threeNodeDefault(self);
        const auto hd = applyHostDerivedDefaults(c, linuxFacts(true, 0, 16 * MiB, 16 * MiB), 4);
        EXPECT_EQ(c.aeronMtuBytes, 65504);
        EXPECT_EQ(c.aeronSocketBufferBytes, 16 * MiB);
        EXPECT_EQ(c.aeronRcvWindowBytes, 4 * MiB);
        EXPECT_EQ(c.workerCoreBase, 1 + self);
        EXPECT_TRUE(c.perWorkerIngressEnabled && c.perWorkerReplication && c.perWorkerEgressEnabled);
        const auto p = deriveShardBasePorts(self, 1);
        EXPECT_EQ(c.workerIngressBasePort, p->ingress);
        EXPECT_EQ(c.workerReplBasePort, p->repl);
        EXPECT_EQ(c.workerEgressControlBasePort, p->egressControl);
        for (const auto& peer : c.peers) {
            const auto pp = deriveShardBasePorts(peer.id, 1);
            EXPECT_EQ(peer.ingressBasePort, pp->ingress);
            EXPECT_EQ(peer.replBasePort, pp->repl);
        }
        EXPECT_FALSE(c.validateShardConfig().has_value()) << *c.validateShardConfig();
        EXPECT_TRUE(hd.warnings.empty()) << hd.warnings;
        EXPECT_NE(hd.summary.find("mode=shard placement=fused workers=1"), std::string::npos) << hd.summary;
        EXPECT_NE(hd.summary.find("mtu=65504 socketBuffer=16777216 rcvWindow=4194304"), std::string::npos) << hd.summary;
    }
}

TEST(HostDefaults, ApplyKeepsEveryExplicitValueAndIsIdempotent) {
    NodeConfig c = threeNodeDefault(1);
    c.aeronMtuBytes = 8192;
    c.aeronSocketBufferBytes = 4 * MiB;
    c.aeronRcvWindowBytes = 1 * MiB;
    c.workerCoreBase = -1;
    c.workerIngressBasePort = 40520;
    c.workerReplBasePort = 40620;
    c.workerEgressControlBasePort = 40420;
    for (auto& p : c.peers) {
        p.ingressBasePort = static_cast<uint16_t>(40500 + 20 * p.id);
        p.replBasePort = static_cast<uint16_t>(40600 + 20 * p.id);
    }
    const NodeConfig before = c;
    applyHostDerivedDefaults(c, linuxFacts(true, 0, 16 * MiB, 16 * MiB), 4);
    EXPECT_EQ(c.aeronMtuBytes, 8192);
    EXPECT_EQ(c.aeronSocketBufferBytes, 4 * MiB);
    EXPECT_EQ(c.aeronRcvWindowBytes, 1 * MiB);
    EXPECT_EQ(c.workerCoreBase, -1);
    EXPECT_EQ(c.workerIngressBasePort, 40520);
    EXPECT_EQ(c.workerReplBasePort, 40620);
    EXPECT_EQ(c.workerEgressControlBasePort, 40420);
    for (std::size_t i = 0; i < c.peers.size(); ++i) {
        EXPECT_EQ(c.peers[i].ingressBasePort, before.peers[i].ingressBasePort);
        EXPECT_EQ(c.peers[i].replBasePort, before.peers[i].replBasePort);
    }
    EXPECT_FALSE(c.validateShardConfig().has_value()) << *c.validateShardConfig();
    // Idempotent: a second apply changes nothing.
    NodeConfig again = c;
    applyHostDerivedDefaults(again, linuxFacts(true, 0, 16 * MiB, 16 * MiB), 4);
    EXPECT_EQ(again.aeronMtuBytes, c.aeronMtuBytes);
    EXPECT_EQ(again.aeronRcvWindowBytes, c.aeronRcvWindowBytes);
    EXPECT_EQ(again.workerIngressBasePort, c.workerIngressBasePort);
    EXPECT_EQ(again.workerCoreBase, c.workerCoreBase);
}

TEST(HostDefaults, SelfPortsComeFromItsAdvertisedPeerEntry) {
    // RUNE_PEERS advertises replBase/ingressBase but the RUNE_WORKER_*_BASE_PORT
    // vars are unset: self takes the advertised ones (validateShardConfig agrees).
    NodeConfig c = threeNodeDefault(2);
    for (auto& p : c.peers) {
        p.ingressBasePort = static_cast<uint16_t>(40500 + 20 * p.id);
        p.replBasePort = static_cast<uint16_t>(40600 + 20 * p.id);
    }
    applyHostDerivedDefaults(c, linuxFacts(true, 0, 16 * MiB, 16 * MiB), 4);
    EXPECT_EQ(c.workerIngressBasePort, 40540);
    EXPECT_EQ(c.workerReplBasePort, 40640);
    EXPECT_FALSE(c.validateShardConfig().has_value()) << *c.validateShardConfig();
}

TEST(HostDefaults, LegacyModeIsUntouchedApartFromTransport) {
    NodeConfig c = threeNodeDefault(0);
    c.workerMode = NodeConfig::WorkerMode::Legacy;
    c.runtimePlacement = NodeConfig::RuntimePlacement::Thread;
    applyHostDerivedDefaults(c, linuxFacts(true, 0, 16 * MiB, 16 * MiB), 4);
    EXPECT_EQ(c.workerCoreBase, -1) << "legacy never auto-pins";
    EXPECT_FALSE(c.workerIngressBasePort.has_value());
    EXPECT_FALSE(c.perWorkerIngressEnabled);
    for (const auto& p : c.peers) EXPECT_EQ(p.ingressBasePort, 0);
    EXPECT_EQ(c.aeronMtuBytes, 65504);
}

TEST(HostDefaults, MacHostAppliesTheCappedBufferAndNoPinning) {
    NodeConfig c = threeNodeDefault(0);
    const auto hd = applyHostDerivedDefaults(c, macFacts(8 * MiB), 12);
    EXPECT_EQ(c.aeronSocketBufferBytes, 8 * MiB);
    EXPECT_EQ(c.aeronRcvWindowBytes, 2 * MiB);
    EXPECT_EQ(c.aeronMtuBytes, 65504);
    EXPECT_EQ(c.workerCoreBase, -1);
    EXPECT_TRUE(hd.socketBufferCapped);
    EXPECT_NE(hd.warnings.find("kern.ipc.maxsockbuf"), std::string::npos);
}

TEST(HostDefaults, EnvOverridesSurviveTheDerivation) {
    // parseNodeConfig → explicit values → apply keeps them.
    std::map<std::string, std::string> env = {
        {"RUNE_NODE_ID", "0"}, {"RUNE_PEERS", "0@127.0.0.1:40110:40200:40220"},
        {"RUNE_CLIENT_ENDPOINT", "127.0.0.1:40200"}, {"RUNE_DATA_DIR", "/tmp/hd"},
        {"RUNE_AERON_MTU", "1408"}, {"RUNE_AERON_RCV_WINDOW", "131072"},
        {"RUNE_AERON_SOCKET_BUFFER_BYTES", "2097152"}, {"RUNE_AERON_TERM_BUFFER_BYTES", "4194304"},
        {"RUNE_AERON_TERM_SPARSE", "true"}, {"RUNE_WORKER_MODE", "legacy"},
        {"RUNE_RUNTIME_PLACEMENT", "thread"}, {"RUNE_WORKER_CORE_BASE", "-1"}};
    auto r = parseNodeConfig([&](const char* k) -> const char* {
        auto it = env.find(k);
        return it == env.end() ? nullptr : it->second.c_str();
    });
    ASSERT_TRUE(r.config.has_value()) << r.error;
    NodeConfig c = *r.config;
    applyHostDerivedDefaults(c, linuxFacts(true, 0, 16 * MiB, 16 * MiB), 4);
    EXPECT_EQ(c.aeronMtuBytes, 1408);
    EXPECT_EQ(c.aeronRcvWindowBytes, 131072);
    EXPECT_EQ(c.aeronSocketBufferBytes, 2 * MiB);
    EXPECT_EQ(c.aeronTermBufferBytes, 4 * MiB);
    EXPECT_TRUE(c.aeronTermBufferSparse);
    EXPECT_FALSE(c.shardMode());
    EXPECT_FALSE(c.fused());
    EXPECT_EQ(c.workerCoreBase, -1);
}

TEST(HostDefaults, EnvWithNothingSetIsTheDerivedDefault) {
    std::map<std::string, std::string> env = {
        {"RUNE_NODE_ID", "1"},
        {"RUNE_PEERS", "0@127.0.0.1:40110:40200:40220,1@127.0.0.1:40111:40201:40221,2@127.0.0.1:40112:40202:40222"},
        {"RUNE_CLIENT_ENDPOINT", "0.0.0.0:40201"}, {"RUNE_DATA_DIR", "/tmp/hd"}};
    auto r = parseNodeConfig([&](const char* k) -> const char* {
        auto it = env.find(k);
        return it == env.end() ? nullptr : it->second.c_str();
    });
    ASSERT_TRUE(r.config.has_value()) << r.error;
    NodeConfig c = *r.config;
    EXPECT_TRUE(c.shardMode());
    EXPECT_TRUE(c.fused());
    applyHostDerivedDefaults(c, linuxFacts(true, 0, 16 * MiB, 16 * MiB), 4);
    EXPECT_EQ(c.workerCoreBase, 2);
    EXPECT_EQ(c.aeronMtuBytes, 65504);
    EXPECT_FALSE(c.validateShardConfig().has_value()) << *c.validateShardConfig();
}

// ---------------------------------------------------------------------------
// io_uring when available, automatic fallback
// ---------------------------------------------------------------------------
TEST(HostDefaults, AutoUringFallsBackToPortableWhenSetupFails) {
    std::string why;
    const auto k = resolveIoBackendKind({IoBackendKind::Uring, /*auto*/true},
        [] { throw std::system_error(ENOSYS, std::generic_category(), "io_uring_setup failed"); }, why);
    EXPECT_EQ(k, IoBackendKind::Portable);
    EXPECT_NE(why.find("io_uring_setup failed"), std::string::npos) << why;
}

TEST(HostDefaults, ExplicitUringStillFailsLoudly) {
    std::string why;
    EXPECT_THROW((void)resolveIoBackendKind({IoBackendKind::Uring, /*auto*/false},
        [] { throw std::system_error(ENOSYS, std::generic_category(), "io_uring_setup failed"); }, why),
        std::system_error);
}

TEST(HostDefaults, WorkingUringAndOtherKindsAreUnchanged) {
    std::string why;
    int calls = 0;
    EXPECT_EQ(resolveIoBackendKind({IoBackendKind::Uring, true}, [&] { ++calls; }, why), IoBackendKind::Uring);
    EXPECT_EQ(calls, 1);
    EXPECT_TRUE(why.empty());
    EXPECT_EQ(resolveIoBackendKind({IoBackendKind::Portable, false}, [&] { ++calls; }, why), IoBackendKind::Portable);
    EXPECT_EQ(resolveIoBackendKind({IoBackendKind::Invalid, false}, [&] { ++calls; }, why), IoBackendKind::Invalid);
    EXPECT_EQ(calls, 1) << "only a uring request tries uring";
}

TEST(HostDefaults, PlatformDefaultIoBackendIsAutoSelected) {
    // RUNE_IO_BACKEND unset in the test env → auto (uring on Linux, portable elsewhere).
    if (std::getenv(kIoBackendEnv) != nullptr) GTEST_SKIP() << "RUNE_IO_BACKEND set in the environment";
    const auto r = ioBackendRequestFromEnv();
    EXPECT_TRUE(r.autoSelected);
#if defined(__linux__)
    EXPECT_EQ(r.kind, IoBackendKind::Uring);
#else
    EXPECT_EQ(r.kind, IoBackendKind::Portable);
#endif
    IoBackend b(64, 64);
    EXPECT_TRUE(b.acceptsSubmissions()) << "the default backend always works (uring or its fallback)";
}
