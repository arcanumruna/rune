// HostDefaultsTest.cpp — the findings §4 config as host-derived DEFAULTS
// (HostDefaults.h).  Pure functions: every case is a plain facts → value check.
#include <rune/node/AeronChannel.h>
#include <rune/node/HostDefaults.h>
#include <rune/node/IoBackend.h>
#include <rune/node/NodeConfig.h>

#include <gtest/gtest.h>

#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <map>
#include <set>
#include <string>
#include <system_error>
#include <tuple>

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
void expectReceiverValid(const AeronTransportDerived& d, int64_t term, const std::string& ctx) {
    // Aeron's receiver window is min(window, term/2); the receive endpoint
    // refuses an image whose sender mtu exceeds it (review I5).
    EXPECT_LE(d.mtuBytes, std::min<int64_t>(d.rcvWindowBytes, term / 2)) << ctx;
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
    // Stock Ubuntu: rmem_max = wmem_max = 212992 (the kernel doubles: 425984 real).
    const auto d = deriveAeronTransport(linuxFacts(true, 0, 212992, 212992), autoReq());
    EXPECT_EQ(d.socketBufferBytes, 212992);
    EXPECT_TRUE(d.socketBufferCapped);
    EXPECT_EQ(d.rcvWindowBytes, 65536);     // pow2floor(min(4 MiB, 425984/4 = 106496, 8 MiB))
    EXPECT_EQ(d.mtuBytes, 16384);           // <= window/4
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
    EXPECT_EQ(d.rcvWindowBytes, 512 * 1024);   // (2 x 1 MiB kernel) / 4
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
    EXPECT_EQ(d.rcvWindowBytes, 16 * 1024);   // term / 4
    EXPECT_EQ(d.mtuBytes, 4096);              // window / 4
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

TEST(HostDefaults, ExplicitSocketBufferAboveTheOsLimitIsClampedLoudly) {
    AeronTransportRequest r = autoReq();
    r.socketBufferBytes = 16 * MiB;
    const auto d = deriveAeronTransport(linuxFacts(true, 0, 212992, 212992), r);
    EXPECT_EQ(d.socketBufferBytes, 212992);
    EXPECT_NE(d.warnings.find("net.core.rmem_max=16777216"), std::string::npos) << d.warnings;
    expectDriverValid(d, 212992, 212992, "explicit buffer > sysctl");
    // macOS REFUSES (not caps) a setsockopt above kern.ipc.maxsockbuf and keeps
    // the 9216 default → the clamp is what keeps aeron_driver_init valid (I4).
    const auto m = deriveAeronTransport(macFacts(8 * MiB), r);
    EXPECT_EQ(m.socketBufferBytes, 8 * MiB);
    EXPECT_NE(m.warnings.find("kern.ipc.maxsockbuf"), std::string::npos) << m.warnings;
}

TEST(HostDefaults, MtuNeverExceedsTheReceiversTermHalfWindow) {
    // Review I5: explicit 4 MiB window + 64 KiB term: Aeron's receiver window is
    // min(4 MiB, 32 KiB) — a 65504 MTU would be refused by every receiver.
    AeronTransportRequest r = autoReq(64 * 1024);
    r.rcvWindowBytes = 4 * MiB;
    const auto d = deriveAeronTransport(linuxFacts(true, 0, 16 * MiB, 16 * MiB), r);
    expectReceiverValid(d, 64 * 1024, "explicit window, small term");
    EXPECT_LE(d.mtuBytes, 8192);
    r.mtuBytes = 65504;   // explicit too: clamped loudly
    const auto e = deriveAeronTransport(linuxFacts(true, 0, 16 * MiB, 16 * MiB), r);
    expectReceiverValid(e, 64 * 1024, "explicit window + mtu, small term");
    EXPECT_FALSE(e.warnings.empty());
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
        expectReceiverValid(d, term, ctx);
        if (b == kAeronAuto) EXPECT_LE(d.socketBufferBytes, 16 * MiB) << ctx;
        if (w == kAeronAuto) {
            EXPECT_TRUE(isPow2(d.rcvWindowBytes)) << ctx;
            EXPECT_LE(d.rcvWindowBytes, 4 * MiB) << ctx;
            EXPECT_LE(d.rcvWindowBytes, std::max<int64_t>(term / 2, 64)) << ctx;
            const int64_t kernel = (plat == HostPlatform::Linux ? 2 : 1) * int64_t{d.socketBufferBytes};
            if (b == kAeronAuto) EXPECT_LE(d.rcvWindowBytes, std::max<int64_t>(kernel / 4, 64)) << ctx;
        }
        if (m == kAeronAuto && w == kAeronAuto)
            EXPECT_LE(d.mtuBytes, std::max<int64_t>(std::min<int64_t>(d.rcvWindowBytes, term / 2) / 4, 64)) << ctx;
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
    for (const char* h : {"10.0.0.1", "203.0.113.7", "0.0.0.0", "::", "", "example.com"}) EXPECT_FALSE(isLoopbackHost(h)) << h;
    for (const char* h : {"0.0.0.0", "::", "", "*"}) EXPECT_TRUE(isWildcardHost(h)) << h;
    EXPECT_FALSE(isWildcardHost("127.0.0.1"));
}

TEST(HostDefaults, EndpointHostsFollowThePeersForAWildcardClientBind) {
    NodeConfig c;
    c.nodeId = 0;
    PeerConfig p0; p0.id = 0; p0.host = "127.0.0.1";
    PeerConfig p1; p1.id = 1; p1.host = "127.0.0.1";
    c.peers = {p0, p1};
    c.clientEndpoint.channel = "aeron:udp?endpoint=127.0.0.1:40200";
    EXPECT_EQ(nodeEndpointHosts(c), std::vector<std::string>{"127.0.0.1"});
    // A wildcard bind is not where datagrams go: the peers (and the advertised
    // client endpoint = the peer host) decide (k3s regression, see below).
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
    EXPECT_EQ(p0->ingress, 8200);
    EXPECT_EQ(p0->repl, 8264);
    EXPECT_EQ(p0->egressControl, 8328);
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
    EXPECT_FALSE(deriveShardBasePorts(127, 1)) << "block would reach the ephemeral range";
    EXPECT_FALSE(deriveShardBasePorts(-1, 1));
    EXPECT_FALSE(deriveShardBasePorts(0, 65)) << "> kMaxShardWorkers";
}

TEST(HostDefaults, CoreBaseIsTheBenchLayoutOnlyForAVisibleCoLocatedCluster) {
    const std::vector<int32_t> four{0, 1, 2, 3};
    // 3 co-located (loopback) nodes x 1 worker on 4 cores → cores 1, 2, 3 (cluster.sh RUNE_SHARD=1).
    for (int32_t i = 0; i < 3; ++i)
        EXPECT_EQ(deriveWorkerCoreBase(HostPlatform::Linux, true, i, 3, 1, four), 1 + i);
    const std::vector<int32_t> eight{0, 1, 2, 3, 4, 5, 6, 7};
    EXPECT_EQ(deriveWorkerCoreBase(HostPlatform::Linux, true, 1, 3, 2, eight), 3);
    EXPECT_EQ(deriveWorkerCoreBase(HostPlatform::Linux, true, 0, 3, 1, {0, 1, 2}), -1) << "does not fit";
    EXPECT_EQ(deriveWorkerCoreBase(HostPlatform::Linux, true, 0, 1, 1, {0, 1}), 1) << "single loopback node";
    // Review I1: the affinity MASK, not the online count; consecutive per node.
    EXPECT_EQ(deriveWorkerCoreBase(HostPlatform::Linux, true, 2, 3, 1, {4, 5, 6, 7}), 7);
    EXPECT_EQ(deriveWorkerCoreBase(HostPlatform::Linux, true, 0, 1, 2, {0, 2, 3}), 2);
    EXPECT_EQ(deriveWorkerCoreBase(HostPlatform::Linux, true, 0, 1, 2, {0, 1, 3}), -1) << "not consecutive";
    EXPECT_EQ(deriveWorkerCoreBase(HostPlatform::Linux, true, 0, 1, 1, {}), -1) << "unknown mask";
    // Non-loopback (containers on one VM look like dedicated hosts) → never auto-pinned.
    EXPECT_EQ(deriveWorkerCoreBase(HostPlatform::Linux, false, 0, 3, 1, eight), -1);
    EXPECT_EQ(deriveWorkerCoreBase(HostPlatform::MacOS, true, 0, 3, 1, eight), -1) << "no affinity on macOS";
    EXPECT_EQ(deriveWorkerCoreBase(HostPlatform::Other, true, 0, 1, 1, eight), -1);
}

TEST(HostDefaults, TermsAreNonSparseOnlyOnTmpfs) {
    EXPECT_FALSE(deriveTermSparse(std::nullopt, /*tmpfs*/true));
    EXPECT_TRUE(deriveTermSparse(std::nullopt, false)) << "disk-backed dir: no 48 MiB zero-fill per log";
    EXPECT_TRUE(deriveTermSparse(true, true)) << "explicit wins";
    EXPECT_FALSE(deriveTermSparse(false, false)) << "explicit wins";
}

// ---------------------------------------------------------------------------
// applyHostDerivedDefaults
// ---------------------------------------------------------------------------
namespace {
/// k3s-node-01: Linux, 4 CPUs, sysctls at 16 MiB, Aeron dir on /dev/shm.
HostNetFacts k3sFacts() {
    HostNetFacts f = linuxFacts(true, 0, 16 * MiB, 16 * MiB);
    f.allowedCpus = {0, 1, 2, 3};
    f.aeronDirTmpfs = true;
    return f;
}
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
    c.clientEndpoint.channel = "aeron:udp?endpoint=127.0.0.1:" + std::to_string(40200 + self);
    return c;
}
} // namespace

TEST(HostDefaults, DefaultConfigIsShardFusedWithTheFindingsSizes) {
    const NodeConfig c;
    EXPECT_TRUE(c.shardMode());
    EXPECT_TRUE(c.fused());
    EXPECT_EQ(c.workerCount, 1);
    EXPECT_EQ(c.aeronTermBufferBytes, 1 * MiB) << "measured 2026-10-03 (findings §4.2/§4.8)";
    EXPECT_FALSE(c.aeronTermBufferSparse.has_value()) << "derived (non-sparse on tmpfs)";
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
        const auto hd = applyHostDerivedDefaults(c, k3sFacts());
        EXPECT_EQ(c.aeronMtuBytes, 65504);
        EXPECT_EQ(c.aeronSocketBufferBytes, 16 * MiB);
        EXPECT_EQ(c.aeronRcvWindowBytes, 256 * 1024) << "term/4 of the 1 MiB term (measured config)";
        EXPECT_EQ(c.workerCoreBase, 1 + self);
        EXPECT_EQ(c.aeronTermBufferSparse, false) << "tmpfs → non-sparse (findings §4)";
        EXPECT_TRUE(c.perWorkerIngressEnabled && c.perWorkerReplication && c.perWorkerEgressEnabled);
        const auto p = deriveShardBasePorts(self, 1);
        EXPECT_EQ(c.workerIngressBasePort, p->ingress);
        EXPECT_EQ(c.workerReplBasePort, p->repl);
        EXPECT_EQ(c.workerEgressControlBasePort, p->egressControl);
        for (const auto& peer : c.peers) {
            const auto pp = deriveShardBasePorts(peer.id, 1);
            EXPECT_EQ(peer.ingressBasePort, pp->ingress);
            EXPECT_EQ(peer.replBasePort, pp->repl);
            EXPECT_EQ(peer.egressControlBasePort, pp->egressControl) << "advertised to clients (B1)";
        }
        EXPECT_FALSE(c.validateShardConfig().has_value()) << *c.validateShardConfig();
        EXPECT_TRUE(hd.warnings.empty()) << hd.warnings;
        EXPECT_NE(hd.summary.find("mode=shard placement=fused workers=1"), std::string::npos) << hd.summary;
        EXPECT_NE(hd.summary.find("mtu=65504 socketBuffer=16777216 rcvWindow=262144"), std::string::npos) << hd.summary;
    }
}

// The effective-config line proves which UDP path a run measured (bench rule: verify the measured path).
// Fix round 1 (review L5): GSO send and GRO receive are reported separately — the bindings install with
// either one (a kernel with UDP_GRO but no UDP_SEGMENT still runs GRO).
TEST(HostDefaults, SummaryReportsUdpGsoAndGroSeparatelyRegression) {
    struct Case { bool want, gso, gro; const char* g; const char* r; };
    for (const Case c : {Case{true, true, true, "gso=on", "gro=on"},
                         Case{true, false, true, "gso=unsupported", "gro=on"},
                         Case{true, true, false, "gso=on", "gro=unsupported"},
                         Case{true, false, false, "gso=unsupported", "gro=unsupported"},
                         Case{false, true, true, "gso=off", "gro=off"}}) {
        NodeConfig cfg = threeNodeDefault(0);
        cfg.aeronUdpGso = c.want;
        HostNetFacts f = k3sFacts();
        f.udpGso = c.gso;
        f.udpGro = c.gro;
        const auto hd = applyHostDerivedDefaults(cfg, f);
        EXPECT_NE(hd.summary.find(std::string(" ") + c.g + " " + c.r + " "), std::string::npos) << hd.summary;
    }
}

// Never busy-spin (user rule 2026-10-03; measured: no p50/p99 gain over backoff, and it pegs cores the
// box needs): a shard worker pinned to its own core still idles with BACKOFF by default.  Busy-spin only
// on the explicit RUNE_WORKER_IDLE=busyspin opt-in.
TEST(HostDefaults, PinnedShardWorkersBackOffByDefaultRegression) {
    NodeConfig c = threeNodeDefault(0);
    const auto hd = applyHostDerivedDefaults(c, k3sFacts());
    ASSERT_GE(c.workerCoreBase, 0) << "precondition: pinned";
    EXPECT_FALSE(c.shardWorkersBusySpin()) << "a pinned shard worker must not busy-spin by default";
    EXPECT_NE(hd.summary.find("coreBase=1(backoff)"), std::string::npos) << hd.summary;

    NodeConfig spin = threeNodeDefault(0);
    spin.workerIdleStrategy = NodeConfig::WorkerIdleStrategy::BusySpin;   // explicit opt-in only
    const auto hs = applyHostDerivedDefaults(spin, k3sFacts());
    EXPECT_TRUE(spin.shardWorkersBusySpin());
    EXPECT_NE(hs.summary.find("coreBase=1(busy-spin)"), std::string::npos) << hs.summary;
}

// A7: the effective-config line names the shard idle (park-until-work + its derived shape, or the
// legacy backoff when RUNE_IDLE_WAKE=0, or busy-spin on the explicit opt-in) — proof of the measured path.
TEST(HostDefaults, SummaryNamesShardIdleShape) {
    NodeConfig c = threeNodeDefault(0);
    const auto hd = applyHostDerivedDefaults(c, k3sFacts());
    EXPECT_NE(hd.summary.find("idle=wake+doorbell{spins=auto yields=auto parkUs=1000-1000}"), std::string::npos)
        << hd.summary;

    NodeConfig nobell = threeNodeDefault(0);   // A9 off: A7's shape
    nobell.idleDoorbell = false;
    const auto hn = applyHostDerivedDefaults(nobell, k3sFacts());
    EXPECT_NE(hn.summary.find("idle=wake{spins=auto yields=auto parkUs=1-200}"), std::string::npos) << hn.summary;

    NodeConfig off = threeNodeDefault(0);
    off.idleWake = false;
    const auto ho = applyHostDerivedDefaults(off, k3sFacts());
    EXPECT_NE(ho.summary.find("idle=backoff{spins=auto yields=auto parkUs=1-200}"), std::string::npos) << ho.summary;

    NodeConfig ex = threeNodeDefault(0);
    ex.idleSpins = 1; ex.idleYields = 2; ex.idleDoorbellMinParkUs = 50; ex.idleDoorbellParkUs = 2000;
    const auto he = applyHostDerivedDefaults(ex, k3sFacts());
    EXPECT_NE(he.summary.find("idle=wake+doorbell{spins=1 yields=2 parkUs=50-2000}"), std::string::npos) << he.summary;

    NodeConfig spin = threeNodeDefault(0);
    spin.workerIdleStrategy = NodeConfig::WorkerIdleStrategy::BusySpin;
    const auto hs = applyHostDerivedDefaults(spin, k3sFacts());
    EXPECT_NE(hs.summary.find("idle=busy-spin"), std::string::npos) << hs.summary;
}

// Low-volume control publications (per-session acks, gateway egress) get their own small term: at the
// data term (16 MiB) an ack stream filled a term every ~10 s and the rotation's term cleaning stalled the
// worker pass 100+ ms (p99 11-31 ms -> 311-352 us with 1 MiB, findings §4.2).
TEST(HostDefaults, ControlPublicationsUseTheirOwnSmallTermRegression) {
    NodeConfig c = threeNodeDefault(0);
    EXPECT_EQ(c.aeronControlTermBufferBytes, 1024 * 1024);
    const auto hd = applyHostDerivedDefaults(c, k3sFacts());
    EXPECT_NE(hd.summary.find("ctlTerm=1048576"), std::string::npos) << hd.summary;
    EXPECT_NE(hd.summary.find("fsyncMs{worker=0 follower=0}"), std::string::npos) << hd.summary;
    // Floor: the ack term's window (term/2) must hold a full MTU datagram — 64 KiB at MTU 65504 is raised.
    NodeConfig small = threeNodeDefault(0);
    small.aeronControlTermBufferBytes = 64 * 1024;
    const auto hs = applyHostDerivedDefaults(small, k3sFacts());
    ASSERT_EQ(small.aeronMtuBytes, 65504);
    EXPECT_EQ(small.aeronControlTermBufferBytes, 128 * 1024);
    EXPECT_NE(hs.warnings.find("RUNE_AERON_CONTROL_TERM_BYTES"), std::string::npos) << hs.warnings;
    using rune::node::withTermLength;
    EXPECT_EQ(withTermLength("aeron:udp?endpoint=127.0.0.1:40900", 1 << 20),
              "aeron:udp?endpoint=127.0.0.1:40900|term-length=1048576");
    EXPECT_EQ(withTermLength("aeron:udp?endpoint=h:1|term-length=65536", 1 << 20),
              "aeron:udp?endpoint=h:1|term-length=65536") << "an explicit per-channel term wins";
    EXPECT_EQ(withTermLength("aeron:ipc", 1 << 20), "aeron:ipc?term-length=1048576");
    EXPECT_EQ(withTermLength("aeron:udp?endpoint=h:1", 0), "aeron:udp?endpoint=h:1") << "0 = driver default";
}

// A10 regression: runtime-created control publications use sparse terms (no tmpfs fallocate → no EINTR).
TEST(AeronChannelRegression, ControlPublicationsUseSparseTerms) {
    using rune::node::withSparseTerms;
    using rune::node::withTermLength;
    EXPECT_EQ(withSparseTerms(withTermLength("aeron:udp?endpoint=127.0.0.1:40900", 1 << 20)),
              "aeron:udp?endpoint=127.0.0.1:40900|term-length=1048576|sparse=true");
    EXPECT_EQ(withSparseTerms("aeron:udp?endpoint=h:1|sparse=false"), "aeron:udp?endpoint=h:1|sparse=false")
        << "an explicit per-channel value wins";
    EXPECT_EQ(withSparseTerms("aeron:ipc"), "aeron:ipc?sparse=true");
}

// LAT-1 (f): io_uring punts blocking ops to io-wq kernel threads that inherit the submitter's affinity —
// i.e. they ran ON the pinned worker's own core and preempted it.  With pinned workers the derived default
// binds io-wq to the cores no co-located node's worker uses (core 0 on the 4-core bench box).
TEST(HostDefaults, IoWorkersStayOffWorkerCoresRegression) {
    for (int32_t self = 0; self < 3; ++self) {
        NodeConfig c = threeNodeDefault(self);
        const auto hd = applyHostDerivedDefaults(c, k3sFacts());
        ASSERT_EQ(c.workerCoreBase, 1 + self);
        EXPECT_EQ(c.ioWorkerCpus, std::vector<int32_t>{0}) << "node " << self;
        EXPECT_NE(hd.summary.find("ioWq=0 "), std::string::npos) << hd.summary;
    }
    NodeConfig unpinned = threeNodeDefault(0);
    unpinned.workerCoreBase = -1;
    (void)applyHostDerivedDefaults(unpinned, k3sFacts());
    EXPECT_TRUE(unpinned.ioWorkerCpus.empty()) << "unpinned workers: io-wq keeps the kernel default";
    NodeConfig expl = threeNodeDefault(0);
    expl.ioWorkerCpus = {2, 3};
    (void)applyHostDerivedDefaults(expl, k3sFacts());
    EXPECT_EQ(expl.ioWorkerCpus, (std::vector<int32_t>{2, 3})) << "an explicit RUNE_IOWQ_CORES wins";
    // An explicit list outside the allowed CPUs is filtered (with a warning), never passed to the kernel.
    NodeConfig outside = threeNodeDefault(0);
    outside.ioWorkerCpus = {2, 6};
    const auto hdo = applyHostDerivedDefaults(outside, k3sFacts());
    EXPECT_EQ(outside.ioWorkerCpus, std::vector<int32_t>{2}) << "CPU 6 is not in the 4-CPU allowed set";
    EXPECT_NE(hdo.warnings.find("RUNE_IOWQ_CORES"), std::string::npos) << hdo.warnings;
    // Restricted allowed mask {2..7}: workers at INDICES 1..3 = CPUs 3,4,5 -> io-wq on the rest: 2,6,7.
    auto restricted = k3sFacts();
    restricted.allowedCpus = {2, 3, 4, 5, 6, 7};
    NodeConfig r = threeNodeDefault(0);
    (void)applyHostDerivedDefaults(r, restricted);
    ASSERT_EQ(r.workerCoreBase, 3);
    EXPECT_EQ(r.ioWorkerCpus, (std::vector<int32_t>{2, 6, 7}));
    // An explicit core base (loopback peers): the other nodes' cores are unknown -> io-wq inherits.
    NodeConfig eb = threeNodeDefault(0);
    eb.workerCoreBase = 6;
    auto eight = k3sFacts();
    eight.allowedCpus = {0, 1, 2, 3, 4, 5, 6, 7};
    (void)applyHostDerivedDefaults(eb, eight);
    EXPECT_TRUE(eb.ioWorkerCpus.empty()) << "explicit RUNE_WORKER_CORE_BASE: no guessed io-wq set";
    // The layout does not fit (3 nodes on 3 CPUs) -> unpinned -> inherit.
    auto three = k3sFacts();
    three.allowedCpus = {0, 1, 2};
    NodeConfig nf = threeNodeDefault(0);
    (void)applyHostDerivedDefaults(nf, three);
    EXPECT_EQ(nf.workerCoreBase, -1);
    EXPECT_TRUE(nf.ioWorkerCpus.empty());
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
        p.egressControlBasePort = static_cast<uint16_t>(40400 + 20 * p.id);
    }
    c.aeronTermBufferSparse = true;
    const NodeConfig before = c;
    applyHostDerivedDefaults(c, k3sFacts());
    EXPECT_EQ(c.aeronMtuBytes, 8192);
    EXPECT_EQ(c.aeronSocketBufferBytes, 4 * MiB);
    EXPECT_EQ(c.aeronRcvWindowBytes, 1 * MiB);
    EXPECT_EQ(c.workerCoreBase, -1);
    EXPECT_EQ(c.workerIngressBasePort, 40520);
    EXPECT_EQ(c.workerReplBasePort, 40620);
    EXPECT_EQ(c.workerEgressControlBasePort, 40420);
    EXPECT_EQ(c.aeronTermBufferSparse, true);
    for (std::size_t i = 0; i < c.peers.size(); ++i) {
        EXPECT_EQ(c.peers[i].ingressBasePort, before.peers[i].ingressBasePort);
        EXPECT_EQ(c.peers[i].replBasePort, before.peers[i].replBasePort);
    }
    EXPECT_FALSE(c.validateShardConfig().has_value()) << *c.validateShardConfig();
    // Idempotent: a second apply changes nothing.
    NodeConfig again = c;
    applyHostDerivedDefaults(again, k3sFacts());
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
    applyHostDerivedDefaults(c, k3sFacts());
    EXPECT_EQ(c.workerIngressBasePort, 40540);
    EXPECT_EQ(c.workerReplBasePort, 40640);
    EXPECT_FALSE(c.validateShardConfig().has_value()) << *c.validateShardConfig();
}

TEST(HostDefaults, ExplicitSelfBaseNotAdvertisedInPeersRefusesStart) {
    // Review I3: RUNE_WORKER_INGRESS_BASE_PORT set, RUNE_PEERS silent → every
    // OTHER node derives 8200+192*id for this node; refuse instead of silently
    // routing peers / clients to a port nobody binds.
    NodeConfig c = threeNodeDefault(1);
    c.workerIngressBasePort = 40520;
    applyHostDerivedDefaults(c, k3sFacts());
    const auto err = c.validateShardConfig();
    ASSERT_TRUE(err.has_value());
    EXPECT_NE(err->find("advertise it in RUNE_PEERS"), std::string::npos) << *err;
    // The egress-control base was never required in RUNE_PEERS (older harnesses
    // set only the env var): kept for self, started, but WARNED (peers advertise
    // the derived base for this node).
    NodeConfig e = threeNodeDefault(1);
    e.workerEgressControlBasePort = 40420;
    const auto hd = applyHostDerivedDefaults(e, k3sFacts());
    EXPECT_FALSE(e.validateShardConfig().has_value()) << *e.validateShardConfig();
    EXPECT_EQ(e.effectiveWorkerEgressControlBasePort(), 40420);
    EXPECT_NE(hd.warnings.find("not advertised in RUNE_PEERS"), std::string::npos) << hd.warnings;
    // An ADVERTISED base that disagrees with the env var is still refused.
    NodeConfig x = threeNodeDefault(1);
    x.workerEgressControlBasePort = 40420;
    x.peers[1].egressControlBasePort = 40999;
    applyHostDerivedDefaults(x, k3sFacts());
    const auto xerr = x.validateShardConfig();
    ASSERT_TRUE(xerr.has_value());
    EXPECT_NE(xerr->find("6th RUNE_PEERS segment"), std::string::npos) << *xerr;
}

TEST(HostDefaults, EveryNodeComputesTheSamePeerBases) {
    // B1/I3: node A's view of node B's bases == node B's own effective bases.
    std::array<NodeConfig, 3> v{threeNodeDefault(0), threeNodeDefault(1), threeNodeDefault(2)};
    for (auto& c : v) applyHostDerivedDefaults(c, k3sFacts());
    for (const auto& viewer : v)
        for (const auto& p : viewer.peers) {
            const NodeConfig& owner = v[static_cast<std::size_t>(p.id)];
            EXPECT_EQ(p.ingressBasePort, *owner.workerIngressBasePort);
            EXPECT_EQ(p.replBasePort, *owner.workerReplBasePort);
            EXPECT_EQ(p.egressControlBasePort, owner.effectiveWorkerEgressControlBasePort());
        }
}

TEST(HostDefaults, LegacyModeIsUntouchedApartFromTransport) {
    NodeConfig c = threeNodeDefault(0);
    c.workerMode = NodeConfig::WorkerMode::Legacy;
    c.runtimePlacement = NodeConfig::RuntimePlacement::Thread;
    applyHostDerivedDefaults(c, k3sFacts());
    EXPECT_EQ(c.workerCoreBase, -1) << "legacy never auto-pins";
    EXPECT_FALSE(c.workerIngressBasePort.has_value());
    EXPECT_FALSE(c.perWorkerIngressEnabled);
    for (const auto& p : c.peers) EXPECT_EQ(p.ingressBasePort, 0);
    EXPECT_EQ(c.aeronMtuBytes, 65504);
}

TEST(HostDefaults, MacHostAppliesTheCappedBufferAndNoPinning) {
    NodeConfig c = threeNodeDefault(0);
    const auto hd = applyHostDerivedDefaults(c, macFacts(8 * MiB));
    EXPECT_EQ(c.aeronSocketBufferBytes, 8 * MiB);
    EXPECT_EQ(c.aeronRcvWindowBytes, 256 * 1024) << "term/4 of the 1 MiB default term";
    EXPECT_EQ(c.aeronMtuBytes, 65504);
    EXPECT_EQ(c.workerCoreBase, -1);
    EXPECT_EQ(c.aeronTermBufferSparse, true) << "no tmpfs on macOS";
    EXPECT_TRUE(hd.socketBufferCapped);
    EXPECT_NE(hd.warnings.find("kern.ipc.maxsockbuf"), std::string::npos);
}

TEST(HostDefaults, EnvOverridesSurviveTheDerivation) {
    // parseNodeConfig → explicit values → apply keeps them.
    std::map<std::string, std::string> env = {
        {"RUNE_NODE_ID", "0"}, {"RUNE_PEERS", "0@127.0.0.1:40110:40200:40220"},
        {"RUNE_CLIENT_ENDPOINT", "127.0.0.1:40200"}, {"RUNE_DATA_DIR", "/tmp/hd"}, {"RUNE_AUTH", "disabled"},
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
    applyHostDerivedDefaults(c, k3sFacts());
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
        {"RUNE_CLIENT_ENDPOINT", "127.0.0.1:40201"}, {"RUNE_DATA_DIR", "/tmp/hd"},
        {"RUNE_CLUSTER_KEY_FILE", "/tmp/hd.key"}};
    auto r = parseNodeConfig([&](const char* k) -> const char* {
        auto it = env.find(k);
        return it == env.end() ? nullptr : it->second.c_str();
    });
    ASSERT_TRUE(r.config.has_value()) << r.error;
    NodeConfig c = *r.config;
    EXPECT_TRUE(c.shardMode());
    EXPECT_TRUE(c.fused());
    applyHostDerivedDefaults(c, k3sFacts());
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

// Review I8 / M7: garbage never becomes a crippling MTU or a busy-spin.
TEST(HostDefaults, InvalidMtuIsRefusedAndGarbageCoreBaseDisables) {
    auto parse = [](std::map<std::string, std::string> extra) {
        std::map<std::string, std::string> env = {
            {"RUNE_NODE_ID", "0"}, {"RUNE_PEERS", "0@127.0.0.1:40110"},
            {"RUNE_CLIENT_ENDPOINT", "127.0.0.1:40200"}, {"RUNE_DATA_DIR", "/tmp/hd"}, {"RUNE_AUTH", "disabled"}};
        for (auto& [k, v] : extra) env[k] = v;
        return parseNodeConfig([env](const char* k) -> const char* {
            auto it = env.find(k);
            return it == env.end() ? nullptr : it->second.c_str();
        });
    };
    for (const char* bad : {"0", "-5", "32", "65505", "abc"})
        EXPECT_FALSE(parse({{"RUNE_AERON_MTU", bad}}).config.has_value()) << bad;
    EXPECT_EQ(parse({{"RUNE_AERON_MTU", "8192"}}).config->aeronMtuBytes, 8192);
    EXPECT_EQ(parse({{"RUNE_WORKER_CORE_BASE", "notanumber"}}).config->workerCoreBase, -1);
    // 6th RUNE_PEERS segment: the egress-control base.
    auto r = parse({{"RUNE_PEERS", "0@127.0.0.1:40110:40200:40220:40600:40500:40400"}});
    ASSERT_TRUE(r.config.has_value()) << r.error;
    EXPECT_EQ(r.config->peers[0].egressControlBasePort, 40400);
    EXPECT_FALSE(parse({{"RUNE_PEERS", "0@127.0.0.1:1:2:3:4:5:6:7"}}).config.has_value()) << "max 6 segments";
}

// REGRESSION (k3s, broker 7ad0b2a): cluster.sh-style config — client bind
// 0.0.0.0, every peer 127.0.0.1 — derived mtu=1472 (the egress NIC) instead of
// 65504: 14-18k rec/s vs 136,234 with 65504.  Datagrams go to the PEERS and to
// the ADVERTISED client endpoint (the peer host), never to the bind address: a
// wildcard bind with loopback peers is loopback.
TEST(HostDefaultsRegression, WildcardBindWithLoopbackPeersIsLoopbackRegression) {
    NodeConfig c = threeNodeDefault(0);
    c.clientEndpoint.channel = "aeron:udp?endpoint=0.0.0.0:40200";
    const auto hosts = nodeEndpointHosts(c);
    for (const auto& h : hosts) EXPECT_TRUE(isLoopbackHost(h)) << h;
    const auto facts = probeHostNetFacts(hosts);           // the REAL probe
    EXPECT_TRUE(facts.loopback);
    if (std::min(facts.rmemMax, facts.wmemMax) < 1024 * 1024)
        GTEST_SKIP() << "host socket limits < 1 MiB clamp the MTU below 65504";
    applyHostDerivedDefaults(c, facts);
    EXPECT_EQ(c.aeronMtuBytes, 65504) << "loopback peers + wildcard bind must derive the 64 KiB loopback MTU";
    // Same with the pure facts of the k3s box.
    NodeConfig k = threeNodeDefault(1);
    k.clientEndpoint.channel = "aeron:udp?endpoint=0.0.0.0:40201";
    HostNetFacts kf = k3sFacts();
    kf.loopback = true;
    for (const auto& h : nodeEndpointHosts(k)) kf.loopback = kf.loopback && isLoopbackHost(h);
    applyHostDerivedDefaults(k, kf);
    EXPECT_EQ(k.aeronMtuBytes, 65504);
}

TEST(HostDefaultsRegression, MixedPeersUseTheEgressInterfaceRegression) {
    // A non-loopback peer: the NIC path decides (never 65504 over a real NIC).
    NodeConfig c = threeNodeDefault(0);
    c.peers[2].host = "10.1.2.3";
    c.clientEndpoint.channel = "aeron:udp?endpoint=0.0.0.0:40200";
    bool lo = true;
    for (const auto& h : nodeEndpointHosts(c)) lo = lo && isLoopbackHost(h);
    EXPECT_FALSE(lo);
}

// D2 (thread placement, 4-core box): every non-worker broker thread (threaded node driver, runtime, admin,
// cleaner, ...) inherited the full mask and the scheduler ran it on the pinned workers' cores — ~10x the
// workers' involuntary switches.  Pure complement first.
TEST(HostDefaults, NonWorkerCpusAreTheComplementRegression) {
    EXPECT_EQ(complementCpus({0, 1, 2, 3}, {1, 2, 3}), std::vector<int32_t>{0});
    EXPECT_EQ(complementCpus({0, 1, 2, 3}, {1}), (std::vector<int32_t>{0, 2, 3}));
    EXPECT_TRUE(complementCpus({1, 2}, {1, 2}).empty()) << "empty complement";
    EXPECT_TRUE(complementCpus({}, {1}).empty()) << "unknown mask";
    EXPECT_EQ(complementCpus({0, 2, 5, 7, 9}, {2, 7}), (std::vector<int32_t>{0, 5, 9})) << "non-contiguous mask";
    EXPECT_EQ(complementCpus({0, 2, 5}, {4, 6}), (std::vector<int32_t>{0, 2, 5})) << "excluded CPUs outside the mask";
}

// D2: the derived non-worker set — every co-located node's worker cores are excluded (core 0 on the bench box),
// for the derived layout AND for an explicit core base that follows the same convention (the bench harness
// passes RUNE_WORKER_CORE_BASE=1+i*W explicitly).
TEST(HostDefaults, NonWorkerThreadsStayOffWorkerCoresRegression) {
    for (int32_t self = 0; self < 3; ++self) {
        NodeConfig c = threeNodeDefault(self);
        const auto hd = applyHostDerivedDefaults(c, k3sFacts());
        ASSERT_EQ(c.workerCoreBase, 1 + self);
        EXPECT_EQ(c.nonWorkerCpus, std::vector<int32_t>{0}) << "node " << self;
        EXPECT_NE(hd.summary.find("nonWorker=0 "), std::string::npos) << hd.summary;
        NodeConfig e = threeNodeDefault(self);
        e.workerCoreBase = 1 + self;   // explicit, conventional layout
        (void)applyHostDerivedDefaults(e, k3sFacts());
        EXPECT_EQ(e.nonWorkerCpus, std::vector<int32_t>{0}) << "explicit conventional base, node " << self;
    }
    NodeConfig unpinned = threeNodeDefault(0);
    unpinned.workerCoreBase = -1;
    const auto hu = applyHostDerivedDefaults(unpinned, k3sFacts());
    EXPECT_TRUE(unpinned.nonWorkerCpus.empty()) << "unpinned workers: affinity unchanged";
    EXPECT_NE(hu.summary.find("nonWorker=inherit "), std::string::npos) << hu.summary;
    // Non-contiguous allowed mask {0,2,5,7,9}: W=1 workers at INDICES 1..3 = CPUs 2,5,7 -> {0,9}.
    auto sparse = k3sFacts();
    sparse.allowedCpus = {0, 2, 5, 7, 9};
    NodeConfig s = threeNodeDefault(1);
    (void)applyHostDerivedDefaults(s, sparse);
    ASSERT_EQ(s.workerCoreBase, 5);
    EXPECT_EQ(s.nonWorkerCpus, (std::vector<int32_t>{0, 9}));
    // An explicit, unconventional base: only this node's own worker cores are known -> their complement.
    NodeConfig eb = threeNodeDefault(0);
    eb.workerCoreBase = 6;
    auto eight = k3sFacts();
    eight.allowedCpus = {0, 1, 2, 3, 4, 5, 6, 7};
    (void)applyHostDerivedDefaults(eb, eight);
    EXPECT_EQ(eb.nonWorkerCpus, (std::vector<int32_t>{0, 1, 2, 3, 4, 5, 7}));
    // RUNE_NONWORKER_CORES wins, filtered to the allowed mask (a CPU outside it is refused by the kernel).
    NodeConfig ov = threeNodeDefault(0);
    ov.nonWorkerCpus = {3, 9};
    const auto ho = applyHostDerivedDefaults(ov, k3sFacts());
    EXPECT_EQ(ov.nonWorkerCpus, std::vector<int32_t>{3});
    EXPECT_NE(ho.warnings.find("RUNE_NONWORKER_CORES"), std::string::npos) << ho.warnings;
    // Idempotent (applied once at start, but a second pass must not change it).
    NodeConfig twice = threeNodeDefault(2);
    (void)applyHostDerivedDefaults(twice, k3sFacts());
    (void)applyHostDerivedDefaults(twice, k3sFacts());
    EXPECT_EQ(twice.nonWorkerCpus, std::vector<int32_t>{0});
}

// D2: no CPU left for the non-worker threads -> affinity unchanged + ONE warning (never pin onto a worker core).
TEST(HostDefaults, NonWorkerEmptyComplementWarnsAndLeavesAffinityRegression) {
    auto one = k3sFacts();
    one.allowedCpus = {1};
    NodeConfig c = threeNodeDefault(0);
    c.peers.resize(1);
    c.workerCoreBase = 1;
    const auto hd = applyHostDerivedDefaults(c, one);
    EXPECT_TRUE(c.nonWorkerCpus.empty());
    const auto first = hd.warnings.find("non-worker");
    ASSERT_NE(first, std::string::npos) << hd.warnings;
    EXPECT_EQ(hd.warnings.find("non-worker", first + 1), std::string::npos) << "warned once: " << hd.warnings;
}

// D2 review Md-1: RUNE_NONWORKER_CORES was honoured with UNPINNED workers (multi-host, explicit base -1,
// legacy mode).  Workers re-pin only when they have a core, so every worker created inside start()'s scope
// kept the non-worker set for good (all workers on core 0, no warning).  Now: ignored + warned unless the
// workers are pinned (shard mode, workerCoreBase >= 0); with pinned workers this node's own worker cores are
// dropped from the override (warned) — the control plane never lands on a worker core.
TEST(HostDefaultsRegression, NonWorkerOverrideIgnoredUnlessWorkersPinnedRegression) {
    NodeConfig unpinned = threeNodeDefault(0);
    unpinned.workerCoreBase = -1;
    unpinned.nonWorkerCpus  = {0};
    const auto hu = applyHostDerivedDefaults(unpinned, k3sFacts());
    EXPECT_TRUE(unpinned.nonWorkerCpus.empty()) << "unpinned workers would inherit the override";
    EXPECT_NE(hu.warnings.find("RUNE_NONWORKER_CORES ignored"), std::string::npos) << hu.warnings;
    EXPECT_NE(hu.summary.find("nonWorker=inherit "), std::string::npos) << hu.summary;

    NodeConfig legacy = threeNodeDefault(0);
    legacy.workerMode    = NodeConfig::WorkerMode::Legacy;
    legacy.nonWorkerCpus = {0};
    const auto hl = applyHostDerivedDefaults(legacy, k3sFacts());
    EXPECT_TRUE(legacy.nonWorkerCpus.empty()) << "legacy workers never re-pin";
    EXPECT_NE(hl.warnings.find("RUNE_NONWORKER_CORES ignored"), std::string::npos) << hl.warnings;

    NodeConfig overlap = threeNodeDefault(0);   // pinned: derived base 1 (W=1)
    overlap.nonWorkerCpus = {0, 1};
    const auto ho = applyHostDerivedDefaults(overlap, k3sFacts());
    ASSERT_EQ(overlap.workerCoreBase, 1);
    EXPECT_EQ(overlap.nonWorkerCpus, std::vector<int32_t>{0}) << "own worker core dropped from the override";
    EXPECT_NE(ho.warnings.find("worker core"), std::string::npos) << ho.warnings;

    NodeConfig pinned = threeNodeDefault(1);    // pinned + disjoint override: honoured verbatim, no warning
    pinned.nonWorkerCpus = {0};
    const auto hp = applyHostDerivedDefaults(pinned, k3sFacts());
    EXPECT_EQ(pinned.nonWorkerCpus, std::vector<int32_t>{0});
    EXPECT_EQ(hp.warnings.find("RUNE_NONWORKER_CORES"), std::string::npos) << hp.warnings;
}

// D2 round 2: the effective-config line proves the control-plane idle a measurement ran with.
TEST(HostDefaults, EffectiveConfigNamesTheControlPlaneIdleRegression) {
    NodeConfig c = threeNodeDefault(0);
    c.runtimePlacement = NodeConfig::RuntimePlacement::Thread;
    const auto hd = applyHostDerivedDefaults(c, k3sFacts());
    EXPECT_NE(hd.summary.find("drvIdle=backoff{1-0-100000-2000000} ctlIdle=10-0-100us-2000us "), std::string::npos)
        << hd.summary;
    EXPECT_NE(hd.summary.find("derived={"), std::string::npos) << "line not truncated: " << hd.summary;
}

// D2 review R2-L4 (lows-review Md-3): the proof-of-path fields name what really runs — fused placement pumps the node
// driver as an invoker inside worker 0's pass (no idle loop, no runtime thread); a busy-spin runtime thread spins.
TEST(HostDefaults, EffectiveConfigIdleFieldsMatchThePlacementRegression) {
    NodeConfig f = threeNodeDefault(0);
    ASSERT_TRUE(f.fused());
    const auto hf = applyHostDerivedDefaults(f, k3sFacts());
    EXPECT_NE(hf.summary.find("drvIdle=invoker ctlIdle=worker0 "), std::string::npos) << hf.summary;
    NodeConfig b = threeNodeDefault(0);
    b.runtimePlacement    = NodeConfig::RuntimePlacement::Thread;
    b.runtimeIdleStrategy = NodeConfig::WorkerIdleStrategy::BusySpin;
    const auto hb = applyHostDerivedDefaults(b, k3sFacts());
    EXPECT_NE(hb.summary.find(" ctlIdle=busyspin "), std::string::npos) << hb.summary;
}

// D2 review R2-L3 / R2-L4: Aeron's yield / spin / noop idle strategies take no init args — RUNE_AERON_DRIVER_IDLE_ARGS
// with one of them is ignored by Aeron: say so, and do not print args that do not apply on the effective-config line.
TEST(HostDefaults, DriverIdleArgsWithArglessStrategyWarnRegression) {
    NodeConfig c = threeNodeDefault(0);
    c.runtimePlacement   = NodeConfig::RuntimePlacement::Thread;   // fused: the node driver is an invoker
    c.driverIdleStrategy = "yield";
    c.driverIdleArgs     = "1-0-1us-1ms";
    const auto hd = applyHostDerivedDefaults(c, k3sFacts());
    EXPECT_NE(hd.warnings.find("RUNE_AERON_DRIVER_IDLE_ARGS"), std::string::npos) << hd.warnings;
    EXPECT_NE(hd.summary.find("drvIdle=yield "), std::string::npos) << hd.summary;
    NodeConfig b = threeNodeDefault(0);
    b.runtimePlacement   = NodeConfig::RuntimePlacement::Thread;
    b.driverIdleStrategy = "backoff";
    b.driverIdleArgs     = "1-0-1us-1ms";
    const auto hb = applyHostDerivedDefaults(b, k3sFacts());
    EXPECT_EQ(hb.warnings.find("RUNE_AERON_DRIVER_IDLE_ARGS"), std::string::npos) << hb.warnings;
    EXPECT_NE(hb.summary.find("drvIdle=backoff{1-0-1us-1ms} "), std::string::npos) << hb.summary;
}

// D2 review R2-L2: in shard mode RUNE_IDLE_MAX_PARK_US no longer drives the runtime / admin / node-driver threads
// (RUNE_CONTROL_IDLE_MAX_PARK_US does) — an operator who set it explicitly is told.
TEST(HostDefaults, IdleMaxParkUsInShardModeWarnsRegression) {
    NodeConfig c = threeNodeDefault(0);
    ASSERT_TRUE(c.shardMode());
    c.runtimePlacement      = NodeConfig::RuntimePlacement::Thread;
    c.idleMaxParkUs         = 50;
    c.idleMaxParkUsExplicit = true;
    const auto hd = applyHostDerivedDefaults(c, k3sFacts());
    EXPECT_NE(hd.warnings.find("RUNE_CONTROL_IDLE_MAX_PARK_US"), std::string::npos) << hd.warnings;
    // Review Md-2: fused (default) placement — the runtime and the node driver ride worker 0, so the knob still
    // caps them: no warning.
    NodeConfig f = threeNodeDefault(0);
    ASSERT_TRUE(f.fused());
    f.idleMaxParkUs         = 50;
    f.idleMaxParkUsExplicit = true;
    const auto hf = applyHostDerivedDefaults(f, k3sFacts());
    EXPECT_EQ(hf.warnings.find("RUNE_IDLE_MAX_PARK_US"), std::string::npos) << hf.warnings;
    NodeConfig d = threeNodeDefault(0);   // not set explicitly: silent
    const auto hn = applyHostDerivedDefaults(d, k3sFacts());
    EXPECT_EQ(hn.warnings.find("RUNE_IDLE_MAX_PARK_US"), std::string::npos) << hn.warnings;
}

TEST(NodeConfigEnv, IdleMaxParkUsMarksExplicitRegression) {
    const auto parse = [](const char* park) {
        std::map<std::string, std::string> env = {
            {"RUNE_NODE_ID", "0"}, {"RUNE_PEERS", "0@127.0.0.1:40110:40200:40220"},
            {"RUNE_CLIENT_ENDPOINT", "127.0.0.1:40200"}, {"RUNE_DATA_DIR", "/tmp/hd"}, {"RUNE_AUTH", "disabled"}};
        if (park != nullptr) env["RUNE_IDLE_MAX_PARK_US"] = park;
        return parseNodeConfig([env](const char* k) -> const char* {
            auto it = env.find(k);
            return it == env.end() ? nullptr : it->second.c_str();
        });
    };
    const auto r = parse("75");
    ASSERT_TRUE(r.config.has_value()) << r.error;
    EXPECT_EQ(r.config->idleMaxParkUs, 75);
    EXPECT_TRUE(r.config->idleMaxParkUsExplicit);
    const auto d = parse(nullptr);
    ASSERT_TRUE(d.config.has_value()) << d.error;
    EXPECT_FALSE(d.config->idleMaxParkUsExplicit);
}

// A7 review L7: a park cap above 10 ms is accepted (<= 1 s, NodeConfig) but named at startup — a parked
// worker takes fd-less work that late.
TEST(HostDefaults, LongIdleMaxParkWarnsAtStartup) {
    NodeConfig c = threeNodeDefault(0);
    c.idleMaxParkUs = 20'000;
    EXPECT_NE(applyHostDerivedDefaults(c, k3sFacts()).warnings.find("up to that late"), std::string::npos);
    NodeConfig d = threeNodeDefault(0);   // the 200 us default: silent
    EXPECT_EQ(applyHostDerivedDefaults(d, k3sFacts()).warnings.find("up to that late"), std::string::npos);
}

// ---------------------------------------------------------------------------
// DR-BUG-RESTART-BIND (dr-e2e test_dr8_chaos_soak, run 4): a SIGKILLed follower restarted in a fresh netns
// refused to start — "WorkerReplicationTransport[0]: inbound aeron:udp?endpoint=0.0.0.0:41064 ... (98) Address
// already in use".  41064 (the derived shard repl base) lay inside Linux's ephemeral range 32768–60999, and the
// broker's own Aeron publications bind ephemeral ports while the node starts, before the shard planes bind
// theirs: a random one took 41064.  The kernel never auto-binds outside its range, so every derived port now
// lies below every default range, and a bound port inside the probed range is a loud startup WARN.
// ---------------------------------------------------------------------------
TEST(HostDefaultsRegression, DerivedShardPortsLieBelowEveryDefaultEphemeralRangeRegression) {
    // RED before the fix: node 0's block [41000, 41191] lay inside Linux's default 32768–60999.
    constexpr int32_t kLinuxLow = 32768, kMacLow = 49152;
    for (int32_t n = 0; n <= kMaxDerivedShardNodeId; ++n) {
        const auto p = deriveShardBasePorts(n, NodeConfig::kMaxShardWorkers);
        ASSERT_TRUE(p) << n;
        for (int32_t w = 0; w < NodeConfig::kMaxShardWorkers; ++w)
            for (const int32_t port : {p->ingress + w, p->repl + w, p->egressControl + w}) {
                EXPECT_GT(port, 1023) << "node " << n << ": privileged port " << port;
                EXPECT_LT(port, kLinuxLow) << "node " << n << ": port " << port << " inside the Linux ephemeral range";
                EXPECT_LT(port, kMacLow) << "node " << n << ": port " << port << " inside the macOS ephemeral range";
            }
    }
    // The Linux-default host derives a config with NO ephemeral conflict: a restart always binds.
    NodeConfig c = threeNodeDefault(1);
    for (auto& peer : c.peers) { peer.consensusPort = 7110; peer.clientPort = 7200; peer.replicationPort = 7222; }
    c.clientEndpoint.channel = "aeron:udp?endpoint=0.0.0.0:7200";
    HostNetFacts f = k3sFacts();
    f.ephemeralPortLow = 32768; f.ephemeralPortHigh = 60999;
    const auto hd = applyHostDerivedDefaults(c, f);
    EXPECT_TRUE(ephemeralPortConflicts(nodeBoundPorts(c), f).empty());
    EXPECT_EQ(hd.warnings.find("ephemeral"), std::string::npos) << hd.warnings;
}

TEST(HostDefaultsRegression, NodeBoundPortsListsEveryPortTheNodeBindsRegression) {
    NodeConfig c = threeNodeDefault(2);
    c.workerCount = 2;
    applyHostDerivedDefaults(c, k3sFacts());
    const auto d = deriveShardBasePorts(2, 2);
    ASSERT_TRUE(d);
    std::vector<int32_t> want = {40112, 40202, 40222, d->ingress, d->ingress + 1, d->repl, d->repl + 1,
                                 d->egressControl, d->egressControl + 1};
    std::sort(want.begin(), want.end());
    EXPECT_EQ(nodeBoundPorts(c), want) << "consensus, client, replication + every worker's three shard ports";
}

TEST(HostDefaultsRegression, BoundPortInsideTheEphemeralRangeWarnsUnlessReservedRegression) {
    // The dr-e2e / docker compose ports (40111 / 40200 / 40222) lie inside Linux's default range.
    NodeConfig c = threeNodeDefault(0);
    HostNetFacts f = k3sFacts();
    f.ephemeralPortLow = 32768; f.ephemeralPortHigh = 60999;
    const auto hd = applyHostDerivedDefaults(c, f);
    EXPECT_EQ(ephemeralPortConflicts(nodeBoundPorts(c), f), (std::vector<int32_t>{40110, 40200, 40220}));
    EXPECT_NE(hd.warnings.find("UDP port(s) 40110,40200,40220"), std::string::npos) << hd.warnings;
    EXPECT_NE(hd.warnings.find("ip_local_reserved_ports=40110,40200,40220"), std::string::npos) << hd.warnings;
    // Reserved (net.ipv4.ip_local_reserved_ports) → never auto-bound → no warning.
    NodeConfig r = threeNodeDefault(0);
    f.reservedPorts = parseReservedPorts("40110,40200-40220\n");
    const auto hr = applyHostDerivedDefaults(r, f);
    EXPECT_TRUE(ephemeralPortConflicts(nodeBoundPorts(r), f).empty());
    EXPECT_EQ(hr.warnings.find("ephemeral"), std::string::npos) << hr.warnings;
    // A host whose range covers the DERIVED block (e.g. 1024–65535) is loud about the shard ports too.
    NodeConfig w = threeNodeDefault(0);
    HostNetFacts wide = k3sFacts();
    wide.ephemeralPortLow = 1024; wide.ephemeralPortHigh = 65535;
    const auto hw = applyHostDerivedDefaults(w, wide);
    const auto p0 = deriveShardBasePorts(0, 1);
    EXPECT_NE(hw.warnings.find(std::to_string(p0->repl)), std::string::npos) << hw.warnings;
    // Unknown range → no check, no warning.
    NodeConfig u = threeNodeDefault(0);
    const auto hu = applyHostDerivedDefaults(u, k3sFacts());
    EXPECT_EQ(hu.warnings.find("ephemeral"), std::string::npos) << hu.warnings;
}

TEST(HostDefaultsRegression, ReservedPortsParseLinuxSyntaxRegression) {
    using R = std::vector<std::pair<int32_t, int32_t>>;
    EXPECT_EQ(parseReservedPorts(""), R{});
    EXPECT_EQ(parseReservedPorts("\n"), R{});
    EXPECT_EQ(parseReservedPorts("8000-8100,9092\n"), (R{{8000, 8100}, {9092, 9092}}));
    EXPECT_EQ(parseReservedPorts("x,70000,5-3,40111"), (R{{40111, 40111}})) << "malformed items are skipped";
}

TEST(HostDefaultsRegression, ProbedEphemeralRangeIsSaneOnThisHostRegression) {
    const HostNetFacts f = probeHostNetFacts({});
#if defined(__linux__) || defined(__APPLE__)
    ASSERT_GT(f.ephemeralPortLow, 0) << "the probe must read the kernel's ephemeral range";
    EXPECT_GE(f.ephemeralPortHigh, f.ephemeralPortLow);
    EXPECT_LE(f.ephemeralPortHigh, 65535);
    // On a default-configured host no derived port is ever ephemeral.
    if (f.ephemeralPortLow >= kLowestDefaultEphemeralPort) {
        const auto p = deriveShardBasePorts(kMaxDerivedShardNodeId, NodeConfig::kMaxShardWorkers);
        ASSERT_TRUE(p);
        EXPECT_LT(p->egressControl + NodeConfig::kMaxShardWorkers - 1, f.ephemeralPortLow);
    }
#else
    EXPECT_EQ(f.ephemeralPortLow, 0);
#endif
}

// ---------------------------------------------------------------------------
// DR-bugs fix round 1
// ---------------------------------------------------------------------------
// Review B-I2: the link MTU must carry one Aeron datagram (Aeron MTU + 28 IPv4 / 48 IPv6).  RED: `<=` for `<`
// flips the 1500/1472 IPv4 case.
TEST(HostDefaultsRegression, LinkMtuBelowAeronMtuIsDetectedRegression) {
    const auto a = checkLinkMtu(1200, 1472, false);
    EXPECT_TRUE(a.below); EXPECT_EQ(a.linkMtu, 1200); EXPECT_EQ(a.needed, 1500);
    EXPECT_FALSE(checkLinkMtu(1500, 1472, false).below);
    const auto v6 = checkLinkMtu(1500, 1472, true);
    EXPECT_TRUE(v6.below); EXPECT_EQ(v6.needed, 1520);
    EXPECT_FALSE(checkLinkMtu(0, 1472, false).below) << "unknown link MTU is never 'below'";
}

// Review A-I5 / B-M1: legacy mode with per-worker egress binds effectiveWorkerEgressControlBasePort()+w (default
// 40300, inside Linux's range): listed and WARNed.  RED: no legacy branch in nodeBoundPorts.
TEST(HostDefaultsRegression, LegacyPerWorkerEgressPortsAreListedAndWarnedRegression) {
    NodeConfig c = threeNodeDefault(0);
    c.workerMode = NodeConfig::WorkerMode::Legacy;
    c.runtimePlacement = NodeConfig::RuntimePlacement::Thread;
    c.perWorkerEgressEnabled = true;
    c.workerCount = 2;
    HostNetFacts f = k3sFacts();
    f.ephemeralPortLow = 32768; f.ephemeralPortHigh = 60999;
    const auto hd = applyHostDerivedDefaults(c, f);
    const auto ports = nodeBoundPorts(c);
    EXPECT_NE(std::find(ports.begin(), ports.end(), 40300), ports.end());
    EXPECT_NE(std::find(ports.begin(), ports.end(), 40301), ports.end());
    EXPECT_NE(hd.warnings.find("40300"), std::string::npos) << hd.warnings;
    EXPECT_NE(hd.warnings.find("40301"), std::string::npos) << hd.warnings;
}

// Review A-M6: a co-located peer's egress-control ports are part of the collision check.  RED: omit them.
TEST(HostDefaultsRegression, CoLocatedPeerEgressControlCollisionRefusesStartRegression) {
    NodeConfig c = threeNodeDefault(1);
    applyHostDerivedDefaults(c, k3sFacts());
    ASSERT_FALSE(c.validateShardConfig().has_value()) << *c.validateShardConfig();
    for (auto& p : c.peers)
        if (p.id == 0) p.egressControlBasePort = static_cast<uint16_t>(*c.workerIngressBasePort);   // = node 1's ingress
    const auto err = c.validateShardConfig();
    ASSERT_TRUE(err.has_value()) << "a co-located peer's egress-control port collides with this node's ingress";
    EXPECT_NE(err->find("co-located peer 0"), std::string::npos) << *err;
}

// Review A-I4 ruling (rolling upgrade across the 41000 -> 8200 move is not supported): the startup line names
// the derived base so a mixed-version cluster is diagnosable.  RED: no derivedBase field.
TEST(HostDefaultsRegression, StartupLineNamesTheDerivedShardPortBaseRegression) {
    NodeConfig c = threeNodeDefault(0);
    const auto hd = applyHostDerivedDefaults(c, k3sFacts());
    EXPECT_NE(hd.summary.find("derivedBase=" + std::to_string(kDerivedShardPortBase)), std::string::npos) << hd.summary;
}

// Re-check I1: the watchdog's target resolves on a cold thread; the control thread reads it by one ioctl.  RED:
// resolveMtuProbeTarget always invalid (the watchdog would be off everywhere).
TEST(HostDefaultsRegression, MtuProbeTargetResolvesOnceAndReadsByIoctlRegression) {
    const MtuProbeTarget lo = resolveMtuProbeTarget("127.0.0.1");
    ASSERT_TRUE(lo.valid);
    const std::string name(lo.ifname);
    EXPECT_TRUE(name == "lo0" || name == "lo") << name;
    EXPECT_FALSE(lo.ipv6);
    const int fd = ::socket(AF_INET, SOCK_DGRAM, 0);
    ASSERT_GE(fd, 0);
    EXPECT_GT(readInterfaceMtu(lo, fd), 0);
    EXPECT_EQ(readInterfaceMtu(MtuProbeTarget{}, fd), 0) << "an invalid target reads unknown";
    ::close(fd);
    EXPECT_FALSE(resolveMtuProbeTarget("no-such-host.invalid").valid);
}
