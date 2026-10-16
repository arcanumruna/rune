// Real Aeron driver tests for rune_gso_bindings (Linux): spec 2026-10-03-aeron-udp-gso-gro-design.md §5,
// plan Review Focus 1-5.  Each rig = one embedded driver (SHARED threading, own dir) + an Aeron C client.
#include <gtest/gtest.h>

#include <limits>

#include <cstddef>   // aeronmd.h uses size_t without including <stddef.h>
extern "C" {
#include <aeronmd/aeronmd.h>
#include <aeron/aeronc.h>
#include "rune/protocol/net/rune_gso_bindings.h"
}

#include "rune/node/EmbeddedDriver.h"

#include <net/if.h>
#include <sys/ioctl.h>
#include <sys/socket.h>
#include <sched.h>
#include <sys/utsname.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <cstdio>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <string>
#include <thread>
#include <vector>

namespace {
using Clock = std::chrono::steady_clock;
constexpr auto kDeadline = std::chrono::seconds(20);

int g_rigSeq = 0;

struct Rig {
    aeron_driver_context_t* dctx = nullptr;
    aeron_driver_t*         driver = nullptr;
    rune_gso_bindings_t*    gso = nullptr;
    aeron_context_t*        cctx = nullptr;
    aeron_t*                client = nullptr;
    std::string             dir;
    rune_gso_totals_t       totals{};
    bool                    closed = false;

    // gsoMode: 0 = stock Aeron, 1 = GSO+GRO, 2 = GSO+GRO with an injected sendmsg errno (inject before start),
    // 3 = GSO+GRO with `injectCount` received GRO buffers flagged MSG_TRUNC.
    int injectSkip = 0;   // gsoMode 4: GSO calls let through before the injection (rune_gso_bindings_test_inject_after)
    bool start(int gsoMode, std::size_t mtu, int injectErr = 0, int injectCount = 0) {
        dir = "/dev/shm/rune-gso-test-" + std::to_string(::getpid()) + "-" + std::to_string(++g_rigSeq);
        if (aeron_driver_context_init(&dctx) < 0) return false;
        aeron_driver_context_set_dir(dctx, dir.c_str());
        aeron_driver_context_set_dir_delete_on_start(dctx, true);
        aeron_driver_context_set_dir_delete_on_shutdown(dctx, true);
        aeron_driver_context_set_threading_mode(dctx, AERON_THREADING_MODE_SHARED);
        aeron_driver_context_set_shared_idle_strategy(dctx, "backoff");
        aeron_driver_context_set_term_buffer_length(dctx, 1 << 20);
        aeron_driver_context_set_mtu_length(dctx, mtu);
        aeron_driver_context_set_network_publication_max_messages_per_send(dctx, 16);
        if (gsoMode != 0) {
            gso = rune_gso_bindings_create(1, 1);
            if (gso == nullptr) return false;   // the VM kernel supports both: NULL here is a failure
            if (gsoMode == 2) rune_gso_bindings_test_inject(gso, injectErr, injectCount);
            if (gsoMode == 4) rune_gso_bindings_test_inject_after(gso, injectErr, injectCount, injectSkip);
            if (gsoMode == 3) rune_gso_bindings_test_inject_recv_trunc(gso, injectCount);
            if (rune_gso_bindings_install(dctx, gso) < 0) return false;
        }
        if (aeron_driver_init(&driver, dctx) < 0) return false;
        if (aeron_driver_start(driver, false) < 0) return false;
        if (aeron_context_init(&cctx) < 0) return false;
        aeron_context_set_dir(cctx, dir.c_str());
        if (aeron_init(&client, cctx) < 0) return false;
        return aeron_start(client) == 0;
    }
    void close() {
        if (closed) return;
        closed = true;
        if (client) aeron_close(client);
        if (cctx) aeron_context_close(cctx);
        if (driver) aeron_driver_close(driver);
        if (gso) rune_gso_bindings_totals(gso, &totals);
        if (dctx) aeron_driver_context_close(dctx);
        if (gso) rune_gso_bindings_delete(gso);
    }
    ~Rig() { close(); }
};

template <class F>
bool pollUntil(F&& done) {
    const auto end = Clock::now() + kDeadline;
    while (!done()) {
        if (Clock::now() > end) return false;
        sched_yield();
    }
    return true;
}

aeron_subscription_t* addSub(aeron_t* c, const char* ch, int32_t stream) {
    aeron_async_add_subscription_t* a = nullptr;
    if (aeron_async_add_subscription(&a, c, ch, stream, nullptr, nullptr, nullptr, nullptr) < 0) return nullptr;
    aeron_subscription_t* s = nullptr;
    pollUntil([&] { return aeron_async_add_subscription_poll(&s, a) != 0; });
    return s;
}
aeron_exclusive_publication_t* addPub(aeron_t* c, const char* ch, int32_t stream) {
    aeron_async_add_exclusive_publication_t* a = nullptr;
    if (aeron_async_add_exclusive_publication(&a, c, ch, stream) < 0) return nullptr;
    aeron_exclusive_publication_t* p = nullptr;
    pollUntil([&] { return aeron_async_add_exclusive_publication_poll(&p, a) != 0; });
    return p;
}

uint8_t patternByte(std::size_t msg, std::size_t i) { return static_cast<uint8_t>((msg * 131u + i * 7u) ^ (i >> 8)); }

struct Received {
    std::size_t next = 0;
    std::vector<std::size_t> sizes;
    bool corrupt = false;
};
void onFragment(void* clientd, const uint8_t* buf, size_t len, aeron_header_t*) {
    auto* r = static_cast<Received*>(clientd);
    const std::size_t m = r->next;
    if (m >= r->sizes.size() || len != r->sizes[m]) { r->corrupt = true; ++r->next; return; }
    for (std::size_t i = 0; i < len; ++i)
        if (buf[i] != patternByte(m, i)) { r->corrupt = true; break; }
    ++r->next;
}

// Publishes `sizes` on `pubRig`, subscribes on `subRig`, checks every message arrives once, in order, byte-exact.
void roundTrip(Rig& pubRig, Rig& subRig, const std::string& channel, const std::vector<std::size_t>& sizes,
               const std::string& subChannel = "", aeron_subscription_t* existingSub = nullptr) {
    aeron_subscription_t* sub = existingSub != nullptr
        ? existingSub : addSub(subRig.client, (subChannel.empty() ? channel : subChannel).c_str(), 1001);
    ASSERT_NE(sub, nullptr) << aeron_errmsg();
    aeron_exclusive_publication_t* pub = addPub(pubRig.client, channel.c_str(), 1001);
    ASSERT_NE(pub, nullptr) << aeron_errmsg();
    ASSERT_TRUE(pollUntil([&] { return aeron_exclusive_publication_is_connected(pub); }));

    aeron_fragment_assembler_t* fa = nullptr;
    Received rx;
    rx.sizes = sizes;
    ASSERT_EQ(aeron_fragment_assembler_create(&fa, onFragment, &rx), 0);

    std::vector<uint8_t> buf;
    for (std::size_t m = 0; m < sizes.size(); ++m) {
        buf.resize(sizes[m]);
        for (std::size_t i = 0; i < buf.size(); ++i) buf[i] = patternByte(m, i);
        ASSERT_TRUE(pollUntil([&] {
            aeron_subscription_poll(sub, aeron_fragment_assembler_handler, fa, 64);
            return aeron_exclusive_publication_offer(pub, buf.data(), buf.size(), nullptr, nullptr) > 0;
        })) << "offer of message " << m << " never accepted";
    }
    ASSERT_TRUE(pollUntil([&] {
        aeron_subscription_poll(sub, aeron_fragment_assembler_handler, fa, 64);
        return rx.next >= sizes.size();
    })) << "received " << rx.next << " of " << sizes.size();
    EXPECT_FALSE(rx.corrupt);
    EXPECT_EQ(rx.next, sizes.size());
    aeron_fragment_assembler_delete(fa);
}

std::vector<std::size_t> mixedSizes(std::size_t mtu, int count) {
    const std::size_t payload = mtu - 32;
    const std::size_t maxMsg = (1u << 20) / 8;   // term 1 MiB -> max message = term/8
    const std::size_t pattern[] = {1, 100, payload, payload + 1, 3 * payload, 22 * payload + 7, maxMsg};
    std::vector<std::size_t> v;
    for (int i = 0; i < count; ++i) v.push_back(std::min(pattern[i % 7], maxMsg));   // offer() rejects > term/8
    return v;
}

// Sum of the driver's system counter whose label starts with `label` (e.g. "Retransmits sent").
int64_t systemCounter(aeron_t* client, const char* label) {
    struct Find { const char* label; int64_t value; } f{label, -1};
    aeron_counters_reader_foreach_counter(
        aeron_counters_reader(client),
        [](int64_t value, int32_t, int32_t typeId, const uint8_t*, size_t, const char* l, size_t ll, void* d) {
            auto* fd = static_cast<Find*>(d);
            const std::size_t n = std::strlen(fd->label);
            if (typeId == 0 && ll >= n && std::strncmp(l, fd->label, n) == 0) fd->value = value;
        },
        &f);
    return f.value;
}
constexpr int32_t kGsoPathMtuCounterTypeId = 1000222;   // rune_gso_bindings.c
// Value of the (first) counter of `typeId` in the client's CnC.
int64_t counterByType(aeron_t* client, int32_t typeId) {
    struct Find { int32_t type; int64_t value; } f{typeId, -1};
    aeron_counters_reader_foreach_counter(
        aeron_counters_reader(client),
        [](int64_t value, int32_t, int32_t t, const uint8_t*, size_t, const char*, size_t, void* d) {
            auto* fd = static_cast<Find*>(d);
            if (t == fd->type && fd->value < 0) fd->value = value;
        },
        &f);
    return f.value;
}
// RAII: lowers the loopback MTU (SIOCSIFMTU) and restores it.
struct LoopbackMtu {
    bool ok = false;
    int  old = 0;
    explicit LoopbackMtu(int mtu) {
        const int fd = ::socket(AF_INET, SOCK_DGRAM, 0);
        if (fd < 0) return;
        ifreq ifr{};
        std::strncpy(ifr.ifr_name, "lo", IFNAMSIZ - 1);
        if (::ioctl(fd, SIOCGIFMTU, &ifr) == 0) {
            old = ifr.ifr_mtu;
            ifr.ifr_mtu = mtu;
            ok = ::ioctl(fd, SIOCSIFMTU, &ifr) == 0;
        }
        ::close(fd);
    }
    ~LoopbackMtu() {
        if (!ok) return;
        const int fd = ::socket(AF_INET, SOCK_DGRAM, 0);
        if (fd < 0) return;
        ifreq ifr{};
        std::strncpy(ifr.ifr_name, "lo", IFNAMSIZ - 1);
        ifr.ifr_mtu = old;
        (void)::ioctl(fd, SIOCSIFMTU, &ifr);
        ::close(fd);
    }
};
std::string udp(int port, std::size_t mtu, const char* host = "localhost") {
    return "aeron:udp?endpoint=" + std::string(host) + ":" + std::to_string(port) + "|mtu=" + std::to_string(mtu);
}
} // namespace

TEST(UdpGsoBindings, PlatformSupportsBothOnTheBenchKernel) {
    utsname u{};
    int major = 0, minor = 0;
    if (::uname(&u) == 0) std::sscanf(u.release, "%d.%d", &major, &minor);
    if (major < 5) GTEST_SKIP() << "kernel " << u.release << " < 5.0: UDP_GRO unavailable (UDP_SEGMENT needs 4.18)";
    EXPECT_EQ(rune_gso_platform_supported(), 3);
}

TEST(UdpGsoBindings, DisabledCreatesNothing) {
    EXPECT_EQ(rune_gso_bindings_create(0, 0), nullptr);
}

TEST(UdpGsoBindings, RoundTripAllSizesMtu1408UsesGsoAndGro) {
    Rig r;
    ASSERT_TRUE(r.start(1, 1408)) << aeron_errmsg();
    roundTrip(r, r, udp(24601, 1408), mixedSizes(1408, 140));
    r.close();
    EXPECT_GT(r.totals.gso_datagrams, 0u) << "the GSO path never ran";
    EXPECT_GT(r.totals.gro_datagrams, 0u) << "the GRO split never ran";
    EXPECT_EQ(r.totals.gso_fallbacks, 0u);
    EXPECT_EQ(r.totals.gro_ctrunc_drops, 0u);
}

TEST(UdpGsoBindings, RoundTripMtu8192) {
    Rig r;
    ASSERT_TRUE(r.start(1, 8192)) << aeron_errmsg();
    roundTrip(r, r, udp(24602, 8192), mixedSizes(8192, 70));
    r.close();
    EXPECT_GT(r.totals.gso_datagrams, 0u);
}

TEST(UdpGsoBindings, StockRigCountsNothing) {
    // GSO off (stock bindings): the same traffic, no rune_gso state at all — the default-off path is untouched.
    Rig r;
    ASSERT_TRUE(r.start(0, 1408)) << aeron_errmsg();
    roundTrip(r, r, udp(24609, 1408), mixedSizes(1408, 70));
    r.close();
    EXPECT_EQ(r.gso, nullptr);
    EXPECT_EQ(r.totals.gso_datagrams, 0u);
}

TEST(UdpGsoBindings, MixedPeersBothDirections) {
    {
        Rig gsoPub, plainSub;
        ASSERT_TRUE(gsoPub.start(1, 1408)) << aeron_errmsg();
        ASSERT_TRUE(plainSub.start(0, 1408)) << aeron_errmsg();
        roundTrip(gsoPub, plainSub, udp(24603, 1408), mixedSizes(1408, 70));
        gsoPub.close();
        EXPECT_GT(gsoPub.totals.gso_datagrams, 0u);
    }
    {
        Rig plainPub, groSub;
        ASSERT_TRUE(plainPub.start(0, 1408)) << aeron_errmsg();
        ASSERT_TRUE(groSub.start(1, 1408)) << aeron_errmsg();
        roundTrip(plainPub, groSub, udp(24604, 1408), mixedSizes(1408, 70));
        groSub.close();
        // The receiving socket ran with UDP_GRO on and delivered the stock burst byte-exact (roundTrip).  On
        // loopback the kernel never coalesces independently sent datagrams (GRO merging happens in a NIC's
        // NAPI path; lo only hands over GSO super-datagrams whole), so gro_datagrams stays 0 here by design.
        EXPECT_EQ(groSub.totals.gro_transports, 1u) << "GRO must be enabled on the subscriber's receive socket";
        EXPECT_EQ(groSub.totals.gro_trunc_drops + groSub.totals.gro_ctrunc_drops, 0u);
        EXPECT_EQ(groSub.totals.gso_sends, 0u) << "the GRO-side rig only subscribed";
    }
}

TEST(UdpGsoBindings, InjectedEagainIsRetriedWithoutLoss) {
    Rig r;
    ASSERT_TRUE(r.start(2, 1408, EAGAIN, 50)) << aeron_errmsg();
    roundTrip(r, r, udp(24605, 1408), mixedSizes(1408, 70));
    r.close();
    EXPECT_GT(r.totals.gso_datagrams, 0u) << "GSO must resume after the injected EAGAINs";
    EXPECT_EQ(r.totals.gso_fallbacks, 0u) << "EAGAIN is back-pressure, not 'unsupported'";
}

TEST(UdpGsoBindings, UnsupportedGsoFallsBackPerTransport) {
    Rig r;
    ASSERT_TRUE(r.start(2, 1408, EIO, 1)) << aeron_errmsg();
    roundTrip(r, r, udp(24606, 1408), mixedSizes(1408, 70));
    r.close();
    EXPECT_GE(r.totals.gso_fallbacks, 1u);
}

// DR-BUG-MTU (dr-e2e test_dr12_mtu_reduction_large_batch): after `ip link set mtu 1200` under the 1472-byte
// Aeron MTU the broker derived at boot, the kernel answers every sendmsg(UDP_SEGMENT) whose segment exceeds the
// path MTU with EMSGSIZE (measured, 5.15) — persistently, while a plain send of the same datagram is
// IP-fragmented and delivered.  EMSGSIZE was a send error: Aeron re-sent the identical GSO batch forever, every
// multi-fragment burst stalled, heartbeats kept the session connected, nothing was reported.  Persistent
// injection = the real condition.  RED before the fix: the round trip never completed (20 s deadline).
TEST(UdpGsoBindings, GsoSegmentAbovePathMtuFallsBackAndDeliversRegression) {
    Rig r;
    ASSERT_TRUE(r.start(2, 1408, EMSGSIZE, std::numeric_limits<int>::max())) << aeron_errmsg();
    roundTrip(r, r, udp(24617, 1408), mixedSizes(1408, 70));
    // Review B-I2: the live counter (driver still running).
    EXPECT_GE(counterByType(r.client, kGsoPathMtuCounterTypeId), 1) << "rune_gso_path_mtu_fallbacks must count live";
    r.close();
    EXPECT_GE(r.totals.gso_path_mtu_fallbacks, 1u) << "counted as path-MTU groups";
    EXPECT_EQ(r.totals.gso_fallbacks, 0u) << "a path-MTU refusal never turns GSO off for the transport (A-I3)";
}

// Sibling: the broker's egress shape (MDC dynamic control) under the same persistent EMSGSIZE.
TEST(UdpGsoBindings, MdcGsoSegmentAbovePathMtuFallsBackAndDeliversRegression) {
    Rig pub, sub;
    ASSERT_TRUE(pub.start(2, 1408, EMSGSIZE, std::numeric_limits<int>::max())) << aeron_errmsg();
    ASSERT_TRUE(sub.start(1, 1408)) << aeron_errmsg();
    const std::string pubCh = "aeron:udp?control=localhost:24618|control-mode=dynamic|mtu=1408";
    const std::string subCh = "aeron:udp?endpoint=localhost:24619|control=localhost:24618|control-mode=dynamic";
    roundTrip(pub, sub, pubCh, mixedSizes(1408, 70), subCh);
    pub.close();
    sub.close();
    EXPECT_GE(pub.totals.gso_path_mtu_fallbacks, 1u);
    EXPECT_EQ(pub.totals.gso_fallbacks, 0u) << "per destination, never per transport (A-I3)";
}

// Review A-I3: one EMSGSIZE (one destination's PMTU, or a transient MTU dip) must not turn GSO off for the
// transport — after the hold the destination is re-probed and GSO resumes.  Here the injected refusal is single;
// the next round trip after the hold period must use GSO again.  RED: the per-transport `gso_on = 0` (no GSO
// sends after the refusal, gso_fallbacks 1).
TEST(UdpGsoBindings, PathMtuFallbackKeepsGsoForLaterBatchesRegression) {
    Rig r;
    ASSERT_TRUE(r.start(2, 1408, EMSGSIZE, 1)) << aeron_errmsg();
    aeron_subscription_t* sub = addSub(r.client, udp(24620, 1408).c_str(), 1001);   // one image for both rounds
    ASSERT_NE(sub, nullptr) << aeron_errmsg();
    roundTrip(r, r, udp(24620, 1408), mixedSizes(1408, 70), "", sub);
    std::this_thread::sleep_for(std::chrono::milliseconds(10'500));   // past the 10 s per-destination hold
    roundTrip(r, r, udp(24620, 1408), mixedSizes(1408, 70), "", sub);
    r.close();
    EXPECT_EQ(r.totals.gso_fallbacks, 0u);
    EXPECT_EQ(r.totals.gso_path_mtu_fallbacks, 1u);
    EXPECT_GE(r.totals.gso_sends, 1u) << "GSO must resume for the destination after the hold";
}

// Review B-M4: the fallback when part of the batch was already sent as GSO (`done > 0`): the plain re-send covers
// exactly the rest — no short send, nothing re-sent.  skip = 1: the first GSO group goes through, then EMSGSIZE.
// RED: `return r < 0 ? r : (int)r;` (drops `done`): a short send, the prefix re-sent ("Short sends" > 0).
TEST(UdpGsoBindings, PathMtuFallbackAfterAPartlySentBatchDeliversExactlyOnceRegression) {
    Rig r;
    r.injectSkip = 1;
    ASSERT_TRUE(r.start(4, 1408, EMSGSIZE, std::numeric_limits<int>::max())) << aeron_errmsg();
    const std::size_t payload = 1408 - 32;
    std::vector<std::size_t> sizes;
    // 2 full frames + a short one: every message ends a GSO group, so a send call holding several messages
    // holds several groups — the second one fails with the first already sent (done > 0).
    for (int i = 0; i < 60; ++i) sizes.push_back(2 * payload + 500);
    const int64_t shortBefore = systemCounter(r.client, "Short sends");
    roundTrip(r, r, udp(24621, 1408), sizes);
    EXPECT_EQ(systemCounter(r.client, "Short sends"), shortBefore) << "the plain re-send must cover exactly the rest";
    r.close();
    EXPECT_GE(r.totals.gso_sends, 1u);
    EXPECT_GE(r.totals.gso_path_mtu_fallbacks, 1u);
}

// Review B-M3: a kernel that answers EINVAL for a GSO segment above the route MTU is diagnosed as a path-MTU
// condition (getsockopt IP_MTU), not "GSO unsupported".  Lowers the loopback MTU (needs CAP_NET_ADMIN): runs in
// the Linux gate (RUNE_LINUX_GATE=1, container with NET_ADMIN); never a silent skip there.  RED: EINVAL ->
// UNSUPPORTED (gso_fallbacks 1, gso_path_mtu_fallbacks 0).
TEST(UdpGsoBindings, EinvalAboveTheRouteMtuIsAPathMtuFallbackRegression) {
    const char* gate = std::getenv("RUNE_LINUX_GATE");
    if (gate == nullptr || std::string(gate) != "1") GTEST_SKIP() << "Linux gate only (needs CAP_NET_ADMIN)";
    LoopbackMtu lo(1300);
    ASSERT_TRUE(lo.ok) << "RUNE_LINUX_GATE=1 but the loopback MTU could not be lowered (CAP_NET_ADMIN?)";
    Rig r;
    ASSERT_TRUE(r.start(2, 1408, EINVAL, std::numeric_limits<int>::max())) << aeron_errmsg();
    roundTrip(r, r, udp(24622, 1408), mixedSizes(1408, 40));
    r.close();
    EXPECT_GE(r.totals.gso_path_mtu_fallbacks, 1u);
    EXPECT_EQ(r.totals.gso_fallbacks, 0u);
}

TEST(UdpGsoBindings, DataLossRecoversWithGroOn) {
    // Aeron's loss interceptor drops ~5% of DATA frames (type 0x01 -> mask bit 0x2) on receive; NAKs recover.
    ::setenv("AERON_UDP_CHANNEL_INCOMING_INTERCEPTORS", "loss", 1);
    ::setenv("AERON_UDP_CHANNEL_TRANSPORT_BINDINGS_LOSS_ARGS", "rate=0.05|recv-msg-mask=0x2", 1);
    Rig r;
    const bool ok = r.start(1, 1408);
    ::unsetenv("AERON_UDP_CHANNEL_INCOMING_INTERCEPTORS");
    ::unsetenv("AERON_UDP_CHANNEL_TRANSPORT_BINDINGS_LOSS_ARGS");
    ASSERT_TRUE(ok) << aeron_errmsg();
    roundTrip(r, r, udp(24607, 1408), mixedSizes(1408, 70));
    EXPECT_GT(systemCounter(r.client, "Retransmits sent"), 0) << "the loss interceptor never dropped anything";
    r.close();
    EXPECT_GT(r.totals.gro_datagrams, 0u);
}

TEST(UdpGsoBindings, Ipv6RoundTrip) {
    Rig r;
    ASSERT_TRUE(r.start(1, 1408)) << aeron_errmsg();
    roundTrip(r, r, "aeron:udp?endpoint=[::1]:24608|mtu=1408", mixedSizes(1408, 70));
    r.close();
    EXPECT_GT(r.totals.gso_datagrams, 0u);
}

// Broker wiring (plan Task 3): the embedded driver installs the bindings by default and raises Aeron's
// msgs-per-send 2 -> 16 so a burst forms GSO groups; RUNE_AERON_UDP_GSO=0 (udpGso=false) is stock Aeron.
TEST(UdpGsoBindings, EmbeddedDriverInstallsGsoByDefault) {
    rune::node::EmbeddedDriverConfig on;
    on.dir = "/dev/shm/rune-gso-ed-on-" + std::to_string(::getpid());
    on.idleStrategy = "backoff";
    on.mtuBytes = 1408;   // a network MTU: GSO groups MTU datagrams -> 16 msgs/send
    rune::node::EmbeddedDriver a(on, rune::node::DriverMode::Threaded);
    EXPECT_TRUE(a.udpGsoActive());
    EXPECT_EQ(a.maxMessagesPerSend(), 16u);
    a.close();
    EXPECT_TRUE(a.udpGsoActive()) << "stays reportable after close";

    rune::node::EmbeddedDriverConfig off = on;
    off.dir += "-off";
    off.udpGso = false;
    rune::node::EmbeddedDriver b(off, rune::node::DriverMode::Threaded);
    EXPECT_FALSE(b.udpGsoActive());
    EXPECT_EQ(b.maxMessagesPerSend(), 2u) << "GSO off: Aeron's default batching, unchanged";

    rune::node::EmbeddedDriverConfig explicitCfg = on;
    explicitCfg.dir += "-explicit";
    explicitCfg.maxMessagesPerSend = 4;
    rune::node::EmbeddedDriver c(explicitCfg, rune::node::DriverMode::Threaded);
    EXPECT_TRUE(c.udpGsoActive());
    EXPECT_EQ(c.maxMessagesPerSend(), 4u) << "an explicit RUNE_AERON_MAX_MSGS_PER_SEND wins";
}

// Fix round 1 (review M1): GRO only on RECEIVER-affinity sockets.  The sender agent polls its control
// sockets with mtu-sized buffers: a GRO-merged burst of status messages / NAKs would be truncated.
TEST(UdpGsoBindings, GroNeverEnabledOnTheSenderControlSocketRegression) {
    Rig r;
    ASSERT_TRUE(r.start(1, 1408)) << aeron_errmsg();
    roundTrip(r, r, udp(24610, 1408), mixedSizes(1408, 14));
    r.close();
    EXPECT_EQ(r.totals.transports, 2u) << "one send endpoint + one receive destination";
    EXPECT_EQ(r.totals.gro_transports, 1u) << "GRO on the receive destination only, never the send endpoint";
    EXPECT_GT(r.totals.gro_datagrams, 0u);
}

// Re-review M1: a truncated GRO buffer delivers its complete leading segments and drops only the damaged tail
// (Aeron NAKs recover it); GRO is NEVER turned off — once disabled, the kernel stops attaching the segment-size
// cmsg to buffers already coalesced in the queue and each would reach Aeron as one datagram (corruption).
// Delivery stays byte-exact and GRO keeps splitting after the truncation.
TEST(UdpGsoBindings, TruncatedGroBufferDropsTailNeverDisablesGroRegression) {
    Rig r;
    ASSERT_TRUE(r.start(3, 1408, 0, 1)) << aeron_errmsg();
    roundTrip(r, r, udp(24611, 1408), mixedSizes(1408, 70));
    r.close();
    EXPECT_EQ(r.totals.gro_trunc_drops, 1u);
    EXPECT_EQ(r.totals.gro_disabled, 0u) << "GRO stays on after a MSG_TRUNC";
    EXPECT_EQ(r.totals.gro_transports, 1u);
    EXPECT_GT(r.totals.gro_datagrams, 0u) << "GRO keeps coalescing/splitting after the truncation";
}

// Fix round 1 (review L6): ENOBUFS on a GSO send is handled like stock sendmmsg (a send error; the batch is
// re-sent from snd-pos) — never a GSO fallback, nothing lost, GSO resumes.
TEST(UdpGsoBindings, InjectedEnobufsIsRetriedWithoutLossRegression) {
    Rig r;
    ASSERT_TRUE(r.start(2, 1408, ENOBUFS, 20)) << aeron_errmsg();
    roundTrip(r, r, udp(24612, 1408), mixedSizes(1408, 70));
    r.close();
    EXPECT_GT(r.totals.gso_datagrams, 0u) << "GSO must resume after the injected ENOBUFS";
    EXPECT_EQ(r.totals.gso_fallbacks, 0u) << "ENOBUFS is back-pressure, not 'unsupported'";
}

// Fix round 1 (review gap 7): multi-destination-cast with dynamic control (the broker's egress shape):
// the publication sends to every subscriber that registered on its control endpoint.
TEST(UdpGsoBindings, MdcDynamicControlRoundTripUsesGsoAndGro) {
    Rig r;
    ASSERT_TRUE(r.start(1, 1408)) << aeron_errmsg();
    roundTrip(r, r, "aeron:udp?control=localhost:24613|control-mode=dynamic|mtu=1408", mixedSizes(1408, 70),
              "aeron:udp?endpoint=localhost:24614|control=localhost:24613|mtu=1408");
    r.close();
    EXPECT_GT(r.totals.gso_datagrams, 0u);
    EXPECT_GT(r.totals.gro_datagrams, 0u);
    EXPECT_EQ(r.totals.gso_fallbacks, 0u);
}

// Fix round 1 (review L3): multi-destination subscription — the receive destination is added AND removed on
// the receiver agent (aeron_driver_receiver.c on_remove_destination closes it there); the totals of a
// transport closed off the conductor are still collected.
TEST(UdpGsoBindings, MdsDestinationRemovedOnTheReceiverIsCounted) {
    Rig r;
    ASSERT_TRUE(r.start(1, 1408)) << aeron_errmsg();
    aeron_subscription_t* sub = addSub(r.client, "aeron:udp?control-mode=manual", 1001);
    ASSERT_NE(sub, nullptr) << aeron_errmsg();
    const std::string dest = "aeron:udp?endpoint=localhost:24615";   // the publication carries mtu=1408
    aeron_async_destination_t* ad = nullptr;
    ASSERT_EQ(aeron_subscription_async_add_destination(&ad, r.client, sub, dest.c_str()), 0) << aeron_errmsg();
    int addResult = 0;
    ASSERT_TRUE(pollUntil([&] { return (addResult = aeron_subscription_async_destination_poll(ad)) != 0; }));
    ASSERT_EQ(addResult, 1) << aeron_errmsg();
    roundTrip(r, r, udp(24615, 1408), mixedSizes(1408, 70), "", sub);
    aeron_async_destination_t* rd = nullptr;
    ASSERT_EQ(aeron_subscription_async_remove_destination(&rd, r.client, sub, dest.c_str()), 0) << aeron_errmsg();
    int removeResult = 0;
    ASSERT_TRUE(pollUntil([&] { return (removeResult = aeron_subscription_async_destination_poll(rd)) != 0; }));
    ASSERT_EQ(removeResult, 1) << aeron_errmsg();
    r.close();
    EXPECT_GT(r.totals.gro_datagrams, 0u) << "the receiver-closed destination's GRO counts were lost";
    EXPECT_GE(r.totals.transports, 2u);
}

// Fix round 1 (review M2): at the loopback-derived 64 KiB MTU two datagrams never fit one GSO send, so the
// driver keeps Aeron's 2 msgs/send (GSO/GRO bindings still installed: GRO still helps the receiver).
TEST(UdpGsoBindings, LoopbackMtuKeepsAeronDefaultMsgsPerSendRegression) {
    rune::node::EmbeddedDriverConfig c;
    c.dir = "/dev/shm/rune-gso-ed-64k-" + std::to_string(::getpid());
    c.idleStrategy = "backoff";
    c.mtuBytes = 65504;
    rune::node::EmbeddedDriver d(c, rune::node::DriverMode::Threaded);
    EXPECT_TRUE(d.udpGsoActive());
    EXPECT_EQ(d.maxMessagesPerSend(), 2u);
}
