// Real Aeron driver tests for rune_gso_bindings (Linux): spec 2026-10-03-aeron-udp-gso-gro-design.md §5,
// plan Review Focus 1-5.  Each rig = one embedded driver (SHARED threading, own dir) + an Aeron C client.
#include <gtest/gtest.h>

#include <cstddef>   // aeronmd.h uses size_t without including <stddef.h>
extern "C" {
#include <aeronmd/aeronmd.h>
#include <aeron/aeronc.h>
#include "rune/protocol/net/rune_gso_bindings.h"
}

#include "rune/node/EmbeddedDriver.h"

#include <sched.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <string>
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

    // gsoMode: 0 = stock Aeron, 1 = GSO+GRO, 2 = GSO+GRO with an injected sendmsg errno (inject before start).
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
void roundTrip(Rig& pubRig, Rig& subRig, const std::string& channel, const std::vector<std::size_t>& sizes) {
    aeron_subscription_t* sub = addSub(subRig.client, channel.c_str(), 1001);
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
std::string udp(int port, std::size_t mtu, const char* host = "localhost") {
    return "aeron:udp?endpoint=" + std::string(host) + ":" + std::to_string(port) + "|mtu=" + std::to_string(mtu);
}
} // namespace

TEST(UdpGsoBindings, PlatformSupportsBothOnTheBenchKernel) {
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
