// tests/transport/AeronTransportGsoTest.cpp — UDP GSO/GRO bindings in the client's embedded driver (Linux).
// rune-broker spec 2026-10-03-aeron-udp-gso-gro-design.md; component: rune-protocol rune_gso_bindings.
//
// Real driver, UDP loopback (ingress == egress channel + stream, so offer() -> poll() echoes through the
// kernel), MTU 1408: messages larger than the MTU fragment into runs of equal-size datagrams, which the
// bindings send as one sendmsg(UDP_SEGMENT) per run and receive as GRO-coalesced buffers split back into
// datagrams.  The totals prove both paths actually ran; udpGso=false proves the stock path is unchanged.
// Polls with a deadline + sched_yield (tests never sleep).
#include <rune/client/transport/AeronTransport.h>

#include <gtest/gtest.h>

#include <sched.h>
#include <unistd.h>

#include <cerrno>
#include <chrono>
#include <cstdint>
#include <string>
#include <vector>

namespace {
using Transport = rune::client::transport::AeronTransport;
using Config    = rune::client::transport::TransportConfig;
constexpr auto kDeadline = std::chrono::seconds(20);

int g_seq = 0;

int freeUdpPort() {
    return 25000 + static_cast<int>((::getpid() * 7 + (++g_seq) * 13) % 20000);
}

Config udpLoopbackConfig(const char* tag, int32_t streamId) {
    Config cfg;
    const std::string ch = "aeron:udp?endpoint=127.0.0.1:" + std::to_string(freeUdpPort());
    cfg.ingressChannel  = ch;
    cfg.ingressStreamId = streamId;
    cfg.egressChannel   = ch;
    cfg.egressStreamId  = streamId;
    cfg.mtuLength       = 1408;
    cfg.dataDir = std::string("/tmp/rune_gso_client_") + tag + "_" + std::to_string(static_cast<long>(::getpid())) +
                  "_" + std::to_string(g_seq);
    return cfg;
}

template <class F>
bool pollUntil(F&& done) {
    const auto end = std::chrono::steady_clock::now() + kDeadline;
    while (!done()) {
        if (std::chrono::steady_clock::now() > end) return false;
        sched_yield();
    }
    return true;
}

uint8_t patternByte(std::size_t msg, std::size_t i) { return static_cast<uint8_t>((msg * 131u + i * 7u) ^ (i >> 8)); }

// Echo `count` messages of mixed sizes (1 B .. 40 KiB: single datagrams and long fragment runs) through the
// transport; every message must come back once, in order, byte-exact.
void echoMixed(Transport& t, int count) {
    ASSERT_TRUE(pollUntil([&] { return t.ingressConnected() && t.egressConnected(); })) << "never connected";
    const std::size_t payload = 1408 - 32;
    const std::size_t sizes[] = {1, 100, payload, payload + 1, 3 * payload, 29 * payload + 7, 40 * 1024};
    std::size_t next = 0;
    bool corrupt = false;
    std::vector<std::size_t> sent;
    auto handler = [&](const uint8_t* buf, std::size_t len) {
        if (next >= sent.size() || len != sent[next]) { corrupt = true; ++next; return; }
        for (std::size_t i = 0; i < len; ++i)
            if (buf[i] != patternByte(next, i)) { corrupt = true; break; }
        ++next;
    };
    std::vector<uint8_t> buf;
    for (int m = 0; m < count; ++m) {
        const std::size_t len = sizes[m % 7];
        buf.resize(len);
        for (std::size_t i = 0; i < len; ++i) buf[i] = patternByte(static_cast<std::size_t>(m), i);
        sent.push_back(len);
        ASSERT_TRUE(pollUntil([&] {
            t.poll(handler, 64);
            return t.offer(buf.data(), buf.size());
        })) << "offer of message " << m << " never accepted";
    }
    ASSERT_TRUE(pollUntil([&] {
        t.poll(handler, 64);
        return next >= sent.size();
    })) << "echoed " << next << " of " << sent.size();
    EXPECT_FALSE(corrupt);
    EXPECT_EQ(next, sent.size());
}
} // namespace

TEST(AeronTransportGso, UdpEchoUsesGsoSendAndGroReceiveByDefault) {
    Transport t{udpLoopbackConfig("on", 2101)};
    t.start();
    EXPECT_TRUE(t.udpGsoActive());
    echoMixed(t, 140);
    t.stop();
    const auto tot = t.gsoTotals();
    EXPECT_GT(tot.gso_sends, 0u) << "the GSO send path never ran";
    EXPECT_GT(tot.gso_datagrams, tot.gso_sends) << "GSO sends must carry > 1 datagram each";
    EXPECT_GT(tot.gro_datagrams, 0u) << "the GRO split never ran";
    EXPECT_EQ(tot.gso_fallbacks, 0u);
    EXPECT_EQ(tot.gro_ctrunc_drops, 0u);
    EXPECT_TRUE(t.udpGsoActive()) << "stays reportable after stop";
}

TEST(AeronTransportGso, GsoOffIsStockAeronAndStillEchoes) {
    auto cfg = udpLoopbackConfig("off", 2102);
    cfg.udpGso = false;
    Transport t{cfg};
    t.start();
    EXPECT_FALSE(t.udpGsoActive());
    echoMixed(t, 70);
    t.stop();
    const auto tot = t.gsoTotals();
    EXPECT_EQ(tot.gso_sends, 0u);
    EXPECT_EQ(tot.gro_datagrams, 0u);
}

TEST(AeronTransportGso, ExplicitMaxMessagesPerSendStillUsesGso) {
    auto cfg = udpLoopbackConfig("m4", 2103);
    cfg.maxMessagesPerSend = 4;   // explicit value wins over the GSO default of 16; GSO groups <= 4
    Transport t{cfg};
    t.start();
    echoMixed(t, 70);
    t.stop();
    const auto tot = t.gsoTotals();
    EXPECT_GT(tot.gso_sends, 0u);
    EXPECT_LE(tot.gso_datagrams, 4u * tot.gso_sends) << "a GSO group never exceeds msgs-per-send";
}

TEST(AeronTransportGso, UnsupportedGsoFallsBackToSendmmsgWithoutLoss) {
    // The first GSO sendmsg fails as if the route/device could not segment (EIO): that transport turns GSO
    // off and re-sends through Aeron's stock sendmmsg; every message still arrives byte-exact.
    Transport t{udpLoopbackConfig("eio", 2104)};
    t.injectGsoSendErrorsForTest(EIO, 1);
    t.start();
    echoMixed(t, 70);
    t.stop();
    EXPECT_GE(t.gsoTotals().gso_fallbacks, 1u);
}
