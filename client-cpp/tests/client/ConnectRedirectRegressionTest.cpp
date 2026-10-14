// tests/client/ConnectRedirectRegressionTest.cpp
//
// Follower-connect hang (2026-10-14, follower-connect-hang.md): the SEED's
// connect-time Redirect.
//
//   1. A gateway answering the Connect with ErrorResponse{Redirect, <other
//      gateway's channel>} is FOLLOWED: the seed retargets to that channel and
//      connects there.  RED before the fix: the seed's SessionEndpointIo
//      retarget was a no-op, so every resend went back to the redirecting
//      gateway until the redirect budget ran out (6 Connects, 0 on the target).
//   2. A connect whose seed GIVES UP (redirect budget exhausted) ends at once,
//      explicitly NotConnected — never waits out the connect deadline.  RED
//      before the fix: Disconnected was silent, the connect completed Timeout
//      only at connectTimeoutMs.
//
// Two fake gateways (one Aeron client on the Client's embedded driver), each
// subscribed on its own UDP ingress endpoint, same stream id; both answer on
// the Client's ipc egress.

#include "../support/AeronTestErrors.h"
#include "../support/TestCompletions.h"
#include <rune/client/Client.h>
#include <rune/client/codec/Codec.h>

#include <Aeron.h>
#include <ExclusivePublication.h>
#include <FragmentAssembler.h>
#include <Subscription.h>

#include <gtest/gtest.h>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <filesystem>
#include <memory>
#include <string>
#include <thread>

using namespace std::chrono_literals;
using namespace rune::client;
namespace rcc = rune::client::codec;

namespace {

constexpr int32_t kInStream = 4310;
constexpr int32_t kEgStream = 4311;

std::string uniqueDir(const char* prefix) {
    static std::atomic<int> ctr{0};
    return (std::filesystem::temp_directory_path() /
            (std::string(prefix) + "_" + std::to_string(::getpid()) + "_" + std::to_string(ctr++))).string();
}

/// A free loopback UDP port (bound once, released).
int freeUdpPort() {
    const int fd = ::socket(AF_INET, SOCK_DGRAM, 0);
    sockaddr_in a{};
    a.sin_family      = AF_INET;
    a.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    a.sin_port        = 0;
    ::bind(fd, reinterpret_cast<sockaddr*>(&a), sizeof a);
    socklen_t l = sizeof a;
    ::getsockname(fd, reinterpret_cast<sockaddr*>(&a), &l);
    ::close(fd);
    return ntohs(a.sin_port);
}

/// Gateway A answers every Connect with Redirect{redirectTo}; gateway B (if
/// any) accepts every Connect.  Counts the Connects each one saw.
struct TwoGateways {
    std::string chanA, chanB, redirectTo;
    int connectsA = 0, connectsB = 0;

    std::shared_ptr<aeron::Aeron>                aeron;
    std::shared_ptr<aeron::Subscription>         subA, subB;
    std::shared_ptr<aeron::ExclusivePublication> pub;
    std::unique_ptr<aeron::FragmentAssembler>    asmA, asmB;
    uint8_t reply[2048]{};

    TwoGateways(const std::string& aeronDir, std::string a, std::string b, std::string to)
        : chanA(std::move(a)), chanB(std::move(b)), redirectTo(std::move(to)) {
        aeron::Context ctx;
        ctx.errorHandler(test_support::countAeronError);
        ctx.aeronDir(aeronDir);
        aeron = aeron::Aeron::connect(ctx);
        const int64_t ia = aeron->addSubscription(chanA, kInStream);
        const int64_t ib = aeron->addSubscription(chanB, kInStream);
        const int64_t ip = aeron->addExclusivePublication("aeron:ipc", kEgStream);
        for (const auto dl = std::chrono::steady_clock::now() + 5s; std::chrono::steady_clock::now() < dl;) {
            if (!subA) subA = aeron->findSubscription(ia);
            if (!subB) subB = aeron->findSubscription(ib);
            if (!pub) pub = aeron->findExclusivePublication(ip);
            if (subA && subB && pub) break;
            std::this_thread::sleep_for(2ms);
        }
        if (!subA || !subB || !pub) throw std::runtime_error("TwoGateways: sub/pub never resolved");
        asmA = std::make_unique<aeron::FragmentAssembler>(
            [this](aeron::concurrent::AtomicBuffer& b, aeron::util::index_t o, aeron::util::index_t l, aeron::Header&) {
                onConnect(/*isA=*/true, reinterpret_cast<const uint8_t*>(b.buffer() + o), static_cast<std::size_t>(l));
            });
        asmB = std::make_unique<aeron::FragmentAssembler>(
            [this](aeron::concurrent::AtomicBuffer& b, aeron::util::index_t o, aeron::util::index_t l, aeron::Header&) {
                onConnect(/*isA=*/false, reinterpret_cast<const uint8_t*>(b.buffer() + o), static_cast<std::size_t>(l));
            });
    }
    ~TwoGateways() { asmA.reset(); asmB.reset(); pub.reset(); subA.reset(); subB.reset(); aeron.reset(); }

    void poll() {
        subA->poll(asmA->handler(), 16);
        subB->poll(asmB->handler(), 16);
    }
    void send(std::size_t n) {
        if (n == 0) return;
        aeron::concurrent::AtomicBuffer ab(reply, static_cast<aeron::util::index_t>(n));
        for (int i = 0; i < 500 && pub->offer(ab, 0, static_cast<aeron::util::index_t>(n)) <= 0; ++i)
            std::this_thread::sleep_for(1ms);
    }
    void onConnect(bool isA, const uint8_t* buf, std::size_t len) {
        const auto tid = rcc::peekTemplate(buf, len);
        if (!tid || static_cast<rcc::TemplateId>(*tid) != rcc::TemplateId::Connect) return;
        const auto req = rcc::decodeConnectRequest(buf, len);
        if (!req) return;
        if (isA) {
            ++connectsA;
            send(rcc::encodeErrorResponse(reply, sizeof reply, static_cast<int16_t>(rcc::ErrorCode::Redirect),
                                          redirectTo, 0, req->correlationId));
        } else {
            ++connectsB;
            send(rcc::encodeConnectResponse(reply, sizeof reply, static_cast<int16_t>(rcc::ErrorCode::OK),
                                            /*leaderMemberId=*/2, /*assignedSessionId=*/77, req->sessionId,
                                            req->correlationId));
        }
    }
};

ClientConfig clientConfig(const std::string& dir, const std::string& seed, uint32_t connectTimeoutMs) {
    ClientConfig cfg;
    cfg.gatewayChannel   = seed;
    cfg.gatewayStreamId  = kInStream;
    cfg.egressChannel    = "aeron:ipc";
    cfg.egressStreamId   = kEgStream;
    cfg.dataDir          = dir;
    cfg.connectTimeoutMs = connectTimeoutMs;
    cfg.idleMode         = IdleMode::LowLatency;
    return cfg;
}

} // namespace

TEST(ConnectRedirectRegression, TheSeedFollowsAConnectTimeRedirectToTheNamedGatewayRegression) {
    const std::string dir = uniqueDir("rune_conn_redirect");
    const std::string a = "aeron:udp?endpoint=127.0.0.1:" + std::to_string(freeUdpPort());
    const std::string b = "aeron:udp?endpoint=127.0.0.1:" + std::to_string(freeUdpPort());
    auto client = std::make_unique<Client>(clientConfig(dir, a, 8'000));
    std::this_thread::sleep_for(150ms);   // the embedded driver is up before the gateways attach
    TwoGateways gw(dir + "/aeron", a, b, /*redirectTo=*/b);

    test_support::TestCq cq(*client);
    auto fut = cq.issue([&](Completions& q, Token t) { return client->connect(q, t); });
    for (const auto dl = std::chrono::steady_clock::now() + 12s; !fut.ready() && std::chrono::steady_clock::now() < dl;) {
        gw.poll();
        std::this_thread::sleep_for(2ms);
    }
    ASSERT_TRUE(fut.ready()) << "the connect neither completed nor failed (A saw " << gw.connectsA << ", B saw "
                             << gw.connectsB << ")";
    EXPECT_NO_THROW(fut.check()) << "A saw " << gw.connectsA << " Connects, B saw " << gw.connectsB;
    EXPECT_EQ(gw.connectsA, 1) << "one Connect to the redirecting gateway, never resent there";
    EXPECT_GE(gw.connectsB, 1) << "the Connect reached the gateway the Redirect named";
    client->close();
    std::error_code ec;
    std::filesystem::remove_all(dir, ec);
}

TEST(ConnectRedirectRegression, AConnectWhoseRedirectBudgetRunsOutEndsNotConnectedAtOnceRegression) {
    const std::string dir = uniqueDir("rune_conn_redirect_loop");
    const std::string a = "aeron:udp?endpoint=127.0.0.1:" + std::to_string(freeUdpPort());
    const std::string b = "aeron:udp?endpoint=127.0.0.1:" + std::to_string(freeUdpPort());
    // A long connect timeout: the explicit end must come from the exhausted
    // budget, not from the deadline.
    auto client = std::make_unique<Client>(clientConfig(dir, a, 30'000));
    std::this_thread::sleep_for(150ms);
    TwoGateways gw(dir + "/aeron", a, b, /*redirectTo=*/a);   // A redirects to itself, forever

    test_support::TestCq cq(*client);
    const auto t0 = std::chrono::steady_clock::now();
    auto fut = cq.issue([&](Completions& q, Token t) { return client->connect(q, t); });
    for (const auto dl = t0 + 40s; !fut.ready() && std::chrono::steady_clock::now() < dl;) {
        gw.poll();
        std::this_thread::sleep_for(2ms);
    }
    ASSERT_TRUE(fut.ready()) << "the connect neither completed nor failed";
    const auto took = std::chrono::steady_clock::now() - t0;
    EXPECT_EQ(fut.status(), OpStatus::NotConnected) << "explicit NotConnected, got: " << fut.describe();
    EXPECT_LT(took, 10s) << "ends when the budget runs out, not at the 30 s connect deadline";
    EXPECT_EQ(gw.connectsB, 0);
    EXPECT_GE(gw.connectsA, 2) << "the redirect loop was followed until the budget ran out";
    client->close();
    std::error_code ec;
    std::filesystem::remove_all(dir, ec);
}
