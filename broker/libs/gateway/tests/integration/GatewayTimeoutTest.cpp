// GatewayTimeoutTest.cpp — BR-5 Part 1: session keepalive-timeout reaping tests.
//
// All tests use the injected nowNs clock (no wall-time waits for reaping).
// The ipc harness is still required because ClientGateway needs a live Aeron
// subscription to construct; however, reapExpired(nowNs) is driven purely via
// the manually-advanced nowNs values — real time is irrelevant to reap logic.
//
// Scenarios:
//  BR5T-1:  just-before boundary — session present (nowNs < t0+timeout)
//  BR5T-2:  just-after boundary  — session reaped  (nowNs > t0+timeout)
//  BR5T-3:  keepalive refresh keeps session alive past t0+timeout
//  BR5T-4:  multiple sessions — only expired ones reaped
//  BR5T-5:  reaped session's Ping gets no Pong (session gone)
//  BR5T-6:  reap loop is bounded (single-writer, finishes in O(n))

#include <rune/gateway/ClientGateway.h>
#include <rune/protocol/client/ControlMessages.h>
#include <rune/protocol/client/DataMessages.h>
#include <rune/protocol/client/GroupMessages.h>
#include <rune/node/WorkerOp.h>
#include <rune/node/WorkerPool.h>
#include <rune/metadata/ShardRouter.h>
#include <rune/protocol/client/PingMessages.h>
#include <rune/protocol/client/Protocol.h>
#include <rune/protocol/client/MessageHeader.h>

#include <Aeron.h>
#include <Publication.h>
#include <Subscription.h>
#include <FragmentAssembler.h>
#include <concurrent/AtomicBuffer.h>

#include <gtest/gtest.h>

extern "C" {
#include <aeronmd/aeronmd.h>
}

#include <atomic>
#include <chrono>
#include <filesystem>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

namespace fs = std::filesystem;
using namespace std::chrono_literals;
namespace pc = rune::protocol::client;

namespace rune::gateway {
namespace {

// ---------------------------------------------------------------------------
// Unique temp-path generator (getpid + atomic counter, distinct from BR-4)
// ---------------------------------------------------------------------------
static fs::path makeTempDir(std::string_view tag = "") {
    static std::atomic<uint32_t> g_seq{0};
    auto seq  = g_seq.fetch_add(1, std::memory_order_relaxed);
    auto name = std::string("rune_gwto_") + std::string(tag) +
                std::to_string(static_cast<long>(::getpid())) +
                "_" + std::to_string(seq);
    return fs::temp_directory_path() / name;
}

struct TempDir {
    fs::path path;
    explicit TempDir(std::string_view tag = "") : path(makeTempDir(tag)) {}
    ~TempDir() {
        std::error_code ec;
        fs::remove_all(path, ec);
    }
};

// Wall-clock nowNs helper (only used for pumping Aeron, not for reap logic).
static int64_t wallNowNs() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count();
}

// ---------------------------------------------------------------------------
// GatewayTimeoutFixture — embedded driver + Aeron client.
// Gateway is constructed with a SHORT timeout (100 ms expressed in ns) so
// tests can advance the injected clock by a small integer offset rather than
// waiting 10 seconds of real time.
// ---------------------------------------------------------------------------
class GatewayTimeoutFixture : public ::testing::Test {
public:
    static constexpr int32_t kIngressStream = 301;
    static constexpr int32_t kEgressBase    = 400;
    static constexpr int32_t kThisNodeId    = 9;
    static constexpr int     kPumpIter      = 50'000;

    // Short timeout used for all reap tests: 100 ms = 100_000_000 ns.
    static constexpr int64_t kTimeoutNs = 100'000'000LL; // 100 ms

    void SetUp() override {
        tmpDir_ = std::make_unique<TempDir>("to_");
        startDriver();
        connectAeron();

        // In invoker mode the gateway's constructor (which blocks on its ingress subscription) needs the conductor
        // run for it: a helper thread invokes it for exactly that call, then is joined (single caller at a time).
        // The helper is bounded (iteration cap + 30 s, above the client's 10 s driver timeout that bounds the ctor's
        // resolve) and is joined on every exit, a throwing ctor included (a joinable std::thread would terminate).
        std::atomic<bool> ctorDone{false};
        std::thread invoker;
        if (useInvoker_) invoker = std::thread([&] {
            const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds{30};
            for (int i = 0; i < 300'000 && !ctorDone.load(std::memory_order_acquire) &&
                            std::chrono::steady_clock::now() < deadline; ++i) {
                aeron_->conductorAgentInvoker().invoke();
                std::this_thread::sleep_for(std::chrono::microseconds{100});
            }
        });
        const auto joinInvoker = [&] {
            ctorDone.store(true, std::memory_order_release);
            if (invoker.joinable()) invoker.join();
        };
        try {
            // Gateway with a 100 ms session timeout.
            gw_ = std::make_unique<ClientGateway>(
                aeron_,
                "aeron:ipc",
                kIngressStream,
                kThisNodeId,
                kTimeoutNs);
        } catch (...) {
            joinInvoker();
            throw;
        }
        joinInvoker();
    }

    void TearDown() override {
        gw_.reset();
        aeron_.reset();
        stopDriver();
        tmpDir_.reset();
    }

    // -----------------------------------------------------------------------
    // TestClient — ingress Publication + egress Subscription.
    // -----------------------------------------------------------------------
    struct TestClient {
        std::shared_ptr<aeron::Publication>   ingressPub;
        std::shared_ptr<aeron::Subscription>  egressSub;
        int32_t                               egressStreamId;
        std::string                           egressChannel{"aeron:ipc"};
    };

    TestClient makeClient(int32_t egressStreamId) {
        int64_t pubRegId = aeron_->addPublication("aeron:ipc", kIngressStream);
        int64_t subRegId = aeron_->addSubscription("aeron:ipc", egressStreamId);

        std::shared_ptr<aeron::Publication>  pub;
        std::shared_ptr<aeron::Subscription> sub;

        pump([&]() {
            if (!pub) pub = aeron_->findPublication(pubRegId);
            if (!sub) sub = aeron_->findSubscription(subRegId);
            return pub && sub;
        }, nullptr, nullptr, kPumpIter, "makeClient");

        return TestClient{pub, sub, egressStreamId};
    }

    static void offerBounded(aeron::Publication& pub,
                             const uint8_t* buf, std::size_t len,
                             std::string_view label = "offer") {
        aeron::concurrent::AtomicBuffer ab(
            const_cast<uint8_t*>(buf),
            static_cast<aeron::util::index_t>(len));
        bool sent = false;
        for (int i = 0; i < kPumpIter && !sent; ++i) {
            if (pub.offer(ab, 0, static_cast<aeron::util::index_t>(len)) > 0)
                sent = true;
            else
                std::this_thread::sleep_for(std::chrono::microseconds{100});
        }
        ASSERT_TRUE(sent) << "offerBounded timed out: " << label;
    }

    template <typename DoneFn>
    void pump(DoneFn&& done,
              aeron::Subscription*       clientSub = nullptr,
              aeron::FragmentAssembler*  clientAsm = nullptr,
              int                        maxIter   = kPumpIter,
              std::string_view           label     = "pump") {
        using clock = std::chrono::steady_clock;
        const auto deadline = clock::now() + std::chrono::seconds{10};

        for (int i = 0; i < maxIter && clock::now() < deadline; ++i) {
            invokeConductor();
            if (gw_) gw_->doWork(wallNowNs());
            if (clientSub && clientAsm)
                clientSub->poll(clientAsm->handler(), 10);
            if (done()) return;
            std::this_thread::sleep_for(std::chrono::microseconds{100});
        }
        ASSERT_TRUE(done()) << "pump timed out: " << label;
    }

    // -----------------------------------------------------------------------
    // doConnect — send ConnectRequest; wait for ConnectResponse.
    // Returns the injected nowNs used as lastSeenNs for the session.
    // -----------------------------------------------------------------------
    struct ConnectResult {
        pc::ConnectResponse resp;
        int64_t             connectNs;  // the nowNs passed to doWork during connect
    };

    ConnectResult doConnect(TestClient& client, int64_t correlationId,
                            int64_t connectNs = 0) {
        pc::ConnectRequest req;
        req.sessionId       = 0;
        req.correlationId   = correlationId;
        req.egressStreamId  = client.egressStreamId;
        req.clientVersion   = 1;
        req.egressChannel   = client.egressChannel;
        uint8_t sc[512] = {};
        const std::size_t len = req.encode(sc);

        pump([&]() { return client.ingressPub->isConnected(); },
             nullptr, nullptr, kPumpIter, "waitPubConnected");

        offerBounded(*client.ingressPub, sc, len, "ConnectRequest");

        // If caller supplied a specific nowNs, drive doWork with that clock;
        // otherwise use the wall clock (the Connect path just needs to proceed).
        std::optional<pc::ConnectResponse> resp;
        aeron::FragmentAssembler asm_([&](aeron::concurrent::AtomicBuffer& buf,
                                          aeron::util::index_t offset,
                                          aeron::util::index_t blen,
                                          aeron::Header&) {
            auto dec = pc::ConnectResponse::decode(
                reinterpret_cast<const uint8_t*>(buf.buffer() + offset),
                static_cast<std::size_t>(blen));
            if (dec) resp = *dec;
        });

        // Pump with the specified connectNs so the session's lastSeenNs is
        // deterministic.  (Each doWork call advances ingress + sets lastSeenNs.)
        using clock = std::chrono::steady_clock;
        const auto deadline = clock::now() + std::chrono::seconds{10};
        for (int i = 0; i < kPumpIter && clock::now() < deadline; ++i) {
            invokeConductor();
            if (gw_) gw_->doWork(connectNs == 0 ? wallNowNs() : connectNs);
            client.egressSub->poll(asm_.handler(), 10);
            if (resp.has_value()) break;
            std::this_thread::sleep_for(std::chrono::microseconds{100});
        }
        EXPECT_TRUE(resp.has_value()) << "No ConnectResponse received";
        return {resp.value_or(pc::ConnectResponse{}), connectNs};
    }

    static constexpr std::size_t kScratch = 512;
    uint8_t scratch_[kScratch] = {};

    // A subclass sets this in its ctor to run the Aeron client conductor by hand (AgentInvoker): the client then
    // learns a driver response only when invokeConductor() runs, so an in-flight registration stays in flight
    // deterministically.  It also shortens the driver's publication linger so a released publication's counters
    // disappear within the test.  Default off: every other test here keeps the threaded conductor.
    bool useInvoker_ = false;
    void invokeConductor() {
        if (useInvoker_ && aeron_) aeron_->conductorAgentInvoker().invoke();
    }

    std::unique_ptr<ClientGateway> gw_;
    std::shared_ptr<aeron::Aeron>  aeron_;

private:
    void startDriver() {
        const std::string aeronDir = (tmpDir_->path / "aeron").string();
        fs::create_directories(tmpDir_->path);

        aeron_driver_context_init(&driverCtx_);
        aeron_driver_context_set_dir(driverCtx_, aeronDir.c_str());
        aeron_driver_context_set_threading_mode(driverCtx_, AERON_THREADING_MODE_SHARED);
        aeron_driver_context_set_dir_delete_on_start(driverCtx_, true);
        aeron_driver_context_set_dir_delete_on_shutdown(driverCtx_, true);
        aeron_driver_context_set_shared_idle_strategy(driverCtx_, "sleep-ns");
        aeron_driver_context_set_term_buffer_sparse_file(driverCtx_, true);
        aeron_driver_context_set_term_buffer_length(driverCtx_, 64 * 1024);
        if (useInvoker_) {
            aeron_driver_context_set_publication_linger_timeout_ns(driverCtx_, 10'000'000ULL);
            // The driver reaps a released publication on its housekeeping timer (default 1 s: a release took a fixed
            // ~2 s, within 1 s of expectReleased's old deadline).  10 ms keeps the release well inside the deadline.
            aeron_driver_context_set_timer_interval_ns(driverCtx_, 10'000'000ULL);
        }

        ASSERT_EQ(aeron_driver_init(&driver_, driverCtx_), 0)
            << "aeron_driver_init failed: " << aeron_errmsg();
        ASSERT_EQ(aeron_driver_start(driver_, /*manual_main_loop=*/true), 0)
            << "aeron_driver_start failed: " << aeron_errmsg();

        driverRunning_.store(true, std::memory_order_release);
        driverThread_ = std::thread([this]() {
            while (driverRunning_.load(std::memory_order_acquire)) {
                aeron_driver_main_idle_strategy(driver_,
                                                aeron_driver_main_do_work(driver_));
            }
        });

        // No caller-side pumping: aeron_driver_init/start already wrote
        // cnc.dat and stamped the heartbeat synchronously before driverThread_
        // even starts; pumping here would be a second concurrent pumper on
        // the single-pumper driver (spec Sec 3.6).
        const std::string cncPath = aeronDir + "/cnc.dat";
        ASSERT_TRUE(fs::exists(cncPath)) << "Aeron CnC not created within timeout";
    }

    void connectAeron() {
        const std::string aeronDir = (tmpDir_->path / "aeron").string();
        aeron::Context ctx;
        ctx.aeronDir(aeronDir);
        if (useInvoker_) ctx.useConductorAgentInvoker(true);
        aeron_ = aeron::Aeron::connect(ctx);
        ASSERT_NE(aeron_, nullptr) << "Aeron::connect failed";
    }

    void stopDriver() {
        if (driverRunning_.exchange(false, std::memory_order_acq_rel)) {
            if (driverThread_.joinable()) driverThread_.join();
        }
        if (driver_) {
            static constexpr int kDrainMs = 50;
            const auto dl = std::chrono::steady_clock::now() +
                            std::chrono::milliseconds{kDrainMs};
            while (std::chrono::steady_clock::now() < dl) {
                if (aeron_driver_main_do_work(driver_) == 0) {
                    aeron_driver_main_do_work(driver_);
                    break;
                }
                std::this_thread::sleep_for(std::chrono::microseconds{200});
            }
            aeron_driver_close(driver_);
            driver_ = nullptr;
        }
        if (driverCtx_) {
            aeron_driver_context_close(driverCtx_);
            driverCtx_ = nullptr;
        }
    }

    std::unique_ptr<TempDir>   tmpDir_;
    aeron_driver_context_t*    driverCtx_   = nullptr;
    aeron_driver_t*            driver_      = nullptr;
    std::atomic<bool>          driverRunning_{false};
    std::thread                driverThread_;
};

// ===========================================================================
// BR5T-1: Just-before timeout boundary — session must still be present.
//
// Create session at t0; call doWork(t0 + timeout - 1) → sessionCount()==1.
// ===========================================================================
TEST_F(GatewayTimeoutFixture, Reap_JustBefore_SessionPresent) {
    auto client = makeClient(kEgressBase + 1);

    // Connect at t0 = 1'000'000'000 ns (arbitrary fixed value).
    const int64_t t0 = 1'000'000'000LL;
    auto result = doConnect(client, /*corrId=*/1, t0);
    ASSERT_EQ(gw_->sessionCount(), 1u);

    // Advance to just before expiry (timeout - 1 ns).
    const int64_t tBefore = t0 + kTimeoutNs - 1;
    gw_->reapExpired(tBefore);
    EXPECT_EQ(gw_->sessionCount(), 1u) << "Session should survive just-before boundary";
}

// ===========================================================================
// BR5T-2: Just-after timeout boundary — session must be reaped.
//
// Create session at t0; call reapExpired(t0 + timeout + 1) → sessionCount()==0.
// ===========================================================================
TEST_F(GatewayTimeoutFixture, Reap_JustAfter_SessionReaped) {
    auto client = makeClient(kEgressBase + 2);

    const int64_t t0 = 1'000'000'000LL;
    auto result = doConnect(client, /*corrId=*/2, t0);
    ASSERT_EQ(gw_->sessionCount(), 1u);

    // Advance to just after expiry (timeout + 1 ns).
    const int64_t tAfter = t0 + kTimeoutNs + 1;
    gw_->reapExpired(tAfter);
    EXPECT_EQ(gw_->sessionCount(), 0u) << "Session should be reaped just-after boundary";
}

// ===========================================================================
// BR5T-3: Keepalive refresh keeps session alive past t0+timeout.
//
// Connect at t0; send Keepalive at t0+Δ (Δ < timeout); advance clock to
// t0+timeout+1 — session still alive because lastSeen is now t0+Δ (>t0+timeout-Δ).
// Then advance past t0+Δ+timeout → session finally reaped.
// ===========================================================================
TEST_F(GatewayTimeoutFixture, Reap_KeepaliveRefreshPreventsExpiry) {
    auto client = makeClient(kEgressBase + 3);

    const int64_t t0    = 1'000'000'000LL;
    const int64_t delta = kTimeoutNs / 2;  // 50 ms < 100 ms timeout

    auto result = doConnect(client, /*corrId=*/3, t0);
    ASSERT_GE(result.resp.assignedSessionId, 1);
    const int64_t sid = result.resp.assignedSessionId;

    // Send Keepalive at t0+Δ.
    const int64_t tKeepalive = t0 + delta;
    pc::Keepalive ka;
    ka.sessionId = sid;
    uint8_t kaBuf[64] = {};
    const std::size_t kaLen = ka.encode(kaBuf);
    offerBounded(*client.ingressPub, kaBuf, kaLen, "Keepalive");

    // Drive doWork with tKeepalive so the session's lastSeenNs is updated.
    using clock = std::chrono::steady_clock;
    const auto deadline = clock::now() + std::chrono::seconds{5};
    for (int i = 0; i < kPumpIter && clock::now() < deadline; ++i) {
        gw_->doWork(tKeepalive);
        std::this_thread::sleep_for(std::chrono::microseconds{100});
        // Wait until the Keepalive is processed (ingress drains).
        // We don't have a side-channel to observe this, so pump 500 iterations.
        if (i > 500) break;
    }
    ASSERT_EQ(gw_->sessionCount(), 1u);

    // Now reap at t0+timeout+1.  Session's lastSeen = t0+Δ.
    // Idle = (t0+timeout+1) - (t0+Δ) = timeout+1-Δ = 50ms+1ns < timeout(100ms).
    // Session must NOT be reaped.
    const int64_t tCheck1 = t0 + kTimeoutNs + 1;
    gw_->reapExpired(tCheck1);
    EXPECT_EQ(gw_->sessionCount(), 1u)
        << "Session touched at t0+Δ should still be alive at t0+timeout+1";

    // Reap at t0+Δ+timeout+1.  Idle = timeout+1 > timeout → reap.
    const int64_t tCheck2 = t0 + delta + kTimeoutNs + 1;
    gw_->reapExpired(tCheck2);
    EXPECT_EQ(gw_->sessionCount(), 0u)
        << "Session should be reaped at t0+Δ+timeout+1";
}

// ===========================================================================
// BR5T-4: Multiple sessions — only expired ones reaped in a single tick.
//
// Strategy: use wall-clock t0 sampled right before connecting A, then connect
// B with a later wall-clock t1.  Manually call reapExpired with synthetic
// offsets relative to t0 to test the boundary:
//   - reapExpired(t0 + timeout + 1): A reaped (idle = timeout+1), B alive.
//
// Both clients are resolved BEFORE any connects so makeClient's wall-clock
// doWork calls run when there are no sessions yet.
// ===========================================================================
TEST_F(GatewayTimeoutFixture, Reap_MultiSession_OnlyExpiredReaped) {
    // Resolve BOTH clients before any connects (no sessions → no reap hazard).
    auto clientA = makeClient(kEgressBase + 4);
    auto clientB = makeClient(kEgressBase + 5);

    // Sample t0 right before connecting A (uses wall clock, consistent with
    // the pump helper's doWork calls).
    const int64_t t0 = wallNowNs();

    // Connect A with wall-clock-based t0.
    auto resultA = doConnect(clientA, /*corrId=*/4, t0);
    ASSERT_GE(resultA.resp.assignedSessionId, 1);
    ASSERT_EQ(gw_->sessionCount(), 1u);

    // Connect B (also wall-clock-based; doConnect uses wallNowNs() when
    // connectNs==0).  Session B's lastSeen ≈ wallNowNs() >> t0 + timeout in
    // the reap check below only if we waited >100ms between the two connects,
    // which we did not.  To be safe, also sample t1 and pass it explicitly.
    const int64_t t1 = wallNowNs();
    auto resultB = doConnect(clientB, /*corrId=*/5, t1);
    ASSERT_GE(resultB.resp.assignedSessionId, 1);
    ASSERT_EQ(gw_->sessionCount(), 2u);

    // Now invoke reapExpired with synthetic large timestamps:
    //   tReap_A = t0 + timeout + 1: A is expired (idle = timeout+1 > timeout),
    //             B is NOT (idle = tReap_A - t1 = t0+timeout+1 - t1 ≈ timeout+1 - (t1-t0)).
    //             Since t1 - t0 ≥ 1 ns, idle_B = timeout+1 - (t1-t0) < timeout. ✓
    //
    // If t1-t0 > 1 ns (i.e., any real time elapsed), session B is safe.
    // We assert the difference is within 50ms (the test runs in well under 1s).
    const int64_t tReapA = t0 + kTimeoutNs + 1;
    ASSERT_LT(t1 - t0, kTimeoutNs / 2)
        << "Sanity: both connects must complete within timeout/2 of each other";

    gw_->reapExpired(tReapA);

    EXPECT_EQ(gw_->sessionCount(), 1u)
        << "Only session A (lastSeen=t0) should be reaped; session B (lastSeen=t1) survives";
}

// ===========================================================================
// BR5T-5: Reaped session's Ping gets no Pong.
//
// Connect; reap at t+timeout+1; send Ping for the old sessionId → no Pong.
// ===========================================================================
TEST_F(GatewayTimeoutFixture, Reap_AfterReap_PingGetsNoPong) {
    auto client = makeClient(kEgressBase + 6);

    const int64_t t0 = 1'000'000'000LL;
    auto result = doConnect(client, /*corrId=*/6, t0);
    ASSERT_GE(result.resp.assignedSessionId, 1);
    const int64_t sid = result.resp.assignedSessionId;

    // Reap.
    gw_->reapExpired(t0 + kTimeoutNs + 1);
    ASSERT_EQ(gw_->sessionCount(), 0u);

    // Send Ping with the now-reaped sessionId.
    pc::Ping ping;
    ping.sessionId     = sid;
    ping.correlationId = 77;
    ping.echo          = "ghost-after-reap";
    uint8_t pingBuf[256] = {};
    const std::size_t pingLen = ping.encode(pingBuf);
    offerBounded(*client.ingressPub, pingBuf, pingLen, "PingAfterReap");

    std::optional<pc::Pong> pong;
    aeron::FragmentAssembler asm_([&](aeron::concurrent::AtomicBuffer& buf,
                                      aeron::util::index_t offset,
                                      aeron::util::index_t len,
                                      aeron::Header&) {
        auto dec = pc::Pong::decode(
            reinterpret_cast<const uint8_t*>(buf.buffer() + offset),
            static_cast<std::size_t>(len));
        if (dec) pong = *dec;
    });

    // Bounded no-response wait (2000 iterations).
    for (int i = 0; i < 2000; ++i) {
        gw_->doWork(t0 + kTimeoutNs + 2);
        client.egressSub->poll(asm_.handler(), 10);
        if (pong.has_value()) break;
        std::this_thread::sleep_for(std::chrono::microseconds{100});
    }

    EXPECT_FALSE(pong.has_value())
        << "Reaped session must not produce a Pong";
}

// ===========================================================================
// BR5T-6: Real multi-session bounded-reap test.
//
// Establish N=4 real sessions via the loopback harness.  Drive each connect
// with a wall-clock-sampled connectNs so lastSeenNs is deterministic relative
// to the captured t0.  Then:
//   (a) reapExpired(farFutureNs): all N reaped in one tick → sessionCount N→0.
//   (b) In a second pass with two groups (tA < tB), reapExpired at a threshold
//       between the two groups' lastSeen values reaps only the older subset.
//
// No busy-spin; deterministic (synthetic reap thresholds relative to real
// connect times, guarded the same way as BR5T-4).
// ===========================================================================
TEST_F(GatewayTimeoutFixture, Reap_Bounded_ManySessionsAllReaped) {
    // -----------------------------------------------------------------------
    // Part (a): all-reap.  Resolve 4 clients, connect them all, then reap all.
    // -----------------------------------------------------------------------
    static constexpr int kN = 4;
    // Stream IDs 407..410 (offset chosen to avoid collision with other tests).
    std::vector<TestClient> clients;
    clients.reserve(kN);
    for (int i = 0; i < kN; ++i) {
        clients.push_back(makeClient(kEgressBase + 7 + i));
    }

    // Sample t0 right before connecting (wall clock, same strategy as BR5T-4).
    const int64_t t0 = wallNowNs();

    for (int i = 0; i < kN; ++i) {
        auto result = doConnect(clients[i], /*corrId=*/70 + i, t0);
        ASSERT_GE(result.resp.assignedSessionId, 1)
            << "connect failed for client " << i;
    }
    ASSERT_EQ(gw_->sessionCount(), static_cast<std::size_t>(kN));

    // Time the reap call — must complete well under 1 s (bounded O(N) scan).
    using clock = std::chrono::steady_clock;
    const auto before = clock::now();
    gw_->reapExpired(t0 + kTimeoutNs * 100);   // far future → everything stale
    const auto after  = clock::now();

    EXPECT_EQ(gw_->sessionCount(), 0u)
        << "All " << kN << " sessions must be reaped in one tick";

    const auto elapsedMs = std::chrono::duration_cast<std::chrono::milliseconds>(
        after - before).count();
    EXPECT_LT(elapsedMs, 1000)
        << "reapExpired must complete in bounded time (< 1 s for " << kN << " sessions)";

    // -----------------------------------------------------------------------
    // Part (b): selective reap across two groups.
    //
    // Connect 2 "old" sessions at tA, then 2 "new" sessions at tB > tA.
    // A threshold between (tA + timeout) and (tB + timeout) reaps only the A
    // group — B group survives.
    // -----------------------------------------------------------------------
    // Stream IDs 411..414 (non-overlapping with above).
    std::vector<TestClient> groupA, groupB;
    groupA.reserve(2);
    groupB.reserve(2);
    for (int i = 0; i < 2; ++i)
        groupA.push_back(makeClient(kEgressBase + 11 + i));
    for (int i = 0; i < 2; ++i)
        groupB.push_back(makeClient(kEgressBase + 13 + i));

    const int64_t tA = wallNowNs();
    for (int i = 0; i < 2; ++i) {
        auto r = doConnect(groupA[i], /*corrId=*/80 + i, tA);
        ASSERT_GE(r.resp.assignedSessionId, 1) << "groupA connect " << i;
    }
    ASSERT_EQ(gw_->sessionCount(), 2u);

    const int64_t tB = wallNowNs();
    ASSERT_LT(tB - tA, kTimeoutNs / 2)
        << "Sanity: A and B connects must be within timeout/2 of each other";

    for (int i = 0; i < 2; ++i) {
        auto r = doConnect(groupB[i], /*corrId=*/82 + i, tB);
        ASSERT_GE(r.resp.assignedSessionId, 1) << "groupB connect " << i;
    }
    ASSERT_EQ(gw_->sessionCount(), 4u);

    // Threshold: tA+timeout+1.  Group-A idle = timeout+1 > timeout → reaped.
    // Group-B idle = tA+timeout+1-tB = timeout+1-(tB-tA) < timeout → survives.
    const int64_t tSelectiveReap = tA + kTimeoutNs + 1;
    gw_->reapExpired(tSelectiveReap);

    EXPECT_EQ(gw_->sessionCount(), 2u)
        << "Selective reap: only groupA (lastSeen=tA) should be gone; groupB (lastSeen=tB) survives";

    // Cleanup: reap remaining B group.
    gw_->reapExpired(tB + kTimeoutNs + 1);
    EXPECT_EQ(gw_->sessionCount(), 0u);
}

// ===========================================================================
// BR5T-7: Ping-only client survives past original timeout window.
//
// A session that sends ONLY Pings (no explicit Keepalive) must stay alive
// as long as Ping traffic continues — because handlePing now calls touch().
// After Ping traffic stops, the session must be reaped once it goes idle for
// longer than sessionTimeoutNs.
//
// Driven entirely with injected clock via doWork(tNow) + reapExpired(tNow).
// ===========================================================================
TEST_F(GatewayTimeoutFixture, Reap_PingOnlyClientSurvivesWhilePinging) {
    auto client = makeClient(kEgressBase + 15);

    const int64_t t0    = 1'000'000'000LL;
    const int64_t delta = kTimeoutNs / 4;  // 25 ms between pings

    auto result = doConnect(client, /*corrId=*/90, t0);
    ASSERT_GE(result.resp.assignedSessionId, 1);
    const int64_t sid = result.resp.assignedSessionId;
    ASSERT_EQ(gw_->sessionCount(), 1u);

    // -----------------------------------------------------------------------
    // Phase 1: send Pings at t0+delta, t0+2*delta, t0+3*delta, t0+4*delta.
    // Each Ping arrives with doWork(tPing) so nowNs_ == tPing when processed.
    // After each Ping, reapExpired(tPing+timeout-1) must NOT reap the session
    // (session was just touched at tPing).
    // -----------------------------------------------------------------------
    // Helper lambda: send one Ping and drive doWork with the given nowNs so
    // the gateway processes it at exactly that injected timestamp.
    auto sendPingAt = [&](int64_t tNow) {
        pc::Ping ping;
        ping.sessionId     = sid;
        ping.correlationId = 99;
        ping.echo          = "keepme";
        uint8_t pingBuf[256] = {};
        const std::size_t pingLen = ping.encode(pingBuf);
        offerBounded(*client.ingressPub, pingBuf, pingLen, "Ping");

        // Drain with tNow so the gateway processes the Ping at that clock value.
        using clock = std::chrono::steady_clock;
        const auto deadline = clock::now() + std::chrono::seconds{5};
        for (int i = 0; i < kPumpIter && clock::now() < deadline; ++i) {
            gw_->doWork(tNow);
            std::this_thread::sleep_for(std::chrono::microseconds{100});
            if (i > 500) break;   // Ping is in-flight; let ingress settle
        }
    };

    // Send 5 Pings spread over 5*delta (well beyond t0+timeout if delta<timeout).
    static constexpr int kPings = 5;
    for (int p = 1; p <= kPings; ++p) {
        const int64_t tPing = t0 + static_cast<int64_t>(p) * delta;
        sendPingAt(tPing);

        // At tPing + timeout - 1, session is still alive (just touched at tPing).
        gw_->reapExpired(tPing + kTimeoutNs - 1);
        EXPECT_EQ(gw_->sessionCount(), 1u)
            << "Session should be alive just before timeout after Ping #" << p;
    }

    // -----------------------------------------------------------------------
    // Phase 2: stop sending Pings.  Session's lastSeen == t0 + kPings*delta.
    // At lastSeen + timeout + 1 the session must be reaped.
    // -----------------------------------------------------------------------
    const int64_t tLastPing = t0 + static_cast<int64_t>(kPings) * delta;

    // Just before: still alive.
    gw_->reapExpired(tLastPing + kTimeoutNs - 1);
    EXPECT_EQ(gw_->sessionCount(), 1u)
        << "Session should be alive just before timeout after last Ping";

    // Just after: reaped.
    gw_->reapExpired(tLastPing + kTimeoutNs + 1);
    EXPECT_EQ(gw_->sessionCount(), 0u)
        << "Session should be reaped once Ping traffic stops for > timeout";
}


// ===========================================================================
// F23 regression: ANY request from a session counts as liveness (Kafka: a
// connection is idle only when it sends no requests).  Before the fix only
// Keepalive and Ping refreshed lastSeenNs, so a client that produced (or
// subscribed) continuously without explicit keepalives was reaped after
// sessionTimeoutNs — its subscriptions vanished and pushes stopped
// (GatewayDataBench SubscribeDelivery "hung" once a run passed 10 s).
// ===========================================================================
namespace {
void sendAndPump(ClientGateway& gw, aeron::Publication& pub,
                 const uint8_t* buf, std::size_t len, int64_t atNs) {
    aeron::concurrent::AtomicBuffer ab(const_cast<uint8_t*>(buf),
                                       static_cast<aeron::util::index_t>(len));
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds{5};
    while (pub.offer(ab, 0, static_cast<aeron::util::index_t>(len)) < 0 &&
           std::chrono::steady_clock::now() < deadline) {
        std::this_thread::yield();
    }
    // Pump with the injected clock so the message is processed AT atNs.
    for (int i = 0; i < 600; ++i) {
        gw.doWork(atNs);
        std::this_thread::yield();
    }
}
}  // namespace

TEST_F(GatewayTimeoutFixture, ProduceRefreshesSessionLivenessRegression) {
    auto client = makeClient(kEgressBase + 40);
    const int64_t t0    = 1'000'000'000LL;
    const int64_t delta = kTimeoutNs / 2;
    auto result = doConnect(client, /*corrId=*/40, t0);
    ASSERT_GE(result.resp.assignedSessionId, 1);
    const int64_t sid = result.resp.assignedSessionId;

    // A Produce (no topic registry here — the gateway still identifies the
    // session before rejecting it) at t0+Δ, and NO keepalive.
    std::vector<uint8_t> batch(64, 0xAB);
    pc::Produce prod;
    prod.sessionId = sid; prod.correlationId = 41; prod.topicId = 0; prod.partition = 0;
    prod.partitionKnown = 1; prod.recordBatch = std::span<const uint8_t>(batch.data(), batch.size());
    std::vector<uint8_t> buf(256 + batch.size());
    const std::size_t n = prod.encode(buf.data());
    sendAndPump(*gw_, *client.ingressPub, buf.data(), n, t0 + delta);

    gw_->reapExpired(t0 + kTimeoutNs + 1);
    EXPECT_EQ(gw_->sessionCount(), 1u)
        << "a session that produced at t0+Δ must not be reaped at t0+timeout+1";
}

TEST_F(GatewayTimeoutFixture, SubscribeRefreshesSessionLivenessRegression) {
    auto client = makeClient(kEgressBase + 42);
    const int64_t t0    = 1'000'000'000LL;
    const int64_t delta = kTimeoutNs / 2;
    auto result = doConnect(client, /*corrId=*/42, t0);
    ASSERT_GE(result.resp.assignedSessionId, 1);
    const int64_t sid = result.resp.assignedSessionId;

    pc::Subscribe sub;
    sub.sessionId = sid; sub.correlationId = 43; sub.topicId = 0; sub.partition = 0;
    sub.partitionKnown = 1; sub.fromOffset = 0;
    uint8_t buf[256] = {};
    const std::size_t n = sub.encode(buf);
    sendAndPump(*gw_, *client.ingressPub, buf, n, t0 + delta);

    gw_->reapExpired(t0 + kTimeoutNs + 1);
    EXPECT_EQ(gw_->sessionCount(), 1u)
        << "a session that subscribed at t0+Δ must not be reaped at t0+timeout+1";
}

// F23 (review): per-worker ingress (forced on in shard mode) delivers Produce
// straight to the worker — it never passes onFragment.  Every such produce
// comes back to the gateway as a ProduceResult; that must refresh liveness, or
// a shard-mode producer without keepalives is reaped after sessionTimeoutNs.
TEST_F(GatewayTimeoutFixture, WorkerIngressProduceResultRefreshesSessionLivenessRegression) {
    auto client = makeClient(kEgressBase + 44);
    const int64_t t0    = 1'000'000'000LL;
    const int64_t delta = kTimeoutNs / 2;
    auto result = doConnect(client, /*corrId=*/44, t0);
    ASSERT_GE(result.resp.assignedSessionId, 1);
    const int64_t sid = result.resp.assignedSessionId;

    gw_->doWork(t0 + delta);                       // gateway clock at t0+Δ
    rune::node::WorkerReturn ret;
    ret.type          = rune::node::WorkerReturn::Type::ProduceResult;
    ret.sessionId     = sid;
    ret.correlationId = 45;
    ret.baseOffset    = 0;
    gw_->handleWorkerReturnForTest(ret);           // a worker-ingress produce's result

    gw_->reapExpired(t0 + kTimeoutNs + 1);
    EXPECT_EQ(gw_->sessionCount(), 1u)
        << "a session whose worker-ingress produce returned at t0+Δ must not be reaped at t0+timeout+1";
}

// F23 (review minor): pin the generic request branch with a group template.
TEST_F(GatewayTimeoutFixture, GroupHeartbeatRefreshesSessionLivenessRegression) {
    auto client = makeClient(kEgressBase + 46);
    const int64_t t0    = 1'000'000'000LL;
    const int64_t delta = kTimeoutNs / 2;
    auto result = doConnect(client, /*corrId=*/46, t0);
    ASSERT_GE(result.resp.assignedSessionId, 1);
    const int64_t sid = result.resp.assignedSessionId;

    pc::Heartbeat hb;
    hb.sessionId = sid; hb.correlationId = 47;
    uint8_t buf[512] = {};
    const std::size_t n = hb.encode(buf);
    sendAndPump(*gw_, *client.ingressPub, buf, n, t0 + delta);

    gw_->reapExpired(t0 + kTimeoutNs + 1);
    EXPECT_EQ(gw_->sessionCount(), 1u)
        << "a session that sent a group Heartbeat at t0+Δ must not be reaped at t0+timeout+1";
}

// ===========================================================================
// Issue #1 — a record-egress publication registration abandoned while it is still in flight (its session closed, or
// its resolve timed out) must be RELEASED on the media driver, never leaked.  The Aeron client is run by hand
// (useInvoker_), so the registration stays AWAITING_MEDIA_DRIVER on the client until invokeConductor(): the
// in-flight window is deterministic.  The observable is the driver's publisher-limit counter keyed by the
// registration id: it exists while the registration lives and disappears only once the publication is released.
// ===========================================================================
class RecordEgressLifetimeRegression : public GatewayTimeoutFixture {
protected:
    explicit RecordEgressLifetimeRegression(int32_t workers = 1)
        : pool_(workers, rune::metadata::ShardRouter(workers), 1 << 16, 1 << 16, 16, 16) { useInvoker_ = true; }

    // The driver's publisher-limit counter (type 1) of registration `regId`, if it exists.
    bool pubLimitCounterExists(int64_t regId) {
        bool found = false;
        aeron_->countersReader().forEach(
            [&](int32_t, int32_t typeId, const aeron::concurrent::AtomicBuffer& key, const std::string&) {
                if (typeId == 1 && key.getInt64(0) == regId) found = true;
            });
        return found;
    }

    // Connect a session and start its record-egress registration with a Subscribe, in flight.
    void startInFlightEgress(TestClient& client, int64_t& sid, int64_t& regId) {
        auto result = doConnect(client, /*corrId=*/60, 1'000'000'000LL);
        ASSERT_GE(result.resp.assignedSessionId, 1);
        sid = result.resp.assignedSessionId;
        gw_->setWorkerPool(&pool_);
        gw_->forwardSubscribeForTest(/*subId=*/1001, sid, /*topic=*/0, /*partition=*/0, 0, 0, false, false, 7);
        regId = gw_->recordEgressRegIdForTest(sid, 0);
        ASSERT_GE(regId, 0) << "the Subscribe must start a record-egress registration";
        waitDriverHolds(regId);
    }

    // The driver allocates the registration's counter on its own thread; the client conductor is not invoked, so
    // the client still believes the registration is in flight (bounded: iteration cap + 10 s).
    void waitDriverHolds(int64_t regId) {
        using clock = std::chrono::steady_clock;
        const auto deadline = clock::now() + std::chrono::seconds{10};
        for (int i = 0; i < kPumpIter && !pubLimitCounterExists(regId) && clock::now() < deadline; ++i)
            std::this_thread::sleep_for(std::chrono::microseconds{100});
        ASSERT_TRUE(pubLimitCounterExists(regId)) << "the driver never allocated registration " << regId;
    }

    // Subscribe `subId` of session `sid` to (topic, partition) and wait until its owner worker's record-egress
    // registration is held by the driver while still in flight on the client.  Returns that registration id.
    int64_t subscribeInFlight(int64_t sid, int64_t subId, int32_t topic, int32_t partition) {
        gw_->forwardSubscribeForTest(subId, sid, topic, partition, 0, 0, false, false, 7);
        const int64_t regId = gw_->recordEgressRegIdForTest(sid, pool_.ownerWorkerId(topic, partition));
        EXPECT_GE(regId, 0) << "the Subscribe must start a record-egress registration";
        if (regId >= 0) waitDriverHolds(regId);
        return regId;
    }

    // Drive gateway + client conductor (bounded) until the abandoned registration is fully released.
    void expectReleased(int64_t regId, std::size_t pendingBefore) {
        using clock = std::chrono::steady_clock;
        const auto deadline = clock::now() + std::chrono::seconds{10};
        for (int i = 0; i < kPumpIter && clock::now() < deadline; ++i) {
            invokeConductor();
            gw_->doWork(wallNowNs());
            if (gw_->pendingConnectCount() == pendingBefore && !pubLimitCounterExists(regId)) break;
            std::this_thread::sleep_for(std::chrono::microseconds{100});
        }
        EXPECT_EQ(gw_->pendingConnectCount(), pendingBefore) << "the draining registration never resolved";
        EXPECT_FALSE(pubLimitCounterExists(regId))
            << "the abandoned record-egress registration " << regId << " is still alive on the media driver (leaked)";
    }

    rune::node::WorkerPool pool_;
};

TEST_F(RecordEgressLifetimeRegression, ASessionClosedWhileItsEgressPubResolvesReleasesTheRegistrationRegression) {
    auto client = makeClient(kEgressBase + 60);
    const std::size_t before = gw_->pendingConnectCount();
    int64_t sid = 0, regId = -1;
    startInFlightEgress(client, sid, regId);
    ASSERT_FALSE(HasFatalFailure());

    gw_->closeSessionForTest(sid);   // same cycle: the registration is still AWAITING_MEDIA_DRIVER on the client

    EXPECT_EQ(gw_->recordEgressRegIdForTest(sid, 0), -1) << "the close drops the pending entry";
    EXPECT_GE(gw_->pendingConnectCount(), before + 1)
        << "the abandoned in-flight registration must be queued for release, not forgotten";
    expectReleased(regId, before);
}

TEST_F(RecordEgressLifetimeRegression, ATimedOutEgressPubResolveReleasesTheRegistrationRegression) {
    auto client = makeClient(kEgressBase + 60);
    const std::size_t before = gw_->pendingConnectCount();
    int64_t sid = 0, regId = -1;
    startInFlightEgress(client, sid, regId);
    ASSERT_FALSE(HasFatalFailure());

    // The resolve is still in flight when its timeout (default 5 s) is exceeded.
    gw_->pollPendingRecordEgressForTest(wallNowNs() + 60'000'000'000LL);

    EXPECT_EQ(gw_->recordEgressRegIdForTest(sid, 0), -1) << "the timeout drops the pending entry";
    EXPECT_GE(gw_->pendingConnectCount(), before + 1)
        << "the abandoned in-flight registration must be queued for release, not forgotten";
    expectReleased(regId, before);
}

// ===========================================================================
// Adversarial review of PR #559 (issue #1): every sibling entry point that abandons an in-flight record-egress
// registration — the session reaper, a Disconnect frame on the wire, a session holding one registration per worker,
// a re-Subscribe of the same worker while pending, a close after the resolve already timed out, and many sessions
// at once.  Same deterministic in-flight window as above (client conductor run by hand).  Every test asserts the
// registration is queued for release, released on the driver, and the drain table back to its starting size
// (an id queued twice, or a stale one, would never drain).
// ===========================================================================

// The session REAPER (doWork with the injected clock past the session timeout) is the most common close path.
TEST_F(RecordEgressLifetimeRegression, ReapedSessionReleasesItsInFlightEgressPubAdversarialRegression) {
    auto client = makeClient(kEgressBase + 61);
    const std::size_t before = gw_->pendingConnectCount();
    int64_t sid = 0, regId = -1;
    startInFlightEgress(client, sid, regId);
    ASSERT_FALSE(HasFatalFailure());
    ASSERT_EQ(gw_->sessionCount(), 1u);

    // Connected at t=1 s; one doWork just past the 100 ms session timeout (well inside the 5 s resolve timeout, so
    // only the reaper drops it).  The conductor is not invoked: the registration is still in flight on the client.
    gw_->doWork(1'000'000'000LL + kTimeoutNs + 1);

    ASSERT_EQ(gw_->sessionCount(), 0u) << "the reaper must close the expired session";
    EXPECT_EQ(gw_->recordEgressRegIdForTest(sid, 0), -1) << "the reap drops the pending entry";
    EXPECT_EQ(gw_->pendingConnectCount(), before + 1)
        << "the reaped session's in-flight registration must be queued for release exactly once";
    expectReleased(regId, before);
}

// A client Disconnect frame on the wire (handleDisconnect), driven through the real ingress subscription.
TEST_F(RecordEgressLifetimeRegression, DisconnectFrameReleasesTheInFlightEgressPubAdversarialRegression) {
    auto client = makeClient(kEgressBase + 62);
    const std::size_t before = gw_->pendingConnectCount();
    int64_t sid = 0, regId = -1;
    startInFlightEgress(client, sid, regId);
    ASSERT_FALSE(HasFatalFailure());

    pc::Disconnect d;
    d.sessionId     = sid;
    d.correlationId = 61;
    uint8_t frame[pc::Disconnect::ENCODED_SIZE] = {};
    offerBounded(*client.ingressPub, frame, d.encode(frame), "Disconnect");
    ASSERT_FALSE(HasFatalFailure());

    // Same injected clock as the connect (no reap, no resolve timeout), conductor NOT invoked: only the Disconnect
    // can close the session, and its registration stays in flight on the client.  Bounded drive.
    using clock = std::chrono::steady_clock;
    const auto deadline = clock::now() + std::chrono::seconds{10};
    for (int i = 0; i < kPumpIter && gw_->sessionCount() != 0 && clock::now() < deadline; ++i) {
        gw_->doWork(1'000'000'000LL);
        if (gw_->sessionCount() != 0) std::this_thread::sleep_for(std::chrono::microseconds{100});
    }
    ASSERT_EQ(gw_->sessionCount(), 0u) << "the Disconnect frame never closed the session";
    EXPECT_EQ(gw_->recordEgressRegIdForTest(sid, 0), -1) << "the Disconnect drops the pending entry";
    EXPECT_EQ(gw_->pendingConnectCount(), before + 1)
        << "the disconnected session's in-flight registration must be queued for release exactly once";
    expectReleased(regId, before);
}

// A re-Subscribe on the SAME worker while its resolve is pending joins the one registration (never a second,
// which the close would then have to release too): exactly one release.
TEST_F(RecordEgressLifetimeRegression, ResubscribeSameWorkerWhilePendingReleasesOneRegistrationAdversarialRegression) {
    auto client = makeClient(kEgressBase + 63);
    const std::size_t before = gw_->pendingConnectCount();
    int64_t sid = 0, regId = -1;
    startInFlightEgress(client, sid, regId);
    ASSERT_FALSE(HasFatalFailure());

    gw_->forwardSubscribeForTest(/*subId=*/1002, sid, /*topic=*/0, /*partition=*/0, 0, 0, false, false, 8);
    EXPECT_EQ(gw_->recordEgressRegIdForTest(sid, 0), regId) << "a re-Subscribe must join the pending registration";
    EXPECT_EQ(gw_->pendingRecordEgressCountForTest(), 1u);
    EXPECT_EQ(gw_->pendingEgressWaitersForTest(), 2) << "both Subscribes wait on the one registration";

    gw_->closeSessionForTest(sid);
    EXPECT_EQ(gw_->pendingRecordEgressCountForTest(), 0u);
    EXPECT_EQ(gw_->pendingEgressWaitersForTest(), 0) << "the waiters die with the session";
    EXPECT_EQ(gw_->pendingConnectCount(), before + 1) << "exactly one registration is queued for release";
    expectReleased(regId, before);
}

// A close AFTER the resolve already timed out (the timeout queued it) must not queue the same id again: a second
// copy could never drain once the first released it (the conductor no longer knows the id).
TEST_F(RecordEgressLifetimeRegression, CloseAfterEgressResolveTimeoutQueuesTheReleaseOnceAdversarialRegression) {
    auto client = makeClient(kEgressBase + 64);
    const std::size_t before = gw_->pendingConnectCount();
    int64_t sid = 0, regId = -1;
    startInFlightEgress(client, sid, regId);
    ASSERT_FALSE(HasFatalFailure());

    gw_->pollPendingRecordEgressForTest(wallNowNs() + 60'000'000'000LL);   // resolve timeout: queued for release
    ASSERT_EQ(gw_->pendingConnectCount(), before + 1);
    gw_->closeSessionForTest(sid);                                         // then the session goes
    gw_->closeSessionForTest(sid);                                         // ... twice (unknown: a no-op)
    EXPECT_EQ(gw_->pendingConnectCount(), before + 1) << "the close must not queue the timed-out id a second time";
    expectReleased(regId, before);
}

// Resources: many sessions, each with an in-flight registration, all closed in one burst — every registration is
// released and the drain table returns to its starting size (bounded, fully drained).
TEST_F(RecordEgressLifetimeRegression, ManySessionsClosedWithInFlightEgressReleaseEveryRegistrationAdversarialRegression) {
    constexpr int kSessions = 6;
    std::vector<TestClient> clients;
    for (int i = 0; i < kSessions; ++i) clients.push_back(makeClient(kEgressBase + 70 + i));
    const std::size_t before = gw_->pendingConnectCount();

    // Sessions stamped an hour ahead of the wall clock the helpers drive doWork with: no session is reaped while the
    // next one connects (the injected clock, not wall time, decides).
    const int64_t t0 = wallNowNs() + 3'600'000'000'000LL;
    std::vector<int64_t> sids;
    for (int i = 0; i < kSessions; ++i) {
        auto r = doConnect(clients[static_cast<std::size_t>(i)], /*corrId=*/80 + i, t0);
        ASSERT_GE(r.resp.assignedSessionId, 1);
        sids.push_back(r.resp.assignedSessionId);
    }
    ASSERT_EQ(gw_->sessionCount(), static_cast<std::size_t>(kSessions));
    gw_->setWorkerPool(&pool_);

    std::vector<int64_t> regIds;
    for (int i = 0; i < kSessions; ++i) {
        regIds.push_back(subscribeInFlight(sids[static_cast<std::size_t>(i)], 2000 + i, 0, 0));
        ASSERT_FALSE(HasFatalFailure());
        ASSERT_GE(regIds.back(), 0);
    }
    for (const int64_t sid : sids) gw_->closeSessionForTest(sid);

    EXPECT_EQ(gw_->sessionCount(), 0u);
    EXPECT_EQ(gw_->pendingRecordEgressCountForTest(), 0u);
    EXPECT_EQ(gw_->pendingConnectCount(), before + kSessions) << "every in-flight registration is queued once";
    for (const int64_t regId : regIds) expectReleased(regId, before);
}

// One session, two workers: a registration in flight on EACH worker — the close releases both.
class RecordEgressLifetimeTwoWorkerRegression : public RecordEgressLifetimeRegression {
protected:
    RecordEgressLifetimeTwoWorkerRegression() : RecordEgressLifetimeRegression(2) {}
};

TEST_F(RecordEgressLifetimeTwoWorkerRegression, CloseReleasesTheInFlightEgressPubOfEveryWorkerAdversarialRegression) {
    // Two partitions of topic 0 owned by different workers.
    const int32_t p0 = 0;
    int32_t p1 = -1;
    for (int32_t p = 1; p < 64 && p1 < 0; ++p)
        if (pool_.ownerWorkerId(0, p) != pool_.ownerWorkerId(0, p0)) p1 = p;
    ASSERT_GE(p1, 0) << "no partition of topic 0 maps to the second worker";

    auto client = makeClient(kEgressBase + 65);
    const std::size_t before = gw_->pendingConnectCount();
    auto r = doConnect(client, /*corrId=*/65, 1'000'000'000LL);
    ASSERT_GE(r.resp.assignedSessionId, 1);
    const int64_t sid = r.resp.assignedSessionId;
    gw_->setWorkerPool(&pool_);

    const int64_t reg0 = subscribeInFlight(sid, 3001, 0, p0);
    ASSERT_FALSE(HasFatalFailure());
    const int64_t reg1 = subscribeInFlight(sid, 3002, 0, p1);
    ASSERT_FALSE(HasFatalFailure());
    ASSERT_GE(reg0, 0);
    ASSERT_GE(reg1, 0);
    ASSERT_NE(reg0, reg1) << "each worker gets its own registration";
    ASSERT_EQ(gw_->pendingRecordEgressCountForTest(), 2u);

    gw_->closeSessionForTest(sid);

    EXPECT_EQ(gw_->pendingRecordEgressCountForTest(), 0u);
    EXPECT_EQ(gw_->pendingConnectCount(), before + 2) << "both workers' registrations are queued for release";
    expectReleased(reg0, before);
    expectReleased(reg1, before);
}
} // namespace
} // namespace rune::gateway
