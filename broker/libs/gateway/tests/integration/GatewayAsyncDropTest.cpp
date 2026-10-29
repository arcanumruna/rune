// GatewayAsyncDropTest.cpp — BR-7a fix: tests for async pending-connect
// timeout, cap/eviction, and resolve-after-cycles paths.
//
// All tests use injected nowNs for the reap/timeout clock so they run in
// bounded wall time regardless of kDefaultConnectResolveTimeoutNs.
//
// Setup:
//   - One embedded Aeron media driver (unique temp dir per fixture).
//   - One Aeron client shared by the gateway and test harness.
//   - Gateway uses a SHORT connectResolveTimeoutNs (10 ms) so timeout tests
//     do not wait 5 seconds.
//
// Unique temp paths: getpid() + atomic counter.
// Bounded drivers: hard cap + ASSERT on all waits.
//
// Tests:
//   GW-DR-1  Timeout-drop: pending-connect for unreachable egress drains to
//            zero after timeout (pendingConnectCount() → 0); registration
//            released (drain table also reaches zero).
//   GW-DR-2  Cap/eviction: spamming > kMaxPendingConnects Connects keeps the
//            pending table bounded; evicted entries are released (drain table
//            stays bounded).
//   GW-DR-3  Resolve-after-cycles (positive): a Connect whose egress resolves
//            after several doWork cycles produces ConnectResponse{OK} and
//            pendingConnectCount() → 0.  (Complement to GW-1 in loopback tests.)

#include <rune/gateway/ClientGateway.h>
#include <rune/protocol/client/ControlMessages.h>
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
// Unique temp-path generator (getpid + atomic counter)
// ---------------------------------------------------------------------------
static fs::path makeTempDir(std::string_view tag = "") {
    static std::atomic<uint32_t> g_seq{0};
    auto seq  = g_seq.fetch_add(1, std::memory_order_relaxed);
    auto name = std::string("rune_gwdr_") + std::string(tag) +
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

static int64_t wallNowNs() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count();
}

// ---------------------------------------------------------------------------
// GatewayAsyncDropFixture — embedded driver + Aeron client.
//
// Gateway is built with a very short connectResolveTimeoutNs (10 ms = 10_000_000
// ns) so timeout tests need only advance the injected clock by 10 ms + 1 ns.
// ---------------------------------------------------------------------------
class GatewayAsyncDropFixture : public ::testing::Test {
public:
    static constexpr int32_t kIngressStream   = 501;
    static constexpr int32_t kEgressBase      = 600;
    static constexpr int32_t kThisNodeId      = 11;
    static constexpr int     kPumpIter        = 50'000;

    // Short connect-resolve timeout used in all drop tests: 10 ms.
    static constexpr int64_t kConnectTimeoutNs = 10'000'000LL; // 10 ms

    void SetUp() override {
        tmpDir_ = std::make_unique<TempDir>("dr_");
        startDriver();
        connectAeron();

        gw_ = std::make_unique<ClientGateway>(
            aeron_,
            "aeron:ipc",
            kIngressStream,
            kThisNodeId,
            ClientGateway::kDefaultSessionTimeoutNs,
            nullptr,              // leaderView: always leader
            kConnectTimeoutNs);   // short egress-resolve timeout
    }

    void TearDown() override {
        gw_.reset();
        aeron_.reset();
        stopDriver();
        tmpDir_.reset();
    }

    // -----------------------------------------------------------------------
    // Ingress publication: test harness → gateway.
    // -----------------------------------------------------------------------
    std::shared_ptr<aeron::Publication> makeIngressPub() {
        int64_t regId = aeron_->addPublication("aeron:ipc", kIngressStream);
        std::shared_ptr<aeron::Publication> pub;
        pump([&]() {
            if (!pub) pub = aeron_->findPublication(regId);
            return pub != nullptr;
        }, kPumpIter, "makeIngressPub resolve");
        pump([&]() { return pub->isConnected(); }, kPumpIter, "makeIngressPub connected");
        return pub;
    }

    // -----------------------------------------------------------------------
    // Egress subscription: test harness subscribes on the per-session channel.
    // -----------------------------------------------------------------------
    std::shared_ptr<aeron::Subscription> makeEgressSub(int32_t egressStreamId) {
        int64_t regId = aeron_->addSubscription("aeron:ipc", egressStreamId);
        std::shared_ptr<aeron::Subscription> sub;
        pump([&]() {
            if (!sub) sub = aeron_->findSubscription(regId);
            return sub != nullptr;
        }, kPumpIter, "makeEgressSub resolve");
        return sub;
    }

    // -----------------------------------------------------------------------
    // offerBounded — publish bytes with bounded retry.
    // -----------------------------------------------------------------------
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

    // -----------------------------------------------------------------------
    // pump — drive gateway doWork(nowNs) + optional condition, with hard cap.
    // -----------------------------------------------------------------------
    template <typename DoneFn>
    void pumpNs(DoneFn&& done, int64_t nowNs,
                int maxIter = kPumpIter,
                std::string_view label = "pump") {
        using clock = std::chrono::steady_clock;
        const auto deadline = clock::now() + std::chrono::seconds{15};
        for (int i = 0; i < maxIter && clock::now() < deadline; ++i) {
            if (gw_) gw_->doWork(nowNs);
            if (done()) return;
            std::this_thread::sleep_for(std::chrono::microseconds{200});
        }
        ASSERT_TRUE(done()) << "pumpNs timed out: " << label;
    }

    template <typename DoneFn>
    void pump(DoneFn&& done,
              int maxIter = kPumpIter,
              std::string_view label = "pump") {
        pumpNs(std::forward<DoneFn>(done), wallNowNs(), maxIter, label);
    }

    // -----------------------------------------------------------------------
    // encodeConnectRequest — encode a ConnectRequest into buf, return length.
    // -----------------------------------------------------------------------
    static std::size_t encodeConnect(uint8_t* buf, std::size_t bufLen,
                                     int64_t correlationId,
                                     const std::string& egressChannel,
                                     int32_t egressStreamId) {
        pc::ConnectRequest req;
        req.sessionId       = 0;
        req.correlationId   = correlationId;
        req.egressStreamId  = egressStreamId;
        req.clientVersion   = 1;
        req.egressChannel   = egressChannel;
        EXPECT_LE(req.egressChannel.size() + 32, bufLen);
        return req.encode(buf);
    }

    std::unique_ptr<ClientGateway> gw_;
    std::shared_ptr<aeron::Aeron>  aeron_;

private:
    void startDriver() {
        const std::string aeronDir = (tmpDir_->path / "aeron").string();
        fs::create_directories(tmpDir_->path);

        aeron_driver_context_init(&driverCtx_);
        aeron_driver_context_set_dir(driverCtx_, aeronDir.c_str());
        aeron_driver_context_set_threading_mode(driverCtx_,
                                                AERON_THREADING_MODE_SHARED);
        aeron_driver_context_set_dir_delete_on_start(driverCtx_, true);
        aeron_driver_context_set_dir_delete_on_shutdown(driverCtx_, true);
        aeron_driver_context_set_shared_idle_strategy(driverCtx_, "sleep-ns");
        aeron_driver_context_set_term_buffer_sparse_file(driverCtx_, true);
        aeron_driver_context_set_term_buffer_length(driverCtx_, 64 * 1024);

        ASSERT_EQ(aeron_driver_init(&driver_, driverCtx_), 0)
            << "aeron_driver_init failed: " << aeron_errmsg();
        ASSERT_EQ(aeron_driver_start(driver_, /*manual_main_loop=*/true), 0)
            << "aeron_driver_start failed: " << aeron_errmsg();

        driverRunning_.store(true, std::memory_order_release);
        driverThread_ = std::thread([this]() {
            while (driverRunning_.load(std::memory_order_acquire)) {
                aeron_driver_main_idle_strategy(
                    driver_, aeron_driver_main_do_work(driver_));
            }
        });

        // No caller-side pumping: aeron_driver_init/start already wrote
        // cnc.dat and stamped the heartbeat synchronously before driverThread_
        // even starts; pumping here would be a second concurrent pumper on
        // the single-pumper driver (spec Sec 3.6).
        const std::string cncPath = aeronDir + "/cnc.dat";
        ASSERT_TRUE(fs::exists(cncPath))
            << "Aeron CnC file not created within timeout";
    }

    void connectAeron() {
        const std::string aeronDir = (tmpDir_->path / "aeron").string();
        aeron::Context ctx;
        ctx.aeronDir(aeronDir);
        aeron_ = aeron::Aeron::connect(ctx);
        ASSERT_NE(aeron_, nullptr) << "Aeron::connect failed";
    }

    void stopDriver() {
        if (driverRunning_.exchange(false, std::memory_order_acq_rel)) {
            if (driverThread_.joinable()) driverThread_.join();
        }
        if (driver_) {
            static constexpr int kDrainMs = 50;
            const auto deadline =
                std::chrono::steady_clock::now() + std::chrono::milliseconds{kDrainMs};
            while (std::chrono::steady_clock::now() < deadline) {
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
// GW-DR-1: Timeout-drop test.
//
// Send a ConnectRequest whose egressChannel is "aeron:udp?endpoint=127.0.0.1:1"
// (a valid UDP channel with no subscriber, so the session can never be used).
// The publication itself DOES resolve (Aeron registers a UDP ExclusivePublication
// even without a subscriber — creation is independent of connectivity).
//
// Strategy with injected clock:
//   1. Send Connect at tBase.
//   2. Drive doWork with tBase — pending entry enters the table.
//   3. Drive doWork with tBase + kConnectTimeoutNs + 1 — timeout fires, the
//      pending entry is dropped, and the drain table is also polled.
//   4. Keep pumping until pendingConnectCount() == 0 (drain table clears once
//      the publication resolves and is released).
//   5. Assert sessionCount() == 0 (no session created for the bogus egress).
//
// Outcome for 127.0.0.1:1: Aeron UDP ExclusivePublication resolves (driver
// allocates the log buffer + port) — it does NOT wait for a subscriber.  So
// the drain table clears within a few doWork cycles, releasing the registration.
// ===========================================================================
TEST_F(GatewayAsyncDropFixture, Timeout_BogusEgress_PendingDrainsToZero) {
    auto ingressPub = makeIngressPub();

    // t=0 (synthetic base for this test).
    const int64_t tBase = 1'000'000'000LL;

    // Send ConnectRequest pointing at a UDP endpoint with no subscriber.
    uint8_t sc[256] = {};
    const std::size_t reqLen = encodeConnect(
        sc, sizeof(sc),
        /*correlationId=*/1001,
        "aeron:udp?endpoint=127.0.0.1:1",
        kEgressBase + 1);
    offerBounded(*ingressPub, sc, reqLen, "Connect-bogus-egress");

    // Drive at tBase so the gateway picks up the Connect.
    pumpNs([&]() { return gw_->pendingConnectCount() > 0; },
           tBase, kPumpIter, "pendingCount > 0");

    // There must be exactly one pending entry.
    EXPECT_EQ(gw_->pendingConnectCount(), 1u)
        << "One pending entry expected after sending a single Connect";

    // Advance past timeout: tBase + connectResolveTimeoutNs + 1.
    const int64_t tTimeout = tBase + kConnectTimeoutNs + 1;

    // Drive doWork at tTimeout — timeout-drop fires and moves the entry to
    // the drain table (if the publication hasn't resolved yet) OR drops it
    // immediately (if already resolved).
    // Keep pumping until pendingConnectCount() reaches 0 (drain table clears).
    pumpNs([&]() { return gw_->pendingConnectCount() == 0; },
           tTimeout, kPumpIter, "pendingCount == 0 after timeout");

    EXPECT_EQ(gw_->pendingConnectCount(), 0u)
        << "Pending table + drain table must be empty after timeout and release";
    EXPECT_EQ(gw_->sessionCount(), 0u)
        << "No session must be created for a timed-out bogus egress";
}

// ===========================================================================
// GW-DR-2: Cap/eviction test.
//
// Spam kMaxPendingConnects + 10 Connects in rapid succession, each pointing
// at the bogus UDP endpoint.  The pending table must never exceed
// kMaxPendingConnects simultaneously — evictions occur inline before each
// new enqueue, releasing the oldest registration via drainingRegistrations_.
//
// Strategy: drive doWork at a FROZEN tBase (before timeout) between each
// sent Connect, so publications may resolve and get promoted to sessions
// OR timeout before we advance the clock.  After sending all Connects,
// advance the clock past the timeout to drain the remainder.  Assert:
//   - At no point did we ASSERT/crash (bounded send loop).
//   - After the timeout sweep, pendingConnectCount() == 0.
//   - sessionCount() is whatever it is (sessions from resolved pubs are
//     acceptable; the cap prevents the PENDING table from being unbounded).
//
// The test exercises the eviction code path (the warn log "cap reached")
// when publications don't resolve before the next Connect arrives.
// ===========================================================================
TEST_F(GatewayAsyncDropFixture, CapEviction_BoundedPendingTable) {
    auto ingressPub = makeIngressPub();

    // Use a frozen clock so publications don't timeout during the spam phase.
    const int64_t tBase = 2'000'000'000LL;

    // Total Connects to send: cap + 10 extra to exercise eviction.
    const std::size_t kTotal = ClientGateway::kMaxPendingConnects + 10;

    // Track observed peak pending count during the spam phase.
    std::size_t peakPending = 0;

    for (std::size_t i = 0; i < kTotal; ++i) {
        uint8_t sc[256] = {};
        const std::size_t reqLen = encodeConnect(
            sc, sizeof(sc),
            static_cast<int64_t>(2000 + i),
            // Use unique UDP streams to avoid channel-reuse issues.
            "aeron:udp?endpoint=127.0.0.1:1",
            kEgressBase + 50 + static_cast<int32_t>(i));
        offerBounded(*ingressPub, sc, reqLen, "Connect-spam");

        // Drive a few doWork cycles at frozen tBase to drain ingress.
        for (int j = 0; j < 10; ++j) {
            gw_->doWork(tBase);
            std::this_thread::sleep_for(std::chrono::microseconds{100});
        }

        // Record peak — the sum of pending + draining must not grow unbounded.
        const std::size_t cur = gw_->pendingConnectCount();
        if (cur > peakPending) peakPending = cur;
    }

    // Peak combined (pending + drain) must be bounded at most 2*cap:
    // - main table: bounded at kMaxPendingConnects
    // - drain table: evicted entries, also bounded at kMaxPendingConnects
    EXPECT_LE(peakPending,
              ClientGateway::kMaxPendingConnects * 2)
        << "Combined pending + drain table must be bounded (≤ 2*cap="
        << (ClientGateway::kMaxPendingConnects * 2) << "); observed peak=" << peakPending;

    // Now advance past timeout to drain all remaining pending + drain entries.
    const int64_t tTimeout = tBase + kConnectTimeoutNs + 1;
    pumpNs([&]() { return gw_->pendingConnectCount() == 0; },
           tTimeout, kPumpIter, "pendingCount == 0 after cap+timeout");

    EXPECT_EQ(gw_->pendingConnectCount(), 0u)
        << "All pending + drain entries must clear after timeout";
    // sessionCount may be > 0 if some UDP pubs resolved before timeout — that is
    // correct behaviour (we only assert the PENDING table stays bounded, not that
    // sessions aren't created from successfully resolved publications).
}

// ===========================================================================
// GW-DR-3: Resolve-after-cycles (positive path).
//
// A Connect whose egress is a valid aeron:ipc channel (no subscriber needed
// for the publication to resolve) must:
//   1. Enter the pending table on the first doWork cycle.
//   2. Resolve within several subsequent doWork cycles.
//   3. Produce ConnectResponse{OK} on the egress subscription.
//   4. pendingConnectCount() → 0 after resolution.
//   5. sessionCount() == 1.
//
// This test exercises the resolve-after-N-cycles branch of pollPendingConnects
// on a live gateway with a real egress subscription to receive the response.
// ===========================================================================
TEST_F(GatewayAsyncDropFixture, ResolveAfterCycles_SessionCreated_PendingClears) {
    auto ingressPub  = makeIngressPub();
    auto egressSub   = makeEgressSub(kEgressBase + 200);

    const int64_t tBase = 3'000'000'000LL;

    uint8_t sc[256] = {};
    const std::size_t reqLen = encodeConnect(
        sc, sizeof(sc),
        /*correlationId=*/3001,
        "aeron:ipc",
        kEgressBase + 200);
    offerBounded(*ingressPub, sc, reqLen, "Connect-good");

    // Pump with a FROZEN gateway clock until ConnectResponse arrives: the
    // 10 ms connect timeout must never race the client conductor's own
    // resolution latency (wall-clock pumping made this test timing-dependent).
    // The wall deadline below only bounds the loop.
    std::optional<pc::ConnectResponse> resp;
    aeron::FragmentAssembler respAsm(
        [&](aeron::concurrent::AtomicBuffer& buf,
            aeron::util::index_t             offset,
            aeron::util::index_t             len,
            aeron::Header&) {
            auto dec = pc::ConnectResponse::decode(
                reinterpret_cast<const uint8_t*>(buf.buffer() + offset),
                static_cast<std::size_t>(len));
            if (dec) resp = *dec;
        });

    using clock = std::chrono::steady_clock;
    const auto deadline = clock::now() + std::chrono::seconds{10};
    for (int i = 0; i < kPumpIter && clock::now() < deadline; ++i) {
        gw_->doWork(tBase);
        egressSub->poll(respAsm.handler(), 10);
        if (resp.has_value()) break;
        std::this_thread::sleep_for(std::chrono::microseconds{200});
    }

    ASSERT_TRUE(resp.has_value()) << "ConnectResponse not received within timeout";
    EXPECT_EQ(resp->code, static_cast<int16_t>(pc::ErrorCode::OK));
    EXPECT_GE(resp->assignedSessionId, 1);
    EXPECT_EQ(resp->correlationId, 3001LL);

    // After resolution: pending table must be empty.
    EXPECT_EQ(gw_->pendingConnectCount(), 0u)
        << "Pending table must be empty after resolution";
    EXPECT_EQ(gw_->sessionCount(), 1u)
        << "One session must be active after successful Connect";
}

// ===========================================================================
// GW-DR-4: No-subscriber / not-yet-connected → no orphaned session (FIX A +
//          DR-BUG-2 stash-and-retry).
//
// Intent: when ConnectResponse offer fails (publication not yet connected to
// any subscriber), the gateway MUST NOT create a server-side session the client
// never learned about.
//
// DR-BUG-2 FIX changes the contract here:
//   BEFORE: on failed send → entry erased immediately (pub released).
//   AFTER:  on failed send → entry KEPT, pub STASHED, send retried each cycle.
//           Entry is only removed once the send succeeds (session created) OR
//           the connectResolveTimeoutNs elapses.
//
// Strategy:
//   1. Send a Connect whose egressChannel is a valid aeron:ipc stream but for
//      which NO egress subscriber exists yet (the gateway's ExclusivePublication
//      resolves but isConnected() returns false → sendOnPub returns false).
//   2. Drive doWork at a FROZEN tBase (before timeout) until the pub resolves
//      and send attempts begin.  Assert sessionCount() == 0 throughout.
//   3. Advance clock past timeout (tBase + kConnectTimeoutNs + 1 ns).
//   4. After timeout sweep: pendingConnectCount()==0 (entry dropped cleanly).
//   5. sessionCount() must still be 0 (FIX A: no orphaned session ever created).
//
// The test is deterministic because the egress stream intentionally has no
// subscriber — isConnected() reliably returns false until a subscriber joins.
// ===========================================================================
TEST_F(GatewayAsyncDropFixture, BackPressure_SendFails_NoOrphanSession) {
    auto ingressPub = makeIngressPub();

    // Use a stream that has NO subscriber — publication resolves but
    // isConnected() returns false → sendOnPub returns false on every attempt.
    constexpr int32_t kNoSubEgressStream = kEgressBase + 300;

    // Frozen base timestamp: entries won't time out while we drive at tBase.
    const int64_t tBase = 4'000'000'000LL;

    uint8_t sc[256] = {};
    const std::size_t reqLen = encodeConnect(
        sc, sizeof(sc),
        /*correlationId=*/4001,
        "aeron:ipc",
        kNoSubEgressStream);
    offerBounded(*ingressPub, sc, reqLen, "Connect-no-subscriber");

    // Phase 1: drive at tBase until the pending entry appears.
    pumpNs([&]() { return gw_->pendingConnectCount() > 0; },
           tBase, kPumpIter, "pendingCount > 0");
    EXPECT_EQ(gw_->sessionCount(), 0u)
        << "No session must be created while retrying ConnectResponse";

    // Phase 2: drive a while longer at tBase — the pub resolves and sendOnPub
    // is attempted repeatedly (stash-and-retry).  Session must stay 0.
    for (int i = 0; i < 200; ++i) {
        gw_->doWork(tBase);
        ASSERT_EQ(gw_->sessionCount(), 0u)
            << "Orphaned session detected at iteration " << i
            << " (FIX A violation: session created before ConnectResponse was sent)";
        std::this_thread::sleep_for(std::chrono::microseconds{200});
    }

    EXPECT_EQ(gw_->pendingConnectCount(), 1u)
        << "entry must be kept (stash-and-retry) until the timeout, not dropped by a re-find throw";

    // Phase 3: advance past timeout — entry must be dropped cleanly.
    const int64_t tTimeout = tBase + kConnectTimeoutNs + 1;
    pumpNs([&]() { return gw_->pendingConnectCount() == 0; },
           tTimeout, kPumpIter, "pendingCount == 0 after timeout");

    // After timeout: pending table must be empty (entry dropped + released).
    EXPECT_EQ(gw_->pendingConnectCount(), 0u)
        << "Pending entry must be erased after connectResolveTimeoutNs (pub released)";

    // No orphan: session count must still be zero.
    EXPECT_EQ(gw_->sessionCount(), 0u)
        << "No session must be created when ConnectResponse send never succeeded (FIX A)";
}

// Regression: Aeron 1.51 findExclusivePublication erases the registration after
// the first non-null return; re-finding a stashed pub threw "Unknown registration
// id" and dropped the Connect.  A late egress subscriber must still get its
// ConnectResponse.
TEST_F(GatewayAsyncDropFixture, LateEgressSubscriberStillGetsConnectResponseRegression) {
    auto ingressPub = makeIngressPub();
    constexpr int32_t kLateStream = kEgressBase + 400;
    const int64_t tBase = 5'000'000'000LL;

    uint8_t sc[256] = {};
    const std::size_t reqLen = encodeConnect(sc, sizeof(sc), /*correlationId=*/5001,
                                             "aeron:ipc", kLateStream);
    offerBounded(*ingressPub, sc, reqLen, "Connect-late-subscriber");

    pumpNs([&]() { return gw_->pendingConnectCount() > 0; },
           tBase, kPumpIter, "pendingCount > 0");
    for (int i = 0; i < 200; ++i) {
        gw_->doWork(tBase);
        std::this_thread::sleep_for(std::chrono::microseconds{200});
    }
    ASSERT_EQ(gw_->pendingConnectCount(), 1u)
        << "entry must be kept (stash-and-retry), not dropped by a re-find throw";
    ASSERT_EQ(gw_->sessionCount(), 0u);

    // Create the subscription WITHOUT pumping the gateway (makeEgressSub pumps at the wall clock,
    // which would time the frozen-clock pending entry out).
    const int64_t subReg = aeron_->addSubscription("aeron:ipc", kLateStream);
    std::shared_ptr<aeron::Subscription> egressSub;
    for (int i = 0; i < kPumpIter && !egressSub; ++i) {
        egressSub = aeron_->findSubscription(subReg);
        if (!egressSub) std::this_thread::sleep_for(std::chrono::microseconds{200});
    }
    ASSERT_NE(egressSub, nullptr) << "late egress subscription did not resolve";
    std::optional<pc::ConnectResponse> resp;
    aeron::FragmentAssembler respAsm(
        [&](aeron::concurrent::AtomicBuffer& buf, aeron::util::index_t offset,
            aeron::util::index_t len, aeron::Header&) {
            auto dec = pc::ConnectResponse::decode(
                reinterpret_cast<const uint8_t*>(buf.buffer() + offset),
                static_cast<std::size_t>(len));
            if (dec) resp = *dec;
        });
    using clock = std::chrono::steady_clock;
    const auto deadline = clock::now() + std::chrono::seconds{10};
    for (int i = 0; i < kPumpIter && clock::now() < deadline; ++i) {
        gw_->doWork(tBase);
        egressSub->poll(respAsm.handler(), 10);
        if (resp.has_value()) break;
        std::this_thread::sleep_for(std::chrono::microseconds{200});
    }
    ASSERT_TRUE(resp.has_value()) << "ConnectResponse not received after late subscriber";
    EXPECT_EQ(resp->code, static_cast<int16_t>(pc::ErrorCode::OK));
    EXPECT_EQ(resp->correlationId, 5001LL);
    EXPECT_EQ(gw_->pendingConnectCount(), 0u);
    EXPECT_EQ(gw_->sessionCount(), 1u);
}

} // namespace
} // namespace rune::gateway
