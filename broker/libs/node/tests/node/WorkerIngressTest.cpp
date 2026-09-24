// ---------------------------------------------------------------------------
// WorkerIngressTest.cpp — unit tests for WorkerIngress (E5.1.2a).
//
// Reuses the GatewayLoopbackFixture's embedded Aeron IPC driver pattern
// (same code structure as GatewayLoopbackTest.cpp / BrokerNodeGatewayTest.cpp).
//
// Tests:
//
//   1. DeliverExactBytes
//        Publish a known payload on stream S via an ExclusivePublication →
//        poll WorkerIngress on stream S → assert handler receives exact bytes
//        and correct length (zero-copy compare).
//
//   2. StreamIsolation
//        Two WorkerIngress on different stream ids (S1, S2); two publications
//        (one per stream).  Publish A on S1, B on S2.  Assert:
//          - ingress on S1 receives A and never B.
//          - ingress on S2 receives B and never A.
//        (stream-level isolation: each worker sees only its own stream's frames.)
//
// Fixture: one embedded Aeron media driver (SHARED threading, IPC, sparse
// term buffers, 64 KiB term length) + one shared Aeron client.  Mirrors the
// GatewayLoopbackFixture in libs/gateway/tests/integration/GatewayLoopbackTest.cpp.
// ---------------------------------------------------------------------------

#include <rune/node/WorkerIngress.h>

#include <Aeron.h>
#include <ExclusivePublication.h>
#include <Subscription.h>
#include <concurrent/AtomicBuffer.h>

#include <gtest/gtest.h>

extern "C" {
#include <aeronmd/aeronmd.h>
}

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <memory>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

namespace fs = std::filesystem;
using namespace std::chrono_literals;

namespace rune::node {
namespace {

// ---------------------------------------------------------------------------
// Unique temp-path generator (getpid + atomic counter)
// ---------------------------------------------------------------------------
static fs::path makeTempDir(std::string_view tag = "") {
    static std::atomic<uint32_t> g_seq{0};
    auto seq  = g_seq.fetch_add(1, std::memory_order_relaxed);
    auto name = std::string("rune_wi_") + std::string(tag) +
                std::to_string(static_cast<long>(::getpid())) +
                "_" + std::to_string(seq);
    return fs::temp_directory_path() / name;
}

struct TempDir {
    fs::path path;
    explicit TempDir(std::string_view tag = "")
        : path(makeTempDir(tag)) {}
    ~TempDir() {
        std::error_code ec;
        fs::remove_all(path, ec);
    }
};

// ---------------------------------------------------------------------------
// WorkerIngressFixture — one embedded Aeron IPC driver + one Aeron client.
//
// Mirrors the driver startup/shutdown pattern of GatewayLoopbackFixture
// (libs/gateway/tests/integration/GatewayLoopbackTest.cpp:103-340).
// ---------------------------------------------------------------------------
class WorkerIngressFixture : public ::testing::Test {
public:
    static constexpr int kPollIterMax = 50'000;

    void SetUp() override {
        tmpDir_ = std::make_unique<TempDir>("fix_");
        startDriver();
        connectAeron();
    }

    void TearDown() override {
        aeron_.reset();
        stopDriver();
        tmpDir_.reset();
    }

    // -----------------------------------------------------------------------
    // makeExclusivePub — resolve an ExclusivePublication on (channel, stream).
    // -----------------------------------------------------------------------
    std::shared_ptr<aeron::ExclusivePublication> makeExclusivePub(
            const std::string& channel, int32_t stream) {
        int64_t regId = aeron_->addExclusivePublication(channel, stream);
        std::shared_ptr<aeron::ExclusivePublication> pub;
        waitBounded([&]() {
            pub = aeron_->findExclusivePublication(regId);
            return pub != nullptr;
        }, "makeExclusivePub");
        return pub;
    }

    // -----------------------------------------------------------------------
    // offerBounded — offer a raw byte buffer on a publication.
    // -----------------------------------------------------------------------
    static void offerBounded(aeron::ExclusivePublication& pub,
                             const uint8_t* data, std::size_t len,
                             std::string_view label = "offer") {
        aeron::concurrent::AtomicBuffer ab(
            const_cast<uint8_t*>(data),
            static_cast<aeron::util::index_t>(len));

        bool sent = false;
        for (int i = 0; i < kPollIterMax && !sent; ++i) {
            int64_t pos = pub.offer(ab, 0, static_cast<aeron::util::index_t>(len));
            if (pos > 0) {
                sent = true;
            } else {
                std::this_thread::sleep_for(100us);
            }
        }
        ASSERT_TRUE(sent) << "offerBounded timed out: " << label;
    }

    // -----------------------------------------------------------------------
    // pollUntil — drive WorkerIngress until done() returns true or cap reached.
    // -----------------------------------------------------------------------
    template <typename Handler, typename DoneFn>
    void pollUntil(WorkerIngress& ingress, int fragLimit,
                   Handler&& handler, DoneFn&& done,
                   std::string_view label = "pollUntil") {
        using clk = std::chrono::steady_clock;
        const auto deadline = clk::now() + std::chrono::seconds{10};
        for (int i = 0; i < kPollIterMax && clk::now() < deadline; ++i) {
            ingress.poll(fragLimit, handler);
            if (done()) return;
            std::this_thread::sleep_for(100us);
        }
        ASSERT_TRUE(done()) << "pollUntil timed out: " << label;
    }

    std::shared_ptr<aeron::Aeron>  aeron_;

private:
    // -----------------------------------------------------------------------
    // waitBounded — spin until cond() returns true or we hit the cap.
    // -----------------------------------------------------------------------
    template <typename Cond>
    void waitBounded(Cond&& cond, std::string_view label = "wait") {
        using clk = std::chrono::steady_clock;
        const auto deadline = clk::now() + std::chrono::seconds{10};
        for (int i = 0; i < kPollIterMax && clk::now() < deadline; ++i) {
            if (cond()) return;
            std::this_thread::sleep_for(1ms);
        }
        ASSERT_TRUE(cond()) << "waitBounded timed out: " << label;
    }

    void startDriver() {
        const std::string aeronDir = (tmpDir_->path / "aeron").string();
        fs::create_directories(tmpDir_->path);

        // Mirror GatewayLoopbackFixture::startDriver() exactly.
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
                    driver_,
                    aeron_driver_main_do_work(driver_));
            }
        });

        // Wait for cnc.dat to appear (bounded).
        const std::string cncPath = aeronDir + "/cnc.dat";
        for (int i = 0; i < 400; ++i) {
            aeron_driver_main_do_work(driver_);
            if (fs::exists(cncPath)) break;
            std::this_thread::sleep_for(1ms);
        }
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
            // Drain residual work.
            const auto deadline =
                std::chrono::steady_clock::now() + 50ms;
            while (std::chrono::steady_clock::now() < deadline) {
                if (aeron_driver_main_do_work(driver_) == 0) {
                    aeron_driver_main_do_work(driver_);
                    break;
                }
                std::this_thread::sleep_for(200us);
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
// TEST 1: DeliverExactBytes
//
// Publish a known payload on stream S via ExclusivePublication.
// Poll WorkerIngress until handler is called.
// Assert handler receives the exact bytes (zero-copy compare) and length.
// ===========================================================================
TEST_F(WorkerIngressFixture, DeliverExactBytes) {
    constexpr int32_t      kStream  = 42;
    const     std::string  kChannel = "aeron:ipc";
    constexpr std::size_t  kPayloadLen = 32;

    // Known payload with a recognisable pattern.
    uint8_t payload[kPayloadLen];
    for (std::size_t i = 0; i < kPayloadLen; ++i) {
        payload[i] = static_cast<uint8_t>(0xA0 ^ (i & 0xFF));
    }

    // Create ingress first so the publication can discover the subscriber.
    WorkerIngress ingress(aeron_, kChannel, kStream);
    ASSERT_TRUE(ingress.resolved()) << "WorkerIngress did not resolve";
    ASSERT_EQ(ingress.streamId(), kStream);

    // Create publisher.
    auto pub = makeExclusivePub(kChannel, kStream);
    ASSERT_TRUE(pub != nullptr);

    // Wait for the publication to connect to the subscription's image.
    {
        using clk = std::chrono::steady_clock;
        const auto deadline = clk::now() + std::chrono::seconds{10};
        while (clk::now() < deadline && !pub->isConnected()) {
            ingress.poll(10, [](const uint8_t*, std::size_t, aeron::Header&) {});
            std::this_thread::sleep_for(1ms);
        }
        ASSERT_TRUE(pub->isConnected()) << "Publication never connected";
    }

    // Offer the payload.
    offerBounded(*pub, payload, kPayloadLen, "DeliverExactBytes");

    // Poll until received.
    bool    received = false;
    bool    bytesOk  = false;
    std::size_t receivedLen = 0;

    pollUntil(
        ingress, /*fragLimit=*/10,
        [&](const uint8_t* data, std::size_t len, aeron::Header& /*hdr*/) {
            received    = true;
            receivedLen = len;
            // Zero-copy compare: data points directly into the term buffer.
            bytesOk = (len == kPayloadLen) &&
                      (std::memcmp(data, payload, kPayloadLen) == 0);
        },
        [&]() { return received; },
        "DeliverExactBytes");

    ASSERT_TRUE(received)                  << "Handler never invoked";
    ASSERT_EQ(receivedLen,   kPayloadLen)  << "Length mismatch";
    ASSERT_TRUE(bytesOk)                   << "Payload bytes mismatch";
}

// ===========================================================================
// TEST 2: StreamIsolation
//
// Two WorkerIngress on different stream ids (S1, S2).
// Publish payload A on S1, payload B on S2.
// Assert:
//   - ingress1 (S1) receives A with correct bytes, never B.
//   - ingress2 (S2) receives B with correct bytes, never A.
// ===========================================================================
TEST_F(WorkerIngressFixture, StreamIsolation) {
    constexpr int32_t      kStream1 = 100;
    constexpr int32_t      kStream2 = 200;
    const     std::string  kChannel = "aeron:ipc";

    // Distinct payloads.
    const uint8_t payloadA[] = {0x01, 0x02, 0x03, 0x04, 0xAA, 0xBB};
    const uint8_t payloadB[] = {0x10, 0x20, 0x30, 0x40, 0xCC, 0xDD};
    constexpr std::size_t kLenA = sizeof(payloadA);
    constexpr std::size_t kLenB = sizeof(payloadB);

    // Create both ingress objects first.
    WorkerIngress ingress1(aeron_, kChannel, kStream1);
    WorkerIngress ingress2(aeron_, kChannel, kStream2);
    ASSERT_TRUE(ingress1.resolved()) << "ingress1 did not resolve";
    ASSERT_TRUE(ingress2.resolved()) << "ingress2 did not resolve";
    ASSERT_EQ(ingress1.streamId(), kStream1);
    ASSERT_EQ(ingress2.streamId(), kStream2);

    // Create publishers.
    auto pub1 = makeExclusivePub(kChannel, kStream1);
    auto pub2 = makeExclusivePub(kChannel, kStream2);
    ASSERT_TRUE(pub1 != nullptr);
    ASSERT_TRUE(pub2 != nullptr);

    // Wait for both publications to connect.
    {
        using clk = std::chrono::steady_clock;
        const auto deadline = clk::now() + std::chrono::seconds{10};
        while (clk::now() < deadline &&
               (!pub1->isConnected() || !pub2->isConnected())) {
            // Pump both ingress subscriptions so images are established.
            auto noop = [](const uint8_t*, std::size_t, aeron::Header&) {};
            ingress1.poll(10, noop);
            ingress2.poll(10, noop);
            std::this_thread::sleep_for(1ms);
        }
        ASSERT_TRUE(pub1->isConnected()) << "pub1 never connected";
        ASSERT_TRUE(pub2->isConnected()) << "pub2 never connected";
    }

    // Offer A on stream 1, B on stream 2.
    offerBounded(*pub1, payloadA, kLenA, "StreamIsolation A");
    offerBounded(*pub2, payloadB, kLenB, "StreamIsolation B");

    // Receive from ingress1 — must be A.
    bool        recv1     = false;
    bool        bytes1Ok  = false;
    bool        saw1Wrong = false;

    pollUntil(
        ingress1, 10,
        [&](const uint8_t* data, std::size_t len, aeron::Header& /*hdr*/) {
            recv1 = true;
            if (len == kLenA && std::memcmp(data, payloadA, kLenA) == 0) {
                bytes1Ok = true;
            } else {
                saw1Wrong = true;  // received something that is not A
            }
        },
        [&]() { return recv1; },
        "StreamIsolation recv1");

    ASSERT_TRUE(recv1)     << "ingress1 received nothing";
    ASSERT_TRUE(bytes1Ok)  << "ingress1 received wrong payload (not A)";
    ASSERT_FALSE(saw1Wrong)<< "ingress1 received unexpected data";

    // Receive from ingress2 — must be B.
    bool        recv2     = false;
    bool        bytes2Ok  = false;
    bool        saw2Wrong = false;

    pollUntil(
        ingress2, 10,
        [&](const uint8_t* data, std::size_t len, aeron::Header& /*hdr*/) {
            recv2 = true;
            if (len == kLenB && std::memcmp(data, payloadB, kLenB) == 0) {
                bytes2Ok = true;
            } else {
                saw2Wrong = true;
            }
        },
        [&]() { return recv2; },
        "StreamIsolation recv2");

    ASSERT_TRUE(recv2)     << "ingress2 received nothing";
    ASSERT_TRUE(bytes2Ok)  << "ingress2 received wrong payload (not B)";
    ASSERT_FALSE(saw2Wrong)<< "ingress2 received unexpected data";

    // Drain both subscriptions one more time to confirm no cross-talk
    // (no additional frames arrive on either ingress).
    int extraOnIngress1 = 0;
    int extraOnIngress2 = 0;
    for (int i = 0; i < 200; ++i) {
        extraOnIngress1 += ingress1.poll(10,
            [&](const uint8_t*, std::size_t, aeron::Header&) {});
        extraOnIngress2 += ingress2.poll(10,
            [&](const uint8_t*, std::size_t, aeron::Header&) {});
        std::this_thread::sleep_for(100us);
    }
    EXPECT_EQ(extraOnIngress1, 0) << "ingress1 received extra frames (cross-talk?)";
    EXPECT_EQ(extraOnIngress2, 0) << "ingress2 received extra frames (cross-talk?)";
}

} // namespace
} // namespace rune::node
