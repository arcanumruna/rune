// WorkerEgressTest.cpp — per-worker dynamic-MDC egress publication (read-rework
// T3/T4, increment I1).  Verifies a WorkerEgress resolves its own dynamic-MDC
// ExclusivePublication and that a self-registering subscriber receives frames
// sent by the (single) owner thread — plus fan-out to two subscribers.
//
// Uses an embedded UDP-capable Aeron media driver (same pattern validated by the
// MDC egress spike).
#include <rune/aeronutil/testing/FreePort.h>
#include <rune/node/EmbeddedDriver.h>
#include <rune/node/WorkerEgress.h>

#include <Aeron.h>
#include <Subscription.h>
#include <FragmentAssembler.h>
#include <concurrent/AtomicBuffer.h>
#include <gtest/gtest.h>
#include <aeronmd/aeronmd.h>

#include <atomic>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <string>
#include <thread>
#include <unistd.h>

using namespace std::chrono_literals;
using namespace rune::node;

namespace {

constexpr int32_t kStream = 2001;
// D11: loopback endpoints from the shared allocator (20000..31999, never bind-0) — the old
// fixed 24xxx ports collided with the allocator's ports in concurrently running tests.
inline std::string freeEndpoint() { return "127.0.0.1:" + std::to_string(rune::aeronutil::testing::allocFreeUdpPort()); }
const std::string kControlEp = freeEndpoint();
const char* const kControl = kControlEp.c_str();

class WorkerEgressFixture : public ::testing::Test {
protected:
    std::unique_ptr<rune::node::EmbeddedDriver> driver_;
    std::shared_ptr<aeron::Aeron> aeron_;
    std::string aeronDir_;

    void SetUp() override {
        startDriver();
        if (HasFatalFailure()) return;  // driver failed to start: don't block in connect
        aeron::Context ctx;
        ctx.aeronDir(aeronDir_);
        aeron_ = aeron::Aeron::connect(ctx);
        ASSERT_NE(aeron_, nullptr);
    }
    void TearDown() override { aeron_.reset(); stopDriver(); }

    void startDriver() {
        aeronDir_ = (std::filesystem::temp_directory_path() /
                     ("rune_wegress_" + std::to_string(::getpid()))).string();
        std::filesystem::remove_all(aeronDir_);
        // One dedicated pumping thread (EmbeddedDriver::Threaded); no caller-side
        // readiness pumping needed — aeron_driver_init already wrote cnc.dat and
        // stamped the heartbeat synchronously before the driver thread starts.
        rune::node::EmbeddedDriverConfig cfg;
        cfg.dir             = aeronDir_;
        cfg.termBufferBytes = 64 * 1024;
        cfg.mtuBytes        = 1408;   // pinned: the fragmentation test needs maxPayload*3 < term/8
        try {
            driver_ = std::make_unique<rune::node::EmbeddedDriver>(cfg, rune::node::DriverMode::Threaded);
        } catch (const std::exception& e) {
            FAIL() << "EmbeddedDriver failed to start: " << e.what();
        }
    }
    void stopDriver() {
        driver_.reset();
    }

    template <typename Pred>
    bool waitFor(Pred p, std::chrono::milliseconds timeout = 5s) {
        auto deadline = std::chrono::steady_clock::now() + timeout;
        while (std::chrono::steady_clock::now() < deadline) {
            if (p()) return true;
            std::this_thread::sleep_for(1ms);
        }
        return p();
    }
    std::shared_ptr<aeron::Subscription> resolveSub(const std::string& ch, int32_t sid) {
        int64_t reg = aeron_->addSubscription(ch, sid);
        std::shared_ptr<aeron::Subscription> sub;
        waitFor([&]() { sub = aeron_->findSubscription(reg); return sub != nullptr; });
        return sub;
    }
    std::string subChannel(const char* endpoint) {
        return std::string("aeron:udp?control=") + kControl +
               "|control-mode=dynamic|endpoint=" + endpoint;
    }
};

TEST_F(WorkerEgressFixture, ResolvesAndDeliversToDynamicSubscribers) {
    WorkerEgress eg(aeron_, kControl, kStream);
    ASSERT_TRUE(eg.resolved()) << "WorkerEgress publication did not resolve";
    EXPECT_EQ(eg.streamId(), kStream);

    auto sub1 = resolveSub(subChannel(freeEndpoint().c_str()), kStream);
    auto sub2 = resolveSub(subChannel(freeEndpoint().c_str()), kStream);
    ASSERT_NE(sub1, nullptr);
    ASSERT_NE(sub2, nullptr);

    ASSERT_TRUE(waitFor([&]() {
        return eg.isConnected() && sub1->isConnected() && sub2->isConnected();
    })) << "egress/subs never connected";

    std::atomic<int> got1{0}, got2{0};
    aeron::FragmentAssembler fa1([&](aeron::concurrent::AtomicBuffer&, aeron::util::index_t,
                                     aeron::util::index_t, aeron::Header&) { got1++; });
    aeron::FragmentAssembler fa2([&](aeron::concurrent::AtomicBuffer&, aeron::util::index_t,
                                     aeron::util::index_t, aeron::Header&) { got2++; });

    // Owner thread sends (single writer) via WorkerEgress::send.
    uint8_t payload[128];
    std::memset(payload, 0x5A, sizeof(payload));
    auto sendOnce = [&]() {
        auto deadline = std::chrono::steady_clock::now() + 3s;
        while (std::chrono::steady_clock::now() < deadline) {
            if (eg.send(payload, sizeof(payload)) > 0) return true;
            std::this_thread::sleep_for(1ms);
        }
        return false;
    };

    ASSERT_TRUE(sendOnce());
    ASSERT_TRUE(waitFor([&]() {
        sub1->poll(fa1.handler(), 10);
        sub2->poll(fa2.handler(), 10);
        return got1.load() >= 1 && got2.load() >= 1;
    })) << "fan-out delivery failed: got1=" << got1 << " got2=" << got2;
}

TEST_F(WorkerEgressFixture, NullAeronIsUnresolvedAndSendIsNoop) {
    WorkerEgress eg(nullptr, kControl, kStream);
    EXPECT_FALSE(eg.resolved());
    uint8_t b[8] = {0};
    EXPECT_LE(eg.send(b, sizeof(b)), 0);  // no publication → non-positive result
}

// Accessors + channel format: dynamic-MDC channel embeds control-mode/control.
// NOTE: fc=min was intentionally REMOVED (min flow control paced the publisher to
// the slowest/late receiver and stalled the reader); default flow control is used.
TEST_F(WorkerEgressFixture, AccessorsAndChannelFormat) {
    const std::string ep = freeEndpoint();
    WorkerEgress eg(aeron_, ep.c_str(), 2099);
    ASSERT_TRUE(eg.resolved());
    EXPECT_EQ(eg.streamId(), 2099);
    EXPECT_EQ(eg.controlEndpoint(), ep);
    const std::string& ch = eg.channel();
    EXPECT_NE(ch.find("control-mode=dynamic"), std::string::npos) << ch;
    EXPECT_NE(ch.find("control=" + ep), std::string::npos) << ch;
    EXPECT_EQ(ch.find("fc=min"), std::string::npos) << "fc=min must NOT be present: " << ch;
    EXPECT_GT(eg.maxPayloadLength(), 0);
}

// send() with NO subscribers connected must not crash and must return <= 0
// (not-connected / back-pressure) so the caller does not advance its cursor.
TEST_F(WorkerEgressFixture, SendWithNoSubscribersReturnsNonPositive) {
    const std::string ep = freeEndpoint();
    WorkerEgress eg(aeron_, ep.c_str(), 2098);
    ASSERT_TRUE(eg.resolved());
    EXPECT_FALSE(eg.isConnected());  // no subscriber yet
    uint8_t payload[64];
    std::memset(payload, 0x11, sizeof(payload));
    EXPECT_LE(eg.send(payload, sizeof(payload)), 0)
        << "send with no connected subscriber must not be accepted";
}

// A frame larger than maxPayloadLength must be fragmented by offer() and
// reassembled into a single message by the subscriber's FragmentAssembler.
TEST_F(WorkerEgressFixture, LargeFrameFragmentsAndReassembles) {
    const std::string controlEp = freeEndpoint();
    const char* control = controlEp.c_str();
    WorkerEgress eg(aeron_, control, 2097);
    ASSERT_TRUE(eg.resolved());
    auto sub = resolveSub(std::string("aeron:udp?control=") + control +
                          "|control-mode=dynamic|endpoint=" + freeEndpoint(), 2097);
    ASSERT_NE(sub, nullptr);
    ASSERT_TRUE(waitFor([&]() { return eg.isConnected() && sub->isConnected(); }));

    const std::size_t big = static_cast<std::size_t>(eg.maxPayloadLength()) * 3 + 17;
    std::vector<uint8_t> payload(big, 0x7E);
    std::atomic<int> msgs{0};
    std::atomic<std::size_t> lastLen{0};
    aeron::FragmentAssembler fa([&](aeron::concurrent::AtomicBuffer&, aeron::util::index_t,
                                    aeron::util::index_t length, aeron::Header&) {
        msgs++; lastLen = static_cast<std::size_t>(length);
    });
    ASSERT_TRUE(waitFor([&]() { return eg.send(payload.data(), payload.size()) > 0; }, 3s));
    ASSERT_TRUE(waitFor([&]() { sub->poll(fa.handler(), 10); return msgs.load() >= 1; }))
        << "fragmented large frame was not reassembled";
    EXPECT_EQ(lastLen.load(), big) << "reassembled length must equal the sent frame size";
}

// A subscriber that joins AFTER the publication exists still receives subsequent
// sends (dynamic-MDC auto-add on connect).
TEST_F(WorkerEgressFixture, LateSubscriberReceivesSubsequentSends) {
    const std::string controlEp = freeEndpoint();
    const char* control = controlEp.c_str();
    WorkerEgress eg(aeron_, control, 2096);
    ASSERT_TRUE(eg.resolved());

    // Late subscriber joins now (after the pub existed).
    auto sub = resolveSub(std::string("aeron:udp?control=") + control +
                          "|control-mode=dynamic|endpoint=" + freeEndpoint(), 2096);
    ASSERT_NE(sub, nullptr);
    ASSERT_TRUE(waitFor([&]() { return eg.isConnected() && sub->isConnected(); }));

    std::atomic<int> got{0};
    aeron::FragmentAssembler fa([&](aeron::concurrent::AtomicBuffer&, aeron::util::index_t,
                                    aeron::util::index_t, aeron::Header&) { got++; });
    uint8_t payload[96];
    std::memset(payload, 0x33, sizeof(payload));
    ASSERT_TRUE(waitFor([&]() { return eg.send(payload, sizeof(payload)) > 0; }, 3s));
    ASSERT_TRUE(waitFor([&]() { sub->poll(fa.handler(), 10); return got.load() >= 1; }))
        << "late-joining subscriber did not receive subsequent send";
}

} // namespace

// ===========================================================================
// Re-check I6: the broker-side "per-worker MDC egress connected" line — none
// while no consumer is connected, exactly one over 100 passes once one is.
// RED: no latch (100 lines) or no isConnected check / no log (0 lines).
// ===========================================================================
#include <spdlog/sinks/ringbuffer_sink.h>
namespace {
struct EgressLogCapture {   // RAII; the egress is driven on this thread only
    std::shared_ptr<spdlog::sinks::ringbuffer_sink_mt> sink = std::make_shared<spdlog::sinks::ringbuffer_sink_mt>(1024);
    std::shared_ptr<spdlog::logger> previous = spdlog::default_logger();
    EgressLogCapture() {
        auto capture = std::make_shared<spdlog::logger>("egress-capture", sink);
        capture->set_level(spdlog::level::info);
        spdlog::set_default_logger(capture);
    }
    ~EgressLogCapture() { spdlog::set_default_logger(previous); }
    int count(const std::string& text) const {
        int n = 0;
        for (const std::string& l : sink->last_formatted()) n += l.find(text) != std::string::npos ? 1 : 0;
        return n;
    }
    EgressLogCapture(const EgressLogCapture&) = delete;
    EgressLogCapture& operator=(const EgressLogCapture&) = delete;
};
} // namespace

TEST_F(WorkerEgressFixture, ConnectedLineIsLoggedExactlyOnceRegression) {
    EgressLogCapture log;
    const std::string controlEp = freeEndpoint();
    WorkerEgress eg(aeron_, controlEp, 2096);
    ASSERT_TRUE(eg.resolved());
    for (int i = 0; i < 100; ++i) eg.noteConnected(7);   // no consumer yet
    EXPECT_FALSE(eg.connectedLogged());
    EXPECT_EQ(log.count("per-worker MDC egress connected"), 0);

    auto sub = resolveSub(std::string("aeron:udp?control=") + controlEp +
                          "|control-mode=dynamic|endpoint=" + freeEndpoint(), 2096);
    ASSERT_NE(sub, nullptr);
    ASSERT_TRUE(waitFor([&]() { return eg.isConnected(); }));
    for (int i = 0; i < 100; ++i) eg.noteConnected(7);
    EXPECT_TRUE(eg.connectedLogged());
    EXPECT_EQ(log.count("per-worker MDC egress connected"), 1);
    EXPECT_EQ(log.count("WorkerEgress[w7] per-worker MDC egress connected (control " + controlEp + ")"), 1);
}
