// WorkerEgressTest.cpp — per-worker dynamic-MDC egress publication (read-rework
// T3/T4, increment I1).  Verifies a WorkerEgress resolves its own dynamic-MDC
// ExclusivePublication and that a self-registering subscriber receives frames
// sent by the (single) owner thread — plus fan-out to two subscribers.
//
// Uses an embedded UDP-capable Aeron media driver (same pattern validated by the
// MDC egress spike).
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
constexpr const char* kControl = "127.0.0.1:24720";

class WorkerEgressFixture : public ::testing::Test {
protected:
    aeron_driver_context_t* driverCtx_ = nullptr;
    aeron_driver_t*         driver_    = nullptr;
    std::thread             driverThread_;
    std::atomic<bool>       driverRunning_{false};
    std::shared_ptr<aeron::Aeron> aeron_;
    std::string aeronDir_;

    void SetUp() override {
        startDriver();
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
        aeron_driver_context_init(&driverCtx_);
        aeron_driver_context_set_dir(driverCtx_, aeronDir_.c_str());
        aeron_driver_context_set_threading_mode(driverCtx_, AERON_THREADING_MODE_SHARED);
        aeron_driver_context_set_dir_delete_on_start(driverCtx_, true);
        aeron_driver_context_set_dir_delete_on_shutdown(driverCtx_, true);
        aeron_driver_context_set_shared_idle_strategy(driverCtx_, "sleep-ns");
        aeron_driver_context_set_term_buffer_sparse_file(driverCtx_, true);
        aeron_driver_context_set_term_buffer_length(driverCtx_, 64 * 1024);
        ASSERT_EQ(aeron_driver_init(&driver_, driverCtx_), 0) << aeron_errmsg();
        ASSERT_EQ(aeron_driver_start(driver_, /*manual_main_loop=*/true), 0) << aeron_errmsg();
        driverRunning_.store(true, std::memory_order_release);
        driverThread_ = std::thread([this]() {
            while (driverRunning_.load(std::memory_order_acquire))
                aeron_driver_main_idle_strategy(driver_, aeron_driver_main_do_work(driver_));
        });
    }
    void stopDriver() {
        if (driverRunning_.exchange(false) && driverThread_.joinable()) driverThread_.join();
        if (driver_) { aeron_driver_close(driver_); driver_ = nullptr; }
        if (driverCtx_) { aeron_driver_context_close(driverCtx_); driverCtx_ = nullptr; }
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

    auto sub1 = resolveSub(subChannel("127.0.0.1:24721"), kStream);
    auto sub2 = resolveSub(subChannel("127.0.0.1:24722"), kStream);
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

} // namespace
