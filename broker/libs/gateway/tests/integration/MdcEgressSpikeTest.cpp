// MdcEgressSpikeTest.cpp — SPIKE (reference) that de-risked the per-worker MDC
// egress mechanic for the read-path rework (T3/T4), against the exact Aeron
// version this repo links.  CONCLUSION: use DYNAMIC MDC (control-mode=dynamic).
//
// Findings:
//   * DYNAMIC MDC works: an ExclusivePublication with control-mode=dynamic|
//     control=<host>:<port> fans out to MULTIPLE subscribers that self-register by
//     connecting to the control address.  The publisher never manages the
//     destination list — the driver auto-adds on connect and auto-removes on
//     liveness timeout.  Ideal for shared-nothing: the worker owns ONLY its pub
//     and offer()s; the control thread NEVER touches it; Subscribe/Unsubscribe
//     reach the worker via its ingress ring.  When one subscriber leaves, the
//     survivor keeps receiving and the pub does not wedge.
//   * MANUAL MDC removeDestination(channel) was BROKEN in this Aeron version:
//     the driver confirmed the remove (findDestinationResponse==true) yet kept
//     delivering to the removed endpoint.  Hence dynamic mode, not manual.
//
// This file documents the exact working channel syntax and guards the mechanic.
#include <rune/aeronutil/testing/FreePort.h>
#include <gtest/gtest.h>
#include <Aeron.h>
#include <ExclusivePublication.h>
#include <Subscription.h>
#include <FragmentAssembler.h>
#include <concurrent/AtomicBuffer.h>
#include <aeronmd/aeronmd.h>
#include <atomic>
#include <chrono>
#include <cstring>
#include <string>
#include <thread>
#include <filesystem>

using namespace std::chrono_literals;

namespace {

constexpr int32_t kEgressStream = 1001;
// Dynamic MDC: the publication advertises a CONTROL address; subscribers connect
// to it and the driver auto-adds them as destinations (and auto-removes on
// liveness timeout).  The publisher never manages the destination list — ideal
// for shared-nothing (the worker owns only the pub; no control-thread touch).
// D11: ports from the shared allocator (the fixed 24700/24611/24612 collided with
// allocator ports in concurrently running tests).
const std::string kControlEp = "127.0.0.1:" + std::to_string(rune::aeronutil::testing::allocFreeUdpPort());
const std::string kPubChannelS  = "aeron:udp?control-mode=dynamic|control=" + kControlEp + "|fc=min";
const std::string kSub1ChannelS = "aeron:udp?control=" + kControlEp + "|control-mode=dynamic|endpoint=127.0.0.1:" +
                                  std::to_string(rune::aeronutil::testing::allocFreeUdpPort());
const std::string kSub2ChannelS = "aeron:udp?control=" + kControlEp + "|control-mode=dynamic|endpoint=127.0.0.1:" +
                                  std::to_string(rune::aeronutil::testing::allocFreeUdpPort());
const char* const kPubChannel  = kPubChannelS.c_str();
const char* const kSub1Channel = kSub1ChannelS.c_str();
const char* const kSub2Channel = kSub2ChannelS.c_str();

class MdcSpikeFixture : public ::testing::Test {
protected:
    aeron_driver_context_t* driverCtx_ = nullptr;
    aeron_driver_t*         driver_    = nullptr;
    std::thread             driverThread_;
    std::atomic<bool>       driverRunning_{false};
    std::shared_ptr<aeron::Aeron> aeron_;

    void SetUp() override {
        startDriver();
        aeron::Context ctx;
        ctx.aeronDir(aeronDir_);
        aeron_ = aeron::Aeron::connect(ctx);
        ASSERT_NE(aeron_, nullptr);
    }
    void TearDown() override {
        aeron_.reset();
        stopDriver();
    }

    std::string aeronDir_;

    void startDriver() {
        aeronDir_ = (std::filesystem::temp_directory_path() /
                     ("rune_mdc_spike_" + std::to_string(::getpid()))).string();
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
            while (driverRunning_.load(std::memory_order_acquire)) {
                aeron_driver_main_idle_strategy(driver_, aeron_driver_main_do_work(driver_));
            }
        });
    }
    void stopDriver() {
        if (driverRunning_.exchange(false)) {
            if (driverThread_.joinable()) driverThread_.join();
        }
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

    std::shared_ptr<aeron::ExclusivePublication> resolvePub(const std::string& ch, int32_t sid) {
        int64_t reg = aeron_->addExclusivePublication(ch, sid);
        std::shared_ptr<aeron::ExclusivePublication> pub;
        waitFor([&]() { pub = aeron_->findExclusivePublication(reg); return pub != nullptr; });
        return pub;
    }
    std::shared_ptr<aeron::Subscription> resolveSub(const std::string& ch, int32_t sid) {
        int64_t reg = aeron_->addSubscription(ch, sid);
        std::shared_ptr<aeron::Subscription> sub;
        waitFor([&]() { sub = aeron_->findSubscription(reg); return sub != nullptr; });
        return sub;
    }
};

TEST_F(MdcSpikeFixture, DynamicMdcFanOutAndSurvivorUnaffected) {
    auto pub  = resolvePub(kPubChannel, kEgressStream);
    ASSERT_NE(pub, nullptr);
    auto sub1 = resolveSub(kSub1Channel, kEgressStream);
    auto sub2 = resolveSub(kSub2Channel, kEgressStream);
    ASSERT_NE(sub1, nullptr);
    ASSERT_NE(sub2, nullptr);

    // Dynamic MDC: subscribers self-register by connecting to the pub's control
    // address.  The owner thread does NOTHING to manage destinations.

    // Wait until the MDC publication sees both destinations connected.
    ASSERT_TRUE(waitFor([&]() { return pub->isConnected() &&
                                       sub1->isConnected() && sub2->isConnected(); }))
        << "MDC pub/subs never connected after addDestination";

    std::atomic<int> got1{0}, got2{0};
    aeron::FragmentAssembler fa1([&](aeron::concurrent::AtomicBuffer&, aeron::util::index_t,
                                     aeron::util::index_t, aeron::Header&) { got1++; });
    aeron::FragmentAssembler fa2([&](aeron::concurrent::AtomicBuffer&, aeron::util::index_t,
                                     aeron::util::index_t, aeron::Header&) { got2++; });

    // Same owner thread writes the publication (offer) — single-owner model.
    auto offerOnce = [&](uint8_t tag) {
        uint8_t payload[64];
        std::memset(payload, tag, sizeof(payload));
        aeron::concurrent::AtomicBuffer buf(payload, sizeof(payload));
        auto deadline = std::chrono::steady_clock::now() + 3s;
        while (std::chrono::steady_clock::now() < deadline) {
            if (pub->offer(buf, 0, sizeof(payload)) > 0) return true;
            std::this_thread::sleep_for(1ms);
        }
        return false;
    };

    // (1) Fan-out: both subscribers receive one message.
    ASSERT_TRUE(offerOnce(0xAB));
    ASSERT_TRUE(waitFor([&]() {
        sub1->poll(fa1.handler(), 10);
        sub2->poll(fa2.handler(), 10);
        return got1.load() >= 1 && got2.load() >= 1;
    })) << "fan-out failed: got1=" << got1 << " got2=" << got2;

    // (2) A subscriber leaving must NOT disrupt the survivor.  Destroy sub2
    // (client disconnect); the driver auto-removes its destination on liveness
    // timeout.  The owner thread keeps offering; sub1 must keep receiving and the
    // publication must not wedge on the departed destination.
    sub2.reset();
    const int base1 = got1.load();
    for (int i = 0; i < 5; ++i) ASSERT_TRUE(offerOnce(0xCD));
    ASSERT_TRUE(waitFor([&]() {
        sub1->poll(fa1.handler(), 10);
        return got1.load() > base1;
    })) << "sub1 stopped receiving after sub2 left (got1=" << got1 << ")";
    fprintf(stderr, "[spike] after sub2 left: got1=%d (base1=%d)\n", got1.load(), base1);
}

} // namespace
