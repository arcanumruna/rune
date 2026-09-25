// WorkerPushTest.cpp — read-rework T3/T4 I3a: in-worker read+encode+push loop.
//
// A Worker owns a real disk Log + its own dynamic-MDC WorkerEgress.  After a
// Subscribe op registers a cursor, Worker::doWork reads whole batches via
// Log::readView (zero-copy), encodes Records frames, and pushes them on its egress.
// A dynamic-MDC subscriber verifies in-order, exactly-once delivery, multi-record
// cursor advance, control-batch skip, subscribe-from-offset, and unsubscribe-stop.
//
// Uses an embedded UDP Aeron driver (same pattern as WorkerEgressTest).  The Log
// is written before driving doWork (single-threaded test), so readView observes
// committed data.  bound = LEO (no replica → read_uncommitted passthrough).
#include <rune/node/Worker.h>
#include <rune/node/WorkerOp.h>
#include <rune/storage/Log.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/protocol/client/DataMessages.h>

#include <Aeron.h>
#include <Subscription.h>
#include <FragmentAssembler.h>
#include <concurrent/AtomicBuffer.h>
#include <gtest/gtest.h>
#include <aeronmd/aeronmd.h>

#include <atomic>
#include <chrono>
#include <filesystem>
#include <string>
#include <thread>
#include <vector>
#include <unistd.h>

using namespace std::chrono_literals;
using namespace rune::node;
namespace pc = rune::protocol::client;

namespace {

constexpr std::size_t kRingBytes = 512 * 1024;

class WorkerPushFixture : public ::testing::Test {
protected:
    aeron_driver_context_t* driverCtx_ = nullptr;
    aeron_driver_t*         driver_    = nullptr;
    std::thread             driverThread_;
    std::atomic<bool>       driverRunning_{false};
    std::shared_ptr<aeron::Aeron> aeron_;
    std::string aeronDir_;

    void SetUp() override {
        startDriver();
        aeron::Context ctx; ctx.aeronDir(aeronDir_);
        aeron_ = aeron::Aeron::connect(ctx);
        ASSERT_NE(aeron_, nullptr);
    }
    void TearDown() override { aeron_.reset(); stopDriver(); }

    void startDriver() {
        aeronDir_ = (std::filesystem::temp_directory_path() /
                     ("rune_wpush_" + std::to_string(::getpid()))).string();
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
        ASSERT_EQ(aeron_driver_start(driver_, true), 0) << aeron_errmsg();
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
        while (std::chrono::steady_clock::now() < deadline) { if (p()) return true; std::this_thread::sleep_for(1ms); }
        return p();
    }
    std::shared_ptr<aeron::Subscription> resolveSub(const std::string& control,
                                                    const std::string& endpoint, int32_t sid) {
        std::string ch = "aeron:udp?control=" + control + "|control-mode=dynamic|endpoint=" + endpoint;
        int64_t reg = aeron_->addSubscription(ch, sid);
        std::shared_ptr<aeron::Subscription> sub;
        waitFor([&]() { sub = aeron_->findSubscription(reg); return sub != nullptr; });
        return sub;
    }
    std::filesystem::path tempLogDir() {
        static std::atomic<int> seq{0};
        auto d = std::filesystem::temp_directory_path() /
                 ("rune_wpush_log_" + std::to_string(::getpid()) + "_" +
                  std::to_string(seq.fetch_add(1)));
        std::filesystem::remove_all(d); std::filesystem::create_directories(d);
        return d;
    }
    // Append a batch of `nrec` records starting at baseOffset; returns nextOffset.
    static int64_t appendBatch(rune::storage::Log& log, int64_t baseOffset, int nrec,
                               bool control = false) {
        rune::protocol::RecordBatchBuilder b(baseOffset, /*ts=*/1000 + baseOffset);
        if (control) b.setControl(true);
        for (int i = 0; i < nrec; ++i)
            b.append("k", "v" + std::to_string(baseOffset + i), 1000 + baseOffset + i);
        return log.append(b.build(), baseOffset + nrec - 1);
    }

    // Collect decoded Records baseOffsets from a subscription into `out`.
    struct Collector {
        std::vector<int64_t> baseOffsets;
        aeron::FragmentAssembler fa;
        Collector()
            : fa([this](aeron::concurrent::AtomicBuffer& buf, aeron::util::index_t off,
                        aeron::util::index_t len, aeron::Header&) {
                  auto dec = pc::Records::decode(
                      reinterpret_cast<const uint8_t*>(buf.buffer() + off),
                      static_cast<std::size_t>(len));
                  if (dec) baseOffsets.push_back(dec->baseOffset);
              }) {}
    };

    WorkerOp subscribeOp(int64_t subId, int32_t topicId, int32_t partition,
                         int64_t nextOffset, rune::storage::Log* log) {
        WorkerOp op;
        op.type           = WorkerOp::Type::Subscribe;
        op.subscriptionId = subId;
        op.sessionId      = 4200 + subId;
        op.topicId        = topicId;
        op.partition      = partition;
        op.nextOffset     = nextOffset;
        op.logPtr         = log;
        op.replica        = nullptr;  // passthrough → bound = LEO
        return op;
    }
};

TEST_F(WorkerPushFixture, DeliversAllBatchesInOrderExactlyOnce) {
    auto dir = tempLogDir();
    rune::storage::Log log(dir.string(), 64LL * 1024 * 1024);
    constexpr int kN = 20;
    for (int i = 0; i < kN; ++i) appendBatch(log, i, 1);
    ASSERT_EQ(log.logEndOffset(), kN);

    const char* control = "127.0.0.1:24810";
    WorkerEgress eg(aeron_, control, 3001);
    ASSERT_TRUE(eg.resolved());
    auto sub = resolveSub(control, "127.0.0.1:24811", 3001);
    ASSERT_NE(sub, nullptr);
    ASSERT_TRUE(waitFor([&]() { return eg.isConnected() && sub->isConnected(); }));

    Worker w(0, kRingBytes, kRingBytes);
    w.setWorkerEgress(&eg);
    ASSERT_TRUE(w.tryEnqueueOp(subscribeOp(1, 10, 0, 0, &log)));

    Collector c;
    ASSERT_TRUE(waitFor([&]() {
        w.doWork(1);
        sub->poll(c.fa.handler(), 64);
        return static_cast<int>(c.baseOffsets.size()) >= kN;
    }, 10s)) << "received " << c.baseOffsets.size() << "/" << kN;

    ASSERT_EQ(static_cast<int>(c.baseOffsets.size()), kN);
    for (int i = 0; i < kN; ++i)
        EXPECT_EQ(c.baseOffsets[i], i) << "out-of-order or gap at index " << i;
    std::filesystem::remove_all(dir);
}

TEST_F(WorkerPushFixture, MultiRecordBatchesAdvanceCursorByRecordCount) {
    auto dir = tempLogDir();
    rune::storage::Log log(dir.string(), 64LL * 1024 * 1024);
    appendBatch(log, 0, 5);   // offsets 0..4
    appendBatch(log, 5, 3);   // offsets 5..7
    appendBatch(log, 8, 2);   // offsets 8..9
    ASSERT_EQ(log.logEndOffset(), 10);

    const char* control = "127.0.0.1:24820";
    WorkerEgress eg(aeron_, control, 3002);
    ASSERT_TRUE(eg.resolved());
    auto sub = resolveSub(control, "127.0.0.1:24821", 3002);
    ASSERT_TRUE(waitFor([&]() { return eg.isConnected() && sub->isConnected(); }));

    Worker w(0, kRingBytes, kRingBytes);
    w.setWorkerEgress(&eg);
    ASSERT_TRUE(w.tryEnqueueOp(subscribeOp(1, 10, 0, 0, &log)));

    Collector c;
    ASSERT_TRUE(waitFor([&]() {
        w.doWork(1); sub->poll(c.fa.handler(), 64);
        return c.baseOffsets.size() >= 3;
    }, 10s)) << "got " << c.baseOffsets.size();
    ASSERT_EQ(c.baseOffsets.size(), 3u);
    EXPECT_EQ(c.baseOffsets[0], 0);
    EXPECT_EQ(c.baseOffsets[1], 5);
    EXPECT_EQ(c.baseOffsets[2], 8);
    std::filesystem::remove_all(dir);
}

TEST_F(WorkerPushFixture, ControlBatchesAreSkipped) {
    auto dir = tempLogDir();
    rune::storage::Log log(dir.string(), 64LL * 1024 * 1024);
    appendBatch(log, 0, 1);                 // data @0
    appendBatch(log, 1, 1, /*control=*/true); // control @1 (must NOT be delivered)
    appendBatch(log, 2, 1);                 // data @2
    ASSERT_EQ(log.logEndOffset(), 3);

    const char* control = "127.0.0.1:24830";
    WorkerEgress eg(aeron_, control, 3003);
    ASSERT_TRUE(eg.resolved());
    auto sub = resolveSub(control, "127.0.0.1:24831", 3003);
    ASSERT_TRUE(waitFor([&]() { return eg.isConnected() && sub->isConnected(); }));

    Worker w(0, kRingBytes, kRingBytes);
    w.setWorkerEgress(&eg);
    ASSERT_TRUE(w.tryEnqueueOp(subscribeOp(1, 10, 0, 0, &log)));

    Collector c;
    ASSERT_TRUE(waitFor([&]() {
        w.doWork(1); sub->poll(c.fa.handler(), 64);
        return c.baseOffsets.size() >= 2;
    }, 10s)) << "got " << c.baseOffsets.size();
    // Exactly the two data batches, control skipped.  Give a moment for any
    // (wrongly) delivered control frame to arrive too.
    for (int i = 0; i < 50; ++i) { w.doWork(1); sub->poll(c.fa.handler(), 64); std::this_thread::sleep_for(1ms); }
    ASSERT_EQ(c.baseOffsets.size(), 2u) << "control batch must not be delivered";
    EXPECT_EQ(c.baseOffsets[0], 0);
    EXPECT_EQ(c.baseOffsets[1], 2);
    std::filesystem::remove_all(dir);
}

TEST_F(WorkerPushFixture, SubscribeFromNonZeroOffset) {
    auto dir = tempLogDir();
    rune::storage::Log log(dir.string(), 64LL * 1024 * 1024);
    for (int i = 0; i < 10; ++i) appendBatch(log, i, 1);

    const char* control = "127.0.0.1:24840";
    WorkerEgress eg(aeron_, control, 3004);
    ASSERT_TRUE(eg.resolved());
    auto sub = resolveSub(control, "127.0.0.1:24841", 3004);
    ASSERT_TRUE(waitFor([&]() { return eg.isConnected() && sub->isConnected(); }));

    Worker w(0, kRingBytes, kRingBytes);
    w.setWorkerEgress(&eg);
    ASSERT_TRUE(w.tryEnqueueOp(subscribeOp(1, 10, 0, /*nextOffset=*/5, &log)));

    Collector c;
    ASSERT_TRUE(waitFor([&]() {
        w.doWork(1); sub->poll(c.fa.handler(), 64);
        return c.baseOffsets.size() >= 5;
    }, 10s)) << "got " << c.baseOffsets.size();
    ASSERT_EQ(c.baseOffsets.size(), 5u);
    for (int i = 0; i < 5; ++i) EXPECT_EQ(c.baseOffsets[i], 5 + i);
    std::filesystem::remove_all(dir);
}

TEST_F(WorkerPushFixture, UnsubscribeStopsFurtherDelivery) {
    auto dir = tempLogDir();
    rune::storage::Log log(dir.string(), 64LL * 1024 * 1024);
    for (int i = 0; i < 5; ++i) appendBatch(log, i, 1);

    const char* control = "127.0.0.1:24850";
    WorkerEgress eg(aeron_, control, 3005);
    ASSERT_TRUE(eg.resolved());
    auto sub = resolveSub(control, "127.0.0.1:24851", 3005);
    ASSERT_TRUE(waitFor([&]() { return eg.isConnected() && sub->isConnected(); }));

    Worker w(0, kRingBytes, kRingBytes);
    w.setWorkerEgress(&eg);
    ASSERT_TRUE(w.tryEnqueueOp(subscribeOp(1, 10, 0, 0, &log)));

    Collector c;
    ASSERT_TRUE(waitFor([&]() {
        w.doWork(1); sub->poll(c.fa.handler(), 64);
        return c.baseOffsets.size() >= 5;
    }, 10s));
    ASSERT_EQ(c.baseOffsets.size(), 5u);

    // Unsubscribe, then produce 5 more — none must be delivered.
    WorkerOp un; un.type = WorkerOp::Type::Unsubscribe; un.subscriptionId = 1;
    ASSERT_TRUE(w.tryEnqueueOp(std::move(un)));
    w.doWork(1);
    for (int i = 5; i < 10; ++i) appendBatch(log, i, 1);
    for (int i = 0; i < 100; ++i) { w.doWork(1); sub->poll(c.fa.handler(), 64); std::this_thread::sleep_for(1ms); }
    EXPECT_EQ(c.baseOffsets.size(), 5u) << "no delivery after unsubscribe";
    std::filesystem::remove_all(dir);
}

TEST_F(WorkerPushFixture, NoEgressWiredIsInertNoCrash) {
    auto dir = tempLogDir();
    rune::storage::Log log(dir.string(), 64LL * 1024 * 1024);
    for (int i = 0; i < 5; ++i) appendBatch(log, i, 1);

    Worker w(0, kRingBytes, kRingBytes);
    // No setWorkerEgress → pushOwnedSubscriptions_ must be a no-op.
    ASSERT_TRUE(w.tryEnqueueOp(subscribeOp(1, 10, 0, 0, &log)));
    for (int i = 0; i < 10; ++i) w.doWork(1);
    // Sub registered but nothing pushed; cursor untouched.
    ASSERT_EQ(w.subscriptionsForTest().size(), 1u);
    EXPECT_EQ(w.subscriptionsForTest()[0].nextOffset, 0);
    std::filesystem::remove_all(dir);
}

} // namespace
