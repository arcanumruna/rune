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
#include <rune/node/EmbeddedDriver.h>
#include <rune/node/Worker.h>
#include <rune/node/WorkerOp.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/transaction/ControlMarker.h>
#include <rune/storage/Log.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/protocol/client/DataMessages.h>
#include <rune/protocol/client/ControlMessages.h>
#include <rune/protocol/client/Protocol.h>

#include <Aeron.h>
#include <Subscription.h>
#include <FragmentAssembler.h>
#include <concurrent/AtomicBuffer.h>
#include <gtest/gtest.h>
#include <aeronmd/aeronmd.h>

#include <atomic>
#include <chrono>
#include <filesystem>
#include <map>
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
    std::unique_ptr<rune::node::EmbeddedDriver> driver_;
    std::shared_ptr<aeron::Aeron> aeron_;
    std::string aeronDir_;

    // Driver term length (UDP).  maxMessageLength = term / 8.
    virtual int32_t termBufferBytes() const { return 64 * 1024; }

    void SetUp() override {
        startDriver();
        if (HasFatalFailure()) return;  // driver failed to start: don't block in connect
        aeron::Context ctx; ctx.aeronDir(aeronDir_);
        aeron_ = aeron::Aeron::connect(ctx);
        ASSERT_NE(aeron_, nullptr);
    }
    void TearDown() override { aeron_.reset(); stopDriver(); }

    void startDriver() {
        aeronDir_ = (std::filesystem::temp_directory_path() /
                     ("rune_wpush_" + std::to_string(::getpid()))).string();
        std::filesystem::remove_all(aeronDir_);
        // One dedicated pumping thread (EmbeddedDriver::Threaded); no caller-side
        // readiness pumping needed — aeron_driver_init already wrote cnc.dat and
        // stamped the heartbeat synchronously before the driver thread starts.
        rune::node::EmbeddedDriverConfig cfg;
        cfg.dir             = aeronDir_;
        cfg.termBufferBytes = termBufferBytes();
        // Pinned (test driver, orthogonal to the broker defaults): the
        // pre-2026-10-02 128 KiB receive window.  The back-pressure tests' depth is
        // publisher limit = sender position + term/2, the sender bounded by this
        // window — a host-derived 4 MiB window (Linux, 16 MiB sysctls) let all 12
        // x 1 MiB offers through and voided the precondition.
        cfg.rcvWindowBytes  = 128 * 1024;
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


// ===========================================================================
// F19 (REGRESSION): per-worker egress (shard mode) skipped any batch whose
// Records frame exceeded the 68 KiB egressScratch_ and advanced the cursor —
// silent consumer data loss — and a frame between maxPayload and the scratch
// but above the publication's maxMessageLength threw out of doWork.
//
// Fixed: deliverable frames go out as a vectored offer (header from scratch +
// batch bytes zero-copy from the log view); a frame above maxMessageLength
// answers ONE ErrorResponse{MessageTooLarge, correlationId = 0, message naming
// the subscription/topic/partition/offset}
// and parks that subscription (cursor not advanced; others unaffected);
// back-pressure keeps the retry-from-same-cursor semantics.
// ===========================================================================
struct F19Egress {
    struct Rec { int64_t subId; int64_t baseOffset; std::vector<uint8_t> bytes; };
    struct Err { int64_t sessionId; int64_t correlationId; int16_t code; std::string message; };
    std::vector<Rec> records;
    std::vector<Err> errors;
    aeron::FragmentAssembler fa;
    F19Egress()
        : fa([this](aeron::concurrent::AtomicBuffer& buf, aeron::util::index_t off,
                    aeron::util::index_t len, aeron::Header&) {
              const auto* p = reinterpret_cast<const uint8_t*>(buf.buffer() + off);
              const auto  n = static_cast<std::size_t>(len);
              if (auto r = pc::Records::decode(p, n))
                  records.push_back({r->subscriptionId, r->baseOffset,
                                     {r->recordBatch.begin(), r->recordBatch.end()}});
              else if (auto e = pc::ErrorResponse::decode(p, n))
                  errors.push_back({e->sessionId, e->correlationId, e->code, std::string(e->message)});
          }) {}
    [[nodiscard]] std::size_t recordsFor(int64_t subId) const {
        std::size_t c = 0;
        for (const auto& r : records) c += (r.subId == subId);
        return c;
    }
};

// Append one batch whose single record value is `valueBytes` long at the log's
// LEO; returns the exact stored bytes.  Built once per size and restamped
// (baseOffset, bytes 0..7, is not CRC-covered); appended without fsync (the
// page cache is visible to readers) to keep the 1 MiB cases fast.
static std::vector<uint8_t> f19AppendSized(rune::storage::Log& log, std::size_t valueBytes) {
    static std::map<std::size_t, std::vector<uint8_t>> built;
    const int64_t base = log.logEndOffset();
    auto& tmpl = built[valueBytes];
    if (tmpl.empty()) {
        std::string value(valueBytes, '\0');
        for (std::size_t i = 0; i < valueBytes; ++i)
            value[i] = static_cast<char>((i * 31u) & 0xffu);
        rune::protocol::RecordBatchBuilder b(0, 1000);
        b.append("k", value, 1000);
        tmpl = b.build();
    }
    std::vector<uint8_t> bytes = tmpl;
    for (int i = 0; i < 8; ++i) bytes[i] = static_cast<uint8_t>(static_cast<uint64_t>(base) >> (8 * i));
    log.appendNoSync(bytes.data(), bytes.size(), base);   // page cache: visible to readers
    return bytes;
}

// 16 MiB UDP term → maxMessageLength 2 MiB: a 1 MiB batch is deliverable.
class WorkerLargeBatchPushRegression : public WorkerPushFixture {
protected:
    int32_t termBufferBytes() const override { return 16 * 1024 * 1024; }
};

TEST_F(WorkerLargeBatchPushRegression, OneMiBBatchDeliveredIntactThenSmallBatch) {
    auto dir = tempLogDir();
    rune::storage::Log log(dir.string(), 64LL * 1024 * 1024);
    const auto big   = f19AppendSized(log, 1024 * 1024);   // offset 0
    const auto small = f19AppendSized(log, 16);            // offset 1

    const char* control = "127.0.0.1:24910";
    WorkerEgress eg(aeron_, control, 3101);
    ASSERT_TRUE(eg.resolved());
    auto sub = resolveSub(control, "127.0.0.1:24911", 3101);
    ASSERT_NE(sub, nullptr);
    ASSERT_TRUE(waitFor([&]() { return eg.isConnected() && sub->isConnected(); }));

    Worker w(0, kRingBytes, kRingBytes);
    w.setWorkerEgress(&eg);
    ASSERT_TRUE(w.tryEnqueueOp(subscribeOp(1, 10, 0, 0, &log)));

    F19Egress c;
    ASSERT_TRUE(waitFor([&]() {
        w.doWork(1); sub->poll(c.fa.handler(), 64);
        return c.recordsFor(1) >= 2 || !c.errors.empty();
    }, 10s)) << "received " << c.records.size() << "/2 (1 MiB batch skipped = F19)";
    ASSERT_TRUE(c.errors.empty());
    ASSERT_EQ(c.recordsFor(1), 2u);
    EXPECT_EQ(c.records[0].baseOffset, 0);
    EXPECT_EQ(c.records[0].bytes, big) << "1 MiB batch not byte-identical after reassembly";
    EXPECT_EQ(c.records[1].baseOffset, 1);
    EXPECT_EQ(c.records[1].bytes, small);
    EXPECT_EQ(w.subscriptionsForTest()[0].nextOffset, 2);
    std::filesystem::remove_all(dir);
}

TEST_F(WorkerLargeBatchPushRegression, BackPressuredLargeOfferRetriesWithoutLoss) {
    auto dir = tempLogDir();
    rune::storage::Log log(dir.string(), 256LL * 1024 * 1024);
    constexpr int kBatches = 12;
    std::vector<std::vector<uint8_t>> stored;
    for (int i = 0; i < kBatches; ++i) stored.push_back(f19AppendSized(log, 1024 * 1024));

    const char* control = "127.0.0.1:24920";
    WorkerEgress eg(aeron_, control, 3102);
    ASSERT_TRUE(eg.resolved());
    auto sub = resolveSub(control, "127.0.0.1:24921", 3102);
    ASSERT_NE(sub, nullptr);
    ASSERT_TRUE(waitFor([&]() { return eg.isConnected() && sub->isConnected(); }));

    Worker w(0, kRingBytes, kRingBytes);
    w.setWorkerEgress(&eg);
    ASSERT_TRUE(w.tryEnqueueOp(subscribeOp(1, 10, 0, 0, &log)));

    // Drive the worker WITHOUT polling the subscriber: flow control closes the
    // window and the 1 MiB offers back-pressure.
    for (int i = 0; i < 4 * kBatches; ++i) w.doWork(1);
    EXPECT_LT(w.subscriptionsForTest()[0].nextOffset, kBatches)
        << "back-pressure never happened (test precondition)";

    F19Egress c;
    ASSERT_TRUE(waitFor([&]() {
        w.doWork(1); sub->poll(c.fa.handler(), 64);
        return c.recordsFor(1) >= static_cast<std::size_t>(kBatches) || !c.errors.empty();
    }, 20s)) << "received " << c.records.size() << "/" << kBatches;
    ASSERT_TRUE(c.errors.empty());
    ASSERT_EQ(c.recordsFor(1), static_cast<std::size_t>(kBatches));
    for (int i = 0; i < kBatches; ++i) {
        EXPECT_EQ(c.records[i].baseOffset, i) << "gap/dup/reorder at " << i;
        EXPECT_EQ(c.records[i].bytes, stored[i]) << "batch " << i << " corrupted";
    }
    EXPECT_EQ(w.subscriptionsForTest()[0].nextOffset, kBatches);
    std::filesystem::remove_all(dir);
}

// 64 KiB UDP term → maxMessageLength 8 KiB: a 16 KiB batch cannot be delivered.
class WorkerOversizePushRegression : public WorkerPushFixture {};

TEST_F(WorkerOversizePushRegression, AboveMaxMessageLengthAnswersErrorNoSilentAdvance) {
    auto dirA = tempLogDir();
    auto dirB = tempLogDir();
    rune::storage::Log logA(dirA.string(), 64LL * 1024 * 1024);
    rune::storage::Log logB(dirB.string(), 64LL * 1024 * 1024);
    f19AppendSized(logA, 16 * 1024);   // A@0: frame > 8 KiB max message
    f19AppendSized(logA, 16);          // A@1: behind the undeliverable batch
    f19AppendSized(logB, 16);          // B: another subscription, must flow
    f19AppendSized(logB, 16);

    const char* control = "127.0.0.1:24930";
    WorkerEgress eg(aeron_, control, 3103);
    ASSERT_TRUE(eg.resolved());
    auto sub = resolveSub(control, "127.0.0.1:24931", 3103);
    ASSERT_NE(sub, nullptr);
    ASSERT_TRUE(waitFor([&]() { return eg.isConnected() && sub->isConnected(); }));

    Worker w(0, kRingBytes, kRingBytes);
    w.setWorkerEgress(&eg);
    ASSERT_TRUE(w.tryEnqueueOp(subscribeOp(1, 10, 0, 0, &logA)));
    ASSERT_TRUE(w.tryEnqueueOp(subscribeOp(2, 10, 1, 0, &logB)));

    F19Egress c;
    ASSERT_TRUE(waitFor([&]() {
        w.doWork(1); sub->poll(c.fa.handler(), 64);
        return !c.errors.empty() && c.recordsFor(2) >= 2;
    }, 10s)) << "errors=" << c.errors.size() << " sub2 records=" << c.recordsFor(2);
    ASSERT_EQ(c.errors.size(), 1u);
    EXPECT_EQ(c.errors[0].correlationId, 0)
        << "a subscription id must never be sent as a correlationId (collides with ping ids)";
    EXPECT_NE(c.errors[0].message.find("subscription 1 topic 10 partition 0 offset 0"), std::string::npos)
        << "message must name the subscription; got: " << c.errors[0].message;
    EXPECT_EQ(c.errors[0].sessionId, 4201);
    EXPECT_EQ(c.errors[0].code, static_cast<int16_t>(pc::ErrorCode::MessageTooLarge));

    // Parked: no delivery past the failed batch, no second error, cursor stays.
    for (int i = 0; i < 200; ++i) { w.doWork(1); sub->poll(c.fa.handler(), 64); }
    EXPECT_EQ(c.recordsFor(1), 0u);
    EXPECT_EQ(c.errors.size(), 1u) << "exactly one error, no spin";
    for (const auto& s : w.subscriptionsForTest()) {
        if (s.subscriptionId == 1) EXPECT_EQ(s.nextOffset, 0) << "silent advance past undelivered batch";
        if (s.subscriptionId == 2) EXPECT_EQ(s.nextOffset, 2);
    }
    std::filesystem::remove_all(dirA);
    std::filesystem::remove_all(dirB);
}

// ===========================================================================
// F15 S4 (design note §2.4, K1): the worker-push egress arm serves
// read_committed from the OWNING worker's RC state — bound = LSO(live HW),
// aborted batches skipped, a pre-carrier group sub CUT at the first aborted
// batch (addendum R-2).  The cursor (worker-owned) is the deterministic
// observation; delivery is checked once the data is releasable.
// ===========================================================================
class NullTransportRc final : public rune::replication::PartitionTransport {
public:
    bool sendFetchRequest(const rune::replication::FetchRequestMsg&) override { return true; }
    bool sendFetchResponse(rune::replication::NodeId, const rune::replication::FetchResponseMsg&) override { return true; }
};
class TxnRcRegression : public WorkerPushFixture {
protected:
    static rune::replication::PartitionConfig leaderCfg() {
        rune::replication::PartitionConfig c;
        c.topicId = 10; c.partitionId = 0; c.selfId = 1; c.minIsr = 1; c.durabilityTier = 1;
        c.fetchIntervalNs = 1'000'000'000LL; c.isrCheckIntervalNs = 1'000'000'000LL; c.pushOnAppend = false;
        c.replicaLagMaxMs = 1'000'000;
        return c;
    }
    static void appendTxn(rune::storage::Log& log, int64_t base, int64_t pid) {
        rune::protocol::RecordBatchBuilder b(base, 1000 + base);
        b.setTransactional(true); b.setProducerId(pid); b.setProducerEpoch(0); b.setBaseSequence(0);
        b.append("k", "txn", 1000 + base);
        (void)log.append(b.build(), base);
    }
    static WorkerOp markerOp(rune::replication::PartitionReplica* r, int64_t pid, bool commit) {
        WorkerOp op;
        op.type = WorkerOp::Type::AppendControlRecord; op.topicId = r->topicId(); op.partition = 0;
        op.replica = r; op.logPtr = r->log(); op.correlationId = pid;
        op.recordBatchBytes = rune::transaction::ControlMarker::build(
            pid, 0, commit ? rune::transaction::ControlType::Commit : rune::transaction::ControlType::Abort, 0);
        return op;
    }
    WorkerOp rcSubscribeOp(int64_t subId, rune::replication::PartitionReplica* r, uint8_t iso,
                           bool isGroup = false, bool carrier = true) {
        WorkerOp op = subscribeOp(subId, r->topicId(), 0, 0, r->log());
        op.replica = r; op.isolation = iso; op.isGroup = isGroup; op.groupCarrierReceived = carrier;
        return op;
    }
};

TEST_F(TxnRcRegression, WorkerPushReadCommittedHoldsOpenTxn) {
    auto dir = tempLogDir();
    rune::storage::Log log(dir.string(), 64LL * 1024 * 1024);
    appendBatch(log, 0, 1);                       // data @0
    appendTxn(log, 1, /*pid=*/7);                 // open txn @1
    appendBatch(log, 2, 1);                       // data @2 (behind the LSO)
    NullTransportRc t;
    rune::replication::PartitionReplica rep(leaderCfg(), rune::replication::Role::Leader, 1, {1}, {1}, &log, &t);
    rep.onLeaderLogAppended();
    ASSERT_EQ(rep.highWatermark(), 3);

    const char* control = "127.0.0.1:24860";
    WorkerEgress eg(aeron_, control, 3010);
    ASSERT_TRUE(eg.resolved());
    auto sub = resolveSub(control, "127.0.0.1:24861", 3010);
    ASSERT_TRUE(waitFor([&]() { return eg.isConnected() && sub->isConnected(); }));

    Worker w(0, kRingBytes, kRingBytes);
    w.setWorkerEgress(&eg);
    w.setOwnedDirect(Worker::OwnedSet{&rep});
    ASSERT_TRUE(w.tryEnqueueOp(rcSubscribeOp(1, &rep, /*read_committed*/1)));
    Collector c;
    ASSERT_TRUE(waitFor([&]() { w.doWork(1); sub->poll(c.fa.handler(), 64); return !c.baseOffsets.empty(); }, 10s));
    for (int i = 0; i < 64; ++i) w.doWork(1);
    ASSERT_EQ(w.subscriptionsForTest().size(), 1u);
    EXPECT_EQ(w.subscriptionsForTest()[0].nextOffset, 1)
        << "the worker push went past the open txn's first offset (HW-bounded, not LSO-bounded)";
    // COMMIT → the LSO advances to the HW → the held batches are released, in order.
    ASSERT_TRUE(w.tryEnqueueOp(markerOp(&rep, 7, /*commit=*/true)));
    ASSERT_TRUE(waitFor([&]() { w.doWork(1); sub->poll(c.fa.handler(), 64); return c.baseOffsets.size() >= 3; }, 10s))
        << "got " << c.baseOffsets.size();
    EXPECT_EQ(c.baseOffsets, (std::vector<int64_t>{0, 1, 2}));
    std::filesystem::remove_all(dir);
}

TEST_F(TxnRcRegression, WorkerPushFiltersAbortedBatch) {
    auto dir = tempLogDir();
    rune::storage::Log log(dir.string(), 64LL * 1024 * 1024);
    appendTxn(log, 0, /*pid=*/7);                 // txn @0, aborted below
    NullTransportRc t;
    rune::replication::PartitionReplica rep(leaderCfg(), rune::replication::Role::Leader, 1, {1}, {1}, &log, &t);
    rep.onLeaderLogAppended();

    const char* control = "127.0.0.1:24870";
    WorkerEgress eg(aeron_, control, 3011);
    ASSERT_TRUE(eg.resolved());
    auto sub = resolveSub(control, "127.0.0.1:24871", 3011);
    ASSERT_TRUE(waitFor([&]() { return eg.isConnected() && sub->isConnected(); }));

    Worker w(0, kRingBytes, kRingBytes);
    w.setWorkerEgress(&eg);
    w.setOwnedDirect(Worker::OwnedSet{&rep});
    ASSERT_TRUE(w.tryEnqueueOp(markerOp(&rep, 7, /*commit=*/false)));   // ABORT @1
    w.doWork(1);
    ASSERT_EQ(log.logEndOffset(), 2);
    appendBatch(log, 2, 1);                       // committed data @2
    rep.onLeaderLogAppended();
    ASSERT_TRUE(w.tryEnqueueOp(rcSubscribeOp(1, &rep, /*read_committed*/1)));
    Collector c;
    ASSERT_TRUE(waitFor([&]() { w.doWork(1); sub->poll(c.fa.handler(), 64);
                                return !c.baseOffsets.empty() && c.baseOffsets.back() == 2; }, 10s));
    EXPECT_EQ(c.baseOffsets, (std::vector<int64_t>{2})) << "read_committed worker push delivered the aborted batch";
    EXPECT_EQ(w.subscriptionsForTest()[0].nextOffset, 3);
    std::filesystem::remove_all(dir);
}

TEST_F(TxnRcRegression, WorkerPushPreCarrierGroupCutsAtAbortedBatch) {
    // R-2 on the worker-push arm: pre-carrier (isolation guessed read_committed)
    // the cursor stops AT the aborted batch; a read_uncommitted carrier then
    // delivers it (never stripped before the true isolation is known).
    auto dir = tempLogDir();
    rune::storage::Log log(dir.string(), 64LL * 1024 * 1024);
    appendTxn(log, 0, /*pid=*/7);
    NullTransportRc t;
    rune::replication::PartitionReplica rep(leaderCfg(), rune::replication::Role::Leader, 1, {1}, {1}, &log, &t);
    rep.onLeaderLogAppended();

    const char* control = "127.0.0.1:24880";
    WorkerEgress eg(aeron_, control, 3012);
    ASSERT_TRUE(eg.resolved());
    auto sub = resolveSub(control, "127.0.0.1:24881", 3012);
    ASSERT_TRUE(waitFor([&]() { return eg.isConnected() && sub->isConnected(); }));

    Worker w(0, kRingBytes, kRingBytes);
    w.setWorkerEgress(&eg);
    w.setOwnedDirect(Worker::OwnedSet{&rep});
    ASSERT_TRUE(w.tryEnqueueOp(markerOp(&rep, 7, /*commit=*/false)));   // ABORT @1
    w.doWork(1);
    appendBatch(log, 2, 1);
    rep.onLeaderLogAppended();
    ASSERT_TRUE(w.tryEnqueueOp(rcSubscribeOp(1, &rep, 1, /*isGroup=*/true, /*carrier=*/false)));
    for (int i = 0; i < 64; ++i) w.doWork(1);
    EXPECT_EQ(w.subscriptionsForTest()[0].nextOffset, 0) << "pre-carrier: the cursor passed an aborted batch";
    ASSERT_TRUE(w.tryEnqueueOp(rcSubscribeOp(1, &rep, /*read_uncommitted carrier*/0, true, true)));
    Collector c;
    ASSERT_TRUE(waitFor([&]() { w.doWork(1); sub->poll(c.fa.handler(), 64);
                                return !c.baseOffsets.empty() && c.baseOffsets.back() == 2; }, 10s));
    EXPECT_EQ(c.baseOffsets, (std::vector<int64_t>{0, 2})) << "the aborted batch was lost before the carrier";
    std::filesystem::remove_all(dir);
}


} // namespace
