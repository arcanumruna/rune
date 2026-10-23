// tests/producer/ProducerClaimTest.cpp — Phase 2b Part B: zero-copy produce
// (Producer::tryClaim / claimBatch) on a bare ProducerImpl, Thread (Ring) and
// Invoker (Direct) ingress: commit / abort / destructor-abort, back-pressure,
// limits, ordering vs sends and Tier-2 batches, the copy-count proofs, and the
// zero-allocation gates.  Every drive loop is iteration-capped; nothing sleeps.
#include <filesystem>
#include "../support/AckLedger.h"
#include "../support/AllocCounter.h"
#include "../support/FrameCapture.h"

#include <rune/client/Client.h>
#include <rune/client/Completions.h>
#include <rune/client/Key.h>
#include <rune/client/TopicHandle.h>
#include <rune/client/producer/BatchBuilder.h>
#include <rune/client/producer/Producer.h>
#include <rune/client/codec/DataCodec.h>
#include <rune/client/producer/ProducerImpl.h>

#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <memory>
#include <vector>
#include <unistd.h>

namespace rc = rune::client;
using rc::Acks;
using rc::Key;
using rc::Producer;
using rc::ProducerConfig;
using rc::SendResult;
using rc::TopicHandle;
using rc::producer::ProducerImpl;
using test_support::FrameCapture;

namespace {
constexpr int64_t kNow = 1'000'000'000;
const TopicHandle kTopic{1, 4};

ProducerConfig voidCfg() {
    ProducerConfig c;
    c.acks        = Acks::None;
    c.lingerUs    = 0;
    c.batchBytes  = 16 * 1024;
    c.maxInFlight = 8;
    return c;
}

std::vector<uint8_t> bytes(std::string_view s) { return {s.begin(), s.end()}; }

void fill(std::span<uint8_t> b, std::string_view s) {
    std::memcpy(b.data(), s.data(), std::min(b.size(), s.size()));
}

// Ring mode: drain the producer ring and flush, a fixed number of passes (a
// pass may stop at a batch boundary; 16 always reach the end in these tests).
void pump(ProducerImpl& impl, const ProducerImpl::SendFn& sink) {
    for (int i = 0; i < 16; ++i) {
        impl.drainRing(kNow, 256);
        impl.flushBatches(kNow, 42, sink);
    }
}

// A Client in Invoker mode is used only to REGISTER Completions queues: the
// producer under test is a bare Ring-mode ProducerImpl whose hooks name that
// Client, so tryClaim(.., Completions&, Token) accepts the queues.
int g_ctr = 0;  // single test thread
rc::ClientConfig regCfg() {
    rc::ClientConfig c;
    c.gatewayChannel  = "aeron:ipc";
    c.gatewayStreamId = 4601;
    c.egressChannel   = "aeron:ipc";
    c.egressStreamId  = 4602;
    c.dataDir = std::filesystem::temp_directory_path().string() + "/claim_test_" + std::to_string(::getpid()) + "_" + std::to_string(g_ctr++);
    c.runnerMode = rc::RunnerMode::Invoker;
    return c;
}
rc::CompletionsConfig smallCq(uint32_t cap = 1u << 14) {
    rc::CompletionsConfig c;
    c.capacityBytes  = cap;
    c.maxResultBytes = 64;
    c.maxErrorText   = 64;
    return c;
}
struct CqRig {
    std::unique_ptr<rc::Client> client = std::make_unique<rc::Client>(regCfg());
    ProducerImpl*               impl;
    Producer                    p;
    explicit CqRig(ProducerConfig cfg = voidCfg(), std::size_t ring = 64 * 1024)
        : impl(new ProducerImpl(cfg, ring)), p(impl) {
        ProducerImpl::Hooks h;
        h.ctx = client.get();
        impl->setHooks(h);
    }
};
constexpr uint32_t kUnit = rc::producer::kAckRecordBytes;
}  // namespace

// ---------------------------------------------------------------------------
// Thread mode (Ring ingress)
// ---------------------------------------------------------------------------

TEST(ProducerClaim, RingClaimCommitDeliversTheRecordWrittenInPlace) {
    auto* impl = new ProducerImpl(voidCfg(), 64 * 1024);
    Producer p(impl);
    const Key key(uint64_t{7});
    rc::Claim c = p.tryClaim(kTopic, key, 5);
    ASSERT_TRUE(c) << static_cast<int>(c.result());
    EXPECT_EQ(c.result(), SendResult::Ok);
    ASSERT_EQ(c.buffer().size(), 5u);
    fill(c.buffer(), "hello");
    EXPECT_TRUE(c.commit());
    EXPECT_FALSE(c) << "a committed claim is settled";
    EXPECT_FALSE(c.commit()) << "settled: a second commit is a no-op";
    FrameCapture cap;
    pump(*impl, cap.fn());
    ASSERT_EQ(cap.recs.size(), 1u);
    EXPECT_EQ(cap.recs[0].value, "hello");
    EXPECT_EQ(cap.recs[0].key, std::string(reinterpret_cast<const char*>(key.bytes().data()), key.size()));
    EXPECT_EQ(cap.recs[0].partition, key.partition(kTopic.partitionCount));
}

TEST(ProducerClaim, RingAbortAndDestructorDeliverNothing) {
    auto* impl = new ProducerImpl(voidCfg(), 64 * 1024);
    Producer p(impl);
    {
        rc::Claim a = p.tryClaim(kTopic, Key(uint64_t{1}), 3);
        ASSERT_TRUE(a);
        fill(a.buffer(), "AAA");
        a.abort();
        EXPECT_FALSE(a);
        EXPECT_FALSE(a.commit());
    }
    {
        rc::Claim d = p.tryClaim(kTopic, Key(uint64_t{2}), 3);
        ASSERT_TRUE(d);
        fill(d.buffer(), "DDD");
    }  // the destructor aborts
    ASSERT_EQ(p.trySend(kTopic, Key(uint64_t{3}), bytes("CCC")), SendResult::Ok);
    FrameCapture cap;
    pump(*impl, cap.fn());
    EXPECT_EQ(cap.values(), std::vector<std::string>{"CCC"});
}

TEST(ProducerClaim, RingClaimIsBackPressuredWhenTheRingIsFullAndLosesNothing) {
    auto* impl = new ProducerImpl(voidCfg(), 4 * 1024);
    Producer p(impl);
    std::vector<rc::Claim> held;
    for (int i = 0; i < 10'000; ++i) {
        rc::Claim c = p.tryClaim(kTopic, Key(static_cast<uint64_t>(i)), 64);
        if (!c) {
            EXPECT_EQ(c.result(), SendResult::BackPressured);
            break;
        }
        std::memset(c.buffer().data(), i & 0xFF, 64);
        held.push_back(std::move(c));
    }
    ASSERT_FALSE(held.empty());
    ASSERT_LT(held.size(), 10'000u) << "a 4 KiB ring must fill";
    for (auto& c : held) EXPECT_TRUE(c.commit());
    FrameCapture cap;
    pump(*impl, cap.fn());
    EXPECT_EQ(cap.recs.size(), held.size());
}

TEST(ProducerClaim, OverCapAndInvalidClaimsAreRefusedNeverRetryable) {
    auto* impl = new ProducerImpl(voidCfg(), 64 * 1024);  // maxMsgLength 8 KiB
    Producer p(impl);
    const Key key(uint64_t{1});  // 8 key bytes
    EXPECT_EQ(p.tryClaim(kTopic, key, static_cast<uint32_t>(Producer::kMaxRecordBytes - 8 + 1)).result(),
              SendResult::MessageTooLarge);
    EXPECT_EQ(p.tryClaim(kTopic, key, 9000).result(), SendResult::ValueTooLarge)
        << "fits 16 KiB but never this ring";
    rc::Claim bad = p.tryClaim(TopicHandle{}, key, 4);
    EXPECT_FALSE(bad);
    EXPECT_EQ(bad.result(), SendResult::InvalidTopic);
    EXPECT_TRUE(bad.buffer().empty());
    EXPECT_FALSE(bad.commit());
    EXPECT_EQ(p.tryClaim(kTopic, key, 4, rc::AckToken{rc::kMaxToken + 1}).result(), SendResult::InvalidArgument);
}

TEST(ProducerClaim, ClosedProducerRefusesClaimsWithClosed) {
    auto* impl = new ProducerImpl(voidCfg(), 64 * 1024);
    Producer p(impl);
    ASSERT_EQ(p.closeImmediately(), rc::OpResult::Accepted);
    EXPECT_EQ(p.tryClaim(kTopic, Key(uint64_t{1}), 4).result(), SendResult::Closed);
}

TEST(ProducerClaim, AckTokenClaimCompletesWithItsOffsetAndAnAbortedOneNever) {
    ProducerConfig cfg = voidCfg();
    cfg.acks = Acks::Leader;
    auto* impl = new ProducerImpl(cfg, 64 * 1024);
    Producer p(impl);
    test_support::AckLedger L;
    L.attach(p);
    rc::Claim a = p.tryClaim(kTopic, Key(uint64_t{1}), 4, rc::AckToken{11});
    ASSERT_TRUE(a);
    fill(a.buffer(), "aaaa");
    rc::Claim b = p.tryClaim(kTopic, Key(uint64_t{1}), 4, rc::AckToken{12});
    ASSERT_TRUE(b);
    b.abort();
    ASSERT_TRUE(a.commit());
    FrameCapture cap;
    pump(*impl, cap.fn());
    ASSERT_EQ(cap.corrs.size(), 1u);
    impl->onProduceResponse(cap.corrs[0], 500, 0);
    ASSERT_EQ(L.recs.size(), 1u);
    EXPECT_EQ(L.recs[0].token, 11u);
    EXPECT_EQ(L.recs[0].value, 500);
    EXPECT_EQ(L.recs[0].status, rc::OpStatus::Ok);
    EXPECT_EQ(L.times(12), 0) << "an aborted claim never completes";
}

// Spec B copy count, Thread mode: the claim lives in the producer ring; the
// duty side copies it into the Produce frame (copy 1), the transport copies the
// frame (copy 2).
TEST(ProducerClaim, RingClaimIsCopiedOnceIntoTheFrame) {
    auto* impl = new ProducerImpl(voidCfg(), 64 * 1024);
    Producer p(impl);
    rc::Claim c = p.tryClaim(kTopic, Key(uint64_t{3}), 8);
    ASSERT_TRUE(c);
    fill(c.buffer(), "RINGBYTE");
    FrameCapture cap;
    cap.watch = c.buffer().data();
    ASSERT_TRUE(c.commit());
    pump(*impl, cap.fn());
    ASSERT_EQ(cap.values(), std::vector<std::string>{"RINGBYTE"});
    EXPECT_FALSE(cap.watchInFrame) << "Thread mode: ring -> frame -> Aeron (2 internal copies)";
}

// Spec B ordering: a claim never overtakes a Tier-2 batch queued before it on
// the same partition (same rule as a send).
TEST(ProducerClaim, ClaimNeverOvertakesAQueuedTier2Batch) {
    auto* impl = new ProducerImpl(voidCfg(), 64 * 1024);
    Producer p(impl);
    const Key key(uint64_t{5});
    ASSERT_EQ(p.trySend(kTopic, key, bytes("A")), SendResult::Ok);
    auto t2 = p.batch(kTopic, key);
    t2.add(bytes("X"));
    t2.add(bytes("Y"));
    ASSERT_EQ(t2.send(), rc::OpResult::Accepted);
    rc::Claim c = p.tryClaim(kTopic, key, 1);
    ASSERT_TRUE(c);
    fill(c.buffer(), "C");
    ASSERT_TRUE(c.commit());
    FrameCapture cap;
    pump(*impl, cap.fn());
    EXPECT_EQ(cap.values(), (std::vector<std::string>{"A", "X", "Y", "C"}));
}


// ---------------------------------------------------------------------------
// Completions credit (Task 4 fix round, I1): every failure branch returns it.
// ---------------------------------------------------------------------------

TEST(ProducerClaim, CqClaimCreditIsReturnedOnAbortDestructorAndRefusal) {
    CqRig r;
    rc::Completions cq(*r.client, smallCq());
    const Key key(uint64_t{1});
    {
        rc::Claim c = r.p.tryClaim(kTopic, key, 4, cq, 1);
        ASSERT_TRUE(c);
        EXPECT_EQ(cq.outstanding(), 1u);
        EXPECT_EQ(cq.reservedBytes(), kUnit);
        c.abort();
        EXPECT_EQ(cq.outstanding(), 0u) << "abort returns the reservation";
        EXPECT_EQ(cq.reservedBytes(), 0u);
    }
    {
        rc::Claim c = r.p.tryClaim(kTopic, key, 4, cq, 2);
        ASSERT_TRUE(c);
        EXPECT_EQ(cq.outstanding(), 1u);
    }  // destructor-abort
    EXPECT_EQ(cq.outstanding(), 0u) << "destructor-abort returns the reservation";
    EXPECT_EQ(cq.reservedBytes(), 0u);
    // A refused claim (after the reservation was made) returns it too.
    rc::Claim big = r.p.tryClaim(kTopic, key, static_cast<uint32_t>(Producer::kMaxRecordBytes), cq, 3);
    EXPECT_FALSE(big);
    EXPECT_EQ(big.result(), SendResult::MessageTooLarge);
    EXPECT_EQ(cq.outstanding(), 0u);
    EXPECT_EQ(cq.reservedBytes(), 0u);
    // A committed claim KEEPS its credit until the completion is polled.
    rc::Claim ok = r.p.tryClaim(kTopic, key, 4, cq, 4);
    ASSERT_TRUE(ok);
    ASSERT_TRUE(ok.commit());
    EXPECT_EQ(cq.outstanding(), 1u);
    EXPECT_EQ(cq.reservedBytes(), kUnit);
}

TEST(ProducerClaim, CqClaimIsRefusedWithoutCreditAndTheSlotIsReleasedWhenTheBatchQueueIsFull) {
    CqRig r;
    rc::Completions cq(*r.client, smallCq());
    rc::Completions bq(*r.client, smallCq(1024));
    r.p.batchAcksTo(bq);
    uint32_t filled = 0;
    while (bq.reserve(kUnit) == rc::OpResult::Accepted) ++filled;
    ASSERT_GT(filled, 0u);
    const uint32_t bqOutstanding = bq.outstanding();
    rc::Claim c = r.p.tryClaim(kTopic, Key(uint64_t{1}), 4, cq, 1);
    EXPECT_FALSE(c);
    EXPECT_EQ(c.result(), SendResult::QueueFull);
    EXPECT_EQ(cq.outstanding(), 0u) << "the per-record credit came back";
    EXPECT_EQ(cq.reservedBytes(), 0u);
    EXPECT_EQ(bq.outstanding(), bqOutstanding) << "no batch unit was taken";
    FrameCapture cap;
    pump(*r.impl, cap.fn());
    EXPECT_TRUE(cap.recs.empty()) << "the claimed slot was aborted, nothing is sent";
    for (uint32_t i = 0; i < filled; ++i) bq.unreserve(kUnit);
}

// Client close (closeFinal) unblocks a claim held across it: commit() reports
// false (nothing was accepted) and every reservation comes back.
TEST(ProducerClaim, CommitAfterTheCloseDrainUnblockIsFalseAndReturnsTheCredit) {
    CqRig r;
    rc::Completions cq(*r.client, smallCq());
    rc::Completions bq(*r.client, smallCq());
    r.p.batchAcksTo(bq);
    rc::Claim c = r.p.tryClaim(kTopic, Key(uint64_t{1}), 4, cq, 1);
    ASSERT_TRUE(c);
    EXPECT_EQ(cq.outstanding(), 1u);
    EXPECT_EQ(bq.outstanding(), 1u);
    r.impl->closeFinal(rc::OpStatus::Closed, 1);
    EXPECT_FALSE(c.commit()) << "released under the writer: nothing was accepted";
    EXPECT_FALSE(c);
    EXPECT_EQ(cq.outstanding(), 0u);
    EXPECT_EQ(cq.reservedBytes(), 0u);
    EXPECT_EQ(bq.outstanding(), 0u);
    EXPECT_EQ(bq.reservedBytes(), 0u);
}

TEST(ProducerClaim, AbortAndDestructorAfterTheCloseDrainUnblockAreSafeAndReturnTheCredit) {
    CqRig r;
    rc::Completions cq(*r.client, smallCq());
    {
        rc::Claim a = r.p.tryClaim(kTopic, Key(uint64_t{1}), 4, cq, 1);
        rc::Claim b = r.p.tryClaim(kTopic, Key(uint64_t{2}), 4, cq, 2);
        ASSERT_TRUE(a);
        ASSERT_TRUE(b);
        EXPECT_EQ(cq.outstanding(), 2u);
        r.impl->closeFinal(rc::OpStatus::Closed, 1);
        a.abort();  // must not throw out of a noexcept function
        EXPECT_EQ(cq.outstanding(), 1u);
    }  // b's destructor
    EXPECT_EQ(cq.outstanding(), 0u);
    EXPECT_EQ(cq.reservedBytes(), 0u);
}

// M1, claim layer: a record the close-drain unblock turned into padding is never
// reported committed.  (The exact verify/publish interleaving is forced in
// M2ORingBuffer.CommitRacingUnblockBetweenVerifyAndPublishThrowsRegression.)
TEST(ProducerClaimRegression, CommitRacingTheCloseUnblockNeverReportsALostRecordCommittedRegression) {
    CqRig r;
    rc::Completions cq(*r.client, smallCq());
    rc::Claim c = r.p.tryClaim(kTopic, Key(uint64_t{1}), 4, cq, 1);
    ASSERT_TRUE(c);
    // Same thread plays the drain: it unblocks the (uncommitted) head claim.
    EXPECT_TRUE(r.impl->testUnblockHeadClaim());
    EXPECT_FALSE(c.commit());
    EXPECT_EQ(cq.outstanding(), 0u) << "abandoned: no completion will follow, so the credit is back";
}

TEST(ProducerClaim, CommitOffTheOwnerThreadOfACarriedQueueStaysOpen) {
    CqRig r;
    rc::Completions cq(*r.client, smallCq());
    rc::Claim c = r.p.tryClaim(kTopic, Key(uint64_t{1}), 4, cq, 1);
    ASSERT_TRUE(c);
    fill(c.buffer(), "abcd");
    bool committed = true;
    std::thread t([&] { committed = c.commit(); });
    t.join();
    EXPECT_FALSE(committed);
    EXPECT_TRUE(c) << "still open: the owner thread settles it";
    EXPECT_EQ(cq.outstanding(), 1u) << "the foreign thread did not touch the counters";
    EXPECT_TRUE(c.commit());
    EXPECT_EQ(cq.outstanding(), 1u);
}

TEST(ProducerClaimAllocRegression, RingClaimSteadyStateAllocatesNothingRegression) {
    auto* impl = new ProducerImpl(voidCfg(), 1 << 20);
    Producer p(impl);
    const ProducerImpl::SendFn sink = [](int32_t, int32_t, int32_t, const uint8_t*, std::size_t) {
        return true;
    };
    auto cycle = [&](uint64_t k) {
        rc::Claim c = p.tryClaim(kTopic, Key(k), 64);
        if (c) {
            std::memset(c.buffer().data(), 0x5A, 64);
            (void)c.commit();
        }
        pump(*impl, sink);
    };
    for (uint64_t k = 0; k < 64; ++k) cycle(k);  // warm-up: builders, frames, ring
    alloc_counter::start();
    for (uint64_t k = 64; k < 1064; ++k) cycle(k);
    EXPECT_EQ(alloc_counter::stop(), 0L) << "tryClaim + commit + drain/flush must not allocate";
}

// Zero-allocation gate, the other claim paths: abort, destructor-abort, the
// Completions overload (abort / destructor-abort) and the AckToken overload
// (commit + ack round trip).
TEST(ProducerClaimAllocRegression, AbortCqAndAckTokenClaimPathsAllocateNothingRegression) {
    ProducerConfig cfg = voidCfg();
    cfg.acks = Acks::Leader;
    CqRig r(cfg, 1 << 20);
    rc::Completions cq(*r.client, smallCq());
    r.p.onAck([](void*, rc::Token, int64_t, rc::OpStatus, uint16_t) noexcept {}, nullptr);
    int64_t lastCorr = 0;
    const ProducerImpl::SendFn sink = [&](int32_t, int32_t, int32_t, const uint8_t* b, std::size_t n) {
        if (const auto v = rc::codec::decodeProduce(b, n)) lastCorr = v->correlationId;
        return true;
    };
    auto cycle = [&](uint64_t k) {
        {
            rc::Claim a = r.p.tryClaim(kTopic, Key(k), 64);
            if (a) a.abort();
        }
        {
            rc::Claim d = r.p.tryClaim(kTopic, Key(k), 64);
        }  // destructor-abort
        {
            rc::Claim a = r.p.tryClaim(kTopic, Key(k), 64, cq, 1);
            if (a) a.abort();
        }
        {
            rc::Claim d = r.p.tryClaim(kTopic, Key(k), 64, cq, 2);
        }
        rc::Claim t = r.p.tryClaim(kTopic, Key(k), 64, rc::AckToken{3});
        if (t) {
            std::memset(t.buffer().data(), 0x5A, 64);
            (void)t.commit();
        }
        lastCorr = 0;
        pump(*r.impl, sink);
        if (lastCorr != 0) r.impl->onProduceResponse(lastCorr, 500, 0);
    };
    for (uint64_t k = 0; k < 64; ++k) cycle(k);
    alloc_counter::start();
    for (uint64_t k = 64; k < 1064; ++k) cycle(k);
    EXPECT_EQ(alloc_counter::stop(), 0L) << "abort / destructor-abort / cq / AckToken claims must not allocate";
    EXPECT_EQ(cq.outstanding(), 0u);
}

// ---------------------------------------------------------------------------
// claimBatch — Thread mode (Ring ingress)
// ---------------------------------------------------------------------------

TEST(ProducerClaim, ClaimBatchLandsAsOneRecordBatchWithContiguousOffsets) {
    auto* impl = new ProducerImpl(voidCfg(), 64 * 1024);
    Producer p(impl);
    const Key key(uint64_t{8});
    rc::BatchClaim b = p.claimBatch(kTopic, key, 3, 2);
    ASSERT_TRUE(b) << static_cast<int>(b.result());
    ASSERT_EQ(b.count(), 3u);
    ASSERT_EQ(b.recordLength(), 2u);
    fill(b.record(0), "B1");
    fill(b.record(1), "B2");
    fill(b.record(2), "B3");
    EXPECT_TRUE(b.record(3).empty()) << "out of range";
    ASSERT_TRUE(b.commit());
    EXPECT_FALSE(b);
    FrameCapture cap;
    pump(*impl, cap.fn());
    ASSERT_EQ(cap.recs.size(), 3u);
    EXPECT_EQ(cap.frames, 1) << "one RecordBatch";
    for (int i = 0; i < 3; ++i) {
        EXPECT_EQ(cap.recs[i].frame, 0);
        EXPECT_EQ(cap.recs[i].offset, i) << "contiguous offsets";
        EXPECT_EQ(cap.recs[i].partition, key.partition(kTopic.partitionCount));
    }
    EXPECT_EQ(cap.values(), (std::vector<std::string>{"B1", "B2", "B3"}));
}

TEST(ProducerClaim, ClaimBatchAbortAndDestructorDeliverNone) {
    auto* impl = new ProducerImpl(voidCfg(), 64 * 1024);
    Producer p(impl);
    {
        rc::BatchClaim a = p.claimBatch(kTopic, Key(uint64_t{1}), 4, 4);
        ASSERT_TRUE(a);
        a.abort();
        EXPECT_TRUE(a.record(0).empty());
    }
    {
        rc::BatchClaim d = p.claimBatch(kTopic, Key(uint64_t{1}), 4, 4);
        ASSERT_TRUE(d);
    }  // the destructor aborts
    ASSERT_EQ(p.trySend(kTopic, Key(uint64_t{1}), bytes("Z")), SendResult::Ok);
    FrameCapture cap;
    pump(*impl, cap.fn());
    EXPECT_EQ(cap.values(), std::vector<std::string>{"Z"});
}

// All n records land in ONE batch: they join an open batch only when they fit
// it whole; otherwise that batch is flushed first and the claim starts a new one.
TEST(ProducerClaim, ClaimBatchJoinsAnOpenBatchOnlyWhenItFitsWhole) {
    {
        ProducerConfig cfg = voidCfg();
        cfg.lingerUs = 60'000'000;  // only a flush ends a batch
        auto* impl = new ProducerImpl(cfg, 64 * 1024);
        Producer p(impl);
        const Key key(uint64_t{2});
        ASSERT_EQ(p.trySend(kTopic, key, bytes("A")), SendResult::Ok);
        rc::BatchClaim b = p.claimBatch(kTopic, key, 3, 2);
        ASSERT_TRUE(b);
        fill(b.record(0), "B1");
        fill(b.record(1), "B2");
        fill(b.record(2), "B3");
        ASSERT_TRUE(b.commit());
        ASSERT_EQ(p.trySend(kTopic, key, bytes("C")), SendResult::Ok);
        p.flush();
        FrameCapture cap;
        pump(*impl, cap.fn());
        EXPECT_EQ(cap.values(), (std::vector<std::string>{"A", "B1", "B2", "B3", "C"}));
        EXPECT_EQ(cap.frames, 1);
        for (int i = 0; i < 5; ++i) EXPECT_EQ(cap.recs[i].offset, i);
    }
    {
        ProducerConfig cfg = voidCfg();
        cfg.lingerUs   = 60'000'000;
        cfg.batchBytes = 256;  // A (~75 wire bytes) + 4 x 40-byte records do not fit
        auto* impl = new ProducerImpl(cfg, 64 * 1024);
        Producer p(impl);
        const Key key(uint64_t{2});
        ASSERT_EQ(p.trySend(kTopic, key, bytes("A")), SendResult::Ok);
        rc::BatchClaim b = p.claimBatch(kTopic, key, 4, 40);
        ASSERT_TRUE(b) << static_cast<int>(b.result());
        for (uint32_t i = 0; i < 4; ++i) std::memset(b.record(i).data(), 'a' + static_cast<int>(i), 40);
        ASSERT_TRUE(b.commit());
        p.flush();
        FrameCapture cap;
        pump(*impl, cap.fn());
        ASSERT_EQ(cap.recs.size(), 5u);
        EXPECT_EQ(cap.recs[0].value, "A");
        EXPECT_NE(cap.recs[1].frame, cap.recs[0].frame) << "A was flushed first";
        for (int i = 1; i < 5; ++i) {
            EXPECT_EQ(cap.recs[i].frame, cap.recs[1].frame) << "the 4 claimed records share one batch";
            EXPECT_EQ(cap.recs[i].offset, i - 1);
            EXPECT_EQ(cap.recs[i].value, std::string(40, static_cast<char>('a' + i - 1)));
        }
    }
}

TEST(ProducerClaim, ClaimBatchNeverOvertakesAQueuedTier2Batch) {
    auto* impl = new ProducerImpl(voidCfg(), 64 * 1024);
    Producer p(impl);
    const Key key(uint64_t{5});
    ASSERT_EQ(p.trySend(kTopic, key, bytes("A")), SendResult::Ok);
    auto t2 = p.batch(kTopic, key);
    t2.add(bytes("X"));
    t2.add(bytes("Y"));
    ASSERT_EQ(t2.send(), rc::OpResult::Accepted);
    rc::BatchClaim b = p.claimBatch(kTopic, key, 2, 2);
    ASSERT_TRUE(b);
    fill(b.record(0), "B1");
    fill(b.record(1), "B2");
    ASSERT_TRUE(b.commit());
    ASSERT_EQ(p.trySend(kTopic, key, bytes("C")), SendResult::Ok);
    FrameCapture cap;
    pump(*impl, cap.fn());
    EXPECT_EQ(cap.values(), (std::vector<std::string>{"A", "X", "Y", "B1", "B2", "C"}));
}

TEST(ProducerClaim, ClaimBatchLimitsAreRefusedNeverRetryable) {
    {
        ProducerConfig cfg = voidCfg();
        cfg.maxFrameBytes = 4096;
        auto* impl = new ProducerImpl(cfg, 64 * 1024);
        Producer p(impl);
        const Key key(uint64_t{1});  // 8 key bytes
        EXPECT_EQ(p.claimBatch(kTopic, key, 0, 4).result(), SendResult::InvalidArgument);
        EXPECT_EQ(p.claimBatch(kTopic, key, 2, static_cast<uint32_t>(Producer::kMaxRecordBytes - 8 + 1)).result(),
                  SendResult::MessageTooLarge)
            << "per-record cap";
        EXPECT_EQ(p.claimBatch(kTopic, key, 100, 100).result(), SendResult::MessageTooLarge)
            << "~12 KiB batch > maxFrameBytes 4096";
        EXPECT_EQ(p.claimBatch(TopicHandle{}, key, 1, 1).result(), SendResult::InvalidTopic);
    }
    {
        ProducerConfig cfg = voidCfg();
        cfg.maxFrameBytes = 0;                                  // no frame cap
        auto* impl = new ProducerImpl(cfg, 64 * 1024);          // ring maxMsgLength 8 KiB
        Producer p(impl);
        EXPECT_EQ(p.claimBatch(kTopic, Key(uint64_t{1}), 100, 100).result(), SendResult::MessageTooLarge)
            << "42 + n x recLen > the ring's maxMsgLength";
    }
}

TEST(ProducerClaim, ClaimBatchDiscardedByCloseReportsOneBatchAckForAllItsRecords) {
    auto* impl = new ProducerImpl(voidCfg(), 64 * 1024);
    Producer p(impl);
    test_support::AckLedger L;
    L.attachBatches(*impl);
    rc::BatchClaim b = p.claimBatch(kTopic, Key(uint64_t{1}), 3, 2);
    ASSERT_TRUE(b);
    ASSERT_TRUE(b.commit());
    ASSERT_EQ(p.closeImmediately(), rc::OpResult::Accepted);  // the batch is still in the ring
    ASSERT_EQ(L.batchAcks.size(), 1u);
    EXPECT_EQ(L.batchAcks[0].recordCount, 3);
    EXPECT_EQ(L.batchAcks[0].baseSequence, -1);
    EXPECT_EQ(L.batchAcks[0].errorCode, rc::BatchAck::kClientFailure);
}

TEST(ProducerClaimAllocRegression, RingClaimBatchSteadyStateAllocatesNothingRegression) {
    auto* impl = new ProducerImpl(voidCfg(), 1 << 20);
    Producer p(impl);
    const ProducerImpl::SendFn sink = [](int32_t, int32_t, int32_t, const uint8_t*, std::size_t) {
        return true;
    };
    auto cycle = [&](uint64_t k) {
        rc::BatchClaim b = p.claimBatch(kTopic, Key(k), 4, 32);
        if (b) {
            for (uint32_t i = 0; i < 4; ++i) std::memset(b.record(i).data(), 0x5A, 32);
            (void)b.commit();
        }
        pump(*impl, sink);
    };
    for (uint64_t k = 0; k < 64; ++k) cycle(k);
    alloc_counter::start();
    for (uint64_t k = 64; k < 1064; ++k) cycle(k);
    EXPECT_EQ(alloc_counter::stop(), 0L) << "claimBatch + commit + drain/flush must not allocate";
}

// ---------------------------------------------------------------------------
// BatchClaim batch-ack credit (n units) and the close-drain unblock
// ---------------------------------------------------------------------------

TEST(ProducerClaim, ClaimBatchReservesNUnitsReturnedOnAbortDestructorAndRefusal) {
    CqRig r;
    rc::Completions bq(*r.client, smallCq());
    r.p.batchAcksTo(bq);
    {
        rc::BatchClaim b = r.p.claimBatch(kTopic, Key(uint64_t{1}), 3, 4);
        ASSERT_TRUE(b);
        EXPECT_EQ(bq.outstanding(), 3u);
        EXPECT_EQ(bq.reservedBytes(), 3 * kUnit);
        b.abort();
        EXPECT_EQ(bq.outstanding(), 0u);
    }
    {
        rc::BatchClaim b = r.p.claimBatch(kTopic, Key(uint64_t{1}), 5, 4);
        ASSERT_TRUE(b);
        EXPECT_EQ(bq.outstanding(), 5u);
    }
    EXPECT_EQ(bq.outstanding(), 0u) << "destructor-abort";
    EXPECT_EQ(bq.reservedBytes(), 0u);
    EXPECT_EQ(r.p.claimBatch(kTopic, Key(uint64_t{1}), 0, 4).result(), SendResult::InvalidArgument);
    EXPECT_EQ(bq.outstanding(), 0u) << "a size refusal takes nothing";
    // A batch bigger than the queue's room is refused QueueFull, all or none, and its slot is released.
    CqRig r2;
    rc::Completions tiny(*r2.client, smallCq(1024));
    r2.p.batchAcksTo(tiny);
    const uint32_t room = 1024 / kUnit;
    rc::BatchClaim huge = r2.p.claimBatch(kTopic, Key(uint64_t{1}), room + 8, 1);
    EXPECT_FALSE(huge);
    EXPECT_EQ(huge.result(), SendResult::QueueFull);
    EXPECT_EQ(tiny.outstanding(), 0u);
    FrameCapture cap;
    pump(*r2.impl, cap.fn());
    EXPECT_TRUE(cap.recs.empty());
    // Committed: the n units stay until the BatchAck is polled.
    rc::BatchClaim ok = r2.p.claimBatch(kTopic, Key(uint64_t{1}), 2, 4);
    ASSERT_TRUE(ok);
    ASSERT_TRUE(ok.commit());
    EXPECT_EQ(tiny.outstanding(), 2u);
}

TEST(ProducerClaim, ClaimBatchCommitAfterTheCloseDrainUnblockIsFalseAndReturnsTheCredit) {
    CqRig r;
    rc::Completions bq(*r.client, smallCq());
    r.p.batchAcksTo(bq);
    rc::BatchClaim b = r.p.claimBatch(kTopic, Key(uint64_t{1}), 3, 4);
    ASSERT_TRUE(b);
    EXPECT_EQ(bq.outstanding(), 3u);
    r.impl->closeFinal(rc::OpStatus::Closed, 1);
    EXPECT_FALSE(b.commit());
    EXPECT_FALSE(b);
    EXPECT_EQ(bq.outstanding(), 0u);
    EXPECT_EQ(bq.reservedBytes(), 0u);
    rc::BatchClaim d = r.p.claimBatch(kTopic, Key(uint64_t{1}), 2, 4);
    EXPECT_FALSE(d) << "a closed ring refuses";
    EXPECT_EQ(d.result(), SendResult::Closed);
    EXPECT_EQ(bq.outstanding(), 0u);
}

TEST(ProducerClaimRegression, ClaimBatchCommitRacingTheCloseUnblockNeverReportsALostBatchCommittedRegression) {
    CqRig r;
    rc::Completions bq(*r.client, smallCq());
    r.p.batchAcksTo(bq);
    rc::BatchClaim b = r.p.claimBatch(kTopic, Key(uint64_t{1}), 3, 4);
    ASSERT_TRUE(b);
    EXPECT_TRUE(r.impl->testUnblockHeadClaim());
    EXPECT_FALSE(b.commit());
    EXPECT_EQ(bq.outstanding(), 0u);
}

TEST(ProducerClaim, ClaimBatchCommitOffTheOwnerThreadOfACarriedQueueStaysOpen) {
    CqRig r;
    rc::Completions bq(*r.client, smallCq());
    r.p.batchAcksTo(bq);
    rc::BatchClaim b = r.p.claimBatch(kTopic, Key(uint64_t{1}), 2, 4);
    ASSERT_TRUE(b);
    bool committed = true;
    std::thread t([&] { committed = b.commit(); });
    t.join();
    EXPECT_FALSE(committed);
    EXPECT_TRUE(b) << "still open: the owner thread settles it";
    EXPECT_EQ(bq.outstanding(), 2u);
    EXPECT_TRUE(b.commit());
    EXPECT_EQ(bq.outstanding(), 2u);
}

TEST(ProducerClaimAllocRegression, ClaimBatchAbortDestructorAndCreditPathsAllocateNothingRegression) {
    CqRig r(voidCfg(), 1 << 20);
    rc::Completions bq(*r.client, smallCq(1u << 20));
    r.p.batchAcksTo(bq);
    const ProducerImpl::SendFn sink = [](int32_t, int32_t, int32_t, const uint8_t*, std::size_t) {
        return true;
    };
    auto cycle = [&](uint64_t k) {
        {
            rc::BatchClaim a = r.p.claimBatch(kTopic, Key(k), 4, 32);
            if (a) a.abort();
        }
        {
            rc::BatchClaim d = r.p.claimBatch(kTopic, Key(k), 4, 32);
        }
        {
            rc::BatchClaim c = r.p.claimBatch(kTopic, Key(k), 4, 32);
            if (c) {
                for (uint32_t i = 0; i < 4; ++i) std::memset(c.record(i).data(), 0x5A, 32);
                (void)c.commit();
            }
        }
        pump(*r.impl, sink);
    };
    for (uint64_t k = 0; k < 64; ++k) cycle(k);
    alloc_counter::start();
    for (uint64_t k = 64; k < 1064; ++k) cycle(k);
    EXPECT_EQ(alloc_counter::stop(), 0L) << "claimBatch abort / destructor / commit must not allocate";
}

TEST(ProducerClaimRegression, ClaimBatchWireSizeIsBoundedEvenWithoutAFrameCapRegression) {
    ProducerConfig cfg = voidCfg();
    cfg.maxFrameBytes = 0;  // no frame cap: the ring's maxMsgLength must still bound the batch
    CqRig r(cfg);
    rc::Completions bq(*r.client, smallCq());
    r.p.batchAcksTo(bq);
    const Key key(uint64_t{1});
    for (const uint32_t n : {100'000'000u, 1'000'000u, 0x7fffffffu}) {
        rc::BatchClaim b = r.p.claimBatch(kTopic, key, n, 0);
        EXPECT_FALSE(b);
        EXPECT_EQ(b.result(), SendResult::MessageTooLarge) << n;
    }
    EXPECT_EQ(r.p.claimBatch(kTopic, key, 0, 0).result(), SendResult::InvalidArgument);
    EXPECT_EQ(bq.outstanding(), 0u) << "nothing was credited";
    FrameCapture cap;
    pump(*r.impl, cap.fn());  // nothing was claimed: the drain has nothing to reserve
    EXPECT_TRUE(cap.recs.empty());
    // A small zero-length batch still works.
    rc::BatchClaim ok = r.p.claimBatch(kTopic, key, 3, 0);
    ASSERT_TRUE(ok);
    ASSERT_TRUE(ok.commit());
    pump(*r.impl, cap.fn());
    EXPECT_EQ(cap.recs.size(), 3u);
}

// ---------------------------------------------------------------------------
// Invoker mode (Direct ingress): the claim is reserved IN the Produce frame
// ---------------------------------------------------------------------------

namespace {
struct DirectRig {
    ProducerImpl* impl;
    Producer      p;
    explicit DirectRig(ProducerConfig cfg = voidCfg())
        : impl(new ProducerImpl(cfg, 4 * 1024)), p(impl) {
        impl->enableDirectIngress({std::this_thread::get_id(), nullptr, nullptr});
    }
    /// One owner duty pass (Direct: drainRing is a no-op; flush the builders).
    void pass(const ProducerImpl::SendFn& sink) { impl->flushBatches(kNow, 42, sink); }
};
}  // namespace

// Spec B copy count, Invoker mode: the claimed bytes ARE the Produce frame's
// bytes — the only copy left is the transport's (frame -> Aeron).
TEST(ProducerClaimDirect, ClaimWritesStraightIntoTheProduceFrame) {
    DirectRig r;
    rc::Claim c = r.p.tryClaim(kTopic, Key(uint64_t{5}), 4);
    ASSERT_TRUE(c) << static_cast<int>(c.result());
    fill(c.buffer(), "WXYZ");
    FrameCapture cap;
    cap.watch = c.buffer().data();
    ASSERT_TRUE(c.commit());
    r.pass(cap.fn());
    ASSERT_EQ(cap.values(), std::vector<std::string>{"WXYZ"});
    EXPECT_TRUE(cap.watchInFrame) << "Invoker: 1 copy (frame -> Aeron)";
}

// Review Focus 3.
TEST(ProducerClaimDirect, OpenClaimBlocksSendsAndIsNeverFlushedHalfWritten) {
    DirectRig r;
    const Key key(uint64_t{1});
    ASSERT_EQ(r.p.trySend(kTopic, key, bytes("A")), SendResult::Ok);
    rc::Claim c = r.p.tryClaim(kTopic, key, 1);
    ASSERT_TRUE(c);
    EXPECT_EQ(r.p.trySend(kTopic, Key(uint64_t{2}), bytes("x")), SendResult::ClaimHeld);
    EXPECT_EQ(r.p.tryClaim(kTopic, Key(uint64_t{2}), 1).result(), SendResult::ClaimHeld);
    EXPECT_EQ(r.p.claimBatch(kTopic, Key(uint64_t{2}), 2, 1).result(), SendResult::ClaimHeld);
    EXPECT_THROW(r.p.send(kTopic, Key(uint64_t{2}), bytes("x")), std::logic_error)
        << "a blocking send fails fast — pumping could never settle the caller's own claim";
    FrameCapture cap;
    r.pass(cap.fn());
    EXPECT_EQ(cap.frames, 0) << "a partition with an open claim is not flushed";
    c.buffer()[0] = 'B';
    ASSERT_TRUE(c.commit());
    ASSERT_EQ(r.p.trySend(kTopic, key, bytes("C")), SendResult::Ok);
    r.pass(cap.fn());
    EXPECT_EQ(cap.values(), (std::vector<std::string>{"A", "B", "C"}));
    ASSERT_EQ(cap.recs.size(), 3u);
    for (int i = 0; i < 3; ++i) EXPECT_EQ(cap.recs[i].offset, i);
}

// Review Focus 2.
TEST(ProducerClaimDirect, CloseAbandonsAnOpenClaimAndItsCommitReturnsFalse) {
    DirectRig r;
    test_support::AckLedger L;
    L.attachBatches(*r.impl);
    rc::Claim c = r.p.tryClaim(kTopic, Key(uint64_t{1}), 3);
    ASSERT_TRUE(c);
    fill(c.buffer(), "abc");
    ASSERT_EQ(r.p.closeImmediately(), rc::OpResult::Accepted);
    EXPECT_EQ(r.impl->claimsAbandoned(), 1u);
    EXPECT_FALSE(c.commit()) << "abandoned under the caller: nothing is sent";
    FrameCapture cap;
    r.pass(cap.fn());
    EXPECT_TRUE(cap.recs.empty());
    EXPECT_TRUE(L.batchAcks.empty()) << "an uncommitted claim was never accepted: no BatchAck";
}

TEST(ProducerClaimDirect, ClaimOffTheOwnerThreadIsWrongThread) {
    DirectRig r;
    SendResult one = SendResult::Ok;
    SendResult many = SendResult::Ok;
    std::thread t([&] {
        one  = r.p.tryClaim(kTopic, Key(uint64_t{1}), 4).result();
        many = r.p.claimBatch(kTopic, Key(uint64_t{1}), 2, 4).result();
    });
    t.join();
    EXPECT_EQ(one, SendResult::WrongThread);
    EXPECT_EQ(many, SendResult::WrongThread);
}

TEST(ProducerClaimDirect, ClaimBatchIsOneRecordBatchAndAbortRollsBackExactly) {
    DirectRig r;
    const Key key(uint64_t{1});
    ASSERT_EQ(r.p.trySend(kTopic, key, bytes("A")), SendResult::Ok);
    {
        rc::BatchClaim a = r.p.claimBatch(kTopic, key, 3, 1);
        ASSERT_TRUE(a);
        a.record(0)[0] = 'x';
        a.abort();
    }
    FrameCapture cap;
    {
        rc::BatchClaim b = r.p.claimBatch(kTopic, key, 2, 2);
        ASSERT_TRUE(b) << static_cast<int>(b.result());
        fill(b.record(0), "B1");
        fill(b.record(1), "B2");
        cap.watch = b.record(1).data();
        ASSERT_TRUE(b.commit());
    }
    ASSERT_EQ(r.p.trySend(kTopic, key, bytes("C")), SendResult::Ok);
    r.pass(cap.fn());
    EXPECT_EQ(cap.values(), (std::vector<std::string>{"A", "B1", "B2", "C"}));
    EXPECT_EQ(cap.frames, 1);
    for (int i = 0; i < 4; ++i) EXPECT_EQ(cap.recs[i].offset, i) << "the aborted batch left no gap";
    EXPECT_TRUE(cap.watchInFrame) << "Invoker claimBatch writes the frame in place too";
}

TEST(ProducerClaimDirect, AckedClaimCompletesWithItsOwnOffset) {
    ProducerConfig cfg = voidCfg();
    cfg.acks = Acks::Leader;
    DirectRig r(cfg);
    test_support::AckLedger L;
    L.attach(r.p);
    const Key key(uint64_t{1});
    ASSERT_EQ(r.p.trySend(kTopic, key, bytes("A")), SendResult::Ok);  // offsetDelta 0, void
    rc::Claim c = r.p.tryClaim(kTopic, key, 1, rc::AckToken{5});      // offsetDelta 1
    ASSERT_TRUE(c);
    c.buffer()[0] = 'B';
    ASSERT_TRUE(c.commit());
    FrameCapture cap;
    r.pass(cap.fn());
    ASSERT_EQ(cap.corrs.size(), 1u);
    r.impl->onProduceResponse(cap.corrs[0], 200, 0);
    ASSERT_EQ(L.recs.size(), 1u);
    EXPECT_EQ(L.recs[0].token, 5u);
    EXPECT_EQ(L.recs[0].value, 201) << "baseOffset + its own index";
}

// Claims are never deferred: inside a callback the producer fires during its
// own duty work, a claim is refused (use trySend() there — it defers).
TEST(ProducerClaimDirect, ClaimFromInsideAProducerCallbackIsBackPressured) {
    ProducerConfig cfg = voidCfg();
    cfg.acks = Acks::Leader;
    DirectRig r(cfg);
    struct Ctx {
        Producer*  p;
        SendResult got   = SendResult::Ok;
        int        calls = 0;
    } ctx{&r.p};
    r.p.onAck(
        [](void* c, rc::Token, int64_t, rc::OpStatus, uint16_t) noexcept {
            auto* x = static_cast<Ctx*>(c);
            ++x->calls;
            x->got = x->p->tryClaim(kTopic, Key(uint64_t{2}), 4).result();
        },
        &ctx);
    ASSERT_EQ(r.p.trySend(kTopic, Key(uint64_t{1}), bytes("a"), rc::AckToken{1}), SendResult::Ok);
    FrameCapture cap;
    r.pass(cap.fn());
    ASSERT_EQ(cap.corrs.size(), 1u);
    r.impl->onProduceResponse(cap.corrs[0], 10, 0);
    ASSERT_EQ(ctx.calls, 1);
    EXPECT_EQ(ctx.got, SendResult::BackPressured);
}

// ---------------------------------------------------------------------------
// Final review I1: an Invoker claim abandoned while the caller still holds it
// (a route failure / the delivery sweep inside a pump) must never leave the
// caller's span inside a buffer the producer appends into or seals.  The
// failure handler retries each failed record with a longer value (the retry
// marker), applied (deferred) at the end of the SAME duty pass — over the
// region the claim was reserved in.  The caller then writes its span; the
// retried bytes on the wire must be exact.
// ---------------------------------------------------------------------------
namespace {
struct RetryOnFailure {
    Producer*                p = nullptr;
    std::vector<std::string> sent;   // value of each AckToken (index = token - 1)
    int                      failed = 0;
    std::vector<rc::Token>   ok;
    static void onAck(void* c, rc::Token t, int64_t, rc::OpStatus st, uint16_t) noexcept {
        auto* x = static_cast<RetryOnFailure*>(c);
        if (st == rc::OpStatus::Ok) {
            x->ok.push_back(t);
            return;
        }
        ++x->failed;
        const std::string v = x->sent[t - 1] + "#retry-attempt-2";
        x->sent.push_back(v);
        (void)x->p->trySend(kTopic, Key(uint64_t{1}), bytes(v), rc::AckToken{x->sent.size()});
    }
    SendResult send(std::string v) {
        sent.push_back(std::move(v));
        return p->trySend(kTopic, Key(uint64_t{1}), bytes(sent.back()), rc::AckToken{sent.size()});
    }
};
uint16_t failEveryRoute(void*, int32_t, int32_t) noexcept { return 6; }  // NOT_LEADER

bool anyByteIs(const std::vector<std::string>& vs, char c) {
    for (const auto& v : vs) {
        if (v.find(c) != std::string::npos) return true;
    }
    return false;
}
}  // namespace

TEST(ProducerClaimRegression, RouteFailureAbandonKeepsTheHeldClaimOutOfTheRetriedFrameRegression) {
    ProducerConfig cfg = voidCfg();
    cfg.acks = Acks::Leader;
    DirectRig r(cfg);
    RetryOnFailure h;
    h.p = &r.p;
    r.p.onAck(&RetryOnFailure::onAck, &h);
    FrameCapture cap;
    r.pass(cap.fn());  // the duty clock
    ASSERT_EQ(h.send("A"), SendResult::Ok);
    ASSERT_EQ(h.send("B"), SendResult::Ok);
    rc::Claim c = r.p.tryClaim(kTopic, Key(uint64_t{1}), 24);
    ASSERT_TRUE(c);

    // A pump while holding (documented): the route fails, the handler retries.
    r.impl->failRoutedBroker(&failEveryRoute, nullptr);
    ASSERT_EQ(h.failed, 2);
    EXPECT_EQ(r.impl->claimsAbandoned(), 1u);
    // The caller, not yet told, finishes encoding into its span.
    std::memset(c.buffer().data(), 'Z', c.buffer().size());
    EXPECT_FALSE(c.commit()) << "abandoned under the caller: nothing is sent";

    r.pass(cap.fn());
    EXPECT_EQ(cap.values(), (std::vector<std::string>{"A#retry-attempt-2", "B#retry-attempt-2"}))
        << "the deferred retries are sent intact after the claim settles";
    EXPECT_FALSE(anyByteIs(cap.values(), 'Z')) << "the abandoned claim's bytes never reach a frame";
    ASSERT_EQ(cap.corrs.size(), 1u);
    r.impl->onProduceResponse(cap.corrs[0], 100, 0);
    EXPECT_EQ(h.ok, (std::vector<rc::Token>{3, 4}));
}

TEST(ProducerClaimRegression, DeliverySweepAbandonKeepsTheHeldBatchClaimOutOfTheRetriedFrameRegression) {
    ProducerConfig cfg = voidCfg();
    cfg.acks              = Acks::Leader;
    cfg.deliveryTimeoutMs = 100;
    DirectRig r(cfg);
    RetryOnFailure h;
    h.p = &r.p;
    r.p.onAck(&RetryOnFailure::onAck, &h);
    FrameCapture cap;
    r.pass(cap.fn());  // the duty clock: the records below are accepted at kNow
    ASSERT_EQ(h.send("early-1"), SendResult::Ok);
    rc::BatchClaim b = r.p.claimBatch(kTopic, Key(uint64_t{1}), 3, 8);
    ASSERT_TRUE(b);

    // The records were never sent (no session): the sweep fails the builder.
    r.impl->sweepTimedOut(std::chrono::steady_clock::time_point(std::chrono::nanoseconds(kNow + 200'000'000)));
    ASSERT_EQ(h.failed, 1);
    EXPECT_EQ(r.impl->claimsAbandoned(), 1u);
    for (uint32_t i = 0; i < b.count(); ++i) std::memset(b.record(i).data(), 'Z', b.recordLength());
    EXPECT_FALSE(b.commit());

    r.impl->flushBatches(kNow + 200'000'000, 42, cap.fn());  // the duty clock moved on with the sweep
    EXPECT_EQ(cap.values(), (std::vector<std::string>{"early-1#retry-attempt-2"}));
    EXPECT_FALSE(anyByteIs(cap.values(), 'Z'));
}

// While an abandoned claim is unsettled the producer sends (the builder no
// longer holds the claim) but refuses a second claim; settling it — abort, or
// the destructor — makes claims available again.
TEST(ProducerClaimRegression, AbandonedClaimRefusesASecondClaimUntilSettledRegression) {
    ProducerConfig cfg = voidCfg();
    cfg.acks = Acks::Leader;
    DirectRig r(cfg);
    test_support::AckLedger L;
    L.attach(r.p);
    FrameCapture cap;
    r.pass(cap.fn());
    ASSERT_EQ(r.p.trySend(kTopic, Key(uint64_t{1}), bytes("A"), rc::AckToken{1}), SendResult::Ok);
    {
        rc::Claim c = r.p.tryClaim(kTopic, Key(uint64_t{1}), 8);
        ASSERT_TRUE(c);
        r.impl->failRoutedBroker(&failEveryRoute, nullptr);
        EXPECT_EQ(r.p.trySend(kTopic, Key(uint64_t{1}), bytes("B")), SendResult::Ok)
            << "the abandoned claim no longer blocks sends";
        EXPECT_EQ(r.p.tryClaim(kTopic, Key(uint64_t{2}), 4).result(), SendResult::ClaimHeld);
        EXPECT_EQ(r.p.claimBatch(kTopic, Key(uint64_t{2}), 2, 4).result(), SendResult::ClaimHeld);
        std::memset(c.buffer().data(), 'Z', c.buffer().size());
    }  // the destructor settles it
    rc::Claim d = r.p.tryClaim(kTopic, Key(uint64_t{1}), 1);
    ASSERT_TRUE(d) << static_cast<int>(d.result());
    d.buffer()[0] = 'C';
    ASSERT_TRUE(d.commit());
    r.pass(cap.fn());
    EXPECT_EQ(cap.values(), (std::vector<std::string>{"B", "C"}));
}

// Repeated live abandons swap the claimed storage with the one quarantine
// buffer: no allocation once warm.
TEST(ProducerClaimAllocRegression, RepeatedLiveAbandonsAllocateNothingRegression) {
    ProducerConfig cfg = voidCfg();
    cfg.acks = Acks::Leader;
    DirectRig r(cfg);
    r.p.onAck([](void*, rc::Token, int64_t, rc::OpStatus, uint16_t) noexcept {}, nullptr);
    const ProducerImpl::SendFn sink = [](int32_t, int32_t, int32_t, const uint8_t*, std::size_t) {
        return true;
    };
    r.pass(sink);
    const std::vector<uint8_t> early = bytes("early");  // built once: the cycle allocates nothing itself
    auto cycle = [&](uint64_t k) {
        (void)r.p.trySend(kTopic, Key(k), early, rc::AckToken{k + 1});
        rc::Claim c = r.p.tryClaim(kTopic, Key(k), 64);
        r.impl->failRoutedBroker(&failEveryRoute, nullptr);
        if (!c.buffer().empty()) std::memset(c.buffer().data(), 'Z', c.buffer().size());
        (void)c.commit();
        r.pass(sink);
    };
    for (uint64_t k = 0; k < 64; ++k) cycle(k);
    alloc_counter::start();
    for (uint64_t k = 64; k < 1064; ++k) cycle(k);
    EXPECT_EQ(alloc_counter::stop(), 0L) << "an abandon swaps storage; it never allocates";
    EXPECT_EQ(r.impl->claimsAbandoned(), 1064u);
}

TEST(ProducerClaimAllocRegression, DirectClaimAndClaimBatchSteadyStateAllocateNothingRegression) {
    DirectRig r;
    const ProducerImpl::SendFn sink = [](int32_t, int32_t, int32_t, const uint8_t*, std::size_t) {
        return true;
    };
    auto cycle = [&](uint64_t k) {
        {
            rc::Claim c = r.p.tryClaim(kTopic, Key(k), 64);
            if (c) {
                std::memset(c.buffer().data(), 0x5A, 64);
                (void)c.commit();
            }
        }
        {
            rc::BatchClaim b = r.p.claimBatch(kTopic, Key(k), 4, 32);
            if (b) {
                for (uint32_t i = 0; i < 4; ++i) std::memset(b.record(i).data(), 0x5B, 32);
                (void)b.commit();
            }
        }
        r.pass(sink);
    };
    for (uint64_t k = 0; k < 64; ++k) cycle(k);  // warm-up: builders, frames, claim offsets
    alloc_counter::start();
    for (uint64_t k = 64; k < 1064; ++k) cycle(k);
    EXPECT_EQ(alloc_counter::stop(), 0L) << "Invoker claims must not allocate in steady state";
}

// A Direct claimBatch with no frame cap (maxFrameBytes == 0) is still bounded:
// Direct has no ring traffic (1 KiB trailer-only ring), so the fallback is the
// max message of the ring Thread mode is sized from.
TEST(ProducerClaimDirect, ClaimBatchWireSizeIsBoundedEvenWithoutAFrameCapRegression) {
    ProducerConfig cfg = voidCfg();
    cfg.maxFrameBytes  = 0;
    DirectRig r(cfg);
    const Key key(uint64_t{1});
    for (const uint32_t n : {100'000'000u, 1'000'000u, 0x7fffffffu}) {
        rc::BatchClaim b = r.p.claimBatch(kTopic, key, n, 0);
        EXPECT_FALSE(b);
        EXPECT_EQ(b.result(), SendResult::MessageTooLarge) << n;
    }
    EXPECT_EQ(r.p.claimBatch(kTopic, key, 0, 0).result(), SendResult::InvalidArgument);
    FrameCapture cap;
    r.pass(cap.fn());
    EXPECT_TRUE(cap.recs.empty());
    rc::BatchClaim ok = r.p.claimBatch(kTopic, key, 3, 0);
    ASSERT_TRUE(ok);
    ASSERT_TRUE(ok.commit());
    r.pass(cap.fn());
    EXPECT_EQ(cap.recs.size(), 3u);
}

// Zero-allocation gate, the other Direct claim paths: abort, destructor-abort
// (single and batch), and a refused (ClaimHeld) claim.
TEST(ProducerClaimAllocRegression, DirectClaimAbortAndDestructorAllocateNothingRegression) {
    DirectRig r;
    const ProducerImpl::SendFn sink = [](int32_t, int32_t, int32_t, const uint8_t*, std::size_t) {
        return true;
    };
    auto cycle = [&](uint64_t k) {
        {
            rc::Claim a = r.p.tryClaim(kTopic, Key(k), 64);
            if (a) a.abort();
        }
        {
            rc::Claim d = r.p.tryClaim(kTopic, Key(k), 64);  // destructor aborts
            (void)r.p.tryClaim(kTopic, Key(k), 8);           // ClaimHeld: refused, no state touched
        }
        {
            rc::BatchClaim b = r.p.claimBatch(kTopic, Key(k), 4, 32);
            if (b) b.abort();
        }
        {
            rc::BatchClaim d = r.p.claimBatch(kTopic, Key(k), 4, 32);  // destructor aborts
        }
        rc::Claim c = r.p.tryClaim(kTopic, Key(k), 16);
        if (c) (void)c.commit();
        r.pass(sink);
    };
    for (uint64_t k = 0; k < 64; ++k) cycle(k);
    alloc_counter::start();
    for (uint64_t k = 64; k < 1064; ++k) cycle(k);
    EXPECT_EQ(alloc_counter::stop(), 0L) << "Invoker abort / destructor / refused claims must not allocate";
}

