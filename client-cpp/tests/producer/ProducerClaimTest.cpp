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
#include <rune/client/producer/ProduceFrameBuilder.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/protocol/client/DataMessages.h>

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

// #322: a credit claim aborted off its queue's owner thread cannot return the credit (owner-thread state): the
// ring record is published as kMsgClaimCreditLost, the duty side counts it and completes nothing.
TEST(ProducerClaimRegression, OffThreadAbortOfACreditClaimIsCountedRegression) {
    CqRig r;
    rc::Completions cq(*r.client, smallCq());
    FrameCapture cap;
    rc::Claim c = r.p.tryClaim(kTopic, Key(uint64_t{1}), 4, cq, 1);
    ASSERT_TRUE(c);
    std::thread t([&] { rc::Claim dead(std::move(c)); });  // destroyed (aborted) off the owner thread
    t.join();
    pump(*r.impl, cap.fn());
    EXPECT_EQ(r.impl->claimCreditLost(), 1u);
    int completions = 0;
    cq.poll([&](const rc::Completion&, std::span<const uint8_t>) { ++completions; });
    EXPECT_EQ(completions, 0) << "nothing completes for a lost credit";
    EXPECT_TRUE(cap.values().empty());
    rc::Claim next = r.p.tryClaim(kTopic, Key(uint64_t{1}), 4, cq, 2);
    EXPECT_TRUE(next) << static_cast<int>(next.result());
}

TEST(ProducerClaimRegression, OffThreadAbortOfACreditBatchClaimIsCountedRegression) {
    CqRig r;
    rc::Completions bq(*r.client, smallCq());
    r.p.batchAcksTo(bq);
    FrameCapture cap;
    rc::BatchClaim b = r.p.claimBatch(kTopic, Key(uint64_t{1}), 3, 4);
    ASSERT_TRUE(b);
    std::thread t([&] { rc::BatchClaim dead(std::move(b)); });
    t.join();
    pump(*r.impl, cap.fn());
    EXPECT_EQ(r.impl->claimCreditLost(), 1u);
    int acks = 0;
    bq.poll([&](const rc::Completion&, std::span<const uint8_t>) { ++acks; });
    EXPECT_EQ(acks, 0) << "no BatchAck for the 3 records";
    EXPECT_TRUE(cap.values().empty());
    rc::BatchClaim next = r.p.claimBatch(kTopic, Key(uint64_t{1}), 2, 4);
    EXPECT_TRUE(next) << static_cast<int>(next.result());
}

// The discarding duty side (closeImmediately) counts it too.
TEST(ProducerClaimRegression, OffThreadAbortOfACreditClaimInADiscardingProducerRegression) {
    CqRig r;
    rc::Completions cq(*r.client, smallCq());
    FrameCapture cap;
    rc::Claim c = r.p.tryClaim(kTopic, Key(uint64_t{1}), 4, cq, 1);
    ASSERT_TRUE(c);
    std::thread t([&] { rc::Claim dead(std::move(c)); });
    t.join();
    ASSERT_EQ(r.p.closeImmediately(), rc::OpResult::Accepted);
    pump(*r.impl, cap.fn());
    EXPECT_EQ(r.impl->claimCreditLost(), 1u);
}

// C1 adversarial review (#322): a Thread-mode claim with no queue settles from any thread exactly as before -
// commit delivers, abort pads (never counted as lost credit).
TEST(ProducerClaimRegression, NoQueueRingClaimSettlesFromAnyThreadAdversarialRegression) {
    auto* impl = new ProducerImpl(voidCfg(), 64 * 1024);
    Producer p(impl);
    rc::Claim a = p.tryClaim(kTopic, Key(uint64_t{1}), 4);
    ASSERT_TRUE(a);
    fill(a.buffer(), "AAAA");
    rc::Claim b = p.tryClaim(kTopic, Key(uint64_t{1}), 4);
    ASSERT_TRUE(b);
    fill(b.buffer(), "BBBB");
    bool committed = false;
    std::thread t([&] {
        committed = a.commit();
        b.abort();
    });
    t.join();
    EXPECT_TRUE(committed) << "no queue: the commit is not owner-bound";
    EXPECT_FALSE(b) << "aborted";
    FrameCapture cap;
    pump(*impl, cap.fn());
    EXPECT_EQ(cap.values(), std::vector<std::string>{"AAAA"});
    EXPECT_EQ(impl->claimCreditLost(), 0u) << "no credit was carried: nothing lost";
    EXPECT_EQ(impl->ringForeignMessages(), 0u);
}

// C1 adversarial review (#322): the credit-lost record sits between ordinary records; the drain counts it and
// delivers the records on both sides in order, never treating it as foreign or stopping the pass on it.
TEST(ProducerClaimRegression, CreditLostRecordBetweenRecordsKeepsOrderAdversarialRegression) {
    CqRig r;
    rc::Completions cq(*r.client, smallCq());
    ASSERT_EQ(r.p.trySend(kTopic, Key(uint64_t{1}), bytes("A")), SendResult::Ok);
    rc::Claim c = r.p.tryClaim(kTopic, Key(uint64_t{1}), 4, cq, 1);
    ASSERT_TRUE(c);
    fill(c.buffer(), "LOST");
    ASSERT_EQ(r.p.trySend(kTopic, Key(uint64_t{1}), bytes("B")), SendResult::Ok);
    std::thread t([&] { c.abort(); });
    t.join();
    EXPECT_FALSE(c);
    c.abort();  // settled: a second abort (and the destructor) does nothing
    FrameCapture cap;
    pump(*r.impl, cap.fn());
    EXPECT_EQ(cap.values(), (std::vector<std::string>{"A", "B"}));
    EXPECT_EQ(r.impl->claimCreditLost(), 1u) << "counted exactly once";
    EXPECT_EQ(r.impl->ringForeignMessages(), 0u);
}

// C1 adversarial review (#322): the close drain's unblock already turned the held claim into padding; the
// off-thread abort that follows must neither throw, count, nor touch the record the ring holds next.
TEST(ProducerClaimRegression, OffThreadAbortAfterUnblockIsANoOpAdversarialRegression) {
    CqRig r;
    rc::Completions cq(*r.client, smallCq());
    rc::Claim c = r.p.tryClaim(kTopic, Key(uint64_t{1}), 4, cq, 1);
    ASSERT_TRUE(c);
    ASSERT_TRUE(r.impl->testUnblockHeadClaim());
    FrameCapture cap;
    pump(*r.impl, cap.fn());  // consumes the padding
    rc::Claim next = r.p.tryClaim(kTopic, Key(uint64_t{1}), 4, cq, 2);
    ASSERT_TRUE(next);
    fill(next.buffer(), "NEXT");
    std::thread t([&] { rc::Claim dead(std::move(c)); });
    t.join();
    ASSERT_TRUE(next.commit()) << "the stale abort must not touch the next claim";
    pump(*r.impl, cap.fn());
    EXPECT_EQ(cap.values(), std::vector<std::string>{"NEXT"});
    EXPECT_EQ(r.impl->claimCreditLost(), 0u) << "already released: nothing to report";
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
    RetryOnFailure h;   // outlives the producer: its close fails the pending records into onAck
    DirectRig r(cfg);
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

// #325: the abandoned span must stay inside the quarantine vector's live size (ASan container-overflow
// otherwise), however the caller writes it.
TEST(ProducerClaimRegression, WritingAnAbandonedSpanStaysInsideTheQuarantineVectorRegression) {
    ProducerConfig cfg = voidCfg();
    cfg.acks = Acks::Leader;
    RetryOnFailure h;  // outlives the rig: ~Producer fails what is pending through onAck
    DirectRig r(cfg);
    h.p = &r.p;
    r.p.onAck(&RetryOnFailure::onAck, &h);
    FrameCapture cap;
    r.pass(cap.fn());  // the duty clock
    ASSERT_EQ(h.send("A"), SendResult::Ok);
    rc::Claim c = r.p.tryClaim(kTopic, Key(uint64_t{1}), 64);
    ASSERT_TRUE(c);

    r.impl->failRoutedBroker(&failEveryRoute, nullptr);
    ASSERT_EQ(r.impl->claimsAbandoned(), 1u);
    ASSERT_EQ(c.buffer().size(), 64u);
    EXPECT_TRUE(r.impl->claimQuarantineCovers(c.buffer().data(), c.buffer().size()))
        << "the quarantined vector must cover the whole claimed span";
    std::memset(c.buffer().data(), 0xAB, c.buffer().size());

    r.pass(cap.fn());
    EXPECT_FALSE(anyByteIs(cap.values(), static_cast<char>(0xAB))) << "the span never reaches a frame";
    EXPECT_FALSE(c.commit());
}

// #325: the close path keeps the records before the claim and quarantines the whole span.
TEST(ProducerClaimRegression, CloseAbandonKeepsTheRecordsBeforeTheClaimAndQuarantinesTheWholeSpanRegression) {
    ProducerConfig cfg = voidCfg();
    cfg.acks = Acks::Leader;
    DirectRig r(cfg);
    FrameCapture cap;
    r.pass(cap.fn());
    ASSERT_EQ(r.p.trySend(kTopic, Key(uint64_t{1}), bytes("A")), SendResult::Ok);
    ASSERT_EQ(r.p.trySend(kTopic, Key(uint64_t{1}), bytes("B")), SendResult::Ok);
    rc::Claim c = r.p.tryClaim(kTopic, Key(uint64_t{1}), 64);
    ASSERT_TRUE(c);

    r.impl->abandonOpenClaimForClose();  // what Producer::close does once the close is issued
    ASSERT_EQ(r.impl->claimsAbandoned(), 1u);
    EXPECT_TRUE(r.impl->claimQuarantineCovers(c.buffer().data(), c.buffer().size()));
    std::memset(c.buffer().data(), 0xAB, c.buffer().size());

    r.pass(cap.fn());  // the drain
    EXPECT_EQ(cap.values(), (std::vector<std::string>{"A", "B"})) << "the records before the claim are delivered";
    EXPECT_FALSE(anyByteIs(cap.values(), static_cast<char>(0xAB))) << "the claim's bytes never reach the wire";
    EXPECT_FALSE(c.commit());
}

// C1 adversarial review (#322): an Invoker claim settled off the owner thread never touches owner state: commit
// is refused and the claim stays open (the owner commits it), an off-owner destroy leaves it held until the
// close abandons it, and the records before it still drain.
TEST(ProducerClaimRegression, InvokerClaimOffOwnerCommitStaysOpenAdversarialRegression) {
    ProducerConfig cfg = voidCfg();
    cfg.acks = Acks::Leader;
    DirectRig r(cfg);
    FrameCapture cap;
    r.pass(cap.fn());
    rc::Claim c = r.p.tryClaim(kTopic, Key(uint64_t{1}), 4);
    ASSERT_TRUE(c);
    fill(c.buffer(), "OWNR");
    bool committed = true;
    std::thread t([&] { committed = c.commit(); });
    t.join();
    EXPECT_FALSE(committed);
    EXPECT_TRUE(c) << "still open";
    EXPECT_EQ(r.p.trySend(kTopic, Key(uint64_t{2}), bytes("x")), SendResult::ClaimHeld);
    ASSERT_TRUE(c.commit()) << "the owner settles it";
    r.pass(cap.fn());
    EXPECT_EQ(cap.values(), std::vector<std::string>{"OWNR"});
}

TEST(ProducerClaimRegression, InvokerClaimDestroyedOffOwnerIsHeldUntilCloseAdversarialRegression) {
    ProducerConfig cfg = voidCfg();
    cfg.acks = Acks::Leader;
    DirectRig r(cfg);
    FrameCapture cap;
    r.pass(cap.fn());
    ASSERT_EQ(r.p.trySend(kTopic, Key(uint64_t{1}), bytes("A")), SendResult::Ok);
    rc::Claim c = r.p.tryClaim(kTopic, Key(uint64_t{1}), 64);
    ASSERT_TRUE(c);
    std::memset(c.buffer().data(), 0xAB, c.buffer().size());
    std::thread t([&] { rc::Claim dead(std::move(c)); });
    t.join();
    EXPECT_EQ(r.p.trySend(kTopic, Key(uint64_t{1}), bytes("x")), SendResult::ClaimHeld) << "held until close";
    EXPECT_EQ(r.impl->claimsAbandoned(), 0u);
    r.impl->abandonOpenClaimForClose();
    EXPECT_EQ(r.impl->claimsAbandoned(), 1u);
    r.pass(cap.fn());
    EXPECT_EQ(cap.values(), std::vector<std::string>{"A"}) << "the record before the claim drains";
    EXPECT_FALSE(anyByteIs(cap.values(), static_cast<char>(0xAB)));
}

TEST(ProducerClaimRegression, DeliverySweepAbandonKeepsTheHeldBatchClaimOutOfTheRetriedFrameRegression) {
    ProducerConfig cfg = voidCfg();
    cfg.acks              = Acks::Leader;
    cfg.deliveryTimeoutMs = 100;
    RetryOnFailure h;   // outlives the producer: its close fails the pending records into onAck
    DirectRig r(cfg);
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


// ===========================================================================
// C10 (#331): claim test gaps.  Every drive loop is iteration-capped; nothing sleeps.
// ===========================================================================
namespace {

/// Acks every frame `cap` has offered that `acked` has not yet seen (baseOffset = 100 * frame index).
void ackNewFrames(ProducerImpl& impl, const FrameCapture& cap, std::size_t& acked) {
    while (acked < cap.corrs.size()) {
        impl.onProduceResponse(cap.corrs[acked], static_cast<int64_t>(acked) * 100, 0);
        ++acked;
    }
}

bool containsRun(const std::vector<std::vector<uint8_t>>& frames, uint8_t b, std::size_t run) {
    for (const auto& f : frames) {
        std::size_t n = 0;
        for (const uint8_t c : f) {
            n = (c == b) ? n + 1 : 0;
            if (n >= run) return true;
        }
    }
    return false;
}

struct CloseTokenLog {
    std::vector<rc::Completion> got;
    static void fn(void* ctx, const rc::Completion& c, std::span<const uint8_t>) noexcept {
        static_cast<CloseTokenLog*>(ctx)->got.push_back(c);
    }
    rc::CompletionHandler handler() { return rc::CompletionHandler{&CloseTokenLog::fn, this}; }
};

}  // namespace

// #331: the pointer the caller wrote through IS the decoded record value of the captured frame (offset equality,
// not range containment): no copy of the claim sits between the caller's span and the wire bytes.
TEST(ProducerClaimRegression, ClaimedValueIsTheDecodedRecordValueRegression) {
    DirectRig r;
    FrameCapture cap;
    cap.keepRaw = true;
    rc::Claim a = r.p.tryClaim(kTopic, Key(uint64_t{5}), 4);
    ASSERT_TRUE(a) << static_cast<int>(a.result());
    fill(a.buffer(), "WXYZ");
    const uint8_t* aPtr = a.buffer().data();
    ASSERT_TRUE(a.commit());
    rc::BatchClaim b = r.p.claimBatch(kTopic, Key(uint64_t{5}), 3, 2);
    ASSERT_TRUE(b);
    fill(b.record(0), "B1");
    fill(b.record(1), "B2");
    fill(b.record(2), "B3");
    const uint8_t* bPtr[3] = {b.record(0).data(), b.record(1).data(), b.record(2).data()};
    ASSERT_TRUE(b.commit());
    r.pass(cap.fn());
    ASSERT_EQ(cap.frames, 1);
    ASSERT_EQ(cap.recs.size(), 4u);
    EXPECT_EQ(cap.recs[0].value, "WXYZ");
    EXPECT_EQ(cap.recs[0].frameBase + cap.recs[0].valueOffset, aPtr) << "the claim span IS the decoded value";
    for (int i = 0; i < 3; ++i) {
        EXPECT_EQ(cap.recs[1 + i].frameBase + cap.recs[1 + i].valueOffset, bPtr[i]) << "batch record " << i;
    }
    // The frame bytes at that offset (the capture's own copy) carry the written value.
    EXPECT_EQ(0, std::memcmp(cap.rawFrames[0].data() + cap.recs[0].valueOffset, "WXYZ", 4));
}

// #331: back-pressure loses nothing AND keeps every record's bytes and the claim order.
TEST(ProducerClaimRegression, BackPressuredClaimsLandWholeAndInOrderRegression) {
    auto* impl = new ProducerImpl(voidCfg(), 4 * 1024);
    Producer p(impl);
    std::vector<rc::Claim>   held;
    std::vector<std::string> expect;
    for (int i = 0; i < 10'000; ++i) {
        rc::Claim c = p.tryClaim(kTopic, Key(uint64_t{7}), 64);
        if (!c) {
            ASSERT_EQ(c.result(), SendResult::BackPressured);
            break;
        }
        const std::string v(64, static_cast<char>('a' + i % 26));
        fill(c.buffer(), v);
        expect.push_back(v);
        held.push_back(std::move(c));
    }
    ASSERT_GT(expect.size(), 2u);
    ASSERT_LT(expect.size(), 10'000u) << "a 4 KiB ring must fill";
    for (auto& c : held) ASSERT_TRUE(c.commit());
    FrameCapture cap;
    pump(*impl, cap.fn());
    EXPECT_EQ(cap.values(), expect) << "every record's bytes, in claim order";
}

// #331: a claim batch that sits in the ring under a full in-flight window (the builder is at batchBytes, so the
// ring drain ABORTs at it) lands WHOLE - one RecordBatch, contiguous offsets, no foreign record between - once an
// ack opens the window.
TEST(ProducerClaimRegression, ARingHeldClaimBatchLandsWholeWhenTheWindowOpensRegression) {
    ProducerConfig cfg = voidCfg();
    cfg.acks        = Acks::Leader;
    cfg.maxInFlight = 1;
    cfg.batchBytes  = 256;
    auto* impl = new ProducerImpl(cfg, 64 * 1024);
    Producer p(impl);
    const Key key(uint64_t{2});
    FrameCapture cap;
    std::size_t  acked = 0;
    ASSERT_EQ(p.trySend(kTopic, key, bytes("A")), SendResult::Ok);
    pump(*impl, cap.fn());
    ASSERT_EQ(cap.frames, 1) << "A is in flight: the window (1) is full";
    int fillers = 0;
    for (; fillers < 64; ++fillers) {  // fill the builder to batchBytes: the next ring record cannot be admitted
        if (p.trySend(kTopic, key, std::vector<uint8_t>(40, 'F')) != SendResult::Ok) break;
    }
    ASSERT_GT(fillers, 0);
    rc::BatchClaim b = p.claimBatch(kTopic, key, 3, 40);
    ASSERT_TRUE(b) << static_cast<int>(b.result());
    for (uint32_t i = 0; i < 3; ++i) std::memset(b.record(i).data(), 'x' + static_cast<int>(i), 40);
    ASSERT_TRUE(b.commit());
    pump(*impl, cap.fn());
    EXPECT_EQ(cap.frames, 1) << "window full: nothing else leaves";
    for (int i = 0; i < 64 && cap.recs.size() < static_cast<std::size_t>(1 + fillers + 3); ++i) {
        ackNewFrames(*impl, cap, acked);
        pump(*impl, cap.fn());
    }
    ASSERT_EQ(cap.recs.size(), static_cast<std::size_t>(1 + fillers + 3));
    EXPECT_EQ(cap.recs[0].value, "A");
    const std::size_t first = cap.recs.size() - 3;
    for (std::size_t i = 1; i < first; ++i) EXPECT_EQ(cap.recs[i].value, std::string(40, 'F'));
    for (int i = 0; i < 3; ++i) {
        EXPECT_EQ(cap.recs[first + i].value, std::string(40, static_cast<char>('x' + i))) << "claim order and bytes";
        EXPECT_EQ(cap.recs[first + i].frame, cap.recs[first].frame) << "the claimed batch is never split";
        EXPECT_EQ(cap.recs[first + i].offset, cap.recs[first].offset + i) << "contiguous offsets";
    }
}

// #331: batchRecords: a claimBatch that would take an open RecordBatch over batchRecords flushes that batch first
// and lands whole in the next one - in the ring (the drain holds it) and in Direct mode (BackPressured, retried).
TEST(ProducerClaimRegression, ClaimBatchOverBatchRecordsFlushesRegression) {
    ProducerConfig cfg = voidCfg();
    cfg.lingerUs     = 60'000'000;  // only the flush ends a batch
    cfg.batchRecords = 3;
    const Key key(uint64_t{2});
    {   // Ring
        auto* impl = new ProducerImpl(cfg, 64 * 1024);
        Producer p(impl);
        ASSERT_EQ(p.trySend(kTopic, key, bytes("A")), SendResult::Ok);
        ASSERT_EQ(p.trySend(kTopic, key, bytes("B")), SendResult::Ok);
        rc::BatchClaim b = p.claimBatch(kTopic, key, 2, 2);  // 2 + 2 > batchRecords (3)
        ASSERT_TRUE(b);
        fill(b.record(0), "C1");
        fill(b.record(1), "C2");
        ASSERT_TRUE(b.commit());
        p.flush();
        FrameCapture cap;
        pump(*impl, cap.fn());
        ASSERT_EQ(cap.values(), (std::vector<std::string>{"A", "B", "C1", "C2"}));
        EXPECT_EQ(cap.frames, 2);
        EXPECT_EQ(cap.recs[0].frame, cap.recs[1].frame);
        EXPECT_EQ(cap.recs[2].frame, cap.recs[3].frame) << "the claimed batch is whole";
        EXPECT_NE(cap.recs[1].frame, cap.recs[2].frame) << "A,B were flushed before it";
        EXPECT_EQ(cap.recs[2].offset, 0);
        EXPECT_EQ(cap.recs[3].offset, 1);
    }
    {   // Direct
        DirectRig r(cfg);
        ASSERT_EQ(r.p.trySend(kTopic, key, bytes("A")), SendResult::Ok);
        ASSERT_EQ(r.p.trySend(kTopic, key, bytes("B")), SendResult::Ok);
        FrameCapture cap;
        EXPECT_EQ(r.p.claimBatch(kTopic, key, 2, 2).result(), SendResult::BackPressured);
        r.pass(cap.fn());  // the refused claim requested the flush
        ASSERT_EQ(cap.frames, 1);
        rc::BatchClaim b = r.p.claimBatch(kTopic, key, 2, 2);
        ASSERT_TRUE(b) << static_cast<int>(b.result());
        fill(b.record(0), "C1");
        fill(b.record(1), "C2");
        ASSERT_TRUE(b.commit());
        r.p.flush();
        r.pass(cap.fn());
        ASSERT_EQ(cap.values(), (std::vector<std::string>{"A", "B", "C1", "C2"}));
        EXPECT_EQ(cap.frames, 2);
        EXPECT_EQ(cap.recs[2].frame, cap.recs[3].frame);
        EXPECT_NE(cap.recs[1].frame, cap.recs[2].frame);
    }
}

// #331: Invoker mode, a Tier-2 batch queued while a Direct claim is open: nothing leaves (the half-written claim
// is never flushed), and once the claim settles the order is the submission order (A, the claim, then the batch).
TEST(ProducerClaimRegression, Tier2BatchQueuedBehindADirectClaimSendsAfterItSettlesRegression) {
    DirectRig r;
    const Key key(uint64_t{5});
    ASSERT_EQ(r.p.trySend(kTopic, key, bytes("A")), SendResult::Ok);
    rc::Claim c = r.p.tryClaim(kTopic, key, 4);
    ASSERT_TRUE(c) << static_cast<int>(c.result());
    auto t2 = r.p.batch(kTopic, key);
    t2.add(bytes("X"));
    t2.add(bytes("Y"));
    ASSERT_EQ(t2.send(), rc::OpResult::Accepted);
    FrameCapture cap;
    r.pass(cap.fn());
    EXPECT_EQ(cap.frames, 0) << "an open claim is never flushed half-written, and nothing overtakes it";
    fill(c.buffer(), "CCCC");
    ASSERT_TRUE(c.commit());
    for (int i = 0; i < 8; ++i) r.pass(cap.fn());
    EXPECT_EQ(cap.values(), (std::vector<std::string>{"A", "CCCC", "X", "Y"}));
}

// #331: closeImmediately() from inside an ack handler (the pendingFail_ path): the walk that fired the handler
// finishes first, then every still-pending acked record - sent-unacked, queued and a committed claim - fails
// exactly once, an open claim is abandoned, and nothing more leaves.
TEST(ProducerClaimRegression, CloseImmediatelyFromAHandlerFailsPendingClaimsRegression) {
    ProducerConfig cfg = voidCfg();
    cfg.acks        = Acks::Leader;
    cfg.maxInFlight = 1;
    DirectRig r(cfg);
    test_support::AckLedger L;
    struct Ctx {
        Producer*                p = nullptr;
        test_support::AckLedger* L = nullptr;
        int                      calls = 0;
    } ctx{&r.p, &L};
    r.p.onAck(
        [](void* c, rc::Token t, int64_t v, rc::OpStatus st, uint16_t ec) noexcept {
            auto* x = static_cast<Ctx*>(c);
            test_support::AckLedger::onAck(x->L, t, v, st, ec);
            if (x->calls++ == 0) (void)x->p->closeImmediately();  // the first completion closes the producer
        },
        &ctx);
    const Key key(uint64_t{1});
    FrameCapture cap;
    r.pass(cap.fn());
    ASSERT_EQ(r.p.trySend(kTopic, key, bytes("A"), rc::AckToken{1}), SendResult::Ok);
    r.pass(cap.fn());
    ASSERT_EQ(cap.frames, 1) << "A is in flight (window 1)";
    ASSERT_EQ(r.p.trySend(kTopic, key, bytes("B"), rc::AckToken{2}), SendResult::Ok);  // queued behind the window
    {
        rc::Claim c3 = r.p.tryClaim(kTopic, key, 1, rc::AckToken{3});
        ASSERT_TRUE(c3);
        c3.buffer()[0] = 'C';
        ASSERT_TRUE(c3.commit());
    }
    rc::Claim open = r.p.tryClaim(kTopic, key, 1, rc::AckToken{4});  // still open when the handler fires
    ASSERT_TRUE(open);
    r.pass(cap.fn());
    ASSERT_EQ(cap.frames, 1);

    r.impl->onProduceResponse(cap.corrs[0], 10, 0);  // A's ack -> handler -> closeImmediately()
    EXPECT_EQ(ctx.calls, 3) << "A (Ok) then B and the committed claim failed by the deferred fail-all";
    EXPECT_EQ(L.times(1), 1);
    EXPECT_EQ(L.last[1].status, rc::OpStatus::Ok);
    for (const rc::Token t : {rc::Token{2}, rc::Token{3}}) {
        EXPECT_EQ(L.times(t), 1) << "token " << t << " completes exactly once";
        EXPECT_NE(L.last[t].status, rc::OpStatus::Ok) << "token " << t;
    }
    EXPECT_EQ(L.times(4), 0) << "an uncommitted claim was never accepted: no completion";
    EXPECT_FALSE(open.commit()) << "abandoned by the close";
    r.pass(cap.fn());
    EXPECT_EQ(cap.frames, 1) << "nothing is sent after the close";
    EXPECT_EQ(r.p.trySend(kTopic, key, bytes("late"), rc::AckToken{5}), SendResult::Closed);
    EXPECT_EQ(L.times(5), 0);
}

// #331: a recycled in-flight buffer (garbage from earlier, larger frames) never reaches the next Direct claim's
// frame: its bytes equal a frame built from scratch by the protocol encoders (header, record framing,
// headerCount - every byte).
TEST(ProducerClaimRegression, DirectClaimAfterARecycledBufferIsByteExactRegression) {
    ProducerConfig cfg = voidCfg();
    cfg.acks        = Acks::Leader;
    cfg.maxInFlight = 2;  // two slots: from the third frame on, the builder is handed a slot's earlier (0xEE-filled) frame
    DirectRig r(cfg);
    const Key key(uint64_t{9});
    FrameCapture cap;
    cap.keepRaw = true;
    std::size_t acked = 0;
    for (int round = 0; round < 8; ++round) {  // big 0xEE frames: the slots' buffers hold garbage once recycled
        rc::Claim c = r.p.tryClaim(kTopic, key, 300, rc::AckToken{static_cast<uint64_t>(round + 1)});
        ASSERT_TRUE(c) << "round " << round;
        std::memset(c.buffer().data(), 0xEE, c.buffer().size());
        ASSERT_TRUE(c.commit());
        r.pass(cap.fn());
        ackNewFrames(*r.impl, cap, acked);
    }
    ASSERT_EQ(acked, 8u);
    rc::Claim last = r.p.tryClaim(kTopic, key, 10, rc::AckToken{99});
    ASSERT_TRUE(last);
    fill(last.buffer(), "0123456789");
    ASSERT_TRUE(last.commit());
    r.pass(cap.fn());
    ASSERT_EQ(cap.frames, 9);
    const std::vector<uint8_t>& got = cap.rawFrames.back();
    const auto                  v   = rc::codec::decodeProduce(got.data(), got.size());
    ASSERT_TRUE(v.has_value());
    rune::protocol::RecordBatchReader rd(v->recordBatch.data(), v->recordBatch.size());
    ASSERT_EQ(rd.recordCount(), 1);
    // The expected frame, from scratch: the same record through a fresh builder + the decoded Produce header.
    rune::protocol::client::Produce hdr;
    hdr.sessionId      = v->sessionId;
    hdr.correlationId  = v->correlationId;
    hdr.topicId        = v->topicId;
    hdr.partition      = v->partition;
    hdr.partitionKnown = v->partitionKnown;
    hdr.acks           = v->acks;
    hdr.producerId     = v->producerId;
    hdr.producerEpoch  = v->producerEpoch;
    hdr.baseSequence   = v->baseSequence;
    rc::producer::ProduceFrameBuilder fresh(0, rd.baseTimestamp());
    const auto kb = key.bytes();
    fresh.append(std::string_view(reinterpret_cast<const char*>(kb.data()), kb.size()), "0123456789",
                 rd.baseTimestamp());
    fresh.setProducerId(v->producerId);
    fresh.setProducerEpoch(v->producerEpoch);
    fresh.setBaseSequence(v->baseSequence);
    const std::size_t len = fresh.seal(hdr);
    ASSERT_EQ(len, got.size());
    rc::transport::sealFrame(fresh.data(), len, rc::producer::ProduceFrameBuilder::frameCrc());  // the accepted offer's CRC
    std::size_t firstDiff = len;
    for (std::size_t i = 0; i < len; ++i) {
        if (fresh.data()[i] != got[i]) { firstDiff = i; break; }
    }
    EXPECT_EQ(firstDiff, len) << "the claim frame is byte-exact (first differing byte, of " << len
                              << "; batch starts at " << rc::producer::ProduceFrameBuilder::kBatchStart
                              << ", records at " << rc::producer::ProduceFrameBuilder::kRecordsStart
                              << "; got=" << static_cast<int>(firstDiff < len ? got[firstDiff] : 0)
                              << " want=" << static_cast<int>(firstDiff < len ? fresh.data()[firstDiff] : 0) << ")";
}

// #331: the close path (begin-close -> abandonOpenClaimForClose, as Producer::close() / Client::close() do): the
// caller keeps writing its span AFTER the close was issued; the drain delivers the records before the claim and no
// offered frame ever contains the written pattern.
TEST(ProducerClaimRegression, WritingAnAbandonedClaimSpanAfterCloseNeverReachesAFrameRegression) {
    ProducerConfig cfg = voidCfg();
    cfg.acks = Acks::Leader;
    DirectRig r(cfg);
    const Key key(uint64_t{1});
    ASSERT_EQ(r.p.trySend(kTopic, key, bytes("A"), rc::AckToken{1}), SendResult::Ok);
    rc::Claim c = r.p.tryClaim(kTopic, key, 64);
    ASSERT_TRUE(c);
    CloseTokenLog closeLog;
    ASSERT_EQ(r.impl->beginClose(rc::CompletionTarget(closeLog.handler()), 9), rc::OpResult::Accepted);
    r.impl->abandonOpenClaimForClose();  // what Producer::close() does once the close is issued
    ASSERT_EQ(r.impl->claimsAbandoned(), 1u);
    std::memset(c.buffer().data(), 0xAB, c.buffer().size());  // written after the abandon

    FrameCapture cap;
    cap.keepRaw = true;
    std::size_t acked = 0;
    for (int i = 0; i < 16 && closeLog.got.empty(); ++i) {
        r.pass(cap.fn());
        ackNewFrames(*r.impl, cap, acked);
        if (!r.impl->lifeOpen()) (void)r.impl->closeDuty(kNow);  // the hub's drain check
    }
    std::memset(c.buffer().data(), 0xAB, c.buffer().size());  // and again while the drain is finishing
    r.pass(cap.fn());
    EXPECT_EQ(cap.values(), std::vector<std::string>{"A"}) << "the record before the claim drains";
    EXPECT_FALSE(containsRun(cap.rawFrames, 0xAB, 8)) << "the abandoned span never reaches a frame";
    ASSERT_EQ(closeLog.got.size(), 1u);
    EXPECT_EQ(closeLog.got[0].status, rc::OpStatus::Ok);
    EXPECT_FALSE(c.commit());
}
