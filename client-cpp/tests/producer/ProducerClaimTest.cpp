// tests/producer/ProducerClaimTest.cpp — Phase 2b Part B: zero-copy produce
// (Producer::tryClaim / claimBatch) on a bare ProducerImpl, Thread (Ring) and
// Invoker (Direct) ingress: commit / abort / destructor-abort, back-pressure,
// limits, ordering vs sends and Tier-2 batches, the copy-count proofs, and the
// zero-allocation gates.  Every drive loop is iteration-capped; nothing sleeps.
#include "../support/AckLedger.h"
#include "../support/AllocCounter.h"
#include "../support/FrameCapture.h"

#include <rune/client/Key.h>
#include <rune/client/TopicHandle.h>
#include <rune/client/producer/BatchBuilder.h>
#include <rune/client/producer/Producer.h>
#include <rune/client/producer/ProducerImpl.h>

#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

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
