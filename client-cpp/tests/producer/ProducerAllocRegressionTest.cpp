// tests/producer/ProducerAllocRegressionTest.cpp
#include <filesystem>
#include "../support/AllocCounter.h"
#include "../support/FakeCoordinator.h"
#include "gen/position.gen.h"

#include <rune/client/Client.h>
#include <rune/client/Completions.h>
#include <rune/client/Key.h>
#include <rune/client/TypedTopic.h>
#include <rune/client/codec/DataCodec.h>
#include <rune/client/producer/BatchBuilder.h>
#include <rune/client/producer/Producer.h>
#include <rune/client/producer/ProducerHub.h>
#include <rune/client/producer/ProducerImpl.h>
#include <rune/client/producer/ProducerRef.h>
#include <rune/protocol/client/MessageHeader.h>
#include <rune/protocol/client/TxnMessages.h>

#include <gtest/gtest.h>

#include <array>
#include <atomic>
#include <cstdint>
#include <cstring>
#include <string>
#include <unistd.h>
#include <vector>

using rune::client::Acks;
using rune::client::Key;
using rune::client::ProducerConfig;
using rune::client::producer::ProducerImpl;
namespace rcc = rune::client::codec;

namespace {
constexpr int64_t kNow = 1'000'000'000;
const std::vector<uint8_t> kVal(16, 0x11);

ProducerConfig ackCfg(int32_t maxInFlight) {
    ProducerConfig c;
    c.acks = Acks::Leader;
    c.lingerUs = 0;
    c.batchBytes = 64 * 1024;
    c.maxInFlight = maxInFlight;
    c.produceTimeoutMs = 1'000;
    c.deliveryTimeoutMs = 10'000;
    return c;
}

int pump(ProducerImpl& impl, const ProducerImpl::SendFn& sink) {
    int flushed = 0;
    for (int i = 0; i < 100'000; ++i) {
        const int d = impl.drainRing(kNow);
        flushed += impl.flushBatches(kNow, 42, sink);
        if (d == 0) break;
    }
    return flushed;
}
} // namespace

// Regression: every acked batch allocated a std::map node + a std::vector frame
// copy (BatchInfo.wireFrame).  Steady state must allocate nothing.
TEST(ProducerAllocRegression, SteadyStateAckedFlushAllocatesNothingRegression) {
    ProducerImpl impl(ackCfg(4));
    int64_t lastCorr = 0;
    const ProducerImpl::SendFn sink = [&lastCorr](int32_t, int32_t, int32_t, const uint8_t* b,
                                                  std::size_t n) {
        const auto v = rcc::decodeProduce(b, n);
        lastCorr = v ? v->correlationId : 0;
        return true;
    };
    auto cycle = [&](uint64_t k) {
        (void)impl.submitSendRecord(1, 0, Key(k), kVal, false, 0);
        pump(impl, sink);
        impl.onProduceResponse(lastCorr, static_cast<int64_t>(k), 0);
    };
    for (uint64_t k = 0; k < 64; ++k) cycle(k);   // warm-up: builder, ring, frame capacity
    alloc_counter::start();
    for (uint64_t k = 64; k < 1064; ++k) cycle(k);
    const long allocs = alloc_counter::stop();
    EXPECT_EQ(allocs, 0L) << "acked produce steady state must not allocate";
    EXPECT_FALSE(impl.hasInFlight());
}

// Regression: the in-flight slot kept its wire frame with vector::assign, whose
// capacity is the EXACT size of the first frame; a frame a few bytes larger (a
// wider timestamp-delta varint as the wall clock moves, or a slightly longer
// value) reallocated in steady state — the intermittent 1-allocation failure of
// SteadyStateAckedFlushAllocatesNothingRegression.  Deterministic here: sizes jitter by 2 bytes.
TEST(ProducerAllocRegression, FrameSizeJitterDoesNotReallocateSlotRegression) {
    ProducerImpl impl(ackCfg(1));  // one slot: every frame reuses it
    int64_t lastCorr = 0;
    const ProducerImpl::SendFn sink = [&lastCorr](int32_t, int32_t, int32_t, const uint8_t* b,
                                                  std::size_t n) {
        const auto v = rcc::decodeProduce(b, n);
        lastCorr = v ? v->correlationId : 0;
        return true;
    };
    const std::vector<uint8_t> small(16, 0x11), large(18, 0x22);
    auto cycle = [&](uint64_t k, const std::vector<uint8_t>& v) {
        (void)impl.submitSendRecord(1, 0, Key(k), v, false, 0);
        pump(impl, sink);
        impl.onProduceResponse(lastCorr, static_cast<int64_t>(k), 0);
    };
    for (uint64_t k = 0; k < 64; ++k) cycle(k, small);  // warm-up at the SMALL size only
    alloc_counter::start();
    for (uint64_t k = 64; k < 1064; ++k) cycle(k, (k & 1) ? large : small);
    const long allocs = alloc_counter::stop();
    EXPECT_EQ(allocs, 0L) << "a slightly larger frame must not reallocate the slot's frame";
    EXPECT_FALSE(impl.hasInFlight());
}

// Regression guard: acks=None void sends (no ack handle, no in-flight slot) must
// not allocate in steady state either.
TEST(ProducerAllocRegression, AcksNoneVoidSteadyStateAllocatesNothingRegression) {
    ProducerConfig cfg = ackCfg(4);
    cfg.acks = Acks::None;
    ProducerImpl impl(cfg);
    const ProducerImpl::SendFn sink = [](int32_t, int32_t, int32_t, const uint8_t*, std::size_t) {
        return true;
    };
    auto cycle = [&](uint64_t k) {
        (void)impl.submitSendRecord(1, 0, Key(k), kVal, false, 0);
        pump(impl, sink);
    };
    for (uint64_t k = 0; k < 64; ++k) cycle(k);
    alloc_counter::start();
    for (uint64_t k = 64; k < 1064; ++k) cycle(k);
    const long allocs = alloc_counter::stop();
    EXPECT_EQ(allocs, 0L) << "acks=None void steady state must not allocate";
    EXPECT_FALSE(impl.hasInFlight());
}

// Phase 2b F Task 13 (seeded txn stress): a claim reserves room for its record first
// (ProduceFrameBuilder::reserveExtra) - an EXACT vector::reserve, so while a builder grew past its high-water
// mark (a partition held for its AddPartitionsToTxn, or a session down) every claim reallocated and copied the
// whole builder: O(records) allocations instead of amortized geometric growth.  1000 claim-style appends into one
// builder must now allocate O(log n) times.
TEST(ProduceFrameBuilderAllocRegression, ReserveExtraGrowsGeometricallyNotOncePerClaimRegression) {
    rune::client::producer::ProduceFrameBuilder b(0, kNow);
    const std::string key = "k";
    constexpr int kRecords = 1000;
    alloc_counter::startThisThread();
    for (int i = 0; i < kRecords; ++i) {
        b.reserveExtra(key.size() + 24 + rune::client::producer::ProduceFrameBuilder::kRecordOverheadMax);
        (void)b.reserveRecord(key, 24, kNow);
    }
    const long allocs = alloc_counter::stopThisThread();
    EXPECT_EQ(b.recordCount(), kRecords);
    EXPECT_LE(allocs, 16) << "reserveExtra reallocated " << allocs << " times for " << kRecords << " claims";
}

// ============================================================================================================
// Phase 2b F Task 14 (Review Focus #6): steady-state transactions allocate nothing.  The deleted
// TransactionalProducerImpl allocated per record (a heap std::vector value in a std::deque, a frame vector per
// batch) and per transaction; the unified producer must allocate nothing on ANY path inside a transaction.
//
// One transactional producer behind its Producer handle (Thread-mode ring ingress: every record and every control
// op crosses the producer ring) wired through a ProducerHub to an at-once coordinator / broker.  The test thread
// plays both sides, strictly alternating: the APP side issues a transaction - beginTransaction, 10 records over
// every produce path (send, sendTo, trySend(cq), trySend(AckToken), batch() Tier-2, tryClaim, claimBatch(2),
// TypedTopic::send, TypedTopic::claim), one sendOffsetsToTransaction, commitTransaction - then the DUTY side
// drains, flushes, answers every coordinator request and Produce frame OK, until the commit completes in the
// Completions queue.  100 warm-up transactions, then 1000 counted ones: the window counts every operator new on
// this thread, so it covers the app-thread and the duty-thread work alike.  No threads, no sleeps.
// Every transaction has the same shape (the same 10 keys, so the same records per partition): a buffer's capacity
// persists at its high-water mark (FD4), and a workload whose per-partition batch keeps growing would still be
// warming up - the steady state is the repeated workload.
// ============================================================================================================
namespace {
namespace rp  = rune::client::producer;
namespace rpc = rune::protocol::client;
namespace rc  = rune::client;

// The producer's per-record CQ completions arrive through Hooks::cqDeliver with hooks.ctx = the Client (the
// identity Completions::client() is checked against); the rig's one queue is reached through this pointer, set
// while the rig lives (single test thread).
rc::Completions* g_txnCq = nullptr;
void txnDeliverRecord(void*, rc::CqRef, const rc::Completion& c) noexcept { (void)g_txnCq->ring().deliver(c, {}); }
void txnDeliverOp(void*, const rc::detail::OpTarget& t, const rc::Completion& c,
                  std::span<const uint8_t> payload) noexcept {
    rc::Completion done = c;
    if (t.handler.fn != nullptr) {
        done.payloadLen = static_cast<uint32_t>(payload.size());
        t.handler(done, payload);
        return;
    }
    done.reservedBytes = t.reservedBytes;
    (void)g_txnCq->ring().deliver(done, payload);
}

rc::ClientConfig txnAllocClientCfg() {
    rc::ClientConfig c;
    c.gatewayChannel  = "aeron:ipc";
    c.gatewayStreamId = 4721;
    c.egressChannel   = "aeron:ipc";
    c.egressStreamId  = 4722;
    c.dataDir         = std::filesystem::temp_directory_path().string() + "/txn_alloc_regression_" + std::to_string(::getpid());
    c.runnerMode      = rc::RunnerMode::Invoker;
    return c;
}
rc::CompletionsConfig txnAllocCqCfg() {
    rc::CompletionsConfig c;
    c.capacityBytes  = 1u << 18;
    c.maxResultBytes = 64;
    c.maxErrorText   = 256;
    return c;
}
ProducerConfig txnAllocCfg() {
    ProducerConfig c;
    c.acks                   = Acks::All;
    c.lingerUs               = 0;
    c.batchBytes             = 64 * 1024;
    c.maxInFlight            = 5;
    c.produceTimeoutMs       = 600'000;
    c.deliveryTimeoutMs      = 600'000;
    c.transactionalId        = "txn-alloc";
    c.transactionOpTimeoutMs = 600'000;
    c.transactionTimeoutMs   = 600'000;
    return c;
}

struct TxnAllocRig {
    struct Req { int64_t corrId; uint16_t templateId; };
    struct Batch { int64_t corrId; int32_t records; };

    std::unique_ptr<rc::Client>  client = std::make_unique<rc::Client>(txnAllocClientCfg());
    rc::Completions              cq{*client, txnAllocCqCfg()};
    ProducerImpl*                impl;
    rc::Producer                 p;
    rc::TypedTopic<rune::gen::Position, rc::ProducerRef> typed;
    rp::ProducerHub              hub;
    ProducerImpl::SendFn         sink;
    std::array<Req, 64>          reqs{};
    std::size_t                  nReqs = 0;
    std::array<Batch, 256>       batches{};
    std::size_t                  nBatches = 0;
    int64_t                      offset = 0;
    // Completions seen (the app side's poll; the onAck handler runs on the duty side).
    int64_t                      cqRecords = 0, offsetsOk = 0, commitsOk = 0, opFailures = 0, recordFailures = 0;
    int64_t                      handlerAcks = 0;
    rc::Token                    token = 0;

    static constexpr rc::TopicHandle kTopic{3, 4};

    TxnAllocRig()
        : impl(new ProducerImpl(txnAllocCfg(), 1 << 20)),
          p((wire(impl, client.get()), impl)),
          typed(rc::ProducerRef{p}, kTopic, uint16_t{1}) {
        g_txnCq = &cq;
        hub.setCoordinatorSink(rp::CoordinatorSink{&TxnAllocRig::offer, &TxnAllocRig::session, this,
                                                   &TxnAllocRig::rediscover});
        hub.add(impl);
        sink = [this](int32_t, int32_t, int32_t, const uint8_t* b, std::size_t n) {
            const auto h = rpc::MessageHeader::decode(b, n);
            const auto v = rcc::decodeProduce(b, n);
            if (!h || !v || v->recordBatch.size() < 61 || nBatches == batches.size()) return false;
            int32_t recs = 0;
            std::memcpy(&recs, v->recordBatch.data() + 57, sizeof recs);   // RecordBatch.recordCount
            batches[nBatches++] = Batch{h->correlationId, recs};
            return true;
        };
        p.onAck([](void* ctx, rc::Token, int64_t, rc::OpStatus st, uint16_t) noexcept {
            auto* self = static_cast<TxnAllocRig*>(ctx);
            ++self->handlerAcks;
            if (st != rc::OpStatus::Ok) ++self->recordFailures;
        }, this);
    }
    ~TxnAllocRig() {
        hub.remove(impl);
        g_txnCq = nullptr;
    }
    static void wire(ProducerImpl* i, rc::Client* c) {
        ProducerImpl::Hooks h;
        h.ctx       = c;
        h.cqDeliver = &txnDeliverRecord;
        h.sink      = rc::detail::CompletionSink{&txnDeliverOp, nullptr};
        i->setHooks(h);
    }
    static rp::CoordinatorSink::Offer offer(void* ctx, const uint8_t* b, std::size_t n) noexcept {
        auto* self = static_cast<TxnAllocRig*>(ctx);
        if (self->nReqs == self->reqs.size()) return rp::CoordinatorSink::Offer::Retry;
        const auto h = rpc::MessageHeader::decode(b, n);
        if (h) self->reqs[self->nReqs++] = Req{h->correlationId, h->templateId};
        return rp::CoordinatorSink::Offer::Accepted;
    }
    static int64_t session(void*) noexcept { return 77; }
    static void    rediscover(void*) noexcept {}

    static int64_t nowNs() {
        return std::chrono::duration_cast<std::chrono::nanoseconds>(
                   std::chrono::steady_clock::now().time_since_epoch()).count();
    }
    void poll() {
        (void)cq.poll([this](const rc::Completion& c, std::span<const uint8_t>) {
            switch (c.op) {
            case rc::OpKind::Send:
                ++cqRecords;
                if (c.status != rc::OpStatus::Ok) ++recordFailures;
                break;
            case rc::OpKind::SendOffsets:
                c.status == rc::OpStatus::Ok ? ++offsetsOk : ++opFailures;
                break;
            case rc::OpKind::CommitTransaction:
            case rc::OpKind::InitTransactions:
                c.status == rc::OpStatus::Ok ? ++commitsOk : ++opFailures;
                break;
            default:
                ++opFailures;
                break;
            }
        });
    }
    /// One duty pass: drain + flush, then every coordinator request and Produce frame answered OK.
    void duty() {
        const int64_t now = nowNs();
        hub.drainAndFlush(now, 42, sink);
        for (std::size_t i = 0; i < nReqs; ++i) {
            rp::ReqKind kind = rp::ReqKind::InitProducerId;
            switch (reqs[i].templateId) {
            case rpc::AddPartitionsToTxn::TEMPLATE_ID: kind = rp::ReqKind::AddPartitions; break;
            case rpc::AddOffsetsToTxn::TEMPLATE_ID:    kind = rp::ReqKind::AddOffsets; break;
            case rpc::TxnOffsetCommit::TEMPLATE_ID:    kind = rp::ReqKind::TxnOffsetCommit; break;
            case rpc::EndTxn::TEMPLATE_ID:             kind = rp::ReqKind::EndTxn; break;
            default: break;
            }
            const auto a = test_support::FakeCoordinator::answer(kind, reqs[i].corrId, 0);
            (void)hub.onCoordinatorResponse(a.templateId, a.data(), a.len, now);
        }
        nReqs = 0;
        for (std::size_t i = 0; i < nBatches; ++i) {
            hub.onProduceResponse(batches[i].corrId, offset, 0);
            offset += batches[i].records;
        }
        nBatches = 0;
    }
    /// Duty passes until `commitsOk` reaches `want` (bounded: a lost completion fails, never hangs).
    bool runUntil(int64_t want) {
        for (int i = 0; i < 1000 && commitsOk < want; ++i) {
            duty();
            poll();
        }
        return commitsOk >= want;
    }
    /// One transaction: 10 records over every produce path + one sendOffsetsToTransaction, then the commit.
    bool transaction(int64_t k, std::span<const rc::OffsetEntry> offsets) {
        const std::array<uint8_t, 16> v{};
        const std::span<const uint8_t> val(v.data(), v.size());
        const int64_t want = commitsOk + 1;
        p.beginTransaction();
        p.send(kTopic, Key(uint64_t{0}), val);                                              // 1  send
        p.sendTo(kTopic, 1, Key(uint64_t{1}), val);                                         // 2  sendTo
        if (p.trySend(kTopic, Key(uint64_t{2}), val, cq, ++token) != rc::SendResult::Ok) return false;    // 3
        if (p.trySend(kTopic, Key(uint64_t{3}), val, rc::AckToken{++token}) != rc::SendResult::Ok) return false;  // 4
        {
            rc::BatchBuilder b = p.batch(kTopic, Key(uint64_t{4}));                         // 5  Tier-2 batch
            b.add(val);
            if (b.send(rc::AckToken{++token}) != rc::OpResult::Accepted) return false;
        }
        {
            rc::Claim c = p.tryClaim(kTopic, Key(uint64_t{5}), 16);                         // 6  tryClaim
            if (!c) return false;
            std::memset(c.buffer().data(), 1, 16);
            if (!c.commit()) return false;
        }
        {
            rc::BatchClaim c = p.claimBatch(kTopic, Key(uint64_t{6}), 2, 16);               // 7-8 claimBatch(2)
            if (!c) return false;
            std::memset(c.record(0).data(), 2, 16);
            std::memset(c.record(1).data(), 3, 16);
            if (!c.commit()) return false;
        }
        rune::gen::Position rec;                                                             // 9  typed send
        rec.userId = 8;
        rec.zone   = int32_t(k & 7);
        rec.seq    = uint32_t(k);
        typed.send(rec);
        {
            auto w = typed.claim(int64_t{9});                                               // 10 typed claim
            if (!w) return false;
            w.zone(1).x(1.5).seq(uint32_t(k));
            if (!w.commit()) return false;
        }
        if (p.sendOffsetsToTransaction(offsets, "grp-alloc", cq, ++token) != rc::OpResult::Accepted) return false;
        if (p.commitTransaction(cq, ++token) != rc::OpResult::Accepted) return false;
        return runUntil(want);
    }
};
}  // namespace

TEST(ProducerTxnAllocRegression, SteadyStateTransactionsAllocateNothingRegression) {
    TxnAllocRig r;
    ASSERT_EQ(r.p.initTransactions(r.cq, ++r.token), rc::OpResult::Accepted);
    ASSERT_TRUE(r.runUntil(1)) << "initTransactions never completed Ok";
    const std::array<rc::OffsetEntry, 4> offs{rc::OffsetEntry{9, 0, 100}, rc::OffsetEntry{9, 1, 200},
                                              rc::OffsetEntry{9, 2, 300}, rc::OffsetEntry{9, 3, 400}};
    constexpr int kWarm = 100, kCounted = 1000;
    for (int k = 0; k < kWarm; ++k) ASSERT_TRUE(r.transaction(k, offs)) << "warm-up transaction " << k;
    alloc_counter::startThisThread();
    bool ok = true;
    int  failedAt = -1;
    for (int k = kWarm; k < kWarm + kCounted && ok; ++k) {
        ok = r.transaction(k, offs);
        if (!ok) failedAt = k;
    }
    const long allocs = alloc_counter::stopThisThread();
    ASSERT_TRUE(ok) << "transaction " << failedAt << " was refused or never committed";
    EXPECT_EQ(allocs, 0L) << "steady-state transactions allocated " << allocs << " times over " << kCounted
                          << " transactions (app + duty side)";
    // Every op completed exactly once, Ok: init + every commit, every sendOffsets, every acked record.
    EXPECT_EQ(r.commitsOk, 1 + kWarm + kCounted);
    EXPECT_EQ(r.offsetsOk, kWarm + kCounted);
    EXPECT_EQ(r.cqRecords, kWarm + kCounted);
    EXPECT_EQ(r.handlerAcks, 2 * (kWarm + kCounted));   // trySend(AckToken) + the Tier-2 batch's AckToken
    EXPECT_EQ(r.opFailures, 0);
    EXPECT_EQ(r.recordFailures, 0);
    EXPECT_EQ(r.offset, int64_t{10} * (kWarm + kCounted)) << "every record of every transaction was acknowledged";
}

namespace {
// Make a heap pointer escape so the compiler cannot elide the new-expression.
// [expr.new] lets it omit a replaceable allocation whose result never escapes:
// at -O1 (the TSan build) clang folded `new Wide{}` + `delete w` away entirely,
// so the counter saw 0 allocations although the align_val_t overload is wired.
inline void escape(void* p) { asm volatile("" : : "g"(p) : "memory"); }
}  // namespace

// The zero-alloc gates must see over-aligned allocations too: they use the
// align_val_t overloads, which bypass the plain replaced operator new.
// Fix (TSan triage): the allocations below escape, so an optimizing build
// (-O1 TSan) still performs them inside the counting window.
TEST(AllocCounterRegression, OverAlignedNewIsCountedRegression) {
    struct alignas(256) Wide { unsigned char b[256]; };
    alloc_counter::startThisThread();
    auto* w = new Wide{};
    escape(w);
    const long n = alloc_counter::stopThisThread();
    EXPECT_EQ(reinterpret_cast<std::uintptr_t>(w) % 256u, 0u);
    delete w;
    EXPECT_EQ(n, 1);

    alloc_counter::start();
    auto* arr = new Wide[3];
    escape(arr);
    const long g = alloc_counter::stop();
    delete[] arr;
    EXPECT_EQ(g, 1);
}
