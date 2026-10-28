// tests/producer/ProducerIdTest.cpp - Phase 2b F Task 4 (FD4): the producer identity machine.
// IdUnassigned -> IdRequested -> IdAssigned | IdFailed on a bare ProducerImpl wired to a FakeCoordinator
// through a ProducerHub.  No threads, no sleeps, no atomics: the clock is the nowNs each tick is handed.
#include "../support/AckLedger.h"
#include "../support/FakeCoordinator.h"

#include <rune/client/Completion.h>
#include <rune/client/Key.h>
#include <rune/client/producer/BatchBuilder.h>
#include <rune/client/codec/DataCodec.h>
#include <rune/client/producer/ProducerHub.h>
#include <rune/client/producer/ProducerImpl.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/protocol/client/TxnMessages.h>

#include <gtest/gtest.h>

#include <cstdint>
#include <fstream>
#include <memory>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

namespace rc = rune::client;
namespace rp = rune::client::producer;
namespace rpc = rune::protocol::client;
namespace rcc = rune::client::codec;
using rc::Acks;
using rc::Key;
using rc::OpStatus;
using rp::ProducerImpl;
using test_support::AckLedger;
using test_support::AnswerFrame;
using test_support::FakeCoordinator;

namespace {

constexpr int64_t kMs  = 1'000'000;
constexpr int64_t kT0  = 1'000'000'000;
const std::vector<uint8_t> kVal(16, 0x5A);

struct Wire {
    int32_t partition = 0;
    int64_t pid       = 0;
    int16_t epoch     = 0;
    int32_t baseSeq   = 0;
};

rc::ProducerConfig cfgOf(Acks acks) {
    rc::ProducerConfig c;
    c.acks              = acks;
    c.lingerUs          = 0;
    c.batchBytes        = 64 * 1024;
    c.maxInFlight       = 8;
    c.produceTimeoutMs  = 1'000;
    c.deliveryTimeoutMs = 10'000;
    return c;
}

AnswerFrame initAnswer(int64_t corr, int64_t pid, int16_t epoch, int16_t ec = 0) {
    AnswerFrame a;
    rpc::InitProducerIdResponse m;
    m.correlationId = corr;
    m.producerId    = ec == 0 ? pid : -1;
    m.producerEpoch = epoch;
    m.errorCode     = ec;
    a.len        = m.encode(a.buf.data());
    a.templateId = rpc::InitProducerIdResponse::TEMPLATE_ID;
    return a;
}

/// A producer wired to a fake coordinator through a hub (the Client's wiring, minus the transport).
struct Rig {
    FakeCoordinator           fake;
    rp::ProducerHub           hub;
    std::unique_ptr<ProducerImpl> impl;
    std::vector<Wire>         wire;
    ProducerImpl::SendFn      sink;
    int64_t                   now = kT0;

    explicit Rig(rc::ProducerConfig cfg, bool direct = false, bool wired = true) {
        if (wired) hub.setCoordinatorSink(fake.sink());
        impl = std::make_unique<ProducerImpl>(cfg, 1 << 16);
        if (direct) impl->enableDirectIngress({std::this_thread::get_id(), nullptr, nullptr});
        hub.add(impl.get());
        sink = [this](int32_t, int32_t p, int32_t, const uint8_t* b, std::size_t n) {
            const auto v = rcc::decodeProduce(b, n);
            if (!v) return false;
            rune::protocol::RecordBatchReader r(v->recordBatch.data(), v->recordBatch.size());
            wire.push_back({p, r.producerId(), r.producerEpoch(), r.baseSequence()});
            return true;
        };
    }
    ~Rig() { hub.remove(impl.get()); }

    int tick(int64_t advanceNs = 0) {
        now += advanceNs;
        return hub.drainAndFlush(now, 42, sink);
    }
    void ticks(int n, int64_t advanceNs = 0) { for (int i = 0; i < n; ++i) tick(advanceNs); }
    void answer(const AnswerFrame& f) { ASSERT_TRUE(hub.onCoordinatorResponse(f.templateId, f.data(), f.len, now)); }
    int64_t lastCorr() const { return fake.offered.back().corrId; }
    std::size_t initRequests() const {
        std::size_t n = 0;
        for (const auto& f : fake.offered) n += f.templateId == rpc::InitProducerId::TEMPLATE_ID ? 1 : 0;
        return n;
    }
    bool send(int32_t part, uint64_t k, bool ack = false, rc::Token t = 0) {
        const int64_t h = ack ? AckLedger::hi(t) : 0;
        if (impl->ingressMode() == ProducerImpl::IngressMode::Direct) {
            return impl->trySubmit(1, part, Key(k), kVal, h) == rc::SendResult::Ok;
        }
        return impl->submitSendRecord(1, part, Key(k), kVal, ack, h);
    }
};

}  // namespace

TEST(ProducerId, AckedProducerSendsInitProducerIdWithAnEmptyIdOnFirstConnectedPass) {
    Rig r(cfgOf(Acks::Leader));
    r.fake.sid = 0;   // the coordinator session is not up yet
    EXPECT_FALSE(r.impl->identityAssigned());
    EXPECT_EQ(r.impl->producerId(), -1);
    r.ticks(5, kMs);
    EXPECT_TRUE(r.fake.offered.empty()) << "nothing is requested (or offered) without a connected session";
    EXPECT_EQ(r.impl->coordinatorLink()->inUse(), 0u);
    r.fake.sid = 77;
    r.ticks(5, kMs);
    ASSERT_EQ(r.initRequests(), 1u) << "exactly one InitProducerId, however many passes follow";
    const auto req = rpc::InitProducerId::decode(r.fake.offered[0].head.data(), r.fake.offered[0].len);
    ASSERT_TRUE(req.has_value());
    EXPECT_TRUE(req->transactionalId.empty()) << "a plain producer sends an empty transactional id";
    EXPECT_EQ(req->transactionTimeoutMs, 0);
    EXPECT_TRUE(std::holds_alternative<rp::IdRequested>(r.impl->identity()));
}

TEST(ProducerIdRegression, RecordsBeforeTheIdArrivesAreHeldNotSentRegression) {
    Rig r(cfgOf(Acks::Leader));
    for (uint64_t i = 0; i < 100; ++i) ASSERT_TRUE(r.send(static_cast<int32_t>(i % 4), i));
    r.ticks(20, kMs);
    ASSERT_EQ(r.initRequests(), 1u);
    EXPECT_TRUE(r.wire.empty()) << "no Produce frame before the id: a record is never sent with a guessed id";
    // Linger/size triggers, an explicit flush and a closing producer do not release them either.
    r.impl->submitFlush();
    r.ticks(20, 100 * kMs);
    EXPECT_TRUE(r.wire.empty());
    r.answer(initAnswer(r.lastCorr(), 5150, 3));
    ASSERT_TRUE(r.impl->identityAssigned());
    EXPECT_EQ(r.impl->producerId(), 5150);
    EXPECT_EQ(r.impl->producerEpoch(), 3);
    r.ticks(20, kMs);
    ASSERT_EQ(r.wire.size(), 4u) << "one batch per partition (25 records each)";
    for (const Wire& w : r.wire) {
        EXPECT_EQ(w.pid, 5150) << "every batch carries the assigned pid";
        EXPECT_EQ(w.epoch, 3) << "and the assigned epoch";
        EXPECT_EQ(w.baseSeq, 0) << "sequences start at 0 per partition";
    }
}

TEST(ProducerIdRegression, AckedProducerUsesTheBrokerAssignedIdRegression) {
    Rig r(cfgOf(Acks::Leader));
    ASSERT_TRUE(r.send(0, 1));
    r.ticks(3, kMs);
    r.answer(initAnswer(r.lastCorr(), 31337, 0));
    r.ticks(3, kMs);
    ASSERT_EQ(r.wire.size(), 1u);
    EXPECT_EQ(r.wire[0].pid, 31337) << "the pid on the wire is the broker's answer, never a drawn one";
    // grep-level: the client-drawn generator and its TODO are gone from the producer sources.
    const std::string here = __FILE__;
    const std::string root = here.substr(0, here.rfind("/tests/"));
    for (const char* f : {"/src/producer/ProducerImpl.cpp", "/include/rune/client/producer/ProducerImpl.h",
                          "/include/rune/client/producer/Producer.h"}) {
        std::ifstream in(root + f);
        ASSERT_TRUE(in.good()) << f;
        std::stringstream ss;
        ss << in.rdbuf();
        EXPECT_EQ(ss.str().find("generateProducerId("), std::string::npos) << f;
        EXPECT_EQ(ss.str().find("TODO(milestone-c)"), std::string::npos) << f;
    }
}

TEST(ProducerId, AcksNoneNeverAsksForAnId) {
    Rig r(cfgOf(Acks::None));
    for (uint64_t i = 0; i < 8; ++i) ASSERT_TRUE(r.send(static_cast<int32_t>(i % 2), i));
    r.ticks(10, kMs);
    EXPECT_TRUE(r.fake.offered.empty()) << "no InitProducerId";
    ASSERT_FALSE(r.wire.empty()) << "acks=None is never held";
    for (const Wire& w : r.wire) {
        EXPECT_EQ(w.pid, -1);
        EXPECT_EQ(w.epoch, -1);
        EXPECT_EQ(w.baseSeq, -1);
    }
}

TEST(ProducerId, ConcurrentTransactionsAndNotCoordinatorRetryThenAssign) {
    Rig r(cfgOf(Acks::Leader));
    r.ticks(2, kMs);
    ASSERT_EQ(r.initRequests(), 1u);
    const int64_t corr = r.lastCorr();
    // 27 ConcurrentTransactions: the link backs off on the duty clock and re-sends the SAME id.
    r.answer(initAnswer(corr, 0, 0, static_cast<int16_t>(rpc::ErrorCode::ConcurrentTransactions)));
    EXPECT_TRUE(std::holds_alternative<rp::IdRequested>(r.impl->identity())) << "retriable: still requesting";
    r.ticks(3, 0);
    EXPECT_EQ(r.initRequests(), 1u) << "not before the backoff elapsed";
    r.ticks(1, 2 * kMs);
    ASSERT_EQ(r.initRequests(), 2u);
    EXPECT_EQ(r.lastCorr(), corr);
    // 14 NotCoordinator (an ErrorResponse): wait for the coordinator move, then re-send.
    const auto nc = FakeCoordinator::errorResponse(corr, rpc::ErrorCode::NotCoordinator, "127.0.0.1:1");
    ASSERT_TRUE(r.hub.onCoordinatorResponse(nc.templateId, nc.data(), nc.len, r.now));
    r.ticks(3, 10 * kMs);
    EXPECT_EQ(r.initRequests(), 2u) << "held until the coordinator moves";
    r.hub.onCoordinatorMoved();
    r.ticks(1, kMs);
    ASSERT_EQ(r.initRequests(), 3u);
    r.answer(initAnswer(corr, 909, 1));
    EXPECT_TRUE(r.impl->identityAssigned());
    EXPECT_EQ(r.impl->producerId(), 909);
    EXPECT_EQ(r.impl->producerEpoch(), 1);
    EXPECT_EQ(r.impl->coordinatorLink()->inUse(), 0u);
}

TEST(ProducerId, NonRetriableAnswerFailsEveryRecordExplicitly) {
    Rig r(cfgOf(Acks::Leader));
    AckLedger L;
    L.attach(*r.impl);
    L.attachBatches(*r.impl);
    for (rc::Token t = 1; t <= 10; ++t) ASSERT_TRUE(r.send(static_cast<int32_t>(t % 4), t, true, t));
    for (uint64_t i = 0; i < 5; ++i) ASSERT_TRUE(r.send(static_cast<int32_t>(i % 4), 100 + i));   // void sends
    r.ticks(3, kMs);
    ASSERT_EQ(r.initRequests(), 1u);
    r.answer(initAnswer(r.lastCorr(), 0, 0, static_cast<int16_t>(rpc::ErrorCode::InvalidRequest)));   // 39
    ASSERT_TRUE(std::holds_alternative<rp::IdFailed>(r.impl->identity()));
    EXPECT_EQ(std::get<rp::IdFailed>(r.impl->identity()).errorCode, 39);
    r.ticks(3, kMs);
    for (rc::Token t = 1; t <= 10; ++t) {
        ASSERT_EQ(L.times(t), 1) << "token " << t << " completes exactly once";
        EXPECT_EQ(L.status(t), OpStatus::DeliveryFailed);
        EXPECT_EQ(L.last.at(t).ec, 39);
    }
    EXPECT_EQ(L.batchAckedRecords(), 15) << "the void sends are counted too";
    for (const auto& a : L.batchAcks) EXPECT_EQ(a.errorCode, rc::BatchAck::kClientFailure);
    // A record accepted AFTER the failure completes the same way.
    ASSERT_TRUE(r.send(1, 500, true, 11));
    ASSERT_TRUE(r.send(2, 501));
    r.ticks(3, kMs);
    ASSERT_EQ(L.times(11), 1);
    EXPECT_EQ(L.status(11), OpStatus::DeliveryFailed);
    EXPECT_EQ(L.last.at(11).ec, 39);
    EXPECT_EQ(L.batchAckedRecords(), 17);
    EXPECT_TRUE(r.wire.empty()) << "nothing was ever sent";
    EXPECT_EQ(r.impl->producerId(), -1);
}

TEST(ProducerId, EpochOverflowRequestsANewId) {
    Rig r(cfgOf(Acks::Leader));
    r.ticks(2, kMs);
    r.answer(initAnswer(r.lastCorr(), 100, 0));
    r.impl->debugSetEpochForTest(INT16_MAX);
    AckLedger L;
    L.attach(*r.impl);
    ASSERT_TRUE(r.send(0, 1, true, 1));
    r.ticks(3, kMs);
    ASSERT_EQ(r.wire.size(), 1u);
    EXPECT_EQ(r.wire[0].pid, 100);
    EXPECT_EQ(r.wire[0].epoch, INT16_MAX);
    // The delivery timeout fails the batch with its outcome unknown: the partition needs a new identity,
    // the epoch space is exhausted -> a cold InitProducerId, not id + 1.
    const std::size_t before = r.initRequests();
    EXPECT_EQ(before, 1u);
    r.ticks(1, 11'000 * kMs);
    r.impl->retransmitStalled(r.now, r.sink);
    EXPECT_FALSE(r.impl->identityAssigned());
    r.ticks(2, kMs);
    ASSERT_EQ(r.initRequests(), 2u) << "a second InitProducerId";
    ASSERT_TRUE(r.send(0, 2, true, 2));
    r.ticks(3, kMs);
    EXPECT_EQ(r.wire.size(), 1u) << "held while the new id is pending";
    r.answer(initAnswer(r.lastCorr(), 200, 0));
    r.ticks(3, kMs);
    ASSERT_EQ(r.wire.size(), 2u);
    EXPECT_EQ(r.wire[1].pid, 200) << "the new pid is used afterwards";
    EXPECT_EQ(r.wire[1].epoch, 0);
    EXPECT_EQ(r.wire[1].baseSeq, 0);
}

TEST(ProducerId, BareRigStartsAssignedWithTheRigId) {
    ProducerImpl impl(cfgOf(Acks::Leader));
    EXPECT_TRUE(impl.identityAssigned());
    EXPECT_EQ(impl.producerId(), rp::kRigProducerId);
    EXPECT_EQ(impl.producerEpoch(), 0);
    EXPECT_FALSE(impl.hasCoordinator());
    // Flushing needs no hub, no link, no coordinator.
    std::vector<Wire> wire;
    const ProducerImpl::SendFn sink = [&](int32_t, int32_t p, int32_t, const uint8_t* b, std::size_t n) {
        const auto v = rcc::decodeProduce(b, n);
        if (!v) return false;
        rune::protocol::RecordBatchReader rd(v->recordBatch.data(), v->recordBatch.size());
        wire.push_back({p, rd.producerId(), rd.producerEpoch(), rd.baseSequence()});
        return true;
    };
    ASSERT_TRUE(impl.submitSendRecord(1, 0, Key(uint64_t{1}), kVal, false, 0));
    impl.drainRing(kT0);
    impl.flushBatches(kT0, 42, sink);
    ASSERT_EQ(wire.size(), 1u);
    EXPECT_EQ(wire[0].pid, rp::kRigProducerId);
    EXPECT_EQ(wire[0].epoch, 0);
    // The handle-side snapshot (FD9) reports it too.
    EXPECT_EQ(impl.appProducerId(), rp::kRigProducerId);
    EXPECT_EQ(impl.appProducerEpoch(), 0);
    // A transactional bare rig has no id until initTransactions (Task 5).
    rc::ProducerConfig tc = cfgOf(Acks::All);
    tc.transactionalId = "tx";
    ProducerImpl txn(tc);
    EXPECT_FALSE(txn.identityAssigned());
    EXPECT_EQ(txn.producerId(), -1);
}

TEST(ProducerIdRegression, BareRigEpochOverflowRotatesLocallyRegression) {
    // No link, no broker: a rig rotates deterministically (never blocks); a wired producer asks the broker.
    ProducerImpl impl(cfgOf(Acks::Leader));
    std::vector<Wire> wire;
    const ProducerImpl::SendFn sink = [&](int32_t, int32_t p, int32_t, const uint8_t* b, std::size_t n) {
        const auto v = rcc::decodeProduce(b, n);
        if (!v) return false;
        rune::protocol::RecordBatchReader rd(v->recordBatch.data(), v->recordBatch.size());
        wire.push_back({p, rd.producerId(), rd.producerEpoch(), rd.baseSequence()});
        return true;
    };
    impl.debugSetEpochForTest(INT16_MAX);
    ASSERT_TRUE(impl.submitSendRecord(1, 0, Key(uint64_t{1}), kVal, false, 0));
    impl.drainRing(kT0);
    impl.flushBatches(kT0, 42, sink);
    ASSERT_EQ(wire.size(), 1u);
    impl.retransmitStalled(kT0 + 11'000 * kMs, sink);
    EXPECT_TRUE(impl.identityAssigned());
    EXPECT_EQ(impl.producerEpoch(), 0);
    ASSERT_TRUE(impl.submitSendRecord(1, 0, Key(uint64_t{2}), kVal, false, 0));
    impl.drainRing(kT0 + 11'000 * kMs);
    impl.flushBatches(kT0 + 11'000 * kMs, 42, sink);
    ASSERT_EQ(wire.size(), 2u);
    EXPECT_EQ(wire[1].epoch, 0);
    EXPECT_EQ(wire[1].baseSeq, 0);
}

TEST(ProducerIdRegression, ClaimsAreHeldUntilTheIdArrivesRegression) {
    for (const bool direct : {false, true}) {
        SCOPED_TRACE(direct ? "Direct" : "Ring");
        Rig r(cfgOf(Acks::Leader), direct);
        const Key key(uint64_t{9});
        ProducerImpl::ClaimGrant g = r.impl->claimRecord(1, 0, key, 16, 0);
        ASSERT_EQ(g.result, rc::SendResult::Ok);
        std::fill(g.base, g.base + 16, uint8_t{0x77});
        ASSERT_TRUE(r.impl->commitClaim(g.token));
        ProducerImpl::ClaimGrant b = r.impl->claimBatch(1, 1, key, 4, 16);
        ASSERT_EQ(b.result, rc::SendResult::Ok);
        ASSERT_TRUE(r.impl->commitClaim(b.token));
        r.ticks(10, kMs);
        ASSERT_EQ(r.initRequests(), 1u);
        EXPECT_TRUE(r.wire.empty()) << "a committed claim waits for the id like any record";
        r.answer(initAnswer(r.lastCorr(), 4711, 2));
        r.ticks(10, kMs);
        ASSERT_EQ(r.wire.size(), 2u);
        for (const Wire& w : r.wire) {
            EXPECT_EQ(w.pid, 4711);
            EXPECT_EQ(w.epoch, 2);
            EXPECT_EQ(w.baseSeq, 0);
        }
    }
}

TEST(ProducerIdRegression, ALostAnswerIsRequestedAgainAfterTheDeadlineRegression) {
    Rig r(cfgOf(Acks::Leader));   // produceTimeoutMs 1000: the request's deadline
    ASSERT_TRUE(r.send(0, 1));
    r.ticks(2, kMs);
    ASSERT_EQ(r.initRequests(), 1u);
    const int64_t first = r.lastCorr();
    r.ticks(1, 1'500 * kMs);   // no answer: the slot expires ...
    r.ticks(2, kMs);           // ... and the machine asks again
    ASSERT_GE(r.initRequests(), 2u);
    EXPECT_NE(r.lastCorr(), first) << "a fresh request id";
    EXPECT_TRUE(r.wire.empty());
    r.answer(initAnswer(first, 1, 0));   // the late answer to the expired request finds no slot
    EXPECT_FALSE(r.impl->identityAssigned());
    r.answer(initAnswer(r.lastCorr(), 77, 0));
    EXPECT_TRUE(r.impl->identityAssigned());
}

TEST(ProducerIdRegression, StatusSnapshotsAreNeverLostWhenTheRingIsFullRegression) {
    Rig r(cfgOf(Acks::Leader));
    // Many transitions (each lost answer = a new request) with the app never draining: the status ring fills
    // and refuses publishes.  The ring is 4096 bytes of 24-byte records (~170 snapshots, Task 5): more rounds
    // than that (the precondition below proves the ring really filled).
    constexpr int kRounds = 4096 / 24 + 32;
    for (int round = 0; round < kRounds; ++round) {
        r.ticks(1, 1'500 * kMs);
        r.ticks(2, kMs);
    }
    ASSERT_GE(r.initRequests(), 10u);
    r.answer(initAnswer(r.lastCorr(), 6161, 5));
    ASSERT_TRUE(r.impl->identityAssigned());
    // The app finally looks: it drains what the ring held (stale: the final snapshot was refused) ...
    EXPECT_EQ(r.impl->appProducerId(), -1) << "precondition: the final snapshot did not fit the ring";
    // ... and the duty side re-offers the newest snapshot from its cold sweep point.
    r.impl->sweepTimedOut(std::chrono::steady_clock::time_point{std::chrono::nanoseconds{r.now}});
    EXPECT_EQ(r.impl->appProducerId(), 6161);
    EXPECT_EQ(r.impl->appProducerEpoch(), 5);
}

TEST(ProducerIdRegression, StatusGettersAreBoundToOneThreadRegression) {
    Rig r(cfgOf(Acks::Leader));
    r.ticks(2, kMs);
    r.answer(initAnswer(r.lastCorr(), 808, 1));
    int64_t offId = 0;
    int16_t offEp = 0;
    std::thread other([&] {
        offId = r.impl->appProducerId();
        offEp = r.impl->appProducerEpoch();
    });
    other.join();
    EXPECT_EQ(offId, -1) << "off the bound thread: not available, the ring is untouched";
    EXPECT_EQ(offEp, -1);
    EXPECT_EQ(r.impl->appProducerId(), 808) << "the bound thread still sees everything";
    EXPECT_EQ(r.impl->appProducerEpoch(), 1);
}

TEST(ProducerIdRegression, ErrorResponseWithCodeZeroIsAProtocolErrorRegression) {
    Rig r(cfgOf(Acks::Leader));
    AckLedger L;
    L.attach(*r.impl);
    ASSERT_TRUE(r.send(0, 1, true, 1));
    r.ticks(2, kMs);
    const auto zero = FakeCoordinator::errorResponse(r.lastCorr(), static_cast<rpc::ErrorCode>(0), "?");
    ASSERT_TRUE(r.hub.onCoordinatorResponse(zero.templateId, zero.data(), zero.len, r.now));
    ASSERT_TRUE(std::holds_alternative<rp::IdFailed>(r.impl->identity()));
    EXPECT_EQ(std::get<rp::IdFailed>(r.impl->identity()).errorCode, 39) << "InvalidRequest, never a code-less failure";
    r.ticks(2, kMs);
    ASSERT_EQ(L.times(1), 1);
    EXPECT_EQ(L.last.at(1).ec, 39);
}

TEST(ProducerIdRegression, FailedIdentityWalksOnlyWhenSomethingIsPendingRegression) {
    Rig r(cfgOf(Acks::Leader));
    AckLedger L;
    L.attach(*r.impl);
    r.ticks(2, kMs);
    r.answer(initAnswer(r.lastCorr(), 0, 0, static_cast<int16_t>(rpc::ErrorCode::InvalidRequest)));
    r.ticks(3, kMs);   // nothing pending: the passes are no-ops
    for (rc::Token t = 1; t <= 3; ++t) {   // records accepted after the failure still complete, each once
        ASSERT_TRUE(r.send(static_cast<int32_t>(t % 2), t, true, t));
        r.ticks(3, kMs);
        ASSERT_EQ(L.times(t), 1);
        EXPECT_EQ(L.status(t), OpStatus::DeliveryFailed);
    }
}

TEST(ProducerIdRegression, HeldRecordsFailAtDeliveryTimeoutWhileTheIdKeepsRetryingRegression) {
    for (const bool direct : {false, true}) {
        SCOPED_TRACE(direct ? "Direct" : "Ring");
        rc::ProducerConfig cfg = cfgOf(Acks::Leader);
        cfg.tier2Slots = 4;
        Rig r(cfg, direct);
        AckLedger L;
        L.attach(*r.impl);
        L.attachBatches(*r.impl);
        r.ticks(1);   // a Direct producer's append clock is the duty pass's (the Client seeds it at creation)
        // void, AckToken, a claim, and a Tier-2 batch: all held, the id never answers.
        ASSERT_TRUE(r.send(0, 1));
        ASSERT_TRUE(r.send(1, 2, true, 1));
        ProducerImpl::ClaimGrant g = r.impl->claimRecord(1, 2, Key(uint64_t{3}), 16, AckLedger::h(2));
        ASSERT_EQ(g.result, rc::SendResult::Ok);
        std::fill(g.base, g.base + 16, uint8_t{1});
        ASSERT_TRUE(r.impl->commitClaim(g.token));
        if (!direct) {   // a Tier-2 batch is a Ring-mode / batch-thread feature
            rc::BatchBuilder bb(r.impl.get(), 1, 3);
            for (int i = 0; i < 4; ++i) bb.add(kVal);
            ASSERT_EQ(bb.send(rc::AckToken{3}), rc::OpResult::Accepted);
        }
        r.ticks(10, kMs);
        EXPECT_TRUE(r.wire.empty());
        EXPECT_EQ(L.times(1), 0);
        // Past deliveryTimeoutMs (10 s) with the identity still re-requesting.
        r.ticks(1, 5'000 * kMs);
        r.ticks(2, kMs);
        r.ticks(1, 6'000 * kMs);
        r.impl->sweepTimedOut(std::chrono::steady_clock::time_point{std::chrono::nanoseconds{r.now}});
        EXPECT_FALSE(r.impl->identityAssigned());
        EXPECT_GE(r.initRequests(), 2u) << "still asking";
        for (rc::Token t = 1; t <= (direct ? 2u : 3u); ++t) {
            ASSERT_EQ(L.times(t), 1) << "token " << t << " exactly once";
            EXPECT_EQ(L.status(t), OpStatus::DeliveryFailed);
        }
        EXPECT_GE(L.batchAckedRecords(), direct ? 3 : 7) << "the void send and the batch are counted";
        r.impl->sweepTimedOut(std::chrono::steady_clock::time_point{std::chrono::nanoseconds{r.now + kMs}});
        for (rc::Token t = 1; t <= 2; ++t) EXPECT_EQ(L.times(t), 1) << "never twice";
    }
}

TEST(ProducerIdRegression, CloseWithHeldRecordsCompletesWithinTheBoundRegression) {
    Rig r(cfgOf(Acks::Leader));
    AckLedger L;
    L.attach(*r.impl);
    ASSERT_TRUE(r.send(0, 1, true, 1));
    r.ticks(3, kMs);
    struct Got { int n = 0; OpStatus st = OpStatus::Ok; } got;
    rc::detail::OpTarget tgt;
    tgt.handler = rc::CompletionHandler{
        [](void* ctx, const rc::Completion& c, std::span<const uint8_t>) noexcept {
            auto* g = static_cast<Got*>(ctx);
            ++g->n;
            g->st = c.status;
        },
        &got};
    r.impl->applyBeginClose(tgt, 9, r.now);
    (void)r.impl->closeIntake();
    r.ticks(1, 9'000 * kMs);
    EXPECT_EQ(got.n, 0) << "inside the bound";
    r.ticks(1, 2'000 * kMs);
    r.impl->sweepTimedOut(std::chrono::steady_clock::time_point{std::chrono::nanoseconds{r.now}});
    r.ticks(2, kMs);
    ASSERT_EQ(got.n, 1) << "completes within deliveryTimeoutMs + one pass";
    EXPECT_EQ(L.times(1), 1);
    EXPECT_EQ(L.status(1), OpStatus::DeliveryFailed);
}

// #343: enableCoordinator moves a hub-fed BARE impl off the rig id - only legal before anything was sent.  With
// acked batches in flight it used to reset the partitions and re-stamp those batches at sequence 0 under the new
// id (duplicate records).  Debug builds assert; release builds refuse (logged): the pid, the sequences and the
// in-flight batches are untouched and no InitProducerId is ever requested.
#if GTEST_HAS_DEATH_TEST
TEST(ProducerIdRegression, EnableCoordinatorWithInFlightBatchesNeverReStampsThemRegression) {
    Rig r(cfgOf(Acks::Leader), /*direct=*/false, /*wired=*/false);
    ASSERT_TRUE(r.send(0, 1, true, 1));
    ASSERT_TRUE(r.send(0, 2, true, 2));
    r.tick();
    ASSERT_EQ(r.wire.size(), 1u) << "one batch in flight";
    ASSERT_EQ(r.wire[0].pid, rp::kRigProducerId);
    const rp::PartitionState* ps = r.impl->partitionState(1, 0);
    ASSERT_NE(ps, nullptr);
    ASSERT_FALSE(ps->inflight.empty());
    const int32_t nextSeq = ps->nextBaseSequence;
    ASSERT_GT(nextSeq, 0);

    EXPECT_DEBUG_DEATH(r.impl->enableCoordinator(r.fake.sink(), 0), "in-flight");

    // (Release builds ran the call above in this process; debug builds ran it in a child.  Either way:)
    ASSERT_TRUE(r.impl->identityAssigned());
    EXPECT_EQ(std::get<rp::IdAssigned>(r.impl->identity()).producerId, rp::kRigProducerId);
    EXPECT_FALSE(r.impl->hasCoordinator()) << "a refused enable wires nothing";
    EXPECT_EQ(ps->producerId, rp::kRigProducerId) << "the partition keeps its identity";
    EXPECT_EQ(ps->nextBaseSequence, nextSeq) << "its sequences are not restarted";
    EXPECT_FALSE(ps->inflight.empty());
    r.ticks(5, kMs);
    EXPECT_EQ(r.initRequests(), 0u) << "no InitProducerId: the batch is never re-stamped under a new id";
    EXPECT_EQ(r.wire.size(), 1u) << "and never re-sent";
    EXPECT_EQ(r.wire[0].baseSeq, 0);
}
#endif
