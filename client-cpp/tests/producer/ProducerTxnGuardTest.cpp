// tests/producer/ProducerTxnGuardTest.cpp - Herald prerequisites Task 4 (R7): the lease produce guard covers a transactional producer's
// commit.  A transactional producer bound to a LeaseSession (LeaseSession::bindProducer) whose guard trips (at risk) or ends (lost)
// can no longer commit: the open transaction fails with LeaseAtRisk and EndTxn(commit) is never sent; EndTxn(abort) always is.
// A commit whose EndTxn already left is decided by its answer.  Rig: a real Client (Invoker) against the InvokerGateway broker model on
// an injected clock; the trip is made by advancing the clock past the armed at-risk instant with the keepalive answers withheld.
// Nothing sleeps; every drive loop is capped.
#include "../support/InvokerGateway.h"
#include "../support/LeaseGatewayRig.h"
#include "../support/TestCompletions.h"

#include <rune/client/Client.h>
#include <rune/client/Completions.h>
#include <rune/client/lease/LeaseSession.h>
#include <rune/codec/Descriptor.h>
#include <rune/codec/Fingerprint.h>
#include <rune/codec/Serialize.h>
#include <rune/protocol/client/LeaseMessages.h>

#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <map>
#include <memory>
#include <optional>
#include <poll.h>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

namespace rc  = rune::client;
namespace rpc = rune::protocol::client;
using rc::OpStatus;
using rc::TxnState;
using TF = test_support::InvokerGateway::TxnFrameKind;

namespace {

constexpr int64_t kMs = 1'000'000;
int64_t g_now = 0;   // the injected duty clock (the test thread is the only thread)
int64_t fakeNow() noexcept { return g_now; }
int64_t realNow() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch()).count();
}
constexpr uint16_t kAtRisk = static_cast<uint16_t>(rc::ClientErrorCode::LeaseAtRisk);

using Row = test_support::LeaseRow;

struct DataDirs {
    std::vector<std::string> dirs;
    ~DataDirs() {
        std::error_code ec;
        for (const std::string& d : dirs) std::filesystem::remove_all(d, ec);
    }
    std::string next() {
        dirs.push_back((std::filesystem::temp_directory_path() /
                        ("txn_guard_" + std::to_string(::getpid()) + "_" + std::to_string(dirs.size()))).string());
        return dirs.back();
    }
} g_dataDirs;

int32_t g_stream = 6400;

rc::ClientConfig invokerCfg() {
    rc::ClientConfig c;
    c.gatewayChannel  = "aeron:ipc";
    c.gatewayStreamId = g_stream;
    c.egressChannel   = "aeron:ipc";
    c.egressStreamId  = g_stream + 1;
    g_stream += 2;
    c.dataDir         = g_dataDirs.next();
    c.runnerMode      = rc::RunnerMode::Invoker;
    c.nanoClock       = &fakeNow;
    g_now             = realNow();
    rc::TopicSpec ts;
    ts.name           = "t";
    ts.topicId        = 1;
    ts.partitionCount = 1;
    c.topics.push_back(ts);
    return c;
}


struct Cb {
    int entered = 0, cleared = 0, lost = 0;
};
struct OnLostF {
    Cb* cb;
    void operator()(rc::LeaseRef, rc::LostReason) const { ++cb->lost; }
};
struct OnRiskF {
    Cb* cb;
    void operator()(rc::LeaseRef, rc::AtRiskEdge e) const { (e == rc::AtRiskEdge::Entered ? cb->entered : cb->cleared)++; }
};

constexpr int64_t kLeaseTtlMs = 3000;
constexpr int64_t kAtRiskAfter = 1500 * kMs;                                     // the armed at-risk instant (ttl/2)
constexpr int64_t kMarginAfter = kLeaseTtlMs * kMs - kLeaseTtlMs * kMs / 50 - 100 * kMs;   // MarginExpired: the lease is lost

rc::ProducerConfig txnCfg(std::string id) {
    rc::ProducerConfig c;
    c.acks                   = rc::Acks::Leader;
    c.lingerUs               = 0;
    c.batchRecords           = 100;
    c.batchBytes             = 64 * 1024;
    c.maxInFlight            = 8;
    c.produceTimeoutMs       = 600'000;
    c.deliveryTimeoutMs      = 1'200'000;
    c.transactionOpTimeoutMs = 600'000;
    c.transactionalId        = std::move(id);
    return c;
}
const std::vector<uint8_t> kVal(16, 0x5A);

/// A Client (Invoker) + gateway stub + LeaseSession, with transactional producers bound to the session's leases.
struct GRig {
    using Session = rc::LeaseSession<4, OnLostF, OnRiskF>;
    std::unique_ptr<rc::Client>        client;
    test_support::InvokerGateway       gw;
    Cb                                 cb;
    std::optional<rc::LeaseTopic<Row>> topic;
    std::unique_ptr<Session>           session;
    std::unique_ptr<rc::Completions>   pq;
    std::map<rc::Token, rc::Completion> got;
    std::map<rc::Token, int>           completions;   ///< per token: how often it completed (a map keyed by token cannot see a double)
    rc::Token                          tok = 0;
    const rc::TopicHandle              data{1, 1};

    GRig() : GRig(invokerCfg()) {}
    explicit GRig(const rc::ClientConfig& c)
        : client(std::make_unique<rc::Client>(c)),
          gw(c.dataDir + "/aeron", c.gatewayChannel, c.gatewayStreamId, c.egressChannel, c.egressStreamId) {
        test_support::configureLeaseGateway(gw);
        connect();
        topic.emplace(openTopic());
        for (int i = 0; i < 4000 && topic->producer().producerId() < 0; ++i) { client->doWork(); gw.pump(); }
        EXPECT_GE(topic->producer().producerId(), 0);
        session = std::make_unique<Session>(*client, OnLostF{&cb}, OnRiskF{&cb}, rc::LeaseSessionOptions{.maxDelayMs = 100});
        settle(4);
        gw.idempotence = false;   // the plain stub path: acks are held or answered by replyAcks
        gw.initPidReply = true;
        gw.replyTxn     = true;
        pq = std::make_unique<rc::Completions>(*client);
    }
    ~GRig() {
        EXPECT_FALSE(gw.anyHeldTxnReplies()) << "a held coordinator answer was never released";
        session.reset();
        topic.reset();
        pq.reset();
    }
    template <class P>
    bool drive(P&& pred, int cap = 4000) {
        for (int i = 0; i < cap; ++i) {
            client->doWork();
            gw.pump();
            if (session) session->doWork(g_now);
            poll();
            if (pred()) return true;
        }
        return false;
    }
    void poll() {
        if (!pq) return;
        pq->poll([&](const rc::Completion& c, std::span<const uint8_t>) { got[c.token] = c; ++completions[c.token]; });
    }
    void connect() { test_support::connectGateway(*client, gw); }
    rc::LeaseTopic<Row> openTopic() { return test_support::openLeaseTopic(*client, gw); }
    /// `rounds` hops of client / stub / session at the current clock.
    void settle(int rounds = 10) {
        for (int i = 0; i < rounds; ++i) {
            const int64_t base = g_now;
            g_now              = base + i * 20'000;
            client->doWork();
            g_now = base;
            gw.pump();
            if (session) session->doWork(g_now);
            poll();
        }
    }
    void at(int64_t t) {
        g_now = t;
        settle();
    }
    [[nodiscard]] rc::LeaseRef addLease(uint64_t id) {
        return session->add(*topic, Row{id}, 5, kLeaseTtlMs, g_now, nullptr);
    }
    /// A transactional producer, initialised, bound to `ref` (before its first write).
    rc::Producer producer(const std::string& txnId, rc::LeaseRef ref) {
        rc::Producer p = client->testProducerWithRingCap(txnCfg(txnId), 64 * 1024);
        const rc::Token t = ++tok;
        EXPECT_EQ(p.initTransactions(*pq, t), rc::OpResult::Accepted);
        EXPECT_TRUE(await(t));
        EXPECT_EQ(got[t].status, OpStatus::Ok);
        EXPECT_EQ(session->bindProducer(ref, p), rc::OpResult::Accepted);
        settle();
        return p;
    }
    /// A transactional producer with no guard.
    rc::Producer producerUnbound(const std::string& txnId) {
        rc::Producer p = client->testProducerWithRingCap(txnCfg(txnId), 64 * 1024);
        const rc::Token t = ++tok;
        EXPECT_EQ(p.initTransactions(*pq, t), rc::OpResult::Accepted);
        EXPECT_TRUE(await(t));
        EXPECT_EQ(got[t].status, OpStatus::Ok);
        return p;
    }
    bool await(rc::Token t, int cap = 4000) { return drive([&] { return got.count(t) != 0; }, cap); }
    rc::Token send(rc::Producer& p) {
        const rc::Token t = ++tok;
        EXPECT_EQ(p.trySend(data, rc::Key(uint64_t{1}), kVal, *pq, t), rc::SendResult::Ok);
        return t;
    }
    rc::Token commit(rc::Producer& p) {
        const rc::Token t = ++tok;
        EXPECT_EQ(p.commitTransaction(*pq, t), rc::OpResult::Accepted);
        return t;
    }
    rc::Token abort(rc::Producer& p) {
        const rc::Token t = ++tok;
        EXPECT_EQ(p.abortTransaction(*pq, t), rc::OpResult::Accepted);
        return t;
    }
    void expectFailedAtRisk(rc::Token t, const char* what) {
        ASSERT_EQ(got.count(t), 1u) << what << ": no completion";
        EXPECT_EQ(got[t].status, OpStatus::DeliveryFailed) << what;
        EXPECT_EQ(got[t].errorCode, kAtRisk) << what;
    }
    [[nodiscard]] int endTxns(int commit) const {
        int n = 0;
        for (const auto& f : gw.txnFrames) n += (f.kind == TF::EndTxn && f.partition == commit) ? 1 : 0;
        return n;
    }
    [[nodiscard]] int endTxnsAny() const { return endTxns(0) + endTxns(1); }
    [[nodiscard]] int frames(TF k) const {
        return static_cast<int>(std::count_if(gw.txnFrames.begin(), gw.txnFrames.end(), [&](const auto& f) { return f.kind == k; }));
    }
    /// Answer the newest keepalive that carried lease key `id`: the guard re-arms (Cleared).
    void rearm(uint64_t id) {
        std::vector<uint8_t> key(8);
        std::memcpy(key.data(), &id, 8);
        const int before = cb.cleared;
        for (int i = 0; i < 60 && cb.cleared == before; ++i) {
            for (std::size_t k = gw.kaSeen.size(); k-- > 0;) {
                const auto& es = gw.kaSeen[k].entries;
                if (std::any_of(es.begin(), es.end(), [&](const auto& e) { return e.key == key; })) {
                    const test_support::InvokerGateway::KaAnswer ok{0, 0, 3000};
                    gw.answerKa(k, std::span(&ok, 1), true);
                    settle();
                    break;
                }
            }
            if (cb.cleared == before) at(g_now + 50 * kMs);
        }
        ASSERT_GT(cb.cleared, before) << "the keepalive answer re-armed the guard";
    }
    /// Bring the open transaction to "one record sent, acked": AddPartitions answered, the data acked.
    void sendAcked(rc::Producer& p) {
        gw.replyAcks = true;
        const rc::Token s = send(p);
        ASSERT_TRUE(await(s));
        ASSERT_EQ(got[s].status, OpStatus::Ok);
        ASSERT_GE(frames(TF::AddPartitionsOk), 1);
    }
    /// A record sent, not acked (the broker holds the ack); its AddPartitions is answered.
    rc::Token sendUnacked(rc::Producer& p) {
        gw.replyAcks = false;
        const rc::Token s = send(p);
        const std::size_t before = gw.held.size();
        EXPECT_TRUE(drive([&] { return gw.held.size() > before; })) << "the record reached the broker and is held";
        return s;
    }
    void trip() {
        const int before = cb.entered;
        at(g_now_base + kAtRiskAfter);
        ASSERT_GT(cb.entered, before) << "the guard tripped";
    }
    int64_t g_now_base = 0;   // the instant the lease was granted
};

}  // namespace

// ---------------------------------------------------------------------------------------------------------------------------

TEST(ProducerTxnGuardRegression, CommitWithAllRecordsAckedBlockedAtRiskRegression) {
    GRig r;
    r.g_now_base = g_now;
    const auto ref = r.addLease(1);
    rc::Producer p = r.producer("tx-a", ref);
    p.beginTransaction();
    r.settle();
    for (int i = 0; i < 3; ++i) ASSERT_NO_FATAL_FAILURE(r.sendAcked(p));
    ASSERT_EQ(p.state(), TxnState::InTransaction);
    ASSERT_NO_FATAL_FAILURE(r.trip());
    EXPECT_EQ(p.state(), TxnState::AbortableError) << "the trip fails the open transaction";
    const rc::Token c = r.commit(p);
    ASSERT_TRUE(r.await(c));
    r.expectFailedAtRisk(c, "commit");
    EXPECT_EQ(r.endTxns(1), 0) << "no EndTxn(commit) leaves";
    EXPECT_EQ(p.state(), TxnState::AbortableError);
}

// O-1: a record the guard refuses inside the ring read poisons the open transaction in the same step.  The begin sits in the ring
// behind the trip (applied after it), the send is accepted at the call; the drive refuses it LeaseAtRisk.  Re-arming the lease then
// must not let the transaction commit without that record.
TEST(ProducerTxnGuardRegression, RecordRefusedByGuardPoisonsTransactionRegression) {
    GRig r;
    r.g_now_base = g_now;
    const auto ref = r.addLease(1);
    rc::Producer p = r.producer("tx-a", ref);
    ASSERT_NO_FATAL_FAILURE(r.trip());
    p.beginTransaction();
    const rc::Token s = ++r.tok;
    const rc::SendResult sr = p.trySend(r.data, rc::Key(uint64_t{1}), kVal, *r.pq, s);
    ASSERT_EQ(sr, rc::SendResult::Ok) << "accepted at the call";
    ASSERT_TRUE(r.await(s));
    r.expectFailedAtRisk(s, "the refused record");
    EXPECT_EQ(p.state(), TxnState::AbortableError) << "the refusal poisons the open transaction";
    ASSERT_NO_FATAL_FAILURE(r.rearm(1));
    const rc::Token c = r.commit(p);
    ASSERT_TRUE(r.await(c));
    EXPECT_NE(r.got[c].status, OpStatus::Ok) << "a poisoned transaction never commits";
    EXPECT_EQ(r.got[c].status, OpStatus::DeliveryFailed);
    EXPECT_EQ(r.got[c].errorCode, kAtRisk);
    EXPECT_EQ(r.endTxns(1), 0);
    const rc::Token a = r.abort(p);
    ASSERT_TRUE(r.await(a));
    EXPECT_EQ(r.got[a].status, OpStatus::Ok);
    EXPECT_EQ(p.state(), TxnState::Ready);
    p.beginTransaction();
    r.settle();
    ASSERT_NO_FATAL_FAILURE(r.sendAcked(p));
    const rc::Token c2 = r.commit(p);
    ASSERT_TRUE(r.await(c2));
    EXPECT_EQ(r.got[c2].status, OpStatus::Ok);
    int produced = 0;
    for (const auto& pr : r.gw.produced) produced += pr.records;
    EXPECT_EQ(produced, 1) << "exactly the one record of the second transaction was produced (the refused one never was)";
}

// I-7: a bind applied onto an already tripped lease must not re-enter the producer ring read (the old path ran failGuarded ->
// failAllPending -> discardRing_ inside the drain's own controlledRead and left the ring head pointing at zeroed memory).
TEST(ProducerTxnGuardRegression, BindOnTrippedLeaseKeepsRingAliveRegression) {
    GRig r;
    r.g_now_base = g_now;
    const auto ref = r.addLease(1);
    ASSERT_NO_FATAL_FAILURE(r.trip());
    rc::Producer p = r.client->testProducerWithRingCap(txnCfg("tx-a"), 64 * 1024);
    const rc::Token i = ++r.tok;
    ASSERT_EQ(p.initTransactions(*r.pq, i), rc::OpResult::Accepted);
    ASSERT_TRUE(r.await(i));
    ASSERT_EQ(r.got[i].status, OpStatus::Ok);
    // back to back, no drive in between: bind, begin, send all sit in the ring together
    ASSERT_EQ(r.session->bindProducer(ref, p), rc::OpResult::Accepted);
    p.beginTransaction();
    const rc::Token s = ++r.tok;
    const rc::SendResult sr = p.trySend(r.data, rc::Key(uint64_t{1}), kVal, *r.pq, s);
    ASSERT_EQ(sr, rc::SendResult::Ok) << "accepted at the call (the begin is still in the ring)";
    r.settle();
    ASSERT_TRUE(r.await(s));
    r.expectFailedAtRisk(s, "send onto the tripped lease");
    EXPECT_EQ(r.completions[s], 1) << "completed exactly once";
    const rc::Token a = r.abort(p);
    ASSERT_TRUE(r.await(a)) << "the ring is alive: the abort is drained";
    EXPECT_EQ(r.got[a].status, OpStatus::Ok);
    EXPECT_EQ(p.state(), TxnState::Ready);
    p.beginTransaction();
    r.settle();
    const rc::Token a2 = r.abort(p);
    ASSERT_TRUE(r.await(a2));
    EXPECT_EQ(r.got[a2].status, OpStatus::Ok);
}

TEST(ProducerTxnGuardRegression, TripFailsOpenTransactionRegression) {
    GRig r;
    r.g_now_base = g_now;
    const auto ref = r.addLease(1);
    rc::Producer p = r.producer("tx-a", ref);
    p.beginTransaction();
    r.settle();
    const rc::Token s1 = r.sendUnacked(p);
    const rc::Token s2 = r.sendUnacked(p);
    ASSERT_EQ(r.got.count(s1) + r.got.count(s2), 0u) << "unacked";
    ASSERT_NO_FATAL_FAILURE(r.trip());
    r.settle();
    r.expectFailedAtRisk(s1, "send 1");
    r.expectFailedAtRisk(s2, "send 2");
    EXPECT_EQ(p.state(), TxnState::AbortableError);
    const rc::Token s3 = ++r.tok;
    ASSERT_EQ(p.trySend(r.data, rc::Key(uint64_t{1}), kVal, *r.pq, s3), rc::SendResult::Ok) << "the issuing-thread gate is still open";
    ASSERT_TRUE(r.await(s3));
    r.expectFailedAtRisk(s3, "send 3");
    const rc::Token c = r.commit(p);
    ASSERT_TRUE(r.await(c));
    r.expectFailedAtRisk(c, "commit");
    EXPECT_EQ(r.endTxns(1), 0);
}

// I-4: an abort already queued in the producer ring when the guard trips is still applied (the trip must not read the ring).
TEST(ProducerTxnGuardRegression, AbortQueuedAtTripStillAbortsRegression) {
    GRig r;
    r.g_now_base = g_now;
    const auto ref = r.addLease(1);
    rc::Producer p = r.producer("tx-a", ref);
    p.beginTransaction();
    r.settle();
    ASSERT_NO_FATAL_FAILURE(r.sendAcked(p));
    const rc::Token a = r.abort(p);   // queued in the ring, no drive
    ASSERT_NO_FATAL_FAILURE(r.trip());
    ASSERT_TRUE(r.await(a));
    EXPECT_EQ(r.got[a].status, OpStatus::Ok) << "an abort is never blocked";
    EXPECT_EQ(r.endTxns(0), 1);
    EXPECT_EQ(r.endTxns(1), 0);
    EXPECT_EQ(p.state(), TxnState::Ready);
}

// I-3: a bind the guard refuses fails the open transaction at the refusal, like a trip does.
TEST(ProducerTxnGuardRegression, RefusedBindFailsOpenTransactionRegression) {
    GRig r;
    r.g_now_base = g_now;
    const auto ref = r.addLease(1);
    rc::Producer p = r.producerUnbound("tx-a");
    p.beginTransaction();
    r.settle();
    ASSERT_NO_FATAL_FAILURE(r.sendAcked(p));
    ASSERT_EQ(p.state(), TxnState::InTransaction);
    ASSERT_EQ(r.session->bindProducer(ref, p), rc::OpResult::Accepted);
    ASSERT_EQ(r.session->remove(ref), rc::OpResult::Accepted);   // the End reaches the guard before the bind
    r.settle();
    ASSERT_EQ(r.session->core().bindRefused(), 1u);
    EXPECT_EQ(p.state(), TxnState::AbortableError) << "the refusal fails the open transaction";
    const rc::Token c = r.commit(p);
    ASSERT_TRUE(r.await(c));
    r.expectFailedAtRisk(c, "commit");
    EXPECT_EQ(r.endTxns(1), 0);
    const rc::Token a = r.abort(p);
    ASSERT_TRUE(r.await(a));
    EXPECT_EQ(r.got[a].status, OpStatus::Ok);
    EXPECT_EQ(r.endTxns(0), 1);
}

// M-1: the guard blocks after AddOffsetsToTxn left and before the first TxnOffsetCommit: the offsets leg stops there.
TEST(ProducerTxnGuardRegression, RefusedBindAfterAddOffsetsBlocksOffsetCommitRegression) {
    GRig r;
    r.g_now_base = g_now;
    const auto ref = r.addLease(1);
    rc::Producer p = r.producerUnbound("tx-a");
    p.beginTransaction();
    r.settle();
    r.gw.holdAddOffsets = true;
    const rc::OffsetEntry e{1, 0, 10};
    const rc::Token o = ++r.tok;
    ASSERT_EQ(p.sendOffsetsToTransaction(std::span<const rc::OffsetEntry>(&e, 1), "g", *r.pq, o), rc::OpResult::Accepted);
    ASSERT_TRUE(r.drive([&] { return r.frames(TF::AddOffsets) == 1; })) << "AddOffsetsToTxn left";
    ASSERT_EQ(r.got.count(o), 0u);
    ASSERT_EQ(r.session->bindProducer(ref, p), rc::OpResult::Accepted);
    ASSERT_EQ(r.session->remove(ref), rc::OpResult::Accepted);
    r.settle();
    ASSERT_EQ(r.session->core().bindRefused(), 1u);
    r.gw.releaseAddOffsets();
    ASSERT_TRUE(r.await(o));
    r.expectFailedAtRisk(o, "sendOffsets");
    EXPECT_EQ(r.frames(TF::AddOffsets), 1);
    EXPECT_EQ(r.frames(TF::TxnOffsetCommit), 0) << "no TxnOffsetCommit leaves";
    EXPECT_EQ(p.state(), TxnState::AbortableError);
}

TEST(ProducerTxnGuardRegression, TripDuringAwaitDataFailsCommitRegression) {
    GRig r;
    r.g_now_base = g_now;
    const auto ref = r.addLease(1);
    rc::Producer p = r.producer("tx-a", ref);
    p.beginTransaction();
    r.settle();
    const rc::Token s = r.sendUnacked(p);
    const rc::Token c = r.commit(p);
    r.settle();
    ASSERT_EQ(p.state(), TxnState::CommittingTransaction);
    ASSERT_EQ(r.got.count(c), 0u) << "the commit waits for the unacked record";
    ASSERT_NO_FATAL_FAILURE(r.trip());
    r.settle();
    r.expectFailedAtRisk(s, "send");
    r.expectFailedAtRisk(c, "commit");
    EXPECT_EQ(r.endTxns(1), 0) << "no EndTxn(commit) leaves";
    EXPECT_EQ(p.state(), TxnState::AbortableError);
}

TEST(ProducerTxnGuardRegression, TripAfterEndSentKeepsCommitRegression) {
    GRig r;
    r.g_now_base = g_now;
    const auto ref = r.addLease(1);
    rc::Producer p = r.producer("tx-a", ref);
    p.beginTransaction();
    r.settle();
    ASSERT_NO_FATAL_FAILURE(r.sendAcked(p));
    r.gw.holdEndTxnReply = true;   // the coordinator decides the commit; its answer is withheld
    const rc::Token c = r.commit(p);
    ASSERT_TRUE(r.drive([&] { return r.endTxns(1) == 1; })) << "EndTxn(commit) left";
    ASSERT_EQ(r.got.count(c), 0u);
    ASSERT_NO_FATAL_FAILURE(r.trip());
    EXPECT_EQ(r.got.count(c), 0u) << "the commit was sent inside the lease: its answer decides";
    EXPECT_EQ(p.state(), TxnState::CommittingTransaction);
    r.gw.releaseEndTxnReplies();
    ASSERT_TRUE(r.await(c));
    EXPECT_EQ(r.got[c].status, OpStatus::Ok);
    EXPECT_EQ(r.endTxns(1), 1) << "one EndTxn(commit): never re-sent or blocked";
    EXPECT_EQ(p.state(), TxnState::Ready);
    // the next transaction: begin is allowed, every send fails until re-arm
    p.beginTransaction();
    r.settle();
    const rc::Token s = r.send(p);
    ASSERT_TRUE(r.await(s));
    r.expectFailedAtRisk(s, "send while tripped");
    const rc::Token c2 = r.commit(p);
    ASSERT_TRUE(r.await(c2));
    r.expectFailedAtRisk(c2, "commit while tripped");
    EXPECT_EQ(r.endTxns(1), 1);
}

TEST(ProducerTxnGuardRegression, AbortAllowedWhileBlockedRegression) {
    GRig r;
    r.g_now_base = g_now;
    const auto ref = r.addLease(1);
    rc::Producer p = r.producer("tx-a", ref);
    p.beginTransaction();
    r.settle();
    ASSERT_NO_FATAL_FAILURE(r.sendAcked(p));
    ASSERT_NO_FATAL_FAILURE(r.trip());
    ASSERT_EQ(p.state(), TxnState::AbortableError);
    const rc::Token a = r.abort(p);
    ASSERT_TRUE(r.await(a));
    EXPECT_EQ(r.got[a].status, OpStatus::Ok);
    EXPECT_EQ(r.endTxns(0), 1) << "EndTxn(abort) is sent while the guard blocks";
    EXPECT_EQ(r.endTxns(1), 0);
    EXPECT_EQ(p.state(), TxnState::Ready);
}

TEST(ProducerTxnGuardRegression, BeginAgainAfterClearedRegression) {
    GRig r;
    r.g_now_base = g_now;
    const auto ref = r.addLease(1);
    rc::Producer p = r.producer("tx-a", ref);
    p.beginTransaction();
    r.settle();
    ASSERT_NO_FATAL_FAILURE(r.sendAcked(p));
    ASSERT_NO_FATAL_FAILURE(r.trip());
    const rc::Token a = r.abort(p);
    ASSERT_TRUE(r.await(a));
    ASSERT_EQ(r.got[a].status, OpStatus::Ok);
    ASSERT_NO_FATAL_FAILURE(r.rearm(1));
    p.beginTransaction();
    r.settle();
    ASSERT_NO_FATAL_FAILURE(r.sendAcked(p));
    const rc::Token c = r.commit(p);
    ASSERT_TRUE(r.await(c));
    EXPECT_EQ(r.got[c].status, OpStatus::Ok);
    EXPECT_EQ(r.endTxns(1), 1);
    EXPECT_EQ(r.endTxns(0), 1);
    EXPECT_EQ(p.state(), TxnState::Ready);
}

TEST(ProducerTxnGuardRegression, SendOffsetsBlockedAtRiskRegression) {
    GRig r;
    r.g_now_base = g_now;
    const auto ref = r.addLease(1);
    rc::Producer p = r.producer("tx-a", ref);
    p.beginTransaction();
    r.settle();
    ASSERT_NO_FATAL_FAILURE(r.trip());
    const rc::OffsetEntry e{1, 0, 10};
    const rc::Token t = ++r.tok;
    ASSERT_EQ(p.sendOffsetsToTransaction(std::span<const rc::OffsetEntry>(&e, 1), "g", *r.pq, t), rc::OpResult::Accepted);
    ASSERT_TRUE(r.await(t));
    r.expectFailedAtRisk(t, "sendOffsets");
    EXPECT_EQ(r.frames(TF::AddOffsets), 0);
    EXPECT_EQ(r.frames(TF::TxnOffsetCommit), 0) << "no TxnOffsetCommit leaves";
    EXPECT_EQ(p.state(), TxnState::AbortableError);
}

TEST(ProducerTxnGuardRegression, GuardEndedIsFinalRegression) {
    GRig r;
    r.g_now_base = g_now;
    const auto ref = r.addLease(1);
    rc::Producer p = r.producer("tx-a", ref);
    p.beginTransaction();
    r.settle();
    ASSERT_NO_FATAL_FAILURE(r.sendAcked(p));
    // a keepalive of the lease, answered only after the loss (a late Ok of the dead lease)
    r.at(r.g_now_base + kLeaseTtlMs * kMs / 3 + kMs);   // the ttl/3 keepalive goes out; its answer is withheld
    std::size_t staleK = r.gw.kaSeen.size(), staleE = 0;
    {
        std::vector<uint8_t> key(8);
        const uint64_t id = 1;
        std::memcpy(key.data(), &id, 8);
        for (std::size_t k = 0; k < r.gw.kaSeen.size(); ++k)
            for (std::size_t e = 0; e < r.gw.kaSeen[k].entries.size(); ++e)
                if (r.gw.kaSeen[k].entries[e].key == key) { staleK = k; staleE = e; }
    }
    ASSERT_LT(staleK, r.gw.kaSeen.size()) << "a keepalive carrying the lease was seen";
    r.at(g_now + kMarginAfter);   // MarginExpired: the lease is lost, the guard ends
    ASSERT_EQ(r.cb.lost, 1);
    const rc::Token c = r.commit(p);
    ASSERT_TRUE(r.await(c));
    r.expectFailedAtRisk(c, "commit after the lease was lost");
    EXPECT_EQ(r.endTxns(1), 0);
    // a late Ok keepalive answer of the dead lease does not unblock
    {
        const test_support::InvokerGateway::KaAnswer ok{static_cast<uint16_t>(staleE), 0, 3000};
        r.gw.answerKa(staleK, std::span(&ok, 1), true);
        r.settle();
        EXPECT_EQ(r.cb.cleared, 0) << "the stale answer does not re-arm the dead lease";
    }
    const rc::Token a = r.abort(p);
    ASSERT_TRUE(r.await(a));
    ASSERT_EQ(r.got[a].status, OpStatus::Ok);
    p.beginTransaction();
    r.settle();
    const rc::Token s = r.send(p);
    ASSERT_TRUE(r.await(s));
    r.expectFailedAtRisk(s, "send on the old producer");
    const rc::Token c2 = r.commit(p);
    ASSERT_TRUE(r.await(c2));
    r.expectFailedAtRisk(c2, "second commit");
    EXPECT_EQ(r.endTxns(1), 0);
    // a producer bound to a new lease of the session works (a producer is bound to one lease, ever)
    const auto ref2 = r.addLease(2);
    rc::Producer p2 = r.producer("tx-b", ref2);
    p2.beginTransaction();
    r.settle();
    ASSERT_NO_FATAL_FAILURE(r.sendAcked(p2));
    const rc::Token c3 = r.commit(p2);
    ASSERT_TRUE(r.await(c3));
    EXPECT_EQ(r.got[c3].status, OpStatus::Ok);
    EXPECT_EQ(r.endTxns(1), 1);
}

// A bind the guard refuses (the lease left before the bind was applied) blocks the producer for good.  Here the transaction is begun AFTER
// the refusal (nothing was open to fail: RefusedBindFailsOpenTransactionRegression covers that), so it is InTransaction and blocked: the
// checks in endStep_ / offsetsStep_ are what refuse the commit and the offsets (never EndTxn(commit)).
TEST(ProducerTxnGuardRegression, RefusedBindBlocksCommitRegression) {
    GRig r;
    r.g_now_base = g_now;
    const auto ref = r.addLease(1);
    rc::Producer p = r.client->testProducerWithRingCap(txnCfg("tx-a"), 64 * 1024);
    const rc::Token i = ++r.tok;
    ASSERT_EQ(p.initTransactions(*r.pq, i), rc::OpResult::Accepted);
    ASSERT_TRUE(r.await(i));
    ASSERT_EQ(r.session->bindProducer(ref, p), rc::OpResult::Accepted);
    ASSERT_EQ(r.session->remove(ref), rc::OpResult::Accepted);   // the End reaches the guard before the bind
    p.beginTransaction();
    r.settle();
    ASSERT_EQ(r.session->core().bindRefused(), 1u) << "the bind was refused: the lease had left";
    ASSERT_EQ(p.state(), TxnState::InTransaction) << "begun after the refusal: open and blocked";
    const rc::OffsetEntry e{1, 0, 10};
    const rc::Token o = ++r.tok;
    ASSERT_EQ(p.sendOffsetsToTransaction(std::span<const rc::OffsetEntry>(&e, 1), "g", *r.pq, o), rc::OpResult::Accepted);
    ASSERT_TRUE(r.await(o));
    r.expectFailedAtRisk(o, "sendOffsets");
    EXPECT_EQ(r.frames(TF::AddOffsets), 0);
    EXPECT_EQ(p.state(), TxnState::AbortableError);
}

TEST(ProducerTxnGuardRegression, RefusedBindBlocksCommitIssuedRegression) {
    GRig r;
    r.g_now_base = g_now;
    const auto ref = r.addLease(1);
    rc::Producer p = r.client->testProducerWithRingCap(txnCfg("tx-a"), 64 * 1024);
    const rc::Token i = ++r.tok;
    ASSERT_EQ(p.initTransactions(*r.pq, i), rc::OpResult::Accepted);
    ASSERT_TRUE(r.await(i));
    ASSERT_EQ(r.session->bindProducer(ref, p), rc::OpResult::Accepted);
    ASSERT_EQ(r.session->remove(ref), rc::OpResult::Accepted);
    p.beginTransaction();
    r.settle();
    ASSERT_EQ(r.session->core().bindRefused(), 1u);
    ASSERT_EQ(p.state(), TxnState::InTransaction);
    const rc::Token c = r.commit(p);
    ASSERT_TRUE(r.await(c));
    r.expectFailedAtRisk(c, "commit");
    EXPECT_EQ(r.endTxns(1), 0) << "never EndTxn(commit)";
    EXPECT_EQ(p.state(), TxnState::AbortableError);
}

TEST(ProducerTxnGuardRegression, UnboundTransactionalProducerUnchangedRegression) {
    GRig r;
    r.g_now_base = g_now;
    (void)r.addLease(1);
    ASSERT_NO_FATAL_FAILURE(r.trip());   // a tripped lease of the session does not touch a producer that is not bound to it
    rc::Producer p = r.producerUnbound("tx-u");
    ASSERT_EQ(p.state(), TxnState::Ready);                                  // 1: initTransactions
    p.beginTransaction();                                                    // 2: an empty transaction commits with no EndTxn
    r.settle();
    rc::Token c = r.commit(p);
    ASSERT_TRUE(r.await(c));
    EXPECT_EQ(r.got[c].status, OpStatus::Ok);
    EXPECT_EQ(r.endTxnsAny(), 0);
    p.beginTransaction();                                                    // 3: data acked, commit -> one EndTxn(commit)
    r.settle();
    ASSERT_NO_FATAL_FAILURE(r.sendAcked(p));
    c = r.commit(p);
    ASSERT_TRUE(r.await(c));
    EXPECT_EQ(r.got[c].status, OpStatus::Ok);
    EXPECT_EQ(r.endTxns(1), 1);
    p.beginTransaction();                                                    // 4: data acked, abort -> one EndTxn(abort)
    r.settle();
    ASSERT_NO_FATAL_FAILURE(r.sendAcked(p));
    const rc::Token a = r.abort(p);
    ASSERT_TRUE(r.await(a));
    EXPECT_EQ(r.got[a].status, OpStatus::Ok);
    EXPECT_EQ(r.endTxns(0), 1);
    p.beginTransaction();                                                    // 5: commit waits for the unacked data, then sends EndTxn
    r.settle();
    const rc::Token s = r.sendUnacked(p);
    c = r.commit(p);
    r.settle();
    EXPECT_EQ(r.endTxns(1), 1) << "EndTxn(commit) waits for the data";
    EXPECT_EQ(r.got.count(c), 0u);
    r.gw.replyAcks = true;
    const auto held = r.gw.held;
    for (const auto& pr : held) r.gw.ack(pr);
    ASSERT_TRUE(r.await(c));
    EXPECT_EQ(r.got[s].status, OpStatus::Ok);
    EXPECT_EQ(r.got[c].status, OpStatus::Ok);
    EXPECT_EQ(r.endTxns(1), 2);
    EXPECT_EQ(p.state(), TxnState::Ready);
}

// ---- the Guard x transaction-state matrix --------------------------------------------------------------------------------------
namespace {

enum class Event : uint8_t { Trip, Lost };
enum class At : uint8_t {
    InTxnNoRecords,   // begin only
    InTxnAcked,       // begin, a record acked
    InTxnUnacked,     // begin, a record sent and not acked
    CommitAwaitData,  // commit issued, a record still unacked
    CommitEndSent,    // EndTxn(commit) left, answer withheld
    AbortAwaitData,   // abort issued, a record still unacked
    ReadyAfterAbort,  // a transaction aborted before the event, none open
};
struct MxRow {
    Event event;
    At    at;
    int   commitEnds;        // EndTxn(commit) frames at the end
    int   abortEnds;         // EndTxn(abort) frames at the end
    bool  pendingOpFails;    // the op issued before the event (commit / unacked send) completes DeliveryFailed / LeaseAtRisk
};
const char* name(Event e) { return e == Event::Trip ? "Trip" : "Lost"; }
const char* name(At a) {
    switch (a) {
    case At::InTxnNoRecords: return "InTxnNoRecords";
    case At::InTxnAcked: return "InTxnAcked";
    case At::InTxnUnacked: return "InTxnUnacked";
    case At::CommitAwaitData: return "CommitAwaitData";
    case At::CommitEndSent: return "CommitEndSent";
    case At::AbortAwaitData: return "AbortAwaitData";
    case At::ReadyAfterAbort: return "ReadyAfterAbort";
    }
    return "?";
}

}  // namespace

TEST(ProducerTxnGuardRegression, GuardTxnMatrixRegression) {
    std::vector<MxRow> rows;
    for (const Event ev : {Event::Trip, Event::Lost}) {
        //             event at                   commit abort pendingFails
        rows.push_back({ev, At::InTxnNoRecords,   0, 0, false});   // never reached the coordinator: the abort sends nothing
        rows.push_back({ev, At::InTxnAcked,       0, 1, false});
        rows.push_back({ev, At::InTxnUnacked,     0, 1, true});
        rows.push_back({ev, At::CommitAwaitData,  0, 1, true});
        rows.push_back({ev, At::CommitEndSent,    1, 0, false});   // decided by its answer
        rows.push_back({ev, At::AbortAwaitData,   0, 1, false});   // an abort is never blocked
        rows.push_back({ev, At::ReadyAfterAbort,  0, 1, false});
    }
    for (const MxRow& row : rows) {
        SCOPED_TRACE(std::string(name(row.event)) + " x " + name(row.at));
        GRig r;
        r.g_now_base = g_now;
        const auto ref = r.addLease(1);
        rc::Producer p = r.producer("tx-m", ref);
        rc::Token pending = 0;
        p.beginTransaction();
        r.settle();
        switch (row.at) {
        case At::InTxnNoRecords: break;
        case At::InTxnAcked: ASSERT_NO_FATAL_FAILURE(r.sendAcked(p)); break;
        case At::InTxnUnacked: pending = r.sendUnacked(p); break;
        case At::CommitAwaitData:
            (void)r.sendUnacked(p);
            pending = r.commit(p);
            r.settle();
            break;
        case At::CommitEndSent:
            ASSERT_NO_FATAL_FAILURE(r.sendAcked(p));
            r.gw.holdEndTxnReply = true;
            pending = r.commit(p);
            ASSERT_TRUE(r.drive([&] { return r.endTxns(1) == 1; }));
            break;
        case At::AbortAwaitData:
            (void)r.sendUnacked(p);
            pending = r.abort(p);
            r.settle();
            break;
        case At::ReadyAfterAbort: {
            ASSERT_NO_FATAL_FAILURE(r.sendAcked(p));
            const rc::Token a = r.abort(p);
            ASSERT_TRUE(r.await(a));
            ASSERT_EQ(r.got[a].status, OpStatus::Ok);
            break;
        }
        }
        if (row.event == Event::Trip) r.at(r.g_now_base + kAtRiskAfter);
        else r.at(r.g_now_base + kMarginAfter);
        ASSERT_GT(row.event == Event::Trip ? r.cb.entered : r.cb.lost, 0);
        r.settle();
        if (row.pendingOpFails) r.expectFailedAtRisk(pending, "the op issued before the event");
        switch (row.at) {
        case At::CommitEndSent:
            EXPECT_EQ(r.got.count(pending), 0u) << "the sent commit is decided by its answer";
            r.gw.releaseEndTxnReplies();
            ASSERT_TRUE(r.await(pending));
            EXPECT_EQ(r.got[pending].status, OpStatus::Ok);
            break;
        case At::AbortAwaitData:
            ASSERT_TRUE(r.await(pending));
            EXPECT_EQ(r.got[pending].status, OpStatus::Ok) << "an abort completes normally";
            break;
        case At::ReadyAfterAbort: {
            p.beginTransaction();   // allowed: every send fails, the commit is refused
            r.settle();
            const rc::Token s = r.send(p);
            ASSERT_TRUE(r.await(s));
            r.expectFailedAtRisk(s, "send after begin");
            const rc::Token c = r.commit(p);
            ASSERT_TRUE(r.await(c));
            r.expectFailedAtRisk(c, "commit after begin");
            break;
        }
        default: {
            const rc::Token c = r.commit(p);   // the transaction can no longer commit
            ASSERT_TRUE(r.await(c));
            r.expectFailedAtRisk(c, "commit");
            EXPECT_EQ(p.state(), TxnState::AbortableError);
            const rc::Token a = r.abort(p);    // an abort always can
            ASSERT_TRUE(r.await(a));
            EXPECT_EQ(r.got[a].status, OpStatus::Ok);
            EXPECT_EQ(p.state(), TxnState::Ready);
            break;
        }
        }
        EXPECT_EQ(r.endTxns(1), row.commitEnds);
        EXPECT_EQ(r.endTxns(0), row.abortEnds);
    }
}
