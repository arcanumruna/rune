// tests/client/ProducerTxnClientTest.cpp - Phase 2b F: the unified Producer against a real Client on Aeron IPC
// (Invoker mode, the InvokerGateway broker model).  Task 4: broker-assigned ids for every acked producer.
#include "../support/InvokerGateway.h"

#include <rune/client/Client.h>
#include <rune/client/Completions.h>
#include <rune/client/Key.h>
#include <rune/client/TypedTopic.h>
#include <rune/client/producer/Producer.h>
#include <rune/client/producer/ProducerRef.h>
#include <rune/codec/Serialize.h>

#include "gen/position.gen.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <map>
#include <memory>
#include <span>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>
#include <unistd.h>

namespace rc = rune::client;

namespace {

int g_ctr = 0;

struct DirSweeper {
    std::vector<std::string> dirs;
    ~DirSweeper() {
        for (const auto& d : dirs) {
            std::error_code ec;
            std::filesystem::remove_all(d, ec);
        }
    }
};
DirSweeper g_sweeper;

rc::ClientConfig invokerCfg(int32_t in, int32_t eg) {
    rc::ClientConfig c;
    c.gatewayChannel  = "aeron:ipc";
    c.gatewayStreamId = in;
    c.egressChannel   = "aeron:ipc";
    c.egressStreamId  = eg;
    c.dataDir = std::string(std::filesystem::temp_directory_path().string() + "/f_txncl_") + std::to_string(::getpid()) + "_" + std::to_string(g_ctr++);
    g_sweeper.dirs.push_back(c.dataDir);
    c.runnerMode = rc::RunnerMode::Invoker;
    rc::TopicSpec ts;
    ts.name           = "t";
    ts.topicId        = 1;
    ts.partitionCount = 1;
    c.topics.push_back(ts);
    return c;
}

struct Rig {
    rc::Client                   client;
    test_support::InvokerGateway gw;
    explicit Rig(const rc::ClientConfig& c)
        : client(c), gw(c.dataDir + "/aeron", c.gatewayChannel, c.gatewayStreamId, c.egressChannel,
                        c.egressStreamId) {}
    void step() {
        client.doWork();
        gw.pump();
    }
    template <class P>
    bool drive(P&& pred) {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
        while (std::chrono::steady_clock::now() < deadline) {
            step();
            if (pred()) return true;
        }
        return false;
    }
    void connect() {
        ASSERT_TRUE(drive([&] { return gw.ready(); }));
        rc::Completions cq(client);
        ASSERT_EQ(client.connect(cq, 1), rc::OpResult::Accepted);
        bool         done = false;
        rc::OpStatus st   = rc::OpStatus::Closed;
        ASSERT_TRUE(drive([&] {
            cq.poll([&](const rc::Completion& c, std::span<const uint8_t>) {
                if (c.token == 1) {
                    done = true;
                    st   = c.status;
                }
            });
            return done;
        }));
        ASSERT_EQ(st, rc::OpStatus::Ok);
        ASSERT_TRUE(drive([&] { return gw.metadataRequests > 0; }));
    }
};

rc::ProducerConfig ackedCfg() {
    rc::ProducerConfig p;
    p.acks              = rc::Acks::Leader;
    p.lingerUs          = 0;
    p.batchRecords      = 1;
    p.maxInFlight       = 8;
    p.produceTimeoutMs  = 30'000;
    p.deliveryTimeoutMs = 60'000;
    return p;
}

}  // namespace

TEST(ProducerTxnClient, PlainAckedProducerGetsItsIdFromTheGatewayBeforeItsFirstProduce) {
    Rig r(invokerCfg(4701, 4702));
    r.connect();
    r.gw.idempotence = true;
    rc::Producer p = r.client.producer(ackedCfg());
    EXPECT_EQ(p.producerId(), -1) << "no id before the broker assigned one";
    const rc::TopicHandle topic = r.client.topic("t");
    const uint64_t        v     = 42;
    // Sent at once, before any pass ran: the record is held, not sent with a guessed id.
    ASSERT_EQ(p.trySend(topic, rc::Key(v), std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(&v), 8)),
              rc::SendResult::Ok);
    ASSERT_TRUE(r.drive([&] { return !r.gw.log.empty(); })) << "the held record is sent once the id arrived";
    EXPECT_EQ(r.gw.plainInitPids, 1) << "exactly one plain InitProducerId";
    ASSERT_EQ(r.gw.log.size(), 1u);
    const int64_t assigned = r.gw.log[0].producerId;
    EXPECT_GE(assigned, 7000) << "the id on the wire is the gateway's answer, not a client-drawn one";
    EXPECT_LT(assigned, 7100);
    EXPECT_EQ(r.gw.log[0].producerEpoch, 0);
    EXPECT_EQ(r.gw.log[0].baseSequence, 0);
    EXPECT_EQ(p.producerId(), assigned) << "the handle reports the broker-assigned id (FD9)";
    EXPECT_EQ(p.producerEpoch(), 0);

    // A second producer on the same client gets a different id.
    rc::Producer p2 = r.client.producer(ackedCfg());
    ASSERT_EQ(p2.trySend(topic, rc::Key(v), std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(&v), 8)),
              rc::SendResult::Ok);
    ASSERT_TRUE(r.drive([&] { return r.gw.log.size() >= 2; }));
    EXPECT_EQ(r.gw.plainInitPids, 2);
    EXPECT_NE(r.gw.log[1].producerId, assigned);
}

TEST(ProducerTxnClientRegression, HeldCompletionsRecordsFailAtDeliveryTimeoutAndCloseIsBoundedRegression) {
    Rig r(invokerCfg(4703, 4704));
    r.connect();
    r.gw.plainInitPidAnswers = false;   // a broker that never answers the id request
    rc::ProducerConfig pc = ackedCfg();
    pc.batchRecords       = 100;   // both records join one builder
    pc.produceTimeoutMs   = 100;
    pc.deliveryTimeoutMs  = 400;
    rc::Producer p = r.client.producer(pc);
    rc::Completions cq(r.client);
    const rc::TopicHandle topic = r.client.topic("t");
    const uint64_t        v     = 7;
    const std::span<const uint8_t> val(reinterpret_cast<const uint8_t*>(&v), 8);
    ASSERT_EQ(p.trySend(topic, rc::Key(v), val, cq, 1), rc::SendResult::Ok);
    ASSERT_EQ(p.trySend(topic, rc::Key(v), val, cq, 4), rc::SendResult::Ok);
    int            done1 = 0, done4 = 0;
    rc::OpStatus   st1   = rc::OpStatus::Ok;
    int            closed = 0;
    ASSERT_TRUE(r.drive([&] {
        cq.poll([&](const rc::Completion& c, std::span<const uint8_t>) {
            if (c.token == 1) { ++done1; st1 = c.status; }
            if (c.token == 4) ++done4;
            if (c.token == 9) ++closed;
        });
        return done1 > 0;
    })) << "a held record must complete, not hang";
    EXPECT_EQ(done1, 1);
    EXPECT_EQ(st1, rc::OpStatus::DeliveryFailed);
    EXPECT_LE(done4, 1);
    EXPECT_GE(r.gw.plainInitPids, 1);
    EXPECT_TRUE(r.gw.log.empty()) << "never sent without an id";
    // Close with a fresh held record: bounded by the delivery timeout.
    ASSERT_EQ(p.trySend(topic, rc::Key(v), val, cq, 3), rc::SendResult::Ok);
    ASSERT_EQ(p.close(cq, 9), rc::OpResult::Accepted);
    ASSERT_TRUE(r.drive([&] {
        cq.poll([&](const rc::Completion& c, std::span<const uint8_t>) {
            if (c.token == 9) ++closed;
        });
        return closed > 0;
    })) << "close() with held records completes";
}

TEST(ProducerTxnClientRegression, StatusGettersOffTheBoundThreadAreNotAvailableRegression) {
    Rig r(invokerCfg(4705, 4706));
    r.connect();
    rc::Producer p = r.client.producer(ackedCfg());
    ASSERT_TRUE(r.drive([&] { return p.producerId() >= 0; }));
    const int64_t id = p.producerId();
    int64_t off = 0;
    int16_t offEp = 0;
    std::thread t([&] { off = p.producerId(); offEp = p.producerEpoch(); });
    t.join();
    EXPECT_EQ(off, -1);
    EXPECT_EQ(offEp, -1);
    EXPECT_EQ(p.producerId(), id);
}

// ---------------------------------------------------------------------------------------------------------------
// Phase 2b F Task 8: the transactional handle over a real Client (BOTH runner modes) and the txn-aware gateway.
// ---------------------------------------------------------------------------------------------------------------
namespace {

int32_t g_stream = 4800;

struct TxnRig {
    bool                         invoker;
    std::unique_ptr<rc::Client>  client;   // constructed first: owns the driver
    test_support::InvokerGateway gw;       // destroyed first
    std::map<rc::Token, rc::Completion> got;

    static rc::ClientConfig cfg(rc::RunnerMode mode, int32_t in, int32_t eg) {
        rc::ClientConfig c = invokerCfg(in, eg);
        c.runnerMode       = mode;
        return c;
    }
    explicit TxnRig(rc::RunnerMode mode, int32_t in = g_stream, int32_t eg = g_stream + 1)
        : TxnRig(cfg(mode, in, eg)) {
        g_stream += 2;
    }
    explicit TxnRig(const rc::ClientConfig& c)
        : invoker(c.runnerMode == rc::RunnerMode::Invoker),
          client(std::make_unique<rc::Client>(c)),
          gw(c.dataDir + "/aeron", c.gatewayChannel, c.gatewayStreamId, c.egressChannel, c.egressStreamId) {
        gw.idempotence = true;   // acked producers: the broker model dedups and keeps the log
        gw.initPidReply = true;  // the transactional InitProducerId is answered
        gw.replyTxn     = true;  // AddPartitions / AddOffsets / TxnOffsetCommit / EndTxn answered
    }
    ~TxnRig() {
        if (client) client->close();
    }
    template <class P>
    bool drive(P&& pred) {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
        while (std::chrono::steady_clock::now() < deadline) {
            if (invoker) client->doWork();   // Thread mode: the runner works by itself
            gw.pump();
            if (pred()) return true;
            if (!invoker) std::this_thread::yield();
        }
        return false;
    }
    void connect() {
        ASSERT_TRUE(drive([&] { return gw.ready(); }));
        rc::Completions cq(*client);
        ASSERT_EQ(client->connect(cq, 1), rc::OpResult::Accepted);
        bool done = false;
        ASSERT_TRUE(drive([&] {
            cq.poll([&](const rc::Completion& c, std::span<const uint8_t>) { if (c.token == 1) done = true; });
            return done;
        }));
        ASSERT_TRUE(drive([&] { return gw.metadataRequests > 0; }));
    }
    /// Poll `cq` into `got` until `tok` completed.
    bool await(rc::Completions& cq, rc::Token tok) {
        return drive([&] {
            cq.poll([&](const rc::Completion& c, std::span<const uint8_t>) { got[c.token] = c; });
            return got.count(tok) != 0;
        });
    }
    /// Run `fn` (a blocking *AndWait call on this thread) while another thread pumps the gateway until `pumped()`
    /// held for a while (deadline bounded).
    template <class Pred, class Fn>
    void whilePumping(Pred&& pumped, Fn&& fn) {
        std::thread t([&] {
            const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
            int extra = 0;
            while (std::chrono::steady_clock::now() < deadline) {
                gw.pump();
                if (pumped() && ++extra > 3000) break;
            }
        });
        try {
            fn();
        } catch (...) {
            t.join();
            throw;
        }
        t.join();
    }
};

rc::ProducerConfig txnCfg() {
    rc::ProducerConfig p;
    p.acks              = rc::Acks::Leader;
    p.transactionalId   = "tx-1";
    p.lingerUs          = 0;
    p.batchRecords      = 100;
    p.maxInFlight       = 8;
    p.produceTimeoutMs  = 30'000;
    p.deliveryTimeoutMs = 60'000;
    return p;
}

std::span<const uint8_t> bytesOf(const std::string& s) {
    return {reinterpret_cast<const uint8_t*>(s.data()), s.size()};
}

std::vector<std::string> strs(const std::vector<std::vector<uint8_t>>& vs) {
    std::vector<std::string> out;
    for (const auto& v : vs) out.emplace_back(v.begin(), v.end());
    return out;
}

const char* modeName(const ::testing::TestParamInfo<rc::RunnerMode>& i) {
    return i.param == rc::RunnerMode::Thread ? "Thread" : "Invoker";
}

/// Init + begin on a fresh transactional producer (token init, void begin).
void initAndBegin(TxnRig& r, rc::Producer& p, rc::Completions& cq, rc::Token tok = 100) {
    ASSERT_EQ(p.initTransactions(cq, tok), rc::OpResult::Accepted);
    ASSERT_TRUE(r.await(cq, tok));
    ASSERT_EQ(r.got[tok].status, rc::OpStatus::Ok) << "errorCode " << r.got[tok].errorCode;
    p.beginTransaction();
}

// Let the duty side and the gateway run: Direct (Invoker) producers hold a path until the pass before it was sent.
void settle(TxnRig& r, int passes = 400) {
    int n = 0;
    ASSERT_TRUE(r.drive([&] { return ++n >= passes; }));
}

// Sends one record on every produce path; returns the values it wrote, in no particular cross-path order.
std::vector<std::string> sendEveryPath(TxnRig& r, rc::Producer& p, rc::TopicHandle topic, int tag) {
    std::vector<std::string> sent;
    const rc::Key key(uint64_t{9});
    const std::string t = std::to_string(tag);
    {   // send (void)
        const std::string v = "send-" + t;
        p.send(topic, key, bytesOf(v));
        sent.push_back(v);
        settle(r);
    }
    {   // trySend
        const std::string v = "trysend-" + t;
        EXPECT_EQ(p.trySend(topic, key, bytesOf(v)), rc::SendResult::Ok);
        sent.push_back(v);
    }
    {   // Tier-2 batch
        const std::string a = "batch-a-" + t, b = "batch-b-" + t;
        rc::BatchBuilder bb = p.batch(topic, key);
        bb.add(bytesOf(a)).add(bytesOf(b));
        bb.send();
        sent.push_back(a);
        sent.push_back(b);
        settle(r);
    }
    {   // tryClaim
        const std::string v = "claim-" + t;
        rc::Claim c = p.tryClaim(topic, key, static_cast<uint32_t>(v.size()));
        for (int i = 0; !c && c.result() == rc::SendResult::BackPressured && i < 200; ++i) {
            settle(r, 20);
            c = p.tryClaim(topic, key, static_cast<uint32_t>(v.size()));
        }
        EXPECT_TRUE(c) << static_cast<int>(c.result());
        if (c) {
            std::memcpy(c.buffer().data(), v.data(), v.size());
            EXPECT_TRUE(c.commit());
            sent.push_back(v);
        }
    }
    {   // claimBatch
        rc::BatchClaim bc = p.claimBatch(topic, key, 2, 8);
        for (int i = 0; !bc && bc.result() == rc::SendResult::BackPressured && i < 200; ++i) {
            settle(r, 20);
            bc = p.claimBatch(topic, key, 2, 8);
        }
        EXPECT_TRUE(bc) << static_cast<int>(bc.result());
        if (bc) {
            const std::string a = "cb1-" + t + "...", b = "cb2-" + t + "...";
            std::memcpy(bc.record(0).data(), a.data(), 8);
            std::memcpy(bc.record(1).data(), b.data(), 8);
            EXPECT_TRUE(bc.commit());
            sent.push_back(a.substr(0, 8));
            sent.push_back(b.substr(0, 8));
        }
    }
    settle(r);
    return sent;
}

using PositionTopic = rc::TypedTopic<rune::gen::Position, rc::ProducerRef>;

std::string positionBytes(const rune::gen::Position& rec) {
    std::string out(rec.encodedSize(), '\0');
    rec.encode(reinterpret_cast<uint8_t*>(out.data()));
    return out;
}

}  // namespace

class ProducerTxnClientModes : public ::testing::TestWithParam<rc::RunnerMode> {};

INSTANTIATE_TEST_SUITE_P(Modes, ProducerTxnClientModes,
                         ::testing::Values(rc::RunnerMode::Thread, rc::RunnerMode::Invoker), modeName);

TEST_P(ProducerTxnClientModes, InitBeginSendCommitIsVisibleToReadCommitted) {
    TxnRig r(GetParam());
    r.connect();
    rc::Producer          p = r.client->producer(txnCfg());
    rc::Completions       cq(*r.client);
    const rc::TopicHandle topic = r.client->topic("t");
    ASSERT_NO_FATAL_FAILURE(initAndBegin(r, p, cq));
    const rc::Key key(uint64_t{1});
    for (const char* v : {"r1", "r2", "r3"}) {
        const std::string s = v;
        ASSERT_EQ(p.trySend(topic, key, bytesOf(s)), rc::SendResult::Ok);
    }
    ASSERT_EQ(p.commitTransaction(cq, 101), rc::OpResult::Accepted);
    ASSERT_TRUE(r.await(cq, 101));
    EXPECT_EQ(r.got[101].status, rc::OpStatus::Ok);
    EXPECT_EQ(strs(r.gw.committedValues(0)), (std::vector<std::string>{"r1", "r2", "r3"}));
    EXPECT_EQ(r.gw.visibleBatches(0, test_support::InvokerGateway::TxnFate::Open), 0u) << "nothing left undecided";
    EXPECT_GE(r.gw.addPartitions, 1);
    EXPECT_EQ(r.gw.endTxnCommits, (std::vector<uint8_t>{1}));
}

TEST_P(ProducerTxnClientModes, RecordsSentBeforeCommitAreInItAfterAreRefusedRegression) {
    TxnRig r(GetParam());
    r.connect();
    rc::Producer          p = r.client->producer(txnCfg());
    rc::Completions       cq(*r.client);
    const rc::TopicHandle topic = r.client->topic("t");
    ASSERT_NO_FATAL_FAILURE(initAndBegin(r, p, cq));
    const rc::Key key(uint64_t{1});
    for (int i = 0; i < 1000; ++i) {
        const std::string v = "v" + std::to_string(i);
        rc::SendResult res = p.trySend(topic, key, bytesOf(v));
        while (res == rc::SendResult::BackPressured) {   // a full ring: let the duty side drain
            ASSERT_TRUE(r.drive([&] { return true; }));
            res = p.trySend(topic, key, bytesOf(v));
        }
        ASSERT_EQ(res, rc::SendResult::Ok) << i;
    }
    ASSERT_EQ(p.commitTransaction(cq, 101), rc::OpResult::Accepted);
    const std::string late = "late";
    EXPECT_EQ(p.trySend(topic, key, bytesOf(late)), rc::SendResult::NotInTransaction)
        << "the gate closed when the commit was issued";
    EXPECT_THROW(p.send(topic, key, bytesOf(late)), std::logic_error);
    ASSERT_TRUE(r.await(cq, 101));
    EXPECT_EQ(r.got[101].status, rc::OpStatus::Ok);
    const auto committed = strs(r.gw.committedValues(0));
    ASSERT_EQ(committed.size(), 1000u) << "the commit holds exactly the records issued before it";
    for (int i = 0; i < 1000; ++i) ASSERT_EQ(committed[static_cast<std::size_t>(i)], "v" + std::to_string(i)) << i;
}

TEST_P(ProducerTxnClientModes, AbortMakesEveryPathInvisible) {
    TxnRig r(GetParam());
    r.gw.replySchemas  = true;
    r.gw.schemaTopicId = 1;
    r.gw.schemaBlob    = rune::codec::serialize(rune::gen::Position::descriptor());
    r.connect();
    rc::Producer          p = r.client->producer(txnCfg());
    rc::Completions       cq(*r.client);
    const rc::TopicHandle topic = r.client->topic("t");
    ASSERT_NO_FATAL_FAILURE(initAndBegin(r, p, cq));
    const std::size_t sent = sendEveryPath(r, p, topic, 1).size();
    PositionTopic typed{rc::ProducerRef(p), topic, uint16_t{1}};   // typed send + claim on the SAME producer
    rune::gen::Position rec;
    rec.userId = 5;
    typed.send(rec);
    settle(r);
    {
        auto w = typed.claim(int64_t{6});
        ASSERT_TRUE(w) << static_cast<int>(w.result());
        w.zone(3);
        ASSERT_TRUE(w.commit());
    }
    ASSERT_NO_FATAL_FAILURE(settle(r));   // everything is on the broker; an abort discards what is still unsent
    ASSERT_EQ(p.abortTransaction(cq, 102), rc::OpResult::Accepted);
    ASSERT_TRUE(r.await(cq, 102));
    EXPECT_EQ(r.got[102].status, rc::OpStatus::Ok);
    EXPECT_TRUE(r.gw.committedValues(0).empty()) << "nothing of an aborted transaction is visible";
    EXPECT_EQ(r.gw.visibleBatches(0, test_support::InvokerGateway::TxnFate::Committed), 0u);
    EXPECT_EQ(r.gw.visibleBatches(0, test_support::InvokerGateway::TxnFate::Open), 0u);
    std::size_t aborted = 0;
    for (const auto& b : r.gw.log) {
        if (b.fate == test_support::InvokerGateway::TxnFate::Aborted) aborted += b.values.size();
    }
    EXPECT_EQ(aborted, sent + 2) << "every path reached the broker in the transaction, and the abort decided them";
    EXPECT_EQ(r.gw.endTxnCommits, (std::vector<uint8_t>{0}));
    // The producer is usable again: a second transaction commits.
    p.beginTransaction();
    const std::string again = "again";
    ASSERT_EQ(p.trySend(topic, rc::Key(uint64_t{1}), bytesOf(again)), rc::SendResult::Ok);
    ASSERT_EQ(p.commitTransaction(cq, 103), rc::OpResult::Accepted);
    ASSERT_TRUE(r.await(cq, 103));
    EXPECT_EQ(r.got[103].status, rc::OpStatus::Ok);
    EXPECT_EQ(strs(r.gw.committedValues(0)), (std::vector<std::string>{"again"}));
}

TEST_P(ProducerTxnClientModes, MixedPathsInOneTransactionCommitTogether) {
    TxnRig r(GetParam());
    r.connect();
    rc::Producer          p = r.client->producer(txnCfg());
    rc::Completions       cq(*r.client);
    const rc::TopicHandle topic = r.client->topic("t");
    ASSERT_NO_FATAL_FAILURE(initAndBegin(r, p, cq));
    PositionTopic typed{rc::ProducerRef(p), topic, uint16_t{1}};
    std::vector<std::string> expect = sendEveryPath(r, p, topic, 7);
    rune::gen::Position rec;
    rec.userId = 5;
    rec.zone   = 1;
    typed.send(rec);
    expect.push_back(positionBytes(rec));
    ASSERT_EQ(p.commitTransaction(cq, 104), rc::OpResult::Accepted);
    ASSERT_TRUE(r.await(cq, 104));
    EXPECT_EQ(r.got[104].status, rc::OpStatus::Ok);
    auto seen = strs(r.gw.committedValues(0));
    std::sort(seen.begin(), seen.end());
    std::sort(expect.begin(), expect.end());
    EXPECT_EQ(seen, expect) << "every path's records are in the one commit, none twice";
}

TEST_P(ProducerTxnClientModes, AndWaitFormsThrowWithTheOutcome) {
    {   // Fenced
        TxnRig r(GetParam());
        r.connect();
        r.gw.initPidCodes.push_back(static_cast<int16_t>(rune::protocol::client::ErrorCode::ProducerFenced));
        rc::Producer p = r.client->producer(txnCfg());
        std::string  what;
        r.whilePumping([&] { return r.gw.initPids >= 1; }, [&] {
            try {
                p.initTransactionsAndWait(std::chrono::seconds(8));
            } catch (const std::runtime_error& e) {
                what = e.what();
            }
        });
        EXPECT_NE(what.find("ProducerFenced"), std::string::npos) << what;
        EXPECT_EQ(p.state(), rc::TxnState::FatalError);
        // Every later op refuses with the same outcome; the plain-state getters stay readable.
        std::string again;
        try {
            p.commitTransactionAndWait(std::chrono::seconds(8));
        } catch (const std::runtime_error& e) {
            again = e.what();
        }
        EXPECT_NE(again.find("ProducerFenced"), std::string::npos) << again;
    }
    {   // Timeout: nothing answers
        TxnRig r(GetParam());
        r.connect();
        rc::Producer p = r.client->producer(txnCfg());
        std::string  what;
        try {
            p.initTransactionsAndWait(std::chrono::milliseconds(300));
        } catch (const std::runtime_error& e) {
            what = e.what();
        }
        EXPECT_NE(what.find("timed out"), std::string::npos) << what;
    }
    {   // A plain producer has no lifecycle
        TxnRig r(GetParam());
        r.connect();
        rc::Producer p = r.client->producer(ackedCfg());
        EXPECT_THROW(p.commitTransactionAndWait(std::chrono::milliseconds(50)), std::logic_error);
        EXPECT_THROW(p.beginTransaction(), std::logic_error);
    }
}

TEST_P(ProducerTxnClientModes, StatusAccessorsFollowTheMachine) {
    TxnRig r(GetParam());
    r.connect();
    rc::Producer          p = r.client->producer(txnCfg());
    rc::Completions       cq(*r.client);
    const rc::TopicHandle topic = r.client->topic("t");
    EXPECT_EQ(p.state(), rc::TxnState::Uninitialized);
    EXPECT_FALSE(p.transactionActive());
    ASSERT_EQ(p.initTransactions(cq, 100), rc::OpResult::Accepted);
    ASSERT_TRUE(r.await(cq, 100));
    ASSERT_TRUE(r.drive([&] { return p.state() == rc::TxnState::Ready; }));
    EXPECT_EQ(p.producerId(), r.gw.initPid);
    EXPECT_EQ(p.producerEpoch(), 0);
    EXPECT_FALSE(p.transactionActive());
    p.beginTransaction();
    ASSERT_TRUE(r.drive([&] { return p.state() == rc::TxnState::InTransaction; }));
    EXPECT_TRUE(p.transactionActive());
    const std::string v = "x";
    ASSERT_EQ(p.trySend(topic, rc::Key(uint64_t{1}), bytesOf(v)), rc::SendResult::Ok);
    // The getters are bound to the batch thread: anywhere else they report "not available" and touch nothing.
    int64_t        offId = 0;
    int16_t        offEp = 0;
    rc::TxnState   offSt = rc::TxnState::Ready;
    bool           offAct = true;
    std::thread t([&] {
        offId  = p.producerId();
        offEp  = p.producerEpoch();
        offSt  = p.state();
        offAct = p.transactionActive();
    });
    t.join();
    EXPECT_EQ(offId, -1);
    EXPECT_EQ(offEp, -1);
    EXPECT_EQ(offSt, rc::TxnState::Uninitialized);
    EXPECT_FALSE(offAct);
    ASSERT_EQ(p.commitTransaction(cq, 101), rc::OpResult::Accepted);
    ASSERT_TRUE(r.await(cq, 101));
    ASSERT_TRUE(r.drive([&] { return p.state() == rc::TxnState::Ready; }));
    EXPECT_FALSE(p.transactionActive());
    EXPECT_EQ(p.producerId(), r.gw.initPid);
}

TEST_P(ProducerTxnClientModes, TypedTopicOverProducerRefWritesInsideTheTransaction) {
    TxnRig r(GetParam());
    r.gw.replySchemas  = true;
    r.gw.schemaTopicId = 1;
    r.gw.schemaBlob    = rune::codec::serialize(rune::gen::Position::descriptor());
    r.connect();
    rc::Producer    p = r.client->producer(txnCfg());
    rc::Completions cq(*r.client);
    ASSERT_NO_FATAL_FAILURE(initAndBegin(r, p, cq));
    std::unique_ptr<PositionTopic> typed;
    r.whilePumping([&] { return r.gw.schemaGets >= 1; }, [&] {
        typed = std::make_unique<PositionTopic>(r.client->openTopicAndWait<rune::gen::Position>("t", p));
    });
    ASSERT_TRUE(typed);
    rune::gen::Position a, b;
    a.userId = 1;
    a.zone   = 10;
    b.userId = 2;
    b.zone   = 20;
    typed->send(a);
    settle(r);
    {
        auto w = typed->claim(int64_t{2});
        ASSERT_TRUE(w);
        w.zone(20);
        ASSERT_TRUE(w.commit());
    }
    ASSERT_EQ(p.commitTransaction(cq, 101), rc::OpResult::Accepted);
    EXPECT_THROW(typed->send(b), std::logic_error) << "outside the transaction the typed topic is refused too";
    {
        auto w = typed->claim(int64_t{3});
        EXPECT_FALSE(w);
        EXPECT_EQ(w.result(), rc::SendResult::NotInTransaction);
    }
    ASSERT_TRUE(r.await(cq, 101));
    EXPECT_EQ(r.got[101].status, rc::OpStatus::Ok);
    EXPECT_EQ(r.gw.committedValues(0).size(), 2u) << "the typed send and the typed claim are in the commit";
}

// Invoker mode: an op off the owner thread is WrongThread, applies nothing, and returns its credit.
namespace {
void ignoreCompletion(void*, const rc::Completion&, std::span<const uint8_t>) noexcept {}
}  // namespace

TEST(ProducerTxnClientRegression, InvokerOpsOffTheOwnerAreWrongThreadRegression) {
    TxnRig r(rc::RunnerMode::Invoker);
    r.connect();
    rc::Producer    p = r.client->producer(txnCfg());
    rc::Completions cq(*r.client);   // owned by THIS thread
    const rc::CompletionHandler h{&ignoreCompletion, nullptr};
    constexpr int   kOps = 5;
    rc::OpResult    viaHandler[kOps]{};
    rc::OpResult    viaCq[kOps]{};
    bool            threw = false;
    std::thread t([&] {
        const rc::OffsetEntry e{1, 0, 5};
        const std::span<const rc::OffsetEntry> es(&e, 1);
        viaHandler[0] = p.initTransactions(h, 1);
        viaHandler[1] = p.commitTransaction(h, 2);
        viaHandler[2] = p.abortTransaction(h, 3);
        viaHandler[3] = p.flush(h, 4);
        viaHandler[4] = p.sendOffsetsToTransaction(es, "g", h, 5);
        viaCq[0] = p.initTransactions(cq, 1);
        viaCq[1] = p.commitTransaction(cq, 2);
        viaCq[2] = p.abortTransaction(cq, 3);
        viaCq[3] = p.flush(cq, 4);
        viaCq[4] = p.sendOffsetsToTransaction(es, "g", cq, 5);
        try {
            p.beginTransaction();
        } catch (const std::logic_error&) {
            threw = true;
        }
    });
    t.join();
    for (int i = 0; i < kOps; ++i) {
        EXPECT_EQ(viaHandler[i], rc::OpResult::WrongThread) << i;
        EXPECT_EQ(viaCq[i], rc::OpResult::WrongThread) << i;
    }
    EXPECT_EQ(cq.reservedBytes(), 0u) << "no credit was kept by a refused op";
    EXPECT_TRUE(threw);
    ASSERT_TRUE(r.drive([&] { return true; }));
    EXPECT_EQ(p.state(), rc::TxnState::Uninitialized) << "nothing was applied";
    EXPECT_EQ(r.gw.initPids, 0);
}

// C1: the largest legal sendOffsets (255-byte group id, 1024 offsets) is a 16680-byte control message.
TEST_P(ProducerTxnClientModes, MaxGroupIdAndMaxOffsetsFitTheControlBufferRegression) {
    TxnRig r(GetParam());
    r.connect();
    rc::Producer    p = r.client->producer(txnCfg());
    rc::Completions cq(*r.client);
    ASSERT_NO_FATAL_FAILURE(initAndBegin(r, p, cq));
    const std::string group(255, 'g');
    std::vector<rc::OffsetEntry> offs(1024);
    for (std::size_t i = 0; i < offs.size(); ++i) offs[i] = rc::OffsetEntry{1, static_cast<int32_t>(i % 5), static_cast<int64_t>(1000 + i)};
    ASSERT_EQ(p.sendOffsetsToTransaction(offs, group, cq, 105), rc::OpResult::Accepted);
    ASSERT_TRUE(r.await(cq, 105));
    EXPECT_EQ(r.got[105].status, rc::OpStatus::Ok) << "errorCode " << r.got[105].errorCode;
    EXPECT_EQ(r.gw.lastTxnOffsetGroup, group);
    ASSERT_EQ(r.gw.pendingTxnOffsets.size(), 1u) << "one producer's staged offsets";
    const auto& staged = r.gw.pendingTxnOffsets.begin()->second;
    ASSERT_EQ(staged.size(), 1024u) << "every entry reached the coordinator, in order";
    for (std::size_t i = 0; i < offs.size(); ++i) {
        EXPECT_EQ(std::get<2>(staged[i]), 1000 + static_cast<int64_t>(i)) << i;
    }
    ASSERT_EQ(p.commitTransaction(cq, 106), rc::OpResult::Accepted);
    ASSERT_TRUE(r.await(cq, 106));
    EXPECT_EQ(r.got[106].status, rc::OpStatus::Ok);
}

// I2: a queue released while its control op is still in the producer ring keeps its registry slot quarantined
// (C6 producer-op accounting) until the op's completion is delivered or dropped; then the slot comes back.
// RED (mutation): no count at issue -> the slot is reused at once; a double count -> it never comes back.
TEST(ProducerTxnClientRegression, ReleasedQueueWithAControlOpInTheRingKeepsItsSlotQuarantinedRegression) {
    TxnRig r(rc::RunnerMode::Invoker);
    r.connect();
    rc::CompletionsConfig tiny;
    tiny.capacityBytes  = 1024;
    tiny.maxResultBytes = 64;
    tiny.maxErrorText   = 64;
    std::vector<std::unique_ptr<rc::Completions>> live;   // fill the registry so a freed slot is the next taken
    for (uint32_t i = 0; i < rc::kMaxCqs - 2; ++i) live.push_back(std::make_unique<rc::Completions>(*r.client, tiny));
    rc::Producer p = r.client->testProducerWithRingCap(txnCfg(), 64 * 1024);   // Ring ingress: ops ride the ring
    auto a = std::make_unique<rc::Completions>(*r.client, tiny);
    const uint32_t aIdx = rc::cqRefIndex(a->cqRef());
    ASSERT_EQ(p.commitTransaction(*a, 1), rc::OpResult::Accepted);   // sits in the ring: no duty pass has run
    a.reset();   // released with the op still owed
    for (int i = 0; i < 64; ++i) {
        rc::Completions churn(*r.client, tiny);
        EXPECT_NE(rc::cqRefIndex(churn.cqRef()), aIdx) << "quarantined slot reused at churn " << i;
    }
    const uint64_t stale0 = r.client->testStaleCqDrops();
    ASSERT_TRUE(r.drive([&] { return r.client->testStaleCqDrops() > stale0; })) << "the op completes into the released queue";
    for (int i = 0; i < 1'000; ++i) { r.client->doWork(); r.gw.pump(); }
    bool reused = false;
    for (int i = 0; i < 8 && !reused; ++i) {
        rc::Completions churn(*r.client, tiny);
        reused = rc::cqRefIndex(churn.cqRef()) == aIdx;
    }
    EXPECT_TRUE(reused) << "the slot returns once the op's completion is accounted";
}

// A broker that keeps the session id on a reconnect (auth Required: resumeAuthenticated_) still lost the in-flight
// coordinator request with the old connection: the client must re-send it, not wait out its deadline.
// RED (mutation): Client::syncCoordinatorSession_ keyed on the session id alone -> the AddOffsets is never re-sent.
TEST(CoordinatorLinkRegression, ReconnectWithTheSameSessionIdResendsInFlightRequestsRegression) {
    TxnRig r(rc::RunnerMode::Invoker);
    r.gw.uniqueSessionIds = true;    // ids are kept ...
    r.gw.newIdOnResume    = false;   // ... on a resume: the SAME id comes back
    r.connect();
    rc::Producer    p = r.client->producer(txnCfg());
    rc::Completions cq(*r.client);
    ASSERT_NO_FATAL_FAILURE(initAndBegin(r, p, cq));
    const rc::Key key(uint64_t{1});
    const std::string v = "x";
    ASSERT_EQ(p.trySend(r.client->topic("t"), key, bytesOf(v)), rc::SendResult::Ok);
    settle(r);
    const int64_t sid = r.gw.lastUniqueSessionId();
    r.gw.holdTxnReplies = true;   // the offsets leg is processed, its answer never leaves
    const rc::OffsetEntry e{1, 0, 7};
    ASSERT_EQ(p.sendOffsetsToTransaction(std::span<const rc::OffsetEntry>(&e, 1), "g", cq, 105), rc::OpResult::Accepted);
    ASSERT_TRUE(r.drive([&] { return r.gw.txnRequestsUnanswered >= 1; }));
    const int connects = r.gw.connects;
    const int before   = r.gw.addOffsets;
    r.gw.expireSession(sid);
    r.gw.holdTxnReplies = false;
    ASSERT_TRUE(r.drive([&] { return r.gw.connects > connects; })) << "no reconnect";
    ASSERT_EQ(r.gw.lastUniqueSessionId(), sid) << "precondition: the broker kept the session id";
    ASSERT_TRUE(r.await(cq, 105)) << "the in-flight request was not re-sent (addOffsets " << r.gw.addOffsets << ")";
    EXPECT_GT(r.gw.addOffsets, before);
    EXPECT_EQ(r.got[105].status, rc::OpStatus::Ok);
}
