// tests/client/ProducerTxnClientTest.cpp - Phase 2b F: the unified Producer against a real Client on Aeron IPC
// (Invoker mode, the InvokerGateway broker model).  Task 4: broker-assigned ids for every acked producer.
#include "../support/FreePort.h"
#include "../support/InvokerGateway.h"

#include <rune/client/Client.h>
#include <rune/client/Completions.h>
#include <rune/client/Key.h>
#include <rune/client/group/GroupMetadata.h>
#include <rune/client/TypedTopic.h>
#include <rune/client/producer/Producer.h>
#include <rune/client/producer/ProducerRef.h>
#include <rune/protocol/client/TxnMessages.h>
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
#include <poll.h>
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
        EXPECT_EQ(bb.send(), rc::OpResult::Accepted);
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

// ---------------------------------------------------------------------------------------------------------------
// #350: the control-op handle paths (Thread mode CQ credit, begin back-pressure, begin after an unfinished end)
// ---------------------------------------------------------------------------------------------------------------
namespace {
/// Parks the Thread-mode runner INSIDE a completion callback until opened, so the producer ring cannot drain: the
/// test thread then fills it deterministically (no timing, no sleeps).  Declare it BEFORE the rig and an Opener AFTER
/// it, so the runner is always released before the Client closes.
/// Pipes, not shared sync (the test-tree law, check_no_atomics_or_mutexes --tests): the runner writes 'P' into
/// toTest when it parked and blocks reading toRunner; open() writes 'G' there.  Each side owns its own ends; the
/// kernel orders the bytes.  The runner's read has a failure-only deadline (it never decides an outcome).
struct RunnerGate {
    int  toTest[2]   = {-1, -1};
    int  toRunner[2] = {-1, -1};
    bool piped       = false;   // test thread only
    bool opened      = false;   // test thread only
    RunnerGate() { piped = ::pipe(toTest) == 0 && ::pipe(toRunner) == 0; }
    ~RunnerGate() {
        for (int fd : {toTest[0], toTest[1], toRunner[0], toRunner[1]})
            if (fd >= 0) ::close(fd);
    }
    RunnerGate(const RunnerGate&)            = delete;
    RunnerGate& operator=(const RunnerGate&) = delete;
    static void on(void* ctx, const rc::Completion&, std::span<const uint8_t>) noexcept {
        auto*      g = static_cast<RunnerGate*>(ctx);
        const char c = 'P';
        (void)!::write(g->toTest[1], &c, 1);
        pollfd pf{g->toRunner[0], POLLIN, 0};
        char   in = 0;
        if (::poll(&pf, 1, 120'000) == 1) (void)!::read(g->toRunner[0], &in, 1);
    }
    [[nodiscard]] rc::CompletionHandler handler() noexcept { return rc::CompletionHandler{&RunnerGate::on, this}; }
    /// Pumps the gateway (capped) until the runner reported it parked.
    template <class Rig>
    [[nodiscard]] bool waitEntered(Rig& r, int cap = 20'000'000) {
        if (!piped) return false;
        for (int i = 0; i < cap; ++i) {
            r.gw.pump();
            pollfd pf{toTest[0], POLLIN, 0};
            char   c = 0;
            if (::poll(&pf, 1, 0) == 1 && ::read(toTest[0], &c, 1) == 1) return c == 'P';
            std::this_thread::yield();
        }
        return false;
    }
    void open() {
        if (!opened && piped) {
            opened       = true;
            const char c = 'G';
            (void)!::write(toRunner[1], &c, 1);
        }
    }
};
struct GateOpener {
    RunnerGate& g;
    ~GateOpener() { g.open(); }
};

/// Polls `cq` until `n` completions arrived (all kept, in order).  cap < 0 (the default, a positive wait): until they
/// arrived, with a failure-only 60 s deadline (a Thread-mode runner on a loaded host is not bounded by a pass count).
/// cap >= 0 (a negative check: nothing more arrives): exactly `cap` passes.
bool collect(TxnRig& r, rc::Completions& cq, std::size_t n, std::vector<rc::Completion>& out, int cap = -1) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(60);
    for (int i = 0; (cap < 0 ? std::chrono::steady_clock::now() < deadline : i < cap) && out.size() < n; ++i) {
        if (r.invoker) r.client->doWork();
        r.gw.pump();
        cq.poll([&](const rc::Completion& c, std::span<const uint8_t>) { out.push_back(c); });
        if (!r.invoker) std::this_thread::yield();
    }
    return out.size() >= n;
}
}  // namespace

// Thread mode: a control op refused for BackPressured (full ring) or Closed (after close) hands its CQ credit back.
// RED (mutation): Producer::issueTxn_ without `q->unreserve(...)` on a refused issue -> the counters stay raised.
TEST(ProducerTxnClientRegression, CqCreditReturnsOnBackPressuredAndClosedRegression) {
    RunnerGate gate;
    TxnRig     r(rc::RunnerMode::Thread);
    GateOpener opener{gate};
    r.connect();
    rc::Producer    p = r.client->testProducerWithRingCap(txnCfg(), 64 * 1024);
    rc::Completions cq(*r.client);
    ASSERT_NO_FATAL_FAILURE(initAndBegin(r, p, cq));
    ASSERT_EQ(cq.reservedBytes(), 0u);
    ASSERT_EQ(cq.outstanding(), 0u);

    // The runner is parked in the completion callback of a flush: the ring cannot drain from here on.
    ASSERT_EQ(p.flush(gate.handler(), 900), rc::OpResult::Accepted);
    ASSERT_TRUE(gate.waitEntered(r)) << "the blocker flush never completed";

    // One accepted op holds credit while the ring fills with records (the credit baseline is NOT zero).
    ASSERT_EQ(p.flush(cq, 1000), rc::OpResult::Accepted);
    const uint32_t reservedBase    = cq.reservedBytes();
    const uint32_t outstandingBase = cq.outstanding();
    ASSERT_GT(reservedBase, 0u);
    ASSERT_EQ(outstandingBase, 1u);
    const rc::TopicHandle topic = r.client->topic("t");
    const std::string     rec   = "fill";
    rc::SendResult        sr    = rc::SendResult::Ok;
    uint32_t              filled = 0;
    for (int i = 0; i < 100'000 && sr == rc::SendResult::Ok; ++i) {
        sr = p.trySend(topic, rc::Key(uint64_t{1}), bytesOf(rec));
        if (sr == rc::SendResult::Ok) ++filled;
    }
    ASSERT_EQ(sr, rc::SendResult::BackPressured) << "the ring never filled (" << filled << " records)";
    for (int i = 0; i < 3; ++i) {   // every refused kind: the credit it reserved comes straight back
        EXPECT_EQ(p.flush(cq, 5000), rc::OpResult::BackPressured);
        EXPECT_EQ(p.commitTransaction(cq, 5001), rc::OpResult::BackPressured);
        EXPECT_EQ(p.abortTransaction(cq, 5002), rc::OpResult::BackPressured);
        const rc::OffsetEntry e{1, 0, 5};
        EXPECT_EQ(p.sendOffsetsToTransaction(std::span<const rc::OffsetEntry>(&e, 1), "g", cq, 5003), rc::OpResult::BackPressured);
        EXPECT_EQ(cq.reservedBytes(), reservedBase) << "BackPressured kept CQ credit (round " << i << ")";
        EXPECT_EQ(cq.outstanding(), outstandingBase);
    }

    // Release the runner: the one accepted op completes Ok exactly once and the credit drains to zero.
    gate.open();
    std::vector<rc::Completion> done;
    ASSERT_TRUE(collect(r, cq, 1, done));
    // A progress barrier, not a pass count (Thread mode): a later flush completes only after the runner delivered
    // everything ahead of it, so a duplicate of 1000 would show up before it.
    ASSERT_EQ(p.flush(cq, 1001), rc::OpResult::Accepted);
    ASSERT_TRUE(collect(r, cq, 2, done));
    EXPECT_FALSE(collect(r, cq, 3, done, 2'000)) << "a third completion";
    ASSERT_EQ(done.size(), 2u);
    EXPECT_EQ(done[0].token, 1000u);
    EXPECT_EQ(done[0].status, rc::OpStatus::Ok);
    EXPECT_EQ(done[1].token, 1001u) << "a second completion for the one op";
    EXPECT_EQ(done[1].status, rc::OpStatus::Ok);
    EXPECT_EQ(cq.reservedBytes(), 0u);
    EXPECT_EQ(cq.outstanding(), 0u);

    // Closed: after the producer closed, a control op is refused Closed and keeps nothing.
    ASSERT_EQ(p.close(cq, 7000), rc::OpResult::Accepted);
    ASSERT_TRUE(r.await(cq, 7000));
    EXPECT_EQ(r.got[7000].status, rc::OpStatus::Ok);
    const uint32_t reservedClosed    = cq.reservedBytes();
    const uint32_t outstandingClosed = cq.outstanding();
    for (int i = 0; i < 3; ++i) {
        EXPECT_EQ(p.flush(cq, 7100), rc::OpResult::Closed);
        EXPECT_EQ(p.commitTransaction(cq, 7101), rc::OpResult::Closed);
        EXPECT_EQ(p.abortTransaction(cq, 7102), rc::OpResult::Closed);
        EXPECT_EQ(p.initTransactions(cq, 7103), rc::OpResult::Closed);
        EXPECT_EQ(cq.reservedBytes(), reservedClosed) << "Closed kept CQ credit (round " << i << ")";
        EXPECT_EQ(cq.outstanding(), outstandingClosed);
    }
    EXPECT_EQ(reservedClosed, 0u);
}

// The Completions-queue twin of ProducerTxnControlRegression.ControlOpAfterCloseCompletesClosedRegression (that one
// uses handler targets on a bare rig): each op kind sits in the producer ring, closeImmediately() discards it, and the
// token completes Closed exactly once into the queue, which gets its credit back.
// RED (mutation): the ring-discard path of ProducerImpl completes the control op without the CQ delivery -> no token.
TEST(ProducerTxnClientRegression, ControlOpAfterCloseCompletesClosedWithCqTargetRegression) {
    enum class K { Init, SendOffsets, Commit, Abort, Flush };
    for (const K kind : {K::Init, K::SendOffsets, K::Commit, K::Abort, K::Flush}) {
        SCOPED_TRACE(static_cast<int>(kind));
        TxnRig r(rc::RunnerMode::Invoker);
        r.connect();
        rc::Producer    p = r.client->testProducerWithRingCap(txnCfg(), 64 * 1024);   // Ring ingress: ops ride the ring
        rc::Completions cq(*r.client);
        const rc::OffsetEntry e{1, 0, 5};
        const std::span<const rc::OffsetEntry> es(&e, 1);
        auto issue = [&](rc::Token t) {
            switch (kind) {
            case K::Init:        return p.initTransactions(cq, t);
            case K::SendOffsets: return p.sendOffsetsToTransaction(es, "g", cq, t);
            case K::Commit:      return p.commitTransaction(cq, t);
            case K::Abort:       return p.abortTransaction(cq, t);
            case K::Flush:       return p.flush(cq, t);
            }
            return rc::OpResult::InvalidArgument;
        };
        ASSERT_EQ(issue(41), rc::OpResult::Accepted);
        EXPECT_GT(cq.reservedBytes(), 0u);
        ASSERT_EQ(cq.outstanding(), 1u);
        ASSERT_EQ(p.closeImmediately(), rc::OpResult::Accepted);   // the ring is discarded by the next duty pass
        std::vector<rc::Completion> done;
        ASSERT_TRUE(collect(r, cq, 1, done));
        EXPECT_EQ(done[0].token, 41u);
        EXPECT_EQ(done[0].status, rc::OpStatus::Closed);
        EXPECT_FALSE(collect(r, cq, 2, done, 2'000)) << "a second completion for the one op";
        EXPECT_EQ(done.size(), 1u) << "exactly once";
        EXPECT_EQ(cq.reservedBytes(), 0u) << "the credit came back";
        EXPECT_EQ(cq.outstanding(), 0u);
        // Issued after the close: refused, nothing completes later, no credit kept.
        EXPECT_NE(issue(42), rc::OpResult::Accepted);
        EXPECT_FALSE(collect(r, cq, 2, done, 2'000)) << "a completion for a refused op";
        EXPECT_EQ(done.size(), 1u);
        EXPECT_EQ(cq.reservedBytes(), 0u);
        EXPECT_EQ(cq.outstanding(), 0u);
    }
}

// beginTransaction() on a full ring waits for room (the duty cycle drains it) and then applies exactly once.
// RED (mutation): Producer::beginTransaction() throws on BackPressured instead of waiting.
TEST(ProducerTxnClientRegression, BeginTransactionWaitsOutBackPressureRegression) {
    TxnRig r(rc::RunnerMode::Invoker);
    r.connect();
    rc::Producer          p = r.client->testProducerWithRingCap(txnCfg(), 4 * 1024);   // small ring: fills fast
    rc::Completions       cq(*r.client);
    const rc::TopicHandle topic = r.client->topic("t");
    ASSERT_EQ(p.initTransactions(cq, 100), rc::OpResult::Accepted);
    ASSERT_TRUE(r.await(cq, 100));
    ASSERT_EQ(r.got[100].status, rc::OpStatus::Ok);
    ASSERT_TRUE(r.drive([&] { return p.state() == rc::TxnState::Ready; }));

    // Nothing drains until the duty cycle runs: fill the ring with flush ops until the first BackPressured.
    uint32_t accepted = 0;
    rc::OpResult last = rc::OpResult::Accepted;
    for (int i = 0; i < 20'000 && last == rc::OpResult::Accepted; ++i) {
        last = p.flush(cq, static_cast<rc::Token>(1000 + i));
        if (last == rc::OpResult::Accepted) ++accepted;
    }
    ASSERT_EQ(last, rc::OpResult::BackPressured) << "the ring never filled";
    ASSERT_GT(accepted, 0u);
    ASSERT_EQ(p.state(), rc::TxnState::Ready) << "begin has not run yet";

    // begin's FIRST attempt meets the full ring; it pumps the duty cycle and retries.
    EXPECT_NO_THROW(p.beginTransaction());
    ASSERT_TRUE(r.drive([&] { return p.state() == rc::TxnState::InTransaction; })) << "begin never applied";
    EXPECT_TRUE(p.transactionActive());

    // The fillers completed exactly once each (a producer holds a bounded number of pending flushes: the rest are
    // refused 19 at once), and the transaction is real and usable once.
    std::vector<rc::Completion> done;
    ASSERT_TRUE(collect(r, cq, accepted, done));
    EXPECT_FALSE(collect(r, cq, accepted + 1, done, 2'000)) << "a filler completed twice";
    EXPECT_EQ(done.size(), accepted);
    uint32_t okFlushes = 0;
    for (const auto& c : done) {
        if (c.status == rc::OpStatus::Ok) {
            ++okFlushes;
        } else {
            EXPECT_EQ(c.status, rc::OpStatus::BrokerError) << c.token;
            EXPECT_EQ(c.errorCode, static_cast<uint16_t>(rune::protocol::client::ErrorCode::InvalidTxnState)) << c.token;
        }
    }
    EXPECT_GE(okFlushes, 1u);
    const std::string v = "after-begin";
    ASSERT_EQ(p.trySend(topic, rc::Key(uint64_t{1}), bytesOf(v)), rc::SendResult::Ok);
    ASSERT_EQ(p.commitTransaction(cq, 101), rc::OpResult::Accepted);
    ASSERT_TRUE(r.await(cq, 101));
    EXPECT_EQ(r.got[101].status, rc::OpStatus::Ok) << "errorCode " << r.got[101].errorCode;
    EXPECT_EQ(strs(r.gw.committedValues(0)), (std::vector<std::string>{"after-begin"}));
    EXPECT_EQ(r.gw.addPartitions, 1) << "one transaction began";
    EXPECT_EQ(r.gw.endTxnCommits, (std::vector<uint8_t>{1})) << "and ended once";
}

// Thread mode: `commit(token); begin(); send()` WITHOUT waiting for the commit's completion.  beginTransaction() must
// follow the previous end's completion (documented at beginTransaction()).  PINNED outcome: the ring is FIFO, so the
// duty side applies the commit, then the begin while the machine is still Committing (refused: counted, void), then
// the record behind it, which is outside any transaction (refused, never produced).  The record poisons the commit:
// the EARLIER commit completes BrokerError(19 InvalidTxnState), nothing is committed, no EndTxn leaves, and the machine
// is AbortableError until the app aborts; then begin / send / commit work as usual.
// The runner is parked while the three calls are issued, so the ring order (not timing) decides.
TEST(ProducerTxnClientRegression, BeginBeforeThePreviousEndCompletesIsRefusedRegression) {
    RunnerGate gate;
    TxnRig     r(rc::RunnerMode::Thread);
    GateOpener opener{gate};
    r.connect();
    rc::Producer          p = r.client->producer(txnCfg());
    rc::Completions       cq(*r.client);
    const rc::TopicHandle topic = r.client->topic("t");
    ASSERT_NO_FATAL_FAILURE(initAndBegin(r, p, cq));
    ASSERT_EQ(p.flush(gate.handler(), 900), rc::OpResult::Accepted);
    ASSERT_TRUE(gate.waitEntered(r));
    const rc::Key     key(uint64_t{1});
    const std::string first = "first", second = "second", third = "third";
    ASSERT_EQ(p.trySend(topic, key, bytesOf(first)), rc::SendResult::Ok);
    ASSERT_EQ(p.commitTransaction(cq, 201), rc::OpResult::Accepted);
    ASSERT_NO_THROW(p.beginTransaction());
    EXPECT_EQ(p.trySend(topic, key, bytesOf(second)), rc::SendResult::Ok) << "the app-side gate is open again";
    gate.open();

    ASSERT_TRUE(r.await(cq, 201));
    EXPECT_EQ(r.got[201].status, rc::OpStatus::BrokerError) << "the EARLIER commit is the op that fails";
    EXPECT_EQ(r.got[201].errorCode, static_cast<uint16_t>(rune::protocol::client::ErrorCode::InvalidTxnState));
    ASSERT_TRUE(r.drive([&] { return p.state() == rc::TxnState::AbortableError; }));
    settle(r);
    EXPECT_EQ(r.gw.endTxns, 0) << "no EndTxn left the producer";
    EXPECT_TRUE(r.gw.committedValues(0).empty()) << "nothing was committed";
    EXPECT_EQ(p.state(), rc::TxnState::AbortableError);

    // Recovery: abort completes Ok, a new transaction works.
    ASSERT_EQ(p.abortTransaction(cq, 202), rc::OpResult::Accepted);
    ASSERT_TRUE(r.await(cq, 202));
    EXPECT_EQ(r.got[202].status, rc::OpStatus::Ok);
    ASSERT_TRUE(r.drive([&] { return p.state() == rc::TxnState::Ready; }));
    p.beginTransaction();
    ASSERT_EQ(p.trySend(topic, key, bytesOf(third)), rc::SendResult::Ok);
    ASSERT_EQ(p.commitTransaction(cq, 203), rc::OpResult::Accepted);
    ASSERT_TRUE(r.await(cq, 203));
    EXPECT_EQ(r.got[203].status, rc::OpStatus::Ok);
    EXPECT_EQ(strs(r.gw.committedValues(0)), (std::vector<std::string>{"third"}));
}

// ---------------------------------------------------------------------------------------------------------------
// #363: the coordinator fallback, on the injected clock.
//
// The transaction coordinator is a remote node (NotCoordinator -> its own session).  When that session stays down for
// more than 5 s (Client::kCoordDownFallbackNs) the route falls back to the seed, whose node names the current
// coordinator, and the producer's in-flight request is re-offered on the new session with its original correlation id.
// The only other test of this is a ~17 s real-broker timing test.  Here one Client (Invoker, UDP sessions to two
// fake nodes) runs on ClientConfig::nanoClock: the clock moves only where the test moves it.
// ---------------------------------------------------------------------------------------------------------------
namespace {

int64_t g_fbNow = 0;
int64_t fbNow() noexcept { return g_fbNow; }
constexpr int64_t kFbGraceNs = 5'000'000'000LL;   // Client::kCoordDownFallbackNs (private): the documented 5 s

int fbFreePort() { return rune::client::testing::allocFreeUdpPort(); }   // cross-process collision-proof
std::string fbUdp(int port) { return "aeron:udp?endpoint=127.0.0.1:" + std::to_string(port); }

struct FallbackRig {
    int                          seedPort;
    int                          nodePort;
    rc::ClientConfig             cfg;
    rc::Client                   client;
    test_support::InvokerGateway gw;
    int                          node = -1;   // the coordinator's endpoint
    rc::Completions              cq{client};
    std::map<rc::Token, rc::Completion> got;

    static rc::ClientConfig makeCfg(int32_t in, int32_t eg, int seedPort) {
        rc::ClientConfig c;
        c.gatewayChannel  = fbUdp(seedPort);
        c.gatewayStreamId = in;
        c.egressChannel   = "aeron:ipc";
        c.egressStreamId  = eg;
        c.dataDir         = std::string(std::filesystem::temp_directory_path().string() + "/f_coordfb_") + std::to_string(::getpid()) + "_" + std::to_string(g_ctr++);
        g_sweeper.dirs.push_back(c.dataDir);
        c.runnerMode      = rc::RunnerMode::Invoker;
        c.nanoClock       = &fbNow;
        c.connectTimeoutMs = 3000;   // below the 5 s grace: a silent coordinator session stays Connecting, never Backoff
        // Nothing else may time out inside the 5 s windows the test jumps over.
        c.responseTimeoutNs = 3'600'000'000'000LL;
        c.pingTimeoutMs     = 3'600'000;
        rc::TopicSpec ts;
        ts.name           = "t";
        ts.topicId        = 1;
        ts.partitionCount = 1;
        c.topics.push_back(ts);
        return c;
    }
    explicit FallbackRig(int32_t in, int32_t eg)
        : seedPort(fbFreePort()), nodePort(fbFreePort()), cfg(makeCfg(in, eg, seedPort)), client(cfg),
          gw(cfg.dataDir + "/aeron", cfg.gatewayChannel, in, cfg.egressChannel, eg) {
        gw.brokerHost       = "127.0.0.1";
        gw.brokerPort       = seedPort;
        gw.uniqueSessionIds = true;
        gw.replyPongs       = true;
        gw.replyFromArrival = true;
        node = gw.addEndpoint(fbUdp(nodePort), in);
        gw.nodeOfEndpoint[node] = node;
        gw.moreBrokers           = {{1, "127.0.0.1", nodePort}};
        gw.partitions            = {{1, 0, 0, 1, in, ""}};   // the data leader is the seed node
        gw.coordinatorEndpoint   = node;                       // the transaction coordinator is the other node
        gw.coordinatorRedirectTo = "127.0.0.1:" + std::to_string(nodePort);
        gw.idempotence           = true;
        gw.initPidReply          = true;
        gw.replyTxn              = true;
        g_fbNow                  = 1'000'000'000'000LL;
    }
    ~FallbackRig() { client.close(); }

    /// One pass; the clock moves by `advanceNs` first.
    void step(int64_t advanceNs = 0) {
        g_fbNow += advanceNs;
        client.doWork();
        gw.pump();
        cq.poll([&](const rc::Completion& c, std::span<const uint8_t>) { got[c.token] = c; });
    }
    template <class P>
    bool drive(P&& pred, int cap = 2'000'000, int64_t advancePerStepNs = 5'000) {
        for (int i = 0; i < cap; ++i) {
            step(advancePerStepNs);
            if (pred()) return true;
        }
        return false;
    }
    void passes(int n) {
        for (int i = 0; i < n; ++i) step(0);
    }
    /// Moves the clock to `target` in 250 ms legs (each with a few real passes), so no single jump stalls a session.
    void advanceTo(int64_t target) {
        while (g_fbNow + 250'000'000LL < target) {
            g_fbNow += 250'000'000LL;
            passes(8);
        }
        g_fbNow = target;
        passes(8);
    }
    [[nodiscard]] uint32_t coordIdx() const { return client.testCoordinatorSessionIndex(); }
    [[nodiscard]] rc::session::SessionState coordState() const { return client.testSessionStateAt(coordIdx()); }
};

}  // namespace

// RED (mutations): coordDownTooLong `>` -> `>=` (fallback at exactly 5 s); no reset of the timer on Connected (the
// second outage inherits the first one's start); CoordinatorLink::onSessionChanged re-stamping the correlation id.
TEST(ProducerTxnClientRegression, CoordinatorFallbackHappensAfterFiveSecondsNotConnectedRegression) {
    FallbackRig r(5101, 5102);
    ASSERT_TRUE(r.drive([&] { return r.gw.ready(); }));
    ASSERT_EQ(r.client.connect(r.cq, 1), rc::OpResult::Accepted);
    ASSERT_TRUE(r.drive([&] { return r.got.count(1) != 0; })) << "the seed never connected";
    ASSERT_EQ(r.got[1].status, rc::OpStatus::Ok);

    rc::ProducerConfig pc = txnCfg();
    pc.transactionOpTimeoutMs = 3'600'000;
    pc.produceTimeoutMs       = 3'600'000;
    pc.deliveryTimeoutMs      = 7'200'000;
    rc::Producer p = r.client.producer(pc);
    const rc::TopicHandle topic = r.client.topic("t");
    ASSERT_EQ(p.initTransactions(r.cq, 100), rc::OpResult::Accepted);
    ASSERT_TRUE(r.drive([&] { return r.got.count(100) != 0; })) << "init never completed (redirects " << r.client.testCoordinatorRedirects() << ")";
    ASSERT_EQ(r.got[100].status, rc::OpStatus::Ok);
    ASSERT_EQ(r.client.testCoordinatorRedirects(), 1u) << "precondition: the coordinator is the other node";
    ASSERT_NE(r.coordIdx(), 0u);
    ASSERT_EQ(r.coordState(), rc::session::SessionState::Connected);
    ASSERT_EQ(r.client.testCoordinatorFallbacks(), 0u);
    const int64_t coordSid = r.gw.coordinatorSessionIds.back();

    // ---- an outage that ends before the edge resets the timer --------------------------------------------------
    // The node stops answering connects and expires the session: the coordinator session is down (Connecting) from T1.
    const int64_t t1 = g_fbNow;
    r.gw.silentEndpoints.insert(r.node);
    r.gw.sendError(r.node, static_cast<int16_t>(rune::protocol::client::ErrorCode::SessionExpired), "expired", coordSid, 0);
    int i1 = 0;
    for (; i1 < 20'000 && r.coordState() == rc::session::SessionState::Connected; ++i1) r.step(0);
    ASSERT_NE(r.coordState(), rc::session::SessionState::Connected) << "the expiry was not seen";
    ASSERT_EQ(g_fbNow, t1) << "the clock moved while waiting for the outage";
    r.passes(40);   // the duty side has seen the coordinator down, at T1
    r.advanceTo(t1 + 1'000'000'000LL);
    EXPECT_EQ(r.client.testCoordinatorFallbacks(), 0u);
    r.gw.silentEndpoints.erase(r.node);   // the node answers again; the session's own connect retry (3 s) finds it
    r.advanceTo(t1 + 3'100'000'000LL);
    ASSERT_TRUE(r.drive([&] { return r.coordState() == rc::session::SessionState::Connected; }, 20'000, 0))
        << "the coordinator session never came back (state " << static_cast<int>(r.coordState()) << ")";
    EXPECT_EQ(r.client.testCoordinatorFallbacks(), 0u) << "fell back although the session was Connected again before 5 s";
    ASSERT_EQ(r.coordIdx(), 1u);

    // ---- a transaction whose EndTxn is in flight at the coordinator ---------------------------------------------
    p.beginTransaction();
    ASSERT_TRUE(r.drive([&] { return p.state() == rc::TxnState::InTransaction; }, 20'000, 0));
    const std::string v = "fb";
    ASSERT_EQ(p.trySend(topic, rc::Key(uint64_t{1}), bytesOf(v), r.cq, 200), rc::SendResult::Ok);
    ASSERT_TRUE(r.drive([&] { return r.got.count(200) != 0; }, 200'000, 0)) << "the record was never acknowledged";
    ASSERT_EQ(r.got[200].status, rc::OpStatus::Ok);
    r.gw.holdTxnReplies = true;   // the coordinator decides the EndTxn, its answer never leaves
    ASSERT_EQ(p.commitTransaction(r.cq, 201), rc::OpResult::Accepted);
    ASSERT_TRUE(r.drive([&] { return r.gw.endTxns >= 1; }, 200'000, 0)) << "the EndTxn never reached the coordinator";
    ASSERT_EQ(r.gw.endTxns, 1);
    ASSERT_FALSE(r.gw.coordDecisions.empty());
    const int64_t endCorr = r.gw.coordDecisions.back().correlationId;
    ASSERT_EQ(r.gw.coordDecisions.back().kind, test_support::InvokerGateway::TxnFrameKind::EndTxn);

    // ---- the second outage: the 5 s edge ------------------------------------------------------------------------
    r.advanceTo(g_fbNow + 1'500'000'000LL);   // T1 + 4.6 s: a timer that was not reset would fire 0.4 s into the outage
    const int64_t t2 = g_fbNow;
    ASSERT_GT(t2 - t1, 4'000'000'000LL);
    r.gw.silentEndpoints.insert(r.node);
    r.gw.sendError(r.node, static_cast<int16_t>(rune::protocol::client::ErrorCode::SessionExpired), "expired", r.gw.coordinatorSessionIds.back(), 0);
    int i2 = 0;
    for (; i2 < 20'000 && r.coordState() == rc::session::SessionState::Connected; ++i2) r.step(0);
    ASSERT_NE(r.coordState(), rc::session::SessionState::Connected) << "the second expiry was not seen";
    ASSERT_EQ(g_fbNow, t2);
    r.passes(40);   // seen down at T2
    EXPECT_EQ(r.client.testCoordinatorFallbacks(), 0u) << "fell back at once: the timer of the first outage was not reset";
    r.gw.coordinatorEndpoint = 0;   // the seed is where the new coordinator is named
    r.advanceTo(t2 + 2'000'000'000LL);
    EXPECT_EQ(r.client.testCoordinatorFallbacks(), 0u);
    r.advanceTo(t2 + 4'750'000'000LL);
    EXPECT_EQ(r.client.testCoordinatorFallbacks(), 0u);
    g_fbNow = t2 + kFbGraceNs - 1;
    r.passes(20);
    EXPECT_EQ(r.client.testCoordinatorFallbacks(), 0u) << "fell back one ns before the 5 s edge";
    g_fbNow = t2 + kFbGraceNs;
    r.passes(20);
    EXPECT_EQ(r.client.testCoordinatorFallbacks(), 0u) << "fell back at exactly 5 s (the edge is 'more than 5 s')";
    EXPECT_NE(r.coordIdx(), 0u);
    EXPECT_EQ(r.gw.endTxns, 1) << "nothing was re-offered before the fallback";
    r.gw.holdTxnReplies = false;   // the seed node, the coordinator now, answers
    g_fbNow = t2 + kFbGraceNs + 1;
    r.passes(1);
    EXPECT_EQ(r.client.testCoordinatorFallbacks(), 1u) << "no fallback one ns past the edge";
    EXPECT_EQ(r.coordIdx(), 0u) << "the route is the seed again";

    // ---- the pending EndTxn is re-offered on the seed, once, with the same correlation id -----------------------
    ASSERT_TRUE(r.drive([&] { return r.got.count(201) != 0; }, 200'000, 0)) << "the commit never completed after the fallback";
    EXPECT_EQ(r.got[201].status, rc::OpStatus::Ok) << "errorCode " << r.got[201].errorCode;
    r.passes(2000);
    EXPECT_EQ(r.gw.endTxns, 2) << "the EndTxn must reach the seed exactly once";
    std::vector<test_support::InvokerGateway::CoordDecision> ends;
    for (const auto& d : r.gw.coordDecisions) if (d.kind == test_support::InvokerGateway::TxnFrameKind::EndTxn) ends.push_back(d);
    ASSERT_EQ(ends.size(), 2u);
    EXPECT_EQ(ends[0].correlationId, endCorr);
    EXPECT_EQ(ends[1].correlationId, endCorr) << "the re-offer carries the original correlation id";
    EXPECT_TRUE(ends[1].resend) << "the coordinator saw the same request again (idempotent)";
    ASSERT_GE(r.gw.coordinatorEndpoints.size(), 2u);
    EXPECT_EQ(r.gw.coordinatorEndpoints.back(), 0) << "the re-offer arrived on the seed node";
    EXPECT_EQ(r.client.testCoordinatorFallbacks(), 1u) << "a second fallback";
    EXPECT_EQ(strs(r.gw.committedValues(0)), (std::vector<std::string>{"fb"}));
}

// ---------------------------------------------------------------------------------------------------------------
// C11 review: adversarial pass over the transactional producer (docs/adversarial-review.md), through the public API.
// ---------------------------------------------------------------------------------------------------------------
namespace {
/// Replays a coordinator answer the client already consumed (or one for a request it never sent): the bytes a stale,
/// duplicated or reordered network delivery would carry.
void replayEndTxnAnswer(TxnRig& r, int64_t sessionId, int64_t corrId, int16_t code) {
    rune::protocol::client::EndTxnResponse rsp;
    rsp.sessionId     = sessionId;
    rsp.correlationId = corrId;
    rsp.errorCode     = code;
    std::vector<uint8_t> out(rune::protocol::client::EndTxnResponse::ENCODED_SIZE);
    const std::size_t n = rsp.encode(out.data());
    r.gw.send(0, out.data(), n);
}
int64_t lastEndTxnCorr(const test_support::InvokerGateway& gw) {
    int64_t c = 0;
    for (const auto& d : gw.coordDecisions)
        if (d.kind == test_support::InvokerGateway::TxnFrameKind::EndTxn) c = d.correlationId;
    return c;
}
}  // namespace

// Protocol state (replays, out of order, session loss mid-op) + data integrity.  The EndTxn answer of transaction 1 is
// replayed (Ok and error codes) while transaction 2 is open, and again while transaction 2's own EndTxn is in flight
// with its answer lost; a forged answer for a correlation id the client never used is replayed too.  None of them may
// complete, fail or poison transaction 2; the session then dies, the in-flight EndTxn is re-sent once with its own
// correlation id, and every record is committed exactly once.
TEST(ProducerTxnClientRegression, StaleEndTxnAnswerReplayedIntoTheNextTransactionIsIgnoredAdversarialRegression) {
    for (const bool ring : {false, true}) {
        SCOPED_TRACE(ring ? "Ring ingress" : "Direct ingress");
        TxnRig r(rc::RunnerMode::Invoker);
        r.gw.uniqueSessionIds = true;
        r.gw.newIdOnResume    = false;
        r.connect();
        rc::Producer    p  = ring ? r.client->testProducerWithRingCap(txnCfg(), 64 * 1024) : r.client->producer(txnCfg());
        rc::Completions cq(*r.client);
        const rc::TopicHandle topic = r.client->topic("t");
        const rc::Key         key(uint64_t{1});
        ASSERT_NO_FATAL_FAILURE(initAndBegin(r, p, cq));
        const std::string a = "t1-a", b = "t2-b", c = "t2-c";
        ASSERT_EQ(p.trySend(topic, key, bytesOf(a)), rc::SendResult::Ok);
        ASSERT_EQ(p.commitTransaction(cq, 201), rc::OpResult::Accepted);
        ASSERT_TRUE(r.await(cq, 201));
        ASSERT_EQ(r.got[201].status, rc::OpStatus::Ok);
        const int64_t sid   = r.gw.lastUniqueSessionId();
        const int64_t corr1 = lastEndTxnCorr(r.gw);
        ASSERT_NE(corr1, 0);

        // (1) transaction 2 open, no EndTxn in flight: stale Ok / Fenced / InvalidTxnState answers for txn 1.
        ASSERT_TRUE(r.drive([&] { return p.state() == rc::TxnState::Ready; }));
        p.beginTransaction();
        ASSERT_EQ(p.trySend(topic, key, bytesOf(b)), rc::SendResult::Ok);
        ASSERT_NO_FATAL_FAILURE(settle(r));
        for (const int16_t code : {int16_t{0}, int16_t{18}, int16_t{19}}) replayEndTxnAnswer(r, sid, corr1, code);
        replayEndTxnAnswer(r, sid, corr1 + (int64_t{1} << 20), 0);   // a correlation id never issued
        ASSERT_NO_FATAL_FAILURE(settle(r));
        EXPECT_EQ(p.state(), rc::TxnState::InTransaction) << "a stale EndTxn answer moved the open transaction";
        EXPECT_EQ(r.got.count(202), 0u);

        // (2) txn 2's EndTxn is processed but its answer is lost; the stale answers arrive while it is in flight.
        ASSERT_EQ(p.trySend(topic, key, bytesOf(c)), rc::SendResult::Ok);
        r.gw.holdTxnReplies = true;
        const int endsBefore = r.gw.endTxns;
        ASSERT_EQ(p.commitTransaction(cq, 202), rc::OpResult::Accepted);
        ASSERT_TRUE(r.drive([&] { return r.gw.endTxns > endsBefore; })) << "txn 2's EndTxn never left";
        const int64_t corr2 = lastEndTxnCorr(r.gw);
        ASSERT_NE(corr2, corr1) << "a new EndTxn reused the old correlation id";
        for (const int16_t code : {int16_t{0}, int16_t{18}, int16_t{19}}) replayEndTxnAnswer(r, sid, corr1, code);
        ASSERT_NO_FATAL_FAILURE(settle(r));
        cq.poll([&](const rc::Completion& x, std::span<const uint8_t>) { r.got[x.token] = x; });
        EXPECT_EQ(r.got.count(202), 0u) << "a stale answer completed the in-flight commit (status "
                                         << (r.got.count(202) ? static_cast<int>(r.got[202].status) : -1) << ")";
        EXPECT_EQ(p.state(), rc::TxnState::CommittingTransaction);

        // (3) the session dies with the EndTxn in flight: it is re-sent with ITS correlation id and completes once.
        r.gw.expireSession(sid);
        r.gw.holdTxnReplies = false;
        ASSERT_TRUE(r.await(cq, 202)) << "the in-flight EndTxn was not re-sent";
        EXPECT_EQ(r.got[202].status, rc::OpStatus::Ok) << "errorCode " << r.got[202].errorCode;
        EXPECT_EQ(lastEndTxnCorr(r.gw), corr2);
        ASSERT_NO_FATAL_FAILURE(settle(r));
        EXPECT_EQ(strs(r.gw.committedValues(0)), (std::vector<std::string>{a, b, c})) << "exactly once, in order";
        EXPECT_EQ(r.gw.endTxnCommits, (std::vector<uint8_t>{1, 1}));
        EXPECT_EQ(cq.outstanding(), 0u);
        EXPECT_EQ(cq.reservedBytes(), 0u);
    }
}

// Input bounds through the public API, both ingress modes: a group id of 0 / 1 / 255 / 256 bytes, 0 / 1 / 1024 /
// 1025 offsets, a member id at kMaxGroupMemberIdBytes and one past it.  An over-cap call completes InvalidArgument
// exactly once (never truncated: the coordinator never sees it), hands its credit back and does not poison the
// transaction; a legal one at every edge reaches the coordinator whole and commits.
TEST(ProducerTxnClientRegression, SendOffsetsBoundsAtEveryEdgeAdversarialRegression) {
    for (const bool ring : {false, true}) {
        SCOPED_TRACE(ring ? "Ring ingress" : "Direct ingress");
        TxnRig r(rc::RunnerMode::Invoker);
        r.connect();
        rc::Producer p = ring ? r.client->testProducerWithRingCap(txnCfg(), 1 << 20) : r.client->producer(txnCfg());
        rc::Completions cq(*r.client);
        ASSERT_NO_FATAL_FAILURE(initAndBegin(r, p, cq));
        std::vector<rc::OffsetEntry> many(1025);
        for (std::size_t i = 0; i < many.size(); ++i) many[i] = rc::OffsetEntry{1, 0, static_cast<int64_t>(i)};
        const std::span<const rc::OffsetEntry> all(many);
        struct Case { const char* what; std::size_t groupLen; std::size_t offsets; std::size_t memberLen; bool ok; };
        const Case cases[] = {
            {"group 0", 0, 1, 0, false},       {"group 1", 1, 1, 0, true},
            {"group 255", 255, 1, 0, true},    {"group 256", 256, 1, 0, false},
            {"1024 offsets", 3, 1024, 0, true}, {"1025 offsets", 3, 1025, 0, false},
            {"member max", 3, 1, rc::kMaxGroupMemberIdBytes, true},
            {"member max+1", 3, 1, rc::kMaxGroupMemberIdBytes + 1, false},
        };
        rc::Token tok = 300;
        for (const Case& k : cases) {
            SCOPED_TRACE(k.what);
            const int    commitsBefore = r.gw.txnOffsetCommits;
            auto staged = [&] {
                std::size_t n = 0;
                for (const auto& [pid, v] : r.gw.pendingTxnOffsets) n += v.size();
                return n;
            };
            const std::size_t stagedBefore = staged();
            std::string  group(k.groupLen, 'g');
            rc::OpResult res;
            if (k.memberLen == 0) {
                res = p.sendOffsetsToTransaction(all.first(k.offsets), group, cq, tok);
            } else {
                rc::GroupMetadata gm;
                gm.groupId      = group;
                gm.generationId = 3;
                gm.memberId.assign(k.memberLen, 'm');
                res = p.sendOffsetsToTransaction(all.first(k.offsets), gm, cq, tok);
            }
            ASSERT_EQ(res, rc::OpResult::Accepted) << "the documented form: accepted, then completes";
            ASSERT_TRUE(r.await(cq, tok));
            if (k.ok) {
                EXPECT_EQ(r.got[tok].status, rc::OpStatus::Ok) << "errorCode " << r.got[tok].errorCode;
                EXPECT_GT(r.gw.txnOffsetCommits, commitsBefore);   // a large set may travel as several requests
                EXPECT_EQ(staged(), stagedBefore + k.offsets) << "offsets lost or duplicated on the way";
                EXPECT_EQ(r.gw.lastTxnOffsetGroup, group) << "the group id was truncated";
            } else {
                EXPECT_EQ(r.got[tok].status, rc::OpStatus::InvalidArgument);
                EXPECT_EQ(r.gw.txnOffsetCommits, commitsBefore) << "an over-cap call reached the coordinator";
                EXPECT_EQ(staged(), stagedBefore);
            }
            EXPECT_EQ(p.state(), rc::TxnState::InTransaction) << "the bound check poisoned the transaction";
            EXPECT_EQ(cq.outstanding(), 0u);
            EXPECT_EQ(cq.reservedBytes(), 0u);
            ++tok;
        }
        const std::string v = "after-bounds";
        ASSERT_EQ(p.trySend(r.client->topic("t"), rc::Key(uint64_t{1}), bytesOf(v)), rc::SendResult::Ok);
        ASSERT_EQ(p.commitTransaction(cq, 400), rc::OpResult::Accepted);
        ASSERT_TRUE(r.await(cq, 400));
        EXPECT_EQ(r.got[400].status, rc::OpStatus::Ok) << "errorCode " << r.got[400].errorCode;
        EXPECT_EQ(strs(r.gw.committedValues(0)), (std::vector<std::string>{v}));
        EXPECT_EQ((r.gw.committedOffsets[std::make_pair(int32_t{1}, int32_t{0})]), 0) << "the last legal call's single offset (0) committed";
    }
}
