// tests/client/ProducerTxnClientTest.cpp - Phase 2b F: the unified Producer against a real Client on Aeron IPC
// (Invoker mode, the InvokerGateway broker model).  Task 4: broker-assigned ids for every acked producer.
#include "../support/InvokerGateway.h"

#include <rune/client/Client.h>
#include <rune/client/Completions.h>
#include <rune/client/Key.h>
#include <rune/client/producer/Producer.h>

#include <gtest/gtest.h>

#include <chrono>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <span>
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
    c.dataDir = std::string("/tmp/f_txncl_") + std::to_string(::getpid()) + "_" + std::to_string(g_ctr++);
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
