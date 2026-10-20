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
