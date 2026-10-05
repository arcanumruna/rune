// tests/client/IngressEndpointRoutingTest.cpp — per-partition worker ingress
// endpoints (shard-mode brokers).  Invoker client + InvokerGateway on one thread;
// the duty-cycle clock is frozen so no retransmit timer can mask a routing bug.
#include "../support/InvokerGateway.h"

#include <rune/client/Client.h>
#include <rune/client/Completions.h>
#include <rune/client/agent/ClientAgent.h>
#include <rune/client/producer/InFlightRing.h>

#include <gtest/gtest.h>

#include <chrono>
#include <span>
#include <string>
#include <unistd.h>
#include <vector>

namespace rc = rune::client;
using namespace std::chrono_literals;

namespace {
int g_ctr = 0;  // test thread only
int64_t frozenNow() noexcept { return 1'000'000'000'000LL; }
const std::vector<uint8_t> kVal{7, 7, 7};
// The clock is frozen: linger must be 0 or a batch would wait forever.
rc::ProducerConfig noLinger() {
    rc::ProducerConfig p;
    p.lingerUs = 0;
    return p;
}

struct Rig {
    rc::ClientConfig             c;
    rc::Client                   client;
    test_support::InvokerGateway gw;
    // seed = the client's gateway channel; node = the channel the fake node listens on.
    // realClock: UDP sessions (seed or per-node) need the SessionMachine's connect
    // retry, which runs on the duty clock — a frozen clock would never retry the
    // ConnectRequest offered before the UDP publication connected.
    Rig(int32_t in, int32_t eg, const std::string& seed = "aeron:ipc", const std::string& node = "aeron:ipc",
        bool realClock = false)
        : c(makeCfg(in, eg, seed, realClock)),
          client(c),
          gw(c.dataDir + "/aeron", node, in, "aeron:ipc", eg) {}
    static rc::ClientConfig makeCfg(int32_t in, int32_t eg, const std::string& seed, bool realClock) {
        rc::ClientConfig c;
        c.gatewayChannel = seed;
        c.gatewayStreamId = in;
        c.egressChannel = "aeron:ipc";
        c.egressStreamId = eg;
        c.dataDir = "/tmp/ingress_ep_" + std::to_string(::getpid()) + "_" + std::to_string(g_ctr++);
        c.runnerMode = rc::RunnerMode::Invoker;
        c.nanoClock = realClock ? nullptr : &frozenNow;
        rc::TopicSpec ts;
        ts.name = "t";
        ts.topicId = 1;
        ts.partitionCount = 1;
        c.topics.push_back(ts);
        return c;
    }
    template <class P>
    bool drive(P&& pred, int cap = 200'000) {
        for (int i = 0; i < cap; ++i) {
            client.doWork();
            gw.pump();
            if (pred()) return true;
        }
        return false;
    }
    // F8c: completion tokens polled while driving (no future).  The frozen
    // clock never expires an op, so the drive cap is the only bound.
    rc::Completions cq{client};  // this (owner) thread's queue
    rc::Token       nextToken = 1;
    // Drive until `t` completes; returns its Completion (status Closed if the cap hit).
    rc::Completion awaitToken(rc::Token t) {
        rc::Completion got{};
        got.status = rc::OpStatus::Closed;
        bool done = false;
        (void)drive([&] {
            cq.poll([&](const rc::Completion& c, std::span<const uint8_t>) {
                if (c.token == t) { done = true; got = c; }
            });
            return done;
        });
        return got;
    }
    void connect() {
        ASSERT_TRUE(drive([&] { return gw.ready(); }));
        const rc::Token t = nextToken++;
        ASSERT_EQ(client.connect(cq, t), rc::OpResult::Accepted);
        ASSERT_EQ(awaitToken(t).status, rc::OpStatus::Ok);
    }
    // One acked send into `cq`; returns its Completion.
    rc::Completion sendAndAwait(rc::Producer& p) {
        const rc::Token t = nextToken++;
        EXPECT_EQ(p.trySend(client.topic("t"), rc::Key(uint64_t{1}), std::span<const uint8_t>(kVal), cq, t),
                  rc::SendResult::Ok);
        return awaitToken(t);
    }
};

// onAck handler target (F8c: plain function pointer + context, duty thread).
struct AckSeen {
    int     fired  = 0;
    int64_t offset = -2;
    static void on(void* ctx, rc::Token, int64_t off, rc::OpStatus, uint16_t) noexcept {
        auto* s = static_cast<AckSeen*>(ctx);
        ++s->fired;
        s->offset = off;
    }
};
} // namespace

TEST(IngressEndpointRouting, ProduceGoesToTheAdvertisedWorkerEndpoint) {
    Rig r(3900, 3901);
    const int w = r.gw.addEndpoint("aeron:udp?endpoint=127.0.0.1:24751", 5001);
    r.gw.partitions = {{1, 0, 0, 1, 5001, "127.0.0.1:24751"}};
    r.connect();
    ASSERT_TRUE(r.drive([&] { return !r.client.testDataChannelFor(1, 0).empty(); }));
    EXPECT_EQ(r.client.testDataChannelFor(1, 0), "aeron:udp?endpoint=127.0.0.1:24751");
    auto p = r.client.producer(noLinger());
    const rc::Completion c = r.sendAndAwait(p);
    ASSERT_EQ(c.status, rc::OpStatus::Ok);
    EXPECT_EQ(c.value, 100) << "acked by the worker's own publication";
    ASSERT_EQ(r.gw.produced.size(), 1u);
    EXPECT_EQ(r.gw.produced[0].endpoint, w) << "produce must reach the worker, not the node";
    EXPECT_EQ(r.gw.produced[0].sessionId, r.gw.assignedSessionId) << "session id from the node Connect";
}

TEST(IngressEndpointRouting, EmptyEndpointKeepsNodeRouting) {
    Rig r(3902, 3903);
    r.gw.partitions = {{1, 0, 0, 1, 0, ""}};
    r.connect();
    ASSERT_TRUE(r.drive([&] { return r.gw.metadataRequests > 0; }));
    auto p = r.client.producer(noLinger());
    ASSERT_EQ(r.sendAndAwait(p).status, rc::OpStatus::Ok);
    ASSERT_EQ(r.gw.produced.size(), 1u);
    EXPECT_EQ(r.gw.produced[0].endpoint, 0);
    EXPECT_TRUE(r.client.testDataChannelFor(1, 0).empty());
}

// Regression (Review Focus 5): a metadata refresh moves the partition to another
// worker while a batch is in flight at the old one.  The batch must be re-offered
// to the new worker immediately (the clock is frozen: no retransmit timer), and a
// late ack from the old worker must not complete the record twice.
TEST(IngressEndpointRouting, EndpointMoveReoffersInFlightBatchesRegression) {
    Rig r(3904, 3905);
    const int wA = r.gw.addEndpoint("aeron:udp?endpoint=127.0.0.1:24761", 5002);
    const int wB = r.gw.addEndpoint("aeron:udp?endpoint=127.0.0.1:24770", 5003);
    r.gw.partitions = {{1, 0, 0, 1, 5002, "127.0.0.1:24761"}};
    r.connect();
    ASSERT_TRUE(r.drive([&] { return r.client.testDataChannelFor(1, 0) == "aeron:udp?endpoint=127.0.0.1:24761"; }));
    r.gw.holdPartitions = {0};
    auto p = r.client.producer(noLinger());
    AckSeen seen;
    p.onAck(&AckSeen::on, &seen);  // setup phase: before the first AckToken send
    ASSERT_EQ(p.trySend(r.client.topic("t"), rc::Key(uint64_t{1}), std::span<const uint8_t>(kVal),
                        rc::AckToken{1}),
              rc::SendResult::Ok);
    const int& fired = seen.fired;
    const int64_t& offset = seen.offset;
    ASSERT_TRUE(r.drive([&] { return r.gw.held.size() == 1; }));
    ASSERT_EQ(r.gw.held[0].endpoint, wA);

    r.gw.partitions = {{1, 0, 0, 2, 5003, "127.0.0.1:24770"}};  // epoch 2: moved to B
    r.gw.holdPartitions.clear();
    r.gw.sendMetadata(r.gw.assignedSessionId, /*correlationId=*/0);  // unsolicited refresh
    ASSERT_TRUE(r.drive([&] { return fired == 1; })) << "in-flight batch not re-offered to the new worker";
    EXPECT_EQ(r.gw.produced.back().endpoint, wB);
    // The same batch as a new attempt (EOS I-2: only the attempt tag differs).
    EXPECT_EQ(rune::client::producer::batchcorr::base(r.gw.produced.back().corrId),
              rune::client::producer::batchcorr::base(r.gw.held[0].corrId))
        << "same batch (idempotent re-offer)";

    r.gw.releaseHeld();  // the old worker acks late
    for (int i = 0; i < 20'000; ++i) {
        r.client.doWork();
        r.gw.pump();
    }
    EXPECT_EQ(fired, 1) << "exactly one completion per record";
    EXPECT_EQ(offset, 100);
}

// A refresh that drops the partition's worker endpoint (the broker left shard
// mode / a legacy leader took over) must drop the worker route too: produce
// returns to the node endpoint, and the in-flight batch follows it at once.
TEST(IngressEndpointRouting, EndpointClearedFallsBackToNode) {
    Rig r(3906, 3907);
    const int w = r.gw.addEndpoint("aeron:udp?endpoint=127.0.0.1:24771", 3906);  // shard mode: the client stream
    r.gw.partitions = {{1, 0, 0, 1, 3906, "127.0.0.1:24771"}};
    r.connect();
    ASSERT_TRUE(r.drive([&] { return !r.client.testDataChannelFor(1, 0).empty(); }));
    r.gw.holdPartitions = {0};
    auto p = r.client.producer(noLinger());
    AckSeen seen;
    p.onAck(&AckSeen::on, &seen);
    ASSERT_EQ(p.trySend(r.client.topic("t"), rc::Key(uint64_t{1}), std::span<const uint8_t>(kVal),
                        rc::AckToken{1}),
              rc::SendResult::Ok);
    const int& fired = seen.fired;
    ASSERT_TRUE(r.drive([&] { return r.gw.held.size() == 1; }));
    ASSERT_EQ(r.gw.held[0].endpoint, w);

    r.gw.partitions = {{1, 0, 0, 2, 3906, ""}};  // epoch 2: no worker endpoint
    r.gw.holdPartitions.clear();
    r.gw.sendMetadata(r.gw.assignedSessionId, /*correlationId=*/0);
    ASSERT_TRUE(r.drive([&] { return r.client.testDataChannelFor(1, 0).empty(); }))
        << "worker route kept after the endpoint was withdrawn";
    ASSERT_TRUE(r.drive([&] { return fired == 1; })) << "in-flight batch not re-offered to the node";
    EXPECT_EQ(r.gw.produced.back().endpoint, 0);
    EXPECT_EQ(rune::client::producer::batchcorr::base(r.gw.produced.back().corrId),
              rune::client::producer::batchcorr::base(r.gw.held[0].corrId));

    const std::size_t before = r.gw.produced.size();
    ASSERT_EQ(r.sendAndAwait(p).status, rc::OpStatus::Ok);
    ASSERT_EQ(r.gw.produced.size(), before + 1);
    EXPECT_EQ(r.gw.produced.back().endpoint, 0) << "new batches go to the node";
}

// Regression: a producer created AFTER the MetadataResponse never learnt the
// partition's advertised ingressStreamId (updateIngressRoute reached only the
// producers registered at that moment), so it published on the default client
// stream instead of the advertised one.
TEST(IngressEndpointRouting, ProducerCreatedAfterMetadataUsesAdvertisedStreamRegression) {
    Rig r(3908, 3909);
    const int s = r.gw.addEndpoint("aeron:ipc", 5004);  // the node, on the advertised stream
    r.gw.partitions = {{1, 0, 0, 1, 5004, ""}};
    r.connect();
    ASSERT_TRUE(r.drive([&] { return r.client.testIngressStreamIdFor(1, 0) == 5004; }));  // metadata landed
    auto p = r.client.producer(noLinger());
    ASSERT_EQ(r.sendAndAwait(p).status, rc::OpStatus::Ok);
    ASSERT_EQ(r.gw.produced.size(), 1u);
    EXPECT_EQ(r.gw.produced[0].endpoint, s) << "published on the default stream, not the advertised one";
}

// ===========================================================================
// Task 16 fix round 1
// ===========================================================================
namespace {
// Per-token completion log (onAck target).
namespace batchcorr = rune::client::producer::batchcorr;
struct AckLog {
    std::vector<std::pair<rc::Token, rc::OpStatus>> seen;
    std::vector<int64_t>                            offsets;  // value, parallel to seen
    static void on(void* ctx, rc::Token t, int64_t v, rc::OpStatus st, uint16_t) noexcept {
        static_cast<AckLog*>(ctx)->seen.emplace_back(t, st);
        static_cast<AckLog*>(ctx)->offsets.push_back(v);
    }
    [[nodiscard]] int count(rc::Token t) const {
        int n = 0;
        for (const auto& [tok, st] : seen) n += tok == t;
        return n;
    }
    [[nodiscard]] bool allOk() const {
        for (const auto& [tok, st] : seen) if (st != rc::OpStatus::Ok) return false;
        return true;
    }
};
bool sendTok(Rig& r, rc::Producer& p, uint64_t t) {
    return p.trySend(r.client.topic("t"), rc::Key(uint64_t{1}), std::span<const uint8_t>(kVal), rc::AckToken{t}) ==
           rc::SendResult::Ok;
}
} // namespace

// C1 (Critical): the partition moves while acks=all batches are in flight at the
// old owner A; A answers ProduceResponse{NotLeader} (retriable — the broker's
// refuseNotOwned_ / moved-replica answer).  The NotLeader ALONE must trigger the
// metadata refresh (no unsolicited MetadataResponse); every stored batch is
// re-offered unchanged to the new owner B; every record completes Ok exactly
// once and the producer epoch never moves.
TEST(IngressEndpointRouting, OldOwnerNotLeaderReoffersToNewOwnerRegression) {
    Rig r(3910, 3911);
    const int wA = r.gw.addEndpoint("aeron:udp?endpoint=127.0.0.1:24781", 3910);
    const int wB = r.gw.addEndpoint("aeron:udp?endpoint=127.0.0.1:24782", 3910);
    r.gw.partitions = {{1, 0, 0, 1, 3910, "127.0.0.1:24781"}};
    r.connect();
    ASSERT_TRUE(r.drive([&] { return r.client.testDataChannelFor(1, 0) == "aeron:udp?endpoint=127.0.0.1:24781"; }));
    rc::ProducerConfig pc = noLinger();
    pc.acks = rc::Acks::All;
    auto p = r.client.producer(pc);
    AckLog log;
    p.onAck(&AckLog::on, &log);
    r.gw.holdPartitions = {0};  // three batches in flight at A, unanswered
    for (uint64_t t = 1; t <= 3; ++t) {
        ASSERT_TRUE(sendTok(r, p, t));
        ASSERT_TRUE(r.drive([&] { return r.gw.held.size() == t; }));
    }
    for (const auto& h : r.gw.held) ASSERT_EQ(h.endpoint, wA);
    const std::vector<test_support::InvokerGateway::Produced> atA = r.gw.held;

    // Ownership moves to B: A refuses (NotLeader); the cluster map now names B.
    r.gw.partitions = {{1, 0, 0, 2, 3910, "127.0.0.1:24782"}};
    r.gw.holdPartitions.clear();
    r.gw.held.clear();
    const int mdBefore = r.gw.metadataRequests;
    for (const auto& h : atA) r.gw.ackAt(h, static_cast<int16_t>(rune::protocol::client::ErrorCode::NotLeader), -1);

    ASSERT_TRUE(r.drive([&] { return log.seen.size() == 3; })) << "stored batches not re-offered to B";
    EXPECT_GT(r.gw.metadataRequests, mdBefore) << "NotLeader did not request metadata";
    EXPECT_TRUE(log.allOk()) << "a NotLeader batch completed with an error";
    for (rc::Token t = 1; t <= 3; ++t) EXPECT_EQ(log.count(t), 1) << "token " << t;
    // Re-offered to B unchanged, in order, under the original epoch.
    std::vector<test_support::InvokerGateway::Produced> atB;
    for (const auto& pr : r.gw.produced) if (pr.endpoint == wB) atB.push_back(pr);
    ASSERT_EQ(atB.size(), 3u);
    for (std::size_t i = 0; i < 3; ++i) {
        // The same batch (a NEW attempt: only the attempt tag differs — EOS I-2).
        EXPECT_EQ(batchcorr::base(atB[i].corrId), batchcorr::base(atA[i].corrId)) << i;
        EXPECT_NE(batchcorr::attempt(atB[i].corrId), batchcorr::attempt(atA[i].corrId)) << i;
        EXPECT_EQ(atB[i].baseSequence, atA[i].baseSequence) << i;
        EXPECT_EQ(atB[i].producerEpoch, atA[i].producerEpoch) << i;
    }
    // Nothing left parked; a new batch goes to B under the same epoch (no bump).
    EXPECT_FALSE(r.client.testRouteParked(1, 0));
    ASSERT_TRUE(sendTok(r, p, 4));
    ASSERT_TRUE(r.drive([&] { return log.count(4) == 1; }));
    EXPECT_EQ(r.gw.produced.back().endpoint, wB);
    EXPECT_EQ(r.gw.produced.back().producerEpoch, atA[0].producerEpoch) << "epoch bumped";
    EXPECT_TRUE(log.allOk());
}

// C1 + EOS composition, against the gateway's broker idempotence model (shared
// partition state = the moved log keeps its producer state): acks=all batches
// 1..3 are APPENDED at the old owner A with their OKs deferred (HW not reached);
// batch 4 reaches A after the move and is refused (not appended).  A answers
// NotLeader for all four (refuseNotOwned_ + the moved-replica waiters).  The
// client must re-offer the SAME identities to B, where 1..3 are duplicates
// (OK with their original offsets) and 4 is new.  Every record completes Ok
// exactly once, the log holds each batch once, no OutOfOrderSequence /
// fencing answer is produced and the producer epoch never moves.
TEST(IngressEndpointRouting, PartitionMoveWithAcksAllInFlightCompletesEachRecordOnceRegression) {
    namespace rp = rune::protocol::client;
    Rig r(3924, 3925);
    r.gw.idempotence  = true;
    r.gw.okDelayPumps = 1 << 30;  // acks=all OKs deferred (until the move turns them into NotLeader)
    const int wA = r.gw.addEndpoint("aeron:udp?endpoint=127.0.0.1:24792", 3924);
    const int wB = r.gw.addEndpoint("aeron:udp?endpoint=127.0.0.1:24793", 3924);
    r.gw.partitions = {{1, 0, 0, 1, 3924, "127.0.0.1:24792"}};
    r.connect();
    ASSERT_TRUE(r.drive([&] { return r.client.testDataChannelFor(1, 0) == "aeron:udp?endpoint=127.0.0.1:24792"; }));
    rc::ProducerConfig pc = noLinger();
    pc.acks = rc::Acks::All;
    auto p = r.client.producer(pc);
    AckLog log;
    p.onAck(&AckLog::on, &log);
    for (uint64_t t = 1; t <= 3; ++t) {
        ASSERT_TRUE(sendTok(r, p, t));
        ASSERT_TRUE(r.drive([&] { return r.gw.log.size() == t; }));
    }
    ASSERT_EQ(r.gw.pendingAnswers(), 3u) << "acks=all OKs deferred at A";
    std::vector<int64_t> origOffsets;
    for (const auto& lb : r.gw.log) origOffsets.push_back(lb.baseOffset);
    const int16_t epoch0 = r.gw.produced.front().producerEpoch;

    // Ownership moves to B.  A refuses what it no longer owns.
    r.gw.partitions         = {{1, 0, 0, 2, 3924, "127.0.0.1:24793"}};
    r.gw.notLeaderEndpoints = {wA};
    ASSERT_TRUE(sendTok(r, p, 4));
    ASSERT_TRUE(r.drive([&] { return r.gw.notLeaderAnswers == 1; })) << "batch 4 never reached A";
    r.gw.okDelayPumps = 0;
    r.gw.failPendingAt(wA, static_cast<int16_t>(rp::ErrorCode::NotLeader));  // moved-replica waiters

    ASSERT_TRUE(r.drive([&] { return log.seen.size() == 4; })) << "stored batches not re-offered to B";
    for (int i = 0; i < 2'000; ++i) {  // nothing more may complete or be sent anywhere
        r.client.doWork();
        r.gw.pump();
    }
    ASSERT_EQ(log.seen.size(), 4u) << "a record completed twice";
    EXPECT_TRUE(log.allOk()) << "a record failed across the move";
    for (rc::Token t = 1; t <= 4; ++t) EXPECT_EQ(log.count(t), 1) << "token " << t;
    for (std::size_t i = 0; i < 4; ++i) {
        if (log.seen[i].first <= 3) {
            EXPECT_EQ(log.offsets[i], origOffsets[static_cast<std::size_t>(log.seen[i].first - 1)])
                << "a stored batch must report its original offset (window duplicate at B)";
        }
    }
    ASSERT_EQ(r.gw.log.size(), 4u) << "a batch was written twice";
    EXPECT_EQ(r.gw.windowDupAnswers, 3) << "B must see 1..3 as duplicates of the same identity";
    EXPECT_EQ(r.gw.oosAnswers, 0);
    EXPECT_EQ(r.gw.fencedAnswers, 0);
    int atB = 0;
    for (const auto& pr : r.gw.produced) {
        EXPECT_EQ(pr.producerEpoch, epoch0) << "epoch bumped";
        atB += pr.endpoint == wB;
    }
    EXPECT_EQ(atB, 4) << "each stored frame re-offered to B exactly once";
    EXPECT_FALSE(r.client.testRouteParked(1, 0));
    // Re-review 1 NEW-2: the re-offered frames (new attempt tag in the header
    // correlationId) still pass the broker's CRC32C and frame/header identity checks.
    EXPECT_EQ(r.gw.crcFailures, 0);
    EXPECT_EQ(r.gw.identityMismatches, 0);
}

// I1: the bare ErrorResponse{NotLeader} retry goes through the partition's route
// (the worker data channel), never the seed's default publication.
TEST(IngressEndpointRouting, BareNotLeaderRetryFollowsThePartitionRouteRegression) {
    Rig r(3912, 3913);
    const int w = r.gw.addEndpoint("aeron:udp?endpoint=127.0.0.1:24783", 3912);
    r.gw.partitions = {{1, 0, 0, 1, 3912, "127.0.0.1:24783"}};
    r.connect();
    ASSERT_TRUE(r.drive([&] { return !r.client.testDataChannelFor(1, 0).empty(); }));
    auto p = r.client.producer(noLinger());
    r.gw.errorNotLeaderNext = 1;
    ASSERT_EQ(r.sendAndAwait(p).status, rc::OpStatus::Ok);
    ASSERT_EQ(r.gw.produced.size(), 2u) << "the NotLeader'd frame was not retried";
    for (const auto& pr : r.gw.produced) EXPECT_EQ(pr.endpoint, w) << "a retry left the partition route";
}

// M1: a stale (lower-epoch) entry with no ingressEndpoint — a lagging / legacy
// node — must not tear down the live worker route.
TEST(IngressEndpointRouting, StaleMetadataLeavesTheWorkerRouteRegression) {
    Rig r(3914, 3915);
    r.gw.addEndpoint("aeron:udp?endpoint=127.0.0.1:24784", 3914);
    r.gw.partitions = {{1, 0, 0, 2, 3914, "127.0.0.1:24784"}};
    r.connect();
    ASSERT_TRUE(r.drive([&] { return !r.client.testDataChannelFor(1, 0).empty(); }));
    const int seen = r.client.testMetadataResponses();
    r.gw.brokerHost = "127.0.0.1";  // a node route the legacy branch could build
    r.gw.brokerPort = 24785;
    r.gw.partitions = {{1, 0, 0, 1, 3914, ""}};  // epoch 1 < 2: stale
    r.gw.sendMetadata(r.gw.assignedSessionId, 0);
    ASSERT_TRUE(r.drive([&] { return r.client.testMetadataResponses() > seen; }));
    EXPECT_EQ(r.client.testDataChannelFor(1, 0), "aeron:udp?endpoint=127.0.0.1:24784");
}

// M2 (departure 1 pinned): a batch already sent to the node is NOT re-offered
// when the first worker route installs — the node's gateway relays it and its
// ack arrives there: exactly one frame, exactly one completion.
TEST(IngressEndpointRouting, FirstWorkerInstallDoesNotReofferNodeBatches) {
    Rig r(3916, 3917);
    const int w = r.gw.addEndpoint("aeron:udp?endpoint=127.0.0.1:24786", 3916);
    r.gw.partitions = {{1, 0, 0, 1, 3916, ""}};
    r.connect();
    ASSERT_TRUE(r.drive([&] { return r.client.testMetadataResponses() > 0; }));
    auto p = r.client.producer(noLinger());
    AckLog log;
    p.onAck(&AckLog::on, &log);
    r.gw.holdPartitions = {0};
    ASSERT_TRUE(sendTok(r, p, 1));
    ASSERT_TRUE(r.drive([&] { return r.gw.held.size() == 1; }));
    ASSERT_EQ(r.gw.held[0].endpoint, 0);

    r.gw.partitions = {{1, 0, 0, 1, 3916, "127.0.0.1:24786"}};
    r.gw.sendMetadata(r.gw.assignedSessionId, 0);
    ASSERT_TRUE(r.drive([&] { return !r.client.testDataChannelFor(1, 0).empty(); }));
    for (int i = 0; i < 2'000; ++i) {
        r.client.doWork();
        r.gw.pump();
    }
    EXPECT_EQ(r.gw.produced.size(), 1u) << "node batch re-offered on the first worker install";
    r.gw.holdPartitions.clear();
    r.gw.releaseHeld();
    ASSERT_TRUE(r.drive([&] { return log.seen.size() == 1; }));
    EXPECT_EQ(log.count(1), 1);
    EXPECT_TRUE(log.allOk());
    // New batches go straight to the worker.
    ASSERT_TRUE(sendTok(r, p, 2));
    ASSERT_TRUE(r.drive([&] { return log.count(2) == 1; }));
    EXPECT_EQ(r.gw.produced.back().endpoint, w);
    EXPECT_EQ(r.gw.produced.size(), 2u);
}

// M4: once no route references the old worker endpoint, its publication closes.
TEST(IngressEndpointRouting, RouteMoveClosesTheOldWorkerPublicationRegression) {
    Rig r(3918, 3919);
    const std::string chA = "aeron:udp?endpoint=127.0.0.1:24787";
    r.gw.addEndpoint(chA, 3918);
    const int wB = r.gw.addEndpoint("aeron:udp?endpoint=127.0.0.1:24788", 3918);
    r.gw.partitions = {{1, 0, 0, 1, 3918, "127.0.0.1:24787"}};
    r.connect();
    ASSERT_TRUE(r.drive([&] { return !r.client.testDataChannelFor(1, 0).empty(); }));
    auto p = r.client.producer(noLinger());
    ASSERT_EQ(r.sendAndAwait(p).status, rc::OpStatus::Ok);
    EXPECT_TRUE(r.client.testPublicationOpen(chA, 3918));

    r.gw.partitions = {{1, 0, 0, 2, 3918, "127.0.0.1:24788"}};
    r.gw.sendMetadata(r.gw.assignedSessionId, 0);
    ASSERT_TRUE(r.drive([&] { return r.client.testDataChannelFor(1, 0) == "aeron:udp?endpoint=127.0.0.1:24788"; }));
    ASSERT_EQ(r.sendAndAwait(p).status, rc::OpStatus::Ok);
    EXPECT_EQ(r.gw.produced.back().endpoint, wB);
    EXPECT_FALSE(r.client.testPublicationOpen(chA, 3918)) << "old worker publication leaked";
}

// M5: a spurious Redirect (same leader) resets the partition onto the node route
// and bumps the cache epoch provisionally; the metadata it requests at once must
// restore the worker route (the provisional epoch never blocks the answer).
TEST(IngressEndpointRouting, SpuriousRedirectRestoresTheWorkerRouteRegression) {
    Rig r(3920, 3921, "aeron:ipc", "aeron:ipc", /*realClock=*/true);
    const std::string node = "aeron:udp?endpoint=127.0.0.1:24789";
    const int n = r.gw.addEndpoint(node, 3920);
    const int w = r.gw.addEndpoint("aeron:udp?endpoint=127.0.0.1:24790", 3920);
    r.gw.brokerHost = "127.0.0.1";
    r.gw.brokerPort = 24789;
    r.gw.partitions = {{1, 0, 0, 1, 3920, "127.0.0.1:24790"}};
    r.connect();
    const std::string worker = "aeron:udp?endpoint=127.0.0.1:24790";
    ASSERT_TRUE(r.drive([&] { return r.client.testDataChannelFor(1, 0) == worker; }));
    auto p = r.client.producer(noLinger());
    ASSERT_EQ(r.sendAndAwait(p).status, rc::OpStatus::Ok);
    ASSERT_EQ(r.gw.produced.back().endpoint, w);

    r.gw.redirectNext = 1;
    r.gw.redirectTo   = node;  // the same leader node: a spurious redirect
    const int mdBefore = r.gw.metadataRequests;
    ASSERT_EQ(r.sendAndAwait(p).status, rc::OpStatus::Ok);
    EXPECT_GT(r.gw.metadataRequests, mdBefore) << "a Redirect did not refresh metadata";
    bool viaNode = false;
    for (const auto& pr : r.gw.produced) viaNode |= pr.endpoint == n;
    EXPECT_TRUE(viaNode) << "the redirected batch was not retried on the node";
    ASSERT_TRUE(r.drive([&] { return r.client.testDataChannelFor(1, 0) == worker; }))
        << "worker route stranded on the node after a spurious redirect";
    ASSERT_EQ(r.sendAndAwait(p).status, rc::OpStatus::Ok);
    EXPECT_EQ(r.gw.produced.back().endpoint, w);
}

// Re-review 1 NEW-1: a MetadataResponse REQUESTED BEFORE a real Redirect (in
// flight, or a lagging re-delivery) still carries the old leader at the old
// epoch.  It must never undo the redirect: while the redirect's provisional
// leader stands, an answer at the confirmed epoch is accepted only when it was
// requested after the redirect (MetadataCache provisionalSince, by request
// correlationId).  The answer requested after it then decides (M5 kept: the
// SpuriousRedirect test above).  RED (pre-fix): the stale answer restored the
// old worker route and the next batch went back to the old owner.
TEST(IngressEndpointRouting, LaggingMetadataNeverUndoesARealRedirectRegression) {
    Rig r(3924, 3925, "aeron:ipc", "aeron:ipc", /*realClock=*/true);
    const std::string node0 = "aeron:udp?endpoint=127.0.0.1:24776";
    const std::string node1 = "aeron:udp?endpoint=127.0.0.1:24778";
    const std::string worker = "aeron:udp?endpoint=127.0.0.1:24777";
    r.gw.addEndpoint(node0, 3924);
    const int w  = r.gw.addEndpoint(worker, 3924);
    const int n1 = r.gw.addEndpoint(node1, 3924);
    r.gw.brokerHost  = "127.0.0.1";
    r.gw.brokerPort  = 24776;
    r.gw.moreBrokers = {{1, "127.0.0.1", 24778}};
    r.gw.partitions  = {{1, 0, 0, 1, 3924, "127.0.0.1:24777"}};
    r.connect();
    ASSERT_TRUE(r.drive([&] { return r.client.testDataChannelFor(1, 0) == worker; }));
    auto p = r.client.producer(noLinger());
    ASSERT_EQ(r.sendAndAwait(p).status, rc::OpStatus::Ok);
    ASSERT_EQ(r.gw.produced.back().endpoint, w);
    ASSERT_FALSE(r.gw.metadataCorrIds.empty());
    const int64_t preRedirectCorr = r.gw.metadataCorrIds.back();  // answered before the redirect

    // The partition really moved to node 1: the worker redirects.  Metadata
    // requests are held from now on; the test answers them itself.
    r.gw.holdMetadata = true;
    r.gw.redirectNext = 1;
    r.gw.redirectTo   = node1;
    ASSERT_EQ(r.sendAndAwait(p).status, rc::OpStatus::Ok);
    ASSERT_EQ(r.gw.produced.back().endpoint, n1) << "precondition: the redirected batch went to node 1";
    ASSERT_EQ(r.client.testLeaderFor(1, 0), 1);
    ASSERT_EQ(r.client.testEpochFor(1, 0), 2) << "precondition: the provisional epoch";
    ASSERT_GT(r.gw.metadataCorrIds.back(), preRedirectCorr) << "precondition: the redirect asked for metadata";
    const int64_t postRedirectCorr = r.gw.metadataCorrIds.back();

    // The stale answer (requested before the redirect): old leader, old epoch.
    const int before = r.client.testMetadataResponses();
    r.gw.sendMetadata(r.gw.assignedSessionId, preRedirectCorr);
    ASSERT_TRUE(r.drive([&] { return r.client.testMetadataResponses() > before; }));
    EXPECT_EQ(r.client.testLeaderFor(1, 0), 1) << "a pre-redirect answer undid the redirect";
    EXPECT_EQ(r.client.testEpochFor(1, 0), 2);
    EXPECT_EQ(r.client.testDataChannelFor(1, 0), node1) << "the route went back to the old owner";
    ASSERT_EQ(r.sendAndAwait(p).status, rc::OpStatus::Ok);
    EXPECT_EQ(r.gw.produced.back().endpoint, n1) << "a batch went back to the old owner";

    // The answer requested after the redirect confirms node 1 at epoch 2.
    r.gw.partitions = {{1, 0, 1, 2, 3924, ""}};
    r.gw.sendMetadata(r.gw.assignedSessionId, postRedirectCorr);
    ASSERT_TRUE(r.drive([&] { return r.client.testMetadataResponses() > before + 1; }));
    EXPECT_EQ(r.client.testLeaderFor(1, 0), 1);
    EXPECT_EQ(r.client.testEpochFor(1, 0), 2);
    ASSERT_EQ(r.sendAndAwait(p).status, rc::OpStatus::Ok);
    EXPECT_EQ(r.gw.produced.back().endpoint, n1);
}

// M6: the node endpoint advertised in metadata names the seed with different URI
// params — the same destination: no route, no second Session.
TEST(IngressEndpointRouting, NodeEndpointWithOtherUriParamsIsTheSeedRegression) {
    EXPECT_TRUE(rune::client::agent::ClientAgent::sameEndpoint("aeron:udp?endpoint=10.0.0.1:40500|mtu=8192",
                                                               "aeron:udp?endpoint=10.0.0.1:40500"));
    EXPECT_TRUE(rune::client::agent::ClientAgent::sameEndpoint("aeron:udp?term-length=65536|endpoint=10.0.0.1:40500",
                                                               "10.0.0.1:40500"));
    EXPECT_FALSE(rune::client::agent::ClientAgent::sameEndpoint("aeron:udp?endpoint=10.0.0.1:40500",
                                                                "aeron:udp?endpoint=10.0.0.1:40501"));
    EXPECT_FALSE(rune::client::agent::ClientAgent::sameEndpoint("aeron:ipc", "aeron:udp?endpoint=10.0.0.1:40500"));
    EXPECT_TRUE(rune::client::agent::ClientAgent::sameEndpoint("aeron:ipc", "aeron:ipc"));
    EXPECT_FALSE(rune::client::agent::ClientAgent::sameEndpoint("aeron:ipc?alias=a", "aeron:ipc?alias=b"));

    Rig r(3922, 3923, "aeron:udp?endpoint=127.0.0.1:24791|mtu=1408", "aeron:udp?endpoint=127.0.0.1:24791",
          /*realClock=*/true);
    r.gw.brokerHost = "127.0.0.1";
    r.gw.brokerPort = 24791;
    r.gw.partitions = {{1, 0, 0, 1, 3922, ""}};  // legacy node route == the seed
    r.connect();
    ASSERT_TRUE(r.drive([&] { return r.client.testMetadataResponses() > 0; }));
    auto p = r.client.producer(noLinger());
    ASSERT_EQ(r.sendAndAwait(p).status, rc::OpStatus::Ok);
    EXPECT_TRUE(r.client.testDataChannelFor(1, 0).empty()) << "a route to the seed itself";
    EXPECT_EQ(r.client.testSessionCount(), 1u) << "a second Session to the seed node";
}



// Re-review 2 R2-1 (M5 regression): a BURST of spurious redirects for one
// partition (two in-flight batches each redirected).  The second redirect
// re-provisions with a later provisionalSince; the only request (sent by the
// first) then predates it and its equal-epoch answer is discarded -- and the
// answer also disarms the backoff refresh the rate-limited second redirect armed.
TEST(IngressEndpointRoutingRegression, SpuriousRedirectBurstRestoresTheWorkerRouteRegression) {
    Rig r(3961, 3962, "aeron:ipc", "aeron:ipc", /*realClock=*/true);
    const std::string node = "aeron:udp?endpoint=127.0.0.1:24841";
    const int n = r.gw.addEndpoint(node, 3961);
    const int w = r.gw.addEndpoint("aeron:udp?endpoint=127.0.0.1:24842", 3961);
    (void)n;
    r.gw.brokerHost = "127.0.0.1";
    r.gw.brokerPort = 24841;
    r.gw.partitions = {{1, 0, 0, 1, 3961, "127.0.0.1:24842"}};
    r.connect();
    const std::string worker = "aeron:udp?endpoint=127.0.0.1:24842";
    ASSERT_TRUE(r.drive([&] { return r.client.testDataChannelFor(1, 0) == worker; }));
    auto p = r.client.producer(noLinger());
    ASSERT_EQ(r.sendAndAwait(p).status, rc::OpStatus::Ok);
    ASSERT_EQ(r.gw.produced.back().endpoint, w);

    r.gw.redirectNext = 2;
    r.gw.redirectTo   = node;  // spurious: the same leader node
    const rc::Token t1 = r.nextToken++;
    const rc::Token t2 = r.nextToken++;
    ASSERT_EQ(p.trySend(r.client.topic("t"), rc::Key(uint64_t{1}), std::span<const uint8_t>(kVal), r.cq, t1),
              rc::SendResult::Ok);
    r.client.doWork();
    ASSERT_EQ(p.trySend(r.client.topic("t"), rc::Key(uint64_t{1}), std::span<const uint8_t>(kVal), r.cq, t2),
              rc::SendResult::Ok);
    int okCount = 0;
    ASSERT_TRUE(r.drive([&] {
        r.cq.poll([&](const rc::Completion& c, std::span<const uint8_t>) {
            if ((c.token == t1 || c.token == t2) && c.status == rc::OpStatus::Ok) ++okCount;
        });
        return okCount == 2;
    }));
    // Give it ~3 s of wall time (>> the 100 ms urgent gap and the initial backoff).
    const auto until = std::chrono::steady_clock::now() + std::chrono::seconds(3);
    bool restored = false;
    while (std::chrono::steady_clock::now() < until && !restored) {
        r.client.doWork();
        r.gw.pump();
        restored = r.client.testDataChannelFor(1, 0) == worker;
    }
    EXPECT_TRUE(restored) << "worker route stranded on the node after a burst of spurious redirects";
}

// Review of 54a2082 (Q2a + Q4 gap): a redirect's guess that NO metadata answer
// ever confirms (the answers omit the partition) must keep the refresh cycle
// alive (>= 2 requests) but back off exponentially — never poll every 100 ms
// forever (~30 requests in 3 s).
TEST(IngressEndpointRoutingRegression, UnconfirmedRedirectGuessRefreshBacksOffRegression) {
    Rig r(3963, 3964, "aeron:ipc", "aeron:ipc", /*realClock=*/true);
    const std::string node = "aeron:udp?endpoint=127.0.0.1:24843";
    (void)r.gw.addEndpoint(node, 3963);
    const int w = r.gw.addEndpoint("aeron:udp?endpoint=127.0.0.1:24844", 3963);
    r.gw.brokerHost = "127.0.0.1";
    r.gw.brokerPort = 24843;
    r.gw.partitions = {{1, 0, 0, 1, 3963, "127.0.0.1:24844"}};
    r.connect();
    const std::string worker = "aeron:udp?endpoint=127.0.0.1:24844";
    ASSERT_TRUE(r.drive([&] { return r.client.testDataChannelFor(1, 0) == worker; }));
    auto p = r.client.producer(noLinger());
    ASSERT_EQ(r.sendAndAwait(p).status, rc::OpStatus::Ok);
    ASSERT_EQ(r.gw.produced.back().endpoint, w);

    r.gw.redirectNext = 1;
    r.gw.redirectTo   = node;
    r.gw.partitions.clear();               // no answer will ever confirm the guess
    const int before = r.gw.metadataRequests;
    const rc::Token t1 = r.nextToken++;
    ASSERT_EQ(p.trySend(r.client.topic("t"), rc::Key(uint64_t{1}), std::span<const uint8_t>(kVal), r.cq, t1),
              rc::SendResult::Ok);
    bool done = false;
    ASSERT_TRUE(r.drive([&] {
        r.cq.poll([&](const rc::Completion& c, std::span<const uint8_t>) { if (c.token == t1) done = true; });
        return done;
    }));
    const auto until = std::chrono::steady_clock::now() + std::chrono::seconds(3);
    while (std::chrono::steady_clock::now() < until) {
        r.client.doWork();
        r.gw.pump();
    }
    const int requests = r.gw.metadataRequests - before;
    EXPECT_GE(requests, 2) << "an unconfirmed guess must keep the refresh cycle alive";
    EXPECT_LE(requests, 8) << "the refresh must back off, not poll every 100 ms (" << requests << " in 3 s)";
}

// DR-15 SIGTERM parity (Docker, 2026-10-05): right after a node stopped, a
// subscriber's Subscribe was redirected to THAT node; the client could not
// retarget to it (host gone) and finished the subscription with
// "Redirect retarget failed" — 150 acked records never consumed.  A redirect to
// an unreachable node is never terminal: the client keeps its session, asks for
// metadata and re-sends the Subscribe on the answer (the cluster has moved on).
TEST(IngressEndpointRouting, SubscribeRedirectedToAnUnreachableNodeIsRetriedRegression) {
    Rig r(3930, 3931, "aeron:ipc", "aeron:ipc", /*realClock=*/true);
    r.connect();
    r.gw.subscribeRedirectNext = 1;
    r.gw.subscribeRedirectTo   = "rune-unresolvable.invalid:40200";
    const int md = r.gw.metadataRequests;
    auto sub = r.client.subscribe(r.client.topic("t"), 0, rc::SubscribeOptions{rc::From::earliest()},
                                  [](const rc::Record&) {});
    ASSERT_TRUE(r.drive([&] { return r.gw.latestSubscription(1, 0) != nullptr || sub.finished(); }))
        << "the redirected Subscribe was never re-sent";
    EXPECT_FALSE(sub.finished()) << "a redirect to an unreachable node finished the subscription";
    EXPECT_NE(r.gw.latestSubscription(1, 0), nullptr);
    EXPECT_GT(r.gw.metadataRequests, md) << "no metadata asked after the failed retarget";
}
