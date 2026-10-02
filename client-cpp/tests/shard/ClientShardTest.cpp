// tests/shard/ClientShardTest.cpp — T15b: ClientShard (Invoker: single-threaded
// against the InvokerGateway) and ShardGroup (Agent mode: routing to the owning
// shard, connectAll, all-or-nothing construction).
//
// Rules (F8c): no std::atomic / mutex / future in tests, no sleeps.  Drive loops
// wait on progress with a 10 s failure deadline.  The ONLY real-time wait is the
// stalled-owner test, which yields on Aeron's own conductor clock until
// ClientConductor.h:436's condition holds (Aeron's clock cannot be injected).
// Cross-thread hand-offs in the Agent tests use pipes (the kernel orders them)
// and thread join.
#include "../support/InvokerGateway.h"

#include <rune/client/shard/ClientShard.h>

#include <Aeron.h>  // aeron::currentTimeMillis — the conductor's own clock (stall test)
#include <gtest/gtest.h>

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <map>
#include <memory>
#include <span>
#include <stdexcept>
#include <string>
#include <thread>
#include <sys/select.h>
#include <unistd.h>
#include <vector>

namespace rc = rune::client;
namespace rs = rune::client::shard;
using namespace std::chrono_literals;

namespace {
int g_ctr = 0;  // test thread only
const std::vector<uint8_t> kVal(32, 0x21);

rs::ShardGroupConfig group(int32_t in, int32_t eg, uint32_t count, rs::ShardMode mode,
                           int32_t partitions = 8) {
    rs::ShardGroupConfig g;
    g.base.gatewayChannel  = "aeron:ipc";
    g.base.gatewayStreamId = in;
    g.base.egressChannel   = "aeron:ipc";
    g.base.egressStreamId  = eg;
    g.base.dataDir = "/tmp/shard_" + std::to_string(::getpid()) + "_" + std::to_string(g_ctr++);
    rc::TopicSpec ts;
    ts.name           = "t";
    ts.topicId        = 1;
    ts.partitionCount = partitions;
    g.base.topics.push_back(ts);
    g.shardCount      = count;
    g.mode            = mode;
    g.producer.acks   = rc::Acks::Leader;
    g.producer.lingerUs = 0;
    return g;
}

rc::Key keyFor(int32_t partition, int32_t partitions) {
    for (uint64_t k = 0; k < 1'000'000; ++k) {
        if (rc::Key(k).partition(partitions) == partition) return rc::Key(k);
    }
    throw std::runtime_error("no key maps to the partition");
}

// Drive until pred holds; the 10 s deadline only bounds a FAILING test.
template <class Step, class P>
bool driveUntil(Step&& step, P&& pred) {
    const auto deadline = std::chrono::steady_clock::now() + 10s;
    while (std::chrono::steady_clock::now() < deadline) {
        step();
        if (pred()) return true;
    }
    return false;
}

struct ShardRig {
    rs::ClientShard              shard;  // owns the driver: constructed first, destroyed last
    test_support::InvokerGateway gw;
    ShardRig(const rs::ShardGroupConfig& g, uint32_t i)
        : shard(g, i),
          gw(rs::shardClientConfig(g, i).dataDir + "/aeron", g.base.gatewayChannel,
             g.base.gatewayStreamId, rs::shardClientConfig(g, i).egressChannel, g.base.egressStreamId) {}
    template <class P>
    bool drive(P&& pred) {
        return driveUntil(
            [&] {
                shard.doWork();
                gw.pump();
            },
            pred);
    }
    // F8c: connect(target, Token) into a Completions owned by this (owner)
    // thread; the completion is polled while driving shard + gateway.
    void connect() {
        ASSERT_TRUE(drive([&] { return gw.ready(); }));
        rc::Completions cq(shard.client());
        ASSERT_EQ(shard.connect(cq, 1), rc::OpResult::Accepted);
        bool        done = false;
        rc::OpStatus st  = rc::OpStatus::Closed;
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

// A pipe used as a one-shot signal between two test threads (the kernel orders
// the write before the read; no shared flag).
struct Signal {
    int fd[2] = {-1, -1};
    Signal() {
        if (::pipe(fd) != 0) throw std::runtime_error("pipe");
    }
    ~Signal() {
        ::close(fd[0]);
        ::close(fd[1]);
    }
    void raise() const {
        const char b = 1;
        (void)!::write(fd[1], &b, 1);
    }
    void wait() const {
        char b = 0;
        (void)!::read(fd[0], &b, 1);
    }
    // Non-blocking check (select(2) with a zero timeout).
    [[nodiscard]] bool raised() const {
        fd_set s;
        FD_ZERO(&s);
        FD_SET(fd[0], &s);
        timeval tv{0, 0};
        return ::select(fd[0] + 1, &s, nullptr, nullptr, &tv) == 1;
    }
};
} // namespace

TEST(ClientShardRegression, InvokerShardProducesAndAcksWithNoThreads) {
    ShardRig r(group(3800, 3801, 1, rs::ShardMode::Invoker), 0);
    r.connect();
    const auto topic = r.shard.topic("t");
    for (uint64_t i = 0; i < 100; ++i) ASSERT_EQ(r.shard.trySend(topic, rc::Key(i), kVal), rc::SendResult::Ok);
    int64_t                    acked      = 0;
    int                        errors     = 0;
    bool                       contiguous = true;
    std::map<int32_t, int32_t> nextSeq;
    ASSERT_TRUE(r.drive([&] {
        r.shard.pollBatchAcks([&](const rc::BatchAck& a) {
            acked += a.recordCount;
            if (a.errorCode != 0) ++errors;
            if (a.baseSequence != nextSeq[a.partition]) contiguous = false;
            nextSeq[a.partition] = a.baseSequence + a.recordCount;
        });
        return acked >= 100;
    }));
    EXPECT_EQ(acked, 100) << "exactly one batch ack per record";
    EXPECT_EQ(errors, 0);
    EXPECT_TRUE(contiguous);
}

TEST(ClientShardRegression, RecordsOfOnePassLeaveAsOneFramePerPartition) {
    ShardRig r(group(3802, 3803, 1, rs::ShardMode::Invoker), 0);
    r.connect();
    const auto    topic = r.shard.topic("t");
    const rc::Key k     = keyFor(3, 8);
    for (int i = 0; i < 10; ++i) ASSERT_EQ(r.shard.trySend(topic, k, kVal), rc::SendResult::Ok);
    const std::size_t before = r.gw.produced.size();
    ASSERT_TRUE(r.drive([&] { return r.gw.produced.size() > before; }));
    EXPECT_EQ(r.gw.produced.back().records, 10) << "one pass = one frame per partition (no linger timer)";
}

TEST(ClientShardRegression, RejectsPartitionsOwnedByAnotherShard) {
    ShardRig   r(group(3804, 3805, 2, rs::ShardMode::Invoker), 0);
    const auto topic   = r.shard.topic("t");
    int32_t    foreign = -1;
    for (int32_t p = 0; p < 8 && foreign < 0; ++p) {
        if (!r.shard.owns(1, p)) foreign = p;
    }
    ASSERT_GE(foreign, 0);
    EXPECT_EQ(r.shard.trySend(topic, keyFor(foreign, 8), kVal), rc::SendResult::WrongShard);
    EXPECT_THROW(r.shard.send(topic, keyFor(foreign, 8), kVal), std::logic_error);
    // Nothing was accepted: no batch ack ever follows.
    int polled = 0;
    for (int i = 0; i < 100; ++i) {
        r.shard.doWork();
        polled += r.shard.pollBatchAcks([](const rc::BatchAck&) {});
    }
    EXPECT_EQ(polled, 0);
}

TEST(ClientShardRegression, DoWorkIsInvokerOnly) {
    auto            g = group(3808, 3809, 1, rs::ShardMode::Agent);
    rs::ClientShard s(g, 0);
    EXPECT_THROW((void)s.doWork(), std::logic_error);
    EXPECT_EQ(s.pollBatchAcks([](const rc::BatchAck&) {}), 0) << "Agent: no batch-ack queue";
}

// Review Focus 1: an owner stall past the Aeron client liveness timeout turns
// sends into Closed — no ::exit, no hang.  Aeron's conductor clock cannot be
// injected (Aeron.cpp:43 hard-wires currentTimeMillis), so the test yields on
// that clock until ClientConductor.h:436's condition holds — no tuned sleep.
TEST(ClientShardRegression, StalledOwnerTurnsSendsIntoClosed) {
    auto g = group(3806, 3807, 1, rs::ShardMode::Invoker);
    // T15a I4: shards take the liveness from the group (default 1 h); this is
    // an explicit override to the smallest value validate() accepts (1 s = 2x
    // the Aeron client's 500 ms keepalive; Task 6's stall test uses the same).
    g.aeronClientLivenessTimeoutNs = rune::client::transport::kMinClientLivenessTimeoutNs;
    ShardRig r(g, 0);
    r.connect();
    // Stall exactly past Aeron's own condition (same clock, same comparison as
    // ClientConductor.h:436: nowMs > timeOfLastDoWorkMs + interServiceTimeoutMs);
    // t0 is taken after connect()'s last doWork.  No tuned sleep.
    const long long livenessMs = g.aeronClientLivenessTimeoutNs / 1'000'000;
    const long long t0         = aeron::currentTimeMillis();
    while (aeron::currentTimeMillis() <= t0 + livenessMs) std::this_thread::yield();
    ASSERT_TRUE(r.drive([&] { return r.shard.closed(); }));
    EXPECT_EQ(r.shard.trySend(r.shard.topic("t"), rc::Key(uint64_t{1}), kVal), rc::SendResult::Closed);
    EXPECT_THROW(r.shard.send(r.shard.topic("t"), rc::Key(uint64_t{1}), kVal), std::runtime_error);
}

TEST(ShardGroupRegression, AgentModeRoutesEachSendToTheOwningShard) {
    auto g          = group(3810, 3811, 2, rs::ShardMode::Agent);
    g.producer.acks = rc::Acks::None;
    rs::ShardGroup grp(g);
    ASSERT_EQ(grp.size(), 2u);
    test_support::InvokerGateway gw0(rs::shardClientConfig(g, 0).dataDir + "/aeron", "aeron:ipc", 3810,
                                     "aeron:ipc", 3811);
    test_support::InvokerGateway gw1(rs::shardClientConfig(g, 1).dataDir + "/aeron", "aeron:ipc", 3810,
                                     "aeron:ipc", 3811);
    // Agent mode: each shard's runner pumps its own driver; the test thread
    // pumps only the gateways (yield between rounds, no sleep).
    auto pumpUntil = [&](auto&& pred) {
        return driveUntil(
            [&] {
                gw0.pump();
                gw1.pump();
                std::this_thread::yield();
            },
            pred);
    };
    ASSERT_TRUE(pumpUntil([&] { return gw0.ready() && gw1.ready(); }));
    // F8c: one Completions per shard, owned by this thread; each shard's runner
    // completes its connect token into it.
    rc::Completions cq0(grp.shard(0).client());
    rc::Completions cq1(grp.shard(1).client());
    ASSERT_EQ(grp.shard(0).connect(cq0, 10), rc::OpResult::Accepted);
    ASSERT_EQ(grp.shard(1).connect(cq1, 11), rc::OpResult::Accepted);
    int  connected = 0, failed = 0;
    auto onConnect = [&](const rc::Completion& c, std::span<const uint8_t>) {
        (c.status == rc::OpStatus::Ok ? connected : failed) += 1;
    };
    ASSERT_TRUE(pumpUntil([&] {
        cq0.poll(onConnect);
        cq1.poll(onConnect);
        return connected + failed == 2;
    }));
    ASSERT_EQ(failed, 0);
    const auto topic = grp.shard(0).topic("t");
    for (int32_t p = 0; p < 8; ++p) grp.send(topic, keyFor(p, 8), kVal);
    ASSERT_TRUE(pumpUntil([&] { return gw0.produced.size() + gw1.produced.size() == 8; }));
    for (const auto& pr : gw0.produced) EXPECT_EQ(rs::shardFor(1, pr.partition, 2), 0u);
    for (const auto& pr : gw1.produced) EXPECT_EQ(rs::shardFor(1, pr.partition, 2), 1u);
    EXPECT_EQ(grp.trySend(rc::TopicHandle{}, rc::Key(uint64_t{1}), kVal), rc::SendResult::InvalidTopic);
}

// connectAll() blocks on this (control) thread while each runner connects; the
// gateways therefore live on a helper thread.  After the sends, every shard's
// blocking producer close() returns only once its records are acked — then the
// helper stops and the test reads what each gateway saw (after join).
TEST(ShardGroupRegression, ConnectAllThenEveryShardDrainsItsOwnPartitions) {
    auto g = group(3812, 3813, 2, rs::ShardMode::Agent);
    rs::ShardGroup grp(g);
    ASSERT_EQ(grp.size(), 2u);
    const std::string dir0 = rs::shardClientConfig(g, 0).dataDir + "/aeron";
    const std::string dir1 = rs::shardClientConfig(g, 1).dataDir + "/aeron";
    Signal                     ready, stop;
    std::vector<int32_t>       seen0, seen1;
    bool                       helperOk = false;
    std::thread helper([&] {
        test_support::InvokerGateway gw0(dir0, "aeron:ipc", 3812, "aeron:ipc", 3813);
        test_support::InvokerGateway gw1(dir1, "aeron:ipc", 3812, "aeron:ipc", 3813);
        const auto deadline = std::chrono::steady_clock::now() + 10s;
        while (!(gw0.ready() && gw1.ready()) && std::chrono::steady_clock::now() < deadline) {
            gw0.pump();
            gw1.pump();
        }
        ready.raise();
        while (!stop.raised()) {
            if (gw0.pump() + gw1.pump() == 0) std::this_thread::yield();
        }
        for (const auto& pr : gw0.produced) seen0.push_back(pr.partition);
        for (const auto& pr : gw1.produced) seen1.push_back(pr.partition);
        helperOk = gw0.ready() && gw1.ready();
    });
    ready.wait();
    ASSERT_NO_THROW(grp.connectAll(10s));
    const auto topic = grp.shard(0).topic("t");
    for (int32_t p = 0; p < 8; ++p) ASSERT_EQ(grp.trySend(topic, keyFor(p, 8), kVal), rc::SendResult::Ok);
    for (uint32_t i = 0; i < grp.size(); ++i) {
        EXPECT_EQ(grp.shard(i).producer().closeAndWait(10s), rc::OpStatus::Ok) << "shard " << i;
    }
    stop.raise();
    helper.join();
    ASSERT_TRUE(helperOk);
    EXPECT_EQ(seen0.size() + seen1.size(), 8u);
    for (int32_t p : seen0) EXPECT_EQ(rs::shardFor(1, p, 2), 0u);
    for (int32_t p : seen1) EXPECT_EQ(rs::shardFor(1, p, 2), 1u);
    // Closed intakes report Closed synchronously.
    EXPECT_EQ(grp.trySend(topic, keyFor(0, 8), kVal), rc::SendResult::Closed);
}

TEST(ShardGroupRegression, AutoShardCountIsResolvedOnceAndRoutesWithIt) {
    // AUTO (the default): min(allowed CPUs, quota) - reserved, capped at the
    // largest partition count (2 here) -> 2 on any box with >= 2 CPUs.
    auto g       = group(3814, 3815, rs::kAutoShards, rs::ShardMode::Agent, 2);
    const auto n = rs::resolveShardCount(g, rs::platformAllowedCpus(), rs::platformQuotaCpus()).count;
    rs::ShardGroup grp(g);
    ASSERT_EQ(grp.size(), n);
    for (int32_t p = 0; p < 2; ++p) EXPECT_EQ(&grp.owner(1, p), &grp.shard(rs::shardFor(1, p, n)));
    for (uint32_t i = 0; i < n; ++i) EXPECT_EQ(grp.shard(i).index(), i);
}

TEST(ShardGroupRegression, InvokerModeRejected) {
    EXPECT_THROW(rs::ShardGroup(group(3816, 3817, 2, rs::ShardMode::Invoker)), std::invalid_argument);
}

// All-or-nothing: shard 1 cannot start (its driver directory is held by an
// active driver), so construction throws and shard 0 — already started — is
// torn down: its directory can be used again right away.
TEST(ShardGroupRegression, ConstructionFailureOfShardKTearsDownEarlierShards) {
    auto g = group(3818, 3819, 2, rs::ShardMode::Agent);
    {
        rs::ClientShard holder(g, 1);  // an active driver on shard-1's directory
        EXPECT_ANY_THROW(rs::ShardGroup{g});
    }
    // Shard 0 was destroyed with the failed group: a new shard 0 starts.
    EXPECT_NO_THROW(rs::ClientShard(g, 0));
    std::error_code ec;
    std::filesystem::remove_all(g.base.dataDir, ec);
}
