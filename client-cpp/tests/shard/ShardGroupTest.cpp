// tests/shard/ShardGroupTest.cpp — T15b: ShardGroup (Agent mode).  Each shard's
// runner pumps its own driver; the fake brokers (one InvokerGateway per shard
// driver) run on ONE helper thread (GatewayThread) or are pumped by the test
// thread.  Per-shard ack ledgers are written by the runners (onBatchAck ctx per
// shard) and read only after the group is closed (runners joined).
//
// Rules (F8c): no std::atomic / mutex / future in tests, no sleeps.  Cross-thread
// hand-offs use pipes (the kernel orders them) and thread join; every wait has a
// deadline; a helper thread is always joined (RAII), even when an ASSERT fails.
#include "../support/AllocCounter.h"
#include "../support/InvokerGateway.h"

#include <rune/client/shard/ClientShard.h>

#include <gtest/gtest.h>

#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <functional>
#include <map>
#include <memory>
#include <set>
#include <span>
#include <stdexcept>
#include <string>
#include <sys/select.h>
#include <thread>
#include <unistd.h>
#include <vector>

namespace rc = rune::client;
namespace rs = rune::client::shard;
using namespace std::chrono_literals;

namespace {
int g_ctr = 0;  // test thread only
const std::vector<uint8_t> kVal(32, 0x21);

// Every group directory is removed at process exit (the drivers delete their
// aeron/ subdirectories; the parents would accumulate otherwise).
struct DirSweeper {
    std::vector<std::string> dirs;
    ~DirSweeper() {
        for (const auto& d : dirs) {
            std::error_code ec;
            std::filesystem::remove_all(d, ec);
        }
    }
};
DirSweeper g_sweeper;  // test thread only

rs::ShardGroupConfig group(int32_t in, int32_t eg, uint32_t count, int32_t partitions = 8) {
    rs::ShardGroupConfig g;
    g.base.gatewayChannel  = "aeron:ipc";
    g.base.gatewayStreamId = in;
    g.base.egressChannel   = "aeron:ipc";
    g.base.egressStreamId  = eg;
    g.base.dataDir = "/tmp/shardgrp_" + std::to_string(::getpid()) + "_" + std::to_string(g_ctr++);
    g_sweeper.dirs.push_back(g.base.dataDir);
    rc::TopicSpec ts;
    ts.name           = "t";
    ts.topicId        = 1;
    ts.partitionCount = partitions;
    g.base.topics.push_back(ts);
    g.shardCount        = count;
    g.mode              = rs::ShardMode::Agent;
    g.producer.acks     = rc::Acks::Leader;
    g.producer.lingerUs = 0;
    return g;
}

rc::Key keyFor(int32_t partition, int32_t partitions = 8) {
    for (uint64_t k = 0; k < 1'000'000; ++k) {
        if (rc::Key(k).partition(partitions) == partition) return rc::Key(k);
    }
    throw std::runtime_error("no key maps to the partition");
}

// A pipe: one-shot signals and command bytes between two test threads.
struct Pipe {
    int fd[2] = {-1, -1};
    Pipe() {
        if (::pipe(fd) != 0) throw std::runtime_error("pipe");
    }
    ~Pipe() {
        ::close(fd[0]);
        ::close(fd[1]);
    }
    Pipe(const Pipe&)            = delete;
    Pipe& operator=(const Pipe&) = delete;
    void put(char b = 1) const { (void)!::write(fd[1], &b, 1); }
    // Blocking read with a deadline; false on timeout.
    bool get(char& b, std::chrono::milliseconds timeout) const {
        fd_set s;
        FD_ZERO(&s);
        FD_SET(fd[0], &s);
        timeval tv{static_cast<long>(timeout.count() / 1000), static_cast<int>((timeout.count() % 1000) * 1000)};
        if (::select(fd[0] + 1, &s, nullptr, nullptr, &tv) != 1) return false;
        return ::read(fd[0], &b, 1) == 1;
    }
    bool tryGet(char& b) const { return get(b, 0ms); }
};

// The fake brokers, one per shard driver, all on ONE helper thread.  setup /
// onCommand / collect run on that thread; results are read after join().  The
// helper stops on stop(), on its own 60 s deadline, or on destruction (RAII:
// an ASSERT failure in the test can never leave it joinable).
struct GatewayThread {
    std::vector<std::string>                                                 dirs;
    std::string                                                              ctlChannel = "aeron:ipc";
    int32_t                                                                  ctlStream  = 0;
    std::string                                                              egChannel  = "aeron:ipc";
    int32_t                                                                  egStream   = 0;
    std::function<void(test_support::InvokerGateway&, std::size_t)>          setup;
    std::function<void(std::vector<std::unique_ptr<test_support::InvokerGateway>>&, char)> onCommand;
    std::function<void(std::vector<std::unique_ptr<test_support::InvokerGateway>>&)>       collect;
    bool        ready  = false;  // written by the helper before `up`, read after it
    bool        timedOut = false;
    Pipe        up, cmd;
    std::thread t;

    void start() {
        t = std::thread([this] {
            std::vector<std::unique_ptr<test_support::InvokerGateway>> gws;
            for (std::size_t i = 0; i < dirs.size(); ++i) {
                gws.push_back(std::make_unique<test_support::InvokerGateway>(dirs[i], ctlChannel, ctlStream,
                                                                             egChannel, egStream));
                if (setup) setup(*gws.back(), i);
            }
            const auto deadline = std::chrono::steady_clock::now() + 60s;
            auto allReady = [&] {
                for (auto& g : gws) {
                    if (!g->ready()) return false;
                }
                return true;
            };
            while (!allReady() && std::chrono::steady_clock::now() < deadline) {
                for (auto& g : gws) g->pump();
            }
            ready = allReady();
            up.put();
            for (;;) {
                if (std::chrono::steady_clock::now() >= deadline) {
                    timedOut = true;
                    break;
                }
                int work = 0;
                for (auto& g : gws) work += g->pump();
                char c = 0;
                if (cmd.tryGet(c)) {
                    if (c == 'q') break;
                    if (onCommand) onCommand(gws, c);
                }
                if (work == 0) std::this_thread::yield();
            }
            if (collect) collect(gws);
        });
        char c = 0;
        (void)up.get(c, 60s);
    }
    void command(char c) const { cmd.put(c); }
    void stop() {
        if (t.joinable()) {
            cmd.put('q');
            t.join();
        }
    }
    ~GatewayThread() { stop(); }
};

std::vector<std::string> shardDirs(const rs::ShardGroupConfig& g, uint32_t n) {
    std::vector<std::string> d;
    for (uint32_t i = 0; i < n; ++i) d.push_back(rs::shardClientConfig(g, i).dataDir + "/aeron");
    return d;
}

// Per-shard ack ledger: written by that shard's runner only (onBatchAck ctx),
// read after the group is closed (runner joined).
struct ShardLedger {
    uint32_t                   shard = 0;
    int64_t                    ok = 0, failed = 0, records = 0;
    std::set<std::thread::id>  threads;
    std::set<int32_t>          partitions;
    static void on(void* ctx, const rc::BatchAck& a) noexcept {
        auto* self = static_cast<ShardLedger*>(ctx);
        self->threads.insert(std::this_thread::get_id());
        self->partitions.insert(a.partition);
        self->records += a.recordCount;
        (a.errorCode == 0 ? self->ok : self->failed) += a.recordCount;
    }
};

// Race-test iterations: RUNE_SHARD_RACE_ITERS (default 50; the design's 1000
// for the TSan gate run).
int raceIters() {
    const char* v = std::getenv("RUNE_SHARD_RACE_ITERS");
    const int   n = (v != nullptr && *v != '\0') ? std::atoi(v) : 0;
    return n > 0 ? n : 50;
}

// Wait (bounded) until shard i of the group publishes `state`.
bool waitForState(rs::ShardGroup& grp, uint32_t shard, rs::ShardState state,
                  std::vector<rs::ShardStatus>* log = nullptr) {
    const auto deadline = std::chrono::steady_clock::now() + 10s;
    bool       seen     = false;
    while (!seen && std::chrono::steady_clock::now() < deadline) {
        grp.pollEvents([&](const rs::ShardStatus& s) {
            if (log) log->push_back(s);
            if (s.shard == shard && s.state == state) seen = true;
        });
        if (!seen) std::this_thread::yield();
    }
    return seen;
}
} // namespace

TEST(ShardGroupRegression, AgentRoutesEachSendToOwningShard) {
    auto g          = group(3910, 3911, 2);
    g.producer.acks = rc::Acks::None;
    rs::ShardGroup grp(g);
    ASSERT_EQ(grp.size(), 2u);
    test_support::InvokerGateway gw0(rs::shardClientConfig(g, 0).dataDir + "/aeron", "aeron:ipc", 3910,
                                     "aeron:ipc", 3911);
    test_support::InvokerGateway gw1(rs::shardClientConfig(g, 1).dataDir + "/aeron", "aeron:ipc", 3910,
                                     "aeron:ipc", 3911);
    // The test thread pumps only the gateways (yield between rounds, no sleep).
    auto pumpUntil = [&](auto&& pred) {
        const auto deadline = std::chrono::steady_clock::now() + 10s;
        while (std::chrono::steady_clock::now() < deadline) {
            gw0.pump();
            gw1.pump();
            if (pred()) return true;
            std::this_thread::yield();
        }
        return false;
    };
    ASSERT_TRUE(pumpUntil([&] { return gw0.ready() && gw1.ready(); }));
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
    for (int32_t p = 0; p < 8; ++p) grp.send(topic, keyFor(p), kVal);
    ASSERT_TRUE(pumpUntil([&] { return gw0.produced.size() + gw1.produced.size() == 8; }));
    for (const auto& pr : gw0.produced) EXPECT_EQ(rs::shardFor(1, pr.partition, 2), 0u);
    for (const auto& pr : gw1.produced) EXPECT_EQ(rs::shardFor(1, pr.partition, 2), 1u);
    EXPECT_EQ(grp.trySend(rc::TopicHandle{}, rc::Key(uint64_t{1}), kVal), rc::SendResult::InvalidTopic);
}

// connectAll() on the control thread; every shard's close drains its own
// partitions; acks reach the per-shard ctx of the owning runner only.
TEST(ShardGroupRegression, ConnectAllThenCloseDrainsEveryShardsOwnPartitions) {
    auto           g = group(3912, 3913, 2);
    rs::ShardGroup grp(g);
    ShardLedger    l0, l1;
    l0.shard = 0;
    l1.shard = 1;
    grp.onBatchAck(0, &ShardLedger::on, &l0);
    grp.onBatchAck(1, &ShardLedger::on, &l1);
    std::vector<int32_t> seen0, seen1;
    GatewayThread        gt;
    gt.dirs      = shardDirs(g, 2);
    gt.ctlStream = 3912;
    gt.egStream  = 3913;
    gt.collect   = [&](auto& gws) {
        for (const auto& pr : gws[0]->produced) seen0.push_back(pr.partition);
        for (const auto& pr : gws[1]->produced) seen1.push_back(pr.partition);
    };
    gt.start();
    ASSERT_TRUE(gt.ready);
    ASSERT_NO_THROW(grp.connectAll(10s));
    std::vector<rs::ShardStatus> log;
    ASSERT_TRUE(waitForState(grp, 0, rs::ShardState::Live, &log));
    const auto topic = grp.shard(0).topic("t");
    for (int32_t p = 0; p < 8; ++p) ASSERT_EQ(grp.trySend(topic, keyFor(p), kVal), rc::SendResult::Ok);
    EXPECT_EQ(grp.close(10s), rc::OpStatus::Ok);
    gt.stop();
    EXPECT_EQ(seen0.size() + seen1.size(), 8u);
    for (int32_t p : seen0) EXPECT_EQ(rs::shardFor(1, p, 2), 0u);
    for (int32_t p : seen1) EXPECT_EQ(rs::shardFor(1, p, 2), 1u);
    EXPECT_EQ(l0.ok + l1.ok, 8);
    EXPECT_EQ(grp.trySend(topic, keyFor(0), kVal), rc::SendResult::Closed);
    grp.pollEvents([&](const rs::ShardStatus& s) { log.push_back(s); });
    int closed = 0;
    for (const auto& s : log) closed += s.state == rs::ShardState::Closed;
    EXPECT_EQ(closed, 2) << "each shard publishes Closed once";
}

// Review finding 7: one deadline for the whole connectAll loop (not N x timeout);
// the error names the shard.  Shards 0 and 1 connect after 600 ms (a slow
// broker), shard 2 never: one 1 s deadline ends at ~1 s; a fresh timeout per
// shard would wait 600 ms + 1 s.
TEST(ShardGroupRegression, ConnectAllUsesOneDeadlineAndNamesTheShard) {
    auto           g = group(3914, 3915, 3);
    rs::ShardGroup grp(g);
    GatewayThread  gt;
    gt.dirs      = shardDirs(g, 3);
    gt.ctlStream = 3914;
    gt.egStream  = 3915;
    gt.setup     = [](test_support::InvokerGateway& gw, std::size_t i) {
        if (i == 2) {
            gw.replyConnects = false;
        } else {
            gw.connectReplyDelay = 600ms;
        }
    };
    gt.start();
    ASSERT_TRUE(gt.ready);
    const auto t0 = std::chrono::steady_clock::now();
    try {
        grp.connectAll(1s);
        ADD_FAILURE() << "connectAll must throw";
    } catch (const std::runtime_error& e) {
        EXPECT_NE(std::string(e.what()).find("shard 2"), std::string::npos) << e.what();
    }
    const auto elapsed = std::chrono::steady_clock::now() - t0;
    EXPECT_GE(elapsed, 950ms);
    EXPECT_LT(elapsed, 1350ms) << "one deadline (~1 s), not a fresh timeout per shard (~1.6 s)";
}

TEST(ShardGroupRegression, AutoShardCountIsResolvedOnceAndRoutesWithIt) {
    auto g       = group(3916, 3917, rs::kAutoShards, 2);
    const auto n = rs::resolveShardCount(g, rs::platformAllowedCpus(), rs::platformQuotaCpus()).count;
    rs::ShardGroup grp(g);
    ASSERT_EQ(grp.size(), n);
    EXPECT_EQ(grp.config().shardCount, n);
    for (int32_t p = 0; p < 2; ++p) EXPECT_EQ(&grp.owner(1, p), &grp.shard(rs::shardFor(1, p, n)));
    for (uint32_t i = 0; i < n; ++i) EXPECT_EQ(grp.shard(i).index(), i);
}

TEST(ShardGroupRegression, InvokerModeRejected) {
    auto g = group(3918, 3919, 2);
    g.mode = rs::ShardMode::Invoker;
    EXPECT_THROW(rs::ShardGroup{g}, std::invalid_argument);
}

namespace {
struct TeardownOrder {
    std::vector<uint32_t> order;
    static void on(void* ctx, uint32_t shard) noexcept { static_cast<TeardownOrder*>(ctx)->order.push_back(shard); }
};
} // namespace

// Review finding 8: all-or-nothing construction names the failing shard and
// tears the started ones down in reverse (N >= 3); destruction is reverse too.
TEST(ShardGroupRegression, ConstructionFailureOfShardKTearsDownZeroToKMinus1) {
    auto          g = group(3920, 3921, 3);
    TeardownOrder failed;
    g.testTeardownHook = &TeardownOrder::on;
    g.testTeardownCtx  = &failed;
    {
        rs::ClientShard holder(g, 2);  // an active driver on shard-2's directory
        try {
            rs::ShardGroup grp(g);
            ADD_FAILURE() << "construction must fail at shard 2";
        } catch (const std::exception& e) {
            EXPECT_NE(std::string(e.what()).find("shard 2"), std::string::npos) << e.what();
        }
    }
    EXPECT_EQ(failed.order, (std::vector<uint32_t>{1, 0}));
    // Shards 0 and 1 were destroyed with the failed group: their dirs are free.
    EXPECT_NO_THROW(rs::ClientShard(g, 0));
    TeardownOrder normal;
    g.testTeardownCtx = &normal;
    { rs::ShardGroup grp(g); }
    EXPECT_EQ(normal.order, (std::vector<uint32_t>{2, 1, 0}));
    std::error_code ec;
    std::filesystem::remove_all(g.base.dataDir, ec);
}

// Review finding 2 (Agent): the Aeron client of a shard closes (fault seam):
// its runner tail-closes the intake (sends -> Closed), every record accepted
// before it completes Closed exactly once, and Failed reaches the control
// thread.  The sibling shard keeps producing.
TEST(ShardGroupRegression, OneShardFailedSiblingsKeepProducing) {
    auto           g = group(3922, 3923, 2);
    rs::ShardGroup grp(g);
    ShardLedger    l0, l1;
    grp.onBatchAck(0, &ShardLedger::on, &l0);
    grp.onBatchAck(1, &ShardLedger::on, &l1);
    GatewayThread gt;
    gt.dirs      = shardDirs(g, 2);
    gt.ctlStream = 3922;
    gt.egStream  = 3923;
    gt.setup     = [](test_support::InvokerGateway& gw, std::size_t i) {
        if (i == 0) gw.holdPartitions = {0, 1, 2, 3, 4, 5, 6, 7};  // shard 0's acks never come
    };
    gt.start();
    ASSERT_TRUE(gt.ready);
    grp.connectAll(10s);
    const auto topic = grp.shard(0).topic("t");
    int        accepted0 = 0;
    for (int32_t p = 0; p < 8; ++p) {
        if (grp.shard(0).owns(1, p)) {
            for (int i = 0; i < 5; ++i) accepted0 += grp.trySend(topic, keyFor(p), kVal) == rc::SendResult::Ok;
        }
    }
    grp.shard(0).client().testInjectTransportFault();
    std::vector<rs::ShardStatus> log;
    ASSERT_TRUE(waitForState(grp, 0, rs::ShardState::Failed, &log));
    EXPECT_EQ(grp.shard(0).status().failure, rs::ShardFailure::TransportClosed);
    for (int32_t p = 0; p < 8; ++p) {
        const rc::SendResult r = grp.trySend(topic, keyFor(p), kVal);
        EXPECT_EQ(r, grp.shard(0).owns(1, p) ? rc::SendResult::Closed : rc::SendResult::Ok) << p;
    }
    EXPECT_TRUE(grp.shard(0).closed());
    EXPECT_FALSE(grp.shard(1).closed());
    EXPECT_EQ(grp.shard(1).producer().closeAndWait(10s), rc::OpStatus::Ok) << "the sibling drains normally";
    grp.closeImmediately();
    gt.stop();
    EXPECT_EQ(l0.records, accepted0) << "every record shard 0 accepted completed exactly once";
    EXPECT_EQ(l0.failed, accepted0);
    EXPECT_EQ(l1.ok, 4) << "4 partitions x 1 send after the sibling failed";
}

// Records claimed by app threads racing the runner's failure tail-close: each
// one is either refused (Closed) or completes exactly once.  TSan target.
TEST(ShardGroupRegression, RecordsClaimedBeforeFailureTailCloseCompletedClosedOnce) {
    for (int iter = 0; iter < raceIters(); ++iter) {
        SCOPED_TRACE(iter);
        auto           g = group(3924, 3925, 1);
        rs::ShardGroup grp(g);
        ShardLedger    l;
        grp.onBatchAck(0, &ShardLedger::on, &l);
        GatewayThread gt;
        gt.dirs      = shardDirs(g, 1);
        gt.ctlStream = 3924;
        gt.egStream  = 3925;
        gt.start();
        ASSERT_TRUE(gt.ready);
        grp.connectAll(10s);
        const auto topic    = grp.shard(0).topic("t");
        int64_t    accepted = 0;
        Pipe       started;
        std::thread sender([&] {
            started.put();
            for (uint64_t k = 0;; ++k) {
                const rc::SendResult r = grp.trySend(topic, rc::Key(k), kVal);
                if (r == rc::SendResult::Ok) ++accepted;
                if (r == rc::SendResult::Closed) break;
                if (k > 5'000'000) break;  // bounded: never spins forever
            }
        });
        char c = 0;
        ASSERT_TRUE(started.get(c, 10s));
        grp.shard(0).client().testInjectTransportFault();
        sender.join();
        ASSERT_TRUE(waitForState(grp, 0, rs::ShardState::Failed));
        grp.closeImmediately();
        gt.stop();
        EXPECT_EQ(l.records, accepted) << "accepted before the tail close -> exactly one completion";
    }
}

// closeImmediately racing in-flight acks: every accepted record completes
// exactly once (acked or Closed), never twice.  TSan target.
TEST(ShardGroupRegression, CloseImmediatelyRacingInflightAcksNoDoubleCompletion) {
    for (int iter = 0; iter < raceIters(); ++iter) {
        SCOPED_TRACE(iter);
        auto           g = group(3926, 3927, 2);
        rs::ShardGroup grp(g);
        ShardLedger    l0, l1;
        grp.onBatchAck(0, &ShardLedger::on, &l0);
        grp.onBatchAck(1, &ShardLedger::on, &l1);
        GatewayThread gt;
        gt.dirs      = shardDirs(g, 2);
        gt.ctlStream = 3926;
        gt.egStream  = 3927;
        gt.start();
        ASSERT_TRUE(gt.ready);
        grp.connectAll(10s);
        const auto topic    = grp.shard(0).topic("t");
        int64_t    accepted = 0;
        for (uint64_t k = 0; k < 2000; ++k) accepted += grp.trySend(topic, rc::Key(k), kVal) == rc::SendResult::Ok;
        grp.closeImmediately();
        gt.stop();
        EXPECT_EQ(l0.records + l1.records, accepted);
    }
}

// begin-close of EVERY intake happens before any drain wait: a send racing
// close() is either Ok (then acked exactly once) or Closed synchronously.
TEST(ShardGroupRegression, CloseClosesAllIntakesBeforeDraining) {
    auto           g = group(3928, 3929, 2);
    rs::ShardGroup grp(g);
    ShardLedger    l0, l1;
    grp.onBatchAck(0, &ShardLedger::on, &l0);
    grp.onBatchAck(1, &ShardLedger::on, &l1);
    GatewayThread gt;
    gt.dirs      = shardDirs(g, 2);
    gt.ctlStream = 3928;
    gt.egStream  = 3929;
    gt.start();
    ASSERT_TRUE(gt.ready);
    grp.connectAll(10s);
    const auto topic    = grp.shard(0).topic("t");
    int64_t    accepted = 0, closedSends = 0;
    Pipe       started;
    std::thread sender([&] {
        started.put();
        int closedSeen = 0;
        for (uint64_t k = 0; k < 5'000'000 && closedSeen < 64; ++k) {
            const rc::SendResult r = grp.trySend(topic, rc::Key(k), kVal);
            if (r == rc::SendResult::Ok) ++accepted;
            if (r == rc::SendResult::Closed) ++closedSeen;
        }
        closedSends = closedSeen;
    });
    char c = 0;
    ASSERT_TRUE(started.get(c, 10s));
    const rc::OpStatus st = grp.close(20s);
    sender.join();
    gt.stop();
    EXPECT_EQ(st, rc::OpStatus::Ok);
    EXPECT_GT(closedSends, 0);
    EXPECT_EQ(l0.ok + l1.ok, accepted) << "every accepted record acked exactly once, none lost";
    EXPECT_EQ(l0.failed + l1.failed, 0);
}

TEST(ShardGroupRegression, PerShardAckCtxNeverSharedAcrossRunners) {
    auto           g = group(3930, 3931, 2);
    rs::ShardGroup grp(g);
    ShardLedger    l0, l1;
    l0.shard = 0;
    l1.shard = 1;
    grp.onBatchAck(0, &ShardLedger::on, &l0);
    grp.onBatchAck(1, &ShardLedger::on, &l1);
    EXPECT_THROW(grp.onBatchAck(2, &ShardLedger::on, &l0), std::out_of_range);
    GatewayThread gt;
    gt.dirs      = shardDirs(g, 2);
    gt.ctlStream = 3930;
    gt.egStream  = 3931;
    gt.start();
    ASSERT_TRUE(gt.ready);
    grp.connectAll(10s);
    const auto topic = grp.shard(0).topic("t");
    for (uint64_t k = 0; k < 400; ++k) grp.send(topic, rc::Key(k), kVal);
    EXPECT_EQ(grp.close(10s), rc::OpStatus::Ok);
    gt.stop();
    EXPECT_EQ(l0.ok + l1.ok, 400);
    ASSERT_EQ(l0.threads.size(), 1u) << "one runner per shard ctx";
    ASSERT_EQ(l1.threads.size(), 1u);
    EXPECT_NE(*l0.threads.begin(), *l1.threads.begin()) << "the runners never share a ctx";
    for (int32_t p : l0.partitions) EXPECT_EQ(rs::shardFor(1, p, 2), 0u);
    for (int32_t p : l1.partitions) EXPECT_EQ(rs::shardFor(1, p, 2), 1u);
}

// Several app threads on ONE partition: the owning shard's M2O ring keeps each
// thread's send order (per (thread, partition)).
TEST(ShardGroupRegression, ManyAppThreadsOnePartitionKeepPerThreadOrder) {
    auto           g = group(3932, 3933, 2);
    rs::ShardGroup grp(g);
    std::vector<std::vector<uint8_t>> got;
    GatewayThread                     gt;
    gt.dirs      = shardDirs(g, 2);
    gt.ctlStream = 3932;
    gt.egStream  = 3933;
    gt.setup     = [](test_support::InvokerGateway& gw, std::size_t) { gw.keepValues = true; };
    gt.collect   = [&](auto& gws) {
        for (auto& gw : gws) {
            for (auto& v : gw->values[5]) got.push_back(v);
        }
    };
    gt.start();
    ASSERT_TRUE(gt.ready);
    grp.connectAll(10s);
    const auto    topic = grp.shard(0).topic("t");
    const rc::Key k     = keyFor(5);
    constexpr int kThreads = 3, kPer = 300;
    std::vector<std::thread> ts;
    for (int t = 0; t < kThreads; ++t) {
        ts.emplace_back([&, t] {
            for (int i = 0; i < kPer; ++i) {
                uint8_t v[8];
                v[0] = static_cast<uint8_t>(t);
                std::memcpy(v + 4, &i, 4);
                grp.send(topic, k, std::span<const uint8_t>(v, 8));
            }
        });
    }
    for (auto& t : ts) t.join();
    EXPECT_EQ(grp.close(10s), rc::OpStatus::Ok);
    gt.stop();
    ASSERT_EQ(got.size(), static_cast<std::size_t>(kThreads * kPer));
    std::map<int, int> next;
    for (const auto& v : got) {
        ASSERT_EQ(v.size(), 8u);
        int i = 0;
        std::memcpy(&i, v.data() + 4, 4);
        EXPECT_EQ(i, next[v[0]]) << "thread " << int(v[0]);
        next[v[0]] = i + 1;
    }
}

// Design §7 (M3): a counting partitioner (the group's test seam for
// Key::partition) sees exactly ONE call per send, and the record lands on the
// partition IT returned — the shard and the producer route by the partition
// they are given, they never re-hash the key.
namespace {
int g_hashCalls = 0;  // the test thread is the only sender here
int32_t countingPartitioner(const rc::Key&, int32_t) noexcept {
    ++g_hashCalls;
    return 5;
}
} // namespace

TEST(ShardGroupRegression, PartitionComputedOnce) {
    auto g            = group(3938, 3939, 2);
    g.testPartitioner = &countingPartitioner;
    rs::ShardGroup grp(g);
    ShardLedger    l0, l1;
    grp.onBatchAck(0, &ShardLedger::on, &l0);
    grp.onBatchAck(1, &ShardLedger::on, &l1);
    std::vector<int32_t> partitions;
    GatewayThread        gt;
    gt.dirs      = shardDirs(g, 2);
    gt.ctlStream = 3938;
    gt.egStream  = 3939;
    gt.collect   = [&](auto& gws) {
        for (auto& gw : gws) {
            for (const auto& pr : gw->produced) partitions.push_back(pr.partition);
        }
    };
    gt.start();
    ASSERT_TRUE(gt.ready);
    grp.connectAll(10s);
    const auto topic = grp.shard(0).topic("t");
    g_hashCalls      = 0;
    for (uint64_t k = 0; k < 50; ++k) ASSERT_EQ(grp.trySend(topic, rc::Key(k), kVal), rc::SendResult::Ok);
    grp.send(topic, rc::Key(uint64_t{99}), kVal);
    EXPECT_EQ(g_hashCalls, 51) << "one hash per send";
    EXPECT_EQ(grp.close(10s), rc::OpStatus::Ok);
    gt.stop();
    EXPECT_EQ(l0.ok + l1.ok, 51);
    ASSERT_FALSE(partitions.empty());
    for (int32_t p : partitions) EXPECT_EQ(p, 5) << "routed by the given partition, never re-hashed";
    EXPECT_EQ(l0.ok, rs::shardFor(1, 5, 2) == 0 ? 51 : 0);
}

// F8c token form: connectAll(ShardCompletions&, base) completes shard i's
// connect exactly once into q.at(i) with token base + i.
TEST(ShardGroupRegression, ConnectAllTokenFormCompletesEachShardOnce) {
    auto           g = group(3940, 3941, 2);
    rs::ShardGroup grp(g);
    GatewayThread  gt;
    gt.dirs      = shardDirs(g, 2);
    gt.ctlStream = 3940;
    gt.egStream  = 3941;
    gt.start();
    ASSERT_TRUE(gt.ready);
    rs::ShardCompletions q(grp);
    ASSERT_EQ(q.size(), 2u);
    ASSERT_EQ(grp.connectAll(q, 100), rc::OpResult::Accepted);
    std::map<uint32_t, std::vector<rc::Token>> got;
    std::map<uint32_t, rc::OpStatus>            st;
    const auto deadline = std::chrono::steady_clock::now() + 10s;
    while (got.size() < 2 && std::chrono::steady_clock::now() < deadline) {
        q.poll([&](uint32_t shard, const rc::Completion& c, std::span<const uint8_t>) {
            got[shard].push_back(c.token);
            st[shard] = c.status;
        });
        std::this_thread::yield();
    }
    ASSERT_EQ(got.size(), 2u);
    EXPECT_EQ(got[0], (std::vector<rc::Token>{100}));
    EXPECT_EQ(got[1], (std::vector<rc::Token>{101}));
    EXPECT_EQ(st[0], rc::OpStatus::Ok);
    EXPECT_EQ(st[1], rc::OpStatus::Ok);
    rs::ShardCompletions other(grp);
    EXPECT_EQ(grp.connectAll(other, rc::kMaxToken), rc::OpResult::InvalidArgument) << "base + i must fit";
    grp.closeImmediately();
}

// R3 (design §4, Draining row): a record claimed before the begin-close and
// committed after it is read, sent and acked BEFORE the begin-close token
// completes Ok.  (The 1000-iteration race proof of the mechanism is
// ProducerClose.BeginCloseWaitsForClaimedUncommittedRecordRegression; this
// proves the shard wiring.)
TEST(ShardGroupRegression, BeginCloseWaitsForClaimedUncommittedRecord) {
    for (int iter = 0; iter < raceIters(); ++iter) {
        SCOPED_TRACE(iter);
        auto           g = group(3942, 3943, 1);
        rs::ShardGroup grp(g);
        ShardLedger    l;
        grp.onBatchAck(0, &ShardLedger::on, &l);
        GatewayThread gt;
        gt.dirs      = shardDirs(g, 1);
        gt.ctlStream = 3942;
        gt.egStream  = 3943;
        gt.start();
        ASSERT_TRUE(gt.ready);
        grp.connectAll(10s);
        const auto topic = grp.shard(0).topic("t");
        Pipe       claimed, intakeClosed;
        long       idx = -1;
        std::thread app([&] {
            idx = grp.shard(0).producer().testClaimRecord(topic, 2, keyFor(2), kVal);
            claimed.put();
            char c = 0;
            (void)intakeClosed.get(c, 10s);
            grp.shard(0).producer().testCommitClaim(idx);
        });
        char c = 0;
        ASSERT_TRUE(claimed.get(c, 10s));
        rc::Completions cq(grp.shard(0).client());
        ASSERT_EQ(grp.shard(0).close(cq, 7), rc::OpResult::Accepted);  // intake closed synchronously
        EXPECT_EQ(grp.trySend(topic, keyFor(2), kVal), rc::SendResult::Closed);
        intakeClosed.put();
        rc::OpStatus st      = rc::OpStatus::Closed;
        int64_t      okAtTok = -1;
        const auto   w = cq.waitFor(7, 10s, [&](const rc::Completion& done, std::span<const uint8_t>) {
            st      = done.status;
            okAtTok = l.ok;  // written by the runner before it completed the token
        });
        app.join();
        ASSERT_GE(idx, 0);
        ASSERT_EQ(w, rc::Completions::Wait::Done);
        EXPECT_EQ(st, rc::OpStatus::Ok);
        EXPECT_EQ(okAtTok, 1) << "the claimed record's ack precedes the token";
        grp.closeImmediately();
        gt.stop();
        EXPECT_EQ(l.records, 1);
    }
}

// M2 (Agent): once failed, the runner parks (1 ms per cycle) and does no I/O —
// it still serves its command ring (registrations, control ops complete Closed
// at once) so the control thread never hangs.
TEST(ShardGroupRegression, FailedRunnerParksAndDoesNoIo) {
    auto           g = group(3944, 3945, 1);
    rs::ShardGroup grp(g);
    int            framesAtMark = -1, framesAtEnd = -1, pings = -1;
    GatewayThread  gt;
    gt.dirs      = shardDirs(g, 1);
    gt.ctlStream = 3944;
    gt.egStream  = 3945;
    gt.onCommand = [&](auto& gws, char c) {
        if (c == 'm') framesAtMark = gws[0]->framesReceived;
    };
    gt.collect = [&](auto& gws) {
        framesAtEnd = gws[0]->framesReceived;
        pings       = gws[0]->pings;
    };
    gt.start();
    ASSERT_TRUE(gt.ready);
    grp.connectAll(10s);
    const auto t0 = std::chrono::steady_clock::now();
    grp.shard(0).client().testInjectTransportFault();
    ASSERT_TRUE(waitForState(grp, 0, rs::ShardState::Failed));
    gt.command('m');
    // 30 control round trips through the failed runner (each Completions
    // registration waits for one duty cycle), and a ping that must complete
    // Closed at once without reaching the broker.
    for (int i = 0; i < 30; ++i) {
        rc::Completions probe(grp.shard(0).client());
        ASSERT_TRUE(probe.valid());
    }
    rc::Completions cq(grp.shard(0).client());
    const uint8_t   echo[] = {1};
    ASSERT_EQ(grp.shard(0).client().ping(echo, cq, 3), rc::OpResult::Accepted);
    rc::OpStatus pst = rc::OpStatus::Ok;
    ASSERT_EQ(cq.waitFor(3, 10s, [&](const rc::Completion& c, std::span<const uint8_t>) { pst = c.status; }),
              rc::Completions::Wait::Done);
    EXPECT_EQ(pst, rc::OpStatus::Closed);
    grp.closeImmediately();
    const auto elapsedMs =
        std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - t0).count();
    gt.stop();
    const uint64_t cycles = grp.shard(0).client().testFailedDutyCycles();  // runner joined
    EXPECT_GT(cycles, 0u);
    EXPECT_LE(cycles, static_cast<uint64_t>(elapsedMs) * 2 + 20)
        << "a parked runner: ~1 cycle per ms, not a spin (" << cycles << " cycles in " << elapsedMs << " ms)";
    EXPECT_EQ(framesAtEnd, framesAtMark) << "no I/O after the failure";
    EXPECT_EQ(pings, 0);
}

TEST(ShardGroupRegression, TrySendZeroAllocation) {
    auto           g = group(3934, 3935, 2);
    g.producer.acks  = rc::Acks::None;
    rs::ShardGroup grp(g);
    GatewayThread  gt;
    gt.dirs      = shardDirs(g, 2);
    gt.ctlStream = 3934;
    gt.egStream  = 3935;
    gt.start();
    ASSERT_TRUE(gt.ready);
    grp.connectAll(10s);
    const auto topic = grp.shard(0).topic("t");
    for (uint64_t k = 0; k < 256; ++k) grp.send(topic, rc::Key(k), kVal);  // warm up
    // The app thread's sends only (the runners and the fake broker run on).
    alloc_counter::startThisThread();
    int ok = 0;
    for (uint64_t k = 0; k < 64; ++k) ok += grp.trySend(topic, rc::Key(k), kVal) == rc::SendResult::Ok;
    const long allocs = alloc_counter::stopThisThread();
    EXPECT_EQ(ok, 64);
    EXPECT_EQ(allocs, 0) << "ShardGroup::trySend must not allocate";
    grp.closeImmediately();
}

// Gate 3 (design I7): N = 2 shards with UDP egress.  Each shard announces its own
// egress port (base + i); the broker replies to the announced channel; every
// record is acked exactly once on the owning shard and NO ack goes to another
// shard's port.  The broker is one gateway hosted on shard 0's driver (it
// reaches shard 1's driver over UDP like a remote broker).
TEST(ShardGroupRegression, UdpEgressAcksArriveOnEachShardsOwnPort) {
    auto g                 = group(3936, 3937, 2);
    g.base.gatewayChannel  = "aeron:udp?endpoint=127.0.0.1:24870";
    g.base.egressChannel   = "aeron:udp?endpoint=127.0.0.1:24880";
    rs::ShardGroup grp(g);
    ShardLedger    l0, l1;
    grp.onBatchAck(0, &ShardLedger::on, &l0);
    grp.onBatchAck(1, &ShardLedger::on, &l1);
    std::map<std::string, int> acksPerEgress;
    std::set<int64_t>          sessions;
    GatewayThread              gt;
    gt.dirs       = {shardDirs(g, 1)[0]};
    gt.ctlChannel = g.base.gatewayChannel;
    gt.ctlStream  = 3936;
    gt.egChannel  = g.base.egressChannel;
    gt.egStream   = 3937;
    gt.setup      = [](test_support::InvokerGateway& gw, std::size_t) {
        gw.replyOnAnnouncedEgress = true;
        gw.uniqueSessionIds       = true;
    };
    gt.collect = [&](auto& gws) {
        acksPerEgress = gws[0]->acksPerEgress;
        for (const auto& pr : gws[0]->produced) sessions.insert(pr.sessionId);
    };
    gt.start();
    ASSERT_TRUE(gt.ready);
    grp.connectAll(10s);
    const auto topic = grp.shard(0).topic("t");
    int        per[2] = {0, 0};
    for (uint64_t k = 0; k < 200; ++k) {
        const rc::Key key(k);
        ++per[rs::shardFor(1, key.partition(8), 2)];
        grp.send(topic, key, kVal);
    }
    EXPECT_EQ(grp.close(10s), rc::OpStatus::Ok);
    gt.stop();
    EXPECT_EQ(l0.ok, per[0]);
    EXPECT_EQ(l1.ok, per[1]);
    EXPECT_EQ(l0.failed + l1.failed, 0);
    EXPECT_EQ(sessions.size(), 2u) << "one session per shard";
    ASSERT_EQ(acksPerEgress.size(), 2u) << "acks went to exactly the two announced egress channels";
    EXPECT_GT(acksPerEgress["aeron:udp?endpoint=127.0.0.1:24880"], 0);
    EXPECT_GT(acksPerEgress["aeron:udp?endpoint=127.0.0.1:24881"], 0);
}
