// AuthAllocRegressionTest.cpp - authentication Task 13 (hard rule 4): the authorization check adds ONE bitmask load to
// the produce hot path and allocates nothing - on the gateway path (PermissionTable::allows) and on the shard worker
// path (WriteGrantSlot, shipped over the lifecycle ring).  Replaces the global operator new (binary-wide), so it lives in
// its own binary (rune_auth_alloc_regression_tests).
#include "support/GatewayProduceRig.h"

#include <rune/gateway/PermissionTable.h>
#include <rune/gateway/SecurityView.h>
#include <rune/node/EmbeddedDriver.h>
#include <rune/node/Worker.h>
#include <rune/node/WorkerIngress.h>
#include <rune/node/WorkerOp.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/security/Permissions.h>
#include <rune/storage/Log.h>

#include <Aeron.h>
#include <FragmentAssembler.h>
#include <gtest/gtest.h>

#include <execinfo.h>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <new>
#include <unistd.h>

namespace {
bool     gArmed       = false;   // single-threaded driving: plain globals, no atomics
uint64_t gArmedAllocs = 0;
uint64_t gArmedBytes  = 0;
void noteAlloc(std::size_t n) {
    ++gArmedAllocs;
    gArmedBytes += n;
    if (gArmedAllocs <= 3) {   // name the culprit
        gArmed = false;
        void* frames[32];
        const int k = ::backtrace(frames, 32);
        std::fprintf(stderr, "auth alloc gate: allocation of %zu bytes:\n", n);
        ::backtrace_symbols_fd(frames, k, 2);
        gArmed = true;
    }
}
}  // namespace
void* operator new(std::size_t n) {
    if (gArmed) [[unlikely]] noteAlloc(n);
    if (void* p = std::malloc(n ? n : 1)) return p;
    throw std::bad_alloc();
}
void* operator new[](std::size_t n) {
    if (gArmed) [[unlikely]] noteAlloc(n);
    if (void* p = std::malloc(n ? n : 1)) return p;
    throw std::bad_alloc();
}
void operator delete(void* p) noexcept { std::free(p); }
void operator delete(void* p, std::size_t) noexcept { std::free(p); }
void operator delete[](void* p) noexcept { std::free(p); }
void operator delete[](void* p, std::size_t) noexcept { std::free(p); }

using namespace rune::node;
namespace fs = std::filesystem;
namespace pc = rune::protocol::client;

namespace {
constexpr int kProduces = 10'000;

struct Arm {
    Arm() { gArmedAllocs = 0; gArmedBytes = 0; gArmed = true; }
    ~Arm() { gArmed = false; }
};

// ── the gateway path ───────────────────────────────────────────────────────────────────────────────────────
struct GatewayAuth {
    rune::node::test::GatewayProduceRig rig{rune::node::test::ProduceTopicMode::StaticRouted};
    rune::gateway::SecurityView view{rune::security::SecurityState{}};
    GatewayAuth() {
        rig.gw.setAuthMode(rune::gateway::GatewayAuthMode::Required, &view);
        rune::gateway::ObjectIndex idx;
        for (int32_t i = 0; i < 4096; ++i) idx.onTopic(i, "t" + std::to_string(i));
        idx.onTopic(rig.kTopic, "granted");
        const std::vector<rune::security::GrantSpec> grants = {
            {rune::security::Operation::Write, {rune::security::ResourceType::Topic, rune::security::PatternKind::Exact, "granted"}}};
        rig.gw.setSessionAuthForTest(rig.sid, rune::gateway::AuthAuthenticated{1, "ws1", rune::gateway::PermissionTable::compile(grants, idx)});
    }
};

// ── the shard worker path: a Worker with worker-side acks over a real invoker driver (aeron:ipc) ───────────────
class NullTransport final : public rune::replication::PartitionTransport {
public:
    bool sendFetchRequest(const rune::replication::FetchRequestMsg&) override { return true; }
    bool sendFetchResponse(rune::replication::NodeId, const rune::replication::FetchResponseMsg&) override { return true; }
};
constexpr int32_t kIngress = 7501, kEgress = 7502;
struct WorkerAuth {
    fs::path dir, adir;
    std::unique_ptr<rune::storage::Log> log;
    NullTransport t;
    std::unique_ptr<rune::replication::PartitionReplica> replica;
    std::unique_ptr<EmbeddedDriver> drv;
    std::shared_ptr<aeron::Aeron> client;
    std::unique_ptr<WorkerIngress> ing;
    std::unique_ptr<Worker> worker;
    std::shared_ptr<aeron::Subscription> egress;
    int64_t now = 1'000;
    uint64_t acks = 0;
    aeron::FragmentAssembler asmb{[this](aeron::concurrent::AtomicBuffer&, aeron::util::index_t, aeron::util::index_t, aeron::Header&) { ++acks; }};

    WorkerAuth() {
        worker = std::make_unique<Worker>(0, 1 << 20, 1 << 20, 64, 64);
        const std::string tag = std::to_string(::getpid());
        dir  = fs::temp_directory_path() / ("rune_aar_log_" + tag);
        adir = fs::temp_directory_path() / ("rune_aar_aeron_" + tag);
        fs::remove_all(dir); fs::remove_all(adir);
        fs::create_directories(dir);
        log = std::make_unique<rune::storage::Log>(dir.string(), 256LL * 1024 * 1024);
        log->recover();
        rune::replication::PartitionConfig c; c.topicId = 1; c.selfId = 1; c.minIsr = 1;
        c.fetchIntervalNs = 1'000'000'000; c.isrCheckIntervalNs = 1'000'000'000'000LL;
        c.replicaLagMaxMs = 600'000; c.pushOnAppend = false;
        replica = std::make_unique<rune::replication::PartitionReplica>(
            c, rune::replication::Role::Leader, 1, std::vector<rune::replication::NodeId>{1},
            std::vector<rune::replication::NodeId>{1}, log.get(), &t);
        EmbeddedDriverConfig dc; dc.dir = adir.string(); dc.termBufferBytes = 1 << 20;
        drv = std::make_unique<EmbeddedDriver>(dc, DriverMode::Invoker);
        aeron::Context ctx; ctx.aeronDir(adir.string()); ctx.useConductorAgentInvoker(true);
        client = aeron::Aeron::connect(ctx);
        ing = std::make_unique<WorkerIngress>(client, "aeron:ipc", kIngress, drv->pumpWith(client.get()));
        worker->setOwnedDirect(Worker::OwnedSet{replica.get()});
        worker->setWorkerIngress(ing.get(), true);
        worker->setShardMode(true);
        worker->setShardClient(client.get());
        const int64_t sreg = client->addSubscription("aeron:ipc", kEgress);
        for (int i = 0; i < 200'000 && !egress; ++i) { pump(); egress = client->findSubscription(sreg); }

        WorkerOp a; a.type = WorkerOp::Type::SessionAnnounce; a.sessionId = 5;
        a.egressStreamId = kEgress; a.egressChannel = "aeron:ipc?term-length=1048576"; a.grantAll = false;
        (void)worker->tryEnqueueOp(std::move(a));
        std::vector<uint64_t> bits(1, uint64_t{1} << 1);            // WRITE on topic 1
        WorkerOp g; g.type = WorkerOp::Type::SessionPermissions; g.sessionId = 5; g.correlationId = 1;
        g.nextOffset = 0; g.hwBound = 1; g.isolation = kPermFlagLast; g.permWords = bits.data(); g.permWordCount = 1;
        (void)worker->tryEnqueueOp(std::move(g));
        for (int i = 0; i < 16; ++i) step();
    }
    ~WorkerAuth() {
        worker.reset(); egress.reset(); ing.reset(); client.reset(); drv.reset();
        std::error_code ec; fs::remove_all(dir, ec); fs::remove_all(adir, ec);
    }
    void pump() { drv->doWork(); client->conductorAgentInvoker().invoke(); }
    void step() { pump(); worker->doWork(now += 10'000); egress->poll(asmb.handler(), 64); }
    std::vector<uint8_t> frame(int64_t corr) const {
        rune::protocol::RecordBatchBuilder b(0, 1000); b.append("k", "v", 1000);
        const auto batch = b.build();
        pc::Produce m; m.sessionId = 5; m.correlationId = corr; m.topicId = 1; m.partition = 0; m.partitionKnown = 1;
        m.acks = 0; m.producerId = -1; m.producerEpoch = 0; m.baseSequence = -1;
        m.recordBatch = std::span<const uint8_t>(batch);
        std::vector<uint8_t> f(m.encodedSize()); m.encode(f.data());
        return f;
    }
};
}  // namespace

TEST(AuthAllocRegression, SteadyStateAuthorizedProduceNeverAllocatesRegression) {
    // The gateway path: Required mode, an authenticated session with a compiled table; 10 000 produces.
    {
        GatewayAuth g;
        std::vector<std::vector<uint8_t>> frames;
        frames.reserve(kProduces);
        for (int i = 0; i < kProduces; ++i) frames.push_back(g.rig.produceFrame(i + 1));
        for (int i = 0; i < 64; ++i) ASSERT_TRUE(g.rig.produceAndAwait(g.rig.produceFrame(100'000 + i)));   // warm-up
        const uint64_t okBefore = g.rig.produceOk;
        {
            Arm arm;
            for (int i = 0; i < kProduces; ++i) {
                g.rig.produce(frames[static_cast<std::size_t>(i)]);
                g.rig.pass();
            }
            for (int i = 0; i < 64; ++i) g.rig.pass();
        }
        EXPECT_EQ(gArmedAllocs, 0u) << "gateway path: " << gArmedBytes << " bytes allocated by 10 000 authorized produces";
        EXPECT_EQ(g.rig.produceOk - okBefore, static_cast<uint64_t>(kProduces)) << "every produce was answered OK";
    }
    // The shard worker path: the grants were shipped; 10 000 produces straight through the worker ingress handler.
    {
        WorkerAuth w;
        std::vector<std::vector<uint8_t>> frames;
        frames.reserve(kProduces + 64);
        for (int i = 0; i < kProduces + 64; ++i) frames.push_back(w.frame(i + 1));
        for (int i = 0; i < 64; ++i) {   // warm-up: buffers reach their peak size
            ASSERT_GE(w.worker->handleIngressFrameForTest(frames[static_cast<std::size_t>(i)].data(), frames[static_cast<std::size_t>(i)].size(), w.now), 0);
            w.step();
        }
        const uint64_t denied = w.worker->workerNotAuthorizedForTest();
        uint64_t allocs = 0;
        for (int i = 64; i < kProduces + 64; ++i) {
            const auto& f = frames[static_cast<std::size_t>(i)];
            {
                Arm arm;
                (void)w.worker->handleIngressFrameForTest(f.data(), f.size(), w.now);
                (void)w.worker->doWork(w.now += 10'000);
                allocs += gArmedAllocs;
            }
            if ((i & 63) == 0) w.egress->poll(w.asmb.handler(), 256);   // the client drains its acks (outside the window)
        }
        EXPECT_EQ(allocs, 0u) << "worker path: allocations across 10 000 authorized produces";
        EXPECT_EQ(w.worker->workerNotAuthorizedForTest(), denied) << "the granted topic was never refused";
        for (int i = 0; i < 200; ++i) w.step();
        EXPECT_GE(w.log->logEndOffset(), kProduces) << "every produce reached the log";
    }
}

TEST(AuthAllocRegression, IdleAuthenticatedSessionAllocatesNothing) {
    WorkerAuth w;
    for (int i = 0; i < 64; ++i) w.step();
    uint64_t allocs = 0;
    for (int i = 0; i < 10'000; ++i) {
        Arm arm;
        (void)w.worker->doWork(w.now += 10'000);
        allocs += gArmedAllocs;
    }
    EXPECT_EQ(allocs, 0u) << "an idle worker holding an authenticated session's grants allocates nothing";

    GatewayAuth g;
    for (int i = 0; i < 64; ++i) g.rig.pass();
    {
        Arm arm;
        for (int i = 0; i < 10'000; ++i) g.rig.pass();
    }
    EXPECT_EQ(gArmedAllocs, 0u) << "an idle gateway + worker with an authenticated session allocates nothing";
}
