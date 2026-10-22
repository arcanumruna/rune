// FilterAllocRegressionTest.cpp - Phase 2c Task 16 (hard rule 4): the filtered push path allocates nothing in steady
// state.  AllocWindow.cpp replaces the global operator new (binary-wide), so this lives in its own binary
// (rune_filter_alloc_regression_tests).  A real Worker on a real disk Log pushes to an embedded-driver IPC egress; the
// window wraps only the worker's own doWork (the test thread IS the worker thread), never the rig's appends or polls.
#include "support/AllocWindow.h"
#include "support/FilterFakes.h"

#include <rune/codec/Filter.h>
#include <rune/codec/FilterPredicate.h>
#include <rune/codec/Serialize.h>
#include <rune/node/EmbeddedDriver.h>
#include <rune/node/Worker.h>
#include <rune/node/WorkerOp.h>
#include <rune/protocol/client/DataMessages.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/storage/Log.h>

#include <Aeron.h>
#include <FragmentAssembler.h>
#include <concurrent/AtomicBuffer.h>
#include <gtest/gtest.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <string>
#include <thread>
#include <vector>

using namespace rune::node;
using namespace rune::node::filter::fakes;
namespace pc = rune::protocol::client;

namespace {

constexpr int32_t  kTopic = 10;
constexpr uint16_t kVer = 2;
constexpr int      kFiltered = 50;
constexpr int64_t  kControlSub = 1000;

int64_t nowNs() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch()).count();
}

class NullTransport final : public rune::replication::PartitionTransport {
public:
    bool sendFetchRequest(const rune::replication::FetchRequestMsg&) override { return true; }
    bool sendFetchResponse(rune::replication::NodeId, const rune::replication::FetchResponseMsg&) override { return true; }
};

struct Fb {
    rune::codec::FilterBytes fb;
    std::size_t n = 0;
    [[nodiscard]] std::span<const uint8_t> view() const { return {fb.bytes.data(), n}; }
};
Fb worldEq(uint32_t w) {
    Fb f;
    rune::codec::FilterWriter fw(std::span<uint8_t>(f.fb.bytes.data(), f.fb.bytes.size()));
    fw.term(1, rune::codec::PrimType::UInt32, 4, rune::Match<uint32_t>(w));
    auto n = fw.finish();
    f.n = n ? *n : 0;
    return f;
}

std::vector<PlayerRow> rowsOf(const std::vector<uint32_t>& worlds) {
    std::vector<PlayerRow> v;
    uint64_t id = 1;
    for (const uint32_t w : worlds) {
        PlayerRow r;
        r.playerId = id++; r.worldId = w; r.channel = 1; r.guild = 3;
        v.push_back(r);
    }
    return v;
}

}  // namespace

TEST(FilterAllocRegression, SteadyStateFilteredPushAllocatesNothingRegression) {
    namespace fs = std::filesystem;
    const fs::path root = fs::temp_directory_path() / ("rune_filteralloc_" + std::to_string(::getpid()));
    fs::remove_all(root);
    fs::create_directories(root / "log");

    EmbeddedDriverConfig dc;
    dc.dir = (root / "aeron").string();
    dc.termBufferBytes = 16 * 1024 * 1024;
    auto driver = std::make_unique<EmbeddedDriver>(dc, DriverMode::Threaded);
    aeron::Context ctx;
    ctx.aeronDir(dc.dir);
    auto aeron = aeron::Aeron::connect(ctx);
    const int64_t pubReg = aeron->addExclusivePublication("aeron:ipc", 9302);
    const int64_t subReg = aeron->addSubscription("aeron:ipc", 9302);
    std::shared_ptr<aeron::ExclusivePublication> pub;
    std::shared_ptr<aeron::Subscription> sub;
    for (int i = 0; i < 1'000'000 && (!pub || !sub || !pub->isConnected()); ++i) {
        if (!pub) pub = aeron->findExclusivePublication(pubReg);
        if (!sub) sub = aeron->findSubscription(subReg);
        std::this_thread::yield();
    }
    ASSERT_TRUE(pub && sub && pub->isConnected()) << "IPC not connected";

    rune::storage::Log log((root / "log").string(), 64LL * 1024 * 1024);
    log.recover();
    rune::replication::PartitionConfig c;
    c.topicId = kTopic; c.partitionId = 0; c.selfId = 1; c.minIsr = 1; c.pushOnAppend = false;
    c.fetchIntervalNs = 1'000'000'000LL; c.isrCheckIntervalNs = 1'000'000'000LL;
    NullTransport t;
    rune::replication::PartitionReplica rep(c, rune::replication::Role::Leader, 1, {1}, {1}, &log, &t);
    rep.onLeaderLogAppended();
    Worker w(0, 1 << 22, 1 << 23, 64, 64);
    w.setOwnedDirect(Worker::OwnedSet{&rep});
    filter::FilterConfig fc;
    fc.noteIntervalNs = 200'000;   // 0.2 ms: position notes fire throughout the run
    w.setFilterConfig(fc);
    for (int i = 0; i < 8; ++i) w.doWork(nowNs());

    auto enqueue = [&](auto&& make) {
        for (int i = 0; i < 100'000; ++i) {
            if (w.tryEnqueueOp(make())) return true;
            w.doWork(nowNs());
        }
        return false;
    };
    ASSERT_TRUE(enqueue([&] {
        WorkerOp op;
        op.type = WorkerOp::Type::FilterSchema;
        op.topicId = kTopic;
        op.clientVersion = kVer;
        op.schemaUpdateBlob = rune::codec::serialize(playerEvent(kVer));
        return op;
    }));
    w.doWork(nowNs());

    std::vector<Fb> filters;
    for (int s = 0; s < kFiltered; ++s) filters.push_back(worldEq(static_cast<uint32_t>(s)));
    auto subscribeOp = [&](int64_t id, const Fb* f) {
        WorkerOp op;
        op.type = WorkerOp::Type::Subscribe;
        op.subscriptionId = id; op.sessionId = 4200 + id; op.correlationId = 9000 + id;
        op.topicId = kTopic; op.partition = 0; op.nextOffset = 0;
        op.logPtr = &log; op.replica = &rep; op.isolation = 0;
        op.egressPub = pub.get();
        if (f != nullptr) op.filterBytes = f->view();
        return op;
    };
    for (int s = 0; s < kFiltered; ++s) ASSERT_TRUE(enqueue([&] { return subscribeOp(s + 1, &filters[static_cast<std::size_t>(s)]); }));
    ASSERT_TRUE(enqueue([&] { return subscribeOp(kControlSub, nullptr); }));
    for (int i = 0; i < 16; ++i) w.doWork(nowNs());

    // Mixed matches: every batch holds 32 records.  Variant 0: one member's world only (a whole-batch zero-copy match);
    // 1: three members' worlds interleaved (three rebuilds); 2: nothing anyone follows (a pure skip, feeding notes);
    // 3: half one member's world, half nobody's (a rebuild plus misses).
    std::vector<std::vector<uint8_t>> variants;
    variants.push_back(makePlayerBatch(0, kVer, rowsOf(std::vector<uint32_t>(32, 7))));
    {
        std::vector<uint32_t> ws;
        for (int i = 0; i < 32; ++i) ws.push_back(static_cast<uint32_t>(10 + i % 3));
        variants.push_back(makePlayerBatch(0, kVer, rowsOf(ws)));
    }
    variants.push_back(makePlayerBatch(0, kVer, rowsOf(std::vector<uint32_t>(32, 900'000))));
    {
        std::vector<uint32_t> ws;
        for (int i = 0; i < 32; ++i) ws.push_back(i % 2 == 0 ? 20u : 900'000u + static_cast<uint32_t>(i));
        variants.push_back(makePlayerBatch(0, kVer, rowsOf(ws)));
    }

    uint64_t dataFrames = 0, controlFrames = 0;
    // IPC fragments at the MTU: reassemble whole messages (the unfiltered control's 32-row batches span several fragments).
    aeron::FragmentAssembler assembler([&](aeron::concurrent::AtomicBuffer& buf, aeron::util::index_t off, aeron::util::index_t len, aeron::Header&) {
        const auto r = pc::Records::decode(reinterpret_cast<const uint8_t*>(buf.buffer() + off), static_cast<std::size_t>(len));
        if (r && !r->recordBatch.empty()) { ++dataFrames; if (r->subscriptionId == kControlSub) ++controlFrames; }
    });
    auto handler = assembler.handler();
    auto drain = [&] { while (sub->poll(handler, 256) > 0) {} };

    auto batchStep = [&](int i, uint64_t& allocs) {
        const auto& b = variants[static_cast<std::size_t>(i) % variants.size()];
        int64_t base = 0;
        (void)log.appendNoSyncRestamp(b.data(), b.size(), 31, base);
        rep.onLeaderLogAppended();
        {
            rune::testing::AllocWindow aw;
            w.doWork(nowNs());
            allocs += aw.count();
        }
        drain();
    };

    uint64_t ignored = 0;
    for (int i = 0; i < 100; ++i) batchStep(i, ignored);   // warm-up: scratch grows once, the first logs fire, members reach InSync
    const auto warm = w.filterCountersForTest();
    uint64_t allocs = 0;
    for (int i = 0; i < 10'000; ++i) {
        batchStep(100 + i, allocs);
        if ((i & 63) == 0) std::this_thread::sleep_for(std::chrono::microseconds(300));   // let the 0.2 ms note interval elapse
    }
    for (int i = 0; i < 64; ++i) { w.doWork(nowNs()); drain(); }

    const auto end = w.filterCountersForTest();
    EXPECT_EQ(allocs, 0u) << "heap allocations on the worker thread across 10,000 steady-state filtered batches";
    // The run must have exercised every path the gate claims: zero-copy, rebuild and notes, with the frames delivered.
    EXPECT_GT(end.zeroCopy, warm.zeroCopy);
    EXPECT_GT(end.rebuilt, warm.rebuilt);
    EXPECT_GT(end.notes, warm.notes);
    EXPECT_GT(dataFrames, 10'000u);
    EXPECT_GE(controlFrames, 10'000u) << "the unfiltered control must be served every batch while the filtered group is alive";

    sub.reset();
    pub.reset();
    aeron.reset();
    driver.reset();
    std::error_code ec;
    fs::remove_all(root, ec);
}
