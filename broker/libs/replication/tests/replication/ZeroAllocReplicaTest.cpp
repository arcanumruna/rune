// ZeroAllocReplicaTest.cpp — proves the leader's data-fetch and push paths do
// not allocate (spec §1a.5, §3.5a.2).  Counts every operator new while armed.
#include <gtest/gtest.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/storage/Log.h>
#include <rune/storage/InMemoryLog.h>
#include <rune/protocol/RecordBatch.h>

#include <atomic>
#include <cstdlib>
#include <filesystem>
#include <new>
#include <string>
#include <vector>
#include <unistd.h>

namespace {
std::atomic<bool>     g_armed{false};
std::atomic<uint64_t> g_allocs{0};
}
void* operator new(std::size_t n) {
    if (g_armed.load(std::memory_order_relaxed)) g_allocs.fetch_add(1, std::memory_order_relaxed);
    if (void* p = std::malloc(n ? n : 1)) return p;
    throw std::bad_alloc();
}
void operator delete(void* p) noexcept { std::free(p); }
void operator delete(void* p, std::size_t) noexcept { std::free(p); }

using namespace rune::replication;
namespace fs = std::filesystem;

namespace {
struct Armed {
    Armed()  { g_allocs.store(0); g_armed.store(true); }
    ~Armed() { g_armed.store(false); }
    uint64_t count() const { return g_allocs.load(); }
};
struct SinkTransport final : PartitionTransport {
    uint64_t bytes = 0, frames = 0;
    bool sendFetchRequest(const FetchRequestMsg&) override { return true; }
    bool sendFetchResponse(NodeId, const FetchResponseMsg& m) override { bytes += m.records.size(); ++frames; return true; }
};
struct Dir {
    fs::path p = fs::temp_directory_path() / ("rune_zalloc_" + std::to_string(::getpid()));
    Dir() { fs::remove_all(p); fs::create_directories(p); }
    ~Dir() { std::error_code ec; fs::remove_all(p, ec); }
};
PartitionConfig cfg(bool push) {
    PartitionConfig c; c.selfId = 1; c.minIsr = 1; c.pushOnAppend = push;
    c.isrCheckIntervalNs = 1'000'000'000'000LL; c.replicaLagMaxMs = 600'000;
    return c;
}
constexpr int64_t kMs = 1'000'000;
}

TEST(ZeroAllocReplicaRegression, DataFetchAllocatesNothing) {
    Dir d; rune::storage::Log log(d.p.string(), 64LL * 1024 * 1024); log.recover();
    SinkTransport t;
    PartitionReplica leader(cfg(/*push=*/false), Role::Leader, 1, {1, 2}, {1, 2}, &log, &t);
    const std::vector<uint8_t> payload(1024, 0x5A);
    for (int i = 0; i < 50; ++i) leader.append(std::span<const uint8_t>(payload));
    leader.onFetchRequest(FetchRequestMsg{1, 0, 2, 0, 1 << 20, 0}, 10 * kMs);   // warm-up
    const uint64_t framesBefore = t.frames;
    uint64_t n = 0;
    {
        Armed a;
        leader.onFetchRequest(FetchRequestMsg{1, 0, 2, 10, 1 << 20, 0}, 20 * kMs);
        n = a.count();
    }
    EXPECT_EQ(t.frames, framesBefore + 1);
    EXPECT_GT(t.bytes, 0u);
    EXPECT_EQ(n, 0u) << "the leader's data-fetch path must not allocate";
}

TEST(ZeroAllocReplicaRegression, PushAllocatesNothing) {
    Dir d; rune::storage::Log log(d.p.string(), 64LL * 1024 * 1024); log.recover();
    SinkTransport t;
    PartitionReplica leader(cfg(/*push=*/true), Role::Leader, 1, {1, 2}, {1, 2}, &log, &t);
    // KIP-320/595: follower 2 reports in first (validated pure ack on its empty
    // log) — a leader pushes only to validated followers.
    (void)leader.onFetchRequest(FetchRequestMsg{1, 0, 2, 0, 0, 0}, 0);
    t.frames = 0; t.bytes = 0;
    // Production produce path: batches land in the log directly, then the leader
    // is told (onLeaderLogAppended) and doWork() COALESCES them into one push.
    // (PartitionReplica::append() pushes inline — it would leave doWork nothing.)
    const std::string value(1024, 'Z');
    auto appendDirect = [&](int n) {
        for (int i = 0; i < n; ++i) {
            const int64_t base = log.logEndOffset();
            rune::protocol::RecordBatchBuilder b(base, 1000);
            b.append("k", value, 1000);
            const auto x = b.build();
            log.appendNoSync(x.data(), x.size(), base);
        }
        leader.onLeaderLogAppended();
    };
    appendDirect(10);
    leader.doWork(1 * kMs);                                       // warm-up push
    EXPECT_EQ(t.frames, 1u) << "warm-up doWork pushes [0,10) as one frame";
    appendDirect(10);
    const uint64_t framesBefore = t.frames;
    uint64_t n = 0;
    {
        Armed a;
        leader.doWork(2 * kMs);                                   // pushes [10,20)
        n = a.count();
    }
    EXPECT_EQ(t.frames, framesBefore + 1);
    EXPECT_EQ(n, 0u) << "the leader's push path (doWork) must not allocate";
}

// A segment ROLL must not make the next fetch allocate: the read mapping of a
// freshly-rolled segment is created with the segment (off the push/fetch path),
// not lazily by the first readView() on the worker's hot path.
TEST(ZeroAllocReplicaRegression, FetchFromFreshlyRolledSegmentAllocatesNothing) {
    Dir d; rune::storage::Log log(d.p.string(), 16LL * 1024); log.recover();   // tiny segments
    SinkTransport t;
    PartitionReplica leader(cfg(/*push=*/false), Role::Leader, 1, {1, 2}, {1, 2}, &log, &t);
    const std::vector<uint8_t> payload(1024, 0x5A);
    for (int i = 0; i < 60; ++i) leader.append(std::span<const uint8_t>(payload));
    ASSERT_GT(log.segmentBaseOffsets().size(), 2u) << "test needs several rolled segments";
    leader.onFetchRequest(FetchRequestMsg{1, 0, 2, 0, 1 << 20, 0}, 10 * kMs);   // warm-up (1st segment)
    const uint64_t framesBefore = t.frames;
    const int64_t lastSegBase = log.segmentBaseOffsets().back();
    uint64_t n = 0;
    {
        Armed a;
        leader.onFetchRequest(FetchRequestMsg{1, 0, 2, lastSegBase, 1 << 20, 0}, 20 * kMs);
        n = a.count();
    }
    EXPECT_EQ(t.frames, framesBefore + 1);
    EXPECT_EQ(n, 0u) << "first fetch from a rolled segment must not allocate its read map";
}

// Phase 2b E2 (fix-on-sight): applyReplicated copied the new suffix of every
// replicated response into a fresh std::vector before appending — a heap
// allocation per apply on the follower's hot path (disk AND transient).
TEST(ZeroAllocReplicaRegression, TransientFollowerApplyAllocatesNothing) {
    rune::storage::InMemoryLog mem(1 << 20, 4096);
    SinkTransport t;
    PartitionConfig c = cfg(/*push=*/false);
    c.selfId = 2;
    PartitionReplica follower(c, Role::Follower, 1, {1, 2}, {1, 2}, &mem, &t);
    auto batchAt = [](int64_t base) {
        rune::protocol::RecordBatchBuilder b(base, 1000);
        b.append("k", std::string(512, 'v'), 1000);
        auto x = b.build();
        rune::protocol::RecordBatchReader::stampLeaderEpoch(x.data(), 1);
        return x;
    };
    const auto b0 = batchAt(0), b1 = batchAt(1);
    FetchResponseMsg r0{};
    r0.leaderEpoch = 1; r0.partition = 0; r0.highWatermark = 1; r0.logStartOffset = 0;
    r0.errorCode = 0; r0.topicId = 0; r0.records = std::span<const uint8_t>(b0);
    follower.applyReplicated(r0);   // warm-up: the epoch-cache entry for epoch 1 is created here
    ASSERT_EQ(follower.logEndOffset(), 1);
    FetchResponseMsg r1 = r0;
    r1.highWatermark = 2;
    r1.records = std::span<const uint8_t>(b1);
    uint64_t n = 0;
    {
        Armed a;
        follower.applyReplicated(r1);
        n = a.count();
    }
    EXPECT_EQ(follower.logEndOffset(), 2);
    EXPECT_EQ(n, 0u) << "the follower's replicated apply must not allocate";
}
