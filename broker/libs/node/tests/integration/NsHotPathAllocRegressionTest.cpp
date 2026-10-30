// NsHotPathAllocRegressionTest.cpp - namespaces Phase 2 Task 15 (CLAUDE.md rules 4, 5, 12; Global Constraints 2-3): the
// namespace gate and the rate counters are free on the hot path.  A produce, a push read (fetch), a view read and the
// Throttled / over-disk refusals of a namespace-tagged partition allocate NOTHING in steady state: 10 000 requests each,
// counted by the repo's AllocWindow (the calling thread's global operator new) around the worker's pass.  The Suspended
// refusal is the gateway's and lives in rune_ns_hotpath_suspend_alloc_tests (NsHotPathSuspendAllocRegressionTest.cpp).
//
// Interaction matrix (path x namespace state):
//   | path                      | namespace state                         | expected                       | test
//   |---------------------------|-----------------------------------------|--------------------------------|-----
//   | leader produce            | tagged, limits set, not throttled       | 0 allocations / 10 000         | NsHotPathAllocRegression.ProduceZeroAllocRegression
//   | push read (fetch)         | tagged, limits set, not throttled       | 0 allocations / 10 000         | NsHotPathAllocRegression.FetchZeroAllocRegression
//   | GetViewRow                | tagged source, limits set               | 0 allocations / 10 000         | NsHotPathAllocRegression.ViewReadZeroAllocRegression
//   | produce / push / view read| Throttled (any kind), over-disk         | refused, 0 allocations / 10 000 each | NsHotPathAllocRegression.ThrottledRefusalZeroAllocRegression
//   | the counters              | every path above                        | the counters moved (not vacuous)| each test asserts the counter delta
// Mutation (Step 1): build the Throttled reply in a std::vector (Worker.h, the produce refusal) -> ThrottledRefusalZeroAllocRegression fails.
#include "HotPathBRig.h"
#include "support/AllocWindow.h"

#include <rune/codec/Descriptor.h>
#include <rune/node/NsLimitAggregator.h>
#include <rune/node/ViewSnapshotChannel.h>
#include <rune/node/WorkerViewSet.h>
#include <rune/protocol/NamespaceLimits.h>
#include <rune/storage/LogReadView.h>
#include <rune/view/PlanCompiler.h>
#include <rune/view/ViewRow.h>

namespace rv = rune::view;
using rune::node::NsMetric;
using rune::node::ThrottleKind;

namespace {

constexpr uint32_t kNs = 7;
constexpr uint16_t kSlot = 1;
constexpr int kRequests = 10'000;
constexpr int kWarmup = 512;

void tag(Rig& rig, bool withLimits = true) {
    rig.log->setNsTag(kNs, kSlot);
    rune::protocol::NamespaceLimits l;
    if (withLimits) {   // every limit set (and far away): the gate has real work to compare against
        l.produceBytesPerSec = 1ull << 40; l.produceRecordsPerSec = 1ull << 40; l.fetchBytesPerSec = 1ull << 40;
        l.requestOpsPerSec = 1ull << 40; l.queryCyclesPerSec = 1ull << 40; l.maxDiskBytes = 1ull << 40;
        l.maxViewMemoryBytes = 1ull << 40;
    }
    rig.worker().applyNsSlotAssignDirect(kSlot, kNs, l);
}

uint64_t drainCount(Rig& rig, int* nonZeroCodes = nullptr) {
    uint64_t n = 0;
    rig.pool.drainAllReturns([&](WorkerReturn r) {
        ++n;
        if (nonZeroCodes && r.errorCode != 0) ++*nonZeroCodes;
    });
    return n;
}

/// `kRequests` steps; each step's worker pass runs inside an AllocWindow (the test side - enqueue, drain - outside).
template <class Enqueue>
uint64_t allocsOver(Rig& rig, int requests, Enqueue&& enqueue, const std::vector<uint8_t>* frame) {
    uint64_t allocs = 0;
    for (int i = 0; i < requests; ++i) {
        enqueue(i);
        rig.now += 1'000'000;
        {
            rune::testing::AllocWindow w;
            if (frame) (void)rig.worker().handleIngressFrameForTest(frame->data(), frame->size(), rig.now);
            (void)rig.worker().doWork(rig.now);
            allocs += w.count();
        }
        (void)drainCount(rig);
    }
    return allocs;
}

// ---- a minimal view over a simulated committed log (the source is the Rig's tagged disk partition, topic 1) ----
constexpr int32_t kView = 0x40000007;
rune::codec::Descriptor charSource() {
    return rune::codec::DescriptorBuilder("character", 1)
        .add("characterId", rune::codec::PrimType::Int64, 1, /*isKey*/ true)
        .add("hp", rune::codec::PrimType::Int32)
        .build();
}
std::vector<uint8_t> le(uint64_t v, int n) {
    std::vector<uint8_t> b(static_cast<std::size_t>(n));
    for (int i = 0; i < n; ++i) b[static_cast<std::size_t>(i)] = static_cast<uint8_t>(v >> (8 * i));
    return b;
}
std::string_view sv(const std::vector<uint8_t>& b) { return {reinterpret_cast<const char*>(b.data()), b.size()}; }
struct SimLog {
    std::vector<uint8_t> bytes;
    int64_t end = 0;
    void append(int n) {
        rune::protocol::RecordBatchBuilder b(0, 0);
        for (int i = 0; i < n; ++i) {
            const auto kb = le(static_cast<uint64_t>(i + 1), 8);
            std::vector<uint8_t> vb;
            for (const auto& x : {le(12, 2), le(1, 2), le(static_cast<uint64_t>(i + 1), 8), le(static_cast<uint32_t>(10 * (i + 1)), 4)})
                vb.insert(vb.end(), x.begin(), x.end());
            b.append(sv(kb), sv(vb), 0);
        }
        bytes = b.build();
        end = n;
    }
    rune::storage::LogReadView read(int64_t from, int64_t) const {
        return from >= 0 && from < end ? rune::storage::LogReadView{bytes.data(), bytes.size(), 0, end} : rune::storage::LogReadView{};
    }
};

struct ViewRig {
    Rig rig;
    rune::node::ViewSnapshotChannel channel;
    rune::node::WorkerViewSet vs{channel, (fs::temp_directory_path() / ("rune-nshp-view-" + std::to_string(::getpid()))).string()};
    rune::codec::Descriptor src = charSource();
    SimLog log;
    ViewRig() {
        tag(rig);
        vs.configureNsSlots(8);
        rune::protocol::NamespaceLimits l;
        l.maxViewMemoryBytes = 1ull << 40;
        vs.onNsSlotAssign(kSlot, kNs, l);
        rv::ViewPlan v;
        v.viewName = "char_hp";
        v.key = "characterId";
        v.sources = {{1, "characterId"}};
        v.output = {{"characterId", rune::codec::PrimType::Int64, 0, "characterId", rv::MergeRule::Replace},
                    {"hp", rune::codec::PrimType::Int32, 0, "hp", rv::MergeRule::Replace}};
        v.persistence = 1;
        const rune::codec::Descriptor* srcs[] = {&src};
        auto comp = rv::compileView(v, srcs, kView);
        EXPECT_TRUE(comp.has_value());
        vs.installView(comp.value().plan, comp.value().rowLayout, src, false);
        log.append(64);
        rig.worker().setViewSet(&vs);
        auto rd = [&](int64_t f, int64_t t) { return log.read(f, t); };
        for (int i = 0; i < 2; ++i)
            (void)vs.driveFolds(1, 0, rd, log.end, rv::kNoSkip, 0, nullptr, rig.rep->leadershipGeneration(), true,
                                rune::node::NsTag{kSlot, kNs});
    }
    void enqueueRead(int i) {
        WorkerOp op;
        op.type = WorkerOp::Type::GetViewRow;
        op.topicId = kView; op.partition = 0; op.sessionId = 1; op.correlationId = 100 + i;
        op.nextOffset = 1 + (i % 64); op.hwBound = 0;
        ASSERT_TRUE(rig.worker().tryEnqueueOp(std::move(op)));
    }
};

}  // namespace

TEST(NsHotPathAllocRegression, ProduceZeroAllocRegression) {
    Rig rig;
    tag(rig);
    const auto frame = produceFrame(1, 1, 0, 100, 1);
    (void)allocsOver(rig, kWarmup, [](int) {}, &frame);   // every container reaches its steady size
    const uint64_t before = rig.worker().nsCounterForTest(kSlot, NsMetric::ProduceRecords);
    const uint64_t allocs = allocsOver(rig, kRequests, [](int) {}, &frame);
    EXPECT_EQ(allocs, 0u) << "the produce path (gate + counters) allocated";
    EXPECT_EQ(rig.worker().nsCounterForTest(kSlot, NsMetric::ProduceRecords) - before, static_cast<uint64_t>(kRequests))
        << "not vacuous: every produce was counted";
    EXPECT_EQ(rig.worker().throttledRefusalsForTest(), 0u);
}

TEST(NsHotPathAllocRegression, FetchZeroAllocRegression) {
    Rig rig;
    tag(rig);
    const auto frame = produceFrame(1, 1, 0, 100, 4);
    for (int i = 0; i < 64; ++i) rig.pass(&frame);
    rig.pool.drainAllReturns([](WorkerReturn) {});
    auto read = [&](int i) { rig.pushRead(1000 + (i & 7), 0); };
    (void)allocsOver(rig, kWarmup, read, nullptr);
    const uint64_t before = rig.worker().nsCounterForTest(kSlot, NsMetric::FetchBytes);
    const uint64_t allocs = allocsOver(rig, kRequests, read, nullptr);
    EXPECT_EQ(allocs, 0u) << "the push-read path (gate + counters) allocated";
    EXPECT_GT(rig.worker().nsCounterForTest(kSlot, NsMetric::FetchBytes), before) << "not vacuous: fetch bytes were counted";
}

TEST(NsHotPathAllocRegression, ViewReadZeroAllocRegression) {
    ViewRig v;
    auto read = [&](int i) { v.enqueueRead(i); };
    (void)allocsOver(v.rig, kWarmup, read, nullptr);
    const uint64_t before = v.rig.worker().nsCounterForTest(kSlot, NsMetric::RequestOps);
    uint64_t ok = 0;
    uint64_t allocs = 0;
    for (int i = 0; i < kRequests; ++i) {
        v.enqueueRead(i);
        v.rig.now += 1'000'000;
        {
            rune::testing::AllocWindow w;
            (void)v.rig.worker().doWork(v.rig.now);
            allocs += w.count();
        }
        v.rig.pool.drainAllReturns([&](WorkerReturn r) { if (r.type == WorkerReturn::Type::ViewRowResult && r.errorCode == 0) ++ok; });
    }
    EXPECT_EQ(allocs, 0u) << "the view-read path (gate + cycle counter) allocated";
    EXPECT_EQ(ok, static_cast<uint64_t>(kRequests)) << "not vacuous: every read was served a row";
    EXPECT_GE(v.rig.worker().nsCounterForTest(kSlot, NsMetric::RequestOps) - before, static_cast<uint64_t>(kRequests));
    EXPECT_GT(v.rig.worker().nsCounterForTest(kSlot, NsMetric::QueryCycles), 0u);
}

TEST(NsHotPathAllocRegression, ThrottledRefusalZeroAllocRegression) {
    // Produce: Throttled (53).
    {
        Rig rig;
        tag(rig);
        const auto frame = produceFrame(1, 1, 0, 100, 1);
        rig.worker().applyNsThrottleDirect(kSlot, kNs, ThrottleKind::ProduceBytes, INT64_MAX);
        (void)allocsOver(rig, kWarmup, [](int) {}, &frame);
        const uint64_t before = rig.worker().throttledRefusalsForTest();
        EXPECT_EQ(allocsOver(rig, kRequests, [](int) {}, &frame), 0u) << "the Throttled produce refusal allocated";
        EXPECT_EQ(rig.worker().throttledRefusalsForTest() - before, static_cast<uint64_t>(kRequests)) << "every produce was refused";
    }
    // Produce: over the disk limit (52).
    {
        Rig rig;
        tag(rig);
        const auto frame = produceFrame(1, 1, 0, 100, 1);
        rig.worker().applyNsThrottleDirect(kSlot, kNs, ThrottleKind::Disk, INT64_MAX);
        (void)allocsOver(rig, kWarmup, [](int) {}, &frame);
        const uint64_t before = rig.worker().overDiskRefusalsForTest();
        EXPECT_EQ(allocsOver(rig, kRequests, [](int) {}, &frame), 0u) << "the over-disk produce refusal allocated";
        EXPECT_EQ(rig.worker().overDiskRefusalsForTest() - before, static_cast<uint64_t>(kRequests));
    }
    // Push read: paced (unanswered) while FetchBytes is throttled.
    {
        Rig rig;
        tag(rig);
        const auto frame = produceFrame(1, 1, 0, 100, 4);
        for (int i = 0; i < 16; ++i) rig.pass(&frame);
        rig.pool.drainAllReturns([](WorkerReturn) {});
        rig.worker().applyNsThrottleDirect(kSlot, kNs, ThrottleKind::FetchBytes, INT64_MAX);
        auto read = [&](int i) { rig.pushRead(2000 + (i & 7), 0); };
        (void)allocsOver(rig, kWarmup, read, nullptr);
        const uint64_t before = rig.worker().throttledRefusalsForTest();
        EXPECT_EQ(allocsOver(rig, kRequests, read, nullptr), 0u) << "the throttled push read allocated";
        EXPECT_EQ(rig.worker().throttledRefusalsForTest() - before, static_cast<uint64_t>(kRequests));
    }
    // View read: Throttled (53).
    {
        ViewRig v;
        v.rig.worker().applyNsThrottleDirect(kSlot, kNs, ThrottleKind::QueryCycles, INT64_MAX);
        auto read = [&](int i) { v.enqueueRead(i); };
        (void)allocsOver(v.rig, kWarmup, read, nullptr);
        const uint64_t before = v.rig.worker().throttledRefusalsForTest();
        EXPECT_EQ(allocsOver(v.rig, kRequests, read, nullptr), 0u) << "the Throttled view-read refusal allocated";
        EXPECT_EQ(v.rig.worker().throttledRefusalsForTest() - before, static_cast<uint64_t>(kRequests));
    }
}
