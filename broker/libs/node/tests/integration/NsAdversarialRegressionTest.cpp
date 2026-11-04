#include "HotPathBRig.h"
#include "TransientRig.h"
#include "support/AllocWindow.h"
#include <rune/protocol/NamespaceLimits.h>
#include <map>
#include <set>
#include <random>

namespace {
using rune::node::NsMetric;
using rune::node::ThrottleKind;
constexpr int16_t kOk = 0;
constexpr int16_t kLimit52     = static_cast<int16_t>(pc::ErrorCode::NamespaceLimitExceeded);
constexpr int16_t kThrottled53 = static_cast<int16_t>(pc::ErrorCode::Throttled);
constexpr int16_t kUnknown     = static_cast<int16_t>(pc::ErrorCode::UnknownTopic);
constexpr int16_t kInvalid     = static_cast<int16_t>(pc::ErrorCode::InvalidRequest);
constexpr int16_t kNotLeader   = static_cast<int16_t>(pc::ErrorCode::NotLeader);
struct Ans { int16_t code = -1; int64_t lastOffset = 0; };

/// Every NsUsage ProduceBytes signal drained anywhere: slot -> nsId (the periodic report in doWork also sends them).
std::map<uint16_t, uint32_t> gReported;
void noteSignal(const WorkerReturn& rr) {
    if (rr.type == WorkerReturn::Type::SessionSignal &&
        rr.lastOffset == static_cast<int64_t>(rune::node::SessionSignalKind::NsUsage) &&
        (rr.sessionId & 0xFF) == static_cast<int64_t>(NsMetric::ProduceBytes))
        gReported[static_cast<uint16_t>(rr.sessionId >> 8)] = static_cast<uint32_t>(rr.correlationId);
}
Ans diskProduce(Rig& rig, int64_t corr, std::size_t valueBytes = 100, int recs = 1) {
    const auto frame = produceFrame(corr, 1, 0, valueBytes, recs);
    rig.pass(&frame);
    Ans a;
    for (int i = 0; i < 64 && a.code == -1; ++i) {
        rig.pool.drainAllReturns([&](WorkerReturn r) {
            noteSignal(r);
            if (r.type == WorkerReturn::Type::ProduceResult && r.correlationId == corr) { a.code = r.errorCode; a.lastOffset = r.lastOffset; }
        });
        if (a.code == -1) rig.pass();
    }
    return a;
}
Ans tProduce(transient_rig::Rig& rig, const std::vector<uint8_t>& batch, int64_t pid = -1, int32_t seq = -1) {
    const int64_t c = ++rig.corr;
    const auto frame = transient_rig::produceFrame(c, batch, 1, pid, 0, seq);
    rig.pass(&frame);
    Ans a;
    for (int i = 0; i < 64 && a.code == -1; ++i) {
        rig.pool.drainAllReturns([&](WorkerReturn r) {
            if (r.type == WorkerReturn::Type::ProduceResult && r.correlationId == c) { a.code = r.errorCode; a.lastOffset = r.lastOffset; }
        });
        if (a.code == -1) rig.pass();
    }
    return a;
}
/// The GATEWAY-op produce entry (WorkerOp::Produce) - a sibling entry point to produceCore_.
Ans tOpProduce(transient_rig::Rig& rig, const std::vector<uint8_t>& batch) {
    const int64_t c = ++rig.corr;
    WorkerOp op;
    op.type = WorkerOp::Type::Produce; op.topicId = transient_rig::kTopic; op.partition = 0;
    op.replica = rig.rep; op.logPtr = nullptr; op.sessionId = 1; op.correlationId = c; op.durabilityTier = 1; op.acks = 1;
    op.recordBatchBytes = batch;
    EXPECT_TRUE(rig.pool.tryEnqueueOp(transient_rig::kTopic, 0, std::move(op)));
    Ans a;
    for (int i = 0; i < 64 && a.code == -1; ++i) {
        rig.pass();
        rig.pool.drainAllReturns([&](WorkerReturn r) {
            if (r.type == WorkerReturn::Type::ProduceResult && r.correlationId == c) { a.code = r.errorCode; a.lastOffset = r.lastOffset; }
        });
    }
    return a;
}
rune::protocol::NamespaceLimits sizeLimits(uint64_t msg, uint64_t batch) {
    rune::protocol::NamespaceLimits l; l.maxMessageBytes = msg; l.maxBatchBytes = batch; return l;
}
}  // namespace

// #728 seeded model check: the report visits exactly the live slots and reports each with ITS nsId.
TEST(NsUsageReportAdversarial, LiveListSeededModelAdversarialRegression) {
    const uint32_t seed = 0x728u;
    gReported.clear();
    SCOPED_TRACE("seed=" + std::to_string(seed));
    std::mt19937 rng(seed);
    Rig rig;
    Worker& w = rig.worker();
    constexpr uint32_t kCap = 48;
    w.configureNsSlots(kCap);
    std::map<uint16_t, uint32_t> model{{0, 0}};
    auto slotOfNs = [&](uint32_t ns) { for (auto& [s, n] : model) if (n == ns) return int(s); return -1; };
    uint32_t nextNs = 1;
    int64_t corr = 0;
    for (int op = 0; op < 4000; ++op) {
        const uint32_t r = rng() % 10;
        const auto slot = static_cast<uint16_t>(rng() % (kCap + 4));
        if (r < 5) {
            const uint32_t ns = (rng() % 4 == 0 && nextNs > 1) ? 1 + rng() % (nextNs - 1) : nextNs++;
            w.applyNsSlotAssignDirect(slot, ns, rune::protocol::NamespaceLimits{});
            if (slot < kCap && !model.contains(slot) && slotOfNs(ns) == -1) model[slot] = ns;
        } else {
            uint32_t ns = rng() % 3 == 0 ? rng() % (nextNs + 1) : (model.contains(slot) ? model[slot] : 12345);
            const bool ok = w.applyNsSlotReleaseDirect(slot, ns);
            const bool expect = slot != 0 && model.contains(slot) && model[slot] == ns;
            ASSERT_EQ(ok, expect) << "op " << op << " release(" << slot << "," << ns << ")";
            if (expect) model.erase(slot);
        }
        if (op % 25 != 0) continue;
        gReported.clear();
        for (auto& [s, n] : model) { rig.log->setNsTag(n, s); ASSERT_EQ(diskProduce(rig, ++corr).code, kOk) << s; }
        rig.pool.drainAllReturns([](WorkerReturn rr) { noteSignal(rr); });
        (void)w.reportNsUsageForTest();
        ASSERT_EQ(w.nsUsageVisitedForTest(), model.size()) << "op " << op;
        rig.pool.drainAllReturns([](WorkerReturn rr) { noteSignal(rr); });
        ASSERT_EQ(gReported, model) << "op " << op << ": the reported (slot, nsId) set is the live set";
    }
}

TEST(NsUsageReportAdversarial, DefaultSlotNeverDroppedAdversarialRegression) {
    Rig rig;
    Worker& w = rig.worker();
    w.configureNsSlots(8);
    EXPECT_FALSE(w.applyNsSlotReleaseDirect(0, 0));
    w.applyNsSlotAssignDirect(0, 5, rune::protocol::NamespaceLimits{});
    w.applyNsSlotAssignDirect(0, 0, rune::protocol::NamespaceLimits{});
    (void)w.reportNsUsageForTest();
    EXPECT_EQ(w.nsUsageVisitedForTest(), 1u);
    for (uint16_t s = 1; s < 8; ++s) w.applyNsSlotAssignDirect(s, 100 + s, rune::protocol::NamespaceLimits{});
    w.applyNsSlotAssignDirect(3, 103, rune::protocol::NamespaceLimits{});
    (void)w.reportNsUsageForTest();
    EXPECT_EQ(w.nsUsageVisitedForTest(), 8u);
    uint64_t allocs = 0;
    {
        rune::testing::AllocWindow win;
        for (uint16_t s = 7; s >= 1; --s) (void)w.applyNsSlotReleaseDirect(s, 100 + s);
        for (uint16_t s = 1; s < 8; ++s) w.applyNsSlotAssignDirect(s, 200 + s, rune::protocol::NamespaceLimits{});
        allocs = win.count();
    }
    EXPECT_EQ(allocs, 0u);
    (void)w.reportNsUsageForTest();
    EXPECT_EQ(w.nsUsageVisitedForTest(), 8u);
}

TEST(NsDiskAdversarial, ReusedSlotStaleProduceAndFreeSlotNotChargedAdversarialRegression) {
    Rig rig;
    Worker& w = rig.worker();
    rig.log->setNsTag(7, 2);
    w.applyNsSlotAssignDirect(2, 7, rune::protocol::NamespaceLimits{});
    ASSERT_EQ(diskProduce(rig, 1, 4096).code, kOk);
    w.scanNamespaceDiskForTest(rig.now);
    ASSERT_GT(w.nsDiskBytesForTest(2), 4096);
    ASSERT_TRUE(w.applyNsSlotReleaseDirect(2, 7));
    EXPECT_EQ(w.nsDiskBytesForTest(2), 0);
    w.scanNamespaceDiskForTest(rig.now);
    EXPECT_EQ(w.nsDiskBytesForTest(2), 0) << "a free slot is not charged by the scan";
    w.applyNsSlotAssignDirect(2, 9, rune::protocol::NamespaceLimits{});
    EXPECT_EQ(diskProduce(rig, 2, 4096).code, kInvalid) << "a stale tag is refused";
    EXPECT_EQ(w.nsDiskBytesForTest(2), 0);
    w.scanNamespaceDiskForTest(rig.now);
    EXPECT_EQ(w.nsDiskBytesForTest(2), 0) << "the scan does not charge the new owner";
    rig.pool.drainAllReturns([](WorkerReturn) {});
    (void)w.reportNsUsageForTest();
    rig.pool.drainAllReturns([&](WorkerReturn rr) {
        if (rr.type == WorkerReturn::Type::SessionSignal && rr.lastOffset == static_cast<int64_t>(rune::node::SessionSignalKind::NsUsage) &&
            (rr.sessionId >> 8) == 2 && (rr.sessionId & 0xFF) == static_cast<int64_t>(NsMetric::DiskBytes))
            ADD_FAILURE() << "slot 2 reported disk bytes " << rr.baseOffset << " for nsId " << rr.correlationId;
    });
}

TEST(NsDiskAdversarial, SlotTagPastCapacityAdversarialRegression) {
    Rig rig;
    Worker& w = rig.worker();
    w.configureNsSlots(4);
    ASSERT_EQ(diskProduce(rig, 1, 1024).code, kOk);
    rig.log->setNsTag(3, 4);
    EXPECT_EQ(diskProduce(rig, 2, 1024).code, kNotLeader);
    w.scanNamespaceDiskForTest(rig.now);
    for (uint16_t s = 0; s < 4; ++s) EXPECT_EQ(w.nsDiskBytesForTest(s), 0) << s;
}

TEST(NsTransientAdversarial, ThrottleRetryAfterThenClearedAdversarialRegression) {
    transient_rig::Rig rig;
    rig.mem.setNsTag(1, 1);
    rig.worker().applyNsSlotAssignDirect(1, 1, rune::protocol::NamespaceLimits{});
    const auto b = transient_rig::batchOf(1, 100);
    const int64_t until = rig.now + 500'000'000;
    rig.worker().applyNsThrottleDirect(1, 1, ThrottleKind::ProduceRecords, until);
    const int64_t end = rig.mem.logEndOffset();
    const Ans a = tProduce(rig, b);
    EXPECT_EQ(a.code, kThrottled53);
    EXPECT_GT(a.lastOffset, 0);
    EXPECT_LE(a.lastOffset, until - (rig.now - 64'000'000));
    EXPECT_EQ(rig.mem.logEndOffset(), end);
    rig.worker().applyNsThrottleDirect(1, 1, ThrottleKind::ProduceRecords, 0);
    EXPECT_EQ(tProduce(rig, b).code, kOk);
    rig.worker().applyNsThrottleDirect(1, 1, ThrottleKind::FetchBytes, INT64_MAX);
    rig.worker().applyNsThrottleDirect(1, 1, ThrottleKind::QueryCycles, INT64_MAX);
    EXPECT_EQ(tProduce(rig, b).code, kOk);
    rig.worker().applyNsThrottleDirect(1, 1, ThrottleKind::RequestOps, INT64_MAX);
    EXPECT_EQ(tProduce(rig, b).code, kThrottled53);
}

TEST(NsTransientAdversarial, BatchBytesBoundaryAdversarialRegression) {
    const auto b = transient_rig::batchOf(3, 200);
    const uint64_t L = b.size();
    struct Cell { uint64_t limit; int16_t expect; } cells[] = {{L - 1, kLimit52}, {L, kOk}, {L + 1, kOk}, {1, kLimit52}};
    for (const Cell& c : cells) {
        SCOPED_TRACE("limit=" + std::to_string(c.limit));
        transient_rig::Rig rig;
        rig.mem.setNsTag(1, 1);
        rig.worker().applyNsSlotAssignDirect(1, 1, sizeLimits(0, c.limit));
        const int64_t end = rig.mem.logEndOffset();
        EXPECT_EQ(tProduce(rig, b).code, c.expect);
        if (c.expect != kOk) EXPECT_EQ(rig.mem.logEndOffset(), end);
    }
}

TEST(NsTransientAdversarial, MessageBytesOneLongRecordAdversarialRegression) {
    rune::protocol::RecordBatchBuilder bb(0, 1000);
    bb.append("k0", std::string(10, 'v'), 1000);
    bb.append("k1", std::string(3000, 'v'), 1001);
    bb.append("k2", std::string(10, 'v'), 1002);
    const auto b = bb.build();
    transient_rig::Rig rig;
    rig.mem.setNsTag(1, 1);
    rig.worker().applyNsSlotAssignDirect(1, 1, sizeLimits(2048, 0));
    EXPECT_EQ(tProduce(rig, b).code, kLimit52);
    rig.worker().applyNsSlotAssignDirect(1, 1, sizeLimits(8192, 0));
    EXPECT_EQ(tProduce(rig, b).code, kOk);
}

TEST(NsTransientAdversarial, GatewayOpProduceGatedAdversarialRegression) {
    transient_rig::Rig rig;
    rig.mem.setNsTag(1, 1);
    rig.worker().applyNsSlotAssignDirect(1, 1, sizeLimits(1024, 0));
    const int64_t end = rig.mem.logEndOffset();
    EXPECT_EQ(tOpProduce(rig, transient_rig::batchOf(1, 4096)).code, kLimit52);
    EXPECT_EQ(rig.mem.logEndOffset(), end);
    rig.worker().applyNsThrottleDirect(1, 1, ThrottleKind::NsDeleting, 0);
    EXPECT_EQ(tOpProduce(rig, transient_rig::batchOf(1, 10)).code, kUnknown);
    EXPECT_EQ(rig.mem.logEndOffset(), end);
}

TEST(NsTransientAdversarial, IdempotentProduceSizeGatedAdversarialRegression) {
    transient_rig::Rig rig;
    rig.mem.setNsTag(1, 1);
    rig.worker().applyNsSlotAssignDirect(1, 1, sizeLimits(1024, 0));
    const int64_t end = rig.mem.logEndOffset();
    EXPECT_EQ(tProduce(rig, transient_rig::batchOf(1, 4096, 7, 0, 0), 7, 0).code, kLimit52);
    EXPECT_EQ(rig.mem.logEndOffset(), end);
}

TEST(NsTransientAdversarial, DeletingThenSlotReuseAdversarialRegression) {
    transient_rig::Rig rig;
    rig.mem.setNsTag(1, 1);
    rig.worker().applyNsSlotAssignDirect(1, 1, sizeLimits(1024, 0));
    rig.worker().applyNsThrottleDirect(1, 1, ThrottleKind::Disk, INT64_MAX);
    EXPECT_EQ(tProduce(rig, transient_rig::batchOf(1, 10)).code, kOk) << "OverDisk exempts a transient partition";
    rig.worker().applyNsThrottleDirect(1, 1, ThrottleKind::NsDeleting, 0);
    EXPECT_EQ(tProduce(rig, transient_rig::batchOf(1, 10)).code, kUnknown);
    rig.worker().applyNsThrottleDirect(1, 1, ThrottleKind::Disk, 0);
    EXPECT_EQ(tProduce(rig, transient_rig::batchOf(1, 10)).code, kUnknown) << "a Disk clear never lifts DELETING";
    ASSERT_TRUE(rig.worker().applyNsSlotReleaseDirect(1, 1));
    rig.worker().applyNsSlotAssignDirect(1, 2, rune::protocol::NamespaceLimits{});
    EXPECT_EQ(tProduce(rig, transient_rig::batchOf(1, 4096)).code, kInvalid) << "stale tag";
    rig.mem.setNsTag(2, 1);
    EXPECT_EQ(tProduce(rig, transient_rig::batchOf(1, 4096)).code, kOk) << "no DELETING, no inherited limit";
    EXPECT_EQ(rig.worker().nsCounterForTest(1, NsMetric::ProduceRecords), 1u);
}

TEST(NsTransientAdversarial, TransientRefusalsZeroAllocAdversarialRegression) {
    transient_rig::Rig rig;
    rig.mem.setNsTag(1, 1);
    rig.worker().applyNsSlotAssignDirect(1, 1, sizeLimits(1024, 0));
    const auto big   = transient_rig::produceFrame(1, transient_rig::batchOf(1, 4096), 1);
    const auto small = transient_rig::produceFrame(2, transient_rig::batchOf(1, 10), 1);
    auto run = [&](const std::vector<uint8_t>& f, int n) {
        uint64_t allocs = 0;
        for (int i = 0; i < n; ++i) {
            rig.now += 1'000'000;
            { rune::testing::AllocWindow win;
              (void)rig.worker().handleIngressFrameForTest(f.data(), f.size(), rig.now);
              (void)rig.worker().doWork(rig.now);
              allocs += win.count(); }
            rig.pool.drainAllReturns([](WorkerReturn) {});
        }
        return allocs;
    };
    (void)run(big, 64);
    const uint64_t sizeBefore = rig.worker().nsSizeRefusalsForTest();
    EXPECT_EQ(run(big, 2000), 0u) << "52";
    EXPECT_EQ(rig.worker().nsSizeRefusalsForTest() - sizeBefore, 2000u);
    rig.worker().applyNsThrottleDirect(1, 1, ThrottleKind::ProduceBytes, INT64_MAX);
    (void)run(small, 64);
    const uint64_t thrBefore = rig.worker().throttledRefusalsForTest();
    EXPECT_EQ(run(small, 2000), 0u) << "53";
    EXPECT_EQ(rig.worker().throttledRefusalsForTest() - thrBefore, 2000u);
    rig.worker().applyNsThrottleDirect(1, 1, ThrottleKind::NsDeleting, 0);
    (void)run(small, 64);
    EXPECT_EQ(run(small, 2000), 0u) << "DELETING";
}

// #728 (disk scan): the scan zeroes the live slots only, never the table capacity.
TEST(NsDiskAdversarial, DiskScanVisitsLiveSlotsOnlyRegression) {
    Rig rig;
    Worker& w = rig.worker();
    w.configureNsSlots(65535);
    for (uint16_t s = 1; s <= 3; ++s) w.applyNsSlotAssignDirect(s, 100 + s, rune::protocol::NamespaceLimits{});
    w.scanNamespaceDiskForTest(rig.now);
    EXPECT_EQ(w.nsDiskScanVisitedForTest(), 4u) << "default + 3 namespaces, not 65535";
    ASSERT_TRUE(w.applyNsSlotReleaseDirect(2, 102));
    w.scanNamespaceDiskForTest(rig.now);
    EXPECT_EQ(w.nsDiskScanVisitedForTest(), 3u);
}

// The table's dense live list: an equal re-place never duplicates, a release swap-pops, no allocation after reset().
TEST(NsSlotTableLiveListRegression, ModelAndZeroAllocRegression) {
    using rune::node::NsSlotTable;
    constexpr uint32_t kCap = 16;
    NsSlotTable t(kCap);
    auto live = [&] { return std::set<uint16_t>(t.liveSlots().begin(), t.liveSlots().end()); };
    std::set<uint16_t> model{0};
    ASSERT_EQ(live(), model);
    ASSERT_TRUE(t.place(3, 30));
    ASSERT_TRUE(t.place(3, 30));   // equal re-place
    ASSERT_TRUE(t.place(5, 50));
    ASSERT_TRUE(t.place(9, 90));
    model = {0, 3, 5, 9};
    EXPECT_EQ(t.liveSlots().size(), 4u) << "an equal re-place must not duplicate";
    EXPECT_EQ(live(), model);
    ASSERT_TRUE(t.release(3, 30));   // swap-pop: the last element (9) moves into slot 3's position
    model.erase(3);
    EXPECT_EQ(t.liveSlots().size(), 3u);
    EXPECT_EQ(live(), model);
    ASSERT_TRUE(t.release(9, 90));   // the moved element: its recorded position must have followed it
    model.erase(9);
    EXPECT_EQ(live(), model);
    ASSERT_TRUE(t.release(5, 50));
    model.erase(5);
    EXPECT_EQ(live(), model);
    EXPECT_FALSE(t.release(0, 0));
    uint64_t allocs = 0;
    {
        rune::testing::AllocWindow win;
        for (int round = 0; round < 50; ++round) {
            for (uint16_t s = 1; s < kCap; ++s) (void)t.place(s, 1000 + s);
            for (uint16_t s = kCap - 1; s >= 1; --s) (void)t.release(s, 1000 + s);
        }
        allocs = win.count();
    }
    EXPECT_EQ(allocs, 0u);
    EXPECT_EQ(live(), (std::set<uint16_t>{0}));
    auto a = t.assign(77);
    ASSERT_TRUE(a.has_value());
    EXPECT_EQ(t.liveSlots().size(), 2u);
    EXPECT_EQ(t.assign(77), a) << "an idempotent assign does not duplicate";
    EXPECT_EQ(t.liveSlots().size(), 2u);
}
