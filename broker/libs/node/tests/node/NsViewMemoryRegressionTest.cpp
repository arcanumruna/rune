// NsViewMemoryRegressionTest.cpp - namespaces Phase 2 Task 12 (D26): a namespace's view-memory limit pauses its views at an
// offset and resumes them.  Deterministic: a WorkerViewSet driven over a simulated committed log (no Aeron, no clocks);
// the paused read is answered by a real Worker (GetViewRow op -> ViewRowResult).
//
// Interaction matrix (limit x {below, at, above} x view state / peer):
//   | state                                   | event                       | expected                                                | test
//   |-----------------------------------------|-----------------------------|---------------------------------------------------------|-----
//   | usage < L / == L / > L                  | fold, then a read           | Ok, Ok, then the next pass pauses: read = Paused(54)    | LimitEnforcementTableRegression.ViewMemoryRegression
//   | paused                                  | GetViewRow (Worker)         | ViewPaused 54 + the paused offset in lastOffset         | NsViewMemoryRegression.PausedReadReturnsCodeAndOffsetRegression
//   | paused, new records appended            | limit raised, then drive    | resumes from the paused offset == recompute from log    | NsViewMemoryRegression.ResumeCatchesUpEqualsRecomputeRegression
//   | at the limit                            | one batch of N new keys     | usage <= L + chunks(N); the next batch is NOT folded    | NsViewMemoryRegression.OvershootAtMostOneBatchRegression
//   | namespace A over                        | namespace B's view          | B keeps folding and serving                             | NsViewMemoryRegression.OtherNamespaceViewsUnaffectedRegression
//   | paused                                  | the view is dropped         | the memory is released, the peer view resumes           | NsViewMemoryRegression.DroppedViewReleasesMemoryRegression
//   | slot released and reassigned            | stale / new tag             | a reused slot starts from zero, never charged by the old | NsViewMemoryRegression.ReusedSlotStartsFromZeroRegression
#include <gtest/gtest.h>
#include <unistd.h>

#include <bit>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <string>
#include <string_view>
#include <vector>

#include <rune/codec/Descriptor.h>
#include <rune/node/ViewSnapshotChannel.h>
#include <rune/node/Worker.h>
#include <rune/node/WorkerOp.h>
#include <rune/node/WorkerViewSet.h>
#include <rune/protocol/NamespaceLimits.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/protocol/client/DataMessages.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/storage/InMemoryLog.h>
#include <rune/storage/LogReadView.h>
#include <rune/view/PlanCompiler.h>
#include <rune/view/ViewRow.h>

#include "support/LimitEnforcementTable.h"

using namespace rune::node;
namespace rv = rune::view;
namespace pc = rune::protocol::client;

namespace {

constexpr int32_t kViewA = 0x40000007, kViewB = 0x40000008;
constexpr int32_t kSrcA = 5, kSrcB = 6;
constexpr uint16_t kSlotA = 1, kSlotB = 2;
constexpr uint32_t kNsA = 11, kNsB = 12;
constexpr std::size_t kChunkBytes = 1024;

std::string tmpDir(const std::string& name) {
    return (std::filesystem::temp_directory_path() / (name + "-" + std::to_string(::getpid()))).string();
}

rune::codec::Descriptor charSource() {
    return rune::codec::DescriptorBuilder("character", 1)
        .add("characterId", rune::codec::PrimType::Int64, 1, /*isKey*/ true)
        .add("hp", rune::codec::PrimType::Int32)
        .build();
}

rv::ViewPlan hpPlan(int32_t srcTopic) {
    rv::ViewPlan v;
    v.viewName    = "char_hp";
    v.key         = "characterId";
    v.sources     = {{srcTopic, "characterId"}};
    v.output      = {{"characterId", rune::codec::PrimType::Int64, 0, "characterId", rv::MergeRule::Replace},
                     {"hp", rune::codec::PrimType::Int32, 0, "hp", rv::MergeRule::Replace}};
    v.persistence = 1;   // transient: no checkpoint IO
    v.sizing.chunkBytes = static_cast<uint32_t>(kChunkBytes);
    return v;
}

std::vector<uint8_t> le(uint64_t v, int n) {
    std::vector<uint8_t> b(static_cast<std::size_t>(n));
    for (int i = 0; i < n; ++i) b[static_cast<std::size_t>(i)] = static_cast<uint8_t>(v >> (8 * i));
    return b;
}
std::string_view sv(const std::vector<uint8_t>& b) { return {reinterpret_cast<const char*>(b.data()), b.size()}; }

std::vector<uint8_t> character(int64_t id, int32_t hp) {
    std::vector<uint8_t> b;
    auto add = [&](const std::vector<uint8_t>& x) { b.insert(b.end(), x.begin(), x.end()); };
    add(le(12, 2)); add(le(1, 2)); add(le(static_cast<uint64_t>(id), 8)); add(le(static_cast<uint32_t>(hp), 4));
    return b;
}

/// A simulated committed log: batches of upserts (key k -> hp 10k), each its own batch.
struct SimLog {
    struct B { std::vector<uint8_t> bytes; int64_t base; int32_t count; };
    std::vector<B> batches;
    int64_t end = 0;
    /// Appends `n` upserts for keys firstKey .. firstKey+n-1 as ONE batch.
    void append(int64_t firstKey, int n, int32_t hpMul = 10) {
        rune::protocol::RecordBatchBuilder b(end, 0);
        for (int i = 0; i < n; ++i) {
            const int64_t k = firstKey + i;
            const auto kb = le(static_cast<uint64_t>(k), 8);
            const auto vb = character(k, static_cast<int32_t>(k * hpMul));
            b.append(sv(kb), sv(vb), 0);
        }
        batches.push_back({b.build(), end, n});
        end += n;
    }
    rune::storage::LogReadView read(int64_t from, int64_t /*to*/) const {
        for (const B& b : batches)
            if (from >= b.base && from < b.base + b.count)
                return rune::storage::LogReadView{b.bytes.data(), b.bytes.size(), b.base, b.base + b.count};
        return {};
    }
};

struct SrcReplica {
    rune::storage::InMemoryLog            log{1 << 20, 1000};
    rune::replication::InMemoryTransport  transport;
    std::unique_ptr<rune::replication::PartitionReplica> replica;
    explicit SrcReplica(int32_t topicId) {
        rune::replication::PartitionConfig cfg{};
        cfg.topicId = topicId; cfg.partitionId = 0; cfg.selfId = 1; cfg.minIsr = 1;
        cfg.replicaLagMaxMs = 10'000; cfg.replicaLagMaxOffset = 10'000;
        cfg.fetchIntervalNs = 1'000'000LL; cfg.isrCheckIntervalNs = 1'000'000LL; cfg.maxFetchBytes = 1 << 20;
        cfg.pushOnAppend = false;
        replica = std::make_unique<rune::replication::PartitionReplica>(
            cfg, rune::replication::Role::Follower, 0, std::vector<rune::replication::NodeId>{1},
            std::vector<rune::replication::NodeId>{1}, &log, &transport);
        replica->becomeLeader(1, {1});
    }
    [[nodiscard]] uint64_t gen() const { return replica->leadershipGeneration(); }
};

struct Rig {
    rv::CompileResult compA, compB;
    rune::codec::Descriptor src = charSource();
    ViewSnapshotChannel channel;
    WorkerViewSet vs{channel, tmpDir("rune-ns-viewmem")};
    uint32_t rowLen = 0;
    uint32_t rowsPerChunk = 0;
    uint64_t chunkB = 0;   ///< the bytes of one table chunk

    Rig(uint64_t limitA, uint64_t limitB = 0, bool installB = false) {
        const rune::codec::Descriptor* srcs[] = {&src};
        compA = rv::compileView(hpPlan(kSrcA), srcs, kViewA).value();
        rowLen = compA.rowLayout.rowLen;
        rowsPerChunk = static_cast<uint32_t>(std::bit_floor(kChunkBytes / rowLen));
        chunkB = static_cast<uint64_t>(rowsPerChunk) * rowLen;
        vs.installView(compA.plan, compA.rowLayout, src, /*persisted*/ false);
        assign(kSlotA, kNsA, limitA * chunkB);
        if (installB) {
            compB = rv::compileView(hpPlan(kSrcB), srcs, kViewB).value();
            vs.installView(compB.plan, compB.rowLayout, src, false);
            assign(kSlotB, kNsB, limitB * chunkB);
        }
    }
    void assign(uint16_t slot, uint32_t ns, uint64_t limitBytes) {
        rune::protocol::NamespaceLimits l;
        l.maxViewMemoryBytes = limitBytes;
        vs.onNsSlotAssign(slot, ns, l);
    }
    int drive(int32_t srcTopic, uint16_t slot, uint32_t ns, const SimLog& log, uint64_t gen = 0) {
        auto rd = [&](int64_t f, int64_t t) { return log.read(f, t); };
        return vs.driveFolds(srcTopic, 0, rd, log.end, rv::kNoAbortedTxns, 0, nullptr, gen, true, NsTag{slot, ns});
    }
    int driveA(const SimLog& log, uint64_t gen = 0) { return drive(kSrcA, kSlotA, kNsA, log, gen); }
    const uint8_t* row(int32_t view, int64_t key, WorkerViewSet::RowStatus& rs) {
        const rv::RowLayout* L = nullptr;
        return vs.getRow(view, 0, static_cast<uint64_t>(key), L, &rs);
    }
};

}  // namespace

// Below / At / Above: usage < L serves, usage == L still serves, the fold that crosses L completes (the overshoot) and
// the NEXT pass pauses the view: reads answer Paused with the offset the view stopped at.
TEST(LimitEnforcementTableRegression, ViewMemoryRegression) {
    using rune::node::test::rowOf;
    const auto* row = rowOf("max-view-memory-bytes");
    ASSERT_NE(row, nullptr);
    EXPECT_EQ(row->index, 23u);
    EXPECT_EQ(row->codeAbove, 54);
    Rig rig(/*limit chunks*/ row->limit);
    const int64_t R = rig.rowsPerChunk;
    ASSERT_GE(R, 4);
    SimLog log;
    WorkerViewSet::RowStatus rs{};

    // Below: one chunk (< 2).
    log.append(1, static_cast<int>(R / 2));
    rig.driveA(log);
    EXPECT_EQ(rig.vs.nsViewMemBytes(kSlotA), rig.chunkB);
    EXPECT_FALSE(rig.vs.nsViewMemOver(kSlotA));
    EXPECT_NE(rig.row(kViewA, 1, rs), nullptr);
    EXPECT_EQ(rs, WorkerViewSet::RowStatus::Ok);

    // At: exactly two chunks == the limit still serves.
    log.append(1 + R / 2, static_cast<int>(R / 2 + 1));
    rig.driveA(log);
    EXPECT_EQ(rig.vs.nsViewMemBytes(kSlotA), 2 * rig.chunkB);
    EXPECT_FALSE(rig.vs.nsViewMemOver(kSlotA)) << "usage == limit is not over";
    EXPECT_NE(rig.row(kViewA, R, rs), nullptr);

    // Above: this fold completes (3 chunks) ...
    log.append(2 + R, static_cast<int>(R));
    rig.driveA(log);
    EXPECT_EQ(rig.vs.nsViewMemBytes(kSlotA), 3 * rig.chunkB);
    EXPECT_TRUE(rig.vs.nsViewMemOver(kSlotA));
    // ... and the next pass pauses the view at the end of what it folded.
    rig.driveA(log);
    EXPECT_EQ(rig.row(kViewA, 1, rs), nullptr);
    EXPECT_EQ(rs, WorkerViewSet::RowStatus::Paused);
    const auto st = rig.vs.instanceServeState(kViewA, 0);
    EXPECT_TRUE(st.paused);
    EXPECT_EQ(st.pausedOffset, log.end);
    EXPECT_EQ(st.foldCursor, log.end);
}

// A paused view answers ViewPaused (54) through the REAL worker serve path, with the paused offset.
TEST(NsViewMemoryRegression, PausedReadReturnsCodeAndOffsetRegression) {
    Rig rig(/*limit chunks*/ 1);
    SrcReplica srcRep(kSrcA);
    SimLog log;
    log.append(1, static_cast<int>(rig.rowsPerChunk) + 1);   // 2 chunks > 1: over after this fold
    rig.driveA(log, srcRep.gen());
    rig.driveA(log, srcRep.gen());                            // pauses
    ASSERT_TRUE(rig.vs.instanceServeState(kViewA, 0).paused);

    Worker w(0);
    w.setOwnedDirect(Worker::OwnedSet{srcRep.replica.get()});
    w.setViewSet(&rig.vs);
    WorkerOp op;
    op.type = WorkerOp::Type::GetViewRow;
    op.topicId = kViewA; op.partition = 0; op.sessionId = 1; op.correlationId = 42;
    op.nextOffset = 1; op.hwBound = 0;
    ASSERT_TRUE(w.tryEnqueueOp(std::move(op)));
    w.doWork(1'000'000LL);
    WorkerReturn got{};
    int n = 0;
    w.drainReturn([&](WorkerReturn r) { got = std::move(r); ++n; });
    ASSERT_EQ(n, 1);
    EXPECT_EQ(got.type, WorkerReturn::Type::ViewRowResult);
    EXPECT_EQ(got.errorCode, static_cast<int16_t>(pc::ViewRowResponse::kViewPaused));
    EXPECT_EQ(got.errorCode, 54);
    EXPECT_EQ(got.lastOffset, log.end) << "the paused offset";
    EXPECT_TRUE(got.chunk.empty());
}

// Raise the limit: the view resumes from its paused offset, folds the records appended while paused and equals a
// recompute of the whole log.
TEST(NsViewMemoryRegression, ResumeCatchesUpEqualsRecomputeRegression) {
    Rig rig(/*limit chunks*/ 1);
    const int64_t R = rig.rowsPerChunk;
    SimLog log;
    log.append(1, static_cast<int>(R) + 1);        // crosses the limit
    rig.driveA(log);
    rig.driveA(log);                               // pauses at end-of-batch-1
    const int64_t pausedAt = rig.vs.instanceServeState(kViewA, 0).pausedOffset;
    ASSERT_EQ(pausedAt, log.end);
    log.append(1000, 7);                           // appended while paused
    rig.driveA(log);
    EXPECT_EQ(rig.vs.instanceServeState(kViewA, 0).foldCursor, pausedAt) << "a paused view folds nothing";
    WorkerViewSet::RowStatus rs{};
    EXPECT_EQ(rig.row(kViewA, 1000, rs), nullptr);
    EXPECT_EQ(rs, WorkerViewSet::RowStatus::Paused);

    rig.assign(kSlotA, kNsA, 100 * rig.chunkB);    // the limit is raised
    rig.driveA(log);
    EXPECT_FALSE(rig.vs.instanceServeState(kViewA, 0).paused);
    EXPECT_EQ(rig.vs.viewResumesForTest(), 1u);
    EXPECT_EQ(rig.vs.instanceServeState(kViewA, 0).foldCursor, log.end);

    // Recompute: a fresh view set folding the whole log in one go.
    Rig ref(/*unlimited*/ 0);
    SimLog all;
    all.append(1, static_cast<int>(R) + 1);
    all.append(1000, 7);
    // fold batch by batch like the log
    ref.driveA(all);
    ref.driveA(all);
    EXPECT_EQ(ref.vs.instanceServeState(kViewA, 0).foldCursor, all.end);
    int checked = 0;
    for (int64_t k : [&] { std::vector<int64_t> ks; for (int64_t i = 1; i <= R + 1; ++i) ks.push_back(i);
                           for (int64_t i = 1000; i < 1007; ++i) ks.push_back(i); return ks; }()) {
        WorkerViewSet::RowStatus a{}, b{};
        const uint8_t* ra = rig.row(kViewA, k, a);
        const uint8_t* rb = ref.row(kViewA, k, b);
        ASSERT_NE(ra, nullptr) << "key " << k;
        ASSERT_NE(rb, nullptr);
        EXPECT_EQ(std::memcmp(ra, rb, rig.rowLen), 0) << "key " << k;
        ++checked;
    }
    EXPECT_EQ(checked, static_cast<int>(R) + 1 + 7);
}

// A single fold pass covers one batch: at the limit, one batch of N new keys overshoots by at most its own chunks and
// the batch after it is NOT folded (the view pauses first).
TEST(NsViewMemoryRegression, OvershootAtMostOneBatchRegression) {
    Rig rig(/*limit chunks*/ 1);
    const int64_t R = rig.rowsPerChunk;
    SimLog log;
    log.append(1, 1);
    rig.driveA(log);
    ASSERT_EQ(rig.vs.nsViewMemBytes(kSlotA), rig.chunkB);   // At the limit (1 chunk)
    const int batchKeys = static_cast<int>(3 * R);
    log.append(100, batchKeys);                              // one batch of 3R new keys
    log.append(5000, 5);                                     // a second batch, queued behind it
    rig.driveA(log);                                         // folds batch 2 only (one pass = one batch)
    const uint64_t overshoot = rig.vs.nsViewMemBytes(kSlotA) - rig.chunkB;
    EXPECT_LE(overshoot, static_cast<uint64_t>((batchKeys + R - 1) / R) * rig.chunkB) << "<= the chunks of one batch";
    EXPECT_TRUE(rig.vs.nsViewMemOver(kSlotA));
    rig.driveA(log);                                         // paused: batch 3 is not folded
    rig.driveA(log);
    WorkerViewSet::RowStatus rs{};
    EXPECT_EQ(rig.row(kViewA, 5000, rs), nullptr);
    EXPECT_EQ(rs, WorkerViewSet::RowStatus::Paused);
    EXPECT_EQ(rig.vs.instanceServeState(kViewA, 0).foldCursor, 1 + batchKeys);
}

// Namespace A over its limit pauses A's views only; B's view keeps folding and serving.
TEST(NsViewMemoryRegression, OtherNamespaceViewsUnaffectedRegression) {
    Rig rig(/*A limit chunks*/ 1, /*B unlimited*/ 0, /*installB*/ true);
    SimLog a, b;
    a.append(1, static_cast<int>(rig.rowsPerChunk) + 1);
    b.append(1, static_cast<int>(rig.rowsPerChunk) * 3);   // B uses 3 chunks: its own usage is not A's
    rig.driveA(a);
    rig.drive(kSrcB, kSlotB, kNsB, b);
    rig.driveA(a);
    EXPECT_TRUE(rig.vs.instanceServeState(kViewA, 0).paused);
    EXPECT_FALSE(rig.vs.nsViewMemOver(kSlotB));
    EXPECT_EQ(rig.vs.nsViewMemBytes(kSlotB), 3 * rig.chunkB);
    b.append(9000, 3);
    rig.drive(kSrcB, kSlotB, kNsB, b);
    WorkerViewSet::RowStatus rs{};
    EXPECT_NE(rig.row(kViewB, 9000, rs), nullptr);
    EXPECT_EQ(rs, WorkerViewSet::RowStatus::Ok);
    EXPECT_FALSE(rig.vs.instanceServeState(kViewB, 0).paused);
}

// Dropping the paused view releases its memory: the namespace is back under the limit.
TEST(NsViewMemoryRegression, DroppedViewReleasesMemoryRegression) {
    Rig rig(/*limit chunks*/ 1);
    SimLog log;
    log.append(1, static_cast<int>(rig.rowsPerChunk) + 1);
    rig.driveA(log);
    ASSERT_TRUE(rig.vs.nsViewMemOver(kSlotA));
    EXPECT_TRUE(rig.vs.uninstallView(kViewA));
    EXPECT_EQ(rig.vs.nsViewMemBytes(kSlotA), 0u);
    EXPECT_FALSE(rig.vs.nsViewMemOver(kSlotA));
}

// A slot freed and reassigned to another namespace starts from zero usage and is never charged by the old view.
TEST(NsViewMemoryRegression, ReusedSlotStartsFromZeroRegression) {
    Rig rig(/*limit chunks*/ 1);
    SimLog log;
    log.append(1, static_cast<int>(rig.rowsPerChunk) + 1);
    rig.driveA(log);
    ASSERT_GT(rig.vs.nsViewMemBytes(kSlotA), 0u);
    rig.vs.onNsSlotRelease(kSlotA, kNsA);
    EXPECT_EQ(rig.vs.nsViewMemBytes(kSlotA), 0u);
    rig.assign(kSlotA, /*new nsId*/ 99, 1 * rig.chunkB);
    EXPECT_EQ(rig.vs.nsViewMemBytes(kSlotA), 0u);
    EXPECT_FALSE(rig.vs.nsViewMemOver(kSlotA));
    // The old view (tagged kNsA) driving again is a stale tag: counted, never charged to the new namespace.
    const uint64_t before = rig.vs.nsSlotMismatchesForTest();
    log.append(50000, 5);
    rig.driveA(log);
    EXPECT_EQ(rig.vs.nsViewMemBytes(kSlotA), 0u);
    EXPECT_GE(rig.vs.nsSlotMismatchesForTest(), before);
}
