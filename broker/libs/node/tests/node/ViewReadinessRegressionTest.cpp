// libs/node/tests/node/ViewReadinessRegressionTest.cpp
//
// Task 16: restart readiness gate (RECOVERING -> REBUILDING_VIEWS -> READY).
//
// The readiness state machine is LOCAL-ONLY: a (view, partition) instance
// reaches READY purely from its own on-disk checkpoint (loaded via
// rune::view::replay) plus a replay of the committed source log tail — no
// cluster/peer dependency.  These tests are deterministic and in-process: a
// fixed committed-range readRange (no real Log, no Aeron, no wall clock) feeds
// the rebuild fold, and the rebuild is driven under a HARD cap (never
// `while(cond) doWork()`), advancing nowNs by a loop index per transition.
//
// Review-focus #5: a GetViewRow for a view still REBUILDING_VIEWS surfaces a
// RETRIABLE not-ready (nullptr + RowStatus::NotReady), never a half-built row.
#include <unistd.h>
#include <filesystem>
#include <string>
#include <gtest/gtest.h>

#include <cstdint>
#include <cstring>
#include <span>
#include <string_view>
#include <vector>

#include <rune/codec/Descriptor.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/storage/LogReadView.h>
#include <rune/view/PlanCompiler.h>
#include <rune/view/ViewCheckpoint.h>
#include <rune/view/ViewRow.h>

#include <rune/node/ViewSnapshotChannel.h>
#include <rune/node/WorkerViewSet.h>

namespace {

// Per-PROCESS view data dirs "<temp>/<name>-<pid>": ctest -j runs each test as its
// own process (and other checkouts run the same suites), so a fixed /tmp/<name>
// let one process's cleanup wipe another's files.  Removed when the tests finish.
std::vector<std::string>& createdTmpDirs() {
    static std::vector<std::string> dirs;
    return dirs;
}
std::string tmpDir(const std::string& name) {
    std::string d = (std::filesystem::temp_directory_path() /
                     (name + "-" + std::to_string(::getpid()))).string();
    createdTmpDirs().push_back(d);
    return d;
}
class TmpDirCleanup : public ::testing::Environment {
public:
    void TearDown() override {
        std::error_code ec;
        for (const std::string& d : createdTmpDirs()) std::filesystem::remove_all(d, ec);
    }
};
[[maybe_unused]] ::testing::Environment* const kTmpDirCleanup =
    ::testing::AddGlobalTestEnvironment(new TmpDirCleanup);

// v1 "character" source: {characterId: Int64 key (tag 0), hp: Int32 (tag 1)}.
rune::codec::Descriptor charSourceV1() {
    return rune::codec::DescriptorBuilder("character", 1)
        .add("characterId", rune::codec::PrimType::Int64, 1, /*isKey*/ true)
        .add("hp", rune::codec::PrimType::Int32)
        .build();
}

// char_hp(characterId, hp) <- source topicId 5 "characterId"/"hp".
rune::view::ViewPlan hpView(uint8_t persistence = 0) {
    rune::view::ViewPlan v;
    v.viewName    = "char_hp";
    v.key         = "characterId";
    v.sources     = {{5, "characterId"}};
    v.output      = {{"characterId", rune::codec::PrimType::Int64, 0, "characterId",
                     rune::view::MergeRule::Replace},
                   {"hp", rune::codec::PrimType::Int32, 0, "hp", rune::view::MergeRule::Replace}};
    v.persistence = persistence;
    return v;
}

std::vector<uint8_t> keyBytes(int64_t id) {
    std::vector<uint8_t> b(8);
    for (int i = 0; i < 8; ++i) b[static_cast<size_t>(i)] = static_cast<uint8_t>(uint64_t(id) >> (8 * i));
    return b;
}
std::string_view asSv(const std::vector<uint8_t>& b) {
    return std::string_view(reinterpret_cast<const char*>(b.data()), b.size());
}

// [blockLength u16][version u16][id i64][hp i32] — a v1 "character" value.
std::vector<uint8_t> encodeCharacter(int64_t id, int32_t hp, uint16_t version) {
    std::vector<uint8_t> b;
    auto le16 = [&](uint16_t v) { b.push_back(uint8_t(v)); b.push_back(uint8_t(v >> 8)); };
    auto le32 = [&](int32_t v) { for (int i = 0; i < 4; ++i) b.push_back(uint8_t(uint32_t(v) >> (8 * i))); };
    auto le64 = [&](int64_t v) { for (int i = 0; i < 8; ++i) b.push_back(uint8_t(uint64_t(v) >> (8 * i))); };
    le16(8 + 4);
    le16(version);
    le64(id);
    le32(hp);
    return b;
}

// A single committed record batch with `count` upserts for key 7 whose hp
// climbs, spanning committed offsets [base, base+count).  Used both to synth a
// checkpoint (fold it then drain a delta) and to drive the rebuild tail.
std::vector<uint8_t> upsertRun(int64_t key, int32_t hpBase, int count, int64_t baseOffset) {
    rune::protocol::RecordBatchBuilder b(baseOffset, /*baseTimestamp*/ 0);
    const auto k = keyBytes(key);
    for (int i = 0; i < count; ++i) {
        const auto v = encodeCharacter(key, hpBase + i, 1);
        b.append(asSv(k), asSv(v), 0);
    }
    return b.build();
}

// A committed batch of two DISTINCT keys at [baseOffset, baseOffset+2) — used
// to grow a table past a small maxRows ceiling (proving grow-on-demand).
std::vector<uint8_t> twoKeyBatch(int64_t k1, int32_t hp1, int64_t k2, int32_t hp2,
                                 int64_t baseOffset) {
    rune::protocol::RecordBatchBuilder b(baseOffset, /*baseTimestamp*/ 0);
    b.append(asSv(keyBytes(k1)), asSv(encodeCharacter(k1, hp1, 1)), 0);
    b.append(asSv(keyBytes(k2)), asSv(encodeCharacter(k2, hp2, 1)), 0);
    return b.build();
}

// Encode a BASE checkpoint record capturing every live row produced by folding
// `src` over [0, foldTo), at O == foldTo (the exclusive cursor, matching how
// WorkerViewSet persists O == foldCursor).
std::vector<uint8_t> makeBaseCheckpoint(const rune::view::CompiledViewPlan& plan,
                                        const rune::view::RowLayout& layout,
                                        const rune::codec::Descriptor& src,
                                        const std::vector<uint8_t>& srcBatch, int64_t foldTo) {
    rune::view::ViewTable tmp(layout, 16, 1u << 16);
    rune::view::VersionDescriptorCache vdc(src);
    rune::storage::LogReadView v{srcBatch.data(), srcBatch.size(), 0, foldTo};
    int64_t cursor = 0;
    rune::view::foldBatch(plan, layout, vdc, tmp, v, foldTo, cursor);
    std::vector<rune::view::DirtyEntry> entries;
    std::vector<uint8_t> rowScratch;
    std::vector<std::size_t> offs;
    tmp.drainDirty([&](uint32_t rid, const uint8_t* row, bool deleted, uint64_t key) {
        rune::view::DirtyEntry e{rid, deleted, key, nullptr};
        if (!deleted && row != nullptr) {
            offs.push_back(rowScratch.size());
            rowScratch.insert(rowScratch.end(), row, row + layout.rowLen);
        } else {
            offs.push_back(SIZE_MAX);
        }
        entries.push_back(e);
    });
    for (std::size_t i = 0; i < entries.size(); ++i)
        if (offs[i] != SIZE_MAX) entries[i].row = rowScratch.data() + offs[i];
    std::vector<uint8_t> out;
    rune::view::encodeDelta(entries, foldTo, layout.rowLen, out);
    return out;
}

} // namespace

// Review-focus #5 gate: install-for-recovery with checkpointO < committedTail
// => the instance is REBUILDING_VIEWS, getRow returns a RETRIABLE not-ready
// (nullptr + RowStatus::NotReady), and allViewsReady() is false.  Driving the
// rebuild under a HARD cap flips the instance to READY and getRow then serves
// the real (fully-rebuilt) row.
TEST(ViewReadinessRegression, GetViewRowNotReadyIsRetriable) {
    auto src = charSourceV1();
    const rune::codec::Descriptor* srcs[] = {&src};
    auto comp = rune::view::compileView(hpView(0), srcs, 0x40000007).value();

    // Build an on-disk checkpoint state by folding offsets [0,2) (hp climbs to
    // 11) into a throwaway table, then encoding that as a BASE record at O=2.
    const auto baseRec =
        makeBaseCheckpoint(comp.plan, comp.rowLayout, src, upsertRun(7, 10, 2, 0), /*foldTo*/ 2);

    rune::node::ViewSnapshotChannel channel;
    rune::node::WorkerViewSet vs(channel, tmpDir("rune-views-test"));

    // Install-for-recovery: checkpointO=2 (from the base), committedTail=4.
    // The source log tail (2,4] still has to be replayed -> REBUILDING_VIEWS.
    const std::span<const std::span<const uint8_t>> noDeltas{};
    vs.installViewForRecovery(comp.plan, comp.rowLayout, src, /*persisted*/ true,
                              /*partition*/ 0, std::span<const uint8_t>(baseRec), noDeltas,
                              /*committedTail*/ 4);

    EXPECT_EQ(vs.readiness(0x40000007, /*partition*/ 0),
              rune::node::ViewReadiness::RebuildingViews);
    EXPECT_FALSE(vs.allViewsReady());

    // getRow must NOT serve a half-built row while rebuilding — retriable.
    const rune::view::RowLayout* L = nullptr;
    rune::node::WorkerViewSet::RowStatus status = rune::node::WorkerViewSet::RowStatus::Ok;
    EXPECT_EQ(vs.getRow(0x40000007, /*partition*/ 0, 7, L, &status), nullptr);
    EXPECT_EQ(status, rune::node::WorkerViewSet::RowStatus::NotReady);

    // The committed source tail (2,4]: offsets [2,4), hp 30 then 31.
    const auto tail = upsertRun(7, 30, /*count*/ 2, /*baseOffset*/ 2);
    auto readRange = [&](int64_t /*from*/, int64_t /*to*/) -> rune::storage::LogReadView {
        return rune::storage::LogReadView{tail.data(), tail.size(), 2, 4};
    };

    // Drive the rebuild under a HARD cap (never unbounded).
    for (int i = 0; i < 64 && !vs.allViewsReady(); ++i)
        vs.driveRebuild(/*srcTopicId*/ 5, /*partition*/ 0, readRange,
                        /*committedTail*/ 4, /*nowNs*/ i);

    EXPECT_TRUE(vs.allViewsReady());
    EXPECT_EQ(vs.readiness(0x40000007, /*partition*/ 0), rune::node::ViewReadiness::Ready);

    // Now READY: getRow serves the fully-rebuilt latest row (hp=31).
    status = rune::node::WorkerViewSet::RowStatus::NotReady;
    const uint8_t* row = vs.getRow(0x40000007, /*partition*/ 0, 7, L, &status);
    ASSERT_NE(row, nullptr);
    ASSERT_NE(L, nullptr);
    EXPECT_EQ(status, rune::node::WorkerViewSet::RowStatus::Ok);
    EXPECT_EQ(rune::view::readSlot(row, *L, 1).i, 31);
}

// Cold all-node restart: NO peers are wired (this is a bare WorkerViewSet with
// no cluster/consensus dependency at all).  The gate must still reach READY
// purely from the local checkpoint (base+deltas) + a local source-log replay.
// Here the checkpoint is EMPTY (brand-new cold start) but the committed source
// log has offsets [0,3): the instance starts REBUILDING_VIEWS at foldCursor=0
// and reaches READY from local state only.
TEST(ViewReadinessRegression, ColdAllNodeRestartReachesReadyFromLocalStateOnly) {
    auto src = charSourceV1();
    const rune::codec::Descriptor* srcs[] = {&src};
    auto comp = rune::view::compileView(hpView(0), srcs, 0x40000007).value();

    rune::node::ViewSnapshotChannel channel;
    rune::node::WorkerViewSet vs(channel, tmpDir("rune-views-test"));

    // Empty checkpoint (no base, no deltas) -> foldCursor starts at 0.  The
    // committed source tail is [0,3) -> REBUILDING_VIEWS (committedTail=3>0).
    const std::span<const uint8_t> noBase{};
    const std::span<const std::span<const uint8_t>> noDeltas{};
    vs.installViewForRecovery(comp.plan, comp.rowLayout, src, /*persisted*/ true,
                              /*partition*/ 0, noBase, noDeltas, /*committedTail*/ 3);

    EXPECT_EQ(vs.readiness(0x40000007, /*partition*/ 0),
              rune::node::ViewReadiness::RebuildingViews);
    EXPECT_FALSE(vs.allViewsReady());

    const auto log = upsertRun(7, 50, /*count*/ 3, /*baseOffset*/ 0);  // offsets [0,3)
    auto readRange = [&](int64_t, int64_t) -> rune::storage::LogReadView {
        return rune::storage::LogReadView{log.data(), log.size(), 0, 3};
    };

    // No peers, no network, no clock — reach READY under a hard cap from local
    // state only.
    for (int i = 0; i < 64 && !vs.allViewsReady(); ++i)
        vs.driveRebuild(/*srcTopicId*/ 5, /*partition*/ 0, readRange,
                        /*committedTail*/ 3, /*nowNs*/ i);

    EXPECT_TRUE(vs.allViewsReady());
    EXPECT_EQ(vs.readiness(0x40000007, /*partition*/ 0), rune::node::ViewReadiness::Ready);

    const rune::view::RowLayout* L = nullptr;
    const uint8_t* row = vs.getRow(0x40000007, /*partition*/ 0, 7, L, nullptr);
    ASSERT_NE(row, nullptr);
    EXPECT_EQ(rune::view::readSlot(row, *L, 1).i, 52);  // last of hp 50,51,52
}

// A brand-new view whose source is already empty/caught up (committedTail==0)
// reaches READY IMMEDIATELY on install-for-recovery — no rebuild step needed.
TEST(ViewReadinessRegression, EmptySourceIsReadyImmediately) {
    auto src = charSourceV1();
    const rune::codec::Descriptor* srcs[] = {&src};
    auto comp = rune::view::compileView(hpView(0), srcs, 0x40000007).value();

    rune::node::ViewSnapshotChannel channel;
    rune::node::WorkerViewSet vs(channel, tmpDir("rune-views-test"));

    const std::span<const uint8_t> noBase{};
    const std::span<const std::span<const uint8_t>> noDeltas{};
    vs.installViewForRecovery(comp.plan, comp.rowLayout, src, /*persisted*/ true,
                              /*partition*/ 0, noBase, noDeltas, /*committedTail*/ 0);

    EXPECT_EQ(vs.readiness(0x40000007, /*partition*/ 0), rune::node::ViewReadiness::Ready);
    EXPECT_TRUE(vs.allViewsReady());
}

// allViewsReady() is inert (true) for a WorkerViewSet with NO instances —
// the no-views case must never gate serving/elections closed.
TEST(ViewReadinessRegression, NoInstancesIsReady) {
    rune::node::ViewSnapshotChannel channel;
    rune::node::WorkerViewSet vs(channel, tmpDir("rune-views-test"));
    EXPECT_TRUE(vs.allViewsReady());
}

// §15 #24: maxRows is an OBSERVABLE capacity-exceeded EVENT, NOT a fold-stop.
// Crossing the ceiling MUST increment the capacity-exceeded counter AND keep
// folding (the table grows on demand) — it must never refuse the fold or freeze
// the cursor.  Two sequential driveFolds passes with maxRows=1: pass 1 (table
// empty at start) folds key 7 (no event); pass 2 (table already at the ceiling)
// fires the event AND still folds key 8, so the table grows to size 2 (> 1).
TEST(ViewReadinessRegression, CapacityExceededEventDoesNotStopFolding) {
    auto src = charSourceV1();
    const rune::codec::Descriptor* srcs[] = {&src};
    auto comp = rune::view::compileView(hpView(0), srcs, 0x40000007).value();
    comp.plan.sizing.maxRows = 1;  // capacity-bounded view

    rune::node::ViewSnapshotChannel channel;
    rune::node::WorkerViewSet vs(channel, tmpDir("rune-views-test"));
    vs.installView(comp.plan, comp.rowLayout, src, /*persisted*/ true);

    const auto b0 = upsertRun(7, 10, /*count*/ 1, /*baseOffset*/ 0);  // key 7 @ [0,1)
    const auto b1 = upsertRun(8, 20, /*count*/ 1, /*baseOffset*/ 1);  // key 8 @ [1,2)
    auto readRange = [&](int64_t from, int64_t /*to*/) -> rune::storage::LogReadView {
        return from == 0 ? rune::storage::LogReadView{b0.data(), b0.size(), 0, 1}
                         : rune::storage::LogReadView{b1.data(), b1.size(), 1, 2};
    };

    vs.driveFolds(/*srcTopicId*/ 5, /*partition*/ 0, readRange, /*bound*/ 1, 0);
    EXPECT_EQ(vs.capacityExceededCount(), 0u) << "table was empty at pass 1 start";

    vs.driveFolds(/*srcTopicId*/ 5, /*partition*/ 0, readRange, /*bound*/ 2, 0);
    EXPECT_GE(vs.capacityExceededCount(), 1u)
        << "crossing maxRows must FIRE the capacity-exceeded event";

    // The fold was NOT refused — both keys are present (table grew past maxRows).
    const rune::view::RowLayout* L = nullptr;
    EXPECT_NE(vs.getRow(0x40000007, 0, 7, L, nullptr), nullptr);
    EXPECT_NE(vs.getRow(0x40000007, 0, 8, L, nullptr), nullptr)
        << "fold must CONTINUE past maxRows (grow-on-demand), not stop";
}

// *Regression* (coordinator IMPORTANT, Task 16 liveness): a capacity-bounded
// view that RECOVERS already AT its maxRows ceiling must still reach Ready via
// driveRebuild — it must NOT stall in RebuildingViews forever.
//
// Pre-fix (maxRows as a hard fold-STOP): driveRebuild's driveView_ returns
// BEFORE foldBatch whenever table.size() >= maxRows, so foldCursor never
// advances past the recovered checkpoint O, never reaches committedTail, and
// the instance is stuck in RebuildingViews — allViewsReady() is NEVER true
// under the hard cap (the whole node would never become ready/serve/elect).
//
// Post-fix (maxRows downgraded to an observable event): the rebuild folds the
// committed tail regardless, growing the table past maxRows, foldCursor reaches
// committedTail, and the instance flips to Ready — allViewsReady() becomes true.
TEST(ViewReadinessRegression, RecoverAtMaxRowsStillReachesReadyNoStall) {
    auto src = charSourceV1();
    const rune::codec::Descriptor* srcs[] = {&src};
    auto comp = rune::view::compileView(hpView(0), srcs, 0x40000007).value();
    comp.plan.sizing.maxRows = 1;  // recovers AT this ceiling (1 live row)

    // Checkpoint: one live row (key 7) at O=1 — table recovers AT maxRows.
    const auto baseRec =
        makeBaseCheckpoint(comp.plan, comp.rowLayout, src, upsertRun(7, 10, 1, 0), /*foldTo*/ 1);

    rune::node::ViewSnapshotChannel channel;
    rune::node::WorkerViewSet vs(channel, tmpDir("rune-views-test"));

    const std::span<const std::span<const uint8_t>> noDeltas{};
    vs.installViewForRecovery(comp.plan, comp.rowLayout, src, /*persisted*/ true,
                              /*partition*/ 0, std::span<const uint8_t>(baseRec), noDeltas,
                              /*committedTail*/ 3);

    EXPECT_EQ(vs.readiness(0x40000007, 0), rune::node::ViewReadiness::RebuildingViews);
    EXPECT_FALSE(vs.allViewsReady());

    // Committed tail (1,3]: two NEW keys (8,9) that would grow the table OVER
    // maxRows — the exact case the old hard ceiling refused to fold.
    const auto tail = twoKeyBatch(8, 80, 9, 90, /*baseOffset*/ 1);  // offsets [1,3)
    auto readRange = [&](int64_t, int64_t) -> rune::storage::LogReadView {
        return rune::storage::LogReadView{tail.data(), tail.size(), 1, 3};
    };

    // Drive the rebuild under a HARD cap.  Pre-fix this loop exhausts without
    // ever reaching Ready (stall); post-fix it reaches Ready.
    for (int i = 0; i < 64 && !vs.allViewsReady(); ++i)
        vs.driveRebuild(/*srcTopicId*/ 5, /*partition*/ 0, readRange, /*committedTail*/ 3, i);

    EXPECT_TRUE(vs.allViewsReady()) << "a view recovered at maxRows must NOT stall in RebuildingViews";
    EXPECT_EQ(vs.readiness(0x40000007, 0), rune::node::ViewReadiness::Ready);

    // Grow-on-demand: the table grew past maxRows, and the capacity event fired.
    EXPECT_GE(vs.capacityExceededCount(), 1u);
    const rune::view::RowLayout* L = nullptr;
    EXPECT_NE(vs.getRow(0x40000007, 0, 7, L, nullptr), nullptr);
    EXPECT_NE(vs.getRow(0x40000007, 0, 8, L, nullptr), nullptr);
    EXPECT_NE(vs.getRow(0x40000007, 0, 9, L, nullptr), nullptr);
}
