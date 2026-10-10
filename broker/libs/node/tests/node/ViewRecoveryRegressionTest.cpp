// libs/node/tests/node/ViewRecoveryRegressionTest.cpp
//
// Task 19b: BrokerNode restart-recovery wiring for views (worker-side pieces).
//
// These in-process regression tests cover the pieces of the restart-recovery
// path that are deterministically testable WITHOUT a true in-process broker
// restart (the real end-to-end gate is the Task 21 docker e2e:
// define -> produce -> GetViewRow -> restart broker -> GetViewRow recovers):
//
//   1. splitViewCheckpointRecords: the on-disk checkpoint file an IO agent
//      writes is a bare concatenation of Task-12 encodeDelta() records (the
//      8-byte routing header is stripped before the bytes hit the file -- see
//      ViewSnapshotIoAgent::handleDeltaFrame_). Recovery must split that file
//      back into individual records to feed rune::view::replay. This proves
//      the splitter is bounds-safe and reproduces the table byte-for-byte.
//
//   2. RecoverFromCheckpointFileThenServe: the worker-side recovery install
//      path end to end -- a recovery-flagged ViewPlanUpdateMsg drained off the
//      install ring (drainInstalls) registers the plan + marks it for
//      recovery; materializeRecoveryInstancesFor then loads THIS (view,
//      partition)'s checkpoint FILE from disk, installs-for-recovery
//      (RebuildingViews, foldCursor = recovered O), and the duty-cycle drive
//      (driveRebuild) folds the committed tail to Ready -> getRow serves the
//      recovered row.
//
//   3. DutyCycleOrderingReachesReadyAndServes: the exact call ordering the
//      worker duty cycle (Worker::driveViews_) now uses for a recovery view --
//      materialize, then driveRebuild (flip to Ready), then driveFolds (steady
//      state). Proves the serve-guard holds until Ready and that the ordering
//      never bypasses the checkpoint with a fresh Ready instance.
#include <gtest/gtest.h>

#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <span>
#include <string_view>
#include <vector>

#include <rune/codec/Descriptor.h>
#include <rune/codec/Serialize.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/storage/LogReadView.h>
#include <rune/view/PlanCompiler.h>
#include <rune/view/ViewCheckpoint.h>
#include <rune/view/ViewPlan.h>
#include <rune/view/ViewRow.h>

#include <rune/node/AdminChannels.h>
#include <rune/node/ViewSnapshotChannel.h>
#include <rune/node/WorkerViewSet.h>

namespace {

constexpr int32_t kViewId  = 0x40000007;
constexpr int32_t kSrcId   = 5;

rune::codec::Descriptor charSourceV1() {
    return rune::codec::DescriptorBuilder("character", 1)
        .add("characterId", rune::codec::PrimType::Int64, 1, /*isKey*/ true)
        .add("hp", rune::codec::PrimType::Int32)
        .build();
}

rune::view::ViewPlan hpView(uint8_t persistence = 0) {
    rune::view::ViewPlan v;
    v.viewName    = "char_hp";
    v.key         = "characterId";
    v.sources     = {{kSrcId, "characterId"}};
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
std::vector<uint8_t> upsertRun(int64_t key, int32_t hpBase, int count, int64_t baseOffset) {
    rune::protocol::RecordBatchBuilder b(baseOffset, /*baseTimestamp*/ 0);
    const auto k = keyBytes(key);
    for (int i = 0; i < count; ++i) {
        const auto v = encodeCharacter(key, hpBase + i, 1);
        b.append(asSv(k), asSv(v), 0);
    }
    return b.build();
}

// Encode a BASE checkpoint record capturing every live row produced by folding
// `src` over [0, foldTo), at O == foldTo -- mirrors ViewReadinessRegressionTest.
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

// Push a recovery-flagged ViewPlanUpdateMsg onto `channel.install` exactly as
// BrokerNode::fanoutViewPlan_ would for the startup-load loop.
void pushRecoveryInstall(rune::node::ViewSnapshotChannel& channel,
                         const rune::view::CompiledViewPlan& plan,
                         const rune::codec::Descriptor& outDesc,
                         const rune::codec::Descriptor& srcDesc, uint8_t persistence) {
    rune::node::ViewPlanUpdateMsg m;
    m.viewTopicId = plan.viewTopicId;
    m.persistence = persistence;
    m.recovery    = true;
    m.planBlob    = rune::view::serialize(plan);
    m.outDescBlob = rune::codec::serialize(outDesc);
    m.srcDescBlob = rune::codec::serialize(srcDesc);
    ASSERT_TRUE(rune::node::encodeViewPlanUpdate(
        channel.install, rune::node::ViewSnapshotChannel::kInstallMsgType, m));
}

std::string uniqueDir(const char* stem) {
    auto p = std::filesystem::temp_directory_path() /
             (std::string(stem) + std::to_string(::getpid()) + "-" +
              std::to_string(reinterpret_cast<uintptr_t>(stem)));
    std::filesystem::create_directories(p);
    return p.string();
}

} // namespace

// (1) The splitter walks a concatenation of encodeDelta() records (base +
// deltas) back into individual record spans, bounds-safely, and replay()
// applied to those spans reproduces the live table.
TEST(ViewRecoveryRegression, SplitCheckpointRecordsRoundTrips) {
    auto src = charSourceV1();
    const rune::codec::Descriptor* srcs[] = {&src};
    auto comp = rune::view::compileView(hpView(0), srcs, kViewId).value();

    // Base at O=2 (key 7, hp climbs to 11), then a DELTA at O=4 for the same key.
    const auto base  = makeBaseCheckpoint(comp.plan, comp.rowLayout, src, upsertRun(7, 10, 2, 0), 2);
    // Build the delta the same way the worker would: fold the tail (2,4] and
    // encode the dirtied row at O=4.
    const auto deltaRec =
        makeBaseCheckpoint(comp.plan, comp.rowLayout, src, upsertRun(7, 30, 2, 0), 4);

    // The on-disk file is the bare concatenation (no routing header).
    std::vector<uint8_t> file;
    file.insert(file.end(), base.begin(), base.end());
    file.insert(file.end(), deltaRec.begin(), deltaRec.end());

    const std::vector<std::span<const uint8_t>> recs =
        rune::node::splitViewCheckpointRecords(file);
    ASSERT_EQ(recs.size(), 2u);
    EXPECT_EQ(recs[0].size(), base.size());
    EXPECT_EQ(recs[1].size(), deltaRec.size());

    rune::view::ViewTable table(comp.rowLayout, 16, 1u << 16);
    const int64_t O = rune::view::replay(table, comp.rowLayout, {}, recs);
    EXPECT_EQ(O, 4);
    const uint8_t* row = table.get(7);
    ASSERT_NE(row, nullptr);
    EXPECT_EQ(rune::view::readSlot(row, comp.rowLayout, 1).i, 31);  // hp 30,31 -> last 31

    // Bounds safety: a truncated tail is dropped (splitter stops before it).
    std::vector<uint8_t> torn = file;
    torn.pop_back();  // chop the last byte of the trailing record's CRC
    const auto recs2 = rune::node::splitViewCheckpointRecords(torn);
    EXPECT_EQ(recs2.size(), 1u) << "a torn trailing record must be dropped, not over-read";

    // Early magic reject: a non-record / misaligned head yields zero records.
    std::vector<uint8_t> garbage(64, 0xAB);
    EXPECT_TRUE(rune::node::splitViewCheckpointRecords(garbage).empty())
        << "a bad magic must reject early, never walk garbage lengths";
}

// (2) End-to-end worker recovery install: a recovery-flagged install off the
// ring + a checkpoint FILE on disk -> RebuildingViews (serve-guarded) ->
// driveRebuild -> Ready -> getRow serves the recovered row.
TEST(ViewRecoveryRegression, RecoverFromCheckpointFileThenServe) {
    auto src = charSourceV1();
    const rune::codec::Descriptor* srcs[] = {&src};
    auto comp = rune::view::compileView(hpView(0), srcs, kViewId).value();

    const std::string dir = uniqueDir("rune-view-recov-");

    // Write THIS (view, partition)'s checkpoint file: a single base at O=2.
    const auto base = makeBaseCheckpoint(comp.plan, comp.rowLayout, src, upsertRun(7, 10, 2, 0), 2);
    const std::string path =
        dir + "/view-" + std::to_string(static_cast<uint32_t>(kViewId)) + "-p0.delta";
    { std::ofstream f(path, std::ios::binary); f.write(reinterpret_cast<const char*>(base.data()),
                                                       static_cast<std::streamsize>(base.size())); }

    rune::node::ViewSnapshotChannel channel;
    rune::node::WorkerViewSet vs(channel, dir);

    // Recovery-flagged install off the ring (as the startup-load fanout sends it).
    pushRecoveryInstall(channel, comp.plan, comp.outputDescriptor, src, /*persistence*/ 0);
    EXPECT_EQ(vs.drainInstalls(), 1);
    EXPECT_TRUE(vs.hasRecovery());
    EXPECT_EQ(vs.instanceCount(), 0u) << "recovery install registers the template, not the instance";

    // Materialize THIS owned (src, partition) for recovery -- loads the file.
    vs.materializeRecoveryInstancesFor(kSrcId, /*partition*/ 0, /*committedTail*/ 4);
    EXPECT_EQ(vs.instanceCount(), 1u);
    EXPECT_EQ(vs.readiness(kViewId, 0), rune::node::ViewReadiness::RebuildingViews);
    EXPECT_FALSE(vs.allViewsReady());

    // Serve-guard: still rebuilding -> retriable not-ready, never a half row.
    const rune::view::RowLayout* L = nullptr;
    rune::node::WorkerViewSet::RowStatus status = rune::node::WorkerViewSet::RowStatus::Ok;
    EXPECT_EQ(vs.getRow(kViewId, 0, 7, L, &status), nullptr);
    EXPECT_EQ(status, rune::node::WorkerViewSet::RowStatus::NotReady);

    // Drive the committed tail (2,4] under a HARD cap -> Ready.
    const auto tail = upsertRun(7, 30, 2, /*baseOffset*/ 2);  // offsets [2,4), hp 30,31
    auto readRange = [&](int64_t, int64_t) -> rune::storage::LogReadView {
        return rune::storage::LogReadView{tail.data(), tail.size(), 2, 4};
    };
    EXPECT_TRUE(vs.hasRecovery()) << "still recovering: the extra duty-cycle work is live";
    for (int i = 0; i < 64 && !vs.allViewsReady(); ++i)
        vs.driveRebuild(kSrcId, 0, readRange, /*committedTail*/ 4, i);

    EXPECT_TRUE(vs.allViewsReady());
    EXPECT_EQ(vs.readiness(kViewId, 0), rune::node::ViewReadiness::Ready);
    // Prune: recovery complete -> hasRecovery() latches back to false so
    // driveViews_ drops the extra materialize/driveRebuild lookups.
    EXPECT_FALSE(vs.hasRecovery())
        << "recoveryViews_ must be pruned once every instance of the view is Ready";

    status = rune::node::WorkerViewSet::RowStatus::NotReady;
    const uint8_t* row = vs.getRow(kViewId, 0, 7, L, &status);
    ASSERT_NE(row, nullptr) << "pruning must NOT lose the view — it still serves";
    EXPECT_EQ(status, rune::node::WorkerViewSet::RowStatus::Ok);
    EXPECT_EQ(rune::view::readSlot(row, *L, 1).i, 31);  // recovered from ckpt + tail

    std::error_code ec;
    std::filesystem::remove_all(dir, ec);
}

// (3) The exact worker duty-cycle ordering for a recovery view: materialize ->
// driveRebuild -> driveFolds. The serve-guard must hold until Ready, and the
// ordering must never materialize a fresh Ready instance that bypasses the
// checkpoint (which a driveFolds-first ordering would).
TEST(ViewRecoveryRegression, DutyCycleOrderingReachesReadyAndServes) {
    auto src = charSourceV1();
    const rune::codec::Descriptor* srcs[] = {&src};
    auto comp = rune::view::compileView(hpView(0), srcs, kViewId).value();

    const std::string dir = uniqueDir("rune-view-duty-");
    // Empty checkpoint (cold start): full rebuild from the log [0,3).
    rune::node::ViewSnapshotChannel channel;
    rune::node::WorkerViewSet vs(channel, dir);
    pushRecoveryInstall(channel, comp.plan, comp.outputDescriptor, src, /*persistence*/ 0);
    ASSERT_EQ(vs.drainInstalls(), 1);

    const auto log = upsertRun(7, 50, 3, /*baseOffset*/ 0);  // offsets [0,3)
    auto readRange = [&](int64_t, int64_t) -> rune::storage::LogReadView {
        return rune::storage::LogReadView{log.data(), log.size(), 0, 3};
    };

    // Simulate driveViews_ for owned (src=5, partition=0), bound (LSO) = 3.
    const rune::view::RowLayout* L = nullptr;
    rune::node::WorkerViewSet::RowStatus status = rune::node::WorkerViewSet::RowStatus::Ok;
    bool everReady = false;
    for (int i = 0; i < 64 && !everReady; ++i) {
        vs.materializeRecoveryInstancesFor(kSrcId, 0, /*committedTail*/ 3);
        // Serve-guard holds until the flip: before the first drive the instance
        // exists but is RebuildingViews.
        if (i == 0) {
            EXPECT_EQ(vs.readiness(kViewId, 0), rune::node::ViewReadiness::RebuildingViews);
            EXPECT_EQ(vs.getRow(kViewId, 0, 7, L, &status), nullptr);
            EXPECT_EQ(status, rune::node::WorkerViewSet::RowStatus::NotReady);
        }
        vs.driveRebuild(kSrcId, 0, readRange, /*committedTail*/ 3, i);
        vs.driveFolds(kSrcId, 0, readRange, /*bound*/ 3, i);
        everReady = vs.allViewsReady();
    }
    EXPECT_TRUE(everReady);

    status = rune::node::WorkerViewSet::RowStatus::NotReady;
    const uint8_t* row = vs.getRow(kViewId, 0, 7, L, &status);
    ASSERT_NE(row, nullptr);
    EXPECT_EQ(status, rune::node::WorkerViewSet::RowStatus::Ok);
    EXPECT_EQ(rune::view::readSlot(row, *L, 1).i, 52);  // hp 50,51,52 -> last 52

    std::error_code ec;
    std::filesystem::remove_all(dir, ec);
}
