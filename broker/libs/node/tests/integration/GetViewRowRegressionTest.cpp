// ---------------------------------------------------------------------------
// GetViewRowRegressionTest.cpp — Task 20 (view engine phase 1): the GetViewRow
// point-read path + the openView fingerprint handshake.
//
// Deterministic + in-process (no Aeron / media driver), mirroring the Task 19
// harness style:
//
//   * GetViewRow worker path — a real Worker driven directly through its SPSC
//     ingress/return rings (the SAME op/return plumbing as PushRead): a view is
//     installed + folded into the worker's own WorkerViewSet, a GetViewRow
//     WorkerOp is enqueued, Worker::doWork drains it (executeGetViewRowWire_),
//     and the ViewRowResult is drained back off the return ring.  Asserts the
//     typed status mapping (OK / AbsentKey) and the round-tripped row bytes.
//
//   * openView handshake — a real AdminWorker driving __topic_schemas + __views
//     (single-node replicas, NullTransport): DefineTopic a source, CreateView
//     over it (which registers the view's typed output topic), then openView by
//     name.  A STALE pinned fingerprint is rejected with SchemaMismatch up front.
//
// Tests:
//   GetViewRowAbsentKeyReturnsTypedAbsent   — Ready view, unknown key -> AbsentKey
//   GetViewRowReturnsCurrentRowAfterFold    — fold a row, GetViewRow -> OK + bytes
//   OpenViewFingerprintMismatchRejected     — stale fingerprint -> SchemaMismatch
// ---------------------------------------------------------------------------

#include <unistd.h>
#include <gtest/gtest.h>
#include "support/CatalogDdlRig.h"

#include <rune/node/Worker.h>
#include <rune/node/WorkerOp.h>
#include <rune/node/WorkerPool.h>
#include <rune/node/WorkerViewSet.h>
#include <rune/node/ReadLeaseFanout.h>
#include <rune/consensus/ReadLease.h>
#include <rune/node/ViewSnapshotChannel.h>
#include <rune/metadata/ShardRouter.h>

#include <rune/node/AdminWorker.h>
#include <rune/node/AdminChannels.h>
#include <rune/node/SchemaRegistry.h>
#include <rune/node/ViewRegistry.h>

#include <rune/replication/PartitionReplica.h>
#include <rune/replication/Types.h>
#include <rune/storage/Log.h>
#include <rune/storage/LogReadView.h>
#include <rune/storage/InMemoryLog.h>

#include <rune/view/PlanCompiler.h>
#include <rune/view/ViewPlan.h>
#include <rune/view/ViewRow.h>
#include <rune/view/ViewTypes.h>

#include <rune/codec/Descriptor.h>
#include <rune/codec/Serialize.h>

#include <rune/protocol/RecordBatch.h>
#include <rune/protocol/client/DataMessages.h>

#include <atomic>
#include <deque>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

using namespace rune::node;
using namespace rune::codec;
using namespace rune::replication;
namespace rv = rune::view;
namespace pc = rune::protocol::client;

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

constexpr int32_t kViewTopicId = 0x40000007;
constexpr int32_t kSrcTopicId  = 5;

// v1 "character" source: {characterId: Int64 key (tag 0), hp: Int32 (tag 1)}.
Descriptor charSourceV1() {
    return DescriptorBuilder("character", 1)
        .add("characterId", PrimType::Int64, 1, /*isKey*/ true)
        .add("hp", PrimType::Int32)
        .build();
}

// char_hp(characterId, hp) <- source topicId 5.
rv::ViewPlan hpView(uint8_t persistence = 1 /*transient: no checkpoint IO in this test*/) {
    rv::ViewPlan v;
    v.viewName    = "char_hp";
    v.key         = "characterId";
    v.sources     = {{kSrcTopicId, "characterId"}};
    v.output      = {{"characterId", PrimType::Int64, 0, "characterId", rv::MergeRule::Replace},
                     {"hp", PrimType::Int32, 0, "hp", rv::MergeRule::Replace}};
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

// One upsert (key, hp) in a single-record batch at offset [0,1).
std::vector<uint8_t> oneUpsert(int64_t key, int32_t hp) {
    rune::protocol::RecordBatchBuilder b(/*baseOffset*/ 0, /*baseTimestamp*/ 0);
    const auto k = keyBytes(key);
    const auto v = encodeCharacter(key, hp, 1);
    b.append(asSv(k), asSv(v), 0);
    return b.build();
}

// Task 22 fix round 1 (C1): a view row is served only by the LEADER of its source
// partition, so every Worker here owns the source partition's replica.  A
// memory-backed single-node replica promoted through becomeLeader (a real
// leadership generation, >= 1, exactly as production).
struct SrcReplica {
    rune::storage::InMemoryLog            log{/*maxBytes=*/1 << 20, /*maxRecords=*/1000};
    rune::replication::InMemoryTransport  transport;
    std::unique_ptr<PartitionReplica>     replica;

    explicit SrcReplica(int32_t topicId = kSrcTopicId, int32_t partition = 0) {
        PartitionConfig cfg{};
        cfg.topicId             = topicId;
        cfg.partitionId         = static_cast<rune::replication::PartitionId>(partition);
        cfg.selfId              = 1;
        cfg.minIsr              = 1;
        cfg.replicaLagMaxMs     = 10'000;
        cfg.replicaLagMaxOffset = 10'000;
        cfg.fetchIntervalNs     = 1'000'000LL;
        cfg.isrCheckIntervalNs  = 1'000'000LL;
        cfg.maxFetchBytes       = 1 << 20;
        cfg.pushOnAppend        = false;
        replica = std::make_unique<PartitionReplica>(cfg, rune::replication::Role::Follower, /*epoch=*/0,
                                                     std::vector<rune::replication::NodeId>{1},
                                                     std::vector<rune::replication::NodeId>{1}, &log, &transport);
        replica->becomeLeader(/*epoch=*/1, {1});
    }
    [[nodiscard]] uint64_t gen() const { return replica->leadershipGeneration(); }
};

// Enqueue a GetViewRow op, drive one worker duty cycle, and drain the single
// ViewRowResult back off the return ring.
WorkerReturn runGetViewRow(Worker& w, int32_t viewTopicId, int32_t partition,
                           uint64_t key, uint64_t requestedFingerprint) {
    WorkerOp op;
    op.type          = WorkerOp::Type::GetViewRow;
    op.topicId       = viewTopicId;                       // viewTopicId (routing + handler)
    op.partition     = partition;
    op.sessionId     = 1;
    op.correlationId = 42;
    op.nextOffset    = static_cast<int64_t>(key);                   // keyU64 (reused field)
    op.hwBound       = static_cast<int64_t>(requestedFingerprint);  // requestedFingerprint (reused field)
    EXPECT_TRUE(w.tryEnqueueOp(std::move(op))) << "GetViewRow enqueue failed (ring full)";

    w.doWork(/*nowNs*/ 1'000'000LL);

    WorkerReturn captured{};
    int count = 0;
    w.drainReturn([&](WorkerReturn r) { captured = std::move(r); ++count; });
    EXPECT_EQ(count, 1) << "exactly one ViewRowResult expected";
    return captured;
}

} // namespace

// ---------------------------------------------------------------------------
// A Ready view that holds key 7: a GetViewRow for an UNKNOWN key answers a
// typed AbsentKey (status 1), never a zero row (empty chunk).
// ---------------------------------------------------------------------------
TEST(GetViewRowRegression, GetViewRowAbsentKeyReturnsTypedAbsent) {
    auto src = charSourceV1();
    const Descriptor* srcs[] = {&src};
    auto comp = rv::compileView(hpView(), srcs, kViewTopicId).value();

    ViewSnapshotChannel channel;
    WorkerViewSet vs(channel, tmpDir("rune-views-gvr-absent"));
    vs.installView(comp.plan, comp.rowLayout, src, /*persisted*/ false);

    // Fold one row (key 7) so the (view, partition 0) instance exists + is Ready.
    const auto batch = oneUpsert(7, 50);
    auto readRange = [&](int64_t, int64_t) -> rune::storage::LogReadView {
        return rune::storage::LogReadView{batch.data(), batch.size(), 0, 1};
    };
    SrcReplica srcRep;
    vs.driveFolds(kSrcTopicId, /*partition*/ 0, readRange, /*bound*/ 1, 0, nullptr, srcRep.gen());

    Worker w(/*workerId*/ 0);
    w.setOwnedDirect(Worker::OwnedSet{srcRep.replica.get()});
    w.setViewSet(&vs);

    const WorkerReturn ret = runGetViewRow(w, kViewTopicId, /*partition*/ 0,
                                           /*key*/ 424242, /*fp*/ 0);
    EXPECT_EQ(ret.type, WorkerReturn::Type::ViewRowResult);
    EXPECT_EQ(ret.errorCode, static_cast<int16_t>(pc::ViewRowResponse::kAbsentKey));
    EXPECT_TRUE(ret.chunk.empty()) << "AbsentKey must carry no row bytes (not a zero row)";
    EXPECT_EQ(ret.recordCount, 0) << "rowLen must be 0 when status != OK";
}

// ---------------------------------------------------------------------------
// Fold a row (key 7, hp 50) into the view; GetViewRow for key 7 answers OK and
// the row bytes decode back to hp=50.
// ---------------------------------------------------------------------------
TEST(GetViewRowRegression, GetViewRowReturnsCurrentRowAfterFold) {
    auto src = charSourceV1();
    const Descriptor* srcs[] = {&src};
    auto comp = rv::compileView(hpView(), srcs, kViewTopicId).value();

    ViewSnapshotChannel channel;
    WorkerViewSet vs(channel, tmpDir("rune-views-gvr-ok"));
    vs.installView(comp.plan, comp.rowLayout, src, /*persisted*/ false);

    const auto batch = oneUpsert(7, 50);
    auto readRange = [&](int64_t, int64_t) -> rune::storage::LogReadView {
        return rune::storage::LogReadView{batch.data(), batch.size(), 0, 1};
    };
    SrcReplica srcRep;
    vs.driveFolds(kSrcTopicId, /*partition*/ 0, readRange, /*bound*/ 1, 0, nullptr, srcRep.gen());

    Worker w(/*workerId*/ 0);
    w.setOwnedDirect(Worker::OwnedSet{srcRep.replica.get()});
    w.setViewSet(&vs);

    const WorkerReturn ret = runGetViewRow(w, kViewTopicId, /*partition*/ 0,
                                           /*key*/ 7, /*fp*/ 0);
    EXPECT_EQ(ret.type, WorkerReturn::Type::ViewRowResult);
    ASSERT_EQ(ret.errorCode, static_cast<int16_t>(pc::ViewRowResponse::kOk));
    ASSERT_EQ(ret.chunk.size(), comp.rowLayout.rowLen)
        << "OK row bytes must be exactly rowLen";
    EXPECT_EQ(ret.recordCount, static_cast<int32_t>(comp.rowLayout.rowLen))
        << "recordCount carries rowLen (informational)";
    // Decode hp (slot 1) straight out of the returned row bytes.
    EXPECT_EQ(rv::readSlot(ret.chunk.data(), comp.rowLayout, 1).i, 50);
}

// ---------------------------------------------------------------------------
// REGRESSION (shard duty-cycle install drain). The live broker runs the SHARD
// worker pass (Worker::doWorkShard_).  Before the fix that pass drove view
// folds but NEVER drained the control->worker view-plan install ring, which
// only the classic doWork_ pass did.  A view plan fanned out by
// BrokerNode::pumpViewFanout_ then sat undrained: templates_ stayed empty,
// viewOutputFingerprint() returned false, and every GetViewRow answered
// UnknownView forever on the live sharded broker.  The in-process tests all
// pre-seed installView() DIRECTLY (bypassing the ring), so they missed it; the
// first real docker/view-engine-e2e.sh caught it.
//
// This drives the install through the ring (exactly as pumpViewFanout_ does)
// and ticks ONE shard duty-cycle pass.
//   RED  (pre-fix):  viewCount()==0 — the shard pass never drained the ring.
//   GREEN (post-fix): viewCount()==1 — doWorkShard_ drains it, like doWork_.
// ---------------------------------------------------------------------------
TEST(GetViewRowRegression, ShardDutyCyclePassDrainsViewInstallRing) {
    auto src = charSourceV1();
    const Descriptor* srcs[] = {&src};
    auto comp = rv::compileView(hpView(), srcs, kViewTopicId).value();

    ViewSnapshotChannel channel;
    WorkerViewSet vs(channel, tmpDir("rune-views-shard-install"));

    // Seed the install ring EXACTLY as BrokerNode::pumpViewFanout_ does.
    ViewPlanUpdateMsg vp;
    vp.viewTopicId = kViewTopicId;
    vp.persistence = 1;       // transient — install is persistence-independent
    vp.recovery    = false;   // plain create-time install
    vp.planBlob    = rv::serialize(comp.plan);
    vp.outDescBlob = rune::codec::serialize(comp.outputDescriptor);
    vp.srcDescBlob = rune::codec::serialize(src);
    ASSERT_TRUE(encodeViewPlanUpdate(channel.install,
                                     ViewSnapshotChannel::kInstallMsgType, vp))
        << "precondition: install frame must encode into the ring";

    Worker w(/*workerId*/ 0);
    w.setShardMode(true);     // the duty-cycle pass the live sharded broker runs
    w.setViewSet(&vs);
    ASSERT_EQ(vs.viewCount(), 0u) << "precondition: nothing installed yet";

    w.doWork(/*nowNs*/ 1'000'000LL);   // one shard duty-cycle pass

    EXPECT_EQ(vs.viewCount(), 1u)
        << "doWorkShard_ must drain the view-plan install ring (else UnknownView)";
    uint64_t fp = 0;
    EXPECT_TRUE(vs.viewOutputFingerprint(kViewTopicId, fp))
        << "installed view's output fingerprint must resolve";
    EXPECT_EQ(fp, comp.plan.outputFingerprint);
}

// ---------------------------------------------------------------------------
// BINDING (catalog history Task 12 / Task 8 review N5): a view's plan posted to
// the worker's install ring BEFORE a GetViewRow op is answered as the installed
// view — never the permanent UnknownView.  The pass drains ops before installs,
// so the worker drains the install ring once before answering "no plan".
//   RED: UnknownView (4) — the op ran in the same pass, ahead of the install.
// Both duty-cycle passes (classic doWork_ and the shard pass).
// ---------------------------------------------------------------------------
TEST(GetViewRowRegression, PlanOnTheInstallRingAheadOfTheReadIsNeverUnknownViewRegression) {
    for (const bool shard : {false, true}) {
        SCOPED_TRACE(shard ? "shard pass" : "classic pass");
        auto src = charSourceV1();
        const Descriptor* srcs[] = {&src};
        auto comp = rv::compileView(hpView(), srcs, kViewTopicId).value();

        ViewSnapshotChannel channel;
        WorkerViewSet vs(channel, tmpDir(shard ? "rune-views-n5-shard" : "rune-views-n5-classic"));
        ViewPlanUpdateMsg vp;
        vp.viewTopicId = kViewTopicId;
        vp.persistence = 1;
        vp.planBlob    = rv::serialize(comp.plan);
        vp.outDescBlob = rune::codec::serialize(comp.outputDescriptor);
        vp.srcDescBlob = rune::codec::serialize(src);
        ASSERT_TRUE(encodeViewPlanUpdate(channel.install, ViewSnapshotChannel::kInstallMsgType, vp));

        SrcReplica srcRep;   // C1: the read reaches the plan check only on the source leader
        Worker w(/*workerId*/ 0);
        w.setShardMode(shard);
        w.setOwnedDirect(Worker::OwnedSet{srcRep.replica.get()});
        w.setViewSet(&vs);
        ASSERT_EQ(vs.viewCount(), 0u) << "precondition: the plan is still on the ring";
        const WorkerReturn ret = runGetViewRow(w, kViewTopicId, 0, /*key=*/7, /*fp=*/0);
        EXPECT_NE(ret.errorCode, static_cast<int16_t>(pc::ViewRowResponse::kUnknownView))
            << "a view whose plan is on the ring is not unknown";
        EXPECT_TRUE(ret.errorCode == static_cast<int16_t>(pc::ViewRowResponse::kAbsentKey) ||
                    ret.errorCode == static_cast<int16_t>(pc::ViewRowResponse::kNotReady))
            << ret.errorCode;
        EXPECT_EQ(vs.viewCount(), 1u);
    }
}

// ---------------------------------------------------------------------------
// MULTI-WORKER routing (IMPORTANT 1): the view-partition table for
// (viewTopicId, p) is folded on the worker that owns the SOURCE partition p
// (driveViews_ iterates owned source replicas).  A GetViewRow is routed by
// viewTopicId through WorkerPool::tryEnqueueOp, so on a >1-worker broker it must
// land on that same source-owner — else it hits a worker that folded nothing and
// answers a SILENT AbsentKey for a live key.  WorkerPool::coLocateViewWithSource
// pins the view's shard group to the source's group so routing agrees.
//
// RED (no co-location): routing by viewTopicId lands on a DIFFERENT worker than
//   the source owner -> AbsentKey.
// GREEN (routed by the source topic): routes to the source owner -> OK + row.
// ---------------------------------------------------------------------------
TEST(GetViewRowRegression, MultiWorkerRoutesToSourceOwnerViaCoLocation) {
    namespace rm = rune::metadata;
    constexpr int32_t kWorkers = 2;

    // Pick a viewTopicId whose NAIVE (singleton-group) partition-0 owner differs
    // from the source owner — the exact mis-route the fix prevents.
    rm::ShardRouter probe(kWorkers);
    const int32_t srcOwner = probe.workerForPartition(kSrcTopicId, 0);
    int32_t viewTopicId = 0x40000010;
    while (probe.workerForPartition(viewTopicId, 0) == srcOwner) ++viewTopicId;
    ASSERT_NE(probe.workerForPartition(viewTopicId, 0), srcOwner)
        << "need a view id that naively mis-routes to prove the fix";

    auto src = charSourceV1();
    const Descriptor* srcs[] = {&src};
    auto comp = rv::compileView(hpView(), srcs, viewTopicId).value();

    WorkerPool pool(kWorkers, rm::ShardRouter(kWorkers));

    // Fan-out installs the TEMPLATE on every worker; only the source-owner folds
    // a row for (viewTopicId, partition 0) — exactly as production driveViews_.
    ViewSnapshotChannel ch0, ch1;
    WorkerViewSet vs0(ch0, tmpDir("rune-views-gvr-mw0"));
    WorkerViewSet vs1(ch1, tmpDir("rune-views-gvr-mw1"));
    vs0.installView(comp.plan, comp.rowLayout, src, /*persisted*/ false);
    vs1.installView(comp.plan, comp.rowLayout, src, /*persisted*/ false);
    WorkerViewSet* vsByWorker[kWorkers] = {&vs0, &vs1};

    const auto batch = oneUpsert(7, 50);
    auto readRange = [&](int64_t, int64_t) -> rune::storage::LogReadView {
        return rune::storage::LogReadView{batch.data(), batch.size(), 0, 1};
    };
    SrcReplica srcRep;
    vsByWorker[srcOwner]->driveFolds(kSrcTopicId, /*partition*/ 0, readRange, /*bound*/ 1, 0, nullptr,
                                     srcRep.gen());

    pool.worker(srcOwner).setOwnedDirect(Worker::OwnedSet{srcRep.replica.get()});
    pool.worker(0).setViewSet(&vs0);
    pool.worker(1).setViewSet(&vs1);

    int32_t routeTopic = viewTopicId;   // the pre-fix routing key; the fix routes by the source
    auto query = [&]() -> WorkerReturn {
        WorkerOp op;
        op.type          = WorkerOp::Type::GetViewRow;
        op.topicId       = viewTopicId;
        op.partition     = 0;
        op.sessionId     = 1;
        op.correlationId = 99;
        op.nextOffset    = 7;   // keyU64
        op.hwBound       = 0;   // requestedFingerprint = latest
        EXPECT_TRUE(pool.tryEnqueueOp(routeTopic, /*partition*/ 0, std::move(op)));
        for (int32_t i = 0; i < kWorkers; ++i) pool.worker(i).doWork(1'000'000LL);
        WorkerReturn captured{};
        int n = 0;
        pool.drainAllReturns([&](WorkerReturn r) { captured = std::move(r); ++n; });
        EXPECT_EQ(n, 1);
        return captured;
    };

    // RED: without co-location, viewTopicId routes to the wrong worker.
    const WorkerReturn pre = query();
    // C1 (Task 22 fix round 1): a worker that does not lead the source partition no longer
    // answers a SILENT AbsentKey for a live key — it answers the not-leader sentinel.
    EXPECT_EQ(pre.errorCode, kViewRowNotLeaderInternal)
        << "a GetViewRow mis-routed to a non-source-owner worker is never a silent AbsentKey";

    // GREEN: co-locate the view's shard group with its source; routing agrees.
    routeTopic = kSrcTopicId;   // VLS B.2: the gateway routes by the SOURCE topic (the sink owns no router entry)
    ASSERT_EQ(pool.ownerWorkerId(routeTopic, 0), srcOwner) << "the source topic routes to the source owner";
    const WorkerReturn post = query();
    ASSERT_EQ(post.errorCode, static_cast<int16_t>(pc::ViewRowResponse::kOk))
        << "post-fix: GetViewRow lands on the source owner (which folded the row)";
    ASSERT_EQ(post.chunk.size(), comp.rowLayout.rowLen);
    EXPECT_EQ(rv::readSlot(post.chunk.data(), comp.rowLayout, 1).i, 50);
}

// ---------------------------------------------------------------------------
// BINDING (Task 22 fix round 1, C1, ex-leader): the source partition's leadership
// moved away.  The worker's view table is frozen at the old leadership's last
// fold, but the broker is now a FOLLOWER of the source — it must answer the
// not-leader sentinel (the gateway redirects), never the stale row.
//   RED (role check deleted): the generation check still answers NotReady (becomeFollower bumps the generation), so the
//   assertion of the sentinel fails; RED (role AND generation checks deleted): OK with the stale hp 1.
// ---------------------------------------------------------------------------
TEST(GetViewRowRegression, ViewReadOnANonLeaderSourceReplicaAnswersNotLeaderNeverAStaleRowRegression) {
    auto src = charSourceV1();
    const Descriptor* srcs[] = {&src};
    auto comp = rv::compileView(hpView(), srcs, kViewTopicId).value();

    ViewSnapshotChannel channel;
    WorkerViewSet vs(channel, tmpDir("rune-views-gvr-exleader"));
    vs.installView(comp.plan, comp.rowLayout, src, /*persisted*/ false);

    const auto batch = oneUpsert(7, 1);   // key 7, hp 1
    auto readRange = [&](int64_t, int64_t) -> rune::storage::LogReadView {
        return rune::storage::LogReadView{batch.data(), batch.size(), 0, 1};
    };
    SrcReplica srcRep;
    vs.driveFolds(kSrcTopicId, /*partition*/ 0, readRange, /*bound*/ 1, 0, nullptr, srcRep.gen());

    Worker w(/*workerId*/ 0);
    w.setOwnedDirect(Worker::OwnedSet{srcRep.replica.get()});
    w.setViewSet(&vs);

    const WorkerReturn before = runGetViewRow(w, kViewTopicId, 0, /*key*/ 7, /*fp*/ 0);
    ASSERT_EQ(before.errorCode, static_cast<int16_t>(pc::ViewRowResponse::kOk));
    EXPECT_EQ(rv::readSlot(before.chunk.data(), comp.rowLayout, 1).i, 1);

    srcRep.replica->becomeFollower(/*leaderEpoch=*/2, /*leaderId=*/2);   // the ex-leader
    ASSERT_NE(srcRep.replica->role(), rune::replication::Role::Leader);

    const WorkerReturn after = runGetViewRow(w, kViewTopicId, 0, /*key*/ 7, /*fp*/ 0);
    EXPECT_EQ(after.errorCode, kViewRowNotLeaderInternal)
        << "an ex-leader never serves its frozen table (stale hp 1)";
    EXPECT_TRUE(after.chunk.empty());
}

// ---------------------------------------------------------------------------
// BINDING (Task 22 fix round 1, C1, new leader): a replica promoted with more
// committed records than one fold pass covers (maxRecordsPerPass = 1) must
// answer NotReady until its fold caught up under THIS leadership — never an
// AbsentKey for a key it has not folded yet — and the catch-up must not make the
// worker election-ineligible (viewsReady stays true).
//   RED (CatchingUp transition removed): AbsentKey for key 5 after the first pass.
//   RED (allViewsReady counts CatchingUp): the viewsReady assertion fails.
// ---------------------------------------------------------------------------
TEST(GetViewRowRegression, ANewLeaderAnswersNotReadyUntilItsReFoldCaughtUpRegression) {
    auto src = charSourceV1();
    const Descriptor* srcs[] = {&src};
    auto comp = rv::compileView(hpView(/*persistence*/ 0), srcs, kViewTopicId).value();

    // A tiny delta ring: its max message (capacity / 8 = 64 B) fits ONE record's checkpoint delta.
    ViewSnapshotChannel channel{OneToOneByteRing{512}, OneToOneByteRing{4096}, OneToOneByteRing{1 << 20}};
    WorkerViewSet vs(channel, tmpDir("rune-views-gvr-newleader"));
    vs.installView(comp.plan, comp.rowLayout, src, /*persisted*/ true);

    SrcReplica srcRep;
    // Five committed source records while leader at generation G.
    for (int64_t id = 1; id <= 5; ++id) {
        const auto k = keyBytes(id);
        const auto v = encodeCharacter(id, static_cast<int32_t>(10 * id), 1);
        ASSERT_GE(srcRep.replica->appendKeyed(std::span<const uint8_t>(k.data(), k.size()),
                                              std::span<const uint8_t>(v.data(), v.size()), false), 0);
    }
    // The promotion: this replica leads under a NEW generation; the view has folded nothing of it.
    const uint64_t genBefore = srcRep.gen();
    srcRep.replica->becomeFollower(/*leaderEpoch=*/2, /*leaderId=*/2);
    srcRep.replica->becomeLeader(/*epoch=*/3, {1});
    ASSERT_GT(srcRep.gen(), genBefore);

    Worker w(/*workerId*/ 0);
    w.setOwnedDirect(Worker::OwnedSet{srcRep.replica.get()});
    w.setViewSet(&vs);

    int notReady = 0;
    bool served = false;
    for (int i = 0; i < 400 && !served; ++i) {
        w.doWork(1'000'000LL * (i + 1));
        channel.deltas.drain([](int32_t, aeron::concurrent::AtomicBuffer&, aeron::util::index_t, aeron::util::index_t) {},
                             1 << 20);   // nobody else consumes the checkpoint deltas here
        EXPECT_TRUE(w.viewsReady()) << "CatchingUp must never gate election eligibility (pass " << i << ")";
        if (!vs.instanceLeadership(kViewTopicId, 0)) continue;   // the RC state is still building
        const WorkerReturn r = runGetViewRow(w, kViewTopicId, 0, /*key*/ 5, /*fp*/ 0);
        ASSERT_NE(r.errorCode, static_cast<int16_t>(pc::ViewRowResponse::kAbsentKey))
            << "pass " << i << ": a key not folded yet is NotReady, never AbsentKey";
        if (r.errorCode == static_cast<int16_t>(pc::ViewRowResponse::kOk)) {
            served = true;
            EXPECT_EQ(rv::readSlot(r.chunk.data(), comp.rowLayout, 1).i, 50);
        } else {
            ASSERT_EQ(r.errorCode, static_cast<int16_t>(pc::ViewRowResponse::kNotReady));
            ++notReady;
        }
    }
    EXPECT_TRUE(served) << "the re-fold must catch up";
    EXPECT_GE(notReady, 2) << "maxRecordsPerPass = 1 over 5 records: several NotReady passes (the vacuity guard)";
}

// ---------------------------------------------------------------------------
// Task 22 fix round 2 (N-C1): the HW floor of a fresh leader can lag what the old leader had already committed, acked and
// served.  A view table is Ready under a leadership only once HW >= leadershipStartOffset() (the log end at election): before
// that it is CatchingUp (NotReady), never a stale row or an AbsentKey for an existing key - and election eligibility is
// unchanged (viewsReady stays true).  Both tests need HW < log end at promotion, so two replicas: a "truth" leader T appends
// keyed character records; the replica under test F is a Follower fed T's bytes with a chosen high watermark (the NB1 pattern,
// PartitionReplicaTest NB1_BecomeLeaderUncommittedTail), on a DISK log so the worker's disk fold path is used.
// ---------------------------------------------------------------------------
namespace {

constexpr rune::replication::NodeId kNodeF = 2, kNodeT = 1;

PartitionConfig lagConfig(rune::replication::NodeId self) {
    PartitionConfig cfg{};
    cfg.topicId             = kSrcTopicId;
    cfg.partitionId         = 0;
    cfg.selfId              = self;
    cfg.minIsr              = 1;
    cfg.replicaLagMaxMs     = 10'000;
    cfg.replicaLagMaxOffset = 10'000;
    cfg.fetchIntervalNs     = 1'000'000LL;
    cfg.isrCheckIntervalNs  = 1'000'000LL;
    cfg.maxFetchBytes       = 1 << 20;
    cfg.pushOnAppend        = false;
    return cfg;
}

struct DiskReplica {
    std::string                           dir;
    std::unique_ptr<rune::storage::Log>   log;
    rune::replication::InMemoryTransport  transport;
    std::unique_ptr<PartitionReplica>     replica;
    DiskReplica(const std::string& tag, rune::replication::NodeId self, Role role) {
        dir = tmpDir("rune-gvr-" + tag);
        std::filesystem::create_directories(dir);
        log = std::make_unique<rune::storage::Log>(dir);
        replica = std::make_unique<PartitionReplica>(lagConfig(self), role, /*epoch=*/1,
                                                     std::vector<rune::replication::NodeId>{kNodeT, kNodeF},
                                                     std::vector<rune::replication::NodeId>{kNodeT, kNodeF},
                                                     log.get(), &transport);
    }
};

/// The truth leader's records: offsets 0..4 = ids 1..5 with hp 10*id; offset 5 = id 1 with hp 99.
void appendTruth(DiskReplica& t, int upTo, int from = 0) {
    const std::pair<int64_t, int32_t> recs[] = {{1, 10}, {2, 20}, {3, 30}, {4, 40}, {5, 50}, {1, 99}};
    for (int i = from; i < upTo; ++i) {
        const auto k = keyBytes(recs[i].first);
        const auto v = encodeCharacter(recs[i].first, recs[i].second, 1);
        ASSERT_GE(t.replica->appendKeyed(std::span<const uint8_t>(k.data(), k.size()),
                                         std::span<const uint8_t>(v.data(), v.size()), false), 0);
    }
}

/// Feed `f` the truth's bytes from `from` with the given high watermark (as a fetch response of `epoch`).
void feed(DiskReplica& t, DiskReplica& f, int64_t from, int64_t hw, int32_t epoch) {
    const auto bytes = t.log->read(from, 1 << 20);
    ASSERT_FALSE(bytes.empty());
    rune::replication::FetchResponseMsg resp{};
    resp.leaderEpoch    = epoch;
    resp.partition      = 0;
    resp.highWatermark  = hw;
    resp.logStartOffset = 0;
    resp.errorCode      = rune::replication::kErrNone;
    resp.records        = std::span<const uint8_t>(bytes.data(), bytes.size());
    f.replica->applyReplicated(resp);
}

struct LagHarness {
    DiskReplica t{"truth", kNodeT, Role::Leader};
    DiskReplica f{"under-test", kNodeF, Role::Follower};
    ViewSnapshotChannel channel;
    std::unique_ptr<WorkerViewSet> vs;
    std::unique_ptr<Worker> w;
    rv::RowLayout layout;
    Descriptor src = charSourceV1();
    int pass = 0;

    LagHarness(bool persisted, const std::string& tag) {
        const Descriptor* srcs[] = {&src};
        auto comp = rv::compileView(hpView(persisted ? 0 : 1), srcs, kViewTopicId).value();
        layout = comp.rowLayout;
        vs = std::make_unique<WorkerViewSet>(channel, tmpDir("rune-views-gvr-lag-" + tag));
        vs->installView(comp.plan, comp.rowLayout, src, persisted);
        w = std::make_unique<Worker>(/*workerId*/ 0);
        w->setOwnedDirect(Worker::OwnedSet{f.replica.get()});
        w->setViewSet(vs.get());
        t.replica->becomeLeader(1, {kNodeT});   // the truth leader of a one-member ISR (appends commit alone)
    }
    void passes(int n) {
        for (int i = 0; i < n; ++i) {
            w->doWork(1'000'000LL * (++pass));
            channel.deltas.drain([](int32_t, aeron::concurrent::AtomicBuffer&, aeron::util::index_t, aeron::util::index_t) {},
                                 1 << 20);
            EXPECT_TRUE(w->viewsReady()) << "CatchingUp must never gate election eligibility (pass " << pass << ")";
        }
    }
    /// {status, hp}: hp is meaningful for OK only.
    std::pair<int, int64_t> read(uint64_t key) {
        const WorkerReturn r = runGetViewRow(*w, kViewTopicId, 0, key, 0);
        if (r.errorCode != static_cast<int16_t>(pc::ViewRowResponse::kOk)) return {r.errorCode, -1};
        return {0, rv::readSlot(r.chunk.data(), layout, 1).i};
    }
    /// Passes until `key` reads OK (bounded); the hp.
    int64_t awaitOk(uint64_t key, int bound = 50) {
        for (int i = 0; i < bound; ++i) {
            passes(1);
            const auto [st, hp] = read(key);
            if (st == 0) return hp;
        }
        return -1;
    }
};

constexpr int kNotReady = static_cast<int>(pc::ViewRowResponse::kNotReady);

}  // namespace

// A fresh leader whose HW (3) lags its start offset (6): the records the old leader committed and served are above the floor.
// RED (every -> Ready flip made unconditional): AbsentKey for key 5, or OK hp 10 for key 1 (older than the 99 the cluster served).
TEST(GetViewRowRegression, ANewLeaderWhoseHighWatermarkLagsItsStartOffsetNeverServesAStaleOrAbsentRowRegression) {
    for (const bool persisted : {false, true}) {
        SCOPED_TRACE(persisted ? "persisted view" : "transient view");
        LagHarness h(persisted, persisted ? "p" : "t");
        ASSERT_NO_FATAL_FAILURE(appendTruth(h.t, 6));
        ASSERT_NO_FATAL_FAILURE(feed(h.t, h.f, 0, /*hw=*/3, /*epoch=*/1));
        const uint64_t genBefore = h.f.replica->leadershipGeneration();
        h.f.replica->becomeLeader(/*epoch=*/2, {kNodeF, kNodeT});
        ASSERT_EQ(h.f.replica->highWatermark(), 3);
        ASSERT_EQ(h.f.replica->leadershipStartOffset(), 6);
        const uint64_t gen = h.f.replica->leadershipGeneration();
        ASSERT_GT(gen, genBefore);

        h.passes(20);
        ASSERT_TRUE(h.vs->instanceLeadership(kViewTopicId, 0).has_value()) << "the fold ran (vacuity guard)";
        EXPECT_EQ(h.read(5).first, kNotReady) << "key 5 is committed above the HW floor: NotReady, never AbsentKey";
        EXPECT_EQ(h.read(1).first, kNotReady) << "key 1 would read hp 10, older than the 99 already served: NotReady";

        // The HW is confirmed (a one-member ISR commits alone; same role => same generation).
        h.f.replica->becomeLeader(/*epoch=*/3, {kNodeF});
        ASSERT_EQ(h.f.replica->leadershipGeneration(), gen);
        ASSERT_GE(h.f.replica->highWatermark(), 6);
        EXPECT_EQ(h.awaitOk(1), 99);
        const auto five = h.read(5);
        EXPECT_EQ(five.first, 0);
        EXPECT_EQ(five.second, 50);
    }
}

// An ex-leader promoted again with its OLD table (cursor 3 >= the lagging floor 3): the unconditional CatchingUp on a new
// generation, not "cursor < bound", is what keeps the frozen table from being served.
// RED (`&& inst.foldCursor < bound` restored in step 0b): OK hp 10 for key 1 (stale).
TEST(GetViewRowRegression, AnExLeaderPromotedAgainNeverServesItsFrozenTableBelowItsStartOffsetRegression) {
    for (const bool persisted : {false, true}) {
        SCOPED_TRACE(persisted ? "persisted view" : "transient view");
        LagHarness h(persisted, persisted ? "xp" : "xt");
        ASSERT_NO_FATAL_FAILURE(appendTruth(h.t, 3));                       // offsets 0..2 (ids 1..3)
        ASSERT_NO_FATAL_FAILURE(feed(h.t, h.f, 0, /*hw=*/3, 1));
        h.f.replica->becomeLeader(/*epoch=*/2, {kNodeF});                   // gen1: a one-member ISR commits alone
        const uint64_t gen1 = h.f.replica->leadershipGeneration();
        EXPECT_EQ(h.awaitOk(1), 10) << "first leadership: the whole log is committed and folded (cursor 3)";

        h.f.replica->becomeFollower(/*leaderEpoch=*/3, kNodeT);             // demoted (gen2): the HW floor is forgotten
        ASSERT_NE(h.f.replica->role(), Role::Leader);
        ASSERT_NO_FATAL_FAILURE(appendTruth(h.t, 6, 3));                    // the interim leader commits 3..5 (id 1 -> 99)
        ASSERT_NO_FATAL_FAILURE(feed(h.t, h.f, 3, /*hw=*/2, 3));            // F holds them but learns only HW 2 (its floor stays 3)
        h.f.replica->becomeLeader(/*epoch=*/4, {kNodeF, kNodeT});           // gen3
        ASSERT_GT(h.f.replica->leadershipGeneration(), gen1);
        // (the replica's HW never regresses below what it already reported - the frozen table's cursor 3 equals the floor)
        ASSERT_EQ(h.f.replica->highWatermark(), 3);
        ASSERT_EQ(h.f.replica->leadershipStartOffset(), 6);

        h.passes(10);
        EXPECT_EQ(h.read(1).first, kNotReady) << "the frozen table (hp 10, cursor 3) is not served below the start offset";
        EXPECT_EQ(h.read(5).first, kNotReady);
        h.f.replica->becomeLeader(/*epoch=*/5, {kNodeF});          // the HW confirms
        ASSERT_GE(h.f.replica->highWatermark(), 6);
        EXPECT_EQ(h.awaitOk(1), 99);
    }
}

// ---------------------------------------------------------------------------
// openView handshake: DefineTopic + CreateView register the view's typed output
// topic; openView with a STALE pinned fingerprint is rejected (SchemaMismatch)
// up front, while the current fingerprint (and 0 = latest) resolve OK.
// ---------------------------------------------------------------------------
namespace {

std::string uniqSuffix() {
    static uint64_t seq = 0;
    const auto ts = std::chrono::steady_clock::now().time_since_epoch().count();
    // The pid keeps ctest -j parallel PROCESSES apart (each starts seq at 0 and the
    // clock can tick identically in two of them; a shared dir was removed under a peer).
    return std::to_string(::getpid()) + "_" + std::to_string(seq++) + "_" + std::to_string(ts);
}

struct TmpDir {
    std::string path;
    explicit TmpDir(const std::string& tag) {
        path = std::filesystem::temp_directory_path().string() + "/rune_gvr_" + tag + "_" + uniqSuffix();
        std::filesystem::create_directories(path);
    }
    ~TmpDir() { std::error_code ec; std::filesystem::remove_all(path, ec); }
};

std::vector<uint8_t> characterBlob() {
    return serialize(DescriptorBuilder("character", 1)
        .add("id", PrimType::Int64, 1, /*isKey=*/true)
        .add("hp", PrimType::Int64)
        .build());
}

std::vector<uint8_t> charHpPlanBlob(int32_t srcTopicId) {
    rv::ViewPlan p;
    p.viewName = "char_hp";
    p.key      = "id";
    p.persistence = 0;
    p.sources.push_back(rv::SourceSpec{srcTopicId, "id"});
    p.output.push_back(rv::AuthoredField{"id", PrimType::Int64, 0, "id", rv::MergeRule::Replace});
    p.output.push_back(rv::AuthoredField{"hp", PrimType::Int64, 0, "hp", rv::MergeRule::Replace});
    return rv::serializeAuthored(p);
}

} // namespace

// Catalog history Task 9: the admin side is the catalog write path — one AdminWorker
// over a __catalog leader replica (CatalogDdlRig); the view's typed output is the
// applier's projection of its CreateView event.
class GetViewRowOpenRegression : public ::testing::Test {
protected:
    void SetUp() override { rig_.becomeServingLeader(); }
    rune::node::test::CatalogDdlRig rig_{"gvr_open", 1};
};

TEST_F(GetViewRowOpenRegression, OpenViewFingerprintMismatchRejected) {
    // 1. Source topic.
    DefineTopicMsg d;
    d.session = 1; d.corr = 10; d.partitionCount = 1; d.name = "character";
    d.keyField = "id"; d.validate = false; d.blob = characterBlob();
    d.meta.requestId = rig_.freshRequestId();
    const auto def = rig_.submitRaw(rune::node::DdlRequest{d});
    ASSERT_EQ(def.code, 0u);
    const int32_t srcTopicId = def.topicId;
    ASSERT_GE(srcTopicId, 0);

    // 2. CreateView over it (registers the view's typed output topic).
    CreateViewMsg cv;
    cv.session = 1; cv.corr = 20; cv.persistence = 0;
    cv.viewName = "char_hp"; cv.keyField = "id"; cv.planBlob = charHpPlanBlob(srcTopicId);
    cv.meta.requestId = rig_.freshRequestId();
    const auto view = rig_.submitRaw(rune::node::DdlRequest{cv});
    ASSERT_EQ(view.code, static_cast<uint16_t>(pc::ErrorCode::OK));
    const int32_t lastViewTopicId_ = view.topicId;
    ASSERT_GE(lastViewTopicId_, 0);
    AdminWorker* adminWorker_ = &rig_.admin();

    // 3. openView by name with fp=0 (latest) resolves OK + the current fingerprint.
    int32_t  gotId = -1;
    uint64_t gotFp = 0;
    EXPECT_EQ(adminWorker_->openView("char_hp", /*fp*/ 0, gotId, gotFp),
              static_cast<uint16_t>(pc::ViewRowResponse::kOk));
    EXPECT_EQ(gotId, lastViewTopicId_);
    ASSERT_NE(gotFp, 0u) << "a registered view output must have a non-zero fingerprint";
    const uint64_t currentFp = gotFp;

    // 4. Pinning the CURRENT fingerprint also resolves OK.
    EXPECT_EQ(adminWorker_->openView("char_hp", currentFp, gotId, gotFp),
              static_cast<uint16_t>(pc::ViewRowResponse::kOk));

    // 5. A STALE (drifted) fingerprint is rejected UP FRONT with SchemaMismatch.
    const uint64_t staleFp = currentFp ^ 0xDEADBEEFULL;  // guaranteed != currentFp
    EXPECT_EQ(adminWorker_->openView("char_hp", staleFp, gotId, gotFp),
              static_cast<uint16_t>(pc::ViewRowResponse::kSchemaMismatch));

    // 6. An unknown view name resolves to UnknownView.
    EXPECT_EQ(adminWorker_->openView("no_such_view", /*fp*/ 0, gotId, gotFp),
              static_cast<uint16_t>(pc::ViewRowResponse::kUnknownView));
}

// =====================================================================================================================
// Issue #82 (VLS Part A): the view read lease, the new-leader serve gate (W) and the read-your-writes fold barrier, on
// the REAL worker op/return path.  The cluster repro (a partitioned ex-leader after an acked failover write) is
// ViewReadLeaseRegression.Partitioned*ExLeaderNeverServesAStaleRowRegression in CatalogClusterIntegrationTest.cpp.
// =====================================================================================================================
namespace {

/// A worker over a leader source replica and a Ready transient view holding key 7 (hp `hp`).
struct LeaseRig {
    Descriptor src = charSourceV1();
    rv::CompiledViewPlan plan;
    rv::RowLayout layout;
    ViewSnapshotChannel channel;
    std::unique_ptr<WorkerViewSet> vs;
    SrcReplica srcRep;
    std::unique_ptr<Worker> w;
    int64_t leaseNow = 5'000'000'000;   // the injected lease clock (the duty clock is the doWork argument)
    std::vector<uint8_t> batch;

    explicit LeaseRig(const std::string& tag, int32_t hp = 1) {
        const Descriptor* srcs[] = {&src};
        auto comp = rv::compileView(hpView(), srcs, kViewTopicId).value();
        plan = comp.plan;
        layout = comp.rowLayout;
        vs = std::make_unique<WorkerViewSet>(channel, tmpDir("rune-views-lease-" + tag));
        vs->installView(comp.plan, comp.rowLayout, src, /*persisted*/ false);
        vs->setLeaseClockForTest(&leaseNow);
        batch = oneUpsert(7, hp);
        auto readRange = [&](int64_t, int64_t) -> rune::storage::LogReadView {
            return rune::storage::LogReadView{batch.data(), batch.size(), 0, 1};
        };
        vs->driveFolds(kSrcTopicId, 0, readRange, /*bound*/ 1, 0, nullptr, srcRep.gen());
        w = std::make_unique<Worker>(0);
        w->setOwnedDirect(Worker::OwnedSet{srcRep.replica.get()});
        w->setViewSet(vs.get());
        w->setLeaseClockForTest(&leaseNow);
    }
    void lease(int64_t endNs) {
        WorkerOp op;
        op.type       = WorkerOp::Type::ReadLease;
        op.nextOffset = endNs;
        ASSERT_TRUE(w->tryEnqueueOp(std::move(op)));
    }
    WorkerReturn read(int64_t dutyNowNs = 1'000'000LL) {
        WorkerOp op;
        op.type = WorkerOp::Type::GetViewRow; op.topicId = kViewTopicId; op.partition = 0;
        op.sessionId = 1; op.correlationId = 42; op.nextOffset = 7; op.hwBound = 0;
        EXPECT_TRUE(w->tryEnqueueOp(std::move(op)));
        w->doWork(dutyNowNs);
        WorkerReturn got{};
        int n = 0;
        w->drainReturn([&](WorkerReturn r) { if (r.type == WorkerReturn::Type::ViewRowResult) { got = std::move(r); ++n; } });
        EXPECT_EQ(n, 1);
        return got;
    }
};
constexpr int16_t kOkCode = static_cast<int16_t>(pc::ViewRowResponse::kOk);

}  // namespace

// A worker that requires a lease (a multi-node broker's) serves a row only while the lease clock is below the lease end;
// the end only grows (a late, shorter grant never shortens it); with no lease at all it never serves.
// RED (the serve check removed): every read answers OK, the expired ones included.
TEST(ViewReadLeaseRegression, AWorkerServesAViewRowOnlyWhileItHoldsAReadLeaseRegression) {
    LeaseRig r("serve");
    EXPECT_EQ(r.read().errorCode, kOkCode) << "the default (single node / unit tests): unbounded";
    r.w->requireViewReadLease();
    EXPECT_EQ(r.read().errorCode, kViewRowNotLeaderInternal) << "required, none held yet";
    r.lease(r.leaseNow + 100'000'000);
    const WorkerReturn ok = r.read();
    ASSERT_EQ(ok.errorCode, kOkCode) << "under a lease";
    EXPECT_EQ(rv::readSlot(ok.chunk.data(), r.layout, 1).i, 1);
    r.leaseNow += 99'999'999;
    EXPECT_EQ(r.read().errorCode, kOkCode) << "1 ns before the end";
    r.lease(r.leaseNow - 50'000'000);   // an older, shorter grant arriving late: the end never moves back
    r.leaseNow += 1;
    EXPECT_EQ(r.read().errorCode, kViewRowNotLeaderInternal) << "at the end: not served (a frozen table, never)";
    EXPECT_EQ(r.w->viewLeaseEndNsForTest(), r.leaseNow);
    EXPECT_GE(r.w->viewReadsWithoutLeaseForTest(), 2u);
    r.lease(r.leaseNow + 50'000'000);   // renewed
    EXPECT_EQ(r.read().errorCode, kOkCode);
    EXPECT_EQ(r.w->readLeasesSeenForTest(), 3u);
}

// VLS A.7 LeaseClockSuspendRegression: the lease is measured on the LEASE clock (counts suspend), never on the duty
// clock: a lease-clock jump (the machine slept) expires it while the duty clock did not move, and a duty-clock jump alone
// expires nothing.
// RED (the check reads the duty clock / the pass clock): the first read after the sleep answers OK.
TEST(ViewReadLeaseRegression, LeaseClockSuspendExpiresTheLeaseTheDutyClockDoesNotRegression) {
    LeaseRig r("suspend");
    r.w->requireViewReadLease();
    r.lease(r.leaseNow + 100'000'000);
    EXPECT_EQ(r.read(/*duty*/ 1'000'000LL).errorCode, kOkCode);
    EXPECT_EQ(r.read(/*duty*/ 1'000'000LL + 3'600'000'000'000LL).errorCode, kOkCode)
        << "an hour on the duty clock alone (e.g. it stopped and resumed elsewhere): the lease clock decides";
    r.leaseNow += 30'000'000'000LL;   // 30 s asleep: the boot-time clock counts them
    EXPECT_EQ(r.read(/*duty*/ 1'000'000LL + 3'600'000'000'000LL).errorCode, kViewRowNotLeaderInternal);
}

// VLS A.7 ViewReadLeaseFifoRegression: the op ring is full when a demotion and a newer lease (issued after the
// demotion committed) arrive.  The bridge holds the demotion (pending); ReadLeaseFanout holds the lease while it waits,
// then posts it BEHIND the demotion: the worker applies the demotion first and never serves its table under the newer
// lease.  RED (fan-out ignores pendingFor): the lease lands first and the read in between answers OK (the stale row).
TEST(ViewReadLeaseRegression, ALeaseNeverOvertakesADemotionOnAFullOpRingRegression) {
    auto src = charSourceV1();
    const Descriptor* srcs[] = {&src};
    auto comp = rv::compileView(hpView(), srcs, kViewTopicId).value();
    ViewSnapshotChannel channel;
    WorkerViewSet vs(channel, tmpDir("rune-views-lease-fifo"));
    vs.installView(comp.plan, comp.rowLayout, src, /*persisted*/ false);
    const auto batch = oneUpsert(7, 1);
    auto readRange = [&](int64_t, int64_t) -> rune::storage::LogReadView {
        return rune::storage::LogReadView{batch.data(), batch.size(), 0, 1};
    };
    SrcReplica srcRep;
    vs.driveFolds(kSrcTopicId, 0, readRange, 1, 0, nullptr, srcRep.gen());
    // A small op ring so it can be filled.
    Worker w(/*workerId*/ 0, /*ingressRingBytes*/ 64 * 1024);
    w.setOwnedDirect(Worker::OwnedSet{srcRep.replica.get()});
    w.setViewSet(&vs);
    int64_t leaseNow = 1'000'000'000;
    w.setLeaseClockForTest(&leaseNow);
    w.requireViewReadLease();
    // The holder: lease A (commitPos 10) before the demotion committed at 15, lease B (commitPos 20) after it.
    rune::consensus::ReadLeaseHolder holder;
    {
        const auto a = holder.nextRequest(leaseNow);
        ASSERT_TRUE(holder.onGrant(a.reqSeq, 1, /*commitPos*/ 10, 50'000'000));
    }
    ReadLeaseFanout fan;
    fan.configure(1);
    std::deque<WorkerOp> pendingRoleOps;   // the bridge's per-worker queue
    auto enqueueLease = [&](int32_t, int64_t end) {
        WorkerOp op; op.type = WorkerOp::Type::ReadLease; op.nextOffset = end;
        return w.tryEnqueueOp(std::move(op));
    };
    auto pendingFor = [&](int32_t) { return !pendingRoleOps.empty(); };
    ASSERT_EQ(fan.step(holder, /*derivedPos*/ 10, pendingFor, enqueueLease), 1) << "lease A delivered";
    const int64_t endA = fan.deliveredForTest(0);
    // Serve once under A (the ring is drained).
    {
        WorkerOp op; op.type = WorkerOp::Type::GetViewRow; op.topicId = kViewTopicId; op.partition = 0;
        op.sessionId = 1; op.correlationId = 1; op.nextOffset = 7;
        ASSERT_TRUE(w.tryEnqueueOp(std::move(op)));
        w.doWork(1'000'000);
        int16_t code = -1;
        w.drainReturn([&](WorkerReturn r) { if (r.type == WorkerReturn::Type::ViewRowResult) code = r.errorCode; });
        ASSERT_EQ(code, kOkCode);
    }
    // Fill the op ring with reads (each would be answered under whatever lease the worker holds when it runs them).
    int fillers = 0;
    for (;; ++fillers) {
        WorkerOp op; op.type = WorkerOp::Type::GetViewRow; op.topicId = kViewTopicId; op.partition = 0;
        op.sessionId = 1; op.correlationId = 1000 + fillers; op.nextOffset = 7;
        if (!w.tryEnqueueOp(std::move(op))) break;
    }
    ASSERT_GT(fillers, 4);
    // The demotion (committed at metadata 15): the ring is full, so the bridge keeps it pending.
    WorkerOp demote;
    demote.type = WorkerOp::Type::BecomeFollower; demote.topicId = kSrcTopicId; demote.partition = 0;
    demote.replica = srcRep.replica.get(); demote.roleEpoch = 2; demote.roleLeaderId = 2;
    if (!w.tryEnqueueOp(WorkerOp(demote))) pendingRoleOps.push_back(demote);
    ASSERT_EQ(pendingRoleOps.size(), 1u) << "the demotion waits for ring space";
    // Lease B, issued after the demotion committed, arrives; this broker applied the metadata up to 20.
    {
        const auto b = holder.nextRequest(leaseNow + 10'000'000);
        ASSERT_TRUE(holder.onGrant(b.reqSeq, 2, /*commitPos*/ 20, 150'000'000));
    }
    EXPECT_EQ(fan.step(holder, /*derivedPos*/ 20, pendingFor, enqueueLease), 0) << "held behind the pending demotion";
    EXPECT_EQ(fan.deliveredForTest(0), endA);
    EXPECT_GE(fan.heldBehindRoleOpsForTest(), 1u);
    // Control passes / worker passes interleave until both are delivered: the bridge flushes first, then the fan-out.
    std::vector<int16_t> codes;
    int lastSeq = 0;
    for (int pass = 0; pass < 200 && (!pendingRoleOps.empty() || fan.deliveredForTest(0) == endA); ++pass) {
        w.doWork(2'000'000 + pass);
        w.drainReturn([&](WorkerReturn r) { if (r.type == WorkerReturn::Type::ViewRowResult) codes.push_back(r.errorCode); });
        while (!pendingRoleOps.empty() && w.tryEnqueueOp(WorkerOp(pendingRoleOps.front()))) pendingRoleOps.pop_front();
        (void)fan.step(holder, 20, pendingFor, enqueueLease);
        ++lastSeq;
    }
    ASSERT_TRUE(pendingRoleOps.empty());
    ASSERT_GT(fan.deliveredForTest(0), endA) << "lease B delivered";
    // Everything queued after the demotion (the lease B, then this read) runs after it: the ex-leader answers NotLeader.
    {
        WorkerOp op; op.type = WorkerOp::Type::GetViewRow; op.topicId = kViewTopicId; op.partition = 0;
        op.sessionId = 1; op.correlationId = 9; op.nextOffset = 7;
        ASSERT_TRUE(w.tryEnqueueOp(std::move(op)));
        for (int i = 0; i < 10; ++i) {
            w.doWork(9'000'000 + i);
            w.drainReturn([&](WorkerReturn r) { if (r.type == WorkerReturn::Type::ViewRowResult) codes.push_back(r.errorCode); });
        }
    }
    ASSERT_NE(srcRep.replica->role(), rune::replication::Role::Leader) << "the demotion was applied";
    ASSERT_FALSE(codes.empty());
    EXPECT_EQ(codes.back(), kViewRowNotLeaderInternal) << "the read after the newer lease is refused: the demotion came first";
}

// ReadLeaseFanout alone: a lease goes to a worker only once the metadata its grant covered is applied and none of that
// worker's role ops waits; each worker independently (one with pending role ops never holds back another); a full ring
// is retried; a delivered lease is never re-posted.
TEST(ViewReadLeaseRegression, TheFanoutGatesEachWorkerOnAppliedMetadataAndItsOwnPendingRoleOpsRegression) {
    rune::consensus::ReadLeaseHolder holder;
    auto grant = [&](int64_t sendNs, int64_t commitPos, int64_t validFor) {
        const auto q = holder.nextRequest(sendNs);
        ASSERT_TRUE(holder.onGrant(q.reqSeq, q.reqSeq, commitPos, validFor));
    };
    ReadLeaseFanout fan;
    fan.configure(3);
    std::vector<std::pair<int32_t, int64_t>> posted;
    bool pending[3] = {false, true, false};
    bool full[3] = {false, false, true};
    auto pendingFor = [&](int32_t w) { return pending[w]; };
    auto enqueue = [&](int32_t w, int64_t end) { if (full[w]) return false; posted.emplace_back(w, end); return true; };
    EXPECT_EQ(fan.step(holder, 100, pendingFor, enqueue), 0) << "nothing granted yet";
    grant(1'000, 50, 100'000'000);
    EXPECT_EQ(fan.step(holder, 49, pendingFor, enqueue), 0) << "the grant covers metadata this broker has not applied";
    EXPECT_EQ(fan.step(holder, 50, pendingFor, enqueue), 1) << "worker 0 only: 1 has pending role ops, 2's ring is full";
    ASSERT_EQ(posted.size(), 1u);
    EXPECT_EQ(posted[0].first, 0);
    const int64_t e1 = posted[0].second;
    EXPECT_EQ(e1, 1'000 + rune::consensus::leaseShrinkByDrift(100'000'000));
    EXPECT_EQ(fan.step(holder, 50, pendingFor, enqueue), 0) << "never re-posted";
    pending[1] = false; full[2] = false;
    EXPECT_EQ(fan.step(holder, 50, pendingFor, enqueue), 2);
    EXPECT_EQ(fan.deliveredForTest(1), e1);
    EXPECT_EQ(fan.deliveredForTest(2), e1);
    grant(20'000'000, 60, 100'000'000);
    EXPECT_EQ(fan.step(holder, 55, pendingFor, enqueue), 0) << "the newer grant's metadata is not applied: the older lease stands";
    EXPECT_EQ(fan.step(holder, 60, pendingFor, enqueue), 3);
    EXPECT_GT(fan.deliveredForTest(0), e1);
}

// VLS A.5.3 / A.7 (ViewReadLeaseRebalanceRegression, time path): a broker that just became the source leader serves
// its view only W after it first drove the table under the new leadership - every lease its predecessor was granted
// before the change has ended by then.  W = 0 (a single node) serves at once.
// RED (predecessorFenced_ removed from the Ready flips): OK right after the promotion.
TEST(ViewReadLeaseRegression, ANewSourceLeaderServesOnlyAfterThePredecessorFenceWaitRegression) {
    for (const bool persisted : {false, true}) {
        SCOPED_TRACE(persisted ? "persisted view" : "transient view");
        LagHarness h(persisted, persisted ? "fwp" : "fwt");
        int64_t leaseNow = 2'000'000'000;
        constexpr int64_t kW = 153'015'000;   // leaseFenceWaitNs(150 ms)
        h.vs->setLeaseClockForTest(&leaseNow);
        h.vs->setPredecessorFenceWaitNs(kW);
        ASSERT_NO_FATAL_FAILURE(appendTruth(h.t, 3));
        ASSERT_NO_FATAL_FAILURE(feed(h.t, h.f, 0, /*hw=*/3, 1));
        h.f.replica->becomeLeader(/*epoch=*/2, {kNodeF});   // promoted: a one-member ISR commits alone
        h.passes(10);
        ASSERT_TRUE(h.vs->instanceLeadership(kViewTopicId, 0).has_value()) << "the fold ran (vacuity guard)";
        EXPECT_EQ(h.read(1).first, kNotReady) << "inside W: the predecessor may still serve under an older lease";
        leaseNow += kW - 1;
        h.passes(2);
        EXPECT_EQ(h.read(1).first, kNotReady) << "1 ns before W";
        leaseNow += 1;
        EXPECT_EQ(h.awaitOk(1), 10) << "W passed: served";
        // A later leadership of the same broker waits again.
        h.f.replica->becomeFollower(/*leaderEpoch=*/3, kNodeT);
        h.f.replica->becomeLeader(/*epoch=*/4, {kNodeF});
        h.passes(5);
        EXPECT_EQ(h.read(1).first, kNotReady);
        leaseNow += kW;
        EXPECT_EQ(h.awaitOk(1), 10);
    }
}

// VLS A.5.5 / A.7 ViewReadYourWriteRegression: a write acked (committed) right before a GetViewRow on the TRUE leader
// is seen by it, even when the fold lags (a persisted view folds one record per pass here): the read waits for the fold
// to reach the committed bound at its arrival - never an AbsentKey for the key just written, never an older value.
// RED (the barrier removed): the first answer is AbsentKey (key 5 not folded yet).
TEST(ViewReadLeaseRegression, ViewReadYourWriteSeesTheWriteAckedBeforeItRegression) {
    auto src = charSourceV1();
    const Descriptor* srcs[] = {&src};
    auto comp = rv::compileView(hpView(/*persistence*/ 0), srcs, kViewTopicId).value();
    ViewSnapshotChannel channel{OneToOneByteRing{512}, OneToOneByteRing{4096}, OneToOneByteRing{1 << 20}};
    WorkerViewSet vs(channel, tmpDir("rune-views-ryw"));
    vs.installView(comp.plan, comp.rowLayout, src, /*persisted*/ true);
    SrcReplica srcRep;
    Worker w(/*workerId*/ 0);
    w.setOwnedDirect(Worker::OwnedSet{srcRep.replica.get()});
    w.setViewSet(&vs);
    auto drainDeltas = [&] {
        channel.deltas.drain([](int32_t, aeron::concurrent::AtomicBuffer&, aeron::util::index_t, aeron::util::index_t) {},
                             1 << 20);
    };
    auto appendChar = [&](int64_t id, int32_t hp) {
        const auto k = keyBytes(id);
        const auto v = encodeCharacter(id, hp, 1);
        ASSERT_GE(srcRep.replica->appendKeyed(std::span<const uint8_t>(k.data(), k.size()),
                                              std::span<const uint8_t>(v.data(), v.size()), false), 0);
    };
    // A Ready table under this leadership (key 1).
    ASSERT_NO_FATAL_FAILURE(appendChar(1, 10));
    bool ready = false;
    for (int i = 0; i < 200 && !ready; ++i) {
        w.doWork(1'000'000LL * (i + 1));
        drainDeltas();
        ready = vs.instanceLeadership(kViewTopicId, 0).has_value() &&
                vs.readiness(kViewTopicId, 0) == ViewReadiness::Ready;
    }
    ASSERT_TRUE(ready);
    // Four more writes, committed at once (one-member ISR): the fold folds one per pass.
    for (int64_t id = 2; id <= 5; ++id) ASSERT_NO_FATAL_FAILURE(appendChar(id, static_cast<int32_t>(10 * id)));
    ASSERT_GE(srcRep.replica->highWatermark(), 5);
    WorkerOp op;
    op.type = WorkerOp::Type::GetViewRow; op.topicId = kViewTopicId; op.partition = 0;
    op.sessionId = 1; op.correlationId = 55; op.nextOffset = 5;
    ASSERT_TRUE(w.tryEnqueueOp(std::move(op)));
    int16_t code = -1;
    int32_t hp = -1;
    int passes = 0;
    for (; passes < 50 && code < 0; ++passes) {
        w.doWork(500'000'000LL + passes);
        drainDeltas();
        w.drainReturn([&](WorkerReturn r) {
            if (r.type != WorkerReturn::Type::ViewRowResult) return;
            code = r.errorCode;
            if (code == kOkCode) hp = static_cast<int32_t>(rv::readSlot(r.chunk.data(), comp.rowLayout, 1).i);
        });
    }
    ASSERT_EQ(code, kOkCode) << "the read after the acked write answered " << code << " (1 = AbsentKey: the fold lagged)";
    EXPECT_EQ(hp, 50);
    EXPECT_GE(passes, 2) << "the fold needed more than one pass (the barrier actually held: vacuity guard)";
    EXPECT_GE(w.viewReadsHeldForTest(), 1u);
    EXPECT_EQ(w.heldViewReadsForTest(), 0u);
}

// The barrier never parks a read forever: a fold that cannot progress (here its checkpoint ring is never drained, so it
// stops after a few deltas) ends the held read with the retriable NotReady at its deadline.
TEST(ViewReadLeaseRegression, AHeldViewReadEndsNotReadyWhenTheFoldCannotReachItsTargetRegression) {
    auto src = charSourceV1();
    const Descriptor* srcs[] = {&src};
    auto comp = rv::compileView(hpView(/*persistence*/ 0), srcs, kViewTopicId).value();
    ViewSnapshotChannel channel{OneToOneByteRing{512}, OneToOneByteRing{4096}, OneToOneByteRing{1 << 20}};
    WorkerViewSet vs(channel, tmpDir("rune-views-ryw-stuck"));
    vs.installView(comp.plan, comp.rowLayout, src, /*persisted*/ true);
    SrcReplica srcRep;
    Worker w(/*workerId*/ 0);
    w.setOwnedDirect(Worker::OwnedSet{srcRep.replica.get()});
    w.setViewSet(&vs);
    auto appendChar = [&](int64_t id, int32_t hp) {
        const auto k = keyBytes(id);
        const auto v = encodeCharacter(id, hp, 1);
        ASSERT_GE(srcRep.replica->appendKeyed(std::span<const uint8_t>(k.data(), k.size()),
                                              std::span<const uint8_t>(v.data(), v.size()), false), 0);
    };
    ASSERT_NO_FATAL_FAILURE(appendChar(1, 10));
    for (int i = 0; i < 200 && !(vs.instanceLeadership(kViewTopicId, 0).has_value() &&
                                 vs.readiness(kViewTopicId, 0) == ViewReadiness::Ready); ++i) {
        w.doWork(1'000'000LL * (i + 1));
        channel.deltas.drain([](int32_t, aeron::concurrent::AtomicBuffer&, aeron::util::index_t, aeron::util::index_t) {},
                             1 << 20);
    }
    ASSERT_TRUE(vs.instanceLeadership(kViewTopicId, 0).has_value());
    for (int64_t id = 2; id <= 40; ++id) ASSERT_NO_FATAL_FAILURE(appendChar(id, 1));
    WorkerOp op;
    op.type = WorkerOp::Type::GetViewRow; op.topicId = kViewTopicId; op.partition = 0;
    op.sessionId = 1; op.correlationId = 77; op.nextOffset = 40;
    ASSERT_TRUE(w.tryEnqueueOp(std::move(op)));
    int16_t code = -1;
    int64_t now = 1'000'000'000;
    int passes = 0;
    for (; passes < 100 && code < 0; ++passes, now += 5'000'000) {   // 5 ms per pass; nobody drains the deltas
        w.doWork(now);
        w.drainReturn([&](WorkerReturn r) { if (r.type == WorkerReturn::Type::ViewRowResult) code = r.errorCode; });
    }
    EXPECT_EQ(code, static_cast<int16_t>(pc::ViewRowResponse::kNotReady)) << "a stuck fold ends the read retriably";
    EXPECT_GE(passes, 19) << "held until its 100 ms deadline (5 ms passes)";
    EXPECT_LE(passes, 23);
    EXPECT_EQ(w.viewReadsHeldExpiredForTest(), 1u);
    EXPECT_EQ(w.heldViewReadsForTest(), 0u);
}
