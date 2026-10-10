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

#include <gtest/gtest.h>

#include <rune/node/Worker.h>
#include <rune/node/WorkerOp.h>
#include <rune/node/WorkerPool.h>
#include <rune/node/WorkerViewSet.h>
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

#include <rune/view/PlanCompiler.h>
#include <rune/view/ViewPlan.h>
#include <rune/view/ViewRow.h>
#include <rune/view/ViewTypes.h>

#include <rune/codec/Descriptor.h>
#include <rune/codec/Serialize.h>

#include <rune/protocol/RecordBatch.h>
#include <rune/protocol/client/DataMessages.h>

#include <atomic>
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
    WorkerViewSet vs(channel, "/tmp/rune-views-gvr-absent");
    vs.installView(comp.plan, comp.rowLayout, src, /*persisted*/ false);

    // Fold one row (key 7) so the (view, partition 0) instance exists + is Ready.
    const auto batch = oneUpsert(7, 50);
    auto readRange = [&](int64_t, int64_t) -> rune::storage::LogReadView {
        return rune::storage::LogReadView{batch.data(), batch.size(), 0, 1};
    };
    vs.driveFolds(kSrcTopicId, /*partition*/ 0, readRange, /*bound*/ 1, 0);

    Worker w(/*workerId*/ 0);
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
    WorkerViewSet vs(channel, "/tmp/rune-views-gvr-ok");
    vs.installView(comp.plan, comp.rowLayout, src, /*persisted*/ false);

    const auto batch = oneUpsert(7, 50);
    auto readRange = [&](int64_t, int64_t) -> rune::storage::LogReadView {
        return rune::storage::LogReadView{batch.data(), batch.size(), 0, 1};
    };
    vs.driveFolds(kSrcTopicId, /*partition*/ 0, readRange, /*bound*/ 1, 0);

    Worker w(/*workerId*/ 0);
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
// GREEN (after coLocateViewWithSource): routes to the source owner -> OK + row.
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
    WorkerViewSet vs0(ch0, "/tmp/rune-views-gvr-mw0");
    WorkerViewSet vs1(ch1, "/tmp/rune-views-gvr-mw1");
    vs0.installView(comp.plan, comp.rowLayout, src, /*persisted*/ false);
    vs1.installView(comp.plan, comp.rowLayout, src, /*persisted*/ false);
    WorkerViewSet* vsByWorker[kWorkers] = {&vs0, &vs1};

    const auto batch = oneUpsert(7, 50);
    auto readRange = [&](int64_t, int64_t) -> rune::storage::LogReadView {
        return rune::storage::LogReadView{batch.data(), batch.size(), 0, 1};
    };
    vsByWorker[srcOwner]->driveFolds(kSrcTopicId, /*partition*/ 0, readRange, /*bound*/ 1, 0);

    pool.worker(0).setViewSet(&vs0);
    pool.worker(1).setViewSet(&vs1);

    // Route by viewTopicId (exactly as the gateway's handleGetViewRow does).
    auto query = [&]() -> WorkerReturn {
        WorkerOp op;
        op.type          = WorkerOp::Type::GetViewRow;
        op.topicId       = viewTopicId;
        op.partition     = 0;
        op.sessionId     = 1;
        op.correlationId = 99;
        op.nextOffset    = 7;   // keyU64
        op.hwBound       = 0;   // requestedFingerprint = latest
        EXPECT_TRUE(pool.tryEnqueueOp(viewTopicId, /*partition*/ 0, std::move(op)));
        for (int32_t i = 0; i < kWorkers; ++i) pool.worker(i).doWork(1'000'000LL);
        WorkerReturn captured{};
        int n = 0;
        pool.drainAllReturns([&](WorkerReturn r) { captured = std::move(r); ++n; });
        EXPECT_EQ(n, 1);
        return captured;
    };

    // RED: without co-location, viewTopicId routes to the wrong worker.
    const WorkerReturn pre = query();
    EXPECT_EQ(pre.errorCode, static_cast<int16_t>(pc::ViewRowResponse::kAbsentKey))
        << "pre-fix: a GetViewRow mis-routed to a non-folding worker answers AbsentKey";

    // GREEN: co-locate the view's shard group with its source; routing agrees.
    pool.coLocateViewWithSource(viewTopicId, kSrcTopicId);
    ASSERT_EQ(pool.ownerWorkerId(viewTopicId, 0), srcOwner)
        << "co-location must route viewTopicId to the source owner";
    const WorkerReturn post = query();
    ASSERT_EQ(post.errorCode, static_cast<int16_t>(pc::ViewRowResponse::kOk))
        << "post-fix: GetViewRow lands on the source owner (which folded the row)";
    ASSERT_EQ(post.chunk.size(), comp.rowLayout.rowLen);
    EXPECT_EQ(rv::readSlot(post.chunk.data(), comp.rowLayout, 1).i, 50);
}

// ---------------------------------------------------------------------------
// openView handshake: DefineTopic + CreateView register the view's typed output
// topic; openView with a STALE pinned fingerprint is rejected (SchemaMismatch)
// up front, while the current fingerprint (and 0 = latest) resolve OK.
// ---------------------------------------------------------------------------
namespace {

std::string uniqSuffix() {
    static std::atomic<uint64_t> seq{0};
    const auto ts = std::chrono::steady_clock::now().time_since_epoch().count();
    return std::to_string(seq.fetch_add(1, std::memory_order_relaxed)) + "_" + std::to_string(ts);
}

struct TmpDir {
    std::string path;
    explicit TmpDir(const std::string& tag) {
        path = std::filesystem::temp_directory_path().string() + "/rune_gvr_" + tag + "_" + uniqSuffix();
        std::filesystem::create_directories(path);
    }
    ~TmpDir() { std::error_code ec; std::filesystem::remove_all(path, ec); }
};

PartitionConfig singleNodeCfg(int32_t topicId) {
    PartitionConfig cfg;
    cfg.topicId             = topicId;
    cfg.partitionId         = 0;
    cfg.selfId              = 1;
    cfg.replicaLagMaxMs     = 10'000;
    cfg.replicaLagMaxOffset = 10'000;
    cfg.minIsr              = 1;
    cfg.fetchIntervalNs     = 1'000'000LL;
    cfg.isrCheckIntervalNs  = 1'000'000LL;
    cfg.maxFetchBytes       = 1 << 20;
    cfg.pushOnAppend        = false;
    cfg.cleanupPolicy       = CleanupPolicy::Compact;
    return cfg;
}

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

class GetViewRowOpenRegression : public ::testing::Test {
protected:
    GetViewRowOpenRegression()
        : schemaDir_("schema"), viewsDir_("views")
        , schemaLog_(schemaDir_.path), viewsLog_(viewsDir_.path) {}

    void SetUp() override {
        schemaLog_.recover();
        viewsLog_.recover();
        schemaTransport_ = std::make_unique<NullPartitionTransport>();
        viewsTransport_  = std::make_unique<NullPartitionTransport>();

        schemaReplica_ = std::make_unique<PartitionReplica>(
            singleNodeCfg(kSchemaTopicId), Role::Leader, /*epoch=*/1,
            std::vector<NodeId>{1}, std::vector<NodeId>{1}, &schemaLog_, schemaTransport_.get());
        viewsReplica_ = std::make_unique<PartitionReplica>(
            singleNodeCfg(kViewsTopicId), Role::Leader, /*epoch=*/1,
            std::vector<NodeId>{1}, std::vector<NodeId>{1}, &viewsLog_, viewsTransport_.get());

        schemaRegistry_ = std::make_unique<SchemaRegistry>();
        viewRegistry_   = std::make_unique<ViewRegistry>();

        AdminDeps deps;
        deps.requestCreateTopic = [this](int64_t ticket, int32_t, const std::string&,
                                         const std::string&, uint8_t) -> bool {
            pendingTicket_ = ticket;
            return true;
        };
        deps.pushValidators = [](const RegistryEntry&, int32_t) {};
        deps.respondDefine  = [this](int64_t, int64_t, int32_t topicId, uint16_t, uint16_t err) {
            lastDefineTopicId_ = topicId; lastDefineError_ = err;
        };
        deps.respondGet   = [](int64_t, int64_t, int32_t, uint16_t, const std::vector<uint8_t>&, uint16_t) {};
        deps.respondAlter = [](int64_t, int64_t, int32_t, uint16_t, uint16_t) {};
        deps.respondCreateView = [this](int64_t, int64_t, int32_t viewTopicId,
                                        uint64_t fp, uint16_t err) {
            lastViewTopicId_ = viewTopicId;
            lastViewFinger_  = fp;
            lastViewError_   = err;
        };
        deps.pushViewPlan = [](const ViewPlanUpdateMsg&) {};

        adminWorker_ = std::make_unique<AdminWorker>(
            *schemaRegistry_, schemaReplica_.get(), /*selfId=*/1, std::vector<int32_t>{1},
            std::move(deps), viewsReplica_.get(), viewRegistry_.get());
        adminWorker_->onRole(AdminRoleMsg{/*leader=*/true, /*epoch=*/1, /*leaderId=*/1});
    }

    void pump_() {
        pendingTicket_ = -1;
        int64_t nowNs = 0;
        for (int i = 0; i < 100; ++i) {
            nowNs += 1'000'000LL;
            adminWorker_->doWork(nowNs);
            if (pendingTicket_ >= 0) {
                CreateTopicReplyMsg reply;
                reply.ticket  = pendingTicket_;
                reply.topicId = nextTopicId_++;
                pendingTicket_ = -1;
                adminWorker_->onCreateTopicReply(reply);
            }
        }
    }

    TmpDir schemaDir_, viewsDir_;
    rune::storage::Log schemaLog_, viewsLog_;
    std::unique_ptr<NullPartitionTransport> schemaTransport_, viewsTransport_;
    std::unique_ptr<PartitionReplica>       schemaReplica_, viewsReplica_;
    std::unique_ptr<SchemaRegistry>         schemaRegistry_;
    std::unique_ptr<ViewRegistry>           viewRegistry_;
    std::unique_ptr<AdminWorker>            adminWorker_;

    int32_t  nextTopicId_       = 0;
    int64_t  pendingTicket_     = -1;
    int32_t  lastDefineTopicId_ = -1;
    uint16_t lastDefineError_   = 0xffff;
    int32_t  lastViewTopicId_   = -2;
    uint64_t lastViewFinger_    = 0;
    uint16_t lastViewError_     = 0xffff;
};

TEST_F(GetViewRowOpenRegression, OpenViewFingerprintMismatchRejected) {
    // 1. Source topic.
    DefineTopicMsg d;
    d.session = 1; d.corr = 10; d.partitionCount = 1; d.name = "character";
    d.keyField = "id"; d.validate = false; d.blob = characterBlob();
    adminWorker_->submitDefineTopic(d);
    pump_();
    ASSERT_EQ(lastDefineError_, 0u);
    const int32_t srcTopicId = lastDefineTopicId_;
    ASSERT_GE(srcTopicId, 0);

    // 2. CreateView over it (registers the view's typed output topic).
    CreateViewMsg cv;
    cv.session = 1; cv.corr = 20; cv.persistence = 0;
    cv.viewName = "char_hp"; cv.keyField = "id"; cv.planBlob = charHpPlanBlob(srcTopicId);
    adminWorker_->submitCreateView(cv);
    pump_();
    ASSERT_EQ(lastViewError_, static_cast<uint16_t>(pc::ErrorCode::OK));
    ASSERT_GE(lastViewTopicId_, 0);

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
