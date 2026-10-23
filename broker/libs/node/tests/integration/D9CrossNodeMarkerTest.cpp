// ---------------------------------------------------------------------------
// D9CrossNodeMarkerTest.cpp — cross-node transaction markers (design note
// docs/superpowers/specs/2026-10-04-d9-cross-node-txn-markers-design.md).
//
// Part 1 (gateway unit, *Regression*): the coordinator gateway forwards a marker
// for a partition led by ANOTHER node over the cluster plane (TxnMarkerChannel)
// instead of its own worker op ring; the answer drives the SAME TCS completion
// as a local marker; a routing-image leader change completes in-flight remote
// markers NotLeader (event-driven, no timeout); a remote-origin marker is
// appended by the leader's owning worker and answered to the ORIGIN node.
//
// Part 2 (integration, *Regression*): three in-process nodes over the REAL
// cluster-plane transport (AeronPartitionTransport, one embedded media driver):
// a transaction spanning partitions led by two nodes, coordinated by a third.
//
// Every wait is a condition loop with a hard iteration cap — never a sleep.
// ---------------------------------------------------------------------------
#include <rune/gateway/ClientGateway.h>
#include <rune/gateway/TopicRegistry.h>
#include <rune/metadata/Gpid.h>
#include <rune/metadata/RoutingImage.h>
#include <rune/node/ReplicaRegistry.h>
#include <rune/node/TransactionCoordinationService.h>
#include <rune/node/Worker.h>
#include <rune/node/WorkerOp.h>
#include <rune/node/WorkerPool.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/protocol/client/DataMessages.h>
#include <rune/protocol/client/Protocol.h>
#include <rune/protocol/client/TxnMessages.h>
#include <rune/replication/AeronPartitionTransport.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/replication/ReplicationMessages.h>
#include <rune/storage/Log.h>
#include <rune/storage/ProducerStateManager.h>
#include <rune/transaction/ControlMarker.h>
#include <rune/aeronutil/testing/FreePort.h>
#include <gtest/gtest.h>

#include <rune/node/EmbeddedDriver.h>
#include <rune/node/BrokerNode.h>
#include "support/TestCluster.h"
#include <rune/node/NodeConfig.h>
#include <rune/node/RoleCounter.h>
#include <CncFileReader.h>
#include <Aeron.h>
#include <Context.h>

#include <array>
#include <atomic>
#include <chrono>
#include <filesystem>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

using namespace rune::node;
using rune::replication::NodeId;
using rune::replication::PartitionConfig;
using rune::replication::PartitionReplica;
using rune::replication::Role;
using rune::replication::WriteTxnMarkerMsg;
using rune::replication::WriteTxnMarkerResponseMsg;
using rune::storage::Log;
namespace fs = std::filesystem;
namespace pc = rune::protocol::client;
namespace tx = rune::transaction;

namespace {
constexpr int16_t kNotLeader = static_cast<int16_t>(pc::ErrorCode::NotLeader);

struct TmpDir {
    fs::path path;
    TmpDir() {
        static std::atomic<int> seq{0};
        const auto t = std::chrono::steady_clock::now().time_since_epoch().count();
        path = fs::temp_directory_path() / ("rune_d9_" + std::to_string(::getpid()) + "_" +
                                            std::to_string(seq++) + "_" + std::to_string(t));
        std::error_code ec;
        fs::remove_all(path, ec);
        fs::create_directories(path);
    }
    ~TmpDir() { std::error_code ec; fs::remove_all(path, ec); }
};
class NullTransport final : public rune::replication::PartitionTransport {
public:
    bool sendFetchRequest(const rune::replication::FetchRequestMsg&) override { return true; }
    bool sendFetchResponse(NodeId, const rune::replication::FetchResponseMsg&) override { return true; }
};
PartitionConfig cfg(NodeId self, int32_t topicId, int32_t part) {
    PartitionConfig c; c.topicId = topicId; c.partitionId = part; c.selfId = self; c.minIsr = 1; c.durabilityTier = 1;
    c.fetchIntervalNs = 1'000'000'000LL; c.isrCheckIntervalNs = 1'000'000'000LL; c.pushOnAppend = false;
    c.replicaLagMaxMs = 1'000'000;
    return c;
}
std::shared_ptr<const rune::metadata::RoutingImage> imageWith(int32_t leader, int32_t epoch, int partitions = 1) {
    std::vector<rune::metadata::RoutingImage::BuildEntry> e;
    for (int p = 0; p < partitions; ++p) e.push_back({rune::metadata::gpidOf(0, p), leader, epoch});
    return rune::metadata::RoutingImage::build(e);
}
// Offset of the n-th REAL control batch in the log, or -1.
int64_t controlBatchOffset(Log& log, int nth = 0) {
    const auto raw = log.read(0, 1 << 20);
    std::size_t pos = 0;
    while (pos + static_cast<std::size_t>(rune::protocol::batch::kRecordBatchOverhead) <= raw.size()) {
        rune::protocol::RecordBatchReader rd(raw.data() + pos, raw.size() - pos);
        const int32_t total = rd.totalLength();
        if (total <= 0 || pos + static_cast<std::size_t>(total) > raw.size()) break;
        if (tx::ControlMarker::isControlBatch(raw.data(), pos, raw.size() - pos) && nth-- == 0) return rd.baseOffset();
        pos += static_cast<std::size_t>(total);
    }
    return -1;
}
int controlBatchCount(Log& log) {
    int n = 0;
    while (controlBatchOffset(log, n) >= 0) ++n;
    return n;
}

// Captures what the gateway hands the cluster plane.
struct FakeChannel {
    struct Sent { int32_t to; WriteTxnMarkerMsg m; std::vector<uint8_t> marker; };
    std::vector<Sent> markers;
    std::vector<std::pair<int32_t, WriteTxnMarkerResponseMsg>> responses;
    bool acceptMarkers = true;
    bool acceptResponses = true;
    static bool sendMarker(void* self, int32_t to, const WriteTxnMarkerMsg& m) {
        auto* f = static_cast<FakeChannel*>(self);
        if (!f->acceptMarkers) return false;
        f->markers.push_back({to, m, std::vector<uint8_t>(m.marker.begin(), m.marker.end())});
        f->markers.back().m.marker = {};
        return true;
    }
    static bool sendResponse(void* self, int32_t to, const WriteTxnMarkerResponseMsg& r) {
        auto* f = static_cast<FakeChannel*>(self);
        if (!f->acceptResponses) return false;
        f->responses.emplace_back(to, r);
        return true;
    }
    // #112: cross-node registration verification frames.
    std::vector<std::pair<int32_t, rune::replication::VerifyTxnPartitionMsg>> verifies;
    std::vector<std::pair<int32_t, rune::replication::VerifyTxnPartitionResponseMsg>> verifyResponses;
    bool acceptVerifies = true;
    bool acceptVerifyResponses = true;
    static bool sendVerify(void* self, int32_t to, const rune::replication::VerifyTxnPartitionMsg& m) {
        auto* f = static_cast<FakeChannel*>(self);
        if (!f->acceptVerifies) return false;
        f->verifies.emplace_back(to, m);
        return true;
    }
    static bool sendVerifyResponse(void* self, int32_t to, const rune::replication::VerifyTxnPartitionResponseMsg& r) {
        auto* f = static_cast<FakeChannel*>(self);
        if (!f->acceptVerifyResponses) return false;
        f->verifyResponses.emplace_back(to, r);
        return true;
    }
    rune::gateway::ClientGateway::TxnMarkerChannel channel() {
        return {&FakeChannel::sendMarker, &FakeChannel::sendResponse, this, &FakeChannel::sendVerify,
                &FakeChannel::sendVerifyResponse};
    }
};

// Records the gateway's completion callbacks into the coordinator.
struct FakeTcs final : rune::gateway::ITxnRequestHandler {
    struct Done { int32_t topicId; int32_t partition; int64_t pid; int16_t err; int32_t epoch; bool landed; };
    std::vector<Done> done;
    size_t handleInitProducerId(const uint8_t*, size_t, int64_t, uint8_t*, size_t) override { return 0; }
    size_t handleAddPartitions(const uint8_t*, size_t, int64_t, uint8_t*, size_t) override { return 0; }
    size_t handleAddOffsets(const uint8_t*, size_t, int64_t, uint8_t*, size_t) override { return 0; }
    size_t handleTxnOffsetCommit(const uint8_t*, size_t, int64_t, uint8_t*, size_t) override { return 0; }
    size_t handleEndTxn(const uint8_t*, size_t, int64_t, uint8_t*, size_t) override { return 0; }
    void onControlMarkerRefused(int32_t t, int32_t p, int64_t pid, int16_t err, int32_t epoch) override {
        done.push_back({t, p, pid, err, epoch, false});
    }
    void onControlMarkerLanded(int32_t t, int32_t p, int64_t pid, int32_t epoch) override {
        done.push_back({t, p, pid, 0, epoch, true});
    }
    // #112: the coordinator's side of the registration verification.
    bool    local           = false;
    int32_t coordNode       = -1;
    int16_t verifyCode      = 0;
    bool    clusterVerifies = true;
    int     verifyCalls     = 0;
    int16_t verifyTxnPartition(int64_t, int16_t, int32_t, int32_t, int64_t) override { ++verifyCalls; return verifyCode; }
    bool    coordinatesLocally() const override { return local; }
    int32_t coordinatorNode() const override { return coordNode; }
    bool    clusterVerifiesTxnPartitions() const override { return clusterVerifies; }
};

// One node: topic 0 with 1 partition, its replica (Leader) on worker 0 of a
// real WorkerPool behind a real ClientGateway (the default Legacy routing).
struct GwRig {
    TmpDir d;
    rune::gateway::TopicRegistry topics{{rune::node::TopicSpec{"t", 1}}, d.path.string()};
    NullTransport t;
    ReplicaRegistry reg;
    WorkerPool pool{1, rune::metadata::ShardRouter(1), 1 << 16, 1 << 16, 16, 16};
    rune::gateway::ClientGateway gw = rune::gateway::ClientGateway::makeForTest(
        [](const uint8_t*, std::size_t) -> int64_t { return 1; });
    PartitionReplica* r = nullptr;
    FakeChannel ch;
    FakeTcs tcs;
    explicit GwRig(int32_t selfId, std::vector<NodeId> isr = {}, const std::vector<uint8_t>* seedMarker = nullptr) {
        if (isr.empty()) isr = {selfId};
        if (seedMarker != nullptr) {   // a marker already in the log (a restarted leader)
            int64_t base = -1;
            topics.log(0, 0)->appendNoSyncRestamp(seedMarker->data(), seedMarker->size(), 0, base);
        }
        auto rep = std::make_unique<PartitionReplica>(cfg(selfId, 0, 0), Role::Leader, 1, isr, isr,
                                                      topics.log(0, 0), &t);
        r = rep.get();
        reg.add(0, 0, std::move(rep));
        pool.assignFromRegistry(reg);
        pool.setRegistry(&reg);
        pool.setRunnersStartedForTest(true);
        gw.setNodeIdForTest(selfId);
        gw.setTopicRegistry(&topics);
        gw.setReplicaRegistry(&reg);
        gw.setWorkerPool(&pool);
        gw.setTxnService(&tcs);
        gw.setTxnMarkerChannel(ch.channel());
        runWorker(1);   // ownership applied
    }
    Worker& w() { return pool.worker(0); }
    void runWorker(int64_t now) {
        w().doWork(now);
        w().drainReturn([&](WorkerReturn ret) { gw.handleWorkerReturnForTest(ret); });
    }
    // A follower's fetch at `offset` (its LEO) reaching the leader's worker —
    // the only way the HW of a multi-replica ISR moves.
    void followerFetched(NodeId follower, int64_t offset, int64_t now) {
        WorkerOp op;
        op.type = WorkerOp::Type::FetchRequest; op.topicId = 0; op.partition = 0; op.replica = r;
        op.fetchRequestPayload.leaderEpoch = 1; op.fetchRequestPayload.partition = 0;
        op.fetchRequestPayload.followerId = follower; op.fetchRequestPayload.fetchOffset = offset;
        op.fetchRequestPayload.maxBytes = 1 << 16; op.fetchRequestPayload.topicId = 0;
        op.fetchRequestPayload.lastFetchedEpoch = offset > 0 ? 1 : -1; op.fetchRequestPayload.nowNs = now;
        ASSERT_TRUE(pool.tryEnqueueOp(0, 0, std::move(op)));
        runWorker(now);
    }
    WriteTxnMarkerMsg remote(int32_t origin, int64_t token, int64_t pid, bool commit, int32_t epoch,
                             std::vector<uint8_t>& keep) {
        keep = tx::ControlMarker::build(pid, 0, commit ? tx::ControlType::Commit : tx::ControlType::Abort, epoch);
        WriteTxnMarkerMsg m{};
        m.coordinatorNodeId = origin; m.coordinatorEpoch = epoch; m.token = token; m.topicId = 0; m.partition = 0;
        m.producerId = pid; m.commit = commit ? 1 : 0; m.marker = std::span<const uint8_t>(keep);
        m.leaderEpoch = 1;   // the rig replica's leader epoch
        return m;
    }
    // A transactional data batch of `pid` through the owning worker's op ring.
    void produceTxnData(int64_t pid, int32_t seq, int64_t corr, int64_t now) {
        rune::protocol::RecordBatchBuilder b(0, 1000);
        b.setTransactional(true); b.setProducerId(pid); b.setProducerEpoch(0); b.setBaseSequence(seq);
        b.append("k", "txn", 1000);
        WorkerOp op;
        op.type = WorkerOp::Type::Produce; op.topicId = 0; op.partition = 0; op.replica = r; op.logPtr = r->log();
        op.sessionId = 1; op.correlationId = corr; op.recordCount = 1; op.durabilityTier = 1; op.recordBatchBytes = b.build();
        op.producerId = pid; op.producerEpoch = 0; op.baseSequence = seq;
        op.targetReplicaId = r->registryId(); op.incarnation = r->incarnation();
        ASSERT_TRUE(w().tryEnqueueOp(std::move(op)));
        runWorker(now);
    }
    void image(int32_t leader, int32_t epoch) {
        gw.setRoutingImageSnapshot(imageWith(leader, epoch));
        (void)gw.pumpTxnMarkersForTest(1);   // the leader-change step runs in the marker pump
    }
    int64_t marker(int64_t pid, bool commit, int32_t epoch = 3) {
        const auto m = tx::ControlMarker::build(pid, 0, commit ? tx::ControlType::Commit : tx::ControlType::Abort, epoch);
        return gw.appendControlRecord(0, 0, std::span<const uint8_t>(m), pid, commit, epoch);
    }
};
} // namespace

// ============================================================================
// Part 1 — gateway unit.
// ============================================================================

// Coordinator node 1, partition led by node 2: the marker goes to node 2 over
// the cluster plane, never onto this node's worker op ring.
TEST(D9RemoteMarkerRegression, RemoteLeaderMarkerIsSentOverTheClusterPlaneNotTheOpRing) {
    GwRig R(/*self=*/1);
    R.gw.setRoutingImageSnapshot(imageWith(/*leader=*/2, /*epoch=*/5));
    ASSERT_EQ(R.marker(/*pid=*/77, /*commit=*/true), rune::gateway::ClientGateway::kControlRecordAsync);
    for (int i = 0; i < 8; ++i) R.runWorker(10 + i);
    EXPECT_EQ(R.r->log()->logEndOffset(), 0) << "the marker was appended locally (op ring), not forwarded";
    ASSERT_EQ(R.ch.markers.size(), 1u) << "exactly one WriteTxnMarker to the leader";
    const auto& s = R.ch.markers[0];
    EXPECT_EQ(s.to, 2);
    EXPECT_EQ(s.m.coordinatorNodeId, 1);
    EXPECT_EQ(s.m.coordinatorEpoch, 3);
    EXPECT_EQ(s.m.topicId, 0); EXPECT_EQ(s.m.partition, 0);
    EXPECT_EQ(s.m.producerId, 77); EXPECT_EQ(s.m.commit, 1);
    EXPECT_EQ(s.marker.size(), tx::ControlMarker::ENCODED_SIZE);
    EXPECT_EQ(R.gw.markersInFlightForTest(), 1u) << "in flight until the leader answers";
    EXPECT_TRUE(R.tcs.done.empty()) << "nothing completes before the answer";
}

TEST(D9RemoteMarkerRegression, RemoteAnswerDrivesLandedAndRefusedLikeALocalMarker) {
    GwRig R(1);
    R.gw.setRoutingImageSnapshot(imageWith(2, 5));
    ASSERT_EQ(R.marker(77, true), rune::gateway::ClientGateway::kControlRecordAsync);
    ASSERT_EQ(R.marker(78, false), rune::gateway::ClientGateway::kControlRecordAsync);
    ASSERT_EQ(R.ch.markers.size(), 2u);
    WriteTxnMarkerResponseMsg ok{};
    ok.token = R.ch.markers[0].m.token; ok.topicId = 0; ok.partition = 0; ok.producerId = 77;
    ok.coordinatorEpoch = 3; ok.errorCode = 0; ok.offset = 41;
    R.gw.onRemoteControlRecordResponse(ok);
    ASSERT_EQ(R.tcs.done.size(), 1u);
    EXPECT_TRUE(R.tcs.done[0].landed);
    EXPECT_EQ(R.tcs.done[0].pid, 77); EXPECT_EQ(R.tcs.done[0].epoch, 3);
    WriteTxnMarkerResponseMsg no = ok;
    no.token = R.ch.markers[1].m.token; no.producerId = 78; no.errorCode = kNotLeader; no.offset = -1;
    R.gw.onRemoteControlRecordResponse(no);
    ASSERT_EQ(R.tcs.done.size(), 2u);
    EXPECT_FALSE(R.tcs.done[1].landed);
    EXPECT_EQ(R.tcs.done[1].err, kNotLeader); EXPECT_EQ(R.tcs.done[1].pid, 78);
    EXPECT_EQ(R.gw.markersInFlightForTest(), 0u);
    R.gw.onRemoteControlRecordResponse(ok);   // a duplicate / late answer is ignored
    EXPECT_EQ(R.tcs.done.size(), 2u);
}

// Event-driven convergence: the routing image moves the partition's leadership
// → every remote marker in flight to the old leader completes NotLeader at once
// (the TCS retry then re-routes it).  An image with the SAME leader/epoch does not.
TEST(D9RemoteMarkerRegression, LeaderChangeCompletesInFlightRemoteMarkersNotLeader) {
    GwRig R(1);
    R.gw.setRoutingImageSnapshot(imageWith(2, 5));
    ASSERT_EQ(R.marker(77, true), rune::gateway::ClientGateway::kControlRecordAsync);
    R.image(2, 5);   // republished, unchanged
    EXPECT_TRUE(R.tcs.done.empty()) << "an unchanged leader must not fail the marker";
    R.image(3, 6);   // leadership moved to node 3
    ASSERT_EQ(R.tcs.done.size(), 1u) << "the in-flight remote marker completes on the leader change";
    EXPECT_FALSE(R.tcs.done[0].landed);
    EXPECT_EQ(R.tcs.done[0].err, kNotLeader);
    EXPECT_EQ(R.tcs.done[0].epoch, 3);
    EXPECT_EQ(R.gw.markersInFlightForTest(), 0u);
    // The re-dispatch (TCS retry) now resolves the NEW leader.
    ASSERT_EQ(R.marker(77, true), rune::gateway::ClientGateway::kControlRecordAsync);
    ASSERT_EQ(R.ch.markers.size(), 2u);
    EXPECT_EQ(R.ch.markers[1].to, 3);
}

// Same leader node, new leader epoch (it was re-elected / restarted): a request
// it may have lost is failed too, so the (idempotent) marker is re-sent.
TEST(D9RemoteMarkerRegression, LeaderEpochBumpCompletesInFlightRemoteMarkersNotLeader) {
    GwRig R(1);
    R.gw.setRoutingImageSnapshot(imageWith(2, 5));
    ASSERT_EQ(R.marker(77, true), rune::gateway::ClientGateway::kControlRecordAsync);
    R.image(2, 6);
    ASSERT_EQ(R.tcs.done.size(), 1u);
    EXPECT_EQ(R.tcs.done[0].err, kNotLeader);
}

// Leadership moving TO this node: the remote marker fails NotLeader and the
// retry takes the local (op ring) path.
TEST(D9RemoteMarkerRegression, LeaderMovingToTheCoordinatorReroutesLocally) {
    GwRig R(1);
    R.gw.setRoutingImageSnapshot(imageWith(2, 5));
    ASSERT_EQ(R.marker(77, true), rune::gateway::ClientGateway::kControlRecordAsync);
    R.image(1, 6);
    ASSERT_EQ(R.tcs.done.size(), 1u);
    EXPECT_EQ(R.tcs.done[0].err, kNotLeader);
    ASSERT_EQ(R.marker(77, true), rune::gateway::ClientGateway::kControlRecordAsync);
    for (int i = 0; i < 16 && R.gw.markersInFlightForTest() > 0; ++i) R.runWorker(10 + i);
    EXPECT_EQ(R.ch.markers.size(), 1u) << "the retry must not be forwarded again";
    EXPECT_EQ(controlBatchOffset(*R.r->log()), 0) << "the retry landed locally";
    ASSERT_EQ(R.tcs.done.size(), 2u);
    EXPECT_TRUE(R.tcs.done[1].landed);
}

// A full outbound ring never drops the marker: it waits in the FIFO queue and is
// sent by a later doWork.
TEST(D9RemoteMarkerRegression, BackPressuredRemoteMarkerIsQueuedAndSentLater) {
    GwRig R(1);
    R.gw.setRoutingImageSnapshot(imageWith(2, 5));
    R.ch.acceptMarkers = false;
    ASSERT_EQ(R.marker(77, true), rune::gateway::ClientGateway::kControlRecordAsync);
    EXPECT_EQ(R.gw.markersQueuedForTest(), 1u);
    R.ch.acceptMarkers = true;
    (void)R.gw.pumpTxnMarkersForTest(100);
    EXPECT_EQ(R.gw.markersQueuedForTest(), 0u);
    ASSERT_EQ(R.ch.markers.size(), 1u);
    EXPECT_EQ(R.ch.markers[0].to, 2);
}

// Leader side: a remote-origin marker is appended by the owning worker and its
// result answered to the ORIGIN node — never to this node's own TCS.
TEST(D9RemoteMarkerRegression, RemoteOriginMarkerIsAppendedAndAnsweredToTheOrigin) {
    GwRig R(/*self=*/2);
    R.gw.setRoutingImageSnapshot(imageWith(2, 5));
    const auto bytes = tx::ControlMarker::build(77, 0, tx::ControlType::Commit, 3);
    WriteTxnMarkerMsg m{};
    m.coordinatorNodeId = 1; m.coordinatorEpoch = 3; m.token = 901; m.topicId = 0; m.partition = 0;
    m.producerId = 77; m.commit = 1; m.marker = std::span<const uint8_t>(bytes);
    R.gw.appendRemoteControlRecord(m);
    EXPECT_EQ(R.r->log()->logEndOffset(), 0) << "appended by the owning worker, not inline";
    for (int i = 0; i < 16 && R.gw.markersInFlightForTest() > 0; ++i) R.runWorker(10 + i);
    EXPECT_EQ(controlBatchOffset(*R.r->log()), 0) << "the remote marker landed as a control batch";
    ASSERT_EQ(R.ch.responses.size(), 1u);
    const auto& [to, r] = R.ch.responses[0];
    EXPECT_EQ(to, 1);
    EXPECT_EQ(r.token, 901); EXPECT_EQ(r.producerId, 77); EXPECT_EQ(r.coordinatorEpoch, 3);
    EXPECT_EQ(r.errorCode, 0); EXPECT_EQ(r.offset, 0);
    EXPECT_TRUE(R.tcs.done.empty()) << "a remote-origin marker must not complete on the LOCAL coordinator";
}

// Leader side, stale routing at the coordinator: this node does not lead the
// partition → NotLeader at once, nothing appended.
// Review MEDIUM-5: the leader's own routing image may lag its replica role
// (the coordinator saw the election first).  The worker's role gate is the
// authority — a Leader replica takes the marker whatever the local image says.
TEST(D9RemoteMarkerRegression, RemoteMarkerOnALeaderWithAStaleRoutingImageIsAppended) {
    GwRig R(/*self=*/2);
    R.image(/*leader=*/3, 6);                                // stale here: the replica IS Leader
    std::vector<uint8_t> keep;
    R.gw.appendRemoteControlRecord(R.remote(1, 902, 77, true, 3, keep));
    for (int i = 0; i < 16 && R.gw.markersInFlightForTest() > 0; ++i) R.runWorker(10 + i);
    EXPECT_EQ(controlBatchOffset(*R.r->log()), 0) << "the Leader replica took the marker";
    ASSERT_EQ(R.ch.responses.size(), 1u);
    EXPECT_EQ(R.ch.responses[0].second.token, 902);
    EXPECT_EQ(R.ch.responses[0].second.errorCode, 0);
}

// A node without the partition at all (no log) answers NotLeader at once.
TEST(D9RemoteMarkerRegression, RemoteMarkerForAnUnknownPartitionIsAnsweredNotLeaderAtOnce) {
    GwRig R(/*self=*/2);
    std::vector<uint8_t> keep;
    auto m = R.remote(1, 907, 77, true, 3, keep);
    m.partition = 5;                                         // topic 0 has one partition here
    R.gw.appendRemoteControlRecord(m);
    ASSERT_EQ(R.ch.responses.size(), 1u);
    EXPECT_EQ(R.ch.responses[0].second.token, 907);
    EXPECT_EQ(R.ch.responses[0].second.errorCode, kNotLeader);
    EXPECT_EQ(R.gw.markersInFlightForTest(), 0u);
}

// Leader side: the worker refuses (the replica was demoted) → NotLeader to the origin.
TEST(D9RemoteMarkerRegression, RemoteMarkerRefusedByTheWorkerIsAnsweredToTheOrigin) {
    GwRig R(2);
    R.gw.setRoutingImageSnapshot(imageWith(2, 5));
    WorkerOp op;
    op.type = WorkerOp::Type::BecomeFollower; op.topicId = 0; op.partition = 0; op.replica = R.r;
    op.logPtr = R.r->log(); op.roleEpoch = 6; op.roleLeaderId = 3;
    op.targetReplicaId = R.r->registryId(); op.incarnation = R.r->incarnation();
    ASSERT_TRUE(R.w().tryEnqueueOp(std::move(op)));
    R.runWorker(5);
    ASSERT_EQ(R.r->role(), Role::Follower);
    const auto bytes = tx::ControlMarker::build(77, 0, tx::ControlType::Commit, 3);
    WriteTxnMarkerMsg m{};
    m.coordinatorNodeId = 1; m.coordinatorEpoch = 3; m.token = 903; m.topicId = 0; m.partition = 0;
    m.producerId = 77; m.commit = 1; m.marker = std::span<const uint8_t>(bytes);
    R.gw.appendRemoteControlRecord(m);
    for (int i = 0; i < 16 && R.gw.markersInFlightForTest() > 0; ++i) R.runWorker(10 + i);
    ASSERT_EQ(R.ch.responses.size(), 1u);
    EXPECT_EQ(R.ch.responses[0].second.token, 903);
    EXPECT_EQ(R.ch.responses[0].second.errorCode, kNotLeader);
    EXPECT_TRUE(R.tcs.done.empty());
}

// Leader side: an answer the outbound ring cannot take is kept and re-sent.
TEST(D9RemoteMarkerRegression, BackPressuredRemoteAnswerIsKeptAndResent) {
    GwRig R(2);
    R.gw.setRoutingImageSnapshot(imageWith(2, 5));
    R.ch.acceptResponses = false;
    const auto bytes = tx::ControlMarker::build(77, 0, tx::ControlType::Commit, 3);
    WriteTxnMarkerMsg m{};
    m.coordinatorNodeId = 1; m.coordinatorEpoch = 3; m.token = 904; m.topicId = 0; m.partition = 0;
    m.producerId = 77; m.commit = 1; m.marker = std::span<const uint8_t>(bytes);
    R.gw.appendRemoteControlRecord(m);
    for (int i = 0; i < 16 && R.gw.markersInFlightForTest() > 0; ++i) R.runWorker(10 + i);
    EXPECT_TRUE(R.ch.responses.empty());
    R.ch.acceptResponses = true;
    (void)R.gw.pumpTxnMarkersForTest(100);
    ASSERT_EQ(R.ch.responses.size(), 1u);
    EXPECT_EQ(R.ch.responses[0].second.token, 904);
    EXPECT_EQ(R.ch.responses[0].second.errorCode, 0);
}

// The coordinator's unload purge (F15 m3) must not drop a marker this node
// appends on BEHALF of another coordinator (a different epoch space).
TEST(D9RemoteMarkerRegression, CoordinatorPurgeKeepsRemoteOriginMarkers) {
    GwRig R(2);
    R.gw.setRoutingImageSnapshot(imageWith(2, 5));
    // Fill the op ring so the remote-origin marker waits in the queue.
    const auto bytes = tx::ControlMarker::build(77, 0, tx::ControlType::Commit, 3);
    WriteTxnMarkerMsg m{};
    m.coordinatorNodeId = 1; m.coordinatorEpoch = 3; m.token = 905; m.topicId = 0; m.partition = 0;
    m.producerId = 77; m.commit = 1; m.marker = std::span<const uint8_t>(bytes);
    std::size_t filled = 0;
    while (filled < 100000) {
        WorkerOp op;
        op.type = WorkerOp::Type::AppendControlRecord; op.topicId = 0; op.partition = 0; op.replica = R.r;
        op.logPtr = R.r->log(); op.correlationId = 1'000'000 + static_cast<int64_t>(filled);
        op.recordBatchBytes = bytes;
        op.targetReplicaId = R.r->registryId(); op.incarnation = R.r->incarnation();
        if (!R.w().tryEnqueueOp(std::move(op))) break;
        ++filled;
    }
    ASSERT_GT(filled, 0u);
    R.gw.appendRemoteControlRecord(m);
    ASSERT_EQ(R.gw.markersQueuedForTest(), 1u) << "precondition: the remote-origin marker waits in the queue";
    EXPECT_EQ(R.gw.purgeQueuedMarkers(/*olderThan=*/1000), 0u) << "a remote-origin marker was purged";
    EXPECT_EQ(R.gw.markersQueuedForTest(), 1u);
}

// Review MAJOR-1: no timer ever completes a remote marker — an unanswered one
// keeps its token for as long as it takes, and a late answer completes it.
TEST(D9RemoteMarkerRegression, UnansweredRemoteMarkerIsNeverCompletedByTimeAndALateAnswerLands) {
    GwRig R(1);
    R.image(2, 5);
    ASSERT_EQ(R.marker(77, true), rune::gateway::ClientGateway::kControlRecordAsync);
    for (int64_t now = 1'000'000'000; now <= 600'000'000'000; now += 1'000'000'000) (void)R.gw.pumpTxnMarkersForTest(now);
    EXPECT_TRUE(R.tcs.done.empty()) << "a remote marker was completed by a timer";
    ASSERT_EQ(R.gw.remoteMarkersInFlightForTest(), 1u);
    ASSERT_EQ(R.ch.markers.size(), 1u) << "no timer re-send either";
    WriteTxnMarkerResponseMsg ok{};
    ok.token = R.ch.markers[0].m.token; ok.topicId = 0; ok.partition = 0; ok.producerId = 77;
    ok.coordinatorEpoch = 3; ok.errorCode = 0; ok.offset = 9;
    R.gw.onRemoteControlRecordResponse(ok);
    ASSERT_EQ(R.tcs.done.size(), 1u);
    EXPECT_TRUE(R.tcs.done[0].landed) << "the late answer completes the marker";
}

// Review MAJOR-1: the cluster plane's link to a peer comes back (down→up edge)
// → every remote marker in flight to it is re-sent with the SAME token; an
// answer to either copy completes it once.  Markers to other peers are not.
TEST(D9RemoteMarkerRegression, PlaneReconnectResendsInFlightRemoteMarkersWithTheSameToken) {
    GwRig R(1);
    std::vector<rune::metadata::RoutingImage::BuildEntry> e{{rune::metadata::gpidOf(0, 0), 2, 5}};
    R.gw.setRoutingImageSnapshot(rune::metadata::RoutingImage::build(e));
    ASSERT_EQ(R.marker(77, true), rune::gateway::ClientGateway::kControlRecordAsync);
    ASSERT_EQ(R.ch.markers.size(), 1u);
    R.gw.onPlaneReconnect(/*peer=*/3);                       // another peer: nothing to re-send
    (void)R.gw.pumpTxnMarkersForTest(10);
    EXPECT_EQ(R.ch.markers.size(), 1u);
    R.gw.onPlaneReconnect(/*peer=*/2);
    (void)R.gw.pumpTxnMarkersForTest(20);
    ASSERT_EQ(R.ch.markers.size(), 2u) << "the in-flight marker was re-sent on the up edge";
    EXPECT_EQ(R.ch.markers[1].to, 2);
    EXPECT_EQ(R.ch.markers[1].m.token, R.ch.markers[0].m.token) << "same token";
    EXPECT_EQ(R.ch.markers[1].marker, R.ch.markers[0].marker);
    R.gw.onPlaneReconnect(/*peer=*/-1);                      // a new inbound image (peer unknown): re-send all
    (void)R.gw.pumpTxnMarkersForTest(30);
    EXPECT_EQ(R.ch.markers.size(), 3u);
    WriteTxnMarkerResponseMsg ok{};
    ok.token = R.ch.markers[0].m.token; ok.topicId = 0; ok.partition = 0; ok.producerId = 77;
    ok.coordinatorEpoch = 3; ok.errorCode = 0; ok.offset = 0;
    R.gw.onRemoteControlRecordResponse(ok);
    R.gw.onRemoteControlRecordResponse(ok);                  // the copy's answer
    ASSERT_EQ(R.tcs.done.size(), 1u) << "completed exactly once";
    EXPECT_TRUE(R.tcs.done[0].landed);
    R.gw.onPlaneReconnect(2);
    (void)R.gw.pumpTxnMarkersForTest(40);
    EXPECT_EQ(R.ch.markers.size(), 3u) << "a completed marker is never re-sent";
}

// A re-send whose outbound ring is full waits in the FIFO queue (never lost).
TEST(D9RemoteMarkerRegression, BackPressuredResendIsQueuedAndSentLater) {
    GwRig R(1);
    R.image(2, 5);
    ASSERT_EQ(R.marker(77, true), rune::gateway::ClientGateway::kControlRecordAsync);
    R.ch.acceptMarkers = false;
    R.gw.onPlaneReconnect(2);
    (void)R.gw.pumpTxnMarkersForTest(10);
    EXPECT_EQ(R.gw.markersQueuedForTest(), 1u);
    R.ch.acceptMarkers = true;
    (void)R.gw.pumpTxnMarkersForTest(20);
    EXPECT_EQ(R.gw.markersQueuedForTest(), 0u);
    ASSERT_EQ(R.ch.markers.size(), 2u);
    EXPECT_EQ(R.ch.markers[1].m.token, R.ch.markers[0].m.token);
}

// Leader side, MAJOR-1: a re-sent copy of a marker still in flight here (same
// origin, token, epoch) is not appended twice — its answer is still owed.
TEST(D9RemoteMarkerRegression, DuplicateRemoteMarkerWhileInFlightIsAppendedOnce) {
    GwRig R(/*self=*/2, /*isr=*/{2, 9});                     // the HW waits for follower 9
    R.image(2, 5);
    std::vector<uint8_t> keep;
    const auto m = R.remote(1, 908, 77, true, 3, keep);
    R.gw.appendRemoteControlRecord(m);
    for (int i = 0; i < 8; ++i) R.runWorker(10 + i);
    R.gw.appendRemoteControlRecord(m);                       // the re-sent copy
    for (int i = 0; i < 8; ++i) R.runWorker(20 + i);
    EXPECT_EQ(controlBatchCount(*R.r->log()), 1) << "the in-flight duplicate was appended again";
    R.followerFetched(9, R.r->log()->logEndOffset(), 30);
    for (int i = 0; i < 8; ++i) R.runWorker(40 + i);
    ASSERT_EQ(R.ch.responses.size(), 1u) << "one answer for the marker";
    EXPECT_EQ(R.ch.responses[0].second.token, 908);
    EXPECT_EQ(R.ch.responses[0].second.errorCode, 0);
}

// Review MEDIUM-1: an answer carrying the right token but another coordinator
// epoch (a previous coordinator load / incarnation) never completes the marker.
TEST(D9RemoteMarkerRegression, AnswerWithAnotherCoordinatorEpochIsIgnored) {
    GwRig R(1);
    R.image(2, 5);
    ASSERT_EQ(R.marker(77, true, /*epoch=*/3), rune::gateway::ClientGateway::kControlRecordAsync);
    WriteTxnMarkerResponseMsg stale{};
    stale.token = R.ch.markers[0].m.token; stale.topicId = 0; stale.partition = 0; stale.producerId = 77;
    stale.coordinatorEpoch = 2; stale.errorCode = 0; stale.offset = 0;
    R.gw.onRemoteControlRecordResponse(stale);
    EXPECT_TRUE(R.tcs.done.empty()) << "a stale-epoch answer completed the current marker";
    EXPECT_EQ(R.gw.remoteMarkersInFlightForTest(), 1u);
}

// Review MEDIUM-1: tokens carry a per-boot incarnation in the high bits, so a
// pre-restart answer can never match a post-restart marker.
TEST(D9RemoteMarkerRegression, MarkerTokensCarryABootIncarnation) {
    GwRig A(1);
    GwRig B(1);
    A.image(2, 5);
    B.image(2, 5);
    ASSERT_EQ(A.marker(77, true), rune::gateway::ClientGateway::kControlRecordAsync);
    ASSERT_EQ(B.marker(77, true), rune::gateway::ClientGateway::kControlRecordAsync);
    const int64_t ta = A.ch.markers[0].m.token;
    const int64_t tb = B.ch.markers[0].m.token;
    EXPECT_GT(ta, 0); EXPECT_GT(tb, 0);
    EXPECT_NE(ta >> 32, 0) << "no incarnation in the high bits";
    EXPECT_NE(ta, tb) << "two gateway incarnations issued the same first token";
}

// Review MAJOR-3: a remote marker is answered only once the partition HW passes
// it (Kafka: WriteTxnMarkers with acks=-1), never at the leader append.
TEST(D9RemoteMarkerRegression, RemoteMarkerIsAnsweredOnlyAfterTheHwPassesIt) {
    GwRig R(/*self=*/2, /*isr=*/{2, 9});
    R.image(2, 5);
    std::vector<uint8_t> keep;
    R.gw.appendRemoteControlRecord(R.remote(1, 909, 77, true, 3, keep));
    for (int i = 0; i < 16; ++i) R.runWorker(10 + i);
    ASSERT_EQ(controlBatchOffset(*R.r->log()), 0) << "precondition: appended";
    EXPECT_TRUE(R.ch.responses.empty()) << "answered before the ISR has the marker";
    R.followerFetched(9, 1, 30);
    for (int i = 0; i < 8 && R.ch.responses.empty(); ++i) R.runWorker(40 + i);
    ASSERT_EQ(R.ch.responses.size(), 1u);
    EXPECT_EQ(R.ch.responses[0].second.errorCode, 0);
    EXPECT_EQ(R.ch.responses[0].second.offset, 0);
}

// Review MAJOR-3, local path: the coordinator counts the marker as landed only
// once the HW passes it.
TEST(D9RemoteMarkerRegression, LocalMarkerLandsOnlyAfterTheHwPassesIt) {
    GwRig R(/*self=*/1, /*isr=*/{1, 9});
    ASSERT_EQ(R.marker(77, true), rune::gateway::ClientGateway::kControlRecordAsync);
    for (int i = 0; i < 16; ++i) R.runWorker(10 + i);
    ASSERT_EQ(controlBatchOffset(*R.r->log()), 0);
    EXPECT_TRUE(R.tcs.done.empty()) << "landed before the ISR has the marker";
    R.followerFetched(9, 1, 30);
    for (int i = 0; i < 8 && R.tcs.done.empty(); ++i) R.runWorker(40 + i);
    ASSERT_EQ(R.tcs.done.size(), 1u);
    EXPECT_TRUE(R.tcs.done[0].landed);
}

// Review MAJOR-3: the leader is demoted while the marker waits for the HW
// (it may be truncated) → NotLeader to the origin, the coordinator re-drives it.
TEST(D9RemoteMarkerRegression, DemotionWhileTheMarkerWaitsForTheHwAnswersNotLeader) {
    GwRig R(/*self=*/2, /*isr=*/{2, 9});
    R.image(2, 5);
    std::vector<uint8_t> keep;
    R.gw.appendRemoteControlRecord(R.remote(1, 910, 77, true, 3, keep));
    for (int i = 0; i < 8; ++i) R.runWorker(10 + i);
    ASSERT_TRUE(R.ch.responses.empty());
    WorkerOp op;
    op.type = WorkerOp::Type::BecomeFollower; op.topicId = 0; op.partition = 0; op.replica = R.r;
    op.logPtr = R.r->log(); op.roleEpoch = 6; op.roleLeaderId = 9;
    op.targetReplicaId = R.r->registryId(); op.incarnation = R.r->incarnation();
    ASSERT_TRUE(R.w().tryEnqueueOp(std::move(op)));
    for (int i = 0; i < 8 && R.ch.responses.empty(); ++i) R.runWorker(20 + i);
    ASSERT_EQ(R.ch.responses.size(), 1u);
    EXPECT_EQ(R.ch.responses[0].second.token, 910);
    EXPECT_EQ(R.ch.responses[0].second.errorCode, kNotLeader);
}

// Review MAJOR-2 (Kafka TRANSACTION_COORDINATOR_FENCED): a marker from a deposed
// coordinator (lower coordinator epoch than the last marker applied for that
// producer here) is refused, NOT appended — it would otherwise close the
// producer's NEXT transaction.  Two coordinator nodes (1 = new, 3 = deposed).
TEST(D9RemoteMarkerRegression, StaleCoordinatorEpochMarkerAfterTheNextTxnDataIsFenced) {
    GwRig R(/*self=*/2);
    R.image(2, 5);
    std::vector<uint8_t> k1, k2;
    R.gw.appendRemoteControlRecord(R.remote(/*origin=*/1, 911, 77, true, /*epoch=*/5, k1));   // T1 COMMIT, new coordinator
    for (int i = 0; i < 16 && R.gw.markersInFlightForTest() > 0; ++i) R.runWorker(10 + i);
    ASSERT_EQ(R.ch.responses.size(), 1u);
    ASSERT_EQ(R.ch.responses[0].second.errorCode, 0);
    // T2's data (same producer) lands on the partition.
    rune::protocol::RecordBatchBuilder b(0, 1000);
    b.setTransactional(true); b.setProducerId(77); b.setProducerEpoch(0); b.setBaseSequence(0);
    b.append("k", "t2", 1000);
    WorkerOp op;
    op.type = WorkerOp::Type::Produce; op.topicId = 0; op.partition = 0; op.replica = R.r; op.logPtr = R.r->log();
    op.sessionId = 1; op.correlationId = 50; op.recordCount = 1; op.durabilityTier = 1; op.recordBatchBytes = b.build();
    op.producerId = 77; op.producerEpoch = 0; op.baseSequence = 0;
    op.targetReplicaId = R.r->registryId(); op.incarnation = R.r->incarnation();
    ASSERT_TRUE(R.w().tryEnqueueOp(std::move(op)));
    R.runWorker(20);
    const int64_t leo = R.r->log()->logEndOffset();
    ASSERT_EQ(leo, 2) << "precondition: T2's batch appended after T1's marker";
    ASSERT_EQ(R.w().rcOpenTxnsForTest(0, 0), 1u) << "precondition: T2 is open";
    // The deposed coordinator's stale COMMIT(T1) arrives late.
    R.gw.appendRemoteControlRecord(R.remote(/*origin=*/3, 912, 77, true, /*epoch=*/4, k2));
    for (int i = 0; i < 16 && R.gw.markersInFlightForTest() > 0; ++i) R.runWorker(30 + i);
    EXPECT_EQ(R.r->log()->logEndOffset(), leo) << "the stale marker was appended";
    EXPECT_EQ(R.w().rcOpenTxnsForTest(0, 0), 1u) << "the stale marker closed T2";
    ASSERT_EQ(R.ch.responses.size(), 2u);
    EXPECT_EQ(R.ch.responses[1].first, 3);
    EXPECT_EQ(R.ch.responses[1].second.errorCode, tx::kMarkerCoordinatorFencedError);
}

// MAJOR-2: the fence is durable — rebuilt from the log's marker batches.
// (a) ProducerStateManager::rebuildFromLog (gateway / inline rebuild).
TEST(D9RemoteMarkerRegression, CoordinatorFenceIsRebuiltFromTheLogByThePsmRebuild) {
    TmpDir d;
    Log log(d.path.string(), 64LL * 1024 * 1024);
    log.recover();
    const auto m = tx::ControlMarker::build(77, 0, tx::ControlType::Commit, /*coordinatorEpoch=*/5);
    int64_t base = -1;
    log.appendNoSyncRestamp(m.data(), m.size(), 0, base);
    rune::storage::ProducerStateManager psm;
    ASSERT_TRUE(psm.rebuildFromLog(0, log, log.logEndOffset(), 0));
    EXPECT_TRUE(psm.markerCoordinatorFenced(0, 77, 0, 4)) << "the rebuilt state lost the coordinator fence";
    EXPECT_FALSE(psm.markerCoordinatorFenced(0, 77, 0, 5));
    EXPECT_FALSE(psm.markerCoordinatorFenced(0, 78, 0, 0)) << "another producer is not fenced";
}

// (b) the owning worker's RC build (a restarted / newly assigned leader).
TEST(D9RemoteMarkerRegression, CoordinatorFenceIsRebuiltFromTheLogByTheWorkerRcBuild) {
    const auto seed = tx::ControlMarker::build(77, 0, tx::ControlType::Commit, /*coordinatorEpoch=*/5);
    GwRig R(/*self=*/2, {}, &seed);
    for (int i = 0; i < 16; ++i) R.runWorker(2 + i);   // the RC / producer-state build
    std::vector<uint8_t> keep;
    R.gw.appendRemoteControlRecord(R.remote(/*origin=*/3, 913, 77, true, /*epoch=*/4, keep));
    for (int i = 0; i < 16 && R.gw.markersInFlightForTest() > 0; ++i) R.runWorker(30 + i);
    EXPECT_EQ(R.r->log()->logEndOffset(), 1) << "the stale marker was appended after a rebuild";
    ASSERT_EQ(R.ch.responses.size(), 1u);
    EXPECT_EQ(R.ch.responses[0].second.errorCode, tx::kMarkerCoordinatorFencedError);
}

// Re-review NEW MAJOR-1: a re-sent copy that trails the first ANSWER (same
// token, same coordinator epoch — the fence cannot tell) must be re-answered
// from memory, never appended: appended after T2's data it would close T2.
TEST(D9RemoteMarkerRegression, CopyTrailingTheAnswerIsReAnsweredNotAppendedAfterTheNextTxnData) {
    GwRig R(/*self=*/2);
    R.image(2, 5);
    std::vector<uint8_t> keep;
    const auto a1 = R.remote(/*origin=*/1, /*token=*/(7LL << 32) | 21, 77, true, /*epoch=*/5, keep);
    R.gw.appendRemoteControlRecord(a1);                       // copy A1 → appended, answered R1
    for (int i = 0; i < 16 && R.gw.markersInFlightForTest() > 0; ++i) R.runWorker(10 + i);
    ASSERT_EQ(R.ch.responses.size(), 1u);
    ASSERT_EQ(R.ch.responses[0].second.errorCode, 0);
    const int64_t markerOffset = R.ch.responses[0].second.offset;
    R.produceTxnData(77, 0, 50, 20);                          // T2 begins on this partition
    const int64_t leo = R.r->log()->logEndOffset();
    ASSERT_EQ(leo, 2);
    ASSERT_EQ(R.w().rcOpenTxnsForTest(0, 0), 1u) << "precondition: T2 is open";
    R.gw.appendRemoteControlRecord(a1);                       // copy A2 of the same token, late
    for (int i = 0; i < 16 && R.gw.markersInFlightForTest() > 0; ++i) R.runWorker(30 + i);
    EXPECT_EQ(R.r->log()->logEndOffset(), leo) << "the trailing copy was appended again";
    EXPECT_EQ(R.w().rcOpenTxnsForTest(0, 0), 1u) << "the trailing copy closed T2";
    ASSERT_EQ(R.ch.responses.size(), 2u) << "the trailing copy must be re-answered";
    EXPECT_EQ(R.ch.responses[1].second.token, a1.token);
    EXPECT_EQ(R.ch.responses[1].second.errorCode, 0);
    EXPECT_EQ(R.ch.responses[1].second.offset, markerOffset) << "re-answered with the ORIGINAL outcome";
}

// NEW MAJOR-1: a trailing copy of an OLDER token of the same coordinator (its
// marker was refused, the retry k2 landed, T2 began) is never appended either.
TEST(D9RemoteMarkerRegression, CopyOfAnOlderTokenOfTheSameCoordinatorIsNotAppended) {
    GwRig R(/*self=*/2);
    R.image(2, 5);
    R.r->setMinIsrForTest(2);                                 // ISR {2} < 2: the first marker is refused
    std::vector<uint8_t> k1, k2;
    const auto first = R.remote(1, (7LL << 32) | 30, 77, true, 5, k1);
    R.gw.appendRemoteControlRecord(first);
    for (int i = 0; i < 16 && R.gw.markersInFlightForTest() > 0; ++i) R.runWorker(10 + i);
    ASSERT_EQ(R.ch.responses.size(), 1u);
    ASSERT_NE(R.ch.responses[0].second.errorCode, 0) << "precondition: refused";
    R.r->setMinIsrForTest(1);
    R.gw.appendRemoteControlRecord(R.remote(1, (7LL << 32) | 31, 77, true, 5, k2));   // the TCS retry, new token
    for (int i = 0; i < 16 && R.gw.markersInFlightForTest() > 0; ++i) R.runWorker(20 + i);
    ASSERT_EQ(R.ch.responses.size(), 2u);
    ASSERT_EQ(R.ch.responses[1].second.errorCode, 0);
    R.produceTxnData(77, 0, 51, 30);
    const int64_t leo = R.r->log()->logEndOffset();
    R.gw.appendRemoteControlRecord(first);                    // a late copy of the refused token
    for (int i = 0; i < 16 && R.gw.markersInFlightForTest() > 0; ++i) R.runWorker(40 + i);
    EXPECT_EQ(R.r->log()->logEndOffset(), leo) << "a stale older-token copy was appended";
    EXPECT_EQ(R.w().rcOpenTxnsForTest(0, 0), 1u) << "a stale older-token copy closed T2";
}

// Re-review 2 MEDIUM-1: the NEW coordinator load's marker was TAKEN here but
// transiently refused (min-ISR), so the PSM fence never rose; a late copy from
// the DEPOSED load (lower coordinator epoch) must still be refused as fenced —
// not appended after T2's data, where it would close T2.
TEST(D9RemoteMarkerRegression, DeposedCoordinatorCopyIsFencedAfterTheNewLoadsMarkerWasRefused) {
    GwRig R(/*self=*/2);
    R.image(2, 5);
    std::vector<uint8_t> k1, k2;
    const auto a1 = R.remote(/*origin=*/1, (7LL << 32) | 40, 77, true, /*epoch=*/5, k1);
    R.gw.appendRemoteControlRecord(a1);                       // T1 COMMIT from load epoch 5
    for (int i = 0; i < 16 && R.gw.markersInFlightForTest() > 0; ++i) R.runWorker(10 + i);
    ASSERT_EQ(R.ch.responses.size(), 1u);
    ASSERT_EQ(R.ch.responses[0].second.errorCode, 0);
    R.produceTxnData(77, 0, 52, 20);                          // T2 begins
    const int64_t leo = R.r->log()->logEndOffset();
    R.r->setMinIsrForTest(2);                                 // transient: the new load's marker is refused
    R.gw.appendRemoteControlRecord(R.remote(/*origin=*/3, (9LL << 32) | 1, 77, false, /*epoch=*/6, k2));
    for (int i = 0; i < 16 && R.gw.markersInFlightForTest() > 0; ++i) R.runWorker(30 + i);
    ASSERT_EQ(R.ch.responses.size(), 2u);
    ASSERT_NE(R.ch.responses[1].second.errorCode, 0) << "precondition: the epoch-6 marker was refused";
    R.r->setMinIsrForTest(1);                                 // the condition clears
    R.gw.appendRemoteControlRecord(a1);                       // the deposed load's late copy (epoch 5)
    for (int i = 0; i < 16 && R.gw.markersInFlightForTest() > 0; ++i) R.runWorker(40 + i);
    EXPECT_EQ(R.r->log()->logEndOffset(), leo) << "the deposed coordinator's copy was appended";
    EXPECT_EQ(R.w().rcOpenTxnsForTest(0, 0), 1u) << "the deposed coordinator's copy closed T2";
    ASSERT_EQ(R.ch.responses.size(), 3u);
    EXPECT_EQ(R.ch.responses[2].first, 1);
    EXPECT_EQ(R.ch.responses[2].second.errorCode, tx::kMarkerCoordinatorFencedError);
}

// Re-review 2 MEDIUM-2: the idempotence index is pruned without a timer —
// (a) when the partition is demoted on this node (the bridge routes BecomeFollower),
TEST(D9RemoteMarkerRegression, RemoteMarkerIndexIsPrunedWhenThePartitionIsDemoted) {
    GwRig R(/*self=*/2);
    R.image(2, 5);
    std::vector<uint8_t> k;
    R.gw.appendRemoteControlRecord(R.remote(1, (7LL << 32) | 50, 77, true, 5, k));
    R.gw.appendRemoteControlRecord(R.remote(1, (7LL << 32) | 51, 78, true, 5, k));
    for (int i = 0; i < 16 && R.gw.markersInFlightForTest() > 0; ++i) R.runWorker(10 + i);
    ASSERT_EQ(R.gw.remoteMarkerIndexSizeForTest(), 2u);
    (void)R.gw.pumpTxnMarkersForTest(20);
    EXPECT_EQ(R.gw.remoteMarkerIndexSizeForTest(), 2u) << "pruned while still leading";
    const auto h = R.reg.lookupHandle(0, 0);
    R.reg.setIntendedRole(0, 0, h.incarnation, ReplicaRegistry::IntendedRole::Follower);   // demotion routed
    (void)R.gw.pumpTxnMarkersForTest(30);
    EXPECT_EQ(R.gw.remoteMarkerIndexSizeForTest(), 0u) << "the demoted partition's entries were kept";
}

// (b) when the partition is removed from this node,
TEST(D9RemoteMarkerRegression, RemoteMarkerIndexIsPrunedWhenThePartitionIsRemoved) {
    GwRig R(/*self=*/2);
    R.image(2, 5);
    std::vector<uint8_t> k;
    R.gw.appendRemoteControlRecord(R.remote(1, (7LL << 32) | 52, 77, true, 5, k));
    for (int i = 0; i < 16 && R.gw.markersInFlightForTest() > 0; ++i) R.runWorker(10 + i);
    ASSERT_EQ(R.gw.remoteMarkerIndexSizeForTest(), 1u);
    R.reg.remove(0, 0);
    (void)R.gw.pumpTxnMarkersForTest(20);
    EXPECT_EQ(R.gw.remoteMarkerIndexSizeForTest(), 0u) << "the removed partition's entries were kept";
}

// (c) when the producer expires (the producer-expiry hook).
TEST(D9RemoteMarkerRegression, RemoteMarkerIndexIsPrunedWhenTheProducerExpires) {
    GwRig R(/*self=*/2);
    R.image(2, 5);
    std::vector<uint8_t> k;
    R.gw.appendRemoteControlRecord(R.remote(1, (7LL << 32) | 53, 77, true, 5, k));
    R.gw.appendRemoteControlRecord(R.remote(1, (7LL << 32) | 54, 78, true, 5, k));
    for (int i = 0; i < 16 && R.gw.markersInFlightForTest() > 0; ++i) R.runWorker(10 + i);
    ASSERT_EQ(R.gw.remoteMarkerIndexSizeForTest(), 2u);
    R.gw.onProducerExpired(77);
    EXPECT_EQ(R.gw.remoteMarkerIndexSizeForTest(), 1u) << "the expired producer's entry was kept";
}

// Re-review NEW MEDIUM-3: the coordinator stamps the target leader epoch it
// routed the marker under.
TEST(D9RemoteMarkerRegression, RemoteMarkerCarriesTheTargetLeaderEpoch) {
    GwRig R(1);
    R.image(2, 5);
    ASSERT_EQ(R.marker(77, true), rune::gateway::ClientGateway::kControlRecordAsync);
    ASSERT_EQ(R.ch.markers.size(), 1u);
    EXPECT_EQ(R.ch.markers[0].m.leaderEpoch, 5);
}

// NEW MEDIUM-3: a copy routed under an OLDER leader epoch of this partition
// (held across leadership changes) is refused NotLeader, nothing appended.
TEST(D9RemoteMarkerRegression, MarkerRoutedUnderAnOlderLeaderEpochIsRefusedNotLeader) {
    GwRig R(/*self=*/2);
    R.image(2, 5);
    WorkerOp op;                                              // re-elected: leader epoch 1 → 3
    op.type = WorkerOp::Type::BecomeLeader; op.topicId = 0; op.partition = 0; op.replica = R.r;
    op.roleEpoch = 3; op.isr = {2};
    ASSERT_TRUE(R.pool.tryEnqueueOp(0, 0, std::move(op)));
    R.runWorker(5);
    ASSERT_EQ(R.r->role(), Role::Leader);
    std::vector<uint8_t> keep;
    auto stale = R.remote(1, 914, 77, true, 5, keep);
    stale.leaderEpoch = 1;                                    // routed while epoch 1 was current
    R.gw.appendRemoteControlRecord(stale);
    for (int i = 0; i < 16 && R.gw.markersInFlightForTest() > 0; ++i) R.runWorker(10 + i);
    EXPECT_EQ(R.r->log()->logEndOffset(), 0) << "a marker routed under an older leader epoch was appended";
    ASSERT_EQ(R.ch.responses.size(), 1u);
    EXPECT_EQ(R.ch.responses[0].second.errorCode, kNotLeader);
    auto fresh = R.remote(1, 915, 77, true, 5, keep);
    fresh.leaderEpoch = 3;
    R.gw.appendRemoteControlRecord(fresh);
    for (int i = 0; i < 16 && R.gw.markersInFlightForTest() > 0; ++i) R.runWorker(20 + i);
    EXPECT_EQ(controlBatchCount(*R.r->log()), 1) << "the current-epoch marker lands";
}

// MAJOR-2 on the LOCAL path: the same fence in the worker's marker append.
TEST(D9RemoteMarkerRegression, LocalStaleCoordinatorEpochMarkerIsFenced) {
    GwRig R(1);
    ASSERT_EQ(R.marker(77, true, /*epoch=*/5), rune::gateway::ClientGateway::kControlRecordAsync);
    for (int i = 0; i < 16 && R.gw.markersInFlightForTest() > 0; ++i) R.runWorker(10 + i);
    ASSERT_EQ(R.tcs.done.size(), 1u);
    ASSERT_TRUE(R.tcs.done[0].landed);
    ASSERT_EQ(R.marker(77, false, /*epoch=*/4), rune::gateway::ClientGateway::kControlRecordAsync);
    for (int i = 0; i < 16 && R.gw.markersInFlightForTest() > 0; ++i) R.runWorker(20 + i);
    EXPECT_EQ(controlBatchCount(*R.r->log()), 1) << "the stale-epoch marker was appended";
    ASSERT_EQ(R.tcs.done.size(), 2u);
    EXPECT_FALSE(R.tcs.done[1].landed);
    EXPECT_EQ(R.tcs.done[1].err, tx::kMarkerCoordinatorFencedError);
    ASSERT_EQ(R.marker(77, true, /*epoch=*/5), rune::gateway::ClientGateway::kControlRecordAsync);   // equal epoch: OK
    for (int i = 0; i < 16 && R.gw.markersInFlightForTest() > 0; ++i) R.runWorker(30 + i);
    EXPECT_EQ(controlBatchCount(*R.r->log()), 2);
}

// ============================================================================
// Issue #112 (F15-FU1) — the registration verification when the transaction
// coordinator runs on ANOTHER node: the leader's gateway forwards the worker's
// ask over the cluster plane (VerifyTxnPartition) and relays the answer; every
// ask gets exactly one answer (a lost one: NotCoordinator, retriable).
// ============================================================================
namespace {
using rune::replication::VerifyTxnPartitionMsg;
using rune::replication::VerifyTxnPartitionResponseMsg;
constexpr int16_t kNotCoordinator = static_cast<int16_t>(pc::ErrorCode::NotCoordinator);
constexpr int16_t kInvalidTxnStateCode = static_cast<int16_t>(pc::ErrorCode::InvalidTxnState);
struct VerifyGwRig : GwRig {
    explicit VerifyGwRig(int32_t self = 1, int32_t coordinator = 2) : GwRig(self) {
        w().setTxnVerification(true);
        tcs.coordNode = coordinator;
    }
    int64_t leo() { return r->log()->logEndOffset(); }
    uint64_t refused() { return w().txnBatchesRefusedUnverifiedForTest(); }
    void answer(std::size_t i, int16_t code, int64_t now) {
        VerifyTxnPartitionResponseMsg resp{};
        static_cast<VerifyTxnPartitionMsg&>(resp) = ch.verifies.at(i).second;
        resp.errorCode = code;
        gw.onRemoteVerifyTxnPartitionResponse(resp);
        runWorker(now);
    }
};
} // namespace

TEST(TxnVerifyRemoteRegression, AskIsForwardedToTheRemoteCoordinatorAndItsConfirmationAdmitsRegression) {
    VerifyGwRig R;
    R.produceTxnData(/*pid=*/77, /*seq=*/0, /*corr=*/1, 10);
    EXPECT_EQ(R.leo(), 0) << "appended before the coordinator confirmed the registration";
    ASSERT_EQ(R.ch.verifies.size(), 1u) << "the ask was not forwarded to the coordinator node";
    const auto& [to, m] = R.ch.verifies[0];
    EXPECT_EQ(to, 2);
    EXPECT_EQ(m.requesterNodeId, 1);
    EXPECT_EQ(m.topicId, 0); EXPECT_EQ(m.partition, 0);
    EXPECT_EQ(m.producerId, 77); EXPECT_EQ(m.producerEpoch, 0);
    EXPECT_EQ(R.tcs.verifyCalls, 0) << "a remote coordinator is never asked locally";
    EXPECT_EQ(R.gw.txnVerifiesInFlightForTest(), 1u);
    R.answer(0, 0, 20);
    EXPECT_EQ(R.gw.txnVerifiesInFlightForTest(), 0u);
    R.produceTxnData(77, 0, 2, 30);   // the client's retry
    EXPECT_EQ(R.leo(), 1);
    R.produceTxnData(77, 1, 3, 40);
    EXPECT_EQ(R.leo(), 2);
    EXPECT_EQ(R.ch.verifies.size(), 1u) << "an open transaction is never re-verified";
}

TEST(TxnVerifyRemoteRegression, RemoteRefusalIsNeverAppendedRegression) {
    VerifyGwRig R;
    R.produceTxnData(77, 0, 1, 10);
    ASSERT_EQ(R.ch.verifies.size(), 1u);
    R.answer(0, kInvalidTxnStateCode, 20);
    R.produceTxnData(77, 0, 2, 30);
    EXPECT_EQ(R.leo(), 0);
    EXPECT_EQ(R.refused(), 2u);
    EXPECT_EQ(R.w().rcOpenTxnsForTest(0, 0), 0u) << "the LSO is pinned by a transaction no marker will close";
}

// A plane reconnect may lose the ask or its answer: the ask is answered NotCoordinator (retriable) at once, the
// client's retry asks again, and a late answer to the lost ask admits nothing.
TEST(TxnVerifyRemoteRegression, PlaneReconnectAnswersTheInFlightAskRetriablyRegression) {
    for (const int32_t peer : {2, -1}) {
        SCOPED_TRACE(peer);
        VerifyGwRig R;
        R.produceTxnData(77, 0, 1, 10);
        ASSERT_EQ(R.ch.verifies.size(), 1u);
        R.gw.onPlaneReconnect(peer);
        (void)R.gw.pumpTxnVerifyForTest(11);
        EXPECT_EQ(R.gw.txnVerifiesInFlightForTest(), 0u) << "a lost ask must never wait forever";
        R.runWorker(12);
        R.produceTxnData(77, 0, 2, 20);
        ASSERT_EQ(R.ch.verifies.size(), 2u) << "the retry must ask again";
        EXPECT_NE(R.ch.verifies[1].second.token, R.ch.verifies[0].second.token);
        R.answer(0, 0, 30);   // the late answer to the lost ask
        R.produceTxnData(77, 0, 3, 40);
        EXPECT_EQ(R.leo(), 0) << "a stale confirmation admitted the batch";
        R.answer(1, 0, 50);
        R.produceTxnData(77, 0, 4, 60);
        EXPECT_EQ(R.leo(), 1);
    }
}

TEST(TxnVerifyRemoteRegression, CoordinatorMoveAnswersTheInFlightAskRetriablyRegression) {
    VerifyGwRig R;
    R.produceTxnData(77, 0, 1, 10);
    ASSERT_EQ(R.ch.verifies.size(), 1u);
    R.tcs.coordNode = 3;
    (void)R.gw.pumpTxnVerifyForTest(11);
    EXPECT_EQ(R.gw.txnVerifiesInFlightForTest(), 0u);
    R.runWorker(12);
    R.produceTxnData(77, 0, 2, 20);
    ASSERT_EQ(R.ch.verifies.size(), 2u);
    EXPECT_EQ(R.ch.verifies[1].first, 3) << "the retry asks the new coordinator";
    R.answer(1, 0, 30);
    R.produceTxnData(77, 0, 3, 40);
    EXPECT_EQ(R.leo(), 1);
}

// The coordinator moves and a NEW ask goes to the new node before the next pump: the ask still out to the old
// node is answered NotCoordinator by the sweep (never left waiting on a node that may never answer).
TEST(TxnVerifyRemoteRegression, AskToTheOldCoordinatorIsSweptAfterANewAskWentToTheNewOneRegression) {
    VerifyGwRig R;
    R.produceTxnData(77, 0, 1, 10);
    ASSERT_EQ(R.ch.verifies.size(), 1u);
    R.tcs.coordNode = 3;
    R.produceTxnData(78, 0, 2, 11);   // another producer's ask, to node 3, before any pump
    ASSERT_EQ(R.ch.verifies.size(), 2u);
    EXPECT_EQ(R.ch.verifies[1].first, 3);
    EXPECT_EQ(R.gw.txnVerifiesInFlightForTest(), 2u);
    (void)R.gw.pumpTxnVerifyForTest(12);
    EXPECT_EQ(R.gw.txnVerifiesInFlightForTest(), 1u) << "the ask to the old coordinator was never swept";
    R.runWorker(13);
    R.produceTxnData(77, 0, 3, 20);
    ASSERT_EQ(R.ch.verifies.size(), 3u) << "pid 77's retry must ask the new coordinator";
    EXPECT_EQ(R.ch.verifies[2].first, 3);
}

TEST(TxnVerifyRemoteRegression, BackPressuredAskIsQueuedAndSentLaterRegression) {
    VerifyGwRig R;
    R.ch.acceptVerifies = false;
    R.produceTxnData(77, 0, 1, 10);
    EXPECT_TRUE(R.ch.verifies.empty());
    EXPECT_EQ(R.gw.txnVerifiesInFlightForTest(), 1u) << "a refused send was dropped";
    (void)R.gw.pumpTxnVerifyForTest(11);
    EXPECT_TRUE(R.ch.verifies.empty());
    R.ch.acceptVerifies = true;
    (void)R.gw.pumpTxnVerifyForTest(12);
    ASSERT_EQ(R.ch.verifies.size(), 1u);
    R.answer(0, 0, 20);
    R.produceTxnData(77, 0, 2, 30);
    EXPECT_EQ(R.leo(), 1);
}

// No coordinator known (election): retriable; a cluster with a member older than the verification admits the
// batch exactly as before #112 (an older coordinator would never answer the ask).
TEST(TxnVerifyRemoteRegression, UnknownCoordinatorRetriesAndAnOlderClusterAdmitsRegression) {
    VerifyGwRig R(1, /*coordinator=*/-1);
    R.produceTxnData(77, 0, 1, 10);
    R.produceTxnData(77, 0, 2, 20);
    EXPECT_EQ(R.leo(), 0);
    EXPECT_TRUE(R.ch.verifies.empty());
    EXPECT_EQ(R.w().txnVerifyRequestsSentForTest(), 2u) << "NotCoordinator must let the retry ask again";
    R.tcs.coordNode = 2;
    R.tcs.clusterVerifies = false;
    R.produceTxnData(77, 0, 3, 30);
    R.produceTxnData(77, 0, 4, 40);
    EXPECT_EQ(R.leo(), 1);
    EXPECT_TRUE(R.ch.verifies.empty()) << "an older cluster's coordinator was asked";
}

// The coordinator node's side: an ask from a partition leader is answered to that leader with the coordinator's
// code and the leader's token echoed; a node that does not coordinate answers NotCoordinator; a back-pressured
// answer is kept and sent later, in order.
TEST(TxnVerifyRemoteRegression, CoordinatorAnswersTheAskerRegression) {
    VerifyGwRig R(/*self=*/2, /*coordinator=*/2);
    R.tcs.local = true;
    R.tcs.verifyCode = kInvalidTxnStateCode;
    VerifyTxnPartitionMsg m{};
    m.requesterNodeId = 3; m.topicId = 0; m.partition = 1; m.token = 4242; m.producerId = 9; m.producerEpoch = 2;
    R.gw.onRemoteVerifyTxnPartition(m);
    ASSERT_EQ(R.ch.verifyResponses.size(), 1u);
    EXPECT_EQ(R.ch.verifyResponses[0].first, 3);
    const auto& a = R.ch.verifyResponses[0].second;
    EXPECT_EQ(a.token, 4242); EXPECT_EQ(a.producerId, 9); EXPECT_EQ(a.producerEpoch, 2);
    EXPECT_EQ(a.topicId, 0); EXPECT_EQ(a.partition, 1);
    EXPECT_EQ(a.errorCode, kInvalidTxnStateCode);
    EXPECT_EQ(R.tcs.verifyCalls, 1);
    R.tcs.local = false;
    R.ch.acceptVerifyResponses = false;
    m.token = 4243;
    R.gw.onRemoteVerifyTxnPartition(m);
    m.token = 4244;
    R.gw.onRemoteVerifyTxnPartition(m);
    EXPECT_EQ(R.ch.verifyResponses.size(), 1u);
    EXPECT_EQ(R.tcs.verifyCalls, 1) << "a node that does not coordinate must not answer from its own state";
    R.ch.acceptVerifyResponses = true;
    (void)R.gw.pumpTxnVerifyForTest(5);
    ASSERT_EQ(R.ch.verifyResponses.size(), 3u) << "a back-pressured answer was lost";
    EXPECT_EQ(R.ch.verifyResponses[1].second.token, 4243);
    EXPECT_EQ(R.ch.verifyResponses[2].second.token, 4244);
    EXPECT_EQ(R.ch.verifyResponses[1].second.errorCode, kNotCoordinator);
}

// Wire round trip of both frames (templates 13 / 14), fixed 32-byte block, little-endian.
TEST(TxnVerifyRemoteRegression, VerifyFramesRoundTripRegression) {
    VerifyTxnPartitionMsg m{};
    m.requesterNodeId = 7; m.topicId = -3; m.token = 0x0102030405060708LL; m.producerId = 1LL << 40;
    m.partition = 65535; m.producerEpoch = 32767; m.errorCode = 99;   // ignored on the ask
    uint8_t buf[VerifyTxnPartitionMsg::TOTAL_SIZE];
    ASSERT_EQ(m.encode(buf, sizeof buf), VerifyTxnPartitionMsg::TOTAL_SIZE);
    EXPECT_EQ(m.encode(buf, sizeof buf - 1), 0u);
    VerifyTxnPartitionMsg d{};
    ASSERT_TRUE(VerifyTxnPartitionMsg::decode(buf, sizeof buf, d));
    EXPECT_EQ(d.requesterNodeId, 7); EXPECT_EQ(d.topicId, -3); EXPECT_EQ(d.token, m.token);
    EXPECT_EQ(d.producerId, m.producerId); EXPECT_EQ(d.partition, 65535); EXPECT_EQ(d.producerEpoch, 32767);
    EXPECT_EQ(d.errorCode, 0);
    VerifyTxnPartitionResponseMsg rd{};
    EXPECT_FALSE(VerifyTxnPartitionResponseMsg::decode(buf, sizeof buf, rd)) << "an ask decoded as an answer";
    EXPECT_FALSE(VerifyTxnPartitionMsg::decode(buf, sizeof buf - 1, d)) << "a truncated frame decoded";
    VerifyTxnPartitionResponseMsg r{};
    static_cast<VerifyTxnPartitionMsg&>(r) = m;
    r.errorCode = 19;
    ASSERT_EQ(r.encode(buf, sizeof buf), VerifyTxnPartitionMsg::TOTAL_SIZE);
    ASSERT_TRUE(VerifyTxnPartitionResponseMsg::decode(buf, sizeof buf, rd));
    EXPECT_EQ(rd.errorCode, 19); EXPECT_EQ(rd.token, m.token);
    EXPECT_FALSE(VerifyTxnPartitionMsg::decode(buf, sizeof buf, d)) << "an answer decoded as an ask";
}

// ============================================================================
// Part 2 — integration: three in-process nodes over the REAL cluster-plane
// transport (one AeronPartitionTransport per node on one embedded driver; real
// partition replication over the same transport).  Topic 0 has 2 partitions,
// each replicated on all 3 nodes: p0 led by node 1, p1 by node 2; node 3 is
// the transaction coordinator and leads nothing.  The test thread is every
// node's runtime + worker thread (one pass = poll / worker / gateway / TCS for
// each live node, the clock advanced explicitly); waits are condition loops
// with a hard pass cap.
// ============================================================================
namespace {
constexpr int32_t kStream      = 731;
constexpr int     kRingBytes   = 1 << 20;
constexpr int     kPassCap     = 200'000;
constexpr int64_t kPassNs      = 1'000'000;   // 1 ms of node time per pass

std::vector<uint8_t> txnBatch(int64_t pid, int16_t epoch, int32_t seq) {
    rune::protocol::RecordBatchBuilder b(0, 1000);
    b.setTransactional(true); b.setProducerId(pid); b.setProducerEpoch(epoch); b.setBaseSequence(seq);
    b.append("k", "txn", 1000);
    return b.build();
}
std::vector<uint8_t> plainBatch() {
    rune::protocol::RecordBatchBuilder b(0, 1000);
    b.append("k", "plain", 1000);
    return b.build();
}

struct Node {
    int32_t id;
    TmpDir d;
    rune::gateway::TopicRegistry topics{{rune::node::TopicSpec{"t", 2}}, d.path.string()};
    ReplicaRegistry reg;
    WorkerPool pool{1, rune::metadata::ShardRouter(1), kRingBytes, kRingBytes, 16, 16};
    std::vector<int16_t> produceErrors;
    rune::gateway::ClientGateway gw = rune::gateway::ClientGateway::makeForTest(
        [this](const uint8_t* b, std::size_t n) -> int64_t {
            if (auto r = pc::ProduceResponse::decode(b, n)) produceErrors.push_back(r->errorCode);
            return 1;
        });
    std::unique_ptr<rune::replication::AeronPartitionTransport> tr;
    std::vector<std::pair<int64_t, int64_t>> recs;   // (subscriptionId, baseOffset) pushed
    int64_t lastSubId = 0;
    int64_t sid = 0;
    std::unique_ptr<TransactionCoordinationService> tcs;
    std::vector<std::vector<uint8_t>> controlFrames;
    bool alive = true;

    Node(int32_t self, std::shared_ptr<aeron::Aeron> client, uint16_t port, const std::array<int32_t, 2>& leaders) : id(self) {
        tr = std::make_unique<rune::replication::AeronPartitionTransport>(
            client, "aeron:udp?endpoint=127.0.0.1:" + std::to_string(port), kStream, self, kRingBytes);
        tr->setApplyRecordsCap(static_cast<int64_t>(kRingBytes) / 8 -
                               static_cast<int64_t>(rune::node::workerOpFrameBytes(0)));
        for (int p = 0; p < 2; ++p) {
            PartitionConfig c = cfg(self, 0, p);
            // A dead follower leaves the ISR after 100 passes of node time (the
            // HW-gated markers need the ISR to shrink to the live replicas).
            c.fetchIntervalNs = kPassNs; c.isrCheckIntervalNs = 10 * kPassNs; c.replicaLagMaxMs = 100;
            const bool lead = leaders[static_cast<std::size_t>(p)] == self;
            auto rep = std::make_unique<PartitionReplica>(c, lead ? Role::Leader : Role::Follower, 1,
                                                          std::vector<NodeId>{1, 2, 3}, std::vector<NodeId>{1, 2, 3},
                                                          topics.log(0, p), tr.get());
            if (!lead) rep->becomeFollower(1, leaders[static_cast<std::size_t>(p)]);
            reg.add(0, p, std::move(rep));
        }
        pool.assignFromRegistry(reg);
        pool.setRegistry(&reg);
        pool.setRunnersStartedForTest(true);
        tr->setReplicaRegistry(&reg);
        tr->setWorkerPool(&pool);
        tr->setTxnMarkerHandlers(&rune::gateway::ClientGateway::onWriteTxnMarkerThunk,
                                 &rune::gateway::ClientGateway::onWriteTxnMarkerResponseThunk, &gw);
        tr->setPlaneReconnectHandler(&rune::gateway::ClientGateway::onPlaneReconnectThunk, &gw);
        gw.setNodeIdForTest(self);
        gw.setTopicRegistry(&topics);
        gw.setReplicaRegistry(&reg);
        gw.setWorkerPool(&pool);
        gw.setCommittedMetadataReady(true);
        gw.setTxnMarkerChannel(rune::gateway::ClientGateway::TxnMarkerChannel{
            &rune::replication::AeronPartitionTransport::sendWriteTxnMarkerThunk,
            &rune::replication::AeronPartitionTransport::sendWriteTxnMarkerResponseThunk, tr.get()});
        gw.setOnEgressFrame([this](int64_t, const uint8_t* b, std::size_t n) {
            if (auto rec = pc::Records::decode(b, n)) recs.emplace_back(rec->subscriptionId, rec->baseOffset);
            else if (auto sr = pc::SubscribeResponse::decode(b, n)) lastSubId = sr->subscriptionId;
        });
        gw.setOnControlFrame([this](int64_t, const uint8_t* b, std::size_t n) { controlFrames.emplace_back(b, b + n); });
        sid = gw.injectTestSession();
    }
    PartitionReplica* rep(int p) { return reg.lookup(0, p); }
    Log& log(int p) { return *rep(p)->log(); }
    void pass(int64_t now) {
        work_ = tr->poll(now);
        pool.worker(0).doWork(now);
        pool.worker(0).drainReturn([&](WorkerReturn ret) { gw.handleWorkerReturnForTest(ret); });
        gw.pushSubscriptionsForTest();
        (void)gw.pumpTxnMarkersForTest(now);
        if (tcs) (void)tcs->doWork(now);
    }
    int work_ = 0;
    int64_t subscribe(int p, uint8_t isolation) {
        pc::Subscribe sub;
        sub.sessionId = sid; sub.correlationId = 9000 + p; sub.topicId = 0; sub.partition = p;
        sub.partitionKnown = 1; sub.fromOffset = 0; sub.isolation = isolation;
        std::vector<uint8_t> frame(sub.encodedSize());
        sub.encode(frame.data());
        lastSubId = 0;
        gw.applySubscribeForTest(frame.data(), frame.size());
        return lastSubId;
    }
    int deliveredCount(int64_t subId, int64_t base) const {
        int n = 0;
        for (const auto& [s, b] : recs) n += (s == subId && b == base) ? 1 : 0;
        return n;
    }
    void produce(int p, const std::vector<uint8_t>& batch, int64_t pid, int16_t epoch, int32_t seq, int64_t corr) {
        pc::Produce msg;
        msg.sessionId = sid; msg.correlationId = corr; msg.topicId = 0; msg.partition = p;
        msg.partitionKnown = 1; msg.acks = 0; msg.producerId = pid; msg.producerEpoch = epoch;
        msg.baseSequence = seq; msg.recordBatch = std::span<const uint8_t>(batch);
        std::vector<uint8_t> frame(msg.encodedSize());
        msg.encode(frame.data());
        gw.applyProduceForTest(frame.data(), frame.size());
    }
};

rune::node::EmbeddedDriverConfig driverConfig(const TmpDir& d) {
    rune::node::EmbeddedDriverConfig dc;
    dc.dir = (d.path / "aeron").string();
    dc.termBufferBytes = 1024 * 1024;
    return dc;
}

struct Cluster {
    TmpDir driverDir;
    rune::node::EmbeddedDriver driver{driverConfig(driverDir), rune::node::DriverMode::Threaded};
    std::shared_ptr<aeron::Aeron> client;
    std::array<uint16_t, 3> ports{};
    std::vector<std::unique_ptr<Node>> nodes;
    int64_t now = 1'000'000;
    std::string txnId = "tx-d9";
    int64_t pid = -1; int16_t epoch = 0; int64_t corr = 100;
    std::array<uint8_t, 4096> out{};

    Cluster() {
        aeron::Context ctx;
        ctx.aeronDir(driver.dir());
        client = aeron::Aeron::connect(ctx);
        for (auto& p : ports) p = rune::aeronutil::testing::allocFreeUdpPort();
        const std::array<int32_t, 2> leaders{1, 2};
        for (int32_t id = 1; id <= 3; ++id)
            nodes.push_back(std::make_unique<Node>(id, client, ports[static_cast<std::size_t>(id - 1)], leaders));
        for (auto& n : nodes)
            for (int32_t peer = 1; peer <= 3; ++peer)
                if (peer != n->id)
                    n->tr->addPeer(peer, "aeron:udp?endpoint=127.0.0.1:" +
                                   std::to_string(ports[static_cast<std::size_t>(peer - 1)]), kStream);
        setRouting({1, 2}, {1, 1});
        auto& c = coord();
        c.tcs = std::make_unique<TransactionCoordinationService>(nullptr, &c.topics, [] { return true; },
                                                                 [] { return NodeId{3}; });
        c.tcs->setGateway(&c.gw);
        c.gw.setTxnService(c.tcs.get());
    }
    ~Cluster() {
        for (auto& n : nodes) {
            n->tr->setTxnMarkerHandlers(nullptr, nullptr, nullptr);
            n->tr->setPlaneReconnectHandler(nullptr, nullptr);
        }
    }
    Node& node(int32_t id) { return *nodes[static_cast<std::size_t>(id - 1)]; }
    Node& coord() { return node(3); }
    void setRouting(std::array<int32_t, 2> leaders, std::array<int32_t, 2> epochs) {
        std::vector<rune::metadata::RoutingImage::BuildEntry> e;
        for (int p = 0; p < 2; ++p)
            e.push_back({rune::metadata::gpidOf(0, p), leaders[static_cast<std::size_t>(p)],
                         epochs[static_cast<std::size_t>(p)]});
        auto img = rune::metadata::RoutingImage::build(e);
        for (auto& n : nodes) if (n->alive) { n->gw.setRoutingImageSnapshot(img); (void)n->gw.pumpTxnMarkersForTest(now); }
    }
    void pass() {
        for (auto& n : nodes) if (n->alive) n->pass(now);
        now += kPassNs;
        std::this_thread::yield();   // let the driver thread move the frames (no sleep)
    }
    template <typename Pred> bool passUntil(Pred p, int cap = kPassCap) {
        for (int i = 0; i < cap; ++i) { if (p()) return true; pass(); }
        return p();
    }
    // --- coordinator requests (TxnMessages through the TCS handlers) -------------
    std::span<const uint8_t> tid() const { return {reinterpret_cast<const uint8_t*>(txnId.data()), txnId.size()}; }
    int16_t initPid() {
        pc::InitProducerId q; q.sessionId = coord().sid; q.correlationId = ++corr; q.transactionTimeoutMs = 600'000;
        q.transactionalId = tid();
        std::vector<uint8_t> f(q.encodedSize()); q.encode(f.data());
        const auto n = coord().tcs->handleInitProducerId(f.data(), f.size(), now, out.data(), out.size());
        auto r = pc::InitProducerIdResponse::decode(out.data(), n);
        if (r && r->errorCode == 0) { pid = r->producerId; epoch = r->producerEpoch; }
        return r ? r->errorCode : -1;
    }
    int16_t addPartitions() {
        pc::AddPartitionsToTxn q; q.sessionId = coord().sid; q.correlationId = ++corr; q.producerId = pid;
        q.producerEpoch = epoch; q.transactionalId = tid();
        const std::array<pc::TxnTopicPartition, 2> tps{pc::TxnTopicPartition{0, 0}, pc::TxnTopicPartition{0, 1}};
        std::vector<uint8_t> f(pc::AddPartitionsToTxn::encodedSizeFor(tid(), tps.size()));
        q.encode(f.data(), std::span<const pc::TxnTopicPartition>(tps));
        const auto n = coord().tcs->handleAddPartitions(f.data(), f.size(), now, out.data(), out.size());
        auto r = pc::AddPartitionsToTxnResponse::decode(out.data(), n);
        if (!r || r->results.size() != 2) return -1;
        return r->results[0].errorCode != 0 ? r->results[0].errorCode : r->results[1].errorCode;
    }
    // EndTxn: the error if answered synchronously, 999 when deferred (its Prepare* record not committed yet).
    int16_t endTxn(bool commit) {
        pc::EndTxn q; q.sessionId = coord().sid; q.correlationId = ++corr; q.producerId = pid;
        q.producerEpoch = epoch; q.commit = commit ? 1 : 0; q.transactionalId = tid();
        std::vector<uint8_t> f(q.encodedSize()); q.encode(f.data());
        const auto n = coord().tcs->handleEndTxn(f.data(), f.size(), now, out.data(), out.size());
        if (n == 0) return 999;
        auto r = pc::EndTxnResponse::decode(out.data(), n);
        return r ? r->errorCode : -1;
    }
    // #38: the EndTxn is answered inline once Prepare* is durable; the txn's progress is its coordinator state.
    rune::transaction::TxnState txnState() { return coord().tcs->txnStateForTest(txnId); }
    // A deferred (second) EndTxn answer: none is ever sent for an EndTxn answered inline (#38).
    std::optional<int16_t> deferredEndTxnError() {
        for (const auto& f : coord().controlFrames)
            if (auto r = pc::EndTxnResponse::decode(f.data(), f.size())) return r->errorCode;
        return std::nullopt;
    }
    // Every replica of partition p holds `leo` records and the leader's HW covers them.
    bool replicated(int p, int64_t leo, int32_t leader) {
        for (auto& n : nodes)
            if (n->alive && n->log(p).logEndOffset() < leo) return false;
        return node(leader).rep(p)->highWatermark() >= leo;
    }
};
} // namespace

// The core D9 scenario: a transaction spanning partitions led by two different
// nodes, coordinated by a third → EndTxn(commit) completes; read_committed
// consumers on each leader see the data only after the commit.
TEST(D9CrossNodeMarkerIntegrationRegression, CommitAcrossTwoRemoteLeadersCompletesAndIsVisibleReadCommitted) {
    Cluster C;
    const int64_t sub0 = C.node(1).subscribe(0, /*read_committed*/1);
    const int64_t sub1 = C.node(2).subscribe(1, 1);
    ASSERT_GT(sub0, 0); ASSERT_GT(sub1, 0);
    ASSERT_EQ(C.initPid(), 0);
    ASSERT_EQ(C.addPartitions(), 0);
    C.node(1).produce(0, txnBatch(C.pid, C.epoch, 0), C.pid, C.epoch, 0, ++C.corr);
    C.node(2).produce(1, txnBatch(C.pid, C.epoch, 0), C.pid, C.epoch, 0, ++C.corr);
    ASSERT_TRUE(C.passUntil([&] { return C.replicated(0, 1, 1) && C.replicated(1, 1, 2); }))
        << "precondition: the txn data replicated on both partitions";
    for (int i = 0; i < 50; ++i) C.pass();
    EXPECT_EQ(C.node(1).deliveredCount(sub0, 0), 0) << "read_committed saw an OPEN transaction's data";
    EXPECT_EQ(C.node(2).deliveredCount(sub1, 0), 0);

    ASSERT_EQ(C.endTxn(/*commit=*/true), 0) << "EndTxn(commit) OK once PrepareCommit is durable (#38)";
    ASSERT_EQ(C.txnState(), rune::transaction::TxnState::PrepareCommit) << "the markers are still in flight";
    EXPECT_EQ(C.addPartitions(), static_cast<int16_t>(pc::ErrorCode::ConcurrentTransactions))
        << "the next txn may not start before the markers land";
    ASSERT_TRUE(C.passUntil([&] { return C.txnState() == rune::transaction::TxnState::CompleteCommit; }))
        << "the txn never completed: a marker for a remotely led partition did not land (refused="
        << C.coord().tcs->controlMarkersRefusedForTest() << ")";
    EXPECT_FALSE(C.deferredEndTxnError().has_value()) << "a second EndTxn answer was sent when the markers landed";
    EXPECT_EQ(controlBatchCount(C.node(1).log(0)), 1) << "one COMMIT marker on p0's leader";
    EXPECT_EQ(controlBatchCount(C.node(2).log(1)), 1) << "one COMMIT marker on p1's leader";
    EXPECT_EQ(C.coord().tcs->awaitingMarkerTxnsForTest(), 0u);
    ASSERT_TRUE(C.passUntil([&] { return C.node(1).deliveredCount(sub0, 0) > 0 && C.node(2).deliveredCount(sub1, 0) > 0; }))
        << "read_committed never saw the committed data";
    for (int i = 0; i < 50; ++i) C.pass();
    EXPECT_EQ(C.node(1).deliveredCount(sub0, 0), 1) << "delivered exactly once";
    EXPECT_EQ(C.node(2).deliveredCount(sub1, 0), 1);
    EXPECT_EQ(C.coord().gw.markersInFlightForTest(), 0u);
}

// Abort variant: the data stays hidden from read_committed; data produced after
// the ABORT marker is delivered (the LSO moved — the check is not vacuous).
TEST(D9CrossNodeMarkerIntegrationRegression, AbortAcrossTwoRemoteLeadersHidesTheData) {
    Cluster C;
    const int64_t sub0 = C.node(1).subscribe(0, 1);
    const int64_t sub1 = C.node(2).subscribe(1, 1);
    ASSERT_EQ(C.initPid(), 0);
    ASSERT_EQ(C.addPartitions(), 0);
    C.node(1).produce(0, txnBatch(C.pid, C.epoch, 0), C.pid, C.epoch, 0, ++C.corr);
    C.node(2).produce(1, txnBatch(C.pid, C.epoch, 0), C.pid, C.epoch, 0, ++C.corr);
    ASSERT_TRUE(C.passUntil([&] { return C.replicated(0, 1, 1) && C.replicated(1, 1, 2); }));
    ASSERT_EQ(C.endTxn(/*commit=*/false), 0) << "EndTxn(abort) OK once PrepareAbort is durable (#38)";
    ASSERT_EQ(C.txnState(), rune::transaction::TxnState::PrepareAbort) << "the markers are still in flight";
    ASSERT_TRUE(C.passUntil([&] { return C.txnState() == rune::transaction::TxnState::CompleteAbort; }))
        << "the abort never completed (refused=" << C.coord().tcs->controlMarkersRefusedForTest() << ")";
    EXPECT_FALSE(C.deferredEndTxnError().has_value()) << "a second EndTxn answer was sent when the markers landed";
    ASSERT_EQ(controlBatchOffset(C.node(1).log(0)), 1) << "the ABORT marker landed on p0's leader after the data";
    ASSERT_EQ(controlBatchOffset(C.node(2).log(1)), 1) << "the ABORT marker landed on p1's leader after the data";
    C.node(1).produce(0, plainBatch(), -1, 0, -1, ++C.corr);
    C.node(2).produce(1, plainBatch(), -1, 0, -1, ++C.corr);
    ASSERT_TRUE(C.passUntil([&] { return C.node(1).deliveredCount(sub0, 2) > 0 && C.node(2).deliveredCount(sub1, 2) > 0; }))
        << "the post-abort data was never delivered (LSO pinned?)";
    EXPECT_EQ(C.node(1).deliveredCount(sub0, 0), 0) << "read_committed saw ABORTED data on p0";
    EXPECT_EQ(C.node(2).deliveredCount(sub1, 0), 0) << "read_committed saw ABORTED data on p1";
}

// Leader kill/move mid-EndTxn: p0's leader (node 1) stops answering right before
// EndTxn; the coordinator's marker to it is lost.  p0's leadership moves to
// node 2 (routing image + role change) → the in-flight marker completes
// NotLeader at once, the TCS retry lands it on the new leader, the transaction
// completes, and the committed data is visible exactly once.
TEST(D9CrossNodeMarkerIntegrationRegression, LeaderKilledMidEndTxnMarkerLandsOnTheNewLeader) {
    Cluster C;
    ASSERT_EQ(C.initPid(), 0);
    ASSERT_EQ(C.addPartitions(), 0);
    C.node(1).produce(0, txnBatch(C.pid, C.epoch, 0), C.pid, C.epoch, 0, ++C.corr);
    C.node(2).produce(1, txnBatch(C.pid, C.epoch, 0), C.pid, C.epoch, 0, ++C.corr);
    ASSERT_TRUE(C.passUntil([&] { return C.replicated(0, 1, 1) && C.replicated(1, 1, 2); }));
    ASSERT_TRUE(C.passUntil([&] { return C.node(2).rep(0)->highWatermark() >= 1 && C.coord().rep(0)->highWatermark() >= 1; }))
        << "precondition: p0's followers know the data is committed";

    C.node(1).alive = false;                                 // p0's leader dies (stops answering)
    ASSERT_EQ(C.endTxn(/*commit=*/true), 0) << "OK once PrepareCommit is durable (#38)";
    ASSERT_EQ(C.txnState(), rune::transaction::TxnState::PrepareCommit);
    ASSERT_TRUE(C.passUntil([&] { return controlBatchCount(C.node(2).log(1)) == 1; }))
        << "p1's marker (live leader) landed";
    for (int i = 0; i < 200; ++i) C.pass();
    ASSERT_EQ(C.txnState(), rune::transaction::TxnState::PrepareCommit)
        << "precondition: p0's marker is stuck on the dead leader, so the txn has not completed";
    EXPECT_EQ(C.addPartitions(), static_cast<int16_t>(pc::ErrorCode::ConcurrentTransactions))
        << "the next txn may not start while p0's marker is unwritten";
    EXPECT_EQ(C.coord().gw.remoteMarkersInFlightForTest(), 1u) << "precondition: p0's marker in flight to node 1";

    // Leadership of p0 moves to node 2 (epoch 2, ISR {2,3}); node 3 follows it.
    {
        WorkerOp op;
        op.type = WorkerOp::Type::BecomeLeader; op.topicId = 0; op.partition = 0; op.replica = C.node(2).rep(0);
        op.roleEpoch = 2; op.isr = {2, 3};
        ASSERT_TRUE(C.node(2).pool.tryEnqueueOp(0, 0, std::move(op)));
        WorkerOp f;
        f.type = WorkerOp::Type::BecomeFollower; f.topicId = 0; f.partition = 0; f.replica = C.coord().rep(0);
        f.roleEpoch = 2; f.roleLeaderId = 2;
        ASSERT_TRUE(C.coord().pool.tryEnqueueOp(0, 0, std::move(f)));
    }
    C.pass();
    ASSERT_EQ(C.node(2).rep(0)->role(), Role::Leader);
    C.setRouting({2, 2}, {2, 1});
    EXPECT_EQ(C.coord().gw.remoteMarkersInFlightForTest(), 0u)
        << "the leader change must complete the in-flight marker at once (no timeout)";
    const int64_t sub0 = C.node(2).subscribe(0, 1);
    ASSERT_GT(sub0, 0);
    ASSERT_TRUE(C.passUntil([&] { return C.txnState() == rune::transaction::TxnState::CompleteCommit; }, 5'000))
        << "the txn never completed after p0's leadership moved (refused="
        << C.coord().tcs->controlMarkersRefusedForTest() << ")";
    EXPECT_FALSE(C.deferredEndTxnError().has_value()) << "a second EndTxn answer was sent when the markers landed";
    EXPECT_EQ(controlBatchCount(C.node(2).log(0)), 1) << "exactly one COMMIT marker on p0's new leader";
    EXPECT_EQ(controlBatchOffset(C.node(2).log(0)), 1) << "after the data";
    ASSERT_TRUE(C.passUntil([&] { return C.node(2).deliveredCount(sub0, 0) > 0; }))
        << "read_committed on the new leader never saw the committed data";
    for (int i = 0; i < 50; ++i) C.pass();
    EXPECT_EQ(C.node(2).deliveredCount(sub0, 0), 1) << "no duplicate visible effect";
    EXPECT_EQ(C.coord().tcs->awaitingMarkerTxnsForTest(), 0u);
}

// Leader side: the partition left this node's registry after the marker was
// accepted (removed / reassigned) → the worker ownership guard refuses it and
// the ORIGIN is answered NotLeader (never an invariant halt, never the local TCS).
TEST(D9RemoteMarkerRegression, RemoteMarkerForAPartitionGoneFromTheRegistryIsAnsweredNotLeader) {
    GwRig R(2);
    R.gw.setRoutingImageSnapshot(imageWith(2, 5));
    R.reg.remove(0, 0);
    const auto bytes = tx::ControlMarker::build(77, 0, tx::ControlType::Commit, 3);
    WriteTxnMarkerMsg m{};
    m.coordinatorNodeId = 1; m.coordinatorEpoch = 3; m.token = 906; m.topicId = 0; m.partition = 0;
    m.producerId = 77; m.commit = 1; m.marker = std::span<const uint8_t>(bytes);
    R.gw.appendRemoteControlRecord(m);
    for (int i = 0; i < 16 && R.gw.markersInFlightForTest() > 0; ++i) R.runWorker(10 + i);
    EXPECT_EQ(R.gw.markersInFlightForTest(), 0u);
    ASSERT_EQ(R.ch.responses.size(), 1u);
    EXPECT_EQ(R.ch.responses[0].first, 1);
    EXPECT_EQ(R.ch.responses[0].second.token, 906);
    EXPECT_EQ(R.ch.responses[0].second.errorCode, kNotLeader);
    EXPECT_TRUE(R.tcs.done.empty()) << "a remote-origin refusal must not reach the LOCAL coordinator";
}

// Review MAJOR-1, transport half: the plane reports the event the re-send hangs
// on.  Node 1's link to node 2 is DOWN (nobody listens; a marker frame to it is
// dropped) → node 2 comes up → the hold router's down→up edge is reported for
// peer 2; node 2's publication to node 1 creates a new inbound image → -1.
// Real AeronPartitionTransports on one embedded driver; condition waits only.
TEST(D9CrossNodeMarkerIntegrationRegression, PlaneReportsPeerUpEdgeAndNewInboundImage) {
    TmpDir dd;
    rune::node::EmbeddedDriver driver{driverConfig(dd), rune::node::DriverMode::Threaded};
    aeron::Context ctx;
    ctx.aeronDir(driver.dir());
    auto client = aeron::Aeron::connect(ctx);
    const uint16_t p1 = rune::aeronutil::testing::allocFreeUdpPort();
    uint16_t p2 = rune::aeronutil::testing::allocFreeUdpPort();
    while (p2 == p1) p2 = rune::aeronutil::testing::allocFreeUdpPort();
    auto chan = [](uint16_t p) { return "aeron:udp?endpoint=127.0.0.1:" + std::to_string(p); };
    struct Events { std::vector<int32_t> peers; };
    Events ev;
    auto onReconnect = [](void* ctx, int32_t peer) { static_cast<Events*>(ctx)->peers.push_back(peer); };

    rune::replication::AeronPartitionTransport a(client, chan(p1), kStream, /*self=*/1, kRingBytes);
    a.setPlaneReconnectHandler(onReconnect, &ev);
    a.addPeer(2, chan(p2), kStream);
    a.addPeer(3, "aeron:udp?endpoint=10.255.255.1:" + std::to_string(p2), kStream);   // another host, never up
    std::vector<uint8_t> keep = tx::ControlMarker::build(77, 0, tx::ControlType::Commit, 3);
    WriteTxnMarkerMsg m{};
    m.coordinatorNodeId = 1; m.coordinatorEpoch = 3; m.token = 1; m.producerId = 77; m.commit = 1;
    m.marker = std::span<const uint8_t>(keep);
    int64_t now = 1;
    auto until = [&](auto pred, auto&& step) {
        for (int i = 0; i < 2'000'000; ++i) { if (pred()) return true; step(); std::this_thread::yield(); }
        return pred();
    };
    ASSERT_TRUE(a.sendWriteTxnMarker(2, m));
    ASSERT_TRUE(until([&] { return a.peerDownForTest(2); }, [&] { (void)a.poll(++now); }))
        << "precondition: the link to the absent peer is down (its frame dropped)";
    ev.peers.clear();

    rune::replication::AeronPartitionTransport b(client, chan(p2), kStream, /*self=*/2, kRingBytes);
    b.addPeer(1, chan(p1), kStream);
    auto has = [&](int32_t peer) { return std::find(ev.peers.begin(), ev.peers.end(), peer) != ev.peers.end(); };
    ASSERT_TRUE(until([&] { return has(2) && a.inboundImagesFromForTest(2) > 0 && a.peerLinkUpForTest(2); },
                      [&] { (void)a.poll(++now); (void)b.poll(now); }))
        << "the plane never reported the reconnect (peer up edge / new inbound image)";
    EXPECT_FALSE(a.peerDownForTest(2));
    EXPECT_FALSE(has(-1)) << "re-review NEW MAJOR-1: a new image is attributed to its peer, not 'everyone'";
    EXPECT_FALSE(has(3)) << "a peer on another host was reported";
}

// ============================================================================
// Part 3 — review MEDIUM-4: the REAL BrokerNode wiring (default runtime config).
// Three BrokerNodes in-process (each with its own embedded driver); a marker
// is handed to one node's gateway (on its runtime thread — exactly where the
// TCS calls it) for a partition another node leads.  It must reach that
// leader over BrokerNode's own transport ↔ gateway wiring, land there as a
// control batch, and its answer must come back (after the HW) and clear the
// coordinator's in-flight entry.  The stuck-markers counter is in the CnC.
// Waits are condition loops on owner-thread round trips (no sleeps).
// ============================================================================
namespace {
// Ports: [0..2] consensus, [3..5] client, [6..8] replication, and the per-worker
// shard bases — [9..11] worker ingress, [12..14] worker replication, [15..17]
// worker egress control (one worker).  All discovered: the DEFAULT runtime is
// shard mode, whose DERIVED base ports are fixed (8200 / 8264 / 8328) and
// collide across concurrently running test processes ("replication plane did
// not resolve — Address already in use": the round-3 flake's root cause).
NodeConfig d9NodeConfig(int nodeId, const fs::path& dataDir, const std::array<uint16_t, 18>& ports) {
    NodeConfig cfg;   // the DEFAULT runtime (placement / worker mode) — not pinned
    cfg.nodeId              = nodeId;
    cfg.dataDir             = dataDir.string();
    cfg.consensusStreamId   = 81;
    cfg.replicationStreamId = 82;
    cfg.electionTimeoutMs        = 150;
    cfg.electionStatusIntervalMs = 20;
    cfg.startupCanvassTimeoutMs  = 100;
    for (int i = 0; i < 3; ++i) {
        PeerConfig p;
        p.id              = i;
        p.host            = "127.0.0.1";
        p.consensusPort   = ports[static_cast<std::size_t>(i)];
        p.clientPort      = ports[static_cast<std::size_t>(3 + i)];
        p.replicationPort = ports[static_cast<std::size_t>(6 + i)];
        p.ingressBasePort       = ports[static_cast<std::size_t>(9 + i)];
        p.replBasePort          = ports[static_cast<std::size_t>(12 + i)];
        p.egressControlBasePort = ports[static_cast<std::size_t>(15 + i)];
        p.workerCount           = 1;
        cfg.peers.push_back(p);
    }
    cfg.clientEndpoint.channel  = "aeron:udp?endpoint=127.0.0.1:" + std::to_string(ports[static_cast<std::size_t>(3 + nodeId)]);
    cfg.clientEndpoint.streamId = 83;
    cfg.workerCount                 = 1;
    cfg.workerIngressBasePort       = ports[static_cast<std::size_t>(9 + nodeId)];
    cfg.workerReplBasePort          = ports[static_cast<std::size_t>(12 + nodeId)];
    cfg.workerEgressControlHost     = "127.0.0.1";
    cfg.workerEgressControlBasePort = ports[static_cast<std::size_t>(15 + nodeId)];
    return cfg;
}
} // namespace

TEST(D9BrokerNodeWiringRegression, MarkerForARemotelyLedPartitionLandsThroughTheRealNodeWiring) {
    TmpDir root;
    std::array<uint16_t, 18> ports{};
    for (std::size_t i = 0; i < ports.size(); ++i) {
        for (;;) {
            const uint16_t p = rune::aeronutil::testing::allocFreeUdpPort();
            if (std::find(ports.begin(), ports.begin() + static_cast<std::ptrdiff_t>(i), p) ==
                ports.begin() + static_cast<std::ptrdiff_t>(i)) { ports[i] = p; break; }
        }
    }
    std::vector<std::unique_ptr<BrokerNode>> nodes;
    for (int i = 0; i < 3; ++i)
        nodes.push_back(std::make_unique<BrokerNode>(d9NodeConfig(i, root.path / ("n" + std::to_string(i)), ports)));
    for (auto& n : nodes) n->start();
    // Progress-driven waits (round-3 ruling): no test-side wall-clock bound —
    // each wait loops on observed progress; the ctest timeout is the bound.
    for (auto& n : nodes) while (!n->awaitLeader(std::chrono::seconds{5})) {}
    {   // Topics exist only through DDL: the 3-partition test topic, defined through the catalog (topic id 0).
        TopicSpec topic;
        topic.name = "d9"; topic.partitionCount = 3;
        ASSERT_EQ(rune::node::test::TestCluster({nodes[0].get(), nodes[1].get(), nodes[2].get()}).defineTopic(topic), 0);
    }

    // Find (coordinator c, partition p, leader L != c) from c's own routing image,
    // with L's replica of p Leader there.
    int coord = -1, part = -1, leader = -1;
    while (coord < 0) {
        for (int c = 0; c < 3 && coord < 0; ++c) {
            const auto found = nodes[static_cast<std::size_t>(c)]->onControlThreadForTest([&, c] {
                std::pair<int, int> r{-1, -1};
                const auto* img = nodes[static_cast<std::size_t>(c)]->gatewayForTest()->routingImage();
                if (img == nullptr) return r;
                for (int p = 0; p < 3; ++p) {
                    const auto& e = img->lookup(rune::metadata::gpidOf(0, p));
                    if (e.leaderNodeId >= 0 && e.leaderNodeId != c) return std::pair<int, int>{p, e.leaderNodeId};
                }
                return r;
            });
            if (found.first < 0) continue;
            const bool leads = nodes[static_cast<std::size_t>(found.second)]->onOwnerThreadForTest(0, found.first,
                [](PartitionReplica* r) { return r != nullptr && r->role() == Role::Leader && r->highWatermark() >= 0; });
            if (leads) { coord = c; part = found.first; leader = found.second; }
        }
        std::this_thread::yield();
    }
    ASSERT_GE(coord, 0) << "no partition led by a node other than a would-be coordinator";
    BrokerNode& C = *nodes[static_cast<std::size_t>(coord)];
    BrokerNode& L = *nodes[static_cast<std::size_t>(leader)];

    const auto marker = tx::ControlMarker::build(/*pid=*/4242, 0, tx::ControlType::Commit, /*coordinatorEpoch=*/1);
    const auto rc = C.onControlThreadForTest([&] {
        return C.gatewayForTest()->appendControlRecord(0, part, std::span<const uint8_t>(marker), 4242, true, 1);
    });
    ASSERT_EQ(rc, rune::gateway::ClientGateway::kControlRecordAsync);
    EXPECT_EQ(C.onControlThreadForTest([&] { return C.gatewayForTest()->remoteMarkersInFlightForTest(); }), 1u)
        << "the marker was not forwarded to the remote leader";

    bool landed = false;
    while (!landed) {
        landed = L.onOwnerThreadForTest(0, part, [](PartitionReplica& r) { return controlBatchCount(*r.log()) == 1; });
        std::this_thread::yield();
    }
    ASSERT_TRUE(landed) << "the marker never landed on the remote leader through the node wiring";
    std::size_t inFlight = 1;
    while (inFlight != 0) {
        inFlight = C.onControlThreadForTest([&] { return C.gatewayForTest()->remoteMarkersInFlightForTest(); });
        std::this_thread::yield();
    }
    EXPECT_EQ(inFlight, 0u) << "the leader's answer never came back through the node wiring";
    EXPECT_EQ(C.onControlThreadForTest([&] { return C.gatewayForTest()->remoteMarkersLandedForTest(); }), 1u)
        << "re-review LOW-1: the answer must be a LANDED outcome, not a refusal";

    // MEDIUM-3: the stuck-markers gauge counter is registered in the CnC.
    bool counter = false;
    {
        aeron::CncFileReader reader = aeron::CncFileReader::mapExisting(C.driverForTest()->dir().c_str());
        aeron::concurrent::CountersReader counters = reader.countersReader();
        counters.forEach([&](std::int32_t, std::int32_t typeId, const aeron::concurrent::AtomicBuffer&,
                             const std::string&) { counter = counter || typeId == kStuckMarkersCounterTypeId; });
    }
    EXPECT_TRUE(counter) << "the stuck-markers counter is not in the CnC";
    for (auto& n : nodes) n->stop();
}

// Re-review NEW MEDIUM-1: a ONE-WAY outage while the sender is idle toward the
// peer (no offer → the hold router never sees it down) is still reported: the
// link state of EVERY peer is edge-detected each poll.  Node 2's receiving
// side goes away and comes back while node 1 sends nothing to it.
TEST(D9CrossNodeMarkerIntegrationRegression, IdleOneWayOutageIsReportedWhenTheLinkReturns) {
    TmpDir dd;
    rune::node::EmbeddedDriver driver{driverConfig(dd), rune::node::DriverMode::Threaded};
    aeron::Context ctx;
    ctx.aeronDir(driver.dir());
    auto client = aeron::Aeron::connect(ctx);
    const uint16_t p1 = rune::aeronutil::testing::allocFreeUdpPort();
    uint16_t p2 = rune::aeronutil::testing::allocFreeUdpPort();
    while (p2 == p1) p2 = rune::aeronutil::testing::allocFreeUdpPort();
    auto chan = [](uint16_t p) { return "aeron:udp?endpoint=127.0.0.1:" + std::to_string(p); };
    struct Events { std::vector<int32_t> peers; };
    Events ev;
    auto onReconnect = [](void* ctx, int32_t peer) { static_cast<Events*>(ctx)->peers.push_back(peer); };
    rune::replication::AeronPartitionTransport a(client, chan(p1), kStream, /*self=*/1, kRingBytes);
    a.setPlaneReconnectHandler(onReconnect, &ev);
    a.addPeer(2, chan(p2), kStream);
    int64_t now = 1;
    auto until = [&](auto pred, auto&& step) {
        for (int i = 0; i < 20'000'000; ++i) { if (pred()) return true; step(); std::this_thread::yield(); }
        return pred();
    };
    auto b = std::make_unique<rune::replication::AeronPartitionTransport>(client, chan(p2), kStream, 2, kRingBytes);
    ASSERT_TRUE(until([&] { return a.peerLinkUpForTest(2); }, [&] { (void)a.poll(++now); (void)b->poll(now); }))
        << "precondition: node 1's link to node 2 is up";
    b.reset();                                                // node 2's receiving side is gone (node 1 idle)
    ASSERT_TRUE(until([&] { return !a.peerLinkUpForTest(2); }, [&] { (void)a.poll(++now); }))
        << "precondition: node 1 observes the link down without sending anything";
    // No offer was made; the per-peer link edge alone tells the hold router
    // (re-review 2 LOW-2: frames held for a gone peer are dropped at once).
    EXPECT_TRUE(a.peerDownForTest(2)) << "the link-down edge did not reach the hold router";
    ev.peers.clear();
    b = std::make_unique<rune::replication::AeronPartitionTransport>(client, chan(p2), kStream, 2, kRingBytes);
    ASSERT_TRUE(until([&] { return std::find(ev.peers.begin(), ev.peers.end(), 2) != ev.peers.end(); },
                      [&] { (void)a.poll(++now); (void)b->poll(now); }))
        << "the idle one-way outage was never reported when the link came back";
}
