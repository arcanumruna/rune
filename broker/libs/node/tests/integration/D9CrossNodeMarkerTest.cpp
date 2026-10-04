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
#include <rune/transaction/ControlMarker.h>
#include <rune/aeronutil/testing/FreePort.h>
#include <gtest/gtest.h>

#include <rune/node/EmbeddedDriver.h>
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
    rune::gateway::ClientGateway::TxnMarkerChannel channel() {
        return {&FakeChannel::sendMarker, &FakeChannel::sendResponse, this};
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
    explicit GwRig(int32_t selfId) {
        auto rep = std::make_unique<PartitionReplica>(cfg(selfId, 0, 0), Role::Leader, 1, std::vector<NodeId>{selfId},
                                                      std::vector<NodeId>{selfId}, topics.log(0, 0), &t);
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
    R.gw.setRoutingImageSnapshot(imageWith(2, 5));   // republished, unchanged
    EXPECT_TRUE(R.tcs.done.empty()) << "an unchanged leader must not fail the marker";
    R.gw.setRoutingImageSnapshot(imageWith(3, 6));   // leadership moved to node 3
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
    R.gw.setRoutingImageSnapshot(imageWith(2, 6));
    ASSERT_EQ(R.tcs.done.size(), 1u);
    EXPECT_EQ(R.tcs.done[0].err, kNotLeader);
}

// Leadership moving TO this node: the remote marker fails NotLeader and the
// retry takes the local (op ring) path.
TEST(D9RemoteMarkerRegression, LeaderMovingToTheCoordinatorReroutesLocally) {
    GwRig R(1);
    R.gw.setRoutingImageSnapshot(imageWith(2, 5));
    ASSERT_EQ(R.marker(77, true), rune::gateway::ClientGateway::kControlRecordAsync);
    R.gw.setRoutingImageSnapshot(imageWith(1, 6));
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
TEST(D9RemoteMarkerRegression, RemoteMarkerOnANonLeaderIsAnsweredNotLeaderAtOnce) {
    GwRig R(/*self=*/2);
    R.gw.setRoutingImageSnapshot(imageWith(/*leader=*/3, 6));
    const auto bytes = tx::ControlMarker::build(77, 0, tx::ControlType::Commit, 3);
    WriteTxnMarkerMsg m{};
    m.coordinatorNodeId = 1; m.coordinatorEpoch = 3; m.token = 902; m.topicId = 0; m.partition = 0;
    m.producerId = 77; m.commit = 1; m.marker = std::span<const uint8_t>(bytes);
    R.gw.appendRemoteControlRecord(m);
    for (int i = 0; i < 8; ++i) R.runWorker(10 + i);
    EXPECT_EQ(R.r->log()->logEndOffset(), 0);
    ASSERT_EQ(R.ch.responses.size(), 1u);
    EXPECT_EQ(R.ch.responses[0].first, 1);
    EXPECT_EQ(R.ch.responses[0].second.token, 902);
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

// Liveness safety net: a request (or its answer) lost with NO leadership change
// (e.g. a frame dropped for a peer the plane saw down) completes as a retriable
// refusal after the coordinator's max retry backoff, so the TCS re-sends it
// (idempotent).  Correctness never depends on it (§3 idempotence).
TEST(D9RemoteMarkerRegression, UnansweredRemoteMarkerIsRetriedAfterTheMaxBackoff) {
    GwRig R(1);
    R.gw.setRoutingImageSnapshot(imageWith(2, 5));
    (void)R.gw.pumpTxnMarkersForTest(1'000);
    ASSERT_EQ(R.marker(77, true), rune::gateway::ClientGateway::kControlRecordAsync);
    (void)R.gw.pumpTxnMarkersForTest(500'000'000);
    EXPECT_TRUE(R.tcs.done.empty()) << "not before the max backoff";
    (void)R.gw.pumpTxnMarkersForTest(2'500'000'000);
    ASSERT_EQ(R.tcs.done.size(), 1u);
    EXPECT_FALSE(R.tcs.done[0].landed);
    EXPECT_EQ(R.tcs.done[0].err, kNotLeader);
    EXPECT_EQ(R.gw.markersInFlightForTest(), 0u);
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
            c.fetchIntervalNs = kPassNs; c.isrCheckIntervalNs = 10 * kPassNs; c.replicaLagMaxMs = 1'000'000;
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
        for (auto& n : nodes) n->tr->setTxnMarkerHandlers(nullptr, nullptr, nullptr);
    }
    Node& node(int32_t id) { return *nodes[static_cast<std::size_t>(id - 1)]; }
    Node& coord() { return node(3); }
    void setRouting(std::array<int32_t, 2> leaders, std::array<int32_t, 2> epochs) {
        std::vector<rune::metadata::RoutingImage::BuildEntry> e;
        for (int p = 0; p < 2; ++p)
            e.push_back({rune::metadata::gpidOf(0, p), leaders[static_cast<std::size_t>(p)],
                         epochs[static_cast<std::size_t>(p)]});
        auto img = rune::metadata::RoutingImage::build(e);
        for (auto& n : nodes) if (n->alive) n->gw.setRoutingImageSnapshot(img);
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
    // EndTxn: the error if answered synchronously, 999 when deferred (markers in flight).
    int16_t endTxn(bool commit) {
        pc::EndTxn q; q.sessionId = coord().sid; q.correlationId = ++corr; q.producerId = pid;
        q.producerEpoch = epoch; q.commit = commit ? 1 : 0; q.transactionalId = tid();
        std::vector<uint8_t> f(q.encodedSize()); q.encode(f.data());
        const auto n = coord().tcs->handleEndTxn(f.data(), f.size(), now, out.data(), out.size());
        if (n == 0) return 999;
        auto r = pc::EndTxnResponse::decode(out.data(), n);
        return r ? r->errorCode : -1;
    }
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

    ASSERT_EQ(C.endTxn(/*commit=*/true), 999) << "EndTxn deferred while the markers are in flight";
    ASSERT_TRUE(C.passUntil([&] { return C.deferredEndTxnError().has_value(); }))
        << "EndTxn never completed: a marker for a remotely led partition did not land (refused="
        << C.coord().tcs->controlMarkersRefusedForTest() << ")";
    EXPECT_EQ(C.deferredEndTxnError().value_or(-1), 0) << "EndTxn(commit) OK";
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
    ASSERT_EQ(C.endTxn(/*commit=*/false), 999);
    ASSERT_TRUE(C.passUntil([&] { return C.deferredEndTxnError().has_value(); }))
        << "EndTxn(abort) never completed (refused=" << C.coord().tcs->controlMarkersRefusedForTest() << ")";
    EXPECT_EQ(C.deferredEndTxnError().value_or(-1), 0);
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
    ASSERT_EQ(C.endTxn(/*commit=*/true), 999);
    ASSERT_TRUE(C.passUntil([&] { return controlBatchCount(C.node(2).log(1)) == 1; }))
        << "p1's marker (live leader) landed";
    for (int i = 0; i < 200; ++i) C.pass();
    ASSERT_FALSE(C.deferredEndTxnError().has_value()) << "precondition: p0's marker is stuck on the dead leader";
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
    ASSERT_TRUE(C.passUntil([&] { return C.deferredEndTxnError().has_value(); }, 5'000))
        << "EndTxn never completed after p0's leadership moved (refused="
        << C.coord().tcs->controlMarkersRefusedForTest() << ")";
    EXPECT_EQ(C.deferredEndTxnError().value_or(-1), 0);
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
