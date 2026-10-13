// TransientGatewayTest.cpp — Phase 2b E2: the gateway serves a TRANSIENT
// partition (no disk Log) through its owning worker — produce routed to the
// worker, Subscribe accepted and clamped to the HW snapshot, PushReads issued
// (legacy worker-pool mode), group subscriptions, txn markers routed — gates it
// by C1 exactly like a disk topic (uncommitted → NotLeader, nothing served),
// answers a full ring TransientLogFull (42) to v6 sessions and NotLeader to
// older ones, and refuses it (UnknownPartition) without a worker pool: no
// gateway-inline path may touch a worker-owned ring.  Deterministic: no Aeron.
#include <rune/gateway/ClientGateway.h>
#include <rune/gateway/TopicRegistry.h>
#include <rune/metadata/Gpid.h>
#include <rune/metadata/RoutingImage.h>
#include <rune/metadata/ShardRouter.h>
#include <rune/node/ReplicaRegistry.h>
#include <rune/node/WorkerOp.h>
#include <rune/node/WorkerPool.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/protocol/client/ControlMessages.h>
#include <rune/protocol/client/DataMessages.h>
#include <rune/protocol/client/Protocol.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/replication/ReplicationMessages.h>
#include <rune/storage/InMemoryLog.h>
#include <rune/transaction/ControlMarker.h>

#include <rune_protocol_internal_sbe/MessageHeader.h>
#include <rune_protocol_internal_sbe/WorkerOp.h>

#include <gtest/gtest.h>

#include <filesystem>
#include <map>
#include <memory>
#include <optional>
#include <random>
#include <span>
#include <string>
#include <vector>

using namespace rune::gateway;
namespace pc = rune::protocol::client;
namespace fs = std::filesystem;
using rune::replication::PartitionConfig;
using rune::replication::PartitionReplica;
using rune::replication::Role;

namespace {

struct TempDir {
    fs::path path;
    TempDir() {
        std::random_device rd;
        path = fs::temp_directory_path() / ("rune-transient-gw-" + std::to_string(rd()));
        fs::create_directories(path);
    }
    ~TempDir() { std::error_code ec; fs::remove_all(path, ec); }
};

std::vector<uint8_t> batchOf(int recs, std::size_t valueBytes = 1) {
    rune::protocol::RecordBatchBuilder b(0, 1000);
    const std::string v(valueBytes, 'v');
    for (int i = 0; i < recs; ++i) b.append("k", v, 1000 + i);
    return b.build();
}

struct RigOpts {
    bool        workers      = true;      // a 1-worker pool (legacy mode: the gateway issues PushReads)
    bool        runtime      = false;     // create the topic at RUNTIME: awaiting its commit (C1) until commit()
    std::size_t ringBytes    = 1u << 20;
    bool        withFollower = false;     // ISR {1,2}: follower 2 never fetches → the HW stays 0
    bool        asFollower   = false;     // this node's replica is a FOLLOWER (leader 2)
    int32_t     partitions   = 1;         // topic 0's partition count (each a leader replica on its own ring)
};

// Gateway + topic 0 "presence" (1 partition, TRANSIENT) + its leader replica on
// an InMemoryLog (built at construction, or — runtime — only on commit(), the
// way BrokerNode builds it on kA2cTopicCommitted).
struct Rig {
    TempDir                                   dir;
    RigOpts                                   o;
    rune::storage::InMemoryLog                mem;           // outlives the replica
    rune::replication::NullPartitionTransport transport;
    rune::node::ReplicaRegistry               reg;
    std::unique_ptr<TopicRegistry>            registry;
    std::unique_ptr<rune::node::WorkerPool>   pool;
    ClientGateway                             gw;
    PartitionReplica*                         rep = nullptr;
    std::map<int64_t, pc::ProduceResponse>    produced;      // by correlationId
    std::map<int64_t, pc::SubscribeResponse>  subscribed;    // by correlationId
    std::map<int64_t, int16_t>                errors;        // ErrorResponse code by correlationId
    std::map<int64_t, std::string>            errorMessages; // ErrorResponse message by correlationId
    std::vector<pc::Records>                  records;       // baseOffset / subscriptionId only
    int64_t                                   now  = 1'000'000'000;
    int64_t                                   corr = 0;

    explicit Rig(RigOpts opts = {})
        : o(opts),
          mem(opts.ringBytes, 10'000),
          gw(ClientGateway::makeForTest([this](const uint8_t* b, std::size_t n) -> int64_t {
              onFrame(b, n, /*sessionEgress=*/false);
              return 1;
          }, 1)) {
        rune::node::TopicSpec ts;
        ts.name = "presence"; ts.partitionCount = o.partitions; ts.topicId = 0;
        ts.persistence = rune::node::PersistenceMode::Transient;
        if (o.runtime) {
            registry = std::make_unique<TopicRegistry>(std::vector<rune::node::TopicSpec>{}, dir.path.string());
            EXPECT_EQ(registry->createTopic(ts), 0);   // C1: awaiting its catalog commit
            EXPECT_TRUE(registry->awaitingCommit(0));
        } else {
            registry = std::make_unique<TopicRegistry>(std::vector<rune::node::TopicSpec>{ts}, dir.path.string());
        }
        if (o.workers) {
            pool = std::make_unique<rune::node::WorkerPool>(1, rune::metadata::ShardRouter(1));
            pool->setRegistry(&reg);
        }
        if (!o.runtime) addReplica();
        gw.setTopicRegistry(registry.get());
        gw.setReplicaRegistry(&reg);
        gw.setCommittedMetadataReady(true);
        gw.setOnEgressFrame([this](int64_t, const uint8_t* b, std::size_t n) { onFrame(b, n, true); });
        if (pool) gw.setWorkerPool(pool.get());
    }
    std::vector<std::unique_ptr<rune::storage::InMemoryLog>> extraMems;   // partitions 1.. (fix round 2)
    std::vector<PartitionReplica*>                           extraReps;
    void addReplica() {
        for (int32_t p = 1; p < o.partitions; ++p) {
            extraMems.push_back(std::make_unique<rune::storage::InMemoryLog>(o.ringBytes, 10'000));
            PartitionConfig c;
            c.topicId = 0; c.partitionId = p; c.selfId = 1; c.minIsr = 1;
            c.replicaLagMaxOffset = 1'000'000'000; c.replicaLagMaxMs = 600'000;
            auto r = std::make_unique<PartitionReplica>(c, Role::Leader, 1, std::vector<int32_t>{1},
                                                        std::vector<int32_t>{1}, extraMems.back().get(), &transport);
            r->setExternalFollowerFlush(true);
            extraReps.push_back(r.get());
            reg.add(0, p, std::move(r));
        }
        PartitionConfig c;
        c.topicId = 0; c.partitionId = 0; c.selfId = 1; c.minIsr = 1;
        c.replicaLagMaxOffset = 1'000'000'000; c.replicaLagMaxMs = 600'000;
        const std::vector<int32_t> set = o.withFollower ? std::vector<int32_t>{1, 2} : std::vector<int32_t>{1};
        const std::vector<int32_t> all = o.asFollower ? std::vector<int32_t>{1, 2} : set;
        auto r = std::make_unique<PartitionReplica>(c, o.asFollower ? Role::Follower : Role::Leader, 1, all, all,
                                                    &mem, &transport);
        r->setExternalFollowerFlush(true);
        rep = r.get();
        reg.add(0, 0, std::move(r));
        if (pool) pool->assignFromRegistry(reg);
    }
    /// What BrokerNode's kA2cTopicCommitted handler does: the topic becomes
    /// servable and its replicas are built (createRuntimeUserReplicas).
    void commit() {
        ASSERT_TRUE(registry->commitTopic(0));
        addReplica();
    }
    void onFrame(const uint8_t* b, std::size_t n, bool sessionEgress) {
        const auto h = pc::MessageHeader::decode(b, n);
        if (!h) return;
        switch (static_cast<pc::TemplateId>(h->templateId)) {
        case pc::TemplateId::ProduceResponse:
            if (auto r = pc::ProduceResponse::decode(b, n)) produced[r->correlationId] = *r;
            break;
        case pc::TemplateId::SubscribeResponse:
            if (auto r = pc::SubscribeResponse::decode(b, n)) subscribed[r->correlationId] = *r;
            break;
        case pc::TemplateId::ErrorResponse:
            if (auto r = pc::ErrorResponse::decode(b, n)) {
                errors[r->correlationId]        = r->code;
                errorMessages[r->correlationId] = std::string(r->message);
            }
            break;
        case pc::TemplateId::Records:
            if (sessionEgress) if (auto r = pc::Records::decode(b, n)) records.push_back(*r);
            break;
        default: break;
        }
    }
    void pass() {
        now += 1'000'000;
        if (pool) (void)pool->worker(0).doWork(now);
        gw.drainWorkerReturnsForTest(now);
    }
    int64_t produce(int64_t sid, int recs, uint8_t acks = 1, std::size_t valueBytes = 1) {
        pc::Produce msg;
        msg.sessionId = sid; msg.correlationId = ++corr; msg.topicId = 0; msg.partition = 0;
        msg.partitionKnown = 1; msg.acks = acks; msg.producerId = -1; msg.producerEpoch = 0; msg.baseSequence = -1;
        const auto batch = batchOf(recs, valueBytes);
        msg.recordBatch = std::span<const uint8_t>(batch);
        std::vector<uint8_t> frame(msg.encodedSize());
        msg.encode(frame.data());
        gw.applyProduceForTest(frame.data(), frame.size());
        return corr;
    }
    std::optional<pc::ProduceResponse> awaitProduced(int64_t c) {
        for (int i = 0; i < 64 && !produced.count(c); ++i) pass();
        const auto it = produced.find(c);
        return it == produced.end() ? std::nullopt : std::optional<pc::ProduceResponse>(it->second);
    }
    void subscribe(int64_t sid, int64_t fromOffset, int64_t subCorr, uint8_t isolation = 0, int32_t partition = 0) {
        pc::Subscribe s;
        s.sessionId = sid; s.correlationId = subCorr; s.topicId = 0; s.partition = partition;
        s.partitionKnown = 1; s.fromOffset = fromOffset; s.isolation = isolation;
        std::vector<uint8_t> buf(s.encodedSize());
        buf.resize(s.encode(buf.data()));
        gw.applySubscribeForTest(buf.data(), buf.size());
    }
    // ---- Remote (cluster-plane) txn markers: a peer coordinator's WriteTxnMarker ----
    std::vector<std::pair<int32_t, rune::replication::WriteTxnMarkerResponseMsg>> markerAnswers;   // (origin, answer)
    std::vector<uint8_t> markerBytes = rune::transaction::ControlMarker::build(7, 0,
                                                                              rune::transaction::ControlType::Commit, 0);
    static bool captureMarkerAnswer(void* self, int32_t to, const rune::replication::WriteTxnMarkerResponseMsg& a) {
        static_cast<Rig*>(self)->markerAnswers.emplace_back(to, a);
        return true;
    }
    void captureMarkerAnswers() {
        gw.setTxnMarkerChannel({.sendMarker = nullptr, .sendResponse = &Rig::captureMarkerAnswer, .ctx = this});
    }
    /// A WriteTxnMarker from coordinator node 9 for presence/0 (producer 7, COMMIT).
    rune::replication::WriteTxnMarkerMsg remoteMarker(int64_t token) const {
        rune::replication::WriteTxnMarkerMsg m{};
        m.coordinatorNodeId = 9; m.coordinatorEpoch = 0; m.token = token; m.topicId = 0; m.partition = 0;
        m.producerId = 7; m.commit = 1; m.leaderEpoch = 1;   // the rig replica's leader epoch
        m.marker = std::span<const uint8_t>(markerBytes);
        return m;
    }
    /// Pass cycles until `n` marker answers arrived (bounded — never a hang).
    bool answersUntil(std::size_t n) {
        for (int i = 0; i < 64 && markerAnswers.size() < n; ++i) pass();
        return markerAnswers.size() >= n;
    }

    /// Push cycles (PushRead → worker → PushReadResult → Records) until n frames (bounded).
    bool pushUntil(std::size_t n) {
        for (int i = 0; i < 64 && records.size() < n; ++i) { gw.pushSubscriptionsForTest(); pass(); }
        return records.size() >= n;
    }
};

constexpr int16_t kOk               = 0;
constexpr int16_t kNotLeader        = static_cast<int16_t>(pc::ErrorCode::NotLeader);
constexpr int16_t kUnknownPartition = static_cast<int16_t>(pc::ErrorCode::UnknownPartition);
constexpr int16_t kTransientLogFull = static_cast<int16_t>(pc::ErrorCode::TransientLogFull);

} // namespace

TEST(TransientGatewayRegression, SubscribeToATransientPartitionIsAcceptedRegression) {
    Rig r;
    const int64_t sid = r.gw.injectTestSession(0);
    r.subscribe(sid, /*Earliest=*/-2, 41);
    ASSERT_TRUE(r.subscribed.count(41));
    EXPECT_EQ(r.subscribed[41].errorCode, kOk) << "was UnknownPartition: the topic has no disk Log";
    EXPECT_EQ(r.gw.subscriptionNextOffsetForTest(r.subscribed[41].subscriptionId), 0);
}

TEST(TransientGatewayRegression, ProduceThroughTheWorkerAndPushToTheSubscriberRegression) {
    Rig r;
    const int64_t sid = r.gw.injectTestSession(0);
    const auto p1 = r.awaitProduced(r.produce(sid, 3, /*acks=all*/1));
    ASSERT_TRUE(p1.has_value());
    ASSERT_EQ(p1->errorCode, kOk) << "was UnknownPartition: the topic has no disk Log";
    EXPECT_EQ(p1->baseOffset, 0);
    const auto p2 = r.awaitProduced(r.produce(sid, 2, 1));
    ASSERT_TRUE(p2.has_value());
    ASSERT_EQ(p2->errorCode, kOk);
    EXPECT_EQ(p2->baseOffset, 3);
    EXPECT_EQ(r.mem.logEndOffset(), 5) << "the owning worker appended to the ring";

    r.subscribe(sid, -2, 42);
    ASSERT_TRUE(r.pushUntil(2)) << "no Records pushed for a transient subscription";
    EXPECT_EQ(r.records[0].baseOffset, 0);
    EXPECT_EQ(r.records[1].baseOffset, 3);
    EXPECT_EQ(r.records[0].subscriptionId, r.subscribed[42].subscriptionId);
}

TEST(TransientGatewayRegression, SubscribeLatestAndExplicitOffsetClampToTheHighWatermarkRegression) {
    Rig r;
    const int64_t sid = r.gw.injectTestSession(0);
    ASSERT_EQ(r.awaitProduced(r.produce(sid, 1)).value().errorCode, kOk);
    ASSERT_EQ(r.awaitProduced(r.produce(sid, 1)).value().errorCode, kOk);
    for (int i = 0; i < 4; ++i) r.pass();   // the worker published its HW
    ASSERT_EQ(r.rep->gatewayHighWatermark(), 2);
    r.subscribe(sid, /*Latest=*/-1, 101);
    r.subscribe(sid, 1, 102);
    r.subscribe(sid, 99, 103);
    for (int64_t c : {101, 102, 103}) ASSERT_EQ(r.subscribed.at(c).errorCode, kOk);
    EXPECT_EQ(r.gw.subscriptionNextOffsetForTest(r.subscribed[101].subscriptionId), 2) << "Latest replays old records";
    EXPECT_EQ(r.gw.subscriptionNextOffsetForTest(r.subscribed[102].subscriptionId), 1);
    EXPECT_EQ(r.gw.subscriptionNextOffsetForTest(r.subscribed[103].subscriptionId), 2) << "beyond the HW clamps to it";
}

TEST(TransientGatewayRegression, GroupSubscriptionOnATransientPartitionIsPushedRegression) {
    Rig r;
    const int64_t sid = r.gw.injectTestSession(0);
    ASSERT_EQ(r.awaitProduced(r.produce(sid, 2)).value().errorCode, kOk);
    const int64_t subId = r.gw.addGroupSubscription(sid, 0, 0, /*fromOffset=*/0, /*isolation=*/0,
                                                    /*pendingCarrier=*/false);
    ASSERT_GE(subId, 0) << "a group member could not be assigned a transient partition (was: -1)";
    ASSERT_TRUE(r.pushUntil(1));
    EXPECT_EQ(r.records[0].subscriptionId, subId);
    EXPECT_EQ(r.records[0].baseOffset, 0);
}

TEST(TransientGatewayRegression, TxnMarkerForATransientPartitionIsRoutedToTheWorkerRegression) {
    Rig r;
    const auto marker = rune::transaction::ControlMarker::build(7, 0, rune::transaction::ControlType::Commit, 0);
    EXPECT_EQ(r.gw.appendControlRecord(0, 0, std::span<const uint8_t>(marker), /*producerId=*/7, /*commit=*/true,
                                       /*coordinatorEpoch=*/0),
              ClientGateway::kControlRecordAsync) << "was -1 (dropped): no disk Log";
    for (int i = 0; i < 8; ++i) r.pass();
    EXPECT_EQ(r.mem.logEndOffset(), 1) << "the owning worker appended the marker to the ring";
}

// C1 (DDL conformance final review) × E2: a RUNTIME transient topic whose
// catalog record has not committed serves NOTHING — produce NotLeader (nothing
// appended), subscribe the bare ErrorResponse{NotLeader} (nothing registered),
// no group subscription, no marker — exactly like an uncommitted disk topic.
// Once committed (and its replica built, as kA2cTopicCommitted does) it serves.
TEST(TransientGatewayRegression, UncommittedTransientTopicServesNothingUntilCommitRegression) {
    Rig r({.workers = true, .runtime = true});
    const int64_t sid = r.gw.injectTestSession(0);
    const int64_t c = r.produce(sid, 1);
    ASSERT_TRUE(r.produced.count(c));
    EXPECT_EQ(r.produced[c].errorCode, kNotLeader) << "an uncommitted transient topic took a produce";
    EXPECT_EQ(r.mem.logEndOffset(), 0);
    r.subscribe(sid, -2, 51);
    EXPECT_FALSE(r.subscribed.count(51)) << "an uncommitted transient topic registered a subscription";
    ASSERT_TRUE(r.errors.count(51));
    EXPECT_EQ(r.errors[51], kNotLeader);
    EXPECT_EQ(r.gw.addGroupSubscription(sid, 0, 0, 0, 0, false), -1);
    const auto marker = rune::transaction::ControlMarker::build(7, 0, rune::transaction::ControlType::Commit, 0);
    EXPECT_EQ(r.gw.appendControlRecord(0, 0, std::span<const uint8_t>(marker), 7, true, 0), -1);

    r.commit();
    const auto ok = r.awaitProduced(r.produce(sid, 2));
    ASSERT_TRUE(ok.has_value());
    EXPECT_EQ(ok->errorCode, kOk) << "the committed transient topic must serve";
    EXPECT_EQ(ok->baseOffset, 0);
    r.subscribe(sid, -2, 52);
    ASSERT_TRUE(r.subscribed.count(52));
    EXPECT_EQ(r.subscribed[52].errorCode, kOk);
    ASSERT_TRUE(r.pushUntil(1));
    EXPECT_EQ(r.records[0].baseOffset, 0);
}

// The DDL final-fix concern: a COMMITTED runtime transient topic produced
// through the gateway (legacy runtime) answered UnknownPartition (8) because it
// has no servable disk Log.  It is served through its owning worker.
TEST(TransientGatewayRegression, CommittedRuntimeTransientTopicServesThroughTheWorkerRegression) {
    Rig r({.workers = true, .runtime = true});
    r.commit();
    const int64_t sid = r.gw.injectTestSession(0);
    for (int64_t want : {0, 2}) {
        const auto p = r.awaitProduced(r.produce(sid, 2, /*acks=all*/1));
        ASSERT_TRUE(p.has_value());
        ASSERT_EQ(p->errorCode, kOk) << "was UnknownPartition (8)";
        EXPECT_EQ(p->baseOffset, want);
    }
    r.subscribe(sid, -2, 61);
    ASSERT_TRUE(r.pushUntil(2));
    EXPECT_EQ(r.records[0].baseOffset, 0);
    EXPECT_EQ(r.records[1].baseOffset, 2);
}

// Ruling 2 on the wire: a ring full of not-yet-replicated records answers 42 to
// a v6 session and NotLeader (its retriable code) to a v5 session; nothing is
// appended either way.
TEST(TransientGatewayRegression, FullRingAnswersTransientLogFullOnlyToV6SessionsRegression) {
    Rig r({.workers = true, .ringBytes = 16 * 1024, .withFollower = true});
    const int64_t v6 = r.gw.injectTestSession(0);
    const int64_t v5 = r.gw.injectTestSession(0, /*clientVersion=*/5);
    int16_t code = kOk;
    for (int i = 0; i < 64 && code == kOk; ++i) {
        const auto p = r.awaitProduced(r.produce(v6, 1, /*acks=*/0, /*valueBytes=*/1000));
        ASSERT_TRUE(p.has_value());
        code = p->errorCode;
    }
    ASSERT_EQ(code, kTransientLogFull) << "a v6 session must see 42 for a full ring";
    const int64_t leo = r.mem.logEndOffset();
    const auto old = r.awaitProduced(r.produce(v5, 1, 0, 1000));
    ASSERT_TRUE(old.has_value());
    EXPECT_EQ(old->errorCode, kNotLeader) << "a v5 session must never see 42";
    EXPECT_EQ(r.mem.logEndOffset(), leo) << "a refused produce appended";
    EXPECT_EQ(r.mem.logStartOffset(), 0) << "an unreplicated record was evicted";
}

// Pins "never inline": without a worker pool nothing may touch a worker-owned
// ring — every path refuses the transient partition explicitly.
TEST(TransientGateway, WithoutAWorkerPoolTransientIsRefusedNeverInline) {
    Rig r({.workers = false});
    const int64_t sid = r.gw.injectTestSession(0);
    const int64_t c = r.produce(sid, 1);
    ASSERT_TRUE(r.produced.count(c));
    EXPECT_EQ(r.produced[c].errorCode, kUnknownPartition);
    EXPECT_EQ(r.mem.logEndOffset(), 0);
    r.subscribe(sid, -2, 9);
    ASSERT_TRUE(r.subscribed.count(9));
    EXPECT_EQ(r.subscribed[9].errorCode, kUnknownPartition);
    EXPECT_EQ(r.gw.addGroupSubscription(sid, 0, 0, 0, 0, false), -1);
}

// ---------------------------------------------------------------------------
// Fix round 1: the cluster-plane entry appendRemoteControlRecord (a PEER
// coordinator's WriteTxnMarker for a transient partition this node leads).
// ---------------------------------------------------------------------------

// (a) With a worker pool the remote marker is routed to the owning worker on the
// op ring with NO disk Log (logPtr == nullptr: the worker appends to the replica's
// ring) — and, run through the worker, it lands and is answered to the origin.
TEST(TransientGatewayRegression, RemoteMarkerForATransientPartitionIsRoutedToTheWorkerRegression) {
    namespace sbe = rune::protocol::internal::sbe;
    {
        Rig r;
        r.captureMarkerAnswers();
        r.gw.appendRemoteControlRecord(r.remoteMarker(5001));
        EXPECT_EQ(r.gw.remoteMarkersInFlightForTest(), 0u) << "a leader-side marker is not a coordinator-side remote one";
        EXPECT_EQ(r.gw.markersInFlightForTest(), 1u) << "the remote marker was not taken";
        EXPECT_TRUE(r.markerAnswers.empty()) << "answered before the worker appended";
        // Read the op the gateway put on worker 0's op ring (this test is its consumer).
        int ops = 0;
        uint64_t logPtr = ~0ull, replica = 0;
        int opType = -1;
        (void)r.pool->worker(0).opRingForDoorbell().drain(
            [&](int32_t, aeron::concurrent::AtomicBuffer& buf, aeron::util::index_t off, aeron::util::index_t len) {
                char* cbuf = reinterpret_cast<char*>(buf.buffer()) + off;
                sbe::MessageHeader mh;
                mh.wrap(cbuf, 0, 0, static_cast<uint64_t>(len));
                sbe::WorkerOp op;
                op.wrapForDecode(cbuf, sbe::MessageHeader::encodedLength(), mh.blockLength(), mh.version(),
                                 static_cast<uint64_t>(len));
                ++ops;
                opType  = static_cast<int>(static_cast<uint8_t>(op.opType()));
                logPtr  = op.logPtr();
                replica = op.replica();
            }, 16);
        ASSERT_EQ(ops, 1);
        EXPECT_EQ(opType, static_cast<int>(rune::node::WorkerOp::Type::AppendControlRecord));
        EXPECT_EQ(logPtr, 0u) << "a transient marker op carried a disk Log*";
        EXPECT_EQ(replica, reinterpret_cast<uint64_t>(r.rep)) << "not stamped with the owning replica";
    }
    {
        Rig r;
        r.captureMarkerAnswers();
        r.gw.appendRemoteControlRecord(r.remoteMarker(5002));
        ASSERT_TRUE(r.answersUntil(1)) << "the origin coordinator was never answered (hang)";
        EXPECT_EQ(r.markerAnswers[0].first, 9);
        EXPECT_EQ(r.markerAnswers[0].second.token, 5002);
        EXPECT_EQ(r.markerAnswers[0].second.errorCode, kOk);
        EXPECT_EQ(r.markerAnswers[0].second.offset, 0);
        EXPECT_EQ(r.mem.logEndOffset(), 1) << "the owning worker appended the marker to the ring";
        EXPECT_EQ(r.gw.markersInFlightForTest(), 0u);
    }
}

// (b) Without a worker pool nothing may append to a worker-owned ring: the
// origin is answered NotLeader (retriable; offset -1), nothing appended.
TEST(TransientGatewayRegression, RemoteMarkerForATransientPartitionWithoutAPoolAnswersNotLeaderRegression) {
    Rig r({.workers = false});
    r.captureMarkerAnswers();
    r.gw.appendRemoteControlRecord(r.remoteMarker(5003));
    ASSERT_EQ(r.markerAnswers.size(), 1u) << "the origin coordinator was never answered (hang)";
    EXPECT_EQ(r.markerAnswers[0].first, 9);
    EXPECT_EQ(r.markerAnswers[0].second.token, 5003);
    EXPECT_EQ(r.markerAnswers[0].second.errorCode, kNotLeader);
    EXPECT_EQ(r.markerAnswers[0].second.offset, -1);
    EXPECT_EQ(r.mem.logEndOffset(), 0);
    EXPECT_EQ(r.gw.markersInFlightForTest(), 0u);
}

// (c) C1: an UNCOMMITTED runtime transient topic takes no remote marker —
// NotLeader to the origin (it re-resolves), nothing appended; once committed
// the coordinator's re-sent copy (same token) lands.
TEST(TransientGatewayRegression, RemoteMarkerForAnUncommittedTransientTopicAnswersNotLeaderRegression) {
    Rig r({.workers = true, .runtime = true});
    r.captureMarkerAnswers();
    r.gw.appendRemoteControlRecord(r.remoteMarker(5004));
    ASSERT_EQ(r.markerAnswers.size(), 1u) << "the origin coordinator was never answered (hang)";
    EXPECT_EQ(r.markerAnswers[0].second.token, 5004);
    EXPECT_EQ(r.markerAnswers[0].second.errorCode, kNotLeader);
    EXPECT_EQ(r.markerAnswers[0].second.offset, -1);
    EXPECT_EQ(r.gw.markersInFlightForTest(), 0u) << "an uncommitted topic took a marker";

    r.commit();
    r.gw.appendRemoteControlRecord(r.remoteMarker(5004));   // the coordinator's retry
    ASSERT_TRUE(r.answersUntil(2));
    EXPECT_EQ(r.markerAnswers[1].second.token, 5004);
    EXPECT_EQ(r.markerAnswers[1].second.errorCode, kOk) << "the committed topic must take the retried marker";
    EXPECT_EQ(r.markerAnswers[1].second.offset, 0);
    EXPECT_EQ(r.mem.logEndOffset(), 1);
}

// (d) Idempotence: a re-sent copy of the same token — while in flight and after
// it was answered — is never appended to the ring again; the answered copy is
// re-answered with the original outcome.
TEST(TransientGatewayRegression, ResentRemoteMarkerForATransientPartitionIsAppendedOnceRegression) {
    Rig r;
    r.captureMarkerAnswers();
    const auto m = r.remoteMarker(5005);
    r.gw.appendRemoteControlRecord(m);
    r.gw.appendRemoteControlRecord(m);                       // copy while the first is in flight
    EXPECT_EQ(r.gw.markersInFlightForTest(), 1u) << "the in-flight copy was routed again";
    ASSERT_TRUE(r.answersUntil(1));
    for (int i = 0; i < 8; ++i) r.pass();
    ASSERT_EQ(r.markerAnswers.size(), 1u) << "one answer for the in-flight pair";
    EXPECT_EQ(r.markerAnswers[0].second.errorCode, kOk);
    EXPECT_EQ(r.mem.logEndOffset(), 1);

    r.gw.appendRemoteControlRecord(m);                       // copy after the answer
    for (int i = 0; i < 8; ++i) r.pass();
    EXPECT_EQ(r.mem.logEndOffset(), 1) << "the answered copy was appended again";
    ASSERT_EQ(r.markerAnswers.size(), 2u) << "the answered copy was not re-answered";
    EXPECT_EQ(r.markerAnswers[1].second.token, 5005);
    EXPECT_EQ(r.markerAnswers[1].second.errorCode, kOk);
    EXPECT_EQ(r.markerAnswers[1].second.offset, 0) << "re-answered with another outcome";
}

// ---------------------------------------------------------------------------
// E2 Task 11 fix round 2 (controller ruling 2026-10-13): read_committed is
// served from a LEADER's RC state only (RcState I2).  A follower used to answer
// the read_committed Subscribe OK and then deliver nothing — a silent stall.
// It now answers the bare ErrorResponse{NotLeader} (retriable: the client
// refreshes metadata and re-routes to the leader) and registers nothing.
// read_uncommitted follower reads are unchanged.  (Disk twins + both worker
// modes: the interaction matrix, R7.)
// ---------------------------------------------------------------------------
TEST(TransientGatewayRegression, ReadCommittedSubscribeOnAFollowerAnswersNotLeaderRegression) {
    Rig r(RigOpts{.asFollower = true});
    const int64_t sid = r.gw.injectTestSession(0);
    r.subscribe(sid, /*Earliest=*/-2, 51, /*read_committed=*/1);
    r.pass();
    EXPECT_FALSE(r.subscribed.count(51)) << "a follower answered read_committed OK (then delivered nothing)";
    ASSERT_TRUE(r.errors.count(51)) << "the read_committed Subscribe was never answered";
    EXPECT_EQ(r.errors[51], kNotLeader);

    r.subscribe(sid, -2, 52, /*read_uncommitted=*/0);   // follower reads stay served
    ASSERT_TRUE(r.subscribed.count(52));
    EXPECT_EQ(r.subscribed[52].errorCode, kOk);
}

TEST(TransientGatewayRegression, ReadCommittedSubscribeOnALeaderIsAcceptedRegression) {
    Rig r;
    const int64_t sid = r.gw.injectTestSession(0);
    r.subscribe(sid, -2, 53, /*read_committed=*/1);
    ASSERT_TRUE(r.subscribed.count(53));
    EXPECT_EQ(r.subscribed[53].errorCode, kOk);
    EXPECT_FALSE(r.errors.count(53));
}

TEST(TransientGatewayRegression, ReadCommittedGroupSubscriptionOnAFollowerIsRefusedRegression) {
    Rig r(RigOpts{.asFollower = true});
    const int64_t sid = r.gw.injectTestSession(0);
    EXPECT_LT(r.gw.addGroupSubscription(sid, 0, 0, 0, /*isolation=*/1, /*pendingCarrier=*/false), 0)
        << "a follower registered a read_committed group subscription";
    const int64_t ru = r.gw.addGroupSubscription(sid, 0, 0, 0, /*isolation=*/0, /*pendingCarrier=*/false);
    ASSERT_GE(ru, 0) << "read_uncommitted group reads on a follower stay served";

    // A group carrier switching that member's subscription to read_committed is refused too.
    r.subscribe(sid, -2, /*untagged carrier=*/0, /*read_committed=*/1);
    ASSERT_TRUE(r.errors.count(0));
    EXPECT_EQ(r.errors[0], kNotLeader);
}

// ---------------------------------------------------------------------------
// Task 11 carry (E2 Task 13): a read_committed subscription ACCEPTED while this
// node led the partition, which it then stops leading (demoted).  No RC state
// serves it here any more — it used to stay registered and push nothing,
// forever (a silent stall; disk and transient, both modes: the BrokerNode
// proof is DemotedLeaderReadCommittedRegression).  The gateway now answers its
// Subscribe correlation id with the bare ErrorResponse{NotLeader} ONCE and
// drops it (the client re-subscribes from its position on the leader).
// read_uncommitted subscriptions keep their follower reads.
// ---------------------------------------------------------------------------
TEST(TransientGatewayRegression, ReadCommittedSubscriptionOnADemotedLeaderAnswersNotLeaderOnceRegression) {
    Rig r;
    const int64_t sid = r.gw.injectTestSession(0);
    r.subscribe(sid, -2, 61, /*read_committed=*/1);
    r.subscribe(sid, -2, 62, /*read_uncommitted=*/0);
    ASSERT_TRUE(r.subscribed.count(61));
    ASSERT_TRUE(r.subscribed.count(62));
    r.gw.pushSubscriptionsForTest();
    r.pass();
    EXPECT_FALSE(r.errors.count(61)) << "a LEADER's read_committed subscription was retired";
    EXPECT_EQ(r.gw.readCommittedSubscriptionsMovedAway(), 0u);

    // Demoted, as a node does it: the owner flips the replica (the rig is its
    // thread) and the control plane records the routed role
    // (ReplicationMetadataBridge::recordIntendedRole_) — the sweep's trigger.
    r.rep->becomeFollower(/*leaderEpoch=*/2, /*leaderId=*/2);
    for (int i = 0; i < 4; ++i) { r.gw.pushSubscriptionsForTest(); r.pass(); }
    EXPECT_FALSE(r.errors.count(61)) << "swept on a pass that is neither a routed role change nor the fallback";
    const auto h = r.reg.lookupHandle(0, 0);
    r.reg.setIntendedRole(0, 0, h.incarnation, rune::node::ReplicaRegistry::IntendedRole::Follower);
    for (int i = 0; i < 4; ++i) { r.gw.pushSubscriptionsForTest(); r.pass(); }
    ASSERT_TRUE(r.errors.count(61))
        << "SILENT STALL: the demoted leader kept the read_committed subscription and never told the client";
    EXPECT_EQ(r.errors[61], kNotLeader) << "the move signal is the retriable bare NotLeader";
    EXPECT_EQ(r.gw.readCommittedSubscriptionsMovedAway(), 1u) << "answered and dropped exactly once";
    EXPECT_FALSE(r.errors.count(62)) << "a read_uncommitted subscription was retired (follower reads are served)";
    // The same correlation id is a NEW subscription once the old one is gone (dedupe
    // index cleared) — and, here on a follower, refused NotLeader at subscribe time.
    r.errors.erase(61);
    r.subscribe(sid, 4, 61, /*read_committed=*/1);
    ASSERT_TRUE(r.errors.count(61));
    EXPECT_EQ(r.errors[61], kNotLeader);
    EXPECT_EQ(r.gw.readCommittedSubscriptionsMovedAway(), 1u);
}

// ---------------------------------------------------------------------------
// Task 13 fix round 1 — every way a node stops serving a subscription reaches
// the client (no silent stall):
//   (a) a group member's partition subscription (the member subscribes to the
//       LEADER with a bit-61-tagged correlation id; the coordinator creates
//       none) is answered NotLeader on THAT id — the client's group code
//       re-routes the partition from its position;
//   (b) a demotion no control-plane route reports (a leader fenced by a higher
//       epoch flips its own role; no intended-role record) is caught by the
//       spread fallback scan (a bounded slice every kMovedAwayScanEveryCycles passes);
//   (c) the partition's replica REMOVED from this node (it moved away) retires
//       every plain subscription, read_uncommitted too.
// ---------------------------------------------------------------------------
TEST(TransientGatewayRegression, GroupMemberPartitionSubscriptionOnADemotedLeaderAnswersNotLeaderRegression) {
    Rig r;
    const int64_t sid = r.gw.injectTestSession(0);
    constexpr int64_t kFetchCorr = (int64_t{1} << 61) | 7;   // rune-client GroupConsumerHub::kFetchCorrTag
    r.subscribe(sid, 0, kFetchCorr, /*read_committed=*/1);
    ASSERT_TRUE(r.subscribed.count(kFetchCorr));
    r.rep->becomeFollower(2, 2);
    const auto h = r.reg.lookupHandle(0, 0);
    r.reg.setIntendedRole(0, 0, h.incarnation, rune::node::ReplicaRegistry::IntendedRole::Follower);
    for (int i = 0; i < 4; ++i) { r.gw.pushSubscriptionsForTest(); r.pass(); }
    ASSERT_TRUE(r.errors.count(kFetchCorr))
        << "SILENT STALL: a group member's partition subscription on a demoted leader was never answered";
    EXPECT_EQ(r.errors[kFetchCorr], kNotLeader);
}

TEST(TransientGatewayRegression, UnroutedDemotionIsCaughtByTheFallbackSweepRegression) {
    Rig r;
    const int64_t sid = r.gw.injectTestSession(0);
    r.subscribe(sid, -2, 71, /*read_committed=*/1);
    ASSERT_TRUE(r.subscribed.count(71));
    r.rep->becomeFollower(2, 2);   // fenced by a higher epoch: the owner flips, nothing is routed
    for (int64_t i = 0; i < 64 * ClientGateway::kMovedAwayScanEveryCyclesForTest && !r.errors.count(71); ++i)
        r.gw.pushSubscriptionsForTest();
    ASSERT_TRUE(r.errors.count(71))
        << "SILENT STALL: a demotion no route reported left the read_committed subscription silent";
    EXPECT_EQ(r.errors[71], kNotLeader);
}

TEST(TransientGatewayRegression, ReplicaRemovedFromThisNodeRetiresEveryPlainSubscriptionRegression) {
    Rig r;
    const int64_t sid = r.gw.injectTestSession(0);
    r.subscribe(sid, -2, 81, /*read_committed=*/1);
    r.subscribe(sid, -2, 82, /*read_uncommitted=*/0);
    ASSERT_TRUE(r.subscribed.count(81));
    ASSERT_TRUE(r.subscribed.count(82));
    r.reg.remove(0, 0);   // the partition moved off this node (ReplicationMetadataBridge::applyDeleteForKey)
    for (int i = 0; i < 4; ++i) r.gw.pushSubscriptionsForTest();
    ASSERT_TRUE(r.errors.count(81)) << "SILENT STALL: read_committed subscription of a removed replica";
    ASSERT_TRUE(r.errors.count(82)) << "SILENT STALL: read_uncommitted subscription of a removed replica";
    EXPECT_EQ(r.errors[81], kNotLeader);
    EXPECT_EQ(r.errors[82], kNotLeader);
    EXPECT_EQ(r.gw.readCommittedSubscriptionsMovedAway(), 2u);
}

// ---------------------------------------------------------------------------
// Task 13 fix round 2.
// ---------------------------------------------------------------------------
// M1: the control plane records the routed role BEFORE the owning worker flips
// the replica (ReplicationMetadataBridge::routeBecomeFollower_ →
// recordIntendedRole_, then the role op).  The routed sweep must already treat
// it as moved — intended == Follower — not wait for the published role (it
// would otherwise only be caught by the fallback scan).
TEST(TransientGatewayRegression, RoutedDemotionIsCaughtBeforeTheOwnerFlipsRegression) {
    Rig r;
    const int64_t sid = r.gw.injectTestSession(0);
    r.subscribe(sid, -2, 91, /*read_committed=*/1);
    ASSERT_TRUE(r.subscribed.count(91));
    const auto h = r.reg.lookupHandle(0, 0);
    r.reg.setIntendedRole(0, 0, h.incarnation, rune::node::ReplicaRegistry::IntendedRole::Follower);
    ASSERT_EQ(r.rep->role(), Role::Leader) << "precondition: the owner has not flipped yet";
    r.gw.pushSubscriptionsForTest();   // ONE pass: the routed trigger, no fallback slice
    ASSERT_TRUE(r.errors.count(91)) << "a routed demotion waited for the owner's flip (fallback only)";
    EXPECT_EQ(r.errors[91], kNotLeader);
}

// M5: the sweep never retires a subscription it can still serve.  Three
// partitions, all led here; subscriptions of both isolations on each.  An
// UNRELATED routed role change (partition 1 demoted) retires only partition
// 1's read_committed subscription; a long run of fallback slices afterwards
// retires nothing more.
TEST(TransientGatewayRegression, SweepNeverRetiresASubscriptionOfAPartitionStillLedHereRegression) {
    Rig r(RigOpts{.partitions = 3});
    const int64_t sid = r.gw.injectTestSession(0);
    int64_t corr = 100;
    std::map<int64_t, std::pair<int32_t, uint8_t>> subs;   // corr -> (partition, isolation)
    for (int32_t p = 0; p < 3; ++p)
        for (uint8_t iso : {uint8_t{0}, uint8_t{1}})
            for (int k = 0; k < 2; ++k) {
                r.subscribe(sid, -2, ++corr, iso, p);
                ASSERT_TRUE(r.subscribed.count(corr)) << "partition " << p;
                subs[corr] = {p, iso};
            }
    PartitionReplica* p1 = r.extraReps[0];
    const auto h = r.reg.lookupHandle(0, 1);
    r.reg.setIntendedRole(0, 1, h.incarnation, rune::node::ReplicaRegistry::IntendedRole::Follower);
    p1->becomeFollower(2, 2);
    for (int64_t i = 0; i < 64 * ClientGateway::kMovedAwayScanEveryCyclesForTest; ++i) r.gw.pushSubscriptionsForTest();
    for (const auto& [c, pi] : subs) {
        const bool shouldRetire = pi.first == 1 && pi.second == 1;
        EXPECT_EQ(r.errors.count(c), shouldRetire ? 1u : 0u)
            << "corr " << c << " partition " << pi.first << " isolation " << int(pi.second);
    }
    EXPECT_EQ(r.gw.readCommittedSubscriptionsMovedAway(), 2u);
}

// M3: before the NotLeader, a session >= 6 gets the POSITION NOTE — an
// empty-batch Records frame of the subscription carrying the broker's cursor
// (here the bind-time resolved Latest = the HW, 2) — so a subscription that
// delivered nothing resumes there instead of re-resolving Latest.  A session
// below 6 gets no note (it would ignore an empty batch anyway).
TEST(TransientGatewayRegression, MovedSubscriptionGetsItsPositionNoteBeforeTheNotLeaderRegression) {
    Rig r;
    const int64_t v6 = r.gw.injectTestSession(0);
    const int64_t v5 = r.gw.injectTestSession(0, /*clientVersion=*/5);
    ASSERT_TRUE(r.awaitProduced(r.produce(v6, 2)).has_value());
    r.subscribe(v6, /*Latest=*/-1, 95, /*read_committed=*/1);
    r.subscribe(v5, /*Latest=*/-1, 96, /*read_committed=*/1);
    ASSERT_TRUE(r.subscribed.count(95));
    ASSERT_TRUE(r.subscribed.count(96));
    const int64_t sub95 = r.subscribed[95].subscriptionId;
    const int64_t sub96 = r.subscribed[96].subscriptionId;
    const std::size_t before = r.records.size();
    const auto h = r.reg.lookupHandle(0, 0);
    r.reg.setIntendedRole(0, 0, h.incarnation, rune::node::ReplicaRegistry::IntendedRole::Follower);
    r.gw.pushSubscriptionsForTest();
    ASSERT_TRUE(r.errors.count(95));
    ASSERT_TRUE(r.errors.count(96));
    int notes95 = 0, notes96 = 0;
    for (std::size_t i = before; i < r.records.size(); ++i) {
        if (r.records[i].subscriptionId == sub95) {
            ++notes95;
            EXPECT_EQ(r.records[i].baseOffset, 2) << "the note carries the bind-time resolved Latest (the HW)";
            EXPECT_EQ(r.records[i].recordBatch.size(), 0u) << "a position note has an empty batch";
            EXPECT_EQ(r.records[i].correlationId, 95);
        }
        if (r.records[i].subscriptionId == sub96) ++notes96;
    }
    EXPECT_EQ(notes95, 1) << "no position note for the v6 session";
    EXPECT_EQ(notes96, 0) << "a position note to a session below 6";
}

// M2: the fallback scan is spread — one slice every kMovedAwayScanEveryCycles
// passes, at most ~kMovedAwayLookupsPerSlice verdicts (whole buckets) per
// slice — never every subscription in one pass; and it still reaches them all.
TEST(TransientGatewayRegression, FallbackScanIsBoundedPerSliceAndCoversEverySubscriptionRegression) {
    Rig r;
    const int64_t sid = r.gw.injectTestSession(0);
    constexpr int kSubs = 200;
    for (int k = 0; k < kSubs; ++k) r.subscribe(sid, -2, 1000 + k, /*read_committed=*/1);
    ASSERT_EQ(r.subscribed.size(), static_cast<std::size_t>(kSubs));
    r.rep->becomeFollower(2, 2);   // unrouted: only the fallback scan can see it
    for (int64_t i = 0; i < ClientGateway::kMovedAwayScanEveryCyclesForTest; ++i) r.gw.pushSubscriptionsForTest();
    const uint64_t firstSlice = r.gw.readCommittedSubscriptionsMovedAway();
    EXPECT_GE(firstSlice, 1u) << "the slice made no progress";
    EXPECT_LE(firstSlice, 48u) << "one slice retired " << firstSlice << " subscriptions (an O(subs) spike)";
    for (int64_t i = 0; i < 4096 && r.gw.readCommittedSubscriptionsMovedAway() < kSubs; ++i)
        r.gw.pushSubscriptionsForTest();
    EXPECT_EQ(r.gw.readCommittedSubscriptionsMovedAway(), static_cast<uint64_t>(kSubs))
        << "SILENT STALL: the spread scan never reached every subscription";
}

// ---------------------------------------------------------------------------
// E2 Task 14 (found by the 3-node transient failover e2e): a client's plain
// subscriptions ride its SEED session, which follows the subscribe Redirect to
// the partition leader.  A controlled stop of that leader answered the moved
// subscriptions a bare NotLeader: the client re-armed them on the same seed
// session — bound to the node that is about to exit — and waited for fresh
// metadata over it, which never came.  The subscriptions timed out (explicit,
// but lost).  The demoted node KNOWS the new leader (the routing image flipped
// before the routed sweep): a v6 session is now answered Redirect{the new
// leader's client endpoint} (the client follows it: retarget + reconnect +
// re-subscribe from its position — the path the v6 client already takes for a
// Redirect).  Below 6, or with no known other leader: NotLeader as before.
// ---------------------------------------------------------------------------
TEST(TransientGatewayRegression, MovedSubscriptionIsRedirectedToTheKnownNewLeaderRegression) {
    Rig r;
    r.gw.setNodeIdForTest(1);
    const std::string kLeaderEndpoint = "aeron:udp?endpoint=rune-3:40200";
    r.gw.setNodeEndpointMap({{1, "aeron:udp?endpoint=rune-2:40200"}, {2, kLeaderEndpoint}});
    const int64_t v6 = r.gw.injectTestSession(0);
    const int64_t v5 = r.gw.injectTestSession(0, /*clientVersion=*/5);
    r.subscribe(v6, -2, 201, /*read_committed=*/1);
    r.subscribe(v5, -2, 202, /*read_committed=*/1);
    ASSERT_TRUE(r.subscribed.count(201));
    ASSERT_TRUE(r.subscribed.count(202));
    // The controller moved partition 0 to node 2 (epoch 2); this node is demoted.
    r.gw.setRoutingImageSnapshot(rune::metadata::RoutingImage::build({{rune::metadata::gpidOf(0, 0), 2, 2}}));
    const auto h = r.reg.lookupHandle(0, 0);
    r.reg.setIntendedRole(0, 0, h.incarnation, rune::node::ReplicaRegistry::IntendedRole::Follower);
    r.gw.pushSubscriptionsForTest();
    ASSERT_TRUE(r.errors.count(201)) << "the moved v6 subscription was never answered";
    EXPECT_EQ(r.errors[201], static_cast<int16_t>(pc::ErrorCode::Redirect))
        << "a v6 subscription moved to a KNOWN leader must be redirected there (a bare NotLeader strands a client "
           "whose seed session sits on this stopping node)";
    EXPECT_EQ(r.errorMessages[201], kLeaderEndpoint);
    ASSERT_TRUE(r.errors.count(202));
    EXPECT_EQ(r.errors[202], kNotLeader) << "a session below 6 keeps the bare NotLeader";
    EXPECT_EQ(r.gw.readCommittedSubscriptionsMovedAway(), 2u);
}

// ... and with no known other leader (NO_LEADER, or this node itself in a stale
// image, or no endpoint for it) the v6 answer stays the bare NotLeader.
TEST(TransientGatewayRegression, MovedSubscriptionWithNoKnownOtherLeaderStaysNotLeaderRegression) {
    for (const int32_t leader : {rune::metadata::RoutingImage::NO_LEADER_NODE_ID, 1, 3}) {
        Rig r;
        r.gw.setNodeIdForTest(1);
        r.gw.setNodeEndpointMap({{1, "aeron:udp?endpoint=rune-2:40200"}, {2, "aeron:udp?endpoint=rune-3:40200"}});
        const int64_t v6 = r.gw.injectTestSession(0);
        r.subscribe(v6, -2, 211, /*read_committed=*/1);
        ASSERT_TRUE(r.subscribed.count(211));
        r.gw.setRoutingImageSnapshot(rune::metadata::RoutingImage::build({{rune::metadata::gpidOf(0, 0), leader, 2}}));
        const auto h = r.reg.lookupHandle(0, 0);
        r.reg.setIntendedRole(0, 0, h.incarnation, rune::node::ReplicaRegistry::IntendedRole::Follower);
        r.gw.pushSubscriptionsForTest();
        ASSERT_TRUE(r.errors.count(211)) << "leader " << leader;
        EXPECT_EQ(r.errors[211], kNotLeader) << "leader " << leader << " (3 has no endpoint)";
        EXPECT_TRUE(r.errorMessages[211].empty()) << "leader " << leader;
    }
}

// Fix round 1 (review I2): the Redirect carve-outs.  A group member's partition
// subscriptions ride per-leader sessions and are re-routed by the client's group
// code on a NotLeader — they keep the bare NotLeader even for a v6 session with
// a known new leader:
//   (a) a member's partition Subscribe (bit-61 tagged correlation id) is
//       answered NotLeader, while a plain one in the same sweep is redirected;
//   (b) a coordinator-created group subscription (isGroup, addGroupSubscription;
//       on its own session — a plain Subscribe on the same session/partition
//       would be taken as its carrier) is never answered Redirect.
TEST(TransientGatewayRegression, MovedGroupSubscriptionsKeepNotLeaderEvenWithAKnownNewLeaderRegression) {
    Rig r;
    r.gw.setNodeIdForTest(1);
    r.gw.setNodeEndpointMap({{1, "aeron:udp?endpoint=rune-2:40200"}, {2, "aeron:udp?endpoint=rune-3:40200"}});
    const int64_t v6 = r.gw.injectTestSession(0);
    const int64_t v6g = r.gw.injectTestSession(0);
    constexpr int64_t kFetchCorr = (int64_t{1} << 61) | 9;   // rune-client GroupConsumerHub::kFetchCorrTag
    r.subscribe(v6, 0, kFetchCorr, /*read_committed=*/1);
    ASSERT_TRUE(r.subscribed.count(kFetchCorr));
    r.subscribe(v6, -2, 221, /*read_committed=*/1);   // a plain one: the control (Redirect)
    ASSERT_TRUE(r.subscribed.count(221));
    const int64_t groupSub = r.gw.addGroupSubscription(v6g, 0, 0, 0, /*isolation=*/1, /*pendingCarrier=*/false);
    ASSERT_GE(groupSub, 0);

    r.gw.setRoutingImageSnapshot(rune::metadata::RoutingImage::build({{rune::metadata::gpidOf(0, 0), 2, 2}}));
    const auto h = r.reg.lookupHandle(0, 0);
    r.reg.setIntendedRole(0, 0, h.incarnation, rune::node::ReplicaRegistry::IntendedRole::Follower);
    for (int i = 0; i < 4; ++i) { r.gw.pushSubscriptionsForTest(); r.pass(); }

    ASSERT_TRUE(r.errors.count(221));
    EXPECT_EQ(r.errors[221], static_cast<int16_t>(pc::ErrorCode::Redirect)) << "control: the plain one is redirected";
    ASSERT_TRUE(r.errors.count(kFetchCorr)) << "SILENT STALL: the member's partition subscription was never answered";
    EXPECT_EQ(r.errors[kFetchCorr], kNotLeader) << "a group member's partition subscription must keep NotLeader";
    EXPECT_TRUE(r.errorMessages[kFetchCorr].empty());
    for (const auto& [corr, code] : r.errors)
        if (corr != 221) EXPECT_NE(code, static_cast<int16_t>(pc::ErrorCode::Redirect)) << "corr " << corr << " redirected";
}

// ---------------------------------------------------------------------------
// E2 final fix wave (Task 13 re-review N1): the fallback slice is bounded by its
// actual movedAway_ PROBES, even when the subscriptions collide into a few
// buckets — it stops mid-bucket and resumes there.  It still reaches every
// subscription.  RED (whole buckets, verdict budget checked between buckets):
// the first slice probed all 200 subscriptions of the squeezed bucket(s).
// ---------------------------------------------------------------------------
TEST(TransientGatewayRegression, FallbackSliceIsBoundedByProbesInACollidingBucketRegression) {
    Rig r;
    const int64_t sid = r.gw.injectTestSession(0);
    constexpr int kSubs = 200;
    for (int k = 0; k < kSubs; ++k) r.subscribe(sid, -2, 3000 + k, /*read_committed=*/1);
    ASSERT_EQ(r.subscribed.size(), static_cast<std::size_t>(kSubs));
    r.gw.squeezeSubscriptionBucketsForTest();
    r.rep->becomeFollower(2, 2);   // unrouted: only the fallback scan sees it
    for (int64_t i = 0; i < ClientGateway::kMovedAwayScanEveryCyclesForTest; ++i) r.gw.pushSubscriptionsForTest();
    EXPECT_GE(r.gw.readCommittedSubscriptionsMovedAway(), 1u) << "the slice made no progress";
    EXPECT_LE(r.gw.movedAwayMaxProbesPerSliceForTest(), ClientGateway::kMovedAwayLookupsPerSliceForTest)
        << "one slice probed " << r.gw.movedAwayMaxProbesPerSliceForTest() << " subscriptions of a colliding bucket";
    for (int64_t i = 0; i < 8192 && r.gw.readCommittedSubscriptionsMovedAway() < kSubs; ++i)
        r.gw.pushSubscriptionsForTest();
    EXPECT_EQ(r.gw.readCommittedSubscriptionsMovedAway(), static_cast<uint64_t>(kSubs))
        << "SILENT STALL: the mid-bucket resume never reached every subscription";
    EXPECT_LE(r.gw.movedAwayMaxProbesPerSliceForTest(), ClientGateway::kMovedAwayLookupsPerSliceForTest);
}

// ---------------------------------------------------------------------------
// E2 final fix wave (Task 13 re-review N2): the moved-away summary is logged
// PROMPTLY (at the end of the very pass that retired the subscriptions) and
// ACCURATELY (that pass's whole count) — then rate-limited by wall time: the
// retirements inside the window are reported in one summary as soon as it
// elapses.  RED (cycle-paced summary at the top of the pass): nothing logged
// in the retiring pass; the next summary only 65536 passes later.
// ---------------------------------------------------------------------------
TEST(TransientGatewayRegression, MovedAwaySummaryIsPromptAndAccurateRegression) {
    Rig r(RigOpts{.partitions = 2});
    const int64_t sid = r.gw.injectTestSession(0);
    for (int k = 0; k < 3; ++k) r.subscribe(sid, -2, 4000 + k, /*read_committed=*/1, /*partition=*/0);
    for (int k = 0; k < 2; ++k) r.subscribe(sid, -2, 4100 + k, /*read_committed=*/1, /*partition=*/1);
    ASSERT_EQ(r.subscribed.size(), 5u);
    int64_t now = 5'000'000'000;
    r.gw.setNowForTest(now);
    const auto h0 = r.reg.lookupHandle(0, 0);
    r.reg.setIntendedRole(0, 0, h0.incarnation, rune::node::ReplicaRegistry::IntendedRole::Follower);
    r.gw.pushSubscriptionsForTest();   // ONE pass: the routed sweep retires partition 0's three
    ASSERT_EQ(r.gw.readCommittedSubscriptionsMovedAway(), 3u);
    EXPECT_EQ(r.gw.movedAwaySummariesForTest(), 1u) << "the retiring pass logged no summary (it lags)";
    EXPECT_EQ(r.gw.lastMovedAwaySummaryCountForTest(), 3u) << "the summary reported a partial count";

    // Inside the rate window: partition 1's two are retired, the summary waits ...
    now += 100'000'000;
    r.gw.setNowForTest(now);
    const auto h1 = r.reg.lookupHandle(0, 1);
    r.reg.setIntendedRole(0, 1, h1.incarnation, rune::node::ReplicaRegistry::IntendedRole::Follower);
    // (M4: a routed change right after a sweep is coalesced into the next one.)
    for (int64_t i = 0; i <= ClientGateway::kRoutedSweepMinGapCyclesForTest &&
                        r.gw.readCommittedSubscriptionsMovedAway() < 5u; ++i)
        r.gw.pushSubscriptionsForTest();
    ASSERT_EQ(r.gw.readCommittedSubscriptionsMovedAway(), 5u);
    EXPECT_EQ(r.gw.movedAwaySummariesForTest(), 1u) << "not rate-limited";
    // ... and is logged on the first pass after the window, with both.
    now += ClientGateway::kMovedAwaySummaryEveryNsForTest;
    r.gw.setNowForTest(now);
    r.gw.pushSubscriptionsForTest();
    EXPECT_EQ(r.gw.movedAwaySummariesForTest(), 2u) << "the carried retirements were never reported";
    EXPECT_EQ(r.gw.lastMovedAwaySummaryCountForTest(), 2u);
}

// ---------------------------------------------------------------------------
// E2 final fix wave (final review M4): routed role changes are COALESCED.  A
// controlled shutdown moves many partitions, one routed role change per pass;
// the sweep (a movedAway_ probe per eligible subscription) used to run on
// every such pass — O(subscriptions) each.  Now the first change after a quiet
// period sweeps at once and later ones share one sweep per
// kRoutedSweepMinGapCycles passes; every demotion is still caught.  RED (one
// sweep per changed pass): 128 sweeps.
// ---------------------------------------------------------------------------
TEST(TransientGatewayRegression, RoutedRoleChangesAreCoalescedIntoBoundedSweepsRegression) {
    Rig r(RigOpts{.partitions = 3});
    const int64_t sid = r.gw.injectTestSession(0);
    for (int k = 0; k < 20; ++k) r.subscribe(sid, -2, 5000 + k, /*read_committed=*/1, /*partition=*/0);
    for (int k = 0; k < 20; ++k) r.subscribe(sid, -2, 5100 + k, /*read_committed=*/1, /*partition=*/2);
    ASSERT_EQ(r.subscribed.size(), 40u);
    // A burst: partition 1 (no subscriptions) changes routed role on 128 consecutive passes.
    const auto h1 = r.reg.lookupHandle(0, 1);
    for (int i = 0; i < 128; ++i) {
        r.reg.setIntendedRole(0, 1, h1.incarnation,
                              (i & 1) ? rune::node::ReplicaRegistry::IntendedRole::Leader
                                      : rune::node::ReplicaRegistry::IntendedRole::Follower);
        r.gw.pushSubscriptionsForTest();
    }
    EXPECT_LE(r.gw.routedMovedAwaySweepsForTest(), 128u / ClientGateway::kRoutedSweepMinGapCyclesForTest + 1)
        << "a routed sweep per changed pass (O(subscriptions) probes each)";
    EXPECT_GE(r.gw.routedMovedAwaySweepsForTest(), 1u) << "the first change after a quiet period swept nothing";
    EXPECT_EQ(r.gw.readCommittedSubscriptionsMovedAway(), 0u) << "a subscription still led here was retired";
    // Partition 2 is demoted inside the burst's gap: caught by the coalesced sweep.
    const auto h2 = r.reg.lookupHandle(0, 2);
    r.reg.setIntendedRole(0, 2, h2.incarnation, rune::node::ReplicaRegistry::IntendedRole::Follower);
    for (int64_t i = 0; i <= ClientGateway::kRoutedSweepMinGapCyclesForTest &&
                        r.gw.readCommittedSubscriptionsMovedAway() < 20u; ++i)
        r.gw.pushSubscriptionsForTest();
    EXPECT_EQ(r.gw.readCommittedSubscriptionsMovedAway(), 20u)
        << "SILENT STALL: a demotion coalesced into a later sweep was never caught";
    for (int k = 0; k < 20; ++k) EXPECT_FALSE(r.errors.count(5000 + k)) << "partition 0 (still led) retired";
}

// ---------------------------------------------------------------------------
// E2 final fix wave (final review M5): `hadReplica` was captured only at
// subscribe time.  A subscription accepted BEFORE this node got a replica of
// the partition (here: a DISK topic served from its Log with no replica
// registered yet; a reassignment then adds this node) was never retired when
// that replica was later removed — it went silent.  The sweep now records the
// replica the first time it sees it.  RED: the removal retired nothing.
// ---------------------------------------------------------------------------
TEST(TransientGatewayRegression, SubscriptionAcceptedBeforeItsReplicaIsRetiredWhenTheReplicaIsRemovedRegression) {
    TempDir dir;
    rune::node::TopicSpec ts;
    ts.name = "inventory"; ts.partitionCount = 1; ts.topicId = 0;   // a DISK topic
    TopicRegistry registry(std::vector<rune::node::TopicSpec>{ts}, dir.path.string());
    rune::replication::NullPartitionTransport transport;
    rune::node::ReplicaRegistry reg;
    std::map<int64_t, int16_t> errors;
    std::map<int64_t, pc::SubscribeResponse> subscribed;
    auto onFrame = [&](const uint8_t* b, std::size_t n) {
        const auto h = pc::MessageHeader::decode(b, n);
        if (!h) return;
        if (static_cast<pc::TemplateId>(h->templateId) == pc::TemplateId::ErrorResponse) {
            if (auto r = pc::ErrorResponse::decode(b, n)) errors[r->correlationId] = r->code;
        } else if (static_cast<pc::TemplateId>(h->templateId) == pc::TemplateId::SubscribeResponse) {
            if (auto r = pc::SubscribeResponse::decode(b, n)) subscribed[r->correlationId] = *r;
        }
    };
    ClientGateway gw = ClientGateway::makeForTest([&](const uint8_t* b, std::size_t n) -> int64_t { onFrame(b, n); return 1; }, 1);
    gw.setTopicRegistry(&registry);
    gw.setReplicaRegistry(&reg);
    gw.setCommittedMetadataReady(true);
    gw.setOnEgressFrame([&](int64_t, const uint8_t* b, std::size_t n) { onFrame(b, n); });
    const int64_t sid = gw.injectTestSession(0);
    pc::Subscribe s;
    s.sessionId = sid; s.correlationId = 77; s.topicId = 0; s.partition = 0;
    s.partitionKnown = 1; s.fromOffset = 0; s.isolation = 0;
    std::vector<uint8_t> buf(s.encodedSize());
    buf.resize(s.encode(buf.data()));
    gw.applySubscribeForTest(buf.data(), buf.size());
    ASSERT_TRUE(subscribed.count(77)) << "precondition: served from the Log with no replica registered";

    // A reassignment adds this node: its replica of the partition is registered.
    PartitionConfig c;
    c.topicId = 0; c.partitionId = 0; c.selfId = 1; c.minIsr = 1;
    c.replicaLagMaxOffset = 1'000'000'000; c.replicaLagMaxMs = 600'000;
    auto rep = std::make_unique<PartitionReplica>(c, Role::Leader, 1, std::vector<int32_t>{1}, std::vector<int32_t>{1},
                                                  registry.log(0, 0), &transport);
    rep->setExternalFollowerFlush(true);
    reg.add(0, 0, std::move(rep));
    for (int64_t i = 0; i < 4 * ClientGateway::kMovedAwayScanEveryCyclesForTest; ++i) gw.pushSubscriptionsForTest();
    EXPECT_FALSE(errors.count(77)) << "retired while its replica is still here";

    // ... and later moves it away again.
    reg.remove(0, 0);
    for (int64_t i = 0; i < 4 * ClientGateway::kMovedAwayScanEveryCyclesForTest && !errors.count(77); ++i)
        gw.pushSubscriptionsForTest();
    ASSERT_TRUE(errors.count(77))
        << "SILENT STALL: a subscription accepted before its replica existed was never retired on the removal";
    EXPECT_EQ(errors[77], kNotLeader);
    EXPECT_EQ(gw.readCommittedSubscriptionsMovedAway(), 1u);
}

// ---------------------------------------------------------------------------
// E2 final fix wave (final review M6): the legacy-mode push loop classified
// every transient subscription on every cycle (servedPartition_: servableLog,
// awaitingCommit, topic, a replica lookup — then a second and a third lookup).
// The classification and replica are now cached on the subscription and
// refreshed only when the replica registry or the catalog changes; delivery is
// unchanged.  RED (classified every pass): 200 classifications for 200 passes.
// ---------------------------------------------------------------------------
TEST(TransientGatewayRegression, LegacyPushClassifiesATransientSubscriptionOncePerRegistryChangeRegression) {
    Rig r(RigOpts{.partitions = 2});
    const int64_t sid = r.gw.injectTestSession(0);
    r.subscribe(sid, 0, 6001, /*read_uncommitted=*/0);
    ASSERT_TRUE(r.subscribed.count(6001));
    ASSERT_TRUE(r.awaitProduced(r.produce(sid, 3)).has_value());
    ASSERT_TRUE(r.pushUntil(1)) << "nothing delivered";
    const uint64_t afterFirst = r.gw.transientClassificationsForTest();
    EXPECT_GE(afterFirst, 1u);
    for (int i = 0; i < 200; ++i) { r.gw.pushSubscriptionsForTest(); r.pass(); }
    EXPECT_EQ(r.gw.transientClassificationsForTest(), afterFirst)
        << "re-classified on cycles with no registry change (four hash probes per subscription per cycle)";
    // A registry change (partition 1's replica replaced) refreshes it once; delivery goes on.
    r.reg.remove(0, 1);
    for (int i = 0; i < 4; ++i) { r.gw.pushSubscriptionsForTest(); r.pass(); }
    EXPECT_EQ(r.gw.transientClassificationsForTest(), afterFirst + 1) << "not refreshed on a registry change";
    const std::size_t before = r.records.size();
    ASSERT_TRUE(r.awaitProduced(r.produce(sid, 2)).has_value());
    ASSERT_TRUE(r.pushUntil(before + 1)) << "the cached classification stopped delivery";
}

// ---------------------------------------------------------------------------
// E2 final fix wave (final review I1) — the broker half of the cross-layer
// cell "a plain moved subscription x a lagging new-leader HW".  After a
// failover the new leader's HW lags (it starts from its follower HW, one fetch
// round behind).  The client re-arms its moved subscription at its position
// (here 3); the leader clamps the explicit offset DOWN to its HW (0) and, once
// the HW moves, pushes from there — batches wholly below the position.  Those
// are re-deliveries: the client's sticky resume floor drops every one of them
// (rune-client-cpp SubscribeNotLeaderRegression.MovedSubscriptionDropsABatch-
// WhollyBelowItsPosition / ...FloorHoldsAcrossALaggingLeadersBatches — the
// client half, driven by exactly this push pattern).
// ---------------------------------------------------------------------------
TEST(TransientGatewayRegression, ReArmedSubscriptionAboveALaggingHwIsPushedFromTheHwRegression) {
    Rig r(RigOpts{.withFollower = true});   // ISR {1,2}: the HW waits for follower 2
    const int64_t sid = r.gw.injectTestSession(0);
    for (int i = 0; i < 3; ++i) ASSERT_EQ(r.awaitProduced(r.produce(sid, 1, /*acks=*/0)).value().errorCode, kOk);
    for (int i = 0; i < 4; ++i) r.pass();
    ASSERT_EQ(r.rep->logEndOffset(), 3);
    ASSERT_EQ(r.rep->gatewayHighWatermark(), 0) << "precondition: the new leader's HW lags its log";

    r.subscribe(sid, /*fromOffset=*/3, 301);   // the moved subscription's position on the old leader
    ASSERT_EQ(r.subscribed.at(301).errorCode, kOk);
    EXPECT_EQ(r.gw.subscriptionNextOffsetForTest(r.subscribed[301].subscriptionId), 0)
        << "an explicit offset above the HW is clamped down to it";

    // Follower 2 catches up: the HW reaches the log end; the leader pushes from 0.
    rune::node::WorkerOp op;
    op.type = rune::node::WorkerOp::Type::FetchRequest; op.topicId = 0; op.partition = 0; op.replica = r.rep;
    op.fetchRequestPayload.leaderEpoch = 1; op.fetchRequestPayload.partition = 0;
    op.fetchRequestPayload.followerId = 2;  op.fetchRequestPayload.fetchOffset = 3;
    op.fetchRequestPayload.maxBytes = 512 * 1024; op.fetchRequestPayload.topicId = 0;
    op.fetchRequestPayload.nowNs = r.now;
    ASSERT_TRUE(r.pool->tryEnqueueOp(0, 0, std::move(op)));
    for (int i = 0; i < 8 && r.rep->gatewayHighWatermark() < 3; ++i) r.pass();
    ASSERT_EQ(r.rep->gatewayHighWatermark(), 3) << "precondition: the follower fetch moved the HW";
    ASSERT_TRUE(r.pushUntil(1)) << "nothing pushed once the HW moved";
    int64_t first = INT64_MAX;
    for (const auto& rec : r.records)
        if (rec.subscriptionId == r.subscribed[301].subscriptionId && !rec.recordBatch.empty())
            first = std::min(first, rec.baseOffset);
    EXPECT_EQ(first, 0) << "the lagging leader pushes from its HW, below the re-armed position (the client drops these)";
}

// E2 final fix wave — FU1: a v7 PLAIN Subscribe on a transient partition whose
// data lineage is not confirmed yet is answered the retriable NotLeader (as
// OffsetFetch is) and registers nothing — it never binds without a lineage
// note.  Once the lineage is known it binds and the note follows.  A pre-v7
// session (no notes, no claims) keeps the old behaviour.  RED (no gate): it
// bound with no note.
namespace {
uint64_t g_fu1Lineage = UINT64_MAX;   // unknown
uint64_t fu1Lineage(void*, int32_t, int32_t) noexcept { return g_fu1Lineage; }
}  // namespace

TEST(TransientGatewayRegression, PlainSubscribeWaitsForTheLineageBeforeBindingRegression) {
    Rig r;
    r.gw.setTransientLineage(&fu1Lineage, nullptr);
    g_fu1Lineage = UINT64_MAX;
    const int64_t v7 = r.gw.injectTestSession(0, rune::protocol::client::kClientVersionLineageNote);
    r.subscribe(v7, /*Earliest=*/-2, 71);
    EXPECT_FALSE(r.subscribed.count(71)) << "bound while the lineage was unknown (no lineage note possible)";
    ASSERT_TRUE(r.errors.count(71));
    EXPECT_EQ(r.errors[71], kNotLeader) << "not the retriable NotLeader";
    EXPECT_EQ(r.gw.subscribesAwaitingLineageForTest(), 1u);

    const int64_t v6 = r.gw.injectTestSession(0, 6);   // an older client: unchanged
    r.subscribe(v6, -2, 72);
    ASSERT_TRUE(r.subscribed.count(72));
    EXPECT_EQ(r.subscribed[72].errorCode, kOk);

    g_fu1Lineage = 0xABCD;   // confirmed
    r.subscribe(v7, -2, 73);
    ASSERT_TRUE(r.subscribed.count(73));
    EXPECT_EQ(r.subscribed[73].errorCode, kOk);
    EXPECT_EQ(r.gw.lineageNotesSentForTest(), 1u) << "bound without its lineage note";
}
