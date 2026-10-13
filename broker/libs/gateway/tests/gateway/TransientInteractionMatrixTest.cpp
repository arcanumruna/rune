// TransientInteractionMatrixTest.cpp — Phase 2b E2 interaction matrix, unit
// level: EVERY cell of operation (9) × entity (persistence × worker mode × role
// × commit state = 16) × timing (7) = 1008 TEST_P instances on a deterministic
// gateway + 1-worker-pool rig (no Aeron).  Each cell asserts the outcome
// support/TransientMatrixExpectations.h names; NotApplicable cells are skipped
// with the rule that excludes them.  Topic 0 "tt" is TRANSIENT, topic 1 "dt" is
// DISK; a cell drives the one its entity names (ParityWithDisk drives both).
// Shard = produce on the worker's ingress; Legacy = produce / markers through
// the gateway's op ring.  Reads go through the gateway's PushRead path in both
// modes (the worker read core both modes share; the shard owned-subscription
// push loop is Task 13, over Aeron).
#include "support/TransientMatrixExpectations.h"

#include <rune/gateway/ClientGateway.h>
#include <rune/gateway/ITxnRequestHandler.h>
#include <rune/gateway/TopicRegistry.h>
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

#include <gtest/gtest.h>

#include <algorithm>
#include <filesystem>
#include <map>
#include <memory>
#include <optional>
#include <ostream>
#include <random>
#include <span>
#include <string>
#include <utility>
#include <vector>

namespace mx = rune::node::test::tmatrix;
namespace pc = rune::protocol::client;
namespace fs = std::filesystem;
using rune::gateway::ClientGateway;
using rune::gateway::TopicRegistry;
using rune::replication::PartitionConfig;
using rune::replication::PartitionReplica;
using mx::Op;
using mx::Outcome;
using mx::Timing;

namespace {

constexpr int32_t kTT  = 0;    // "tt" — transient
constexpr int32_t kDT  = 1;    // "dt" — disk
constexpr int64_t kPid = 77;
constexpr int16_t kOk               = 0;
constexpr int16_t kNotLeader        = static_cast<int16_t>(pc::ErrorCode::NotLeader);
constexpr int16_t kTransientLogFull = static_cast<int16_t>(pc::ErrorCode::TransientLogFull);

struct Cell {
    Op         op;
    mx::Entity e;
    Timing     t;
};

/// gtest / ctest print a cell as its expected outcome (not its raw bytes).
void PrintTo(const Cell& c, std::ostream* os) { *os << "expect " << mx::name(mx::expect(c.op, c.e, c.t)); }

std::vector<Cell> allCells() {
    std::vector<Cell> out;
    for (Op op : mx::kOps)
        for (mx::Persist p : {mx::Persist::Transient, mx::Persist::Disk})
            for (mx::Mode m : {mx::Mode::Shard, mx::Mode::Legacy})
                for (mx::Role r : {mx::Role::Leader, mx::Role::Follower})
                    for (mx::Commit c : {mx::Commit::Committed, mx::Commit::Uncommitted})
                        for (Timing t : mx::kTimings) out.push_back(Cell{op, mx::Entity{p, m, r, c}, t});
    return out;
}

std::string cellName(const testing::TestParamInfo<Cell>& i) {
    const Cell& c = i.param;
    return std::string(mx::name(c.op)) + "_" + (c.e.persist == mx::Persist::Transient ? "Transient" : "Disk") +
           "_" + (c.e.mode == mx::Mode::Shard ? "Shard" : "Legacy") + "_" +
           (c.e.role == mx::Role::Leader ? "Leader" : "Follower") + "_" +
           (c.e.commit == mx::Commit::Committed ? "Committed" : "Uncommitted") + "_" + std::string(mx::name(c.t));
}

struct TempDir {
    fs::path path;
    TempDir() {
        std::random_device rd;
        path = fs::temp_directory_path() / ("rune-tmatrix-" + std::to_string(rd()) + std::to_string(rd()));
        fs::create_directories(path);
    }
    ~TempDir() { std::error_code ec; fs::remove_all(path, ec); }
};

/// Captures the leader's FetchResponses (FollowerBelowRingStart asserts the answer).
class CaptureTransport final : public rune::replication::PartitionTransport {
public:
    bool sendFetchRequest(const rune::replication::FetchRequestMsg&) override { return true; }
    bool sendFetchResponse(rune::replication::NodeId, const rune::replication::FetchResponseMsg& r) override {
        ++responses;
        lastLogStart     = r.logStartOffset;
        lastRecordsBytes = r.records.size();
        return true;
    }
    int         responses        = 0;
    int64_t     lastLogStart     = -1;
    std::size_t lastRecordsBytes = 0;
};

/// The transaction coordinator's view of markers (refused / landed).
class MarkerSpy final : public rune::gateway::ITxnRequestHandler {
public:
    size_t handleInitProducerId(const uint8_t*, size_t, int64_t, uint8_t*, size_t) override { return 0; }
    size_t handleAddPartitions(const uint8_t*, size_t, int64_t, uint8_t*, size_t) override { return 0; }
    size_t handleAddOffsets(const uint8_t*, size_t, int64_t, uint8_t*, size_t) override { return 0; }
    size_t handleTxnOffsetCommit(const uint8_t*, size_t, int64_t, uint8_t*, size_t) override { return 0; }
    size_t handleEndTxn(const uint8_t*, size_t, int64_t, uint8_t*, size_t) override { return 0; }
    void onControlMarkerRefused(int32_t, int32_t, int64_t, int16_t ec, int32_t) override {
        ++refused;
        lastRefused = ec;
    }
    void onControlMarkerLanded(int32_t, int32_t, int64_t, int32_t) override { ++landed; }
    int     refused     = 0;
    int     landed      = 0;
    int16_t lastRefused = 0;
};

std::vector<uint8_t> batchOf(int recs, std::size_t valueBytes, char fill, int64_t pid = -1, int32_t seq = -1,
                             bool txn = false, int64_t base = 0) {
    rune::protocol::RecordBatchBuilder b(base, 1000);
    if (pid >= 0) { b.setProducerId(pid); b.setProducerEpoch(0); b.setBaseSequence(seq); }
    if (txn) b.setTransactional(true);
    const std::string v(valueBytes, fill);
    for (int i = 0; i < recs; ++i) b.append("k" + std::to_string(i), v, 1000 + i);
    return b.build();
}

/// One acked batch of the cell's topic (the delivery model).
struct Acked {
    int64_t base;
    int32_t count;
};

// ---------------------------------------------------------------------------
// The rig: gateway + topic registry (tt, dt) + replica registry + 1-worker pool.
// Built on `dataDir` so a Restart cell rebuilds it on the same disk data.
// ---------------------------------------------------------------------------
struct Rig {
    Cell                                        c;
    fs::path                                    dataDir;
    std::unique_ptr<rune::storage::InMemoryLog> mem;          // tt's ring — new on every (re)build
    CaptureTransport                            transport;
    rune::node::ReplicaRegistry                 reg;
    std::unique_ptr<TopicRegistry>              topics;
    std::unique_ptr<rune::node::WorkerPool>     pool;
    MarkerSpy                                   spy;
    ClientGateway                               gw;
    PartitionReplica*                           rep[2] = {nullptr, nullptr};
    int64_t                                     sid = 0, sidV5 = 0;
    int64_t                                     now = 1'000'000'000, corr = 0;
    std::map<int64_t, int16_t>                  codes;        // corr -> answer code (Produce / Subscribe / Error)
    std::map<int64_t, int64_t>                  bases;        // corr -> ProduceResponse.baseOffset
    std::map<int64_t, int64_t>                  subIds;       // subscribe corr -> subscriptionId
    std::vector<std::pair<int64_t, int64_t>>    pushed;       // (subscriptionId, baseOffset)

    Rig(const Cell& cell, const fs::path& dir)
        : c(cell), dataDir(dir),
          gw(ClientGateway::makeForTest([this](const uint8_t* b, std::size_t n) -> int64_t {
              onFrame(b, n);
              return 1;
          }, 1)) {
        const bool smallRing = c.t == Timing::RingFullUncommitted || c.t == Timing::EvictedCursor ||
                               c.t == Timing::FollowerBelowRingStart;
        mem = std::make_unique<rune::storage::InMemoryLog>(smallRing ? 16u * 1024 : 1u << 20, 100'000);
        rune::node::TopicSpec tt;
        tt.name = "tt"; tt.partitionCount = 1; tt.topicId = kTT;
        tt.persistence = rune::node::PersistenceMode::Transient;
        rune::node::TopicSpec dt;
        dt.name = "dt"; dt.partitionCount = 1; dt.topicId = kDT;
        const bool runtime = c.e.commit == mx::Commit::Uncommitted;
        if (runtime) {
            topics = std::make_unique<TopicRegistry>(std::vector<rune::node::TopicSpec>{}, dataDir.string());
            EXPECT_EQ(topics->createTopic(tt), kTT);   // C1: awaiting the catalog commit
            EXPECT_EQ(topics->createTopic(dt), kDT);
        } else {
            topics = std::make_unique<TopicRegistry>(std::vector<rune::node::TopicSpec>{tt, dt}, dataDir.string());
        }
        pool = std::make_unique<rune::node::WorkerPool>(1, rune::metadata::ShardRouter(1));
        pool->setRegistry(&reg);
        if (!runtime) { addReplica(kTT); addReplica(kDT); }   // C1: replicas only once committed
        pool->assignFromRegistry(reg);
        if (c.e.mode == mx::Mode::Shard) {
            pool->worker(0).setWorkerIngress(nullptr, /*active=*/true);
            pool->worker(0).setShardMode(true);
        }
        gw.setTopicRegistry(topics.get());
        gw.setReplicaRegistry(&reg);
        gw.setCommittedMetadataReady(true);
        gw.setOnEgressFrame([this](int64_t, const uint8_t* b, std::size_t n) { onFrame(b, n); });
        gw.setWorkerPool(pool.get());
        gw.setTxnService(&spy);
        sid   = gw.injectTestSession(0);                       // the current client (v6)
        sidV5 = gw.injectTestSession(0, /*clientVersion=*/5);
        pass(4);
    }

    [[nodiscard]] int32_t topic() const { return c.e.persist == mx::Persist::Transient ? kTT : kDT; }
    [[nodiscard]] PartitionReplica& replica() const { return *rep[topic()]; }

    void addReplica(int32_t topicId) {
        PartitionConfig pcfg;
        pcfg.topicId = topicId; pcfg.partitionId = 0; pcfg.selfId = 1; pcfg.minIsr = 1;
        pcfg.replicaLagMaxOffset = 1'000'000'000; pcfg.replicaLagMaxMs = 600'000; pcfg.followerFlushIntervalNs = 0;
        const bool follower = c.e.role == mx::Role::Follower;
        std::vector<int32_t> all{1}, isr{1};
        if (follower || c.t == Timing::RingFullUncommitted) { all = {1, 2}; isr = {1, 2}; }
        if (c.t == Timing::FollowerBelowRingStart) { all = {1, 2}; isr = {1}; }
        const auto role = follower ? rune::replication::Role::Follower : rune::replication::Role::Leader;
        std::unique_ptr<PartitionReplica> r =
            topicId == kTT
                ? std::make_unique<PartitionReplica>(pcfg, role, 1, all, isr, mem.get(), &transport)
                : std::make_unique<PartitionReplica>(pcfg, role, 1, all, isr, topics->log(kDT, 0), &transport);
        r->setExternalFollowerFlush(true);
        rep[topicId] = r.get();
        reg.add(topicId, 0, std::move(r));
    }
    /// What BrokerNode's kA2cTopicCommitted handler does (C1): servable + replicas built.
    void commitTopics() {
        ASSERT_TRUE(topics->commitTopic(kTT));
        ASSERT_TRUE(topics->commitTopic(kDT));
        addReplica(kTT);
        addReplica(kDT);
        pool->assignFromRegistry(reg);
        pass(4);
    }

    void onFrame(const uint8_t* b, std::size_t n) {
        const auto h = pc::MessageHeader::decode(b, n);
        if (!h) return;
        switch (static_cast<pc::TemplateId>(h->templateId)) {
        case pc::TemplateId::ProduceResponse:
            if (auto r = pc::ProduceResponse::decode(b, n)) { codes.emplace(r->correlationId, r->errorCode);
                                                              bases.emplace(r->correlationId, r->baseOffset); }
            break;
        case pc::TemplateId::SubscribeResponse:
            if (auto r = pc::SubscribeResponse::decode(b, n)) { codes.emplace(r->correlationId, r->errorCode);
                                                                subIds.emplace(r->correlationId, r->subscriptionId); }
            break;
        case pc::TemplateId::ErrorResponse:
            if (auto r = pc::ErrorResponse::decode(b, n)) codes.emplace(r->correlationId, r->code);
            break;
        case pc::TemplateId::Records:
            if (auto r = pc::Records::decode(b, n)) pushed.emplace_back(r->subscriptionId, r->baseOffset);
            break;
        default: break;
        }
    }
    void pass(int n = 1) {
        for (int i = 0; i < n; ++i) {
            now += 1'000'000;
            (void)pool->worker(0).doWork(now);
            gw.drainWorkerReturnsForTest(now);
        }
    }

    // ---- operations ---------------------------------------------------------
    int64_t produceTo(int32_t topicId, uint8_t acks, int recs, std::size_t valueBytes, char fill, int64_t session,
                      int64_t pid = -1, int32_t seq = -1, bool txn = false) {
        const auto batch = batchOf(recs, valueBytes, fill, pid, seq, txn);
        pc::Produce m;
        m.sessionId = session; m.correlationId = ++corr; m.topicId = topicId; m.partition = 0;
        m.partitionKnown = 1; m.acks = acks; m.producerId = pid; m.producerEpoch = 0; m.baseSequence = seq;
        m.recordBatch = std::span<const uint8_t>(batch);
        std::vector<uint8_t> f(m.encodedSize());
        m.encode(f.data());
        if (c.e.mode == mx::Mode::Shard) (void)pool->worker(0).handleIngressFrameForTest(f.data(), f.size(), now);
        else gw.applyProduceForTest(f.data(), f.size());
        return m.correlationId;
    }
    int64_t produce(uint8_t acks, int recs = 2, std::size_t valueBytes = 8, char fill = 'v', int64_t session = 0,
                    int64_t pid = -1, int32_t seq = -1, bool txn = false) {
        return produceTo(topic(), acks, recs, valueBytes, fill, session != 0 ? session : sid, pid, seq, txn);
    }
    /// The answer to `corr` (bounded passes); -1 = unanswered.
    int16_t answer(int64_t c0, int passes = 64) {
        for (int i = 0; i < passes && !codes.count(c0); ++i) pass();
        const auto it = codes.find(c0);
        return it == codes.end() ? int16_t{-1} : it->second;
    }
    /// Produce until OK (retriable answers retried: the RC state builds for an idempotent batch).
    std::optional<int64_t> produceOk(uint8_t acks, int recs = 2, std::size_t valueBytes = 8, char fill = 'v',
                                     int64_t pid = -1, int32_t seq = -1, bool txn = false) {
        for (int a = 0; a < 64; ++a) {
            const int64_t k = produce(acks, recs, valueBytes, fill, 0, pid, seq, txn);
            if (answer(k) == kOk) return bases.at(k);
        }
        return std::nullopt;
    }
    int64_t subscribeTo(int32_t topicId, int64_t from, uint8_t isolation) {
        pc::Subscribe s;
        s.sessionId = sid; s.correlationId = ++corr; s.topicId = topicId; s.partition = 0;
        s.partitionKnown = 1; s.fromOffset = from; s.isolation = isolation;
        std::vector<uint8_t> buf(s.encodedSize());
        buf.resize(s.encode(buf.data()));
        gw.applySubscribeForTest(buf.data(), buf.size());
        return s.correlationId;
    }
    int64_t subscribe(int64_t from, uint8_t isolation = 0) { return subscribeTo(topic(), from, isolation); }
    /// Push cycles; returns the bases delivered to `subId` so far (in order).
    std::vector<int64_t> push(int64_t subId, int cycles = 32) {
        for (int i = 0; i < cycles; ++i) { gw.pushSubscriptionsForTest(); pass(); }
        std::vector<int64_t> out;
        for (const auto& [s, b] : pushed) if (s == subId) out.push_back(b);
        return out;
    }
    int64_t marker(bool commit = true) {
        const auto m = rune::transaction::ControlMarker::build(
            kPid, 0, commit ? rune::transaction::ControlType::Commit : rune::transaction::ControlType::Abort, 0);
        return gw.appendControlRecord(topic(), 0, std::span<const uint8_t>(m), kPid, commit, /*coordinatorEpoch=*/0);
    }
    /// Follower 2's FetchRequest for the cell's partition, as the transport hands it to the owning worker.
    void fetch(int64_t from) {
        rune::node::WorkerOp op;
        op.type = rune::node::WorkerOp::Type::FetchRequest; op.topicId = topic(); op.partition = 0;
        op.fetchRequestPayload.leaderEpoch = 1;
        op.fetchRequestPayload.partition   = 0;
        op.fetchRequestPayload.followerId  = 2;
        op.fetchRequestPayload.fetchOffset = from;
        op.fetchRequestPayload.maxBytes    = 512 * 1024;
        op.fetchRequestPayload.topicId     = topic();
        op.fetchRequestPayload.nowNs       = now;
        ASSERT_TRUE(pool->tryEnqueueOp(topic(), 0, std::move(op)));
        pass(4);
    }
    void disown() { pool->worker(0).setOwnedDirect({}); }
    void reown() {
        rune::node::Worker::OwnedSet s;
        for (PartitionReplica* r : rep) if (r != nullptr) s.push_back(r);
        pool->worker(0).setOwnedDirect(std::move(s));
    }
    [[nodiscard]] int64_t leo() const { return replica().logEndOffset(); }
    [[nodiscard]] int64_t hw() const { return replica().highWatermark(); }
    [[nodiscard]] int64_t ringStart() const { return mem->logStartOffset(); }
    [[nodiscard]] uint64_t skipped() { return pool->worker(0).transientRecordsSkippedForTest(); }
};

/// One more batch (2 records at `base`) replicated into a FOLLOWER replica; HW = its end.
void replicateBatch(PartitionReplica& r, int32_t topicId, int64_t base) {
    auto b = batchOf(2, 8, 'v', -1, -1, false, base);
    rune::protocol::RecordBatchReader::stampLeaderEpoch(b.data(), 1);
    rune::replication::FetchResponseMsg m{};
    m.leaderEpoch = 1; m.partition = 0; m.highWatermark = base + 2; m.logStartOffset = 0; m.errorCode = 0;
    m.topicId = topicId; m.records = std::span<const uint8_t>(b);
    r.applyReplicated(m);
}

/// Two identical batches (offsets 0..3) replicated into a FOLLOWER replica.
void seedFollower(PartitionReplica& r, int32_t topicId) {
    auto b0 = batchOf(2, 8, 'v', -1, -1, false, 0);
    auto b1 = batchOf(2, 8, 'v', -1, -1, false, 2);
    rune::protocol::RecordBatchReader::stampLeaderEpoch(b0.data(), 1);
    rune::protocol::RecordBatchReader::stampLeaderEpoch(b1.data(), 1);
    std::vector<uint8_t> both = b0;
    both.insert(both.end(), b1.begin(), b1.end());
    rune::replication::FetchResponseMsg m{};
    m.leaderEpoch = 1; m.partition = 0; m.highWatermark = 4; m.logStartOffset = 0; m.errorCode = 0;
    m.topicId = topicId; m.records = std::span<const uint8_t>(both);
    r.applyReplicated(m);
}

/// Every batch of `hist` a read from `start` (whole-batch) delivers below `bound`, in order.
std::vector<int64_t> expectedBases(const std::vector<Acked>& hist, int64_t start, int64_t bound) {
    std::vector<int64_t> out;
    for (const Acked& a : hist)
        if (a.base + a.count > start && a.base + a.count <= bound) out.push_back(a.base);
    return out;
}

// ---------------------------------------------------------------------------
// The driver: owns the data dir (survives a Restart), seeds per timing, runs
// the op, asserts the expected outcome.
// ---------------------------------------------------------------------------
struct Driver {
    Cell                 c;
    TempDir              dir;
    std::unique_ptr<Rig> rig;
    std::vector<Acked>   hist;   // acked plain batches of the cell's topic (current ring / log)

    explicit Driver(const Cell& cell) : c(cell) { rig = std::make_unique<Rig>(c, dir.path); }

    void restart() {
        rig.reset();
        rig = std::make_unique<Rig>(c, dir.path);
        if (c.e.persist == mx::Persist::Transient) hist.clear();   // a new, empty ring
    }
    void ackPlain(uint8_t acks, int recs, std::size_t valueBytes = 8) {
        const auto b = rig->produceOk(acks, recs, valueBytes);
        ASSERT_TRUE(b.has_value()) << "seeding produce refused";
        hist.push_back(Acked{*b, recs});
    }
    /// Both follower replicas hold batches 0..1 and 2..3 (HW 4): the cell's history.
    void seedFollowers() {
        seedFollower(*rig->rep[kTT], kTT);
        seedFollower(*rig->rep[kDT], kDT);
        rig->pass(4);
        hist = {Acked{0, 2}, Acked{2, 2}};
    }
    /// The timing's preconditions (data + ring state).
    void seed() {
        Rig& r = *rig;
        if (c.e.commit == mx::Commit::Uncommitted) return;
        if (c.e.role == mx::Role::Follower) {
            seedFollowers();
            return;
        }
        switch (c.t) {
        case Timing::RingFullUncommitted: {
            if (c.op == Op::TxnMarker) ASSERT_TRUE(r.produceOk(0, 1, 8, 'v', kPid, 0, /*txn=*/true).has_value());
            int16_t code = kOk;
            for (int i = 0; i < 64 && code == kOk; ++i) code = r.answer(r.produce(0, 1, 1000));
            ASSERT_EQ(code, kTransientLogFull) << "precondition: the ring is full of uncommitted data";
            // Top it up with the SMALLEST plain batch (1 record "k0", empty value) until even
            // that is refused: no batch — a ~78-byte commit marker included — fits any more.
            code = kOk;
            for (int i = 0; i < 64 && code == kOk; ++i) code = r.answer(r.produce(0, 1, 0));
            ASSERT_EQ(code, kTransientLogFull) << "precondition: not even the smallest batch fits";
            ASSERT_EQ(r.hw(), 0);
            break;
        }
        case Timing::EvictedCursor:
        case Timing::FollowerBelowRingStart:
            for (int i = 0; i < 40; ++i) ackPlain(0, 1, 1000);
            ASSERT_GT(r.ringStart(), 0) << "precondition: the ring evicted committed records";
            if (c.op == Op::TxnMarker) ASSERT_TRUE(r.produceOk(0, 1, 8, 'v', kPid, 0, true).has_value());
            break;
        default:   // Steady, LeaderMovesMidOp, Restart
            // An OPEN transaction only where the cell commits it; a Restart cell seeds plain
            // batches (a disk log would keep an open txn pinning the LSO across the restart).
            if ((c.op == Op::ReadCommitted || c.op == Op::TxnMarker) && c.t != Timing::Restart) {
                ASSERT_TRUE(r.produceOk(0, 2, 8, 'v', kPid, 0, /*txn=*/true).has_value());   // open txn at 0..1
                if (c.op == Op::ReadCommitted) ackPlain(0, 2);                               // plain at 2..3
            } else {
                ackPlain(0, 2);
                ackPlain(0, 2);
            }
            break;
        }
        r.pass(4);
    }

    /// The read op's subscribe (or group) → {answer code, subscriptionId}.
    std::pair<int16_t, int64_t> openRead(int32_t topicId) {
        Rig& r = *rig;
        if (c.op == Op::GroupConsume) {
            const int64_t s = r.gw.addGroupSubscription(r.sid, topicId, 0, /*fromOffset=*/2, 0, false);
            return {s >= 0 ? kOk : kNotLeader, s};
        }
        const int64_t from = c.op == Op::SubEarliest || c.op == Op::ReadCommitted ? -2
                           : c.op == Op::SubLatest ? -1 : 3;
        const int64_t k = r.subscribeTo(topicId, from, c.op == Op::ReadCommitted ? 1 : 0);
        const int16_t code = r.answer(k, 4);
        return {code, r.subIds.count(k) ? r.subIds.at(k) : -1};
    }
    /// The start offset the gateway resolves for the read op (its clamp: [0, HW]).
    [[nodiscard]] int64_t resolvedStart(int64_t hwAtOpen) const {
        switch (c.op) {
        case Op::SubLatest:    return hwAtOpen;
        case Op::SubOffset:    return std::min<int64_t>(3, hwAtOpen);
        case Op::GroupConsume: return std::min<int64_t>(2, hwAtOpen);
        default:               return 0;
        }
    }

    void run(Outcome want) {
        if (c.t == Timing::Restart && c.e.role == mx::Role::Follower) {   // R7 after a restart
            restart();
            seedFollowers();
        }
        Rig* r = rig.get();
        switch (want) {
        case Outcome::NotApplicable: return;

        case Outcome::RetriableNotLeader: {
            if (c.t == Timing::LeaderMovesMidOp) r->disown();
            if (mx::isProduce(c.op)) {
                const int64_t before = c.e.commit == mx::Commit::Committed ? r->leo() : 0;
                EXPECT_EQ(r->answer(r->produce(mx::wireAcks(c.op))), kNotLeader);
                if (c.e.commit == mx::Commit::Committed) EXPECT_EQ(r->leo(), before) << "a refused produce appended";
                else EXPECT_EQ(r->mem->logEndOffset(), 0) << "an uncommitted topic was appended to";
                if (c.t == Timing::LeaderMovesMidOp) {
                    r->reown();
                    const int16_t retry = r->answer(r->produce(mx::wireAcks(c.op)));
                    EXPECT_EQ(retry, c.e.role == mx::Role::Leader ? kOk : kNotLeader) << "the retry on the owner";
                }
            } else {   // Uncommitted × Steady reads (R1); follower read_committed (R7)
                const auto [code, subId] = openRead(r->topic());
                EXPECT_EQ(code, kNotLeader) << "a subscription was registered where it cannot be served";
                EXPECT_LT(subId, 0);
                EXPECT_TRUE(r->subIds.empty()) << "a SubscribeResponse was sent";
            }
            return;
        }

        case Outcome::OkAfterCommit: {   // R2 (C1): refused, commit lands, the retry serves
            if (mx::isProduce(c.op)) {
                EXPECT_EQ(r->answer(r->produce(mx::wireAcks(c.op))), kNotLeader);
                r->commitTopics();
                const int64_t k = r->produce(mx::wireAcks(c.op));
                EXPECT_EQ(r->answer(k), kOk);
                EXPECT_EQ(r->bases.at(k), 0) << "nothing was appended while uncommitted";
            } else {
                EXPECT_EQ(openRead(r->topic()).first, kNotLeader);
                r->commitTopics();
                hist.clear();
                ackPlain(0, 2);
                ackPlain(0, 2);
                const auto [code, subId] = openRead(r->topic());
                ASSERT_EQ(code, kOk);
                const int64_t start = resolvedStart(r->hw());
                EXPECT_EQ(r->push(subId), expectedBases(hist, start, r->hw()));
            }
            return;
        }

        case Outcome::RetriableLogFull: {   // R10 produce: 42 (v6) / NotLeader (v5), nothing appended
            const int64_t leo = r->leo();
            EXPECT_EQ(r->answer(r->produce(mx::wireAcks(c.op), 1, 1000)), kTransientLogFull);
            EXPECT_EQ(r->answer(r->produce(mx::wireAcks(c.op), 1, 1000, 'v', r->sidV5)), kNotLeader)
                << "a v5 session must never see 42";
            EXPECT_EQ(r->leo(), leo) << "a refused produce appended";
            EXPECT_EQ(r->ringStart(), 0) << "an unreplicated record was evicted";
            r->fetch(r->leo());   // follower 2 catches up: HW = LEO
            ASSERT_EQ(r->hw(), r->leo());
            const int64_t k = r->produce(mx::wireAcks(c.op), 1, 1000);
            r->pass(2);
            r->fetch(r->leo());   // acks=all: follower 2 acknowledges the retry too
            EXPECT_EQ(r->answer(k, 256), kOk) << "the retry after catch-up";
            return;
        }

        case Outcome::MarkerRetried: {
            const int64_t leo = r->leo();
            if (c.t == Timing::LeaderMovesMidOp) {   // R9: refused stale, re-routed by the gateway, lands once
                const uint64_t refusedBefore = r->gw.controlRecordsRefusedForTest();
                r->disown();
                EXPECT_EQ(r->marker(), ClientGateway::kControlRecordAsync);
                r->pass(2);
                EXPECT_GT(r->gw.controlRecordsRefusedForTest(), refusedBefore) << "the stale marker was not refused";
                r->reown();
                r->pass(16);
                if (c.e.role == mx::Role::Leader) {
                    EXPECT_EQ(r->spy.landed, 1) << "the re-routed marker did not land exactly once";
                    EXPECT_EQ(r->leo(), leo + 1);
                } else {
                    EXPECT_EQ(r->spy.landed, 0);
                    EXPECT_GE(r->spy.refused, 1) << "a follower must refuse the marker retriably";
                    EXPECT_EQ(r->spy.lastRefused, kNotLeader);
                }
                return;
            }
            EXPECT_EQ(r->marker(), ClientGateway::kControlRecordAsync);
            r->pass(8);
            EXPECT_EQ(r->spy.landed, 0);
            ASSERT_GE(r->spy.refused, 1) << "the refused marker was not surfaced to the coordinator";
            EXPECT_EQ(r->leo(), leo) << "a refused marker appended";
            if (c.e.role == mx::Role::Follower) {     // R7: refused while a follower
                EXPECT_EQ(r->spy.lastRefused, kNotLeader);
                return;
            }
            EXPECT_EQ(r->spy.lastRefused, kTransientLogFull);   // R10: a full ring
            r->fetch(r->leo());                                 // the follower catches up
            EXPECT_EQ(r->marker(), ClientGateway::kControlRecordAsync);   // the coordinator's retry
            r->pass(16);
            r->fetch(r->leo());                                 // the marker itself commits (D9 MAJOR-3)
            r->pass(8);
            EXPECT_EQ(r->spy.landed, 1);
            EXPECT_EQ(r->leo(), leo + 1);
            return;
        }

        case Outcome::UnansweredThenReissued: {   // R9 reads
            const auto [code, subId] = openRead(r->topic());
            ASSERT_EQ(code, kOk) << "the subscription was refused";
            ASSERT_GE(subId, 0) << "no subscription to re-issue";
            const int64_t hwAtOpen = r->hw();
            if (c.op == Op::SubLatest) {   // Latest sits AT the HW: no read is due until new data arrives
                if (c.e.role == mx::Role::Leader) {
                    ackPlain(0, 2);
                } else {
                    const int64_t at = r->leo();
                    replicateBatch(r->replica(), r->topic(), at);
                    r->pass(4);
                    hist.push_back(Acked{at, 2});
                }
                ASSERT_GT(r->hw(), hwAtOpen) << "precondition: data above the Latest cursor";
            }
            const uint64_t chunks = r->pool->worker(0).pushReadChunksForTest();
            r->disown();
            EXPECT_TRUE(r->push(subId, 8).empty()) << "Records pushed for a partition this worker does not own";
            EXPECT_EQ(r->pool->worker(0).pushReadChunksForTest(), chunks) << "read a ring it does not own";
            const uint64_t readsWhileDisowned = r->pool->worker(0).pushReadsForTest();   // stale reads never execute
            r->reown();
            const auto got = r->push(subId);
            if (c.op != Op::ReadCommitted) {
                EXPECT_EQ(got, expectedBases(hist, resolvedStart(hwAtOpen), r->hw()))
                    << "the re-issued read delivered the wrong batches";
                EXPECT_GT(r->pool->worker(0).pushReadsForTest(), readsWhileDisowned)
                    << "the read was never re-issued after ownership returned";
                return;
            }
            // read_committed: the ownership change rebuilt the RC state (a fresh
            // RcState) — the OPEN transaction at 0..1 must still pin the LSO.
            EXPECT_TRUE(got.empty()) << "an OPEN transaction leaked to read_committed after an ownership change";
            ASSERT_EQ(c.e.role, mx::Role::Leader) << "a follower refuses read_committed (R7: RetriableNotLeader)";
            EXPECT_GT(r->pool->worker(0).pushReadsForTest(), readsWhileDisowned)
                << "the read was never re-issued after ownership returned";
            EXPECT_EQ(r->marker(), ClientGateway::kControlRecordAsync);
            r->pass(16);
            EXPECT_EQ(r->push(subId), (std::vector<int64_t>{0, 2})) << "committed txn + plain batch, never the marker";
            return;
        }

        case Outcome::ParityWithDisk: {   // R7 follower reads: transient == disk
            std::vector<int64_t> got[2];
            int16_t code[2] = {-1, -1};
            for (int32_t t : {kTT, kDT}) {
                const auto [cd, subId] = openRead(t);
                code[t] = cd;
                if (c.op == Op::SubLatest && subId >= 0) {   // parity over a real delivery, not empty == empty
                    replicateBatch(*r->rep[t], t, r->rep[t]->logEndOffset());
                    r->pass(4);
                }
                if (subId >= 0) got[t] = r->push(subId);
            }
            if (c.op == Op::SubLatest) EXPECT_FALSE(got[kTT].empty()) << "Latest delivered nothing";
            EXPECT_EQ(code[kTT], code[kDT]) << "a follower's transient answer differs from its disk twin";
            EXPECT_EQ(got[kTT], got[kDT]) << "a follower's transient delivery differs from its disk twin";
            return;
        }

        case Outcome::OkFromRingStart: {   // R11 reads below the ring start
            if (c.t == Timing::FollowerBelowRingStart) {
                const int before = r->transport.responses;
                r->fetch(0);
                ASSERT_GT(r->transport.responses, before) << "a fetch below the ring start was never answered";
                EXPECT_EQ(r->transport.lastLogStart, r->ringStart());
                EXPECT_EQ(r->transport.lastRecordsBytes, 0u);
            }
            const uint64_t skippedBefore = r->skipped();
            const auto [code, subId] = openRead(r->topic());
            ASSERT_EQ(code, kOk);
            const int64_t start = r->ringStart();
            const auto got = r->push(subId, 64);
            ASSERT_FALSE(got.empty()) << "a cursor below the ring start stalled";
            EXPECT_EQ(got.front(), start) << "did not resume at the ring start";
            EXPECT_EQ(got, expectedBases(hist, start, r->hw()));
            EXPECT_EQ(r->skipped() - skippedBefore, static_cast<uint64_t>(start - resolvedStart(r->hw())))
                << "skipped records were not counted exactly";
            return;
        }

        case Outcome::OkEmptyAfterRestart:
        case Outcome::Ok: {
            if (c.t == Timing::FollowerBelowRingStart) {
                r->fetch(0);
                EXPECT_EQ(r->transport.lastLogStart, r->ringStart());
            }
            if (c.t == Timing::Restart) {
                restart();
                r = rig.get();
                if (want == Outcome::OkEmptyAfterRestart) EXPECT_EQ(r->leo(), 0) << "a transient topic kept data";
                if (!mx::isProduce(c.op) && c.op != Op::TxnMarker) ackPlain(0, 2);   // post-restart data
            }
            if (mx::isProduce(c.op)) {
                const int64_t expectBase = r->leo();
                const int64_t k = r->produce(mx::wireAcks(c.op));
                ASSERT_EQ(r->answer(k), kOk);
                EXPECT_EQ(r->bases.at(k), expectBase);
                if (c.op == Op::ProduceAcksAll) EXPECT_GE(r->hw(), expectBase + 2) << "acks=all acked above the HW";
                return;
            }
            if (c.op == Op::TxnMarker) {
                if (c.t == Timing::Restart)
                    ASSERT_TRUE(r->produceOk(0, 1, 8, 'v', kPid, 0, true).has_value());   // a post-restart txn
                const int64_t leo = r->leo();
                EXPECT_EQ(r->marker(), ClientGateway::kControlRecordAsync);
                r->pass(16);
                EXPECT_EQ(r->spy.landed, 1);
                EXPECT_EQ(r->spy.refused, 0);
                EXPECT_EQ(r->leo(), leo + 1);
                return;
            }
            if (c.op == Op::ReadCommitted && (c.t == Timing::Steady || c.t == Timing::LeaderMovesMidOp)) {
                const auto [code, subId] = openRead(r->topic());
                ASSERT_EQ(code, kOk);
                EXPECT_TRUE(r->push(subId).empty()) << "an OPEN transaction leaked to read_committed";
                EXPECT_EQ(r->marker(), ClientGateway::kControlRecordAsync);
                r->pass(16);
                const auto got = r->push(subId);
                EXPECT_EQ(got, (std::vector<int64_t>{0, 2})) << "committed txn + plain batch, never the marker";
                return;
            }
            const auto [code, subId] = openRead(r->topic());
            ASSERT_EQ(code, kOk);
            const int64_t hwAtOpen = r->hw();
            if (c.t == Timing::RingFullUncommitted) {
                EXPECT_TRUE(r->push(subId).empty()) << "delivered past the HW (0)";
                r->fetch(r->leo());   // the follower catches up
                const auto got = r->push(subId, 64);
                ASSERT_FALSE(got.empty());
                EXPECT_EQ(got.front(), resolvedStart(hwAtOpen)) << "a record was evicted before it committed";
                return;
            }
            if (c.op == Op::SubLatest) {
                EXPECT_TRUE(r->push(subId).empty()) << "Latest replayed old records";
                ackPlain(0, 2);
                EXPECT_EQ(r->push(subId), (std::vector<int64_t>{hist.back().base}));
                return;
            }
            EXPECT_EQ(r->push(subId), expectedBases(hist, resolvedStart(hwAtOpen), r->hw()));
            return;
        }
        }
    }
};

class TransientMatrixRegression : public testing::TestWithParam<Cell> {};

TEST_P(TransientMatrixRegression, Cell) {
    const Cell c = GetParam();
    const Outcome want = mx::expect(c.op, c.e, c.t);
    if (want == Outcome::NotApplicable) GTEST_SKIP() << mx::naRule(c.op, c.e, c.t);
    SCOPED_TRACE(std::string("expect ") + std::string(mx::name(want)));
    Driver d(c);
    d.seed();
    if (HasFatalFailure()) return;
    d.run(want);
    // Ring invariant (every transient cell): nothing at/above the HW was evicted.
    if (c.e.persist == mx::Persist::Transient && c.e.commit == mx::Commit::Committed &&
        c.e.role == mx::Role::Leader)
        EXPECT_LE(d.rig->ringStart(), d.rig->hw()) << "an uncommitted record was evicted";
}

INSTANTIATE_TEST_SUITE_P(AllCells, TransientMatrixRegression, testing::ValuesIn(allCells()), cellName);

// The table's shape is pinned: 1008 cells, 366 applicable, by outcome.
TEST(TransientMatrixCountRegression, CellCountsArePinned) {
    std::map<Outcome, int> by;
    int total = 0;
    for (const Cell& c : allCells()) { ++by[mx::expect(c.op, c.e, c.t)]; ++total; }
    EXPECT_EQ(total, 1008);
    EXPECT_EQ(total - by[Outcome::NotApplicable], 366);
    EXPECT_EQ(by[Outcome::RetriableNotLeader], 124);   // + 12 follower read_committed (ruling 2026-10-13)
    EXPECT_EQ(by[Outcome::Ok], 84);
    EXPECT_EQ(by[Outcome::OkAfterCommit], 32);
    EXPECT_EQ(by[Outcome::UnansweredThenReissued], 36);
    EXPECT_EQ(by[Outcome::ParityWithDisk], 32);
    EXPECT_EQ(by[Outcome::OkEmptyAfterRestart], 18);
    EXPECT_EQ(by[Outcome::MarkerRetried], 18);
    EXPECT_EQ(by[Outcome::OkFromRingStart], 16);
    EXPECT_EQ(by[Outcome::RetriableLogFull], 6);
}

} // namespace
