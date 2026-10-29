// ShardWorkerAckTest.cpp — the worker acks the client directly on its own session
// publication (REAL invoker driver, aeron:ipc), notifies the runtime after the ack,
// and fails parked acks=all promptly on demotion.
//
// F15 reconciliation: the post-ack notification is a ProduceResult with
// deferAck == true (session liveness only — F15 S5 removed its PSM facts; never
// an answer) — WorkerReturn PRODUCE_NOTIFY(2) was retired by F15 §7 (RESERVED_2).
#include <rune/codec/Descriptor.h>
#include <rune/codec/Serialize.h>
#include <rune/aeronutil/testing/FreePort.h>
#include <rune/node/EmbeddedDriver.h>
#include <rune/node/OwnerThreadTestTask.h>
#include <rune/node/Worker.h>
#include <rune/node/WorkerIngress.h>
#include <rune/node/WorkerOp.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/protocol/client/DataMessages.h>
#include <rune/protocol/client/Protocol.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/storage/Log.h>

#include <Aeron.h>
#include <FragmentAssembler.h>
#include <gtest/gtest.h>

#include <atomic>
#include <filesystem>
#include <functional>
#include <map>
#include <span>
#include <unistd.h>

using namespace rune::node;
using rune::replication::PartitionConfig;
using rune::replication::PartitionReplica;
using rune::replication::Role;
namespace fs = std::filesystem;
namespace pc = rune::protocol::client;

namespace {
constexpr int kCap = 200'000;
constexpr int32_t kIngress = 7301, kEgress = 7302, kEgress2 = 7303;
// The ack publications a worker opens to its sessions: a small IPC term (the
// driver default is 64 MiB) so a client that stops reading back-pressures them.
constexpr const char* kEgressChannel = "aeron:ipc?term-length=65536";
bool isFailedSignal(const WorkerReturn& r) {
    return r.type == WorkerReturn::Type::SessionSignal &&
           r.lastOffset == static_cast<int64_t>(SessionSignalKind::Failed);
}
class NullTransport final : public rune::replication::PartitionTransport {
public:
    bool sendFetchRequest(const rune::replication::FetchRequestMsg&) override { return true; }
    bool sendFetchResponse(rune::replication::NodeId, const rune::replication::FetchResponseMsg&) override { return true; }
};

struct Fixture {
    fs::path dir, adir;
    std::unique_ptr<rune::storage::Log> log;
    NullTransport t;
    std::unique_ptr<PartitionReplica> replica;
    std::unique_ptr<EmbeddedDriver> drv;
    std::shared_ptr<aeron::Aeron> client;
    std::unique_ptr<WorkerIngress> ing;
    // unique_ptr: the worker (its session publications) must die BEFORE the Aeron client.
    std::unique_ptr<Worker> worker;
    std::shared_ptr<aeron::ExclusivePublication> producePub;
    std::shared_ptr<aeron::Subscription> egress;
    std::map<int64_t, pc::ProduceResponse> acks;
    std::map<int64_t, std::size_t> ackBytes;                   // D6 review M4: the raw frame length per ack
    aeron::FragmentAssembler asmb{[this](aeron::concurrent::AtomicBuffer& b, aeron::util::index_t o,
                                         aeron::util::index_t l, aeron::Header&) {
        if (auto r = pc::ProduceResponse::decode(reinterpret_cast<const uint8_t*>(b.buffer() + o),
                                                 static_cast<std::size_t>(l))) {
            acks[r->correlationId] = *r;
            ackBytes[r->correlationId] = static_cast<std::size_t>(l);
        }
    }};
    int64_t now = 1'000;
    int egressLimit = 64;                                      // session 5's client reads this many acks / step
    bool runtimeDrainsReturns = false;                         // the runtime's ret_ drain (notifications)
    std::vector<WorkerReturn> sessionFailures;
    std::function<void(const WorkerReturn&)> onReturn;         // the emulated runtime's return handler
    // A second client: its own ingress publication (own image) + its own egress.
    std::shared_ptr<aeron::ExclusivePublication> pub2;
    std::shared_ptr<aeron::Subscription> egress2;
    aeron::FragmentAssembler asmb2{[this](aeron::concurrent::AtomicBuffer& b, aeron::util::index_t o,
                                          aeron::util::index_t l, aeron::Header&) {
        if (auto r = pc::ProduceResponse::decode(reinterpret_cast<const uint8_t*>(b.buffer() + o),
                                                 static_cast<std::size_t>(l))) {
            acks[r->correlationId] = *r;
            ackBytes[r->correlationId] = static_cast<std::size_t>(l);
        }
    }};

    /// `preload` (D6 review M3): writes the log before the replica / worker see
    /// it (data replicated or recovered before this leader's state was built);
    /// `rollBytes`: segment roll (small = retention can delete segments);
    /// `session5Version`: session 5's ConnectRequest.clientVersion, carried by
    /// its WorkerOp::SessionAnnounce.
    explicit Fixture(std::vector<rune::replication::NodeId> replicas, int minIsr,
                     const std::function<void(rune::storage::Log&)>& preload = {},
                     int64_t rollBytes = 64LL * 1024 * 1024, int32_t session5Version = 0,
                     std::size_t ringBytes = 1 << 16) {
        // `ringBytes` (D8d): the op / return ring sizes (production: 32 MiB each).
        worker = std::make_unique<Worker>(0, ringBytes, ringBytes, 64, 64);
        static std::atomic<int> seq{0};
        const std::string tag = std::to_string(::getpid()) + "_" + std::to_string(seq++);
        dir  = fs::temp_directory_path() / ("rune_swa_log_" + tag);
        adir = fs::temp_directory_path() / ("rune_swa_aeron_" + tag);
        fs::create_directories(dir);
        log = std::make_unique<rune::storage::Log>(dir.string(), rollBytes);
        log->recover();
        if (preload) preload(*log);
        PartitionConfig c; c.topicId = 1; c.selfId = 1; c.minIsr = minIsr;
        c.fetchIntervalNs = 1'000'000'000; c.isrCheckIntervalNs = 1'000'000'000'000LL;
        c.replicaLagMaxMs = 600'000; c.pushOnAppend = false;
        replica = std::make_unique<PartitionReplica>(c, Role::Leader, 1, replicas, replicas, log.get(), &t);
        EmbeddedDriverConfig dc; dc.dir = adir.string(); dc.termBufferBytes = 64 * 1024;
        drv = std::make_unique<EmbeddedDriver>(dc, DriverMode::Invoker);
        aeron::Context ctx; ctx.aeronDir(adir.string()); ctx.useConductorAgentInvoker(true);
        client = aeron::Aeron::connect(ctx);
        ing = std::make_unique<WorkerIngress>(client, "aeron:ipc", kIngress, drv->pumpWith(client.get()));
        worker->setOwnedDirect(Worker::OwnedSet{replica.get()});
        worker->setWorkerIngress(ing.get(), true);
        worker->setShardMode(true);
        worker->setShardClient(client.get());
        const int64_t sreg = client->addSubscription("aeron:ipc", kEgress);
        const int64_t preg = client->addExclusivePublication("aeron:ipc", kIngress);
        for (int i = 0; i < kCap && !(egress && producePub && producePub->isConnected()); ++i) {
            pump();
            if (!egress) egress = client->findSubscription(sreg);
            if (!producePub) producePub = client->findExclusivePublication(preg);
        }
        WorkerOp a; a.type = WorkerOp::Type::SessionAnnounce; a.grantAll = true; a.sessionId = 5;
        a.egressStreamId = kEgress; a.egressChannel = kEgressChannel; a.clientVersion = session5Version;
        EXPECT_TRUE(worker->tryEnqueueOp(std::move(a)));
    }
    ~Fixture() {
        worker.reset();
        pub2.reset(); egress2.reset();
        producePub.reset(); egress.reset(); ing.reset(); client.reset(); drv.reset();
        std::error_code ec; fs::remove_all(dir, ec); fs::remove_all(adir, ec);
    }
    void pump() { drv->doWork(); client->conductorAgentInvoker().invoke(); }
    void step() {
        pump();
        worker->doWork(now += 10'000);
        if (egress && egressLimit > 0) egress->poll(asmb.handler(), egressLimit);
        if (egress2) egress2->poll(asmb2.handler(), 64);
        if (runtimeDrainsReturns)
            worker->drainReturn([this](WorkerReturn r) {
                if (isFailedSignal(r)) sessionFailures.push_back(r);
                if (onReturn) onReturn(r);
            });
    }
    // The second client: a separate ingress publication (a separate image) and
    // egress stream kEgress2; announced as session `sid` on the lifecycle ring.
    void addSecondClient(int64_t sid, int32_t clientVersion = 0) {
        connectSecondClient();
        announce(sid, kEgress2, clientVersion);
    }
    void connectSecondClient() {
        const int64_t sreg = client->addSubscription("aeron:ipc", kEgress2);
        const int64_t preg = client->addExclusivePublication("aeron:ipc", kIngress);
        for (int i = 0; i < kCap && !(egress2 && pub2 && pub2->isConnected()); ++i) {
            pump();
            if (!egress2) egress2 = client->findSubscription(sreg);
            if (!pub2) pub2 = client->findExclusivePublication(preg);
        }
        ASSERT_TRUE(egress2 && pub2 && pub2->isConnected());
    }
    void announce(int64_t sid, int32_t egressStream, int32_t clientVersion = 0) {
        WorkerOp a; a.type = WorkerOp::Type::SessionAnnounce; a.grantAll = true; a.sessionId = sid;
        a.egressStreamId = egressStream; a.egressChannel = kEgressChannel; a.clientVersion = clientVersion;
        ASSERT_TRUE(worker->tryEnqueueOp(std::move(a)));
    }
    static std::vector<uint8_t> produceFrame(int64_t sid, int64_t corr, uint8_t acks, int32_t topicId = 1,
                                             std::string_view value = "v") {
        rune::protocol::RecordBatchBuilder b(0, 1000); b.append("k", value, 1000);
        const auto batch = b.build();
        pc::Produce m; m.sessionId = sid; m.correlationId = corr; m.topicId = topicId; m.partition = 0;
        m.partitionKnown = 1; m.acks = acks; m.producerId = -1; m.producerEpoch = 0; m.baseSequence = -1;
        m.recordBatch = std::span<const uint8_t>(batch);
        std::vector<uint8_t> f(m.encodedSize()); m.encode(f.data());
        return f;
    }
    /// A gateway-routed produce (WorkerOp::Produce on the op ring), as the
    /// runtime enqueues it for a client that ignores ingressEndpoint.
    bool routedProduce(int64_t sid, int64_t corr, uint8_t acks = 0) {
        rune::protocol::RecordBatchBuilder b(0, 1000); b.append("k", "v", 1000);
        const auto batch = b.build();
        WorkerOp op; op.type = WorkerOp::Type::Produce; op.topicId = 1; op.partition = 0;
        op.replica = replica.get(); op.logPtr = log.get();
        op.sessionId = sid; op.correlationId = corr; op.acks = acks;
        op.producerId = -1; op.producerEpoch = 0; op.baseSequence = -1;
        op.lastOffsetDelta = 0; op.recordCount = 1;
        op.recordBatchView = std::span<const uint8_t>(batch);
        return worker->tryEnqueueOp(std::move(op));
    }
    /// ONE offer attempt (never loops: a paused client's offers back-pressure).
    static bool offerOnce(aeron::ExclusivePublication& pub, std::vector<uint8_t>& f) {
        aeron::concurrent::AtomicBuffer ab(f.data(), static_cast<aeron::util::index_t>(f.size()));
        return pub.offer(ab, 0, static_cast<aeron::util::index_t>(f.size())) > 0;
    }
    void produce(int64_t corr, uint8_t acksAll, int64_t pid = -1, int32_t seq = -1, int64_t sid = 5,
                 aeron::ExclusivePublication* via = nullptr) {
        rune::protocol::RecordBatchBuilder b(0, 1000); b.append("k", "v", 1000);
        // The batch header carries the same producer identity as the frame (the
        // broker rejects a mismatch — EOS-DUP defense).
        if (pid >= 0) { b.setProducerId(pid); b.setProducerEpoch(0); b.setBaseSequence(seq); }
        const auto batch = b.build();
        pc::Produce m; m.sessionId = sid; m.correlationId = corr; m.topicId = 1; m.partition = 0;
        m.partitionKnown = 1; m.acks = acksAll; m.producerId = pid; m.producerEpoch = 0; m.baseSequence = seq;
        m.recordBatch = std::span<const uint8_t>(batch);
        std::vector<uint8_t> f(m.encodedSize()); m.encode(f.data());
        aeron::concurrent::AtomicBuffer ab(f.data(), static_cast<aeron::util::index_t>(f.size()));
        aeron::ExclusivePublication& pub = via ? *via : *producePub;
        for (int i = 0; i < kCap && pub.offer(ab, 0, static_cast<aeron::util::index_t>(f.size())) <= 0; ++i) pump();
    }
    bool awaitAck(int64_t corr) {
        for (int i = 0; i < kCap && !acks.count(corr); ++i) step();
        return acks.count(corr) == 1;
    }
};
} // namespace

TEST(ShardWorkerAckRegression, AcksDirectlyOnTheSessionPublicationNotTheReturnRing) {
    Fixture f({1}, 1);
    f.produce(/*corr=*/42, /*acks=*/0);
    ASSERT_TRUE(f.awaitAck(42));
    EXPECT_EQ(f.acks[42].errorCode, 0);
    int answers = 0, notifies = 0;
    f.worker->drainReturn([&](WorkerReturn r) {
        if (r.type != WorkerReturn::Type::ProduceResult) return;
        if (r.deferAck) ++notifies; else ++answers;
    });
    EXPECT_EQ(answers, 0) << "shard mode: the runtime never relays produce acks";
    EXPECT_EQ(notifies, 1) << "one post-ack notification per ack (session liveness)";
}

TEST(ShardWorkerAckRegression, DemotedLeaderFiresNotLeaderPromptly) {
    Fixture f({1, 2}, /*minIsr=*/2);                          // HW cannot advance without node 2
    f.produce(/*corr=*/7, /*acks=all*/1);
    for (int i = 0; i < 2'000; ++i) f.step();                 // appended + parked
    ASSERT_EQ(f.acks.count(7), 0u);
    f.replica->becomeFollower(/*leaderEpoch=*/2, /*leaderId=*/2);
    ASSERT_TRUE(f.awaitAck(7));
    EXPECT_EQ(f.acks[7].errorCode, static_cast<int16_t>(pc::ErrorCode::NotLeader));
    EXPECT_LT(f.now, 1'000 + Worker::kDefaultAckTimeoutNs) << "must not wait for the 30 s ack timeout";
}

TEST(ShardWorkerAckRegression, ProduceNotifyFollowsTheAckOnTheReturnRing) {
    Fixture f({1}, 1);
    f.produce(/*corr=*/9, /*acks=*/0, /*pid=*/11, /*seq=*/0);
    ASSERT_TRUE(f.awaitAck(9));
    int notifies = 0, answers = 0; int64_t pid = -1; int64_t sid = -1; int16_t err = -1;
    f.worker->drainReturn([&](WorkerReturn r) {
        if (r.type != WorkerReturn::Type::ProduceResult) return;
        if (!r.deferAck) { ++answers; return; }
        ++notifies; pid = r.producerId; sid = r.sessionId; err = r.errorCode;
    });
    EXPECT_EQ(answers, 0);
    EXPECT_EQ(notifies, 1);
    EXPECT_EQ(pid, -1) << "F15 S5: the notification carries no producer-state facts (the worker psm_ is the sole "
                          "authority; the runtime has no mirror to feed)";
    EXPECT_EQ(sid, 5) << "and the session (runtime liveness)";
    EXPECT_EQ(err, 0) << "a notification is never an answer (facts-only invariant)";
}

TEST(ShardWorkerAckRegression, ErrorAckNotifiesLivenessWithoutFacts) {
    Fixture f({1}, 1);
    f.produce(/*corr=*/3, /*acks=*/0, /*pid=*/11, /*seq=*/5);   // OutOfOrderSequence: seq must start at 0
    ASSERT_TRUE(f.awaitAck(3));
    EXPECT_EQ(f.acks[3].errorCode, static_cast<int16_t>(pc::ErrorCode::OutOfOrderSequence));
    int notifies = 0; int64_t pid = 0;
    f.worker->drainReturn([&](WorkerReturn r) {
        if (r.type == WorkerReturn::Type::ProduceResult && r.deferAck) { ++notifies; pid = r.producerId; }
    });
    EXPECT_EQ(notifies, 1);
    EXPECT_EQ(pid, -1) << "a notification never carries producer-state facts";
}

TEST(ShardWorkerAckRegression, GatewayRoutedProduceIsDedupedByTheWorkerPsm) {
    // Spec §3.4: in shard mode the worker psm_ is the single owner — a
    // gateway-received user produce (WorkerOp::Produce) runs the SAME produce
    // core as worker ingress, so a retried idempotent batch is appended once.
    Fixture f({1}, 1);
    rune::protocol::RecordBatchBuilder b(0, 1000); b.append("k", "v", 1000);
    b.setProducerId(11); b.setProducerEpoch(0); b.setBaseSequence(0);   // header == frame identity
    const auto batch = b.build();
    auto routed = [&](int64_t corr) {
        WorkerOp op; op.type = WorkerOp::Type::Produce; op.topicId = 1; op.partition = 0;
        op.replica = f.replica.get(); op.logPtr = f.log.get();
        op.sessionId = 5; op.correlationId = corr; op.acks = 0;
        op.producerId = 11; op.producerEpoch = 0; op.baseSequence = 0;
        op.lastOffsetDelta = 0; op.recordCount = 1;
        op.recordBatchView = std::span<const uint8_t>(batch);
        ASSERT_TRUE(f.worker->tryEnqueueOp(std::move(op)));
    };
    routed(21);
    ASSERT_TRUE(f.awaitAck(21));
    routed(22);
    ASSERT_TRUE(f.awaitAck(22));
    EXPECT_EQ(f.acks[21].errorCode, 0);
    EXPECT_EQ(f.acks[22].errorCode, 0);
    EXPECT_EQ(f.acks[22].baseOffset, f.acks[21].baseOffset) << "duplicate answered with the original offset";
    EXPECT_EQ(f.log->logEndOffset(), 1) << "the retried batch must not be appended twice";
}

// F15 S5 (Kafka parity, re-review 1 I1 residual): a retry of an acks=all
// batch whose original is still below the HW is answered only when the
// original commits — on shard ingress AND on the gateway-routed op path.
TEST(ShardWorkerAckRegression, AcksAllDuplicateAnsweredOnlyWhenTheOriginalCommits) {
    for (bool routed : {false, true}) {
        Fixture f({1, 2}, /*minIsr=*/1);                       // follower 2 in the ISR: the HW waits for it
        rune::protocol::RecordBatchBuilder b(0, 1000); b.append("k", "v", 1000);
        b.setProducerId(11); b.setProducerEpoch(0); b.setBaseSequence(0);   // header == frame identity
        const auto batch = b.build();
        auto send = [&](int64_t corr) {
            if (!routed) { f.produce(corr, /*acks=all*/1, /*pid=*/11, /*seq=*/0); return; }
            WorkerOp op; op.type = WorkerOp::Type::Produce; op.topicId = 1; op.partition = 0;
            op.replica = f.replica.get(); op.logPtr = f.log.get();
            op.sessionId = 5; op.correlationId = corr; op.acks = 1; op.deferAck = true;
            op.producerId = 11; op.producerEpoch = 0; op.baseSequence = 0;
            op.lastOffsetDelta = 0; op.recordCount = 1; op.durabilityTier = 1;
            op.recordBatchView = std::span<const uint8_t>(batch);
            ASSERT_TRUE(f.worker->tryEnqueueOp(std::move(op)));
        };
        send(31);
        for (int i = 0; i < 2'000; ++i) f.step();
        ASSERT_EQ(f.acks.count(31), 0u) << "precondition: the original is parked on the HW (routed=" << routed << ")";
        send(32);                                              // the client's retry
        for (int i = 0; i < 2'000; ++i) f.step();
        EXPECT_EQ(f.log->logEndOffset(), 1) << "one copy (routed=" << routed << ")";
        EXPECT_EQ(f.acks.count(32), 0u)
            << "the duplicate was acked before its original reached the HW (routed=" << routed << ")";
        rune::replication::FetchRequestMsg fr{1, 0, 2, f.log->logEndOffset(), 1 << 20, 0};
        (void)f.replica->onFetchRequest(fr, f.now);
        ASSERT_TRUE(f.awaitAck(31));
        ASSERT_TRUE(f.awaitAck(32));
        EXPECT_EQ(f.acks[31].errorCode, 0);
        EXPECT_EQ(f.acks[32].errorCode, 0);
        EXPECT_EQ(f.acks[32].baseOffset, f.acks[31].baseOffset);
    }
}

// EOS-OOS: a retransmit older than the dedup window (32) but entirely below the
// producer's lastSequence is a KNOWN-OLD duplicate: answered OK with baseOffset
// -1 (Kafka DUPLICATE_SEQUENCE_NUMBER), never OutOfOrderSequence, never
// re-appended.  Rule: its acks=all answer waits until the batch holding
// lastSequence (the newest one; every record of the duplicate is at or below
// it) is under the HW — not before.  acks=leader answers at once.
TEST(ShardWorkerAckRegression, KnownOldDuplicateAnsweredOkOnlyOnceTheNewestBatchCommits) {
    Fixture f({1, 2}, /*minIsr=*/1);                       // follower 2 in the ISR: the HW waits for it
    const int n = rune::protocol::client::kIdempotentMaxInFlightBatches + 2;   // seq 0..33
    for (int i = 0; i < n; ++i) {
        f.produce(100 + i, /*acks=leader*/0, /*pid=*/11, /*seq=*/i);
        ASSERT_TRUE(f.awaitAck(100 + i));
        ASSERT_EQ(f.acks[100 + i].errorCode, 0) << i;
    }
    ASSERT_EQ(f.log->logEndOffset(), n);
    ASSERT_EQ(f.replica->highWatermark(), 0) << "precondition: nothing committed yet";

    f.produce(200, /*acks=all*/1, 11, /*seq=*/0);          // known-old: out of the window
    for (int i = 0; i < 2'000; ++i) f.step();
    EXPECT_EQ(f.acks.count(200), 0u) << "answered before the batch holding lastSequence committed";
    rune::replication::FetchRequestMsg part{1, 0, 2, /*fetchOffset=*/n / 2, 1 << 20, 0};
    (void)f.replica->onFetchRequest(part, f.now);
    for (int i = 0; i < 2'000; ++i) f.step();
    EXPECT_EQ(f.acks.count(200), 0u) << "HW below the newest batch: still parked";
    rune::replication::FetchRequestMsg all{1, 0, 2, /*fetchOffset=*/n, 1 << 20, 0};
    (void)f.replica->onFetchRequest(all, f.now);
    ASSERT_TRUE(f.awaitAck(200));
    EXPECT_EQ(f.acks[200].errorCode, 0) << "a stored batch is never OutOfOrderSequence";
    EXPECT_EQ(f.acks[200].baseOffset, -1) << "offset unknown (Kafka DUPLICATE_SEQUENCE_NUMBER)";

    f.produce(201, /*acks=leader*/0, 11, /*seq=*/1);       // known-old, acks=leader: at once
    ASSERT_TRUE(f.awaitAck(201));
    EXPECT_EQ(f.acks[201].errorCode, 0);
    EXPECT_EQ(f.acks[201].baseOffset, -1);
    EXPECT_EQ(f.log->logEndOffset(), n) << "a known-old duplicate is never re-appended";
}

// EOS-DUP defense: a frame whose producer identity disagrees with the batch
// header is refused (non-retriable SchemaValidationFailed), never appended, and
// counted on the worker.
TEST(ShardWorkerAckRegression, FrameHeaderIdentityMismatchIsRejectedAndCounted) {
    Fixture f({1}, 1);
    rune::protocol::RecordBatchBuilder b(0, 1000); b.append("k", "v", 1000);   // header pid -1
    const auto batch = b.build();
    pc::Produce m; m.sessionId = 5; m.correlationId = 77; m.topicId = 1; m.partition = 0;
    m.partitionKnown = 1; m.acks = 0; m.producerId = 11; m.producerEpoch = 0; m.baseSequence = 0;
    m.recordBatch = std::span<const uint8_t>(batch);
    std::vector<uint8_t> fr(m.encodedSize()); m.encode(fr.data());
    aeron::concurrent::AtomicBuffer ab(fr.data(), static_cast<aeron::util::index_t>(fr.size()));
    for (int i = 0; i < kCap && f.producePub->offer(ab, 0, static_cast<aeron::util::index_t>(fr.size())) <= 0; ++i)
        f.pump();
    ASSERT_TRUE(f.awaitAck(77));
    EXPECT_EQ(f.acks[77].errorCode, static_cast<int16_t>(pc::ErrorCode::SchemaValidationFailed));
    EXPECT_EQ(f.log->logEndOffset(), 0);
    EXPECT_EQ(f.worker->identityMismatchRejects(), 1u);
}

TEST(ShardWorkerAckRegression, ClosedSessionGetsNoAckAndReannounceRestoresIt) {
    Fixture f({1}, 1);
    f.produce(/*corr=*/1, /*acks=*/0);
    ASSERT_TRUE(f.awaitAck(1));
    WorkerOp c; c.type = WorkerOp::Type::SessionClose; c.sessionId = 5;
    ASSERT_TRUE(f.worker->tryEnqueueOp(std::move(c)));
    f.produce(/*corr=*/2, /*acks=*/0);
    for (int i = 0; i < 2'000; ++i) f.step();
    EXPECT_EQ(f.acks.count(2), 0u) << "a closed session's acks are dropped (the client is gone)";
    WorkerOp a; a.type = WorkerOp::Type::SessionAnnounce; a.grantAll = true; a.sessionId = 5;
    a.egressStreamId = kEgress; a.egressChannel = kEgressChannel;
    ASSERT_TRUE(f.worker->tryEnqueueOp(std::move(a)));
    f.produce(/*corr=*/3, /*acks=*/0);
    EXPECT_TRUE(f.awaitAck(3));
}

TEST(ShardWorkerAckRegression, GatewayRoutedProduceDrainedWithItsJustCommittedAnnouncementIsAcked) {
    // Fix round 2 (finding 5, replaces a test the lifecycle ring made vacuous):
    // the runtime commits a session's announcement and, right after, a gateway-
    // routed Produce of that session — both AFTER this pass drained the
    // lifecycle ring, so the produce op runs in this pass's op drain with its
    // session still unknown.  Its ack must be held until the announcement is
    // applied (next pass), never dropped.  A test task on the op ring commits
    // both exactly there.
    Fixture f({1}, 1);
    f.runtimeDrainsReturns = true;
    for (int i = 0; i < 4; ++i) f.step();
    f.connectSecondClient();
    struct Task : OwnerThreadTestTask {
        Fixture* f; bool routed = false;
        explicit Task(Fixture* fx)
            : OwnerThreadTestTask(Target::Replica, 1, 0, &run, Clock::now() + std::chrono::hours(1)), f(fx) {}
        static void run(OwnerThreadTestTask& t, PartitionReplica*) noexcept {
            auto& self = static_cast<Task&>(t);
            WorkerOp a; a.type = WorkerOp::Type::SessionAnnounce; a.grantAll = true; a.sessionId = 6;
            a.egressStreamId = kEgress2; a.egressChannel = kEgressChannel;
            if (!self.f->worker->tryEnqueueOp(std::move(a))) return;
            self.routed = self.f->routedProduce(6, 61);
        }
    };
    auto task = std::make_shared<Task>(&f);
    WorkerOp t; t.type = WorkerOp::Type::TestTask;
    t.egressPub = new OwnerThreadTestTaskRef(task);
    ASSERT_TRUE(f.worker->tryEnqueueOp(std::move(t)));
    ASSERT_TRUE(f.awaitAck(61)) << "the produce of a just-announced session must be acked";
    ASSERT_TRUE(task->routed);
    EXPECT_EQ(f.acks[61].sessionId, 6);
    EXPECT_EQ(f.worker->sessionAcksDroppedForTest(), 0u);
}

// ===========================================================================
// Task 13 fix round 1.
// ===========================================================================
namespace {
bool allAcked(const Fixture& f, int64_t from, int64_t to) {
    for (int64_t c = from; c < to; ++c) if (!f.acks.count(c)) return false;
    return true;
}
} // namespace

// C1 + m7: a client that stops reading its acks (its publication back-
// pressures) must not stall the shard: a role change through the REAL
// BecomeFollower op and a new session's announce + produce are applied and
// answered within a bounded number of passes while it still holds acks.  (Was:
// >= 1024 held acks paused the whole op ring — role ops, announcements and
// closes stuck behind one slow client; only the 5 s stall timer could free it.)
TEST(ShardWorkerAckRegression, SlowSessionBacklogNeverStallsRoleOpsOrNewSessions) {
    Fixture f({1}, 1);
    f.runtimeDrainsReturns = true;                          // as the runtime does every cycle
    for (int i = 0; i < 4; ++i) f.step();
    f.egressLimit = 0;                                      // session 5's client stops reading acks
    const int64_t c0 = 1'000'000;
    int64_t corr = c0;
    int refused = 0;
    for (int i = 0; i < 20'000 && f.worker->sessionAcksPendingForTest() < 1'100 && refused < 1'000; ++i) {
        auto fr = Fixture::produceFrame(5, corr, 0);
        if (Fixture::offerOnce(*f.producePub, fr)) { ++corr; refused = 0; } else ++refused;
        f.step();
    }
    ASSERT_GT(corr - c0, 400) << "session 5 must have out-run its ack publication";
    ASSERT_GT(f.worker->sessionAcksPendingForTest(), 0u) << "precondition: session 5 holds acks";

    f.addSecondClient(6);
    auto fr6 = Fixture::produceFrame(6, 6'000'001, 0);
    ASSERT_TRUE(Fixture::offerOnce(*f.pub2, fr6));
    for (int i = 0; i < 2'000 && !f.acks.count(6'000'001); ++i) f.step();
    ASSERT_EQ(f.acks.count(6'000'001), 1u)
        << "a new session's announce + produce must not wait behind another client's ack backlog";

    WorkerOp bf; bf.type = WorkerOp::Type::BecomeFollower; bf.topicId = 1; bf.partition = 0;
    bf.replica = f.replica.get(); bf.roleEpoch = 2; bf.roleLeaderId = 2;
    ASSERT_TRUE(f.worker->tryEnqueueOp(std::move(bf)));
    for (int i = 0; i < 2'000 && f.replica->role() != Role::Follower; ++i) f.step();
    EXPECT_EQ(f.replica->role(), Role::Follower) << "a role op must not wait behind a slow client's acks";
    EXPECT_GT(f.worker->sessionAcksPendingForTest(), 0u) << "session 5 still held acks during all of it";
    EXPECT_GT(f.worker->intakePausedFramesForTest(), 0u) << "G-A6: session 5's intake was paused, not the shard's";

    f.egressLimit = 64;                                     // session 5 reads again: nothing was lost
    for (int i = 0; i < 50'000 && !allAcked(f, c0, corr); ++i) f.step();
    EXPECT_TRUE(allAcked(f, c0, corr));
    EXPECT_EQ(f.worker->sessionAcksDroppedForTest(), 0u);
}

// C1: a legal client that consumes ONE ack per pass while producing faster
// must never stall another session's intake.  (Was: its held acks grew to the
// queue's capacity and the poll budget of the whole shard reached 0; the stall
// timer never fired because every claim reset it.)
TEST(ShardWorkerAckRegression, TrickleConsumerNeverStallsOtherSessionsIntake) {
    Fixture f({1}, 1);
    f.runtimeDrainsReturns = true;                          // as the runtime does every cycle
    for (int i = 0; i < 4; ++i) f.step();
    f.addSecondClient(6);
    for (int i = 0; i < 4; ++i) f.step();
    f.egressLimit = 1;
    int64_t c5 = 1'000'000;
    const int64_t c6 = 6'000'000;
    int64_t sent6 = 0, late6 = 0;
    for (int i = 0; i < 30'000; ++i) {
        for (int k = 0; k < 8; ++k) {
            auto fr = Fixture::produceFrame(5, c5, 0);
            if (!Fixture::offerOnce(*f.producePub, fr)) break;
            ++c5;
        }
        if (i % 50 == 0) {
            // Session 6's previous produce must have been acked within these 50
            // passes — WHILE session 5 keeps out-producing its consumption.
            if (sent6 > 0 && !f.acks.count(c6 + sent6 - 1)) ++late6;
            auto fr = Fixture::produceFrame(6, c6 + sent6, 0);
            if (Fixture::offerOnce(*f.pub2, fr)) ++sent6;
        }
        f.step();
    }
    EXPECT_GT(c5 - 1'000'000, 30'000) << "precondition: session 5 produced far more than it read";
    EXPECT_EQ(late6, 0) << "session 6 waited behind session 5's held acks";
    for (int i = 0; i < 2'000 && !allAcked(f, c6, c6 + sent6); ++i) f.step();
    EXPECT_EQ(sent6, 600) << "session 6's offers were never back-pressured";
    EXPECT_TRUE(allAcked(f, c6, c6 + sent6)) << "every produce of session 6 must be acked";
    EXPECT_EQ(f.worker->sessionAcksDroppedForTest(), 0u);
}

// C2: no ack of an OPEN session is dropped on time.  A client that stalls for
// longer than any window (6 s of worker clock here) still gets every ack once
// it reads again.  (Was: dropped after kStallDropNs = 5 s, silently.)
TEST(ShardWorkerAckRegression, StalledClientGetsEveryAckAfterAnyStall) {
    Fixture f({1}, 1);
    f.runtimeDrainsReturns = true;                          // as the runtime does every cycle
    for (int i = 0; i < 4; ++i) f.step();
    f.egressLimit = 0;
    const int64_t c0 = 2'000'000;
    int64_t corr = c0;
    int refused = 0;
    for (int i = 0; i < 20'000 && corr - c0 < 600 && refused < 1'000; ++i) {
        auto fr = Fixture::produceFrame(5, corr, 0);
        if (Fixture::offerOnce(*f.producePub, fr)) { ++corr; refused = 0; } else ++refused;
        f.step();
    }
    ASSERT_GT(f.worker->sessionAcksPendingForTest(), 0u) << "precondition: acks held for the stalled client";
    f.now += 6'000'000'000LL;                               // the client stays stalled for 6 s
    for (int i = 0; i < 100; ++i) f.step();
    f.egressLimit = 64;
    for (int i = 0; i < 50'000 && !allAcked(f, c0, corr); ++i) f.step();
    EXPECT_TRUE(allAcked(f, c0, corr)) << "a live session's acks are never dropped on a timer";
    EXPECT_EQ(f.worker->sessionAcksDroppedForTest(), 0u);
}

// C2: a session whose ack publication can never deliver is REPORTED to the
// runtime (WorkerReturn::SessionFailed → the runtime closes it; the client
// reconnects) — never a silent black hole.
TEST(ShardWorkerAckRegression, UndeliverableSessionPublicationIsReportedToTheRuntime) {
    Fixture f({1}, 1);
    WorkerOp a; a.type = WorkerOp::Type::SessionAnnounce; a.grantAll = true; a.sessionId = 8;
    a.egressStreamId = kEgress2; a.egressChannel = "aeron:udp?endpoint=127.0.0.1:notaport";
    ASSERT_TRUE(f.worker->tryEnqueueOp(std::move(a)));
    int failed = 0; int64_t failedSid = 0;
    for (int i = 0; i < 5'000 && failed == 0; ++i) {
        f.step();
        f.worker->drainReturn([&](WorkerReturn r) {
            if (isFailedSignal(r)) { ++failed; failedSid = r.sessionId; }
        });
    }
    ASSERT_EQ(failed, 1) << "an undeliverable session must be reported";
    EXPECT_EQ(failedSid, 8);
    for (int i = 0; i < 200; ++i) {
        f.step();
        f.worker->drainReturn([&](WorkerReturn r) { if (isFailedSignal(r)) ++failed; });
    }
    EXPECT_EQ(failed, 1) << "reported exactly once";
    EXPECT_EQ(f.worker->sessionRegistrationsDrainingForTest(), 0u)
        << "an errored registration is already gone (real Aeron) — never on the drain list";
}

// I1: never skip the client poll for pending ops — continuous runtime op
// traffic (drain limit below the arrival rate) must not starve shard ingress.
// (Was: the poll ran only with the op ring empty after two drains.)
TEST(ShardWorkerAckRegression, ContinuousOpTrafficNeverSkipsTheIngressPoll) {
    Fixture f({1}, 1);
    f.runtimeDrainsReturns = true;                          // as the runtime does every cycle
    for (int i = 0; i < 4; ++i) f.step();
    f.worker->setIngressDrainLimit(1);
    auto unsub = [&](int64_t id) {
        WorkerOp u; u.type = WorkerOp::Type::Unsubscribe; u.subscriptionId = id;
        ASSERT_TRUE(f.worker->tryEnqueueOp(std::move(u)));
    };
    for (int64_t k = 0; k < 3; ++k) unsub(900'000 + k);      // a standing backlog
    auto fr = Fixture::produceFrame(5, 77, 0);
    ASSERT_TRUE(Fixture::offerOnce(*f.producePub, fr));
    for (int i = 0; i < 2'000 && !f.acks.count(77); ++i) {
        unsub(910'000 + 2 * i); unsub(910'001 + 2 * i);     // ops keep arriving
        f.step();
    }
    EXPECT_EQ(f.acks.count(77), 1u) << "shard ingress must be polled every pass";
}

// I1 (aeron:ipc TOCTOU): a client frame can become visible in the same pass as
// its session's announcement is enqueued — AFTER the lifecycle ring was drained
// and BEFORE the poll.  Its ack must be HELD until the announcement is applied
// (next pass), never dropped as "unknown session".  A test task on the op ring
// runs exactly there (op drain → poll).
namespace {
struct MidPassAnnounce : OwnerThreadTestTask {
    Fixture* f;
    bool offered = false;
    explicit MidPassAnnounce(Fixture* fx)
        : OwnerThreadTestTask(Target::Replica, 1, 0, &run, Clock::now() + std::chrono::hours(1)), f(fx) {}
    static void run(OwnerThreadTestTask& t, PartitionReplica*) noexcept {
        auto& self = static_cast<MidPassAnnounce&>(t);
        WorkerOp a; a.type = WorkerOp::Type::SessionAnnounce; a.grantAll = true; a.sessionId = 7;
        a.egressStreamId = kEgress2; a.egressChannel = kEgressChannel;
        if (!self.f->worker->tryEnqueueOp(std::move(a))) return;   // the runtime announces...
        auto fr = Fixture::produceFrame(7, 7'000'001, 0);           // ...the client's frame lands
        self.offered = Fixture::offerOnce(*self.f->pub2, fr);
    }
};
} // namespace

TEST(ShardWorkerAckRegression, FrameVisibleBeforeItsAnnouncementIsAppliedIsStillAcked) {
    Fixture f({1}, 1);
    f.runtimeDrainsReturns = true;                          // as the runtime does every cycle
    for (int i = 0; i < 4; ++i) f.step();
    f.connectSecondClient();
    auto task = std::make_shared<MidPassAnnounce>(&f);
    WorkerOp t; t.type = WorkerOp::Type::TestTask;
    t.egressPub = new OwnerThreadTestTaskRef(task);
    ASSERT_TRUE(f.worker->tryEnqueueOp(std::move(t)));
    for (int i = 0; i < 2'000 && !f.acks.count(7'000'001); ++i) f.step();
    ASSERT_TRUE(task->offered);
    EXPECT_EQ(f.acks.count(7'000'001), 1u) << "the ack of a frame seen before its announcement must be held, not dropped";
    EXPECT_EQ(f.worker->sessionAcksDroppedForTest(), 0u);
}

// I3 (fix on sight (d) of Task 13): shard-ingress produces run the Task 7
// per-topic schema validation — a batch failing it is refused with
// SchemaValidationFailed and never appended.  (Base ede91e8 appended it.)
TEST(ShardWorkerAckRegression, ShardIngressEnforcesTopicSchema) {
    Fixture f({1}, 1);
    const auto desc = rune::codec::DescriptorBuilder("characters")
                          .add("id", rune::codec::PrimType::Int64)
                          .add("type", rune::codec::PrimType::Enum, 1, false, {0, 1, 2})
                          .add("name", rune::codec::PrimType::String)
                          .build();
    WorkerOp su; su.type = WorkerOp::Type::SchemaUpdate; su.topicId = 1;
    su.schemaUpdateBlob = rune::codec::serialize(desc);
    ASSERT_TRUE(f.worker->tryEnqueueOp(std::move(su)));
    for (int i = 0; i < 4; ++i) f.step();
    auto bad = Fixture::produceFrame(5, 31, 0, /*topicId=*/1, /*value=*/"v");   // not a 'characters' record
    ASSERT_TRUE(Fixture::offerOnce(*f.producePub, bad));
    ASSERT_TRUE(f.awaitAck(31));
    EXPECT_EQ(f.acks[31].errorCode, static_cast<int16_t>(pc::ErrorCode::SchemaValidationFailed));
    EXPECT_EQ(f.log->logEndOffset(), 0) << "a schema-invalid batch is never appended";
}

// Fix round 2 (finding 1, critical): C1 on the GATEWAY-ROUTED path — a client
// that ignores ingressEndpoint (every Produce through the node endpoint → op
// ring) and stops reading its acks must not fill the shard's ack queue.  The
// worker signals Held for the session; the runtime (emulated here exactly as
// the gateway does it: no more routed frames for a Held session until
// Released) pauses only that client.  Role ops and other sessions keep flowing
// and the op drain never pauses for want of ack room.
TEST(ShardWorkerAckRegression, GatewayRoutedSlowSessionIsPausedNotTheShard) {
    Fixture f({1}, 1);
    f.runtimeDrainsReturns = true;
    for (int i = 0; i < 4; ++i) f.step();
    f.egressLimit = 0;                                      // session 5 stops reading acks
    bool held5 = false;
    int heldEdges = 0, releasedEdges = 0;
    f.onReturn = [&](const WorkerReturn& r) {
        if (r.type != WorkerReturn::Type::SessionSignal || r.sessionId != 5) return;
        if (r.lastOffset == static_cast<int64_t>(SessionSignalKind::Held)) { held5 = true; ++heldEdges; }
        if (r.lastOffset == static_cast<int64_t>(SessionSignalKind::Released)) { held5 = false; ++releasedEdges; }
    };
    const int64_t c0 = 4'000'000;
    int64_t corr = c0;
    int pausedPasses = 0;
    for (int i = 0; i < 40'000 && corr - c0 < 17'000 && pausedPasses < 200; ++i) {
        if (!held5) {
            pausedPasses = 0;
            for (int k = 0; k < 8 && f.routedProduce(5, corr); ++k) ++corr;
        } else {
            ++pausedPasses;                                 // the gateway ABORTs session 5's frames
        }
        f.step();
    }
    ASSERT_GT(corr - c0, 300) << "session 5 out-ran its ack publication";
    EXPECT_TRUE(held5) << "the worker must report the slow session Held";

    f.addSecondClient(6);
    ASSERT_TRUE(f.routedProduce(6, 6'000'001));
    for (int i = 0; i < 2'000 && !f.acks.count(6'000'001); ++i) f.step();
    EXPECT_EQ(f.acks.count(6'000'001), 1u) << "another session's routed produce is acked";
    WorkerOp bf; bf.type = WorkerOp::Type::BecomeFollower; bf.topicId = 1; bf.partition = 0;
    bf.replica = f.replica.get(); bf.roleEpoch = 2; bf.roleLeaderId = 2;
    ASSERT_TRUE(f.worker->tryEnqueueOp(std::move(bf)));
    for (int i = 0; i < 2'000 && f.replica->role() != Role::Follower; ++i) f.step();
    EXPECT_EQ(f.replica->role(), Role::Follower) << "a role op is never stuck behind a slow client's acks";
    EXPECT_EQ(f.worker->ackRoomOpPausesForTest(), 0u) << "the op drain never ran out of ack room";

    f.egressLimit = 64;                                     // session 5 reads again: nothing lost
    for (int i = 0; i < 50'000 && !allAcked(f, c0, corr); ++i) f.step();
    EXPECT_TRUE(allAcked(f, c0, corr));
    for (int i = 0; i < 100 && held5; ++i) f.step();
    EXPECT_FALSE(held5) << "Released once its acks drained";
    EXPECT_EQ(heldEdges, releasedEdges);
    EXPECT_EQ(f.worker->sessionAcksDroppedForTest(), 0u);
}

// Fix round 2 (finding 9): a session signal the return ring cannot take now is
// deferred (counted), never lost — delivered exactly once when there is room.
TEST(ShardWorkerAckRegression, DeferredSessionSignalIsCountedAndStillDeliveredOnce) {
    Fixture f({1}, 1);
    for (int i = 0; i < 4; ++i) f.step();
    WorkerReturn filler; filler.type = WorkerReturn::Type::OwnershipAck;
    while (f.worker->injectReturnForTest(filler)) {}        // the runtime is slow: ret_ full
    ASSERT_TRUE(f.routedProduce(5, 71));                    // its notification goes to the FIFO
    f.step();
    WorkerOp a; a.type = WorkerOp::Type::SessionAnnounce; a.grantAll = true; a.sessionId = 8;
    a.egressStreamId = kEgress2; a.egressChannel = "aeron:udp?endpoint=127.0.0.1:notaport";
    ASSERT_TRUE(f.worker->tryEnqueueOp(std::move(a)));
    for (int i = 0; i < 50; ++i) f.step();
    EXPECT_GT(f.worker->sessionSignalsDeferredForTest(), 0u) << "deferred while the FIFO is non-empty";
    f.runtimeDrainsReturns = true;                          // the runtime catches up
    for (int i = 0; i < 2'000 && f.sessionFailures.empty(); ++i) f.step();
    for (int i = 0; i < 50; ++i) f.step();
    ASSERT_EQ(f.sessionFailures.size(), 1u);
    EXPECT_EQ(f.sessionFailures[0].sessionId, 8);
}

// ============================================================================
// D6 review M3 — the SHARD-MODE production chain, end to end, on the real
// invoker driver: SessionAnnounce (WorkerOp.clientVersion, through the op
// ring's SBE encode) -> Worker::executeSessionAnnounceWire_ -> the sink's
// per-session version; a produce of producer P (whose every batch retention
// removed before the worker's RC rebuild) on shard ingress -> the worker psm_
// answers UnknownProducerId -> emitProduceAck_ copies the log start
// (ret.baseOffset -> AckEntry.logStartOffset) -> SessionAckSink encodes per
// session -> the client's egress.  A clientVersion-2 session sees 28 + the log
// start (46 B frame); a clientVersion-1 session sees OutOfOrderSequence on the
// pre-D6 38 B frame (review M4).  Dropping either Worker.h line the review
// names (the logStartOffset copy, the announce version argument) turns it red.
// ============================================================================
namespace {
std::vector<uint8_t> pidBatch(int64_t pid, int16_t epoch, int32_t seq) {
    rune::protocol::RecordBatchBuilder b(0, 1000);
    b.setProducerId(pid); b.setProducerEpoch(epoch); b.setBaseSequence(seq);
    b.append("k", "v" + std::to_string(seq), 1000);
    return b.build();
}
void preloadSeqs(rune::storage::Log& log, int64_t pid, int16_t epoch, int32_t from, int32_t to) {
    for (int32_t s = from; s <= to; ++s) {
        const auto b = pidBatch(pid, epoch, s);
        int64_t base = 0;
        (void)log.appendNoSyncRestamp(b.data(), b.size(),
                                      rune::protocol::RecordBatchReader(b.data(), b.size()).lastOffsetDelta(), base);
    }
}
} // namespace

TEST(ShardWorkerAckUnknownProducerIdRegression, ShardChainAnswersUnknownProducerIdPerSessionVersionRegression) {
    int64_t logStart = -1;
    Fixture f({1}, 1,
              [&](rune::storage::Log& log) {
                  preloadSeqs(log, /*P*/ 7, 0, 0, 5);
                  preloadSeqs(log, /*Q*/ 8, 2, 0, 11);
                  ASSERT_GT(log.deleteOldSegments(0), 0);
                  logStart = log.logStartOffset();
              },
              /*rollBytes=*/256, /*session5Version=*/pc::kClientVersion);
    ASSERT_GE(logStart, 6) << "precondition: retention removed every batch of P";
    f.addSecondClient(/*sid=*/6, /*clientVersion=*/1);
    for (int i = 0; i < kCap && f.worker->rcStatusForTest(1, 0) != Worker::kRcReady; ++i) f.step();
    ASSERT_EQ(f.worker->rcStatusForTest(1, 0), Worker::kRcReady) << "precondition: the worker's state is proven";
    ASSERT_EQ(f.worker->sessionAnnouncesSeenForTest(), 2u);
    const int64_t leo = f.log->logEndOffset();

    f.produce(/*corr=*/61, /*acks=*/0, /*pid=*/7, /*seq=*/6, /*sid=*/5);
    ASSERT_TRUE(f.awaitAck(61));
    EXPECT_EQ(f.acks[61].errorCode, static_cast<int16_t>(pc::ErrorCode::UnknownProducerId))
        << "a clientVersion-2 session must see 28 in shard mode (the announce carries the version to the sink)";
    EXPECT_EQ(f.acks[61].logStartOffset, logStart)
        << "the shard ack must carry the log start (else the client always takes U2)";
    EXPECT_EQ(f.ackBytes[61], pc::ProduceResponse::ENCODED_SIZE);

    f.produce(/*corr=*/62, /*acks=*/0, /*pid=*/7, /*seq=*/6, /*sid=*/6, f.pub2.get());
    ASSERT_TRUE(f.awaitAck(62));
    EXPECT_EQ(f.acks[62].errorCode, static_cast<int16_t>(pc::ErrorCode::OutOfOrderSequence))
        << "an old client must never see UnknownProducerId";
    EXPECT_EQ(f.acks[62].logStartOffset, -1);
    EXPECT_EQ(f.ackBytes[62], pc::ProduceResponse::ENCODED_SIZE_V0) << "review M4: the pre-D6 frame";
    EXPECT_EQ(f.log->logEndOffset(), leo) << "an UnknownProducerId batch was appended";

    // The client's U1 re-sequence (new epoch, seq 0) is accepted and acked OK.
    f.produce(/*corr=*/63, /*acks=*/0, /*pid=*/7, /*seq=*/0, /*sid=*/5);
    ASSERT_TRUE(f.awaitAck(63));
    EXPECT_EQ(f.acks[63].errorCode, 0);
    EXPECT_EQ(f.acks[63].baseOffset, leo);
}

// D8c: the worker returns one produce credit per CLAIMED ack of a gateway-routed (op-ring) produce — acks=0 and
// acks=all alike — and none for its direct-ingress produces; a held (unclaimed) ack returns nothing until the client
// reads it.  Credits + held routed acks == routed produces at every step (no credit leaked or invented).
TEST(ShardWorkerAckRegression, RoutedProduceCreditsReturnOnClaimOnlyRegression) {
    Fixture f({1}, 1);
    f.runtimeDrainsReturns = true;
    for (int i = 0; i < 4; ++i) f.step();
    int64_t credits = 0;
    f.onReturn = [&](const WorkerReturn& r) {
        if (r.type == WorkerReturn::Type::SessionSignal && r.sessionId == 5 &&
            r.lastOffset == static_cast<int64_t>(SessionSignalKind::Credit)) {
            EXPECT_GT(r.baseOffset, 0) << "a Credit report carries a positive count";
            credits += r.baseOffset;
        }
    };
    ASSERT_TRUE(f.routedProduce(5, 10, /*acks=*/0));
    ASSERT_TRUE(f.routedProduce(5, 11, /*acks=*/1));        // acks=all (op deferAck unset: answered at append)
    f.produce(12, 0);                                       // direct ingress: no credit
    f.produce(13, 1);
    for (int64_t c : {10, 11, 12, 13}) ASSERT_TRUE(f.awaitAck(c)) << c;
    for (int i = 0; i < 4; ++i) f.step();
    EXPECT_EQ(credits, 2) << "one credit per claimed routed ack, none for direct ones";

    f.egressLimit = 0;                                      // the client stops reading
    credits = 0;
    const int64_t c0 = 100;
    int64_t sent = 0;
    for (int i = 0; i < 400 && sent < 3'000; ++i) {
        for (int k = 0; k < 16 && f.routedProduce(5, c0 + sent); ++k) ++sent;
        f.step();
    }
    for (int i = 0; i < 64; ++i) f.step();
    const auto held = static_cast<int64_t>(f.worker->sessionAcksPendingForTest());
    ASSERT_GT(held, 0) << "precondition: the client's ack window filled";
    EXPECT_EQ(credits + held, sent) << "credits returned for exactly the claimed routed acks";
    f.egressLimit = 64;
    for (int i = 0; i < 50'000 && !allAcked(f, c0, c0 + sent); ++i) f.step();
    ASSERT_TRUE(allAcked(f, c0, c0 + sent));
    for (int i = 0; i < 4; ++i) f.step();
    EXPECT_EQ(credits, sent) << "every routed ack returned its credit once claimed";
}

// D8c fix round 1 (MEDIUM-1): every ANSWER of a gateway-routed produce returns its credit once claimed — the
// leak paths: the ownership-guard refusal (refuseNotOwned_, a stale incarnation), the role-gate NotLeader, a parked
// acks=all failed by demotion (the purge of its waiters) and a parked acks=all failed by the ack timeout.
namespace {
struct CreditCount {
    int64_t n = 0;
    void attach(Fixture& f) {
        f.runtimeDrainsReturns = true;
        f.onReturn = [this](const WorkerReturn& r) {
            if (r.type == WorkerReturn::Type::SessionSignal && r.sessionId == 5 &&
                r.lastOffset == static_cast<int64_t>(SessionSignalKind::Credit))
                n += r.baseOffset;
        };
    }
};
bool routedOp(Fixture& f, int64_t corr, bool acksAll, uint32_t staleIncarnation = 0) {
    rune::protocol::RecordBatchBuilder b(0, 1000); b.append("k", "v", 1000);
    const auto batch = b.build();
    WorkerOp op; op.type = WorkerOp::Type::Produce; op.topicId = 1; op.partition = 0;
    op.replica = f.replica.get(); op.logPtr = f.log.get();
    op.sessionId = 5; op.correlationId = corr; op.acks = acksAll ? 1 : 0; op.deferAck = acksAll;
    op.producerId = -1; op.producerEpoch = 0; op.baseSequence = -1;
    op.lastOffsetDelta = 0; op.recordCount = 1;
    if (staleIncarnation != 0) { op.targetReplicaId = 1; op.incarnation = staleIncarnation; }
    op.recordBatchView = std::span<const uint8_t>(batch);
    return f.worker->tryEnqueueOp(std::move(op));
}
} // namespace

TEST(ShardWorkerAckRegression, RefusedRoutedProduceReturnsItsCreditRegression) {
    Fixture f({1}, 1);
    CreditCount c; c.attach(f);
    for (int i = 0; i < 4; ++i) f.step();
    ASSERT_TRUE(routedOp(f, 31, false, /*staleIncarnation=*/99));      // ownership guard: refuseNotOwned_
    ASSERT_TRUE(f.awaitAck(31));
    EXPECT_EQ(f.acks[31].errorCode, static_cast<int16_t>(pc::ErrorCode::NotLeader));
    for (int i = 0; i < 4; ++i) f.step();
    EXPECT_EQ(c.n, 1) << "the ownership-guard refusal returns the credit";
    f.replica->becomeFollower(/*leaderEpoch=*/2, /*leaderId=*/2);         // role gate: produceCore_ NotLeader
    ASSERT_TRUE(routedOp(f, 32, false));
    ASSERT_TRUE(f.awaitAck(32));
    EXPECT_EQ(f.acks[32].errorCode, static_cast<int16_t>(pc::ErrorCode::NotLeader));
    for (int i = 0; i < 4; ++i) f.step();
    EXPECT_EQ(c.n, 2) << "the role-gate refusal returns the credit";
}

TEST(ShardWorkerAckRegression, ParkedRoutedAcksAllFailedByDemotionReturnsItsCreditRegression) {
    Fixture f({1, 2}, /*minIsr=*/2);                                  // HW cannot advance without node 2
    CreditCount c; c.attach(f);
    for (int i = 0; i < 4; ++i) f.step();
    ASSERT_TRUE(routedOp(f, 41, /*acksAll=*/true));
    for (int i = 0; i < 200; ++i) f.step();
    ASSERT_EQ(f.acks.count(41), 0u) << "parked on the HW";
    EXPECT_EQ(c.n, 0) << "a parked produce holds its credit";
    f.replica->becomeFollower(/*leaderEpoch=*/2, /*leaderId=*/2);         // its waiters are failed (purged)
    ASSERT_TRUE(f.awaitAck(41));
    EXPECT_EQ(f.acks[41].errorCode, static_cast<int16_t>(pc::ErrorCode::NotLeader));
    for (int i = 0; i < 4; ++i) f.step();
    EXPECT_EQ(c.n, 1);
}

TEST(ShardWorkerAckRegression, ParkedRoutedAcksAllTimedOutReturnsItsCreditRegression) {
    Fixture f({1, 2}, /*minIsr=*/2);
    f.worker->setAckTimeoutNs(1'000'000);                              // 1 ms of worker clock (10 µs per step)
    CreditCount c; c.attach(f);
    for (int i = 0; i < 4; ++i) f.step();
    ASSERT_TRUE(routedOp(f, 51, /*acksAll=*/true));
    f.step();
    ASSERT_EQ(f.acks.count(51), 0u);
    EXPECT_EQ(c.n, 0);
    ASSERT_TRUE(f.awaitAck(51));                                       // the ack timeout fires
    EXPECT_NE(f.acks[51].errorCode, 0) << "answered with an error by the timeout";
    for (int i = 0; i < 4; ++i) f.step();
    EXPECT_EQ(c.n, 1) << "the timed-out answer returns the credit";
}

// ============================================================================
// D8d (D8c review LOW-R2): stalled DIRECT (worker-ingress) clients.  Each
// stalled client below has its own ingress publication (its own image, a 2 MiB
// term so one burst out-runs any poll budget) and an ack publication nobody
// reads (no subscriber: NOT_CONNECTED, held from its first ack).
// ============================================================================
namespace {
struct StalledDirectClients {
    std::vector<std::shared_ptr<aeron::ExclusivePublication>> pubs;
    std::vector<std::shared_ptr<aeron::Subscription>> unread;   // connected ack streams nobody polls
    std::vector<int64_t> sids;
    /// `n` sessions sid0.. announced with an unread ack stream, each with its own
    /// ingress publication.  `connected` (review LOW-2): the ack stream has a
    /// subscriber that never polls — the publication connects, delivers its
    /// window, then back-pressures (a client that stopped reading); otherwise
    /// nobody subscribes (never connected).
    void add(Fixture& f, int n, int64_t sid0, bool connected = false, int32_t streamBase = 9'000) {
        if (connected)
            for (int i = 0; i < n; ++i) {
                const int64_t r = f.client->addSubscription("aeron:ipc", streamBase + i);
                std::shared_ptr<aeron::Subscription> sub;
                for (int k = 0; k < kCap && !sub; ++k) { f.pump(); sub = f.client->findSubscription(r); }
                ASSERT_TRUE(sub);
                unread.push_back(std::move(sub));
            }
        std::vector<int64_t> regs;
        for (int i = 0; i < n; ++i)
            regs.push_back(f.client->addExclusivePublication("aeron:ipc?term-length=2m", kIngress));
        pubs.resize(regs.size());
        auto allConnected = [&] {
            for (auto& p : pubs) if (!p || !p->isConnected()) return false;
            return true;
        };
        for (int k = 0; k < kCap && !allConnected(); ++k) {
            f.pump();
            for (std::size_t i = 0; i < regs.size(); ++i)
                if (!pubs[i]) pubs[i] = f.client->findExclusivePublication(regs[i]);
        }
        ASSERT_TRUE(allConnected());
        for (int i = 0; i < n; ++i) {
            sids.push_back(sid0 + i);
            f.announce(sid0 + i, streamBase + i);                // unread (or no subscriber: never connected)
        }
    }
    /// Each client offers up to `frames` acks=Leader produces at once (never loops on refusal).
    int64_t burst(int frames, int64_t corrBase) {
        int64_t sent = 0;
        for (std::size_t i = 0; i < pubs.size(); ++i)
            for (int k = 0; k < frames; ++k) {
                auto fr = Fixture::produceFrame(sids[i], corrBase + static_cast<int64_t>(i) * 1'000'000 + k, 0);
                if (!Fixture::offerOnce(*pubs[i], fr)) break;
                ++sent;
            }
        return sent;
    }
};
} // namespace

// D8d RED: many stalled direct sessions must not stop another direct session's
// intake on the same worker, nor the op drain.  (Was: each stalled client's
// first poll admitted a whole poll budget of produces — up to 4096 acks per
// session at the fragment-limit ceiling —
// until the worker's own-intake half was gone: ackRoom_ hit 0, the op drain
// paused every pass and the poll budget was 0, so session 6's produce was never
// even polled.)
TEST(ShardWorkerAckRegression, ManyStalledDirectSessionsCannotStopAnotherDirectSessionsIntakeRegression) {
    // Production-sized rings (32 MiB): the return ring never limits a poll.
    Fixture f({1}, 1, {}, 64LL * 1024 * 1024, 0, Worker::kReturnByteCapacity);
    f.runtimeDrainsReturns = true;
    // The configurable per-poll ceiling (RUNE_GATEWAY_FRAGMENT_LIMIT <= kMaxFragmentLimit)
    // the worker's reserve is sized for: one poll may admit 4096 frames of one image.
    f.worker->setDesiredFragmentLimit(Worker::kMaxFragmentLimit);
    for (int i = 0; i < 4; ++i) f.step();
    StalledDirectClients st;
    st.add(f, 10, 100);
    f.addSecondClient(6);
    for (int i = 0; i < 8; ++i) f.step();
    const int64_t sent = st.burst(6'000, 10'000'000);
    ASSERT_GT(sent, 40'000) << "precondition: the stalled clients out-ran any poll budget";
    for (int i = 0; i < 200; ++i) f.step();
    ASSERT_GT(f.worker->sessionAcksPendingForTest(), 0u) << "precondition: the worker holds the stalled acks";

    auto fr6 = Fixture::produceFrame(6, 6'000'001, 0);
    ASSERT_TRUE(Fixture::offerOnce(*f.pub2, fr6));
    for (int i = 0; i < 2'000 && !f.acks.count(6'000'001); ++i) f.step();
    EXPECT_EQ(f.acks.count(6'000'001), 1u)
        << "another direct session's produce must be polled and acked while stalled sessions hold acks";
    EXPECT_EQ(f.worker->ackRoomOpPausesForTest(), 0u) << "the op drain never ran out of ack room";
    EXPECT_LE(f.worker->directAcksInFlightForTest(), Worker::kDirectAckBudget)
        << "stalled direct sessions hold at most the worker's direct share";
}

// D8d: when stalled direct sessions fill the worker's whole direct share, the
// worker EXPIRES the session held there with the largest share (SessionSignal
// Expire → the runtime closes it: SessionClose back on the lifecycle ring), its
// queued acks drop and free the share, and a reading session is served.  Never a
// session that is not held (session 6 reads its acks); never more than one in
// flight.  (A small cap — 400, budget 200 — keeps the burst short; the policy is
// the same at the default kDirectAckBudget.)
TEST(ShardWorkerAckRegression, FullDirectShareExpiresTheHeldLargestShareSessionAndServesAnotherRegression) {
    Fixture f({1}, 1, {}, 64LL * 1024 * 1024, 0, Worker::kReturnByteCapacity);
    f.runtimeDrainsReturns = true;
    f.worker->setDirectAckBudgetsForTest(200, 400);
    f.worker->setDesiredFragmentLimit(Worker::kMaxFragmentLimit);   // the bursts settle in a few passes
    std::vector<std::pair<int64_t, int64_t>> expired;       // (sid, share at expiry)
    int closesInFlight = 0, maxClosesInFlight = 0;
    bool largest = true;
    StalledDirectClients st;
    f.onReturn = [&](const WorkerReturn& r) {
        if (r.type != WorkerReturn::Type::SessionSignal ||
            r.lastOffset != static_cast<int64_t>(SessionSignalKind::Expire)) return;
        expired.push_back({r.sessionId, r.baseOffset});
        for (int64_t sid : st.sids)                         // nobody held a larger share at that moment
            if (sid != r.sessionId && f.worker->directAcksInFlightOfForTest(sid) > r.baseOffset) largest = false;
        maxClosesInFlight = std::max(maxClosesInFlight, ++closesInFlight);
        WorkerOp c; c.type = WorkerOp::Type::SessionClose; c.sessionId = r.sessionId;   // the runtime's close
        ASSERT_TRUE(f.worker->tryEnqueueOp(std::move(c)));
    };
    for (int i = 0; i < 4; ++i) f.step();
    st.add(f, 12, 100, /*connected=*/true);  // connected, then stopped reading (review LOW-2: only those are blamed)
    f.addSecondClient(6);
    for (int i = 0; i < 8; ++i) f.step();
    ASSERT_GT(st.burst(4'000, 10'000'000), 40'000);  // > each connected ack window: they stall
    // Their ack publications connect, deliver a window each, then back-pressure: settle until the share is full of
    // held acks.
    // (pending == 400 for 32 passes in a row: not just publications still connecting).
    for (int i = 0, stable = 0; i < 2'000 && stable < 32; ++i) {
        f.step();
        stable = (f.worker->directAcksInFlightForTest() == 400 && f.worker->sessionAcksPendingForTest() == 400)
                     ? stable + 1 : 0;
    }
    ASSERT_EQ(f.worker->directAcksInFlightForTest(), 400u) << "precondition: the stalled sessions filled the share";
    ASSERT_EQ(f.worker->sessionAcksPendingForTest(), 400u) << "... with acks their publications refuse";

    auto fr6 = Fixture::produceFrame(6, 6'000'001, 0);
    ASSERT_TRUE(Fixture::offerOnce(*f.pub2, fr6));
    uint64_t closesSeen = f.worker->sessionClosesSeenForTest();
    for (int i = 0; i < 5'000 && !f.acks.count(6'000'001); ++i) {
        f.step();
        if (f.worker->sessionClosesSeenForTest() != closesSeen) { closesSeen = f.worker->sessionClosesSeenForTest(); --closesInFlight; }
        ASSERT_LE(f.worker->directAcksInFlightForTest(), 400u) << "the cap is never exceeded";
    }
    ASSERT_EQ(f.acks.count(6'000'001), 1u) << "a reading direct session is served once a stalled one expires";
    ASSERT_FALSE(expired.empty());
    EXPECT_LE(expired.size(), st.sids.size()) << "each expiry frees one stalled session's share";
    for (auto [sid, share] : expired) {
        EXPECT_NE(sid, 6) << "a session that reads its acks is never expired";
        EXPECT_GE(sid, 100) << "only the stalled (held) sessions";
        EXPECT_GT(share, 0);
    }
    EXPECT_GE(expired.front().second, 64) << "the first expiry hits a heavy session (its unread acks beyond its ack window)";
    EXPECT_TRUE(largest) << "every expiry hit the largest held share";
    EXPECT_EQ(maxClosesInFlight, 1) << "one expiry in flight until the runtime's close arrives";
    EXPECT_EQ(f.worker->directExpiriesForTest(), expired.size());
    EXPECT_EQ(f.worker->ackRoomOpPausesForTest(), 0u);
    EXPECT_EQ(f.worker->directUnderflowsForTest(), 0u);
}

// D8d: the direct budget is returned on every exit — claim, refusal answer, the
// runtime's close — on the real driver: after stalled and reading direct
// sessions (acks=Leader and acks=all) drain or close, the worker's direct count
// is back to zero.
TEST(ShardWorkerAckRegression, DirectBudgetReturnsToZeroAfterClaimsRefusalsAndCloseRegression) {
    Fixture f({1}, 1);
    f.runtimeDrainsReturns = true;
    for (int i = 0; i < 4; ++i) f.step();
    StalledDirectClients st;
    st.add(f, 2, 100);
    for (int i = 0; i < 8; ++i) f.step();
    for (int64_t c = 0; c < 50; ++c) { f.produce(1'000 + c, 0); f.produce(2'000 + c, 1); }   // acks=Leader + acks=all
    auto bad = Fixture::produceFrame(5, 3'000, 0, /*topicId=*/77);                          // unowned → NotLeader
    ASSERT_TRUE(Fixture::offerOnce(*f.producePub, bad));
    ASSERT_GT(st.burst(300, 20'000'000), 300);
    for (int i = 0; i < 2'000 && !(allAcked(f, 1'000, 1'050) && allAcked(f, 2'000, 2'050) && f.acks.count(3'000)); ++i)
        f.step();
    ASSERT_TRUE(allAcked(f, 1'000, 1'050) && allAcked(f, 2'000, 2'050));
    ASSERT_EQ(f.acks.count(3'000), 1u);
    EXPECT_EQ(f.acks[3'000].errorCode, static_cast<int16_t>(pc::ErrorCode::NotLeader));
    for (int i = 0; i < 4; ++i) f.step();
    EXPECT_EQ(f.worker->directAcksInFlightOfForTest(5), 0u) << "claimed answers (OK and refusal) returned";
    EXPECT_GT(f.worker->directAcksInFlightForTest(), 0u) << "precondition: the stalled sessions hold some";
    for (int64_t sid : st.sids) {
        WorkerOp c; c.type = WorkerOp::Type::SessionClose; c.sessionId = sid;
        ASSERT_TRUE(f.worker->tryEnqueueOp(std::move(c)));
    }
    for (int i = 0; i < 8; ++i) f.step();
    EXPECT_EQ(f.worker->directAcksInFlightForTest(), 0u) << "closed sessions' shares returned";
    EXPECT_EQ(f.worker->directUnderflowsForTest(), 0u);
}

// ============================================================================
// D8d fix round (review MEDIUM-1 / MEDIUM-2 / LOW-1 / LOW-2 / LOW-3).
// ============================================================================

// MEDIUM-1 RED: at the 4096 fragment limit a FEW stalled direct sessions park the
// share in its congested band (2 x 4096 + 256 = 8448: three sessions) without
// ever filling it — a pipelined producer (acks=all parked on the HW) was then
// capped at 64 in flight until the held reaper (~10 min).  Band deferrals of a healthy
// session now count as deferred passes too: the held session with the largest
// share is expired, and the producer gets its pipeline back.
TEST(ShardWorkerAckRegression, StalledSessionsParkingTheBandAreExpiredSoAPipelineIsNotCappedRegression) {
    // ISR {1,2}, minIsr 1: no follower fetches, so session 5's acks=all wait (parked) — a deep pipeline.
    Fixture f({1, 2}, 1, {}, 64LL * 1024 * 1024, 0, Worker::kReturnByteCapacity);
    f.runtimeDrainsReturns = true;
    f.worker->setDesiredFragmentLimit(Worker::kMaxFragmentLimit);
    std::vector<int64_t> expired;
    f.onReturn = [&](const WorkerReturn& r) {
        if (r.type != WorkerReturn::Type::SessionSignal ||
            r.lastOffset != static_cast<int64_t>(SessionSignalKind::Expire)) return;
        expired.push_back(r.sessionId);
        WorkerOp c; c.type = WorkerOp::Type::SessionClose; c.sessionId = r.sessionId;   // the runtime's close
        ASSERT_TRUE(f.worker->tryEnqueueOp(std::move(c)));
    };
    for (int i = 0; i < 4; ++i) f.step();
    // Two dead clients (ack streams never connected: D8b Lost, reaped on their lease) take 2 x 4096, then two
    // clients that connected and stopped reading take the band's start (256) and one light share (<= 64).
    StalledDirectClients dead, stalled;
    dead.add(f, 2, 100);
    stalled.add(f, 2, 200, /*connected=*/true, /*streamBase=*/9'100);
    for (int i = 0; i < 8; ++i) f.step();
    ASSERT_GT(dead.burst(6'000, 10'000'000), 8'192);
    for (int i = 0; i < 32; ++i) f.step();
    ASSERT_GT(stalled.burst(6'000, 20'000'000), 2'000);
    for (int i = 0; i < 64; ++i) f.step();
    ASSERT_GE(f.worker->directAcksInFlightForTest(), Worker::kDirectAckBudget - Worker::kDirectAckBudget / 4)
        << "precondition: the stalled sessions reached the congested band";
    ASSERT_LT(f.worker->directAcksInFlightForTest(), Worker::kDirectAckBudget) << "... without filling the share";

    for (int64_t c = 0; c < 200; ++c) {                     // session 5: a 200-deep acks=all pipeline
        auto fr = Fixture::produceFrame(5, 5'000'000 + c, 1);
        ASSERT_TRUE(Fixture::offerOnce(*f.producePub, fr));
    }
    for (int i = 0; i < 2'000 && f.worker->directAcksInFlightOfForTest(5) < 200; ++i) f.step();
    EXPECT_EQ(f.worker->directAcksInFlightOfForTest(5), 200u)
        << "a healthy pipelined session must not stay capped at 64 by three stalled sessions";
    ASSERT_FALSE(expired.empty()) << "the band-parking stalled sessions are expired";
    for (int64_t sid : expired) EXPECT_GE(sid, 200) << "only connected-then-stalled (held) sessions are expired";
    EXPECT_EQ(f.worker->directUnderflowsForTest(), 0u);
}

// MEDIUM-2 RED: once a session is closed (expired, reaped, failed), the frames
// still buffered on its image are DROPPED before the append — like the gateway
// path (UnknownSession) — so a non-idempotent client that resends after
// SessionExpired cannot duplicate them.  (Was: the closed session was "unknown"
// to the sink, its whole ABORTed backlog was admitted, appended, and the acks
// dropped as orphans.)
TEST(ShardWorkerAckRegression, ClosedSessionsBufferedDirectFramesAreNeverAppendedRegression) {
    Fixture f({1}, 1, {}, 64LL * 1024 * 1024, 0, Worker::kReturnByteCapacity);
    f.runtimeDrainsReturns = true;
    f.worker->setDirectAckBudgetsForTest(200, 400);
    for (int i = 0; i < 4; ++i) f.step();
    StalledDirectClients st;
    st.add(f, 1, 100, /*connected=*/true);
    for (int i = 0; i < 8; ++i) f.step();
    ASSERT_GT(st.burst(3'000, 10'000'000), 1'000);
    for (int i = 0; i < 64; ++i) f.step();
    const int64_t leoBefore = f.log->logEndOffset();
    ASSERT_GT(f.worker->intakePausedFramesForTest(), 0u) << "precondition: the stalled session's backlog waits";
    WorkerOp c; c.type = WorkerOp::Type::SessionClose; c.sessionId = 100;   // the runtime's close (e.g. Expire)
    ASSERT_TRUE(f.worker->tryEnqueueOp(std::move(c)));
    for (int i = 0; i < 400; ++i) f.step();
    EXPECT_EQ(f.log->logEndOffset(), leoBefore) << "a closed session's buffered frames must never be appended";
    EXPECT_EQ(f.worker->directAcksInFlightForTest(), 0u);
    f.produce(77, 0);                                       // other sessions are unaffected
    EXPECT_TRUE(f.awaitAck(77));
    EXPECT_EQ(f.log->logEndOffset(), leoBefore + 1);
}

// LOW-3: an admitted frame that cannot be decoded produces no answer — the
// worker gives its unit back (Worker::handleWorkerIngressFrame_ → releaseDirect),
// so malformed frames cannot leak the share; and parked direct acks=all failed by
// a demotion (the purge of their waiters) return their units.
TEST(ShardWorkerAckRegression, MalformedFramesAndPurgedParkedDirectAcksReturnTheirBudgetRegression) {
    Fixture f({1, 2}, /*minIsr=*/2);                        // HW cannot advance without node 2
    f.runtimeDrainsReturns = true;
    for (int i = 0; i < 4; ++i) f.step();
    auto good = Fixture::produceFrame(5, 1, 0);
    std::vector<uint8_t> bad(good.begin(), good.begin() + 24);   // session id readable, body truncated
    for (int k = 0; k < 50; ++k) ASSERT_TRUE(Fixture::offerOnce(*f.producePub, bad));
    for (int i = 0; i < 20; ++i) f.step();
    EXPECT_EQ(f.worker->directAcksInFlightForTest(), 0u) << "undecodable frames return their units";
    for (int64_t c = 0; c < 20; ++c) f.produce(100 + c, 1);   // parked on the HW
    for (int i = 0; i < 50; ++i) f.step();
    ASSERT_EQ(f.worker->directAcksInFlightOfForTest(5), 20u) << "precondition: 20 parked direct acks=all";
    f.replica->becomeFollower(/*leaderEpoch=*/2, /*leaderId=*/2);   // its waiters are failed (purged)
    for (int64_t c = 0; c < 20; ++c) ASSERT_TRUE(f.awaitAck(100 + c));
    for (int i = 0; i < 4; ++i) f.step();
    EXPECT_EQ(f.worker->directAcksInFlightForTest(), 0u) << "purged parked answers return their units";
    EXPECT_EQ(f.worker->directUnderflowsForTest(), 0u);
}

// Coverage of the Worker's rescan request on a successful bind (commit 92cf150f).  Session A is bound on ingress image 1.
// Image 1 goes away and image 2 (session B's) arrives while the worker does NOT run a duty pass, so the subscription's
// image count is the SAME (1 -> 1) at the worker's next drain: drainUnavailable sees no change.  Only B's WorkerBind,
// handled on the first poll after that, can reveal A's lost image - handleWorkerBind_ requests the rescan on Bound.
// The invoker driver + client conductor are pumped by the test only, so the swap is atomic w.r.t. the drain.
TEST(ShardWorkerAckRegression, RebindAfterMaskedImageLossUnbindsTheDeadImageRegression) {
    Fixture f({1}, 1);
    f.worker->setResponseControl("127.0.0.1:" + std::to_string(rune::aeronutil::testing::allocFreeUdpPort()));
    const std::array<uint8_t, 16> tokA = [] { std::array<uint8_t, 16> t{}; t.fill(0xA1); return t; }();
    const std::array<uint8_t, 16> tokB = [] { std::array<uint8_t, 16> t{}; t.fill(0xB2); return t; }();
    auto announceResponse = [&](int64_t sid, const std::array<uint8_t, 16>& tok) {
        WorkerOp a; a.type = WorkerOp::Type::SessionAnnounce; a.grantAll = true; a.sessionId = sid;
        a.egressStreamId = kEgress; a.responseBind = true; a.bindToken = tok;
        ASSERT_TRUE(f.worker->tryEnqueueOp(std::move(a)));
    };
    auto bindFrame = [](int64_t sid, int64_t corr, const std::array<uint8_t, 16>& tok) {
        pc::WorkerBind b; b.sessionId = sid; b.correlationId = corr; b.token = tok;
        std::vector<uint8_t> frame(pc::WorkerBind::ENCODED_SIZE);
        frame.resize(b.encode(frame.data()));
        return frame;
    };
    announceResponse(11, tokA);
    announceResponse(12, tokB);
    for (int i = 0; i < 20; ++i) f.step();

    // A binds on image 1 (the fixture's first publication).
    const int32_t imageA = f.producePub->sessionId();
    auto bindA = bindFrame(11, 2, tokA);
    ASSERT_TRUE(Fixture::offerOnce(*f.producePub, bindA));
    for (int i = 0; i < kCap && f.worker->workerBindsAcceptedForTest() < 1; ++i) f.step();
    ASSERT_EQ(f.worker->workerBindsAcceptedForTest(), 1u) << "session A never bound";
    for (int i = 0; i < 50; ++i) f.step();                 // several drains: the worker has settled its image count (1)
    ASSERT_TRUE(f.worker->ingressImageAliveForTest(imageA));
    ASSERT_EQ(f.worker->imageUnbindsForTest(), 0u);

    // The swap, with NO worker duty pass in between: image 1 goes away, image 2 arrives.
    f.producePub.reset();
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    for (int i = 0; i < 100'000'000 && f.worker->ingressImageAliveForTest(imageA) && std::chrono::steady_clock::now() < deadline; ++i)
        f.pump();   // real driver time (the publication linger), bounded by the wall deadline
    ASSERT_FALSE(f.worker->ingressImageAliveForTest(imageA)) << "image A never went away";
    f.connectSecondClient();
    ASSERT_NE(f.pub2, nullptr);
    const int32_t imageB = f.pub2->sessionId();
    for (int i = 0; i < 100'000'000 && !f.worker->ingressImageAliveForTest(imageB) && std::chrono::steady_clock::now() < deadline; ++i)
        f.pump();
    ASSERT_TRUE(f.worker->ingressImageAliveForTest(imageB));
    // The subscription holds exactly one image before and after the swap (A, then B): the count did not change.
    EXPECT_FALSE(f.worker->ingressImageAliveForTest(imageA));
    EXPECT_NE(imageA, imageB);
    auto bindB = bindFrame(12, 2, tokB);
    ASSERT_TRUE(Fixture::offerOnce(*f.pub2, bindB));

    // The worker resumes: B binds; A's dead image is found only through the rescan that bind requested.
    for (int i = 0; i < kCap && f.worker->workerBindsAcceptedForTest() < 2; ++i) f.step();
    ASSERT_EQ(f.worker->workerBindsAcceptedForTest(), 2u) << "session B never bound";
    EXPECT_EQ(f.worker->imageUnbindsForTest(), 0u) << "precondition: the masked loss was NOT noticed by the count compare";
    for (int i = 0; i < 200 && f.worker->imageUnbindsForTest() < 1; ++i) f.step();
    EXPECT_GE(f.worker->imageUnbindsForTest(), 1u) << "A's dead image was never unbound: the rescan on a successful bind is missing";
}
