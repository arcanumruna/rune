#pragma once
// CheckedWritesAckRig.h - checked writes Part 03: one Worker over a real Aeron IPC driver, with or without a worker-side ack
// sink (legacy mode), a typed topic, and the helpers the hook and View-ack tests share.  Topic 1, partition 0, node 1 is the leader.
#include <rune/aeronutil/testing/FreePort.h>
#include <rune/codec/Serialize.h>
#include <rune/node/AdminChannels.h>
#include <rune/node/EmbeddedDriver.h>
#include <rune/node/WorkerViewSet.h>
#include <rune/transaction/ControlMarker.h>
#include <rune/view/PlanCompiler.h>
#include <rune/storage/InMemoryLog.h>
#include <rune/node/Worker.h>
#include <rune/node/WorkerIngress.h>
#include <rune/node/WorkerOp.h>
#include <rune/node/WriteState.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/protocol/client/DataMessages.h>
#include <rune/protocol/client/Protocol.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/storage/Log.h>

#include "CheckedWritesBatches.h"

#include <Aeron.h>
#include <FragmentAssembler.h>
#include <gtest/gtest.h>

#include <atomic>
#include <filesystem>
#include <map>
#include <memory>
#include <string>
#include <unistd.h>
#include <vector>

namespace rune::node::cw {

struct AckRigOptions {
    bool ackSink = true;                              ///< false: legacy mode, answers ride the return ring
    std::vector<rune::replication::NodeId> replicas{1};
    int  minIsr = 1;
    bool transient = false;                           ///< the partition is an in-memory ring
    uint8_t durabilityTier = 1;                       ///< 2 = Strict
    int64_t ackTimeoutNs = 50'000'000;
    int32_t clientVersion = 17;
    std::size_t ringBytes = 1 << 16;                  ///< the worker's op and return rings
};

class AckRig {
public:
    using Options = AckRigOptions;
    static constexpr int32_t kTopicId = 1, kIngress = 7501, kEgress = 7502;
    static constexpr int64_t kSession = 5;
    static constexpr int kCap = 200'000;

    explicit AckRig(Options o = {}) : opts_(std::move(o)) {
        static std::atomic<int> seq{0};
        const std::string tag = std::to_string(::getpid()) + "_" + std::to_string(seq++);
        dir_  = std::filesystem::temp_directory_path() / ("rune_cwrig_log_" + tag);
        adir_ = std::filesystem::temp_directory_path() / ("rune_cwrig_aeron_" + tag);
        std::filesystem::create_directories(dir_);
        worker = std::make_unique<Worker>(0, opts_.ringBytes, opts_.ringBytes, 64, 64);
        if (!opts_.transient) {
            log = std::make_unique<rune::storage::Log>(dir_.string(), 64LL * 1024 * 1024);
            log->recover();
        } else {
            mem_ = std::make_unique<rune::storage::InMemoryLog>(4u << 20, 100'000);
        }
        worker->setAckTimeoutNs(opts_.ackTimeoutNs);
        rune::replication::PartitionConfig c;
        c.topicId = kTopicId; c.selfId = 1; c.minIsr = opts_.minIsr; c.durabilityTier = opts_.durabilityTier;
        c.fetchIntervalNs = 1'000'000'000; c.isrCheckIntervalNs = 1'000'000'000'000LL;
        c.replicaLagMaxMs = 600'000; c.pushOnAppend = false;
        if (opts_.transient)
            replica = std::make_unique<rune::replication::PartitionReplica>(c, rune::replication::Role::Leader, 1, opts_.replicas,
                                                                            opts_.replicas, mem_.get(), &transport_);
        else
            replica = std::make_unique<rune::replication::PartitionReplica>(c, rune::replication::Role::Leader, 1, opts_.replicas,
                                                                            opts_.replicas, log.get(), &transport_);
        EmbeddedDriverConfig dc; dc.dir = adir_.string(); dc.termBufferBytes = 64 * 1024;
        drv_ = std::make_unique<EmbeddedDriver>(dc, DriverMode::Invoker);
        aeron::Context ctx; ctx.aeronDir(adir_.string()); ctx.useConductorAgentInvoker(true);
        client_ = aeron::Aeron::connect(ctx);
        worker->setOwnedDirect(Worker::OwnedSet{replica.get()});
        if (!opts_.ackSink) return;
        ingress_ = std::make_unique<WorkerIngress>(client_, "aeron:ipc", kIngress, drv_->pumpWith(client_.get()));
        worker->setWorkerIngress(ingress_.get(), true);
        worker->setShardMode(true);
        worker->setShardClient(client_.get());
        const int64_t sreg = client_->addSubscription("aeron:ipc", kEgress);
        const int64_t preg = client_->addExclusivePublication("aeron:ipc", kIngress);
        for (int i = 0; i < kCap && !(egress_ && pub_ && pub_->isConnected()); ++i) {
            pump();
            if (!egress_) egress_ = client_->findSubscription(sreg);
            if (!pub_) pub_ = client_->findExclusivePublication(preg);
        }
        WorkerOp a; a.type = WorkerOp::Type::SessionAnnounce; a.grantAll = true; a.sessionId = kSession;
        a.egressStreamId = kEgress; a.egressChannel = "aeron:ipc?term-length=65536"; a.clientVersion = opts_.clientVersion;
        EXPECT_TRUE(worker->tryEnqueueOp(std::move(a)));
    }
    ~AckRig() {
        worker.reset();
        vs.reset();
        pub_.reset(); egress_.reset(); ingress_.reset(); client_.reset(); drv_.reset();
        std::error_code ec;
        std::filesystem::remove_all(dir_, ec);
        std::filesystem::remove_all(adir_, ec);
    }

    void pump() { drv_->doWork(); client_->conductorAgentInvoker().invoke(); }
    void step() {
        pump();
        ++stepNo;
        worker->doWork(now += 10'000);
        if (const int64_t leo = this->leo(); leo > lastLeo_) { lastLeo_ = leo; appendedAtNs = now; }
        if (egress_) egress_->poll(asmb_.handler(), 64);
        worker->drainReturn([this](WorkerReturn r) { returns.push_back(std::move(r)); });
    }
    void steps(int n) { for (int i = 0; i < n; ++i) step(); }

    [[nodiscard]] int64_t leo() const { return replica->logEndOffset(); }

    // ---- replication and leadership ----
    /// The follower (node 2) fetches everything: the HW moves to the log end.
    void followerFetch() {
        rune::replication::FetchRequestMsg fr{epoch_, 0, 2, leo(), 1 << 20, kTopicId};
        (void)replica->onFetchRequest(fr, now);
        steps(2);
    }
    void demote() { replica->becomeFollower(++epoch_, 2); steps(2); }
    void promote() { replica->becomeLeader(++epoch_, opts_.replicas); steps(2); }

    /// A transaction coordinator's COMMIT / ABORT marker for `pid`.
    void marker(int64_t pid, bool commit) {
        WorkerOp op;
        op.type = WorkerOp::Type::AppendControlRecord; op.topicId = kTopicId; op.partition = 0; op.replica = replica.get();
        op.logPtr = log.get(); op.correlationId = 9000 + pid; op.producerId = pid;
        op.targetReplicaId = replica->registryId(); op.incarnation = replica->incarnation();
        op.recordBatchBytes = rune::transaction::ControlMarker::build(
            pid, 0, commit ? rune::transaction::ControlType::Commit : rune::transaction::ControlType::Abort, 0);
        ASSERT_TRUE(worker->tryEnqueueOp(std::move(op)));
        steps(3);
    }

    // ---- views ----
    /// A projection view over the typed topic (`id` -> `id`), installed on the worker.
    void installView(int32_t viewTopicId, const char* name = "v") {
        if (!vs) {
            channel = std::make_unique<ViewSnapshotChannel>();
            vs = std::make_unique<WorkerViewSet>(*channel, (std::filesystem::temp_directory_path() /
                                                            ("rune_cwrig_views_" + std::to_string(::getpid()) + "_" + std::to_string(viewSeq++))).string());
            worker->setViewSet(vs.get());
        }
        viewSource_ = rune::codec::DescriptorBuilder("m", 1).add("id", rune::codec::PrimType::UInt64, 1, /*isKey*/ true)
                          .addFixedStr("name", 16).add("bio", rune::codec::PrimType::String).add("blob", rune::codec::PrimType::Bytes)
                          .add("kind", rune::codec::PrimType::Enum, 1, false, {1, 2, 3}).build();
        rune::view::ViewPlan v;
        v.viewName = name; v.key = "id"; v.sources = {{kTopicId, "id"}};
        v.output = {{"id", rune::codec::PrimType::UInt64, 0, "id", rune::view::MergeRule::Replace}};
        const rune::codec::Descriptor* srcs[] = {&viewSource_};
        auto comp = rune::view::compileView(v, srcs, viewTopicId);
        ASSERT_TRUE(comp.has_value());
        vs->installView(comp->plan, comp->rowLayout, viewSource_, /*persisted*/ false);
        steps(2);
    }
    [[nodiscard]] int64_t foldCursor(int32_t viewTopicId) const { return vs->instanceServeState(viewTopicId, 0).foldCursor; }
    [[nodiscard]] uint64_t foldLeadership(int32_t viewTopicId) const { return vs->instanceServeState(viewTopicId, 0).leadership; }

    // ---- typed topic and feature level ----
    void send(WorkerOp op) { ASSERT_TRUE(worker->tryEnqueueOp(std::move(op))); steps(3); }
    void typed(bool legacyValidate = true) {
        WorkerOp v;
        v.type = WorkerOp::Type::SchemaUpdate; v.topicId = kTopicId; v.clientVersion = 1;
        v.schemaUpdateBlob = rune::codec::serialize(matrixDescriptor());
        send(std::move(v));
        TopicWriteSpecWire w;
        w.topicId = kTopicId; w.keyFieldId = kIdTag; w.keyType = rune::codec::PrimType::UInt64; w.keyWidth = 8; w.legacyValidate = legacyValidate;
        WorkerOp s;
        s.type = WorkerOp::Type::WriteSpecUpdate; s.topicId = kTopicId;
        s.schemaUpdateBlob.resize(kTopicWriteSpecWireLen);
        ASSERT_EQ(encodeTopicWriteSpec(w, s.schemaUpdateBlob), kTopicWriteSpecWireLen);
        send(std::move(s));
    }
    void setLevel(int16_t level) {
        WorkerOp op;
        op.type = WorkerOp::Type::ClusterFeatureLevel; op.nextOffset = level;
        send(std::move(op));
    }

    // ---- produce ----
    struct Write {
        int64_t  corr = 0;
        uint8_t  acks = 0;
        int64_t  pid = -1;
        int32_t  seq = -1;
        bool     txn = false;
        std::vector<TypedRecord> records;           ///< default: one valid typed record with id = corr
        std::vector<uint8_t>     attributes;        ///< per record, default 0
    };
    static std::vector<uint8_t> batchFor(const Write& w) {
        rune::protocol::RecordBatchBuilder b(0, 1'000'000);
        if (w.pid >= 0) { b.setProducerId(w.pid); b.setProducerEpoch(0); b.setBaseSequence(w.seq); }
        b.setTransactional(w.txn);
        std::vector<TypedRecord> recs = w.records;
        if (recs.empty()) recs.push_back(validRecord(static_cast<uint64_t>(w.corr)));
        for (std::size_t i = 0; i < recs.size(); ++i)
            b.append(recs[i].key, recs[i].value, 1'000'000, i < w.attributes.size() ? w.attributes[i] : uint8_t{0});
        return b.build();
    }
    /// Through the worker's ingress publication (ack sink) or the op ring (legacy).
    void produce(const Write& w) {
        const auto batch = batchFor(w);
        if (!opts_.ackSink) { ASSERT_TRUE(routed(w, batch)); return; }
        rune::protocol::client::Produce m;
        m.sessionId = kSession; m.correlationId = w.corr; m.topicId = kTopicId; m.partition = 0; m.partitionKnown = 1;
        m.acks = w.acks; m.producerId = w.pid; m.producerEpoch = 0; m.baseSequence = w.seq;
        m.recordBatch = std::span<const uint8_t>(batch);
        std::vector<uint8_t> f(m.encodedSize()); m.encode(f.data());
        aeron::concurrent::AtomicBuffer ab(f.data(), static_cast<aeron::util::index_t>(f.size()));
        for (int i = 0; i < kCap && pub_->offer(ab, 0, static_cast<aeron::util::index_t>(f.size())) <= 0; ++i) pump();
    }
    /// A gateway-routed produce on the op ring (what the runtime enqueues for a client that ignores ingressEndpoint).
    bool routed(const Write& w, const std::vector<uint8_t>& batch) {
        WorkerOp op; op.type = WorkerOp::Type::Produce; op.topicId = kTopicId; op.partition = 0;
        op.replica = replica.get(); op.logPtr = log.get();
        op.sessionId = kSession; op.correlationId = w.corr; op.acks = w.acks;
        op.producerId = w.pid; op.producerEpoch = 0; op.baseSequence = w.seq;
        op.lastOffsetDelta = 0; op.recordCount = 1; op.durabilityTier = 1;
        op.deferAck = (w.acks & rune::protocol::client::kAcksWireAll) != 0 && opts_.ackSink;
        op.recordBatchView = std::span<const uint8_t>(batch);
        return worker->tryEnqueueOp(std::move(op));
    }
    /// A gateway-routed produce (WorkerOp::Produce) of the default batch.
    void send_routed(const Write& w) { const auto batch = batchFor(w); ASSERT_TRUE(routed(w, batch)); }
    /// Steps until the answer to `corr` arrived; the answer's error code, or -1.
    int16_t answer(int64_t corr) {
        for (int i = 0; i < kCap; ++i) {
            if (const auto it = acks.find(corr); it != acks.end()) return it->second.errorCode;
            for (const auto& r : returns)
                if (r.type == WorkerReturn::Type::ProduceResult && !r.deferAck && r.correlationId == corr) return r.errorCode;
            step();
        }
        return -1;
    }

    std::unique_ptr<Worker>                     worker;
    std::unique_ptr<rune::storage::Log>         log;
    std::unique_ptr<rune::replication::PartitionReplica> replica;
    std::map<int64_t, rune::protocol::client::ProduceResponse> acks;
    std::map<int64_t, std::size_t>              ackLen;       ///< raw frame length per ack
    std::map<int64_t, int64_t>                  ackStep;      ///< the step the ack first appeared at
    std::map<int64_t, int64_t>                  ackNow;       ///< the clock at that step
    std::map<int64_t, int>                      ackCount;     ///< frames seen per correlation id
    std::unique_ptr<ViewSnapshotChannel>        channel;
    std::unique_ptr<WorkerViewSet>              vs;
    int64_t                                     stepNo = 0;
    int64_t                                     appendedAtNs = 0;
    std::vector<WorkerReturn>                   returns;
    int64_t                                     now = 1'000;

private:
    class NullTransport final : public rune::replication::PartitionTransport {
    public:
        bool sendFetchRequest(const rune::replication::FetchRequestMsg&) override { return true; }
        bool sendFetchResponse(rune::replication::NodeId, const rune::replication::FetchResponseMsg&) override { return true; }
    };
    Options                                   opts_;
    std::unique_ptr<rune::storage::InMemoryLog> mem_;
    rune::codec::Descriptor                   viewSource_;
    rune::replication::Epoch                  epoch_ = 1;
    int64_t                                   lastLeo_ = 0;
    inline static std::atomic<int>            viewSeq{0};
    std::filesystem::path                     dir_, adir_;
    NullTransport                             transport_;
    std::unique_ptr<EmbeddedDriver>           drv_;
    std::shared_ptr<aeron::Aeron>             client_;
    std::unique_ptr<WorkerIngress>            ingress_;
    std::shared_ptr<aeron::ExclusivePublication> pub_;
    std::shared_ptr<aeron::Subscription>      egress_;
    aeron::FragmentAssembler asmb_{[this](aeron::concurrent::AtomicBuffer& b, aeron::util::index_t o, aeron::util::index_t l, aeron::Header&) {
        if (auto r = rune::protocol::client::ProduceResponse::decode(reinterpret_cast<const uint8_t*>(b.buffer() + o), static_cast<std::size_t>(l))) {
            if (ackCount[r->correlationId]++ == 0) { ackLen[r->correlationId] = static_cast<std::size_t>(l); ackStep[r->correlationId] = stepNo; ackNow[r->correlationId] = now; }
            acks[r->correlationId] = *r;
        }
    }};
};

}  // namespace rune::node::cw
