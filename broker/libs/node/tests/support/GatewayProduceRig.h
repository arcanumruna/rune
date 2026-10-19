#pragma once
// GatewayProduceRig.h — catalog history Task 23 (Task 8 ruling 1): one thread drives a REAL
// ClientGateway (makeForTest) + TopicRegistry + ReplicaRegistry + a 1-worker WorkerPool, exactly as
// F15WorkerPsmBench's GatewayRig: produce = applyProduceForTest -> Worker::doWork ->
// drainWorkerReturnsForTest (the gateway's zero-copy drain).  PORTABLE to the parity base 7d86db94:
// every catalog-only API is under RUNE_T23_HAS_CATALOG.  Single-threaded: plain members.
#if __has_include(<rune/node/CatalogApplier.h>)
#define RUNE_T23_HAS_CATALOG 1
#else
#define RUNE_T23_HAS_CATALOG 0
#endif

#include <rune/gateway/ClientGateway.h>
#include <rune/gateway/TopicRegistry.h>
#include <rune/metadata/Gpid.h>
#include <rune/metadata/RoutingImage.h>
#include <rune/metadata/ShardRouter.h>
#include <rune/node/ReplicaRegistry.h>
#include <rune/node/Worker.h>
#include <rune/node/WorkerPool.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/protocol/client/ControlMessages.h>
#include <rune/protocol/client/DataMessages.h>
#include <rune/protocol/client/MessageHeader.h>
#include <rune/protocol/client/Protocol.h>
#include <rune/replication/PartitionReplica.h>

#include <filesystem>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

#include <unistd.h>

namespace rune::node::test {

enum class ProduceTopicMode : uint8_t { StaticRouted = 0, CatalogPlaced = 1, CatalogUnplaced = 2 };

class GatewayProduceRig {
public:
    static constexpr int32_t kSelf = 1, kTopic = 0, kOtherTopic = 5;
    explicit GatewayProduceRig(ProduceTopicMode mode)
        : dir_(makeDir_()),
          gw(rune::gateway::ClientGateway::makeForTest(
              [this](const uint8_t* b, std::size_t n) -> int64_t { onFrame_(b, n); return 1; }, 1)) {
        namespace pc = rune::protocol::client;
        {
            rune::protocol::RecordBatchBuilder b(0, 1000);
            b.append("k", std::string(100, 'v'), 1000);
            batch_ = b.build();
        }
        rune::node::TopicSpec s;
        s.name = "t";
        s.partitionCount = 1;
        if (mode == ProduceTopicMode::StaticRouted) {
            s.topicId = kTopic;
            registry = std::make_unique<rune::gateway::TopicRegistry>(std::vector<rune::node::TopicSpec>{s}, dir_);
        } else {
#if RUNE_T23_HAS_CATALOG
            registry = std::make_unique<rune::gateway::TopicRegistry>(std::vector<rune::node::TopicSpec>{}, dir_);
            if (registry->createTopicAt(kTopic, s) != kTopic || !registry->placementGated(kTopic))
                throw std::logic_error("GatewayProduceRig: createTopicAt did not give a placement-gated topic");
#else
            throw std::logic_error("catalog modes need the catalog branch");
#endif
        }
        if (mode != ProduceTopicMode::CatalogUnplaced) {
            rune::replication::PartitionConfig c;
            c.topicId = 0; c.partitionId = 0; c.selfId = 1; c.minIsr = 1; c.durabilityTier = 1;
            c.fetchIntervalNs = 1'000'000'000LL; c.isrCheckIntervalNs = 1'000'000'000'000LL;
            c.replicaLagMaxMs = 100'000'000; c.pushOnAppend = false;
            auto rep = std::make_unique<rune::replication::PartitionReplica>(
                c, rune::replication::Role::Leader, 1, std::vector<rune::replication::NodeId>{1},
                std::vector<rune::replication::NodeId>{1}, registry->log(kTopic, 0), &transport);
            replica = rep.get();
            reg.add(kTopic, 0, std::move(rep));
        }
        pool.assignFromRegistry(reg);
        pool.setRegistry(&reg);
        pool.setRunnersStartedForTest(true);
        gw.setTopicRegistry(registry.get());
        gw.setReplicaRegistry(&reg);
        gw.setWorkerPool(&pool);
        gw.setNodeIdForTest(kSelf);
        gw.setOnEgressFrame([this](int64_t, const uint8_t* b, std::size_t n) { onFrame_(b, n); });
        std::vector<rune::metadata::RoutingImage::BuildEntry> img;
        if (mode != ProduceTopicMode::CatalogUnplaced)
            img.push_back({rune::metadata::gpidOf(kTopic, 0), kSelf, 1});
        img.push_back({rune::metadata::gpidOf(kOtherTopic, 0), kSelf, 1});   // the image is never empty
#if RUNE_T23_HAS_CATALOG
        gw.setRoutingImageSnapshot(rune::metadata::RoutingImage::build(img, rune::metadata::RoutingImage::Source::Committed));
#else
        gw.setRoutingImageSnapshot(rune::metadata::RoutingImage::build(img));
#endif
        gw.setCommittedMetadataReady(true);
        sid = gw.injectTestSession();
        pass();
    }
    ~GatewayProduceRig() { std::error_code ec; std::filesystem::remove_all(dir_, ec); }
    GatewayProduceRig(const GatewayProduceRig&) = delete;
    GatewayProduceRig& operator=(const GatewayProduceRig&) = delete;

    std::vector<uint8_t> produceFrame(int64_t corr) const {   // acks 0 (leader), pid -1, partition 0 known
        rune::protocol::client::Produce m;
        m.sessionId = sid; m.correlationId = corr; m.topicId = kTopic; m.partition = 0; m.partitionKnown = 1;
        m.acks = 0; m.producerId = -1; m.producerEpoch = 0; m.baseSequence = -1;
        m.recordBatch = std::span<const uint8_t>(batch_);
        std::vector<uint8_t> f(m.encodedSize());
        m.encode(f.data());
        return f;
    }
    std::vector<uint8_t> subscribeFrame(int64_t corr) const {   // topic 0 partition 0 known, fromOffset 0, isolation 0
        rune::protocol::client::Subscribe m;
        m.sessionId = sid; m.correlationId = corr; m.topicId = kTopic; m.partition = 0; m.partitionKnown = 1;
        m.fromOffset = 0; m.isolation = 0;
        std::vector<uint8_t> f(512);
        f.resize(m.encode(f.data()));
        return f;
    }
    std::vector<uint8_t> unsubscribeFrame(int64_t corr, int64_t subscriptionId) const {
        rune::protocol::client::Unsubscribe m;
        m.sessionId = sid; m.correlationId = corr; m.subscriptionId = subscriptionId;
        std::vector<uint8_t> f(512);
        f.resize(m.encode(f.data()));
        return f;
    }
    void produce(const std::vector<uint8_t>& f)     { gw.applyProduceForTest(f.data(), f.size()); }
    void subscribe(const std::vector<uint8_t>& f)   { gw.applySubscribeForTest(f.data(), f.size()); }
    void unsubscribe(const std::vector<uint8_t>& f) { gw.applyUnsubscribeForTest(f.data(), f.size()); }
    void pass() { now += 1'000'000; (void)pool.worker(0).doWork(now); gw.drainWorkerReturnsForTest(now); }
    /// One produce through the gateway, then passes until IT is answered (<= 64): true when answered OK.
    bool produceAndAwait(const std::vector<uint8_t>& f) {
        const uint64_t before = produceOk + produceErr + produceNotLeader;
        produce(f);
        for (int k = 0; k < 64 && produceOk + produceErr + produceNotLeader == before; ++k) pass();
        return produceOk + produceErr + produceNotLeader == before + 1 && produceErr == 0 && produceNotLeader == 0;
    }
    // Answer counters (plain: single thread).  The egress decodes headers / PODs only (zero-alloc).
    uint64_t produceOk = 0, produceNotLeader = 0, produceErr = 0;
    uint64_t subscribeOk = 0, errNotLeader = 0, errOther = 0, records = 0, otherFrames = 0;
    int64_t  lastSubscriptionId = 0;
    // Members — declaration ORDER matters (pool before gw).
    std::string                                   dir_;
    std::unique_ptr<rune::gateway::TopicRegistry> registry;
    rune::replication::NullPartitionTransport     transport;
    rune::node::ReplicaRegistry                   reg;
    rune::node::WorkerPool                        pool{1, rune::metadata::ShardRouter(1), 1 << 22, 1 << 25, 64, 64};
    rune::gateway::ClientGateway                  gw;
    rune::replication::PartitionReplica*          replica = nullptr;   // null in CatalogUnplaced
    int64_t                                       sid = 0;
    int64_t                                       now = 1'000'000'000;
    std::vector<uint8_t>                          batch_;

private:
    static std::string makeDir_() {
        static int seq = 0;
        const auto p = std::filesystem::temp_directory_path() /
                       ("rune_t23gw_" + std::to_string(::getpid()) + "_" + std::to_string(++seq));
        std::filesystem::create_directories(p);
        return p.string();
    }
    void onFrame_(const uint8_t* b, std::size_t n) {
        namespace pc = rune::protocol::client;
        const auto h = pc::MessageHeader::decode(b, n);
        if (!h) { ++otherFrames; return; }
        switch (h->templateId) {
            case pc::ProduceResponse::TEMPLATE_ID:
                if (const auto r = pc::ProduceResponse::decode(b, n)) {
                    if (r->errorCode == 0) ++produceOk;
                    else if (r->errorCode == static_cast<int16_t>(pc::ErrorCode::NotLeader)) ++produceNotLeader;
                    else ++produceErr;
                } else ++produceErr;
                break;
            case pc::SubscribeResponse::TEMPLATE_ID:
                if (const auto r = pc::SubscribeResponse::decode(b, n); r && r->errorCode == 0) {
                    ++subscribeOk; lastSubscriptionId = r->subscriptionId;
                } else ++errOther;
                break;
            case pc::ErrorResponse::TEMPLATE_ID:
                if (const auto r = pc::ErrorResponse::decode(b, n);
                    r && r->code == static_cast<int16_t>(pc::ErrorCode::NotLeader)) ++errNotLeader;
                else ++errOther;
                break;
            case pc::Records::TEMPLATE_ID: ++records; break;
            default: ++otherFrames; break;
        }
    }
};

}  // namespace rune::node::test
