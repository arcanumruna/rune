#pragma once
// ViewReadRig.h — catalog history Task 23 (Task 8 N4 / Task 22 N-M2): one thread drives a REAL
// ClientGateway + WorkerPool (1 worker, owning `fillers` + the source partition LAST) + a
// WorkerViewSet holding a folded view.  GetViewRow = applyGetViewRowForTest -> Worker::doWork ->
// drainWorkerReturnsForTest.  Mode::SourceLedElsewhere routes the view's source to another node:
// the gateway answers a synchronous Redirect (ErrorResponse 3 carrying the endpoint).
// Branch-only header (the base has no catalog view routing).  Single-threaded: plain members.
#include <rune/gateway/ClientGateway.h>
#include <rune/gateway/TopicRegistry.h>
#include <rune/metadata/Gpid.h>
#include <rune/metadata/RoutingImage.h>
#include <rune/metadata/ShardRouter.h>
#include <rune/node/ReplicaRegistry.h>
#include <rune/node/ViewSnapshotChannel.h>
#include <rune/node/Worker.h>
#include <rune/node/WorkerPool.h>
#include <rune/node/WorkerViewSet.h>
#include <rune/codec/Descriptor.h>
#include <rune/codec/Serialize.h>
#include <rune/protocol/client/ControlMessages.h>
#include <rune/protocol/client/DataMessages.h>
#include <rune/protocol/client/MessageHeader.h>
#include <rune/protocol/client/Protocol.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/storage/InMemoryLog.h>
#include <rune/storage/Log.h>
#include <rune/view/PlanCompiler.h>
#include <rune/view/ViewPlan.h>
#include <rune/view/ViewRow.h>
#include <rune/view/ViewTypes.h>

#include <filesystem>
#include <memory>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#include <unistd.h>

namespace rune::node::test {

namespace view_read_rig_detail {

namespace rc = rune::codec;
namespace rv = rune::view;

constexpr int32_t kSrcTopicId = 5;

// v1 "character" source: {characterId: Int64 key (tag 0), hp: Int32 (tag 1)}.
inline rc::Descriptor charSourceV1() {
    return rc::DescriptorBuilder("character", 1)
        .add("characterId", rc::PrimType::Int64, 1, /*isKey*/ true)
        .add("hp", rc::PrimType::Int32)
        .build();
}
// char_hp(characterId, hp) <- source topicId 5.
inline rv::ViewPlan hpView(uint8_t persistence = 1) {
    rv::ViewPlan v;
    v.viewName    = "char_hp";
    v.key         = "characterId";
    v.sources     = {{kSrcTopicId, "characterId"}};
    v.output      = {{"characterId", rc::PrimType::Int64, 0, "characterId", rv::MergeRule::Replace},
                     {"hp", rc::PrimType::Int32, 0, "hp", rv::MergeRule::Replace}};
    v.persistence = persistence;
    return v;
}
inline std::vector<uint8_t> keyBytes(int64_t id) {
    std::vector<uint8_t> b(8);
    for (int i = 0; i < 8; ++i) b[static_cast<size_t>(i)] = static_cast<uint8_t>(uint64_t(id) >> (8 * i));
    return b;
}
inline std::string_view asSv(const std::vector<uint8_t>& b) {
    return std::string_view(reinterpret_cast<const char*>(b.data()), b.size());
}
// [blockLength u16][version u16][id i64][hp i32] — a v1 "character" value.
inline std::vector<uint8_t> encodeCharacter(int64_t id, int32_t hp, uint16_t version) {
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

}  // namespace view_read_rig_detail

class GatewayViewReadRig {
public:
    static constexpr int32_t kSelf = 1, kOther = 2, kSrcTopic = 5, kViewTopic = 6;
    static constexpr std::string_view kOtherEndpoint = "aeron:udp?endpoint=10.0.0.2:40123";
    enum class Mode : uint8_t { ServedHere, SourceLedElsewhere };

    GatewayViewReadRig(Mode mode, std::size_t fillers)
        : dir_(makeDir_()),
          srcLog(dir_ + "/src"),
          vsDir_(dir_ + "/views"),
          gw(rune::gateway::ClientGateway::makeForTest(
              [this](const uint8_t* b, std::size_t n) -> int64_t { onFrame_(b, n); return 1; }, 1)) {
        namespace vd = view_read_rig_detail;
        namespace pc = rune::protocol::client;
        namespace rv = rune::view;
        using rune::replication::PartitionConfig;
        using rune::replication::PartitionReplica;
        using rune::replication::Role;
        // 2. the source replica (a disk Log, leader of a one-member ISR)
        {
            PartitionConfig cfg{};
            cfg.topicId = kSrcTopic; cfg.partitionId = 0; cfg.selfId = kSelf; cfg.minIsr = 1;
            cfg.replicaLagMaxMs = 10'000; cfg.replicaLagMaxOffset = 10'000;
            cfg.fetchIntervalNs = 1'000'000LL; cfg.isrCheckIntervalNs = 1'000'000LL;
            cfg.maxFetchBytes = 1 << 20; cfg.pushOnAppend = false;
            src = std::make_unique<PartitionReplica>(cfg, Role::Follower, /*epoch=*/1,
                                                     std::vector<rune::replication::NodeId>{kSelf},
                                                     std::vector<rune::replication::NodeId>{kSelf},
                                                     &srcLog, &transport);
            src->becomeLeader(1, {kSelf});
        }
        // 3. rows 1..8: hp = 10 * id
        for (int64_t id = 1; id <= 8; ++id) {
            const auto k = vd::keyBytes(id);
            const auto v = vd::encodeCharacter(id, static_cast<int32_t>(10 * id), 1);
            if (src->appendKeyed(std::span<const uint8_t>(k.data(), k.size()),
                                 std::span<const uint8_t>(v.data(), v.size()), false) < 0)
                throw std::logic_error("GatewayViewReadRig: source append refused");
        }
        (void)src->doWork(now);
        // 4. fillers
        for (std::size_t i = 0; i < fillers; ++i) {
            fillerLogs.push_back(std::make_unique<rune::storage::InMemoryLog>(1 << 12, 16));
            PartitionConfig cfg{};
            cfg.topicId = 100; cfg.partitionId = static_cast<int32_t>(i); cfg.selfId = 1; cfg.minIsr = 1;
            fillerReps.push_back(std::make_unique<PartitionReplica>(
                cfg, Role::Leader, 1, std::vector<rune::replication::NodeId>{1},
                std::vector<rune::replication::NodeId>{1}, fillerLogs.back().get(), &fillerTransport));
        }
        // 5. the view
        srcDesc = vd::charSourceV1();
        {
            const rune::codec::Descriptor* srcs[] = {&srcDesc};
            auto comp = rv::compileView(vd::hpView(1), srcs, kViewTopic).value();
            vs = std::make_unique<rune::node::WorkerViewSet>(channel, vsDir_);
            vs->installView(comp.plan, comp.rowLayout, srcDesc, /*persisted*/ false);
            layout = comp.rowLayout;
        }
        // 6. the worker: fillers first, the source LAST
        {
            rune::node::Worker::OwnedSet owned;
            for (auto& f : fillerReps) owned.push_back(f.get());
            owned.push_back(src.get());
            pool.worker(0).setOwnedDirect(owned);
            pool.worker(0).setViewSet(vs.get());
        }
        // 7. the gateway
        registry = std::make_unique<rune::gateway::TopicRegistry>(std::vector<rune::node::TopicSpec>{}, dir_ + "/reg");
        {
            rune::node::TopicSpec vsSpec;
            vsSpec.name = "char_hp";
            vsSpec.partitionCount = 1;
            if (registry->createTopicAt(kViewTopic, vsSpec) != kViewTopic)
                throw std::logic_error("GatewayViewReadRig: createTopicAt(view sink) did not give the view topic id");
        }
        gw.setTopicRegistry(registry.get());
        gw.setWorkerPool(&pool);
        gw.setNodeIdForTest(kSelf);
        gw.noteViewSource(kViewTopic, kSrcTopic);
        {
            std::vector<rune::metadata::RoutingImage::BuildEntry> img;
            img.push_back({rune::metadata::gpidOf(kViewTopic, 0), kSelf, 1});
            img.push_back({rune::metadata::gpidOf(kSrcTopic, 0), mode == Mode::ServedHere ? kSelf : kOther, 1});
            gw.setRoutingImageSnapshot(
                rune::metadata::RoutingImage::build(img, rune::metadata::RoutingImage::Source::Committed));
        }
        gw.setCommittedMetadataReady(true);
        gw.setNodeEndpointMap({{kOther, std::string(kOtherEndpoint)}, {kSelf, ""}});
        gw.setOnEgressFrame([this](int64_t, const uint8_t* b, std::size_t n) { onFrame_(b, n); });
        sid = gw.injectTestSession();
        // 8. ServedHere: warm until key 7 is served
        if (mode == Mode::ServedHere) {
            uint16_t lastStatus = 0xFFFF;
            for (int i = 0; i < 256 && viewOk == 0; ++i) {
                read(getViewRowFrame(++corr_, 7));
                if (viewOk == 0) lastStatus = lastNonOkStatus_;
            }
            if (viewOk == 0)
                throw std::logic_error("GatewayViewReadRig: key 7 never served (status " + std::to_string(lastStatus) + ")");
            viewOk = viewOther = redirects = errOther = otherFrames = 0;
            lastHp = -1;
        }
    }
    ~GatewayViewReadRig() { std::error_code ec; std::filesystem::remove_all(dir_, ec); }
    GatewayViewReadRig(const GatewayViewReadRig&) = delete;
    GatewayViewReadRig& operator=(const GatewayViewReadRig&) = delete;

    std::vector<uint8_t> getViewRowFrame(int64_t corr, uint64_t key) const {   // pc::GetViewRow, fp 0
        rune::protocol::client::GetViewRow m;
        m.sessionId = sid; m.correlationId = corr; m.viewTopicId = kViewTopic; m.keyU64 = key;
        m.requestedFingerprint = 0;
        std::vector<uint8_t> f(rune::protocol::client::GetViewRow::ENCODED_SIZE);
        m.encode(f.data());
        return f;
    }
    void read(const std::vector<uint8_t>& f) { gw.applyGetViewRowForTest(f.data(), f.size()); pass(); }
    /// A served read: apply, then passes until ITS answer arrives (<= 64); true when OK.
    bool readAndAwait(const std::vector<uint8_t>& f) {
        const uint64_t okBefore = viewOk, before = viewOk + viewOther + redirects + errOther;
        gw.applyGetViewRowForTest(f.data(), f.size());
        for (int k = 0; k < 64 && viewOk + viewOther + redirects + errOther == before; ++k) pass();
        return viewOk == okBefore + 1 && viewOk + viewOther + redirects + errOther == before + 1;
    }
    void pass() {
        now += 1'000'000;
        (void)pool.worker(0).doWork(now);
        channel.deltas.drain([](int32_t, aeron::concurrent::AtomicBuffer&, aeron::util::index_t, aeron::util::index_t) {},
                             1 << 20);
        gw.drainWorkerReturnsForTest(now);
    }
    uint64_t viewOk = 0, viewOther = 0, redirects = 0, errOther = 0, otherFrames = 0;
    int64_t  lastHp = -1;

    std::string                                                    dir_;
    rune::storage::Log                                             srcLog;
    rune::replication::InMemoryTransport                           transport;
    std::unique_ptr<rune::replication::PartitionReplica>           src;
    rune::replication::NullPartitionTransport                      fillerTransport;
    std::vector<std::unique_ptr<rune::storage::InMemoryLog>>       fillerLogs;
    std::vector<std::unique_ptr<rune::replication::PartitionReplica>> fillerReps;
    rune::codec::Descriptor                                        srcDesc;
    rune::node::ViewSnapshotChannel                                channel;
    std::string                                                    vsDir_;
    std::unique_ptr<rune::node::WorkerViewSet>                     vs;
    rune::view::RowLayout                                          layout;
    std::unique_ptr<rune::gateway::TopicRegistry>                  registry;
    rune::node::WorkerPool                                         pool{1, rune::metadata::ShardRouter(1), 1 << 22, 1 << 25, 64, 64};
    rune::gateway::ClientGateway                                   gw;
    int64_t                                                        sid = 0;
    int64_t                                                        now = 1'000'000'000;

private:
    int64_t  corr_ = 0;
    uint16_t lastNonOkStatus_ = 0xFFFF;

    static std::string makeDir_() {
        static int seq = 0;
        const auto p = std::filesystem::temp_directory_path() /
                       ("rune_t23vr_" + std::to_string(::getpid()) + "_" + std::to_string(++seq));
        std::error_code ec;
        std::filesystem::remove_all(p, ec);
        std::filesystem::create_directories(p / "src");
        return p.string();
    }
    void onFrame_(const uint8_t* b, std::size_t n) {
        namespace pc = rune::protocol::client;
        const auto h = pc::MessageHeader::decode(b, n);
        if (!h) { ++otherFrames; return; }
        if (h->templateId == pc::ViewRowResponse::TEMPLATE_ID) {
            if (const auto r = pc::ViewRowResponse::decode(b, n)) {
                if (r->status == 0) {
                    ++viewOk;
                    lastHp = rune::view::readSlot(r->rowBytes, layout, 1).i;
                } else {
                    ++viewOther;
                    lastNonOkStatus_ = r->status;
                }
            } else ++viewOther;
        } else if (h->templateId == pc::ErrorResponse::TEMPLATE_ID) {
            if (const auto r = pc::ErrorResponse::decode(b, n);
                r && r->code == 3 && r->message == kOtherEndpoint) ++redirects;
            else ++errOther;
        } else {
            ++otherFrames;
        }
    }
};

}  // namespace rune::node::test
