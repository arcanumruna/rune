#pragma once
// Part 07 Task 14: rigs for GetViewRowK / ScanViewRows.  CwViewReadWorkerRig drives a real Worker + WorkerViewSet (a
// folded keyed view over cwkeyed::Log); CwViewReadGatewayRig adds a real ClientGateway + WorkerPool in front of it.
// Single-threaded: the test thread runs every duty cycle.
#include <rune/gateway/ClientGateway.h>
#include <rune/gateway/TopicRegistry.h>
#include <rune/metadata/Gpid.h>
#include <rune/metadata/RoutingImage.h>
#include <rune/metadata/ShardRouter.h>
#include <rune/node/ViewSnapshotChannel.h>
#include <rune/node/Worker.h>
#include <rune/node/WorkerPool.h>
#include <rune/node/WorkerViewSet.h>
#include <rune/protocol/client/DataMessages.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/storage/InMemoryLog.h>

#include <gtest/gtest.h>

#include <array>
#include <cstring>
#include <filesystem>
#include <memory>
#include <span>
#include <utility>
#include <stdexcept>
#include <string>
#include <vector>

#include <unistd.h>

#include "node/CheckedWritesKeyedViewFixture.h"

namespace rune::node::test {

inline std::string cwReadDir(const char* what) {
    static int seq = 0;
    const auto p = std::filesystem::temp_directory_path() / ("rune-cw-read-" + std::string(what) + "-" + std::to_string(::getpid()) + "-" + std::to_string(++seq));
    std::error_code ec;
    std::filesystem::remove_all(p, ec);
    return p.string();
}

/// A memory-backed single-node leader of source partition 0 (the Worker serves a view only under a leadership).
struct CwSrcReplica {
    rune::storage::InMemoryLog           log{1 << 20, 1000};
    rune::replication::InMemoryTransport transport;
    std::unique_ptr<rune::replication::PartitionReplica> replica;

    explicit CwSrcReplica(int32_t topicId, int32_t partition = 0) {
        rune::replication::PartitionConfig cfg{};
        cfg.topicId = topicId; cfg.partitionId = partition; cfg.selfId = 1; cfg.minIsr = 1;
        cfg.replicaLagMaxMs = 10'000; cfg.replicaLagMaxOffset = 10'000;
        cfg.fetchIntervalNs = 1'000'000LL; cfg.isrCheckIntervalNs = 1'000'000LL;
        cfg.maxFetchBytes = 1 << 20; cfg.pushOnAppend = false;
        replica = std::make_unique<rune::replication::PartitionReplica>(cfg, rune::replication::Role::Follower, 0,
                                                                        std::vector<rune::replication::NodeId>{1},
                                                                        std::vector<rune::replication::NodeId>{1}, &log, &transport);
        replica->becomeLeader(1, {1});
    }
    [[nodiscard]] uint64_t gen() const { return replica->leadershipGeneration(); }
};

/// Appends `recs` to the source replica one record each (a tombstone has an empty value), committed at once.
inline void appendRecords(rune::replication::PartitionReplica& replica, const rune::codec::Descriptor& d, const std::vector<cwkeyed::Rec>& recs) {
    for (const cwkeyed::Rec& rec : recs) {
        const std::string key = cwkeyed::wireKey(rec.characterId);
        const std::string value = rec.tombstone ? std::string() : cwkeyed::valueOf(d, rec);
        if (replica.appendKeyed({reinterpret_cast<const uint8_t*>(key.data()), key.size()},
                                {reinterpret_cast<const uint8_t*>(value.data()), value.size()}, false) < 0)
            throw std::logic_error("appendRecords: source append refused");
    }
}

/// One ScanViewRows answer, parsed.
struct CwScanReply {
    int16_t code = -1;
    int32_t rowCount = 0;
    std::vector<uint8_t> token;
    std::vector<std::vector<uint8_t>> rows;
};

inline CwScanReply parseScanReturn(const WorkerReturn& r) {
    CwScanReply out;
    out.code = r.errorCode;
    out.rowCount = r.recordCount;
    const std::span<const uint8_t> c = r.chunkBytes();
    if (c.empty()) return out;
    uint32_t tl = 0;
    std::memcpy(&tl, c.data(), 4);
    out.token.assign(c.begin() + 4, c.begin() + 4 + tl);
    uint32_t rl = 0;
    std::memcpy(&rl, c.data() + 4 + tl, 4);
    std::size_t pos = 8 + tl;
    for (std::size_t end = pos + rl; pos < end;) {
        uint32_t n = 0;
        std::memcpy(&n, c.data() + pos, 4);
        out.rows.emplace_back(c.begin() + pos + 4, c.begin() + pos + 4 + n);
        pos += 4 + n;
    }
    return out;
}

/// Enqueues a GetViewRowK / ScanViewRows op the way the gateway builds it.
inline bool postViewRead(Worker& w, WorkerOp::Type type, std::span<const uint8_t> keyOrPrefix, std::span<const uint8_t> token,
                         int32_t viewTopic, uint16_t limit = 0, int64_t minOffset = -1, int64_t corr = 1) {
    std::array<uint8_t, 2 + 128 + 12> tail{};
    tail[0] = static_cast<uint8_t>(keyOrPrefix.size());
    if (!keyOrPrefix.empty()) std::memcpy(tail.data() + 1, keyOrPrefix.data(), keyOrPrefix.size());
    tail[1 + keyOrPrefix.size()] = static_cast<uint8_t>(token.size());
    if (!token.empty()) std::memcpy(tail.data() + 2 + keyOrPrefix.size(), token.data(), token.size());
    WorkerOp op;
    op.type          = type;
    op.topicId       = viewTopic;
    op.partition     = 0;
    op.sessionId     = 1;
    op.correlationId = corr;
    op.nextOffset    = minOffset;
    op.recordCount   = limit;
    op.viewReadTail  = std::span<const uint8_t>(tail.data(), 2 + keyOrPrefix.size() + token.size());
    return w.tryEnqueueOp(op);
}

class CwViewReadWorkerRig {
public:
    CwViewReadWorkerRig()
        : dir(cwReadDir("worker")), vs(channel, dir), srcRep(cwkeyed::kSrcTopic), w(0) {
        w.setOwnedDirect(Worker::OwnedSet{srcRep.replica.get()});
        w.setViewSet(&vs);
    }
    ~CwViewReadWorkerRig() { std::error_code ec; std::filesystem::remove_all(dir, ec); }

    void install(const rune::view::CompileResult& c) {
        vs.installView(c.plan, c.rowLayout, src, false);
        installed.push_back(c.plan.viewTopicId);
    }
    /// Appends `recs` to the source and runs passes until every installed view is Ready and folded to the end.
    void append(const std::vector<cwkeyed::Rec>& recs) {
        appendRecords(*srcRep.replica, src, recs);
        for (int i = 0; i < 200; ++i) {
            step(now += 1'000'000);
            bool done = true;
            for (const int32_t v : installed)
                done &= vs.instanceLeadership(v, 0).has_value() && vs.readiness(v, 0) == rune::node::ViewReadiness::Ready &&
                        vs.foldCursorForTest(v, 0) >= srcRep.replica->highWatermark();
            if (done) return;
        }
        throw std::logic_error("CwViewReadWorkerRig: the views never folded");
    }
    /// One duty pass at `nowNs`; every view answer produced since the last call.
    std::vector<WorkerReturn> pass(int64_t nowNs) {
        step(now = nowNs);
        return std::exchange(answers_, {});
    }
    /// Posts one op, runs one pass; the single answer.
    WorkerReturn ask(WorkerOp::Type type, std::span<const uint8_t> keyOrPrefix, std::span<const uint8_t> token = {},
                     int32_t view = cwkeyed::kViewTopic, uint16_t limit = 0, int64_t minOffset = -1) {
        EXPECT_TRUE(postViewRead(w, type, keyOrPrefix, token, view, limit, minOffset));
        auto answers = pass(now += 1'000'000);
        EXPECT_EQ(answers.size(), 1u);
        return answers.empty() ? WorkerReturn{} : std::move(answers.front());
    }
    WorkerReturn getK(const std::vector<uint8_t>& key, int32_t view = cwkeyed::kViewTopic, int64_t minOffset = -1) {
        return ask(WorkerOp::Type::GetViewRowK, key, {}, view, 0, minOffset);
    }
    CwScanReply scan(const std::vector<uint8_t>& prefix, std::span<const uint8_t> token = {}, uint16_t limit = 0) {
        return parseScanReturn(ask(WorkerOp::Type::ScanViewRows, prefix, token, cwkeyed::kViewTopic, limit));
    }

    std::string dir;
    rune::codec::Descriptor src = cwkeyed::characterSource();
    ViewSnapshotChannel channel;
    WorkerViewSet vs;
    CwSrcReplica srcRep;
    Worker w;
    std::vector<int32_t> installed;
    int64_t now = 1'000'000'000;

private:
    void step(int64_t nowNs) {
        w.doWork(nowNs);
        w.drainReturn([&](WorkerReturn r) {
            if (r.type == WorkerReturn::Type::ViewRowResult || r.type == WorkerReturn::Type::ViewScanResult) answers_.push_back(std::move(r));
        });
    }
    std::vector<WorkerReturn> answers_;
};

/// A real ClientGateway + WorkerPool in front of the keyed view `account_characters` (sink topic kViewTopic, source kSrcTopic).
class CwViewReadGatewayRig {
public:
    static constexpr int32_t kSelf = 1, kSrcTopic = 5, kViewTopic = 6;

    explicit CwViewReadGatewayRig(int32_t sinkPartitions = 1, int32_t workerCount = 1)
        : dir_(cwReadDir("gateway")),
          vs(channel, dir_ + "/views"),
          srcRep(kSrcTopic),
          pool(workerCount, rune::metadata::ShardRouter(workerCount), 1 << 22, 1 << 25, 64, 64),
          gw(rune::gateway::ClientGateway::makeForTest([this](const uint8_t* b, std::size_t n) -> int64_t { onFrame_(b, n); return 1; }, 1)) {
        std::filesystem::create_directories(dir_);
        const auto c = cwkeyed::compileKeyed(srcDesc, {{"userId", 0, "userId"}, {"characterId", 0, "characterId"}}, {"gender"}, kViewTopic);
        layout = c.rowLayout;
        vs.installView(c.plan, c.rowLayout, srcDesc, false);
        const int32_t owner = pool.ownerWorkerId(kSrcTopic, 0);
        for (int32_t i = 0; i < pool.workerCount(); ++i) {
            if (i == owner) pool.worker(i).setOwnedDirect(Worker::OwnedSet{srcRep.replica.get()});
            pool.worker(i).setViewSet(&vs);
        }
        registry = std::make_unique<rune::gateway::TopicRegistry>(std::vector<rune::node::TopicSpec>{}, dir_ + "/reg");
        rune::node::TopicSpec sink;
        sink.name = "account_characters";
        sink.partitionCount = sinkPartitions;
        sink.viewSink = true;
        if (registry->createTopicAt(kViewTopic, sink) != kViewTopic) throw std::logic_error("CwViewReadGatewayRig: sink id");
        gw.setTopicRegistry(registry.get());
        gw.setWorkerPool(&pool);
        gw.setNodeIdForTest(kSelf);
        gw.noteViewSource(kViewTopic, kSrcTopic);
        std::vector<rune::metadata::RoutingImage::BuildEntry> img;
        for (int32_t p = 0; p < sinkPartitions; ++p) img.push_back({rune::metadata::gpidOf(kSrcTopic, p), kSelf, 1});
        img.push_back({rune::metadata::gpidOf(kViewTopic, 0), kSelf, 1});
        gw.setRoutingImageSnapshot(rune::metadata::RoutingImage::build(img, rune::metadata::RoutingImage::Source::Committed));
        gw.setCommittedMetadataReady(true);
        gw.setOnEgressFrame([this](int64_t, const uint8_t* b, std::size_t n) { onFrame_(b, n); });
        sid = gw.injectTestSession();
    }
    ~CwViewReadGatewayRig() { std::error_code ec; std::filesystem::remove_all(dir_, ec); }

    /// Appends `recs` to the source and runs passes until the view is Ready and folded to the end.
    void append(const std::vector<cwkeyed::Rec>& recs) {
        appendRecords(*srcRep.replica, srcDesc, recs);
        for (int i = 0; i < 200; ++i) {
            pass();
            if (vs.instanceLeadership(kViewTopic, 0).has_value() && vs.readiness(kViewTopic, 0) == rune::node::ViewReadiness::Ready &&
                vs.foldCursorForTest(kViewTopic, 0) >= srcRep.replica->highWatermark())
                return;
        }
        throw std::logic_error("CwViewReadGatewayRig: the view never folded");
    }
    void pass() {
        now += 1'000'000;
        for (int32_t i = 0; i < pool.workerCount(); ++i) (void)pool.worker(i).doWork(now);
        gw.drainWorkerReturnsForTest(now);
    }
    /// The workers' summed GetViewRowK / ScanViewRows op counts.
    [[nodiscard]] uint64_t workerGetKs(int32_t worker) { return pool.worker(worker).getViewRowKsForTest(); }

    std::vector<uint8_t> getKFrame(int64_t corr, const std::vector<uint8_t>& key, int32_t partition = -1, int64_t minOffset = -1,
                                   int64_t session = 0) const {
        rune::protocol::client::GetViewRowK m;
        m.sessionId = session != 0 ? session : sid; m.correlationId = corr; m.viewTopicId = kViewTopic; m.partition = partition;
        m.minOffset = minOffset; m.key = key;
        std::vector<uint8_t> f(m.encodedSize());
        m.encode(f.data());
        return f;
    }
    std::vector<uint8_t> scanFrame(int64_t corr, const std::vector<uint8_t>& prefix, std::span<const uint8_t> token = {}, uint16_t limit = 0,
                                   int64_t session = 0) const {
        rune::protocol::client::ScanViewRows m;
        m.sessionId = session != 0 ? session : sid; m.correlationId = corr; m.viewTopicId = kViewTopic; m.limit = limit;
        m.prefix = prefix; m.resumeToken = token;
        std::vector<uint8_t> f(m.encodedSize());
        m.encode(f.data());
        return f;
    }
    void sendK(const std::vector<uint8_t>& f) { gw.applyGetViewRowKForTest(f.data(), f.size()); }
    void sendScan(const std::vector<uint8_t>& f) { gw.applyScanViewRowsForTest(f.data(), f.size()); }
    /// Passes until the egress carried one more frame (<= 64 passes).
    bool awaitFrame() {
        const uint64_t before = frames;
        for (int k = 0; k < 64 && frames == before; ++k) pass();
        return frames == before + 1;
    }
    /// The status of the last frame when it is a ViewRowResponse (73) or ScanViewRowsResponse (90), else -1.
    [[nodiscard]] int32_t lastStatus() const {
        namespace pc = rune::protocol::client;
        const auto h = pc::MessageHeader::decode(lastFrame.data(), lastFrame.size());
        if (!h) return -1;
        if (h->templateId == pc::ViewRowResponse::TEMPLATE_ID) { const auto r = pc::ViewRowResponse::decode(lastFrame.data(), lastFrame.size()); return r ? r->status : -1; }
        if (h->templateId == pc::ScanViewRowsResponse::TEMPLATE_ID) { const auto r = pc::ScanViewRowsResponse::decode(lastFrame.data(), lastFrame.size()); return r ? r->status : -1; }
        if (h->templateId == pc::ErrorResponse::TEMPLATE_ID) { const auto r = pc::ErrorResponse::decode(lastFrame.data(), lastFrame.size()); return r ? 1000 + r->code : -1; }
        return -1;
    }

    std::string dir_;
    rune::codec::Descriptor srcDesc = cwkeyed::characterSource();
    ViewSnapshotChannel channel;
    WorkerViewSet vs;
    CwSrcReplica srcRep;
    rune::view::RowLayout layout;
    std::unique_ptr<rune::gateway::TopicRegistry> registry;
    WorkerPool pool;
    rune::gateway::ClientGateway gw;
    int64_t sid = 0;
    int64_t now = 1'000'000'000;
    uint64_t frames = 0;
    std::vector<uint8_t> lastFrame;

private:
    void onFrame_(const uint8_t* b, std::size_t n) {
        ++frames;
        lastFrame.assign(b, b + n);
    }
};

}  // namespace rune::node::test
