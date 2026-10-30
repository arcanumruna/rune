#pragma once
// CheckedWritesRig.h - checked writes Part 04: one Worker over a leader partition (topic 6, partition 0) with a tag-capable
// source {id uint64 key, v int64, tag uint8}, TestReject views and a projection, driven pass by pass without a driver.
#include <rune/aeronutil/testing/FreePort.h>
#include <rune/codec/Filter.h>
#include <rune/codec/FilterPredicate.h>
#include <rune/codec/Serialize.h>
#include <rune/node/EmbeddedDriver.h>
#include <rune/node/WorkerIngress.h>
#include <rune/node/Worker.h>
#include <rune/node/ViewSnapshotIoAgent.h>
#include <rune/node/WorkerOp.h>
#include <rune/node/WorkerViewSet.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/protocol/client/DataMessages.h>
#include <rune/protocol/client/Protocol.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/storage/ControlRecordLayout.h>
#include <rune/storage/Log.h>
#include <rune/storage/LogCleaner.h>
#include <rune/storage/SkimpyOffsetMap.h>
#include <rune/view/PlanCompiler.h>

#include <Aeron.h>
#include <FragmentAssembler.h>
#include <Subscription.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <thread>
#include <tuple>
#include <string>
#include <unistd.h>
#include <vector>

namespace rune::node::cw {

inline std::filesystem::path rigTmpPath(const char* what) {
    static std::atomic<int> seq{0};
    const auto t = std::chrono::steady_clock::now().time_since_epoch().count();
    auto p = std::filesystem::temp_directory_path() /
             (std::string("rune_cwv_") + what + "_" + std::to_string(::getpid()) + "_" + std::to_string(seq++) + "_" + std::to_string(t));
    std::filesystem::create_directories(p);
    return p;
}

inline rune::codec::Descriptor verdictSourceDescriptor() {
    using rune::codec::PrimType;
    return rune::codec::DescriptorBuilder("t", 1).add("id", PrimType::UInt64, 1, true).add("v", PrimType::Int64).add("tag", PrimType::UInt16).build();
}

struct Rec { uint64_t id; int64_t v; uint8_t tag = 0; bool del = false; };   ///< del: a tombstone of `id` (empty value)

/// What a consumer received: one record of a delivered batch.
struct Delivered { int64_t offset; uint64_t id; uint16_t tag; };
enum class SubKind : uint8_t { Raw, Filtered, PushRead };   ///< Filtered: tag == 1

class NullPartitionTransport final : public rune::replication::PartitionTransport {
public:
    bool sendFetchRequest(const rune::replication::FetchRequestMsg&) override { return true; }
    bool sendFetchResponse(rune::replication::NodeId, const rune::replication::FetchResponseMsg&) override { return true; }
};

class VerdictRig {
public:
    static constexpr int32_t kSrc = 6;
    static constexpr int kMaxPass = 4000;

    /// `follower`: node 2 is in the ISR, so the HW only moves when followerFetch() is called.
    /// `rollBytes`: the log's segment size; compact() needs small segments (1 = one batch per segment).
    explicit VerdictRig(bool follower = false, int64_t rollBytes = 64LL * 1024 * 1024)
        : isr_(follower ? std::vector<rune::replication::NodeId>{1, 2} : std::vector<rune::replication::NodeId>{1}),
          rollBytes_(rollBytes), viewDir_(rigTmpPath("views")), logDir_(rigTmpPath("log")) {
        vs = std::make_unique<WorkerViewSet>(channel, viewDir_.string());
        open();
    }
    ~VerdictRig() {
        std::error_code ec;
        worker.reset();
        vs.reset();
        std::filesystem::remove_all(viewDir_, ec);
        std::filesystem::remove_all(logDir_, ec);
    }

    void send(WorkerOp op) { ASSERT_TRUE(worker->tryEnqueueOp(std::move(op))); passes(3); }
    /// Types the topic (schema plus key facts) and sets the cluster feature level, so the partition gets a write state.
    void typed(int16_t level = 5) {
        WorkerOp l;
        l.type = WorkerOp::Type::ClusterFeatureLevel; l.nextOffset = level;
        send(std::move(l));
        WorkerOp v;
        v.type = WorkerOp::Type::SchemaUpdate; v.topicId = kSrc; v.clientVersion = 1;
        v.schemaUpdateBlob = rune::codec::serialize(verdictSourceDescriptor());
        send(std::move(v));
        WorkerOp f;
        f.type = WorkerOp::Type::FilterSchema; f.topicId = kSrc; f.clientVersion = 1;
        f.schemaUpdateBlob = rune::codec::serialize(verdictSourceDescriptor());
        send(std::move(f));
        TopicWriteSpecWire w;
        w.topicId = kSrc; w.keyFieldId = 0; w.keyType = rune::codec::PrimType::UInt64; w.keyWidth = 8; w.legacyValidate = false;
        WorkerOp s;
        s.type = WorkerOp::Type::WriteSpecUpdate; s.topicId = kSrc;
        s.schemaUpdateBlob.resize(kTopicWriteSpecWireLen);
        ASSERT_EQ(encodeTopicWriteSpec(w, s.schemaUpdateBlob), kTopicWriteSpecWireLen);
        send(std::move(s));
    }

    /// A TestReject view (and the verdict template behind it) over the source.
    void installRejectView(int32_t viewTopicId, uint64_t objectId, TestRejectRule rule, const char* name = "rej", bool persisted = false) {
        if (persisted && !agent) agent = std::make_unique<ViewSnapshotIoAgent>(channel, viewDir_.string(), /*fsyncMs=*/0);
        installs_.push_back([=, this] { installRejectViewNow_(viewTopicId, objectId, rule, name, persisted); });
        installs_.back()();
    }
    void installProjection(int32_t viewTopicId, const char* name = "proj") {
        installs_.push_back([=, this] { installProjectionNow_(viewTopicId, name); });
        installs_.back()();
    }
    /// The reject view `oldTopic` is replaced by `newTopic` (same object, another rule): the old one retires.
    void replaceRejectView(int32_t oldTopic, int32_t newTopic, uint64_t objectId, TestRejectRule rule) {
        vs->uninstallView(oldTopic);
        installs_.erase(installs_.begin());
        installRejectView(newTopic, objectId, rule, "rej2");
    }
    /// A new Worker, view set and replica over the same log: a restart.  The views are re-installed unless `reinstall` is false.
    void restart(bool reinstall = true, int64_t cutTo = -1) {
        worker.reset();
        vs.reset();
        replica.reset();
        if (cutTo >= 0) log->truncateTo(cutTo);
        log.reset();
        vs = std::make_unique<WorkerViewSet>(channel, viewDir_.string());
        open();
        typed();
        recovering_ = true;
        if (reinstall) reinstallAll();
        passes(3);
    }
    /// The follower (node 2) fetches everything: the HW moves to the log end.
    void followerFetch() {
        rune::replication::FetchRequestMsg fr{epoch_, 0, 2, leo(), 1 << 20, kSrc};
        (void)replica->onFetchRequest(fr, now);
        passes(2);
    }
    void truncateTo(int64_t offset) { log->truncateTo(offset); passes(5); }
    /// Another replica takes over: this node's tail above the HW is gone and no memory survives.
    void failover(bool reinstall = true) { restart(reinstall, replica->highWatermark()); }
    void reinstallAll() { for (auto& f : installs_) f(); }
    void demotePromote() {
        replica->becomeFollower(++epoch_, 2);
        passes(3);
        replica->becomeLeader(++epoch_, isr_);
        passes(3);
    }

private:
    void installRejectViewNow_(int32_t viewTopicId, uint64_t objectId, TestRejectRule rule, const char* name, bool persisted) {
        const auto src = verdictSourceDescriptor();
        rune::view::ViewPlan v;
        v.viewName = name; v.key = "id"; v.sources = {{kSrc, "id"}};
        v.output = {{"id", rune::codec::PrimType::UInt64, 0, "id", rune::view::MergeRule::Replace},
                    {"v", rune::codec::PrimType::Int64, 0, "v", rune::view::MergeRule::Replace}};
        v.persistence = persisted ? 0 : 1;
        const rune::codec::Descriptor* srcs[] = {&src};
        auto comp = rune::view::compileView(v, srcs, viewTopicId);
        ASSERT_TRUE(comp.has_value());
        vs->installTestVerdictViewForTest(comp->plan, comp->rowLayout, src, persisted, objectId, rule, recovering_);
    }
    void installProjectionNow_(int32_t viewTopicId, const char* name) {
        const auto src = verdictSourceDescriptor();
        rune::view::ViewPlan v;
        v.viewName = name; v.key = "id"; v.sources = {{kSrc, "id"}};
        v.output = {{"id", rune::codec::PrimType::UInt64, 0, "id", rune::view::MergeRule::Replace},
                    {"v", rune::codec::PrimType::Int64, 0, "v", rune::view::MergeRule::Replace}};
        v.persistence = 1;
        const rune::codec::Descriptor* srcs[] = {&src};
        auto comp = rune::view::compileView(v, srcs, viewTopicId);
        ASSERT_TRUE(comp.has_value());
        vs->installView(comp->plan, comp->rowLayout, src, /*persisted=*/false);
    }

public:

    static std::vector<uint8_t> batch(const std::vector<Rec>& recs, int64_t pid = -1, int32_t seq = -1) {
        rune::protocol::RecordBatchBuilder b(0, 1000);
        if (pid >= 0) { b.setProducerId(pid); b.setProducerEpoch(0); b.setBaseSequence(seq); }
        for (const auto& r : recs) {
            std::vector<uint8_t> key(8), val;
            for (int i = 0; i < 8; ++i) key[static_cast<std::size_t>(i)] = static_cast<uint8_t>(r.id >> (8 * i));
            if (r.del) { b.append({reinterpret_cast<const char*>(key.data()), key.size()}, {}, 1000); continue; }
            const auto le16 = [&](uint16_t x) { val.push_back(uint8_t(x)); val.push_back(uint8_t(x >> 8)); };
            le16(18); le16(1);
            for (int i = 0; i < 8; ++i) val.push_back(static_cast<uint8_t>(r.id >> (8 * i)));
            for (int i = 0; i < 8; ++i) val.push_back(static_cast<uint8_t>(uint64_t(r.v) >> (8 * i)));
            le16(r.tag);
            b.append({reinterpret_cast<const char*>(key.data()), key.size()}, {reinterpret_cast<const char*>(val.data()), val.size()}, 1000);
        }
        return b.build();
    }

    void pass() {
        now += 1'000'000;
        ++passNo_;
        worker->doWork(now);
        pollAcks_();
        worker->drainReturn([&](WorkerReturn r) {
            if (r.type == WorkerReturn::Type::ProduceResult) produceAnswers.push_back(r.errorCode);
        }, [](const TxnReturn&) {});
        if (agent) (void)agent->doWork(now);
        else channel.deltas.drain([](int32_t, aeron::concurrent::AtomicBuffer&, OneToOneByteRing::index_t, OneToOneByteRing::index_t) {});
    }
    void passes(int n) { for (int i = 0; i < n; ++i) pass(); }

    /// A produce through the worker's op path, re-sent while the partition is still building (NotLeader).
    int16_t produce(const std::vector<Rec>& recs) {
        const auto bytes = batch(recs);
        for (int i = 0; i < kMaxPass; ++i) {
            WorkerOp op;
            op.type = WorkerOp::Type::Produce; op.topicId = kSrc; op.partition = 0;
            op.replica = replica.get(); op.logPtr = log.get(); op.sessionId = 1;
            op.correlationId = ++corr; op.durabilityTier = 1; op.recordBatchBytes = bytes;
            EXPECT_TRUE(worker->tryEnqueueOp(std::move(op)));
            produceAnswers.clear();
            pass();
            if (produceAnswers.empty()) return -1;
            if (produceAnswers.back() != static_cast<int16_t>(rune::protocol::client::ErrorCode::NotLeader)) return produceAnswers.back();
        }
        return -2;
    }

    [[nodiscard]] const uint8_t* row(int32_t viewTopicId, uint64_t key) const {
        const rune::view::RowLayout* L = nullptr;
        return vs->getRow(viewTopicId, 0, key, L);
    }
    [[nodiscard]] bool hasRow(int32_t viewTopicId, uint64_t key) const { return row(viewTopicId, key) != nullptr; }
    [[nodiscard]] const VerdictSourceStats* stats() const { return worker->verdictStatsForTest(kSrc, 0); }
    [[nodiscard]] int64_t leo() const { return replica->logEndOffset(); }


    // ---- consumers ----
    /// The records a consumer got, and the frames they came in (base offset, record count as delivered).
    struct Frame { int64_t base; int32_t count; int64_t lastOffsetDelta; };
    struct Inbox { std::vector<Delivered> records; std::vector<Frame> frames; };

    void ensureDriver_() {
        if (driver_) return;
        rune::node::EmbeddedDriverConfig dc;
        dc.dir = (std::filesystem::temp_directory_path() / ("rune_cwv_aeron_" + std::to_string(::getpid()) + "_" + std::to_string(rigSeq()++))).string();
        dc.termBufferBytes = 64 * 1024;
        driver_ = std::make_unique<rune::node::EmbeddedDriver>(dc, rune::node::DriverMode::Threaded);
        aeron::Context ctx; ctx.aeronDir(dc.dir);
        aeron_ = aeron::Aeron::connect(ctx);
    }
    void enableEgress() {
        if (egress_) return;
        ensureDriver_();
        const std::string control = "127.0.0.1:" + std::to_string(rune::aeronutil::testing::allocFreeUdpPort());
        const std::string local   = "127.0.0.1:" + std::to_string(rune::aeronutil::testing::allocFreeUdpPort());
        egress_ = std::make_unique<WorkerEgress>(aeron_, control.c_str(), 3001);
        const int64_t reg = aeron_->addSubscription("aeron:udp?control=" + control + "|control-mode=dynamic|endpoint=" + local, 3001);
        for (int i = 0; i < 5000 && !(sub_ && sub_->isConnected() && egress_->isConnected()); ++i) {
            if (!sub_) sub_ = aeron_->findSubscription(reg);
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        ASSERT_TRUE(sub_ && sub_->isConnected() && egress_->isConnected());
        worker->setWorkerEgress(egress_.get());
    }

    /// A subscription from `from`.  Raw / Filtered ride the worker's egress; PushRead is answered through the return ring.
    int64_t subscribe(SubKind kind, int64_t from = 0) {
        const int64_t id = ++subSeq_;
        kinds_[id] = kind;
        cursors_[id] = from;
        if (kind == SubKind::PushRead) return id;
        enableEgress();
        WorkerOp op;
        op.type = WorkerOp::Type::Subscribe;
        op.subscriptionId = id; op.sessionId = 4200 + id; op.correlationId = 9000 + id;
        op.topicId = kSrc; op.partition = 0; op.nextOffset = from;
        op.replica = replica.get(); op.logPtr = log.get();
        if (kind == SubKind::Filtered) {
            rune::codec::FilterBytes fb;
            rune::codec::FilterWriter w(std::span<uint8_t>(fb.bytes.data(), fb.bytes.size()));
            w.term(2, rune::codec::PrimType::UInt16, 2, rune::Match<uint16_t>(1));
            const auto n = w.finish();
            EXPECT_TRUE(n && *n != 0);
            op.filterBytes = std::span<const uint8_t>(fb.bytes.data(), *n);
            filters_[id] = std::vector<uint8_t>(fb.bytes.begin(), fb.bytes.begin() + static_cast<std::ptrdiff_t>(*n));
            op.filterBytes = filters_[id];
        }
        EXPECT_TRUE(worker->tryEnqueueOp(std::move(op)));
        return id;
    }

    /// Drives until the consumer has nothing more to deliver, then returns what it got so far (cumulative).
    const Inbox& collect(int64_t subId) {
        Inbox& box = inboxes_[subId];
        if (kinds_.at(subId) == SubKind::PushRead) {
            for (int guard = 0; guard < 400; ++guard) {
                WorkerOp op;
                op.type = WorkerOp::Type::PushRead;
                op.subscriptionId = subId; op.sessionId = 4200 + subId; op.topicId = kSrc; op.partition = 0;
                op.nextOffset = cursors_[subId]; op.hwBound = leo();
                op.replica = replica.get(); op.logPtr = log.get();
                EXPECT_TRUE(worker->tryEnqueueOp(std::move(op)));
                const int64_t before = cursors_[subId];
                std::vector<uint8_t> chunk;
                int64_t next = before;
                now += 1'000'000;
                worker->doWork(now);
                worker->drainReturn([&](WorkerReturn r) {
                    if (r.type == WorkerReturn::Type::PushReadResult) { chunk.assign(r.chunkBytes().begin(), r.chunkBytes().end()); next = r.newNextOffset; }
                    if (r.type == WorkerReturn::Type::ProduceResult) produceAnswers.push_back(r.errorCode);
                }, [](const TxnReturn&) {});
                absorb_(box, chunk.data(), chunk.size());
                cursors_[subId] = next;
                if (next == before) break;
            }
            return box;
        }
        int quiet = 0;
        while (quiet < 40) {
            pass();
            std::size_t before = box.frames.size();
            sub_->poll(aeron::FragmentAssembler([&](aeron::concurrent::AtomicBuffer& buf, aeron::util::index_t off, aeron::util::index_t len, aeron::Header&) {
                const auto dec = rune::protocol::client::Records::decode(reinterpret_cast<const uint8_t*>(buf.buffer() + off), static_cast<std::size_t>(len));
                if (dec && dec->subscriptionId == subId) absorb_(box, dec->recordBatch.data(), dec->recordBatch.size());
            }).handler(), 64);
            quiet = box.frames.size() == before ? quiet + 1 : 0;
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        return box;
    }


    // ---- produce acks through the worker's ack sink ----
    struct Ack {
        int16_t code = 0;
        int64_t baseOffset = 0;
        bool    hasVerdicts = false;
        std::vector<std::tuple<uint16_t, uint16_t, uint32_t>> verdicts;
        std::size_t frameLen = 0;
        int64_t step = 0;
    };
    static constexpr int32_t kIngressStream = 7601, kAckStream = 7602;
    static constexpr int64_t kAckSession = 5;

    /// Answers ride the worker's ack sink to a session announced at `clientVersion`.  `arenaEntries` != 0 shrinks the sink's verdict arena.
    void enableAcks(int32_t clientVersion = 17, uint32_t arenaEntries = 0) {
        ensureDriver_();
        ackVersion_ = clientVersion;
        arenaEntries_ = arenaEntries;
        if (!ackIngress_) {
            ackIngress_ = std::make_unique<WorkerIngress>(aeron_, "aeron:ipc", kIngressStream);
            const int64_t sreg = aeron_->addSubscription("aeron:ipc", kAckStream);
            const int64_t preg = aeron_->addExclusivePublication("aeron:ipc", kIngressStream);
            for (int i = 0; i < 5000 && !(ackSub_ && ackPub_ && ackPub_->isConnected()); ++i) {
                if (!ackSub_) ackSub_ = aeron_->findSubscription(sreg);
                if (!ackPub_) ackPub_ = aeron_->findExclusivePublication(preg);
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }
            ASSERT_TRUE(ackSub_ && ackPub_ && ackPub_->isConnected());
        }
        wireAcks_();
    }
    /// A Produce frame on the worker's ingress; returns its correlation id.  `recs` is one batch.
    int64_t produceFrame(const std::vector<Rec>& recs, uint8_t acks, int64_t pid = -1, int32_t seq = -1) {
        const auto bytes = batch(recs, pid, seq);
        const int64_t c = ++corr;
        rune::protocol::client::Produce m;
        m.sessionId = kAckSession; m.correlationId = c; m.topicId = kSrc; m.partition = 0; m.partitionKnown = 1;
        m.acks = acks; m.producerId = pid; m.producerEpoch = 0; m.baseSequence = seq;
        m.recordBatch = std::span<const uint8_t>(bytes);
        std::vector<uint8_t> f(m.encodedSize()); m.encode(f.data());
        aeron::concurrent::AtomicBuffer ab(f.data(), static_cast<aeron::util::index_t>(f.size()));
        for (int i = 0; i < 5000 && ackPub_->offer(ab, 0, static_cast<aeron::util::index_t>(f.size())) <= 0; ++i)
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        return c;
    }
    /// Drives until the ack of `corr` arrived (or `maxPasses`).
    std::optional<Ack> ack(int64_t c, int maxPasses = 600) {
        for (int i = 0; i < maxPasses; ++i) {
            if (const auto it = acksSeen_.find(c); it != acksSeen_.end()) return it->second;
            pass();
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        if (const auto it = acksSeen_.find(c); it != acksSeen_.end()) return it->second;
        return std::nullopt;
    }
    /// Passes with no waiting for a particular ack (a delay check).
    void settle(int n = 40) { for (int i = 0; i < n; ++i) { pass(); std::this_thread::sleep_for(std::chrono::milliseconds(1)); } }
    [[nodiscard]] bool acked(int64_t c) const { return acksSeen_.count(c) != 0; }
    [[nodiscard]] const std::vector<int64_t>& ackOrder() const { return ackOrder_; }
    [[nodiscard]] int64_t passNo() const { return passNo_; }
    [[nodiscard]] const SessionAckSink* sinkForTest() const { return worker->ackSinkForTest(); }

    static int& rigSeq() { static int n = 0; return n; }

    /// The checked-writes control record types (16-19) in the log, in offset order.
    [[nodiscard]] std::vector<int> controlTypes() const {
        std::vector<int> out;
        for (int64_t off = 0; off < leo();) {
            const auto v = log->readView(off, 1 << 20);
            if (v.empty()) break;
            for (std::size_t p = 0; p + 12 <= v.len;) {
                int32_t blen = 0;
                std::memcpy(&blen, v.data + p + 8, 4);
                const std::size_t total = 12 + static_cast<std::size_t>(blen);
                if (p + total > v.len) break;
                const int t = rune::storage::control_record::recordType(v.data + p, total);
                if (t != rune::storage::control_record::kUnparseable) out.push_back(t);
                p += total;
            }
            off = v.nextOffset;
        }
        return out;
    }

    // ---- the log, read back ----
    struct RigLogRec { int64_t offset; uint64_t id; int64_t v; uint8_t tag; bool del; };
    struct RigChange { int64_t offset; std::vector<rune::storage::control_record::ChangeMember> members; };
    struct RigMarker { int64_t offset; rune::storage::control_record::MarkerHeader header; std::vector<std::tuple<int64_t, uint16_t, uint32_t>> entries; bool horizon = false; };
    struct RigLog { std::vector<RigLogRec> data; std::vector<RigChange> changes; std::vector<RigMarker> markers; };

    /// Every data record, change record and marker below `through`, in offset order, straight from the segments.
    [[nodiscard]] RigLog readLog(int64_t through = INT64_MAX) const {
        namespace crl = rune::storage::control_record;
        RigLog out;
        for (int64_t off = log->logStartOffset(); off < std::min<int64_t>(leo(), through);) {
            const auto v = log->readView(off, 1 << 20);
            if (v.empty()) {   // a compacted gap: continue at the next segment
                const auto bases = log->segmentBaseOffsets();
                const auto next = std::upper_bound(bases.begin(), bases.end(), off);
                if (next == bases.end()) break;
                off = *next;
                continue;
            }
            for (std::size_t p = 0; p + 12 <= v.len;) {
                int32_t blen = 0;
                std::memcpy(&blen, v.data + p + 8, 4);
                const std::size_t total = 12 + static_cast<std::size_t>(blen);
                if (p + total > v.len) break;
                const uint8_t* b = v.data + p;
                rune::protocol::RecordBatchReader r(b, total);
                if ((r.attributes() & rune::protocol::batch::kAttrControl) == 0) {
                    (void)rune::protocol::forEachRecordSpan(b, total, [&](const rune::protocol::RecordSpan& rs) {
                        if (rs.offset >= through) return;
                        uint64_t id = 0;
                        for (int i = 0; i < 8; ++i) id |= uint64_t(uint8_t(rs.key[static_cast<std::size_t>(i)])) << (8 * i);
                        if (rs.value.empty()) { out.data.push_back({rs.offset, id, 0, 0, true}); return; }
                        const auto* val = reinterpret_cast<const uint8_t*>(rs.value.data());
                        uint64_t x = 0;
                        for (int i = 0; i < 8; ++i) x |= uint64_t(val[12 + i]) << (8 * i);
                        out.data.push_back({rs.offset, id, static_cast<int64_t>(x), static_cast<uint8_t>(val[20]), false});
                    });
                } else if (r.baseOffset() < through) {
                    const int t = crl::recordType(b, total);
                    if (t == crl::kConstraintSetChange) {
                        std::array<crl::ChangeMember, 32> ms{};
                        uint32_t setId = 0; uint16_t n = 0;
                        if (crl::parseConstraintSetChange(b, total, setId, ms, n)) out.changes.push_back({r.baseOffset(), std::vector<crl::ChangeMember>(ms.begin(), ms.begin() + n)});
                    } else if (t == crl::kVerdictMarker) {
                        RigMarker m{r.baseOffset(), *crl::parseMarkerHeader(b, total), {}, r.hasDeleteHorizon()};
                        (void)crl::forEachMarkerEntry(b, total, [&](int64_t o, uint16_t c, uint32_t cid) { m.entries.emplace_back(o, c, cid); });
                        out.markers.push_back(std::move(m));
                    }
                }
                p += total;
            }
            off = v.nextOffset;
        }
        return out;
    }

    [[nodiscard]] const PartitionWriteState* writeState() const { return worker->writeStateForTest(kSrc, 0); }

    /// One past the last data record or change record: what the judge and the markers must cover.
    [[nodiscard]] int64_t dataEnd() const {
        const auto l = readLog();
        int64_t end = 0;
        if (!l.data.empty()) end = std::max(end, l.data.back().offset + 1);
        if (!l.changes.empty()) end = std::max(end, l.changes.back().offset + 1);
        return end;
    }

    /// Passes (with follower fetches when the rig has a follower) until the judge reached `target` and the markers covering it are
    /// committed.  false = it did not within kMaxPass passes.
    bool driveUntilJudged(int64_t target = -1) {
        if (target < 0) target = dataEnd();
        for (int i = 0; i < kMaxPass; ++i) {
            const VerdictSource* judge = vs->verdictSource(kSrc, 0);
            const PartitionWriteState* w = writeState();
            if (judge != nullptr && w != nullptr && judge->judgedThrough() >= target && w->markers.committedThrough() >= target) return true;
            if (isr_.size() > 1 && i % 8 == 0) followerFetch();
            pass();
        }
        const VerdictSource* judge = vs->verdictSource(kSrc, 0);
        const PartitionWriteState* w = writeState();
        ADD_FAILURE() << "judgedThrough " << (judge ? judge->judgedThrough() : -9) << " committed " << (w ? w->markers.committedThrough() : -9) << " target " << target << " leo " << leo();
        return false;
    }

    // ---- compaction ----
    struct Compacted { int64_t bound = 0; int64_t passEnd = 0; int installed = 0; };
    /// Seals the active segment (a reservation record rolls it), then one cleaner pass over [0, firstUncleanable) with the replica's
    /// published bounds - what LogCleanerAgent does for one log - and the owner's swap drain (`drain` false leaves the outputs queued,
    /// see drainSwaps).  `nowMs` is the cleaner's clock (delete horizons).  Needs a small `rollBytes`.
    Compacted compact(int64_t nowMs = 1'000'000, bool seal = true, bool drain = true) {
        namespace crl = rune::storage::control_record;
        if (seal) {
            std::vector<uint8_t> f(crl::detail::batchBytes(1));
            std::size_t pos = crl::detail::beginBatch(f.data(), crl::kReservation, 1);
            f[pos++] = crl::kPayloadVersion;
            crl::detail::sealBatch(f.data(), pos);
            EXPECT_GE(replica->appendControlBatch(f, false), 0);
            passes(4);
        }
        Compacted out;
        for (int i = 0; i < 16 && (log->runCleanerFileOps(nowMs, replica->fileDeleteDelayMs()), log->cleanerFileOpsPending()); ++i) passes(1);   // the posted renames of the last install
        out.bound = replica->cleanerVerdictBound();
        const int64_t markerEnd = replica->cleanerVerdictMarkerEnd();
        const auto snap = log->cleanerSnapshot();
        if (!snap || snap->size() < 1) return out;
        std::vector<int64_t> bases, sizes;
        for (const auto& si : *snap) { bases.push_back(si.baseOffset); sizes.push_back(si.sizeBytes); }
        int64_t uncleanable = std::min({snap->back().endOffset, replica->cleanerHighWatermark(), replica->cleanerLastStableOffset(), out.bound});
        for (const auto& si : *snap) {
            if (log->cleanerCompactionAllowedThrough(si.endOffset)) continue;
            uncleanable = std::min(uncleanable, si.baseOffset);
            break;
        }
        rune::storage::LogCleaner cleaner;
        cleaner.setMarkerRemovalBound(replica->cleanerMarkerRemovalBound());
        cleaner.setVerdictBounds(out.bound, markerEnd);
        cleaner.setDeferredInstall(&cleanedSeq_);
        rune::storage::SkimpyOffsetMap map(rune::storage::SkimpyOffsetMap::maxSlotsFromBudget(1 << 22));
        auto res = cleaner.produceCleanedRangeBounded(log->dir(), bases, sizes, uncleanable, 0, nowMs * 1'000'000, 1000, map, false, 1 << 20, nullptr, nowMs);
        out.passEnd = res.passEnd;
        for (const auto& g : res.groups) {
            std::vector<rune::storage::Log::CleanedMember> members;
            for (const int64_t gb : g.bases) {
                uint64_t lineage = rune::storage::Log::kAnyLineage;
                for (const auto& si : *snap) if (si.baseOffset == gb) lineage = si.lineage;
                members.push_back({gb, lineage});
            }
            auto prebuilt = g.hasOutput ? log->openCleanedOutput(g.bases.front(), g.seq, g.nextOffset) : nullptr;
            EXPECT_TRUE(log->enqueueCleanedGroup(members.data(), members.size(), g.seq, g.hasOutput, g.nextOffset, prebuilt.get()));
            (void)prebuilt.release();   // the owner owns it now
        }
        if (drain) out.installed = drainSwaps();
        return out;
    }
    /// The owner's drain of the queued compaction outputs; the count of installs (and rejections) it did.
    int drainSwaps() {
        int n = 0;
        for (int i = 0; i < kMaxPass && log->cleanerSwapsPending(); ++i) n += replica->drainCleanerSwapsOnOwner();   // one group per call
        passes(2);
        return n;
    }

    std::unique_ptr<rune::storage::Log> log;
    std::unique_ptr<rune::replication::PartitionReplica> replica;
    ViewSnapshotChannel channel;
    std::unique_ptr<WorkerViewSet> vs;
    std::unique_ptr<ViewSnapshotIoAgent> agent;   ///< only with a persisted view: it writes the checkpoint files
    std::unique_ptr<Worker> worker;
    int64_t now = 0, corr = 0;
    std::vector<int16_t> produceAnswers;

private:
    void absorb_(Inbox& box, const uint8_t* data, std::size_t len) {
        for (std::size_t pos = 0; pos + 12 <= len;) {
            rune::protocol::RecordBatchReader r(data + pos, len - pos);
            const int32_t total = r.totalLength();
            if (total <= 0 || pos + static_cast<std::size_t>(total) > len) break;
            box.frames.push_back(Frame{r.baseOffset(), r.recordCount(), r.lastOffsetDelta()});
            (void)rune::protocol::forEachRecordSpan(data + pos, static_cast<std::size_t>(total), [&](const rune::protocol::RecordSpan& rs) {
                const auto* v = reinterpret_cast<const uint8_t*>(rs.value.data());
                uint64_t id = 0;
                for (int i = 0; i < 8; ++i) id |= uint64_t(v[4 + i]) << (8 * i);
                box.records.push_back(Delivered{rs.offset, id, static_cast<uint16_t>(v[20] | (v[21] << 8))});
            });
            pos += static_cast<std::size_t>(total);
        }
    }
    void wireAcks_() {
        if (!ackIngress_) return;
        worker->setWorkerIngress(ackIngress_.get(), true);
        worker->setShardMode(true);
        worker->setShardClient(aeron_.get());
        if (arenaEntries_ != 0) worker->setVerdictAckArenaEntriesForTest(arenaEntries_);
        WorkerOp a; a.type = WorkerOp::Type::SessionAnnounce; a.grantAll = true; a.sessionId = kAckSession;
        a.egressStreamId = kAckStream; a.egressChannel = "aeron:ipc?term-length=65536"; a.clientVersion = ackVersion_;
        EXPECT_TRUE(worker->tryEnqueueOp(std::move(a)));
    }
    void pollAcks_() {
        if (!ackSub_) return;
        ackSub_->poll(aeron::FragmentAssembler([&](aeron::concurrent::AtomicBuffer& b, aeron::util::index_t o, aeron::util::index_t l, aeron::Header&) {
            const auto r = rune::protocol::client::ProduceResponse::decode(reinterpret_cast<const uint8_t*>(b.buffer() + o), static_cast<std::size_t>(l));
            if (!r) return;
            Ack a;
            a.code = r->errorCode; a.baseOffset = r->baseOffset; a.hasVerdicts = r->hasVerdicts; a.frameLen = static_cast<std::size_t>(l); a.step = passNo_;
            r->forEachVerdict([&](uint16_t i, uint16_t code, uint32_t cid) { a.verdicts.emplace_back(i, code, cid); });
            if (acksSeen_.emplace(r->correlationId, std::move(a)).second) ackOrder_.push_back(r->correlationId);
        }).handler(), 64);
    }
    std::unique_ptr<WorkerIngress> ackIngress_;
    std::shared_ptr<aeron::Subscription> ackSub_;
    std::shared_ptr<aeron::ExclusivePublication> ackPub_;
    std::map<int64_t, Ack> acksSeen_;
    std::vector<int64_t> ackOrder_;
    int32_t ackVersion_ = 17;
    uint32_t arenaEntries_ = 0;
    int64_t passNo_ = 0;
    std::unique_ptr<rune::node::EmbeddedDriver> driver_;
    std::shared_ptr<aeron::Aeron> aeron_;
    std::unique_ptr<WorkerEgress> egress_;
    std::shared_ptr<aeron::Subscription> sub_;
    std::map<int64_t, SubKind> kinds_;
    std::map<int64_t, int64_t> cursors_;
    std::map<int64_t, Inbox> inboxes_;
    std::map<int64_t, std::vector<uint8_t>> filters_;
    int64_t subSeq_ = 0;

    void open() {
        log = std::make_unique<rune::storage::Log>(logDir_.string(), rollBytes_);
        log->recover();
        rune::replication::PartitionConfig c{};
        c.topicId = kSrc; c.partitionId = 0; c.selfId = 1; c.minIsr = 1;
        c.replicaLagMaxMs = 1'000'000; c.replicaLagMaxOffset = 1'000'000;
        c.fetchIntervalNs = 1'000'000'000LL; c.isrCheckIntervalNs = 1'000'000'000LL;
        c.maxFetchBytes = 1 << 20; c.pushOnAppend = false;
        replica = std::make_unique<rune::replication::PartitionReplica>(c, rune::replication::Role::Leader, 1, isr_, isr_, log.get(), &transport_);
        worker = std::make_unique<Worker>(0);
        worker->setFilterConfig(filter::FilterConfig{});
        worker->setOwnedDirect(Worker::OwnedSet{replica.get()});
        worker->setViewSet(vs.get());
        if (egress_) worker->setWorkerEgress(egress_.get());
        wireAcks_();
    }

    std::vector<rune::replication::NodeId> isr_;
    int64_t rollBytes_;
    uint64_t cleanedSeq_ = 0;
    std::vector<std::function<void()>> installs_;
    int64_t epoch_ = 1;
    bool recovering_ = false;   ///< the views of a restarted node load their checkpoints
    std::filesystem::path viewDir_, logDir_;
    NullPartitionTransport transport_;
};

}  // namespace rune::node::cw
