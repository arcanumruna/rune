#pragma once
// CheckedWritesRig.h - checked writes Part 04: one Worker over a leader partition (topic 6, partition 0) with a tag-capable
// source {id uint64 key, v int64, tag uint8}, TestReject views and a projection, driven pass by pass without a driver.
#include <rune/aeronutil/testing/FreePort.h>
#include <rune/codec/Evolve.h>
#include <rune/codec/Filter.h>
#include <rune/crypto/Aead.h>
#include <rune/protocol/client/CryptoEnvelope.h>
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
#include <rune/transaction/ControlMarker.h>
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
#include <set>
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

/// The key field of the source topic: an integer type, or (FixedStr) a string of `fixedLen` bytes.
struct KeyShape { rune::codec::PrimType type = rune::codec::PrimType::UInt64; uint16_t fixedLen = 0; };

inline rune::codec::Descriptor keyedSourceDescriptor(const KeyShape& s) {
    using rune::codec::PrimType;
    rune::codec::DescriptorBuilder b("t", 1);
    if (s.type == PrimType::FixedStr) b.addFixedStr("id", s.fixedLen, 1, true);
    else b.add("id", s.type, 1, true);
    return b.add("v", PrimType::Int64).add("tag", PrimType::UInt16).build();
}

/// The wire width of the key: records carry the id as that many little-endian bytes, and the value's id field is as wide.
inline uint32_t keyWidthOf(const KeyShape& s) { return rune::codec::fieldWidth(keyedSourceDescriptor(s).fields[0]); }

inline rune::codec::Descriptor verdictSourceDescriptor() { return keyedSourceDescriptor(KeyShape{}); }

/// del: a tombstone of `id` (empty value).  intent: the record's attribute bits 0-1 (0 upsert, 1 insert, 2 update / removeExisting).
struct Rec { uint64_t id; int64_t v; uint8_t tag = 0; bool del = false; uint8_t intent = 0; };

/// What a consumer received: one record of a delivered batch.
inline constexpr uint16_t kTombstoneTag = 0xFFFF;   ///< Delivered::tag of a tombstone
struct Delivered { int64_t offset; uint64_t id; uint16_t tag; uint8_t intent = 0; };
enum class SubKind : uint8_t { Raw, Filtered, PushRead };   ///< Filtered: tag == 1

class NullPartitionTransport final : public rune::replication::PartitionTransport {
public:
    bool sendFetchRequest(const rune::replication::FetchRequestMsg&) override { return true; }
    bool sendFetchResponse(rune::replication::NodeId, const rune::replication::FetchResponseMsg&) override { return true; }
};

/// The replica and worker settings a cell may change: minIsr and durabilityTier (2 = Strict) of the partition, the acks=all commit-wait (0 = the default).
struct RigTuning { int minIsr = 1; uint8_t durabilityTier = 1; int64_t ackTimeoutNs = 0; };

class VerdictRig {
public:
    static constexpr int32_t kSrc = 6;
    static constexpr int kMaxPass = 4000;

    /// `follower`: node 2 is in the ISR, so the HW only moves when followerFetch() is called.
    /// `rollBytes`: the log's segment size; compact() needs small segments (1 = one batch per segment).
    explicit VerdictRig(bool follower = false, int64_t rollBytes = 64LL * 1024 * 1024, RigTuning tuning = {})
        : isr_(follower ? std::vector<rune::replication::NodeId>{1, 2} : std::vector<rune::replication::NodeId>{1}),
          rollBytes_(rollBytes), tuning_(tuning), viewDir_(rigTmpPath("views")), logDir_(rigTmpPath("log")) {
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
    void typed(int16_t level = 5) { typedWith_(level, KeyShape{}, false); }
    /// typed() for a source whose key is `s`, with the write spec's key.index flag `enabled`.  A restart brings the topic back the same way.
    void createKeyIndex(KeyShape s = {}, bool enabled = true) {
        keyShape_ = s;
        keyIndexOn_ = enabled;
        typedWith_(5, s, enabled);
    }
    /// The write spec is sent again with key.index set: an adoption of a live topic.
    void enableKeyIndex() {
        requestKeyIndex();
        passes(3);
    }
    /// The same, queued only: the next pass takes it, so a frame sent right after it meets the adoption before any change record is logged.
    void requestKeyIndex() {
        keyIndexOn_ = true;
        ASSERT_TRUE(worker->tryEnqueueOp(specOp_(true)));
    }
    /// Schema version 2 drops the source's v field: its SchemaUpdate (materialized as of 2) and its FilterSchema (the evolving blob).  Default key only.
    void dropValueField() {
        using rune::codec::PrimType;
        rune::codec::DescriptorBuilder b("t", 2);
        b.addTagged("id", 0, PrimType::UInt64, 1, 0xFFFF, true, {}).addTagged("v", 1, PrimType::Int64, 1, 2, false, {}).addTagged("tag", 2, PrimType::UInt16, 1, 0xFFFF, false, {});
        const rune::codec::Descriptor evolving = b.build();
        WorkerOp v;
        v.type = WorkerOp::Type::SchemaUpdate; v.topicId = kSrc; v.clientVersion = 2;
        v.schemaUpdateBlob = rune::codec::serialize(rune::codec::materializeAsOf(evolving, 2));
        send(std::move(v));
        WorkerOp f;
        f.type = WorkerOp::Type::FilterSchema; f.topicId = kSrc; f.clientVersion = 2;
        f.schemaUpdateBlob = rune::codec::serialize(evolving);
        send(std::move(f));
    }
    /// The judges of this rig (created later ones too) do not run: nothing is activated, replayed or judged.
    void holdJudge(bool on) {
        holdJudge_ = on;
        vs->holdJudgesForTest(on);
    }
    /// The ids in [0, 4096) the key index holds, as decimal strings; the table's size must be that many.
    std::set<std::string> keyIndexKeys() {
        std::set<std::string> out;
        const uint32_t kw = keyWidthOf(keyShape_);
        std::string key(kw, '\0');
        for (uint64_t id = 0; id < 4096; ++id) {
            for (uint32_t i = 0; i < kw; ++i) key[i] = i < 8 ? static_cast<char>(id >> (8 * i)) : '\0';
            if (vs->keyIndexContainsForTest(kSrc, 0, key)) out.insert(std::to_string(id));
        }
        EXPECT_EQ(out.size(), vs->keyIndexSizeForTest(kSrc, 0)) << "a key outside [0, 4096)";
        return out;
    }

    /// A TestReject view (and the verdict template behind it) over the source.
    void installRejectView(int32_t viewTopicId, uint64_t objectId, TestRejectRule rule, const char* name = "rej", bool persisted = false) {
        if (persisted && !agent) agent = std::make_unique<ViewSnapshotIoAgent>(channel, viewDir_.string(), /*fsyncMs=*/0);
        installs_.emplace_back(viewTopicId, [=, this] { installRejectViewNow_(viewTopicId, objectId, rule, name, persisted); });
        installs_.back().second();
    }
    void installProjection(int32_t viewTopicId, const char* name = "proj") {
        installs_.emplace_back(viewTopicId, [=, this] { installProjectionNow_(viewTopicId, name); });
        installs_.back().second();
    }
    /// count(*) of the records folded per id (a view key is an 8-byte field: the tag cannot group).
    void installCountById(int32_t viewTopicId, const char* name = "cnt") {
        installs_.emplace_back(viewTopicId, [=, this] { installCountNow_(viewTopicId, name); });
        installs_.back().second();
    }
    /// The verdict view goes away (its object leaves the constraint set).
    void dropRejectView(int32_t viewTopic) {
        vs->uninstallView(viewTopic);
        std::erase_if(installs_, [&](const auto& i) { return i.first == viewTopic; });
    }
    /// The reject view `oldTopic` is replaced by `newTopic` (same object, another rule): the old one retires.
    void replaceRejectView(int32_t oldTopic, int32_t newTopic, uint64_t objectId, TestRejectRule rule) {
        dropRejectView(oldTopic);
        installRejectView(newTopic, objectId, rule, "rej2");
    }
    /// A new Worker, view set and replica over the same log: a restart.  The views are re-installed unless `reinstall` is false.
    /// `specDelayPasses`: the write spec carrying key.index reaches the worker that many passes after restart() returns; until then the
    /// topic is typed without it (the leader scan restores the logged set from the log).
    void restart(bool reinstall = true, int64_t cutTo = -1, int specDelayPasses = 0) {
        worker.reset();
        vs.reset();
        replica.reset();
        if (cutTo >= 0) (void)log->truncateTo(cutTo);
        log.reset();
        epoch_ = 1;   // the new replica starts at epoch 1
        vs = std::make_unique<WorkerViewSet>(channel, viewDir_.string());
        vs->holdJudgesForTest(holdJudge_);
        open();
        typedWith_(5, keyShape_, keyIndexOn_ && specDelayPasses == 0);
        recovering_ = true;
        if (reinstall) reinstallAll();
        passes(3);
        delayedSpecIn_ = keyIndexOn_ ? specDelayPasses : 0;
    }
    /// The follower (node 2) fetches everything, or up to `to`: the HW moves there.
    void followerFetch(int64_t to = -1) {
        rune::replication::FetchRequestMsg fr{epoch_, 0, 2, to < 0 ? leo() : to, 1 << 20, kSrc};
        (void)replica->onFetchRequest(fr, now);
        passes(2);
    }
    void truncateTo(int64_t offset) { (void)log->truncateTo(offset); passes(5); }
    /// The replica cuts its log (a follower meeting a new leader's history): the log generation moves.
    void truncateReplicaTo(int64_t offset) { replica->truncateLogForTest(offset); passes(5); }
    /// Another replica takes over: this node's tail above the HW is gone and no memory survives.
    void failover(bool reinstall = true) { restart(reinstall, replica->highWatermark()); }
    void reinstallAll() { for (auto& i : installs_) i.second(); }
    void demote() {
        replica->becomeFollower(++epoch_, 2);
        passes(3);
    }
    void demotePromote() {
        demote();
        replica->becomeLeader(++epoch_, isr_);
        passes(3);
    }
    /// Leader again without a pass: the state has not seen the leadership yet.
    void promoteWithoutPass() { replica->becomeLeader(++epoch_, isr_); }
    void promote() {
        replica->becomeLeader(++epoch_, isr_);
        passes(3);
    }
    /// As a follower: the leader (node 2) replicates `batches` at the LEO, committed.
    void replicate(std::vector<std::vector<uint8_t>> batches) {
        std::vector<uint8_t> bytes;
        int64_t base = leo();
        for (auto& b : batches) {
            const int64_t count = 1 + rune::protocol::RecordBatchReader(b.data(), b.size()).lastOffsetDelta();
            for (int i = 0; i < 8; ++i) b[static_cast<std::size_t>(i)] = static_cast<uint8_t>(static_cast<uint64_t>(base) >> (8 * i));
            rune::protocol::RecordBatchReader::stampLeaderEpoch(b.data(), static_cast<int32_t>(epoch_));
            bytes.insert(bytes.end(), b.begin(), b.end());
            base += count;
        }
        rune::replication::FetchResponseMsg resp{};
        resp.leaderEpoch = static_cast<rune::replication::Epoch>(epoch_);
        resp.partition = 0; resp.topicId = kSrc; resp.highWatermark = base; resp.logStartOffset = log->logStartOffset();
        resp.records = std::span<const uint8_t>(bytes.data(), bytes.size());
        replica->applyReplicated(resp);
        passes(3);
    }
    /// A constraint-set change batch (base offset 0, stamped by replicate()).
    static std::vector<uint8_t> changeBatch(uint32_t setId, std::vector<rune::storage::control_record::ChangeMember> members) {
        std::vector<uint8_t> b(rune::storage::control_record::changeBatchBytes(static_cast<uint16_t>(members.size())));
        EXPECT_EQ(rune::storage::control_record::buildConstraintSetChange(b, setId, members), b.size());
        return b;
    }

private:
    [[nodiscard]] WorkerOp specOp_(bool keyIndex) const {
        TopicWriteSpecWire w;
        w.topicId = kSrc; w.keyFieldId = 0; w.keyType = keyShape_.type; w.legacyValidate = false; w.keyIndex = keyIndex;
        w.keyWidth = static_cast<uint8_t>(keyWidthOf(keyShape_));
        WorkerOp s;
        s.type = WorkerOp::Type::WriteSpecUpdate; s.topicId = kSrc;
        s.schemaUpdateBlob.resize(kTopicWriteSpecWireLen);
        EXPECT_EQ(encodeTopicWriteSpec(w, s.schemaUpdateBlob), kTopicWriteSpecWireLen);
        return s;
    }
    void typedWith_(int16_t level, const KeyShape& shape, bool keyIndex) {
        keyShape_ = shape;
        WorkerOp l;
        l.type = WorkerOp::Type::ClusterFeatureLevel; l.nextOffset = level;
        send(std::move(l));
        const auto blob = rune::codec::serialize(keyedSourceDescriptor(shape));
        WorkerOp v;
        v.type = WorkerOp::Type::SchemaUpdate; v.topicId = kSrc; v.clientVersion = 1;
        v.schemaUpdateBlob = blob;
        send(std::move(v));
        WorkerOp f;
        f.type = WorkerOp::Type::FilterSchema; f.topicId = kSrc; f.clientVersion = 1;
        f.schemaUpdateBlob = blob;
        send(std::move(f));
        send(specOp_(keyIndex));
    }
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
    void installCountNow_(int32_t viewTopicId, const char* name) {
        const auto src = verdictSourceDescriptor();
        rune::view::ViewPlan v;
        v.viewName = name; v.key = "id"; v.sources = {{kSrc, "id"}};
        v.output = {{"id", rune::codec::PrimType::UInt64, 0, "id", rune::view::MergeRule::Replace},
                    {"n", rune::codec::PrimType::Int64, 0, "", rune::view::MergeRule::CountAll}};
        v.persistence = 1;
        const rune::codec::Descriptor* srcs[] = {&src};
        auto comp = rune::view::compileView(v, srcs, viewTopicId);
        ASSERT_TRUE(comp.has_value());
        vs->installView(comp->plan, comp->rowLayout, src, /*persisted=*/false);
    }
    void installProjectionNow_(int32_t viewTopicId, const char* name) {
        const auto src = verdictSourceDescriptor();
        rune::view::ViewPlan v;
        v.viewName = name; v.key = "id"; v.sources = {{kSrc, "id"}};
        v.output = {{"id", rune::codec::PrimType::UInt64, 0, "id", rune::view::MergeRule::Replace},
                    {"v", rune::codec::PrimType::Int64, 0, "v", rune::view::MergeRule::Replace},
                    {"tag", rune::codec::PrimType::UInt16, 0, "tag", rune::view::MergeRule::Replace}};
        v.persistence = 1;
        const rune::codec::Descriptor* srcs[] = {&src};
        auto comp = rune::view::compileView(v, srcs, viewTopicId);
        ASSERT_TRUE(comp.has_value());
        vs->installView(comp->plan, comp->rowLayout, src, /*persisted=*/false);
    }

public:

    /// `shape`: the source's key; the wire key and the value's id field are that wide (the id's little-endian bytes, zero padded).
    static std::vector<uint8_t> batch(const std::vector<Rec>& recs, int64_t pid = -1, int32_t seq = -1, const KeyShape& shape = {}, bool txn = false) {
        const uint32_t kw = keyWidthOf(shape);
        const auto idByte = [](uint64_t id, uint32_t i) { return static_cast<uint8_t>(i < 8 ? id >> (8 * i) : 0); };
        rune::protocol::RecordBatchBuilder b(0, 1000);
        if (pid >= 0) { b.setProducerId(pid); b.setProducerEpoch(0); b.setBaseSequence(seq); }
        b.setTransactional(txn);
        for (const auto& r : recs) {
            std::vector<uint8_t> key(kw), val;
            for (uint32_t i = 0; i < kw; ++i) key[i] = idByte(r.id, i);
            if (r.del) { b.append({reinterpret_cast<const char*>(key.data()), key.size()}, {}, 1000, r.intent); continue; }
            const auto le16 = [&](uint16_t x) { val.push_back(uint8_t(x)); val.push_back(uint8_t(x >> 8)); };
            le16(static_cast<uint16_t>(kw + 10)); le16(1);
            for (uint32_t i = 0; i < kw; ++i) val.push_back(idByte(r.id, i));
            for (int i = 0; i < 8; ++i) val.push_back(static_cast<uint8_t>(uint64_t(r.v) >> (8 * i)));
            le16(r.tag);
            b.append({reinterpret_cast<const char*>(key.data()), key.size()}, {reinterpret_cast<const char*>(val.data()), val.size()}, 1000, r.intent);
        }
        return b.build();
    }

    /// One worker pass; the driver and the client conductor run in it too (Invoker), so the rig is single-threaded.
    int pass() {
        now += 1'000'000;
        ++passNo_;
        if (delayedSpecIn_ > 0 && --delayedSpecIn_ == 0) EXPECT_TRUE(worker->tryEnqueueOp(specOp_(true)));
        pumpAeron_();
        const int work = worker->doWork(now);
        pumpAeron_();
        pollAcks_();
        worker->drainReturn([&](WorkerReturn r) {
            if (r.type == WorkerReturn::Type::ProduceResult) produceAnswers.push_back(r.errorCode);
        }, [](const TxnReturn&) {});
        if (agent) (void)agent->doWork(now);
        else channel.deltas.drain([this](int32_t, aeron::concurrent::AtomicBuffer& buf, OneToOneByteRing::index_t off, OneToOneByteRing::index_t) {
            int32_t viewTopicId;
            std::memcpy(&viewTopicId, buf.buffer() + off, 4);
            deltaViewIds.push_back(viewTopicId);
        });
        return work;
    }
    void passes(int n) { for (int i = 0; i < n; ++i) pass(); }

    /// A produce through the worker's op path, re-sent while the partition is still building (NotLeader).
    int16_t produce(const std::vector<Rec>& recs) {
        const auto bytes = batch(recs, -1, -1, keyShape_);
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
    /// A row of the projection / verdict view: (v, tag); the verdict views have no tag column (tag = 0).  nullopt = absent.
    [[nodiscard]] std::optional<std::pair<int64_t, uint16_t>> idRow(int32_t viewTopicId, uint64_t key) const {
        const rune::view::RowLayout* L = nullptr;
        const uint8_t* r = vs->getRow(viewTopicId, 0, key, L);
        if (r == nullptr) return std::nullopt;
        const auto v = rune::view::readSlot(r, *L, 1);
        const uint16_t tag = L->slotOffset.size() > 2 ? static_cast<uint16_t>(rune::view::readSlot(r, *L, 2).u) : uint16_t{0};
        return std::make_pair(v.i, tag);
    }
    /// The count(*) view's n for an id; nullopt = no row.
    [[nodiscard]] std::optional<int64_t> countRow(int32_t viewTopicId, uint64_t id) const {
        const rune::view::RowLayout* L = nullptr;
        const uint8_t* r = vs->getRow(viewTopicId, 0, id, L);
        if (r == nullptr) return std::nullopt;
        return rune::view::readSlot(r, *L, 1).i;
    }
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
        driver_ = std::make_unique<rune::node::EmbeddedDriver>(dc, rune::node::DriverMode::Invoker);
        aeron::Context ctx; ctx.aeronDir(dc.dir); ctx.useConductorAgentInvoker(true);
        aeron_ = aeron::Aeron::connect(ctx);
    }
    void enableEgress() {
        if (egress_) return;
        ensureDriver_();
        const std::string control = "127.0.0.1:" + std::to_string(rune::aeronutil::testing::allocFreeUdpPort());
        const std::string local   = "127.0.0.1:" + std::to_string(rune::aeronutil::testing::allocFreeUdpPort());
        egress_ = std::make_unique<WorkerEgress>(aeron_, control.c_str(), 3001, driver_->pumpWith(aeron_.get()));
        const int64_t reg = aeron_->addSubscription("aeron:udp?control=" + control + "|control-mode=dynamic|endpoint=" + local, 3001);
        ASSERT_TRUE(egress_->resolved());
        while (!(sub_ && sub_->isConnected() && egress_->isConnected())) {
            if (!sub_) sub_ = aeron_->findSubscription(reg);
            pass();
        }
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
            for (;;) {
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
        // The subscription cursor reached what the HW and the judge allow, the worker is idle, the image has read everything the egress
        // published and nothing new was polled.
        for (;;) {
            const int work = pass();
            const std::size_t before = box.frames.size();
            sub_->poll(aeron::FragmentAssembler([&](aeron::concurrent::AtomicBuffer& buf, aeron::util::index_t off, aeron::util::index_t len, aeron::Header&) {
                const auto dec = rune::protocol::client::Records::decode(reinterpret_cast<const uint8_t*>(buf.buffer() + off), static_cast<std::size_t>(len));
                if (dec && dec->subscriptionId == subId) absorb_(box, dec->recordBatch.data(), dec->recordBatch.size());
            }).handler(), 64);
            if (work == 0 && box.frames.size() == before && worker->subscriptionPositionForTest(subId) >= deliverable_() && delivered_()) break;
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
    void enableAcks(int32_t clientVersion = 17, uint32_t arenaEntries = 0, bool encrypted = false) {
        ensureDriver_();
        ackEncrypted_ = encrypted;
        if (encrypted) {
            rune::crypto::AeadKey c2b{}, b2c{};
            for (std::size_t i = 0; i < sizeof(c2b.bytes); ++i) { c2b.bytes[i] = static_cast<uint8_t>(i * 5 + 1); b2c.bytes[i] = static_cast<uint8_t>(i * 7 + 3); }
            std::memcpy(c2bKey_, c2b.bytes, sizeof(c2bKey_));
            std::memcpy(b2cKey_, b2c.bytes, sizeof(b2cKey_));
            c2bSeal_ = std::make_unique<rune::crypto::AeadCipher>(rune::crypto::Cipher::Aes256Gcm, c2b, kC2bPrefix);
            b2cOpen_ = std::make_unique<rune::crypto::AeadCipher>(rune::crypto::Cipher::Aes256Gcm, b2c, kB2cPrefix);
        }
        ackVersion_ = clientVersion;
        arenaEntries_ = arenaEntries;
        if (!ackIngress_) {
            ackIngress_ = std::make_unique<WorkerIngress>(aeron_, "aeron:ipc", kIngressStream, driver_->pumpWith(aeron_.get()));
            ASSERT_TRUE(ackIngress_->resolved());
            const int64_t sreg = aeron_->addSubscription("aeron:ipc", kAckStream);
            const int64_t preg = aeron_->addExclusivePublication("aeron:ipc", kIngressStream);
            while (!(ackSub_ && ackPub_ && ackPub_->isConnected())) {
                if (!ackSub_) ackSub_ = aeron_->findSubscription(sreg);
                if (!ackPub_) ackPub_ = aeron_->findExclusivePublication(preg);
                pass();
            }
        }
        wireAcks_();
    }
    /// A Produce frame on the worker's ingress; returns its correlation id.  `recs` is one batch.
    int64_t produceFrame(const std::vector<Rec>& recs, uint8_t acks, int64_t pid = -1, int32_t seq = -1, bool txn = false) {
        const auto bytes = batch(recs, pid, seq, keyShape_, txn);
        const int64_t c = ++corr;
        rune::protocol::client::Produce m;
        m.sessionId = kAckSession; m.correlationId = c; m.topicId = kSrc; m.partition = 0; m.partitionKnown = 1;
        m.acks = acks; m.producerId = pid; m.producerEpoch = 0; m.baseSequence = seq;
        m.recordBatch = std::span<const uint8_t>(bytes);
        std::vector<uint8_t> f(m.encodedSize()); m.encode(f.data());
        if (ackEncrypted_) f = sealC2b_(f);
        aeron::concurrent::AtomicBuffer ab(f.data(), static_cast<aeron::util::index_t>(f.size()));
        int64_t pos;
        while ((pos = ackPub_->offer(ab, 0, static_cast<aeron::util::index_t>(f.size()))) < 0 && pos >= aeron::ADMIN_ACTION) pass();   // back-pressure / admin: let the worker drain
        EXPECT_GT(pos, 0) << "the produce frame was refused: " << pos;
        return c;
    }
    /// The same batch the way a gateway routes it: a Produce op on the op ring, answered through the ack sink.
    int64_t routedFrame(const std::vector<Rec>& recs, uint8_t acks, int64_t pid = -1, int32_t seq = -1) {
        const auto bytes = batch(recs, pid, seq, keyShape_);
        WorkerOp op;
        op.type = WorkerOp::Type::Produce; op.topicId = kSrc; op.partition = 0;
        op.replica = replica.get(); op.logPtr = log.get();
        op.sessionId = kAckSession; op.correlationId = ++corr; op.acks = acks;
        op.producerId = pid; op.producerEpoch = 0; op.baseSequence = seq;
        op.lastOffsetDelta = static_cast<int32_t>(recs.size()) - 1; op.recordCount = static_cast<int32_t>(recs.size());
        op.durabilityTier = replica->durabilityTier();
        op.deferAck = (acks & rune::protocol::client::kAcksWireAll) != 0;
        op.recordBatchView = std::span<const uint8_t>(bytes);
        EXPECT_TRUE(worker->tryEnqueueOp(std::move(op)));
        return corr;
    }
    /// A transaction coordinator's COMMIT / ABORT marker for `pid`.
    void marker(int64_t pid, bool commit) {
        WorkerOp op;
        op.type = WorkerOp::Type::AppendControlRecord; op.topicId = kSrc; op.partition = 0; op.replica = replica.get();
        op.logPtr = log.get(); op.correlationId = 9000 + pid; op.producerId = pid;
        op.targetReplicaId = replica->registryId(); op.incarnation = replica->incarnation();
        op.recordBatchBytes = rune::transaction::ControlMarker::build(
            pid, 0, commit ? rune::transaction::ControlType::Commit : rune::transaction::ControlType::Abort, 0);
        ASSERT_TRUE(worker->tryEnqueueOp(std::move(op)));
        passes(3);
    }
    /// Drives until the ack of `corr` arrived (or `maxPasses`).
    std::optional<Ack> ack(int64_t c, int maxPasses = 600) {
        for (int i = 0; i < maxPasses; ++i) {
            if (const auto it = acksSeen_.find(c); it != acksSeen_.end()) return it->second;
            pass();
        }
        if (const auto it = acksSeen_.find(c); it != acksSeen_.end()) return it->second;
        return std::nullopt;
    }
    /// Like ack(), but the follower fetches along the way (a rig with a follower holds the HW otherwise).
    std::optional<Ack> ackFetching(int64_t c, int maxPasses = 3000) {
        for (int i = 0; i < maxPasses; ++i) {
            if (const auto it = acksSeen_.find(c); it != acksSeen_.end()) return it->second;
            if (isr_.size() > 1 && i % 8 == 0) followerFetch();
            pass();
        }
        if (const auto it = acksSeen_.find(c); it != acksSeen_.end()) return it->second;
        return std::nullopt;
    }
    /// Passes with no waiting for a particular ack (a delay check).
    void settle(int n = 40) { for (int i = 0; i < n; ++i) pass(); }
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
    struct RigLogRec { int64_t offset; uint64_t id; int64_t v; uint8_t tag; bool del; uint8_t intent = 0; };
    struct RigChange { int64_t offset; std::vector<rune::storage::control_record::ChangeMember> members; };
    struct RigMarker { int64_t offset; rune::storage::control_record::MarkerHeader header; std::vector<std::tuple<int64_t, uint16_t, uint32_t>> entries; bool horizon = false; };
    struct RigLog { std::vector<RigLogRec> data; std::vector<RigChange> changes; std::vector<RigMarker> markers; };

    /// Every data record, change record and marker below `through`, in offset order, straight from the segments.
    [[nodiscard]] RigLog readLog(int64_t through = INT64_MAX) const;

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
    /// committed.  false = it did not within `maxPasses` passes (a Strict append waits for an fsync that takes wall-clock time).
    bool driveUntilJudged(int64_t target = -1, int maxPasses = kMaxPass) {
        if (target < 0) target = dataEnd();
        for (int i = 0; i < maxPasses; ++i) {
            const VerdictSource* judge = vs->verdictSource(kSrc, 0);
            const PartitionWriteState* w = writeState();
            if ((judge == nullptr || judge->judgedThrough() >= target) && w != nullptr && w->markers.committedThrough() >= target) return true;   // no judge: the last member left
            if (isr_.size() > 1 && i % 8 == 0) followerFetch();
            pass();
        }
        const VerdictSource* judge = vs->verdictSource(kSrc, 0);
        const PartitionWriteState* w = writeState();
        ADD_FAILURE() << "judgedThrough " << (judge ? judge->judgedThrough() : -9) << " committed " << (w ? w->markers.committedThrough() : -9) << " target " << target << " leo " << leo() << " hw " << replica->highWatermark() << " waiting " << (judge ? judge->waitingForInstall() : -1) << " stalled " << (judge ? judge->stalled() : -1) << " stalls " << (stats() ? stats()->verdictStalls : 0) << " waitsInstall " << (stats() ? stats()->judgeWaitsForInstall : 0) << " markers " << (w ? w->markers.appendedThrough() : -9);
        return false;
    }

    /// Drives until the view's instance folded through `target` (and serves).
    void driveUntilFolded(int32_t viewTopicId, int64_t target) {
        for (;;) {
            const auto st = vs->instanceServeState(viewTopicId, 0);
            if (st.hosted && st.ready && st.foldCursor >= target) return;
            pass();
        }
    }

    // ---- compaction ----
    struct Compacted { int64_t bound = 0; int64_t passEnd = 0; int installed = 0; std::size_t groups = 0; };
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
        out.groups = res.groups.size();
        for (const auto& g : res.groups) {
            std::vector<rune::storage::Log::CleanedMember> members;
            for (const int64_t gb : g.bases) {
                uint64_t lineage = rune::storage::Log::kAnyLineage;
                for (const auto& si : *snap) if (si.baseOffset == gb) lineage = si.lineage;
                members.push_back({gb, lineage});
            }
            auto prebuilt = g.hasOutput ? log->openCleanedOutput(g.bases.front(), g.seq, g.nextOffset) : nullptr;
            bool queued = false;
            for (int tries = 0; !queued && tries < kMaxPass; ++tries) {   // a full swap ring (256 slots) is drained by the owner, one group per call
                queued = log->enqueueCleanedGroup(members.data(), members.size(), g.seq, g.hasOutput, g.nextOffset, prebuilt.get());
                if (!queued) out.installed += replica->drainCleanerSwapsOnOwner();
            }
            EXPECT_TRUE(queued);
            if (queued) (void)prebuilt.release();   // the owner owns it now
        }
        if (drain) out.installed += drainSwaps();
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
    std::vector<int32_t> deltaViewIds;   ///< the view topic of every checkpoint frame the worker pushed (only without a persisted view's agent)

private:
    void pumpAeron_() {
        if (!driver_) return;
        driver_->doWork();
        aeron_->conductorAgentInvoker().invoke();
    }
    /// The offset below which a consumer may be handed records: the HW, and for a verdict source the judge's cursor.
    [[nodiscard]] int64_t deliverable_() const {
        const PartitionWriteState* w = writeState();
        return std::min({leo(), replica->highWatermark(), w != nullptr ? w->verdictKnownThrough() : INT64_MAX});
    }
    /// Every egress frame the worker published has been read off the image.
    [[nodiscard]] bool delivered_() const {
        if (!sub_ || !egress_ || egress_->pub() == nullptr) return true;
        const auto image = sub_->imageBySessionId(egress_->pub()->sessionId());
        return image != nullptr && image->position() == egress_->pub()->position();
    }
    void absorb_(Inbox& box, const uint8_t* data, std::size_t len) {
        const uint32_t kw = keyWidthOf(keyShape_), idBytes = std::min<uint32_t>(kw, 8);
        for (std::size_t pos = 0; pos + 12 <= len;) {
            rune::protocol::RecordBatchReader r(data + pos, len - pos);
            const int32_t total = r.totalLength();
            if (total <= 0 || pos + static_cast<std::size_t>(total) > len) break;
            box.frames.push_back(Frame{r.baseOffset(), r.recordCount(), r.lastOffsetDelta()});
            (void)rune::protocol::forEachRecordSpan(data + pos, static_cast<std::size_t>(total), [&](const rune::protocol::RecordSpan& rs) {
                uint64_t id = 0;
                if (rs.value.empty()) {   // a tombstone: the key carries the id
                    for (uint32_t i = 0; i < idBytes; ++i) id |= uint64_t(uint8_t(rs.key[i])) << (8 * i);
                    box.records.push_back(Delivered{rs.offset, id, kTombstoneTag, static_cast<uint8_t>(rs.attributes & 0x03)});
                    return;
                }
                const auto* v = reinterpret_cast<const uint8_t*>(rs.value.data());
                for (uint32_t i = 0; i < idBytes; ++i) id |= uint64_t(v[4 + i]) << (8 * i);
                box.records.push_back(Delivered{rs.offset, id, static_cast<uint16_t>(v[4 + kw + 8] | (v[4 + kw + 9] << 8)), static_cast<uint8_t>(rs.attributes & 0x03)});
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
        if (ackEncrypted_) {
            a.sessionEncrypted = true; a.senderKeyValid = true; a.c2bKeyValid = true;
            a.senderCipher = static_cast<uint8_t>(rune::crypto::Cipher::Aes256Gcm);
            std::memcpy(a.senderKey, b2cKey_, sizeof(a.senderKey)); std::memcpy(a.senderKeyPrefix, kB2cPrefix, sizeof(a.senderKeyPrefix));
            std::memcpy(a.c2bKey, c2bKey_, sizeof(a.c2bKey)); std::memcpy(a.c2bKeyPrefix, kC2bPrefix, sizeof(a.c2bKeyPrefix));
        }
        EXPECT_TRUE(worker->tryEnqueueOp(std::move(a)));
    }
    std::vector<uint8_t> sealC2b_(const std::vector<uint8_t>& plain) {
        namespace pc = rune::protocol::client;
        const uint64_t sq = ++c2bSeq_;
        std::vector<uint8_t> out(plain.size() + pc::kCryptoOverhead);
        std::memcpy(out.data(), plain.data(), pc::MessageHeader::ENCODED_SIZE);
        pc::CryptoSubHeader sub;
        sub.version = 1;
        sub.senderId = 1;   // the receiver: worker 0
        sub.seq = sq;
        sub.encode(out.data() + pc::MessageHeader::ENCODED_SIZE);
        const std::size_t n = c2bSeal_->seal(sq, out.data(), pc::kCryptoHeaderBytes, plain.data() + pc::MessageHeader::ENCODED_SIZE,
                                             plain.size() - pc::MessageHeader::ENCODED_SIZE, out.data() + pc::kCryptoHeaderBytes);
        out.resize(pc::kCryptoHeaderBytes + n);
        return out;
    }
    void pollAcks_() {
        if (!ackSub_) return;
        ackSub_->poll(aeron::FragmentAssembler([&](aeron::concurrent::AtomicBuffer& b, aeron::util::index_t o, aeron::util::index_t l, aeron::Header&) {
            const uint8_t* wire = reinterpret_cast<const uint8_t*>(b.buffer() + o);
            std::size_t wireLen = static_cast<std::size_t>(l);
            std::vector<uint8_t> plain;
            if (ackEncrypted_) {
                namespace pc = rune::protocol::client;
                const auto sub = pc::CryptoSubHeader::decode(wire + pc::MessageHeader::ENCODED_SIZE, wireLen - pc::MessageHeader::ENCODED_SIZE);
                if (!sub) return;
                plain.resize(wireLen);
                const long n = b2cOpen_->open(sub->seq, wire, pc::kCryptoHeaderBytes, wire + pc::kCryptoHeaderBytes, wireLen - pc::kCryptoHeaderBytes, plain.data() + pc::MessageHeader::ENCODED_SIZE);
                if (n < 0) return;
                std::memcpy(plain.data(), wire, pc::MessageHeader::ENCODED_SIZE);
                plain.resize(pc::MessageHeader::ENCODED_SIZE + static_cast<std::size_t>(n));
                wire = plain.data();
                wireLen = plain.size();
            }
            const auto r = rune::protocol::client::ProduceResponse::decode(wire, wireLen);
            if (!r) return;
            Ack a;
            a.code = r->errorCode; a.baseOffset = r->baseOffset; a.hasVerdicts = r->hasVerdicts; a.frameLen = static_cast<std::size_t>(l) - (ackEncrypted_ ? rune::protocol::client::kCryptoOverhead : 0); a.step = passNo_;
            r->forEachVerdict([&](uint16_t i, uint16_t code, uint32_t cid) { a.verdicts.emplace_back(i, code, cid); });
            if (acksSeen_.emplace(r->correlationId, std::move(a)).second) ackOrder_.push_back(r->correlationId);
        }).handler(), 64);
    }
    std::unique_ptr<WorkerIngress> ackIngress_;
    std::shared_ptr<aeron::Subscription> ackSub_;
    std::shared_ptr<aeron::ExclusivePublication> ackPub_;
    std::map<int64_t, Ack> acksSeen_;
    std::vector<int64_t> ackOrder_;
    static constexpr uint8_t kC2bPrefix[4] = {9, 8, 7, 6}, kB2cPrefix[4] = {1, 2, 3, 4};
    bool ackEncrypted_ = false;
    uint8_t c2bKey_[32] = {}, b2cKey_[32] = {};
    uint64_t c2bSeq_ = 0;
    std::unique_ptr<rune::crypto::AeadCipher> c2bSeal_, b2cOpen_;
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
        c.topicId = kSrc; c.partitionId = 0; c.selfId = 1; c.minIsr = tuning_.minIsr; c.durabilityTier = tuning_.durabilityTier;
        c.replicaLagMaxMs = 1'000'000; c.replicaLagMaxOffset = 1'000'000;
        c.fetchIntervalNs = 1'000'000'000LL; c.isrCheckIntervalNs = 1'000'000'000LL;
        c.maxFetchBytes = 1 << 20; c.pushOnAppend = false;
        replica = std::make_unique<rune::replication::PartitionReplica>(c, rune::replication::Role::Leader, 1, isr_, isr_, log.get(), &transport_);
        worker = std::make_unique<Worker>(0);
        worker->setFilterConfig(filter::FilterConfig{});
        if (tuning_.ackTimeoutNs != 0) worker->setAckTimeoutNs(tuning_.ackTimeoutNs);
        worker->setOwnedDirect(Worker::OwnedSet{replica.get()});
        worker->setViewSet(vs.get());
        if (rcScanBytesPerPass != 0) worker->setRcScanBytesPerPass(rcScanBytesPerPass);
        if (egress_) worker->setWorkerEgress(egress_.get());
        wireAcks_();
    }

    std::vector<rune::replication::NodeId> isr_;
 public:
    int32_t rcScanBytesPerPass = 0;   ///< the leadership scan budget of a (re)started worker; 0 = the default
 private:
    int64_t rollBytes_;
    RigTuning tuning_;
    uint64_t cleanedSeq_ = 0;
    std::vector<std::pair<int32_t, std::function<void()>>> installs_;   ///< (view topic, install): replayed by a restart
    int64_t epoch_ = 1;
    bool recovering_ = false;   ///< the views of a restarted node load their checkpoints
    KeyShape keyShape_;
    bool keyIndexOn_ = false, holdJudge_ = false;
    int delayedSpecIn_ = 0;
    std::filesystem::path viewDir_, logDir_;
    NullPartitionTransport transport_;
};

/// Every data record, change record and marker of `log` below `through`, in offset order (the log's own thread reads it).
inline VerdictRig::RigLog readLogOf(rune::storage::Log& log, int64_t through = INT64_MAX) {
    const int64_t leo = log.logEndOffset();
    namespace crl = rune::storage::control_record;
    VerdictRig::RigLog out;
    for (int64_t off = log.logStartOffset(); off < std::min<int64_t>(leo, through);) {
        const auto v = log.readView(off, 1 << 20);
        if (v.empty()) {   // a compacted gap: continue at the next segment
            const auto bases = log.segmentBaseOffsets();
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
                    const std::size_t kw = rs.key.size();   // the wire key is the id, kw bytes wide
                    for (std::size_t i = 0; i < std::min<std::size_t>(kw, 8); ++i) id |= uint64_t(uint8_t(rs.key[i])) << (8 * i);
                    const auto intent = static_cast<uint8_t>(rs.attributes & 0x03);
                    if (rs.value.empty()) { out.data.push_back({rs.offset, id, 0, 0, true, intent}); return; }
                    const auto* val = reinterpret_cast<const uint8_t*>(rs.value.data());
                    uint64_t x = 0;
                    for (int i = 0; i < 8; ++i) x |= uint64_t(val[4 + kw + i]) << (8 * i);
                    out.data.push_back({rs.offset, id, static_cast<int64_t>(x), static_cast<uint8_t>(val[4 + kw + 8]), false, intent});
                });
            } else if (r.baseOffset() < through) {
                const int t = crl::recordType(b, total);
                if (t == crl::kConstraintSetChange) {
                    std::array<crl::ChangeMember, 32> ms{};
                    uint32_t setId = 0; uint16_t n = 0;
                    if (crl::parseConstraintSetChange(b, total, setId, ms, n)) out.changes.push_back({r.baseOffset(), std::vector<crl::ChangeMember>(ms.begin(), ms.begin() + n)});
                } else if (t == crl::kVerdictMarker) {
                    VerdictRig::RigMarker m{r.baseOffset(), *crl::parseMarkerHeader(b, total), {}, r.hasDeleteHorizon()};
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

inline VerdictRig::RigLog VerdictRig::readLog(int64_t through) const { return readLogOf(*log, through); }

}  // namespace rune::node::cw
