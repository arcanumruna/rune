#pragma once
// FilterMatrixRig.h - Phase 2c Task 14: the rig of the worker-level filter interaction matrix.
//
// One Rig per cell: an embedded UDP Aeron driver, a WorkerEgress + subscriber, a source (a real disk Log or a transient
// InMemoryLog ring) behind a PartitionReplica (RU: read_uncommitted over the high watermark; RC: read_committed over the
// last stable offset), one or more real Workers, a ModelClient that applies the frames the way the real client does
// (record offsets, whole-batch straddle drops, position notes) and an expected-set model computed from the records the
// rig appended.  The matrix test (WorkerFilterMatrixTest.cpp) drives the timing scripts and reads every expectation from
// FilterMatrixExpectations.h.
#include "support/AllocWindow.h"
#include "support/FilterFakes.h"
#include "support/FilterMatrixExpectations.h"

#include <rune/aeronutil/testing/FreePort.h>
#include <rune/codec/Filter.h>
#include <rune/codec/FilterPredicate.h>
#include <rune/codec/Serialize.h>
#include <rune/node/EmbeddedDriver.h>
#include <rune/node/Worker.h>
#include <rune/node/WorkerOp.h>
#include <rune/protocol/Crc32c.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/protocol/client/DataMessages.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/storage/InMemoryLog.h>
#include <rune/storage/Log.h>
#include <rune/transaction/ControlMarker.h>

#include <Aeron.h>
#include <FragmentAssembler.h>
#include <Subscription.h>
#include <concurrent/AtomicBuffer.h>
#include <gtest/gtest.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstring>
#include <deque>
#include <filesystem>
#include <map>
#include <memory>
#include <set>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

namespace rune::node::filtermatrix {

using namespace std::chrono_literals;
using namespace rune::node::filter::fakes;
namespace pc = rune::protocol::client;

inline constexpr int32_t kTopic = 10;
inline constexpr std::size_t kRingBytes = 512 * 1024;

[[nodiscard]] inline int64_t nowNs() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch()).count();
}

// ---------------------------------------------------------------------------------------------------------------
// Members: the filters of the five ops
// ---------------------------------------------------------------------------------------------------------------
enum class MK : uint8_t { Unfiltered, WorldEq, ChannelIn, WorldEqChannelIn, PlayerIn };

inline constexpr uint32_t kWorld = 7;
inline constexpr uint64_t kPlayer1 = 1001, kPlayer2 = 1002;

struct FilterBuf {
    rune::codec::FilterBytes fb;
    std::size_t n = 0;
    [[nodiscard]] std::span<const uint8_t> view() const { return {fb.bytes.data(), n}; }
};
[[nodiscard]] inline FilterBuf filterFor(MK k) {
    using rune::codec::PrimType;
    FilterBuf f;
    rune::codec::FilterWriter w(std::span<uint8_t>(f.fb.bytes.data(), f.fb.bytes.size()));
    static const std::vector<int32_t> chans{1, 3};
    static const std::vector<uint64_t> players{kPlayer1, kPlayer2};
    switch (k) {
    case MK::Unfiltered: return f;
    case MK::WorldEq: w.term(1, PrimType::UInt32, 4, rune::Match<uint32_t>(kWorld)); break;
    case MK::ChannelIn:
        w.term(2, PrimType::Int32, 4, rune::Match<int32_t>(rune::anyOf(std::span<const int32_t>(chans))));
        break;
    case MK::WorldEqChannelIn:
        w.term(1, PrimType::UInt32, 4, rune::Match<uint32_t>(kWorld));
        w.term(2, PrimType::Int32, 4, rune::Match<int32_t>(rune::anyOf(std::span<const int32_t>(chans))));
        break;
    case MK::PlayerIn:
        w.term(0, PrimType::UInt64, 8, rune::Match<uint64_t>(rune::anyOf(std::span<const uint64_t>(players))));
        break;
    }
    auto n = w.finish();
    if (!n || *n == 0) throw std::runtime_error("filter encode failed");
    f.n = *n;
    return f;
}

[[nodiscard]] inline std::vector<MK> membersOf(Op o) {
    switch (o) {
    case Op::Unfiltered: return {MK::Unfiltered};
    case Op::Eq: return {MK::WorldEq};
    case Op::In: return {MK::ChannelIn};
    case Op::EqAndIn: return {MK::WorldEqChannelIn};
    case Op::MixedIndexFields: return {MK::WorldEq, MK::PlayerIn, MK::Unfiltered};
    }
    return {};
}

// ---------------------------------------------------------------------------------------------------------------
// Rows: M matches every filter; W only the world; C only the channel; N nothing; P only the player filter
// ---------------------------------------------------------------------------------------------------------------
[[nodiscard]] inline PlayerRow rowM() { return PlayerRow{.playerId = kPlayer1, .worldId = kWorld, .channel = 1, .guild = 3}; }
[[nodiscard]] inline PlayerRow rowW() { return PlayerRow{.playerId = 5, .worldId = kWorld, .channel = 2, .guild = 3}; }
[[nodiscard]] inline PlayerRow rowC() { return PlayerRow{.playerId = 6, .worldId = 1, .channel = 3, .guild = 3}; }
[[nodiscard]] inline PlayerRow rowN() { return PlayerRow{.playerId = 7, .worldId = 1, .channel = 2, .guild = 3}; }
[[nodiscard]] inline PlayerRow rowP() { return PlayerRow{.playerId = kPlayer2, .worldId = 2, .channel = 4, .guild = 3}; }
[[nodiscard]] inline std::vector<PlayerRow> rep(const PlayerRow& r, int n) { return std::vector<PlayerRow>(static_cast<std::size_t>(n), r); }

// ---------------------------------------------------------------------------------------------------------------
// The model: every appended record and batch
// ---------------------------------------------------------------------------------------------------------------
enum class TxnState : uint8_t { Open, Committed, Aborted };

struct Rec {
    int64_t off = 0;
    PlayerRow row;
    uint16_t ver = 2;
    int64_t pid = -1;      ///< >= 0: a transactional record of that producer
    int batch = 0;
};
struct BatchRec {
    int64_t base = 0, last = 0;
    std::vector<uint8_t> bytes;   ///< empty for control batches the worker built
    bool control = false;
    int64_t pid = -1;
    uint16_t ver = 2;
};

[[nodiscard]] inline bool hasChannel(uint16_t ver) { return ver < 3; }   // v3 drops channel (v4 has none either)
[[nodiscard]] inline bool matches(MK k, const Rec& r) {
    const bool chan = hasChannel(r.ver) && (r.row.channel == 1 || r.row.channel == 3);
    switch (k) {
    case MK::Unfiltered: return true;
    case MK::WorldEq: return r.row.worldId == kWorld;
    case MK::ChannelIn: return chan;
    case MK::WorldEqChannelIn: return r.row.worldId == kWorld && chan;
    case MK::PlayerIn: return r.row.playerId == kPlayer1 || r.row.playerId == kPlayer2;
    }
    return false;
}

// ---------------------------------------------------------------------------------------------------------------
// What the egress delivers, in arrival order
// ---------------------------------------------------------------------------------------------------------------
struct Collector {
    struct Frame { int64_t subId, base; std::vector<uint8_t> bytes; int64_t atNs; };
    std::vector<Frame> frames;
    std::vector<std::string> errors;
    aeron::FragmentAssembler fa;
    Collector()
        : fa([this](aeron::concurrent::AtomicBuffer& buf, aeron::util::index_t off, aeron::util::index_t len, aeron::Header&) {
              const auto* p = reinterpret_cast<const uint8_t*>(buf.buffer() + off);
              const auto n = static_cast<std::size_t>(len);
              if (auto r = pc::Records::decode(p, n))
                  frames.push_back({r->subscriptionId, r->baseOffset, {r->recordBatch.begin(), r->recordBatch.end()}, nowNs()});
              else if (auto e = pc::ErrorResponse::decode(p, n))
                  errors.push_back(std::string(e->message));
          }) {}
};

/// The client side of one subscription: applies frames the way the real client does.
struct MemberView {
    int64_t start = 0;                       ///< the position the subscription asked for
    int64_t pos = 0;                         ///< max(last delivered + 1, batch end, note offset)
    std::vector<int64_t> delivered;
    std::vector<std::vector<uint8_t>> dataFrames;
    std::vector<int64_t> dataFrameBases;
    std::vector<int64_t> notes;
    std::vector<int64_t> noteAtNs;
    std::vector<std::string> violations;
    std::size_t straddleDrops = 0;
    bool any = false;
};

[[nodiscard]] inline bool crcOk(const std::vector<uint8_t>& b) {
    using namespace rune::protocol::batch;
    if (b.size() < static_cast<std::size_t>(kRecordBatchOverhead)) return false;
    const uint32_t stored = rune::protocol::detail::getLE32(b.data() + kCrcOffset);
    return stored == rune::protocol::crc32c(b.data() + kAttributesOffset, b.size() - kAttributesOffset);
}

// ---------------------------------------------------------------------------------------------------------------
// The source: a real disk Log or a transient ring, behind one PartitionReplica
// ---------------------------------------------------------------------------------------------------------------
struct NullTransport final : rune::replication::PartitionTransport {
    bool sendFetchRequest(const rune::replication::FetchRequestMsg&) override { return true; }
    bool sendFetchResponse(rune::replication::NodeId, const rune::replication::FetchResponseMsg&) override { return true; }
};
[[nodiscard]] inline rune::replication::PartitionConfig leaderCfg() {
    rune::replication::PartitionConfig c;
    c.topicId = kTopic; c.partitionId = 0; c.selfId = 1; c.minIsr = 1; c.durabilityTier = 1;
    c.fetchIntervalNs = 1'000'000'000LL; c.isrCheckIntervalNs = 1'000'000'000LL; c.pushOnAppend = false;
    c.replicaLagMaxMs = 1'000'000;
    return c;
}

struct Member {
    int64_t sub = 0;
    MK kind = MK::Unfiltered;
    std::size_t worker = 0;
    int filterIdx = -1;   ///< index of the member inside its FilterGroup (-1: unfiltered)
};

struct RigConfig {
    std::size_t ringBytes = 1u << 26;          ///< transient ring capacity (and the disk segment limit)
    int64_t noteIntervalNs = 2'000'000;        ///< 2 ms
    int64_t versionWaitNs = 50'000'000;        ///< 50 ms
};

class Rig {
public:
    Rig(Op op, Entity ent, RigConfig cfg = {}) : op_(op), ent_(ent), cfg_(cfg) {
        static std::atomic<int> seq{0};
        id_ = seq.fetch_add(1);
        aeronDir_ = (std::filesystem::temp_directory_path() / ("rune_fmx_" + std::to_string(::getpid()) + "_" + std::to_string(id_))).string();
        std::filesystem::remove_all(aeronDir_);
        rune::node::EmbeddedDriverConfig dc;
        dc.dir = aeronDir_;
        dc.termBufferBytes = 64 * 1024;
        dc.rcvWindowBytes = 128 * 1024;
        driver_ = std::make_unique<rune::node::EmbeddedDriver>(dc, rune::node::DriverMode::Threaded);
        aeron::Context ctx;
        ctx.aeronDir(aeronDir_);
        aeron_ = aeron::Aeron::connect(ctx);
        openEgress_();
        if (transient()) {
            mem_ = std::make_unique<rune::storage::InMemoryLog>(cfg_.ringBytes, 8192);
            replica_ = std::make_unique<rune::replication::PartitionReplica>(
                leaderCfg(), rune::replication::Role::Leader, 1, std::vector<rune::replication::NodeId>{1},
                std::vector<rune::replication::NodeId>{1}, mem_.get(), &transport_);
            replica_->setRegistryHandle(3, 400);
        } else {
            dir_ = std::filesystem::temp_directory_path() / ("rune_fmx_log_" + std::to_string(::getpid()) + "_" + std::to_string(id_));
            std::filesystem::remove_all(dir_);
            std::filesystem::create_directories(dir_);
            log_ = std::make_unique<rune::storage::Log>(dir_.string(), 64LL * 1024 * 1024);
            replica_ = std::make_unique<rune::replication::PartitionReplica>(
                leaderCfg(), rune::replication::Role::Leader, 1, std::vector<rune::replication::NodeId>{1},
                std::vector<rune::replication::NodeId>{1}, log_.get(), &transport_);
        }
    }

    ~Rig() {
        workers_.clear();
        sub_.reset();
        egress_.reset();
        aeron_.reset();
        driver_.reset();
        replica_.reset();
        std::error_code ec;
        std::filesystem::remove_all(aeronDir_, ec);
        if (!dir_.empty()) std::filesystem::remove_all(dir_, ec);
    }
    Rig(const Rig&) = delete;
    Rig& operator=(const Rig&) = delete;

    [[nodiscard]] bool transient() const { return isTransient(ent_); }
    [[nodiscard]] bool rc() const { return isRc(ent_); }
    [[nodiscard]] Op op() const { return op_; }
    [[nodiscard]] Worker& w(std::size_t i = 0) { return *workers_.at(i); }
    [[nodiscard]] rune::replication::PartitionReplica& replica() { return *replica_; }
    [[nodiscard]] rune::storage::InMemoryLog* mem() { return mem_.get(); }
    [[nodiscard]] int64_t logEnd() const { return transient() ? mem_->logEndOffset() : log_->logEndOffset(); }
    [[nodiscard]] int64_t logStart() const { return transient() ? mem_->logStartOffset() : log_->logStartOffset(); }
    [[nodiscard]] const std::vector<Rec>& recs() const { return recs_; }
    [[nodiscard]] const std::vector<BatchRec>& batches() const { return batches_; }

    // ---- workers ----
    std::size_t addWorker(bool owned = true, std::optional<int64_t> noteIntervalNs = std::nullopt) {
        auto wk = std::make_unique<Worker>(0, kRingBytes, kRingBytes);
        wk->setWorkerEgress(egress_.get());
        filter::FilterConfig fc;
        fc.noteIntervalNs = noteIntervalNs.value_or(cfg_.noteIntervalNs);
        fc.versionWaitNs = cfg_.versionWaitNs;
        wk->setFilterConfig(fc);
        if (owned) wk->setOwnedDirect(Worker::OwnedSet{replica_.get()});
        workers_.push_back(std::move(wk));
        cold_.push_back(3);
        return workers_.size() - 1;
    }
    void enqueue(std::size_t wi, WorkerOp op) {
        ASSERT_TRUE(workers_.at(wi)->tryEnqueueOp(std::move(op)));
        cold_.at(wi) = 3;   // a control op: the passes that apply it are cold
    }
    void installSchema(std::size_t wi, uint16_t version) {
        WorkerOp op;
        op.type = WorkerOp::Type::FilterSchema;
        op.topicId = kTopic;
        op.clientVersion = version;
        op.schemaUpdateBlob = rune::codec::serialize(playerEvent(version));
        enqueue(wi, std::move(op));
    }
    void disown(std::size_t wi) {
        WorkerOp op;
        op.type = WorkerOp::Type::SetOwnership;   // the partition moved away: empty owned set
        enqueue(wi, std::move(op));
    }

    /// Subscribes the op's members on worker `wi` at `from`, ids idBase+1...  Returns them.
    std::vector<Member> subscribeMembers(std::size_t wi, int64_t from, int64_t idBase = 0) {
        std::vector<Member> out;
        int fi = 0;
        for (const MK k : membersOf(op_)) {
            Member m;
            m.sub = idBase + static_cast<int64_t>(out.size()) + 1;
            m.kind = k;
            m.worker = wi;
            m.filterIdx = k == MK::Unfiltered ? -1 : fi++;
            filters_.push_back(std::make_unique<FilterBuf>(filterFor(k)));
            enqueue(wi, subscribeOp_(m.sub, from, k == MK::Unfiltered ? nullptr : filters_.back().get()));
            auto& v = client_[m.sub];
            if (!v.any && v.delivered.empty() && v.notes.empty()) { v.start = from; v.pos = from; }
            out.push_back(m);
        }
        return out;
    }
    /// The warm-up pass of the cell: the engine's scratch grows once, on the first rebuild of a batch of a given size.
    /// A throwaway filtered member rebuilds one `rows`-row batch on worker `wi`, then leaves; the measured passes of the
    /// cell start after it.  Returns the log end the cell's members subscribe from.  The control cell (no filter) needs
    /// no warm-up.
    int64_t prime(std::size_t wi, uint16_t ver, int rows = 120) {
        if (op_ == Op::Unfiltered) return logEnd();
        std::vector<PlayerRow> v;
        for (int i = 0; i < rows; ++i) v.push_back(i % 2 == 0 ? rowM() : rowN());
        const int64_t from = append(v, ver);
        filters_.push_back(std::make_unique<FilterBuf>(filterFor(MK::WorldEq)));
        constexpr int64_t kPrimeSub = 99;
        WorkerOp pop = subscribeOp_(kPrimeSub, from, filters_.back().get());
        pop.isolation = 0;   // read_uncommitted: it must not build the read_committed state before the cell's own data is in
        enqueue(wi, std::move(pop));
        client_[kPrimeSub].start = client_[kPrimeSub].pos = from;
        EXPECT_TRUE(pumpUntil([&] { return client_[kPrimeSub].pos >= logEnd(); })) << "the warm-up member never caught up";
        Member pm;
        pm.sub = kPrimeSub;
        pm.worker = wi;
        unsubscribe(pm);
        settle(20ms);
        return logEnd();
    }
    /// The same subscription (same id and filter) moved to worker `wi`, resuming at `from`.
    Member resubscribe(const Member& m, std::size_t wi, int64_t from) {
        Member n = m;
        n.worker = wi;
        filters_.push_back(std::make_unique<FilterBuf>(filterFor(m.kind)));
        enqueue(wi, subscribeOp_(m.sub, from, m.kind == MK::Unfiltered ? nullptr : filters_.back().get()));
        return n;
    }
    void unsubscribe(const Member& m) {
        WorkerOp op;
        op.type = WorkerOp::Type::Unsubscribe;
        op.subscriptionId = m.sub;
        enqueue(m.worker, std::move(op));
    }

    // ---- appends ----
    int64_t append(const std::vector<PlayerRow>& rows, uint16_t ver = 2, BatchOpts o = {}) {
        const int64_t base = logEnd();
        auto bytes = makePlayerBatch(base, ver, rows, o);
        const int64_t last = base + static_cast<int64_t>(rows.size()) - 1;
        if (transient()) {
            EXPECT_GE(mem_->append(bytes, last), 0);
        } else {
            log_->append(bytes, last);
        }
        replica_->onLeaderLogAppended();
        const int bi = static_cast<int>(batches_.size());
        for (std::size_t i = 0; i < rows.size(); ++i)
            recs_.push_back({base + static_cast<int64_t>(i), rows[i], ver, o.transactional ? o.producerId : -1, bi});
        if (o.transactional && !txn_.count(o.producerId)) txn_[o.producerId] = TxnState::Open;
        batches_.push_back({base, last, std::move(bytes), false, o.transactional ? o.producerId : -1, ver});
        return base;
    }
    /// Appends `n` batches of `rows` at version `ver` (a wave).
    void appendWave(int n, const std::vector<PlayerRow>& rows, uint16_t ver = 2) { for (int i = 0; i < n; ++i) append(rows, ver); }
    int64_t appendTxn(int64_t pid, const std::vector<PlayerRow>& rows, uint16_t ver = 2) {
        BatchOpts o;
        o.transactional = true;
        o.producerId = pid;
        return append(rows, ver, o);
    }
    /// Sends a COMMIT / ABORT marker through worker `wi` and waits until it landed in the log.
    void marker(std::size_t wi, int64_t pid, bool commit) {
        const int64_t before = logEnd();
        WorkerOp op;
        op.type = WorkerOp::Type::AppendControlRecord;
        op.topicId = kTopic;
        op.partition = 0;
        op.replica = replica_.get();
        op.logPtr = transient() ? nullptr : log_.get();
        op.correlationId = pid;
        op.targetReplicaId = replica_->registryId();
        op.incarnation = replica_->incarnation();
        op.recordBatchBytes = rune::transaction::ControlMarker::build(
            pid, 0, commit ? rune::transaction::ControlType::Commit : rune::transaction::ControlType::Abort, 0);
        enqueue(wi, std::move(op));
        ASSERT_TRUE(pumpUntil([&] { return logEnd() > before; })) << "the marker never reached the log";
        batches_.push_back({before, logEnd() - 1, {}, true, pid, 2});
        txn_[pid] = commit ? TxnState::Committed : TxnState::Aborted;
    }

    // ---- the model ----
    /// RU: the log end; RC: the last stable offset (the first offset of the earliest open transaction).
    [[nodiscard]] int64_t bound() const {
        if (!rc()) return logEnd();
        int64_t lso = logEnd();
        for (const auto& b : batches_)
            if (b.pid >= 0 && !b.control) {
                const auto it = txn_.find(b.pid);
                if (it != txn_.end() && it->second == TxnState::Open) lso = std::min(lso, b.base);
            }
        return lso;
    }
    [[nodiscard]] TxnState txnState(int64_t pid) const { return txn_.at(pid); }
    [[nodiscard]] bool batchVisible(const BatchRec& b) const {
        if (b.control) return false;                       // markers are never delivered as data
        if (evictedLo_ <= b.base && b.base < evictedHi_) return false;
        if (!rc()) return true;
        if (b.last >= bound()) return false;
        return b.pid < 0 || txn_.at(b.pid) == TxnState::Committed;
    }
    [[nodiscard]] bool recVisible(const Rec& r) const {
        return batchVisible(batches_[static_cast<std::size_t>(r.batch)]) && !skipVersions_.count(r.ver);
    }
    /// The exact set the member must have delivered: matching, isolation-visible records at or after `start`.
    [[nodiscard]] std::vector<int64_t> expectedSet(MK k, int64_t start) const {
        std::vector<int64_t> v;
        for (const auto& r : recs_)
            if (r.off >= start && recVisible(r) && matches(k, r)) v.push_back(r.off);
        return v;
    }
    /// The batches an unfiltered member receives, byte for byte, in order.
    [[nodiscard]] std::vector<const BatchRec*> expectedBatches(int64_t start) const {
        std::vector<const BatchRec*> v;
        for (const auto& b : batches_)
            if (b.last >= start && batchVisible(b)) v.push_back(&b);
        return v;
    }
    /// Where the member's position must end up: filtered -> the bound (notes carry it); unfiltered -> the end of the
    /// last batch it receives.
    [[nodiscard]] int64_t targetPos(const Member& m) const {
        if (m.kind != MK::Unfiltered) return std::max(bound(), client_.at(m.sub).start);
        int64_t p = client_.at(m.sub).start;
        for (const auto* b : expectedBatches(client_.at(m.sub).start)) p = std::max(p, b->last + 1);
        return p;
    }
    void setEvicted(int64_t lo, int64_t hi) { evictedLo_ = lo; evictedHi_ = hi; }
    void skipVersion(uint16_t v) { skipVersions_.insert(v); }
    void clearSkipVersions() { skipVersions_.clear(); }

    // ---- pumping ----
    /// One pass of every worker, one poll of the subscriber, the model client applies what arrived.
    void step() {
        for (std::size_t i = 0; i < workers_.size(); ++i) stepWorker_(i);
        sub_->poll(coll_.fa.handler(), 64);
        applyFrames_();
    }
    /// A pass of every worker WITHOUT polling the subscriber (egress back-pressure).
    void stepUnpolled() { for (std::size_t i = 0; i < workers_.size(); ++i) stepWorker_(i); }
    template <typename Pred>
    bool pumpUntil(Pred p, std::chrono::milliseconds timeout = 20s) {
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        while (std::chrono::steady_clock::now() < deadline) { step(); if (p()) return true; }
        step();
        return p();
    }
    void settle(std::chrono::milliseconds d) {
        const auto deadline = std::chrono::steady_clock::now() + d;
        while (std::chrono::steady_clock::now() < deadline) { step(); std::this_thread::sleep_for(100us); }
        step();
    }
    /// Pumps until every member reached its target position, then runs a settle window so a stray frame shows up.
    [[nodiscard]] bool converge(const std::vector<Member>& ms, std::chrono::milliseconds timeout = 30s,
                                std::chrono::milliseconds quiet = 40ms) {
        const bool ok = pumpUntil([&] {
            for (const auto& m : ms) if (client_[m.sub].pos < targetPos(m)) return false;
            return true;
        }, timeout);
        settle(quiet);
        return ok;
    }

    // ---- the verdict ----
    [[nodiscard]] MemberView& view(int64_t sub) { return client_[sub]; }
    [[nodiscard]] Collector& collector() { return coll_; }

    /// Assertions 2, 3 and 4 for one member at the current model state.
    void verifyMember(const Member& m, const std::string& where) {
        const MemberView& v = client_[m.sub];
        SCOPED_TRACE("member " + std::to_string(m.sub) + " kind " + std::to_string(static_cast<int>(m.kind)) + " @ " + where);
        for (const auto& s : v.violations) ADD_FAILURE() << s;
        EXPECT_TRUE(coll_.errors.empty()) << "error frames: " << (coll_.errors.empty() ? "" : coll_.errors.front());
        EXPECT_EQ(v.pos, targetPos(m)) << "the model position";
        if (m.kind == MK::Unfiltered) {
            const auto exp = expectedBatches(v.start);
            ASSERT_EQ(v.dataFrames.size(), exp.size()) << "unfiltered frames";
            for (std::size_t i = 0; i < exp.size(); ++i) {
                EXPECT_EQ(v.dataFrames[i], exp[i]->bytes) << "unfiltered frame " << i << " is not the log batch byte for byte";
                EXPECT_EQ(v.dataFrameBases[i], exp[i]->base);
            }
            EXPECT_TRUE(v.notes.empty()) << "a position note reached an unfiltered subscription";
        } else {
            EXPECT_EQ(v.delivered, expectedSet(m.kind, v.start)) << "the delivered set";
        }
    }
    void verifyAll(const std::vector<Member>& ms, const std::string& where) { for (const auto& m : ms) verifyMember(m, where); }

    /// Assertion 6 for the plain counters: zeroCopy / rebuilt / notes follow from the frames the filtered members got.
    void verifyCounters(std::size_t wi, const std::vector<Member>& ms, uint64_t absentExpected, uint64_t unknownExpected,
                        const std::string& where,
                        const std::map<int64_t, std::pair<std::size_t, std::size_t>>* from = nullptr) {
        SCOPED_TRACE("counters @ " + where);
        uint64_t zc = 0, rb = 0, notes = 0;
        std::map<int64_t, const BatchRec*> byBase;
        for (const auto& b : batches_) if (!b.control) byBase[b.base] = &b;
        for (const auto& m : ms) {
            if (m.kind == MK::Unfiltered || m.worker != wi) continue;
            const MemberView& v = client_[m.sub];
            const auto mk = from != nullptr && from->count(m.sub) ? from->at(m.sub) : std::pair<std::size_t, std::size_t>{0, 0};
            notes += v.notes.size() - mk.second;
            for (std::size_t i = mk.first; i < v.dataFrames.size(); ++i) {
                const auto it = byBase.find(v.dataFrameBases[i]);
                if (it != byBase.end() && it->second->bytes == v.dataFrames[i]) ++zc; else ++rb;
            }
        }
        const auto c = w(wi).filterCountersForTest();
        EXPECT_EQ(c.zeroCopy, zc) << "zeroCopy";
        EXPECT_EQ(c.rebuilt, rb) << "rebuilt";
        EXPECT_EQ(c.notes, notes) << "notes";
        EXPECT_EQ(c.absentExcluded, absentExpected) << "absentExcluded";
        EXPECT_EQ(c.unknownVersionExcluded, unknownExpected) << "unknownVersionExcluded";
        EXPECT_EQ(c.malformed, 0u);
        EXPECT_EQ(c.nullValueExcluded, 0u);
        EXPECT_EQ(c.overflowBatches, 0u);
    }

    /// Assertion 5: the allocations of the measured passes (hot, after the first delivery, no control op in flight).
    void verifyNoAllocation(const std::string& where) {
        EXPECT_EQ(hotAllocs_, 0u) << "heap allocations inside " << hotPasses_ << " measured passes @ " << where;
    }
    [[nodiscard]] uint64_t hotPasses() const { return hotPasses_; }

    [[nodiscard]] const filter::FilterGroup* groupOf(const Member& m) { return w(m.worker).filterGroupOfSubForTest(m.sub); }
    [[nodiscard]] bool inSync(const Member& m) {
        const auto* g = groupOf(m);
        return g != nullptr && std::holds_alternative<filter::InSync>(g->stateOf(static_cast<uint32_t>(m.filterIdx)));
    }
    [[nodiscard]] static std::size_t filteredCount(const std::vector<Member>& ms) {
        std::size_t n = 0;
        for (const auto& m : ms) n += (m.kind != MK::Unfiltered);
        return n;
    }

    void resetHotMeasure() { hotAllocs_ = 0; hotPasses_ = 0; }

private:
    void openEgress_() {
        const std::string control = "127.0.0.1:" + std::to_string(rune::aeronutil::testing::allocFreeUdpPort());
        const int32_t stream = 3500 + id_;
        egress_ = std::make_unique<WorkerEgress>(aeron_, control.c_str(), stream);
        ASSERT_TRUE(egress_->resolved());
        const std::string ch = "aeron:udp?control=" + control + "|control-mode=dynamic|endpoint=127.0.0.1:" +
                               std::to_string(rune::aeronutil::testing::allocFreeUdpPort());
        const int64_t reg = aeron_->addSubscription(ch, stream);
        const auto deadline = std::chrono::steady_clock::now() + 5s;
        while (std::chrono::steady_clock::now() < deadline && !(sub_ = aeron_->findSubscription(reg))) std::this_thread::sleep_for(1ms);
        ASSERT_NE(sub_, nullptr);
        while (std::chrono::steady_clock::now() < deadline && !(egress_->isConnected() && sub_->isConnected()))
            std::this_thread::sleep_for(1ms);
        ASSERT_TRUE(egress_->isConnected() && sub_->isConnected());
    }
    WorkerOp subscribeOp_(int64_t subId, int64_t from, const FilterBuf* f) {
        WorkerOp op;
        op.type = WorkerOp::Type::Subscribe;
        op.subscriptionId = subId;
        op.sessionId = 4200 + subId;
        op.correlationId = 9000 + subId;
        op.topicId = kTopic;
        op.partition = 0;
        op.nextOffset = from;
        op.replica = replica_.get();
        op.isolation = rc() ? 1 : 0;
        if (transient()) {
            op.logPtr = nullptr;
            op.targetReplicaId = replica_->registryId();
            op.incarnation = replica_->incarnation();   // as WorkerPool stamps it
        } else {
            op.logPtr = log_.get();
        }
        if (f != nullptr) op.filterBytes = f->view();
        return op;
    }

    void stepWorker_(std::size_t i) {
        Worker& wk = *workers_[i];
        const bool hot = cold_[i] == 0 && anyFrame_;
        if (cold_[i] > 0) --cold_[i];
        if (!hot) { wk.doWork(nowNs()); return; }
        const uint64_t skipsBefore = wk.transientCursorSkipsForTest();
        const auto cb = wk.filterCountersForTest();
        uint64_t n;
        {
            rune::testing::AllocWindow aw;
            wk.doWork(nowNs());
            n = aw.count();
        }
        // The first time an exclusion / eviction happens the engine logs one line (spdlog formats it): not a
        // steady-state pass.  Everything else in a measured pass must not allocate.
        const auto ca = wk.filterCountersForTest();
        const bool firstEvent = (skipsBefore == 0 && wk.transientCursorSkipsForTest() > 0) ||
                                (cb.absentExcluded == 0 && ca.absentExcluded > 0) ||
                                (cb.unknownVersionExcluded == 0 && ca.unknownVersionExcluded > 0);
        if (firstEvent) return;
        ++hotPasses_;
        hotAllocs_ += n;
    }

    void applyFrames_() {
        for (; consumed_ < coll_.frames.size(); ++consumed_) {
            const auto& f = coll_.frames[consumed_];
            anyFrame_ = true;
            MemberView& v = client_[f.subId];
            v.any = true;
            const std::string tag = "sub " + std::to_string(f.subId) + " frame " + std::to_string(consumed_) + ": ";
            if (f.bytes.empty()) {   // a position note
                if (f.base < v.pos) v.violations.push_back(tag + "note " + std::to_string(f.base) + " behind position " + std::to_string(v.pos));
                v.pos = std::max(v.pos, f.base);
                v.notes.push_back(f.base);
                v.noteAtNs.push_back(f.atNs);
                continue;
            }
            if (!crcOk(f.bytes)) v.violations.push_back(tag + "invalid batch CRC");
            rune::protocol::RecordBatchReader r(f.bytes.data(), f.bytes.size());
            const int64_t base = r.baseOffset();
            const int64_t end = base + r.lastOffsetDelta() + 1;
            if (base != f.base) v.violations.push_back(tag + "frame base differs from the batch base");
            if (end < v.pos) v.violations.push_back(tag + "batch end " + std::to_string(end) + " behind position " + std::to_string(v.pos));
            int64_t prev = -1;
            r.forEachRecord([&](std::string_view, std::string_view, int64_t o, int64_t) {
                if (o <= prev) v.violations.push_back(tag + "records not ascending at " + std::to_string(o));
                prev = o;
                if (o < v.start) { ++v.straddleDrops; return; }   // whole-batch straddle: the client drops it
                if (o < v.pos) { v.violations.push_back(tag + "duplicate / regressing record " + std::to_string(o)); return; }
                v.delivered.push_back(o);
                v.pos = o + 1;
            });
            v.pos = std::max(v.pos, end);
            v.dataFrames.push_back(f.bytes);
            v.dataFrameBases.push_back(f.base);
        }
    }

    Op op_;
    Entity ent_;
    RigConfig cfg_;
    int id_ = 0;
    std::string aeronDir_;
    std::filesystem::path dir_;
    std::unique_ptr<rune::node::EmbeddedDriver> driver_;
    std::shared_ptr<aeron::Aeron> aeron_;
    std::unique_ptr<WorkerEgress> egress_;
    std::shared_ptr<aeron::Subscription> sub_;
    std::unique_ptr<rune::storage::Log> log_;
    std::unique_ptr<rune::storage::InMemoryLog> mem_;
    NullTransport transport_;
    std::unique_ptr<rune::replication::PartitionReplica> replica_;
    std::deque<std::unique_ptr<Worker>> workers_;
    std::vector<int> cold_;
    std::vector<std::unique_ptr<FilterBuf>> filters_;
    Collector coll_;
    std::size_t consumed_ = 0;
    std::map<int64_t, MemberView> client_;
    std::vector<Rec> recs_;
    std::vector<BatchRec> batches_;
    std::unordered_map<int64_t, TxnState> txn_;
    int64_t evictedLo_ = 0, evictedHi_ = 0;
    std::set<uint16_t> skipVersions_;
    bool anyFrame_ = false;
    uint64_t hotAllocs_ = 0, hotPasses_ = 0;
};

}  // namespace rune::node::filtermatrix
