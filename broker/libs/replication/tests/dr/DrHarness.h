#pragma once
// ---------------------------------------------------------------------------
// DrHarness.h — E2 / DUR-1: fast, deterministic, in-process disaster-recovery
// harness for the replication data plane.
//
// What is REAL:  rune::storage::Log (files on disk, recover(), truncateTo),
//                PartitionReplica (leader/follower, ISR, HW, KIP-101 probe,
//                DR-BUG-6 completeness gate + reverse reconcile, epoch fencing),
//                ProducerStateManager (rebuild from the log after a crash),
//                FailoverElection (the controller's production election rule).
// What is MODELLED (and how):
//   * The network: one in-process message queue between per-node transport
//     endpoints.  Faults: cut a directed link (frames lost on the wire, like
//     UDP loss — the sender still sees "accepted"), isolate a node, drop the
//     next N frames of a kind, uniform delivery delay in steps.  A crashed node
//     loses every frame in flight to or from it (its Aeron images die with it).
//   * The control plane: a tiny controller holding {leader, epoch, isr} that
//     elects with the REAL rune::metadata::FailoverElection over an omniscient
//     ReplicaPositionTable (every live replica's positionReport()).  Controller
//     ISR semantics follow DynamicBalancer: the ISR only changes on a leader
//     change (failover: elected ∪ (isr ∩ live)).
//   * The produce path: the leader appends exactly as a worker does —
//     Log::appendNoSyncRestamp (page cache, no fsync; A3 default: periodic
//     fsync OFF) + PartitionReplica::onLeaderLogAppended(); an acks=all produce
//     is refused below min-ISR and ACKED once the leader's served HW passes it
//     while the same leader incarnation + epoch still leads (else it fails —
//     never acked).
//   * Crashes: CrashKind::ProcessKill — the process dies, the page cache (every
//     pwrite) survives.  CrashKind::PowerLoss — everything not fsynced is lost:
//     each segment .log is truncated to its durable extent (the max of what an
//     inline sync made durable in any incarnation — Log::syncedExtentsForTest —
//     and what survived the previous power loss).  PowerLossTorn additionally
//     leaves a torn partial batch (the first bytes of the lost region) at the
//     tail.  The leader-epoch checkpoint is written tmp+fsync+rename and so
//     always survives.
//   * Time: an injected clock advanced 1 ms per step; every drive loop is
//     bounded and returns false on a stall.  No sleeps, no threads.
//
// "Kill-all" in this harness = ProcessKill of every node: page caches survive,
// so every acks=all-acked record (on a majority's page cache) must survive.  A
// simultaneous POWER LOSS of all nodes is outside the guarantee (periodic fsync
// is off by default; durability comes from replication — the Kafka model).
// ---------------------------------------------------------------------------

#include <gtest/gtest.h>

#include <rune/replication/PartitionReplica.h>
#include <rune/replication/InternalIsrKnowledge.h>
#include <rune/metadata/FailoverElection.h>
#include <rune/consensus/AckTerm.h>
#include <rune/metadata/ReplicaPositionTable.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/storage/Log.h>
#include <rune/storage/ProducerStateManager.h>
#include <rune/storage/ProducePath.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <deque>
#include <filesystem>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <unistd.h>
#include <vector>

namespace rune::replication::dr {

using rune::storage::Log;

inline constexpr int64_t kStepNs   = 1'000'000LL;   // 1 ms per step
inline constexpr int     kTopicId  = 7;
inline constexpr int     kPartId   = 0;

enum class CrashKind : uint8_t { ProcessKill, PowerLoss, PowerLossTorn };

struct DrConfig {
    int     minIsr                  = 2;
    int64_t replicaLagMaxMs         = 50;
    int64_t fetchIntervalNs         = kStepNs;
    int64_t isrCheckIntervalNs      = kStepNs;
    int64_t followerFlushIntervalNs = 0;            // A3 default: OFF
    int64_t segmentRollBytes        = 64LL * 1024 * 1024;
    int     deliveryDelaySteps      = 0;            // extra steps a frame spends on the wire
    /// Production seal mode (worker-driven replicas: setExternalFollowerFlush →
    /// Log::setDeferSealSync): a roll queues the seal fsync for the owning
    /// worker's io backend.  The harness never completes it, so a sealed segment
    /// is NOT durable until then (worst case for a power loss).
    bool    deferSealSync           = false;
    /// Per-replica fetch budget (PartitionConfig::maxFetchBytes).  1 = every
    /// answer carries exactly ONE (oversize-exempt) batch — the regime of any
    /// batch >= maxFetchBytes.
    int32_t maxFetchBytes           = 1 << 20;
    /// Partition topic id.  >= 0: a USER partition — its leader proposes ISR
    /// changes to the harness controller (Kafka AlterPartition, as BrokerNode
    /// wires it) and the controller elects only from its committed ISR.  < 0:
    /// an INTERNAL partition — led by the consensus leader (moveLeader may pick
    /// any replica), leader-local ISR seeded with every replica, no lag shrink,
    /// consensus-dead members fenced out (see fenceDeadForInternal_).
    int32_t topicId                 = kTopicId;
};

// ---------------------------------------------------------------------------
// Network frames
// ---------------------------------------------------------------------------
enum class Kind : uint8_t { FetchReq, FetchResp, EpochReq, EpochResp, ReconReq, ReconResp, AlterIsr,
                           AlterIsrResp, InternalIsr, InternalIsrAck };
inline constexpr int kKinds = 10;
/// The harness controller's address on the modelled network (AlterIsr target).
inline constexpr NodeId kControllerAddr = 0;

struct Frame {
    Kind     kind{};
    NodeId   from = -1;
    NodeId   to   = -1;
    uint64_t due  = 0;
    FetchRequestMsg              freq{};
    FetchResponseMsg             fresp{};
    std::vector<uint8_t>         recs;     // owns fresp.records
    OffsetsForLeaderEpochReqMsg  ereq{};
    OffsetsForLeaderEpochRespMsg eresp{};
    AlterIsrMsg                  alter{};
    AlterIsrResponseMsg          alterResp{};
    InternalIsrMsg               iisr{};
    InternalIsrAckMsg            iack{};
};

class DrCluster;

/// One per node (as in production: a node's replica talks through its node's
/// transport).  Generic kind → virtual dispatch, test fakes only.
class DrEndpoint final : public PartitionTransport {
public:
    DrEndpoint(DrCluster* c, NodeId self) : c_(c), self_(self) {}
    bool sendFetchRequest(const FetchRequestMsg&) override { return true; }   // leader unknown: lost
    bool sendFetchRequestTo(NodeId leader, const FetchRequestMsg& req) override;
    bool sendFetchResponse(NodeId to, const FetchResponseMsg& resp) override;
    bool sendOffsetsForLeaderEpochRequest(NodeId to, const OffsetsForLeaderEpochReqMsg& m) override;
    bool sendOffsetsForLeaderEpochResponse(NodeId to, const OffsetsForLeaderEpochRespMsg& m) override;
    bool sendReconcileRequest(NodeId to, const FetchRequestMsg& m) override;
    bool sendReconcileResponse(NodeId to, const FetchResponseMsg& m) override;
    bool sendAlterIsr(const AlterIsrMsg& m) override;
    bool sendInternalIsr(NodeId to, const InternalIsrMsg& m) override;
    bool sendInternalIsrAck(NodeId to, const InternalIsrAckMsg& m) override;
private:
    DrCluster* c_;
    NodeId     self_;
};

struct DrNode {
    NodeId                            id = -1;
    std::string                       dir;
    std::unique_ptr<Log>              log;
    std::unique_ptr<DrEndpoint>       ep;
    std::unique_ptr<PartitionReplica> rep;
    /// INTERNAL partition: the node's durable internal-ISR knowledge (a file in
    /// its dir — survives crashes, as in production).
    std::unique_ptr<InternalIsrKnowledge> isrKnowledge;
    bool                              up = false;
    uint32_t                          incarnation = 0;
    /// The node's consensus terms (as its Election holds them): the highest
    /// candidate term seen (raised by ANY RequestVote, granted or denied), the
    /// highest term it GRANTED a vote in, and the term of the leader it
    /// follows / is.  The ack rule reads rune::consensus::ackTerm (BrokerNode
    /// wires Election::ackTermId the same way).
    int64_t                           candidateTerm  = 0;
    int64_t                           votedTerm      = 0;
    int64_t                           leadershipTerm = 0;
    std::map<std::string, int64_t>    durableFloor;   // .log path → bytes known durable
};

/// A produce the harness issued (acks=all), and what became of it.
struct Pending {
    uint64_t id;
    Offset   offset;
    NodeId   leader;
    Epoch    epoch;
    uint32_t incarnation;
};

struct IdemResult {
    enum class Status : uint8_t { Appended, Duplicate, OutOfOrder, Fenced, Refused };
    Status status = Status::Refused;
    Offset offset = -1;
};

inline std::string drUniq() {
    static std::atomic<uint64_t> seq{0};
    return std::to_string(::getpid()) + "_" + std::to_string(seq.fetch_add(1)) + "_" +
           std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
}

class DrCluster {
public:
    explicit DrCluster(int n, DrConfig cfg = {}) : cfg_(cfg) {
        root_ = (std::filesystem::temp_directory_path() / ("rune_dr_" + drUniq())).string();
        std::filesystem::create_directories(root_);
        for (int i = 1; i <= n; ++i) replicas_.push_back(i);
        nodes_.resize(static_cast<std::size_t>(n));
        for (int i = 0; i < n; ++i) {
            nodes_[i].id  = i + 1;
            nodes_[i].dir = root_ + "/n" + std::to_string(i + 1);
            std::filesystem::create_directories(nodes_[i].dir);
        }
    }
    ~DrCluster() {
        for (auto& n : nodes_) { n.rep.reset(); n.ep.reset(); n.log.reset(); }
        std::error_code ec;
        std::filesystem::remove_all(root_, ec);
    }
    DrCluster(const DrCluster&) = delete;
    DrCluster& operator=(const DrCluster&) = delete;

    // ---------------------------------------------------------------- lifecycle
    /// Boot every node (production order: replica built as Follower@0, then
    /// the metadata bridge applies the assignment) with `leader` at epoch 1.
    void start(NodeId leader = 1) {
        for (auto& n : nodes_) open_(n);
        meta_.leader = leader;
        meta_.epoch  = 1;
        meta_.isr    = replicas_;
        for (auto& n : nodes_) observeTerm_(n, 1);
        for (auto& n : nodes_) applyMetadata(n.id);
    }

    void crash(NodeId id, CrashKind kind, std::size_t tornBytes = 9) {
        DrNode& n = node(id);
        ASSERT_TRUE(n.up) << "crash of a node that is down";
        const auto ext = n.log->syncedExtentsForTest();
        n.rep.reset(); n.ep.reset(); n.log.reset(); n.isrKnowledge.reset();
        n.up = false;
        purge_(id);
        for (const auto& e : ext)
            if (e.syncedBytes > n.durableFloor[e.logPath]) n.durableFloor[e.logPath] = e.syncedBytes;
        if (kind == CrashKind::ProcessKill) return;   // page cache survives
        for (auto& f : std::filesystem::directory_iterator(n.dir)) {
            if (f.path().extension() != ".log") continue;
            const std::string p = f.path().string();
            const auto size = static_cast<int64_t>(std::filesystem::file_size(p));
            const auto it = n.durableFloor.find(p);
            const int64_t keep = std::min<int64_t>(size, it == n.durableFloor.end() ? 0 : it->second);
            int64_t newSize = keep;
            if (kind == CrashKind::PowerLossTorn && size > keep)
                newSize = std::min<int64_t>(size, keep + static_cast<int64_t>(tornBytes));
            std::filesystem::resize_file(p, static_cast<uintmax_t>(newSize));
            n.durableFloor[p] = keep;
        }
        lostOnPowerLoss_ = true;
    }

    /// Restart a crashed node: recover its log from disk and build a fresh
    /// replica (Follower@0) — the control plane applies metadata separately.
    void restart(NodeId id) {
        DrNode& n = node(id);
        ASSERT_FALSE(n.up);
        open_(n);
    }

    /// The metadata bridge: (re)apply the current assignment to node `id`.
    void applyMetadata(NodeId id) {
        DrNode& n = node(id);
        if (!n.up) return;
        if (meta_.leader == id) {
            n.rep->becomeLeader(meta_.epoch, meta_.isr, cfg_.topicId >= 0 ? meta_.partitionEpoch : -1);
        } else if (meta_.leader >= 0) {
            n.rep->becomeFollower(meta_.epoch, meta_.leader);
        }
    }

    /// Controller failover: elect with the production FailoverElection over
    /// (isr ∩ live) and the replicas' positions; bump the epoch and push the
    /// assignment to every live node.  Returns the new leader (-1 = none).
    /// `unreachable`: live nodes the controller cannot see (lost heartbeat) —
    /// neither candidates nor told about the new assignment.
    /// `positionsFrom` >= 0: the controller knows only THAT node's own position
    /// report (production: a controller sees the positions of its local
    /// replicas) — the election then prefers it.  -1 = omniscient.
    NodeId failover(std::initializer_list<NodeId> unreachable = {}, NodeId positionsFrom = -1) {
        auto reachable = [&](NodeId id) {
            return std::find(unreachable.begin(), unreachable.end(), id) == unreachable.end();
        };
        rune::metadata::ReplicaPositionTable pos;
        std::vector<int32_t> live;
        for (auto& n : nodes_) {
            if (!n.up || !reachable(n.id)) continue;
            live.push_back(n.id);
            if (capturedPositions_ || (positionsFrom >= 0 && n.id != positionsFrom)) continue;
            auto r = n.rep->positionReport();
            r.topicId = cfg_.topicId; r.partition = kPartId;
            pos.report(r);
        }
        if (capturedPositions_) pos = *capturedPositions_;
        rune::metadata::ElectionInputs in;
        in.cur.leaderNode     = rune::metadata::NO_LEADER;
        in.cur.isr            = meta_.isr;
        in.cur.replicas       = replicas_;
        in.cur.leaderEpoch    = static_cast<int32_t>(meta_.epoch);
        in.cur.partitionEpoch = 0;
        in.liveNodes          = live;
        in.positions          = &pos;
        const auto out = rune::metadata::FailoverElection::elect(cfg_.topicId, kPartId, in);
        ++meta_.epoch;
        ++meta_.partitionEpoch;
        if (out.change.newLeader == rune::metadata::NO_LEADER ||
            out.change.newLeader == rune::metadata::NO_LEADER_CHANGE) {
            meta_.leader = -1;
            return -1;
        }
        meta_.leader = out.change.newLeader;
        meta_.isr.assign(out.change.newIsr.begin(), out.change.newIsr.end());
        for (auto& n : nodes_) if (reachable(n.id)) applyMetadata(n.id);
        return meta_.leader;
    }

    /// The controller's position table as production feeds it: node `from`'s
    /// PUBLISHED controller snapshot (PartitionReplica::controllerReplicaLeo —
    /// a leader publishes its view of every follower) is reported into a table
    /// the controller keeps (latest wins), and every later failover() elects
    /// over that table instead of the omniscient one.  Call again to refresh.
    void controllerLearnPositionsFrom(NodeId from) {
        if (!capturedPositions_) capturedPositions_.emplace();
        DrNode& n = node(from);
        if (!n.up) return;
        const int32_t ep = n.rep->controllerLeaderEpoch();
        if (ep < 0) return;
        for (NodeId id : replicas_) {
            const Offset leo = n.rep->controllerReplicaLeo(id);
            if (leo < 0) continue;
            capturedPositions_->report(ReplicaPositionReport{cfg_.topicId, kPartId, id, ep, leo});
        }
    }

    /// Controller: hand leadership to `id` explicitly (preferred-leader move /
    /// operator action), epoch+1, ISR unchanged.
    /// USER partition: Kafka preferred / controlled leader move — refused (false)
    /// unless `id` is in the controller's COMMITTED ISR.  INTERNAL partition:
    /// the consensus leader moves to `id` whatever the data ISR (bridge).
    bool moveLeader(NodeId id) {
        const bool inIsr = std::find(meta_.isr.begin(), meta_.isr.end(), id) != meta_.isr.end();
        if (cfg_.topicId >= 0 && !inIsr) return false;
        // INTERNAL: the consensus can only move to a node it can elect.
        if (cfg_.topicId < 0) exchangeKnowledge_();
        if (cfg_.topicId < 0 && !consensusElectable_(id)) return false;
        ++meta_.epoch;
        ++meta_.partitionEpoch;
        meta_.leader = id;
        if (!inIsr) meta_.isr.insert(meta_.isr.begin(), id);
        if (cfg_.topicId < 0)   // a consensus move: every reachable node sees the new term
            for (auto& n : nodes_)
                if (n.up && cut_.count({n.id, kControllerAddr}) == 0) observeTerm_(n, meta_.epoch);
        for (auto& n : nodes_) applyMetadata(n.id);
        return true;
    }
    /// INTERNAL partition: the CONSENSUS election (Aeron Election) that picks
    /// the internal leader.  A candidate wins with votes from a majority of the
    /// live (up, consensus-reachable) nodes, its own included; any live node
    /// may stand, and another node votes for it only if
    /// consensusVoteGate_(voter, candidate) allows it.  `preferred` wins when electable (e.g. the node
    /// whose election timer fired first), else the lowest electable id.
    /// Epoch + 1, the assignment goes to every reachable node.  Returns the new
    /// leader, -1 = nobody electable (the consensus stays leaderless).
    /// One round of the election knowledge exchange among the live,
    /// consensus-reachable nodes (template 58), without electing anyone.
    void exchangeKnowledge() { exchangeKnowledge_(); }
    NodeId consensusElect(NodeId preferred) {
        if (cfg_.topicId < 0) exchangeKnowledge_();
        NodeId winner = -1;
        if (preferred >= 0 && consensusElectable_(preferred)) winner = preferred;
        for (auto& n : nodes_) if (winner < 0 && consensusElectable_(n.id)) winner = n.id;
        if (winner < 0) return -1;
        ++meta_.epoch;
        meta_.leader = winner;
        // Every consensus-reachable node saw the RequestVote / NewLeadershipTerm.
        for (auto& n : nodes_)
            if (n.up && cut_.count({n.id, kControllerAddr}) == 0) observeTerm_(n, meta_.epoch);
        for (auto& n : nodes_) if (cut_.count({kControllerAddr, n.id}) == 0) applyMetadata(n.id);
        return winner;
    }

    /// Node `voter` has voted in (or observed) consensus term `term` — e.g. it
    /// granted a candidate's RequestVote — while its replicas may still follow
    /// the previous leader (NewLeadershipTerm not yet applied by the bridge).
    void consensusVote(NodeId voter, int64_t term) {
        DrNode& n = node(voter);
        n.candidateTerm = std::max(n.candidateTerm, term);
        n.votedTerm     = std::max(n.votedTerm, term);
    }
    /// Node `voter` DENIED a RequestVote of term `term` (stale log / candidacy
    /// rule) — Election's deny branch raises candidateTermId (also in Closed,
    /// on a live leader and its followers), and nothing else.
    void consensusDenyVote(NodeId voter, int64_t term) {
        DrNode& n = node(voter);
        n.candidateTerm = std::max(n.candidateTerm, term);
    }

    /// The controller's committed ISR contains `id`.
    bool controllerIsrContains(NodeId id) const {
        return std::find(meta_.isr.begin(), meta_.isr.end(), id) != meta_.isr.end();
    }
    uint64_t alterIsrCommitted() const noexcept { return alterIsrCommitted_; }
    uint64_t alterIsrStale() const noexcept { return alterIsrStale_; }
    uint64_t fenceCommitted() const noexcept { return fenceCommitted_; }
    uint64_t alterIsrIneligible() const noexcept { return alterIsrIneligible_; }


    // ---------------------------------------------------------------- driving
    void step() {
        ++step_;
        clock_ += kStepNs;
        fenceDeadForInternal_();
        fenceDeadForUser_();
        for (auto& n : nodes_) if (n.up) n.rep->doWork(clock_);
        deliver_();
        settleAcks_();
    }
    void run(int steps) { for (int i = 0; i < steps; ++i) step(); }
    bool driveUntil(const std::function<bool()>& done, int cap = 2000) {
        for (int i = 0; i < cap && !done(); ++i) step();
        return done();
    }

    // ---------------------------------------------------------------- network faults
    void cut(NodeId a, NodeId b)     { cut_.insert({a, b}); }
    void cutBoth(NodeId a, NodeId b) { cut(a, b); cut(b, a); }
    void heal(NodeId a, NodeId b)    { cut_.erase({a, b}); cut_.erase({b, a}); }
    void isolate(NodeId a)  {
        for (NodeId r : replicas_) if (r != a) cutBoth(a, r);
        cutBoth(a, kControllerAddr);   // its AlterIsr / metadata are lost too
    }
    void healAll()          { cut_.clear(); }
    void dropNext(Kind k, int count) { dropNext_[static_cast<int>(k)] += count; }
    uint64_t framesLost() const noexcept { return framesLost_; }

    // ---------------------------------------------------------------- produce
    /// acks=all produce of `count` single-record batches on the current
    /// leader (production worker path).  Returns how many were APPENDED
    /// (a refusal — no leader / incomplete / below min-ISR — stops early).
    int produce(int count) {
        int appended = 0;
        for (int i = 0; i < count; ++i) {
            if (!appendOne_(nextId_, /*pid*/-1, 0, -1).isOK()) break;
            ++nextId_;
            ++appended;
        }
        return appended;
    }
    /// Produce and drive until every one of them is acked (or failed).
    int produceAcked(int count, int cap = 2000) {
        EXPECT_TRUE(waitWritable(cap)) << "no writable leader";
        const std::size_t before = acked_.size();
        const int n = produce(count);
        EXPECT_EQ(n, count) << "produce refused";
        EXPECT_TRUE(driveUntil([&] { return pending_.empty(); }, cap)) << "acks did not settle";
        return static_cast<int>(acked_.size() - before);
    }

    /// Drive until the controller's leader is up, Leader-role, complete and
    /// its ISR meets min-ISR (a client would retry until then).
    bool waitWritable(int cap = 2000) {
        return driveUntil([&] {
            DrNode* L = leaderNode_();
            return L != nullptr && L->rep->acceptsProduce() &&
                   static_cast<int>(L->rep->isr().size()) >= L->rep->minIsr();
        }, cap);
    }

    /// Idempotent produce of ONE batch (pid, producerEpoch, seq) — validated
    /// against the leader's producer state (rebuilt from the log on every
    /// leadership / log-generation change, as the worker does).
    IdemResult produceIdempotent(int64_t pid, int16_t pepoch, int32_t seq, uint64_t id) {
        IdemResult r;
        const auto o = appendOne_(id, pid, pepoch, seq);
        if (o.isDuplicate) { r.status = IdemResult::Status::Duplicate; r.offset = o.baseOffset; return r; }
        if (!o.isOK()) {
            using EC = rune::protocol::client::ErrorCode;
            if (o.error == EC::OutOfOrderSequence)   r.status = IdemResult::Status::OutOfOrder;
            else if (o.error == EC::InvalidProducerEpoch) r.status = IdemResult::Status::Fenced;
            return r;
        }
        r.status = IdemResult::Status::Appended;
        r.offset = o.baseOffset;
        return r;
    }

    // ---------------------------------------------------------------- inspection
    DrNode& node(NodeId id) { return nodes_.at(static_cast<std::size_t>(id - 1)); }
    PartitionReplica& rep(NodeId id) { return *node(id).rep; }
    bool up(NodeId id) { return node(id).up; }
    NodeId leader() const noexcept { return meta_.leader; }
    Epoch  epoch()  const noexcept { return meta_.epoch; }
    const std::vector<NodeId>& controllerIsr() const noexcept { return meta_.isr; }
    const std::set<uint64_t>& acked() const noexcept { return acked_; }
    std::size_t pendingCount() const noexcept { return pending_.size(); }
    std::size_t failedCount() const noexcept { return failed_; }
    int64_t clock() const noexcept { return clock_; }

    /// Record ids in node `id`'s log in offset order (whole log, not HW-bounded).
    std::vector<uint64_t> ids(NodeId id) {
        std::vector<uint64_t> out;
        DrNode& n = node(id);
        if (!n.up) return out;
        Offset at = n.log->logStartOffset();
        const Offset leo = n.log->logEndOffset();
        for (int guard = 0; at < leo && guard < 100000; ++guard) {
            const auto bytes = n.log->read(at, 1 << 20);
            if (bytes.empty()) break;
            std::size_t pos = 0;
            Offset next = at;
            while (pos + rune::protocol::batch::kRecordBatchOverhead <= bytes.size()) {
                rune::protocol::RecordBatchReader r(bytes.data() + pos, bytes.size() - pos);
                const int32_t total = r.totalLength();
                if (total <= 0 || pos + static_cast<std::size_t>(total) > bytes.size()) break;
                r.forEach([&](std::string_view, std::string_view v, int64_t, int64_t) {
                    uint64_t x = 0;
                    if (v.size() == sizeof x) std::memcpy(&x, v.data(), sizeof x);
                    out.push_back(x);
                });
                next = r.baseOffset() + r.lastOffsetDelta() + 1;
                pos += static_cast<std::size_t>(total);
            }
            if (next <= at) break;
            at = next;
        }
        return out;
    }

    /// (baseOffset, partitionLeaderEpoch) of every batch in node `id`'s log —
    /// two replicas holding the same records under different epochs have
    /// DIVERGED (same ids, different lineage), which ids() alone cannot see.
    std::vector<std::pair<Offset, int32_t>> batchEpochs(NodeId id) {
        std::vector<std::pair<Offset, int32_t>> out;
        DrNode& n = node(id);
        if (!n.up) return out;
        Offset at = n.log->logStartOffset();
        const Offset leo = n.log->logEndOffset();
        for (int guard = 0; at < leo && guard < 100000; ++guard) {
            const auto bytes = n.log->read(at, 1 << 20);
            if (bytes.empty()) break;
            std::size_t pos = 0;
            Offset next = at;
            while (pos + rune::protocol::batch::kRecordBatchOverhead <= bytes.size()) {
                rune::protocol::RecordBatchReader r(bytes.data() + pos, bytes.size() - pos);
                const int32_t total = r.totalLength();
                if (total <= 0 || pos + static_cast<std::size_t>(total) > bytes.size()) break;
                out.emplace_back(r.baseOffset(), r.partitionLeaderEpoch());
                next = r.baseOffset() + r.lastOffsetDelta() + 1;
                pos += static_cast<std::size_t>(total);
            }
            if (next <= at) break;
            at = next;
        }
        return out;
    }

    /// Every acked id is present EXACTLY ONCE on node `id`.
    void expectAckedPresent(NodeId id, const char* where = "") {
        const auto v = ids(id);
        std::map<uint64_t, int> count;
        for (uint64_t x : v) ++count[x];
        int missing = 0, dup = 0;
        uint64_t firstMissing = 0;
        for (uint64_t a : acked_) {
            const auto it = count.find(a);
            if (it == count.end()) { if (missing++ == 0) firstMissing = a; }
            else if (it->second > 1) ++dup;
        }
        EXPECT_EQ(missing, 0) << where << ": node " << id << " lost " << missing
                              << " ACKED records (first id " << firstMissing << ") of "
                              << acked_.size() << "; log holds " << v.size();
        EXPECT_EQ(dup, 0) << where << ": node " << id << " holds " << dup << " acked records twice";
    }

    /// No id appears twice on node `id`.
    void expectNoDuplicates(NodeId id) {
        const auto v = ids(id);
        std::set<uint64_t> s(v.begin(), v.end());
        EXPECT_EQ(s.size(), v.size()) << "node " << id << " holds duplicate records";
    }

    /// Every live replica holds the same records in the same order, under the
    /// same leader epochs, as the leader.
    bool converged() {
        DrNode* L = leaderNode_();
        if (!L) return false;
        const Offset leo = L->rep->logEndOffset();
        for (auto& n : nodes_) {
            if (!n.up || n.id == L->id) continue;
            if (n.rep->logEndOffset() != leo) return false;
        }
        const auto ref = ids(L->id);
        const auto refEp = batchEpochs(L->id);
        for (auto& n : nodes_) {
            if (!n.up || n.id == L->id) continue;
            if (ids(n.id) != ref) return false;
            if (batchEpochs(n.id) != refEp) return false;   // same records, same lineage
        }
        return true;
    }

    /// One line per node: role/epoch/LEO/HW/ISR + controller view (diagnostics).
    std::string dump() {
        std::string s = "controller{leader=" + std::to_string(meta_.leader) + " epoch=" +
                         std::to_string(meta_.epoch) + " isr=";
        for (NodeId i : meta_.isr) s += std::to_string(i) + ",";
        s += "} acked=" + std::to_string(acked_.size()) + " failed=" + std::to_string(failed_) + "\n";
        for (auto& n : nodes_) {
            s += "  n" + std::to_string(n.id);
            if (!n.up) { s += " DOWN\n"; continue; }
            s += std::string(n.rep->role() == Role::Leader ? " L" : " F") +
                 " ep=" + std::to_string(n.rep->epoch()) +
                 " leo=" + std::to_string(n.rep->logEndOffset()) +
                 " hw=" + std::to_string(n.rep->highWatermark()) +
                 " cacheLatest=" + std::to_string(n.rep->leaderEpochCacheForTest().latestEpoch());
            if (n.rep->role() == Role::Leader) {
                s += " isr=";
                for (NodeId i : n.rep->isr()) s += std::to_string(i) + ",";
            }
            s += "\n";
        }
        return s;
    }

    bool leaderIsrContains(NodeId member) {
        DrNode* L = leaderNode_();
        if (!L) return false;
        const auto& isr = L->rep->isr();
        return std::find(isr.begin(), isr.end(), member) != isr.end();
    }

    // Endpoint entry point.
    /// Frames of `k` handed to the transport by node `from` (accepted or lost).
    uint64_t sentBy(NodeId from, Kind k) const noexcept {
        return sent_[static_cast<std::size_t>(from)][static_cast<int>(k)];
    }

    bool send(Frame&& f) {
        const int k = static_cast<int>(f.kind);
        if (f.from >= 0 && f.from < 8) ++sent_[static_cast<std::size_t>(f.from)][k];
        if (dropNext_[k] > 0) { --dropNext_[k]; ++framesLost_; return true; }
        if (cut_.count({f.from, f.to}) != 0) { ++framesLost_; return true; }   // lost on the wire
        f.due = step_ + 1 + static_cast<uint64_t>(cfg_.deliveryDelaySteps);
        wire_.push_back(std::move(f));
        return true;
    }

private:
    struct Meta {
        NodeId              leader = -1;
        Epoch               epoch  = 0;
        std::vector<NodeId> isr;
        int32_t             partitionEpoch = 0;   ///< AlterIsr compare-and-set version (MEDIUM-1)
    };

    PartitionConfig cfgFor_(NodeId id) const {
        PartitionConfig c;
        c.topicId                 = cfg_.topicId;
        c.partitionId             = kPartId;
        c.selfId                  = id;
        c.replicaLagMaxMs         = cfg_.replicaLagMaxMs;
        c.replicaLagMaxOffset     = 10'000;
        c.minIsr                  = cfg_.minIsr;
        c.fetchIntervalNs         = cfg_.fetchIntervalNs;
        c.isrCheckIntervalNs      = cfg_.isrCheckIntervalNs;
        c.followerFlushIntervalNs = cfg_.followerFlushIntervalNs;
        c.segmentRollBytes        = cfg_.segmentRollBytes;
        c.maxFetchBytes           = cfg_.maxFetchBytes;
        return c;
    }

    void open_(DrNode& n) {
        n.log = std::make_unique<Log>(n.dir, cfg_.segmentRollBytes);
        n.log->recover();
        if (cfg_.deferSealSync) n.log->setDeferSealSync(true);
        n.ep  = std::make_unique<DrEndpoint>(this, n.id);
        n.rep = std::make_unique<PartitionReplica>(cfgFor_(n.id), Role::Follower, /*epoch=*/0,
                                                   replicas_, replicas_, n.log.get(), n.ep.get());
        if (cfg_.topicId >= 0) n.rep->setIsrChannel(n.ep.get());   // as BrokerNode (multi-node)
        if (cfg_.topicId < 0) {                                       // as BrokerNode (internal)
            n.isrKnowledge = std::make_unique<InternalIsrKnowledge>(n.dir + "/internal-isr.state");
            n.isrKnowledge->setConsensusTermSource(&DrCluster::termOf_, &n);   // as BrokerNode (Election)
            n.rep->setInternalIsrKnowledge(n.isrKnowledge.get());
        }
        n.up = true;
        ++n.incarnation;
        n.rep->setProcessIncarnation(n.incarnation);   // KIP-903 (as ReplicaRegistry::add)
    }

    /// INTERNAL partition: NodeRuntimeAgent's Kafka broker fencing — the leader
    /// drops members the cluster considers dead from the ISR
    /// (PartitionReplica::shrinkIsrToLive with the consensus-eligible set).  A
    /// node is consensus-live here when it is up and can reach the controller
    /// address (isolate() cuts it; a data-plane-only cut does not).
    void fenceDeadForInternal_() {
        if (cfg_.topicId >= 0 || meta_.leader < 0) return;
        DrNode& L = node(meta_.leader);
        if (!L.up || L.rep->role() != Role::Leader) return;
        std::vector<int32_t> live;
        for (auto& n : nodes_)
            if (n.up && cut_.count({n.id, kControllerAddr}) == 0) live.push_back(n.id);
        if (std::find(live.begin(), live.end(), L.id) == live.end()) live.push_back(L.id);
        L.rep->shrinkIsrToLive(std::span<const int32_t>(live.data(), live.size()), clock_);
    }

    /// USER partition: the controller's broker fencing (MetadataService
    /// rebalance tick) — a node the controller considers dead (down, or cut
    /// from the controller address) leaves the committed ISR while the leader
    /// is alive.  Counted in fenceCommitted().
    void fenceDeadForUser_() {
        if (cfg_.topicId < 0 || meta_.leader < 0) return;
        auto liveToController = [&](NodeId id) {
            return node(id).up && cut_.count({id, kControllerAddr}) == 0;
        };
        if (!liveToController(meta_.leader)) return;
        std::vector<NodeId> isr;
        for (NodeId n : meta_.isr) if (n == meta_.leader || liveToController(n)) isr.push_back(n);
        if (isr.size() == meta_.isr.size()) return;
        meta_.isr = std::move(isr);
        ++meta_.partitionEpoch;
        ++fenceCommitted_;
        for (auto& n : nodes_)
            if (cut_.count({kControllerAddr, n.id}) == 0) applyMetadata(n.id);
    }

    /// The harness controller (MetadataService::onAlterIsr semantics): accept a
    /// proposal from the current leader at the current epoch, commit the new
    /// ISR (partitionEpoch + 1) and push the assignment to every node it can
    /// reach (the leader sees a same-epoch becomeLeader).
    void controllerAlterIsr_(const AlterIsrMsg& m) {
        auto reply = [&](int16_t code) {
            Frame f; f.kind = Kind::AlterIsrResp; f.from = kControllerAddr; f.to = m.leaderId;
            f.alterResp.topicId = m.topicId; f.alterResp.partition = m.partition;
            f.alterResp.leaderId = m.leaderId; f.alterResp.leaderEpoch = m.leaderEpoch;
            f.alterResp.partitionEpoch = meta_.partitionEpoch; f.alterResp.errorCode = code;
            (void)send(std::move(f));
        };
        if (m.leaderId != meta_.leader || m.leaderEpoch != meta_.epoch) {
            reply(kAlterIsrFencedLeaderEpoch);
            return;
        }
        // Compare-and-set on the monotonic partitionEpoch (Kafka; MEDIUM-1).
        if (m.partitionEpoch != meta_.partitionEpoch) {
            ++alterIsrStale_;
            reply(kAlterIsrInvalidUpdateVersion);
            return;
        }
        // KIP-841 (MEDIUM-2): never ADD a replica the controller considers dead.
        auto liveToController = [&](NodeId id) {
            return node(id).up && cut_.count({id, kControllerAddr}) == 0;
        };
        std::vector<NodeId> isr;
        bool dropped = false;
        for (int i = 0; i < m.isrCount; ++i) {
            const NodeId n = m.isr[static_cast<std::size_t>(i)];
            const bool added = std::find(meta_.isr.begin(), meta_.isr.end(), n) == meta_.isr.end();
            if (added && n != m.leaderId && !liveToController(n)) { dropped = true; continue; }
            isr.push_back(n);
        }
        if (std::find(isr.begin(), isr.end(), m.leaderId) == isr.end()) {
            reply(kAlterIsrInvalidRequest);
            return;
        }
        std::vector<NodeId> a = isr, b = meta_.isr;
        std::sort(a.begin(), a.end()); std::sort(b.begin(), b.end());
        if (a == b) {
            if (dropped) ++alterIsrIneligible_;
            reply(dropped ? kAlterIsrIneligibleReplica : kAlterIsrOk);
            return;
        }
        meta_.isr = std::move(isr);
        ++meta_.partitionEpoch;
        ++alterIsrCommitted_;
        for (auto& n : nodes_)
            if (cut_.count({kControllerAddr, n.id}) == 0) applyMetadata(n.id);
    }

    DrNode* leaderNode_() {
        if (meta_.leader < 0) return nullptr;
        DrNode& L = node(meta_.leader);
        if (!L.up || L.rep->role() != Role::Leader) return nullptr;
        return &L;
    }

    /// The production produce path: the worker's role gate
    /// (PartitionReplica::acceptsProduce — the SAME predicate Worker /
    /// ClientGateway call) then the shared rune::storage::handleProduceBatch core
    /// (min-ISR gate, PSM validate/update, page-cache append) and
    /// onLeaderLogAppended().  The producer state is the leader's, rebuilt from
    /// its log on every leadership / log-generation change (as the worker does).
    rune::storage::ProduceOutcome appendOne_(uint64_t id, int64_t pid, int16_t pepoch, int32_t seq) {
        rune::storage::ProduceOutcome refused;
        refused.error = rune::protocol::client::ErrorCode::NotLeader;
        DrNode* L = leaderNode_();
        if (L == nullptr || !L->rep->acceptsProduce()) return refused;
        ensurePsm_(*L);
        rune::protocol::RecordBatchBuilder b(0, clock_ / 1'000'000);
        if (pid >= 0) {
            b.setProducerId(pid);
            b.setProducerEpoch(pepoch);
            b.setBaseSequence(seq);
        }
        char v[sizeof id];
        std::memcpy(v, &id, sizeof id);
        b.append(std::string_view{}, std::string_view(v, sizeof v), clock_ / 1'000'000);
        const auto bytes = b.build();
        const auto o = rune::storage::handleProduceBatch(
            bytes.data(), bytes.size(), cfg_.topicId, kPartId, pid, pepoch, seq, *L->log, psm_,
            /*acksAll=*/true, static_cast<int>(L->rep->isr().size()), L->rep->minIsr(),
            /*durabilityTier=*/1);
        if (!o.isOK() || o.isDuplicate) return o;
        L->rep->onLeaderLogAppended();
        pending_.push_back(Pending{id, o.baseOffset, L->id, L->rep->epoch(), L->incarnation});
        return o;
    }

    void settleAcks_() {
        for (auto it = pending_.begin(); it != pending_.end();) {
            DrNode& n = node(it->leader);
            const bool same = n.up && n.incarnation == it->incarnation &&
                              n.rep->role() == Role::Leader && n.rep->epoch() == it->epoch;
            if (!same) { ++failed_; it = pending_.erase(it); continue; }
            if (n.rep->highWatermark() > it->offset) {
                acked_.insert(it->id);
                it = pending_.erase(it);
                continue;
            }
            ++it;
        }
    }

    void ensurePsm_(DrNode& L) {
        const uint64_t gen = (static_cast<uint64_t>(L.incarnation) << 48) ^
                             (L.rep->leadershipGeneration() << 32) ^ L.rep->logGeneration() ^
                             (static_cast<uint64_t>(L.id) << 56);
        if (psmValid_ && psmGen_ == gen) return;
        psm_.clearPartition(cfg_.topicId, kPartId);
        (void)psm_.rebuildFromLog(cfg_.topicId, *L.log, L.log->logEndOffset(), kPartId);
        psmGen_   = gen;
        psmValid_ = true;
    }

    void purge_(NodeId id) {
        for (auto it = wire_.begin(); it != wire_.end();) {
            if (it->from == id || it->to == id) it = wire_.erase(it);
            else ++it;
        }
    }

    void deliver_() {
        std::deque<Frame> now;
        for (auto it = wire_.begin(); it != wire_.end();) {
            if (it->due <= step_) { now.push_back(std::move(*it)); it = wire_.erase(it); }
            else ++it;
        }
        for (auto& f : now) {
            if (f.kind == Kind::AlterIsr) { controllerAlterIsr_(f.alter); continue; }
            DrNode& n = node(f.to);
            if (!n.up) continue;
            PartitionReplica& r = *n.rep;
            switch (f.kind) {
                case Kind::FetchReq:  (void)r.onFetchRequest(f.freq, clock_); break;
                case Kind::FetchResp:
                    f.fresp.records = std::span<const uint8_t>(f.recs.data(), f.recs.size());
                    r.applyReplicated(f.fresp); break;
                case Kind::EpochReq:  r.onOffsetsForLeaderEpochRequest(f.ereq); break;
                case Kind::EpochResp: r.applyOffsetsForLeaderEpochResponse(f.eresp); break;
                case Kind::ReconReq:  r.onReconcileRequest(f.freq); break;
                case Kind::ReconResp:
                    f.fresp.records = std::span<const uint8_t>(f.recs.data(), f.recs.size());
                    r.applyReconcileResponse(f.fresp); break;
                case Kind::AlterIsr: break;   // handled above
                case Kind::AlterIsrResp: r.onAlterIsrResponse(f.alterResp); break;
                case Kind::InternalIsr:    r.onInternalIsr(f.iisr); break;
                case Kind::InternalIsrAck: r.onInternalIsrAck(f.iack); break;
            }
        }
    }

    DrConfig                cfg_;
    std::string             root_;
    std::vector<NodeId>     replicas_;
    std::vector<DrNode>     nodes_;
    Meta                    meta_;
    std::deque<Frame>       wire_;
    std::set<std::pair<NodeId, NodeId>> cut_;
    int                     dropNext_[kKinds] = {};
    uint64_t                framesLost_ = 0;
    uint64_t                sent_[8][kKinds] = {};
    uint64_t                alterIsrCommitted_ = 0;
    uint64_t                alterIsrStale_     = 0;
    uint64_t                fenceCommitted_    = 0;
    uint64_t                alterIsrIneligible_ = 0;
    uint64_t                step_  = 0;
    int64_t                 clock_ = 0;
    uint64_t                nextId_ = 1;
    std::deque<Pending>     pending_;
    std::set<uint64_t>      acked_;
    std::size_t             failed_ = 0;
    bool                    lostOnPowerLoss_ = false;
    std::optional<rune::metadata::ReplicaPositionTable> capturedPositions_;   ///< see controllerLearnPositionsFrom
    /// Consensus vote rule (voter, candidate) — Election::mayVoteFor as the
    /// broker wires it: InternalIsrKnowledge::eligible of the voter.
    static int64_t termOf_(const void* p) noexcept {
        const auto* n = static_cast<const DrNode*>(p);
        return rune::consensus::ackTerm(n->candidateTerm, n->votedTerm, n->leadershipTerm);
    }
    static void observeTerm_(DrNode& n, int64_t term) noexcept {   // NewLeadershipTerm seen
        n.candidateTerm  = std::max(n.candidateTerm, term);
        n.leadershipTerm = std::max(n.leadershipTerm, term);
    }
    bool consensusVoteGate_(NodeId voter, NodeId candidate) {
        DrNode& v = node(voter);
        return v.isrKnowledge == nullptr || v.isrKnowledge->eligible(candidate);
    }
    /// Round-3 CRITICAL-R3: the election participants (live, consensus-
    /// reachable) exchange their newest durable metadata-ISR state while they
    /// canvass / vote (CandidacyState): each adopts the newest one held by any
    /// participant (InternalIsrKnowledge::adopt — durable, never acked).
    void exchangeKnowledge_() {
        const InternalIsrState* newest = nullptr;
        for (auto& n : nodes_) {
            if (!n.up || cut_.count({n.id, kControllerAddr}) != 0 || !n.isrKnowledge) continue;
            const InternalIsrState* st = n.isrKnowledge->candidacyState();
            if (st && (!newest || st->newerThan(newest->leaderEpoch, newest->version))) newest = st;
        }
        if (!newest) return;
        const InternalIsrState copy = *newest;
        for (auto& n : nodes_)
            if (n.up && cut_.count({n.id, kControllerAddr}) == 0 && n.isrKnowledge)
                (void)n.isrKnowledge->adopt(copy);
    }
    bool consensusElectable_(NodeId cand) {
        std::vector<NodeId> live;
        for (auto& n : nodes_)
            if (n.up && cut_.count({n.id, kControllerAddr}) == 0) live.push_back(n.id);
        if (std::find(live.begin(), live.end(), cand) == live.end()) return false;
        // A candidate always may stand (re-review MAJOR-A), but EVERY vote —
        // its own included — is gated by the voter's durable knowledge (round 3
        // CRITICAL-R3: Election::gateSelfVote_).
        std::size_t votes = 0;
        for (NodeId v : live) if (consensusVoteGate_(v, cand)) ++votes;
        return votes >= nodes_.size() / 2 + 1;
    }
    rune::storage::ProducerStateManager psm_;
    uint64_t                psmGen_   = 0;
    bool                    psmValid_ = false;
};

// ---------------------------------------------------------------------------
// DrEndpoint
// ---------------------------------------------------------------------------
inline bool DrEndpoint::sendFetchRequestTo(NodeId leader, const FetchRequestMsg& req) {
    Frame f; f.kind = Kind::FetchReq; f.from = self_; f.to = leader; f.freq = req;
    return c_->send(std::move(f));
}
inline bool DrEndpoint::sendFetchResponse(NodeId to, const FetchResponseMsg& resp) {
    Frame f; f.kind = Kind::FetchResp; f.from = self_; f.to = to; f.fresp = resp;
    f.recs.assign(resp.records.begin(), resp.records.end());
    f.fresp.records = {};
    return c_->send(std::move(f));
}
inline bool DrEndpoint::sendOffsetsForLeaderEpochRequest(NodeId to, const OffsetsForLeaderEpochReqMsg& m) {
    Frame f; f.kind = Kind::EpochReq; f.from = self_; f.to = to; f.ereq = m;
    return c_->send(std::move(f));
}
inline bool DrEndpoint::sendOffsetsForLeaderEpochResponse(NodeId to, const OffsetsForLeaderEpochRespMsg& m) {
    Frame f; f.kind = Kind::EpochResp; f.from = self_; f.to = to; f.eresp = m;
    return c_->send(std::move(f));
}
inline bool DrEndpoint::sendReconcileRequest(NodeId to, const FetchRequestMsg& m) {
    Frame f; f.kind = Kind::ReconReq; f.from = self_; f.to = to; f.freq = m;
    return c_->send(std::move(f));
}
inline bool DrEndpoint::sendAlterIsr(const AlterIsrMsg& m) {
    Frame f; f.kind = Kind::AlterIsr; f.from = self_; f.to = kControllerAddr; f.alter = m;
    return c_->send(std::move(f));
}
inline bool DrEndpoint::sendInternalIsr(NodeId to, const InternalIsrMsg& m) {
    Frame f; f.kind = Kind::InternalIsr; f.from = self_; f.to = to; f.iisr = m;
    return c_->send(std::move(f));
}
inline bool DrEndpoint::sendInternalIsrAck(NodeId to, const InternalIsrAckMsg& m) {
    Frame f; f.kind = Kind::InternalIsrAck; f.from = self_; f.to = to; f.iack = m;
    return c_->send(std::move(f));
}
inline bool DrEndpoint::sendReconcileResponse(NodeId to, const FetchResponseMsg& m) {
    Frame f; f.kind = Kind::ReconResp; f.from = self_; f.to = to; f.fresp = m;
    f.recs.assign(m.records.begin(), m.records.end());
    f.fresp.records = {};
    return c_->send(std::move(f));
}

} // namespace rune::replication::dr
