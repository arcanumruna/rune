#pragma once
// ClusterHarness.h — Deterministic single-threaded multi-node cluster harness.
//
// Usage in tests:
//   ClusterHarness h(3);
//   int64_t now = h.startNs();
//   ASSERT_TRUE(h.driveUntil([&]{ return h.leaderCount()==1; }, now));
//   EXPECT_EQ(h.leaderCount(), 1);
//
// KEY DESIGN CONSTRAINTS (per P2P-9 spec):
//  - No real threads, no wall-time, no busy-spin.
//  - step(nowNs) calls doWork(nowNs) on each alive node ONCE.
//  - driveUntil() jumps clock to min(next-deadline across alive nodes),
//    calls step(), checks pred, and FAILS (returns false) if pred not met
//    within maxSteps.  Tests ASSERT_TRUE the return value.
//  - BusSink routes encoded bytes to target node's IngressRing.
//    Partition / kill / drop / delay policies gate delivery.

#include <rune/consensus/ConsensusAgent.h>
#include <rune/consensus/Election.h>
#include <rune/consensus/ElectionState.h>
#include <rune/consensus/IngressRing.h>
#include <rune/consensus/NodeStateFile.h>
#include <rune/consensus/TermLog.h>
#include <rune/consensus/ClusterMember.h>
#include <rune/consensus/SbeConsensusPublisher.h>
#include <rune/consensus/MessageSink.h>
#include <rune/consensus/Types.h>

#include <cassert>
#include <cstddef>
#include <chrono>
#include <climits>
#include <deque>
#include <filesystem>
#include <functional>
#include <memory>
#include <optional>
#include <random>
#include <set>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <unistd.h>

namespace rune::consensus::test {

namespace fs = std::filesystem;

// ---------------------------------------------------------------------------
// TempDir — per-node unique temp directory for NodeStateFile + TermLog.
// ---------------------------------------------------------------------------
struct TempDir {
    fs::path dir;

    /// A placeholder that owns nothing (never touches the disk): a later move-assign gives it a real directory.
    explicit TempDir(std::nullptr_t) noexcept {}

    explicit TempDir(std::string_view prefix = "rune_cluster_") {
        static int ctr = 0;   // single-threaded harness: no atomic (shared-nothing)
        auto ts = std::chrono::steady_clock::now().time_since_epoch().count();
        dir     = fs::temp_directory_path() /
                  (std::string(prefix) + std::to_string(::getpid()) + "_" + std::to_string(ts) + "_" +
                   std::to_string(ctr++));
        fs::create_directories(dir);
    }

    ~TempDir() { removeNow_(); }

    // NOT copyable — unique ownership.
    TempDir(const TempDir&)            = delete;
    TempDir& operator=(const TempDir&) = delete;

    // Movable.  Invariant: every directory a TempDir created is owned by exactly one live TempDir and is removed
    // when that object dies or is overwritten; a moved-from TempDir owns nothing.
    TempDir(TempDir&& o) noexcept : dir(std::exchange(o.dir, {})) {}
    TempDir& operator=(TempDir&& o) noexcept {
        if (this != &o) {
            removeNow_();
            dir = std::exchange(o.dir, {});
        }
        return *this;
    }

    std::string nsfPath()  const { return (dir / "nodestate.nsf").string(); }
    std::string termPath() const { return (dir / "termlog.tl").string(); }

private:
    void removeNow_() noexcept {
        if (!dir.empty()) {
            std::error_code ec;
            fs::remove_all(dir, ec);
        }
        dir.clear();
    }
};

// ---------------------------------------------------------------------------
// Forward declaration
// ---------------------------------------------------------------------------
class ClusterHarness;

// ---------------------------------------------------------------------------
// PendingMsg — a message queued for delayed delivery.
// ---------------------------------------------------------------------------
struct PendingMsg {
    NodeId        from;
    NodeId        to;
    std::vector<uint8_t> bytes;
    int           deliverAtStep;  // deliver when harness.step_ >= deliverAtStep
};

// ---------------------------------------------------------------------------
// BusSink — MessageSink injected into SbeConsensusPublisher.
// Routes send() through the harness bus (partition/drop/delay logic lives in
// ClusterHarness::deliver()).
// ---------------------------------------------------------------------------
class BusSink final : public MessageSink {
public:
    BusSink(ClusterHarness& harness, NodeId fromId)
        : harness_(harness), fromId_(fromId) {}

    void send(NodeId toMember, const uint8_t* buf, std::size_t len) override;

private:
    ClusterHarness& harness_;
    NodeId          fromId_;
};

// ---------------------------------------------------------------------------
// NodeBundle — per-node collection of owned objects.
// ---------------------------------------------------------------------------
struct NodeBundle {
    NodeId id;

    // Persistent temp paths (survive restartNode).
    TempDir tempDir{nullptr};   // a placeholder; the harness ctor assigns the real directory once

    // Owned persistence objects — recreated on restart.
    std::unique_ptr<NodeStateFile>      nsf;
    std::unique_ptr<TermLog>            tl;

    // Ingress ring — owned here; reused across restarts.
    std::unique_ptr<IngressRing>        ring;

    // BusSink must outlive publisher (publisher_ holds ref to sink_).
    std::unique_ptr<BusSink>            sink;

    // Publisher + Election + Agent — recreated on restart.
    std::unique_ptr<SbeConsensusPublisher> publisher;
    std::unique_ptr<Election>           election;
    std::unique_ptr<ConsensusAgent>     agent;

    bool alive = false;

    NodeBundle() = default;

    // Not copyable — unique_ptrs can't be copied.
    NodeBundle(const NodeBundle&)            = delete;
    NodeBundle& operator=(const NodeBundle&) = delete;

    // Movable.
    NodeBundle(NodeBundle&&) noexcept            = default;
    NodeBundle& operator=(NodeBundle&&) noexcept = default;
};

// ---------------------------------------------------------------------------
// ClusterHarness
// ---------------------------------------------------------------------------
class ClusterHarness {
public:
    // Timing constants (nanoseconds) — small to keep step counts down.
    static constexpr int64_t kElectionTimeoutNs      = 100'000'000LL;  // 100 ms in sim-ns
    static constexpr int64_t kStatusIntervalNs        =  20'000'000LL;  //  20 ms
    static constexpr int64_t kStartupCanvassTimeoutNs = 200'000'000LL;  // 200 ms
    static constexpr int64_t kStepQuantumNs           =  10'000'000LL;  //  10 ms minimum clock advance

    // -----------------------------------------------------------------------
    // Construction
    // -----------------------------------------------------------------------
    explicit ClusterHarness(int nodeCount,
                            uint64_t rngSeed   = 0xDEADBEEFCAFEBABEULL,
                            int      delaySteps = 0,
                            double   dropRate   = 0.0,
                            bool     reorder    = false)
        : nodeCount_(nodeCount)
        , rng_(rngSeed)
        , dropRate_(dropRate)
        , delaySteps_(delaySteps)
        , reorder_(reorder)
    {
        nodes_.reserve(static_cast<std::size_t>(nodeCount));
        for (int i = 0; i < nodeCount; ++i) {
            nodes_.emplace_back();
            nodes_.back().id      = i;
            nodes_.back().tempDir = TempDir("rune_node" + std::to_string(i) + "_");
            nodes_.back().ring    = std::make_unique<IngressRing>(64 * 1024);
            buildNode(i, /*startup=*/true);
        }
    }

    ~ClusterHarness() = default;

    // Not copyable or movable.
    ClusterHarness(const ClusterHarness&)            = delete;
    ClusterHarness& operator=(const ClusterHarness&) = delete;

    // -----------------------------------------------------------------------
    // Clock
    // -----------------------------------------------------------------------
    int64_t startNs() const noexcept { return kStartupCanvassTimeoutNs + kElectionTimeoutNs; }

    // -----------------------------------------------------------------------
    // step(nowNs) — advance every alive node by one doWork.
    // Then deliver all pending delayed messages whose deliverAtStep <= step_.
    //
    // After doWork, check if any closed-follower nodes need a new election
    // because their leader has been killed (Aeron ConsensusModuleAgent does
    // this via a leader heartbeat timeout check — we simulate it by detecting
    // that a Closed node's leaderMemberId is in killedNodes_).
    // -----------------------------------------------------------------------
    void step(int64_t nowNs) {
        ++step_;

        // Deliver reordered/delayed messages first.
        deliverPending(nowNs);

        for (int i = 0; i < nodeCount_; ++i) {
            if (nodes_[i].alive) {
                nodes_[i].agent->doWork(nowNs);
            }
        }

        // Re-election trigger: if a node is Closed but its leader is dead,
        // it needs to start a new election (mimics ConsensusModuleAgent
        // detecting leader heartbeat timeout and calling new Election).
        triggerReelectionsIfNeeded(nowNs);
    }

    // -----------------------------------------------------------------------
    // driveUntil — bounded drive loop.
    // Jumps nowNs to min(next-deadline across alive nodes) then steps.
    // Returns true if pred() becomes true within maxSteps.
    // Returns false (does NOT hang) if the cap is reached.
    // -----------------------------------------------------------------------
    bool driveUntil(const std::function<bool()>& pred,
                    int64_t&                     nowNs,
                    int                          maxSteps = 500)
    {
        for (int i = 0; i < maxSteps; ++i) {
            if (pred()) return true;

            // Jump to earliest deadline or advance by quantum.
            int64_t minDeadline = nextMinDeadlineNs(nowNs);
            nowNs = std::max(nowNs + kStepQuantumNs, minDeadline);

            step(nowNs);
        }
        return pred();
    }

    // -----------------------------------------------------------------------
    // Partition / heal policies
    // -----------------------------------------------------------------------

    // Drop ALL messages crossing the partition boundary between setA and setB.
    void partition(std::set<NodeId> setA, std::set<NodeId> setB) {
        partitionA_ = std::move(setA);
        partitionB_ = std::move(setB);
    }

    // Remove partition policy (all nodes can communicate again).
    void heal() {
        partitionA_.clear();
        partitionB_.clear();
    }

    // -----------------------------------------------------------------------
    // Kill / restart
    // -----------------------------------------------------------------------

    // Kill: node stops being stepped AND its in/out messages are dropped.
    void killNode(NodeId id) {
        assert(id >= 0 && id < nodeCount_);
        nodes_[id].alive = false;
        killedNodes_.insert(id);
    }

    // Restart: recreate ConsensusAgent from the SAME NodeStateFile + TermLog paths,
    // proving persistence.  The IngressRing is reused (same object, cleared).
    void restartNode(NodeId id) {
        assert(id >= 0 && id < nodeCount_);
        killedNodes_.erase(id);
        // Rebuild from the persisted temp paths.
        buildNode(id, /*startup=*/false);
        nodes_[id].alive = true;
    }

    // -----------------------------------------------------------------------
    // Accessors
    // -----------------------------------------------------------------------

    // Number of alive nodes in LeaderReady or Closed AND whose election is closed
    // (or in LeaderReady which is the pre-close leader state).
    int leaderCount() const {
        int n = 0;
        for (int i = 0; i < nodeCount_; ++i) {
            if (!nodes_[i].alive) continue;
            if (isLeader(i)) ++n;
        }
        return n;
    }

    // IDs of alive leader nodes.
    std::vector<NodeId> leaders() const {
        std::vector<NodeId> res;
        for (int i = 0; i < nodeCount_; ++i) {
            if (nodes_[i].alive && isLeader(i)) res.push_back(i);
        }
        return res;
    }

    // The leadershipTermId for node id (from Election accessor).
    Term termOf(NodeId id) const {
        assert(id >= 0 && id < nodeCount_);
        if (!nodes_[id].agent) return -1;
        return nodes_[id].election->leadershipTermId();
    }

    // The candidateTermId for node id (from Election accessor).
    Term candidateTermOf(NodeId id) const {
        assert(id >= 0 && id < nodeCount_);
        if (!nodes_[id].agent) return -1;
        return nodes_[id].election->candidateTermId();
    }

    // Election state of node id.
    ElectionState stateOf(NodeId id) const {
        assert(id >= 0 && id < nodeCount_);
        if (!nodes_[id].agent) return ElectionState::Init;
        return nodes_[id].agent->state();
    }

    bool isAlive(NodeId id) const {
        assert(id >= 0 && id < nodeCount_);
        return nodes_[id].alive;
    }

    int nodeCount() const noexcept { return nodeCount_; }

    // Check if node is in a "leader" state (LeaderReady or Closed-as-leader).
    // A node is considered a leader if its election's leaderMemberId == thisMemberId
    // AND it's in LeaderReady or Closed state.
    bool isLeader(NodeId id) const {
        if (!nodes_[id].alive || !nodes_[id].election) return false;
        const auto& el = *nodes_[id].election;
        ElectionState s = el.state();
        if (s == ElectionState::LeaderReady || s == ElectionState::Closed) {
            // leaderMemberId == self means this node is the leader.
            return el.leaderMemberId() == id;
        }
        return false;
    }

    // Check election safety: at no point are two nodes both leader in the SAME term.
    // Returns true if safe (no violations).
    bool checkElectionSafety() const {
        // Build map: term -> set of leader ids
        std::unordered_map<Term, std::vector<NodeId>> termToLeaders;
        for (int i = 0; i < nodeCount_; ++i) {
            if (!nodes_[i].alive) continue;
            if (isLeader(i)) {
                Term t = termOf(i);
                termToLeaders[t].push_back(i);
            }
        }
        for (auto& [t, leaders] : termToLeaders) {
            if (leaders.size() > 1) return false;
        }
        return true;
    }

    // -----------------------------------------------------------------------
    // Public raw inject — delivers bytes directly into node `to`'s IngressRing,
    // bypassing partition/kill/drop/delay policies.  Used by tests to inject
    // crafted (e.g. stale) messages that a peer would otherwise not send.
    // -----------------------------------------------------------------------
    void injectRaw(NodeId to, const uint8_t* buf, std::size_t len) {
        if (to < 0 || to >= nodeCount_) return;
        if (!nodes_[to].alive)         return;
        nodes_[to].ring->write(buf, len);
    }

    // -----------------------------------------------------------------------
    // Bus delivery — called by BusSink::send
    // -----------------------------------------------------------------------
    void deliver(NodeId from, NodeId to, const uint8_t* buf, std::size_t len) {
        // Drop if sender or receiver is killed.
        if (killedNodes_.count(from) || killedNodes_.count(to)) return;

        // Drop if crossing a partition.
        if (isCrossPartition(from, to)) return;

        // Drop rate (seeded RNG — deterministic).
        if (dropRate_ > 0.0) {
            std::uniform_real_distribution<double> dist(0.0, 1.0);
            if (dist(rng_) < dropRate_) return;
        }

        std::vector<uint8_t> bytes(buf, buf + len);

        if (delaySteps_ > 0) {
            // Queue for delayed delivery.
            int deliverAt = step_ + delaySteps_;
            if (reorder_) {
                // Add 0..delaySteps_ random jitter.
                std::uniform_int_distribution<int> jitter(0, delaySteps_);
                deliverAt = step_ + jitter(rng_);
            }
            pendingMsgs_.push_back(PendingMsg{from, to, std::move(bytes), deliverAt});
        } else if (reorder_ && !pendingMsgs_.empty()) {
            // Insert at a random position in the pending queue (step 0 delay reorder).
            std::uniform_int_distribution<std::size_t> pos(0, pendingMsgs_.size());
            auto it = pendingMsgs_.begin() + static_cast<ptrdiff_t>(pos(rng_));
            pendingMsgs_.insert(it, PendingMsg{from, to, std::move(bytes), step_});
        } else {
            // Immediate delivery.
            injectInto(to, bytes.data(), bytes.size());
        }
    }

private:
    // -----------------------------------------------------------------------
    // Build / rebuild a node's Election + Agent from its persisted paths.
    // -----------------------------------------------------------------------
    void buildNode(int idx, bool startup) {
        NodeBundle& nb = nodes_[idx];

        // Recreate persistence objects (open existing files if they exist).
        nb.nsf = std::make_unique<NodeStateFile>(nb.tempDir.nsfPath(), 0);
        nb.tl  = std::make_unique<TermLog>(nb.tempDir.termPath());

        // Recreate sink (needs harness ref).
        nb.sink = std::make_unique<BusSink>(*this, idx);

        // Recreate publisher.
        nb.publisher = std::make_unique<SbeConsensusPublisher>(*nb.sink);

        // Build cluster member list.
        std::vector<ClusterMember> members = makeMembers(nodeCount_);

        // Derive term state from TermLog (for restarts: last committed term).
        Term leadershipTermId    = 0;
        Term logLeadershipTermId = 0;
        {
            auto last = nb.tl->lastTermEntry();
            if (last.has_value()) {
                leadershipTermId    = last->leadershipTermId;
                logLeadershipTermId = last->leadershipTermId;
            }
        }
        // CandidateTermId from NodeStateFile (for restart: persisted term).
        Term persistedCandidate = nb.nsf->candidateTermId();
        if (persistedCandidate != NodeStateFile::NULL_VALUE) {
            leadershipTermId    = std::max(leadershipTermId, persistedCandidate);
            logLeadershipTermId = std::max(logLeadershipTermId, persistedCandidate);
        }

        // Use a distinct RNG seed per node for nomination backoff randomisation.
        uint64_t nodeSeed = rng_() ^ (static_cast<uint64_t>(idx) * 0x9e3779b97f4a7c15ULL);

        nb.election = std::make_unique<Election>(
            /*isNodeStartup=*/startup,
            /*gracefulClosedLeaderId=*/-1,
            leadershipTermId,
            logLeadershipTermId,
            /*logPosition=*/0,
            /*appendPosition=*/0,
            members,
            /*thisMemberId=*/idx,
            *nb.publisher,
            *nb.nsf,
            *nb.tl,
            kElectionTimeoutNs,
            kStatusIntervalNs,
            kStartupCanvassTimeoutNs,
            nodeSeed);

        nb.agent = std::make_unique<ConsensusAgent>(*nb.election, *nb.ring);
        nb.alive = true;
    }

    // Build a ClusterMember list for N nodes with 0-based IDs.
    static std::vector<ClusterMember> makeMembers(int n) {
        std::vector<ClusterMember> ms(static_cast<std::size_t>(n));
        for (int i = 0; i < n; ++i) {
            ms[i].id                 = i;
            ms[i].logPosition        = 0;
            ms[i].logLeadershipTermId = 0;
            ms[i].leadershipTermId   = 0;
        }
        return ms;
    }

    // Re-election trigger: detect alive nodes whose election is Closed but whose
    // leader is unreachable (killed OR partitioned away from this node).
    // These nodes need a new Election, just as ConsensusModuleAgent does
    // when it detects leader heartbeat timeout.
    //
    // A node needs re-election if:
    //  - alive and Election is Closed
    //  - NOT themselves the leader (leaderMemberId != self)
    //  - their leader is killed OR is cross-partition from this node
    //
    // We use a per-node "re-election cooldown" counter to avoid rebuilding
    // on every step once the trigger fires.  We rebuild only when the leader
    // becomes newly unreachable (edge-triggered, not level-triggered).
    void triggerReelectionsIfNeeded(int64_t nowNs) {
        // First pass: find the maximum leadershipTermId across all alive, non-Closed
        // elections (i.e., an actively running election in a newer term).
        Term maxActiveElectionTerm = -1;
        for (int i = 0; i < nodeCount_; ++i) {
            if (!nodes_[i].alive || !nodes_[i].election) continue;
            // A node in LeaderReady or Closed-as-leader represents the authoritative term.
            if (isLeader(i)) {
                maxActiveElectionTerm = std::max(maxActiveElectionTerm, nodes_[i].election->leadershipTermId());
            }
        }

        for (int i = 0; i < nodeCount_; ++i) {
            if (!nodes_[i].alive) continue;
            if (!nodes_[i].election) continue;

            const auto& el = *nodes_[i].election;
            if (el.state() != ElectionState::Closed) continue;

            NodeId leaderId = el.leaderMemberId();

            // CASE 1: This node is a Closed FOLLOWER whose leader is unreachable
            // (killed or partitioned away). It needs a new election.
            if (leaderId != i) {
                bool leaderDead        = (leaderId >= 0 && leaderId < nodeCount_)
                                         && killedNodes_.count(leaderId) > 0;
                bool leaderPartitioned = (leaderId >= 0 && leaderId < nodeCount_)
                                         && isCrossPartition(i, leaderId);

                if (leaderDead || leaderPartitioned) {
                    rebuildElectionForNode(i, nowNs);
                    continue;
                }
            }

            // CASE 2: This node is a Closed LEADER whose term is stale — another
            // node is a leader in a HIGHER term (e.g., after heal from partition).
            // In real Aeron, ConsensusModuleAgent detects a higher term NLT and
            // triggers a new election.  We simulate this here.
            if (leaderId == i) {
                if (maxActiveElectionTerm > el.leadershipTermId()) {
                    // A higher-term leader exists — we need to step down.
                    rebuildElectionForNode(i, nowNs);
                    continue;
                }
            }
        }
    }

    // Rebuild just the Election + Agent for node idx, preserving ring + persistence.
    // Called when a follower's leader dies and a new election is needed.
    void rebuildElectionForNode(int idx, int64_t /*nowNs*/) {
        NodeBundle& nb = nodes_[idx];

        // Re-open persisted state files (they already exist).
        nb.nsf = std::make_unique<NodeStateFile>(nb.tempDir.nsfPath(), 0);
        nb.tl  = std::make_unique<TermLog>(nb.tempDir.termPath());

        // New sink + publisher.
        nb.sink      = std::make_unique<BusSink>(*this, idx);
        nb.publisher = std::make_unique<SbeConsensusPublisher>(*nb.sink);

        // Derive term state from TermLog + NodeStateFile.
        Term leadershipTermId    = 0;
        Term logLeadershipTermId = 0;
        {
            auto last = nb.tl->lastTermEntry();
            if (last.has_value()) {
                leadershipTermId    = last->leadershipTermId;
                logLeadershipTermId = last->leadershipTermId;
            }
        }
        Term persistedCandidate = nb.nsf->candidateTermId();
        if (persistedCandidate != NodeStateFile::NULL_VALUE) {
            leadershipTermId    = std::max(leadershipTermId, persistedCandidate);
            logLeadershipTermId = std::max(logLeadershipTermId, persistedCandidate);
        }

        uint64_t nodeSeed = rng_() ^ (static_cast<uint64_t>(idx) * 0x9e3779b97f4a7c15ULL);

        nb.election = std::make_unique<Election>(
            /*isNodeStartup=*/false,
            /*gracefulClosedLeaderId=*/-1,
            leadershipTermId,
            logLeadershipTermId,
            /*logPosition=*/0,
            /*appendPosition=*/0,
            makeMembers(nodeCount_),
            /*thisMemberId=*/idx,
            *nb.publisher,
            *nb.nsf,
            *nb.tl,
            kElectionTimeoutNs,
            kStatusIntervalNs,
            kStartupCanvassTimeoutNs,
            nodeSeed);

        nb.agent = std::make_unique<ConsensusAgent>(*nb.election, *nb.ring);
    }

    // Returns the earliest next-deadline across alive nodes, or nowNs+quantum.
    int64_t nextMinDeadlineNs(int64_t nowNs) const {
        int64_t minD = nowNs + kStepQuantumNs;
        for (int i = 0; i < nodeCount_; ++i) {
            if (!nodes_[i].alive || !nodes_[i].election) continue;
            int64_t d = nodes_[i].election->nextDeadlineNs();
            if (d > nowNs && d < minD) minD = d;
        }
        return minD;
    }

    // Deliver pending delayed messages that are due.
    void deliverPending(int64_t /*nowNs*/) {
        for (auto it = pendingMsgs_.begin(); it != pendingMsgs_.end(); ) {
            if (it->deliverAtStep <= step_) {
                // Check kill/partition at delivery time too.
                if (!killedNodes_.count(it->from) &&
                    !killedNodes_.count(it->to) &&
                    !isCrossPartition(it->from, it->to)) {
                    injectInto(it->to, it->bytes.data(), it->bytes.size());
                }
                it = pendingMsgs_.erase(it);
            } else {
                ++it;
            }
        }
    }

    // Write bytes directly into a node's IngressRing.
    void injectInto(NodeId to, const uint8_t* buf, std::size_t len) {
        if (to < 0 || to >= nodeCount_) return;
        if (!nodes_[to].alive)         return;
        nodes_[to].ring->write(buf, len);
    }

    // Returns true if sending from→to crosses the active partition boundary.
    bool isCrossPartition(NodeId from, NodeId to) const {
        if (partitionA_.empty() && partitionB_.empty()) return false;
        bool fromInA = partitionA_.count(from) > 0;
        bool fromInB = partitionB_.count(from) > 0;
        bool toInA   = partitionA_.count(to)   > 0;
        bool toInB   = partitionB_.count(to)   > 0;
        return (fromInA && toInB) || (fromInB && toInA);
    }

    // -----------------------------------------------------------------------
    // State
    // -----------------------------------------------------------------------
    int          nodeCount_;
    std::vector<NodeBundle> nodes_;

    // Fault injection.
    std::set<NodeId>  killedNodes_;
    std::set<NodeId>  partitionA_;
    std::set<NodeId>  partitionB_;
    double            dropRate_;
    int               delaySteps_;
    bool              reorder_;

    // Pending delayed messages.
    std::deque<PendingMsg> pendingMsgs_;

    // Seeded RNG for determinism.
    std::mt19937_64 rng_;

    // Step counter (for delay policy).
    int step_{0};
};

// ---------------------------------------------------------------------------
// BusSink::send — implementation (needs full ClusterHarness definition).
// ---------------------------------------------------------------------------
inline void BusSink::send(NodeId toMember, const uint8_t* buf, std::size_t len) {
    harness_.deliver(fromId_, toMember, buf, len);
}

} // namespace rune::consensus::test
