// ---------------------------------------------------------------------------
// ConsensusCandidacyElectionTest.cpp — DUR-1 round-4 follow-up LOW-B.
//
// The consensus candidacy gate (Election::setVoteGate over the durable
// __cluster_metadata ISR knowledge), the template-58 knowledge exchange
// (BrokerNode.cpp setCandidacyExchange lambdas) and the internal-ISR ack-term
// source (Election::ackTermId) were proven only by unit tests and the DR model
// (all-to-all, instantaneous exchange).  These tests drive them END TO END over
// real Aeron: in-process BrokerNodes on loopback, each with its own embedded
// media driver, real consensus + replication transports.  No Docker.
//
//   (a) ExcludedMemberNeverConsensusLeader_CommittedMetadataSurvives
//       A node fenced out of the committed metadata ISR while dead, restarted
//       after the leader died, can never become consensus leader; metadata
//       records committed while it was dead survive.
//   (b) StaleKnowledgeLearnedThroughExchange_SurvivorsElect
//       Two survivors whose own knowledge excludes each other (one missed the
//       newest internal-ISR frames): without the exchange the cluster is
//       headless; through the canvass/vote exchange the stale node learns the
//       newest state and the cluster elects (MAJOR-A liveness).
//   (c) DeniedCandidacyNeverFreezesLiveLeaderMetadataIsr
//       A RequestVote of an excluded member at term T+1 is denied by every
//       live node (raising their candidate term) — the live leader at T must
//       still commit a member removal (MAJOR-R3).
//
// Waits are condition polls with deadlines; every poll iteration is a control-
// thread round trip on a live node (paced by its duty cycle) — no sleeps.
// ---------------------------------------------------------------------------
#include "../support/TestNodeConfig.h"
#include <rune/aeronutil/testing/FreePort.h>

#include <rune/consensus/Election.h>
#include <rune/consensus/ElectionState.h>
#include <rune/consensus/SbeConsensusPublisher.h>
#include <rune/consensus/UdpMessageSink.h>
#include <rune/node/BrokerNode.h>
#include "support/TestCluster.h"
#include <rune/node/EmbeddedDriver.h>
#include <rune/node/NodeConfig.h>
#include <rune/node/StorageIdentity.h>
#include <rune/replication/InternalIsrKnowledge.h>
#include <rune/replication/PartitionReplica.h>

#include <Aeron.h>
#include <gtest/gtest.h>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <functional>
#include <memory>
#include <set>
#include <stdexcept>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

namespace fs = std::filesystem;
using namespace std::chrono_literals;
using rune::node::BrokerNode;
using rune::node::NodeConfig;
using rune::replication::InternalIsrKnowledge;
using rune::replication::InternalIsrState;
using rune::replication::PartitionReplica;

namespace {

constexpr int32_t kMetaTopic = InternalIsrKnowledge::kCandidacyTopicId;   // -3

struct CceDir {
    fs::path path;
    explicit CceDir(std::string_view tag) {
        static std::atomic<uint32_t> seq{0};
        path = fs::temp_directory_path() /
               ("rune_cce_" + std::string(tag) + "_" + std::to_string(::getpid()) + "_" +
                std::to_string(seq.fetch_add(1)));
        fs::create_directories(path);
    }
    ~CceDir() { std::error_code ec; fs::remove_all(path, ec); }
};

// D11: never an ephemeral port — the kernel hands a just-freed ephemeral port to
// the next process that binds port 0, so concurrently running test binaries
// collided ("Address already in use" at gateway construction).
uint16_t freeUdpPort() { return rune::aeronutil::testing::allocFreeUdpPort(); }

/// Distinct free loopback UDP ports.
std::vector<uint16_t> freePorts(int n) {
    std::set<uint16_t> s;
    while (static_cast<int>(s.size()) < n) s.insert(freeUdpPort());
    return {s.begin(), s.end()};
}

/// Election view of one node, read on its control thread.
struct ElectionView {
    rune::consensus::ElectionState state{};
    int32_t leader        = -1;
    int64_t leadershipTerm = -1;
    int64_t candidateTerm = -1;
    int64_t ackTerm       = -1;
    [[nodiscard]] bool closedWith(int32_t l) const noexcept {
        return state == rune::consensus::ElectionState::Closed && leader == l;
    }
};

/// The metadata replica (__cluster_metadata) of one node, read on its owner
/// (control) thread.
struct MetaView {
    bool    present = false;
    bool    leader  = false;
    bool    pending = false;
    int64_t hw = -1, leo = -1;
    std::vector<int32_t> isr;
};

std::string str(const InternalIsrState& s) {
    std::string r = "(e" + std::to_string(s.leaderEpoch) + ",v" + std::to_string(s.version) + "){";
    for (int i = 0; i < s.count; ++i) r += (i ? "," : "") + std::to_string(s.members[static_cast<std::size_t>(i)]);
    return r + "}";
}
bool sameState(const InternalIsrState& a, const InternalIsrState& b) {
    return a.leaderEpoch == b.leaderEpoch && a.version == b.version;
}

/// N in-process BrokerNodes on loopback (real Aeron consensus + replication).
class Cluster {
public:
    explicit Cluster(int n) : n_(n) {
        const auto p = freePorts(3 * n);
        for (int i = 0; i < n; ++i) {
            cons_.push_back(p[static_cast<std::size_t>(i)]);
            client_.push_back(p[static_cast<std::size_t>(n + i)]);
            repl_.push_back(p[static_cast<std::size_t>(2 * n + i)]);
            dirs_.push_back(std::make_unique<CceDir>("n" + std::to_string(i)));
        }
        nodes_.resize(static_cast<std::size_t>(n));
    }
    ~Cluster() { for (int i = 0; i < n_; ++i) stop(i); }

    [[nodiscard]] NodeConfig config(int id) const {
        NodeConfig cfg;
        rune::node::test::pinLegacyRuntime(cfg);
        cfg.nodeId                   = id;
        cfg.dataDir                  = dirs_[static_cast<std::size_t>(id)]->path.string();
        cfg.consensusStreamId        = 60;
        cfg.replicationStreamId      = 70;
        cfg.electionTimeoutMs        = 150;
        cfg.electionStatusIntervalMs = 20;
        cfg.startupCanvassTimeoutMs  = 100;
        for (int i = 0; i < n_; ++i) {
            rune::node::PeerConfig pc;
            pc.id              = i;
            pc.host            = "127.0.0.1";
            pc.consensusPort   = cons_[static_cast<std::size_t>(i)];
            pc.clientPort      = client_[static_cast<std::size_t>(i)];
            pc.replicationPort = repl_[static_cast<std::size_t>(i)];
            cfg.peers.push_back(pc);
        }
        cfg.clientEndpoint.channel  = "aeron:udp?endpoint=127.0.0.1:" + std::to_string(client_[static_cast<std::size_t>(id)]);
        cfg.clientEndpoint.streamId = 80;
        return cfg;
    }
    /// The test topic (created by DDL once the cluster settled): every node leads a partition, so a death
    /// moves one (metadata records).
    [[nodiscard]] rune::node::TopicSpec topic() const {
        rune::node::TopicSpec t;
        t.name           = "cce";
        t.partitionCount = n_;
        return t;
    }
    /// Define the test topic through the catalog, on the nodes that are up.
    [[nodiscard]] int32_t defineTopic() {
        std::vector<BrokerNode*> up;
        for (auto& n : nodes_) if (n) up.push_back(n.get());
        return rune::node::test::TestCluster(std::move(up)).defineTopic(topic());
    }

    void start(int id) {
        nodes_[static_cast<std::size_t>(id)] = std::make_unique<BrokerNode>(config(id));
        nodes_[static_cast<std::size_t>(id)]->start();
    }
    /// `start` with a config tweak (storageFormat = Auto / New, testCandidacyHoldAtBoot, testHoldFollowerFetchesMs).
    void startWith(int id, const std::function<void(NodeConfig&)>& tweak) {
        NodeConfig cfg = config(id);
        if (tweak) tweak(cfg);
        nodes_[static_cast<std::size_t>(id)] = std::make_unique<BrokerNode>(cfg);
        nodes_[static_cast<std::size_t>(id)]->start();
    }
    /// The data dir's contents only (no storage.meta): the node restarts with the format its config names.
    void wipeRaw(int id) {
        std::error_code ec;
        for (const auto& e : fs::directory_iterator(dirs_[static_cast<std::size_t>(id)]->path, ec))
            fs::remove_all(e.path(), ec);
    }
    /// A replaced disk the production way (ISR design v3 Step 5): the contents go, then `storage format --join`.
    void wipe(int id) {
        wipeRaw(id);
        ASSERT_EQ(rune::node::StorageIdentity::format(dirs_[static_cast<std::size_t>(id)]->path.string(),
                                                      rune::node::StorageIdentity::Origin::Joiner,
                                                      rune::node::StorageIdentity::Mode::None, false).code, 0);
    }
    /// storage.meta's `origin=` value ("" when absent) - read from the test thread (nothing shared in memory).
    [[nodiscard]] std::string originOnDisk(int id) const {
        std::ifstream in(dirs_[static_cast<std::size_t>(id)]->path / "storage.meta");
        std::string line;
        while (std::getline(in, line)) if (line.rfind("origin=", 0) == 0) return line.substr(7);
        return {};
    }
    void stop(int id) {
        auto& n = nodes_[static_cast<std::size_t>(id)];
        if (n) { n->stop(); n.reset(); }
    }
    [[nodiscard]] bool up(int id) const { return nodes_[static_cast<std::size_t>(id)] != nullptr; }
    BrokerNode& node(int id) { return *nodes_[static_cast<std::size_t>(id)]; }
    [[nodiscard]] int size() const noexcept { return n_; }
    [[nodiscard]] std::string knowledgePath(int id) const {
        return dirs_[static_cast<std::size_t>(id)]->path.string() + "/internal-isr.state";
    }
    [[nodiscard]] uint16_t consensusPort(int id) const { return cons_[static_cast<std::size_t>(id)]; }

    ElectionView election(int id) {
        return node(id).onControlThreadForTest([&] {
            ElectionView v;
            if (const auto* e = node(id).electionForTest()) {
                v.state          = e->state();
                v.leader         = e->leaderMemberId();
                v.leadershipTerm = e->leadershipTermId();
                v.candidateTerm  = e->candidateTermId();
                v.ackTerm        = e->ackTermId();
            }
            return v;
        });
    }
    /// The newest durable metadata-ISR state of a node (leaderEpoch -1 = none).
    InternalIsrState knowledge(int id) {
        return node(id).onControlThreadForTest([&] {
            InternalIsrState s{};
            if (const auto* k = node(id).internalIsrKnowledgeForTest())
                if (const auto* m = k->latest(kMetaTopic, 0)) s = *m;
            return s;
        });
    }
    MetaView meta(int id) {
        return node(id).onOwnerThreadForTest(kMetaTopic, 0, [](PartitionReplica* r) {
            MetaView v;
            if (r == nullptr) return v;
            v.present = true;
            v.leader  = r->role() == rune::replication::Role::Leader;
            v.pending = r->isrProposalPendingForTest() || r->internalSeedPendingForTest();
            v.hw      = r->highWatermark();
            v.leo     = r->logEndOffset();
            v.isr.assign(r->isr().begin(), r->isr().end());
            return v;
        });
    }

    /// Every live node in `ids` sees `l` as the consensus leader (Closed).
    bool agreeOn(int32_t l, std::initializer_list<int> ids) {
        for (int id : ids) if (!election(id).closedWith(l)) return false;
        return true;
    }
    /// The consensus leader agreed by every node in `ids`, -1 if none (yet).
    int32_t agreedLeader(std::initializer_list<int> ids) {
        int32_t l = -2;
        for (int id : ids) {
            const auto v = election(id);
            if (v.state != rune::consensus::ElectionState::Closed || v.leader < 0) return -1;
            if (l == -2) l = v.leader;
            else if (l != v.leader) return -1;
        }
        return l < 0 ? -1 : l;
    }

private:
    int                                      n_;
    std::vector<uint16_t>                    cons_, client_, repl_;
    std::vector<std::unique_ptr<CceDir>>     dirs_;
    std::vector<std::unique_ptr<BrokerNode>> nodes_;
};

/// Condition poll with a deadline.  `cond` must perform at least one control-
/// thread round trip (it paces the loop on a node's duty cycle — no sleep).
template <class F>
bool until(F&& cond, std::chrono::milliseconds budget) {
    const auto deadline = std::chrono::steady_clock::now() + budget;
    while (std::chrono::steady_clock::now() < deadline)
        if (cond()) return true;
    return false;
}

/// The cluster settled on one consensus leader whose metadata replica leads
/// with committed records and no ISR change in flight.  Returns it (-1 = no).
int32_t settledLeader(Cluster& c, std::initializer_list<int> ids, std::chrono::milliseconds budget) {
    int32_t l = -1;
    const bool ok = until([&] {
        l = c.agreedLeader(ids);
        if (l < 0) return false;
        const auto m = c.meta(l);
        return m.present && m.leader && !m.pending && m.hw > 0;
    }, budget);
    return ok ? l : -1;
}

bool contains(const std::vector<int32_t>& v, int32_t x) { return std::find(v.begin(), v.end(), x) != v.end(); }

} // namespace

// ===========================================================================
// (a) A node fenced out of the committed __cluster_metadata ISR while dead can
// never become consensus leader once restarted, and the metadata records
// committed while it was dead survive the leader's death.
//
// 3 nodes, leader L.  X dies; L fences it: {L,Y} commits on L + Y, and L
// commits metadata records X never received.  Y and L die; X and Y restart
// together (equal consensus logs and terms: a fair race).  X's own file still
// names X (it never heard of its removal), X may stand — Y denies it, and X's
// own vote is gated once it learns {L,Y} — so Y wins (X grants it), leading
// __cluster_metadata with every committed record; X catches up.  (Without the
// gate X wins about half of the races and Y truncates the committed records.)
// ===========================================================================
TEST(ConsensusCandidacyElectionRegression, ExcludedMemberNeverConsensusLeader_CommittedMetadataSurvives) {
    Cluster c(3);
    for (int i = 0; i < 3; ++i) c.start(i);
    const int32_t L = settledLeader(c, {0, 1, 2}, 20s);
    ASSERT_GE(L, 0) << "no settled consensus/metadata leader";
    ASSERT_EQ(c.defineTopic(), 0);
    const int X = (L + 1) % 3, Y = (L + 2) % 3;
    const int64_t xLeo = c.meta(X).leo;

    c.stop(X);
    InternalIsrState committed{};
    ASSERT_TRUE(until([&] {
        const auto kl = c.knowledge(L), ky = c.knowledge(Y);
        const auto m = c.meta(L);
        committed = kl;
        return !m.pending && !contains(m.isr, X) && sameState(kl, ky) &&
               kl.contains(L) && kl.contains(Y) && !kl.contains(X) && m.hw > xLeo;
    }, 30s)) << "X was never fenced out by a committed metadata ISR (with records it missed): L knows "
             << str(c.knowledge(L)) << ", Y knows " << str(c.knowledge(Y)) << ", L hw=" << c.meta(L).hw
             << " xLeo=" << xLeo;
    {
        InternalIsrKnowledge xFile(c.knowledgePath(X));     // X is down: read its durable view
        const auto* xs = xFile.latest(kMetaTopic, 0);
        ASSERT_TRUE(xs == nullptr || xs->contains(X)) << "precondition: X never heard of its removal";
    }
    const int64_t hw = c.meta(L).hw;

    // Y, then L die (neither stands alone first, so X's and Y's consensus logs
    // and candidate terms are equal: the restart below is a FAIR race — only
    // the candidacy gate keeps X out).  Both restart together.
    c.stop(Y);
    c.stop(L);
    c.start(X);
    c.start(Y);
    bool xLed = false;
    ASSERT_TRUE(until([&] {
        const auto vx = c.election(X);
        if (vx.closedWith(X)) xLed = true;
        return c.agreeOn(Y, {X, Y});
    }, 30s)) << "the survivors never elected Y (L=" << L << " X=" << X << " Y=" << Y << "): X=" << static_cast<int>(c.election(X).state) << "/"
             << c.election(X).leader << " Y=" << static_cast<int>(c.election(Y).state) << "/"
             << c.election(Y).leader;
    EXPECT_FALSE(xLed) << "X (outside the committed metadata ISR) became consensus leader";
    EXPECT_FALSE(c.election(Y).closedWith(X));

    // Y leads __cluster_metadata with every record committed before L died; X
    // (which had none of them) catches up instead of truncating anybody.
    ASSERT_TRUE(until([&] { const auto m = c.meta(Y); return m.leader && m.leo >= hw; }, 15s));
    EXPECT_TRUE(until([&] { return c.meta(X).leo >= hw && c.meta(Y).hw >= hw; }, 30s))
        << "committed metadata did not survive: hw before=" << hw << " Y hw=" << c.meta(Y).hw
        << " X leo=" << c.meta(X).leo;
    EXPECT_GE(c.meta(Y).leo, hw);
}

// ===========================================================================
// (b) MAJOR-A liveness through the real BrokerNode exchange wiring.
//
// 3 nodes, leader L.  Y dies: {L,X} commits (L + X).  X dies; Y restarts and
// is re-admitted: {L,Y} commits (L + Y) — X missed it.  L dies; X restarts.
// Now X's own durable view admits only {L,X} and Y's only {L,Y}: each denies
// the other, so without the knowledge exchange the consensus stays headless.
// Through template 58 (CanvassPosition / RequestVote / Vote) X learns the newer
// {L,Y}, grants Y, and the cluster elects Y.
// ===========================================================================
TEST(ConsensusCandidacyElectionRegression, StaleKnowledgeLearnedThroughExchange_SurvivorsElect) {
    Cluster c(3);
    for (int i = 0; i < 3; ++i) c.start(i);
    const int32_t L = settledLeader(c, {0, 1, 2}, 20s);
    ASSERT_GE(L, 0) << "no settled consensus/metadata leader";
    ASSERT_EQ(c.defineTopic(), 0);
    const int X = (L + 1) % 3, Y = (L + 2) % 3;

    c.stop(Y);
    ASSERT_TRUE(until([&] {
        const auto kl = c.knowledge(L), kx = c.knowledge(X);
        return !c.meta(L).pending && sameState(kl, kx) && kl.contains(X) && !kl.contains(Y);
    }, 30s)) << "{L,X} never committed: L knows " << str(c.knowledge(L)) << ", X " << str(c.knowledge(X));

    c.stop(X);
    c.start(Y);
    InternalIsrState ly{};
    ASSERT_TRUE(until([&] {
        if (!c.agreeOn(L, {L, Y})) return false;
        const auto kl = c.knowledge(L), ky = c.knowledge(Y);
        const auto m = c.meta(L);
        ly = kl;
        return m.leader && !m.pending && sameState(kl, ky) && kl.contains(Y) && !kl.contains(X);
    }, 30s)) << "L never re-admitted Y without X: L knows " << str(c.knowledge(L)) << ", Y "
             << str(c.knowledge(Y));
    {
        InternalIsrKnowledge xFile(c.knowledgePath(X));     // X is down: its durable view
        const auto* xs = xFile.latest(kMetaTopic, 0);
        ASSERT_NE(xs, nullptr);
        ASSERT_TRUE(xs->contains(X) && !xs->contains(Y))
            << "precondition: X's own knowledge excludes Y, got " << str(*xs);
        ASSERT_TRUE(ly.newerThan(xs->leaderEpoch, xs->version));
    }

    c.stop(L);
    c.start(X);
    ASSERT_TRUE(until([&] { return c.agreedLeader({X, Y}) >= 0; }, 30s))
        << "headless: the survivors never elected (X knows " << str(c.knowledge(X)) << ", Y knows "
        << str(c.knowledge(Y)) << ")";
    EXPECT_TRUE(c.agreeOn(Y, {X, Y})) << "the elected leader is not Y";
    const auto kx = c.knowledge(X);
    EXPECT_TRUE(kx.contains(Y)) << "X never learned the newer state through the exchange: " << str(kx);
    EXPECT_FALSE(ly.newerThan(kx.leaderEpoch, kx.version))
        << "X holds an older state " << str(kx) << " than the one it must have learned " << str(ly);
}

// ===========================================================================
// (c) MAJOR-R3 over real Aeron: a DENIED RequestVote never freezes the live
// leader's metadata ISR.
//
// 5 configured nodes, X (one of them) never runs: L fences it, {L,A,B,C}
// commits.  X's RequestVote for term T+1 (sent over Aeron to every running
// node with a real SbeConsensusPublisher) is denied by all of them — X is
// outside the newest durable ISR — and the denial raises their candidate term.
// C then dies: L (still leader at T) must commit {L,A,B} on L + A + B.  With
// the ack term reading the candidate term, A, B and L itself refused every
// state of term T: the removal never committed (metadata HW pinned by a dead
// member, with a live majority).
// ===========================================================================
TEST(ConsensusCandidacyElectionRegression, DeniedCandidacyNeverFreezesLiveLeaderMetadataIsr) {
    constexpr int kN = 5;
    constexpr int X = 4;                      // configured, never started
    Cluster c(kN);
    for (int i = 0; i < kN; ++i) if (i != X) c.start(i);
    const int32_t L = settledLeader(c, {0, 1, 2, 3}, 30s);
    ASSERT_GE(L, 0) << "no settled consensus/metadata leader";
    ASSERT_EQ(c.defineTopic(), 0);
    std::vector<int> others;
    for (int i = 0; i < kN; ++i) if (i != X && i != L) others.push_back(i);
    const int A = others[0], B = others[1], C = others[2];

    ASSERT_TRUE(until([&] {
        const auto kl = c.knowledge(L), ka = c.knowledge(A), kb = c.knowledge(B);
        const auto m = c.meta(L);
        return !m.pending && !contains(m.isr, X) && kl.count == 4 && !kl.contains(X) &&
               sameState(kl, ka) && sameState(kl, kb);
    }, 30s)) << "{L,A,B,C} never committed: L knows " << str(c.knowledge(L));
    const auto before = c.election(L);
    const int64_t T = before.leadershipTerm;

    // X stands for T+1: RequestVote over Aeron to every running node.
    CceDir xDir("rogue");
    rune::node::EmbeddedDriverConfig dc;
    dc.dir = (xDir.path / "aeron").string();
    rune::node::EmbeddedDriver driver(dc, rune::node::DriverMode::Threaded);
    aeron::Context ctx;
    ctx.aeronDir(dc.dir);
    auto aeronClient = aeron::Aeron::connect(ctx);
    std::unordered_map<rune::consensus::NodeId, rune::consensus::UdpMessageSink::PeerEndpoint> eps;
    for (int i = 0; i < kN; ++i)
        if (i != X) eps[i] = {"aeron:udp?endpoint=127.0.0.1:" + std::to_string(c.consensusPort(i)), 60};
    rune::consensus::UdpMessageSink sink(aeronClient, eps);
    rune::consensus::SbeConsensusPublisher pub(sink);
    auto deniedBy = [&](int id) { return c.election(id).candidateTerm > T; };
    ASSERT_TRUE(until([&] {
        for (int i = 0; i < kN; ++i)
            if (i != X)   // log far ahead: the candidacy gate alone decides
                pub.requestVote(i, /*logLeadershipTermId=*/T, /*logPosition=*/int64_t{1} << 40, T + 1, X);
        return deniedBy(L) && deniedBy(A) && deniedBy(B) && deniedBy(C);
    }, 15s)) << "X's candidacy was never denied by every live node";
    EXPECT_TRUE(c.agreeOn(L, {L, A, B, C})) << "the denied candidacy deposed the live leader";
    for (int id : {L, A, B, C})
        EXPECT_EQ(c.election(id).ackTerm, T) << "node " << id << ": a DENIED vote raised the ack term";

    // C dies: the live leader must still commit its removal on L + A + B.
    c.stop(C);
    EXPECT_TRUE(until([&] {
        const auto m = c.meta(L);
        const auto kl = c.knowledge(L), ka = c.knowledge(A), kb = c.knowledge(B);
        return !m.pending && !contains(m.isr, C) && kl.count == 3 && !kl.contains(C) &&
               sameState(kl, ka) && sameState(kl, kb);
    }, 30s)) << "the live leader's metadata ISR removal never committed after a denied candidacy: L knows "
             << str(c.knowledge(L)) << ", A " << str(c.knowledge(A)) << ", B " << str(c.knowledge(B))
             << ", L pending=" << c.meta(L).pending;
    EXPECT_TRUE(c.agreeOn(L, {L, A, B})) << "leadership changed — the scenario no longer pins MAJOR-R3";
    EXPECT_EQ(c.election(L).leadershipTerm, T);
}

// ===========================================================================
// ISR v3 Step 9 - the storage gate end to end.
// ===========================================================================
namespace {
using rune::node::StorageFormat;

std::string candidacyOf(Cluster& c, int id) {
    return c.node(id).onControlThreadForTest([&] { return std::string(c.node(id).electionForTest()->candidacyName()); });
}
int gateStateOf(Cluster& c, int id) {
    return c.node(id).onControlThreadForTest([&] { return c.node(id).storageGateStateForTest(); });
}
}  // namespace

// §3.6 / A-C2: a Joiner's candidacy hold is a LEVEL set at construction - the first canvass is already held, and it
// stays held while the storage is unconfirmed.  The joiner cannot fetch (testHoldFollowerFetchesMs), so its own view of
// the metadata never confirms it deterministically.  RED: the change-only call (the hold only on a gate transition).
TEST(StorageGateWiringRegression, JoinerElectionIsHeldFromBootRegression) {
    Cluster c(3);
    for (int i = 0; i < 3; ++i) c.start(i);
    const int32_t L = settledLeader(c, {0, 1, 2}, 20s);
    ASSERT_GE(L, 0);
    const int X = (L + 1) % 3;
    c.stop(X);
    c.wipe(X);
    c.startWith(X, [](NodeConfig& k) { k.testHoldFollowerFetchesMs = 60'000; });
    EXPECT_EQ(candidacyOf(c, X), "AwaitingStorage") << "the joiner was a candidate right after start()";
    std::this_thread::sleep_for(3 * 100ms);   // 3x the canvass timeout
    EXPECT_EQ(candidacyOf(c, X), "AwaitingStorage") << "the joiner stood for election";
    EXPECT_EQ(gateStateOf(c, X), 1);
}

// B-I8 + A-C2 (deterministic): a wiped, unconfirmed member never becomes consensus leader while the others hold, nor
// after they are released; a leader is elected and the committed metadata survives the hand-over.  Issue #53: the
// original premise - X, wiped and restarted as a Joiner, is still in the committed metadata ISR so every vote would go
// to it - no longer holds.  The broker fences (NodeRuntimeAgent shrinkIsrToLive, Kafka-style, ~450 ms) every internal
// partition's ISR down to the members the consensus leader sees as live and eligible, so X is out of the metadata ISR
// before the test can observe it.  What replaces "the others hold" is that fencing: X is not in the knowledge of L or Y,
// so no vote goes to it.  After L resigns it no longer fences (#572: only the consensus leader has liveness knowledge) -
// it clears its fenced mask, but X stays out: it keeps testHoldFollowerFetchesMs, so it never proves a validated position
// and never re-enters the ISR.  RED: removing the internal-partition fencing (NodeRuntimeAgent: shrinkIsrToLive on
// topicId < 0 leaders) fails the fencing assertion; fencing on a resigned leader too (#572) leaves Y fenced out and no
// leader is agreed.  This test does NOT pin X's own storage hold: with X fenced out, dropping the hold (both
// setStorageHold sites) stays green here - that hold is pinned by
// StorageGateWiringRegression.JoinerElectionIsHeldFromBootRegression (PR #574 review).
// Both non-X members are held before the member that currently leads is resigned (leadership can move L -> Y while X
// restarts, which made a resign of the stale L a silent no-op).
TEST(ConsensusCandidacyElectionRegression, WipedMemberNeverConsensusLeader_CommittedMetadataSurvivesRegression) {
    Cluster c(3);
    for (int i = 0; i < 3; ++i) c.start(i);
    const int32_t L = settledLeader(c, {0, 1, 2}, 20s);
    ASSERT_GE(L, 0);
    ASSERT_EQ(c.defineTopic(), 0);
    const int X = (L + 1) % 3, Y = (L + 2) % 3;
    const int64_t hw = c.meta(L).hw;

    c.stop(X);
    c.wipe(X);
    c.startWith(X, [](NodeConfig& k) { k.testHoldFollowerFetchesMs = 60'000; });
    ASSERT_TRUE(until([&] { return !c.knowledge(L).contains(X) && !c.knowledge(Y).contains(X); }, 5s))
        << "a wiped, unconfirmed member is fenced out of the committed metadata ISR";
    EXPECT_TRUE(c.knowledge(L).contains(Y)) << "precondition: L still lists Y in the metadata ISR";

    // Hold both members that can lead (X is fenced out) BEFORE choosing whom to resign: the consensus leader can
    // move between them while X restarts (under TSan load it did), and once both are held it can no longer move.
    c.node(Y).setTestCandidacyHold(true);
    c.node(L).setTestCandidacyHold(true);
    int32_t current = -1;
    ASSERT_TRUE(until([&] { current = c.agreedLeader({L, Y}); return current >= 0; }, 5s))
        << "no settled consensus leader among the two held members";
    ASSERT_TRUE(current == L || current == Y);
    ASSERT_TRUE(c.node(current).resignConsensusForTest()) << "the current consensus leader " << current << " did not resign";
    // 2 s window: ample to see an unheld X win if it could (it did so within ~0.3 s when it was still in the ISR).
    const auto window = std::chrono::steady_clock::now() + 2s;
    bool xLed = false, agreed = false;
    std::string isrHistory;   // diagnostics only: L's metadata-ISR knowledge over the window
    std::string lastK;
    const auto noteKnowledge = [&] {
        if (const std::string k = str(c.knowledge(L)); k != lastK) {
            lastK = k;
            isrHistory += " " + std::to_string(std::chrono::duration_cast<std::chrono::milliseconds>(
                              std::chrono::steady_clock::now().time_since_epoch()).count() % 100000) + ":" + k;
        }
    };
    while (std::chrono::steady_clock::now() < window) {
        for (int i = 0; i < 3; ++i) if (c.election(i).closedWith(X)) xLed = true;
        if (c.agreedLeader({0, 1, 2}) != -1) agreed = true;
        noteKnowledge();
        std::this_thread::sleep_for(10ms);
    }
    EXPECT_FALSE(xLed) << "the wiped member became consensus leader (L's ISR knowledge over the window:" << isrHistory << ")";
    EXPECT_FALSE(agreed) << "a leader was agreed while every candidate was held";

    c.node(Y).setTestCandidacyHold(false);
    c.node(L).setTestCandidacyHold(false);
    int32_t leader = -1;
    const bool elected = until([&] {
        for (int i = 0; i < 3; ++i) if (c.election(i).closedWith(X)) xLed = true;
        noteKnowledge();
        leader = c.agreedLeader({L, Y});
        return leader >= 0;
    }, 15s);
    EXPECT_FALSE(xLed) << "the wiped member became consensus leader";
    ASSERT_TRUE(elected) << "no leader was agreed after both holds were released (L's ISR knowledge:" << isrHistory
                         << " knowY=" << str(c.knowledge(Y)) << ")";
    EXPECT_NE(leader, X) << "the wiped member became consensus leader";
    EXPECT_TRUE(until([&] {
        for (int i = 0; i < 3; ++i) if (c.election(i).closedWith(X)) xLed = true;
        const auto m = c.meta(leader);
        return m.leader && m.leo >= hw;
    }, 15s)) << "committed metadata was lost across the hand-over to " << leader << " (leo=" << c.meta(leader).leo
             << " hw=" << hw << ")";
    EXPECT_FALSE(xLed) << "the wiped member became consensus leader";
}

// #572: a resigned consensus leader never fences a live peer out of the internal (metadata) ISR.  Only the consensus
// leader receives AppendPosition, so only it knows who is live; after Election::resign the old leader's liveness decays
// and - before the fix - the fencing pass (NodeRuntimeAgent, shrinkIsrToLive on every internal partition it still leads)
// shrank the committed metadata ISR to itself within ~450 ms.  Y acked that, was no longer electable, and with L held
// no member could win.  RED: shrink on every node regardless of consensus leadership (the leader gate in
// NodeRuntimeAgent's failover slow tick).
TEST(ConsensusCandidacyElectionRegression, AResignedLeaderNeverFencesALivePeerOutOfTheMetadataIsrRegression) {
    Cluster c(3);
    for (int i = 0; i < 3; ++i) c.start(i);
    const int32_t L = settledLeader(c, {0, 1, 2}, 20s);
    ASSERT_GE(L, 0);
    ASSERT_EQ(c.defineTopic(), 0);
    const int X = (L + 1) % 3, Y = (L + 2) % 3;
    const auto allIn = [&](const InternalIsrState& k) { return k.contains(L) && k.contains(X) && k.contains(Y); };
    ASSERT_TRUE(until([&] { return allIn(c.knowledge(L)) && allIn(c.knowledge(Y)); }, 5s))
        << "precondition: every member is in the committed metadata ISR (knowL=" << str(c.knowledge(L))
        << " knowY=" << str(c.knowledge(Y)) << ")";
    const int64_t hw = c.meta(L).hw;

    c.node(X).setTestCandidacyHold(true);
    c.node(Y).setTestCandidacyHold(true);
    c.node(L).setTestCandidacyHold(true);
    c.node(L).resignConsensusForTest();
    // 2 s window: more than 3x the failover timeout (3 x leaderHeartbeatTimeout; the issue saw Y fenced at ~450 ms).
    const auto window = std::chrono::steady_clock::now() + 2s;
    std::string firstLoss;
    while (std::chrono::steady_clock::now() < window && firstLoss.empty()) {
        const auto kl = c.knowledge(L), ky = c.knowledge(Y);
        if (!kl.contains(X) || !kl.contains(Y) || !ky.contains(X) || !ky.contains(Y))
            firstLoss = "knowL=" + str(kl) + " knowY=" + str(ky);
        std::this_thread::sleep_for(10ms);
    }
    EXPECT_TRUE(firstLoss.empty()) << "the resigned leader fenced a live peer out of the metadata ISR: " << firstLoss;

    c.node(Y).setTestCandidacyHold(false);
    EXPECT_TRUE(until([&] { return c.agreedLeader({0, 1, 2}) == Y; }, 10s))
        << "Y never led after its hold was released (knowL=" << str(c.knowledge(L)) << " knowY=" << str(c.knowledge(Y))
        << " agreed=" << c.agreedLeader({0, 1, 2}) << ")";
    EXPECT_TRUE(until([&] { const auto m = c.meta(Y); return m.leader && m.leo >= hw; }, 10s))
        << "committed metadata was lost across the hand-over (leo=" << c.meta(Y).leo << " hw=" << hw << ")";
}

// PR #601 adversarial review (#572, sibling entry point + leader flap): the consensus leader STANDS DOWN
// (Election::standDown - the halted-applier path, not resign) twice with the other members held, so each time no node
// leads for longer than the fencing timeout (3 x leaderHeartbeatTimeout); after each hold it stands again and wins
// (leader -> not leader -> leader).  No live member may leave ANY node's committed metadata ISR at any point, and after
// the flaps the metadata log still commits (a second DDL) with every member in the ISR.  Seeded: the hold of each
// flap is drawn from [2000, 2400) ms (the seed is printed on failure).  RED: drop the consensus-leader gate in
// NodeRuntimeAgent's failover slow tick (shrink on every node) - the stood-down leader fences X and Y.
TEST(ConsensusCandidacyElectionRegression, StoodDownLeaderFlapNeverFencesALivePeerAdversarialRegression) {
    const uint32_t seed = 601u;
    SCOPED_TRACE("seed=" + std::to_string(seed));
    uint32_t rng = seed;
    const auto next = [&] { rng = rng * 1664525u + 1013904223u; return rng >> 8; };
    Cluster c(3);
    for (int i = 0; i < 3; ++i) c.start(i);
    const int32_t L = settledLeader(c, {0, 1, 2}, 20s);
    ASSERT_GE(L, 0);
    ASSERT_EQ(c.defineTopic(), 0);
    const int X = (L + 1) % 3, Y = (L + 2) % 3;
    const auto allIn = [&](const InternalIsrState& k) { return k.contains(L) && k.contains(X) && k.contains(Y); };
    ASSERT_TRUE(until([&] { return allIn(c.knowledge(L)) && allIn(c.knowledge(X)) && allIn(c.knowledge(Y)); }, 5s))
        << "precondition: every member is in the committed metadata ISR";
    const int64_t hw0 = c.meta(L).hw;

    c.node(X).setTestCandidacyHold(true);
    c.node(Y).setTestCandidacyHold(true);
    std::string firstLoss;
    const auto check = [&] {
        if (!firstLoss.empty()) return;
        const auto kl = c.knowledge(L), kx = c.knowledge(X), ky = c.knowledge(Y);
        if (!allIn(kl) || !allIn(kx) || !allIn(ky))
            firstLoss = "knowL=" + str(kl) + " knowX=" + str(kx) + " knowY=" + str(ky);
    };
    for (int flap = 0; flap < 2; ++flap) {
        SCOPED_TRACE("flap " + std::to_string(flap));
        const int64_t holdMs = 2000 + static_cast<int64_t>(next() % 400);
        ASSERT_TRUE(c.node(L).standDownConsensusForTest(holdMs)) << "precondition: L leads and stands down";
        bool sawHeadless = false;
        // Never L while held; L wins again once the hold ends (X and Y only vote).
        const bool releed = until([&] {
            check();
            const auto v = c.election(L);
            if (!v.closedWith(L)) sawHeadless = true;
            return sawHeadless && c.agreedLeader({0, 1, 2}) == L;
        }, 20s);
        ASSERT_TRUE(sawHeadless);
        ASSERT_TRUE(releed) << "L never led again after its stand-down hold (agreed=" << c.agreedLeader({0, 1, 2})
                            << " knowL=" << str(c.knowledge(L)) << ")";
        EXPECT_TRUE(firstLoss.empty()) << "a live member left the committed metadata ISR: " << firstLoss;
    }
    c.node(X).setTestCandidacyHold(false);
    c.node(Y).setTestCandidacyHold(false);

    // The metadata log still commits after the flaps, with every member in its ISR.
    rune::node::TopicSpec t2 = c.topic();
    t2.name = "cce2";
    std::vector<BrokerNode*> up{&c.node(0), &c.node(1), &c.node(2)};
    EXPECT_GE(rune::node::test::TestCluster(std::move(up)).defineTopic(t2), 0) << "no DDL commits after the flaps";
    EXPECT_TRUE(until([&] { check(); const auto m = c.meta(L); return m.leader && m.hw > hw0 && m.isr.size() == 3; },
                      15s))
        << "the metadata HW did not advance with the full ISR (hw=" << c.meta(L).hw << " hw0=" << hw0
        << " isr=" << c.meta(L).isr.size() << ")";
    EXPECT_TRUE(firstLoss.empty()) << "a live member left the committed metadata ISR: " << firstLoss;
}

// R-I1: three nodes formatted `auto`, all held at boot, decide their storage first; releasing the holds forms the
// cluster and every node is a Founder.  RED: decide Joiner on any heard peer.
TEST(ConsensusCandidacyElectionRegression, FreshClusterFormsWithEveryNodeFounderRegression) {
    Cluster c(3);
    for (int i = 0; i < 3; ++i)
        c.startWith(i, [](NodeConfig& k) { k.storageFormat = StorageFormat::Auto; k.testCandidacyHoldAtBoot = true; });
    for (int i = 0; i < 3; ++i)
        ASSERT_TRUE(until([&] { return gateStateOf(c, i) != 2; }, 30s)) << "node " << i << " never left UNDECIDED";
    for (int i = 0; i < 3; ++i) c.node(i).setTestCandidacyHold(false);
    ASSERT_TRUE(until([&] { return c.agreedLeader({0, 1, 2}) >= 0; }, 10s)) << "no leader after the holds were released";
    for (int i = 0; i < 3; ++i) EXPECT_EQ(c.originOnDisk(i), "founder") << "node " << i;
}

// A node that starts minutes after the others and hears an established cluster joins as a Joiner and is confirmed -
// the first two found.  RED: a unanimity founding rule.
TEST(ConsensusCandidacyElectionRegression, SequentialStartStillFormsRegression) {
    Cluster c(3);
    const auto autoFmt = [](NodeConfig& k) { k.storageFormat = StorageFormat::Auto; };
    c.startWith(0, autoFmt);
    std::this_thread::sleep_for(2s);
    c.startWith(1, autoFmt);
    ASSERT_TRUE(until([&] { return c.agreedLeader({0, 1}) >= 0; }, 30s)) << "0 and 1 never formed a cluster";
    std::this_thread::sleep_for(2s);
    c.startWith(2, autoFmt);
    ASSERT_TRUE(until([&] { return c.originOnDisk(2) == "joiner"; }, 30s)) << "node 2 never decided Joiner";
    EXPECT_TRUE(until([&] { return gateStateOf(c, 2) == 0; }, 30s)) << "node 2 was never CONFIRMED";
    EXPECT_EQ(c.originOnDisk(0), "founder");
    EXPECT_EQ(c.originOnDisk(1), "founder");
}

// v2-review C1: an Undecided cluster founds at runtime and its controller BOOTSTRAPS the metadata log - the
// mayBootstrap thunk is read at the claim, from the live origin.  RED: evaluate it once in BrokerNode::start and keep
// the bool (origin Undecided -> false -> the controller halts and resigns; no ready controller ever exists).
TEST(ConsensusCandidacyElectionRegression, FreshUndecidedThreeNodeClusterBootstrapsRegression) {
    Cluster c(3);
    for (int i = 0; i < 3; ++i)
        c.startWith(i, [](NodeConfig& k) { k.storageFormat = StorageFormat::Auto; });
    int32_t L = -1;
    ASSERT_TRUE(until([&] { L = c.agreedLeader({0, 1, 2}); return L >= 0; }, 30s)) << "no consensus leader";
    EXPECT_TRUE(until([&] {
        return c.node(L).onControlThreadForTest([&] {
            auto* md = c.node(L).metadataServiceForTest();
            return md != nullptr && md->controllerReady() &&
                   md->featureLevel() == rune::replication::MetadataFeature::kLatest;
        });
    }, 30s)) << "the leader never became a ready controller at the latest feature level";
}

// Replaces v2's fail-stop: `--new` formatted into a RUNNING cluster joins it instead of founding; it grants no vote
// until CONFIRMED.  RED: the founder rule on mode New.
TEST(StorageFormatRegression, NewFormattedIntoARunningClusterJoinsRegression) {
    Cluster c(3);
    for (int i = 0; i < 3; ++i) c.start(i);
    const int32_t L = settledLeader(c, {0, 1, 2}, 20s);
    ASSERT_GE(L, 0);
    const int X = (L + 1) % 3;
    c.stop(X);
    c.wipeRaw(X);
    c.startWith(X, [](NodeConfig& k) { k.storageFormat = StorageFormat::New; });
    ASSERT_TRUE(until([&] { return c.originOnDisk(X) == "joiner"; }, 10s)) << "X never decided Joiner";
    bool confirmed = false;
    ASSERT_TRUE(until([&] {
        const auto r = c.node(X).onControlThreadForTest([&] {
            return std::make_pair(c.node(X).storageGateStateForTest(), c.node(X).electionForTest()->votesGrantedForTest());
        });
        if (r.first != 0) EXPECT_EQ(r.second, 0u) << "a vote was granted by an unconfirmed storage";
        confirmed = r.first == 0;
        return confirmed;
    }, 30s)) << "X was never CONFIRMED";
}

// C1 (CP2 review): history is transitive in the canvass flags - a Joiner that never led still advertises history, so an
// Undecided peer cannot count it as "fresh".  Both other nodes are stopped: L has no quorum, only canvasses (no NLT), so
// the wiped X learns of history from L's canvass flag alone and holds no term of its own.
// RED: the old flag formula (boot history / own terms only) gives 2.
TEST(StorageGateWiringRegression, AJoinerAdvertisesHistoryRegression) {
    Cluster c(3);
    for (int i = 0; i < 3; ++i) c.start(i);
    const int32_t L = settledLeader(c, {0, 1, 2}, 20s);
    ASSERT_GE(L, 0);
    const int X = (L + 1) % 3, Y = (L + 2) % 3;
    c.stop(X);
    c.stop(Y);
    c.wipe(X);
    c.startWith(X, [](NodeConfig& k) { k.testHoldFollowerFetchesMs = 60'000; });
    EXPECT_TRUE(until([&] {
        return c.node(X).onControlThreadForTest([&] { return c.node(X).canvassFlagsForTest(); }) == 3;
    }, 5s)) << "a joiner advertised fresh flags (" << c.node(X).onControlThreadForTest([&] { return c.node(X).canvassFlagsForTest(); }) << ")";
}

// C1-R: the founding dwell must cover the transport's reconnect to a restarted peer (resolver recheck 1 s + pending
// backoff 1 s).  RED: the old max(300 ms, 3 x status) dwell.
TEST(StorageGateWiringRegression, FoundingDwellCoversPeerReResolutionRegression) {
    Cluster c(3);
    for (int i = 0; i < 3; ++i) c.start(i);
    ASSERT_GE(settledLeader(c, {0, 1, 2}, 20s), 0);
    const int64_t dwell = c.node(0).onControlThreadForTest([&] { return c.node(0).foundingLongDwellNsForTest(); });
    EXPECT_GE(dwell, rune::node::PeerResolverHub::kRecheckNs + rune::node::PeerResolverHub::kPendingMaxNs);
}

// C1 / C1-R (CP2): two replaced disks restart (auto) next to one live node L that holds the history, while L's frames do
// not reach them for 1.5 s (an address change: the transport reconnects late).  Neither wiped node may found over L: no
// wiped node leads and L's committed metadata is never truncated.  `dropOnlyToX2`: only X2 loses L's frames - X2 must
// then join through X1's history flag.  Run with --gtest_repeat=20.
// RED: the 300 ms dwell (both found at once) / without the transitive flag (X2 founds on X1's fresh canvass).
void twoReplacedDisksScenario(bool dropOnlyToX2) {
    Cluster c(3);
    for (int i = 0; i < 3; ++i) c.start(i);
    const int32_t L = settledLeader(c, {0, 1, 2}, 20s);
    ASSERT_GE(L, 0);
    ASSERT_EQ(c.defineTopic(), 0);
    const int X1 = (L + 1) % 3, X2 = (L + 2) % 3;
    const int64_t hw = c.meta(L).hw;
    c.stop(X1);
    c.stop(X2);
    c.wipeRaw(X1);
    c.wipeRaw(X2);
    const auto fmt = [&](bool drop) {
        return [drop, L](NodeConfig& k) {
            k.storageFormat = StorageFormat::Auto;
            k.testHoldFollowerFetchesMs = 60'000;   // the data plane would hand them L's history too: model the dead link
            if (drop) { k.testDropConsensusFromPeer = L; k.testDropConsensusForMs = 1500; }
        };
    };
    c.startWith(X1, fmt(!dropOnlyToX2));   // both restart together: each first hears the other's "fresh" canvass
    c.startWith(X2, fmt(true));
    ASSERT_TRUE(until([&] { return c.originOnDisk(X1) == "joiner"; }, 20s)) << "X1 decided " << c.originOnDisk(X1);
    ASSERT_TRUE(until([&] { return c.originOnDisk(X2) == "joiner"; }, 20s))
        << "X2 decided " << c.originOnDisk(X2) << ": it founded over a live history node";
    const auto end = std::chrono::steady_clock::now() + 5s;
    while (std::chrono::steady_clock::now() < end) {
        EXPECT_FALSE(c.election(X1).closedWith(X1) || c.election(X1).closedWith(X2));
        EXPECT_FALSE(c.election(X2).closedWith(X1) || c.election(X2).closedWith(X2));
        EXPECT_GE(c.meta(L).leo, hw) << "the live node's committed metadata was truncated";
        std::this_thread::sleep_for(20ms);
    }
}
TEST(ConsensusCandidacyElectionRegression, TwoReplacedDisksNeverFoundOverALiveHistoryMemberRegression) {
    twoReplacedDisksScenario(false);
}
TEST(ConsensusCandidacyElectionRegression, ReplacedDiskNeverFoundsOnAJoinersFreshCanvassRegression) {
    twoReplacedDisksScenario(true);
}

// CP2 review M6: the abstention is switched OFF by the unsafe recovery vote (and only by it) on an unconfirmed storage.
// RED: drop the `!storageRecoveryVote` term in BrokerNode::abstainGateThunk_.
TEST(StorageGateWiringRegression, RecoveryVoteSwitchesAbstentionOffRegression) {
    Cluster c(3);
    for (int i = 0; i < 3; ++i) c.start(i);
    const int32_t L = settledLeader(c, {0, 1, 2}, 20s);
    ASSERT_GE(L, 0);
    const int X = (L + 1) % 3;
    c.stop(X);
    c.wipe(X);
    c.startWith(X, [](NodeConfig& k) { k.testHoldFollowerFetchesMs = 60'000; });
    ASSERT_EQ(gateStateOf(c, X), 1);
    EXPECT_TRUE(c.node(X).onControlThreadForTest([&] { return c.node(X).abstainsForTest(); }))
        << "an unconfirmed storage must abstain";
    c.stop(X);
    c.startWith(X, [](NodeConfig& k) { k.testHoldFollowerFetchesMs = 60'000; k.storageRecoveryVote = true; });
    ASSERT_EQ(gateStateOf(c, X), 1);
    EXPECT_FALSE(c.node(X).onControlThreadForTest([&] { return c.node(X).abstainsForTest(); }))
        << "the recovery vote must turn abstention off";
}
