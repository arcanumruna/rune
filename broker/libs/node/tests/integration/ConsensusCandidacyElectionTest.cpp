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
#include <memory>
#include <set>
#include <stdexcept>
#include <string>
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
