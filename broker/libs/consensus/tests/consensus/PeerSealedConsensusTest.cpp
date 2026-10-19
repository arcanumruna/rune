// PeerSealedConsensusTest.cpp — authentication plan Task 16 (D34): the consensus traffic of three real Election /
// ConsensusAgent nodes over REAL Aeron UDP (loopback), every frame sealed by the sink and opened by the ingress.
// One test thread plays the control thread of all three nodes (each node's objects are only ever touched from it).
#include <gtest/gtest.h>

extern "C" {
#include <aeronmd/aeronmd.h>
}
#include <Aeron.h>
#include <Context.h>

#include <rune/aeronutil/testing/FreePort.h>
#include <rune/consensus/AeronIngress.h>
#include <rune/consensus/ClusterMember.h>
#include <rune/consensus/ConsensusAgent.h>
#include <rune/consensus/Election.h>
#include <rune/consensus/ElectionState.h>
#include <rune/consensus/IngressRing.h>
#include <rune/consensus/NodeStateFile.h>
#include <rune/consensus/SbeConsensusPublisher.h>
#include <rune/consensus/TermLog.h>
#include <rune/consensus/UdpMessageSink.h>
#include <rune/peerlink/PeerFrameCrypto.h>

#include <atomic>
#include <chrono>
#include <filesystem>
#include <memory>
#include <thread>
#include <unistd.h>

namespace fs = std::filesystem;
namespace pl = rune::peerlink;
using namespace rune::consensus;
using namespace std::chrono_literals;

namespace {

class MediaDriver {
public:
    MediaDriver() {
        static std::atomic<int> seq{0};
        dir_ = (fs::temp_directory_path() / ("rune_psc_" + std::to_string(::getpid()) + "_" + std::to_string(seq++))).string();
        if (aeron_driver_context_init(&ctx_) < 0) throw std::runtime_error(aeron_errmsg());
        aeron_driver_context_set_dir(ctx_, dir_.c_str());
        aeron_driver_context_set_threading_mode(ctx_, AERON_THREADING_MODE_SHARED);
        aeron_driver_context_set_dir_delete_on_start(ctx_, true);
        aeron_driver_context_set_dir_delete_on_shutdown(ctx_, true);
        aeron_driver_context_set_shared_idle_strategy(ctx_, "sleep-ns");
        aeron_driver_context_set_term_buffer_sparse_file(ctx_, true);
        aeron_driver_context_set_term_buffer_length(ctx_, 64 * 1024);
        if (aeron_driver_init(&driver_, ctx_) < 0) throw std::runtime_error(aeron_errmsg());
        if (aeron_driver_start(driver_, true) < 0) throw std::runtime_error(aeron_errmsg());
        running_.store(true);
        thread_ = std::thread([this] { while (running_.load()) aeron_driver_main_idle_strategy(driver_, aeron_driver_main_do_work(driver_)); });
    }
    ~MediaDriver() {
        running_.store(false);
        if (thread_.joinable()) thread_.join();
        aeron_driver_close(driver_);
        aeron_driver_context_close(ctx_);
    }
    [[nodiscard]] const std::string& dir() const { return dir_; }
private:
    aeron_driver_context_t* ctx_ = nullptr;
    aeron_driver_t*         driver_ = nullptr;
    std::string             dir_;
    std::atomic<bool>       running_{false};
    std::thread             thread_;
};

struct TempDir {
    fs::path dir;
    explicit TempDir(const std::string& p) {
        static std::atomic<int> c{0};
        dir = fs::temp_directory_path() / (p + std::to_string(::getpid()) + "_" + std::to_string(c++));
        fs::create_directories(dir);
    }
    ~TempDir() { std::error_code ec; fs::remove_all(dir, ec); }
};

pl::LinkKeys linkFor(bool lowToHigh, uint8_t base) {
    pl::LinkKeys k; k.epoch = 1;
    k.sendMaster.fill(lowToHigh ? base : static_cast<uint8_t>(base + 1));
    k.recvMaster.fill(lowToHigh ? static_cast<uint8_t>(base + 1) : base);
    return k;
}

constexpr int32_t kConsensusStream = 90;

struct Node {
    int32_t                               id;
    TempDir                               dir;
    std::unique_ptr<IngressRing>          ring;
    std::unique_ptr<AeronIngress>         ingress;
    std::unique_ptr<UdpMessageSink>       sink;
    std::unique_ptr<SbeConsensusPublisher> publisher;
    std::unique_ptr<NodeStateFile>        nsf;
    std::unique_ptr<TermLog>              tl;
    std::unique_ptr<Election>             election;
    std::unique_ptr<ConsensusAgent>       agent;
    explicit Node(int32_t i) : id(i), dir("psc_n" + std::to_string(i) + "_") {}
};

/// Three consensus nodes (ids 1..3) over UDP loopback.  `sealedNodes`: the nodes built PeerSealed.
struct Cluster {
    MediaDriver                   driver;
    std::shared_ptr<aeron::Aeron> aeron;
    std::vector<uint16_t>         ports;
    std::vector<std::unique_ptr<Node>> nodes;

    explicit Cluster(bool sealedAll = true) {
        aeron::Context ctx; ctx.aeronDir(driver.dir());
        aeron = aeron::Aeron::connect(ctx);
        for (int i = 0; i < 3; ++i) ports.push_back(rune::aeronutil::testing::allocFreeUdpPort());
        for (int32_t id = 1; id <= 3; ++id) nodes.push_back(std::make_unique<Node>(id));
        for (auto& n : nodes) {   // subscriptions first, then the publications
            n->ring = std::make_unique<IngressRing>(64 * 1024);
            n->ingress = std::make_unique<AeronIngress>(aeron, chan(n->id), kConsensusStream, *n->ring, rune::aeronutil::AeronPump{},
                                                       sealedAll ? sealed(n->id) : pl::PeerCryptoMode{pl::PeerPlain{}});
        }
        for (auto& n : nodes) {
            std::unordered_map<NodeId, UdpMessageSink::PeerEndpoint> eps;
            for (int32_t o = 1; o <= 3; ++o) if (o != n->id) eps[o] = {chan(o), kConsensusStream};
            n->sink = std::make_unique<UdpMessageSink>(aeron, eps, rune::aeronutil::AeronPump{}, PeerResolutionSource{},
                                                       sealedAll ? sealed(n->id) : pl::PeerCryptoMode{pl::PeerPlain{}});
            n->publisher = std::make_unique<SbeConsensusPublisher>(*n->sink);
            n->nsf = std::make_unique<NodeStateFile>((n->dir.dir / "nodestate.nsf").string(), 0);
            n->tl  = std::make_unique<TermLog>((n->dir.dir / "termlog.tl").string());
            std::vector<ClusterMember> ms(3);
            for (int i = 0; i < 3; ++i) { ms[static_cast<std::size_t>(i)].id = i + 1; ms[static_cast<std::size_t>(i)].logPosition = 0;
                                          ms[static_cast<std::size_t>(i)].logLeadershipTermId = 0; ms[static_cast<std::size_t>(i)].leadershipTermId = 0; }
            n->election = std::make_unique<Election>(true, -1, 0, 0, 0, 0, std::move(ms), n->id, *n->publisher, *n->nsf, *n->tl,
                /*electionTimeoutNs=*/300'000'000LL, /*statusIntervalNs=*/50'000'000LL, /*canvassTimeoutNs=*/500'000'000LL,
                /*rngSeed=*/static_cast<uint64_t>(n->id) * 0x9e3779b97f4a7c15ULL);
            n->agent = std::make_unique<ConsensusAgent>(*n->election, *n->ring);
        }
    }
    [[nodiscard]] std::string chan(int32_t id) const { return "aeron:udp?endpoint=127.0.0.1:" + std::to_string(ports[static_cast<std::size_t>(id - 1)]); }
    static pl::PeerCryptoMode sealed(int32_t id) { return pl::PeerFrameCrypto(id, 0); }

    Node& node(int32_t id) { return *nodes[static_cast<std::size_t>(id - 1)]; }
    /// Install the pair (a, b) link on all four transports (sink + ingress of each side).
    void link(int32_t a, int32_t b, uint8_t base, bool wrongOnB = false) {
        const auto ka = linkFor(true, base), kb = linkFor(false, wrongOnB ? static_cast<uint8_t>(base + 40) : base);
        ASSERT_TRUE(node(a).sink->installPeerLink(b, ka));    ASSERT_TRUE(node(a).ingress->installPeerLink(b, ka));
        ASSERT_TRUE(node(b).sink->installPeerLink(a, kb));    ASSERT_TRUE(node(b).ingress->installPeerLink(a, kb));
    }
    static int64_t nowNs() {
        return std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch()).count();
    }
    void step() {
        for (auto& n : nodes) { n->ingress->poll(10); n->agent->doWork(nowNs()); }
    }
    template <class Pred> bool runUntil(std::chrono::milliseconds within, Pred&& done) {
        const auto deadline = std::chrono::steady_clock::now() + within;
        while (std::chrono::steady_clock::now() < deadline) {
            step();
            if (done()) return true;
            std::this_thread::sleep_for(500us);
        }
        return false;
    }
    void runFor(std::chrono::milliseconds d) { (void)runUntil(d, [] { return false; }); }
    [[nodiscard]] bool settledOn(std::initializer_list<int32_t> ids, int32_t& leader) {
        int32_t l = -2;
        for (int32_t id : ids) {
            const auto& e = node(id).agent->election();
            if (e.state() != ElectionState::Closed || e.leaderMemberId() < 0) return false;
            if (l == -2) l = e.leaderMemberId(); else if (l != e.leaderMemberId()) return false;
        }
        leader = l;
        return true;
    }
};

} // namespace

TEST(PeerSealedConsensusRegression, ElectionCompletesOverSealedFrames) {
    Cluster c;
    c.link(1, 2, 1); c.link(1, 3, 11); c.link(2, 3, 21);   // pre-installed, one key pair per link
    int32_t leader = -1;
    ASSERT_TRUE(c.runUntil(15s, [&] { return c.settledOn({1, 2, 3}, leader); })) << "no election over sealed frames";
    EXPECT_GE(leader, 1); EXPECT_LE(leader, 3);
    for (auto& n : c.nodes) {
        EXPECT_EQ(n->ingress->peerFrameDrops(), 0u) << "node " << n->id << ": every frame opened";
        EXPECT_EQ(n->sink->peerFrameDrops(), 0u) << "node " << n->id << ": every frame had a link";
        EXPECT_TRUE(n->sink->peerSealed());
    }
}

TEST(PeerSealedConsensusRegression, SealingStartsOnlyOnceTheLinkExists) {
    Cluster c;
    c.runFor(300ms);                                         // no links yet: nothing can be sent
    int32_t leader = -1;
    EXPECT_FALSE(c.settledOn({1, 2, 3}, leader));
    uint64_t dropped = 0;
    for (auto& n : c.nodes) dropped += n->sink->peerFrameDrops();
    EXPECT_GT(dropped, 0u) << "the frames were dropped at the sender, counted";
    c.link(1, 2, 1); c.link(1, 3, 11); c.link(2, 3, 21);
    ASSERT_TRUE(c.runUntil(15s, [&] { return c.settledOn({1, 2, 3}, leader); })) << "the cluster forms once the links exist";
}

TEST(PeerSealedConsensusRegression, AKeylessNodeNeverAffectsTheTerm) {
    Cluster c;
    c.link(1, 2, 1);                                          // 1 <-> 2 linked; node 3 has no link to anyone
    int32_t leader = -1;
    ASSERT_TRUE(c.runUntil(15s, [&] { return c.settledOn({1, 2}, leader); })) << "two linked nodes elect between them";
    const Term term1 = c.node(1).agent->election().leadershipTermId();
    const Term cand1 = c.node(1).agent->election().candidateTermId();
    const Term cand2 = c.node(2).agent->election().candidateTermId();
    // The keyless node keeps standing for election on its own; none of it reaches the others.
    c.runFor(1500ms);
    EXPECT_EQ(c.node(1).agent->election().leadershipTermId(), term1) << "node 3 cannot move the term";
    EXPECT_EQ(c.node(1).agent->election().candidateTermId(), cand1);
    EXPECT_EQ(c.node(2).agent->election().candidateTermId(), cand2);
    EXPECT_NE(c.node(3).agent->election().state(), ElectionState::Closed);
    EXPECT_GT(c.node(3).sink->peerFrameDrops(), 0u) << "its frames never left";
    EXPECT_EQ(c.node(1).ingress->peerFrameDrops(), 0u);
    EXPECT_EQ(c.node(2).ingress->peerFrameDrops(), 0u);
}

TEST(PeerSealedConsensusRegression, FramesThatArriveButDoNotOpenNeverReachTheElection) {
    Cluster c;
    c.link(1, 2, 1);
    c.link(1, 3, 11, /*wrongOnB=*/true);                       // node 3 holds other keys for node 1
    c.link(2, 3, 21, /*wrongOnB=*/true);
    int32_t leader = -1;
    ASSERT_TRUE(c.runUntil(15s, [&] { return c.settledOn({1, 2}, leader); }));
    const Term term = c.node(1).agent->election().leadershipTermId();
    c.runFor(1500ms);
    EXPECT_EQ(c.node(1).agent->election().leadershipTermId(), term);
    EXPECT_GT(c.node(1).ingress->peerFrameDrops() + c.node(2).ingress->peerFrameDrops(), 0u)
        << "node 3's frames arrived, failed to open, and were dropped before the ring";
    EXPECT_NE(c.node(3).agent->election().state(), ElectionState::Closed);
}

TEST(PeerSealedConsensusRegression, APeerPlainClusterStillElects) {
    Cluster c(/*sealedAll=*/false);
    int32_t leader = -1;
    ASSERT_TRUE(c.runUntil(15s, [&] { return c.settledOn({1, 2, 3}, leader); }));
    for (auto& n : c.nodes) EXPECT_FALSE(n->sink->peerSealed());
}
