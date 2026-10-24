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
#include <rune/consensus/ConsensusAdapter.h>
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
#include <rune/peerlink/PeerHandshake.h>

#include <algorithm>
#include <atomic>
#include <deque>
#include <functional>
#include <set>
#include <string>
#include <random>
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

// Issue #17: UdpMessageSink::sendIdleKeepalives on its own (injected) clock over real sealed UDP.  Node 2 is linked to
// 1 and 3; node 1 only to 2 (it holds no send epoch for 3).  Idle: exactly one keepalive per linked member per 250 ms.
// While consensus frames flow to a member, none goes to it.  The receiver's ingress opens each one (authFrames).
TEST(ConsensusKeepaliveRegression, AnIdleSealedSinkSendsAKeepaliveEveryIntervalRegression) {
    Cluster c;
    c.link(1, 2, 1); c.link(2, 3, 21);
    constexpr int64_t kMs = 1'000'000, kIv = UdpMessageSink::kPeerKeepaliveNs;
    static_assert(kIv == 250 * kMs);
    auto pollAll = [&] { for (auto& n : c.nodes) (void)n->ingress->poll(10); };
    // Warm-up (real time bounds it): the publications connect; every linked ordered pair delivers a keepalive.
    int64_t t = 0;
    const auto deadline = std::chrono::steady_clock::now() + 10s;
    auto delivered = [&] {
        return c.node(2).ingress->authFrames(1) > 0 && c.node(1).ingress->authFrames(2) > 0 &&
               c.node(3).ingress->authFrames(2) > 0 && c.node(2).ingress->authFrames(3) > 0;
    };
    while (!delivered() && std::chrono::steady_clock::now() < deadline) {
        t += kIv;
        for (int32_t id : {1, 2, 3}) (void)c.node(id).sink->sendIdleKeepalives(t);
        pollAll();
        std::this_thread::sleep_for(2ms);
    }
    ASSERT_TRUE(delivered()) << "keepalives never got through";
    EXPECT_EQ(c.node(1).ingress->authFrames(3), 0u);
    EXPECT_EQ(c.node(3).ingress->authFrames(1), 0u) << "no link 1 <-> 3: no keepalive";
    // Idle, 1 s of injected time in 50 ms steps: a keepalive to each linked member at every 250 ms boundary only.
    const int64_t t0 = t;
    int fromNode2 = 0, fromNode1 = 0;
    for (int64_t s = 1; s <= 20; ++s) {
        const int64_t now = t0 + s * 50 * kMs;
        const int n2 = c.node(2).sink->sendIdleKeepalives(now);
        const int n1 = c.node(1).sink->sendIdleKeepalives(now);
        const bool due = (s * 50 * kMs) % kIv == 0;
        EXPECT_EQ(n2, due ? 2 : 0) << "step " << s << ": node 2 -> {1, 3}";
        EXPECT_EQ(n1, due ? 1 : 0) << "step " << s << ": node 1 -> {2} only (no epoch for 3)";
        fromNode2 += n2; fromNode1 += n1;
    }
    EXPECT_EQ(fromNode2, 8);
    EXPECT_EQ(fromNode1, 4);
    // Consensus frames flow from node 2 to node 1 every 50 ms: node 1 gets no keepalive, node 3 still does.
    const int64_t t1 = t0 + 20 * 50 * kMs;
    std::array<uint8_t, StandDownMsg::TOTAL_SIZE> frame{};
    (void)StandDownMsg::encode(frame.data(), 0, /*term=*/1, /*leader=*/2);
    const uint64_t to1Before = c.node(1).ingress->authFrames(2);
    for (int64_t s = 1; s <= 20; ++s) {
        const int64_t now = t1 + s * 50 * kMs;
        c.node(2).sink->send(1, frame.data(), frame.size());
        const int n2 = c.node(2).sink->sendIdleKeepalives(now);
        EXPECT_EQ(n2, (s * 50 * kMs) % kIv == 0 ? 1 : 0) << "step " << s << ": only node 3 is idle";
    }
    // The receivers opened what was sent (sealed, authenticated): node 1 saw the 20 consensus frames.
    const auto settle = std::chrono::steady_clock::now() + 5s;
    while (c.node(1).ingress->authFrames(2) < to1Before + 20 && std::chrono::steady_clock::now() < settle) {
        pollAll();
        std::this_thread::sleep_for(1ms);
    }
    EXPECT_GE(c.node(1).ingress->authFrames(2), to1Before + 20);
    for (auto& n : c.nodes) EXPECT_EQ(n->ingress->peerFrameDrops(), 0u) << "node " << n->id << ": every keepalive opened";
    // A PeerPlain sink never sends one.
    UdpMessageSink plain(c.aeron, {}, rune::aeronutil::AeronPump{}, PeerResolutionSource{}, pl::PeerPlain{});
    EXPECT_EQ(plain.sendIdleKeepalives(10 * kIv), 0);
}

// Issue #17: the sealed idle keepalive (template 65) is consumed by the adapter - its job was done when the ingress
// opened it - and never dispatched nor counted as an ignored frame.  An unknown template still is.
namespace {
struct CountingHandler final : ConsensusMessageHandler {
    int calls = 0;
    void onCanvassPosition(int64_t, Term, int64_t, Term, NodeId, int32_t) override { ++calls; }
    void onRequestVote(int64_t, Term, int64_t, Term, NodeId, int32_t) override { ++calls; }
    void onVote(int64_t, Term, Term, int64_t, NodeId, NodeId, bool) override { ++calls; }
    void onNewLeadershipTerm(int64_t, Term, Term, int64_t, int64_t, Term, int64_t, int64_t, int64_t, int64_t, NodeId, int32_t,
                             int32_t, bool, int64_t) override { ++calls; }
    void onAppendPosition(int64_t, Term, int64_t, NodeId, uint8_t) override { ++calls; }
    void onCommitPosition(int64_t, Term, int64_t, NodeId) override { ++calls; }
    void onStandDown(int64_t, Term, NodeId) override { ++calls; }
};
} // namespace

TEST(ConsensusKeepaliveRegression, TheAdapterIgnoresAKeepaliveWithoutCountingItRegression) {
    CountingHandler handler;
    ConsensusAdapter adapter{handler};
    std::array<uint8_t, PeerKeepaliveMsg::TOTAL_SIZE> buf{};
    ASSERT_EQ(PeerKeepaliveMsg::encode(buf.data(), 0), ConsensusMessageHeader::ENCODED_SIZE) << "header only";
    const auto h = ConsensusMessageHeader::decode(buf.data(), 0);
    EXPECT_EQ(h.templateId, 65);
    EXPECT_EQ(h.blockLength, 0);
    EXPECT_EQ(h.schemaId, kSchemaId);
    EXPECT_EQ(h.version, kSchemaVersion) << "no schema bump: no decoder checks the version";
    for (int i = 0; i < 5; ++i) adapter.onFragment(i, buf.data(), buf.size());
    EXPECT_EQ(handler.calls, 0) << "never dispatched";
    EXPECT_EQ(adapter.ignoredFrameCount(), 0) << "never counted as an ignored / unknown frame";
    buf[2] = 66;   // control: an unknown template is still counted
    adapter.onFragment(9, buf.data(), buf.size());
    EXPECT_EQ(adapter.ignoredFrameCount(), 1);
}

// ---- Review of PR #551 (issue #17), adversarial: the keepalive is the ONLY liveness signal a follower pair has, so a ----
// copy of it must never count twice and a forgery must never count at all.  Over the real frame crypto the consensus
// ingress uses: keepalives reordered inside the replay window all open once; every one replayed is a Replay; a byte
// flipped at every offset, a frame truncated at every length, or one sealed under another link's keys never opens -
// authFrames (the liveness counter BrokerNode::servicePeerLinks_ feeds the handshake) moves only for real, fresh ones.
// Then the adapter: a keepalive with trailing bytes is still consumed silently, a truncated header is counted, and a
// keepalive between real frames never disturbs their dispatch.
TEST(ConsensusKeepaliveRegression, AKeepaliveReplayedReorderedTamperedOrTruncatedNeverCountsAdversarialRegression) {
    pl::PeerFrameCrypto tx(1, 0), rx(2, 0), stranger(1, 0);
    ASSERT_TRUE(tx.installLink(2, linkFor(true, 7), 0));
    ASSERT_TRUE(rx.installLink(1, linkFor(false, 7), 0));
    ASSERT_TRUE(stranger.installLink(2, linkFor(true, 70), 0));   // another cluster's / an old key holder's link
    std::array<uint8_t, PeerKeepaliveMsg::TOTAL_SIZE> ka{};
    ASSERT_EQ(PeerKeepaliveMsg::encode(ka.data(), 0), ka.size());
    using Wire = std::vector<uint8_t>;
    auto sealOne = [&](pl::PeerFrameCrypto& c) {
        Wire w(ka.size() + pl::kPeerFrameOverhead);
        const auto n = c.seal(2, ka.data(), ka.size(), w.data(), w.size());
        EXPECT_EQ(n, w.size());
        return w;
    };
    auto openOne = [&](const Wire& w) {
        uint8_t out[64]; std::size_t outLen = 0; int32_t from = -1; uint32_t epoch = 0;
        const auto r = rx.open(w.data(), w.size(), out, sizeof out, outLen, from, epoch);
        if (r == pl::PeerFrameCrypto::OpenResult::Ok) {
            EXPECT_EQ(outLen, ka.size());
            EXPECT_EQ(peekTemplateId(out, 0), PeerKeepaliveMsg::TEMPLATE_ID);
        }
        return r;
    };
    std::vector<Wire> sent;
    for (int i = 0; i < 40; ++i) sent.push_back(sealOne(tx));
    std::mt19937_64 rng(17);
    auto shuffled = sent;
    std::shuffle(shuffled.begin(), shuffled.end(), rng);                  // reordered inside the 64-wide window
    for (const auto& w : shuffled) EXPECT_EQ(openOne(w), pl::PeerFrameCrypto::OpenResult::Ok);
    ASSERT_EQ(rx.authFrames(1), 40u);
    for (const auto& w : sent) EXPECT_EQ(openOne(w), pl::PeerFrameCrypto::OpenResult::Replay) << "a duplicate is a replay";
    EXPECT_EQ(rx.authFrames(1), 40u) << "duplicated keepalives never count as liveness";

    const Wire fresh = sealOne(tx);
    for (std::size_t off = 0; off < fresh.size(); ++off) {
        Wire t = fresh; t[off] ^= 0x01;
        EXPECT_NE(openOne(t), pl::PeerFrameCrypto::OpenResult::Ok) << "a byte flipped at offset " << off;
    }
    for (std::size_t len = 0; len < fresh.size(); ++len) {
        const Wire t(fresh.begin(), fresh.begin() + static_cast<std::ptrdiff_t>(len));
        EXPECT_NE(openOne(t), pl::PeerFrameCrypto::OpenResult::Ok) << "truncated to " << len;
    }
    for (int i = 0; i < 5; ++i) EXPECT_NE(openOne(sealOne(stranger)), pl::PeerFrameCrypto::OpenResult::Ok) << "another link's keys";
    EXPECT_EQ(rx.authFrames(1), 40u) << "no forgery counted";
    EXPECT_EQ(openOne(fresh), pl::PeerFrameCrypto::OpenResult::Ok) << "control: the untouched fresh keepalive opens";
    EXPECT_EQ(rx.authFrames(1), 41u);

    // The adapter: trailing bytes are tolerated (consumed, not counted); a header cut short is counted; dispatch of the
    // frames around a keepalive is unchanged.
    CountingHandler handler;
    ConsensusAdapter adapter{handler};
    std::array<uint8_t, 128> big{};
    ASSERT_EQ(PeerKeepaliveMsg::encode(big.data(), 0), ConsensusMessageHeader::ENCODED_SIZE);
    for (std::size_t len = ConsensusMessageHeader::ENCODED_SIZE; len <= big.size(); ++len) adapter.onFragment(1, big.data(), len);
    EXPECT_EQ(adapter.ignoredFrameCount(), 0);
    for (std::size_t len = 0; len < ConsensusMessageHeader::ENCODED_SIZE; ++len) adapter.onFragment(1, big.data(), len);
    EXPECT_EQ(adapter.ignoredFrameCount(), static_cast<int64_t>(ConsensusMessageHeader::ENCODED_SIZE));
    std::array<uint8_t, StandDownMsg::TOTAL_SIZE> sd{};
    (void)StandDownMsg::encode(sd.data(), 0, /*term=*/1, /*leader=*/2);
    for (int i = 0; i < 10; ++i) {
        adapter.onFragment(2, ka.data(), ka.size());
        adapter.onFragment(2, sd.data(), sd.size());
    }
    EXPECT_EQ(handler.calls, 10) << "every StandDown between keepalives dispatched, no keepalive dispatched";
}

// ---- Issue #274 (consensus plane): no frame is lost across re-handshakes under traffic.  Two sealed consensus ends ----
// (UdpMessageSink -> AeronIngress, sender id 0) exchange a numbered StandDown stream both ways (one frame each way per
// ms) while two REAL PeerHandshakes re-key the link twice within 2 s; every handshake frame is held kHsDelay (another
// stream: the data wins the race).  Keys go to BOTH the sink and the ingress of a node, as BrokerNode installs them.
// Every numbered frame arrives exactly once; no UnknownEpoch drop.  Mutation: receiveFirst = false -> red.
namespace {

pl::ClusterKeys clusterKeys() {
    const std::string k(48, 'k');
    return {*rune::crypto::deriveClusterKey({reinterpret_cast<const uint8_t*>(k.data()), k.size()}), std::nullopt};
}

/// Two real handshakes, index 0 = node 1 (initiator), 1 = node 2; a frame is delivered `delayNs` after it left.
struct Shake {
    pl::PeerHandshake hs[2]{pl::PeerHandshake(1, 2, 10, clusterKeys()), pl::PeerHandshake(2, 1, 20, clusterKeys())};
    struct InFlight { int to; std::vector<uint8_t> f; int64_t dueNs; };
    std::deque<InFlight> q;
    std::function<void(int, const pl::LinkKeys&)> install;
    int64_t delayNs = 10'000'000, nowNs = 0;
    void absorb(int i, pl::PeerHandshake::Out o) {
        if (o.established) install(i, *o.established);
        if (o.frame) q.push_back({1 - i, std::vector<uint8_t>(o.frame->data(), o.frame->data() + o.frame->size()), nowNs + delayNs});
    }
    void deliver(int to, const std::vector<uint8_t>& f) {
        pl::PeerHandshake& h = hs[to];
        const uint8_t* b = f.data(); const std::size_t n = f.size();
        switch (static_cast<pl::PeerMsg>(pl::peekPeerMsg(b, n))) {
            case pl::PeerMsg::HelloV2:     { pl::PeerHelloV2 m;     if (pl::PeerHelloV2::decode(b, n, m))     absorb(to, h.onHelloV2(m, nowNs)); break; }
            case pl::PeerMsg::ChallengeV2: { pl::PeerChallengeV2 m; if (pl::PeerChallengeV2::decode(b, n, m)) absorb(to, h.onChallengeV2(m, nowNs)); break; }
            case pl::PeerMsg::ProofV2:     { pl::PeerProofV2 m;     if (pl::PeerProofV2::decode(b, n, m))     absorb(to, h.onProofV2(m, nowNs)); break; }
            case pl::PeerMsg::Accept:      { pl::PeerAccept m;      if (pl::PeerAccept::decode(b, n, m))      absorb(to, h.onAccept(m, nowNs)); break; }
            case pl::PeerMsg::Confirm:     { pl::PeerConfirm m;     if (pl::PeerConfirm::decode(b, n, m))     absorb(to, h.onConfirm(m, nowNs)); break; }
            case pl::PeerMsg::Refuse:      { pl::PeerRefuse m;      if (pl::PeerRefuse::decode(b, n, m))      absorb(to, h.onRefuse(m, nowNs)); break; }
            default: ADD_FAILURE() << "unexpected handshake frame tag " << int(b[0]);
        }
    }
    void step(int64_t now) {
        nowNs = now;
        absorb(0, hs[0].tick(now)); absorb(1, hs[1].tick(now));
        while (!q.empty() && q.front().dueNs <= now) { InFlight f = std::move(q.front()); q.pop_front(); deliver(f.to, f.f); }
    }
    [[nodiscard]] bool established() const {
        return std::holds_alternative<pl::Established>(hs[0].state()) && std::holds_alternative<pl::Established>(hs[1].state());
    }
};

struct Elapsed {
    std::chrono::steady_clock::time_point t0 = std::chrono::steady_clock::now();
    [[nodiscard]] int64_t ns() const {
        return 1 + std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - t0).count();
    }
};

} // namespace

TEST(PeerSealedConsensusRegression, NoFrameIsLostAcrossTwoRehandshakesUnderTrafficRegression) {
    MediaDriver driver;
    aeron::Context ctx; ctx.aeronDir(driver.dir());
    auto aeron = aeron::Aeron::connect(ctx);
    const uint16_t ports[2] = {rune::aeronutil::testing::allocFreeUdpPort(), rune::aeronutil::testing::allocFreeUdpPort()};
    auto chan = [&](int i) { return "aeron:udp?endpoint=127.0.0.1:" + std::to_string(ports[i]); };
    struct End {
        std::unique_ptr<IngressRing> ring; std::unique_ptr<AeronIngress> ingress; std::unique_ptr<UdpMessageSink> sink;
        std::multiset<int64_t> got;
    } end[2];
    for (int i = 0; i < 2; ++i) {
        end[i].ring = std::make_unique<IngressRing>(64 * 1024);
        end[i].ingress = std::make_unique<AeronIngress>(aeron, chan(i), kConsensusStream, *end[i].ring, rune::aeronutil::AeronPump{},
                                                        pl::PeerFrameCrypto(i + 1, 0));
    }
    for (int i = 0; i < 2; ++i) {
        std::unordered_map<NodeId, UdpMessageSink::PeerEndpoint> eps{{2 - i, {chan(1 - i), kConsensusStream}}};
        end[i].sink = std::make_unique<UdpMessageSink>(aeron, eps, rune::aeronutil::AeronPump{}, PeerResolutionSource{},
                                                       pl::PeerFrameCrypto(i + 1, 0));
    }
    Shake sh;
    sh.install = [&](int i, const pl::LinkKeys& k) {
        const int32_t peer = 2 - i;
        EXPECT_TRUE(end[i].sink->installPeerLink(peer, k)) << "sink of node " << i + 1;
        EXPECT_TRUE(end[i].ingress->installPeerLink(peer, k)) << "ingress of node " << i + 1;
    };
    const Elapsed clock;
    auto pumpOnce = [&] {
        const int64_t now = clock.ns();
        for (int i = 0; i < 2; ++i) {
            (void)end[i].ingress->poll(10);
            (void)end[i].ring->drain([&](int32_t, aeron::concurrent::AtomicBuffer& b, aeron::util::index_t off, aeron::util::index_t len) {
                const auto* f = reinterpret_cast<const uint8_t*>(b.buffer() + off);
                if (static_cast<std::size_t>(len) >= StandDownMsg::TOTAL_SIZE && peekTemplateId(f, 0) == StandDownMsg::TEMPLATE_ID)
                    end[i].got.insert(StandDownMsg::decode(f, 0).leadershipTermId);
            }, 100);
            int32_t peer = -1; uint32_t ep = 0;
            if (end[i].ingress->takePeerUsedEpoch(peer, ep)) sh.absorb(i, sh.hs[i].onPeerUsedEpoch(ep, now));
        }
        sh.step(now);
        std::this_thread::yield();
    };
    auto send = [&](int from, int64_t seq) {
        std::array<uint8_t, StandDownMsg::TOTAL_SIZE> f{};
        (void)StandDownMsg::encode(f.data(), 0, seq, from + 1);
        end[from].sink->send(2 - from, f.data(), f.size());
    };
    for (int i = 0; i < 10'000'000 && !sh.established() && clock.ns() < 5'000'000'000; ++i) pumpOnce();
    ASSERT_TRUE(sh.established());
    for (int i = 0; i < 10'000'000 && (end[0].got.count(0) == 0 || end[1].got.count(0) == 0) && clock.ns() < 15'000'000'000; ++i) {
        if (i % 1000 == 0) { send(0, 0); send(1, 0); }
        pumpOnce();
    }
    ASSERT_TRUE(end[0].got.count(0) > 0 && end[1].got.count(0) > 0) << "the publications never connected";

    constexpr int64_t kN = 400, kPaceNs = 1'000'000;
    int rekeys = 0;
    int64_t nextSend = clock.ns();
    for (int64_t seq = 1; seq <= kN;) {
        pumpOnce();
        if (clock.ns() < nextSend) continue;
        nextSend += kPaceNs;
        if (seq == 100 || seq == 250) {   // the second re-handshake ~150 ms after the first (< 2 s)
            ASSERT_TRUE(sh.established()) << "the previous re-handshake had not completed by " << seq;
            auto o = sh.hs[0].rekeyForTest(clock.ns());
            ASSERT_TRUE(o.frame) << "rekey at " << seq;
            sh.absorb(0, std::move(o));
            ++rekeys;
        }
        send(0, seq); send(1, seq);
        ++seq;
    }
    auto numbered = [](const End& e) { return static_cast<int64_t>(e.got.size() - e.got.count(0)); };
    const int64_t drainBy = clock.ns() + 10'000'000'000;
    for (int i = 0; i < 10'000'000 && (numbered(end[0]) < kN || numbered(end[1]) < kN || !sh.q.empty()) && clock.ns() < drainBy; ++i)
        pumpOnce();
    std::multiset<int64_t> want;
    for (int64_t s = 1; s <= kN; ++s) want.insert(s);
    for (auto& e : end) e.got.erase(0);
    auto diff = [&](const std::multiset<int64_t>& got) {
        std::string d;
        for (int64_t s = 1; s <= kN; ++s)
            if (const auto c = got.count(s); c != 1) d += " " + std::to_string(s) + "x" + std::to_string(c);
        return d;
    };
    EXPECT_EQ(end[1].got, want) << "node 1 -> node 2, seq x count:" << diff(end[1].got);
    EXPECT_EQ(end[0].got, want) << "node 2 -> node 1, seq x count:" << diff(end[0].got);
    EXPECT_EQ(rekeys, 2);
    for (int i = 0; i < 2; ++i) {
        EXPECT_EQ(end[i].ingress->unknownEpochDrops(), 0u) << "node " << i + 1;
        EXPECT_EQ(end[i].ingress->peerFrameDrops(), 0u) << "node " << i + 1;
        EXPECT_EQ(end[i].sink->peerFrameDrops(), 0u) << "node " << i + 1;
    }
    EXPECT_EQ(end[0].sink->peerLinkEpoch(2), 3u);
    EXPECT_EQ(end[1].sink->peerLinkEpoch(1), 3u) << "the responder promoted the last epoch";
}
