// PeerSealedTransportTest.cpp — authentication plan Task 16 (D34): every inter-broker replication frame sealed, over
// REAL Aeron.  Part 1: two WorkerReplicationTransportImpl (the per-worker / admin plane transport) over aeron:ipc with
// a PeerFrameCrypto installed.  Part 2: two AeronPartitionTransports (the legacy control-thread transport) driving
// real PartitionReplicas over aeron:udp loopback.
#include <rune/aeronutil/testing/FreePort.h>
#include <gtest/gtest.h>

extern "C" {
#include <aeronmd/aeronmd.h>
}

#include <Aeron.h>
#include <Context.h>
#include <Subscription.h>

#include <rune/peerlink/PeerFrameCrypto.h>
#include <rune/peerlink/PeerHandshake.h>
#include <rune/replication/AeronPartitionTransport.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/replication/ReplicationMessages.h>
#include <rune/replication/WorkerReplicationTransport.h>
#include <rune/storage/Log.h>

#include <atomic>
#include <deque>
#include <functional>
#include <set>
#include <chrono>
#include <filesystem>
#include <memory>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

namespace fs = std::filesystem;
namespace pl = rune::peerlink;
namespace rr = rune::replication;
using namespace std::chrono_literals;

namespace {

class MediaDriver {
public:
    MediaDriver() {
        static std::atomic<int> seq{0};
        dir_ = (fs::temp_directory_path() / ("rune_pst_" + std::to_string(::getpid()) + "_" + std::to_string(seq++))).string();
        if (aeron_driver_context_init(&ctx_) < 0) throw std::runtime_error(aeron_errmsg());
        aeron_driver_context_set_dir(ctx_, dir_.c_str());
        aeron_driver_context_set_threading_mode(ctx_, AERON_THREADING_MODE_SHARED);
        aeron_driver_context_set_dir_delete_on_start(ctx_, true);
        aeron_driver_context_set_dir_delete_on_shutdown(ctx_, true);
        aeron_driver_context_set_shared_idle_strategy(ctx_, "sleep-ns");
        aeron_driver_context_set_term_buffer_sparse_file(ctx_, true);
        aeron_driver_context_set_term_buffer_length(ctx_, 64 * 1024);
        aeron_driver_context_set_ipc_term_buffer_length(ctx_, 64 * 1024);
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

/// The directional link keys a pair shares: node A holds linkFor(true), node B linkFor(false).
pl::LinkKeys linkFor(bool aToB, uint32_t epoch = 1, uint8_t base = 1) {
    pl::LinkKeys k; k.epoch = epoch;
    k.sendMaster.fill(aToB ? base : static_cast<uint8_t>(base + 1));
    k.recvMaster.fill(aToB ? static_cast<uint8_t>(base + 1) : base);
    return k;
}

pl::PeerCryptoMode sealed(int32_t self, uint8_t sender) { return pl::PeerFrameCrypto(self, sender); }

/// A raw subscriber: every fragment Aeron delivers on a stream, as written on the wire.
struct Sniffer {
    std::shared_ptr<aeron::Subscription> sub;
    std::vector<std::vector<uint8_t>>    frags;
    Sniffer(aeron::Aeron& a, const std::string& ch, int32_t stream) {
        const int64_t id = a.addSubscription(ch, stream);
        for (int i = 0; i < 5000 && !sub; ++i) { sub = a.findSubscription(id); if (!sub) std::this_thread::sleep_for(1ms); }
    }
    void poll() {
        sub->poll([this](aeron::concurrent::AtomicBuffer& b, aeron::util::index_t o, aeron::util::index_t l, aeron::Header&) {
            frags.emplace_back(b.buffer() + o, b.buffer() + o + l);
        }, 16);
    }
    [[nodiscard]] bool anyContains(const std::string& needle) const {
        for (const auto& f : frags) {
            const std::string s(f.begin(), f.end());
            if (s.find(needle) != std::string::npos) return true;
        }
        return false;
    }
};

constexpr int32_t kStreamA = 600, kStreamB = 601;   // A listens on 600, B on 601

struct Frame { int32_t topic = 0, partition = 0; uint16_t tid = 0; std::vector<uint8_t> bytes; };

/// Two per-worker transports over aeron:ipc: A = node 1 (inbound 600, peer pub -> 601), B = node 2 (inbound 601, -> 600).
struct WrtPair {
    MediaDriver                   driver;
    std::shared_ptr<aeron::Aeron> aeron;
    std::unique_ptr<rr::WorkerReplicationTransport> a, b;
    std::unique_ptr<Sniffer> toB, toA;

    WrtPair(pl::PeerCryptoMode ca, pl::PeerCryptoMode cb) {
        aeron::Context ctx; ctx.aeronDir(driver.dir());
        aeron = aeron::Aeron::connect(ctx);
        toB = std::make_unique<Sniffer>(*aeron, "aeron:ipc", kStreamB);
        toA = std::make_unique<Sniffer>(*aeron, "aeron:ipc", kStreamA);
        a = std::make_unique<rr::WorkerReplicationTransport>(aeron, 0, "aeron:ipc", kStreamA,
            std::vector<std::pair<rr::NodeId, std::string>>{{2, "aeron:ipc"}}, kStreamB, rune::aeronutil::AeronPump{}, std::move(ca));
        b = std::make_unique<rr::WorkerReplicationTransport>(aeron, 0, "aeron:ipc", kStreamB,
            std::vector<std::pair<rr::NodeId, std::string>>{{1, "aeron:ipc"}}, kStreamA, rune::aeronutil::AeronPump{}, std::move(cb));
    }
    void link(uint8_t baseA = 1, uint8_t baseB = 1) {
        ASSERT_TRUE(a->installPeerLink(2, linkFor(true, 1, baseA)));
        ASSERT_TRUE(b->installPeerLink(1, linkFor(false, 1, baseB)));
    }
    /// Polls `t` until `want` frames arrived or 1 s passed; the sniffers keep reading.
    template <class T> std::vector<Frame> pollFrames(T& t, std::size_t want, std::chrono::milliseconds within = 1000ms) {
        std::vector<Frame> got;
        const auto deadline = std::chrono::steady_clock::now() + within;
        while (std::chrono::steady_clock::now() < deadline && got.size() < want) {
            (void)t.drivePeerResolve();
            (void)t.poll(8, [&](int32_t topic, int32_t part, uint16_t tid, const uint8_t* f, std::size_t n) {
                got.push_back({topic, part, tid, std::vector<uint8_t>(f, f + n)});
            });
            toA->poll(); toB->poll();
            std::this_thread::sleep_for(1ms);
        }
        return got;
    }
    void settle(std::chrono::milliseconds d = 100ms) {
        const auto deadline = std::chrono::steady_clock::now() + d;
        while (std::chrono::steady_clock::now() < deadline) { toA->poll(); toB->poll(); std::this_thread::sleep_for(1ms); }
    }
    /// Both pubs connected (the subscriber side is up) - sends before this are back-pressured, not interesting.
    void awaitConnected() {
        const auto deadline = std::chrono::steady_clock::now() + 5s;
        while (std::chrono::steady_clock::now() < deadline) {
            (void)a->drivePeerResolve(); (void)b->drivePeerResolve();
            if (a->allPeersResolved() && b->allPeersResolved()) { std::this_thread::sleep_for(50ms); return; }
            std::this_thread::sleep_for(1ms);
        }
    }
};

rr::FetchRequestMsg fetchRequest() {
    rr::FetchRequestMsg r{};
    r.leaderEpoch = 3; r.partition = 7; r.followerId = 1; r.fetchOffset = 42; r.maxBytes = 4096; r.topicId = 5;
    return r;
}

std::vector<uint8_t> records(std::size_t n, const std::string& marker) {
    std::vector<uint8_t> v(n);
    for (std::size_t i = 0; i < n; ++i) v[i] = static_cast<uint8_t>(marker[i % marker.size()]);
    return v;
}

} // namespace

// ---------------------------------------------------------------------------------------------------------------
// Part 1 - WorkerReplicationTransport
// ---------------------------------------------------------------------------------------------------------------
TEST(PeerSealedTransportRegression, FetchRoundTripsSealed) {
    WrtPair p(sealed(1, 3), sealed(2, 3));
    ASSERT_TRUE(p.a->resolved() && p.b->resolved());
    p.link();
    p.awaitConnected();
    EXPECT_EQ(p.a->peerLinkEpoch(2), 1u);

    // A -> B: a FetchRequest (one claimed MTU payload, sealed straight into the term buffer).
    ASSERT_TRUE(p.a->sendFetchRequest(fetchRequest()));
    auto reqs = p.pollFrames(*p.b, 1);
    ASSERT_EQ(reqs.size(), 1u);
    EXPECT_EQ(reqs[0].tid, rr::FetchRequestMsg::TEMPLATE_ID);
    EXPECT_EQ(reqs[0].topic, 5); EXPECT_EQ(reqs[0].partition, 7);
    auto dreq = rr::FetchRequestMsg::decode(reqs[0].bytes.data(), 0, reqs[0].bytes.size());
    EXPECT_EQ(dreq.fetchOffset, 42); EXPECT_EQ(dreq.maxBytes, 4096);

    // B -> A: a small and a LARGE (fragmented, sealed in the scratch) FetchResponse; records must survive intact.
    for (std::size_t n : {std::size_t{1000}, std::size_t{6000}}) {
        const auto recs = records(n, "SECRET-RECORDS");
        rr::FetchResponseMsg resp{};
        resp.leaderEpoch = 3; resp.partition = 7; resp.highWatermark = 99; resp.logStartOffset = 0; resp.errorCode = 0;
        resp.topicId = 5; resp.records = std::span<const uint8_t>(recs);
        ASSERT_TRUE(p.b->sendFetchResponse(1, resp)) << "records " << n;
        auto got = p.pollFrames(*p.a, 1);
        ASSERT_EQ(got.size(), 1u) << "records " << n;
        EXPECT_EQ(got[0].tid, rr::FetchResponseMsg::TEMPLATE_ID);
        auto d = rr::FetchResponseMsg::decode(got[0].bytes.data(), 0, got[0].bytes.size());
        ASSERT_EQ(d.records.size(), n);
        EXPECT_TRUE(std::equal(d.records.begin(), d.records.end(), recs.begin())) << "records " << n;
        EXPECT_EQ(d.highWatermark, 99);
    }
    p.settle();
    // The wire carried only sealed frames: the peer magic first, and never the plaintext records.
    ASSERT_FALSE(p.toA->frags.empty());
    ASSERT_FALSE(p.toB->frags.empty());
    EXPECT_EQ(p.toB->frags.front()[0], pl::kPeerFrameMagic);
    EXPECT_EQ(p.toA->frags.front()[0], pl::kPeerFrameMagic);
    EXPECT_FALSE(p.toA->anyContains("SECRET-RECORDS")) << "the records never crossed the wire in the clear";
    EXPECT_EQ(p.a->peerFrameDrops(), 0u);
    EXPECT_EQ(p.b->peerFrameDrops(), 0u);
}

TEST(PeerSealedTransportRegression, NoLinkMeansNothingIsSentAndItIsCounted) {
    WrtPair p(sealed(1, 3), sealed(2, 3));
    p.awaitConnected();
    EXPECT_FALSE(p.a->sendFetchRequest(fetchRequest())) << "no established link -> the frame is not sent";
    const auto recs = records(500, "x");
    rr::FetchResponseMsg resp{};
    resp.partition = 7; resp.topicId = 5; resp.records = std::span<const uint8_t>(recs);
    EXPECT_FALSE(p.a->sendFetchResponse(2, resp));
    EXPECT_EQ(p.a->peerFrameDrops(), 2u);
    EXPECT_EQ(p.a->peerCrypto()->noLinkDrops(), 2u);
    p.settle(200ms);
    EXPECT_TRUE(p.toB->frags.empty()) << "nothing reached the wire";
    EXPECT_TRUE(p.pollFrames(*p.b, 1, 100ms).empty());

    // The link comes up: the same sends now go out sealed (the fetch retry path heals the plane).
    p.link();
    ASSERT_TRUE(p.a->sendFetchRequest(fetchRequest()));
    EXPECT_EQ(p.pollFrames(*p.b, 1).size(), 1u);
}

TEST(PeerSealedTransportRegression, AWrongKeyPeerIsDroppedNeverDispatched) {
    WrtPair p(sealed(1, 3), sealed(2, 3));
    p.link(/*baseA=*/1, /*baseB=*/50);   // B holds other keys for node 1
    p.awaitConnected();
    ASSERT_TRUE(p.a->sendFetchRequest(fetchRequest())) << "A believes the link is up";
    EXPECT_TRUE(p.pollFrames(*p.b, 1, 300ms).empty()) << "B never dispatches a frame that did not open";
    EXPECT_EQ(p.b->peerFrameDrops(), 1u);
    EXPECT_EQ(p.b->peerCrypto()->dropsOf(pl::PeerFrameCrypto::OpenResult::BadTag), 1u);

    // A node with no link at all for the sender: UnknownPeer; counted as an unknown-epoch-or-peer drop, not dispatched.
    WrtPair q(sealed(1, 3), sealed(2, 3));
    ASSERT_TRUE(q.a->installPeerLink(2, linkFor(true)));        // B never learned A's keys
    q.awaitConnected();
    ASSERT_TRUE(q.a->sendFetchRequest(fetchRequest()));
    EXPECT_TRUE(q.pollFrames(*q.b, 1, 300ms).empty());
    EXPECT_EQ(q.b->peerCrypto()->dropsOf(pl::PeerFrameCrypto::OpenResult::UnknownPeer), 1u);
}

TEST(PeerSealedTransportRegression, PlaintextFramesAreNeverDispatchedBySealedPlanes) {
    {   // a PeerPlain sender -> a sealed receiver: NotPeerFrame
        WrtPair p(pl::PeerPlain{}, sealed(2, 3));
        ASSERT_TRUE(p.b->installPeerLink(1, linkFor(false)));
        p.awaitConnected();
        ASSERT_TRUE(p.a->sendFetchRequest(fetchRequest()));
        EXPECT_TRUE(p.pollFrames(*p.b, 1, 300ms).empty());
        EXPECT_EQ(p.b->peerCrypto()->dropsOf(pl::PeerFrameCrypto::OpenResult::NotPeerFrame), 1u);
    }
    // (A sealed sender talking to a PeerPlain receiver is a mixed-mode cluster: the auth mode is cluster-wide
    //  configuration, and a PeerPlain transport stays byte-identical to today by design - it is not defended.)
}

TEST(PeerSealedTransportRegression, ABrokerWithNoCryptoSendsExactlyTodaysBytes) {
    WrtPair p(pl::PeerPlain{}, pl::PeerPlain{});
    p.awaitConnected();
    ASSERT_TRUE(p.a->sendFetchRequest(fetchRequest()));
    ASSERT_EQ(p.pollFrames(*p.b, 1).size(), 1u);
    p.settle();
    ASSERT_FALSE(p.toB->frags.empty());
    EXPECT_EQ(p.toB->frags.front().size(), rr::FetchRequestMsg::TOTAL_SIZE) << "no header, no tag";
    EXPECT_NE(p.toB->frags.front()[0], pl::kPeerFrameMagic);
    EXPECT_FALSE(p.a->peerSealed());
    EXPECT_EQ(p.a->peerFrameDrops(), 0u);
}

// ---------------------------------------------------------------------------------------------------------------
// Part 2 - AeronPartitionTransport (the legacy control-thread transport), real PartitionReplicas over aeron:udp
// ---------------------------------------------------------------------------------------------------------------
namespace {

rr::PartitionConfig replConfig(rr::NodeId selfId) {
    rr::PartitionConfig cfg;
    cfg.partitionId = 0; cfg.selfId = selfId; cfg.replicaLagMaxMs = 5000; cfg.replicaLagMaxOffset = 100000;
    cfg.minIsr = 1; cfg.fetchIntervalNs = 5'000'000LL; cfg.isrCheckIntervalNs = 10'000'000LL; cfg.maxFetchBytes = 1 << 20;
    return cfg;
}

struct AptPair {
    MediaDriver                   driver;
    std::shared_ptr<aeron::Aeron> aeron;
    TempDir                       ld{"pst_l"}, fd{"pst_f"};
    rune::storage::Log            ldrLog{ld.dir.string()}, flwLog{fd.dir.string()};
    std::unique_ptr<rr::AeronPartitionTransport> ldr, flw;
    std::unique_ptr<rr::PartitionReplica> leader, follower;
    int64_t simNs = 0;

    explicit AptPair(bool sealedMode) {
        aeron::Context ctx; ctx.aeronDir(driver.dir());
        aeron = aeron::Aeron::connect(ctx);
        const std::string cl = "aeron:udp?endpoint=127.0.0.1:" + std::to_string(rune::aeronutil::testing::allocFreeUdpPort());
        const std::string cf = "aeron:udp?endpoint=127.0.0.1:" + std::to_string(rune::aeronutil::testing::allocFreeUdpPort());
        constexpr int32_t kStream = 500;
        ldr = std::make_unique<rr::AeronPartitionTransport>(aeron, cl, kStream, 1, 4 * 1024 * 1024, rune::aeronutil::AeronPump{},
                                                            sealedMode ? sealed(1, 1) : pl::PeerCryptoMode{pl::PeerPlain{}});
        ldr->setPublicationMaxMessageBytes(rr::aeronMaxMessageLength(64 * 1024));
        ldr->addPeer(2, cf, kStream);
        flw = std::make_unique<rr::AeronPartitionTransport>(aeron, cf, kStream, 2, 4 * 1024 * 1024, rune::aeronutil::AeronPump{},
                                                            sealedMode ? sealed(2, 1) : pl::PeerCryptoMode{pl::PeerPlain{}});
        flw->setPublicationMaxMessageBytes(rr::aeronMaxMessageLength(64 * 1024));
        flw->addPeer(1, cl, kStream);
        const std::vector<rr::NodeId> nodes{1, 2};
        leader = std::make_unique<rr::PartitionReplica>(replConfig(1), rr::Role::Leader, 1, nodes, nodes, &ldrLog, ldr.get());
        follower = std::make_unique<rr::PartitionReplica>(replConfig(2), rr::Role::Follower, 1, nodes, nodes, &flwLog, flw.get());
        ldr->registerReplica(0, 0, leader.get());
        flw->registerReplica(0, 0, follower.get());
    }
    void link(uint8_t baseL = 1, uint8_t baseF = 1) {
        ASSERT_TRUE(ldr->installPeerLink(2, linkFor(true, 1, baseL)));
        ASSERT_TRUE(flw->installPeerLink(1, linkFor(false, 1, baseF)));
    }
    rr::Offset append(std::size_t bytes, int n) {
        rr::Offset leo = -1;
        for (int i = 0; i < n; ++i) {
            std::vector<uint8_t> payload(bytes, static_cast<uint8_t>(0xA0 + i));
            if (leader->append(payload) < 0) return -1;
            leo = leader->logEndOffset();
        }
        return leo;
    }
    bool runUntilConverged(rr::Offset target, std::chrono::milliseconds within) {
        const auto deadline = std::chrono::steady_clock::now() + within;
        while (std::chrono::steady_clock::now() < deadline) {
            simNs += 5'000'000LL;
            ldr->poll(simNs); flw->poll(simNs);
            leader->doWork(simNs); follower->doWork(simNs);
            if (follower->logEndOffset() >= target && leader->highWatermark() >= target) return true;
            std::this_thread::sleep_for(1ms);
        }
        return false;
    }
    void runFor(std::chrono::milliseconds d) { (void)runUntilConverged(INT64_MAX, d); }
};

} // namespace

TEST(PeerSealedAeronPartitionTransportRegression, FetchRoundTripsSealed) {
    AptPair p(true);
    p.link();
    EXPECT_EQ(p.ldr->peerLinkEpoch(2), 1u);
    const rr::Offset target = p.append(32, 5);
    ASSERT_GT(target, 0);
    ASSERT_TRUE(p.runUntilConverged(target, 10s)) << "follower LEO " << p.follower->logEndOffset() << " target " << target;
    EXPECT_EQ(p.ldr->peerFrameDrops(), 0u);
    EXPECT_EQ(p.flw->peerFrameDrops(), 0u);
    EXPECT_TRUE(p.ldr->peerSealed());
}

TEST(PeerSealedAeronPartitionTransportRegression, ALargeBacklogFragmentsSealedFramesAndStillConverges) {
    AptPair p(true);
    p.link();
    // ~173 B batches x 500: far over one MTU per fetch response -> every response is sealed WHOLE, then fragmented.
    const rr::Offset target = p.append(100, 500);
    ASSERT_GT(target, 0);
    ASSERT_TRUE(p.runUntilConverged(target, 20s)) << "follower LEO " << p.follower->logEndOffset() << " target " << target;
    EXPECT_EQ(p.ldr->peerFrameDrops(), 0u);
    EXPECT_EQ(p.flw->peerFrameDrops(), 0u);
}

TEST(PeerSealedAeronPartitionTransportRegression, NoLinkMeansNothingIsSentAndItIsCounted) {
    AptPair p(true);
    const rr::Offset target = p.append(32, 3);
    p.runFor(400ms);
    EXPECT_EQ(p.follower->logEndOffset(), 0) << "no link -> not a single fetch left the follower";
    EXPECT_GT(p.flw->peerFrameDrops(), 0u);
    EXPECT_EQ(p.ldr->peerFrameDrops(), 0u) << "the leader never received a frame";
    // The keys arrive: the plane heals (the down -> up edge re-drives the fetch).
    p.link();
    ASSERT_TRUE(p.runUntilConverged(target, 10s));
}

TEST(PeerSealedAeronPartitionTransportRegression, AWrongKeyPeerIsDroppedNeverDispatched) {
    AptPair p(true);
    p.link(/*baseL=*/1, /*baseF=*/50);
    (void)p.append(32, 3);
    p.runFor(500ms);
    EXPECT_EQ(p.follower->logEndOffset(), 0) << "the leader never dispatched a fetch that did not open";
    EXPECT_GT(p.ldr->peerFrameDrops(), 0u);
}

TEST(PeerSealedAeronPartitionTransportRegression, UnsealedModeIsUnchanged) {
    AptPair p(false);
    const rr::Offset target = p.append(32, 5);
    ASSERT_TRUE(p.runUntilConverged(target, 10s));
    EXPECT_FALSE(p.ldr->peerSealed());
    EXPECT_EQ(p.ldr->peerFrameDrops(), 0u);
}

// X080: a back-pressured (ABORTed) fragment is redelivered after ANOTHER image's fragments were polled; the replay
// window is committed only after a non-ABORT dispatch, so nothing is lost with two sending peers.
namespace {
struct AbortSink {
    bool refuse = true;
    std::multiset<int32_t> got;
};
bool abortSinkHandler(void* ctx, const rr::AlterIsrResponseMsg& r) noexcept {
    auto* s = static_cast<AbortSink*>(ctx);
    if (s->refuse) return false;
    s->got.insert(r.sendSeq);
    return true;
}
}  // namespace

TEST(PeerFrameRegression, AbortRedeliveryWithTwoImagesLosesNothingRegression) {
    MediaDriver driver;
    aeron::Context ctx; ctx.aeronDir(driver.dir());
    auto aeron = aeron::Aeron::connect(ctx);
    auto url = [] { return "aeron:udp?endpoint=127.0.0.1:" + std::to_string(rune::aeronutil::testing::allocFreeUdpPort()); };
    const std::string c1 = url(), c2 = url(), c3 = url();
    constexpr int32_t kStream = 501;
    rr::AeronPartitionTransport rx(aeron, c1, kStream, 1, 4 * 1024 * 1024, rune::aeronutil::AeronPump{}, sealed(1, 1));
    rr::AeronPartitionTransport s2(aeron, c2, kStream, 2, 4 * 1024 * 1024, rune::aeronutil::AeronPump{}, sealed(2, 1));
    rr::AeronPartitionTransport s3(aeron, c3, kStream, 3, 4 * 1024 * 1024, rune::aeronutil::AeronPump{}, sealed(3, 1));
    s2.addPeer(1, c1, kStream); s3.addPeer(1, c1, kStream);
    rx.addPeer(2, c2, kStream); rx.addPeer(3, c3, kStream);
    ASSERT_TRUE(rx.installPeerLink(2, linkFor(false, 1, 1))); ASSERT_TRUE(rx.installPeerLink(3, linkFor(false, 1, 2)));
    ASSERT_TRUE(s2.installPeerLink(1, linkFor(true, 1, 1))); ASSERT_TRUE(s3.installPeerLink(1, linkFor(true, 1, 2)));
    AbortSink sink;
    rx.setForeignAlterIsrResponseHandler(&abortSinkHandler, &sink);
    constexpr int kEach = 5;
    int64_t now = 0;
    auto pump = [&](int ms) {
        for (int i = 0; i < ms; ++i) { now += 1'000'000; rx.poll(now); s2.poll(now); s3.poll(now); std::this_thread::sleep_for(1ms); }
    };
    pump(300);   // let the images connect
    for (int i = 0; i < kEach; ++i) {
        rr::AlterIsrResponseMsg m; m.topicId = -1; m.partition = 0;
        m.sendSeq = 100 + i; for (int t = 0; t < 50 && !s2.sendAlterIsrResponse(1, m); ++t) pump(5);
        m.sendSeq = 200 + i; for (int t = 0; t < 50 && !s3.sendAlterIsrResponse(1, m); ++t) pump(5);
    }
    pump(300);   // every poll ABORTs: both images' fragments stay pending, redelivered each round
    EXPECT_TRUE(sink.got.empty());
    sink.refuse = false;
    pump(500);
    std::multiset<int32_t> want;
    for (int i = 0; i < kEach; ++i) { want.insert(100 + i); want.insert(200 + i); }
    EXPECT_EQ(sink.got, want) << "every refused frame is delivered exactly once";
    EXPECT_EQ(rx.peerFrameDrops(), 0u);
}

// ---------------------------------------------------------------------------------------------------------------
// Issue #274: no frame is lost across a re-handshake under traffic.  Two sealed AeronPartitionTransports (the legacy
// control-thread plane) exchange a numbered stream in both directions (one frame each way per ms) while two REAL
// PeerHandshakes (node 1 initiates) re-key the link twice within 2 s.  Every handshake frame is held kHsDelay before it
// is delivered (it rides another stream: nothing orders it against the data, and here the data always wins the race);
// every install goes to the transport exactly as the PeerLinkAgent hands it over (receive-only first at the responder,
// promoted by the Confirm or an authenticated frame).  Every numbered frame must arrive exactly once and none may be
// dropped as UnknownEpoch.  Mutation: the responder installing send-and-receive at once (receiveFirst = false) -> red.
// ---------------------------------------------------------------------------------------------------------------
namespace {

pl::ClusterKeys clusterKeys() {
    const std::string k(48, 'k');
    return {*rune::crypto::deriveClusterKey({reinterpret_cast<const uint8_t*>(k.data()), k.size()}), std::nullopt};
}

/// Two real handshakes, index 0 = node 1 (initiator), 1 = node 2.  Their clock is the test's elapsed time; a frame is
/// delivered `delayNs` after it left.
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

/// Elapsed ns since construction (> 0): the handshakes' clock in the real-Aeron no-loss tests.
struct Elapsed {
    std::chrono::steady_clock::time_point t0 = std::chrono::steady_clock::now();
    [[nodiscard]] int64_t ns() const {
        return 1 + std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - t0).count();
    }
};

struct NumberedSink { std::multiset<int32_t> got; };
bool numberedSinkHandler(void* ctx, const rr::AlterIsrResponseMsg& r) noexcept {
    static_cast<NumberedSink*>(ctx)->got.insert(r.sendSeq);
    return true;
}

} // namespace

TEST(PeerSealedTransportRegression, NoFrameIsLostAcrossARehandshakeUnderTrafficRegression) {
    MediaDriver driver;
    aeron::Context ctx; ctx.aeronDir(driver.dir());
    auto aeron = aeron::Aeron::connect(ctx);
    auto url = [] { return "aeron:udp?endpoint=127.0.0.1:" + std::to_string(rune::aeronutil::testing::allocFreeUdpPort()); };
    const std::string c1 = url(), c2 = url();
    constexpr int32_t kStream = 502;
    rr::AeronPartitionTransport n1(aeron, c1, kStream, 1, 4 * 1024 * 1024, rune::aeronutil::AeronPump{}, sealed(1, 1));
    rr::AeronPartitionTransport n2(aeron, c2, kStream, 2, 4 * 1024 * 1024, rune::aeronutil::AeronPump{}, sealed(2, 1));
    n1.addPeer(2, c2, kStream); n2.addPeer(1, c1, kStream);
    NumberedSink at1, at2;
    n1.setForeignAlterIsrResponseHandler(&numberedSinkHandler, &at1);
    n2.setForeignAlterIsrResponseHandler(&numberedSinkHandler, &at2);
    Shake sh;
    sh.install = [&](int i, const pl::LinkKeys& k) {
        EXPECT_TRUE(i == 0 ? n1.installPeerLink(2, k) : n2.installPeerLink(1, k)) << "install at node " << i + 1;
    };
    const Elapsed clock;
    auto pumpOnce = [&] {
        const int64_t now = clock.ns();
        (void)n1.poll(now); (void)n2.poll(now);
        int32_t peer = -1; uint32_t ep = 0;   // BrokerNode::servicePeerLinks_ forwards these every duty cycle
        if (n1.takePeerUsedEpoch(peer, ep)) sh.absorb(0, sh.hs[0].onPeerUsedEpoch(ep, now));
        if (n2.takePeerUsedEpoch(peer, ep)) sh.absorb(1, sh.hs[1].onPeerUsedEpoch(ep, now));
        sh.step(now);
        std::this_thread::yield();
    };
    // The first link, then the images connect: probe 0 crosses each way.
    for (int i = 0; i < 10'000'000 && !sh.established() && clock.ns() < 5'000'000'000; ++i) pumpOnce();
    ASSERT_TRUE(sh.established());
    auto send = [&](rr::AeronPartitionTransport& t, int32_t to, int32_t seq) {
        rr::AlterIsrResponseMsg m; m.topicId = -1; m.partition = 0; m.sendSeq = seq;
        for (int t2 = 0; t2 < 100'000; ++t2) { if (t.sendAlterIsrResponse(to, m)) return true; pumpOnce(); }
        return false;
    };
    for (int i = 0; i < 10'000'000 && (at1.got.count(0) == 0 || at2.got.count(0) == 0) && clock.ns() < 15'000'000'000; ++i) {
        if (i % 1000 == 0) { (void)send(n1, 2, 0); (void)send(n2, 1, 0); }
        pumpOnce();
    }
    ASSERT_TRUE(at1.got.count(0) > 0 && at2.got.count(0) > 0) << "the images never connected";

    constexpr int32_t kN = 400;
    constexpr int64_t kPaceNs = 1'000'000;
    int rekeys = 0;
    int64_t nextSend = clock.ns();
    for (int32_t seq = 1; seq <= kN;) {
        pumpOnce();
        if (clock.ns() < nextSend) continue;
        nextSend += kPaceNs;
        if (seq == 100 || seq == 250) {   // two same-incarnation re-handshakes ~150 ms apart (< 2 s)
            ASSERT_TRUE(sh.established()) << "the previous re-handshake had not completed by " << seq;
            auto o = sh.hs[0].rekeyForTest(clock.ns());
            ASSERT_TRUE(o.frame) << "rekey at " << seq;
            sh.absorb(0, std::move(o));
            ++rekeys;
        }
        ASSERT_TRUE(send(n1, 2, seq)) << seq;
        ASSERT_TRUE(send(n2, 1, seq)) << seq;
        ++seq;
    }
    auto numbered = [](const NumberedSink& at) { return at.got.size() - at.got.count(0); };   // the warm-up probes aside
    const int64_t drainBy = clock.ns() + 10'000'000'000;
    for (int i = 0; i < 10'000'000 && (numbered(at1) < kN || numbered(at2) < kN || !sh.q.empty()) && clock.ns() < drainBy; ++i)
        pumpOnce();
    std::multiset<int32_t> want;
    for (int32_t s = 1; s <= kN; ++s) want.insert(s);
    at1.got.erase(0); at2.got.erase(0);
    auto diff = [&](const std::multiset<int32_t>& got) {
        std::string d;
        for (int32_t s = 1; s <= kN; ++s) {
            const auto c = got.count(s);
            if (c != 1) d += " " + std::to_string(s) + "x" + std::to_string(c);
        }
        return d;
    };
    EXPECT_EQ(at2.got, want) << "node 1 -> node 2: every numbered frame exactly once; seq x count:" << diff(at2.got);
    EXPECT_EQ(at1.got, want) << "node 2 -> node 1: every numbered frame exactly once; seq x count:" << diff(at1.got);
    EXPECT_EQ(rekeys, 2);
    EXPECT_EQ(n1.unknownEpochFrames(), 0u);
    EXPECT_EQ(n2.unknownEpochFrames(), 0u);
    EXPECT_EQ(n1.peerFrameDrops(), 0u);
    EXPECT_EQ(n2.peerFrameDrops(), 0u);
    EXPECT_EQ(n1.peerLinkEpoch(2), 3u) << "both re-handshakes completed at the initiator";
    EXPECT_EQ(n2.peerLinkEpoch(1), 3u) << "and were promoted at the responder";
}
