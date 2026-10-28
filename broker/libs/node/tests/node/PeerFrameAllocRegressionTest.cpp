// PeerFrameAllocRegressionTest.cpp — authentication plan Task 16 (hard rule 4): the steady state of SEALED
// replication allocates nothing on the thread that owns the transport.  Replaces the global operator new (binary-wide),
// so it lives in its own binary (rune_peer_frame_alloc_regression_tests) - the Security/Catalog alloc-gate pattern.
// Only allocations made by THIS test thread count (the embedded media driver thread allocates freely).
#include <gtest/gtest.h>

extern "C" {
#include <aeronmd/aeronmd.h>
}
#include <Aeron.h>
#include <Context.h>

#include <rune/aeronutil/testing/FreePort.h>
#include <rune/consensus/AeronIngress.h>
#include <rune/consensus/ConsensusAdapter.h>
#include <rune/consensus/IngressRing.h>
#include <rune/consensus/ConsensusMessages.h>
#include <rune/consensus/UdpMessageSink.h>
#include <rune/node/EmbeddedDriver.h>
#include <rune/node/PeerLinkAgent.h>
#include <rune/peerlink/PeerFrameCrypto.h>
#include <rune/peerlink/PeerHandshake.h>
#include <rune/replication/AeronPartitionTransport.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/replication/ReplicationMessages.h>
#include <rune/replication/WorkerReplicationTransport.h>
#include <rune/storage/Log.h>

#include <openssl/crypto.h>
#include <openssl/evp.h>
#include <pthread.h>

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <new>
#include <thread>
#include <unistd.h>
#include <vector>
#include <execinfo.h>

namespace {
std::atomic<bool> gArmed{false};
pthread_t         gOwner;
std::atomic<uint64_t> gAllocs{0};

bool mine() { return gArmed.load(std::memory_order_relaxed) && pthread_equal(pthread_self(), gOwner); }
void report(std::size_t n) {
    if (gAllocs.fetch_add(1) < 3) {
        gArmed.store(false);
        void* frames[32];
        const int k = ::backtrace(frames, 32);
        std::fprintf(stderr, "peer frame alloc gate: allocation of %zu bytes:\n", n);
        ::backtrace_symbols_fd(frames, k, 2);
        gArmed.store(true);
    }
}
}  // namespace
void* operator new(std::size_t n) {
    if (mine()) [[unlikely]] report(n);
    if (void* p = std::malloc(n ? n : 1)) return p;
    throw std::bad_alloc();
}
void* operator new[](std::size_t n) {
    if (mine()) [[unlikely]] report(n);
    if (void* p = std::malloc(n ? n : 1)) return p;
    throw std::bad_alloc();
}
// OpenSSL allocates through its own hooks (not operator new): count those too, on the owner thread, while armed.
namespace {
void* countingMalloc(std::size_t n, const char*, int) { if (mine()) report(n); return std::malloc(n ? n : 1); }
void* countingRealloc(void* p, std::size_t n, const char*, int) { if (mine()) report(n); return std::realloc(p, n ? n : 1); }
void  countingFree(void* p, const char*, int) { std::free(p); }
const bool kHooked = (CRYPTO_set_mem_functions(&countingMalloc, &countingRealloc, &countingFree) == 1);
}  // namespace
void operator delete(void* p) noexcept { std::free(p); }
void operator delete(void* p, std::size_t) noexcept { std::free(p); }
void operator delete[](void* p) noexcept { std::free(p); }
void operator delete[](void* p, std::size_t) noexcept { std::free(p); }
// The nothrow forms route through the replaced throwing forms. Otherwise a nothrow allocation (libstdc++'s
// get_temporary_buffer under std::stable_sort, e.g. in gtest) comes from the default/ASan allocator but is released
// by the replaced delete: ASan alloc-dealloc-mismatch, and the allocation goes uncounted.
void* operator new(std::size_t n, const std::nothrow_t&) noexcept {
    try { return ::operator new(n); } catch (...) { return nullptr; }
}
void* operator new[](std::size_t n, const std::nothrow_t&) noexcept {
    try { return ::operator new[](n); } catch (...) { return nullptr; }
}
void operator delete(void* p, const std::nothrow_t&) noexcept { ::operator delete(p); }
void operator delete[](void* p, const std::nothrow_t&) noexcept { ::operator delete[](p); }

namespace fs = std::filesystem;
namespace pl = rune::peerlink;
namespace rr = rune::replication;
using namespace std::chrono_literals;

namespace {

struct Armed {
    Armed() { gOwner = pthread_self(); gAllocs.store(0); gArmed.store(true); }
    ~Armed() { gArmed.store(false); }
};

pl::LinkKeys linkFor(bool aToB) {
    pl::LinkKeys k; k.epoch = 1;
    k.sendMaster.fill(aToB ? 1 : 2); k.recvMaster.fill(aToB ? 2 : 1);
    return k;
}

class MediaDriver {
public:
    MediaDriver() : driver_(makeCfg(), rune::node::DriverMode::Threaded) {}
    [[nodiscard]] const std::string& dir() const { return driver_.dir(); }
private:
    static rune::node::EmbeddedDriverConfig makeCfg() {
        static std::atomic<int> seq{0};
        rune::node::EmbeddedDriverConfig c;
        c.dir = (fs::temp_directory_path() / ("rune_pfa_" + std::to_string(::getpid()) + "_" + std::to_string(seq++))).string();
        c.termBufferBytes  = 64 * 1024;
        c.termBufferSparse = true;
        c.udpGso           = false;
        c.epollPoll        = false;
        return c;
    }
    rune::node::EmbeddedDriver driver_;   // the one dedicated pump thread
};

}  // namespace

// The gate itself: it sees operator new AND OpenSSL's own allocations on the owner thread (else a zero proves nothing).
TEST(PeerFrameAllocRegression, TheGateSeesBothKindsOfAllocation) {
    ASSERT_TRUE(kHooked);
    { Armed armed; std::vector<uint8_t> v(100); v[0] = 1; EXPECT_EQ(v[0], 1); }
    EXPECT_GE(gAllocs.load(), 1u) << "operator new";
    { Armed armed; EVP_CIPHER_CTX* c = EVP_CIPHER_CTX_new(); EVP_CIPHER_CTX_free(c); }
    EXPECT_GE(gAllocs.load(), 1u) << "OpenSSL's allocator";
}

// The crypto alone: seal / sealInPlace / open of a replication-sized frame, nothing on the heap.
TEST(PeerFrameAllocRegression, SealAndOpenAllocateNothing) {
    ASSERT_TRUE(kHooked) << "the OpenSSL allocation hooks must be installed before OpenSSL initialises";
    pl::PeerFrameCrypto a(1, 3), b(2, 3);
    ASSERT_TRUE(a.installLink(2, linkFor(true), 0));
    ASSERT_TRUE(b.installLink(1, linkFor(false), 0));
    std::vector<uint8_t> plain(16 * 1024, 0x5A), wire(plain.size() + 32), out(plain.size() + 32), inplace(plain.size() + 32);
    std::size_t outLen = 0; int32_t from = -1; uint32_t epoch = 0;
    { const auto n = a.seal(2, plain.data(), plain.size(), wire.data(), wire.size()); ASSERT_GT(n, 0u); ASSERT_EQ(b.open(wire.data(), n, out.data(), out.size(), outLen, from, epoch), pl::PeerFrameCrypto::OpenResult::Ok); }
    uint64_t sealed = 0;
    {
        Armed armed;
        for (int i = 0; i < 2000; ++i) {
            const auto n = a.seal(2, plain.data(), plain.size(), wire.data(), wire.size());
            sealed += n;
            if (b.open(wire.data(), n, out.data(), out.size(), outLen, from, epoch) != pl::PeerFrameCrypto::OpenResult::Ok) ADD_FAILURE_AT(__FILE__, __LINE__);
            std::memcpy(inplace.data() + pl::kPeerHeaderBytes, plain.data(), plain.size());
            const auto m = a.sealInPlace(2, inplace.data(), plain.size(), inplace.size());
            sealed += m;
            if (b.open(inplace.data(), m, out.data(), out.size(), outLen, from, epoch) != pl::PeerFrameCrypto::OpenResult::Ok) ADD_FAILURE_AT(__FILE__, __LINE__);
        }
    }
    EXPECT_GT(sealed, 0u);
    EXPECT_EQ(gAllocs.load(), 0u);
    // A refusal path allocates nothing either (a tampered frame, no-link send).
    wire[40] ^= 1;
    {
        Armed armed;
        for (int i = 0; i < 1000; ++i) {
            (void)b.open(wire.data(), plain.size() + 32, out.data(), out.size(), outLen, from, epoch);
            (void)a.seal(99, plain.data(), 100, wire.data(), wire.size());
        }
    }
    EXPECT_EQ(gAllocs.load(), 0u);
}

// Handshake frame processing that needs no crypto - the frames an attacker can send at will - allocates nothing:
// forged Refuse / unknown-epoch hints / bad-key Hello / idle ticks, and bogus sender ids at the frame layer.
TEST(PeerFrameAllocRegression, HandshakeFrameProcessingNeverAllocatesRegression) {
    ASSERT_TRUE(kHooked);
    namespace pl = rune::peerlink;
    std::string k(48, 'k');
    auto m = rune::crypto::deriveClusterKey({reinterpret_cast<const uint8_t*>(k.data()), k.size()});
    ASSERT_TRUE(m);
    pl::ClusterKeys keys{*m, std::nullopt};
    pl::PeerHandshake a(1, 2, 10, keys), b(2, 1, 20, keys);
    (void)a.tick(0);   // an initiator in HelloSent (setup may allocate)
    pl::PeerRefuse r; r.from = 2; r.to = 1;
    pl::PeerHello bad; bad.from = 1; bad.to = 2; bad.keyId = 0xdead; bad.incarnation = 1;
    pl::PeerChallenge ch; ch.from = 2; ch.to = 1; ch.keyId = 0xbeef;   // not our key id
    pl::PeerAccept acc; acc.from = 2; acc.to = 1;
    pl::PeerFrameCrypto fa(1, 3), fb(2, 3);
    ASSERT_TRUE(fa.installLink(2, linkFor(true), 0)); ASSERT_TRUE(fb.installLink(1, linkFor(false), 0));
    std::vector<uint8_t> wire(64), out(64);
    const auto n = fa.seal(2, reinterpret_cast<const uint8_t*>("payload"), 7, wire.data(), wire.size());
    std::size_t outLen = 0; int32_t from = -1; uint32_t epoch = 0;
    uint64_t seen = 0;
    {
        Armed armed;
        for (int i = 1; i <= 2000; ++i) {
            const int64_t now = i * 1'000'000;
            for (auto reason : {pl::RefuseReason::NoCommonKey, pl::RefuseReason::BadProof, pl::RefuseReason::NoLink}) {
                r.reason = reason;
                seen += a.onRefuse(r, now).log ? 1 : 0;
            }
            seen += a.onUnknownEpoch(static_cast<uint32_t>(i), now).frame ? 1 : 0;
            seen += a.onChallenge(ch, now).frame ? 1 : 0;
            seen += a.onAccept(acc, now).frame ? 1 : 0;
            seen += b.onHello(bad, now).frame ? 1 : 0;     // a Refuse{NoCommonKey}: a fixed buffer, no heap
            seen += a.tick(now).frame ? 1 : 0;              // the Hello repeat
            wire[1] = static_cast<uint8_t>(10 + (i % 200));   // a bogus sender id
            (void)fb.open(wire.data(), n, out.data(), out.size(), outLen, from, epoch);
        }
    }
    EXPECT_GT(seen, 0u);
    EXPECT_EQ(gAllocs.load(), 0u);
}

// The real thing: two sealed per-worker transports over aeron:ipc; a request, a one-MTU response and a LARGE
// (fragmented, scratch-sealed) response per round, polled by their owner threads - the test thread owns both.
TEST(PeerFrameAllocRegression, SteadySealedReplicationAllocatesNothing) {
    MediaDriver driver;
    aeron::Context ctx; ctx.aeronDir(driver.dir());
    auto aeron = aeron::Aeron::connect(ctx);
    constexpr int32_t kStreamA = 700, kStreamB = 701;
    rr::WorkerReplicationTransport a(aeron, 0, "aeron:ipc", kStreamA,
        {{2, "aeron:ipc"}}, kStreamB, rune::aeronutil::AeronPump{}, pl::PeerFrameCrypto(1, 3));
    rr::WorkerReplicationTransport b(aeron, 0, "aeron:ipc", kStreamB,
        {{1, "aeron:ipc"}}, kStreamA, rune::aeronutil::AeronPump{}, pl::PeerFrameCrypto(2, 3));
    ASSERT_TRUE(a.resolved() && b.resolved());
    ASSERT_TRUE(a.installPeerLink(2, linkFor(true)));
    ASSERT_TRUE(b.installPeerLink(1, linkFor(false)));
    for (int i = 0; i < 5000 && !(a.allPeersResolved() && b.allPeersResolved()); ++i) { (void)a.drivePeerResolve(); (void)b.drivePeerResolve(); std::this_thread::sleep_for(1ms); }
    std::this_thread::sleep_for(100ms);

    rr::FetchRequestMsg req{};
    req.leaderEpoch = 3; req.partition = 7; req.followerId = 1; req.fetchOffset = 42; req.maxBytes = 4096; req.topicId = 5;
    const std::vector<uint8_t> small(900, 0x11), large(6000, 0x22);
    uint64_t gotReq = 0, gotResp = 0;
    auto onB = [&](int32_t, int32_t, uint16_t tid, const uint8_t*, std::size_t) { if (tid == rr::FetchRequestMsg::TEMPLATE_ID) ++gotReq; };
    auto onA = [&](int32_t, int32_t, uint16_t tid, const uint8_t*, std::size_t) { if (tid == rr::FetchResponseMsg::TEMPLATE_ID) ++gotResp; };
    auto round = [&] {
        rr::FetchResponseMsg resp{};
        resp.leaderEpoch = 3; resp.partition = 7; resp.highWatermark = 99; resp.topicId = 5;
        (void)a.sendFetchRequest(req);
        resp.records = std::span<const uint8_t>(small); (void)b.sendFetchResponse(1, resp);
        resp.records = std::span<const uint8_t>(large); (void)b.sendFetchResponse(1, resp);
        for (int i = 0; i < 20; ++i) { (void)b.poll(8, onB); (void)a.poll(8, onA); }
    };
    for (int i = 0; i < 200; ++i) { round(); std::this_thread::sleep_for(200us); }   // warm-up: the assembler's per-image buffer, ...
    ASSERT_GT(gotReq, 0u); ASSERT_GT(gotResp, 0u);
    const uint64_t req0 = gotReq, resp0 = gotResp;
    {
        Armed armed;
        for (int i = 0; i < 2000; ++i) round();
    }
    EXPECT_EQ(gAllocs.load(), 0u) << "sealed replication allocated on the transport's thread in steady state";
    EXPECT_GT(gotReq, req0);
    EXPECT_GT(gotResp, resp0) << "frames really flowed during the armed window";
    EXPECT_EQ(a.peerFrameDrops() + b.peerFrameDrops(), 0u);
}

// Review of PR #551 (issue #17), hot path: the sealed idle keepalive runs on the control thread every duty cycle.  In
// steady state, sendIdleKeepalives (the idle check AND the seal + send of a due keepalive) and the receive side (the
// consensus ingress opening it, the ring, the adapter consuming it) allocate nothing on that thread.
// Mutation: building the keepalive frame in a std::vector inside sendIdleKeepalives turns this red.
namespace {
struct NoopConsensusHandler final : rune::consensus::ConsensusMessageHandler {
    uint64_t calls = 0;
    void onCanvassPosition(int64_t, rune::consensus::Term, int64_t, rune::consensus::Term, rune::consensus::NodeId, int32_t) override { ++calls; }
    void onRequestVote(int64_t, rune::consensus::Term, int64_t, rune::consensus::Term, rune::consensus::NodeId, int32_t) override { ++calls; }
    void onVote(int64_t, rune::consensus::Term, rune::consensus::Term, int64_t, rune::consensus::NodeId, rune::consensus::NodeId, bool) override { ++calls; }
    void onNewLeadershipTerm(int64_t, rune::consensus::Term, rune::consensus::Term, int64_t, int64_t, rune::consensus::Term, int64_t, int64_t,
                             int64_t, int64_t, rune::consensus::NodeId, int32_t, int32_t, bool, int64_t) override { ++calls; }
    void onAppendPosition(int64_t, rune::consensus::Term, int64_t, rune::consensus::NodeId, uint8_t) override { ++calls; }
    void onCommitPosition(int64_t, rune::consensus::Term, int64_t, rune::consensus::NodeId) override { ++calls; }
};
}  // namespace

TEST(PeerFrameAllocRegression, IdleKeepalivesAllocateNothingAdversarialRegression) {
    namespace rc = rune::consensus;
    ASSERT_TRUE(kHooked);
    MediaDriver driver;
    aeron::Context ctx; ctx.aeronDir(driver.dir());
    auto aeron = aeron::Aeron::connect(ctx);
    constexpr int32_t kStream = 702;
    rc::IngressRing ring(64 * 1024);
    rc::AeronIngress ingress(aeron, "aeron:ipc", kStream, ring, rune::aeronutil::AeronPump{}, pl::PeerFrameCrypto(2, 0));
    std::unordered_map<rc::NodeId, rc::UdpMessageSink::PeerEndpoint> eps{{2, {"aeron:ipc", kStream}}};
    rc::UdpMessageSink sink(aeron, eps, rune::aeronutil::AeronPump{}, rc::PeerResolutionSource{}, pl::PeerFrameCrypto(1, 0));
    ASSERT_TRUE(sink.installPeerLink(2, linkFor(true)));
    ASSERT_TRUE(ingress.installPeerLink(1, linkFor(false)));
    NoopConsensusHandler handler;
    rc::ConsensusAdapter adapter{handler};
    auto drain = [&] {
        (void)ingress.poll(10);
        (void)ring.drain([&](int32_t, aeron::concurrent::AtomicBuffer& b, aeron::util::index_t off, aeron::util::index_t len) {
            adapter.onFragment(0, b.buffer() + off, static_cast<std::size_t>(len));
        }, 64);
    };
    int64_t t = 0;
    for (int i = 0; i < 5000 && ingress.authFrames(1) == 0; ++i) {   // warm-up: the publication connects, the first opens
        t += rc::UdpMessageSink::kPeerKeepaliveNs;
        (void)sink.sendIdleKeepalives(t);
        drain();
        std::this_thread::sleep_for(1ms);
    }
    ASSERT_GT(ingress.authFrames(1), 0u) << "keepalives never got through";
    for (int i = 0; i < 50; ++i) { t += rc::UdpMessageSink::kPeerKeepaliveNs; (void)sink.sendIdleKeepalives(t); drain(); }
    const uint64_t sent0 = sink.keepalivesSent(), opened0 = ingress.authFrames(1);
    {
        Armed armed;
        for (int i = 0; i < 2000; ++i) {
            t += rc::UdpMessageSink::kPeerKeepaliveNs / 5;   // 4 idle checks, then a due keepalive
            (void)sink.sendIdleKeepalives(t);
            drain();
        }
    }
    EXPECT_EQ(gAllocs.load(), 0u) << "the idle keepalive allocated on the control thread in steady state";
    EXPECT_EQ(sink.keepalivesSent() - sent0, 400u) << "one keepalive per 250 ms of injected time";
    for (int i = 0; i < 2000 && ingress.authFrames(1) < opened0 + 400; ++i) { drain(); std::this_thread::sleep_for(1ms); }
    EXPECT_EQ(ingress.authFrames(1), opened0 + 400) << "every keepalive was really sealed, sent and opened";
    EXPECT_EQ(adapter.ignoredFrameCount(), 0);
    EXPECT_EQ(handler.calls, 0u);
}

// Issue #275 (f): the two other sealed planes in steady state, on the thread that owns them.
//   - legacy replication: two sealed AeronPartitionTransports (sender id 1) driving a real leader / follower
//     PartitionReplica pair - the follower's fetch, the leader's (empty) response, both sealed and opened;
//   - consensus: UdpMessageSink seals consensus frames, AeronIngress opens them into the IngressRing, the adapter
//     dispatches them.
// Mutation: a std::vector built per sealed frame in AeronPartitionTransport's send path (or the sink's) turns this red.
TEST(PeerFrameAllocRegression, SealedLegacyAndConsensusPathsAllocateNothingRegression) {
    namespace rc = rune::consensus;
    ASSERT_TRUE(kHooked);
    MediaDriver driver;
    aeron::Context ctx; ctx.aeronDir(driver.dir());
    auto aeron = aeron::Aeron::connect(ctx);

    // ---- legacy replication (the control-thread AeronPartitionTransport) ----
    const fs::path dir = fs::temp_directory_path() / ("rune_pfa_logs_" + std::to_string(::getpid()));
    fs::create_directories(dir / "l"); fs::create_directories(dir / "f");
    {
        constexpr int32_t kL = 710, kF = 711;
        rune::storage::Log ldrLog((dir / "l").string()), flwLog((dir / "f").string());
        rr::AeronPartitionTransport ldr(aeron, "aeron:ipc", kL, 1, 4 * 1024 * 1024, rune::aeronutil::AeronPump{}, pl::PeerFrameCrypto(1, 1));
        rr::AeronPartitionTransport flw(aeron, "aeron:ipc", kF, 2, 4 * 1024 * 1024, rune::aeronutil::AeronPump{}, pl::PeerFrameCrypto(2, 1));
        ldr.setPublicationMaxMessageBytes(rr::aeronMaxMessageLength(64 * 1024));
        flw.setPublicationMaxMessageBytes(rr::aeronMaxMessageLength(64 * 1024));
        ldr.addPeer(2, "aeron:ipc", kF);
        flw.addPeer(1, "aeron:ipc", kL);
        ASSERT_TRUE(ldr.installPeerLink(2, linkFor(true)));
        ASSERT_TRUE(flw.installPeerLink(1, linkFor(false)));
        auto cfg = [](rr::NodeId self) {
            rr::PartitionConfig c;
            c.partitionId = 0; c.selfId = self; c.replicaLagMaxMs = 5000; c.replicaLagMaxOffset = 100000; c.minIsr = 1;
            c.fetchIntervalNs = 5'000'000LL; c.isrCheckIntervalNs = 10'000'000LL; c.maxFetchBytes = 1 << 20;
            return c;
        };
        const std::vector<rr::NodeId> nodes{1, 2};
        rr::PartitionReplica leader(cfg(1), rr::Role::Leader, 1, nodes, nodes, &ldrLog, &ldr);
        rr::PartitionReplica follower(cfg(2), rr::Role::Follower, 1, nodes, nodes, &flwLog, &flw);
        ldr.registerReplica(0, 0, &leader);
        flw.registerReplica(0, 0, &follower);
        int64_t simNs = 0;
        auto step = [&] { simNs += 5'000'000LL; (void)ldr.poll(simNs); (void)flw.poll(simNs); leader.doWork(simNs); follower.doWork(simNs); };
        for (int i = 0; i < 5; ++i) { std::vector<uint8_t> v(64, static_cast<uint8_t>(i)); ASSERT_GE(leader.append(v), 0); }
        const rr::Offset target = leader.logEndOffset();
        for (int i = 0; i < 20'000 && !(follower.logEndOffset() >= target && leader.highWatermark() >= target); ++i) {
            step(); std::this_thread::sleep_for(100us);
        }
        ASSERT_GE(follower.logEndOffset(), target) << "warm-up: the sealed fetch loop converged";
        for (int i = 0; i < 500; ++i) step();   // steady state: empty fetches, every buffer sized
        const uint64_t ldrFrames0 = ldr.authFrames(2), flwFrames0 = flw.authFrames(1);
        {
            Armed armed;
            for (int i = 0; i < 2000; ++i) step();
        }
        EXPECT_EQ(gAllocs.load(), 0u) << "the sealed legacy replication plane allocated in steady state";
        EXPECT_GT(ldr.authFrames(2), ldrFrames0) << "fetch requests really reached the leader, sealed";
        EXPECT_GT(flw.authFrames(1), flwFrames0) << "fetch responses really reached the follower, sealed";
        EXPECT_EQ(ldr.peerFrameDrops() + flw.peerFrameDrops(), 0u);
    }
    std::error_code ec;
    fs::remove_all(dir, ec);

    // ---- consensus (UdpMessageSink -> AeronIngress -> IngressRing -> ConsensusAdapter) ----
    constexpr int32_t kStream = 712;
    rc::IngressRing ring(64 * 1024);
    rc::AeronIngress ingress(aeron, "aeron:ipc", kStream, ring, rune::aeronutil::AeronPump{}, pl::PeerFrameCrypto(2, 0));
    std::unordered_map<rc::NodeId, rc::UdpMessageSink::PeerEndpoint> eps{{2, {"aeron:ipc", kStream}}};
    rc::UdpMessageSink sink(aeron, eps, rune::aeronutil::AeronPump{}, rc::PeerResolutionSource{}, pl::PeerFrameCrypto(1, 0));
    ASSERT_TRUE(sink.installPeerLink(2, linkFor(true)));
    ASSERT_TRUE(ingress.installPeerLink(1, linkFor(false)));
    NoopConsensusHandler handler;
    rc::ConsensusAdapter adapter{handler};
    std::array<uint8_t, rc::CommitPositionMsg::TOTAL_SIZE> commit{};
    std::array<uint8_t, rc::AppendPositionMsg::TOTAL_SIZE> append{};
    auto round = [&](int64_t i) {
        (void)rc::CommitPositionMsg::encode(commit.data(), 0, /*term=*/1, /*logPosition=*/i, /*leader=*/1);
        (void)rc::AppendPositionMsg::encode(append.data(), 0, /*term=*/1, /*logPosition=*/i, /*follower=*/1, /*flags=*/0);
        sink.send(2, commit.data(), commit.size());
        sink.send(2, append.data(), append.size());
        (void)ingress.poll(10);
        (void)ring.drain([&](int32_t, aeron::concurrent::AtomicBuffer& b, aeron::util::index_t off, aeron::util::index_t len) {
            adapter.onFragment(0, b.buffer() + off, static_cast<std::size_t>(len));
        }, 64);
    };
    for (int i = 0; i < 5000 && handler.calls == 0; ++i) { round(i); std::this_thread::sleep_for(1ms); }   // the publication connects
    ASSERT_GT(handler.calls, 0u) << "consensus frames never got through";
    for (int i = 0; i < 200; ++i) round(i);
    const uint64_t calls0 = handler.calls;
    {
        Armed armed;
        for (int i = 0; i < 2000; ++i) round(1000 + i);
    }
    EXPECT_EQ(gAllocs.load(), 0u) << "the sealed consensus plane allocated on the control thread in steady state";
    EXPECT_GT(handler.calls, calls0) << "consensus frames really flowed during the armed window";
    EXPECT_EQ(ingress.peerFrameDrops() + sink.peerFrameDrops(), 0u);
    EXPECT_EQ(adapter.ignoredFrameCount(), 0);
}

// Issue #275 (g): an idle Established PeerLinkAgent - the control thread's per-cycle doWork (the subscription poll and
// every handshake's tick) plus the authenticated-frame observation BrokerNode feeds it - allocates nothing.
// Mutation: a std::string built per tick in PeerLinkAgent::doWork turns this red.
TEST(PeerFrameAllocRegression, AnIdleEstablishedPeerLinkAgentAllocatesNothingRegression) {
    ASSERT_TRUE(kHooked);
    MediaDriver driver;
    aeron::Context ctx; ctx.aeronDir(driver.dir());
    auto aeron = aeron::Aeron::connect(ctx);
    std::string k(48, 'k');
    auto m = rune::crypto::deriveClusterKey({reinterpret_cast<const uint8_t*>(k.data()), k.size()});
    ASSERT_TRUE(m);
    const uint16_t ports[2] = {rune::aeronutil::testing::allocFreeUdpPort(), rune::aeronutil::testing::allocFreeUdpPort()};
    auto chan = [&](int i) { return "aeron:udp?endpoint=127.0.0.1:" + std::to_string(ports[i]); };
    std::unique_ptr<rune::node::PeerLinkAgent> agents[2];
    for (int i = 0; i < 2; ++i) {
        rune::node::PeerLinkAgent::Params p;
        p.selfId = i + 1; p.incarnation = 100 + static_cast<uint64_t>(i); p.streamId = 41; p.keys = {*m, std::nullopt};
        p.bindChannel = chan(i);
        p.peers.push_back({2 - i, chan(1 - i)});
        agents[i] = std::make_unique<rune::node::PeerLinkAgent>(aeron, std::move(p));
    }
    auto nowNs = [] { return std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch()).count(); };
    for (int i = 0; i < 10'000 && !(agents[0]->establishedPeers() == 1 && agents[1]->establishedPeers() == 1); ++i) {
        for (auto& a : agents) (void)a->doWork(nowNs());
        std::this_thread::sleep_for(1ms);
    }
    ASSERT_EQ(agents[0]->establishedPeers(), 1);
    ASSERT_EQ(agents[1]->establishedPeers(), 1);
    for (int i = 0; i < 200; ++i) for (auto& a : agents) (void)a->doWork(nowNs());   // drain any late handshake frame
    uint64_t auth = 0;
    {
        Armed armed;
        for (int i = 0; i < 5000; ++i) {
            const int64_t now = nowNs();
            for (int a = 0; a < 2; ++a) {
                (void)agents[a]->doWork(now);
                agents[a]->observeAuthFrames(2 - a, ++auth, now);   // the transports' liveness counter, as BrokerNode feeds it
            }
        }
    }
    EXPECT_EQ(gAllocs.load(), 0u) << "an idle established PeerLinkAgent allocated on the control thread";
    EXPECT_EQ(agents[0]->establishedPeers(), 1);
    EXPECT_EQ(agents[1]->establishedPeers(), 1);
}
