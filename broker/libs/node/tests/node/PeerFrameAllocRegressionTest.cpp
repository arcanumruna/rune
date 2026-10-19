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

#include <rune/peerlink/PeerFrameCrypto.h>
#include <rune/replication/ReplicationMessages.h>
#include <rune/replication/WorkerReplicationTransport.h>

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
    MediaDriver() {
        static std::atomic<int> seq{0};
        dir_ = (fs::temp_directory_path() / ("rune_pfa_" + std::to_string(::getpid()) + "_" + std::to_string(seq++))).string();
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
