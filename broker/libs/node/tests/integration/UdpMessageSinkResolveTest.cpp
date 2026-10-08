// UdpMessageSinkResolveTest.cpp — DR-BUG-4 regression test.
//
// Root cause (DR-4 quorum-loss recovery): UdpMessageSink resolved every peer
// consensus publication SYNCHRONOUSLY at construction and let a driver error
// escape.  addExclusivePublication() is async; findExclusivePublication() THROWS
// if the media driver returned an error — e.g. a peer hostname that does not yet
// resolve because that peer container is still (re)starting.  The escaping
// exception aborted BrokerNode::start() and the node EXITED.  When two nodes are
// restarted at once (quorum-loss recovery), each restarted node tried to resolve
// the OTHER still-down peer, threw "unknown host", and crashed — so the cluster
// never re-formed its ISR and acks=All could never reach minIsr(2).
//
// The fix makes peer resolution NON-FATAL and SELF-HEALING:
//   * The constructor swallows per-peer resolve errors (never throws).
//   * A peer whose registration errored is RE-REGISTERED so DNS is retried.
//   * resolvePending() runs one non-blocking probe per unresolved peer on every
//     send() (the single consensus duty-cycle thread).
//
// These tests use a real embedded Aeron media driver (no Docker):
//   1. UnresolvablePeer_DoesNotThrow_AtConstruction — a peer at a bogus
//      hostname must NOT abort construction (the core regression).
//   2. UnresolvablePeer_Send_IsSafeNoOp — sending to the unresolvable peer must
//      not throw and must be a silent drop; resolution keeps being retried.
//   3. ResolvablePeer_ResolvesAndConnects — a peer with a live subscription on
//      127.0.0.1 resolves and send() delivers (sanity: the fix does not break
//      the happy path).

#include <rune/aeronutil/testing/FreePort.h>
#include <rune/node/UdpMessageSink.h>

#include <gtest/gtest.h>

extern "C" {
#include <aeronmd/aeronmd.h>
}

#include <Aeron.h>
#include <Context.h>
#include <Subscription.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <string>
#include <thread>
#include <unistd.h>

#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

namespace fs = std::filesystem;
using namespace std::chrono_literals;
using rune::consensus::NodeId;
using rune::node::UdpMessageSink;

namespace {

// ---------------------------------------------------------------------------
// EmbeddedMediaDriver — RAII wrapper (mirrors AeronPartitionTransportIntegrationTest)
// ---------------------------------------------------------------------------
class EmbeddedMediaDriver {
public:
    explicit EmbeddedMediaDriver(const std::string& dir = "") {
        if (aeron_driver_context_init(&ctx_) < 0) {
            throw std::runtime_error(std::string("driver context init: ") + aeron_errmsg());
        }
        if (!dir.empty()) {
            aeron_driver_context_set_dir(ctx_, dir.c_str());
        }
        aeron_driver_context_set_threading_mode(ctx_, AERON_THREADING_MODE_SHARED);
        aeron_driver_context_set_dir_delete_on_start(ctx_, true);
        aeron_driver_context_set_dir_delete_on_shutdown(ctx_, true);
        aeron_driver_context_set_shared_idle_strategy(ctx_, "sleep-ns");
        aeron_driver_context_set_term_buffer_sparse_file(ctx_, true);
        aeron_driver_context_set_term_buffer_length(ctx_, 64 * 1024);
        aeron_driver_context_set_ipc_term_buffer_length(ctx_, 64 * 1024);

        if (aeron_driver_init(&driver_, ctx_) < 0) {
            aeron_driver_context_close(ctx_);
            throw std::runtime_error(std::string("driver init: ") + aeron_errmsg());
        }
        if (aeron_driver_start(driver_, true) < 0) {
            aeron_driver_close(driver_);
            aeron_driver_context_close(ctx_);
            throw std::runtime_error(std::string("driver start: ") + aeron_errmsg());
        }
        aeronDir_ = aeron_driver_context_get_dir(ctx_);
        running_.store(true, std::memory_order_release);
        thread_ = std::thread([this]() {
            while (running_.load(std::memory_order_acquire)) {
                aeron_driver_main_idle_strategy(driver_, aeron_driver_main_do_work(driver_));
            }
        });
    }

    ~EmbeddedMediaDriver() {
        running_.store(false, std::memory_order_release);
        if (thread_.joinable()) thread_.join();
        aeron_driver_close(driver_);
        aeron_driver_context_close(ctx_);
    }

    [[nodiscard]] const std::string& aeronDir() const { return aeronDir_; }

private:
    aeron_driver_context_t* ctx_    = nullptr;
    aeron_driver_t*         driver_ = nullptr;
    std::string             aeronDir_;
    std::atomic<bool>       running_{false};
    std::thread             thread_;
};

struct TempDir {
    fs::path dir;
    explicit TempDir(const std::string& prefix = "rune_udpsink_") {
        static std::atomic<int> ctr{0};
        auto ts = std::chrono::steady_clock::now().time_since_epoch().count();
        dir = fs::temp_directory_path() / (prefix + std::to_string(::getpid()) + "_" +
                                           std::to_string(ts) + "_" +
                                           std::to_string(ctr.fetch_add(1)));
        fs::create_directories(dir);
    }
    ~TempDir() { std::error_code ec; fs::remove_all(dir, ec); }
};

static uint16_t discoverFreeUdpPort() {
    return rune::aeronutil::testing::allocFreeUdpPort();   // D8: never an ephemeral port
}

static std::shared_ptr<aeron::Aeron> connectClient(const std::string& aeronDir) {
    aeron::Context ctx;
    ctx.aeronDir(aeronDir);
    return aeron::Aeron::connect(ctx);
}

constexpr int32_t kStreamId = 77;

} // namespace

// ===========================================================================
// TEST 1: a peer whose hostname does not resolve must NOT abort construction.
// This is the core DR-BUG-4 regression (previously the constructor threw the
// driver's "unknown host" RegistrationException and BrokerNode::start() aborted).
// ===========================================================================
TEST(UdpMessageSinkResolve, UnresolvablePeer_DoesNotThrow_AtConstruction) {
    TempDir td("rune_udpsink_driver_");
    EmbeddedMediaDriver driver((td.dir / "aeron").string());
    auto aeron = connectClient(driver.aeronDir());

    std::unordered_map<NodeId, UdpMessageSink::PeerEndpoint> peers;
    // A hostname that must never resolve.
    peers[1] = UdpMessageSink::PeerEndpoint{
        "aeron:udp?endpoint=rune-does-not-exist.invalid:40111", kStreamId};

    // MUST NOT throw — a temporarily/permanently unresolvable peer cannot abort
    // node startup.  (Before the fix this threw and crashed the node.)
    std::unique_ptr<UdpMessageSink> sink;
    ASSERT_NO_THROW(
        sink = std::make_unique<UdpMessageSink>(aeron, peers));
    ASSERT_NE(sink, nullptr);
}

// ===========================================================================
// TEST 2: sending to an unresolvable peer is a safe silent no-op (never throws),
// and repeated sends keep retrying resolution without spinning/aborting.
// ===========================================================================
TEST(UdpMessageSinkResolve, UnresolvablePeer_Send_IsSafeNoOp) {
    TempDir td("rune_udpsink_driver_");
    EmbeddedMediaDriver driver((td.dir / "aeron").string());
    auto aeron = connectClient(driver.aeronDir());

    std::unordered_map<NodeId, UdpMessageSink::PeerEndpoint> peers;
    peers[1] = UdpMessageSink::PeerEndpoint{
        "aeron:udp?endpoint=rune-does-not-exist.invalid:40111", kStreamId};

    UdpMessageSink sink(aeron, peers);

    const std::string payload = "hello";
    // Repeated sends must not throw; the peer is never resolvable so each send
    // silently drops (and internally re-registers to retry DNS).
    for (int i = 0; i < 50; ++i) {
        ASSERT_NO_THROW(sink.send(
            1,
            reinterpret_cast<const uint8_t*>(payload.data()),
            payload.size()));
        // Also sending to an UNKNOWN peer id must be a silent no-op.
        ASSERT_NO_THROW(sink.send(
            99,
            reinterpret_cast<const uint8_t*>(payload.data()),
            payload.size()));
        std::this_thread::sleep_for(2ms);
    }
}

// ===========================================================================
// TEST 3: happy path — a peer with a live 127.0.0.1 subscription resolves and
// send() delivers.  Ensures the self-healing rework did not break normal wiring.
// ===========================================================================
TEST(UdpMessageSinkResolve, ResolvablePeer_ResolvesAndDelivers) {
    TempDir td("rune_udpsink_driver_");
    EmbeddedMediaDriver driver((td.dir / "aeron").string());

    // One client hosts the peer's ingress subscription; another hosts the sink.
    auto subClient  = connectClient(driver.aeronDir());
    auto sinkClient = connectClient(driver.aeronDir());

    const uint16_t peerPort = discoverFreeUdpPort();
    const std::string peerChannel =
        "aeron:udp?endpoint=127.0.0.1:" + std::to_string(peerPort);

    // Bring up the peer's ingress subscription so the publication can connect.
    int64_t subRegId = subClient->addSubscription(peerChannel, kStreamId);
    std::shared_ptr<aeron::Subscription> sub;
    auto subDeadline = std::chrono::steady_clock::now() + 5s;
    while (std::chrono::steady_clock::now() < subDeadline) {
        sub = subClient->findSubscription(subRegId);
        if (sub) break;
        std::this_thread::sleep_for(1ms);
    }
    ASSERT_NE(sub, nullptr) << "peer subscription did not resolve";

    std::unordered_map<NodeId, UdpMessageSink::PeerEndpoint> peers;
    peers[1] = UdpMessageSink::PeerEndpoint{peerChannel, kStreamId};

    UdpMessageSink sink(sinkClient, peers);

    // Deliver a message: send() runs resolvePending() then offers when connected.
    const std::string payload = "consensus-frame";
    std::atomic<int> received{0};
    aeron::fragment_handler_t handler =
        [&](aeron::concurrent::AtomicBuffer&, aeron::util::index_t,
            aeron::util::index_t, aeron::Header&) {
            received.fetch_add(1, std::memory_order_relaxed);
        };

    auto deadline = std::chrono::steady_clock::now() + 10s;
    while (received.load(std::memory_order_relaxed) == 0 &&
           std::chrono::steady_clock::now() < deadline) {
        sink.send(1, reinterpret_cast<const uint8_t*>(payload.data()), payload.size());
        sub->poll(handler, 10);
        std::this_thread::sleep_for(2ms);
    }

    EXPECT_GT(received.load(std::memory_order_relaxed), 0)
        << "resolvable peer never received a frame — happy path broken";
}
