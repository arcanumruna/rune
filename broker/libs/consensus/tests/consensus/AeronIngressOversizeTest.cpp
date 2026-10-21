// AeronIngressOversizeTest.cpp — #254: an inbound consensus frame larger than the ingress ring's maximum message is
// dropped, counted and logged rate-limited by AeronIngress::poll; it never throws out of the consensus duty cycle.
// Real Aeron UDP (loopback) into a real AeronIngress, plain and sealed.  The raw publication uses a 1 MiB term so it
// can carry frames above the ring maximum (Aeron's own message limit is term / 8).
#include <gtest/gtest.h>

extern "C" {
#include <aeronmd/aeronmd.h>
}
#include <Aeron.h>
#include <Context.h>
#include <ExclusivePublication.h>

#include <rune/aeronutil/testing/FreePort.h>
#include <rune/consensus/AeronIngress.h>
#include <rune/consensus/IngressRing.h>
#include <rune/consensus/UdpMessageSink.h>
#include <rune/peerlink/PeerFrameCrypto.h>

#include <spdlog/sinks/ringbuffer_sink.h>
#include <spdlog/spdlog.h>

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
        dir_ = (fs::temp_directory_path() / ("rune_aio_" + std::to_string(::getpid()) + "_" + std::to_string(seq++))).string();
        if (aeron_driver_context_init(&ctx_) < 0) throw std::runtime_error(aeron_errmsg());
        aeron_driver_context_set_dir(ctx_, dir_.c_str());
        aeron_driver_context_set_threading_mode(ctx_, AERON_THREADING_MODE_SHARED);
        aeron_driver_context_set_dir_delete_on_start(ctx_, true);
        aeron_driver_context_set_dir_delete_on_shutdown(ctx_, true);
        aeron_driver_context_set_shared_idle_strategy(ctx_, "sleep-ns");
        aeron_driver_context_set_term_buffer_sparse_file(ctx_, true);
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

pl::LinkKeys linkFor(bool lowToHigh, uint8_t base) {
    pl::LinkKeys k; k.epoch = 1;
    k.sendMaster.fill(lowToHigh ? base : static_cast<uint8_t>(base + 1));
    k.recvMaster.fill(lowToHigh ? static_cast<uint8_t>(base + 1) : base);
    return k;
}

constexpr int32_t kStream = 91;

/// One ingress (node 1) and a raw publication into it, playing a peer (node 2) or an attacker.
struct Rig {
    MediaDriver                                 driver;
    std::shared_ptr<aeron::Aeron>               aeron;
    uint16_t                                    port = rune::aeronutil::testing::allocFreeUdpPort();
    IngressRing                                 ring{kDefaultIngressCapacity};
    std::unique_ptr<AeronIngress>               ingress;
    std::shared_ptr<aeron::ExclusivePublication> pub;
    std::optional<pl::PeerFrameCrypto>          peer;   // the sender's crypto in sealed mode
    std::shared_ptr<spdlog::sinks::ringbuffer_sink_mt> logSink = std::make_shared<spdlog::sinks::ringbuffer_sink_mt>(256);
    std::shared_ptr<spdlog::logger>             prevLogger = spdlog::default_logger();

    explicit Rig(bool sealed) {
        spdlog::set_default_logger(std::make_shared<spdlog::logger>("aio", logSink));
        aeron::Context ctx; ctx.aeronDir(driver.dir());
        aeron = aeron::Aeron::connect(ctx);
        const std::string chan = "aeron:udp?endpoint=127.0.0.1:" + std::to_string(port);
        ingress = std::make_unique<AeronIngress>(aeron, chan, kStream, ring, rune::aeronutil::AeronPump{},
                                                 sealed ? pl::PeerCryptoMode{pl::PeerFrameCrypto(1, 0)} : pl::PeerCryptoMode{pl::PeerPlain{}});
        if (sealed) {
            peer.emplace(2, 0);
            EXPECT_TRUE(peer->installLink(1, linkFor(false, 7), pl::steadyNowNs()));
            EXPECT_TRUE(ingress->installPeerLink(2, linkFor(true, 7)));
        }
        const int64_t id = aeron->addExclusivePublication(chan + "|term-length=1m", kStream);
        const auto deadline = std::chrono::steady_clock::now() + 10s;
        while (!(pub = aeron->findExclusivePublication(id)) && std::chrono::steady_clock::now() < deadline) std::this_thread::sleep_for(1ms);
        if (!pub) throw std::runtime_error("publication not resolved");
        while (!pub->isConnected() && std::chrono::steady_clock::now() < deadline) { ingress->poll(10); std::this_thread::sleep_for(1ms); }
        if (!pub->isConnected()) throw std::runtime_error("publication not connected");
    }
    ~Rig() { spdlog::set_default_logger(prevLogger); }

    /// Offer one frame (plaintext of `len` bytes; sealed by the peer in sealed mode).
    void send(std::size_t len) {
        std::vector<uint8_t> plain(len, 0x5A), wire;
        if (peer) {
            wire.resize(len + pl::kPeerFrameOverhead);
            const std::size_t n = peer->seal(1, plain.data(), len, wire.data(), wire.size());
            ASSERT_EQ(n, wire.size());
        } else {
            wire = std::move(plain);
        }
        aeron::concurrent::AtomicBuffer buf(wire.data(), static_cast<aeron::util::index_t>(wire.size()));
        const auto deadline = std::chrono::steady_clock::now() + 10s;
        while (pub->offer(buf, 0, static_cast<aeron::util::index_t>(wire.size())) < 0) {
            ASSERT_LT(std::chrono::steady_clock::now(), deadline) << "offer back-pressured for 10 s";
            ingress->poll(10);
            drainRing();
        }
    }
    /// Poll until `done()` (bounded); every frame that reached the ring is counted in `delivered`.
    template <class Pred> bool pollUntil(Pred&& done) {
        for (int i = 0; i < 200'000; ++i) {
            ingress->poll(10);
            drainRing();
            if (done()) return true;
            if ((i & 63) == 63) std::this_thread::sleep_for(100us);
        }
        return false;
    }
    void drainRing() {
        ring.drain([this](int32_t, aeron::concurrent::AtomicBuffer&, aeron::util::index_t, aeron::util::index_t len) {
            ++delivered; lastLen = static_cast<std::size_t>(len);
        }, 64);
    }
    [[nodiscard]] std::size_t oversizeLines() const {
        std::size_t n = 0;
        for (const auto& l : logSink->last_formatted()) if (l.find("exceeds the") != std::string::npos) ++n;
        return n;
    }
    uint64_t    delivered = 0;
    std::size_t lastLen = 0;
};

void runOversize(bool sealed, std::size_t oversizeLen) {
    Rig r(sealed);
    const std::size_t max = r.ring.maxMsgLength();
    ASSERT_EQ(r.ingress->maxFrameLength(), max);
    ASSERT_EQ(max, kMaxConsensusFrameBytes);
    ASSERT_GT(oversizeLen, max);

    // One oversized frame: dropped and counted, never thrown; the ring stays untouched.
    r.send(oversizeLen);
    ASSERT_NO_THROW({ ASSERT_TRUE(r.pollUntil([&] { return r.ingress->oversizeDrops() == 1; })); });
    EXPECT_EQ(r.delivered, 0u);
    EXPECT_EQ(r.oversizeLines(), 1u);

    // The boundary: a frame of exactly the ring maximum still arrives; the ingress keeps working.
    r.send(max);
    ASSERT_NO_THROW({ ASSERT_TRUE(r.pollUntil([&] { return r.delivered == 1; })); });
    EXPECT_EQ(r.lastLen, max);
    EXPECT_EQ(r.ingress->oversizeDrops(), 1u);

    // A flood: every frame is counted, the log line is rate-limited (one per 1024 drops).
    constexpr int kFlood = 2100;
    for (int i = 0; i < kFlood; ++i) r.send(max + 1);
    ASSERT_NO_THROW({ ASSERT_TRUE(r.pollUntil([&] { return r.ingress->oversizeDrops() == 1u + kFlood; })); });
    EXPECT_EQ(r.delivered, 1u);
    EXPECT_EQ(r.oversizeLines(), 3u) << "drops 0, 1024 and 2048 log; the rest are only counted";
    if (sealed) EXPECT_EQ(r.ingress->peerFrameDrops(), 0u) << "an oversized frame is refused before any cipher work";
}

} // namespace

TEST(AeronIngressRegression, AnOversizedInboundFrameIsDroppedCountedAndLoggedNeverThrown) {
    runOversize(/*sealed=*/false, kMaxConsensusFrameBytes + 1);
}

TEST(AeronIngressRegression, AnOversizedSealedFrameIsDroppedCountedAndLoggedNeverThrown) {
    runOversize(/*sealed=*/true, 12 * 1024);   // opened fine before #254, then threw at the ring
}

TEST(AeronIngressRegression, TheSealedSenderBoundIsTheIngressRingMaximum) {
    EXPECT_EQ(UdpMessageSink::kMaxSealedPayload, kMaxConsensusFrameBytes);
    IngressRing ring{kDefaultIngressCapacity};
    EXPECT_EQ(ring.maxMsgLength(), kMaxConsensusFrameBytes);
}
