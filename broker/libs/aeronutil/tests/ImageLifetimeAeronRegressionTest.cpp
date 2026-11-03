// ImageLifetimeAeronRegressionTest.cpp — real-Aeron regression: Aeron 1.51's C++ wrapper drops an image's list
// reference in the unavailable-image callback (Image(sub, img) retains only images still in the list, ~Image() always
// releases; fixed upstream by aeron-io/aeron 2f9e9e49f1, #2226).  The threaded client conductor then deletes the image
// at its next lingering check even while a poll that started on the older list still iterates it: aeron_image_poll
// reads a freed image (seen as SEGFAULTs in ShardTestClient::poll_ and ClientGateway::doWork under load).
// Deterministic: drain 16 images, close their publications (10 ms linger), stall inside a handler of an earlier image
// until they all went unavailable and the lingering check ran, then calloc a spread of sizes - a wrongly freed image's
// chunk comes back zeroed and the rest of the poll reads a NULL subscriber position.
#include <rune/node/EmbeddedDriver.h>

#include <Aeron.h>
#include <Context.h>
#include <ExclusivePublication.h>
#include <Subscription.h>
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <memory>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

using namespace std::chrono_literals;
namespace fs = std::filesystem;

namespace {
constexpr int kStream = 1002;
constexpr int kImages = 16;
constexpr int kMaxSpins = 20000;

TEST(ImageLifetimeAeronRegression, UnavailableImageIsNotFreedUnderAnInFlightPoll) {
    const fs::path root = fs::temp_directory_path() / ("rune-imagelife-" + std::to_string(::getpid()));
    fs::create_directories(root);
    rune::node::EmbeddedDriverConfig cfg;
    cfg.dir              = (root / "aeron").string();
    cfg.termBufferBytes  = 64 * 1024;
    cfg.termBufferSparse = true;
    cfg.udpGso           = false;
    cfg.epollPoll        = false;
    auto driver = std::make_unique<rune::node::EmbeddedDriver>(std::move(cfg), rune::node::DriverMode::Threaded);

    static std::atomic<int> gone{0};
    aeron::Context sc;
    sc.aeronDir(driver->dir());
    sc.unavailableImageHandler([](aeron::Image&) { gone.fetch_add(1); });
    auto subClient = aeron::Aeron::connect(sc);   // threaded conductor: deletes images off the polling thread
    aeron::Context pc;
    pc.aeronDir(driver->dir());
    auto pubClient = aeron::Aeron::connect(pc);   // its own client, so the subscriber's log mappings are not shared

    const int64_t sid = subClient->addSubscription("aeron:ipc", kStream);
    std::shared_ptr<aeron::Subscription> sub;
    for (int i = 0; i < kMaxSpins && !sub; ++i) { sub = subClient->findSubscription(sid); std::this_thread::sleep_for(1ms); }
    ASSERT_TRUE(sub);
    auto addPub = [&]() -> std::shared_ptr<aeron::ExclusivePublication> {
        const int64_t id = pubClient->addExclusivePublication("aeron:ipc?linger=10ms", kStream);
        std::shared_ptr<aeron::ExclusivePublication> p;
        for (int i = 0; i < kMaxSpins && !(p && p->isConnected()); ++i) {
            if (!p) p = pubClient->findExclusivePublication(id);
            std::this_thread::sleep_for(100us);
        }
        return p;
    };
    uint8_t msg[64] = {};
    aeron::concurrent::AtomicBuffer ab(msg, sizeof(msg));
    auto offer = [&](aeron::ExclusivePublication& p) {
        for (int i = 0; i < kMaxSpins && p.offer(ab, 0, sizeof(msg)) < 0; ++i) std::this_thread::sleep_for(100us);
    };
    auto drain = [&](std::chrono::milliseconds d) {
        for (const auto end = std::chrono::steady_clock::now() + d; std::chrono::steady_clock::now() < end;)
            sub->poll([](aeron::concurrent::AtomicBuffer&, aeron::util::index_t, aeron::util::index_t, aeron::Header&) {}, 64);
    };

    std::vector<void*> reused;
    for (int round = 0; round < 2; ++round) {
        auto a = addPub();
        ASSERT_TRUE(a && a->isConnected());
        std::vector<std::shared_ptr<aeron::ExclusivePublication>> xs;
        for (int i = 0; i < kImages; ++i) {
            xs.push_back(addPub());
            ASSERT_TRUE(xs.back() && xs.back()->isConnected());
            offer(*xs.back());
        }
        drain(500ms);
        const int before = gone.load();
        xs.clear();   // closed and drained: each image goes unavailable after the 10 ms linger
        offer(*a);
        const int32_t aSession = a->sessionId();
        bool stalled = false;
        for (int i = 0; i < kMaxSpins && !stalled; ++i)
            sub->poll([&](aeron::concurrent::AtomicBuffer&, aeron::util::index_t, aeron::util::index_t, aeron::Header& h) {
                if (stalled || h.sessionId() != aSession) return;
                stalled = true;
                for (const auto end = std::chrono::steady_clock::now() + 5s;
                     std::chrono::steady_clock::now() < end && gone.load() < before + kImages;)
                    std::this_thread::sleep_for(1ms);
                std::this_thread::sleep_for(1500ms);   // the conductor's lingering check runs meanwhile
                for (std::size_t size = 64; size <= 1024; size += 16)
                    for (int k = 0; k < 64; ++k) reused.push_back(std::calloc(1, size));
            }, 64);
        ASSERT_TRUE(stalled);
        EXPECT_EQ(gone.load() - before, kImages) << "the closed images never went unavailable during the stall";
        drain(200ms);
    }
    for (void* p : reused) std::free(p);
    sub.reset();
    subClient.reset();
    pubClient.reset();
    driver.reset();
    std::error_code ec;
    fs::remove_all(root, ec);
}
} // namespace
