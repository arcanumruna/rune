// tests/transport/AeronTransportTest.cpp
//
// CC-3 Acceptance tests — aeron:ipc loopback echo for AeronTransport.
//
// All tests use a unique driver dir (getpid() + atomic counter) to avoid
// collisions when tests run in parallel or the previous run left stale files.
//
// Design rules enforced here:
//   - Bounded pump loops: hard cap (kPumpMax) + ASSERT — no busy-spin.
//   - sleep_for between polls (NO busy-spin).
//   - The loop-back trick: ingress channel == egress channel + same stream id.
//     A message offer'd on the ExclusivePublication is received on the
//     Subscription (IPC loopback via the same stream).
//   - Multi-frame: payload > Aeron default MTU (4 KiB) forces fragmentation;
//     FragmentAssembler reassembles it back.

#include <rune/client/transport/AeronTransport.h>

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <stdexcept>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

using namespace std::chrono_literals;
using Transport = rune::client::transport::AeronTransport;
using Config    = rune::client::transport::TransportConfig;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

namespace {

static std::atomic<int> g_dirCounter{0};

/// Unique driver dir: /tmp/cc3_test_<pid>_<counter>
static std::string uniqueDataDir() {
    const int c = g_dirCounter.fetch_add(1, std::memory_order_relaxed);
    return std::string("/tmp/cc3_test_") +
           std::to_string(static_cast<long>(::getpid())) + "_" +
           std::to_string(c);
}

/// Build a loopback config: ingress == egress channel+stream.
/// Both pub and sub land on the same IPC stream, so offer() → poll() loops back.
static Config makeLoopbackConfig(int32_t streamId = 1000) {
    Config cfg;
    cfg.ingressChannel  = "aeron:ipc";
    cfg.ingressStreamId = streamId;
    cfg.egressChannel   = "aeron:ipc";
    cfg.egressStreamId  = streamId;
    cfg.dataDir         = uniqueDataDir();
    return cfg;
}

// Pump constants — hard cap to avoid infinite loops; ASSERT on timeout.
static constexpr int  kConnectPumpMax = 2'000; // iterations to await connection
static constexpr int  kPollPumpMax    = 2'000; // iterations to await echo

static constexpr auto kPumpSleep = 2ms;        // sleep between polls (NO busy-spin)

/// Bounded pump until ingressConnected() && egressConnected().
static void awaitConnected(Transport& t) {
    for (int i = 0; i < kConnectPumpMax; ++i) {
        if (t.ingressConnected() && t.egressConnected()) return;
        std::this_thread::sleep_for(kPumpSleep);
    }
    ASSERT_TRUE(t.ingressConnected() && t.egressConnected())
        << "Timed out waiting for ingress+egress connection";
}

/// Bounded offer loop — returns true when accepted.
static bool boundedOffer(Transport& t, const uint8_t* buf, std::size_t len) {
    for (int i = 0; i < kPollPumpMax; ++i) {
        if (t.offer(buf, len)) return true;
        std::this_thread::sleep_for(kPumpSleep);
    }
    return false;
}

} // namespace

// ---------------------------------------------------------------------------
// Test: IpcLoopbackEcho — known byte pattern (NUL + 0xFF)
// ---------------------------------------------------------------------------
TEST(AeronTransport, IpcLoopbackEcho) {
    Transport t{makeLoopbackConfig()};
    t.start();

    awaitConnected(t);
    ASSERT_TRUE(t.ingressConnected()) << "Ingress not connected";
    ASSERT_TRUE(t.egressConnected())  << "Egress not connected";

    // Payload with NUL and 0xFF as required by the brief.
    const uint8_t kPayload[] = {0xDE, 0xAD, 0xBE, 0xEF, 0x00, 0xFF, 0x42, 0x01};
    constexpr std::size_t kPayloadLen = sizeof(kPayload);

    ASSERT_TRUE(boundedOffer(t, kPayload, kPayloadLen))
        << "offer() timed out — never accepted";

    // Poll until handler fires.
    std::vector<uint8_t> received;
    bool fired = false;
    for (int i = 0; i < kPollPumpMax && !fired; ++i) {
        t.poll([&](const uint8_t* buf, std::size_t len) {
            received.assign(buf, buf + len);
            fired = true;
        }, 10);
        if (!fired) std::this_thread::sleep_for(kPumpSleep);
    }

    ASSERT_TRUE(fired) << "poll() handler never called — echo timed out";
    ASSERT_EQ(received.size(), kPayloadLen);
    EXPECT_EQ(std::memcmp(received.data(), kPayload, kPayloadLen), 0)
        << "Received bytes differ from sent bytes";

    t.stop();
}

// ---------------------------------------------------------------------------
// Test: IpcLoopbackEchoEmpty — empty payload
// ---------------------------------------------------------------------------
TEST(AeronTransport, IpcLoopbackEchoEmpty) {
    Transport t{makeLoopbackConfig(1001)};
    t.start();

    awaitConnected(t);

    // Empty payload.
    ASSERT_TRUE(boundedOffer(t, nullptr, 0))
        << "Empty offer() timed out";

    std::vector<uint8_t> received;
    bool fired = false;
    for (int i = 0; i < kPollPumpMax && !fired; ++i) {
        t.poll([&](const uint8_t* buf, std::size_t len) {
            if (buf && len > 0) {
                received.assign(buf, buf + len);
            }
            fired = true;
        }, 10);
        if (!fired) std::this_thread::sleep_for(kPumpSleep);
    }

    ASSERT_TRUE(fired) << "Empty-payload echo timed out";
    EXPECT_EQ(received.size(), 0u);

    t.stop();
}

// ---------------------------------------------------------------------------
// Test: IpcLoopbackMultiFrame — payload > MTU, exercises FragmentAssembler
// ---------------------------------------------------------------------------
TEST(AeronTransport, IpcLoopbackMultiFrame) {
    Transport t{makeLoopbackConfig(1002)};
    t.start();

    awaitConnected(t);

    // Build a payload larger than the default Aeron MTU (1 408 bytes for IPC).
    // We use 8 KiB to guarantee fragmentation across multiple frames.
    constexpr std::size_t kBigLen = 8 * 1024;
    std::vector<uint8_t> bigPayload(kBigLen);
    for (std::size_t i = 0; i < kBigLen; ++i) {
        bigPayload[i] = static_cast<uint8_t>(i & 0xFF);
    }
    // Embed a NUL and 0xFF for good measure.
    bigPayload[0]          = 0x00;
    bigPayload[kBigLen - 1] = 0xFF;

    ASSERT_TRUE(boundedOffer(t, bigPayload.data(), kBigLen))
        << "Multi-frame offer() timed out";

    // Poll until the FULLY reassembled frame arrives.
    std::vector<uint8_t> received;
    bool fired = false;
    for (int i = 0; i < kPollPumpMax && !fired; ++i) {
        t.poll([&](const uint8_t* buf, std::size_t len) {
            received.assign(buf, buf + len);
            fired = true;
        }, 10);
        if (!fired) std::this_thread::sleep_for(kPumpSleep);
    }

    ASSERT_TRUE(fired)
        << "Multi-frame poll() handler never called — FragmentAssembler did not reassemble";
    ASSERT_EQ(received.size(), kBigLen)
        << "Reassembled size mismatch";
    EXPECT_EQ(std::memcmp(received.data(), bigPayload.data(), kBigLen), 0)
        << "Reassembled bytes differ from original payload";

    t.stop();
}

// ---------------------------------------------------------------------------
// Test: StopIdempotent — second stop() is a no-op
// ---------------------------------------------------------------------------
TEST(AeronTransport, StopIdempotent) {
    Transport t{makeLoopbackConfig(1003)};
    t.start();
    awaitConnected(t);

    t.stop();
    // Second stop() must not crash.
    EXPECT_NO_THROW(t.stop());
}

// ---------------------------------------------------------------------------
// Test: DtorAfterStartWithoutStop — dtor calls stop() implicitly
// ---------------------------------------------------------------------------
TEST(AeronTransport, DtorAfterStartWithoutStop) {
    EXPECT_NO_THROW({
        Transport t{makeLoopbackConfig(1004)};
        t.start();
        awaitConnected(t);
        // dtor called here without explicit stop().
    });
}

// ---------------------------------------------------------------------------
// Regression (spec §3.6): a driver is driven by exactly one thread.  start()
// used to pump aeron_driver_main_do_work() from the caller thread while the
// driver thread was already running the same duty cycle.
// ---------------------------------------------------------------------------
TEST(AeronTransportRegression, StartupPumpsDriverFromOneThreadOnlyRegression) {
    Transport t{makeLoopbackConfig(1010)};
    t.start();
    EXPECT_EQ(t.callerPumpViolations(), 0)
        << "start() pumped the driver from the caller thread while driverThread_ ran";
    awaitConnected(t);
    t.stop();
    EXPECT_EQ(t.callerPumpViolations(), 0)
        << "stop() may drain the driver only after joining driverThread_";
}

// ---------------------------------------------------------------------------
// Task 3: explicit transport configuration — MTU reaches the driver.
// ---------------------------------------------------------------------------

// The configured MTU reaches the driver: a UDP publication's max payload is
// mtuLength - DATA_HEADER_LENGTH (32).  Aeron's default would give 1408 - 32.
TEST(AeronTransport, UdpPublicationHonoursConfiguredMtu) {
    Config cfg = makeLoopbackConfig(1020);
    cfg.ingressChannel = "aeron:udp?endpoint=127.0.0.1:24711";  // no subscriber needed
    cfg.mtuLength      = 8192;
    Transport t{cfg};
    t.start();
    EXPECT_EQ(t.ingressMaxPayloadLength(), 8192 - 32);
    t.stop();
}

TEST(AeronTransport, InvalidMtuFailsStartWithClearError) {
    Config cfg = makeLoopbackConfig(1021);
    cfg.mtuLength = 1000;  // not a multiple of 32
    Transport t{cfg};
    try {
        t.start();
        FAIL() << "start() must reject an invalid MTU";
    } catch (const std::invalid_argument& e) {
        EXPECT_NE(std::string(e.what()).find("multiple of 32"), std::string::npos);
    }
}
