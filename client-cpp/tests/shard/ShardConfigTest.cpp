// tests/shard/ShardConfigTest.cpp — T15a: per-shard configuration, AUTO shard
// count, core assignment, D1 (driver on the duty thread) and the shard driver
// timeouts (design §2, §2a, §6 D1/I4/R2).
//
// Rules (design D7): no std::atomic, no sleeps; every drive loop has a hard
// iteration cap.  The AUTO / pinning tests are pure: the CPU set and the cgroup
// quota are injected, so nothing depends on the test machine except the two
// tests that check the platform probes themselves.
#include "../support/InvokerGateway.h"

#include <rune/client/Client.h>
#include <rune/client/shard/ShardConfig.h>
#include <rune/client/transport/AeronTransport.h>

#include <gtest/gtest.h>

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <future>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

#if defined(__APPLE__)
#include <mach/mach.h>
#include <sys/sysctl.h>
#elif defined(__linux__)
#include <sched.h>
#endif

namespace rc = rune::client;
namespace rs = rune::client::shard;
namespace rt = rune::client::transport;
using namespace std::chrono_literals;

namespace {

int g_ctr = 0;  // gtest runs tests on one thread

std::string uniqueDir(const char* prefix) {
    return std::string("/tmp/") + prefix + "_" + std::to_string(static_cast<long>(::getpid())) + "_" +
           std::to_string(g_ctr++);
}

rc::TopicSpec topic(const char* name, int32_t id, int32_t partitions) {
    rc::TopicSpec t;
    t.name           = name;
    t.topicId        = id;
    t.partitionCount = partitions;
    return t;
}

rs::CpuSet cpus(uint32_t n) {
    rs::CpuSet s;
    for (uint32_t i = 0; i < n; ++i) s.push_back(static_cast<int32_t>(i));
    return s;
}

rs::ShardGroupConfig autoGroup(std::vector<rc::TopicSpec> topics) {
    rs::ShardGroupConfig g;
    g.base.topics = std::move(topics);
    return g;  // shardCount defaults to AUTO
}

// OS thread count of this process (cold, test-only).
int processThreadCount() {
#if defined(__APPLE__)
    thread_act_array_t     list  = nullptr;
    mach_msg_type_number_t count = 0;
    if (task_threads(mach_task_self(), &list, &count) != KERN_SUCCESS) return -1;
    for (mach_msg_type_number_t i = 0; i < count; ++i) mach_port_deallocate(mach_task_self(), list[i]);
    vm_deallocate(mach_task_self(), reinterpret_cast<vm_address_t>(list), count * sizeof(thread_act_t));
    return static_cast<int>(count);
#elif defined(__linux__)
    int n = 0;
    for ([[maybe_unused]] const auto& e : std::filesystem::directory_iterator("/proc/self/task")) ++n;
    return n;
#else
    return -1;
#endif
}

rs::ShardGroupConfig ipcGroup(int32_t in, int32_t eg, uint32_t count, rs::ShardMode mode) {
    rs::ShardGroupConfig g;
    g.base.gatewayChannel  = "aeron:ipc";
    g.base.gatewayStreamId = in;
    g.base.egressChannel   = "aeron:ipc";
    g.base.egressStreamId  = eg;
    g.base.dataDir         = uniqueDir("shard_cfg");
    g.base.topics.push_back(topic("t", 1, 8));
    g.shardCount = count;
    g.mode       = mode;
    return g;
}

} // namespace

// ---------------------------------------------------------------------------
// Per-shard config
// ---------------------------------------------------------------------------

TEST(ShardConfigRegression, DerivesDirPortCoreTermAndKeepsMtu1408) {
    rs::ShardGroupConfig g;
    g.base.dataDir       = "/tmp/x";
    g.base.egressChannel = "aeron:udp?endpoint=127.0.0.1:40400";
    g.shardCount         = 4;
    g.firstCore          = 2;

    const rc::ClientConfig c1 = rs::shardClientConfig(g, 1);
    EXPECT_EQ(c1.dataDir, "/tmp/x/shard-1");
    EXPECT_EQ(c1.egressChannel, "aeron:udp?endpoint=127.0.0.1:40401");
    EXPECT_EQ(c1.runnerMode, rc::RunnerMode::Thread);
    EXPECT_EQ(c1.runnerCore, 3);
    EXPECT_TRUE(c1.driverOnDutyThread) << "D1: the shard's driver runs on its duty thread";
    EXPECT_TRUE(c1.reconnectForeverAfterLive) << "R1: shard sessions never give up once Live";
    EXPECT_EQ(c1.termBufferBytes, 1u * 1024 * 1024) << "I5: 1 MiB shard term";
    EXPECT_EQ(c1.mtuBytes, rt::kAeronDefaultMtuLength) << "per-shard MTU stays 1408 (jumbo opt-in)";
    EXPECT_EQ(c1.mtuBytes, 1408u);
    EXPECT_EQ(c1.aeronClientLivenessTimeoutNs, rt::kMaxClientLivenessTimeoutNs);

    g.base.mtuBytes = rt::kJumboMtuLength;  // explicit opt-in is carried through
    EXPECT_EQ(rs::shardClientConfig(g, 0).mtuBytes, rt::kJumboMtuLength);

    // The layout overload takes the resolved count + core (AUTO / pin = true).
    const rc::ClientConfig c2 = rs::shardClientConfig(g, 2, 4, 6);
    EXPECT_EQ(c2.runnerCore, 6);
    EXPECT_EQ(c2.egressChannel, "aeron:udp?endpoint=127.0.0.1:40402");

    g.mode = rs::ShardMode::Invoker;
    const rc::ClientConfig c3 = rs::shardClientConfig(g, 3);
    EXPECT_EQ(c3.runnerMode, rc::RunnerMode::Invoker);
    EXPECT_EQ(c3.runnerCore, -1) << "Invoker: the app thread is the owner; nothing to pin";
    EXPECT_TRUE(c3.driverOnDutyThread);
    EXPECT_THROW((void)rs::shardClientConfig(g, 4), std::invalid_argument) << "index out of range";

    // M6: Invoker shards batch at the end of the pass (lingerUs = invokerLingerUs = 0).
    g.producer.lingerUs = 5000;
    EXPECT_EQ(rs::shardProducerConfig(g).lingerUs, 0);
    g.invokerLingerUs = 250;
    EXPECT_EQ(rs::shardProducerConfig(g).lingerUs, 250);
    g.mode = rs::ShardMode::Agent;
    EXPECT_EQ(rs::shardProducerConfig(g).lingerUs, 5000);

    // The single-index overload needs an explicit count: AUTO resolves first.
    g.shardCount = rs::kAutoShards;
    EXPECT_THROW((void)rs::shardClientConfig(g, 0), std::invalid_argument);
}

TEST(ShardConfigRegression, DerivedPortCollisionRejectedBeforeAnyDriverStarts) {
    rs::ShardGroupConfig g;
    g.base.dataDir        = uniqueDir("shard_collide");
    g.base.gatewayChannel = "aeron:udp?endpoint=127.0.0.1:40402";
    g.base.egressChannel  = "aeron:udp?endpoint=127.0.0.1:40400";
    g.shardCount          = 4;
    try {
        rs::validateShardGroupConfig(g, 4);
        FAIL() << "shard 2's egress port 40402 is the gateway port";
    } catch (const std::invalid_argument& e) {
        EXPECT_NE(std::string(e.what()).find("shard 2"), std::string::npos) << e.what();
        EXPECT_NE(std::string(e.what()).find("40402"), std::string::npos) << e.what();
    }
    EXPECT_FALSE(std::filesystem::exists(g.base.dataDir)) << "nothing was started";

    // localhost and 127.0.0.1 are the same loopback host.
    g.base.gatewayChannel = "aeron:udp?endpoint=localhost:40403";
    EXPECT_THROW(rs::validateShardGroupConfig(g, 4), std::invalid_argument);
    EXPECT_NO_THROW(rs::validateShardGroupConfig(g, 3)) << "N = 3 uses 40400..40402 only";

    // A different host's port is not a collision.
    g.base.gatewayChannel = "aeron:udp?endpoint=10.1.2.3:40402";
    EXPECT_NO_THROW(rs::validateShardGroupConfig(g, 4));

    // base + N - 1 must fit in a port.
    g.base.egressChannel = "aeron:udp?endpoint=127.0.0.1:65534";
    EXPECT_NO_THROW(rs::validateShardGroupConfig(g, 2));
    EXPECT_THROW(rs::validateShardGroupConfig(g, 3), std::invalid_argument);

    // IPC egress has no ports; N must still be >= 1.
    g.base.egressChannel = "aeron:ipc";
    EXPECT_NO_THROW(rs::validateShardGroupConfig(g, 64));
    EXPECT_THROW(rs::validateShardGroupConfig(g, 0), std::invalid_argument);
}

TEST(ShardConfigRegression, CoresOutsideAffinityMaskRejected) {
    rs::ShardGroupConfig g;
    g.shardCount = 3;
    g.firstCore  = 2;
    // Pure: injected mask {0,1,2,3}, pinning supported: cores 2,3,4 — 4 is outside.
    try {
        (void)rs::assignShardCores(g, 3, {0, 1, 2, 3}, /*pinningSupported=*/true);
        FAIL() << "core 4 is not in the affinity mask";
    } catch (const std::invalid_argument& e) {
        EXPECT_NE(std::string(e.what()).find("0,1,2,3"), std::string::npos)
            << "lists the allowed cores: " << e.what();
    }
    EXPECT_EQ(rs::assignShardCores(g, 2, {0, 1, 2, 3}, true), (std::vector<int32_t>{2, 3}));
    // A hole in the mask is rejected too.
    EXPECT_THROW((void)rs::assignShardCores(g, 3, {0, 2, 4, 6}, true), std::invalid_argument);

    // The real platform: on Linux one past the highest allowed core is rejected;
    // where pinning is unsupported (macOS) any firstCore >= 0 is rejected.
    const rs::CpuSet allowed = rs::platformAllowedCpus();
    ASSERT_FALSE(allowed.empty());
    g.shardCount = 1;
    if (rs::platformPinningSupported()) {
        g.firstCore = allowed.back() + 1;
        EXPECT_THROW((void)rs::assignShardCores(g, 1, allowed, true), std::invalid_argument);
        g.firstCore = allowed.front();
        EXPECT_EQ(rs::assignShardCores(g, 1, allowed, true), (std::vector<int32_t>{allowed.front()}));
    } else {
        g.firstCore = 0;
        EXPECT_THROW((void)rs::assignShardCores(g, 1, allowed, false), std::invalid_argument);
    }
}

TEST(ShardConfigRegression, MaxFrameAboveTermEighthRejected) {
    rs::ShardGroupConfig g;
    g.shardCount = 1;
    ASSERT_EQ(g.shardTermBufferBytes, 1u * 1024 * 1024);
    ASSERT_EQ(g.producer.maxFrameBytes, 65536);
    EXPECT_NO_THROW(rs::validateShardGroupConfig(g, 1)) << "1 MiB / 8 = 128 KiB >= 64 KiB";

    g.shardTermBufferBytes = 512u * 1024;  // term/8 = 64 KiB == maxFrameBytes: allowed
    EXPECT_NO_THROW(rs::validateShardGroupConfig(g, 1));

    g.shardTermBufferBytes = 256u * 1024;  // term/8 = 32 KiB < 64 KiB
    try {
        rs::validateShardGroupConfig(g, 1);
        FAIL() << "maxFrameBytes > term/8 must be rejected";
    } catch (const std::invalid_argument& e) {
        EXPECT_NE(std::string(e.what()).find("maxFrameBytes"), std::string::npos) << e.what();
    }
    g.producer.maxFrameBytes = 32 * 1024;
    EXPECT_NO_THROW(rs::validateShardGroupConfig(g, 1));
}

// ---------------------------------------------------------------------------
// AUTO shard count (pure: injected CpuSet + quota)
// ---------------------------------------------------------------------------

TEST(ShardCountAutoRegression, EqualsAffinityMaskSize) {
    const auto g = autoGroup({topic("a", 1, 64)});
    ASSERT_EQ(g.shardCount, rs::kAutoShards) << "AUTO is the default";
    const auto r = rs::resolveShardCount(g, {0, 2, 5, 7}, rs::kNoCpuQuota);
    EXPECT_EQ(r.count, 4u);
    EXPECT_NE(r.reason.find("AUTO"), std::string::npos) << r.reason;
}

TEST(ShardCountAutoRegression, SubtractsReservedCores) {
    auto g          = autoGroup({topic("a", 1, 64)});
    g.reservedCores = 3;
    EXPECT_EQ(rs::resolveShardCount(g, cpus(8), rs::kNoCpuQuota).count, 5u);
}

TEST(ShardCountAutoRegression, CappedAtMaxPartitionCount) {
    const auto g = autoGroup({topic("a", 1, 4), topic("b", 2, 6)});
    const auto r = rs::resolveShardCount(g, cpus(16), rs::kNoCpuQuota);
    EXPECT_EQ(r.count, 6u) << "max(partitionCount), not the sum (10)";
    // Every shard owns at least one partition of the widest topic.
    std::vector<int> owned(r.count, 0);
    for (const auto& t : g.base.topics)
        for (int32_t p = 0; p < t.partitionCount; ++p) ++owned[rs::shardFor(t.topicId, p, r.count)];
    for (uint32_t s = 0; s < r.count; ++s) EXPECT_GE(owned[s], 1) << "shard " << s << " idle";
}

TEST(ShardCountAutoRegression, ExplicitCountOverridesAuto) {
    auto g       = autoGroup({topic("a", 1, 64)});
    g.shardCount = 3;
    EXPECT_EQ(rs::resolveShardCount(g, cpus(16), rs::kNoCpuQuota).count, 3u);
    g.base.topics.clear();
    EXPECT_EQ(rs::resolveShardCount(g, cpus(16), 2).count, 3u) << "explicit wins over topics and quota";
}

TEST(ShardCountAutoRegression, NeverBelowOne) {
    auto g          = autoGroup({topic("a", 1, 64)});
    g.reservedCores = 4;
    EXPECT_EQ(rs::resolveShardCount(g, cpus(2), rs::kNoCpuQuota).count, 1u);
    g.reservedCores = 0;
    EXPECT_EQ(rs::resolveShardCount(g, cpus(1), rs::kNoCpuQuota).count, 1u);
    EXPECT_EQ(rs::resolveShardCount(g, {}, rs::kNoCpuQuota).count, 1u);
}

TEST(ShardCountAutoRegression, NoTopicsResolvesToOneAndLogs) {
    const auto g = autoGroup({});
    const auto r = rs::resolveShardCount(g, cpus(64), rs::kNoCpuQuota);
    EXPECT_EQ(r.count, 1u);
    EXPECT_NE(r.reason.find("no topics known at construction; AUTO → 1"), std::string::npos)
        << "the logged reason: " << r.reason;
}

TEST(ShardCountAutoRegression, CgroupQuotaBoundsAffinityMask) {
    const auto g = autoGroup({topic("a", 1, 64)});
    // quota 2.5 CPUs = ceil(250000 / 100000) = 3
    const int32_t quota = rs::quotaCpusFromCfs(250'000, 100'000);
    EXPECT_EQ(quota, 3);
    EXPECT_EQ(rs::resolveShardCount(g, cpus(16), quota).count, 3u);
    // "max" = unbounded
    EXPECT_EQ(rs::parseCgroupV2CpuMax("max 100000\n"), rs::kNoCpuQuota);
    EXPECT_EQ(rs::resolveShardCount(g, cpus(16), rs::parseCgroupV2CpuMax("max 100000")).count, 16u);
    EXPECT_EQ(rs::parseCgroupV2CpuMax("250000 100000\n"), 3);
    EXPECT_EQ(rs::parseCgroupV2CpuMax("200000 100000"), 2);
    EXPECT_EQ(rs::parseCgroupV2CpuMax("50000 100000"), 1) << "half a CPU still runs one shard";
    EXPECT_EQ(rs::parseCgroupV2CpuMax("garbage"), rs::kNoCpuQuota);
    EXPECT_EQ(rs::parseCgroupV2CpuMax(""), rs::kNoCpuQuota);
    EXPECT_EQ(rs::quotaCpusFromCfs(-1, 100'000), rs::kNoCpuQuota) << "cgroup v1: -1 = no quota";
    EXPECT_EQ(rs::quotaCpusFromCfs(100'000, 0), rs::kNoCpuQuota);
    // A quota larger than the mask does not raise N.
    EXPECT_EQ(rs::resolveShardCount(g, cpus(4), 32).count, 4u);
}

TEST(ShardCountAutoRegression, InvokerModeRejectsAuto) {
    auto g = autoGroup({topic("a", 1, 64)});
    g.mode = rs::ShardMode::Invoker;
    EXPECT_THROW((void)rs::resolveShardCount(g, cpus(16), rs::kNoCpuQuota), std::invalid_argument);
    g.shardCount = 2;
    EXPECT_EQ(rs::resolveShardCount(g, cpus(16), rs::kNoCpuQuota).count, 2u);
}

TEST(ShardPinningRegression, AutoPinsIthAllowedCoreSkippingReserved) {
    auto g          = autoGroup({topic("a", 1, 64)});
    g.reservedCores = 1;
    g.pin           = true;
    const rs::CpuSet mask{1, 3, 4, 6};
    const auto       n = rs::resolveShardCount(g, mask, rs::kNoCpuQuota).count;
    ASSERT_EQ(n, 3u);
    EXPECT_EQ(rs::assignShardCores(g, n, mask, /*pinningSupported=*/true),
              (std::vector<int32_t>{3, 4, 6}))
        << "the lowest reservedCores cores (1) are skipped";
    // More shards than the cores left after the reserved ones.
    g.shardCount = 4;
    EXPECT_THROW((void)rs::assignShardCores(g, 4, mask, true), std::invalid_argument);
    // No pinning requested: nobody is pinned.
    g.pin = false;
    EXPECT_EQ(rs::assignShardCores(g, 4, mask, true), (std::vector<int32_t>{-1, -1, -1, -1}));
}

TEST(ShardPinningRegression, PinOnUnsupportedPlatformRejected) {
    auto g = autoGroup({topic("a", 1, 64)});
    g.pin  = true;
    EXPECT_THROW((void)rs::assignShardCores(g, 2, cpus(4), /*pinningSupported=*/false),
                 std::invalid_argument)
        << "rejected rather than a silent no-pin";
    g.pin       = false;
    g.firstCore = 0;
    EXPECT_THROW((void)rs::assignShardCores(g, 2, cpus(4), false), std::invalid_argument);
    g.firstCore = -1;
    EXPECT_EQ(rs::assignShardCores(g, 2, cpus(4), false), (std::vector<int32_t>{-1, -1}));
#if defined(__APPLE__)
    EXPECT_FALSE(rs::platformPinningSupported()) << "macOS has no thread affinity API";
#elif defined(__linux__)
    EXPECT_TRUE(rs::platformPinningSupported());
#endif
}

TEST(ShardCountAutoRegression, PlatformCpuSetMatchesSchedGetaffinity) {
    const rs::CpuSet got = rs::platformAllowedCpus();
    ASSERT_FALSE(got.empty());
    for (std::size_t i = 1; i < got.size(); ++i) EXPECT_LT(got[i - 1], got[i]) << "ascending, unique";
#if defined(__linux__)
    cpu_set_t set;
    CPU_ZERO(&set);
    ASSERT_EQ(sched_getaffinity(0, sizeof(set), &set), 0);
    rs::CpuSet expect;
    for (int c = 0; c < CPU_SETSIZE; ++c)
        if (CPU_ISSET(c, &set)) expect.push_back(c);
    EXPECT_EQ(got, expect);
#elif defined(__APPLE__)
    int    active = 0;
    size_t len    = sizeof(active);
    ASSERT_EQ(sysctlbyname("hw.activecpu", &active, &len, nullptr, 0), 0);
    EXPECT_EQ(got, cpus(static_cast<uint32_t>(active)));
    EXPECT_EQ(rs::platformQuotaCpus(), rs::kNoCpuQuota) << "no cgroups on macOS";
#endif
}

// ---------------------------------------------------------------------------
// D1: the driver runs on the duty thread; shard driver timeouts (I4, R2)
// ---------------------------------------------------------------------------

TEST(ShardConfigRegression, ThreadRunnerWithDriverOnDutyThreadUsesInvokerDriver) {
    rc::ClientConfig c;
    c.runnerMode = rc::RunnerMode::Thread;
    EXPECT_EQ(rc::toAgentConfig(c).transport.driverMode, rt::DriverMode::Thread) << "plain default";
    c.driverOnDutyThread = true;
    EXPECT_EQ(rc::toAgentConfig(c).transport.driverMode, rt::DriverMode::Invoker)
        << "D1: DriverMode::Invoker under RunnerMode::Thread";
    c.runnerMode = rc::RunnerMode::Invoker;
    EXPECT_EQ(rc::toAgentConfig(c).transport.driverMode, rt::DriverMode::Invoker);
    c.driverOnDutyThread = false;
    EXPECT_EQ(rc::toAgentConfig(c).transport.driverMode, rt::DriverMode::Invoker);

    // Shard clients in both modes.
    rs::ShardGroupConfig g = ipcGroup(4000, 4001, 2, rs::ShardMode::Agent);
    EXPECT_EQ(rc::toAgentConfig(rs::shardClientConfig(g, 1)).transport.driverMode,
              rt::DriverMode::Invoker);
    EXPECT_TRUE(rc::toAgentConfig(rs::shardClientConfig(g, 1)).session.reconnectForeverAfterLive);
}

TEST(ShardConfigRegression, PlainClientDefaultUnchanged) {
    const rc::ClientConfig c;
    EXPECT_FALSE(c.driverOnDutyThread);
    EXPECT_FALSE(c.reconnectForeverAfterLive);
    EXPECT_EQ(c.runnerMode, rc::RunnerMode::Thread);
    EXPECT_EQ(c.termBufferBytes, 4u * 1024 * 1024);
    EXPECT_EQ(c.mtuBytes, 1408u);
    EXPECT_EQ(c.aeronClientLivenessTimeoutNs, 10'000'000'000LL);
    const auto a = rc::toAgentConfig(c);
    EXPECT_EQ(a.transport.driverMode, rt::DriverMode::Thread);
    EXPECT_EQ(a.transport.clientLivenessTimeoutNs, 10'000'000'000LL);
    EXPECT_FALSE(a.session.reconnectForeverAfterLive);
    EXPECT_EQ(rt::mediaDriverTimeoutMsFor(a.transport), rt::kAeronDefaultMediaDriverTimeoutMs);

    // A real plain Thread-mode Client still runs a driver thread and an Aeron
    // conductor thread besides its runner.
    rc::ClientConfig real;
    real.gatewayStreamId = 4010;
    real.egressStreamId  = 4011;
    real.dataDir         = uniqueDir("plain_client");
    const int before = processThreadCount();
    {
        rc::Client client(real);
        EXPECT_TRUE(client.testTransport().usesDriverThread());
        EXPECT_FALSE(client.testTransport().usesConductorInvoker());
        EXPECT_EQ(client.testTransport().aeronMediaDriverTimeoutMs(), rt::kAeronDefaultMediaDriverTimeoutMs);
        if (before > 0) {
            EXPECT_GE(processThreadCount() - before, 3) << "runner + driver + conductor";
        }
    }
}

TEST(ShardConfigRegression, DriverOnDutyThreadRunsNoDriverOrConductorThread) {
    const rs::ShardGroupConfig g = ipcGroup(4020, 4021, 1, rs::ShardMode::Agent);
    const rc::ClientConfig     c = rs::shardClientConfig(g, 0);
    ASSERT_EQ(c.runnerMode, rc::RunnerMode::Thread);
    ASSERT_TRUE(c.driverOnDutyThread);
    // Sanitizer runtimes (TSan) start a helper thread at the process's first
    // pthread_create and keep it.  Create (and keep alive across both counts)
    // one parked thread first, so `before` already includes any such helper and
    // the deltas measure only the Client's own threads (ctest runs this test
    // alone, so the Client would otherwise be the first pthread_create).  A
    // joined thread would not do: macOS may still list an exited thread.
    std::promise<void> release;
    std::thread parked([f = release.get_future()]() mutable { f.wait(); });
    const int before = processThreadCount();
    ASSERT_GT(before, 0);
    {
        rc::Client client(c);
        EXPECT_EQ(processThreadCount() - before, 1) << "only the runner: no driver, no conductor thread";
        EXPECT_FALSE(client.testTransport().usesDriverThread());
        EXPECT_TRUE(client.testTransport().usesConductorInvoker());
        client.close();  // joins the runner: the counter below has no concurrent writer
        EXPECT_EQ(client.testTransport().callerPumpViolations(), 0) << "T2: exactly one pumper";
    }
    EXPECT_EQ(processThreadCount(), before);
    release.set_value();
    parked.join();
}

TEST(ShardConfigRegression, DutyThreadLivenessDefaultsUnbounded) {
    for (const rs::ShardMode mode : {rs::ShardMode::Agent, rs::ShardMode::Invoker}) {
        SCOPED_TRACE(mode == rs::ShardMode::Agent ? "Agent" : "Invoker");
        rs::ShardGroupConfig g = ipcGroup(4030, 4031, 2, mode);
        ASSERT_EQ(g.aeronClientLivenessTimeoutNs, rt::kMaxClientLivenessTimeoutNs);
        const rc::ClientConfig c = rs::shardClientConfig(g, 1);
        EXPECT_EQ(c.aeronClientLivenessTimeoutNs, rt::kMaxClientLivenessTimeoutNs) << "1 h";
        EXPECT_EQ(rc::toAgentConfig(c).transport.clientLivenessTimeoutNs, rt::kMaxClientLivenessTimeoutNs);
        EXPECT_TRUE(rs::shardConfigWarnings(g, 2).empty());

        // Still overridable, with a warning.
        g.aeronClientLivenessTimeoutNs = 5'000'000'000LL;
        EXPECT_EQ(rs::shardClientConfig(g, 1).aeronClientLivenessTimeoutNs, 5'000'000'000LL);
        const auto w = rs::shardConfigWarnings(g, 2);
        ASSERT_EQ(w.size(), 1u);
        EXPECT_NE(w[0].find("aeronClientLivenessTimeoutNs"), std::string::npos) << w[0];
    }
    // BusySpin under AUTO without pinning warns (N runners would peg N cores).
    auto g          = autoGroup({topic("a", 1, 8)});
    g.base.idleMode = rc::IdleMode::BusySpin;
    const auto w    = rs::shardConfigWarnings(g, 8);
    ASSERT_EQ(w.size(), 1u);
    EXPECT_NE(w[0].find("BusySpin"), std::string::npos) << w[0];
    g.pin = true;
    EXPECT_TRUE(rs::shardConfigWarnings(g, 8).empty());
}

TEST(ShardConfigRegression, DutyThreadMediaDriverTimeoutEqualsLiveness) {
    // Pure: the timeout the transport applies to its aeron::Context.
    rt::TransportConfig tc;
    tc.driverMode              = rt::DriverMode::Invoker;
    tc.clientLivenessTimeoutNs = rt::kMaxClientLivenessTimeoutNs;
    EXPECT_EQ(rt::mediaDriverTimeoutMsFor(tc), rt::kMaxClientLivenessTimeoutNs / 1'000'000);
    tc.driverMode = rt::DriverMode::Thread;
    EXPECT_EQ(rt::mediaDriverTimeoutMsFor(tc), rt::kAeronDefaultMediaDriverTimeoutMs)
        << "separate driver thread: Aeron's default 10 s driver timeout, unchanged";

    // Real: the Context of a D1 shard client's Aeron client carries it.
    const rs::ShardGroupConfig g = ipcGroup(4040, 4041, 1, rs::ShardMode::Agent);
    rc::Client client(rs::shardClientConfig(g, 0));
    EXPECT_EQ(client.testTransport().aeronMediaDriverTimeoutMs(),
              rt::kMaxClientLivenessTimeoutNs / 1'000'000)
        << "R2: mediaDriverTimeout == liveness (1 h) when the driver shares the duty thread";
}

TEST(ShardConfigRegression, DutyCyclePumpsDriverBeforeConductor) {
    rt::TransportConfig tc;
    tc.ingressChannel  = "aeron:ipc";
    tc.ingressStreamId = 4050;
    tc.egressChannel   = "aeron:ipc";
    tc.egressStreamId  = 4050;
    tc.dataDir         = uniqueDir("pump_order");
    tc.driverMode      = rt::DriverMode::Invoker;
    rt::AeronTransport t{tc};
    t.start();

    struct Recorder {
        rt::PumpStep steps[64];
        int          n = 0;
    } rec;
    t.testSetPumpStepObserver(
        [](void* ctx, rt::PumpStep s) noexcept {
            auto* r = static_cast<Recorder*>(ctx);
            if (r->n < 64) r->steps[r->n++] = s;
        },
        &rec);
    for (int i = 0; i < 5; ++i) (void)t.dutyCycle();
    t.testSetPumpStepObserver(nullptr, nullptr);
    ASSERT_EQ(rec.n, 10);
    for (int i = 0; i < 10; i += 2) {
        EXPECT_EQ(rec.steps[i], rt::PumpStep::Driver) << "cycle " << i / 2;
        EXPECT_EQ(rec.steps[i + 1], rt::PumpStep::Conductor) << "cycle " << i / 2;
    }
    t.stop();
}

// TSan: the constructing thread pumps the driver + conductor during start(); the
// runner then owns them.  Start, runner start, and the first duty cycles that
// touch driver state (connect through the driver, metadata) must be race-free.
TEST(ShardConfigRegression, DriverHandoffStartThreadToRunnerIsRaceFree) {
    for (int iter = 0; iter < 3; ++iter) {
        SCOPED_TRACE(iter);
        rs::ShardGroupConfig g = ipcGroup(4060 + 2 * iter, 4061 + 2 * iter, 1, rs::ShardMode::Agent);
        g.base.idleMode        = rc::IdleMode::BusySpin;  // the runner reacts without parking
        const rc::ClientConfig c = rs::shardClientConfig(g, 0);
        rc::Client             client(c);
        // The gateway attaches to the shard's driver (pumped only by the runner)
        // and is destroyed before the client tears that driver down.
        auto gw = std::make_unique<test_support::InvokerGateway>(
            c.dataDir + "/aeron", c.gatewayChannel, c.gatewayStreamId, c.egressChannel,
            c.egressStreamId);
        constexpr int kCap = 5'000'000;
        auto pumpUntil = [&](auto&& pred) {
            for (int i = 0; i < kCap; ++i) {
                gw->pump();
                if (pred()) return true;
                std::this_thread::yield();  // give the runner the CPU on a small box
            }
            return false;
        };
        ASSERT_TRUE(pumpUntil([&] { return gw->ready(); }));
        auto f = client.connect();
        ASSERT_TRUE(pumpUntil([&] { return f.wait_for(0s) == std::future_status::ready; }));
        f.get();
        ASSERT_TRUE(pumpUntil([&] { return gw->metadataRequests > 0; }));
        gw.reset();
        client.close();
        EXPECT_EQ(client.testTransport().callerPumpViolations(), 0);
    }
}
