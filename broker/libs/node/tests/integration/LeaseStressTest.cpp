// LeaseStressTest.cpp - lease topics Task 18 (spec 16.4): the seeded model-checked lease stress.
//
//   LeaseStressRegression.SingleNodeSeededRegression/<seed>  leg S: ONE real authenticating shard broker, the injected lease clock,
//                                                            seeds 1..20, 3 000 steps each.
//   LeaseStressRegression.TrioSeededRegression/<seed>        leg T: THREE real brokers (replication factor 3, acks=all) on the real clock,
//                                                            seeds 1..20, 200 steps each, at most 3 leader kills per seed.
//
//   LeaseStressRegression.TransientSingleNodeRegression/<seed>  leg TS (lease topics Task 32, spec Appendix A): a TRANSIENT lease topic on a 256 KiB
//                                                            ring, ONE node, the injected clock: ring pressure (Churn), carry, full restarts with
//                                                            the rebirth fence; the invariants T1..T5 after every step.  Seeds 1..3.
//   LeaseStressRegression.TransientTrioRegression/<seed>     leg TT: the same on THREE nodes (real clock): leader kills keep every lease (T1),
//                                                            a full restart of all three fences grants (T4).  Seeds 1..2.
// RUNE_LEASE_STRESS_SEED=<n> runs only seed n (of any leg).
//
// Why two legs: the injected lease clock works on a single node only (LeaseTestClock.h: on several nodes the election stamps the view read
// lease on the real clock), so the 3 000-step run on simulated time is single-node and the multi-node run uses the real clock.
// The invariants I1..I8 and the model are the same in both (LeaseModel.h, LeaseStressDriver.h).
#include "support/LeaseBrokerRig.h"
#include "support/LeaseStressDriver.h"
#include "support/LeaseTrio.h"

#include <gtest/gtest.h>

#include <chrono>
#include <iostream>
#include <memory>
#include <optional>
#include <random>

using namespace rune::node;
using namespace rune::node::test;
using namespace rune::node::test::leasestress;
using namespace std::chrono_literals;
namespace rc = rune::client;
namespace pc = rune::protocol::client;

namespace {

// ------------------------------------------------------------------------------------------------ leg S: one node, injected clock

constexpr int64_t kTransientRingBytes = 256 * 1024;   ///< the smallest ring the leg uses: carry starts at half of it, the reserve is 64 KiB

/// Transient: a transient lease topic on a small ring (max_ttl 2 s), a node restart is a full restart (spec Appendix A).
template <bool Transient>
class SingleEnvT {
public:
    static constexpr bool        kInjected = true;
    static constexpr bool        kTransient = Transient;
    static constexpr const char* kLegName = Transient ? "transient-single" : "single-node";
    static constexpr int64_t     kI3SlackNs = 0;            ///< rho = 0: the clock is exact
    static constexpr int64_t     kI7SlackNs = kTickNs;      ///< one duty tick (the wheel's 1 ms tick)
    static constexpr int64_t     kTopicMaxTtlMs = Transient ? 2000 : 10'000;
    static constexpr int64_t     kRingBytes = kTransientRingBytes;

    explicit SingleEnvT(const std::string& tag)
        : rig_(tag, 60'000, 1000, 600'000,
               Transient ? std::function<void(NodeConfig&)>([](NodeConfig& c) { c.memoryLogRingBytes = static_cast<int32_t>(kTransientRingBytes); })
                         : std::function<void(NodeConfig&)>{}),
          nowNs_(1'000'000'000'000LL) {
        auto s = Transient ? lbr::spec(2000, 1000, 2000) : lbr::spec(60'000, 1000, 600'000);
        s.checkpoint = 1;
        s.checkpointIntervalMs = 1000;
        s.checkpointRate = 1000;
        rc::TopicOptions to;
        to.lease = s;
        to.config.cleanupPolicy = 1;
        const auto r = rig_.ua().defineTopicAndWait("st", 2, "claim", lbr::claimBlob(), Transient ? rc::Persistence::Transient : rc::Persistence::Disk, to, {}, 60s);
        if (r.errorCode != 0) throw std::runtime_error("define lease st: " + r.detail);
        topicId_ = r.topicId;
        rc::LeaseTopicConfig cfg;
        // wall time: a lease write slower than this is a hang, not a lease problem.  The transient leg keeps the default op timeout (ttl / 3):
        // a grant during the rebirth fence must fail at once with Throttled instead of being parked until a Timeout.
        cfg.opTimeoutMs = Transient ? 0 : 20'000;
        opsClient_ = connect_("a/ua", lbr::kUserPw);   // the operations client is ours: RejectAll ends its session for good, and it is then replaced
        ops_.emplace(opsClient_->openLeaseTopicAndWait<LeaseClaim>("st", cfg, 30s));
        obsClient_ = connect_("admin", lbr::kAdminPw);   // the observer: the lease view reads and the log tap
        reads_.emplace(obsClient_->openLeaseTopicAndWait<LeaseClaim>("a/st", {}, 30s));
        tap_ = std::make_unique<LogTap>(*obsClient_, topicId_, 2, false, Transient);
    }
    ~SingleEnvT() {
        tap_.reset();
        if (ops_) ops_->close();
        if (reads_) reads_->close();
        ops_.reset();
        if (opsClient_) opsClient_->close();
        if (obsClient_) obsClient_->close();
    }

    Lt&      ops() { return *ops_; }
    Lt&      reads() { return *reads_; }
    LogTap&  tap() { return *tap_; }
    int      partitions() const { return 2; }
    int64_t  nowNs() const { return Transient ? rig_.leaseNowNs() : nowNs_; }
    uint32_t ttlChoice(std::mt19937_64& rng) const {
        if constexpr (Transient) {
            static constexpr uint32_t k[] = {1000, 1000, 1000, 1500, 2000};
            return k[rng() % (sizeof k / sizeof k[0])];
        } else {
            static constexpr uint32_t k[] = {1000, 1000, 1000, 1500, 2000, 3000, 5000, 10'000};
            return k[rng() % (sizeof k / sizeof k[0])];
        }
    }
    void advance(int64_t ns) {
        if constexpr (Transient) {
            rig_.advance(std::chrono::milliseconds((ns + 999'999) / 1'000'000));   // the rig moves whole ms: round up, the driver re-reads nowNs()
        } else {
            rig_.advance(std::chrono::milliseconds(ns / 1'000'000));
            nowNs_ += ns;
        }
    }

    // ---- transient mode
    rc::Client& watchClient() { return *obsClient_; }
    rc::Client& tapClient() { return *obsClient_; }
    int32_t     topicId() const { return topicId_; }
    /// A node restart empties the rings: a full restart.  Blocks until the clients are served again (bounded).
    void fullRestart() {
        rig_.restartKeepingClients();
        const auto end = std::chrono::steady_clock::now() + 90s;
        for (;;) {
            try {
                (void)reads_->getAndWait(lbr::claimRow("k00"), -1, 5s);
                const rc::LeaseResult r = ops_->keepAliveOnceAndWait(lbr::claimRow("k00"), 7, {}, 5s);
                if (r.errorCode == kNotFound) return;
            } catch (const std::exception&) {}
            if (std::chrono::steady_clock::now() >= end) throw std::runtime_error("the clients never recovered after the full restart");
            std::this_thread::sleep_for(20ms);
        }
    }
    struct RtSnap { bool has = false; uint8_t fence = 0; int64_t deadlineNs = 0; uint64_t carried = 0; uint64_t used = 0, cap = 0, live = 0; };
    RtSnap runtime(int p) {
        BrokerNode& n = rig_.broker().node();
        const int32_t topic = topicId_;
        for (int32_t w = 0; w < n.workerCountForTest(); ++w) {
            const RtSnap s = n.onWorkerThreadForTest(w, [topic, p](Worker& wk) {
                RtSnap r;
                if (const auto* rep = wk.findOwnedReplicaForTest(topic, p); rep != nullptr && rep->memoryLog() != nullptr) {
                    r.used = rep->memoryLog()->bytesFrom(0);
                    r.cap  = rep->transientCapacityBytes();
                }
                r.live = wk.leaseTransientLiveBytesForTest(topic, p);
                const auto* rt = wk.leaseRuntimeForTest(topic, p);
                if (rt == nullptr) return r;
                r.has     = true;
                r.fence   = rt->fenceStateCode();
                r.carried = rt->stats().carried;
                if (const auto* f = std::get_if<rune::node::lease::LeaseRuntime::FenceFencing>(&rt->fenceState())) r.deadlineNs = f->deadlineNs;
                return r;
            });
            if (s.has) return s;
        }
        return {};
    }
    bool fenceOpen(int p) { const RtSnap s = runtime(p); return s.has && s.fence == 3; }
    /// The Fencing deadline of partition `p` after a restart (polls, bounded, until the leader decided it); 0 = open.
    int64_t fenceDeadlineNs(int p) {
        const auto end = std::chrono::steady_clock::now() + 30s;
        for (;;) {
            const RtSnap s = runtime(p);
            if (s.has && (s.fence == 2 || s.fence == 3)) return s.deadlineNs;
            if (std::chrono::steady_clock::now() >= end) return 0;
            std::this_thread::sleep_for(2ms);
        }
    }
    uint64_t carriedRecords() {
        uint64_t sum = 0;
        for (int p = 0; p < 2; ++p) { const RtSnap s = runtime(p); if (std::getenv("RUNE_LEASE_STRESS_DEBUG")) std::fprintf(stderr, "ring p%d used %llu live %llu cap %llu carried %llu\n", p, (unsigned long long)s.used, (unsigned long long)s.live, (unsigned long long)s.cap, (unsigned long long)s.carried); sum += s.carried; }
        return sum;
    }
    bool suspend(bool rejectAll) {
        return rig_.admin().suspendNamespace("a", rejectAll ? pc::NsSuspendMode::RejectAll : pc::NsSuspendMode::ReadOnly).has_value();
    }
    bool resume() { return rig_.admin().resumeNamespace("a").has_value(); }
    /// RejectAll ended the namespace's sessions and the client failed for good ("the broker rejected the session"): a new session, a new handle.
    bool recoverOps() {
        try {
            ops_->close();
            ops_.reset();
            opsClient_->close();
            opsClient_ = connect_("a/ua", lbr::kUserPw);
            rc::LeaseTopicConfig cfg;
            cfg.opTimeoutMs = Transient ? 0 : 20'000;   // see the constructor
            ops_.emplace(opsClient_->openLeaseTopicAndWait<LeaseClaim>("st", cfg, 30s));
            return true;
        } catch (const std::exception&) {
            return false;
        }
    }
    bool alterCheckpoint(bool on, uint32_t intervalMs) {
        namespace pcl = rune::protocol;
        const pc::ConfigChangeWire ch[] = {{static_cast<uint8_t>(pcl::ConfigKey::LeaseCheckpoint), on ? 1 : 0},
                                           {static_cast<uint8_t>(pcl::ConfigKey::LeaseCheckpointIntervalMs), intervalMs}};
        const auto a = opsClient_->alterTopicConfigAndWait("st", 0, ch, {}, 30s);
        return a.errorCode == 0;
    }
    /// Checkpoint(R) records the broker appended (its own count, to compare with what the tap saw).
    uint64_t checkpointsAppended() {
        BrokerNode& n = rig_.broker().node();
        uint64_t sum = 0;
        const int32_t topic = topicId_;
        for (int32_t w = 0; w < n.workerCountForTest(); ++w)
            for (int32_t p = 0; p < 2; ++p)
                sum += n.onWorkerThreadForTest(w, [topic, p](Worker& wk) -> uint64_t {
                    const auto* rt = wk.leaseRuntimeForTest(topic, p);
                    return rt != nullptr ? rt->stats().checkpointsAppended : 0;
                });
        return sum;
    }
    /// The judge's stale-broker rejections, summed over the workers and the two partitions.
    uint64_t staleBroker() {
        BrokerNode& n = rig_.broker().node();
        uint64_t sum = 0;
        const int32_t topic = topicId_;
        for (int32_t w = 0; w < n.workerCountForTest(); ++w)
            for (int32_t p = 0; p < 2; ++p)
                sum += n.onWorkerThreadForTest(w, [topic, p](Worker& wk) -> uint64_t {
                    const auto* s = wk.verdictStatsForTest(topic, p);
                    return s != nullptr ? s->leaseRejectedStaleBroker : 0;
                });
        return sum;
    }

private:
    /// A client with the low-latency idle strategy (the default Balanced one parks up to 1 ms per cycle, which dominates a 3 000-step run).
    std::unique_ptr<rc::Client> connect_(const std::string& user, const std::string& pw) {
        rc::ClientConfig c;
        c.idleMode        = rc::IdleMode::LowLatency;
        c.gatewayChannel  = rig_.broker().channel();
        c.gatewayStreamId = 1;
        c.egressChannel   = "aeron:udp?endpoint=127.0.0.1:" + std::to_string(rune::aeronutil::testing::allocFreeUdpPort());
        c.egressStreamId  = 2;
        c.dataDir         = rig_.broker().clientDir().string();
        c.username        = user;
        c.password        = pw;
        c.offerCrypto     = true;
        c.tlsTrustPem     = AuthBroker::tlsCaPem();
        auto cl = std::make_unique<rc::Client>(c);
        cl->connectAndWait(20s);
        return cl;
    }

    LeaseBrokerRig           rig_;
    std::unique_ptr<rc::Client> opsClient_, obsClient_;
    int64_t                  nowNs_;
    int32_t                  topicId_ = -1;
    std::optional<Lt>        ops_, reads_;
    std::unique_ptr<LogTap>  tap_;
};

using SingleEnv          = SingleEnvT<false>;
using TransientSingleEnv = SingleEnvT<true>;

// ------------------------------------------------------------------------------------------------ leg T: three nodes, real clock

/// Transient: a transient lease topic (max_ttl 1 s, the smallest) on a 256 KiB ring on every node; a full restart stops and starts all three.
template <bool Transient>
class TrioEnvT {
public:
    static constexpr bool        kInjected = false;
    static constexpr bool        kTransient = Transient;
    static constexpr const char* kLegName = Transient ? "transient-trio" : "trio";
    static constexpr int64_t     kI3SlackNs = 17'000'000;   ///< rho = 1 % of the largest ttl (1.5 s) + 2 ms
    static constexpr int64_t     kI7SlackNs = 515'000'000;  ///< ttl*1.01 (the 1 % of 1.5 s) + 500 ms: a broker cycle, a commit and the tap
    static constexpr int64_t     kTopicMaxTtlMs = Transient ? 1000 : 1500;
    static constexpr int64_t     kRingBytes = kTransientRingBytes;

    explicit TrioEnvT(const std::string& tag)
        : trio_(tag, [this](NodeConfig& c) {
              extendNs_ = leaderHeartbeatTimeoutNs(c);
              if (Transient) c.memoryLogRingBytes = static_cast<int32_t>(kTransientRingBytes);
          }) {
        if constexpr (Transient) topic_ = trio_.defineTransientLease("st", 2, 1000, 1000, 1000);
        else topic_ = trio_.defineLease("st", 2, 1000, 1000, 5000, /*checkpoint=*/true, /*checkpointIntervalMs=*/1000);
        rc::LeaseTopicConfig cfg;
        cfg.opTimeoutMs = 2000;
        const auto end = lt3::Clock::now() + 90s;
        for (;;) {   // the client's catalog view may trail the DDL for a moment
            try { ops_.emplace(trio_.client().template openLeaseTopicAndWait<LeaseClaim>("st", cfg, 30s)); break; }
            catch (const std::exception&) { if (lt3::Clock::now() >= end) throw; std::this_thread::sleep_for(200ms); }
        }
        tap_ = std::make_unique<LogTap>(trio_.client(), topic_.id, 2, true, Transient);
    }
    ~TrioEnvT() {
        tap_.reset();
        if (ops_) ops_->close();
    }

    Lt&      ops() { return *ops_; }
    Lt&      reads() { return *ops_; }
    LogTap&  tap() { return *tap_; }
    int      partitions() const { return 2; }
    int64_t  nowNs() const { return rune::consensus::leaseClockNowNs(); }
    uint32_t ttlChoice(std::mt19937_64& rng) const {
        if constexpr (Transient) {
            (void)rng;
            return 1000;   // max_ttl is the floor: one ttl
        } else {
            static constexpr uint32_t k[] = {1000, 1000, 1200, 1500};
            return k[rng() % (sizeof k / sizeof k[0])];
        }
    }
    /// T_ready + extend + R (R <= the largest checkpoint interval, 2 s) + the slack of a quiet cluster.
    int64_t promotionWindowNs() const { return extendNs_ + 2'000'000'000LL + kI7SlackNs; }

    bool alterCheckpoint(bool on, uint32_t intervalMs) {
        namespace pcl = rune::protocol;
        const pc::ConfigChangeWire ch[] = {{static_cast<uint8_t>(pcl::ConfigKey::LeaseCheckpoint), on ? 1 : 0},
                                           {static_cast<uint8_t>(pcl::ConfigKey::LeaseCheckpointIntervalMs), intervalMs}};
        try {
            return trio_.client().alterTopicConfigAndWait("st", 0, ch, {}, 30s).errorCode == 0;
        } catch (const std::exception&) {
            return false;
        }
    }
    /// Kills the leader of `partition` unless it is the node the client is seeded on; returns the dead node, or -1 (skipped).
    int killLeader(int partition) {
        for (int i = 0; i < 2; ++i) {
            const int p = (partition + i) % 2;
            const int32_t leader = trio_.leaderOf(topic_, p);
            if (leader < 1 || leader == trio_.seedNode()) continue;
            return trio_.killLeader(topic_, p);
        }
        return -1;
    }
    void restartNode(int id) { trio_.restartNode(id, topic_); }
    uint64_t staleBroker() {
        uint64_t sum = 0;
        const int32_t topic = topic_.id;
        for (int id : trio_.nodes().runningNodes()) {
            BrokerNode& n = trio_.nodes().node(id);
            for (int32_t w = 0; w < n.workerCountForTest(); ++w)
                for (int32_t p = 0; p < 2; ++p)
                    sum += n.onWorkerThreadForTest(w, [topic, p](Worker& wk) -> uint64_t {
                        const auto* s = wk.verdictStatsForTest(topic, p);
                        return s != nullptr ? s->leaseRejectedStaleBroker : 0;
                    });
        }
        return sum;
    }

    // ---- transient mode
    rc::Client& watchClient() { return trio_.client(); }
    rc::Client& tapClient() { return trio_.client(); }
    int32_t     topicId() const { return topic_.id; }
    /// Stops all three nodes (a clean stop) and starts them again over their data dirs: every ring is reborn.  Blocks until the clients are served
    /// again (bounded).  The real client stays open and reconnects on its own.
    void fullRestart() {
        for (int id : {3, 2, 1}) trio_.nodes().stop(id);
        for (int id : {1, 2, 3}) trio_.nodes().startNode(id);
        for (int p = 0; p < 2; ++p)
            if (trio_.leaderOf(topic_, p) < 1) throw std::runtime_error("no partition leader after the full restart");
        const auto end = std::chrono::steady_clock::now() + 90s;
        for (;;) {
            try {
                (void)ops_->getAndWait(lbr::claimRow("k00"), -1, 5s);
                const rc::LeaseResult r = ops_->keepAliveOnceAndWait(lbr::claimRow("k00"), 7, {}, 5s);
                if (r.errorCode == kNotFound) return;
            } catch (const std::exception&) {}
            if (std::chrono::steady_clock::now() >= end) throw std::runtime_error("the client never recovered after the full restart");
            std::this_thread::sleep_for(20ms);
        }
    }
    bool fenceOpen(int p) {
        const int32_t leader = trio_.leaderOf(topic_, p);
        if (leader < 1) return false;
        const auto s = trio_.runtimeOn(leader, topic_.id, p);
        return s.has && s.fence == 3;
    }
    int64_t fenceDeadlineNs(int) { return 0; }   // unknown from here: the driver uses restart time + max_ttl as a lower bound
    uint64_t carriedRecords() {
        uint64_t sum = 0;
        for (int id : trio_.nodes().runningNodes())
            for (int p = 0; p < 2; ++p) sum += trio_.runtimeOn(id, topic_.id, p).carried;
        return sum;
    }

private:
    int64_t                  extendNs_ = 0;
    LeaseTrio                trio_;
    LeaseTrio::Topic         topic_;
    std::optional<Lt>        ops_;
    std::unique_ptr<LogTap>  tap_;
};
using TrioEnv          = TrioEnvT<false>;
using TransientTrioEnv = TrioEnvT<true>;

struct Timing { std::string name; double seconds; };
std::vector<Timing>& timings() {
    static std::vector<Timing> t;
    return t;
}

}  // namespace

class LeaseStressRegression : public testing::TestWithParam<int> {
public:
    static void TearDownTestSuite() {
        auto v = timings();
        std::sort(v.begin(), v.end(), [](const Timing& a, const Timing& b) { return a.seconds > b.seconds; });
        for (std::size_t i = 0; i < v.size() && i < 5; ++i) std::cout << "[ slowest ] " << v[i].name << " " << v[i].seconds << " s\n";
    }
};

/// The seed filter: RUNE_LEASE_STRESS_SEED=n runs only seed n; otherwise seeds up to `defaultMax`.
static bool seedWanted(int seed, int defaultMax) {
    if (const char* e = std::getenv("RUNE_LEASE_STRESS_SEED")) return seed == std::atoi(e);
    return seed <= defaultMax;
}

TEST_P(LeaseStressRegression, SingleNodeSeededRegression) {
    const int seed = GetParam();
    if (!seedWanted(seed, 20)) GTEST_SKIP() << "RUNE_LEASE_STRESS_SEED";
    const auto t0 = std::chrono::steady_clock::now();
    SingleEnv env("lease-stress-s" + std::to_string(seed));
    Driver<SingleEnv> d(env, static_cast<uint64_t>(seed), 3000);
    const bool ok = d.run();
    const double secs = std::chrono::duration<double>(std::chrono::steady_clock::now() - t0).count();
    timings().push_back({"single-node seed " + std::to_string(seed), secs});
    std::cout << d.summary() << " | broker appended " << env.checkpointsAppended() << " checkpoints | " << secs << " s\n";
    EXPECT_TRUE(ok) << d.failure();
    EXPECT_LT(secs, 20.0) << "an instance finishes in under 20 s";
}

TEST_P(LeaseStressRegression, TrioSeededRegression) {
    const int seed = GetParam();
    if (!seedWanted(seed, 20)) GTEST_SKIP() << "RUNE_LEASE_STRESS_SEED";
    const auto t0 = std::chrono::steady_clock::now();
    TrioEnv env("lease-stress-t" + std::to_string(seed));
    Driver<TrioEnv> d(env, static_cast<uint64_t>(seed), 200);
    const double setupSecs = std::chrono::duration<double>(std::chrono::steady_clock::now() - t0).count();
    const bool ok = d.run();
    const double secs = std::chrono::duration<double>(std::chrono::steady_clock::now() - t0).count();
    timings().push_back({"trio seed " + std::to_string(seed), secs});
    std::cout << d.summary() << " | setup " << setupSecs << " s | " << secs << " s\n";
    for (const std::string& s : d.slowSteps()) std::cout << "  slow: " << s << "\n";
    EXPECT_TRUE(ok) << d.failure();
    EXPECT_LT(secs, 20.0) << "an instance finishes in under 20 s";
}

// Lease topics Task 32 (spec Appendix A, A.8): a transient lease topic on a 256 KiB ring, one node, the injected clock.  Churn pushes the ring past
// its carry threshold (T1/T3: no append is refused while carry can make room), a full restart reborns the ring (T4 the fence holds every grant,
// T5 holders get LeaseNotFound and the watch reports each lost key ended once), T2 a reader from the ring's head folds every live lease.
// Mutations: (a) LeaseRuntime::emitCarries a no-op -> a Churn append is refused, red; (b) LeaseRuntime::grantHoldNs returns 0 -> "T4: grant during
// fence", red.
TEST_P(LeaseStressRegression, TransientSingleNodeRegression) {
    const int seed = GetParam();
    if (!seedWanted(seed, 3)) GTEST_SKIP() << "RUNE_LEASE_STRESS_SEED";
    const auto t0 = std::chrono::steady_clock::now();
    TransientSingleEnv env("lease-stress-ts" + std::to_string(seed));
    Driver<TransientSingleEnv> d(env, static_cast<uint64_t>(seed), 130);
    const bool ok = d.run();
    const double secs = std::chrono::duration<double>(std::chrono::steady_clock::now() - t0).count();
    timings().push_back({"transient-single seed " + std::to_string(seed), secs});
    std::cout << d.summary() << " | " << secs << " s\n";
    for (const std::string& s : d.slowSteps()) std::cout << "  slow: " << s << "\n";
    EXPECT_TRUE(ok) << d.failure();
    EXPECT_GT(d.stats().carried + d.stats().carriedAtRestart, 0u) << "the leg must exercise carry (the churn never crossed the threshold)";
    EXPECT_GE(d.stats().fullRestarts, 1u) << "the leg must exercise a full restart";
    EXPECT_LT(secs, 20.0) << "an instance finishes in under 20 s";
}

// The same on three nodes (real clock): leader kills keep every lease and never fence (T1), a full restart of all three fences grants (T4).
TEST_P(LeaseStressRegression, TransientTrioRegression) {
    const int seed = GetParam();
    if (!seedWanted(seed, 2)) GTEST_SKIP() << "RUNE_LEASE_STRESS_SEED";
    const auto t0 = std::chrono::steady_clock::now();
    TransientTrioEnv env("lease-stress-tt" + std::to_string(seed));
    Driver<TransientTrioEnv> d(env, static_cast<uint64_t>(seed), 120);
    const double setupSecs = std::chrono::duration<double>(std::chrono::steady_clock::now() - t0).count();
    const bool ok = d.run();
    const double secs = std::chrono::duration<double>(std::chrono::steady_clock::now() - t0).count();
    timings().push_back({"transient-trio seed " + std::to_string(seed), secs});
    std::cout << d.summary() << " | setup " << setupSecs << " s | " << secs << " s\n";
    for (const std::string& s : d.slowSteps()) std::cout << "  slow: " << s << "\n";
    EXPECT_TRUE(ok) << d.failure();
    EXPECT_LT(secs, 60.0) << "an instance finishes in under 60 s";
}

INSTANTIATE_TEST_SUITE_P(Seeds, LeaseStressRegression, testing::Range(1, 21));
