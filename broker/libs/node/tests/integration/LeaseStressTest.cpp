// LeaseStressTest.cpp - lease topics Task 18 (spec 16.4): the seeded model-checked lease stress.
//
//   LeaseStressRegression.SingleNodeSeededRegression/<seed>  leg S: ONE real authenticating shard broker, the injected lease clock,
//                                                            seeds 1..20, 3 000 steps each.
//   LeaseStressRegression.TrioSeededRegression/<seed>        leg T: THREE real brokers (replication factor 3, acks=all) on the real clock,
//                                                            seeds 1..20, 200 steps each, at most 3 leader kills per seed.
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

class SingleEnv {
public:
    static constexpr bool        kInjected = true;
    static constexpr const char* kLegName = "single-node";
    static constexpr int64_t     kI3SlackNs = 0;            ///< rho = 0: the clock is exact
    static constexpr int64_t     kI7SlackNs = kTickNs;      ///< one duty tick (the wheel's 1 ms tick)
    static constexpr int64_t     kTopicMaxTtlMs = 10'000;

    explicit SingleEnv(const std::string& tag) : rig_(tag, 60'000, 1000, 600'000), nowNs_(1'000'000'000'000LL) {
        auto s = lbr::spec(60'000, 1000, 600'000);
        s.checkpoint = 1;
        s.checkpointIntervalMs = 1000;
        s.checkpointRate = 1000;
        rc::TopicOptions to;
        to.lease = s;
        to.config.cleanupPolicy = 1;
        const auto r = rig_.ua().defineTopicAndWait("st", 2, "claim", lbr::claimBlob(), rc::Persistence::Disk, to, {}, 60s);
        if (r.errorCode != 0) throw std::runtime_error("define lease st: " + r.detail);
        topicId_ = r.topicId;
        rc::LeaseTopicConfig cfg;
        cfg.opTimeoutMs = 20'000;   // wall time: a lease write slower than this is a hang, not a lease problem
        opsClient_ = connect_("a/ua", lbr::kUserPw);   // the operations client is ours: RejectAll ends its session for good, and it is then replaced
        ops_.emplace(opsClient_->openLeaseTopicAndWait<LeaseClaim>("st", cfg, 30s));
        obsClient_ = connect_("admin", lbr::kAdminPw);   // the observer: the lease view reads and the log tap
        reads_.emplace(obsClient_->openLeaseTopicAndWait<LeaseClaim>("a/st", {}, 30s));
        tap_ = std::make_unique<LogTap>(*obsClient_, topicId_, 2, false);
    }
    ~SingleEnv() {
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
    int64_t  nowNs() const { return nowNs_; }
    uint32_t ttlChoice(std::mt19937_64& rng) const {
        static constexpr uint32_t k[] = {1000, 1000, 1000, 1500, 2000, 3000, 5000, 10'000};
        return k[rng() % (sizeof k / sizeof k[0])];
    }
    void advance(int64_t ns) {
        rig_.advance(std::chrono::milliseconds(ns / 1'000'000));
        nowNs_ += ns;
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
            cfg.opTimeoutMs = 20'000;
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

// ------------------------------------------------------------------------------------------------ leg T: three nodes, real clock

class TrioEnv {
public:
    static constexpr bool        kInjected = false;
    static constexpr const char* kLegName = "trio";
    static constexpr int64_t     kI3SlackNs = 17'000'000;   ///< rho = 1 % of the largest ttl (1.5 s) + 2 ms
    static constexpr int64_t     kI7SlackNs = 515'000'000;  ///< ttl*1.01 (the 1 % of 1.5 s) + 500 ms: a broker cycle, a commit and the tap
    static constexpr int64_t     kTopicMaxTtlMs = 1500;

    explicit TrioEnv(const std::string& tag)
        : trio_(tag, [this](NodeConfig& c) { extendNs_ = leaderHeartbeatTimeoutNs(c); }) {
        topic_ = trio_.defineLease("st", 2, 1000, 1000, 5000, /*checkpoint=*/true, /*checkpointIntervalMs=*/1000);
        rc::LeaseTopicConfig cfg;
        cfg.opTimeoutMs = 2000;
        const auto end = lt3::Clock::now() + 90s;
        for (;;) {   // the client's catalog view may trail the DDL for a moment
            try { ops_.emplace(trio_.client().openLeaseTopicAndWait<LeaseClaim>("st", cfg, 30s)); break; }
            catch (const std::exception&) { if (lt3::Clock::now() >= end) throw; std::this_thread::sleep_for(200ms); }
        }
        tap_ = std::make_unique<LogTap>(trio_.client(), topic_.id, 2, true);
    }
    ~TrioEnv() {
        tap_.reset();
        if (ops_) ops_->close();
    }

    Lt&      ops() { return *ops_; }
    Lt&      reads() { return *ops_; }
    LogTap&  tap() { return *tap_; }
    int      partitions() const { return 2; }
    int64_t  nowNs() const { return rune::consensus::leaseClockNowNs(); }
    uint32_t ttlChoice(std::mt19937_64& rng) const {
        static constexpr uint32_t k[] = {1000, 1000, 1200, 1500};
        return k[rng() % (sizeof k / sizeof k[0])];
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

private:
    int64_t                  extendNs_ = 0;
    LeaseTrio                trio_;
    LeaseTrio::Topic         topic_;
    std::optional<Lt>        ops_;
    std::unique_ptr<LogTap>  tap_;
};

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

TEST_P(LeaseStressRegression, SingleNodeSeededRegression) {
    const int seed = GetParam();
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

INSTANTIATE_TEST_SUITE_P(Seeds, LeaseStressRegression, testing::Range(1, 21));
