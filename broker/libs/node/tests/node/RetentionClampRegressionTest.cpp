// RetentionClampRegressionTest.cpp - namespaces Phase 2 Task 13 (D27): the effective retention of a topic is
// min(topic retention, the namespace maximum), read by the cleaner at every retention pass.
//
// Deterministic: the cleaner is driven by direct doWork calls (registry mode) and the owner's retention renames by
// NodeRuntimeAgent::doWork (a deletion = the .log renamed .deleted); the namespace maximums arrive as the cleaner's own
// kNsLimitMsgType frames.
//
// Interaction matrix (topic retention x namespace maximum x timing):
//   | topic retention | ns maximum       | timing                    | expected                                        | test
//   |-----------------|------------------|---------------------------|-------------------------------------------------|-----
//   | huge / unbounded| small ms         | first pass                | old segments deleted                            | RetentionClampRegression.TimeClampedRegression
//   | huge / unbounded| small bytes      | first pass                | segments deleted down to the bytes              | RetentionClampRegression.BytesClampedRegression
//   | small           | huge             | first pass                | deleted (the topic's own, lower value wins)     | RetentionClampRegression.TopicLowerWinsRegression
//   | unbounded       | huge, then small | a pass between the two    | kept, then deleted on the NEXT pass             | RetentionClampRegression.LoweredLimitNextPassRegression
//   | unbounded       | small            | internal topic (id < 0)   | never clamped                                   | RetentionClampRegression.InternalTopicsNotClampedRegression
//   | unbounded       | small, wrong nsId| stale message             | not clamped by another namespace's slot         | RetentionClampRegression.StaleNamespaceTagNeverClampsRegression
//   | unbounded       | small, then freed| slot released             | clamp gone                                      | RetentionClampRegression.ReleasedSlotClampsNothingRegression
//   | -1 / 0 / n      | 0 / n            | arithmetic                | min with -1 = unbounded, 0 = unlimited          | RetentionClampRegression.MinArithmeticRegression
#include <gtest/gtest.h>

#include <rune/consensus/AgentRunner.h>
#include <rune/node/CleanerViewChannel.h>
#include <rune/node/LogCleanerAgent.h>
#include <rune/node/NodeRuntimeAgent.h>
#include <rune/node/ReplicaRegistry.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/storage/Log.h>

#include <atomic>
#include <cstring>
#include <filesystem>
#include <memory>
#include <string>
#include <vector>

#include <unistd.h>

namespace fs = std::filesystem;
using namespace rune::replication;
using rune::protocol::ConfigKey;

namespace {

fs::path tempDir(const char* stem) {
    static std::atomic<int> seq{0};
    auto p = fs::temp_directory_path() /
             (std::string(stem) + "_" + std::to_string(::getpid()) + "_" + std::to_string(seq++));
    fs::create_directories(p);
    return p;
}

int countExt(const fs::path& dir, const char* ext) {
    int n = 0;
    for (auto& e : fs::directory_iterator(dir)) n += e.path().extension() == ext ? 1 : 0;
    return n;
}

class NullTransport : public PartitionTransport {
public:
    bool sendFetchRequest(const FetchRequestMsg&) override { return true; }
    bool sendFetchResponse(NodeId, const FetchResponseMsg&) override { return true; }
};
class NoConsensus : public rune::consensus::Agent {
public:
    int doWork(int64_t) override { return 0; }
};


constexpr uint16_t kSlot = 3;
constexpr uint32_t kNs   = 77;
constexpr int64_t  kNowMs = 1'700'000'000'000LL;   // a fixed epoch-ms "now" (2023): the 1970 records are old, deterministically
constexpr int64_t  kHuge = 10'000'000'000'000LL;   // ~316 years of ms: far beyond "now" for the 1970 records

/// [slot:u16][pad:u16][nsId:u32][maxRetentionMs:u64][maxRetentionBytes:u64] - the kNsLimitMsgType layout.
std::vector<uint8_t> nsFrame(uint16_t slot, uint32_t nsId, uint64_t ms, uint64_t bytes) {
    std::vector<uint8_t> f(rune::node::CleanerViewChannel::kNsLimitBytes, 0);
    std::memcpy(f.data(), &slot, 2);
    std::memcpy(f.data() + 4, &nsId, 4);
    std::memcpy(f.data() + 8, &ms, 8);
    std::memcpy(f.data() + 16, &bytes, 8);
    return f;
}

/// A Delete-policy leader of namespace (kNs, kSlot) whose log holds 20 OLD (1970) records over several segments.
struct Rig {
    fs::path dir = tempDir("rune_retention_clamp");
    rune::storage::Log log{dir.string(), /*rollBytes=*/128};
    NullTransport transport;
    rune::node::ReplicaRegistry reg;
    PartitionReplica* replica = nullptr;
    int segmentsBefore = 0;
    int64_t clockMs = 0;

    Rig(int32_t topicId, int64_t topicRetentionMs, int64_t topicRetentionBytes, uint32_t logNsId = kNs) {
        log.setNsTag(logNsId, kSlot);
        int64_t off = 0;
        for (int i = 0; i < 20; ++i) {
            rune::protocol::RecordBatchBuilder b(off, 1000);
            b.append("k" + std::to_string(i), "v", 1000);
            off = log.append(b.build(), off);
        }
        rune::protocol::RecordBatchBuilder fill(off, 1000);
        fill.append("fill", std::string(200, 'x'), 1000);
        off = log.append(fill.build(), off);
        segmentsBefore = countExt(dir, ".log");

        PartitionConfig cfg;
        cfg.topicId = topicId; cfg.partitionId = 0; cfg.selfId = 1; cfg.minIsr = 1;
        cfg.replicaLagMaxMs = 10'000; cfg.replicaLagMaxOffset = 10'000;
        cfg.fetchIntervalNs = 1'000'000LL; cfg.isrCheckIntervalNs = 1'000'000LL;
        cfg.maxFetchBytes = 1 << 20; cfg.pushOnAppend = false;
        cfg.cleanupPolicy = CleanupPolicy::Delete;
        cfg.retentionMs = topicRetentionMs; cfg.retentionBytes = topicRetentionBytes;
        cfg.retentionCheckMs = 1; cfg.fileDeleteDelayMs = 60'000;
        auto r = std::make_unique<PartitionReplica>(cfg, Role::Leader, 1, std::vector<NodeId>{1},
                                                    std::vector<NodeId>{1}, &log, &transport);
        r->becomeLeader(1, {1});
        r->onLeaderLogAppended();
        replica = r.get();
        reg.add(topicId, 0, std::move(r));
    }
    ~Rig() { std::error_code ec; fs::remove_all(dir, ec); }

    /// `n` retention passes at a FIXED epoch-ms "now" (the cleaner's own cadence reads the wall clock, which would make the
    /// number of passes depend on elapsed real time); the owner (runtime agent) then renames *.deleted.
    void cycles(rune::node::LogCleanerAgent& cleaner, int n) {
        NoConsensus none;
        rune::node::NodeRuntimeAgent duty(none, nullptr, nullptr, nullptr, &reg, nullptr);
        for (int i = 0; i < n; ++i) {
            ++clockMs;
            cleaner.doWork(clockMs * 1'000'000LL);   // the cycle's housekeeping (staging renames, sealed snapshot)
            cleaner.runRetentionForTest(kNowMs);
            duty.doWork(clockMs * 1'000'000LL);
        }
    }
    [[nodiscard]] int deleted() const { return countExt(dir, ".deleted"); }
};

}  // namespace

TEST(RetentionClampRegression, TimeClampedRegression) {
    Rig rig(41, /*topic ms*/ kHuge, /*topic bytes*/ -1);
    ASSERT_GE(rig.segmentsBefore, 2);
    rune::node::LogCleanerAgent cleaner(&rig.reg);
    rig.cycles(cleaner, 8);
    EXPECT_EQ(rig.deleted(), 0) << "non-vacuous: the topic's own retention (huge) deletes nothing";
    const auto f = nsFrame(kSlot, kNs, /*max ms*/ 1, 0);
    ASSERT_TRUE(cleaner.applyNsLimitMessage(f.data(), f.size()));
    rig.cycles(cleaner, 8);
    EXPECT_GT(rig.deleted(), 0) << "min(topic, namespace maximum) = 1 ms deletes the 1970 segments";
}

TEST(RetentionClampRegression, BytesClampedRegression) {
    Rig rig(42, -1, /*topic bytes*/ 1LL << 30);
    ASSERT_GE(rig.segmentsBefore, 2);
    rune::node::LogCleanerAgent cleaner(&rig.reg);
    rig.cycles(cleaner, 8);
    EXPECT_EQ(rig.deleted(), 0);
    const auto f = nsFrame(kSlot, kNs, 0, /*max bytes*/ 100);
    ASSERT_TRUE(cleaner.applyNsLimitMessage(f.data(), f.size()));
    rig.cycles(cleaner, 8);
    EXPECT_GT(rig.deleted(), 0) << "the namespace's size maximum trims the log below the topic's 1 GiB";
}

TEST(RetentionClampRegression, TopicLowerWinsRegression) {
    Rig rig(43, /*topic ms*/ 1, -1);
    rune::node::LogCleanerAgent cleaner(&rig.reg);
    const auto f = nsFrame(kSlot, kNs, static_cast<uint64_t>(kHuge), 0);   // the namespace allows far more
    ASSERT_TRUE(cleaner.applyNsLimitMessage(f.data(), f.size()));
    rig.cycles(cleaner, 8);
    EXPECT_GT(rig.deleted(), 0) << "the topic's own, lower retention still applies";
}

TEST(RetentionClampRegression, LoweredLimitNextPassRegression) {
    Rig rig(44, -1, -1);
    rune::node::LogCleanerAgent cleaner(&rig.reg);
    const auto hi = nsFrame(kSlot, kNs, static_cast<uint64_t>(kHuge), 0);
    ASSERT_TRUE(cleaner.applyNsLimitMessage(hi.data(), hi.size()));
    rig.cycles(cleaner, 8);
    EXPECT_EQ(rig.deleted(), 0) << "a high maximum deletes nothing";
    const auto lo = nsFrame(kSlot, kNs, 1, 0);   // the SetLimits lowers it: the NEXT pass applies it, no restart
    ASSERT_TRUE(cleaner.applyNsLimitMessage(lo.data(), lo.size()));
    rig.cycles(cleaner, 8);
    EXPECT_GT(rig.deleted(), 0);
}

TEST(RetentionClampRegression, InternalTopicsNotClampedRegression) {
    Rig rig(-7, -1, -1);   // an internal topic: topicId < 0
    rune::node::LogCleanerAgent cleaner(&rig.reg);
    const auto f = nsFrame(kSlot, kNs, 1, 1);
    ASSERT_TRUE(cleaner.applyNsLimitMessage(f.data(), f.size()));
    rig.cycles(cleaner, 8);
    EXPECT_EQ(rig.deleted(), 0) << "an internal topic is never clamped by a namespace maximum";
    int64_t ms = -1, bytes = -1;
    cleaner.clampRetentionForTest(-7, kSlot, kNs, ms, bytes);
    EXPECT_EQ(ms, -1);
    EXPECT_EQ(bytes, -1);
}

TEST(RetentionClampRegression, StaleNamespaceTagNeverClampsRegression) {
    Rig rig(45, -1, -1, /*the log belongs to ANOTHER namespace*/ kNs + 1);
    rune::node::LogCleanerAgent cleaner(&rig.reg);
    const auto f = nsFrame(kSlot, kNs, 1, 1);   // the slot's current owner is kNs
    ASSERT_TRUE(cleaner.applyNsLimitMessage(f.data(), f.size()));
    rig.cycles(cleaner, 8);
    EXPECT_EQ(rig.deleted(), 0) << "a slot's maximum applies only to the namespace that owns it";
}

TEST(RetentionClampRegression, ReleasedSlotClampsNothingRegression) {
    Rig rig(46, -1, -1);
    rune::node::LogCleanerAgent cleaner(&rig.reg);
    const auto f = nsFrame(kSlot, kNs, 1, 0);
    ASSERT_TRUE(cleaner.applyNsLimitMessage(f.data(), f.size()));
    const auto rel = nsFrame(kSlot, rune::node::CleanerViewChannel::kNsLimitFree, 0, 0);
    ASSERT_TRUE(cleaner.applyNsLimitMessage(rel.data(), rel.size()));
    rig.cycles(cleaner, 8);
    EXPECT_EQ(rig.deleted(), 0);
    const std::vector<uint8_t> bad(5, 0);
    EXPECT_FALSE(cleaner.applyNsLimitMessage(bad.data(), bad.size())) << "a malformed frame is refused whole";
}

TEST(RetentionClampRegression, MinArithmeticRegression) {
    rune::node::LogCleanerAgent cleaner(nullptr);
    const auto f = nsFrame(kSlot, kNs, 1000, 4096);
    ASSERT_TRUE(cleaner.applyNsLimitMessage(f.data(), f.size()));
    auto clamp = [&](int64_t ms, int64_t bytes) {
        cleaner.clampRetentionForTest(5, kSlot, kNs, ms, bytes);
        return std::pair{ms, bytes};
    };
    EXPECT_EQ(clamp(-1, -1), (std::pair<int64_t, int64_t>{1000, 4096}));       // unbounded -> the maximum
    EXPECT_EQ(clamp(5000, 9999), (std::pair<int64_t, int64_t>{1000, 4096}));   // higher -> the maximum
    EXPECT_EQ(clamp(10, 20), (std::pair<int64_t, int64_t>{10, 20}));           // lower wins
    EXPECT_EQ(clamp(1000, 4096), (std::pair<int64_t, int64_t>{1000, 4096}));   // equal
    const auto unl = nsFrame(kSlot, kNs, 0, 0);                                // 0 = unlimited: no clamp
    ASSERT_TRUE(cleaner.applyNsLimitMessage(unl.data(), unl.size()));
    EXPECT_EQ(clamp(-1, 77), (std::pair<int64_t, int64_t>{-1, 77}));
}
