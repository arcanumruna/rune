// LogCleanerPolicyOverrideTest.cpp — catalog history Task 8 (ruling 5): an
// AlterTopicConfig the catalog applied reaches the CLEANER as a per-topic policy
// override (CleanerViewChannel::kPolicyMsgType on its view ring), consulted
// before the replica's construction-time config in runRetention and the
// compaction gates.  The replica's own config is never touched.
//
// Deterministic: the cleaner is driven by direct doWork calls (registry mode) and
// the owner's retention renames by NodeRuntimeAgent::doWork — no threads, no
// sleeps in the decisive assertions (a deletion = the .log renamed .deleted).
#include <gtest/gtest.h>

#include <rune/consensus/AgentRunner.h>
#include <rune/node/CleanerViewChannel.h>
#include <rune/node/LogCleanerAgent.h>
#include <rune/node/NodeRuntimeAgent.h>
#include <rune/node/ReplicaRegistry.h>
#include <rune/protocol/CatalogTypes.h>
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

/// [topicId:i32][count:u16][(key:u8, value:i64) * count] — the kPolicyMsgType layout.
std::vector<uint8_t> policyFrame(int32_t topicId, std::vector<std::pair<ConfigKey, int64_t>> kv) {
    std::vector<uint8_t> f(rune::node::CleanerViewChannel::kPolicyHeaderBytes +
                           kv.size() * rune::node::CleanerViewChannel::kPolicyEntryBytes);
    const auto count = static_cast<uint16_t>(kv.size());
    std::memcpy(f.data(), &topicId, 4);
    std::memcpy(f.data() + 4, &count, 2);
    for (std::size_t i = 0; i < kv.size(); ++i) {
        uint8_t* e = f.data() + 6 + i * 9;
        e[0] = static_cast<uint8_t>(kv[i].first);
        std::memcpy(e + 1, &kv[i].second, 8);
    }
    return f;
}

/// A Delete-policy leader whose OWN config keeps everything (retention off).
struct Rig {
    fs::path dir = tempDir("rune_policy_override");
    rune::storage::Log log{dir.string(), /*rollBytes=*/128};
    NullTransport transport;
    rune::node::ReplicaRegistry reg;
    PartitionReplica* replica = nullptr;
    int segmentsBefore = 0;

    explicit Rig(int32_t topicId) {
        int64_t off = 0;
        for (int i = 0; i < 20; ++i) {   // OLD records (1970): any small retention deletes them
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
        cfg.retentionMs = -1; cfg.retentionBytes = -1;           // the replica keeps EVERYTHING
        cfg.retentionCheckMs = 1; cfg.fileDeleteDelayMs = 60'000;
        auto r = std::make_unique<PartitionReplica>(cfg, Role::Leader, 1, std::vector<NodeId>{1},
                                                    std::vector<NodeId>{1}, &log, &transport);
        r->becomeLeader(1, {1});
        r->onLeaderLogAppended();
        replica = r.get();
        reg.add(topicId, 0, std::move(r));
    }
    ~Rig() { std::error_code ec; fs::remove_all(dir, ec); }

    /// Bounded: the cleaner proposes, the owner (runtime agent) renames *.deleted.
    void cycles(rune::node::LogCleanerAgent& cleaner, int n) {
        NoConsensus none;
        rune::node::NodeRuntimeAgent duty(none, nullptr, nullptr, nullptr, &reg, nullptr);
        for (int i = 0; i < n; ++i) {
            cleaner.doWork(static_cast<int64_t>(i) * 1'000'000LL);
            duty.doWork(static_cast<int64_t>(i) * 1'000'000LL);
        }
    }
};

} // namespace

// RED: the override ignored (runRetention reads replica->retentionMs() only) — the
// topic keeps every segment its ALTER TOPIC ... SET retention.ms said to delete.
TEST(LogCleanerPolicyOverrideRegression, OverrideRetentionDeletesSegmentsTheReplicaConfigWouldKeep) {
    Rig rig(41);
    ASSERT_GE(rig.segmentsBefore, 2);
    {   // non-vacuous: the replica's own config deletes nothing
        rune::node::LogCleanerAgent plain(&rig.reg);
        rig.cycles(plain, 10);
        EXPECT_EQ(countExt(rig.dir, ".log"), rig.segmentsBefore);
        EXPECT_EQ(countExt(rig.dir, ".deleted"), 0);
    }
    // A fresh cleaner (its retention cadence starts due on the first pass).
    rune::node::LogCleanerAgent cleaner(&rig.reg);
    const auto f = policyFrame(41, {{ConfigKey::RetentionMs, 1}});
    ASSERT_TRUE(cleaner.applyPolicyMessage(f.data(), f.size()));
    ASSERT_NE(cleaner.policyOverrideForTest(41), nullptr);
    EXPECT_EQ(cleaner.policyOverrideForTest(41)->retentionMs, std::optional<int64_t>{1});
    rig.cycles(cleaner, 10);
    EXPECT_GT(countExt(rig.dir, ".deleted"), 0) << "the override's retention.ms deleted the old segments";
    EXPECT_LT(countExt(rig.dir, ".log"), rig.segmentsBefore);
    EXPECT_GE(countExt(rig.dir, ".log"), 1) << "the active segment is never deleted";
    EXPECT_EQ(rig.replica->retentionMs(), -1) << "the replica's own config is never touched";
}

TEST(LogCleanerPolicyOverrideRegression, OverridesMergePerKeyAndAnotherTopicIsUntouched) {
    Rig rig(42);
    rune::node::LogCleanerAgent cleaner(&rig.reg);
    // Another topic's override never applies to this replica.
    auto other = policyFrame(99, {{ConfigKey::RetentionMs, 1}});
    ASSERT_TRUE(cleaner.applyPolicyMessage(other.data(), other.size()));
    rig.cycles(cleaner, 10);
    EXPECT_EQ(countExt(rig.dir, ".deleted"), 0);

    auto a = policyFrame(42, {{ConfigKey::RetentionBytes, 123}, {ConfigKey::MinCleanableDirtyRatioPpm, 250'000}});
    auto b = policyFrame(42, {{ConfigKey::MaxCompactionLagMs, 7}});
    ASSERT_TRUE(cleaner.applyPolicyMessage(a.data(), a.size()));
    ASSERT_TRUE(cleaner.applyPolicyMessage(b.data(), b.size()));
    ASSERT_TRUE(cleaner.applyPolicyMessage(b.data(), b.size()));   // the same effect again: idempotent
    const auto* ov = cleaner.policyOverrideForTest(42);
    ASSERT_NE(ov, nullptr);
    EXPECT_EQ(ov->retentionBytes, std::optional<int64_t>{123}) << "an earlier key survives a later change";
    EXPECT_EQ(ov->minCleanableDirtyRatio, std::optional<double>{0.25});
    EXPECT_EQ(ov->maxCompactionLagMs, std::optional<int64_t>{7});
    EXPECT_FALSE(ov->retentionMs.has_value()) << "an unset key falls back to the replica's config";
}

TEST(LogCleanerPolicyOverrideRegression, ClearAndMalformedFramesAreExplicit) {
    rune::node::ReplicaRegistry reg;
    rune::node::LogCleanerAgent cleaner(&reg);
    auto set = policyFrame(5, {{ConfigKey::DeleteRetentionMs, 10}});
    ASSERT_TRUE(cleaner.applyPolicyMessage(set.data(), set.size()));
    // A malformed frame is refused WHOLE — never half-applied.
    auto bad = policyFrame(5, {{ConfigKey::RetentionMs, 1}, {ConfigKey::MinCompactionLagMs, 3}});
    bad[6 + 9] = 0xEE;   // second entry: unknown key
    EXPECT_FALSE(cleaner.applyPolicyMessage(bad.data(), bad.size()));
    EXPECT_FALSE(cleaner.applyPolicyMessage(set.data(), set.size() - 1)) << "short frame";
    ASSERT_NE(cleaner.policyOverrideForTest(5), nullptr);
    EXPECT_FALSE(cleaner.policyOverrideForTest(5)->retentionMs.has_value()) << "half-applied";
    // count 0 = the topic was dropped: the override is cleared (twice: idempotent).
    auto clear = policyFrame(5, {});
    ASSERT_TRUE(cleaner.applyPolicyMessage(clear.data(), clear.size()));
    ASSERT_TRUE(cleaner.applyPolicyMessage(clear.data(), clear.size()));
    EXPECT_EQ(cleaner.policyOverrideForTest(5), nullptr);
}

// The production delivery: the control thread writes the frame on the cleaner's
// view ring; the cleaner applies it on its own pass.
TEST(LogCleanerPolicyOverrideRegression, OverrideArrivesOnTheCleanerViewRing) {
    rune::node::CleanerViewChannel ch;
    rune::node::LogCleanerAgent cleaner(nullptr);
    cleaner.attachViewChannel(&ch);
    const auto f = policyFrame(7, {{ConfigKey::MinCompactionLagMs, 55}});
    ASSERT_TRUE(ch.view.write(f.data(), f.size(), rune::node::CleanerViewChannel::kPolicyMsgType));
    EXPECT_EQ(cleaner.policyUpdatesForTest(), 0u);
    (void)cleaner.doWork(0);
    EXPECT_EQ(cleaner.policyUpdatesForTest(), 1u);
    ASSERT_NE(cleaner.policyOverrideForTest(7), nullptr);
    EXPECT_EQ(cleaner.policyOverrideForTest(7)->minCompactionLagMs, std::optional<int64_t>{55});
}

// Fix-on-sight (Task 8): the CP-7 checkpoint map is keyed by Log*.  A dropped
// topic's Log is freed once every holder acked (Task 8's drop sequence) and a new
// Log may be allocated at the SAME address — a stale checkpoint would seed the new
// log's clean offset from the dropped one (skipping its first compaction range).
// The cleaner now forgets a checkpoint whose Log left its view.  RED: the entry
// survives the view swap.
TEST(LogCleanerPolicyOverrideRegression, DroppedLogsCheckpointIsForgottenOnTheViewSwapRegression) {
    const fs::path dir = tempDir("rune_ckpt_prune");
    rune::storage::Log log(dir.string(), /*rollBytes=*/128);
    NullTransport transport;
    PartitionConfig cfg;
    cfg.topicId = 77; cfg.partitionId = 0; cfg.selfId = 1; cfg.minIsr = 1;
    cfg.replicaLagMaxMs = 10'000; cfg.replicaLagMaxOffset = 10'000;
    cfg.fetchIntervalNs = 1'000'000LL; cfg.isrCheckIntervalNs = 1'000'000LL;
    cfg.maxFetchBytes = 1 << 20; cfg.pushOnAppend = false;
    cfg.cleanupPolicy = CleanupPolicy::Compact; cfg.minCleanableDirtyRatio = 0.0;
    cfg.deleteRetentionMs = 86'400'000LL;
    PartitionReplica replica(cfg, Role::Leader, 1, std::vector<NodeId>{1}, std::vector<NodeId>{1}, &log, &transport);
    replica.publishCleanerLso(PartitionReplica::kNoCleanerLso);
    replica.becomeLeader(1, {1});
    for (int v = 0; v < 200; ++v) {
        rune::protocol::RecordBatchBuilder b(v, 1000);
        b.append("k1", "v" + std::to_string(v), 1000);
        log.append(b.build(), v);
    }
    replica.onLeaderLogAppended();
    ASSERT_GE(log.segmentBaseOffsets().size(), 2u);

    rune::node::CleanerViewChannel ch;
    rune::node::LogCleanerAgent cleaner(nullptr);
    cleaner.attachViewChannel(&ch);
    auto sendView = [&](uint64_t gen, std::vector<PartitionReplica*> rs) {
        std::vector<uint8_t> f(rune::node::CleanerViewChannel::kHeaderBytes + 8 * rs.size(), 0);
        const uint32_t total = static_cast<uint32_t>(rs.size()), index = 0, count = total;
        std::memcpy(f.data(), &gen, 8);
        std::memcpy(f.data() + 8, &total, 4);
        std::memcpy(f.data() + 12, &index, 4);
        std::memcpy(f.data() + 16, &count, 4);
        for (std::size_t i = 0; i < rs.size(); ++i) {
            const auto v = static_cast<uint64_t>(reinterpret_cast<std::uintptr_t>(rs[i]));
            std::memcpy(f.data() + rune::node::CleanerViewChannel::kHeaderBytes + 8 * i, &v, 8);
        }
        ASSERT_TRUE(ch.view.write(f.data(), f.size(), rune::node::CleanerViewChannel::kViewMsgType));
    };
    sendView(1, {&replica});
    for (int i = 0; i < 20 && cleaner.checkpointCountForTest() == 0; ++i)
        (void)cleaner.doWork(static_cast<int64_t>(i) * 1'000'000LL);
    ASSERT_EQ(cleaner.checkpointCountForTest(), 1u) << "non-vacuous: a compaction pass opened its checkpoint";

    sendView(2, {});   // the topic was dropped: its replica left the view
    (void)cleaner.doWork(100'000'000LL);
    EXPECT_EQ(cleaner.viewGenerationForTest(), 2u);
    EXPECT_EQ(cleaner.checkpointCountForTest(), 0u) << "a dropped Log's checkpoint must not outlive it";
    std::error_code ec;
    fs::remove_all(dir, ec);
}
