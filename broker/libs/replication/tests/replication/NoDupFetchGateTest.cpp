// NoDupFetchGateTest.cpp — DR-style tests of the leader's no-dup-fetch gate and
// the follower's unicast fast-ack (spec §3.5).  Injected clock only: every time is
// an explicit nowNs argument; no sleeps, no spinning.
#include <gtest/gtest.h>

#include <rune/replication/PartitionReplica.h>
#include <rune/replication/FollowerSlots.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/storage/Log.h>

#include <filesystem>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>
#include <unistd.h>

using namespace rune::replication;
namespace fs = std::filesystem;

namespace {

constexpr NodeId  kLeader = 1, kF2 = 2, kF3 = 3;
constexpr int64_t kMs = 1'000'000;

struct RecordingTransport final : PartitionTransport {
    struct Resp { NodeId to; FetchResponseMsg msg; std::vector<uint8_t> records; };
    struct Req  { NodeId to; FetchRequestMsg msg; bool unicast; };
    std::vector<Resp> responses;
    std::vector<Req>  requests;

    void sendFetchRequest(const FetchRequestMsg& r) override { requests.push_back({-1, r, false}); }
    void sendFetchRequestTo(NodeId leader, const FetchRequestMsg& r) override {
        requests.push_back({leader, r, true});
    }
    void sendFetchResponse(NodeId to, const FetchResponseMsg& m) override {
        Resp x{to, m, std::vector<uint8_t>(m.records.begin(), m.records.end())};
        x.msg.records = {};
        responses.push_back(std::move(x));
    }
    // Responses addressed to `to` since the last call; clears ALL recorded responses.
    std::vector<Resp> takeFor(NodeId to) {
        std::vector<Resp> out;
        for (auto& r : responses) if (r.to == to) out.push_back(r);
        responses.clear();
        return out;
    }
};

struct TmpDir {
    fs::path path;
    explicit TmpDir(const char* tag) {
        static int seq = 0;
        path = fs::temp_directory_path() /
               (std::string("rune_nodup_") + tag + "_" + std::to_string(::getpid()) + "_" +
                std::to_string(seq++));
        fs::create_directories(path);
    }
    ~TmpDir() { std::error_code ec; fs::remove_all(path, ec); }
};

PartitionConfig cfgFor(NodeId self) {
    PartitionConfig c;
    c.partitionId = 0; c.selfId = self; c.minIsr = 1;
    c.replicaLagMaxMs = 60'000; c.replicaLagMaxOffset = 1'000'000;
    c.fetchIntervalNs = 1 * kMs; c.isrCheckIntervalNs = 1'000 * kMs;
    c.maxFetchBytes = 1 << 20; c.pushOnAppend = true;
    return c;
}

FetchRequestMsg fetch(NodeId follower, Offset off, int32_t maxBytes = 1 << 20, Epoch epoch = 1) {
    return FetchRequestMsg{epoch, 0, follower, off, maxBytes, 0};
}

// Leader with 10 single-record batches, pushed once (at t=1ms) to every ISR follower.
struct LeaderFixture {
    TmpDir dir{"leader"};
    rune::storage::Log log{dir.path.string(), 64LL * 1024 * 1024};
    RecordingTransport t;
    std::unique_ptr<PartitionReplica> leader;

    explicit LeaderFixture(std::vector<NodeId> isr = {kLeader, kF2, kF3}) {
        log.recover();
        leader = std::make_unique<PartitionReplica>(
            cfgFor(kLeader), Role::Leader, /*epoch=*/1,
            std::vector<NodeId>{kLeader, kF2, kF3}, std::move(isr), &log, &t);
        // Produce hot path: append straight to the Log, then onLeaderLogAppended().
        // (The direct append() API pushes inline per call; the hot path's push is
        // COALESCED in doWork — exactly one push covering [0,10).)
        for (Offset o = 0; o < 10; ++o) {
            rune::protocol::RecordBatchBuilder b(o, 1000);
            b.append("k", std::string(64, 'x'), 1000);
            log.append(b.build(), o);
        }
        leader->onLeaderLogAppended();
        leader->doWork(1 * kMs);   // coalesced push [0,10) to the ISR followers
        EXPECT_EQ(t.takeFor(kF2).size(), 1u) << "precondition: exactly one push to F2";
    }
};

} // namespace

TEST(NoDupFetchGateRegression, AdvancingFollowerGetsAckOnly) {
    LeaderFixture f;
    ASSERT_EQ(f.leader->onFetchRequest(fetch(kF2, 4), 2 * kMs), kErrNone);
    auto r1 = f.t.takeFor(kF2);
    ASSERT_EQ(r1.size(), 1u);
    EXPECT_TRUE(r1[0].records.empty()) << "pushed range must not be re-sent while F2 advances";
    f.leader->onFetchRequest(fetch(kF2, 8), 3 * kMs);
    auto r2 = f.t.takeFor(kF2);
    ASSERT_EQ(r2.size(), 1u);
    EXPECT_TRUE(r2[0].records.empty());
}

TEST(NoDupFetchGateRegression, StuckFollowerIsServedDataAfterResendWindow) {
    LeaderFixture f;
    f.leader->onFetchRequest(fetch(kF2, 4), 2 * kMs);
    ASSERT_TRUE(f.t.takeFor(kF2).at(0).records.empty());
    f.leader->onFetchRequest(fetch(kF2, 4), 2 * kMs + kFetchResendNs);
    auto r = f.t.takeFor(kF2);
    ASSERT_EQ(r.size(), 1u);
    ASSERT_FALSE(r[0].records.empty()) << "stuck >= kFetchResendNs must be served data";
    rune::protocol::RecordBatchReader rd(r[0].records.data(), r[0].records.size());
    EXPECT_LE(rd.baseOffset(), 4);
    EXPECT_GE(rd.baseOffset() + rd.lastOffsetDelta(), 4);
}

TEST(NoDupFetchGateRegression, LostPushRecoveredWithoutDuplicateFlood) {
    LeaderFixture f;   // the fixture's push to F3 is "lost": F3 never applied it
    f.leader->onFetchRequest(fetch(kF3, 0), 2 * kMs);
    auto a = f.t.takeFor(kF3);
    ASSERT_EQ(a.size(), 1u);
    EXPECT_TRUE(a[0].records.empty());
    f.leader->onFetchRequest(fetch(kF3, 0), 7 * kMs);            // identical, inside window
    EXPECT_TRUE(f.t.takeFor(kF3).empty()) << "identical request inside the window is conflated";
    f.leader->onFetchRequest(fetch(kF3, 0), 2 * kMs + kFetchResendNs);
    auto c = f.t.takeFor(kF3);
    ASSERT_EQ(c.size(), 1u);
    EXPECT_FALSE(c[0].records.empty()) << "lost push re-served after the window";
}

TEST(NoDupFetchGateRegression, LeaderChangeResetsPushCoverage) {
    LeaderFixture f;
    f.leader->becomeFollower(/*leaderEpoch=*/2, /*leaderId=*/kF2);
    f.leader->becomeLeader(/*newEpoch=*/3, {kLeader, kF2, kF3});
    f.t.responses.clear();
    f.leader->onFetchRequest(fetch(kF2, 4, 1 << 20, /*epoch=*/3), 3 * kMs);
    auto r = f.t.takeFor(kF2);
    ASSERT_EQ(r.size(), 1u);
    EXPECT_FALSE(r[0].records.empty())
        << "nothing was pushed in THIS leadership — data must be served at once";
}

TEST(NoDupFetchGate, FollowerOutsideIsrWasNeverPushedAndIsServedImmediately) {
    LeaderFixture f({kLeader, kF2});   // F3 not in ISR → never pushed
    f.leader->onFetchRequest(fetch(kF3, 0), 2 * kMs);
    auto r = f.t.takeFor(kF3);
    ASSERT_EQ(r.size(), 1u);
    EXPECT_FALSE(r[0].records.empty());
}

TEST(NoDupFetchGate, PureFastAckNeverCarriesData) {
    LeaderFixture f;
    f.leader->onFetchRequest(fetch(kF2, 0, /*maxBytes=*/0), 2 * kMs + 5 * kFetchResendNs);
    auto r = f.t.takeFor(kF2);
    ASSERT_EQ(r.size(), 1u);
    EXPECT_TRUE(r[0].records.empty());
}

TEST(FollowerSlotsRegression, ReplicaSetAboveCapacityRefusedAtConstruction) {
    TmpDir d{"cap"};
    rune::storage::Log log{d.path.string(), 64LL * 1024 * 1024};
    log.recover();
    RecordingTransport t;
    std::vector<NodeId> big;
    for (NodeId i = 0; i < static_cast<NodeId>(FollowerSlots::kMaxReplicas) + 1; ++i) big.push_back(i);
    PartitionConfig c = cfgFor(0);
    EXPECT_THROW(PartitionReplica(c, Role::Follower, 1, big, big, &log, &t), std::invalid_argument);
}

TEST(FastAckUnicastRegression, FollowerFastAckTargetsItsLeader) {
    TmpDir d{"follower"};
    rune::storage::Log log{d.path.string(), 64LL * 1024 * 1024};
    log.recover();
    RecordingTransport t;
    PartitionReplica follower(cfgFor(kF2), Role::Follower, 1,
                              {kLeader, kF2, kF3}, {kLeader, kF2, kF3}, &log, &t);
    follower.becomeFollower(/*leaderEpoch=*/1, /*leaderId=*/kLeader);
    rune::protocol::RecordBatchBuilder b(0, 1000);
    b.append("k", "v", 1000);
    const std::vector<uint8_t> batch = b.build();
    FetchResponseMsg resp{1, 0, 0, 0, kErrNone, 0, std::span<const uint8_t>(batch)};
    follower.applyReplicated(resp);
    ASSERT_FALSE(t.requests.empty()) << "apply of new data must emit a fast-ack";
    const auto& ack = t.requests.back();
    EXPECT_TRUE(ack.unicast) << "fast-ack must be unicast to the leader, not broadcast";
    EXPECT_EQ(ack.to, kLeader);
    EXPECT_EQ(ack.msg.maxBytes, 0);
}
