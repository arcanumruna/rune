// IsrProposalIdTest.cpp - the leader's AlterIsr proposal state machine (ISR protocol, Step 4):
// per-send correlation (proposalId + sendSeq), no clearing from AwaitingMetadata, no clearing by an answer
// to an older send, exponential hold after repeated older-send answers.
#include <gtest/gtest.h>

#include <rune/replication/PartitionReplica.h>
#include <rune/storage/InMemoryLog.h>

#include <algorithm>
#include <atomic>
#include <filesystem>
#include <memory>
#include <string>
#include <unistd.h>
#include <vector>

using namespace rune::replication;
namespace fs = std::filesystem;

namespace {

struct Dir {
    fs::path path;
    Dir() {
        static std::atomic<int> seq{0};
        path = fs::temp_directory_path() / ("rune_isrprop_" + std::to_string(::getpid()) + "_" + std::to_string(seq++));
        fs::create_directories(path);
    }
    ~Dir() { std::error_code ec; fs::remove_all(path, ec); }
};

struct CapturingTransport final : InMemoryTransport {
    std::vector<AlterIsrMsg> sent;
    bool sendAlterIsr(const AlterIsrMsg& m) override { sent.push_back(m); return true; }
};

PartitionConfig cfg() {
    PartitionConfig c;
    c.topicId = 0; c.partitionId = 0; c.selfId = 1;
    c.replicaLagMaxMs = 10'000; c.replicaLagMaxOffset = 10'000; c.minIsr = 1;
    c.fetchIntervalNs = 1'000'000; c.isrCheckIntervalNs = 1'000'000; c.maxFetchBytes = 1 << 20;
    return c;
}

const std::vector<NodeId> kAll{1, 2, 3};

std::size_t proposals(const CapturingTransport& t) {
    std::size_t n = 0;
    for (const auto& m : t.sent) if (m.isrCount > 0) ++n;
    return n;
}
const AlterIsrMsg& lastProposal(const CapturingTransport& t) {
    for (auto it = t.sent.rbegin(); it != t.sent.rend(); ++it) if (it->isrCount > 0) return *it;
    static const AlterIsrMsg none{};
    return none;
}
AlterIsrResponseMsg ans(int16_t code, int32_t pe, int64_t id, int32_t seq) {
    AlterIsrResponseMsg r{};
    r.topicId = 0; r.partition = 0; r.leaderId = 1; r.leaderEpoch = 5;
    r.partitionEpoch = pe; r.errorCode = code; r.proposalId = id; r.sendSeq = seq;
    return r;
}

struct Rig {
    Dir d;
    rune::storage::InMemoryLog log{1 << 20, 10'000};
    CapturingTransport t;
    std::unique_ptr<PartitionReplica> r;
    int64_t now = 1'000'000'000;
    int64_t id1 = 0;
    int64_t storage3 = 0;   // the storage id follower 3 fetches with (0 = a v3 follower)
    explicit Rig(int64_t selfStorage = 0, int64_t storage3_ = 0, std::vector<NodeId> committed = {1, 2}) : storage3(storage3_) {
        r = std::make_unique<PartitionReplica>(cfg(), Role::Follower, 0, kAll, kAll, &log, &t);
        r->setIsrChannel(&t);
        r->setLeaderEpochFenceFile((d.path / "fence-0-0").string());   // REQUIRED: no fence = first leadership refused
        if (selfStorage != 0) r->setStorageId(selfStorage);
        r->becomeLeader(5, committed, /*pe=*/7);
        EXPECT_EQ(r->role(), Role::Leader);
        for (int i = 0; i < 10; ++i) EXPECT_GE(r->append(std::vector<uint8_t>(16, uint8_t(i))), 0);
        fetch();
        for (int i = 0; i < 50 && proposals(t) == 0; ++i) r->doWork(now += 1'000'000);
        id1 = r->pendingProposalIdForTest();
    }
    // Follower 3 fetches at the log end (it is caught up: a candidate for the ISR).
    void fetch() {
        FetchRequestMsg f{};
        f.leaderEpoch = 5; f.partition = 0; f.followerId = 3; f.fetchOffset = 10; f.maxBytes = 0;
        f.topicId = 0; f.lastFetchedEpoch = 5; f.incarnation = 1; f.storageId = storage3;
        (void)r->onFetchRequest(f, now);
    }
    void run(int64_t totalNs, int64_t stepNs, bool keepFetching = false) {
        for (int64_t e = 0; e < totalNs; e += stepNs) {
            if (keepFetching) fetch();
            r->doWork(now += stepNs);
        }
    }
    std::string state() const { return r->isrProposalStateNameForTest(); }
    bool maximalHas(NodeId n) const {
        const auto& m = r->maximalIsrForTest();
        return std::find(m.begin(), m.end(), n) != m.end();
    }
};

}  // namespace

TEST(IsrProposalRegression, IneligibleForAnOlderSendOfTheSameProposalNeverClearsItRegression) {
    Rig g;
    ASSERT_EQ(lastProposal(g.t).isrCount, 3);   // P1 = {1,2,3}
    ASSERT_NE(g.id1, 0);
    for (int i = 0; i < 50; ++i) g.r->doWork(g.now += 1'000'000);
    ASSERT_EQ(lastProposal(g.t).sendSeq, 2);
    const std::size_t before = proposals(g.t);
    g.r->onAlterIsrResponse(ans(kAlterIsrIneligibleReplica, 7, g.id1, 1));   // the answer to send #1, not the newest
    EXPECT_EQ(g.state(), "AwaitingCommit");
    EXPECT_TRUE(g.maximalHas(3));
    g.run(900'000'000, 10'000'000);
    EXPECT_EQ(proposals(g.t), before) << "a resend was sent inside the hold after an older-send answer";
    g.r->onAlterIsrResponse(ans(kAlterIsrIneligibleReplica, 7, g.id1, 2));   // the newest send's answer clears
    EXPECT_EQ(g.state(), "None");
}

TEST(IsrProposalRegression, AnswersNeverClearFromAwaitingMetadataRegression) {
    Rig g;
    g.r->onAlterIsrResponse(ans(kAlterIsrInvalidUpdateVersion, 8, g.id1, 1));
    ASSERT_EQ(g.state(), "AwaitingMetadata");
    g.r->onAlterIsrResponse(ans(kAlterIsrIneligibleReplica, 7, g.id1, 1));
    g.r->onAlterIsrResponse(ans(kAlterIsrInvalidRequest, 7, g.id1, 1));
    EXPECT_EQ(g.state(), "AwaitingMetadata");
    EXPECT_EQ(g.r->answersWhileAwaitingMetadataForTest(), 2u);
}

TEST(IsrProposalRegression, LateInvalidRequestForAnOlderBaseNeverClearsTheCurrentOneRegression) {
    Rig g;
    g.r->becomeLeader(5, {1, 2}, 8);                       // the assignment at pe 8 resolves P1
    ASSERT_EQ(g.state(), "None");
    const std::size_t before = proposals(g.t);
    for (int i = 0; i < 200 && proposals(g.t) == before; ++i) { g.fetch(); g.r->doWork(g.now += 1'000'000); }
    ASSERT_GT(proposals(g.t), before) << "no second proposal";
    const int64_t id2 = g.r->pendingProposalIdForTest();
    ASSERT_NE(id2, g.id1);
    ASSERT_EQ(lastProposal(g.t).partitionEpoch, 8);
    g.r->onAlterIsrResponse(ans(kAlterIsrInvalidRequest, 7, g.id1, 1));   // another proposal's id
    EXPECT_NE(g.state(), "None");
    g.r->onAlterIsrResponse(ans(kAlterIsrInvalidRequest, 7, 0, 0));       // a v3 controller's answer, an older base
    EXPECT_NE(g.state(), "None");
}

TEST(IsrProposalRegression, LateIneligibleForAnEarlierProposalAtTheSameBaseNeverClearsTheCurrentOneRegression) {
    Rig g;
    g.r->onAlterIsrResponse(ans(kAlterIsrIneligibleReplica, 7, g.id1, 1));
    ASSERT_EQ(g.state(), "None");
    const std::size_t before = proposals(g.t);
    for (int i = 0; i < 400 && proposals(g.t) == before; ++i) { g.fetch(); g.r->doWork(g.now += 1'000'000); }
    ASSERT_GT(proposals(g.t), before) << "no second proposal after the back-off";
    const int64_t id2 = g.r->pendingProposalIdForTest();
    ASSERT_NE(id2, g.id1);
    ASSERT_EQ(lastProposal(g.t).partitionEpoch, 7);
    g.r->onAlterIsrResponse(ans(kAlterIsrIneligibleReplica, 7, g.id1, 1));   // late answer of P1, same base
    EXPECT_NE(g.state(), "None");
    EXPECT_TRUE(g.maximalHas(3));
}

TEST(IsrProposalRegression, StaleVersionAnswerWaitsAtTheSlowCadenceAndWarnsOnceRegression) {
    Rig g;
    const std::size_t before = proposals(g.t);
    g.r->onAlterIsrResponse(ans(kAlterIsrInvalidUpdateVersion, 8, g.id1, 1));
    ASSERT_EQ(g.state(), "AwaitingMetadata");
    g.run(500'000'000, 10'000'000);
    EXPECT_LE(proposals(g.t) - before, 1u) << "AwaitingMetadata re-sent at the fast cadence";
    g.run(10'000'000'000, 10'000'000);
    EXPECT_EQ(g.state(), "AwaitingMetadataWarned");
    g.r->becomeLeader(5, {1, 2, 3}, 8);
    EXPECT_EQ(g.state(), "None");
}

TEST(IsrProposalRegression, ResendKeepsIdAndBaseAndAdvancesSeqRegression) {
    Rig g;
    const int32_t seq0 = lastProposal(g.t).sendSeq;
    ASSERT_EQ(seq0, 1);
    std::vector<AlterIsrMsg> sends;
    for (int i = 0; i < 400 && sends.size() < 2; ++i) {
        const std::size_t n = proposals(g.t);
        g.r->doWork(g.now += 1'000'000);
        if (proposals(g.t) > n) sends.push_back(lastProposal(g.t));
    }
    ASSERT_EQ(sends.size(), 2u);
    for (const auto& m : sends) { EXPECT_EQ(m.proposalId, g.id1); EXPECT_EQ(m.partitionEpoch, 7); }
    EXPECT_EQ(sends[0].sendSeq, 2);
    EXPECT_EQ(sends[1].sendSeq, 3);
}

TEST(IsrProposalRegression, RepeatedOlderSendAnswersBackOffExponentiallyRegression) {
    Rig g;
    std::vector<int64_t> at;   // the clock of each resend after the first
    std::size_t n = proposals(g.t);
    for (int step = 0; step < 6000 && at.size() < 5; ++step) {
        g.r->doWork(g.now += 10'000'000);
        if (proposals(g.t) > n) {
            n = proposals(g.t);
            at.push_back(g.now);
            g.r->onAlterIsrResponse(ans(kAlterIsrIneligibleReplica, 7, g.id1, 1));   // always the oldest seq
        }
    }
    ASSERT_EQ(at.size(), 5u);
    const int64_t want[] = {1'000'000'000, 2'000'000'000, 4'000'000'000, 8'000'000'000};
    for (int i = 0; i < 4; ++i)
        EXPECT_NEAR(static_cast<double>(at[static_cast<std::size_t>(i) + 1] - at[static_cast<std::size_t>(i)]),
                    static_cast<double>(want[i]), 60'000'000.0) << "gap " << i;
}

// CP1 review M1: an Ok at the base clears a proposal only for the NEWEST send, only at the base, and only when our
// committed view holds every proposed member (a shrink P = {1,2} of the committed {1,2,3}).
// RED: drop the `atBase` test (the pe-8 Ok clears) / drop the newest-send rule (the seq-1 Ok clears).
TEST(IsrProposalRegression, OkAtTheBaseClearsOnlyTheNewestSendRegression) {
    Dir d;
    rune::storage::InMemoryLog log(1 << 20, 10'000);
    CapturingTransport t;
    auto r = std::make_unique<PartitionReplica>(cfg(), Role::Follower, 0, kAll, kAll, &log, &t);
    r->setIsrChannel(&t);
    r->setLeaderEpochFenceFile((d.path / "fence-0-0").string());
    r->becomeLeader(5, {1, 2, 3}, /*pe=*/7);
    ASSERT_EQ(r->role(), Role::Leader);
    int64_t now = 1'000'000'000;
    const int32_t live[] = {1, 2};
    r->shrinkIsrToLive(std::span<const int32_t>(live, 2), now);   // P = {1,2} at base 7 (send #1)
    ASSERT_EQ(std::string(r->isrProposalStateNameForTest()), "AwaitingCommit");
    const int64_t id = r->pendingProposalIdForTest();
    r->doWork(now += 60'000'000);   // send #2
    ASSERT_EQ(r->pendingLastSeqForTest(), 2);
    r->onAlterIsrResponse(ans(kAlterIsrOk, 8, id, 2));   // not at our base
    EXPECT_EQ(std::string(r->isrProposalStateNameForTest()), "AwaitingCommit");
    r->onAlterIsrResponse(ans(kAlterIsrOk, 7, id, 1));   // an older send
    EXPECT_EQ(std::string(r->isrProposalStateNameForTest()), "AwaitingCommit");
    r->onAlterIsrResponse(ans(kAlterIsrOk, 7, id, 2));   // the newest send, at the base
    EXPECT_EQ(std::string(r->isrProposalStateNameForTest()), "None");
    EXPECT_EQ(r->okAnswersDisagreeingForTest(), 0u);
}

// CP1 review M1 (guard): an Ok at the base for a set naming a member our committed ISR lacks means the views disagree;
// clearing would stop counting a member the controller can elect.  RED: no guard (the Ok clears, 3 leaves the maximal ISR).
TEST(IsrProposalRegression, OkAtTheBaseThatNamesAnUncountedMemberKeepsItCountedRegression) {
    Rig g;   // committed {1,2}, P = {1,2,3} at base 7, send #1
    ASSERT_EQ(g.r->pendingLastSeqForTest(), 1);
    g.r->onAlterIsrResponse(ans(kAlterIsrOk, 7, g.id1, 1));
    EXPECT_EQ(g.state(), "AwaitingMetadata");
    EXPECT_TRUE(g.maximalHas(3));
    EXPECT_EQ(g.r->okAnswersDisagreeingForTest(), 1u);
}

// CP1 review M5: the send ordinal saturates instead of overflowing (signed overflow is UB; UBSan traps it).
// RED: `++a->lastSeq` (INT32_MIN, or a UBSan trap).
TEST(IsrProposalRegression, SendOrdinalSaturatesInsteadOfOverflowingRegression) {
    Rig g;
    g.r->setPendingLastSeqForTest(INT32_MAX);
    g.r->doWork(g.now += 60'000'000);   // a resend
    EXPECT_EQ(g.r->pendingLastSeqForTest(), INT32_MAX);
    EXPECT_GE(lastProposal(g.t).sendSeq, 0);
}

// ISR v3 Step 6 (A-I9): a NEW storage id is a new process whatever its incarnation (a wiped dir restarts the counter
// low), and a storage the follower replaced is a dead process's.  RED: the WARN-only rule (the inc-5 fetch is
// dropped as stale, so step 2's return value is kErrStaleIncarnation).
TEST(FollowerSlotRegression, NewStorageWithAnOlderIncarnationIsANewProcessRegression) {
    Rig g(0, 0, {1, 2, 3});   // 3 is a committed member
    FetchRequestMsg f{};
    f.leaderEpoch = 5; f.partition = 0; f.topicId = 0; f.maxBytes = 0;
    const auto fetch = [&](NodeId id, Offset off, int32_t lastEpoch, int64_t inc, int64_t storage) {
        f.followerId = id; f.fetchOffset = off; f.lastFetchedEpoch = lastEpoch; f.incarnation = inc; f.storageId = storage;
        return g.r->onFetchRequest(f, g.now);
    };
    (void)fetch(2, 10, 5, 1, 0x2);
    (void)fetch(3, 10, 5, 900, 0xA);
    g.r->doWork(g.now += 1'000'000);
    EXPECT_EQ(g.r->highWatermark(), 10);
    EXPECT_EQ(g.r->followerStorageForTest(3), 0xA);
    // The wiped disk of node 3: a new storage, a LOW incarnation, an empty log.
    EXPECT_NE(fetch(3, 0, -1, 5, 0xB), kErrStaleIncarnation);
    EXPECT_EQ(g.r->followerStorageForTest(3), 0xB);
    const std::size_t before = proposals(g.t);
    for (int i = 0; i < 200 && proposals(g.t) == before; ++i) {
        (void)fetch(2, 10, 5, 1, 0x2);
        g.r->doWork(g.now += 1'000'000);
    }
    ASSERT_GT(proposals(g.t), before) << "no proposal after the new process";
    const AlterIsrMsg& p = lastProposal(g.t);
    std::vector<int32_t> members(p.isr.begin(), p.isr.begin() + p.isrCount);
    std::sort(members.begin(), members.end());
    EXPECT_EQ(members, (std::vector<int32_t>{1, 2})) << "3 is unclean: below the HW with a new process";
    // A late frame of the dead process (storage 0xA, a HIGHER incarnation than 5).
    EXPECT_EQ(fetch(3, 10, 5, 901, 0xA), kErrStaleIncarnation);
    EXPECT_EQ(g.r->followerStorageForTest(3), 0xB);
}

// ISR v3 Step 6 (R4): a proposal captures each member's storage ONCE, at creation; later fetches carrying another
// storage change nothing in the resend.  RED: fill isrStorage from the slot at send time.
TEST(IsrProposalRegression, ProposalCapturesEachMembersStorageAtCreationRegression) {
    Rig g(0x5, 0xA);   // self storage 0x5; follower 3 fetches with 0xA
    const auto storageOf = [](const AlterIsrMsg& m, int32_t node) {
        for (int i = 0; i < m.isrCount; ++i) if (m.isr[static_cast<std::size_t>(i)] == node) return m.isrStorage[static_cast<std::size_t>(i)];
        return int64_t{-1};
    };
    const AlterIsrMsg p1 = lastProposal(g.t);
    ASSERT_EQ(p1.isrCount, 3);
    EXPECT_EQ(storageOf(p1, 1), 0x5);
    EXPECT_EQ(storageOf(p1, 3), 0xA);
    // 3 fetches under a NEW storage, at the HW (so it is not unclean).
    FetchRequestMsg f{};
    f.leaderEpoch = 5; f.partition = 0; f.followerId = 3; f.fetchOffset = 10; f.maxBytes = 0;
    f.topicId = 0; f.lastFetchedEpoch = 5; f.incarnation = 9; f.storageId = 0xB;
    EXPECT_NE(g.r->onFetchRequest(f, g.now), kErrStaleIncarnation);
    ASSERT_EQ(g.r->followerStorageForTest(3), 0xB);
    const std::size_t before = proposals(g.t);
    for (int i = 0; i < 400 && proposals(g.t) == before; ++i) g.r->doWork(g.now += 1'000'000);
    ASSERT_GT(proposals(g.t), before) << "no resend";
    const AlterIsrMsg& again = lastProposal(g.t);
    EXPECT_EQ(again.proposalId, p1.proposalId);
    EXPECT_EQ(storageOf(again, 3), 0xA) << "the resend re-read the slot instead of the capture";
}
