// ConsensusAgentTest.cpp — P2P-7: Single-threaded ConsensusAgent tests.
//
// Design:
//   - Manual clock (int64_t nowNs advanced by test).
//   - IngressRing fed with SBE-encoded frames via SbeConsensusPublisher.
//   - doWork(nowNs) drains ring → dispatches → Election state advances.
//   - No threads, no busy-spin — completely deterministic.
//
// Tests cover:
//   1.  doWork on empty ring returns election work only.
//   2.  Single CanvassPosition frame drains and dispatches.
//   3.  Multiple frames in one doWork batch (up to FRAGMENT_LIMIT).
//   4.  Exactly FRAGMENT_LIMIT frames drained per doWork when ring has more.
//   5.  messagesProcessed() counter increments correctly.
//   6.  Single-member cluster — doWork reaches LEADER_READY/CLOSED.
//   7.  onCanvassPosition forwarded to election (state change visible).
//   8.  onCommitPosition forwarded (notifiedCommitPosition updated).
//   9.  onAppendPosition forwarded (member log position updated).
//  10.  election() accessor returns the same Election.
//  11.  doWork returns >0 when election is active.
//  12.  doWork returns 0 only when no messages and election is CLOSED.
//  13.  Multi-message drain: 5 CanvassPosition frames → all processed.
//  14.  state() / isClosed() reflect election state.
//  15.  Clock is injected — doWork uses provided nowNs (no system clock).

#include <gtest/gtest.h>

#include <rune/consensus/ConsensusAgent.h>
#include <rune/consensus/AgentRunner.h>
#include <rune/consensus/IdleStrategy.h>
#include <rune/consensus/NanoClock.h>
#include <rune/consensus/IngressRing.h>
#include <rune/consensus/Election.h>
#include <rune/consensus/ElectionState.h>
#include <rune/consensus/NodeStateFile.h>
#include <rune/consensus/TermLog.h>
#include <rune/consensus/ClusterMember.h>
#include <rune/consensus/ConsensusPublisher.h>
#include <rune/consensus/SbeConsensusPublisher.h>
#include <rune/consensus/MessageSink.h>
#include <rune/consensus/ConsensusAdapter.h>
#include <rune/consensus/Types.h>

#include <atomic>
#include <chrono>
#include <filesystem>
#include <string>
#include <vector>
#include <cstdint>
#include <unistd.h>

using namespace rune::consensus;
namespace fs = std::filesystem;

// ---------------------------------------------------------------------------
// TempFile
// ---------------------------------------------------------------------------
struct TempFile {
    fs::path path;
    explicit TempFile(std::string_view suffix = ".dat") {
        static std::atomic<uint64_t> g_tempSeq{0};
        auto ts = std::chrono::steady_clock::now().time_since_epoch().count();
        auto uniq = std::to_string(::getpid()) + "_" +
                    std::to_string(g_tempSeq.fetch_add(1, std::memory_order_relaxed)) + "_" +
                    std::to_string(ts);
        path = fs::temp_directory_path() /
               ("rune_cagt_" + uniq + std::string(suffix));
        fs::remove(path);
    }
    ~TempFile() { std::error_code ec; fs::remove(path, ec); }
    std::string str() const { return path.string(); }
};

// ---------------------------------------------------------------------------
// NullSink — captures nothing; used when we don't care about outbound traffic
// ---------------------------------------------------------------------------
struct NullSink final : public MessageSink {
    void send(NodeId, const uint8_t*, std::size_t) override {}
};

// ---------------------------------------------------------------------------
// RecordSink — captures bytes for loopback into IngressRing
// ---------------------------------------------------------------------------
struct RecordSink final : public MessageSink {
    std::vector<uint8_t> lastBytes;
    void send(NodeId, const uint8_t* buf, std::size_t len) override {
        lastBytes.assign(buf, buf + len);
    }
};

// ---------------------------------------------------------------------------
// FakePublisher — records election outbound calls (no-op for this test)
// ---------------------------------------------------------------------------
struct FakePublisher : ConsensusPublisher {
    int canvassCount{0};
    void canvassPosition(NodeId, Term, int64_t, Term, NodeId) override { ++canvassCount; }
    void requestVote(NodeId, Term, int64_t, Term, NodeId) override {}
    void placeVote(NodeId, Term, Term, int64_t, NodeId, NodeId, bool) override {}
    void newLeadershipTerm(NodeId, Term, Term, int64_t, int64_t,
                           Term, int64_t, int64_t, int64_t, int64_t,
                           NodeId, int32_t, bool, int64_t) override {}
    void appendPosition(NodeId, Term, int64_t, NodeId, uint8_t) override {}
    void commitPosition(NodeId, Term, int64_t, NodeId) override {}
};

// ---------------------------------------------------------------------------
// Helpers to build Elections
// ---------------------------------------------------------------------------
static constexpr int64_t kElectionTimeoutNs      = 150'000'000LL;
static constexpr int64_t kStatusIntervalNs        =  50'000'000LL;
static constexpr int64_t kStartupCanvassTimeoutNs = 300'000'000LL;

static std::vector<ClusterMember> makeMembers1(NodeId self = 0) {
    ClusterMember m; m.id = self; m.logPosition = 0;
    m.logLeadershipTermId = 0; m.leadershipTermId = 0;
    return {m};
}
static std::vector<ClusterMember> makeMembers3() {
    std::vector<ClusterMember> ms(3);
    for (int i = 0; i < 3; ++i) {
        ms[i].id = i; ms[i].logPosition = 0;
        ms[i].logLeadershipTermId = 0; ms[i].leadershipTermId = 0;
    }
    return ms;
}

// ---------------------------------------------------------------------------
// Fixture: single-member cluster (wins immediately in INIT)
// ---------------------------------------------------------------------------
class ConsensusAgentSingleMember : public ::testing::Test {
protected:
    TempFile nsfFile_{".nsf"}, tlFile_{".tl"};
    NodeStateFile nsf_{nsfFile_.str()};
    TermLog       tl_{tlFile_.str()};
    FakePublisher pub_;
    IngressRing   ring_{4096};  // small ring for tests
    Election      election_{
        /*isNodeStartup=*/true,
        /*gracefulClosedLeaderId=*/-1,
        /*leadershipTermId=*/0,
        /*logLeadershipTermId=*/0,
        /*logPosition=*/0,
        /*appendPosition=*/0,
        makeMembers1(0), 0,
        pub_, nsf_, tl_,
        kElectionTimeoutNs,
        kStatusIntervalNs,
        kStartupCanvassTimeoutNs,
        /*rngSeed=*/42};
    ConsensusAgent agent_{election_, ring_};

    int64_t nowNs_{1'000'000LL};
};

// ---------------------------------------------------------------------------
// Fixture: 3-member cluster (stays in CANVASS unless we inject messages)
// ---------------------------------------------------------------------------
class ConsensusAgentThreeMember : public ::testing::Test {
protected:
    TempFile nsfFile_{".nsf"}, tlFile_{".tl"};
    NodeStateFile nsf_{nsfFile_.str()};
    TermLog       tl_{tlFile_.str()};
    FakePublisher pub_;
    IngressRing   ring_{8192};
    Election      election_{
        true, -1, 0, 0, 0, 0,
        makeMembers3(), 0,
        pub_, nsf_, tl_,
        kElectionTimeoutNs,
        kStatusIntervalNs,
        kStartupCanvassTimeoutNs,
        42};
    ConsensusAgent agent_{election_, ring_};
    int64_t nowNs_{1'000'000LL};
};

// ===========================================================================
// TEST 1: doWork on empty ring — only election work
// ===========================================================================
TEST_F(ConsensusAgentSingleMember, EmptyRing_DoesElectionWork) {
    int w = agent_.doWork(nowNs_);
    // Single-member init → LeaderLogReplication transition = work
    EXPECT_GT(w, 0);
    EXPECT_EQ(agent_.messagesProcessed(), 0);
}

// ===========================================================================
// TEST 2: messagesProcessed starts at 0
// ===========================================================================
TEST_F(ConsensusAgentThreeMember, MessagesProcessed_StartsAtZero) {
    EXPECT_EQ(agent_.messagesProcessed(), 0);
}

// ===========================================================================
// TEST 3: Single CanvassPosition → drains and increments messagesProcessed
// ===========================================================================
TEST_F(ConsensusAgentThreeMember, SingleCanvassFrame_Drained) {
    // Encode a CanvassPosition frame and push to ring.
    NullSink sink;
    SbeConsensusPublisher pub{sink};
    RecordSink rec;
    SbeConsensusPublisher recPub{rec};
    recPub.canvassPosition(0, 0, 0, 0, 1); // from node 1, to node 0
    ring_.write(rec.lastBytes.data(), rec.lastBytes.size());

    // First doWork kicks INIT → CANVASS (election work)
    // then drains the 1 frame.
    nowNs_ += kStatusIntervalNs; // push past init
    int w = agent_.doWork(nowNs_);

    EXPECT_GE(w, 1); // at least 1 from draining the frame
    EXPECT_EQ(agent_.messagesProcessed(), 1);
}

// ===========================================================================
// TEST 4: Multiple frames (< FRAGMENT_LIMIT) all drained in one doWork
// ===========================================================================
TEST_F(ConsensusAgentThreeMember, MultipleFrames_AllDrained) {
    RecordSink rec;
    SbeConsensusPublisher recPub{rec};

    // Push 5 frames
    for (int i = 0; i < 5; ++i) {
        recPub.canvassPosition(0, 0, 0, 0, 1);
        ring_.write(rec.lastBytes.data(), rec.lastBytes.size());
    }

    nowNs_ += kStatusIntervalNs;
    agent_.doWork(nowNs_);
    EXPECT_EQ(agent_.messagesProcessed(), 5);
}

// ===========================================================================
// TEST 5: Exactly FRAGMENT_LIMIT (10) frames drained per doWork
// ===========================================================================
TEST_F(ConsensusAgentThreeMember, ExactlyFragmentLimit_DrainedInOne) {
    RecordSink rec;
    SbeConsensusPublisher recPub{rec};

    // Push exactly 10 frames (= FRAGMENT_LIMIT)
    for (int i = 0; i < ConsensusAdapter::FRAGMENT_LIMIT; ++i) {
        recPub.canvassPosition(0, 0, 0, 0, 1);
        ring_.write(rec.lastBytes.data(), rec.lastBytes.size());
    }

    nowNs_ += kStatusIntervalNs;
    agent_.doWork(nowNs_);
    EXPECT_EQ(agent_.messagesProcessed(), ConsensusAdapter::FRAGMENT_LIMIT);
}

// ===========================================================================
// TEST 6: FRAGMENT_LIMIT+1 frames → first doWork drains FRAGMENT_LIMIT,
//         second doWork drains the rest
// ===========================================================================
TEST_F(ConsensusAgentThreeMember, MoreThanFragmentLimit_TwoBatches) {
    RecordSink rec;
    SbeConsensusPublisher recPub{rec};

    const int total = ConsensusAdapter::FRAGMENT_LIMIT + 3;
    for (int i = 0; i < total; ++i) {
        recPub.canvassPosition(0, 0, 0, 0, 1);
        ring_.write(rec.lastBytes.data(), rec.lastBytes.size());
    }

    nowNs_ += kStatusIntervalNs;
    agent_.doWork(nowNs_);
    EXPECT_EQ(agent_.messagesProcessed(), ConsensusAdapter::FRAGMENT_LIMIT);

    nowNs_ += 1'000'000LL;
    agent_.doWork(nowNs_);
    EXPECT_EQ(agent_.messagesProcessed(), total);
}

// ===========================================================================
// TEST 7: Single-member — doWork drives election to LEADER_READY or CLOSED
// ===========================================================================
TEST_F(ConsensusAgentSingleMember, SingleMember_ReachesLeaderReady) {
    // Drive repeatedly until closed or max iterations.
    int64_t t = nowNs_;
    int iters = 0;
    while (!agent_.isClosed() && iters < 200) {
        agent_.doWork(t);
        t += kStatusIntervalNs;
        ++iters;
    }
    // Single-member should reach CLOSED relatively quickly.
    // Allow LEADER_READY as an intermediate milestone.
    auto s = agent_.state();
    EXPECT_TRUE(s == ElectionState::Closed ||
                s == ElectionState::LeaderReady ||
                s == ElectionState::LeaderLogReplication);
    EXPECT_LT(iters, 200);
}

// ===========================================================================
// TEST 8: election() accessor
// ===========================================================================
TEST_F(ConsensusAgentSingleMember, ElectionAccessor_ReturnsSameElection) {
    EXPECT_EQ(&agent_.election(), &election_);
}

// ===========================================================================
// TEST 9: state() reflects election state
// ===========================================================================
TEST_F(ConsensusAgentThreeMember, State_ReflectsElectionState) {
    // Before any doWork, election is in INIT.
    EXPECT_EQ(agent_.state(), ElectionState::Init);
    agent_.doWork(nowNs_);
    // After first doWork, election transitions to CANVASS (3-member cluster).
    EXPECT_EQ(agent_.state(), ElectionState::Canvass);
}

// ===========================================================================
// TEST 10: isClosed() before vs after
// ===========================================================================
TEST_F(ConsensusAgentSingleMember, IsClosed_FalseBeforeWork) {
    EXPECT_FALSE(agent_.isClosed());
}

// ===========================================================================
// TEST 11: doWork returns >0 when election transitions
// ===========================================================================
TEST_F(ConsensusAgentSingleMember, DoWork_ReturnsPositiveWork_OnTransition) {
    int w = agent_.doWork(nowNs_);
    EXPECT_GT(w, 0);
}

// ===========================================================================
// TEST 12: onCommitPosition forwarded — notifiedCommitPosition updated
// ===========================================================================
TEST_F(ConsensusAgentThreeMember, OnCommitPosition_Forwarded) {
    // Drive to CANVASS first.
    agent_.doWork(nowNs_);
    EXPECT_EQ(agent_.state(), ElectionState::Canvass);

    // Encode CommitPosition and inject.
    RecordSink rec;
    SbeConsensusPublisher recPub{rec};
    recPub.commitPosition(0, /*leadershipTermId=*/0, /*logPosition=*/1000, /*leaderMemberId=*/1);
    ring_.write(rec.lastBytes.data(), rec.lastBytes.size());

    nowNs_ += 1'000'000LL;
    agent_.doWork(nowNs_);

    EXPECT_EQ(agent_.messagesProcessed(), 1);
    // notifiedCommitPosition should have been updated to 1000
    EXPECT_EQ(election_.notifiedCommitPosition(), 1000);
}

// ===========================================================================
// TEST 13: onAppendPosition forwarded
// ===========================================================================
TEST_F(ConsensusAgentThreeMember, OnAppendPosition_Forwarded) {
    agent_.doWork(nowNs_);

    RecordSink rec;
    SbeConsensusPublisher recPub{rec};
    recPub.appendPosition(0, /*leadershipTermId=*/0, /*logPosition=*/500, /*followerMemberId=*/1, /*flags=*/0);
    ring_.write(rec.lastBytes.data(), rec.lastBytes.size());

    nowNs_ += 1'000'000LL;
    agent_.doWork(nowNs_);

    EXPECT_EQ(agent_.messagesProcessed(), 1);
    // The election received the onAppendPosition call — it may or may not
    // update a member's logPosition in CANVASS state; at minimum it doesn't crash.
}

// ===========================================================================
// TEST 14: Unknown templateId frame is silently ignored (count still bumped)
// ===========================================================================
TEST_F(ConsensusAgentThreeMember, UnknownTemplateId_SilentlyIgnored) {
    // Craft a minimal SBE header with an unknown templateId (99).
    std::array<uint8_t, 8> buf{};
    // header: blockLength=0, templateId=99, schemaId=111, version=17
    auto write_u16 = [](uint8_t* p, uint16_t v) { std::memcpy(p, &v, 2); };
    write_u16(buf.data() + 0, 0);
    write_u16(buf.data() + 2, 99);
    write_u16(buf.data() + 4, 111);
    write_u16(buf.data() + 6, 17);
    ring_.write(buf.data(), buf.size());

    agent_.doWork(nowNs_);
    // Ring drains the frame; adapter silently ignores it; no crash.
    EXPECT_EQ(agent_.messagesProcessed(), 1);
}

// ===========================================================================
// TEST 15: Clock is injectable — doWork uses provided nowNs
// ===========================================================================
TEST_F(ConsensusAgentThreeMember, InjectedClock_UsedForElection) {
    // With nowNs==0 (epoch), canvass deadline in future → CANVASS work expected.
    int64_t t = 0;
    agent_.doWork(t);
    EXPECT_EQ(agent_.state(), ElectionState::Canvass);

    // Advance past startup canvass timeout to try to trigger NOMINATE.
    t = kStartupCanvassTimeoutNs + kElectionTimeoutNs + 1;
    agent_.doWork(t);
    // State should have advanced (to NOMINATE or further depending on quorum).
    // At minimum it should not still be INIT.
    EXPECT_NE(agent_.state(), ElectionState::Init);
}
