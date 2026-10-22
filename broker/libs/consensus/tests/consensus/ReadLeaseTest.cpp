// ReadLeaseTest.cpp — issue #82 (VLS Part A): the controller-granted, quorum-capped view read lease.
//
//   ReadLeaseRegression.*            the pure state machines (ReadLease.h): the drift math, the grantor's QL from the
//                                    members' echoes, the holder's lease end and its commitPos-gated delivery.
//   ReadLeaseModelRegression.*       a seeded model over drift extremes and delays: a broker's lease always ends (real
//                                    time) before the earliest moment a new controller could be elected (C4).
//   ElectionReadLeaseRegression.*    the Election wiring: requests ride the follower's AppendPosition, the CLOSED leader
//                                    answers them and leases itself, a grant is a leader heartbeat, a member holding a
//                                    lease is never judged dead.
#include <rune/consensus/ClusterMember.h>
#include <rune/consensus/ConsensusPublisher.h>
#include <rune/consensus/Election.h>
#include <rune/consensus/NodeStateFile.h>
#include <rune/consensus/ReadLease.h>
#include <rune/consensus/TermLog.h>

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <random>
#include <string>
#include <vector>
#include <unistd.h>

using namespace rune::consensus;
namespace fs = std::filesystem;

namespace {

constexpr int64_t kMs = 1'000'000;
constexpr int64_t kH  = 150 * kMs;   // the stickiness window (leaderHeartbeatTimeoutNs at the defaults)

// =====================================================================================================================
// Pure state machines
// =====================================================================================================================
}  // namespace

TEST(ReadLeaseRegression, DriftMathShrinksAndGrowsByOnePlusRhoSquaredRegression) {
    EXPECT_EQ(leaseShrinkByDrift(0), 0);
    EXPECT_EQ(leaseShrinkByDrift(-5), 0);
    EXPECT_EQ(leaseShrinkByDrift(10'201), 10'000);
    EXPECT_EQ(leaseShrinkByDrift(150 * kMs), 147'044'407);   // floor(150e6 * 10000 / 10201)
    EXPECT_EQ(leaseGrowByDrift(10'000), 10'201);
    EXPECT_EQ(leaseGrowByDrift(150 * kMs), 153'015'000);
    EXPECT_EQ(kReadLeaseFenceWaitNs, leaseGrowByDrift(kReadLeaseMaxNs)) << "#491: W from the wire cap L, never a node's own H";
    EXPECT_EQ(kReadLeaseFenceWaitNs, 204'020'000);
    // No overflow at the extremes, and shrink(grow(x)) <= x <= grow(shrink(x)) + rounding.
    EXPECT_GT(leaseShrinkByDrift(INT64_MAX), 0);
    for (int64_t x : {int64_t{1}, int64_t{7}, 10'000 * kMs, 123'456'789'012LL}) {
        EXPECT_LE(leaseShrinkByDrift(leaseGrowByDrift(x)), x);
        EXPECT_GE(leaseGrowByDrift(leaseShrinkByDrift(x)) + 2, x);
    }
}

TEST(ReadLeaseRegression, GrantorGivesNoLeaseUntilAMajorityEchoedAGrantRegression) {
    ReadLeaseGrantor g;
    const NodeId members[] = {1, 2, 3};
    g.configure(members, /*self=*/1, kH);
    int64_t now = 1'000 * kMs;
    // First requests: nobody heard a grant yet -> t* is "never" -> no lease (the echo still starts).
    const auto g2 = g.onRequest(2, /*heardGrantSeq=*/0, now);
    EXPECT_GT(g2.grantSeq, 0);
    EXPECT_EQ(g2.validForNs, 0) << "no member echoed a grant: QL is unbounded below, nothing is granted";
    EXPECT_EQ(g.validForNs(now), 0);
    // Member 2 echoes g2 (sent at `now`) 20 ms later: {self = now', 2 = now, 3 = never} -> t* = now (2nd largest).
    now += 20 * kMs;
    const auto g2b = g.onRequest(2, g2.grantSeq, now);
    EXPECT_EQ(g.echoSendNsForTest(2), now - 20 * kMs);
    EXPECT_EQ(g2b.validForNs, leaseShrinkByDrift(kH) - 20 * kMs) << "QL - now = t* + H/(1+rho)^2 - now";
    EXPECT_LE(g2b.validForNs, kReadLeaseMaxNs);
    EXPECT_TRUE(g.leaseMayRun(2, now + g2b.validForNs - 1));
    EXPECT_FALSE(g.leaseMayRun(2, now + g2b.validForNs));
    EXPECT_FALSE(g.leaseMayRun(3, now)) << "member 3 never got a lease";
    // The controller's own lease follows the same QL.
    EXPECT_EQ(g.selfValidForNs(now), g2b.validForNs);
}

TEST(ReadLeaseRegression, GrantorLeaseDecaysToZeroWhenTheMajorityStopsEchoingRegression) {
    ReadLeaseGrantor g;
    const NodeId members[] = {1, 2, 3};
    g.configure(members, 1, kH);
    int64_t now = 5'000 * kMs;
    int64_t heard2 = 0, heard3 = 0;
    for (int i = 0; i < 10; ++i, now += 20 * kMs) {   // steady state: both followers echo
        heard2 = g.onRequest(2, heard2, now).grantSeq;
        heard3 = g.onRequest(3, heard3, now).grantSeq;
    }
    const int64_t steady = g.validForNs(now);
    EXPECT_GT(steady, 100 * kMs);
    // The controller is cut off (no echo arrives): QL is frozen at the last echoed send time, the lease shrinks with
    // every tick and reaches 0 no later than t* + H/(1+rho)^2.
    const int64_t tStar = g.echoSendNsForTest(2) > g.echoSendNsForTest(3) ? g.echoSendNsForTest(2) : g.echoSendNsForTest(3);
    for (int64_t t = now; t < tStar + leaseShrinkByDrift(kH); t += 5 * kMs) EXPECT_EQ(g.validForNs(t), tStar + leaseShrinkByDrift(kH) - t);
    EXPECT_EQ(g.validForNs(tStar + leaseShrinkByDrift(kH)), 0);
    EXPECT_EQ(g.selfValidForNs(tStar + leaseShrinkByDrift(kH) + 1), 0) << "a partitioned controller stops leasing itself";
}

TEST(ReadLeaseRegression, GrantorIgnoresUnknownAndStaleEchoesRegression) {
    ReadLeaseGrantor g;
    const NodeId members[] = {1, 2, 3};
    g.configure(members, 1, kH);
    const int64_t now = 10 * kMs;
    EXPECT_EQ(g.onRequest(9, 0, now).grantSeq, -1) << "not a member";
    EXPECT_EQ(g.onRequest(1, 0, now).grantSeq, -1) << "self never requests";
    const auto a = g.onRequest(2, 0, now);
    // An echo of a sequence this member was never sent (another member's) is not evidence.
    const auto b = g.onRequest(3, a.grantSeq, now + kMs);
    EXPECT_EQ(g.echoSendNsForTest(3), ReadLeaseGrantor::kNever);
    // A new leadership forgets the echoes (they were for the previous one); sequences keep counting.
    (void)g.onRequest(2, a.grantSeq, now + 2 * kMs);
    EXPECT_NE(g.echoSendNsForTest(2), ReadLeaseGrantor::kNever);
    g.onLeadership();
    EXPECT_EQ(g.echoSendNsForTest(2), ReadLeaseGrantor::kNever);
    EXPECT_EQ(g.validForNs(now + 3 * kMs), 0);
    EXPECT_GT(g.onRequest(2, b.grantSeq, now + 4 * kMs).grantSeq, b.grantSeq);
}

TEST(ReadLeaseRegression, HolderLeaseStartsAtTheRequestSendTimeAndIsShrunkByDriftRegression) {
    ReadLeaseHolder h;
    const auto rq = h.nextRequest(/*sendNs=*/1'000 * kMs);
    EXPECT_EQ(rq.heardGrantSeq, 0);
    EXPECT_FALSE(h.onGrant(rq.reqSeq + 1, 5, 0, 100 * kMs)) << "an unknown request";
    EXPECT_EQ(h.latestEndNs(), ReadLeaseHolder::kNone);
    EXPECT_TRUE(h.onGrant(rq.reqSeq, /*grantSeq=*/5, /*commitPos=*/42, 100 * kMs));
    EXPECT_EQ(h.heardGrantSeq(), 5) << "echoed by the next request";
    EXPECT_EQ(h.nextRequest(1'020 * kMs).heardGrantSeq, 5);
    EXPECT_EQ(h.latestEndNs(), 1'000 * kMs + leaseShrinkByDrift(100 * kMs));
    // A grant above the broker's own L is capped there.
    const auto rq2 = h.nextRequest(2'000 * kMs);
    EXPECT_TRUE(h.onGrant(rq2.reqSeq, 6, 42, 10'000 * kMs));
    EXPECT_EQ(h.latestEndNs(), 2'000 * kMs + leaseShrinkByDrift(kReadLeaseMaxNs));
    // A zero lease is heard (echo) but grants nothing.
    const auto rq3 = h.nextRequest(3'000 * kMs);
    EXPECT_TRUE(h.onGrant(rq3.reqSeq, 7, 42, 0));
    EXPECT_EQ(h.heardGrantSeq(), 7);
    EXPECT_EQ(h.latestEndNs(), 2'000 * kMs + leaseShrinkByDrift(kReadLeaseMaxNs));
    h.onNewLeader();
    EXPECT_EQ(h.nextRequest(3'100 * kMs).heardGrantSeq, 0) << "a new leader's grant sequences are its own";
}

TEST(ReadLeaseRegression, HolderDeliversOnlyLeasesWhoseCommitPosIsAppliedRegression) {
    ReadLeaseHolder h;
    int64_t t = 0;
    auto grant = [&](int64_t commitPos, int64_t validFor) {
        const auto rq = h.nextRequest(t);
        ASSERT_TRUE(h.onGrant(rq.reqSeq, rq.reqSeq, commitPos, validFor));
        t += 20 * kMs;
    };
    grant(10, 100 * kMs);   // end e1
    grant(20, 100 * kMs);   // end e2 > e1
    grant(30, 100 * kMs);   // end e3 > e2
    const int64_t e1 = leaseShrinkByDrift(100 * kMs);
    const int64_t e2 = 20 * kMs + e1, e3 = 40 * kMs + e1;
    EXPECT_EQ(h.deliverableEndNs(9), ReadLeaseHolder::kNone) << "a grant issued at metadata 10 needs 10 applied";
    EXPECT_EQ(h.deliverableEndNs(10), e1);
    EXPECT_EQ(h.deliverableEndNs(25), e2) << "the newest lease the applied metadata covers - never a newer one";
    EXPECT_EQ(h.deliverableEndNs(30), e3);
    // Under steady metadata churn the newest grant is always slightly ahead of the applied position: the older grants
    // in the ring still deliver (no availability loss from conflation).
    for (int i = 0; i < 20; ++i) grant(100 + i, 100 * kMs);
    EXPECT_GT(h.deliverableEndNs(100 + 18), ReadLeaseHolder::kNone);
    EXPECT_EQ(h.deliverableEndNs(99), ReadLeaseHolder::kNone);
}

// =====================================================================================================================
// The model: drift extremes x delays x echo ages.  Real time is the test's; each node's lease clock runs at rate r
// in [1/(1+rho), 1+rho] with its own offset.  A follower that HEARD a grant at real time tau denies every vote until its
// own clock has advanced H, i.e. until tau + H / r_f >= tau + H / (1+rho): no new controller can exist before
//     newControllerEarliest = (send time of the latest grant a majority echoed) + H / (1+rho)   (the majority includes
// the controller, which counts "now").  The broker's lease (its own clock) must end, in real time, before that.
// =====================================================================================================================
TEST(ReadLeaseModelRegression, ABrokersLeaseAlwaysEndsBeforeAnyNewControllerCanExistRegression) {
    std::mt19937_64 rng(0x82'82'82);
    const double rho = 0.01;
    std::uniform_int_distribution<int> pick(0, 2);
    std::uniform_real_distribution<double> unit(0.0, 1.0);
    const double rates[] = {1.0 / (1.0 + rho), 1.0, 1.0 + rho};
    int checked = 0, granted = 0;
    for (int trial = 0; trial < 20'000; ++trial) {
        const double rc = rates[pick(rng)], rb = rates[pick(rng)];
        const double cOff = unit(rng) * 1e12, bOff = unit(rng) * 1e12;
        auto C = [&](double tau) { return static_cast<int64_t>(cOff + rc * tau); };
        auto B = [&](double tau) { return static_cast<int64_t>(bOff + rb * tau); };
        ReadLeaseGrantor g;
        const NodeId members[] = {1, 2, 3};   // 1 = controller, 2 = the broker, 3 = silent
        g.configure(members, 1, kH);
        ReadLeaseHolder h;
        // A grant to the broker at real tau0 (its echo arrives with the next request).
        double tau = 1e9 + unit(rng) * 1e9;
        const auto rq0 = h.nextRequest(B(tau));
        const double d0 = unit(rng) * 30e6;                  // request one-way delay
        const auto gr0 = g.onRequest(2, rq0.heardGrantSeq, C(tau + d0));
        const double tauGrant0 = tau + d0;                   // the echoed grant's real send time
        const double d1 = unit(rng) * 30e6;                  // grant one-way delay
        (void)h.onGrant(rq0.reqSeq, gr0.grantSeq, 0, gr0.validForNs);
        // The next request leaves after a random gap (the echo age), the grant answers it after a delay.
        const double sendTau = tauGrant0 + d1 + unit(rng) * 160e6;
        const auto rq1 = h.nextRequest(B(sendTau));
        const double recvTau = sendTau + unit(rng) * 30e6;
        const auto gr1 = g.onRequest(2, rq1.heardGrantSeq, C(recvTau));
        (void)h.onGrant(rq1.reqSeq, gr1.grantSeq, 0, gr1.validForNs);
        ++checked;
        if (gr1.validForNs <= 0) continue;
        ++granted;
        // The broker's lease end (its clock) back in real time.
        const int64_t endLocal = h.latestEndNs();
        const double endTau = sendTau + static_cast<double>(endLocal - B(sendTau)) / rb;
        const double newControllerEarliest = tauGrant0 + static_cast<double>(kH) / (1.0 + rho);
        ASSERT_LE(endTau, newControllerEarliest + 2.0)   // 2 ns: integer rounding of the model's clocks
            << "trial " << trial << " rc=" << rc << " rb=" << rb << " echo age " << (recvTau - tauGrant0) / 1e6 << " ms";
        // The controller's own record of the grant ends no earlier than the broker's lease (eligibility is safe).
        const double ctrlEndTau = recvTau + static_cast<double>(gr1.validForNs) / rc;
        ASSERT_GE(ctrlEndTau + 2.0, endTau) << "trial " << trial;
    }
    EXPECT_EQ(checked, 20'000);
    EXPECT_GT(granted, 5'000) << "the model must actually grant leases (vacuity guard)";
}

// =====================================================================================================================
// Election wiring
// =====================================================================================================================
namespace {

struct TempFile {
    fs::path path;
    explicit TempFile(std::string_view suffix) {
        static std::atomic<uint64_t> seq{0};
        path = fs::temp_directory_path() /
               ("rune_rl_" + std::to_string(::getpid()) + "_" + std::to_string(seq.fetch_add(1)) + std::string(suffix));
        fs::remove(path);
    }
    ~TempFile() { std::error_code ec; fs::remove(path, ec); }
    std::string str() const { return path.string(); }
};

struct ReadLeaseFakePublisher : ConsensusPublisher {   // unique name (ODR): see ElectionLogTest
    struct Req   { NodeId to; Term term; NodeId member; int64_t reqSeq; int64_t heard; };
    struct GrantC { NodeId to; Term term; NodeId leader; NodeId member; int64_t reqSeq; int64_t grantSeq; int64_t commitPos;
                    int64_t validFor; };
    std::vector<Req>    requests;
    std::vector<GrantC> grants;
    int appendPositions = 0;
    void canvassPosition(NodeId, Term, int64_t, Term, NodeId) override {}
    void requestVote(NodeId, Term, int64_t, Term, NodeId) override {}
    void placeVote(NodeId, Term, Term, int64_t, NodeId, NodeId, bool) override {}
    void newLeadershipTerm(NodeId, Term, Term, int64_t, int64_t, Term, int64_t, int64_t, int64_t, int64_t, NodeId, int32_t,
                           bool, int64_t) override {}
    void appendPosition(NodeId, Term, int64_t, NodeId, uint8_t) override { ++appendPositions; }
    void commitPosition(NodeId, Term, int64_t, NodeId) override {}
    void readLeaseRequest(NodeId to, Term term, NodeId member, int64_t reqSeq, int64_t heard) override {
        requests.push_back({to, term, member, reqSeq, heard});
    }
    void readLeaseGrant(NodeId to, Term term, NodeId leader, NodeId member, int64_t reqSeq, int64_t grantSeq,
                        int64_t commitPos, int64_t validFor) override {
        grants.push_back({to, term, leader, member, reqSeq, grantSeq, commitPos, validFor});
    }
};

constexpr int64_t kElectionTimeoutNs = 150 * kMs;
constexpr int64_t kStatusIntervalNs  = 20 * kMs;

std::vector<ClusterMember> members3() {
    std::vector<ClusterMember> ms(3);
    for (int i = 0; i < 3; ++i) ms[static_cast<std::size_t>(i)].id = i;
    return ms;
}

struct Rig {
    TempFile nsfFile{".nsf"}, tlFile{".tl"};
    NodeStateFile nsf{nsfFile.str()};
    TermLog tl{tlFile.str()};
    ReadLeaseFakePublisher pub;
    int64_t leaseNow = 7'000 * kMs;   // the injected lease clock (independent of the duty clock)
    int64_t commitPos = 1234;
    Election e;
    explicit Rig(NodeId self)
        : e(true, -1, 0, 0, 0, 0, members3(), self, pub, nsf, tl, kElectionTimeoutNs, kStatusIntervalNs,
            /*startupCanvass*/ 300 * kMs, /*seed*/ 0) {
        e.setLeaseClockForTest([](void* c) noexcept { return *static_cast<int64_t*>(c); }, &leaseNow);
        e.setReadLeaseCommitPosSource([](void* c) noexcept { return *static_cast<int64_t*>(c); }, &commitPos);
    }
};

/// Node 0 wins term 1 and reaches CLOSED as the leader (peers 1, 2 vote and append).
void leaderClosed(Rig& r, int64_t& now) {
    Election& e = r.e;
    e.doWork(now);
    e.onCanvassPosition(0, 0, 0, 1);
    e.onCanvassPosition(0, 0, 0, 2);
    now += 300 * kMs + 1;
    e.doWork(now);
    now += kElectionTimeoutNs * 10;
    e.doWork(now);
    if (e.preBallotActiveForTest()) { for (NodeId v = 0; v < 3; ++v) e.onPreVoteResponse(e.preBallotTermForTest(), v, true); e.doWork(now); }
    now += 1;
    e.doWork(now);
    const Term t = e.candidateTermId();
    e.onVote(0, 0, t, 0, 1, true);
    e.onVote(0, 0, t, 0, 2, true);
    for (int i = 0; i < 100 && !e.isClosed(); ++i) {
        now += 1 * kMs;
        e.doWork(now);
        e.onAppendPosition(now, e.leadershipTermId(), e.appendPosition(), 1, 0);
        e.onAppendPosition(now, e.leadershipTermId(), e.appendPosition(), 2, 0);
    }
    ASSERT_TRUE(e.isClosed());
    ASSERT_EQ(e.leaderMemberId(), 0);
}

/// Node 0 follows leader 1 at term 1, CLOSED.
void followerClosed(Rig& r, int64_t& now) {
    Election& e = r.e;
    e.doWork(now);
    e.onRequestVote(0, 0, 1, 1, now);
    e.onNewLeadershipTerm(now, 0, 1, 0, 0, 1, 0, 0, 0, 0, 1, 0, true, 0);
    for (int i = 0; i < 100 && !e.isClosed(); ++i) { now += kMs; e.doWork(now); }
    ASSERT_TRUE(e.isClosed());
    ASSERT_EQ(e.leaderMemberId(), 1);
}

}  // namespace

TEST(ElectionReadLeaseRegression, AClosedFollowerSendsALeaseRequestWithEveryAppendPositionRegression) {
    Rig r(0);
    int64_t now = 1'000 * kMs;
    ASSERT_NO_FATAL_FAILURE(followerClosed(r, now));
    r.pub.requests.clear();
    r.pub.appendPositions = 0;
    for (int i = 0; i < 5; ++i) {
        now += kStatusIntervalNs;
        r.e.onCommitPosition(now, 1, 0, 1);   // the leader keeps heartbeating
        r.e.doWork(now);
    }
    ASSERT_EQ(r.pub.appendPositions, 5);
    ASSERT_EQ(r.pub.requests.size(), 5u) << "one lease request per AppendPosition";
    for (const auto& q : r.pub.requests) {
        EXPECT_EQ(q.to, 1);
        EXPECT_EQ(q.term, r.e.leadershipTermId());
        EXPECT_EQ(q.member, 0);
    }
    EXPECT_LT(r.pub.requests[0].reqSeq, r.pub.requests[4].reqSeq);
}

TEST(ElectionReadLeaseRegression, TheClosedLeaderGrantsQuorumCappedLeasesAndLeasesItselfRegression) {
    Rig r(0);
    int64_t now = 1'000 * kMs;
    ASSERT_NO_FATAL_FAILURE(leaderClosed(r, now));
    const Term term = r.e.leadershipTermId();
    // Wrong term / unknown member / self: no grant.
    r.e.onReadLeaseRequest(now, term + 1, 1, 1, 0);
    r.e.onReadLeaseRequest(now, term, 7, 1, 0);
    r.e.onReadLeaseRequest(now, term, 0, 1, 0);
    EXPECT_TRUE(r.pub.grants.empty());
    // Two rounds: the first grants nothing (no echo yet), the second (echoing the first) a QL-capped lease.
    r.e.onReadLeaseRequest(now, term, 1, /*reqSeq*/ 11, /*heard*/ 0);
    ASSERT_EQ(r.pub.grants.size(), 1u);
    const auto first = r.pub.grants[0];
    EXPECT_EQ(first.to, 1);
    EXPECT_EQ(first.leader, 0);
    EXPECT_EQ(first.member, 1);
    EXPECT_EQ(first.reqSeq, 11);
    EXPECT_EQ(first.validFor, 0);
    EXPECT_EQ(first.commitPos, 1234) << "the commitPos source is read at grant time";
    r.leaseNow += 20 * kMs;
    r.commitPos = 1300;
    r.e.onReadLeaseRequest(now, term, 1, 12, first.grantSeq);
    ASSERT_EQ(r.pub.grants.size(), 2u);
    EXPECT_EQ(r.pub.grants[1].validFor, leaseShrinkByDrift(kElectionTimeoutNs) - 20 * kMs);
    EXPECT_EQ(r.pub.grants[1].commitPos, 1300);
    // The leader's own lease: renewed every heartbeat interval with the same cap.
    now += kStatusIntervalNs;
    r.e.doWork(now);
    EXPECT_EQ(r.e.readLease().latestEndNs(), r.leaseNow + leaseShrinkByDrift(leaseShrinkByDrift(kElectionTimeoutNs) - 20 * kMs));
    EXPECT_EQ(r.e.readLease().deliverableEndNs(1299), ReadLeaseHolder::kNone);
    EXPECT_EQ(r.e.readLease().deliverableEndNs(1300), r.e.readLease().latestEndNs());
}

TEST(ElectionReadLeaseRegression, AGrantFromTheFollowedLeaderLeasesAndHeartbeatsAnyOtherIsIgnoredRegression) {
    Rig r(0);
    int64_t now = 1'000 * kMs;
    ASSERT_NO_FATAL_FAILURE(followerClosed(r, now));
    now += kStatusIntervalNs;
    r.e.onCommitPosition(now, 1, 0, 1);
    r.e.doWork(now);
    ASSERT_FALSE(r.pub.requests.empty());
    const auto q = r.pub.requests.back();
    const Term term = r.e.leadershipTermId();
    // Not from the followed leader / another term / another recipient: ignored.
    r.e.onReadLeaseGrant(now, term, 2, 0, q.reqSeq, 5, 10, 100 * kMs);
    r.e.onReadLeaseGrant(now, term + 1, 1, 0, q.reqSeq, 5, 10, 100 * kMs);
    r.e.onReadLeaseGrant(now, term, 1, 2, q.reqSeq, 5, 10, 100 * kMs);
    EXPECT_EQ(r.e.readLease().latestEndNs(), ReadLeaseHolder::kNone);
    r.e.onReadLeaseGrant(now, term, 1, 0, q.reqSeq, 5, 10, 100 * kMs);
    EXPECT_EQ(r.e.readLease().latestEndNs(), r.leaseNow + leaseShrinkByDrift(100 * kMs));
    // A grant is a leader heartbeat: grants alone (no NLT / CommitPosition) keep the follower CLOSED past H.
    for (int i = 0; i < 20; ++i) {
        now += kStatusIntervalNs;
        r.e.doWork(now);
        const auto qq = r.pub.requests.back();
        r.e.onReadLeaseGrant(now, term, 1, 0, qq.reqSeq, 6 + i, 10, 100 * kMs);
    }
    EXPECT_TRUE(r.e.isClosed()) << "400 ms of grants and no other leader message: the leader is alive";
    EXPECT_EQ(r.e.leaderMemberId(), 1);
}

TEST(ElectionReadLeaseRegression, AMemberHoldingALeaseIsNeverJudgedDeadRegression) {
    Rig r(0);
    int64_t now = 1'000 * kMs;
    ASSERT_NO_FATAL_FAILURE(leaderClosed(r, now));
    const Term term = r.e.leadershipTermId();
    // Members 1 and 2 echo once: member 1 then holds a ~127 ms lease.
    r.e.onReadLeaseRequest(now, term, 1, 1, 0);
    r.e.onReadLeaseRequest(now, term, 2, 1, 0);
    const int64_t g1 = r.pub.grants[0].grantSeq, g2 = r.pub.grants[1].grantSeq;
    r.leaseNow += 20 * kMs;
    r.e.onReadLeaseRequest(now, term, 2, 2, g2);
    r.e.onReadLeaseRequest(now, term, 1, 2, g1);
    const int64_t valid = r.pub.grants.back().validFor;
    ASSERT_GT(valid, 100 * kMs);
    // Past the leader grace: member 1 sends nothing more.  With a 10 ms liveness timeout it would be dead at once -
    // but its lease runs: it stays eligible until the lease ends (lease clock), then it is dropped.
    now += 10'000 * kMs;
    r.e.onAppendPosition(now, term, 0, 2, 0);
    NodeId out[8];
    std::size_t n = r.e.eligibleNodeIds(now, 10 * kMs, out, 8);
    EXPECT_EQ(std::vector<NodeId>(out, out + n), (std::vector<NodeId>{0, 1, 2})) << "member 1's lease may still run";
    r.leaseNow += valid;   // the lease is over on the controller's clock
    n = r.e.eligibleNodeIds(now, 10 * kMs, out, 8);
    EXPECT_EQ(std::vector<NodeId>(out, out + n), (std::vector<NodeId>{0, 2}));
}

TEST(ElectionReadLeaseRegression, ANewControllersFailoverGraceCoversEveryLeaseOfThePreviousOneRegression) {
    Rig r(0);
    int64_t now = 1'000 * kMs;
    ASSERT_NO_FATAL_FAILURE(leaderClosed(r, now));
    // A liveness timeout shorter than W (a lease the previous controller granted may still run): the grace is W.
    NodeId out[8];
    const int64_t w = r.e.readLeaseFenceWaitNs();
    EXPECT_EQ(w, kReadLeaseFenceWaitNs);
    // Inside W of the leadership start every member is eligible, whatever the timeout; after W the silent ones go.
    // (The grace runs from the first leader state, at most a few ms before `now`.)
    std::size_t n = r.e.eligibleNodeIds(now + 2 * kMs, 1 * kMs, out, 8);
    EXPECT_EQ(n, 3u) << "a 1 ms liveness timeout must not drop a member inside W of the leadership start";
    n = r.e.eligibleNodeIds(now + w - 50 * kMs, 1 * kMs, out, 8);
    EXPECT_EQ(n, 3u) << "still inside W";
    n = r.e.eligibleNodeIds(now + w + 1 * kMs, 1 * kMs, out, 8);
    EXPECT_EQ(std::vector<NodeId>(out, out + n), (std::vector<NodeId>{0})) << "past W, silent and lease-less: dropped";
}

// =====================================================================================================================
// PR #490 adversarial review: hostile / buggy grant and request values through the pure state machines.
// =====================================================================================================================
TEST(ReadLeaseRegression, HolderRefusesOrCapsEveryHostileGrantValueAdversarialRegression) {
    ReadLeaseHolder h;
    const int64_t s = 1'000 * kMs;
    const auto rq = h.nextRequest(s);
    ASSERT_EQ(rq.reqSeq, 1);
    // Unknown / never-sent / non-positive request sequences: refused, no lease, no echo.
    for (const int64_t bad : {int64_t{0}, int64_t{-1}, INT64_MIN, int64_t{2}, int64_t{65}, INT64_MAX}) {
        EXPECT_FALSE(h.onGrant(bad, 5, 0, 100 * kMs)) << "reqSeq " << bad;
    }
    EXPECT_EQ(h.heardGrantSeq(), 0);
    EXPECT_EQ(h.latestEndNs(), ReadLeaseHolder::kNone);
    // Non-positive validFor: the echo counts, no lease.
    for (const int64_t v : {int64_t{0}, int64_t{-1}, INT64_MIN}) EXPECT_TRUE(h.onGrant(1, 3, 0, v));
    EXPECT_EQ(h.heardGrantSeq(), 3);
    EXPECT_EQ(h.latestEndNs(), ReadLeaseHolder::kNone);
    // An oversize validFor (a buggy / skewed grantor) is capped at L, then shrunk by the drift budget.
    EXPECT_TRUE(h.onGrant(1, 4, 0, INT64_MAX));
    EXPECT_EQ(h.latestEndNs(), s + leaseShrinkByDrift(kReadLeaseMaxNs));
    // A replayed grant never extends the lease past its own request's bound; an older grant seq never lowers the echo.
    EXPECT_TRUE(h.onGrant(1, 2, 0, INT64_MAX));
    EXPECT_EQ(h.latestEndNs(), s + leaseShrinkByDrift(kReadLeaseMaxNs));
    EXPECT_EQ(h.heardGrantSeq(), 4);
    // A request whose ring slot was reused (64 newer requests) is unknown: a very late grant for it is refused.
    for (int i = 0; i < static_cast<int>(ReadLeaseHolder::kRequestRing); ++i) (void)h.nextRequest(s + (i + 1) * kMs);
    EXPECT_FALSE(h.onGrant(1, 9, 0, 100 * kMs));
    // A lease whose commitPos is above what was applied is never deliverable, whatever its length.
    ReadLeaseHolder g;
    const auto r2 = g.nextRequest(s);
    ASSERT_TRUE(g.onGrant(r2.reqSeq, 1, /*commitPos=*/INT64_MAX, 100 * kMs));
    EXPECT_EQ(g.deliverableEndNs(INT64_MAX - 1), ReadLeaseHolder::kNone);
    EXPECT_EQ(g.deliverableEndNs(INT64_MAX), s + leaseShrinkByDrift(100 * kMs));
}

TEST(ReadLeaseRegression, GrantorNeverGrantsPastHUnderAnyEchoPatternAdversarialRegression) {
    const NodeId ids[] = {0, 1, 2, 3, 4};
    std::mt19937_64 rng(0x490490);
    for (int trial = 0; trial < 2'000; ++trial) {
        SCOPED_TRACE("trial " + std::to_string(trial) + " seed 0x490490");
        ReadLeaseGrantor gr;
        const std::size_t n = 3 + 2 * (trial % 2);   // 3 or 5 members
        gr.configure(std::span<const NodeId>(ids, n), /*self=*/0, kH);
        int64_t now = 10'000 * kMs;
        std::vector<int64_t> lastGrant(n, 0);
        for (int step = 0; step < 200; ++step) {
            now += static_cast<int64_t>(rng() % (30 * kMs));
            const auto m = static_cast<NodeId>(1 + rng() % (n - 1));
            int64_t echo = 0;
            switch (rng() % 5) {
                case 0: echo = lastGrant[static_cast<std::size_t>(m)]; break;                  // honest
                case 1: echo = static_cast<int64_t>(rng());                                    // garbage
                case 2: echo = -static_cast<int64_t>(rng() % 100);                             // negative
                case 3: echo = gr.grantSeqForTest() + 1 + static_cast<int64_t>(rng() % 10);   // from the future
                default: echo = lastGrant[static_cast<std::size_t>(1 + rng() % (n - 1))];      // another member's
            }
            const auto g = gr.onRequest(m, echo, now);
            ASSERT_GE(g.grantSeq, 1);
            lastGrant[static_cast<std::size_t>(m)] = g.grantSeq;
            ASSERT_GE(g.validForNs, 0);
            ASSERT_LE(g.validForNs, leaseShrinkByDrift(kH)) << "a grant longer than H/(1+rho)^2";
            ASSERT_LE(g.validForNs, kReadLeaseMaxNs);
        }
        // Self, unknown and out-of-range members get nothing.
        for (const NodeId bad : {NodeId{0}, NodeId{-1}, NodeId{31}, NodeId{32}, static_cast<NodeId>(n)})
            EXPECT_LT(gr.onRequest(bad, 1, now).grantSeq, 0) << "member " << bad;
    }
}

// Issue #486 (VLS A.8): the fence a leader move that drops the old leader from the ISR waits on.  Open only when this
// node is the CLOSED leader, past W since its leadership began (a predecessor's grants have ended), and no lease it
// granted the old leader may still run.  A withheld member's requests are answered validFor 0 (the echo still counts,
// so QL keeps advancing); a withheld controller stops leasing itself.  The wait is bounded: with renewal withheld, the
// last lease ends within L.
// RED: the W term removed (opens inside the first W); the leaseMayRun term removed (opens under a running lease);
// setWithheld ignored (the lease keeps being renewed: the fence never opens).
TEST(ElectionReadLeaseRegression, TheMoveFenceOpensOnlyPastWOnceTheOldLeadersLeaseEndedRegression) {
    Rig r(0);
    int64_t now = 1'000 * kMs;
    EXPECT_FALSE(r.e.readLeaseMoveFenceOpen(1, now)) << "not the leader";
    ASSERT_NO_FATAL_FAILURE(leaderClosed(r, now));
    const Term term = r.e.leadershipTermId();
    EXPECT_FALSE(r.e.readLeaseMoveFenceOpen(2, now)) << "inside W: even a member this controller never leased waits";
    EXPECT_FALSE(r.e.readLeaseMoveFenceOpen(2, now + kReadLeaseFenceWaitNs - 50 * kMs)) << "the grace began a few ms before now";
    EXPECT_TRUE(r.e.readLeaseMoveFenceOpen(2, now + kReadLeaseFenceWaitNs + kMs)) << "past W, never leased: open";
    // Member 1 holds a ~127 ms lease.
    r.e.onReadLeaseRequest(now, term, 1, 1, 0);
    r.e.onReadLeaseRequest(now, term, 2, 1, 0);
    const int64_t g1 = r.pub.grants[0].grantSeq, g2 = r.pub.grants[1].grantSeq;
    r.leaseNow += 20 * kMs;
    r.e.onReadLeaseRequest(now, term, 2, 2, g2);
    r.e.onReadLeaseRequest(now, term, 1, 2, g1);
    const int64_t valid = r.pub.grants.back().validFor;
    ASSERT_GT(valid, 100 * kMs);
    EXPECT_FALSE(r.e.readLeaseMoveFenceOpen(1, now)) << "inside W of this leadership: a predecessor's lease may run";
    now += kReadLeaseFenceWaitNs;
    EXPECT_FALSE(r.e.readLeaseMoveFenceOpen(1, now)) << "member 1's lease may still run";
    // Withheld: member 1's next requests get no lease, but its echo keeps QL moving for everyone else.
    r.e.setReadLeaseWithheld(1u << 1);
    r.leaseNow += 20 * kMs;
    r.e.onReadLeaseRequest(now, term, 1, 3, r.pub.grants.back().grantSeq);
    EXPECT_EQ(r.pub.grants.back().validFor, 0) << "a withheld member's lease is not renewed";
    r.e.onReadLeaseRequest(now, term, 2, 3, r.pub.grants[2].grantSeq);
    EXPECT_GT(r.pub.grants.back().validFor, 0) << "the others are still leased";
    EXPECT_FALSE(r.e.readLeaseMoveFenceOpen(1, now)) << "its last lease still runs";
    r.leaseNow += valid;   // past the end of the last lease member 1 was granted
    EXPECT_TRUE(r.e.readLeaseMoveFenceOpen(1, now)) << "bounded: the last lease ended";
}

TEST(ElectionReadLeaseRegression, AWithheldControllerStopsLeasingItselfAndItsOwnMoveWaitsRegression) {
    Rig r(0);
    int64_t now = 1'000 * kMs;
    ASSERT_NO_FATAL_FAILURE(leaderClosed(r, now));
    const Term term = r.e.leadershipTermId();
    r.e.onReadLeaseRequest(now, term, 1, 1, 0);
    r.leaseNow += 20 * kMs;
    r.e.onReadLeaseRequest(now, term, 1, 2, r.pub.grants.back().grantSeq);
    now += kStatusIntervalNs;
    r.e.doWork(now);   // the self lease
    const int64_t selfEnd = r.e.readLease().latestEndNs();
    ASSERT_GT(selfEnd, r.leaseNow);
    now += kReadLeaseFenceWaitNs;
    EXPECT_FALSE(r.e.readLeaseMoveFenceOpen(0, now)) << "the controller's own lease may still run";
    r.e.setReadLeaseWithheld(1u << 0);
    for (int i = 0; i < 20; ++i) {
        now += kStatusIntervalNs;
        r.e.onAppendPosition(now, term, r.e.appendPosition(), 1, 0);   // the followers stay active
        r.e.onAppendPosition(now, term, r.e.appendPosition(), 2, 0);
        r.e.doWork(now);
    }
    ASSERT_TRUE(r.e.isClosed());
    EXPECT_EQ(r.e.readLease().latestEndNs(), selfEnd) << "a withheld controller never renews its own lease";
    r.leaseNow = selfEnd + kReadLeaseMaxNs;
    EXPECT_TRUE(r.e.readLeaseMoveFenceOpen(0, now));
}
