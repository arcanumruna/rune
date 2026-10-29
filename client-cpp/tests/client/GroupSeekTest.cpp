// tests/client/GroupSeekTest.cpp — Phase 2b F Task 10: GroupConsumer::seekToCommitted() / seek().
//
// The consumer half of the exactly-once loop: after a transaction abort the consumer rewinds to the
// committed offsets without a restart.  Rig: one Invoker client + the InvokerGateway in its group
// mode over real Aeron IPC (tests/support/GroupRig.h); single-threaded, condition-driven, no sleeps:
// every wait is a bounded pump loop.
#include "../support/GroupRig.h"

#include <rune/client/Completions.h>
#include <rune/client/agent/AgentRingMessages.h>
#include <rune/client/detail/HubCommand.h>
#include <rune/client/detail/OpTarget.h>
#include <rune/client/group/GroupConsumerHub.h>
#include <rune/protocol/client/OffsetMessages.h>

#include <chrono>
#include <map>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

namespace {

constexpr int32_t kTopic = 1;

// Everything the application sees on the duty thread (= this thread).
struct Seen {
    std::vector<int64_t> offsets[2];   // partition -> every record offset delivered, in order
    int total() const { return static_cast<int>(offsets[0].size() + offsets[1].size()); }
};

void wire(rc::GroupConsumer& gc, Seen& seen) {
    gc.onRecords([&seen](const rc::RecordSet& rs) {
        rs.forEachRecord([&](auto, auto, int64_t offset, int64_t) { seen.offsets[rs.partition].push_back(offset); });
    });
}

struct Done {
    int         times = 0;
    rc::Completion c{};
    std::string text;
};

// Every completion of the rig's queue, by token.
void collect(GroupRig& r, std::map<rc::Token, Done>& out) {
    r.cq.poll([&](const rc::Completion& c, std::span<const uint8_t> p) {
        auto& d = out[c.token];
        ++d.times;
        d.c    = c;
        d.text.assign(reinterpret_cast<const char*>(p.data()), p.size());
    });
}

bool driveUntilDone(GroupRig& r, std::map<rc::Token, Done>& out, rc::Token t, int cap = 4'000'000) {
    return r.drive([&] { collect(r, out); return out.count(t) != 0; }, cap);
}

// A single-node group over `partitions` of topic "t" (all assigned), joined and Stable with one
// subscription per partition.
struct Fixture {
    GroupRig        r;
    rc::GroupConsumer gc;
    Seen            seen;
    std::map<rc::Token, Done> done;
    rc::GroupConfig cfg;

    static rc::GroupConfig cfgOf(bool autoCommit, int sessionMs, int commitMs) {
        rc::GroupConfig c;
        c.autoCommit           = autoCommit;
        c.sessionTimeoutMs     = sessionMs;
        c.autoCommitIntervalMs = commitMs;
        return c;
    }
    // `holdFetch`: the Assign OffsetFetch is held at the gateway from the start (the fixture then does not wait for
    // the subscriptions; the test releases the answer).
    Fixture(int32_t in, int32_t eg, std::vector<std::pair<int32_t, int32_t>> assigned,
            rc::GroupConfig gcfg = cfgOf(false, 30000, 5000),
            std::map<std::pair<int32_t, int32_t>, int64_t> committed = {}, RigOptions ro = {}, bool holdFetch = false)
        : r(in, eg, ro), gc(prepare(in, std::move(assigned), gcfg, std::move(committed))) {
        wire(gc, seen);
        r.gw.holdOffsetFetch = holdFetch;
        r.connect();
        EXPECT_TRUE(r.drive([&] { return !r.gw.pendingJoins.empty(); }));
        r.gw.releaseJoins();
        if (holdFetch) {
            EXPECT_TRUE(r.drive([&] { return r.gw.offsetFetches > 0; }));
            return;
        }
        for (const auto& [t, p] : r.gw.groupAssignment) {
            EXPECT_TRUE(r.drive([&] { return r.gw.latestSubscription(t, p) != nullptr; })) << "no subscription for " << p;
        }
    }
    rc::GroupConsumer prepare(int32_t in, std::vector<std::pair<int32_t, int32_t>> assigned, const rc::GroupConfig& gcfg,
                              std::map<std::pair<int32_t, int32_t>, int64_t> committed) {
        r.gw.committedOffsets = std::move(committed);
        r.gw.partitions      = {{1, 0, 0, 1, in, ""}, {1, 1, 0, 1, in, ""}};
        r.gw.groupAssignment = std::move(assigned);
        return r.client.subscribeGroup("g", {"t"}, gcfg);
    }
    rc::Token tok() { return r.nextToken++; }
    // Records delivered so far on `p` (waited for).
    bool consumed(int32_t p, std::size_t n) { return r.drive([&] { return seen.offsets[p].size() >= n; }); }
};

// Committed offsets of (topic 1, partition) in every OffsetCommit the gateway received.
std::vector<int64_t> committedOf(const test_support::InvokerGateway& gw, int32_t partition) {
    std::vector<int64_t> v;
    for (const auto& f : gw.offsetCommits) {
        const auto oc = rune::protocol::client::OffsetCommit::decode(f.data(), f.size());
        if (!oc) continue;
        for (const auto& e : oc->entries) {
            if (e.topicId == kTopic && e.partition == partition) v.push_back(e.offset);
        }
    }
    return v;
}

// True after the injected duty clock advanced `window` (1 ms per pass, one pump per step) without `pred` ever
// holding.  The rig must run on manualClockOptions(); the step count is the hard cap, no wall time is read.
template <class P>
bool staysFalseFor(GroupRig& r, P&& pred, std::chrono::milliseconds window) {
    constexpr int64_t kStepNs = 1'000'000;
    const int steps           = static_cast<int>(window.count());
    const int64_t savedStep   = r.opt.stepNs;
    r.opt.stepNs              = kStepNs;
    int  passes  = 0;
    bool fired   = false;
    (void)r.drive([&] {
        if (pred()) { fired = true; return true; }
        return ++passes >= steps;
    }, steps);
    r.opt.stepNs = savedStep;
    return !fired;
}

// C16: a manual duty clock.  Setup runs with a 100 us step per pass; freeze() stops it (and runs a few passes so no
// sweep that was already due is still pending): from then on only message transit can make progress.
int64_t g_clockNs = 1'000'000'000LL;
int64_t manualClock() noexcept { return g_clockNs; }
RigOptions manualClockOptions() {
    RigOptions o;
    g_clockNs  = 1'000'000'000LL;
    o.nanoClock = &manualClock;
    o.manualNs  = &g_clockNs;
    o.stepNs    = 100'000;
    return o;
}
void freeze(GroupRig& r) {
    for (int i = 0; i < 50; ++i) { r.client.doWork(); r.gw.pump(); }   // drain a sweep that was already due
    r.opt.stepNs = 0;
    for (int i = 0; i < 50; ++i) { r.client.doWork(); r.gw.pump(); }
}

} // namespace

// Commit offset 40 on a partition, consume to 100, rewind: the member re-reads exactly the
// uncommitted records (40..99) and nothing before 40.  RED before the feature: no such call.
TEST(GroupSeekRegression, SeekToCommittedRedeliversExactlyTheUncommittedRecordsRegression) {
    Fixture f(4301, 4302, {{kTopic, 1}}, Fixture::cfgOf(false, 30000, 5000), {{{kTopic, 1}, 40}});
    const auto* s1 = f.r.gw.latestSubscription(kTopic, 1);
    ASSERT_NE(s1, nullptr);
    EXPECT_EQ(s1->fromOffset, 40) << "the subscription must start at the committed offset";
    const auto sub1 = *s1;
    f.r.gw.pushRecords(sub1, 40, 60);                  // 40..99
    ASSERT_TRUE(f.consumed(1, 60));
    EXPECT_EQ(f.seen.offsets[1].front(), 40);
    EXPECT_EQ(f.seen.offsets[1].back(), 99);

    const std::size_t before = f.seen.offsets[1].size();
    const rc::Token t = f.tok();
    ASSERT_EQ(f.gc.seekToCommitted(rc::CompletionTarget(f.r.cq), t), rc::OpResult::Accepted);
    ASSERT_TRUE(driveUntilDone(f.r, f.done, t));
    EXPECT_EQ(f.done[t].times, 1);
    EXPECT_EQ(f.done[t].c.status, rc::OpStatus::Ok);
    const auto* s2 = f.r.gw.latestSubscription(kTopic, 1);
    ASSERT_NE(s2, nullptr);
    EXPECT_NE(s2->corrId, sub1.corrId) << "a new subscription instance";
    EXPECT_EQ(s2->fromOffset, 40);
    f.r.gw.pushRecords(*s2, 40, 60);                   // the broker re-reads 40..99
    ASSERT_TRUE(f.consumed(1, before + 60));
    std::vector<int64_t> again(f.seen.offsets[1].begin() + static_cast<std::ptrdiff_t>(before), f.seen.offsets[1].end());
    ASSERT_EQ(again.size(), 60u);
    for (int i = 0; i < 60; ++i) EXPECT_EQ(again[static_cast<std::size_t>(i)], 40 + i);
}

TEST(GroupSeek, SeekRestartsOnePartitionAtTheOffset) {
    Fixture f(4303, 4304, {{kTopic, 0}, {kTopic, 1}});
    const auto p0 = *f.r.gw.latestSubscription(kTopic, 0);
    const auto p1 = *f.r.gw.latestSubscription(kTopic, 1);
    f.r.gw.pushRecords(p0, 0, 10);
    f.r.gw.pushRecords(p1, 0, 10);
    ASSERT_TRUE(f.consumed(0, 10));
    ASSERT_TRUE(f.consumed(1, 10));

    const rc::Token t = f.tok();
    ASSERT_EQ(f.gc.seek({kTopic, 1}, 3, rc::CompletionTarget(f.r.cq), t), rc::OpResult::Accepted);
    ASSERT_TRUE(driveUntilDone(f.r, f.done, t));
    EXPECT_EQ(f.done[t].c.status, rc::OpStatus::Ok);
    EXPECT_EQ(f.done[t].c.op, rc::OpKind::Seek);
    const auto* n1 = f.r.gw.latestSubscription(kTopic, 1);
    ASSERT_NE(n1, nullptr);
    EXPECT_NE(n1->corrId, p1.corrId);
    EXPECT_EQ(n1->fromOffset, 3);
    // The other partition is untouched: same instance, never unsubscribed.
    EXPECT_EQ(f.r.gw.latestSubscription(kTopic, 0)->corrId, p0.corrId);
    for (const int64_t u : f.r.gw.unsubscribed) EXPECT_NE(u, p0.subscriptionId) << "partition 0 was unsubscribed";
    bool oldP1Gone = false;
    for (const int64_t u : f.r.gw.unsubscribed) oldP1Gone = oldP1Gone || u == p1.subscriptionId;
    EXPECT_TRUE(oldP1Gone) << "the rewound partition's old subscription must be released";
    // The rewound partition re-reads from the offset; partition 0 keeps its stream.
    f.r.gw.pushRecords(*n1, 3, 7);
    f.r.gw.pushRecords(p0, 10, 2);
    ASSERT_TRUE(f.consumed(1, 17));
    ASSERT_TRUE(f.consumed(0, 12));
    EXPECT_EQ(f.seen.offsets[1][10], 3);
}

TEST(GroupSeekRegression, SeekOnAnUnassignedPartitionIsInvalidArgumentRegression) {
    Fixture f(4305, 4306, {{kTopic, 1}});
    const rc::Token t = f.tok();
    ASSERT_EQ(f.gc.seek({kTopic, 0}, 5, rc::CompletionTarget(f.r.cq), t), rc::OpResult::Accepted);
    ASSERT_TRUE(driveUntilDone(f.r, f.done, t));
    EXPECT_EQ(f.done[t].times, 1);
    EXPECT_EQ(f.done[t].c.status, rc::OpStatus::InvalidArgument);
    EXPECT_EQ(f.done[t].text, "partition not assigned");
    // The *AndWait form throws (the completion needs no gateway: the Invoker pump suffices).
    EXPECT_THROW(f.gc.seekAndWait({kTopic, 0}, 5, std::chrono::seconds(5)), std::invalid_argument);
    // The fire-and-forget form reports nothing and must not disturb the consumer.
    f.gc.seek({kTopic, 0}, 5);
    EXPECT_FALSE(f.gc.finished());
    // Nothing was restarted: the assigned partition's subscription is the original.
    EXPECT_EQ(f.r.gw.unsubscribed.size(), 0u);
}

// An auto-commit tick between the command and the OffsetFetch answer must not commit a stale
// position: the rewound partitions are unseeded until the answer.
TEST(GroupSeekRegression, AutoCommitNeverCommitsAStalePositionDuringARewindRegression) {
    Fixture f(4307, 4308, {{kTopic, 1}}, Fixture::cfgOf(true, 30000, 40), {}, manualClockOptions());
    const auto s = *f.r.gw.latestSubscription(kTopic, 1);
    f.r.gw.pushRecords(s, 0, 100);
    ASSERT_TRUE(f.consumed(1, 100));
    ASSERT_TRUE(f.r.drive([&] { const auto c = committedOf(f.r.gw, 1); return !c.empty() && c.back() == 100; }))
        << "positive control: auto-commit commits the consumed position";

    f.r.gw.holdOffsetFetch = true;
    const int fetches = f.r.gw.offsetFetches;
    const std::size_t commitsBefore = f.r.gw.offsetCommits.size();
    const rc::Token t = f.tok();
    ASSERT_EQ(f.gc.seekToCommitted(rc::CompletionTarget(f.r.cq), t), rc::OpResult::Accepted);
    ASSERT_TRUE(f.r.drive([&] { return f.r.gw.offsetFetches > fetches; })) << "no OffsetFetch for the rewind";
    // Many auto-commit ticks (40 ms) pass while the answer is held.
    EXPECT_TRUE(staysFalseFor(f.r, [&] { return f.r.gw.offsetCommits.size() > commitsBefore; },
                              std::chrono::milliseconds(400)))
        << "an OffsetCommit went out for a rewound partition before the OffsetFetch answer";
    EXPECT_EQ(f.done.count(t), 0u);
    f.r.gw.holdOffsetFetch = false;
    f.r.gw.releaseOffsetFetches();
    ASSERT_TRUE(driveUntilDone(f.r, f.done, t));
    EXPECT_EQ(f.done[t].c.status, rc::OpStatus::Ok);
}

TEST(GroupSeek, RebalanceDuringARewindCompletesRejoined) {
    Fixture f(4309, 4310, {{kTopic, 1}}, Fixture::cfgOf(false, 600, 5000));   // a heartbeat every 200 ms
    f.r.gw.holdOffsetFetch = true;
    const rc::Token t = f.tok();
    ASSERT_EQ(f.gc.seekToCommitted(rc::CompletionTarget(f.r.cq), t), rc::OpResult::Accepted);
    f.r.gw.rebalancing = true;   // the next heartbeat answers RebalanceInProgress
    ASSERT_TRUE(driveUntilDone(f.r, f.done, t, 40'000'000));
    EXPECT_EQ(f.done[t].times, 1);
    EXPECT_EQ(f.done[t].c.status, rc::OpStatus::Rejoined);
}

TEST(GroupSeek, PushesOfTheOldSubscriptionAreIgnoredAfterTheSeek) {
    Fixture f(4311, 4312, {{kTopic, 1}}, Fixture::cfgOf(false, 30000, 5000), {}, manualClockOptions());
    const auto old = *f.r.gw.latestSubscription(kTopic, 1);
    f.r.gw.pushRecords(old, 0, 5);
    ASSERT_TRUE(f.consumed(1, 5));
    const rc::Token t = f.tok();
    ASSERT_EQ(f.gc.seek({kTopic, 1}, 2, rc::CompletionTarget(f.r.cq), t), rc::OpResult::Accepted);
    ASSERT_TRUE(driveUntilDone(f.r, f.done, t));
    ASSERT_EQ(f.done[t].c.status, rc::OpStatus::Ok);
    // The gateway pushes one more batch on the OLD instance after the Unsubscribe.
    f.r.gw.pushRecords(old, 5, 4);
    EXPECT_TRUE(staysFalseFor(f.r, [&] { return f.seen.offsets[1].size() > 5; }, std::chrono::milliseconds(150)))
        << "a retired subscription's batch was delivered";
    const auto cur = *f.r.gw.latestSubscription(kTopic, 1);
    f.r.gw.pushRecords(cur, 2, 3);
    ASSERT_TRUE(f.consumed(1, 8));
    EXPECT_EQ(f.seen.offsets[1][5], 2);
}

TEST(GroupSeek, SecondRewindSupersedesTheFirst) {
    Fixture f(4313, 4314, {{kTopic, 1}});
    f.r.gw.holdOffsetFetch = true;
    const rc::Token a = f.tok(), b = f.tok();
    ASSERT_EQ(f.gc.seekToCommitted(rc::CompletionTarget(f.r.cq), a), rc::OpResult::Accepted);
    ASSERT_EQ(f.gc.seekToCommitted(rc::CompletionTarget(f.r.cq), b), rc::OpResult::Accepted);
    ASSERT_TRUE(driveUntilDone(f.r, f.done, a));
    EXPECT_EQ(f.done[a].c.status, rc::OpStatus::Superseded);
    EXPECT_EQ(f.done.count(b), 0u);
    f.r.gw.holdOffsetFetch = false;
    f.r.gw.releaseOffsetFetches();
    ASSERT_TRUE(driveUntilDone(f.r, f.done, b));
    EXPECT_EQ(f.done[b].c.status, rc::OpStatus::Ok);
    EXPECT_EQ(f.done[a].times, 1);
    EXPECT_EQ(f.done[b].times, 1);
}

TEST(GroupSeek, ClosingTheConsumerCompletesAPendingRewindClosed) {
    Fixture f(4315, 4316, {{kTopic, 1}});
    f.r.gw.holdOffsetFetch = true;
    const rc::Token t = f.tok();
    ASSERT_EQ(f.gc.seekToCommitted(rc::CompletionTarget(f.r.cq), t), rc::OpResult::Accepted);
    f.gc.close();
    ASSERT_TRUE(driveUntilDone(f.r, f.done, t));
    EXPECT_EQ(f.done[t].c.status, rc::OpStatus::Closed);
    EXPECT_EQ(f.done[t].times, 1);
}

namespace {
// Replaces the fixture's onRecords: records every offset, and on the FIRST batch runs `onFirst`
// inside the callback.
template <class Fn>
void wireRewindOnFirstBatch(Fixture& f, Fn onFirst) {
    auto fired = std::make_shared<bool>(false);
    f.gc.onRecords([&f, fired, onFirst](const rc::RecordSet& rs) {
        rs.forEachRecord([&](auto, auto, int64_t offset, int64_t) { f.seen.offsets[rs.partition].push_back(offset); });
        if (!*fired) { *fired = true; onFirst(rs); }
    });
    (void)f.r.drive([] { return false; }, 2'000);   // the setter travels the ring: a few cycles
}
} // namespace

// A seek to the current batch's start, issued INSIDE onRecords, must win over the position advance
// that follows the callback: the batch is redelivered.  RED before the fix: the advance overwrote
// the seek, the re-pushed batch was dropped as a duplicate (Ok, nothing delivered).
TEST(GroupSeekRegression, SeekInsideOnRecordsToTheBatchStartRedeliversTheBatchRegression) {
    Fixture f(4317, 4318, {{kTopic, 1}});
    const rc::Token t = f.tok();
    wireRewindOnFirstBatch(f, [&](const rc::RecordSet& rs) {
        EXPECT_EQ(f.gc.seek({kTopic, 1}, rs.baseOffset, rc::CompletionTarget(f.r.cq), t), rc::OpResult::Accepted);
    });
    const auto s = *f.r.gw.latestSubscription(kTopic, 1);
    f.r.gw.pushRecords(s, 0, 5);
    ASSERT_TRUE(f.consumed(1, 5));
    ASSERT_TRUE(driveUntilDone(f.r, f.done, t));
    EXPECT_EQ(f.done[t].c.status, rc::OpStatus::Ok);
    const auto cur = *f.r.gw.latestSubscription(kTopic, 1);
    ASSERT_NE(cur.corrId, s.corrId);
    EXPECT_EQ(cur.fromOffset, 0);
    f.r.gw.pushRecords(cur, 0, 5);
    ASSERT_TRUE(f.consumed(1, 10)) << "the batch the callback rewound to was never redelivered";
    EXPECT_EQ(f.seen.offsets[1][5], 0);
}

TEST(GroupSeekRegression, SeekToCommittedInsideOnRecordsRedeliversTheBatchRegression) {
    Fixture f(4319, 4320, {{kTopic, 1}});
    const rc::Token t = f.tok();
    wireRewindOnFirstBatch(f, [&](const rc::RecordSet&) {
        EXPECT_EQ(f.gc.seekToCommitted(rc::CompletionTarget(f.r.cq), t), rc::OpResult::Accepted);
    });
    const auto s = *f.r.gw.latestSubscription(kTopic, 1);
    f.r.gw.pushRecords(s, 0, 5);
    ASSERT_TRUE(f.consumed(1, 5));
    ASSERT_TRUE(driveUntilDone(f.r, f.done, t));
    EXPECT_EQ(f.done[t].c.status, rc::OpStatus::Ok);
    const auto cur = *f.r.gw.latestSubscription(kTopic, 1);
    ASSERT_NE(cur.corrId, s.corrId);
    EXPECT_EQ(cur.fromOffset, 0);
    f.r.gw.pushRecords(cur, 0, 5);
    ASSERT_TRUE(f.consumed(1, 10));
    EXPECT_EQ(f.seen.offsets[1][5], 0);
}

// seek(-1) used to land on the unseeded sentinel: the partition was never subscribed again.
TEST(GroupSeekRegression, NegativeSeekOffsetIsInvalidArgumentNotAStallRegression) {
    Fixture f(4321, 4322, {{kTopic, 1}});
    const auto s = *f.r.gw.latestSubscription(kTopic, 1);
    const rc::Token t = f.tok();
    ASSERT_EQ(f.gc.seek({kTopic, 1}, -1, rc::CompletionTarget(f.r.cq), t), rc::OpResult::Accepted);
    ASSERT_TRUE(driveUntilDone(f.r, f.done, t));
    EXPECT_EQ(f.done[t].c.status, rc::OpStatus::InvalidArgument);
    EXPECT_EQ(f.done[t].text, "offset must be >= 0");
    EXPECT_THROW(f.gc.seekAndWait({kTopic, 1}, -5, std::chrono::seconds(5)), std::invalid_argument);
    EXPECT_THROW(f.gc.seek({kTopic, 1}, -1), std::invalid_argument);
    // Nothing was disturbed: the original subscription still feeds the partition.
    EXPECT_EQ(f.r.gw.latestSubscription(kTopic, 1)->corrId, s.corrId);
    f.r.gw.pushRecords(s, 0, 3);
    ASSERT_TRUE(f.consumed(1, 3));
}

// The void seek must not drop a refusal at submit: a closed consumer throws.
TEST(GroupSeekRegression, VoidSeekOnAClosedConsumerThrowsNeverDropsItRegression) {
    Fixture f(4323, 4324, {{kTopic, 1}});
    f.gc.close();
    EXPECT_THROW(f.gc.seek({kTopic, 1}, 0), std::runtime_error);
}

// #378: the blocking rewind ends with a typed GroupOpError: Rejoined (the group is not Stable when applied) and
// Timeout are told apart by status(), never by text; SessionLostError is a GroupOpError too.
TEST(GroupSeekRegression, BlockingRewindThrowsATypedGroupOpErrorRegression) {
    Fixture f(4325, 4326, {{kTopic, 1}}, Fixture::cfgOf(false, 600, 5000));   // a heartbeat every 200 ms
    f.r.gw.rebalancing = true;   // the next heartbeat answers RebalanceInProgress -> the member rejoins
    ASSERT_TRUE(f.r.drive([&] { return !f.r.gw.pendingJoins.empty(); })) << "the member never went to rejoin";
    bool rejoined = false;
    try {
        f.gc.seekToCommittedAndWait(std::chrono::seconds(5));
    } catch (const rc::GroupOpError& e) {
        rejoined = e.status() == rc::OpStatus::Rejoined;
    }
    EXPECT_TRUE(rejoined);

    try {
        throw rc::SessionLostError("lost");
    } catch (const rc::GroupOpError& e) {
        EXPECT_EQ(e.status(), rc::OpStatus::SessionLost);
    }
}

// ===========================================================================
// #356: group seek test gaps
// ===========================================================================

// F1: a rewind whose fetch never answers completes Timeout exactly once, leaves nothing stuck, and the next rewind
// is admitted and completes Ok.  Mutation: drop the Timeout branch of advanceRewind_ -> never completes.
TEST(GroupSeekRegression, RewindTimeoutCompletesOnceAndLeavesNoStuckStateRegression) {
    rc::GroupConfig cfg = Fixture::cfgOf(false, 30000, 5000);
    cfg.commitSyncTimeoutMs = 400;   // the rewind's duty-time deadline
    Fixture f(4910, 4911, {{kTopic, 1}}, cfg);
    f.r.gw.holdOffsetFetch = true;
    const rc::Token a = f.tok();
    ASSERT_EQ(f.gc.seekToCommitted(rc::CompletionTarget(f.r.cq), a), rc::OpResult::Accepted);
    ASSERT_TRUE(driveUntilDone(f.r, f.done, a, 40'000'000)) << "the rewind never timed out";
    EXPECT_EQ(f.done[a].c.status, rc::OpStatus::Timeout);
    f.r.gw.holdOffsetFetch = false;
    f.r.gw.releaseOffsetFetches();   // the answer to the timed-out fetch arrives late
    const rc::Token b = f.tok();
    ASSERT_EQ(f.gc.seekToCommitted(rc::CompletionTarget(f.r.cq), b), rc::OpResult::Accepted);
    ASSERT_TRUE(driveUntilDone(f.r, f.done, b, 40'000'000)) << "the second rewind was not admitted";
    EXPECT_EQ(f.done[b].c.status, rc::OpStatus::Ok);
    for (int i = 0; i < 500; ++i) { f.r.client.doWork(); f.r.gw.pump(); }
    collect(f.r, f.done);
    EXPECT_EQ(f.done[a].times, 1);
    EXPECT_EQ(f.done[b].times, 1);
}

// F2: the session goes down mid-rewind: exactly one SessionLost; after the reconnect and Stable, a new rewind is Ok.
TEST(GroupSeekRegression, RewindSessionLostCompletesOnceAndLeavesNoStuckStateRegression) {
    Fixture f(4912, 4913, {{kTopic, 1}});
    f.r.gw.holdOffsetFetch = true;
    const rc::Token a = f.tok();
    ASSERT_EQ(f.gc.seekToCommitted(rc::CompletionTarget(f.r.cq), a), rc::OpResult::Accepted);
    const int fetches = f.r.gw.offsetFetches;
    ASSERT_TRUE(f.r.drive([&] { return f.r.gw.offsetFetches > fetches; })) << "the rewind never reached the OffsetFetch";
    f.r.gw.holdOffsetFetch = false;
    f.r.gw.heldOffsetFetches.clear();
    const int connects = f.r.gw.connectsAt[0];
    f.r.gw.expireSession(f.r.seedSid);
    ASSERT_TRUE(driveUntilDone(f.r, f.done, a, 40'000'000));
    EXPECT_EQ(f.done[a].c.status, rc::OpStatus::SessionLost);
    // The consumer stays Stable across the reconnect; wait for the new session, then rewind again.
    ASSERT_TRUE(f.r.drive([&] { return f.r.gw.connectsAt[0] > connects; }, 40'000'000)) << "the seed never reconnected";
    for (int i = 0; i < 2'000; ++i) { f.r.client.doWork(); f.r.gw.pump(); }
    const rc::Token b = f.tok();
    ASSERT_EQ(f.gc.seekToCommitted(rc::CompletionTarget(f.r.cq), b), rc::OpResult::Accepted);
    ASSERT_TRUE(driveUntilDone(f.r, f.done, b, 40'000'000));
    EXPECT_EQ(f.done[b].c.status, rc::OpStatus::Ok);
    collect(f.r, f.done);
    EXPECT_EQ(f.done[a].times, 1);
    EXPECT_EQ(f.done[b].times, 1);
}

// F3: the ring encoding of a HubCommand carries every field (including the second payload word `e`).
TEST(HubCommandMsgRegression, EncodeDecodeRoundTripsEveryFieldRegression) {
    using rune::client::agent::ring_msg::HubCommandMsg;
    static_assert(HubCommandMsg::ENCODED_SIZE == 80, "the HubCommand frame is 24 header + 56 body bytes");
    rune::client::detail::HubCommand c;
    c.op     = rune::client::detail::HubCommand::Op::GroupSeek;
    c.handle = 0x1122334455667788ULL;
    c.a      = 0x0102030405060708LL;
    c.b      = -0x1112131415161718LL;
    c.c      = 0x2122232425262728LL;
    c.d      = -0x3132333435363738LL;
    c.e      = 0x4142434445464748LL;
    uint8_t buf[HubCommandMsg::ENCODED_SIZE]{};
    ASSERT_EQ(HubCommandMsg::encode(c, buf), HubCommandMsg::ENCODED_SIZE);
    const auto d = HubCommandMsg::decode(buf, sizeof buf);
    ASSERT_TRUE(d.has_value());
    EXPECT_EQ(d->op, c.op);
    EXPECT_EQ(d->handle, c.handle);
    EXPECT_EQ(d->a, c.a);
    EXPECT_EQ(d->b, c.b);
    EXPECT_EQ(d->c, c.c);
    EXPECT_EQ(d->d, c.d);
    EXPECT_EQ(d->e, c.e);
    EXPECT_FALSE(HubCommandMsg::decode(buf, sizeof buf - 1).has_value()) << "a truncated frame is refused";
}

// F4: a rebalance during a rewind completes it Rejoined; driving on through the rejoin to Stable and further
// cycles never completes it again.
TEST(GroupSeekRegression, RebalanceDuringARewindCompletesRejoinedExactlyOnceRegression) {
    Fixture f(4914, 4915, {{kTopic, 1}}, Fixture::cfgOf(false, 600, 5000));   // a heartbeat every 200 ms
    const auto firstSub = *f.r.gw.latestSubscription(kTopic, 1);
    f.r.gw.holdOffsetFetch = true;
    const rc::Token t = f.tok();
    ASSERT_EQ(f.gc.seekToCommitted(rc::CompletionTarget(f.r.cq), t), rc::OpResult::Accepted);
    f.r.gw.rebalancing = true;
    ASSERT_TRUE(driveUntilDone(f.r, f.done, t, 40'000'000));
    EXPECT_EQ(f.done[t].c.status, rc::OpStatus::Rejoined);
    f.r.gw.rebalancing = false;
    f.r.gw.holdOffsetFetch = false;
    f.r.gw.releaseOffsetFetches();   // the old fetch's answer arrives late
    ASSERT_TRUE(f.r.drive([&] { return !f.r.gw.pendingJoins.empty(); }, 40'000'000));
    f.r.gw.releaseJoins();
    ASSERT_TRUE(f.r.drive([&] {
        const auto* s = f.r.gw.latestSubscription(kTopic, 1);
        return s != nullptr && s->corrId != firstSub.corrId;
    }, 40'000'000)) << "the member never reached Stable again";
    for (int i = 0; i < 2'000; ++i) { f.r.client.doWork(); f.r.gw.pump(); }
    collect(f.r, f.done);
    EXPECT_EQ(f.done[t].times, 1) << "the rewind completed again after the rejoin";
}

// F5: a rewind is not Ok until the new subscription of the rewound partition is answered.
TEST(GroupSeekRegression, ARewindIsNotOkUntilItsSubscribeIsAnsweredRegression) {
    Fixture f(4916, 4917, {{kTopic, 1}});
    f.r.gw.holdSubscribeResponses = true;
    const std::size_t subs = f.r.gw.subscribed.size();
    const rc::Token t = f.tok();
    ASSERT_EQ(f.gc.seekToCommitted(rc::CompletionTarget(f.r.cq), t), rc::OpResult::Accepted);
    ASSERT_TRUE(f.r.drive([&] { return f.r.gw.subscribed.size() > subs; })) << "the rewound partition was never re-subscribed";
    for (int i = 0; i < 3'000; ++i) { f.r.client.doWork(); f.r.gw.pump(); }
    collect(f.r, f.done);
    EXPECT_EQ(f.done.count(t), 0u) << "completed before the Subscribe was answered";
    f.r.gw.releaseSubscribeResponses();
    ASSERT_TRUE(driveUntilDone(f.r, f.done, t, 40'000'000));
    EXPECT_EQ(f.done[t].c.status, rc::OpStatus::Ok);
    EXPECT_EQ(f.done[t].times, 1);
}

// ===========================================================================
// C3 adversarial review (#378 / #413): the typed error covers EVERY non-Ok exit of a blocking group call.
// ===========================================================================

// #378: the most common failure of a blocking rewind is the caller's own wait budget running out (the OffsetFetch is
// never answered).  The header documents "GroupOpError ... on any other non-Ok outcome or wait timeout", so a caller
// that catches GroupOpError and branches on status() must see Timeout here.  RED today: Client::andWait_ throws a
// plain std::runtime_error("...: timed out waiting") for Completions::Wait::TimedOut, which escapes the
// GroupOpError handler.
TEST(GroupSeekRegression, ABlockingRewindWaitTimeoutIsATypedGroupOpErrorAdversarialRegression) {
    Fixture f(4996, 4997, {{kTopic, 1}});   // commitSyncTimeoutMs default: far above the 50 ms wait below
    f.r.gw.holdOffsetFetch = true;            // the rewind's OffsetFetch is never answered
    bool typed = false, untyped = false;
    rc::OpStatus st = rc::OpStatus::Ok;
    try {
        f.gc.seekToCommittedAndWait(std::chrono::milliseconds(50));
    } catch (const rc::GroupOpError& e) {
        typed = true;
        st    = e.status();
    } catch (const std::runtime_error&) {
        untyped = true;
    }
    EXPECT_FALSE(untyped) << "the wait timeout escaped as a plain std::runtime_error";
    EXPECT_TRUE(typed);
    EXPECT_EQ(st, rc::OpStatus::Timeout);
}

// ===========================================================================
// C16 (#371, #357): a rewind progresses on the duty pass, not on the 100 ms sweep
// ===========================================================================

// #371: with the clock frozen (so no 100 ms sweep can ever fire) seekToCommitted still sends its OffsetFetch,
// takes the answer, resubscribes and completes Ok.  RED before the fix: the OffsetFetch and the completion were
// reachable only from the sweep (never completes with the clock frozen).
TEST(GroupSeekRegression, SeekToCommittedCompletesWithoutWaitingForTheSweepRegression) {
    Fixture f(5601, 5602, {{kTopic, 1}}, Fixture::cfgOf(false, 30000, 5000), {{{kTopic, 1}, 40}}, manualClockOptions());
    freeze(f.r);
    const int fetches = f.r.gw.offsetFetches;
    const rc::Token t = f.tok();
    ASSERT_EQ(f.gc.seekToCommitted(rc::CompletionTarget(f.r.cq), t), rc::OpResult::Accepted);
    ASSERT_TRUE(driveUntilDone(f.r, f.done, t, 20'000)) << "the rewind needed the sweep";
    EXPECT_EQ(f.done[t].c.status, rc::OpStatus::Ok);
    EXPECT_EQ(f.done[t].times, 1);
    EXPECT_EQ(f.r.gw.offsetFetches, fetches + 1) << "exactly one OffsetFetch for the rewind";
    EXPECT_EQ(f.r.gw.latestSubscription(kTopic, 1)->fromOffset, 40);
}

TEST(GroupSeekRegression, SeekCompletesWithoutWaitingForTheSweepRegression) {
    Fixture f(5603, 5604, {{kTopic, 0}, {kTopic, 1}}, Fixture::cfgOf(false, 30000, 5000), {}, manualClockOptions());
    freeze(f.r);
    const rc::Token t = f.tok();
    ASSERT_EQ(f.gc.seek({kTopic, 1}, 7, rc::CompletionTarget(f.r.cq), t), rc::OpResult::Accepted);
    ASSERT_TRUE(driveUntilDone(f.r, f.done, t, 20'000)) << "the seek needed the sweep";
    EXPECT_EQ(f.done[t].c.status, rc::OpStatus::Ok);
    EXPECT_EQ(f.done[t].times, 1);
    EXPECT_EQ(f.r.gw.latestSubscription(kTopic, 1)->fromOffset, 7);
}

// ===========================================================================
// C16 (#357.1): a seek while the group is not Stable reports Rejoined, like seekToCommitted
// ===========================================================================

TEST(GroupSeekRegression, SeekDuringARebalanceCompletesRejoinedNotInvalidArgumentRegression) {
    Fixture f(5605, 5606, {{kTopic, 1}}, Fixture::cfgOf(false, 600, 5000));   // a heartbeat every 200 ms
    f.r.gw.rebalancing = true;
    ASSERT_TRUE(f.r.drive([&] { return !f.r.gw.pendingJoins.empty(); }, 40'000'000)) << "the member never went to rejoin";
    const rc::Token t = f.tok(), u = f.tok();
    ASSERT_EQ(f.gc.seek({kTopic, 1}, 5, rc::CompletionTarget(f.r.cq), t), rc::OpResult::Accepted);
    ASSERT_EQ(f.gc.seekToCommitted(rc::CompletionTarget(f.r.cq), u), rc::OpResult::Accepted);
    ASSERT_TRUE(driveUntilDone(f.r, f.done, t));
    ASSERT_TRUE(driveUntilDone(f.r, f.done, u));
    EXPECT_EQ(f.done[t].c.status, rc::OpStatus::Rejoined) << "seek and seekToCommitted must agree";
    EXPECT_EQ(f.done[u].c.status, rc::OpStatus::Rejoined);
    EXPECT_EQ(f.done[t].times, 1);
    // The blocking forms map it the same way: a typed GroupOpError(Rejoined), never std::invalid_argument.
    for (int form = 0; form < 2; ++form) {
        bool rejoined = false, other = false;
        try {
            if (form == 0) f.gc.seekAndWait({kTopic, 1}, 5, std::chrono::seconds(5));
            else           f.gc.seekToCommittedAndWait(std::chrono::seconds(5));
        } catch (const rc::GroupOpError& e) {
            rejoined = e.status() == rc::OpStatus::Rejoined;
        } catch (...) {
            other = true;
        }
        EXPECT_TRUE(rejoined) << "form " << form;
        EXPECT_FALSE(other) << "form " << form;
    }
}

// Guard: a partition that is NOT assigned while Stable keeps InvalidArgument (also in the blocking form).
TEST(GroupSeekRegression, SeekOnANonAssignedPartitionWhileStableStaysInvalidArgumentRegression) {
    Fixture f(5607, 5608, {{kTopic, 1}});
    const rc::Token t = f.tok();
    ASSERT_EQ(f.gc.seek({kTopic, 0}, 5, rc::CompletionTarget(f.r.cq), t), rc::OpResult::Accepted);
    ASSERT_TRUE(driveUntilDone(f.r, f.done, t));
    EXPECT_EQ(f.done[t].c.status, rc::OpStatus::InvalidArgument);
    EXPECT_EQ(f.done[t].text, "partition not assigned");
    EXPECT_THROW(f.gc.seekAndWait({kTopic, 0}, 5, std::chrono::seconds(5)), std::invalid_argument);
}

// ===========================================================================
// C16 (#357.5): a seek keeps an unrelated Assign OffsetFetch in flight
// ===========================================================================

// The Assign fetch (both partitions) is in flight when a seek positions p1.  Exactly ONE OffsetFetch goes on the
// wire: its answer seeds p0 from the committed offset and leaves the seeked p1 alone (the seek's offset wins even
// though the answer carries p1 too).  Mutations: an unconditional reset of the fetch -> two fetches; dropping the
// fence (staleFetchCorr) -> the answer moves p1 to its committed offset and the seeked records are dropped.
TEST(GroupSeekRegression, ASeekOnOnePartitionKeepsAnUnrelatedAssignFetchRegression) {
    Fixture f(5609, 5610, {{kTopic, 0}, {kTopic, 1}}, Fixture::cfgOf(false, 30000, 5000),
              {{{kTopic, 0}, 30}, {{kTopic, 1}, 55}}, manualClockOptions(), /*holdFetch=*/true);
    ASSERT_EQ(f.r.gw.offsetFetches, 1) << "the Assign fetch";
    const rc::Token t = f.tok();
    ASSERT_EQ(f.gc.seek({kTopic, 1}, 3, rc::CompletionTarget(f.r.cq), t), rc::OpResult::Accepted);
    ASSERT_TRUE(driveUntilDone(f.r, f.done, t, 20'000));
    EXPECT_EQ(f.done[t].c.status, rc::OpStatus::Ok);
    ASSERT_NE(f.r.gw.latestSubscription(kTopic, 1), nullptr);
    EXPECT_EQ(f.r.gw.latestSubscription(kTopic, 1)->fromOffset, 3);
    EXPECT_EQ(f.r.gw.latestSubscription(kTopic, 0), nullptr) << "p0 is unseeded until the Assign answer";
    // Many sweeps pass (300 ms of duty clock): no second fetch may go out.
    for (int i = 0; i < 4'000; ++i) { f.r.client.doWork(); f.r.gw.pump(); }
    EXPECT_EQ(f.r.gw.offsetFetches, 1) << "the seek cancelled the Assign fetch (a second OffsetFetch was sent)";
    // The held answer arrives: p0 is seeded from it, p1 keeps the seek.
    f.r.gw.holdOffsetFetch = false;
    f.r.gw.releaseOffsetFetches();
    ASSERT_TRUE(f.r.drive([&] { return f.r.gw.latestSubscription(kTopic, 0) != nullptr; }, 20'000))
        << "p0 was never seeded from the Assign answer";
    EXPECT_EQ(f.r.gw.latestSubscription(kTopic, 0)->fromOffset, 30);
    EXPECT_EQ(f.r.gw.offsetFetches, 1);
    const auto s1 = *f.r.gw.latestSubscription(kTopic, 1);
    EXPECT_EQ(s1.fromOffset, 3);
    f.r.gw.pushRecords(s1, 3, 7);
    ASSERT_TRUE(f.consumed(1, 7)) << "the answer moved the seeked partition: its records were dropped";
    EXPECT_EQ(f.seen.offsets[1].front(), 3);
}

// ===========================================================================
// C16 (#357.4): FetchSub::rewound is state - every phase handles every event
// ===========================================================================

namespace {

// A bare hub (no Client, no network): one Stable consumer over p0 / p1 with both partitions bound.  The router is a
// fake that accepts every Subscribe; completions go straight to the handler target.
struct PhaseRig {
    using Hub   = rune::client::group::GroupConsumerHub;
    using Impl  = rune::client::GroupConsumerImpl;
    using Phase = Impl::FetchPhase;
    static constexpr int64_t kSid = 77;

    struct Seenc { int times = 0; rc::OpStatus status = rc::OpStatus::Ok; };

    Hub                     hub;
    std::unique_ptr<Impl>   impl = std::make_unique<Impl>();
    std::map<rc::Token, Seenc> seen;
    int64_t                 now = 1'000'000'000;
    int                     subscribes = 0;
    rune::client::group::SendFn send = [](int32_t, const uint8_t*, std::size_t) { return true; };
    const rc::TopicPartition p0{kTopic, 0}, p1{kTopic, 1};

    static uint32_t leader(void*, int32_t, int32_t) { return 0; }
    static int64_t  sid(void*, uint32_t) { return kSid; }
    static uint64_t epoch(void*, uint32_t) { return 1; }
    static rune::client::detail::SendResult offer(void* ctx, uint32_t, const uint8_t*, std::size_t) {
        ++static_cast<PhaseRig*>(ctx)->subscribes;
        return rune::client::detail::SendResult::Accepted;
    }
    static void onDone(void* ctx, const rc::Completion& c, std::span<const uint8_t>) noexcept {
        auto& s = static_cast<PhaseRig*>(ctx)->seen[c.token];
        ++s.times;
        s.status = c.status;
    }

    PhaseRig() {
        Hub::FetchRouter fr;
        fr.ctx = this; fr.leaderSession = &leader; fr.sessionId = &sid; fr.epoch = &epoch; fr.offer = &offer;
        hub.setFetchRouter(fr);
        hub.setSessionConnected(true, kSid, 1);
        impl->groupId      = "g";
        impl->topicIds     = {kTopic};
        impl->cfg.autoCommit = false;
        impl->memberId     = "m";
        impl->generationId = 1;
        impl->state        = rc::GroupConsumerState::Stable;
        impl->active       = true;
        impl->assignment[p0] = 0;
        impl->assignment[p1] = 0;
        impl->fetch[p0].offsetKnown = true;
        impl->fetch[p1].offsetKnown = true;
        hub.add(impl.get());
        (void)hub.reconcileFetch();
        bindAll();
    }
    void bindAll() {
        int64_t n = 100;
        for (auto& [tp, fs] : impl->fetch) {
            if (fs.epoch != 0 && fs.subscriptionId == 0 && fs.corrId != 0) (void)hub.onFetchSubscribeResponse(fs.corrId, 0, ++n);
        }
    }
    void drive() { for (int i = 0; i < 3; ++i) { now += 1'000; (void)hub.driveRewinds(now, kSid, send); } }
    rune::client::detail::HubCommand cmd(rune::client::detail::HubCommand::Op op, rc::Token t, int64_t off = 0,
                                         rc::TopicPartition tp = {}) {
        rune::client::detail::HubCommand c;
        c.op = op; c.handle = reinterpret_cast<uint64_t>(impl.get()); c.a = static_cast<int64_t>(t); c.d = off;
        c.e = static_cast<int64_t>((static_cast<uint64_t>(static_cast<uint32_t>(tp.topicId)) << 32) | static_cast<uint32_t>(tp.partition));
        rune::client::detail::OpTarget target;
        target.handler = rc::CompletionHandler{&onDone, this};
        rune::client::detail::encodeTarget(target, c);
        return c;
    }
    using Op = rune::client::detail::HubCommand::Op;
    void seek(rc::Token t, int64_t off) { hub.applyCommandDirect(cmd(Op::GroupSeek, t, off, p1), now); }
    void seekToCommitted(rc::Token t) { hub.applyCommandDirect(cmd(Op::GroupSeekToCommitted, t), now); }
    // The OffsetFetch answer to the latest fetch: p0 -> 10, p1 -> 20.
    void answerOffsetFetch() {
        if (impl->offsetFetchCorrId == 0) impl->offsetFetchCorrId = Hub::kOffsetFetchCorrTag | 999;   // an answer needs a fetch in flight
        rune::protocol::client::OffsetFetchResult r[2]{};
        r[0].topicId = kTopic; r[0].partition = 0; r[0].offset = 10;
        r[1].topicId = kTopic; r[1].partition = 1; r[1].offset = 20;
        rune::protocol::client::OffsetFetchResponse m;
        m.sessionId = kSid; m.correlationId = impl->offsetFetchCorrId;
        std::vector<uint8_t> buf(rune::protocol::client::OffsetFetchResponse::encodedSizeFor(2));
        const std::size_t n = m.encode(buf.data(), std::span<const rune::protocol::client::OffsetFetchResult>(r, 2));
        hub.onOffsetFetchResponse(buf.data(), n);
    }
    Phase phaseOf(const rc::TopicPartition& tp) const { return impl->fetch.at(tp).phase; }
};

enum class Start { AwaitingOffset, AwaitingRebind, Bound };
enum class Event { OffsetAnswered, RebindAnswered, Revoke, Rejoin, Close, SecondSeek };

struct Expect {
    bool        entry;        // p1's FetchSub still exists
    PhaseRig::Phase phase;    // meaningful when entry
    int         t1Times;      // completions of the first rewind
    rc::OpStatus t1Status;    // meaningful when t1Times == 1
    int         t2Times;      // completions of the second rewind (SecondSeek only: pending => 0)
};

} // namespace

TEST(GroupSeekRegression, EveryFetchPhaseHandlesEveryEventRegression) {
    using P = PhaseRig::Phase;
    using S = rc::OpStatus;
    struct Row { Start start; Event ev; Expect x; const char* name; };
    const Row rows[] = {
        // ---- RewindAwaitingOffset: seekToCommitted, the OffsetFetch answer not yet in ----
        {Start::AwaitingOffset, Event::OffsetAnswered, {true, P::Bound, 1, S::Ok, 0}, "AwaitingOffset+OffsetAnswered (seed -> rebind -> bound; Ok once the rebind is answered)"},
        {Start::AwaitingOffset, Event::RebindAnswered, {true, P::RewindAwaitingOffset, 0, S::Ok, 0}, "AwaitingOffset+RebindAnswered (no subscription exists: no-op)"},
        {Start::AwaitingOffset, Event::Revoke,         {false, P::Bound, 1, S::Rejoined, 0}, "AwaitingOffset+Revoke"},
        {Start::AwaitingOffset, Event::Rejoin,         {false, P::Bound, 1, S::Rejoined, 0}, "AwaitingOffset+Rejoin"},
        {Start::AwaitingOffset, Event::Close,          {false, P::Bound, 1, S::Closed, 0}, "AwaitingOffset+Close"},
        {Start::AwaitingOffset, Event::SecondSeek,     {true, P::RewindAwaitingRebind, 1, S::Superseded, 0}, "AwaitingOffset+SecondSeek"},
        // ---- RewindAwaitingRebind: seek p1, the new subscription not yet answered ----
        {Start::AwaitingRebind, Event::OffsetAnswered, {true, P::RewindAwaitingRebind, 0, S::Ok, 0}, "AwaitingRebind+OffsetAnswered (never moves a seeked partition)"},
        {Start::AwaitingRebind, Event::RebindAnswered, {true, P::Bound, 1, S::Ok, 0}, "AwaitingRebind+RebindAnswered"},
        {Start::AwaitingRebind, Event::Revoke,         {false, P::Bound, 1, S::Rejoined, 0}, "AwaitingRebind+Revoke"},
        {Start::AwaitingRebind, Event::Rejoin,         {false, P::Bound, 1, S::Rejoined, 0}, "AwaitingRebind+Rejoin"},
        {Start::AwaitingRebind, Event::Close,          {false, P::Bound, 1, S::Closed, 0}, "AwaitingRebind+Close"},
        {Start::AwaitingRebind, Event::SecondSeek,     {true, P::RewindAwaitingRebind, 1, S::Superseded, 0}, "AwaitingRebind+SecondSeek"},
        // ---- Bound: the rewind already completed Ok ----
        {Start::Bound, Event::OffsetAnswered, {true, P::Bound, 1, S::Ok, 0}, "Bound+OffsetAnswered"},
        {Start::Bound, Event::RebindAnswered, {true, P::Bound, 1, S::Ok, 0}, "Bound+RebindAnswered (duplicate)"},
        {Start::Bound, Event::Revoke,         {false, P::Bound, 1, S::Ok, 0}, "Bound+Revoke"},
        {Start::Bound, Event::Rejoin,         {false, P::Bound, 1, S::Ok, 0}, "Bound+Rejoin"},
        {Start::Bound, Event::Close,          {false, P::Bound, 1, S::Ok, 0}, "Bound+Close"},
        {Start::Bound, Event::SecondSeek,     {true, P::RewindAwaitingRebind, 1, S::Ok, 0}, "Bound+SecondSeek"},
    };
    constexpr rc::Token T1 = 1, T2 = 2;
    for (const Row& row : rows) {
        SCOPED_TRACE(row.name);
        PhaseRig r;
        // ---- reach the start phase ----
        switch (row.start) {
        case Start::AwaitingOffset:
            r.seekToCommitted(T1);
            r.drive();   // sends the OffsetFetch; the answer is held back
            ASSERT_EQ(r.phaseOf(r.p1), P::RewindAwaitingOffset);
            ASSERT_EQ(r.phaseOf(r.p0), P::RewindAwaitingOffset);
            break;
        case Start::AwaitingRebind:
            r.seek(T1, 3);
            ASSERT_EQ(r.phaseOf(r.p1), P::RewindAwaitingRebind);
            ASSERT_EQ(r.phaseOf(r.p0), P::Bound) << "a seek restarts one partition only";
            break;
        case Start::Bound:
            r.seek(T1, 3);
            r.bindAll();
            r.drive();
            ASSERT_EQ(r.phaseOf(r.p1), P::Bound);
            ASSERT_EQ(r.seen[T1].times, 1);
            break;
        }
        // ---- the event ----
        switch (row.ev) {
        case Event::OffsetAnswered:
            r.answerOffsetFetch();
            if (row.start == Start::AwaitingOffset) {
                EXPECT_EQ(r.phaseOf(r.p1), P::RewindAwaitingRebind) << "the answer seeds it and starts the rebind wait";
                EXPECT_EQ(r.impl->assignment[r.p1], 20);
                r.bindAll();   // now the subscription instance answers
            } else {
                EXPECT_EQ(r.impl->assignment[r.p1], row.start == Start::AwaitingRebind ? 3 : r.impl->assignment[r.p1])
                    << "an answer never moves a seeked partition";
            }
            break;
        case Event::RebindAnswered: r.bindAll(); break;
        case Event::Revoke:
            r.impl->assignment.erase(r.p1);
            ++r.impl->generationId;
            (void)r.hub.reconcileFetch();
            break;
        case Event::Rejoin: r.hub.onCoordinatorMoved(); break;
        case Event::Close:
            r.hub.applyCommandDirect(r.cmd(PhaseRig::Op::GroupClose, 0), r.now);
            break;
        case Event::SecondSeek: r.seek(T2, 5); break;
        }
        r.drive();
        r.drive();   // more passes never complete anything again
        // ---- the outcome ----
        const Expect& x = row.x;
        EXPECT_EQ(r.impl->fetch.count(r.p1) != 0, x.entry);
        if (x.entry && r.impl->fetch.count(r.p1) != 0) EXPECT_EQ(r.phaseOf(r.p1), x.phase);
        if (row.ev == Event::SecondSeek) {
            ASSERT_EQ(r.impl->fetch.count(r.p0), 1u);
            EXPECT_EQ(r.phaseOf(r.p0), P::Bound) << "the superseded rewind released every partition it owned";
        }
        EXPECT_EQ(r.seen[T1].times, x.t1Times);
        if (x.t1Times == 1) EXPECT_EQ(r.seen[T1].status, x.t1Status);
        EXPECT_EQ(r.seen[T2].times, x.t2Times) << "the second rewind is still pending";
    }
}

// ===========================================================================
// C16 adversarial review (#357, #371): the rewind state machine and its OffsetFetch, attacked through the public API
// ===========================================================================

#include "../support/AllocCounter.h"

namespace {
std::size_t passesUntil(GroupRig& r, std::map<rc::Token, Done>& out, int n) {
    for (int i = 0; i < n; ++i) { r.client.doWork(); r.gw.pump(); collect(r, out); }
    return out.size();
}
} // namespace

// #371 x pacing: a seekToCommitted whose OffsetFetch is answered UnstableOffsetCommit (a transaction holds the key)
// is re-fetched on the duty pass now, so the pacing is the only thing between the pass and a storm.  With the clock
// frozen, 20k passes send exactly ONE OffsetFetch (the next one is due 100 ms of duty time later); once the hold
// lifts and the clock moves the rewind completes Ok exactly once at the committed offset.
// Mutation: drop the Idle pacing check in retryOffsetFetchFor_ -> one OffsetFetch per pass.
TEST(GroupSeekRegression, AHeldOffsetNeverStormsOffsetFetchesFromTheDutyPassAdversarialRegression) {
    Fixture f(5711, 5712, {{kTopic, 1}}, Fixture::cfgOf(false, 30000, 5000), {{{kTopic, 1}, 40}}, manualClockOptions());
    freeze(f.r);
    const int fetches = f.r.gw.offsetFetches;
    f.r.gw.offsetFetchErrorCode  = static_cast<int16_t>(rune::protocol::client::ErrorCode::UnstableOffsetCommit);
    f.r.gw.offsetFetchErrorsNext = 1'000'000;
    const rc::Token t = f.tok();
    ASSERT_EQ(f.gc.seekToCommitted(rc::CompletionTarget(f.r.cq), t), rc::OpResult::Accepted);
    (void)passesUntil(f.r, f.done, 20'000);
    EXPECT_EQ(f.r.gw.offsetFetches, fetches + 1) << "the duty pass re-sent the held OffsetFetch without pacing";
    EXPECT_EQ(f.done.count(t), 0u) << "a held offset ended the rewind";
    EXPECT_FALSE(f.gc.finished()) << "UnstableOffsetCommit was charged to the fetch budget";
    // The transaction completes; duty time moves: the next paced fetch seeds and the rewind completes.
    f.r.gw.offsetFetchErrorsNext = 0;
    f.r.opt.stepNs = 100'000;
    ASSERT_TRUE(driveUntilDone(f.r, f.done, t, 40'000));
    EXPECT_EQ(f.done[t].c.status, rc::OpStatus::Ok);
    EXPECT_EQ(f.done[t].times, 1);
    EXPECT_LE(f.r.gw.offsetFetches, fetches + 3) << "paced: 100 ms, then the answer";
    EXPECT_EQ(f.r.gw.latestSubscription(kTopic, 1)->fromOffset, 40);
}

// #357.5 x duplicate answers: the Assign answer is replayed AFTER it already seeded p0 and the seeked p1 is Bound
// (the answered corr is never cleared, so the replay passes the correlation check).  The fence still holds: p1 keeps
// the seek.  Then a seekToCommitted takes a fresh fetch whose answer DOES move p1 (the fence never outlives its
// fetch).  Mutation: drop `staleFetchCorr == corr` from onOffsetFetchResponse -> red.
TEST(GroupSeekRegression, AReplayedAssignAnswerNeverMovesABoundSeekAdversarialRegression) {
    Fixture f(5713, 5714, {{kTopic, 0}, {kTopic, 1}}, Fixture::cfgOf(false, 30000, 5000),
              {{{kTopic, 0}, 30}, {{kTopic, 1}, 55}}, manualClockOptions(), /*holdFetch=*/true);
    const rc::Token t = f.tok();
    ASSERT_EQ(f.gc.seek({kTopic, 1}, 3, rc::CompletionTarget(f.r.cq), t), rc::OpResult::Accepted);
    ASSERT_TRUE(driveUntilDone(f.r, f.done, t, 20'000));
    ASSERT_EQ(f.done[t].c.status, rc::OpStatus::Ok);
    ASSERT_EQ(f.r.gw.heldOffsetFetches.size(), 1u);
    const std::vector<uint8_t> answer = f.r.gw.heldOffsetFetches.front();
    f.r.gw.holdOffsetFetch = false;
    f.r.gw.releaseOffsetFetches();
    ASSERT_TRUE(f.r.drive([&] { return f.r.gw.latestSubscription(kTopic, 0) != nullptr; }, 20'000));
    const auto s1 = *f.r.gw.latestSubscription(kTopic, 1);
    ASSERT_EQ(s1.fromOffset, 3);
    for (int dup = 0; dup < 3; ++dup) f.r.gw.send(0, answer.data(), answer.size());
    (void)passesUntil(f.r, f.done, 2'000);
    EXPECT_EQ(f.r.gw.latestSubscription(kTopic, 1)->corrId, s1.corrId) << "a replayed answer re-made the seeked fetch";
    f.r.gw.pushRecords(s1, 3, 7);   // 3..9
    ASSERT_TRUE(f.consumed(1, 7)) << "a replayed answer moved the seeked partition: its records were dropped";
    EXPECT_EQ(f.seen.offsets[1].front(), 3);
    // A later seekToCommitted is not fenced: its own fetch moves p1 to the committed offset.
    const rc::Token u = f.tok();
    ASSERT_EQ(f.gc.seekToCommitted(rc::CompletionTarget(f.r.cq), u), rc::OpResult::Accepted);
    ASSERT_TRUE(driveUntilDone(f.r, f.done, u, 20'000));
    EXPECT_EQ(f.done[u].c.status, rc::OpStatus::Ok);
    EXPECT_EQ(f.r.gw.latestSubscription(kTopic, 1)->fromOffset, 55) << "the fence outlived its fetch";
    EXPECT_EQ(f.r.gw.latestSubscription(kTopic, 0)->fromOffset, 30);
}

// #357.5 x a second seek: both partitions are seeked (p1, then p0 - which superseded nothing, p1's seek completed)
// while the Assign fetch is in flight.  Its answer moves neither.  p0's seek is the first position p0 ever had (the
// fetch was its only seed).  Mutation: drop the fence -> p1 jumps to 55 (and/or p0 to 30).
TEST(GroupSeekRegression, TwoSeeksUnderOneInFlightAssignFetchBothHoldAdversarialRegression) {
    Fixture f(5715, 5716, {{kTopic, 0}, {kTopic, 1}}, Fixture::cfgOf(false, 30000, 5000),
              {{{kTopic, 0}, 30}, {{kTopic, 1}, 55}}, manualClockOptions(), /*holdFetch=*/true);
    const rc::Token t = f.tok(), u = f.tok();
    ASSERT_EQ(f.gc.seek({kTopic, 1}, 3, rc::CompletionTarget(f.r.cq), t), rc::OpResult::Accepted);
    ASSERT_TRUE(driveUntilDone(f.r, f.done, t, 20'000));
    ASSERT_EQ(f.gc.seek({kTopic, 0}, 4, rc::CompletionTarget(f.r.cq), u), rc::OpResult::Accepted);
    ASSERT_TRUE(driveUntilDone(f.r, f.done, u, 20'000));
    EXPECT_EQ(f.done[t].c.status, rc::OpStatus::Ok);
    EXPECT_EQ(f.done[u].c.status, rc::OpStatus::Ok);
    EXPECT_EQ(f.r.gw.offsetFetches, 1);
    const auto s0 = *f.r.gw.latestSubscription(kTopic, 0);
    const auto s1 = *f.r.gw.latestSubscription(kTopic, 1);
    EXPECT_EQ(s0.fromOffset, 4);
    EXPECT_EQ(s1.fromOffset, 3);
    f.r.gw.holdOffsetFetch = false;
    f.r.gw.releaseOffsetFetches();
    (void)passesUntil(f.r, f.done, 2'000);
    f.r.gw.pushRecords(s0, 4, 2);
    f.r.gw.pushRecords(s1, 3, 2);
    ASSERT_TRUE(f.r.drive([&] { return f.seen.offsets[0].size() >= 2 && f.seen.offsets[1].size() >= 2; }, 20'000))
        << "the in-flight answer moved a seeked partition (p0 " << f.seen.offsets[0].size() << ", p1 "
        << f.seen.offsets[1].size() << ")";
    EXPECT_EQ(f.seen.offsets[0].front(), 4);
    EXPECT_EQ(f.seen.offsets[1].front(), 3);
    EXPECT_EQ(f.done[t].times, 1);
    EXPECT_EQ(f.done[u].times, 1);
}

// Concurrency x generation: an Assign of a new generation lands while seekToCommitted waits for its (held) fetch,
// with the clock frozen.  The rewind completes Rejoined exactly once on the duty pass; the stale answer released
// afterwards completes nothing again and seeds nothing; the new generation's own fetch seeds the partition.
TEST(GroupSeekRegression, AnAssignMidRewindCompletesRejoinedOnceAndTheStaleAnswerIsInertAdversarialRegression) {
    Fixture f(5717, 5718, {{kTopic, 1}}, Fixture::cfgOf(false, 30000, 5000), {{{kTopic, 1}, 40}}, manualClockOptions());
    freeze(f.r);
    f.r.gw.holdOffsetFetch = true;
    const int fetches = f.r.gw.offsetFetches;
    const rc::Token t = f.tok();
    ASSERT_EQ(f.gc.seekToCommitted(rc::CompletionTarget(f.r.cq), t), rc::OpResult::Accepted);
    ASSERT_TRUE(f.r.drive([&] { return f.r.gw.offsetFetches == fetches + 1; }, 20'000));
    f.r.gw.committedOffsets[{kTopic, 1}] = 44;   // a commit lands between the two fetches
    f.r.gw.sendAssign(f.r.gw.groupSessionIds.back(), 2, {{kTopic, 1}});
    ASSERT_TRUE(driveUntilDone(f.r, f.done, t, 20'000)) << "the Assign never ended the rewind";
    EXPECT_EQ(f.done[t].c.status, rc::OpStatus::Rejoined);
    ASSERT_TRUE(f.r.drive([&] { return f.r.gw.offsetFetches == fetches + 2; }, 20'000)) << "the Assign's own fetch";
    // Release the stale answer (40) and the Assign's answer (44) together.
    f.r.gw.holdOffsetFetch = false;
    f.r.gw.releaseOffsetFetches();
    ASSERT_TRUE(f.r.drive([&] {
        collect(f.r, f.done);
        const auto* s = f.r.gw.latestSubscription(kTopic, 1);
        return s != nullptr && s->fromOffset == 44;
    }, 20'000)) << "the new generation's answer never seeded the partition";
    (void)passesUntil(f.r, f.done, 2'000);
    EXPECT_EQ(f.done[t].times, 1) << "a rewind completed twice";
    EXPECT_EQ(f.r.gw.latestSubscription(kTopic, 1)->fromOffset, 44) << "the stale answer seeded the partition";
}

// Whole-area finding (pre-existing; OffsetFetch seeding): a COMPLETED seek backwards below the committed offset is
// silently undone by an Assign that keeps the partition (the coordinator's incremental assignment, Stable
// throughout): onAssign re-fetches the WHOLE assignment and the answer seeds a Bound, offset-known partition up to
// its committed offset (`position < committed`), so every replayed record of the still-bound subscription is then
// dropped as a "duplicate".  Fixed: onAssign fetches only the offset-unknown (newly added) partitions.
TEST(GroupSeekRegression, ACompletedSeekBackwardsSurvivesAnAssignThatKeepsThePartitionAdversarialRegression) {
    Fixture f(5719, 5720, {{kTopic, 0}, {kTopic, 1}}, Fixture::cfgOf(false, 30000, 5000),
              {{{kTopic, 0}, 30}, {{kTopic, 1}, 55}});
    const rc::Token t = f.tok();
    ASSERT_EQ(f.gc.seek({kTopic, 1}, 3, rc::CompletionTarget(f.r.cq), t), rc::OpResult::Accepted);
    ASSERT_TRUE(driveUntilDone(f.r, f.done, t));
    ASSERT_EQ(f.done[t].c.status, rc::OpStatus::Ok);
    const auto s1 = *f.r.gw.latestSubscription(kTopic, 1);
    ASSERT_EQ(s1.fromOffset, 3);
    f.r.gw.pushRecords(s1, 3, 2);   // 3, 4 replayed
    ASSERT_TRUE(f.consumed(1, 2));
    const int fetches = f.r.gw.offsetFetches;
    f.r.gw.sendAssign(f.r.gw.groupSessionIds.back(), 2, {{kTopic, 0}, {kTopic, 1}});   // both kept
    for (int i = 0; i < 2'000; ++i) { f.r.client.doWork(); f.r.gw.pump(); }   // the Assign (and any fetch) applied
    EXPECT_EQ(f.r.gw.offsetFetches, fetches) << "an Assign that only keeps known partitions re-fetched them";
    const auto now1 = *f.r.gw.latestSubscription(kTopic, 1);
    EXPECT_EQ(now1.corrId, s1.corrId) << "the kept partition was re-subscribed";
    f.r.gw.pushRecords(now1, 5, 10);   // the replay goes on: 5..14
    EXPECT_TRUE(f.consumed(1, 12)) << "the Assign answer moved the seeked partition to its committed offset: the replay "
                                      "5..14 was dropped (delivered " << f.seen.offsets[1].size() << ")";
}

// Hot path: driveRewinds runs on EVERY duty pass.  Idle (no rewind) and with a rewind waiting for its fetch (the
// fetch re-sent every 50 ms of duty time), thousands of passes allocate nothing.
TEST(GroupSeekRegression, DriveRewindsAllocatesNothingIdleOrPendingAdversarialRegression) {
    PhaseRig r;
    r.impl->cfg.sessionTimeoutMs = 50;   // a 50 ms answer timeout: the pending fetch is re-sent many times below
    for (int i = 0; i < 100; ++i) { r.now += 1'000'000; (void)r.hub.driveRewinds(r.now, PhaseRig::kSid, r.send); }
    alloc_counter::startThisThread();
    int idleWork = 0;
    for (int i = 0; i < 5'000; ++i) { r.now += 1'000'000; idleWork += r.hub.driveRewinds(r.now, PhaseRig::kSid, r.send); }
    const long idleAllocs = alloc_counter::stopThisThread();
    EXPECT_EQ(idleAllocs, 0) << "driveRewinds allocated with no rewind pending";
    EXPECT_EQ(idleWork, 0);
    constexpr rc::Token T = 9;
    int sends = 0;
    r.send = [&sends](int32_t, const uint8_t*, std::size_t) { ++sends; return true; };
    r.seekToCommitted(T);
    for (int i = 0; i < 300; ++i) { r.now += 1'000'000; (void)r.hub.driveRewinds(r.now, PhaseRig::kSid, r.send); }   // warm: scratch sized
    const int warmSends = sends;
    alloc_counter::startThisThread();
    for (int i = 0; i < 5'000; ++i) { r.now += 1'000'000; (void)r.hub.driveRewinds(r.now, PhaseRig::kSid, r.send); }
    const long pendingAllocs = alloc_counter::stopThisThread();
    EXPECT_EQ(pendingAllocs, 0) << "driveRewinds allocated while a rewind waited for its fetch";
    EXPECT_GT(sends, warmSends) << "the window never re-sent the fetch (the attack did not reach the send path)";
    EXPECT_EQ(r.seen[T].times, 0) << "the rewind ended inside the window (deadline 30 s)";
}

// ===========================================================================
// C18 (#685, #684): the OffsetFetch machine across a session drop, and a seek that supersedes seekToCommitted
// ===========================================================================

// #685: a seekToCommitted whose OffsetFetch answer is held (in flight) when the seed session drops.  The answer can
// never come over the new session; the fetch machine stayed Awaiting until sessionTimeoutMs (30 s here) before it
// re-sent.  Now the session transition resets it: a NEW OffsetFetch goes out within 500 ms of duty time after the
// reconnect.  Mutation: drop the resetOffsetFetch_ calls (setSessionConnected, onSessionDown) -> no new fetch.
TEST(GroupSeekRegression, SeekToCommittedRefetchesRightAfterASessionDropRegression) {
    Fixture f(5721, 5722, {{kTopic, 1}}, Fixture::cfgOf(false, 30000, 5000), {{{kTopic, 1}, 40}}, manualClockOptions());
    f.r.gw.holdOffsetFetch = true;
    const int fetches0 = f.r.gw.offsetFetches;
    const rc::Token t = f.tok();
    ASSERT_EQ(f.gc.seekToCommitted(rc::CompletionTarget(f.r.cq), t), rc::OpResult::Accepted);
    ASSERT_TRUE(f.r.drive([&] { return f.r.gw.offsetFetches > fetches0; }, 20'000)) << "the fetch never went out";
    const int fetchesHeld = f.r.gw.offsetFetches;

    const int connects = f.r.gw.connectsAt[0];
    f.r.gw.expireSession(f.r.seedSid);
    ASSERT_TRUE(f.r.drive([&] { return f.r.gw.connectsAt[0] > connects; }, 50'000)) << "the seed never reconnected";
    const int64_t t0 = g_clockNs;
    constexpr int64_t kWindowNs = 500'000'000LL;   // far below sessionTimeoutMs (30 s)
    EXPECT_TRUE(f.r.drive([&] { return f.r.gw.offsetFetches > fetchesHeld || g_clockNs - t0 > kWindowNs; }, 50'000));
    EXPECT_GT(f.r.gw.offsetFetches, fetchesHeld)
        << "no new OffsetFetch within 500 ms of duty time after the reconnect (waited for sessionTimeoutMs)";
}

// #684: a seek that supersedes a seekToCommitted leaves RewindResubscribe while the OTHER partition is still
// offset-unknown; its fetch waited for the Client's 100 ms sweep.  Both commands are issued back to back (one duty
// drain), so the seekToCommitted never sent its fetch.  The duty clock is frozen: the sweep cannot fire, so the
// OffsetFetch for p1 can only come from driveRewinds.  Mutation: restore the AwaitFetch-only condition -> none.
TEST(GroupSeekRegression, ASeekSupersedingSeekToCommittedFetchesTheOtherPartitionsAtOnceRegression) {
    Fixture f(5723, 5724, {{kTopic, 0}, {kTopic, 1}}, Fixture::cfgOf(false, 30000, 5000),
              {{{kTopic, 0}, 30}, {{kTopic, 1}, 55}}, manualClockOptions());
    freeze(f.r);
    f.r.gw.holdOffsetFetch = true;   // the answer is held: its entries show which partitions were asked
    const int fetches0 = f.r.gw.offsetFetches;
    const rc::Token t = f.tok(), u = f.tok();
    ASSERT_EQ(f.gc.seekToCommitted(rc::CompletionTarget(f.r.cq), t), rc::OpResult::Accepted);
    ASSERT_EQ(f.gc.seek({kTopic, 0}, 7, rc::CompletionTarget(f.r.cq), u), rc::OpResult::Accepted);
    (void)passesUntil(f.r, f.done, 200);   // frozen clock
    ASSERT_EQ(f.done[t].c.status, rc::OpStatus::Superseded);
    ASSERT_EQ(f.r.gw.offsetFetches, fetches0 + 1) << "no OffsetFetch for the offset-unknown partition on the duty pass";
    ASSERT_EQ(f.r.gw.heldOffsetFetches.size(), 1u);
    const auto& frame = f.r.gw.heldOffsetFetches.front();
    const auto rsp = rune::protocol::client::OffsetFetchResponse::decode(frame.data(), frame.size());
    ASSERT_TRUE(rsp.has_value());
    ASSERT_EQ(rsp->results.size(), 1u);
    EXPECT_EQ(rsp->results[0].partition, 1) << "the fetch must name p1 (p0 is positioned by the seek)";
}

// ===========================================================================
// C18 adversarial review (#685, #684): the OffsetFetch machine across a session drop and a superseding seek,
// attacked through the public API
// ===========================================================================

namespace {
// Drives until the seed session is Connected again after `connectsBefore` (a reconnect), then FREEZES the duty clock
// on that very pass: nothing paced (the 100 ms sweep) can run before the test acts.
bool reconnectAndFreeze(GroupRig& r, int connectsBefore, int cap = 200'000) {
    return r.drive([&] {
        if (r.gw.connectsAt[0] > connectsBefore &&
            r.client.testSessionStateAt(0) == rc::session::SessionState::Connected) {
            r.opt.stepNs = 0;
            return true;
        }
        return false;
    }, cap);
}
} // namespace

// #685 x protocol state (an answer of a dead session): seekToCommitted's OffsetFetch is in flight (held) when the
// seed session drops; while it is down the group's committed offsets move (a transactional commit: 30 -> 44,
// 55 -> 60).  After the reconnect the dead session's answer (30 / 55) arrives BEFORE the re-send.  It must not seed
// anything: the partitions start from the re-sent fetch's answer (44 / 60).  Before the fix the answer was matched by
// correlationId alone (offsetFetchCorrId survived resetOffsetFetch_), so it seeds 30 / 55 and records 30..43 and
// 55..59 are re-delivered although they were committed.
// Fix: resetOffsetFetch_ forgets offsetFetchCorrId.
TEST(GroupSeekRegression, ADeadSessionsOffsetFetchAnswerNeverSeedsAfterTheReconnectAdversarialRegression) {
    Fixture f(5725, 5726, {{kTopic, 0}, {kTopic, 1}}, Fixture::cfgOf(false, 30000, 5000),
              {{{kTopic, 0}, 30}, {{kTopic, 1}, 55}}, manualClockOptions());
    f.r.gw.holdOffsetFetch = true;
    const int fetches0 = f.r.gw.offsetFetches;
    const rc::Token t = f.tok();
    ASSERT_EQ(f.gc.seekToCommitted(rc::CompletionTarget(f.r.cq), t), rc::OpResult::Accepted);
    ASSERT_TRUE(f.r.drive([&] { return f.r.gw.offsetFetches > fetches0; }, 20'000));
    ASSERT_EQ(f.r.gw.heldOffsetFetches.size(), 1u);   // encoded now: 30 / 55
    f.r.gw.committedOffsets[{kTopic, 0}] = 44;
    f.r.gw.committedOffsets[{kTopic, 1}] = 60;
    const int fetchesHeld = f.r.gw.offsetFetches;
    const int connects    = f.r.gw.connectsAt[0];
    f.r.gw.expireSession(f.r.seedSid);
    ASSERT_TRUE(reconnectAndFreeze(f.r, connects)) << "the seed never reconnected";
    collect(f.r, f.done);
    ASSERT_EQ(f.done[t].c.status, rc::OpStatus::SessionLost);
    ASSERT_EQ(f.r.gw.offsetFetches, fetchesHeld) << "precondition: the re-send already went out";
    ASSERT_TRUE(f.r.gw.pendingJoins.empty()) << "precondition: the member rejoins (it must stay Stable)";
    // The dead session's answer lands first.
    f.r.gw.holdOffsetFetch = false;
    f.r.gw.releaseOffsetFetches();
    (void)passesUntil(f.r, f.done, 2'000);
    // Time moves: the re-sent fetch is answered with the current committed offsets.
    f.r.opt.stepNs = 100'000;
    const int64_t until = g_clockNs + 1'000'000'000LL;   // 1 s of duty time: ten sweeps
    ASSERT_TRUE(f.r.drive([&] { return g_clockNs >= until; }, 20'000));
    ASSERT_NE(f.r.gw.latestSubscription(kTopic, 0), nullptr);
    ASSERT_NE(f.r.gw.latestSubscription(kTopic, 1), nullptr);
    EXPECT_EQ(f.r.gw.latestSubscription(kTopic, 0)->fromOffset, 44) << "the dead session's answer seeded p0";
    EXPECT_EQ(f.r.gw.latestSubscription(kTopic, 1)->fromOffset, 60) << "the dead session's answer seeded p1";
}

// #685 x sibling entry point (the eager Assign fetch, not a seek): the Assign OffsetFetch is held when the seed
// session drops.  No rewind is in flight, so only the Client's paced sweep re-sends; it must do so right after the
// reconnect, not after sessionTimeoutMs (30 s), and the partitions then start at the committed offsets.
// Mutation: drop both resetOffsetFetch_ calls -> the fetch stays Awaiting for 30 s of duty time.
TEST(GroupSeekRegression, AnAssignFetchInFlightAtASessionDropIsResentAfterTheReconnectAdversarialRegression) {
    Fixture f(5727, 5728, {{kTopic, 0}, {kTopic, 1}}, Fixture::cfgOf(false, 30000, 5000),
              {{{kTopic, 0}, 30}, {{kTopic, 1}, 55}}, manualClockOptions(), /*holdFetch=*/true);
    ASSERT_EQ(f.r.gw.heldOffsetFetches.size(), 1u);
    ASSERT_EQ(f.r.gw.latestSubscription(kTopic, 0), nullptr);
    const int fetchesHeld = f.r.gw.offsetFetches;
    const int connects    = f.r.gw.connectsAt[0];
    f.r.gw.holdOffsetFetch = false;   // the re-sent fetch is answered at once; the dead one is never released
    f.r.gw.expireSession(f.r.seedSid);
    ASSERT_TRUE(reconnectAndFreeze(f.r, connects)) << "the seed never reconnected";
    ASSERT_TRUE(f.r.gw.pendingJoins.empty()) << "precondition: the member stays Stable (no rejoin)";
    f.r.opt.stepNs = 100'000;
    const int64_t t0 = g_clockNs;
    constexpr int64_t kWindowNs = 500'000'000LL;   // far below sessionTimeoutMs (30 s)
    ASSERT_TRUE(f.r.drive([&] {
        return (f.r.gw.latestSubscription(kTopic, 0) != nullptr && f.r.gw.latestSubscription(kTopic, 1) != nullptr) ||
               g_clockNs - t0 > kWindowNs;
    }, 50'000));
    EXPECT_GT(f.r.gw.offsetFetches, fetchesHeld) << "no OffsetFetch re-sent within 500 ms after the reconnect";
    ASSERT_NE(f.r.gw.latestSubscription(kTopic, 0), nullptr) << "p0 never subscribed after the reconnect";
    ASSERT_NE(f.r.gw.latestSubscription(kTopic, 1), nullptr) << "p1 never subscribed after the reconnect";
    EXPECT_EQ(f.r.gw.latestSubscription(kTopic, 0)->fromOffset, 30);
    EXPECT_EQ(f.r.gw.latestSubscription(kTopic, 1)->fromOffset, 55);
}

// #684 x resources (a storm): the seek that superseded a seekToCommitted stays in RewindResubscribe (its
// SubscribeResponse is held) while every OffsetFetch for the offset-unknown p1 is answered UnstableOffsetCommit.
// driveRewinds now calls retryOffsetFetchFor_ on EVERY pass in that state; with the clock frozen 20k passes must
// still send exactly ONE fetch (the pacing).  Then the hold lifts and time moves: the seek completes Ok once at 7
// and p1 starts at its committed 55.  Mutations: drop the Idle pacing check -> a fetch per pass; restore the
// AwaitFetch-only condition -> no fetch at all while frozen.
TEST(GroupSeekRegression, AResubscribingSeekNeverStormsOffsetFetchesForAHeldPartitionAdversarialRegression) {
    Fixture f(5729, 5730, {{kTopic, 0}, {kTopic, 1}}, Fixture::cfgOf(false, 30000, 5000),
              {{{kTopic, 0}, 30}, {{kTopic, 1}, 55}}, manualClockOptions());
    freeze(f.r);
    const int fetches0 = f.r.gw.offsetFetches;
    f.r.gw.offsetFetchErrorCode   = static_cast<int16_t>(rune::protocol::client::ErrorCode::UnstableOffsetCommit);
    f.r.gw.offsetFetchErrorsNext  = 1'000'000;
    f.r.gw.holdSubscribeResponses = true;
    const rc::Token t = f.tok(), u = f.tok();
    ASSERT_EQ(f.gc.seekToCommitted(rc::CompletionTarget(f.r.cq), t), rc::OpResult::Accepted);
    ASSERT_EQ(f.gc.seek({kTopic, 0}, 7, rc::CompletionTarget(f.r.cq), u), rc::OpResult::Accepted);
    (void)passesUntil(f.r, f.done, 20'000);   // frozen clock
    ASSERT_EQ(f.done[t].c.status, rc::OpStatus::Superseded);
    EXPECT_EQ(f.r.gw.offsetFetches, fetches0 + 1)
        << "RewindResubscribe: the duty pass sent no fetch (0) or stormed (>1) for the held partition";
    EXPECT_EQ(f.done.count(u), 0u) << "the seek completed before its SubscribeResponse";
    // The transaction completes and the subscribe answer arrives; duty time moves.
    f.r.gw.offsetFetchErrorsNext = 0;
    f.r.gw.releaseSubscribeResponses();
    f.r.opt.stepNs = 100'000;
    ASSERT_TRUE(driveUntilDone(f.r, f.done, u, 40'000));
    EXPECT_EQ(f.done[u].c.status, rc::OpStatus::Ok);
    EXPECT_EQ(f.done[u].times, 1);
    EXPECT_EQ(f.done[t].times, 1);
    ASSERT_TRUE(f.r.drive([&] {
        const auto* s = f.r.gw.latestSubscription(kTopic, 1);
        return s != nullptr && s->fromOffset == 55;
    }, 40'000)) << "p1 never started at its committed offset";
    EXPECT_EQ(f.r.gw.latestSubscription(kTopic, 0)->fromOffset, 7) << "the seek on p0 was overridden";
    EXPECT_LE(f.r.gw.offsetFetches, fetches0 + 4) << "paced: 100 ms doubling, then the answer";
}

// #684 x concurrency (supersede with the fetch already in flight): seekToCommitted's OffsetFetch for p0 + p1 is out
// (held) when seek(p0, 7) supersedes it.  The superseding seek must not duplicate the in-flight fetch, and when the
// superseded seekToCommitted's answer (30 / 55) finally lands, p1 takes 55 but p0 KEEPS the seek (7).  Each
// completion fires exactly once.
TEST(GroupSeekRegression, ASupersededSeekToCommittedsLateAnswerNeverMovesTheSeekAdversarialRegression) {
    Fixture f(5731, 5732, {{kTopic, 0}, {kTopic, 1}}, Fixture::cfgOf(false, 30000, 5000),
              {{{kTopic, 0}, 30}, {{kTopic, 1}, 55}}, manualClockOptions());
    freeze(f.r);
    f.r.gw.holdOffsetFetch = true;
    const int fetches0 = f.r.gw.offsetFetches;
    const rc::Token t = f.tok(), u = f.tok();
    ASSERT_EQ(f.gc.seekToCommitted(rc::CompletionTarget(f.r.cq), t), rc::OpResult::Accepted);
    ASSERT_TRUE(f.r.drive([&] { return f.r.gw.offsetFetches > fetches0; }, 2'000)) << "seekToCommitted sent no fetch";
    ASSERT_EQ(f.r.gw.heldOffsetFetches.size(), 1u);
    ASSERT_EQ(f.gc.seek({kTopic, 0}, 7, rc::CompletionTarget(f.r.cq), u), rc::OpResult::Accepted);
    (void)passesUntil(f.r, f.done, 5'000);   // frozen clock
    ASSERT_EQ(f.done[t].c.status, rc::OpStatus::Superseded);
    EXPECT_EQ(f.r.gw.offsetFetches, fetches0 + 1) << "the superseding seek duplicated the in-flight fetch";
    ASSERT_TRUE(driveUntilDone(f.r, f.done, u, 5'000)) << "the seek never completed";
    EXPECT_EQ(f.done[u].c.status, rc::OpStatus::Ok);
    ASSERT_NE(f.r.gw.latestSubscription(kTopic, 0), nullptr);
    EXPECT_EQ(f.r.gw.latestSubscription(kTopic, 0)->fromOffset, 7);
    // The superseded fetch's answer lands.
    f.r.gw.holdOffsetFetch = false;
    f.r.gw.releaseOffsetFetches();
    (void)passesUntil(f.r, f.done, 5'000);
    f.r.opt.stepNs = 100'000;
    const int64_t until = g_clockNs + 1'000'000'000LL;   // 1 s of duty time: any sweep runs
    ASSERT_TRUE(f.r.drive([&] { collect(f.r, f.done); return g_clockNs >= until; }, 20'000));
    ASSERT_NE(f.r.gw.latestSubscription(kTopic, 1), nullptr);
    EXPECT_EQ(f.r.gw.latestSubscription(kTopic, 1)->fromOffset, 55);
    const auto s0 = *f.r.gw.latestSubscription(kTopic, 0);
    EXPECT_EQ(s0.fromOffset, 7) << "the superseded answer moved the seek";
    // A moved position (30) without a resubscribe drops the records 7..29 the seek asked for.
    f.r.gw.pushRecords(s0, 7, 5);   // 7..11
    ASSERT_TRUE(f.consumed(0, 5)) << "the superseded answer moved p0's position: the seek's records were dropped";
    EXPECT_EQ(f.seen.offsets[0].front(), 7);
    EXPECT_EQ(f.done[t].times, 1);
    EXPECT_EQ(f.done[u].times, 1);
}
