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
    Fixture(int32_t in, int32_t eg, std::vector<std::pair<int32_t, int32_t>> assigned,
            rc::GroupConfig gcfg = cfgOf(false, 30000, 5000),
            std::map<std::pair<int32_t, int32_t>, int64_t> committed = {})
        : r(in, eg), gc(prepare(in, std::move(assigned), gcfg, std::move(committed))) {
        wire(gc, seen);
        r.connect();
        EXPECT_TRUE(r.drive([&] { return !r.gw.pendingJoins.empty(); }));
        r.gw.releaseJoins();
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

// True after the loop ran `ms` of wall time without `pred` ever holding (a bounded quiet wait).
template <class P>
bool staysFalseFor(GroupRig& r, P&& pred, std::chrono::milliseconds ms) {
    const auto start = std::chrono::steady_clock::now();
    bool fired = false;
    (void)r.drive([&] {
        if (pred()) { fired = true; return true; }
        return std::chrono::steady_clock::now() - start > ms;
    }, 80'000'000);
    return !fired;
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
    Fixture f(4307, 4308, {{kTopic, 1}}, Fixture::cfgOf(true, 30000, 40));
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
    Fixture f(4311, 4312, {{kTopic, 1}});
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
