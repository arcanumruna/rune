// CatalogLeadershipTest.cpp — catalog history Task 11: the catalog leadership
// and availability rules on THREE brokers' write paths (CatalogTrio: real
// __catalog replicas, appliers and AdminWorkers over one in-memory network).
//
//   spec §5.2 a new leader refuses DDL (retriable NotLeader) until every record
//            of its log is committed (HW == LEO) and applied (applier at the HW);
//            an in-flight DDL on a demoted leader is answered NotLeader; a retry
//            with the same requestId gets the ORIGINAL result once it committed,
//            and is executed ONCE when the new leader truncated it — never a
//            phantom success;
//   spec §5.5 below minIsr every DDL is NotLeader; produce on an existing topic
//            is unaffected (F24);
//   spec §9   a halted broker answers Internal and stays Halted whatever its role.
//
// Every DDL in every state is answered exactly once (answersSentFor == 1).
#include "support/CatalogTrio.h"

#include <rune/node/CatalogHaltPolicy.h>

#include <gtest/gtest.h>
#include <spdlog/sinks/ringbuffer_sink.h>
#include <spdlog/spdlog.h>

#include <chrono>
#include <string>
#include <vector>

using namespace std::chrono_literals;
using namespace rune::node;
using namespace rune::node::test;

namespace {
constexpr uint16_t kOk        = 0;
constexpr uint16_t kNotLeader = 2;
constexpr uint16_t kInternal  = 6;
} // namespace

// Review focus (Task 11): "a new catalog leader refuses DDL until it applied up to
// its HW".  Broker 2 holds every committed event but applied only the first five
// (its applier is paused before the last three commit).  RED (gate on the replica
// role alone): 2 serves at once and validates `x` against a stale catalog.
TEST(CatalogLeadershipRegression, NewLeaderAnswersNotLeaderUntilCaughtUpRegression) {
    CatalogTrio t("ld_catchup");                       // 3 brokers, leader 1
    t.commitEvents(1, 5);                              // 5 DDLs committed via broker 1
    t.isolate(3);                                      // 3 stops fetching but keeps its log
    t.holdApply(2);                                    // test seam: 2's applier is paused
    t.commitEvents(1, 3);                              // 3 more: 2 holds them (committed), 3 misses them
    t.deisolate(3);
    ASSERT_EQ(t.replica(2).logEndOffset(), t.replica(1).logEndOffset()) << "2 has everything";
    t.moveCatalogLeadership(/*to=*/2);                 // 2 is in the ISR and has everything
    EXPECT_EQ(t.admin(2).roleName(), "LeaderCatchingUp");
    const auto refused = t.define(2, {.name = "x", .partitions = 1});
    EXPECT_EQ(refused.code, kNotLeader) << "never validates against a stale catalog";
    EXPECT_EQ(t.admin(2).roleName(), "LeaderCatchingUp");
    EXPECT_EQ(t.admin(2).eventsAppended(), 0u) << "nothing appended while catching up";
    t.releaseApply(2);
    ASSERT_TRUE(t.awaitRole(2, "LeaderServing", 10s));
    EXPECT_EQ(t.admin(2).applier().nextOffset(), t.replica(2).highWatermark());
    EXPECT_EQ(t.define(2, {.name = "x", .partitions = 1}).code, kOk);
    EXPECT_EQ(t.define(2, {.name = "ev0", .partitions = 1}).code, 25) << "validated against the WHOLE catalog";
    EXPECT_TRUE(t.allDigestsEqual()) << "3 caught up as a follower of 2";
}

// The other half of the gate: a new leader whose log holds a previous epoch's
// UNCOMMITTED tail (HW < LEO) refuses DDL until that tail commits — even with
// everything it committed applied.  RED (gate on "applied up to the HW" alone):
// 2 builds `other` on a state that lacks the tail it will commit first.
TEST(CatalogLeadershipRegression, NewLeaderWithAnUncommittedTailAnswersNotLeaderUntilItCommitsRegression) {
    CatalogTrio t("ld_tail");
    const rune::protocol::RequestId rid{.hi = 0xC11E, .lo = 7};
    t.holdHighWatermark(1);
    const int64_t corr = t.submitDefineNoWait(1, {.name = "t", .partitions = 1, .requestId = rid});
    t.replicateTo({2});                                // only 2 receives the event
    t.moveCatalogLeadership(2);
    t.holdHighWatermark(2);                            // nobody's ack reaches 2: the tail stays uncommitted
    ASSERT_EQ(t.answerFor(1, corr).value_or(CatalogTrio::Answer{.code = 99}).code, kNotLeader);
    for (int i = 0; i < 50; ++i) t.pump();
    EXPECT_LT(t.replica(2).highWatermark(), t.replica(2).logEndOffset());
    EXPECT_EQ(t.admin(2).applier().nextOffset(), t.replica(2).highWatermark()) << "all it committed is applied";
    EXPECT_EQ(t.admin(2).roleName(), "LeaderCatchingUp");
    EXPECT_EQ(t.define(2, {.name = "other", .partitions = 1}).code, kNotLeader);
    t.releaseHighWatermark();
    ASSERT_TRUE(t.awaitRole(2, "LeaderServing", 10s));
    const auto retry = t.define(2, {.name = "t", .partitions = 1, .requestId = rid});
    EXPECT_EQ(retry.code, kOk) << "the committed tail's original result";
    EXPECT_EQ(t.countEventsWithRequestId(rid), 1u);
    EXPECT_TRUE(t.allDigestsEqual());
}

// Review focus #1: a DDL whose leader loses leadership after the append must be
// neither lost nor applied twice; the retrying client gets the original success,
// not TopicExists.
TEST(CatalogLeadershipRegression, RetryAfterLeaderChangeReturnsTheOriginalResultOnceRegression) {
    CatalogTrio t("ld_retry");
    const rune::protocol::RequestId rid{.hi = 0xC11E, .lo = 5};
    t.holdHighWatermark(1);                            // 1 appends, the HW does not move yet
    const int64_t corr = t.submitDefineNoWait(1, {.name = "t", .partitions = 1, .typed = true, .requestId = rid});
    ASSERT_FALSE(t.answerFor(1, corr).has_value()) << "awaiting its commit";
    t.replicateTo({2, 3});                             // the event reaches 2 and 3 …
    t.moveCatalogLeadership(2);                        // … then 1 loses leadership before answering
    ASSERT_TRUE(t.answerFor(1, corr).has_value());
    EXPECT_EQ(t.answerFor(1, corr)->code, kNotLeader);
    t.releaseHighWatermark();
    ASSERT_TRUE(t.awaitRole(2, "LeaderServing", 10s));
    const auto retry = t.define(2, {.name = "t", .partitions = 1, .typed = true, .requestId = rid});
    EXPECT_EQ(retry.code, kOk);
    EXPECT_EQ(t.countEventsWithRequestId(rid), 1u) << "no DDL lost or duplicated";
    EXPECT_TRUE(t.allDigestsEqual());
    const auto log = t.recordedLog(2);
    ASSERT_FALSE(log.empty());
    EXPECT_EQ(retry.topicId, t.admin(2).applier().state().topicByName(rune::catalog::kDefaultNsId, "t")->topicId) << "the original topic id";
    EXPECT_EQ(t.answersSentFor(1, corr), 1u) << "answered exactly once";
    for (int32_t b = 1; b <= 3; ++b) EXPECT_EQ(t.countEventsWithRequestId(rid, b), 1u) << "broker " << b;
}

// Binding (Task 10 → 11): the NEW leader never received the old leader's
// uncommitted event and truncates it away.  The client got the explicit,
// retriable NotLeader; its dedupe retry is EXECUTED once on the new leader —
// never a phantom success (an OK for an event no log keeps).  RED (answering an
// in-flight event OK on demotion, or deduping against the old leader's local
// tail): an OK for an event that is truncated.
TEST(CatalogLeadershipRegression, NewLeaderTruncatesTheOldLeadersUncommittedEventAndTheRetryRunsOnceRegression) {
    CatalogTrio t("ld_trunc");
    const rune::protocol::RequestId rid{.hi = 0xC11E, .lo = 9};
    const int64_t before = t.replica(1).logEndOffset();
    t.holdHighWatermark(1);
    const int64_t corr = t.submitDefineNoWait(1, {.name = "t", .partitions = 1, .requestId = rid});
    ASSERT_EQ(t.replica(1).logEndOffset(), before + 1) << "1 appended the event";
    ASSERT_EQ(t.countEventsWithRequestId(rid, 1), 1u);
    t.moveCatalogLeadership(2);                        // 2 never saw it (no replicateTo)
    ASSERT_TRUE(t.answerFor(1, corr).has_value());
    EXPECT_EQ(t.answerFor(1, corr)->code, kNotLeader) << "explicit and retriable — never OK, never a hang";
    t.releaseHighWatermark();
    ASSERT_TRUE(t.awaitRole(2, "LeaderServing", 10s));
    ASSERT_TRUE(t.awaitConverged());
    EXPECT_EQ(t.countEventsWithRequestId(rid, 1), 0u) << "the new leader's history truncated 1's tail";
    EXPECT_EQ(t.replica(1).logEndOffset(), before);
    EXPECT_EQ(t.admin(1).applier().state().topicByName(rune::catalog::kDefaultNsId, "t"), nullptr) << "never applied anywhere";

    // A retry that lands on the old leader (now a follower) is retriable, never OK.
    const auto onOld = t.define(1, {.name = "t", .partitions = 1, .requestId = rid});
    EXPECT_EQ(onOld.code, kNotLeader);
    // The retry on the new leader runs the DDL once.
    const auto retry = t.define(2, {.name = "t", .partitions = 1, .requestId = rid});
    EXPECT_EQ(retry.code, kOk);
    ASSERT_TRUE(t.allDigestsEqual());
    for (int32_t b = 1; b <= 3; ++b) {
        EXPECT_EQ(t.countEventsWithRequestId(rid, b), 1u) << "broker " << b;
        const auto* topic = t.admin(b).applier().state().topicByName(rune::catalog::kDefaultNsId, "t");
        ASSERT_NE(topic, nullptr) << "broker " << b;
        EXPECT_EQ(topic->topicId, retry.topicId);
    }
    // And again: now it dedupes to that result.
    const auto again = t.define(2, {.name = "t", .partitions = 1, .requestId = rid});
    EXPECT_EQ(again.code, kOk);
    EXPECT_EQ(again.topicId, retry.topicId);
    EXPECT_EQ(t.countEventsWithRequestId(rid), 1u);
    EXPECT_EQ(t.answersSentFor(1, corr), 1u);
}

// Spec §5.5: below minIsr the catalog accepts no change (retriable NotLeader, at
// once — never a wait-out), every started DDL is answered; produce and consume on
// an existing topic really happen on the survivor (F24).
TEST(CatalogLeadershipRegression, BelowMinIsrEveryDdlIsNotLeaderAndProduceIsUnaffected) {
    CatalogTrio t("ld_minisr");
    ASSERT_EQ(t.define(1, {.name = "data", .partitions = 1}).code, kOk);
    t.kill(2); t.kill(3);
    EXPECT_EQ(t.define(1, {.name = "more", .partitions = 1}).code, kNotLeader);
    const rune::protocol::RequestId rid{.hi = 0xC11E, .lo = 11};
    EXPECT_EQ(t.define(1, {.name = "more2", .partitions = 1, .requestId = rid}).code, kNotLeader);
    EXPECT_EQ(t.countEventsWithRequestId(rid, 1), 0u) << "nothing appended below minIsr";
    EXPECT_EQ(t.admin(1).startedCount(), t.admin(1).answeredCount());
    EXPECT_FALSE(t.admin(1).hasInFlight());
    // F24: produce and consume on the existing topic really happen, below the catalog's minIsr.
    const auto off = t.produce(1, "data", {0xAB, 0xCD});              // acks=leader on the survivor
    ASSERT_TRUE(off.has_value()) << "produce on an existing topic is unaffected (spec §5.5)";
    EXPECT_EQ(t.consumeFrom(1, "data", *off), (std::vector<uint8_t>{0xAB, 0xCD}));
}

// Spec §9 / review focus #5: a broker whose applier halts refuses DDL with
// Internal and stays Halted through every later role (it never serves again in
// this process); the other brokers keep working and take the leadership.  RED
// (halt checked only while leading): the halted broker serves DDL once it is
// re-promoted.
TEST(CatalogLeadershipRegression, HaltedLeaderAnswersInternalAndStaysHaltedThroughEveryRoleRegression) {
    CatalogTrio t("ld_halt");
    t.admin(1).applierForTest().tamperNextFingerprintForTest();
    const auto inFlight = t.define(1, {.name = "a", .partitions = 1});
    EXPECT_EQ(inFlight.code, kInternal) << "halted at its own event";
    ASSERT_TRUE(t.awaitRole(1, "Halted", 1s));
    EXPECT_EQ(t.define(1, {.name = "b", .partitions = 1}).code, kInternal);
    const auto next = t.dropTopic(1, "a");             // a DdlResponse carries the detail
    EXPECT_EQ(next.code, kInternal);
    EXPECT_EQ(next.detail, "catalog apply halted at offset 0");
    // The followers applied `a` (the leader's fingerprint was right — only 1's apply was tampered).
    ASSERT_TRUE(t.awaitRole(2, "Follower", 1s));
    t.moveCatalogLeadership(2);
    ASSERT_TRUE(t.awaitRole(2, "LeaderServing", 10s));
    EXPECT_NE(t.admin(2).applier().state().topicByName(rune::catalog::kDefaultNsId, "a"), nullptr);
    EXPECT_EQ(t.define(2, {.name = "c", .partitions = 1}).code, kOk) << "every other broker keeps working";
    EXPECT_EQ(t.admin(1).roleName(), "Halted") << "a follower role does not clear the halt";
    t.moveCatalogLeadership(1);
    for (int i = 0; i < 50; ++i) t.pump();
    EXPECT_EQ(t.admin(1).roleName(), "Halted") << "nor does a re-promotion";
    EXPECT_EQ(t.define(1, {.name = "d", .partitions = 1}).code, kInternal);
    EXPECT_EQ(t.admin(1).startedCount(), t.admin(1).answeredCount());
}

// The whole class: every DDL — in flight, queued — is answered exactly once
// across a leader change: the in-flight one NotLeader at the demotion, the queued
// ones NotLeader on the follower; the retry of each on the new leader succeeds
// once.  RED: a queued DDL answered twice, or never.
TEST(CatalogLeadershipRegression, InFlightAndQueuedDdlsAreEachAnsweredOnceAcrossALeaderChangeRegression) {
    CatalogTrio t("ld_queue");
    t.holdHighWatermark(1);
    std::vector<int64_t> corrs;
    std::vector<rune::protocol::RequestId> rids;
    for (uint64_t i = 0; i < 3; ++i) {
        rids.push_back({0xC11E, 100 + i});
        corrs.push_back(t.submitDefineNoWait(1, {.name = "q" + std::to_string(i), .partitions = 1,
                                                  .requestId = rids.back()}));
    }
    t.replicateTo({2, 3});
    t.moveCatalogLeadership(2);
    t.releaseHighWatermark();
    for (int i = 0; i < 100; ++i) t.pump();
    for (int64_t c : corrs) {
        ASSERT_TRUE(t.answerFor(1, c).has_value()) << c;
        EXPECT_EQ(t.answerFor(1, c)->code, kNotLeader) << c;
        EXPECT_EQ(t.answersSentFor(1, c), 1u) << c;
    }
    ASSERT_TRUE(t.awaitRole(2, "LeaderServing", 10s));
    for (uint64_t i = 0; i < 3; ++i) {
        EXPECT_EQ(t.define(2, {.name = "q" + std::to_string(i), .partitions = 1, .requestId = rids[i]}).code, kOk);
        EXPECT_EQ(t.countEventsWithRequestId(rids[i]), 1u) << i;
    }
    EXPECT_EQ(t.admin(1).startedCount(), t.admin(1).answeredCount());
    EXPECT_TRUE(t.allDigestsEqual());
}

// Task 24 finds the catalog leader by this line: every role transition logs
// "AdminWorker: __catalog role -> <roleName()> (epoch N)" at info.
namespace {
/// RAII (review M7): the capture logger never leaks into later tests, even when
/// an ASSERT returns early.
struct LogCapture {
    std::shared_ptr<spdlog::sinks::ringbuffer_sink_mt> sink = std::make_shared<spdlog::sinks::ringbuffer_sink_mt>(512);
    std::shared_ptr<spdlog::logger> previous = spdlog::default_logger();
    LogCapture() {
        auto capture = std::make_shared<spdlog::logger>("trio-capture", sink);
        capture->set_level(spdlog::level::info);
        spdlog::set_default_logger(capture);
    }
    ~LogCapture() { spdlog::set_default_logger(previous); }
    LogCapture(const LogCapture&) = delete;
    LogCapture& operator=(const LogCapture&) = delete;
};
} // namespace

TEST(CatalogLeadershipRegression, EveryRoleTransitionIsLoggedWithItsEpoch) {
    LogCapture log;
    const auto& sink = log.sink;
    {
        CatalogTrio t("ld_log");
        t.moveCatalogLeadership(2);
        ASSERT_TRUE(t.awaitRole(2, "LeaderServing", 10s));
    }
    auto seen = [&sink](const std::string& text) {
        for (const std::string& l : sink->last_formatted())
            if (l.find("[info]") != std::string::npos && l.find(text) != std::string::npos) return true;
        return false;
    };
    EXPECT_TRUE(seen("AdminWorker: __catalog role -> LeaderCatchingUp (epoch 1)"));
    EXPECT_TRUE(seen("AdminWorker: __catalog role -> LeaderServing (epoch 1)"));
    EXPECT_TRUE(seen("AdminWorker: __catalog role -> Follower (epoch 2)"));
    EXPECT_TRUE(seen("AdminWorker: __catalog role -> LeaderServing (epoch 2)"));
}

// Review M1: a halted controller that leads AGAIN (its stand-down produced no
// lasting healthy leader) doubles its candidacy hold, capped — repeats are never
// immediate and never unbounded.
TEST(CatalogLeadershipRegression, RepeatedHaltedStandDownsBackOffAndAreCapped) {
    constexpr int64_t base = 100;
    EXPECT_EQ(rune::node::haltedStandDownHoldNs(base, 0), 100);
    EXPECT_EQ(rune::node::haltedStandDownHoldNs(base, 1), 200);
    EXPECT_EQ(rune::node::haltedStandDownHoldNs(base, 2), 400);
    EXPECT_EQ(rune::node::haltedStandDownHoldNs(base, rune::node::kHaltedStandDownMaxShift), base << 5);
    EXPECT_EQ(rune::node::haltedStandDownHoldNs(base, 1000), base << 5) << "capped";
}

// Review M4: a broker that halts AT its own in-flight event answers Internal —
// and says the event may have committed (it did, on every other broker) and how
// to learn its result.  RED: a bare "catalog apply halted" an operator reads as
// "failed".
TEST(CatalogLeadershipRegression, HaltAtItsOwnEventSaysTheEventMayHaveCommittedRegression) {
    CatalogTrio t("ld_halt_own");
    ASSERT_EQ(t.define(1, {.name = "a", .partitions = 1}).code, kOk);
    t.admin(1).applierForTest().tamperNextFingerprintForTest();
    const auto a = t.dropTopic(1, "a");                 // a DdlResponse: it carries the detail
    EXPECT_EQ(a.code, kInternal);
    EXPECT_NE(a.detail.find("may have committed"), std::string::npos) << a.detail;
    EXPECT_NE(a.detail.find("same requestId"), std::string::npos) << a.detail;
    for (int i = 0; i < 50; ++i) t.pump();
    EXPECT_EQ(t.admin(2).applier().state().topicByName(rune::catalog::kDefaultNsId, "a"), nullptr) << "it did commit: applied elsewhere";
}
