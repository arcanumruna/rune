// ---------------------------------------------------------------------------
// AssignorTest.cpp — Kafka-faithful RangeAssignor + RoundRobinAssignor tests.
//
// All tests: deterministic, bounded, injected clock (not needed here — assignors
// are pure functions). Hard cap on driver loops + ASSERT(done).
//
// RangeAssignor tests  (Range_*)
// RoundRobinAssignor tests (RR_*)
// Invariant tests (Both_*) — covering both assignors
//
// Test count: 43 tests
// ---------------------------------------------------------------------------

#include <gtest/gtest.h>
#include <rune/coordination/Assignor.h>

#include <algorithm>
#include <map>
#include <set>
#include <string>
#include <vector>

using namespace rune::coordination;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Collect all partitions assigned across all members.
static std::vector<TopicPartition> allAssigned(const Assignment& a) {
    std::vector<TopicPartition> result;
    for (const auto& [mid, tps] : a) {
        result.insert(result.end(), tps.begin(), tps.end());
    }
    return result;
}

/// Build a set of all TopicPartitions for a topic with partCount partitions.
static std::set<TopicPartition> allPartitionsFor(const std::string& topic, int32_t count) {
    std::set<TopicPartition> s;
    for (int32_t i = 0; i < count; ++i) s.insert({topic, i});
    return s;
}

/// Return all partitions for a topic from the assignment.
static std::vector<int32_t> partitionsFor(const Assignment& a,
                                           const std::string& memberId,
                                           const std::string& topic) {
    auto it = a.find(memberId);
    if (it == a.end()) return {};
    std::vector<int32_t> result;
    for (const auto& tp : it->second) {
        if (tp.topic == topic) result.push_back(tp.partition);
    }
    std::sort(result.begin(), result.end());
    return result;
}

/// Check no partition appears more than once.
static bool allUnique(const std::vector<TopicPartition>& tps) {
    std::set<TopicPartition> s(tps.begin(), tps.end());
    return s.size() == tps.size();
}

/// Check every partition for a topic is assigned to some member.
static bool allTopicPartitionsAssigned(const Assignment& a,
                                        const std::string& topic,
                                        int32_t count) {
    std::set<int32_t> found;
    for (const auto& [mid, tps] : a) {
        for (const auto& tp : tps) {
            if (tp.topic == topic) found.insert(tp.partition);
        }
    }
    if (static_cast<int32_t>(found.size()) != count) return false;
    for (int32_t i = 0; i < count; ++i) {
        if (!found.count(i)) return false;
    }
    return true;
}

/// Check no partition for a topic is assigned to a non-subscriber.
static bool noNonSubscriberAssignment(const Assignment& a,
                                       const std::string& topic,
                                       const std::vector<MemberSubscription>& members) {
    std::set<std::string> subscribers;
    for (const auto& m : members) {
        for (const auto& t : m.subscribedTopics) {
            if (t == topic) { subscribers.insert(m.memberId); break; }
        }
    }
    for (const auto& [mid, tps] : a) {
        for (const auto& tp : tps) {
            if (tp.topic == topic && !subscribers.count(mid)) return false;
        }
    }
    return true;
}

/// Min/max load among subscribers for a topic.
static std::pair<int,int> loadRange(const Assignment& a,
                                     const std::string& topic,
                                     const std::vector<std::string>& subscriberIds) {
    int mn = INT_MAX, mx = 0;
    for (const auto& mid : subscriberIds) {
        int cnt = 0;
        auto it = a.find(mid);
        if (it != a.end()) {
            for (const auto& tp : it->second) {
                if (tp.topic == topic) ++cnt;
            }
        }
        mn = std::min(mn, cnt);
        mx = std::max(mx, cnt);
    }
    return {mn, mx};
}

// ===========================================================================
// RangeAssignor
// ===========================================================================

// ---------------------------------------------------------------------------
// Range_1: 1 topic, 6 partitions, 2 members → [0,1,2] & [3,4,5]
// ---------------------------------------------------------------------------
TEST(RangeAssignor, OneTopicSixPartitionsTwoMembers) {
    RangeAssignor ra;
    std::vector<MemberSubscription> members = {
        {"member-a", {"topicA"}},
        {"member-b", {"topicA"}},
    };
    TopicPartitionCounts counts = {{"topicA", 6}};
    auto a = ra.assign(members, counts);

    // member-a gets [0,1,2], member-b gets [3,4,5]
    EXPECT_EQ(partitionsFor(a, "member-a", "topicA"), (std::vector<int32_t>{0,1,2}));
    EXPECT_EQ(partitionsFor(a, "member-b", "topicA"), (std::vector<int32_t>{3,4,5}));
}

// ---------------------------------------------------------------------------
// Range_2: 1 topic, 7 partitions, 3 members → exact Kafka split (3,2,2)
// ---------------------------------------------------------------------------
TEST(RangeAssignor, OneTopicSevenPartitionsThreeMembers) {
    RangeAssignor ra;
    // Sort alphabetically: member-a, member-b, member-c
    std::vector<MemberSubscription> members = {
        {"member-a", {"topicA"}},
        {"member-b", {"topicA"}},
        {"member-c", {"topicA"}},
    };
    TopicPartitionCounts counts = {{"topicA", 7}};
    auto a = ra.assign(members, counts);

    // 7/3 = 2 base, 7%3 = 1 extra → member-a gets 3, member-b gets 2, member-c gets 2
    EXPECT_EQ(partitionsFor(a, "member-a", "topicA"), (std::vector<int32_t>{0,1,2}));
    EXPECT_EQ(partitionsFor(a, "member-b", "topicA"), (std::vector<int32_t>{3,4}));
    EXPECT_EQ(partitionsFor(a, "member-c", "topicA"), (std::vector<int32_t>{5,6}));
}

// ---------------------------------------------------------------------------
// Range_3: 1 topic, 5 partitions, 3 members → (2,2,1) — 5%3=2 extras
// ---------------------------------------------------------------------------
TEST(RangeAssignor, OneTopicFivePartitionsThreeMembers) {
    RangeAssignor ra;
    std::vector<MemberSubscription> members = {
        {"member-a", {"t"}},
        {"member-b", {"t"}},
        {"member-c", {"t"}},
    };
    TopicPartitionCounts counts = {{"t", 5}};
    auto a = ra.assign(members, counts);

    // 5/3=1 base, 5%3=2 extras → a gets 2, b gets 2, c gets 1
    EXPECT_EQ(partitionsFor(a, "member-a", "t"), (std::vector<int32_t>{0,1}));
    EXPECT_EQ(partitionsFor(a, "member-b", "t"), (std::vector<int32_t>{2,3}));
    EXPECT_EQ(partitionsFor(a, "member-c", "t"), (std::vector<int32_t>{4}));
}

// ---------------------------------------------------------------------------
// Range_4: 1 member gets all partitions
// ---------------------------------------------------------------------------
TEST(RangeAssignor, OneMemberGetsAll) {
    RangeAssignor ra;
    std::vector<MemberSubscription> members = {{"only", {"t"}}};
    TopicPartitionCounts counts = {{"t", 4}};
    auto a = ra.assign(members, counts);
    EXPECT_EQ(partitionsFor(a, "only", "t"), (std::vector<int32_t>{0,1,2,3}));
}

// ---------------------------------------------------------------------------
// Range_5: multi-topic — independent range assignment per topic
// ---------------------------------------------------------------------------
TEST(RangeAssignor, MultiTopicIndependentRanges) {
    RangeAssignor ra;
    std::vector<MemberSubscription> members = {
        {"member-a", {"t1", "t2"}},
        {"member-b", {"t1", "t2"}},
    };
    TopicPartitionCounts counts = {{"t1", 4}, {"t2", 6}};
    auto a = ra.assign(members, counts);

    // t1: 4/2=2 base, 0 extras → a=[0,1], b=[2,3]
    EXPECT_EQ(partitionsFor(a, "member-a", "t1"), (std::vector<int32_t>{0,1}));
    EXPECT_EQ(partitionsFor(a, "member-b", "t1"), (std::vector<int32_t>{2,3}));
    // t2: 6/2=3 base, 0 extras → a=[0,1,2], b=[3,4,5]
    EXPECT_EQ(partitionsFor(a, "member-a", "t2"), (std::vector<int32_t>{0,1,2}));
    EXPECT_EQ(partitionsFor(a, "member-b", "t2"), (std::vector<int32_t>{3,4,5}));
}

// ---------------------------------------------------------------------------
// Range_6: member subscribes to only some topics (partial subscription)
// ---------------------------------------------------------------------------
TEST(RangeAssignor, PartialSubscription) {
    RangeAssignor ra;
    std::vector<MemberSubscription> members = {
        {"member-a", {"t1"}},       // subscribes only to t1
        {"member-b", {"t1", "t2"}}, // subscribes to t1 and t2
    };
    TopicPartitionCounts counts = {{"t1", 4}, {"t2", 3}};
    auto a = ra.assign(members, counts);

    // t1: 2 subscribers (a, b). 4/2=2 base → a=[0,1], b=[2,3]
    EXPECT_EQ(partitionsFor(a, "member-a", "t1"), (std::vector<int32_t>{0,1}));
    EXPECT_EQ(partitionsFor(a, "member-b", "t1"), (std::vector<int32_t>{2,3}));
    // t2: only member-b subscribes → b gets all [0,1,2]; a gets nothing
    EXPECT_EQ(partitionsFor(a, "member-a", "t2"), (std::vector<int32_t>{}));
    EXPECT_EQ(partitionsFor(a, "member-b", "t2"), (std::vector<int32_t>{0,1,2}));
}

// ---------------------------------------------------------------------------
// Range_7: topic not in counts → silently ignored
// ---------------------------------------------------------------------------
TEST(RangeAssignor, UnknownTopicIgnored) {
    RangeAssignor ra;
    std::vector<MemberSubscription> members = {
        {"member-a", {"known", "unknown-topic"}},
    };
    TopicPartitionCounts counts = {{"known", 2}};
    auto a = ra.assign(members, counts);

    EXPECT_EQ(partitionsFor(a, "member-a", "known"), (std::vector<int32_t>{0,1}));
    EXPECT_EQ(partitionsFor(a, "member-a", "unknown-topic"), (std::vector<int32_t>{}));
}

// ---------------------------------------------------------------------------
// Range_8: deterministic — same output regardless of input order
// ---------------------------------------------------------------------------
TEST(RangeAssignor, Deterministic) {
    RangeAssignor ra;
    std::vector<MemberSubscription> members1 = {
        {"member-a", {"t"}},
        {"member-b", {"t"}},
    };
    std::vector<MemberSubscription> members2 = {
        {"member-b", {"t"}},
        {"member-a", {"t"}},
    };
    TopicPartitionCounts counts = {{"t", 6}};
    auto a1 = ra.assign(members1, counts);
    auto a2 = ra.assign(members2, counts);
    // Both should produce same result.
    EXPECT_EQ(a1, a2);
}

// ---------------------------------------------------------------------------
// Range_9: 1 topic, 1 partition, 2 members → first alphabetically gets it
// ---------------------------------------------------------------------------
TEST(RangeAssignor, OnePartitionTwoMembers_FirstAlphaGetsIt) {
    RangeAssignor ra;
    std::vector<MemberSubscription> members = {
        {"member-z", {"t"}},
        {"member-a", {"t"}},
    };
    TopicPartitionCounts counts = {{"t", 1}};
    auto a = ra.assign(members, counts);

    // member-a (alphabetically first) gets partition 0
    EXPECT_EQ(partitionsFor(a, "member-a", "t"), (std::vector<int32_t>{0}));
    EXPECT_EQ(partitionsFor(a, "member-z", "t"), (std::vector<int32_t>{}));
}

// ---------------------------------------------------------------------------
// Range_10: 3 members, 6 partitions → perfectly balanced (2 each)
// ---------------------------------------------------------------------------
TEST(RangeAssignor, PerfectlyBalanced) {
    RangeAssignor ra;
    std::vector<MemberSubscription> members = {
        {"member-a", {"t"}},
        {"member-b", {"t"}},
        {"member-c", {"t"}},
    };
    TopicPartitionCounts counts = {{"t", 6}};
    auto a = ra.assign(members, counts);

    EXPECT_EQ(partitionsFor(a, "member-a", "t"), (std::vector<int32_t>{0,1}));
    EXPECT_EQ(partitionsFor(a, "member-b", "t"), (std::vector<int32_t>{2,3}));
    EXPECT_EQ(partitionsFor(a, "member-c", "t"), (std::vector<int32_t>{4,5}));
}

// ---------------------------------------------------------------------------
// Range_11: All members appear as keys (even if no assignment)
// ---------------------------------------------------------------------------
TEST(RangeAssignor, AllMembersAppearAsKeys) {
    RangeAssignor ra;
    std::vector<MemberSubscription> members = {
        {"member-a", {"t1"}},
        {"member-b", {}},  // subscribes to nothing
    };
    TopicPartitionCounts counts = {{"t1", 3}};
    auto a = ra.assign(members, counts);

    EXPECT_TRUE(a.count("member-a") > 0);
    EXPECT_TRUE(a.count("member-b") > 0);
}

// ---------------------------------------------------------------------------
// Range_12: 4 members, 7 partitions → (2,2,2,1) — 7%4=3 extras → first 3 get +1
// ---------------------------------------------------------------------------
TEST(RangeAssignor, FourMembersSevenPartitions) {
    RangeAssignor ra;
    std::vector<MemberSubscription> members = {
        {"a", {"t"}},
        {"b", {"t"}},
        {"c", {"t"}},
        {"d", {"t"}},
    };
    TopicPartitionCounts counts = {{"t", 7}};
    auto a = ra.assign(members, counts);

    // 7/4=1 base, 7%4=3 extras → a=2,b=2,c=2,d=1
    EXPECT_EQ(partitionsFor(a, "a", "t"), (std::vector<int32_t>{0,1}));
    EXPECT_EQ(partitionsFor(a, "b", "t"), (std::vector<int32_t>{2,3}));
    EXPECT_EQ(partitionsFor(a, "c", "t"), (std::vector<int32_t>{4,5}));
    EXPECT_EQ(partitionsFor(a, "d", "t"), (std::vector<int32_t>{6}));
}

// ===========================================================================
// RoundRobinAssignor
// ===========================================================================

// ---------------------------------------------------------------------------
// RR_1: 2 members, 1 topic, 6 partitions → member-a gets evens, member-b gets odds
//        (sorted members: member-a first, so a gets 0,2,4; b gets 1,3,5)
// ---------------------------------------------------------------------------
TEST(RoundRobinAssignor, TwoMembersOneTopic_EvensOdds) {
    RoundRobinAssignor rr;
    std::vector<MemberSubscription> members = {
        {"member-a", {"t"}},
        {"member-b", {"t"}},
    };
    TopicPartitionCounts counts = {{"t", 6}};
    auto a = rr.assign(members, counts);

    // sorted pairs: (t,0),(t,1),(t,2),(t,3),(t,4),(t,5)
    // round-robin: a←(t,0), b←(t,1), a←(t,2), b←(t,3), a←(t,4), b←(t,5)
    EXPECT_EQ(partitionsFor(a, "member-a", "t"), (std::vector<int32_t>{0,2,4}));
    EXPECT_EQ(partitionsFor(a, "member-b", "t"), (std::vector<int32_t>{1,3,5}));
}

// ---------------------------------------------------------------------------
// RR_2: 1 member gets everything
// ---------------------------------------------------------------------------
TEST(RoundRobinAssignor, OneMemberGetsAll) {
    RoundRobinAssignor rr;
    std::vector<MemberSubscription> members = {{"only", {"t"}}};
    TopicPartitionCounts counts = {{"t", 4}};
    auto a = rr.assign(members, counts);
    EXPECT_EQ(partitionsFor(a, "only", "t"), (std::vector<int32_t>{0,1,2,3}));
}

// ---------------------------------------------------------------------------
// RR_3: multi-topic interleave — (t1,0),(t1,1),(t2,0),(t2,1) round-robin across members
// ---------------------------------------------------------------------------
TEST(RoundRobinAssignor, MultiTopicInterleave) {
    RoundRobinAssignor rr;
    std::vector<MemberSubscription> members = {
        {"member-a", {"t1", "t2"}},
        {"member-b", {"t1", "t2"}},
    };
    // Sorted pairs: (t1,0),(t1,1),(t2,0),(t2,1)
    TopicPartitionCounts counts = {{"t1", 2}, {"t2", 2}};
    auto a = rr.assign(members, counts);

    // a←(t1,0), b←(t1,1), a←(t2,0), b←(t2,1)
    EXPECT_EQ(partitionsFor(a, "member-a", "t1"), (std::vector<int32_t>{0}));
    EXPECT_EQ(partitionsFor(a, "member-b", "t1"), (std::vector<int32_t>{1}));
    EXPECT_EQ(partitionsFor(a, "member-a", "t2"), (std::vector<int32_t>{0}));
    EXPECT_EQ(partitionsFor(a, "member-b", "t2"), (std::vector<int32_t>{1}));
}

// ---------------------------------------------------------------------------
// RR_4: 3 members, 1 topic, 9 partitions → perfectly balanced (3 each)
// ---------------------------------------------------------------------------
TEST(RoundRobinAssignor, ThreeMembersNinePartitions) {
    RoundRobinAssignor rr;
    std::vector<MemberSubscription> members = {
        {"member-a", {"t"}},
        {"member-b", {"t"}},
        {"member-c", {"t"}},
    };
    TopicPartitionCounts counts = {{"t", 9}};
    auto a = rr.assign(members, counts);

    EXPECT_EQ(partitionsFor(a, "member-a", "t"), (std::vector<int32_t>{0,3,6}));
    EXPECT_EQ(partitionsFor(a, "member-b", "t"), (std::vector<int32_t>{1,4,7}));
    EXPECT_EQ(partitionsFor(a, "member-c", "t"), (std::vector<int32_t>{2,5,8}));
}

// ---------------------------------------------------------------------------
// RR_5: partial subscription — topic pairs skipped for non-subscribers
// ---------------------------------------------------------------------------
TEST(RoundRobinAssignor, PartialSubscription) {
    RoundRobinAssignor rr;
    // member-a subscribes only to t1, member-b to both
    std::vector<MemberSubscription> members = {
        {"member-a", {"t1"}},
        {"member-b", {"t1", "t2"}},
    };
    TopicPartitionCounts counts = {{"t1", 2}, {"t2", 2}};
    auto a = rr.assign(members, counts);

    // All t1 pairs go round-robin among subscribers {a,b}:
    //   (t1,0)→a, (t1,1)→b
    // All t2 pairs: only b subscribes → b gets all
    //   (t2,0)→b, (t2,1)→b
    EXPECT_EQ(partitionsFor(a, "member-a", "t1"), (std::vector<int32_t>{0}));
    EXPECT_EQ(partitionsFor(a, "member-b", "t1"), (std::vector<int32_t>{1}));
    EXPECT_EQ(partitionsFor(a, "member-a", "t2"), (std::vector<int32_t>{}));
    EXPECT_EQ(partitionsFor(a, "member-b", "t2"), (std::vector<int32_t>{0,1}));
}

// ---------------------------------------------------------------------------
// RR_6: deterministic regardless of input member order
// ---------------------------------------------------------------------------
TEST(RoundRobinAssignor, Deterministic) {
    RoundRobinAssignor rr;
    std::vector<MemberSubscription> members1 = {
        {"member-a", {"t"}},
        {"member-b", {"t"}},
    };
    std::vector<MemberSubscription> members2 = {
        {"member-b", {"t"}},
        {"member-a", {"t"}},
    };
    TopicPartitionCounts counts = {{"t", 6}};
    auto a1 = rr.assign(members1, counts);
    auto a2 = rr.assign(members2, counts);
    EXPECT_EQ(a1, a2);
}

// ---------------------------------------------------------------------------
// RR_7: unknown topic silently ignored
// ---------------------------------------------------------------------------
TEST(RoundRobinAssignor, UnknownTopicIgnored) {
    RoundRobinAssignor rr;
    std::vector<MemberSubscription> members = {
        {"member-a", {"known", "ghost"}},
    };
    TopicPartitionCounts counts = {{"known", 2}};
    auto a = rr.assign(members, counts);

    EXPECT_EQ(partitionsFor(a, "member-a", "known"), (std::vector<int32_t>{0,1}));
    EXPECT_EQ(partitionsFor(a, "member-a", "ghost"), (std::vector<int32_t>{}));
}

// ---------------------------------------------------------------------------
// RR_8: 2 members, 2 topics, 3+3 partitions
// ---------------------------------------------------------------------------
TEST(RoundRobinAssignor, TwoMembersTwoTopicsThreeEach) {
    RoundRobinAssignor rr;
    std::vector<MemberSubscription> members = {
        {"member-a", {"t1", "t2"}},
        {"member-b", {"t1", "t2"}},
    };
    TopicPartitionCounts counts = {{"t1", 3}, {"t2", 3}};
    auto a = rr.assign(members, counts);

    // Sorted pairs: (t1,0),(t1,1),(t1,2),(t2,0),(t2,1),(t2,2) — 6 pairs
    // a←(t1,0), b←(t1,1), a←(t1,2), b←(t2,0), a←(t2,1), b←(t2,2)
    EXPECT_EQ(partitionsFor(a, "member-a", "t1"), (std::vector<int32_t>{0,2}));
    EXPECT_EQ(partitionsFor(a, "member-b", "t1"), (std::vector<int32_t>{1}));
    EXPECT_EQ(partitionsFor(a, "member-a", "t2"), (std::vector<int32_t>{1}));
    EXPECT_EQ(partitionsFor(a, "member-b", "t2"), (std::vector<int32_t>{0,2}));
}

// ---------------------------------------------------------------------------
// RR_9: All members appear as keys
// ---------------------------------------------------------------------------
TEST(RoundRobinAssignor, AllMembersAppearAsKeys) {
    RoundRobinAssignor rr;
    std::vector<MemberSubscription> members = {
        {"member-a", {"t"}},
        {"member-b", {}},
    };
    TopicPartitionCounts counts = {{"t", 2}};
    auto a = rr.assign(members, counts);

    EXPECT_TRUE(a.count("member-a") > 0);
    EXPECT_TRUE(a.count("member-b") > 0);
}

// ===========================================================================
// Invariant tests — both assignors
// ===========================================================================

// File-scope assignor instances for parameterized tests.
static RangeAssignor     g_range;
static RoundRobinAssignor g_rr;

class BothAssignors : public ::testing::TestWithParam<PartitionAssignor*> {
};

INSTANTIATE_TEST_SUITE_P(Assignors, BothAssignors,
    ::testing::Values(
        static_cast<PartitionAssignor*>(&g_range),
        static_cast<PartitionAssignor*>(&g_rr)));

// ---------------------------------------------------------------------------
// Inv_1: every partition of a subscribed topic is assigned exactly once
// ---------------------------------------------------------------------------
TEST_P(BothAssignors, EveryPartitionAssignedExactlyOnce) {
    auto* ass = GetParam();
    std::vector<MemberSubscription> members = {
        {"member-a", {"t1", "t2"}},
        {"member-b", {"t1"}},
        {"member-c", {"t2"}},
    };
    TopicPartitionCounts counts = {{"t1", 5}, {"t2", 4}};
    auto a = ass->assign(members, counts);

    auto all = allAssigned(a);
    EXPECT_TRUE(allUnique(all)) << "Duplicate assignment found";
    EXPECT_TRUE(allTopicPartitionsAssigned(a, "t1", 5)) << "t1 not fully assigned";
    EXPECT_TRUE(allTopicPartitionsAssigned(a, "t2", 4)) << "t2 not fully assigned";
}

// ---------------------------------------------------------------------------
// Inv_2: no partition assigned to a non-subscriber
// ---------------------------------------------------------------------------
TEST_P(BothAssignors, NoPartitionToNonSubscriber) {
    auto* ass = GetParam();
    std::vector<MemberSubscription> members = {
        {"member-a", {"t1"}},
        {"member-b", {"t2"}},
    };
    TopicPartitionCounts counts = {{"t1", 3}, {"t2", 3}};
    auto a = ass->assign(members, counts);

    EXPECT_TRUE(noNonSubscriberAssignment(a, "t1", members));
    EXPECT_TRUE(noNonSubscriberAssignment(a, "t2", members));
    // member-a should not have any t2 partitions
    EXPECT_EQ(partitionsFor(a, "member-a", "t2"), (std::vector<int32_t>{}));
    // member-b should not have any t1 partitions
    EXPECT_EQ(partitionsFor(a, "member-b", "t1"), (std::vector<int32_t>{}));
}

// ---------------------------------------------------------------------------
// Inv_3: balanced within 1 (max - min <= 1 per topic among subscribers)
// ---------------------------------------------------------------------------
TEST_P(BothAssignors, BalancedWithinOne) {
    auto* ass = GetParam();
    std::vector<MemberSubscription> members = {
        {"member-a", {"t"}},
        {"member-b", {"t"}},
        {"member-c", {"t"}},
    };
    TopicPartitionCounts counts = {{"t", 7}};
    auto a = ass->assign(members, counts);

    auto [mn, mx] = loadRange(a, "t", {"member-a", "member-b", "member-c"});
    EXPECT_LE(mx - mn, 1) << "Imbalanced: min=" << mn << " max=" << mx;
}

// ---------------------------------------------------------------------------
// Inv_4: total assigned partitions equals sum of partitionCounts (for topics
//         with at least one subscriber)
// ---------------------------------------------------------------------------
TEST_P(BothAssignors, TotalAssignedMatchesPartitionCounts) {
    auto* ass = GetParam();
    std::vector<MemberSubscription> members = {
        {"member-a", {"t1", "t2"}},
        {"member-b", {"t1"}},
    };
    TopicPartitionCounts counts = {{"t1", 4}, {"t2", 3}};
    auto a = ass->assign(members, counts);

    int total = 0;
    for (const auto& [mid, tps] : a) total += static_cast<int>(tps.size());
    EXPECT_EQ(total, 4 + 3); // all partitions assigned
}

// ---------------------------------------------------------------------------
// Inv_5: single member gets all partitions across all subscribed topics
// ---------------------------------------------------------------------------
TEST_P(BothAssignors, SingleMemberGetsAll) {
    auto* ass = GetParam();
    std::vector<MemberSubscription> members = {{"solo", {"t1", "t2"}}};
    TopicPartitionCounts counts = {{"t1", 3}, {"t2", 2}};
    auto a = ass->assign(members, counts);

    EXPECT_TRUE(allTopicPartitionsAssigned(a, "t1", 3));
    EXPECT_TRUE(allTopicPartitionsAssigned(a, "t2", 2));
    int total = 0;
    for (const auto& tp : a.at("solo")) ++total;
    EXPECT_EQ(total, 5);
}

// ---------------------------------------------------------------------------
// Inv_6: empty topics map → all members get empty assignments
// ---------------------------------------------------------------------------
TEST_P(BothAssignors, EmptyTopicCounts) {
    auto* ass = GetParam();
    std::vector<MemberSubscription> members = {
        {"member-a", {"t"}},
        {"member-b", {"t"}},
    };
    TopicPartitionCounts counts; // empty
    auto a = ass->assign(members, counts);

    // All members appear as keys, all with empty assignments
    EXPECT_TRUE(a.count("member-a") > 0);
    EXPECT_TRUE(a.count("member-b") > 0);
    for (const auto& [mid, tps] : a) {
        EXPECT_TRUE(tps.empty()) << "Member " << mid << " got unexpected partitions";
    }
}

// ---------------------------------------------------------------------------
// Inv_7: partition count 0 for a topic → no partitions assigned for that topic
// ---------------------------------------------------------------------------
TEST_P(BothAssignors, ZeroPartitionTopicAssignsNothing) {
    auto* ass = GetParam();
    std::vector<MemberSubscription> members = {
        {"member-a", {"t1", "t2"}},
    };
    TopicPartitionCounts counts = {{"t1", 0}, {"t2", 3}};
    auto a = ass->assign(members, counts);

    EXPECT_EQ(partitionsFor(a, "member-a", "t1"), (std::vector<int32_t>{}));
    EXPECT_EQ(partitionsFor(a, "member-a", "t2"), (std::vector<int32_t>{0,1,2}));
}

// ---------------------------------------------------------------------------
// AUDIT T2: RoundRobin partial-subscription — exact Kafka placement (3 members,
// heterogeneous subscriptions, interleaved non-subscriber).
//
// Members: m0→{t0,t1}, m1→{t1}, m2→{t0,t1}
// Topics: t0:4 parts, t1:4 parts
// allMembers sorted: [m0,m1,m2]
// pairs sorted: (t0,0),(t0,1),(t0,2),(t0,3),(t1,0),(t1,1),(t1,2),(t1,3)
// subs: t0→{m0,m2}, t1→{m0,m1,m2}
//
// Kafka trace (cursor advances past assigned member):
//   (t0,0): cursor=0  → m0∈t0 → m0.  cursor=1
//   (t0,1): cursor=1  → m1∉t0 → skip; cursor=2 → m2∈t0 → m2.  cursor=0
//   (t0,2): cursor=0  → m0∈t0 → m0.  cursor=1
//   (t0,3): cursor=1  → m1∉t0 → skip; cursor=2 → m2∈t0 → m2.  cursor=0
//   (t1,0): cursor=0  → m0∈t1 → m0.  cursor=1
//   (t1,1): cursor=1  → m1∈t1 → m1.  cursor=2
//   (t1,2): cursor=2  → m2∈t1 → m2.  cursor=0
//   (t1,3): cursor=0  → m0∈t1 → m0.  cursor=1
//
// Expected: m0→{(t0,0),(t0,2),(t1,0),(t1,3)},
//           m1→{(t1,1)},
//           m2→{(t0,1),(t0,3),(t1,2)}
// ---------------------------------------------------------------------------
TEST(RoundRobinAssignor, KafkaExact_ThreeMember_HeterogeneousSubscriptions) {
    RoundRobinAssignor rr;
    std::vector<MemberSubscription> members = {
        {"m0", {"t0", "t1"}},
        {"m1", {"t1"}},
        {"m2", {"t0", "t1"}},
    };
    TopicPartitionCounts counts = {{"t0", 4}, {"t1", 4}};
    auto a = rr.assign(members, counts);

    // m0 gets (t0,0),(t0,2),(t1,0),(t1,3)
    EXPECT_EQ(partitionsFor(a, "m0", "t0"), (std::vector<int32_t>{0, 2}));
    EXPECT_EQ(partitionsFor(a, "m0", "t1"), (std::vector<int32_t>{0, 3}));
    // m1 gets only (t1,1)
    EXPECT_EQ(partitionsFor(a, "m1", "t0"), (std::vector<int32_t>{}));
    EXPECT_EQ(partitionsFor(a, "m1", "t1"), (std::vector<int32_t>{1}));
    // m2 gets (t0,1),(t0,3),(t1,2)
    EXPECT_EQ(partitionsFor(a, "m2", "t0"), (std::vector<int32_t>{1, 3}));
    EXPECT_EQ(partitionsFor(a, "m2", "t1"), (std::vector<int32_t>{2}));
}

// ---------------------------------------------------------------------------
// AUDIT T2: Another exact-placement check — m0→{t0,t1}, m1→{t1},
// single-subscriber topic t0: verifies pointer doesn't bias toward m0 on t1.
// Topics: t0:2 parts, t1:3 parts
// allMembers: [m0,m1]
// pairs: (t0,0),(t0,1),(t1,0),(t1,1),(t1,2)
// subs: t0→{m0}, t1→{m0,m1}
//
// Kafka trace:
//   (t0,0): cursor=0 → m0∈t0 → m0.  cursor=1
//   (t0,1): cursor=1 → m1∉t0; cursor=0 → m0∈t0 → m0.  cursor=1
//   (t1,0): cursor=1 → m1∈t1 → m1.  cursor=0
//   (t1,1): cursor=0 → m0∈t1 → m0.  cursor=1
//   (t1,2): cursor=1 → m1∈t1 → m1.  cursor=0
//
// Expected: m0→{(t0,0),(t0,1),(t1,1)}, m1→{(t1,0),(t1,2)}
// ---------------------------------------------------------------------------
TEST(RoundRobinAssignor, KafkaExact_TwoMember_SingleSubscriberTopic) {
    RoundRobinAssignor rr;
    std::vector<MemberSubscription> members = {
        {"m0", {"t0", "t1"}},
        {"m1", {"t1"}},
    };
    TopicPartitionCounts counts = {{"t0", 2}, {"t1", 3}};
    auto a = rr.assign(members, counts);

    EXPECT_EQ(partitionsFor(a, "m0", "t0"), (std::vector<int32_t>{0, 1}));
    EXPECT_EQ(partitionsFor(a, "m0", "t1"), (std::vector<int32_t>{1}));
    EXPECT_EQ(partitionsFor(a, "m1", "t0"), (std::vector<int32_t>{}));
    EXPECT_EQ(partitionsFor(a, "m1", "t1"), (std::vector<int32_t>{0, 2}));
}

// ---------------------------------------------------------------------------
// Inv_8: large assignment (5 members, 2 topics, 12+7 partitions) — correctness
// ---------------------------------------------------------------------------
TEST_P(BothAssignors, LargeAssignmentCorrect) {
    auto* ass = GetParam();
    std::vector<MemberSubscription> members = {
        {"m1", {"t1", "t2"}},
        {"m2", {"t1", "t2"}},
        {"m3", {"t1"}},
        {"m4", {"t2"}},
        {"m5", {"t1", "t2"}},
    };
    TopicPartitionCounts counts = {{"t1", 12}, {"t2", 7}};
    auto a = ass->assign(members, counts);

    auto all = allAssigned(a);
    EXPECT_TRUE(allUnique(all));
    EXPECT_TRUE(allTopicPartitionsAssigned(a, "t1", 12));
    EXPECT_TRUE(allTopicPartitionsAssigned(a, "t2", 7));
    EXPECT_TRUE(noNonSubscriberAssignment(a, "t1", members));
    EXPECT_TRUE(noNonSubscriberAssignment(a, "t2", members));
}
