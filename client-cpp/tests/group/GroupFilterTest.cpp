// tests/group/GroupFilterTest.cpp — Phase 2c Task 12: GroupConfig::filter on the hub (no Aeron, the
// GroupConsumerHubTest style): the filter rides JoinGroup and every routed fetch Subscribe, a mismatch ends the
// consumer, filtered groups follow position notes, and old brokers / non-routed hubs never get a silent
// unfiltered fetch.
#include <rune/client/group/GroupConsumer.h>
#include <rune/client/group/GroupConsumerHub.h>

#include <rune/protocol/client/GroupMessages.h>
#include <rune/protocol/client/OffsetMessages.h>
#include <rune/protocol/client/Protocol.h>

#include <gtest/gtest.h>

#include <chrono>
#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <vector>

using namespace rune::client;
using namespace rune::client::group;
namespace rpc = rune::protocol::client;

namespace {
constexpr int32_t kTopicId   = 42;
constexpr int64_t kSessionId = 99;

// Arbitrary filter bytes: the hub never interprets them, it only carries them.
rune::codec::FilterBytes someFilter() {
    rune::codec::FilterBytes f;
    for (uint16_t i = 0; i < 9; ++i) f.bytes[i] = static_cast<uint8_t>(0xA0 + i);
    f.len = 9;
    return f;
}

std::vector<uint8_t> joinOk(int32_t gen, const std::string& member) {
    rpc::JoinGroupResponse m;
    m.sessionId = kSessionId; m.generationId = gen; m.errorCode = 0;
    m.memberId = {reinterpret_cast<const uint8_t*>(member.data()), member.size()};
    std::vector<uint8_t> b(m.encodedSize());
    m.encode(b.data());
    return b;
}

std::vector<uint8_t> joinErr(int16_t code, const std::string& detail) {
    rpc::JoinGroupResponse m;
    m.sessionId = kSessionId; m.errorCode = code; m.detail = detail;
    std::vector<uint8_t> b(m.encodedSizeWithDetail());
    m.encodeFor(b.data(), rpc::kClientVersion, b.size());
    return b;
}

std::vector<uint8_t> syncOk(const std::vector<rpc::TopicPartition32>& asgn) {
    rpc::SyncGroupResponse m;
    m.sessionId = kSessionId; m.errorCode = 0;
    std::vector<uint8_t> b(rpc::SyncGroupResponse::encodedSizeFor(asgn.size()));
    m.encode(b.data(), asgn);
    return b;
}

int64_t g_lastOffsetFetchCorr = 0;

std::vector<uint8_t> offsetFetchResponse(int32_t partition, int64_t offset) {
    rpc::OffsetFetchResult r;
    r.topicId = kTopicId; r.partition = partition; r.offset = offset; r.errorCode = 0;
    rpc::OffsetFetchResponse m;
    m.sessionId = kSessionId; m.correlationId = g_lastOffsetFetchCorr;
    std::vector<uint8_t> b(rpc::OffsetFetchResponse::encodedSizeFor(1));
    m.encode(b.data(), std::span<const rpc::OffsetFetchResult>(&r, 1));
    return b;
}

struct Sink {
    std::vector<std::vector<uint8_t>> frames;
    SendFn fn() {
        return [this](int32_t, const uint8_t* b, std::size_t n) -> bool {
            if (auto of = rpc::OffsetFetch::decode(b, n)) g_lastOffsetFetchCorr = of->correlationId;
            frames.emplace_back(b, b + n);
            return true;
        };
    }
    [[nodiscard]] std::vector<std::vector<uint8_t>> ofTemplate(rpc::TemplateId t) const {
        std::vector<std::vector<uint8_t>> v;
        for (const auto& f : frames)
            if (f.size() >= 4 && static_cast<uint16_t>(f[2] | (f[3] << 8)) == static_cast<uint16_t>(t)) v.push_back(f);
        return v;
    }
};

// A fake ClientAgent: every partition led by session `leaderOf` (default 0 = the seed).
struct FakeRouter {
    std::map<uint32_t, int64_t> sids{{0u, kSessionId}};
    std::map<int32_t, uint32_t> leader;
    std::map<uint32_t, int32_t> versions;   // brokerVersion per session (absent = current)
    struct Offer { uint32_t session; std::vector<uint8_t> frame; };
    std::vector<Offer> offers;
    static uint32_t leaderSession(void* c, int32_t, int32_t p) {
        auto* s = static_cast<FakeRouter*>(c);
        const auto it = s->leader.find(p);
        return it == s->leader.end() ? 0u : it->second;
    }
    static int64_t sessionId(void* c, uint32_t i) {
        auto* s = static_cast<FakeRouter*>(c);
        const auto it = s->sids.find(i);
        return it == s->sids.end() ? 0 : it->second;
    }
    static uint64_t epoch(void* c, uint32_t i) { return sessionId(c, i) == 0 ? 0 : 1; }
    static int32_t brokerVersion(void* c, uint32_t i) {
        auto* s = static_cast<FakeRouter*>(c);
        const auto it = s->versions.find(i);
        return it == s->versions.end() ? rpc::kBrokerVersion : it->second;
    }
    static detail::SendResult offer(void* c, uint32_t i, const uint8_t* b, std::size_t n) {
        static_cast<FakeRouter*>(c)->offers.push_back({i, std::vector<uint8_t>(b, b + n)});
        return detail::SendResult::Accepted;
    }
    GroupConsumerHub::FetchRouter router() {
        GroupConsumerHub::FetchRouter r;
        r.ctx = this; r.leaderSession = &leaderSession; r.sessionId = &sessionId; r.epoch = &epoch;
        r.offer = &offer; r.brokerVersion = &brokerVersion;
        return r;
    }
    [[nodiscard]] std::vector<codec::SubscribeView> subscribes(uint32_t session) const {
        std::vector<codec::SubscribeView> v;
        for (const auto& o : offers) {
            if (o.session != session) continue;
            if (auto s = codec::decodeSubscribe(o.frame.data(), o.frame.size())) v.push_back(*s);
        }
        return v;
    }
};

class GroupFilterHubTest : public ::testing::Test {
protected:
    void make(bool filtered) {
        hub_  = std::make_unique<GroupConsumerHub>();
        impl_ = std::make_unique<GroupConsumerImpl>();
        impl_->groupId  = "g";
        impl_->topicIds = {kTopicId};
        impl_->cfg.autoCommit = false;
        if (filtered) impl_->cfg.filter(someFilter());
        impl_->active = true;
        impl_->state  = GroupConsumerState::Unjoined;
        hub_->add(impl_.get());
    }
    void toStable() {   // one partition (0) of kTopicId
        hub_->onConnected(kSessionId, sink_.fn());
        auto j = joinOk(1, "m1");
        hub_->onJoinGroupResponse(j.data(), j.size(), kSessionId, sink_.fn());
        auto s = syncOk({{kTopicId, 0}});
        hub_->onSyncGroupResponse(s.data(), s.size(), kSessionId, sink_.fn());
    }
    void seed(int64_t offset) {
        auto r = offsetFetchResponse(0, offset);
        hub_->onOffsetFetchResponse(r.data(), r.size());
    }
    codec::RecordsView note(int64_t base, int64_t corr, int64_t subId = 700) const {
        codec::RecordsView rv;
        rv.sessionId = kSessionId; rv.correlationId = corr; rv.subscriptionId = subId;
        rv.topicId = kTopicId; rv.partition = 0; rv.baseOffset = base;
        rv.recordBatch = {};
        return rv;
    }
    std::unique_ptr<GroupConsumerHub> hub_;
    std::unique_ptr<GroupConsumerImpl> impl_;
    Sink sink_;
    FakeRouter fr_;
};
} // namespace

TEST_F(GroupFilterHubTest, JoinGroupCarriesTheFilter) {
    make(true);
    hub_->setFetchRouter(fr_.router());
    hub_->onConnected(kSessionId, sink_.fn());
    const auto joins = sink_.ofTemplate(rpc::TemplateId::JoinGroup);
    ASSERT_EQ(joins.size(), 1u);
    const auto jg = rpc::JoinGroup::decode(joins[0].data(), joins[0].size(), rpc::kClientVersion);
    ASSERT_TRUE(jg.has_value());
    const auto want = someFilter();
    EXPECT_EQ(std::vector<uint8_t>(jg->filter.begin(), jg->filter.end()),
              std::vector<uint8_t>(want.span().begin(), want.span().end()));
}

TEST_F(GroupFilterHubTest, UnfilteredJoinGroupIsByteIdenticalRegression) {
    make(false);
    hub_->onConnected(kSessionId, sink_.fn());
    const auto joins = sink_.ofTemplate(rpc::TemplateId::JoinGroup);
    ASSERT_EQ(joins.size(), 1u);
    rpc::JoinGroup m;
    m.sessionId = kSessionId; m.correlationId = 0; m.sessionTimeoutMs = impl_->cfg.sessionTimeoutMs;
    const std::string g = "g";
    m.groupId = {reinterpret_cast<const uint8_t*>(g.data()), g.size()};
    const std::vector<int32_t> topics{kTopicId};
    std::vector<uint8_t> want(m.encodedSize(topics.size()));
    ASSERT_EQ(m.encode(want.data(), std::span<const int32_t>(topics)), want.size());
    EXPECT_EQ(joins[0], want) << "no filter trailer";
}

TEST_F(GroupFilterHubTest, FetchSubscribesCarryTheFilter) {
    make(true);
    hub_->setFetchRouter(fr_.router());
    toStable();
    seed(10);
    const auto subs = fr_.subscribes(0);
    ASSERT_EQ(subs.size(), 1u);
    const auto want = someFilter();
    EXPECT_EQ(std::vector<uint8_t>(subs[0].filter.begin(), subs[0].filter.end()),
              std::vector<uint8_t>(want.span().begin(), want.span().end()));
    EXPECT_EQ(subs[0].fromOffset, 10);
    EXPECT_EQ(subs[0].partitionKnown, 1);
}

TEST_F(GroupFilterHubTest, MismatchEndsTheConsumerWithFilterMismatchRegression) {
    make(true);
    hub_->setFetchRouter(fr_.router());
    hub_->onConnected(kSessionId, sink_.fn());
    auto e = joinErr(static_cast<int16_t>(rpc::ErrorCode::InvalidRequest),
                     std::string(rpc::kGroupFilterMismatchPrefix) + ": the group uses filter 00ff");
    hub_->onJoinGroupResponse(e.data(), e.size(), kSessionId, sink_.fn());
    EXPECT_FALSE(impl_->active);
    EXPECT_EQ(impl_->failReason, GroupFailReason::FilterMismatch);
    // No rejoin storm: pass the join deadline and keep ticking.
    for (int i = 0; i < 20; ++i) hub_->doWork(static_cast<int64_t>(i) * 60'000'000'000LL, kSessionId, sink_.fn());
    EXPECT_EQ(sink_.ofTemplate(rpc::TemplateId::JoinGroup).size(), 1u);
}

TEST_F(GroupFilterHubTest, OtherInvalidRequestStaysRefusedByBrokerRegression) {
    make(true);
    hub_->setFetchRouter(fr_.router());
    hub_->onConnected(kSessionId, sink_.fn());
    auto e = joinErr(static_cast<int16_t>(rpc::ErrorCode::InvalidRequest), "filter too large");
    hub_->onJoinGroupResponse(e.data(), e.size(), kSessionId, sink_.fn());
    EXPECT_FALSE(impl_->active);
    EXPECT_EQ(impl_->failReason, GroupFailReason::RefusedByBroker);
}

TEST_F(GroupFilterHubTest, PositionNoteMovesAFilteredGroupsPositionRegression) {
    make(true);
    hub_->setFetchRouter(fr_.router());
    toStable();
    seed(10);
    const TopicPartition tp{kTopicId, 0};
    const int64_t corr = fr_.subscribes(0).at(0).correlationId;
    hub_->onPositionNote(note(5000, corr));
    EXPECT_EQ(impl_->assignment.at(tp), 5000);
    hub_->onPositionNote(note(4000, corr));   // older: never backwards
    EXPECT_EQ(impl_->assignment.at(tp), 5000);
    hub_->onPositionNote(note(9000, corr + 1));   // another instance's: ignored
    EXPECT_EQ(impl_->assignment.at(tp), 5000);
    sink_.frames.clear();
    impl_->commitRequested = true;
    hub_->doWork(1, kSessionId, sink_.fn());
    ASSERT_EQ(impl_->commitEntriesScratch.size(), 1u);
    EXPECT_EQ(impl_->commitEntriesScratch[0].offset, 5000);
}

TEST_F(GroupFilterHubTest, PositionNoteIsIgnoredByAnUnfilteredGroupRegression) {
    make(false);
    hub_->setFetchRouter(fr_.router());
    toStable();
    seed(10);
    const int64_t corr = fr_.subscribes(0).at(0).correlationId;
    hub_->onPositionNote(note(5000, corr));
    EXPECT_EQ(impl_->assignment.at(TopicPartition{kTopicId, 0}), 10);
}

TEST_F(GroupFilterHubTest, OldSeedRefusesTheFilteredGroupRegression) {
    make(true);
    hub_->setFetchRouter(fr_.router());
    hub_->setBrokerVersion(rpc::kBrokerVersionFilter - 1);
    hub_->onConnected(kSessionId, sink_.fn());
    EXPECT_FALSE(impl_->active);
    EXPECT_EQ(impl_->failReason, GroupFailReason::BrokerTooOld);
    EXPECT_TRUE(sink_.ofTemplate(rpc::TemplateId::JoinGroup).empty());
}

TEST_F(GroupFilterHubTest, UnfilteredGroupStillRunsOnAnOldSeedRegression) {
    make(false);
    hub_->setFetchRouter(fr_.router());
    hub_->setBrokerVersion(rpc::kBrokerVersionFilter - 1);
    hub_->onConnected(kSessionId, sink_.fn());
    EXPECT_TRUE(impl_->active);
    EXPECT_EQ(sink_.ofTemplate(rpc::TemplateId::JoinGroup).size(), 1u);
}

TEST_F(GroupFilterHubTest, OldPartitionLeaderIsSkippedNeverFetchedUnfilteredRegression) {
    make(true);
    fr_.leader[0]  = 1;
    fr_.sids[1]    = 501;
    fr_.versions[1] = rpc::kBrokerVersionFilter - 1;
    hub_->setFetchRouter(fr_.router());
    toStable();
    seed(10);
    EXPECT_TRUE(fr_.subscribes(1).empty()) << "a node below the filter version is never sent a Subscribe";
    EXPECT_TRUE(fr_.subscribes(0).empty());
    // The node reconnects upgraded: reconcile now subscribes it, with the filter.
    fr_.versions[1] = rpc::kBrokerVersionFilter;
    hub_->reconcileFetch();
    const auto subs = fr_.subscribes(1);
    ASSERT_EQ(subs.size(), 1u);
    EXPECT_FALSE(subs[0].filter.empty());
}

TEST_F(GroupFilterHubTest, FilteredGroupOnANonRoutedHubFailsNeverDeliversUnfilteredRegression) {
    make(true);   // no router: the legacy path would reach the broker's carrier Subscribe (which refuses filters)
    hub_->onConnected(kSessionId, sink_.fn());
    EXPECT_FALSE(impl_->active);
    EXPECT_EQ(impl_->failReason, GroupFailReason::FilterMismatch);
    EXPECT_TRUE(sink_.frames.empty()) << "nothing is sent for it: no JoinGroup, no Subscribe";
}

TEST_F(GroupFilterHubTest, UnfilteredGroupOnANonRoutedHubStillRunsRegression) {
    make(false);
    hub_->onConnected(kSessionId, sink_.fn());
    EXPECT_TRUE(impl_->active);
    EXPECT_EQ(sink_.ofTemplate(rpc::TemplateId::JoinGroup).size(), 1u);
}
