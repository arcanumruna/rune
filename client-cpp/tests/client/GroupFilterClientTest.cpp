// tests/client/GroupFilterClientTest.cpp — Phase 2c Task 12 (fix round 1): the Client.cpp wiring of filtered
// consumer groups, end to end over the InvokerGateway: the filter on JoinGroup and the fetch Subscribe, the
// position note reaching the hub, and refusal details reaching onError.  Single-threaded; bounded pump loops.
#include "../support/GroupRig.h"

#include "../../examples/filter/PlayerEventSchema.h"
#include "player_event.gen.h"

#include <rune/client/Client.h>
#include <rune/client/group/GroupConsumer.h>
#include <rune/protocol/client/OffsetMessages.h>

#include <gtest/gtest.h>

#include <algorithm>
#include <string>
#include <vector>

namespace {
using E = rune::gen::PlayerEvent;

struct FilteredGroup : GroupRig {
    std::vector<std::string> errors;
    explicit FilteredGroup(int32_t in) : GroupRig(in, in + 1) {
        gw.groupAssignment = {{1, 0}};
        client.onError([this](const std::string& m) { errors.push_back(m); });
    }
    void passes(int n) { for (int i = 0; i < n; ++i) { client.doWork(); gw.pump(); } }
    [[nodiscard]] bool errorMentions(const std::string& needle) const {
        return std::any_of(errors.begin(), errors.end(),
                           [&](const std::string& e) { return e.find(needle) != std::string::npos; });
    }
    [[nodiscard]] std::vector<int64_t> committed(int32_t partition) const {
        std::vector<int64_t> v;
        for (const auto& f : gw.offsetCommits) {
            const auto oc = rune::protocol::client::OffsetCommit::decode(f.data(), f.size());
            if (!oc) continue;
            for (const auto& e : oc->entries)
                if (e.topicId == 1 && e.partition == partition) v.push_back(e.offset);
        }
        return v;
    }
};

rc::GroupConfig filteredCfg() {
    rc::GroupConfig c;
    c.filter(E::Filter{.worldId = 7u});
    return c;
}
std::vector<uint8_t> wantBytes() {
    const auto b = E::Filter{.worldId = 7u}.encode();
    return std::vector<uint8_t>(b.span().begin(), b.span().end());
}
} // namespace

TEST(GroupFilterClient, FilterRidesJoinAndFetchAndNotesMoveTheCommitRegression) {
    FilteredGroup r(4500);
    auto gc = r.client.subscribeGroup("g", {"t"}, filteredCfg());
    r.connect();
    ASSERT_TRUE(r.drive([&] { return !r.gw.pendingJoins.empty(); }));
    r.gw.releaseJoins();
    ASSERT_TRUE(r.drive([&] { return r.subAt(0, 0) != nullptr; }));
    ASSERT_FALSE(r.gw.joinFilters.empty());
    EXPECT_EQ(r.gw.joinFilters.back(), wantBytes()) << "JoinGroup carries the config's filter";
    const auto s0 = *r.subAt(0, 0);
    EXPECT_EQ(s0.filter, wantBytes()) << "the fetch Subscribe carries it too";
    // The Client routes an empty-batch Records frame (a position note) to the filtered group.
    r.gw.pushNote(s0, 5000);
    int tick = 0;
    ASSERT_TRUE(r.drive([&] {
        if (++tick % 50 == 1) gc.commit();   // a few commits, never a flood of commands
        const auto c = r.committed(0);
        return std::find(c.begin(), c.end(), 5000) != c.end();
    })) << "the note never moved the group's position";
}

TEST(GroupFilterClient, JoinMismatchDetailReachesOnErrorAndEndsTheGroupRegression) {
    FilteredGroup r(4502);
    r.gw.joinErrorCode   = static_cast<int16_t>(rune::protocol::client::ErrorCode::InvalidRequest);
    r.gw.joinErrorDetail = "group filter mismatch: the group uses filter 00ff";
    auto gc = r.client.subscribeGroup("g", {"t"}, filteredCfg());
    r.connect();
    ASSERT_TRUE(r.drive([&] { return !r.gw.pendingJoins.empty(); }));
    r.gw.releaseJoins();
    ASSERT_TRUE(r.drive([&] { return gc.failReason() == rc::GroupFailReason::FilterMismatch; }));
    EXPECT_TRUE(r.errorMentions("group filter mismatch")) << "the broker's detail reaches onError";
    r.passes(300);
    EXPECT_EQ(r.gw.joinGroups, 1) << "no rejoin storm";
}

TEST(GroupFilterClient, LeaderFilterRefusalSurfacesAndEndsTheGroupNoRetryLoopRegression) {
    FilteredGroup r(4504);
    r.gw.subscribeErrorCode   = static_cast<int16_t>(rune::protocol::client::ErrorCode::InvalidRequest);
    r.gw.subscribeErrorDetail = "filter needs a typed topic";
    auto gc = r.client.subscribeGroup("g", {"t"}, filteredCfg());
    r.connect();
    ASSERT_TRUE(r.drive([&] { return !r.gw.pendingJoins.empty(); }));
    r.gw.releaseJoins();
    ASSERT_TRUE(r.drive([&] { return gc.failReason() == rc::GroupFailReason::RefusedByBroker; }));
    EXPECT_TRUE(r.errorMentions("filter needs a typed topic")) << "the leader's detail reaches onError";
    const auto frames = r.gw.subscribeFrames.size();
    r.passes(400);   // metadata cycles would re-send a parked partition
    EXPECT_EQ(r.gw.subscribeFrames.size(), frames) << "the refused Subscribe is never retried";
    EXPECT_EQ(frames, 1u);
}
