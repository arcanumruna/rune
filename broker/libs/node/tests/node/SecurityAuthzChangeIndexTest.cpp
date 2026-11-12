// SecurityAuthzChangeIndexTest.cpp - issues #218 / #67: the per-principal staleness rule of the admin thread's fence.
// A request stamped at offset S is stale only if an event applied after S could have changed what ITS user may do:
// events about that user, and every role-side / wide event; an unknown or anonymous principal is stale on any change.
#include "support/SecurityRig.h"

#include <rune/node/SecurityAuthzChangeIndex.h>

#include <gtest/gtest.h>

#include <string>
#include <vector>

using rune::node::SecurityAuthzChangeIndex;
namespace sec = rune::security;

namespace {

sec::SecurityEvent event(sec::Ops ops, uint32_t nsId, const std::string& name) {
    sec::SecurityEvent e;
    e.kind     = sec::kindOf(ops);
    e.ops      = std::move(ops);
    e.nsId     = nsId;
    e.objectName = name;
    return e;
}
sec::GrantSpec topicRead() {
    return sec::GrantSpec{sec::Operation::Read, {sec::ResourceType::Topic, sec::PatternKind::Exact, "t"}};
}

struct Row {
    const char*        what;
    sec::SecurityEvent ev;
    bool               selfStale;    // the requester is the event's user ("bob" in namespace 0)
    bool               otherStale;   // another known user ("alice") - also stands for a role member / non-member
};

std::vector<Row> rows() {
    const std::string who = "bob";
    return {
        {"CreateUser self", event(sec::CreateUserOps{}, 0, who), true, false},
        {"AlterUser self", event(sec::AlterUserOps{sec::DisableUser{}}, 0, who), true, false},
        {"DropUser self", event(sec::DropUserOps{}, 0, who), true, false},
        {"GrantRole self", event(sec::GrantRoleOps{1, "r"}, 0, who), true, false},
        {"RevokeRole self", event(sec::RevokeRoleOps{1, "r"}, 0, who), true, false},
        {"Grant to the user", event(sec::GrantOps{sec::GranteeKind::User, topicRead()}, 0, who), true, false},
        {"Revoke from the user", event(sec::RevokeOps{sec::GranteeKind::User, topicRead()}, 0, who), true, false},
        {"Grant to a role", event(sec::GrantOps{sec::GranteeKind::Role, topicRead()}, 0, "r"), true, true},
        {"Revoke from a role", event(sec::RevokeOps{sec::GranteeKind::Role, topicRead()}, 0, "r"), true, true},
        {"CreateRole", event(sec::CreateRoleOps{}, 0, "r"), true, true},
        {"DropRole", event(sec::DropRoleOps{}, 0, "r"), true, true},
        {"Bootstrap", event(sec::BootstrapOps{}, 0, "admin"), true, true},
        {"NamespacePurged", event(sec::NamespacePurgedOps{}, 7, "arena"), true, true},
    };
}

constexpr int64_t kAt = 10;

SecurityAuthzChangeIndex primed() {
    SecurityAuthzChangeIndex idx;
    idx.note(event(sec::CreateUserOps{}, 0, "bob"), 1);
    idx.note(event(sec::CreateUserOps{}, 0, "alice"), 2);
    return idx;
}

}  // namespace

TEST(SecurityAuthzChangeIndexRegression, EveryEventKindStalesExactlyTheRequestsItCouldChangeRegression) {
    for (const Row& r : rows()) {
        SCOPED_TRACE(r.what);
        SecurityAuthzChangeIndex idx = primed();
        idx.note(r.ev, kAt);
        const bool dropsSelf = r.ev.kind == sec::EventKind::DropUser;
        // stamp = offset - 1: the event applied after the stamp.
        EXPECT_EQ(idx.changedSince(0, "bob", kAt - 1), r.selfStale) << "the requester itself";
        EXPECT_EQ(idx.changedSince(0, "alice", kAt - 1), r.otherStale) << "another user";
        EXPECT_TRUE(idx.changedSince(0, "zed", kAt - 1)) << "an unknown principal fails safe to the strict rule";
        EXPECT_TRUE(idx.changedSince(0, "", kAt - 1)) << "anonymous";
        // stamp = offset: nothing newer than the stamp, so only unknown / anonymous principals stay stale.
        EXPECT_EQ(idx.changedSince(0, "bob", kAt), dropsSelf) << "a dropped user is unknown";
        EXPECT_FALSE(idx.changedSince(0, "alice", kAt));
        EXPECT_TRUE(idx.changedSince(0, "zed", kAt));
        EXPECT_TRUE(idx.changedSince(0, "", kAt));
        // The in-flight classification gives the same answer for one event, without an index.
        EXPECT_EQ(SecurityAuthzChangeIndex::affects(r.ev, 0, "bob"), r.selfStale);
        EXPECT_EQ(SecurityAuthzChangeIndex::affects(r.ev, 0, "alice"), r.otherStale);
        EXPECT_TRUE(SecurityAuthzChangeIndex::affects(r.ev, 0, "")) << "anonymous";
    }
}

TEST(SecurityAuthzChangeIndexRegression, ADroppedUserThatIsCreatedAgainIsKnownAgainRegression) {
    SecurityAuthzChangeIndex idx = primed();
    idx.note(event(sec::DropUserOps{}, 0, "bob"), 10);
    EXPECT_TRUE(idx.changedSince(0, "bob", 10)) << "dropped: unknown, stale on any change";
    idx.note(event(sec::CreateUserOps{}, 0, "bob"), 11);
    EXPECT_TRUE(idx.changedSince(0, "bob", 10)) << "the re-creation applied after the stamp";
    EXPECT_FALSE(idx.changedSince(0, "bob", 11));
}

TEST(SecurityAuthzChangeIndexRegression, ANameInAnotherNamespaceIsAnotherUserRegression) {
    SecurityAuthzChangeIndex idx = primed();
    idx.note(event(sec::CreateUserOps{}, 7, "bob"), 5);
    idx.note(event(sec::AlterUserOps{sec::DisableUser{}}, 7, "bob"), kAt);
    EXPECT_TRUE(idx.changedSince(7, "bob", kAt - 1));
    EXPECT_FALSE(idx.changedSince(0, "bob", kAt - 1)) << "no cross-talk: (0, bob) was not touched";
    SecurityAuthzChangeIndex other = primed();
    other.note(event(sec::CreateUserOps{}, 7, "bob"), 5);
    other.note(event(sec::AlterUserOps{sec::DisableUser{}}, 0, "bob"), kAt);
    EXPECT_TRUE(other.changedSince(0, "bob", kAt - 1));
    EXPECT_FALSE(other.changedSince(7, "bob", kAt - 1));
    EXPECT_FALSE(SecurityAuthzChangeIndex::affects(event(sec::AlterUserOps{sec::DisableUser{}}, 7, "bob"), 0, "bob"));
}

TEST(SecurityAuthzChangeIndexRegression, TheFencePredicateNeverStalesAtTheStampAndAlwaysAheadOfItRegression) {
    SecurityAuthzChangeIndex idx = primed();
    idx.note(event(sec::CreateRoleOps{}, 0, "r"), 5);   // wide
    // stamp == applied: never stale, not even for an unknown principal (no lookup).
    EXPECT_FALSE(idx.staleAt(5, 5, 0, "zed", 0));
    EXPECT_FALSE(idx.staleAt(5, 5, 0, "", 0));
    // stamp > applied cannot happen: stale.
    EXPECT_TRUE(idx.staleAt(5, 6, 0, "bob", 0));
    // stamp < applied: the index decides; the local name starts at `localAt` (a prefixed principal).
    EXPECT_TRUE(idx.staleAt(5, 4, 0, "bob", 0)) << "the wide event at 5";
    idx.note(event(sec::AlterUserOps{sec::DisableUser{}}, 0, "alice"), 8);
    EXPECT_FALSE(idx.staleAt(8, 5, 0, "bob", 0)) << "an event about another user";
    EXPECT_TRUE(idx.staleAt(8, 5, 0, "alice", 0));
    EXPECT_TRUE(idx.staleAt(8, 5, 0, "ns/alice", 3)) << "the prefix is skipped";
    EXPECT_TRUE(idx.staleAt(8, 5, 0, "ns/", 3)) << "an empty local name is unknown";
    EXPECT_TRUE(idx.staleAt(8, 5, 0, "", 0)) << "anonymous";
}

// Every boot replays [0, checkpoint] through applyRecord_ (SecurityApplier::replay_), so the index sees every event
// of the log: a user that exists always has an entry (CreateUser / Bootstrap), and a restart does not make an admin
// "unknown".  An admin with no event of its own since the boot pipelines across events about others.
TEST(SecurityAuthzChangeIndexRegression, AnAdminWithNoEventSinceTheBootIsKnownAfterARestartRegression) {
    rune::node::test::SecurityRig rig("authz_index_restart", /*goLive=*/true);
    rig.appendEvents(0, 8);
    ASSERT_EQ(rig.applier.applyCommitted(rig.replica()), 8);
    rig.restart();   // same log and checkpoint, a fresh applier
    ASSERT_GE(rig.applier.replayLocal(rig.log(), rig.rec.bootEffects()), 0);
    ASSERT_EQ(rig.applier.phaseName(), "Booted");
    ASSERT_TRUE(rig.applier.goLive(rig.rec.liveEffects()));
    const auto& idx = [&]() -> const SecurityAuthzChangeIndex& { return rig.applier.authzChanges(); }();
    ASSERT_EQ(rig.applier.state().appliedOffset(), 7);
    // "admin" (bootstrap, event 0) has had no event of its own since: known, not stale after the last wide event (4).
    EXPECT_FALSE(idx.changedSince(0, "admin", 7));
    EXPECT_TRUE(idx.changedSince(0, "admin", 3)) << "the role grants at 4 are wide";
    EXPECT_TRUE(idx.changedSince(0, "nobody", 7)) << "a user that does not exist stays strict";
    rig.appendEvents(8, 9);   // ALTER USER ws1: another user
    ASSERT_EQ(rig.applier.applyCommitted(rig.replica()), 1);
    EXPECT_FALSE(idx.changedSince(0, "admin", 7)) << "across an event about another user";
    rig.appendEvents(9, 11);   // 9: REVOKE from a role (wide), 10: ALTER USER admin
    ASSERT_EQ(rig.applier.applyCommitted(rig.replica()), 2);
    EXPECT_TRUE(idx.changedSince(0, "admin", 9)) << "across an event about itself";
    EXPECT_FALSE(idx.changedSince(0, "admin", 10));
}
