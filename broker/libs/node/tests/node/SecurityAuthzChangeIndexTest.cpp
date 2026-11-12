// SecurityAuthzChangeIndexTest.cpp - issues #218 / #67: the per-principal staleness rule of the admin thread's fence.
// A request stamped at offset S is stale only if an event applied after S could have changed what ITS user may do, and
// "could have changed" is the state machine's own affected-users set (sec::Applied), driven here through a REAL
// SecurityState: a role-side event stales the role's MEMBERS only; the bootstrap stales everyone; an unknown or
// anonymous principal is stale on any change.
#include <rune/node/SecurityAuthzChangeIndex.h>
#include <rune/security/SecurityState.h>

#include <gtest/gtest.h>

#include <stdexcept>
#include <string>

using rune::node::SecurityAuthzChangeIndex;
namespace sec = rune::security;

namespace {

sec::ScramVerifier verifier() {
    sec::ScramVerifier v;
    v.salt.assign(16, 0x11);
    v.iterations = 4096;
    v.storedKey.fill(0x31);
    v.serverKey.fill(0x51);
    return v;
}
sec::GrantSpec topicRead() {
    return sec::GrantSpec{sec::Operation::Read, {sec::ResourceType::Topic, sec::PatternKind::Exact, "t"}};
}
sec::GrantSpec arenaTopic() {
    return sec::GrantSpec{sec::Operation::Read, {sec::ResourceType::Topic, sec::PatternKind::Exact, "arena/t"}};
}

/// A real security state plus the index fed exactly like SecurityApplier feeds it: note(ev, applied, state, offset).
struct World {
    sec::SecurityState        s;
    SecurityAuthzChangeIndex  idx;
    int64_t                   offset = -1;

    /// The event for `ops` about `name` in namespace `ns`; ids and versions come from the current state.
    [[nodiscard]] sec::SecurityEvent make(sec::Ops ops, const std::string& name, uint32_t ns = 0) const {
        sec::SecurityEvent e;
        e.kind       = sec::kindOf(ops);
        e.ops        = std::move(ops);
        e.nsId       = ns;
        e.objectName = name;
        e.principal.authenticated = "admin";
        const bool roleTarget = e.kind == sec::EventKind::CreateRole || e.kind == sec::EventKind::DropRole ||
            (e.kind == sec::EventKind::Grant && std::get<sec::GrantOps>(e.ops).grantee == sec::GranteeKind::Role) ||
            (e.kind == sec::EventKind::Revoke && std::get<sec::RevokeOps>(e.ops).grantee == sec::GranteeKind::Role);
        const bool create = e.kind == sec::EventKind::CreateUser || e.kind == sec::EventKind::CreateRole ||
                            e.kind == sec::EventKind::Bootstrap;
        if (e.kind == sec::EventKind::NamespacePurged) {
            e.objectId = 0; e.prevVersion = 0; e.newVersion = 0;
        } else if (create) {
            e.objectId = s.nextObjectId(); e.prevVersion = 0; e.newVersion = 1;
        } else if (roleTarget) {
            const sec::RoleObject* r = s.roleByName(ns, name);
            if (r == nullptr) throw std::logic_error("no role " + name);
            e.objectId = r->objectId; e.prevVersion = r->version; e.newVersion = r->version + 1;
        } else {
            const sec::UserObject* u = s.userByName(ns, name);
            if (u == nullptr) throw std::logic_error("no user " + name);
            e.objectId = u->objectId; e.prevVersion = u->version; e.newVersion = u->version + 1;
        }
        auto fp = sec::previewFingerprint(s, e);
        if (!fp) throw std::logic_error("preview: " + fp.error().detail);
        e.fingerprint = *fp;
        return e;
    }
    /// Applies `e` at the next offset and notes it; returns that offset.
    int64_t apply(const sec::SecurityEvent& e) {
        auto a = sec::apply(s, e, offset + 1);
        if (!a) throw std::logic_error("apply: " + a.error().detail);
        ++offset;
        idx.note(e, *a, s, offset);
        return offset;
    }
    int64_t emit(sec::Ops ops, const std::string& name, uint32_t ns = 0) {
        return apply(make(std::move(ops), name, ns));
    }
    [[nodiscard]] uint64_t roleId(const std::string& n, uint32_t ns = 0) const { return s.roleByName(ns, n)->objectId; }
    [[nodiscard]] bool stale(uint32_t ns, const std::string& who, int64_t stamp) const {
        return idx.changedSince(s, ns, who, stamp);
    }
};

/// admin (bootstrap), role r with a grant, alice = member of r, bob = no member, and "alice" in namespace 7 (a role
/// "r" exists there too, with no members).
World base() {
    World w;
    w.emit(sec::BootstrapOps{verifier(), {}}, "admin");
    w.emit(sec::CreateRoleOps{}, "r");
    w.emit(sec::CreateUserOps{verifier()}, "alice");
    w.emit(sec::CreateUserOps{verifier()}, "bob");
    w.emit(sec::GrantRoleOps{w.roleId("r"), "r"}, "alice");
    w.emit(sec::CreateRoleOps{}, "r", 7);
    w.emit(sec::CreateUserOps{verifier()}, "alice", 7);
    return w;
}

struct Who { uint32_t ns; const char* name; };
constexpr Who kAlice{0, "alice"}, kBob{0, "bob"}, kAliceNs7{7, "alice"};

/// Applies `ev` and checks, for alice / bob / ns7-alice, both the post-apply answer (stale for the stamp offset-1) and
/// the pending-event answer (`affects`, asked before the apply) against the expectation.
void expectEvent(World& w, const char* what, const sec::SecurityEvent& ev, bool aliceStale, bool bobStale,
                 bool aliceNs7Stale) {
    SCOPED_TRACE(what);
    const bool pre[] = {SecurityAuthzChangeIndex::affects(w.s, ev, kAlice.ns, kAlice.name),
                        SecurityAuthzChangeIndex::affects(w.s, ev, kBob.ns, kBob.name),
                        SecurityAuthzChangeIndex::affects(w.s, ev, kAliceNs7.ns, kAliceNs7.name)};
    EXPECT_TRUE(SecurityAuthzChangeIndex::affects(w.s, ev, 0, "")) << "anonymous";
    EXPECT_TRUE(SecurityAuthzChangeIndex::affects(w.s, ev, 0, "zed")) << "an unknown principal";
    const int64_t at = w.apply(ev);
    EXPECT_EQ(w.stale(kAlice.ns, kAlice.name, at - 1), aliceStale) << "alice (member of r)";
    EXPECT_EQ(w.stale(kBob.ns, kBob.name, at - 1), bobStale) << "bob (no member)";
    EXPECT_EQ(w.stale(kAliceNs7.ns, kAliceNs7.name, at - 1), aliceNs7Stale) << "alice of another namespace";
    EXPECT_EQ(pre[0], aliceStale) << "affects: alice";
    EXPECT_EQ(pre[1], bobStale) << "affects: bob";
    EXPECT_EQ(pre[2], aliceNs7Stale) << "affects: alice of another namespace";
    // stamp == offset: nothing newer than the stamp, so only unknown / anonymous principals stay stale.
    EXPECT_FALSE(w.stale(kBob.ns, kBob.name, at));
    EXPECT_TRUE(w.stale(0, "zed", at));
    EXPECT_TRUE(w.stale(0, "", at));
}

}  // namespace

TEST(SecurityAuthzChangeIndexRegression, ARoleSideEventStalesOnlyTheRolesMembersRegression) {
    World w = base();
    expectEvent(w, "Grant to the role", w.make(sec::GrantOps{sec::GranteeKind::Role, topicRead()}, "r"),
                true, false, false);
    expectEvent(w, "Revoke from the role", w.make(sec::RevokeOps{sec::GranteeKind::Role, topicRead()}, "r"),
                true, false, false);
    expectEvent(w, "CreateRole", w.make(sec::CreateRoleOps{}, "r2"), false, false, false);
    expectEvent(w, "DropRole (alice a member)", w.make(sec::DropRoleOps{}, "r"), true, false, false);
}

TEST(SecurityAuthzChangeIndexRegression, AUserSideEventStalesThatUserOnlyRegression) {
    World w = base();
    expectEvent(w, "Grant to the user", w.make(sec::GrantOps{sec::GranteeKind::User, topicRead()}, "alice"),
                true, false, false);
    expectEvent(w, "Revoke from the user", w.make(sec::RevokeOps{sec::GranteeKind::User, topicRead()}, "alice"),
                true, false, false);
    expectEvent(w, "GrantRole", w.make(sec::GrantRoleOps{w.roleId("r"), "r"}, "bob"), false, true, false);
    expectEvent(w, "RevokeRole", w.make(sec::RevokeRoleOps{w.roleId("r"), "r"}, "bob"), false, true, false);
    expectEvent(w, "AlterUser", w.make(sec::AlterUserOps{sec::DisableUser{}}, "bob"), false, true, false);
    expectEvent(w, "CreateUser", w.make(sec::CreateUserOps{verifier()}, "carol"), false, false, false);
    EXPECT_FALSE(w.stale(0, "carol", w.offset)) << "the new user is known";
}

TEST(SecurityAuthzChangeIndexRegression, AUserThatJoinsARoleAfterItsGrantIsStaleBecauseOfTheMembershipRegression) {
    World w = base();
    w.emit(sec::GrantOps{sec::GranteeKind::Role, topicRead()}, "r");
    const int64_t before = w.offset;
    EXPECT_FALSE(w.stale(0, "bob", before));
    w.emit(sec::GrantRoleOps{w.roleId("r"), "r"}, "bob");
    EXPECT_TRUE(w.stale(0, "bob", before)) << "the GrantRole is an event about bob";
    // From now on bob IS a member: the next role grant stales him too.
    w.emit(sec::RevokeOps{sec::GranteeKind::Role, topicRead()}, "r");
    EXPECT_TRUE(w.stale(0, "bob", w.offset - 1));
}

TEST(SecurityAuthzChangeIndexRegression, ADroppedUserThatIsCreatedAgainIsKnownAgainRegression) {
    World w = base();
    const int64_t stamp = w.offset;
    w.emit(sec::DropUserOps{}, "bob");
    EXPECT_TRUE(w.stale(0, "bob", w.offset)) << "dropped: unknown, stale on any stamp";
    EXPECT_TRUE(w.stale(0, "bob", stamp));
    w.emit(sec::CreateUserOps{verifier()}, "bob");
    EXPECT_TRUE(w.stale(0, "bob", stamp)) << "a re-created name does not inherit \"unchanged\"";
    EXPECT_TRUE(w.stale(0, "bob", w.offset - 1));
    EXPECT_FALSE(w.stale(0, "bob", w.offset)) << "known again";
}

TEST(SecurityAuthzChangeIndexRegression, ANamespacePurgeStalesTheErasedAndTheRefreshedOnlyRegression) {
    World w = base();
    w.emit(sec::CreateUserOps{verifier()}, "carol");   // refreshed: holds a grant that names the purged namespace
    w.emit(sec::GrantOps{sec::GranteeKind::User, arenaTopic()}, "carol");
    w.emit(sec::CreateUserOps{verifier()}, "dave");    // untouched survivor
    w.emit(sec::CreateUserOps{verifier()}, "gone", 7);
    const int64_t stamp = w.offset;
    ASSERT_NE(w.s.userByName(7, "gone"), nullptr);
    const auto purge = w.make(sec::NamespacePurgedOps{}, "arena", 7);
    EXPECT_TRUE(SecurityAuthzChangeIndex::affects(w.s, purge, 0, "dave")) << "a pending purge: every principal";
    w.apply(purge);
    ASSERT_EQ(w.s.userByName(7, "gone"), nullptr);
    EXPECT_TRUE(w.stale(7, "gone", stamp)) << "an erased user is unknown";
    EXPECT_TRUE(w.stale(7, "gone", w.offset));
    EXPECT_TRUE(w.stale(0, "carol", stamp)) << "a refreshed survivor";
    EXPECT_FALSE(w.stale(0, "dave", stamp)) << "an untouched survivor";
    EXPECT_FALSE(w.stale(0, "bob", stamp));
    EXPECT_FALSE(w.stale(0, "carol", w.offset));
    // The index forgot the erased users: nothing leaks as the namespace churns.
    EXPECT_EQ(w.idx.usersForTest(), w.s.users().size() - 1) << "one entry per live user but the bootstrap admin";
}

TEST(SecurityAuthzChangeIndexRegression, TheBootstrapStalesEveryoneRegression) {
    World w;
    w.emit(sec::BootstrapOps{verifier(), {}}, "admin");
    const int64_t first = w.offset;
    w.emit(sec::CreateUserOps{verifier()}, "bob");
    // A user created after the bootstrap is not stale for a stamp at or after the bootstrap ...
    EXPECT_FALSE(w.stale(0, "admin", first));
    // ... and every known user is stale for a stamp before it.
    EXPECT_TRUE(w.stale(0, "admin", first - 1));
    EXPECT_TRUE(w.stale(0, "bob", first - 1));
    sec::SecurityEvent boot;
    boot.kind = sec::EventKind::Bootstrap;
    boot.ops  = sec::BootstrapOps{};
    EXPECT_TRUE(SecurityAuthzChangeIndex::affects(w.s, boot, 0, "bob")) << "a pending bootstrap: every principal";
}

TEST(SecurityAuthzChangeIndexRegression, TheFencePredicateNeverStalesAtTheStampAndAlwaysAheadOfItRegression) {
    World w = base();
    w.emit(sec::GrantOps{sec::GranteeKind::Role, topicRead()}, "r");   // about alice (member), not bob
    const int64_t at = w.offset;
    // stamp == applied: never stale, not even for an unknown principal (no lookup).
    EXPECT_FALSE(w.idx.staleAt(w.s, at, at, 0, "zed", 0));
    EXPECT_FALSE(w.idx.staleAt(w.s, at, at, 0, "", 0));
    // stamp > applied cannot happen: stale.
    EXPECT_TRUE(w.idx.staleAt(w.s, at, at + 1, 0, "bob", 0));
    // stamp < applied: the index decides; the local name starts at `localAt` (a prefixed principal).
    EXPECT_FALSE(w.idx.staleAt(w.s, at, at - 1, 0, "bob", 0)) << "a non-member";
    EXPECT_TRUE(w.idx.staleAt(w.s, at, at - 1, 0, "alice", 0)) << "a member";
    EXPECT_FALSE(w.idx.staleAt(w.s, at, at - 1, 0, "ns/bob", 3));
    EXPECT_TRUE(w.idx.staleAt(w.s, at, at - 1, 0, "ns/alice", 3));
    EXPECT_TRUE(w.idx.staleAt(w.s, at, at - 1, 0, "zed", 0)) << "unknown";
    EXPECT_TRUE(w.idx.staleAt(w.s, at, at - 1, 0, "", 0)) << "anonymous";
    EXPECT_TRUE(w.idx.staleAt(w.s, at, at - 1, 0, "bob", 9)) << "localAt past the end: empty local name";
}

TEST(SecurityAuthzChangeIndexRegression, ANameInAnotherNamespaceIsAnotherUserRegression) {
    World w = base();
    w.emit(sec::AlterUserOps{sec::DisableUser{}}, "alice", 7);
    EXPECT_TRUE(w.stale(7, "alice", w.offset - 1));
    EXPECT_FALSE(w.stale(0, "bob", w.offset - 1));
    const auto enable7  = w.make(sec::AlterUserOps{sec::EnableUser{}}, "alice", 7);
    const auto disable0 = w.make(sec::AlterUserOps{sec::DisableUser{}}, "bob");
    EXPECT_FALSE(SecurityAuthzChangeIndex::affects(w.s, enable7, 0, "bob"));
    EXPECT_FALSE(SecurityAuthzChangeIndex::affects(w.s, disable0, 7, "alice"));
}
