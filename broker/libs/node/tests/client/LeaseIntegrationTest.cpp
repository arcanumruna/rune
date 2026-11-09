// LeaseIntegrationTest.cpp - lease topics Task 17 (spec §16.2): the 1-node broker tests on LeaseBrokerRig (a REAL authenticating shard
// broker, REAL clients, the injected lease clock - no sleeps for any lease deadline).  Polls below are bounded by an iteration cap whose
// end is fixed before the loop; each poll is a broker round trip.
#include "support/LeaseBrokerRig.h"

#include <rune/client/lease/LeaseTopic.h>
#include <rune/protocol/client/Protocol.h>

#include <gtest/gtest.h>

#include <map>

using namespace rune::node::test;
using namespace std::chrono_literals;
namespace rc = rune::client;
namespace pc = rune::protocol::client;
using rune::gen::LeaseClaim;

namespace {
constexpr uint16_t kNotFound = static_cast<uint16_t>(pc::ErrorCode::LeaseNotFound);
constexpr uint16_t kTooLarge = static_cast<uint16_t>(pc::ErrorCode::LeaseTtlTooLarge);

using Lt = rc::LeaseTopic<LeaseClaim>;

/// Polls (capped) until the lease view of `r` has no row.
bool waitGone(Lt& lt, const LeaseClaim& r) {
    for (int i = 0; i < 600; ++i)
        if (!lt.getAndWait(r).has_value()) return true;
    return false;
}
bool waitHas(Lt& lt, const LeaseClaim& r) {
    for (int i = 0; i < 600; ++i)
        if (lt.getAndWait(r).has_value()) return true;
    return false;
}
int64_t remainingMs(Lt& lt, const LeaseClaim& r) {
    rc::LeaseTtlInfo info;
    const rc::LeaseResult t = lt.timeToLiveAndWait(r, &info);
    return t.ok() ? info.remainingMs : -2;
}
}  // namespace

TEST(LeaseIntegrationRegression, GrantExpiryRegression) {
    LeaseBrokerRig rig("lease-int-expiry");
    auto lt = rig.ua().openLeaseTopicAndWait<LeaseClaim>("t", {}, 30s);
    rc::LeaseOptions o;
    o.ttlMs = 5000;
    o.holder = 3;
    const LeaseClaim r = rig.row("k-exp", 77);
    const rc::LeaseResult g = lt.grantAndWait(r, o);
    ASSERT_TRUE(g.ok()) << int(g.status) << " " << g.errorCode;
    bool seen = false;
    for (int i = 0; i < 2000 && !seen; ++i) seen = rig.userViewRow(r).has_value();
    ASSERT_TRUE(seen) << "view status " << rig.lastViewStatus() << ": the grant projects into the user view";
    ASSERT_TRUE(lt.getAndWait(r).has_value());

    rig.advance(4s);
    EXPECT_TRUE(lt.getAndWait(r).has_value()) << "not yet due";

    rig.advance(2s);   // 6 s > 5 s
    EXPECT_TRUE(waitGone(lt, r)) << "an expired lease leaves the lease view";
    bool userGone = false;
    for (int i = 0; i < 2000 && !userGone; ++i) userGone = !rig.userViewRow(r).has_value();
    EXPECT_TRUE(userGone) << "and the user view";
    const rc::LeaseResult again = lt.grantAndWait(r, o);
    ASSERT_TRUE(again.ok()) << "the key is free again";
    EXPECT_GT(again.version, g.version) << "a regrant has a larger version";
}

TEST(LeaseIntegrationRegression, FencingRegression) {
    LeaseBrokerRig rig("lease-int-fence");
    auto lt = rig.ua().openLeaseTopicAndWait<LeaseClaim>("t", {}, 30s);
    const LeaseClaim r = rig.row("k-fence", 1);
    const rc::LeaseResult g1 = lt.grantAndWait(r);
    ASSERT_TRUE(g1.ok());
    ASSERT_TRUE(lt.revokeAndWait(r, g1.version).ok());
    const rc::LeaseResult g2 = lt.grantAndWait(r);
    ASSERT_TRUE(g2.ok());
    ASSERT_GT(g2.version, g1.version);

    // The first holder (version g1) is fenced out of every write and keepalive.
    const rc::LeaseResult u = lt.updateAndWait(rig.row("k-fence", 2), g1.version);
    EXPECT_FALSE(u.ok());
    EXPECT_EQ(u.errorCode, kNotFound) << "update at a stale version";
    const rc::LeaseResult rv = lt.revokeAndWait(r, g1.version);
    EXPECT_FALSE(rv.ok());
    EXPECT_EQ(rv.errorCode, kNotFound) << "revoke at a stale version";
    const rc::LeaseResult ka = lt.keepAliveOnceAndWait(r, g1.version);
    EXPECT_FALSE(ka.ok());
    EXPECT_EQ(ka.errorCode, kNotFound) << "keepalive at a stale version";
    const auto row = lt.getAndWait(r);
    ASSERT_TRUE(row.has_value());
    EXPECT_EQ(row->lease_version, g2.version) << "the live lease is untouched by the fenced calls";
    EXPECT_EQ(row->a, 1u);
    // The current holder still can.
    EXPECT_TRUE(lt.updateAndWait(rig.row("k-fence", 2), g2.version).ok());
    EXPECT_TRUE(lt.keepAliveOnceAndWait(r, g2.version).ok());
}

// M15: an update re-uses the deadline; only a keepalive renews it.
TEST(LeaseIntegrationRegression, UpdateDoesNotRenewRegression) {
    LeaseBrokerRig rig("lease-int-norenew");
    auto lt = rig.ua().openLeaseTopicAndWait<LeaseClaim>("t", {}, 30s);
    rc::LeaseOptions o;
    o.ttlMs = 10'000;
    const LeaseClaim r = rig.row("k-nr", 5);
    const rc::LeaseResult g = lt.grantAndWait(r, o);
    ASSERT_TRUE(g.ok());
    rig.advance(6s);
    const rc::LeaseResult u = lt.updateAndWait(rig.row("k-nr", 6), g.version);
    ASSERT_TRUE(u.ok()) << int(u.status) << " " << u.errorCode;
    const int64_t rem = remainingMs(lt, r);
    EXPECT_GT(rem, 0);
    EXPECT_LE(rem, 4500) << "an update leaves the old deadline (4 s left of 10 s), it does not re-arm to 10 s";
    rig.advance(5s);   // 11 s since the grant
    EXPECT_TRUE(waitGone(lt, r)) << "the lease expires on the grant's deadline although it was updated at 6 s";

    // The same sequence with a keepalive instead survives.
    const LeaseClaim r2 = rig.row("k-nr2", 7);
    const rc::LeaseResult g2 = lt.grantAndWait(r2, o);
    ASSERT_TRUE(g2.ok());
    rig.advance(6s);
    ASSERT_TRUE(lt.keepAliveOnceAndWait(r2, g2.version).ok());
    rig.advance(5s);
    EXPECT_TRUE(lt.getAndWait(r2).has_value()) << "a keepalive re-arms the full ttl";
    EXPECT_GT(remainingMs(lt, r2), 4500);
}

// A restart (clean stop and start) loses nothing: the same lease rows, with the remaining time kept to the broker's checkpoint
// granularity.  4 keys x 2 000 updates (recipe fallback scale) with checkpoint intervals passed in between.
TEST(LeaseIntegrationRegression, CompactionRestartRegression) {
    constexpr int kKeys = 4, kUpdates = 2000, kRounds = 20;
    LeaseBrokerRig rig("lease-int-restart", 3'600'000, 1000, 86'400'000);
    auto lt = rig.ua().openLeaseTopicAndWait<LeaseClaim>("t", {}, 30s);
    rc::LeaseOptions o;
    o.ttlMs = 1'200'000;
    std::map<int, int64_t> version;
    std::vector<LeaseClaim> rows;
    for (int k = 0; k < kKeys; ++k) {
        rows.push_back(rig.row("ck-" + std::to_string(k), uint64_t(k)));
        const rc::LeaseResult g = lt.grantAndWait(rows.back(), o);
        ASSERT_TRUE(g.ok());
        version[k] = g.version;
    }
    for (int u = 0; u < kUpdates; ++u)
        for (int k = 0; k < kKeys; ++k) {
            const rc::LeaseResult r = lt.updateAndWait(rig.row("ck-" + std::to_string(k), uint64_t(1000 + u)), version[k]);
            ASSERT_TRUE(r.ok()) << "key " << k << " update " << u << ": " << int(r.status) << " " << r.errorCode;
            version[k] = r.version;
            if (u % (kUpdates / kRounds) == 0 && k == 0) rig.advance(1s);   // checkpoint intervals pass between updates
        }
    // Snapshot of the lease view rows.
    struct Snap { int64_t version; uint64_t a; };
    std::map<int, Snap> before;
    for (int k = 0; k < kKeys; ++k) {
        ASSERT_TRUE(waitHas(lt, rows[k]));
        const auto row = lt.getAndWait(rows[k]);
        ASSERT_TRUE(row.has_value());
        before[k] = {row->lease_version, row->a};
        EXPECT_EQ(row->lease_version, version[k]);
        EXPECT_EQ(row->a, uint64_t(1000 + kUpdates - 1));
    }
    const int64_t remBefore = remainingMs(lt, rows[0]);
    ASSERT_GT(remBefore, 0);
    lt.close();

    rig.restart();
    auto lt2 = rig.ua().openLeaseTopicAndWait<LeaseClaim>("t", {}, 30s);
    for (int k = 0; k < kKeys; ++k) {
        ASSERT_TRUE(waitHas(lt2, rows[k])) << "key " << k << " survives the restart";
        const auto row = lt2.getAndWait(rows[k]);
        ASSERT_TRUE(row.has_value());
        EXPECT_EQ(row->lease_version, before[k].version) << "identical version, key " << k;
        EXPECT_EQ(row->a, before[k].a) << "identical row, key " << k;
    }
    const int64_t remAfter = remainingMs(lt2, rows[0]);
    EXPECT_GT(remAfter, 0) << "still live";
    EXPECT_LE(remAfter, 1'200'000 + 1000) << "at most the full ttl (plus one checkpoint interval)";
    EXPECT_GE(remAfter, remBefore - 60'000) << "the remaining time is kept to the checkpoint granularity (no re-arm, no loss)";
    lt2.close();
}

TEST(LeaseIntegrationRegression, GatingRegression) {
    LeaseBrokerRig rig("lease-int-gate");
    // A non-lease topic is not a lease topic.
    EXPECT_ANY_THROW((void)rig.ua().openLeaseTopicAndWait<LeaseClaim>("n", {}, 10s));

    // Namespaces: a/t and b/t are different topics; the same key lives in both.
    auto ltA = rig.ua().openLeaseTopicAndWait<LeaseClaim>("t", {}, 30s);
    auto ltB = rig.ub().openLeaseTopicAndWait<LeaseClaim>("t", {}, 30s);
    const LeaseClaim r = rig.row("k-gate", 9);
    const rc::LeaseResult ga = ltA.grantAndWait(r);
    ASSERT_TRUE(ga.ok());
    EXPECT_FALSE(ltB.getAndWait(r).has_value()) << "b/t does not see a/t's lease";
    const rc::LeaseResult gb = ltB.grantAndWait(r);
    EXPECT_TRUE(gb.ok()) << "the same key is free in b/t: " << int(gb.status) << " " << gb.errorCode;
    EXPECT_EQ(ltA.getAndWait(r)->lease_version, ga.version);

    // The lease bound: a ttl above lease.max_ttl is refused with its own code.
    rc::LeaseOptions big;
    big.ttlMs = 700'000;
    const rc::LeaseResult tl = ltA.grantAndWait(rig.row("k-big", 1), big);
    EXPECT_FALSE(tl.ok());
    EXPECT_TRUE(tl.status == rc::OpStatus::InvalidArgument || tl.errorCode == kTooLarge)
        << "the handle refuses it locally (the broker's own LeaseTtlTooLarge is pinned in the unit table); status " << int(tl.status);

    // A v17 session does not understand the lease codes: its lease ops are refused, never half-applied.
    bool refused = false;
    try {
        auto lt17 = rig.ua17().openLeaseTopicAndWait<LeaseClaim>("t", {}, 10s);
        const rc::LeaseResult g17 = lt17.grantAndWait(rig.row("k-v17", 1));
        refused = !g17.ok();
    } catch (const std::exception&) {
        refused = true;
    }
    EXPECT_TRUE(refused) << "a v17 session cannot grant a lease";
    EXPECT_FALSE(ltA.getAndWait(rig.row("k-v17", 1)).has_value());
}
