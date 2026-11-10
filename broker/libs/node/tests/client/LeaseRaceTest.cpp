// LeaseRaceTest.cpp - lease topics Task 17 (spec §16.2): a client op racing the lease's own expiry on a REAL 1-node broker, at four points
// around the deadline.  Before the deadline the op must win; long after it the Expire must have won; in between (the deadline itself and
// one millisecond past it) either may win, but the reply and the final state must agree: an acknowledged op took effect and no Expire
// follows it, a refused op leaves the lease expired.  The lease clock is the injected one; no sleeps.
#include "support/LeaseBrokerRig.h"

#include <rune/client/lease/LeaseTopic.h>
#include <rune/protocol/client/Protocol.h>

#include <gtest/gtest.h>

#include <thread>

using namespace rune::node::test;
using namespace std::chrono_literals;
namespace rc = rune::client;
namespace pc = rune::protocol::client;
using rune::gen::LeaseClaim;

namespace {
constexpr uint16_t kNotFound = static_cast<uint16_t>(pc::ErrorCode::LeaseNotFound);
constexpr uint16_t kKeyExists = static_cast<uint16_t>(pc::ErrorCode::KeyExists);
using Lt = rc::LeaseTopic<LeaseClaim>;

struct Point { const char* name; int64_t offsetMs; bool mustWin; bool mustLose; };
constexpr Point kPoints[] = {{"BeforeDue", -1000, true, false}, {"AtDeadline", 0, false, false}, {"JustPast", 1, false, false},
                             {"LongPast", 20'000, false, true}};
constexpr int64_t kTtlMs = 10'000;

/// Grants `key` with the fixed ttl and moves the clock to deadline + offset.
int64_t grantAndMove(LeaseBrokerRig& rig, Lt& lt, const LeaseClaim& r, int64_t offsetMs) {
    rc::LeaseOptions o;
    o.ttlMs = kTtlMs;
    const rc::LeaseResult g = lt.grantAndWait(r, o);
    EXPECT_TRUE(g.ok());
    rig.advance(std::chrono::milliseconds(kTtlMs + offsetMs));
    return g.version;
}
/// A keepalive at the deadline INSTANT is parked by the worker until the lease clock passes it (it can be neither granted nor refused at
/// now == deadline), so the clock must move: the call runs on a helper thread while the test thread advances the clock by 1 ms.  Either
/// order of the two is a valid interleaving (a keepalive just past the deadline is the next point).
rc::LeaseResult keepAliveAt(LeaseBrokerRig& rig, Lt& lt, const LeaseClaim& r, int64_t v, int64_t offsetMs) {
    if (offsetMs != 0) return lt.keepAliveOnceAndWait(r, v);
    rc::LeaseResult res;
    std::thread th([&] { res = lt.keepAliveOnceAndWait(r, v); });
    rig.advance(1ms);
    th.join();
    return res;
}
/// Whether the lease is still in the view; polls are bounded (each is a round trip) and settle once the answer is stable.
bool alive(Lt& lt, const LeaseClaim& r) {
    bool a = lt.getAndWait(r).has_value();
    for (int i = 0; i < 200; ++i) {   // an Expire in flight lands within a few round trips
        const bool b = lt.getAndWait(r).has_value();
        if (b != a) a = b;
    }
    return a;
}
}  // namespace

TEST(LeaseRaceRegression, KeepAliveAtFourPointsRegression) {
    LeaseBrokerRig rig("lease-race-ka", 60'000, 1000, 600'000);
    auto lt = rig.ua().openLeaseTopicAndWait<LeaseClaim>("t", {}, 30s);
    int idx = 0;
    for (const Point& p : kPoints) {
        const LeaseClaim r = rig.row("ka-" + std::string(p.name), uint64_t(++idx));
        const int64_t v = grantAndMove(rig, lt, r, p.offsetMs);
        const rc::LeaseResult ka = keepAliveAt(rig, lt, r, v, p.offsetMs);
        if (p.mustWin) EXPECT_TRUE(ka.ok()) << p.name;
        if (p.mustLose) EXPECT_EQ(ka.errorCode, kNotFound) << p.name << " status " << int(ka.status);
        if (ka.ok()) {
            EXPECT_TRUE(lt.getAndWait(r).has_value()) << p.name << ": an acknowledged keepalive keeps the lease";
            rig.advance(5s);   // 5 s into the renewed ttl: still alive, the old deadline no longer applies
            EXPECT_TRUE(alive(lt, r)) << p.name << ": no Expire follows an acknowledged keepalive";
            rig.advance(10s);
            bool gone = false;
            for (int i = 0; i < 600 && !gone; ++i) gone = !lt.getAndWait(r).has_value();
            EXPECT_TRUE(gone) << p.name << ": and it expires on the renewed deadline";
        } else {
            EXPECT_EQ(ka.errorCode, kNotFound) << p.name << " a refused keepalive is LeaseNotFound, status " << int(ka.status) << " code " << ka.errorCode << " ttl " << ka.ttlMs;
            bool gone = false;
            for (int i = 0; i < 600 && !gone; ++i) gone = !lt.getAndWait(r).has_value();
            EXPECT_TRUE(gone) << p.name << ": a refused keepalive means the lease expired";
            EXPECT_TRUE(lt.grantAndWait(r).ok()) << p.name << ": and the key is free";
        }
    }
}

TEST(LeaseRaceRegression, RevokeAndUpdateAtFourPointsRegression) {
    LeaseBrokerRig rig("lease-race-ru", 60'000, 1000, 600'000);
    auto lt = rig.ua().openLeaseTopicAndWait<LeaseClaim>("t", {}, 30s);
    int idx = 100;
    for (const Point& p : kPoints) {
        {   // update
            const LeaseClaim r = rig.row("up-" + std::string(p.name), uint64_t(++idx));
            const int64_t v = grantAndMove(rig, lt, r, p.offsetMs);
            const rc::LeaseResult u = lt.updateAndWait(rig.row("up-" + std::string(p.name), 4242), v);
            if (p.mustWin) EXPECT_TRUE(u.ok()) << "update " << p.name;
            if (p.mustLose) EXPECT_EQ(u.errorCode, kNotFound) << "update " << p.name << " status " << int(u.status);
            if (u.ok()) {
                const auto row = lt.getAndWait(r);
                if (row.has_value()) EXPECT_EQ(row->a, 4242u) << "update " << p.name << ": the acknowledged update is in the row";
                else ADD_FAILURE() << "update " << p.name << ": acknowledged but the lease is gone without an Expire after it";
                rig.advance(30s);   // an update never renews: the grant's deadline (long past now) still applies
                bool gone = false;
                for (int i = 0; i < 600 && !gone; ++i) gone = !lt.getAndWait(r).has_value();
                EXPECT_TRUE(gone) << "update " << p.name << ": the lease still ends on the grant's deadline";
            } else {
                EXPECT_EQ(u.errorCode, kNotFound) << "update " << p.name;
                bool gone = false;
                for (int i = 0; i < 600 && !gone; ++i) gone = !lt.getAndWait(r).has_value();
                EXPECT_TRUE(gone) << "update " << p.name << ": refused means expired";
            }
        }
        {   // revoke
            const LeaseClaim r = rig.row("rv-" + std::string(p.name), uint64_t(++idx));
            const int64_t v = grantAndMove(rig, lt, r, p.offsetMs);
            const rc::LeaseResult rv = lt.revokeAndWait(r, v);
            if (p.mustWin) EXPECT_TRUE(rv.ok()) << "revoke " << p.name;
            if (p.mustLose) EXPECT_EQ(rv.errorCode, kNotFound) << "revoke " << p.name << " status " << int(rv.status);
            if (!rv.ok()) EXPECT_EQ(rv.errorCode, kNotFound) << "revoke " << p.name;
            bool gone = false;
            for (int i = 0; i < 600 && !gone; ++i) gone = !lt.getAndWait(r).has_value();
            EXPECT_TRUE(gone) << "revoke " << p.name << ": revoked or expired, the key is gone either way";
            const rc::LeaseResult g = lt.grantAndWait(r);
            EXPECT_TRUE(g.ok()) << "revoke " << p.name << ": and free";
            if (rv.ok() && g.ok()) EXPECT_GT(g.version, v);
        }
    }
}

// M4: contenders' grants of a live key are rejected (KeyExists) and must not defer its Expire.
TEST(LeaseRaceRegression, ContenderGrantsDoNotDeferExpireRegression) {
    LeaseBrokerRig rig("lease-race-contend", 60'000, 1000, 600'000);
    auto lt = rig.ua().openLeaseTopicAndWait<LeaseClaim>("t", {}, 30s);
    rc::LeaseOptions o;
    o.ttlMs = kTtlMs;
    const LeaseClaim r = rig.row("k-contend", 1);
    const rc::LeaseResult g = lt.grantAndWait(r, o);
    ASSERT_TRUE(g.ok());
    for (int sec = 1; sec <= 9; ++sec) {   // a contender retries every second of the lease's life
        rig.advance(1s);
        const rc::LeaseResult c = lt.grantAndWait(r, o);
        EXPECT_EQ(c.errorCode, kKeyExists) << "second " << sec;
        EXPECT_FALSE(c.ok());
    }
    const auto row = lt.getAndWait(r);
    ASSERT_TRUE(row.has_value());
    EXPECT_EQ(row->lease_version, g.version) << "the contenders changed nothing";
    rig.advance(2s);   // 11 s since the grant
    bool gone = false;
    for (int i = 0; i < 600 && !gone; ++i) gone = !lt.getAndWait(r).has_value();
    EXPECT_TRUE(gone) << "the contenders' rejected grants did not defer the Expire past the grant's own deadline";
}
