// tests/client/HubCommandClientTest.cpp — F8a: subscriber / group-consumer
// operations are commands on the agent ring, applied by the duty thread (or by
// close()'s final drain).  Client-level lifecycle regressions, both runner
// modes.  No sleeps: every wait is a bounded drive loop or an immediate check.
#include "../support/InvokerGateway.h"

#include <rune/client/Client.h>

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include "../support/TestCompletions.h"
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>
#include <unistd.h>

namespace rc = rune::client;
using namespace std::chrono_literals;

namespace {

std::atomic<int> g_hubCtr{0};

rc::ClientConfig hubCfg(int32_t in, int32_t eg, rc::RunnerMode mode) {
    rc::ClientConfig c;
    c.gatewayChannel  = "aeron:ipc";
    c.gatewayStreamId = in;
    c.egressChannel   = "aeron:ipc";
    c.egressStreamId  = eg;
    c.dataDir = "/tmp/hub_client_" + std::to_string(::getpid()) + "_" +
                std::to_string(g_hubCtr.fetch_add(1));
    c.runnerMode = mode;
    rc::TopicSpec ts;
    ts.name = "t";
    ts.topicId = 1;
    ts.partitionCount = 1;
    c.topics.push_back(ts);
    return c;
}

/// F8c C4: a handler-target ledger (exactly-once counts per token).  The
/// handler runs on the duty thread (or the closing thread): only read it once
/// that thread is this one (Invoker) or joined (after close()).
struct HandlerLedger {
    int             times = 0;
    rc::Completion  last{};
    static void on(void* ctx, const rc::Completion& c, std::span<const uint8_t>) noexcept {
        auto* self = static_cast<HandlerLedger*>(ctx);
        ++self->times;
        self->last = c;
    }
    rc::CompletionHandler handler() { return rc::CompletionHandler{&HandlerLedger::on, this}; }
};

test_support::PendingOp commitSyncOp(test_support::TestCq& cq, rc::GroupConsumer& gc) {
    return cq.issue([&](rc::Completions& q, rc::Token t) { return gc.commitSync(q, t); });
}

rc::Subscription subscribeT(rc::Client& c) {
    return c.subscribe(c.topic("t"), /*partition=*/0, rc::SubscribeOptions{rc::From::earliest()},
                       [](const rc::Record&) {});
}

const char* modeName(rc::RunnerMode m) { return m == rc::RunnerMode::Invoker ? "Invoker" : "Thread"; }

} // namespace

// Commands still sitting in the agent ring when close() runs (Invoker: nothing
// ever drained them) are applied by close()'s final drain and then finished —
// a commitSync completes Closed (exactly once), handles report inactive.
TEST(HubCommandClient, CommandsStillInTheRingAreFinishedByCloseRegression) {
    int port = 3960;
    for (const rc::RunnerMode mode : {rc::RunnerMode::Invoker, rc::RunnerMode::Thread}) {
        SCOPED_TRACE(modeName(mode));
        rc::Client client(hubCfg(port, port + 1, mode));
        port += 2;
        test_support::TestCq cq(client);
        auto sub = subscribeT(client);
        auto gc  = client.subscribeGroup("g", {"t"});
        auto f   = commitSyncOp(cq, gc);
        ASSERT_TRUE(f.accepted()) << f.describe();
        EXPECT_TRUE(sub.active());
        EXPECT_TRUE(gc.active());

        client.close();
        ASSERT_TRUE(f.ready()) << "commitSync must be completed by close(), not stranded";
        // Never connected: the hub completes it SessionLost when it applies it
        // (level-triggered, fix3 I2) — by the runner, or by close()'s drain.
        EXPECT_EQ(f.status(), rc::OpStatus::SessionLost);
        EXPECT_EQ(cq.timesCompleted(f.token()), 1);
        EXPECT_TRUE(cq.q().finished());
        EXPECT_FALSE(sub.active());
        EXPECT_FALSE(gc.active());
    }
}

// Handles may outlive their Client: once the Client closed, they never touch it
// again (unsubscribe / commit / close are no-ops, commitSync fails at once).
TEST(HubCommandClient, HandlesOutliveTheirClientRegression) {
    int port = 3966;
    for (const rc::RunnerMode mode : {rc::RunnerMode::Invoker, rc::RunnerMode::Thread}) {
        SCOPED_TRACE(modeName(mode));
        auto client = std::make_unique<rc::Client>(hubCfg(port, port + 1, mode));
        port += 2;
        auto sub = subscribeT(*client);
        auto gc  = client->subscribeGroup("g", {"t"});
        client.reset();  // close + destroy

        EXPECT_FALSE(sub.active());
        EXPECT_EQ(sub.position(), 0);
        EXPECT_NO_THROW(sub.unsubscribe());
        EXPECT_NO_THROW(gc.commit());
        HandlerLedger led;
        EXPECT_EQ(gc.commitSync(led.handler(), 1), rc::OpResult::Closed)
            << "rejected synchronously: the handle never touches the dead Client";
        EXPECT_EQ(led.times, 0);
        EXPECT_THROW(gc.commitSyncAndWait(1s), std::runtime_error);
        EXPECT_NO_THROW(gc.close());
        // Destructors run here, after the Client is gone.
    }
}

// subscribe() / subscribeGroup() after close() return finished handles (the
// command is never submitted) instead of handles that look live forever.
TEST(HubCommandClient, SubscribeAfterCloseReturnsFinishedHandlesRegression) {
    rc::Client client(hubCfg(3972, 3973, rc::RunnerMode::Invoker));
    client.close();
    auto sub = subscribeT(client);
    EXPECT_FALSE(sub.active());
    auto gc = client.subscribeGroup("g", {"t"});
    EXPECT_FALSE(gc.active());
    HandlerLedger led;
    EXPECT_EQ(gc.commitSync(led.handler(), 1), rc::OpResult::Closed);
    EXPECT_EQ(led.times, 0);
}

// A command committed to the ring AFTER close()'s final drain already ran (the
// exact window the post-submit re-check exists for) is reported as closed:
// the handle is finished and a commitSync is rejected Closed — never stranded.
TEST(HubCommandClient, CommandRacingCloseIsNeverStrandedRegression) {
    {
        // Thread mode: the app thread's registration goes through the ring (on
        // the Invoker owner it is applied directly and cannot race close()).
        rc::Client client(hubCfg(3974, 3975, rc::RunnerMode::Thread));
        client.testSetBeforeRegistrationSubmitHook([&] { client.close(); });
        auto sub = subscribeT(client);  // close() runs inside, before the submit
        EXPECT_FALSE(sub.active());
    }
    {
        rc::Client client(hubCfg(3976, 3977, rc::RunnerMode::Thread));
        client.testSetBeforeRegistrationSubmitHook([&] { client.close(); });
        auto gc = client.subscribeGroup("g", {"t"});
        EXPECT_FALSE(gc.active());
    }
    {
        // Thread mode: an app thread's commitSync goes through the ring (on the
        // Invoker owner it would be applied directly, never racing the ring).
        rc::Client client(hubCfg(3978, 3979, rc::RunnerMode::Thread));
        auto gc = client.subscribeGroup("g", {"t"});
        client.testSetBeforeRegistrationSubmitHook([&] { client.close(); });
        HandlerLedger led;
        EXPECT_EQ(gc.commitSync(led.handler(), 1), rc::OpResult::Closed)
            << "a commitSync racing close() is rejected (linearized by the ring), never stranded";
        EXPECT_EQ(led.times, 0);
        EXPECT_FALSE(gc.active());
    }
}

// One group per Client — the guard is app-side now (the hub is duty-owned).
TEST(HubCommandClient, SecondSubscribeGroupThrowsRegression) {
    rc::Client client(hubCfg(3980, 3981, rc::RunnerMode::Invoker));
    auto gc = client.subscribeGroup("g", {"t"});
    EXPECT_THROW((void)client.subscribeGroup("g2", {"t"}), std::logic_error);
}

// Back-pressure from inside a duty-cycle callback with a FULL command ring:
// commit() is applied directly on the duty thread (fix1 C1(a) — it never needs
// the ring); a ring-only command (a callback setter) fails fast (cannot pump
// from inside the cycle) — never dropped silently, never a self-deadlock.
// From the owner thread outside a callback a full ring is pumped.
TEST(HubCommandClient, HubCommandFromCallbackWithFullRingFailsFastRegression) {
    rc::ClientConfig c = hubCfg(3982, 3983, rc::RunnerMode::Invoker);
    rc::Client client(c);
    test_support::InvokerGateway gw(c.dataDir + "/aeron", c.gatewayChannel, c.gatewayStreamId,
                                    c.egressChannel, c.egressStreamId);
    auto gc = client.subscribeGroup("g", {"t"});
    bool threw = false;
    bool commitThrew = true;
    client.onConnected([&](int64_t) {
        client.testFillCommandRing();
        try {
            gc.commit();
            commitThrew = false;
        } catch (...) {
        }
        try {
            gc.onRecords([](const rc::RecordSet&) {});
        } catch (const std::logic_error&) {
            threw = true;
        }
    });
    auto drive = [&](auto pred) {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
        while (std::chrono::steady_clock::now() < deadline) {
            client.doWork();
            gw.pump();
            if (pred()) return true;
        }
        return false;
    };
    ASSERT_TRUE(drive([&] { return gw.ready(); }));
    test_support::TestCq cq(client);
    auto f = cq.issue([&](rc::Completions& q, rc::Token t) { return client.connect(q, t); });
    ASSERT_TRUE(drive([&] { return f.ready(); }));
    EXPECT_FALSE(commitThrew) << "commit() inside a callback is applied directly";
    EXPECT_TRUE(threw) << "a ring-only command fails fast on a full ring inside a callback";

    // Outside the cycle, on the owner thread: a full ring is pumped, not thrown.
    client.testFillCommandRing();
    EXPECT_NO_THROW(gc.onRecords([](const rc::RecordSet&) {}));
    EXPECT_TRUE(gc.active());
}

// ===========================================================================
// F8a fix round 1 — Client level, Invoker mode against the InvokerGateway.
// ===========================================================================
namespace {

rc::ClientConfig withOpts(rc::ClientConfig c, std::size_t termBytes, rc::NanoClock clock) {
    // IPC publications take their term length from the channel (the driver's
    // IPC default is 64 MiB): a small term makes back-pressure reachable.
    if (termBytes != 0) c.gatewayChannel = "aeron:ipc?term-length=" + std::to_string(termBytes);
    c.nanoClock = clock;
    return c;
}

struct HubRig {
    rc::ClientConfig             cfg;
    rc::Client                   client;
    test_support::InvokerGateway gw;
    explicit HubRig(int32_t in, std::size_t termBytes = 0, rc::NanoClock clock = nullptr)
        : cfg(withOpts(hubCfg(in, in + 1, rc::RunnerMode::Invoker), termBytes, clock)), client(cfg),
          gw(cfg.dataDir + "/aeron", cfg.gatewayChannel, cfg.gatewayStreamId,
             cfg.egressChannel, cfg.egressStreamId) {}

    template <class P>
    // cap < 0 (default): drive until pred holds, bounded by a 10 s deadline that
    // only a FAILING test reaches (iteration counts depend on CPU speed).
    bool drive(P&& pred, int cap = -1) {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
        for (int i = 0; cap < 0 ? std::chrono::steady_clock::now() < deadline : i < cap; ++i) {
            client.doWork();
            gw.pump();
            if (pred()) return true;
        }
        return false;
    }
    /// A bounded number of cycles with no predicate (lets in-flight frames land).
    void settle(int cycles = 2'000) {
        for (int i = 0; i < cycles; ++i) { client.doWork(); gw.pump(); }
    }
    // F8c C3: the test thread's completions (Invoker: this thread is the owner).
    test_support::TestCq cq{client};
    test_support::PendingOp pingOp(const rc::Bytes& payload) {
        return cq.issue([&](rc::Completions& q, rc::Token t) { return client.ping(payload, q, t); });
    }
    void connect() {
        ASSERT_TRUE(drive([&] { return gw.ready(); }));
        auto f = cq.issue([&](rc::Completions& q, rc::Token t) { return client.connect(q, t); });
        ASSERT_TRUE(drive([&] { return f.ready(); }));
        ASSERT_TRUE(f.ok()) << f.describe();
    }
};

/// M2: a two-outcome event the OTHER thread always completes — it either hit
/// the full ring (kFull, set by the ring-full hook) or finished without ever
/// meeting it (kNoFull).  Waiting on it is therefore bounded: no hang, and the
/// test FAILS (instead of hanging) if the full ring was never met.
enum : int { kPending = 0, kFull = 1, kNoFull = 2 };
void signal(std::atomic<int>& ev, int v) {
    int expected = kPending;
    ev.compare_exchange_strong(expected, v);
    ev.notify_all();
}
int waitEvent(std::atomic<int>& ev) {
    for (int v = ev.load(); v == kPending; v = ev.load()) ev.wait(kPending);
    return ev.load();
}

} // namespace

// C1(b): a full command ring cannot stop an unsubscribe.  A non-owner thread's
// unsubscribe waits on the duty thread's progress (the owner pumping doWork),
// never a timeout, never a throw; dispatch stops; no callback after finished().
TEST(HubCommandClient, RingFullUnsubscribeStillStopsDispatchRegression) {
    HubRig r(3984);
    r.connect();
    std::atomic<int> got{0};  // written on the owner (duty) thread, read by the test thread = same
    auto sub = r.client.subscribe(r.client.topic("t"), 0, rc::SubscribeOptions{rc::From::earliest()},
                                  [&](const rc::Record&) { got.fetch_add(1); });
    ASSERT_TRUE(r.drive([&] { return !r.gw.subscribed.empty() && sub.subscriptionId() > 0; }));
    r.gw.pushRecords(r.gw.subscribed[0], 0, 1);
    ASSERT_TRUE(r.drive([&] { return got.load() == 1; }));

    std::atomic<int> ev{kPending};
    r.client.testSetHubCommandRingFullHook([&] { signal(ev, kFull); });
    r.client.testFillCommandRing();
    std::atomic<bool> returned{false};
    std::thread t([&] {
        sub.unsubscribe();  // the handle's owner for this call; never throws
        returned.store(true);
        signal(ev, kNoFull);
    });
    if (waitEvent(ev) != kFull) {
        t.join();
        FAIL() << "the unsubscribe never met the full ring";
    }
    ASSERT_TRUE(r.drive([&] { return returned.load() && sub.finished(); }));
    t.join();

    EXPECT_FALSE(sub.active());
    ASSERT_TRUE(r.drive([&] { return !r.gw.unsubscribed.empty(); }));
    EXPECT_EQ(r.gw.unsubscribed[0], r.gw.subscribed[0].subscriptionId);
    r.gw.pushRecords(r.gw.subscribed[0], 1, 3);
    r.settle();
    EXPECT_EQ(got.load(), 1) << "no callback after finished()";
    EXPECT_EQ(sub.position(), 1) << "position() final after finished()";
}

// C1(b) / F8c C7: the destructor under a FULL command ring never waits.  In
// Invoker mode releasing a handle is duty work (the doWork() contract — here
// the off-owner destroy: the owner is not pumping meanwhile), applied directly
// even off the owner thread: the subscription is unsubscribed at once and its
// handler never runs again.
TEST(HubCommandClient, DestructorUnderFullRingStopsDispatchRegression) {
    HubRig r(3986);
    r.connect();
    std::atomic<int> got{0};
    auto sub = std::make_unique<rc::Subscription>(r.client.subscribe(
        r.client.topic("t"), 0, rc::SubscribeOptions{rc::From::earliest()},
        [&](const rc::Record&) { got.fetch_add(1); }));
    ASSERT_TRUE(r.drive([&] { return !r.gw.subscribed.empty() && sub->subscriptionId() > 0; }));

    std::atomic<int> fullHits{0};
    r.client.testSetHubCommandRingFullHook([&] { fullHits.fetch_add(1); });
    r.client.testFillCommandRing();
    std::thread t([&] { sub.reset(); });  // returns without any duty progress
    t.join();
    EXPECT_EQ(fullHits.load(), 0) << "the release never waited on the full ring";
    ASSERT_TRUE(r.drive([&] { return !r.gw.unsubscribed.empty(); }));
    r.gw.pushRecords(r.gw.subscribed[0], 0, 2);
    r.settle();
    EXPECT_EQ(got.load(), 0) << "the destroyed subscription's handler must never run";
    EXPECT_EQ(r.client.testSubscriptionImplsHeld(), 0u) << "the duty side let go (reaped)";
}

// C1(a): unsubscribe from inside the handler on the duty thread is applied
// directly — the rest of the batch is not delivered, finished() follows.
TEST(HubCommandClient, UnsubscribeInsideHandlerAppliesDirectlyRegression) {
    HubRig r(3988);
    r.connect();
    int got = 0;
    std::unique_ptr<rc::Subscription> sub;
    sub = std::make_unique<rc::Subscription>(r.client.subscribe(
        r.client.topic("t"), 0, rc::SubscribeOptions{rc::From::earliest()},
        [&](const rc::Record&) {
            ++got;
            sub->unsubscribe();
        }));
    ASSERT_TRUE(r.drive([&] { return !r.gw.subscribed.empty() && sub->subscriptionId() > 0; }));
    r.client.testFillCommandRing();  // the direct path must not need the ring
    r.gw.pushRecords(r.gw.subscribed[0], 0, 4);
    ASSERT_TRUE(r.drive([&] { return sub->finished(); }));
    EXPECT_EQ(got, 1);
    EXPECT_EQ(sub->position(), 1);
    // fix2 REQUIRED 1: the Unsubscribe frame itself must not depend on the
    // (full) command ring — the gateway sees it.
    ASSERT_TRUE(r.drive([&] { return !r.gw.unsubscribed.empty(); }))
        << "the Unsubscribe frame was lost to the full command ring";
    EXPECT_EQ(r.gw.unsubscribed[0], r.gw.subscribed[0].subscriptionId);
}

// F8c C4 (was I6): the TOKEN form of commitSync is allowed from inside a
// duty-cycle callback (it is just a command applied there) and completes
// exactly once; only the blocking commitSyncAndWait() throws there (it would
// wait for the very duty thread it blocks).
TEST(HubCommandClient, CommitSyncTokenFromCallbackIsAllowedRegression) {
    HubRig r(3990);
    auto gc = r.client.subscribeGroup("g", {"t"});
    HandlerLedger led;
    rc::OpResult inCbHandler = rc::OpResult::Closed;
    rc::OpResult inCbCq      = rc::OpResult::Closed;
    test_support::PendingOp cqOp;
    bool waitThrew = false;
    bool threwOther = false;
    r.client.onConnected([&](int64_t) {
        try {
            inCbHandler = gc.commitSync(led.handler(), 7);
            cqOp        = commitSyncOp(r.cq, gc);  // supersedes the handler one
            inCbCq      = cqOp.issued();
        } catch (...) {
            threwOther = true;
        }
        try {
            gc.commitSyncAndWait(1s);
        } catch (const std::logic_error&) {
            waitThrew = true;
        }
    });
    r.connect();
    EXPECT_FALSE(threwOther) << "the token form must not throw inside a callback";
    EXPECT_EQ(inCbHandler, rc::OpResult::Accepted);
    EXPECT_EQ(inCbCq, rc::OpResult::Accepted);
    EXPECT_TRUE(waitThrew) << "the blocking form must refuse inside a callback";
    // The first was superseded by the second (both applied directly, in order).
    ASSERT_EQ(led.times, 1);
    EXPECT_EQ(led.last.token, 7u);
    EXPECT_EQ(led.last.status, rc::OpStatus::Superseded);
    EXPECT_FALSE(cqOp.ready()) << "pending: the consumer never became Stable";
    // Outside a callback, on the owner thread, both forms are allowed.
    auto later = commitSyncOp(r.cq, gc);
    ASSERT_TRUE(later.accepted()) << later.describe();
    ASSERT_TRUE(cqOp.ready());
    EXPECT_EQ(cqOp.status(), rc::OpStatus::Superseded);
    r.client.close();
    ASSERT_TRUE(later.ready());
    EXPECT_EQ(later.status(), rc::OpStatus::Closed);
    EXPECT_EQ(led.times, 1) << "exactly once";
    EXPECT_EQ(r.cq.timesCompleted(cqOp.token()), 1);
    EXPECT_EQ(r.cq.timesCompleted(later.token()), 1);
}

// F8c C4 (spec Q6): a commitSync that never gets an answer completes Timeout
// after GroupConfig::commitSyncTimeoutMs of DUTY time — a stepped injected
// clock, no sleeps — exactly once (close() later delivers nothing more).
namespace {
int64_t g_csNow = 1'000'000'000;  // test thread only
int64_t csNow() noexcept { return g_csNow; }
} // namespace

TEST(HubCommandClient, CommitSyncTimesOutRegression) {
    HubRig r(4014, 0, &csNow);
    r.connect();
    rc::GroupConfig gcfg;
    gcfg.commitSyncTimeoutMs = 500;
    auto gc = r.client.subscribeGroup("g", {"t"}, gcfg);
    auto f  = commitSyncOp(r.cq, gc);  // never Stable (no group protocol here): no answer
    ASSERT_TRUE(f.accepted()) << f.describe();
    r.settle(200);
    ASSERT_FALSE(f.ready()) << "the clock has not moved";
    g_csNow += 400'000'000LL;  // 400 ms: still inside the deadline
    r.settle(200);
    ASSERT_FALSE(f.ready()) << "completed before its deadline";
    g_csNow += 200'000'000LL;  // 600 ms total > 500 ms
    ASSERT_TRUE(r.drive([&] { return f.ready(); }, 2'000)) << "never timed out on the duty clock";
    EXPECT_EQ(f.status(), rc::OpStatus::Timeout);
    EXPECT_EQ(r.client.state(), rc::session::SessionState::Connected) << "not a session loss";
    r.client.close();
    EXPECT_EQ(r.cq.timesCompleted(f.token()), 1) << "exactly once";
}

// ===========================================================================
// F8a fix round 2
// ===========================================================================

// ROOT CAUSE: a frame accepted into the command ring is never dropped by the
// drain when the transport refuses it (back-pressure): it stays at the head of
// the ring and is delivered once the publication drains.
TEST(HubCommandClient, RingDrainedFrameRetriedUnderTransportBackPressureRegression) {
    HubRig r(3992, /*termBytes=*/64 * 1024);
    r.connect();
    r.gw.pauseIngress = true;  // the gateway stops consuming: the publication fills
    constexpr int kPings = 120;  // ~120 KiB >> the 64 KiB term's window
    std::vector<test_support::PendingOp> futs;
    for (int i = 0; i < kPings; ++i) {
        futs.push_back(r.pingOp(rc::Bytes(1024, static_cast<uint8_t>(i))));
        ASSERT_TRUE(futs.back().accepted()) << futs.back().describe();
    }
    r.settle(5'000);
    ASSERT_GT(r.client.testIngressOfferRefused(), 0u) << "the transport must have refused (back-pressure)";
    r.gw.pauseIngress = false;
    ASSERT_TRUE(r.drive([&] { return r.gw.pings == kPings; }))
        << "only " << r.gw.pings << " of " << kPings << " pings delivered — refused frames were dropped";
    EXPECT_EQ(r.client.testStaleFramesDropped(), 0u);
}

// fix2 REQUIRED 1: the transport refuses the Unsubscribe (back-pressure): it
// stays pending and is retried by the duty cycle until the gateway sees it.
TEST(HubCommandClient, UnsubscribeRetriedWhenTheTransportRefusesItRegression) {
    HubRig r(3994, /*termBytes=*/64 * 1024);
    r.connect();
    auto sub = r.client.subscribe(r.client.topic("t"), 0, rc::SubscribeOptions{rc::From::earliest()},
                                  [](const rc::Record&) {});
    ASSERT_TRUE(r.drive([&] { return !r.gw.subscribed.empty() && sub.subscriptionId() > 0; }));

    r.gw.pauseIngress = true;
    std::vector<test_support::PendingOp> futs;
    for (int i = 0; i < 120; ++i) {
        futs.push_back(r.pingOp(rc::Bytes(1024, 1)));
        ASSERT_TRUE(futs.back().accepted()) << futs.back().describe();
    }
    r.settle(5'000);
    ASSERT_GT(r.client.testIngressOfferRefused(), 0u) << "the publication must be back-pressured";

    sub.unsubscribe();  // owner thread = duty thread: applied directly, offer refused
    r.settle(100);
    EXPECT_TRUE(sub.finished()) << "dispatch stops at once, independent of the wire";
    EXPECT_EQ(r.client.testPendingUnsubscribes(), 1u) << "refused: kept for retry, not dropped";

    r.gw.pauseIngress = false;
    ASSERT_TRUE(r.drive([&] { return !r.gw.unsubscribed.empty(); }))
        << "the refused Unsubscribe was never retried";
    EXPECT_EQ(r.gw.unsubscribed[0], r.gw.subscribed[0].subscriptionId);
    EXPECT_EQ(r.client.testPendingUnsubscribes(), 0u);
}

// fix2 REQUIRED 3 (IM-2): the broker goes away and never comes back — the
// session-DOWN transition itself fails a pending commitSync promptly (no
// timeout of its own: driven by the session machine's liveness verdict).
namespace {
int64_t g_hubNow = 1'000'000'000;  // test thread only
int64_t hubNow() noexcept { return g_hubNow; }
} // namespace

TEST(HubCommandClient, CommitSyncFailsWhenTheSessionGoesDownRegression) {
    HubRig r(3996, 0, &hubNow);
    r.connect();
    auto gc = r.client.subscribeGroup("g", {"t"});
    auto f = commitSyncOp(r.cq, gc);  // owner thread: applied directly; pending (not Stable)
    r.settle(100);
    ASSERT_FALSE(f.ready());
    ASSERT_EQ(r.client.state(), rc::session::SessionState::Connected);

    // The broker is gone: no more egress, no reconnect.  Advance the duty
    // clock past the session's response timeout and pump ONLY the client.
    g_hubNow += 11'000'000'000LL;
    bool down = false;
    for (int i = 0; i < 1'000 && !f.ready(); ++i) {
        r.client.doWork();
        down = down || r.client.state() != rc::session::SessionState::Connected;
    }
    EXPECT_TRUE(down);
    ASSERT_TRUE(f.ready()) << "commitSync must complete on the session-down transition";
    EXPECT_EQ(f.status(), rc::OpStatus::SessionLost);
}

// fix2 (found while writing the IM-2 test): on the Invoker owner a direct
// command must never overtake its own registration still queued in the ring.
TEST(HubCommandClient, DirectCommandNeverOvertakesItsOwnRegistrationRegression) {
    HubRig r(3998);
    r.connect();
    {
        auto gc = r.client.subscribeGroup("g", {"t"});
        auto f = commitSyncOp(r.cq, gc);  // before any doWork(): the add must already be applied
        r.settle(100);
        EXPECT_FALSE(f.ready()) << "commitSync failed as if the consumer did not exist";
        EXPECT_TRUE(gc.active());
    }
    int got = 0;
    auto sub = r.client.subscribe(r.client.topic("t"), 0, rc::SubscribeOptions{rc::From::earliest()},
                                  [&](const rc::Record&) { ++got; });
    sub.unsubscribe();  // before any doWork()
    ASSERT_TRUE(r.drive([&] { return sub.finished(); }));
    r.settle(500);
    if (!r.gw.subscribed.empty()) {
        r.gw.pushRecords(r.gw.subscribed[0], 0, 2);
        r.settle(500);
    }
    EXPECT_EQ(got, 0) << "an unsubscribed subscription came alive after its late adoption";
}

// ===========================================================================
// F8a fix round 3
// ===========================================================================

// I1(a): a frame no publication can carry fails at submit (synchronously) and
// never reaches the ring — later commands are not wedged behind it.
TEST(HubCommandClient, OversizePingFailsAtSubmitAndNeverWedgesTheRingRegression) {
    HubRig r(4000, /*termBytes=*/64 * 1024);  // max message = 8 KiB
    r.connect();
    ASSERT_EQ(r.client.testMaxFrameLength(), 8u * 1024);
    auto big = r.pingOp(rc::Bytes(16 * 1024, 7));
    EXPECT_EQ(big.issued(), rc::OpResult::InvalidArgument) << "an oversize frame must fail at submit";
    auto small = r.pingOp(rc::Bytes(64, 1));
    ASSERT_TRUE(small.accepted()) << small.describe();
    ASSERT_TRUE(r.drive([&] { return r.gw.pings == 1; })) << "a later command was wedged";
}

// I1(b): an offer the transport can never accept (Fatal) is dropped, logged and
// counted; later commands proceed.
TEST(HubCommandClient, FatalOfferIsDroppedAndLaterCommandsProceedRegression) {
    HubRig r(4002);
    r.connect();
    ASSERT_TRUE(r.drive([&] { return r.gw.metadataRequests > 0; }));  // ring idle
    r.settle(200);
    r.client.testForceIngressOffers(rc::transport::AeronTransport::OfferResult::Fatal, 1);
    auto a = r.pingOp(rc::Bytes(8, 1));
    auto b = r.pingOp(rc::Bytes(8, 2));
    ASSERT_TRUE(r.drive([&] { return r.gw.pings >= 1; })) << "the ring was wedged by a Fatal offer";
    r.settle(200);
    EXPECT_EQ(r.gw.pings, 1);
    EXPECT_EQ(r.client.testIngressFatalDropped(), 1u);
}

// I2: a commitSync issued while the session is ALREADY down fails promptly
// (level-triggered), instead of waiting forever for a commit that cannot go out.
TEST(HubCommandClient, CommitSyncAfterTheBrokerIsGoneFailsPromptlyRegression) {
    HubRig r(4004, 0, &hubNow);
    r.connect();
    auto gc = r.client.subscribeGroup("g", {"t"});
    r.settle(50);
    g_hubNow += 11'000'000'000LL;  // the broker is gone: no egress, no reconnect
    for (int i = 0; i < 1'000 && r.client.state() == rc::session::SessionState::Connected; ++i) {
        r.client.doWork();
    }
    ASSERT_NE(r.client.state(), rc::session::SessionState::Connected);
    auto f = commitSyncOp(r.cq, gc);  // issued AFTER the session-down transition
    for (int i = 0; i < 100 && !f.ready(); ++i) r.client.doWork();
    ASSERT_TRUE(f.ready()) << "a commitSync issued while the session is down must fail promptly";
    EXPECT_EQ(f.status(), rc::OpStatus::SessionLost);
}

// I3 (Invoker): handles created on a NON-owner thread (their Add still queued
// in the ring) then closed / unsubscribed on the owner (direct path) — the
// later adoption must honour it.
TEST(HubCommandClient, DirectCloseBeforeAdoptionOfAForeignThreadHandleIsHonouredRegression) {
    HubRig r(4006);
    r.connect();
    std::unique_ptr<rc::GroupConsumer> gc;
    std::unique_ptr<rc::Subscription>  sub;
    int got = 0;
    std::thread t([&] {  // non-owner: both registrations go through the ring
        gc  = std::make_unique<rc::GroupConsumer>(r.client.subscribeGroup("g", {"t"}));
        sub = std::make_unique<rc::Subscription>(r.client.subscribe(
            r.client.topic("t"), 0, rc::SubscribeOptions{rc::From::earliest()},
            [&](const rc::Record&) { ++got; }));
    });
    t.join();  // hand-off to the owner; nothing has been drained yet
    gc->close();         // owner = duty thread: applied directly, before adoption
    sub->unsubscribe();
    r.settle(1'000);
    EXPECT_TRUE(gc->finished());
    EXPECT_TRUE(r.client.testGroupHubEmpty()) << "a closed consumer was revived by its late adoption";
    EXPECT_TRUE(sub->finished());
    EXPECT_TRUE(r.gw.subscribed.empty()) << "an unsubscribed subscription was revived by its late adoption";
}

// I4: the adoption-time Subscribe never depends on command-ring space.
TEST(HubCommandClient, SubscribeAdoptedWithAFullRingStillReachesTheBrokerRegression) {
    HubRig r(4008);
    r.connect();
    r.client.testFillCommandRing();
    auto sub = r.client.subscribe(r.client.topic("t"), 0, rc::SubscribeOptions{rc::From::earliest()},
                                  [](const rc::Record&) {});  // owner: adopted directly
    ASSERT_TRUE(r.drive([&] { return !r.gw.subscribed.empty(); }))
        << "the Subscribe frame was lost to the full command ring";
    ASSERT_TRUE(r.drive([&] { return sub.subscriptionId() > 0; }));
}

// I4: a Subscribe the transport refuses at adoption is retried per duty cycle.
TEST(HubCommandClient, SubscribeRefusedAtAdoptionIsRetriedRegression) {
    HubRig r(4010, /*termBytes=*/64 * 1024);
    r.connect();
    r.gw.pauseIngress = true;
    std::vector<test_support::PendingOp> futs;
    for (int i = 0; i < 120; ++i) {
        futs.push_back(r.pingOp(rc::Bytes(1024, 1)));
        ASSERT_TRUE(futs.back().accepted()) << futs.back().describe();
    }
    r.settle(5'000);
    ASSERT_GT(r.client.testIngressOfferRefused(), 0u);
    auto sub = r.client.subscribe(r.client.topic("t"), 0, rc::SubscribeOptions{rc::From::earliest()},
                                  [](const rc::Record&) {});
    r.settle(100);
    EXPECT_EQ(r.client.testUnsentSubscribes(), 1u);
    r.gw.pauseIngress = false;
    ASSERT_TRUE(r.drive([&] { return !r.gw.subscribed.empty() && sub.subscriptionId() > 0; }));
    EXPECT_EQ(r.client.testUnsentSubscribes(), 0u);
}

// I3 (Thread mode): an app-thread handle whose SubscribeAdd is still queued in
// the ring, unsubscribed from inside a runner callback (direct path) — the
// later adoption must drop it (no Subscribe ever sent, finished()).
TEST(HubCommandClient, RunnerCallbackUnsubscribesAnUnadoptedAppHandleRegression) {
    rc::ClientConfig c = hubCfg(4012, 4013, rc::RunnerMode::Thread);
    rc::Client client(c);
    test_support::InvokerGateway gw(c.dataDir + "/aeron", c.gatewayChannel, c.gatewayStreamId,
                                    c.egressChannel, c.egressStreamId);
    auto pump = [&](auto pred, int cap = 2'000'000) {
        for (int i = 0; i < cap; ++i) {
            gw.pump();
            if (pred()) return true;
            std::this_thread::yield();
        }
        return false;
    };
    ASSERT_TRUE(pump([&] { return gw.ready(); }));
    test_support::TestCq cq(client);
    auto cf = cq.issue([&](rc::Completions& q, rc::Token t) { return client.connect(q, t); });
    ASSERT_TRUE(pump([&] { return cf.ready(); }));

    std::unique_ptr<rc::Subscription> b;  // written by the test thread before `ev`
    std::atomic<int> inHandler{kPending}; // runner -> test: "blocked in A's handler"
    std::atomic<int> bReady{kPending};    // test -> runner: "b exists (Add queued)"
    std::atomic<bool> handled{false};
    auto a = client.subscribe(client.topic("t"), 0, rc::SubscribeOptions{rc::From::earliest()},
        [&](const rc::Record&) {
            if (handled.exchange(true)) return;
            signal(inHandler, kFull);
            if (waitEvent(bReady) == kFull && b) b->unsubscribe();  // runner: direct path
        });
    ASSERT_TRUE(pump([&] { return gw.subscribed.size() == 1; }));
    ASSERT_TRUE(pump([&] { return a.subscriptionId() > 0; }));
    gw.pushRecords(gw.subscribed[0], 0, 1);
    if (!pump([&] { return inHandler.load() == kFull; })) {
        signal(bReady, kNoFull);
        FAIL() << "the runner never entered the handler";
    }
    // The runner is parked inside A's handler: B's SubscribeAdd stays queued.
    b = std::make_unique<rc::Subscription>(client.subscribe(
        client.topic("t"), 0, rc::SubscribeOptions{rc::From::earliest()}, [](const rc::Record&) {}));
    signal(bReady, kFull);
    ASSERT_TRUE(pump([&] { return b->finished(); })) << "the direct unsubscribe was lost";
    // fix4 M3: FIFO barrier instead of "pump a while".  The ping is submitted
    // AFTER B's SubscribeAdd on the same command ring, and every control frame
    // (a direct Subscribe sent at B's adoption, or the ring-drained ping) goes
    // out on the same ingress publication — so once the gateway has seen the
    // ping, it has already seen any Subscribe B's adoption could have sent.
    auto barrier = cq.issue([&](rc::Completions& q, rc::Token t) { return client.ping(rc::Bytes(8, 1), q, t); });
    ASSERT_TRUE(pump([&] { return gw.pings == 1; })) << "the barrier ping never arrived";
    EXPECT_EQ(gw.subscribed.size(), 1u) << "the unsubscribed handle was revived by its late adoption";
    client.close();
}

// ===========================================================================
// F8a fix round 4
// ===========================================================================

// I-1: a Subscribe adopted between the ConnectResponse and the end of the
// duty cycle (here: from the onConnected callback, which runs inside the
// ConnectResponse handler) must reach the broker.  Before: the hubs learned
// "connected" only at the END of the cycle, so the direct adoption saw "down",
// sent nothing and never retried — no later ConnectResponse re-sends it.
TEST(HubCommandClient, SubscribeFromOnConnectedCallbackReachesTheBrokerRegression) {
    HubRig r(4014);
    std::unique_ptr<rc::Subscription> sub;
    r.client.onConnected([&](int64_t) {
        sub = std::make_unique<rc::Subscription>(subscribeT(r.client));  // duty thread: direct
    });
    r.connect();
    ASSERT_TRUE(sub);
    ASSERT_TRUE(r.drive([&] { return !r.gw.subscribed.empty(); }))
        << "the Subscribe adopted inside the ConnectResponse handler was lost";
    ASSERT_TRUE(r.drive([&] { return sub->subscriptionId() > 0; }));
}

// I-1, the reviewer's exact path: after a reconnect, the ConnectResponse and a
// Records push arrive in ONE poll; the record handler subscribes (duty thread,
// direct) — the new Subscribe must reach the broker.
TEST(HubCommandClient, SubscribeFromARecordHandlerInTheReconnectPollReachesTheBrokerRegression) {
    HubRig r(4016, 0, &hubNow);
    r.connect();
    std::unique_ptr<rc::Subscription> b;
    int got = 0;
    auto a = r.client.subscribe(r.client.topic("t"), 0, rc::SubscribeOptions{rc::From::earliest()},
        [&](const rc::Record&) {
            if (++got == 1) b = std::make_unique<rc::Subscription>(subscribeT(r.client));
        });
    ASSERT_TRUE(r.drive([&] { return a.subscriptionId() > 0; }));
    ASSERT_EQ(r.gw.subscribed.size(), 1u);

    // The session goes down (liveness timeout on the duty clock) ...
    g_hubNow += 11'000'000'000LL;
    for (int i = 0; i < 1'000 && r.client.state() == rc::session::SessionState::Connected; ++i) {
        r.client.doWork();
    }
    ASSERT_NE(r.client.state(), rc::session::SessionState::Connected);
    // ... and resumes: the gateway answers the next Connect with
    // [ConnectResponse, Records(A)] back to back on one publication.
    r.gw.recordsAfterNextConnect.emplace_back(r.gw.subscribed[0], 0);
    const int connectsBefore = r.gw.connects;
    ASSERT_TRUE(r.drive([&] { return b != nullptr; })) << "A's handler never ran after the reconnect";
    ASSERT_GT(r.gw.connects, connectsBefore);
    ASSERT_TRUE(r.drive([&] { return r.gw.subscribed.size() == 2; }))
        << "the Subscribe adopted in the reconnect poll was lost";
    ASSERT_TRUE(r.drive([&] { return b->subscriptionId() > 0; }));
}

// M1: a Subscribe the transport can never carry (Fatal) fails the subscription
// at once — it is not re-offered forever.
TEST(HubCommandClient, FatalSubscribeOfferFailsTheSubscriptionRegression) {
    HubRig r(4018);
    r.connect();
    ASSERT_TRUE(r.drive([&] { return r.gw.metadataRequests > 0; }));
    r.settle(200);
    r.client.testForceIngressOffers(rc::transport::AeronTransport::OfferResult::Fatal, 1);
    auto sub = subscribeT(r.client);  // owner: adopted directly -> Fatal
    r.settle(200);
    EXPECT_TRUE(sub.finished()) << "a never-deliverable Subscribe must fail the subscription";
    EXPECT_FALSE(sub.active());
    EXPECT_EQ(r.client.testUnsentSubscribes(), 0u) << "never kept for retry";
    EXPECT_EQ(r.client.testIngressFatalDropped(), 1u);
    EXPECT_TRUE(r.gw.subscribed.empty());
    auto ok = subscribeT(r.client);
    ASSERT_TRUE(r.drive([&] { return ok.subscriptionId() > 0; })) << "later subscribes proceed";
}

// M1: an Unsubscribe the transport can never carry finishes locally — dropped,
// never kept pending / re-offered.
TEST(HubCommandClient, FatalUnsubscribeOfferFinishesLocallyRegression) {
    HubRig r(4020);
    r.connect();
    auto sub = subscribeT(r.client);
    ASSERT_TRUE(r.drive([&] { return sub.subscriptionId() > 0; }));
    r.settle(200);
    r.client.testForceIngressOffers(rc::transport::AeronTransport::OfferResult::Fatal, 1);
    sub.unsubscribe();  // owner: applied directly -> Fatal
    EXPECT_TRUE(sub.finished());
    EXPECT_EQ(r.client.testPendingUnsubscribes(), 0u) << "a Fatal Unsubscribe is never retried";
    auto barrier = r.pingOp(rc::Bytes(8, 1));  // FIFO barrier on the same publication
    ASSERT_TRUE(r.drive([&] { return r.gw.pings == 1; }));
    EXPECT_TRUE(r.gw.unsubscribed.empty());
    EXPECT_EQ(r.client.testIngressFatalDropped(), 1u);
}

// M2: Fatal drops are all counted but logged only on the first and every
// 1024th — a stream of them never floods stderr.
TEST(HubCommandClient, FatalDropLogIsRateLimitedRegression) {
    HubRig r(4022);
    r.connect();
    ASSERT_TRUE(r.drive([&] { return r.gw.metadataRequests > 0; }));
    r.settle(200);
    r.client.testForceIngressOffers(rc::transport::AeronTransport::OfferResult::Fatal, 3);
    testing::internal::CaptureStderr();
    auto p1 = r.pingOp(rc::Bytes(8, 1));
    auto p2 = r.pingOp(rc::Bytes(8, 2));
    auto p3 = r.pingOp(rc::Bytes(8, 3));
    const bool all = r.drive([&] { return r.client.testIngressFatalDropped() == 3; });
    const std::string err = testing::internal::GetCapturedStderr();
    ASSERT_TRUE(all);
    std::size_t lines = 0;
    for (std::size_t pos = 0; (pos = err.find("can never accept", pos)) != std::string::npos; ++pos) ++lines;
    EXPECT_EQ(lines, 1u) << err;
}

// M1: a Subscribe frame longer than the transport's max message length is
// rejected synchronously by subscribe() — it never reaches the duty thread.
TEST(HubCommandClient, OversizeSubscribeFrameFailsAtSubscribeRegression) {
    HubRig r(4024);
    r.connect();
    r.client.testCapMaxFrameLength(16);  // Invoker, single thread: below any Subscribe frame
    EXPECT_THROW((void)subscribeT(r.client), std::length_error);
    r.client.testCapMaxFrameLength(0);   // uncapped
    r.settle(200);
    EXPECT_TRUE(r.gw.subscribed.empty());
    auto ok = subscribeT(r.client);
    ASSERT_TRUE(r.drive([&] { return ok.subscriptionId() > 0; }));
}

// ===========================================================================
// F8c C5 — transactional producer on completion tokens (Client level)
// ===========================================================================
namespace {
const std::vector<uint8_t> kTxnVal{1, 2, 3};
}

// Client close completes EVERY outstanding txn token Closed, exactly once,
// whatever its phase (InitProducerId in flight, two-phase sendOffsets, an
// EndTxn waiting for its records): CQ targets before the queue is handed back
// (finished()), handler targets after.  Ops the transaction state machine
// refuses (abort / init while committing — fix round 1) complete at once,
// also exactly once.  Later ops are rejected synchronously.  Both runner modes.
TEST(HubCommandClient, TxnCloseCompletesAllClosedRegression) {
    {
        SCOPED_TRACE("Invoker");
        HubRig r(4016);
        auto tp = r.client.transactionalProducer("tx-close-all");
        r.connect();
        r.gw.initPidReply = true;
        auto init = r.cq.issue([&](rc::Completions& q, rc::Token t) { return tp.initTransactions(q, t); });
        ASSERT_TRUE(r.drive([&] { return init.ready(); }));
        ASSERT_TRUE(init.ok()) << init.describe();
        r.gw.initPidReply = false;  // from here the coordinator never answers
        tp.beginTransaction();
        tp.send(r.client.topic("t"), rc::Key(uint64_t{1}), std::span<const uint8_t>(kTxnVal));
        auto offs = r.cq.issue([&](rc::Completions& q, rc::Token t) {
            return tp.sendOffsetsToTransaction({{{1, 0}, 5}}, "g", q, t);
        });
        auto commit = r.cq.issue([&](rc::Completions& q, rc::Token t) { return tp.commitTransaction(q, t); });
        auto abort  = r.cq.issue([&](rc::Completions& q, rc::Token t) { return tp.abortTransaction(q, t); });
        auto init2  = r.cq.issue([&](rc::Completions& q, rc::Token t) { return tp.initTransactions(q, t); });
        // A handler target on a refused op: delivered once, at once.
        HandlerLedger led;
        ASSERT_EQ(tp.initTransactions(led.handler(), 99), rc::OpResult::Accepted);
        for (auto* op : {&offs, &commit, &abort, &init2}) ASSERT_TRUE(op->accepted()) << op->describe();
        ASSERT_TRUE(r.drive([&] { return r.gw.addPartitions >= 1 && r.gw.addOffsets >= 1; }))
            << "the requests never reached the coordinator";
        r.settle(200);
        for (auto* op : {&offs, &commit}) EXPECT_FALSE(op->ready()) << op->describe();
        for (auto* op : {&abort, &init2}) {  // refused by state (Committing): at once
            ASSERT_TRUE(op->ready());
            EXPECT_EQ(op->status(), rc::OpStatus::BrokerError);
        }
        EXPECT_EQ(led.times, 1);
        EXPECT_EQ(led.last.status, rc::OpStatus::BrokerError);

        r.client.closeImmediately();  // C7: the discarding close (close() aborts first)
        for (auto* op : {&offs, &commit}) {
            ASSERT_TRUE(op->ready()) << "stranded by close()";
            EXPECT_EQ(op->status(), rc::OpStatus::Closed);
            EXPECT_EQ(r.cq.timesCompleted(op->token()), 1);
        }
        for (auto* op : {&abort, &init2}) EXPECT_EQ(r.cq.timesCompleted(op->token()), 1);
        EXPECT_TRUE(r.cq.q().finished());
        EXPECT_EQ(led.times, 1);
        EXPECT_EQ(tp.commitTransaction(led.handler(), 100), rc::OpResult::Closed);
        EXPECT_EQ(led.times, 1);
        // A handler-target op still pending at close completes Closed (after the hand-back).
    }
    {
        SCOPED_TRACE("Invoker, handler pending at close");
        HubRig r(4022);
        auto tp = r.client.transactionalProducer("tx-close-handler");
        HandlerLedger led;
        ASSERT_EQ(tp.initTransactions(led.handler(), 5), rc::OpResult::Accepted);  // never connected
        r.settle(50);
        EXPECT_EQ(led.times, 0);
        r.client.closeImmediately();  // C7: the discarding close (close() aborts first)
        EXPECT_EQ(led.times, 1);
        EXPECT_EQ(led.last.status, rc::OpStatus::Closed);
    }
    {
        SCOPED_TRACE("Thread");
        rc::Client client(hubCfg(4018, 4019, rc::RunnerMode::Thread));
        test_support::TestCq cq(client);
        auto tp = client.transactionalProducer("tx-close-thread");
        auto init = cq.issue([&](rc::Completions& q, rc::Token t) { return tp.initTransactions(q, t); });
        ASSERT_TRUE(init.accepted()) << init.describe();
        EXPECT_FALSE(init.ready()) << "never connected: nothing answers it";
        client.closeImmediately();
        ASSERT_TRUE(init.ready());
        EXPECT_EQ(init.status(), rc::OpStatus::Closed);
        EXPECT_EQ(cq.timesCompleted(init.token()), 1);
        EXPECT_TRUE(cq.q().finished());
    }
}

// F14 end to end: InitProducerId answered NotCoordinator re-discovers the
// coordinator through a MetadataRequest, then retries; a following
// ConcurrentTransactions backs off and retries; then Ok.
TEST(HubCommandClient, TxnNotCoordinatorRediscoversThroughMetadataRegression) {
    HubRig r(4020);
    auto tp = r.client.transactionalProducer("tx-nc");
    r.connect();
    ASSERT_TRUE(r.drive([&] { return r.gw.metadataRequests >= 1; }));
    r.settle(200);
    const int mdBefore = r.gw.metadataRequests;
    r.gw.initPidReply = true;
    r.gw.initPidCodes = {static_cast<int16_t>(rune::protocol::client::ErrorCode::NotCoordinator),
                         static_cast<int16_t>(rune::protocol::client::ErrorCode::ConcurrentTransactions)};
    auto init = r.cq.issue([&](rc::Completions& q, rc::Token t) { return tp.initTransactions(q, t); });
    ASSERT_TRUE(r.drive([&] { return init.ready(); })) << "never retried after NotCoordinator";
    EXPECT_EQ(init.status(), rc::OpStatus::Ok) << init.describe();
    EXPECT_EQ(init.outcome().c.value, 4242);
    EXPECT_EQ(r.gw.initPids, 3) << "NotCoordinator -> re-discover -> retry -> 27 -> retry";
    EXPECT_EQ(r.gw.metadataRequests, mdBefore + 1) << "NotCoordinator must re-discover (one MetadataRequest)";
    EXPECT_EQ(tp.producerId(), 4242);
    EXPECT_EQ(r.cq.timesCompleted(init.token()), 1);
}
