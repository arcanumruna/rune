// tests/client/HubCommandClientTest.cpp — F8a: subscriber / group-consumer
// operations are commands on the agent ring, applied by the duty thread (or by
// close()'s final drain).  Client-level lifecycle regressions, both runner
// modes.  No sleeps: every wait is a bounded drive loop or an immediate check.
#include "../support/InvokerGateway.h"

#include <rune/client/Client.h>

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <future>
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

template <class F>
bool readyNow(F& f) { return f.wait_for(0s) == std::future_status::ready; }

rc::Subscription subscribeT(rc::Client& c) {
    return c.subscribe(c.topic("t"), /*partition=*/0, rc::SubscribeOptions{rc::From::earliest()},
                       [](const rc::Record&) {});
}

const char* modeName(rc::RunnerMode m) { return m == rc::RunnerMode::Invoker ? "Invoker" : "Thread"; }

} // namespace

// Commands still sitting in the agent ring when close() runs (Invoker: nothing
// ever drained them) are applied by close()'s final drain and then finished —
// a commitSync future fails, handles report inactive.  Never a hang.
TEST(HubCommandClient, CommandsStillInTheRingAreFinishedByCloseRegression) {
    int port = 3960;
    for (const rc::RunnerMode mode : {rc::RunnerMode::Invoker, rc::RunnerMode::Thread}) {
        SCOPED_TRACE(modeName(mode));
        rc::Client client(hubCfg(port, port + 1, mode));
        port += 2;
        auto sub = subscribeT(client);
        auto gc  = client.subscribeGroup("g", {"t"});
        auto f   = gc.commitSync();
        EXPECT_TRUE(sub.active());
        EXPECT_TRUE(gc.active());

        client.close();
        ASSERT_TRUE(readyNow(f)) << "commitSync must be failed by close(), not stranded";
        EXPECT_THROW(f.get(), std::runtime_error);
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
        auto f = gc.commitSync();
        ASSERT_TRUE(readyNow(f));
        EXPECT_THROW(f.get(), std::runtime_error);
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
    auto f = gc.commitSync();
    ASSERT_TRUE(readyNow(f));
    EXPECT_THROW(f.get(), std::runtime_error);
}

// A command committed to the ring AFTER close()'s final drain already ran (the
// exact window the post-submit re-check exists for) is reported as closed:
// the handle is finished and a commitSync future is failed — never stranded.
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
        auto f = gc.commitSync();
        ASSERT_TRUE(readyNow(f)) << "a commitSync racing close() must fail at once";
        EXPECT_THROW(f.get(), std::runtime_error);
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
        for (int i = 0; i < 200'000; ++i) {
            client.doWork();
            gw.pump();
            if (pred()) return true;
        }
        return false;
    };
    ASSERT_TRUE(drive([&] { return gw.ready(); }));
    auto f = client.connect();
    ASSERT_TRUE(drive([&] { return readyNow(f); }));
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
    bool drive(P&& pred, int cap = 200'000) {
        for (int i = 0; i < cap; ++i) {
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
    void connect() {
        ASSERT_TRUE(drive([&] { return gw.ready(); }));
        auto f = client.connect();
        ASSERT_TRUE(drive([&] { return readyNow(f); }));
        f.get();
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

// C1(b): the destructor under a full ring blocks only until the duty thread
// makes progress, then dispatch is stopped.
TEST(HubCommandClient, DestructorUnderFullRingStopsDispatchRegression) {
    HubRig r(3986);
    r.connect();
    std::atomic<int> got{0};
    auto sub = std::make_unique<rc::Subscription>(r.client.subscribe(
        r.client.topic("t"), 0, rc::SubscribeOptions{rc::From::earliest()},
        [&](const rc::Record&) { got.fetch_add(1); }));
    ASSERT_TRUE(r.drive([&] { return !r.gw.subscribed.empty() && sub->subscriptionId() > 0; }));

    std::atomic<int> ev{kPending};
    r.client.testSetHubCommandRingFullHook([&] { signal(ev, kFull); });
    r.client.testFillCommandRing();
    std::atomic<bool> destroyed{false};
    std::thread t([&] {
        sub.reset();
        destroyed.store(true);
        signal(ev, kNoFull);
    });
    if (waitEvent(ev) != kFull) {
        t.join();
        FAIL() << "the destructor never met the full ring";
    }
    ASSERT_TRUE(r.drive([&] { return destroyed.load() && !r.gw.unsubscribed.empty(); }));
    t.join();
    r.gw.pushRecords(r.gw.subscribed[0], 0, 2);
    r.settle();
    EXPECT_EQ(got.load(), 0) << "the destroyed subscription's handler must never run";
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

// I6: commitSync() from inside a duty-cycle callback throws instead of
// creating a future that can never resolve.
TEST(HubCommandClient, CommitSyncFromDutyCallbackThrowsRegression) {
    HubRig r(3990);
    auto gc = r.client.subscribeGroup("g", {"t"});
    bool threw = false;
    r.client.onConnected([&](int64_t) {
        try {
            (void)gc.commitSync();
        } catch (const std::logic_error&) {
            threw = true;
        }
    });
    r.connect();
    EXPECT_TRUE(threw);
    // Outside a callback, on the owner thread, it is allowed (the app pumps).
    EXPECT_NO_THROW((void)gc.commitSync());
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
    std::vector<std::future<rc::Bytes>> futs;  // pre-existing API (F8c converts it)
    for (int i = 0; i < kPings; ++i) futs.push_back(r.client.ping(rc::Bytes(1024, static_cast<uint8_t>(i))));
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
    std::vector<std::future<rc::Bytes>> futs;  // pre-existing API (F8c converts it)
    for (int i = 0; i < 120; ++i) futs.push_back(r.client.ping(rc::Bytes(1024, 1)));
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
    auto f = gc.commitSync();  // owner thread: applied directly; pending (not Stable)
    r.settle(100);
    ASSERT_FALSE(readyNow(f));
    ASSERT_EQ(r.client.state(), rc::session::SessionState::Connected);

    // The broker is gone: no more egress, no reconnect.  Advance the duty
    // clock past the session's response timeout and pump ONLY the client.
    g_hubNow += 11'000'000'000LL;
    bool down = false;
    for (int i = 0; i < 1'000 && !readyNow(f); ++i) {
        r.client.doWork();
        down = down || r.client.state() != rc::session::SessionState::Connected;
    }
    EXPECT_TRUE(down);
    ASSERT_TRUE(readyNow(f)) << "commitSync must fail on the session-down transition";
    EXPECT_THROW(f.get(), std::runtime_error);
}

// fix2 (found while writing the IM-2 test): on the Invoker owner a direct
// command must never overtake its own registration still queued in the ring.
TEST(HubCommandClient, DirectCommandNeverOvertakesItsOwnRegistrationRegression) {
    HubRig r(3998);
    r.connect();
    {
        auto gc = r.client.subscribeGroup("g", {"t"});
        auto f = gc.commitSync();  // before any doWork(): the add must already be applied
        r.settle(100);
        EXPECT_FALSE(readyNow(f)) << "commitSync failed as if the consumer did not exist";
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
