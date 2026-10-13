// tests/client/CompletionsClientTest.cpp — F8c C2 (the Client's completion-
// queue registry + the §3.3 release protocol) and C3 (control ops on
// completion tokens; duty-owned pending table; *AndWait; openTopicAndWait).
//
// Invoker-mode tests are single-threaded: the test thread is the owner, drives
// client.doWork() + gw.pump() with iteration caps and advances an injected
// clock instead of sleeping.  Thread-mode tests exercise the real runner (TSan:
// cross-thread traffic only through rings).
#include "../support/InvokerGateway.h"
#include "../support/TestCompletions.h"

#include <rune/client/Client.h>
#include <rune/client/Completions.h>
#include <rune/codec/Descriptor.h>
#include <rune/codec/Fingerprint.h>
#include <rune/codec/Serialize.h>

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <span>
#include <stdexcept>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

#if defined(__APPLE__)
#include <mach/mach.h>
#endif

namespace rc  = rune::client;
namespace rcs = rune::client::session;
using test_support::PendingOp;
using test_support::TestCq;

namespace {

std::atomic<int> g_ctr{0};
int64_t          g_now = 1'000'000'000;  // injected duty clock (test thread only)
int64_t fakeNow() noexcept { return g_now; }

rc::ClientConfig cfgFor(int32_t in, rc::RunnerMode mode = rc::RunnerMode::Invoker,
                        rc::NanoClock clock = nullptr) {
    rc::ClientConfig c;
    c.gatewayChannel  = "aeron:ipc";
    c.gatewayStreamId = in;
    c.egressChannel   = "aeron:ipc";
    c.egressStreamId  = in + 1;
    c.dataDir = "/tmp/cq_client_" + std::to_string(::getpid()) + "_" + std::to_string(g_ctr.fetch_add(1));
    c.runnerMode = mode;
    c.nanoClock  = clock;
    rc::TopicSpec ts;
    ts.name = "t";
    ts.topicId = 1;
    ts.partitionCount = 1;
    c.topics.push_back(ts);
    return c;
}

struct Rig {
    rc::ClientConfig             cfg;
    rc::Client                   client;  // constructed first: owns the driver
    test_support::InvokerGateway gw;      // destroyed first
    explicit Rig(rc::ClientConfig c)
        : cfg(std::move(c)), client(cfg),
          gw(cfg.dataDir + "/aeron", cfg.gatewayChannel, cfg.gatewayStreamId, cfg.egressChannel,
             cfg.egressStreamId) {}

    template <class P>
    bool drive(P&& pred, int cap = 200'000, int64_t stepNs = 0) {
        for (int i = 0; i < cap; ++i) {
            g_now += stepNs;
            client.doWork();
            gw.pump();
            if (pred()) return true;
        }
        return false;
    }
    void settle(int cycles = 2'000) {
        for (int i = 0; i < cycles; ++i) {
            client.doWork();
            gw.pump();
        }
    }
    void connect(TestCq& cq) {
        ASSERT_TRUE(drive([&] { return gw.ready(); }));
        auto f = cq.issue([&](rc::Completions& q, rc::Token t) { return client.connect(q, t); });
        ASSERT_TRUE(drive([&] { return f.ready(); }));
        ASSERT_TRUE(f.ok()) << f.describe();
    }
};

PendingOp pingOn(TestCq& cq, rc::Client& c, std::vector<uint8_t> payload) {
    return cq.issue([&](rc::Completions& q, rc::Token t) { return c.ping(payload, q, t); });
}

/// A CompletionHandler target that records every completion (duty thread;
/// in Invoker tests that is the test thread itself).
struct HandlerLog {
    int                  calls = 0;
    rc::Completion       last{};
    std::vector<uint8_t> payload;
    void on(const rc::Completion& c, std::span<const uint8_t> p) {
        ++calls;
        last = c;
        payload.assign(p.begin(), p.end());
    }
    rc::CompletionHandler handler() { return rc::CompletionHandler::of<&HandlerLog::on>(*this); }
};

// OS thread count of this process (cold, test-only).
int processThreadCount() {
#if defined(__APPLE__)
    thread_act_array_t     list  = nullptr;
    mach_msg_type_number_t count = 0;
    if (task_threads(mach_task_self(), &list, &count) != KERN_SUCCESS) return -1;
    for (mach_msg_type_number_t i = 0; i < count; ++i) mach_port_deallocate(mach_task_self(), list[i]);
    vm_deallocate(mach_task_self(), reinterpret_cast<vm_address_t>(list), count * sizeof(thread_act_t));
    return static_cast<int>(count);
#elif defined(__linux__)
    int n = 0;
    for ([[maybe_unused]] const auto& e : std::filesystem::directory_iterator("/proc/self/task")) ++n;
    return n;
#else
    return -1;
#endif
}

} // namespace

// ===========================================================================
// C2 — completion-queue registry + §3.3 release protocol
// ===========================================================================

// A registered Completions outlives its Client: close() completes the
// outstanding op Closed and hands the ring back (producer-close), so the queue
// is finished(), refuses new ops, and is destroyed without touching the
// destroyed Client.  Both runner modes (ASan: no use-after-free of the Client).
TEST(CompletionsClient, CompletionsOutliveClientSeeClosedRegression) {
    for (const rc::RunnerMode mode : {rc::RunnerMode::Invoker, rc::RunnerMode::Thread}) {
        SCOPED_TRACE(mode == rc::RunnerMode::Invoker ? "Invoker" : "Thread");
        auto client = std::make_unique<rc::Client>(cfgFor(mode == rc::RunnerMode::Invoker ? 5200 : 5202, mode));
        auto q = std::make_unique<rc::Completions>(*client);
        ASSERT_NE(q->cqRef(), rc::kNoCqRef) << "registered with the Client";
        ASSERT_EQ(client->connect(*q, 11), rc::OpResult::Accepted);  // no gateway: stays pending
        if (mode == rc::RunnerMode::Invoker) {
            client->doWork();
            EXPECT_EQ(client->testPendingControlOps(), 1u);
        }
        EXPECT_EQ(q->outstanding(), 1u);

        client.reset();  // close + destroy: the queue outlives its Client

        int n = 0;
        rc::Completion got{};
        q->poll([&](const rc::Completion& c, std::span<const uint8_t>) {
            ++n;
            got = c;
        });
        EXPECT_EQ(n, 1) << "exactly one completion for the outstanding op";
        EXPECT_EQ(got.token, 11u);
        EXPECT_EQ(got.status, rc::OpStatus::Closed);
        EXPECT_EQ(q->outstanding(), 0u);
        EXPECT_EQ(q->reservedBytes(), 0u) << "every credit returned";
        EXPECT_TRUE(q->finished()) << "handed back: producer-closed and drained";
        EXPECT_EQ(q->reserve(q->reservationFor(rc::OpKind::Ping)), rc::OpResult::Closed);
        q.reset();  // frees locally — never touches the destroyed Client
    }
}

// A completion addressed to a released (and reused) registry slot fails the
// generation check and is dropped — never delivered to the slot's new queue.
// End to end: an op in flight on queue A, A destroyed (CqRelease), B registered
// in the same slot, the op times out on the duty clock.
TEST(CompletionsClient, StaleCqRefIsDroppedByGenerationRegression) {
    g_now = 1'000'000'000;
    rc::ClientConfig c = cfgFor(5204, rc::RunnerMode::Invoker, &fakeNow);
    c.connectTimeoutMs = 50;
    rc::Client client(c);

    auto a = std::make_unique<rc::Completions>(client);
    const rc::CqRef refA = a->cqRef();
    ASSERT_EQ(client.connect(*a, 1), rc::OpResult::Accepted);  // no gateway: pending
    client.doWork();
    ASSERT_EQ(client.testPendingControlOps(), 1u);
    a.reset();         // Invoker owner between cycles: a CqRelease command (ring order)
    client.doWork();   // ... applied: unlinked (quarantined while the connect is pending)
    EXPECT_EQ(client.testRegisteredCqs(), 0u);

    rc::Completions b(client);
    const rc::CqRef refB = b.cqRef();
    ASSERT_NE(refB, refA) << "never the released (slot, generation)";

    g_now += 200'000'000;  // past connectTimeoutMs and a sweep interval
    for (int i = 0; i < 10 && client.testPendingControlOps() != 0; ++i) client.doWork();
    ASSERT_EQ(client.testPendingControlOps(), 0u) << "the connect timed out on the duty clock";
    EXPECT_EQ(client.testStaleCqDrops(), 1u) << "its completion was addressed to the stale ref";
    int n = 0;
    b.poll([&](const rc::Completion&, std::span<const uint8_t>) { ++n; });
    EXPECT_EQ(n, 0) << "a stale completion was misdelivered to the slot's new queue";

    // The live reference still delivers (duty-thread test seam).
    const uint32_t rec = b.reservationFor(rc::OpKind::Ping);
    ASSERT_EQ(b.reserve(rec), rc::OpResult::Accepted);
    client.testDeliverToCqRef(refB, 78, rec);
    rc::Token tok = 0;
    b.poll([&](const rc::Completion& done, std::span<const uint8_t>) { tok = done.token; });
    EXPECT_EQ(tok, 78u);
}

// The credit counters of a Completions are owner-thread state: issuing (or
// reserving) from another thread is refused with WrongThread, never accepted.
TEST(CompletionsClient, CompletionsUsedOffOwnerThreadIsWrongThreadRegression) {
    rc::Client client(cfgFor(5206, rc::RunnerMode::Thread));
    rc::Completions q(client);  // owner: this thread
    rc::OpResult viaClient = rc::OpResult::Accepted;
    rc::OpResult viaQueue  = rc::OpResult::Accepted;
    std::thread other([&] {
        viaClient = client.ping(std::vector<uint8_t>{1, 2}, q, 1);
#ifndef NDEBUG
        viaQueue = q.reserve(q.reservationFor(rc::OpKind::Ping));
#else
        viaQueue = rc::OpResult::WrongThread;  // the queue-level check is debug-only
#endif
    });
    other.join();
    EXPECT_EQ(viaClient, rc::OpResult::WrongThread);
    EXPECT_EQ(viaQueue, rc::OpResult::WrongThread);
    EXPECT_EQ(q.outstanding(), 0u);
    EXPECT_EQ(q.reservedBytes(), 0u);
    // Invoker mode: a Completions can only be registered on the owner thread.
    rc::Client inv(cfgFor(5208, rc::RunnerMode::Invoker));
    std::thread offOwner([&] { EXPECT_THROW(rc::Completions bad(inv), std::logic_error); });
    offOwner.join();
}

// §3.3 release racing Client close, many times over: K app threads destroy
// their registered queues (each with an op outstanding) while the test thread
// closes the Client.  Every ring is freed EXACTLY once — by the duty side
// (release accepted before the close), or by its owner (after close handed it
// back) — never leaked, never double-freed (ASan), no data race (TSan).
namespace {
std::atomic<int> g_ringsFreed{0};
void countRingFree(const rc::detail::CompletionRing*) noexcept { g_ringsFreed.fetch_add(1); }
} // namespace

TEST(CompletionsClient, CqReleaseRacingCloseNeverLeaksNorDoubleFreesRegression) {
    constexpr int kIterations = 40;
    constexpr int kQueues     = 6;
    rc::detail::CompletionRing::testOnDestroy = &countRingFree;  // no thread running yet
    for (int it = 0; it < kIterations; ++it) {
        g_ringsFreed.store(0);
        auto client = std::make_unique<rc::Client>(cfgFor(5210, rc::RunnerMode::Thread));
        std::atomic<int>  ready{0};
        std::atomic<bool> go{false};
        std::atomic<int>  accepted{0};
        std::vector<std::thread> ts;
        for (int k = 0; k < kQueues; ++k) {
            ts.emplace_back([&, k] {
                auto q = std::make_unique<rc::Completions>(*client);
                if (client->connect(*q, static_cast<rc::Token>(k + 1)) == rc::OpResult::Accepted) {
                    accepted.fetch_add(1);
                }
                ready.fetch_add(1);
                while (!go.load()) std::this_thread::yield();
                if ((k + it) % 3 == 0) std::this_thread::yield();  // vary the interleaving
                q.reset();  // §3.3 release, racing close()
            });
        }
        while (ready.load() != kQueues) std::this_thread::yield();
        go.store(true);
        if (it % 2 == 0) std::this_thread::yield();
        client->closeImmediately();  // C7: the teardown races the releases (close() adds its own queue)
        for (auto& t : ts) t.join();
        EXPECT_EQ(accepted.load(), kQueues);
        EXPECT_EQ(client->testRegisteredCqs(), 0u) << "iteration " << it;
        client.reset();
        ASSERT_EQ(g_ringsFreed.load(), kQueues) << "iteration " << it << ": a ring leaked or was freed twice";
    }
    rc::detail::CompletionRing::testOnDestroy = nullptr;
}

// ===========================================================================
// C3 — control ops on completion tokens
// ===========================================================================

// connect() completes exactly once, both into a Completions and to a
// CompletionHandler; onConnected still fires; connect while Connected is Ok.
TEST(CompletionsClient, ConnectCompletesOnceViaCqAndHandlerRegression) {
    Rig r(cfgFor(5220));
    int onConnected = 0;
    r.client.onConnected([&](int64_t) { ++onConnected; });
    TestCq cq(r.client);
    ASSERT_TRUE(r.drive([&] { return r.gw.ready(); }));
    auto f = cq.issue([&](rc::Completions& q, rc::Token t) { return r.client.connect(q, t); });
    ASSERT_TRUE(f.accepted());
    ASSERT_TRUE(r.drive([&] { return f.ready(); }));
    EXPECT_EQ(f.status(), rc::OpStatus::Ok);
    EXPECT_EQ(f.outcome().c.value, 42) << "value = the assigned sessionId";
    EXPECT_EQ(f.outcome().c.op, rc::OpKind::Connect);
    r.settle();
    EXPECT_EQ(cq.timesCompleted(f.token()), 1);
    EXPECT_EQ(onConnected, 1);
    EXPECT_EQ(r.client.state(), rcs::SessionState::Connected);

    // Handler target: called on the duty thread (here, inside doWork()).
    HandlerLog log;
    ASSERT_EQ(r.client.connect(log.handler(), 9), rc::OpResult::Accepted);
    ASSERT_TRUE(r.drive([&] { return log.calls != 0; }));
    r.settle();
    EXPECT_EQ(log.calls, 1);
    EXPECT_EQ(log.last.token, 9u);
    EXPECT_EQ(log.last.status, rc::OpStatus::Ok) << "already Connected: completes Ok at once";
    EXPECT_EQ(log.last.value, 42);
    EXPECT_EQ(r.client.testPendingControlOps(), 0u);
    EXPECT_EQ(cq.timesCompleted(f.token()), 1) << "the first connect completed again";
}

// The Pong's echo is the completion's inline payload — binary bytes, the empty
// echo, and many sizes across a small queue that wraps many times.
TEST(CompletionsClient, PingEchoPayloadDeliveredInlineRegression) {
    Rig r(cfgFor(5222));
    r.gw.replyPongs = true;
    TestCq cq(r.client);
    r.connect(cq);

    const std::vector<uint8_t> binary{0x00, 0xFF, 'a', 0x00, 0x7F};
    auto b = pingOn(cq, r.client, binary);
    auto e = pingOn(cq, r.client, {});
    ASSERT_TRUE(r.drive([&] { return b.ready() && e.ready(); }));
    EXPECT_EQ(b.echo(), binary);
    EXPECT_TRUE(e.echo().empty());
    EXPECT_EQ(e.outcome().c.payloadLen, 0u);

    // A 4 KiB queue: records of up to ~350 bytes wrap it many times.
    rc::CompletionsConfig small;
    small.capacityBytes = 4096;
    small.maxResultBytes = 300;
    small.maxErrorText   = 64;
    rc::Completions q(r.client, small);
    for (int i = 0; i < 200; ++i) {
        std::vector<uint8_t> payload(static_cast<std::size_t>((i * 37) % 300));
        for (std::size_t j = 0; j < payload.size(); ++j) payload[j] = static_cast<uint8_t>(i + j);
        const rc::Token t = 1000 + static_cast<rc::Token>(i);
        ASSERT_EQ(r.client.ping(payload, q, t), rc::OpResult::Accepted) << i;
        std::vector<uint8_t> got;
        bool done = false;
        ASSERT_TRUE(r.drive([&] {
            q.poll([&](const rc::Completion& c, std::span<const uint8_t> p) {
                ASSERT_EQ(c.token, t);
                ASSERT_EQ(c.status, rc::OpStatus::Ok);
                got.assign(p.begin(), p.end());
                done = true;
            });
            return done;
        })) << i;
        ASSERT_EQ(got, payload) << "echo " << i << " corrupted across the ring wrap";
    }
    EXPECT_EQ(q.outstanding(), 0u);
    EXPECT_EQ(q.ring().lostCompletions(), 0u);

    // Handler target: the payload span is the echo.
    HandlerLog log;
    ASSERT_EQ(r.client.ping(binary, log.handler(), 5), rc::OpResult::Accepted);
    ASSERT_TRUE(r.drive([&] { return log.calls == 1; }));
    EXPECT_EQ(log.payload, binary);
}

// An op's own deadline is on the DUTY clock: with the injected clock frozen it
// never times out however many cycles run; stepped past the deadline it
// completes Timeout exactly once.  No sleeps.
TEST(CompletionsClient, ControlOpTimeoutCompletesTimeoutRegression) {
    g_now = 1'000'000'000;
    rc::ClientConfig c = cfgFor(5224, rc::RunnerMode::Invoker, &fakeNow);
    c.pingTimeoutMs    = 50;
    c.ddlTimeoutMs     = 50;   // catalog history: a DDL's own deadline (DefineTopic below)
    c.connectTimeoutMs = 30'000;
    Rig r(c);
    TestCq cq(r.client);
    r.connect(cq);  // replies are immediate: no clock movement needed

    std::vector<uint8_t> blob{1, 2, 3};
    auto p = pingOn(cq, r.client, {7});  // the gateway never answers pings / DDL here
    auto d = cq.issue([&](rc::Completions& q, rc::Token t) {
        return r.client.defineTopic("x", 1, "", false, blob, q, t);
    });
    auto g = cq.issue([&](rc::Completions& q, rc::Token t) { return r.client.getTopicSchema("x", q, t); });
    r.settle(5'000);  // frozen clock: nothing may time out
    EXPECT_FALSE(p.ready());
    EXPECT_FALSE(d.ready());
    EXPECT_FALSE(g.ready());
    EXPECT_EQ(r.client.testPendingControlOps(), 3u);

    g_now += 200'000'000;  // past pingTimeoutMs and the 100 ms sweep cadence
    ASSERT_TRUE(r.drive([&] { return p.ready() && d.ready() && g.ready(); }, 100));
    EXPECT_EQ(p.status(), rc::OpStatus::Timeout);
    EXPECT_EQ(d.status(), rc::OpStatus::Timeout);
    EXPECT_EQ(g.status(), rc::OpStatus::Timeout);
    r.settle();
    EXPECT_EQ(cq.timesCompleted(p.token()), 1);
    EXPECT_EQ(cq.timesCompleted(d.token()), 1);
    EXPECT_EQ(cq.timesCompleted(g.token()), 1);
    EXPECT_EQ(r.client.testPendingControlOps(), 0u);

    // connect: same rule (its own connectTimeoutMs).
    rc::ClientConfig c2 = cfgFor(5226, rc::RunnerMode::Invoker, &fakeNow);
    c2.connectTimeoutMs = 50;
    Rig r2(c2);
    r2.gw.replyConnects = false;
    TestCq cq2(r2.client);
    ASSERT_TRUE(r2.drive([&] { return r2.gw.ready(); }));
    auto k = cq2.issue([&](rc::Completions& q, rc::Token t) { return r2.client.connect(q, t); });
    r2.settle(5'000);
    EXPECT_FALSE(k.ready());
    g_now += 200'000'000;
    ASSERT_TRUE(r2.drive([&] { return k.ready(); }, 100));
    EXPECT_EQ(k.status(), rc::OpStatus::Timeout);
    EXPECT_EQ(cq2.timesCompleted(k.token()), 1);
}

// A session-fatal ErrorResponse (SessionExpired) completes EVERY pending control
// op of the session SessionLost — exactly once each, promptly.
TEST(CompletionsClient, SessionFatalErrorCompletesAllPendingSessionLostRegression) {
    Rig r(cfgFor(5228));
    TestCq cq(r.client);
    r.connect(cq);
    std::vector<uint8_t> blob{9};
    auto p1 = pingOn(cq, r.client, {1});
    auto p2 = pingOn(cq, r.client, {2});
    auto d  = cq.issue([&](rc::Completions& q, rc::Token t) {
        return r.client.defineTopic("y", 1, "", false, blob, q, t);
    });
    auto g  = cq.issue([&](rc::Completions& q, rc::Token t) { return r.client.getTopicSchema("y", q, t); });
    ASSERT_TRUE(r.drive([&] { return r.gw.pings == 2 && r.gw.defines == 1 && r.gw.schemaGets == 1; }));
    ASSERT_EQ(r.client.testPendingControlOps(), 4u);

    r.gw.pingErrorCode = static_cast<int16_t>(rune::client::codec::ErrorCode::SessionExpired);
    auto p3 = pingOn(cq, r.client, {3});
    ASSERT_TRUE(r.drive([&] { return p1.ready() && p2.ready() && p3.ready() && d.ready() && g.ready(); }));
    for (const PendingOp* op : {&p1, &p2, &p3, &d, &g}) {
        EXPECT_EQ(op->status(), rc::OpStatus::SessionLost) << op->describe();
        EXPECT_EQ(op->outcome().c.errorCode,
                  static_cast<uint16_t>(rune::client::codec::ErrorCode::SessionExpired));
    }
    r.settle();
    for (const PendingOp* op : {&p1, &p2, &p3, &d, &g}) EXPECT_EQ(cq.timesCompleted(op->token()), 1);
    EXPECT_EQ(r.client.testPendingControlOps(), 0u);
}

// No-hang fix round 2: the broker answers a Ping it could not decode with
// ErrorResponse{InvalidRequest (39)} on the ping's correlationId — the ping
// completes BrokerError with that code at once; other pending ops are untouched.
TEST(CompletionsClient, PingInvalidRequestErrorCompletesThatPingRegression) {
    Rig r(cfgFor(5450));
    TestCq cq(r.client);
    r.connect(cq);
    std::vector<uint8_t> blob{9};
    auto d = cq.issue([&](rc::Completions& q, rc::Token t) {
        return r.client.defineTopic("z", 1, "", false, blob, q, t);
    });
    ASSERT_TRUE(r.drive([&] { return r.gw.defines == 1; }));
    r.gw.pingErrorCode = static_cast<int16_t>(rune::client::codec::ErrorCode::InvalidRequest);
    auto p = pingOn(cq, r.client, {1});
    ASSERT_TRUE(r.drive([&] { return p.ready(); }));
    EXPECT_EQ(p.status(), rc::OpStatus::BrokerError);
    EXPECT_EQ(p.outcome().c.errorCode, static_cast<uint16_t>(rune::client::codec::ErrorCode::InvalidRequest));
    r.settle();
    EXPECT_FALSE(d.ready()) << "a non-fatal error answering another request completed this op";
}

// No-hang fix round 2: an ErrorResponse answering ANOTHER request no longer
// fails a pending connect (it used to: any ErrorResponse completed the connect
// BrokerError); only the connect's own correlationId or a session-fatal one does.
TEST(CompletionsClient, ForeignErrorResponseDoesNotFailAPendingConnectRegression) {
    Rig r(cfgFor(5452));
    r.gw.replyConnects = false;
    TestCq cq(r.client);
    ASSERT_TRUE(r.drive([&] { return r.gw.ready(); }));
    auto c = cq.issue([&](rc::Completions& q, rc::Token t) { return r.client.connect(q, t); });
    ASSERT_TRUE(r.drive([&] { return r.gw.connects >= 1; }));
    ASSERT_FALSE(r.gw.connectCorrIds.empty());
    r.gw.sendError(0, static_cast<int16_t>(rune::client::codec::ErrorCode::InvalidRequest), "", 0,
                   r.gw.connectCorrIds.back() + 777);
    r.settle();
    EXPECT_FALSE(c.ready()) << "an error answering another request failed the pending connect";
    r.gw.sendError(0, static_cast<int16_t>(rune::client::codec::ErrorCode::InvalidRequest), "", 0,
                   r.gw.connectCorrIds.back());
    ASSERT_TRUE(r.drive([&] { return c.ready(); }));
    EXPECT_EQ(c.status(), rc::OpStatus::BrokerError);
}

// A second connect() while one is pending completes the older one Superseded
// (it used to throw "connect() called while another connect is pending" into
// the old future); the newer one completes exactly once (here: Closed).
TEST(CompletionsClient, SecondConnectSupersedesFirstRegression) {
    Rig r(cfgFor(5230));
    r.gw.replyConnects = false;
    TestCq cq(r.client);
    ASSERT_TRUE(r.drive([&] { return r.gw.ready(); }));
    auto first = cq.issue([&](rc::Completions& q, rc::Token t) { return r.client.connect(q, t); });
    ASSERT_TRUE(r.drive([&] { return r.gw.connects >= 1; }));
    auto second = cq.issue([&](rc::Completions& q, rc::Token t) { return r.client.connect(q, t); });
    ASSERT_TRUE(r.drive([&] { return first.ready(); }, 100));
    EXPECT_EQ(first.status(), rc::OpStatus::Superseded);
    EXPECT_FALSE(second.ready());
    EXPECT_EQ(r.client.testPendingControlOps(), 1u);
    r.client.close();
    ASSERT_TRUE(second.ready());
    EXPECT_EQ(second.status(), rc::OpStatus::Closed);
    EXPECT_EQ(cq.timesCompleted(first.token()), 1);
    EXPECT_EQ(cq.timesCompleted(second.token()), 1);
}

// close() completes every outstanding control token Closed — pending on the
// duty side or still in the command ring, queue or handler target — exactly
// once; then the queue is finished.  Both runner modes.
TEST(CompletionsClient, CloseCompletesEveryControlTokenClosedRegression) {
    {
        SCOPED_TRACE("Invoker");
        Rig r(cfgFor(5232));
        TestCq cq(r.client);
        r.connect(cq);
        std::vector<uint8_t> blob{4};
        auto p = pingOn(cq, r.client, {1});
        auto d = cq.issue([&](rc::Completions& q, rc::Token t) {
            return r.client.defineTopic("z", 1, "", false, blob, q, t);
        });
        HandlerLog log;
        ASSERT_EQ(r.client.ping(std::vector<uint8_t>{2}, log.handler(), 77), rc::OpResult::Accepted);
        ASSERT_TRUE(r.drive([&] { return r.gw.pings == 2 && r.gw.defines == 1; }));
        // Issued AFTER the last duty cycle: still in the command ring at close.
        auto g    = cq.issue([&](rc::Completions& q, rc::Token t) { return r.client.getTopicSchema("z", q, t); });
        auto ring = pingOn(cq, r.client, {3});
        HandlerLog ringLog;
        ASSERT_EQ(r.client.listTopics(ringLog.handler(), 78), rc::OpResult::Accepted);
        ASSERT_EQ(r.client.testPendingControlOps(), 3u);

        r.client.closeImmediately();  // F8c C7: close() would first pump the queued ops
        for (const PendingOp* op : {&p, &d, &g, &ring}) {
            ASSERT_TRUE(op->ready()) << "not completed by close()";
            EXPECT_EQ(op->status(), rc::OpStatus::Closed) << op->describe();
            EXPECT_EQ(cq.timesCompleted(op->token()), 1);
        }
        EXPECT_EQ(log.calls, 1);
        EXPECT_EQ(log.last.status, rc::OpStatus::Closed);
        EXPECT_EQ(ringLog.calls, 1);
        EXPECT_EQ(ringLog.last.status, rc::OpStatus::Closed);
        EXPECT_TRUE(cq.q().finished());
        EXPECT_EQ(r.client.ping(std::vector<uint8_t>{1}, cq.q(), 99), rc::OpResult::Closed)
            << "an op issued after close is rejected synchronously";
    }
    {
        SCOPED_TRACE("Thread");
        rc::Client client(cfgFor(5234, rc::RunnerMode::Thread));  // no gateway
        TestCq cq(client);
        auto k  = cq.issue([&](rc::Completions& q, rc::Token t) { return client.connect(q, t); });
        auto k2 = cq.issue([&](rc::Completions& q, rc::Token t) { return client.connect(q, t); });
        auto l  = cq.issue([&](rc::Completions& q, rc::Token t) { return client.listTopics(q, t); });
        ASSERT_TRUE(l.waitReady()) << "listTopics needs no broker";
        client.closeImmediately();
        ASSERT_TRUE(k.ready());
        ASSERT_TRUE(k2.ready());
        EXPECT_EQ(k.status(), rc::OpStatus::Superseded);
        EXPECT_EQ(k2.status(), rc::OpStatus::Closed);
        EXPECT_EQ(cq.timesCompleted(k.token()), 1);
        EXPECT_EQ(cq.timesCompleted(k2.token()), 1);
        EXPECT_TRUE(cq.q().finished());
    }
}

namespace {
test_support::InvokerGateway* s_gwSchema = nullptr;  // test thread only
} // namespace

// The polled form of getTopicSchema reserves maxResultBytes: a larger
// descriptor completes ResultTooLarge (value = its real size, no payload, the
// credit returned); the callback form and getTopicSchemaAndWait get it whole.
TEST(CompletionsClient, SchemaLargerThanReservationIsResultTooLargeRegression) {
    Rig r(cfgFor(5236));
    r.gw.replySchemas = true;
    r.gw.schemaBlob.assign(1000, 0x5A);
    TestCq cq(r.client);
    r.connect(cq);

    rc::CompletionsConfig small;
    small.capacityBytes  = 4096;
    small.maxResultBytes = 256;
    small.maxErrorText   = 64;
    rc::Completions q(r.client, small);
    ASSERT_EQ(r.client.getTopicSchema("big", q, 3), rc::OpResult::Accepted);
    rc::Completion got{};
    std::size_t    payloadLen = 99;
    ASSERT_TRUE(r.drive([&] {
        return q.poll([&](const rc::Completion& c, std::span<const uint8_t> p) {
                   got        = c;
                   payloadLen = p.size();
               }) != 0;
    }));
    EXPECT_EQ(got.status, rc::OpStatus::ResultTooLarge);
    EXPECT_EQ(got.value, 1000) << "value = the real size";
    EXPECT_EQ(payloadLen, 0u);
    EXPECT_EQ(q.outstanding(), 0u);
    EXPECT_EQ(q.reservedBytes(), 0u);
    EXPECT_EQ(q.ring().lostCompletions(), 0u);

    HandlerLog log;
    ASSERT_EQ(r.client.getTopicSchema("big", log.handler(), 4), rc::OpResult::Accepted);
    ASSERT_TRUE(r.drive([&] { return log.calls == 1; }));
    EXPECT_EQ(log.last.status, rc::OpStatus::Ok);
    EXPECT_EQ(log.payload.size(), 1000u) << "the callback form has no cap";

    // The wait pumps only the Client: the duty-cycle hook pumps the gateway.
    s_gwSchema = &r.gw;
    r.client.testSetDutyCycleHook([] { s_gwSchema->pump(); });
    const rc::TopicSchemaResult whole = r.client.getTopicSchemaAndWait("big");
    r.client.testSetDutyCycleHook(nullptr);
    s_gwSchema = nullptr;
    EXPECT_EQ(whole.errorCode, 0u);
    EXPECT_EQ(whole.descriptorBlob.size(), 1000u);
    EXPECT_EQ(whole.topicId, 7);
}

// openTopicAndWait<T> runs the fingerprint gate on the calling thread: no
// thread is ever spawned (the old openTopic<T> continuation was std::async).
namespace {
rune::codec::Descriptor openTopicDescriptor() {
    return rune::codec::DescriptorBuilder("characters", 1)
        .add("id", rune::codec::PrimType::Int64)
        .add("userId", rune::codec::PrimType::Int64, 1, /*isKey=*/true)
        .build();
}
struct OpenStub {
    static const uint64_t kFingerprint;
    static constexpr std::string_view kSchemaName = "characters";
    std::size_t encodedSize() const noexcept { return 1; }
    std::size_t encode(uint8_t* b) const noexcept {
        b[0] = 1;
        return 1;
    }
};
const uint64_t OpenStub::kFingerprint = rune::codec::fingerprint(openTopicDescriptor());
struct StaleStub : OpenStub {
    static const uint64_t kFingerprint;
};
const uint64_t StaleStub::kFingerprint = OpenStub::kFingerprint ^ 1;
int g_maxThreadsSeen = 0;  // test thread only (the Invoker duty cycle runs on it)
test_support::InvokerGateway* s_gw = nullptr;  // test thread only
void sampleThreads() {
    const int n = processThreadCount();
    if (n > g_maxThreadsSeen) g_maxThreadsSeen = n;
}
} // namespace

TEST(CompletionsClient, OpenTopicSpawnsNoThreadRegression) {
    Rig r(cfgFor(5238));
    r.gw.replySchemas = true;
    r.gw.schemaBlob   = rune::codec::serialize(openTopicDescriptor());
    r.gw.schemaTopicId = 9;
    TestCq cq(r.client);
    r.connect(cq);
    const int before = processThreadCount();
    ASSERT_GT(before, 0);
    g_maxThreadsSeen = before;
    // The wait pumps only the Client (doWork); the hook samples the thread
    // count in every duty cycle and pumps the gateway (another invoker Aeron
    // client on this same thread) so its reply can arrive.
    s_gw = &r.gw;
    r.client.testSetDutyCycleHook([] {
        sampleThreads();
        s_gw->pump();
    });
    auto topic = r.client.openTopicAndWait<OpenStub>("characters");
    EXPECT_EQ(g_maxThreadsSeen, before) << "openTopicAndWait spawned a thread";
    EXPECT_EQ(processThreadCount(), before);
    EXPECT_EQ(r.gw.schemaGets, 1);
    (void)topic;

    // The stale-type gate still throws — on this thread.
    EXPECT_THROW((void)r.client.openTopicAndWait<StaleStub>("characters"), std::runtime_error);
    EXPECT_EQ(g_maxThreadsSeen, before);
    r.client.testSetDutyCycleHook(nullptr);
    s_gw = nullptr;
}

// ===========================================================================
// C3 — the remaining Client atomics are gone (duty-owned state + rings)
// ===========================================================================
namespace {
/// Thread-mode rig: the Client's runner is a real thread; the gateway is an
/// invoker Aeron client pumped by the test thread.
struct ThreadRig {
    rc::ClientConfig             cfg;
    rc::Client                   client;
    test_support::InvokerGateway gw;
    explicit ThreadRig(int32_t in)
        : cfg(cfgFor(in, rc::RunnerMode::Thread)), client(cfg),
          gw(cfg.dataDir + "/aeron", cfg.gatewayChannel, cfg.gatewayStreamId, cfg.egressChannel,
             cfg.egressStreamId) {}
    template <class P>
    bool pump(P&& pred, int cap = 5'000'000) {
        for (int i = 0; i < cap; ++i) {
            gw.pump();
            if (pred()) return true;
            std::this_thread::yield();  // give the runner the CPU on a small box
        }
        return false;
    }
};
} // namespace

// Client::state() on the owner thread reads what the duty thread PUBLISHED into
// a StatusRing (ClientAgent::snapshot_, an atomic, is gone): Connected is
// visible once the connect completion is (published first, in ring order on
// the duty thread).  Any other thread is refused.  TSan: no data race.
TEST(CompletionsClient, StateIsPublishedToTheOwnerThroughARingRegression) {
    ThreadRig r(5240);
    TestCq cq(r.client);
    EXPECT_EQ(r.client.state(), rcs::SessionState::Disconnected);
    ASSERT_TRUE(r.pump([&] { return r.gw.ready(); }));
    auto f = cq.issue([&](rc::Completions& q, rc::Token t) { return r.client.connect(q, t); });
    // Read state() WHILE the runner drives the transition (TSan: a direct read
    // of the duty-owned session machine here is a data race).
    ASSERT_TRUE(r.pump([&] { return r.client.state() == rcs::SessionState::Connected && f.ready(); }));
    ASSERT_TRUE(f.ok()) << f.describe();
    EXPECT_EQ(r.client.state(), rcs::SessionState::Connected)
        << "the status must be published before the connect completion";
    bool threw = false;
    std::thread other([&] {
        try {
            (void)r.client.state();
        } catch (const std::logic_error&) {
            threw = true;
        }
    });
    other.join();
    EXPECT_TRUE(threw) << "state() off the owner / duty thread must be refused";
    r.client.close();
}

// A callback setter is a command for the duty thread (connectCalled_, an
// atomic, is gone): set from the app thread while the runner is live and
// Connected, it is installed in ring order and fires — no race (TSan), no
// "set after connect" refusal.
TEST(CompletionsClient, CallbackSetterIsARingCommandRegression) {
    ThreadRig r(5242);
    TestCq cq(r.client);
    ASSERT_TRUE(r.pump([&] { return r.gw.ready(); }));
    auto f = cq.issue([&](rc::Completions& q, rc::Token t) { return r.client.connect(q, t); });
    ASSERT_TRUE(r.pump([&] { return f.ready(); }));
    ASSERT_TRUE(f.ok()) << f.describe();

    std::atomic<int> errors{0};
    r.gw.pingErrorCode = 99;  // a non-fatal ErrorResponse for every ping
    // Replace onError (after connect) while the runner is invoking it for
    // earlier errors: every replacement is a ring command, applied between
    // invocations (TSan: a direct write here races the runner's call).
    std::vector<PendingOp> earlier;
    for (int i = 0; i < 20; ++i) {
        earlier.push_back(cq.issue([&](rc::Completions& q, rc::Token t) {
            return r.client.ping(std::vector<uint8_t>{1}, q, t);
        }));
        r.client.onError([&](const std::string&) { errors.fetch_add(1); });
        r.gw.pump();
    }
    ASSERT_TRUE(r.pump([&] {
        for (const auto& e : earlier) if (!e.ready()) return false;
        return true;
    }));
    r.client.onError([&](const std::string&) { errors.fetch_add(1000); });
    auto p = cq.issue([&](rc::Completions& q, rc::Token t) {
        return r.client.ping(std::vector<uint8_t>{1}, q, t);
    });
    ASSERT_TRUE(r.pump([&] { return p.ready(); }));
    EXPECT_EQ(p.status(), rc::OpStatus::BrokerError);
    EXPECT_EQ(p.outcome().c.errorCode, 99u);
    const std::vector<uint8_t> text = p.bytes();
    EXPECT_EQ(std::string(text.begin(), text.end()), "ping refused");
    r.client.close();  // joins the runner: every onError call happened before
    EXPECT_GE(errors.load(), 1000) << "the callback set after connect() was not installed";
    EXPECT_LT(errors.load(), 2000) << "the last callback ran once";
}

// Correlation ids come from the duty-owned counter (nextCorrId_, an atomic, is
// gone): a Subscribe from an app thread gets its id when the duty thread adopts
// it, shared with the control ops — distinct and non-zero on the wire.
TEST(CompletionsClient, SubscribeCorrelationIdIsDutyAssignedRegression) {
    ThreadRig r(5244);
    TestCq cq(r.client);
    ASSERT_TRUE(r.pump([&] { return r.gw.ready(); }));
    auto f = cq.issue([&](rc::Completions& q, rc::Token t) { return r.client.connect(q, t); });
    ASSERT_TRUE(r.pump([&] { return f.ready(); }));
    ASSERT_TRUE(f.ok()) << f.describe();
    std::unique_ptr<rc::Subscription> s1;
    std::unique_ptr<rc::Subscription> s2;
    std::thread other([&] {  // a second app thread subscribes concurrently
        s2 = std::make_unique<rc::Subscription>(r.client.subscribe(
            r.client.topic("t"), 0, rc::SubscribeOptions{rc::From::earliest()}, [](const rc::Record&) {}));
    });
    s1 = std::make_unique<rc::Subscription>(r.client.subscribe(
        r.client.topic("t"), 0, rc::SubscribeOptions{rc::From::earliest()}, [](const rc::Record&) {}));
    other.join();
    auto p = cq.issue([&](rc::Completions& q, rc::Token t) {
        return r.client.ping(std::vector<uint8_t>{1}, q, t);
    });
    ASSERT_TRUE(r.pump([&] { return r.gw.subscribeCorrIds.size() == 2 && r.gw.pings == 1; }));
    ASSERT_TRUE(r.pump([&] { return s1->subscriptionId() > 0 && s2->subscriptionId() > 0; }))
        << "both Subscribes must bind (distinct correlation ids)";
    const int64_t a = r.gw.subscribeCorrIds[0], b = r.gw.subscribeCorrIds[1], c = r.gw.pingCorrIds[0];
    EXPECT_NE(a, 0);
    EXPECT_NE(b, 0);
    EXPECT_NE(a, b);
    EXPECT_NE(c, a);
    EXPECT_NE(c, b);
    s1.reset();
    s2.reset();
    r.client.close();
}

// ===========================================================================
// F8c C2+C3 fix round 1
// ===========================================================================
namespace {
/// Hang guard: a deadlock FAILS the test (the process exits non-zero with a
/// message) instead of hanging CI.  The watchdog's own wait is a backstop only.
struct Watchdog {
    std::atomic<bool> done{false};
    std::thread       t;
    explicit Watchdog(const char* what, int seconds = 30)
        : t([this, what, seconds] {
              for (int i = 0; i < seconds * 100 && !done.load(); ++i) {
                  std::this_thread::sleep_for(std::chrono::milliseconds(10));
              }
              if (!done.load()) {
                  std::fprintf(stderr, "[  FAILED  ] watchdog: %s did not return within %d s (deadlock)\n",
                               what, seconds);
                  std::fflush(stderr);
                  std::_Exit(3);
              }
          }) {}
    ~Watchdog() {
        done.store(true);
        t.join();
    }
};

std::atomic<bool> g_parkRunner{false};
std::atomic<bool> g_runnerParked{false};
void parkRunnerHook() {
    if (!g_parkRunner.load()) return;
    g_runnerParked.store(true);
    while (g_parkRunner.load()) std::this_thread::yield();
}
void noopHandler(void*, const rc::Completion&, std::span<const uint8_t>) noexcept {}
} // namespace

// I1: a registered Completions whose last owner is a user callable replaced
// while its setter command is still in the command ring at close(): the final
// drain (on the CLOSING thread) destroys the callable and so the queue.  Its
// release must not wait for a hand-back that only this same thread would do.
TEST(CompletionsClient, QueueReleasedOnTheClosingThreadDuringTheDrainRegression) {
    Watchdog wd("close() with a queue released inside its own final drain");
    rc::Client client(cfgFor(5250, rc::RunnerMode::Thread));
    auto cq = std::make_shared<rc::Completions>(client);  // owner: this thread
    g_parkRunner.store(true);
    g_runnerParked.store(false);
    client.testSetDutyCycleHook(&parkRunnerHook);
    while (!g_runnerParked.load()) std::this_thread::yield();  // runner parked at the top of a cycle
    for (int i = 0; i < 200; ++i) client.onError([](const std::string&) {});  // > ringDrainLimit
    client.onError([keep = cq](const std::string&) {});  // holds the queue
    client.onError([](const std::string&) {});           // replacing it destroys `keep`
    cq.reset();                                          // the callable holds the last reference
    std::thread unpark([&] {
        rc::CompletionHandler h{&noopHandler, nullptr};
        while (client.listTopics(h, 1) != rc::OpResult::Closed) std::this_thread::yield();
        g_parkRunner.store(false);  // the runner resumes only after the ring closed
    });
    client.closeImmediately();  // the drain on THIS thread destroys the queue (C7: teardown only)
    unpark.join();
    EXPECT_EQ(client.testRegisteredCqs(), 0u);
}

// I1, Invoker variant: the Client is driven on a worker and destroyed on main
// (the supported off-owner destroy); the drain on main frees the queue.
TEST(CompletionsClient, QueueReleasedDuringAnOffOwnerCloseDrainRegression) {
    Watchdog wd("off-owner close() with a queue released inside its drain");
    std::unique_ptr<rc::Client>      client;
    std::shared_ptr<rc::Completions> cq;
    std::thread owner([&] {
        client = std::make_unique<rc::Client>(cfgFor(5252, rc::RunnerMode::Invoker));
        cq     = std::make_shared<rc::Completions>(*client);
    });
    owner.join();
    // Off the owner thread a setter is a ring command (applied by the drain).
    client->onError([keep = cq](const std::string&) {});
    client->onError([](const std::string&) {});
    cq.reset();
    client.reset();  // close() on main: its drain destroys the queue
    SUCCEED();
}

// I2: close()'s drain gives up behind a claim it cannot unblock (fix5) with a
// CqAdd behind it — the registration is never applied.  The constructor must
// not wait forever: it returns a born-finished (Closed) queue.
TEST(CompletionsClient, AbandonedCloseDrainLeavesTheQueueBornFinishedRegression) {
    Watchdog wd("Completions constructor behind an abandoned close drain");
    rc::Client client(cfgFor(5254, rc::RunnerMode::Thread));
    const auto idx = client.testClaimCommandRingWithoutCommitting(8);  // the orphan
    ASSERT_GE(idx, 0);
    client.testZeroCommandRingClaimHeader(idx);  // header-less: the runner stalls behind it
    client.testDisableCloseDrainUnblock();       // ... and close() cannot unblock it
    client.testSetCloseDrainUnblockAfterYields(16);
    const std::int64_t tailBefore = client.testCommandRingTail();
    std::unique_ptr<rc::Completions> q;
    rc::OpResult afterwards = rc::OpResult::Accepted;
    uint32_t     outstanding = 99;
    std::thread ctor([&] {  // CqAdd behind the orphan; this thread owns the queue
        q = std::make_unique<rc::Completions>(client);
        afterwards  = q->reserve(q->reservationFor(rc::OpKind::Ping));
        outstanding = q->outstanding();
        q.reset();  // born finished: freed locally
    });
    while (client.testCommandRingTail() == tailBefore) std::this_thread::yield();
    client.closeImmediately();  // abandons its drain: the CqAdd is never applied (C7: teardown only)
    ctor.join();
    EXPECT_EQ(afterwards, rc::OpResult::Closed) << "born finished";
    EXPECT_EQ(outstanding, 0u) << "the registration credit was returned";
    EXPECT_EQ(client.testCloseDrainUnblockCalls(), rc::agent::ClientAgent::kCloseDrainMaxFailedUnblocks);
}

// I3: the 5-bit generation must not wrap on one hot slot.  A stale op on queue
// A, A released, 31 more register/release cycles, then queue B: the stale
// completion is dropped and B is untouched.  And a (slot, generation) pair
// never repeats within kMaxCqs x 32 registrations (fresh slots first, FIFO reuse).
TEST(CompletionsClient, GenerationNeverWrapsOnAHotSlotRegression) {
    g_now = 1'000'000'000;
    rc::ClientConfig c = cfgFor(5256, rc::RunnerMode::Invoker, &fakeNow);
    c.connectTimeoutMs = 50;
    rc::Client client(c);
    rc::CompletionsConfig tiny;
    tiny.capacityBytes  = 1024;
    tiny.maxResultBytes = 64;
    tiny.maxErrorText   = 64;
    auto a = std::make_unique<rc::Completions>(client, tiny);
    ASSERT_EQ(client.connect(*a, 1), rc::OpResult::Accepted);
    client.doWork();
    a.reset();
    client.doWork();
    for (int i = 0; i < 31; ++i) {
        { rc::Completions tmp(client, tiny); }
        client.doWork();  // the release is a ring command between cycles
    }
    rc::Completions b(client, tiny);
    g_now += 200'000'000;
    for (int i = 0; i < 10 && client.testPendingControlOps() != 0; ++i) client.doWork();
    ASSERT_EQ(client.testPendingControlOps(), 0u);
    int n = 0;
    b.poll([&](const rc::Completion&, std::span<const uint8_t>) { ++n; });
    EXPECT_EQ(n, 0) << "a stale completion was misdelivered after a generation wrap";
    EXPECT_EQ(client.testStaleCqDrops(), 1u);
    EXPECT_EQ(b.outstanding(), 0u);
    EXPECT_EQ(b.reservedBytes(), 0u);

    // No (slot, generation) repeats within kMaxCqs x 32 registrations.
    std::vector<uint8_t> seen(1u << 15, 0);
    for (uint32_t i = 0; i < rc::kMaxCqs * 32 - 64; ++i) {
        rc::CqRef r = rc::kNoCqRef;
        {
            rc::Completions q(client, tiny);
            r = q.cqRef();
        }
        client.doWork();  // the release is a ring command between cycles
        ASSERT_NE(r, rc::kNoCqRef);
        ASSERT_EQ(seen[r & 0x7FFF], 0) << "(slot, generation) " << r << " repeated after " << i;
        seen[r & 0x7FFF] = 1;
    }
}

// Liveness: a control record larger than the command ring can ever carry is
// rejected synchronously (InvalidArgument) — not reported as a full ring that
// *AndWait would retry forever, ignoring its timeout.
TEST(CompletionsClient, OversizeControlRecordIsRejectedNotRetriedForeverRegression) {
    Watchdog wd("pingAndWait with a record over the command ring's limit");
    Rig r(cfgFor(5258));
    r.client.testCapCommandMsgLength(256);
    rc::Completions q(r.client);
    const std::vector<uint8_t> big(512, 0x42);
    EXPECT_EQ(r.client.ping(big, q, 1), rc::OpResult::InvalidArgument);
    EXPECT_EQ(q.outstanding(), 0u);
    EXPECT_THROW((void)r.client.pingAndWait(big, std::chrono::seconds(2)), std::runtime_error);
}

// ===========================================================================
// F8c C2+C3 fix round 2
// ===========================================================================

// I3 (absolute): with almost every registry slot held by long-lived queues the
// free list holds only a few slots, so FIFO reuse alone repeats a (slot,
// generation) pair within ~32 x free releases.  A released slot must stay out
// of reuse while any pending op still targets it — however long the churn —
// and be recycled once that op completed (its completion dropped).
TEST(CompletionsClient, SlotNeverRecycledWhileAnOpTargetsItRegression) {
    g_now = 1'000'000'000;
    rc::ClientConfig c = cfgFor(5260, rc::RunnerMode::Invoker, &fakeNow);
    c.connectTimeoutMs = 50;
    rc::Client client(c);
    rc::CompletionsConfig tiny;
    tiny.capacityBytes  = 1024;
    tiny.maxResultBytes = 64;
    tiny.maxErrorText   = 64;
    std::vector<std::unique_ptr<rc::Completions>> live;
    for (uint32_t i = 0; i < rc::kMaxCqs - 4; ++i) live.push_back(std::make_unique<rc::Completions>(client, tiny));
    auto a = std::make_unique<rc::Completions>(client, tiny);
    const uint32_t slotA = rc::cqRefIndex(a->cqRef());
    ASSERT_EQ(client.connect(*a, 1), rc::OpResult::Accepted);  // no gateway: stays pending
    client.doWork();
    a.reset();
    client.doWork();  // the release is applied: slot A quarantined, not freed
    for (int i = 0; i < 40'000; ++i) {
        auto b = std::make_unique<rc::Completions>(client, tiny);
        ASSERT_NE(rc::cqRefIndex(b->cqRef()), slotA)
            << "slot A recycled after " << i << " cycles while its connect is still pending";
        b.reset();
        client.doWork();
    }
    rc::Completions b(client, tiny);
    g_now += 200'000'000;  // the connect times out: its completion is dropped
    for (int i = 0; i < 10 && client.testPendingControlOps() != 0; ++i) client.doWork();
    ASSERT_EQ(client.testPendingControlOps(), 0u);
    EXPECT_EQ(client.testStaleCqDrops(), 1u);
    int n = 0;
    b.poll([&](const rc::Completion&, std::span<const uint8_t>) { ++n; });
    EXPECT_EQ(n, 0);
    EXPECT_EQ(b.outstanding(), 0u);
    EXPECT_EQ(b.reservedBytes(), 0u);
    bool reused = false;  // ... and slot A returns to service afterwards
    for (int i = 0; i < 16 && !reused; ++i) {
        auto q = std::make_unique<rc::Completions>(client, tiny);
        reused = rc::cqRefIndex(q->cqRef()) == slotA;
        q.reset();
        client.doWork();
    }
    EXPECT_TRUE(reused) << "slot A was never recycled after its op completed";
}

namespace {
/// A worker thread that owns a registered Completions; stopping it (RAII)
/// joins the thread, whose exit destroys that queue (review CrossJoin).
struct QueueWorker {
    std::atomic<bool> stop{false};
    std::atomic<bool> registered{false};
    std::thread       t;
    explicit QueueWorker(rc::Client& c, rc::Completions* adopt = nullptr)
        : t([this, &c, adopt] {
              std::unique_ptr<rc::Completions> mine(adopt != nullptr ? adopt : new rc::Completions(c));
              registered.store(true);
              while (!stop.load()) std::this_thread::yield();
          }) {}  // ~mine on exit: a release on THIS thread, racing close()
    ~QueueWorker() {
        stop.store(true);
        t.join();
    }
};
} // namespace

// I5: a callable destroyed by close()'s final drain stops + joins a worker
// whose exit releases ITS registered queue.  The worker is not the closer: its
// release must not wait for a hand-back the closer (blocked in the join) never
// reaches — last-one-frees handshake, nobody waits.
TEST(CompletionsClient, NonCloserReleaseDuringCloseNeverWaitsRegression) {
    Watchdog wd("close() joining a worker that releases its queue");
    rc::detail::CompletionRing::testOnDestroy = &countRingFree;
    g_ringsFreed.store(0);
    {
        rc::Client client(cfgFor(5262, rc::RunnerMode::Thread));
        auto w = std::make_shared<QueueWorker>(client);
        while (!w->registered.load()) std::this_thread::yield();
        ASSERT_EQ(client.testRegisteredCqs(), 1u);
        g_parkRunner.store(true);
        g_runnerParked.store(false);
        client.testSetDutyCycleHook(&parkRunnerHook);
        while (!g_runnerParked.load()) std::this_thread::yield();
        for (int i = 0; i < 200; ++i) client.onError([](const std::string&) {});
        client.onError([keep = w](const std::string&) {});
        client.onError([](const std::string&) {});  // the drain destroys `keep` -> joins the worker
        w.reset();
        std::thread unpark([&] {
            rc::CompletionHandler h{&noopHandler, nullptr};
            while (client.listTopics(h, 1) != rc::OpResult::Closed) std::this_thread::yield();
            g_parkRunner.store(false);
        });
        client.closeImmediately();  // C7: the runner is parked — teardown only
        unpark.join();
    }
    EXPECT_EQ(g_ringsFreed.load(), 1) << "the worker's ring was leaked or freed twice";
    rc::detail::CompletionRing::testOnDestroy = nullptr;
}

// I5, Invoker variant: the Client is destroyed off its owner; the drain joins a
// worker that releases a queue registered on the owner thread.
TEST(CompletionsClient, NonCloserReleaseDuringAnOffOwnerCloseNeverWaitsRegression) {
    Watchdog wd("off-owner close() joining a worker that releases its queue");
    rc::detail::CompletionRing::testOnDestroy = &countRingFree;
    g_ringsFreed.store(0);
    {
        std::unique_ptr<rc::Client>      client;
        std::shared_ptr<QueueWorker>     w;
        std::thread owner([&] {
            client = std::make_unique<rc::Client>(cfgFor(5264, rc::RunnerMode::Invoker));
            w = std::make_shared<QueueWorker>(*client, new rc::Completions(*client));  // owner-registered
        });
        owner.join();
        while (!w->registered.load()) std::this_thread::yield();
        client->onError([keep = w](const std::string&) {});  // off-owner: a ring command
        client->onError([](const std::string&) {});
        w.reset();
        client.reset();  // close() on main: the drain joins the worker
    }
    EXPECT_EQ(g_ringsFreed.load(), 1);
    rc::detail::CompletionRing::testOnDestroy = nullptr;
}

// n2: *AndWait's issue loop on a FULL command ring ends at its deadline
// (one budget for issue + wait) instead of retrying forever.
TEST(CompletionsClient, AndWaitOnAFullCommandRingTimesOutRegression) {
    Watchdog wd("pingAndWait on a full command ring");
    Rig r(cfgFor(5266));
    r.client.testFillCommandRing();
    try {
        (void)r.client.pingAndWait(std::vector<uint8_t>{1}, std::chrono::nanoseconds(0));
        ADD_FAILURE() << "pingAndWait must throw";
    } catch (const std::runtime_error& e) {
        EXPECT_NE(std::string(e.what()).find("timed out (command ring full)"), std::string::npos) << e.what();
    }
}

// ===========================================================================
// F8c C2+C3 fix round 3
// ===========================================================================
namespace {
rc::Client*                      g_icClient = nullptr;
std::unique_ptr<rc::Completions> g_icQ;
std::unique_ptr<rc::Completions> g_icB;
rc::CompletionsConfig            g_icTiny{4096, 256, 64};
rc::CqRef                        g_icRefQ = rc::kNoCqRef;
int                              g_icCalls = 0;
int                              g_icReleaseAt = 0;  // handler call that releases Q
int                              g_icChurnUntil = 0;  // handler calls that churn
void inCycleChurn(void*, const rc::Completion&, std::span<const uint8_t>) noexcept {
    const int call = g_icCalls++;
    if (call == g_icReleaseAt) {
        g_icRefQ = g_icQ->cqRef();
        g_icQ.reset();  // in-cycle release while Q's op is still in the command ring
    }
    if (call >= g_icReleaseAt && call < g_icChurnUntil) {
        rc::Completions tmp(*g_icClient, g_icTiny);  // in-cycle churn of the free slots
    }
    if (call == g_icChurnUntil) g_icB = std::make_unique<rc::Completions>(*g_icClient, g_icTiny);
}

/// The review's InCycleRelease shape: kMaxCqs-2 live queues + Q (one spare
/// slot, so the churn can proceed while Q's slot is held); `backlog`
/// handler-form ops, then Q's op (beyond one bounded drain); a handler releases
/// Q in-cycle and churns `churn` register/release pairs; B registers after.
void runInCycleRelease(int32_t stream, int backlog, int releaseAt, int churn) {
    g_now = 1'000'000'000;
    rc::Client client(cfgFor(stream, rc::RunnerMode::Invoker, &fakeNow));
    g_icClient = &client;
    g_icCalls = 0;
    g_icReleaseAt  = releaseAt;
    g_icChurnUntil = releaseAt + churn;
    std::vector<std::unique_ptr<rc::Completions>> live;
    for (uint32_t i = 0; i < rc::kMaxCqs - 2; ++i) live.push_back(std::make_unique<rc::Completions>(client, g_icTiny));
    g_icQ = std::make_unique<rc::Completions>(client, g_icTiny);
    const rc::CompletionHandler h{&inCycleChurn, nullptr};
    for (int i = 0; i < backlog; ++i) ASSERT_EQ(client.listTopics(h, 1), rc::OpResult::Accepted);
    ASSERT_EQ(client.listTopics(*g_icQ, 777), rc::OpResult::Accepted);  // behind the backlog
    for (int i = 0; i < 8 && client.testPendingControlOps() == 0 && g_icCalls <= backlog; ++i) client.doWork();
    for (int i = 0; i < 8; ++i) client.doWork();
    ASSERT_TRUE(g_icB != nullptr);
    int n = 0;
    g_icB->poll([&](const rc::Completion&, std::span<const uint8_t>) { ++n; });
    EXPECT_EQ(n, 0) << "Q's op (refQ " << g_icRefQ << ") misdelivered to B (ref " << g_icB->cqRef() << ")";
    EXPECT_NE(g_icB->cqRef(), g_icRefQ);
    EXPECT_EQ(g_icB->outstanding(), 0u);
    EXPECT_EQ(g_icB->reservedBytes(), 0u);
    g_icB.reset();
    g_icQ.reset();
    g_icClient = nullptr;
}
} // namespace

// I3': the Invoker owner releases a queue INSIDE a duty cycle while one of its
// ops (issued between cycles) is still in the command ring (past the bounded
// drain).  The release must not free the slot before that op is applied
// (counted): its completion is dropped, never delivered to the slot's next owner.
TEST(CompletionsClient, InCycleReleaseWithARingQueuedOpNeverMisdeliversRegression) {
    // Without the fence, the free ring alternates {spare, Q's slot}: Q's slot
    // is reused every 2nd registration, so 63 churns + B land B on Q's slot
    // with Q's generation (32 reuses).
    runInCycleRelease(5270, /*backlog=*/128, /*releaseAt=*/0, /*churn=*/63);
}

// I3', variant: the op is held behind a backlog across TWO bounded drains;
// the release and the churn span both cycles.
TEST(CompletionsClient, InCycleReleaseWithAnOpHeldAcrossTwoCyclesNeverMisdeliversRegression) {
    // Release at handler call 100 of the first drain; the 127 churns span it
    // and the second drain (64 reuses of Q's slot without the fence); Q's op
    // (record 301) is applied in the third.
    runInCycleRelease(5272, /*backlog=*/300, /*releaseAt=*/100, /*churn=*/127);
}

// No-hang fix round 3 (I1, client side): the broker's EXPIRING state delivers an
// unsolicited SessionExpired (correlationId 0) once the spill could not hold the
// session's answers — every pending control op completes SessionLost.
TEST(CompletionsClient, UnsolicitedSessionExpiredCompletesEveryPendingOpSessionLostRegression) {
    Rig r(cfgFor(5454));
    TestCq cq(r.client);
    r.connect(cq);
    std::vector<uint8_t> blob{9};
    auto d = cq.issue([&](rc::Completions& q, rc::Token t) {
        return r.client.defineTopic("x", 1, "", false, blob, q, t);
    });
    auto g = cq.issue([&](rc::Completions& q, rc::Token t) { return r.client.getTopicSchema("x", q, t); });
    ASSERT_TRUE(r.drive([&] { return r.gw.defines == 1 && r.gw.schemaGets == 1; }));
    r.gw.sendError(0, static_cast<int16_t>(rune::client::codec::ErrorCode::SessionExpired), "", /*sid=*/0,
                   /*corrId=*/0);
    ASSERT_TRUE(r.drive([&] { return d.ready() && g.ready(); }));
    EXPECT_EQ(d.status(), rc::OpStatus::SessionLost);
    EXPECT_EQ(g.status(), rc::OpStatus::SessionLost);
    r.settle();
    EXPECT_EQ(r.client.testPendingControlOps(), 0u);
}

// No-hang fix round 4 (item 6): the CLIENT's own liveness check (no answer
// within responseTimeoutNs — e.g. the broker's SessionExpired was lost at the
// lease deadline) declares the session lost: every in-flight control op
// completes SessionLost at that edge, exactly as on a broker SessionExpired —
// never later as Timeout through its per-op deadline.  RED (round 3): they
// stayed pending until pingTimeoutMs and completed Timeout.
TEST(CompletionsClient, OwnResponseTimeoutCompletesInFlightOpsSessionLostRegression) {
    rc::ClientConfig c = cfgFor(5458, rc::RunnerMode::Invoker, &fakeNow);
    c.responseTimeoutNs = 1'000'000'000LL;   // 1 s of silence = the session is lost
    c.pingTimeoutMs     = 60'000;            // the per-op deadline is far later
    Rig r(std::move(c));
    TestCq cq(r.client);
    r.connect(cq);
    std::vector<uint8_t> blob{9};
    auto d = cq.issue([&](rc::Completions& q, rc::Token t) {
        return r.client.defineTopic("x", 1, "", false, blob, q, t);
    });
    auto g = cq.issue([&](rc::Completions& q, rc::Token t) { return r.client.getTopicSchema("x", q, t); });
    ASSERT_TRUE(r.drive([&] { return r.gw.defines == 1 && r.gw.schemaGets == 1; }));
    ASSERT_FALSE(d.ready());
    const int64_t t0 = g_now;
    // 1 ms per duty cycle; the broker answers nothing (no Pongs either).
    ASSERT_TRUE(r.drive([&] { return d.ready() && g.ready(); }, 200'000, 1'000'000));
    EXPECT_EQ(d.status(), rc::OpStatus::SessionLost) << d.describe();
    EXPECT_EQ(g.status(), rc::OpStatus::SessionLost) << g.describe();
    EXPECT_LT(g_now - t0, 5'000'000'000LL) << "completed by the per-op deadline, not the session-loss edge";
    r.settle();
    EXPECT_EQ(r.client.testPendingControlOps(), 0u);
}

// No-hang fix round 5 (N14): the broker's REAL unsolicited SessionExpired names
// the session in its header (ClientGateway sends sessionId = the expiring
// session's id, correlationId 0).  For the seed session's own id, every pending
// control op completes SessionLost — the `opt->sessionId != 0` branch, which
// the sid-0 test above never took.
TEST(CompletionsClient, SessionExpiredWithTheSeedsRealSessionIdCompletesPendingOpsSessionLostRegression) {
    Rig r(cfgFor(5460));
    TestCq cq(r.client);
    ASSERT_TRUE(r.drive([&] { return r.gw.ready(); }));
    auto c = cq.issue([&](rc::Completions& q, rc::Token t) { return r.client.connect(q, t); });
    ASSERT_TRUE(r.drive([&] { return c.ready(); }));
    ASSERT_TRUE(c.ok()) << c.describe();
    const int64_t seedSid = c.outcome().c.value;
    ASSERT_NE(seedSid, 0);
    std::vector<uint8_t> blob{9};
    auto d = cq.issue([&](rc::Completions& q, rc::Token t) {
        return r.client.defineTopic("x", 1, "", false, blob, q, t);
    });
    auto g = cq.issue([&](rc::Completions& q, rc::Token t) { return r.client.getTopicSchema("x", q, t); });
    ASSERT_TRUE(r.drive([&] { return r.gw.defines == 1 && r.gw.schemaGets == 1; }));
    r.gw.sendError(0, static_cast<int16_t>(rune::client::codec::ErrorCode::SessionExpired), "", seedSid,
                   /*corrId=*/0);
    ASSERT_TRUE(r.drive([&] { return d.ready() && g.ready(); }));
    EXPECT_EQ(d.status(), rc::OpStatus::SessionLost) << d.describe();
    EXPECT_EQ(g.status(), rc::OpStatus::SessionLost) << g.describe();
    r.settle();
    EXPECT_EQ(r.client.testPendingControlOps(), 0u);
}
