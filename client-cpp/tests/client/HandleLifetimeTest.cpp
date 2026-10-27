// tests/client/HandleLifetimeTest.cpp — F8c C7: move-only handles and the §3.3
// release protocol (no shared_ptr, no refcount, no waits), handle destructors
// (Producer, plain or transactional = closeImmediately, Subscription =
// unsubscribe, GroupConsumer = close) and the Client close() / closeImmediately()
// pair (user decision 2).
//
// Every handle impl is owned jointly by its handle and the duty side; each side
// detaches its share of the impl's ownership word (a ring word) exactly once and
// whichever detaches LAST frees it.  rcd::testOnImplFree counts the frees (and
// where they happened).
//
// Invoker-mode tests are single-threaded (the test thread is the owner and
// drives doWork() + the gateway with a progress-or-10 s-deadline loop); the
// Thread-mode tests exercise the real runner (TSan: cross-thread traffic only
// through rings / the ownership words).  Nothing sleeps to pass time.
#include <filesystem>
#include "../support/AckLedger.h"
#include "../support/InvokerGateway.h"
#include "../support/TestCompletions.h"

#include <rune/client/Client.h>
#include <rune/client/Completions.h>
#include <rune/client/producer/ProducerImpl.h>

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <type_traits>
#include <unistd.h>
#include <vector>

namespace rc  = rune::client;
namespace rcd = rune::client::detail;
using rc::AckToken;
using rc::OpResult;
using rc::OpStatus;
using rc::SendResult;
using test_support::AckLedger;
using test_support::PendingOp;
using test_support::TestCq;

namespace {

std::atomic<int> g_ctr{0};
const std::vector<uint8_t> kVal(16, 0x5C);

// ---- free accounting (the testOnImplFree seam) --------------------------------
std::atomic<int>  g_frees[5];            // by HandleKind
std::atomic<int>  g_freesInDoWork{0};    // Invoker tests: freed inside client.doWork()
bool              g_inDoWork = false;    // Invoker tests: the test (owner) thread only
std::atomic<int>  g_freesOnMain{0};      // freed on the registered "main" thread
std::thread::id   g_mainThread{};        // set before any thread that may free runs

void countFree(rcd::HandleKind k, const void*) noexcept {
    g_frees[static_cast<int>(k)].fetch_add(1);
    if (g_inDoWork) g_freesInDoWork.fetch_add(1);
    if (std::this_thread::get_id() == g_mainThread) g_freesOnMain.fetch_add(1);
}
int frees(rcd::HandleKind k) { return g_frees[static_cast<int>(k)].load(); }
int totalFrees() {
    int n = 0;
    for (auto& f : g_frees) n += f.load();
    return n;
}
/// Installs the free hook for one test (no other thread runs yet) and removes
/// it at the end (every thread that may free has been joined by then).
struct FreeHook {
    FreeHook() {
        for (auto& f : g_frees) f.store(0);
        g_freesInDoWork.store(0);
        g_freesOnMain.store(0);
        g_inDoWork  = false;
        g_mainThread = std::this_thread::get_id();
        rcd::testOnImplFree = &countFree;
    }
    ~FreeHook() { rcd::testOnImplFree = nullptr; }
};

/// Aborts the process if a test hangs (a release must never wait).
struct Watchdog {
    std::atomic<bool> done{false};
    std::thread       t;
    explicit Watchdog(const char* what)
        : t([this, what] {
              const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(60);
              while (!done.load()) {
                  if (std::chrono::steady_clock::now() > deadline) {
                      std::fprintf(stderr, "WATCHDOG: hung in %s\n", what);
                      std::abort();
                  }
                  std::this_thread::yield();
              }
          }) {}
    ~Watchdog() {
        done.store(true);
        t.join();
    }
};

rc::ClientConfig cfgFor(int32_t in, rc::RunnerMode mode = rc::RunnerMode::Invoker) {
    rc::ClientConfig c;
    c.gatewayChannel  = "aeron:ipc";
    c.gatewayStreamId = in;
    c.egressChannel   = "aeron:ipc";
    c.egressStreamId  = in + 1;
    c.dataDir = std::filesystem::temp_directory_path().string() + "/handle_life_" + std::to_string(::getpid()) + "_" + std::to_string(g_ctr.fetch_add(1));
    c.runnerMode         = mode;
    c.subscribeTimeoutMs = 200;  // an unanswered Subscribe is swept quickly (Thread-mode tests)
    rc::TopicSpec ts;
    ts.name           = "t";
    ts.topicId        = 1;
    ts.partitionCount = 1;
    c.topics.push_back(ts);
    return c;
}

/// A transactional producer's config (Phase 2b F: a Producer with a transactionalId; acks=all).
rc::ProducerConfig txnCfg(std::string id) {
    rc::ProducerConfig c;
    c.acks            = rc::Acks::All;
    c.transactionalId = std::move(id);
    return c;
}

rc::ProducerConfig ackCfg() {
    rc::ProducerConfig c;
    c.acks              = rc::Acks::Leader;
    c.lingerUs          = 60'000'000;  // only a flush (or a closing producer) ends a batch
    c.batchBytes        = 64 * 1024;
    c.maxInFlight       = 8;
    c.produceTimeoutMs  = 1'000;
    c.deliveryTimeoutMs = 10'000;
    return c;
}

struct Rig {
    rc::ClientConfig              cfg;
    std::unique_ptr<rc::Client>   client;
    test_support::InvokerGateway  gw;
    explicit Rig(rc::ClientConfig c)
        : cfg(std::move(c)), client(std::make_unique<rc::Client>(cfg)),
          gw(cfg.dataDir + "/aeron", cfg.gatewayChannel, cfg.gatewayStreamId, cfg.egressChannel,
             cfg.egressStreamId) {}
    int doWork() {
        g_inDoWork = true;
        const int w = client->doWork();
        g_inDoWork = false;
        return w;
    }
    // Drive until pred holds; the 10 s deadline is only reached by a FAILING test.
    template <class P>
    bool drive(P&& pred) {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
        while (std::chrono::steady_clock::now() < deadline) {
            doWork();
            gw.pump();
            if (pred()) return true;
        }
        return false;
    }
    void connect() {
        ASSERT_TRUE(drive([&] { return gw.ready(); }));
        TestCq cq(*client);
        auto f = cq.issue([&](rc::Completions& q, rc::Token t) { return client->connect(q, t); });
        ASSERT_TRUE(drive([&] { return f.ready(); }));
        ASSERT_TRUE(f.ok()) << f.describe();
    }
};

/// Pumps the (invoker-driven) gateway from a helper thread while the test
/// thread blocks in Client::close() (whose Invoker form pumps only the
/// Client).  The gateway is touched by this thread alone until stop().
struct GatewayPump {
    test_support::InvokerGateway& gw;
    std::atomic<bool>             stopFlag{false};
    std::thread                   t;
    explicit GatewayPump(test_support::InvokerGateway& g)
        : gw(g), t([this] {
              while (!stopFlag.load(std::memory_order_acquire)) {
                  if (gw.pump() == 0) std::this_thread::yield();
              }
          }) {}
    void stop() {
        if (t.joinable()) {
            stopFlag.store(true, std::memory_order_release);
            t.join();
        }
    }
    ~GatewayPump() { stop(); }
};

void noopRecords(const rc::Record&) {}

} // namespace

// ===========================================================================
// Move-only handles (spec §3.1 rules 1 and 3)
// ===========================================================================

static_assert(!std::is_copy_constructible_v<rc::Producer> && !std::is_copy_assignable_v<rc::Producer>);
static_assert(std::is_nothrow_move_constructible_v<rc::Producer> &&
              std::is_nothrow_move_assignable_v<rc::Producer>);
static_assert(!std::is_copy_constructible_v<rc::Subscription> && !std::is_copy_constructible_v<rc::GroupConsumer>);
static_assert(!std::is_copy_constructible_v<rc::BatchBuilder>);
struct StubT {
    static constexpr uint64_t kFingerprint = 0;
};
static_assert(!std::is_copy_constructible_v<rc::TypedTopic<StubT>> &&
              std::is_move_constructible_v<rc::TypedTopic<StubT>>);

// Moving a handle moves its share (a moved-from handle releases nothing);
// move-assigning over a live handle releases the old one (its destructor
// semantics) — exactly one free per impl.
TEST(HandleLifetime, MoveOnlyHandlesRegression) {
    FreeHook hook;
    Rig r(cfgFor(5400));
    r.connect();
    const auto topic = r.client->topic("t");
    {
        rc::Producer a = r.client->producer(ackCfg());
        rc::Producer b = std::move(a);
        EXPECT_FALSE(a.valid());  // NOLINT(bugprone-use-after-move): moved-from is checkable
        EXPECT_EQ(a.trySend(topic, rc::Key(uint64_t{1}), kVal), SendResult::InvalidTopic);
        EXPECT_TRUE(b.valid());
        EXPECT_EQ(b.trySend(topic, rc::Key(uint64_t{1}), kVal), SendResult::Ok);
        rc::Producer c = r.client->producer(ackCfg());
        c = std::move(b);  // c's own producer: released (closeImmediately)
        EXPECT_EQ(c.trySend(topic, rc::Key(uint64_t{2}), kVal), SendResult::Ok);
        ASSERT_TRUE(r.drive([&] { return frees(rcd::HandleKind::Producer) == 1; }))
            << "the overwritten producer is freed once the duty side let go";

        rc::Producer t1 = r.client->producer(txnCfg("tx-move"));
        rc::Producer t2 = std::move(t1);
        EXPECT_FALSE(t1.valid());                            // NOLINT(bugprone-use-after-move)
        EXPECT_EQ(t1.closeImmediately(), OpResult::InvalidArgument);  // a moved-from handle: no impl
        rc::Subscription s1 = r.client->subscribe(topic, 0, {}, &noopRecords);
        rc::Subscription s2 = std::move(s1);
        EXPECT_FALSE(s1.active());  // NOLINT(bugprone-use-after-move)
    }
    ASSERT_TRUE(r.drive([&] { return totalFrees() == 4; }));
    EXPECT_EQ(frees(rcd::HandleKind::Producer), 3) << "two plain + one transactional producer";
    EXPECT_EQ(frees(rcd::HandleKind::Subscription), 1);
    r.client->closeImmediately();
    r.client.reset();
    EXPECT_EQ(totalFrees(), 4) << "no impl freed twice";
}

// ===========================================================================
// §3.3 (a): an accepted release — the duty side lets go last and frees
// ===========================================================================

// Invoker mode: every handle kind released on the owner thread, outside a duty
// cycle, is applied directly (duty work) but freed only at the end of the NEXT
// duty cycle — by the duty side (the handle already let go), once nothing on
// the duty side references the impl.  RED (parent): the hubs held a
// shared_ptr; a released handle's impl stayed registered until Client close.
TEST(HandleLifetime, HandleReleaseAcceptedDutyFreesRegression) {
    FreeHook hook;
    Rig r(cfgFor(5402));
    r.connect();
    const auto topic = r.client->topic("t");
    {
        auto p   = r.client->producer(ackCfg());
        auto tp  = r.client->producer(txnCfg("tx-release"));
        auto sub = r.client->subscribe(topic, 0, {}, &noopRecords);
        auto gc  = r.client->subscribeGroup("g-release", {"t"});
        ASSERT_TRUE(r.drive([&] {
            return r.client->testProducerCount() == 2 &&
                   r.client->testSubscriptionImplsHeld() == 1 && r.client->testGroupImplsHeld() == 1 &&
                   sub.subscriptionId() > 0;
        }));
    }  // all four destroyed here: owner thread, outside a duty cycle
    EXPECT_EQ(totalFrees(), 0) << "the duty side still holds its share";
    EXPECT_EQ(r.client->testPendingReleases(), 4u);
    ASSERT_TRUE(r.drive([&] { return totalFrees() == 4; }));
    EXPECT_EQ(frees(rcd::HandleKind::Producer), 2) << "the plain and the transactional producer";
    for (const auto k : {rcd::HandleKind::Subscription, rcd::HandleKind::GroupConsumer}) {
        EXPECT_EQ(frees(k), 1) << "kind " << static_cast<int>(k);
    }
    EXPECT_EQ(g_freesInDoWork.load(), 4) << "each one freed by the duty side (the handle let go first)";
    EXPECT_EQ(r.client->testPendingReleases(), 0u);
    EXPECT_EQ(r.client->testProducerCount(), 0u);
    EXPECT_EQ(r.client->testSubscriptionImplsHeld(), 0u);
    EXPECT_EQ(r.client->testGroupImplsHeld(), 0u);
    ASSERT_FALSE(r.gw.unsubscribed.empty()) << "the Subscription destructor unsubscribed";
    r.client->closeImmediately();
    EXPECT_EQ(r.client->testImplsHandedBack(), 0u) << "nothing left to hand back";
    r.client.reset();
    EXPECT_EQ(totalFrees(), 4);
}

// Thread mode: handles destroyed on an app thread release through the agent
// command ring (FIFO behind their own ops); the runner applies the release and
// the impl is freed exactly once.  TSan: the only shared memory is the rings
// and the ownership words.
TEST(HandleLifetime, ThreadModeReleaseFreesExactlyOnceRegression) {
    Watchdog wd("Thread-mode handle release");
    FreeHook hook;
    auto client = std::make_unique<rc::Client>(cfgFor(5404, rc::RunnerMode::Thread));  // never connected
    const auto topic = client->topic("t");
    {
        auto p   = client->producer(ackCfg());
        auto tp  = client->producer(txnCfg("tx-thread"));
        auto sub = client->subscribe(topic, 0, {}, &noopRecords);
        auto gc  = client->subscribeGroup("g-thread", {"t"});
        EXPECT_EQ(p.trySend(topic, rc::Key(uint64_t{1}), kVal), SendResult::Ok);
    }
    aeron::concurrent::BackoffIdleStrategy idle(10, 5, std::chrono::duration<long, std::nano>(1'000),
                                                std::chrono::milliseconds(1));
    const auto backstop = std::chrono::steady_clock::now() + std::chrono::seconds(20);  // hang guard only
    while (totalFrees() < 4 && std::chrono::steady_clock::now() < backstop) idle.idle(0);
    ASSERT_EQ(totalFrees(), 4) << "a released impl was never freed";
    client->close();
    EXPECT_EQ(client->testImplsHandedBack(), 0u);
    client.reset();
    EXPECT_EQ(frees(rcd::HandleKind::Producer), 2) << "the plain and the transactional producer";
    for (const auto k : {rcd::HandleKind::Subscription, rcd::HandleKind::GroupConsumer}) {
        EXPECT_EQ(frees(k), 1) << "kind " << static_cast<int>(k);
    }
}

// ===========================================================================
// §3.3 (b): Client close hands every impl back; the handle frees it later
// ===========================================================================

// The handles outlive their Client: close() hands each impl back (the duty
// side detaches — nothing is freed while the handle lives), the Client is
// destroyed, and each handle then frees its impl on its own thread without
// touching the (gone) Client (ASan).  Both runner modes.
TEST(HandleLifetime, HandleReleaseAfterCloseHandleFreesRegression) {
    for (const rc::RunnerMode mode : {rc::RunnerMode::Invoker, rc::RunnerMode::Thread}) {
        SCOPED_TRACE(mode == rc::RunnerMode::Invoker ? "Invoker" : "Thread");
        FreeHook hook;
        auto client = std::make_unique<rc::Client>(cfgFor(mode == rc::RunnerMode::Invoker ? 5406 : 5408, mode));
        const auto topic = client->topic("t");
        std::optional<rc::Producer>              p(client->producer(ackCfg()));
        std::optional<rc::Producer>              tp(client->producer(txnCfg("tx-after")));
        std::optional<rc::Subscription>          sub(client->subscribe(topic, 0, {}, &noopRecords));
        std::optional<rc::GroupConsumer>         gc(client->subscribeGroup("g-after", {"t"}));
        if (mode == rc::RunnerMode::Invoker) {
            for (int i = 0; i < 10; ++i) client->doWork();  // adopt the ring registrations
        }
        client->close();
        EXPECT_EQ(client->testImplsHandedBack(), 4u);
        EXPECT_EQ(totalFrees(), 0) << "handed back, still owned by the live handles";
        client.reset();  // the handles outlive the Client
        EXPECT_EQ(p->trySend(topic, rc::Key(uint64_t{1}), kVal), SendResult::Closed);
        EXPECT_EQ(p->closeImmediately(), OpResult::Closed);
        EXPECT_EQ(tp->closeImmediately(), OpResult::Closed);
        EXPECT_TRUE(sub->finished());
        EXPECT_TRUE(gc->finished());
        p.reset();
        tp.reset();
        sub.reset();
        gc.reset();
        EXPECT_EQ(totalFrees(), 4);
        EXPECT_EQ(g_freesOnMain.load(), 4) << "each handle freed its impl itself";
    }
}

// ===========================================================================
// Release racing close: exactly one free per impl, nothing waits
// ===========================================================================

// The ownership word itself: the duty side and the handle detach concurrently
// (one acq_rel RMW each) — exactly one of them frees the impl, every time.
// Both word kinds: the M2O ring's (ProducerImpl) and the O2O status ring's
// (Subscription / GroupConsumer / transactional Producer impls).  1000
// iterations each (TSan: the freeing side observes the other side's writes;
// ASan: never freed twice, never used after free).
TEST(HandleLifetime, ReleaseRacingCloseExactlyOneFreeRegression) {
    FreeHook hook;
    constexpr int kIterations = 1000;
    auto race = [](auto* impl, auto dutyDetach, auto handleDetach, auto touchDuty, auto touchHandle) {
        std::atomic<int> go{0};
        std::thread duty([&] {
            go.fetch_add(1);
            while (go.load() < 2) {}
            touchDuty(impl);
            if (dutyDetach(impl)) delete impl;
        });
        go.fetch_add(1);
        while (go.load() < 2) {}
        touchHandle(impl);
        if (handleDetach(impl)) delete impl;
        duty.join();
    };
    for (int i = 0; i < kIterations; ++i) {
        auto* p = new rc::producer::ProducerImpl(ackCfg(), 1024);
        race(
            p, [](rc::producer::ProducerImpl* x) { return x->detachDuty(); },
            [](rc::producer::ProducerImpl* x) { return x->detachHandle(); },
            [](rc::producer::ProducerImpl* x) { x->setOrdinal(7); },              // a duty write
            [](rc::producer::ProducerImpl* x) { (void)x->dutyDetached(); });       // a handle read
        auto* s = new rc::SubscriptionImpl();
        race(
            s, [](rc::SubscriptionImpl* x) { return x->status.detachProducer(); },
            [](rc::SubscriptionImpl* x) { return x->status.detachConsumer(); },
            [](rc::SubscriptionImpl* x) { x->nextOffset = 9; },
            [](rc::SubscriptionImpl* x) { rc::SubscriptionStatus st; (void)x->status.drainLatest(st); });
        auto* g = new rc::GroupConsumerImpl();
        race(
            g, [](rc::GroupConsumerImpl* x) { return x->status.detachProducer(); },
            [](rc::GroupConsumerImpl* x) { return x->status.detachConsumer(); },
            [](rc::GroupConsumerImpl* x) { x->generationId = 3; },
            [](rc::GroupConsumerImpl* x) { rc::GroupConsumerStatus st; (void)x->status.drainLatest(st); });
    }
    EXPECT_EQ(frees(rcd::HandleKind::Producer), kIterations);
    EXPECT_EQ(frees(rcd::HandleKind::Subscription), kIterations);
    EXPECT_EQ(frees(rcd::HandleKind::GroupConsumer), kIterations);
}

// Client level: handles of every kind destroyed on worker threads while the
// main thread closes the Client (close() and closeImmediately() alternately) —
// each impl freed exactly once, no release ever waits (Thread mode; ASan + TSan).
TEST(HandleLifetime, ClientReleaseRacingCloseExactlyOneFreeRegression) {
    FreeHook hook;
    constexpr int kIterations = 30;
    constexpr int kWorkers    = 4;
    int created = 0;
    for (int it = 0; it < kIterations; ++it) {
        // Armed per iteration: a hang in any one is caught, while the 30 Client lifecycles (an embedded driver
        // each, 1-2 s under TSan) are not held to one 60 s budget.
        Watchdog wd("handle releases racing Client close");
        auto client = std::make_unique<rc::Client>(cfgFor(5410, rc::RunnerMode::Thread));
        const auto topic = client->topic("t");
        std::atomic<int>  ready{0};
        std::atomic<bool> go{false};
        std::atomic<int>  made{0};
        std::vector<std::thread> ts;
        for (int k = 0; k < kWorkers; ++k) {
            ts.emplace_back([&, k] {
                std::optional<rc::Producer>              p(client->producer(ackCfg()));
                std::optional<rc::Producer>              tp(client->producer(txnCfg("tx-" + std::to_string(k))));
                std::optional<rc::Subscription>          sub(client->subscribe(topic, 0, {}, &noopRecords));
                std::optional<rc::GroupConsumer>         gc;
                if (k == 0) gc.emplace(client->subscribeGroup("g-race", {"t"}));  // one group per Client
                made.fetch_add(k == 0 ? 4 : 3);
                // A record only when the Client is closed immediately (close()
                // would drain it until its delivery timeout: never connected).
                if (it % 2 == 1) (void)p->trySend(topic, rc::Key(static_cast<uint64_t>(k)), kVal);
                ready.fetch_add(1);
                while (!go.load()) std::this_thread::yield();
                if ((k + it) % 3 == 0) std::this_thread::yield();  // vary the interleaving
                p.reset();
                tp.reset();
                sub.reset();
                gc.reset();
            });
        }
        while (ready.load() != kWorkers) std::this_thread::yield();
        go.store(true);
        if (it % 2 == 0) client->close(); else client->closeImmediately();
        for (auto& t : ts) t.join();
        client.reset();
        created += made.load();
        ASSERT_EQ(totalFrees(), created) << "iteration " << it << ": an impl leaked or was freed twice";
    }
}

// ===========================================================================
// A handle destroyed inside its own callback: the free waits for the walk
// ===========================================================================

namespace {
struct SelfDestroyingProducer {
    std::optional<rc::Producer>* p     = nullptr;
    AckLedger                    led;
    int                          freesInHandler = -1;
    static void onAck(void* ctx, rc::Token t, int64_t v, OpStatus st, uint16_t ec) noexcept {
        auto* self = static_cast<SelfDestroyingProducer*>(ctx);
        AckLedger::onAck(&self->led, t, v, st, ec);
        if (self->p->has_value()) {
            self->p->reset();  // destroyed inside its own ack handler (the duty walk)
            self->freesInHandler = totalFrees();
        }
    }
};
struct SelfDestroyingTxn {
    std::optional<rc::Producer>* tp = nullptr;
    int  calls          = 0;
    int  freesInHandler = -1;
    static void on(void* ctx, const rc::Completion&, std::span<const uint8_t>) noexcept {
        auto* self = static_cast<SelfDestroyingTxn*>(ctx);
        ++self->calls;
        self->tp->reset();
        self->freesInHandler = totalFrees();
    }
};
struct SelfDestroyingGroup {
    std::optional<rc::GroupConsumer>* gc = nullptr;
    int      calls          = 0;
    OpStatus status         = OpStatus::Ok;
    int      freesInHandler = -1;
    static void on(void* ctx, const rc::Completion& c, std::span<const uint8_t>) noexcept {
        auto* self = static_cast<SelfDestroyingGroup*>(ctx);
        ++self->calls;
        self->status = c.status;
        self->gc->reset();
        self->freesInHandler = totalFrees();
    }
};
} // namespace

// A Producer destroyed inside its own onAck, a Subscription inside its own
// record handler, a transactional Producer inside its own completion handler
// and a GroupConsumer inside its own commitSync completion: the destructor's
// semantics apply at once, but the impl is freed only after the walk that
// fired the callback finished (end of the duty cycle) — never under it (ASan).
// Every accepted op still completes exactly once.
TEST(HandleLifetime, HandleDestroyedInsideOwnCallbackDefersFreeRegression) {
    FreeHook hook;
    Rig r(cfgFor(5412));
    r.connect();
    const auto topic = r.client->topic("t");

    {  // Producer, from its own onAck (inside the ProduceResponse walk)
        SCOPED_TRACE("Producer");
        std::optional<rc::Producer> p(r.client->producer(ackCfg()));
        SelfDestroyingProducer ctx;
        ctx.p = &p;
        (*p).onAck(&SelfDestroyingProducer::onAck, &ctx);
        for (rc::Token t = 1; t <= 3; ++t) {
            ASSERT_EQ(p->trySend(topic, rc::Key(uint64_t{1}), kVal, AckToken{t}), SendResult::Ok);
        }
        p->flush();
        ASSERT_TRUE(r.drive([&] { return ctx.led.recs.size() == 3 && frees(rcd::HandleKind::Producer) == 1; }));
        EXPECT_EQ(ctx.freesInHandler, 0) << "freed under its own ack walk";
        EXPECT_TRUE(ctx.led.exactlyOnce(1, 4));
    }
    {  // Subscription, from its own record handler (inside onRecords)
        SCOPED_TRACE("Subscription");
        std::optional<rc::Subscription> sub;
        int got = 0, freesInHandler = -1;
        sub.emplace(r.client->subscribe(topic, 0, rc::SubscribeOptions{rc::From::earliest()},
                                        [&](const rc::Record&) {
                                            ++got;
                                            sub.reset();
                                            freesInHandler = totalFrees();
                                        }));
        ASSERT_TRUE(r.drive([&] { return !r.gw.subscribed.empty() && sub.has_value() && sub->subscriptionId() > 0; }));
        r.gw.pushRecords(r.gw.subscribed.back(), 0, 3);
        ASSERT_TRUE(r.drive([&] { return frees(rcd::HandleKind::Subscription) == 1; }));
        EXPECT_EQ(got, 1) << "no record after the handle was destroyed";
        EXPECT_EQ(freesInHandler, 1) << "only the producer above was freed while the handler ran";
    }
    {  // transactional Producer, from its own initTransactions completion
        SCOPED_TRACE("transactional Producer");
        r.gw.initPidReply = true;
        std::optional<rc::Producer> tp(r.client->producer(txnCfg("tx-self")));
        SelfDestroyingTxn ctx;
        ctx.tp = &tp;
        ASSERT_EQ(tp->initTransactions(rc::CompletionHandler{&SelfDestroyingTxn::on, &ctx}, 1), OpResult::Accepted);
        ASSERT_TRUE(r.drive([&] { return frees(rcd::HandleKind::Producer) == 2; }));
        EXPECT_EQ(ctx.calls, 1);
        EXPECT_EQ(ctx.freesInHandler, 2) << "freed under its own response walk";
    }
    {  // GroupConsumer, from its own commitSync completion (applied directly on
       // the owner, outside a duty cycle: completed at once, SessionLost —
       // nothing joined)
        SCOPED_TRACE("GroupConsumer");
        rc::ClientConfig c = cfgFor(5414);
        rc::Client unconnected(c);
        std::optional<rc::GroupConsumer> gc(unconnected.subscribeGroup("g-self", {"t"}));
        SelfDestroyingGroup ctx;
        ctx.gc = &gc;
        ASSERT_EQ(gc->commitSync(rc::CompletionHandler{&SelfDestroyingGroup::on, &ctx}, 1), OpResult::Accepted);
        EXPECT_EQ(ctx.calls, 1);
        EXPECT_EQ(ctx.status, OpStatus::SessionLost);
        EXPECT_EQ(ctx.freesInHandler, 3) << "freed under its own completion";
        EXPECT_EQ(frees(rcd::HandleKind::GroupConsumer), 0) << "the duty share waits for a duty cycle";
        g_inDoWork = true;
        (void)unconnected.doWork();
        g_inDoWork = false;
        EXPECT_EQ(frees(rcd::HandleKind::GroupConsumer), 1);
    }
}

// ===========================================================================
// Invoker off-owner destroy: through the ring, bounded by the liveness contract
// ===========================================================================

// The Client is driven on a worker and its handles are destroyed on main after
// the worker joined (the documented join-then-destroy).  Each release goes
// through the command ring (spec §3.3; F8c C7 fix round 1, I-2) — never
// touching duty state off the owner.  With the command ring FULL nobody can
// drain it: the release is ORPHANED at once (F8c C7 fix round 2, F1 — no
// wait, no time bound) and the Client's own (off-owner) close frees the impl.
// Every impl freed exactly once; no release waits (watchdog).
TEST(HandleLifetime, InvokerOffOwnerDestroyNeverWaitsRegression) {
    Watchdog wd("Invoker off-owner handle destroy");
    for (const bool fullRing : {false, true}) {
        SCOPED_TRACE(fullRing ? "full command ring" : "ring with room");
        FreeHook hook;
        std::unique_ptr<rc::Client>               client;
        std::optional<rc::Producer>               p;
        std::optional<rc::Producer>               tp;
        std::optional<rc::Subscription>           sub;
        std::optional<rc::GroupConsumer>          gc;
        std::thread owner([&] {
            client = std::make_unique<rc::Client>(cfgFor(fullRing ? 5416 : 5438));
            const auto topic = client->topic("t");
            p.emplace(client->producer(ackCfg()));
            tp.emplace(client->producer(txnCfg("tx-offowner")));
            sub.emplace(client->subscribe(topic, 0, {}, &noopRecords));
            gc.emplace(client->subscribeGroup("g-offowner", {"t"}));
            for (int i = 0; i < 10; ++i) client->doWork();  // adopt the ring registrations
            if (fullRing) client->testFillCommandRing();
        });
        owner.join();
        p.reset();  // each returns at once: accepted onto the ring, or orphaned (full)
        tp.reset();
        sub.reset();
        gc.reset();
        EXPECT_EQ(totalFrees(), 0) << "nothing frees an impl off the owner";
        client.reset();  // off-owner close: drains / hands back, frees each impl once
        EXPECT_EQ(totalFrees(), 4);
        EXPECT_EQ(frees(rcd::HandleKind::Producer), 2) << "the plain and the transactional producer";
        for (const auto k : {rcd::HandleKind::Subscription, rcd::HandleKind::GroupConsumer}) {
            EXPECT_EQ(frees(k), 1) << "kind " << static_cast<int>(k);
        }
    }
}

// F1: the command-ring head is stuck behind a back-pressured send (the drain
// ABORTs it every cycle while the transport refuses it) and the ring is FULL,
// while the Invoker owner keeps pumping.  A worker destroys its Subscription:
// the release is orphaned — it never waits and is never dropped.  The duty
// side applies it in FIFO order once the head moves (the back-pressure lifts):
// the Unsubscribe reaches the gateway, the handler stops for good, and the
// impl is freed WITHOUT closing the Client.  RED (round 1): after
// aeronClientLivenessTimeoutNs of a frozen head the handle gave up and only
// detached — the subscription was never unsubscribed nor freed while the
// Client lived (its handler kept running after its destructor returned).
TEST(HandleLifetime, InvokerOffOwnerReleaseUnderAbortedHeadStillAppliesRegression) {
    Watchdog wd("off-owner release under an ABORTed command-ring head");
    FreeHook hook;
    rc::ClientConfig cfg = cfgFor(5442);
    cfg.aeronClientLivenessTimeoutNs = 1'000'000'000LL;  // round 1's bound: 1 s of frozen head
    Rig r(cfg);
    r.connect();
    const auto topic = r.client->topic("t");
    std::atomic<int> delivered{0};
    std::optional<rc::Subscription> sub(r.client->subscribe(
        topic, 0, rc::SubscribeOptions{rc::From::earliest()},
        [&](const rc::Record&) { delivered.fetch_add(1); }));
    ASSERT_TRUE(r.drive([&] { return !r.gw.subscribed.empty() && sub->subscriptionId() > 0; }));
    int64_t next = 0;
    r.gw.pushRecords(r.gw.subscribed[0], next, 2);
    next += 2;
    ASSERT_TRUE(r.drive([&] { return delivered.load() == 2; }));

    // The head: a session frame the transport refuses (ABORT, retried every cycle).
    r.client->testForceIngressOffers(rc::transport::AeronTransport::OfferResult::Retry, 1 << 30);
    const int pingsBefore = r.gw.pings;
    ASSERT_TRUE(r.client->testSubmitSessionPing());
    for (int i = 0; i < 100; ++i) r.doWork();  // it is at the head now, refused every cycle
    r.client->testFillCommandRing();

    std::atomic<bool> released{false};
    std::thread worker([&, s = std::move(sub)]() mutable {
        s.reset();  // the ring is full: orphaned at once
        released.store(true);
    });
    // The owner keeps pumping (and records keep flowing) while the head is stuck.
    ASSERT_TRUE(r.drive([&] {
        r.gw.pushRecords(r.gw.subscribed[0], next, 1);
        ++next;
        return released.load();
    }));
    worker.join();
    for (int i = 0; i < 200; ++i) r.doWork(), r.gw.pump();
    EXPECT_EQ(frees(rcd::HandleKind::Subscription), 0) << "FIFO: not before the head passes its fence";
    EXPECT_EQ(r.gw.pings, pingsBefore) << "the head send is still being refused";

    r.client->testForceIngressOffers(rc::transport::AeronTransport::OfferResult::Accepted, 0);  // lift it
    ASSERT_TRUE(r.drive([&] {
        return r.gw.pings > pingsBefore && !r.gw.unsubscribed.empty() &&
               frees(rcd::HandleKind::Subscription) == 1;
    })) << "the orphaned release was never applied";
    const int after = delivered.load();
    r.gw.pushRecords(r.gw.subscribed[0], next, 3);
    for (int i = 0; i < 2'000; ++i) r.doWork(), r.gw.pump();
    EXPECT_EQ(delivered.load(), after) << "the handler ran after its release was applied";
    EXPECT_EQ(r.client->testSubscriptionImplsHeld(), 0u);
}

// ---- N1 / N2 rigs: the command-ring head stuck behind an ABORTed send ------
namespace {
// Thread mode: the runner performs the stuck-head seams itself (they touch
// duty state) — from the injected duty clock, which only the runner calls in
// these tests (the app thread never reads it: a Ring producer, no heartbeat
// wait).  Test-only globals; atomics are fine in tests.
std::atomic<rc::Client*>    g_seamClient{nullptr};
std::atomic<int>            g_seamAction{0};  // 1: stick the head, 2: lift it
std::atomic<long>           g_runnerClockCalls{0};
std::thread::id             g_testThread{};
int64_t seamClock() noexcept {
    if (std::this_thread::get_id() != g_testThread) {
        g_runnerClockCalls.fetch_add(1);
        rc::Client* c = g_seamClient.load();
        const int a = g_seamAction.exchange(0);
        if (c != nullptr && a == 1) {
            c->testForceIngressOffers(rc::transport::AeronTransport::OfferResult::Retry, 1 << 30);
            (void)c->testSubmitSessionPing();
        } else if (c != nullptr && a == 2) {
            c->testForceIngressOffers(rc::transport::AeronTransport::OfferResult::Accepted, 0);
        }
    }
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
               std::chrono::steady_clock::now().time_since_epoch()).count();
}
/// The runner advanced at least `n` more duty-clock reads (cycles).
void awaitRunner(long n) {
    const long target = g_runnerClockCalls.load() + n;
    while (g_runnerClockCalls.load() < target) std::this_thread::yield();
}
} // namespace

// N1: an orphaned release keeps FIFO order behind the handle's own queued ops.
// Thread mode: an app thread begin-closes its Producer (records still in the
// builder) — the BeginClose command queues behind a head stuck on an ABORTed
// send — then fills the ring and destroys the Producer: the release is
// orphaned.  The runner notes it, but may apply it only once the head passed
// its fence, i.e. after the BeginClose: the close DRAINS (every record Ok)
// instead of the destructor's closeImmediately discarding them; then the impl
// is freed without closing the Client.  RED (mutation `true ||` on the fence
// in applyDueOrphans_): applied early, the records complete Closed.
TEST(HandleLifetime, InvokerOffOwnerOrphanedReleaseKeepsFifoRegression) {
    Watchdog wd("orphaned release behind a queued begin-close");
    FreeHook hook;
    g_testThread = std::this_thread::get_id();
    g_seamAction.store(0);
    rc::ClientConfig cfg = cfgFor(5444, rc::RunnerMode::Thread);
    cfg.nanoClock = &seamClock;
    cfg.idleMode  = rc::IdleMode::LowLatency;
    auto client = std::make_unique<rc::Client>(cfg);  // owns the embedded driver
    g_seamClient.store(client.get());
    auto gwp = std::make_unique<test_support::InvokerGateway>(
        cfg.dataDir + "/aeron", cfg.gatewayChannel, cfg.gatewayStreamId, cfg.egressChannel, cfg.egressStreamId);
    test_support::InvokerGateway& gw = *gwp;
    {
        GatewayPump pump(gw);  // the gateway answers on its own thread from here on
        client->connectAndWait(std::chrono::seconds(10));
        const auto topic = client->topic("t");
        TestCq cq(*client);
        std::optional<rc::Producer> p(client->producer(ackCfg()));  // linger 60 s: records wait in the builder
        std::vector<PendingOp> recs;
        for (int i = 0; i < 3; ++i) recs.push_back(cq.send(*p, topic, rc::Key(uint64_t{1}), kVal));
        g_seamAction.store(1);  // the runner sticks the head (forced Retry + a session frame)
        awaitRunner(200);
        auto tok = cq.issue([&](rc::Completions& q, rc::Token t) { return p->close(q, t); });
        ASSERT_TRUE(tok.accepted()) << tok.describe();  // its BeginClose queues behind the stuck head
        client->testFillCommandRing();
        p.reset();  // ring full: the release is orphaned
        awaitRunner(500);  // the hint is taken and the orphan noted while the head is stuck
        EXPECT_EQ(totalFrees(), 0);
        for (const auto& rec : recs) EXPECT_FALSE(rec.ready()) << "nothing applied before the head moved";
        g_seamAction.store(2);  // lift the back-pressure
        for (const auto& rec : recs) {
            ASSERT_TRUE(rec.waitReady()) << "a record never completed";
            EXPECT_EQ(rec.status(), OpStatus::Ok) << "discarded (the release overtook the close): " << rec.describe();
            EXPECT_EQ(cq.timesCompleted(rec.token()), 1);
        }
        ASSERT_TRUE(tok.waitReady());
        EXPECT_EQ(tok.status(), OpStatus::Ok);
        const auto backstop = std::chrono::steady_clock::now() + std::chrono::seconds(20);  // hang guard only
        while (frees(rcd::HandleKind::Producer) == 0 && std::chrono::steady_clock::now() < backstop) awaitRunner(1);
        EXPECT_EQ(frees(rcd::HandleKind::Producer), 1) << "freed without closing the Client";
        client->closeImmediately();
        pump.stop();
    }
    g_seamClient.store(nullptr);
    gwp.reset();
    client.reset();
    EXPECT_EQ(frees(rcd::HandleKind::Producer), 1);
}

// N2: a handle destroyed BEFORE its registration was adopted (its Add /
// register command queued behind a head stuck on an ABORTed send, the ring
// full): the release is orphaned, the orphan hint is taken and the scan runs
// while the impl is not held yet — only the adoption-time check notes it.
// Once the head moves, the impl is adopted, its release applied (closed /
// unsubscribed) and it is freed without closing the Client.  All four handle
// kinds.  RED (mutation: the four adoption checks removed): never freed until
// close (a Subscription would keep dispatching after its destructor).
TEST(HandleLifetime, InvokerOffOwnerReleaseOrphanedBeforeAdoptionRegression) {
    // TxnProducer is a transactional rc::Producer (Phase 2b F: HandleKind has no TxnProducer any more); the
    // enumerator values keep the per-kind gateway stream ids of the original matrix.
    enum class Kind : int { Producer = 1, TxnProducer = 2, Subscription = 3, GroupConsumer = 4 };
    for (const Kind kind : {Kind::Subscription, Kind::Producer, Kind::GroupConsumer, Kind::TxnProducer}) {
        SCOPED_TRACE("kind " + std::to_string(static_cast<int>(kind)));
        Watchdog wd("orphaned release before adoption");
        FreeHook hook;
        Rig r(cfgFor(5446 + 2 * static_cast<int>(kind)));
        r.connect();
        const auto topic = r.client->topic("t");
        r.client->testForceIngressOffers(rc::transport::AeronTransport::OfferResult::Retry, 1 << 30);
        ASSERT_TRUE(r.client->testSubmitSessionPing());
        for (int i = 0; i < 50; ++i) r.doWork();  // the head is stuck now
        auto held = [&]() -> std::size_t {
            switch (kind) {
            case Kind::Subscription:  return r.client->testSubscriptionImplsHeld();
            case Kind::Producer:
            case Kind::TxnProducer:   return r.client->testProducerCount();
            case Kind::GroupConsumer: return r.client->testGroupImplsHeld();
            }
            return 0;
        };
        const rcd::HandleKind freed = kind == Kind::Subscription  ? rcd::HandleKind::Subscription
                                      : kind == Kind::GroupConsumer ? rcd::HandleKind::GroupConsumer
                                                                    : rcd::HandleKind::Producer;
        // A worker (off the owner: every registration goes through the ring)
        // creates the handle, fills the ring and destroys the handle.
        std::thread worker([&] {
            std::optional<rc::Subscription>          sub;
            std::optional<rc::Producer>              p;
            std::optional<rc::GroupConsumer>         gc;
            std::optional<rc::Producer>              tp;
            switch (kind) {
            case Kind::Subscription:  sub.emplace(r.client->subscribe(topic, 0, {}, &noopRecords)); break;
            case Kind::Producer:      p.emplace(r.client->producer(ackCfg())); break;
            case Kind::GroupConsumer: gc.emplace(r.client->subscribeGroup("g-early", {"t"})); break;
            case Kind::TxnProducer:   tp.emplace(r.client->producer(txnCfg("tx-early"))); break;
            }
            r.client->testFillCommandRing();
            sub.reset();  // ring full: orphaned before its Add was adopted
            p.reset();
            gc.reset();
            tp.reset();
        });
        worker.join();
        for (int i = 0; i < 200; ++i) r.doWork(), r.gw.pump();  // hint taken, scan ran: not held yet
        EXPECT_EQ(held(), 0u) << "the registration is still behind the stuck head";
        EXPECT_EQ(totalFrees(), 0);
        r.client->testForceIngressOffers(rc::transport::AeronTransport::OfferResult::Accepted, 0);
        EXPECT_TRUE(r.drive([&] { return frees(freed) == 1; })) << "the orphaned release was never applied";
        EXPECT_EQ(held(), 0u);
        if (kind == Kind::Subscription) {
            EXPECT_FALSE(r.gw.unsubscribed.empty()) << "the subscription was unsubscribed";
        }
        r.client->closeImmediately();
        r.client.reset();
        EXPECT_EQ(totalFrees(), 1) << "no impl freed twice";
    }
}

// I-2: handles owned by a worker thread are destroyed while the Invoker owner
// keeps pumping (records flowing to the subscription, the group / txn
// producers registered): each release travels the command ring and is applied
// by the owner — no duty state is touched off the owner (TSan), each impl is
// freed exactly once, and the subscription's handler never runs once its
// release was applied.  RED (pre-fix, TSan): the worker applied the release
// directly — data races with the owner's duty cycle.
TEST(HandleLifetime, InvokerOffOwnerReleaseWhileOwnerPumpsRegression) {
    Watchdog wd("Invoker off-owner release while the owner pumps");
    FreeHook hook;
    Rig r(cfgFor(5440));
    r.connect();
    const auto topic = r.client->topic("t");
    std::atomic<int>  delivered{0};
    std::atomic<int>  stage{0};  // 1: handles created; 2: destroy now; 3: destroyed
    std::thread worker([&] {
        std::optional<rc::Subscription>          sub(r.client->subscribe(
            topic, 0, rc::SubscribeOptions{rc::From::earliest()},
            [&](const rc::Record&) { delivered.fetch_add(1); }));
        std::optional<rc::Producer>              tp(r.client->producer(txnCfg("tx-worker")));
        std::optional<rc::GroupConsumer>         gc(r.client->subscribeGroup("g-worker", {"t"}));
        stage.store(1);
        while (stage.load() != 2) std::this_thread::yield();
        sub.reset();  // concurrently with the owner's doWork() below
        tp.reset();
        gc.reset();
        stage.store(3);
    });
    ASSERT_TRUE(r.drive([&] { return stage.load() == 1 && !r.gw.subscribed.empty(); }));
    ASSERT_TRUE(r.drive([&] { return r.client->testSubscriptionImplsHeld() == 1 &&
                                     r.client->testProducerCount() == 1 &&
                                     r.client->testGroupImplsHeld() == 1; }));
    int64_t next = 0;
    r.gw.pushRecords(r.gw.subscribed[0], next, 4);
    next += 4;
    ASSERT_TRUE(r.drive([&] { return delivered.load() >= 4; }));
    stage.store(2);
    // Keep records flowing while the worker destroys its handles.
    ASSERT_TRUE(r.drive([&] {
        if (stage.load() != 3) {
            r.gw.pushRecords(r.gw.subscribed[0], next, 1);
            ++next;
        }
        return stage.load() == 3 && totalFrees() == 3;
    }));
    worker.join();
    EXPECT_EQ(frees(rcd::HandleKind::Subscription), 1);
    EXPECT_EQ(frees(rcd::HandleKind::Producer), 1) << "the transactional producer";
    EXPECT_EQ(frees(rcd::HandleKind::GroupConsumer), 1);
    const int after = delivered.load();
    r.gw.pushRecords(r.gw.subscribed[0], next, 3);
    for (int i = 0; i < 2'000; ++i) r.doWork(), r.gw.pump();
    EXPECT_EQ(delivered.load(), after) << "a handler ran after its release was applied";
    r.client->closeImmediately();
    r.client.reset();
    EXPECT_EQ(totalFrees(), 3) << "no impl freed twice";
}

// ===========================================================================
// Producer destructor = closeImmediately (user decision 2)
// ===========================================================================

// close() is the delivering path: records accepted before it complete Ok —
// also when the handle is destroyed right after the begin-close (a destructor
// never aborts a close in progress: the drain continues on the duty side and
// its token still completes), and the impl is freed only once drained.
TEST(HandleLifetime, ProducerDtorFlushesAcceptedRecordsRegression) {
    FreeHook hook;
    Rig r(cfgFor(5418));
    r.connect();
    const auto topic = r.client->topic("t");
    TestCq cq(*r.client);
    r.gw.replyAcks = false;  // acks held until released
    std::vector<PendingOp> recs;
    PendingOp tok;
    {
        auto p = r.client->producer(ackCfg());  // linger 60 s: the records wait in the builder
        for (int i = 0; i < 5; ++i) recs.push_back(cq.send(p, topic, rc::Key(uint64_t{1}), kVal));
        tok = cq.issue([&](rc::Completions& q, rc::Token t) { return p.close(q, t); });
        ASSERT_TRUE(tok.accepted());
    }  // destroyed with the drain pending: NOT closeImmediately
    ASSERT_TRUE(r.drive([&] { return !r.gw.held.empty(); })) << "the close flushed the builder";
    EXPECT_FALSE(tok.ready());
    EXPECT_EQ(totalFrees(), 0) << "the drain still needs the impl";
    r.gw.releaseHeld();
    ASSERT_TRUE(r.drive([&] { return tok.ready() && frees(rcd::HandleKind::Producer) == 1; }));
    EXPECT_EQ(tok.status(), OpStatus::Ok);
    for (const auto& rec : recs) {
        ASSERT_TRUE(rec.ready());
        EXPECT_EQ(rec.status(), OpStatus::Ok) << rec.describe();
        EXPECT_EQ(cq.timesCompleted(rec.token()), 1);
    }
    EXPECT_EQ(cq.timesCompleted(tok.token()), 1);
}

// Destroying an OPEN producer is closeImmediately(): every accepted record not
// yet acknowledged — in flight, in the builder — completes Closed exactly once,
// synchronously on the owner (Invoker: applied directly), and nothing more is
// sent.  RED (parent): the destructor of a copyable handle did nothing; the
// records stayed outstanding until Client close.
TEST(HandleLifetime, ProducerDtorIsCloseImmediatelyRegression) {
    FreeHook hook;
    Rig r(cfgFor(5420));
    r.connect();
    const auto topic = r.client->topic("t");
    TestCq cq(*r.client);
    r.gw.replyAcks = false;
    std::vector<PendingOp> recs;
    {
        auto p = r.client->producer(ackCfg());
        for (int i = 0; i < 3; ++i) recs.push_back(cq.send(p, topic, rc::Key(uint64_t{1}), kVal));
        p.flush();
        ASSERT_TRUE(r.drive([&] { return r.gw.held.size() == 1; }));  // in flight, unacked
        for (int i = 0; i < 2; ++i) recs.push_back(cq.send(p, topic, rc::Key(uint64_t{1}), kVal));  // builder
    }
    for (const auto& rec : recs) {
        ASSERT_TRUE(rec.ready()) << "completed by the destructor itself";
        EXPECT_EQ(rec.status(), OpStatus::Closed);
        EXPECT_EQ(cq.timesCompleted(rec.token()), 1);
    }
    const auto producedBefore = r.gw.produced.size();
    r.gw.releaseHeld();  // a late ack completes nothing
    ASSERT_TRUE(r.drive([&] { return frees(rcd::HandleKind::Producer) == 1; }));
    for (const auto& rec : recs) EXPECT_EQ(cq.timesCompleted(rec.token()), 1);
    EXPECT_EQ(r.gw.produced.size(), producedBefore) << "nothing sent after the destructor";
}

// closeImmediately() (the abort) completes every outstanding record AND a
// pending begin-close token Closed, exactly once; the destructor afterwards
// changes nothing (the producer is no longer open) and frees it.
TEST(HandleLifetime, ProducerAbortFailsClosedRegression) {
    FreeHook hook;
    Rig r(cfgFor(5422));
    r.connect();
    const auto topic = r.client->topic("t");
    TestCq cq(*r.client);
    r.gw.replyAcks = false;
    std::vector<PendingOp> recs;
    PendingOp tok;
    {
        auto p = r.client->producer(ackCfg());
        for (int i = 0; i < 4; ++i) recs.push_back(cq.send(p, topic, rc::Key(uint64_t{1}), kVal));
        tok = cq.issue([&](rc::Completions& q, rc::Token t) { return p.close(q, t); });
        ASSERT_TRUE(r.drive([&] { return !r.gw.held.empty(); }));  // the drain is in flight
        EXPECT_EQ(p.closeImmediately(), OpResult::Accepted);
        ASSERT_TRUE(tok.ready());
        EXPECT_EQ(tok.status(), OpStatus::Closed);
        for (const auto& rec : recs) {
            ASSERT_TRUE(rec.ready());
            EXPECT_EQ(rec.status(), OpStatus::Closed);
        }
        EXPECT_EQ(p.trySend(topic, rc::Key(uint64_t{1}), kVal), SendResult::Closed);
    }
    r.gw.releaseHeld();
    ASSERT_TRUE(r.drive([&] { return frees(rcd::HandleKind::Producer) == 1; }));
    EXPECT_EQ(cq.timesCompleted(tok.token()), 1);
    for (const auto& rec : recs) EXPECT_EQ(cq.timesCompleted(rec.token()), 1);
}

// A BatchBuilder borrows its producer (spec §3.1 rule 1): used after the
// Producer was destroyed (while the duty side still holds the impl) it throws
// instead of touching a producer nobody owns.
TEST(HandleLifetime, BatchBuilderOutlivingItsProducerIsCaughtRegression) {
    FreeHook hook;
    Rig r(cfgFor(5424));
    r.connect();
    const auto topic = r.client->topic("t");
    std::optional<rc::Producer> p(r.client->producer(ackCfg()));
    {
        auto b = p->batch(topic, rc::Key(uint64_t{1}));
        b.add(kVal);
        p.reset();
        EXPECT_THROW((void)b.send(), std::logic_error);
    }   // b dies while the duty side still holds the impl (its destructor reads the detach flag); once the
        // duty side frees the impl, any use of b - its destructor included - touches freed memory
    ASSERT_TRUE(r.drive([&] { return frees(rcd::HandleKind::Producer) == 1; }));
}

// #372 (test only): an Invoker owner's blocking send behind a held transactional Tier-2 batch completes once the
// broker answers AddPartitions.  The send pumps the client duty cycle itself; the answer comes from the gateway
// thread, as from a real broker.
TEST(TxnInvokerRegression, BlockingSendBehindAHeldTier2BatchCompletesWhenTheBrokerAnswersRegression) {
    Rig r(cfgFor(5448));
    r.connect();
    r.gw.initPidReply = true;
    r.gw.replyTxn     = true;
    r.gw.idempotence  = true;  // the broker model keeps the log
    auto tp = r.client->producer(txnCfg("tx-372"));
    TestCq cq(*r.client);
    auto init = cq.issue([&](rc::Completions& q, rc::Token t) { return tp.initTransactions(q, t); });
    ASSERT_TRUE(r.drive([&] { return init.ready(); }));
    ASSERT_TRUE(init.ok()) << init.describe();
    tp.beginTransaction();
    const auto topic = r.client->topic("t");
    {
        GatewayPump pump(r.gw);
        auto b = tp.batch(topic, rc::Key(uint64_t{1}));
        b.add(kVal);
        ASSERT_EQ(b.send(), OpResult::Accepted);  // held until AddPartitions is answered
        const auto t0 = std::chrono::steady_clock::now();
        tp.send(topic, rc::Key(uint64_t{1}), kVal);  // blocking, behind the held batch
        EXPECT_LT(std::chrono::steady_clock::now() - t0, std::chrono::seconds(10));
        auto commit = cq.issue([&](rc::Completions& q, rc::Token t) { return tp.commitTransaction(q, t); });
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
        while (!commit.ready() && std::chrono::steady_clock::now() < deadline) r.doWork();
        pump.stop();
        ASSERT_TRUE(commit.ready()) << "the commit never completed";
        EXPECT_TRUE(commit.ok()) << commit.describe();
    }
    EXPECT_EQ(r.gw.endTxnCommits, (std::vector<uint8_t>{1}));
    EXPECT_EQ(r.gw.committedValues(0).size(), 2u) << "both records committed";
    r.client->close();
}

// ===========================================================================
// Client close() drains, closeImmediately() / the destructor discard
// ===========================================================================

// close() first drains every producer — records in a builder (linger 60 s) are
// flushed and acknowledged Ok, also those of a producer created right before
// close() (its registration still in the command ring: the drain command is
// applied behind it) — then tears down.  RED (parent): close() completed them
// Closed without sending.
TEST(HandleLifetime, ClientCloseDrainsProducersBeforeTeardownRegression) {
    FreeHook hook;
    Rig r(cfgFor(5426));
    r.connect();
    const auto topic = r.client->topic("t");
    TestCq cq(*r.client);
    auto p = r.client->producer(ackCfg());
    std::vector<PendingOp> recs;
    for (int i = 0; i < 3; ++i) recs.push_back(cq.send(p, topic, rc::Key(uint64_t{1}), kVal));
    ASSERT_TRUE(r.drive([&] { return r.client->testProducerCount() == 1; }));
    auto late = r.client->producer(ackCfg());  // registration not yet adopted
    recs.push_back(cq.send(late, topic, rc::Key(uint64_t{2}), kVal));
    EXPECT_TRUE(r.gw.produced.empty()) << "nothing flushed before close()";
    {
        GatewayPump pump(r.gw);  // the gateway answers while close() pumps the Client
        r.client->close();
        pump.stop();
    }
    for (const auto& rec : recs) {
        ASSERT_TRUE(rec.ready());
        EXPECT_EQ(rec.status(), OpStatus::Ok) << rec.describe();
        EXPECT_EQ(cq.timesCompleted(rec.token()), 1);
    }
    EXPECT_EQ(p.trySend(topic, rc::Key(uint64_t{1}), kVal), SendResult::Closed);
}

// The producer-registered-right-before-close() case on its own: no other
// producer keeps the drain pending, so only ring order (the drain command
// queued BEHIND the registration) gets its record drained.  RED (mutation):
// applying the drain directly on the Invoker owner settles it before the
// registration is adopted — the record completes Closed.
TEST(HandleLifetime, ClientCloseDrainsAProducerRegisteredJustBeforeRegression) {
    Rig r(cfgFor(5436));
    r.connect();
    const auto topic = r.client->topic("t");
    TestCq cq(*r.client);
    auto late = r.client->producer(ackCfg());  // registration still in the command ring
    auto rec  = cq.send(late, topic, rc::Key(uint64_t{2}), kVal);
    ASSERT_TRUE(rec.accepted());
    {
        GatewayPump pump(r.gw);
        r.client->close();
        pump.stop();
    }
    ASSERT_TRUE(rec.ready());
    EXPECT_EQ(rec.status(), OpStatus::Ok) << rec.describe();
    EXPECT_EQ(cq.timesCompleted(rec.token()), 1);
}

// closeImmediately() — and the Client destructor — discard instead: every
// accepted record completes Closed exactly once, nothing is flushed.
TEST(HandleLifetime, ClientCloseImmediatelyAndDestructorDiscardRegression) {
    for (const bool viaDtor : {false, true}) {
        SCOPED_TRACE(viaDtor ? "destructor" : "closeImmediately");
        FreeHook hook;
        Rig r(cfgFor(viaDtor ? 5428 : 5430));
        r.connect();
        const auto topic = r.client->topic("t");
        TestCq cq(*r.client);  // outlives the Client: it then holds only Closed completions
        auto p = r.client->producer(ackCfg());
        std::vector<PendingOp> recs;
        for (int i = 0; i < 3; ++i) recs.push_back(cq.send(p, topic, rc::Key(uint64_t{1}), kVal));
        ASSERT_TRUE(r.drive([&] { return r.client->testProducerCount() == 1; }));
        if (viaDtor) r.client.reset(); else r.client->closeImmediately();
        for (const auto& rec : recs) {
            ASSERT_TRUE(rec.ready());
            EXPECT_EQ(rec.status(), OpStatus::Closed);
            EXPECT_EQ(cq.timesCompleted(rec.token()), 1);
        }
        EXPECT_TRUE(r.gw.produced.empty()) << "nothing flushed";
    }
}

// close() closes every transactional producer first: an open broker
// transaction is aborted (EndTxn(abort) sent and answered) before the
// teardown.  closeImmediately() sends no EndTxn (discard locally).
TEST(HandleLifetime, ClientCloseAbortsOpenTransactionsRegression) {
    for (const bool immediately : {false, true}) {
        SCOPED_TRACE(immediately ? "closeImmediately" : "close");
        Rig r(cfgFor(immediately ? 5432 : 5434));
        r.connect();
        r.gw.initPidReply = true;
        r.gw.replyTxn     = true;
        auto tp = r.client->producer(txnCfg("tx-client-close"));
        TestCq cq(*r.client);
        auto init = cq.issue([&](rc::Completions& q, rc::Token t) { return tp.initTransactions(q, t); });
        ASSERT_TRUE(r.drive([&] { return init.ready(); }));
        ASSERT_TRUE(init.ok()) << init.describe();
        tp.beginTransaction();
        tp.send(r.client->topic("t"), rc::Key(uint64_t{1}), kVal);
        ASSERT_TRUE(r.drive([&] { return r.gw.addPartitions == 1 && !r.gw.produced.empty(); }));
        ASSERT_TRUE(tp.transactionActive());
        if (immediately) {
            r.client->closeImmediately();
            EXPECT_EQ(r.gw.endTxns, 0) << "discarded locally";
        } else {
            GatewayPump pump(r.gw);
            r.client->close();
            pump.stop();
            ASSERT_EQ(r.gw.endTxnCommits.size(), 1u) << "the open transaction was ended";
            EXPECT_EQ(r.gw.endTxnCommits[0], 0) << "with EndTxn(abort)";
        }
        EXPECT_EQ(tp.commitTransaction(rc::CompletionHandler{[](void*, const rc::Completion&,
                                                                std::span<const uint8_t>) noexcept {},
                                                            nullptr},
                                       1),
                  OpResult::Closed);
    }
}
