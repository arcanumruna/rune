// tests/subscriber/SubscriberHubCommandTest.cpp
//
// F8a — SubscriberHub is duty-thread owned (no mutex, no atomics).  App-thread
// operations reach it only as HubCommands; duty-side state reaches the app only
// through each subscription's StatusRing.  These tests drive the hub directly
// (the test thread plays the duty thread unless a test says otherwise) and the
// last one runs a real app thread against a real duty thread over an M2O
// command ring, for TSan.

#include <rune/client/subscriber/SubscriberHub.h>
#include <rune/client/subscriber/Subscription.h>
#include <rune/client/codec/DataCodec.h>
#include <rune/client/detail/HubCommand.h>
#include <rune/client/detail/M2ORingBuffer.h>
#include <rune/protocol/RecordBatch.h>

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstring>
#include <memory>
#include <thread>
#include <vector>

namespace rc  = rune::client;
namespace rcd = rune::client::detail;
using rune::client::subscriber::SubscriberHub;
using rune::client::subscriber::SubscribeRequest;

namespace {

constexpr int64_t kSession = 7;
constexpr int32_t kTopic   = 3;
constexpr int32_t kPart    = 0;

struct Frame { std::vector<uint8_t> bytes; };

struct CaptureSend {
    std::vector<Frame> frames;
    SubscriberHub::SendFn fn() {
        return [this](int32_t, const uint8_t* b, std::size_t n) {
            frames.push_back({std::vector<uint8_t>(b, b + n)});
            return true;
        };
    }
    int countUnsubscribe(int64_t subId) const {
        int n = 0;
        for (const auto& f : frames) {
            auto u = rc::codec::decodeUnsubscribe(f.bytes.data(), f.bytes.size());
            if (u && u->subscriptionId == subId) ++n;
        }
        return n;
    }
    int countSubscribe() const {
        int n = 0;
        for (const auto& f : frames)
            if (rc::codec::decodeSubscribe(f.bytes.data(), f.bytes.size())) ++n;
        return n;
    }
};

// A sink that only RECORDS commands — proves the handle writes no shared state:
// nothing changes on the duty side until the test applies the command.
struct CapturingSink {
    std::vector<rcd::HubCommand> cmds;
    int calls = 0;
    static rcd::HubSubmit fn(void* ctx, const rcd::HubCommand& c) {
        auto* self = static_cast<CapturingSink*>(ctx);
        ++self->calls;
        self->cmds.push_back(c);
        return rcd::HubSubmit::Accepted;
    }
};

// F8c C7: the test owns the impl's memory (a bare hub holds raw pointers; a
// handle built on it never frees it — no duty side ever detaches).
std::unique_ptr<rc::SubscriptionImpl> makeImpl(int64_t corrId, int* delivered = nullptr) {
    auto impl = std::make_unique<rc::SubscriptionImpl>();
    impl->correlationId = corrId;
    impl->topicId       = kTopic;
    impl->partition     = kPart;
    impl->handler       = [delivered](const rc::Record&) { if (delivered) ++*delivered; };
    return impl;
}

rcd::HubCommand addCmd(rc::SubscriptionImpl* impl) {
    auto req = SubscriberHub::makeSubscribeRequest(
        impl, kSession, /*partitionKnown=*/1, /*fromOffset=*/-2, /*iso=*/0, {},
        std::chrono::steady_clock::now() + std::chrono::hours(1));
    rcd::HubCommand c;
    c.op     = rcd::HubCommand::Op::SubscribeAdd;
    c.handle = reinterpret_cast<uint64_t>(req.release());
    return c;
}

rc::codec::SubscribeResponseView bindRsp(int64_t corrId, int64_t subId) {
    rc::codec::SubscribeResponseView r;
    r.sessionId = kSession;
    r.correlationId = corrId;
    r.subscriptionId = subId;
    return r;
}

// A Records view over `storage` (count records at baseOffset).
rc::codec::RecordsView records(std::vector<uint8_t>& storage, int64_t subId,
                               int64_t baseOffset, int32_t count) {
    rune::protocol::RecordBatchBuilder b(baseOffset, 1'000'000);
    for (int i = 0; i < count; ++i) b.append("k", "v", 1'000'000 + i);
    storage = b.build();
    rc::codec::RecordsView rv;
    rv.subscriptionId = subId;
    rv.topicId        = kTopic;
    rv.partition      = kPart;
    rv.baseOffset     = baseOffset;
    rv.recordBatch    = std::span<const uint8_t>(storage);
    rv.recordCount    = count;
    return rv;
}

int g_subFrees = 0;  // F8c C7: impl frees counted by rcd::testOnImplFree (test thread only)

} // namespace

// SubscribeAdd is applied by the duty thread: it registers the pending entry
// and sends the Subscribe frame — the app thread only built the request.
TEST(SubscriberHubCommand, SubscribeAddRegistersAndSendsOnDutyThreadRegression) {
    SubscriberHub hub;
    CaptureSend   send;
    auto impl = makeImpl(11);
    EXPECT_EQ(hub.pendingCount(), 0u);
    hub.applyCommand(addCmd(impl.get()), 0, kSession, send.fn());
    EXPECT_EQ(hub.pendingCount(), 1u);
    EXPECT_EQ(send.countSubscribe(), 1);
    EXPECT_TRUE(hub.hasPending());
}

// Duty-owned state (subscriptionId, position) reaches the handle only through
// the status ring.
TEST(SubscriberHubCommand, SubscriptionStatePublishedThroughStatusRingRegression) {
    SubscriberHub hub;
    CaptureSend   send;
    CapturingSink sink;
    int delivered = 0;
    auto impl = makeImpl(12, &delivered);
    rc::Subscription sub(impl.get(), &CapturingSink::fn, &sink);

    hub.applyCommand(addCmd(impl.get()), 0, kSession, send.fn());
    EXPECT_EQ(sub.subscriptionId(), 0);
    EXPECT_TRUE(sub.active());

    hub.onSubscribeResponse(bindRsp(12, 77), 0, kSession, send.fn());
    EXPECT_EQ(sub.subscriptionId(), 77);
    EXPECT_EQ(sub.position(), 0);

    std::vector<uint8_t> st;
    hub.onRecords(records(st, 77, 10, 3));
    EXPECT_EQ(delivered, 3);
    EXPECT_EQ(sub.position(), 13);
    EXPECT_TRUE(sub.active());
    EXPECT_EQ(sink.calls, 0) << "reading state must never submit anything";
}

// unsubscribe() is a command, not a shared write: until the duty thread applies
// it, the hub (and the duty-owned impl fields) are untouched.
TEST(SubscriberHubCommand, UnsubscribeIsACommandNotASharedWriteRegression) {
    SubscriberHub hub;
    CaptureSend   send;
    CapturingSink sink;
    auto impl = makeImpl(13);
    rc::Subscription sub(impl.get(), &CapturingSink::fn, &sink);
    hub.applyCommand(addCmd(impl.get()), 0, kSession, send.fn());
    hub.onSubscribeResponse(bindRsp(13, 88), 0, kSession, send.fn());
    ASSERT_EQ(hub.activeCount(), 1u);

    sub.unsubscribe();
    EXPECT_FALSE(sub.active()) << "the handle reports inactive at once (handle-local)";
    ASSERT_EQ(sink.cmds.size(), 1u);
    EXPECT_EQ(sink.cmds[0].op, rcd::HubCommand::Op::Unsubscribe);
    // F8c C3: identified by the impl address (the correlationId is duty-
    // assigned and never read by the app thread).
    EXPECT_EQ(sink.cmds[0].handle, reinterpret_cast<uint64_t>(impl.get()));
    EXPECT_EQ(sink.cmds[0].a, 0);
    EXPECT_EQ(hub.activeCount(), 1u) << "app thread touched no hub state";
    EXPECT_TRUE(impl->active)        << "app thread touched no duty-owned impl field";

    hub.applyCommand(sink.cmds[0], /*unsubCorrId=*/500, kSession, send.fn());
    EXPECT_EQ(hub.activeCount(), 0u);
    EXPECT_FALSE(impl->active);
    EXPECT_TRUE(impl->status.closed());
    EXPECT_EQ(send.countUnsubscribe(88), 1);

    sub.unsubscribe();  // idempotent: no second command
    EXPECT_EQ(sink.calls, 1);
}

// CB-5 A2 at hub level: unsubscribe applied while the subscribe is still
// pending -> the late SubscribeResponse is not routed and sends the real-id
// Unsubscribe.
TEST(SubscriberHubCommand, UnsubscribeBeforeResponseSendsLateUnsubscribeRegression) {
    SubscriberHub hub;
    CaptureSend   send;
    int delivered = 0;
    auto impl = makeImpl(14, &delivered);
    hub.applyCommand(addCmd(impl.get()), 0, kSession, send.fn());

    rcd::HubCommand u;
    u.op = rcd::HubCommand::Op::Unsubscribe;
    u.a  = 14;
    hub.applyCommand(u, 600, kSession, send.fn());
    EXPECT_TRUE(impl->status.closed());

    hub.onSubscribeResponse(bindRsp(14, 99), /*unsubCorrId=*/601, kSession, send.fn());
    EXPECT_EQ(hub.activeCount(), 0u);
    EXPECT_EQ(hub.pendingCount(), 0u);
    EXPECT_EQ(send.countUnsubscribe(99), 1);

    std::vector<uint8_t> st;
    hub.onRecords(records(st, 99, 0, 2));
    EXPECT_EQ(delivered, 0);
}

// The app never drains its status ring while many Records frames arrive; the
// ring refuses ordinary updates near-full, but the FINAL snapshot always fits:
// after the subscription finishes, position() is exact.
TEST(SubscriberHubCommand, StatusRingNearFullNeverLosesFinalPositionRegression) {
    SubscriberHub hub;
    CaptureSend   send;
    CapturingSink sink;
    auto impl = makeImpl(15);
    rc::Subscription sub(impl.get(), &CapturingSink::fn, &sink);
    hub.applyCommand(addCmd(impl.get()), 0, kSession, send.fn());
    hub.onSubscribeResponse(bindRsp(15, 5), 0, kSession, send.fn());

    std::vector<uint8_t> st;
    constexpr int kFrames = 500;  // far more snapshots than the ring holds
    for (int i = 0; i < kFrames; ++i) hub.onRecords(records(st, 5, i, 1));
    EXPECT_TRUE(impl->statusDirty) << "the near-full ring must have refused updates";

    hub.deactivateAll();  // the Client-close path
    EXPECT_FALSE(sub.active());
    EXPECT_EQ(sub.position(), kFrames);
    EXPECT_EQ(sub.subscriptionId(), 5);
}

// fix1 I4: a refused (dirty) snapshot is retried by the duty cycle as soon as
// the app has drained (progress-based) — not by the 100 ms sweep.
TEST(SubscriberHubCommand, DirtyStatusRepublishedEveryDutyCycleRegression) {
    SubscriberHub hub;
    CaptureSend   send;
    CapturingSink sink;
    auto impl = makeImpl(16);
    rc::Subscription sub(impl.get(), &CapturingSink::fn, &sink);
    hub.applyCommand(addCmd(impl.get()), 0, kSession, send.fn());
    hub.onSubscribeResponse(bindRsp(16, 6), 0, kSession, send.fn());

    std::vector<uint8_t> st;
    constexpr int kFrames = 300;
    for (int i = 0; i < kFrames; ++i) hub.onRecords(records(st, 6, i, 1));
    ASSERT_EQ(hub.dirtyCount(), 1u);
    EXPECT_EQ(hub.republishDirty(), 0) << "app has not drained: still refused";
    EXPECT_LT(sub.position(), kFrames) << "stale until the duty cycle republishes";

    EXPECT_EQ(hub.republishDirty(), 1) << "the app drained: next duty cycle publishes";
    EXPECT_EQ(hub.dirtyCount(), 0u);
    EXPECT_EQ(sub.position(), kFrames);
}

// Once the duty side finished a subscription (here: Client close), its handle
// never touches the sink (i.e. the possibly-destroyed Client) again.
TEST(SubscriberHubCommand, FinishedHandleNeverTouchesTheClientRegression) {
    SubscriberHub hub;
    CaptureSend   send;
    CapturingSink sink;
    // F8c C7: shared by the handle and the hub — the Client close hand-back
    // (handBackAll) detaches the duty share; the handle then frees it.
    int freed = 0;
    rcd::testOnImplFree = [](rcd::HandleKind, const void*) noexcept { ++g_subFrees; };
    g_subFrees = 0;
    rc::SubscriptionImpl* impl = makeImpl(17).release();
    {
        rc::Subscription sub(impl, &CapturingSink::fn, &sink);
        hub.applyCommand(addCmd(impl), 0, kSession, send.fn());
        hub.onSubscribeResponse(bindRsp(17, 8), 0, kSession, send.fn());
        hub.deactivateAll();
        hub.handBackAll([](rc::SubscriptionImpl* s) noexcept {
            if (s->status.detachProducer()) delete s;
        });
        EXPECT_FALSE(sub.active());
        sub.unsubscribe();
        EXPECT_EQ(sink.calls, 0);
        freed = g_subFrees;
    }  // dtor: still no call; the handle frees the impl (the duty share is gone)
    EXPECT_EQ(sink.calls, 0);
    EXPECT_EQ(freed, 0);
    EXPECT_EQ(g_subFrees, 1);
    rcd::testOnImplFree = nullptr;
}

// Subscribe deadline expiry finishes the subscription for the app.
TEST(SubscriberHubCommand, TimedOutSubscribeFinishesHandleRegression) {
    SubscriberHub hub;
    CaptureSend   send;
    CapturingSink sink;
    auto impl = makeImpl(18);
    rc::Subscription sub(impl.get(), &CapturingSink::fn, &sink);
    auto req = SubscriberHub::makeSubscribeRequest(
        impl.get(), kSession, 1, -2, 0, {}, std::chrono::steady_clock::now());
    hub.adoptSubscribe(std::move(req), send.fn());
    hub.sweepTimedOut(std::chrono::steady_clock::now() + std::chrono::seconds(1));
    EXPECT_EQ(hub.pendingCount(), 0u);
    EXPECT_FALSE(sub.active());
}

// ---------------------------------------------------------------------------
// fix1 C1(a): a handle used ON the duty thread (from inside its own handler)
// applies the unsubscribe directly; the hub defers the erase / ring close to
// the end of the dispatch.
// ---------------------------------------------------------------------------
namespace {
struct DirectSubSink {
    SubscriberHub* hub;
    CaptureSend*   send;
    int            calls = 0;
    static rcd::HubSubmit fn(void* ctx, const rcd::HubCommand& c) {
        auto* self = static_cast<DirectSubSink*>(ctx);
        ++self->calls;
        rcd::HubCommand cmd = c;
        // F8c C7: a handle's release carries its destructor semantics
        // (unsubscribe), as the Client applies it.
        if (cmd.op == rcd::HubCommand::Op::HandleRelease) cmd.op = rcd::HubCommand::Op::Unsubscribe;
        self->hub->applyCommand(cmd, 700 + self->calls, kSession, self->send->fn());
        return rcd::HubSubmit::Accepted;
    }
};
} // namespace

TEST(SubscriberHubCommand, UnsubscribeInsideHandlerStopsTheBatchAndDefersFinishRegression) {
    SubscriberHub hub;
    CaptureSend   send;
    DirectSubSink ds{&hub, &send};
    auto impl = makeImpl(30);
    // dutyThread = this thread: the handle takes the duty path, like a call
    // from inside a callback on the runner.
    rc::Subscription sub(impl.get(), &DirectSubSink::fn, &ds, std::this_thread::get_id());
    int delivered = 0;
    bool finishedInsideHandler = true;
    impl->handler = [&](const rc::Record&) {
        ++delivered;
        sub.unsubscribe();
        finishedInsideHandler = sub.finished();
    };
    hub.applyCommand(addCmd(impl.get()), 0, kSession, send.fn());
    hub.onSubscribeResponse(bindRsp(30, 40), 0, kSession, send.fn());

    std::vector<uint8_t> st;
    hub.onRecords(records(st, 40, 0, 5));
    EXPECT_EQ(delivered, 1) << "no further record of the batch after the in-handler unsubscribe";
    EXPECT_FALSE(finishedInsideHandler) << "finished() must not be true while a handler runs";
    EXPECT_TRUE(sub.finished());
    EXPECT_EQ(hub.activeCount(), 0u);
    EXPECT_EQ(send.countUnsubscribe(40), 1);
    EXPECT_EQ(sub.position(), 1) << "final position = last delivered + 1";

    hub.onRecords(records(st, 40, 5, 2));
    EXPECT_EQ(delivered, 1) << "no callbacks after finished()";
}

// Destroying the Subscription from inside its own handler must not free the
// running std::function: F8c C7 — the hub keeps referencing the impl until the
// dispatch ends (release() refuses while it runs), and only the duty side's
// reap afterwards lets the last share go.
TEST(SubscriberHubCommand, DestroyHandleInsideHandlerKeepsImplAliveUntilDispatchEndsRegression) {
    SubscriberHub hub;
    CaptureSend   send;
    DirectSubSink ds{&hub, &send};
    g_subFrees = 0;
    rcd::testOnImplFree = [](rcd::HandleKind, const void*) noexcept { ++g_subFrees; };
    rc::SubscriptionImpl* impl = makeImpl(31).release();  // shared: handle + hub (§3.3)
    auto sub = std::make_unique<rc::Subscription>(impl, &DirectSubSink::fn, &ds,
                                                  std::this_thread::get_id());
    int  freesInHandler    = -1;
    bool releasedInHandler = true;
    int  delivered         = 0;
    impl->handler = [&](const rc::Record&) {
        ++delivered;
        sub.reset();  // dtor -> release (unsubscribe applied directly on this thread)
        freesInHandler    = g_subFrees;
        releasedInHandler = hub.release(impl);  // the duty side may not forget it yet
    };
    hub.applyCommand(addCmd(impl), 0, kSession, send.fn());
    hub.onSubscribeResponse(bindRsp(31, 41), 0, kSession, send.fn());

    std::vector<uint8_t> st;
    hub.onRecords(records(st, 41, 0, 3));
    EXPECT_EQ(delivered, 1);
    EXPECT_EQ(freesInHandler, 0) << "the impl (and the running handler) must outlive the handle";
    EXPECT_FALSE(releasedInHandler) << "a running handler keeps the hub's reference";
    EXPECT_EQ(g_subFrees, 0);
    // The Client's end-of-cycle reap: nothing references it any more.
    ASSERT_TRUE(hub.release(impl));
    EXPECT_EQ(hub.activeCount(), 0u);
    ASSERT_TRUE(impl->status.detachProducer()) << "the handle let go first: the duty side frees";
    delete impl;
    EXPECT_EQ(g_subFrees, 1);
    rcd::testOnImplFree = nullptr;
}

// ---------------------------------------------------------------------------
// Real threads: an app thread subscribes / reads position / unsubscribes while
// a duty thread drains a real M2O command ring and dispatches Records.  The
// only shared memory is the two rings (command ring, status ring) — TSan proves
// there is no other.
// ---------------------------------------------------------------------------
namespace {

struct RingChannel {
    static constexpr int32_t kCap = 64 * 1024;
    std::unique_ptr<uint8_t[]> mem{new uint8_t[kCap + rcd::M2ORingBuffer::trailerLength()]()};
    rcd::M2ORingBuffer ring{mem.get(), static_cast<std::size_t>(kCap + rcd::M2ORingBuffer::trailerLength())};

    static rcd::HubSubmit fn(void* ctx, const rcd::HubCommand& c) {
        auto* self = static_cast<RingChannel*>(ctx);
        while (!self->ring.writeWith(1, sizeof(c), [&](aeron::concurrent::AtomicBuffer& b,
                                                      aeron::util::index_t at) {
            std::memcpy(b.buffer() + at, &c, sizeof(c));
        })) {
            std::this_thread::yield();  // full: the duty thread drains it
        }
        return rcd::HubSubmit::Accepted;
    }

    template <class F>
    int drain(F&& apply) {
        return ring.read([&](int32_t, aeron::concurrent::AtomicBuffer& b, aeron::util::index_t at,
                             aeron::util::index_t) {
            rcd::HubCommand c;
            std::memcpy(&c, b.buffer() + at, sizeof(c));
            apply(c);
        });
    }
};

} // namespace

TEST(SubscriberHubCommand, AppAndDutyThreadsShareOnlyRingsRegression) {
    SubscriberHub hub;          // duty-thread owned
    RingChannel   ch;
    std::atomic<bool> appDone{false};   // test-harness flag only
    constexpr int kMinRecords = 100;
    std::atomic<int> delivered{0};      // test-harness observation only
    int deliveredAfterFinish = 0;       // duty thread only (read after join)

    auto impl = makeImpl(21);
    impl->handler = [&](const rc::Record&) { delivered.fetch_add(1, std::memory_order_relaxed); };

    // The duty thread keeps dispatching Records the whole time, so the app's
    // reads (position/subscriptionId) and its unsubscribe() overlap live
    // dispatch — any shared write would be a TSan-visible race.
    std::thread duty([&] {
        CaptureSend send;
        std::vector<uint8_t> st;
        bool bound = false;
        int  next  = 0;
        while (!appDone.load(std::memory_order_acquire)) {
            ch.drain([&](const rcd::HubCommand& c) { hub.applyCommand(c, 900, kSession, send.fn()); });
            if (!bound && hub.pendingCount() == 1) {
                hub.onSubscribeResponse(bindRsp(21, 31), 0, kSession, send.fn());
                bound = true;
            }
            if (bound) {
                const int before = delivered.load(std::memory_order_relaxed);
                hub.onRecords(records(st, 31, next++, 1));
                if (impl->status.closed() && delivered.load(std::memory_order_relaxed) != before) {
                    ++deliveredAfterFinish;
                }
            }
            hub.sweepTimedOut(std::chrono::steady_clock::now());
        }
    });

    // App thread (this one): subscribe, read state during live dispatch, unsubscribe.
    auto sub = std::make_unique<rc::Subscription>(impl.get(), &RingChannel::fn, &ch);
    (void)RingChannel::fn(&ch, addCmd(impl.get()));
    while (sub->position() < kMinRecords) std::this_thread::yield();
    EXPECT_EQ(sub->subscriptionId(), 31);
    EXPECT_TRUE(sub->active());
    sub->unsubscribe();
    EXPECT_FALSE(sub->active());
    while (!impl->status.closed()) std::this_thread::yield();  // duty applied the command
    const int64_t finalPos = sub->position();
    appDone.store(true, std::memory_order_release);
    duty.join();

    EXPECT_GE(delivered.load(), kMinRecords);
    EXPECT_EQ(deliveredAfterFinish, 0) << "no dispatch once the duty thread finished it";
    EXPECT_EQ(finalPos, delivered.load()) << "final snapshot == last delivered + 1";
    EXPECT_EQ(hub.activeCount(), 0u);
}

// ---------------------------------------------------------------------------
// F8a fix round 2 — REQUIRED 1: Unsubscribe frames go straight to the
// transport (direct send) and are retried by the duty cycle when refused.
// ---------------------------------------------------------------------------
namespace {
struct RefusingTransport {
    int refuseNext = 0;          // refuse (Retry) this many offers, then accept
    int fatalNext  = 0;          // then reject this many as never deliverable (Fatal)
    int fatals     = 0;
    std::vector<Frame> accepted;
    static rcd::SendResult fn(void* ctx, const uint8_t* b, std::size_t n) {
        auto* self = static_cast<RefusingTransport*>(ctx);
        if (self->refuseNext > 0) { --self->refuseNext; return rcd::SendResult::Retry; }
        if (self->fatalNext > 0) { --self->fatalNext; ++self->fatals; return rcd::SendResult::Fatal; }
        self->accepted.push_back({std::vector<uint8_t>(b, b + n)});
        return rcd::SendResult::Accepted;
    }
    int subscribes() const {
        int c = 0;
        for (const auto& f : accepted) {
            if (rc::codec::decodeSubscribe(f.bytes.data(), f.bytes.size())) ++c;
        }
        return c;
    }
    int unsubscribesFor(int64_t subId) const {
        int c = 0;
        for (const auto& f : accepted) {
            auto u = rc::codec::decodeUnsubscribe(f.bytes.data(), f.bytes.size());
            if (u && u->subscriptionId == subId) ++c;
        }
        return c;
    }
};
} // namespace

TEST(SubscriberHubCommand, RefusedUnsubscribeRetriedNextDutyCycleRegression) {
    SubscriberHub hub;
    CaptureSend   send;
    RefusingTransport tx;
    hub.setDirectSend(&RefusingTransport::fn, &tx);
    auto impl = makeImpl(50);
    hub.applyCommand(addCmd(impl.get()), 0, kSession, send.fn());
    hub.onSubscribeResponse(bindRsp(50, 60), 0, kSession, send.fn());

    tx.refuseNext = 2;
    rcd::HubCommand u;
    u.op = rcd::HubCommand::Op::Unsubscribe;
    u.a  = 50;
    hub.applyCommand(u, 900, kSession, send.fn());
    EXPECT_TRUE(impl->status.closed()) << "dispatch stops independent of the wire";
    EXPECT_EQ(hub.pendingUnsubscribes(), 1u);
    EXPECT_EQ(send.countUnsubscribe(60), 0) << "never via the command ring";

    std::vector<uint8_t> st;
    int before = 0;
    impl->handler = [&](const rc::Record&) { ++before; };
    hub.onRecords(records(st, 60, 0, 2));
    EXPECT_EQ(before, 0) << "a pending-unsubscribe entry never dispatches";

    EXPECT_EQ(hub.retryPendingUnsubscribes(kSession), 0);  // refused again
    EXPECT_EQ(hub.retryPendingUnsubscribes(kSession), 1);
    EXPECT_EQ(tx.unsubscribesFor(60), 1);
    EXPECT_EQ(hub.pendingUnsubscribes(), 0u);
    EXPECT_EQ(hub.activeCount(), 0u);
}

TEST(SubscriberHubCommand, PendingUnsubscribeDroppedWhenTheSessionChangesRegression) {
    SubscriberHub hub;
    CaptureSend   send;
    RefusingTransport tx;
    hub.setDirectSend(&RefusingTransport::fn, &tx);
    auto impl = makeImpl(51);
    hub.applyCommand(addCmd(impl.get()), 0, kSession, send.fn());
    hub.onSubscribeResponse(bindRsp(51, 61), 0, kSession, send.fn());
    tx.refuseNext = 1;
    rcd::HubCommand u;
    u.op = rcd::HubCommand::Op::Unsubscribe;
    u.a  = 51;
    hub.applyCommand(u, 901, kSession, send.fn());
    ASSERT_EQ(hub.pendingUnsubscribes(), 1u);
    hub.retryPendingUnsubscribes(kSession + 1);  // a new session: the old one is gone
    EXPECT_EQ(hub.pendingUnsubscribes(), 0u);
    EXPECT_EQ(hub.unsubscribesDropped(), 1u);
    EXPECT_EQ(tx.unsubscribesFor(61), 0) << "never sent under a session it does not belong to";
    EXPECT_EQ(hub.activeCount(), 0u);
}

// The late-response (CB-5 A2) Unsubscribe gets the same direct send + retry.
TEST(SubscriberHubCommand, LateResponseUnsubscribeRetriedWhenRefusedRegression) {
    SubscriberHub hub;
    CaptureSend   send;
    RefusingTransport tx;
    hub.setDirectSend(&RefusingTransport::fn, &tx);
    auto impl = makeImpl(52);
    hub.applyCommand(addCmd(impl.get()), 0, kSession, send.fn());
    rcd::HubCommand u;
    u.op = rcd::HubCommand::Op::Unsubscribe;
    u.a  = 52;
    hub.applyCommand(u, 902, kSession, send.fn());  // still pending: no frame yet
    tx.refuseNext = 1;
    hub.onSubscribeResponse(bindRsp(52, 62), 903, kSession, send.fn());
    EXPECT_EQ(hub.pendingUnsubscribes(), 1u);
    EXPECT_EQ(hub.retryPendingUnsubscribes(kSession), 1);
    EXPECT_EQ(tx.unsubscribesFor(62), 1);
    EXPECT_EQ(hub.activeCount(), 0u);
}

// fix3: a SubscribeResponse of a session that is no longer current never binds
// (its subscription died with that session).
TEST(SubscriberHubCommand, ResponseOfAnOldSessionNeverBindsRegression) {
    SubscriberHub hub;
    CaptureSend   send;
    auto impl = makeImpl(70);
    hub.applyCommand(addCmd(impl.get()), 0, kSession, send.fn());
    auto old = bindRsp(70, 80);
    old.sessionId = kSession - 1;
    hub.onSubscribeResponse(old, 0, kSession, send.fn());
    EXPECT_EQ(hub.activeCount(), 0u);
    EXPECT_EQ(hub.pendingCount(), 1u) << "kept pending for the current session";
    EXPECT_EQ(hub.staleResponsesIgnored(), 1u);
}

// ===========================================================================
// F8a fix round 4
// ===========================================================================

// I-1 (belt and braces): a Subscribe adopted while the hub believes the session
// is down is kept as UNSENT (offered once connected) — never silently dropped.
TEST(SubscriberHubCommand, SubscribeAdoptedWhileSessionDownIsKeptUnsentRegression) {
    SubscriberHub hub;
    CaptureSend   send;
    RefusingTransport tx;
    hub.setDirectSend(&RefusingTransport::fn, &tx);
    hub.setSessionConnected(false);
    auto impl = makeImpl(90);
    hub.applyCommand(addCmd(impl.get()), 0, kSession, send.fn());
    EXPECT_EQ(tx.subscribes(), 0);
    EXPECT_EQ(hub.unsentSubscribes(), 1u) << "dropped without a retry";
    EXPECT_EQ(hub.retryUnsentSubscribes(), 0) << "never offered while down";
    hub.setSessionConnected(true);
    EXPECT_EQ(hub.retryUnsentSubscribes(), 1);
    EXPECT_EQ(tx.subscribes(), 1);
    EXPECT_EQ(hub.unsentSubscribes(), 0u);
    EXPECT_EQ(hub.pendingCount(), 1u);
}

// M1: a Subscribe the transport can never carry fails the subscription — at
// adoption, on a per-cycle retry, or on a reconnect resend — never re-offered.
TEST(SubscriberHubCommand, FatalSubscribeOfferFailsTheSubscriptionRegression) {
    SubscriberHub hub;
    CaptureSend   send;
    RefusingTransport tx;
    hub.setDirectSend(&RefusingTransport::fn, &tx);

    // At adoption.
    auto a = makeImpl(91);
    tx.fatalNext = 1;
    hub.applyCommand(addCmd(a.get()), 0, kSession, send.fn());
    EXPECT_FALSE(a->active);
    EXPECT_TRUE(a->status.closed()) << "the handle learns it failed";
    EXPECT_EQ(hub.pendingCount(), 0u);
    EXPECT_EQ(hub.unsentSubscribes(), 0u);

    // On the per-cycle retry of a refused one.
    auto b = makeImpl(92);
    tx.refuseNext = 1;
    hub.applyCommand(addCmd(b.get()), 0, kSession, send.fn());
    ASSERT_EQ(hub.unsentSubscribes(), 1u);
    tx.fatalNext = 1;
    EXPECT_EQ(hub.retryUnsentSubscribes(), 0);
    EXPECT_FALSE(b->active);
    EXPECT_TRUE(b->status.closed());
    EXPECT_EQ(hub.unsentSubscribes(), 0u);
    EXPECT_EQ(hub.pendingCount(), 0u);

    // On a reconnect resend (c fails, d still goes out).
    auto c = makeImpl(93);
    auto d = makeImpl(94);
    hub.applyCommand(addCmd(c.get()), 0, kSession, send.fn());
    hub.applyCommand(addCmd(d.get()), 0, kSession, send.fn());
    ASSERT_EQ(tx.subscribes(), 2);
    tx.fatalNext = 1;
    EXPECT_EQ(hub.resendPending(kSession + 1, send.fn()), 1);
    EXPECT_FALSE(c->active);
    EXPECT_TRUE(c->status.closed());
    EXPECT_TRUE(d->active);
    EXPECT_EQ(hub.pendingCount(), 1u);
    EXPECT_EQ(tx.subscribes(), 3);

    EXPECT_EQ(hub.retryUnsentSubscribes(), 0);
    EXPECT_EQ(tx.fatals, 3) << "a Fatal frame is never re-offered";
}

// M1: an Unsubscribe the transport can never carry finishes locally (dropped,
// never kept pending) — on the direct path and on the late-response (A2) path.
TEST(SubscriberHubCommand, FatalUnsubscribeFinishesLocallyRegression) {
    SubscriberHub hub;
    CaptureSend   send;
    RefusingTransport tx;
    hub.setDirectSend(&RefusingTransport::fn, &tx);

    auto a = makeImpl(95);
    hub.applyCommand(addCmd(a.get()), 0, kSession, send.fn());
    hub.onSubscribeResponse(bindRsp(95, 65), 0, kSession, send.fn());
    ASSERT_EQ(hub.activeCount(), 1u);
    tx.fatalNext = 1;
    rcd::HubCommand u;
    u.op = rcd::HubCommand::Op::Unsubscribe;
    u.a  = 95;
    hub.applyCommand(u, 905, kSession, send.fn());
    EXPECT_TRUE(a->status.closed());
    EXPECT_EQ(hub.pendingUnsubscribes(), 0u) << "a Fatal Unsubscribe is never retried";
    EXPECT_EQ(hub.activeCount(), 0u);

    auto b = makeImpl(96);
    hub.applyCommand(addCmd(b.get()), 0, kSession, send.fn());
    u.a = 96;
    hub.applyCommand(u, 906, kSession, send.fn());  // still pending: late Unsubscribe
    tx.fatalNext = 1;
    hub.onSubscribeResponse(bindRsp(96, 66), 907, kSession, send.fn());
    EXPECT_EQ(hub.pendingUnsubscribes(), 0u);
    EXPECT_EQ(hub.activeCount(), 0u);

    EXPECT_EQ(hub.retryPendingUnsubscribes(kSession), 0);
    EXPECT_EQ(tx.fatals, 2);
    EXPECT_EQ(tx.unsubscribesFor(65) + tx.unsubscribesFor(66), 0);
}

// CG round 4 (A-MEDIUM-1): a pending plain subscription is bound by a push
// only when the client HAS a session and the push is that session's.  During
// a fresh connect (session id 0) another client's push on a shared publication
// carrying the same small correlationId must not bind it.  RED (round 3): 0
// meant "unchecked" — it bound the foreign subscription.
TEST(SubscriberHubCommand, PushNeverBindsAPendingSubscriptionWithoutASessionRegression) {
    SubscriberHub hub;
    CaptureSend   send;
    int delivered = 0;
    auto impl = makeImpl(7, &delivered);
    hub.applyCommand(addCmd(impl.get()), 0, kSession, send.fn());
    std::vector<uint8_t> st;
    auto foreign = records(st, /*subId=*/9001, 0, 2);
    foreign.correlationId = 7;           // another client's Subscribe, same small id
    foreign.sessionId     = 555;         // ... on its own session
    hub.onRecords(foreign, /*currentSessionId=*/0);  // this client is mid-connect
    EXPECT_EQ(delivered, 0);
    EXPECT_EQ(impl->subscriptionId, 0) << "a foreign push bound the pending subscription";
    // Its own push, once the session exists, binds it.
    auto own = records(st, /*subId=*/77, 0, 2);
    own.correlationId = 7;
    own.sessionId     = kSession;
    hub.onRecords(own, kSession);
    EXPECT_EQ(impl->subscriptionId, 77);
    EXPECT_EQ(delivered, 2);
}

// PR #441 F4 version skew: how a client that does not know RecordEgressUnavailable (49) handles it.  The SubscriberHub
// treats ANY non-zero SubscribeResponse code it is not told about as a terminal refusal of that pending Subscribe:
// finished explicitly (RejectedByBroker, the broker's code kept), nothing left pending - never a re-subscribe loop.
// The Client's bounded retry of 49 sits in front of this hub (Client.cpp); an older client reaches this path directly.
TEST(SubscriberHubCommand, AnUnknownSubscribeRefusalCodeFinishesThePendingSubscribeExplicitlyRegression) {
    for (const int16_t code : {int16_t{49}, int16_t{250}}) {
        SubscriberHub hub;
        CaptureSend   send;
        auto impl = makeImpl(40 + code);
        hub.applyCommand(addCmd(impl.get()), 0, kSession, send.fn());
        ASSERT_EQ(hub.pendingCount(), 1u);
        auto rsp = bindRsp(40 + code, 0);
        rsp.errorCode = code;
        hub.onSubscribeResponse(rsp, 0, kSession, send.fn());
        EXPECT_EQ(hub.pendingCount(), 0u) << "code " << code << ": nothing stays pending";
        EXPECT_EQ(impl->failReason, rc::SubscriptionFailReason::RejectedByBroker) << "code " << code;
        EXPECT_EQ(impl->brokerErrorCode, static_cast<uint16_t>(code)) << "the broker's code is kept";
        EXPECT_FALSE(impl->active);
        EXPECT_EQ(send.countSubscribe(), 1) << "never re-sent";
    }
}

// ===========================================================================
// #446: a Subscribe OK that binds nothing (its correlationId timed out / swept, or it came on another live
// session than the pending Subscribe expects) used to be dropped while the broker kept the subscription and
// pushed records until the session closed.  It is now released with one best-effort Unsubscribe.
// ===========================================================================
namespace {
/// A hub's view of the node sessions: records what is offered where, answers sessionIndexOf with `index`.
struct NodeFake {
    uint32_t index = 0;   // 0 none, UINT32_MAX ambiguous
    int64_t  corr  = 9000;
    int      offersRefused = 0;
    std::vector<std::pair<uint32_t, Frame>> offered;
    static uint32_t idxOf(void* ctx, int64_t) { return static_cast<NodeFake*>(ctx)->index; }
    static uint32_t leader(void*, int32_t, int32_t) { return 0; }
    static int64_t  sid(void*, uint32_t) { return 0; }
    static uint64_t epoch(void*, uint32_t) { return 0; }
    static int64_t  next(void* ctx) { return ++static_cast<NodeFake*>(ctx)->corr; }
    static rcd::SendResult offer(void* ctx, uint32_t s, const uint8_t* b, std::size_t n) {
        auto* self = static_cast<NodeFake*>(ctx);
        if (self->offersRefused > 0) { --self->offersRefused; return rcd::SendResult::Retry; }
        self->offered.emplace_back(s, Frame{std::vector<uint8_t>(b, b + n)});
        return rcd::SendResult::Accepted;
    }
    SubscriberHub::NodeRouter router() {
        SubscriberHub::NodeRouter r;
        r.ctx = this; r.leaderSession = &leader; r.sessionId = &sid; r.epoch = &epoch;
        r.offer = &offer; r.nextCorrId = &next; r.sessionIndexOf = &idxOf;
        return r;
    }
};

/// The Unsubscribe frames among a transport's accepted frames (the Subscribe itself was sent through it too).
std::vector<rc::codec::UnsubscribeView> unsubscribesOf(const RefusingTransport& tx) {
    std::vector<rc::codec::UnsubscribeView> out;
    for (const auto& f : tx.accepted)
        if (auto u = rc::codec::decodeUnsubscribe(f.bytes.data(), f.bytes.size())) out.push_back(*u);
    return out;
}

/// Subscribe `corr`, then let the client's subscribe timeout sweep it.
void subscribeAndTimeOut(SubscriberHub& hub, CaptureSend& send, rc::SubscriptionImpl* impl) {
    hub.applyCommand(addCmd(impl), 0, kSession, send.fn());
    ASSERT_EQ(hub.pendingCount(), 1u);
    hub.sweepTimedOut(std::chrono::steady_clock::now() + std::chrono::hours(2));
    ASSERT_EQ(hub.pendingCount(), 0u);
}
} // namespace

// RED (before): the late OK was dropped; no Unsubscribe ever left and the broker kept pushing.
TEST(SubscriberHubRegression, ALateOkAfterTheSubscribeTimeoutIsUnsubscribedRegression) {
    SubscriberHub hub;
    CaptureSend   send;
    RefusingTransport tx;
    hub.setDirectSend(&RefusingTransport::fn, &tx);
    auto impl = makeImpl(80);
    subscribeAndTimeOut(hub, send, impl.get());
    hub.onSubscribeResponse(bindRsp(80, 4711), /*unsubCorrId=*/777, kSession, send.fn());
    const auto unsubs = unsubscribesOf(tx);
    ASSERT_EQ(unsubs.size(), 1u) << "exactly one Unsubscribe for the unbound OK";
    EXPECT_EQ(unsubs[0].subscriptionId, 4711);
    EXPECT_EQ(unsubs[0].sessionId, kSession);
    EXPECT_EQ(unsubs[0].correlationId, 777);
    EXPECT_EQ(hub.lateOksReleased(), 1u);
    EXPECT_EQ(hub.lateOksUnreleased(), 0u);
    EXPECT_EQ(hub.activeCount(), 0u);
}

// Records-first binding: the id is in use, so the OK that follows must not tear it down.
TEST(SubscriberHubRegression, ALateOkForAnIdAlreadyBoundByRecordsIsNotUnsubscribedRegression) {
    SubscriberHub hub;
    CaptureSend   send;
    RefusingTransport tx;
    hub.setDirectSend(&RefusingTransport::fn, &tx);
    int delivered = 0;
    auto impl = makeImpl(81, &delivered);
    hub.applyCommand(addCmd(impl.get()), 0, kSession, send.fn());
    std::vector<uint8_t> st;
    auto rv = records(st, /*subId=*/4712, 0, 2);
    rv.correlationId = 81;
    rv.sessionId     = kSession;
    hub.onRecords(rv, kSession);                     // binds the pending subscription
    ASSERT_EQ(hub.activeCount(), 1u);
    ASSERT_EQ(delivered, 2);
    hub.onSubscribeResponse(bindRsp(81, 4712), 778, kSession, send.fn());
    EXPECT_TRUE(unsubscribesOf(tx).empty()) << "the bound subscription was unsubscribed";
    EXPECT_EQ(hub.activeCount(), 1u);
    EXPECT_TRUE(impl->active);
    EXPECT_EQ(hub.lateOksReleased(), 0u);
}

// An OK stamped with a session that is neither the seed nor any node session: nothing to send on, counted.
TEST(SubscriberHubRegression, ALateOkFromADeadSessionIsNotUnsubscribedRegression) {
    SubscriberHub hub;
    CaptureSend   send;
    RefusingTransport tx;
    NodeFake      nodes;                              // sessionIndexOf answers 0: no such live session
    hub.setDirectSend(&RefusingTransport::fn, &tx);
    hub.setNodeRouter(nodes.router());
    auto impl = makeImpl(82);
    subscribeAndTimeOut(hub, send, impl.get());
    auto rsp = bindRsp(82, 4713);
    rsp.sessionId = 99;                               // not the seed (7), not a live node session
    hub.onSubscribeResponse(rsp, 779, kSession, send.fn());
    EXPECT_TRUE(unsubscribesOf(tx).empty());
    EXPECT_TRUE(nodes.offered.empty());
    EXPECT_EQ(hub.lateOksReleased(), 0u);
    EXPECT_EQ(hub.lateOksUnreleased(), 1u);
}

// A node session that is alive and carries that broker session id gets the Unsubscribe - and only it.
TEST(SubscriberHubRegression, ALateOkOnANodeSessionIsUnsubscribedThereRegression) {
    SubscriberHub hub;
    CaptureSend   send;
    RefusingTransport tx;
    NodeFake      nodes;
    nodes.index = 2;
    hub.setDirectSend(&RefusingTransport::fn, &tx);
    hub.setNodeRouter(nodes.router());
    auto impl = makeImpl(83);
    subscribeAndTimeOut(hub, send, impl.get());
    auto rsp = bindRsp(83, 4714);
    rsp.sessionId = 55;
    hub.onSubscribeResponse(rsp, 780, kSession, send.fn());
    EXPECT_TRUE(unsubscribesOf(tx).empty()) << "the seed must not get a node session's Unsubscribe";
    ASSERT_EQ(nodes.offered.size(), 1u);
    EXPECT_EQ(nodes.offered[0].first, 2u);
    const auto u = rc::codec::decodeUnsubscribe(nodes.offered[0].second.bytes.data(), nodes.offered[0].second.bytes.size());
    ASSERT_TRUE(u.has_value());
    EXPECT_EQ(u->subscriptionId, 4714);
    EXPECT_EQ(u->sessionId, 55);
    EXPECT_EQ(u->correlationId, 780);
    EXPECT_EQ(hub.lateOksReleased(), 1u);
}

// The same OK arrives for a PENDING Subscribe that expects another session (the I2 check): still released.
TEST(SubscriberHubRegression, AnOkOnAnotherLiveSessionThanThePendingOneIsUnsubscribedThereRegression) {
    SubscriberHub hub;
    CaptureSend   send;
    RefusingTransport tx;
    NodeFake      nodes;
    nodes.index = 3;
    hub.setDirectSend(&RefusingTransport::fn, &tx);
    hub.setNodeRouter(nodes.router());
    auto impl = makeImpl(84);
    hub.applyCommand(addCmd(impl.get()), 0, kSession, send.fn());
    auto rsp = bindRsp(84, 4715);
    rsp.sessionId = 56;                               // not the pending one's (seed 7)
    hub.onSubscribeResponse(rsp, 781, kSession, send.fn());
    EXPECT_EQ(hub.pendingCount(), 1u) << "the pending Subscribe stays for its own session";
    ASSERT_EQ(nodes.offered.size(), 1u);
    EXPECT_EQ(nodes.offered[0].first, 3u);
    EXPECT_EQ(hub.lateOksReleased(), 1u);
}

// Two sessions claim the id (the seed and a node, or two nodes): ambiguous, so nothing is sent.
TEST(SubscriberHubRegression, ALateOkMatchingTwoSessionsIsNotUnsubscribedRegression) {
    {
        SubscriberHub hub;
        CaptureSend   send;
        RefusingTransport tx;
        NodeFake      nodes;
        nodes.index = 2;                              // a node also carries the seed's id
        hub.setDirectSend(&RefusingTransport::fn, &tx);
        hub.setNodeRouter(nodes.router());
        auto impl = makeImpl(85);
        subscribeAndTimeOut(hub, send, impl.get());
        hub.onSubscribeResponse(bindRsp(85, 4716), 782, kSession, send.fn());   // sessionId == seed
        EXPECT_TRUE(unsubscribesOf(tx).empty());
        EXPECT_TRUE(nodes.offered.empty());
        EXPECT_EQ(hub.lateOksUnreleased(), 1u);
    }
    {
        SubscriberHub hub;
        CaptureSend   send;
        NodeFake      nodes;
        nodes.index = UINT32_MAX;                     // two nodes carry it
        hub.setNodeRouter(nodes.router());
        auto impl = makeImpl(86);
        subscribeAndTimeOut(hub, send, impl.get());
        auto rsp = bindRsp(86, 4717);
        rsp.sessionId = 55;
        hub.onSubscribeResponse(rsp, 783, kSession, send.fn());
        EXPECT_TRUE(nodes.offered.empty());
        EXPECT_EQ(hub.lateOksUnreleased(), 1u);
    }
}

// A refused offer is one attempt only: counted unreleased, never retried.
TEST(SubscriberHubRegression, ARefusedLateOkUnsubscribeIsCountedAndNotRetriedRegression) {
    SubscriberHub hub;
    CaptureSend   send;
    RefusingTransport tx;
    hub.setDirectSend(&RefusingTransport::fn, &tx);
    auto impl = makeImpl(87);
    subscribeAndTimeOut(hub, send, impl.get());
    tx.refuseNext = 1;                               // the late OK's one Unsubscribe attempt
    hub.onSubscribeResponse(bindRsp(87, 4718), 784, kSession, send.fn());
    EXPECT_EQ(hub.lateOksUnreleased(), 1u);
    EXPECT_EQ(hub.lateOksReleased(), 0u);
    EXPECT_EQ(hub.pendingUnsubscribes(), 0u);
    EXPECT_EQ(hub.retryPendingUnsubscribes(kSession), 0);
    EXPECT_TRUE(unsubscribesOf(tx).empty());
}

// An error answer binds nothing broker-side, so there is nothing to release.
TEST(SubscriberHubRegression, AnErrorAnswerAfterTimeoutSendsNothingRegression) {
    SubscriberHub hub;
    CaptureSend   send;
    RefusingTransport tx;
    hub.setDirectSend(&RefusingTransport::fn, &tx);
    auto impl = makeImpl(88);
    subscribeAndTimeOut(hub, send, impl.get());
    auto rsp = bindRsp(88, 0);
    rsp.errorCode = 12;
    hub.onSubscribeResponse(rsp, 785, kSession, send.fn());
    auto withId = bindRsp(88, 4719);
    withId.errorCode = 12;
    hub.onSubscribeResponse(withId, 786, kSession, send.fn());
    EXPECT_TRUE(unsubscribesOf(tx).empty());
    EXPECT_EQ(hub.lateOksReleased(), 0u);
    EXPECT_EQ(hub.lateOksUnreleased(), 0u);
}

// ===========================================================================
// #446 adversarial (M31+ review): replays and cross-session answers around the late-OK release.
// ===========================================================================

// The binding OK replayed (a broker answering a resent Subscribe from its dedupe index repeats the same id):
// the id is bound here, so no replay may release it, and the subscription keeps delivering.
// RED by mutation: drop the activeById_ check in releaseUnboundOk_.
TEST(SubscriberHubRegression, ABindingOkReplayedNeverReleasesTheBoundSubscriptionAdversarialRegression) {
    SubscriberHub hub;
    CaptureSend   send;
    RefusingTransport tx;
    NodeFake      nodes;                              // no node session claims the seed's id
    hub.setDirectSend(&RefusingTransport::fn, &tx);
    hub.setNodeRouter(nodes.router());
    int delivered = 0;
    auto impl = makeImpl(120, &delivered);
    hub.applyCommand(addCmd(impl.get()), 0, kSession, send.fn());
    hub.onSubscribeResponse(bindRsp(120, 4800), 900, kSession, send.fn());
    ASSERT_EQ(hub.activeCount(), 1u);
    for (int replay = 0; replay < 3; ++replay)
        hub.onSubscribeResponse(bindRsp(120, 4800), 901 + replay, kSession, send.fn());
    EXPECT_TRUE(unsubscribesOf(tx).empty()) << "a replayed OK released the live subscription";
    EXPECT_EQ(send.countUnsubscribe(4800), 0);
    EXPECT_TRUE(nodes.offered.empty());
    EXPECT_EQ(hub.lateOksReleased(), 0u);
    EXPECT_EQ(hub.lateOksUnreleased(), 0u);
    std::vector<uint8_t> st;
    hub.onRecords(records(st, 4800, 0, 3), kSession);
    EXPECT_EQ(delivered, 3);
    EXPECT_TRUE(impl->active);
}

// The same Subscribe correlation answered on two sessions: a node session (an earlier route) binds broker-side
// instance X and pushes, and the seed (the pending one's own session) binds Y.  X's Records never bind the
// pending subscription, X's OK is released on that node session only, Y binds and delivers, and X's later
// pushes are never delivered to it.  RED by mutation: drop releaseUnboundOk_ from the session-mismatch branch
// of onSubscribeResponse (X is never released).
TEST(SubscriberHubRegression, ACorrelationAnsweredOnTwoSessionsBindsOnlyItsOwnAndReleasesTheOtherAdversarialRegression) {
    SubscriberHub hub;
    CaptureSend   send;
    RefusingTransport tx;
    NodeFake      nodes;
    nodes.index = 4;                                  // broker session 61 is node session 4
    hub.setDirectSend(&RefusingTransport::fn, &tx);
    hub.setNodeRouter(nodes.router());
    int delivered = 0;
    auto impl = makeImpl(121, &delivered);
    hub.applyCommand(addCmd(impl.get()), 0, kSession, send.fn());

    std::vector<uint8_t> st;
    auto x = records(st, /*subId=*/4810, 0, 2);
    x.correlationId = 121;
    x.sessionId     = 61;                             // not the pending one's session
    hub.onRecords(x, kSession);
    EXPECT_EQ(hub.pendingCount(), 1u) << "a push from another session bound the pending subscription";
    EXPECT_EQ(delivered, 0);

    auto okX = bindRsp(121, 4810);
    okX.sessionId = 61;
    hub.onSubscribeResponse(okX, 910, kSession, send.fn());
    ASSERT_EQ(nodes.offered.size(), 1u) << "X is released where it lives";
    EXPECT_EQ(nodes.offered[0].first, 4u);
    const auto u = rc::codec::decodeUnsubscribe(nodes.offered[0].second.bytes.data(),
                                                nodes.offered[0].second.bytes.size());
    ASSERT_TRUE(u.has_value());
    EXPECT_EQ(u->subscriptionId, 4810);
    EXPECT_EQ(u->sessionId, 61);
    EXPECT_TRUE(unsubscribesOf(tx).empty()) << "nothing released on the seed";
    EXPECT_EQ(hub.pendingCount(), 1u);

    hub.onSubscribeResponse(bindRsp(121, 4811), 911, kSession, send.fn());   // Y on the seed
    ASSERT_EQ(hub.activeCount(), 1u);
    EXPECT_EQ(impl->subscriptionId, 4811);
    auto y = records(st, 4811, 0, 3);
    y.correlationId = 121;
    y.sessionId     = kSession;
    hub.onRecords(y, kSession);
    EXPECT_EQ(delivered, 3);
    hub.onRecords(x, kSession);                       // X's push still in flight
    EXPECT_EQ(delivered, 3) << "X's records reached Y's handler";
    EXPECT_EQ(hub.lateOksReleased(), 1u);
    EXPECT_EQ(hub.lateOksUnreleased(), 0u);
    EXPECT_TRUE(unsubscribesOf(tx).empty());
    EXPECT_EQ(nodes.offered.size(), 1u);
}

// Seeded random interleavings of: the binding OK, replays of it, stray OKs naming fresh ids on the seed,
// error answers naming ids, and Records for the bound id.  Invariant: the bound id is never
// unsubscribed, every stray id is unsubscribed exactly once on the seed, and every bound record is delivered.
// RED by mutation: drop the activeById_ check in releaseUnboundOk_ (a replay releases the bound id).
TEST(SubscriberHubRegression, LateOkReleaseUnderSeededRandomInterleavingsAdversarialRegression) {
    for (uint32_t seed = 1; seed <= 40; ++seed) {
        SCOPED_TRACE(::testing::Message() << "seed=" << seed);
        uint32_t rng = seed * 2654435761u;
        auto next = [&] { rng ^= rng << 13; rng ^= rng >> 17; rng ^= rng << 5; return rng; };
        SubscriberHub hub;
        CaptureSend   send;
        RefusingTransport tx;
        NodeFake      nodes;
        hub.setDirectSend(&RefusingTransport::fn, &tx);
        hub.setNodeRouter(nodes.router());
        int delivered = 0;
        auto impl = makeImpl(130, &delivered);
        hub.applyCommand(addCmd(impl.get()), 0, kSession, send.fn());
        bool bound = false;
        int expected = 0;
        int64_t strayId = 5000, unsubCorr = 2000;
        std::vector<int64_t> strays;
        std::vector<int64_t> errored;
        std::vector<uint8_t> st;
        for (int step = 0; step < 60; ++step) {
            switch (next() % 5) {
            case 0: case 1:                           // the binding OK (first one binds, later ones replay)
                hub.onSubscribeResponse(bindRsp(130, 4900), ++unsubCorr, kSession, send.fn());
                bound = true;
                break;
            case 2: {                                 // an error answer naming an id: binds nothing, never released
                auto e = bindRsp(800'000 + step, ++strayId);
                e.errorCode = 12;
                hub.onSubscribeResponse(e, ++unsubCorr, kSession, send.fn());
                errored.push_back(strayId);
                break;
            }
            case 3:                                   // a push for the bound id
                if (bound) {
                    auto rv = records(st, 4900, expected, 1);
                    hub.onRecords(rv, kSession);
                    ++expected;
                }
                break;
            default:                                  // a stray OK with no correlation at all
                hub.onSubscribeResponse(bindRsp(999'000 + step, ++strayId), ++unsubCorr, kSession, send.fn());
                strays.push_back(strayId);
                break;
            }
        }
        EXPECT_EQ(send.countUnsubscribe(4900) + tx.unsubscribesFor(4900), 0) << "the bound id was released";
        for (int64_t id : strays) EXPECT_EQ(tx.unsubscribesFor(id), 1) << "stray id " << id;
        for (int64_t id : errored) EXPECT_EQ(tx.unsubscribesFor(id), 0) << "error-answer id " << id;
        EXPECT_EQ(hub.lateOksReleased(), strays.size());
        EXPECT_EQ(delivered, expected);
        if (bound) EXPECT_TRUE(impl->active);
    }
}
