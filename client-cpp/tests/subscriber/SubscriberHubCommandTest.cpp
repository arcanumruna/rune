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

std::shared_ptr<rc::SubscriptionImpl> makeImpl(int64_t corrId, int* delivered = nullptr) {
    auto impl = std::make_shared<rc::SubscriptionImpl>();
    impl->correlationId = corrId;
    impl->topicId       = kTopic;
    impl->partition     = kPart;
    impl->handler       = [delivered](const rc::Record&) { if (delivered) ++*delivered; };
    return impl;
}

rcd::HubCommand addCmd(std::shared_ptr<rc::SubscriptionImpl> impl) {
    auto req = SubscriberHub::makeSubscribeRequest(
        std::move(impl), kSession, /*partitionKnown=*/1, /*fromOffset=*/-2, /*iso=*/0, {},
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

} // namespace

// SubscribeAdd is applied by the duty thread: it registers the pending entry
// and sends the Subscribe frame — the app thread only built the request.
TEST(SubscriberHubCommand, SubscribeAddRegistersAndSendsOnDutyThreadRegression) {
    SubscriberHub hub;
    CaptureSend   send;
    auto impl = makeImpl(11);
    EXPECT_EQ(hub.pendingCount(), 0u);
    hub.applyCommand(addCmd(impl), 0, kSession, send.fn());
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
    rc::Subscription sub(impl, &CapturingSink::fn, &sink);

    hub.applyCommand(addCmd(impl), 0, kSession, send.fn());
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
    rc::Subscription sub(impl, &CapturingSink::fn, &sink);
    hub.applyCommand(addCmd(impl), 0, kSession, send.fn());
    hub.onSubscribeResponse(bindRsp(13, 88), 0, kSession, send.fn());
    ASSERT_EQ(hub.activeCount(), 1u);

    sub.unsubscribe();
    EXPECT_FALSE(sub.active()) << "the handle reports inactive at once (handle-local)";
    ASSERT_EQ(sink.cmds.size(), 1u);
    EXPECT_EQ(sink.cmds[0].op, rcd::HubCommand::Op::Unsubscribe);
    EXPECT_EQ(sink.cmds[0].a, 13);
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
    hub.applyCommand(addCmd(impl), 0, kSession, send.fn());

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
    rc::Subscription sub(impl, &CapturingSink::fn, &sink);
    hub.applyCommand(addCmd(impl), 0, kSession, send.fn());
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
    rc::Subscription sub(impl, &CapturingSink::fn, &sink);
    hub.applyCommand(addCmd(impl), 0, kSession, send.fn());
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
    auto impl = makeImpl(17);
    {
        rc::Subscription sub(impl, &CapturingSink::fn, &sink);
        hub.applyCommand(addCmd(impl), 0, kSession, send.fn());
        hub.onSubscribeResponse(bindRsp(17, 8), 0, kSession, send.fn());
        hub.deactivateAll();
        EXPECT_FALSE(sub.active());
        sub.unsubscribe();
        EXPECT_EQ(sink.calls, 0);
    }  // dtor: still no call
    EXPECT_EQ(sink.calls, 0);
}

// Subscribe deadline expiry finishes the subscription for the app.
TEST(SubscriberHubCommand, TimedOutSubscribeFinishesHandleRegression) {
    SubscriberHub hub;
    CaptureSend   send;
    CapturingSink sink;
    auto impl = makeImpl(18);
    rc::Subscription sub(impl, &CapturingSink::fn, &sink);
    auto req = SubscriberHub::makeSubscribeRequest(
        impl, kSession, 1, -2, 0, {}, std::chrono::steady_clock::now());
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
        self->hub->applyCommand(c, 700 + self->calls, kSession, self->send->fn());
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
    rc::Subscription sub(impl, &DirectSubSink::fn, &ds, std::this_thread::get_id());
    int delivered = 0;
    bool finishedInsideHandler = true;
    impl->handler = [&](const rc::Record&) {
        ++delivered;
        sub.unsubscribe();
        finishedInsideHandler = sub.finished();
    };
    hub.applyCommand(addCmd(impl), 0, kSession, send.fn());
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
// running std::function: the routing entry (and its impl reference) lives until
// the dispatch ends.
TEST(SubscriberHubCommand, DestroyHandleInsideHandlerKeepsImplAliveUntilDispatchEndsRegression) {
    SubscriberHub hub;
    CaptureSend   send;
    DirectSubSink ds{&hub, &send};
    auto impl = makeImpl(31);
    std::weak_ptr<rc::SubscriptionImpl> weak = impl;
    auto sub = std::make_unique<rc::Subscription>(impl, &DirectSubSink::fn, &ds,
                                                  std::this_thread::get_id());
    bool aliveAfterReset = false;
    int delivered = 0;
    impl->handler = [&](const rc::Record&) {
        ++delivered;
        sub.reset();  // dtor -> unsubscribe -> applied directly on this thread
        aliveAfterReset = !weak.expired();
    };
    hub.applyCommand(addCmd(impl), 0, kSession, send.fn());
    hub.onSubscribeResponse(bindRsp(31, 41), 0, kSession, send.fn());
    impl.reset();  // only the hub (and the handle) hold it now

    std::vector<uint8_t> st;
    hub.onRecords(records(st, 41, 0, 3));
    EXPECT_EQ(delivered, 1);
    EXPECT_TRUE(aliveAfterReset) << "the impl (and the running handler) must outlive the reset";
    EXPECT_TRUE(weak.expired()) << "released once the dispatch ended";
    EXPECT_EQ(hub.activeCount(), 0u);
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
    auto sub = std::make_unique<rc::Subscription>(impl, &RingChannel::fn, &ch);
    (void)RingChannel::fn(&ch, addCmd(impl));
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
    hub.applyCommand(addCmd(impl), 0, kSession, send.fn());
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
    hub.applyCommand(addCmd(impl), 0, kSession, send.fn());
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
    hub.applyCommand(addCmd(impl), 0, kSession, send.fn());
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
    hub.applyCommand(addCmd(impl), 0, kSession, send.fn());
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
    hub.applyCommand(addCmd(impl), 0, kSession, send.fn());
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
    hub.applyCommand(addCmd(a), 0, kSession, send.fn());
    EXPECT_FALSE(a->active);
    EXPECT_TRUE(a->status.closed()) << "the handle learns it failed";
    EXPECT_EQ(hub.pendingCount(), 0u);
    EXPECT_EQ(hub.unsentSubscribes(), 0u);

    // On the per-cycle retry of a refused one.
    auto b = makeImpl(92);
    tx.refuseNext = 1;
    hub.applyCommand(addCmd(b), 0, kSession, send.fn());
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
    hub.applyCommand(addCmd(c), 0, kSession, send.fn());
    hub.applyCommand(addCmd(d), 0, kSession, send.fn());
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
    hub.applyCommand(addCmd(a), 0, kSession, send.fn());
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
    hub.applyCommand(addCmd(b), 0, kSession, send.fn());
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
