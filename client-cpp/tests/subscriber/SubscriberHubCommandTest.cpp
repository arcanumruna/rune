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

// A refused (dirty) snapshot is republished by the periodic sweep once the app
// drained — position() converges without any further Records frame.
TEST(SubscriberHubCommand, DirtyStatusRepublishedBySweepRegression) {
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
    ASSERT_TRUE(impl->statusDirty);
    EXPECT_LT(sub.position(), kFrames) << "stale until the duty thread republishes";

    hub.sweepTimedOut(std::chrono::steady_clock::now());
    EXPECT_FALSE(impl->statusDirty);
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
