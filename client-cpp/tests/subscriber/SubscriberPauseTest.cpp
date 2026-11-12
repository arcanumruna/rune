// tests/subscriber/SubscriberPauseTest.cpp
//
// Subscription::pause / resume / paused.  Client-only: no wire change and no broker change.  The tests drive the
// SubscriberHub directly (the test thread plays the duty thread) on the in-process gateway stub that
// SubscriberHubCommandTest.cpp uses: the test pushes Records frames and reads the frames the client sent.
// Every drive loop has a hard iteration cap; nothing waits on wall time.

#include "../support/AllocCounter.h"

#include <rune/client/subscriber/SubscriberHub.h>
#include <rune/client/subscriber/Subscription.h>
#include <rune/client/codec/DataCodec.h>
#include <rune/client/detail/HubCommand.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/protocol/client/Protocol.h>

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <cstdio>
#include <cstdlib>
#include <string>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <random>
#include <thread>
#include <type_traits>
#include <vector>

namespace rc  = rune::client;
namespace rcd = rune::client::detail;
using rune::client::subscriber::SubscriberHub;

namespace rune::client {
/// The friend the handle declares: puts the command counter next to its wrap.
struct SubscriptionTestAccess {
    static void setSeq(Subscription& s, uint32_t n) { s.appSeq_ = n; }
};
}  // namespace rune::client

namespace {

constexpr int64_t kSession = 7;
constexpr int32_t kTopic   = 3;
constexpr int32_t kPart    = 0;
constexpr int64_t kHour    = 3'600'000'000'000LL;

/// The transport the hub sends through (direct send).  Decodes what it carries; counts without allocating when
/// `store` is false (the allocation gate).
struct Tx {
    bool                  store = true;
    bool                  refuseUnsub = false;   // answer Retry to an Unsubscribe (the transport is back-pressured)
    int                   subscribes = 0;
    int                   unsubscribes = 0;
    int64_t               lastSubCorr = 0;
    int64_t               lastSubFrom = 0;
    int64_t               lastSubSession = 0;
    uint8_t               lastSubKnown = 0;
    std::vector<int64_t>  unsubIds;
    std::vector<int64_t>  subFroms;

    static rcd::SendResult fn(void* ctx, const uint8_t* b, std::size_t n) {
        auto* self = static_cast<Tx*>(ctx);
        if (auto s = rc::codec::decodeSubscribe(b, n)) {
            ++self->subscribes;
            self->lastSubCorr    = s->correlationId;
            self->lastSubFrom    = s->fromOffset;
            self->lastSubSession = s->sessionId;
            self->lastSubKnown   = s->partitionKnown;
            if (self->store) self->subFroms.push_back(s->fromOffset);
        } else if (auto u = rc::codec::decodeUnsubscribe(b, n)) {
            if (self->refuseUnsub) return rcd::SendResult::Retry;
            ++self->unsubscribes;
            if (self->store) self->unsubIds.push_back(u->subscriptionId);
        }
        return rcd::SendResult::Accepted;
    }
    [[nodiscard]] int unsubsFor(int64_t id) const {
        int c = 0;
        for (int64_t x : unsubIds) c += x == id;
        return c;
    }
};

rc::codec::SubscribeResponseView bindRsp(int64_t corr, int64_t subId, int64_t sid) {
    rc::codec::SubscribeResponseView r;
    r.sessionId      = sid;
    r.correlationId  = corr;
    r.subscriptionId = subId;
    return r;
}

/// A Records view over `storage` (count records at baseOffset).
rc::codec::RecordsView records(std::vector<uint8_t>& storage, int64_t subId, int64_t baseOffset, int32_t count) {
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

/// One subscription on one hub, with a handle whose commands the "Client" (this rig) applies to the hub at once.
struct Rig {
    SubscriberHub                          hub;
    Tx                                     tx;
    int64_t                                sid      = kSession;
    int64_t                                nextCorr = 1000;
    int64_t                                nowNs    = 0;
    int64_t                                corr     = 0;   // the live Subscribe correlation id
    std::vector<int64_t>                   got;            // every offset the handler saw
    std::function<void(const rc::Record&)> hook;           // runs inside the handler, after the offset is logged
    std::unique_ptr<rc::SubscriptionImpl>  impl;
    std::optional<rc::Subscription>        sub;
    std::vector<uint8_t>                   st;
    // The command ring's delay: with `deferApp`, a command issued off the duty thread (the AppThread) is queued and the
    // test applies it later with applyQueued() (a Records frame can be processed in between, as in a real Client).
    bool                                   deferApp = false;
    bool                                   viaRing  = false;   // the test thread's handle calls go through the command ring (applied at once), not the direct path
    std::thread::id                        dutyId   = std::this_thread::get_id();
    std::vector<rcd::HubCommand>           queued;
    void applyQueued() {
        auto q = queued;
        queued.clear();
        for (auto& cmd : q) hub.applyCommand(cmd, ++nextCorr, sid, noSend, nowNs);
    }
    SubscriberHub::SendFn                  noSend = [](int32_t, const uint8_t*, std::size_t) { return true; };

    Rig() { hub.setDirectSend(&Tx::fn, &tx); got.reserve(1 << 16); }

    /// The Client's hubCommandTrampoline_, in miniature.  Pause / resume from the duty thread (this test thread, unless
    /// `viaRing`) are decided and applied by the hub and answer with its verdict (Applied / Refused); "only if applied
    /// directly" (cmd.c) elsewhere does nothing; any other call is the command ring: queued (`deferApp`, off the duty
    /// thread) or applied at once, and answered Accepted.
    static rcd::HubSubmit sink(void* ctx, const rcd::HubCommand& c) {
        using Op = rcd::HubCommand::Op;
        auto* r = static_cast<Rig*>(ctx);
        rcd::HubCommand cmd = c;
        const bool onDuty = std::this_thread::get_id() == r->dutyId;
        if (cmd.op == Op::SubscribePause || cmd.op == Op::SubscribeResume) {
            auto* impl = reinterpret_cast<rc::SubscriptionImpl*>(cmd.handle);
            if (onDuty && !r->viaRing) {
                const bool ok = cmd.op == Op::SubscribePause
                    ? r->hub.pauseDirect(*impl, cmd.a, static_cast<uint32_t>(cmd.b), ++r->nextCorr, r->sid, r->noSend)
                    : r->hub.resumeDirect(*impl, static_cast<uint32_t>(cmd.b), ++r->nextCorr, r->sid, r->nowNs, r->noSend);
                return ok ? rcd::HubSubmit::Applied : rcd::HubSubmit::Refused;
            }
            if (cmd.op == Op::SubscribePause && cmd.c == 1) return rcd::HubSubmit::Accepted;   // not decidable here
        }
        if (r->deferApp && (!onDuty || r->viaRing)) { r->queued.push_back(cmd); return rcd::HubSubmit::Accepted; }
        if (cmd.op == Op::HandleRelease) cmd.op = Op::Unsubscribe;
        r->hub.applyCommand(cmd, ++r->nextCorr, r->sid, r->noSend, r->nowNs);
        return rcd::HubSubmit::Accepted;
    }

    /// Adopt a subscription From::earliest.  dutyHandle: the handle takes the duty path (a call from a callback); else it is
    /// an app-thread handle (its commands still apply at once, as the ring would).
    void open(int64_t c, bool dutyHandle = true, int64_t fromWire = -2) {
        corr                 = c;
        impl                 = std::make_unique<rc::SubscriptionImpl>();
        impl->correlationId  = c;
        impl->topicId        = kTopic;
        impl->partition      = kPart;
        impl->fromWire       = fromWire;
        impl->handler        = [this](const rc::Record& r) { got.push_back(r.offset); if (hook) hook(r); };
        sub.emplace(impl.get(), &Rig::sink, this, dutyHandle ? std::this_thread::get_id() : std::thread::id{});
        auto req = SubscriberHub::makeSubscribeRequest(impl.get(), sid, 1, fromWire, 0, {}, kHour);
        rcd::HubCommand add;
        add.op     = rcd::HubCommand::Op::SubscribeAdd;
        add.handle = reinterpret_cast<uint64_t>(req.release());
        hub.applyCommand(add, 0, sid, noSend, nowNs);
    }
    void bind(int64_t subId) { hub.onSubscribeResponse(bindRsp(corr, subId, sid), 0, sid, noSend); }
    void push(int64_t subId, int64_t base, int32_t count) { hub.onRecords(records(st, subId, base, count)); }
    /// The answer to the Subscribe the client sent last (what resume() put on the wire).
    void bindLast(int64_t subId) {
        corr = tx.lastSubCorr;
        bind(subId);
    }
    ~Rig() {
        // The test owns the impl memory: a handle built on a bare hub never frees it.
        sub.reset();
    }
};

/// A persistent "application thread": run(f) executes f on it and returns when f is done (a capped handshake; no
/// allocation per call).  The hub is driven by the test thread (the duty thread of the Rig's handle); the calls f makes on
/// the handle take the APP path.  The calls never overlap the test thread's own, so the hub is never used concurrently.
struct AppThread {
    std::atomic<int>   st{0};   // 0 idle, 1 work posted, 2 work done
    void             (*fn)(void*) = nullptr;
    void*              ctx = nullptr;
    std::atomic<bool>  quit{false};
    std::thread        th;
    AppThread() {
        th = std::thread([this] {
            while (!quit.load(std::memory_order_acquire)) {
                if (st.load(std::memory_order_acquire) == 1) {
                    fn(ctx);
                    st.store(2, std::memory_order_release);
                } else {
                    std::this_thread::yield();
                }
            }
        });
    }
    ~AppThread() {
        quit.store(true, std::memory_order_release);
        th.join();
    }
    template <class F>
    void run(F&& f) {
        fn  = [](void* c) { (*static_cast<std::remove_reference_t<F>*>(c))(); };
        ctx = &f;
        st.store(1, std::memory_order_release);
        bool done = false;
        for (long spin = 0; spin < 200'000'000L && !done; ++spin) {
            done = st.load(std::memory_order_acquire) == 2;
            if (!done) std::this_thread::yield();
        }
        EXPECT_TRUE(done) << "the application thread never finished";
        st.store(0, std::memory_order_release);
    }
};

}  // namespace

// A handler that cannot take `rec` calls pause(rec.offset): the batch stops there, the broker side is released.
TEST(SubscriberPauseRegression, PauseInHandlerStopsMidBatchRegression) {
    Rig r;
    r.open(11);
    r.bind(40);
    int pauses = 0;
    r.hook = [&](const rc::Record& rec) {
        if (rec.offset == 4 && pauses == 0) { ++pauses; EXPECT_TRUE(r.sub->pause(rec.offset)); }
    };
    r.push(40, 0, 10);
    EXPECT_EQ(r.got, (std::vector<int64_t>{0, 1, 2, 3, 4})) << "4 is the refused one; nothing after it";
    EXPECT_TRUE(r.sub->paused());
    EXPECT_FALSE(r.sub->active());
    EXPECT_FALSE(r.sub->finished());
    EXPECT_EQ(r.tx.unsubsFor(40), 1) << "the broker-side subscription is released";
    EXPECT_EQ(r.sub->position(), 4);
    EXPECT_EQ(r.hub.activeCount(), 0u);
    EXPECT_EQ(r.hub.pendingCount(), 0u);
}

TEST(SubscriberPauseRegression, ResumeRedeliversFromOffsetRegression) {
    Rig r;
    r.open(11);
    r.bind(40);
    bool paused = false;
    r.hook = [&](const rc::Record& rec) {
        if (rec.offset == 4 && !paused) { paused = true; EXPECT_TRUE(r.sub->pause(rec.offset)); }
    };
    r.push(40, 0, 10);
    ASSERT_TRUE(r.sub->paused());

    const int before = r.tx.subscribes;
    EXPECT_TRUE(r.sub->resume());
    EXPECT_EQ(r.tx.subscribes, before + 1);
    EXPECT_EQ(r.tx.lastSubFrom, 4) << "Subscribe with start offset 4";
    EXPECT_EQ(r.tx.lastSubKnown, 1);
    EXPECT_NE(r.tx.lastSubCorr, 11) << "a new correlation id";
    EXPECT_EQ(r.hub.pendingCount(), 1u);
    EXPECT_FALSE(r.sub->paused());
    EXPECT_TRUE(r.sub->active());

    r.got.clear();
    r.bindLast(41);
    r.push(41, 0, 10);   // the broker pushes a whole batch from below the position
    EXPECT_EQ(r.got, (std::vector<int64_t>{4, 5, 6, 7, 8, 9})) << "each once, from the position";
    EXPECT_EQ(r.sub->position(), 10);
    EXPECT_FALSE(r.sub->resume()) << "not paused any more";
}

// pause() with no offset: delivery continues after the last record handed to the handler, from the app thread.
TEST(SubscriberPauseRegression, PauseDefaultContinuesAfterLastRegression) {
    Rig r;
    r.open(11, /*dutyHandle=*/false);
    r.bind(40);
    r.push(40, 0, 10);
    ASSERT_EQ(r.got.size(), 10u);
    ASSERT_EQ(r.sub->position(), 10);
    EXPECT_TRUE(r.sub->pause());
    EXPECT_TRUE(r.sub->paused());
    EXPECT_FALSE(r.sub->active());
    EXPECT_EQ(r.tx.unsubsFor(40), 1);
    EXPECT_TRUE(r.sub->resume());
    EXPECT_EQ(r.tx.lastSubFrom, 10);
    EXPECT_FALSE(r.sub->paused());
    r.got.clear();
    r.bindLast(41);
    r.push(41, 8, 6);   // 8..13: a batch straddling the position
    EXPECT_EQ(r.got, (std::vector<int64_t>{10, 11, 12, 13}));
}

TEST(SubscriberPauseRegression, LateBatchesOfOldIdDroppedRegression) {
    Rig r;
    r.open(11);
    r.bind(40);
    bool paused = false;
    r.hook = [&](const rc::Record& rec) {
        if (rec.offset == 4 && !paused) { paused = true; EXPECT_TRUE(r.sub->pause(rec.offset)); }
    };
    r.push(40, 0, 10);
    ASSERT_TRUE(r.sub->paused());
    ASSERT_EQ(r.got.size(), 5u);
    r.push(40, 10, 5);
    r.push(40, 15, 5);
    EXPECT_EQ(r.got.size(), 5u) << "no handler call for the old subscription id";
    EXPECT_EQ(r.sub->position(), 4);
}

TEST(SubscriberPauseRegression, OtherSubscriptionKeepsFlowingRegression) {
    Rig a;
    a.open(11);
    a.bind(40);
    // A second subscription on the same hub.
    std::vector<int64_t> bGot;
    auto bImpl = std::make_unique<rc::SubscriptionImpl>();
    bImpl->correlationId = 12;
    bImpl->topicId       = kTopic;
    bImpl->partition     = kPart;
    bImpl->fromWire      = -2;
    bImpl->handler       = [&](const rc::Record& rec) { bGot.push_back(rec.offset); };
    rc::Subscription bSub(bImpl.get(), &Rig::sink, &a, std::this_thread::get_id());
    {
        auto req = SubscriberHub::makeSubscribeRequest(bImpl.get(), kSession, 1, -2, 0, {}, kHour);
        rcd::HubCommand add;
        add.op     = rcd::HubCommand::Op::SubscribeAdd;
        add.handle = reinterpret_cast<uint64_t>(req.release());
        a.hub.applyCommand(add, 0, kSession, a.noSend, 0);
    }
    a.hub.onSubscribeResponse(bindRsp(12, 50, kSession), 0, kSession, a.noSend);
    ASSERT_TRUE(a.sub->pause());
    ASSERT_TRUE(a.sub->paused());
    const int unsubsBefore = a.tx.unsubscribes;
    for (int i = 0; i < 10; ++i) {
        std::vector<uint8_t> st;
        a.hub.onRecords(records(st, 50, i * 10, 10));
    }
    EXPECT_EQ(bGot.size(), 100u) << "all 100 records pushed to the other subscription are delivered";
    EXPECT_EQ(a.got.size(), 0u);
    EXPECT_FALSE(bSub.paused());
    EXPECT_TRUE(bSub.active());
    EXPECT_EQ(a.tx.unsubscribes, unsubsBefore) << "pausing one touches no other";
    EXPECT_EQ(a.hub.activeCount(), 1u);
}

// A paused subscription is bound to no session: a session loss ends nothing, and the new session gets it on resume only.
TEST(SubscriberPauseRegression, PausedAcrossSessionLossResumesExactlyOnceRegression) {
    Rig r;
    r.open(11);
    r.bind(40);
    bool paused = false;
    r.hook = [&](const rc::Record& rec) {
        if (rec.offset == 4 && !paused) { paused = true; EXPECT_TRUE(r.sub->pause(rec.offset)); }
    };
    r.push(40, 0, 10);
    ASSERT_TRUE(r.sub->paused());

    // The session is lost and a new one established, as the hub's session-loss path does.
    r.hub.setSessionConnected(false);
    EXPECT_EQ(r.hub.endSeedBound(), 0) << "the paused subscription is not bound on the lost session";
    r.sid = 99;
    r.hub.setSessionConnected(true);
    const int before = r.tx.subscribes;
    EXPECT_EQ(r.hub.resendPending(r.sid, r.noSend), 0);
    EXPECT_EQ(r.tx.subscribes, before) << "the recovery does not re-subscribe a paused subscription";
    EXPECT_TRUE(r.sub->paused());
    EXPECT_FALSE(r.sub->finished());

    EXPECT_TRUE(r.sub->resume());
    EXPECT_EQ(r.tx.subscribes, before + 1);
    EXPECT_EQ(r.tx.lastSubFrom, 4);
    EXPECT_EQ(r.tx.lastSubSession, 99) << "on the new session";
    r.got.clear();
    r.bindLast(61);
    r.push(61, 0, 10);
    EXPECT_EQ(r.got, (std::vector<int64_t>{4, 5, 6, 7, 8, 9}));
}

TEST(SubscriberPauseRegression, PausePendingSubscriptionRegression) {
    Rig r;
    r.open(11, /*dutyHandle=*/false);
    ASSERT_EQ(r.hub.pendingCount(), 1u);
    const int unsubsBefore = r.tx.unsubscribes;
    EXPECT_TRUE(r.sub->pause());
    EXPECT_TRUE(r.sub->paused());
    EXPECT_EQ(r.hub.pendingCount(), 0u) << "a paused subscription is in neither table";
    EXPECT_EQ(r.hub.activeCount(), 0u);
    EXPECT_EQ(r.tx.unsubscribes, unsubsBefore) << "the broker id is not known yet: nothing to release";

    // The late OK for the old correlation id is released with an Unsubscribe.
    r.hub.onSubscribeResponse(bindRsp(11, 40, r.sid), 777, r.sid, r.noSend);
    EXPECT_EQ(r.tx.unsubsFor(40), 1);
    EXPECT_EQ(r.hub.lateOksReleased(), 1u);
    EXPECT_EQ(r.hub.activeCount(), 0u);

    const int subsBefore = r.tx.subscribes;
    EXPECT_TRUE(r.sub->resume());
    EXPECT_EQ(r.tx.subscribes, subsBefore + 1);
    EXPECT_EQ(r.tx.lastSubFrom, -2) << "the original from";
    EXPECT_EQ(r.hub.pendingCount(), 1u);
}

TEST(SubscriberPauseRegression, UnsubscribeWhilePausedFinishesRegression) {
    Rig r;
    r.open(11);
    r.bind(40);
    r.push(40, 0, 3);
    ASSERT_TRUE(r.sub->pause());
    ASSERT_TRUE(r.sub->paused());
    r.sub->unsubscribe();
    EXPECT_TRUE(r.sub->finished());
    EXPECT_FALSE(r.sub->paused());
    EXPECT_FALSE(r.sub->resume());
    EXPECT_FALSE(r.sub->pause());
    EXPECT_EQ(r.sub->position(), 3);
    EXPECT_EQ(r.tx.unsubsFor(40), 1) << "released once (at the pause), not again";
}

TEST(SubscriberPauseRegression, UnsubscribeWhilePausedFromAppThreadRegression) {
    Rig r;
    r.open(11, /*dutyHandle=*/false);
    r.bind(40);
    r.push(40, 0, 3);
    ASSERT_TRUE(r.sub->pause());
    r.sub->unsubscribe();   // the app-thread form: a command the duty side applies by handle address
    EXPECT_TRUE(r.sub->finished());
    EXPECT_FALSE(r.sub->resume());
}

TEST(SubscriberPauseRegression, ClientCloseWhilePausedFinishesRegression) {
    Rig r;
    r.open(11);
    r.bind(40);
    r.push(40, 0, 3);
    ASSERT_TRUE(r.sub->pause());
    ASSERT_TRUE(r.sub->paused());
    r.hub.deactivateAll();   // the Client-close path
    EXPECT_TRUE(r.sub->finished());
    EXPECT_FALSE(r.sub->paused());
    EXPECT_FALSE(r.sub->resume());
}

TEST(SubscriberPauseRegression, SessionRevokedWhilePausedFinishesRegression) {
    Rig r;
    r.open(11);
    r.bind(40);
    ASSERT_TRUE(r.sub->pause());
    EXPECT_EQ(r.hub.endAllLive(rc::SubscriptionFailReason::SessionRevoked, 46), 1);
    EXPECT_TRUE(r.sub->finished());
    EXPECT_EQ(r.sub->failReason(), rc::SubscriptionFailReason::SessionRevoked);
    EXPECT_FALSE(r.sub->resume());
}

TEST(SubscriberPauseRegression, PauseAboveDeliveredRefusedRegression) {
    Rig r;
    r.open(11);
    r.bind(40);
    r.push(40, 0, 10);   // last delivered = 9, position 10
    ASSERT_EQ(r.sub->position(), 10);
    const int unsubs = r.tx.unsubscribes;
    EXPECT_FALSE(r.sub->pause(11)) << "last + 2";
    EXPECT_FALSE(r.sub->paused());
    EXPECT_TRUE(r.sub->active());
    EXPECT_EQ(r.tx.unsubscribes, unsubs);
    EXPECT_EQ(r.sub->position(), 10);
    EXPECT_EQ(r.hub.activeCount(), 1u);
    r.push(40, 10, 2);
    EXPECT_EQ(r.got.size(), 12u) << "delivery went on";
    EXPECT_TRUE(r.sub->pause(12)) << "last + 1 is allowed";
}

// After one pause / resume cycle, later cycles allocate nothing.
TEST(SubscriberPauseRegression, PauseResumeNoAllocRegression) {
    Rig r;
    r.tx.store = false;
    r.open(11);
    r.bind(40);
    r.got.reserve(1 << 12);
    std::vector<uint8_t> frame;
    {   // one batch, built once
        rune::protocol::RecordBatchBuilder b(0, 1'000'000);
        for (int i = 0; i < 3; ++i) b.append("k", "v", 1'000'000 + i);
        frame = b.build();
    }
    int64_t id = 40;
    auto cycle = [&] {
        EXPECT_TRUE(r.sub->pause());
        EXPECT_TRUE(r.sub->resume());
        r.bindLast(++id);
        rc::codec::RecordsView rv;
        rv.subscriptionId = id;
        rv.topicId        = kTopic;
        rv.partition      = kPart;
        rv.baseOffset     = 0;
        rv.recordBatch    = std::span<const uint8_t>(frame);
        rv.recordCount    = 3;
        r.hub.onRecords(rv);
    };
    cycle();   // warm-up: the first cycle may allocate (the parked request and the map nodes)
    ASSERT_EQ(r.got.size(), 3u);
    alloc_counter::startThisThread();
    for (int i = 0; i < 100; ++i) cycle();
    const long allocs = alloc_counter::stopThisThread();
    EXPECT_EQ(allocs, 0) << "pause / resume cycles after the first allocate nothing";
    EXPECT_FALSE(r.sub->paused());
    EXPECT_EQ(r.hub.activeCount(), 1u);
    EXPECT_EQ(r.hub.pendingCount(), 0u);
}

// A LeaseWatch's subscriptions are not pausable: the handle refuses.
TEST(SubscriberPauseRegression, NotPausableHandleRefusesRegression) {
    Rig r;
    r.open(11);
    r.bind(40);
    r.sub->markNotPausable();
    EXPECT_FALSE(r.sub->pause());
    EXPECT_FALSE(r.sub->pause(0));
    EXPECT_FALSE(r.sub->paused());
    EXPECT_FALSE(r.sub->resume());
    EXPECT_TRUE(r.sub->active());
    EXPECT_EQ(r.tx.unsubscribes, 0);
    r.push(40, 0, 3);
    EXPECT_EQ(r.got.size(), 3u);
}

// A pause inside the handler takes effect at once, and a default pause() there keeps the record being handled as delivered.
TEST(SubscriberPauseRegression, DefaultPauseInHandlerCountsTheCurrentRecordRegression) {
    Rig r;
    r.open(11);
    r.bind(40);
    r.hook = [&](const rc::Record& rec) {
        if (rec.offset == 2) EXPECT_TRUE(r.sub->pause());
    };
    r.push(40, 0, 6);
    EXPECT_EQ(r.got, (std::vector<int64_t>{0, 1, 2}));
    EXPECT_EQ(r.sub->position(), 3) << "the record handed to the handler counts";
    ASSERT_TRUE(r.sub->resume());
    EXPECT_EQ(r.tx.lastSubFrom, 3);
}

// An in-handler pause(rec.offset) works on a log with gaps (the position is far below the record's offset).
TEST(SubscriberPauseRegression, PauseOfRecordAfterAGapRegression) {
    Rig r;
    r.open(11);
    r.bind(40);
    r.push(40, 0, 2);   // position 2
    r.hook = [&](const rc::Record& rec) {
        if (rec.offset == 100) EXPECT_TRUE(r.sub->pause(rec.offset)) << "the record's own offset is allowed";
    };
    r.push(40, 100, 3);
    EXPECT_EQ(r.got, (std::vector<int64_t>{0, 1, 100}));
    EXPECT_EQ(r.sub->position(), 100);
    ASSERT_TRUE(r.sub->resume());
    EXPECT_EQ(r.tx.lastSubFrom, 100);
}

// pause(offset) of a subscription that delivered nothing, to offset 0: resumes at 0, not at the original From.
TEST(SubscriberPauseRegression, PauseToZeroResumesAtZeroRegression) {
    Rig r;
    r.open(11, true, /*fromWire=*/-1);   // From::latest
    r.bind(40);
    ASSERT_TRUE(r.sub->pause(0));
    ASSERT_TRUE(r.sub->resume());
    EXPECT_EQ(r.tx.lastSubFrom, 0);
}

// A paused subscription pausing again only lowers its position; resume once; a refused Unsubscribe is not lost.
TEST(SubscriberPauseRegression, PauseWhilePausedLowersThePositionOnlyRegression) {
    Rig r;
    r.open(11);
    r.bind(40);
    r.push(40, 0, 10);
    ASSERT_TRUE(r.sub->pause());
    EXPECT_EQ(r.sub->position(), 10);
    EXPECT_TRUE(r.sub->pause(7));
    EXPECT_EQ(r.sub->position(), 7);
    EXPECT_FALSE(r.sub->pause(9)) << "may not be raised";
    EXPECT_EQ(r.sub->position(), 7);
    EXPECT_TRUE(r.sub->pause()) << "no offset: unchanged";
    EXPECT_EQ(r.sub->position(), 7);
    EXPECT_EQ(r.tx.unsubsFor(40), 1);
    ASSERT_TRUE(r.sub->resume());
    EXPECT_EQ(r.tx.lastSubFrom, 7);
}

// ---------------------------------------------------------------------------------------------------------------------------
// The matrix: pause(resumeAt) x the state of the subscription.
// ---------------------------------------------------------------------------------------------------------------------------
namespace {
enum class St { Pending, BoundEmpty, BoundDelivered, Paused, Unsubscribed, TimedOut, MovedPending };
const char* name(St s) {
    switch (s) {
    case St::Pending: return "Pending";
    case St::BoundEmpty: return "BoundEmpty";
    case St::BoundDelivered: return "BoundDelivered";
    case St::Paused: return "Paused";
    case St::Unsubscribed: return "Unsubscribed";
    case St::TimedOut: return "TimedOut";
    case St::MovedPending: return "MovedPending";
    }
    return "?";
}
}  // namespace

TEST(SubscriberPauseRegression, PauseMatrixRegression) {
    const St states[]       = {St::Pending, St::BoundEmpty, St::BoundDelivered, St::Paused,
                               St::Unsubscribed, St::TimedOut, St::MovedPending};
    const int64_t resumeAts[] = {-1, 0, 2, 5, 6};
    int cases = 0;
    for (const St s : states) {
        for (const int64_t at : resumeAts) {
            // duty: a callback on the duty thread (Thread mode); invoker: the owner thread of an Invoker client (an app-path
            // handle whose sink applies at once and answers with the hub's verdict); ring: an application thread (the
            // command is applied through the ring and answered "accepted").
            for (const int kind : {0, 1, 2}) {
                const bool dutyHandle = kind == 0;
                SCOPED_TRACE(std::string(name(s)) + " resumeAt=" + std::to_string(at) + (kind == 0 ? " duty" : kind == 1 ? " invoker" : " ring"));
                Rig r;
                r.viaRing = kind == 2;
                r.open(11, dutyHandle);
                int64_t bound = 0;
                bool    finished = false, wasPaused = false, wasBound = false;
                switch (s) {
                case St::Pending: break;
                case St::BoundEmpty: r.bind(40); wasBound = true; break;
                case St::BoundDelivered: r.bind(40); r.push(40, 0, 5); bound = 5; wasBound = true; break;
                case St::Paused:
                    r.bind(40); r.push(40, 0, 5);
                    ASSERT_TRUE(r.sub->pause(2));
                    bound = 2; wasPaused = true;
                    break;
                case St::Unsubscribed:
                    r.bind(40); r.push(40, 0, 5);
                    r.sub->unsubscribe();
                    bound = 5; finished = true;
                    break;
                case St::TimedOut:
                    ASSERT_EQ(r.hub.sweepTimedOut(2 * kHour), 1);
                    finished = true;
                    break;
                case St::MovedPending:
                    r.bind(40); r.push(40, 0, 5);
                    ASSERT_TRUE(r.hub.rearmMovedAway(11, 2000, 2001, r.sid, r.sid, 10 * kHour));
                    r.corr = 2000;
                    bound = 5;
                    break;
                }
                const int unsubsBefore = r.tx.unsubscribes;
                const bool ok = !finished && (at < 0 || at <= bound);
                EXPECT_EQ(r.sub->pause(at), ok);
                EXPECT_EQ(r.sub->paused(), !finished && (wasPaused || ok));
                EXPECT_EQ(r.sub->finished(), finished);
                EXPECT_EQ(r.sub->active(), !finished && !(wasPaused || ok));
                const int64_t expectPos = (ok && at >= 0) ? at : bound;
                if (s != St::Pending && s != St::TimedOut && s != St::BoundEmpty) EXPECT_EQ(r.sub->position(), expectPos);
                EXPECT_EQ(r.tx.unsubscribes - unsubsBefore, (ok && wasBound) ? 1 : 0);
                if (ok || wasPaused) {
                    EXPECT_EQ(r.hub.pendingCount(), 0u);
                    EXPECT_EQ(r.hub.activeCount(), 0u);
                }
                // resume: exactly the paused ones subscribe again.
                const int subsBefore = r.tx.subscribes;
                const bool wantResume = !finished && (wasPaused || ok);
                EXPECT_EQ(r.sub->resume(), wantResume);
                EXPECT_EQ(r.tx.subscribes - subsBefore, wantResume ? 1 : 0);
                EXPECT_FALSE(r.sub->resume()) << "never twice";
                ++cases;
            }
        }
    }
    EXPECT_EQ(cases, 105);
}

// ---------------------------------------------------------------------------------------------------------------------------
// Seeded stress against a model: the delivered offsets, with each pause(offset) rewinding the expected next offset.
// ---------------------------------------------------------------------------------------------------------------------------
TEST(SubscriberPauseRegression, PauseStressModelRegression) {
    const uint32_t seed = [] {
        if (const char* e = std::getenv("RUNE_PAUSE_SEED")) return static_cast<uint32_t>(std::strtoul(e, nullptr, 10));
        return 20261111u;
    }();
    std::printf("PauseStressModelRegression seed=%u\n", seed);
    std::mt19937 rng(seed);
    auto rnd = [&](int lo, int hi) { return std::uniform_int_distribution<int>(lo, hi)(rng); };

    Rig r;
    r.open(11);   // From::earliest
    enum class Inst { Pending, Bound, Paused };
    Inst    inst         = Inst::Pending;
    int64_t modelNext    = 0;     // the next offset the application must see
    int64_t pendingFrom  = 0;     // where the pending instance's broker side starts reading
    int64_t cursor       = 0;     // the live broker instance's next batch
    int64_t instSubId    = 0;     // the live bound instance
    int64_t lastBoundId  = 0;
    int64_t idGen        = 100;
    std::vector<std::pair<int64_t, int64_t>> staleOks;   // (corr, subId): pending Subscribes that were paused away
    int64_t pendingSubIdForStale = 0;
    int     handlerMode  = 0;     // 0 none, 1 pause(offset) at the next record, 2 pause() at the next record
    int64_t pauseAtOffset = -1;
    long    deliveries   = 0, pauses = 0, resumes = 0, moves = 0, losses = 0;

    auto toPaused = [&] {   // the instance leaves the tables
        if (inst == Inst::Bound) { lastBoundId = instSubId; }
        if (inst == Inst::Pending) staleOks.emplace_back(r.corr, ++idGen);
        inst = Inst::Paused;
    };

    r.hook = [&](const rc::Record& rec) {
        ++deliveries;
        EXPECT_EQ(inst, Inst::Bound);
        EXPECT_EQ(rec.offset, modelNext) << "seed " << seed;
        modelNext = rec.offset + 1;
        if (handlerMode != 0 && (pauseAtOffset < 0 || rec.offset >= pauseAtOffset)) {
            const int unsubs = r.tx.unsubscribes;
            if (handlerMode == 1) {
                EXPECT_TRUE(r.sub->pause(rec.offset));
                modelNext = rec.offset;   // refused
            } else {
                EXPECT_TRUE(r.sub->pause());
            }
            EXPECT_EQ(r.tx.unsubscribes, unsubs + 1);
            handlerMode = 0;
            ++pauses;
            toPaused();
        }
    };

    for (int step = 0; step < 10000; ++step) {
        ASSERT_FALSE(::testing::Test::HasFailure()) << "seed " << seed << " step " << step;
        switch (rnd(0, 9)) {
        case 0:
        case 1: {   // the broker answers the pending Subscribe (and any stale one)
            if (inst == Inst::Pending) {
                instSubId = ++idGen;
                r.hub.onSubscribeResponse(bindRsp(r.corr, instSubId, r.sid), 0, r.sid, r.noSend);
                inst   = Inst::Bound;
                cursor = std::max<int64_t>(0, pendingFrom - rnd(0, 3));
            }
            if (!staleOks.empty() && rnd(0, 1) == 0) {
                const auto [c, id] = staleOks.back();
                staleOks.pop_back();
                const int unsubs = r.tx.unsubscribes;
                r.hub.onSubscribeResponse(bindRsp(c, id, r.sid), 0, r.sid, r.noSend);
                EXPECT_EQ(r.tx.unsubscribes, unsubs + 1) << "a late OK of a paused-away Subscribe is released";
            }
            break;
        }
        case 2:
        case 3:
        case 4: {   // push a batch (to the live instance, or to an old one that must be dropped)
            const int n = rnd(1, 6);
            if (inst == Inst::Bound) {
                handlerMode   = rnd(0, 9) == 0 ? 1 : rnd(0, 9) == 0 ? 2 : 0;
                pauseAtOffset = handlerMode != 0 && rnd(0, 1) ? cursor + rnd(0, n) : -1;
                const int64_t posBefore = r.sub->position();
                (void)posBefore;
                r.push(instSubId, cursor, n);
                if (inst == Inst::Bound) cursor += n;    // fully delivered (or trimmed)
                handlerMode = 0;
            } else {
                const size_t before = r.got.size();
                r.push(lastBoundId != 0 ? lastBoundId : 9999, rnd(0, 50), n);
                EXPECT_EQ(r.got.size(), before) << "a batch of a released instance is dropped";
            }
            break;
        }
        case 5: {   // pause() from outside a handler
            const bool ok = r.sub->pause();
            EXPECT_TRUE(ok);
            if (inst != Inst::Paused) { toPaused(); ++pauses; }
            break;
        }
        case 6: {   // pause(offset) from outside a handler
            const int64_t off = rnd(0, static_cast<int>(modelNext) + 2);
            const bool want = off <= modelNext;
            const bool ok = r.sub->pause(off);
            EXPECT_EQ(ok, want) << "seed " << seed << " off " << off << " next " << modelNext;
            if (ok) {
                modelNext = off;
                if (inst != Inst::Paused) { toPaused(); ++pauses; }
            }
            EXPECT_EQ(r.sub->position(), modelNext);
            break;
        }
        case 7: {   // resume
            const int subs = r.tx.subscribes;
            const bool ok = r.sub->resume();
            EXPECT_EQ(ok, inst == Inst::Paused);
            if (ok) {
                ++resumes;
                EXPECT_EQ(r.tx.subscribes, subs + 1);
                EXPECT_TRUE(r.tx.lastSubFrom == modelNext || (modelNext == 0 && r.tx.lastSubFrom == -2))
                    << "seed " << seed << " from " << r.tx.lastSubFrom << " next " << modelNext;
                r.corr      = r.tx.lastSubCorr;
                pendingFrom = modelNext;
                inst        = Inst::Pending;
            } else {
                EXPECT_EQ(r.tx.subscribes, subs);
            }
            break;
        }
        case 8: {   // a session loss: a paused (or pending) subscription outlives it
            if (inst == Inst::Bound) break;   // a bound one would end with SessionLost (not a pause concern)
            r.hub.setSessionConnected(false);
            EXPECT_EQ(r.hub.endSeedBound(), 0);
            r.sid += 1;
            r.hub.setSessionConnected(true);
            (void)r.hub.resendPending(r.sid, r.noSend);
            staleOks.clear();   // they died with the session
            EXPECT_FALSE(r.sub->finished());
            ++losses;
            break;
        }
        case 9: {   // a leader move
            const int64_t newCorr = ++r.nextCorr;
            ++r.nextCorr;
            const bool ok = r.hub.rearmMovedAway(r.corr, newCorr, newCorr + 1, r.sid, r.sid, 100 * kHour);
            EXPECT_EQ(ok, inst == Inst::Bound);
            if (ok) {
                r.corr      = newCorr;
                inst        = Inst::Pending;
                pendingFrom = modelNext;
                ++moves;
            }
            break;
        }
        }
        EXPECT_EQ(r.sub->paused(), inst == Inst::Paused);
        EXPECT_FALSE(r.sub->finished());
        EXPECT_EQ(r.sub->position(), modelNext) << "seed " << seed << " step " << step;
    }
    std::printf("  deliveries=%ld pauses=%ld resumes=%ld moves=%ld losses=%ld\n", deliveries, pauses, resumes, moves, losses);
    EXPECT_GT(deliveries, 1000);
    EXPECT_GT(pauses, 100);
    EXPECT_GT(resumes, 100);
    EXPECT_GT(moves, 10);
    EXPECT_GT(losses, 10);
}

// A SubscribePause command that raced a rewind and carries a position above the bound is clamped to "no offset":
// the position is not moved forward past what was delivered.
TEST(SubscriberPauseRegression, PauseCommandAboveBoundIsClampedRegression) {
    Rig r;
    r.open(11, false);
    r.bind(40);
    r.push(40, 0, 10);   // delivered up to 9, position 10
    ASSERT_EQ(r.sub->position(), 10);
    rcd::HubCommand cmd;
    cmd.op     = rcd::HubCommand::Op::SubscribePause;
    cmd.handle = reinterpret_cast<uint64_t>(r.impl.get());
    cmd.a      = 500;   // far above the bound
    r.hub.applyCommand(cmd, ++r.nextCorr, r.sid, r.noSend, r.nowNs);
    EXPECT_EQ(r.impl->nextOffset, 10) << "clamped: the position did not jump";
    EXPECT_TRUE(r.impl->paused);
}

// ---------------------------------------------------------------------------------------------------------
// A pause made INSIDE the handler (duty thread) is visible to the application thread, which can resume it.
// The test thread is the duty thread; the AppThread's calls on the same handle take the app path.
// ---------------------------------------------------------------------------------------------------------

TEST(SubscriberPauseRegression, HandlerPauseVisibleToAppThreadRegression) {
    Rig r;
    AppThread app;
    r.open(11);
    r.bind(40);
    bool paused = false;
    r.hook = [&](const rc::Record& rec) {
        if (rec.offset == 4 && !paused) { paused = true; EXPECT_TRUE(r.sub->pause(rec.offset)); }
    };
    r.push(40, 0, 10);
    ASSERT_EQ(r.got, (std::vector<int64_t>{0, 1, 2, 3, 4}));
    bool pausedSeen = false, activeSeen = true, resumed = false, pausedAfter = true;
    app.run([&] {
        pausedSeen = r.sub->paused();
        activeSeen = r.sub->active();
        resumed    = r.sub->resume();
        pausedAfter = r.sub->paused();
    });
    EXPECT_TRUE(pausedSeen) << "the app thread sees the pause the handler made";
    EXPECT_FALSE(activeSeen);
    EXPECT_TRUE(resumed);
    EXPECT_FALSE(pausedAfter);
    EXPECT_EQ(r.tx.lastSubFrom, 4) << "the client sent a Subscribe at 4";
    r.got.clear();
    r.bindLast(41);
    r.push(41, 0, 10);
    EXPECT_EQ(r.got, (std::vector<int64_t>{4, 5, 6, 7, 8, 9})) << "4..9 once each";
    EXPECT_FALSE(r.sub->paused());
}

TEST(SubscriberPauseRegression, HandlerPauseAppResumeCyclesRegression) {
    constexpr int kCycles = 50;
    Rig r;
    AppThread app;
    r.open(11);
    r.bind(40);
    int64_t refuse = -1;
    r.hook = [&](const rc::Record& rec) {
        if (rec.offset == refuse) { refuse = -1; EXPECT_TRUE(r.sub->pause(rec.offset)); }
    };
    std::vector<int64_t> expect;
    int64_t id = 40;
    for (int k = 0; k < kCycles; ++k) {
        const int64_t b = 3 * k;
        refuse = b + 1;
        r.push(id, b, 3);   // b, then b+1 refused
        expect.insert(expect.end(), {b, b + 1});
        bool first = false, second = true;
        app.run([&] {
            first  = r.sub->resume();
            second = r.sub->resume();
        });
        ASSERT_TRUE(first) << "cycle " << k;
        ASSERT_FALSE(second) << "cycle " << k << ": a second resume right after finds it not paused";
        ASSERT_EQ(r.tx.lastSubFrom, b + 1) << "cycle " << k;
        r.bindLast(++id);
        r.push(id, b + 1, 2);   // b+1 (accepted now), b+2
        expect.insert(expect.end(), {b + 1, b + 2});
    }
    EXPECT_EQ(r.got, expect) << "each offset delivered once, the refused one handed over again after the resume";
    EXPECT_EQ(r.sub->position(), 3 * kCycles);
    EXPECT_FALSE(r.sub->paused());
}

TEST(SubscriberPauseRegression, HandlerResumeClearsAppViewRegression) {
    Rig r;
    AppThread app;
    r.open(11);
    r.bind(40);
    r.push(40, 0, 5);
    bool paused = false;
    app.run([&] { paused = r.sub->pause(); });   // the app pauses
    ASSERT_TRUE(paused);
    ASSERT_TRUE(r.sub->paused());
    EXPECT_TRUE(r.sub->resume()) << "the duty thread resumes it (as a callback of another subscription would)";
    bool pausedSeen = true, resumedAgain = true;
    app.run([&] {
        pausedSeen   = r.sub->paused();
        resumedAgain = r.sub->resume();
    });
    EXPECT_FALSE(pausedSeen) << "the app thread learns of the duty-thread resume";
    EXPECT_FALSE(resumedAgain);
    r.got.clear();
    r.bindLast(41);
    r.push(41, 5, 3);
    EXPECT_EQ(r.got, (std::vector<int64_t>{5, 6, 7}));
}

TEST(SubscriberPauseRegression, AppPauseThenAppResumeBackToBackRegression) {
    Rig r;
    AppThread app;
    r.open(11);
    r.bind(40);
    bool hp = false;
    r.hook = [&](const rc::Record& rec) {
        if (rec.offset == 2 && !hp) { hp = true; EXPECT_TRUE(r.sub->pause(rec.offset)); }   // an earlier handler pause (seq 1)
    };
    r.push(40, 0, 5);
    app.run([&] { EXPECT_TRUE(r.sub->resume()); });
    r.bindLast(41);
    r.push(41, 2, 3);   // 2 (accepted), 3, 4
    ASSERT_EQ(r.sub->position(), 5);
    bool p = false, rs = false, pausedAfter = true;
    app.run([&] {   // before any duty cycle runs
        p  = r.sub->pause();
        rs = r.sub->resume();
        pausedAfter = r.sub->paused();
    });
    EXPECT_TRUE(p);
    EXPECT_TRUE(rs);
    EXPECT_FALSE(pausedAfter) << "the app's own intent wins: no stale paused()";
    EXPECT_EQ(r.tx.lastSubFrom, 5);
    r.got.clear();
    r.bindLast(42);
    r.push(42, 5, 3);
    EXPECT_EQ(r.got, (std::vector<int64_t>{5, 6, 7}));
    bool pausedLater = true;
    app.run([&] { pausedLater = r.sub->paused(); });
    EXPECT_FALSE(pausedLater);
}

TEST(SubscriberPauseRegression, HandlerPauseAppResumeNoAllocRegression) {
    constexpr int kCycles = 101;   // 1 warm-up + 100
    Rig r;
    AppThread app;
    r.tx.store = false;
    r.open(11);
    r.bind(40);
    r.got.reserve(1 << 12);
    std::vector<std::vector<uint8_t>> whole(kCycles), rest(kCycles);   // built before the window
    for (int k = 0; k < kCycles; ++k) {
        rune::protocol::RecordBatchBuilder b(3 * k, 1'000'000);
        for (int i = 0; i < 3; ++i) b.append("k", "v", 1'000'000 + i);
        whole[k] = b.build();
        rune::protocol::RecordBatchBuilder c(3 * k + 1, 1'000'000);
        for (int i = 0; i < 2; ++i) c.append("k", "v", 1'000'000 + i);
        rest[k] = c.build();
    }
    int64_t refuse = -1;
    r.hook = [&](const rc::Record& rec) {
        if (rec.offset == refuse) { refuse = -1; (void)r.sub->pause(rec.offset); }
    };
    int64_t id = 40;
    bool resumed = false;
    auto resumeOnApp = [&] { resumed = r.sub->resume(); };
    auto feed = [&](const std::vector<uint8_t>& frame, int64_t base, int32_t count, int64_t subId) {
        rc::codec::RecordsView rv;
        rv.subscriptionId = subId;
        rv.topicId        = kTopic;
        rv.partition      = kPart;
        rv.baseOffset     = base;
        rv.recordBatch    = std::span<const uint8_t>(frame);
        rv.recordCount    = count;
        r.hub.onRecords(rv);
    };
    auto cycle = [&](int k) {
        refuse = 3 * k + 1;
        feed(whole[k], 3 * k, 3, id);
        resumed = false;
        app.run(resumeOnApp);
        EXPECT_TRUE(resumed);
        r.bindLast(++id);
        feed(rest[k], 3 * k + 1, 2, id);
    };
    cycle(0);   // warm-up: the first cycle may allocate (parked nodes)
    ASSERT_EQ(r.sub->position(), 3);
    alloc_counter::start();
    for (int k = 1; k < kCycles; ++k) cycle(k);
    const long allocs = alloc_counter::stop();
    EXPECT_EQ(allocs, 0) << "handler-pause / app-resume cycles after the first allocate nothing";
    EXPECT_EQ(r.sub->position(), 3 * kCycles);
    EXPECT_FALSE(r.sub->paused());
}

// ---------------------------------------------------------------------------------------------------------
// Batch C review repros (red first): Invoker-mode handler pause, pause + resume in one handler call, the app handle's
// view after a handler pause, a refused Unsubscribe at the pause, and the final-status reserve.
// ---------------------------------------------------------------------------------------------------------

// C-1: Invoker mode (the handle has no duty-thread id): pause(rec.offset) inside the handler, mid-batch.
TEST(SubscriberPauseRegression, InvokerHandlerPauseAtRecordOffsetMidBatchRegression) {
    Rig r;
    r.open(11, /*dutyHandle=*/false);
    r.bind(40);
    bool ok = true; int calls = 0;
    r.hook = [&](const rc::Record& rec) {
        if (rec.offset == 4 && calls == 0) { ++calls; ok = r.sub->pause(rec.offset); }
    };
    r.push(40, 0, 10);
    EXPECT_TRUE(ok) << "pause(rec.offset) refused inside the handler in Invoker mode";
    EXPECT_EQ(r.got, (std::vector<int64_t>{0, 1, 2, 3, 4}));
    EXPECT_TRUE(r.sub->paused());
}

// I-1: pause(rec.offset) then resume() inside the same handler call: the rest of the old batch is dropped, record 4 once more.
TEST(SubscriberPauseRegression, PauseThenResumeInsideHandlerRegression) {
    Rig r;
    r.open(11);
    r.bind(40);
    int n = 0;
    r.hook = [&](const rc::Record& rec) {
        if (rec.offset == 4 && n == 0) { ++n; EXPECT_TRUE(r.sub->pause(rec.offset)); EXPECT_TRUE(r.sub->resume()); }
    };
    r.push(40, 0, 10);
    EXPECT_EQ(r.got, (std::vector<int64_t>{0, 1, 2, 3, 4})) << "the old batch stops at the pause";
    EXPECT_EQ(r.tx.lastSubFrom, 4);
    r.bindLast(41);
    r.push(41, 4, 6);
    EXPECT_EQ(r.got, (std::vector<int64_t>{0, 1, 2, 3, 4, 4, 5, 6, 7, 8, 9})) << "4 handed over again, everything else once";
}

// I-1: pause() (no offset) then resume() inside the same handler call: the current record counts as delivered.
TEST(SubscriberPauseRegression, DefaultPauseThenResumeInsideHandlerRegression) {
    Rig r;
    r.open(11);
    r.bind(40);
    int n = 0;
    r.hook = [&](const rc::Record& rec) {
        if (rec.offset == 4 && n == 0) { ++n; EXPECT_TRUE(r.sub->pause()); EXPECT_TRUE(r.sub->resume()); }
    };
    r.push(40, 0, 10);
    EXPECT_EQ(r.got, (std::vector<int64_t>{0, 1, 2, 3, 4}));
    EXPECT_EQ(r.tx.lastSubFrom, 5);
    r.bindLast(41);
    r.push(41, 5, 5);
    EXPECT_EQ(r.got, (std::vector<int64_t>{0, 1, 2, 3, 4, 5, 6, 7, 8, 9})) << "each once";
}

// C-2: a handler pause earlier in life, then an app-thread pause whose command is applied after a Records frame.
TEST(SubscriberPauseRegression, AppPauseRacingAFrameAfterAHandlerPauseRegression) {
    Rig r;
    AppThread app;
    r.deferApp = true;
    r.open(11);
    r.bind(40);
    bool hp = false;
    r.hook = [&](const rc::Record& rec) {
        if (rec.offset == 2 && !hp) { hp = true; EXPECT_TRUE(r.sub->pause(rec.offset)); }
    };
    r.push(40, 0, 5);
    bool seen = false, res = false;
    app.run([&] { seen = r.sub->paused(); res = r.sub->resume(); });
    ASSERT_TRUE(seen); ASSERT_TRUE(res);
    r.applyQueued();
    r.bindLast(41);                              // the bind snapshot
    app.run([&] { (void)r.sub->position(); });   // the app polls: drains the bind snapshot
    bool p = false;
    app.run([&] { p = r.sub->pause(); });        // app-thread pause: queued on the ring
    ASSERT_TRUE(p);
    r.push(41, 2, 3);                            // a Records frame is dispatched before the ring is drained
    app.run([&] { (void)r.sub->paused(); });     // the app polls: drains the snapshot that frame published
    r.applyQueued();                             // the duty thread applies the pause
    ASSERT_TRUE(r.impl->paused) << "the hub paused it";
    bool after = false, resumed = false;
    app.run([&] { after = r.sub->paused(); resumed = r.sub->resume(); });
    EXPECT_TRUE(after) << "the app thread must see its own pause";
    EXPECT_TRUE(resumed) << "resume() must work: the subscription is paused in the hub";
}

// I-2: the transport refuses the Unsubscribe at the pause; then the session is lost: a paused subscription is untouched.
TEST(SubscriberPauseRegression, RefusedUnsubscribeAtPauseThenSessionLossRegression) {
    Rig r;
    r.open(11);
    r.bind(40);
    r.push(40, 0, 5);
    r.tx.refuseUnsub = true;
    ASSERT_TRUE(r.sub->pause());
    ASSERT_TRUE(r.sub->paused());
    r.hub.setSessionConnected(false);
    EXPECT_EQ(r.hub.endSeedBound(), 0) << "a paused subscription is bound to no session";
    EXPECT_FALSE(r.sub->finished()) << "a session loss does nothing to a paused subscription";
    EXPECT_TRUE(r.sub->paused());
    r.tx.refuseUnsub = false;
    r.sid += 1;
    r.hub.setSessionConnected(true);
    EXPECT_TRUE(r.sub->resume()) << "resume() subscribes again at the stored offset";
    EXPECT_EQ(r.tx.lastSubFrom, 5);
}

// M-1: refused Unsubscribe at the pause; then unsubscribe(): nothing is left to scan once the owed release went out.
TEST(SubscriberPauseRegression, RefusedUnsubscribeAtPauseThenUnsubscribeRegression) {
    Rig r;
    r.open(11);
    r.bind(40);
    r.push(40, 0, 5);
    r.tx.refuseUnsub = true;
    ASSERT_TRUE(r.sub->pause());
    r.tx.refuseUnsub = false;
    r.sub->unsubscribe();
    EXPECT_TRUE(r.sub->finished());
    EXPECT_EQ(r.hub.activeCount(), 0u);
    for (int i = 0; i < 10; ++i) (void)r.hub.retryPendingUnsubscribes(r.sid);
    EXPECT_EQ(r.hub.pendingUnsubscribes(), 0u) << "the owed release was sent and forgotten";
    EXPECT_EQ(r.tx.unsubsFor(40), 1);
}

// I-3: refused Unsubscribe at the pause and again at the resume: the old broker subscription is still released.
TEST(SubscriberPauseRegression, RefusedUnsubscribeAtPauseAndResumeRegression) {
    Rig r;
    r.open(11);
    r.bind(40);
    r.push(40, 0, 5);
    r.tx.refuseUnsub = true;
    ASSERT_TRUE(r.sub->pause());
    ASSERT_TRUE(r.sub->resume());
    r.tx.refuseUnsub = false;
    for (int i = 0; i < 10; ++i) (void)r.hub.retryPendingUnsubscribes(r.sid);
    EXPECT_EQ(r.tx.unsubsFor(40), 1) << "the broker-side subscription 40 is released once the transport accepts";
}

// I-4: pause / resume cycles made on the duty thread with no app-thread read: the re-bind snapshots must not eat the reserve.
TEST(SubscriberPauseRegression, ManyResumesKeepTheFinalStatusRegression) {
    Rig r;
    AppThread app;
    r.open(11);
    r.bind(40);
    int64_t id = 40;
    for (int i = 0; i < 40; ++i) {
        ASSERT_TRUE(r.sub->pause());
        ASSERT_TRUE(r.sub->resume());
        r.bindLast(++id);
        r.push(id, i, 1);
    }
    EXPECT_EQ(r.hub.endAllLive(rc::SubscriptionFailReason::SessionRevoked, 46), 1);
    rc::SubscriptionFailReason why = rc::SubscriptionFailReason::None; int64_t pos = -1; bool fin = false;
    app.run([&] { fin = r.sub->finished(); why = r.sub->failReason(); pos = r.sub->position(); });
    EXPECT_TRUE(fin);
    EXPECT_EQ(why, rc::SubscriptionFailReason::SessionRevoked) << "the final snapshot was lost";
    EXPECT_EQ(pos, 40);
}

// M-3: after a data reset the position is back at 0: a duty-path pause(oldOffset) names a record that no longer exists.
TEST(SubscriberPauseRegression, DataResetLowersThePauseBoundRegression) {
    Rig r;
    r.open(11);
    r.bind(40);
    r.push(40, 0, 10);                                   // delivered up to 9
    ASSERT_TRUE(r.hub.rearmMovedAway(11, 2000, 2001, r.sid, r.sid, 10 * kHour));
    ASSERT_TRUE(r.hub.restartFromLogStart(2000, r.sid, 10 * kHour));
    ASSERT_EQ(r.impl->nextOffset, 0);
    EXPECT_FALSE(r.sub->pause(9)) << "the old delivered offset is above the new position";
    EXPECT_FALSE(r.sub->paused());
    EXPECT_TRUE(r.sub->pause(0));
}

// M-2: a handle whose subscription the hub has not adopted (its SubscribeAdd is still queued) cannot be paused or resumed
// on the duty thread: the verdict is the hub's, so the handle says false instead of claiming a pause nothing applied.
TEST(SubscriberPauseRegression, PauseOfAnUnadoptedSubscriptionRefusedRegression) {
    Rig r;
    auto impl2 = std::make_unique<rc::SubscriptionImpl>();
    impl2->topicId = kTopic;
    rc::Subscription s2(impl2.get(), &Rig::sink, &r, r.dutyId);
    EXPECT_FALSE(s2.pause());
    EXPECT_FALSE(s2.paused());
    EXPECT_FALSE(s2.resume());
    EXPECT_FALSE(impl2->paused);
}

// ---------------------------------------------------------------------------------------------------------------------------
// I-6: the in-handler timings x handle kind x record position x refused Unsubscribe.
// ---------------------------------------------------------------------------------------------------------------------------
TEST(SubscriberPauseRegression, PauseInHandlerTimingMatrixRegression) {
    enum class Op { PauseOff, PauseDefault, PauseOffThenResume, PauseDefaultThenResume, ResumeWhileRunning };
    const char* opName[] = {"PauseOff", "PauseDefault", "PauseOffThenResume", "PauseDefaultThenResume", "ResumeWhileRunning"};
    int cases = 0;
    for (const bool dutyHandle : {true, false}) {
        for (const Op op : {Op::PauseOff, Op::PauseDefault, Op::PauseOffThenResume, Op::PauseDefaultThenResume, Op::ResumeWhileRunning}) {
            for (const int64_t k : {0, 4, 9}) {
                for (const bool refuse : {false, true}) {
                    SCOPED_TRACE(std::string(dutyHandle ? "duty " : "invoker ") + opName[static_cast<int>(op)] + " at " +
                                 std::to_string(k) + (refuse ? " unsub refused" : ""));
                    Rig r;
                    r.open(11, dutyHandle);
                    r.bind(40);
                    r.tx.refuseUnsub = refuse;
                    bool fired = false;
                    r.hook = [&](const rc::Record& rec) {
                        if (rec.offset != k || fired) return;
                        fired = true;
                        switch (op) {
                        case Op::PauseOff:               EXPECT_TRUE(r.sub->pause(rec.offset)); break;
                        case Op::PauseDefault:           EXPECT_TRUE(r.sub->pause()); break;
                        case Op::PauseOffThenResume:     EXPECT_TRUE(r.sub->pause(rec.offset)); EXPECT_TRUE(r.sub->resume()); break;
                        case Op::PauseDefaultThenResume: EXPECT_TRUE(r.sub->pause()); EXPECT_TRUE(r.sub->resume()); break;
                        case Op::ResumeWhileRunning:     EXPECT_FALSE(r.sub->resume()); break;
                        }
                    };
                    r.push(40, 0, 10);
                    std::vector<int64_t> want;
                    const bool pauses  = op != Op::ResumeWhileRunning;
                    const bool resumes = op == Op::PauseOffThenResume || op == Op::PauseDefaultThenResume;
                    const bool offset  = op == Op::PauseOff || op == Op::PauseOffThenResume;
                    for (int64_t i = 0; i <= (pauses ? k : 9); ++i) want.push_back(i);
                    EXPECT_EQ(r.got, want) << "the old frame stops at the pause";
                    const int64_t pos = !pauses ? 10 : offset ? k : k + 1;
                    EXPECT_EQ(r.impl->nextOffset, pos);
                    EXPECT_EQ(r.impl->paused, pauses && !resumes);
                    EXPECT_FALSE(r.sub->finished());
                    if (pauses) {
                        // The Unsubscribe is on the wire, or owed: it goes out once the transport accepts, exactly once.
                        EXPECT_EQ(r.tx.unsubsFor(40), refuse ? 0 : 1);
                        r.tx.refuseUnsub = false;
                        for (int i = 0; i < 3; ++i) (void)r.hub.retryPendingUnsubscribes(r.sid);
                        EXPECT_EQ(r.tx.unsubsFor(40), 1);
                        EXPECT_EQ(r.hub.pendingUnsubscribes(), 0u);
                    }
                    if (resumes) {
                        EXPECT_EQ(r.tx.lastSubFrom, pos);
                        r.bindLast(41);
                        if (pos < 10) r.push(41, pos, 10 - static_cast<int32_t>(pos));
                        std::vector<int64_t> all = want;
                        for (int64_t i = pos; i < 10; ++i) all.push_back(i);
                        EXPECT_EQ(r.got, all) << "the rest exactly once (the refused record again for pause(offset))";
                    } else if (pauses) {
                        r.hub.setSessionConnected(false);   // a session loss does nothing to a paused subscription
                        EXPECT_EQ(r.hub.endSeedBound(), 0);
                        EXPECT_FALSE(r.sub->finished());
                        EXPECT_TRUE(r.sub->paused());
                        r.sub->unsubscribe();
                        EXPECT_TRUE(r.sub->finished());
                    }
                    EXPECT_EQ(r.hub.pendingUnsubscribes(), 0u);
                    ++cases;
                }
            }
        }
    }
    EXPECT_EQ(cases, 60);
}

// I-6: commands from the application thread that the duty thread applies a frame (or several) late.
TEST(SubscriberPauseRegression, AppCommandsDelayedByFramesRegression) {
    struct Case { const char* name; bool pauseFirst; bool resumeToo; int64_t at; };
    const Case cases[] = {{"pause", true, false, -1}, {"pause then resume", true, true, -1}, {"pause(3)", true, false, 3},
                          {"pause(3) then resume", true, true, 3}, {"pause above the snapshot", true, false, 7}};
    for (const Case& c : cases) {
        SCOPED_TRACE(c.name);
        Rig r;
        AppThread app;
        r.deferApp = true;
        r.open(11);
        r.bind(40);
        r.push(40, 0, 5);                                   // the snapshot says: position 5
        bool p = false, rs = false;
        app.run([&] { p = r.sub->pause(c.at); });
        if (c.at == 7) { EXPECT_FALSE(p) << "above the app's last snapshot: refused, nothing queued"; EXPECT_TRUE(r.queued.empty()); continue; }
        ASSERT_TRUE(p);
        app.run([&] { EXPECT_TRUE(r.sub->paused()); if (c.resumeToo) rs = r.sub->resume(); });
        EXPECT_EQ(rs, c.resumeToo);
        r.push(40, 5, 5);                                   // a frame is dispatched before the ring is drained
        EXPECT_EQ(r.got.size(), 10u) << "the pause had not been applied yet";
        r.applyQueued();
        r.hub.republishDirty();
        bool paused = false;
        app.run([&] { paused = r.sub->paused(); });
        EXPECT_EQ(paused, r.impl->paused) << "the handle converges to the hub";
        EXPECT_EQ(r.impl->paused, !c.resumeToo);
        if (c.at >= 0) EXPECT_EQ(r.impl->nextOffset, c.at);
        else           EXPECT_EQ(r.impl->nextOffset, 10);
        if (!c.resumeToo) {                                 // and a later resume() from the application thread works
            bool again = false;
            app.run([&] { again = r.sub->resume(); });
            EXPECT_TRUE(again);
            r.applyQueued();
            EXPECT_FALSE(r.impl->paused);
        }
    }
}

// I-2 / M-6: the owed list is full: the pause is refused (nothing changes), never silently dropped.
TEST(SubscriberPauseRegression, FullOwedListRefusesThePauseRegression) {
    Rig r;
    r.open(11);
    r.bind(40);
    r.push(40, 0, 1);
    r.tx.refuseUnsub = true;
    int64_t id = 40;
    int ok = 0;
    for (int i = 0; i < 200 && r.sub->pause(); ++i) {   // every cycle owes one release
        ++ok;
        ASSERT_TRUE(r.sub->resume());
        r.bindLast(++id);
        r.push(id, i + 1, 1);
    }
    EXPECT_EQ(ok, 64) << "the fixed capacity of the owed list";
    EXPECT_FALSE(r.sub->paused()) << "the 65th pause is refused";
    EXPECT_EQ(r.hub.activeCount(), 1u) << "and the subscription is still bound and delivering";
    const size_t before = r.got.size();
    r.push(id, 65, 1);
    EXPECT_EQ(r.got.size(), before + 1);
    // Once the transport accepts again every owed release goes out, then a pause is possible again.
    r.tx.refuseUnsub = false;
    for (int i = 0; i < 3; ++i) (void)r.hub.retryPendingUnsubscribes(r.sid);
    EXPECT_EQ(r.hub.pendingUnsubscribes(), 0u);
    for (int64_t x = 40; x < id; ++x) EXPECT_EQ(r.tx.unsubsFor(x), 1) << "owed release of " << x;
    EXPECT_TRUE(r.sub->pause());
}

// ---------------------------------------------------------------------------------------------------------------------------
// I-5: the second model-checked stress.  Operations: push (with a pause / pause + resume made inside the handler), direct
// pause / pause(offset) / resume on the owner thread, application-thread pause / resume that the duty thread applies later
// (applyQueued is its own operation), a session loss in every state, a leader move, a refusing transport, and the duty
// cycle that retries owed releases.  Invariants: each offset is delivered exactly once, in order; the hub's paused state
// and position equal the model's; the handle's paused() converges to the hub's once the ring is empty; every release the
// broker is owed goes out exactly once, or is dropped with its session; nothing is left pending at the end.
// ---------------------------------------------------------------------------------------------------------------------------
namespace {
struct StressTotals { long deliveries = 0, pauses = 0, resumes = 0, handlerPauseResume = 0, appOps = 0, applies = 0, moves = 0,
                      lossesPaused = 0, lossesBound = 0, refusedPauses = 0, owedSent = 0, owedDropped = 0; };

void runPauseStressEpisode(uint32_t seed, int episode, StressTotals& tot) {
    std::mt19937 rng(seed * 7919u + static_cast<uint32_t>(episode) * 104729u + 1u);
    auto rnd = [&](int lo, int hi) { return std::uniform_int_distribution<int>(lo, hi)(rng); };
    SCOPED_TRACE("seed " + std::to_string(seed) + " episode " + std::to_string(episode));

    Rig r;
    r.open(11, /*dutyHandle=*/false);   // an Invoker-owner handle: direct calls get the hub's verdict, "app" calls are queued
    r.deferApp = true;
    enum class Inst { Pending, Bound, Paused };
    Inst    inst         = Inst::Pending;
    int64_t modelNext    = 0;
    int64_t pendingFrom  = 0;
    int64_t cursor       = 0;
    int64_t instSubId    = 0;
    int64_t lastBoundId  = 0;
    int64_t idGen        = 100;
    std::vector<std::pair<int64_t, int64_t>> staleOks;    // (corr, subId): pending Subscribes that were paused away
    struct Owed { int64_t id; int64_t sid; };
    std::vector<Owed>         owed;                         // the releases the hub must retry
    std::map<int64_t, int>    expectUnsub;                  // broker subscription id -> how many Unsubscribes must reach the wire
    int     handlerMode  = 0;   // 0 none, 1 pause(off), 2 pause(), 3 pause(off)+resume, 4 pause()+resume
    int64_t pauseAtOffset = -1;
    bool    ended        = false;

    auto toPaused = [&] {
        if (inst == Inst::Bound) {
            lastBoundId = instSubId;
            if (r.tx.refuseUnsub) owed.push_back({instSubId, r.sid});
            else                  expectUnsub[instSubId] += 1;
        }
        if (inst == Inst::Pending) staleOks.emplace_back(r.corr, ++idGen);
        inst = Inst::Paused;
    };
    auto settleOwed = [&] {   // what one duty cycle of retryPendingUnsubscribes does to the owed list
        for (auto it = owed.begin(); it != owed.end();) {
            if (it->sid != r.sid)          { expectUnsub[it->id] += 0; ++tot.owedDropped; it = owed.erase(it); }
            else if (!r.tx.refuseUnsub)    { expectUnsub[it->id] += 1; ++tot.owedSent; it = owed.erase(it); }
            else                           ++it;
        }
    };
    auto dutyCycle = [&] { (void)r.hub.retryPendingUnsubscribes(r.sid); settleOwed(); };
    auto ensureRoom = [&] {   // the owed list holds 64: keep the model away from the refusal (its own test covers it)
        if (r.tx.refuseUnsub && owed.size() >= 60) { r.tx.refuseUnsub = false; dutyCycle(); r.tx.refuseUnsub = true; }
    };
    auto resumedNow = [&] { ++tot.resumes; pendingFrom = modelNext; inst = Inst::Pending; r.corr = r.tx.lastSubCorr; };

    r.hook = [&](const rc::Record& rec) {
        ++tot.deliveries;
        EXPECT_EQ(inst, Inst::Bound);
        EXPECT_EQ(rec.offset, modelNext);
        modelNext = rec.offset + 1;
        if (handlerMode != 0 && (pauseAtOffset < 0 || rec.offset >= pauseAtOffset)) {
            const int mode = handlerMode;
            handlerMode = 0;
            const int unsubs = r.tx.unsubscribes;
            if (mode == 1 || mode == 3) { EXPECT_TRUE(r.sub->pause(rec.offset)); modelNext = rec.offset; }
            else                        { EXPECT_TRUE(r.sub->pause()); }
            EXPECT_EQ(r.tx.unsubscribes, unsubs + (r.tx.refuseUnsub ? 0 : 1));
            toPaused();
            ++tot.pauses;
            if (mode >= 3) {
                EXPECT_TRUE(r.sub->resume());
                ++tot.handlerPauseResume;
                resumedNow();
            }
        }
    };

    for (int step = 0; step < 400 && !ended; ++step) {
        ASSERT_FALSE(::testing::Test::HasFailure()) << "step " << step;
        switch (rnd(0, 14)) {
        case 0:
        case 1: {   // the broker answers the pending Subscribe (and any stale one)
            if (inst == Inst::Pending) {
                instSubId = ++idGen;
                r.hub.onSubscribeResponse(bindRsp(r.corr, instSubId, r.sid), 0, r.sid, r.noSend);
                inst   = Inst::Bound;
                cursor = std::max<int64_t>(0, pendingFrom - rnd(0, 3));
            }
            if (!staleOks.empty() && rnd(0, 1) == 0) {
                const auto [c, id] = staleOks.back();
                staleOks.pop_back();
                const int unsubs = r.tx.unsubscribes;
                r.hub.onSubscribeResponse(bindRsp(c, id, r.sid), 0, r.sid, r.noSend);
                EXPECT_EQ(r.tx.unsubscribes, unsubs + (r.tx.refuseUnsub ? 0 : 1)) << "a late OK of a paused-away Subscribe is released once";
            }
            break;
        }
        case 2:
        case 3:
        case 4: {   // push a batch (to the live instance, or to an old one that must be dropped)
            const int n = rnd(1, 6);
            if (inst == Inst::Bound) {
                ensureRoom();
                const int pick = rnd(0, 19);
                handlerMode   = pick == 0 ? 1 : pick == 1 ? 2 : pick == 2 ? 3 : pick == 3 ? 4 : 0;
                pauseAtOffset = handlerMode != 0 && rnd(0, 1) ? cursor + rnd(0, n) : -1;
                r.push(instSubId, cursor, n);
                if (inst == Inst::Bound) cursor += n;
                handlerMode = 0;
            } else {
                const size_t before = r.got.size();
                r.push(lastBoundId != 0 ? lastBoundId : 9999, rnd(0, 50), n);
                EXPECT_EQ(r.got.size(), before) << "a batch of a released instance is dropped";
            }
            break;
        }
        case 5: {   // pause() on the owner thread, outside a handler
            ensureRoom();
            const int unsubs = r.tx.unsubscribes;
            EXPECT_TRUE(r.sub->pause());
            EXPECT_EQ(r.tx.unsubscribes, unsubs + (inst == Inst::Bound && !r.tx.refuseUnsub ? 1 : 0));
            if (inst != Inst::Paused) { toPaused(); ++tot.pauses; }
            break;
        }
        case 6: {   // pause(offset) on the owner thread
            ensureRoom();
            const int64_t off = rnd(0, static_cast<int>(modelNext) + 2);
            const bool want = off <= modelNext;
            const bool ok = r.sub->pause(off);
            EXPECT_EQ(ok, want) << "off " << off << " next " << modelNext;
            if (ok) {
                modelNext = off;
                if (inst != Inst::Paused) { toPaused(); ++tot.pauses; }
            }
            break;
        }
        case 7: {   // resume() on the owner thread
            const int subs = r.tx.subscribes;
            const bool ok = r.sub->resume();
            if (r.queued.empty()) EXPECT_EQ(ok, inst == Inst::Paused);
            if (ok) {
                EXPECT_EQ(inst, Inst::Paused) << "the hub resumes only a paused subscription";
                EXPECT_EQ(r.tx.subscribes, subs + 1);
                EXPECT_TRUE(r.tx.lastSubFrom == modelNext || (modelNext == 0 && r.tx.lastSubFrom == -2))
                    << "from " << r.tx.lastSubFrom << " next " << modelNext;
                resumedNow();
            } else {
                EXPECT_EQ(r.tx.subscribes, subs);
            }
            break;
        }
        case 8: {   // pause from the application thread: queued on the command ring
            ++tot.appOps;
            r.viaRing = true;
            const bool sent = r.sub->pause(rnd(0, 1) ? -1 : rnd(0, static_cast<int>(modelNext)));
            r.viaRing = false;
            if (sent) EXPECT_TRUE(r.sub->paused()) << "a handle sees its own pause at once, before the hub applied it";
            break;
        }
        case 9: {   // resume from the application thread
            ++tot.appOps;
            r.viaRing = true;
            const bool sent = r.sub->resume();
            r.viaRing = false;
            if (sent) EXPECT_FALSE(r.sub->paused()) << "a handle sees its own resume at once, before the hub applied it";
            break;
        }
        case 10: {  // the duty thread applies what is queued (a Records frame may have been dispatched before)
            ensureRoom();
            const auto q = r.queued;
            for (const auto& cmd : q) {
                if (cmd.op == rcd::HubCommand::Op::SubscribePause) {
                    const int64_t eff = (cmd.a >= 0 && cmd.a <= modelNext) ? cmd.a : -1;   // above the bound: treated as "no offset"
                    if (eff >= 0) modelNext = eff;
                    if (inst != Inst::Paused) { toPaused(); ++tot.pauses; }
                } else if (inst == Inst::Paused) {
                    const int subs = r.tx.subscribes;
                    r.hub.applyCommand(cmd, ++r.nextCorr, r.sid, r.noSend, r.nowNs);   // applied here: the Subscribe goes out
                    EXPECT_EQ(r.tx.subscribes, subs + 1);
                    resumedNow();
                    continue;
                }
                r.hub.applyCommand(cmd, ++r.nextCorr, r.sid, r.noSend, r.nowNs);
            }
            r.queued.clear();
            r.hub.republishDirty();
            ++tot.applies;
            break;
        }
        case 11: {  // a session loss
            if (inst == Inst::Bound) {   // a bound one ends with SessionLost (as the hub does today): the episode is over
                r.hub.setSessionConnected(false);
                EXPECT_EQ(r.hub.endSeedBound(), 1);
                EXPECT_TRUE(r.sub->finished());
                EXPECT_EQ(r.sub->failReason(), rc::SubscriptionFailReason::SessionLost);
                ++tot.lossesBound;
                ended = true;
            } else {                     // paused or pending: it outlives the loss
                r.hub.setSessionConnected(false);
                EXPECT_EQ(r.hub.endSeedBound(), 0);
                r.sid += 1;
                r.hub.setSessionConnected(true);
                (void)r.hub.resendPending(r.sid, r.noSend);
                staleOks.clear();
                EXPECT_FALSE(r.sub->finished());
                ++tot.lossesPaused;
            }
            break;
        }
        case 12: {  // a leader move
            const int64_t newCorr = ++r.nextCorr;
            ++r.nextCorr;
            const int64_t oldId = instSubId;
            const int unsubs = r.tx.unsubscribes;
            const bool ok = r.hub.rearmMovedAway(r.corr, newCorr, newCorr + 1, r.sid, r.sid, 100 * kHour);
            EXPECT_EQ(ok, inst == Inst::Bound);
            if (ok) {
                if (!r.tx.refuseUnsub) { expectUnsub[oldId] += 1; EXPECT_EQ(r.tx.unsubscribes, unsubs + 1); }
                r.corr = newCorr;
                inst = Inst::Pending;
                pendingFrom = modelNext;
                ++tot.moves;
            }
            break;
        }
        case 13: {  // the transport starts / stops refusing Unsubscribes
            r.tx.refuseUnsub = !r.tx.refuseUnsub;
            break;
        }
        case 14: {  // a duty cycle
            dutyCycle();
            break;
        }
        }
        if (ended) break;
        (void)r.sub->position();   // the handle drains its status ring
        EXPECT_EQ(r.impl->paused, inst == Inst::Paused) << "step " << step;
        EXPECT_EQ(r.impl->nextOffset, modelNext) << "step " << step;
        EXPECT_EQ(r.hub.activeCount(), inst == Inst::Bound ? 1u : 0u);
        EXPECT_EQ(r.hub.pendingCount(), inst == Inst::Pending ? 1u : 0u);
        EXPECT_FALSE(r.sub->finished());
        if (r.queued.empty()) EXPECT_EQ(r.sub->paused(), inst == Inst::Paused) << "the handle converges to the hub, step " << step;
    }
    // The end: the transport accepts; every owed release goes out (or is dropped with its session).
    r.tx.refuseUnsub = false;
    for (int i = 0; i < 3; ++i) dutyCycle();
    EXPECT_EQ(r.hub.pendingUnsubscribes(), 0u) << "nothing left to retry";
    EXPECT_TRUE(owed.empty());
    for (const auto& [id, n] : expectUnsub) EXPECT_EQ(r.tx.unsubsFor(id), n) << "broker subscription " << id;
    ++tot.deliveries;   // (keeps the totals non-trivial for an empty episode)
    --tot.deliveries;
}
}  // namespace

TEST(SubscriberPauseRegression, PauseStressRingRefusalAndHandlerRegression) {
    const uint32_t seeds[] = {20261111u, 7u, 99991u, 31337u, 424242u};
    StressTotals tot;
    for (const uint32_t seed : seeds) {
        for (int ep = 0; ep < 30; ++ep) {
            runPauseStressEpisode(seed, ep, tot);
            ASSERT_FALSE(::testing::Test::HasFailure()) << "seed " << seed << " episode " << ep;
        }
    }
    std::printf("  deliveries=%ld pauses=%ld resumes=%ld handlerPR=%ld appOps=%ld applies=%ld moves=%ld lossPaused=%ld lossBound=%ld owedSent=%ld owedDropped=%ld\n",
                tot.deliveries, tot.pauses, tot.resumes, tot.handlerPauseResume, tot.appOps, tot.applies, tot.moves,
                tot.lossesPaused, tot.lossesBound, tot.owedSent, tot.owedDropped);
    EXPECT_GT(tot.deliveries, 1000);
    EXPECT_GT(tot.pauses, 300);
    EXPECT_GT(tot.resumes, 100);
    EXPECT_GT(tot.handlerPauseResume, 10);
    EXPECT_GT(tot.appOps, 300);
    EXPECT_GT(tot.applies, 100);
    EXPECT_GT(tot.lossesPaused, 20);
    EXPECT_GT(tot.lossesBound, 10);
    EXPECT_GT(tot.owedSent, 10);
    EXPECT_GT(tot.owedDropped, 0);
}

// N-1: the command number 0 means "no number" (a duty-thread call).  Across the wrap of the 32-bit counter the handle skips
// 0, so the echo keeps matching and a handler pause is still adopted by the handle.
TEST(SubscriberPauseRegression, EchoNumberSkipsZeroOnWrapRegression) {
    Rig r;
    AppThread app;
    r.open(11);
    r.bind(40);
    rc::SubscriptionTestAccess::setSeq(*r.sub, 0xFFFFFFFEu);
    r.impl->appSeqApplied = 0xFFFFFFFEu;   // the hub echoed everything so far
    bool p = false, rs = false;
    app.run([&] { p = r.sub->pause(); });      // number 0xFFFFFFFF
    app.run([&] { rs = r.sub->resume(); });    // the next number: must not be 0
    ASSERT_TRUE(p);
    ASSERT_TRUE(rs);
    r.bindLast(41);
    bool hp = false;
    r.hook = [&](const rc::Record& rec) {
        if (rec.offset == 1 && !hp) { hp = true; EXPECT_TRUE(r.sub->pause(rec.offset)); }   // a duty-thread call: no number
    };
    r.push(41, 0, 4);
    ASSERT_TRUE(r.impl->paused);
    bool seen = false, back = false;
    app.run([&] { seen = r.sub->paused(); back = r.sub->resume(); });
    EXPECT_TRUE(seen) << "the handler's pause is adopted after the counter wrapped";
    EXPECT_TRUE(back);
    EXPECT_NE(r.impl->appSeqApplied, 0u);
}

// N-4: a pause() inside the catalog caught-up callback is not "inside a record handler": no record is being handled, so
// resume() must not settle the position to deliveringOffset + 1.  (Today the two agree on a catalog stream; the test makes
// them differ - an offset consumed without delivery - to pin the rule.)
TEST(SubscriberPauseRegression, PauseInsideCaughtUpCallbackDoesNotRewindRegression) {
    Rig r;
    r.open(11);
    r.impl->topicId = rune::protocol::client::kCatalogTopicId;
    int64_t hw = -1;
    bool    resumedOk = false;
    r.impl->onCaughtUp = [&](int64_t h, uint64_t) {
        hw = h;
        EXPECT_TRUE(r.sub->pause());
        resumedOk = r.sub->resume();
    };
    r.bind(40);
    {
        auto first = records(r.st, 40, 0, 3);   // delivered 0..2: deliveringOffset 2, nextOffset 3
        first.topicId = rune::protocol::client::kCatalogTopicId;
        r.hub.onRecords(first);
    }
    ASSERT_EQ(r.got, (std::vector<int64_t>{0, 1, 2}));
    r.impl->nextOffset = 7;                 // offsets 3..6 were consumed without being delivered
    std::array<uint8_t, 8> epoch{};         // the caught-up marker: an 8-byte epoch body, baseOffset = the high watermark
    rc::codec::RecordsView rv;
    rv.subscriptionId = 40;
    rv.topicId        = rune::protocol::client::kCatalogTopicId;
    rv.partition      = kPart;
    rv.baseOffset     = 7;
    rv.recordBatch    = std::span<const uint8_t>(epoch);
    r.hub.onRecords(rv);
    ASSERT_EQ(hw, 7);
    ASSERT_TRUE(resumedOk);
    EXPECT_EQ(r.impl->nextOffset, 7) << "the position must not go back to the last delivered record + 1";
    EXPECT_EQ(r.tx.lastSubFrom, 7) << "the re-subscribe starts at the position";
    r.bindLast(41);
    auto second = records(r.st, 41, 7, 2);
    second.topicId = rune::protocol::client::kCatalogTopicId;
    r.hub.onRecords(second);
    EXPECT_EQ(r.got, (std::vector<int64_t>{0, 1, 2, 7, 8})) << "no duplicate, no skip";
}

// N-5: a refused pause changes nothing - not the paused state, the dispatch markers, nor the release bookkeeping.
TEST(SubscriberPauseRegression, RefusedPauseChangesNothingRegression) {
    Rig r;
    r.open(11);
    r.bind(40);
    r.push(40, 0, 1);
    r.tx.refuseUnsub = true;
    int64_t id = 40;
    for (int i = 0; i < 64; ++i) {   // fill the owed list
        ASSERT_TRUE(r.sub->pause());
        ASSERT_TRUE(r.sub->resume());
        r.bindLast(++id);
        r.push(id, i + 1, 1);
    }
    const int64_t corrBefore = r.impl->unsubCorrId, sidBefore = r.impl->unsubSessionId;
    bool refused = false;
    r.hook = [&](const rc::Record& rec) {
        if (rec.offset == 65) refused = !r.sub->pause();   // inside the handler: the markers would be set
    };
    r.push(id, 65, 3);
    ASSERT_TRUE(refused) << "the owed list is full";
    EXPECT_FALSE(r.impl->paused);
    EXPECT_FALSE(r.impl->pausedOnce && r.impl->paused);
    EXPECT_FALSE(r.impl->pauseAfterCurrent);
    EXPECT_FALSE(r.impl->stopDispatch);
    EXPECT_EQ(r.impl->unsubCorrId, corrBefore);
    EXPECT_EQ(r.impl->unsubSessionId, sidBefore);
    EXPECT_EQ(r.got.back(), 67) << "the batch ran to its end: the refused pause stopped nothing";
}
