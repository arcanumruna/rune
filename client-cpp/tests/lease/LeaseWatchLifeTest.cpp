// tests/lease/LeaseWatchLifeTest.cpp - the LeaseWatch life states (Fresh, Running, Failed, Closing, Closed): closeAsync() never blocks and never
// calls client.doWork(); isClosed(); a destructor that returns at once.  Against the stub broker of LeaseWatchRig.h on a fake clock; every drive
// loop is capped and nothing sleeps.
#include "LeaseWatchRig.h"

#include <map>

namespace {

using Life = rc::LeaseWatchBase::Life;

constexpr int kCap = 200;   // the hard iteration cap of every drive loop here

void breakPartition(WRig& r, int32_t p);

/// Closes the watch from inside the first event of `closeOn`; forwards every event to the recorder first.
struct CloseOn {
    Recorder*           rec = nullptr;
    rc::LeaseWatchBase* w   = nullptr;
    Kind                closeOn = Kind::Vanished;
    bool                armed = true;
    static void fn(void* c, const rc::LeaseEvent& e) {
        auto* h = static_cast<CloseOn*>(c);
        h->rec->on(e);
        if (h->armed && e.kind == h->closeOn) { h->armed = false; h->w->closeAsync(); }
    }
};

/// 2 partitions, 3 leases granted before the watch starts, the watch driven to Ready.
struct LifeRig {
    WRig             r;
    rc::LeaseWatchBase w;
    Recorder         rec;
    explicit LifeRig(int32_t in, uint32_t parts = 2) : r(invokerCfg(in, in + 1, static_cast<int32_t>(parts)), parts), w(*r.client, r.topic->core()) {
        for (int i = 0; i < 3; ++i) r.stub->parts[static_cast<std::size_t>(i) % parts].grant(kb(i));
        w.setHandler(&Recorder::fn, &rec);
    }
    void startReady() {
        ASSERT_EQ(startWith(w, {Bytes{}}), rc::OpResult::Accepted);
        ASSERT_TRUE(r.drive(w, [&] { return w.isReady(); }));
        ASSERT_EQ(rec.count(Kind::Granted), 3);
    }
    /// At most kCap rounds until the watch is closed.
    bool driveClosed() {
        for (int i = 0; i < kCap; ++i) {
            r.round(w);
            if (w.isClosed()) return true;
        }
        return false;
    }
};

TEST(LeaseWatchLifeRegression, CloseAsyncThenClosedRegression) {
    LifeRig f(6101);
    f.startReady();
    const std::size_t events = f.rec.evs.size();
    f.w.closeAsync();
    EXPECT_FALSE(f.w.isClosed());
    EXPECT_EQ(f.w.life(), Life::Closing);
    EXPECT_FALSE(f.w.isReady());
    ASSERT_TRUE(f.driveClosed());
    EXPECT_EQ(f.w.life(), Life::Closed);
    f.r.rounds(f.w, 10);   // the Unsubscribe frames reach the stub broker
    EXPECT_EQ(f.r.gw.unsubscribed.size(), f.r.parts);
    EXPECT_EQ(f.rec.evs.size(), events) << "no event after closeAsync";
}

// Pins the property (a Closed watch's destructor pumps nothing); it does NOT guard the destructor's fast path: with the fast path removed the
// wait loop below it also breaks at once (a Closed watch holds no subscription), and in Invoker mode unsubscribe() finishes synchronously even
// for a Running watch.  The Thread-mode destructor tests (LeaseWatchThreadRegression) cover the destructor in every life, and one destroys the
// watch after the client.
TEST(LeaseWatchLifeRegression, DestructorOfClosedWatchDoesNoWorkRegression) {
    WRig r(invokerCfg(6103, 6104, 2), 2);
    for (int i = 0; i < 3; ++i) r.stub->parts[static_cast<std::size_t>(i) % 2].grant(kb(i));
    auto w = std::make_unique<rc::LeaseWatchBase>(*r.client, r.topic->core());
    Recorder rec;
    w->setHandler(&Recorder::fn, &rec);
    ASSERT_EQ(startWith(*w, {Bytes{}}), rc::OpResult::Accepted);
    ASSERT_TRUE(r.drive(*w, [&] { return w->isReady(); }));
    w->closeAsync();
    for (int i = 0; i < kCap && !w->isClosed(); ++i) r.round(*w);
    ASSERT_TRUE(w->isClosed());
    const uint64_t before   = r.client->dutyCyclesForTest();
    const std::size_t unsub = r.gw.unsubscribed.size();
    w.reset();
    EXPECT_EQ(r.client->dutyCyclesForTest(), before) << "the destructor of a closed watch pumps nothing";
    EXPECT_EQ(r.gw.unsubscribed.size(), unsub) << "and sends nothing";
}

TEST(LeaseWatchLifeRegression, CloseAsyncInsideHandlerRegression) {
    WRig r(invokerCfg(6105, 6106, 2), 2);
    for (int i = 0; i < 3; ++i) r.stub->parts[static_cast<std::size_t>(i) % 2].grant(kb(i));
    rc::LeaseWatchBase w(*r.client, r.topic->core());
    struct H {
        rc::LeaseWatchBase* w;
        Recorder rec;
        bool armed = true;
        int granted = 0, after = 0;
        static void fn(void* c, const rc::LeaseEvent& e) {
            auto* h = static_cast<H*>(c);
            h->rec.on(e);
            if (e.kind != Kind::Granted) { if (h->w->isClosed() || h->w->life() == Life::Closing) ++h->after; return; }
            ++h->granted;
            if (h->armed) h->w->closeAsync();
        }
    } h{&w};
    w.setHandler(&H::fn, &h);
    ASSERT_EQ(startWith(w, {Bytes{}}), rc::OpResult::Accepted);
    for (int i = 0; i < kCap && !w.isClosed(); ++i) r.round(w);
    EXPECT_TRUE(w.isClosed());
    EXPECT_EQ(w.life(), Life::Closed);
    EXPECT_EQ(h.granted, 1) << "the other leases' events are not delivered";
    EXPECT_EQ(h.after, 0);
    EXPECT_EQ(w.liveCount(), 1u) << "and the other leases are not inserted silently either";
    // The restart folds to the live set: the lease the closed run announced is confirmed, the two it never announced are granted.
    h.armed = false;
    const Bytes none;
    const std::span<const uint8_t> sp[] = {std::span<const uint8_t>(none)};
    ASSERT_EQ(w.restart(sp), rc::OpResult::Accepted);
    ASSERT_TRUE(r.drive(w, [&] { return h.rec.readies == 1; }));
    EXPECT_EQ(w.liveCount(), 3u);
    EXPECT_EQ(h.rec.held.size(), 3u);
    EXPECT_EQ(h.rec.count(Kind::Confirmed), 1);
    EXPECT_EQ(h.rec.count(Kind::Granted), 3);
    EXPECT_EQ(h.rec.bad, 0);
}

// F-11 (iii): closeAsync while the client's broker session is lost.  The broker expired the session; the close is issued before the
// client reconnected.  The watch still ends Closed (no hang, no event after the close, every subscription finished), and a restart over
// the re-established session runs again to Ready.
TEST(LeaseWatchLifeRegression, CloseAsyncWhileSessionLostThenRestartRegression) {
    LifeRig f(6107);
    f.startReady();
    const std::size_t events = f.rec.evs.size();
    const std::size_t connectsBefore = f.r.gw.connectAuthTokens.size();
    f.r.gw.expireSession(f.r.client->testSeedSessionId());
    f.w.closeAsync();
    EXPECT_EQ(f.w.life(), Life::Closing);
    ASSERT_TRUE(f.driveClosed()) << "the close completes although the session is gone";
    EXPECT_EQ(f.w.life(), Life::Closed);
    EXPECT_EQ(f.rec.evs.size(), events) << "no event after closeAsync, whatever the session did";
    f.r.rounds(f.w, 30);
    EXPECT_EQ(f.rec.evs.size(), events);
    const Bytes none;
    const std::span<const uint8_t> sp[] = {std::span<const uint8_t>(none)};
    ASSERT_EQ(f.w.restart(sp), rc::OpResult::Accepted);
    ASSERT_TRUE(f.r.drive(f.w, [&] { return f.rec.readies == 2; })) << "the restarted watch reached Ready on the new session";
    EXPECT_EQ(f.rec.bad, 0);
    EXPECT_EQ(f.w.liveCount(), 3u);
    EXPECT_GT(f.r.gw.connectAuthTokens.size(), connectsBefore) << "the session really was lost and re-established (a new Connect)";
}

TEST(LeaseWatchLifeRegression, CloseAsyncOnFreshIsClosedRegression) {
    WRig r(invokerCfg(6107, 6108, 2), 2);
    const uint64_t before = r.client->dutyCyclesForTest();
    {
        rc::LeaseWatchBase w(*r.client, r.topic->core());
        EXPECT_TRUE(w.isClosed());
        EXPECT_EQ(w.life(), Life::Fresh);
        w.closeAsync();
        EXPECT_TRUE(w.isClosed());
        EXPECT_EQ(w.life(), Life::Closed);
        EXPECT_EQ(startWith(w, {Bytes{}}), rc::OpResult::InvalidArgument) << "a closed watch cannot start";
    }
    EXPECT_EQ(r.client->dutyCyclesForTest(), before);
}

TEST(LeaseWatchLifeRegression, CloseAsyncOnFailedRegression) {
    WRig r(invokerCfg(6109, 6110, 1), 1);
    r.stub->parts[0].grant(kb(1));
    r.stub->errorCode = static_cast<int16_t>(rpc::ErrorCode::NotAuthorized);
    r.stub->errorNext = 1'000'000;
    rc::LeaseWatchBase w(*r.client, r.topic->core());
    Recorder rec;
    w.setHandler(&Recorder::fn, &rec);
    ASSERT_EQ(startWith(w, {Bytes{}}), rc::OpResult::Accepted);
    for (int i = 0; i < kCap && !w.failed(); ++i) { g_now += 2 * kMs; r.round(w); }
    ASSERT_EQ(w.life(), Life::Failed);
    EXPECT_FALSE(w.isClosed());
    w.closeAsync();
    EXPECT_EQ(w.life(), Life::Closing);
    for (int i = 0; i < kCap && !w.isClosed(); ++i) { g_now += 2 * kMs; r.round(w); }
    EXPECT_EQ(w.life(), Life::Closed);
    EXPECT_EQ(rec.failures, 1);
    EXPECT_EQ(rec.evs.size(), 1u) << "only the Failed event";
}

TEST(LeaseWatchLifeRegression, CloseAsyncTwiceRegression) {
    LifeRig f(6111);
    f.startReady();
    f.w.closeAsync();
    f.w.closeAsync();
    EXPECT_EQ(f.w.life(), Life::Closing);
    ASSERT_TRUE(f.driveClosed());
    f.w.closeAsync();
    EXPECT_EQ(f.w.life(), Life::Closed);
    f.r.rounds(f.w, 20);
    EXPECT_EQ(f.r.gw.unsubscribed.size(), f.r.parts);
}

TEST(LeaseWatchLifeRegression, LateRecordsAfterCloseDroppedRegression) {
    LifeRig f(6113);
    f.startReady();
    const std::size_t events = f.rec.evs.size();
    const uint32_t live = f.w.liveCount();
    f.w.closeAsync();
    // Records written to the log after closeAsync and before the unsubscribe is answered.
    for (uint32_t p = 0; p < f.r.parts; ++p) f.r.stub->parts[p].grant(kb(10 + static_cast<int>(p)));
    ASSERT_TRUE(f.driveClosed());
    f.r.rounds(f.w, 10);
    EXPECT_EQ(f.rec.evs.size(), events);
    EXPECT_EQ(f.w.liveCount(), live);
}

// The records are already in the watch's ring (the duty thread wrote them) when closeAsync() is called: doWork drops them, the ring ends empty.
TEST(LeaseWatchLifeRegression, RecordsInRingAtCloseDroppedRegression) {
    LifeRig f(6115);
    f.startReady();
    f.r.rounds(f.w, 10);
    const std::size_t events = f.rec.evs.size();
    const uint32_t live = f.w.liveCount();
    for (uint32_t p = 0; p < f.r.parts; ++p) f.r.stub->parts[p].grant(kb(10 + static_cast<int>(p)));
    for (uint32_t p = 0; p < f.r.parts; ++p) (void)f.r.stub->feed(static_cast<int32_t>(p));
    f.r.client->doWork();
    f.r.gw.pump();
    f.r.client->doWork();
    ASSERT_FALSE(f.w.recordRingEmptyForTest()) << "the late records reached the ring";
    f.w.closeAsync();
    ASSERT_TRUE(f.driveClosed());
    f.r.rounds(f.w, 10);
    EXPECT_TRUE(f.w.recordRingEmptyForTest()) << "settleClosing_ drained the ring";
    EXPECT_EQ(f.rec.evs.size(), events);
    EXPECT_EQ(f.w.liveCount(), live);
}

// A handler closes the watch in the middle of one ring drain: the records still queued behind the one being handled are dropped, not applied.
TEST(LeaseWatchLifeRegression, CloseInsideRingDrainDropsRestOfRingRegression) {
    WRig r(invokerCfg(6117, 6118, 1), 1);
    for (int i = 0; i < 2; ++i) r.stub->parts[0].grant(kb(i));
    rc::LeaseWatchBase w(*r.client, r.topic->core());
    Recorder rec;
    CloseOn h{&rec, &w, Kind::Granted, false};
    w.setHandler(&CloseOn::fn, &h);
    ASSERT_EQ(startWith(w, {Bytes{}}), rc::OpResult::Accepted);
    ASSERT_TRUE(r.drive(w, [&] { return w.isReady(); }));
    r.rounds(w, 10);
    ASSERT_EQ(w.liveCount(), 2u);
    h.armed = true;
    for (int i = 10; i < 14; ++i) r.stub->parts[0].grant(kb(i));
    (void)r.stub->feed(0);
    r.client->doWork();
    r.gw.pump();
    r.client->doWork();
    ASSERT_FALSE(w.recordRingEmptyForTest()) << "four records are queued in the ring";
    (void)w.doWork(g_now);   // the first record's Granted closes the watch; the other three are behind it in the same drain
    EXPECT_EQ(w.life(), Life::Closing);
    EXPECT_EQ(rec.count(Kind::Granted), 3) << "two scanned + the one that closed";
    EXPECT_EQ(w.liveCount(), 3u) << "the records behind it were dropped, not applied silently";
    for (int i = 0; i < kCap && !w.isClosed(); ++i) r.round(w);
    ASSERT_TRUE(w.isClosed());
    EXPECT_TRUE(w.recordRingEmptyForTest());
    EXPECT_EQ(rec.count(Kind::Granted), 3);
}

// A scan page that is still held by the broker when the watch is closed: its answer reaches the dropped queue, no row is applied, and after a
// restart the stale answer (an earlier scan token) is not mistaken for the new scan's.
TEST(LeaseWatchLifeRegression, ScanAnswerAfterCloseDroppedRegression) {
    WRig r(invokerCfg(6119, 6120, 1), 1);
    for (int i = 0; i < 3; ++i) r.stub->parts[0].grant(kb(i));
    rc::LeaseWatchBase w(*r.client, r.topic->core());
    Recorder rec;
    w.setHandler(&Recorder::fn, &rec);
    r.stub->hold = true;
    ASSERT_EQ(startWith(w, {Bytes{}}), rc::OpResult::Accepted);
    ASSERT_TRUE(r.drive(w, [&] { return r.stub->heldCount() == 1; }));
    w.closeAsync();
    r.stub->release();   // the answer arrives after the close
    for (int i = 0; i < kCap && !w.isClosed(); ++i) r.round(w);
    ASSERT_TRUE(w.isClosed());
    r.rounds(w, 10);
    EXPECT_TRUE(rec.evs.empty());
    EXPECT_EQ(w.liveCount(), 0u);
    const Bytes none;
    const std::span<const uint8_t> sp[] = {std::span<const uint8_t>(none)};
    ASSERT_EQ(w.restart(sp), rc::OpResult::Accepted);
    ASSERT_TRUE(r.drive(w, [&] { return rec.readies == 1; }));
    EXPECT_EQ(rec.count(Kind::Granted), 3) << "each lease exactly once: the stale page did not count";
    EXPECT_EQ(rec.count(Kind::Confirmed), 0);
    EXPECT_EQ(w.liveCount(), 3u);
    EXPECT_EQ(rec.bad, 0);
}

// The "LeaseWatch life" matrix: the state the watch is in x the operation applied to it -> the state it ends in.
TEST(LeaseWatchLifeRegression, LifeMatrixRegression) {
    enum class In { Fresh, Scanning, Ready, Failed, Closing, Closed };
    enum class Do { CloseAsync, CloseAsyncTwice, Start, DoWork, RegisterName, Restart, Record, Destroy };
    // Record: the broker grants a new lease and a round of everything runs (the record / the scan answer arrives); Destroy: the destructor.
    struct Row { In in; Do op; Life out; bool closed; };
    const Row table[] = {
        {In::Fresh, Do::CloseAsync, Life::Closed, true},        {In::Fresh, Do::CloseAsyncTwice, Life::Closed, true},
        {In::Fresh, Do::Start, Life::Running, false},           {In::Fresh, Do::DoWork, Life::Fresh, true},
        {In::Fresh, Do::RegisterName, Life::Fresh, true},
        {In::Fresh, Do::Restart, Life::Fresh, true},            {In::Fresh, Do::Record, Life::Fresh, true},
        {In::Fresh, Do::Destroy, Life::Fresh, true},
        {In::Scanning, Do::CloseAsync, Life::Closing, false},   {In::Scanning, Do::CloseAsyncTwice, Life::Closing, false},
        {In::Scanning, Do::Start, Life::Running, false},        {In::Scanning, Do::DoWork, Life::Running, false},
        {In::Scanning, Do::RegisterName, Life::Running, false},
        {In::Scanning, Do::Restart, Life::Running, false},      {In::Scanning, Do::Record, Life::Running, false},
        {In::Scanning, Do::Destroy, Life::Running, false},
        {In::Ready, Do::CloseAsync, Life::Closing, false},      {In::Ready, Do::CloseAsyncTwice, Life::Closing, false},
        {In::Ready, Do::Start, Life::Running, false},           {In::Ready, Do::DoWork, Life::Running, false},
        {In::Ready, Do::RegisterName, Life::Running, false},
        {In::Ready, Do::Restart, Life::Running, false},         {In::Ready, Do::Record, Life::Running, false},
        {In::Ready, Do::Destroy, Life::Running, false},
        {In::Failed, Do::CloseAsync, Life::Closing, false},     {In::Failed, Do::CloseAsyncTwice, Life::Closing, false},
        {In::Failed, Do::Start, Life::Failed, false},           {In::Failed, Do::DoWork, Life::Failed, false},
        {In::Failed, Do::RegisterName, Life::Failed, false},
        {In::Failed, Do::Restart, Life::Failed, false},        {In::Failed, Do::Record, Life::Failed, false},
        {In::Failed, Do::Destroy, Life::Failed, false},
        {In::Closing, Do::CloseAsync, Life::Closing, false},    {In::Closing, Do::CloseAsyncTwice, Life::Closing, false},
        {In::Closing, Do::Start, Life::Closing, false},         {In::Closing, Do::DoWork, Life::Closed, true},
        {In::Closing, Do::RegisterName, Life::Closing, false},
        {In::Closing, Do::Restart, Life::Closing, false},       {In::Closing, Do::Record, Life::Closed, true},
        {In::Closing, Do::Destroy, Life::Closing, false},
        {In::Closed, Do::CloseAsync, Life::Closed, true},       {In::Closed, Do::CloseAsyncTwice, Life::Closed, true},
        {In::Closed, Do::Start, Life::Closed, true},            {In::Closed, Do::DoWork, Life::Closed, true},
        {In::Closed, Do::RegisterName, Life::Closed, true},
        {In::Closed, Do::Restart, Life::Running, false},        {In::Closed, Do::Record, Life::Closed, true},
        {In::Closed, Do::Destroy, Life::Closed, true},
    };
    int n = 0;
    for (const Row& row : table) {
        SCOPED_TRACE("row " + std::to_string(n++));
        WRig r(invokerCfg(6200 + 2 * n, 6201 + 2 * n, 1), 1);
        r.stub->parts[0].grant(kb(1));
        if (row.in == In::Failed) {
            r.stub->errorCode = static_cast<int16_t>(rpc::ErrorCode::NotAuthorized);
            r.stub->errorNext = 1'000'000;
        }
        rc::LeaseWatchOptions o;
        o.names          = 1;
        o.waitersPerName = 1;
        auto wp = std::make_unique<rc::LeaseWatchBase>(*r.client, r.topic->core(), o);
        rc::LeaseWatchBase& w = *wp;
        Recorder rec;
        w.setHandler(&Recorder::fn, &rec);
        if (row.in != In::Fresh) ASSERT_EQ(startWith(w, {Bytes{}}), rc::OpResult::Accepted);
        if (row.in == In::Ready || row.in == In::Closing || row.in == In::Closed)
            ASSERT_TRUE(r.drive(w, [&] { return w.isReady(); }, kCap));
        if (row.in == In::Failed) {
            for (int i = 0; i < kCap && !w.failed(); ++i) { g_now += 2 * kMs; r.round(w); }
            ASSERT_EQ(w.life(), Life::Failed);
        }
        if (row.in == In::Closing || row.in == In::Closed) w.closeAsync();
        if (row.in == In::Closed) {
            for (int i = 0; i < kCap && !w.isClosed(); ++i) r.round(w);
            ASSERT_EQ(w.life(), Life::Closed);
        }
        const std::size_t events = rec.evs.size();
        const uint32_t live = w.liveCount();
        bool destroyed = false;
        switch (row.op) {
        case Do::CloseAsync: w.closeAsync(); break;
        case Do::CloseAsyncTwice: w.closeAsync(); w.closeAsync(); break;
        case Do::Start: {
            const rc::OpResult res = startWith(w, {Bytes{}});
            EXPECT_EQ(res, row.in == In::Fresh ? rc::OpResult::Accepted : rc::OpResult::InvalidArgument);
            break;
        }
        case Do::DoWork: (void)w.doWork(g_now); break;
        case Do::RegisterName: {
            const int32_t id = w.registerName(std::span<const uint8_t>(kb(1).data(), 1));
            if (row.in == In::Fresh) EXPECT_EQ(id, 0);
            else EXPECT_EQ(id, -1) << "names freeze at the first start";
            break;
        }
        case Do::Restart: {
            const Bytes none;
            const std::span<const uint8_t> sp[] = {std::span<const uint8_t>(none)};
            const rc::OpResult res = w.restart(sp);
            EXPECT_EQ(res, row.in == In::Closed ? rc::OpResult::Accepted : rc::OpResult::InvalidArgument);
            break;
        }
        case Do::Record:
            r.stub->parts[0].grant(kb(5));
            r.rounds(w, 4);
            break;
        case Do::Destroy: {
            const uint64_t cycles = r.client->dutyCyclesForTest();
            wp.reset();
            destroyed = true;
            if (row.in == In::Fresh || row.in == In::Closed) EXPECT_EQ(r.client->dutyCyclesForTest(), cycles) << "nothing to wait for";
            for (int i = 0; i < 20; ++i) { r.client->doWork(); r.gw.pump(); }   // the client carries on without the watch
            break;
        }
        }
        if (row.op == Do::Record && row.in == In::Ready) EXPECT_EQ(rec.evs.size(), events + 1) << "the record is delivered";
        else if (row.op == Do::Record && row.in == In::Scanning) EXPECT_GT(rec.evs.size(), events) << "the scan answer is delivered";
        else if (row.op != Do::Restart && row.op != Do::Start && row.op != Do::DoWork) EXPECT_EQ(rec.evs.size(), events) << "no event";
        if (destroyed) continue;
        EXPECT_EQ(w.life(), row.out);
        EXPECT_EQ(w.isClosed(), row.closed);
        if ((row.in == In::Closing || row.in == In::Closed) && row.op != Do::Restart) {
            EXPECT_EQ(rec.evs.size(), events) << "no event once closing";
            EXPECT_EQ(w.liveCount(), live) << "and no change to the table";
        }
        // Every row ends the same way: a closing watch reaches Closed within the cap.
        w.closeAsync();
        for (int i = 0; i < kCap && !w.isClosed(); ++i) { g_now += 2 * kMs; r.round(w); }
        EXPECT_TRUE(w.isClosed());
    }
}

// ---- restart ----

using Ids = std::set<Bytes>;
Ids heldKeys(const Recorder& rec) {
    Ids k;
    for (const auto& h : rec.held) k.insert(h.first);
    return k;
}
void closeToClosed(WRig& r, rc::LeaseWatchBase& w) {
    w.closeAsync();
    for (int i = 0; i < kCap && !w.isClosed(); ++i) r.round(w);
    ASSERT_TRUE(w.isClosed());
}

TEST(LeaseWatchRestartRegression, RestartAfterCloseFoldsToLiveSetRegression) {
    LifeRig f(6301);
    for (int i = 3; i < 5; ++i) f.r.stub->parts[static_cast<std::size_t>(i) % 2].grant(kb(i));   // 5 leases: 0..4
    ASSERT_EQ(startWith(f.w, {Bytes{}}), rc::OpResult::Accepted);
    ASSERT_TRUE(f.r.drive(f.w, [&] { return f.w.isReady(); }));
    ASSERT_EQ(f.rec.count(Kind::Granted), 5);
    closeToClosed(f.r, f.w);
    f.r.stub->parts[1].revoke(kb(1));
    f.r.stub->parts[0].update(kb(2));
    f.r.stub->parts[0].grant(kb(6));
    ASSERT_EQ(startWith(f.w, {Bytes{}}), rc::OpResult::InvalidArgument);
    const Bytes none;
    const std::span<const uint8_t> sp[] = {std::span<const uint8_t>(none)};
    ASSERT_EQ(f.w.restart(sp), rc::OpResult::Accepted);
    EXPECT_EQ(f.w.life(), Life::Running);
    EXPECT_FALSE(f.w.isReady());
    ASSERT_TRUE(f.r.drive(f.w, [&] { return f.rec.readies == 2; }));
    EXPECT_EQ(heldKeys(f.rec), (Ids{kb(0), kb(2), kb(3), kb(4), kb(6)}));
    EXPECT_EQ(f.rec.bad, 0);
    EXPECT_EQ(f.rec.readies, 2);
    EXPECT_EQ(f.rec.count(Kind::Vanished), 1);
    const Bytes k1 = kb(1), k6 = kb(6);
    EXPECT_EQ(f.rec.count(Kind::Vanished, &k1), 1);
    EXPECT_EQ(f.rec.count(Kind::Granted), 6);
    EXPECT_EQ(f.rec.count(Kind::Granted, &k6), 1);
    EXPECT_EQ(f.rec.count(Kind::Confirmed), 4) << "leases 0, 2, 3, 4 are still held";
    const Bytes k2 = kb(2);
    ASSERT_EQ(f.rec.count(Kind::Confirmed, &k2), 1);
    EXPECT_NE(f.r.stub->parts[0].table.at(k2).updated, f.r.stub->parts[0].table.at(k2).granted) << "lease 2 was updated while closed";
    for (const auto& e : f.rec.evs) {
        if (e.kind != Kind::Confirmed) continue;
        EXPECT_TRUE(e.fromScan);
        EXPECT_EQ(e.bytes, LeaseModel::row(e.key, f.r.stub->parts[static_cast<std::size_t>(e.key[1]) % 2].table.at(e.key)));
        if (e.key == k2) EXPECT_EQ(e.bytes, LeaseModel::row(k2, f.r.stub->parts[0].table.at(k2))) << "lease 2's Confirmed carries the updated row";
    }
    EXPECT_EQ(f.w.liveCount(), 5u);
    EXPECT_TRUE(f.w.isReady());
    // The restarted watch is live: a later grant is delivered.
    f.r.stub->parts[1].grant(kb(7));
    ASSERT_TRUE(f.r.drive(f.w, [&] { return f.rec.held.size() == 6; }));
    EXPECT_EQ(f.rec.bad, 0);
}

// F-11 (ii): restart across a leader change.  The first Subscribes of the restarted watch are answered NotLeader (the partitions' leaders
// moved while the watch was closed): the client retries them, and the re-run still brackets every partition once and folds to the live set.
TEST(LeaseWatchRestartRegression, RestartAcrossLeaderChangeRegression) {
    LifeRig f(6311);
    f.startReady();
    closeToClosed(f.r, f.w);
    f.r.stub->parts[1].revoke(kb(1));
    f.r.stub->parts[0].grant(kb(6));
    const std::size_t mark = f.rec.evs.size();
    f.r.gw.subscribeErrorFirstN    = 2;   // one per partition: the old leader no longer leads
    f.r.gw.subscribeErrorFirstCode = static_cast<int16_t>(rune::protocol::client::ErrorCode::NotLeader);
    const Bytes none;
    const std::span<const uint8_t> sp[] = {std::span<const uint8_t>(none)};
    ASSERT_EQ(f.w.restart(sp), rc::OpResult::Accepted);
    ASSERT_TRUE(f.r.drive(f.w, [&] { return f.rec.readies == 2; })) << "the restarted watch became ready again after the retries";
    f.r.rounds(f.w, 20);
    EXPECT_EQ(f.w.life(), Life::Running);
    int begin[2] = {0, 0}, end[2] = {0, 0};
    for (std::size_t i = mark; i < f.rec.evs.size(); ++i) {
        const Recorder::Ev& e = f.rec.evs[i];
        if (e.kind == Kind::RerunBegin) { ASSERT_LT(e.part, 2u); ++begin[e.part]; }
        if (e.kind == Kind::RerunEnd) { ASSERT_LT(e.part, 2u); ++end[e.part]; }
    }
    for (int p = 0; p < 2; ++p) {
        EXPECT_EQ(begin[p], 1) << "partition " << p;
        EXPECT_EQ(end[p], 1) << "partition " << p;
    }
    EXPECT_TRUE(f.rec.open.empty());
    EXPECT_EQ(f.rec.bad, 0);
    EXPECT_EQ(heldKeys(f.rec), (Ids{kb(0), kb(2), kb(6)}));
    const Bytes k1 = kb(1);
    EXPECT_EQ(f.rec.count(Kind::Vanished, &k1), 1);
    EXPECT_EQ(f.r.gw.subscribeErrorFirstN, 0) << "both NotLeader answers were consumed (the retries happened)";
}

TEST(LeaseWatchRestartRegression, RestartAfterFailedRegression) {
    WRig r(invokerCfg(6303, 6304, 1), 1);
    r.stub->parts[0].grant(kb(1));
    r.stub->errorCode = static_cast<int16_t>(rpc::ErrorCode::NotAuthorized);
    r.stub->errorNext = 1'000'000;
    rc::LeaseWatchBase w(*r.client, r.topic->core());
    Recorder rec;
    w.setHandler(&Recorder::fn, &rec);
    ASSERT_EQ(startWith(w, {Bytes{}}), rc::OpResult::Accepted);
    for (int i = 0; i < kCap && !w.failed(); ++i) { g_now += 2 * kMs; r.round(w); }
    ASSERT_EQ(w.life(), Life::Failed);
    w.closeAsync();
    for (int i = 0; i < kCap && !w.isClosed(); ++i) { g_now += 2 * kMs; r.round(w); }
    ASSERT_EQ(w.life(), Life::Closed);
    r.stub->errorNext = 0;
    r.stub->errorCode = 0;
    const Bytes none;
    const std::span<const uint8_t> sp[] = {std::span<const uint8_t>(none)};
    ASSERT_EQ(w.restart(sp), rc::OpResult::Accepted);
    EXPECT_FALSE(w.failed());
    ASSERT_TRUE(r.drive(w, [&] { return w.isReady(); }));
    EXPECT_FALSE(w.failed());
    EXPECT_EQ(rec.failures, 1);
    EXPECT_EQ(rec.readies, 1);
    EXPECT_EQ(heldKeys(rec), (Ids{kb(1)}));
    EXPECT_EQ(rec.bad, 0);
}

TEST(LeaseWatchRestartRegression, RestartWithNarrowerPrefixesVanishesRegression) {
    WRig r(invokerCfg(6305, 6306, 2), 2);
    // kb(i) has the group i % 4 in byte 0: groups 0 and 1, two leases each.
    for (int i : {0, 4, 1, 5}) r.stub->parts[static_cast<std::size_t>(i) % 2].grant(kb(i));
    rc::LeaseWatchBase w(*r.client, r.topic->core());
    Recorder rec;
    w.setHandler(&Recorder::fn, &rec);
    ASSERT_EQ(startWith(w, {Bytes{0x00}, Bytes{0x01}}), rc::OpResult::Accepted);
    ASSERT_TRUE(r.drive(w, [&] { return w.isReady(); }));
    ASSERT_EQ(w.liveCount(), 4u);
    closeToClosed(r, w);
    const Bytes g0{0x00};
    const std::span<const uint8_t> sp[] = {std::span<const uint8_t>(g0)};
    ASSERT_EQ(w.restart(sp), rc::OpResult::Accepted);
    ASSERT_TRUE(r.drive(w, [&] { return rec.readies == 2; }));
    const Bytes k1 = kb(1), k5 = kb(5);
    EXPECT_EQ(rec.count(Kind::Vanished), 2);
    EXPECT_EQ(rec.count(Kind::Vanished, &k1), 1);
    EXPECT_EQ(rec.count(Kind::Vanished, &k5), 1);
    EXPECT_EQ(w.liveCount(), 2u);
    EXPECT_EQ(heldKeys(rec), (Ids{kb(0), kb(4)}));
    EXPECT_EQ(rec.bad, 0);
    // A grant under the dropped prefix is not delivered any more.
    r.stub->parts[1].grant(kb(9));
    r.rounds(w, 30);
    EXPECT_EQ(w.liveCount(), 2u);
}

TEST(LeaseWatchRestartRegression, RestartWithWiderPrefixesGrantsRegression) {
    WRig r(invokerCfg(6325, 6326, 2), 2);
    for (int i : {0, 4, 1, 5}) r.stub->parts[static_cast<std::size_t>(i) % 2].grant(kb(i));   // groups 0 and 1, two leases each
    rc::LeaseWatchBase w(*r.client, r.topic->core());
    Recorder rec;
    w.setHandler(&Recorder::fn, &rec);
    ASSERT_EQ(startWith(w, {Bytes{0x00}}), rc::OpResult::Accepted);
    ASSERT_TRUE(r.drive(w, [&] { return w.isReady(); }));
    ASSERT_EQ(w.liveCount(), 2u);
    closeToClosed(r, w);
    const Bytes none;
    const std::span<const uint8_t> sp[] = {std::span<const uint8_t>(none)};
    ASSERT_EQ(w.restart(sp), rc::OpResult::Accepted);
    EXPECT_EQ(w.prefixCount(), 1u);
    ASSERT_TRUE(r.drive(w, [&] { return rec.readies == 2; }));
    const Bytes k0 = kb(0), k4 = kb(4), k1 = kb(1), k5 = kb(5);
    EXPECT_EQ(rec.count(Kind::Vanished), 0);
    EXPECT_EQ(rec.count(Kind::Confirmed), 2);
    EXPECT_EQ(rec.count(Kind::Confirmed, &k0), 1);
    EXPECT_EQ(rec.count(Kind::Confirmed, &k4), 1);
    EXPECT_EQ(rec.count(Kind::Granted, &k1), 1);
    EXPECT_EQ(rec.count(Kind::Granted, &k5), 1);
    EXPECT_EQ(w.liveCount(), 4u);
    EXPECT_EQ(heldKeys(rec), (Ids{kb(0), kb(4), kb(1), kb(5)}));
    // A grant under the new prefix is delivered.
    r.stub->parts[1].grant(kb(9));
    ASSERT_TRUE(r.drive(w, [&] { return rec.held.size() == 5; }));
    EXPECT_EQ(rec.bad, 0);
}

// The watch's thread allocates in exactly one place on a (re)start: Client::subscribe (control plane: one Subscription object per partition
// when the first page answers).  Everything else allocates nothing, exactly: closeAsync(), restart(), and every w.doWork() in which no
// subscription was opened (the scans, the fold, the settle to Closed).  A second close / restart cycle allocates exactly what the first did.
TEST(LeaseWatchRestartRegression, RestartNoAllocRegression) {
    WRig r(invokerCfg(6307, 6308, 2), 2);
    for (int i = 0; i < 8; ++i) r.stub->parts[static_cast<std::size_t>(i) % 2].grant(kb(i));
    rc::LeaseWatchBase w(*r.client, r.topic->core());
    long events = 0;
    w.setHandler([](void* c, const rc::LeaseEvent&) { ++*static_cast<long*>(c); }, &events);
    const Bytes none;
    const std::span<const uint8_t> sp[] = {std::span<const uint8_t>(none)};
    struct Tally { long subscribe = 0, stray = 0, close = 0, restart = 0; };
    // One window per watch-thread call; the stub and the client are outside it.
    const auto step = [&](Tally& t) {
        for (uint32_t p = 0; p < 2; ++p) (void)r.stub->feed(static_cast<int32_t>(p));
        r.client->doWork();
        r.gw.pump();
        const uint64_t opened = w.subscriptionsOpened();
        alloc_counter::startThisThread();
        (void)w.doWork(g_now);
        const long a = alloc_counter::stopThisThread();
        (w.subscriptionsOpened() == opened ? t.stray : t.subscribe) += a;
    };
    Tally first;
    ASSERT_EQ(w.startFromNow(sp), rc::OpResult::Accepted);
    for (int i = 0; i < kCap && !w.isReady(); ++i) step(first);
    ASSERT_TRUE(w.isReady());
    for (int i = 0; i < 20; ++i) step(first);
    EXPECT_GT(first.subscribe, 0) << "the baseline sees the subscribe allocations";
    EXPECT_EQ(first.stray, 0L) << "a scan, the handoff and the fold allocate nothing";
    const auto cycle = [&](Tally& t) {
        alloc_counter::startThisThread();
        w.closeAsync();
        t.close += alloc_counter::stopThisThread();
        bool restarted = false;
        for (int i = 0; i < kCap && !(restarted && w.isReady()); ++i) {
            step(t);
            if (!restarted && w.isClosed()) {
                alloc_counter::startThisThread();
                restarted = w.restart(sp) == rc::OpResult::Accepted;
                t.restart += alloc_counter::stopThisThread();
            }
        }
        ASSERT_TRUE(restarted);
        ASSERT_TRUE(w.isReady());
        for (int i = 0; i < 20; ++i) step(t);
    };
    Tally c1, c2;
    cycle(c1);
    cycle(c2);
    EXPECT_EQ(c1.close, 0L) << "closeAsync allocates nothing";
    EXPECT_EQ(c1.restart, 0L) << "restart allocates nothing";
    EXPECT_EQ(c1.stray, 0L) << "no doWork window without a subscribe allocates";
    EXPECT_EQ(c2.close, 0L);
    EXPECT_EQ(c2.restart, 0L);
    EXPECT_EQ(c2.stray, 0L);
    EXPECT_GT(c1.subscribe, 0L);
    EXPECT_LE(c1.subscribe, first.subscribe) << "the restart opens the same subscriptions as the first start";
    EXPECT_EQ(c2.subscribe, c1.subscribe) << "a second close / restart cycle allocates exactly what the first did";
}

TEST(LeaseWatchRestartRegression, RestartRefusedUnlessClosedRegression) {
    const Bytes none;
    const std::span<const uint8_t> sp[] = {std::span<const uint8_t>(none)};
    for (int state = 0; state < 4; ++state) {   // Fresh, Running, Failed, Closing
        SCOPED_TRACE("state " + std::to_string(state));
        WRig r(invokerCfg(6309 + 2 * state, 6310 + 2 * state, 1), 1);
        r.stub->parts[0].grant(kb(1));
        if (state == 2) { r.stub->errorCode = static_cast<int16_t>(rpc::ErrorCode::NotAuthorized); r.stub->errorNext = 1'000'000; }
        rc::LeaseWatchBase w(*r.client, r.topic->core());
        Recorder rec;
        w.setHandler(&Recorder::fn, &rec);
        if (state >= 1) ASSERT_EQ(startWith(w, {Bytes{}}), rc::OpResult::Accepted);
        if (state == 1) ASSERT_TRUE(r.drive(w, [&] { return w.isReady(); }));
        if (state == 2) for (int i = 0; i < kCap && !w.failed(); ++i) { g_now += 2 * kMs; r.round(w); }
        if (state == 3) { ASSERT_TRUE(r.drive(w, [&] { return w.isReady(); })); w.closeAsync(); }
        const Life before = w.life();
        const std::size_t events = rec.evs.size();
        const uint32_t live = w.liveCount();
        const bool ready = w.isReady();
        EXPECT_EQ(w.restart(sp), rc::OpResult::InvalidArgument);
        EXPECT_EQ(w.life(), before);
        EXPECT_EQ(rec.evs.size(), events);
        EXPECT_EQ(w.liveCount(), live);
        EXPECT_EQ(w.isReady(), ready);
    }
    // An invalid prefix set on a Closed watch is refused as startFromNow would, and the watch stays Closed.
    WRig r(invokerCfg(6320, 6321, 1), 1);
    rc::LeaseWatchBase w(*r.client, r.topic->core());
    w.closeAsync();
    ASSERT_EQ(w.life(), Life::Closed);
    EXPECT_EQ(w.restart({}), rc::OpResult::InvalidArgument);
    EXPECT_EQ(w.life(), Life::Closed);
    EXPECT_EQ(w.restart(sp), rc::OpResult::Accepted);
    EXPECT_EQ(w.life(), Life::Running);
}

TEST(LeaseWatchRestartRegression, RestartKeepsNamesRegression) {
    WRig r(invokerCfg(6323, 6324, 2), 2);
    const auto key = [](const std::string& s) { return test_support::bytesOf(s); };
    (void)r.stub->parts[0].grant(key("q/a"));
    const int64_t vb = r.stub->parts[1].grant(key("q/b"));
    rc::LeaseWatchOptions o;
    o.names          = 2;
    o.waitersPerName = 8;
    o.maxLive        = 32;
    rc::LeaseWatchBase w(*r.client, r.topic->core(), o);
    Recorder rec;
    w.setHandler(&Recorder::fn, &rec);
    const int32_t q = w.registerName(key("q"));
    ASSERT_GE(q, 0);
    struct Bound { int events = 0, readies = 0; } bound;
    ASSERT_TRUE(w.bindName(q, [](void* c, const rc::LeaseEvent& e) {
        auto* b = static_cast<Bound*>(c);
        ++b->events;
        if (e.kind == Kind::Ready) ++b->readies;
    }, &bound));
    ASSERT_EQ(startWith(w, {Bytes{}}), rc::OpResult::Accepted);
    ASSERT_TRUE(r.drive(w, [&] { return w.isReady(); }));
    ASSERT_TRUE(w.lowest(q).has_value());
    closeToClosed(r, w);
    const int before = bound.events;
    r.stub->parts[0].revoke(key("q/a"));
    const int64_t vc = r.stub->parts[0].grant(key("q/c"));
    const Bytes none;
    const std::span<const uint8_t> sp[] = {std::span<const uint8_t>(none)};
    ASSERT_EQ(w.restart(sp), rc::OpResult::Accepted);
    ASSERT_TRUE(r.drive(w, [&] { return rec.readies == 2; }));
    ASSERT_TRUE(w.lowest(q).has_value());
    EXPECT_EQ(w.lowest(q)->first, std::min(vb, vc));
    EXPECT_EQ(bound.readies, 2);
    EXPECT_EQ(bound.events - before, 4) << "Vanished q/a, Granted q/c, Confirmed q/b, Ready reach the bound recipe";
    EXPECT_EQ(w.registerName(key("q")), -1) << "names stay frozen after the first start";
    EXPECT_EQ(rec.bad, 0);
}


// ---- close inside a handler / a failed push, then restart (review batch A: C1, C2) ----

TEST(LeaseWatchRestartRegression, CloseAsyncInsideVanishedThenRestartFoldsRegression) {
    WRig r(invokerCfg(6401, 6402, 1), 1);
    LeaseModel& m = r.stub->parts[0];
    const Bytes A = kb(1), B = kb(5);
    m.grant(A);
    m.grant(B);
    rc::LeaseWatchBase w(*r.client, r.topic->core());
    Recorder rec;
    CloseOn h{&rec, &w, Kind::Vanished};
    w.setHandler(&CloseOn::fn, &h);
    ASSERT_EQ(startWith(w, {Bytes{}}), rc::OpResult::Accepted);
    ASSERT_TRUE(r.drive(w, [&] { return w.isReady(); }));
    r.rounds(w, 10);
    ASSERT_EQ(w.liveCount(), 2u);
    breakPartition(r, 0);
    m.revoke(A);   // both end while the partition is not watched
    m.revoke(B);
    r.rounds(w, 40);
    g_now += 2 * kMs;
    ASSERT_TRUE(r.drive(w, [&] { return rec.count(Kind::Vanished) == 1; }));
    EXPECT_EQ(w.life(), Life::Closing);
    EXPECT_EQ(w.liveCount(), 1u) << "the second lease is NOT erased silently";
    for (int i = 0; i < kCap && !w.isClosed(); ++i) r.round(w);
    ASSERT_TRUE(w.isClosed());
    rec.endBrackets();   // closeAsync ends the re-run bracket
    EXPECT_EQ(rec.count(Kind::Vanished), 1);
    const Bytes none;
    const std::span<const uint8_t> sp[] = {std::span<const uint8_t>(none)};
    ASSERT_EQ(w.restart(sp), rc::OpResult::Accepted);
    ASSERT_TRUE(r.drive(w, [&] { return rec.readies == 2; }));
    EXPECT_EQ(rec.count(Kind::Vanished), 2) << "the lease the closed handler never saw end vanishes after the restart";
    EXPECT_TRUE(rec.held.empty());
    EXPECT_EQ(w.liveCount(), 0u);
    EXPECT_EQ(rec.bad, 0);
}

TEST(LeaseWatchRestartRegression, CloseAsyncInsideReplaceVanishedThenRestartFoldsRegression) {
    WRig r(invokerCfg(6403, 6404, 1), 1);
    LeaseModel& m = r.stub->parts[0];
    const Bytes A = kb(1);
    m.grant(A);
    rc::LeaseWatchBase w(*r.client, r.topic->core());
    Recorder rec;
    CloseOn h{&rec, &w, Kind::Vanished};
    w.setHandler(&CloseOn::fn, &h);
    ASSERT_EQ(startWith(w, {Bytes{}}), rc::OpResult::Accepted);
    ASSERT_TRUE(r.drive(w, [&] { return w.isReady(); }));
    r.rounds(w, 10);
    breakPartition(r, 0);
    m.revoke(A);
    const int64_t v2 = m.grant(A);   // another incarnation while the partition is not watched
    r.rounds(w, 40);
    g_now += 2 * kMs;
    ASSERT_TRUE(r.drive(w, [&] { return rec.count(Kind::Vanished) == 1; }));
    EXPECT_EQ(w.liveCount(), 0u) << "the replacing incarnation is not inserted silently";
    for (int i = 0; i < kCap && !w.isClosed(); ++i) r.round(w);
    ASSERT_TRUE(w.isClosed());
    rec.endBrackets();
    const Bytes none;
    const std::span<const uint8_t> sp[] = {std::span<const uint8_t>(none)};
    ASSERT_EQ(w.restart(sp), rc::OpResult::Accepted);
    ASSERT_TRUE(r.drive(w, [&] { return rec.readies == 2; }));
    EXPECT_EQ(rec.count(Kind::Granted, &A), 2) << "the new incarnation is announced as Granted";
    EXPECT_EQ(rec.count(Kind::Confirmed), 0);
    EXPECT_EQ(rec.held, (Held{{A, v2}}));
    EXPECT_EQ(rec.bad, 0);
}

// The name's heap holds one waiter: the second lease of the name fails the first run.  The failed push must leave nothing behind in the live
// set (it was never announced and is in no heap): variant 0 revokes the lease the failed run DID announce while closed (the restart fails the
// same way again, cleanly), variant 1 revokes the one it did not (the restart folds to the announced one).
TEST(LeaseWatchRestartRegression, RestartAfterNameHeapFullRegression) {
    for (int variant = 0; variant < 2; ++variant) {
        SCOPED_TRACE("variant " + std::to_string(variant));
        WRig r(invokerCfg(6405 + 2 * variant, 6406 + 2 * variant, 1), 1);
        const auto key = [](const std::string& s) { return test_support::bytesOf(s); };
        LeaseModel& m = r.stub->parts[0];
        const Bytes qa = key("q/a"), qb = key("q/b");
        m.grant(qa);
        m.grant(qb);
        rc::LeaseWatchOptions o;
        o.names          = 1;
        o.waitersPerName = 1;
        o.maxLive        = 32;
        rc::LeaseWatchBase w(*r.client, r.topic->core(), o);
        Recorder rec;
        w.setHandler(&Recorder::fn, &rec);
        const int32_t q = w.registerName(key("q"));
        ASSERT_GE(q, 0);
        ASSERT_EQ(startWith(w, {Bytes{}}), rc::OpResult::Accepted);
        for (int i = 0; i < kCap && !w.failed(); ++i) r.round(w);
        ASSERT_TRUE(w.failed()) << "the second waiter does not fit the name's heap";
        ASSERT_EQ(rec.count(Kind::Granted), 1);
        EXPECT_EQ(w.liveCount(), 1u) << "the entry whose push failed never joined the live set";
        const Bytes first  = rec.evs[0].key;   // the lease the scan order announced
        const Bytes second = first == qa ? qb : qa;
        closeToClosed(r, w);
        rec.endBrackets();
        m.revoke(variant == 0 ? first : second);   // ends while closed
        const Bytes none;
        const std::span<const uint8_t> sp[] = {std::span<const uint8_t>(none)};
        ASSERT_EQ(w.restart(sp), rc::OpResult::Accepted);
        if (variant == 0) {
            for (int i = 0; i < kCap && !w.failed(); ++i) r.round(w);
            ASSERT_TRUE(w.failed()) << "the same scan fails the same way";
            EXPECT_EQ(rec.failures, 2);
            EXPECT_EQ(rec.count(Kind::Confirmed), 0);
            EXPECT_EQ(rec.count(Kind::Granted, &second), 0) << "never announced";
            EXPECT_EQ(w.liveCount(), 1u) << "still only the announced lease";
            EXPECT_EQ(rec.bad, 0);
            continue;
        }
        ASSERT_TRUE(r.drive(w, [&] { return rec.readies == 1; }));
        EXPECT_FALSE(w.failed());
        EXPECT_EQ(rec.count(Kind::Confirmed, &first), 1);
        EXPECT_EQ(rec.count(Kind::Confirmed), 1) << "a lease that was never announced is not confirmed";
        EXPECT_EQ(rec.count(Kind::Vanished), 0) << "nor does it vanish";
        ASSERT_TRUE(w.lowest(q).has_value()) << "the name's heap still holds the announced lease";
        EXPECT_EQ(Bytes(w.lowest(q)->second.begin(), w.lowest(q)->second.end()), first);
        EXPECT_EQ(w.liveCount(), 1u);
        // The announced lease ends: the name's heap must not underflow.
        m.revoke(first);
        ASSERT_TRUE(r.drive(w, [&] { return rec.count(Kind::Revoked) == 1; }));
        EXPECT_FALSE(w.lowest(q).has_value());
        EXPECT_EQ(w.liveCount(), 0u);
        EXPECT_EQ(rec.bad, 0);
    }
}

// ---- re-run events: Confirmed / RerunBegin / RerunEnd ----

/// One event of the sequence a case expects: (kind, partition, key, version); Begin / End carry no key, Ready / Failed partition -1.
struct Seq {
    Kind    kind;
    int32_t part;
    Bytes   key;
    int64_t ver;
    bool operator==(const Seq&) const = default;
};
std::string describeSeq(const std::vector<Seq>& v) {
    static const char* names[] = {"Granted", "Updated", "Revoked", "Expired", "Vanished", "Ready", "Failed", "Confirmed", "RerunBegin", "RerunEnd"};
    std::string s;
    for (const Seq& e : v) {
        s += names[static_cast<int>(e.kind)];
        if (!e.key.empty()) s += "(" + std::to_string(e.key[1]) + "@" + std::to_string(e.ver) + ")";
        else if (e.part >= 0 && (e.kind == Kind::RerunBegin || e.kind == Kind::RerunEnd)) s += "(p" + std::to_string(e.part) + ")";
        s += " ";
    }
    return s;
}
std::vector<Seq> seqFrom(const Recorder& rec, std::size_t mark) {
    std::vector<Seq> v;
    for (std::size_t i = mark; i < rec.evs.size(); ++i) {
        const Recorder::Ev& e = rec.evs[i];
        v.push_back({e.kind, e.part, e.key, e.ver});
    }
    return v;
}
const Bytes kNoKey;

/// The fold of one partition's re-run, written down independently of the watch: what the scan rows (in scan-hash order) and the unconfirmed
/// leftovers do to the live set.
struct Sim {
    std::map<Bytes, int64_t> live;
    std::set<Bytes>          conf;
    std::vector<Seq>         out;
    void begin(bool emit) {
        conf.clear();
        if (emit) out.push_back({Kind::RerunBegin, 0, kNoKey, 0});
    }
    void row(const Bytes& k, int64_t v) {
        const auto it = live.find(k);
        if (it != live.end() && it->second == v) {
            if (conf.insert(k).second) out.push_back({Kind::Confirmed, 0, k, v});
            return;
        }
        if (it != live.end()) out.push_back({Kind::Vanished, 0, k, it->second});
        live[k] = v;
        conf.insert(k);
        out.push_back({Kind::Granted, 0, k, v});
    }
    void done() {
        for (auto it = live.begin(); it != live.end();) {
            if (conf.count(it->first) != 0) { ++it; continue; }
            out.push_back({Kind::Vanished, 0, it->first, it->second});
            it = live.erase(it);
        }
        out.push_back({Kind::RerunEnd, 0, kNoKey, 0});
    }
};
std::vector<std::pair<Bytes, int64_t>> rowsByHash(const LeaseModel& m) {
    std::vector<std::pair<Bytes, int64_t>> rows;
    for (const auto& [k, h] : m.table) rows.emplace_back(k, h.version);
    std::sort(rows.begin(), rows.end(), [](const auto& a, const auto& b) {
        return rune::protocol::scanHash(a.first, 1) < rune::protocol::scanHash(b.first, 1);
    });
    return rows;
}

void breakPartition(WRig& r, int32_t p) {
    const auto sub = *r.gw.latestSubscription(1, p);
    r.gw.sendError(p, static_cast<int16_t>(rpc::ErrorCode::NotAuthorized), "denied", sub.sessionId, sub.corrId);
}

TEST(LeaseWatchRerunRegression, UpdateWhileBrokenIsConfirmedRegression) {
    WRig r(invokerCfg(7001, 7002, 1), 1);
    const Bytes A = kb(1);
    const int64_t va = r.stub->parts[0].grant(A);
    rc::LeaseWatchBase w(*r.client, r.topic->core());
    Recorder rec;
    w.setHandler(&Recorder::fn, &rec);
    ASSERT_EQ(startWith(w, {Bytes{}}), rc::OpResult::Accepted);
    ASSERT_TRUE(r.drive(w, [&] { return w.isReady(); }));
    r.rounds(w, 10);
    const std::size_t mark = rec.evs.size();
    breakPartition(r, 0);
    r.stub->parts[0].update(A);   // while broken: new row bytes, same version, no record the resubscribed stream replays
    r.rounds(w, 40);
    g_now += 2 * kMs;
    ASSERT_TRUE(r.drive(w, [&] { return rec.count(Kind::RerunEnd) == 1; }));
    r.rounds(w, 30);
    EXPECT_EQ(describeSeq(seqFrom(rec, mark)), describeSeq({{Kind::RerunBegin, 0, kNoKey, 0}, {Kind::Confirmed, 0, A, va}, {Kind::RerunEnd, 0, kNoKey, 0}}));
    ASSERT_EQ(rec.evs.size() - mark, 3u);
    const Recorder::Ev& c = rec.evs[mark + 1];
    EXPECT_EQ(c.kind, Kind::Confirmed);
    EXPECT_TRUE(c.fromScan);
    EXPECT_EQ(c.bytes, LeaseModel::row(A, r.stub->parts[0].table.at(A))) << "the scanned view row, with the update";
    EXPECT_EQ(c.off, va) << "the row's lease_granted_offset";
    EXPECT_EQ(rec.count(Kind::Granted), 1);
    EXPECT_EQ(rec.count(Kind::Updated), 0);
    EXPECT_EQ(rec.bad, 0);
}

TEST(LeaseWatchRerunRegression, FirstScanHasNoRerunEventsRegression) {
    LifeRig f(7003);
    f.startReady();
    f.r.rounds(f.w, 20);
    EXPECT_EQ(f.rec.count(Kind::Confirmed), 0);
    EXPECT_EQ(f.rec.count(Kind::RerunBegin), 0);
    EXPECT_EQ(f.rec.count(Kind::RerunEnd), 0);
    EXPECT_EQ(f.rec.bad, 0);
}

// Each timing x each change made while the partition was not watched: the exact event sequence of the partition's re-run.
TEST(LeaseWatchRerunRegression, RerunFoldMatrixRegression) {
    enum class Cause { Break, BreakAgain, Restart, Fail, Close };
    enum class Chg { None, Update, End, Regrant, Grant };
    const Cause causes[] = {Cause::Break, Cause::BreakAgain, Cause::Restart, Cause::Fail, Cause::Close};
    const Chg   chgs[]   = {Chg::None, Chg::Update, Chg::End, Chg::Regrant, Chg::Grant};
    const Bytes A = kb(1), B = kb(5), C = kb(9), G = kb(13);
    int n = 0;
    for (const Cause cause : causes)
        for (const Chg chg : chgs) {
            SCOPED_TRACE("cause " + std::to_string(static_cast<int>(cause)) + " change " + std::to_string(static_cast<int>(chg)));
            ++n;
            WRig r(invokerCfg(7100 + 2 * n, 7101 + 2 * n, 1), 1);
            LeaseModel& m = r.stub->parts[0];
            m.grant(A);
            m.grant(B);
            m.grant(C);
            rc::LeaseWatchOptions o;
            o.maxRows = 1;   // a page a lease
            rc::LeaseWatchBase w(*r.client, r.topic->core(), o);
            Recorder rec;
            w.setHandler(&Recorder::fn, &rec);
            ASSERT_EQ(startWith(w, {Bytes{}}), rc::OpResult::Accepted);
            ASSERT_TRUE(r.drive(w, [&] { return w.isReady(); }));
            r.rounds(w, 10);
            Sim sim;
            for (const auto& [k, h] : m.table) sim.live[k] = h.version;
            const std::size_t baseline = r.stub->seen.size();
            const std::size_t mark     = rec.evs.size();
            const auto change = [&] {
                switch (chg) {
                case Chg::None: break;
                case Chg::Update: m.update(A); break;
                case Chg::End: m.revoke(A); break;
                case Chg::Regrant: m.revoke(A); m.grant(A); break;
                case Chg::Grant: m.grant(G); break;
                }
            };
            const auto fold = [&](bool again) {
                sim.begin(true);
                const auto rows = rowsByHash(m);
                if (again) { sim.row(rows[0].first, rows[0].second); sim.begin(false); }
                for (const auto& [k, v] : rows) sim.row(k, v);
                sim.done();
            };
            const Bytes none;
            const std::span<const uint8_t> sp[] = {std::span<const uint8_t>(none)};
            switch (cause) {
            case Cause::Restart:
                closeToClosed(r, w);
                change();
                ASSERT_EQ(w.restart(sp), rc::OpResult::Accepted);
                ASSERT_TRUE(r.drive(w, [&] { return rec.readies == 2; }));
                fold(false);
                sim.out.push_back({Kind::Ready, -1, kNoKey, 0});
                break;
            case Cause::Break:
                breakPartition(r, 0);
                change();
                r.rounds(w, 40);
                g_now += 2 * kMs;
                ASSERT_TRUE(r.drive(w, [&] { return rec.count(Kind::RerunEnd) == 1; }));
                fold(false);
                break;
            case Cause::BreakAgain: {
                breakPartition(r, 0);
                change();
                r.rounds(w, 40);
                const int64_t firstSub = r.gw.latestSubscription(1, 0)->subscriptionId;
                r.stub->onRequest = [&](const LeaseStub::ScanSeen&) {
                    if (r.stub->seen.size() == baseline + 2) r.stub->hold = true;   // the re-run's second page is computed, delivered late
                };
                g_now += 2 * kMs;
                ASSERT_TRUE(r.drive(w, [&] { return r.stub->heldCount() == 1; }));
                r.rounds(w, 5);
                ASSERT_NE(r.gw.latestSubscription(1, 0)->subscriptionId, firstSub) << "the re-run's own subscription";
                breakPartition(r, 0);   // the re-run is broken again, one page in
                r.rounds(w, 10);
                r.stub->release();   // the held page is stale now
                g_now += 5 * kMs;
                ASSERT_TRUE(r.drive(w, [&] { return rec.count(Kind::RerunEnd) == 1; }));
                fold(true);
                break;
            }
            case Cause::Fail:
                breakPartition(r, 0);
                change();
                r.rounds(w, 40);
                r.stub->errorCode = static_cast<int16_t>(rpc::ErrorCode::NotAuthorized);
                r.stub->errorNext = 1'000'000;
                g_now += 2 * kMs;
                ASSERT_TRUE(r.drive(w, [&] { return w.failed(); }));
                sim.begin(true);
                sim.out.push_back({Kind::Failed, -1, kNoKey, 0});
                break;
            case Cause::Close:
                breakPartition(r, 0);
                change();
                r.rounds(w, 40);
                r.stub->hold = true;
                g_now += 2 * kMs;
                ASSERT_TRUE(r.drive(w, [&] { return !rec.open.empty(); }));
                w.closeAsync();
                for (int i = 0; i < kCap && !w.isClosed(); ++i) r.round(w);
                ASSERT_TRUE(w.isClosed());
                sim.begin(true);
                break;
            }
            r.rounds(w, 30);
            EXPECT_EQ(describeSeq(seqFrom(rec, mark)), describeSeq(sim.out));
            EXPECT_EQ(rec.bad, 0);
            for (std::size_t i = mark; i < rec.evs.size(); ++i) {
                const Recorder::Ev& e = rec.evs[i];
                if (e.kind != Kind::Confirmed) continue;
                EXPECT_TRUE(e.fromScan);
                EXPECT_EQ(e.bytes, LeaseModel::row(e.key, m.table.at(e.key)));
                EXPECT_EQ(e.off, m.table.at(e.key).granted);
            }
            if (cause == Cause::Fail || cause == Cause::Close) {
                EXPECT_FALSE(rec.open.empty()) << "no RerunEnd";
                EXPECT_EQ(rec.count(Kind::RerunEnd), 0);
            } else {
                EXPECT_TRUE(rec.open.empty());
                Held want;
                for (const auto& [k, h] : m.table) want.insert({k, h.version});
                EXPECT_EQ(rec.held, want);
            }
        }
}

TEST(LeaseWatchRerunRegression, RerunBreaksAgainOnePairRegression) {
    WRig r(invokerCfg(7005, 7006, 1), 1);
    for (int i : {1, 5, 9}) r.stub->parts[0].grant(kb(i));
    rc::LeaseWatchOptions o;
    o.maxRows = 1;
    rc::LeaseWatchBase w(*r.client, r.topic->core(), o);
    Recorder rec;
    w.setHandler(&Recorder::fn, &rec);
    ASSERT_EQ(startWith(w, {Bytes{}}), rc::OpResult::Accepted);
    ASSERT_TRUE(r.drive(w, [&] { return w.isReady(); }));
    r.rounds(w, 10);
    const std::size_t baseline = r.stub->seen.size();
    breakPartition(r, 0);
    r.rounds(w, 40);
    r.stub->onRequest = [&](const LeaseStub::ScanSeen&) {
        if (r.stub->seen.size() == baseline + 2) r.stub->hold = true;
    };
    g_now += 2 * kMs;
    ASSERT_TRUE(r.drive(w, [&] { return r.stub->heldCount() == 1; }));
    r.rounds(w, 5);
    EXPECT_EQ(r.stub->seen.size(), baseline + 2) << "the re-run issued exactly one page and asked for the next";
    breakPartition(r, 0);
    r.rounds(w, 10);
    r.stub->release();
    g_now += 5 * kMs;
    ASSERT_TRUE(r.drive(w, [&] { return rec.count(Kind::RerunEnd) == 1; }));
    r.rounds(w, 30);
    EXPECT_EQ(w.reruns(), 2u);
    EXPECT_EQ(rec.count(Kind::RerunBegin), 1);
    EXPECT_EQ(rec.count(Kind::RerunEnd), 1);
    EXPECT_TRUE(rec.open.empty());
    EXPECT_EQ(rec.bad, 0);
}

TEST(LeaseWatchRerunRegression, RerunEndAfterOverflowRegression) {
    WRig r(invokerCfg(7007, 7008, 1), 1);
    const Bytes B = kb(5);
    for (int i : {1, 5, 9}) r.stub->parts[0].grant(kb(i));
    rc::LeaseWatchOptions o;
    o.handoffBuffer = 4;
    o.maxRows       = 1;
    rc::LeaseWatchBase w(*r.client, r.topic->core(), o);
    struct Probe {
        Recorder rec;
        rc::LeaseWatchBase* w = nullptr;
        uint64_t resubsAtEnd = 99;
        static void fn(void* c, const rc::LeaseEvent& e) {
            auto* p = static_cast<Probe*>(c);
            if (e.kind == Kind::RerunEnd) p->resubsAtEnd = p->w->resubscribes();
            p->rec.on(e);
        }
    } probe;
    probe.w = &w;
    Recorder& rec = probe.rec;
    w.setHandler(&Probe::fn, &probe);
    ASSERT_EQ(startWith(w, {Bytes{}}), rc::OpResult::Accepted);
    ASSERT_TRUE(r.drive(w, [&] { return w.isReady(); }));
    r.rounds(w, 10);
    const std::size_t baseline = r.stub->seen.size();
    breakPartition(r, 0);
    r.rounds(w, 40);
    r.stub->onRequest = [&](const LeaseStub::ScanSeen&) {
        if (r.stub->seen.size() == baseline + 2) r.stub->hold = true;
    };
    g_now += 2 * kMs;
    ASSERT_TRUE(r.drive(w, [&] { return r.stub->heldCount() == 1; }));
    r.rounds(w, 5);
    for (int i = 0; i < 10; ++i) r.stub->parts[0].update(B);   // more than the handoff list holds, while the re-run's page 2 is outstanding
    r.rounds(w, 30);
    EXPECT_EQ(rec.count(Kind::RerunBegin), 1);
    EXPECT_EQ(rec.count(Kind::RerunEnd), 0) << "the scan is not done";
    r.stub->release();
    ASSERT_TRUE(r.drive(w, [&] { return rec.count(Kind::RerunEnd) == 1; }));
    r.rounds(w, 30);
    EXPECT_EQ(probe.resubsAtEnd, 1u) << "RerunEnd came when the resubscribed partition was Live, not when the scan finished";
    EXPECT_EQ(w.resubscribes(), 1u);
    EXPECT_EQ(rec.count(Kind::RerunBegin), 1);
    EXPECT_EQ(rec.count(Kind::RerunEnd), 1);
    std::size_t firstUpdated = rec.evs.size(), end = rec.evs.size();
    for (std::size_t i = 0; i < rec.evs.size(); ++i) {
        if (rec.evs[i].kind == Kind::Updated && firstUpdated == rec.evs.size()) firstUpdated = i;
        if (rec.evs[i].kind == Kind::RerunEnd) end = i;
    }
    EXPECT_LT(firstUpdated, end) << "the buffered records are applied before RerunEnd";
    EXPECT_TRUE(rec.open.empty());
    EXPECT_EQ(rec.bad, 0);
}

TEST(LeaseWatchRerunRegression, FailDuringRerunNoEndRegression) {
    WRig r(invokerCfg(7009, 7010, 1), 1);
    r.stub->parts[0].grant(kb(1));
    rc::LeaseWatchBase w(*r.client, r.topic->core());
    Recorder rec;
    w.setHandler(&Recorder::fn, &rec);
    ASSERT_EQ(startWith(w, {Bytes{}}), rc::OpResult::Accepted);
    ASSERT_TRUE(r.drive(w, [&] { return w.isReady(); }));
    r.rounds(w, 10);
    breakPartition(r, 0);
    r.rounds(w, 40);
    r.stub->errorCode = static_cast<int16_t>(rpc::ErrorCode::NotAuthorized);
    r.stub->errorNext = 1'000'000;
    g_now += 2 * kMs;
    ASSERT_TRUE(r.drive(w, [&] { return w.failed(); }));
    for (int i = 0; i < 20; ++i) { g_now += 2 * kMs; r.round(w); }
    EXPECT_EQ(rec.count(Kind::RerunBegin), 1);
    EXPECT_EQ(rec.count(Kind::RerunEnd), 0);
    EXPECT_EQ(rec.failures, 1);
    EXPECT_EQ(rec.evs.back().kind, Kind::Failed);
}

TEST(LeaseWatchRerunRegression, RestartBracketsEveryPartitionRegression) {
    LifeRig f(7011);
    f.startReady();
    closeToClosed(f.r, f.w);
    const std::size_t mark = f.rec.evs.size();
    const Bytes none;
    const std::span<const uint8_t> sp[] = {std::span<const uint8_t>(none)};
    ASSERT_EQ(f.w.restart(sp), rc::OpResult::Accepted);
    ASSERT_TRUE(f.r.drive(f.w, [&] { return f.rec.readies == 2; }));
    f.r.rounds(f.w, 20);
    std::size_t ready = 0;
    int begin[2] = {0, 0}, end[2] = {0, 0};
    for (std::size_t i = mark; i < f.rec.evs.size(); ++i) {
        const Recorder::Ev& e = f.rec.evs[i];
        if (e.kind == Kind::Ready) ready = i;
        if (e.kind == Kind::RerunBegin) { ASSERT_TRUE(e.part == 0 || e.part == 1); ++begin[e.part]; EXPECT_EQ(ready, 0u) << "before the second Ready"; }
        if (e.kind == Kind::RerunEnd) { ASSERT_TRUE(e.part == 0 || e.part == 1); ++end[e.part]; EXPECT_EQ(ready, 0u) << "before the second Ready"; }
    }
    EXPECT_NE(ready, 0u);
    for (int p = 0; p < 2; ++p) {
        EXPECT_EQ(begin[p], 1) << "partition " << p;
        EXPECT_EQ(end[p], 1) << "partition " << p;
    }
    EXPECT_TRUE(f.rec.open.empty());
    EXPECT_EQ(f.rec.bad, 0);
}

// A page answered with a transient error is asked again; the re-run confirms each lease once.
TEST(LeaseWatchRerunRegression, RetriedPageConfirmsOnceRegression) {
    WRig r(invokerCfg(7013, 7014, 1), 1);
    for (int i : {1, 5, 9}) r.stub->parts[0].grant(kb(i));
    rc::LeaseWatchOptions o;
    o.maxRows = 1;
    rc::LeaseWatchBase w(*r.client, r.topic->core(), o);
    Recorder rec;
    w.setHandler(&Recorder::fn, &rec);
    ASSERT_EQ(startWith(w, {Bytes{}}), rc::OpResult::Accepted);
    ASSERT_TRUE(r.drive(w, [&] { return w.isReady(); }));
    r.rounds(w, 10);
    const std::size_t baseline = r.stub->seen.size();
    breakPartition(r, 0);
    r.rounds(w, 40);
    r.stub->onRequest = [&](const LeaseStub::ScanSeen&) {
        if (r.stub->seen.size() == baseline + 2) {   // the re-run's second page: answered Throttled once, then asked again
            r.stub->errorCode = static_cast<int16_t>(rpc::ErrorCode::Throttled);
            r.stub->errorNext = 1;
        }
    };
    g_now += 2 * kMs;
    for (int i = 0; i < 300 && rec.count(Kind::RerunEnd) == 0; ++i) { g_now += 2 * kMs; r.round(w); }
    ASSERT_EQ(rec.count(Kind::RerunEnd), 1);
    r.rounds(w, 20);
    EXPECT_EQ(r.stub->seen.size(), baseline + 4) << "page 2 was asked twice";
    for (int i : {1, 5, 9}) {
        const Bytes k = kb(i);
        EXPECT_EQ(rec.count(Kind::Confirmed, &k), 1) << "lease " << i;
    }
    EXPECT_EQ(rec.count(Kind::Confirmed), 3);
    EXPECT_EQ(rec.count(Kind::RerunBegin), 1);
    EXPECT_EQ(w.reruns(), 1u);
    EXPECT_EQ(rec.bad, 0);
}

// One re-run's scan delivers a row twice (the broker answers the second page as the first again): the lease is confirmed once.
TEST(LeaseWatchRerunRegression, DuplicateRowConfirmsOnceRegression) {
    WRig r(invokerCfg(7019, 7020, 1), 1);
    for (int i : {1, 5, 9}) r.stub->parts[0].grant(kb(i));
    rc::LeaseWatchOptions o;
    o.maxRows = 1;
    rc::LeaseWatchBase w(*r.client, r.topic->core(), o);
    Recorder rec;
    w.setHandler(&Recorder::fn, &rec);
    ASSERT_EQ(startWith(w, {Bytes{}}), rc::OpResult::Accepted);
    ASSERT_TRUE(r.drive(w, [&] { return w.isReady(); }));
    r.rounds(w, 10);
    const std::size_t baseline = r.stub->seen.size();
    breakPartition(r, 0);
    r.rounds(w, 40);
    r.stub->onRequest = [&](const LeaseStub::ScanSeen&) {
        if (r.stub->seen.size() == baseline + 2) r.stub->rewindNext = 1;   // the re-run's second page is answered as the first page again
    };
    g_now += 2 * kMs;
    ASSERT_TRUE(r.drive(w, [&] { return rec.count(Kind::RerunEnd) == 1; }));
    r.rounds(w, 20);
    EXPECT_EQ(r.stub->seen.size(), baseline + 4) << "page 1, page 2 (rewound), page 2 again, page 3";
    for (int i : {1, 5, 9}) {
        const Bytes k = kb(i);
        EXPECT_EQ(rec.count(Kind::Confirmed, &k), 1) << "lease " << i;
    }
    EXPECT_EQ(rec.count(Kind::Confirmed), 3);
    EXPECT_EQ(rec.count(Kind::Granted), 3);
    EXPECT_EQ(rec.count(Kind::Vanished), 0);
    EXPECT_EQ(w.reruns(), 1u);
    EXPECT_EQ(rec.bad, 0);
}

// A first scan that breaks IS bracketed: the Granted events of the first attempt were delivered, so the retry reconciles them (Confirmed for
// the lease it already announced, Granted for the rest), between RerunBegin and RerunEnd, before the first Ready.
TEST(LeaseWatchRerunRegression, FirstScanBrokenIsBracketedRegression) {
    WRig r(invokerCfg(7021, 7022, 1), 1);
    for (int i : {1, 5, 9}) r.stub->parts[0].grant(kb(i));
    rc::LeaseWatchOptions o;
    o.maxRows = 1;
    rc::LeaseWatchBase w(*r.client, r.topic->core(), o);
    Recorder rec;
    w.setHandler(&Recorder::fn, &rec);
    r.stub->onRequest = [&](const LeaseStub::ScanSeen&) {
        if (r.stub->seen.size() == 2) r.stub->hold = true;   // the first scan's second page stays at the broker
    };
    ASSERT_EQ(startWith(w, {Bytes{}}), rc::OpResult::Accepted);
    ASSERT_TRUE(r.drive(w, [&] { return r.stub->heldCount() == 1; }));
    r.rounds(w, 5);
    ASSERT_EQ(rec.evs.size(), 1u) << "the first page's lease was announced";
    ASSERT_EQ(rec.evs[0].kind, Kind::Granted);
    const Bytes first = rec.evs[0].key;
    Sim sim;
    sim.live[first] = rec.evs[0].ver;
    sim.begin(true);
    for (const auto& [k, v] : rowsByHash(r.stub->parts[0])) sim.row(k, v);
    sim.done();
    std::vector<Seq> want{{Kind::Granted, 0, first, rec.evs[0].ver}};
    want.insert(want.end(), sim.out.begin(), sim.out.end());
    want.push_back({Kind::Ready, -1, kNoKey, 0});
    breakPartition(r, 0);   // the first scan's subscription breaks
    r.rounds(w, 10);
    r.stub->onRequest = nullptr;
    r.stub->release();   // the held page is stale now
    g_now += 5 * kMs;
    ASSERT_TRUE(r.drive(w, [&] { return rec.readies == 1; }));
    r.rounds(w, 30);
    EXPECT_EQ(describeSeq(seqFrom(rec, 0)), describeSeq(want));
    EXPECT_EQ(rec.count(Kind::Confirmed, &first), 1);
    EXPECT_EQ(rec.count(Kind::RerunBegin), 1);
    EXPECT_EQ(rec.count(Kind::RerunEnd), 1);
    EXPECT_EQ(w.reruns(), 1u);
    EXPECT_TRUE(rec.open.empty());
    EXPECT_EQ(rec.bad, 0);
}

// The data reset x the five changes of the new lineage: the exact sequence of the re-run (the fold, written down independently).
TEST(LeaseWatchRerunRegression, DataResetFoldMatrixRegression) {
    enum class Chg { None, Update, End, Regrant, Grant };
    int n = 0;
    for (const Chg chg : {Chg::None, Chg::Update, Chg::End, Chg::Regrant, Chg::Grant}) {
        SCOPED_TRACE("change " + std::to_string(static_cast<int>(chg)));
        ++n;
        WRig r(invokerCfg(7300 + 2 * n, 7301 + 2 * n, 2), 2);
        const Bytes A = kb(0), B = kb(4), D = kb(12), G = kb(8);   // all in partition 0 of the stub
        r.stub->parts[0].grant(A);
        r.stub->parts[0].grant(B);
        rc::LeaseWatchBase w(*r.client, r.topic->core());
        Recorder rec;
        w.setHandler(&Recorder::fn, &rec);
        ASSERT_EQ(startWith(w, {Bytes{}}), rc::OpResult::Accepted);
        ASSERT_TRUE(r.drive(w, [&] { return w.isReady() && r.stub->caughtUp(0); }));
        r.rounds(w, 10);
        const auto first = *r.gw.latestSubscription(1, 0);
        r.gw.sendLineageNote(first, 1);
        r.stub->parts[0].grant(D);
        ASSERT_TRUE(r.drive(w, [&] { return rec.count(Kind::Granted, &D) == 1; }));
        r.rounds(w, 10);
        const std::size_t mark = rec.evs.size();
        Sim sim;
        for (const auto& h : rec.held) sim.live[h.first] = h.second;
        // The reborn partition: the same three leases at the same versions, then the change.
        r.gw.partitionLineage[0] = 2;
        {
            const std::size_t keyWidth = r.stub->parts[0].keyWidth;
            r.stub->parts[0] = LeaseModel{};
            r.stub->parts[0].keyWidth = keyWidth;
        }
        LeaseModel& m = r.stub->parts[0];
        m.grant(A);
        m.grant(B);
        m.grant(D);
        switch (chg) {
        case Chg::None: break;
        case Chg::Update: m.update(A); break;
        case Chg::End: m.revoke(A); break;
        case Chg::Regrant: m.revoke(A); m.grant(A); break;
        case Chg::Grant: m.grant(G); break;
        }
        sim.begin(true);
        for (const auto& [k, v] : rowsByHash(m)) sim.row(k, v);
        sim.done();
        r.gw.sendError(0, static_cast<int16_t>(rpc::ErrorCode::NotLeader), "", first.sessionId, first.corrId);
        ASSERT_TRUE(r.drive(w, [&] { g_now += kMs / 10; return r.gw.dataResetAnswers == 1 && w.dataResetRerunsForTest() >= 1; }));
        g_now += 2 * kMs;
        ASSERT_TRUE(r.drive(w, [&] { return rec.count(Kind::RerunEnd) == 1; }));
        r.rounds(w, 30);
        EXPECT_EQ(describeSeq(seqFrom(rec, mark)), describeSeq(sim.out));
        for (std::size_t i = mark; i < rec.evs.size(); ++i) {
            const Recorder::Ev& e = rec.evs[i];
            if (e.kind != Kind::Confirmed) continue;
            EXPECT_EQ(e.bytes, LeaseModel::row(e.key, m.table.at(e.key)));
        }
        EXPECT_TRUE(rec.open.empty());
        EXPECT_EQ(rec.bad, 0);
    }
}

// Failed or closed in the middle of a re-run (no RerunEnd), then restarted: the consumer drops its bracket, the restart opens a new one and
// folds the watch's table to the broker's, as the model says.
TEST(LeaseWatchRerunRegression, RestartAfterMidRerunCloseOrFailRegression) {
    for (const bool fail : {false, true}) {
        SCOPED_TRACE(fail ? "fail" : "close");
        WRig r(invokerCfg(7400 + (fail ? 2 : 0), 7401 + (fail ? 2 : 0), 1), 1);
        LeaseModel& m = r.stub->parts[0];
        const Bytes A = kb(1), B = kb(5), C = kb(9), G = kb(13);
        m.grant(A);
        m.grant(B);
        m.grant(C);
        rc::LeaseWatchOptions o;
        o.maxRows = 1;
        rc::LeaseWatchBase w(*r.client, r.topic->core(), o);
        Recorder rec;
        w.setHandler(&Recorder::fn, &rec);
        ASSERT_EQ(startWith(w, {Bytes{}}), rc::OpResult::Accepted);
        ASSERT_TRUE(r.drive(w, [&] { return w.isReady(); }));
        r.rounds(w, 10);
        breakPartition(r, 0);
        m.update(A);
        m.revoke(B);
        m.grant(G);
        r.rounds(w, 40);
        if (fail) {
            r.stub->errorCode = static_cast<int16_t>(rpc::ErrorCode::NotAuthorized);
            r.stub->errorNext = 1'000'000;
            g_now += 2 * kMs;
            ASSERT_TRUE(r.drive(w, [&] { return w.failed(); }));
        } else {
            r.stub->hold = true;
            g_now += 2 * kMs;
            ASSERT_TRUE(r.drive(w, [&] { return !rec.open.empty() && r.stub->heldCount() == 1; }));
        }
        EXPECT_FALSE(rec.open.empty()) << "the re-run was in progress";
        w.closeAsync();
        for (int i = 0; i < kCap && !w.isClosed(); ++i) { g_now += 2 * kMs; r.round(w); }
        ASSERT_TRUE(w.isClosed());
        r.stub->errorNext = 0;
        r.stub->errorCode = 0;
        r.stub->release();
        rec.endBrackets();
        EXPECT_EQ(rec.count(Kind::RerunEnd), 0);
        Sim sim;
        for (const auto& h : rec.held) sim.live[h.first] = h.second;
        const std::size_t mark = rec.evs.size();
        sim.begin(true);
        for (const auto& [k, v] : rowsByHash(m)) sim.row(k, v);
        sim.done();
        sim.out.push_back({Kind::Ready, -1, kNoKey, 0});
        const Bytes none;
        const std::span<const uint8_t> sp[] = {std::span<const uint8_t>(none)};
        ASSERT_EQ(w.restart(sp), rc::OpResult::Accepted);
        ASSERT_TRUE(r.drive(w, [&] { return rec.readies == 2; }));
        r.rounds(w, 30);
        EXPECT_EQ(describeSeq(seqFrom(rec, mark)), describeSeq(sim.out));
        Held want;
        for (const auto& [k, h] : m.table) want.insert({k, h.version});
        EXPECT_EQ(rec.held, want);
        EXPECT_EQ(w.liveCount(), m.table.size());
        EXPECT_TRUE(rec.open.empty());
        EXPECT_EQ(rec.bad, 0) << "the restart's RerunBegin is not a second open bracket";
    }
}

// A reborn partition (data reset) is a re-run: Begin, the old lineage's Vanished, the new lineage's Granted, End.
TEST(LeaseWatchRerunRegression, DataResetRerunIsBracketedRegression) {
    WRig r(invokerCfg(7015, 7016, 2), 2);
    const Bytes A = kb(0), B = kb(4), C = kb(8), D = kb(12);   // all in partition 0 of the stub
    r.stub->parts[0].grant(A);
    r.stub->parts[0].grant(B);
    rc::LeaseWatchBase w(*r.client, r.topic->core());
    Recorder rec;
    w.setHandler(&Recorder::fn, &rec);
    ASSERT_EQ(startWith(w, {Bytes{}}), rc::OpResult::Accepted);
    ASSERT_TRUE(r.drive(w, [&] { return w.isReady() && r.stub->caughtUp(0); }));
    r.rounds(w, 10);
    const auto first = *r.gw.latestSubscription(1, 0);
    r.gw.sendLineageNote(first, 1);
    r.stub->parts[0].grant(D);
    ASSERT_TRUE(r.drive(w, [&] { return rec.count(Kind::Granted, &D) == 1; }));
    r.rounds(w, 10);
    const std::size_t mark = rec.evs.size();
    r.gw.partitionLineage[0] = 2;
    { const std::size_t keyWidth = r.stub->parts[0].keyWidth; r.stub->parts[0] = LeaseModel{}; r.stub->parts[0].keyWidth = keyWidth; }
    const int64_t vc = r.stub->parts[0].grant(C);
    r.gw.sendError(0, static_cast<int16_t>(rpc::ErrorCode::NotLeader), "", first.sessionId, first.corrId);
    ASSERT_TRUE(r.drive(w, [&] { g_now += kMs / 10; return r.gw.dataResetAnswers == 1 && w.dataResetRerunsForTest() >= 1; }));
    g_now += 2 * kMs;
    ASSERT_TRUE(r.drive(w, [&] { return rec.count(Kind::RerunEnd) == 1; }));
    r.rounds(w, 30);
    const std::vector<Seq> got = seqFrom(rec, mark);
    ASSERT_GE(got.size(), 6u);
    EXPECT_EQ(got.front().kind, Kind::RerunBegin);
    EXPECT_EQ(got.back().kind, Kind::RerunEnd);
    EXPECT_EQ(rec.count(Kind::RerunBegin), 1);
    EXPECT_EQ(rec.count(Kind::Vanished), 3);
    EXPECT_EQ(rec.count(Kind::Confirmed), 0) << "nothing of the old lineage survives";
    EXPECT_EQ(describeSeq({got[got.size() - 2]}), describeSeq({{Kind::Vanished, 0, got[got.size() - 2].key, got[got.size() - 2].ver}}) ) << "Vanished closes the run, before RerunEnd";
    EXPECT_EQ(rec.count(Kind::Granted, &C), 1);
    (void)vc;
    EXPECT_TRUE(rec.open.empty());
    EXPECT_EQ(rec.bad, 0);
}

// The LeaseSession observes the watch: the three new kinds change nothing for the lease it holds.
TEST(LeaseWatchRerunRegression, SessionObserverIgnoresRerunEventsRegression) {
    WRig r(invokerCfg(7017, 7018, 1), 1);
    r.makeSession();
    Bytes key1(8, 0);
    key1[0] = 1;
    rc::OpResult err = rc::OpResult::Accepted;
    (void)r.session->add(*r.topic, Row{1}, 5, 3000, g_now, &err);
    ASSERT_EQ(err, rc::OpResult::Accepted);
    r.stub->parts[0].setRow(key1, 5, 0);
    rc::LeaseWatchBase w(*r.client, r.topic->core());
    Recorder rec;
    w.setHandler(&Recorder::fn, &rec);
    ASSERT_TRUE(r.session->observe(w));
    ASSERT_EQ(startWith(w, {Bytes{}}), rc::OpResult::Accepted);
    ASSERT_TRUE(r.drive(w, [&] { return w.isReady(); }));
    r.rounds(w, 10);
    breakPartition(r, 0);
    r.rounds(w, 40);
    g_now += 2 * kMs;
    ASSERT_TRUE(r.drive(w, [&] { return rec.count(Kind::RerunEnd) == 1; }));
    r.rounds(w, 30);
    EXPECT_EQ(rec.count(Kind::Confirmed, &key1), 1);
    EXPECT_TRUE(r.cb.lost.empty());
    EXPECT_EQ(r.session->liveCount(), 1u);
    EXPECT_TRUE(r.session->unobserve(w));
}

// ---- Thread mode: the client's runner thread drives the subscriptions, a pump thread plays the broker ----

/// A Thread-mode client against the stub broker.  The stub and the gateway belong to the pump thread while it runs; the test thread touches
/// them only while the pump is stopped.  Every wait is capped by the wall clock and backs off through an idle strategy.
struct ThreadRig {
    rc::ClientConfig                              cfg;
    std::unique_ptr<rc::Client>                   client;
    std::unique_ptr<test_support::InvokerGateway> gw;
    std::unique_ptr<LeaseStub>                    stub;
    std::optional<rc::LeaseTopic<Row>>            topic;
    std::thread                                   pump;
    int                                           stopFd[2] = {-1, -1};
    uint32_t                                      parts;

    ThreadRig(int32_t in, int32_t eg, uint32_t partitions) : parts(partitions) {
        cfg            = invokerCfg(in, eg, static_cast<int32_t>(partitions));
        cfg.runnerMode = rc::RunnerMode::Thread;
        cfg.nanoClock  = nullptr;   // the real clock: the runner thread must not read the test's fake one
        client = std::make_unique<rc::Client>(cfg);   // the client first: it owns the media driver the stub broker attaches to
        gw = std::make_unique<test_support::InvokerGateway>(cfg.dataDir + "/aeron", cfg.gatewayChannel, cfg.gatewayStreamId, cfg.egressChannel,
                                                            cfg.egressStreamId);
        gw->idempotence            = true;
        gw->replySchemas           = true;
        gw->schemaBlob             = rune::codec::serialize(watchDescriptor());
        gw->schemaTopicId          = 1;
        gw->schemaPartitionCount   = static_cast<int32_t>(partitions);
        gw->schemaLeaseViewTopicId = 9;
        rune::protocol::TopicKeyFeatures f;
        f.keyIndex = true;
        gw->schemaKeyFeatures = f;
        gw->schemaLease       = kSpec;
        gw->kaAutoOk          = true;
        stub = std::make_unique<LeaseStub>(*gw, 1, partitions);
        stub->install();
    }
    ~ThreadRig() {
        stopPump();
        topic.reset();
        gw.reset();
    }
    void startPump() {
        ASSERT_EQ(::pipe(stopFd), 0);
        pump = std::thread([this] {
            pollfd pfd{stopFd[0], POLLIN, 0};
            const auto wallEnd = std::chrono::steady_clock::now() + std::chrono::seconds(30);   // the pump's own hard cap
            while (::poll(&pfd, 1, 0) == 0 && std::chrono::steady_clock::now() < wallEnd) {
                for (uint32_t p = 0; p < parts; ++p) (void)stub->feed(static_cast<int32_t>(p));
                gw->pump();
            }
        });
    }
    void stopPump() {
        if (pump.joinable()) {
            const char b = 0;
            (void)!::write(stopFd[1], &b, 1);
            pump.join();
        }
        for (int& fd : stopFd) if (fd >= 0) { ::close(fd); fd = -1; }
    }
    void open() {
        startPump();
        client->connectAndWait(std::chrono::seconds(10));
        topic.emplace(client->openLeaseTopicAndWait<Row>("lease_t", {}, std::chrono::seconds(10)));
    }
    /// Drives the watch on this thread until `pred` holds; false after 5 s.
    template <class P>
    bool wait(rc::LeaseWatchBase& w, P&& pred) {
        const auto end = std::chrono::steady_clock::now() + std::chrono::seconds(5);
        rc::DefaultWaitIdle idle;
        while (!pred()) {
            if (std::chrono::steady_clock::now() >= end) return false;
            (void)w.doWork(client->nowNs());
            idle.idle();
        }
        return true;
    }
};

// Closing lasts while the runner thread works the Unsubscribe off; the restart over the same table folds an update, an end and a grant made
// while the watch was closed (the broker stub is edited while its pump is stopped).
TEST(LeaseWatchThreadRegression, ThreadModeCloseRestartRegression) {
    ThreadRig t(6501, 6502, 1);
    LeaseModel& m = t.stub->parts[0];
    for (int i = 0; i < 3; ++i) m.grant(kb(i));
    t.open();
    ASSERT_TRUE(t.topic.has_value());
    rc::LeaseWatchBase w(*t.client, t.topic->core());
    Recorder rec;
    w.setHandler(&Recorder::fn, &rec);
    ASSERT_EQ(startWith(w, {Bytes{}}), rc::OpResult::Accepted);
    ASSERT_TRUE(t.wait(w, [&] { return w.isReady(); }));
    ASSERT_EQ(rec.count(Kind::Granted), 3);
    const std::size_t events = rec.evs.size();
    w.closeAsync();
    EXPECT_EQ(w.life(), Life::Closing);
    EXPECT_FALSE(w.isClosed());
    EXPECT_FALSE(w.isReady());
    ASSERT_TRUE(t.wait(w, [&] { return w.isClosed(); }));
    EXPECT_EQ(w.life(), Life::Closed);
    EXPECT_EQ(rec.evs.size(), events) << "no event after closeAsync";
    t.stopPump();
    m.update(kb(1));
    m.revoke(kb(2));
    m.grant(kb(3));
    t.startPump();
    const Bytes none;
    const std::span<const uint8_t> sp[] = {std::span<const uint8_t>(none)};
    ASSERT_EQ(w.restart(sp), rc::OpResult::Accepted);
    ASSERT_TRUE(t.wait(w, [&] { return rec.readies == 2; }));
    const Bytes k0 = kb(0), k1 = kb(1), k2 = kb(2), k3 = kb(3);
    EXPECT_EQ(rec.count(Kind::Confirmed, &k0), 1);
    EXPECT_EQ(rec.count(Kind::Confirmed, &k1), 1);
    EXPECT_EQ(rec.count(Kind::Vanished, &k2), 1);
    EXPECT_EQ(rec.count(Kind::Granted, &k3), 1);
    EXPECT_EQ(w.liveCount(), 3u);
    EXPECT_EQ(heldKeys(rec), (Ids{kb(0), kb(1), kb(3)}));
    EXPECT_EQ(rec.bad, 0);
    t.stopPump();
    w.closeAsync();
    ASSERT_TRUE(t.wait(w, [&] { return w.isClosed(); }));
}

// The destructor in Thread mode, in every life: Running and Closing wait for the runner thread to finish the subscriptions; Closed returns at
// once.  The client is fine afterwards: a new watch reaches Ready.
TEST(LeaseWatchThreadRegression, ThreadModeDestructorInEveryLifeRegression) {
    ThreadRig t(6503, 6504, 1);
    for (int i = 0; i < 3; ++i) t.stub->parts[0].grant(kb(i));
    t.open();
    ASSERT_TRUE(t.topic.has_value());
    for (int variant = 0; variant < 3; ++variant) {   // Running, Closing, Closed
        SCOPED_TRACE("variant " + std::to_string(variant));
        auto w = std::make_unique<rc::LeaseWatchBase>(*t.client, t.topic->core());
        Recorder rec;
        w->setHandler(&Recorder::fn, &rec);
        ASSERT_EQ(startWith(*w, {Bytes{}}), rc::OpResult::Accepted);
        ASSERT_TRUE(t.wait(*w, [&] { return w->isReady(); }));
        if (variant >= 1) w->closeAsync();
        if (variant == 2) ASSERT_TRUE(t.wait(*w, [&] { return w->isClosed(); }));
        const std::size_t events = rec.evs.size();
        w.reset();   // returns: a hang here is a failed test run
        EXPECT_EQ(rec.evs.size(), events) << "no event from a destructor";
    }
    rc::LeaseWatchBase again(*t.client, t.topic->core());
    Recorder rec;
    again.setHandler(&Recorder::fn, &rec);
    ASSERT_EQ(startWith(again, {Bytes{}}), rc::OpResult::Accepted);
    ASSERT_TRUE(t.wait(again, [&] { return again.isReady(); }));
    EXPECT_EQ(rec.count(Kind::Granted), 3);
    t.stopPump();
    again.closeAsync();
    ASSERT_TRUE(t.wait(again, [&] { return again.isClosed(); }));
}

// A Closed watch holds no subscription: its destructor does not touch the client, so it may run after the client is gone.
TEST(LeaseWatchThreadRegression, ThreadModeClosedWatchOutlivesClientRegression) {
    auto t = std::make_unique<ThreadRig>(6505, 6506, 1);
    t->stub->parts[0].grant(kb(1));
    t->open();
    ASSERT_TRUE(t->topic.has_value());
    auto w = std::make_unique<rc::LeaseWatchBase>(*t->client, t->topic->core());
    ASSERT_EQ(startWith(*w, {Bytes{}}), rc::OpResult::Accepted);
    ASSERT_TRUE(t->wait(*w, [&] { return w->isReady(); }));
    w->closeAsync();
    ASSERT_TRUE(t->wait(*w, [&] { return w->isClosed(); }));
    t->stopPump();
    t->topic.reset();
    t->gw.reset();
    const bool closedBefore = w->isClosed();
    t->client.reset();   // the client is gone
    w.reset();           // and the closed watch is destroyed without touching it (a use of the dead client is the sanitizer legs' finding)
    EXPECT_TRUE(closedBefore) << "the watch was Closed when the client went away";
}

}  // namespace
