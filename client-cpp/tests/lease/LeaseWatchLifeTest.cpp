// tests/lease/LeaseWatchLifeTest.cpp - the LeaseWatch life states (Fresh, Running, Failed, Closing, Closed): closeAsync() never blocks and never
// calls client.doWork(); isClosed(); a destructor that returns at once.  Against the stub broker of LeaseWatchRig.h on a fake clock; every drive
// loop is capped and nothing sleeps.
#include "LeaseWatchRig.h"

#include <map>

namespace {

using Life = rc::LeaseWatchBase::Life;

constexpr int kCap = 200;   // the hard iteration cap of every drive loop here

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
    const uint64_t before = r.client->dutyCyclesForTest();
    w.reset();
    EXPECT_EQ(r.client->dutyCyclesForTest(), before) << "the destructor of a closed watch pumps nothing";
}

TEST(LeaseWatchLifeRegression, CloseAsyncInsideHandlerRegression) {
    WRig r(invokerCfg(6105, 6106, 2), 2);
    for (int i = 0; i < 3; ++i) r.stub->parts[static_cast<std::size_t>(i) % 2].grant(kb(i));
    rc::LeaseWatchBase w(*r.client, r.topic->core());
    struct H {
        rc::LeaseWatchBase* w;
        int granted = 0, after = 0;
        static void fn(void* c, const rc::LeaseEvent& e) {
            auto* h = static_cast<H*>(c);
            if (e.kind != Kind::Granted) { if (h->w->isClosed() || h->w->life() == Life::Closing) ++h->after; return; }
            ++h->granted;
            h->w->closeAsync();
        }
    } h{&w};
    w.setHandler(&H::fn, &h);
    ASSERT_EQ(startWith(w, {Bytes{}}), rc::OpResult::Accepted);
    for (int i = 0; i < kCap && !w.isClosed(); ++i) r.round(w);
    EXPECT_TRUE(w.isClosed());
    EXPECT_EQ(w.life(), Life::Closed);
    EXPECT_EQ(h.granted, 1) << "the other leases' events are not delivered";
    EXPECT_EQ(h.after, 0);
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

// The "LeaseWatch life" matrix: the state the watch is in x the operation applied to it -> the state it ends in.
TEST(LeaseWatchLifeRegression, LifeMatrixRegression) {
    enum class In { Fresh, Scanning, Ready, Failed, Closing, Closed };
    enum class Do { CloseAsync, CloseAsyncTwice, Start, DoWork, RegisterName, Restart };
    struct Row { In in; Do op; Life out; bool closed; };
    const Row table[] = {
        {In::Fresh, Do::CloseAsync, Life::Closed, true},        {In::Fresh, Do::CloseAsyncTwice, Life::Closed, true},
        {In::Fresh, Do::Start, Life::Running, false},           {In::Fresh, Do::DoWork, Life::Fresh, true},
        {In::Fresh, Do::RegisterName, Life::Fresh, true},
        {In::Fresh, Do::Restart, Life::Fresh, true},
        {In::Scanning, Do::CloseAsync, Life::Closing, false},   {In::Scanning, Do::CloseAsyncTwice, Life::Closing, false},
        {In::Scanning, Do::Start, Life::Running, false},        {In::Scanning, Do::DoWork, Life::Running, false},
        {In::Scanning, Do::RegisterName, Life::Running, false},
        {In::Scanning, Do::Restart, Life::Running, false},
        {In::Ready, Do::CloseAsync, Life::Closing, false},      {In::Ready, Do::CloseAsyncTwice, Life::Closing, false},
        {In::Ready, Do::Start, Life::Running, false},           {In::Ready, Do::DoWork, Life::Running, false},
        {In::Ready, Do::RegisterName, Life::Running, false},
        {In::Ready, Do::Restart, Life::Running, false},
        {In::Failed, Do::CloseAsync, Life::Closing, false},     {In::Failed, Do::CloseAsyncTwice, Life::Closing, false},
        {In::Failed, Do::Start, Life::Failed, false},           {In::Failed, Do::DoWork, Life::Failed, false},
        {In::Failed, Do::RegisterName, Life::Failed, false},
        {In::Failed, Do::Restart, Life::Failed, false},
        {In::Closing, Do::CloseAsync, Life::Closing, false},    {In::Closing, Do::CloseAsyncTwice, Life::Closing, false},
        {In::Closing, Do::Start, Life::Closing, false},         {In::Closing, Do::DoWork, Life::Closed, true},
        {In::Closing, Do::RegisterName, Life::Closing, false},
        {In::Closing, Do::Restart, Life::Closing, false},
        {In::Closed, Do::CloseAsync, Life::Closed, true},       {In::Closed, Do::CloseAsyncTwice, Life::Closed, true},
        {In::Closed, Do::Start, Life::Closed, true},            {In::Closed, Do::DoWork, Life::Closed, true},
        {In::Closed, Do::RegisterName, Life::Closed, true},
        {In::Closed, Do::Restart, Life::Running, false},
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
        rc::LeaseWatchBase w(*r.client, r.topic->core());
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
        switch (row.op) {
        case Do::CloseAsync: w.closeAsync(); break;
        case Do::CloseAsyncTwice: w.closeAsync(); w.closeAsync(); break;
        case Do::Start: (void)startWith(w, {Bytes{}}); break;
        case Do::DoWork: (void)w.doWork(g_now); break;
        case Do::RegisterName: (void)w.registerName(std::span<const uint8_t>(kb(1).data(), 1)); break;
        case Do::Restart: {
            const Bytes none;
            const std::span<const uint8_t> sp[] = {std::span<const uint8_t>(none)};
            const rc::OpResult res = w.restart(sp);
            EXPECT_EQ(res, row.in == In::Closed ? rc::OpResult::Accepted : rc::OpResult::InvalidArgument);
            break;
        }
        }
        EXPECT_EQ(w.life(), row.out);
        EXPECT_EQ(w.isClosed(), row.closed);
        if ((row.in == In::Closing || row.in == In::Closed) && row.op != Do::Restart) EXPECT_EQ(rec.evs.size(), events) << "no event once closing";
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
    EXPECT_EQ(f.w.liveCount(), 5u);
    EXPECT_TRUE(f.w.isReady());
    // The restarted watch is live: a later grant is delivered.
    f.r.stub->parts[1].grant(kb(7));
    ASSERT_TRUE(f.r.drive(f.w, [&] { return f.rec.held.size() == 6; }));
    EXPECT_EQ(f.rec.bad, 0);
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

// The watch's thread allocates in exactly one place on a (re)start: Client::subscribe (control plane: one Subscription object per partition
// when the first page answers).  The restart adds nothing of its own: its allocations are at most those of the first start, which opens the same
// subscriptions.  (Everything else - closeAsync, restart, the scans, the fold - allocates nothing.)
TEST(LeaseWatchRestartRegression, RestartNoAllocRegression) {
    WRig r(invokerCfg(6307, 6308, 2), 2);
    for (int i = 0; i < 8; ++i) r.stub->parts[static_cast<std::size_t>(i) % 2].grant(kb(i));
    rc::LeaseWatchBase w(*r.client, r.topic->core());
    long events = 0;
    w.setHandler([](void* c, const rc::LeaseEvent&) { ++*static_cast<long*>(c); }, &events);
    const Bytes none;
    const std::span<const uint8_t> sp[] = {std::span<const uint8_t>(none)};
    // One window per watch-thread call; the stub and the client are outside it.
    const auto pumpOnce = [&](long& allocs, auto&& inWindow) {
        for (uint32_t p = 0; p < 2; ++p) (void)r.stub->feed(static_cast<int32_t>(p));
        r.client->doWork();
        r.gw.pump();
        alloc_counter::startThisThread();
        (void)w.doWork(g_now);
        inWindow();
        allocs += alloc_counter::stopThisThread();
    };
    long firstStart = 0;
    ASSERT_EQ(w.startFromNow(sp), rc::OpResult::Accepted);
    for (int i = 0; i < kCap && !w.isReady(); ++i) pumpOnce(firstStart, [] {});
    ASSERT_TRUE(w.isReady());
    r.rounds(w, 20);
    long again = 0;
    alloc_counter::startThisThread();
    w.closeAsync();
    again += alloc_counter::stopThisThread();
    bool restarted = false;
    for (int i = 0; i < kCap && !(restarted && w.isReady()); ++i)
        pumpOnce(again, [&] { if (!restarted && w.isClosed()) restarted = w.restart(sp) == rc::OpResult::Accepted; });
    ASSERT_TRUE(restarted);
    ASSERT_TRUE(w.isReady());
    EXPECT_GT(firstStart, 0) << "the baseline sees the subscribe allocations";
    EXPECT_LE(again, firstStart) << "closeAsync + restart + the second run allocate no more than the first start (its subscribes)";
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
    EXPECT_GE(bound.events - before, 3) << "Vanished q/a, Granted q/c, Ready reach the bound recipe";
    EXPECT_EQ(w.registerName(key("q")), -1) << "names stay frozen after the first start";
    EXPECT_EQ(rec.bad, 0);
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

}  // namespace
