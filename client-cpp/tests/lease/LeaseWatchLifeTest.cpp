// tests/lease/LeaseWatchLifeTest.cpp - the LeaseWatch life states (Fresh, Running, Failed, Closing, Closed): closeAsync() never blocks and never
// calls client.doWork(); isClosed(); a destructor that returns at once.  Against the stub broker of LeaseWatchRig.h on a fake clock; every drive
// loop is capped and nothing sleeps.
#include "LeaseWatchRig.h"

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

}  // namespace
