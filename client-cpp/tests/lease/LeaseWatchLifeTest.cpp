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
    enum class Do { CloseAsync, CloseAsyncTwice, Start, DoWork, RegisterName };
    struct Row { In in; Do op; Life out; bool closed; };
    const Row table[] = {
        {In::Fresh, Do::CloseAsync, Life::Closed, true},        {In::Fresh, Do::CloseAsyncTwice, Life::Closed, true},
        {In::Fresh, Do::Start, Life::Running, false},           {In::Fresh, Do::DoWork, Life::Fresh, true},
        {In::Fresh, Do::RegisterName, Life::Fresh, true},
        {In::Scanning, Do::CloseAsync, Life::Closing, false},   {In::Scanning, Do::CloseAsyncTwice, Life::Closing, false},
        {In::Scanning, Do::Start, Life::Running, false},        {In::Scanning, Do::DoWork, Life::Running, false},
        {In::Scanning, Do::RegisterName, Life::Running, false},
        {In::Ready, Do::CloseAsync, Life::Closing, false},      {In::Ready, Do::CloseAsyncTwice, Life::Closing, false},
        {In::Ready, Do::Start, Life::Running, false},           {In::Ready, Do::DoWork, Life::Running, false},
        {In::Ready, Do::RegisterName, Life::Running, false},
        {In::Failed, Do::CloseAsync, Life::Closing, false},     {In::Failed, Do::CloseAsyncTwice, Life::Closing, false},
        {In::Failed, Do::Start, Life::Failed, false},           {In::Failed, Do::DoWork, Life::Failed, false},
        {In::Failed, Do::RegisterName, Life::Failed, false},
        {In::Closing, Do::CloseAsync, Life::Closing, false},    {In::Closing, Do::CloseAsyncTwice, Life::Closing, false},
        {In::Closing, Do::Start, Life::Closing, false},         {In::Closing, Do::DoWork, Life::Closed, true},
        {In::Closing, Do::RegisterName, Life::Closing, false},
        {In::Closed, Do::CloseAsync, Life::Closed, true},       {In::Closed, Do::CloseAsyncTwice, Life::Closed, true},
        {In::Closed, Do::Start, Life::Closed, true},            {In::Closed, Do::DoWork, Life::Closed, true},
        {In::Closed, Do::RegisterName, Life::Closed, true},
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
        }
        EXPECT_EQ(w.life(), row.out);
        EXPECT_EQ(w.isClosed(), row.closed);
        if (row.in == In::Closing || row.in == In::Closed) EXPECT_EQ(rec.evs.size(), events) << "no event once closing";
        // Every row ends the same way: a closing watch reaches Closed within the cap.
        w.closeAsync();
        for (int i = 0; i < kCap && !w.isClosed(); ++i) { g_now += 2 * kMs; r.round(w); }
        EXPECT_TRUE(w.isClosed());
    }
}

}  // namespace
