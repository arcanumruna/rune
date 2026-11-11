// Pause / resume of one subscription through the real Client (ring + trampoline), with a LeaseSession on the SAME client:
// pausing a data subscription never starves the session's keepalives.  Fake time; every drive loop is capped.
#include "../lease/LeaseWatchRig.h"

namespace {

void noopRecords(const rc::Record&) {}

TEST(SubscriberPauseClientRegression, OtherSubscriptionKeepsFlowingLeaseSessionKeepsAliveRegression) {
    WRig r(invokerCfg(7311, 7312, 1), 2);
    r.makeSession();
    rc::OpResult err = rc::OpResult::Accepted;
    (void)r.session->add(*r.topic, Row{1}, 5, 3000, g_now, &err);
    ASSERT_EQ(err, rc::OpResult::Accepted);

    const rc::TopicHandle th{r.topic->core().topicId(), r.topic->core().partitionCount(), rc::TopicKind::Typed, rune::protocol::Partitioner::Fnv1a};
    int got1 = 0, got2 = 0;
    rc::Subscription sub  = r.client->subscribe(th, 1, {}, [&](const rc::Record&) { ++got1; });
    rc::LeaseWatchBase w(*r.client, r.topic->core());   // only to reuse the rig's round()
    for (int i = 0; i < 400 && r.gw.subscribed.size() < 1; ++i) r.round(w);
    rc::Subscription sub2 = r.client->subscribe(th, 2, {}, [&](const rc::Record&) { ++got2; });
    for (int i = 0; i < 400 && r.gw.subscribed.size() < 2; ++i) r.round(w);
    ASSERT_EQ(r.gw.subscribed.size(), 2u);
    const auto first  = r.gw.subscribed[0];
    const auto second = r.gw.subscribed[1];
    const std::size_t subs0 = r.gw.subscribed.size();

    ASSERT_TRUE(sub.pause());
    EXPECT_TRUE(sub.paused());
    EXPECT_FALSE(sub.active());
    r.rounds(w, 20);

    const std::size_t ka0 = r.gw.kaSeen.size();
    const int64_t ttl = 3000 * kMs;
    for (int step = 0; step < 60; ++step) {   // 3 x ttl of fake time while the data subscription is paused
        g_now += ttl / 20;
        r.rounds(w, 4);
    }
    EXPECT_GT(r.gw.kaSeen.size(), ka0);       // the session kept answering keepalives
    EXPECT_TRUE(r.cb.lost.empty());
    EXPECT_TRUE(r.cb.atRisk.empty()) << "pausing a subscription never puts the lease at risk";
    // The other subscription keeps flowing while the first one is paused.
    r.gw.pushRecords(second, 0, 3);
    for (int i = 0; i < 60 && got2 < 3; ++i) r.round(w);
    EXPECT_EQ(got2, 3);
    EXPECT_EQ(got1, 0);
    EXPECT_TRUE(sub2.active());
    (void)first;
    EXPECT_EQ(r.session->liveCount(), 1u);
    EXPECT_EQ(r.gw.subscribed.size(), subs0); // a pause sends no new Subscribe

    ASSERT_TRUE(sub.resume());
    for (int i = 0; i < 400 && r.gw.subscribed.size() == subs0; ++i) r.round(w);
    EXPECT_GT(r.gw.subscribed.size(), subs0); // the resume re-armed the subscription on the wire
    EXPECT_FALSE(sub.paused());
    EXPECT_TRUE(r.cb.lost.empty());
    EXPECT_TRUE(r.cb.atRisk.empty());
}

// C-1 on the real Invoker Client: the handler of record 4 of a 10-record frame pauses at its own offset.  The frame's records
// are above the last status snapshot, so the pause is decided by the hub on the owner thread, not by the handle's cache.
TEST(SubscriberPauseClientRegression, InvokerHandlerPauseAtRecordOffsetMidFrameRegression) {
    WRig r(invokerCfg(7313, 7314, 1), 2);
    const rc::TopicHandle th{r.topic->core().topicId(), r.topic->core().partitionCount(), rc::TopicKind::Typed, rune::protocol::Partitioner::Fnv1a};
    std::unique_ptr<rc::Subscription> sub;
    std::vector<int64_t> got;
    bool paused = false;
    int  pauseCalls = 0;
    sub = std::make_unique<rc::Subscription>(r.client->subscribe(th, 1, {}, [&](const rc::Record& rec) {
        got.push_back(rec.offset);
        if (rec.offset == 4 && pauseCalls++ == 0) paused = sub->pause(rec.offset);
    }));
    rc::LeaseWatchBase w(*r.client, r.topic->core());
    for (int i = 0; i < 400 && r.gw.latestSubscription(1, 1) == nullptr; ++i) r.round(w);
    const auto* gs = r.gw.latestSubscription(1, 1);
    ASSERT_NE(gs, nullptr);
    const auto subscribed0 = r.gw.subscribed.size();
    r.gw.pushRecords(*gs, 0, 10);
    for (int i = 0; i < 60 && got.size() < 5; ++i) r.round(w);
    r.rounds(w, 20);
    EXPECT_TRUE(paused) << "pause(rec.offset) was refused in a handler of the Invoker owner";
    EXPECT_EQ(got, (std::vector<int64_t>{0, 1, 2, 3, 4})) << "nothing after the refused record";
    EXPECT_TRUE(sub->paused());
    EXPECT_FALSE(sub->active());

    ASSERT_TRUE(sub->resume());
    for (int i = 0; i < 400 && r.gw.subscribed.size() == subscribed0; ++i) r.round(w);
    ASSERT_GT(r.gw.subscribed.size(), subscribed0);
    EXPECT_EQ(r.gw.subscribed.back().fromOffset, 4) << "resumes at the refused record";
}

}  // namespace
