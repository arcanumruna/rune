// SubscriptionPauseBrokerRegressionTest.cpp - Rune prerequisites for Herald, Task 7 (R9): Subscription::pause(resumeAt) / resume() /
// paused() on REAL brokers, the REAL client, default config.
//   1. BackpressuredTailLosesNothing        (1 node)   a handler that pauses on a full 64-slot ring, a consumer that resumes it when
//                                                      empty: 5000 records arrive 0..4999 in order, and a LeaseSession on the same
//                                                      client never reports at-risk or lost.
//   2. SecondSubscriptionUnaffected         (1 node)   a subscription paused for the whole test does not slow another one.
//   3. LeaderChangeWhilePaused              (3 nodes)  pause at offset 500 of 1000, stop the partition leader, resume on the new one.
//   4. ReadCommittedPauseInsideTransaction  (1 node)   a read_committed subscription pauses inside a committed transaction's batch.
// Every drive loop has a hard iteration cap and FAILS rather than hangs; nothing waits on wall time.  Tests 1, 3 and 4 end with a
// SENTINEL record produced after the expected ones: a duplicate delivered after the last expected record arrives before the
// sentinel, so the exact sequence (and position() == last + 1) is asserted only once the sentinel was seen.  Test 2 needs none
// (its paused subscription never resumes; it settles on 50 more records of the other topic).
#include "client/ClusterClient.h"
#include "client/OneBroker.h"

#include "gen/lease_claim.gen.h"

#include <rune/client/Client.h>
#include <rune/client/Completions.h>
#include <rune/client/lease/LeaseSession.h>
#include <rune/client/lease/LeaseTopic.h>
#include <rune/client/subscriber/Subscription.h>
#include <rune/codec/Descriptor.h>
#include <rune/codec/Serialize.h>
#include <rune/protocol/LeaseSpec.h>

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <string>
#include <thread>
#include <vector>

using namespace std::chrono_literals;
using namespace rune::node::test;
using namespace rune::node::test::catalog_cluster;
namespace rc = rune::client;

namespace {

using rune::gen::LeaseClaim;

constexpr uint64_t kSpinCap = 40'000'000;   // hard iteration cap of every drive loop (a yield each: seconds, never forever)

/// The handler reads the handle it belongs to, which the test thread is still assigning when the first batch of an
/// already-produced topic arrives: the handler waits (bounded) for `armed`, set right after subscribe() returned.
void awaitArmed(const std::atomic<bool>& armed) {
    for (uint64_t spin = 0; spin < kSpinCap && !armed.load(std::memory_order_acquire); ++spin) std::this_thread::yield();
}

rc::SendResult spinSend(rc::Producer& p, rc::Completions& cq, rc::TopicHandle th, uint64_t key, const std::string& v,
                        rc::Token token) {
    rc::SendResult r = rc::SendResult::BackPressured;
    for (uint64_t spin = 0; spin < kSpinCap; ++spin) {
        r = p.trySend(th, rc::Key(key), std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(v.data()), v.size()), cq, token);
        if (r == rc::SendResult::Ok) return r;
        if (r == rc::SendResult::QueueFull) {
            (void)cq.poll([](const rc::Completion&, std::span<const uint8_t>) {});
        } else if (r != rc::SendResult::BackPressured) {
            return r;
        }
        std::this_thread::yield();
    }
    return r;
}

/// Produces `n` records (value "r<i>") with ack tokens i+1 on `th` and waits until every one completed Ok (hard cap).
/// Partition 0 of a 1-partition topic: the offsets are the sequence.  Returns the number of Ok completions.
int produceAll(rc::Client& client, rc::TopicHandle th, int n, rc::Acks acks, int first = 0) {
    rc::ProducerConfig pc;
    pc.acks     = acks;
    pc.lingerUs = 1;
    rc::Producer p = client.producer(pc);
    rc::Completions cq(client);
    int ok = 0, done = 0;
    auto count = [&](const rc::Completion& c, std::span<const uint8_t>) {
        ++done;
        if (c.status == rc::OpStatus::Ok) ++ok;
    };
    for (int i = 0; i < n; ++i) {
        if (spinSend(p, cq, th, static_cast<uint64_t>(first + i), "r" + std::to_string(first + i), static_cast<rc::Token>(i) + 1) !=
            rc::SendResult::Ok)
            break;
        (void)cq.poll(count);
    }
    p.flush();
    for (uint64_t spin = 0; spin < kSpinCap && done < n; ++spin) {
        if (cq.poll(count) == 0) std::this_thread::yield();
    }
    p.close();
    return ok;
}

rc::TopicSpec defineOne(rc::Client& c, const std::string& name, int32_t replication = 1) {
    rc::TopicOptions to;
    to.replicationFactor = replication;
    const auto r = c.defineTopicAndWait(name, 1, "", {}, rc::Persistence::Disk, to, {}, 60s);
    if (r.errorCode != 0) throw std::runtime_error("define " + name + ": " + r.detail);
    return rc::TopicSpec{name, r.topicId, 1};
}

/// A fresh client of `b` that knows `topics` (a TopicHandle is valid only for topics named in the config).
std::unique_ptr<rc::Client> openClientOn(OneBroker& b, std::vector<rc::TopicSpec> topics) {
    rc::ClientConfig cfg = b.clientConfig();
    cfg.topics           = std::move(topics);
    auto client          = std::make_unique<rc::Client>(cfg);
    client->connectAndWait(30s);
    return client;
}

/// The consumer side of a handler: a fixed single-producer / single-consumer ring (the duty thread offers, the test thread
/// drains), like a real application's.  A full ring REFUSES (tryPush returns false) and never blocks the duty thread.
struct TestRing {
    static constexpr std::size_t kCap = 64;
    std::array<int64_t, kCap> slots{};
    alignas(64) std::atomic<uint64_t> head{0};   // written by the consumer
    alignas(64) std::atomic<uint64_t> tail{0};   // written by the producer
    std::atomic<int> pauses{0};
    /// The handler body: push, or - when full - pause naming the refused record.
    void offer(rc::Subscription& sub, const rc::Record& r) {
        const uint64_t t = tail.load(std::memory_order_relaxed);
        if (t - head.load(std::memory_order_acquire) >= kCap) {
            pauses.fetch_add(1, std::memory_order_relaxed);
            (void)sub.pause(r.offset);
            return;
        }
        slots[t % kCap] = r.offset;
        tail.store(t + 1, std::memory_order_release);
    }
    /// Test thread: up to `max` records, in order.
    std::size_t drainInto(std::vector<int64_t>& out, std::size_t max = static_cast<std::size_t>(-1)) {
        const uint64_t h = head.load(std::memory_order_relaxed);
        const uint64_t n = std::min<uint64_t>(tail.load(std::memory_order_acquire) - h, max);
        for (uint64_t i = 0; i < n; ++i) out.push_back(slots[(h + i) % kCap]);
        head.store(h + n, std::memory_order_release);
        return static_cast<std::size_t>(n);
    }
    [[nodiscard]] bool emptyForConsumer() const {
        return head.load(std::memory_order_relaxed) == tail.load(std::memory_order_acquire);
    }
};

// ---- the lease topic of test 1 ----
rc::TopicOptions leaseTopicOptions() {
    rc::TopicOptions to;
    to.lease = rune::protocol::LeaseSpec{.ttlMs = 60'000, .minTtlMs = 1000, .maxTtlMs = 600'000, .maxKeys = 1000, .expiryBatch = 64,
                                         .checkpoint = 0, .checkpointIntervalMs = 1000, .checkpointRate = 1};
    to.config.cleanupPolicy = 1;
    return to;
}
std::vector<uint8_t> claimBlob() {
    return rune::codec::serialize(rune::codec::DescriptorBuilder("lease_claim")
                                      .addFixedStr("claim", 32, 1, true)
                                      .add("a", rune::codec::PrimType::UInt64)
                                      .addFixedStr("b", 8)
                                      .build());
}
LeaseClaim claimRow(std::string_view key) {
    LeaseClaim r;
    std::memcpy(r.claim.data(), key.data(), std::min(key.size(), r.claim.size()));
    return r;
}
struct CountLost {
    int* n;
    void operator()(rc::LeaseRef, rc::LostReason) const { ++*n; }
};
struct CountRisk {
    int* n;
    void operator()(rc::LeaseRef, rc::AtRiskEdge) const { ++*n; }
};

/// The application thread's view of position() is eventual (a drained status snapshot): poll it, capped.
bool awaitPosition(rc::Subscription& sub, int64_t want) {
    for (uint64_t spin = 0; spin < kSpinCap; ++spin) {
        if (sub.position() == want) return true;
        std::this_thread::yield();
    }
    return false;
}

void expectSequence(const std::vector<int64_t>& got, int64_t first, int64_t count) {
    ASSERT_EQ(static_cast<int64_t>(got.size()), count);
    for (int64_t i = 0; i < count; ++i)
        ASSERT_EQ(got[static_cast<std::size_t>(i)], first + i) << "at index " << i;
}

}  // namespace

// ---------------------------------------------------------------------------------------------------------
// 1. A handler that cannot take a record pauses; the consumer resumes it; nothing is lost, duplicated or reordered.
// ---------------------------------------------------------------------------------------------------------
TEST(SubscriptionPauseBrokerRegression, BackpressuredTailLosesNothingRegression) {
    constexpr int kRecords = 5000;
    OneBroker b("pause-tail", OneBroker::Runtime::Shard);
    const rc::TopicSpec spec = defineOne(*b.connectClient(), "tail");
    auto cp = openClientOn(b, {spec});
    rc::Client& c = *cp;
    const rc::TopicHandle th = c.topic("tail");
    ASSERT_EQ(produceAll(c, th, kRecords, rc::Acks::Leader), kRecords);

    // A LeaseSession (ttl 2 s) on the same client, driven from the consumer loop.
    {
        const auto r = c.defineTopicAndWait("claims", 1, "claim", claimBlob(), rc::Persistence::Disk, leaseTopicOptions(), {}, 60s);
        ASSERT_EQ(r.errorCode, 0) << r.detail;
    }
    auto lt = c.openLeaseTopicAndWait<LeaseClaim>("claims", {}, 30s);
    int lost = 0, risk = 0;
    rc::LeaseSession<4, CountLost, CountRisk> session(c, CountLost{&lost}, CountRisk{&risk});
    rc::LeaseOptions lo;
    lo.ttlMs = 2000;
    const LeaseClaim row = claimRow("tail-lease");
    const int64_t sentNs = c.nowNs();
    const rc::LeaseResult g = lt.grantAndWait(row, lo);
    ASSERT_TRUE(g.ok()) << int(g.status) << " " << g.errorCode;
    rc::OpResult err = rc::OpResult::Accepted;
    const rc::LeaseRef ref = session.add(lt, row, g.version, 2000, sentNs, &err);
    ASSERT_EQ(err, rc::OpResult::Accepted);

    TestRing ring;
    std::optional<rc::Subscription> sub;
    rc::SubscribeOptions so;
    so.from = rc::From::earliest();
    std::atomic<bool> armed{false};
    sub = c.subscribe(th, 0, so, [&](const rc::Record& r) {
        awaitArmed(armed);
        ring.offer(*sub, r);
    });
    armed.store(true, std::memory_order_release);

    std::vector<int64_t> got;
    got.reserve(kRecords);
    int resumes = 0;
    uint64_t spin = 0;
    for (; spin < kSpinCap && got.size() < static_cast<std::size_t>(kRecords); ++spin) {
        (void)ring.drainInto(got, 8);   // a consumer slower than the delivery: the 64-slot ring fills and the handler pauses
        // Resume only with the ring empty: pause() of the handler took effect on the duty thread; the consumer resumes it.
        if (ring.emptyForConsumer() && sub->paused() && sub->resume()) ++resumes;
        (void)session.doWork(c.nowNs());
        std::this_thread::yield();
    }
    ASSERT_EQ(got.size(), static_cast<std::size_t>(kRecords)) << "stalled after " << spin << " iterations, " << resumes << " resumes, "
                                                              << ring.pauses.load() << " pauses";
    // The sentinel: a duplicate delivered after the last expected record would arrive before it.
    ASSERT_EQ(produceAll(c, th, 1, rc::Acks::Leader, kRecords), 1);
    for (spin = 0; spin < kSpinCap && got.size() < static_cast<std::size_t>(kRecords) + 1; ++spin) {
        (void)ring.drainInto(got, 8);
        if (ring.emptyForConsumer() && sub->paused() && sub->resume()) ++resumes;
        (void)session.doWork(c.nowNs());
        std::this_thread::yield();
    }
    ASSERT_EQ(got.size(), static_cast<std::size_t>(kRecords) + 1) << "the sentinel never arrived (or a duplicate came before it)";
    ASSERT_NO_FATAL_FAILURE(expectSequence(got, 0, kRecords + 1));
    EXPECT_TRUE(awaitPosition(*sub, kRecords + 2)) << "position() " << sub->position();
    EXPECT_GE(ring.pauses.load(), 10) << "a 64-slot ring under a 5000-record tail pauses at least 10 times";
    EXPECT_GE(resumes, 10);
    EXPECT_EQ(lost, 0) << "the LeaseSession never lost a lease";
    EXPECT_EQ(risk, 0) << "the LeaseSession never put a lease at risk";
    EXPECT_EQ(session.stateOf(ref), rc::LeaseEntryState::Held);
    sub->unsubscribe();
    session.close();
    for (uint64_t i = 0; i < kSpinCap && !session.isClosed(); ++i) {
        (void)session.doWork(c.nowNs());
        std::this_thread::yield();
    }
    EXPECT_TRUE(session.isClosed());
}

// ---------------------------------------------------------------------------------------------------------
// 2. A subscription paused for the whole test does not touch another subscription of the same client.
// ---------------------------------------------------------------------------------------------------------
TEST(SubscriptionPauseBrokerRegression, SecondSubscriptionUnaffectedRegression) {
    constexpr int kX = 200, kY = 1000;
    OneBroker b("pause-second", OneBroker::Runtime::Shard);
    rc::Client* admin = b.connectClient();
    const rc::TopicSpec specX = defineOne(*admin, "x");
    const rc::TopicSpec specY = defineOne(*admin, "y");
    auto cp = openClientOn(b, {specX, specY});
    rc::Client& c = *cp;
    ASSERT_EQ(produceAll(c, c.topic("x"), kX, rc::Acks::Leader), kX);
    ASSERT_EQ(produceAll(c, c.topic("y"), kY, rc::Acks::Leader), kY);

    std::mutex mu;
    std::vector<int64_t> xs, ys;
    std::atomic<bool> xPaused{false};
    std::optional<rc::Subscription> sx, sy;
    rc::SubscribeOptions so;
    so.from = rc::From::earliest();
    std::atomic<bool> armed{false};
    sx = c.subscribe(c.topic("x"), 0, so, [&](const rc::Record& r) {
        awaitArmed(armed);
        {
            std::lock_guard<std::mutex> g(mu);
            xs.push_back(r.offset);
        }
        if (r.offset == 0) {   // X takes no record at all: it pauses on the first and stays paused
            (void)sx->pause(r.offset);
            xPaused.store(true, std::memory_order_release);
        }
    });
    sy = c.subscribe(c.topic("y"), 0, so, [&](const rc::Record& r) {
        std::lock_guard<std::mutex> g(mu);
        ys.push_back(r.offset);
    });

    armed.store(true, std::memory_order_release);
    uint64_t spin = 0;
    for (; spin < kSpinCap; ++spin) {
        {
            std::lock_guard<std::mutex> g(mu);
            if (ys.size() >= static_cast<std::size_t>(kY)) break;
        }
        std::this_thread::yield();
    }
    ASSERT_LT(spin, kSpinCap) << "Y stalled";
    ASSERT_TRUE(xPaused.load(std::memory_order_acquire));
    {
        std::lock_guard<std::mutex> g(mu);
        ASSERT_NO_FATAL_FAILURE(expectSequence(ys, 0, kY));
        // X: exactly the one record it refused, and nothing after it although 199 more are on the broker.
        EXPECT_EQ(xs, std::vector<int64_t>{0});
    }
    // Y keeps flowing for a fresh batch while X stays paused.
    ASSERT_EQ(produceAll(c, c.topic("y"), 50, rc::Acks::Leader, kY), 50);
    for (spin = 0; spin < kSpinCap; ++spin) {
        {
            std::lock_guard<std::mutex> g(mu);
            if (ys.size() >= static_cast<std::size_t>(kY + 50)) break;
        }
        std::this_thread::yield();
    }
    {
        std::lock_guard<std::mutex> g(mu);
        ASSERT_NO_FATAL_FAILURE(expectSequence(ys, 0, kY + 50));
        EXPECT_EQ(xs, std::vector<int64_t>{0}) << "X was paused for the whole test";
    }
    sx->unsubscribe();
    sy->unsubscribe();
}

// ---------------------------------------------------------------------------------------------------------
// 3. Three nodes, acks=all: the partition's leader changes while the subscription is paused.
// ---------------------------------------------------------------------------------------------------------
TEST(SubscriptionPauseBrokerRegression, LeaderChangeWhilePausedRegression) {
    constexpr int kRecords = 1000, kPauseAt = 500;
    ThreeNodes cluster = ThreeNodes::start("pause_leader");
    ASSERT_TRUE(cluster.awaitCatalogLeader(30s));
    auto admin = connectClient(cluster, cluster.runningNodes().front());
    const rc::TopicSpec spec = defineOne(*admin, "moving", 3);
    std::vector<int32_t> leaders;
    ASSERT_TRUE(awaitLeaders(cluster, spec.topicId, 1, leaders, 30s));
    const int32_t victim = leaders[0];
    int survivor = -1;
    for (int id : cluster.runningNodes())
        if (id != victim) { survivor = id; break; }
    ASSERT_GT(survivor, 0);

    // The producer and the subscriber both live on survivors.
    auto cfgOf = [&](int seed) {
        auto cfg = clientConfigFor(cluster, seed);
        cfg.topics = {spec};
        cfg.connectTimeoutMs = 30'000;
        cfg.pingTimeoutMs    = 30'000;
        return cfg;
    };
    auto client = connectClient(cluster, cfgOf(survivor));
    ASSERT_EQ(produceAll(*client, client->topic("moving"), kRecords, rc::Acks::All), kRecords);

    std::mutex mu;
    std::vector<int64_t> got;
    std::atomic<bool> paused{false};
    std::optional<rc::Subscription> sub;
    rc::SubscribeOptions so;
    so.from = rc::From::earliest();
    std::atomic<bool> armed{false};
    sub = client->subscribe(client->topic("moving"), 0, so, [&](const rc::Record& r) {
        awaitArmed(armed);
        if (r.offset == kPauseAt && !paused.load(std::memory_order_acquire)) {
            (void)sub->pause(r.offset);   // refuse 500
            paused.store(true, std::memory_order_release);
            return;
        }
        std::lock_guard<std::mutex> g(mu);
        got.push_back(r.offset);
    });
    armed.store(true, std::memory_order_release);
    uint64_t spin = 0;
    for (; spin < kSpinCap && !paused.load(std::memory_order_acquire); ++spin) std::this_thread::yield();
    ASSERT_TRUE(paused.load()) << "never reached offset " << kPauseAt;
    {
        std::lock_guard<std::mutex> g(mu);
        ASSERT_NO_FATAL_FAILURE(expectSequence(got, 0, kPauseAt));
    }

    cluster.stop(victim);
    ASSERT_TRUE(awaitLeaders(cluster, spec.topicId, 1, leaders, 60s));
    ASSERT_NE(leaders[0], victim);

    for (spin = 0; spin < kSpinCap && !sub->paused(); ++spin) std::this_thread::yield();
    // The move happened while paused, and this client followed it: a witness record (offset kRecords) is produced acks=all through
    // the new leader BEFORE resume().  The paused subscription holds no route, so it must not see it yet.
    ASSERT_EQ(produceAll(*client, client->topic("moving"), 1, rc::Acks::All, kRecords), 1) << "the client never reached the new leader";
    ASSERT_TRUE(sub->paused());
    EXPECT_EQ(client->testPendingUnsubscribes(), 0u) << "no release is owed: the pause's Unsubscribe was settled before the move";
    {
        std::lock_guard<std::mutex> g(mu);
        EXPECT_EQ(got.size(), static_cast<std::size_t>(kPauseAt)) << "nothing is delivered while paused";
    }
    ASSERT_TRUE(sub->resume());
    for (spin = 0; spin < kSpinCap; ++spin) {
        {
            std::lock_guard<std::mutex> g(mu);
            if (got.size() >= static_cast<std::size_t>(kRecords) + 1) break;
        }
        std::this_thread::yield();
    }
    {
        std::lock_guard<std::mutex> g(mu);
        ASSERT_EQ(got.size(), static_cast<std::size_t>(kRecords) + 1) << "stalled after the leader change";
    }
    // Sentinel (offset kRecords + 1): a duplicate of the resumed range would arrive before it.
    ASSERT_EQ(produceAll(*client, client->topic("moving"), 1, rc::Acks::All, kRecords + 1), 1);
    for (spin = 0; spin < kSpinCap; ++spin) {
        {
            std::lock_guard<std::mutex> g(mu);
            if (got.size() >= static_cast<std::size_t>(kRecords) + 2) break;
        }
        std::this_thread::yield();
    }
    {
        std::lock_guard<std::mutex> g(mu);
        ASSERT_EQ(got.size(), static_cast<std::size_t>(kRecords) + 2) << "sentinel missing or duplicates before it";
        ASSERT_NO_FATAL_FAILURE(expectSequence(got, 0, kRecords + 2));   // 0..499, then 500..1000 once each, then the sentinel
    }
    EXPECT_TRUE(awaitPosition(*sub, kRecords + 1)) << "position() " << sub->position();
    EXPECT_GE(ring.pauses.load(), 10) << "a 64-slot ring under a 5000-record tail pauses at least 10 times";
    EXPECT_GE(resumes, 10);
    EXPECT_EQ(lost, 0) << "the LeaseSession never lost a lease";
    EXPECT_EQ(risk, 0) << "the LeaseSession never put a lease at risk";
    EXPECT_EQ(session.stateOf(ref), rc::LeaseEntryState::Held);
    sub->unsubscribe();
    session.close();
    for (uint64_t i = 0; i < kSpinCap && !session.isClosed(); ++i) {
        (void)session.doWork(c.nowNs());
        std::this_thread::yield();
    }
    EXPECT_TRUE(session.isClosed());
}

// ---------------------------------------------------------------------------------------------------------
// 2. A subscription paused for the whole test does not touch another subscription of the same client.
// ---------------------------------------------------------------------------------------------------------
TEST(SubscriptionPauseBrokerRegression, SecondSubscriptionUnaffectedRegression) {
    constexpr int kX = 200, kY = 1000;
    OneBroker b("pause-second", OneBroker::Runtime::Shard);
    rc::Client* admin = b.connectClient();
    const rc::TopicSpec specX = defineOne(*admin, "x");
    const rc::TopicSpec specY = defineOne(*admin, "y");
    auto cp = openClientOn(b, {specX, specY});
    rc::Client& c = *cp;
    ASSERT_EQ(produceAll(c, c.topic("x"), kX, rc::Acks::Leader), kX);
    ASSERT_EQ(produceAll(c, c.topic("y"), kY, rc::Acks::Leader), kY);

    std::mutex mu;
    std::vector<int64_t> xs, ys;
    std::atomic<bool> xPaused{false};
    std::optional<rc::Subscription> sx, sy;
    rc::SubscribeOptions so;
    so.from = rc::From::earliest();
    std::atomic<bool> armed{false};
    sx = c.subscribe(c.topic("x"), 0, so, [&](const rc::Record& r) {
        awaitArmed(armed);
        {
            std::lock_guard<std::mutex> g(mu);
            xs.push_back(r.offset);
        }
        if (r.offset == 0) {   // X takes no record at all: it pauses on the first and stays paused
            (void)sx->pause(r.offset);
            xPaused.store(true, std::memory_order_release);
        }
    });
    sy = c.subscribe(c.topic("y"), 0, so, [&](const rc::Record& r) {
        std::lock_guard<std::mutex> g(mu);
        ys.push_back(r.offset);
    });

    armed.store(true, std::memory_order_release);
    uint64_t spin = 0;
    for (; spin < kSpinCap; ++spin) {
        {
            std::lock_guard<std::mutex> g(mu);
            if (ys.size() >= static_cast<std::size_t>(kY)) break;
        }
        std::this_thread::yield();
    }
    ASSERT_LT(spin, kSpinCap) << "Y stalled";
    ASSERT_TRUE(xPaused.load(std::memory_order_acquire));
    {
        std::lock_guard<std::mutex> g(mu);
        ASSERT_NO_FATAL_FAILURE(expectSequence(ys, 0, kY));
        // X: exactly the one record it refused, and nothing after it although 199 more are on the broker.
        EXPECT_EQ(xs, std::vector<int64_t>{0});
    }
    // Y keeps flowing for a fresh batch while X stays paused.
    ASSERT_EQ(produceAll(c, c.topic("y"), 50, rc::Acks::Leader, kY), 50);
    for (spin = 0; spin < kSpinCap; ++spin) {
        {
            std::lock_guard<std::mutex> g(mu);
            if (ys.size() >= static_cast<std::size_t>(kY + 50)) break;
        }
        std::this_thread::yield();
    }
    {
        std::lock_guard<std::mutex> g(mu);
        ASSERT_NO_FATAL_FAILURE(expectSequence(ys, 0, kY + 50));
        EXPECT_EQ(xs, std::vector<int64_t>{0}) << "X was paused for the whole test";
    }
    sx->unsubscribe();
    sy->unsubscribe();
}

// ---------------------------------------------------------------------------------------------------------
// 3. Three nodes, acks=all: the partition's leader changes while the subscription is paused.
// ---------------------------------------------------------------------------------------------------------
TEST(SubscriptionPauseBrokerRegression, LeaderChangeWhilePausedRegression) {
    constexpr int kRecords = 1000, kPauseAt = 500;
    ThreeNodes cluster = ThreeNodes::start("pause_leader");
    ASSERT_TRUE(cluster.awaitCatalogLeader(30s));
    auto admin = connectClient(cluster, cluster.runningNodes().front());
    const rc::TopicSpec spec = defineOne(*admin, "moving", 3);
    std::vector<int32_t> leaders;
    ASSERT_TRUE(awaitLeaders(cluster, spec.topicId, 1, leaders, 30s));
    const int32_t victim = leaders[0];
    int survivor = -1;
    for (int id : cluster.runningNodes())
        if (id != victim) { survivor = id; break; }
    ASSERT_GT(survivor, 0);

    // The producer and the subscriber both live on survivors.
    auto cfgOf = [&](int seed) {
        auto cfg = clientConfigFor(cluster, seed);
        cfg.topics = {spec};
        cfg.connectTimeoutMs = 30'000;
        cfg.pingTimeoutMs    = 30'000;
        return cfg;
    };
    auto client = connectClient(cluster, cfgOf(survivor));
    ASSERT_EQ(produceAll(*client, client->topic("moving"), kRecords, rc::Acks::All), kRecords);

    std::mutex mu;
    std::vector<int64_t> got;
    std::atomic<bool> paused{false};
    std::optional<rc::Subscription> sub;
    rc::SubscribeOptions so;
    so.from = rc::From::earliest();
    std::atomic<bool> armed{false};
    sub = client->subscribe(client->topic("moving"), 0, so, [&](const rc::Record& r) {
        awaitArmed(armed);
        if (r.offset == kPauseAt && !paused.load(std::memory_order_acquire)) {
            (void)sub->pause(r.offset);   // refuse 500
            paused.store(true, std::memory_order_release);
            return;
        }
        std::lock_guard<std::mutex> g(mu);
        got.push_back(r.offset);
    });
    armed.store(true, std::memory_order_release);
    uint64_t spin = 0;
    for (; spin < kSpinCap && !paused.load(std::memory_order_acquire); ++spin) std::this_thread::yield();
    ASSERT_TRUE(paused.load()) << "never reached offset " << kPauseAt;
    {
        std::lock_guard<std::mutex> g(mu);
        ASSERT_NO_FATAL_FAILURE(expectSequence(got, 0, kPauseAt));
    }

    cluster.stop(victim);
    ASSERT_TRUE(awaitLeaders(cluster, spec.topicId, 1, leaders, 60s));
    ASSERT_NE(leaders[0], victim);

    for (spin = 0; spin < kSpinCap && !sub->paused(); ++spin) std::this_thread::yield();
    ASSERT_TRUE(sub->resume());
    for (spin = 0; spin < kSpinCap; ++spin) {
        {
            std::lock_guard<std::mutex> g(mu);
            if (got.size() >= static_cast<std::size_t>(kRecords)) break;
        }
        std::this_thread::yield();
    }
    {
        std::lock_guard<std::mutex> g(mu);
        ASSERT_EQ(got.size(), static_cast<std::size_t>(kRecords)) << "stalled after the leader change";
    }
    // Sentinel (offset 1000): a duplicate of the resumed range would arrive before it.
    ASSERT_EQ(produceAll(*client, client->topic("moving"), 1, rc::Acks::All, kRecords), 1);
    for (spin = 0; spin < kSpinCap; ++spin) {
        {
            std::lock_guard<std::mutex> g(mu);
            if (got.size() >= static_cast<std::size_t>(kRecords) + 1) break;
        }
        std::this_thread::yield();
    }
    {
        std::lock_guard<std::mutex> g(mu);
        ASSERT_EQ(got.size(), static_cast<std::size_t>(kRecords) + 1) << "sentinel missing or duplicates before it";
        ASSERT_NO_FATAL_FAILURE(expectSequence(got, 0, kRecords + 1));   // 0..499, then 500..999 once each, then the sentinel
    }
    EXPECT_TRUE(awaitPosition(*sub, kRecords + 1)) << "position() " << sub->position();
    sub->unsubscribe();
    client->close();
    admin->close();
}

// ---------------------------------------------------------------------------------------------------------
// 4. read_committed: a pause in the middle of a committed transaction's batch; the control marker is skipped as usual.
// ---------------------------------------------------------------------------------------------------------
TEST(SubscriptionPauseBrokerRegression, ReadCommittedPauseInsideTransactionRegression) {
    constexpr int kBatch = 10, kSecond = 3, kPauseAt = 4;
    OneBroker b("pause-txn", OneBroker::Runtime::Shard);
    const rc::TopicSpec spec = defineOne(*b.connectClient(), "txn");
    auto cp = openClientOn(b, {spec});
    rc::Client& c = *cp;
    const rc::TopicHandle th = c.topic("txn");

    {
        rc::ProducerConfig pc;
        pc.acks          = rc::Acks::All;
        pc.lingerUs      = 1;
        pc.transactionalId = "pause-txn";
        rc::Producer p = c.producer(pc);
        p.initTransactionsAndWait(30s);
        // Transaction 1: one batch of kBatch records (offsets 0..9), marker at 10.
        p.beginTransaction();
        rc::BatchBuilder bb = p.batch(th, rc::Key(1));
        for (int i = 0; i < kBatch; ++i) {
            const std::string v = "t1-" + std::to_string(i);
            bb.add(std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(v.data()), v.size()));
        }
        ASSERT_EQ(bb.send(), rc::OpResult::Accepted);
        ASSERT_TRUE(p.commitTransactionAndWait(30s).ok);
        // Transaction 2: kSecond records (offsets 11..13), marker at 14.
        p.beginTransaction();
        for (int i = 0; i < kSecond; ++i) {
            const std::string v = "t2-" + std::to_string(i);
            p.send(th, rc::Key(2), std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(v.data()), v.size()));
        }
        ASSERT_TRUE(p.commitTransactionAndWait(30s).ok);
        p.close();
    }

    std::mutex mu;
    std::vector<int64_t> got;
    std::atomic<bool> paused{false};
    std::optional<rc::Subscription> sub;
    rc::SubscribeOptions so;
    so.from      = rc::From::earliest();
    so.isolation = rc::Isolation::ReadCommitted;
    std::atomic<bool> armed{false};
    sub = c.subscribe(th, 0, so, [&](const rc::Record& r) {
        awaitArmed(armed);
        if (r.offset == kPauseAt && !paused.load(std::memory_order_acquire)) {   // inside the batch: refuse record 4
            (void)sub->pause(r.offset);
            paused.store(true, std::memory_order_release);
            return;
        }
        std::lock_guard<std::mutex> g(mu);
        got.push_back(r.offset);
    });
    armed.store(true, std::memory_order_release);
    uint64_t spin = 0;
    for (; spin < kSpinCap && !paused.load(std::memory_order_acquire); ++spin) std::this_thread::yield();
    ASSERT_TRUE(paused.load()) << "the pause never happened";
    for (spin = 0; spin < kSpinCap && !sub->paused(); ++spin) std::this_thread::yield();
    ASSERT_TRUE(sub->resume());

    const std::size_t want = kBatch + kSecond;
    for (spin = 0; spin < kSpinCap; ++spin) {
        {
            std::lock_guard<std::mutex> g(mu);
            if (got.size() >= want) break;
        }
        std::this_thread::yield();
    }
    // Sentinel: a plain (non-transactional) record at offset 15, after the second marker at 14.
    constexpr int kSentinel = kBatch + 1 + kSecond + 1;
    ASSERT_EQ(produceAll(c, th, 1, rc::Acks::All, 0), 1);
    for (spin = 0; spin < kSpinCap; ++spin) {
        {
            std::lock_guard<std::mutex> g(mu);
            if (got.size() >= want + 1) break;
        }
        std::this_thread::yield();
    }
    {
        std::lock_guard<std::mutex> g(mu);
        // Each record once; the markers at offsets 10 and 14 are never delivered; the sentinel closes the sequence.
        std::vector<int64_t> expect;
        for (int i = 0; i < kBatch; ++i) expect.push_back(i);
        for (int i = 0; i < kSecond; ++i) expect.push_back(kBatch + 1 + i);
        expect.push_back(kSentinel);
        EXPECT_EQ(got, expect);
    }
    EXPECT_TRUE(awaitPosition(*sub, kSentinel + 1)) << "position() " << sub->position();
    sub->unsubscribe();
}
