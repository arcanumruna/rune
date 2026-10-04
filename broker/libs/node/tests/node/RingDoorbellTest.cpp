// RingDoorbellTest.cpp — A9: the O2O ring doorbell (detail/RingDoorbell.h) never loses a wakeup.
//
// Consumer wake (Dekker): the consumer's {markParking, anyWatchReady} and the producer's {publish, notify}
// are interleaved in all 6 orders through the test seams (commitWithoutNotifyForTest / notifyConsumerForTest,
// markParking / anyWatchReady); in every order the consumer either sees the record (does not park) or finds
// its doorbell rung.  The same for the producer-side space wait.  Stress tests race real threads and never
// sleep: a consumer that parks on the doorbell with a long poll timeout would time out on a lost wakeup.
//
// Review L-5: the sequential interleaving tests cannot see a weakened fence.  The ping-pong stress catches a
// fence weakened to release/acquire on x86-64 (TSO: store->load reordering) — that is the platform that must run
// it (the Linux VM suite); on arm64 macOS clang lowers a release fence to dmb ish and only a REMOVED fence fails.
#include <rune/node/OneToOneByteRing.h>
#include <rune/node/ParkUntilWorkIdle.h>
#include <rune/node/detail/RingDoorbell.h>
#include <rune/consensus/ConsensusAgent.h>   // Agent (before AgentRunner.h)
#include <rune/consensus/AgentRunner.h>
#include <rune/consensus/NanoClock.h>

#include <gtest/gtest.h>

#include <poll.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <string>
#include <thread>
#include <vector>

using rune::node::OneToOneByteRing;
using rune::node::ParkUntilWorkIdle;
using rune::node::IdleShape;
using rune::node::detail::RingDoorbell;

namespace {

int drainCount(OneToOneByteRing& r) {
    return r.drain([](int32_t, aeron::concurrent::AtomicBuffer&, aeron::util::index_t, aeron::util::index_t) {},
                   1 << 20);
}

// One producer step / one consumer step of the wake protocol.
enum class Step { Publish, Notify, Mark, Check };

std::string name(const std::array<Step, 4>& o) {
    std::string s;
    for (Step x : o) s += x == Step::Publish ? "P" : x == Step::Notify ? "N" : x == Step::Mark ? "M" : "C";
    return s;
}

} // namespace

// Every interleaving of producer {publish, notify} with consumer {mark parking, check rings}: no lost wakeup.
TEST(RingDoorbellRegression, NoLostWakeupInAnyInterleavingRegression) {
    std::vector<std::array<Step, 4>> orders;
    std::array<Step, 4> o{Step::Publish, Step::Notify, Step::Mark, Step::Check};
    std::sort(o.begin(), o.end());
    do {
        const auto pos = [&](Step s) { return std::find(o.begin(), o.end(), s) - o.begin(); };
        if (pos(Step::Publish) < pos(Step::Notify) && pos(Step::Mark) < pos(Step::Check)) orders.push_back(o);
    } while (std::next_permutation(o.begin(), o.end()));
    ASSERT_EQ(orders.size(), 6u);

    for (const auto& order : orders) {
        SCOPED_TRACE(name(order));
        RingDoorbell bell;
        ASSERT_TRUE(bell.valid());
        OneToOneByteRing ring(4096);
        ASSERT_TRUE(ring.attachConsumerDoorbell(bell));
        const uint8_t payload[16] = {};
        aeron::util::index_t claim = -1;
        bool checked = false, ready = false;
        for (Step s : order) {
            switch (s) {
            case Step::Publish:
                claim = ring.tryClaim(1, sizeof payload);
                ASSERT_GE(claim, 0);
                std::memcpy(ring.base() + claim, payload, sizeof payload);
                ring.commitWithoutNotifyForTest(claim);
                break;
            case Step::Notify: ring.notifyConsumerForTest(); break;
            case Step::Mark:   bell.markParking(); break;
            case Step::Check:  ready = bell.anyWatchReady(); checked = true; break;
            }
        }
        ASSERT_TRUE(checked);
        const bool parks = !ready;
        EXPECT_TRUE(!parks || bell.firedForTest())
            << "the consumer parks with a committed record and no doorbell: LOST WAKEUP";
        bell.disarm(bell.firedForTest());
        EXPECT_FALSE(bell.firedForTest()) << "disarm drains the doorbell";
        EXPECT_EQ(drainCount(ring), 1);
    }
}

// The classic window: the consumer's last read found nothing, the producer committed before the consumer
// armed (no one parking: no ring) — the arm must see the record and refuse to park.
TEST(RingDoorbellRegression, CommitBetweenLastReadAndArmIsNotLostRegression) {
    RingDoorbell bell;
    OneToOneByteRing ring(4096);
    ASSERT_TRUE(ring.attachConsumerDoorbell(bell));
    EXPECT_EQ(drainCount(ring), 0);                     // the pass: empty
    const uint8_t b[8] = {1};
    ASSERT_TRUE(ring.write(b, sizeof b, 1));            // commit: nobody parking → no syscall, no ring
    EXPECT_FALSE(bell.firedForTest());
    EXPECT_FALSE(bell.armForPark()) << "a committed record waits: the consumer must not park";
    EXPECT_FALSE(bell.parkingForTest()) << "a refused arm leaves the parking word clear";
    EXPECT_EQ(drainCount(ring), 1);
    EXPECT_TRUE(bell.armForPark()) << "empty again: park";
    ASSERT_TRUE(ring.write(b, sizeof b, 1));
    EXPECT_TRUE(bell.firedForTest()) << "a commit while parking rings the doorbell";
    bell.disarm(true);
}

// A claimed but not yet committed record is not work (a late commit must not keep the consumer awake) —
// the commit rings the parked consumer.
TEST(RingDoorbell, UncommittedClaimParksAndTheCommitRings) {
    RingDoorbell bell;
    OneToOneByteRing ring(4096);
    ASSERT_TRUE(ring.attachConsumerDoorbell(bell));
    const auto claim = ring.tryClaim(1, 8);
    ASSERT_GE(claim, 0);
    EXPECT_TRUE(bell.armForPark());
    EXPECT_FALSE(bell.firedForTest());
    ring.commit(claim);
    EXPECT_TRUE(bell.firedForTest());
    bell.disarm(true);
    EXPECT_EQ(drainCount(ring), 1);
}

// A gated-off watch (a ring its consumer deliberately leaves unconsumed) does not keep the consumer awake, but
// a new commit still rings it.
TEST(RingDoorbell, GatedWatchIsIgnoredButStillRings) {
    RingDoorbell bell;
    OneToOneByteRing ring(4096);
    bool enabled = true;
    ASSERT_TRUE(ring.attachConsumerDoorbell(bell, &enabled));
    const uint8_t b[8] = {};
    ASSERT_TRUE(ring.write(b, sizeof b, 1));
    EXPECT_FALSE(bell.armForPark());
    enabled = false;
    EXPECT_TRUE(bell.armForPark()) << "a gated ring holding records must not prevent the park";
    ASSERT_TRUE(ring.write(b, sizeof b, 1));
    EXPECT_TRUE(bell.firedForTest());
    bell.disarm(true);
}

// One doorbell, two rings: several commits during one park write the fd once (exchange dedupe), and the
// padding record at a wrap counts as work (the next read consumes it).
TEST(RingDoorbell, TwoRingsOneParkOneWriteAndWrapPaddingIsWork) {
    RingDoorbell bell;
    OneToOneByteRing a(4096), b(4096);
    ASSERT_TRUE(a.attachConsumerDoorbell(bell));
    ASSERT_TRUE(b.attachConsumerDoorbell(bell));
    EXPECT_EQ(bell.watchCount(), 2);
    const uint8_t x[8] = {};
    EXPECT_TRUE(bell.armForPark());
    ASSERT_TRUE(a.write(x, sizeof x, 1));
    EXPECT_FALSE(bell.parkingForTest()) << "the first commit took the parking word";
    ASSERT_TRUE(b.write(x, sizeof x, 1));
    EXPECT_TRUE(bell.firedForTest());
    bell.disarm(true);
    EXPECT_FALSE(bell.firedForTest());
    EXPECT_EQ(drainCount(a) + drainCount(b), 2);

    // Wrap: 4096-byte ring, 400-byte records (408 framed) — the 11th no longer fits before the end: padding.
    OneToOneByteRing w(4096);
    RingDoorbell wb;
    ASSERT_TRUE(w.attachConsumerDoorbell(wb));
    std::vector<uint8_t> rec(400);
    for (int i = 0; i < 9; ++i) ASSERT_TRUE(w.write(rec.data(), rec.size(), 1));
    EXPECT_EQ(drainCount(w), 9);
    for (int i = 0; i < 2; ++i) ASSERT_TRUE(w.write(rec.data(), rec.size(), 1));   // the 2nd wraps: padding
    int got = 0;
    for (int pass = 0; pass < 4 && got < 2; ++pass) {
        got += drainCount(w);
        if (got < 2) EXPECT_FALSE(wb.armForPark()) << "padding / a record at the head is work";
    }
    EXPECT_EQ(got, 2);
    EXPECT_TRUE(wb.armForPark());
}

// Space wait, both orders that matter: (a) the producer arms and finds no space → the consumer's read rings
// it; (b) the consumer freed space before the arm → the producer's re-check sees the space (no park).
TEST(RingDoorbellRegression, SpaceWaitNeverLosesTheFreedSpaceRegression) {
    for (const bool consumerFirst : {false, true}) {
        SCOPED_TRACE(consumerFirst ? "consumer freed space before the arm" : "arm before the consumer read");
        RingDoorbell producerBell;
        OneToOneByteRing ring(1024);
        ring.attachProducerDoorbell(producerBell);
        std::vector<uint8_t> rec(100);
        while (ring.write(rec.data(), rec.size(), 1)) {}
        ASSERT_FALSE(ring.hasSpaceForMsg(static_cast<int64_t>(rec.size())));
        ASSERT_TRUE(ring.spaceWaitArmedForTest()) << "a refused claim with a producer doorbell arms the wait (M-2)";
        ring.disarmSpaceWait();   // this producer stops waiting: start each order from a clean word
        if (consumerFirst) {
            EXPECT_GT(drainCount(ring), 0);
            EXPECT_FALSE(producerBell.firedForTest()) << "nobody waited: no ring";
            ring.armSpaceWait();
            EXPECT_TRUE(ring.hasSpaceForMsg(static_cast<int64_t>(rec.size()))) << "the re-check sees the space";
            ring.disarmSpaceWait();
        } else {
            ring.armSpaceWait();
            ASSERT_FALSE(ring.hasSpaceForMsg(static_cast<int64_t>(rec.size()))) << "still full: park";
            EXPECT_GT(drainCount(ring), 0);
            EXPECT_TRUE(producerBell.firedForTest()) << "freed space must ring the waiting producer";
            EXPECT_FALSE(ring.spaceWaitArmedForTest()) << "one ring per wait";
            producerBell.disarm(true);
            (void)drainCount(ring);
            EXPECT_FALSE(producerBell.firedForTest()) << "a drain with no waiter does not ring";
        }
    }
}

// Stress, never sleeps: two producer threads (two rings) race one consumer that parks on the shared doorbell
// whenever both rings look empty.  Each producer waits (yielding) until its message was consumed before the
// next one, so EVERY message is a "last" message: one whose commit lands in the consumer's drain → arm
// window and is not seen would leave the consumer in poll() — its 5 s timeout reports the lost wakeup.
TEST(RingDoorbellRegression, StressTwoProducersOneParkingConsumerRegression) {
    constexpr int kPerProducer = 50'000;
    RingDoorbell bell;
    OneToOneByteRing r0(1 << 16), r1(1 << 16);
    ASSERT_TRUE(r0.attachConsumerDoorbell(bell));
    ASSERT_TRUE(r1.attachConsumerDoorbell(bell));
    std::atomic<int64_t> consumed[2] = {0, 0};
    std::atomic<bool> stop{false};
    auto produce = [&](OneToOneByteRing& r, int k) {
        for (int64_t i = 0; i < kPerProducer && !stop.load(); ++i) {
            ASSERT_TRUE(r.write(reinterpret_cast<const uint8_t*>(&i), sizeof i, 1));
            while (consumed[k].load() <= i && !stop.load()) std::this_thread::yield();
        }
    };
    std::thread p0(produce, std::ref(r0), 0), p1(produce, std::ref(r1), 1);
    int64_t next[2] = {0, 0};
    int parks = 0, lost = 0;
    bool ordered = true;
    while ((next[0] < kPerProducer || next[1] < kPerProducer) && lost == 0) {
        int n = 0;
        for (int k = 0; k < 2; ++k) {
            n += (k == 0 ? r0 : r1).drain(
                [&](int32_t, aeron::concurrent::AtomicBuffer& buf, aeron::util::index_t off, aeron::util::index_t) {
                    int64_t v;
                    std::memcpy(&v, buf.buffer() + off, sizeof v);
                    ordered = ordered && v == next[k];
                    ++next[k];
                    consumed[k].store(next[k]);
                },
                1 << 20);
        }
        if (n > 0) continue;
        if (!bell.armForPark()) continue;
        ++parks;
        pollfd pf{bell.fd(), POLLIN, 0};
        const int r = ::poll(&pf, 1, 5'000);
        if (r == 0) ++lost;
        bell.disarm(r > 0);
    }
    stop.store(true);
    p0.join();
    p1.join();
    EXPECT_EQ(lost, 0) << "a parked consumer missed a commit (lost wakeup)";
    EXPECT_TRUE(ordered);
    EXPECT_EQ(next[0], kPerProducer);
    EXPECT_EQ(next[1], kPerProducer);
    EXPECT_GT(parks, 0) << "the stress must actually exercise parks";
}

// Stress, never sleeps: a tiny ring, both sides park — the producer on its doorbell when the ring is full
// (space wait), the consumer on its doorbell when it is empty.  Either lost wakeup times a 5 s poll out.
TEST(RingDoorbellRegression, StressBothSidesParkRegression) {
    constexpr int kN = 300'000;
    RingDoorbell consumerBell, producerBell;
    OneToOneByteRing ring(1024);
    ASSERT_TRUE(ring.attachConsumerDoorbell(consumerBell));
    ring.attachProducerDoorbell(producerBell);
    std::atomic<int> producerLost{0};
    std::atomic<int> producerParks{0};
    std::thread producer([&] {
        for (int i = 0; i < kN && producerLost.load() == 0;) {
            int64_t v = i;
            if (ring.write(reinterpret_cast<const uint8_t*>(&v), sizeof v, 1)) { ++i; continue; }
            ring.armSpaceWait();
            // Re-check = the claim itself (it reads the consumer's head after the fence).
            if (ring.write(reinterpret_cast<const uint8_t*>(&v), sizeof v, 1)) { ring.disarmSpaceWait(); ++i; continue; }
            producerParks.fetch_add(1);
            pollfd pf{producerBell.fd(), POLLIN, 0};
            const int r = ::poll(&pf, 1, 5'000);
            if (r == 0) producerLost.fetch_add(1);
            producerBell.disarm(r > 0);
        }
    });
    int64_t next = 0;
    int consumerLost = 0, consumerParks = 0;
    bool ordered = true;
    while (next < kN && consumerLost == 0 && producerLost.load() == 0) {
        const int n = ring.drain(
            [&](int32_t, aeron::concurrent::AtomicBuffer& buf, aeron::util::index_t off, aeron::util::index_t) {
                int64_t v;
                std::memcpy(&v, buf.buffer() + off, sizeof v);
                ordered = ordered && v == next;
                ++next;
            },
            7);   // small batches: the producer keeps hitting a full ring
        if (n > 0) continue;
        if (!consumerBell.armForPark()) continue;
        ++consumerParks;
        pollfd pf{consumerBell.fd(), POLLIN, 0};
        const int r = ::poll(&pf, 1, 5'000);
        if (r == 0) ++consumerLost;
        consumerBell.disarm(r > 0);
    }
    producer.join();
    EXPECT_EQ(consumerLost, 0) << "consumer lost a wakeup";
    EXPECT_EQ(producerLost.load(), 0) << "producer lost a space wakeup";
    EXPECT_TRUE(ordered);
    EXPECT_EQ(next, kN);
    EXPECT_GT(producerParks.load() + consumerParks, 0);
}

// ParkUntilWorkIdle + AgentRunner: a runner parked on a 2 s park takes a cross-thread ring message at once
// (the doorbell ends the park) — without the doorbell it sleeps the park out.
namespace {
struct RingAgent final : rune::consensus::Agent {
    OneToOneByteRing& ring;
    std::atomic<int64_t> takenNs{0};
    std::atomic<uint64_t> passes{0};
    explicit RingAgent(OneToOneByteRing& r) : ring(r) {}
    const char* name() const noexcept override { return "ring-agent"; }
    static int64_t steadyNs() {
        return std::chrono::duration_cast<std::chrono::nanoseconds>(
                   std::chrono::steady_clock::now().time_since_epoch()).count();
    }
    int doWork(int64_t) override {
        passes.fetch_add(1);
        const int n = drainCount(ring);
        if (n > 0 && takenNs.load() == 0) takenNs.store(steadyNs());
        return n;
    }
};
template <class Pred>
bool waitFor(std::chrono::milliseconds limit, Pred&& pred) {
    const auto deadline = std::chrono::steady_clock::now() + limit;
    while (std::chrono::steady_clock::now() < deadline) {
        if (pred()) return true;
        std::this_thread::yield();
    }
    return pred();
}
int64_t parkedRunnerTakesRingMessageNs(bool withDoorbell) {
    RingDoorbell bell;
    OneToOneByteRing ring(4096);
    if (withDoorbell) EXPECT_TRUE(ring.attachConsumerDoorbell(bell));
    RingAgent agent(ring);
    // A readiness fd that never fires (the read end of an idle pipe) stands in for the driver fds.
    int quiet[2];
    EXPECT_EQ(::pipe(quiet), 0);
    ParkUntilWorkIdle idle(IdleShape{0, 0, 2'000'000'000, 2'000'000'000}, &quiet[0], 1,
                           withDoorbell ? &bell : nullptr);
    rune::consensus::MonotonicNanoClock clk;
    rune::consensus::AgentRunner runner(agent, idle, clk);
    runner.start();
    // Parked: two passes happened (the first ran, the runner is in its 2 s park after the second at most).
    EXPECT_TRUE(waitFor(std::chrono::seconds(5), [&] { return agent.passes.load() >= 1; }));
    for (int i = 0; i < 1000; ++i) std::this_thread::yield();
    const int64_t posted = RingAgent::steadyNs();
    const uint8_t b[8] = {};
    EXPECT_TRUE(ring.write(b, sizeof b, 1));
    EXPECT_TRUE(waitFor(std::chrono::seconds(5), [&] { return agent.takenNs.load() != 0; }));
    const int64_t latency = agent.takenNs.load() - posted;
    runner.stop();
    bell.ring();                       // end the current park so the join does not wait it out
    (void)::write(quiet[1], "x", 1);   // (the no-doorbell arm parks on the pipe)
    runner.join();
    ::close(quiet[0]);
    ::close(quiet[1]);
    return latency;
}
} // namespace

TEST(RingDoorbellRegression, ParkedRunnerTakesRingMessageAtOnceRegression) {
    const int64_t ns = parkedRunnerTakesRingMessageNs(/*withDoorbell=*/true);
    EXPECT_LT(ns, 500'000'000) << "the parked runner slept out its 2 s park: the doorbell did not end it";
}

// Review M-2: a producer whose claim is refused arms the space wait inside tryClaim (and re-claims once), so a
// producer retry loop with no explicit arm still wakes on the consumer's freed space.  Without a producer
// doorbell a refusal arms nothing.
TEST(RingDoorbellRegression, RefusedClaimArmsTheSpaceWaitItselfRegression) {
    RingDoorbell producerBell;
    OneToOneByteRing ring(1024);
    ring.attachProducerDoorbell(producerBell);
    std::vector<uint8_t> rec(100);
    int n = 0;
    while (ring.write(rec.data(), rec.size(), 1)) ++n;
    ASSERT_GT(n, 0);
    EXPECT_TRUE(ring.spaceWaitArmedForTest());
    EXPECT_FALSE(producerBell.firedForTest());
    EXPECT_GT(drainCount(ring), 0);
    EXPECT_TRUE(producerBell.firedForTest()) << "the consumer's freed space must wake the refused producer";
    producerBell.disarm(true);
    EXPECT_TRUE(ring.write(rec.data(), rec.size(), 1));
    EXPECT_FALSE(ring.spaceWaitArmedForTest()) << "a successful claim leaves no wait armed";

    OneToOneByteRing plain(1024);
    while (plain.write(rec.data(), rec.size(), 1)) {}
    EXPECT_FALSE(plain.spaceWaitArmedForTest()) << "no producer doorbell: nothing armed, no fence";
}
