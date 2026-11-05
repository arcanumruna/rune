// CheckedWritesAllocRegressionTest.cpp - checked writes Part 03 (hard rule): the write hook and the View acks allocate
// nothing in steady state - the hook's check and stage, the View waiter queue and its fold/HW/deadline release.  Replaces
// the global operator new (binary-wide) through AllocWindow.cpp, so it lives in its own binary.
#include "support/AllocWindow.h"
#include "support/CheckedWritesAckRig.h"
#include "support/CheckedWritesRig.h"

#include <gtest/gtest.h>

#include <array>

using namespace rune::node;
using namespace rune::node::cw;

namespace {
constexpr int16_t kLevel = 5;
constexpr uint8_t kAll = 1, kAllView = 3;
constexpr uint8_t kAcksView = rune::protocol::client::kAcksWireView;
constexpr int32_t kView = 0x40000001;
}  // namespace

// The test thread drives the worker, so the window brackets exactly one batch's enqueue, doWork and drainReturn; the rig's
// own bookkeeping (the ack maps, the egress poll) runs between windows.  The records reuse 16 keys: a view's row table
// growing for new keys is the fold's, not the hook's or the ack's.
TEST(CheckedWritesAllocRegression, SteadyStateHookAndViewAcksAllocateNothingRegression) {
    AckRigOptions o;
    o.replicas     = {1, 2};
    o.minIsr       = 2;
    o.ackTimeoutNs = 5'000'000;
    AckRig r(o);
    r.typed();
    r.setLevel(kLevel);
    r.installView(kView);

    int64_t corr = 0;
    int32_t seq = 0;
    const auto write = [&](int records, uint8_t acks, uint8_t attr) {
        AckRig::Write w;
        w.corr = ++corr;
        w.acks = acks;
        w.pid  = 11;
        w.seq  = seq;
        for (int i = 0; i < records; ++i) w.records.push_back(validRecord(static_cast<uint64_t>(i)));
        w.attributes.assign(static_cast<std::size_t>(records), attr);
        return w;
    };
    const auto drive = [&](const AckRig::Write& w, bool release, int rounds) {
        const auto batchBytes = AckRig::batchFor(w);
        uint64_t allocs = 0;
        {
            rune::testing::AllocWindow win;
            WorkerOp op;
            op.type = WorkerOp::Type::Produce; op.topicId = AckRig::kTopicId; op.partition = 0;
            op.replica = r.replica.get(); op.logPtr = r.log.get();
            op.sessionId = AckRig::kSession; op.correlationId = w.corr; op.acks = w.acks;
            op.producerId = w.pid; op.producerEpoch = 0; op.baseSequence = w.seq;
            op.recordCount = static_cast<int32_t>(w.records.size()); op.lastOffsetDelta = op.recordCount - 1; op.durabilityTier = 1;
            op.deferAck = true;
            op.recordBatchView = std::span<const uint8_t>(batchBytes);
            if (!r.worker->tryEnqueueOp(std::move(op))) ADD_FAILURE() << "op ring full";
            for (int i = 0; i < rounds; ++i) {
                r.worker->doWork(r.now += 10'000);
                if (release && i == 1)
                    (void)r.replica->onFetchRequest(rune::replication::FetchRequestMsg{r.replica->epoch(), 0, 2, r.leo(), 1 << 20, AckRig::kTopicId}, r.now);
            }
            r.worker->drainReturn([](WorkerReturn&&) {});
            allocs = win.count();
        }
        r.steps(2);   // the egress poll and the rig's bookkeeping, outside the window
        return allocs;
    };
    const auto mix = [&](int n, uint64_t& allocs) {
        for (int i = 0; i < n; ++i) {
            const int records = i % 2 == 0 ? 1 : 16;
            const uint8_t acks = i % 3 == 0 ? kAllView : kAll;
            const auto w = write(records, acks, 0);
            allocs += drive(w, /*release=*/true, 6);
            seq += records;
        }
    };

    uint64_t warm = 0;
    mix(200, warm);
    ASSERT_EQ(r.acks.size(), 200u) << "every warm-up batch was acked";

    uint64_t allocs = 0;
    mix(2000, allocs);
    for (int i = 0; i < 100; ++i) {   // hook refusals: an insert intent on a topic without a key index
        const auto w = write(1, kAllView, 1);
        allocs += drive(w, false, 3);
    }
    for (int i = 0; i < 50; ++i) {    // View deadline expiries: minIsr 2 and no follower fetch
        const auto w = write(1, kAllView, 0);
        allocs += drive(w, false, 600);
        seq += 1;
    }
    EXPECT_EQ(allocs, 0u);
    EXPECT_EQ(r.worker->hookRefusalsForTest(HookRefusal::NoKeyIndex), 100u);
}

// Part 04: judging, marker emission, the ordinary fold, the cut Raw / Filtered / PushRead reads and a v17 View-ack writer
// allocate nothing in steady state.  The window brackets one worker pass (op enqueue + doWork + drainReturn) per batch; the
// batch bytes, the client frames, the view channel drain and the subscribers' polling are the rig's.
TEST(CheckedWritesAllocRegression, SteadyStateJudgingAndMarkersAllocateNothingRegression) {
    VerdictRig r;
    r.typed();
    TestRejectRule rule;
    rule.mode = TestRejectRule::Mode::TagEquals;
    rule.tag = 9;
    r.installRejectView(0x40000601, 7, rule);
    r.installProjection(0x40000602);
    r.enableAcks();
    r.passes(60);
    const int64_t raw = r.subscribe(SubKind::Raw);
    const int64_t filtered = r.subscribe(SubKind::Filtered);
    r.passes(60);

    int64_t corr = 1000, pushNext = 0;
    const auto recordsOf = [&](int records, int round) {
        std::vector<Rec> recs;
        for (int i = 0; i < records; ++i) recs.push_back({static_cast<uint64_t>(i % 16 + 1), round, static_cast<uint8_t>((round + i) % 10 == 0 ? 9 : 1)});
        return recs;
    };
    const auto drive = [&](int round, int records) -> uint64_t {
        const auto bytes = VerdictRig::batch(recordsOf(records, round));
        if (round % 4 == 0) (void)r.produceFrame(recordsOf(2, round + 1), kAcksView);   // a v17 View-ack writer, offered by the client side
        uint64_t allocs = 0;
        {
            rune::testing::AllocWindow win;
            WorkerOp op;
            op.type = WorkerOp::Type::Produce; op.topicId = VerdictRig::kSrc; op.partition = 0;
            op.replica = r.replica.get(); op.logPtr = r.log.get(); op.sessionId = 1;
            op.correlationId = ++corr; op.durabilityTier = 1;
            op.recordBatchView = std::span<const uint8_t>(bytes);
            if (!r.worker->tryEnqueueOp(std::move(op))) ADD_FAILURE() << "op ring full";
            WorkerOp read;
            read.type = WorkerOp::Type::PushRead; read.topicId = VerdictRig::kSrc; read.partition = 0;
            read.replica = r.replica.get(); read.logPtr = r.log.get();
            read.subscriptionId = 777; read.sessionId = 4999; read.nextOffset = pushNext; read.hwBound = INT64_MAX;
            if (!r.worker->tryEnqueueOp(std::move(read))) ADD_FAILURE() << "op ring full";
            r.now += 1'000'000;
            r.worker->doWork(r.now);
            r.worker->drainReturnView([&](WorkerReturn&& ret) {
                if (ret.type == WorkerReturn::Type::PushReadResult && ret.errorCode == 0) pushNext = ret.newNextOffset;
            });
            allocs = win.count();
        }
        r.passes(1);   // the view agent drain, the ack and egress polling are the rig's
        if (round % 50 == 0) (void)r.collect(raw);   // polls the shared egress image (the other subscription's frames are dropped)
        return allocs;
    };
    for (int i = 0; i < 300; ++i) (void)drive(i, i % 2 == 0 ? 1 : 16);
    r.passes(300);   // markers are emitted on the interval
    const uint64_t markersBefore = r.stats()->markersWritten;
    const uint64_t rejectedBefore = r.stats()->rejectionsConstraint;
    uint64_t worst = 0, total = 0;
    for (int i = 300; i < 3300; ++i) {
        const uint64_t a = drive(i, i % 2 == 0 ? 1 : 16);
        total += a;
        worst = std::max(worst, a);
    }
    EXPECT_GT(r.stats()->markersWritten - markersBefore, 5u) << "markers were emitted in the window";
    EXPECT_GT(r.stats()->rejectionsConstraint - rejectedBefore, 100u);
    EXPECT_GT(pushNext, 0) << "the PushRead was answered";
    EXPECT_GT(r.ackOrder().size(), 500u) << "the View-ack writer was answered";
    EXPECT_GT(r.worker->subscriptionPositionForTest(raw), 1000) << "the Raw subscription was served";
    EXPECT_GT(r.worker->subscriptionPositionForTest(filtered), 1000) << "the Filtered subscription was served";
    EXPECT_EQ(total, 0u) << "worst batch allocated " << worst;
}

// Part 05: judging intents against the key index allocates nothing in steady state - the prepare / evaluate / apply of
// every intent, the 56 / 57 rejections and their View-ack verdicts, the markers, the projection's fold and the Raw delivery.
// The warm-up grows the key index (4096 slots at start) and the projection past 10,000 ids; the measured batches reuse them.
// Each window brackets the worker's passes of one batch (doWork + drainReturnView); the frame encode and offer, the ack and
// egress polling are the rig's, between windows.
TEST(CheckedWritesAllocRegression, SteadyStateIntentJudgingAllocatesNothingRegression) {
    VerdictRig r;
    r.createKeyIndex();
    r.installProjection(0x40000602);
    r.enableAcks();
    r.worker->setVerdictIntervalNs(20'000'000);
    r.passes(60);
    const int64_t raw = r.subscribe(SubKind::Raw);
    r.passes(60);

    constexpr uint64_t kIds = 10'000;
    constexpr int64_t kPid = 42;
    uint64_t lcg = 0x9E3779B97F4A7C15ull;
    const auto next = [&] { lcg = lcg * 6364136223846793005ull + 1442695040888963407ull; return lcg >> 33; };
    const auto recordsOf = [&](int records, int round) {
        std::vector<Rec> recs;
        for (int i = 0; i < records; ++i) {
            const uint64_t id = next() % kIds + 1, pick = next() % 10;
            if (pick < 4) recs.push_back({id, round, 1, false, 1});        // insert
            else if (pick < 6) recs.push_back({id, round, 1, false, 2});   // update
            else if (pick < 8) recs.push_back({id, 0, 0, true, 2});        // removeExisting
            else recs.push_back({id, round, 1, false, 0});                 // plain upsert
        }
        return recs;
    };
    constexpr int kWarmup = 2000;
    const auto judged = [&] { const VerdictSourceStats& s = *r.stats(); return s.rejectionsKeyExists + s.rejectionsKeyNotFound + s.markersWritten; };
    uint64_t judgedInWindows = 0;   // the 56s, 57s and markers of the measured span that a window saw
    int32_t seq = 0;
    const auto drive = [&](int round) -> uint64_t {
        const int records = round % 2 == 0 ? 1 : 16;
        (void)r.produceFrame(recordsOf(records, round), kAllView, kPid, seq);
        seq += records;
        const bool measured = round >= kWarmup;
        const uint64_t judgedBefore = measured ? judged() : 0;
        uint64_t allocs = 0;
        {
            rune::testing::AllocWindow w;
            for (int i = 0; i < 3; ++i) {
                r.now += 1'000'000;
                r.worker->doWork(r.now);
                r.worker->drainReturnView([](WorkerReturn&&) {});
            }
            allocs = w.count();
        }
        if (measured) judgedInWindows += judged() - judgedBefore;
        r.passes(1);   // the ack and egress polling are the rig's
        if (round % 50 == 0) (void)r.collect(raw);
        return allocs;
    };

    for (int i = 0; i < kWarmup; ++i) (void)drive(i);
    r.passes(100);
    ASSERT_NE(r.stats(), nullptr);
    EXPECT_GT(r.vs->keyIndexSizeForTest(VerdictRig::kSrc, 0), 4096u) << "the warm-up grew the key index past its initial slots";
    const VerdictSourceStats before = *r.stats();
    const std::size_t acksBefore = r.ackOrder().size();
    EXPECT_EQ(acksBefore, 2000u) << "every warm-up batch was acked";

    uint64_t worst = 0, total = 0;
    for (int i = kWarmup; i < kWarmup + 5000; ++i) {
        const uint64_t a = drive(i);
        total += a;
        worst = std::max(worst, a);
    }
    EXPECT_EQ(judgedInWindows, judged() - (before.rejectionsKeyExists + before.rejectionsKeyNotFound + before.markersWritten))
        << "every rejection and marker of the measured batches happened inside a window";
    r.passes(100);
    const VerdictSourceStats& after = *r.stats();
    EXPECT_GT(after.rejectionsKeyExists - before.rejectionsKeyExists, 1000u);
    EXPECT_GT(after.rejectionsKeyNotFound - before.rejectionsKeyNotFound, 1000u);
    EXPECT_GT(after.markersWritten - before.markersWritten, 100u) << "markers were emitted in the window";
    EXPECT_EQ(r.ackOrder().size() - acksBefore, 5000u) << "every batch was acked";
    EXPECT_GT(r.worker->subscriptionPositionForTest(raw), 10'000) << "the Raw subscription was served";
    EXPECT_EQ(total, 0u) << "worst batch allocated " << worst;
}

// Part 06 (hard rule): key-0 creates, imports above the counter and updates from 8 idempotent producers allocate nothing in steady
// state - the reservations (every 64 ids), the in-place patching of the ids and the side-window records included.  A batch holds
// only key-0 creates, or only imports, or only updates.  Every create is a new id, so the warm-up runs until the
// counter passes the key index's second 64 KiB chunk (131,072 ids); the measured batches stay inside the third chunk and the
// table's 262,144 slots.  It also fills every producer's window.
TEST(CheckedWritesAllocRegression, SteadyStateAutoIncCreatesAllocateNothingRegression) {
    VerdictRig r;
    r.createAutoIncTopic({.reserve = 64});
    r.installProjection(0x40000602);
    r.enableAcks();
    r.worker->setVerdictIntervalNs(20'000'000);
    r.passes(60);
    const int64_t raw = r.subscribe(SubKind::Raw);
    r.passes(60);

    constexpr int kProducers = 8;
    constexpr uint64_t kWarmupIds = 131'100;
    uint64_t lcg = 0x9E3779B97F4A7C15ull;
    const auto next = [&] { lcg = lcg * 6364136223846793005ull + 1442695040888963407ull; return lcg >> 33; };
    std::array<int32_t, kProducers> seq{};
    const auto recordsOf = [&](int records, int round) {
        std::vector<Rec> recs;
        const uint64_t counter = r.autoIncState()->nextN, pick = counter < 2 ? 0 : next() % 10;
        for (int i = 0; i < records; ++i) {
            if (pick < 6) recs.push_back({0, round, 1, false, 1});                                        // key-0 create
            else if (pick < 8) recs.push_back({counter + static_cast<uint64_t>(i), round, 1, false, 1});  // import above the counter
            else recs.push_back({next() % (counter - 1) + 1, round, 1, false, 2});                        // update
        }
        return recs;
    };
    const auto drive = [&](int round) -> uint64_t {
        const int records = round % 2 == 0 ? 1 : 16, producer = round % kProducers;
        (void)r.produceFrame(recordsOf(records, round), kAllView, 100 + producer, seq[producer]);
        seq[producer] += records;
        uint64_t allocs = 0;
        {
            rune::testing::AllocWindow w;
            for (int i = 0; i < 3; ++i) {
                r.now += 1'000'000;
                r.worker->doWork(r.now);
                r.worker->drainReturnView([](WorkerReturn&&) {});
            }
            allocs = w.count();
        }
        r.passes(1);
        if (round % 50 == 0) (void)r.collect(raw);
        return allocs;
    };

    int round = 0;
    while (r.autoIncState()->nextN < kWarmupIds) (void)drive(round++);
    r.passes(100);
    const std::size_t acksBefore = r.ackOrder().size();
    EXPECT_EQ(acksBefore, static_cast<std::size_t>(round)) << "every warm-up batch was acked";
    const std::size_t reservationsBefore = r.reservations().size();

    uint64_t worst = 0, total = 0;
    for (const int end = round + 5000; round < end; ++round) {
        const uint64_t a = drive(round);
        total += a;
        worst = std::max(worst, a);
    }
    r.passes(100);
    EXPECT_EQ(r.ackOrder().size() - acksBefore, 5000u) << "every batch was acked";
    EXPECT_GT(r.reservations().size() - reservationsBefore, 100u) << "the measured batches crossed reservations";
    EXPECT_GT(r.worker->subscriptionPositionForTest(raw), 10'000) << "the Raw subscription was served";
    EXPECT_EQ(total, 0u) << "worst batch allocated " << worst;
}
