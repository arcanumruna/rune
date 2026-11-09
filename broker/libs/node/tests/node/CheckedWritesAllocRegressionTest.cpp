// CheckedWritesAllocRegressionTest.cpp - checked writes Part 03 (hard rule): the write hook and the View acks allocate
// nothing in steady state - the hook's check and stage, the View waiter queue and its fold/HW/deadline release.  Replaces
// the global operator new (binary-wide) through AllocWindow.cpp, so it lives in its own binary.
#include "support/AllocWindow.h"
#include "support/CheckedWritesAckRig.h"
#include "support/CheckedWritesRig.h"
#include "support/CheckedWritesViewReadRig.h"

#include <gtest/gtest.h>

#include <rune/codec/Descriptor.h>
#include <rune/view/ViewRow.h>
#include <rune/view/ViewTable.h>

#include <algorithm>
#include <array>
#include <cctype>
#include <functional>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <vector>

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

// Part 07 (hard rule): the keyed fold (upserts, renames, moves, deletes through a plain-key and a lower() key view), the
// GetViewRowK and ScanViewRows serve and the held reads allocate nothing in steady state.  The 03 rig above has no keyed
// view, so this uses Task 14's worker rig: the test thread drives the Worker and its WorkerViewSet.  The source appends and
// the replica bookkeeping run between windows; a window brackets the fold passes and the posting, serving and draining of
// reads.  The key space (240 characters, at most 8 under a user) is fully inserted and churned in the warm-up, so the tables and
// prefix sets are at their steady size.
TEST(CheckedWritesAllocRegression, KeyedFoldAndViewReadsAllocateNothingRegression) {
    namespace pc = rune::protocol::client;
    constexpr int32_t kAccountView = cwkeyed::kViewTopic, kLowerView = cwkeyed::kViewTopic + 1;
    constexpr uint64_t kCharacters = 240, kUsers = 60;
    constexpr std::array<const char*, 8> kNames = {"Alice", "BOB", "carol", "DaVe", "\xC3\x84RGER", "eve", "Mallory", "TRENT"};

    rune::node::test::CwViewReadWorkerRig r;
    r.install(cwkeyed::accountCharacters(r.src));
    r.install(cwkeyed::compileKeyed(r.src, {{"name", 1, "nameKey"}, {"characterId", 0, "characterId"}}, {"gender"}, kLowerView));

    uint64_t lcg = 0x9E3779B97F4A7C15ull;
    const auto next = [&] { lcg = lcg * 6364136223846793005ull + 1442695040888963407ull; return lcg >> 33; };
    std::array<uint64_t, kCharacters + 1> userOf{};
    std::array<const char*, kCharacters + 1> nameOf{};
    const auto churn = [&](int n) {
        std::vector<cwkeyed::Rec> recs;
        for (int i = 0; i < n; ++i) {
            const uint64_t id = next() % kCharacters + 1, pick = next() % 10;
            if (userOf[id] == 0 || pick == 9) { userOf[id] = (id + next() % 2) % kUsers + 1; nameOf[id] = kNames[next() % kNames.size()]; }   // upsert
            else if (pick < 4) nameOf[id] = kNames[next() % kNames.size()];                                                    // rename
            else if (pick < 7) userOf[id] = (id + next() % 2) % kUsers + 1;                                                          // move
            const bool erase = pick == 8;
            recs.push_back({id, userOf[id], nameOf[id], static_cast<uint16_t>(id % 3), erase});
            if (erase) userOf[id] = 0;
        }
        return recs;
    };
    const auto folded = [&] {
        const int64_t hw = r.srcRep.replica->highWatermark();
        return r.vs.foldCursorForTest(kAccountView, 0) >= hw && r.vs.foldCursorForTest(kLowerView, 0) >= hw;
    };
    uint64_t answered = 0, rows = 0, notReady = 0, lowerAnswers = 0, lowerOk = 0;   // answered: ViewRowResult and ViewScanResult only
    const auto drain = [&] {
        r.w.drainReturnView([&](WorkerReturn&& ret) {
            if (ret.type != WorkerReturn::Type::ViewRowResult && ret.type != WorkerReturn::Type::ViewScanResult) return;
            ++answered;
            if (ret.type == WorkerReturn::Type::ViewScanResult) rows += static_cast<uint64_t>(ret.recordCount);
            if (ret.errorCode == static_cast<int16_t>(pc::ViewRowResponse::kNotReady)) ++notReady;
            if (ret.readTopicId == kLowerView) {
                ++lowerAnswers;
                lowerOk += ret.errorCode == 0 ? 1 : 0;
            }
        });
    };
    const auto foldToEnd = [&] {
        for (int i = 0; i < 200 && !folded(); ++i) {
            r.w.doWork(r.now += 1'000'000);
            drain();
        }
        return folded();
    };
    struct Read {
        WorkerOp::Type type;
        std::vector<uint8_t> key;
        int32_t view;
        uint16_t limit;
    };
    const auto planReads = [&](int n) {   // the keys are built outside the window
        std::vector<Read> plan;
        for (int i = 0; i < n; ++i) {
            const uint64_t id = next() % kCharacters + 1, user = userOf[id] != 0 ? userOf[id] : 1;
            switch (i % 3) {
                case 0: plan.push_back({WorkerOp::Type::GetViewRowK, cwkeyed::beKey({user, id}), kAccountView, 0}); break;
                case 1: plan.push_back({WorkerOp::Type::ScanViewRows, cwkeyed::beKey({user}), kAccountView, 12}); break;
                default: plan.push_back({WorkerOp::Type::GetViewRowK, cwkeyed::nameKey("alice", id), kLowerView, 0}); break;
            }
        }
        return plan;
    };
    uint64_t posted = 0;
    const auto reads = [&](const std::vector<Read>& plan, int64_t minOffset = -1) {
        posted += plan.size();
        for (std::size_t i = 0; i < plan.size(); ++i) {
            if (!rune::node::test::postViewRead(r.w, plan[i].type, plan[i].key, {}, plan[i].view, plan[i].limit, minOffset)) ADD_FAILURE() << "op ring full";
            if (i % 8 == 7 || i + 1 == plan.size()) {
                r.w.doWork(r.now += 1'000'000);
                drain();
            }
        }
    };

    std::vector<cwkeyed::Rec> seed;
    for (uint64_t id = 1; id <= kCharacters; ++id) {
        userOf[id] = (id + 1) % kUsers + 1;
        nameOf[id] = kNames[id % kNames.size()];
        seed.push_back({id, userOf[id], nameOf[id], static_cast<uint16_t>(id % 3), false});
    }
    rune::node::test::appendRecords(*r.srcRep.replica, r.src, seed);
    ASSERT_TRUE(foldToEnd());
    for (int b = 0; b < 600; ++b) {   // 9,600 records, renames included: the prefix-set blocks reach their steady classes
        rune::node::test::appendRecords(*r.srcRep.replica, r.src, churn(16));
        ASSERT_TRUE(foldToEnd());
    }
    reads(planReads(500));
    ASSERT_EQ(answered, posted);

    // The gate relies on the prefix-set pool being at its steady size class before the window opens (the warm-up above):
    // a pool chunk is allocated only when a class free list runs dry, which is amortised growth, not a per-op allocation.
    const uint64_t postedBefore = posted, rowsBefore = rows, lowerBefore = lowerAnswers, lowerOkBefore = lowerOk;
    uint64_t allocs = 0;
    const auto timed = [&](auto&& body) {
        rune::testing::AllocWindow w;
        body();
        allocs += w.count();
    };
    for (int b = 0; b < 625; ++b) {   // 10,000 records
        rune::node::test::appendRecords(*r.srcRep.replica, r.src, churn(16));
        timed([&] { EXPECT_TRUE(foldToEnd()); });
        if (b % 5 == 0) {
            const auto plan = planReads(16);
            timed([&] { reads(plan); });
        }
    }
    const auto plan = planReads(3000);   // 2,000 GetViewRowK and 1,000 scans of pages of 12
    timed([&] { reads(plan); });

    // 100 held reads: held inside a window, released by one more record folded inside a window.
    auto held = planReads(100);
    for (auto& h : held) { h.type = WorkerOp::Type::GetViewRowK; h.key = cwkeyed::beKey({1, 1}); h.view = kAccountView; h.limit = 0; }
    const int64_t wanted = r.srcRep.replica->highWatermark();
    timed([&] { reads(held, wanted); });
    EXPECT_EQ(r.w.heldViewReadsForTest(), 100u);
    const uint64_t heldAnswered = answered;
    EXPECT_EQ(heldAnswered, postedBefore + 5000u) << "no held read has been answered yet";
    rune::node::test::appendRecords(*r.srcRep.replica, r.src, churn(1));
    timed([&] {
        EXPECT_TRUE(foldToEnd());
        r.w.doWork(r.now += 1'000'000);
        drain();
    });
    EXPECT_EQ(r.w.heldViewReadsForTest(), 0u);
    EXPECT_EQ(answered - heldAnswered, 100u) << "every held read was answered exactly once";
    EXPECT_EQ(answered, posted) << "one answer per read";
    EXPECT_EQ(notReady, 0u) << "no held read expired";
    EXPECT_GT((lowerOk - lowerOkBefore) * 20, lowerAnswers - lowerBefore) << "at least 5% of the lower-view reads hit a row";
    EXPECT_GT(rows - rowsBefore, 0u) << "the scans returned rows";
    EXPECT_EQ(allocs, 0u);
}

// Part 08 (hard rule): GetStats allocates nothing - the worker collects into its scratch and gathers into the return ring; the
// gateway routes it and encodes the answer into its scratch.  Both scopes, 1,000 of each, through the worker alone and through
// the gateway.
TEST(CheckedWritesAllocRegression, StatsServingAllocatesNothingRegression) {
    namespace pc = rune::protocol::client;
    {
        VerdictRig r;
        r.createAutoIncTopic({});
        r.installProjection(0x40000602);
        r.enableAcks();
        r.passes(60);
        ASSERT_TRUE(r.ack(r.produceFrame({Rec{0, 1, 0, false, 1}, Rec{0, 2, 0, false, 1}}, kAllView, 100, 0)));
        ASSERT_TRUE(r.driveUntilJudged());

        uint64_t answered = 0;
        const auto ask = [&](pc::StatsScope scope, int32_t id) {
            WorkerOp op;
            op.type = WorkerOp::Type::GetStats; op.topicId = id; op.partition = 0; op.sessionId = 1; op.correlationId = 1;
            op.recordCount = static_cast<int32_t>(scope);
            const bool queued = r.worker->tryEnqueueOp(std::move(op));
            r.now += 1'000'000;
            r.worker->doWork(r.now);
            r.worker->drainReturnView([&](WorkerReturn&& ret) { answered += ret.type == WorkerReturn::Type::StatsResult && ret.errorCode == 0 ? 1 : 0; });
            return queued;
        };
        for (int i = 0; i < 4; ++i) { (void)ask(pc::StatsScope::TopicPartition, VerdictRig::kSrc); (void)ask(pc::StatsScope::ViewPartition, 0x40000602); }
        answered = 0;
        bool queued = true;
        {
            rune::testing::AllocWindow w;
            for (int i = 0; i < 1000; ++i) queued &= ask(pc::StatsScope::TopicPartition, VerdictRig::kSrc);
            EXPECT_EQ(w.count(), 0u) << "1000 topic GetStats on the worker";
        }
        {
            rune::testing::AllocWindow w;
            for (int i = 0; i < 1000; ++i) queued &= ask(pc::StatsScope::ViewPartition, 0x40000602);
            EXPECT_EQ(w.count(), 0u) << "1000 view GetStats on the worker";
        }
        EXPECT_TRUE(queued);
        EXPECT_EQ(answered, 2000u);
    }
    {
        using rune::node::test::CwViewReadGatewayRig;
        CwViewReadGatewayRig rig;
        rune::node::TopicSpec src;
        src.name = "character";
        ASSERT_EQ(rig.registry->createTopicAt(CwViewReadGatewayRig::kSrcTopic, src), CwViewReadGatewayRig::kSrcTopic);
        const auto frameOf = [&](pc::StatsScope scope, int32_t id) {
            pc::GetStats m;
            m.sessionId = rig.sid; m.correlationId = 1; m.scope = scope; m.objectId = id; m.partition = 0;
            std::vector<uint8_t> f(m.encodedSize());
            m.encode(f.data());
            return f;
        };
        const auto topicFrame = frameOf(pc::StatsScope::TopicPartition, CwViewReadGatewayRig::kSrcTopic);
        const auto viewFrame = frameOf(pc::StatsScope::ViewPartition, CwViewReadGatewayRig::kViewTopic);
        for (int i = 0; i < 4; ++i) {
            rig.gw.applyGetStatsForTest(topicFrame.data(), topicFrame.size());
            ASSERT_TRUE(rig.awaitFrame());
            rig.gw.applyGetStatsForTest(viewFrame.data(), viewFrame.size());
            ASSERT_TRUE(rig.awaitFrame());
        }
        uint64_t answered = 0;
        for (const auto* frame : {&topicFrame, &viewFrame}) {
            rune::testing::AllocWindow w;
            for (int i = 0; i < 1000; ++i) {
                rig.gw.applyGetStatsForTest(frame->data(), frame->size());
                answered += rig.awaitFrame() ? 1 : 0;
            }
            EXPECT_EQ(w.count(), 0u) << (frame == &topicFrame ? "1000 topic GetStats through the gateway" : "1000 view GetStats through the gateway");
        }
        EXPECT_EQ(answered, 2000u);
    }
}

// Part 09 (hard rule): judging with a unique view and a check view allocates nothing in steady state - the claims, the owner
// and extras tables, the group counts, the 55 verdicts of each view, the promotions of an extra claimant and the markers.  The
// source keeps the key index, character_name, account_stats and account_characters; 8 names had 20 claimants each before the
// constraints were created, so the owner of a name leaves (deleted or renamed) and the next claimant is promoted all through
// the run.  Characters 161..244 churn through a pool of 64 names; account 41 is full, so every move into it is refused.  A
// batch's records touch distinct characters and distinct names, and the model of the names and accounts follows the acks.
// Accounts empty and refill as characters are deleted and moved.
// The window brackets the worker's passes of one batch; the frame, the ack wait and the model are the rig's.
TEST(CheckedWritesAllocRegression, SteadyStateUniqueAndCheckJudgingAllocatesNothingRegression) {
    using Char = VerdictRig::Char;
    using CV = ConstrainedView;
    constexpr uint8_t kLeader = 1;
    constexpr uint16_t kViolation = 55;
    constexpr uint64_t kClaimed = 160, kMobile = 244, kFull = 41, kPool = 64;
    constexpr int kWarmup = 300, kMeasured = 3000;
    const auto lower = [](std::string n) { for (char& c : n) c = static_cast<char>(std::tolower(static_cast<unsigned char>(c))); return n; };

    VerdictRig r;
    r.createCharacterTopic(false);
    r.enableAcks();
    r.worker->setVerdictIntervalNs(20'000'000);
    r.passes(60);
    const int64_t raw = r.subscribe(SubKind::Raw);

    std::array<std::string, 257> name;
    std::array<uint64_t, 257> user{};
    std::array<bool, 257> alive{};
    std::map<std::string, uint64_t> owner;   // lower(name) of the pool -> the character that owns it
    std::vector<Char> seed;
    for (uint64_t id = 1; id <= 256; ++id) {
        user[id] = id >= 245 ? kFull : id % 40 + 1;
        name[id] = id <= kClaimed ? (id % 3 == 0 ? "DUP" : id % 3 == 1 ? "dup" : "Dup") + std::to_string((id - 1) % 8) : "Z" + std::to_string(id);
        alive[id] = true;
        seed.push_back(Char{.id = id, .userId = user[id], .name = name[id]});
    }
    for (std::size_t at = 0; at < seed.size(); at += 16) {
        const std::vector<Char> chunk(seed.begin() + static_cast<std::ptrdiff_t>(at), seed.begin() + static_cast<std::ptrdiff_t>(std::min(seed.size(), at + 16)));
        const auto a = r.ack(r.appendCharacters(chunk, kLeader));
        ASSERT_TRUE(a && a->code == 0);
    }
    ASSERT_TRUE(r.driveUntilJudgeReaches());
    for (const CV v : {CV::CharacterName, CV::AccountStats, CV::AccountCharacters}) r.createConstrainedView(v);
    ASSERT_TRUE(r.driveUntilJudgeReaches());
    for (const auto& [v, l] : r.constrained) r.driveUntilFolded(l.topicId, r.dataEnd());
    ASSERT_EQ(r.uniqueConflicts(CV::CharacterName), 152u) << "8 names with 20 claimants each: 19 extras per name";

    uint64_t lcg = 0x9E3779B97F4A7C15ull;
    const auto next = [&](uint64_t n) { lcg = lcg * 6364136223846793005ull + 1442695040888963407ull; return (lcg >> 33) % n; };
    const auto poolName = [](uint64_t p, bool upper) { return std::string(upper ? "P" : "p") + std::to_string(p); };
    // A pool name nobody owns (starting at a random one), or "" when all 64 are taken.
    const auto freePoolName = [&](const std::set<std::string>& usedNow) {
        for (uint64_t i = 0, p = next(kPool); i < kPool; ++i, p = (p + 1) % kPool) {
            const std::string n = poolName(p, next(2) == 0);
            if (owner.count(lower(n)) == 0 && usedNow.count(lower(n)) == 0) return n;
        }
        return std::string();
    };
    const auto ownerOfClaimed = [&](uint64_t group) -> uint64_t {   // the smallest live claimant of dup<group>
        for (uint64_t id = group + 1; id <= kClaimed; id += 8)
            if (alive[id]) return id;
        return 0;
    };

    int32_t seq = 0;
    uint64_t ownedRejections = 0, countRejections = 0, accepted = 0;
    const auto planBatch = [&](int round, int records) {
        std::vector<Char> out;
        std::set<uint64_t> usedIds;
        std::set<std::string> usedNames;
        const auto fresh = [&](uint64_t id) { return usedIds.insert(id).second; };
        if (round % 20 == 0) {   // the owner of a claimed name leaves: deleted or renamed away, and the next claimant is promoted
            const uint64_t group = static_cast<uint64_t>(round / 20) % 8, id = ownerOfClaimed(group);
            const std::string to = freePoolName(usedNames);
            const bool erase = (round / 20) % 2 == 0 || to.empty();
            if (id != 0 && fresh(id)) {
                if (erase) out.push_back(Char{.id = id, .tombstone = true});
                else { usedNames.insert(lower(to)); out.push_back(Char{.id = id, .userId = user[id], .name = to}); }
            }
        }
        while (out.size() < static_cast<std::size_t>(records)) {
            const uint64_t pick = next(100), id = kClaimed + 1 + next(kMobile - kClaimed);
            if (pick < 10) {   // a name another character owns
                if (!alive[id] || !fresh(id)) continue;
                std::string taken;
                for (const auto& [lowered, who] : owner)
                    if (who != id && usedNames.count(lowered) == 0 && (taken.empty() || next(4) == 0)) taken = lowered;
                if (taken.empty()) continue;
                usedNames.insert(taken);
                out.push_back(Char{.id = id, .userId = user[id], .name = taken});
            } else if (pick < 20) {   // into the full account
                if (!alive[id] || !fresh(id)) continue;
                out.push_back(Char{.id = id, .userId = kFull, .name = name[id]});
            } else if (pick < 30) {   // delete
                if (!alive[id] || !fresh(id)) continue;
                out.push_back(Char{.id = id, .tombstone = true});
            } else if (pick < 45) {   // create
                if (alive[id] || !fresh(id)) continue;
                const std::string to = freePoolName(usedNames);
                if (to.empty()) continue;
                usedNames.insert(lower(to));
                out.push_back(Char{.id = id, .userId = next(40) + 1, .name = to});
            } else if (pick < 70) {   // rename, now and then to the own name in another case
                if (!alive[id] || !fresh(id)) continue;
                const bool ownName = next(5) == 0 && owner.count(lower(name[id])) != 0;
                const std::string to = ownName ? (name[id][0] == 'P' ? lower(name[id]) : "P" + name[id].substr(1)) : freePoolName(usedNames);
                if (to.empty()) { usedIds.erase(id); continue; }
                usedNames.insert(lower(to));
                out.push_back(Char{.id = id, .userId = user[id], .name = to});
            } else {   // a move or a plain update, of any character (a claimant keeps its name)
                const uint64_t any = 1 + next(kMobile);
                if (!alive[any] || !fresh(any)) continue;
                const bool moves = pick < 85;
                out.push_back(Char{.id = any, .userId = moves ? next(40) + 1 : user[any], .name = name[any], .lastPlayed = static_cast<int64_t>(round)});
            }
        }
        return out;
    };
    const auto settle = [&](const std::vector<Char>& sent, const VerdictRig::Ack& a) {
        std::set<std::size_t> refused;
        for (const auto& [index, code, cid] : a.verdicts) {
            refused.insert(index);
            ownedRejections += cid == (7u << 16);
            countRejections += cid == ((8u << 16) | 1);
            EXPECT_EQ(code, kViolation);
        }
        for (std::size_t i = 0; i < sent.size(); ++i) {
            if (refused.count(i) != 0) continue;
            ++accepted;
            const Char& c = sent[i];
            if (alive[c.id] && owner.count(lower(name[c.id])) != 0 && owner[lower(name[c.id])] == c.id) owner.erase(lower(name[c.id]));
            alive[c.id] = !c.tombstone;
            if (c.tombstone) continue;
            user[c.id] = c.userId;
            name[c.id] = c.name;
            if (lower(c.name)[0] == 'p') owner[lower(c.name)] = c.id;
        }
    };
    const auto drive = [&](int round, uint64_t& windowRejections) -> uint64_t {
        const int records = round % 2 == 0 ? 1 : 16;
        const std::vector<Char> sent = planBatch(round, records);
        const int64_t c = r.appendCharacters(sent, kAllView, 41, seq);
        seq += static_cast<int32_t>(sent.size());
        const uint64_t before = r.stats()->rejectionsConstraint;
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
        windowRejections += r.stats()->rejectionsConstraint - before;
        r.passes(1);
        if (round % 50 == 0) (void)r.collect(raw);
        const auto a = r.ack(c);
        EXPECT_TRUE(a && a->code == 0) << "round " << round;
        if (a) settle(sent, *a);
        return allocs;
    };
    uint64_t ignored = 0;
    for (int round = 0; round < kWarmup; ++round) (void)drive(round, ignored);
    r.passes(100);
    const size_t conflictsBefore = r.uniqueConflicts(CV::CharacterName);
    const uint64_t rejectedBefore = r.stats()->rejectionsConstraint;
    const uint64_t ownedBefore = ownedRejections, countBefore = countRejections;

    uint64_t total = 0, worst = 0, windowRejections = 0;
    for (int round = kWarmup; round < kWarmup + kMeasured; ++round) {
        const uint64_t a = drive(round, windowRejections);
        total += a;
        worst = std::max(worst, a);
    }
    r.passes(100);
    const uint64_t rejected = r.stats()->rejectionsConstraint - rejectedBefore;
    EXPECT_GT(ownedRejections - ownedBefore, 200u) << "character_name refused takes";
    EXPECT_GT(countRejections - countBefore, 200u) << "account_stats refused moves into the full account";
    EXPECT_EQ(windowRejections, rejected) << "every rejection of the measured batches happened inside a window";
    EXPECT_LT(r.uniqueConflicts(CV::CharacterName) + 50, conflictsBefore) << "more than 50 extra claimants were promoted in the measured span";
    EXPECT_GT(accepted, static_cast<uint64_t>(kMeasured));
    EXPECT_GT(r.worker->subscriptionPositionForTest(raw), 1000) << "the Raw subscription was served";
    EXPECT_EQ(total, 0u) << "worst batch allocated " << worst;
}

// REGRESSION (Task 8 review 4, N-2): a view whose every window erases and rewrites its 2000 rows (past twice the dirty set's
// reserve) drains and trims its dirty set with no allocation once warm: the second large window in a row keeps the capacity.
// The trim gave it back after every drain, so each window allocated it again.  A small window then gives the capacity back.
TEST(CheckedWritesAllocRegression, HighChurnDirtyDrainAllocatesNothingRegression) {
    const rune::view::RowLayout layout = rune::view::buildRowLayout(
        rune::codec::DescriptorBuilder("v", 1).add("id", rune::codec::PrimType::Int64, 1, true).add("hp", rune::codec::PrimType::Int32).build());
    rune::view::ViewTable t(layout, 64, layout.rowLen * 256);
    for (uint64_t k = 0; k < 2000; ++k) (void)t.put(k);
    const auto window = [&](uint64_t from, uint64_t n) {
        for (uint64_t k = from; k < from + n; ++k) (void)t.erase(k % 2000);
        for (uint64_t k = from; k < from + n; ++k) (void)t.put(k % 2000);
        t.drainDirtyKeyed([](auto&&...) {});
        t.trimDirty();
    };
    for (uint64_t w = 0; w < 4; ++w) window(0, 2000);   // warm-up
    uint64_t allocs = 0;
    {
        rune::testing::AllocWindow aw;
        for (int w = 0; w < 50; ++w) window(0, 2000);
        allocs = aw.count();
    }
    EXPECT_EQ(allocs, 0u) << "steady churn keeps its dirty set";
    EXPECT_GT(t.dirtyBytes(), 0u) << "the kept growth stays charged";
    window(0, 10);
    EXPECT_EQ(t.dirtyBytes(), 0u) << "a small window gives it back";
}

// Review 5 N-3: a long span's window after steady churn was kept at its push, and a view gone quiet kept it charged for good.
TEST(CheckedWritesAllocRegression, LongWindowAfterSteadyChurnIsGivenBackRegression) {
    const rune::view::RowLayout layout = rune::view::buildRowLayout(
        rune::codec::DescriptorBuilder("v", 1).add("id", rune::codec::PrimType::Int64, 1, true).add("hp", rune::codec::PrimType::Int32).build());
    rune::view::ViewTable t(layout, 64, layout.rowLen * 256);
    for (uint64_t k = 0; k < 2000; ++k) (void)t.put(k);
    const auto window = [&](int rounds) {   // every round rewrites the whole table: a deleted entry per erase
        for (int r = 0; r < rounds; ++r) {
            for (uint64_t k = 0; k < 2000; ++k) (void)t.erase(k);
            for (uint64_t k = 0; k < 2000; ++k) (void)t.put(k);
        }
        t.drainDirtyKeyed([](auto&&...) {});
        t.trimDirty();
    };
    for (int w = 0; w < 6; ++w) window(1);
    const size_t steady = t.dirtyBytes();
    ASSERT_GT(steady, 0u) << "steady churn keeps its dirty set";
    window(10);   // the long span's window, pushed; then the view goes quiet
    EXPECT_LE(t.dirtyBytes(), steady) << "no standing charge past the steady window's";
}

// Review 7 m-3: an empty drain (a member's quiet judge pass, a pass of only control records) counted as a window of 0, so the next
// real window was more than twice it and was given back at its push: an allocation per window again.
TEST(CheckedWritesAllocRegression, EmptyDrainsBetweenWindowsAllocateNothingRegression) {
    const rune::view::RowLayout layout = rune::view::buildRowLayout(
        rune::codec::DescriptorBuilder("v", 1).add("id", rune::codec::PrimType::Int64, 1, true).add("hp", rune::codec::PrimType::Int32).build());
    rune::view::ViewTable t(layout, 64, layout.rowLen * 256);
    for (uint64_t k = 0; k < 2000; ++k) (void)t.put(k);
    const auto window = [&] {
        for (uint64_t k = 0; k < 2000; ++k) (void)t.erase(k);
        for (uint64_t k = 0; k < 2000; ++k) (void)t.put(k);
        t.drainDirtyKeyed([](auto&&...) {});
        t.trimDirty();
        t.drainDirtyKeyed([](auto&&...) {});   // an empty drain, then its trim
        t.trimDirty();
    };
    for (int w = 0; w < 4; ++w) window();   // warm-up
    uint64_t allocs = 0;
    {
        rune::testing::AllocWindow aw;
        for (int w = 0; w < 20; ++w) window();
        allocs = aw.count();
    }
    EXPECT_EQ(allocs, 0u) << "an empty drain is not a window";
    EXPECT_GT(t.dirtyBytes(), 0u);
}

// Part 11: the vote, the lock pause, the marker's step and an atomic batch allocate nothing in steady state.  Two rigs joined as a
// coordinator would: topic character under its three views (unique name, account stats, account characters) and topic entry under
// the balances.  The mix: transactions of 2..16 characters and a debit, 20 % voted no (10 % a taken name, 10 % an overdraw), 10 %
// aborted after both yes, an outside write on a locked row pausing behind every 5th; atomic batches, 10 % refused whole.  The window
// brackets the worker's passes of each step (the write, a vote, a marker, an atomic batch) until the step is done, and asserts it was;
// the frames, the acks and the polling are the rig's.
// The records reuse 256 characters and 64 entries: a view's row table growing for new keys is the fold's.
namespace {
struct TxnAllocMix {
    TxnPair pair;
    uint64_t allocs = 0, expectedCold = 0, committed = 0, votedNo = 0, abortedAfterYes = 0, lockedWrites = 0, atomicRefused = 0;
    uint64_t lcg = 0x9E3779B97F4A7C15ull;
    std::map<int64_t, int32_t> seqA, seqB;
    int32_t seqLocked = 0;
    int txns = 0, batches = 0;

    static constexpr uint64_t kChars = 256, kEntries = 64, kAccounts = 8;
    static constexpr uint8_t kAcksViewAll = rune::protocol::client::kAcksWireView | rune::protocol::client::kAcksWireAll;
    static constexpr uint8_t kAcksAll = rune::protocol::client::kAcksWireAll;

    TxnAllocMix() : pair(setup()) {
        VerdictRig& a = pair.a;
        VerdictRig& b = pair.b;
        for (uint64_t at = 1; at <= kChars; at += 16) {
            std::vector<VerdictRig::Char> chunk;
            for (uint64_t id = at; id < at + 16; ++id) chunk.push_back(character(id, 0));
            EXPECT_TRUE(a.ack(a.appendCharacters(chunk, 1)));
        }
        std::vector<VerdictRig::Entry> open;
        for (uint64_t acc = 1; acc <= kAccounts; ++acc) open.push_back({acc, acc, 1000, "open"});
        EXPECT_TRUE(b.ack(b.appendEntries(open, kAcksAll)));
        EXPECT_TRUE(a.driveUntilJudgeReaches());
        EXPECT_TRUE(b.driveUntilJudgeReaches());
        for (const ConstrainedView v : {ConstrainedView::CharacterName, ConstrainedView::AccountStats, ConstrainedView::AccountCharacters}) a.createConstrainedView(v);
        b.createConstrainedView(ConstrainedView::Ledger);
        for (VerdictRig* r : {&a, &b}) {
            EXPECT_TRUE(r->driveUntilJudgeReaches());
            for (const auto& [v, l] : r->constrained) r->driveUntilFolded(l.topicId, r->dataEnd());
        }
    }

    static std::function<void(VerdictRig&)> setup() {
        auto n = std::make_shared<int>(0);
        return [n](VerdictRig& r) {
            r.worker->setVerdictIntervalNs(1'000'000);
            if ((*n)++ == 0) r.createCharacterTopic(false);
            else r.createLedgerTopic();
            r.enableAcks();
            r.passes(60);
        };
    }
    static VerdictRig::Char character(uint64_t id, int64_t lastPlayed, bool takeNameOf = false, uint64_t other = 0) {
        return VerdictRig::Char{.id = id, .userId = id % 40 + 1, .name = "c" + std::to_string(takeNameOf ? other : id), .lastPlayed = lastPlayed};
    }
    uint64_t next(uint64_t n) { lcg = lcg * 6364136223846793005ull + 1442695040888963407ull; return (lcg >> 33) % n; }

    /// The worker's passes of `r` until `done()`, bracketed: what a pass does is the gate, the rig's polling between windows is not.
    /// `done` reads counters only, and must hold: a step's work left past its window would go unmeasured.
    template <class Done, class Body>
    void window(VerdictRig& r, Done&& done, Body&& body) {
        bool reached = false;
        {
            rune::testing::AllocWindow w;
            body();
            for (int i = 0; !(reached = done()) && i < 200; ++i) {
                r.now += 1'000'000;
                r.worker->doWork(r.now);
                r.worker->drainReturn([](WorkerReturn&&) {});
            }
            allocs += w.count();
        }
        EXPECT_TRUE(reached) << "txn " << txns << " batch " << batches;
    }
    template <class Done>
    void window(VerdictRig& r, Done&& done) { window(r, std::forward<Done>(done), [] {}); }
    /// The step's appends are in, under the HW, and the worker holds none of their acks.
    static auto appended(VerdictRig& r, int64_t leo0) {
        return [&r, leo0] { return r.leo() > leo0 && r.replica->highWatermark() >= r.leo() && r.worker->heldAcksForTest() == 0; };
    }
    /// ... and the judge is past them (a marker applied, a batch judged), and the markers cover the data through `marked`.
    static auto judged(VerdictRig& r, int64_t leo0, int64_t marked = 0) {
        return [&r, leo0, marked, in = appended(r, leo0)] {
            const VerdictSource* j = r.vs->verdictSource(VerdictRig::kSrc, 0);
            return in() && j != nullptr && j->judgedThrough() >= r.leo() && r.writeState()->markers.committedThrough() >= marked;
        };
    }

    struct Vote { bool answered = false; bool yes = false; };
    Vote vote(VerdictRig& r, int64_t pid) {
        Vote v;
        rune::testing::AllocWindow w;
        WorkerOp op;
        op.type = WorkerOp::Type::TxnVote; op.topicId = VerdictRig::kSrc; op.partition = 0; op.replica = r.replica.get();
        op.logPtr = r.log.get(); op.correlationId = ++voteToken; op.producerId = pid; op.producerEpoch = 0; op.sessionId = -1;
        op.targetReplicaId = r.replica->registryId(); op.incarnation = r.replica->incarnation();
        EXPECT_TRUE(r.worker->tryEnqueueOp(std::move(op)));
        for (int i = 0; i < 30 && !v.answered; ++i) {
            r.now += 1'000'000;
            r.worker->doWork(r.now);
            r.worker->drainReturn([&](WorkerReturn&& ret) {
                if (ret.type != WorkerReturn::Type::TxnVoteResult || ret.correlationId != voteToken) return;
                v.answered = true;
                v.yes = ret.errorCode == 0 && ret.recordCount == 0;
            });
        }
        allocs += w.count();
        return v;
    }
    int64_t voteToken = 400'000;

    void marker(VerdictRig& r, int64_t pid, bool commit) {
        WorkerOp op;
        op.type = WorkerOp::Type::AppendControlRecord; op.topicId = VerdictRig::kSrc; op.partition = 0; op.replica = r.replica.get();
        op.logPtr = r.log.get(); op.correlationId = 9000 + pid; op.producerId = pid;
        op.targetReplicaId = r.replica->registryId(); op.incarnation = r.replica->incarnation();
        op.recordBatchBytes = rune::transaction::ControlMarker::build(
            pid, 0, commit ? rune::transaction::ControlType::Commit : rune::transaction::ControlType::Abort, 0);
        const std::size_t aborted = r.worker->rcAbortedRangesForTest(VerdictRig::kSrc, 0).size();
        const bool coldGrowth = !commit && aborted >= rune::transaction::AbortedTxnIndex::kDefaultReservedRanges && (aborted & (aborted - 1)) == 0;
        expectedCold += coldGrowth ? 1 : 0;
        window(r, judged(r, r.leo(), commit ? r.dataEnd() : 0), [&] { EXPECT_TRUE(r.worker->tryEnqueueOp(std::move(op))); });
        EXPECT_TRUE(commit ? r.driveUntilJudged() : r.driveUntilJudgeReaches());
    }

    void transaction() {
        const int t = txns++;
        VerdictRig& a = pair.a;
        VerdictRig& b = pair.b;
        const int64_t pid = 100 + t % 16;
        const bool takenName = t % 10 == 3, overdraw = t % 10 == 8, abortAfterYes = t % 10 == 7, locked = t % 5 == 1;
        std::vector<VerdictRig::Char> chars;
        std::set<uint64_t> ids;
        const std::size_t records = 2 + next(15);
        while (chars.size() < records) {
            const uint64_t id = 1 + next(kChars);
            if (!ids.insert(id).second) continue;
            const uint64_t other = id % kChars + 1;
            chars.push_back(character(id, t, takenName && chars.empty() && ids.count(other) == 0, other));
        }
        const uint64_t entryId = 100 + static_cast<uint64_t>(t) % kEntries;
        const VerdictRig::Entry debit{entryId, entryId % kAccounts + 1, overdraw ? -1'000'000 : -10, "d"};
        const int32_t sa = seqA[pid], sb = seqB[pid];
        seqA[pid] += static_cast<int32_t>(chars.size());
        seqB[pid] += 1;
        const int64_t leoA = a.leo(), leoB = b.leo();
        const int64_t ca = a.appendCharacters(chars, kAcksAll, pid, sa, /*txn=*/true);
        const int64_t cb = b.appendEntries(std::span<const VerdictRig::Entry>(&debit, 1), kAcksAll, pid, sb, /*txn=*/true);
        window(a, appended(a, leoA));
        window(b, appended(b, leoB));
        EXPECT_TRUE(a.ack(ca));
        EXPECT_TRUE(b.ack(cb));
        const Vote va = vote(a, pid), vb = vote(b, pid);
        EXPECT_TRUE(va.answered && vb.answered) << "txn " << t;
        const bool yes = va.yes && vb.yes;
        EXPECT_EQ(yes, !takenName && !overdraw) << "txn " << t;
        votedNo += yes ? 0 : 1;
        int64_t lockedAck = -1;
        if (locked && yes) {   // a plain write to a row the open transaction holds: its verdict waits for the marker
            const uint64_t pauses = a.writeState()->txn.stats.lockPauses;
            lockedAck = a.appendCharacters(std::vector<VerdictRig::Char>{character(chars.front().id, -t)}, kAcksViewAll, 500, seqLocked++);
            window(a, [&] { return a.writeState()->txn.stats.lockPauses > pauses; });
            ++lockedWrites;
        }
        const bool commit = yes && !abortAfterYes;
        abortedAfterYes += yes && abortAfterYes ? 1 : 0;
        committed += commit ? 1 : 0;
        marker(a, pid, commit);
        marker(b, pid, commit);
        if (lockedAck >= 0) {
            const auto ack = a.ack(lockedAck);
            EXPECT_TRUE(ack && ack->code == 0);
        }
    }

    void atomicBatch() {
        const int i = batches++;
        VerdictRig& a = pair.a;
        std::vector<VerdictRig::Char> chars;
        std::set<uint64_t> ids;
        const std::size_t records = 2 + next(15);
        const bool refused = i % 10 == 5;
        while (chars.size() < records) {
            const uint64_t id = 1 + next(kChars);
            if (!ids.insert(id).second) continue;
            const uint64_t other = id % kChars + 1;
            chars.push_back(character(id, i, refused && chars.empty() && ids.count(other) == 0, other));
        }
        const int64_t leo = a.leo();
        const int64_t c = a.appendCharacters(chars, kAcksViewAll, -1, -1, /*txn=*/false, /*atomic=*/true);
        window(a, judged(a, leo));
        const auto ack = a.ack(c);
        ASSERT_TRUE(ack);
        EXPECT_EQ(ack->code, 0);
        EXPECT_EQ(!ack->verdicts.empty(), refused) << "batch " << i;
        atomicRefused += ack->verdicts.empty() ? 0 : 1;
    }
};
}  // namespace

// The abort index's vector doubles past its 256 reserved ranges (AbortedTxnIndex: cold growth, counted): the one allocation the
// full-length run expects, at each power of two of aborted ranges.
static void runTxnAllocMix(int warm, int measured) {
    TxnAllocMix m;
    for (int i = 0; i < warm; ++i) { m.transaction(); m.atomicBatch(); }
    m.allocs = m.expectedCold = m.committed = m.votedNo = m.abortedAfterYes = m.lockedWrites = m.atomicRefused = 0;
    const uint64_t pausesBefore = m.pair.a.writeState()->txn.stats.lockPauses;
    for (int i = 0; i < measured; ++i) { m.transaction(); m.atomicBatch(); }
    EXPECT_EQ(m.allocs, m.expectedCold);
    EXPECT_GT(m.committed, static_cast<uint64_t>(measured / 2));
    EXPECT_GT(m.votedNo, static_cast<uint64_t>(measured / 10));
    EXPECT_GT(m.abortedAfterYes, static_cast<uint64_t>(measured / 20));
    EXPECT_GT(m.lockedWrites, static_cast<uint64_t>(measured / 10));
    EXPECT_GT(m.pair.a.writeState()->txn.stats.lockPauses, pausesBefore);
    EXPECT_GT(m.atomicRefused, static_cast<uint64_t>(measured / 20));
}

TEST(CheckedWritesAllocRegression, SteadyStateTxnVotesAndAtomicBatchesAllocateNothingRegression) { runTxnAllocMix(50, 150); }

TEST(CheckedWritesAllocRegression, SteadyStateTxnVotesAndAtomicBatchesAllocateNothingFullLengthRegression) { runTxnAllocMix(200, 2000); }
