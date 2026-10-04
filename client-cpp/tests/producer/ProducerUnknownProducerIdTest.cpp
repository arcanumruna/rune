// tests/producer/ProducerUnknownProducerIdTest.cpp — D6: UNKNOWN_PRODUCER_ID.
//
// The broker answers UnknownProducerId (28) + the partition's logStartOffset
// when its (proven, rebuilt) producer state has no entry for this producer and
// the batch's baseSequence != 0 — retention removed every batch of this
// producer before a failover / restart.  The batch was NOT appended.  Rule U
// (Kafka TransactionManager.canRetry / KIP-360), applied like C to the CURRENT
// attempt of batch b:
//   U1  lastAckedOffset(partition) < logStartOffset -> re-sequence exactly as C3
//       (new identity per rule F, b and everything behind it re-stamped and
//       re-sent) — nothing fails (the accepted KIP-360 duplicate window applies);
//   U2  otherwise (D6 review M1, Kafka canRetry's idempotent branch: always
//       retry with an epoch bump) -> ALSO re-sequenced: b's attempts would sit
//       above lastAckedOffset >= logStart, inside the rebuilt range, so a 28
//       proves none of them is stored — the re-send is duplicate-free.  Counted
//       as broker state loss (diagnostic), never an app-visible failure.
//
// Part 1: bare ProducerImpl (no Aeron, frames captured from the sink, answers
// injected).  Part 2: end to end — a real Invoker Client against the
// InvokerGateway broker model (retention + failover = retainAndRebuild).
#include "../support/AckLedger.h"
#include "../support/InvokerGateway.h"

#include <rune/client/Client.h>
#include <rune/client/codec/DataCodec.h>
#include <rune/client/producer/BatchBuilder.h>
#include <rune/client/producer/ProducerImpl.h>
#include <rune/client/session/SessionMachine.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/protocol/client/Protocol.h>

#include <gtest/gtest.h>

#include <chrono>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <string>
#include <unistd.h>
#include <vector>

namespace rc  = rune::client;
namespace rcc = rune::client::codec;
namespace rpc = rune::protocol::client;
using rc::producer::ProducerImpl;
using test_support::AckLedger;

namespace {

constexpr int64_t kNow = 1'000'000'000;
constexpr int16_t kUnknownPid = static_cast<int16_t>(rpc::ErrorCode::UnknownProducerId);
constexpr int16_t kOos        = static_cast<int16_t>(rpc::ErrorCode::OutOfOrderSequence);

struct Frame {
    int64_t corr;
    int64_t producerId;
    int16_t epoch;
    int32_t seq;
    int32_t records;
};
struct Capture {
    std::vector<Frame> frames;
    ProducerImpl::SendFn fn() {
        return [this](int32_t, int32_t, int32_t, const uint8_t* b, std::size_t n) {
            const auto v = rcc::decodeProduce(b, n);
            if (!v) return false;
            rune::protocol::RecordBatchReader r(v->recordBatch.data(), v->recordBatch.size());
            frames.push_back({v->correlationId, v->producerId, v->producerEpoch, v->baseSequence, r.recordCount()});
            return true;
        };
    }
};
struct BatchAcks {
    std::vector<rc::BatchAck> acks;
    static void on(void* ctx, const rc::BatchAck& a) noexcept { static_cast<BatchAcks*>(ctx)->acks.push_back(a); }
    [[nodiscard]] int failures() const {
        int n = 0;
        for (const auto& a : acks) n += a.errorCode != 0 ? 1 : 0;
        return n;
    }
};

rc::ProducerConfig bareCfg() {
    rc::ProducerConfig c;
    c.acks              = rc::Acks::Leader;
    c.lingerUs          = 0;
    c.maxInFlight       = 8;
    c.produceTimeoutMs  = 60'000;   // no retransmit timer inside a test
    c.deliveryTimeoutMs = 120'000;
    return c;
}

void pump(ProducerImpl& impl, const ProducerImpl::SendFn& sink) {
    for (int i = 0; i < 100'000; ++i) {
        const int d = impl.drainRing(kNow);
        impl.flushBatches(kNow, 42, sink);
        if (d == 0) break;
    }
}

const std::vector<uint8_t> kVal(16, 0x11);

// A bare producer with `n` single-record batches in flight on (1, 0).
struct Bare {
    ProducerImpl impl{bareCfg()};
    Capture      cap;
    BatchAcks    A;
    ProducerImpl::SendFn sink = cap.fn();
    explicit Bare(int n) {
        impl.setOnBatchAck(&BatchAcks::on, &A);
        for (int i = 0; i < n; ++i) {
            EXPECT_TRUE(impl.submitSendRecord(1, 0, rc::Key(uint64_t{1}), kVal, false, 0));
            pump(impl, sink);
        }
        EXPECT_EQ(cap.frames.size(), static_cast<std::size_t>(n));
    }
    const rc::producer::PartitionState& ps() { return *impl.partitionState(1, 0); }
};

} // namespace

// ---------------------------------------------------------------------------
// Part 1 — rule U on a bare ProducerImpl
// ---------------------------------------------------------------------------

// lastAckedOffset: the max acked lastOffset (-1 = none); a known-old OK -1 does
// not move it; a multi-record batch counts its LAST offset.
TEST(UnknownProducerIdRegression, LastAckedOffsetTracksTheMaxAckedLastOffsetRegression) {
    Bare b(3);
    EXPECT_EQ(b.ps().lastAckedOffset, -1) << "nothing acked yet";
    b.impl.onProduceResponse(b.cap.frames[1].corr, 120, 0);
    EXPECT_EQ(b.ps().lastAckedOffset, 120);
    b.impl.onProduceResponse(b.cap.frames[0].corr, 100, 0);
    EXPECT_EQ(b.ps().lastAckedOffset, 120) << "the max, not the latest";
    b.impl.onProduceResponse(b.cap.frames[2].corr, -1, 0);   // known-old duplicate: offset unknown
    EXPECT_EQ(b.ps().lastAckedOffset, 120) << "a known-old OK -1 must not move it";
    // A 3-record (Tier-2) batch acked at 200 -> its last offset 202.
    {
        rc::BatchBuilder bb(&b.impl, 1, 0);
        bb.add(kVal); bb.add(kVal); bb.add(kVal);
        ASSERT_EQ(bb.send(), rc::OpResult::Accepted);
    }
    pump(b.impl, b.sink);
    ASSERT_EQ(b.cap.frames.size(), 4u);
    ASSERT_EQ(b.cap.frames[3].records, 3);
    b.impl.onProduceResponse(b.cap.frames[3].corr, 200, 0);
    EXPECT_EQ(b.ps().lastAckedOffset, 202);
    EXPECT_EQ(b.A.failures(), 0);
}

// U1: every record this producer had acked is below the log start -> the
// producer state was legitimately removed: b and every batch behind it are
// re-sequenced (new epoch, from 0) and re-sent — nothing fails.
TEST(UnknownProducerIdRegression, U1RetentionResequencesWithoutFailingAnythingRegression) {
    Bare b(3);                                                   // seq 0, 1, 2 at epoch e
    const int16_t e = b.cap.frames[0].epoch;
    b.impl.onProduceResponse(b.cap.frames[0].corr, 100, 0);      // seq 0 acked at offset 100
    ASSERT_EQ(b.ps().lastAckedOffset, 100);
    // seq 1 was sent before seq 0's OK was processed: like C2 its answer may
    // predate that OK — it is re-sent first (same identity), not re-sequenced.
    b.impl.onProduceResponse(b.cap.frames[1].corr, 0, kUnknownPid, /*logStartOffset=*/500);
    pump(b.impl, b.sink);
    ASSERT_EQ(b.cap.frames.size(), 4u);
    EXPECT_EQ(b.cap.frames[3].epoch, e);
    EXPECT_EQ(b.cap.frames[3].seq, 1);
    EXPECT_EQ(b.ps().epoch, e);
    // The re-sent attempt's answer is UnknownProducerId again: now conclusive.
    b.impl.onProduceResponse(b.cap.frames[3].corr, 0, kUnknownPid, /*logStartOffset=*/500);
    pump(b.impl, b.sink);
    EXPECT_EQ(b.A.failures(), 0) << "U1 must not fail anything";
    ASSERT_EQ(b.cap.frames.size(), 6u) << "seq 1 and seq 2 re-sent";
    EXPECT_EQ(b.cap.frames[4].epoch, e + 1) << "rule F: a new epoch";
    EXPECT_EQ(b.cap.frames[4].seq, 0) << "re-sequenced from 0";
    EXPECT_EQ(b.cap.frames[5].epoch, e + 1);
    EXPECT_EQ(b.cap.frames[5].seq, 1);
    // A stale attempt's answer (seq 2's FIRST attempt, also 28) is ignored (rule A).
    b.impl.onProduceResponse(b.cap.frames[2].corr, 0, kUnknownPid, 500);
    pump(b.impl, b.sink);
    EXPECT_EQ(b.cap.frames.size(), 6u) << "a superseded attempt's error never re-sequences again";
    EXPECT_EQ(b.A.failures(), 0);
    EXPECT_EQ(b.ps().epoch, e + 1) << "no second bump";
    b.impl.onProduceResponse(b.cap.frames[4].corr, 600, 0);
    b.impl.onProduceResponse(b.cap.frames[5].corr, 601, 0);
    pump(b.impl, b.sink);
    ASSERT_EQ(b.A.acks.size(), 3u);
    for (const auto& a : b.A.acks) EXPECT_EQ(a.errorCode, 0);
    EXPECT_FALSE(b.impl.hasInFlight());
}

// U1 also when nothing was acked yet but the log start is past 0 (this
// producer's records were all removed before any of its acks arrived... or it
// is new to a partition whose head was retained away).
TEST(UnknownProducerIdRegression, U1NothingAckedLogStartPastZeroResequencesRegression) {
    Bare b(1);
    const int16_t e = b.cap.frames[0].epoch;
    b.impl.onProduceResponse(b.cap.frames[0].corr, 0, kUnknownPid, /*logStartOffset=*/10);
    pump(b.impl, b.sink);
    EXPECT_EQ(b.A.failures(), 0);
    ASSERT_EQ(b.cap.frames.size(), 2u);
    EXPECT_EQ(b.cap.frames[1].epoch, e + 1);
    EXPECT_EQ(b.cap.frames[1].seq, 0);
}

// U2 (D6 review M1): the broker should still know us (a record we had acked
// is at or above the log start) — it lost producer state.  b was provably NOT
// appended (any attempt of b would sit above lastAckedOffset >= logStart, in
// the rebuilt range, and would have created an entry): b and every batch
// behind it are re-sequenced (new epoch, from 0) and re-sent, exactly as U1 —
// nothing fails, the partition is not unresolved.  Counted as state loss.
TEST(UnknownProducerIdRegression, U2AckedAboveTheLogStartResequencesWithoutFailingRegression) {
    Bare b(3);
    const int16_t e = b.cap.frames[0].epoch;
    b.impl.onProduceResponse(b.cap.frames[0].corr, 100, 0);
    b.impl.onProduceResponse(b.cap.frames[1].corr, 0, kUnknownPid, /*logStartOffset=*/50);
    pump(b.impl, b.sink);                                        // C2: re-sent once (predates the OK)
    ASSERT_EQ(b.cap.frames.size(), 4u);
    b.impl.onProduceResponse(b.cap.frames[3].corr, 0, kUnknownPid, /*logStartOffset=*/50);
    pump(b.impl, b.sink);
    EXPECT_EQ(b.A.failures(), 0) << "a batch the broker provably did not append must not fail (Kafka KIP-360)";
    EXPECT_FALSE(b.ps().unresolved) << "nothing is in doubt";
    ASSERT_EQ(b.cap.frames.size(), 6u) << "seq 1 and seq 2 re-sent";
    EXPECT_EQ(b.cap.frames[4].epoch, e + 1) << "rule F: a new epoch";
    EXPECT_EQ(b.cap.frames[4].seq, 0);
    EXPECT_EQ(b.cap.frames[5].epoch, e + 1);
    EXPECT_EQ(b.cap.frames[5].seq, 1);
    EXPECT_EQ(b.impl.unknownProducerIdStateLoss(), 1u) << "acked data at/above the log start was lost (diagnostic)";
    b.impl.onProduceResponse(b.cap.frames[4].corr, 700, 0);
    b.impl.onProduceResponse(b.cap.frames[5].corr, 701, 0);
    pump(b.impl, b.sink);
    ASSERT_EQ(b.A.acks.size(), 3u);
    for (const auto& a : b.A.acks) EXPECT_EQ(a.errorCode, 0);
    EXPECT_FALSE(b.impl.hasInFlight());
}

TEST(UnknownProducerIdRegression, U2NothingAckedLogStartZeroResequencesRegression) {
    // Nothing acked and log start 0: any attempt of b would be at >= 0 = the
    // log start, inside the rebuilt range — a 28 proves it is not stored.
    Bare b(1);
    const int16_t e = b.cap.frames[0].epoch;
    b.impl.onProduceResponse(b.cap.frames[0].corr, 0, kUnknownPid, /*logStartOffset=*/0);
    pump(b.impl, b.sink);
    EXPECT_EQ(b.A.failures(), 0);
    ASSERT_EQ(b.cap.frames.size(), 2u);
    EXPECT_EQ(b.cap.frames[1].epoch, e + 1);
    EXPECT_EQ(b.cap.frames[1].seq, 0);
    EXPECT_EQ(b.impl.unknownProducerIdStateLoss(), 0u) << "nothing acked: no acked data was lost";
    b.impl.onProduceResponse(b.cap.frames[1].corr, 0, 0);
    pump(b.impl, b.sink);
    ASSERT_EQ(b.A.acks.size(), 1u);
    EXPECT_EQ(b.A.acks[0].errorCode, 0);
}

// Like C1: an older live batch of b's identity exists -> b is parked (its
// answer may be a gap in front of it), not re-sequenced; the oldest's own
// answer decides.
TEST(UnknownProducerIdRegression, OlderLiveBatchParksLikeC1Regression) {
    Bare b(2);
    const int16_t e = b.cap.frames[0].epoch;
    b.impl.onProduceResponse(b.cap.frames[1].corr, 0, kUnknownPid, 500);
    pump(b.impl, b.sink);
    EXPECT_EQ(b.A.failures(), 0);
    EXPECT_EQ(b.ps().epoch, e) << "parked, not re-sequenced, while seq 0 is unresolved";
    EXPECT_EQ(b.ps().parkedCount, 1);
    // The oldest's CURRENT attempt (a fast retransmit may have re-sent it).
    int64_t headCorr = 0;
    for (const Frame& f : b.cap.frames)
        if (f.seq == 0 && f.epoch == e) headCorr = f.corr;
    b.impl.onProduceResponse(headCorr, 0, kUnknownPid, 500);   // the oldest: U1
    pump(b.impl, b.sink);
    EXPECT_EQ(b.A.failures(), 0);
    EXPECT_EQ(b.ps().epoch, e + 1);
    ASSERT_GE(b.cap.frames.size(), 4u);
    EXPECT_EQ(b.cap.frames[b.cap.frames.size() - 2].seq, 0);
    EXPECT_EQ(b.cap.frames.back().seq, 1);
}

// Like C3: b is the oldest but a FAILED predecessor left a gap — that gap
// explains why the broker does not know us (its head was never written): b and
// the batches behind it are re-sequenced, not failed, even with log start 0.
TEST(UnknownProducerIdRegression, FailedPredecessorGapResequencesLikeC3Regression) {
    Bare b(2);
    const int16_t e = b.cap.frames[0].epoch;
    b.impl.onProduceResponse(b.cap.frames[0].corr, 0,
                             static_cast<int16_t>(rpc::ErrorCode::NotPartitionLeader));   // D: head fails
    b.impl.onProduceResponse(b.cap.frames[1].corr, 0, kUnknownPid, /*logStartOffset=*/0);
    pump(b.impl, b.sink);
    ASSERT_EQ(b.A.acks.size(), 1u) << "only the head failed";
    ASSERT_EQ(b.cap.frames.size(), 3u);
    EXPECT_EQ(b.cap.frames[2].epoch, e + 1);
    EXPECT_EQ(b.cap.frames[2].seq, 0);
}

// The client announces a version that understands UnknownProducerId.
TEST(UnknownProducerIdRegression, ClientAnnouncesTheUnknownProducerIdVersionRegression) {
    rc::session::SessionConfig cfg;
    EXPECT_EQ(cfg.clientVersion, rpc::kClientVersion);
    EXPECT_TRUE(rpc::clientUnderstandsUnknownProducerId(cfg.clientVersion));
}

// ---------------------------------------------------------------------------
// Part 2 — end to end: a real Client (Invoker mode, real Aeron IPC) against the
// InvokerGateway broker model: retention + failover -> the producer continues
// with no spurious failure.
// ---------------------------------------------------------------------------
namespace {

int     g_ctr = 0;
int64_t g_now = 0;
int64_t fakeNow() noexcept { return g_now; }
int64_t realNow() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
}
struct DirSweeper {
    std::vector<std::string> dirs;
    ~DirSweeper() {
        for (const auto& d : dirs) {
            std::error_code ec;
            std::filesystem::remove_all(d, ec);
        }
    }
};
DirSweeper g_sweeper;

rc::ClientConfig invokerCfg(int32_t in, int32_t eg) {
    rc::ClientConfig c;
    c.gatewayChannel  = "aeron:ipc";
    c.gatewayStreamId = in;
    c.egressChannel   = "aeron:ipc";
    c.egressStreamId  = eg;
    c.dataDir = std::string("/tmp/d6_upid_") + std::to_string(::getpid()) + "_" + std::to_string(g_ctr++);
    g_sweeper.dirs.push_back(c.dataDir);
    c.runnerMode = rc::RunnerMode::Invoker;
    c.nanoClock  = &fakeNow;
    g_now        = realNow();
    rc::TopicSpec ts;
    ts.name           = "t";
    ts.topicId        = 1;
    ts.partitionCount = 1;
    c.topics.push_back(ts);
    return c;
}

std::vector<uint8_t> idValue(uint64_t id) {
    std::vector<uint8_t> v(8);
    std::memcpy(v.data(), &id, 8);
    return v;
}
uint64_t idOf(const std::vector<uint8_t>& v) {
    uint64_t id = ~uint64_t{0};
    if (v.size() >= 8) std::memcpy(&id, v.data(), 8);
    return id;
}
std::vector<uint64_t> loggedIds(const test_support::InvokerGateway& gw) {
    std::vector<uint64_t> ids;
    for (const auto& b : gw.log)
        for (const auto& v : b.values) ids.push_back(idOf(v));
    return ids;
}

struct Rig {
    rc::Client                   client;
    test_support::InvokerGateway gw;
    explicit Rig(const rc::ClientConfig& c)
        : client(c), gw(c.dataDir + "/aeron", c.gatewayChannel, c.gatewayStreamId, c.egressChannel,
                        c.egressStreamId) {}
    void step() {
        client.doWork();
        gw.pump();
    }
    template <class P>
    bool drive(P&& pred) {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
        while (std::chrono::steady_clock::now() < deadline) {
            step();
            if (pred()) return true;
        }
        return false;
    }
    void connect() {
        ASSERT_TRUE(drive([&] { return gw.ready(); }));
        rc::Completions cq(client);
        ASSERT_EQ(client.connect(cq, 1), rc::OpResult::Accepted);
        bool         done = false;
        rc::OpStatus st   = rc::OpStatus::Closed;
        ASSERT_TRUE(drive([&] {
            cq.poll([&](const rc::Completion& c, std::span<const uint8_t>) {
                if (c.token == 1) {
                    done = true;
                    st   = c.status;
                }
            });
            return done;
        }));
        ASSERT_EQ(st, rc::OpStatus::Ok);
        ASSERT_TRUE(drive([&] { return gw.metadataRequests > 0; }));
    }
    void send(rc::Producer& p, rc::TopicHandle topic, uint64_t id, rc::Token token) {
        const auto v = idValue(id);
        for (int i = 0; i < 100'000; ++i) {
            const rc::SendResult r = p.trySend(topic, rc::Key(id), std::span<const uint8_t>(v), rc::AckToken{token});
            if (r == rc::SendResult::Ok) return;
            ASSERT_EQ(r, rc::SendResult::BackPressured);
            step();
        }
        FAIL() << "record " << id << " never accepted";
    }
};

rc::ProducerConfig e2eProducer() {
    rc::ProducerConfig p;
    p.acks              = rc::Acks::Leader;
    p.lingerUs          = 0;
    p.batchRecords      = 1;
    p.maxInFlight       = 8;
    p.produceTimeoutMs  = 60'000;
    p.deliveryTimeoutMs = 120'000;
    return p;
}

} // namespace

// The D6 scenario end to end.  The producer writes 5 records (acked at
// 100..104).  Then retention removes all of them and the partition fails over
// (the new leader rebuilds its producer state from [logStart, LEO): this
// producer is gone).  The next 5 records: the first meets UnknownProducerId
// with logStartOffset 105 > lastAckedOffset 104 -> U1: re-sequenced under a new
// epoch from 0 and re-sent — every record Ok exactly once, nothing fails.
// RED (pre-D6): the broker answered OutOfOrderSequence and rule C4 failed the
// next-in-line record (BrokerError 20) — a spurious failure.
TEST(UnknownProducerIdE2ERegression, RetentionPlusFailoverProducerContinuesWithoutSpuriousFailureRegression) {
    Rig r(invokerCfg(4060, 4061));
    r.connect();
    EXPECT_EQ(r.gw.lastConnectClientVersion, rpc::kClientVersion) << "the client announces it understands 28";
    r.gw.idempotence = true;
    AckLedger    L;
    rc::Producer p = r.client.producer(e2eProducer());
    L.attach(p);
    p.onBatchAck(&AckLedger::onBatch, &L);
    const rc::TopicHandle topic = r.client.topic("t");
    for (int i = 0; i < 5; ++i) r.send(p, topic, static_cast<uint64_t>(i), static_cast<rc::Token>(i + 1));
    ASSERT_TRUE(r.drive([&] { return L.count.size() >= 5; }));
    for (int i = 1; i <= 5; ++i) ASSERT_EQ(L.status(i), rc::OpStatus::Ok);

    r.gw.retainAndRebuild(r.gw.nextOffset);                  // retention + failover
    ASSERT_TRUE(r.gw.log.empty());
    ASSERT_EQ(r.gw.knownProducers(), 0u) << "precondition: the rebuilt state forgot this producer";

    for (int i = 5; i < 10; ++i) r.send(p, topic, static_cast<uint64_t>(i), static_cast<rc::Token>(i + 1));
    ASSERT_TRUE(r.drive([&] { return L.count.size() >= 10; })) << "completed " << L.count.size();
    for (int i = 0; i < 20; ++i) r.step();
    EXPECT_GT(r.gw.unknownPidAnswers, 0) << "precondition: the broker answered UnknownProducerId";
    EXPECT_TRUE(L.exactlyOnce(1, 11));
    for (int i = 1; i <= 10; ++i) EXPECT_EQ(L.status(i), rc::OpStatus::Ok) << "record " << i - 1 << " failed";
    for (const auto& a : L.batchAcks) EXPECT_EQ(a.errorCode, 0) << "batch seq " << a.baseSequence;
    EXPECT_EQ(loggedIds(r.gw), (std::vector<uint64_t>{5, 6, 7, 8, 9})) << "every record once, in order";
    ASSERT_FALSE(r.gw.log.empty());
    EXPECT_EQ(r.gw.log.front().baseSequence, 0) << "re-sequenced from 0";
    EXPECT_EQ(r.gw.log.front().producerEpoch, 1) << "under a new epoch";
    EXPECT_EQ(r.gw.crcFailures, 0);
    EXPECT_EQ(r.gw.identityMismatches, 0);
}

// The version gate seen from the client: a broker session that treats this
// client as OLD (clientVersion 1) keeps answering OutOfOrderSequence — the
// pre-D6 behaviour, where rule C4 fails the next-in-line batch.  (This is what
// "until both sides ship" means; with the version announced, Part 2 above.)
TEST(UnknownProducerIdE2ERegression, OldClientVersionKeepsTheOutOfOrderAnswerRegression) {
    Rig r(invokerCfg(4062, 4063));
    r.connect();
    r.gw.idempotence = true;
    for (auto& [sid, v] : r.gw.clientVersions) v = 1;          // the broker's view: an old client
    AckLedger    L;
    rc::Producer p = r.client.producer(e2eProducer());
    L.attach(p);
    const rc::TopicHandle topic = r.client.topic("t");
    for (int i = 0; i < 3; ++i) r.send(p, topic, static_cast<uint64_t>(i), static_cast<rc::Token>(i + 1));
    ASSERT_TRUE(r.drive([&] { return L.count.size() >= 3; }));
    r.gw.retainAndRebuild(r.gw.nextOffset);
    r.send(p, topic, 3, 4);
    ASSERT_TRUE(r.drive([&] { return L.count.size() >= 4; }));
    EXPECT_EQ(r.gw.unknownPidAnswers, 0) << "an old client never sees UnknownProducerId";
    EXPECT_GT(r.gw.oosAnswers, 0);
    EXPECT_EQ(L.status(4), rc::OpStatus::BrokerError) << "pre-D6: the spurious C4 failure";
}

// Loss, not retention: the state forgot this producer although its acked
// records are still at/above the log start (an unclean leader) -> U2 (D6
// review M1, Kafka parity): the batch was provably not appended, so it is
// re-sequenced and re-sent — every record Ok exactly once, no spurious failure.
TEST(UnknownProducerIdE2ERegression, LostProducerStateAboveTheLogStartResequencesExactlyOnceRegression) {
    Rig r(invokerCfg(4064, 4065));
    r.connect();
    r.gw.idempotence = true;
    AckLedger    L;
    rc::Producer p = r.client.producer(e2eProducer());
    L.attach(p);
    const rc::TopicHandle topic = r.client.topic("t");
    for (int i = 0; i < 3; ++i) r.send(p, topic, static_cast<uint64_t>(i), static_cast<rc::Token>(i + 1));
    ASSERT_TRUE(r.drive([&] { return L.count.size() >= 3; }));
    r.gw.forgetProducers();                                    // logStartOffset stays 0
    r.send(p, topic, 3, 4);
    ASSERT_TRUE(r.drive([&] { return L.count.size() >= 4; }));
    EXPECT_GT(r.gw.unknownPidAnswers, 0);
    EXPECT_EQ(L.status(4), rc::OpStatus::Ok) << "U2: provably not appended -> re-sequenced, not failed";
    r.send(p, topic, 4, 5);
    ASSERT_TRUE(r.drive([&] { return L.count.size() >= 5; }));
    for (int i = 0; i < 20; ++i) r.step();
    EXPECT_EQ(L.status(5), rc::OpStatus::Ok);
    EXPECT_TRUE(L.exactlyOnce(1, 6));
    EXPECT_EQ(loggedIds(r.gw), (std::vector<uint64_t>{0, 1, 2, 3, 4})) << "every record once, in order";
    EXPECT_EQ(r.gw.crcFailures, 0);
    EXPECT_EQ(r.gw.identityMismatches, 0);
}
