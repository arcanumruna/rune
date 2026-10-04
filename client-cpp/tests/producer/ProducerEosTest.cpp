// tests/producer/ProducerEosTest.cpp — EOS-OOS / EOS-DUP follow-ups.
//
// Idempotence end to end: an Invoker Client (or ClientShard) against the
// InvokerGateway's broker idempotence model (ProducerStateManager semantics on
// the FRAME identity + the EOS-OOS rulings: a known-old duplicate is answered
// OK with baseOffset -1) and its fault injection (a Produce frame lost on the
// wire, a ProduceResponse lost).  Plus bare-ProducerImpl checks of the
// idempotent in-flight clamp and the acks=None frame identity.
//
// Every test here uses only the public API and the bare ProducerImpl seams that
// predate the fixes, so the file builds against the pre-fix sources too (the
// RED runs).  Single-threaded: the test thread drives the client and the fake
// broker; the injected clock moves only where a test advances it.
#include "../support/AckLedger.h"
#include "../support/InvokerGateway.h"

#include <rune/client/Client.h>
#include <rune/client/codec/DataCodec.h>
#include <rune/client/producer/BatchBuilder.h>
#include <rune/client/producer/ProducerImpl.h>
#include <rune/client/shard/ClientShard.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/protocol/client/Protocol.h>

#include <gtest/gtest.h>

#include <chrono>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <map>
#include <set>
#include <span>
#include <string>
#include <unistd.h>
#include <vector>

namespace rc = rune::client;
namespace rs = rune::client::shard;
namespace rcc = rune::client::codec;
using test_support::AckLedger;

namespace {

int     g_ctr = 0;  // test thread only
int64_t g_now = 0;  // injected duty clock (the test thread is the only thread)
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

std::string freshDir(const char* tag) {
    std::string d = std::string("/tmp/eos_") + tag + "_" + std::to_string(::getpid()) + "_" + std::to_string(g_ctr++);
    g_sweeper.dirs.push_back(d);
    return d;
}

rc::ClientConfig invokerCfg(int32_t in, int32_t eg, int32_t partitions = 1) {
    rc::ClientConfig c;
    c.gatewayChannel  = "aeron:ipc";
    c.gatewayStreamId = in;
    c.egressChannel   = "aeron:ipc";
    c.egressStreamId  = eg;
    c.dataDir         = freshDir("client");
    c.runnerMode      = rc::RunnerMode::Invoker;
    c.nanoClock       = &fakeNow;
    g_now             = realNow();
    rc::TopicSpec ts;
    ts.name           = "t";
    ts.topicId        = 1;
    ts.partitionCount = partitions;
    c.topics.push_back(ts);
    return c;
}

// A key that maps to `partition` (of `partitions`).
rc::Key keyFor(int32_t partition, int32_t partitions) {
    for (uint64_t k = 0; k < 1'000'000; ++k) {
        if (rc::Key(k).partition(partitions) == partition) return rc::Key(k);
    }
    return rc::Key(uint64_t{0});
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

// The record ids of one partition as written to the gateway's log, log order.
std::vector<uint64_t> loggedIds(const test_support::InvokerGateway& gw, int32_t partition) {
    std::vector<uint64_t> ids;
    for (const auto& b : gw.log) {
        if (b.partition != partition) continue;
        for (const auto& v : b.values) ids.push_back(idOf(v));
    }
    return ids;
}

struct Rig {
    rc::Client                   client;  // constructed first: owns the driver
    test_support::InvokerGateway gw;      // destroyed first
    explicit Rig(const rc::ClientConfig& c)
        : client(c), gw(c.dataDir + "/aeron", c.gatewayChannel, c.gatewayStreamId, c.egressChannel,
                        c.egressStreamId) {}

    void step(int64_t advanceNs = 0) {
        g_now += advanceNs;
        client.doWork();
        gw.pump();
    }
    template <class P>
    bool drive(P&& pred, int64_t advanceNs = 0) {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
        while (std::chrono::steady_clock::now() < deadline) {
            step(advanceNs);
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
    // One record per batch (batchRecords = 1): the Direct builder takes the next
    // record only after a duty pass flushed the previous one.
    void sendRecord(rc::Producer& p, rc::TopicHandle topic, uint64_t id, rc::Token token) {
        sendRecordKeyed(p, topic, rc::Key(id), id, token);
    }
    void sendRecordKeyed(rc::Producer& p, rc::TopicHandle topic, const rc::Key& key, uint64_t id, rc::Token token) {
        const auto v = idValue(id);
        for (int i = 0; i < 100'000; ++i) {
            const rc::SendResult r = p.trySend(topic, key, std::span<const uint8_t>(v), rc::AckToken{token});
            if (r == rc::SendResult::Ok) return;
            ASSERT_EQ(r, rc::SendResult::BackPressured);
            step();
        }
        FAIL() << "record " << id << " never accepted";
    }
};

rc::ProducerConfig eosProducer(int32_t maxInFlight, int64_t produceTimeoutMs, rc::Acks acks = rc::Acks::Leader) {
    rc::ProducerConfig p;
    p.acks              = acks;
    p.lingerUs          = 0;
    p.batchRecords      = 1;
    p.maxInFlight       = maxInFlight;
    p.produceTimeoutMs  = produceTimeoutMs;
    p.deliveryTimeoutMs = 30'000;
    return p;
}

} // namespace

// ---------------------------------------------------------------------------
// EOS-OOS item 3 (Kafka parity): a frame lost mid-window makes the broker
// answer OutOfOrderSequence to every later in-flight batch.  Those batches are
// NOT the oldest in flight — the lost one is — so they must be retried once it
// resolves, never failed to the app (the k3s gates saw them reported FAILED
// while the records ended up stored, or not, at random) and without an epoch
// bump.  RED (pre-fix): every batch behind the lost frame completes
// BrokerError(20) and the producer bumps its epoch.
// ---------------------------------------------------------------------------
TEST(EosOosRegression, LostFrameMidWindowIsRetriedNotFailedRegression) {
    Rig r(invokerCfg(3960, 3961));
    r.connect();
    r.gw.idempotence = true;
    r.gw.dropProduce = {3};  // the 3rd Produce frame (sequence 2) never arrives
    // produceTimeoutMs >> the test: the clock never moves, so recovery cannot
    // come from the retransmit timer — only from the OOS answers themselves.
    AckLedger    L;  // outlives the producer (its close completes into L)
    rc::Producer p = r.client.producer(eosProducer(8, 60'000));
    L.attach(p);
    p.onBatchAck(&AckLedger::onBatch, &L);
    const rc::TopicHandle topic = r.client.topic("t");
    constexpr int kN = 10;
    for (int i = 0; i < kN; ++i) r.sendRecord(p, topic, static_cast<uint64_t>(i), static_cast<rc::Token>(i + 1));
    ASSERT_TRUE(r.drive([&] { return static_cast<int>(L.count.size()) >= kN; }))
        << "completed " << L.count.size() << " of " << kN;
    r.step();
    ASSERT_GT(r.gw.oosAnswers, 0) << "precondition: the lost frame made the broker answer OutOfOrderSequence";
    EXPECT_TRUE(L.exactlyOnce(1, kN + 1));
    for (int i = 1; i <= kN; ++i) {
        EXPECT_EQ(L.status(i), rc::OpStatus::Ok) << "record " << i - 1 << " must not be failed (OOS behind a lost frame)";
        EXPECT_GE(L.value(i), 100) << "record " << i - 1;
    }
    EXPECT_EQ(L.batchAckedRecords(), kN);
    for (const auto& a : L.batchAcks) EXPECT_EQ(a.errorCode, 0) << "batch seq " << a.baseSequence;
    std::vector<uint64_t> want;
    for (int i = 0; i < kN; ++i) want.push_back(static_cast<uint64_t>(i));
    EXPECT_EQ(loggedIds(r.gw, 0), want) << "every record exactly once, in order";
    for (const auto& b : r.gw.log) EXPECT_EQ(b.producerEpoch, 0) << "no epoch bump (seq " << b.baseSequence << ")";
}

// ---------------------------------------------------------------------------
// EOS-OOS item 2: a retransmit of a batch the broker already wrote, older than
// its dedup window, is answered OK with baseOffset -1 (Kafka DUPLICATE_SEQUENCE
// parity).  It is a SUCCESS with an unknown offset: every record completes Ok
// (-1) exactly once, its BatchAck is errorCode 0 / baseOffset -1, and the
// producer neither fails anything nor bumps its epoch.  RED (pre-fix): the
// agent turned the OK -1 into the "-errorCode" sentinel -1 = error 1, so the
// record failed BrokerError(1) and the epoch was bumped.  Tier-1 records and
// Tier-2 batches (the Tier-2 handle) both.
// ---------------------------------------------------------------------------
TEST(EosOosRegression, KnownOldDuplicateCompletesOkOnceWithUnknownOffsetRegression) {
    for (const bool tier2 : {false, true}) {
        SCOPED_TRACE(tier2 ? "tier-2" : "tier-1");
        Rig r(invokerCfg(tier2 ? 3964 : 3962, tier2 ? 3965 : 3963));
        r.connect();
        r.gw.idempotence       = true;
        r.gw.idempotenceWindow = 2;    // small window: the first batch falls out of it
        r.gw.dropAnswer        = {1};  // the first batch is written, its answer is lost
        AckLedger    L;  // outlives the producer (its close completes into L)
        rc::Producer p = r.client.producer(eosProducer(8, /*produceTimeoutMs=*/50));
        L.attach(p);
        p.onBatchAck(&AckLedger::onBatch, &L);
        const rc::TopicHandle topic = r.client.topic("t");
        constexpr int kN = 6;
        for (int i = 0; i < kN; ++i) {
            if (!tier2) {
                r.sendRecord(p, topic, static_cast<uint64_t>(i), static_cast<rc::Token>(i + 1));
                continue;
            }
            rc::BatchBuilder bb = p.batch(topic, rc::Key(uint64_t{0}));
            const auto       v  = idValue(static_cast<uint64_t>(i));
            bb.add(std::span<const uint8_t>(v));
            ASSERT_EQ(bb.send(rc::AckToken{static_cast<rc::Token>(i + 1)}), rc::OpResult::Accepted);
            r.step();
        }
        // Batches 2..6 are answered at once; batch 1 waits for its retransmit.
        ASSERT_TRUE(r.drive([&] { return static_cast<int>(L.count.size()) >= kN - 1; }));
        EXPECT_EQ(L.times(1), 0);
        // Past produceTimeoutMs: batch 1 is retransmitted -> known-old duplicate.
        ASSERT_TRUE(r.drive([&] { return L.times(1) > 0; }, /*advanceNs=*/1'000'000));
        r.step();
        ASSERT_GE(r.gw.oldDupAnswers, 1) << "precondition: answered as a known-old duplicate";
        EXPECT_TRUE(L.exactlyOnce(1, kN + 1));
        EXPECT_EQ(L.status(1), rc::OpStatus::Ok) << "a stored record must never be reported failed";
        EXPECT_EQ(L.value(1), -1) << "offset unknown";
        for (int i = 2; i <= kN; ++i) {
            EXPECT_EQ(L.status(i), rc::OpStatus::Ok);
            EXPECT_GE(L.value(i), 100);
        }
        ASSERT_EQ(L.batchAcks.size(), static_cast<std::size_t>(kN));
        for (const auto& a : L.batchAcks) {
            EXPECT_EQ(a.errorCode, 0) << "batch seq " << a.baseSequence;
            if (a.baseSequence == 0) EXPECT_EQ(a.baseOffset, -1);
        }
        // No epoch bump: the next batch continues the sequence in epoch 0 and is written.
        if (!tier2) {
            r.sendRecord(p, topic, kN, kN + 1);
        } else {
            rc::BatchBuilder bb = p.batch(topic, rc::Key(uint64_t{0}));
            const auto       v  = idValue(kN);
            bb.add(std::span<const uint8_t>(v));
            ASSERT_EQ(bb.send(rc::AckToken{kN + 1}), rc::OpResult::Accepted);
        }
        ASSERT_TRUE(r.drive([&] { return L.times(kN + 1) > 0; }));
        EXPECT_EQ(L.status(kN + 1), rc::OpStatus::Ok);
        std::vector<uint64_t> want;
        for (int i = 0; i <= kN; ++i) want.push_back(static_cast<uint64_t>(i));
        EXPECT_EQ(loggedIds(r.gw, 0), want) << "every record exactly once";
        for (const auto& b : r.gw.log) EXPECT_EQ(b.producerEpoch, 0) << "no epoch bump (seq " << b.baseSequence << ")";
        p.closeImmediately();
    }
}

// ---------------------------------------------------------------------------
// The shard ack path (ClientShard, Invoker: BatchAcks through the shard-owned
// Completions, decoded with batchAckOf) under both faults at once: a frame lost
// mid-window (OOS behind it -> retried) and a lost answer whose retransmit is a
// known-old duplicate (OK, baseOffset -1).  Every record is acked Ok exactly
// once; nothing fails.  RED (pre-fix): the OOS batches and the duplicate fail.
// ---------------------------------------------------------------------------
TEST(EosOosRegression, ShardBatchAcksSurviveLostFrameAndLostAnswerRegression) {
    rs::ShardGroupConfig g;
    g.base.gatewayChannel  = "aeron:ipc";
    g.base.gatewayStreamId = 3966;
    g.base.egressChannel   = "aeron:ipc";
    g.base.egressStreamId  = 3967;
    g.base.dataDir         = freshDir("shard");
    g.base.nanoClock       = &fakeNow;
    g_now                  = realNow();
    rc::TopicSpec ts;
    ts.name           = "t";
    ts.topicId        = 1;
    ts.partitionCount = 1;
    g.base.topics.push_back(ts);
    g.shardCount = 1;
    g.mode       = rs::ShardMode::Invoker;
    g.producer   = eosProducer(8, /*produceTimeoutMs=*/50);

    std::map<int32_t, int>       seqAcked;  // baseSequence -> times acked Ok
    int64_t                      ok = 0, failed = 0;
    int                          unknownOffset = 0;
    rs::ClientShard              shard(g, 0);  // owns the driver: constructed before the gateway
    const rc::ClientConfig       cc = rs::shardClientConfig(g, 0);
    test_support::InvokerGateway gw(cc.dataDir + "/aeron", g.base.gatewayChannel, g.base.gatewayStreamId,
                                    cc.egressChannel, g.base.egressStreamId);
    auto step = [&](int64_t adv = 0) {
        g_now += adv;
        shard.doWork();
        gw.pump();
        shard.pollBatchAcks([&](const rc::BatchAck& a) {
            if (a.errorCode != 0) {
                failed += a.recordCount;
                return;
            }
            ok += a.recordCount;
            ++seqAcked[a.baseSequence];
            if (a.baseOffset < 0) ++unknownOffset;
        });
        shard.pollEvents([](const rs::ShardStatus&) {});
    };
    auto drive = [&](auto pred, int64_t adv = 0) {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
        while (std::chrono::steady_clock::now() < deadline) {
            step(adv);
            if (pred()) return true;
        }
        return false;
    };
    ASSERT_TRUE(drive([&] { return gw.ready(); }));
    {
        rc::Completions cq(shard.client());
        ASSERT_EQ(shard.connect(cq, 1), rc::OpResult::Accepted);
        bool done = false;
        ASSERT_TRUE(drive([&] {
            cq.poll([&](const rc::Completion& c, std::span<const uint8_t>) { done = done || c.token == 1; });
            return done;
        }));
        ASSERT_TRUE(drive([&] { return gw.metadataRequests > 0; }));
    }
    gw.idempotence       = true;
    gw.idempotenceWindow = 2;
    gw.dropAnswer        = {1};  // written; answer lost -> retransmit = known-old duplicate
    gw.dropProduce       = {4};  // lost on the wire -> OOS behind it
    const rc::TopicHandle topic = shard.topic("t");
    constexpr int kN = 8;
    for (int i = 0; i < kN; ++i) {
        const auto v = idValue(static_cast<uint64_t>(i));
        bool       sent = false;
        for (int k = 0; k < 100'000 && !sent; ++k) {
            const rc::SendResult r = shard.trySend(topic, rc::Key(uint64_t{0}), std::span<const uint8_t>(v));
            if (r == rc::SendResult::Ok) {
                sent = true;
            } else {
                ASSERT_EQ(r, rc::SendResult::BackPressured);
                step();
            }
        }
        ASSERT_TRUE(sent);
        step();
    }
    ASSERT_TRUE(drive([&] { return ok + failed >= kN; }, /*advanceNs=*/1'000'000))
        << "ok=" << ok << " failed=" << failed;
    step();
    EXPECT_GT(gw.oosAnswers, 0) << "precondition: the lost frame made the broker answer OutOfOrderSequence";
    EXPECT_GE(gw.oldDupAnswers, 1) << "precondition: the lost answer's retransmit is a known-old duplicate";
    EXPECT_EQ(failed, 0);
    EXPECT_EQ(ok, kN);
    EXPECT_EQ(unknownOffset, 1);
    for (int s = 0; s < kN; ++s) EXPECT_EQ(seqAcked[s], 1) << "sequence " << s << " acked exactly once";
    std::vector<uint64_t> want;
    for (int i = 0; i < kN; ++i) want.push_back(static_cast<uint64_t>(i));
    EXPECT_EQ(loggedIds(gw, 0), want) << "every record exactly once, in order";
    for (const auto& b : gw.log) EXPECT_EQ(b.producerEpoch, 0) << "no epoch bump (seq " << b.baseSequence << ")";
    shard.closeImmediately();
}

// ---------------------------------------------------------------------------
// Bare ProducerImpl (no Aeron): frames captured from the sink.
// ---------------------------------------------------------------------------
namespace {

constexpr int64_t kNow = 1'000'000'000;

struct FrameId {
    int64_t frameProducerId;
    int16_t frameEpoch;
    int32_t frameSeq;
    int64_t headerProducerId;
    int16_t headerEpoch;
    int32_t headerSeq;
};
struct Capture {
    std::vector<FrameId> frames;
    rc::producer::ProducerImpl::SendFn fn() {
        return [this](int32_t, int32_t, int32_t, const uint8_t* b, std::size_t n) {
            const auto v = rcc::decodeProduce(b, n);
            if (!v) return false;
            rune::protocol::RecordBatchReader r(v->recordBatch.data(), v->recordBatch.size());
            frames.push_back({v->producerId, v->producerEpoch, v->baseSequence, r.producerId(), r.producerEpoch(),
                              r.baseSequence()});
            return true;
        };
    }
};

void pump(rc::producer::ProducerImpl& impl, const rc::producer::ProducerImpl::SendFn& sink) {
    for (int i = 0; i < 100'000; ++i) {
        const int d = impl.drainRing(kNow);
        impl.flushBatches(kNow, 42, sink);
        if (d == 0) break;
    }
}

const std::vector<uint8_t> kVal(16, 0x11);

rc::ProducerConfig bareCfg(rc::Acks acks, int32_t maxInFlight) {
    rc::ProducerConfig c;
    c.acks              = acks;
    c.lingerUs          = 0;
    c.maxInFlight       = maxInFlight;
    c.produceTimeoutMs  = 1'000;
    c.deliveryTimeoutMs = 10'000;
    return c;
}

} // namespace

// EOS-OOS item 1: an idempotent producer's per-partition in-flight window is
// clamped to the broker's dedup window (rune::protocol::client::
// kIdempotentMaxInFlightBatches = 32): a retransmit can then never be older
// than the window.  RED (pre-fix): maxInFlight 64 put 64 batches in flight.
// acks=None is not idempotent: not clamped (and has no in-flight window).
TEST(EosOosRegression, IdempotentMaxInFlightClampedToBrokerWindowRegression) {
    for (const rc::Acks acks : {rc::Acks::Leader, rc::Acks::All}) {
        constexpr int32_t kWindow = rune::protocol::client::kIdempotentMaxInFlightBatches;
        rc::producer::ProducerImpl impl(bareCfg(acks, 2 * kWindow));
        EXPECT_EQ(impl.config().maxInFlight, kWindow);
        Capture    cap;
        const auto sink = cap.fn();
        for (int i = 0; i < 2 * kWindow; ++i) {
            ASSERT_TRUE(impl.submitSendRecord(1, 0, rc::Key(uint64_t{1}), kVal, false, 0));
            pump(impl, sink);  // one record per batch
        }
        EXPECT_EQ(cap.frames.size(), static_cast<std::size_t>(kWindow))
            << "no more than the broker's dedup window outstanding";
    }
    rc::producer::ProducerImpl none(bareCfg(rc::Acks::None, 64));
    EXPECT_EQ(none.config().maxInFlight, 64) << "acks=None is not idempotent: never clamped";
    static_assert(rune::protocol::client::kIdempotentMaxInFlightBatches < 64);
    rc::producer::ProducerImpl small(bareCfg(rc::Acks::Leader, 8));
    EXPECT_EQ(small.config().maxInFlight, 8) << "a value inside the window is kept";
}

// Review I4 / m1: acks=None frames are non-idempotent on BOTH the frame and the
// batch header: producerId / producerEpoch / baseSequence are -1/-1/-1 in both,
// for Tier-1 and Tier-2.  RED (pre-fix): the frame's epoch was the Produce
// default 0 against the header's -1.
TEST(EosOosRegression, AcksNoneFrameIdentityIsMinusOneLikeTheHeaderRegression) {
    rc::producer::ProducerImpl impl(bareCfg(rc::Acks::None, 8));
    Capture    cap;
    const auto sink = cap.fn();
    ASSERT_TRUE(impl.submitSendRecord(1, 0, rc::Key(uint64_t{1}), kVal, false, 0));
    pump(impl, sink);
    {
        rc::BatchBuilder bb(&impl, 1, 0);
        bb.add(kVal);
        ASSERT_EQ(bb.send(), rc::OpResult::Accepted);
    }
    pump(impl, sink);
    ASSERT_EQ(cap.frames.size(), 2u);
    for (std::size_t i = 0; i < 2; ++i) {
        SCOPED_TRACE(i == 0 ? "tier-1" : "tier-2");
        const FrameId& f = cap.frames[i];
        EXPECT_EQ(f.frameProducerId, -1);
        EXPECT_EQ(f.frameEpoch, -1);
        EXPECT_EQ(f.frameSeq, -1);
        EXPECT_EQ(f.headerProducerId, -1);
        EXPECT_EQ(f.headerEpoch, -1);
        EXPECT_EQ(f.headerSeq, -1);
    }
}

// ===========================================================================
// EOS follow-ups fix round 1 (review eos-followups-review.md).  acks=All with
// the gateway's deferred OK answers (M-3): an error answer can overtake an
// earlier OK, as on a real broker.
// ===========================================================================

namespace {
// Produced entry (in arrival order, dropped frames excluded) for a baseSequence.
int producedWithSeq(const test_support::InvokerGateway& gw, int32_t partition, int32_t seq) {
    int n = 0;
    for (const auto& pr : gw.produced) n += (pr.partition == partition && pr.baseSequence == seq) ? 1 : 0;
    return n;
}
} // namespace

// I-2: an error answer of a SUPERSEDED attempt (B's first send got OOS while A
// was lost; the answer is delayed) must never fail B once B's later attempt may
// be stored.  A and B are both retransmitted; A is acked; THEN B's first OOS
// arrives — B is now the oldest batch.  RED (pre-fix: one id for every attempt):
// B completes BrokerError(20) and the epoch is bumped, although B's
// retransmit is stored (the log has it).
TEST(EosOosRegression, ErrorOfSupersededAttemptNeverFailsTheBatchRegression) {
    Rig r(invokerCfg(3970, 3971));
    r.connect();
    r.gw.idempotence  = true;
    r.gw.okDelayPumps = 3;        // acks=All: OKs are deferred, errors are not
    r.gw.dropProduce  = {1};      // A's first frame is lost
    r.gw.holdAnswer   = {2, 4};   // B's first answer (OOS) and B's retransmit's answer
    AckLedger    L;
    rc::Producer p = r.client.producer(eosProducer(8, /*produceTimeoutMs=*/50, rc::Acks::All));
    L.attach(p);
    const rc::TopicHandle topic = r.client.topic("t");
    r.sendRecord(p, topic, 0, 1);  // A
    r.sendRecord(p, topic, 1, 2);  // B
    // Past produceTimeoutMs both are retransmitted (A first): frames 3 (A), 4 (B).
    ASSERT_TRUE(r.drive([&] { return r.gw.produceFrames >= 4; }, /*advanceNs=*/1'000'000));
    ASSERT_EQ(producedWithSeq(r.gw, 0, 1), 2) << "precondition: frames 2 and 4 are B's";
    ASSERT_TRUE(r.drive([&] { return L.times(1) > 0; }));  // A's retransmit acked (deferred OK)
    ASSERT_EQ(L.status(1), rc::OpStatus::Ok);
    r.gw.releaseAnswer(2);  // B's FIRST attempt's OOS arrives now, after A resolved
    for (int i = 0; i < 50; ++i) r.step();
    EXPECT_EQ(L.times(2), 0) << "an error of a superseded attempt must be ignored";
    r.gw.releaseAnswer(4);
    ASSERT_TRUE(r.drive([&] { return L.times(2) > 0; }));
    EXPECT_EQ(L.status(2), rc::OpStatus::Ok) << "B is stored: it must not be reported failed";
    EXPECT_EQ(loggedIds(r.gw, 0), (std::vector<uint64_t>{0, 1}));
    for (const auto& b : r.gw.log) EXPECT_EQ(b.producerEpoch, 0) << "no epoch bump";
}

// I-2: an OOS of the CURRENT attempt that was sent before the predecessor's ack
// arrived may predate that ack (answers reorder: errors are immediate, acks=all
// OKs are deferred, routes differ).  It is retried, not taken as a rejection of
// the next-in-line batch.  RED (pre-fix): B fails BrokerError(20).
TEST(EosOosRegression, OosSentBeforeThePredecessorsAckIsRetriedRegression) {
    Rig r(invokerCfg(3972, 3973));
    r.connect();
    r.gw.idempotence  = true;
    r.gw.okDelayPumps = 3;
    r.gw.dropProduce  = {1};  // A's first frame is lost
    r.gw.holdAnswer   = {2};  // B's OOS is delayed past A's ack
    AckLedger    L;
    rc::Producer p = r.client.producer(eosProducer(8, /*produceTimeoutMs=*/50, rc::Acks::All));
    L.attach(p);
    const rc::TopicHandle topic = r.client.topic("t");
    r.sendRecord(p, topic, 0, 1);  // A at t0
    r.step(30'000'000);
    r.sendRecord(p, topic, 1, 2);  // B at t0 + 30 ms: its timer fires after A's
    ASSERT_TRUE(r.drive([&] { return r.gw.produceFrames >= 3; }, /*advanceNs=*/1'000'000));  // A retransmitted
    ASSERT_TRUE(r.drive([&] { return L.times(1) > 0; }));  // frozen clock: B is not retransmitted
    ASSERT_EQ(L.status(1), rc::OpStatus::Ok);
    ASSERT_EQ(producedWithSeq(r.gw, 0, 1), 1) << "precondition: B was sent once";
    r.gw.releaseAnswer(2);
    ASSERT_TRUE(r.drive([&] { return L.times(2) > 0; }));
    EXPECT_EQ(L.status(2), rc::OpStatus::Ok);
    EXPECT_EQ(loggedIds(r.gw, 0), (std::vector<uint64_t>{0, 1}));
    for (const auto& b : r.gw.log) EXPECT_EQ(b.producerEpoch, 0) << "no epoch bump";
}

// I-4: a failure on partition 0 must not fence an already-STORED retransmit on
// partition 1.  X (P1) is written but its answer is lost; Y (P0) is rejected;
// Z (P1) is then sent; X's retransmit follows.  Kafka: the epoch bump of P0's
// unresolved sequence never moves P1, whose in-flight batches finish under the
// epoch they were sent with.  RED (pre-fix: producer-wide immediate bump): Z
// goes out at epoch 1 seq 0, the broker moves P1 to epoch 1, X's retransmit is
// fenced (InvalidProducerEpoch) and X is reported FAILED although stored.
TEST(EosOosRegression, EpochBumpNeverFencesAStoredRetransmitOfAnotherPartitionRegression) {
    Rig r(invokerCfg(3974, 3975, /*partitions=*/2));
    r.connect();
    r.gw.idempotence = true;
    r.gw.dropAnswer  = {1};                                                     // X: stored, answer lost
    r.gw.errorFor    = {{2, static_cast<int16_t>(rcc::ErrorCode::NotPartitionLeader)}};  // Y rejected
    AckLedger    L;
    rc::Producer p = r.client.producer(eosProducer(8, /*produceTimeoutMs=*/50));
    L.attach(p);
    const rc::TopicHandle topic = r.client.topic("t");
    const rc::Key k0 = keyFor(0, 2), k1 = keyFor(1, 2);
    r.sendRecordKeyed(p, topic, k1, 10, 1);  // X on P1
    r.sendRecordKeyed(p, topic, k0, 20, 2);  // Y on P0
    ASSERT_TRUE(r.drive([&] { return L.times(2) > 0; }));
    ASSERT_EQ(L.status(2), rc::OpStatus::BrokerError) << "precondition: Y rejected";
    r.sendRecordKeyed(p, topic, k1, 11, 3);  // Z on P1
    ASSERT_TRUE(r.drive([&] { return L.times(3) > 0; }));
    EXPECT_EQ(L.status(3), rc::OpStatus::Ok);
    // X's retransmit (past produceTimeoutMs).
    ASSERT_TRUE(r.drive([&] { return L.times(1) > 0; }, /*advanceNs=*/1'000'000));
    EXPECT_EQ(L.status(1), rc::OpStatus::Ok) << "X is stored: an epoch bump must not fail it";
    EXPECT_EQ(r.gw.fencedAnswers, 0);
    EXPECT_EQ(loggedIds(r.gw, 1), (std::vector<uint64_t>{10, 11})) << "X and Z exactly once";
}

// I-4: the batches parked behind a head the broker REJECTED (OutOfOrderSequence
// on the next-in-line batch: its sequences were never written) are
// re-sequenced under a new epoch (sequence 0..) and re-sent — not failed
// (Kafka: requestEpochBumpForPartition + startSequencesAtBeginning).  Only the
// head fails.  RED (pre-fix): F1 and F2 fail too.
TEST(EosOosRegression, FollowersOfARejectedHeadAreResequencedNotFailedRegression) {
    Rig r(invokerCfg(3976, 3977));
    r.connect();
    r.gw.idempotence = true;
    // H (epoch 0, seq 0) is rejected on every attempt; its first answer arrives
    // after F1's and F2's.
    r.gw.errorIf = [](int32_t part, int16_t epoch, int32_t seq) -> int16_t {
        return (part == 0 && epoch == 0 && seq == 0) ? static_cast<int16_t>(rcc::ErrorCode::OutOfOrderSequence) : 0;
    };
    r.gw.holdAnswer = {1};
    AckLedger    L;
    rc::Producer p = r.client.producer(eosProducer(8, /*produceTimeoutMs=*/60'000));
    L.attach(p);
    const rc::TopicHandle topic = r.client.topic("t");
    r.sendRecord(p, topic, 0, 1);  // H
    r.sendRecord(p, topic, 1, 2);  // F1
    r.sendRecord(p, topic, 2, 3);  // F2
    // F1, F2 met the gap (D6: a never-written head leaves no producer entry, so
    // a current client gets UnknownProducerId there — the same "not appended").
    ASSERT_TRUE(r.drive([&] { return r.gw.oosAnswers + r.gw.unknownPidAnswers >= 3; }));
    for (int i = 0; i < 20; ++i) r.step();
    r.gw.releaseAnswer(1);
    ASSERT_TRUE(r.drive([&] { return L.count.size() >= 3; })) << "completed " << L.count.size();
    for (int i = 0; i < 20; ++i) r.step();
    EXPECT_EQ(L.status(1), rc::OpStatus::BrokerError) << "the rejected head fails";
    EXPECT_EQ(L.status(2), rc::OpStatus::Ok) << "F1 is re-sequenced and re-sent, not failed";
    EXPECT_EQ(L.status(3), rc::OpStatus::Ok) << "F2 is re-sequenced and re-sent, not failed";
    EXPECT_TRUE(L.exactlyOnce(1, 4));
    EXPECT_EQ(loggedIds(r.gw, 0), (std::vector<uint64_t>{1, 2}));
    ASSERT_EQ(r.gw.log.size(), 2u);
    EXPECT_EQ(r.gw.log[0].producerEpoch, 1);
    EXPECT_EQ(r.gw.log[0].baseSequence, 0) << "re-sequenced from 0 under the new epoch";
    EXPECT_EQ(r.gw.log[1].baseSequence, 1);
    // NEW-2: the re-stamped frames (frame + header identity, CRC) are broker-valid.
    EXPECT_EQ(r.gw.crcFailures, 0);
    EXPECT_EQ(r.gw.identityMismatches, 0);
}

// I-4: same for a head that FAILS by its delivery timeout (lost on every
// attempt): once the parked followers meet the gap as the oldest batches, the
// gap is proven (the head was never written) — they are re-sequenced and
// written, not failed.  RED (pre-fix): they fail BrokerError(20).
TEST(EosOosRegression, FollowersOfATimedOutHeadAreResequencedNotFailedRegression) {
    Rig r(invokerCfg(3978, 3979));
    r.connect();
    r.gw.idempotence = true;
    r.gw.dropIf = [](int32_t part, int16_t epoch, int32_t seq) { return part == 0 && epoch == 0 && seq == 0; };
    AckLedger          L;
    rc::ProducerConfig pc = eosProducer(8, /*produceTimeoutMs=*/50);
    pc.deliveryTimeoutMs  = 500;
    rc::Producer p = r.client.producer(pc);
    L.attach(p);
    const rc::TopicHandle topic = r.client.topic("t");
    r.sendRecord(p, topic, 0, 1);  // H: never arrives
    r.step();                      // H's first send at t0
    r.step(20'000'000);
    r.sendRecord(p, topic, 1, 2);  // F1
    r.sendRecord(p, topic, 2, 3);  // F2
    ASSERT_TRUE(r.drive([&] { return L.times(1) > 0; }, /*advanceNs=*/1'000'000));
    EXPECT_EQ(L.status(1), rc::OpStatus::DeliveryFailed);
    ASSERT_TRUE(r.drive([&] { return L.count.size() >= 3; })) << "completed " << L.count.size();
    EXPECT_EQ(L.status(2), rc::OpStatus::Ok) << "F1 re-sequenced, not failed";
    EXPECT_EQ(L.status(3), rc::OpStatus::Ok) << "F2 re-sequenced, not failed";
    EXPECT_EQ(loggedIds(r.gw, 0), (std::vector<uint64_t>{1, 2}));
    EXPECT_EQ(r.gw.crcFailures, 0);  // NEW-2: the re-stamped frames are broker-valid
    EXPECT_EQ(r.gw.identityMismatches, 0);
}

// ===========================================================================
// Re-review 1, NEW-2: the broker model checks what the real broker checks
// before dedup — the RecordBatch CRC32C (ProducePath step 4 -> Internal) and
// the Produce FRAME identity against the CRC-covered batch HEADER identity
// (step 4b -> SchemaValidationFailed) — and the frames of every resend path
// pass both.
// ===========================================================================
namespace {
// One idempotent Produce frame: the frame carries (fPid, fEpoch, fSeq), the
// batch header (hPid, hEpoch, hSeq).
std::vector<uint8_t> rawProduce(int64_t sessionId, int64_t corrId, int64_t fPid, int16_t fEpoch, int32_t fSeq,
                                int64_t hPid, int16_t hEpoch, int32_t hSeq, uint64_t id) {
    rune::protocol::RecordBatchBuilder b(0, 1'000'000);
    b.setProducerId(hPid);
    b.setProducerEpoch(hEpoch);
    b.setBaseSequence(hSeq);
    const auto v = idValue(id);
    b.append(std::string_view("k"), std::string_view(reinterpret_cast<const char*>(v.data()), v.size()), 1'000'000);
    const std::vector<uint8_t> batch = b.build();
    rune::protocol::client::Produce msg;
    msg.sessionId      = sessionId;
    msg.correlationId  = corrId;
    msg.topicId        = 1;
    msg.partition      = 0;
    msg.partitionKnown = 1;
    msg.producerId     = fPid;
    msg.producerEpoch  = fEpoch;
    msg.baseSequence   = fSeq;
    msg.recordBatch    = std::span<const uint8_t>(batch);
    std::vector<uint8_t> out(msg.encodedSize());
    out.resize(msg.encode(out.data()));
    return out;
}
void expectBrokerValid(const test_support::InvokerGateway& gw) {
    EXPECT_EQ(gw.crcFailures, 0) << "a frame failed the broker's CRC32C check";
    EXPECT_EQ(gw.identityMismatches, 0) << "a frame's identity differs from its batch header's";
}
} // namespace

// The model itself.  RED (pre-fix model): every one of the three bad frames was
// appended (log size 4) and none was counted.
TEST(EosFrameValidityRegression, BrokerModelRejectsCrcAndIdentityMismatchRegression) {
    Rig r(invokerCfg(3980, 3981));
    r.connect();
    r.gw.idempotence = true;
    const int64_t sid = r.gw.assignedSessionId;
    // Frame sequence 1 vs header sequence 0.
    auto seqMismatch = rawProduce(sid, 11, 7, 0, 1, 7, 0, 0, 1);
    r.gw.injectFrame(0, seqMismatch.data(), seqMismatch.size());
    // The EOS-DUP signature: frame pid -1, header pid 7.
    auto pidMismatch = rawProduce(sid, 12, -1, 0, -1, 7, 0, 0, 2);
    r.gw.injectFrame(0, pidMismatch.data(), pidMismatch.size());
    // Consistent identity, one record byte flipped (CRC no longer matches).
    auto corrupt = rawProduce(sid, 13, 7, 0, 0, 7, 0, 0, 3);
    corrupt[corrupt.size() - 2] ^= 0x5A;
    r.gw.injectFrame(0, corrupt.data(), corrupt.size());
    EXPECT_EQ(r.gw.identityMismatches, 2);
    EXPECT_EQ(r.gw.crcFailures, 1);
    EXPECT_TRUE(r.gw.log.empty()) << "an invalid frame was appended";
    // A valid frame is still appended.
    auto good = rawProduce(sid, 14, 7, 0, 0, 7, 0, 0, 4);
    r.gw.injectFrame(0, good.data(), good.size());
    ASSERT_EQ(r.gw.log.size(), 1u);
    EXPECT_EQ(loggedIds(r.gw, 0), (std::vector<uint64_t>{4}));
}

// Epoch move (an unresolved head: its outcome unknown -> the partition resets to
// a new epoch at 0 once nothing is in flight): the next batch, encoded under the
// new identity, passes both checks and is written.
TEST(EosFrameValidityRegression, EpochMoveFramesAreBrokerValidRegression) {
    Rig r(invokerCfg(3982, 3983));
    r.connect();
    r.gw.idempotence = true;
    r.gw.errorFor    = {{1, static_cast<int16_t>(rcc::ErrorCode::NotPartitionLeader)}};  // D: outcome unknown
    AckLedger    L;
    rc::Producer p = r.client.producer(eosProducer(8, /*produceTimeoutMs=*/50));
    L.attach(p);
    const rc::TopicHandle topic = r.client.topic("t");
    r.sendRecord(p, topic, 0, 1);
    ASSERT_TRUE(r.drive([&] { return L.times(1) > 0; }));
    ASSERT_EQ(L.status(1), rc::OpStatus::BrokerError);
    r.sendRecord(p, topic, 1, 2);
    r.sendRecord(p, topic, 2, 3);
    ASSERT_TRUE(r.drive([&] { return L.times(2) > 0 && L.times(3) > 0; }));
    EXPECT_EQ(L.status(2), rc::OpStatus::Ok);
    EXPECT_EQ(L.status(3), rc::OpStatus::Ok);
    ASSERT_EQ(r.gw.log.size(), 2u);
    EXPECT_EQ(r.gw.log[0].producerEpoch, 1) << "precondition: the partition moved to a new epoch";
    EXPECT_EQ(r.gw.log[0].baseSequence, 0);
    expectBrokerValid(r.gw);
}

// Overflow epoch move (sequenceRoom_): before the int32 counter would pass
// INT32_MAX the partition moves to a new epoch at 0; every frame on both sides
// of the move passes both checks and every record is written once.
TEST(EosFrameValidityRegression, OverflowEpochMoveFramesAreBrokerValidRegression) {
    Rig r(invokerCfg(3984, 3985));
    r.connect();
    r.gw.idempotence = true;
    AckLedger    L;
    rc::Producer p = r.client.producer(eosProducer(8, /*produceTimeoutMs=*/50));
    L.attach(p);
    const rc::TopicHandle topic = r.client.topic("t");
    r.sendRecord(p, topic, 0, 1);
    ASSERT_TRUE(r.drive([&] { return L.times(1) > 0; }));
    ASSERT_EQ(r.gw.log.size(), 1u);
    const int64_t pid = r.gw.log[0].producerId;
    // The broker already holds this producer up to INT32_MAX - 2.
    r.gw.seedSequence(1, pid, 0, 0, INT32_MAX - 2);
    p.testSetNextSequence(topic, 0, INT32_MAX - 1);
    for (int i = 1; i <= 3; ++i) r.sendRecord(p, topic, static_cast<uint64_t>(i), static_cast<rc::Token>(i + 1));
    ASSERT_TRUE(r.drive([&] { return L.count.size() >= 4; }));
    EXPECT_TRUE(L.exactlyOnce(1, 5));
    for (int t = 1; t <= 4; ++t) EXPECT_EQ(L.status(t), rc::OpStatus::Ok) << t;
    EXPECT_EQ(loggedIds(r.gw, 0), (std::vector<uint64_t>{0, 1, 2, 3}));
    ASSERT_EQ(r.gw.log.size(), 4u);
    EXPECT_EQ(r.gw.log[1].baseSequence, INT32_MAX - 1);
    EXPECT_EQ(r.gw.log[2].producerEpoch, 1) << "precondition: the epoch moved before the overflow";
    EXPECT_EQ(r.gw.log[2].baseSequence, 0);
    expectBrokerValid(r.gw);
}
