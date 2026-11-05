// CheckedWritesAutoIncWorkerTest.cpp - checked writes Part 06 Task 9: autoincrement on a real Worker.  The counter is
// activated and rebuilt by the leadership scan, a key-0 create gets dense ids above every reservation, a retried key-0
// batch answers its original ids from the side window, and rejected or aborted ids are burned, never reused.
#include "CheckedWritesRig.h"

#include <rune/node/RcState.h>
#include <rune/storage/InMemoryLog.h>

#include <spdlog/sinks/ringbuffer_sink.h>
#include <spdlog/spdlog.h>

#include <algorithm>
#include <cerrno>
#include <numeric>

using namespace rune::node;
using namespace rune::node::cw;
using rune::protocol::client::ErrorCode;

namespace {
namespace pc = rune::protocol::client;
constexpr uint8_t kAll = pc::kAcksWireAll;
constexpr uint8_t kAllView = pc::kAcksWireView | pc::kAcksWireAll;
constexpr int16_t kNotLeader = static_cast<int16_t>(ErrorCode::NotLeader);
constexpr int16_t kInvalidRequest = static_cast<int16_t>(ErrorCode::InvalidRequest);
constexpr int16_t kSchemaValidationFailed = static_cast<int16_t>(ErrorCode::SchemaValidationFailed);
constexpr int16_t kNotAuthorized = static_cast<int16_t>(ErrorCode::NotAuthorized);
constexpr int16_t kDuplicateSequence = static_cast<int16_t>(ErrorCode::DuplicateSequence);
constexpr uint16_t kConstraintViolation = 55, kKeyExists = 56;
constexpr int32_t kRej = 0x40000601, kProj = 0x40000602;
constexpr uint64_t kRejObj = 77;
using V = std::vector<std::tuple<uint16_t, uint16_t, uint32_t>>;

/// A key-0 create (the broker assigns the id), an import of `id`, an update, a removeExisting, a plain upsert.
Rec create(uint8_t tag = 0) { return Rec{0, 1, tag, false, 1}; }
Rec import(uint64_t id, uint8_t tag = 0) { return Rec{id, 1, tag, false, 1}; }
Rec upd(uint64_t id) { return Rec{id, 2, 0, false, 2}; }
Rec rmEx(uint64_t id) { return Rec{id, 0, 0, true, 2}; }
Rec ups(uint64_t id) { return Rec{id, 1, 0, false, 0}; }
std::vector<Rec> creates(int k) { return std::vector<Rec>(static_cast<std::size_t>(k), create()); }

TestRejectRule tagRule(uint8_t tag) { TestRejectRule r; r.mode = TestRejectRule::Mode::TagEquals; r.tag = tag; return r; }

/// An idempotent producer: every frame continues its sequence.
struct Producer {
    VerdictRig& r;
    int64_t     pid;
    uint8_t     acks = kAllView;
    bool        fetching = false;   ///< the rig has a follower: the acks need its fetches
    int32_t     seq = 0;

    int64_t send(const std::vector<Rec>& recs) {
        const int64_t c = r.produceFrame(recs, acks, pid, seq);
        seq += static_cast<int32_t>(recs.size());
        return c;
    }
    /// The same sequence again: nothing is advanced (a refused frame, or a retry).
    int64_t resend(const std::vector<Rec>& recs, int32_t atSeq) { return r.produceFrame(recs, acks, pid, atSeq); }
    std::optional<VerdictRig::Ack> ackOf(int64_t c) { return fetching ? r.ackFetching(c) : r.ack(c); }
    std::optional<VerdictRig::Ack> sendAck(const std::vector<Rec>& recs) { return ackOf(send(recs)); }
    /// `k` creates in one batch, acked OK: their ids.
    std::vector<uint64_t> createIds(int k, int32_t partitions = 1) {
        const auto a = sendAck(creates(k));
        EXPECT_TRUE(a && a->code == 0) << "code " << (a ? a->code : -99);
        std::vector<uint64_t> ids;
        if (!a) return ids;
        EXPECT_NE(a->assignedBase, 0u);
        for (int i = 0; i < k; ++i) ids.push_back(a->assignedBase + static_cast<uint64_t>(i) * static_cast<uint64_t>(partitions));
        return ids;
    }
};

/// An autoincrement topic of `shape` with acks to a v17 session.  `rollBytes`: the log's segment size (compact() needs small ones).
struct AutoInc {
    VerdictRig r;
    explicit AutoInc(AutoIncShape shape = {}, bool follower = false, int64_t rollBytes = 64LL * 1024 * 1024) : r(follower, rollBytes) {
        r.createAutoIncTopic(shape);
        r.enableAcks();
        r.passes(20);
    }
};

/// Records' ids in the log, in offset order (data records only).
std::vector<uint64_t> loggedIds(const VerdictRig& r) {
    std::vector<uint64_t> out;
    for (const auto& d : r.readLog().data) out.push_back(d.id);
    return out;
}

/// One batch with an explicit wire key and value key field (the rig's batch() writes the same id into both).
std::vector<uint8_t> rawBatch(uint64_t keyId, uint64_t valueId, uint8_t intent, int64_t pid, int32_t seq) {
    rune::protocol::RecordBatchBuilder b(0, 1000);
    b.setProducerId(pid); b.setProducerEpoch(0); b.setBaseSequence(seq);
    std::vector<uint8_t> key(8), val;
    for (int i = 0; i < 8; ++i) key[std::size_t(i)] = static_cast<uint8_t>(keyId >> (8 * i));
    const auto le16 = [&](uint16_t x) { val.push_back(uint8_t(x)); val.push_back(uint8_t(x >> 8)); };
    le16(18); le16(1);
    for (int i = 0; i < 8; ++i) val.push_back(static_cast<uint8_t>(valueId >> (8 * i)));
    for (int i = 0; i < 8; ++i) val.push_back(i == 0 ? 1 : 0);
    le16(0);
    b.append({reinterpret_cast<const char*>(key.data()), key.size()}, {reinterpret_cast<const char*>(val.data()), val.size()}, 1000, intent);
    return b.build();
}

/// Captures spdlog output for the scope's lifetime.
struct LogCapture {
    std::shared_ptr<spdlog::sinks::ringbuffer_sink_mt> sink = std::make_shared<spdlog::sinks::ringbuffer_sink_mt>(512);
    std::shared_ptr<spdlog::logger> prev = spdlog::default_logger();
    LogCapture() {
        auto lg = std::make_shared<spdlog::logger>("capture", sink);
        lg->set_level(spdlog::level::warn);
        lg->set_pattern("%l %v");
        spdlog::set_default_logger(lg);
    }
    ~LogCapture() { spdlog::set_default_logger(prev); }
    [[nodiscard]] std::size_t count(const std::string& needle) const {
        std::size_t n = 0;
        for (const auto& l : sink->last_formatted()) n += l.find(needle) != std::string::npos ? 1 : 0;
        return n;
    }
};
}  // namespace

TEST(CheckedWritesAutoIncWorker, DenseIdsFromOne) {
    AutoInc g;
    VerdictRig& r = g.r;
    Producer p{r, 42};
    std::vector<uint64_t> ids;
    for (int k = 1; ids.size() < 100; k = k % 8 + 1) {
        const auto got = p.createIds(std::min<int>(k, 100 - static_cast<int>(ids.size())));
        ASSERT_FALSE(got.empty());
        ids.insert(ids.end(), got.begin(), got.end());
    }
    std::vector<uint64_t> want(100);
    std::iota(want.begin(), want.end(), 1);
    EXPECT_EQ(ids, want);
    EXPECT_EQ(loggedIds(r), want) << "the log holds the patched ids";
    const auto resv = r.reservations();
    ASSERT_EQ(resv.size(), 1u);
    EXPECT_EQ(resv[0].second, 4096u);
    EXPECT_LT(resv[0].first, r.readLog().data.front().offset) << "the reservation precedes the first data batch";
    ASSERT_TRUE(r.autoIncState());
    EXPECT_EQ(r.autoIncState()->nextN, 101u) << "the counter id";
    EXPECT_EQ(r.autoIncState()->reservedThroughN, 4096u);
    EXPECT_EQ(r.autoIncStats().idsBurned, 0u);
    EXPECT_EQ(r.autoIncStats().reservationsAppended, 1u);
}

TEST(CheckedWritesAutoIncWorker, IdsAreNeverReusedAfterADelete) {
    AutoInc g;
    VerdictRig& r = g.r;
    Producer p{r, 42};
    const auto ids = p.createIds(10);
    ASSERT_EQ(ids.back(), 10u);
    const auto a = p.sendAck({rmEx(10)});
    ASSERT_TRUE(a);
    EXPECT_EQ(a->code, 0);
    EXPECT_TRUE(a->verdicts.empty());
    EXPECT_EQ(p.createIds(1), std::vector<uint64_t>{11});
}

TEST(CheckedWritesAutoIncWorker, AllOnlyCreateOnAKeyIndexOnlyTopicIsAcked) {
    AutoInc g;
    VerdictRig& r = g.r;
    Producer p{r, 42, kAll};
    const auto a = p.sendAck(creates(3));
    ASSERT_TRUE(a);
    EXPECT_EQ(a->code, 0);
    EXPECT_EQ(a->assignedBase, 1u);
    EXPECT_FALSE(a->hasVerdicts) << "an All ack carries no trailer";
}

// P6-7: until the leadership scan ends the partition assigns nothing; the first id after it is above every earlier one.  The RC
// produce gate (an idempotent batch waits for the same scan) answers NotLeader before the write hook's rule 8d is reached.
TEST(CheckedWritesAutoIncWorker, ActivationAnswersNotLeaderUntilTheScanEnds) {
    AutoInc g;
    VerdictRig& r = g.r;
    Producer p{r, 42};
    uint64_t maxId = 0;
    for (int i = 0; i < 24; ++i) maxId = std::max(maxId, p.createIds(2).back());
    r.rcScanBytesPerPass = 1;   // one batch per pass
    r.restart();
    ASSERT_FALSE(r.autoIncState()) << "precondition: the scan is still running";
    const auto refused = p.ackOf(p.resend(creates(1), p.seq));
    ASSERT_TRUE(refused);
    EXPECT_EQ(refused->code, kNotLeader);
    EXPECT_EQ(r.readLog().data.size(), 48u) << "nothing appended";
    for (int i = 0; i < VerdictRig::kMaxPass && !r.autoIncState(); ++i) r.pass();
    ASSERT_TRUE(r.autoIncState());
    const auto ids = p.createIds(1);
    ASSERT_EQ(ids.size(), 1u);
    EXPECT_GT(ids[0], maxId);
}

TEST(CheckedWritesAutoIncRegression, FailoverAroundAReservationResumesAboveItRegression) {
    const AutoIncShape shape{.reserve = 8};
    {   // (a) just before: R 8, next 9
        AutoInc g(shape);
        VerdictRig& r = g.r;
        Producer p{r, 42};
        ASSERT_EQ(p.createIds(8).back(), 8u);
        r.failover();
        ASSERT_TRUE(r.autoIncState());
        EXPECT_EQ(r.autoIncState()->nextN, 9u);
        EXPECT_EQ(r.autoIncStats().idsBurned, 0u);
        EXPECT_EQ(p.createIds(1), std::vector<uint64_t>{9});
        ASSERT_EQ(r.reservations().size(), 2u);
        EXPECT_EQ(r.reservations().back().second, 16u);
    }
    {   // (b) just after: the crossing batch takes 9 and appends R 16, both committed
        AutoInc g(shape);
        VerdictRig& r = g.r;
        Producer p{r, 42};
        ASSERT_EQ(p.createIds(8).back(), 8u);
        EXPECT_EQ(p.createIds(1), std::vector<uint64_t>{9});
        ASSERT_EQ(r.reservations().back().second, 16u);
        r.failover();
        ASSERT_TRUE(r.autoIncState());
        EXPECT_EQ(r.autoIncState()->nextN, 17u) << "R + 1";
        EXPECT_EQ(r.autoIncStats().idsBurned, 7u) << "16 - 9";
        EXPECT_EQ(p.createIds(1), std::vector<uint64_t>{17});
    }
    {   // (c) the reservation committed, its data not: the data is cut, the counter still resumes above R
        AutoInc g(shape, /*follower=*/true);
        VerdictRig& r = g.r;
        Producer p{r, 42, kAllView, /*fetching=*/true};
        ASSERT_EQ(p.createIds(8).back(), 8u);
        (void)p.send(creates(1));
        r.passes(3);
        const auto resv = r.reservations();
        ASSERT_EQ(resv.size(), 2u);
        ASSERT_EQ(resv.back().second, 16u);
        r.followerFetch(resv.back().first + 1);
        ASSERT_EQ(r.replica->highWatermark(), resv.back().first + 1);
        r.failover();
        EXPECT_EQ(loggedIds(r).back(), 8u) << "id 9's batch is cut";
        ASSERT_TRUE(r.autoIncState());
        EXPECT_EQ(r.autoIncState()->nextN, 17u);
        EXPECT_EQ(r.autoIncStats().idsBurned, 8u) << "16 - 8";
        p.seq -= 1;   // the cut batch was never acked: the producer resends its sequence
        EXPECT_EQ(p.createIds(1), std::vector<uint64_t>{17});
    }
}

TEST(CheckedWritesAutoIncRegression, TruncatedReservationTakesItsDataWithItRegression) {
    AutoInc g(AutoIncShape{.reserve = 8}, /*follower=*/true);
    VerdictRig& r = g.r;
    Producer p{r, 42, kAllView, /*fetching=*/true};
    ASSERT_EQ(p.createIds(8).back(), 8u);
    const int64_t hw = r.replica->highWatermark();
    const int32_t seq = p.seq;
    const int64_t lost = p.send(creates(4));
    r.passes(3);
    ASSERT_EQ(r.reservations().back().second, 16u);
    ASSERT_GT(r.leo(), hw);
    ASSERT_LE(hw, r.reservations().back().first) << "R 16 and its data are above the HW";
    r.failover();   // a leader's unreplicated tail is cut only by the next leadership
    ASSERT_EQ(r.leo(), hw);
    ASSERT_TRUE(r.autoIncState());
    EXPECT_EQ(r.autoIncState()->nextN, 9u) << "R 16 went with the cut: the counter resumes at R 8 + 1";
    const auto a = p.ackOf(p.resend(creates(4), seq));
    ASSERT_TRUE(a);
    EXPECT_EQ(a->code, 0);
    EXPECT_EQ(a->assignedBase, 9u) << "ids 9-12 again: they were never acked";
    EXPECT_FALSE(r.acked(lost) && r.ack(lost, 1)->code == 0) << "the cut batch is never acked OK";
    EXPECT_EQ(r.autoIncStats().idsBurned, 0u);
    std::size_t r16 = 0;
    for (const auto& [off, n] : r.reservations()) r16 += n == 16 ? 1 : 0;
    EXPECT_EQ(r16, 1u) << "exactly one R 16";
    EXPECT_EQ(loggedIds(r), (std::vector<uint64_t>{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}));
}

// Task 9 fix round 1 (finding 4): the old leader's own worker, holding Ready{13, R 16} and the cut batch's producer state, follows
// the new leader's history and leads again: the counter is rebuilt from the cut log, never from its memory.
TEST(CheckedWritesAutoIncRegression, SameWorkerTruncationRebuildsTheCounterFromTheCutLogRegression) {
    AutoInc g(AutoIncShape{.reserve = 8}, /*follower=*/true);
    VerdictRig& r = g.r;
    Producer p{r, 42, kAllView, /*fetching=*/true};
    ASSERT_EQ(p.createIds(8).back(), 8u);
    const int64_t hw = r.replica->highWatermark();
    const int32_t seq = p.seq;
    (void)p.send(creates(4));
    r.passes(3);
    ASSERT_EQ(r.reservations().back().second, 16u);
    ASSERT_TRUE(r.autoIncState());
    ASSERT_EQ(r.autoIncState()->nextN, 13u) << "precondition: the memory holds the cut batch's ids";
    r.demoteByOp();
    r.truncateReplicaTo(hw);
    ASSERT_EQ(r.leo(), hw);
    r.promote();
    for (int i = 0; i < VerdictRig::kMaxPass && !r.autoIncState(); ++i) r.pass();
    ASSERT_TRUE(r.autoIncState());
    EXPECT_EQ(r.autoIncState()->nextN, 9u) << "R 8 + 1: R 16 went with the cut";
    const auto a = p.ackOf(p.resend(creates(4), seq));
    ASSERT_TRUE(a);
    EXPECT_EQ(a->code, 0);
    EXPECT_EQ(a->assignedBase, 9u) << "ids 9-12 again";
    EXPECT_EQ(r.autoIncState()->nextN, 13u);
    std::size_t r16 = 0;
    for (const auto& [off, n] : r.reservations()) r16 += n == 16 ? 1 : 0;
    EXPECT_EQ(r16, 1u) << "R 16 re-appended once";
    EXPECT_EQ(loggedIds(r), (std::vector<uint64_t>{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}));
    ASSERT_TRUE(r.driveUntilJudged());
    EXPECT_EQ(r.autoIncStats().assignedKeyExists, 0u);
}

// P6-7: a demoted partition assigns nothing until its next leadership scan rebuilds the counter.
TEST(CheckedWritesAutoIncRegression, BecomeFollowerDeactivatesTheCounterRegression) {
    AutoInc g;
    VerdictRig& r = g.r;
    Producer p{r, 42};
    ASSERT_EQ(p.createIds(3).back(), 3u);
    r.demoteByOp();
    EXPECT_FALSE(r.autoIncState()) << "a follower's counter is not Ready";
    r.promote();
    for (int i = 0; i < VerdictRig::kMaxPass && !r.autoIncState(); ++i) r.pass();
    ASSERT_TRUE(r.autoIncState());
    EXPECT_GT(p.createIds(1).front(), 3u);
}

// Review Focus 2.
TEST(CheckedWritesAutoIncRegression, ImportAboveTheReservationThenFailoverNeverReusesRegression) {
    AutoInc g;
    VerdictRig& r = g.r;
    Producer p{r, 42};
    ASSERT_EQ(p.createIds(3).back(), 3u);
    const auto imp = p.sendAck({import(5000)});
    ASSERT_TRUE(imp);
    EXPECT_EQ(imp->code, 0);
    EXPECT_TRUE(imp->verdicts.empty());
    EXPECT_EQ(imp->assignedBase, 0u) << "an import answers 0 (P6-3)";
    ASSERT_TRUE(r.autoIncState());
    EXPECT_GE(r.autoIncState()->reservedThroughN, 5000u);
    EXPECT_EQ(r.autoIncState()->nextN, 5001u);
    r.failover();
    const auto afterFailover = p.createIds(4);
    ASSERT_EQ(afterFailover.size(), 4u);
    EXPECT_GT(afterFailover.front(), 5000u);
    r.demotePromote();
    const auto afterPromote = p.createIds(4);
    ASSERT_EQ(afterPromote.size(), 4u);
    EXPECT_GT(afterPromote.front(), afterFailover.back());
    ASSERT_TRUE(r.driveUntilJudged());
    EXPECT_EQ(r.autoIncStats().assignedKeyExists, 0u);
}

// R04e: a key-0 batch whose ack was lost, retried with the same producer, epoch and sequence, answers its original offsets and ids.
TEST(CheckedWritesAutoIncRegression, LostAckRetryReturnsTheSameIdsRegression) {
    enum class Run { ViewParked, ViewIndexed, All };
    for (const Run run : {Run::ViewParked, Run::ViewIndexed, Run::All}) {
        SCOPED_TRACE(static_cast<int>(run));
        AutoInc g;
        VerdictRig& r = g.r;
        Producer p{r, 42, run == Run::All ? kAll : kAllView};
        ASSERT_EQ(p.createIds(2).back(), 2u);
        if (run == Run::ViewParked) r.holdJudge(true);
        const int32_t seq = p.seq;
        const int64_t first = p.send(creates(4));
        r.passes(3);
        const int64_t leo = r.leo();
        if (run != Run::ViewParked) ASSERT_TRUE(p.ackOf(first));
        const uint64_t replaysBefore = r.stats() ? r.stats()->verdictReplays : 0;
        const int64_t retry = p.resend(creates(4), seq);
        r.passes(3);
        EXPECT_EQ(r.leo(), leo) << "the retry appends nothing";
        if (run == Run::ViewParked) {
            EXPECT_FALSE(r.acked(retry)) << "parked behind the judge";
            r.holdJudge(false);
        }
        const auto a = p.ackOf(first), b = p.ackOf(retry);
        ASSERT_TRUE(a && b);
        EXPECT_EQ(a->code, 0);
        EXPECT_EQ(a->assignedBase, 3u);
        EXPECT_EQ(b->code, a->code);
        EXPECT_EQ(b->baseOffset, a->baseOffset);
        EXPECT_EQ(b->assignedBase, a->assignedBase);
        EXPECT_EQ(b->hasVerdicts, a->hasVerdicts);
        EXPECT_EQ(b->verdicts, a->verdicts);
        EXPECT_EQ(b->frameLen, a->frameLen) << "the same frame";
        if (run != Run::All) {
            ASSERT_NE(r.stats(), nullptr);
            EXPECT_EQ(r.stats()->verdictReplays, replaysBefore + 1);
        }
        EXPECT_EQ(r.autoIncStats().windowMisses, 0u);
        for (int i = 0; i < 33; ++i) ASSERT_TRUE(p.sendAck({create()}));
        const auto old = p.ackOf(p.resend(creates(4), seq));
        ASSERT_TRUE(old);
        if (run == Run::All) {
            // An All ack has no DuplicateSequence answer: a known-old duplicate is OK with offset -1 (StoredIdUnknown on the client).
            EXPECT_EQ(old->code, 0);
            EXPECT_EQ(old->baseOffset, -1);
        } else {
            EXPECT_EQ(old->code, kDuplicateSequence);
        }
        EXPECT_EQ(old->assignedBase, 0u);
        EXPECT_EQ(r.autoIncStats().windowMisses, 0u) << "a known-old duplicate is not a window miss";
    }
}

// Review Focus 1: the ack is lost, the leader fails over (memory gone), the retry answers the original ids from the rebuilt window.
TEST(CheckedWritesAutoIncRegression, RetryAfterFailoverReturnsTheIdsFromTheRebuiltWindowRegression) {
    AutoInc g;
    VerdictRig& r = g.r;
    Producer p{r, 42};
    ASSERT_EQ(p.createIds(1).back(), 1u);
    const int32_t seq = p.seq;
    const auto orig = p.sendAck(creates(4));
    ASSERT_TRUE(orig);
    ASSERT_EQ(orig->assignedBase, 2u);
    const auto retryMatches = [&](const char* when) {
        const auto a = p.ackOf(p.resend(creates(4), seq));
        ASSERT_TRUE(a) << when;
        EXPECT_EQ(a->code, 0) << when;
        EXPECT_EQ(a->baseOffset, orig->baseOffset) << when;
        EXPECT_EQ(a->assignedBase, 2u) << when;
        EXPECT_EQ(r.autoIncStats().windowMisses, 0u) << when;
    };
    r.failover();
    retryMatches("after a failover");
    for (int i = 0; i < 31; ++i) ASSERT_TRUE(p.sendAck({create()}));
    r.failover();
    retryMatches("31 batches later, after a failover");
    r.restart();
    retryMatches("after a restart");
    EXPECT_EQ(r.leo(), r.replica->highWatermark());
}

// Review Focus 4: each raw shape is refused definitely, nothing appended, the counter unchanged.
TEST(CheckedWritesAutoIncRegression, RawAutoIncShapesAreRefusedAndTheCounterHoldsRegression) {
    const auto refused = [](VerdictRig& r, int64_t corr, int16_t code, const char* what) {
        const auto before = r.autoIncState();
        const int64_t leo = r.leo();
        const auto a = r.ack(corr);
        ASSERT_TRUE(a) << what;
        EXPECT_EQ(a->code, code) << what;
        EXPECT_EQ(r.leo(), leo) << what << ": nothing appended";
        ASSERT_TRUE(before && r.autoIncState()) << what;
        EXPECT_EQ(r.autoIncState()->nextN, before->nextN) << what;
        EXPECT_EQ(r.autoIncState()->reservedThroughN, before->reservedThroughN) << what;
    };
    {
        AutoInc g;
        VerdictRig& r = g.r;
        Producer p{r, 42};
        ASSERT_EQ(p.createIds(3).back(), 3u);
        refused(r, r.produceBytes(rawBatch(0, 7, 1, 42, p.seq), kAllView, 42, p.seq), kSchemaValidationFailed, "key 0, value id 7");
        refused(r, p.resend({ups(3)}, p.seq), kInvalidRequest, "a plain upsert");
        refused(r, p.resend({create(), upd(1)}, p.seq), kInvalidRequest, "a mixed key-0 batch");
        refused(r, r.produceFrame(creates(1), /*acks=*/0, 42, p.seq), kInvalidRequest, "a create with acks 0");
        refused(r, r.produceFrame(creates(1), kAllView), kInvalidRequest, "a create from a non-idempotent producer");
        EXPECT_EQ(r.worker->hookRefusalsForTest(HookRefusal::UpsertOnAutoIncrement), 1u);
        EXPECT_EQ(r.worker->hookRefusalsForTest(HookRefusal::MixedKeyZeroBatch), 1u);
        EXPECT_EQ(r.worker->hookRefusalsForTest(HookRefusal::KeyZeroCreateNeedsIdempotentAll), 2u);
        EXPECT_EQ(p.createIds(1), std::vector<uint64_t>{4}) << "the counter held";
    }
    {
        AutoInc g(AutoIncShape{.partitions = 2});
        VerdictRig& r = g.r;
        Producer p{r, 42};
        ASSERT_EQ(p.createIds(1, 2).back(), 2u) << "partition 0 of 2: ids 2, 4, ...";
        refused(r, p.resend({import(3)}, p.seq), kInvalidRequest, "an import of id 3 on P 2 / p 0");
        EXPECT_EQ(r.worker->hookRefusalsForTest(HookRefusal::ImportOutOfRange), 1u);
    }
    {
        AutoInc g(AutoIncShape{.keyType = rune::codec::PrimType::Int64});
        VerdictRig& r = g.r;
        Producer p{r, 42};
        ASSERT_EQ(p.createIds(1).back(), 1u);
        refused(r, p.resend({import(static_cast<uint64_t>(int64_t{-1}))}, p.seq), kInvalidRequest, "an int64 import of -1");
        EXPECT_EQ(r.worker->hookRefusalsForTest(HookRefusal::ImportOutOfRange), 1u);
    }
}

TEST(CheckedWritesAutoIncWorker, IdOfARejectedCreateIsBurned) {
    AutoInc g;
    VerdictRig& r = g.r;
    r.installRejectView(kRej, kRejObj, tagRule(9));
    r.passes(40);
    Producer p{r, 42};
    const auto a = p.sendAck({create(1), create(9), create(1)});
    ASSERT_TRUE(a);
    EXPECT_EQ(a->code, 0);
    ASSERT_EQ(a->verdicts.size(), 1u);
    EXPECT_EQ(std::get<0>(a->verdicts[0]), 1);
    EXPECT_EQ(std::get<1>(a->verdicts[0]), kConstraintViolation);
    EXPECT_EQ(r.autoIncStats().idsBurned, 1u);
    EXPECT_EQ(p.createIds(1), std::vector<uint64_t>{a->assignedBase + 3});
    const auto imp = p.sendAck({import(1000, 9)});
    ASSERT_TRUE(imp);
    EXPECT_EQ(imp->verdicts.size(), 1u);
    EXPECT_EQ(std::get<1>(imp->verdicts.at(0)), kConstraintViolation);
    EXPECT_EQ(r.autoIncStats().idsBurned, 1u) << "a rejected import burns nothing";
}

TEST(CheckedWritesAutoIncRegression, ImportBelowTheCounterIsNotAReuseAlarmRegression) {
    AutoInc g;
    VerdictRig& r = g.r;
    Producer p{r, 42};
    ASSERT_EQ(p.createIds(3).back(), 3u);
    const uint64_t burned = r.autoIncStats().idsBurned;
    LogCapture logs;
    const auto a = p.sendAck({import(2)});
    ASSERT_TRUE(a);
    EXPECT_EQ(a->verdicts, (V{{0, kKeyExists, 0}}));
    EXPECT_EQ(r.autoIncStats().assignedKeyExists, 0u);
    EXPECT_EQ(logs.count("AUTOINC-ID-REUSE"), 0u);
    EXPECT_EQ(r.autoIncStats().idsBurned, burned);
}

TEST(CheckedWritesAutoIncWorker, KeyZeroOnAConstrainedSourceNeedsView) {
    AutoInc g;
    VerdictRig& r = g.r;
    r.installRejectView(kRej, kRejObj, tagRule(9));
    r.passes(40);
    Producer p{r, 42, kAll};
    const auto a = p.sendAck(creates(1));
    ASSERT_TRUE(a);
    EXPECT_EQ(a->code, kInvalidRequest);
    EXPECT_EQ(r.worker->hookRefusalsForTest(HookRefusal::KeyZeroCreateNeedsView), 1u);
}

TEST(CheckedWritesAutoIncWorker, AbortedTransactionBurnsItsIds) {
    AutoInc g;
    VerdictRig& r = g.r;
    const auto t = r.ack(r.produceFrame(creates(2), kAll, 77, 0, /*txn=*/true));
    ASSERT_TRUE(t);
    ASSERT_EQ(t->code, 0);
    EXPECT_EQ(t->assignedBase, 1u);
    r.marker(77, /*commit=*/false);
    Producer p{r, 42};
    EXPECT_EQ(p.createIds(1), std::vector<uint64_t>{3});
    ASSERT_TRUE(r.driveUntilJudged());
    EXPECT_EQ(r.keyIndexKeys(), (std::set<std::string>{"3"}));
    EXPECT_EQ(r.autoIncStats().idsBurned, 2u);
}

// 8f: producers that fill the window within one pass are refused retriably; the window grows on the next pass.
TEST(CheckedWritesAutoIncWorker, WindowFullIsRetriableUntilItGrows) {
    VerdictRig r;
    r.reserveAssignedBases(4);
    r.createAutoIncTopic();
    r.enableAcks();
    r.passes(20);
    std::vector<int64_t> corrs;
    for (int64_t pid = 1; pid <= 5; ++pid) corrs.push_back(r.produceFrame(creates(1), kAllView, pid, 0));   // one burst, one pass
    for (int64_t pid = 1; pid <= 4; ++pid) {
        const auto a = r.ack(corrs[std::size_t(pid - 1)]);
        ASSERT_TRUE(a);
        EXPECT_EQ(a->code, 0) << "producer " << pid;
    }
    const auto full = r.ack(corrs[4]);
    ASSERT_TRUE(full);
    EXPECT_EQ(full->code, kNotLeader);
    EXPECT_EQ(r.worker->hookRefusalsForTest(HookRefusal::AutoIncWindowFull), 1u);
    r.passes(2);
    EXPECT_EQ(r.writeState()->assignedBases.capacity(), 8u);
    Producer fifth{r, 5};
    EXPECT_EQ(fifth.createIds(1), std::vector<uint64_t>{5});
}

TEST(CheckedWritesAutoIncWorker, GatewayRoutedCreatesUseTheOpScratch) {
    AutoInc g;
    VerdictRig& r = g.r;
    const auto routed = [&](const std::vector<Rec>& recs, int32_t seq, bool importGranted) {
        return r.ack(r.routedFrame(recs, kAllView, 42, seq, importGranted));
    };
    const auto a = routed(creates(3), 0, true);
    ASSERT_TRUE(a);
    EXPECT_EQ(a->code, 0);
    EXPECT_EQ(a->assignedBase, 1u);
    EXPECT_EQ(loggedIds(r), (std::vector<uint64_t>{1, 2, 3}));
    const auto denied = routed({import(100)}, 3, false);
    ASSERT_TRUE(denied);
    EXPECT_EQ(denied->code, kNotAuthorized);
    const auto granted = routed({import(100)}, 3, true);
    ASSERT_TRUE(granted);
    EXPECT_EQ(granted->code, 0);
    const auto b = routed(creates(2), 4, false);
    ASSERT_TRUE(b);
    EXPECT_EQ(b->assignedBase, 101u) << "creates need no grant; the import moved the counter";
}

// P6-13: an assigned id is still judged by the key index, which never finds it.  Mutation check (Task 9 report): with
// stage()'s commitNext skipped the second batch reuses ids and assignedKeyExists turns non-zero.
TEST(CheckedWritesAutoIncWorker, KeyIndexNeverRejectsAnAssignedId) {
    AutoInc g;
    VerdictRig& r = g.r;
    Producer p{r, 42};
    std::set<uint64_t> ids;
    for (int i = 0; i < 250; ++i) {
        const auto got = p.createIds(8);
        ASSERT_EQ(got.size(), 8u);
        ids.insert(got.begin(), got.end());
    }
    ASSERT_TRUE(r.driveUntilJudged());
    EXPECT_EQ(ids.size(), 2000u);
    EXPECT_EQ(*ids.rbegin(), 2000u);
    EXPECT_EQ(r.autoIncStats().assignedKeyExists, 0u);
    EXPECT_EQ(r.stats()->rejectionsKeyExists, 0u);
}

// C-31: the shard ingress puts the session's IMPORT grant into the write facts.
TEST(CheckedWritesAutoIncWorker, ShardIngressFactsCarryTheGrant) {
    AutoInc g;
    VerdictRig& r = g.r;
    ASSERT_TRUE(r.worker->enforcingForTest());
    r.bindGrants(/*import=*/true);
    Producer p{r, 42};
    ASSERT_TRUE(p.sendAck({import(500)}));
    EXPECT_TRUE(r.worker->lastWriteFactsForTest().importGranted);
    EXPECT_EQ(r.worker->lastWriteFactsForTest().producerId, 42);
    r.bindGrants(/*import=*/false);
    const auto a = p.ackOf(p.resend({import(600)}, p.seq));
    ASSERT_TRUE(a);
    EXPECT_EQ(a->code, kNotAuthorized);
    EXPECT_FALSE(r.worker->lastWriteFactsForTest().importGranted);
}

// Task 8 review carry-over: an append that fails after stage() took the ids burns them.
TEST(CheckedWritesAutoIncRegression, AppendFailureAfterTheIdsAreTakenBurnsThemRegression) {
    AutoInc g;
    VerdictRig& r = g.r;
    Producer p{r, 42};
    ASSERT_EQ(p.createIds(2).back(), 2u);
    const int64_t leo = r.leo();
    r.log->setWriteFaultForTest(EIO);
    const auto a = p.ackOf(p.resend(creates(3), p.seq));
    ASSERT_TRUE(a);
    EXPECT_NE(a->code, 0);
    EXPECT_EQ(r.leo(), leo);
    ASSERT_TRUE(r.autoIncState());
    EXPECT_EQ(r.autoIncState()->nextN, 6u) << "the ids are taken";
    EXPECT_EQ(r.autoIncStats().idsBurned, 3u) << "and counted";
}

namespace {
struct ScanFeed {
    int begins = 0, ends = 0, dataBatches = 0;
    bool endWithoutBegin = false;
    int64_t lastBeginStart = -1;
};
void feedBegin(void* c, int64_t logStart, int64_t) noexcept {
    auto& f = *static_cast<ScanFeed*>(c);
    if (f.begins != f.ends) f.endWithoutBegin = true;   // a begin before the previous scan ended: counted as unpaired
    ++f.begins;
    f.lastBeginStart = logStart;
}
void feedData(void* c, const uint8_t*, std::size_t, int64_t) noexcept { ++static_cast<ScanFeed*>(c)->dataBatches; }
void feedEnd(void* c) noexcept {
    auto& f = *static_cast<ScanFeed*>(c);
    if (f.ends >= f.begins) f.endWithoutBegin = true;
    ++f.ends;
}
std::vector<uint8_t> producerBatch(int64_t pid, int32_t seq) {
    rune::protocol::RecordBatchBuilder b(0, 1000);
    b.setProducerId(pid); b.setProducerEpoch(0); b.setBaseSequence(seq);
    b.append("k", "v", 1000);
    return b.build();
}
}  // namespace

// D-2: the memory-backed (ring) scan and the no-ring edge begin and end the feed exactly once each, and the ring scan feeds every
// producer data batch, so a counter bound to it is rebuilt there too.
TEST(CheckedWritesAutoIncRegression, RingScanPairsBeginAndEndAndFeedsDataBatchesRegression) {
    rune::storage::InMemoryLog mem(1 << 20, 1000);
    for (int i = 0; i < 6; ++i) {
        auto b = producerBatch(9, i);
        int64_t base = 0;
        ASSERT_GE(mem.appendNoSyncRestamp(b.data(), b.size(), 0, base), 0);
    }
    ScanFeed feed;
    RcState rc;
    rc.reserve(8);
    rc.reserveRingRebuild();
    rc.setVerdictHooks(RcVerdictHooks{.begin = &feedBegin, .ctx = &feed, .dataBatch = &feedData, .end = &feedEnd});
    RcFacts f;
    f.leader = true; f.epoch = 1;
    for (int i = 0; i < 64 && !rc.ready(); ++i) (void)rc.advance(f, nullptr, /*budget=*/1, &mem);
    ASSERT_TRUE(rc.ready());
    EXPECT_EQ(feed.begins, 1);
    EXPECT_EQ(feed.ends, 1);
    EXPECT_EQ(feed.dataBatches, 6);
    for (int i = 0; i < 8; ++i) (void)rc.advance(f, nullptr, 1, &mem);
    EXPECT_EQ(feed.ends, 1) << "a Ready state never ends again";
    f.leadershipGeneration = 2;   // a new leadership scans again
    for (int i = 0; i < 64 && (feed.ends < 2 || !rc.ready()); ++i) (void)rc.advance(f, nullptr, 1, &mem);
    EXPECT_EQ(feed.begins, 2);
    EXPECT_EQ(feed.ends, 2);
    EXPECT_EQ(feed.dataBatches, 12);
    f.leadershipGeneration = 3;   // no ring at all: Ready at once, still one begin and one end
    (void)rc.advance(f, nullptr, 1, nullptr);
    ASSERT_TRUE(rc.ready());
    EXPECT_EQ(feed.begins, 3);
    EXPECT_EQ(feed.ends, 3);
    EXPECT_FALSE(feed.endWithoutBegin);
}

// Task 9 fix round 1 (finding 1): a scan budget below one batch header still moves the disk scan, one batch per pass.
TEST(CheckedWritesAutoIncRegression, DiskScanBelowTheBatchOverheadReachesReadyRegression) {
    for (const int32_t budget : {1, 16}) {
        SCOPED_TRACE(budget);
        const auto dir = rigTmpPath("rcbudget");
        {
            rune::storage::Log log(dir.string(), 64LL * 1024 * 1024);
            constexpr int kBatches = 12;
            for (int i = 0; i < kBatches; ++i) {
                auto b = producerBatch(9, i);
                int64_t base = 0;
                ASSERT_GE(log.appendNoSyncRestamp(b.data(), b.size(), 0, base), 0);
            }
            RcState rc;
            rc.reserve(8);
            RcFacts f;
            f.leader = true; f.epoch = 1;
            int passes = 0;
            for (; passes < kBatches + 8 && !rc.ready(); ++passes) (void)rc.advance(f, &log, budget);
            EXPECT_TRUE(rc.ready()) << "still scanning after " << passes << " passes";
        }
        std::error_code ec;
        std::filesystem::remove_all(dir, ec);
    }
}

namespace {
/// demotePromote() on the same worker, then passes until the new leadership's scan made the counter Ready.
void reLead(VerdictRig& r) {
    r.demotePromote();
    for (int i = 0; i < VerdictRig::kMaxPass && !r.autoIncState(); ++i) r.pass();
    ASSERT_TRUE(r.autoIncState());
}
}  // namespace

// Task 9 fix round 1 (finding 2): a rescan of the same reservation on the same worker counts its gap once.
TEST(CheckedWritesAutoIncRegression, ReLeadershipCountsTheBurnedGapOnceRegression) {
    AutoInc g;
    VerdictRig& r = g.r;
    Producer p{r, 42};
    ASSERT_EQ(p.createIds(3).back(), 3u);
    reLead(r);
    EXPECT_EQ(r.autoIncStats().idsBurned, 4093u);
    reLead(r);
    EXPECT_EQ(r.autoIncStats().idsBurned, 4093u) << "the second scan over R 4096 adds nothing";
    EXPECT_EQ(p.createIds(1), std::vector<uint64_t>{4097});
}

// Task 9 fix round 1 (finding 2): ids an append failure burned are not counted again by the next scan's gap.
TEST(CheckedWritesAutoIncRegression, AppendFailureThenReLeadershipCountsEachIdOnceRegression) {
    AutoInc g;
    VerdictRig& r = g.r;
    Producer p{r, 42};
    ASSERT_EQ(p.createIds(2).back(), 2u);
    r.log->setWriteFaultForTest(EIO);
    ASSERT_TRUE(p.ackOf(p.resend(creates(3), p.seq)));
    ASSERT_EQ(r.autoIncStats().idsBurned, 3u);
    r.log->setWriteFaultForTest(0);
    reLead(r);
    EXPECT_EQ(r.autoIncStats().idsBurned, 4096u - 2u) << "R - 2: ids 3-5 once, 6-4096 once";
}

// Task 9 fix round 1 (finding 3, P6-8): a window whose load passes 75 % grows on the next pass, not at the 1024-pass maintenance.
TEST(CheckedWritesAutoIncRegression, WindowGrowsOnTheNextPassAfterItPassesThreeQuartersRegression) {
    VerdictRig r;
    r.reserveAssignedBases(8);
    r.createAutoIncTopic();
    r.enableAcks();
    r.passes(20);
    for (int64_t pid = 1; pid <= 6; ++pid) {
        Producer p{r, pid};
        ASSERT_EQ(p.createIds(1).size(), 1u);
    }
    r.passes(3);
    EXPECT_EQ(r.writeState()->assignedBases.capacity(), 16u);
    for (int64_t pid = 7; pid <= 9; ++pid) {
        Producer p{r, pid};
        const auto ids = p.createIds(1);
        ASSERT_EQ(ids.size(), 1u) << "producer " << pid;
    }
    EXPECT_EQ(r.worker->hookRefusalsForTest(HookRefusal::AutoIncWindowFull), 0u);
}

// Task 9 fix round 1 (finding 5): an import behind an update in its batch still raises the scan's highest assigned n, so
// the burned gap is not overstated.
TEST(CheckedWritesAutoIncRegression, ImportBehindAnUpdateCountsAsAssignedInTheScanRegression) {
    AutoInc g;
    VerdictRig& r = g.r;
    Producer p{r, 42};
    ASSERT_EQ(p.createIds(3).back(), 3u);
    const auto a = p.sendAck({upd(1), import(100)});
    ASSERT_TRUE(a);
    ASSERT_EQ(a->code, 0);
    ASSERT_TRUE(a->verdicts.empty());
    reLead(r);
    EXPECT_EQ(r.autoIncStats().idsBurned, 4096u - 100u) << "R - 100, not R - 3";
}

// Task 9 fix round 1 (finding 8): a ring scan restarted by eviction begins the feed again from the new ring start, so the
// window and the counter scan never hold entries fed before the restart.
TEST(CheckedWritesAutoIncRegression, RingScanRestartBeginsTheFeedAgainRegression) {
    rune::storage::InMemoryLog mem(1 << 20, 4);   // at most 4 batches: later appends evict the oldest
    const auto append = [&](int32_t seq) {
        auto b = producerBatch(9, seq);
        int64_t base = 0;
        ASSERT_GE(mem.appendNoSyncRestamp(b.data(), b.size(), 0, base), 0);
    };
    for (int i = 0; i < 4; ++i) append(i);
    ScanFeed feed;
    RcState rc;
    rc.reserve(8);
    rc.reserveRingRebuild();
    rc.setVerdictHooks(RcVerdictHooks{.begin = &feedBegin, .ctx = &feed, .dataBatch = &feedData, .end = &feedEnd});
    RcFacts f;
    f.leader = true; f.epoch = 1;
    (void)rc.advance(f, nullptr, /*budget=*/1, &mem);
    ASSERT_FALSE(rc.ready());
    ASSERT_EQ(feed.begins, 1);
    for (int i = 4; i < 8; ++i) append(i);
    ASSERT_GT(mem.logStartOffset(), 1) << "precondition: the ring evicted past the scan cursor";
    for (int i = 0; i < 64 && !rc.ready(); ++i) (void)rc.advance(f, nullptr, 1, &mem);
    ASSERT_TRUE(rc.ready());
    EXPECT_EQ(feed.begins, 2) << "the restart began the feed again";
    EXPECT_EQ(feed.lastBeginStart, mem.logStartOffset());
    EXPECT_EQ(feed.ends, 1);
}

// Task 10 (Review Focus 3): the highest id, removed and compacted away with its tombstone, is never assigned again (the cleaner
// kept the latest reservation) and never comes back (its insert went with the tombstone).  Fix round 1: the rig cleans from
// lastCleanOffset as the agent does, and the first passes run while id 50's batch and the remove are still protected.  RED (before
// the watermark stopped at the oldest protected batch): those passes moved the watermark past both, the later maps never held key
// 50, so its insert stayed while the tombstone expired - id 50 came back.
TEST(CheckedWritesAutoIncCleanerRegression, HighestIdDeletedAndCompactedIsNeverReusedNorResurrectedRegression) {
    AutoInc g({}, false, /*rollBytes=*/1);
    VerdictRig& r = g.r;
    Producer a{r, 42};
    ASSERT_EQ(a.createIds(50).back(), 50u);
    const auto rm = a.sendAck({rmEx(50)});
    ASSERT_TRUE(rm && rm->code == 0);
    ASSERT_TRUE(r.driveUntilJudged());
    const auto has50 = [&] {
        for (const auto& d : r.readLog().data) if (d.id == 50) return true;
        return false;
    };
    (void)r.compact();
    (void)r.compact(1'005'000);   // past the delete horizon
    ASSERT_TRUE(has50()) << "both batches are in A's last 32: kept whole, the tombstone pinned";
    for (uint64_t id = 1; id <= 40; ++id) ASSERT_TRUE(a.sendAck({upd(id)}));   // both leave A's last 32
    ASSERT_TRUE(r.driveUntilJudged());
    for (int i = 0; i < 6 && has50(); ++i) (void)r.compact(1'010'000 + i * 5'000);
    ASSERT_FALSE(has50()) << "neither id 50's insert nor its tombstone is in the log";
    EXPECT_FALSE(r.reservations().empty()) << "the latest reservation is kept";
    r.restart(/*reinstall=*/false);
    for (int i = 0; i < VerdictRig::kMaxPass && !r.autoIncState(); ++i) r.pass();
    const auto ids = a.createIds(1);
    ASSERT_EQ(ids.size(), 1u);
    EXPECT_GT(ids[0], 50u) << "from the kept reservation, never a reuse";
    const int64_t sub = r.subscribe(SubKind::Raw, r.log->logStartOffset());
    for (const auto& d : r.collect(sub).records) EXPECT_FALSE(d.id == 50 && d.tag != kTombstoneTag) << "id 50 delivered live at " << d.offset;
    r.installProjection(kProj);
    r.driveUntilFolded(kProj, r.leo());
    EXPECT_FALSE(r.hasRow(kProj, 50)) << "a fresh projection has no row 50";
    EXPECT_TRUE(r.hasRow(kProj, 45));
}

// Fix round 2 (M1): a restart reloads the watermark the window held (the cleaner checkpoint), and cleaning on from it never brings
// a deleted id back.  RED (restoring where the last pass ended instead): the next passes started above id 50's insert and tombstone,
// so the tombstone went past its horizon once the window released the insert, and the insert stayed.
TEST(CheckedWritesAutoIncCleanerRegression, RestartFromAHeldWatermarkNeverResurrectsADeletedIdRegression) {
    AutoInc g({}, false, /*rollBytes=*/1);
    VerdictRig& r = g.r;
    Producer a{r, 42};
    ASSERT_EQ(a.createIds(50).back(), 50u);
    const auto rm = a.sendAck({rmEx(50)});
    ASSERT_TRUE(rm && rm->code == 0);
    ASSERT_TRUE(r.driveUntilJudged());
    const auto has50 = [&] {
        for (const auto& d : r.readLog().data) if (d.id == 50) return true;
        return false;
    };
    (void)r.compact();
    (void)r.compact(1'005'000);   // past the delete horizon
    for (uint64_t id = 1; id <= 10; ++id) ASSERT_TRUE(a.sendAck({upd(id)}));   // both batches stay in A's last 32
    ASSERT_TRUE(r.driveUntilJudged());
    (void)r.compact(1'006'000);
    ASSERT_TRUE(has50());
    const int64_t held = r.replica->lastCleanOffset();
    ASSERT_GT(held, 0) << "the cleaner has a watermark to persist";
    ASSERT_GT(r.replica->cleanerProgress(), held) << "held below where the passes ended";
    r.restart(/*reinstall=*/false);
    EXPECT_EQ(r.replica->lastCleanOffset(), held) << "the checkpoint is reloaded";
    for (int i = 0; i < VerdictRig::kMaxPass && !r.autoIncState(); ++i) r.pass();
    for (uint64_t id = 11; id <= 40; ++id) ASSERT_TRUE(a.sendAck({upd(id)}));   // both leave A's last 32
    ASSERT_TRUE(r.driveUntilJudged());
    for (int i = 0; i < 6 && has50(); ++i) (void)r.compact(1'010'000 + i * 5'000);
    ASSERT_FALSE(has50()) << "neither id 50's insert nor its tombstone is in the log";
    const int64_t sub = r.subscribe(SubKind::Raw, r.log->logStartOffset());
    for (const auto& d : r.collect(sub).records) EXPECT_FALSE(d.id == 50 && d.tag != kTombstoneTag) << "id 50 delivered live at " << d.offset;
    r.installProjection(kProj);
    r.driveUntilFolded(kProj, r.leo());
    EXPECT_FALSE(r.hasRow(kProj, 50)) << "a fresh projection has no row 50";
}

// Task 10 (section 15): the ack of a key-0 batch is lost, its ids are updated, the log is compacted and the leader fails over: the
// retry still answers the original ids, because the protected batch kept its records.
TEST(CheckedWritesAutoIncCleanerRegression, RetryAfterFailoverAndCompactionStillReturnsItsIdsRegression) {
    AutoInc g({}, false, /*rollBytes=*/1);
    VerdictRig& r = g.r;
    Producer p{r, 42};
    ASSERT_EQ(p.createIds(1).back(), 1u);
    const int32_t seq = p.seq;
    const auto orig = p.sendAck(creates(4));   // the client never sees this ack
    ASSERT_TRUE(orig);
    ASSERT_EQ(orig->assignedBase, 2u);
    for (int i = 0; i < 10; ++i) ASSERT_TRUE(p.sendAck({upd(2 + static_cast<uint64_t>(i % 4))}));
    ASSERT_TRUE(r.driveUntilJudged());
    (void)r.compact();
    (void)r.compact(1'005'000);
    r.failover();
    for (int i = 0; i < VerdictRig::kMaxPass && !r.autoIncState(); ++i) r.pass();
    ASSERT_TRUE(r.autoIncState()) << "the leadership scan over the compacted log ends";
    const auto a = p.ackOf(p.resend(creates(4), seq));
    ASSERT_TRUE(a);
    EXPECT_EQ(a->code, 0);
    EXPECT_EQ(a->baseOffset, orig->baseOffset);
    EXPECT_EQ(a->assignedBase, 2u) << "the original ids";
    EXPECT_EQ(r.autoIncStats().windowMisses, 0u);
    EXPECT_EQ(loggedIds(r).size(), 15u) << "the retry appended nothing and the protected batch kept all four";
}

// Task 10 (ruling D-6): the cleaner reads only the sealed segments, so the latest reservation may lie in the active one.  The older
// sealed reservations go, the highest sealed one and the active one stay, and a restart resumes above the active one.
TEST(CheckedWritesAutoIncCleanerRegression, LatestReservationInTheActiveSegmentSurvivesACleanRegression) {
    AutoInc g(AutoIncShape{.reserve = 8}, false, /*rollBytes=*/2048);
    VerdictRig& r = g.r;
    Producer p{r, 42};
    const auto latestIsActive = [&] {
        const auto resv = r.reservations();
        return resv.size() >= 4 && resv.back().first >= r.log->segmentBaseOffsets().back() && resv[resv.size() - 2].first < r.log->segmentBaseOffsets().back();
    };
    uint64_t maxId = 0;
    for (int i = 0; i < 2000 && !latestIsActive(); ++i) {
        maxId = p.createIds(1).back();
        ASSERT_TRUE(r.driveUntilJudged());   // the markers covering the sealed segments are in them
    }
    ASSERT_TRUE(latestIsActive()) << "precondition: the latest reservation is in the active segment";
    const auto before = r.reservations();
    const int64_t activeBase = r.log->segmentBaseOffsets().back();
    (void)r.compact(1'000'000, /*seal=*/false);
    (void)r.compact(1'005'000, /*seal=*/false);
    ASSERT_EQ(r.log->segmentBaseOffsets().back(), activeBase) << "precondition: nothing rolled";
    std::vector<std::pair<int64_t, uint64_t>> want;
    for (std::size_t i = 0; i < before.size(); ++i)
        if (before[i].first >= activeBase || (i + 1 < before.size() && before[i + 1].first >= activeBase)) want.push_back(before[i]);
    EXPECT_EQ(r.reservations(), want) << "the highest sealed reservation and the active one stay, the older ones go";
    r.restart();
    const auto ids = p.createIds(1);
    ASSERT_EQ(ids.size(), 1u);
    EXPECT_GT(ids[0], maxId) << "no id reuse";
    EXPECT_EQ(ids[0], before.back().second + 1) << "the counter resumes above the active reservation";
}
