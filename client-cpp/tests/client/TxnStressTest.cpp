// tests/client/TxnStressTest.cpp - Phase 2b F Task 13: seeded, model-checked stress of the unified transactional
// Producer against an independent model (tests/support/TxnModel.h).
//
// One seed = one run of kSteps random steps on a real Client in Invoker mode over Aeron IPC to the InvokerGateway in
// its transaction-aware mode (the rig of TxnMatrixTest; no code shared with ProducerImpl).  Each step is one of:
//   a data op of the 9 paths on a random partition (send, sendTo, trySend cq, trySend AckToken, batch(), tryClaim,
//   claimBatch, TypedTopic::send, TypedTopic::claim), begin, commit, abort, flush, sendOffsets, seekToCommitted
//   (the rig's group consumer), a scripted 27 / 14 / 19 / 18 on the next coordinator answer, dropping the session
//   (the gateway goes deaf: every frame is lost until the reconnect), reconnecting (the gateway hears again and
//   expires the old session; the client re-establishes and re-sends).
// After each step the rig runs the client and the gateway until both are quiet (bounded), then feeds the model the
// coordinator's decisions and the completions, in that order.  Invariants:
//   * after every completed commit / abort the read_committed view of every partition equals the model's committed
//     set (and the group's committed offset the model's);
//   * at the end: every token op completed exactly once (the model), no produce before its partition's AddPartitions
//     OK (the frame log), per (pid, epoch, partition) sequences gap-free in the log (and no out-of-order answer on
//     the wire), no frame from a fenced producer after it learned of the fence, zero allocations in every measured
//     op issue after its producer's warm-up, every accepted record reported in exactly one BatchAck.
// A failure prints the seed and the whole step log.  RUNE_TXN_STRESS_SEEDS=N widens the run to seeds 1..N;
// RUNE_TXN_STRESS_LOG=1 prints a passing seed's coverage summary and step log too.
// Single-threaded, condition-driven: every wait is a bounded deadline loop, never a sleep.
#include "../support/AllocCounter.h"
#include "../support/InvokerGateway.h"
#include "../support/TxnModel.h"

#include <rune/client/Client.h>
#include <rune/client/Completions.h>
#include <rune/client/Key.h>
#include <rune/client/TypedTopic.h>
#include <rune/client/group/GroupConsumer.h>
#include <rune/client/producer/Producer.h>
#include <rune/client/producer/ProducerRef.h>

#include "gen/position.gen.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <filesystem>
#include <map>
#include <memory>
#include <optional>
#include <random>
#include <set>
#include <span>
#include <stdexcept>
#include <string>
#include <tuple>
#include <vector>
#include <unistd.h>

namespace rc = rune::client;
namespace txm = txnmodel;
using Gw     = test_support::InvokerGateway;

namespace {

constexpr int32_t kTopicId    = 1;
constexpr int32_t kPartitions = 3;
constexpr int     kSteps      = 400;
constexpr int     kDefaultSeeds = 64;
using PositionTopic = rc::TypedTopic<rune::gen::Position, rc::ProducerRef>;

enum class DataOp : uint8_t { Send, SendTo, TrySendCq, TrySendAck, Batch, TryClaim, ClaimBatch, TypedSend, TypedClaim };
constexpr int kDataOps = 9;
const char* name(DataOp o) {
    switch (o) {
    case DataOp::Send:       return "send";
    case DataOp::SendTo:     return "sendTo";
    case DataOp::TrySendCq:  return "trySendCq";
    case DataOp::TrySendAck: return "trySendAck";
    case DataOp::Batch:      return "batch";
    case DataOp::TryClaim:   return "tryClaim";
    case DataOp::ClaimBatch: return "claimBatch";
    case DataOp::TypedSend:  return "typedSend";
    case DataOp::TypedClaim: return "typedClaim";
    }
    return "?";
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

std::span<const uint8_t> bytesOf(const std::string& s) { return {reinterpret_cast<const uint8_t*>(s.data()), s.size()}; }

int64_t keyForPartition(int32_t partition) {
    for (int64_t u = 1;; ++u) {
        if (rc::Key(u).partition(kPartitions) == partition) return u;
    }
}

std::string positionBytes(int64_t userId, int32_t zone) {
    rune::gen::Position rec;
    rec.userId = userId;
    rec.zone   = zone;
    std::string out(rec.encodedSize(), '\0');
    rec.encode(reinterpret_cast<uint8_t*>(out.data()));
    return out;
}

txm::Leg legOf(const Gw::CoordDecision& d) {
    switch (d.kind) {
    case Gw::TxnFrameKind::AddPartitionsReq: return txm::Leg::AddPartitions;
    case Gw::TxnFrameKind::AddOffsets:       return txm::Leg::AddOffsets;
    case Gw::TxnFrameKind::TxnOffsetCommit:  return txm::Leg::TxnOffsetCommit;
    default:                                 return d.commit != 0 ? txm::Leg::EndTxnCommit : txm::Leg::EndTxnAbort;
    }
}

class Stress {
public:
    explicit Stress(uint32_t seed) : seed_(seed), rng_(0x5EED'0000'0000ull + seed) {
        for (int32_t p = 0; p < kPartitions; ++p) keys_[static_cast<std::size_t>(p)] = keyForPartition(p);
        const std::string n = std::to_string(seed);
        topicName_          = "st-topic-" + n;
        txnId_              = "st-txn-" + n;
        group_              = "st-group-" + n;
        cfg_.gatewayChannel  = "aeron:ipc";
        cfg_.gatewayStreamId = 16000 + 2 * static_cast<int32_t>(seed);
        cfg_.egressChannel   = "aeron:ipc";
        cfg_.egressStreamId  = 16001 + 2 * static_cast<int32_t>(seed);
        cfg_.dataDir         = std::string(std::filesystem::temp_directory_path().string() + "/f_txnst_") + std::to_string(::getpid()) + "_" + n;
        g_sweeper.dirs.push_back(cfg_.dataDir);
        cfg_.runnerMode = rc::RunnerMode::Invoker;
        rc::TopicSpec ts;
        ts.name           = topicName_;
        ts.topicId        = kTopicId;
        ts.partitionCount = kPartitions;
        cfg_.topics.push_back(ts);
        client_ = std::make_unique<rc::Client>(cfg_);
        gw_     = std::make_unique<Gw>(cfg_.dataDir + "/aeron", cfg_.gatewayChannel, cfg_.gatewayStreamId,
                                       cfg_.egressChannel, cfg_.egressStreamId);
        gw_->idempotence      = true;
        gw_->initPidReply     = true;
        gw_->replyTxn         = true;
        gw_->uniqueSessionIds = true;
        gw_->newIdOnResume    = true;
        for (int32_t p = 0; p < kPartitions; ++p) gw_->partitions.push_back({kTopicId, p, 0, 1, cfg_.gatewayStreamId, ""});
        // The rig's group consumer (seekToCommitted): partition 0, its offsets committed by the transactions.
        gw_->groupProtocol   = true;
        gw_->holdJoins       = true;
        gw_->replyPongs      = true;
        gw_->groupAssignment = {{kTopicId, 0}};
        gw_->committedOffsets[{kTopicId, 0}] = 0;
        cq_ = std::make_unique<rc::Completions>(*client_);
        arrived_.reserve(1u << 16);
    }
    ~Stress() {
        typed_.reset();
        gc_.reset();
        p_.reset();
        if (client_) client_->close();
    }

    void run();

private:
    // ---- rig -------------------------------------------------------------------------------------------------------
    uint32_t                         seed_;
    std::mt19937_64                  rng_;
    std::array<int64_t, kPartitions> keys_{};
    std::string                      topicName_, txnId_, group_;
    rc::ClientConfig                 cfg_;
    std::unique_ptr<rc::Client>      client_;
    std::unique_ptr<Gw>              gw_;
    std::unique_ptr<rc::Completions> cq_;
    std::optional<rc::Producer>      p_;
    rc::TopicHandle                  topic_{};
    std::optional<PositionTopic>     typed_;
    std::optional<rc::GroupConsumer> gc_;

    struct Arrival { rc::Token token; txm::Result r; };
    std::vector<Arrival>      arrived_;     // completions (cq + onAck), in arrival order; reserved: no allocation
    std::deque<std::string>   keep_;        // payload storage (stable addresses)

    txm::TxnModel model_;
    std::vector<std::string> log_;          // the step log (printed on failure)
    std::size_t decisionsSeen_ = 0;
    rc::Token   nextToken_     = 1000;
    uint64_t    nextValue_     = 0;
    int64_t     nextOffset_    = 1;
    int         acceptedRecords_ = 0;
    bool        warm_          = false;     // the current producer finished its warm-up: op issues are measured
    // A control-plane step (drop, reconnect, seekToCommitted: session establishment, rejoin, OffsetFetch): its passes
    // are not measured.  Reset at every step.
    bool        controlPlane_  = false;
    long        measuredPasses_ = 0;
    long        measured_      = 0;
    bool        dead_          = false;     // the session is dropped (the gateway deaf, or mute)
    int         deadSteps_     = 0;
    int         step_          = -1;
    int         replacements_  = 0;
    int16_t     epoch_         = -1;        // the current producer's epoch
    // A fenced producer: after the settle that showed the fence, no frame of its epoch may leave.
    std::optional<std::pair<std::size_t, int16_t>> fencedMark_;
    // The group consumer's rewind.
    rc::Token seekToken_     = 0;
    bool      seekDisturbed_ = false;
    int64_t   seekExpectA_ = 0, seekExpectB_ = 0;
    std::map<std::string, int> counts_;     // step kinds (summary)
    std::map<std::string, int> stats_;      // what the run reached (summary)
    static const char* legName_(txm::Leg l) {
        switch (l) {
        case txm::Leg::AddPartitions:   return "AddPartitions";
        case txm::Leg::AddOffsets:      return "AddOffsets";
        case txm::Leg::TxnOffsetCommit: return "TxnOffsetCommit";
        case txm::Leg::EndTxnCommit:    return "EndTxnCommit";
        case txm::Leg::EndTxnAbort:     return "EndTxnAbort";
        }
        return "?";
    }

    // ---- driving ---------------------------------------------------------------------------------------------------
    static void onAckThunk(void* ctx, rc::Token token, int64_t, rc::OpStatus st, uint16_t ec) noexcept {
        static_cast<Stress*>(ctx)->arrived_.push_back({token, {st, ec}});
    }
    static void onBatchAckThunk(void* ctx, const rc::BatchAck& a) noexcept {
        auto* s = static_cast<Stress*>(ctx);
        s->ackedRecords_ += a.recordCount;
        if (a.partition >= 0 && a.partition < kPartitions) s->ackedCum_[static_cast<std::size_t>(a.partition)] += a.recordCount;
    }
    // Per partition of the current producer: records accepted / reported in a BatchAck, and where the last Tier-2
    // batch ends (a record after a queued Tier-2 batch is back-pressured until that batch leaves).
    std::array<int64_t, kPartitions> acceptedCum_{}, ackedCum_{}, lastTier2End_{};
    int64_t ackedRecords_ = 0;
    /// A blocking void send (send / sendTo / TypedTopic::send) in Invoker mode pumps only the CLIENT while it is
    /// back-pressured; this rig's broker is pumped by the same thread, so such a send could never finish.  It is
    /// issued only where it cannot be back-pressured: no unsent Tier-2 batch ahead of it, the builder far from full.
    [[nodiscard]] bool voidMayBlock_(int32_t p) const {
        const auto i = static_cast<std::size_t>(p);
        return lastTier2End_[i] > ackedCum_[i] || acceptedCum_[i] - ackedCum_[i] >= 64;
    }
    /// One pass, absorbed in causal order: the client's completions (they answer coordinator decisions of EARLIER
    /// passes - the gateway's answers are read by the next doWork), then the decisions the gateway takes now.
    int pass() {
        int  w      = 0;
        long allocs = 0;
        const bool measure = warm_ && !controlPlane_;
        if (measure) {
            // The duty cycle (AddPartitions hold / release, coordinator re-sends, EndTxn, BatchAcks, dedup answers) is
            // measured too; only doWork() is in the window (the per-thread counter excludes the gateway's pump()).
            allocs = window_([&] { w = client_->doWork(); });
        } else {
            w = client_->doWork();
        }
        cq_->poll([&](const rc::Completion& c, std::span<const uint8_t>) { arrived_.push_back({c.token, {c.status, c.errorCode}}); ++w; });
        absorbCompletions_();
        const int metaBefore = gw_->metadataRequests;
        w += gw_->pump();
        absorbDecisions_();
        // Exempt (control plane): applying a cluster view - this doWork read the MetadataResponse the gateway gave in
        // the previous pass (a NotCoordinator rediscovery; the metadata apply path builds its tables).  The pass that
        // SENDS the request is measured (Task 13 fix round 1: it allocated; fixed).
        const bool metaNow  = gw_->metadataRequests != metaBefore;
        const bool metadata = metaLastPass_;
        metaLastPass_ = metaNow;
        if (measure) {
            if (metadata) {
                ++stats_["dutyPassMetadataExempt"];
            } else {
                ++measuredPasses_;
                if (allocs != 0) fail("a duty cycle (doWork) allocated " + std::to_string(allocs) + " times after the warm-up");
            }
        }
        return w;
    }
    bool metaLastPass_ = false;
    /// Run until the client and the gateway are both quiet (bounded).
    void settle() {
        int idle = 0;
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
        for (int i = 0; i < 4000 && idle < 24 && std::chrono::steady_clock::now() < deadline; ++i) {
            idle = pass() == 0 ? idle + 1 : 0;
        }
        absorb();
    }
    template <class P>
    bool drive(P&& pred, std::chrono::milliseconds limit = std::chrono::seconds(10)) {
        const auto deadline = std::chrono::steady_clock::now() + limit;
        while (std::chrono::steady_clock::now() < deadline) {
            pass();
            if (pred()) return true;
        }
        return false;
    }
    /// Feed the model everything not absorbed yet (completions of a synchronous issue, then decisions).
    void absorb() {
        absorbCompletions_();
        absorbDecisions_();
    }
    void absorbDecisions_() {
        for (; decisionsSeen_ < gw_->coordDecisions.size(); ++decisionsSeen_) {
            const auto& d = gw_->coordDecisions[decisionsSeen_];
            // An older epoch: a request of a replaced (fenced) producer.  A newer one: this producer after a KIP-360
            // bump (the gateway's epochs only grow).
            if (d.producerEpoch < epoch_) continue;
            epoch_ = d.producerEpoch;
            if (!d.answered) {   // the client never hears it: it re-sends the request on the next session
                if (d.code != 0) note("  decision " + std::to_string(static_cast<int>(d.kind)) + " code " + std::to_string(d.code) + " (answer lost)");
                continue;
            }
            // One request, one FINAL decision: a re-send's answer repeats it (the client acts on the first it hears).
            // A retriable answer (27 / 14) is not final - the re-sent request is decided afresh.
            if (d.code != 27 && d.code != 14 &&
                !heard_.insert(std::make_tuple(d.producerId, d.producerEpoch, d.correlationId)).second) {
                continue;
            }
            const txm::State before = model_.state;
            if (d.code != 0) ++stats_["code" + std::to_string(d.code) + "@" + legName_(legOf(d))];
            model_.decision(legOf(d), d.code);
            if (d.code != 0) note("  decision " + std::to_string(static_cast<int>(d.kind)) + " code " + std::to_string(d.code) +
                                  " -> " + txm::name(model_.state));
            if (before != txm::State::Fatal && model_.state == txm::State::Fatal) fatalSeen_ = true;
        }
    }
    void absorbCompletions_() {
        for (const Arrival& a : arrived_) {
            if (a.token == seekToken_ && seekToken_ != 0) {
                seekDone_(a.r);
                continue;
            }
            const txm::State before = model_.state;
            const auto ap = model_.complete(a.token, a.r);
            if (ap.ended == txm::TxnModel::Ended::Committed) ++stats_["commitOk"];
            if (ap.ended == txm::TxnModel::Ended::Aborted) ++stats_["abortOk"];
            if (ap.kind == txm::TokenKind::Commit || ap.kind == txm::TokenKind::Abort) {
                note("  " + std::string(txm::name(ap.kind)) + " " + std::to_string(a.token) + " -> " + txm::TxnModel::str(a.r) +
                     " state " + txm::name(model_.state));
                // The read_committed view is the model's whenever no end is in flight (a refused abort while a commit
                // runs leaves that commit in doubt: its EndTxn may already have landed).
                if (model_.state != txm::State::Committing && model_.state != txm::State::Aborting) {
                    ++stats_["rcChecks"];
                    checkReadCommitted_();
                }
            }
            if (before != txm::State::Fatal && model_.state == txm::State::Fatal) fatalSeen_ = true;
        }
        arrived_.clear();
    }
    bool fatalSeen_ = false;
    std::set<std::tuple<int64_t, int16_t, int64_t>> heard_;   // coordinator requests whose answer the model applied

    std::chrono::steady_clock::time_point t0_ = std::chrono::steady_clock::now();
    void note(std::string s) {
        const auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - t0_).count();
        log_.push_back(std::to_string(ms) + "ms " + std::move(s));
    }
    void fail(const std::string& why) { model_.errors.push_back("step " + std::to_string(step_) + ": " + why); }

    // ---- checks ----------------------------------------------------------------------------------------------------
    void checkReadCommitted_() {
        for (int32_t p = 0; p < kPartitions; ++p) {
            std::multiset<std::string> rc;
            for (const auto& v : gw_->committedValues(p)) rc.emplace(v.begin(), v.end());
            const auto& want = model_.committed[p];
            if (rc != want) {
                fail("read_committed view of partition " + std::to_string(p) + " has " + std::to_string(rc.size()) +
                     " records, the model " + std::to_string(want.size()) + diff_(rc, want));
            }
        }
        const int64_t off = gw_->committedOffsets[{kTopicId, 0}];
        if (off != model_.committedOffset) {
            fail("the group's committed offset is " + std::to_string(off) + ", the model's " + std::to_string(model_.committedOffset));
        }
    }
    static std::string diff_(const std::multiset<std::string>& got, const std::multiset<std::string>& want) {
        std::string s;
        for (const auto& v : got) if (got.count(v) > want.count(v)) { s += " extra[" + printable_(v) + "]"; if (s.size() > 300) break; }
        for (const auto& v : want) if (want.count(v) > got.count(v)) { s += " missing[" + printable_(v) + "]"; if (s.size() > 600) break; }
        return s;
    }
    static std::string printable_(const std::string& v) {
        std::string s;
        for (const char c : v) s += (c >= 32 && c < 127) ? c : '.';
        return s;
    }
    void checkFrameLog_() {
        // No transactional batch before its partition's AddPartitions OK (within the transaction of its epoch).
        std::map<int16_t, std::set<int32_t>> added;
        for (std::size_t i = 0; i < gw_->txnFrames.size(); ++i) {
            const auto& f = gw_->txnFrames[i];
            switch (f.kind) {
            case Gw::TxnFrameKind::AddPartitionsOk:
                if (f.code == 0) added[f.producerEpoch].insert(f.partition);
                break;
            case Gw::TxnFrameKind::EndTxn:
                if (f.code == 0) added[f.producerEpoch].clear();
                break;
            case Gw::TxnFrameKind::Produce:
                if (!f.transactional) fail("frame " + std::to_string(i) + ": a transactional producer's batch lacks the TRANSACTIONAL attribute");
                if (added[f.producerEpoch].count(f.partition) == 0) {
                    fail("frame " + std::to_string(i) + ": partition " + std::to_string(f.partition) + " (epoch " +
                         std::to_string(f.producerEpoch) + ") produced before its AddPartitions OK");
                }
                break;
            default: break;
            }
        }
        // Per (pid, epoch, partition) sequences gap-free in the log; none out of order on the wire.
        std::map<std::tuple<int64_t, int16_t, int32_t>, int32_t> next;
        for (const auto& b : gw_->log) {
            if (b.producerId < 0) continue;
            const auto key = std::make_tuple(b.producerId, b.producerEpoch, b.partition);
            const auto it  = next.find(key);
            const int32_t want = it == next.end() ? 0 : it->second;
            if (b.baseSequence != want) {
                fail("sequence gap: pid " + std::to_string(b.producerId) + " epoch " + std::to_string(b.producerEpoch) + " partition " +
                     std::to_string(b.partition) + " batch at " + std::to_string(b.baseSequence) + ", expected " + std::to_string(want));
            }
            next[key] = b.baseSequence + static_cast<int32_t>(b.values.size());
        }
        if (gw_->oosAnswers != 0) fail(std::to_string(gw_->oosAnswers) + " OutOfOrderSequence answers (a sequence gap on the wire)");
        if (gw_->unknownPidAnswers != 0) fail(std::to_string(gw_->unknownPidAnswers) + " UnknownProducerId answers");
        if (gw_->fencedAnswers != 0) fail(std::to_string(gw_->fencedAnswers) + " InvalidProducerEpoch answers to a produce");
    }
    void checkNoFrameAfterFence_() {
        if (!fencedMark_) return;
        const auto [mark, epoch] = *fencedMark_;
        for (std::size_t i = mark; i < gw_->txnFrames.size(); ++i) {
            const auto& f = gw_->txnFrames[i];
            if (f.kind != Gw::TxnFrameKind::AddPartitionsOk && f.producerEpoch == epoch) {
                fail("frame " + std::to_string(i) + " (kind " + std::to_string(static_cast<int>(f.kind)) + ") left the fenced producer (epoch " +
                     std::to_string(epoch) + ")");
            }
        }
        fencedMark_.reset();
    }

    // ---- setup -----------------------------------------------------------------------------------------------------
    void connect() {
        ASSERT_TRUE(drive([&] { return gw_->ready(); }));
        const rc::Token t = nextToken_++;
        ASSERT_EQ(client_->connect(*cq_, t), rc::OpResult::Accepted);
        bool ok = false;
        ASSERT_TRUE(rawAwait_(t, ok));
        ASSERT_TRUE(ok);
        ASSERT_TRUE(drive([&] { return gw_->metadataRequests > 0; }));
        // Aeron's client conductor creates its heartbeat counter handle once, on its first keepalive check after the
        // driver published the counter (KEEPALIVE_TIMEOUT_MS = 500 ms): a one-time third-party allocation, run past
        // here so the measured duty cycles never see it.
        const auto until = std::chrono::steady_clock::now() + std::chrono::milliseconds(1200);
        while (std::chrono::steady_clock::now() < until) pass();
    }
    /// Waits for one token outside the model (connect / init); `ok` = completed Ok.
    bool rawAwait_(rc::Token t, bool& ok) {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
        while (std::chrono::steady_clock::now() < deadline) {
            client_->doWork();
            gw_->pump();
            bool got = false;
            cq_->poll([&](const rc::Completion& c, std::span<const uint8_t>) {
                if (c.token == t) { got = true; ok = c.status == rc::OpStatus::Ok; }
                else arrived_.push_back({c.token, {c.status, c.errorCode}});
            });
            if (got) return true;
        }
        return false;
    }
    void groupReady() {
        rc::GroupConfig gcfg;
        gcfg.autoCommit          = false;
        gcfg.sessionTimeoutMs    = 30000;
        gcfg.commitSyncTimeoutMs = 3000;
        gc_.emplace(client_->subscribeGroup(group_, {topicName_}, gcfg));
        gc_->onRecords([](const rc::RecordSet&) {});
        ASSERT_TRUE(drive([&] { return !gw_->pendingJoins.empty(); }));
        gw_->releaseJoins();
        gw_->holdJoins = false;   // a rejoin after a reconnect is answered at once
        ASSERT_TRUE(drive([&] { return gw_->latestSubscription(kTopicId, 0) != nullptr; })) << "no subscription";
    }
    rc::ProducerConfig producerCfg() const {
        rc::ProducerConfig c;
        c.lingerUs               = 0;
        c.batchRecords           = 100;
        c.maxInFlight            = 8;
        c.produceTimeoutMs       = 30'000;
        c.deliveryTimeoutMs      = 60'000;
        c.transactionOpTimeoutMs = 30'000;
        c.acks                   = rc::Acks::Leader;
        c.transactionalId        = txnId_;
        return c;
    }
    /// A (new) producer: init, then a warm-up - every op path on every partition in a committed transaction, an
    /// aborted one, every refusal - so the measured steps after it see only steady-state paths.
    void makeProducer() {
        warm_ = false;
        p_.emplace(client_->producer(producerCfg()));
        p_->onAck(&Stress::onAckThunk, this);
        p_->onBatchAck(&Stress::onBatchAckThunk, this);
        topic_ = client_->topic(topicName_);
        typed_.emplace(rc::ProducerRef(*p_), topic_, uint16_t{1});
        const rc::Token t = nextToken_++;
        ASSERT_EQ(p_->initTransactions(*cq_, t), rc::OpResult::Accepted);
        bool ok = false;
        ASSERT_TRUE(rawAwait_(t, ok)) << "initTransactions never completed";
        ASSERT_TRUE(ok) << "initTransactions failed";
        ASSERT_TRUE(drive([&] { return p_->state() == rc::TxnState::Ready; }));
        epoch_ = p_->producerEpoch();
        model_.initialized();
        note("producer epoch " + std::to_string(epoch_) + " initialized");
        // Warm-up (scripted, through the same op paths, checked by the model like every step).
        doBegin_();
        for (int32_t part = 0; part < kPartitions; ++part) {
            for (int o = 0; o < kDataOps; ++o) {
                (void)doData_(static_cast<DataOp>(o), part);
                settle();
            }
        }
        doSendOffsets_();
        doFlush_();
        doCommit_();
        ASSERT_TRUE(drive([&] { return model_.state == txm::State::Ready && model_.idle(); })) << "the warm-up commit never completed";
        doBegin_();
        for (int o = 0; o < kDataOps; ++o) (void)doData_(static_cast<DataOp>(o), o % kPartitions);
        doSendOffsets_();
        doAbort_();
        ASSERT_TRUE(drive([&] { return model_.state == txm::State::Ready && model_.idle(); })) << "the warm-up abort never completed";
        for (int o = 0; o < kDataOps; ++o) (void)doData_(static_cast<DataOp>(o), 0);   // refused: the gate is closed
        // The backlog high-water marks (the builder, the ack ring): every partition filled until back-pressured with
        // the broker unreachable - once while its AddPartitions is outstanding, once after it was added.
        ASSERT_NO_FATAL_FAILURE(backlog_(/*addedFirst=*/false));
        ASSERT_NO_FATAL_FAILURE(backlog_(/*addedFirst=*/true));
        // In-flight slot buffers circulate with the builder's (ProduceFrameBuilder::swapFrameInto) and converge to
        // its capacity one swap at a time: one batch per slot after the backlog's high-water mark warms every slot.
        ASSERT_NO_FATAL_FAILURE(cycleSlots_());
        doCommit_();        // 19: no transaction
        doAbort_();         // 19
        doSendOffsets_();   // 19
        doFlush_();
        ASSERT_TRUE(drive([&] { return model_.idle(); }));
        warm_ = true;
        note("producer epoch " + std::to_string(epoch_) + " warm");
    }
    void cycleSlots_() {
        doBegin_();
        for (int32_t part = 0; part < kPartitions; ++part) {
            for (int i = 0; i < 16; ++i) {   // > maxInFlight: every in-flight slot of the partition carries a batch
                (void)doData_(DataOp::TrySendAck, part);
                ASSERT_TRUE(drive([&] { return acceptedCum_ == ackedCum_; })) << "a slot-cycling batch was never acknowledged";
            }
        }
        doCommit_();
        ASSERT_TRUE(drive([&] { return model_.state == txm::State::Ready && model_.idle(); })) << "the slot-cycling commit never completed";
    }
    void backlog_(bool addedFirst) {
        doBegin_();
        if (addedFirst) {
            for (int32_t part = 0; part < kPartitions; ++part) (void)doData_(DataOp::TrySendAck, part);
            ASSERT_TRUE(drive([&] { return acceptedCum_ == ackedCum_; })) << "the backlog's first records were never acknowledged";
        }
        drop_();
        for (int32_t part = 0; part < kPartitions; ++part) {
            for (int i = 0; i < 150 && doData_(DataOp::TrySendAck, part); ++i) {}   // far beyond a random dead window
        }
        reconnect_();
        ASSERT_TRUE(drive([&] { return acceptedCum_ == ackedCum_; })) << "the backlog was never acknowledged";
        doCommit_();
        ASSERT_TRUE(drive([&] { return model_.state == txm::State::Ready && model_.idle(); })) << "the backlog commit never completed";
    }
    /// After FatalError: every token of the fenced producer completes, then a new producer (same id) replaces it.
    void replaceProducer() {
        ++replacements_;
        warm_ = false;   // closing the fenced producer and warming its replacement: not steady state
        note("replace the fenced producer (epoch " + std::to_string(epoch_) + ")");
        if (!drive([&] { return model_.idle(); }, std::chrono::seconds(5))) {
            fail("a token of the fenced producer never completed:" + model_.describeOutstanding());
            return;
        }
        settle();
        checkNoFrameAfterFence_();
        if (dead_) reconnect_();
        gw_->txnCoordCodes.clear();   // a scripted code still queued is dropped (the new producer starts clean)
        typed_.reset();
        p_.reset();
        settle();
        if (acceptedCum_ != ackedCum_) {
            fail("the fenced producer's records were not all reported in a BatchAck");
        }
        acceptedCum_  = {};
        ackedCum_     = {};
        lastTier2End_ = {};
        model_.replaced();
        epoch_ = static_cast<int16_t>(epoch_ + 1);   // a late decision of the fenced epoch never reaches the new model
        makeProducer();
    }

    // ---- ops (each: the model's verdict, the issue (measured after warm-up), the check) ----------------------------
    rc::Token tok() { return nextToken_++; }
    /// A unique value of a FIXED size (kValueBytes): the builder's high-water mark is bounded by the record count.
    static constexpr std::size_t kValueBytes = 24;
    std::string value(const char* tag) {
        char buf[kValueBytes + 1];
        std::snprintf(buf, sizeof buf, "%.4s-%u-%llu........................", tag, seed_, static_cast<unsigned long long>(nextValue_++));
        return std::string(buf, kValueBytes);
    }
    void measure_(long allocs, const char* what) {
        if (!warm_) return;
        ++measured_;
        if (allocs != 0) fail(std::string(what) + " allocated " + std::to_string(allocs) + " times after the warm-up");
    }
    template <class F>
    long window_(F&& body) {
        alloc_counter::startThisThread();
        body();
        return alloc_counter::stopThisThread();
    }

    bool doData_(DataOp op, int32_t part) {
        if ((op == DataOp::Send || op == DataOp::SendTo || op == DataOp::TypedSend) && voidMayBlock_(part)) {
            if (!dead_) (void)drive([&] { return !voidMayBlock_(part); }, std::chrono::milliseconds(300));
            if (voidMayBlock_(part)) {
                note(std::string(name(op)) + " p" + std::to_string(part) + ": a blocking void form here would wait for this rig's own broker - issued as trySendAck");
                op = DataOp::TrySendAck;
            }
        }
        const int64_t k = keys_[static_cast<std::size_t>(part)];
        const rc::Key key(k);
        std::vector<std::string> vals;
        int32_t zone = 0;   // the typed ops' unique field
        switch (op) {
        case DataOp::Batch:
            for (int i = 0; i < 3; ++i) vals.push_back(value("b"));
            break;
        case DataOp::ClaimBatch:
            for (int i = 0; i < 3; ++i) {
                char buf[16];
                std::snprintf(buf, sizeof buf, "c%07llu", static_cast<unsigned long long>(nextValue_++ % 10'000'000ull));
                vals.emplace_back(buf, 8);
            }
            break;
        case DataOp::TypedSend:
        case DataOp::TypedClaim:
            zone = static_cast<int32_t>(nextValue_++);
            vals.push_back(positionBytes(k, zone));
            break;
        default:
            vals.push_back(value(name(op)));
            break;
        }
        std::vector<const std::string*> v;
        for (const auto& s : vals) { keep_.push_back(s); v.push_back(&keep_.back()); }
        const bool tokenForm = op == DataOp::TrySendCq || op == DataOp::TrySendAck;
        const rc::Token t = tokenForm ? tok() : 0;
        rune::gen::Position rec;
        rec.userId = k;
        rec.zone   = zone;
        // The model is told after the issue: completions are only absorbed by the settle that follows.
        bool expectAccept = model_.accepts();
        enum class Got : uint8_t { Accepted, Refused, BackPressured, Other };
        std::string what;
        bool        threw = false;
        auto fromSend = [&](rc::SendResult r) {
            if (r == rc::SendResult::Ok) return Got::Accepted;
            if (r == rc::SendResult::NotInTransaction) return Got::Refused;
            if (r == rc::SendResult::BackPressured || r == rc::SendResult::ClaimHeld) return Got::BackPressured;
            what = "SendResult " + std::to_string(static_cast<int>(r));
            return Got::Other;
        };
        auto voidForm = [&](auto&& body) {
            try {
                body();
                return Got::Accepted;
            } catch (const std::logic_error& e) {
                threw = true;
                what  = e.what();
                return Got::Refused;
            } catch (const std::exception& e) {
                threw = true;
                what  = e.what();
                return Got::Other;
            }
        };
        auto issueOnce = [&]() -> Got {
            switch (op) {
            case DataOp::Send:       return voidForm([&] { p_->send(topic_, key, bytesOf(*v[0])); });
            case DataOp::SendTo:     return voidForm([&] { p_->sendTo(topic_, part, key, bytesOf(*v[0])); });
            case DataOp::TrySendCq:  return fromSend(p_->trySend(topic_, key, bytesOf(*v[0]), *cq_, t));
            case DataOp::TrySendAck: return fromSend(p_->trySend(topic_, key, bytesOf(*v[0]), rc::AckToken{t}));
            case DataOp::Batch: {
                rc::OpResult r = rc::OpResult::Accepted;
                const Got g = voidForm([&] {
                    rc::BatchBuilder bb = p_->batch(topic_, key);
                    bb.add(bytesOf(*v[0])).add(bytesOf(*v[1])).add(bytesOf(*v[2]));
                    r = bb.send();
                });
                if (g != Got::Accepted) return g;
                if (r == rc::OpResult::Accepted) return Got::Accepted;
                if (r == rc::OpResult::BackPressured) return Got::BackPressured;
                what = "batch OpResult " + std::to_string(static_cast<int>(r));
                return Got::Other;
            }
            case DataOp::TryClaim: {
                rc::Claim c = p_->tryClaim(topic_, key, static_cast<uint32_t>(v[0]->size()));
                if (!c) return fromSend(c.result());
                std::memcpy(c.buffer().data(), v[0]->data(), v[0]->size());
                if (c.commit()) return Got::Accepted;
                what = "claim commit false";
                return Got::Other;
            }
            case DataOp::ClaimBatch: {
                rc::BatchClaim bc = p_->claimBatch(topic_, key, 3, 8);
                if (!bc) return fromSend(bc.result());
                for (uint32_t i = 0; i < 3; ++i) std::memcpy(bc.record(i).data(), v[i]->data(), 8);
                if (bc.commit()) return Got::Accepted;
                what = "claimBatch commit false";
                return Got::Other;
            }
            case DataOp::TypedSend: return voidForm([&] { typed_->send(rec); });
            case DataOp::TypedClaim: {
                auto w = typed_->claim(k);
                if (!w) return fromSend(w.result());
                w.zone(zone);
                if (w.commit()) return Got::Accepted;
                what = "typed claim commit false";
                return Got::Other;
            }
            }
            return Got::Other;
        };
        Got got      = Got::Other;
        int attempts = 0;
        // Back-pressure: the duty cycle drains, the app retries (a real app's loop).  With the broker unreachable
        // (the gateway deaf) nothing drains - the op is not taken (no effect), as an app would retry it later.
        const auto bpDeadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(1500);
        bool gaveUp = false;
        for (;; ++attempts) {
            threw        = false;
            expectAccept = model_.accepts();   // the retries' settles may have moved the machine
            const long allocs = window_([&] { got = issueOnce(); });
            if (!threw) measure_(allocs, name(op));
            if (got != Got::BackPressured) break;
            if (dead_ && attempts >= 3) { gaveUp = true; break; }
            if (std::chrono::steady_clock::now() >= bpDeadline) break;
            settle();
        }
        const bool accepted = got == Got::Accepted;
        if (accepted) {
            acceptedRecords_ += static_cast<int>(vals.size());
            acceptedCum_[static_cast<std::size_t>(part)] += static_cast<int64_t>(vals.size());
            if (op == DataOp::Batch) lastTier2End_[static_cast<std::size_t>(part)] = acceptedCum_[static_cast<std::size_t>(part)];
            (void)model_.data(part, vals, t);
        }
        note(std::string(name(op)) + " p" + std::to_string(part) + (t != 0 ? " tok " + std::to_string(t) : "") +
             (accepted ? " accepted" : got == Got::Refused ? " refused" : gaveUp ? " not taken (back-pressured, broker unreachable)" : " FAILED") + (attempts > 0 ? " after " + std::to_string(attempts) + " back-pressured tries" : "") +
             (what.empty() ? "" : " (" + what + ")") + " [" + txm::name(model_.state) + "]");
        if (got == Got::BackPressured && !gaveUp) fail(std::string(name(op)) + " stayed back-pressured for 1.5 s with the broker reachable");
        // A closed gate (or a machine not InTransaction) refuses BEFORE any back-pressure check: never "not taken".
        if (gaveUp && !expectAccept) fail(std::string(name(op)) + " answered BackPressured where the model refuses it");
        else if (got == Got::Other) fail(std::string(name(op)) + " failed: " + what);
        else if (!gaveUp && accepted != expectAccept) {
            fail(std::string(name(op)) + (expectAccept ? " was refused; the model: accepted" : " was accepted; the model: refused"));
        }
        return accepted;
    }
    void doBegin_() {
        model_.begin();
        const long allocs = window_([&] { p_->beginTransaction(); });
        measure_(allocs, "beginTransaction");
        note("begin txn " + std::to_string(model_.txn));
    }
    void doCommit_() {
        const rc::Token t = tok();
        model_.commit(t);
        rc::OpResult r{};
        const long allocs = window_([&] { r = p_->commitTransaction(*cq_, t); });
        measure_(allocs, "commitTransaction");
        note("commit tok " + std::to_string(t));
        if (r != rc::OpResult::Accepted) {
            fail("commitTransaction returned " + std::to_string(static_cast<int>(r)));
            model_.complete(t, txm::kOk);
        }
    }
    void doAbort_() {
        const rc::Token t = tok();
        model_.abort(t);
        rc::OpResult r{};
        const long allocs = window_([&] { r = p_->abortTransaction(*cq_, t); });
        measure_(allocs, "abortTransaction");
        note("abort tok " + std::to_string(t));
        if (r != rc::OpResult::Accepted) {
            fail("abortTransaction returned " + std::to_string(static_cast<int>(r)));
            model_.complete(t, txm::kOk);
        }
    }
    void doFlush_() {
        const rc::Token t = tok();
        model_.flush(t);
        rc::OpResult r{};
        const long allocs = window_([&] { r = p_->flush(*cq_, t); });
        measure_(allocs, "flush");
        note("flush tok " + std::to_string(t));
        if (r != rc::OpResult::Accepted) {
            fail("flush returned " + std::to_string(static_cast<int>(r)));
            model_.complete(t, txm::kOk);
        }
    }
    void doSendOffsets_() {
        const rc::Token t   = tok();
        const int64_t   off = nextOffset_++;
        const rc::OffsetEntry e{kTopicId, 0, off};
        model_.sendOffsets(t, off);
        rc::OpResult r{};
        const long allocs = window_([&] {
            r = p_->sendOffsetsToTransaction(std::span<const rc::OffsetEntry>(&e, 1), group_, *cq_, t);
        });
        measure_(allocs, "sendOffsetsToTransaction");
        note("sendOffsets " + std::to_string(off) + " tok " + std::to_string(t));
        if (r != rc::OpResult::Accepted) {
            fail("sendOffsetsToTransaction returned " + std::to_string(static_cast<int>(r)));
            model_.complete(t, txm::kOk);
        }
    }
    void doSeek_() {
        controlPlane_ = true;
        if (seekToken_ != 0) return;   // one rewind at a time
        seekToken_     = tok();
        seekDisturbed_ = dead_;
        seekExpectA_   = model_.committedOffset;
        const rc::OpResult r = gc_->seekToCommitted(rc::CompletionTarget(*cq_), seekToken_);
        note("seekToCommitted tok " + std::to_string(seekToken_) + " (committed " + std::to_string(seekExpectA_) + ") offsetFetches " +
             std::to_string(gw_->offsetFetches) + " subs " + std::to_string(gw_->subscribed.size()) + " joins " + std::to_string(gw_->joinGroups));
        if (r != rc::OpResult::Accepted) {
            fail("seekToCommitted returned " + std::to_string(static_cast<int>(r)));
            seekToken_ = 0;
            return;
        }
        // Its OffsetFetch leaves on the group hub's next sweep (~100-200 ms): the step waits for the rewind, so it is
        // not always overtaken by the next session drop.
        // (A dropped session: it completes after the reconnect.)
        if (!dead_ && !drive([&] { return seekToken_ == 0; }, std::chrono::milliseconds(1500))) {
            fail("seekToCommitted never completed with the session up");
        }
    }
    void seekDone_(const txm::Result& r) {
        seekExpectB_ = model_.committedOffset;
        note("  seekToCommitted -> " + txm::TxnModel::str(r) + " offsetFetches " + std::to_string(gw_->offsetFetches) + " subs " +
             std::to_string(gw_->subscribed.size()) + " joins " + std::to_string(gw_->joinGroups));
        ++stats_["seek" + std::to_string(static_cast<int>(r.status))];
        if (r.status == rc::OpStatus::Ok) {
            ++stats_["seekOk"];
            const auto* sub = gw_->latestSubscription(kTopicId, 0);
            if (sub == nullptr) fail("seekToCommitted Ok but no subscription");
            else if (sub->fromOffset != seekExpectA_ && sub->fromOffset != seekExpectB_) {
                fail("seekToCommitted re-subscribed from " + std::to_string(sub->fromOffset) + ", the committed offset is " +
                     std::to_string(seekExpectB_));
            }
        } else if (!seekDisturbed_ || (r.status != rc::OpStatus::Rejoined && r.status != rc::OpStatus::Timeout &&
                                       r.status != rc::OpStatus::SessionLost)) {
            fail("seekToCommitted completed " + txm::TxnModel::str(r));
        }
        seekToken_ = 0;
    }
    void inject_(int16_t code) {
        gw_->txnCoordCodes.push_back(code);
        note("inject " + std::to_string(code) + " on the next coordinator answer");
    }
    void drop_() {
        controlPlane_ = true;
        if (dead_) return;
        dead_      = true;
        deadSteps_ = 0;
        if (seekToken_ != 0) seekDisturbed_ = true;
        const bool mute = std::uniform_int_distribution<int>(0, 1)(rng_) == 1;
        if (mute) {
            // Every produce is appended and every coordinator request decided (an EndTxn(commit) lands), but no
            // answer reaches the client: on the new session it re-sends - the broker deduplicates the batches.
            gw_->holdTxnReplies = true;
            for (int i = 1; i <= 4000; ++i) gw_->dropAnswer.insert(gw_->produceFrames + i);
            note("drop the session (the gateway goes mute: frames applied, answers lost)");
        } else {
            gw_->deadEndpoints.insert(0);
            note("drop the session (the gateway goes deaf)");
        }
    }
    void reconnect_() {
        controlPlane_ = true;
        const int connects = gw_->connects;
        gw_->deadEndpoints.erase(0);
        gw_->holdTxnReplies = false;
        gw_->dropAnswer.clear();
        dead_ = false;
        if (seekToken_ != 0) seekDisturbed_ = true;
        gw_->expireSession(gw_->lastUniqueSessionId());
        note("reconnect (the old session expired)");
        if (!drive([&] { return gw_->connects > connects; })) fail("the client did not reconnect");
    }

    void randomStep_() {
        std::uniform_int_distribution<int> pick(0, 99);
        int r = pick(rng_);
        if (dead_ && ++deadSteps_ > 6) r = 99;   // a dead window stays short
        auto data = [&] {
            const auto op   = static_cast<DataOp>(std::uniform_int_distribution<int>(0, kDataOps - 1)(rng_));
            const int32_t p = std::uniform_int_distribution<int32_t>(0, kPartitions - 1)(rng_);
            ++counts_[name(op)];
            (void)doData_(op, p);
        };
        if (r < 42) return data();
        if (r < 52) {
            if (model_.state != txm::State::Ready) return data();   // begin only after the previous end completed
            ++counts_["begin"];
            return doBegin_();
        }
        if (r < 61) { ++counts_["commit"]; return doCommit_(); }
        if (r < 66) { ++counts_["abort"]; return doAbort_(); }
        if (r < 70) { ++counts_["flush"]; return doFlush_(); }
        if (r < 76) { ++counts_["sendOffsets"]; return doSendOffsets_(); }
        if (r < 78) { ++counts_["seekToCommitted"]; return doSeek_(); }
        if (r < 81) { ++counts_["inject27"]; return inject_(27); }
        if (r < 84) { ++counts_["inject14"]; return inject_(14); }
        if (r < 87) { ++counts_["inject19"]; return inject_(19); }
        if (r < 89) { ++counts_["inject18"]; return inject_(18); }
        if (r < 94) { ++counts_["drop"]; return drop_(); }
        ++counts_["reconnect"];
        reconnect_();
    }

    /// The end: heal, end the open transaction, every token completes.
    void windDown_() {
        note("wind down");
        if (dead_) reconnect_();
        gw_->txnCoordCodes.clear();
        settle();
        for (int round = 0; round < 8; ++round) {
            if (model_.state == txm::State::Fatal) {
                replaceProducer();
                if (::testing::Test::HasFatalFailure()) return;
                continue;
            }
            if (model_.state == txm::State::InTxn) doCommit_();
            else if (model_.state == txm::State::Abortable) doAbort_();
            if (!drive([&] { return model_.idle() && seekToken_ == 0 &&
                                    (model_.state == txm::State::Ready || model_.state == txm::State::Fatal ||
                                     model_.state == txm::State::Abortable); })) {
                fail("hang: tokens still outstanding at the end:" + model_.describeOutstanding() + " (state " +
                     txm::name(model_.state) + ")");
                return;
            }
            if (model_.state == txm::State::Ready) break;
        }
        settle();
        checkReadCommitted_();
    }

public:
    void runImpl() {
        ASSERT_NO_FATAL_FAILURE(connect());
        ASSERT_NO_FATAL_FAILURE(groupReady());
        ASSERT_NO_FATAL_FAILURE(makeProducer());
        settle();
        for (step_ = 0; step_ < kSteps; ++step_) {
            if (model_.state == txm::State::Fatal) {
                ASSERT_NO_FATAL_FAILURE(replaceProducer());
            }
            note("#" + std::to_string(step_));
            const auto t0 = std::chrono::steady_clock::now();
            controlPlane_ = false;
            randomStep_();
            fatalSeen_ = false;
            settle();
            const auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - t0).count();
            if (ms > 250) note("  (slow step: " + std::to_string(ms) + " ms)");
            if (fatalSeen_) fencedMark_ = std::make_pair(gw_->txnFrames.size(), epoch_);
            if (model_.errors.size() > 0) break;   // the first divergence ends the run (its log is the evidence)
        }
        if (model_.errors.empty()) windDown_();
        if (::testing::Test::HasFatalFailure()) return;
        if (model_.errors.empty() && !model_.idle()) fail("tokens never completed:" + model_.describeOutstanding());
        checkFrameLog_();
        if (model_.errors.empty() && ackedRecords_ != acceptedRecords_) {
            fail("BatchAcks report " + std::to_string(ackedRecords_) + " records, " + std::to_string(acceptedRecords_) + " were accepted");
        }
    }
    void report() {
        if (model_.errors.empty() && !::testing::Test::HasFailure()) {
            if (std::getenv("RUNE_TXN_STRESS_LOG") == nullptr) return;   // pristine output; the summary on request
            std::string s, r;
            for (const auto& [k, n] : counts_) s += " " + k + "=" + std::to_string(n);
            for (const auto& [k, n] : stats_) r += " " + k + "=" + std::to_string(n);
            std::size_t committedBatches = 0, abortedBatches = 0;
            for (int32_t p = 0; p < kPartitions; ++p) {
                committedBatches += gw_->visibleBatches(p, Gw::TxnFate::Committed);
                abortedBatches += gw_->visibleBatches(p, Gw::TxnFate::Aborted);
            }
            std::printf("[seed %u] %d steps, %ld measured issues, %ld measured duty passes, %d replacements, %d epoch bumps, %zu committed / %zu aborted "
                        "batches, %d frames re-sent dup;%s |%s\n",
                        seed_, kSteps, measured_, measuredPasses_, replacements_, gw_->initPids - 1 - replacements_, committedBatches, abortedBatches,
                        gw_->windowDupAnswers, r.c_str(), s.c_str());
            for (const auto& l : log_) std::printf("    %s\n", l.c_str());
            return;
        }
        std::string all;
        for (const auto& e : model_.errors) all += "  " + e + "\n";
        std::string steps;
        for (const auto& l : log_) steps += "    " + l + "\n";
        std::string wire = "  gateway: produceFrames " + std::to_string(gw_->produceFrames) + " oos " + std::to_string(gw_->oosAnswers) +
                           " fenced " + std::to_string(gw_->fencedAnswers) + " unknownPid " + std::to_string(gw_->unknownPidAnswers) +
                           " identityMismatch " + std::to_string(gw_->identityMismatches) + " windowDup " +
                           std::to_string(gw_->windowDupAnswers) + " initPids " + std::to_string(gw_->initPids) + " connects " +
                           std::to_string(gw_->connects) + "\n  decisions:";
        for (const auto& d : gw_->coordDecisions) {
            wire += " " + std::to_string(static_cast<int>(d.kind)) + "/" + std::to_string(d.code) + "/e" + std::to_string(d.producerEpoch);
        }
        ADD_FAILURE() << "seed " << seed_ << ": " << model_.errors.size() << " violation(s)\n" << all << wire << "\n"
                      << "  step log (seed " << seed_ << "):\n" << steps;
    }
};

void Stress::run() {
    runImpl();
    report();
}

class TxnStressRegression : public ::testing::TestWithParam<uint32_t> {};

TEST_P(TxnStressRegression, SeededInterleavingsMatchTheModelRegression) {
    Stress s(GetParam());
    s.run();
}

std::vector<uint32_t> seeds() {
    int n = kDefaultSeeds;
    if (const char* e = std::getenv("RUNE_TXN_STRESS_SEEDS")) {
        const int v = std::atoi(e);
        if (v > 0) n = v;
    }
    std::vector<uint32_t> out;
    for (int i = 1; i <= n; ++i) out.push_back(static_cast<uint32_t>(i));
    return out;
}

}  // namespace

INSTANTIATE_TEST_SUITE_P(TxnStress, TxnStressRegression, ::testing::ValuesIn(seeds()),
                         [](const ::testing::TestParamInfo<uint32_t>& i) { return "Seed" + std::to_string(i.param); });
