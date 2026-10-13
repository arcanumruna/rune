// TransientDataPathStressTest.cpp — Phase 2b E2 interaction matrix, seeded
// model-checked stress over the SHARED state of one transient partition: the
// ring written by produce (plain + transactional, acks=leader and acks=all),
// its eviction floor moved by a REAL follower 2's fetches (ISR {1,2}: the HW is
// the follower's position), read by push reads at random cursors in both
// isolations, COMMIT / ABORT markers interleaved — plus the timings that reset
// that state: the follower restarting below the leader's ring start, the
// partition's ownership moving away and back (R9), a full restart (R12), and a
// restart into a topic whose catalog record has not committed (R1/R2).
//
// The reference model is independent of the code under test: it records every
// acked batch (base, count, bytes, CRC, transaction fate), derives every
// expected outcome from the plan's expectation table (`mx::expect`, rules
// R1-R12) plus the ring's documented rules (evict oldest-first only below the
// HW, only to make room; refuse an append that would evict at/above it), and is
// checked against the leader ring, the follower ring and every answer after
// EVERY step.  Documented rules the model also encodes: follower 2 leaves the
// ISR when it fetches below the HW (DUR-1 / KIP-966) and re-enters once caught
// up; an identical fetch may be conflated (spec 3.5); a control batch is never
// delivered; a transactional producer none of whose data batches is left in the
// ring may be unknown after an ownership change (idle: Kafka parity, KIP-360; an
// open transaction ALL of whose batches were evicted: the finding-B ruling's
// unrecoverable case) — an open transaction with a batch still in the ring stays
// known and keeps read_committed pinned (finding-B ruling, option (c)).
// 64 seeds x 3000 steps (the Regression gate) + 20 random seeds (smoke).  A
// failure prints its seed, step, and a delta-minimised op trace that replays
// deterministically (`replay()`): a real bug's Regression test pins it.
// RUNE_STRESS_VERBOSE=1 prints every step; RUNE_STRESS_NO_MINIMISE=1 skips the
// minimisation (survey runs).
#include "TransientRig.h"
#include "support/TransientMatrixExpectations.h"

#include <rune/replication/ReplicationMessages.h>

#include <spdlog/spdlog.h>

#include <algorithm>
#include <array>
#include <chrono>
#include <cstdlib>
#include <cstdio>
#include <map>
#include <memory>
#include <optional>
#include <random>
#include <set>
#include <sstream>
#include <string>
#include <vector>

using namespace transient_rig;

namespace {

namespace mx = rune::node::test::tmatrix;   // never `tm` (collides with <ctime>'s struct tm)
using rune::protocol::RecordBatchReader;

constexpr int16_t     kOk               = 0;
constexpr int16_t     kNotLeader        = static_cast<int16_t>(pc::ErrorCode::NotLeader);
constexpr int16_t     kTransientLogFull = static_cast<int16_t>(pc::ErrorCode::TransientLogFull);
constexpr int64_t     kPidBase          = 7;
constexpr int         kSteps            = 3000;
constexpr std::size_t kRingBytes        = 32 * 1024;
constexpr std::size_t kMaxBatches       = 1000;
constexpr int         kPassCap          = 64;   // every bounded wait in this file

uint32_t crcOf(const uint8_t* d) { return rune::protocol::detail::getLE32(d + rune::protocol::batch::kCrcOffset); }

// ---------------------------------------------------------------------------
// The op trace: every step is DATA (replayable, minimisable).  Cursors are
// resolved against the model at execution time, so a sub-trace stays valid.
// ---------------------------------------------------------------------------
enum class K : uint8_t { Produce, ProduceTxn, Marker, Fetch, Read, FollowerRestart, Disown, Reown, Restart, Commit };
enum class Cur : uint8_t { Earliest, Latest, Offset };

struct Step {
    K        k     = K::Produce;
    mx::Op   op    = mx::Op::ProduceAcks0;   // Produce: which produce row (acks byte = mx::wireAcks)
    uint8_t  iso   = 0;                      // Read
    Cur      cur   = Cur::Offset;            // Read
    bool     flag  = false;                  // Marker: commit; Restart: into an uncommitted topic
    int32_t  recs  = 1;                      // Produce
    int32_t  bytes = 100;                    // Produce
    uint32_t raw   = 0;                      // Read: cursor selector; Fetch: maxBytes
};

std::string str(const Step& s) {
    std::ostringstream o;
    switch (s.k) {
    case K::Produce:    o << "produce " << mx::name(s.op) << " recs=" << s.recs << " bytes=" << s.bytes; break;
    case K::ProduceTxn: o << "produceTxn"; break;
    case K::Marker:     o << "marker " << (s.flag ? "COMMIT" : "ABORT"); break;
    case K::Fetch:      o << "followerFetch maxBytes=" << s.raw; break;
    case K::Read:
        o << "pushRead iso=" << int(s.iso) << " cursor="
          << (s.cur == Cur::Earliest ? "earliest" : s.cur == Cur::Latest ? "latest" : "offset")
          << " raw=" << s.raw;
        break;
    case K::FollowerRestart: o << "followerRestart"; break;
    case K::Disown:          o << "ownershipAway"; break;
    case K::Reown:           o << "ownershipBack"; break;
    case K::Restart:         o << "fullRestart" << (s.flag ? " (topic uncommitted)" : ""); break;
    case K::Commit:          o << "topicCommitted"; break;
    }
    return o.str();
}

// Trace builders (the Regression tests below pin minimal traces with them).
Step produce(mx::Op op, int recs, int bytes) { Step s; s.k = K::Produce; s.op = op; s.recs = recs; s.bytes = bytes; return s; }
Step op(K k, bool flag = false) { Step s; s.k = k; s.flag = flag; return s; }
Step fetch(uint32_t maxBytes) { Step s; s.k = K::Fetch; s.raw = maxBytes; return s; }
Step read(uint8_t iso, Cur cur, uint32_t raw = 0) { Step s; s.k = K::Read; s.iso = iso; s.cur = cur; s.raw = raw; return s; }

// ---------------------------------------------------------------------------
// Coverage distribution.
// ---------------------------------------------------------------------------
struct Dist {
    std::array<uint64_t, 7>  timing{};    // mx::Timing of every classified op
    std::array<uint64_t, 10> outcome{};   // mx::Outcome observed (checked against the table)
    uint64_t ringNearFull = 0;            // ring class "either" (wrap padding decides): Ok or 42 both valid
    uint64_t uncommittedOps = 0, ringFullRefusals = 0, evictedReads = 0, followerResets = 0;
    uint64_t belowStartFetches = 0, leaderMoves = 0, restarts = 0, uncommittedRestarts = 0, commits = 0;
    uint64_t rcHeld = 0, rcBuildRetries = 0, acksAllAcked = 0, markersLanded = 0, markersRefused = 0;
    uint64_t gapFramesIgnored = 0, followerPartialApplies = 0, evictions = 0, steps = 0, seeds = 0;
    uint64_t isrShrinks = 0, isrExpands = 0, producerExpired = 0, conflatedFetches = 0, openTxnAllEvicted = 0, rebuildPins = 0;

    void add(const Dist& d) {
        for (std::size_t i = 0; i < timing.size(); ++i) timing[i] += d.timing[i];
        for (std::size_t i = 0; i < outcome.size(); ++i) outcome[i] += d.outcome[i];
        ringNearFull += d.ringNearFull; uncommittedOps += d.uncommittedOps; ringFullRefusals += d.ringFullRefusals;
        evictedReads += d.evictedReads; followerResets += d.followerResets; belowStartFetches += d.belowStartFetches;
        leaderMoves += d.leaderMoves; restarts += d.restarts; uncommittedRestarts += d.uncommittedRestarts;
        commits += d.commits; rcHeld += d.rcHeld; rcBuildRetries += d.rcBuildRetries; acksAllAcked += d.acksAllAcked;
        markersLanded += d.markersLanded; markersRefused += d.markersRefused; gapFramesIgnored += d.gapFramesIgnored;
        followerPartialApplies += d.followerPartialApplies; evictions += d.evictions; steps += d.steps;
        isrShrinks += d.isrShrinks; isrExpands += d.isrExpands; producerExpired += d.producerExpired; openTxnAllEvicted += d.openTxnAllEvicted; rebuildPins += d.rebuildPins; conflatedFetches += d.conflatedFetches;
        seeds += d.seeds;
    }
    [[nodiscard]] std::string line() const {
        std::ostringstream o;
        o << "seeds=" << seeds << " steps=" << steps << " | timing:";
        for (std::size_t i = 0; i < timing.size(); ++i)
            o << ' ' << mx::name(static_cast<mx::Timing>(i)) << '=' << timing[i];
        o << " RingNearFull=" << ringNearFull << " | outcome:";
        for (std::size_t i = 0; i < outcome.size(); ++i)
            if (outcome[i] != 0) o << ' ' << mx::name(static_cast<mx::Outcome>(i)) << '=' << outcome[i];
        o << " | ringFull(42)=" << ringFullRefusals << " evictedCursor=" << evictedReads
          << " followerReset=" << followerResets << " belowStartFetch=" << belowStartFetches
          << " leaderMove=" << leaderMoves << " restart=" << restarts << " uncommittedTopic=" << uncommittedRestarts
          << " (ops " << uncommittedOps << ", commits " << commits << ") evictions=" << evictions
          << " acksAllAcked=" << acksAllAcked << " markers landed/refused=" << markersLanded << '/' << markersRefused
          << " rcHeld=" << rcHeld << " rcBuildRetries=" << rcBuildRetries << " gapFrames=" << gapFramesIgnored
          << " followerPartial=" << followerPartialApplies << " isr out/in=" << isrShrinks << '/' << isrExpands << " conflatedFetch=" << conflatedFetches
          << " producerUnknown(28/20)=" << producerExpired << " openTxnAllEvicted=" << openTxnAllEvicted
          << " openTxnPinnedAtRingStart=" << rebuildPins;
        return o.str();
    }
};
Dist g_total;   // test-binary aggregate (printed once by the environment below)

// ---------------------------------------------------------------------------
// The model.
// ---------------------------------------------------------------------------
enum class Fate : uint8_t { Plain, Open, Committed, Aborted, Control };
struct MBatch {
    int64_t     base;
    int32_t     count;
    Fate        fate;
    std::size_t bytes;
    uint32_t    crc;
    int64_t     pid = -1;   // the transactional producer (data or marker); -1 = plain
    [[nodiscard]] int64_t end() const { return base + count; }
};

/// The partition's serving state — one explicit state, one code path each.
enum class Own : uint8_t { Owned, Disowned, Uncommitted };

/// The leader's FetchResponses / pushes, copied (the view points into its ring).
class CaptureTransport final : public rune::replication::PartitionTransport {
public:
    struct Frame {
        int64_t              hw, logStart;
        int16_t              err;
        std::vector<uint8_t> records;
    };
    bool sendFetchRequest(const rune::replication::FetchRequestMsg&) override { return true; }
    bool sendFetchResponse(rune::replication::NodeId to, const rune::replication::FetchResponseMsg& r) override {
        if (to != 2) return true;
        frames.push_back(Frame{r.highWatermark, r.logStartOffset, r.errorCode,
                               std::vector<uint8_t>(r.records.begin(), r.records.end())});
        return true;
    }
    std::vector<Frame> frames;
};

struct Failure {
    std::string id;       // the check that failed (stable across replays)
    std::string detail;
    int         step = -1;
};

#define REQ(cond, id, detail)                                         \
    do {                                                              \
        if (!(cond)) {                                                \
            std::ostringstream os_;                                   \
            os_ << detail;                                            \
            fail(id, os_.str());                                      \
            return;                                                   \
        }                                                             \
    } while (0)
#define BAIL() do { if (failed()) return; } while (0)

class Stress {
public:
    Stress() { boot(/*uncommitted=*/false); }

    /// Generate + execute `kSteps` from `seed`; then quiesce.  The executed trace is kept.
    void run(uint32_t seed, int steps = kSteps) {
        std::mt19937 rng(seed);
        for (step_ = 0; step_ < steps && !failed(); ++step_) {
            trace_.push_back(next(rng));
            exec(trace_.back());
        }
        if (!failed()) quiesce();
    }
    /// Deterministic replay of a trace (+ quiesce).
    void replay(const std::vector<Step>& t) {
        for (step_ = 0; step_ < static_cast<int>(t.size()) && !failed(); ++step_) {
            trace_.push_back(t[static_cast<std::size_t>(step_)]);
            exec(trace_.back());
        }
        if (!failed()) quiesce();
    }

    [[nodiscard]] bool failed() const { return failure_.has_value(); }
    [[nodiscard]] const Failure& failure() const { return *failure_; }
    [[nodiscard]] const std::vector<Step>& trace() const { return trace_; }
    [[nodiscard]] const Dist& dist() const { return d_; }

private:
    std::unique_ptr<CaptureTransport>           tx_;
    std::unique_ptr<Rig>                        rig_;
    std::unique_ptr<rune::storage::InMemoryLog> fmem_;   // follower 2's ring (outlives fol_)
    std::unique_ptr<rune::replication::NullPartitionTransport> ftx_;
    std::unique_ptr<PartitionReplica>           fol_;

    // ---- model ----
    std::vector<MBatch> batches_;   // every acked batch since the last restart, in offset order
    int64_t     leo_ = 0, hw_ = 0, ringStart_ = 0;
    std::size_t maxLen_ = 0;        // largest batch ever appended (bounds wrap padding)
    Own         own_ = Own::Owned;
    int64_t     pid_ = kPidBase;
    int32_t     seq_ = 0;
    bool        txnOpen_ = false;
    std::optional<bool> decision_;  // the coordinator's commit/abort, retried until it lands
    std::optional<bool> markerRetry_;   // a LANDED marker answered NotLeader (moved before its HW): re-sent
    bool        justRestarted_ = false;
    std::optional<Step> refusedUncommitted_;   // R2: the produce the client retries after the commit
    int64_t     fStart_ = 0, fLeo_ = 0;
    uint64_t    fResets_ = 0;
    // Follower 2's ISR membership (DUR-1 / KIP-966): a fetch below the HW (it restarted and
    // lost records) evicts it — the HW then follows the leader alone (minIsr 1) — and it
    // re-enters only once caught up.  The re-entry's moment is the leader's ISR cadence: the
    // model OBSERVES it and checks it was allowed.
    bool        inIsr_ = true;
    // The read_committed / producer state an ownership change rebuilds is scanned from the
    // ring: an IDLE transactional producer (no open transaction) none of whose data batches
    // is left there is unknown to the new owner — its next produce may answer
    // UnknownProducerId (28) / OutOfOrderSequence (20; a marker alone carries no sequence).
    // Kafka parity: retention drops an idle producer's state; the client re-inits (KIP-360).
    // An OPEN transaction with a batch still in the ring stays known and pinned (ruling
    // "finding B", option (c)): no allowance.  An open transaction ALL of whose batches were
    // evicted is unrecoverable (the ruling's documented case): the rebuilt state forgets it
    // (read_committed follows the evicted-cursor rule, its LSO pin is gone) and its next
    // transactional produce may be refused 28/20 — the client aborts, then re-inits.
    bool        producerLost_ = false;
    bool        openForgotten_ = false;      // the open txn was forgotten by the last rebuild
    bool        reinitAfterAbort_ = false;   // the client aborts the forgotten txn, then re-inits
    // Finding-B ruling, option (c): a rebuild that finds an open transaction in the ring whose
    // first in-ring batch is not provably its first (ring start > 0 and no marker of its
    // producer earlier in the ring) pins it at the ring start of that moment until it resolves.
    std::optional<int64_t> rebuildPin_;
    int64_t     lastFetch_ = -1, hwAtLastFetch_ = 0, hwAtStepStart_ = 0, leoAtLastFetch_ = -1;
    int64_t     incarnation_ = 1;   // follower 2's process incarnation

    struct Pending { enum Kind : uint8_t { AcksAll, Marker } kind; int64_t id; int64_t base; int64_t end; bool commit = false; };
    std::vector<Pending> pending_;                      // answers owed once the HW passes `end`
    std::set<int64_t>    heldReads_;                    // read_committed reads held while the RC state builds
    std::map<int64_t, std::pair<int16_t, int64_t>> produceAns_;   // corr  -> (code, base)
    std::map<int64_t, WorkerReturn>                readAns_;      // subId -> answer
    std::map<int64_t, std::pair<int16_t, int64_t>> markerAns_;    // token -> (code, base)

    int64_t corr_ = 0, sub_ = 1, token_ = 1;
    int     step_ = 0;
    bool    appendedThisStep_ = false;
    std::vector<Step>      trace_;
    std::optional<Failure> failure_;
    Dist                   d_;

    void fail(const std::string& id, const std::string& detail) {
        if (!failure_) failure_ = Failure{id, detail, step_};
    }

    // ---------------- generation ----------------
    Step next(std::mt19937& rng) {
        std::uniform_int_distribution<int> pick(0, 999);
        Step s;
        if (own_ == Own::Disowned && pick(rng) < 250) { s.k = K::Reown; return s; }
        if (own_ == Own::Uncommitted && pick(rng) < 250) { s.k = K::Commit; return s; }
        const int p = pick(rng);
        if (p < 3) { s.k = K::Restart; s.flag = std::bernoulli_distribution(0.5)(rng); return s; }
        if (p < 10) { s.k = K::FollowerRestart; return s; }
        if (p < 20 && own_ == Own::Owned) { s.k = K::Disown; return s; }
        if (p < 400) {
            static constexpr std::array<mx::Op, 4> ops{mx::Op::ProduceAcks0, mx::Op::ProduceAcks1,
                                                       mx::Op::ProduceAcksAll, mx::Op::ProduceAcks0};
            s.k = K::Produce;
            s.op = ops[std::uniform_int_distribution<std::size_t>(0, ops.size() - 1)(rng)];
            s.recs = std::uniform_int_distribution<int>(1, 3)(rng);
            s.bytes = std::uniform_int_distribution<int>(100, 1500)(rng);
            return s;
        }
        if (p < 550) { s.k = K::Fetch; s.raw = std::uniform_int_distribution<uint32_t>(4096, 16384)(rng); return s; }
        if (p < 750) {
            s.k = K::Read;
            s.iso = std::bernoulli_distribution(0.3)(rng) ? 1 : 0;
            const int c = std::uniform_int_distribution<int>(0, 99)(rng);
            s.cur = c < 15 ? Cur::Earliest : c < 30 ? Cur::Latest : Cur::Offset;
            s.raw = rng();
            return s;
        }
        if (p < 870) { s.k = K::ProduceTxn; return s; }
        s.k = K::Marker;
        s.flag = std::bernoulli_distribution(0.5)(rng);
        return s;
    }

    // ---------------- rig plumbing ----------------
    void boot(bool uncommitted) {
        fol_.reset(); fmem_.reset(); rig_.reset();
        tx_  = std::make_unique<CaptureTransport>();
        rig_ = std::make_unique<Rig>(kRingBytes, kMaxBatches, /*withFollower=*/true, tx_.get());
        ftx_  = std::make_unique<rune::replication::NullPartitionTransport>();
        newFollower();
        own_ = uncommitted ? Own::Uncommitted : Own::Owned;
        if (uncommitted) { rig_->worker().setOwnedDirect({}); rig_->pass(); }
        batches_.clear(); pending_.clear(); heldReads_.clear();
        produceAns_.clear(); readAns_.clear(); markerAns_.clear();
        leo_ = hw_ = ringStart_ = 0;
        seq_ = 0; txnOpen_ = false; decision_.reset(); markerRetry_.reset(); refusedUncommitted_.reset();
        producerLost_ = openForgotten_ = reinitAfterAbort_ = false;
        rebuildPin_.reset();
        inIsr_ = true; lastFetch_ = -1; hwAtLastFetch_ = 0; hwAtStepStart_ = 0; leoAtLastFetch_ = -1;
    }
    /// Follower 2 as a fresh process: an empty ring of the same (node-wide) size, a new incarnation.
    void newFollower() {
        fol_.reset(); fmem_.reset();
        fmem_ = std::make_unique<rune::storage::InMemoryLog>(kRingBytes, kMaxBatches);
        rune::replication::PartitionConfig c;
        c.topicId = kTopic; c.partitionId = 0; c.selfId = 2; c.minIsr = 1;
        c.replicaLagMaxOffset = 1'000'000'000; c.replicaLagMaxMs = 600'000; c.followerFlushIntervalNs = 0;
        fol_ = std::make_unique<PartitionReplica>(c, Role::Follower, 1, std::vector<int32_t>{1, 2},
                                                  std::vector<int32_t>{1, 2}, fmem_.get(), ftx_.get());
        fStart_ = fLeo_ = 0; fResets_ = 0;
        ++incarnation_;
    }
    void setOwned(bool owned) {
        Worker::OwnedSet s;
        if (owned) s.push_back(rig_->rep);
        rig_->worker().setOwnedDirect(std::move(s));
    }
    void drain() {
        rig_->pool.drainAllReturns([&](WorkerReturn r) {
            switch (r.type) {
            case WorkerReturn::Type::ProduceResult:
                if (r.deferAck) return;   // facts only, no answer
                produceAns_[r.correlationId] = {r.errorCode, r.baseOffset};
                break;
            case WorkerReturn::Type::PushReadResult: readAns_.insert_or_assign(r.subscriptionId, std::move(r)); break;
            case WorkerReturn::Type::ControlRecordAppended: markerAns_[r.correlationId] = {r.errorCode, r.baseOffset}; break;
            case WorkerReturn::Type::ControlRecordRefused: markerAns_[r.correlationId] = {r.errorCode, -1}; break;
            default: break;
            }
        });
    }
    void passDrain(int n = 1) { for (int i = 0; i < n; ++i) { rig_->pass(); drain(); } }

    // ---------------- ring-rule helpers (the model's R10 / retention bounds) ----------------
    enum class RingClass : uint8_t { MustAccept, MustRefuse, Either };
    [[nodiscard]] std::size_t firstHeldIdx(int64_t off) const {   // first model batch with end() > off
        return static_cast<std::size_t>(std::upper_bound(batches_.begin(), batches_.end(), off,
            [](int64_t o, const MBatch& b) { return o < b.end(); }) - batches_.begin());
    }
    [[nodiscard]] const MBatch* containing(int64_t off) const {
        const std::size_t i = firstHeldIdx(off);
        return i < batches_.size() && batches_[i].base <= off ? &batches_[i] : nullptr;
    }
    /// R10: an append of `len` bytes must never evict a batch whose last offset is >= the HW.
    /// The ring may waste < one batch of wrap padding before the new batch and keep < one
    /// batch of padding live, so between the two bounds either answer is valid.
    [[nodiscard]] RingClass ringClass(std::size_t len) const {
        std::size_t protectedBytes = 0, protectedCount = 0;
        for (std::size_t i = firstHeldIdx(hw_); i < batches_.size(); ++i) {   // end() > hw  <=>  last >= hw
            protectedBytes += batches_[i].bytes;
            ++protectedCount;
        }
        const std::size_t maxLen = std::max(maxLen_, len);
        if (protectedBytes + len > kRingBytes || protectedCount + 1 > kMaxBatches) return RingClass::MustRefuse;
        if (protectedBytes + len + 2 * maxLen <= kRingBytes) return RingClass::MustAccept;
        return RingClass::Either;
    }
    /// Retention: the ring evicts oldest-first, only to make room: the LAST batch it evicted
    /// could not have stayed (its bytes onward + the new batch + padding exceed the ring).
    void checkEvictionWasNeeded(int64_t oldStart, std::size_t len) {
        const int64_t newStart = rig_->mem.logStartOffset();
        if (newStart == oldStart) return;
        ++d_.evictions;
        REQ(appendedThisStep_, "evict.withoutAppend", "the ring start moved " << oldStart << "->" << newStart
                                                       << " on a step that appended nothing");
        const std::size_t lastEvicted = firstHeldIdx(newStart - 1);
        REQ(lastEvicted < batches_.size() && batches_[lastEvicted].end() == newStart, "evict.notBoundary",
            "the ring start " << newStart << " is not a batch boundary");
        REQ(batches_[lastEvicted].end() <= hw_, "evict.aboveHw",
            "evicted [" << batches_[lastEvicted].base << "," << newStart << ") at/above the HW " << hw_);
        std::size_t bytesFrom = 0, countFrom = 0;
        for (std::size_t i = lastEvicted; i + 1 < batches_.size(); ++i) { bytesFrom += batches_[i].bytes; ++countFrom; }
        REQ(bytesFrom + 2 * len + maxLen_ > kRingBytes || countFrom + 1 > kMaxBatches, "evict.unneeded",
            "evicted the batch at " << batches_[lastEvicted].base << " though " << bytesFrom << " held bytes + a "
                                    << len << "-byte append fit the " << kRingBytes << "-byte ring");
    }

    [[nodiscard]] mx::Entity entity() const {
        return mx::Entity{mx::Persist::Transient, mx::Mode::Shard, mx::Role::Leader,
                          own_ == Own::Uncommitted ? mx::Commit::Uncommitted : mx::Commit::Committed};
    }
    void count(mx::Timing t, mx::Outcome o) {
        ++d_.timing[static_cast<std::size_t>(t)];
        ++d_.outcome[static_cast<std::size_t>(o)];
    }

    // ---------------- execution ----------------
    void exec(const Step& s) {
        ++d_.steps;
        appendedThisStep_ = false;
        hwAtStepStart_ = hw_;
        const int64_t startBefore = rig_->mem.logStartOffset();
        std::size_t appendLen = 0;
        switch (s.k) {
        case K::Produce:    appendLen = doProduce(s, /*txn=*/false, /*afterCommit=*/false); break;
        case K::ProduceTxn: appendLen = doProduce(s, /*txn=*/true, false); break;
        case K::Marker:     appendLen = doMarker(s); break;
        case K::Fetch:      doFetch(s.raw); break;
        case K::Read:       doRead(s); break;
        case K::FollowerRestart: newFollower(); break;
        case K::Disown:
            if (own_ != Own::Owned) break;
            setOwned(false); passDrain(4); own_ = Own::Disowned; ++d_.leaderMoves;
            break;
        case K::Reown:
            if (own_ != Own::Disowned) break;
            setOwned(true); passDrain(4); own_ = Own::Owned;
            if (std::none_of(batches_.begin() + static_cast<std::ptrdiff_t>(firstHeldIdx(ringStart_)), batches_.end(),
                             [&](const MBatch& b) { return b.pid == pid_ && b.fate != Fate::Control; })) {
                producerLost_ = true;
                if (txnOpen_) { openForgotten_ = true; ++d_.openTxnAllEvicted; }
            } else if (txnOpen_ && ringStart_ > 0) {
                bool markerFirst = false;   // a marker of this producer before its first in-ring open batch
                for (std::size_t i = firstHeldIdx(ringStart_); i < batches_.size(); ++i) {
                    const MBatch& b = batches_[i];
                    if (b.pid != pid_) continue;
                    if (b.fate == Fate::Control) { markerFirst = true; continue; }
                    if (b.fate == Fate::Open) break;
                }
                if (!markerFirst) { rebuildPin_ = ringStart_; ++d_.rebuildPins; }   // each rebuild re-pins
            }
            break;
        case K::Restart:
            boot(s.flag);
            ++d_.restarts;
            if (s.flag) ++d_.uncommittedRestarts;
            ++pid_;   // a restarted transactional client re-inits (InitProducerId): a fresh producer
            justRestarted_ = true;
            REQ(rig_->mem.logEndOffset() == 0 && rig_->mem.logStartOffset() == 0 && rig_->rep->highWatermark() == 0,
                "restart.notEmpty", "R12: a transient topic kept data across a full restart");
            break;
        case K::Commit:
            if (own_ != Own::Uncommitted) break;
            setOwned(true); passDrain(2); own_ = Own::Owned; ++d_.commits;
            if (refusedUncommitted_) {   // R2: the client's retry after the commit
                const Step retry = *refusedUncommitted_;
                refusedUncommitted_.reset();
                appendLen = doProduce(retry, false, /*afterCommit=*/true);
            }
            break;
        }
        BAIL();
        observeIsr(); BAIL();
        applyFrames(); BAIL();
        settleAnswers(); BAIL();
        if (s.k == K::Restart) { /* a new ring: nothing to compare against */ }
        else if (appendLen != 0) checkEvictionWasNeeded(startBefore, appendLen);
        else REQ(rig_->mem.logStartOffset() == startBefore, "evict.withoutAppend",
                 "the ring start moved " << startBefore << "->" << rig_->mem.logStartOffset()
                                         << " on a step that appended nothing");
        BAIL();
        checkInvariants();
        if (verbose_)
            std::printf("  #%d %-55s leo=%lld hw=%lld start=%lld isr2=%d follower=[%lld,%lld) own=%d\n", step_,
                        str(s).c_str(), (long long)leo_, (long long)hw_, (long long)ringStart_, int(inIsr_),
                        (long long)fStart_, (long long)fLeo_, int(own_));
    }
    const bool verbose_ = std::getenv("RUNE_STRESS_VERBOSE") != nullptr;

    void observeIsr() {
        const auto& isr = rig_->rep->isr();
        const bool in = std::find(isr.begin(), isr.end(), 2) != isr.end();
        if (in == inIsr_) return;
        REQ(in, "isr.shrink", "follower 2 left the ISR without fetching below the HW");
        REQ(lastFetch_ >= 0 && lastFetch_ >= hwAtLastFetch_, "isr.expand",
            "follower 2 re-entered the ISR without catching up (last fetch " << lastFetch_ << ", HW " << hwAtLastFetch_ << ")");
        inIsr_ = true;
        ++d_.isrExpands;
        // Re-entered before or after this step's append: the HW is the leader-alone value, or it
        // stayed at max(HW, min(LEO, its position)) — never backwards either way.
        const int64_t obs = rig_->rep->highWatermark();
        const int64_t stayed = std::max(hwAtStepStart_, std::min(leo_, lastFetch_));
        REQ(obs == hw_ || obs == stayed, "isr.expandHw",
            "after follower 2 re-entered the HW is " << obs << " (model: " << hw_ << " or " << stayed << ")");
        hw_ = obs;
    }

    /// One produce; returns the appended length (0 = nothing appended).
    std::size_t doProduce(const Step& s, bool txn, bool afterCommit) {
        // The transactional client cannot add this partition to a new transaction until the
        // previous one's marker is acknowledged (CONCURRENT_TRANSACTIONS): it waits.
        if (txn && (markerRetry_ || reinitAfterAbort_ || std::any_of(pending_.begin(), pending_.end(),
                                                [](const Pending& p) { return p.kind == Pending::Marker; })))
            return 0;
        const mx::Op op = txn ? mx::Op::ProduceAcks0 : s.op;
        const uint8_t acks = mx::wireAcks(op);
        const int n = txn ? 1 : s.recs;
        const auto batch = txn ? batchOf(1, 200, pid_, 0, seq_, /*txn=*/true)
                               : batchOf(n, static_cast<std::size_t>(s.bytes));
        const std::size_t len = batch.size();
        const RingClass rc = ringClass(len);

        // The cell (op, entity, timing) and its expected outcome(s), from the table.
        mx::Timing t = mx::Timing::Steady;
        if (afterCommit)                  t = mx::Timing::CommitLandsMidOp;
        else if (own_ == Own::Disowned)   t = mx::Timing::LeaderMovesMidOp;
        else if (own_ == Own::Owned) {
            if (justRestarted_)                  t = mx::Timing::Restart;
            else if (rc == RingClass::MustRefuse) t = mx::Timing::RingFullUncommitted;
        }
        const mx::Entity e = afterCommit ? mx::Entity{mx::Persist::Transient, mx::Mode::Shard, mx::Role::Leader,
                                                      mx::Commit::Uncommitted}
                                         : entity();
        if (own_ == Own::Uncommitted) ++d_.uncommittedOps;
        std::vector<mx::Outcome> allowed{mx::expect(op, e, t)};
        if (own_ == Own::Owned && rc == RingClass::Either && !afterCommit) {
            allowed.push_back(mx::expect(op, e, mx::Timing::RingFullUncommitted));
            ++d_.ringNearFull;
        }
        // R12 / R2 run on an empty-or-fresh ring: whatever they say, R10 still binds the ring.
        if (own_ == Own::Owned && rc == RingClass::MustRefuse && t != mx::Timing::RingFullUncommitted)
            allowed = {mx::Outcome::RetriableLogFull};
        if (own_ == Own::Owned && rc == RingClass::Either && t != mx::Timing::Steady)
            allowed.push_back(mx::Outcome::RetriableLogFull);
        justRestarted_ = false;
        for (mx::Outcome o : allowed)
            if (o == mx::Outcome::NotApplicable) { fail("model.na", std::string("a produce cell is NotApplicable: ") +
                                                                     std::string(mx::name(t))); return 0; }

        const int64_t leoBefore = leo_;
        int16_t code = -1;
        int64_t base = -1;
        for (int attempt = 0; attempt < 16; ++attempt) {   // txn: NotLeader while the RC state builds (retriable)
            const int64_t c = ++corr_;
            const auto frame = produceFrame(c, batch, acks, txn ? pid_ : -1, 0, txn ? seq_ : -1);
            rig_->pass(&frame);
            drain();
            code = -1;
            for (int i = 0; i < kPassCap; ++i) {
                if (auto it = produceAns_.find(c); it != produceAns_.end()) {
                    code = it->second.first; base = it->second.second; produceAns_.erase(it);
                    break;
                }
                if (acks == 1 && rig_->mem.logEndOffset() != leoBefore) {   // appended; the ack waits for the HW
                    pending_.push_back(Pending{Pending::AcksAll, c, leoBefore, leoBefore + n});
                    break;
                }
                passDrain();
            }
            const bool rcBuilding = txn && own_ == Own::Owned && code == kNotLeader &&
                                    rig_->mem.logEndOffset() == leoBefore;
            if (!rcBuilding) break;
            ++d_.rcBuildRetries;
        }
        const bool appended = rig_->mem.logEndOffset() != leoBefore;
        return finishProduce(s, txn, afterCommit, allowed, t, appended, code, base, leoBefore, n, len, crcOf(batch.data()));
    }

    std::size_t finishProduce(const Step& s, bool txn, bool afterCommit, const std::vector<mx::Outcome>& allowed,
                              mx::Timing t, bool appended, int16_t code, int64_t base, int64_t leoBefore, int n,
                              std::size_t len, uint32_t crc) {
        std::size_t out = 0;
        auto body = [&]() {
            mx::Outcome got;
            if (appended) {
                REQ(rig_->mem.logEndOffset() == leoBefore + n, "produce.wrongLength",
                    "a produce of " << n << " records moved the LEO " << leoBefore << "->" << rig_->mem.logEndOffset());
                if (code != -1) {
                    REQ(code == kOk, "produce.appendedButRefused", "appended, answered " << code);
                    REQ(base == leoBefore, "produce.offLogEnd", "landed at " << base << ", the LEO was " << leoBefore);
                    REQ(mx::wireAcks(txn ? mx::Op::ProduceAcks0 : s.op) == 0 || (inIsr_ ? hw_ : leoBefore + n) >= leoBefore + n,
                        "produce.acksAllBeforeHw", "an acks=all produce was acked before the HW passed it");
                }
                got = afterCommit ? mx::Outcome::OkAfterCommit
                    : t == mx::Timing::Restart ? mx::Outcome::OkEmptyAfterRestart : mx::Outcome::Ok;
                if (t == mx::Timing::Restart) REQ(leoBefore == 0, "restart.offsetsNotFromZero", "landed at " << leoBefore);
            } else {
                REQ(code != -1, "produce.unanswered", "a refused produce was never answered (a hang)");
                if (code == kTransientLogFull) { got = mx::Outcome::RetriableLogFull; ++d_.ringFullRefusals; }
                else if (code == kNotLeader)   got = mx::Outcome::RetriableNotLeader;
                else if (txn && producerLost_ && (code == 28 || code == 20)) {
                    ++d_.producerExpired;
                    if (txnOpen_) { decision_ = false; reinitAfterAbort_ = true; }   // abort it, then re-init
                    else { ++pid_; seq_ = 0; producerLost_ = false; }                 // re-init (KIP-360)
                    return;
                }
                else {
                    std::ostringstream o;
                    o << "a produce answered " << code << " (txn=" << txn << " pid=" << pid_ << " seq=" << seq_
                      << " txnOpen=" << txnOpen_ << " producerLost=" << producerLost_ << " ringStart=" << ringStart_
                      << "; this producer's batches:";
                    for (const MBatch& b : batches_) if (b.pid == pid_) o << ' ' << b.base << (b.fate == Fate::Control ? "c" : "");
                    o << ")";
                    fail("produce.code" + std::to_string(code), o.str());
                    return;
                }
            }
            const bool ok = std::any_of(allowed.begin(), allowed.end(), [&](mx::Outcome a) {
                if (a == got) return true;
                return appended && (a == mx::Outcome::Ok || a == mx::Outcome::OkEmptyAfterRestart ||
                                    a == mx::Outcome::OkAfterCommit);
            });
            std::ostringstream want;
            for (mx::Outcome a : allowed) want << mx::name(a) << ' ';
            REQ(ok, "produce.outcome", str(s) << (txn ? " (txn)" : "") << " in " << mx::name(t) << ": got "
                                              << mx::name(got) << ", the table says " << want.str());
            count(t, got);
            if (got == mx::Outcome::RetriableNotLeader && own_ == Own::Uncommitted && !txn) refusedUncommitted_ = s;
            if (!appended) return;
            appendedThisStep_ = true;
            batches_.push_back(MBatch{leoBefore, n, txn ? Fate::Open : Fate::Plain, len, crc, txn ? pid_ : -1});
            leo_ += n;
            if (!inIsr_) hw_ = leo_;   // the leader alone is the ISR
            maxLen_ = std::max(maxLen_, len);
            if (txn) { ++seq_; txnOpen_ = true; }
            out = len;
        };
        body();
        return out;
    }

    std::size_t doMarker(const Step& s) {
        if (!txnOpen_ && !markerRetry_) return 0;   // R1/R2: no transaction includes this partition
        if (!markerRetry_ && !decision_) decision_ = s.flag;   // the coordinator decided; it retries THAT decision
        const bool commit = markerRetry_ ? *markerRetry_ : *decision_;
        const auto bytes = rune::transaction::ControlMarker::build(
            pid_, 0, commit ? rune::transaction::ControlType::Commit : rune::transaction::ControlType::Abort, 0);
        const std::size_t len = bytes.size();
        const RingClass rc = ringClass(len);
        mx::Timing t = own_ == Own::Disowned ? mx::Timing::LeaderMovesMidOp
                     : rc == RingClass::MustRefuse ? mx::Timing::RingFullUncommitted : mx::Timing::Steady;
        std::vector<mx::Outcome> allowed{mx::expect(mx::Op::TxnMarker, entity(), t)};
        if (own_ == Own::Owned && rc == RingClass::Either) {
            allowed.push_back(mx::expect(mx::Op::TxnMarker, entity(), mx::Timing::RingFullUncommitted));
            ++d_.ringNearFull;
        }
        const int64_t token = token_++;
        const int64_t leoBefore = leo_;
        WorkerOp op;
        op.type = WorkerOp::Type::AppendControlRecord; op.topicId = kTopic; op.partition = 0;
        op.correlationId = token; op.producerId = pid_;
        op.recordBatchBytes = bytes;
        if (!rig_->pool.tryEnqueueOp(kTopic, 0, std::move(op))) { fail("marker.enqueue", "op ring full"); return 0; }
        for (int i = 0; i < kPassCap; ++i) {
            passDrain();
            if (markerAns_.count(token) != 0 || rig_->mem.logEndOffset() != leoBefore) break;
        }
        const bool appended = rig_->mem.logEndOffset() != leoBefore;
        std::size_t out = 0;
        auto body = [&]() {
            mx::Outcome got;
            if (appended) {
                REQ(rig_->mem.logEndOffset() == leoBefore + 1, "marker.length", "a marker appended more than one record");
                got = mx::Outcome::Ok;
                pending_.push_back(Pending{Pending::Marker, token, leoBefore, leoBefore + 1, commit});   // D9: may wait for the HW
                ++d_.markersLanded;
            } else {
                auto it = markerAns_.find(token);
                REQ(it != markerAns_.end(), "marker.unanswered", "a refused marker was never answered (a hang)");
                REQ(it->second.first == kTransientLogFull || it->second.first == kNotLeader, "marker.code",
                    "a refused marker answered " << it->second.first);
                markerAns_.erase(it);
                got = mx::Outcome::MarkerRetried;
                ++d_.markersRefused;
            }
            std::ostringstream want;
            for (mx::Outcome a : allowed) want << mx::name(a) << ' ';
            REQ(std::find(allowed.begin(), allowed.end(), got) != allowed.end(), "marker.outcome",
                str(s) << " in " << mx::name(t) << ": got " << mx::name(got) << ", the table says " << want.str());
            count(t, got);
            if (!appended) return;
            appendedThisStep_ = true;
            markerRetry_.reset();   // a re-sent marker finds no open batch: it changes no fate
            for (MBatch& b : batches_) if (b.fate == Fate::Open) b.fate = commit ? Fate::Committed : Fate::Aborted;
            batches_.push_back(MBatch{leoBefore, 1, Fate::Control, len, crcOf(bytes.data()), pid_});
            leo_ += 1;
            if (!inIsr_) hw_ = leo_;
            maxLen_ = std::max(maxLen_, len);
            txnOpen_ = false;
            decision_.reset();
            openForgotten_ = false;
            rebuildPin_.reset();
            if (reinitAfterAbort_) { reinitAfterAbort_ = false; producerLost_ = false; ++pid_; seq_ = 0; }
            out = len;
        };
        body();
        return out;
    }

    void doFetch(uint32_t maxBytes) {
        if (own_ != Own::Owned) return;   // the owning worker is elsewhere / there is no replica yet
        const int64_t f = fLeo_;
        const bool below = f < ringStart_;
        WorkerOp op;
        op.type = WorkerOp::Type::FetchRequest; op.topicId = kTopic; op.partition = 0; op.replica = rig_->rep;
        const rune::replication::FetchRequestMsg nf = fol_->nextFetch();   // its LEO + lineage (KIP-320)
        op.fetchRequestPayload.leaderEpoch      = nf.leaderEpoch;
        op.fetchRequestPayload.partition        = nf.partition;
        op.fetchRequestPayload.followerId       = nf.followerId;
        op.fetchRequestPayload.fetchOffset      = nf.fetchOffset;
        op.fetchRequestPayload.topicId          = kTopic;
        op.fetchRequestPayload.lastFetchedEpoch = nf.lastFetchedEpoch;
        op.fetchRequestPayload.maxBytes    = static_cast<int32_t>(maxBytes);
        op.fetchRequestPayload.incarnation = incarnation_;      // KIP-903: a restart is a new process
        op.fetchRequestPayload.nowNs       = rig_->now;
        const bool identical = f == lastFetch_ && leo_ == leoAtLastFetch_;   // spec 3.5: may be conflated
        const std::size_t framesBefore = tx_->frames.size();
        REQ(rig_->pool.tryEnqueueOp(kTopic, 0, std::move(op)), "fetch.enqueue", "op ring full");
        passDrain(4);
        if (inIsr_ && f < hw_) { inIsr_ = false; ++d_.isrShrinks; }   // DUR-1: it lost committed records
        hw_ = inIsr_ ? std::max(hw_, std::min(f, leo_))   // ISR {1,2}: min(leader LEO, follower position); never back
                     : leo_;                              // ISR {1}
        lastFetch_ = f; hwAtLastFetch_ = hw_; leoAtLastFetch_ = leo_;
        REQ(rig_->rep->highWatermark() == hw_, "fetch.hw",
            "after a fetch at " << f << " the HW is " << rig_->rep->highWatermark() << ", the model says " << hw_);
        if (below) {   // R11 FollowerBelowRingStart
            ++d_.belowStartFetches;
            if (identical && tx_->frames.size() == framesBefore) { ++d_.conflatedFetches; return; }
            REQ(tx_->frames.size() > framesBefore, "fetch.belowStartUnanswered",
                "a fetch at " << f << " below the ring start " << ringStart_ << " was never answered (a stall)");
            const auto& fr = tx_->frames.back();
            REQ(fr.records.empty() && fr.logStart == ringStart_, "fetch.belowStartAnswer",
                "the below-start answer carried " << fr.records.size() << " bytes, logStartOffset " << fr.logStart
                                                  << " (ring start " << ringStart_ << ")");
            count(mx::Timing::FollowerBelowRingStart, mx::Outcome::Ok);
        }
    }

    /// Every frame the leader sent follower 2 (pushes + fetch answers), applied in order.
    void applyFrames() {
        for (std::size_t i = 0; i < tx_->frames.size(); ++i) {
            applyFrame(tx_->frames[i]);
            BAIL();
        }
        tx_->frames.clear();
    }
    void applyFrame(const CaptureTransport::Frame& fr) {
        REQ(fr.err == 0, "frame.err", "a frame carried error " << fr.err);
        REQ(fr.logStart == ringStart_ || fr.logStart == rig_->mem.logStartOffset(), "frame.logStart",
            "a transient frame's logStartOffset " << fr.logStart << " is not the ring start " << ringStart_);
        REQ(fr.hw <= hw_, "frame.hw", "a frame carried HW " << fr.hw << " above the leader's " << hw_);
        // Its records: consecutive whole batches, each exactly an acked one.
        int64_t first = -1, end = -1;
        std::size_t pos = 0;
        while (pos < fr.records.size()) {
            RecordBatchReader b(fr.records.data() + pos, fr.records.size() - pos);
            REQ(b.totalLength() > 0 && pos + static_cast<std::size_t>(b.totalLength()) <= fr.records.size(),
                "frame.torn", "a frame carried a torn batch");
            const MBatch* mb = containing(b.baseOffset());
            REQ(mb != nullptr && mb->base == b.baseOffset() && mb->count == b.lastOffsetDelta() + 1 &&
                    mb->crc == crcOf(fr.records.data() + pos),
                "frame.batch", "a frame carried a batch at " << b.baseOffset() << " the model never acked as such");
            REQ(end == -1 || b.baseOffset() == end, "frame.gap", "a frame skipped offsets " << end << ".." << b.baseOffset());
            if (first == -1) first = b.baseOffset();
            end = mb->end();
            pos += static_cast<std::size_t>(b.totalLength());
        }
        // The follower's rules: below the leader's ring start -> restart the ring empty there;
        // then append the frame's new suffix if it is contiguous (a gap = a lost push: ignored).
        if (fr.logStart > fLeo_) { fStart_ = fLeo_ = fr.logStart; ++fResets_; ++d_.followerResets; }
        int64_t want = fLeo_;
        bool mustApply = false;
        if (first != -1 && first > fLeo_) ++d_.gapFramesIgnored;
        if (first != -1 && first <= fLeo_ && end > fLeo_) {
            want = end;
            std::size_t prot = 0, cnt = 0, mx = maxLen_;
            for (std::size_t i = firstHeldIdx(std::min(fLeo_, fr.hw)); i < batches_.size() && batches_[i].base < end; ++i) {
                prot += batches_[i].bytes; ++cnt;
            }
            mustApply = prot + 2 * mx <= kRingBytes && cnt <= kMaxBatches;
        }
        rune::replication::FetchResponseMsg m{};
        m.leaderEpoch = 1; m.partition = 0; m.topicId = kTopic;
        m.highWatermark = fr.hw; m.logStartOffset = fr.logStart; m.errorCode = fr.err;
        m.records = std::span<const uint8_t>(fr.records);
        const int64_t before = fLeo_;
        fol_->applyReplicated(m);
        const int64_t got = fol_->logEndOffset();
        REQ(fol_->transientLogResetsForTest() == fResets_, "follower.resets",
            "follower resets " << fol_->transientLogResetsForTest() << ", the model says " << fResets_);
        if (mustApply) {
            REQ(got == want, "follower.apply", "the follower at " << before << " applied a frame [" << first << ","
                                                                  << end << ") to LEO " << got << " (want " << want << ")");
        } else {
            REQ(got >= before && got <= want && (got == before || containing(got - 1) != nullptr), "follower.apply",
                "the follower at " << before << " moved to " << got << " on a frame [" << first << "," << end << ")");
            if (got != want) ++d_.followerPartialApplies;
        }
        if (got != before) {
            const MBatch* lastApplied = containing(got - 1);
            REQ(lastApplied != nullptr && lastApplied->end() == got, "follower.boundary",
                "the follower's LEO " << got << " is not a batch boundary");
        }
        fLeo_ = got;
    }

    /// Owed answers: an acks=all ack / a landed marker's answer is due once the HW passes it.
    void settleAnswers() {
        if (own_ == Own::Owned) {
            for (int i = 0; i < kPassCap; ++i) {
                bool due = false;
                for (const Pending& p : pending_) {
                    const bool answered = p.kind == Pending::AcksAll ? produceAns_.count(p.id) != 0
                                                                     : markerAns_.count(p.id) != 0;
                    if (!answered && hw_ >= p.end) due = true;
                }
                if (!due) break;
                passDrain();
            }
        }
        for (auto it = pending_.begin(); it != pending_.end();) {
            if (it->kind == Pending::AcksAll) {
                auto a = produceAns_.find(it->id);
                if (a == produceAns_.end()) {
                    REQ(own_ != Own::Owned || hw_ < it->end, "acksAll.unanswered",
                        "an acks=all produce [" << it->base << "," << it->end << ") is unanswered with the HW at " << hw_);
                    ++it; continue;
                }
                if (a->second.first == kOk) {
                    REQ(a->second.second == it->base, "acksAll.base", "acked at " << a->second.second << ", landed at " << it->base);
                    REQ(hw_ >= it->end, "acksAll.beforeHw", "an acks=all produce was acked before the HW passed it");
                    ++d_.acksAllAcked;
                } else {
                    REQ(a->second.first == kNotLeader && own_ != Own::Owned, "acksAll.code",
                        "a parked acks=all produce answered " << a->second.first);   // R9: the client retries
                }
                produceAns_.erase(a);
            } else {
                auto a = markerAns_.find(it->id);
                if (a == markerAns_.end()) {
                    REQ(own_ != Own::Owned || hw_ < it->end, "marker.answerOwed",
                        "a landed marker at " << it->base << " is unanswered with the HW at " << hw_);
                    ++it; continue;
                }
                if (a->second.first == kNotLeader && own_ != Own::Owned) {
                    // R9: ownership moved before the HW passed it — refused stale; the coordinator
                    // re-sends the same decision (a duplicate marker is harmless: no open batch).
                    const MBatch* m = containing(it->base);
                    REQ(m != nullptr && m->fate == Fate::Control, "marker.answer", "no marker at " << it->base);
                    markerRetry_ = it->commit;
                } else {
                    REQ(a->second.first == kOk && a->second.second == it->base, "marker.answer",
                        "a landed marker at " << it->base << " answered (" << a->second.first << ", " << a->second.second << ")");
                }
                markerAns_.erase(a);
            }
            it = pending_.erase(it);
        }
        // read_committed reads answered late (held while the RC state built): still bounded by the LSO.
        for (auto it = heldReads_.begin(); it != heldReads_.end();) {
            auto a = readAns_.find(*it);
            if (a == readAns_.end()) { ++it; continue; }
            checkChunk(a->second, /*iso=*/1, lso(), -1); BAIL();
            readAns_.erase(a);
            it = heldReads_.erase(it);
        }
        REQ(produceAns_.empty(), "answer.stray", "a produce answer nobody asked for (corr " << produceAns_.begin()->first << ")");
        REQ(markerAns_.empty(), "answer.stray", "a marker answer nobody asked for (token " << markerAns_.begin()->first << ")");
    }

    /// read_committed bound: the HW, pinned at the first offset of an OPEN transaction.
    [[nodiscard]] int64_t lso() const {
        const int64_t bound = rebuildPin_ ? std::min(*rebuildPin_, hw_) : hw_;
        for (std::size_t i = firstHeldIdx(-1); i < batches_.size(); ++i)
            if (batches_[i].fate == Fate::Open && !openForgotten_ && batches_[i].base < hw_)
                return std::min(bound, batches_[i].base);
        return bound;
    }
    /// Plan, "Every cell asserts" 3: never a control batch (either isolation); read_committed
    /// additionally never an aborted or open transaction.
    [[nodiscard]] static bool deliverable(Fate f, uint8_t iso) {
        return iso == 0 ? f != Fate::Control : f == Fate::Plain || f == Fate::Committed;
    }

    /// The batches of one PushRead answer: each exactly an acked one, below the bound,
    /// read_committed sees no open / aborted / control batch, no deliverable batch skipped.
    /// `from` >= 0: the read's effective start (contiguity from there is checked).
    void checkChunk(const WorkerReturn& r, uint8_t iso, int64_t bound, int64_t from) {
        const auto chunk = r.chunkBytes();
        std::size_t pos = 0;
        int64_t expectNext = from;   // iso 0: the next base; iso 1: the first offset not yet accounted for
        while (pos < chunk.size()) {
            RecordBatchReader b(chunk.data() + pos, chunk.size() - pos);
            REQ(b.totalLength() > 0 && pos + static_cast<std::size_t>(b.totalLength()) <= chunk.size(), "read.torn",
                "a torn batch in a push read");
            const int64_t base = b.baseOffset();
            const MBatch* mb = containing(base);
            REQ(mb != nullptr, "read.unknownBatch", "delivered a batch the model never acked at " << base);
            REQ(mb->base == base && mb->count == b.lastOffsetDelta() + 1 && mb->crc == crcOf(chunk.data() + pos),
                "read.batchDiffers", "the batch delivered at " << base << " differs from the acked one");
            REQ(mb->end() <= bound, "read.pastBound", "delivered [" << base << "," << mb->end() << ") past the "
                                                                    << (iso ? "LSO " : "HW ") << bound);
            REQ(mb->fate != Fate::Control, "read.marker", "a control batch was delivered at " << base);
            if (iso == 1) {
                REQ(mb->fate != Fate::Aborted, "read.rcAborted", "an aborted batch reached read_committed at " << base);
                REQ(mb->fate != Fate::Open, "read.rcOpen", "an open transaction reached read_committed at " << base);
            }
            if (expectNext >= 0) {
                for (std::size_t i = firstHeldIdx(expectNext); i < batches_.size() && batches_[i].base < base; ++i)
                    REQ(!deliverable(batches_[i].fate, iso), "read.hole",
                        "iso " << int(iso) << " skipped the deliverable batch at " << batches_[i].base
                               << " (delivered " << basesOf(chunk) << ")");
                expectNext = mb->end();
            }
            pos += static_cast<std::size_t>(b.totalLength());
        }
    }

    static std::string basesOf(std::span<const uint8_t> chunk) {
        std::ostringstream o;
        std::size_t pos = 0;
        while (pos + 12 <= chunk.size()) {
            RecordBatchReader b(chunk.data() + pos, chunk.size() - pos);
            if (b.totalLength() <= 0) break;
            o << '[' << b.baseOffset() << ".." << b.baseOffset() + b.lastOffsetDelta() << (b.isControl() ? " ctl" : "") << ']';
            pos += static_cast<std::size_t>(b.totalLength());
        }
        return o.str();
    }

    void doRead(const Step& s) {
        if (own_ != Own::Owned) {   // R9 (moved away) / R1 (uncommitted): no Records while disowned
            const int64_t sub = sub_++;
            const uint64_t chunksBefore = rig_->worker().pushReadChunksForTest();
            rig_->pushRead(sub, 0, s.iso);
            passDrain(4);
            REQ(rig_->worker().pushReadChunksForTest() == chunksBefore, "read.disownedRingRead",
                "a worker that does not own the partition read its ring (a cross-thread read)");
            const mx::Op op = s.iso == 1 ? mx::Op::ReadCommitted : mx::Op::SubEarliest;
            const mx::Timing t = own_ == Own::Disowned ? mx::Timing::LeaderMovesMidOp : mx::Timing::Steady;
            const mx::Outcome want = mx::expect(op, entity(), t);
            if (own_ == Own::Uncommitted) ++d_.uncommittedOps;
            if (auto a = readAns_.find(sub); a != readAns_.end()) {
                REQ(a->second.chunkBytes().empty(), "read.disowned",
                    "a worker that does not own the partition delivered " << a->second.chunkBytes().size() << " bytes");
                readAns_.erase(a);
            }
            count(t, want);
            return;
        }
        const int64_t c = s.cur == Cur::Earliest ? 0 : s.cur == Cur::Latest ? hw_
                        : static_cast<int64_t>(s.raw % static_cast<uint64_t>(leo_ + 3));
        const mx::Op op = s.iso == 1 ? mx::Op::ReadCommitted
                        : s.cur == Cur::Earliest ? mx::Op::SubEarliest
                        : s.cur == Cur::Latest   ? mx::Op::SubLatest : mx::Op::SubOffset;
        const mx::Timing t = justRestarted_ ? mx::Timing::Restart
                           : c < ringStart_ ? mx::Timing::EvictedCursor : mx::Timing::Steady;
        justRestarted_ = false;
        const mx::Outcome want = mx::expect(op, entity(), t);
        const uint64_t skippedBefore = rig_->worker().transientRecordsSkippedForTest();
        const int64_t sub = sub_++;
        rig_->pushRead(sub, c, s.iso);
        for (int i = 0; i < kPassCap && readAns_.count(sub) == 0; ++i) passDrain();
        auto a = readAns_.find(sub);
        if (a == readAns_.end()) {
            REQ(s.iso == 1, "read.unanswered", "a read_uncommitted read at " << c << " was never answered (a hang)");
            heldReads_.insert(sub);   // held while the RC state builds; answered (and checked) later
            ++d_.rcHeld;
            return;
        }
        const WorkerReturn r = std::move(a->second);
        readAns_.erase(a);
        if (r.pushRcNotReady()) {
            REQ(s.iso == 1 && r.chunkBytes().empty(), "read.rcNotReady", "an RC-not-ready answer carried data");
            ++d_.rcHeld;
            return;
        }
        const int64_t start = std::max(c, ringStart_);
        const int64_t bound = s.iso == 1 ? lso() : hw_;
        const uint64_t skipped = rig_->worker().transientRecordsSkippedForTest() - skippedBefore;
        REQ(skipped == static_cast<uint64_t>(start - c), "read.skipCount",
            "a read at " << c << " (ring start " << ringStart_ << ") counted " << skipped << " skipped records");
        // The first batch the read may deliver: the one holding `start` (a cursor inside a batch re-reads it).
        const MBatch* at = containing(start);
        const int64_t from = at != nullptr ? at->base : start;
        checkChunk(r, s.iso, bound, from); BAIL();
        const auto chunk = r.chunkBytes();
        mx::Outcome got = mx::Outcome::Ok;
        if (!chunk.empty()) {
            const int64_t firstBase = RecordBatchReader(chunk.data(), chunk.size()).baseOffset();
            if (c < ringStart_) {
                ++d_.evictedReads;
                REQ(firstBase >= ringStart_, "read.evictedStart",   // + no deliverable hole from it (checkChunk)
                    "a cursor " << c << " below the ring start " << ringStart_ << " resumed at " << firstBase);
                got = mx::Outcome::OkFromRingStart;
            }
        } else {
            // Never a stall: a deliverable batch within the bound must come out (or the cursor must move past
            // filtered read_committed batches).
            bool any = false;
            for (std::size_t i = firstHeldIdx(start); i < batches_.size() && batches_[i].end() <= bound; ++i)
                if (deliverable(batches_[i].fate, s.iso)) { any = true; break; }
            REQ(!any || r.newNextOffset > start, "read.stall",
                "a read at " << c << " delivered nothing below the " << (s.iso ? "LSO " : "HW ") << bound);
            if (c < ringStart_) { ++d_.evictedReads; got = mx::Outcome::OkFromRingStart; }
        }
        if (want == mx::Outcome::OkEmptyAfterRestart) got = want;   // R12: only post-restart records (checked above)
        if (want == mx::Outcome::Ok && op == mx::Op::SubLatest && got == mx::Outcome::OkFromRingStart) got = want;
        REQ(got == want || (want == mx::Outcome::OkFromRingStart && chunk.empty()), "read.outcome",
            str(s) << " at " << c << " in " << mx::name(t) << ": got " << mx::name(got) << ", the table says "
                   << mx::name(want));
        count(t, want);
    }

    void checkInvariants() {
        const int64_t start = rig_->mem.logStartOffset();
        REQ(rig_->mem.logEndOffset() == leo_, "inv.leo", "the ring's LEO " << rig_->mem.logEndOffset() << ", the model's " << leo_);
        REQ(rig_->rep->highWatermark() == hw_, "inv.hw", "the HW " << rig_->rep->highWatermark() << ", the model's " << hw_);
        REQ(start <= hw_, "inv.evictedUncommitted", "the ring start " << start << " is above the HW " << hw_);
        REQ(start >= ringStart_, "inv.startBackwards", "the ring start moved backwards " << ringStart_ << "->" << start);
        ringStart_ = start;
        if (start < leo_) {
            const MBatch* first = containing(start);
            REQ(first != nullptr && first->base == start, "inv.startBoundary", "the ring start " << start << " is not a batch boundary");
        }
        for (std::size_t i = firstHeldIdx(start); i < batches_.size(); ++i) {   // every held batch is exactly the acked one
            const MBatch& b = batches_[i];
            const auto v = rig_->mem.readView(b.base, 1);
            REQ(!v.empty(), "inv.unreadable", "held offset " << b.base << " unreadable");
            RecordBatchReader r(v.data, v.len);
            REQ(r.baseOffset() == b.base && r.lastOffsetDelta() + 1 == b.count && crcOf(v.data) == b.crc,
                "inv.heldDiffers", "the ring's batch at " << b.base << " differs from the acked one");
        }
        // The follower's ring: exactly the acked batches it holds, its LEO the model's.
        REQ(fmem_->logEndOffset() == fLeo_, "inv.followerLeo", "the follower's LEO " << fmem_->logEndOffset() << ", the model's " << fLeo_);
        const int64_t fs = fmem_->logStartOffset();
        REQ(fs >= fStart_ && fs <= fLeo_, "inv.followerStart", "the follower's ring start " << fs << " outside [" << fStart_ << "," << fLeo_ << "]");
        fStart_ = fs;
        for (std::size_t i = firstHeldIdx(fs); i < batches_.size() && batches_[i].base < fLeo_; ++i) {
            const MBatch& b = batches_[i];
            const auto v = fmem_->readView(b.base, 1);
            REQ(!v.empty(), "inv.followerUnreadable", "the follower's offset " << b.base << " unreadable");
            RecordBatchReader r(v.data, v.len);
            REQ(r.baseOffset() == b.base && r.lastOffsetDelta() + 1 == b.count && crcOf(v.data) == b.crc,
                "inv.followerDiffers", "the follower's batch at " << b.base << " differs from the acked one");
        }
    }

    /// The end of every run: the partition serves again, the transaction ends, the follower
    /// catches up — then nothing may be left unanswered (no hangs) and the rings agree.
    void quiesce() {
        step_ = static_cast<int>(trace_.size());
        if (own_ == Own::Disowned) { Step s; s.k = K::Reown; exec(s); BAIL(); }
        if (own_ == Own::Uncommitted) { Step s; s.k = K::Commit; exec(s); BAIL(); }
        for (int round = 0; round < 400 && (fLeo_ < leo_ || hw_ < leo_ || txnOpen_ || !pending_.empty()); ++round) {
            Step s;
            if (txnOpen_ && round % 2 == 0) { s.k = K::Marker; s.flag = true; }
            else { s.k = K::Fetch; s.raw = 16384; }
            rig_->now += 100'000'000;   // past any fetch conflation window
            exec(s); BAIL();
        }
        REQ(fLeo_ == leo_ && hw_ == leo_ && !txnOpen_, "quiesce.stall",
            "never converged: follower " << fLeo_ << ", HW " << hw_ << ", LEO " << leo_ << ", txn open " << txnOpen_);
        for (int i = 0; i < kPassCap && (!pending_.empty() || !heldReads_.empty()); ++i) { passDrain(); settleAnswers(); BAIL(); }
        REQ(pending_.empty(), "quiesce.unanswered", pending_.size() << " owed answers never arrived (a hang)");
        REQ(heldReads_.empty(), "quiesce.heldRead", heldReads_.size() << " held read_committed reads never answered");
        // A final full read of each isolation from 0: everything held, nothing stalls.
        Step r; r.k = K::Read; r.cur = Cur::Earliest;
        exec(r); BAIL();
        r.iso = 1; exec(r);
    }
};

/// Delta-minimise a failing trace: the smallest sub-trace (by greedy chunk removal)
/// that still fails the SAME check.  Bounded in replays and wall time.
std::vector<Step> minimise(std::vector<Step> t, const std::string& id) {
    const auto t0 = std::chrono::steady_clock::now();
    auto fails = [&](const std::vector<Step>& c) {
        Stress s;
        s.replay(c);
        return s.failed() && s.failure().id == id;
    };
    std::size_t n = 2;
    int replays = 0;
    while (t.size() >= 2 && replays < 4000 &&
           std::chrono::steady_clock::now() - t0 < std::chrono::seconds(120)) {
        const std::size_t chunk = (t.size() + n - 1) / n;
        bool removed = false;
        for (std::size_t i = 0; i < t.size(); i += chunk) {
            std::vector<Step> c(t.begin(), t.begin() + static_cast<std::ptrdiff_t>(i));
            c.insert(c.end(), t.begin() + static_cast<std::ptrdiff_t>(std::min(t.size(), i + chunk)), t.end());
            ++replays;
            if (fails(c)) { t = std::move(c); n = std::max<std::size_t>(n - 1, 2); removed = true; break; }
        }
        if (!removed) {
            if (n >= t.size()) break;
            n = std::min(t.size(), n * 2);
        }
    }
    return t;
}

std::string traceText(const std::vector<Step>& t) {
    std::ostringstream o;
    for (std::size_t i = 0; i < t.size(); ++i) o << "  #" << i << ' ' << str(t[i]) << '\n';
    return o.str();
}

void runSeed(uint32_t seed) {
    Stress s;
    s.run(seed);
    Dist d = s.dist();
    d.seeds = 1;
    g_total.add(d);
    std::printf("[stress] seed %u: %s\n", seed, d.line().c_str());
    if (!s.failed()) return;
    const Failure f = s.failure();
    std::vector<Step> full = s.trace();
    if (f.step >= 0 && static_cast<std::size_t>(f.step) + 1 < full.size()) full.resize(static_cast<std::size_t>(f.step) + 1);
    if (std::getenv("RUNE_STRESS_NO_MINIMISE") != nullptr) {   // survey runs: the seed + step suffice
        ADD_FAILURE() << "seed " << seed << " failed at step " << f.step << " [" << f.id << "]: " << f.detail;
        return;
    }
    const auto min = minimise(full, f.id);
    Stress again;
    again.replay(min);
    ADD_FAILURE() << "seed " << seed << " failed at step " << f.step << " [" << f.id << "]: " << f.detail
                  << "\nminimal op trace (" << min.size() << " steps; replays deterministically):\n" << traceText(min)
                  << "  -> " << (again.failed() ? "at step " + std::to_string(again.failure().step) + ": " +
                                                      again.failure().detail
                                                : std::string("(did not reproduce)"));
}

class StressTotals : public ::testing::Environment {
public:
    // The worker's rate-limited "a reader fell behind its ring" warning is EXPECTED here
    // (thousands of evicted cursors); keep the output to the distribution + failures.
    void SetUp() override { spdlog::set_level(spdlog::level::err); }
    void TearDown() override {
        if (g_total.seeds > 1) std::printf("[stress] distribution: %s\n", g_total.line().c_str());
    }
};
[[maybe_unused]] ::testing::Environment* const g_env = ::testing::AddGlobalTestEnvironment(new StressTotals);

class TransientDataPathStressRegression : public testing::TestWithParam<uint32_t> {};

TEST_P(TransientDataPathStressRegression, SeededModelChecked) {
    SCOPED_TRACE("seed " + std::to_string(GetParam()));
    runSeed(GetParam());
}

INSTANTIATE_TEST_SUITE_P(Seeds, TransientDataPathStressRegression, testing::Range(1u, 65u));

/// Smoke: 20 fresh random seeds per run (printed, so a failure reproduces with
/// `--gtest_filter='*SeededModelChecked/<n>'` after adding it to the Range or via replay).
TEST(TransientDataPathStressSmoke, TwentyRandomSeeds) {
    std::random_device rd;
    for (int i = 0; i < 20; ++i) {
        const uint32_t seed = rd();
        SCOPED_TRACE("random seed " + std::to_string(seed));
        runSeed(seed);
    }
}

// ---------------------------------------------------------------------------
// Bug found by seed 41 (fixed in InMemoryLog, its own commit): a produce the
// floor forced to refuse (42) had first EVICTED the committed batches below the
// HW — committed records lost for an append that never happened.  The minimal
// trace; its last produce is refused and the ring start must not move.
// ---------------------------------------------------------------------------
TEST(TransientDataPathStressRegression, RefusedProduceEvictsNothingTraceRegression) {
    using O = mx::Op;
    const std::vector<Step> t{
        produce(O::ProduceAcks0, 1, 1260), fetch(8226), produce(O::ProduceAcksAll, 2, 953),
        produce(O::ProduceAcksAll, 2, 441), produce(O::ProduceAcks1, 2, 776), op(K::ProduceTxn),
        read(0, Cur::Offset, 99757190), produce(O::ProduceAcks0, 3, 441), produce(O::ProduceAcks0, 3, 1389),
        produce(O::ProduceAcks1, 2, 1290), produce(O::ProduceAcks0, 3, 1410), op(K::ProduceTxn),
        op(K::FollowerRestart), produce(O::ProduceAcks0, 1, 453), produce(O::ProduceAcksAll, 1, 1118),
        produce(O::ProduceAcks0, 1, 1223), op(K::ProduceTxn), produce(O::ProduceAcks1, 3, 1150),
        produce(O::ProduceAcks0, 3, 347), fetch(4609), produce(O::ProduceAcks0, 3, 796),
        produce(O::ProduceAcks1, 2, 1041), fetch(11116), produce(O::ProduceAcks1, 1, 1068),
        produce(O::ProduceAcksAll, 1, 379), produce(O::ProduceAcks0, 2, 1327)};
    Stress s;
    s.replay(t);
    if (s.failed()) ADD_FAILURE() << s.failure().id << " at step " << s.failure().step << ": " << s.failure().detail;
    EXPECT_GE(s.dist().ringFullRefusals, 1u) << "the trace no longer reaches a refused produce";
}

// ---------------------------------------------------------------------------
// Finding B (controller ruling, option (c) refined; fixed in RcState's ring
// rebuild): an OPEN transaction whose FIRST batch the ring evicted while a later
// batch of it is still in the ring, then an ownership change.  The rebuilt
// read_committed state opened it at its first IN-RING batch, so read_committed
// delivered the committed records between the ring start and that batch.  It is
// now pinned at the ring start; the producer stays known; COMMIT resolves it.
// ---------------------------------------------------------------------------
TEST(TransientDataPathStressRegression, FindingB_OpenTransactionEvictedThenOwnershipMovedKeepsTheLso) {
    std::vector<Step> t{op(K::ProduceTxn)};
    for (int i = 0; i < 12; ++i) t.push_back(produce(mx::Op::ProduceAcks0, 1, 1500));
    t.push_back(op(K::ProduceTxn));         // the same (still open) transaction's 2nd batch
    for (int i = 0; i < 10; ++i) { t.push_back(fetch(16384)); t.push_back(produce(mx::Op::ProduceAcks0, 1, 1500)); }
    t.push_back(fetch(16384));
    t.push_back(read(1, Cur::Earliest));   // live state: pinned
    t.push_back(op(K::Disown));
    t.push_back(op(K::Reown));
    t.push_back(read(1, Cur::Earliest));   // rebuilt state: must stay pinned
    t.push_back(op(K::ProduceTxn));         // the producer is still known
    t.push_back(op(K::Marker, /*commit=*/true));
    t.push_back(fetch(16384));
    t.push_back(read(1, Cur::Earliest));   // resolved: delivered
    Stress s;
    s.replay(t);
    if (s.failed()) ADD_FAILURE() << s.failure().id << " at step " << s.failure().step << ": " << s.failure().detail;
    EXPECT_EQ(s.dist().producerExpired, 0u) << "the open transaction's producer was forgotten";
    EXPECT_GT(s.dist().evictions, 0u);
}

// The ruling's documented edge: an open transaction ALL of whose batches the ring
// evicted, then an ownership change — unrecoverable on transient (the data is
// gone): the rebuilt state forgets it, read_committed follows the evicted-cursor
// rule, the producer's next transactional produce is UnknownProducerId and the
// client aborts and re-inits.  Pinned so the model's allowance stays exercised.
TEST(TransientDataPathStressRegression, OpenTransactionAllEvictedThenOwnershipMovedIsForgottenRegression) {
    std::vector<Step> t{op(K::ProduceTxn)};
    for (int i = 0; i < 15; ++i) t.push_back(produce(mx::Op::ProduceAcks0, 1, 1500));
    for (int i = 0; i < 6; ++i) { t.push_back(fetch(16384)); t.push_back(produce(mx::Op::ProduceAcks0, 1, 1500)); }
    t.push_back(fetch(16384));
    t.push_back(op(K::Disown));
    t.push_back(op(K::Reown));
    t.push_back(read(1, Cur::Earliest));
    t.push_back(op(K::ProduceTxn));         // UnknownProducerId -> the client aborts
    t.push_back(op(K::Marker, false));
    t.push_back(op(K::ProduceTxn));         // re-initialised producer
    Stress s;
    s.replay(t);
    if (s.failed()) ADD_FAILURE() << s.failure().id << " at step " << s.failure().step << ": " << s.failure().detail;
    EXPECT_EQ(s.dist().openTxnAllEvicted, 1u);
    EXPECT_EQ(s.dist().producerExpired, 1u);
}

} // namespace
