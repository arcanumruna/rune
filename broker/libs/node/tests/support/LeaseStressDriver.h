#pragma once
// LeaseStressDriver.h - lease topics Task 18 (spec 16.4): the seeded model-checked lease stress, one driver over two environments.
//
//   Leg S (SingleEnv, LeaseBrokerRig): ONE real broker, the INJECTED lease clock.  The clock moves only when the driver advances it (to the
//     model's next deadline, or by a random 1..50 ms), so every deadline is exact (rho = 0).  The wheel's tick (kLeaseWheelTickNs, 1 ms) is
//     the one duty tick of slack the liveness bound gets (I7 / I8).
//   Leg T (TrioEnv, LeaseTrio): THREE real brokers on the REAL lease clock, leader kills and restarts (at most 3 per seed).  I3 uses rho = 1 %
//     plus 2 ms; I7 uses ttl*1.01 + 500 ms after a quiet cluster and, after a promotion, T_ready + extend + R + 500 ms.
//
// The driver talks to the broker only through the real client: lease ops through a LeaseTopic handle, the view through getAndWait, and the log
// through raw subscriptions from offset 0 (LogTap).  Every step draws from one std::mt19937_64(seed); the weights are the tables below.
//
// Transient mode (lease topics Task 32, spec Appendix A): an Env with kTransient = true runs a TRANSIENT lease topic on a small ring.  Two more
// steps (Churn: filler updates that push the ring past its carry threshold; FullRestart: every node restarts, the rings are reborn, every
// lease is lost) and the invariants T1..T5 after every step: a live lease is never lost (T1) and a view recomputed from the ring has it
// (T2), a client append is never refused while carry could make room (T1/T3), no grant succeeds while the rebirth fence holds (T4), holders
// get LeaseNotFound and a LeaseWatch reports every lost key ended exactly once (T5).  The Env then also provides
//   E::kTransient, E::kRingBytes, void fullRestart() (blocks until the clients serve again), bool fenceOpen(), int64_t fenceDeadlineNs()
//   (0: unknown), uint64_t carriedRecords(), rc::Client& watchClient()
//
// Where the Environment must provide (E = Env):
//   E::kInjected, E::kLegName, Lt& ops(), Lt& reads(), LogTap& tap(), int64_t nowNs(), uint32_t ttlChoice(rng), uint64_t staleBroker(),
//   bool alterCheckpoint(bool on, uint32_t intervalMs), E::kI3SlackNs, E::kI7SlackNs, E::kTopicMaxTtlMs
//   S only: void advance(int64_t ns), bool suspend(bool rejectAll), bool resume(), bool recoverOps()
//   T only: int32_t killLeader(int partition) (-1: skipped), void restartNode(int id), int64_t promotionWindowNs()
//
// What it observes of the log: a committed record is stamped `rxNs` = a lease-clock reading taken at or after its append (the Expire / Checkpoint
// records carry timestamp 0, so the record's own timestamp is not usable): on leg T the tap handler reads the real lease clock the moment the
// record arrives; on leg S the driver stamps the records of a drain with the injected clock (which only it moves).  An Expire seen at rxNs was
// appended at or before rxNs, so "rxNs < the acknowledged deadline" proves it early.
#include "support/LeaseModel.h"

#include "gen/lease_claim.gen.h"

#include <rune/client/Client.h>
#include <rune/client/lease/LeaseTopic.h>
#include <rune/client/lease/LeaseWatch.h>
#include <rune/consensus/LeaseClock.h>
#include <rune/node/lease/LeaseWheel.h>
#include <rune/protocol/LeaseRecord.h>
#include <rune/protocol/client/Protocol.h>

#include <algorithm>
#include <array>
#include <chrono>
#include <cstring>
#include <deque>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <random>
#include <set>
#include <string>
#include <thread>
#include <vector>

namespace rune::node::test::leasestress {

namespace rc = rune::client;
namespace lm = rune::node::test::leasemodel;
namespace pc = rune::protocol::client;
using rune::gen::LeaseClaim;
using Lt = rc::LeaseTopic<LeaseClaim>;
using namespace std::chrono_literals;

inline constexpr int64_t kTickNs = rune::node::lease::kLeaseWheelTickNs;   ///< the duty tick of slack (spec 16.4: I7)
inline constexpr int kKeys       = 32;                                     ///< the key space of a seed
inline constexpr int kProbeKeys  = 8;                                      ///< k32..k39: barrier keys, one per partition candidate
inline constexpr int kModelKeys  = kKeys + kProbeKeys;
inline constexpr int kTraceLines = 30;
inline constexpr uint16_t kNotFound  = static_cast<uint16_t>(pc::ErrorCode::LeaseNotFound);
inline constexpr uint16_t kKeyExists = static_cast<uint16_t>(pc::ErrorCode::KeyExists);

// ------------------------------------------------------------------------------------------------ steps and weights

enum class Step : uint8_t {
    Grant, KeepAlive, KeepAliveGap, Update, Revoke, AlterCheckpoint, Suspend, Resume, Advance, Sleep, KillLeader, RestartNode, Churn, FullRestart, kCount
};
inline constexpr std::size_t kSteps = static_cast<std::size_t>(Step::kCount);
inline constexpr const char* kStepNames[kSteps] = {"Grant", "KeepAlive", "KeepAliveGap", "Update", "Revoke", "AlterCheckpoint",
                                                   "Suspend", "Resume", "Advance", "Sleep", "KillLeader", "RestartNode", "Churn", "FullRestart"};

//                                          Grant KeepAlive Gap Update Revoke Alter Suspend Resume Advance Sleep Kill Restart
inline constexpr std::array<uint32_t, kSteps> kWeightsS = {24, 22, 6, 12, 6, 2, 1, 0, 14, 0, 0, 0};   ///< Resume is forced while suspended
inline constexpr std::array<uint32_t, kSteps> kWeightsT = {26, 24, 8, 12, 6, 2, 0, 0, 0, 7, 3, 0};    ///< Restart is forced after a kill
//                                                 Grant KeepAlive Gap Update Revoke Alter Suspend Resume Advance Sleep Kill Restart Churn FullRestart
inline constexpr std::array<uint32_t, kSteps> kWeightsTS = {24, 22, 6, 12, 6, 1, 0, 0, 14, 0, 0, 0, 4, 2};   ///< transient, one node, injected clock
inline constexpr std::array<uint32_t, kSteps> kWeightsTT = {26, 24, 8, 12, 6, 1, 0, 0, 0, 7, 3, 0, 5, 2};    ///< transient, three nodes, real clock
inline constexpr uint16_t kLogFull = static_cast<uint16_t>(pc::ErrorCode::TransientLogFull);
inline constexpr uint16_t kThrottled = static_cast<uint16_t>(pc::ErrorCode::Throttled);
inline constexpr int kChurnAppends = 900;   ///< filler updates of one Churn: enough to cross the carry threshold of the 256 KiB ring within a step or two

// ------------------------------------------------------------------------------------------------ the log tap

/// Raw subscriptions from offset 0 to every partition of the lease topic.  The handlers run on the client's duty thread and append to one
/// queue (test code: the mutex is the hand-off, the driver drains it on its own thread).
///
/// A tap on a TRANSIENT topic (`ring` = true) has a ring behind it: a subscription the broker ended because its cursor fell below the ring
/// start (DataReset) or a data-reset count that grew means records were lost to the tap.  Per partition the tap then starts a new epoch:
/// it drops everything the old subscription queued, forgets its position, resubscribes from Earliest (the ring's head) and tells the driver
/// (the `gaps` of drain) so the model recomputes its fold from the ring.  rebirth() does the same for every partition after a full restart.
class LogTap {
public:
    LogTap(rc::Client& c, int32_t topicId, int32_t partitions, bool stampRealClock, bool ring = false)
        : stampReal_(stampRealClock), ring_(ring), client_(&c), topicId_(topicId), partitions_(partitions) {
        next_.assign(static_cast<std::size_t>(partitions), 0);
        epoch_.assign(static_cast<std::size_t>(partitions), 0);
        seenResets_.assign(static_cast<std::size_t>(partitions), 0);
        subs_.resize(static_cast<std::size_t>(partitions));
        for (int32_t p = 0; p < partitions; ++p) subscribe_(p);
    }
    ~LogTap() {
        for (auto& s : subs_)
            if (s) s->unsubscribe();
    }
    [[nodiscard]] std::string describe() const {
        std::string d;
        for (std::size_t i = 0; i < subs_.size(); ++i)
            d += " p" + std::to_string(i) + "{active " + std::to_string(subs_[i]->active()) + " finished " + std::to_string(subs_[i]->finished()) + " fail " +
                 std::to_string(static_cast<int>(subs_[i]->failReason())) + " pos " + std::to_string(subs_[i]->position()) + "}";
        return d;
    }
    LogTap(const LogTap&) = delete;
    LogTap& operator=(const LogTap&) = delete;

    /// Moves everything received since the last drain to `out` (cleared first); an unstamped record gets `stampNs`.  A ring tap appends to
    /// `gaps` the partitions whose fold the driver must recompute (their old queue is gone, the ring replays from its head).
    void drain(std::vector<lm::LogRec>& out, int64_t stampNs, std::vector<int32_t>* gaps = nullptr) {
        if (gaps != nullptr) gaps->clear();
        if (ring_) resync_(gaps);
        out.clear();
        {
            std::lock_guard<std::mutex> g(mu_);
            out.swap(q_);
        }
        for (lm::LogRec& r : out)
            if (r.rxNs == 0) r.rxNs = stampNs;
    }
    /// A full restart: every partition's data is a new lineage.  The caller resets the model alongside.
    void rebirth() {
        for (int32_t p = 0; p < partitions_; ++p) restart_(p);
        std::lock_guard<std::mutex> g(mu_);
        q_.clear();
    }
    [[nodiscard]] uint32_t resets() const noexcept { return resets_; }

private:
    void subscribe_(int32_t p) {
        const rc::TopicHandle h{topicId_, partitions_, rc::TopicKind::Typed};
        rc::SubscribeOptions o;
        int64_t              at;
        uint32_t             epoch;
        {
            std::lock_guard<std::mutex> g(mu_);
            at    = next_[static_cast<std::size_t>(p)];
            epoch = epoch_[static_cast<std::size_t>(p)];
        }
        o.from = at == 0 ? rc::From::earliest() : rc::From::offset(at);
        subs_[static_cast<std::size_t>(p)].emplace(client_->subscribe(h, p, o, [this, epoch](const rc::Record& r) { onRecord_(r, epoch); }));
        seenResets_[static_cast<std::size_t>(p)] = 0;
    }
    /// A new epoch for `p`: drop what the old subscription queued, forget the position, read from the ring's head.
    void restart_(int32_t p) {
        const std::size_t i = static_cast<std::size_t>(p);
        if (subs_[i]) subs_[i]->unsubscribe();
        {
            std::lock_guard<std::mutex> g(mu_);
            ++epoch_[i];
            next_[i] = 0;
            q_.erase(std::remove_if(q_.begin(), q_.end(), [p](const lm::LogRec& r) { return r.partition == p; }), q_.end());
        }
        subscribe_(p);
    }
    void resync_(std::vector<int32_t>* gaps) {
        for (int32_t p = 0; p < partitions_; ++p) {
            const std::size_t i = static_cast<std::size_t>(p);
            const rc::Subscription& s = *subs_[i];
            const bool dataReset = s.finished() && s.failReason() == rc::SubscriptionFailReason::DataReset;
            const bool grew      = s.dataResets() > seenResets_[i];
            if (dataReset || grew) {
                ++resets_;
                restart_(p);
                if (gaps != nullptr) gaps->push_back(p);
            } else if (s.finished() && s.failReason() != rc::SubscriptionFailReason::None) {
                // the session was lost (a leader change moved it): continue from the position, the same lineage
                subs_[i]->unsubscribe();
                subscribe_(p);
            }
        }
    }
    void onRecord_(const rc::Record& r, uint32_t epoch) {
        {
            std::lock_guard<std::mutex> g(mu_);
            if (epoch != epoch_[static_cast<std::size_t>(r.partition)]) return;   // an old subscription's late record
            int64_t& nx = next_[static_cast<std::size_t>(r.partition)];
            if (r.offset < nx) return;   // a re-subscription's overlap
            nx = r.offset + 1;
        }
        lm::LogRec x;
        x.partition = r.partition;
        x.offset    = r.offset;
        const auto op = rune::protocol::record::leaseOpOf(r.attributes);
        x.op = static_cast<lm::RecOp>(op);
        x.carry = rune::protocol::record::isLeaseCarry(r.attributes, rune::protocol::record::TopicTraits{true});
        if (r.value.size() >= 12) std::memcpy(&x.version, r.value.data() + rune::protocol::record::kLeaseVersionValueOffset, 8);
        if (op == rune::protocol::record::LeaseOp::Put && r.value.size() >= 76) std::memcpy(&x.a, r.value.data() + 68, 8);
        if (op == rune::protocol::record::LeaseOp::Checkpoint && r.value.size() >= 36)
            std::memcpy(&x.remainingMs, r.value.data() + rune::protocol::record::kLeaseRemainingValueOffset, 8);
        x.key.assign(reinterpret_cast<const char*>(r.key.data()), strnlen(reinterpret_cast<const char*>(r.key.data()), r.key.size()));
        x.rxNs = stampReal_ ? rune::consensus::leaseClockNowNs() : 0;
        std::lock_guard<std::mutex> g(mu_);
        q_.push_back(std::move(x));
    }
    bool                          stampReal_;
    bool                          ring_;
    rc::Client*                   client_;
    int32_t                       topicId_, partitions_;
    std::vector<int64_t>          next_;
    std::vector<uint32_t>         epoch_, seenResets_;
    uint32_t                      resets_ = 0;
    std::mutex                    mu_;
    std::vector<lm::LogRec>       q_;
    std::vector<std::optional<rc::Subscription>> subs_;
};

/// A LeaseWatch's event log: the live set (key -> version), each key's end count, and protocol violations (an end of an unheld key).
struct WLog {
    std::map<std::string, int64_t> live;
    std::map<std::string, int>     ended, granted;
    int                            bad = 0, readies = 0, failed = 0;
    static std::string keyOf(const rc::LeaseEvent& e) {
        const std::string raw(reinterpret_cast<const char*>(e.key.data()), e.key.size());
        return std::string(raw.c_str(), strnlen(raw.c_str(), raw.size()));
    }
    static void fn(void* c, const rc::LeaseEvent& e) {
        auto& w = *static_cast<WLog*>(c);
        const std::string k = keyOf(e);
        switch (e.kind) {
        case rc::LeaseEventKind::Granted: w.live[k] = e.version; ++w.granted[k]; break;
        case rc::LeaseEventKind::Updated: break;
        case rc::LeaseEventKind::Revoked:
        case rc::LeaseEventKind::Expired:
        case rc::LeaseEventKind::Vanished:
            if (w.live.erase(k) == 0) ++w.bad;
            ++w.ended[k];
            break;
        case rc::LeaseEventKind::Ready: ++w.readies; break;
        case rc::LeaseEventKind::Failed: ++w.failed; break;
        }
    }
};

// ------------------------------------------------------------------------------------------------ the driver

struct ScopedSecs {
    double&                               acc;
    std::chrono::steady_clock::time_point t0 = std::chrono::steady_clock::now();
    explicit ScopedSecs(double& a) : acc(a) {}
    ~ScopedSecs() { acc += std::chrono::duration<double>(std::chrono::steady_clock::now() - t0).count(); }
};

template <class Env>
class Driver {
public:
    struct Stats {
        std::array<uint32_t, kSteps> steps{};
        uint32_t grantsAcked = 0, keepalivesAcked = 0, updatesAcked = 0, revokesAcked = 0, expiries = 0, fencedRefused = 0;
        uint32_t viewReads = 0, viewPolls = 0, remainingRows = 0;
        double secOps = 0, secView = 0, secDue = 0, secFull = 0, secRecover = 0, secAlter = 0, secBarrier = 0;
        uint32_t doubts = 0, skipped = 0, fullChecks = 0, i7Deferred = 0, kills = 0, restarts = 0, suspendedAdvances = 0, maxHops = 0;
        // transient mode
        uint32_t churnAppends = 0, churnSkipped = 0, tapResets = 0, fullRestarts = 0, fenceThrottled = 0, holderProbes = 0, logFullExcused = 0,
                 recomputes = 0, recomputeDeferred = 0, fencedShort = 0, throttledOpen = 0, watchChecks = 0;
        uint64_t carried = 0, carriedAtRestart = 0, carriesSeen = 0, staleCarries = 0, fillerRecs = 0;
        double secChurn = 0, secRestart = 0;
    };

    Driver(Env& env, uint64_t seed, int steps) : env_(env), seed_(seed), steps_(steps), rng_(seed), model_(kModelKeys) {
        expectA_.fill(0);
    }

    /// Runs every step; true when every invariant held after every step.  failure() names seed, step, invariant and the last 30 steps.
    bool run() {
        try {
            if (!findProbeKeys_()) return false;
            if constexpr (Env::kTransient) {
                if (!setupTransient_()) return false;
            }
            for (step_ = 1; step_ <= steps_; ++step_) {
                const auto t0 = std::chrono::steady_clock::now();
                if (!oneStep_()) return false;
                const auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - t0).count();
                if (ms >= 700) slow_.push_back("step " + std::to_string(step_) + " took " + std::to_string(ms) + " ms");
            }
            --step_;
            const auto f0 = std::chrono::steady_clock::now();
            const bool fin = finish_();
            slow_.push_back("finish took " + std::to_string(std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - f0).count()) + " ms");
            return fin;
        } catch (const std::exception& e) {
            return fail_("exception", e.what());
        }
    }
    [[nodiscard]] const std::string& failure() const noexcept { return failure_; }
    [[nodiscard]] const std::vector<std::string>& slowSteps() const noexcept { return slow_; }
    [[nodiscard]] const Stats& stats() const noexcept { return stats_; }
    [[nodiscard]] std::string summary() const {
        std::string s = Env::kLegName;
        s += " seed " + std::to_string(seed_) + ":";
        for (std::size_t i = 0; i < kSteps; ++i)
            if (stats_.steps[i]) s += std::string(" ") + kStepNames[i] + "=" + std::to_string(stats_.steps[i]);
        s += " | acked grants " + std::to_string(stats_.grantsAcked) + " ka " + std::to_string(stats_.keepalivesAcked) + " upd " + std::to_string(stats_.updatesAcked) +
             " revokes " + std::to_string(stats_.revokesAcked) + " | expiries " + std::to_string(stats_.expiries) +
             " fenced-refused " + std::to_string(stats_.fencedRefused) + " doubts " + std::to_string(stats_.doubts) + " skipped " + std::to_string(stats_.skipped) +
             " fullChecks " + std::to_string(stats_.fullChecks) + " i7Deferred " + std::to_string(stats_.i7Deferred) + " kills " + std::to_string(stats_.kills) +
             " suspendedAdvances " + std::to_string(stats_.suspendedAdvances) + " maxHops " + std::to_string(stats_.maxHops) +
             " | view reads " + std::to_string(stats_.viewReads) + " rows with a checkpointed remaining " + std::to_string(stats_.remainingRows) + " | secs ops " + std::to_string(stats_.secOps) + " view " + std::to_string(stats_.secView) + " due " + std::to_string(stats_.secDue) + " full " + std::to_string(stats_.secFull) +
             " recover " + std::to_string(stats_.secRecover) + " alter " + std::to_string(stats_.secAlter);
        if constexpr (Env::kTransient) {
            s += " | transient: churn appends " + std::to_string(stats_.churnAppends) + " (skipped " + std::to_string(stats_.churnSkipped) + ") carried " + std::to_string(stats_.carried + stats_.carriedAtRestart) +
                 " (folded " + std::to_string(stats_.carriesSeen) + ", stale " + std::to_string(stats_.staleCarries) + ") fillerRecs " + std::to_string(stats_.fillerRecs) +
                 " tapResets " + std::to_string(stats_.tapResets) + " fullRestarts " + std::to_string(stats_.fullRestarts) + " fenceThrottled " + std::to_string(stats_.fenceThrottled) +
                 " fencedShort " + std::to_string(stats_.fencedShort) + " holderProbes " + std::to_string(stats_.holderProbes) + " recomputes " + std::to_string(stats_.recomputes) +
                 " (deferred " + std::to_string(stats_.recomputeDeferred) + ") watchChecks " + std::to_string(stats_.watchChecks) + " logFullExcused " + std::to_string(stats_.logFullExcused) +
                 " | secs churn " + std::to_string(stats_.secChurn) + " restart " + std::to_string(stats_.secRestart);
        }
        return s;
    }

private:
    enum class Kind { Ok, NotFound, KeyExists, Other };
    static Kind classify_(const rc::LeaseResult& r) {
        if (r.ok()) return Kind::Ok;
        if (r.errorCode == kNotFound) return Kind::NotFound;
        if (r.errorCode == kKeyExists) return Kind::KeyExists;
        return Kind::Other;
    }
    static std::string text_(const rc::LeaseResult& r) { return "status " + std::to_string(int(r.status)) + " code " + std::to_string(r.errorCode); }

    [[nodiscard]] Lt& ops_() { return env_.ops(); }
    [[nodiscard]] static bool isFiller_(const std::string& key) noexcept { return key.size() >= 2 && key[0] == 'z' && key[1] == 'f'; }
    [[nodiscard]] LeaseClaim rowNamed_(const std::string& n, uint64_t a) const {
        LeaseClaim r;
        if (!n.empty()) std::memcpy(r.claim.data(), n.data(), std::min(n.size(), r.claim.size()));
        r.a = a;
        return r;
    }
    /// The stale version a call on an ended key presents: a version it once held (this run, or before a full restart), else 7.
    [[nodiscard]] static int64_t staleVersion_(const lm::KeyModel& m) {
        if (!m.granted.empty()) return m.granted.back();
        if (!m.lostVersions.empty()) return m.lostVersions.back();
        return 7;
    }
    [[nodiscard]] uint32_t pick_(uint32_t n) { return static_cast<uint32_t>(rng_() % n); }
    [[nodiscard]] LeaseClaim row_(int k, uint64_t a) const {
        LeaseClaim r;
        const std::string& n = model_.key(k).name;
        if (!n.empty()) std::memcpy(r.claim.data(), n.data(), std::min(n.size(), r.claim.size()));
        r.a = a;
        return r;
    }
    void trace_(const std::string& s) {
        trace_buf_.push_back("step " + std::to_string(step_) + ": " + s);
        if (std::getenv("RUNE_LEASE_STRESS_DEBUG") != nullptr) std::fprintf(stderr, "%s\n", trace_buf_.back().c_str());   // a live trace for diagnosis
        if (trace_buf_.size() > kTraceLines) trace_buf_.pop_front();
    }
    bool fail_(const std::string& invariant, const std::string& detail) {
        if (!failure_.empty()) return false;
        failure_ = std::string(Env::kLegName) + " FAILED seed " + std::to_string(seed_) + " step " + std::to_string(step_) + " [" + invariant + "] " + detail +
                   "\n--- the last " + std::to_string(trace_buf_.size()) + " steps ---\n";
        for (const std::string& l : trace_buf_) failure_ += l + "\n";
        return false;
    }
    bool failModel_(const std::string& msg) {
        const std::size_t c = msg.find(':');
        return fail_(c == std::string::npos ? "model" : msg.substr(0, c), msg);
    }

    // ---------------------------------------------------------------- the log

    /// Pulls the new committed records and folds them into the model; false on a violated invariant.
    bool drain_() {
        env_.tap().drain(recs_, env_.nowNs(), &gaps_);
        if constexpr (Env::kTransient) {
            for (const int32_t p : gaps_) {   // the tap fell behind the ring: the fold of that partition is recomputed from the ring's head
                ++stats_.tapResets;
                for (int k = 0; k < kModelKeys; ++k)
                    if (keyPart_[static_cast<std::size_t>(k)] == p) model_.resetFold(k);
                trace_("tap reset on partition " + std::to_string(p) + " (the ring replays from its head)");
            }
        }
        for (const lm::LogRec& r : recs_) {
            if constexpr (Env::kTransient) {
                if (isFiller_(r.key)) { ++stats_.fillerRecs; continue; }
            }
            if (r.op == lm::RecOp::Expire) ++stats_.expiries;
            if (r.op == lm::RecOp::Expire || r.op == lm::RecOp::Revoke) {
                const int ek = model_.indexOf(r.key);
                if (ek >= 0 && ek < kKeys && std::find(ended_.begin(), ended_.end(), ek) == ended_.end()) ended_.push_back(ek);
            }
            const std::string e = model_.onRecord(r, Env::kI3SlackNs);
            if (!e.empty()) return failModel_(e + " [record offset " + std::to_string(r.offset) + " partition " + std::to_string(r.partition) + "]");
        }
        if constexpr (Env::kTransient) {
            stats_.carriesSeen = model_.carries;
            stats_.staleCarries = model_.staleCarries;
            pumpWatch_();
        }
        return true;
    }
    /// Polls (capped) until the model's live lease of `k` has its end in the log.
    bool awaitEnd_(int k) {
        for (int i = 0; i < kPollCap; ++i) {
            if (!drain_()) return false;
            if (model_.key(k).live == 0) return true;
            std::this_thread::sleep_for(1ms);
        }
        return fail_("I1", "grant acknowledged for " + model_.key(k).name + " but the previous version " + std::to_string(model_.key(k).live) + " has no end record in the log");
    }

    // ---------------------------------------------------------------- probes (a barrier for the tap)

    bool findProbeKeys_() {
        probe_.assign(static_cast<std::size_t>(env_.partitions()), -1);
        for (int k = kKeys; k < kModelKeys; ++k) {
            const int p = ops_().partitionOf(row_(k, 0));
            if (p >= 0 && p < static_cast<int>(probe_.size()) && probe_[static_cast<std::size_t>(p)] < 0) probe_[static_cast<std::size_t>(p)] = k;
        }
        for (int p : probe_)
            if (p < 0) return fail_("setup", "no probe key for every partition");
        return true;
    }
    /// Grants and revokes one probe key per partition and waits until the tap shows each grant: the tap then holds every record committed before it
    /// on that partition (a partition's stream is ordered).  False when a probe cannot be written (the caller decides what that means).
    bool barrier_(bool* written = nullptr) {
        if (written) *written = false;
        for (int p : probe_) {
            lm::KeyModel& m = model_.key(p);
            if (m.live != 0) {   // a probe left over from a refused revoke
                const rc::LeaseResult rv = ops_().revokeAndWait(row_(p, 0), m.live);
                if (rv.ok()) (void)model_.revokeAcked(p, m.live);
                else return true;
            }
            rc::LeaseOptions o;
            o.ttlMs = 1000;
            const int64_t before = env_.nowNs();
            const rc::LeaseResult g = ops_().grantAndWait(row_(p, 0), o);
            const int64_t after = env_.nowNs();
            if (!g.ok()) return true;   // not written: the caller's check is deferred
            if (m.live != 0 && !awaitEnd_(p)) return false;
            const std::string e = model_.grantAcked(p, g.version, g.ttlMs, before, after);
            if (!e.empty()) return failModel_(e);
            // The tap never re-subscribes: a subscription that goes silent after a leader change is a failure here (the
            // gateway moves a subscription to records needing verdicts off a demoted leader, servableHere_).
            bool seen = false;
            for (int i = 0; i < kPollCap && !seen; ++i) {
                if (!drain_()) return false;
                seen = m.seenGrants.count(g.version) != 0;
                if (!seen) std::this_thread::sleep_for(1ms);
            }
            if (!seen) return fail_("tap", "the probe grant " + std::to_string(g.version) + " never reached the log tap; subs" + env_.tap().describe());
            const rc::LeaseResult rv = ops_().revokeAndWait(row_(p, 0), g.version);
            if (rv.ok()) {
                const std::string e2 = model_.revokeAcked(p, g.version);
                if (!e2.empty()) return failModel_(e2);
            } else {
                m.kept = false;   // it expires on its own
            }
            if (written) *written = true;
        }
        return true;
    }

    // ---------------------------------------------------------------- the lease view

    struct ViewRead { bool err = false; std::optional<LeaseClaim::LeasesRow> row; };
    ViewRead readView_(int k) {
        ViewRead v;
        ++stats_.viewReads;
        try {
            v.row = env_.reads().getAndWait(row_(k, 0));
        } catch (const std::exception&) {
            v.err = true;
        }
        return v;
    }
    /// I1 for one key: the lease view agrees with what the driver was told (polls, capped: the view trails an Expire or a Revoke, and on a
    /// real clock the lease may end under us - the log decides, so the model is re-read after every drain).
    bool checkKeyView_(int k, const char* inv = "I1") {
        ScopedSecs t(stats_.secView);
        std::string last;
        for (int i = 0; i < kPollCap; ++i) {
            if (!drain_()) return false;
            const lm::KeyModel& m = model_.key(k);
            if (m.doubt) return true;
            const ViewRead v = readView_(k);
            if (!v.err) {
                if (m.live == 0 && !v.row.has_value()) return true;
                if (m.live != 0 && v.row.has_value() && v.row->lease_version == m.live) return true;
                last = std::string("the lease view of ") + m.name + " shows " + (v.row ? "version " + std::to_string(v.row->lease_version) : std::string("no row")) +
                       " but the model's live version is " + std::to_string(m.live);
            } else {
                last = "the lease view read of " + m.name + " failed";
            }
            std::this_thread::sleep_for(1ms);
        }
        return fail_(inv, last);
    }

    /// I5: after the tap holds what the model expects, the lease view rows equal the recompute from the log, and the counters agree.
    bool settle_(bool* deferred) {
        *deferred = false;
        if (!awaitGhosts_()) return false;
        if (suspended_) { *deferred = true; return true; }   // a barrier needs writes
        if (!Env::kInjected || anyDoubt_()) {
            bool written = false;
            if (!barrier_(&written)) return false;
            if (!written) { *deferred = true; return true; }
        }
        for (int k = 0; k < kKeys; ++k) {
            if (model_.key(k).doubt) {
                model_.resolve(k);
                expectA_[static_cast<std::size_t>(k)] = model_.fold(k).a;
            }
        }
        for (int i = 0; i < kPollCap; ++i) {
            if (!drain_()) return false;
            bool caughtUp = true;
            for (int k = 0; k < kKeys && caughtUp; ++k) {
                const lm::KeyModel& m = model_.key(k);
                if (m.foldLive != m.live || !m.pendingRevoke.empty()) caughtUp = false;
                else if (m.live != 0 && m.foldA != expectA_[static_cast<std::size_t>(k)] && Env::kInjected) caughtUp = false;
            }
            if (caughtUp) return true;
            std::this_thread::sleep_for(1ms);
        }
        for (int k = 0; k < kKeys; ++k) {
            const lm::KeyModel& m = model_.key(k);
            if (m.foldLive != m.live) return fail_("I5", "the log fold of " + m.name + " has live version " + std::to_string(m.foldLive) + " but the model has " + std::to_string(m.live));
            if (!m.pendingRevoke.empty()) return fail_("I4", "the acknowledged revoke of " + m.name + " version " + std::to_string(*m.pendingRevoke.begin()) + " has no Revoke record in the log");
            if (m.live != 0 && m.foldA != expectA_[static_cast<std::size_t>(k)] && Env::kInjected)
                return fail_("I5", "the log fold of " + m.name + " has a=" + std::to_string(m.foldA) + " but the last acknowledged write was a=" + std::to_string(expectA_[static_cast<std::size_t>(k)]));
        }
        return true;
    }
    [[nodiscard]] bool anyDoubt_() const {
        for (int k = 0; k < kKeys; ++k)
            if (model_.key(k).doubt) return true;
        return false;
    }

    bool fullCheck_(const char* why) {
        ScopedSecs t(stats_.secFull);
        bool deferred = false;
        if (!settle_(&deferred)) return false;
        if (deferred) { fullDue_ = true; trace_(std::string("fullCheck deferred (") + why + ")"); return true; }
        fullDue_ = false;
        ++stats_.fullChecks;
        // every acknowledged grant is in the log (I2); every ended version has exactly one end record (I4).  A transient ring's head has lost
        // history (an evicted grant or end): there the live leases are what the ring must show (T2, below).
        for (int k = 0; k < kKeys && !Env::kTransient; ++k) {
            const lm::KeyModel& m = model_.key(k);
            for (int64_t v : m.granted)
                if (!m.seenGrants.count(v)) return fail_("I2", "the acknowledged grant " + std::to_string(v) + " of " + m.name + " is not in the log");
            const std::size_t ended = m.seenGrants.size() - (m.foldLive != 0 ? 1u : 0u);
            if (m.ends.size() != ended && Env::kInjected)
                return fail_("I4", m.name + " has " + std::to_string(m.seenGrants.size()) + " grants and " + std::to_string(m.ends.size()) + " end records (live " + std::to_string(m.foldLive) + ")");
        }
        // the lease view rows equal a recompute from the log
        for (int k = 0; k < kKeys; ++k) {
            bool ok = false;
            std::string last;
            for (int i = 0; i < kPollCap && !ok; ++i) {
                const lm::Model::Fold f1 = model_.fold(k);
                const ViewRead v = readView_(k);
                if (!drain_()) return false;
                const lm::Model::Fold f2 = model_.fold(k);
                if (v.err) { last = "a view read failed"; }
                else {
                    auto agrees = [&](const lm::Model::Fold& f) {
                        if (f.live == 0) return !v.row.has_value();
                        return v.row.has_value() && v.row->lease_version == f.live && v.row->a == f.a;
                    };
                    ok = agrees(f1) || agrees(f2);
                    if (ok && v.row.has_value()) {
                        const std::string e = model_.checkRemaining(k, v.row->lease_version, v.row->lease_remaining_ms, env_.nowNs(), Env::kI3SlackNs);
                        if (!e.empty()) return failModel_(e);
                        if (v.row->lease_remaining_ms != 0) ++stats_.remainingRows;
                    }
                    if (!ok) last = std::string("view ") + (v.row ? "version " + std::to_string(v.row->lease_version) + " a=" + std::to_string(v.row->a) : std::string("none")) +
                                     " vs log fold version " + std::to_string(f2.live) + " a=" + std::to_string(f2.a);
                }
                if (!ok) std::this_thread::sleep_for(1ms);
            }
            if (!ok) return fail_("I5", "the lease view row of " + model_.key(k).name + " differs from the recompute from the log: " + last);
        }
        const uint64_t stale = env_.staleBroker();
        if (stale != 0) return fail_("I4", "stale_broker == " + std::to_string(stale) + ", want 0");
        if constexpr (Env::kTransient) {
            if (!ringRecompute_(why)) return false;
        }
        trace_(std::string("fullCheck ok (") + why + ")");
        return true;
    }

    // ---------------------------------------------------------------- liveness

    /// I7 / I8: every lease whose latest possible deadline plus the slack has passed has its end record.  Polls (1 ms each, at most `polls`)
    /// because the end needs a broker cycle and a commit; on the real clock the slack already contains them, so the poll is short.
    bool checkDue_(int polls) {
        ScopedSecs t(stats_.secDue);
        for (int i = 0;; ++i) {
            if (!drain_()) return false;
            const int64_t now = env_.nowNs();
            if constexpr (!Env::kInjected) {
                if (now < promoteUntilNs_) { ++stats_.i7Deferred; return true; }
            }
            const std::vector<int> d = model_.due(now, Env::kI7SlackNs);
            if (d.empty()) return true;
            if (i >= polls) {
                const lm::KeyModel& m = model_.key(d[0]);
                return fail_(suspendedMode_ == Suspended::None ? "I7" : "I8",
                             "the lease of " + m.name + " version " + std::to_string(m.live) + " is " + std::to_string((now - m.hi) / 1'000'000) +
                                 " ms past its latest possible deadline and has no end record (" + std::to_string(d.size()) + " due, namespace: " +
                                 (suspendedMode_ == Suspended::None ? "active" : suspendedMode_ == Suspended::ReadOnly ? "ReadOnly" : "RejectAll") + ")");
            }
            std::this_thread::sleep_for(1ms);
        }
    }

    // ---------------------------------------------------------------- one acknowledged call each

    bool opGrant_(int k) {
        lm::KeyModel& m = model_.key(k);
        viewDue_ = true;
        const uint32_t ttl = env_.ttlChoice(rng_);
        const uint64_t a = ++aSeq_;
        rc::LeaseOptions o;
        o.ttlMs = ttl;
        std::chrono::nanoseconds heldTimeout = 10s;
        if constexpr (Env::kTransient) {
            if (fenceUntilP_[static_cast<std::size_t>(keyPart_[static_cast<std::size_t>(k)])] != 0) heldTimeout = 1s;   // a held grant is retried by the client until this
        }
        const int64_t before = env_.nowNs();
        rc::LeaseResult r;
        try { ScopedSecs t(stats_.secOps); r = ops_().grantAndWait(row_(k, a), o, heldTimeout); } catch (const std::exception& e) { r = rc::LeaseResult{rc::OpStatus::Timeout, 0}; if (Env::kInjected) return fail_("grant", e.what()); }
        const int64_t after = env_.nowNs();
        trace_("grant " + m.name + " ttl " + std::to_string(ttl) + " a=" + std::to_string(a) + " -> " + (r.ok() ? "Ok v" + std::to_string(r.version) : text_(r)) + " (live " + std::to_string(m.live) + ")");
        if constexpr (Env::kTransient) {
            int64_t& fence = fenceUntilP_[static_cast<std::size_t>(keyPart_[static_cast<std::size_t>(k)])];
            if (!r.ok() && r.errorCode == kLogFull) return failLogFull_("grant", m.name);
            if (fence != 0) {   // the rebirth fence (A.6): a grant waits for it, every one is Throttled until the lost leases have run out
                if (r.ok()) {
                    if (after < fence)
                        return fail_("T4", "T4: grant during fence: " + m.name + " was granted at " + std::to_string(after) + " ns, " + std::to_string((fence - after) / lm::kNsPerMs) +
                                               " ms before the fence deadline (a lease lost in the restart may still be believed by its holder)");
                    fence = 0;
                } else if (r.errorCode == kThrottled || r.status == rc::OpStatus::Timeout) {   // held: the client retries Throttled until its op timeout
                    ++stats_.fenceThrottled;
                    noteHeld_(k, r, after, fence);
                    return true;
                } else if (r.errorCode == kKeyExists) {
                    return fail_("T4", "a grant of " + m.name + " answered KeyExists during the fence: a row survived the full restart");
                } else if (Env::kInjected) {
                    return fail_("T4", "a grant of " + m.name + " during the fence answered " + text_(r) + ", want Throttled");
                } else {
                    noteHeld_(k, r, after, fence);
                    return true;   // three nodes: no leader yet, a timeout: not a success
                }
            } else if (!r.ok() && r.errorCode == kThrottled && !Env::kInjected) {
                if (after > promoteUntilNs_)
                    return fail_("T1", "a grant of " + m.name + " answered Throttled " + std::to_string((after - promoteUntilNs_) / lm::kNsPerMs) +
                                           " ms after the promotion window: a leader change keeps the lineage and must not fence");
                ++stats_.throttledOpen;
                return true;
            }
        }
        const bool expectFree = m.live == 0 && !m.doubt;
        switch (classify_(r)) {
        case Kind::Ok:
            if (suspended_) return fail_(suspendedMode_ == Suspended::ReadOnly ? "I8" : "I8", "a grant was acknowledged while the namespace is suspended");
            if (m.live != 0 && !awaitEnd_(k)) return false;
            if (const std::string e = model_.grantAcked(k, r.version, r.ttlMs, before, after); !e.empty()) return failModel_(e);
            expectA_[static_cast<std::size_t>(k)] = a;
            ++stats_.grantsAcked;
            return true;
        case Kind::KeyExists:
            if (suspended_) return true;
            if (expectFree && Env::kInjected) return fail_("I1", "a grant of the free key " + m.name + " answered KeyExists");
            if (expectFree) { model_.inDoubt(k, after, Env::kTopicMaxTtlMs); ++stats_.doubts; }
            return true;
        default:
            if (suspended_) return true;
            if (Env::kInjected) return fail_("grant", "the grant of " + m.name + " was refused: " + text_(r));
            model_.inDoubt(k, after, Env::kTopicMaxTtlMs);
            ++stats_.doubts;
            return true;
        }
    }

    /// `right`: the live version; otherwise a version the lease does not hold (fenced).
    bool opKeepAlive_(int k, bool right) {
        lm::KeyModel& m = model_.key(k);
        int64_t version;
        if (right && m.live != 0) version = m.live;
        else if (m.live != 0) version = m.live + 1 + pick_(5);
        else if (!m.granted.empty()) version = m.granted[pick_(static_cast<uint32_t>(m.granted.size()))];
        else version = staleVersion_(m);
        const bool isRight = version == m.live && m.live != 0 && !m.doubt;
        if (!isRight) viewDue_ = true;
        const int64_t before = env_.nowNs();
        rc::LeaseResult r;
        try { ScopedSecs t(stats_.secOps); r = ops_().keepAliveOnceAndWait(row_(k, 0), version); } catch (const std::exception& e) { r = rc::LeaseResult{rc::OpStatus::Timeout, 0}; if (Env::kInjected) return fail_("keepalive", e.what()); }
        const int64_t after = env_.nowNs();
        trace_("keepalive " + m.name + " v" + std::to_string(version) + (isRight ? " (live)" : " (fenced)") + " -> " + (r.ok() ? "Ok ttl " + std::to_string(r.ttlMs) : text_(r)));
        switch (classify_(r)) {
        case Kind::Ok:
            if (suspendedMode_ == Suspended::RejectAll) return fail_("I8", "a keepalive was acknowledged under RejectAll");
            if (!isRight) {
                if (m.doubt && version == m.live) { model_.inDoubt(k, after, r.ttlMs); return true; }
                return fail_("I1", "a keepalive of " + m.name + " at the fenced version " + std::to_string(version) + " was acknowledged (live " + std::to_string(m.live) + ")");
            }
            if (const std::string e = model_.keepAliveAcked(k, version, r.ttlMs, before, after); !e.empty()) return failModel_(e);
            ++stats_.keepalivesAcked;
            return true;
        case Kind::NotFound:
            if (!isRight) { ++stats_.fencedRefused; return true; }
            if (Env::kInjected) return fail_("I1", "the keepalive of the live lease " + m.name + " v" + std::to_string(version) + " answered LeaseNotFound");
            if (after >= m.lo) return true;   // it may have expired: the log shows it
            return fail_("I1", "the keepalive of " + m.name + " v" + std::to_string(version) + " answered LeaseNotFound " + std::to_string((m.lo - after) / 1'000'000) + " ms before the acknowledged deadline");
        default:
            if (suspended_) return true;
            if (!isRight) return true;
            if (Env::kInjected) return fail_("keepalive", "the keepalive of " + m.name + " was refused: " + text_(r));
            model_.inDoubt(k, after, Env::kTopicMaxTtlMs);
            ++stats_.doubts;
            return true;
        }
    }

    bool opUpdate_(int k, bool right) {
        lm::KeyModel& m = model_.key(k);
        const int64_t version = (right && m.live != 0) ? m.live : (m.live != 0 ? m.live + 1 + static_cast<int64_t>(pick_(5)) : staleVersion_(m));
        const bool isRight = version == m.live && m.live != 0 && !m.doubt;
        viewDue_ = true;
        const uint64_t a = ++aSeq_;
        rc::LeaseResult r;
        try { ScopedSecs t(stats_.secOps); r = ops_().updateAndWait(row_(k, a), version); } catch (const std::exception& e) { r = rc::LeaseResult{rc::OpStatus::Timeout, 0}; if (Env::kInjected) return fail_("update", e.what()); }
        const int64_t after = env_.nowNs();
        trace_("update " + m.name + " v" + std::to_string(version) + (isRight ? " (live)" : " (fenced)") + " a=" + std::to_string(a) + " -> " + (r.ok() ? "Ok" : text_(r)));
        if constexpr (Env::kTransient) {
            if (!r.ok() && r.errorCode == kLogFull) return failLogFull_("update", m.name);
        }
        switch (classify_(r)) {
        case Kind::Ok:
            if (suspended_) return fail_("I8", "an update was acknowledged while the namespace is suspended");
            if (!isRight) {
                if (m.doubt && version == m.live) { expectA_[static_cast<std::size_t>(k)] = a; return true; }
                return fail_("I1", "an update of " + m.name + " at the fenced version " + std::to_string(version) + " was acknowledged (live " + std::to_string(m.live) + ")");
            }
            expectA_[static_cast<std::size_t>(k)] = a;
            ++stats_.updatesAcked;
            return true;
        case Kind::NotFound:
            if (!isRight) { ++stats_.fencedRefused; return true; }
            if (Env::kInjected) return fail_("I1", "the update of the live lease " + m.name + " v" + std::to_string(version) + " answered LeaseNotFound");
            if (after >= m.lo) return true;
            return fail_("I1", "the update of " + m.name + " v" + std::to_string(version) + " answered LeaseNotFound before the acknowledged deadline");
        default:
            if (suspended_ || !isRight) return true;
            if (Env::kInjected) return fail_("update", "the update of " + m.name + " was refused: " + text_(r));
            model_.inDoubt(k, after, Env::kTopicMaxTtlMs);
            ++stats_.doubts;
            return true;
        }
    }

    bool opRevoke_(int k, bool right) {
        lm::KeyModel& m = model_.key(k);
        const int64_t version = (right && m.live != 0) ? m.live : (m.live != 0 ? m.live + 1 + static_cast<int64_t>(pick_(5)) : staleVersion_(m));
        const bool isRight = version == m.live && m.live != 0 && !m.doubt;
        viewDue_ = true;
        rc::LeaseResult r;
        try { ScopedSecs t(stats_.secOps); r = ops_().revokeAndWait(row_(k, 0), version); } catch (const std::exception& e) { r = rc::LeaseResult{rc::OpStatus::Timeout, 0}; if (Env::kInjected) return fail_("revoke", e.what()); }
        const int64_t after = env_.nowNs();
        trace_("revoke " + m.name + " v" + std::to_string(version) + (isRight ? " (live)" : " (fenced)") + " -> " + (r.ok() ? "Ok" : text_(r)));
        if constexpr (Env::kTransient) {
            if (!r.ok() && r.errorCode == kLogFull) return failLogFull_("revoke", m.name);
        }
        switch (classify_(r)) {
        case Kind::Ok:
            if (suspended_) return fail_("I8", "a revoke was acknowledged while the namespace is suspended");
            if (!isRight) {
                if (m.doubt && version == m.live) { m.pendingRevoke.insert(version); m.live = 0; return true; }
                return fail_("I1", "a revoke of " + m.name + " at the fenced version " + std::to_string(version) + " was acknowledged (live " + std::to_string(m.live) + ")");
            }
            if (const std::string e = model_.revokeAcked(k, version); !e.empty()) return failModel_(e);
            ++stats_.revokesAcked;
            return true;
        case Kind::NotFound:
            if (!isRight) { ++stats_.fencedRefused; return true; }
            if (Env::kInjected) return fail_("I1", "the revoke of the live lease " + m.name + " v" + std::to_string(version) + " answered LeaseNotFound");
            if (after >= m.lo) return true;
            return fail_("I1", "the revoke of " + m.name + " v" + std::to_string(version) + " answered LeaseNotFound before the acknowledged deadline");
        default:
            if (suspended_ || !isRight) return true;
            if (Env::kInjected) return fail_("revoke", "the revoke of " + m.name + " was refused: " + text_(r));
            model_.inDoubt(k, after, Env::kTopicMaxTtlMs);
            ++stats_.doubts;
            return true;
        }
    }

    // ---------------------------------------------------------------- transient mode (spec Appendix A)

    struct Filler {
        std::string name;
        int64_t     version = 0;   ///< 0: no live filler lease
        int64_t     hiNs    = 0;   ///< latest possible deadline
        int64_t     ttlMs   = 0;
        uint64_t    a       = 0;
    };

    void pumpWatch_() {
        if (watch_) watch_->doWork(env_.watchClient().nowNs());
    }
    /// A grant held by the fence timed out at the client: the client's producer keeps it and appends it once the fence opens, so the key is in doubt
    /// (the log decides) and a ghost until the tap has shown the record.
    void noteHeld_(int k, const rc::LeaseResult& r, int64_t afterNs, int64_t fenceNs) {
        if (r.errorCode == kThrottled) return;   // a definite answer: nothing was appended
        model_.inDoubt(k, std::max(afterNs, fenceNs), Env::kTopicMaxTtlMs);
        ghosts_.insert(k);
        ++stats_.doubts;
    }
    /// Waits (bounded) for the held grants of noteHeld_ to reach the log once the fence is open; a grant the client dropped never arrives.
    bool awaitGhosts_() {
        if (ghosts_.empty() || fenceActive_()) return true;
        for (int i = 0; i < 1800; ++i) {
            if (!drain_()) return false;
            bool all = true;
            for (const int k : ghosts_) all = all && model_.key(k).foldLive != 0;
            if (all) break;
            std::this_thread::sleep_for(1ms);
        }
        ghosts_.clear();
        return true;
    }
    [[nodiscard]] bool fenceActive_() const {
        for (const int64_t f : fenceUntilP_)
            if (f != 0) return true;
        return false;
    }
    [[nodiscard]] int liveModelKeys_() const {
        int n = 0;
        for (int k = 0; k < kModelKeys; ++k) n += model_.key(k).live != 0 ? 1 : 0;
        return n;
    }
    /// True when the live leases (model keys + fillers, about 256 bytes each with the record batch) fill more than half the ring: a refusal is then right.
    [[nodiscard]] bool ringFullOfLive_() const { return static_cast<int64_t>(liveModelKeys_() + static_cast<int>(fill_.size())) * 256 > Env::kRingBytes / 2; }
    /// A client append answered TransientLogFull: with room to carry that is a violation (T1/T3), except while a node is down or a promotion
    /// is settling (the leader's floor then waits for a follower).  true = excused (nothing was appended), false = failed.
    bool failLogFull_(const char* what, const std::string& key) {
        if (ringFullOfLive_()) { ++stats_.logFullExcused; return true; }
        if constexpr (!Env::kInjected) {
            if (nodeDown_ != 0 || env_.nowNs() < promoteUntilNs_) { ++stats_.logFullExcused; return true; }
        }
        return fail_("T1", std::string("T1/T3: ring refused while carry should make room: the ") + what + " of " + key + " answered TransientLogFull (live " + std::to_string(liveModelKeys_()) +
                               " model leases, ring " + std::to_string(Env::kRingBytes) + " bytes)");
    }

    /// The gen-0 fence opens on its own (an open record); the driver waits (bounded) so the first grants meet an open partition.
    bool awaitFencesOpen_(const char* why) {
        const auto end = std::chrono::steady_clock::now() + 60s;
        for (;;) {
            bool all = true;
            for (int p = 0; p < env_.partitions(); ++p) all = all && env_.fenceOpen(p);
            if (all) return true;
            if (std::chrono::steady_clock::now() >= end) return fail_("T4", std::string("the rebirth fence never opened (") + why + ")");
            std::this_thread::sleep_for(2ms);
        }
    }
    bool setupTransient_() {
        keyPart_.assign(static_cast<std::size_t>(kModelKeys), -1);
        for (int k = 0; k < kModelKeys; ++k) keyPart_[static_cast<std::size_t>(k)] = ops_().partitionOf(row_(k, 0));
        fenceUntilP_.assign(static_cast<std::size_t>(env_.partitions()), 0);
        fill_.assign(static_cast<std::size_t>(env_.partitions()), Filler{});
        for (int p = 0; p < env_.partitions(); ++p) {
            for (int i = 0; i < 64 && fill_[static_cast<std::size_t>(p)].name.empty(); ++i) {
                const std::string n = "zf" + std::to_string(i);
                if (ops_().partitionOf(rowNamed_(n, 0)) == p) fill_[static_cast<std::size_t>(p)].name = n;
            }
            if (fill_[static_cast<std::size_t>(p)].name.empty()) return fail_("setup", "no filler key for partition " + std::to_string(p));
        }
        if (!awaitFencesOpen_("gen 0")) return false;
        rc::LeaseWatchOptions wo;
        wo.maxLive = 256;
        watch_ = std::make_unique<rc::LeaseWatch<LeaseClaim>>(env_.watchClient(), env_.reads(), wo);
        watch_->setHandler(&WLog::fn, &wlog_);
        const std::vector<uint8_t> none;
        const std::span<const uint8_t> prefixes[] = {std::span<const uint8_t>(none)};
        if (watch_->startFromNow(prefixes) != rc::OpResult::Accepted) return fail_("setup", "the lease watch did not start");
        const auto end = std::chrono::steady_clock::now() + 30s;
        while (!watch_->isReady()) {
            pumpWatch_();
            if (watch_->failed() || std::chrono::steady_clock::now() >= end) return fail_("setup", "the lease watch never became ready");
            std::this_thread::sleep_for(2ms);
        }
        return true;
    }

    /// T5 / the watch invariant: the watch's live set is the model's (polls, capped: the watch trails the log).  Fillers are not in the model.
    bool syncWatch_(const char* why) {
        ++stats_.watchChecks;
        std::string last;
        for (int i = 0; i < kPollCap; ++i) {
            if (!drain_()) return false;
            bool ok = true;
            for (int k = 0; k < kModelKeys && ok; ++k) {
                const lm::KeyModel& m = model_.key(k);
                if (m.doubt) continue;
                const auto it = wlog_.live.find(m.name);
                if (m.live != 0 ? (it == wlog_.live.end() || it->second != m.live) : it != wlog_.live.end()) {
                    ok = false;
                    last = "the watch shows " + m.name + (it == wlog_.live.end() ? " not live" : " at version " + std::to_string(it->second)) + " but the model's live version is " + std::to_string(m.live);
                }
            }
            if (ok) return true;
            std::this_thread::sleep_for(1ms);
        }
        if (watch_->failed()) last += " (the watch failed)";
        return fail_("T5", std::string("the lease watch disagrees with the model (") + why + "): " + last);
    }

    // ---- fillers and churn

    /// A live filler lease for partition `p` (granted on demand); false when it cannot be had now (fence, a leader change on three nodes).
    bool ensureFiller_(int p) {
        Filler& f = fill_[static_cast<std::size_t>(p)];
        if (f.version != 0) return true;
        if (fenceUntilP_[static_cast<std::size_t>(p)] != 0) return false;
        rc::LeaseOptions o;
        o.ttlMs = static_cast<uint32_t>(Env::kTopicMaxTtlMs);
        const int64_t before = env_.nowNs();
        rc::LeaseResult r;
        try { r = ops_().grantAndWait(rowNamed_(f.name, ++f.a), o); } catch (const std::exception&) { r = rc::LeaseResult{rc::OpStatus::Timeout, 0}; }
        const int64_t after = env_.nowNs();
        trace_("filler " + f.name + " grant -> " + (r.ok() ? "Ok v" + std::to_string(r.version) : text_(r)));
        if (r.ok()) {
            f.version = r.version;
            f.ttlMs   = r.ttlMs;
            f.hiNs    = after + r.ttlMs * lm::kNsPerMs;
            (void)before;
            return true;
        }
        if (r.errorCode == kLogFull) return failLogFull_("filler grant", f.name) && false;
        if (Env::kInjected && r.errorCode != kThrottled) return fail_("T1", "the filler grant of " + f.name + " was refused: " + text_(r)) && false;
        return false;
    }
    /// Keeps the fillers alive (they are not in the model, so maintain_ would not).
    bool maintainFillers_() {
        const int64_t now = env_.nowNs();
        for (std::size_t p = 0; p < fill_.size(); ++p) {
            Filler& f = fill_[p];
            if (f.version == 0 || f.hiNs - now >= f.ttlMs * lm::kNsPerMs / 2) continue;
            rc::LeaseResult r;
            try { r = ops_().keepAliveOnceAndWait(rowNamed_(f.name, 0), f.version); } catch (const std::exception&) { r = rc::LeaseResult{rc::OpStatus::Timeout, 0}; }
            const int64_t after = env_.nowNs();
            if (r.ok()) { f.hiNs = std::max(f.hiNs, after + r.ttlMs * lm::kNsPerMs); continue; }
            if (r.errorCode == kNotFound) {
                if (Env::kInjected && !fenceActive_()) return fail_("T1", "the filler lease " + f.name + " v" + std::to_string(f.version) + " was lost: its keepalive answered LeaseNotFound");
                f.version = 0;   // three nodes: it may have lapsed in a failover
            }
        }
        return true;
    }
    /// One churn step: filler updates that push the ring past its carry threshold.  Every append must succeed (a Throttled one is retried).
    bool opChurn_() {
        ScopedSecs ts(stats_.secChurn);
        if constexpr (Env::kTransient) {
            const int p = static_cast<int>(pick_(static_cast<uint32_t>(fill_.size())));
            if (fenceUntilP_[static_cast<std::size_t>(p)] != 0 || ringFullOfLive_()) { ++stats_.churnSkipped; trace_("churn skipped (fence or a ring full of live leases)"); return true; }
            if (!ensureFiller_(p)) {
                if (!failure_.empty()) return false;
                ++stats_.churnSkipped;
                return true;
            }
            Filler& f = fill_[static_cast<std::size_t>(p)];
            int done = 0;
            for (int i = 0; i < kChurnAppends; ++i) {
                rc::LeaseResult r;
                for (int t = 0; t < 2000; ++t) {
                    try { r = ops_().updateAndWait(rowNamed_(f.name, ++f.a), f.version); } catch (const std::exception&) { r = rc::LeaseResult{rc::OpStatus::Timeout, 0}; }
                    if (r.ok() || r.errorCode != kThrottled) break;
                    std::this_thread::sleep_for(1ms);
                }
                if (r.ok()) { ++done; ++stats_.churnAppends; }
                else if (r.errorCode == kLogFull) {
                    trace_("churn " + std::to_string(done) + " appends, then TransientLogFull");
                    return failLogFull_("filler update", f.name);
                } else if (r.errorCode == kNotFound) {
                    if (Env::kInjected) return fail_("T1", "the filler lease " + f.name + " was lost: an update answered LeaseNotFound");
                    f.version = 0;
                    break;
                } else {
                    if (Env::kInjected) return fail_("churn", "a filler update was refused: " + text_(r));
                    break;   // three nodes: a failover in the middle of the churn
                }
                if ((i & 31) == 31) {
                    if constexpr (!Env::kInjected) { if (!maintain_()) return false; }
                    pumpWatch_();
                }
            }
            trace_("churn " + std::to_string(done) + " filler updates on partition " + std::to_string(p));
            stats_.carried = env_.carriedRecords();
            // T1 after the churn: every lease the model holds is still live in the view, and a view recomputed from the ring has it (T2)
            if (!drain_()) return false;
            for (int k = 0; k < kModelKeys; ++k) {
                const lm::KeyModel& m = model_.key(k);
                if (m.live != 0 && !m.doubt && !checkKeyView_(k, "T1")) return false;
            }
            if (nodeDown_ == 0 || Env::kInjected) {
                bool deferred = false;
                if (!settle_(&deferred)) return false;
                if (deferred) ++stats_.recomputeDeferred;
                else if (!ringRecompute_("churn")) return false;
            }
        }
        return true;
    }

    /// T2: a reader that starts at the ring's head (a follower that reset its ring to the leader's start, a view rebuilt after a promotion)
    /// folds every lease the model holds, with its version and its row.  A fresh subscription from Earliest, folded leniently, compared with
    /// the log fold the model keeps (which settle_ has matched to the acknowledged state).
    bool ringRecompute_(const char* why) {
        if constexpr (Env::kTransient) {
            LogTap scratch(env_.tapClient(), env_.topicId(), env_.partitions(), false, false);
            std::map<std::string, std::pair<int64_t, uint64_t>> live;   // key -> (version, a)
            std::set<std::pair<std::string, int64_t>> endedVers;
            std::vector<lm::LogRec> rec;
            std::string last;
            for (int i = 0; i < kPollCap; ++i) {
                if (!drain_()) return false;
                scratch.drain(rec, 0);
                for (const lm::LogRec& r : rec) {
                    if (isFiller_(r.key)) continue;
                    if (r.op == lm::RecOp::Put) {
                        if (r.carry && endedVers.count({r.key, r.version})) continue;   // a stale Carry after its key's end: never applied
                        live[r.key] = {r.version, r.a};
                    } else if (r.op == lm::RecOp::Revoke || r.op == lm::RecOp::Expire) {
                        endedVers.insert({r.key, r.version});
                        const auto it = live.find(r.key);
                        if (it != live.end() && it->second.first == r.version) live.erase(it);
                    }
                }
                bool agree = true;
                for (int k = 0; k < kModelKeys && agree; ++k) {
                    const lm::KeyModel& m = model_.key(k);
                    if (m.doubt) continue;
                    const auto it = live.find(m.name);
                    const int64_t got = it == live.end() ? 0 : it->second.first;
                    if (got != m.foldLive || (m.foldLive != 0 && it->second.second != m.foldA)) {
                        agree = false;
                        last = std::string("a reader from the ring's head folds ") + m.name + " at version " + std::to_string(got) +
                               (it != live.end() ? " a=" + std::to_string(it->second.second) : std::string()) + " but the log fold (model live " + std::to_string(m.live) + ") has version " +
                               std::to_string(m.foldLive) + " a=" + std::to_string(m.foldA);
                    }
                }
                if (agree) { ++stats_.recomputes; return true; }
                std::this_thread::sleep_for(1ms);
            }
            return fail_("T2", std::string("a view rebuilt from the ring lacks a live lease (") + why + "): " + last);
        }
        return true;
    }

    // ---- the rebirth fence and the full restart

    /// Moves `fenceUntilP_` to open where the runtime says so (a partition whose fence deadline passed and whose open record folded).
    bool updateFence_() {
        if (!fenceActive_()) { watchLost_.clear(); return true; }
        const int64_t now = env_.nowNs();
        for (int p = 0; p < env_.partitions(); ++p) {
            int64_t& f = fenceUntilP_[static_cast<std::size_t>(p)];
            if (f != 0 && now >= f && env_.fenceOpen(p)) f = 0;
        }
        // T5: a lost key is ended exactly once, however long the fence lasts
        for (const auto& [name, base] : watchLost_) {
            const auto it = wlog_.ended.find(name);
            if (it != wlog_.ended.end() && it->second - base > 1)
                return fail_("T5", "the watch reported " + name + " ended " + std::to_string(it->second - base) + " times after the full restart");
        }
        return true;
    }
    /// Injected clock: moves it through the fence.  One ms short of the deadline a grant is still Throttled (never early); past it the fence
    /// opens (the open record goes through the expiry path) and grants are judged again.
    bool stepAdvanceFence_() {
        if constexpr (Env::kInjected && Env::kTransient) {
            int pMax = 0;
            for (int p = 0; p < env_.partitions(); ++p)
                if (fenceUntilP_[static_cast<std::size_t>(p)] > fenceUntilP_[static_cast<std::size_t>(pMax)]) pMax = p;
            const int64_t dl  = fenceUntilP_[static_cast<std::size_t>(pMax)];
            int64_t       now = env_.nowNs();
            if (dl - 2 * lm::kNsPerMs > now) {
                env_.advance(dl - 2 * lm::kNsPerMs - now);
                const int k = probe_[static_cast<std::size_t>(pMax)];
                rc::LeaseOptions o;
                o.ttlMs        = 1000;
                o.opTimeoutMs  = 1000;
                rc::LeaseResult g;
                try { g = ops_().grantAndWait(row_(k, 0), o, 1s); } catch (const std::exception&) { g = rc::LeaseResult{rc::OpStatus::Timeout, 0}; }
                trace_("fence probe 2 ms short of the deadline: grant " + model_.key(k).name + " -> " + (g.ok() ? "Ok" : text_(g)));
                if (g.ok()) return fail_("T4", "T4: grant during fence: a grant succeeded 2 ms before the fence deadline");
                if (g.errorCode != kThrottled && g.status != rc::OpStatus::Timeout) return fail_("T4", "a grant 2 ms short of the fence deadline answered " + text_(g) + ", want Throttled");
                ++stats_.fencedShort;
                noteHeld_(k, g, env_.nowNs(), dl);
            }
            now = env_.nowNs();
            if (dl + 2 * kTickNs > now) env_.advance(dl + 2 * kTickNs - now);
            for (int i = 0; i < kPollCap; ++i) {
                bool all = true;
                for (int p = 0; p < env_.partitions(); ++p) all = all && env_.fenceOpen(p);
                if (all) break;
                std::this_thread::sleep_for(1ms);
            }
            for (int p = 0; p < env_.partitions(); ++p)
                if (!env_.fenceOpen(p)) return fail_("T4", "the fence of partition " + std::to_string(p) + " never opened after the clock passed its deadline");
            std::fill(fenceUntilP_.begin(), fenceUntilP_.end(), int64_t{0});
            trace_("fence open");
        }
        return true;
    }

    /// T5, holders: the keepalive / update / revoke of a pre-restart lease answers LeaseNotFound (retried while the answer is not definite).
    bool probeLostHolders_() {
        int i = 0;
        for (const auto& [k, version] : lost_) {
            ++stats_.holderProbes;
            const int kind = i++ % 3;
            bool done = false;
            std::string last;
            for (int t = 0; t < kPollCap && !done; ++t) {
                rc::LeaseResult r;
                try {
                    if (kind == 0) r = ops_().keepAliveOnceAndWait(row_(k, 0), version, {}, 5s);
                    else if (kind == 1) r = ops_().updateAndWait(row_(k, ++aSeq_), version, {}, 5s);
                    else r = ops_().revokeAndWait(row_(k, 0), version, {}, 5s);
                } catch (const std::exception&) { r = rc::LeaseResult{rc::OpStatus::Timeout, 0}; }
                if (r.ok()) return fail_("T5", std::string("the ") + (kind == 0 ? "keepalive" : kind == 1 ? "update" : "revoke") + " of " + model_.key(k).name + " v" + std::to_string(version) + ", a lease of the lost lineage, was acknowledged");
                if (r.errorCode == kNotFound) { done = true; break; }
                last = text_(r);
                std::this_thread::sleep_for(2ms);
            }
            if (!done) return fail_("T5", "the " + std::string(kind == 0 ? "keepalive" : kind == 1 ? "update" : "revoke") + " of " + model_.key(k).name + " v" + std::to_string(version) +
                                              " never answered LeaseNotFound (last: " + last + ")");
        }
        return true;
    }
    /// T5, watchers: every key the watch held before the restart is reported ended, exactly once.
    bool awaitWatchEnded_() {
        const auto end = std::chrono::steady_clock::now() + 60s;
        for (;;) {
            if (!drain_()) return false;
            std::string missing;
            for (const auto& [name, base] : watchLost_) {
                const auto it = wlog_.ended.find(name);
                if (it == wlog_.ended.end() || it->second < base + 1) missing += " " + name;
            }
            if (missing.empty()) break;
            if (std::chrono::steady_clock::now() >= end || watch_->failed())
                return fail_("T5", "the lease watch never reported these pre-restart keys ended:" + missing + (watch_->failed() ? " (the watch failed)" : ""));
            std::this_thread::sleep_for(2ms);
        }
        for (const auto& [name, base] : watchLost_)
            if (wlog_.ended[name] != base + 1) return fail_("T5", "the watch reported " + name + " ended " + std::to_string(wlog_.ended[name] - base) + " times after the full restart, want 1");
        return true;
    }
    bool stepFullRestart_() {
        if constexpr (Env::kTransient) {
            ScopedSecs ts(stats_.secRestart);
            // quiesce: the tap holds everything, the watch shows exactly the model's live set
            if (!awaitGhosts_()) return false;
            ghosts_.clear();
            if (!drain_()) return false;
            if (!syncWatch_("before the full restart")) return false;
            lost_.clear();
            for (int k = 0; k < kModelKeys; ++k) {
                const lm::KeyModel& m = model_.key(k);
                if (m.live != 0 && !m.doubt) lost_.emplace_back(k, m.live);
            }
            watchLost_.clear();
            for (const auto& [name, v] : wlog_.live) { (void)v; watchLost_[name] = wlog_.ended[name]; }
            stats_.carriedAtRestart += env_.carriedRecords();
            stats_.carried = 0;
            const int64_t tBegin = env_.nowNs();
            trace_("full restart: " + std::to_string(lost_.size()) + " model leases and " + std::to_string(watchLost_.size()) + " watched leases are lost");
            env_.fullRestart();
            // the new lineage: offsets, so versions, start over; every lease is gone
            model_.rebirth();
            env_.tap().rebirth();
            ended_.clear();
            expectA_.fill(0);
            for (Filler& f : fill_) f.version = 0;
            ++restartsDone_;
            ++stats_.fullRestarts;
            for (int p = 0; p < env_.partitions(); ++p) {
                const int64_t dl = env_.fenceDeadlineNs(p);
                fenceUntilP_[static_cast<std::size_t>(p)] = dl != 0 ? dl : tBegin + Env::kTopicMaxTtlMs * lm::kNsPerMs;
            }
            if constexpr (!Env::kInjected) promoteUntilNs_ = std::max(promoteUntilNs_, env_.nowNs() + env_.promotionWindowNs());
            trace_("restarted; the fence holds grants until " + std::to_string((fenceUntilP_[0] - env_.nowNs()) / lm::kNsPerMs) + " ms from now (partition 0)");
            if (!probeLostHolders_()) return false;
            if (!awaitWatchEnded_()) return false;
        }
        return true;
    }

    // ---------------------------------------------------------------- maintenance (the driver keeps its kept leases alive)

    /// Keepalives the kept leases that are within half a ttl of their latest possible deadline.
    bool maintain_() {
        if constexpr (Env::kTransient) {
            if (!maintainFillers_()) return false;
        }
        if (suspendedMode_ == Suspended::RejectAll) {
            for (int k = 0; k < kKeys; ++k) model_.key(k).kept = false;   // RejectAll refuses keepalives: nothing can be kept
            return true;
        }
        const int64_t now = env_.nowNs();
        for (int k = 0; k < kKeys; ++k) {
            const lm::KeyModel& m = model_.key(k);
            if (m.live == 0 || m.doubt || !m.kept) continue;
            if (m.hi - now < m.ttlMs * lm::kNsPerMs / 2)
                if (!opKeepAlive_(k, true)) return false;
        }
        return true;
    }

    // ---------------------------------------------------------------- the steps

    int pickKey_() { return static_cast<int>(pick_(kKeys)); }
    int pickLiveKey_(bool keptOnly) {
        std::vector<int> c;
        for (int k = 0; k < kKeys; ++k) {
            const lm::KeyModel& m = model_.key(k);
            if (m.live != 0 && !m.doubt && (!keptOnly || m.kept)) c.push_back(k);
        }
        return c.empty() ? pickKey_() : c[pick_(static_cast<uint32_t>(c.size()))];
    }

    Step chooseStep_() {
        std::array<uint32_t, kSteps> w = Env::kTransient ? (Env::kInjected ? kWeightsTS : kWeightsTT) : (Env::kInjected ? kWeightsS : kWeightsT);
        if constexpr (Env::kInjected) {
            if (suspended_) { w[static_cast<std::size_t>(Step::Suspend)] = 0; w[static_cast<std::size_t>(Step::Resume)] = 18; w[static_cast<std::size_t>(Step::Advance)] *= 3; w[static_cast<std::size_t>(Step::AlterCheckpoint)] = 0; }
        } else {
            if (nodeDown_ != 0) {
                w[static_cast<std::size_t>(Step::KillLeader)] = 0;
                w[static_cast<std::size_t>(Step::RestartNode)] = (step_ - killedAtStep_ >= 6) ? 1000 : 6;
            }
            if (kills_ >= kMaxKills) w[static_cast<std::size_t>(Step::KillLeader)] = 0;
        }
        if constexpr (Env::kTransient) {
            if (restartsDone_ >= kMaxFullRestarts || nodeDown_ != 0) w[static_cast<std::size_t>(Step::FullRestart)] = 0;
            if (nodeDown_ != 0 || fenceActive_()) w[static_cast<std::size_t>(Step::Churn)] = 0;
        }
        uint32_t total = 0;
        for (uint32_t x : w) total += x;
        uint32_t r = pick_(total);
        for (std::size_t i = 0; i < kSteps; ++i) {
            if (r < w[i]) return static_cast<Step>(i);
            r -= w[i];
        }
        return Step::Grant;
    }

    bool oneStep_() {
        // The driver keeps its kept leases alive first (on the real clock this is what lets a lease survive many steps).
        if constexpr (!Env::kInjected) { if (!maintain_()) return false; }
        const Step s = chooseStep_();
        ++stats_.steps[static_cast<std::size_t>(s)];
        int touched = -1;
        switch (s) {
        case Step::Grant: {
            touched = pickKey_();
            // a grant mostly targets a free key; sometimes a live one (KeyExists)
            if (model_.key(touched).live != 0 && pick_(100) < 70) {
                for (int i = 0; i < 8 && model_.key(touched).live != 0; ++i) touched = pickKey_();
            }
            if (model_.key(touched).doubt) { ++stats_.skipped; trace_("grant skipped (doubt)"); break; }
            if constexpr (Env::kTransient && Env::kInjected) {
                // a held grant stays in the client's producer queue and holds every later write of its partition back until the injected clock
                // opens the fence: one single-node grant is held per fence (stepAdvanceFence_'s probe), the rest wait
                if (fenceUntilP_[static_cast<std::size_t>(keyPart_[static_cast<std::size_t>(touched)])] != 0) { ++stats_.skipped; trace_("grant skipped (fence)"); break; }
            }
            if (!opGrant_(touched)) return false;
            if (model_.key(touched).live != 0 && model_.key(touched).kept) {
                if (pick_(100) < 35) { model_.key(touched).kept = false; model_.key(touched).abandonedAtNs = env_.nowNs(); }   // granted and left alone
            }
            break;
        }
        case Step::KeepAlive: {
            const bool wantRight = pick_(100) < 70;
            touched = wantRight ? pickLiveKey_(true) : pickKey_();
            if (model_.key(touched).doubt) { ++stats_.skipped; break; }
            // the live version on a kept lease; a fenced version or an ended key otherwise
            const bool right = wantRight && model_.key(touched).live != 0 && model_.key(touched).kept;
            if (!opKeepAlive_(touched, right)) return false;
            break;
        }
        case Step::KeepAliveGap: {
            touched = pickLiveKey_(true);
            lm::KeyModel& m = model_.key(touched);
            m.kept = false;
            m.abandonedAtNs = env_.nowNs();
            trace_("gap " + m.name + " (no keepalive from now on; latest deadline in " + std::to_string((m.hi - env_.nowNs()) / 1'000'000) + " ms)");
            break;
        }
        case Step::Update: {
            touched = pickLiveKey_(false);
            if (model_.key(touched).doubt) { ++stats_.skipped; break; }
            if (!opUpdate_(touched, pick_(100) < 75)) return false;
            break;
        }
        case Step::Revoke: {
            touched = pickLiveKey_(false);
            if (model_.key(touched).doubt) { ++stats_.skipped; break; }
            if (!opRevoke_(touched, pick_(100) < 75)) return false;
            break;
        }
        case Step::AlterCheckpoint: {
            static constexpr uint32_t kIntervalsS[] = {1000, 2500, 7000};
            static constexpr uint32_t kIntervalsT[] = {1000, 2000};
            const bool on = pick_(100) < 75;
            const uint32_t iv = Env::kInjected ? kIntervalsS[pick_(3)] : kIntervalsT[pick_(2)];
            ScopedSecs ta(stats_.secAlter);
            const bool ok = env_.alterCheckpoint(on, iv);
            trace_("alter checkpoint " + std::string(on ? "on" : "off") + " interval " + std::to_string(iv) + " -> " + (ok ? "ok" : "refused"));
            if (!ok) {
                if (Env::kInjected) return fail_("alter", "AlterTopicConfig of the lease checkpoint was refused");
                ++stats_.skipped;
            }
            break;
        }
        case Step::Suspend:
            if constexpr (Env::kInjected) {
                const bool rejectAll = pick_(2) == 0;
                if (!env_.suspend(rejectAll)) return fail_("suspend", "the namespace suspend was refused");
                suspended_ = true;
                suspendedMode_ = rejectAll ? Suspended::RejectAll : Suspended::ReadOnly;
                suspendedAtStep_ = step_;
                trace_(std::string("suspend ") + (rejectAll ? "RejectAll" : "ReadOnly"));
            }
            break;
        case Step::Resume:
            if constexpr (Env::kInjected) {
                if (!suspended_) break;
                if (!env_.resume()) return fail_("resume", "the namespace resume was refused");
                suspended_ = false;
                const bool wasRejectAll = suspendedMode_ == Suspended::RejectAll;
                suspendedMode_ = Suspended::None;
                trace_("resume");
                if (wasRejectAll && !(ScopedSecs(stats_.secRecover), env_.recoverOps())) return fail_("resume", "the operations client never recovered after RejectAll was lifted");
            }
            break;
        case Step::Advance:
            if constexpr (Env::kInjected) {
                if (suspended_) ++stats_.suspendedAdvances;
                if (!stepAdvance_()) return false;
            }
            break;
        case Step::Sleep:
            if constexpr (!Env::kInjected) {
                const uint32_t ms = 20 + pick_(130);
                trace_("sleep " + std::to_string(ms) + " ms");
                std::this_thread::sleep_for(std::chrono::milliseconds(ms));
            }
            break;
        case Step::KillLeader:
            if constexpr (!Env::kInjected) {
                const int dead = env_.killLeader(static_cast<int>(pick_(2)));
                if (dead < 0) { ++stats_.skipped; trace_("kill skipped (the leader is the client's seed node)"); break; }
                ++kills_; ++stats_.kills;
                nodeDown_ = dead;
                killedAtStep_ = step_;
                promoteUntilNs_ = std::max(promoteUntilNs_, env_.nowNs() + env_.promotionWindowNs());
                trace_("killed leader node " + std::to_string(dead));
            }
            break;
        case Step::RestartNode:
            if constexpr (!Env::kInjected) {
                if (nodeDown_ == 0) break;
                env_.restartNode(nodeDown_);
                trace_("restarted node " + std::to_string(nodeDown_));
                nodeDown_ = 0;
                ++stats_.restarts;
                promoteUntilNs_ = std::max(promoteUntilNs_, env_.nowNs() + env_.promotionWindowNs());
                // I5 after every restart (leg T): the view rows equal the recompute from the log, at the next quiet point
                if (!drain_()) return false;
                if (!fullCheck_("restart")) return false;
            }
            break;
        case Step::Churn:
            if constexpr (Env::kTransient) {
                if (!opChurn_()) return false;
            }
            break;
        case Step::FullRestart:
            if constexpr (Env::kTransient) {
                if (!stepFullRestart_()) return false;
            }
            break;
        case Step::kCount: break;
        }
        // ---- the invariants, after EVERY step
        if (!drain_()) return false;
        if constexpr (Env::kTransient) {
            if (!updateFence_()) return false;
        }
        if (!(Env::kInjected == false && nodeDown_ != 0)) {
            // the lease view of the touched key after a write or a fenced call, and of every key that ended since the last check (up to 6 a step;
            // the rest at the next full check)
            if (touched >= 0 && viewDue_ && !checkKeyView_(touched)) return false;
            int n = 0;
            while (!ended_.empty() && n++ < 2) {
                const int ek = ended_.back();
                ended_.pop_back();
                if (ek != touched || !viewDue_) { if (!checkKeyView_(ek)) return false; }
            }
            if (ended_.size() > 24) ended_.clear();
        }
        viewDue_ = false;
        if constexpr (!Env::kInjected) {
            if (!checkDue_(100)) return false;
        }
        const int fullEvery = Env::kInjected ? 500 : 100;
        if ((step_ % fullEvery == 0 || (fullDue_ && !suspended_)) && !(Env::kInjected == false && nodeDown_ != 0)) {
            if (!fullCheck_("periodic")) return false;
        }
        if (step_ % 50 == 0) {
            const uint64_t stale = env_.staleBroker();
            if (stale != 0) return fail_("I4", "stale_broker == " + std::to_string(stale) + ", want 0");
        }
        return true;
    }

    /// Leg S: the clock moves to the next abandoned lease's deadline (+ the duty tick of slack, +1 tick) or by 1..50 ms, in hops short enough
    /// to keep the kept leases alive; after each hop every lease past its deadline must end (I7), suspended or not (I8).
    bool stepAdvance_() {
        if constexpr (Env::kInjected) {
            if constexpr (Env::kTransient) {
                if (fenceActive_()) return stepAdvanceFence_();   // no lease is live while the fence holds: the clock goes straight through it
            }
            int64_t now = env_.nowNs();
            int64_t target;
            const int64_t nextAb = model_.nextDeadlineOfAbandoned();
            if (nextAb != 0 && nextAb > now && pick_(100) < 45) target = nextAb + 2 * kTickNs;
            else target = now + static_cast<int64_t>(1 + pick_(50)) * lm::kNsPerMs;
            if (target <= now) target = now + lm::kNsPerMs;
            const int64_t from = now;
            uint32_t hops = 0;
            while (now < target) {
                if (++hops > 400) return fail_("driver", "an advance needed more than 400 hops");
                if (!maintain_()) return false;
                int64_t next = target;
                for (int k = 0; k < kKeys; ++k) {
                    const lm::KeyModel& m = model_.key(k);
                    if (m.live == 0 || m.doubt) continue;
                    if (m.kept) next = std::min(next, m.hi - 2 * kTickNs);
                }
                if constexpr (Env::kTransient) {
                    for (const Filler& f : fill_)
                        if (f.version != 0) next = std::min(next, f.hiNs - 2 * kTickNs);
                }
                if (next <= now) next = now + kTickNs;   // a kept lease that could not be renewed (suspension): it will end
                // never stop inside a lease's [deadline, deadline + tick) window: the end is allowed but not required there
                for (int pass = 0; pass < 4; ++pass) {
                    for (int k = 0; k < kKeys; ++k) {
                        const lm::KeyModel& m = model_.key(k);
                        if (m.live != 0 && !m.doubt && m.hi <= next && next < m.hi + kTickNs) next = m.hi + kTickNs;
                    }
                }
                env_.advance(next - now);
                now = env_.nowNs();
                if (!checkDue_(kPollCap)) return false;
            }
            stats_.maxHops = std::max(stats_.maxHops, hops);
            trace_("advance " + std::to_string((now - from) / lm::kNsPerMs) + " ms in " + std::to_string(hops) + " hop(s)" + (suspended_ ? " [suspended]" : ""));
        }
        return true;
    }

    // ---------------------------------------------------------------- the end of a run

    bool finish_() {
        step_ = steps_ + 1;
        trace_("finish");
        if constexpr (Env::kTransient) {
            stats_.carried = env_.carriedRecords();
            if constexpr (Env::kInjected) {
                if (fenceActive_() && !stepAdvanceFence_()) return false;
            } else {
                const auto fenceEnd = std::chrono::steady_clock::now() + 60s;
                while (fenceActive_()) {   // the rebirth fence ends on its own (real clock)
                    if (!updateFence_()) return false;
                    if (std::chrono::steady_clock::now() >= fenceEnd) return fail_("T4", "the rebirth fence never opened");
                    std::this_thread::sleep_for(5ms);
                }
            }
        }
        if constexpr (Env::kInjected) {
            if (suspended_) {
                if (!env_.resume()) return fail_("resume", "the final namespace resume was refused");
                const bool wasRejectAll = suspendedMode_ == Suspended::RejectAll;
                suspended_ = false;
                suspendedMode_ = Suspended::None;
                if (wasRejectAll && !(ScopedSecs(stats_.secRecover), env_.recoverOps())) return fail_("resume", "the operations client never recovered after RejectAll was lifted");
            }
            for (int k = 0; k < kKeys; ++k) model_.key(k).kept = false;
            // every lease ends: one clock jump past the latest possible deadline
            int64_t latest = env_.nowNs();
            for (int k = 0; k < kKeys; ++k)
                if (model_.key(k).live != 0 && !model_.key(k).doubt) latest = std::max(latest, model_.key(k).hi);
            if (latest + 2 * kTickNs > env_.nowNs()) env_.advance(latest + 2 * kTickNs - env_.nowNs());
            if (!checkDue_(kPollCap)) return false;
        } else {
            if (nodeDown_ != 0) { env_.restartNode(nodeDown_); nodeDown_ = 0; ++stats_.restarts; }
            for (int k = 0; k < kKeys; ++k) model_.key(k).kept = false;
            promoteUntilNs_ = std::max(promoteUntilNs_, env_.nowNs() + env_.promotionWindowNs());
            // wait (real time, bounded) for every lease to end
            const int64_t endNs = env_.nowNs() + Env::kTopicMaxTtlMs * lm::kNsPerMs + env_.promotionWindowNs() + Env::kI7SlackNs + 5'000'000'000LL;
            while (env_.nowNs() < endNs) {
                if (!drain_()) return false;
                bool anyLive = false;
                for (int k = 0; k < kKeys; ++k)
                    if (model_.key(k).live != 0 && !model_.key(k).doubt) anyLive = true;
                if (!anyLive) break;
                std::this_thread::sleep_for(20ms);
            }
            if (!checkDue_(kPollCap)) return false;
        }
        if (!fullCheck_("final")) return false;
        if (fullDue_) return fail_("I5", "the final check was deferred");
        if constexpr (Env::kTransient) {
            if (!syncWatch_("final")) return false;
            if (wlog_.bad != 0) return fail_("T5", "the watch reported " + std::to_string(wlog_.bad) + " end(s) of a key it did not hold");
            if (watch_->failed() || wlog_.failed != 0) return fail_("T5", "the lease watch failed");
        }
        for (int k = 0; k < kKeys; ++k) {
            const lm::KeyModel& m = model_.key(k);
            if (m.foldLive != 0 && !m.doubt && m.hi != 0)
                return fail_("I7", "after the clock passed every deadline, " + m.name + " version " + std::to_string(m.foldLive) + " is still live in the log");
        }
        return true;
    }

    enum class Suspended { None, ReadOnly, RejectAll };
    std::vector<std::string> slow_;
    static constexpr int kPollCap  = 4000;   ///< polls (1 ms each) a wait gets: the broker's own threads act, the deadline is the failure
    static constexpr int kMaxKills = 2;   ///< the recipe allows 3; 2 keeps a seed under 20 s (each kill+restart costs 2-4 s of failover)

    Env&                  env_;
    uint64_t              seed_;
    int                   steps_;
    int                   step_ = 0;
    std::mt19937_64       rng_;
    lm::Model             model_;
    Stats                 stats_;
    std::vector<lm::LogRec> recs_;
    std::vector<int>        ended_;     ///< keys with an end record drained since the last per-step view check
    bool                    viewDue_ = false;   ///< the step changed or probed the touched key's state: its view is checked
    std::deque<std::string> trace_buf_;
    std::string           failure_;
    std::array<uint64_t, kModelKeys> expectA_{};
    uint64_t              aSeq_ = 0;
    std::vector<int>      probe_;
    bool                  suspended_ = false;
    Suspended             suspendedMode_ = Suspended::None;
    int                   suspendedAtStep_ = 0;
    bool                  fullDue_ = false;
    int                   nodeDown_ = 0;
    int                   killedAtStep_ = 0;
    int                   kills_ = 0;
    int64_t               promoteUntilNs_ = 0;
    // transient mode
    static constexpr int  kMaxFullRestarts = 2;   ///< each costs a broker restart and a fence
    std::vector<int32_t>  gaps_;                  ///< the partitions a tap reset reported at the last drain
    std::vector<int>      keyPart_;               ///< model key -> partition
    std::set<int>         ghosts_;                ///< keys with a held grant the client may still deliver (noteHeld_)
    std::vector<int64_t>  fenceUntilP_;           ///< per partition: a lower bound of the rebirth fence's end (0: open)
    std::vector<Filler>   fill_;                  ///< one filler lease per partition (outside the model)
    std::unique_ptr<rc::LeaseWatch<LeaseClaim>> watch_;
    WLog                  wlog_;
    std::vector<std::pair<int, int64_t>> lost_;   ///< (model key, version) held at the last full restart
    std::map<std::string, int> watchLost_;        ///< keys the watch held at the last full restart -> their end count then
    int                   restartsDone_ = 0;
};

}  // namespace rune::node::test::leasestress
