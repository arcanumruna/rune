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
#include <rune/consensus/LeaseClock.h>
#include <rune/node/lease/LeaseWheel.h>
#include <rune/protocol/LeaseRecord.h>
#include <rune/protocol/client/Protocol.h>

#include <algorithm>
#include <array>
#include <chrono>
#include <cstring>
#include <deque>
#include <mutex>
#include <optional>
#include <random>
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
    Grant, KeepAlive, KeepAliveGap, Update, Revoke, AlterCheckpoint, Suspend, Resume, Advance, Sleep, KillLeader, RestartNode, kCount
};
inline constexpr std::size_t kSteps = static_cast<std::size_t>(Step::kCount);
inline constexpr const char* kStepNames[kSteps] = {"Grant", "KeepAlive", "KeepAliveGap", "Update", "Revoke", "AlterCheckpoint",
                                                   "Suspend", "Resume", "Advance", "Sleep", "KillLeader", "RestartNode"};

//                                          Grant KeepAlive Gap Update Revoke Alter Suspend Resume Advance Sleep Kill Restart
inline constexpr std::array<uint32_t, kSteps> kWeightsS = {24, 22, 6, 12, 6, 2, 1, 0, 14, 0, 0, 0};   ///< Resume is forced while suspended
inline constexpr std::array<uint32_t, kSteps> kWeightsT = {26, 24, 8, 12, 6, 2, 0, 0, 0, 7, 3, 0};    ///< Restart is forced after a kill

// ------------------------------------------------------------------------------------------------ the log tap

/// Raw subscriptions from offset 0 to every partition of the lease topic.  The handlers run on the client's duty thread and append to one
/// queue (test code: the mutex is the hand-off, the driver drains it on its own thread).
class LogTap {
public:
    LogTap(rc::Client& c, int32_t topicId, int32_t partitions, bool stampRealClock) : stampReal_(stampRealClock), client_(&c), topicId_(topicId), partitions_(partitions) {
        next_.assign(static_cast<std::size_t>(partitions), 0);
        subscribeAll_();
    }
    ~LogTap() {
        for (auto& s : subs_) s.unsubscribe();
    }
    [[nodiscard]] std::string describe() const {
        std::string d;
        for (std::size_t i = 0; i < subs_.size(); ++i)
            d += " p" + std::to_string(i) + "{active " + std::to_string(subs_[i].active()) + " finished " + std::to_string(subs_[i].finished()) + " fail " +
                 std::to_string(static_cast<int>(subs_[i].failReason())) + " pos " + std::to_string(subs_[i].position()) + "}";
        return d;
    }
    LogTap(const LogTap&) = delete;
    LogTap& operator=(const LogTap&) = delete;

    /// Moves everything received since the last drain to `out` (cleared first); an unstamped record gets `stampNs`.
    void drain(std::vector<lm::LogRec>& out, int64_t stampNs) {
        out.clear();
        {
            std::lock_guard<std::mutex> g(mu_);
            out.swap(q_);
        }
        for (lm::LogRec& r : out)
            if (r.rxNs == 0) r.rxNs = stampNs;
    }

private:
    void subscribeAll_() {
        const rc::TopicHandle h{topicId_, partitions_, rc::TopicKind::Typed};
        for (int32_t p = 0; p < partitions_; ++p) {
            rc::SubscribeOptions o;
            int64_t              at;
            {
                std::lock_guard<std::mutex> g(mu_);
                at = next_[static_cast<std::size_t>(p)];
            }
            o.from = at == 0 ? rc::From::earliest() : rc::From::offset(at);
            subs_.push_back(client_->subscribe(h, p, o, [this](const rc::Record& r) { onRecord_(r); }));
        }
    }
    void onRecord_(const rc::Record& r) {
        {
            std::lock_guard<std::mutex> g(mu_);
            int64_t& nx = next_[static_cast<std::size_t>(r.partition)];
            if (r.offset < nx) return;   // a re-subscription's overlap
            nx = r.offset + 1;
        }
        lm::LogRec x;
        x.partition = r.partition;
        x.offset    = r.offset;
        const auto op = rune::protocol::record::leaseOpOf(r.attributes);
        x.op = static_cast<lm::RecOp>(op);
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
    rc::Client*                   client_;
    int32_t                       topicId_, partitions_;
    std::vector<int64_t>          next_;
    std::mutex                    mu_;
    std::vector<lm::LogRec>       q_;
    std::vector<rc::Subscription> subs_;
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
    };

    Driver(Env& env, uint64_t seed, int steps) : env_(env), seed_(seed), steps_(steps), rng_(seed), model_(kModelKeys) {
        expectA_.fill(0);
    }

    /// Runs every step; true when every invariant held after every step.  failure() names seed, step, invariant and the last 30 steps.
    bool run() {
        try {
            if (!findProbeKeys_()) return false;
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
        env_.tap().drain(recs_, env_.nowNs());
        for (const lm::LogRec& r : recs_) {
            if (r.op == lm::RecOp::Expire) ++stats_.expiries;
            if (r.op == lm::RecOp::Expire || r.op == lm::RecOp::Revoke) {
                const int ek = model_.indexOf(r.key);
                if (ek >= 0 && ek < kKeys && std::find(ended_.begin(), ended_.end(), ek) == ended_.end()) ended_.push_back(ek);
            }
            const std::string e = model_.onRecord(r, Env::kI3SlackNs);
            if (!e.empty()) return failModel_(e + " [record offset " + std::to_string(r.offset) + " partition " + std::to_string(r.partition) + "]");
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
    bool checkKeyView_(int k) {
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
        return fail_("I1", last);
    }

    /// I5: after the tap holds what the model expects, the lease view rows equal the recompute from the log, and the counters agree.
    bool settle_(bool* deferred) {
        *deferred = false;
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
        // every acknowledged grant is in the log (I2); every ended version has exactly one end record (I4)
        for (int k = 0; k < kKeys; ++k) {
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
        const int64_t before = env_.nowNs();
        rc::LeaseResult r;
        try { ScopedSecs t(stats_.secOps); r = ops_().grantAndWait(row_(k, a), o); } catch (const std::exception& e) { r = rc::LeaseResult{rc::OpStatus::Timeout, 0}; if (Env::kInjected) return fail_("grant", e.what()); }
        const int64_t after = env_.nowNs();
        trace_("grant " + m.name + " ttl " + std::to_string(ttl) + " a=" + std::to_string(a) + " -> " + (r.ok() ? "Ok v" + std::to_string(r.version) : text_(r)) + " (live " + std::to_string(m.live) + ")");
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
        else version = m.granted.empty() ? int64_t{7} : m.granted[pick_(static_cast<uint32_t>(m.granted.size()))];
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
        const int64_t version = (right && m.live != 0) ? m.live : (m.live != 0 ? m.live + 1 + static_cast<int64_t>(pick_(5)) : (m.granted.empty() ? int64_t{7} : m.granted.back()));
        const bool isRight = version == m.live && m.live != 0 && !m.doubt;
        viewDue_ = true;
        const uint64_t a = ++aSeq_;
        rc::LeaseResult r;
        try { ScopedSecs t(stats_.secOps); r = ops_().updateAndWait(row_(k, a), version); } catch (const std::exception& e) { r = rc::LeaseResult{rc::OpStatus::Timeout, 0}; if (Env::kInjected) return fail_("update", e.what()); }
        const int64_t after = env_.nowNs();
        trace_("update " + m.name + " v" + std::to_string(version) + (isRight ? " (live)" : " (fenced)") + " a=" + std::to_string(a) + " -> " + (r.ok() ? "Ok" : text_(r)));
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
        const int64_t version = (right && m.live != 0) ? m.live : (m.live != 0 ? m.live + 1 + static_cast<int64_t>(pick_(5)) : (m.granted.empty() ? int64_t{7} : m.granted.back()));
        const bool isRight = version == m.live && m.live != 0 && !m.doubt;
        viewDue_ = true;
        rc::LeaseResult r;
        try { ScopedSecs t(stats_.secOps); r = ops_().revokeAndWait(row_(k, 0), version); } catch (const std::exception& e) { r = rc::LeaseResult{rc::OpStatus::Timeout, 0}; if (Env::kInjected) return fail_("revoke", e.what()); }
        const int64_t after = env_.nowNs();
        trace_("revoke " + m.name + " v" + std::to_string(version) + (isRight ? " (live)" : " (fenced)") + " -> " + (r.ok() ? "Ok" : text_(r)));
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

    // ---------------------------------------------------------------- maintenance (the driver keeps its kept leases alive)

    /// Keepalives the kept leases that are within half a ttl of their latest possible deadline.
    bool maintain_() {
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
        std::array<uint32_t, kSteps> w = Env::kInjected ? kWeightsS : kWeightsT;
        if constexpr (Env::kInjected) {
            if (suspended_) { w[static_cast<std::size_t>(Step::Suspend)] = 0; w[static_cast<std::size_t>(Step::Resume)] = 18; w[static_cast<std::size_t>(Step::Advance)] *= 3; w[static_cast<std::size_t>(Step::AlterCheckpoint)] = 0; }
        } else {
            if (nodeDown_ != 0) {
                w[static_cast<std::size_t>(Step::KillLeader)] = 0;
                w[static_cast<std::size_t>(Step::RestartNode)] = (step_ - killedAtStep_ >= 6) ? 1000 : 6;
            }
            if (kills_ >= kMaxKills) w[static_cast<std::size_t>(Step::KillLeader)] = 0;
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
        case Step::kCount: break;
        }
        // ---- the invariants, after EVERY step
        if (!drain_()) return false;
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
                if (next <= now) next = now + kTickNs;   // a kept lease that could not be renewed (suspension): it will end
                // never stop inside a lease's [deadline, deadline + tick) window: the end is allowed but not required there
                for (int pass = 0; pass < 4; ++pass) {
                    for (int k = 0; k < kKeys; ++k) {
                        const lm::KeyModel& m = model_.key(k);
                        if (m.live != 0 && !m.doubt && m.hi <= next && next < m.hi + kTickNs) next = m.hi + kTickNs;
                    }
                }
                env_.advance(next - now);
                now = next;
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
};

}  // namespace rune::node::test::leasestress
