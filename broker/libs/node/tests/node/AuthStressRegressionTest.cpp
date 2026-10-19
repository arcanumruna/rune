// AuthStressRegressionTest.cpp - authentication Task 18 (spec §11 "Seeded, model-checked stress", D40): ONE real
// authenticating shard-mode broker, racing security DDL (GRANT / REVOKE / GRANT ROLE / REVOKE ROLE / DISABLE / ENABLE,
// sent WITHOUT waiting for their answers) against produce (worker-ingress path and gateway-routed path), subscribe and
// user-issued topic creation, checked against the INDEPENDENT PermissionModel (support/PermissionModel.h).
//
// Invariants: (1) no OK without the model allowing it at SOME instant of [request sent, answer seen] - the changes are
// applied in log order, so the instants are the prefix states [answered-before-send, sent-before-answer]; (2) every
// change that (per the model) takes READ from a live subscription ends it with NotAuthorized / SessionRevoked within
// 1 s of the change's answer (also when the revoke RACED the Subscribe), with no Record at or above the log end seen at the end; (3) every request is answered within 5 s; (4) convergence: with all
// changes answered, a probe of every session x topic answers exactly what the model says.
// RUNE_AUTH_STRESS_SEEDS / RUNE_AUTH_STRESS_STEPS override the 64 seeds x 300 steps.  A failure prints the seed and the
// step list.
#include "support/AuthRig.h"
#include "support/PermissionModel.h"

#include <rune/protocol/RecordBatch.h>
#include <rune/security/SecurityReplayer.h>
#include <rune/storage/Log.h>

#include <algorithm>
#include <cstdlib>
#include <random>
#include <sstream>

using namespace rune::node;
using namespace rune::node::test;
namespace pc = rune::protocol::client;
namespace pm = rune::node::test::pmodel;
using ST = rune::protocol::SecurityResourceType;
using SO = rune::protocol::SecurityOperation;
using SP = rune::protocol::SecurityPatternKind;
using Clock = std::chrono::steady_clock;
using namespace std::chrono_literals;

namespace {

/// Invariant 4 (digest): the broker's SecurityView digest equals the digest of `__security` replayed from offset 0 by the
/// shared verifying SecurityReplayer (the library every client folds the log through).  Empty string = equal.
std::string digestConvergence(BrokerNode& node) {
    struct Replay { uint64_t digest = 0; int64_t applied = -1; std::string error; };
    const Replay replay = node.onAdminThreadForTest([&node]() -> Replay {
        Replay out;
        rune::storage::Log* log = node.securityLogForTest();
        if (log == nullptr) { out.error = "no __security log"; return out; }
        rune::security::SecurityReplayer r;
        int64_t next = 0;
        const int64_t leo = log->logEndOffset();
        while (next < leo) {
            const std::vector<uint8_t> data = log->read(next, 1 << 22);
            if (data.empty()) break;
            std::size_t pos = 0;
            const int64_t before = next;
            while (pos < data.size() && out.error.empty()) {
                rune::protocol::RecordBatchReader reader(data.data() + pos, data.size() - pos);
                if (!reader.ensureValid()) break;
                const auto tl = static_cast<std::size_t>(reader.totalLength());
                reader.forEachRecord([&](std::string_view, std::string_view val, int64_t off, int64_t) {
                    if (off < next || !out.error.empty()) return;
                    const auto fed = r.feed(off, std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(val.data()), val.size()));
                    if (!fed) { out.error = rune::security::describe(fed.error()); return; }
                    next = off + 1;
                });
                if (tl == 0) break;
                pos += tl;
            }
            if (!out.error.empty() || next == before) break;
        }
        out.digest = r.state().digest();
        out.applied = r.state().appliedOffset();
        return out;
    });
    if (!replay.error.empty()) return "the __security log does not replay: " + replay.error;
    for (int i = 0; i < 400; ++i) {   // the view catches up to the log end within a moment
        const auto v = node.securityViewForTest();
        if (v.appliedOffset == replay.applied) {
            if (v.halted) return "the SecurityView is halted";
            return v.digest == replay.digest ? std::string{} : "SecurityView digest " + std::to_string(v.digest) +
                   " != replayed digest " + std::to_string(replay.digest) + " at offset " + std::to_string(replay.applied);
        }
        std::this_thread::sleep_for(10ms);
    }
    return "the SecurityView never reached the log end (replay applied " + std::to_string(replay.applied) + ")";
}

int envInt(const char* name, int dflt) {
    const char* v = std::getenv(name);
    return v && *v ? std::atoi(v) : dflt;
}

/// What the invariants actually checked (non-vacuity, review I6): asserted to be nonzero over a full run.
struct Tally {
    uint64_t shardOk = 0, routedOk = 0, subOk = 0, createOk = 0;          // successes checked by invariant 1
    uint64_t mustEndMet = 0, mustEndRacing = 0;                           // invariant 2: ends required and met in 1 s (racing: the revoke raced the Subscribe)
    uint64_t recordsChecked = 0, endedSubsChecked = 0;                    // "no Records after the end"
    uint64_t probes = 0, shardDeniedProbes = 0, routedDeniedProbes = 0, shardOkProbes = 0, routedOkProbes = 0;
    uint64_t sessionsProbed = 0, disabledLoginsRefused = 0, epochsChecked = 0;
    void add(const Tally& o) {
        shardOk += o.shardOk; routedOk += o.routedOk; subOk += o.subOk; createOk += o.createOk; mustEndMet += o.mustEndMet;
        mustEndRacing += o.mustEndRacing; recordsChecked += o.recordsChecked; endedSubsChecked += o.endedSubsChecked;
        probes += o.probes; shardDeniedProbes += o.shardDeniedProbes; routedDeniedProbes += o.routedDeniedProbes;
        shardOkProbes += o.shardOkProbes; routedOkProbes += o.routedOkProbes; sessionsProbed += o.sessionsProbed;
        disabledLoginsRefused += o.disabledLoginsRefused; epochsChecked += o.epochsChecked;
    }
    [[nodiscard]] std::string str() const {
        std::ostringstream o;
        o << "shardOk=" << shardOk << " routedOk=" << routedOk << " subOk=" << subOk << " createOk=" << createOk
          << " mustEndMet=" << mustEndMet << " (racing " << mustEndRacing << ") endedSubs=" << endedSubsChecked
          << " records=" << recordsChecked << " probes=" << probes << " (shard denied " << shardDeniedProbes << ", routed denied "
          << routedDeniedProbes << ", shard ok " << shardOkProbes << ", routed ok " << routedOkProbes << ") sessions=" << sessionsProbed
          << " disabledLoginsRefused=" << disabledLoginsRefused << " epochs=" << epochsChecked;
        return o.str();
    }
};

struct Sess {
    int         user = 0;
    ShardTestClient* c = nullptr;
    AuthWireClient*  w = nullptr;
    int64_t     sid = 0;
    bool        alive = false;
};

struct Sub {
    enum class St { Pending, Live, MustEnd, Ended } st = St::Pending;
    int         sess = 0;
    std::string topic;
    int32_t     topicId = 0;
    int64_t     corr = 0, subId = 0;
    std::size_t estSent = 0;     // changes sent when the SubscribeResponse OK was seen
    Clock::time_point deadline;  // MustEnd: the error is due (1 s after the answer of the change that took READ)
    Clock::time_point liveAt;    // when its SubscribeResponse OK was seen
    bool        racing = false;  // MustEnd found by the revoke-raced-the-Subscribe rule
    int         endCode = 0;
    int64_t     bound = 0;       // the partition's log end when the end was seen: a Record at or above it is a violation
    std::size_t sentSteps = 0;
    std::size_t lo_ = 0;
    Clock::time_point sentAt_;
};

struct Req {
    enum class Kind { Produce, Create } kind = Kind::Produce;
    int         sess = 0;
    std::string topic;
    int32_t     topicId = 0;
    bool        routed = false;
    int64_t     corr = 0;
    std::size_t lo = 0;          // changes answered (prefix) when sent
    Clock::time_point sentAt;
    std::size_t step = 0;
};

struct Pending { std::size_t idx; int64_t corr; pm::Change c; std::size_t lo; bool answered = false; int code = 0; Clock::time_point answeredAt; };

class Seed {
public:
    Seed(AuthRig& rig, std::vector<AuthWireClient*> uw, int seed, int steps, Tally& tally)
        : rig_(rig), uw_(std::move(uw)), seed_(seed), steps_(steps), tally_(tally), rng_(static_cast<uint64_t>(seed) * 0x9E3779B97F4A7C15ull + 1) {}

    std::string run() {
        setup_();
        for (std::size_t s = 0; s < static_cast<std::size_t>(steps_) && fail_.empty(); ++s) {
            step_ = s;
            pump_(2ms);
            reconnect_();
            doStep_();
            check_();
        }
        if (fail_.empty()) finish_();
        teardown_();
        if (fail_.empty()) return {};
        std::ostringstream o;
        o << "AUTH STRESS FAILURE seed=" << seed_ << ": " << fail_ << "\nreplay: RUNE_AUTH_STRESS_FIRST_SEED=" << seed_
          << " RUNE_AUTH_STRESS_SEEDS=1 rune_auth_stress_tests --gtest_filter='AuthStress*'\nsteps (" << log_.size() << "):\n";
        for (const std::string& l : log_) o << "  " << l << "\n";
        return o.str();
    }

private:
    // ── names ──
    std::string user_(int u) const { return "s" + std::to_string(seed_) + "u" + std::to_string(u); }
    std::string role_(int r) const { return "s" + std::to_string(seed_) + "r" + std::to_string(r); }
    std::string P() const { return "s" + std::to_string(seed_) + "."; }
    std::vector<std::string> topicNames_() const {
        std::vector<std::string> v;
        for (const char* n : {"a.1", "a.2", "a.3", "b.1", "b.2", "n.1", "n.2"}) v.push_back(P() + n);
        return v;
    }
    std::vector<std::string> prefixes_() const { return {P() + "a.", P() + "b.", P() + "n.", P() + "a.1", P()}; }

    void note_(const std::string& s) { log_.push_back("#" + std::to_string(step_) + " " + s); }
    void failWith_(const std::string& s) { if (fail_.empty()) fail_ = "step #" + std::to_string(step_) + ": " + s; }

    // ── setup / teardown ──
    void setup_() {
        auto& adm = rig_.broker().admin();
        const std::string ch = rig_.ch();
        const int64_t as = rig_.broker().adminSession();
        for (int r = 0; r < 2; ++r) { model_.addRole(role_(r)); if (adm.createRole(ch, as, role_(r)).errorCode) failWith_("create role"); }
        for (int u = 0; u < 2; ++u) { model_.addUser(user_(u)); if (adm.createUser(ch, as, user_(u), AuthRig::kPw).errorCode) failWith_("create user"); }
        states_.push_back(model_);
        for (const std::string& t : topicNames_()) exists_[t] = -1;
        for (const std::string& t : {topicNames_()[0], topicNames_()[1], topicNames_()[3], topicNames_()[5]}) builderCreate_(t);
        // 2 users x 3 sessions, one wire client (own Aeron driver, own egress image) per session
        for (std::size_t i = 0; i < uw_.size(); ++i) { sess_.push_back({static_cast<int>(i / 3), &uw_[i]->raw(), uw_[i], 0, false}); login_(i); }
    }
    std::vector<ShardTestClient*> clients_() {
        std::vector<ShardTestClient*> v{&rig_.c(), &rig_.broker().admin().raw()};
        for (AuthWireClient* w : uw_) v.push_back(&w->raw());
        return v;
    }
    void login_(std::size_t i) {
        Sess& s = sess_[i];
        // a login the per-source throttle refuses (retry_after hint, spec §5: a refused disabled-user attempt arms it for
        // this host) is retried like the real client does - bounded
        std::string lastLogin;
        for (int attempt = 0; attempt < 30; ++attempt) {
            s.sid = s.w->connect(rig_.ch());
            if (s.sid <= 0) { failWith_("connect"); return; }
            const auto l = s.w->login(rig_.ch(), s.sid, user_(s.user), {AuthRig::kPw, AuthRig::kPw});
            if (l.ok()) { s.alive = true; return; }
            lastLogin = std::string("answered=") + (l.answered() ? "yes" : "no") + " code=" + std::to_string(l.errorCode()) + " first=" + (l.first ? "yes" : "no");
            std::this_thread::sleep_for(200ms);
        }
        failWith_("login " + user_(s.user) + " kept failing (" + lastLogin + ")");
    }
    void builderCreate_(const std::string& name) {
        const int32_t id = rig_.defineTopic(rig_.builder, name);
        if (id < 0) { failWith_("builder create " + name); return; }
        exists_[name] = id;
        rig_.awaitListed(id);
    }
    void teardown_() {
        for (Sess& s : sess_) if (s.alive) { s.c->disconnect(rig_.ch(), AuthRig::stream(), s.sid); s.alive = false; }
    }

    // ── pumping / observation ──
    void pump_(std::chrono::milliseconds d) {
        const auto end = Clock::now() + d;
        do { for (auto* c : clients_()) c->pollFor(0ms, [] { return false; }); } while (Clock::now() < end);
        observe_();
    }
    std::size_t answeredPrefix_() const {   // changes are applied in order: the highest answered index bounds the applied prefix
        std::size_t p = 0;
        for (const Pending& c : changes_) if (c.answered) p = std::max(p, c.idx + 1);
        return p;
    }
    bool allowedSomewhere_(std::size_t lo, std::size_t hi, int user, pm::Op op, const std::string& topic) const {
        for (std::size_t k = lo; k <= hi && k < states_.size(); ++k)
            if (states_[k].allowed(user_(user), op, topic)) return true;
        return false;
    }
    void observe_() {
        const std::size_t hi = states_.size() - 1;
        const auto now = Clock::now();
        // security DDL answers
        for (Pending& c : changes_) {
            if (c.answered) continue;
            auto& adm = rig_.broker().admin().raw();
            const auto a = adm.awaitAnyDdlAnswer(c.corr, Clock::now() + 1ms);
            if (!a) { if (now - sentAt_[c.idx] > 5s) { std::string un; for (const Pending& q : changes_) if (!q.answered) un += " #" + std::to_string(q.idx) + "(" + q.c.str() + ")"; failWith_("unanswered security DDL #" + std::to_string(c.idx) + ": " + c.c.str() + "; all unanswered:" + un + "; gate{" + gateStr_() + "}; answered changes: " + std::to_string(std::count_if(changes_.begin(), changes_.end(), [](const Pending& q) { return q.answered; })) + "/" + std::to_string(changes_.size())); } continue; }
            if (a->responseTemplate == static_cast<uint16_t>(pc::TemplateId::DdlResponse)) {
                const auto d = adm.awaitDdl(c.corr, Clock::now() + 1s);
                c.code = d ? d->errorCode : 0xFFFF;
            } else c.code = a->code;
            c.answered = true;
            c.answeredAt = now;
            const bool expectRefusal = refusal_[c.idx] != pm::Refusal::None;
            if ((c.code != 0) != expectRefusal)
                failWith_("DDL '" + c.c.str() + "' answered code " + std::to_string(c.code) + " but the model predicts " + (expectRefusal ? "a refusal" : "OK"));
            // invariant 2: subscriptions that existed before this change was sent and lose READ in the prefix state
            for (Sub& s : subs_) {
                if (s.st != Sub::St::Live || s.estSent > c.idx) continue;
                if (!states_[c.idx + 1].allowed(user_(sess_[static_cast<std::size_t>(s.sess)].user), pm::Op::Read, s.topic)) {
                    s.st = Sub::St::MustEnd; s.deadline = now + 1s; s.racing = false;
                }
            }
        }
        // the contiguous answered prefix: changes are applied in log order, so once changes 0..k-1 are ALL answered the
        // broker has applied at least states_[k]
        std::size_t contig = 0;
        while (contig < changes_.size() && changes_[contig].answered) ++contig;
        // requests
        for (std::size_t i = 0; i < reqs_.size();) {
            Req& r = reqs_[i];
            Sess& s = sess_[static_cast<std::size_t>(r.sess)];
            int code = kNoAnswer;
            bool got = false;
            if (r.kind == Req::Kind::Produce) {
                if (const auto a = s.c->ackFor(r.corr)) {
                    code = a->errorCode; got = true;
                    if (code == 0) maxAcked_[r.topicId] = std::max<int64_t>(maxAcked_.count(r.topicId) ? maxAcked_[r.topicId] : -1, a->baseOffset);
                }
            } else if (const auto a = s.c->awaitDefine(r.corr, Clock::now() + 1ms)) { code = a->errorCode; got = true; if (code == 0) exists_[r.topic] = a->topicId; }
            if (!got && revokedFrame_(s)) { got = true; code = 46; if (r.kind == Req::Kind::Create) unknown_.insert(r.topic); }   // the create may or may not have happened   // "SessionRevoked on every in-flight request" (spec §10)
            if (!got) {
                if (now - r.sentAt > 5s) { failWith_("unanswered request (step " + std::to_string(r.step) + ")"); return; }
                ++i; continue;
            }
            if (code == 0 && !allowedSomewhere_(r.lo, hi, s.user, r.kind == Req::Kind::Produce ? pm::Op::Write : pm::Op::Create, r.topic))
                failWith_(std::string("UNAUTHORIZED SUCCESS: ") + (r.kind == Req::Kind::Produce ? "produce" : "create") + " on " + r.topic + " by " +
                      user_(s.user) + " sent at step " + std::to_string(r.step) + ", states [" + std::to_string(r.lo) + "," + std::to_string(hi) + "] never allow it");
            if (code == 0) {
                if (r.kind == Req::Kind::Create) ++tally_.createOk;
                else ++(r.routed ? tally_.routedOk : tally_.shardOk);
            }
            reqs_.erase(reqs_.begin() + static_cast<std::ptrdiff_t>(i));
        }
        // subscribes + subscription ends
        for (Sub& s : subs_) {
            Sess& se = sess_[static_cast<std::size_t>(s.sess)];
            if (s.st == Sub::St::Pending) {
                if (const auto code = se.c->awaitSubscribeCode(s.corr, Clock::now() + 1ms)) {
                    if (*code == 0) {
                        if (!allowedSomewhere_(s.lo_, hi, se.user, pm::Op::Read, s.topic))
                            failWith_("UNAUTHORIZED SUCCESS: subscribe on " + s.topic + " by " + user_(se.user) + " sent at step " + std::to_string(s.sentSteps) + " never allowed in states [" + std::to_string(s.lo_) + "," + std::to_string(hi) + "]");
                        s.subId = se.c->subscriptionIdOf(s.corr).value_or(0);
                        s.st = Sub::St::Live; s.estSent = hi; s.liveAt = now;
                        ++tally_.subOk;
                    } else s.st = Sub::St::Ended;
                } else if (revokedFrame_(se)) { s.st = Sub::St::Ended; s.endCode = 46; s.bound = logEndBound_(s); }
                else if (now - s.sentAt_ > 5s) failWith_("unanswered subscribe (step " + std::to_string(s.sentSteps) + ")");
            }
            // invariant 2, the revoke that RACED the Subscribe: it was authorised at some applied prefix <= estSent; once the
            // changes 0..estSent-1 are all answered the broker has applied at least states_[estSent], and if that denies READ
            // some change after the authorisation took it from this live subscription - it must end within 1 s
            if (s.st == Sub::St::Live && s.estSent <= contig &&
                !states_[s.estSent].allowed(user_(se.user), pm::Op::Read, s.topic)) {
                Clock::time_point last = s.liveAt;
                for (std::size_t k = 0; k < s.estSent; ++k) last = std::max(last, changes_[k].answeredAt);
                s.st = Sub::St::MustEnd; s.deadline = last + 1s; s.racing = true;
            }
            if (s.st == Sub::St::Live || s.st == Sub::St::MustEnd) {
                const auto& errs = se.c->errors();
                for (std::size_t e = 0; e < errs.size(); ++e) {
                    if (errs[e].correlationId != s.corr) continue;
                    s.endCode = errs[e].code;
                    if (s.st == Sub::St::MustEnd && s.endCode != 45 && s.endCode != 46)
                        failWith_("subscription on " + s.topic + " ended with code " + std::to_string(s.endCode) + ", expected 45/46");
                    if (s.st == Sub::St::MustEnd) { ++tally_.mustEndMet; tally_.mustEndRacing += s.racing ? 1 : 0; }
                    s.st = Sub::St::Ended; s.bound = logEndBound_(s);
                    break;
                }
                if (s.st == Sub::St::MustEnd && now > s.deadline)
                    failWith_("REVOKE DID NOT END the subscription on " + s.topic + " of " + user_(se.user) + " (1 s after the change's answer)");
            }
        }
    }
    std::string gateStr_() {
        const auto g = rig_.broker().node().securityGateForTest();
        return "held=" + std::to_string(g.held) + " applied=" + std::to_string(g.applied) + " refreshed=" + std::to_string(g.refreshed) +
               " gwEnforced=" + std::to_string(g.gatewayEnforced) + " pendingEnforce=" + std::to_string(g.pendingEnforce) +
               " shipperPending=" + std::to_string(g.shipperPending);
    }
    /// An upper bound of the partition's log end now: the highest acked offset + 1 plus every produce still unanswered
    /// (each appends one record).  A Record of an ended subscription at or above it can only come from a produce sent
    /// after the end - the worker must no longer push it.
    int64_t logEndBound_(const Sub& s) const {
        int64_t b = (maxAcked_.count(s.topicId) ? maxAcked_.at(s.topicId) : -1) + 1;
        for (const Req& r : reqs_) b += (r.kind == Req::Kind::Produce && r.topicId == s.topicId) ? 1 : 0;
        return b;
    }
    static bool revokedFrame_(Sess& s) {
        for (const auto& e : s.c->errors()) if (e.sessionId == s.sid && e.code == 46) return true;
        return false;
    }
    static std::size_t pushes_(Sess& s, int64_t subId) {
        std::size_t n = 0;
        for (const auto& p : s.c->records()) n += p.subscriptionId == subId ? 1 : 0;
        return n;
    }

    // ── steps ──
    void reconnect_() {
        if (changes_.empty() || std::any_of(changes_.begin(), changes_.end(), [](const Pending& c) { return !c.answered; })) return;
        for (std::size_t i = 0; i < sess_.size(); ++i)
            if (!sess_[i].alive && model_.isEnabled(user_(sess_[i].user))) { login_(i); note_("reconnect session " + std::to_string(i)); }
    }
    int pick_(int n) { return static_cast<int>(rng_() % static_cast<uint64_t>(n)); }
    std::size_t aliveSession_() {
        std::vector<std::size_t> a;
        for (std::size_t i = 0; i < sess_.size(); ++i) if (sess_[i].alive) a.push_back(i);
        return a.empty() ? sess_.size() : a[static_cast<std::size_t>(pick_(static_cast<int>(a.size())))];
    }
    std::vector<std::string> existing_() const {
        std::vector<std::string> v;
        for (const auto& [n, id] : exists_) if (id >= 0) v.push_back(n);
        return v;
    }
    pm::GrantSpec randomGrant_() {
        pm::GrantSpec g;
        const int op = pick_(10);
        g.op = op < 4 ? pm::Op::Read : op < 8 ? pm::Op::Write : pm::Op::Create;
        if (g.op == pm::Op::Create && pick_(3) == 0) { g.cluster = true; return g; }
        if (pick_(2) == 0) { g.prefix = true; const auto p = prefixes_(); g.text = p[static_cast<std::size_t>(pick_(static_cast<int>(p.size())))]; }
        else { const auto t = topicNames_(); g.text = t[static_cast<std::size_t>(pick_(static_cast<int>(t.size())))]; }
        return g;
    }
    void sendChange_(pm::Change c) {
        auto& wc = rig_.broker().admin();
        auto& raw = wc.raw();
        pc::SecurityDdl m;
        m.sessionId = rig_.broker().adminSession();
        using K = pm::Change::Kind;
        std::string stmt;
        switch (c.kind) {
        case K::Grant: case K::Revoke:
            m.kind = c.kind == K::Grant ? pc::SecurityOpKind::Grant : pc::SecurityOpKind::Revoke; m.name = c.who;
            m.granteeKind = c.whoIsRole ? 1 : 0;
            m.op = static_cast<uint8_t>(c.grant.op == pm::Op::Read ? SO::Read : c.grant.op == pm::Op::Write ? SO::Write : SO::Create);
            m.resourceType = static_cast<uint8_t>(c.grant.cluster ? ST::Cluster : ST::Topic);
            m.patternKind = static_cast<uint8_t>(c.grant.prefix ? SP::Prefix : SP::Exact);
            // (a `?: ""` here would build a temporary std::string the view dangles from)
            if (!c.grant.cluster) m.pattern = c.grant.text;
            stmt = "GRANT ...;"; break;
        case K::GrantRole: m.kind = pc::SecurityOpKind::GrantRole; m.name = c.who; m.roleName = c.role; stmt = "GRANT ROLE;"; break;
        case K::RevokeRole: m.kind = pc::SecurityOpKind::RevokeRole; m.name = c.who; m.roleName = c.role; stmt = "REVOKE ROLE;"; break;
        case K::Disable: case K::Enable:
            m.kind = pc::SecurityOpKind::AlterUser; m.name = c.who;
            m.change = c.kind == K::Disable ? pc::UserChangeKind::Disable : pc::UserChangeKind::Enable; stmt = "ALTER USER ...;"; break;
        }
        m.correlationId = raw.nextCorrelationId();
        m.ext.clientName = "auth-stress"; m.ext.statement = stmt;
        m.ext.requestId = {0xA57E'5500'0000'0000ull | static_cast<uint64_t>(seed_), ++reqCounter_};
        std::vector<uint8_t> b(m.encodedSize());
        b.resize(m.encode(b.data()));
        if (!raw.send(rig_.ch(), AuthWireClient::kClientStream, b.data(), b.size())) { failWith_("DDL send"); return; }
        const std::size_t idx = changes_.size();
        pm::Model next = states_.back();
        refusal_.push_back(next.apply(c));
        model_ = next;
        states_.push_back(next);
        sentAt_.push_back(Clock::now());
        changes_.push_back(Pending{idx, m.correlationId, c, answeredPrefix_()});
    }
    void doStep_() {
        const int roll = pick_(100);
        using K = pm::Change::Kind;
        if (roll < 36) {   // security change
            pm::Change c;
            const int k = pick_(100);
            c.who = user_(pick_(2));
            if (k < 38) { c.kind = K::Grant; c.grant = randomGrant_(); if (pick_(3) == 0) { c.whoIsRole = true; c.who = role_(pick_(2)); } }
            else if (k < 70) {
                c.kind = K::Revoke;
                if (pick_(3) == 0) { c.whoIsRole = true; c.who = role_(pick_(2)); }
                c.grant = randomGrant_();
                // revoke what is actually there most of the time (a random revoke is mostly a refusal)
                if (pick_(4) != 0) { const auto g = heldGrant_(c.who, c.whoIsRole); if (g) c.grant = *g; }
            }
            else if (k < 78) { c.kind = K::GrantRole; c.role = role_(pick_(2)); }
            else if (k < 88) { c.kind = K::RevokeRole; c.role = role_(pick_(2)); }
            else if (k < 93) c.kind = K::Disable;
            else c.kind = K::Enable;
            note_("DDL " + c.str());
            sendChange_(c);
            return;
        }
        const auto ex = existing_();
        const std::size_t si = aliveSession_();
        if (si == sess_.size() || ex.empty()) { note_("idle"); return; }
        Sess& s = sess_[si];
        if (roll < 66) {   // produce (worker ingress or gateway-routed)
            const std::string t = ex[static_cast<std::size_t>(pick_(static_cast<int>(ex.size())))];
            Req r; r.kind = Req::Kind::Produce; r.sess = static_cast<int>(si); r.topic = t; r.topicId = exists_[t]; r.routed = pick_(2) == 0;
            r.corr = s.c->nextCorrelationId(); r.lo = answeredPrefix_(); r.sentAt = Clock::now(); r.step = step_;
            note_(std::string("produce(") + (r.routed ? "routed" : "shard") + ") " + t + " as " + user_(s.user) + "/" + std::to_string(si));
            const std::string& chan = r.routed ? rig_.ch() : rig_.broker().workerIngressChannel();
            if (!s.c->produce(chan, AuthRig::stream(), s.sid, r.corr, r.topicId, 0, 1, makeBatch(1))) { failWith_("produce send"); return; }
            reqs_.push_back(r);
        } else if (roll < 84) {   // subscribe
            const std::string t = ex[static_cast<std::size_t>(pick_(static_cast<int>(ex.size())))];
            Sub sub; sub.sess = static_cast<int>(si); sub.topic = t; sub.topicId = exists_[t];
            sub.lo_ = answeredPrefix_(); sub.sentAt_ = Clock::now(); sub.sentSteps = step_;
            sub.corr = s.c->sendSubscribe(rig_.ch(), AuthRig::stream(), s.sid, sub.topicId, 0);
            note_("subscribe " + t + " as " + user_(s.user) + "/" + std::to_string(si));
            if (sub.corr == 0) { failWith_("subscribe send"); return; }
            subs_.push_back(sub);
        } else if (roll < 94) {   // CREATE TOPIC by a user
            std::vector<std::string> free;
            for (const std::string& n : topicNames_()) if (exists_[n] < 0 && !creating_.count(n) && !unknown_.count(n)) free.push_back(n);
            if (free.empty()) { note_("idle"); return; }
            const std::string t = free[static_cast<std::size_t>(pick_(static_cast<int>(free.size())))];
            Req r; r.kind = Req::Kind::Create; r.sess = static_cast<int>(si); r.topic = t;
            r.corr = s.c->sendDefineTopic(rig_.ch(), AuthRig::stream(), s.sid, t, 1, authRigCharBlob());
            r.lo = answeredPrefix_(); r.sentAt = Clock::now(); r.step = step_;
            note_("create " + t + " as " + user_(s.user) + "/" + std::to_string(si));
            if (r.corr == 0) { failWith_("create send"); return; }
            reqs_.push_back(r);
            creating_.insert(t);
        } else {   // builder drops a topic that nothing is subscribed to / recreates a missing one
            quiesceCreates_();
            if (!fail_.empty()) return;
            const std::string t = topicNames_()[static_cast<std::size_t>(pick_(7))];
            if (unknown_.count(t)) { note_("idle"); return; }   // a create answered by the session revocation: existence unknown
            if (exists_[t] >= 0) {
                const bool subbed = std::any_of(subs_.begin(), subs_.end(), [&](const Sub& x) { return x.topic == t && x.st != Sub::St::Ended; });
                if (subbed) { note_("idle"); return; }
                note_("builder DROP " + t);
                if (rig_.dropTopicCode(rig_.builder, t) == 0) exists_[t] = -1;
            } else { note_("builder CREATE " + t); builderCreate_(t); }
        }
    }
    void quiesceCreates_() {   // a user create in flight must be settled before the builder touches the same pool
        const auto end = Clock::now() + 6s;
        while (std::any_of(reqs_.begin(), reqs_.end(), [](const Req& r) { return r.kind == Req::Kind::Create; }) && Clock::now() < end && fail_.empty()) pump_(2ms);
        creating_.clear();
        for (const Req& r : reqs_) if (r.kind == Req::Kind::Create) creating_.insert(r.topic);
    }
    std::optional<pm::GrantSpec> heldGrant_(const std::string& who, bool isRole) {
        // the independent model has no accessor for its own sets by design: probe the candidates
        std::vector<pm::GrantSpec> cands;
        for (pm::Op op : {pm::Op::Read, pm::Op::Write, pm::Op::Create}) {
            for (const std::string& t : topicNames_()) { pm::GrantSpec g; g.op = op; g.text = t; cands.push_back(g); }
            for (const std::string& t : prefixes_()) { pm::GrantSpec g; g.op = op; g.prefix = true; g.text = t; cands.push_back(g); }
        }
        { pm::GrantSpec g; g.op = pm::Op::Create; g.cluster = true; cands.push_back(g); }
        std::vector<pm::GrantSpec> held;
        for (const pm::GrantSpec& g : cands) {
            pm::Model m = model_;
            pm::Change c; c.kind = pm::Change::Kind::Revoke; c.who = who; c.whoIsRole = isRole; c.grant = g;
            if (m.apply(c) == pm::Refusal::None) held.push_back(g);
        }
        if (held.empty()) return std::nullopt;
        return held[static_cast<std::size_t>(pick_(static_cast<int>(held.size())))];
    }
    void check_() {
        observe_();
        // a disabled user's sessions are dead the moment the DISABLE is answered
        for (const Pending& c : changes_)
            if (c.answered && c.c.kind == pm::Change::Kind::Disable && c.code == 0)
                for (Sess& s : sess_) if (s.alive && user_(s.user) == c.c.who && !states_.back().isEnabled(c.c.who)) { s.alive = false; }
    }

    // ── end of a seed: drain, then convergence ──
    void finish_() {
        step_ = static_cast<std::size_t>(steps_);
        const auto end = Clock::now() + 8s;
        while ((!reqs_.empty() || std::any_of(changes_.begin(), changes_.end(), [](const Pending& c) { return !c.answered; }) ||
                std::any_of(subs_.begin(), subs_.end(), [](const Sub& s) { return s.st == Sub::St::Pending || s.st == Sub::St::MustEnd; })) &&
               Clock::now() < end && fail_.empty())
            pump_(2ms);
        if (!fail_.empty()) return;
        // sessions of a user disabled at the end are dead; the model decides
        const pm::Model& fin = states_.back();
        for (Sess& s : sess_) if (!fin.isEnabled(user_(s.user))) s.alive = false;
        // subscriptions still live must still be allowed (nothing may survive a revoke) - give the broker its second
        const auto settle = Clock::now() + 1500ms;
        for (;;) {
            pump_(5ms);
            bool bad = false;
            for (const Sub& s : subs_)
                if (s.st == Sub::St::Live && exists_[s.topic] >= 0 && !fin.allowed(user_(sess_[static_cast<std::size_t>(s.sess)].user), pm::Op::Read, s.topic) && sess_[static_cast<std::size_t>(s.sess)].alive) bad = true;
            if (!bad || Clock::now() > settle || !fail_.empty()) {
                if (bad && fail_.empty()) for (const Sub& s : subs_)
                    if (s.st == Sub::St::Live && !fin.allowed(user_(sess_[static_cast<std::size_t>(s.sess)].user), pm::Op::Read, s.topic) && sess_[static_cast<std::size_t>(s.sess)].alive && exists_[s.topic] >= 0)
                        { failWith_("CONVERGENCE: subscription on " + s.topic + " of " + user_(sess_[static_cast<std::size_t>(s.sess)].user) + " survived; the final model denies READ"); break; }
                break;
            }
        }
        if (!fail_.empty()) return;
        // no Records after the end of a subscription: nothing at or above the log end seen when it ended
        for (const Sub& s : subs_) {
            if (s.st != Sub::St::Ended || s.subId == 0 || s.endCode == 0) continue;
            ++tally_.endedSubsChecked;
            for (const auto& p : sess_[static_cast<std::size_t>(s.sess)].c->records()) {
                if (p.subscriptionId != s.subId) continue;
                ++tally_.recordsChecked;
                if (p.baseOffset >= s.bound) {
                    failWith_("Records delivered on " + s.topic + " after its ErrorResponse(" + std::to_string(s.endCode) + "): offset " +
                              std::to_string(p.baseOffset) + " >= the log end " + std::to_string(s.bound) + " at the end (sub of step " +
                              std::to_string(s.sentSteps) + ", user " + user_(sess_[static_cast<std::size_t>(s.sess)].user) + ")");
                    return;
                }
            }
        }
        if (const std::string d = digestConvergence(rig_.broker().node()); !d.empty()) { failWith_("CONVERGENCE: " + d); return; }
        // broker side (review I5): every shard-produce frame the worker admitted was answered, and every enforcing session
        // holds a shipped permission epoch on the worker
        {
            const auto until = Clock::now() + 3s;
            std::size_t inFlight = rig_.broker().node().workerDirectAcksInFlightForTest(0);
            while (inFlight != 0 && Clock::now() < until) { pump_(10ms); inFlight = rig_.broker().node().workerDirectAcksInFlightForTest(0); }
            if (inFlight != 0) { failWith_("BROKER: " + std::to_string(inFlight) + " shard produce frame(s) admitted by the worker were never answered"); return; }
            for (const Sess& se : sess_) {
                if (!se.alive) continue;
                if (rig_.broker().node().workerPermissionEpochForTest(0, se.sid) == 0) { failWith_("BROKER: the worker holds no permission epoch for a live session"); return; }
                ++tally_.epochsChecked;
            }
            if (rig_.broker().node().securityAppliedOffsetOnControlForTest() != rig_.broker().node().securityViewForTest().appliedOffset) {
                failWith_("BROKER: the control thread's applied offset probe disagrees with the view snapshot"); return;
            }
        }
        // a disabled user cannot log in (a fresh login attempt is refused AuthenticationFailed)
        for (int u = 0; u < 2; ++u) {
            if (fin.isEnabled(user_(u)) || seed_ % 16 != 0) continue;   // each refusal arms the per-source login throttle: sampled
            const auto l = uw_[static_cast<std::size_t>(u) * 3]->connectAndLogin(rig_.ch(), user_(u), AuthRig::kPw);
            if (l.ok()) { failWith_("CONVERGENCE: a disabled user " + user_(u) + " logged in"); return; }
            ++tally_.disabledLoginsRefused;
        }
        // probes: EVERY alive session x existing topic x BOTH produce paths (worker ingress, gateway-routed) + Subscribe
        step_ = static_cast<std::size_t>(steps_) + 1;
        for (std::size_t i = 0; i < sess_.size() && fail_.empty(); ++i) {
            Sess& s = sess_[i];
            if (!s.alive) continue;
            ++tally_.sessionsProbed;
            for (const std::string& t : existing_()) {
                const bool wantW = fin.allowed(user_(s.user), pm::Op::Write, t), wantR = fin.allowed(user_(s.user), pm::Op::Read, t);
                for (const bool routed : {false, true}) {
                    int code = kNoAnswer;
                    for (int a = 0; a < 80; ++a) {   // transient answers (NotLeader / UnknownTopic while a topic settles) are retried
                        const int64_t corr = s.c->nextCorrelationId();
                        const std::string& chan = routed ? rig_.ch() : rig_.broker().workerIngressChannel();
                        if (!s.c->produce(chan, AuthRig::stream(), s.sid, corr, exists_[t], 0, 1, makeBatch(1))) break;
                        const auto ack = s.c->awaitProduceResponseUntil(corr, Clock::now() + 5s);
                        code = ack ? ack->errorCode : kNoAnswer;
                        if (code == 0 || code == 45 || code == kNoAnswer) break;
                        std::this_thread::sleep_for(25ms);
                    }
                    ++tally_.probes;
                    if (wantW != (code == 0) || (!wantW && code != 45))
                        failWith_(std::string("CONVERGENCE: ") + (routed ? "routed" : "shard-path") + " produce on " + t + " as " + user_(s.user) + "/" + std::to_string(i) +
                                  " answered " + std::to_string(code) + ", model says " + (wantW ? "allowed" : "denied"));
                    (wantW ? (routed ? tally_.routedOkProbes : tally_.shardOkProbes) : (routed ? tally_.routedDeniedProbes : tally_.shardDeniedProbes)) += 1;
                    if (!fail_.empty()) return;
                }
                int sc = kNoAnswer;
                for (int a = 0; a < 80; ++a) {
                    sc = s.c->awaitSubscribeCode(s.c->sendSubscribe(rig_.ch(), AuthRig::stream(), s.sid, exists_[t], 0), Clock::now() + 5s).value_or(kNoAnswer);
                    if (sc != static_cast<int>(pc::ErrorCode::NotLeader)) break;
                    std::this_thread::sleep_for(25ms);
                }
                ++tally_.probes;
                if (wantR != (sc == 0) || (!wantR && sc != 45))
                    failWith_("CONVERGENCE: subscribe on " + t + " as " + user_(s.user) + "/" + std::to_string(i) + " answered " + std::to_string(sc) +
                              ", model says " + (wantR ? "allowed" : "denied"));
                if (!fail_.empty()) return;
            }
        }
    }

    AuthRig& rig_;
    std::vector<AuthWireClient*> uw_;
    int seed_, steps_;
    Tally& tally_;
    std::map<int32_t, int64_t> maxAcked_;   // topic id -> highest acked base offset
    std::mt19937_64 rng_;
    std::size_t step_ = 0;
    std::string fail_;
    std::vector<std::string> log_;
    pm::Model model_;
    std::vector<pm::Model> states_;
    std::vector<pm::Refusal> refusal_;
    std::vector<Clock::time_point> sentAt_;
    std::vector<Pending> changes_;
    std::vector<Sess> sess_;
    std::vector<Req> reqs_;
    std::vector<Sub> subs_;
    std::map<std::string, int32_t> exists_;
    std::set<std::string> creating_, unknown_;
    uint64_t reqCounter_ = 0;
};

}  // namespace

TEST(AuthStressRegression, SeededRandomGrantRevokeTrafficHoldsEveryInvariant) {
    const int seeds = envInt("RUNE_AUTH_STRESS_SEEDS", 64);
    const int steps = envInt("RUNE_AUTH_STRESS_STEPS", 300);
    const int first = envInt("RUNE_AUTH_STRESS_FIRST_SEED", 0);   // replay: FIRST_SEED=N SEEDS=1
    AuthBroker::Options o;
    o.shard = true;
    AuthRig rig("stress", o);
    std::vector<AuthWireClient*> wires;
    for (int i = 0; i < 6; ++i) { wires.push_back(&rig.broker().wire()); wires.back()->cacheSaltedPasswords(true); }
    Tally tally;
    const auto t0 = Clock::now();
    for (int seed = first; seed < first + seeds; ++seed) {
        Tally t;
        Seed s(rig, wires, seed, steps, t);
        const std::string failure = s.run();
        ASSERT_TRUE(failure.empty()) << failure;
        tally.add(t);
    }
    std::cout << "[auth-stress] " << seeds << " seeds x " << steps << " steps in "
              << std::chrono::duration_cast<std::chrono::seconds>(Clock::now() - t0).count() << " s\n[auth-stress] " << tally.str() << "\n";
    if (seeds >= 64 && steps >= 300) {   // non-vacuity floors of a full run (review I6)
        EXPECT_GE(tally.shardOk, 100u);
        EXPECT_GE(tally.routedOk, 100u);
        EXPECT_GE(tally.subOk, 100u);
        EXPECT_GE(tally.createOk, 1u);
        EXPECT_GE(tally.mustEndMet, 20u);
        EXPECT_GE(tally.recordsChecked, 1u);
        EXPECT_GE(tally.shardDeniedProbes, 50u);
        EXPECT_GE(tally.routedDeniedProbes, 50u);
        EXPECT_GE(tally.shardOkProbes, 50u);
        EXPECT_GE(tally.routedOkProbes, 50u);
        EXPECT_GE(tally.disabledLoginsRefused, 1u);
        EXPECT_GE(tally.epochsChecked, 100u);
    }
}
