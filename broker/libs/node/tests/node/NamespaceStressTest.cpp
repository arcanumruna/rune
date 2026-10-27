// NamespaceStressTest.cpp - Namespaces Task 13 (spec section 8): a seeded, model-checked stress of the namespace
// walls on ONE real authenticating broker (default config).  Per seed: a random mix of CREATE NAMESPACE / topic /
// view / user DDL (root) and cross-namespace probes (namespace users), checked against an in-test model.
//
// Invariants: (1) a caller never sees another namespace's object (name, Metadata listing, view row); (2) a hidden
// object's answer is byte-equal to the answer for a never-created twin of the same shape; (3) a broker restart
// changes nothing (namespaces, topics, security digest); (4) namespace ids are strictly increasing.
//
// The plan caps a seed at 2000 steps; 300 keeps the 20 seeds fast (RUNE_NS_STRESS_STEPS overrides, hard cap 2000).
// Every failure message carries seed=<n> step=<k> op=<...> (SCOPED_TRACE).  No sleeps; every wait has a deadline.
//
// TODO(Phase 2, broker/docs/superpowers/plans/ - namespace delete): once a namespace can be deleted, assert nothing
// of a deleted nsId survives (topics, views, users, disk dirs) after every step and after a restart.

#include "support/AuthRig.h"

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <map>
#include <random>
#include <set>
#include <string>
#include <vector>

namespace rune::node::test {
namespace {

namespace pc = rune::protocol::client;
using Clk = std::chrono::steady_clock;

constexpr int32_t kMissingId = 98'765;
constexpr int kRestartEvery = 50;
constexpr std::size_t kMaxNamespaces = 8;

struct Answer {
    int code = kNoAnswer;
    std::vector<uint8_t> body;   ///< the answer frame without the message header
    int64_t corr = 0;
    std::vector<int32_t> ids;    ///< ids the request carried (blanked on compare)
    std::string asked;           ///< the name the request carried (blanked on compare)
};

void blank(std::vector<uint8_t>& f, const void* v, std::size_t n) {
    uint8_t le[8];
    std::memcpy(le, v, n);
    for (std::size_t i = 0; i + n <= f.size(); ++i)
        if (std::memcmp(f.data() + i, le, n) == 0) std::memset(f.data() + i, 0, n);
}
/// Both answers' correlation ids and sent ids blanked, the asked names replaced by '?' (same length by construction).
std::vector<uint8_t> normalized(const Answer& a, const Answer& other) {
    std::vector<uint8_t> f = a.body;
    for (const Answer* x : {&a, &other}) {
        blank(f, &x->corr, 8);
        for (const int32_t id : x->ids) blank(f, &id, 4);
        if (!x->asked.empty())
            for (std::size_t i = 0; i + x->asked.size() <= f.size(); ++i)
                if (std::memcmp(f.data() + i, x->asked.data(), x->asked.size()) == 0)
                    std::memset(f.data() + i, '?', x->asked.size());
    }
    return f;
}
std::vector<uint8_t> strip(std::vector<uint8_t> f) {
    constexpr std::size_t h = pc::MessageHeader::ENCODED_SIZE;
    if (f.size() <= h) return {};
    f.erase(f.begin(), f.begin() + h);
    return f;
}
std::string hex(const std::vector<uint8_t>& v) {
    static const char* d = "0123456789abcdef";
    std::string s;
    for (const uint8_t b : v) { s += d[b >> 4]; s += d[b & 15]; }
    return s;
}
std::vector<AuthGrant> dataGrants() {
    using namespace grants;
    std::vector<AuthGrant> g;
    for (const SO op : {SO::Read, SO::Write, SO::Describe, SO::Create, SO::Alter, SO::Drop})
        g.push_back(prefix(op, ST::Topic, ""));
    for (const SO op : {SO::Read, SO::Create, SO::Alter, SO::Drop}) g.push_back(prefix(op, ST::View, ""));
    g.push_back(prefix(SO::Read, ST::Group, ""));
    g.push_back(prefix(SO::Write, ST::TransactionalId, ""));
    g.push_back(prefix(SO::Describe, ST::TransactionalId, ""));
    return g;
}

struct NsModel {
    uint32_t nsId = 0;
    std::string name;
    std::map<std::string, int32_t> topics;   ///< local name -> topic id
    std::map<std::string, int32_t> views;    ///< local name -> view topic id
    std::set<std::string> viewedTopics;      ///< topics some view reads (not droppable)
    std::vector<std::string> users;          ///< full user names
};

struct Session {
    int64_t sid = 0;
    Clk::time_point last{};
};

struct Fingerprint {
    std::vector<std::string> namespaces;
    std::vector<int32_t> topics;
    uint64_t digest = 0;
    bool operator==(const Fingerprint&) const = default;
};

}  // namespace

class NamespaceStressRegression : public ::testing::TestWithParam<uint32_t> {
protected:
    NamespaceStressRegression() : seed_(GetParam()), r_("nsst" + std::to_string(GetParam())), rng_(GetParam()) {}

    static int stepsFromEnv() {
        int steps = 300;
        if (const char* e = std::getenv("RUNE_NS_STRESS_STEPS")) steps = std::atoi(e);
        return std::clamp(steps, 1, 2000);
    }
    static std::string full(const NsModel& n, const std::string& local) {
        return n.nsId == 0 ? local : n.name + "/" + local;
    }
    std::string userName(const NsModel& n, const std::string& k) const { return n.nsId == 0 ? "u" + k : n.name + "/u" + k; }
    uint32_t rnd(uint32_t n) { return static_cast<uint32_t>(rng_() % n); }

    // ── sessions ────────────────────────────────────────────────────────────────────────────────────────────
    /// Sessions idle for > 5 s are reaped by the broker (no keepalives): a session unused for 2 s is logged in again.
    int64_t fresh(Session& s, const std::string& user, const std::string& pw) {
        const auto now = Clk::now();
        if (s.sid == 0 || now - s.last > std::chrono::seconds(2)) s.sid = r_.login(user, pw, pc::kClientVersionNamespaces);
        s.last = now;
        return s.sid;
    }
    int64_t rootSid() { return fresh(root_, "admin", AuthBroker::kAdminPassword); }
    int64_t userSid(const std::string& user) { return fresh(sess_[user], user, AuthRig::kPw); }
    void relogin(const std::string& user) {
        if (user.empty()) root_.sid = 0; else sess_[user].sid = 0;
    }

    // ── root DDL ────────────────────────────────────────────────────────────────────────────────────────────
    uint32_t createNamespace(const std::string& name, int& code) {
        pc::NamespaceDdlRequest m;
        m.sessionId = rootSid();
        m.correlationId = r_.c().nextCorrelationId();
        m.ext.clientName = "ns-stress";
        m.ext.requestId = rune::protocol::RequestId{0x57E55, ++rid_};
        m.kind = pc::NamespaceDdlKind::Create;
        m.name = name;
        std::vector<uint8_t> buf(m.encodedSize());
        buf.resize(m.encode(buf.data()));
        code = kNoAnswer;
        if (!r_.c().send(r_.ch(), AuthRig::stream(), buf.data(), buf.size())) return 0;
        const auto any = r_.c().awaitAnyDdlAnswer(m.correlationId, AuthRig::dl(15));
        if (!any) return 0;
        code = any->code;
        const auto a = r_.c().awaitDdl(m.correlationId, AuthRig::dl(1));
        return a && code == 0 ? static_cast<uint32_t>(a->objectId) : 0;
    }
    bool createUser(const std::string& user) {
        const int64_t sid = rootSid();
        if (r_.wire().createUser(r_.ch(), sid, user, AuthRig::kPw).errorCode != 0) return false;
        for (const AuthGrant& g : dataGrants())
            if (r_.wire().grant(r_.ch(), sid, user, false, g.op, g.type, g.kind, g.pattern).errorCode != 0) return false;
        return true;
    }
    /// Root's Metadata listing until it holds every id of `want` and none of `gone` (bounded).
    bool awaitIds(const std::vector<int32_t>& want, const std::vector<int32_t>& gone = {}) {
        for (int i = 0; i < 400; ++i) {
            const auto ids = r_.metadataTopics(rootSid());
            const auto has = [&](int32_t id) { return std::find(ids.begin(), ids.end(), id) != ids.end(); };
            if (std::all_of(want.begin(), want.end(), has) &&
                std::none_of(gone.begin(), gone.end(), has)) return true;
            r_.c().pollFor(std::chrono::milliseconds(25), [] { return false; });
        }
        return false;
    }

    // ── probes ──────────────────────────────────────────────────────────────────────────────────────────────
    Answer schemaOf(int64_t sid, const std::string& name) {
        Answer a;
        a.asked = name;
        a.corr = r_.c().sendGetTopicSchema(r_.ch(), AuthRig::stream(), sid, name);
        const auto ans = r_.c().awaitSchema(a.corr, AuthRig::dl());
        a.code = ans ? ans->errorCode : kNoAnswer;
        if (const auto raw = r_.c().awaitRaw(a.corr, AuthRig::dl(2))) a.body = strip(*raw);
        return a;
    }
    Answer viewRowOf(int64_t sid, int32_t id) {
        Answer a;
        a.ids = {id};
        a.corr = r_.c().sendGetViewRow(r_.ch(), AuthRig::stream(), sid, id, 1);
        const auto ans = r_.c().awaitViewRowOrError(a.corr, AuthRig::dl());
        if (const auto raw = r_.c().awaitRaw(a.corr, AuthRig::dl(2))) a.body = strip(*raw);
        if (!ans) return a;
        const int status = ans->isError ? ans->errorCode : ans->status;
        a.code = !ans->isError && status >= 0 && status <= 2 ? 0 : status;
        return a;
    }
    /// One probe as `user`, re-logging in once when the session was reaped.
    template <class F>
    Answer withRetry(const std::string& user, F&& f) {
        Answer a = f(userSid(user));
        if (a.code == kNoAnswer || a.code == 0xFFFF) {
            relogin(user);
            a = f(userSid(user));
        }
        return a;
    }

    // ── fingerprint ─────────────────────────────────────────────────────────────────────────────────────────
    Fingerprint fingerprint() {
        Fingerprint f;
        pc::ListNamespaces m;
        m.sessionId = rootSid();
        m.correlationId = r_.c().nextCorrelationId();
        m.afterNsId = 0;
        m.max = 100;
        std::vector<uint8_t> buf(m.encodedSize());
        buf.resize(m.encode(buf.data()));
        EXPECT_TRUE(r_.c().send(r_.ch(), AuthRig::stream(), buf.data(), buf.size()));
        const auto raw = r_.rawAnswer(m.correlationId);
        const auto l = raw.empty() ? std::nullopt : pc::ListNamespacesResponse::decode(raw.data(), raw.size());
        EXPECT_TRUE(l.has_value()) << "ListNamespaces answered nothing decodable";
        if (l) for (const auto& i : l->namespaces) f.namespaces.push_back(std::to_string(i.nsId) + ":" + i.name);
        std::sort(f.namespaces.begin(), f.namespaces.end());
        f.topics = r_.metadataTopics(rootSid());
        std::sort(f.topics.begin(), f.topics.end());
        f.digest = r_.broker().node().securityViewForTest().digest;
        return f;
    }
    std::string show(const Fingerprint& f) {
        std::string s = "ns[";
        for (const auto& n : f.namespaces) s += n + " ";
        s += "] topics[";
        for (const int32_t t : f.topics) s += std::to_string(t) + " ";
        return s + "] digest=" + std::to_string(f.digest);
    }

    // ── steps ───────────────────────────────────────────────────────────────────────────────────────────────
    void addUser(NsModel& n, const std::string& k, const std::string& what) {
        const std::string u = userName(n, k);
        ASSERT_TRUE(createUser(u)) << what << ": create user " << u;
        n.users.push_back(u);
    }
    void stepCreateNamespace(int step) {
        const std::string name = "n" + std::to_string(step);
        int code = 0;
        const uint32_t id = createNamespace(name, code);
        ASSERT_EQ(code, 0) << "CREATE NAMESPACE " << name;
        ASSERT_EQ(id, lastNsId_ + 1) << "namespace ids must be strictly increasing (invariant 4)";
        lastNsId_ = id;
        NsModel n;
        n.nsId = id;
        n.name = name;
        ns_.push_back(n);
        addUser(ns_.back(), "0", "new namespace");
    }
    void stepCreateTopic(int step) {
        NsModel& n = ns_[rnd(static_cast<uint32_t>(ns_.size()))];
        const std::string local = "t" + std::to_string(step);
        const int32_t id = r_.defineTopic(rootSid(), full(n, local));
        ASSERT_GE(id, 0) << "create topic " << full(n, local);
        n.topics[local] = id;
        ASSERT_TRUE(awaitIds({id})) << "topic " << full(n, local) << " (id " << id << ") never listed for root";
    }
    void stepDropTopic() {
        std::vector<std::pair<NsModel*, std::string>> c;
        for (NsModel& n : ns_)
            for (const auto& [l, id] : n.topics) if (!n.viewedTopics.count(l)) c.emplace_back(&n, l);
        if (c.empty()) return stepProbe(0);
        const auto& [n, l] = c[rnd(static_cast<uint32_t>(c.size()))];
        const int32_t id = n->topics[l];
        ASSERT_EQ(r_.dropTopicCode(rootSid(), full(*n, l)), 0) << "drop topic " << full(*n, l);
        n->topics.erase(l);
        ASSERT_TRUE(awaitIds({}, {id})) << "dropped topic id " << id << " still listed";
    }
    void stepCreateView(int step) {
        std::vector<std::pair<NsModel*, std::string>> c;
        for (NsModel& n : ns_)
            for (const auto& [l, id] : n.topics) c.emplace_back(&n, l);
        if (c.empty()) return stepProbe(step);
        const auto& [n, l] = c[rnd(static_cast<uint32_t>(c.size()))];
        const std::string local = "v" + std::to_string(step);
        const int32_t id = r_.createView(rootSid(), full(*n, local), n->topics[l]);
        ASSERT_GE(id, 0) << "create view " << full(*n, local) << " on " << l;
        n->views[local] = id;
        n->viewedTopics.insert(l);
    }
    void stepCreateUser(int step) {
        addUser(ns_[rnd(static_cast<uint32_t>(ns_.size()))], std::to_string(step), "create user");
    }

    void stepProbe(int step) {
        // Caller: a random user of a random namespace (A).  Object: an existing topic / view of a random namespace
        // (B), or with 20% a name never created.
        const NsModel& a = ns_[rnd(static_cast<uint32_t>(ns_.size()))];
        const std::string& user = a.users[rnd(static_cast<uint32_t>(a.users.size()))];
        const NsModel& b = ns_[rnd(static_cast<uint32_t>(ns_.size()))];
        struct Obj { std::string local; int32_t id; bool view; bool exists; };
        std::vector<Obj> objs;
        for (const auto& [l, id] : b.topics) objs.push_back({l, id, false, true});
        for (const auto& [l, id] : b.views) objs.push_back({l, id, true, true});
        Obj o{"nope" + std::to_string(step), kMissingId, false, false};
        if (!objs.empty() && rnd(100) >= 20) o = objs[rnd(static_cast<uint32_t>(objs.size()))];
        const bool same = a.nsId == b.nsId;
        const bool visible = same && o.exists;
        trace_ += " probe:" + user + "->" + b.name + "/" + o.local + (o.view ? "(view)" : "(topic)") + (o.exists ? "" : "(missing)");

        if (o.view) {
            const Answer got = withRetry(user, [&](int64_t sid) { return viewRowOf(sid, o.id); });
            if (visible) {
                ASSERT_EQ(got.code, 0) << "own view must be readable";
            } else {
                ASSERT_NE(got.code, 0) << "a foreign view's row must be hidden (invariant 1)";
                const Answer twin = withRetry(user, [&](int64_t sid) { return viewRowOf(sid, kMissingId); });
                ASSERT_EQ(got.code, twin.code) << "hidden view code differs from the missing twin (invariant 2)";
                ASSERT_EQ(hex(normalized(got, twin)), hex(normalized(twin, got)))
                    << "hidden view answer bytes differ from the missing twin (invariant 2)";
            }
            return;
        }
        // A caller names its own namespace's objects unqualified; a foreign one qualified (default: unqualified).
        const std::string asked = (same || b.nsId == 0) ? o.local : b.name + "/" + o.local;
        const Answer got = withRetry(user, [&](int64_t sid) { return schemaOf(sid, asked); });
        if (visible) {
            ASSERT_EQ(got.code, 0) << "own topic " << asked << " must resolve";
        } else {
            ASSERT_NE(got.code, 0) << "hidden/missing topic " << asked << " must not resolve (invariant 1)";
            const auto slash = asked.find('/');   // same qualification form, same length, never created
            const std::string twinName = slash == std::string::npos ? std::string(asked.size(), 'q')
                : asked.substr(0, slash + 1) + std::string(asked.size() - slash - 1, 'q');
            const Answer twin = withRetry(user, [&](int64_t sid) { return schemaOf(sid, twinName); });
            ASSERT_EQ(twinName.size(), asked.size());
            ASSERT_EQ(got.code, twin.code) << "hidden code differs from the missing twin " << twinName << " (invariant 2)";
            ASSERT_EQ(hex(normalized(got, twin)), hex(normalized(twin, got)))
                << "hidden answer bytes differ from the missing twin " << twinName << " (invariant 2)";
        }
        // by id: the Metadata listing of the caller
        const auto ids = r_.metadataTopics(userSid(user));
        const bool listed = std::find(ids.begin(), ids.end(), o.id) != ids.end();
        ASSERT_EQ(listed, visible) << "Metadata listing of topic id " << o.id << " for " << user << " (invariant 1)";
    }

    void restartAndCheck() {
        std::vector<int32_t> want;
        for (const NsModel& n : ns_) for (const auto& [l, id] : n.topics) want.push_back(id);
        ASSERT_TRUE(awaitIds(want)) << "before restart: model topics not all listed";
        const Fingerprint f1 = fingerprint();
        r_.restart();
        root_ = {};
        sess_.clear();
        rootSid();
        for (const NsModel& n : ns_) for (const std::string& u : n.users) userSid(u);
        ASSERT_TRUE(awaitIds(want)) << "after restart: model topics not all listed";
        const Fingerprint f2 = fingerprint();
        ASSERT_TRUE(f1 == f2) << "restart changed the world (invariant 3)\n  before: " << show(f1) << "\n  after:  " << show(f2);
    }

    void run() {
        const int steps = stepsFromEnv();
        ns_.push_back(NsModel{0, "default", {}, {}, {}, {}});
        rootSid();
        addUser(ns_[0], "0", "default user");
        ASSERT_FALSE(HasFatalFailure());
        for (int step = 1; step <= steps; ++step) {
            trace_.clear();
            const uint32_t roll = rnd(100);
            const char* op = "probe";
            if (roll < 5 && ns_.size() < kMaxNamespaces) op = "create-namespace";
            else if (roll < 25) op = "create-topic";
            else if (roll < 35) op = "drop-topic";
            else if (roll < 45) op = "create-view";
            else if (roll < 50) op = "create-user";
            SCOPED_TRACE("seed=" + std::to_string(seed_) + " step=" + std::to_string(step) + " op=" + op);
            if (!std::strcmp(op, "create-namespace")) stepCreateNamespace(step);
            else if (!std::strcmp(op, "create-topic")) stepCreateTopic(step);
            else if (!std::strcmp(op, "drop-topic")) stepDropTopic();
            else if (!std::strcmp(op, "create-view")) stepCreateView(step);
            else if (!std::strcmp(op, "create-user")) stepCreateUser(step);
            else stepProbe(step);
            if (HasFatalFailure() || HasNonfatalFailure()) {
                ADD_FAILURE() << "seed=" << seed_ << " step=" << step << " op=" << op << trace_;
                return;
            }
            if (step % kRestartEvery == 0) {
                SCOPED_TRACE("seed=" + std::to_string(seed_) + " step=" + std::to_string(step) + " op=restart");
                restartAndCheck();
                if (HasFatalFailure() || HasNonfatalFailure()) return;
            }
        }
    }

    uint32_t seed_;
    AuthRig r_;
    std::mt19937 rng_;
    std::vector<NsModel> ns_;
    uint32_t lastNsId_ = 0;
    uint64_t rid_ = 0;
    Session root_;
    std::map<std::string, Session> sess_;
    std::string trace_;
};

TEST_P(NamespaceStressRegression, SeededModelRegression) { run(); }

INSTANTIATE_TEST_SUITE_P(Seeds, NamespaceStressRegression, ::testing::Range(1u, 21u));

}  // namespace rune::node::test
