// NamespaceStressTest.cpp - Namespaces Task 13 (spec section 8): a seeded, model-checked stress of the namespace
// walls on ONE real authenticating broker (default config).  Per seed: a random mix of CREATE NAMESPACE / topic /
// view / user DDL (root) and cross-namespace probes (namespace users), checked against an in-test model.
//
// Invariants: (1) a caller never sees another namespace's object (name, Metadata listing, view row); (2) a hidden
// object's answer is byte-equal to the answer for a never-created twin of the same shape; (3) a broker restart
// changes nothing (namespaces, topics, security digest); (4) namespace ids are strictly increasing.
//
// Phase 2 Task 18 (spec §8) adds the lifecycle to the op set: suspend (ReadOnly / RejectAll), resume, delete, recreate,
// SetLimits up and down, produce, commit offset, open / commit / abort transactions (a real client), crash and restart
// (every 50 steps) and, in NamespaceLifecycleStressTrio, leader change on three real AdminWorkers.  A model map holds the
// expected registry (name, state, limits), the per-namespace contents and the transactions.  After EVERY step:
//   (5) the registry equals the model (names, states, maxTopics; a deleted id is absent or DELETING, never ACTIVE);
//   (6) nothing of a deleted nsId resolves by name or by id (topics, views), and a re-created name holds none of it;
//   (7) topic counts respect the limits (a lowered limit keeps what exists and refuses the next create);
//   (8) a ReadOnly namespace refuses produce and offset commits with 51; offsets never move back;
// and at every checkpoint (every 100 steps and at the end):
//   (9) read_committed shows every committed transaction record and no aborted one, whatever aborted it (the client,
//       a suspend, a delete);
//   (10) the deleted namespaces have been erased (reaper to the end): absent from the listing, their directories gone.
// The Phase 1 TODO (delete invariant) is closed by (5), (6) and (10).
//
// The plan caps a seed at 2000 steps; 300 keeps the 20 seeds fast (RUNE_NS_STRESS_STEPS overrides, hard cap 2000).
// Every failure message carries seed=<n> step=<k> op=<...> (SCOPED_TRACE).  Every wait has a deadline (a hard cap that
// FAILS, never a hang): the broker runs on its own threads, so the bounded polls are on its observable state only.

#include "support/AuthRig.h"
#include "support/CatalogTrio.h"

#include <rune/client/Client.h>
#include <rune/client/Completions.h>
#include <rune/client/producer/Producer.h>
#include <rune/client/subscriber/Subscription.h>
#include <rune/node/NamespacePaths.h>

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <map>
#include <random>
#include <set>
#include <thread>
#include <memory>
#include <mutex>
#include <filesystem>
#include <string>
#include <vector>

namespace rune::node::test {
namespace {

namespace pc = rune::protocol::client;
namespace rc = rune::client;
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
    uint8_t state = 0;                       ///< NsStateTag: 0 Active, 1 SuspendedReadOnly, 2 SuspendedRejectAll
    uint64_t maxTopics = 0;                  ///< the stored maxTopics (0 = unlimited, the default record)
    bool lowered = false;                    ///< SetLimits lowered it to at or below the topic count (creates refused at the limit)
    std::map<std::string, int64_t> committed;   ///< topic local name -> the group offset last committed (monotone)
    std::map<std::string, std::vector<std::string>> acked;   ///< topic local name -> plain records acked (the model of the log)
};

/// A deleted namespace: what must never be visible again.
struct GoneNs {
    uint32_t nsId = 0;
    std::string name;
    std::vector<std::string> topicNames;     ///< qualified: <name>/<local>
    std::vector<int32_t> topicIds, viewIds;
    std::vector<std::string> users;
    bool erasedSeen = false;
};

/// The transactions of one namespace's topic, in the order opened.
struct TxnRec {
    uint32_t nsId = 0;
    std::string ns, topic;                   ///< namespace name, local topic name
    std::vector<std::string> values;
    enum class St { Open, Committed, Aborted } st = St::Open;
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
    ~NamespaceStressRegression() override { closeAllTxnClients(); }

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
    NsModel* byName(const std::string& name) {
        for (NsModel& n : ns_) if (n.name == name) return &n;
        return nullptr;
    }
    NsModel* byId(uint32_t id) {
        for (NsModel& n : ns_) if (n.nsId == id) return &n;
        return nullptr;
    }
    /// A random live namespace satisfying `pred` (nullptr: none).
    template <class P>
    NsModel* pickNs(P&& pred) {
        std::vector<NsModel*> c;
        for (NsModel& n : ns_) if (pred(n)) c.push_back(&n);
        return c.empty() ? nullptr : c[rnd(static_cast<uint32_t>(c.size()))];
    }
    NsModel* pickActive() { return pickNs([](const NsModel& n) { return n.state == 0; }); }
    std::filesystem::path dataDir() { return r_.broker().dir() / "broker"; }

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
    void evictSessions(const std::vector<std::string>& users) {
        for (const std::string& u : users) sess_.erase(u);
    }
    /// A login that may be refused: its AuthServerFinal code (0 = logged in).
    int loginCode(const std::string& user) {
        AuthWireClient& w = r_.wire();
        const int64_t sid = w.connect(r_.ch(), pc::kClientVersionNamespaces);
        if (sid <= 0) return kNoAnswer;
        const auto l = w.login(r_.ch(), sid, user, {AuthRig::kPw, AuthRig::kPw});
        if (l.ok()) { r_.broker().noteLoginOk(); return 0; }
        return l.answered() ? static_cast<int>(l.errorCode()) : kNoAnswer;
    }

    // ── root DDL ────────────────────────────────────────────────────────────────────────────────────────────
    /// One NamespaceDdl as root; the answer code, and (Create) the new nsId.
    int nsDdl(pc::NamespaceDdlKind kind, const std::string& name, uint8_t mode = 1, int field = -1, uint64_t value = 0,
              uint32_t* idOut = nullptr) {
        pc::NamespaceDdlRequest m;
        m.sessionId = rootSid();
        m.correlationId = r_.c().nextCorrelationId();
        m.ext.clientName = "ns-stress";
        m.ext.requestId = rune::protocol::RequestId{0x57E55, ++rid_};
        m.kind = kind;
        m.name = name;
        if (kind == pc::NamespaceDdlKind::Suspend) m.mode = static_cast<pc::NsSuspendMode>(mode);
        if (kind == pc::NamespaceDdlKind::SetLimits && field >= 0) {
            m.presentMask[static_cast<std::size_t>(field) >> 3] = static_cast<uint8_t>(1u << (field & 7));
            (&m.limits.maxTopics)[field] = value;
        }
        std::vector<uint8_t> buf(m.encodedSize());
        buf.resize(m.encode(buf.data()));
        if (!r_.c().send(r_.ch(), AuthRig::stream(), buf.data(), buf.size())) return kNoAnswer;
        const auto any = r_.c().awaitAnyDdlAnswer(m.correlationId, AuthRig::dl(15));
        if (!any) return kNoAnswer;
        if (idOut != nullptr) {
            const auto a = r_.c().awaitDdl(m.correlationId, AuthRig::dl(1));
            *idOut = a && any->code == 0 ? static_cast<uint32_t>(a->objectId) : 0;
        }
        return any->code;
    }
    uint32_t createNamespace(const std::string& name, int& code) {
        uint32_t id = 0;
        code = nsDdl(pc::NamespaceDdlKind::Create, name, 1, -1, 0, &id);
        return id;
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
    struct Listed { uint32_t nsId; std::string name; uint8_t state; uint64_t maxTopics; };
    std::vector<Listed> listNamespaces() {
        std::vector<Listed> out;
        uint32_t after = 0;
        for (int page = 0; page < 100; ++page) {
            pc::ListNamespaces m;
            m.sessionId = rootSid();
            m.correlationId = r_.c().nextCorrelationId();
            m.afterNsId = after;
            m.max = 100;
            std::vector<uint8_t> buf(m.encodedSize());
            buf.resize(m.encode(buf.data()));
            EXPECT_TRUE(r_.c().send(r_.ch(), AuthRig::stream(), buf.data(), buf.size()));
            const auto raw = r_.rawAnswer(m.correlationId);
            const auto l = raw.empty() ? std::nullopt : pc::ListNamespacesResponse::decode(raw.data(), raw.size());
            EXPECT_TRUE(l.has_value()) << "ListNamespaces answered nothing decodable";
            if (!l) return out;
            for (const auto& i : l->namespaces) out.push_back({i.nsId, i.name, i.state, i.limits.maxTopics});
            if (l->nextAfter == 0) return out;
            after = l->nextAfter;
        }
        ADD_FAILURE() << "ListNamespaces paging cap";
        return out;
    }
    Fingerprint fingerprint() {
        Fingerprint f;
        for (const Listed& i : listNamespaces())
            f.namespaces.push_back(std::to_string(i.nsId) + ":" + i.name + ":" + std::to_string(i.state) + ":" +
                                   std::to_string(i.maxTopics));
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

    // ── steps: the Phase 1 mix (Active namespaces only: a suspended namespace's DDL is not modelled) ─────────
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
        NsModel* np = pickActive();
        if (np == nullptr) return stepProbe(step);
        NsModel& n = *np;
        const std::string local = "t" + std::to_string(step);
        if (n.lowered) {
            // A limit lowered by SetLimits: at or over it the create is refused and nothing changes (lowering deleted nothing).
            // Under it the count rule (topics alone, or topics plus view sinks) is the broker's: not modelled, skipped.
            if (n.topics.size() < n.maxTopics) return stepProbe(step);
            ASSERT_EQ(r_.defineTopicCode(rootSid(), full(n, local)), static_cast<int>(pc::ErrorCode::NamespaceLimitExceeded))
                << "create topic past maxTopics=" << n.maxTopics << " in " << n.name;
            trace_ += " refused-at-limit";
            return;
        }
        const int32_t id = r_.defineTopic(rootSid(), full(n, local));
        ASSERT_GE(id, 0) << "create topic " << full(n, local);
        n.topics[local] = id;
        ASSERT_TRUE(awaitIds({id})) << "topic " << full(n, local) << " (id " << id << ") never listed for root";
    }
    void stepDropTopic() {
        std::vector<std::pair<NsModel*, std::string>> c;
        for (NsModel& n : ns_)
            if (n.state == 0)
                for (const auto& [l, id] : n.topics) if (!n.viewedTopics.count(l) && !txnTopic(n.nsId, l)) c.emplace_back(&n, l);
        if (c.empty()) return stepProbe(0);
        const auto& [n, l] = c[rnd(static_cast<uint32_t>(c.size()))];
        const int32_t id = n->topics[l];
        ASSERT_EQ(r_.dropTopicCode(rootSid(), full(*n, l)), 0) << "drop topic " << full(*n, l);
        n->topics.erase(l);
        n->acked.erase(l);
        n->committed.erase(l);
        ASSERT_TRUE(awaitIds({}, {id})) << "dropped topic id " << id << " still listed";
    }
    void stepCreateView(int step) {
        std::vector<std::pair<NsModel*, std::string>> c;
        for (NsModel& n : ns_)
            if (n.state == 0 && !n.lowered)
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
        NsModel* n = pickActive();
        if (n == nullptr) return stepProbe(step);
        addUser(*n, std::to_string(step), "create user");
    }

    void stepProbe(int step) {
        // Caller: a random user of a random namespace (A) that can be logged in (not RejectAll).  Object: an existing
        // topic / view of a random namespace (B), or with 20% a name never created.
        const NsModel* ap = pickNs([](const NsModel& n) { return n.state != 2 && !n.users.empty(); });
        if (ap == nullptr) return;
        const NsModel& a = *ap;
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

    // ── steps: the lifecycle ─────────────────────────────────────────────────────────────────────────────────
    static constexpr int kMaxTopicsField = 0;
    void stepSuspend() {
        NsModel* n = pickNs([](const NsModel& x) { return x.nsId != 0; });
        if (n == nullptr) return;
        const uint8_t mode = static_cast<uint8_t>(1 + rnd(2));
        trace_ += " suspend:" + n->name + (mode == 1 ? "(RO)" : "(RA)");
        ASSERT_EQ(nsDdl(pc::NamespaceDdlKind::Suspend, n->name, mode), 0) << "suspend " << n->name << " mode " << int(mode);
        n->state = mode;
        platformAbort(n->nsId);
        if (mode == 2) evictSessions(n->users);
    }
    void stepResume() {
        NsModel* n = pickNs([](const NsModel& x) { return x.nsId != 0 && x.state != 0; });
        if (n == nullptr) return;
        trace_ += " resume:" + n->name;
        ASSERT_EQ(nsDdl(pc::NamespaceDdlKind::Resume, n->name), 0) << "resume " << n->name;
        n->state = 0;
    }
    void stepDelete() {
        NsModel* n = pickNs([](const NsModel& x) { return x.nsId != 0; });
        if (n == nullptr) return;
        trace_ += " delete:" + n->name;
        GoneNs g;
        g.nsId = n->nsId;
        g.name = n->name;
        for (const auto& [l, id] : n->topics) { g.topicNames.push_back(n->name + "/" + l); g.topicIds.push_back(id); }
        for (const auto& [l, id] : n->views) { g.topicNames.push_back(n->name + "/" + l); g.viewIds.push_back(id); }
        g.users = n->users;
        ASSERT_EQ(nsDdl(pc::NamespaceDdlKind::Delete, n->name), 0) << "delete " << n->name;
        platformAbort(n->nsId);
        closeTxnClient(n->name);
        evictSessions(n->users);
        for (const std::string& u : g.users)
            ASSERT_NE(loginCode(u), 0) << "a user of the deleted namespace " << n->name << " logged in";
        gone_.push_back(std::move(g));
        ns_.erase(ns_.begin() + (n - ns_.data()));
    }
    void stepRecreate() {
        std::vector<std::size_t> c;
        for (std::size_t i = 0; i < gone_.size(); ++i)
            if (byName(gone_[i].name) == nullptr) c.push_back(i);
        if (c.empty()) return;
        const GoneNs old = gone_[c[rnd(static_cast<uint32_t>(c.size()))]];
        trace_ += " recreate:" + old.name;
        int code = 0;
        const uint32_t id = createNamespace(old.name, code);
        ASSERT_EQ(code, 0) << "re-create " << old.name << " (the name is free at once)";
        ASSERT_EQ(id, lastNsId_ + 1) << "a recreated name gets a new, increasing id";
        lastNsId_ = id;
        NsModel n;
        n.nsId = id;
        n.name = old.name;
        ns_.push_back(n);
        // The new namespace holds nothing of the old one: no topic of the old id is listed once it is erased.
        ASSERT_TRUE(awaitErased(old.nsId)) << "the old " << old.name << " (id " << old.nsId << ") was never erased";
        const auto ids = r_.metadataTopics(rootSid());
        for (const int32_t t : old.topicIds)
            ASSERT_TRUE(std::find(ids.begin(), ids.end(), t) == ids.end()) << "topic id " << t << " of the old namespace is listed";
        addUser(ns_.back(), "0", "recreated namespace");
    }
    void stepSetLimits() {
        NsModel* n = pickNs([](const NsModel& x) { return x.nsId != 0; });
        if (n == nullptr) return;
        const bool lower = !n->topics.empty() && rnd(2) == 0;
        uint64_t v = 1000;
        if (lower) v = 1 + rnd(static_cast<uint32_t>(n->topics.size()));   // 1..topics: at or below the usage
        trace_ += " setlimits:" + n->name + "=" + std::to_string(v);
        ASSERT_EQ(nsDdl(pc::NamespaceDdlKind::SetLimits, n->name, 1, kMaxTopicsField, v), 0) << "set maxTopics=" << v << " on " << n->name;
        n->maxTopics = v;
        n->lowered = lower;
        if (lower)   // lowering deletes nothing: every model topic is still listed
            for (const auto& [l, id] : n->topics) ASSERT_TRUE(awaitIds({id})) << "lowering maxTopics deleted topic " << l;
    }
    void stepProduce() {
        NsModel* n = pickNs([](const NsModel& x) { return x.state != 2 && !x.topics.empty() && !x.users.empty(); });
        if (n == nullptr) return;
        const auto it = std::next(n->topics.begin(), rnd(static_cast<uint32_t>(n->topics.size())));
        const std::string user = n->users[0];
        trace_ += " produce:" + full(*n, it->first) + "(state " + std::to_string(n->state) + ")";
        int code = kNoAnswer;
        for (int attempt = 0; attempt < 40; ++attempt) {
            code = r_.produce(userSid(user), it->second);
            if (code != static_cast<int>(pc::ErrorCode::NotLeader)) break;
            r_.c().pollFor(std::chrono::milliseconds(25), [] { return false; });
        }
        if (n->state == 0) {
            ASSERT_EQ(code, 0) << "produce in an ACTIVE namespace";
            n->acked[it->first].push_back("");
        } else {
            ASSERT_EQ(code, static_cast<int>(pc::ErrorCode::NamespaceSuspended)) << "produce in a ReadOnly namespace (invariant 8)";
        }
    }
    void stepCommitOffset() {
        NsModel* n = pickNs([](const NsModel& x) { return x.state != 2 && !x.topics.empty() && !x.users.empty(); });
        if (n == nullptr) return;
        const auto it = std::next(n->topics.begin(), rnd(static_cast<uint32_t>(n->topics.size())));
        const std::string user = n->users[0];
        const std::string group = "g" + it->first;
        trace_ += " commit:" + full(*n, it->first) + "(state " + std::to_string(n->state) + ")";
        const int64_t sid = userSid(user);
        const auto [member, gen] = r_.joinStable(sid, group, {it->second});
        ASSERT_GE(gen, 0) << "join " << group << " in " << n->name << " (JoinGroup is allowed in ReadOnly)";
        const int64_t prev = n->committed.count(it->first) ? n->committed[it->first] : 0;
        const int64_t next = prev + 1 + rnd(5);
        const int code = r_.commitAs(sid, group, member, gen, it->second, next);
        if (n->state == 0) {
            ASSERT_EQ(code, 0) << "commit in an ACTIVE namespace";
            n->committed[it->first] = next;
        } else {
            ASSERT_EQ(code, static_cast<int>(pc::ErrorCode::NamespaceSuspended)) << "OffsetCommit in a ReadOnly namespace (invariant 8)";
        }
        if (n->committed.count(it->first))
            ASSERT_EQ(r_.committedOffset(sid, group, it->second), n->committed[it->first])
                << "the committed offset moved (offsets never move back)";
    }

    // ── transactions (a real client per namespace) ───────────────────────────────────────────────────────────
    struct TxnLive {
        std::unique_ptr<rc::Client> client;
        std::unique_ptr<rc::Producer> producer;
        std::size_t rec = 0;
    };
    std::unique_ptr<rc::Client> newClient(const std::string& user, const std::string& pw, std::vector<rc::TopicSpec> topics = {}) {
        rc::ClientConfig c;
        c.gatewayChannel  = r_.ch();
        c.gatewayStreamId = 1;
        c.egressChannel   = "aeron:udp?endpoint=127.0.0.1:" + std::to_string(rune::aeronutil::testing::allocFreeUdpPort());
        c.egressStreamId  = 2;
        c.dataDir         = r_.broker().clientDir().string();
        c.username        = user;
        c.password        = pw;
        c.offerCrypto     = true;
        c.tlsTrustPem     = AuthBroker::tlsCaPem();
        c.topics          = std::move(topics);
        auto cl = std::make_unique<rc::Client>(c);
        cl->connectAndWait(std::chrono::seconds(20));
        return cl;
    }
    rc::Client& rootClient() {
        if (!rootClient_) rootClient_ = newClient("admin", AuthBroker::kAdminPassword);
        return *rootClient_;
    }
    bool txnTopic(uint32_t nsId, const std::string& topic) const {
        for (const TxnRec& t : txns_) if (t.nsId == nsId && t.topic == topic) return true;
        return false;
    }
    void closeTxnClient(const std::string& ns) {
        auto it = live_.find(ns);
        if (it == live_.end()) return;
        try { if (it->second.producer) it->second.producer->close(); } catch (...) {}
        try { if (it->second.client) it->second.client->close(); } catch (...) {}
        live_.erase(it);
    }
    void closeAllTxnClients() {
        while (!live_.empty()) closeTxnClient(live_.begin()->first);
        try { if (rootClient_) rootClient_->close(); } catch (...) {}
        rootClient_.reset();
    }
    /// The platform aborts the namespace's open transaction (suspend either mode, delete): the model records it.
    void platformAbort(uint32_t nsId) {
        for (TxnRec& t : txns_)
            if (t.nsId == nsId && t.st == TxnRec::St::Open) t.st = TxnRec::St::Aborted;
    }
    void stepTxnBegin(int step) {
        NsModel* n = pickNs([&](const NsModel& x) {
            return x.state == 0 && !x.topics.empty() && !x.users.empty() && live_.find(x.name) == live_.end();
        });
        if (n == nullptr) return;
        const auto it = std::next(n->topics.begin(), rnd(static_cast<uint32_t>(n->topics.size())));
        trace_ += " txn-begin:" + full(*n, it->first);
        TxnLive l;
        l.client = newClient(n->users[0], AuthRig::kPw, {rc::TopicSpec{it->first, it->second, 1}});
        rc::ProducerConfig cfg;
        cfg.acks = rc::Acks::All;
        cfg.transactionalId = "tx";
        l.producer = std::make_unique<rc::Producer>(l.client->producer(cfg));
        l.producer->initTransactionsAndWait(std::chrono::seconds(30));
        l.producer->beginTransaction();
        TxnRec rec;
        rec.nsId = n->nsId;
        rec.ns = n->name;
        rec.topic = it->first;
        for (int i = 0; i < 3; ++i) rec.values.push_back("tx" + std::to_string(seed_) + "_" + std::to_string(step) + "_" + std::to_string(i));
        for (std::size_t i = 0; i < rec.values.size(); ++i)
            l.producer->send(l.client->topic(it->first), rc::Key(static_cast<int64_t>(i)),
                             std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(rec.values[i].data()), rec.values[i].size()));
        l.producer->flushAndWait(std::chrono::seconds(30));
        txns_.push_back(std::move(rec));
        l.rec = txns_.size() - 1;
        live_.emplace(n->name, std::move(l));
    }
    /// Ends the namespace's live transaction: commit or abort (Active); a platform-aborted one must refuse the commit.
    void endTxn(const std::string& ns, bool commit) {
        auto it = live_.find(ns);
        if (it == live_.end()) return;
        TxnRec& rec = txns_[it->second.rec];
        const NsModel* n = byName(ns);
        const bool platformAborted = rec.st == TxnRec::St::Aborted;
        trace_ += std::string(" txn-") + (commit ? "commit:" : "abort:") + ns + (platformAborted ? "(aborted by the platform)" : "");
        std::string err;
        try {
            if (commit) it->second.producer->commitTransactionAndWait(std::chrono::seconds(30));
            else it->second.producer->abortTransactionAndWait(std::chrono::seconds(30));
        } catch (const std::exception& e) { err = e.what(); }
        if (platformAborted) {
            if (commit) EXPECT_FALSE(err.empty()) << "a commit succeeded after the platform aborted the transaction of " << ns;
        } else if (n != nullptr && n->state == 0) {
            EXPECT_TRUE(err.empty()) << "ending a transaction in an ACTIVE namespace failed: " << err;
            rec.st = commit ? TxnRec::St::Committed : TxnRec::St::Aborted;
        } else {
            ADD_FAILURE() << "an open transaction in a non-ACTIVE namespace was not aborted by the platform";
        }
        closeTxnClient(ns);
    }
    void stepTxnEnd() {
        std::vector<std::string> c;
        for (const auto& [ns, l] : live_) c.push_back(ns);
        if (c.empty()) return;
        endTxn(c[rnd(static_cast<uint32_t>(c.size()))], rnd(2) == 0);
    }
    void resolveAllTxns() {
        std::vector<std::string> c;
        for (const auto& [ns, l] : live_) c.push_back(ns);
        for (const std::string& ns : c) endTxn(ns, /*commit=*/false);
    }
    /// read_committed shows every committed transaction record and no aborted one.
    void verifyTransactions() {
        std::set<std::pair<uint32_t, std::string>> done;
        for (const TxnRec& t : txns_) {
            const NsModel* n = byId(t.nsId);
            if (n == nullptr || !n->topics.count(t.topic) || !done.insert({t.nsId, t.topic}).second) continue;
            const int32_t tid = n->topics.at(t.topic);
            const rc::TopicHandle th{tid, 1};
            rc::Client& root = rootClient();
            const std::string sentinel = "S" + std::to_string(seed_) + "_" + std::to_string(++sentinels_);
            {
                rc::ProducerConfig pcfg;
                pcfg.acks = rc::Acks::All;
                rc::Producer plain = root.producer(pcfg);
                plain.send(th, rc::Key(int64_t{1}), std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(sentinel.data()), sentinel.size()));
                ASSERT_EQ(plain.closeAndWait(std::chrono::seconds(30)), rc::OpStatus::Ok) << "sentinel produce on " << full(*n, t.topic);
            }
            std::mutex mu;
            std::vector<std::string> values;
            rc::SubscribeOptions o;
            o.from = rc::From::earliest();
            o.isolation = rc::Isolation::ReadCommitted;
            rc::Subscription sub = root.subscribe(th, 0, o, [&](const rc::Record& r) {
                std::lock_guard<std::mutex> lk(mu);
                values.emplace_back(reinterpret_cast<const char*>(r.value.data()), r.value.size());
            });
            auto snap = [&] { std::lock_guard<std::mutex> lk(mu); return values; };
            bool seen = false;
            for (int i = 0; i < 20'000 && !seen; ++i) {   // 20 000 x 2 ms: a hard cap that fails
                const auto s = snap();
                seen = std::find(s.begin(), s.end(), sentinel) != s.end();
                if (!seen) std::this_thread::sleep_for(std::chrono::milliseconds(2));
            }
            sub.unsubscribe();
            ASSERT_TRUE(seen) << "the sentinel never reached read_committed on " << full(*n, t.topic)
                              << ": a transaction is still holding the last stable offset";
            std::multiset<std::string> got;
            for (const std::string& v : snap()) if (v.rfind("tx", 0) == 0) got.insert(v);
            std::multiset<std::string> want;
            for (const TxnRec& u : txns_)
                if (u.nsId == t.nsId && u.topic == t.topic) {
                    ASSERT_NE(u.st, TxnRec::St::Open) << "an open transaction at the checkpoint";
                    if (u.st == TxnRec::St::Committed) for (const std::string& v : u.values) want.insert(v);
                }
            ASSERT_TRUE(got == want) << "read_committed on " << full(*n, t.topic) << " differs from the committed model (" << got.size()
                                     << " visible, " << want.size() << " committed): a committed record is lost or an aborted one is visible";
        }
    }

    // ── deleted namespaces ───────────────────────────────────────────────────────────────────────────────────
    bool awaitErased(uint32_t nsId) {
        for (int i = 0; i < 4000; ++i) {   // 4000 x 25 ms: the reaper runs to the end, a hard cap that fails
            bool present = false;
            for (const Listed& l : listNamespaces()) present = present || l.nsId == nsId;
            if (!present) return true;
            r_.c().pollFor(std::chrono::milliseconds(25), [] { return false; });
        }
        return false;
    }
    void awaitAllGoneErased() {
        for (GoneNs& g : gone_) {
            if (g.erasedSeen) continue;
            ASSERT_TRUE(awaitErased(g.nsId)) << "deleted " << g.name << " (id " << g.nsId << ") was never erased";
            g.erasedSeen = true;
            ASSERT_TRUE(waitGone(nsRoot(dataDir(), g.nsId))) << "ns-" << g.nsId << " directory survived the delete";
            ASSERT_TRUE(waitGone(std::filesystem::path(nsRoot(dataDir(), g.nsId).string() + ".deleting")));
            ASSERT_TRUE(r_.broker().node().nsKeyedStoresCleanForTest(g.nsId))
                << "the consumer-offset or transaction-state store still holds a key of the erased namespace " << g.name << " (id " << g.nsId << ")";
            const auto ids = r_.metadataTopics(rootSid());
            for (const int32_t t : g.topicIds)
                ASSERT_TRUE(std::find(ids.begin(), ids.end(), t) == ids.end()) << "topic id " << t << " of erased " << g.name << " is listed";
        }
    }
    static bool waitGone(const std::filesystem::path& p) {
        for (int i = 0; i < 2000; ++i) {   // the off-thread purge of ns-<id>.deleting: 2000 x 10 ms
            if (!std::filesystem::exists(p)) return true;
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        return !std::filesystem::exists(p);
    }

    // ── invariants after every step ──────────────────────────────────────────────────────────────────────────
    void checkInvariants() {
        const std::vector<Listed> listed = listNamespaces();
        std::map<std::string, Listed> live;
        for (const Listed& l : listed) {
            if (l.state == 3) continue;
            live[l.name] = l;
        }
        // (5) the registry equals the model
        ASSERT_EQ(live.size(), ns_.size()) << "the registry lists a different set of live namespaces than the model";
        for (const NsModel& n : ns_) {
            ASSERT_TRUE(live.count(n.name)) << "namespace " << n.name << " missing from the registry";
            const Listed& l = live[n.name];
            ASSERT_EQ(l.nsId, n.nsId) << n.name;
            ASSERT_EQ(l.state, n.state) << n.name << " state";
            ASSERT_EQ(l.maxTopics, n.maxTopics) << n.name << " maxTopics";
        }
        for (const GoneNs& g : gone_)
            for (const Listed& l : listed)
                if (l.nsId == g.nsId) ASSERT_EQ(l.state, 3) << "deleted " << g.name << " (id " << g.nsId << ") is " << int(l.state) << ", not DELETING";
        // (6) nothing of the most recent deleted namespaces resolves
        std::size_t seen = 0;
        for (auto it = gone_.rbegin(); it != gone_.rend() && seen < 3; ++it, ++seen) {
            if (it->erasedSeen) continue;
            for (const std::string& q : it->topicNames) {
                const Answer a = schemaOf(rootSid(), q);
                ASSERT_NE(a.code, 0) << "topic " << q << " of the deleted namespace " << it->name << " still resolves (invariant 6)";
            }
            for (const int32_t v : it->viewIds) {
                const Answer a = viewRowOf(rootSid(), v);
                ASSERT_NE(a.code, 0) << "view id " << v << " of the deleted namespace " << it->name << " still readable (invariant 6)";
            }
        }
    }
    void checkpoint() {
        resolveAllTxns();
        awaitAllGoneErased();
        if (HasFatalFailure() || HasNonfatalFailure()) return;
        verifyTransactions();
        if (HasFatalFailure() || HasNonfatalFailure()) return;
        // produced = what the log holds, for topics no transaction touched (transaction markers occupy offsets)
        for (const NsModel& n : ns_)
            for (const auto& [l, id] : n.topics) {
                const auto a = n.acked.find(l);
                if (a == n.acked.end() || txnTopic(n.nsId, l)) continue;
                ASSERT_EQ(r_.logEnd(id), static_cast<int64_t>(a->second.size())) << "produced != logged on " << full(n, l);
            }
    }

    void restartAndCheck() {
        resolveAllTxns();
        closeAllTxnClients();
        awaitAllGoneErased();
        if (HasFatalFailure() || HasNonfatalFailure()) return;
        std::vector<int32_t> want;
        for (const NsModel& n : ns_) for (const auto& [l, id] : n.topics) want.push_back(id);
        ASSERT_TRUE(awaitIds(want)) << "before restart: model topics not all listed";
        const Fingerprint f1 = fingerprint();
        r_.restart();
        root_ = {};
        sess_.clear();
        rootSid();
        for (const NsModel& n : ns_) if (n.state != 2) for (const std::string& u : n.users) userSid(u);
        ASSERT_TRUE(awaitIds(want)) << "after restart: model topics not all listed";
        const Fingerprint f2 = fingerprint();
        ASSERT_TRUE(f1 == f2) << "restart changed the world (invariant 3)\n  before: " << show(f1) << "\n  after:  " << show(f2);
        // the gates survived: a RejectAll namespace refuses its logins, a ReadOnly one its produce
        for (const NsModel& n : ns_) {
            if (n.state == 2 && !n.users.empty())
                ASSERT_EQ(loginCode(n.users[0]), static_cast<int>(pc::ErrorCode::NamespaceSuspended)) << n.name << " RejectAll after restart";
            if (n.state == 1 && !n.topics.empty() && !n.users.empty())
                ASSERT_EQ(r_.produce(userSid(n.users[0]), n.topics.begin()->second), static_cast<int>(pc::ErrorCode::NamespaceSuspended))
                    << n.name << " ReadOnly after restart";
        }
    }

    void run() {
        const int steps = stepsFromEnv();
        ns_.push_back(NsModel{});
        ns_[0].name = "default";
        rootSid();
        addUser(ns_[0], "0", "default user");
        ASSERT_FALSE(HasFatalFailure());
        for (int step = 1; step <= steps; ++step) {
            trace_.clear();
            const uint32_t roll = rnd(100);
            const char* op = "probe";
            if (roll < 4 && ns_.size() < kMaxNamespaces) op = "create-namespace";
            else if (roll < 16) op = "create-topic";
            else if (roll < 21) op = "drop-topic";
            else if (roll < 26) op = "create-view";
            else if (roll < 29) op = "create-user";
            else if (roll < 34) op = "suspend";
            else if (roll < 38) op = "resume";
            else if (roll < 41) op = "delete";
            else if (roll < 46) op = "recreate";
            else if (roll < 50) op = "set-limits";
            else if (roll < 58) op = "produce";
            else if (roll < 64) op = "commit-offset";
            else if (roll < 72) op = "txn-begin";
            else if (roll < 78) op = "txn-end";
            SCOPED_TRACE("seed=" + std::to_string(seed_) + " step=" + std::to_string(step) + " op=" + op);
            ++tally_[op];
            try {
                if (!std::strcmp(op, "create-namespace")) stepCreateNamespace(step);
                else if (!std::strcmp(op, "create-topic")) stepCreateTopic(step);
                else if (!std::strcmp(op, "drop-topic")) stepDropTopic();
                else if (!std::strcmp(op, "create-view")) stepCreateView(step);
                else if (!std::strcmp(op, "create-user")) stepCreateUser(step);
                else if (!std::strcmp(op, "suspend")) stepSuspend();
                else if (!std::strcmp(op, "resume")) stepResume();
                else if (!std::strcmp(op, "delete")) stepDelete();
                else if (!std::strcmp(op, "recreate")) stepRecreate();
                else if (!std::strcmp(op, "set-limits")) stepSetLimits();
                else if (!std::strcmp(op, "produce")) stepProduce();
                else if (!std::strcmp(op, "commit-offset")) stepCommitOffset();
                else if (!std::strcmp(op, "txn-begin")) stepTxnBegin(step);
                else if (!std::strcmp(op, "txn-end")) stepTxnEnd();
                else stepProbe(step);
                if (!HasFatalFailure() && !HasNonfatalFailure()) checkInvariants();
            } catch (const std::exception& e) {
                ADD_FAILURE() << "seed=" << seed_ << " step=" << step << " op=" << op << " threw: " << e.what() << trace_;
                return;
            }
            if (HasFatalFailure() || HasNonfatalFailure()) {
                ADD_FAILURE() << "seed=" << seed_ << " step=" << step << " op=" << op << trace_;
                return;
            }
            if (step % kCheckpointEvery == 0) {
                SCOPED_TRACE("seed=" + std::to_string(seed_) + " step=" + std::to_string(step) + " op=checkpoint");
                checkpoint();
                if (HasFatalFailure() || HasNonfatalFailure()) return;
            }
            if (step % kRestartEvery == 0) {
                SCOPED_TRACE("seed=" + std::to_string(seed_) + " step=" + std::to_string(step) + " op=restart");
                restartAndCheck();
                if (HasFatalFailure() || HasNonfatalFailure()) return;
            }
        }
        SCOPED_TRACE("seed=" + std::to_string(seed_) + " op=final-checkpoint");
        checkpoint();
        std::string t = "seed=" + std::to_string(seed_) + " ops:";
        for (const auto& [k, v] : tally_) t += " " + k + "=" + std::to_string(v);
        t += " gone=" + std::to_string(gone_.size()) + " txns=" + std::to_string(txns_.size());
        std::size_t committed = 0, aborted = 0;
        for (const TxnRec& x : txns_) (x.st == TxnRec::St::Committed ? committed : aborted)++;
        std::printf("[ STRESS ] %s committed=%zu aborted=%zu\n", t.c_str(), committed, aborted);
    }

    static constexpr int kCheckpointEvery = 100;
    uint32_t seed_;
    AuthRig r_;
    std::mt19937 rng_;
    std::vector<NsModel> ns_;
    std::vector<GoneNs> gone_;
    std::vector<TxnRec> txns_;
    std::map<std::string, TxnLive> live_;
    std::unique_ptr<rc::Client> rootClient_;
    uint32_t lastNsId_ = 0;
    uint64_t rid_ = 0;
    int sentinels_ = 0;
    Session root_;
    std::map<std::string, Session> sess_;
    std::string trace_;
    std::map<std::string, int> tally_;
};

TEST_P(NamespaceStressRegression, SeededModelRegression) { run(); }

INSTANTIATE_TEST_SUITE_P(Seeds, NamespaceStressRegression, ::testing::Range(1u, 21u));


// ── the 3-node variant: the catalog ops under random leader changes (three real AdminWorkers, CatalogTrio) ────────────────
// Seeded and model-checked like the one-node stress: after EVERY step all three brokers' registries equal the model
// (name, state, maxTopics, nsId), a deleted id is absent or DELETING and is erased on all three once the reaper (whichever
// broker leads at the time) has finished, and the catalog digests agree.
namespace {

struct TrioNs {
    uint32_t nsId = 0;
    uint8_t state = 0;          // 0 Active, 1 RO, 2 RA
    uint64_t maxTopics = 0;
    std::size_t topics = 0;
    bool limited = false;
};
struct TrioGone { uint32_t nsId; std::string name; };

}  // namespace

class NamespaceLifecycleStressTrioRegression : public ::testing::TestWithParam<uint32_t> {
protected:
    NamespaceLifecycleStressTrioRegression() : seed_(GetParam()), rng_(GetParam()), trio_(("lcstress" + std::to_string(GetParam())).c_str()) {}

    uint32_t rnd(uint32_t n) { return static_cast<uint32_t>(rng_() % n); }
    int64_t submit(uint8_t kind, const std::string& name, uint8_t mode = 1, int field = -1, uint64_t value = 0) {
        NamespaceDdlMsg m;
        m.session = CatalogTrio::kSession; m.corr = 70'000 + ++seq_; m.kind = kind; m.name = name; m.mode = mode;
        m.clientVersion = 14;
        m.meta.sessionId = CatalogTrio::kSession;
        m.meta.requestId = rune::protocol::RequestId{0x57E5, static_cast<uint64_t>(m.corr)};
        if (kind == 2 && field >= 0) {
            m.mask[static_cast<std::size_t>(field) >> 3] = static_cast<uint8_t>(1u << (field & 7));
            (&m.limits.maxTopics)[field] = value;
        }
        const int64_t corr = m.corr;
        trio_.admin(leader_).submit(DdlRequest{std::move(m)});
        return corr;
    }
    int ddl(uint8_t kind, const std::string& name, uint8_t mode = 1, int field = -1, uint64_t value = 0) {
        return trio_.awaitAnswer(leader_, submit(kind, name, mode, field, value)).code;
    }
    const rune::catalog::NamespaceRecord* rec(int32_t b, const std::string& name) {
        return trio_.admin(b).applier().state().namespaces().byName(name);
    }
    bool erasedOn(int32_t b, uint32_t id) { return trio_.admin(b).applier().state().namespaces().byId(id) == nullptr; }

    void stepCreate(int step) {
        if (ns_.size() >= 6) return;
        const std::string name = "n" + std::to_string(step);
        const int64_t c = trio_.submitCreateNamespaceNoWait(leader_, name, {});
        ASSERT_EQ(trio_.awaitAnswer(leader_, c).code, 0) << "create " << name;
        const auto* r = rec(leader_, name);
        ASSERT_NE(r, nullptr);
        ASSERT_GT(r->nsId, lastId_) << "ids strictly increase";
        lastId_ = r->nsId;
        ns_[name] = TrioNs{r->nsId, 0, 0, 0, false};
    }
    void stepTopic(int step) {
        for (auto& [name, n] : ns_) {
            if (n.state != 0 || rnd(2) == 0) continue;
            if (n.limited && n.topics < n.maxTopics) return;   // under a lowered limit: the count rule is the broker's, not modelled
            const auto a = trio_.define(leader_, {.name = "t" + std::to_string(step), .partitions = 1, .nsId = n.nsId});
            if (n.limited) ASSERT_EQ(a.code, static_cast<int>(rune::protocol::client::ErrorCode::NamespaceLimitExceeded)) << name << " at maxTopics";
            else { ASSERT_EQ(a.code, 0) << name; ++n.topics; }
            return;
        }
    }
    TrioNs* pickNs() {
        if (ns_.empty()) return nullptr;
        auto it = std::next(ns_.begin(), rnd(static_cast<uint32_t>(ns_.size())));
        return &it->second;
    }
    std::string nameOf(const TrioNs* n) {
        for (const auto& [name, x] : ns_) if (&x == n) return name;
        return {};
    }
    void stepSuspend() {
        TrioNs* n = pickNs();
        if (n == nullptr) return;
        const uint8_t mode = static_cast<uint8_t>(1 + rnd(2));
        ASSERT_EQ(ddl(3, nameOf(n), mode), 0);
        n->state = mode;
    }
    void stepResume() {
        TrioNs* n = pickNs();
        if (n == nullptr) return;
        ASSERT_EQ(ddl(4, nameOf(n)), 0);
        n->state = 0;
    }
    void stepDelete() {
        TrioNs* n = pickNs();
        if (n == nullptr) return;
        const std::string name = nameOf(n);
        ASSERT_EQ(ddl(5, name), 0);
        gone_.push_back({n->nsId, name});
        ns_.erase(name);
    }
    void stepRecreate() {
        for (const TrioGone& g : gone_) {
            if (ns_.count(g.name)) continue;
            const int64_t c = trio_.submitCreateNamespaceNoWait(leader_, g.name, {});
            ASSERT_EQ(trio_.awaitAnswer(leader_, c).code, 0) << "the name is free at once: " << g.name;
            const auto* r = rec(leader_, g.name);
            ASSERT_NE(r, nullptr);
            ASSERT_GT(r->nsId, lastId_);
            lastId_ = r->nsId;
            ns_[g.name] = TrioNs{r->nsId, 0, 0, 0, false};
            return;
        }
    }
    void stepSetLimits() {
        TrioNs* n = pickNs();
        if (n == nullptr) return;
        const bool lower = n->topics > 0 && rnd(2) == 0;
        const uint64_t v = lower ? 1 + rnd(static_cast<uint32_t>(n->topics)) : 1000;
        ASSERT_EQ(ddl(2, nameOf(n), 1, 0, v), 0);
        n->maxTopics = v;
        n->limited = lower;
    }
    void stepLeaderChange() {
        int32_t to = leader_;
        while (to == leader_) to = static_cast<int32_t>(1 + rnd(3));
        trio_.moveCatalogLeadership(to);
        ASSERT_TRUE(trio_.awaitRole(to, "LeaderServing", std::chrono::seconds(10))) << "no leader serving on " << to;
        leader_ = to;
        trace_ += " leader->" + std::to_string(to);
    }

    void check() {
        ASSERT_TRUE(trio_.awaitConverged());
        for (const int32_t b : {1, 2, 3}) {
            for (const auto& [name, n] : ns_) {
                const auto* r = rec(b, name);
                ASSERT_NE(r, nullptr) << name << " missing on broker " << b;
                ASSERT_EQ(r->nsId, n.nsId) << name << " on broker " << b;
                ASSERT_EQ(static_cast<uint8_t>(rune::catalog::tagOf(r->state)), n.state) << name << " state on broker " << b;
                ASSERT_EQ(r->limits.maxTopics, n.maxTopics) << name << " maxTopics on broker " << b;
            }
            for (const TrioGone& g : gone_) {
                const auto* byId = trio_.admin(b).applier().state().namespaces().byId(g.nsId);
                if (byId != nullptr) ASSERT_EQ(rune::catalog::tagOf(byId->state), rune::catalog::NsStateTag::Deleting)
                    << "deleted id " << g.nsId << " is live on broker " << b;
            }
        }
        ASSERT_TRUE(trio_.allDigestsEqual());
    }
    void finish() {
        for (const TrioGone& g : gone_)
            for (const int32_t b : {1, 2, 3}) {
                for (int i = 0; i < CatalogTrio::kCap && !erasedOn(b, g.nsId); ++i) trio_.pump();
                ASSERT_TRUE(erasedOn(b, g.nsId)) << "deleted id " << g.nsId << " never erased on broker " << b;
            }
        check();
    }

    void run() {
        const int steps = []{ int n = 60; if (const char* e = std::getenv("RUNE_NS_STRESS_STEPS")) n = std::min(std::atoi(e), 200); return std::max(n, 1); }();
        for (int step = 1; step <= steps; ++step) {
            trace_.clear();
            const uint32_t roll = rnd(100);
            const char* op = "create";
            if (roll < 15) op = "create";
            else if (roll < 35) op = "topic";
            else if (roll < 48) op = "suspend";
            else if (roll < 60) op = "resume";
            else if (roll < 70) op = "delete";
            else if (roll < 80) op = "recreate";
            else if (roll < 90) op = "set-limits";
            else op = "leader-change";
            SCOPED_TRACE("seed=" + std::to_string(seed_) + " step=" + std::to_string(step) + " op=" + op);
            if (!std::strcmp(op, "create")) stepCreate(step);
            else if (!std::strcmp(op, "topic")) stepTopic(step);
            else if (!std::strcmp(op, "suspend")) stepSuspend();
            else if (!std::strcmp(op, "resume")) stepResume();
            else if (!std::strcmp(op, "delete")) stepDelete();
            else if (!std::strcmp(op, "recreate")) stepRecreate();
            else if (!std::strcmp(op, "set-limits")) stepSetLimits();
            else stepLeaderChange();
            if (!HasFatalFailure() && !HasNonfatalFailure()) check();
            if (HasFatalFailure() || HasNonfatalFailure()) {
                ADD_FAILURE() << "seed=" << seed_ << " step=" << step << " op=" << op << trace_;
                return;
            }
        }
        finish();
    }

    uint32_t seed_;
    std::mt19937 rng_;
    CatalogTrio trio_;
    int32_t leader_ = 1;
    int seq_ = 0;
    uint32_t lastId_ = 0;
    std::map<std::string, TrioNs> ns_;
    std::vector<TrioGone> gone_;
    std::string trace_;
};

TEST_P(NamespaceLifecycleStressTrioRegression, SeededModelRegression) { run(); }

INSTANTIATE_TEST_SUITE_P(Seeds, NamespaceLifecycleStressTrioRegression, ::testing::Range(1u, 7u));

}  // namespace rune::node::test
