// NamespaceTrioTest.cpp - namespaces Task 14 (spec §8): the 3-node slice on THREE real authenticating BrokerNodes (real
// Aeron UDP, sealed peer links, default config, namespace-aware v14 sessions through the raw-frame wire client):
//   * a namespace, its users and its topics replicate to every broker (ListNamespaces, login, metadata, disk layout);
//   * metadata on a follower is filtered by the caller's namespace;
//   * a restarted follower catches up the namespaces it missed;
//   * the matrix's 14 LeaderChange timing cells (7 DDL ops x {ClusterAdmin, NsAdminSame}, OwnNs): the leader that
//     orders the op commits it, its answer is held, it is killed, and the SAME request id retried on the new leader
//     answers the committed outcome (same object id) with the effect applied exactly once.
// Convergence comes from ISR + idempotent restore; every wait is a capped poll, never a tuned sleep.
//
// Interaction matrix (this file's slice):
//   | operation (7 DDL ops)                  | caller              | timing       | expected                                     | test
//   |----------------------------------------|---------------------|--------------|----------------------------------------------|------
//   | CreateNamespace .. Grant               | root, a/admin       | leader kill  | retry (same rid) = committed outcome, once   | LeaderChangeMidCreateConvergesRegression
//   | create ns / user / topic               | root                | sequential   | visible on every node, nsId equal            | CreateReplicates / LoginOnAnyNode / DiskLayout
//   | create ns / user / topic               | root                | follower down| the follower catches up on restart           | FollowerRestartCatchesUp
//   | metadata                               | a/alice, root       | sequential   | alice: a/t only; root: all                   | MetadataFilteredOnFollower
#include "support/AuthRig.h"
#include "support/AuthTrio.h"
#include "support/NamespaceMatrixExpectations.h"

#include <rune/node/NamespacePaths.h>
#include <rune/protocol/client/NamespaceMessages.h>

#include <gtest/gtest.h>

#include <algorithm>
#include <cstdlib>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <vector>

using namespace rune::node;
using namespace rune::node::test;
using namespace rune::node::test::nsmatrix;
using namespace std::chrono_literals;
namespace pc = rune::protocol::client;
using SO = rune::protocol::SecurityOperation;
using ST = rune::protocol::SecurityResourceType;
using SP = rune::protocol::SecurityPatternKind;
using RequestId = rune::protocol::RequestId;

namespace {

constexpr int kNoAns = 0xFFFF;

struct Ans {
    int code = kNoAns;
    uint64_t id = 0;
    std::string detail;
};

/// One DDL of the seven the timing cells cover (the encoders mirror NamespaceRig::sendTimed).
struct Req {
    Op op = Op::CreateTopic;
    std::string name;
    uint64_t arg = 0;     ///< SetNamespaceLimits: maxTopics
    RequestId rid{};      ///< none = a fresh id per send
    int32_t src = 0;      ///< CreateView: the source topic id
};

std::string localOf(const std::string& name) {
    const auto slash = name.rfind('/');
    return slash == std::string::npos ? name : name.substr(slash + 1);
}
bool has(const std::vector<int32_t>& v, int32_t x) { return std::find(v.begin(), v.end(), x) != v.end(); }

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
std::vector<AuthGrant> nsAdminGrants(const std::string& ns) {
    auto g = dataGrants();
    g.push_back(grants::exact(SO::Admin, ST::Namespace, ns));
    return g;
}

/// A 3-broker authenticating cluster plus namespace-aware (client version 14) sessions of any user on any broker.
class World {
public:
    explicit World(const std::string& tag) : t(tag) {
        t.start();
        t.bootstrapAdmin();
        t.wire().raw().captureRawAnswers(true);   // ListNamespaces answers are read as raw frames
    }
    AuthTrio t;
    int32_t tA = -1;   ///< topic id of a/t

    static std::string pwOf(const std::string& user) { return user == "admin" ? AuthTrio::kAdminPassword : AuthTrio::kPw; }

    // ── sessions (replaced when idle for half the broker's session timeout, dropped with their broker) ──────────
    int64_t sid(const std::string& user, int node) {
        Slot& s = sess_[{user, node}];
        const auto now = Clock3::now();
        if (s.sid == 0 || now - s.used > 5s) {
            s.sid = 0;
            int64_t got = 0;
            if (!AuthTrio::until(30s, [&] {
                    const int64_t c = t.wire().connect(t.channel(node), pc::kClientVersionNamespaces);
                    if (c <= 0) return false;
                    if (!t.wire().login(t.channel(node), c, user, {pwOf(user), pwOf(user)}).ok()) return false;
                    got = c;
                    return true;
                }))
                throw std::runtime_error("World: login of " + user + " on node " + std::to_string(node) + " never accepted");
            s.sid = got;
        }
        s.used = now;
        return s.sid;
    }
    void forget(int node) {
        for (auto it = sess_.begin(); it != sess_.end();) it = it->first.second == node ? sess_.erase(it) : std::next(it);
    }
    void stop(int node) { forget(node); t.stopNode(node); }
    void restart(int node) { forget(node); t.restart(node, t.keyContentsPath(node)); }
    ShardTestClient& raw() { return t.wire().raw(); }

    // ── requests ──────────────────────────────────────────────────────────────────────────────────────────────
    int64_t send(int node, int64_t sid, const Req& r) {
        auto& c = raw();
        const std::string ch = t.channel(node);
        constexpr int32_t st = AuthWireClient::kClientStream;
        RequestId rid = r.rid;
        if (rid.none()) rid = RequestId{0xB0C0, ++rid_};
        switch (r.op) {
            case Op::CreateNamespace: case Op::SetNamespaceLimits: {
                pc::NamespaceDdlRequest m;
                m.sessionId = sid;
                m.correlationId = c.nextCorrelationId();
                m.ext.clientName = "ns-trio";
                m.ext.requestId = rid;
                m.kind = r.op == Op::CreateNamespace ? pc::NamespaceDdlKind::Create : pc::NamespaceDdlKind::SetLimits;
                m.name = r.name;
                if (r.op == Op::SetNamespaceLimits) { m.presentMask[0] = 1; m.limits.maxTopics = r.arg; }
                std::vector<uint8_t> buf(m.encodedSize());
                buf.resize(m.encode(buf.data()));
                return c.send(ch, st, buf.data(), buf.size()) ? m.correlationId : 0;
            }
            case Op::CreateTopic:
                return c.sendDefineTopic(ch, st, sid, r.name, 1, authRigCharBlob(), 0, rid, 0, authRigTopicConfig());
            case Op::DropTopic: return c.sendDropTopic(ch, st, sid, r.name, rid);
            case Op::CreateView:
                return c.sendCreateView(ch, st, sid, r.name, "id", 0, authRigViewPlan(localOf(r.name), {r.src}), rid);
            case Op::CreateUser: case Op::Grant: {
                if (!ver_) ver_ = AuthWireClient::verifierOf(AuthTrio::kPw);   // ONE verifier: a retry is byte-identical
                pc::SecurityDdl m;
                m.sessionId = sid;
                m.correlationId = c.nextCorrelationId();
                m.ext.clientName = "ns-trio";
                m.ext.requestId = rid;
                std::string stmt, pattern;
                if (r.op == Op::CreateUser) {
                    m.kind = pc::SecurityOpKind::CreateUser;
                    m.name = r.name;
                    m.verifier = ver_->wire();
                    stmt = "CREATE USER " + r.name + " WITH PASSWORD '<redacted>';";
                } else {
                    pattern = "g1";
                    m.kind = pc::SecurityOpKind::Grant;
                    m.name = r.name;
                    m.granteeKind = 0;
                    m.op = static_cast<uint8_t>(SO::Read);
                    m.resourceType = static_cast<uint8_t>(ST::Topic);
                    m.patternKind = static_cast<uint8_t>(SP::Prefix);
                    m.pattern = pattern;
                    stmt = "GRANT ...;";
                }
                m.ext.statement = stmt;
                std::vector<uint8_t> buf(m.encodedSize());
                buf.resize(m.encode(buf.data()));
                return !buf.empty() && c.send(ch, st, buf.data(), buf.size()) ? m.correlationId : 0;
            }
            default: return 0;
        }
    }
    Ans await(int64_t corr, std::chrono::milliseconds within) {
        Ans a;
        if (corr == 0) { a.detail = "send failed"; return a; }
        auto& c = raw();
        const auto any = c.awaitAnyDdlAnswer(corr, Clock3::now() + within);
        if (!any) { a.detail = "no answer"; return a; }
        a.code = any->code;
        a.detail = any->message;
        const auto now = Clock3::now() + 1s;   // the answer is already in: the lookup returns at once
        if (any->responseTemplate == static_cast<uint16_t>(pc::TemplateId::DefineTopicResponse)) {
            if (const auto d = c.awaitDefine(corr, now)) a.id = static_cast<uint64_t>(d->topicId);
        } else if (any->responseTemplate == static_cast<uint16_t>(pc::TemplateId::CreateViewResponse)) {
            if (const auto d = c.awaitCreateView(corr, now)) a.id = static_cast<uint64_t>(d->viewTopicId);
        } else if (any->responseTemplate == static_cast<uint16_t>(pc::TemplateId::DdlResponse)) {
            if (const auto d = c.awaitDdl(corr, now)) a.id = d->objectId;
        }
        return a;
    }
    /// `r` as `user`, tried on `prefer` first and re-sent to the other brokers on Redirect (3) / NotLeader (2) / not
    /// current (48); a dead session (no answer / 44 / 46) is replaced.  Bounded.
    Ans call(const std::string& user, int prefer, const Req& r, std::chrono::milliseconds within = 60s) {
        const auto deadline = Clock3::now() + within;
        Ans last;
        for (;;) {
            for (int k = 0; k < 3; ++k) {
                const int id = (prefer - 1 + k) % 3 + 1;
                if (!t.cluster().running(id)) continue;
                int64_t s = 0;
                try { s = sid(user, id); } catch (const std::exception&) { continue; }
                last = await(send(id, s, r), 15s);
                if (last.code == kNoAns || last.code == 44 || last.code == 46) { forget(id); continue; }
                if (last.code != 2 && last.code != 3 && last.code != 48) return last;
            }
            if (Clock3::now() >= deadline) return last;
            std::this_thread::sleep_for(50ms);
        }
    }

    // ── reads ─────────────────────────────────────────────────────────────────────────────────────────────────
    std::optional<std::vector<pc::NamespaceInfo>> listNs(const std::string& user, int node) {
        try {
            const int64_t s = sid(user, node);
            auto& c = raw();
            pc::ListNamespaces m;
            m.sessionId = s; m.correlationId = c.nextCorrelationId(); m.afterNsId = 0; m.max = 100;
            std::vector<uint8_t> buf(m.encodedSize());
            buf.resize(m.encode(buf.data()));
            if (!c.send(t.channel(node), AuthWireClient::kClientStream, buf.data(), buf.size())) return std::nullopt;
            const auto raw_ = c.awaitRaw(m.correlationId, Clock3::now() + 5s);
            if (!raw_) return std::nullopt;
            const auto h = pc::MessageHeader::decode(raw_->data(), raw_->size());
            if (!h || h->templateId != pc::ListNamespacesResponse::TEMPLATE_ID) return std::nullopt;
            const auto l = pc::ListNamespacesResponse::decode(raw_->data(), raw_->size());
            if (!l) return std::nullopt;
            return l->namespaces;
        } catch (const std::exception&) { return std::nullopt; }
    }
    /// nsId of `name` as `user` lists it on `node`; -1 = not listed / no answer.
    int64_t nsIdOf(const std::string& user, int node, const std::string& name) {
        const auto l = listNs(user, node);
        if (!l) return -1;
        for (const auto& i : *l) if (i.name == name) return i.nsId;
        return -1;
    }
    int countNs(const std::string& user, int node, const std::string& name) {
        int n = 0;
        if (const auto l = listNs(user, node)) for (const auto& i : *l) n += i.name == name;
        return n;
    }
    int64_t maxTopicsOf(const std::string& user, int node, const std::string& name) {
        if (const auto l = listNs(user, node))
            for (const auto& i : *l) if (i.name == name) return static_cast<int64_t>(i.limits.maxTopics);
        return -1;
    }
    std::vector<int32_t> ids(const std::string& user, int node) {
        std::vector<int32_t> v;
        try {
            const int64_t s = sid(user, node);
            for (const PartitionInfo& p : raw().metadata(t.channel(node), AuthWireClient::kClientStream, s))
                if (!has(v, p.topicId)) v.push_back(p.topicId);
        } catch (const std::exception&) {}
        std::sort(v.begin(), v.end());
        return v;
    }

    int firstNodeOtherThan(int a, int b = 0) const {
        for (int i = 1; i <= 3; ++i) if (i != a && i != b) return i;
        return 0;
    }

private:
    struct Slot { int64_t sid = 0; Clock3::time_point used{}; };
    std::map<std::pair<std::string, int>, Slot> sess_;
    uint64_t rid_ = 0;
    std::optional<AuthWireClient::Verifier> ver_;
};

/// namespace `a` (nsId 1) with users a/admin (namespace admin) and a/alice (data grants), and the typed topic a/t,
/// replicated to every broker.
std::unique_ptr<World> setupWorld(const std::string& tag) {
    auto w = std::make_unique<World>(tag);
    const Ans ns = w->call("admin", 1, Req{Op::CreateNamespace, "a"});
    if (ns.code != 0 || ns.id != 1) throw std::runtime_error("setupWorld: CREATE NAMESPACE a: " + std::to_string(ns.code) + " id " + std::to_string(ns.id) + " " + ns.detail);
    w->t.createUser(1, "a/admin");
    for (const AuthGrant& g : nsAdminGrants("a")) w->t.grant(1, "a/admin", g.op, g.type, g.kind, g.pattern);
    w->t.createUser(1, "a/alice");
    for (const AuthGrant& g : dataGrants()) w->t.grant(1, "a/alice", g.op, g.type, g.kind, g.pattern);
    const Ans tp = w->call("admin", 1, Req{Op::CreateTopic, "a/t"});
    if (tp.code != 0) throw std::runtime_error("setupWorld: topic a/t: " + std::to_string(tp.code) + " " + tp.detail);
    w->tA = static_cast<int32_t>(tp.id);
    if (!w->t.awaitDigestsAgree(0, 30s)) throw std::runtime_error("setupWorld: security digests never agreed");
    for (int n = 1; n <= 3; ++n)
        if (!AuthTrio::until(30s, [&] { return has(w->ids("admin", n), w->tA); }))
            throw std::runtime_error("setupWorld: a/t never listed on node " + std::to_string(n));
    return w;
}

}  // namespace

// ── the 3-node slice ─────────────────────────────────────────────────────────────────────────────────────────────

TEST(NamespaceTrioRegression, CreateReplicatesRegression) {
    auto w = setupWorld("nstrio_repl");
    for (int n = 1; n <= 3; ++n)
        EXPECT_TRUE(AuthTrio::until(30s, [&] { return w->nsIdOf("admin", n, "a") == 1; })) << "root lists namespace a (1) on node " << n;
}

TEST(NamespaceTrioRegression, LoginOnAnyNodeRegression) {
    auto w = setupWorld("nstrio_login");
    for (int n = 1; n <= 3; ++n) {
        // a/alice's session is namespace 1: the one namespace a non-admin sees is its own (D15)
        std::optional<std::vector<pc::NamespaceInfo>> l;
        ASSERT_TRUE(AuthTrio::until(30s, [&] { l = w->listNs("a/alice", n); return l.has_value() && !l->empty(); })) << "node " << n;
        ASSERT_EQ(l->size(), 1u) << "node " << n;
        EXPECT_EQ((*l)[0].nsId, 1u) << "node " << n;
        EXPECT_EQ((*l)[0].name, "a") << "node " << n;
        EXPECT_TRUE(AuthTrio::until(30s, [&] { return has(w->ids("a/alice", n), w->tA); })) << "alice lists a/t on node " << n;
    }
}

TEST(NamespaceTrioRegression, MetadataFilteredOnFollowerRegression) {
    auto w = setupWorld("nstrio_meta");
    const Ans dt = w->call("admin", 1, Req{Op::CreateTopic, "dt"});
    ASSERT_EQ(dt.code, 0) << dt.detail;
    ASSERT_EQ(w->call("admin", 1, Req{Op::CreateNamespace, "b"}).code, 0);
    const Ans bt = w->call("admin", 1, Req{Op::CreateTopic, "b/t"});
    ASSERT_EQ(bt.code, 0) << bt.detail;
    const int cl = w->t.cluster().settledLeader();
    ASSERT_GT(cl, 0);
    const int f = w->firstNodeOtherThan(cl);
    const auto dtId = static_cast<int32_t>(dt.id), btId = static_cast<int32_t>(bt.id);
    ASSERT_TRUE(AuthTrio::until(30s, [&] {
        const auto r = w->ids("admin", f);
        return has(r, w->tA) && has(r, dtId) && has(r, btId);
    })) << "root lists all three on follower " << f;
    const auto alice = w->ids("a/alice", f);
    EXPECT_TRUE(has(alice, w->tA));
    EXPECT_FALSE(has(alice, dtId)) << "alice never sees a default topic";
    EXPECT_FALSE(has(alice, btId)) << "alice never sees another namespace's topic";
    EXPECT_EQ(alice.size(), 1u);
}

TEST(NamespaceTrioRegression, FollowerRestartCatchesUpNamespacesRegression) {
    auto w = setupWorld("nstrio_catchup");
    const int cl = w->t.cluster().settledLeader();
    const int sl = security_cluster::settledSecurityLeader(w->t.cluster());
    ASSERT_GT(cl, 0);
    ASSERT_GT(sl, 0);
    const int f = w->firstNodeOtherThan(cl, sl);
    ASSERT_GT(f, 0);
    w->stop(f);
    const Ans b = w->call("admin", cl, Req{Op::CreateNamespace, "b"});
    ASSERT_EQ(b.code, 0) << b.detail;
    EXPECT_EQ(b.id, 2u);
    const Ans bt = w->call("admin", cl, Req{Op::CreateTopic, "b/t"});
    ASSERT_EQ(bt.code, 0) << bt.detail;
    w->t.createUser(sl, "b/bob");
    w->restart(f);
    ASSERT_TRUE(AuthTrio::until(60s, [&] { return w->nsIdOf("admin", f, "a") == 1 && w->nsIdOf("admin", f, "b") == 2; }))
        << "the restarted follower lists a=1, b=2";
    std::optional<std::vector<pc::NamespaceInfo>> l;
    ASSERT_TRUE(AuthTrio::until(60s, [&] { l = w->listNs("b/bob", f); return l.has_value() && !l->empty(); })) << "b/bob logs in on the restarted follower";
    ASSERT_EQ(l->size(), 1u);
    EXPECT_EQ((*l)[0].nsId, 2u);
    EXPECT_TRUE(AuthTrio::until(60s, [&] { return has(w->ids("admin", f), static_cast<int32_t>(bt.id)); })) << "root lists b/t on the restarted follower";
}

TEST(NamespaceTrioRegression, DiskLayoutOnEveryNodeRegression) {
    auto w = setupWorld("nstrio_disk");
    for (int n = 1; n <= 3; ++n) {
        const auto dir = topicsRoot(w->t.cluster().dirs[static_cast<std::size_t>(n - 1)]->path, 1) / "t";
        EXPECT_TRUE(AuthTrio::until(30s, [&] { return std::filesystem::exists(dir); })) << "node " << n << ": " << dir.filename().string() << " under ns-1/topics";
    }
}

// ── the 14 LeaderChange timing cells ─────────────────────────────────────────────────────────────────────────────

namespace {

struct LCell { Op op; Caller caller; };
std::vector<LCell> leaderChangeCells() {
    std::vector<LCell> v;
    for (std::size_t o = 0; o < kOps; ++o)
        for (const Caller c : {Caller::ClusterAdmin, Caller::NsAdminSame})
            if (isTimingCell(Op(o), c, Target::OwnNs, Timing::LeaderChange)) v.push_back({Op(o), c});
    return v;
}
std::string lcName(const LCell& c) { return std::string(name(c.op)) + "_" + name(c.caller); }

/// What a repeat of an applied first op answers (as NamespaceMatrixTest's dupCode).
int dupCode(Op o) {
    switch (o) {
        case Op::CreateNamespace: case Op::CreateTopic: return 25;
        case Op::CreateView: return 32;
        case Op::CreateUser: case Op::Grant: return 39;
        case Op::DropTopic: return 7;
        default: return 0;   // SetNamespaceLimits
    }
}
/// How many events of `n`'s `__security` log carry request id `rid` (its first word, little endian, is stored once per
/// event that carries the id).
int countInLog(BrokerNode& n, const RequestId& rid) {
    return n.onAdminThreadForTest([&n, &rid]() -> int {
        const std::vector<uint8_t> bytes = n.securityLogForTest()->read(0, 1 << 22);
        const std::string_view hay(reinterpret_cast<const char*>(bytes.data()), bytes.size());
        char needle[8];
        for (int i = 0; i < 8; ++i) needle[i] = static_cast<char>((rid.hi >> (8 * i)) & 0xFF);
        const std::string_view nd(needle, 8);
        int c = 0;
        for (std::size_t p = hay.find(nd); p != std::string_view::npos; p = hay.find(nd, p + 1)) ++c;
        return c;
    });
}
bool securityOp(Op o) { return o == Op::CreateUser || o == Op::Grant; }

}  // namespace

TEST(NamespaceTrioCountRegression, LeaderChangeCellCountRegression) {
    EXPECT_EQ(leaderChangeCells().size(), 14u);
    EXPECT_EQ(countCells().timing, 56u);   // 42 one-node cells (Task 12b) + these 14
}

class NamespaceTrioLeaderChangeRegression : public ::testing::TestWithParam<LCell> {};

TEST_P(NamespaceTrioLeaderChangeRegression, LeaderChangeMidCreateConvergesRegression) {
    const LCell& p = GetParam();
    const bool root = p.caller == Caller::ClusterAdmin;
    const std::string label = lcName(p);
    const Outcome first = expect(p.op, p.caller, isCreateOp(p.op) ? Target::Missing : Target::OwnNs);
    const bool ok = first.kind == Kind::Ok;
    const int wantCode = ok ? 0 : first.code;
    const int dup = ok ? dupCode(p.op) : first.code;

    auto w = setupWorld("nstrio_lc_" + label);
    World& W = *w;
    AuthTrio& t = W.t;
    const std::string user = root ? "admin" : "a/admin";
    auto qa = [&](const std::string& local) { return root ? "a/" + local : local; };
    constexpr uint64_t lim = 777;

    // the request and what it creates (qualified for verification)
    Req req;
    req.op = p.op;
    req.rid = RequestId{0xB2B, 14};
    std::string full;
    int32_t dropId = -1;
    switch (p.op) {
        case Op::CreateNamespace: req.name = "n1"; full = "n1"; break;
        case Op::SetNamespaceLimits: req.name = "a"; req.arg = lim; full = "a"; break;
        case Op::CreateTopic: req.name = qa("k1"); full = "a/k1"; break;
        case Op::DropTopic: req.name = qa("d1"); full = "a/d1"; break;
        case Op::CreateView: req.name = qa("kv1"); full = "a/kv1"; req.src = W.tA; break;
        case Op::CreateUser: req.name = qa("u1"); full = "a/u1"; break;
        case Op::Grant: req.name = qa("x"); full = "a/x"; break;
        default: FAIL() << "not a timing op";
    }
    // pre-create what the op needs
    if (p.op == Op::DropTopic) {
        const Ans d = W.call("admin", 1, Req{Op::CreateTopic, "a/d1"});
        ASSERT_EQ(d.code, 0) << d.detail;
        dropId = static_cast<int32_t>(d.id);
        for (int n = 1; n <= 3; ++n)
            ASSERT_TRUE(AuthTrio::until(30s, [&] { return has(W.ids("admin", n), dropId); })) << "a/d1 listed on node " << n;
    }
    if (p.op == Op::Grant) t.createUser(1, "a/x");
    ASSERT_TRUE(t.awaitDigestsAgree(0, 30s));

    // a. L orders the op
    const bool sec = securityOp(p.op);
    auto leaderNow = [&]() -> int {
        return sec ? security_cluster::settledSecurityLeader(t.cluster()) : t.cluster().settledLeader();
    };
    ASSERT_TRUE(AuthTrio::until(60s, [&] { return leaderNow() > 0; }));
    const int L = leaderNow();
    const int S = W.firstNodeOtherThan(L);
    const int S2 = W.firstNodeOtherThan(L, S);
    const std::vector<int32_t> ids0 = W.ids("admin", S);
    const int64_t secBefore = t.node(S).securityViewForTest().appliedOffset;

    // b. hold L's answer; the op is committed behind it
    const int64_t sidL = W.sid(user, L);
    if (sec) t.node(L).holdSecurityMirrorForTest(true); else t.node(L).holdCatalogAnswersForTest(true);
    const int64_t corr = W.send(L, sidL, req);
    ASSERT_NE(corr, 0) << label << ": send";
    const auto early = W.raw().awaitAnyDdlAnswer(corr, Clock3::now() + 2s);
    if (ok) {
        EXPECT_FALSE(early.has_value()) << label << ": the held leader never answers";
    } else if (early) {
        EXPECT_EQ(early->code, first.code) << label << ": the refusal [" << early->message << "]";
    }

    // c. every other node applied it; the id the committed op gave its object, read on a survivor
    uint64_t committedId = 0;
    if (ok) {
        for (const int n : {S, S2}) {
            switch (p.op) {
                case Op::CreateNamespace:
                    ASSERT_TRUE(AuthTrio::until(60s, [&] { return W.nsIdOf("admin", n, "n1") > 0; })) << label << ": n1 on node " << n;
                    committedId = static_cast<uint64_t>(W.nsIdOf("admin", n, "n1"));
                    break;
                case Op::SetNamespaceLimits:
                    ASSERT_TRUE(AuthTrio::until(60s, [&] { return W.maxTopicsOf("admin", n, "a") == static_cast<int64_t>(lim); })) << label << ": limit on node " << n;
                    break;
                case Op::CreateTopic: case Op::CreateView:
                    ASSERT_TRUE(AuthTrio::until(60s, [&] { return W.ids("admin", n).size() > ids0.size(); })) << label << ": object on node " << n;
                    for (const int32_t id : W.ids("admin", n)) if (!has(ids0, id)) committedId = static_cast<uint64_t>(id);
                    break;
                case Op::DropTopic:
                    ASSERT_TRUE(AuthTrio::until(60s, [&] { return !has(W.ids("admin", n), dropId); })) << label << ": drop on node " << n;
                    break;
                case Op::CreateUser:   // securityUserIdForTest reads `default` only: the applied offset stands in
                case Op::Grant:
                    ASSERT_TRUE(AuthTrio::until(60s, [&] { return t.node(n).securityViewForTest().appliedOffset > secBefore; })) << label << ": grant on node " << n;
                    break;
                default: break;
            }
        }
    }

    // d. kill L; a new leader of that log
    t.node(L).crashForTest();
    W.stop(L);
    ASSERT_TRUE(AuthTrio::until(60s, [&] { const int l = leaderNow(); return l > 0 && l != L; })) << label << ": a new leader";
    const int L2 = leaderNow();

    // e. the SAME request (same bytes, same request id) on the new leader
    const Ans again = W.call(user, L2, req);
    EXPECT_EQ(again.code, wantCode) << label << ": the retry [" << again.detail << "]";
    if (ok && committedId != 0 && (p.op == Op::CreateNamespace || p.op == Op::CreateTopic || p.op == Op::CreateView))
        EXPECT_EQ(again.id, committedId) << label << ": the retry is answered with the id of the ONE committed op";

    // f. the effect happened once: the same op under a NEW request id answers the duplicate code
    Req fresh = req;
    fresh.rid = RequestId{};
    const Ans third = W.call(user, L2, fresh);
    EXPECT_EQ(third.code, dup) << label << ": the effect was not applied exactly once [" << third.detail << "]";
    if (ok && p.op == Op::SetNamespaceLimits)
        EXPECT_EQ(W.maxTopicsOf("admin", L2, "a"), static_cast<int64_t>(lim)) << label;

    if (ok && sec) {
        ASSERT_TRUE(t.awaitDigestsAgree(secBefore + 1, 30s));
        for (BrokerNode* n : t.cluster().live())
            EXPECT_EQ(countInLog(*n, req.rid), 1) << label << ": node " << n->nodeIdForTest() << ": exactly one event for the request";
    }

    // g. L comes back and holds it exactly once
    if (ok) {
        W.restart(L);
        ASSERT_TRUE(t.awaitDigestsAgree(sec ? secBefore + 1 : 0, 60s)) << label << ": digests agree after the restart";
        switch (p.op) {
            case Op::CreateNamespace:
                EXPECT_TRUE(AuthTrio::until(60s, [&] { return W.countNs("admin", L, "n1") == 1; })) << label;
                EXPECT_EQ(W.nsIdOf("admin", L, "n1"), static_cast<int64_t>(committedId)) << label;
                break;
            case Op::SetNamespaceLimits:
                EXPECT_TRUE(AuthTrio::until(60s, [&] { return W.maxTopicsOf("admin", L, "a") == static_cast<int64_t>(lim); })) << label;
                break;
            case Op::CreateTopic: case Op::CreateView: {
                EXPECT_TRUE(AuthTrio::until(60s, [&] { return has(W.ids("admin", L), static_cast<int32_t>(committedId)); })) << label << ": the object on the restarted leader";
                EXPECT_EQ(W.ids("admin", L).size(), W.ids("admin", L2).size()) << label << ": exactly once";
                break;
            }
            case Op::DropTopic:
                EXPECT_TRUE(AuthTrio::until(60s, [&] { const auto v = W.ids("admin", L); return !v.empty() && !has(v, dropId); })) << label;
                break;
            default: break;   // CreateUser / Grant: the digests agree and the log holds the request once (below)
        }
        if (sec) EXPECT_EQ(countInLog(t.node(L), req.rid), 1) << label << ": the restarted leader's log holds the request once";
    }
}

INSTANTIATE_TEST_SUITE_P(All, NamespaceTrioLeaderChangeRegression, ::testing::ValuesIn(leaderChangeCells()),
                         [](const auto& info) { return lcName(info.param); });
