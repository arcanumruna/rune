// NamespaceLifecycleTrioRegressionTest.cpp - namespaces Phase 2 Task 19 (spec §8): the lifecycle on THREE real
// authenticating BrokerNodes (real Aeron UDP, sealed peer links, default config; AuthTrio.h): suspend closes the
// namespace's sessions on every node; a delete finishes after the `__catalog` leader is killed mid-delete; a follower that
// missed the whole delete converges when it rejoins; connection and rate limits apply per node; acks=all loses nothing
// across a suspend / resume cycle.  Convergence comes from the registry (rule 16): every wait is a capped poll on
// observable state, never a tuned sleep.
//
// Interaction matrix (this file's slice; the Op x Caller x Target matrix is NamespaceLifecycleMatrixTest):
//   | operation            | state / timing                         | nodes | expected                                           | test
//   |----------------------|----------------------------------------|-------|----------------------------------------------------|-----
//   | suspend RejectAll    | a session of `a` on each node          | 3     | all three closed; logins 51; resume restores       | SuspendConvergesOnEveryNode
//   | suspend ReadOnly     | a session of `a` on each node          | 3     | DDL refused 51 on each node; resume restores       | SuspendConvergesOnEveryNode
//   | delete               | the catalog leader killed after 2 steps| 3     | the new leader finishes; dirs gone on every node   | DeleteFinishesAfterLeaderKill
//   | delete               | a follower down for the whole delete   | 3     | it rejoins and converges (registry, dirs, users)   | FollowerRejoinsAfterDelete
//   | SetLimits            | maxConnections=1, produceBytesPerSec   | 3     | per node: one session each; limit on every worker; | ThrottleOnEveryNode
//   |                      |                                        |       | Throttled only where the partition leads           |
//   | produce acks=all     | across suspend RO / resume             | 3     | produced = consumed = every replica's log          | AcksAllNoLossAcrossSuspendResume
#include "support/AuthRig.h"
#include "support/AuthTrio.h"

#include <rune/client/Client.h>
#include <rune/client/Completions.h>
#include <rune/client/producer/Producer.h>
#include <rune/client/subscriber/Subscription.h>
#include <rune/node/NamespacePaths.h>
#include <rune/node/Worker.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/protocol/client/NamespaceMessages.h>

#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <filesystem>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <vector>

using namespace rune::node;
using namespace rune::node::test;
using namespace std::chrono_literals;
namespace pc = rune::protocol::client;
namespace rc = rune::client;
using SO = rune::protocol::SecurityOperation;
using ST = rune::protocol::SecurityResourceType;
using SP = rune::protocol::SecurityPatternKind;
using RequestId = rune::protocol::RequestId;

namespace {

constexpr int kNoAns = 0xFFFF;
constexpr int kSuspended = static_cast<int>(pc::ErrorCode::NamespaceSuspended);
constexpr int kLimit = static_cast<int>(pc::ErrorCode::NamespaceLimitExceeded);
constexpr int kThrottled = static_cast<int>(pc::ErrorCode::Throttled);
constexpr int kAuthFailed = static_cast<int>(pc::ErrorCode::AuthenticationFailed);

struct Ans { int code = kNoAns; uint64_t id = 0; std::string detail; };

bool has(const std::vector<int32_t>& v, int32_t x) { return std::find(v.begin(), v.end(), x) != v.end(); }

std::vector<AuthGrant> dataGrants() {
    using namespace grants;
    std::vector<AuthGrant> g;
    for (const SO op : {SO::Read, SO::Write, SO::Describe, SO::Create, SO::Alter, SO::Drop})
        g.push_back(prefix(op, ST::Topic, ""));
    g.push_back(prefix(SO::Read, ST::Group, ""));
    return g;
}

std::vector<uint8_t> bigBatch(std::size_t valueBytes) {
    rune::protocol::RecordBatchBuilder b(0, 1000);
    b.append("k", std::string(valueBytes, 'v'), 1000);
    return b.build();
}

/// A 3-broker authenticating cluster plus namespace-aware (client version 14) raw sessions of any user on any broker.
class World {
public:
    explicit World(const std::string& tag) : t(tag) {
        t.start();
        t.bootstrapAdmin();
        t.wire().raw().captureRawAnswers(true);
    }
    AuthTrio t;
    int32_t tA = -1, tA2 = -1;   ///< topic ids of a/t and a/t2

    static std::string pwOf(const std::string& user) { return user == "admin" ? AuthTrio::kAdminPassword : AuthTrio::kPw; }
    ShardTestClient& raw() { return t.wire().raw(); }

    // ── sessions ──────────────────────────────────────────────────────────────────────────────────────────────
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
    void forgetAll() { sess_.clear(); }
    void stop(int node) { forget(node); t.stopNode(node); }
    void restart(int node) { forget(node); t.restart(node, t.keyContentsPath(node)); }

    /// One login attempt, uncached: its code (0 = logged in) and session.
    struct Login { int code = kNoAns; int64_t sid = 0; };
    Login tryLogin(int node, const std::string& user) {
        const int64_t c = t.wire().connect(t.channel(node), pc::kClientVersionNamespaces);
        if (c <= 0) return {};
        const auto l = t.wire().login(t.channel(node), c, user, {pwOf(user), pwOf(user)});
        if (l.ok()) return {0, c};
        return {l.answered() ? static_cast<int>(l.errorCode()) : kNoAns, 0};
    }

    // ── requests ──────────────────────────────────────────────────────────────────────────────────────────────
    int64_t sendNs(int node, int64_t sid, pc::NamespaceDdlKind kind, const std::string& name, uint8_t mode = 1, int field = -1,
                   uint64_t value = 0) {
        pc::NamespaceDdlRequest m;
        m.sessionId = sid;
        m.correlationId = raw().nextCorrelationId();
        m.ext.clientName = "ns-lc-trio";
        m.ext.requestId = RequestId{0x1C70, ++rid_};
        m.kind = kind;
        m.name = name;
        if (kind == pc::NamespaceDdlKind::Suspend) m.mode = static_cast<pc::NsSuspendMode>(mode);
        if ((kind == pc::NamespaceDdlKind::SetLimits || kind == pc::NamespaceDdlKind::Create) && field >= 0) {
            m.presentMask[static_cast<std::size_t>(field) >> 3] = static_cast<uint8_t>(1u << (field & 7));
            (&m.limits.maxTopics)[field] = value;
        }
        std::vector<uint8_t> buf(m.encodedSize());
        buf.resize(m.encode(buf.data()));
        return raw().send(t.channel(node), AuthWireClient::kClientStream, buf.data(), buf.size()) ? m.correlationId : 0;
    }
    int64_t sendDefine(int node, int64_t sid, const std::string& name) {
        return raw().sendDefineTopic(t.channel(node), AuthWireClient::kClientStream, sid, name, 1, authRigCharBlob(), 0,
                                     RequestId{0x1C71, ++rid_}, 0, authRigTopicConfig());
    }
    Ans await(int64_t corr, std::chrono::milliseconds within) {
        Ans a;
        if (corr == 0) { a.detail = "send failed"; return a; }
        const auto any = raw().awaitAnyDdlAnswer(corr, Clock3::now() + within);
        if (!any) { a.detail = "no answer"; return a; }
        a.code = any->code;
        a.detail = any->message;
        const auto now = Clock3::now() + 1s;
        if (any->responseTemplate == static_cast<uint16_t>(pc::TemplateId::DefineTopicResponse)) {
            if (const auto d = raw().awaitDefine(corr, now)) a.id = static_cast<uint64_t>(d->topicId);
        } else if (any->responseTemplate == static_cast<uint16_t>(pc::TemplateId::DdlResponse)) {
            if (const auto d = raw().awaitDdl(corr, now)) a.id = d->objectId;
        }
        return a;
    }
    /// A request builder `f(node, sid)` as `user`, tried on `prefer` first and re-sent to the other running brokers on
    /// Redirect (3) / NotLeader (2) / not current (48); a dead session is replaced.  Bounded.
    template <class F>
    Ans call(const std::string& user, int prefer, F&& f, std::chrono::milliseconds within = 60s) {
        const auto deadline = Clock3::now() + within;
        Ans last;
        for (;;) {
            for (int k = 0; k < 3; ++k) {
                const int id = (prefer - 1 + k) % 3 + 1;
                if (!t.cluster().running(id)) continue;
                int64_t s = 0;
                try { s = sid(user, id); } catch (const std::exception&) { continue; }
                last = await(f(id, s), 15s);
                if (last.code == kNoAns || last.code == 44 || last.code == 46) { forget(id); continue; }
                if (last.code != 2 && last.code != 3 && last.code != 48) return last;
            }
            if (Clock3::now() >= deadline) return last;
            std::this_thread::sleep_for(50ms);
        }
    }
    Ans ns(int prefer, pc::NamespaceDdlKind kind, const std::string& name, uint8_t mode = 1, int field = -1, uint64_t value = 0) {
        return call("admin", prefer, [&](int n, int64_t s) { return sendNs(n, s, kind, name, mode, field, value); });
    }
    Ans defineAs(const std::string& user, int node, const std::string& name) {
        return await(sendDefine(node, sid(user, node), name), 20s);
    }

    // ── reads ─────────────────────────────────────────────────────────────────────────────────────────────────
    std::optional<std::vector<pc::NamespaceInfo>> listNs(const std::string& user, int node) {
        try {
            const int64_t s = sid(user, node);
            pc::ListNamespaces m;
            m.sessionId = s; m.correlationId = raw().nextCorrelationId(); m.afterNsId = 0; m.max = 100;
            std::vector<uint8_t> buf(m.encodedSize());
            buf.resize(m.encode(buf.data()));
            if (!raw().send(t.channel(node), AuthWireClient::kClientStream, buf.data(), buf.size())) return std::nullopt;
            const auto r = raw().awaitRaw(m.correlationId, Clock3::now() + 5s);
            if (!r) return std::nullopt;
            const auto h = pc::MessageHeader::decode(r->data(), r->size());
            if (!h || h->templateId != pc::ListNamespacesResponse::TEMPLATE_ID) return std::nullopt;
            const auto l = pc::ListNamespacesResponse::decode(r->data(), r->size());
            if (!l) return std::nullopt;
            return l->namespaces;
        } catch (const std::exception&) { return std::nullopt; }
    }
    /// The entry of nsId `id` as root lists it on `node` (nullopt: not listed / no answer).
    std::optional<pc::NamespaceInfo> entry(int node, uint32_t id) {
        const auto l = listNs("admin", node);
        if (!l) return std::nullopt;
        for (const auto& i : *l) if (i.nsId == id) return i;
        return std::nullopt;
    }
    /// True once `node` answers a listing that does not hold `id`.
    bool erasedOn(int node, uint32_t id) {
        const auto l = listNs("admin", node);
        if (!l) return false;
        for (const auto& i : *l) if (i.nsId == id) return false;
        return true;
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
    [[nodiscard]] std::filesystem::path dataDir(int node) { return t.cluster().dirs[static_cast<std::size_t>(node - 1)]->path; }
    bool dirsGone(int node, uint32_t id) {
        return !std::filesystem::exists(nsRoot(dataDir(node), id)) &&
               !std::filesystem::exists(std::filesystem::path(nsRoot(dataDir(node), id).string() + ".deleting"));
    }
    int firstNodeOtherThan(int a, int b = 0) const {
        for (int i = 1; i <= 3; ++i) if (i != a && i != b) return i;
        return 0;
    }

    /// One acks=1 produce of `valueBytes` straight to the partition leader's worker ingress; nullopt: no answer.
    std::optional<pc::ProduceResponse> produce(int64_t sid, int node, int32_t topicId, std::size_t valueBytes) {
        std::string channel;
        int32_t stream = AuthWireClient::kClientStream;
        for (const PartitionInfo& p : raw().metadata(t.channel(node), AuthWireClient::kClientStream, sid))
            if (p.topicId == topicId && p.partition == 0 && !p.ingressEndpoint.empty()) {
                channel = "aeron:udp?endpoint=" + p.ingressEndpoint;
                stream = p.ingressStreamId;
            }
        if (channel.empty()) return std::nullopt;
        const int64_t corr = raw().nextCorrelationId();
        if (!raw().produce(channel, stream, sid, corr, topicId, 0, 1, bigBatch(valueBytes))) return std::nullopt;
        return raw().awaitProduceResponse(corr);
    }
    int leaderOf(int32_t topicId) {
        for (const PartitionInfo& p : raw().metadata(t.channel(1), AuthWireClient::kClientStream, sid("admin", 1)))
            if (p.topicId == topicId && p.partition == 0) return p.leaderId;
        return -1;
    }

private:
    struct Slot { int64_t sid = 0; Clock3::time_point used{}; };
    std::map<std::pair<std::string, int>, Slot> sess_;
    uint64_t rid_ = 0;
};

/// namespace `a` (nsId 1) with users a/alice and a/bob (data grants) and the typed topics a/t and a/t2, replicated to
/// every broker.
std::unique_ptr<World> setupWorld(const std::string& tag) {
    auto w = std::make_unique<World>(tag);
    const Ans ns = w->ns(1, pc::NamespaceDdlKind::Create, "a");
    if (ns.code != 0 || ns.id != 1) throw std::runtime_error("setupWorld: CREATE NAMESPACE a: " + std::to_string(ns.code) + " " + ns.detail);
    for (const char* u : {"a/alice", "a/bob"}) {
        w->t.createUser(1, u);
        for (const AuthGrant& g : dataGrants()) w->t.grant(1, u, g.op, g.type, g.kind, g.pattern);
    }
    auto def = [&](const char* name) {
        const Ans a = w->call("admin", 1, [&](int n, int64_t s) { return w->sendDefine(n, s, name); });
        if (a.code != 0) throw std::runtime_error(std::string("setupWorld: topic ") + name + ": " + std::to_string(a.code) + " " + a.detail);
        return static_cast<int32_t>(a.id);
    };
    w->tA = def("a/t");
    w->tA2 = def("a/t2");
    if (!w->t.awaitDigestsAgree(0, 30s)) throw std::runtime_error("setupWorld: security digests never agreed");
    for (int n = 1; n <= 3; ++n)
        if (!AuthTrio::until(30s, [&] { return has(w->ids("admin", n), w->tA) && has(w->ids("admin", n), w->tA2); }))
            throw std::runtime_error("setupWorld: a/t never listed on node " + std::to_string(n));
    return w;
}

}  // namespace

// Suspend RejectAll: a session of `a` on each node; all three are closed, a login gets 51 on each, resume restores.
// Suspend ReadOnly: alice's DDL is refused 51 on each node's gate; resume restores it.
TEST(NamespaceLifecycleTrioRegression, SuspendConvergesOnEveryNodeRegression) {
    auto w = setupWorld("nslct_susp");
    World& W = *w;
    int64_t s[4] = {};
    for (int n = 1; n <= 3; ++n) s[n] = W.sid("a/alice", n);
    ASSERT_EQ(W.ns(1, pc::NamespaceDdlKind::Suspend, "a", 2).code, 0);
    for (int n = 1; n <= 3; ++n) {
        EXPECT_TRUE(AuthTrio::until(30s, [&] { return !W.t.node(n).gatewayHasSessionForTest(s[n]); }))
            << "node " << n << ": the session of the RejectAll namespace is still open";
        EXPECT_TRUE(AuthTrio::until(30s, [&] { return W.tryLogin(n, "a/alice").code == kSuspended; }))
            << "node " << n << ": a login to the RejectAll namespace must get NamespaceSuspended";
    }
    W.forgetAll();
    ASSERT_EQ(W.ns(1, pc::NamespaceDdlKind::Resume, "a").code, 0);
    for (int n = 1; n <= 3; ++n)
        EXPECT_TRUE(AuthTrio::until(30s, [&] { return W.tryLogin(n, "a/alice").code == 0; })) << "node " << n << ": login after resume";

    ASSERT_EQ(W.ns(1, pc::NamespaceDdlKind::Suspend, "a", 1).code, 0);
    for (int n = 1; n <= 3; ++n)
        EXPECT_TRUE(AuthTrio::until(30s, [&] { return W.defineAs("a/alice", n, "ro" + std::to_string(n)).code == kSuspended; }))
            << "node " << n << ": DDL in a ReadOnly namespace must get NamespaceSuspended";
    ASSERT_EQ(W.ns(1, pc::NamespaceDdlKind::Resume, "a").code, 0);
    for (int n = 1; n <= 3; ++n)
        EXPECT_EQ(W.call("a/alice", n, [&](int node, int64_t sid) { return W.sendDefine(node, sid, "ok" + std::to_string(n)); }).code, 0)
            << "node " << n << ": DDL after resume (routed to the catalog leader)";
}

// The catalog leader is killed after the reaper took 2 of its steps: the new leader's reaper re-derives from the registry
// and finishes; the namespace is erased and its directories are gone on every node, the restarted old leader included.
TEST(NamespaceLifecycleTrioRegression, DeleteFinishesAfterLeaderKillRegression) {
    auto w = setupWorld("nslct_kill");
    World& W = *w;
    const int L = W.t.cluster().settledLeader();
    ASSERT_GT(L, 0);
    BrokerNode& ln = W.t.node(L);
    ln.onAdminThreadForTest([&] { ln.adminWorkerAgentForTest()->worker().reaperForTest().setIssueBudgetForTest(2); return 0; });
    ASSERT_EQ(W.ns(L, pc::NamespaceDdlKind::Delete, "a").code, 0);
    ASSERT_TRUE(AuthTrio::until(60s, [&] {
        return ln.onAdminThreadForTest([&] { return ln.adminWorkerAgentForTest()->worker().reaper().eventsIssued(); }) == 2;
    })) << "the leader's reaper never took its 2 steps";
    for (const int n : {W.firstNodeOtherThan(L), W.firstNodeOtherThan(L, W.firstNodeOtherThan(L))})
        EXPECT_FALSE(W.erasedOn(n, 1)) << "node " << n << ": the budget should have stopped the delete before the end";
    W.stop(L);   // the kill
    ASSERT_TRUE(W.t.cluster().awaitCatalogLeaderOtherThan(L, 60s)) << "no new catalog leader";
    const int S1 = W.firstNodeOtherThan(L), S2 = W.firstNodeOtherThan(L, S1);
    for (const int n : {S1, S2}) {
        ASSERT_TRUE(AuthTrio::until(120s, [&] { return W.erasedOn(n, 1); })) << "node " << n << ": the namespace was never erased";
        EXPECT_TRUE(AuthTrio::until(60s, [&] { return W.dirsGone(n, 1); })) << "node " << n << ": ns-1 directory survived";
    }
    W.restart(L);
    ASSERT_TRUE(AuthTrio::until(120s, [&] { return W.erasedOn(L, 1); })) << "the restarted old leader never converged";
    EXPECT_TRUE(AuthTrio::until(60s, [&] { return W.dirsGone(L, 1); })) << "node " << L << " (restarted): ns-1 directory survived";
    // The name is free and the new namespace holds nothing of the old one.
    const Ans again = W.ns(S1, pc::NamespaceDdlKind::Create, "a");
    ASSERT_EQ(again.code, 0) << again.detail;
    EXPECT_GT(again.id, 1u);
    for (const int n : {1, 2, 3}) {
        EXPECT_TRUE(AuthTrio::until(60s, [&] { const auto ids = W.ids("admin", n); return !has(ids, W.tA) && !has(ids, W.tA2); }))
            << "node " << n << ": a topic of the old namespace is listed";
    }
}

// A follower down for the whole delete rejoins and converges: registry, directories and security records.
TEST(NamespaceLifecycleTrioRegression, FollowerRejoinsAfterDeleteRegression) {
    auto w = setupWorld("nslct_rejoin");
    World& W = *w;
    const int cl = W.t.cluster().settledLeader();
    const int sl = security_cluster::settledSecurityLeader(W.t.cluster());
    ASSERT_GT(cl, 0);
    ASSERT_GT(sl, 0);
    const int f = W.firstNodeOtherThan(cl, sl);
    ASSERT_GT(f, 0);
    W.stop(f);
    ASSERT_EQ(W.ns(cl, pc::NamespaceDdlKind::Delete, "a").code, 0);
    for (const int n : {cl, W.firstNodeOtherThan(f, cl)})
        ASSERT_TRUE(AuthTrio::until(120s, [&] { return W.erasedOn(n, 1); })) << "node " << n << ": never erased";
    const Ans again = W.ns(cl, pc::NamespaceDdlKind::Create, "a");
    ASSERT_EQ(again.code, 0) << again.detail;
    ASSERT_GT(again.id, 1u);
    W.restart(f);
    ASSERT_TRUE(AuthTrio::until(120s, [&] { return W.erasedOn(f, 1); })) << "the rejoined follower still lists the deleted namespace";
    EXPECT_TRUE(AuthTrio::until(60s, [&] { return W.dirsGone(f, 1); })) << "the rejoined follower kept ns-1 on disk";
    ASSERT_TRUE(AuthTrio::until(60s, [&] { const auto e = W.entry(f, static_cast<uint32_t>(again.id)); return e && e->name == "a" && e->state == 0; }))
        << "the follower does not list the re-created namespace";
    EXPECT_TRUE(AuthTrio::until(60s, [&] { return W.tryLogin(f, "a/alice").code == kAuthFailed; }))
        << "the old namespace's user still logs in on the rejoined follower";
    EXPECT_TRUE(AuthTrio::until(60s, [&] { const auto ids = W.ids("admin", f); return !has(ids, W.tA) && !has(ids, W.tA2); }))
        << "a topic of the old namespace is listed on the rejoined follower";
}

// Limits are per node (D24, D28): maxConnections counts the sessions of THIS node, every node's worker holds the rate limit,
// and Throttled is answered where the partition leads (the usage is that node's).
TEST(NamespaceLifecycleTrioRegression, ThrottleOnEveryNodeRegression) {
    auto w = setupWorld("nslct_thr");
    World& W = *w;
    constexpr int kMaxConnections = 18, kProduceBytes = 13;
    // connections: one session of `a` per node, a second refused 52 on that node
    ASSERT_EQ(W.ns(1, pc::NamespaceDdlKind::SetLimits, "a", 1, kMaxConnections, 1).code, 0);
    World::Login held[4];
    for (int n = 1; n <= 3; ++n) {
        ASSERT_TRUE(AuthTrio::until(30s, [&] { held[n] = W.tryLogin(n, "a/alice"); return held[n].code == 0; })) << "node " << n << ": the first session";
        EXPECT_EQ(W.tryLogin(n, "a/bob").code, kLimit) << "node " << n << ": the second session of the namespace on this node";
    }
    ASSERT_EQ(W.ns(1, pc::NamespaceDdlKind::SetLimits, "a", 1, kMaxConnections, 0).code, 0);   // unlimited again
    for (int n = 1; n <= 3; ++n)
        EXPECT_TRUE(AuthTrio::until(30s, [&] { return W.tryLogin(n, "a/bob").code == 0; })) << "node " << n << ": connection limit lifted";

    // rate: the limit reaches every node's worker; the leader of a/t throttles, the others never saw the traffic
    constexpr uint64_t kRate = 20'000;
    ASSERT_EQ(W.ns(1, pc::NamespaceDdlKind::SetLimits, "a", 1, kProduceBytes, kRate).code, 0);
    const uint32_t nsId = 1;
    for (int n = 1; n <= 3; ++n) {
        const uint16_t slot = W.t.node(n).nsSlotOfForTest(nsId);
        ASSERT_NE(slot, kNoNsSlot) << "node " << n;
        EXPECT_TRUE(AuthTrio::until(30s, [&] {
            return W.t.node(n).onWorkerThreadForTest(0, [slot](Worker& wk) { return wk.nsSlotsForTest().limits(slot).produceBytesPerSec; }) == kRate;
        })) << "node " << n << ": the worker never got the rate limit";
    }
    const int leader = W.leaderOf(W.tA);
    ASSERT_GT(leader, 0);
    const int64_t sid = W.sid("a/alice", leader);
    bool throttled = false;
    int served = 0;
    for (int i = 0; i < 400 && !throttled; ++i) {
        const auto r = W.produce(sid, leader, W.tA, 4096);
        ASSERT_TRUE(r.has_value()) << "a produce on node " << leader << " got no answer (" << i << ")";
        if (r->errorCode == 0) ++served;
        else {
            throttled = true;
            EXPECT_EQ(r->errorCode, kThrottled);
            EXPECT_GT(r->logStartOffset, 0) << "the retry-after rides logStartOffset";
        }
    }
    EXPECT_TRUE(throttled) << "never throttled after " << served << " served produces";
    for (int n = 1; n <= 3; ++n) {
        const uint64_t refusals = W.t.node(n).onWorkerThreadForTest(0, [](Worker& wk) { return wk.throttledRefusalsForTest(); });
        if (n == leader) EXPECT_GT(refusals, 0u) << "node " << n << " leads a/t";
        else EXPECT_EQ(refusals, 0u) << "node " << n << " saw none of the traffic";
    }
}

// acks=all across a suspend RO / resume cycle: every record the client was told was acked is on every replica and is
// consumed; every record sent while ReadOnly was refused with 51 and is nowhere.
TEST(NamespaceLifecycleTrioRegression, AcksAllNoLossAcrossSuspendResumeRegression) {
    auto w = setupWorld("nslct_acks");
    World& W = *w;
    rc::ClientConfig cc;
    cc.gatewayChannel  = W.t.channel(1);
    cc.gatewayStreamId = AuthWireClient::kClientStream;
    cc.egressChannel   = "aeron:udp?endpoint=127.0.0.1:" + std::to_string(rune::aeronutil::testing::allocFreeUdpPort());
    cc.egressStreamId  = 2;
    cc.dataDir         = (W.t.cluster().clientsDir->path / "lc_client").string();
    cc.username        = "a/alice";
    cc.password        = AuthTrio::kPw;
    cc.offerCrypto     = true;
    cc.tlsTrustPem     = AuthTrio::tlsCaPem();
    cc.topics          = {rc::TopicSpec{"t", W.tA, 1}};
    rc::Client c(cc);
    c.connectAndWait(30s);
    const rc::TopicHandle th = c.topic("t");
    rc::ProducerConfig pcfg;
    pcfg.acks = rc::Acks::All;
    pcfg.lingerUs = 0;
    rc::Producer p = c.producer(pcfg);
    rc::Completions cq(c);
    uint64_t key = 0;
    struct Tally { int ok = 0, refused = 0, other = 0; int lastCode = 0; int lastStatus = 0; };
    auto burst = [&](int n) {
        Tally tl;
        const std::vector<uint8_t> v(64, 9);
        int sent = 0, done = 0;
        const auto deadline = Clock3::now() + 60s;
        while (done < n && Clock3::now() < deadline) {
            if (sent < n && p.trySend(th, rc::Key(++key), std::span<const uint8_t>(v.data(), v.size()), cq, key) == rc::SendResult::Ok) ++sent;
            else if (sent < n) --key;
            p.flush();
            (void)cq.poll([&](const rc::Completion& d, std::span<const uint8_t>) {
                ++done;
                if (d.status == rc::OpStatus::Ok) ++tl.ok;
                else if (d.errorCode == kSuspended) ++tl.refused;
                else { ++tl.other; tl.lastCode = d.errorCode; tl.lastStatus = static_cast<int>(d.status); }
            });
            std::this_thread::sleep_for(1ms);
        }
        return tl;
    };
    const Tally before = burst(50);
    EXPECT_EQ(before.ok, 50) << "refused " << before.refused << ", other " << before.other;
    ASSERT_EQ(W.ns(1, pc::NamespaceDdlKind::Suspend, "a", 1).code, 0);
    Tally during;
    // The suspend reaches the gate: no record is acked any more (a bounded wait), and every refusal is NamespaceSuspended.
    ASSERT_TRUE(AuthTrio::until(30s, [&] { during = burst(10); return during.ok == 0; }))
        << "ReadOnly never stopped acking produces (ok " << during.ok << ")";
    EXPECT_EQ(during.refused, 10) << "ReadOnly must refuse every produce with NamespaceSuspended (51): ok " << during.ok
        << ", refused " << during.refused << ", other " << during.other << ", last other: status " << during.lastStatus
        << " errorCode " << during.lastCode;
    ASSERT_EQ(W.ns(1, pc::NamespaceDdlKind::Resume, "a").code, 0);
    Tally after;
    ASSERT_TRUE(AuthTrio::until(30s, [&] { after = burst(50); return after.ok == 50; })) << "after resume: ok " << after.ok << ", other " << after.other;
    p.close();

    const int total = before.ok + after.ok;
    // Every replica holds exactly the acked records.
    for (int n = 1; n <= 3; ++n) {
        const int64_t end = W.t.node(n).onOwnerThreadForTest(W.tA, 0, [](rune::replication::PartitionReplica* r) {
            return r != nullptr ? r->logEndOffset() : int64_t{-1};
        });
        EXPECT_TRUE(AuthTrio::until(60s, [&] {
            return W.t.node(n).onOwnerThreadForTest(W.tA, 0, [](rune::replication::PartitionReplica* r) {
                       return r != nullptr ? r->logEndOffset() : int64_t{-1};
                   }) == total;
        })) << "node " << n << ": log end " << end << ", acked " << total;
    }
    // ... and the consumer sees exactly them.
    std::atomic<int> got{0};
    rc::SubscribeOptions so;
    so.from = rc::From::earliest();
    rc::Subscription sub = c.subscribe(th, 0, so, [&](const rc::Record&) { ++got; });
    EXPECT_TRUE(AuthTrio::until(40s, [&] { return got.load() >= total; })) << "consumed " << got.load() << " of " << total;
    std::this_thread::sleep_for(200ms);
    EXPECT_EQ(got.load(), total) << "produced != consumed";
    sub.unsubscribe();
}
