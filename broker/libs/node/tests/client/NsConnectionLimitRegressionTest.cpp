// NsConnectionLimitRegressionTest.cpp - namespaces Phase 2 Task 10 (D24, #236): connection counts per namespace and per
// node, the open-transaction and transaction-time limits, on REAL authenticating BrokerNodes (default config), v14
// sessions through the raw-frame wire client and the real client.  The gateway-level unit cells (injected clock) live in
// rune_gateway_auth_login_tests; the coordinator cells in rune_transaction_tests.
//
// Interaction matrix (limit x {below, at, above} x close path / node):
//   | limit                       | operation              | state                          | expected                                     | test
//   |-----------------------------|------------------------|--------------------------------|----------------------------------------------|-----
//   | max-connections 2           | login after the proof  | 0, 1, 2 live                   | ok, ok, 52; count == live                    | LimitEnforcementTableRegression.ConnectionsRegression
//   | max-connections-per-user 2  | login of one user      | other user unaffected          | ok, ok, 52                                   | ...ConnectionsRegression
//   | max-sessions-per-user 2     | login of one user      | other user unaffected          | ok, ok, 52                                   | ...ConnectionsRegression
//   | the count                   | client close, idle     | each close path                | back to 0 (refusal frees room)               | NsConnectionLimitRegression.ReleasedOnEveryClosePathRegression
//   |                             | timeout, revoke,       |                                |                                              |
//   |                             | suspend RA, restart    |                                |                                              |
//   | max-connections 2           | login on each of 3     | 3-node cluster                 | 2 per node succeed, the 3rd per node refused | NsConnectionLimitRegression.PerNodeNotClusterRegression
//   | max-open-txns 1             | the 2nd open txn       | real client                    | 52; room back after the 1st commits          | LimitEnforcementTableRegression.OpenTxnsBrokerRegression
//   | max-txn-time-ms 30000       | InitProducerId(timeout)| real client                    | 30000 ok, 31000 refused 52                   | LimitEnforcementTableRegression.TxnTimeBrokerRegression
#include "support/AuthRig.h"
#include "support/AuthTrio.h"
#include "support/LimitEnforcementTable.h"

#include <rune/client/Client.h>
#include <rune/client/NamespaceLimitFields.h>
#include <rune/client/producer/Producer.h>

#include <gtest/gtest.h>

#include <chrono>
#include <functional>
#include <memory>
#include <string>
#include <thread>
#include <vector>

using namespace rune::node;
using namespace rune::node::test;
using namespace rune::node::test::grants;
namespace pc = rune::protocol::client;
namespace rc = rune::client;
using namespace std::chrono_literals;

namespace {

constexpr const char* kAdminPw = "namespace-admin-password-1";
constexpr const char* kUserPw  = "namespace-user-password-12";
constexpr uint16_t kLimit = 52;

std::unique_ptr<rc::Client> connected(AuthBroker& b, const std::string& user) {
    rc::ClientConfig c;
    c.gatewayChannel  = b.channel();
    c.gatewayStreamId = 1;
    c.egressChannel   = "aeron:udp?endpoint=127.0.0.1:" + std::to_string(rune::aeronutil::testing::allocFreeUdpPort());
    c.egressStreamId  = 2;
    c.dataDir         = b.clientDir().string();
    c.username        = user;
    c.password        = user == "admin" ? kAdminPw : kUserPw;
    c.offerCrypto     = true;
    c.tlsTrustPem     = AuthBroker::tlsCaPem();
    auto cl = std::make_unique<rc::Client>(c);
    cl->connectAndWait(20s);
    return cl;
}

std::vector<AuthGrant> userGrants() {
    return {prefix(SO::Create, ST::Topic, ""), prefix(SO::Describe, ST::Topic, ""), prefix(SO::Write, ST::Topic, ""),
            prefix(SO::Read, ST::Topic, ""), prefix(SO::Write, ST::TransactionalId, ""), prefix(SO::Describe, ST::TransactionalId, "")};
}

rc::NamespaceLimitsPatch patchOf(std::initializer_list<std::pair<const char*, uint64_t>> kv) {
    rc::NamespaceLimitsPatch p;
    for (const auto& [f, v] : kv)
        if (!p.set(*rc::namespaceLimitIndex(f), v)) throw std::runtime_error(std::string("patch ") + f);
    return p;
}

bool until(const std::function<bool()>& pred, std::chrono::seconds limit = 30s) {
    const auto deadline = std::chrono::steady_clock::now() + limit;
    while (std::chrono::steady_clock::now() < deadline) {
        if (pred()) return true;
        std::this_thread::sleep_for(10ms);
    }
    return pred();
}

/// A v14 login of `user` on `b` through `w`; the Login (ok() / errorCode()).
AuthWireClient::Login loginV14(AuthBroker& b, AuthWireClient& w, const std::string& user) {
    const int64_t sid = w.connect(b.channel(), pc::kClientVersionNamespaces);
    if (sid <= 0) throw std::runtime_error("connect");
    return w.login(b.channel(), sid, user, {kUserPw, kUserPw});
}

}  // namespace

TEST(LimitEnforcementTableRegression, ConnectionsRegression) {
    struct Case { const char* field; };
    for (const Case cs : {Case{"max-connections"}, Case{"max-connections-per-user"}, Case{"max-sessions-per-user"}}) {
        const LimitRow& row = *rowOf(cs.field);
        AuthBroker b(std::string("lim_conn_") + cs.field);
        b.bootstrapAdmin(kAdminPw);
        auto admin = connected(b, "admin");
        const auto ns = admin->createNamespace("c", patchOf({{cs.field, row.limit}}));
        ASSERT_TRUE(ns.has_value()) << ns.error().detail;
        for (const char* u : {"c/u1", "c/u2", "c/u3"}) b.createUser(u, kUserPw, {});
        AuthWireClient& w = b.wire();
        const bool perUser = std::string(cs.field) != "max-connections";
        // Total: three DIFFERENT users; per user: the SAME user three times.
        const std::string a = "c/u1", bb = perUser ? "c/u1" : "c/u2", c3 = perUser ? "c/u1" : "c/u3";
        const auto l1 = loginV14(b, w, a);
        const auto l2 = loginV14(b, w, bb);
        EXPECT_TRUE(l1.ok()) << cs.field << " below";
        EXPECT_TRUE(l2.ok()) << cs.field << " at: usage == limit";
        EXPECT_EQ(b.node().nsConnectionsForTest(*ns), 2u);
        const auto l3 = loginV14(b, w, c3);
        EXPECT_FALSE(l3.ok()) << cs.field << " above";
        EXPECT_EQ(l3.errorCode(), kLimit) << cs.field;
        ASSERT_TRUE(l3.fin.has_value());
        EXPECT_NE(std::string(l3.fin->detail).find(std::string(cs.field) == "max-sessions-per-user" ? "sessions" : "connections"),
                  std::string::npos) << l3.fin->detail << " (" << cs.field << ")";
        EXPECT_EQ(b.node().nsConnectionsForTest(*ns), 2u) << "a refused login is never counted";
        if (perUser) {   // another user of the namespace is unaffected by one user's cap
            EXPECT_TRUE(loginV14(b, w, "c/u2").ok()) << cs.field;
            EXPECT_EQ(b.node().nsConnectionsForTest(*ns), 3u);
        }
        // The cluster admin (default namespace) is never refused by a namespace's cap.
        EXPECT_TRUE(loginV14(b, w, "admin").fin.has_value());
        // Room comes back when a session ends.
        (void)w.raw().disconnect(b.channel(), AuthWireClient::kClientStream, l1.sessionId);
        ASSERT_TRUE(until([&] { return b.node().nsConnectionsForTest(*ns) == (perUser ? 2u : 1u); })) << cs.field;
        EXPECT_TRUE(loginV14(b, w, c3).ok()) << cs.field << ": after a release";
    }
}

TEST(NsConnectionLimitRegression, ReleasedOnEveryClosePathRegression) {
    AuthBroker b("lim_release");
    b.bootstrapAdmin(kAdminPw);
    auto admin = connected(b, "admin");
    const auto ns = admin->createNamespace("r");
    ASSERT_TRUE(ns.has_value());
    for (const char* u : {"r/a", "r/b", "r/c"}) b.createUser(u, kUserPw, {});
    auto count = [&] { return b.node().nsConnectionsForTest(*ns); };
    AuthWireClient& w = b.wire();
    {   // client close
        const auto l = loginV14(b, w, "r/b");
        ASSERT_TRUE(l.ok());
        ASSERT_EQ(count(), 1u);
        (void)w.raw().disconnect(b.channel(), AuthWireClient::kClientStream, l.sessionId);
        EXPECT_TRUE(until([&] { return count() == 0; })) << "client Disconnect";
    }
    {   // idle timeout: a wire that sends no keepalives is reaped after the gateway's session timeout
        AuthWireClient& idle = b.wire();
        idle.raw().keepSessionsAlive(false);
        const auto l = loginV14(b, idle, "r/b");
        ASSERT_TRUE(l.ok());
        ASSERT_EQ(count(), 1u);
        EXPECT_TRUE(until([&] { return count() == 0; }, 40s)) << "idle timeout";
    }
    {   // revoke: the user is disabled, its sessions are revoked
        const auto l = loginV14(b, w, "r/a");
        ASSERT_TRUE(l.ok());
        ASSERT_EQ(count(), 1u);
        b.disableUser("r/a");
        EXPECT_TRUE(until([&] { return count() == 0; })) << "revoke";
    }
    {   // suspend RejectAll closes every session of the namespace; resume lets logins in again
        ASSERT_TRUE(loginV14(b, w, "r/b").ok());
        ASSERT_TRUE(loginV14(b, w, "r/c").ok());
        ASSERT_EQ(count(), 2u);
        ASSERT_TRUE(admin->suspendNamespace("r", pc::NsSuspendMode::RejectAll).has_value());
        EXPECT_TRUE(until([&] { return count() == 0; })) << "suspend RejectAll";
        ASSERT_TRUE(admin->resumeNamespace("r").has_value());
    }
    {   // gateway shutdown: a restarted broker starts every count at zero
        ASSERT_TRUE(loginV14(b, w, "r/b").ok());
        ASSERT_EQ(count(), 1u);
        b.restart(/*reloginAdmin=*/false);
        EXPECT_EQ(count(), 0u) << "restart";
        AuthWireClient& w2 = b.wire();
        EXPECT_TRUE(until([&] { return loginV14(b, w2, "r/b").ok(); })) << "and the namespace serves logins again";
        EXPECT_EQ(count(), 1u);
    }
}

TEST(NsConnectionLimitRegression, PerNodeNotClusterRegression) {
    AuthTrio t("lim_trio");
    t.start();
    t.bootstrapAdmin();
    rc::ClientConfig cc;
    cc.gatewayChannel  = t.channel(1);
    cc.gatewayStreamId = AuthWireClient::kClientStream;
    cc.egressChannel   = "aeron:udp?endpoint=127.0.0.1:" + std::to_string(rune::aeronutil::testing::allocFreeUdpPort());
    cc.egressStreamId  = 2;
    cc.dataDir         = (t.cluster().clientsDir->path / "rc_admin").string();
    cc.username        = "admin";
    cc.password        = AuthTrio::kAdminPassword;
    cc.offerCrypto     = true;
    cc.tlsTrustPem     = AuthTrio::tlsCaPem();
    std::unique_ptr<rc::Client> admin;
    ASSERT_TRUE(AuthTrio::until(60s, [&] {
        try { admin = std::make_unique<rc::Client>(cc); admin->connectAndWait(30s); return true; } catch (...) { return false; }
    }));
    const auto ns = admin->createNamespace("a", patchOf({{"max-connections", 2}}));
    ASSERT_TRUE(ns.has_value()) << ns.error().detail;
    for (const char* u : {"a/u1", "a/u2", "a/u3"}) t.createUser(1, u, AuthTrio::kPw);
    AuthWireClient& w = t.wire();
    for (int node = 1; node <= 3; ++node) {
        std::vector<AuthWireClient::Login> logins;
        for (const char* u : {"a/u1", "a/u2"}) {
            AuthWireClient::Login l;
            ASSERT_TRUE(AuthTrio::until(60s, [&] {   // the namespace and the user reach this node asynchronously
                const int64_t sid = w.connect(t.channel(node), pc::kClientVersionNamespaces);
                if (sid <= 0) return false;
                l = w.login(t.channel(node), sid, u, {AuthTrio::kPw, AuthTrio::kPw});
                return l.ok();
            })) << "node " << node << " " << u;
            logins.push_back(l);
        }
        const int64_t sid3 = w.connect(t.channel(node), pc::kClientVersionNamespaces);
        const auto over = w.login(t.channel(node), sid3, "a/u3", {AuthTrio::kPw, AuthTrio::kPw});
        EXPECT_EQ(over.errorCode(), kLimit) << "node " << node << ": the 3rd on this node is refused although the cluster holds " << (node - 1) * 2 + 2;
        EXPECT_EQ(t.node(node).nsConnectionsForTest(*ns), 2u) << "node " << node;
    }
}

TEST(LimitEnforcementTableRegression, OpenTxnsBrokerRegression) {
    AuthBroker b("lim_opentxns");
    b.bootstrapAdmin(kAdminPw);
    auto admin = connected(b, "admin");
    ASSERT_TRUE(admin->createNamespace("t", patchOf({{"max-open-txns", 1}})).has_value());
    b.createUser("t/u", kUserPw, userGrants());
    auto u = connected(b, "t/u");
    const auto def = u->defineTopicAndWait("t", 1, "", false, {}, 20s);
    ASSERT_EQ(def.errorCode, 0) << def.detail;
    const rc::TopicHandle th{def.topicId, 1};
    auto openTxn = [&](const std::string& id, rc::Producer& p) {
        (void)id;
        p.initTransactionsAndWait(20s);
        p.beginTransaction();
        const uint8_t v[1] = {1};
        ASSERT_EQ(p.trySend(th, rc::Key(int64_t{1}), std::span<const uint8_t>(v, 1)), rc::SendResult::Ok);
    };
    rc::ProducerConfig pcfg;
    pcfg.acks = rc::Acks::All;
    pcfg.transactionalId = "x1";
    rc::Producer p1 = u->producer(pcfg);
    openTxn("x1", p1);
    p1.flushAndWait(20s);   // AddPartitions answered: p1's transaction is open
    pcfg.transactionalId = "x2";
    rc::Producer p2 = u->producer(pcfg);
    openTxn("x2", p2);
    std::string err;
    try { p2.flushAndWait(20s); p2.commitTransactionAndWait(20s); } catch (const std::exception& e) { err = e.what(); }
    EXPECT_FALSE(err.empty()) << "Above: the 2nd open transaction is refused";
    EXPECT_NE(err.find("52"), std::string::npos) << err;
    p1.commitTransactionAndWait(20s);   // the 1st ends ...
    pcfg.transactionalId = "x3";
    rc::Producer p3 = u->producer(pcfg);
    openTxn("x3", p3);
    EXPECT_NO_THROW({ p3.flushAndWait(20s); p3.commitTransactionAndWait(20s); }) << "... and room is back";
}

TEST(LimitEnforcementTableRegression, TxnTimeBrokerRegression) {
    const LimitRow& row = *rowOf("max-txn-time-ms");
    AuthBroker b("lim_txntime");
    b.bootstrapAdmin(kAdminPw);
    auto admin = connected(b, "admin");
    ASSERT_TRUE(admin->createNamespace("t", patchOf({{"max-txn-time-ms", row.limit}})).has_value());
    b.createUser("t/u", kUserPw, userGrants());
    auto u = connected(b, "t/u");
    auto initWith = [&](const std::string& id, int32_t timeoutMs) {
        rc::ProducerConfig pcfg;
        pcfg.acks = rc::Acks::All;
        pcfg.transactionalId = id;
        pcfg.transactionTimeoutMs = timeoutMs;
        rc::Producer p = u->producer(pcfg);
        std::string err;
        try { p.initTransactionsAndWait(20s); } catch (const std::exception& e) { err = e.what(); }
        return err;
    };
    EXPECT_EQ(initWith("lo", static_cast<int32_t>(row.limit) - 1), "") << "Below";
    EXPECT_EQ(initWith("at", static_cast<int32_t>(row.limit)), "") << "At";
    const std::string over = initWith("hi", static_cast<int32_t>(row.limit) + 1);
    EXPECT_FALSE(over.empty()) << "Above";
    EXPECT_NE(over.find("52"), std::string::npos) << over;
}
