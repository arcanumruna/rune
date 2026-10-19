// SecurityHaltTest.cpp - authentication Task 14 (D28): a halted `__security` apply FAILS CLOSED.  One real authenticating
// broker; the applier's next fingerprint is damaged (tamperNextFingerprintForTest), then any security DDL halts it.
#include "support/AuthRig.h"

#include <rune/node/AdminWorker.h>

using namespace rune::node;
using namespace rune::node::test;
using namespace rune::node::test::grants;
namespace pc = rune::protocol::client;
using ST = rune::protocol::SecurityResourceType;
using SO = rune::protocol::SecurityOperation;
using namespace std::chrono_literals;

TEST(SecurityHaltRegression, HaltRevokesEverySessionAndRefusesLoginsRegression) {
    LogCapture log;
    AuthRig r("halt");
    AuthBroker& b = r.broker();
    std::string name;
    const int64_t user = r.newUser({exact(SO::Read, ST::Topic, "orders")}, &name);
    const int64_t corr = r.c().sendSubscribe(r.ch(), r.stream(), user, r.orders, 0);
    ASSERT_EQ(r.c().awaitSubscribeCode(corr, AuthRig::dl()).value_or(-1), 0);

    BrokerNode& n = b.node();
    n.onAdminThreadForTest([&n]() {
        n.adminWorkerAgentForTest()->worker().securityApplierForTest()->tamperNextFingerprintForTest();
        return 0;
    });
    // Any security DDL: it commits, the applier's fingerprint check fails, the applier HALTS.  It must never be answered OK.
    pc::SecurityDdl m;
    m.sessionId = b.adminSession(); m.kind = pc::SecurityOpKind::CreateRole; m.name = "halting";
    const auto ddl = b.admin().securityDdl(r.ch(), m, "CREATE ROLE halting;", 3s);
    EXPECT_NE(ddl.errorCode, 0) << "a halted apply never claims enforcement";

    const auto deadline = std::chrono::steady_clock::now() + 20s;
    while (!n.securityViewForTest().halted && std::chrono::steady_clock::now() < deadline) std::this_thread::sleep_for(10ms);
    ASSERT_TRUE(n.securityViewForTest().halted);

    // Every authenticated session is revoked: its live subscription, then the session-level frame with the halt detail.
    const int revoked = static_cast<int>(pc::ErrorCode::SessionRevoked);
    int onSub = -1, onSession = -1;
    std::string detail;
    r.c().pollUntil([&] {
        for (const auto& e : r.c().errors()) {
            if (e.correlationId == corr) onSub = e.code;
            if (e.correlationId == 0 && e.sessionId == user) { onSession = e.code; detail = e.message; }
        }
        return onSub != -1 && onSession != -1;
    });
    EXPECT_EQ(onSub, revoked);
    EXPECT_EQ(onSession, revoked);
    EXPECT_EQ(detail, "security log halted on this broker");
    EXPECT_FALSE(r.c().pingError(r.ch(), r.stream(), user, AuthRig::dl(1)).has_value()) << "the session is gone";

    // New logins are refused (AuthenticationFailed), the broker itself stays up.
    const int64_t s2 = r.wire().connect(r.ch());
    ASSERT_GT(s2, 0) << "the broker still accepts a Connect";
    EXPECT_EQ(r.wire().login(r.ch(), s2, name, {AuthRig::kPw, AuthRig::kPw}).errorCode(), 44);
    const auto halted = b.counter(kSecurityApplyHaltedCounterTypeId);
    ASSERT_TRUE(halted.has_value());
    EXPECT_GT(*halted, 0) << "counter 1'000'226 is raised";
    const auto revokedCount = b.counter(kSessionsRevokedCounterTypeId);
    ASSERT_TRUE(revokedCount.has_value());
    EXPECT_GE(*revokedCount, 2) << "the user's and the admin's sessions (counter 232)";
    EXPECT_GE(log.count("SECURITY APPLY HALTED"), 1);
    EXPECT_GE(log.count("docs/SECURITY.md#halted"), 1);
}
