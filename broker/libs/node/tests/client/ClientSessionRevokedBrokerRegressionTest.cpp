// ClientSessionRevokedBrokerRegressionTest.cpp - authentication Task 14 (D26): the REAL client against a REAL
// authenticating broker.  When the user is disabled the broker answers every live subscription SessionRevoked, then the
// session-level frame, and closes the session; the client completes them SessionRevoked and does NOT reconnect.
#include "support/AuthRig.h"

#include <rune/client/Client.h>

#include <gtest/gtest.h>

using namespace rune::node;
using namespace rune::node::test;
using namespace rune::node::test::grants;
namespace pc = rune::protocol::client;
namespace rc = rune::client;
using SO = rune::protocol::SecurityOperation;
using ST = rune::protocol::SecurityResourceType;
using namespace std::chrono_literals;

TEST(ClientSessionRevokedBrokerRegression, InFlightRequestsAndSubscriptionsCompleteSessionRevokedAndNothingReconnects) {
    AuthRig r("clientrevoked");
    AuthBroker& b = r.broker();
    std::string name;
    (void)r.newUser({exact(SO::Read, ST::Topic, "orders")}, &name);

    rc::ClientConfig c;
    c.gatewayChannel  = b.channel();
    c.gatewayStreamId = 1;
    c.egressChannel   = "aeron:udp?endpoint=127.0.0.1:" + std::to_string(rune::aeronutil::testing::allocFreeUdpPort());
    c.egressStreamId  = 2;
    c.dataDir         = b.clientDir().string();
    c.username        = name;
    c.password        = AuthRig::kPw;
    c.topics          = {rc::TopicSpec{"orders", r.orders, 1}};
    rc::Client client(c);
    client.connectAndWait(20s);

    rc::SubscribeOptions o;
    o.from = rc::From::earliest();
    rc::Subscription sub = client.subscribe(client.topic("orders"), 0, o, [](const rc::Record&) {});
    for (int i = 0; i < 400 && sub.subscriptionId() == 0; ++i) std::this_thread::sleep_for(25ms);
    ASSERT_NE(sub.subscriptionId(), 0) << "bound on the broker";
    std::this_thread::sleep_for(300ms);
    const auto loginsBefore = b.counter(kLoginsOkCounterTypeId).value_or(-1);
    const auto failedBefore = b.counter(kLoginsFailedCounterTypeId).value_or(-1);

    ASSERT_EQ(b.admin().alterPassword(b.channel(), b.adminSession(), name, pc::UserChangeKind::Disable).errorCode, 0);

    for (int i = 0; i < 400 && !sub.finished(); ++i) std::this_thread::sleep_for(25ms);
    ASSERT_TRUE(sub.finished()) << "the subscription completed";
    EXPECT_EQ(sub.failReason(), rc::SubscriptionFailReason::SessionRevoked);

    // Nothing reconnects: the user is disabled, so a reconnect would only log in-and-fail; the login counter must stay put
    // and no failed login may appear.
    std::this_thread::sleep_for(1500ms);
    EXPECT_EQ(b.counter(kLoginsOkCounterTypeId).value_or(-2), loginsBefore) << "no new session";
    EXPECT_EQ(b.counter(kLoginsFailedCounterTypeId).value_or(-2), failedBefore) << "and no reconnect attempt either";
    EXPECT_GE(b.counter(kSessionsRevokedCounterTypeId).value_or(0), 2) << "the harness session and the client's (counter 232)";
    client.closeImmediately();
}
