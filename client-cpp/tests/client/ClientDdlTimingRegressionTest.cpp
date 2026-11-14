// Catalog history (Task 5 fix round 1): the TIMING axis of the DDL interaction
// matrix — every DDL op × {close while awaiting a resend / held for a node,
// the seed lost while awaiting, a duplicate answer while awaiting, the leader's
// session lost mid-DDL, a Fatal offer on resend / on redirect, a resend refused
// past the budget}.  Each cell: exactly one completion with the expected
// status, nothing more on later duty cycles, and the DDL bookkeeping back to
// zero.  Plus I1: a DDL follows its session — lost leader session -> re-sent
// toward the seed with the SAME requestId; a seed loss never fails a DDL held
// on another session; the generated requestId is reported.
// Non-blocking ops on the single-threaded fake (F7).
#include <filesystem>
#include "../support/FreePort.h"
#include "../support/InvokerGateway.h"
#include "../support/TestCompletions.h"
#include <rune/client/Client.h>
#include <rune/protocol/client/SchemaMessages.h>
#include <gtest/gtest.h>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <functional>
#include <optional>
#include <span>
#include <string>
#include <sys/socket.h>
#include <tuple>
#include <unistd.h>
#include <vector>

namespace pc = rune::protocol::client;
namespace rc = rune::client;
using rune::protocol::RequestId;
using Offer = rune::client::transport::AeronTransport::OfferResult;

namespace {
int64_t g_now = 1'000'000'000;   // test thread only: invoker mode has one thread
int64_t fakeNow() noexcept { return g_now; }
int g_ctr = 0;                   // test thread only

std::string udp(int port) { return "aeron:udp?endpoint=127.0.0.1:" + std::to_string(port); }

rc::ClientConfig invokerCfg(int32_t in, int32_t eg) {
    rc::ClientConfig c;
    c.gatewayChannel = "aeron:ipc"; c.gatewayStreamId = in; c.egressChannel = "aeron:ipc"; c.egressStreamId = eg;
    c.dataDir = std::filesystem::temp_directory_path().string() + "/ddl_timing_" + std::to_string(::getpid()) + "_" + std::to_string(g_ctr++);
    c.runnerMode = rc::RunnerMode::Invoker;
    c.nanoClock = &fakeNow;
    c.ddlTimeoutMs = 3'000;
    return c;
}
struct Rig {
    rc::Client                   client;
    test_support::InvokerGateway gw;
    explicit Rig(const rc::ClientConfig& c)
        : client(c), gw(c.dataDir + "/aeron", c.gatewayChannel, c.gatewayStreamId, c.egressChannel, c.egressStreamId) {}
    template <class P> bool drive(P&& pred, int64_t stepNs = 0) {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
        while (std::chrono::steady_clock::now() < deadline) {
            g_now += stepNs; client.doWork(); gw.pump();
            if (pred()) return true;
        }
        return false;
    }
    void passes(int n, int64_t stepNs = 0) { for (int i = 0; i < n; ++i) { g_now += stepNs; client.doWork(); gw.pump(); } }
    void connect() {
        ASSERT_TRUE(drive([&] { return gw.ready(); }));
        test_support::TestCq cq(client);
        auto f = cq.issue([&](rc::Completions& q, rc::Token t) { return client.connect(q, t); });
        ASSERT_TRUE(drive([&] { return f.ready(); }));
        ASSERT_TRUE(f.ok()) << f.describe();
    }
    void answerError(int ep, pc::ErrorCode code, const std::string& msg, int64_t session, int64_t corr) {
        gw.sendError(ep, static_cast<int16_t>(code), msg, session, corr);
    }
};
std::string payloadText(const test_support::PendingOp& f) {
    const auto b = f.bytes(); return {b.begin(), b.end()};
}

enum class DdlOp { DefineTopic, AlterTopic, CreateView, DropTopic, DropView, AlterView, AlterTopicConfig };
const char* name(DdlOp o) {
    switch (o) {
    case DdlOp::DefineTopic: return "DefineTopic";
    case DdlOp::AlterTopic: return "AlterTopic";
    case DdlOp::CreateView: return "CreateView";
    case DdlOp::DropTopic: return "DropTopic";
    case DdlOp::DropView: return "DropView";
    case DdlOp::AlterView: return "AlterView";
    case DdlOp::AlterTopicConfig: return "AlterTopicConfig";
    }
    return "?";
}
uint16_t templateOf(DdlOp o) {
    switch (o) {
    case DdlOp::DefineTopic: return pc::DefineTopic::TEMPLATE_ID;
    case DdlOp::AlterTopic: return pc::AlterTopic::TEMPLATE_ID;
    case DdlOp::CreateView: return pc::CreateView::TEMPLATE_ID;
    case DdlOp::DropTopic: return pc::DropTopic::TEMPLATE_ID;
    case DdlOp::DropView: return pc::DropView::TEMPLATE_ID;
    case DdlOp::AlterView: return pc::AlterView::TEMPLATE_ID;
    case DdlOp::AlterTopicConfig: return pc::AlterTopicConfig::TEMPLATE_ID;
    }
    return 0;
}
std::optional<pc::DdlExtension> extOf(DdlOp o, const uint8_t* f, std::size_t len) {
    switch (o) {
    case DdlOp::DefineTopic: { auto m = pc::DefineTopic::decode(f, len); if (m && m->hasExt) return m->ext; break; }
    case DdlOp::AlterTopic: { auto m = pc::AlterTopic::decode(f, len); if (m && m->hasExt) return m->ext; break; }
    case DdlOp::CreateView: { auto m = pc::CreateView::decode(f, len); if (m && m->hasExt) return m->ext; break; }
    case DdlOp::DropTopic: { auto m = pc::DropTopic::decode(f, len); if (m) return m->ext; break; }
    case DdlOp::DropView: { auto m = pc::DropView::decode(f, len); if (m) return m->ext; break; }
    case DdlOp::AlterView: { auto m = pc::AlterView::decode(f, len); if (m) return m->ext; break; }
    case DdlOp::AlterTopicConfig: { auto m = pc::AlterTopicConfig::decode(f, len); if (m) return m->ext; break; }
    }
    return std::nullopt;
}
void answerOk(test_support::InvokerGateway& gw, DdlOp o, int ep, int64_t sid, int64_t corr) {
    std::vector<uint8_t> out(256);
    std::size_t n = 0;
    switch (o) {
    case DdlOp::DefineTopic: {
        pc::DefineTopicResponse r; r.sessionId = sid; r.correlationId = corr; r.topicId = 9; r.version = 5;
        n = r.encode(out.data()); break;
    }
    case DdlOp::AlterTopic: {
        pc::AlterTopicResponse r; r.sessionId = sid; r.correlationId = corr; r.topicId = 9; r.newVersion = 5;
        n = r.encode(out.data()); break;
    }
    case DdlOp::CreateView: {
        pc::CreateViewResponse r; r.sessionId = sid; r.correlationId = corr; r.viewTopicId = 9; r.outputFingerprint = 5;
        n = r.encode(out.data()); break;
    }
    default: {
        pc::DdlResponse r; r.sessionId = sid; r.correlationId = corr; r.objectId = 9; r.newVersion = 5;
        n = r.encode(out.data()); break;
    }
    }
    gw.send(ep, out.data(), n);
}
rc::OpResult issue(rc::Client& c, DdlOp o, rc::Completions& q, rc::Token t) {
    static const uint8_t kBlob[] = {1, 2, 3};
    const std::span<const uint8_t> blob(kBlob, sizeof kBlob);
    static const pc::ConfigChangeWire kChange{1, 60'000};
    switch (o) {
    case DdlOp::DefineTopic: return c.defineTopic("t", 1, "", {}, rc::Persistence::Disk, {}, {}, q, t);
    case DdlOp::AlterTopic: return c.alterTopic("t", 1, blob, {}, q, t);
    case DdlOp::CreateView: return c.createView("v", "k", 0, blob, {}, q, t);
    case DdlOp::DropTopic: return c.dropTopic("t", {}, q, t);
    case DdlOp::DropView: return c.dropView("v", {}, q, t);
    case DdlOp::AlterView: return c.alterView("v", "k", 0, 1, blob, {}, q, t);
    case DdlOp::AlterTopicConfig: return c.alterTopicConfig("t", 1, std::span(&kChange, 1), {}, q, t);
    }
    return rc::OpResult::InvalidArgument;
}

/// One DDL frame as the fake received it.
struct Arrival { int ep; int64_t sid; int64_t corr; RequestId id; };
} // namespace

enum class Timing {
    CloseWhileAwaitingResend, CloseWhileHeldForNode, SeedLostWhileAwaitingResend, DuplicateAnswerWhileAwaiting,
    LeaderSessionLost, FatalOfferOnResend, FatalOfferOnRedirect, ResendPastBudgetEnd,
    // Task 5 re-review F3 / F1: the SEED's session is lost while the DDL is in
    // flight on it (DdlSent) — it comes back (resend, same requestId, OK), or it
    // never does inside the budget (SessionLost — never a misleading NotLeader).
    SeedLostWhileSent, SeedLostWhileSentPastBudget,
};
const char* name(Timing t) {
    switch (t) {
    case Timing::CloseWhileAwaitingResend: return "CloseWhileAwaitingResend";
    case Timing::CloseWhileHeldForNode: return "CloseWhileHeldForNode";
    case Timing::SeedLostWhileAwaitingResend: return "SeedLostWhileAwaitingResend";
    case Timing::DuplicateAnswerWhileAwaiting: return "DuplicateAnswerWhileAwaiting";
    case Timing::LeaderSessionLost: return "LeaderSessionLost";
    case Timing::FatalOfferOnResend: return "FatalOfferOnResend";
    case Timing::FatalOfferOnRedirect: return "FatalOfferOnRedirect";
    case Timing::ResendPastBudgetEnd: return "ResendPastBudgetEnd";
    case Timing::SeedLostWhileSent: return "SeedLostWhileSent";
    case Timing::SeedLostWhileSentPastBudget: return "SeedLostWhileSentPastBudget";
    }
    return "?";
}

class ClientDdlTimingMatrixRegression : public ::testing::TestWithParam<std::tuple<DdlOp, Timing>> {};

TEST_P(ClientDdlTimingMatrixRegression, ExactlyOneCompletionAndNoLeftoverState) {
    const auto [op, timing] = GetParam();
    rc::ClientConfig cfg = invokerCfg(4440, 4441);
    if (timing == Timing::ResendPastBudgetEnd || timing == Timing::SeedLostWhileSentPastBudget)
        cfg.ddlRetryBudget = std::chrono::milliseconds(300);
    Rig r(cfg);
    r.gw.uniqueSessionIds = true;   // every node session its own id (a session-fatal answer names one)
    const std::string l1Ch = udp(rune::client::testing::allocFreeUdpPort());
    const std::string l2Ch = udp(rune::client::testing::allocFreeUdpPort());
    const int l1 = r.gw.addNode(l1Ch, 4440);
    const int l2 = r.gw.addNode(l2Ch, 4440);
    const std::string nowhere = udp(rune::client::testing::allocFreeUdpPort());   // no node listens there
    r.connect();

    std::vector<Arrival> at;
    r.gw.templateHandlers[templateOf(op)] = [&](int ep, const uint8_t* f, std::size_t len) {
        const auto ext = extOf(op, f, len);
        ASSERT_TRUE(ext.has_value());
        const auto h = pc::MessageHeader::decode(f, len);
        ASSERT_TRUE(h.has_value());
        at.push_back(Arrival{ep, h->sessionId, h->correlationId, ext->requestId});
        const std::size_t n = at.size();
        switch (timing) {
        case Timing::CloseWhileAwaitingResend:
            r.answerError(ep, pc::ErrorCode::NotLeader, "", h->sessionId, h->correlationId);
            break;
        case Timing::ResendPastBudgetEnd:   // #687: the node it is sent on never comes up (a refused offer on a
                                            // CONNECTED session is back-pressure, no longer "unreachable")
            if (n == 1) r.answerError(ep, pc::ErrorCode::NotLeader, "", h->sessionId, h->correlationId);
            else        r.answerError(ep, pc::ErrorCode::Redirect, nowhere, h->sessionId, h->correlationId);
            break;
        case Timing::CloseWhileHeldForNode:
            r.answerError(ep, pc::ErrorCode::Redirect, nowhere, h->sessionId, h->correlationId);
            break;
        case Timing::SeedLostWhileAwaitingResend:
        case Timing::DuplicateAnswerWhileAwaiting:
        case Timing::FatalOfferOnResend:
            if (n == 1) r.answerError(ep, pc::ErrorCode::NotLeader, "", h->sessionId, h->correlationId);
            else answerOk(r.gw, op, ep, h->sessionId, h->correlationId);
            break;
        case Timing::LeaderSessionLost:
            if (ep == 0) {
                r.answerError(ep, pc::ErrorCode::Redirect, n == 1 ? l1Ch : l2Ch, h->sessionId, h->correlationId);
            } else if (ep == l1) {   // the leader appends, then its session dies before it answers
                r.answerError(ep, pc::ErrorCode::SessionExpired, "expired", h->sessionId, 0);
                r.gw.deadEndpoints.insert(l1);
            } else if (ep == l2) {
                answerOk(r.gw, op, ep, h->sessionId, h->correlationId);
            }
            break;
        case Timing::FatalOfferOnRedirect:
            r.client.testForceIngressOffers(Offer::Fatal, 1);   // the redirect's offer (to the seed itself) fails
            r.answerError(ep, pc::ErrorCode::Redirect, "aeron:ipc", h->sessionId, h->correlationId);
            break;
        case Timing::SeedLostWhileSent:
            if (n == 1) r.gw.expireSession(h->sessionId);   // the seed appends, then its session dies unanswered
            else answerOk(r.gw, op, ep, h->sessionId, h->correlationId);
            break;
        case Timing::SeedLostWhileSentPastBudget:
            r.gw.expireSession(h->sessionId);
            r.gw.deadEndpoints.insert(0);   // ... and the seed never answers again (no reconnect)
            break;
        }
    };

    test_support::TestCq cq(r.client);
    auto f = cq.issue([&](rc::Completions& q, rc::Token t) { return issue(r.client, op, q, t); });
    ASSERT_TRUE(f.accepted());
    auto awaiting = [&] { return r.client.testRoutedCtlAwaitingResend() == 1; };
    bool closed = false;

    switch (timing) {
    case Timing::CloseWhileAwaitingResend:
    case Timing::CloseWhileHeldForNode:
        ASSERT_TRUE(r.drive(awaiting));   // frozen clock: the resend never comes due
        r.client.closeImmediately();
        closed = true;
        break;
    case Timing::SeedLostWhileAwaitingResend: {
        ASSERT_TRUE(r.drive(awaiting));
        r.gw.expireSession(at.front().sid);   // the seed's session dies while the DDL waits
        ASSERT_TRUE(r.drive([&] { return f.ready(); }, /*stepNs=*/10'000'000));
        break;
    }
    case Timing::DuplicateAnswerWhileAwaiting:
        ASSERT_TRUE(r.drive(awaiting));
        // Stale answers for a DDL that is not in flight: ignored.
        r.answerError(0, pc::ErrorCode::NotLeader, "", at[0].sid, at[0].corr);
        r.answerError(0, pc::ErrorCode::Redirect, nowhere, at[0].sid, at[0].corr);
        r.passes(200);
        ASSERT_FALSE(f.ready()) << f.describe();
        EXPECT_EQ(r.client.testRoutedCtlAwaitingResend(), 1);
        ASSERT_TRUE(r.drive([&] { return f.ready(); }, /*stepNs=*/10'000'000));
        break;
    case Timing::FatalOfferOnResend:
        ASSERT_TRUE(r.drive(awaiting));
        r.client.testForceIngressOffers(Offer::Fatal, 1);
        g_now += 150'000'000;   // past the 100 ms backoff in one step: the next offer is the resend
        ASSERT_TRUE(r.drive([&] { return f.ready(); }));
        break;
    case Timing::ResendPastBudgetEnd:
        ASSERT_TRUE(r.drive(awaiting));
        ASSERT_TRUE(r.drive([&] { return f.ready(); }, /*stepNs=*/20'000'000));
        break;
    case Timing::LeaderSessionLost:
    case Timing::FatalOfferOnRedirect:
    case Timing::SeedLostWhileSent:
    case Timing::SeedLostWhileSentPastBudget:
        ASSERT_TRUE(r.drive([&] { return f.ready(); }, /*stepNs=*/10'000'000));
        break;
    }

    ASSERT_TRUE(f.ready());
    const auto& c = f.outcome().c;
    switch (timing) {
    case Timing::CloseWhileAwaitingResend:
    case Timing::CloseWhileHeldForNode:
        EXPECT_EQ(c.status, rc::OpStatus::Closed);
        break;
    case Timing::SeedLostWhileAwaitingResend:
    case Timing::DuplicateAnswerWhileAwaiting:
        ASSERT_TRUE(f.ok()) << f.describe();
        EXPECT_EQ(c.value, 9);
        ASSERT_EQ(at.size(), 2u) << "one resend";
        EXPECT_EQ(at[0].id, at[1].id) << "the resend carries the SAME requestId";
        break;
    case Timing::FatalOfferOnResend:
        EXPECT_EQ(c.status, rc::OpStatus::NotConnected);
        EXPECT_EQ(at.size(), 1u) << "the refused resend never reached the node";
        break;
    case Timing::LeaderSessionLost:
        ASSERT_TRUE(f.ok()) << f.describe();
        EXPECT_EQ(c.value, 9);
        ASSERT_EQ(at.size(), 4u);
        EXPECT_EQ(at[0].ep, 0);
        EXPECT_EQ(at[1].ep, l1);
        EXPECT_EQ(at[2].ep, 0) << "re-sent toward the seed once the leader's session was lost";
        EXPECT_EQ(at[3].ep, l2);
        for (const Arrival& a : at) EXPECT_EQ(a.id, at[0].id) << "the SAME requestId on every resend";
        break;
    case Timing::FatalOfferOnRedirect:
        EXPECT_EQ(c.status, rc::OpStatus::NotConnected);
        break;
    case Timing::ResendPastBudgetEnd:
        // Issue #97: the resend's node was never reachable - a session loss, not a NotLeader.
        EXPECT_EQ(c.status, rc::OpStatus::SessionLost);
        EXPECT_EQ(payloadText(f).rfind("node unreachable", 0), 0u) << payloadText(f);
        break;
    case Timing::SeedLostWhileSent:
        ASSERT_TRUE(f.ok()) << f.describe();
        EXPECT_EQ(c.value, 9);
        ASSERT_EQ(at.size(), 2u) << "one resend, after the seed's session came back";
        EXPECT_EQ(at[0].ep, 0);
        EXPECT_EQ(at[1].ep, 0);
        EXPECT_GE(r.gw.connects, 2) << "the resend went out after the seed session was re-established";
        EXPECT_EQ(at[0].id, at[1].id) << "the resend carries the SAME requestId";
        break;
    case Timing::SeedLostWhileSentPastBudget:
        EXPECT_EQ(c.status, rc::OpStatus::SessionLost) << "F1: a session loss reads SessionLost, not NotLeader";
        EXPECT_NE(payloadText(f).find("session lost"), std::string::npos) << payloadText(f);
        EXPECT_EQ(at.size(), 1u);
        break;
    }
    // Nothing more, ever: later duty cycles deliver no second completion, and
    // the DDL bookkeeping is back to zero.
    if (!closed) r.passes(300, 10'000'000);
    EXPECT_EQ(cq.timesCompleted(f.token()), 1);
    EXPECT_EQ(r.client.testRoutedCtlAwaitingResend(), 0);
    EXPECT_EQ(r.client.testPendingControlOps(), 0u);
}

INSTANTIATE_TEST_SUITE_P(
    AllOpsAllTimings, ClientDdlTimingMatrixRegression,
    ::testing::Combine(::testing::Values(DdlOp::DefineTopic, DdlOp::AlterTopic, DdlOp::CreateView, DdlOp::DropTopic,
                                         DdlOp::DropView, DdlOp::AlterView, DdlOp::AlterTopicConfig),
                       ::testing::Values(Timing::CloseWhileAwaitingResend, Timing::CloseWhileHeldForNode,
                                         Timing::SeedLostWhileAwaitingResend, Timing::DuplicateAnswerWhileAwaiting,
                                         Timing::LeaderSessionLost, Timing::FatalOfferOnResend,
                                         Timing::FatalOfferOnRedirect, Timing::ResendPastBudgetEnd,
                                         Timing::SeedLostWhileSent, Timing::SeedLostWhileSentPastBudget)),
    [](const ::testing::TestParamInfo<std::tuple<DdlOp, Timing>>& i) {
        return std::string(name(std::get<0>(i.param))) + "_" + name(std::get<1>(i.param));
    });

// Issue #97 (F1): a DDL redirected to a node whose session never comes up ends SessionLost ("node unreachable"),
// never NotLeader - the request may have been applied.  The budget (300 ms) is shorter than the DDL timeout (3 s).
TEST(ClientDdlRegression, DdlWithUnreachableNodeCompletesSessionLostNotNotLeaderRegression) {
    rc::ClientConfig cfg = invokerCfg(4446, 4447);
    cfg.ddlRetryBudget = std::chrono::milliseconds(300);
    Rig r(cfg);
    r.connect();
    const std::string nowhere = udp(rune::client::testing::allocFreeUdpPort());   // no node listens there
    int answers = 0;
    r.gw.templateHandlers[pc::DropTopic::TEMPLATE_ID] = [&](int ep, const uint8_t* f, std::size_t len) {
        auto m = pc::DropTopic::decode(f, len);
        ASSERT_TRUE(m.has_value());
        if (answers++ == 0) r.answerError(ep, pc::ErrorCode::NotLeader, "", m->sessionId, m->correlationId);
        else                r.answerError(ep, pc::ErrorCode::Redirect, nowhere, m->sessionId, m->correlationId);
    };
    test_support::TestCq cq(r.client);
    auto f = cq.issue([&](rc::Completions& q, rc::Token t) { return r.client.dropTopic("t", {}, q, t); });
    ASSERT_TRUE(f.accepted());
    ASSERT_TRUE(r.drive([&] { return f.ready(); }, /*stepNs=*/20'000'000)) << "the DDL must end, not hang";
    const auto& c = f.outcome().c;
    EXPECT_EQ(c.status, rc::OpStatus::SessionLost);
    EXPECT_EQ(payloadText(f).rfind("node unreachable", 0), 0u) << payloadText(f);
    EXPECT_GE(answers, 2) << "the redirect to the unreachable node was followed";
    r.passes(100, 20'000'000);
    EXPECT_EQ(cq.timesCompleted(f.token()), 1);
    EXPECT_EQ(r.client.testPendingControlOps(), 0u);
}

// Issue #97 (F1): the node is up and keeps answering NotLeader - the budget ends NotLeader with no "unreachable" claim.
TEST(ClientDdlRegression, DdlNotLeaderUntilBudgetEndsReportsNotLeaderWithoutUnreachableTextRegression) {
    rc::ClientConfig cfg = invokerCfg(4448, 4449);
    cfg.ddlRetryBudget = std::chrono::milliseconds(300);
    Rig r(cfg);
    r.connect();
    r.gw.templateHandlers[pc::DropTopic::TEMPLATE_ID] = [&](int ep, const uint8_t* f, std::size_t len) {
        auto m = pc::DropTopic::decode(f, len);
        ASSERT_TRUE(m.has_value());
        r.answerError(ep, pc::ErrorCode::NotLeader, "", m->sessionId, m->correlationId);
    };
    test_support::TestCq cq(r.client);
    auto f = cq.issue([&](rc::Completions& q, rc::Token t) { return r.client.dropTopic("t", {}, q, t); });
    ASSERT_TRUE(f.accepted());
    ASSERT_TRUE(r.drive([&] { return f.ready(); }, /*stepNs=*/20'000'000)) << "the DDL must end, not hang";
    const auto& c = f.outcome().c;
    EXPECT_EQ(c.status, rc::OpStatus::BrokerError);
    EXPECT_EQ(c.errorCode, static_cast<uint16_t>(pc::ErrorCode::NotLeader));
    const std::string text = payloadText(f);
    EXPECT_EQ(text.find("could not be reached"), std::string::npos) << text;
    EXPECT_EQ(text.find("unreachable"), std::string::npos) << text;
    EXPECT_NE(text.find("not leader: retry budget exhausted"), std::string::npos) << text;
}

// C17 adversarial review (#97): NodeUnreachable is a statement about the CURRENT resend, never sticky.  The DDL is
// answered NotLeader, redirected to a node that is down (resends refused: NodeUnreachable recorded), then that node
// comes up and answers NotLeader itself until the budget ends: the DDL ends BrokerError NotLeader with the NotLeader
// text, never SessionLost "node unreachable".
TEST(ClientDdlRegression, AdversarialANodeThatComesUpAndAnswersNotLeaderEndsNotLeaderRegression) {
    rc::ClientConfig cfg = invokerCfg(4450, 4451);
    cfg.ddlRetryBudget = std::chrono::milliseconds(2'000);
    Rig r(cfg);
    r.connect();
    const std::string later = udp(rune::client::testing::allocFreeUdpPort());   // nobody listens there yet
    int seedAnswers = 0, nodeAnswers = 0;
    int nodeEp = -1;
    r.gw.templateHandlers[pc::DropTopic::TEMPLATE_ID] = [&](int ep, const uint8_t* f, std::size_t len) {
        auto m = pc::DropTopic::decode(f, len);
        ASSERT_TRUE(m.has_value());
        if (ep == nodeEp) { ++nodeAnswers; r.answerError(ep, pc::ErrorCode::NotLeader, "", m->sessionId, m->correlationId); return; }
        if (seedAnswers++ == 0) r.answerError(ep, pc::ErrorCode::NotLeader, "", m->sessionId, m->correlationId);
        else                    r.answerError(ep, pc::ErrorCode::Redirect, later, m->sessionId, m->correlationId);
    };
    test_support::TestCq cq(r.client);
    auto f = cq.issue([&](rc::Completions& q, rc::Token t) { return r.client.dropTopic("t", {}, q, t); });
    ASSERT_TRUE(f.accepted());
    ASSERT_TRUE(r.drive([&] { return seedAnswers >= 2; }, 20'000'000)) << "redirected to the down node";
    r.passes(10, 20'000'000);                       // ~200 ms of refused resends toward the down node
    ASSERT_FALSE(f.ready()) << "inside the 2 s budget";
    nodeEp = r.gw.addNode(later, cfg.gatewayStreamId);   // the node comes up
    // Its UDP image connects on the real clock: hold the fake clock until the node's subscription sees the
    // client's publication, then step it 1 ms at a time (inside the 2 s budget) until the node answers a resend.
    ASSERT_TRUE(r.drive([&] { return r.gw.endpoints[static_cast<std::size_t>(nodeEp)]->sub &&
                                     r.gw.endpoints[static_cast<std::size_t>(nodeEp)]->sub->isConnected(); }, 0))
        << "the client's publication to the node never connected";
    ASSERT_TRUE(r.drive([&] { return nodeAnswers >= 1 || f.ready(); }, 1'000'000)) << "the node never got the resend";
    ASSERT_TRUE(r.drive([&] { return f.ready(); }, 20'000'000)) << "the DDL must end, not hang";
    EXPECT_GE(nodeAnswers, 1) << "the node that came up answered the resend";
    const auto& c = f.outcome().c;
    EXPECT_EQ(c.status, rc::OpStatus::BrokerError) << payloadText(f);
    EXPECT_EQ(c.errorCode, static_cast<uint16_t>(pc::ErrorCode::NotLeader));
    EXPECT_EQ(payloadText(f).find("unreachable"), std::string::npos) << payloadText(f);
    r.passes(100, 20'000'000);
    EXPECT_EQ(cq.timesCompleted(f.token()), 1);
    EXPECT_EQ(r.client.testPendingControlOps(), 0u);
}

// I1 (review): the catalog leader's session is lost after the redirected DDL
// reached it (it may have appended); the DDL is re-sent toward the seed with
// the SAME frame and requestId, the seed redirects it to the new leader, and
// the original success comes back — never a Timeout, never a fresh requestId.
TEST(ClientDdlRedirectRegression, LeaderSessionLostMidDdlIsResentWithTheSameRequestIdRegression) {
    Rig r(invokerCfg(4442, 4443));
    r.gw.uniqueSessionIds = true;
    const std::string aCh = udp(rune::client::testing::allocFreeUdpPort());
    const std::string cCh = udp(rune::client::testing::allocFreeUdpPort());
    const int a = r.gw.addNode(aCh, 4442);
    const int cNode = r.gw.addNode(cCh, 4442);
    r.connect();
    std::vector<Arrival> at;
    int seedAnswers = 0;
    r.gw.templateHandlers[pc::DropTopic::TEMPLATE_ID] = [&](int ep, const uint8_t* f, std::size_t len) {
        auto m = pc::DropTopic::decode(f, len);
        ASSERT_TRUE(m.has_value());
        at.push_back(Arrival{ep, m->sessionId, m->correlationId, m->ext.requestId});
        if (ep == 0) {   // B: A leads first, then C
            r.answerError(ep, pc::ErrorCode::Redirect, seedAnswers++ == 0 ? aCh : cCh, m->sessionId, m->correlationId);
        } else if (ep == a) {   // A appends, then dies before answering
            r.answerError(ep, pc::ErrorCode::SessionExpired, "expired", m->sessionId, 0);
            r.gw.deadEndpoints.insert(a);
        } else if (ep == cNode) {   // C: the dedupe answers the original result
            pc::DdlResponse resp; resp.sessionId = m->sessionId; resp.correlationId = m->correlationId;
            resp.objectId = 77; resp.newVersion = 3;
            r.gw.sendDdlResponse(ep, resp);
        }
    };
    test_support::TestCq cq(r.client);
    auto f = cq.issue([&](rc::Completions& q, rc::Token t) { return r.client.dropTopic("t", {}, q, t); });
    ASSERT_TRUE(r.drive([&] { return f.ready(); }, /*stepNs=*/10'000'000));
    ASSERT_TRUE(f.ok()) << f.describe();
    EXPECT_EQ(f.outcome().c.value, 77);
    ASSERT_EQ(at.size(), 4u);
    EXPECT_EQ(at[1].ep, a);
    EXPECT_EQ(at[3].ep, cNode);
    for (const Arrival& x : at) EXPECT_EQ(x.id, at[0].id);
    EXPECT_EQ(r.client.requestIdOf(f.outcome().c), at[0].id) << "the generated requestId is reported";
}

// I1 converse: the SEED's session is lost while the DDL is in flight on the
// leader's own session — the DDL is not failed; the leader's answer completes it.
TEST(ClientDdlRedirectRegression, SeedLossDoesNotFailADdlInFlightOnTheLeaderSessionRegression) {
    Rig r(invokerCfg(4444, 4445));
    r.gw.uniqueSessionIds = true;
    const std::string aCh = udp(rune::client::testing::allocFreeUdpPort());
    const int a = r.gw.addNode(aCh, 4444);
    r.connect();
    std::vector<Arrival> at;
    r.gw.templateHandlers[pc::DropView::TEMPLATE_ID] = [&](int ep, const uint8_t* f, std::size_t len) {
        auto m = pc::DropView::decode(f, len);
        ASSERT_TRUE(m.has_value());
        at.push_back(Arrival{ep, m->sessionId, m->correlationId, m->ext.requestId});
        if (ep == 0) r.answerError(ep, pc::ErrorCode::Redirect, aCh, m->sessionId, m->correlationId);
        // A holds its answer (below).
    };
    test_support::TestCq cq(r.client);
    auto f = cq.issue([&](rc::Completions& q, rc::Token t) { return r.client.dropView("v", {}, q, t); });
    ASSERT_TRUE(r.drive([&] { return at.size() == 2; }, /*stepNs=*/10'000'000));
    ASSERT_EQ(at[1].ep, a);
    r.gw.expireSession(at[0].sid);   // the seed's session dies
    r.passes(200, 1'000'000);
    ASSERT_FALSE(f.ready()) << "a seed loss failed a DDL in flight on the leader's session: " << f.describe();
    pc::DdlResponse resp; resp.sessionId = at[1].sid; resp.correlationId = at[1].corr; resp.objectId = 8;
    r.gw.sendDdlResponse(a, resp);
    ASSERT_TRUE(r.drive([&] { return f.ready(); }, /*stepNs=*/1'000'000));
    ASSERT_TRUE(f.ok()) << f.describe();
    EXPECT_EQ(f.outcome().c.value, 8);
    EXPECT_EQ(at.size(), 2u) << "no resend: the leader's session never left Connected";
}

// I1: the generated requestId is reported on every completion — OK and failure —
// as {Client::instanceId(), Completion.reserved0}; an explicit one is the caller's.
TEST(ClientDdlRedirectRegression, GeneratedRequestIdIsReportedOnEveryCompletionRegression) {
    Rig r(invokerCfg(4446, 4447));
    r.connect();
    std::vector<RequestId> wire;
    int n = 0;
    r.gw.templateHandlers[pc::AlterView::TEMPLATE_ID] = [&](int ep, const uint8_t* f, std::size_t len) {
        auto m = pc::AlterView::decode(f, len);
        ASSERT_TRUE(m.has_value());
        wire.push_back(m->ext.requestId);
        if (n++ == 0) {
            pc::DdlResponse resp; resp.sessionId = m->sessionId; resp.correlationId = m->correlationId;
            r.gw.sendDdlResponse(ep, resp);
        } else {
            r.answerError(ep, pc::ErrorCode::InvalidRequest, "no", m->sessionId, m->correlationId);
        }
    };
    static const uint8_t kPlan[] = {1};
    test_support::TestCq cq(r.client);
    auto ok = cq.issue([&](rc::Completions& q, rc::Token t) {
        return r.client.alterView("v", "k", 0, 1, std::span<const uint8_t>(kPlan, 1), {}, q, t);
    });
    ASSERT_TRUE(r.drive([&] { return ok.ready(); }));
    auto bad = cq.issue([&](rc::Completions& q, rc::Token t) {
        return r.client.alterView("v", "k", 0, 1, std::span<const uint8_t>(kPlan, 1), {}, q, t);
    });
    ASSERT_TRUE(r.drive([&] { return bad.ready(); }));
    ASSERT_EQ(wire.size(), 2u);
    EXPECT_EQ(wire[0].hi, r.client.instanceId());
    EXPECT_EQ(r.client.requestIdOf(ok.outcome().c), wire[0]);
    EXPECT_EQ(r.client.requestIdOf(bad.outcome().c), wire[1]);
    EXPECT_NE(wire[0], wire[1]);
}

// F-A (broker diag 2026-10-16), the composed client path: the broker SILENTLY forgot the session a DDL is in flight
// on (its lease reaped a quiet session: no reply channel, so no answer and no Pong can ever come).  The client's own
// stale-session detection (no inbound frame for responseTimeoutNs) resumes it; the DDL is re-sent on the NEW session
// with the SAME requestId and its answer completes it exactly once.  Never silence: if the session never comes back,
// exactly one explicit SessionLost / Timeout by the DDL budget.
namespace {
rc::ClientConfig silentForgetCfg(int32_t in, int32_t eg) {
    rc::ClientConfig c = invokerCfg(in, eg);
    c.responseTimeoutNs = 1'000'000'000LL;   // 1 s stale threshold (the default 10 s, scaled down)
    c.ddlTimeoutMs = 5'000;
    return c;
}
}  // namespace

TEST(ClientDdlRedirectRegression, BrokerSilentlyForgetsTheSessionMidDdlEndsInOneExplicitOutcomeRegression) {
    Rig r(silentForgetCfg(4448, 4449));
    r.gw.uniqueSessionIds = true;
    r.gw.replyPongs = false;   // the forgotten session: nothing ever comes back on it
    r.connect();
    const int connectsBefore = r.gw.connects;
    std::vector<Arrival> at;
    r.gw.templateHandlers[pc::DropTopic::TEMPLATE_ID] = [&](int ep, const uint8_t* f, std::size_t len) {
        auto m = pc::DropTopic::decode(f, len);
        ASSERT_TRUE(m.has_value());
        at.push_back(Arrival{ep, m->sessionId, m->correlationId, m->ext.requestId});
        if (at.size() == 1) return;   // the broker reaped the session: the DDL is dropped unanswered
        r.gw.replyPongs = true;       // the new session is healthy
        pc::DdlResponse resp; resp.sessionId = m->sessionId; resp.correlationId = m->correlationId;
        resp.objectId = 41; resp.newVersion = 2;
        r.gw.sendDdlResponse(ep, resp);
    };
    test_support::TestCq cq(r.client);
    auto f = cq.issue([&](rc::Completions& q, rc::Token t) { return r.client.dropTopic("t", {}, q, t); });
    ASSERT_TRUE(f.accepted());
    ASSERT_TRUE(r.drive([&] { return f.ready(); }, /*stepNs=*/10'000'000));
    ASSERT_TRUE(f.ok()) << f.describe();
    EXPECT_EQ(f.outcome().c.value, 41);
    ASSERT_EQ(at.size(), 2u) << "exactly one resend, after the client resumed its stale session";
    EXPECT_NE(at[1].sid, at[0].sid) << "the resend went out on the NEW session";
    EXPECT_EQ(at[1].id, at[0].id) << "the resend carries the SAME requestId (the broker dedupes it)";
    EXPECT_GT(r.gw.connects, connectsBefore) << "the stale session was re-established";
    r.passes(300, 10'000'000);
    EXPECT_EQ(cq.timesCompleted(f.token()), 1);
    EXPECT_EQ(r.client.testRoutedCtlAwaitingResend(), 0);
    EXPECT_EQ(r.client.testPendingControlOps(), 0u);
}

TEST(ClientDdlRedirectRegression, BrokerSilentlyForgetsTheSessionAndNeverReacceptsEndsExplicitlyByTheBudgetRegression) {
    Rig r(silentForgetCfg(4450, 4451));
    r.gw.uniqueSessionIds = true;
    r.gw.replyPongs = false;
    r.connect();
    std::vector<Arrival> at;
    r.gw.templateHandlers[pc::DropTopic::TEMPLATE_ID] = [&](int ep, const uint8_t* f, std::size_t len) {
        auto m = pc::DropTopic::decode(f, len);
        ASSERT_TRUE(m.has_value());
        at.push_back(Arrival{ep, m->sessionId, m->correlationId, m->ext.requestId});
        r.gw.replyConnects = false;   // reaped, and the broker never accepts this client again
    };
    test_support::TestCq cq(r.client);
    const int64_t issuedNs = g_now;
    auto f = cq.issue([&](rc::Completions& q, rc::Token t) { return r.client.dropTopic("t", {}, q, t); });
    ASSERT_TRUE(f.accepted());
    ASSERT_TRUE(r.drive([&] { return f.ready(); }, /*stepNs=*/10'000'000)) << "the DDL hung past its budget";
    const auto st = f.outcome().c.status;
    EXPECT_TRUE(st == rc::OpStatus::SessionLost || st == rc::OpStatus::Timeout) << f.describe();
    EXPECT_LE(g_now - issuedNs, 5'000'000'000LL + 200'000'000LL) << "ended after the DDL budget";
    EXPECT_EQ(at.size(), 1u);
    r.passes(300, 10'000'000);
    EXPECT_EQ(cq.timesCompleted(f.token()), 1);
    EXPECT_EQ(r.client.testRoutedCtlAwaitingResend(), 0);
    EXPECT_EQ(r.client.testPendingControlOps(), 0u);
}

// ===========================================================================
// #893: a routed control op in flight on a node session that has gone silent (no Pong for more than two keepalive
// intervals, long before the stale-session timeout) is also offered ONCE to the seed, which names the current leader.
// responseTimeoutNs is 60 s and the op deadline 120 s of injected time: only the suspect edge can explain a second
// arrival at the seed.  "Arrivals" = the frames the fake gateway received on one endpoint.  No wall-clock waits: every
// loop is capped and advances the injected clock.
// ===========================================================================
namespace {
constexpr int64_t kSuspectKaNs   = 50'000'000;   // keepalive interval
constexpr int64_t kSuspectStepNs = 5'000'000;    // injected time per pass

rc::ClientConfig suspectCfg(int32_t in, int32_t eg) {
    rc::ClientConfig c = invokerCfg(in, eg);
    c.keepaliveIntervalNs = kSuspectKaNs;
    c.responseTimeoutNs   = 60'000'000'000LL;
    c.ddlTimeoutMs        = 120'000;
    c.traceDdlRetries     = true;
    return c;
}

/// What one op kind needs from the shared scenarios: how to issue it, its template, and an Ok answer.
struct OpKit {
    const char*                                                        name;
    uint16_t                                                           tid;
    std::function<rc::OpResult(rc::Client&, rc::Completions&, rc::Token)> issue;
    std::function<void(Rig&, int ep, const pc::MessageHeader&, int64_t value)> answerOk;
};

OpKit dropKit() {
    return {"DropTopic", pc::DropTopic::TEMPLATE_ID,
            [](rc::Client& c, rc::Completions& q, rc::Token t) { return c.dropTopic("t", {}, q, t); },
            [](Rig& r, int ep, const pc::MessageHeader& h, int64_t value) {
                pc::DdlResponse resp; resp.sessionId = h.sessionId; resp.correlationId = h.correlationId;
                resp.objectId = static_cast<int32_t>(value); resp.newVersion = 3;
                r.gw.sendDdlResponse(ep, resp);
            }};
}
OpKit viewKit() {
    return {"GetViewRow", pc::GetViewRow::TEMPLATE_ID,
            [](rc::Client& c, rc::Completions& q, rc::Token t) { return c.getViewRow(0x40000007, 101, 0, q, t); },
            [](Rig& r, int ep, const pc::MessageHeader& h, int64_t value) {
                const std::vector<uint8_t> row(static_cast<std::size_t>(value), 7);
                pc::ViewRowResponse resp;
                resp.sessionId = h.sessionId; resp.correlationId = h.correlationId;
                resp.status = pc::ViewRowResponse::kOk; resp.rowLen = static_cast<uint32_t>(row.size());
                resp.rowBytes = row.data(); resp.rowBytesLen = static_cast<uint32_t>(row.size());
                std::vector<uint8_t> out(resp.encodedSize());
                const std::size_t n = resp.encode(out.data());
                r.gw.send(ep, out.data(), n);
            }};
}
OpKit leaseKit() {
    return {"LeaseKeepAlive", pc::LeaseKeepAlive::TEMPLATE_ID,
            [](rc::Client& c, rc::Completions& q, rc::Token t) {
                static const uint8_t kKey[] = {1, 2, 3, 4, 5, 6, 7, 8};
                return c.leaseKeepAlive(1, 0, std::span<const uint8_t>(kKey, sizeof kKey), 1, 0, q, t);
            },
            [](Rig& r, int ep, const pc::MessageHeader& h, int64_t value) {
                pc::LeaseKeepAliveResponse resp;
                const std::vector<pc::LeaseKeepAliveResponse::Entry> es{{0, 0, static_cast<uint64_t>(value)}};
                resp.sessionId = h.sessionId; resp.correlationId = h.correlationId; resp.final = 1; resp.entries = es;
                std::vector<uint8_t> out(resp.encodedSize());
                const std::size_t n = resp.encode(out.data());
                r.gw.send(ep, out.data(), n);
            }};
}

struct Suspect {
    Rig                                    r;
    std::vector<std::pair<std::string, int>> nodes;   // (channel, endpoint)
    std::vector<Arrival>                   at;
    std::vector<int64_t>                   atNs;
    int                                    redirectNotes = 0;   // followed Redirects (traceDdlRetries), accumulated
    Suspect(int32_t in, int32_t eg, int nodeCount) : r(suspectCfg(in, eg)) {
        r.gw.uniqueSessionIds = true;
        r.gw.replyPongs       = true;
        for (int i = 0; i < nodeCount; ++i) {
            std::string ch = udp(rune::client::testing::allocFreeUdpPort());
            const int ep = r.gw.addNode(ch, in);
            nodes.emplace_back(std::move(ch), ep);
        }
        r.connect();
    }
    [[nodiscard]] const std::string& ch(std::size_t i) const { return nodes[i].first; }
    [[nodiscard]] int                ep(std::size_t i) const { return nodes[i].second; }
    [[nodiscard]] int count(int endpoint) const {
        int n = 0;
        for (const Arrival& a : at) n += a.ep == endpoint ? 1 : 0;
        return n;
    }
    /// The nth (0-based) arrival at `endpoint`, or nullptr.
    [[nodiscard]] const Arrival* nth(int endpoint, int n) const {
        for (const Arrival& a : at) if (a.ep == endpoint && n-- == 0) return &a;
        return nullptr;
    }
    /// Installs the recording handler for `tid`; `fn(endpoint, header)` answers (or not).
    template <class F> void on(uint16_t tid, F fn) {
        r.gw.templateHandlers[tid] = [this, tid, fn](int endpoint, const uint8_t* f, std::size_t len) {
            const auto h = pc::MessageHeader::decode(f, len);
            ASSERT_TRUE(h.has_value());
            RequestId id{};
            if (tid == pc::DropTopic::TEMPLATE_ID) {
                const auto m = pc::DropTopic::decode(f, len);
                ASSERT_TRUE(m.has_value());
                id = m->ext.requestId;
            }
            at.push_back(Arrival{endpoint, h->sessionId, h->correlationId, id});
            atNs.push_back(g_now);
            fn(endpoint, *h);
        };
    }
    void redirect(int from, const std::string& to, const pc::MessageHeader& h) {
        r.answerError(from, pc::ErrorCode::Redirect, to, h.sessionId, h.correlationId);
    }
    /// Advances `passes` capped passes of injected time until `pred` holds.
    template <class P> bool until(P&& pred, int maxPasses) {
        for (int i = 0; i < maxPasses; ++i) {
            if (pred()) return true;
            r.passes(1, kSuspectStepNs);
        }
        return pred();
    }
    void run(int intervals) { r.passes(static_cast<int>(intervals * kSuspectKaNs / kSuspectStepNs), kSuspectStepNs); }
    int followedRedirects() {
        std::vector<rc::Client::DdlRetryNote> notes;
        (void)r.client.takeDdlRetryNotes(notes);
        for (const auto& n : notes) redirectNotes += n.kind == rc::Client::DdlRetryNote::Kind::Redirect ? 1 : 0;
        return redirectNotes;
    }
};

// Scenario 1: the seed redirects the op to node A; A swallows it and stops answering Pongs; the seed then names node
// C: the op is asked at the seed within three keepalive intervals of the silence and follows it to C.
void runSilentNodeFollowsSeedToNewLeader(const OpKit& kit, int32_t in, int32_t eg) {
    SCOPED_TRACE(kit.name);
    Suspect s(in, eg, 2);
    const int A = s.ep(0), C = s.ep(1);
    int seedN = 0;
    s.on(kit.tid, [&](int ep, const pc::MessageHeader& h) {
        if (ep == 0) s.redirect(0, ++seedN == 1 ? s.ch(0) : s.ch(1), h);
        else if (ep == A) s.r.gw.noPongEndpoints.insert(A);   // swallowed, and silent from now on
        else if (ep == C) kit.answerOk(s.r, C, h, 77);
    });
    test_support::TestCq cq(s.r.client);
    auto f = cq.issue([&](rc::Completions& q, rc::Token t) { return kit.issue(s.r.client, q, t); });
    ASSERT_TRUE(f.accepted());
    ASSERT_TRUE(s.until([&] { return s.count(A) == 1; }, 400)) << "the op never reached node A";
    const int64_t tSilent = g_now;
    const uint64_t staleBefore = s.r.client.testStaleLossesAt(1);
    ASSERT_TRUE(s.until([&] { return s.count(0) >= 2; }, 200))
        << "the seed was never asked: nothing reaches it before the 60 s stale timeout";
    EXPECT_LE(g_now - tSilent, 3 * kSuspectKaNs) << "asked later than three keepalive intervals after the silence";
    ASSERT_TRUE(s.until([&] { return f.ready(); }, 400)) << "the op never completed";
    ASSERT_TRUE(f.ok()) << f.describe();
    EXPECT_EQ(s.count(0), 2) << "the seed is asked exactly once";
    EXPECT_EQ(s.count(A), 1);
    EXPECT_EQ(s.count(C), 1);
    for (const Arrival& x : s.at) {
        EXPECT_EQ(x.id, s.at[0].id) << "the same requestId on every arrival";
        EXPECT_EQ(x.corr, s.at[0].corr) << "the same corrId on every arrival";
    }
    s.run(10);
    EXPECT_EQ(cq.timesCompleted(f.token()), 1);
    EXPECT_EQ(s.r.client.testRoutedCtlAwaitingResend(), 0);
    EXPECT_EQ(s.r.client.testPendingControlOps(), 0u);
    EXPECT_EQ(s.r.client.testStaleLossesAt(1), staleBefore) << "node A's session was never declared stale";
}
} // namespace

TEST(ClientDdlSuspectRegression, ADdlOnASilentNodeAsksTheSeedAndFollowsItToTheNewLeaderRegression) {
    runSilentNodeFollowsSeedToNewLeader(dropKit(), 4560, 4561);
}

TEST(ClientDdlSuspectRegression, AViewReadOnASilentNodeAsksTheSeedRegression) {
    runSilentNodeFollowsSeedToNewLeader(viewKit(), 4562, 4563);
}

TEST(ClientDdlSuspectRegression, ALeaseOpOnASilentNodeAsksTheSeedRegression) {
    runSilentNodeFollowsSeedToNewLeader(leaseKit(), 4564, 4565);
}

// The seed still names the silent node (it is slow, not dead): the copy changes nothing.  `laterHops` real Redirect
// hops follow once A answers; the ignored Redirect must not have used one of the kMaxDdlRedirects hops.
namespace {
void runSeedStillNamesTheSilentNode(int laterHops, int32_t in, int32_t eg) {
    SCOPED_TRACE(laterHops);
    Suspect s(in, eg, 3);
    const int A = s.ep(0), C = s.ep(1), D = s.ep(2);
    const OpKit kit = dropKit();
    s.on(kit.tid, [&](int ep, const pc::MessageHeader& h) {
        if (ep == 0) s.redirect(0, s.ch(0), h);   // the original AND the copy: "A leads"
        else if (ep == A) s.r.gw.noPongEndpoints.insert(A);
        else if (ep == C) s.redirect(C, s.ch(2), h);
        else if (ep == D) kit.answerOk(s.r, D, h, 9);
    });
    test_support::TestCq cq(s.r.client);
    auto f = cq.issue([&](rc::Completions& q, rc::Token t) { return kit.issue(s.r.client, q, t); });
    ASSERT_TRUE(f.accepted());
    ASSERT_TRUE(s.until([&] { return s.count(0) >= 2; }, 400)) << "the seed was never asked";
    const int hopsBefore = s.followedRedirects();
    ASSERT_EQ(hopsBefore, 1) << "only the original seed -> A Redirect was followed";
    s.run(20);
    EXPECT_EQ(s.count(0), 2) << "the seed is asked once per session episode, not every cycle";
    EXPECT_EQ(s.count(A), 1) << "the seed's Redirect to the node the op is already on is ignored";
    EXPECT_EQ(s.followedRedirects(), 1);
    EXPECT_FALSE(f.ready()) << "the op failed or completed without an answer: " << f.describe();
    EXPECT_EQ(s.r.client.testRoutedCtlAwaitingResend(), 0);
    // A wakes up and answers.
    const Arrival a0 = *s.nth(A, 0);
    pc::MessageHeader h; h.sessionId = a0.sid; h.correlationId = a0.corr;
    if (laterHops == 0) kit.answerOk(s.r, A, h, 5);
    else                s.redirect(A, s.ch(1), h);   // A -> C -> D answers Ok: two later hops, three in all
    ASSERT_TRUE(s.until([&] { return f.ready(); }, 400)) << "the op never completed";
    ASSERT_TRUE(f.ok()) << f.describe();
    EXPECT_EQ(f.outcome().c.value, laterHops == 0 ? 5 : 9);
    EXPECT_EQ(cq.timesCompleted(f.token()), 1);
    EXPECT_EQ(s.r.client.testPendingControlOps(), 0u);
}
} // namespace

TEST(ClientDdlSuspectRegression, ASeedThatStillNamesTheSilentNodeChangesNothingRegression) {
    runSeedStillNamesTheSilentNode(0, 4566, 4567);
}

// Two later real hops (A -> C -> D) make three in all = kMaxDdlRedirects: they all fit only if the ignored Redirect
// used no hop (with it the fourth would fail the op).
TEST(ClientDdlSuspectRegression, AnIgnoredSeedRedirectUsesNoHopRegression) {
    runSeedStillNamesTheSilentNode(2, 4568, 4569);
}

// The silent node answers late, after the op followed the seed to C: its answer completes the op once; C's later
// answer is dropped.
TEST(ClientDdlSuspectRegression, ALateAnswerFromTheSilentNodeCompletesTheOpOnceRegression) {
    Suspect s(4570, 4571, 2);
    const int A = s.ep(0), C = s.ep(1);
    const OpKit kit = dropKit();
    int seedN = 0;
    s.on(kit.tid, [&](int ep, const pc::MessageHeader& h) {
        if (ep == 0) s.redirect(0, ++seedN == 1 ? s.ch(0) : s.ch(1), h);
        else if (ep == A) s.r.gw.noPongEndpoints.insert(A);
        // C holds its answer
    });
    test_support::TestCq cq(s.r.client);
    auto f = cq.issue([&](rc::Completions& q, rc::Token t) { return kit.issue(s.r.client, q, t); });
    ASSERT_TRUE(f.accepted());
    ASSERT_TRUE(s.until([&] { return s.count(C) == 1; }, 400)) << "the op never followed the seed to C";
    ASSERT_FALSE(f.ready());
    const Arrival a0 = *s.nth(A, 0);
    const Arrival c0 = *s.nth(C, 0);
    pc::MessageHeader ha; ha.sessionId = a0.sid; ha.correlationId = a0.corr;
    kit.answerOk(s.r, A, ha, 11);
    ASSERT_TRUE(s.until([&] { return f.ready(); }, 200));
    ASSERT_TRUE(f.ok()) << f.describe();
    EXPECT_EQ(f.outcome().c.value, 11);
    pc::MessageHeader hc; hc.sessionId = c0.sid; hc.correlationId = c0.corr;
    kit.answerOk(s.r, C, hc, 22);
    s.run(10);
    EXPECT_EQ(cq.timesCompleted(f.token()), 1) << "C's later answer is dropped";
    EXPECT_EQ(f.outcome().c.value, 11);
    EXPECT_EQ(s.r.client.testPendingControlOps(), 0u);
    EXPECT_EQ(s.r.client.testRoutedCtlAwaitingResend(), 0);
}

// A silent seed is not asked (a copy offered onto it is swallowed); un-silenced, it is asked within three intervals.
TEST(ClientDdlSuspectRegression, ASilentSeedIsNotAskedRegression) {
    Suspect s(4572, 4573, 1);
    const int A = s.ep(0);
    const OpKit kit = dropKit();
    s.on(kit.tid, [&](int ep, const pc::MessageHeader& h) {
        if (ep == 0) s.redirect(0, s.ch(0), h);
        else if (ep == A) {   // A and the seed both stop answering Pongs
            s.r.gw.noPongEndpoints.insert(A);
            s.r.gw.noPongEndpoints.insert(0);
        }
    });
    test_support::TestCq cq(s.r.client);
    auto f = cq.issue([&](rc::Completions& q, rc::Token t) { return kit.issue(s.r.client, q, t); });
    ASSERT_TRUE(f.accepted());
    ASSERT_TRUE(s.until([&] { return s.count(A) == 1; }, 400));
    s.run(10);
    EXPECT_EQ(s.count(0), 1) << "a silent seed was asked";
    EXPECT_FALSE(f.ready());
    s.r.gw.noPongEndpoints.erase(0);   // only the seed comes back
    const int64_t tBack = g_now;
    ASSERT_TRUE(s.until([&] { return s.count(0) >= 2; }, 200)) << "the live seed was never asked";
    EXPECT_LE(g_now - tBack, 3 * kSuspectKaNs);
}

// An op the seed itself took is never asked at this edge, silent seed or not.
TEST(ClientDdlSuspectRegression, AnOpInFlightOnTheSeedSessionIsNotAskedTwiceRegression) {
    Suspect s(4574, 4575, 0);
    const OpKit kit = dropKit();
    s.on(kit.tid, [&](int, const pc::MessageHeader&) {});   // the seed swallows it
    test_support::TestCq cq(s.r.client);
    auto f = cq.issue([&](rc::Completions& q, rc::Token t) { return kit.issue(s.r.client, q, t); });
    ASSERT_TRUE(f.accepted());
    ASSERT_TRUE(s.until([&] { return s.count(0) == 1; }, 400));
    s.run(20);
    EXPECT_EQ(s.count(0), 1) << "a live seed was offered a second copy";
    s.r.gw.noPongEndpoints.insert(0);
    s.run(10);
    EXPECT_EQ(s.count(0), 1) << "a silent seed was offered a second copy";
    EXPECT_FALSE(f.ready());
}

// The seed's transport refuses the copy for some cycles: the op stays in flight on A unchanged, and the copy goes out
// exactly once when the offer is accepted.
TEST(ClientDdlSuspectRegression, ASeedRefusingTheCopyIsAskedAgainNextCycleRegression) {
    Suspect s(4576, 4577, 1);
    const int A = s.ep(0);
    const OpKit kit = dropKit();
    s.on(kit.tid, [&](int ep, const pc::MessageHeader& h) {
        if (ep == 0) s.redirect(0, s.ch(0), h);
        else if (ep == A) {
            s.r.gw.noPongEndpoints.insert(A);
            s.r.client.testForceIngressOffers(Offer::Retry, 8);   // the seed's transport refuses the next offers
        }
    });
    test_support::TestCq cq(s.r.client);
    auto f = cq.issue([&](rc::Completions& q, rc::Token t) { return kit.issue(s.r.client, q, t); });
    ASSERT_TRUE(f.accepted());
    ASSERT_TRUE(s.until([&] { return s.count(A) == 1; }, 400));
    ASSERT_TRUE(s.until([&] { return s.count(0) >= 2; }, 400))
        << "the copy never went out once the offers were accepted";
    s.run(20);
    EXPECT_EQ(s.count(0), 2) << "exactly one copy";
    EXPECT_EQ(s.count(A), 1);
    EXPECT_EQ(s.followedRedirects(), 1);
    EXPECT_FALSE(f.ready());
}

// A's session is lost and re-established (a new episode): the op is offered on it again, goes silent again, and the
// seed is asked once more - once per episode, never twice in one.
TEST(ClientDdlSuspectRegression, AReconnectedNodeSessionMayBeAskedAgainRegression) {
    Suspect s(4578, 4579, 1);
    const int A = s.ep(0);
    const OpKit kit = dropKit();
    s.on(kit.tid, [&](int ep, const pc::MessageHeader& h) {
        if (ep == 0) s.redirect(0, s.ch(0), h);
        else if (ep == A) s.r.gw.noPongEndpoints.insert(A);   // silent again on every episode
    });
    test_support::TestCq cq(s.r.client);
    auto f = cq.issue([&](rc::Completions& q, rc::Token t) { return kit.issue(s.r.client, q, t); });
    ASSERT_TRUE(f.accepted());
    ASSERT_TRUE(s.until([&] { return s.count(0) >= 2; }, 400)) << "the first episode never asked the seed";
    s.run(10);
    ASSERT_EQ(s.count(0), 2) << "asked twice in one episode";
    const int64_t firstSid = s.nth(A, 0)->sid;
    s.r.gw.expireSession(firstSid);   // A's session dies; it reconnects (a new episode)
    ASSERT_TRUE(s.until([&] { return s.count(0) >= 3; }, 400))
        << "the lost session's op was never re-sent toward the seed";
    ASSERT_TRUE(s.until([&] { return s.count(A) >= 2; }, 400)) << "the op never reached A's new session";
    ASSERT_TRUE(s.until([&] { return s.count(0) >= 4; }, 400)) << "the second episode never asked the seed";
    s.run(20);
    EXPECT_EQ(s.count(0), 4) << "asked twice in the second episode";
    EXPECT_EQ(s.count(A), 2);
    EXPECT_FALSE(f.ready());
}

// A bootstrap's challenge exchange is per node: a bootstrap in flight on a silent node is never asked at the suspect
// edge (the copy would be the signed frame, answered against a challenge the seed never issued for this node).
TEST(ClientDdlSuspectRegression, ABootstrapOpIsNotAskedAtTheSuspectEdgeRegression) {
    Suspect s(4580, 4581, 1);
    const int A = s.ep(0);
    s.on(pc::SecurityBootstrapChallengeRequest::TEMPLATE_ID, [&](int ep, const pc::MessageHeader& h) {
        if (ep == 0) s.redirect(0, s.ch(0), h);
        else if (ep == A) s.r.gw.noPongEndpoints.insert(A);   // swallowed, silent from now on
    });
    const std::string keyFile = std::string(64, 'a') + "\n";
    const std::vector<uint8_t> keyBytes(keyFile.begin(), keyFile.end());
    test_support::TestCq cq(s.r.client);
    auto f = cq.issue([&](rc::Completions& q, rc::Token t) {
        return s.r.client.bootstrapSecurity("admin", "a one-time password 24", keyBytes, q, t);
    });
    ASSERT_TRUE(f.accepted());
    ASSERT_TRUE(s.until([&] { return s.count(A) == 1; }, 400)) << "the challenge request never reached node A";
    s.run(20);
    EXPECT_EQ(s.count(0), 1) << "the seed was asked about a bootstrap";
    EXPECT_EQ(s.count(A), 1);
    for (const uint16_t t : s.r.gw.bootstrapOrder)
        EXPECT_EQ(t, pc::SecurityBootstrapChallengeRequest::TEMPLATE_ID) << "the signed bootstrap frame was sent";
    EXPECT_FALSE(f.ready());
}

// The seed, asked at the suspect edge, is the new leader and answers the op itself: no Redirect, NotLeader or session
// loss would drop the route hint naming the silent node, so asking the seed drops it: the next op starts on the seed.
TEST(ClientDdlSuspectRegression, SuspectEdgeDropsTheRouteHintOfTheSilentNodeRegression) {
    Suspect s(4582, 4583, 1);
    const int A = s.ep(0);
    const OpKit kit = viewKit();
    int phase = 0;   // 0: learn the hint through the seed's Redirect; 1: A goes silent, the seed answers; 2: the seed
    s.on(kit.tid, [&](int ep, const pc::MessageHeader& h) {
        if (ep == 0) {
            if (phase == 0) s.redirect(0, s.ch(0), h);
            else kit.answerOk(s.r, 0, h, 7);
        } else if (ep == A) {
            if (phase == 0) kit.answerOk(s.r, A, h, 5);
            else s.r.gw.noPongEndpoints.insert(A);   // swallowed, and silent from now on
        }
    });
    test_support::TestCq cq(s.r.client);
    auto f1 = cq.issue([&](rc::Completions& q, rc::Token t) { return kit.issue(s.r.client, q, t); });
    ASSERT_TRUE(f1.accepted());
    ASSERT_TRUE(s.until([&] { return f1.ready(); }, 400)) << "the first op never completed";
    ASSERT_TRUE(f1.ok()) << f1.describe();
    EXPECT_EQ(s.r.client.routeHintLearned(), 1u) << "the Redirect to A is remembered";
    EXPECT_EQ(s.r.client.routeHintHits(), 0u);
    EXPECT_EQ(s.r.client.routeHintCleared(), 0u);

    phase = 1;
    auto f2 = cq.issue([&](rc::Completions& q, rc::Token t) { return kit.issue(s.r.client, q, t); });
    ASSERT_TRUE(f2.accepted());
    ASSERT_TRUE(s.until([&] { return f2.ready(); }, 400)) << "the second op never completed";
    ASSERT_TRUE(f2.ok()) << f2.describe();
    EXPECT_EQ(f2.outcome().c.value, 7) << "the seed answered the copy";
    EXPECT_EQ(s.r.client.routeHintHits(), 1u) << "the second op started on the remembered node";
    EXPECT_EQ(s.count(A), 2) << "the first op's frame and the second's";
    EXPECT_EQ(s.r.client.routeHintCleared(), 1u) << "asking the seed must drop the hint of the silent node";

    phase = 2;
    const int seedBefore = s.count(0), nodeBefore = s.count(A);
    auto f3 = cq.issue([&](rc::Completions& q, rc::Token t) { return kit.issue(s.r.client, q, t); });
    ASSERT_TRUE(f3.accepted());
    ASSERT_TRUE(s.until([&] { return f3.ready(); }, 400)) << "the third op never completed";
    ASSERT_TRUE(f3.ok()) << f3.describe();
    EXPECT_EQ(s.count(0), seedBefore + 1) << "the third op is first offered to the seed";
    EXPECT_EQ(s.count(A), nodeBefore) << "the third op must not start on the silent node";
    EXPECT_EQ(s.r.client.routeHintHits(), 1u);
    EXPECT_EQ(s.r.client.routeHintCleared(), 1u);
    EXPECT_EQ(s.r.client.testPendingControlOps(), 0u);
}
