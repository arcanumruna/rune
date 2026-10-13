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
#include "../support/InvokerGateway.h"
#include "../support/TestCompletions.h"
#include <rune/client/Client.h>
#include <rune/protocol/client/SchemaMessages.h>
#include <gtest/gtest.h>

#include <arpa/inet.h>
#include <netinet/in.h>
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

int freePort() {   // never a fixed UDP port (GroupRoutingRegressionTest MEDIUM-6)
    const int fd = ::socket(AF_INET, SOCK_DGRAM, 0);
    sockaddr_in a{};
    a.sin_family      = AF_INET;
    a.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    a.sin_port        = 0;
    (void)::bind(fd, reinterpret_cast<sockaddr*>(&a), sizeof a);
    socklen_t len = sizeof a;
    (void)::getsockname(fd, reinterpret_cast<sockaddr*>(&a), &len);
    ::close(fd);
    return ntohs(a.sin_port);
}
std::string udp(int port) { return "aeron:udp?endpoint=127.0.0.1:" + std::to_string(port); }

rc::ClientConfig invokerCfg(int32_t in, int32_t eg) {
    rc::ClientConfig c;
    c.gatewayChannel = "aeron:ipc"; c.gatewayStreamId = in; c.egressChannel = "aeron:ipc"; c.egressStreamId = eg;
    c.dataDir = "/tmp/ddl_timing_" + std::to_string(::getpid()) + "_" + std::to_string(g_ctr++);
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
    case DdlOp::DefineTopic: return c.defineTopic("t", 1, "", {}, false, rc::Persistence::Disk, {}, {}, q, t);
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
    }
    return "?";
}

class ClientDdlTimingMatrixRegression : public ::testing::TestWithParam<std::tuple<DdlOp, Timing>> {};

TEST_P(ClientDdlTimingMatrixRegression, ExactlyOneCompletionAndNoLeftoverState) {
    const auto [op, timing] = GetParam();
    rc::ClientConfig cfg = invokerCfg(4440, 4441);
    if (timing == Timing::ResendPastBudgetEnd) cfg.ddlRetryBudget = std::chrono::milliseconds(300);
    Rig r(cfg);
    r.gw.uniqueSessionIds = true;   // every node session its own id (a session-fatal answer names one)
    const std::string l1Ch = udp(freePort());
    const std::string l2Ch = udp(freePort());
    const int l1 = r.gw.addEndpoint(l1Ch, 4440);
    const int l2 = r.gw.addEndpoint(l2Ch, 4440);
    const std::string nowhere = udp(freePort());   // no node listens there
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
        case Timing::ResendPastBudgetEnd:
            r.answerError(ep, pc::ErrorCode::NotLeader, "", h->sessionId, h->correlationId);
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
        }
    };

    test_support::TestCq cq(r.client);
    auto f = cq.issue([&](rc::Completions& q, rc::Token t) { return issue(r.client, op, q, t); });
    ASSERT_TRUE(f.accepted());
    auto awaiting = [&] { return r.client.testDdlAwaitingResend() == 1; };
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
        EXPECT_EQ(r.client.testDdlAwaitingResend(), 1);
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
        r.client.testForceIngressOffers(Offer::Retry, 1'000'000);   // the seed refuses every offer
        ASSERT_TRUE(r.drive([&] { return f.ready(); }, /*stepNs=*/20'000'000));
        r.client.testForceIngressOffers(Offer::Accepted, 0);
        break;
    case Timing::LeaderSessionLost:
    case Timing::FatalOfferOnRedirect:
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
        EXPECT_EQ(c.status, rc::OpStatus::BrokerError);
        EXPECT_EQ(c.errorCode, static_cast<uint16_t>(pc::ErrorCode::NotLeader));
        EXPECT_NE(payloadText(f).find("could not be reached"), std::string::npos) << payloadText(f);
        break;
    }
    // Nothing more, ever: later duty cycles deliver no second completion, and
    // the DDL bookkeeping is back to zero.
    if (!closed) r.passes(300, 10'000'000);
    EXPECT_EQ(cq.timesCompleted(f.token()), 1);
    EXPECT_EQ(r.client.testDdlAwaitingResend(), 0);
    EXPECT_EQ(r.client.testPendingControlOps(), 0u);
}

INSTANTIATE_TEST_SUITE_P(
    AllOpsAllTimings, ClientDdlTimingMatrixRegression,
    ::testing::Combine(::testing::Values(DdlOp::DefineTopic, DdlOp::AlterTopic, DdlOp::CreateView, DdlOp::DropTopic,
                                         DdlOp::DropView, DdlOp::AlterView, DdlOp::AlterTopicConfig),
                       ::testing::Values(Timing::CloseWhileAwaitingResend, Timing::CloseWhileHeldForNode,
                                         Timing::SeedLostWhileAwaitingResend, Timing::DuplicateAnswerWhileAwaiting,
                                         Timing::LeaderSessionLost, Timing::FatalOfferOnResend,
                                         Timing::FatalOfferOnRedirect, Timing::ResendPastBudgetEnd)),
    [](const ::testing::TestParamInfo<std::tuple<DdlOp, Timing>>& i) {
        return std::string(name(std::get<0>(i.param))) + "_" + name(std::get<1>(i.param));
    });

// I1 (review): the catalog leader's session is lost after the redirected DDL
// reached it (it may have appended); the DDL is re-sent toward the seed with
// the SAME frame and requestId, the seed redirects it to the new leader, and
// the original success comes back — never a Timeout, never a fresh requestId.
TEST(ClientDdlRedirectRegression, LeaderSessionLostMidDdlIsResentWithTheSameRequestIdRegression) {
    Rig r(invokerCfg(4442, 4443));
    r.gw.uniqueSessionIds = true;
    const std::string aCh = udp(freePort());
    const std::string cCh = udp(freePort());
    const int a = r.gw.addEndpoint(aCh, 4442);
    const int cNode = r.gw.addEndpoint(cCh, 4442);
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
    const std::string aCh = udp(freePort());
    const int a = r.gw.addEndpoint(aCh, 4444);
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
