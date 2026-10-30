// NamespaceSuspendRegressionTest.cpp - namespaces Phase 2 Task 3: a REAL client against a REAL authenticating
// BrokerNode (default config).  Suspend ReadOnly / RejectAll, resume, delete; cluster-admin exemption; the old-client
// code; the login of a suspended namespace.
#include "support/AuthRig.h"

#include <rune/client/Client.h>
#include <rune/client/Completions.h>
#include <rune/client/producer/Producer.h>
#include <rune/client/subscriber/Subscription.h>

#include <gtest/gtest.h>

#include <atomic>
#include <memory>
#include <string>
#include <thread>

using namespace rune::node;
using namespace rune::node::test;
using namespace rune::node::test::grants;
namespace pc = rune::protocol::client;
namespace rc = rune::client;
using namespace std::chrono_literals;

namespace {

constexpr const char* kAdminPw = "namespace-admin-password-1";
constexpr const char* kUserPw = "namespace-user-password-12";
constexpr uint16_t kSuspended = static_cast<uint16_t>(pc::ErrorCode::NamespaceSuspended);

rc::ClientConfig newClientCfg(AuthBroker& b, const std::string& user, const std::string& password,
                              int32_t announceVersion = 0) {
    rc::ClientConfig c;
    c.gatewayChannel  = b.channel();
    c.gatewayStreamId = 1;
    c.egressChannel   = "aeron:udp?endpoint=127.0.0.1:" + std::to_string(rune::aeronutil::testing::allocFreeUdpPort());
    c.egressStreamId  = 2;
    c.dataDir         = b.clientDir().string();
    c.username        = user;
    c.password        = password;
    c.offerCrypto     = true;
    c.tlsTrustPem     = AuthBroker::tlsCaPem();
    c.testAnnounceClientVersion = announceVersion;
    return c;
}
std::unique_ptr<rc::Client> newClient(AuthBroker& b, const std::string& user, const std::string& password,
                                      int32_t announceVersion = 0) {
    return std::make_unique<rc::Client>(newClientCfg(b, user, password, announceVersion));
}
std::unique_ptr<rc::Client> connected(AuthBroker& b, const std::string& user, const std::string& password,
                                      int32_t announceVersion = 0) {
    auto c = newClient(b, user, password, announceVersion);
    c->connectAndWait(20s);
    return c;
}
std::string connectError(rc::Client& c) {
    try { c.connectAndWait(20s); } catch (const std::runtime_error& e) { return e.what(); }
    return {};
}

struct Outcome { rc::OpStatus status = rc::OpStatus::Timeout; uint16_t code = 0; };

/// One acked produce of one record; the broker's verdict.
Outcome produceOne(rc::Client& c, rc::TopicHandle th, uint64_t key, std::chrono::seconds wait = 20s) {
    rc::ProducerConfig pcfg;
    pcfg.acks     = rc::Acks::Leader;
    pcfg.lingerUs = 0;
    auto p = c.producer(pcfg);
    rc::Completions cq(c);
    const uint8_t v[8] = {1, 2, 3, 4, 5, 6, 7, 8};
    if (p.trySend(th, rc::Key(key), std::span<const uint8_t>(v, 8), cq, 1) != rc::SendResult::Ok) return {rc::OpStatus::InvalidArgument, 0};
    p.flush();
    Outcome out;
    const auto deadline = std::chrono::steady_clock::now() + wait;
    bool done = false;
    while (!done && std::chrono::steady_clock::now() < deadline) {
        (void)cq.poll([&](const rc::Completion& d, std::span<const uint8_t>) { out = {d.status, d.errorCode}; done = true; });
        if (!done) std::this_thread::sleep_for(1ms);
    }
    p.close();
    return out;
}

size_t fetchCount(rc::Client& c, rc::TopicHandle th, size_t want) {
    std::atomic<size_t> n{0};
    rc::SubscribeOptions o;
    o.from = rc::From::earliest();
    rc::Subscription sub = c.subscribe(th, 0, o, [&](const rc::Record&) { ++n; });
    const auto deadline = std::chrono::steady_clock::now() + 20s;
    while (n.load() < want && std::chrono::steady_clock::now() < deadline) std::this_thread::sleep_for(5ms);
    sub.unsubscribe();
    return n.load();
}

/// True when the client's session no longer answers (the broker ended it): a ping fails or times out.
bool sessionEnded(rc::Client& c) {
    try {
        const uint8_t p[1] = {0};
        (void)c.pingAndWait(std::span<const uint8_t>(p, 1), 2s);
        return false;
    } catch (const std::exception&) {
        return true;
    }
}

/// The SessionsRevoked counter reached at least `want` (the count also holds a session a closed client left behind).
bool revokedAtLeast(AuthBroker& b, int64_t want) {
    const auto deadline = std::chrono::steady_clock::now() + 10s;
    while (std::chrono::steady_clock::now() < deadline) {
        if (b.counter(typeIdOf(RuneMetric::SessionsRevoked)).value_or(0) >= want) return true;
        std::this_thread::sleep_for(10ms);
    }
    return false;
}

/// Namespaces a and b, a user in each with data grants, a topic `t` in each, the cluster admin's client.
struct Fix {
    AuthBroker b;
    std::unique_ptr<rc::Client> admin;
    rc::TopicHandle tA{}, tB{};
    explicit Fix(const std::string& tag) : b(tag) {
        b.bootstrapAdmin(kAdminPw);
        admin = connected(b, "admin", kAdminPw);
        for (const char* ns : {"a", "b"}) {
            auto r = admin->createNamespace(ns);
            if (!r) throw std::runtime_error(std::string("createNamespace ") + ns + ": " + r.error().detail);
        }
        for (const char* u : {"a/ua", "a/ua2", "b/ub"})
            b.createUser(u, kUserPw,
                         {prefix(SO::Create, ST::Topic, ""), prefix(SO::Describe, ST::Topic, ""),
                          prefix(SO::Write, ST::Topic, ""), prefix(SO::Read, ST::Topic, "")});
        tA = defineT("a/ua");
        tB = defineT("b/ub");
    }
    rc::TopicHandle defineT(const std::string& user) {
        auto c = connected(b, user, kUserPw);
        const auto r = c->defineTopicAndWait("t", 1, "", {}, 20s);
        if (r.errorCode != 0) throw std::runtime_error("define t: " + r.detail);
        c->close();
        return rc::TopicHandle{r.topicId, 1};
    }
};

}  // namespace

// ReadOnly: the namespace's produce and DDL get NamespaceSuspended (51); its reads are still served; another
// namespace is untouched.
TEST(NamespaceSuspendRegression, ReadOnlyRefusesProduceAllowsFetchRegression) {
    Fix f("nssusp_ro");
    auto ua = connected(f.b, "a/ua", kUserPw);
    auto ub = connected(f.b, "b/ub", kUserPw);
    for (uint64_t k = 0; k < 3; ++k) {
        const Outcome o = produceOne(*ua, f.tA, k);
        ASSERT_EQ(o.status, rc::OpStatus::Ok) << o.code;
    }
    ASSERT_TRUE(f.admin->suspendNamespace("a", pc::NsSuspendMode::ReadOnly).has_value());

    const Outcome refused = produceOne(*ua, f.tA, 99);
    EXPECT_NE(refused.status, rc::OpStatus::Ok);
    EXPECT_EQ(refused.code, kSuspended) << "status " << static_cast<int>(refused.status);
    EXPECT_EQ(fetchCount(*ua, f.tA, 3), 3u) << "reads are still served";
    const auto def = ua->defineTopicAndWait("t2", 1, "", {}, 20s);
    EXPECT_EQ(def.errorCode, kSuspended) << "DDL is refused";
    EXPECT_EQ(produceOne(*ub, f.tB, 1).status, rc::OpStatus::Ok) << "another namespace is unaffected";
}

// RejectAll: every session of the namespace ends; the other namespace's session lives.
TEST(NamespaceSuspendRegression, RejectAllClosesSessionsRegression) {
    Fix f("nssusp_ra");
    auto a1 = connected(f.b, "a/ua", kUserPw);
    auto a2 = connected(f.b, "a/ua2", kUserPw);
    auto ub = connected(f.b, "b/ub", kUserPw);
    ASSERT_EQ(produceOne(*a1, f.tA, 1).status, rc::OpStatus::Ok);
    const auto before = f.b.counter(typeIdOf(RuneMetric::SessionsRevoked)).value_or(0);
    ASSERT_TRUE(f.admin->suspendNamespace("a", pc::NsSuspendMode::RejectAll).has_value());
    ASSERT_TRUE(revokedAtLeast(f.b, before + 2)) << "both `a` sessions revoked";
    EXPECT_TRUE(sessionEnded(*a1));
    EXPECT_TRUE(sessionEnded(*a2));
    EXPECT_EQ(produceOne(*ub, f.tB, 1).status, rc::OpStatus::Ok);
}

// e2e step 12: a producer that is open (records in flight, acks=all) when RejectAll ends its session.  Every record
// completes, and close() returns promptly instead of waiting out a drain that can never be answered.
TEST(NamespaceSuspendRegression, RejectAllOpenProducerThenCloseReturnsPromptlyRegression) {
    Fix f("nssusp_close");
    std::unique_ptr<rc::Client> a1;
    {
        // As the e2e's world-server client: the low-latency idle mode.
        auto c = newClientCfg(f.b, "a/ua", kUserPw);
        c.idleMode = rc::IdleMode::LowLatency;
        a1 = std::make_unique<rc::Client>(c);
    }
    a1->connectAndWait(20s);
    rc::ProducerConfig pcfg;
    pcfg.acks             = rc::Acks::All;
    pcfg.lingerUs         = 1;
    pcfg.batchBytes       = 1;
    pcfg.maxInFlight      = 16;
    pcfg.produceTimeoutMs = 20000;
    {
        auto p = a1->producer(pcfg);
        rc::Completions cq(*a1);
        const uint8_t v[8] = {1, 2, 3, 4, 5, 6, 7, 8};
        auto one = [&](uint64_t k) {
            rc::Completion out;
            out.status = rc::OpStatus::Timeout;
            if (p.trySend(f.tA, rc::Key(k), std::span<const uint8_t>(v, 8), cq, k) != rc::SendResult::Ok) return out;
            p.flush();
            (void)cq.waitFor(k, 20s, [&](const rc::Completion& d, std::span<const uint8_t>) { out = d; });
            return out;
        };
        ASSERT_EQ(one(1).status, rc::OpStatus::Ok);
        ASSERT_TRUE(f.admin->suspendNamespace("a", pc::NsSuspendMode::RejectAll).has_value());
        const auto t0 = std::chrono::steady_clock::now();
        for (uint64_t k = 2; k < 60; ++k) (void)one(k);
        EXPECT_LT(std::chrono::steady_clock::now() - t0, 30s) << "every record after the session end completes";
    }
    const auto t1 = std::chrono::steady_clock::now();
    a1->close();
    EXPECT_LT(std::chrono::steady_clock::now() - t1, 10s) << "close() after a RejectAll session end returns promptly";
}

// RejectAll ends each session with a code its client knows as "the session ended": NamespaceSuspended (51) for a v14+
// client, SessionRevoked (46) below (a v13 client has no 51; the InvalidRequest it used to get was no session end, so it
// kept a dead session and every pending op waited out its timeout).  Both clients reject for good, nothing reconnects.
TEST(NamespaceSuspendRegression, RejectAllEndsEachSessionWithACodeItsClientKnowsRegression) {
    Fix f("nssusp_racode");
    auto v14 = connected(f.b, "a/ua", kUserPw);
    auto v13 = connected(f.b, "a/ua2", kUserPw, 13);
    ASSERT_EQ(produceOne(*v14, f.tA, 1).status, rc::OpStatus::Ok);
    // A live subscription on each: the revoke ends it with the session's code (readable from this thread).
    std::atomic<size_t> n14{0}, n13{0};
    rc::SubscribeOptions o;
    o.from = rc::From::earliest();
    rc::Subscription s14 = v14->subscribe(f.tA, 0, o, [&](const rc::Record&) { ++n14; });
    rc::Subscription s13 = v13->subscribe(f.tA, 0, o, [&](const rc::Record&) { ++n13; });
    const auto bound = std::chrono::steady_clock::now() + 20s;
    while ((n14.load() == 0 || n13.load() == 0) && std::chrono::steady_clock::now() < bound) std::this_thread::sleep_for(5ms);
    ASSERT_GT(n14.load(), 0u);
    ASSERT_GT(n13.load(), 0u);
    ASSERT_TRUE(f.admin->suspendNamespace("a", pc::NsSuspendMode::RejectAll).has_value());
    const auto deadline = std::chrono::steady_clock::now() + 10s;
    while (std::chrono::steady_clock::now() < deadline && (!s14.finished() || !s13.finished()))
        std::this_thread::sleep_for(5ms);
    ASSERT_TRUE(s14.finished());
    ASSERT_TRUE(s13.finished());
    EXPECT_EQ(s14.brokerErrorCode(), static_cast<uint16_t>(pc::ErrorCode::NamespaceSuspended));
    EXPECT_EQ(s13.brokerErrorCode(), static_cast<uint16_t>(pc::ErrorCode::SessionRevoked));
    const auto ended = std::chrono::steady_clock::now() + 10s;
    while (std::chrono::steady_clock::now() < ended && (v14->state() != rc::session::SessionState::Rejected ||
                                                        v13->state() != rc::session::SessionState::Rejected))
        std::this_thread::sleep_for(5ms);
    EXPECT_EQ(v14->state(), rc::session::SessionState::Rejected);
    EXPECT_EQ(v13->state(), rc::session::SessionState::Rejected);
}

TEST(NamespaceSuspendRegression, RejectAllLoginRightProofSuspendedRegression) {
    Fix f("nssusp_login");
    ASSERT_TRUE(f.admin->suspendNamespace("a", pc::NsSuspendMode::RejectAll).has_value());
    auto c = newClient(f.b, "a/ua", kUserPw);
    const std::string err = connectError(*c);
    EXPECT_NE(err.find("51"), std::string::npos) << err;
}

TEST(NamespaceSuspendRegression, RejectAllLoginWrongProofAuthFailedRegression) {
    Fix f("nssusp_wrong");
    ASSERT_TRUE(f.admin->suspendNamespace("a", pc::NsSuspendMode::RejectAll).has_value());
    auto c = newClient(f.b, "a/ua", "not the right password");
    const std::string err = connectError(*c);
    EXPECT_NE(err.find("AuthenticationFailed"), std::string::npos) << err;
    EXPECT_NE(err.find("44"), std::string::npos) << err;
}

TEST(NamespaceSuspendRegression, ResumeRestoresServiceRegression) {
    Fix f("nssusp_resume");
    auto ua = connected(f.b, "a/ua", kUserPw);
    ASSERT_TRUE(f.admin->suspendNamespace("a", pc::NsSuspendMode::ReadOnly).has_value());
    ASSERT_EQ(produceOne(*ua, f.tA, 1).code, kSuspended);
    ASSERT_TRUE(f.admin->resumeNamespace("a").has_value());
    EXPECT_EQ(produceOne(*ua, f.tA, 2).status, rc::OpStatus::Ok) << "the same session produces again";

    ASSERT_TRUE(f.admin->suspendNamespace("a", pc::NsSuspendMode::RejectAll).has_value());
    ASSERT_TRUE(f.admin->resumeNamespace("a").has_value());
    auto again = newClient(f.b, "a/ua", kUserPw);
    EXPECT_EQ(connectError(*again), "") << "a new login works after resume";
    EXPECT_EQ(produceOne(*again, f.tA, 3).status, rc::OpStatus::Ok);
}

// The cluster admin lives in `default`: suspending `default` does not touch its session.  The gateway lets its
// requests through (the catalog itself still refuses a CreateTopic in a non-ACTIVE namespace: 39, not 51).
TEST(NamespaceSuspendRegression, ClusterAdminExemptRegression) {
    Fix f("nssusp_admin");
    const auto r = f.admin->defineTopicAndWait("dt", 1, "", {}, 20s);
    ASSERT_EQ(r.errorCode, 0) << r.detail;
    const rc::TopicHandle th{r.topicId, 1};
    ASSERT_TRUE(f.admin->suspendNamespace("default", pc::NsSuspendMode::ReadOnly).has_value());
    EXPECT_EQ(produceOne(*f.admin, th, 1).status, rc::OpStatus::Ok) << "the admin still produces";
    EXPECT_EQ(fetchCount(*f.admin, th, 1), 1u);
    const auto ddl = f.admin->defineTopicAndWait("dt2", 1, "", {}, 20s);
    EXPECT_EQ(ddl.errorCode, 0) << ddl.detail << " (the cluster admin's DDL applies in a suspended namespace)";
    ASSERT_TRUE(f.admin->resumeNamespace("default").has_value());
}

// A client below version 14 does not know code 51: the gate answers InvalidRequest (39).
TEST(NamespaceSuspendRegression, OldClientGetsInvalidRequestRegression) {
    Fix f("nssusp_old");
    f.b.createUser("olduser", kUserPw,
                   {prefix(SO::Create, ST::Topic, "old."), prefix(SO::Describe, ST::Topic, "old."),
                    prefix(SO::Write, ST::Topic, "old.")});
    auto old = connected(f.b, "olduser", kUserPw, 13);
    const auto d = old->defineTopicAndWait("old.t", 1, "", {}, 20s);
    ASSERT_EQ(d.errorCode, 0) << d.detail;
    const rc::TopicHandle th{d.topicId, 1};
    ASSERT_EQ(produceOne(*old, th, 1).status, rc::OpStatus::Ok);
    ASSERT_TRUE(f.admin->suspendNamespace("default", pc::NsSuspendMode::ReadOnly).has_value());
    // The cluster admin is exempt, `olduser` is not.
    const Outcome o = produceOne(*old, th, 2);
    EXPECT_NE(o.status, rc::OpStatus::Ok);
    EXPECT_EQ(o.code, static_cast<uint16_t>(pc::ErrorCode::InvalidRequest)) << "status " << static_cast<int>(o.status);
    ASSERT_TRUE(f.admin->resumeNamespace("default").has_value());
}

TEST(NamespaceSuspendRegression, DeleteClosesSessionsAndRefusesLoginRegression) {
    Fix f("nssusp_delete");
    auto a1 = connected(f.b, "a/ua", kUserPw);
    auto ub = connected(f.b, "b/ub", kUserPw);
    const auto before = f.b.counter(typeIdOf(RuneMetric::SessionsRevoked)).value_or(0);
    const auto del = f.admin->deleteNamespace("a");
    ASSERT_TRUE(del.has_value()) << del.error().errorCode << " " << del.error().detail;
    ASSERT_TRUE(revokedAtLeast(f.b, before + 1)) << "the `a` session was closed";
    EXPECT_TRUE(sessionEnded(*a1));
    auto again = newClient(f.b, "a/ua", kUserPw);
    const std::string err = connectError(*again);
    EXPECT_NE(err.find("44"), std::string::npos) << err;
    EXPECT_EQ(produceOne(*ub, f.tB, 1).status, rc::OpStatus::Ok);
}
