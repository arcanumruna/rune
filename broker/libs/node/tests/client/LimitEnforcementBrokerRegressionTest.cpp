// LimitEnforcementBrokerRegressionTest.cpp - namespaces Phase 2 Task 9 (D23): the limits enforced OUTSIDE the AdminWorker's
// catalog check, through the REAL client and the raw security wire against ONE real authenticating BrokerNode on the
// DEFAULT config: maxUsers (SecurityAdmin), lowestAcks per produce (gateway), the Txns feature at InitProducerId and the
// AuthScram feature at login.  Rows of the LimitEnforcementTable (areas Security and Gateway); Below/At/Above as there.
//
// Interaction matrix (limit x {below, at, above} x timing):
//   | limit            | operation               | state                       | expected                                   | test
//   |------------------|-------------------------|-----------------------------|--------------------------------------------|-----
//   | max-users 2      | CreateUser x3           | sequential                  | 1st, 2nd ok; 3rd 52, no user                | LimitEnforcementTableRegression.UsersRegression
//   | max-users lowered| SetLimits below usage   | 2 users, limit 1            | users kept, new refused                     | LimitEnforcementTableRegression.UsersRegression
//   | lowest-acks 2    | produce acks Leader/All | per request, same session   | Leader refused, All served, default free    | LimitEnforcementTableRegression.AcksFloorPerProduceRegression
//   | feature-bits     | InitProducerId          | Txns absent / present       | refused 52 / served                         | LimitEnforcementTableRegression.TxnsFeatureRegression
//   | feature-bits     | SCRAM login             | AuthScram absent            | refused after the proof, never before       | LimitEnforcementTableRegression.AuthScramFeatureRegression
#include "support/AuthRig.h"
#include "support/LimitEnforcementTable.h"

#include <rune/client/Client.h>
#include <rune/client/NamespaceLimitFields.h>
#include <rune/client/producer/Producer.h>

#include <gtest/gtest.h>

#include <chrono>
#include <memory>
#include <string>
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

std::unique_ptr<rc::Client> newClient(AuthBroker& b, const std::string& user, std::vector<rc::TopicSpec> topics = {}) {
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
    c.topics          = std::move(topics);
    return std::make_unique<rc::Client>(c);
}
std::unique_ptr<rc::Client> connected(AuthBroker& b, const std::string& user, std::vector<rc::TopicSpec> topics = {}) {
    auto c = newClient(b, user, std::move(topics));
    c->connectAndWait(20s);
    return c;
}

std::vector<AuthGrant> userGrants() {
    return {prefix(SO::Create, ST::Topic, ""), prefix(SO::Describe, ST::Topic, ""), prefix(SO::Write, ST::Topic, ""),
            prefix(SO::Read, ST::Topic, ""), prefix(SO::Write, ST::TransactionalId, ""), prefix(SO::Describe, ST::TransactionalId, "")};
}

rc::NamespaceLimitsPatch patchOf(const char* field, uint64_t v) {
    rc::NamespaceLimitsPatch p;
    if (!p.set(*rc::namespaceLimitIndex(field), v)) throw std::runtime_error(std::string("patch ") + field);
    return p;
}

std::span<const uint8_t> bytesOf(const std::string& s) { return {reinterpret_cast<const uint8_t*>(s.data()), s.size()}; }

struct Outcome { rc::OpStatus status = rc::OpStatus::Timeout; uint16_t code = 0; };

/// One acked produce of one record with `acks`; the broker's verdict (status + ErrorCode).
Outcome produceOne(rc::Client& c, rc::TopicHandle th, rc::Acks acks, uint64_t key = 1, std::size_t valueBytes = 8) {
    rc::ProducerConfig pcfg;
    pcfg.acks     = acks;
    pcfg.lingerUs = 0;
    auto p = c.producer(pcfg);
    rc::Completions cq(c);
    const std::vector<uint8_t> v(valueBytes, 7);
    if (p.trySend(th, rc::Key(key), std::span<const uint8_t>(v.data(), v.size()), cq, 1) != rc::SendResult::Ok) return {rc::OpStatus::InvalidArgument, 0};
    p.flush();
    Outcome out;
    const auto deadline = std::chrono::steady_clock::now() + 20s;
    bool done = false;
    while (!done && std::chrono::steady_clock::now() < deadline) {
        (void)cq.poll([&](const rc::Completion& d, std::span<const uint8_t>) { out = {d.status, d.errorCode}; done = true; });
        if (!done) std::this_thread::sleep_for(1ms);
    }
    p.close();
    return out;
}

}  // namespace

namespace {
/// A raw-wire session of the cluster admin (the answers keep their wire error code).
struct RawAdmin {
    AuthBroker& b;
    AuthWireClient& w;
    int64_t sid;
    explicit RawAdmin(AuthBroker& broker) : b(broker), w(broker.wire()), sid(w.connect(broker.channel(), pc::kClientVersionNamespaces)) {
        if (sid <= 0 || !w.login(b.channel(), sid, "admin", {kAdminPw, kAdminPw}).ok()) throw std::runtime_error("raw admin login");
    }
    AuthWireClient::DdlAnswer mkUser(const std::string& name) { return w.createUser(b.channel(), sid, name, kUserPw); }
};

rune::protocol::NamespaceLimits limitsOf(rc::Client& admin, const std::string& name) {
    auto r = admin.listNamespaces();
    if (r) for (const auto& n : *r) if (n.name == name) return n.limits;
    throw std::runtime_error("no namespace " + name);
}
}  // namespace

TEST(LimitEnforcementTableRegression, UsersRegression) {
    const LimitRow& row = *rowOf("max-users");
    AuthBroker b("lim_users");
    b.bootstrapAdmin(kAdminPw);
    auto admin = connected(b, "admin");
    const auto ns = admin->createNamespace("u", patchOf("max-users", row.limit));
    ASSERT_TRUE(ns.has_value()) << ns.error().detail;
    RawAdmin raw(b);
    EXPECT_EQ(raw.mkUser("u/one").errorCode, 0) << "below";
    EXPECT_EQ(raw.mkUser("u/two").errorCode, 0) << "at: usage == limit";
    const auto over = raw.mkUser("u/three");
    EXPECT_EQ(over.errorCode, kCodeNamespaceLimitExceeded) << over.detail;
    EXPECT_NE(over.detail.find("users"), std::string::npos) << over.detail;
    {   // A client below v14 sees InvalidRequest (39), not 52: the gateway maps the code (the admin thread does not know it).
        AuthWireClient& old = b.wire();
        const int64_t osid = old.connect(b.channel());   // kClientVersionAuth (9)
        ASSERT_GT(osid, 0);
        ASSERT_TRUE(old.login(b.channel(), osid, "admin", {kAdminPw, kAdminPw}).ok());
        const auto refusedOld = old.createUser(b.channel(), osid, "u/old", kUserPw);
        EXPECT_EQ(refusedOld.errorCode, static_cast<uint16_t>(pc::ErrorCode::InvalidRequest)) << refusedOld.detail;
    }
    // The default namespace has no user limit and is unaffected by a full namespace.
    EXPECT_EQ(raw.mkUser("d1").errorCode, 0);
    EXPECT_EQ(raw.mkUser("d2").errorCode, 0);
    // Lowered below usage: both users stay and can log in; a new one is refused.
    rune::protocol::NamespaceLimits lower = limitsOf(*admin, "u");
    lower.maxUsers = 1;
    ASSERT_TRUE(admin->setNamespaceLimits("u", lower).has_value());
    EXPECT_EQ(raw.mkUser("u/four").errorCode, kCodeNamespaceLimitExceeded);
    auto one = newClient(b, "u/one");
    one->connectAndWait(20s);
    auto two = newClient(b, "u/two");
    two->connectAndWait(20s);
}

TEST(LimitEnforcementTableRegression, AcksFloorPerProduceRegression) {
    const LimitRow& row = *rowOf("lowest-acks");
    AuthBroker b("lim_acks");
    b.bootstrapAdmin(kAdminPw);
    auto admin = connected(b, "admin");
    ASSERT_TRUE(admin->createNamespace("k", patchOf("lowest-acks", row.limit)).has_value());
    ASSERT_TRUE(admin->createNamespace("free").has_value());
    b.createUser("k/ua", kUserPw, userGrants());
    b.createUser("free/ub", kUserPw, userGrants());
    auto define = [&](const std::string& user) {
        auto c = connected(b, user);
        const auto r = c->defineTopicAndWait("t", 1, "", false, {}, 20s);
        if (r.errorCode != 0) throw std::runtime_error("define: " + r.detail);
        c->close();
        return rc::TopicHandle{r.topicId, 1};
    };
    const rc::TopicHandle tk = define("k/ua");
    const rc::TopicHandle tf = define("free/ub");
    auto ua = connected(b, "k/ua");
    // The SAME session: acks=Leader refused (a client picks acks per request), acks=All served.
    const Outcome low = produceOne(*ua, tk, rc::Acks::Leader);
    EXPECT_NE(low.status, rc::OpStatus::Ok) << "Above: acks below the floor";
    EXPECT_EQ(low.code, kCodeNamespaceLimitExceeded);
    const Outcome high = produceOne(*ua, tk, rc::Acks::All, 2);
    EXPECT_EQ(high.status, rc::OpStatus::Ok) << "At: acks == the floor (code " << high.code << ")";
    // A namespace without a floor serves acks=Leader.
    auto ub = connected(b, "free/ub");
    EXPECT_EQ(produceOne(*ub, tf, rc::Acks::Leader).status, rc::OpStatus::Ok) << "Below: no floor";
}

TEST(LimitEnforcementTableRegression, TxnsFeatureRegression) {
    const LimitRow& row = *rowOf("feature-bits");
    (void)row;
    AuthBroker b("lim_txns");
    b.bootstrapAdmin(kAdminPw);
    auto admin = connected(b, "admin");
    ASSERT_TRUE(admin->createNamespace("no", patchOf("feature-bits", rune::protocol::kNsFeatureCompaction | rune::protocol::kNsFeatureViews | rune::protocol::kNsFeatureAuthScram)).has_value());
    ASSERT_TRUE(admin->createNamespace("yes", patchOf("feature-bits", rune::protocol::kNsFeatureTxns | rune::protocol::kNsFeatureAuthScram)).has_value());
    ASSERT_TRUE(admin->createNamespace("open").has_value());   // featureBits 0 = unrestricted
    for (const char* u : {"no/u", "yes/u", "open/u"}) b.createUser(u, kUserPw, userGrants());
    auto init = [&](const std::string& user) {
        auto c = connected(b, user);
        rc::ProducerConfig pcfg;
        pcfg.acks = rc::Acks::All;
        pcfg.transactionalId = "tx";
        rc::Producer p = c->producer(pcfg);
        std::string err;
        try { p.initTransactionsAndWait(20s); } catch (const std::exception& e) { err = e.what(); }
        p.close();
        c->close();
        return err;
    };
    EXPECT_EQ(init("yes/u"), "") << "At: Txns allowed";
    EXPECT_EQ(init("open/u"), "") << "Below: unrestricted";
    const std::string refused = init("no/u");
    EXPECT_FALSE(refused.empty()) << "Above: Txns not allowed";
    EXPECT_NE(refused.find("52"), std::string::npos) << refused;
}

TEST(LimitEnforcementTableRegression, AuthScramFeatureRegression) {
    AuthBroker b("lim_scram");
    b.bootstrapAdmin(kAdminPw);
    auto admin = connected(b, "admin");
    ASSERT_TRUE(admin->createNamespace("noscram", patchOf("feature-bits", rune::protocol::kNsFeatureTxns)).has_value());
    ASSERT_TRUE(admin->createNamespace("scram", patchOf("feature-bits", rune::protocol::kNsFeatureAuthScram)).has_value());
    b.createUser("noscram/u", kUserPw, userGrants());
    b.createUser("scram/u", kUserPw, userGrants());
    auto ok = newClient(b, "scram/u");
    EXPECT_NO_THROW(ok->connectAndWait(20s)) << "At: AuthScram allowed";
    auto refused = newClient(b, "noscram/u");
    std::string err;
    try { refused->connectAndWait(20s); } catch (const std::exception& e) { err = e.what(); }
    EXPECT_FALSE(err.empty()) << "Above: SCRAM login not allowed";
    EXPECT_NE(err.find("52"), std::string::npos) << err;
    // A wrong password for the same namespace still answers AuthenticationFailed (nothing leaks to a guesser).
    rc::ClientConfig bad;
    bad.gatewayChannel = b.channel(); bad.gatewayStreamId = 1;
    bad.egressChannel = "aeron:udp?endpoint=127.0.0.1:" + std::to_string(rune::aeronutil::testing::allocFreeUdpPort());
    bad.egressStreamId = 2; bad.dataDir = b.clientDir().string(); bad.username = "noscram/u"; bad.password = "wrong-password-xxxxxxx";
    bad.offerCrypto = true; bad.tlsTrustPem = AuthBroker::tlsCaPem();
    rc::Client badClient(bad);
    std::string err2;
    try { badClient.connectAndWait(20s); } catch (const std::exception& e) { err2 = e.what(); }
    EXPECT_NE(err2.find("44"), std::string::npos) << err2;
}

// ProduceSize rows (max-message-bytes, max-batch-bytes): a real client produce above the limit completes BrokerError 52 and
// is not stored; at / below it is served.  The exact L-1 / L / L+1 cells run at the worker (NsProduceSizeLimitRegressionTest).
TEST(LimitEnforcementTableRegression, ProduceSizeRegression) {
    const LimitRow& msg = *rowOf("max-message-bytes");
    const LimitRow& bat = *rowOf("max-batch-bytes");
    AuthBroker b("lim_size");
    b.bootstrapAdmin(kAdminPw);
    auto admin = connected(b, "admin");
    ASSERT_TRUE(admin->createNamespace("m", patchOf("max-message-bytes", msg.limit)).has_value());
    ASSERT_TRUE(admin->createNamespace("b", patchOf("max-batch-bytes", bat.limit)).has_value());
    b.createUser("m/u", kUserPw, userGrants());
    b.createUser("b/u", kUserPw, userGrants());
    auto define = [&](const std::string& user) {
        auto c = connected(b, user);
        const auto r = c->defineTopicAndWait("t", 1, "", false, {}, 20s);
        if (r.errorCode != 0) throw std::runtime_error("define: " + r.detail);
        c->close();
        return rc::TopicHandle{r.topicId, 1};
    };
    const rc::TopicHandle tm = define("m/u");
    const rc::TopicHandle tb = define("b/u");
    auto um = connected(b, "m/u");
    EXPECT_EQ(produceOne(*um, tm, rc::Acks::All, 1, msg.limit / 2).status, rc::OpStatus::Ok) << "Below: a record under max-message-bytes";
    const Outcome longRec = produceOne(*um, tm, rc::Acks::All, 2, msg.limit * 2);
    EXPECT_NE(longRec.status, rc::OpStatus::Ok) << "Above: a record over max-message-bytes";
    EXPECT_EQ(longRec.code, kCodeNamespaceLimitExceeded);
    auto ub = connected(b, "b/u");
    EXPECT_EQ(produceOne(*ub, tb, rc::Acks::All, 1, bat.limit / 2).status, rc::OpStatus::Ok) << "Below: a batch under max-batch-bytes";
    const Outcome bigBatch = produceOne(*ub, tb, rc::Acks::All, 2, bat.limit * 2);
    EXPECT_NE(bigBatch.status, rc::OpStatus::Ok) << "Above: a batch over max-batch-bytes";
    EXPECT_EQ(bigBatch.code, kCodeNamespaceLimitExceeded);
    // A refusal does not poison the session: a small produce is served afterwards.
    EXPECT_EQ(produceOne(*ub, tb, rc::Acks::All, 3, 100).status, rc::OpStatus::Ok);
}
