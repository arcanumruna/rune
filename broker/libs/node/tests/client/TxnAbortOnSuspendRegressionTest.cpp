// TxnAbortOnSuspendRegressionTest.cpp - namespaces Phase 2 Task 4: an open transaction is aborted when its namespace
// is suspended (ReadOnly or RejectAll) or deleted, and when its session is revoked for any reason.
//
//   * TransactionCoordinator::abortAllForSession / abortAllForNamespace (unit): the count, only Ongoing transactions,
//     only that session / namespace, idempotent, zero allocation.
//   * A REAL client against a REAL authenticating BrokerNode (default config): a transactional producer holds an open
//     transaction of 10 records; the cluster admin acts; a read_committed reader then sees none of the 10 and the
//     reader's last-stable-offset moved past them (a plain SENTINEL record written after the transaction becomes
//     visible only once the ABORT marker is in the log), and the producer's next call fails.
//
// Interaction matrix (event x namespace x timing):
//   | event                      | namespaces           | timing                    | expected                       | test
//   |----------------------------|----------------------|---------------------------|--------------------------------|-----
//   | coordinator, by session    | 2 sessions, 3 states | sequential                | only Ongoing of that session   | AbortAllForSession...
//   | coordinator, by namespace  | a, b, default        | sequential                | only Ongoing of that nsId      | AbortAllForNamespace...
//   | coordinator, allocation    | a                    | steady state              | 0 allocations                  | ...ZeroAllocation
//   | suspend ReadOnly           | a (open txn), b      | txn open                  | aborted, next call 51          | SuspendReadOnlyAborts
//   | suspend RejectAll          | a (open txn), b      | txn open                  | aborted, session closed        | SuspendRejectAllAborts
//   | delete                     | a (open txn), b      | txn open                  | aborted, session closed        | DeleteAborts
//   | revoke: SET PASSWORD TERM. | a (open txn)         | txn open                  | aborted, session closed        | RevokeReasonPasswordChange
//   | revoke: DROP USER          | a (open txn)         | txn open                  | aborted, session closed        | RevokeReasonUserDropped
//   | grant revoked (WRITE)      | a (open txn)         | txn open                  | aborted, next call refused     | RevokeReasonGrantRevoked
//   | suspend + delete           | a and b open txns    | b commits after the op    | b committed, a aborted         | OtherNamespaceUntouched
#include "support/AllocWindow.h"
#include "support/AuthRig.h"

#include <rune/client/Client.h>
#include <rune/client/Completions.h>
#include <rune/client/producer/Producer.h>
#include <rune/client/subscriber/Subscription.h>
#include <rune/coordination/NsScopedId.h>
#include <rune/transaction/TransactionCoordinator.h>

#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

using namespace rune::node;
using namespace rune::node::test;
using namespace rune::node::test::grants;
namespace pc = rune::protocol::client;
namespace rc = rune::client;
namespace co = rune::coordination;
namespace tx = rune::transaction;
using namespace std::chrono_literals;

// ── the coordinator methods ───────────────────────────────────────────────────────────────────────────────────
namespace {

/// Opens an Ongoing transaction `id` (partition t/0) driven by session `sid` (0 = the session is not recorded).
void openTxn(tx::TransactionCoordinator& coord, const std::string& id, int64_t sid, int64_t nowNs = 1'000) {
    const auto r = coord.initProducerId(id, 30'000, nowNs);
    ASSERT_TRUE(r.ok()) << id;
    const auto add = coord.addPartitions(id, r.producerId, r.producerEpoch, {tx::TopicPartition{"t", 0}}, nowNs + 1'000);
    ASSERT_TRUE(add.ok()) << id;
    if (sid != 0) coord.noteSession(id, sid);
    ASSERT_EQ(coord.metadata(id)->state, tx::TxnState::Ongoing) << id;
}

}  // namespace

TEST(TxnAbortCoordinatorRegression, AbortAllForSessionOnlyThatSessionOngoingRegression) {
    tx::TransactionCoordinator coord;
    openTxn(coord, "s1a", 1);
    openTxn(coord, "s1b", 1);
    openTxn(coord, "s2a", 2);
    openTxn(coord, "nosession", 0);
    // Session 1's third transaction already ended (PrepareCommit): never touched.
    openTxn(coord, "s1c", 1);
    const auto* c = coord.metadata("s1c");
    ASSERT_EQ(coord.endTransaction("s1c", c->producerId, c->producerEpoch, /*commit=*/true, 2'000).errorCode, 0);
    ASSERT_EQ(coord.metadata("s1c")->state, tx::TxnState::PrepareCommit);
    // And one that never began a transaction (Empty) with session 1 recorded.
    ASSERT_TRUE(coord.initProducerId("s1empty", 30'000, 1'000).ok());
    coord.noteSession("s1empty", 1);

    EXPECT_EQ(coord.abortAllForSession(1, 3'000), 2u) << "s1a and s1b";
    for (const char* id : {"s1a", "s1b"}) {
        EXPECT_EQ(coord.metadata(id)->state, tx::TxnState::PrepareAbort) << id;
        EXPECT_TRUE(coord.metadata(id)->markersPending) << id << ": the caller must write the ABORT markers";
    }
    EXPECT_EQ(coord.metadata("s1c")->state, tx::TxnState::PrepareCommit) << "an ending transaction is left alone";
    EXPECT_EQ(coord.metadata("s1empty")->state, tx::TxnState::Empty);
    EXPECT_EQ(coord.metadata("s2a")->state, tx::TxnState::Ongoing) << "another session's";
    EXPECT_EQ(coord.metadata("nosession")->state, tx::TxnState::Ongoing);
    EXPECT_EQ(coord.abortAllForSession(1, 4'000), 0u) << "idempotent";
    EXPECT_EQ(coord.abortAllForSession(0, 4'000), 0u) << "session 0 = unknown: never matches";
    EXPECT_EQ(coord.metadata("nosession")->state, tx::TxnState::Ongoing);
    EXPECT_EQ(coord.abortAllForSession(99, 4'000), 0u) << "a session with no transaction";
    EXPECT_EQ(coord.abortAllForSession(2, 5'000), 1u);
    EXPECT_EQ(coord.metadata("s2a")->state, tx::TxnState::PrepareAbort);
}

TEST(TxnAbortCoordinatorRegression, AbortAllForNamespaceOnlyOngoingOfThatNamespaceRegression) {
    tx::TransactionCoordinator coord;
    const std::string a1 = co::scopedId(1, "x1"), a2 = co::scopedId(1, "x2"), a3 = co::scopedId(1, "x3");
    const std::string b1 = co::scopedId(2, "x1"), d1 = "x1";
    openTxn(coord, a1, 1);
    openTxn(coord, a2, 1);
    openTxn(coord, a3, 1);
    openTxn(coord, b1, 2);
    openTxn(coord, d1, 3);
    const auto* m = coord.metadata(a3);
    ASSERT_EQ(coord.endTransaction(a3, m->producerId, m->producerEpoch, /*commit=*/false, 2'000).errorCode, 0);
    ASSERT_EQ(coord.metadata(a3)->state, tx::TxnState::PrepareAbort);
    ASSERT_FALSE(coord.metadata(a3)->markersPending) << "the client-driven abort writes its own markers";

    EXPECT_EQ(coord.abortAllForNamespace(1, 3'000), 2u) << "a1 and a2; a3 is already ending";
    EXPECT_TRUE(coord.metadata(a1)->markersPending);
    EXPECT_TRUE(coord.metadata(a2)->markersPending);
    EXPECT_FALSE(coord.metadata(a3)->markersPending) << "an ending transaction keeps its own marker path";
    EXPECT_EQ(coord.metadata(b1)->state, tx::TxnState::Ongoing);
    EXPECT_EQ(coord.metadata(d1)->state, tx::TxnState::Ongoing);
    EXPECT_EQ(coord.abortAllForNamespace(1, 4'000), 0u);
    EXPECT_EQ(coord.abortAllForNamespace(7, 4'000), 0u);
}

TEST(TxnAbortCoordinatorRegression, AbortAllZeroAllocationRegression) {
    tx::TransactionCoordinator coord;
    for (int i = 0; i < 8; ++i) openTxn(coord, co::scopedId(1, "x" + std::to_string(i)), 5);
    for (int i = 0; i < 8; ++i) openTxn(coord, co::scopedId(2, "y" + std::to_string(i)), 6);
    {
        rune::testing::AllocWindow w;
        EXPECT_EQ(coord.abortAllForSession(99, 3'000), 0u);
        EXPECT_EQ(coord.abortAllForNamespace(99, 3'000), 0u);
        EXPECT_EQ(coord.abortAllForSession(5, 3'000), 8u);
        EXPECT_EQ(coord.abortAllForNamespace(2, 3'000), 8u);
        EXPECT_EQ(coord.abortAllForSession(5, 3'000), 0u);
        EXPECT_EQ(w.count(), 0u) << "the abort iterates txnMap_ in place";
    }
}

// ── a real client against a real broker ───────────────────────────────────────────────────────────────────────
namespace {

constexpr const char* kAdminPw = "namespace-admin-password-1";
constexpr const char* kUserPw = "namespace-user-password-12";
constexpr const char* kNewPw = "namespace-user-password-34";

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
    auto client = std::make_unique<rc::Client>(c);
    client->connectAndWait(20s);
    return client;
}

std::vector<AuthGrant> userGrants() {
    return {prefix(SO::Create, ST::Topic, ""), prefix(SO::Describe, ST::Topic, ""), prefix(SO::Write, ST::Topic, ""),
            prefix(SO::Read, ST::Topic, ""), prefix(SO::Read, ST::Group, ""), prefix(SO::Describe, ST::Group, ""),
            prefix(SO::Write, ST::TransactionalId, ""), prefix(SO::Describe, ST::TransactionalId, "")};
}

/// Namespaces a and b, a user in each (`a/ua`, `b/ub`) with data + txn grants, a topic `t` in each.
struct Fix {
    AuthBroker b;
    std::unique_ptr<rc::Client> admin;
    rc::TopicSpec tA, tB;
    explicit Fix(const std::string& tag) : b(tag) {
        b.bootstrapAdmin(kAdminPw);
        admin = newClient(b, "admin");
        for (const char* ns : {"a", "b"}) {
            auto r = admin->createNamespace(ns);
            if (!r) throw std::runtime_error(std::string("createNamespace ") + ns + ": " + r.error().detail);
        }
        for (const char* u : {"a/ua", "b/ub"}) b.createUser(u, kUserPw, userGrants());
        tA = defineT("a/ua");
        tB = defineT("b/ub");
    }
    rc::TopicSpec defineT(const std::string& user) {
        auto c = newClient(b, user);
        const auto r = c->defineTopicAndWait("t", 1, "", false, {}, 20s);
        if (r.errorCode != 0) throw std::runtime_error("define t: " + r.detail);
        c->close();
        return rc::TopicSpec{"t", r.topicId, 1};
    }
    static rc::TopicHandle handleOf(const rc::TopicSpec& s) { return rc::TopicHandle{s.topicId, 1}; }
};

std::span<const uint8_t> bytesOf(const std::string& s) { return {reinterpret_cast<const uint8_t*>(s.data()), s.size()}; }

bool waitFor(const std::function<bool()>& pred, std::chrono::seconds limit = 30s) {
    const auto deadline = std::chrono::steady_clock::now() + limit;
    while (std::chrono::steady_clock::now() < deadline) {
        if (pred()) return true;
        std::this_thread::sleep_for(2ms);
    }
    return pred();
}

/// A transactional producer (id "tx") of `c` with an OPEN transaction of 10 records "<tag>0".."<tag>9" that reached the
/// log (flushed: the partition is registered with the coordinator).
struct OpenTxn {
    rc::Producer p;
    OpenTxn(rc::Client& c, const char* topic, const std::string& tag, int n = 10) : p(make(c)) {
        p.initTransactionsAndWait(30s);
        p.beginTransaction();
        const rc::TopicHandle th = c.topic(topic);
        for (int i = 0; i < n; ++i) {
            const std::string v = tag + std::to_string(i);
            p.send(th, rc::Key(static_cast<int64_t>(i)), bytesOf(v));
        }
        p.flushAndWait(30s);
    }
    static rc::Producer make(rc::Client& c) {
        rc::ProducerConfig tcfg;
        tcfg.acks            = rc::Acks::All;
        tcfg.transactionalId = "tx";
        return c.producer(tcfg);
    }
    /// What the producer's next transactional call does: the error text, or "" when it succeeded.
    std::string commitError() {
        try { p.commitTransactionAndWait(15s); } catch (const std::exception& e) { return e.what(); }
        return {};
    }
};

/// One plain acked record `v`.
void produceOne(rc::Client& c, rc::TopicHandle th, const std::string& v) {
    rc::ProducerConfig pcfg;
    pcfg.acks = rc::Acks::All;
    rc::Producer p = c.producer(pcfg);
    p.send(th, rc::Key(int64_t{1}), bytesOf(v));
    ASSERT_EQ(p.closeAndWait(30s), rc::OpStatus::Ok);
}

/// A live read_committed subscription of partition 0 from the earliest offset.
struct CommittedReader {
    std::mutex mu;
    std::vector<std::string> values;
    rc::Subscription sub;
    CommittedReader(rc::Client& c, rc::TopicHandle th) : sub(open(c, th)) {}
    rc::Subscription open(rc::Client& c, rc::TopicHandle th) {
        rc::SubscribeOptions o;
        o.from      = rc::From::earliest();
        o.isolation = rc::Isolation::ReadCommitted;
        return c.subscribe(th, 0, o, [this](const rc::Record& r) {
            std::lock_guard<std::mutex> lk(mu);
            values.emplace_back(reinterpret_cast<const char*>(r.value.data()), r.value.size());
        });
    }
    ~CommittedReader() { sub.unsubscribe(); }
    std::vector<std::string> snapshot() {
        std::lock_guard<std::mutex> lk(mu);
        return values;
    }
    bool has(const std::string& v) {
        const auto s = snapshot();
        return std::find(s.begin(), s.end(), v) != s.end();
    }
};

/// The shared scenario.  `ua` holds an open transaction of 10 records on a/t.  A plain SENTINEL is appended after the
/// transaction's records: a read_committed reader cannot deliver it while the transaction is open (the LSO is pinned at
/// the transaction's first record).  `op` then runs; the sentinel must arrive (the ABORT marker moved the LSO) and none
/// of the 10 records.  The reader is the cluster admin's (exempt from every suspend mode, and it outlives the namespace's
/// users).
struct Scenario {
    Fix f;
    std::unique_ptr<rc::Client> ua;
    std::unique_ptr<OpenTxn> txn;
    std::unique_ptr<CommittedReader> reader;
    explicit Scenario(const std::string& tag) : f(tag) {
        ua  = newClient(f.b, "a/ua", {f.tA});
        txn = std::make_unique<OpenTxn>(*ua, "t", "x");
        produceOne(*f.admin, Fix::handleOf(f.tA), "S");
        reader = std::make_unique<CommittedReader>(*f.admin, Fix::handleOf(f.tA));
        // The premise: the open transaction pins the LSO, the reader sees nothing yet.
        std::this_thread::sleep_for(500ms);
        EXPECT_TRUE(reader->snapshot().empty()) << "premise: an open transaction blocks read_committed";
    }
    void expectAborted() {
        ASSERT_TRUE(waitFor([&] { return reader->has("S"); }, 40s))
            << "the sentinel never became visible: the open transaction was not aborted (its ABORT marker is not in the log)";
        for (const std::string& v : reader->snapshot()) EXPECT_EQ(v, "S") << "an aborted record is visible to read_committed";
    }
};

}  // namespace

TEST(TxnAbortOnSuspendRegression, SuspendReadOnlyAbortsRegression) {
    Scenario s("txnabort_ro");
    ASSERT_TRUE(s.f.admin->suspendNamespace("a", pc::NsSuspendMode::ReadOnly).has_value());
    s.expectAborted();
    const std::string err = s.txn->commitError();
    EXPECT_FALSE(err.empty()) << "the producer's next call must fail (NamespaceSuspended)";
}

TEST(TxnAbortOnSuspendRegression, SuspendRejectAllAbortsRegression) {
    Scenario s("txnabort_ra");
    ASSERT_TRUE(s.f.admin->suspendNamespace("a", pc::NsSuspendMode::RejectAll).has_value());
    s.expectAborted();
    EXPECT_FALSE(s.txn->commitError().empty()) << "the session is closed: the producer's next call fails";
}

TEST(TxnAbortOnSuspendRegression, DeleteAbortsRegression) {
    Scenario s("txnabort_del");
    const auto del = s.f.admin->deleteNamespace("a");
    ASSERT_TRUE(del.has_value()) << del.error().errorCode << " " << del.error().detail;
    s.expectAborted();
    EXPECT_FALSE(s.txn->commitError().empty()) << "the session is closed: the producer's next call fails";
}

TEST(TxnAbortOnSuspendRegression, RevokeReasonPasswordChangeAbortsRegression) {
    Scenario s("txnabort_pw");
    // SET PASSWORD ... TERMINATE SESSIONS: the session-revoking password change.
    const auto a = s.f.b.admin().alterPassword(s.f.b.channel(), s.f.b.freshAdminSid_(), "a/ua", pc::UserChangeKind::SetPassword,
                                               kNewPw, /*terminate=*/true);
    ASSERT_EQ(a.errorCode, 0) << a.detail;
    s.expectAborted();
    EXPECT_FALSE(s.txn->commitError().empty());
}

TEST(TxnAbortOnSuspendRegression, RevokeReasonUserDroppedAbortsRegression) {
    Scenario s("txnabort_drop");
    const auto a = s.f.b.admin().dropUser(s.f.b.channel(), s.f.b.freshAdminSid_(), "a/ua");
    ASSERT_EQ(a.errorCode, 0) << a.detail;
    s.expectAborted();
    EXPECT_FALSE(s.txn->commitError().empty());
}

// A revoked WRITE grant does not close the session (a refresh), but the transaction it may no longer end is aborted.
TEST(TxnAbortOnSuspendRegression, RevokeReasonGrantRevokedAbortsRegression) {
    Scenario s("txnabort_grant");
    for (const AuthGrant& g : {prefix(SO::Write, ST::TransactionalId, ""), prefix(SO::Write, ST::Topic, "")}) {
        const auto a = s.f.b.admin().revoke(s.f.b.channel(), s.f.b.freshAdminSid_(), "a/ua", false, g.op, g.type, g.kind, g.pattern);
        ASSERT_EQ(a.errorCode, 0) << a.detail;
    }
    s.expectAborted();
    EXPECT_FALSE(s.txn->commitError().empty()) << "the producer lost its WRITE grants";
}

// b's transaction commits normally while a is suspended and then deleted; a's is aborted.
TEST(TxnAbortOnSuspendRegression, OtherNamespaceUntouchedRegression) {
    Scenario s("txnabort_other");
    auto ub = newClient(s.f.b, "b/ub", {s.f.tB});
    OpenTxn txnB(*ub, "t", "y", 10);
    ASSERT_TRUE(s.f.admin->suspendNamespace("a", pc::NsSuspendMode::ReadOnly).has_value());
    s.expectAborted();
    ASSERT_TRUE(s.f.admin->deleteNamespace("a").has_value());
    EXPECT_EQ(txnB.commitError(), "") << "b's transaction is untouched and commits";
    CommittedReader rb(*s.f.admin, Fix::handleOf(s.f.tB));
    ASSERT_TRUE(waitFor([&] { return rb.snapshot().size() >= 10; }));
    const auto got = rb.snapshot();
    EXPECT_EQ(got.size(), 10u);
    for (int i = 0; i < 10; ++i) EXPECT_EQ(got[static_cast<size_t>(i)], "y" + std::to_string(i));
}
