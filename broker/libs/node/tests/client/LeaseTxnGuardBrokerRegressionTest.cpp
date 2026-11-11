// LeaseTxnGuardBrokerRegressionTest.cpp - Herald prerequisites Task 5 (R7): a lease-guarded transactional producer cannot commit after it lost
// its lease, and initTransactions(transactionalId) fences the old producer, against REAL brokers (1 node and 3 nodes, default config).
//
// Two (or three) Invoker-mode clients "A" and "B" share the transactionalId `tx-1`; each holds a LeaseSession on the lease `role/1`; a
// read-committed subscriber "R" reads the data topic.  Every client's clock is injected (real time + a per-client offset), so a test can
// move one client's clock without moving the broker's: the at-risk instant is crossed by jumping A's clock, never by waiting.  The only
// real waits are the broker's own lease expiry (stalled holder) and failover; each is a bounded pump loop (the deadline is the failure).
//
// A negative check ("this record is invisible") is only valid once R demonstrably advanced past the records in question, so each test ends
// with a committed SENTINEL transaction: a read_committed partition stream is ordered and blocked at the last stable offset.
#include "client/ClusterClient.h"
#include "client/OneBroker.h"
#include "gen/lease_claim.gen.h"
#include "support/CatalogBrokerCluster.h"

#include <rune/client/Client.h>
#include <rune/client/Completions.h>
#include <rune/client/Key.h>
#include <rune/client/lease/LeaseSession.h>
#include <rune/client/lease/LeaseTopic.h>
#include <rune/client/producer/Producer.h>
#include <rune/client/subscriber/Subscription.h>
#include <rune/codec/Descriptor.h>
#include <rune/codec/Serialize.h>
#include <rune/protocol/LeaseSpec.h>

#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <map>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <thread>
#include <vector>

using namespace std::chrono_literals;
using namespace rune::node::test;
using namespace rune::node::test::catalog_cluster;
namespace rc = rune::client;
using rune::gen::LeaseClaim;
using Clk = std::chrono::steady_clock;

namespace {

constexpr int64_t kMs      = 1'000'000;
constexpr uint16_t kAtRisk = static_cast<uint16_t>(rc::ClientErrorCode::LeaseAtRisk);

// ---- injected clocks: the real monotonic clock plus a per-client offset (the test thread is the only thread that reads or moves them) ----
int64_t realNow() noexcept {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(Clk::now().time_since_epoch()).count();
}
int64_t g_off[4] = {0, 0, 0, 0};
template <int I>
int64_t clk() noexcept {
    return realNow() + g_off[I];
}
constexpr rc::NanoClock kClocks[4] = {&clk<0>, &clk<1>, &clk<2>, &clk<3>};

LeaseClaim claimRow(std::string_view key) {
    LeaseClaim r;
    std::memcpy(r.claim.data(), key.data(), std::min(key.size(), r.claim.size()));
    return r;
}
std::span<const uint8_t> bytes(const std::string& s) { return {reinterpret_cast<const uint8_t*>(s.data()), s.size()}; }

rc::ProducerConfig txnConfig(const std::string& txnId) {
    rc::ProducerConfig p;
    p.acks                   = rc::Acks::All;
    p.lingerUs               = 1;
    p.batchBytes             = 1;
    p.maxInFlight            = 16;
    p.produceTimeoutMs       = 30'000;
    p.deliveryTimeoutMs      = 40'000;
    p.transactionOpTimeoutMs = 25'000;
    p.transactionalId        = txnId;
    return p;
}

struct Cb {
    int entered = 0, cleared = 0, lost = 0;
};
struct OnLostF {
    Cb* cb;
    void operator()(rc::LeaseRef, rc::LostReason) const { ++cb->lost; }
};
struct OnRiskF {
    Cb* cb;
    void operator()(rc::LeaseRef, rc::AtRiskEdge e) const { (e == rc::AtRiskEdge::Entered ? cb->entered : cb->cleared)++; }
};
using Session = rc::LeaseSession<4, OnLostF, OnRiskF>;
using Lt      = rc::LeaseTopic<LeaseClaim>;

// ---- the cluster: 1 node (OneBroker) or 3 nodes (ThreeNodes), a lease topic `claims` and a data topic `data` (1 partition each) ----
struct Rig {
    int                           nodes;
    std::unique_ptr<OneBroker>    one;
    std::unique_ptr<ThreeNodes>   three;
    int32_t                       ctl = -1;
    rc::TopicSpec                 data;
    bool                          fastFailureDetection = false;

    Rig(int n, const char* tag, uint32_t leaseTtlMs, bool fastDetect = false) : nodes(n), fastFailureDetection(fastDetect) {
        g_off[0] = g_off[1] = g_off[2] = g_off[3] = 0;
        std::unique_ptr<rc::Client> adminOwned;
        rc::Client*                 admin = nullptr;
        if (n == 1) {
            one   = std::make_unique<OneBroker>(tag, OneBroker::Runtime::Shard);
            admin = one->connectClient();
        } else {
            three = std::make_unique<ThreeNodes>(ThreeNodes::start(tag));
            if (!three->awaitCatalogLeader(30s)) throw std::runtime_error("no catalog leader");
            ctl = awaitController(*three);
            if (ctl <= 0) throw std::runtime_error("no controller");
            adminOwned = connectClient(*three, ctl);
            admin      = adminOwned.get();
        }
        // The lease topic: compacted, ttl `leaseTtlMs`.
        rc::TopicOptions lo;
        lo.replicationFactor = n;
        lo.lease = rune::protocol::LeaseSpec{.ttlMs = leaseTtlMs, .minTtlMs = 1000, .maxTtlMs = 600'000, .maxKeys = 1000, .expiryBatch = 64,
                                             .checkpoint = 0, .checkpointIntervalMs = 1000, .checkpointRate = 1};
        lo.config.cleanupPolicy = 1;
        const auto blob = rune::codec::serialize(rune::codec::DescriptorBuilder("lease_claim")
                                                     .addFixedStr("claim", 32, 1, true)
                                                     .add("a", rune::codec::PrimType::UInt64)
                                                     .addFixedStr("b", 8)
                                                     .build());
        const auto lr = admin->defineTopicAndWait("claims", 1, "claim", blob, rc::Persistence::Disk, lo, {}, 60s);
        if (lr.errorCode != 0) throw std::runtime_error("define claims: " + lr.detail);
        rc::TopicOptions dopt;
        dopt.replicationFactor = n;
        const auto dr = admin->defineTopicAndWait("data", 1, {}, {}, rc::Persistence::Disk, dopt, {}, 60s);
        if (dr.errorCode != 0) throw std::runtime_error("define data: " + dr.detail);
        data = rc::TopicSpec{"data", dr.topicId, 1};
        if (n == 3) {
            std::vector<int32_t> leaders;
            if (!awaitLeaders(*three, lr.topicId, 1, leaders, 60s)) throw std::runtime_error("no leader for claims");
            if (!awaitLeaders(*three, dr.topicId, 1, leaders, 60s)) throw std::runtime_error("no leader for data");
            adminOwned->close();
        }
    }

    /// A node a client can be seeded on, other than `avoid` (3 nodes: the k-th such running node; 1 node: the only one).
    [[nodiscard]] int seedNode(int k, int avoid = -1) const {
        std::vector<int> ids;
        for (int id : three->runningNodes())
            if (id != avoid) ids.push_back(id);
        return ids[static_cast<std::size_t>(k) % ids.size()];
    }
    [[nodiscard]] rc::ClientConfig config(int k, int clockIdx, int avoid = -1) {
        rc::ClientConfig c = n1() ? one->clientConfig() : clientConfigFor(*three, seedNode(k, avoid));
        c.topics                       = {data};
        c.runnerMode                   = rc::RunnerMode::Invoker;
        c.nanoClock                    = kClocks[clockIdx];
        c.connectTimeoutMs             = 30'000;
        c.pingTimeoutMs                = 30'000;
        c.aeronClientLivenessTimeoutNs = 120'000'000'000LL;
        c.keepaliveIntervalNs          = 100'000'000LL;
        if (fastFailureDetection) c.responseTimeoutNs = 500'000'000LL;   // a stopped broker is noticed after 0.5 s of silence
        return c;
    }
    [[nodiscard]] bool n1() const { return nodes == 1; }
    int32_t dataLeader() { return leaderOn(three->node(three->runningNodes().front()), data.topicId, 0); }
};

// ---- one client: lease topic handle, session, completions, transactional producer ----
class Actor {
public:
    Actor(Rig& rig, int k, int clockIdx, int avoid = -1) : rig_(rig), idx_(clockIdx) {
        client = std::make_unique<rc::Client>(rig.config(k, clockIdx, avoid));
        client->connectAndWait(30s);
        cq = std::make_unique<rc::Completions>(*client);
    }
    virtual ~Actor() {
        session.reset();
        if (producer) producer->closeImmediately();
        producer.reset();
        lt.reset();
        cq.reset();
        client->close();
    }

    void step() {
        (void)client->doWork();
        if (session) session->doWork(client->nowNs());
        poll();
    }
    /// A pass that leaves the session alone (its keepalive answers stay unread).
    void stepNoSession() {
        (void)client->doWork();
        poll();
    }
    void poll() { (void)cq->poll([&](const rc::Completion& c, std::span<const uint8_t>) { got[c.token] = c; }); }
    void moveClock(int64_t ns) { g_off[idx_] += ns; }

    /// Grants `role/1` (one attempt; the result is the broker's answer).
    rc::LeaseResult grantOnce(uint64_t holder, uint64_t ttlMs) {
        if (!lt) lt.emplace(client->openLeaseTopicAndWait<LeaseClaim>("claims", {}, 30s));
        rc::LeaseOptions o;
        o.holder = holder;
        o.ttlMs  = ttlMs;
        sentNs_  = client->nowNs();
        return lt->grantAndWait(claimRow("role/1"), o);
    }
    /// The session keeps the granted lease alive.
    void keep(const rc::LeaseResult& g, uint64_t ttlMs) {
        session = std::make_unique<Session>(*client, OnLostF{&cb}, OnRiskF{&cb}, rc::LeaseSessionOptions{});
        rc::OpResult err = rc::OpResult::Accepted;
        ref = session->add(*lt, claimRow("role/1"), g.version, static_cast<int64_t>(ttlMs), sentNs_, &err);
        ASSERT_EQ(err, rc::OpResult::Accepted);
        ttlMs_ = ttlMs;
    }

    rc::Producer& makeProducer(const std::string& txnId) {
        producer.emplace(client->producer(txnConfig(txnId)));
        return *producer;
    }
    rc::Token tok() { return ++tok_; }
    bool await(rc::Token t, std::initializer_list<Actor*> pumped, std::chrono::seconds within = 60s);
    rc::Completion result(rc::Token t) const { return got.at(t); }

    rc::Token init() {
        const rc::Token t = tok();
        EXPECT_EQ(producer->initTransactions(*cq, t), rc::OpResult::Accepted);
        return t;
    }
    rc::Token commit() {
        const rc::Token t = tok();
        EXPECT_EQ(producer->commitTransaction(*cq, t), rc::OpResult::Accepted);
        return t;
    }
    rc::Token abort() {
        const rc::Token t = tok();
        EXPECT_EQ(producer->abortTransaction(*cq, t), rc::OpResult::Accepted);
        return t;
    }
    /// One send; BackPressured is retried a bounded number of passes.  The token completes with the ack (or a failure).
    rc::SendResult send(const std::string& v, rc::Token& t, bool pumpSession = true) {
        t = tok();
        rc::SendResult r = rc::SendResult::BackPressured;
        for (int i = 0; i < 200'000 && r == rc::SendResult::BackPressured; ++i) {
            r = producer->trySend(client->topic("data"), rc::Key(uint64_t{1}), bytes(v), *cq, t);
            if (r == rc::SendResult::BackPressured) pumpSession ? step() : stepNoSession();
        }
        return r;
    }
    /// Bind the producer to the lease (BackPressured until the session's guard block exists: pump and retry, bounded).
    void bind() {
        rc::OpResult r = rc::OpResult::BackPressured;
        for (int i = 0; i < 100'000 && r == rc::OpResult::BackPressured; ++i) {
            r = session->bindProducer(ref, *producer);
            if (r == rc::OpResult::BackPressured) step();
        }
        ASSERT_EQ(r, rc::OpResult::Accepted);
    }

    std::unique_ptr<rc::Client>      client;
    std::unique_ptr<rc::Completions> cq;
    std::optional<Lt>                lt;
    std::unique_ptr<Session>         session;
    std::optional<rc::Producer>      producer;
    rc::LeaseRef                     ref{};
    Cb                               cb;
    std::map<rc::Token, rc::Completion> got;

private:
    Rig&     rig_;
    int      idx_;
    int64_t  sentNs_ = 0;
    uint64_t ttlMs_  = 0;
    rc::Token tok_   = 0;
};

/// Pumps `as` on this thread until `pred` holds.  Bounded twice: a wall-clock deadline and a pass cap (the failure, never a hang).
template <class P>
bool pumpUntil(std::initializer_list<Actor*> as, P&& pred, std::chrono::seconds within = 60s) {
    constexpr uint64_t kPassCap = 200'000'000;
    const auto         end      = Clk::now() + within;
    for (uint64_t i = 0; i < kPassCap && Clk::now() < end; ++i) {
        for (Actor* a : as) a->step();
        if (pred()) return true;
        std::this_thread::yield();
    }
    return pred();
}
bool Actor::await(rc::Token t, std::initializer_list<Actor*> pumped, std::chrono::seconds within) {
    return pumpUntil(pumped, [&] { return got.count(t) != 0; }, within);
}

// ---- R: a read-committed reader of the data topic ----
class Reader : public Actor {
public:
    Reader(Rig& rig, int k, int clockIdx, int avoid = -1) : Actor(rig, k, clockIdx, avoid) {
        rc::SubscribeOptions o;
        o.from      = rc::From::earliest();
        o.isolation = rc::Isolation::ReadCommitted;
        sub_.emplace(client->subscribe(client->topic("data"), 0, o, [this](const rc::Record& r) {
            seen.emplace_back(reinterpret_cast<const char*>(r.value.data()), r.value.size());
        }));
    }
    ~Reader() override { if (sub_) sub_->unsubscribe(); }
    [[nodiscard]] bool has(const std::string& v) const { return std::find(seen.begin(), seen.end(), v) != seen.end(); }
    std::vector<std::string> seen;

private:
    std::optional<rc::Subscription> sub_;
};

std::vector<std::string> sorted(std::vector<std::string> v) {
    std::sort(v.begin(), v.end());
    return v;
}
std::vector<std::string> values(const std::string& prefix, int n) {
    std::vector<std::string> v;
    for (int i = 0; i < n; ++i) v.push_back(prefix + std::to_string(i));
    return v;
}

/// begin + sends (each awaited: acked) + nothing else.  `pumped` are the actors driven while waiting.
void sendAcked(Actor& a, const std::vector<std::string>& vs, std::initializer_list<Actor*> pumped) {
    for (const std::string& v : vs) {
        rc::Token t = 0;
        ASSERT_EQ(a.send(v, t), rc::SendResult::Ok) << v;
        ASSERT_TRUE(a.await(t, pumped)) << "no completion for " << v;
        ASSERT_EQ(a.result(t).status, rc::OpStatus::Ok) << v << " code " << a.result(t).errorCode;
    }
}
/// A committed sentinel transaction, then R has seen it.  Returns after R delivered "sentinel".
void sentinel(Actor& w, Reader& r, std::initializer_list<Actor*> pumped) {
    w.producer->beginTransaction();
    ASSERT_NO_FATAL_FAILURE(sendAcked(w, {"sentinel"}, pumped));
    const rc::Token c = w.commit();
    ASSERT_TRUE(w.await(c, pumped));
    ASSERT_EQ(w.result(c).status, rc::OpStatus::Ok) << "sentinel commit, code " << w.result(c).errorCode;
    ASSERT_TRUE(pumpUntil({&r}, [&] { return r.has("sentinel"); })) << "R never delivered the sentinel";
}
/// Holder A acquires the lease (first attempt) and keeps it; A's producer is initialised and bound.
void takeLease(Actor& a, uint64_t holder, uint64_t ttlMs, const std::string& txnId, bool bindProducer = true) {
    const rc::LeaseResult g = a.grantOnce(holder, ttlMs);
    ASSERT_TRUE(g.ok()) << int(g.status) << " " << g.errorCode;
    ASSERT_NO_FATAL_FAILURE(a.keep(g, ttlMs));
    a.makeProducer(txnId);
    const rc::Token i = a.init();
    ASSERT_TRUE(a.await(i, {&a}));
    ASSERT_EQ(a.result(i).status, rc::OpStatus::Ok);
    if (bindProducer) ASSERT_NO_FATAL_FAILURE(a.bind());
}

constexpr uint64_t kTtlMs = 2000;

class LeaseTxnGuardBrokerRegression : public ::testing::TestWithParam<int> {};

}  // namespace

// A holds the lease, binds, begins, sends 10, commits; R reads the 10.
TEST_P(LeaseTxnGuardBrokerRegression, GuardedCommitHappyPath) {
    Rig rig(GetParam(), "ltg_happy", kTtlMs);
    Actor  a(rig, 0, 0);
    Reader r(rig, 1, 1);
    ASSERT_NO_FATAL_FAILURE(takeLease(a, 1, kTtlMs, "tx-1"));
    a.producer->beginTransaction();
    const auto vs = values("A", 10);
    ASSERT_NO_FATAL_FAILURE(sendAcked(a, vs, {&a}));
    const rc::Token c = a.commit();
    ASSERT_TRUE(a.await(c, {&a}));
    ASSERT_EQ(a.result(c).status, rc::OpStatus::Ok) << "code " << a.result(c).errorCode;
    EXPECT_EQ(a.cb.lost, 0);
    EXPECT_EQ(a.cb.entered, 0);
    ASSERT_NO_FATAL_FAILURE(sentinel(a, r, {&a}));
    auto expect = vs;
    expect.push_back("sentinel");
    EXPECT_EQ(sorted(r.seen), sorted(expect));
}

// A begins and sends 5 (acked), then stalls for the whole ttl (not driven).  B is granted the lease after the broker expired A's, runs
// initTransactions(tx-1), begins, sends 5 and commits.  A is driven again and commits: refused (LeaseAtRisk, or Fenced when the guard event
// was not processed yet - never OK).  R reads exactly B's 5 (A's 5 never become visible); A's next send does not succeed.
TEST_P(LeaseTxnGuardBrokerRegression, StalledHolderCannotCommit) {
    Rig rig(GetParam(), "ltg_stall", kTtlMs);
    Actor  a(rig, 0, 0);
    Actor  b(rig, 1, 1);
    Reader r(rig, 2, 2);
    ASSERT_NO_FATAL_FAILURE(takeLease(a, 1, kTtlMs, "tx-1"));
    a.producer->beginTransaction();
    ASSERT_NO_FATAL_FAILURE(sendAcked(a, values("A", 5), {&a}));

    // A stalls: only B (and nobody else) is driven.  B's grant is refused (KeyExists) until the broker expires A's lease.
    rc::LeaseResult g;
    const auto      deadline = Clk::now() + 60s;
    for (int attempt = 0; attempt < 100'000 && Clk::now() < deadline; ++attempt) {
        g = b.grantOnce(2, kTtlMs);
        if (g.ok()) break;
        for (const auto next = Clk::now() + 25ms; Clk::now() < next;) b.stepNoSession();
    }
    ASSERT_TRUE(g.ok()) << "B was never granted the lease: " << int(g.status) << " " << g.errorCode;
    ASSERT_NO_FATAL_FAILURE(b.keep(g, kTtlMs));
    b.makeProducer("tx-1");
    const rc::Token bi = b.init();
    ASSERT_TRUE(b.await(bi, {&b}));
    ASSERT_EQ(b.result(bi).status, rc::OpStatus::Ok);
    ASSERT_NO_FATAL_FAILURE(b.bind());
    b.producer->beginTransaction();
    ASSERT_NO_FATAL_FAILURE(sendAcked(b, values("B", 5), {&b}));
    const rc::Token bc = b.commit();
    ASSERT_TRUE(b.await(bc, {&b}));
    ASSERT_EQ(b.result(bc).status, rc::OpStatus::Ok) << "code " << b.result(bc).errorCode;

    // A runs again.  Its guard tripped before A's first drain (A's at-risk instant is earlier than the broker expiry B waited for), so its
    // next send is accepted at the call (the transaction is still open client-side) and fails LeaseAtRisk, and its commit is refused with
    // LeaseAtRisk too - never Ok, never a fence (the guard answers before the coordinator is asked).
    rc::Token st = 0;
    ASSERT_EQ(a.send("A-after", st), rc::SendResult::Ok);
    ASSERT_TRUE(a.await(st, {&a, &b}));
    EXPECT_EQ(a.result(st).status, rc::OpStatus::DeliveryFailed) << "A's send: code " << a.result(st).errorCode;
    EXPECT_EQ(a.result(st).errorCode, kAtRisk);
    const rc::Token ac = a.commit();
    ASSERT_TRUE(a.await(ac, {&a, &b}));
    EXPECT_EQ(a.result(ac).status, rc::OpStatus::DeliveryFailed) << "A's commit: code " << a.result(ac).errorCode;
    EXPECT_EQ(a.result(ac).errorCode, kAtRisk);
    rc::Token st2 = 0;
    EXPECT_EQ(a.send("A-later", st2), rc::SendResult::NotInTransaction) << "after the refused commit A is outside a transaction";

    ASSERT_NO_FATAL_FAILURE(sentinel(b, r, {&b}));
    auto expect = values("B", 5);
    expect.push_back("sentinel");
    EXPECT_EQ(sorted(r.seen), sorted(expect)) << "R reads exactly B's records";
}

// No leases.  A begins, sends 3 (acked), does not commit.  B initTransactions(tx-1): R never sees A's 3; A's commit and A's send complete
// Fenced; B commits 2 and R reads exactly those 2.
TEST_P(LeaseTxnGuardBrokerRegression, InitTransactionsAbortsOldOpenTransaction) {
    Rig rig(GetParam(), "ltg_init", kTtlMs);
    Actor  a(rig, 0, 0);
    Actor  b(rig, 1, 1);
    Reader r(rig, 2, 2);
    a.makeProducer("tx-1");
    const rc::Token ai = a.init();
    ASSERT_TRUE(a.await(ai, {&a}));
    ASSERT_EQ(a.result(ai).status, rc::OpStatus::Ok);
    a.producer->beginTransaction();
    ASSERT_NO_FATAL_FAILURE(sendAcked(a, values("A", 3), {&a}));

    b.makeProducer("tx-1");
    const rc::Token bi = b.init();
    ASSERT_TRUE(b.await(bi, {&a, &b}));
    ASSERT_EQ(b.result(bi).status, rc::OpStatus::Ok);

    // A's commit reaches the coordinator with the old epoch: Fenced.  (A sends nothing first: a record answered 17 / 18 by the partition
    // leader would make the producer FatalError before its commit left, and the fence would then be produced locally.)
    const rc::Token ac = a.commit();
    ASSERT_TRUE(a.await(ac, {&a, &b}));
    EXPECT_EQ(a.result(ac).status, rc::OpStatus::Fenced) << "A's commit: code " << a.result(ac).errorCode;
    EXPECT_EQ(a.producer->state(), rc::TxnState::FatalError);
    rc::Token st = 0;
    EXPECT_EQ(a.send("A-later", st), rc::SendResult::NotInTransaction) << "after the fenced commit A is outside a transaction";

    b.producer->beginTransaction();
    ASSERT_NO_FATAL_FAILURE(sendAcked(b, values("B", 2), {&a, &b}));
    const rc::Token bc = b.commit();
    ASSERT_TRUE(b.await(bc, {&a, &b}));
    ASSERT_EQ(b.result(bc).status, rc::OpStatus::Ok) << "code " << b.result(bc).errorCode;
    ASSERT_NO_FATAL_FAILURE(sentinel(b, r, {&a, &b}));
    auto expect = values("B", 2);
    expect.push_back("sentinel");
    EXPECT_EQ(sorted(r.seen), sorted(expect)) << "A's 3 records are never visible";
}

// The send-first order: A's record, sent after B's initTransactions(tx-1), is answered InvalidProducerEpoch / ProducerFenced by the partition
// leader (a broker error, as EosBrokerRegression documents); the producer is FatalError(Fenced) then and its commit completes Fenced.
TEST_P(LeaseTxnGuardBrokerRegression, InitTransactionsFencesOldProducersSend) {
    Rig rig(GetParam(), "ltg_fsend", kTtlMs);
    Actor  a(rig, 0, 0);
    Actor  b(rig, 1, 1);
    Reader r(rig, 2, 2);
    a.makeProducer("tx-1");
    const rc::Token ai = a.init();
    ASSERT_TRUE(a.await(ai, {&a}));
    ASSERT_EQ(a.result(ai).status, rc::OpStatus::Ok);
    a.producer->beginTransaction();
    ASSERT_NO_FATAL_FAILURE(sendAcked(a, values("A", 3), {&a}));
    b.makeProducer("tx-1");
    const rc::Token bi = b.init();
    ASSERT_TRUE(b.await(bi, {&a, &b}));
    ASSERT_EQ(b.result(bi).status, rc::OpStatus::Ok);

    rc::Token st = 0;
    ASSERT_EQ(a.send("A-after", st), rc::SendResult::Ok);
    ASSERT_TRUE(a.await(st, {&a, &b}));
    EXPECT_EQ(a.result(st).status, rc::OpStatus::BrokerError) << "A's send: code " << a.result(st).errorCode;
    EXPECT_TRUE(a.result(st).errorCode == 17 || a.result(st).errorCode == 18) << "code " << a.result(st).errorCode;
    EXPECT_EQ(a.producer->state(), rc::TxnState::FatalError);
    const rc::Token ac = a.commit();
    ASSERT_TRUE(a.await(ac, {&a, &b}));
    EXPECT_EQ(a.result(ac).status, rc::OpStatus::Fenced) << "A's commit: code " << a.result(ac).errorCode;

    b.producer->beginTransaction();
    ASSERT_NO_FATAL_FAILURE(sendAcked(b, values("B", 2), {&a, &b}));
    const rc::Token bc = b.commit();
    ASSERT_TRUE(b.await(bc, {&a, &b}));
    ASSERT_EQ(b.result(bc).status, rc::OpStatus::Ok) << "code " << b.result(bc).errorCode;
    ASSERT_NO_FATAL_FAILURE(sentinel(b, r, {&a, &b}));
    auto expect = values("B", 2);
    expect.push_back("sentinel");
    EXPECT_EQ(sorted(r.seen), sorted(expect)) << "A's records are never visible";
}

// The guard ALONE refuses the commit: no second producer exists, so the broker cannot fence A.  A's clock jumps past the at-risk instant
// and the commit is issued before the client runs a pass: it completes DeliveryFailed / LeaseAtRisk with no EndTxn(commit) sent, so R
// reads none of A's records; after the guard clears, A aborts and a fresh transaction commits.
TEST_P(LeaseTxnGuardBrokerRegression, TrippedHolderCommitRefusedByGuardAlone) {
    Rig rig(GetParam(), "ltg_alone", kTtlMs);
    Actor  a(rig, 0, 0);
    Reader r(rig, 1, 1);
    ASSERT_NO_FATAL_FAILURE(takeLease(a, 1, kTtlMs, "tx-1"));
    a.producer->beginTransaction();
    ASSERT_NO_FATAL_FAILURE(sendAcked(a, values("A", 5), {&a}));
    const uint64_t okBefore = a.session->core().okAnswers();
    ASSERT_TRUE(pumpUntil({&a}, [&] { return a.session->core().okAnswers() > okBefore; })) << "no keepalive answered";
    a.moveClock(1100 * kMs);
    const rc::Token c = a.commit();   // before any step: the trip has not run yet
    for (uint64_t i = 0; i < 2'000'000 && a.got.count(c) == 0; ++i) a.stepNoSession();
    ASSERT_EQ(a.got.count(c), 1u) << "the commit completed";
    EXPECT_EQ(a.result(c).status, rc::OpStatus::DeliveryFailed) << "code " << a.result(c).errorCode;
    EXPECT_EQ(a.result(c).errorCode, kAtRisk);
    EXPECT_EQ(a.producer->state(), rc::TxnState::AbortableError);
    ASSERT_TRUE(pumpUntil({&a}, [&] { return a.cb.cleared >= 1; })) << "the guard was never cleared";
    const rc::Token ab = a.abort();
    ASSERT_TRUE(a.await(ab, {&a}));
    ASSERT_EQ(a.result(ab).status, rc::OpStatus::Ok) << "abort, code " << a.result(ab).errorCode;
    ASSERT_NO_FATAL_FAILURE(sentinel(a, r, {&a}));   // begins its own transaction
    EXPECT_EQ(sorted(r.seen), (std::vector<std::string>{"sentinel"})) << "none of A's 5 records became visible";
}

// A begins, sends 2 (acked).  The at-risk instant passes (A's clock jumps; the broker's lease is untouched), A's session reads no answer:
// A's writes fail LeaseAtRisk.  The session runs on (Cleared); A aborts, begins, sends 4 and commits; R reads exactly those 4.
TEST_P(LeaseTxnGuardBrokerRegression, AtRiskThenClearedContinues) {
    Rig rig(GetParam(), "ltg_risk", kTtlMs);
    Actor  a(rig, 0, 0);
    Reader r(rig, 1, 1);
    ASSERT_NO_FATAL_FAILURE(takeLease(a, 1, kTtlMs, "tx-1"));
    a.producer->beginTransaction();
    ASSERT_NO_FATAL_FAILURE(sendAcked(a, values("pre", 2), {&a}));

    // Re-arm the guard from a fresh keepalive, then jump A's clock past the at-risk instant (ttl/2) but not the margin (about ttl).
    const uint64_t okBefore = a.session->core().okAnswers();
    ASSERT_TRUE(pumpUntil({&a}, [&] { return a.session->core().okAnswers() > okBefore; })) << "no keepalive answered";
    a.moveClock(1100 * kMs);
    // The send is issued before the client runs a pass: it is accepted at the call, then the trip (the first pass) fails it.  The session
    // reads that event, and the keepalive answers, only when it runs - so run the client alone until the risky write completed.
    rc::Token risky = 0;
    ASSERT_EQ(a.send("risky", risky, /*pumpSession=*/false), rc::SendResult::Ok);
    for (uint64_t i = 0; i < 2'000'000 && a.got.count(risky) == 0; ++i) a.stepNoSession();
    ASSERT_EQ(a.got.count(risky), 1u) << "the risky write completes";
    EXPECT_EQ(a.result(risky).status, rc::OpStatus::DeliveryFailed);
    EXPECT_EQ(a.result(risky).errorCode, kAtRisk);
    EXPECT_EQ(a.producer->state(), rc::TxnState::AbortableError) << "the trip failed the open transaction";
    rc::Token later = 0;
    EXPECT_EQ(a.send("later", later, /*pumpSession=*/false), rc::SendResult::NotInTransaction) << "the failed transaction refuses the write at the call";
    EXPECT_EQ(a.cb.entered, 0) << "the session has not run since the jump";

    // Keepalives resume: the guard is cleared.
    ASSERT_TRUE(pumpUntil({&a}, [&] { return a.cb.cleared >= 1; })) << "the guard was never cleared";
    EXPECT_EQ(a.cb.entered, 1) << "the session saw the trip once";
    EXPECT_EQ(a.cb.lost, 0);
    const rc::Token ab = a.abort();
    ASSERT_TRUE(a.await(ab, {&a}));
    ASSERT_EQ(a.result(ab).status, rc::OpStatus::Ok) << "abort, code " << a.result(ab).errorCode;
    a.producer->beginTransaction();
    const auto vs = values("post", 4);
    ASSERT_NO_FATAL_FAILURE(sendAcked(a, vs, {&a}));
    const rc::Token c = a.commit();
    ASSERT_TRUE(a.await(c, {&a}));
    ASSERT_EQ(a.result(c).status, rc::OpStatus::Ok) << "code " << a.result(c).errorCode;
    ASSERT_NO_FATAL_FAILURE(sentinel(a, r, {&a}));
    auto expect = vs;
    expect.push_back("sentinel");
    EXPECT_EQ(sorted(r.seen), sorted(expect));
}

// 3 nodes: A commits while the data partition's leader is stopped; the lease stays held (ttl 30 s here, so a slow failover is not a
// lease loss); the commit completes OK after the failover and R reads each record once.
TEST_P(LeaseTxnGuardBrokerRegression, LeaderChangeMidCommitGuarded) {
    if (GetParam() != 3) GTEST_SKIP() << "needs three nodes";
    Rig rig(3, "ltg_leader", 30'000, /*fastDetect=*/true);
    const int32_t victim = rig.dataLeader();
    ASSERT_GT(victim, 0);
    Actor  a(rig, 0, 0, victim);
    Reader r(rig, 1, 1, victim);
    ASSERT_NO_FATAL_FAILURE(takeLease(a, 1, 30'000, "tx-1"));
    a.producer->beginTransaction();
    std::vector<std::string> vs = values("L", 20);
    std::vector<rc::Token>   toks;
    for (const std::string& v : vs) {
        rc::Token t = 0;
        ASSERT_EQ(a.send(v, t), rc::SendResult::Ok);
        toks.push_back(t);
    }
    (void)a.client->doWork();   // frames to the victim are on the wire
    const rc::Token c = a.commit();
    rig.three->stop(victim);
    ASSERT_TRUE(a.await(c, {&a}, 150s)) << "the commit never completed after the failover";
    EXPECT_EQ(a.result(c).status, rc::OpStatus::Ok) << "code " << a.result(c).errorCode;
    for (rc::Token t : toks) {
        ASSERT_EQ(a.got.count(t), 1u);
        EXPECT_EQ(a.result(t).status, rc::OpStatus::Ok);
    }
    EXPECT_EQ(a.cb.lost, 0) << "the lease stays held";
    EXPECT_EQ(a.cb.entered, 0) << "never at risk";
    EXPECT_EQ(a.session->stateOf(a.ref), rc::LeaseEntryState::Held);

    std::vector<int32_t> leaders;
    ASSERT_TRUE(awaitLeaders(*rig.three, rig.data.topicId, 1, leaders, 60s));
    ASSERT_NO_FATAL_FAILURE(sentinel(a, r, {&a}));
    auto expect = vs;
    expect.push_back("sentinel");
    EXPECT_EQ(sorted(r.seen), sorted(expect)) << "every record exactly once";
}

INSTANTIATE_TEST_SUITE_P(Nodes, LeaseTxnGuardBrokerRegression, ::testing::Values(1, 3),
                         [](const ::testing::TestParamInfo<int>& i) { return "N" + std::to_string(i.param); });
