// AuthClusterIntegrationTest.cpp - authentication Task 19 (spec §11 "Brokers", §12): THREE real authenticating
// BrokerNodes over real Aeron UDP (sealed peer links): __security replicates to every broker and the digests agree,
// logins work through every broker, a REVOKE ends live subscriptions on every broker within 100 ms, a leader kill
// mid security DDL applies the retried request once, peer keys right / wrong / none, key rotation across rolling
// restarts.  Every wait is bounded.
#include "rune/node/MetadataService.h"
#include "support/AuthMatrixExpectations.h"
#include "support/AuthRig.h"
#include "support/AuthTrio.h"

#include <algorithm>
#include <atomic>
#include <map>
#include <cstdio>
#include <cstdlib>
#include <future>
#include <mutex>
#include <thread>
#include <set>
#include <spdlog/sinks/base_sink.h>

using namespace rune::node;
using namespace rune::node::test;
using namespace rune::node::test::grants;
using namespace std::chrono_literals;
namespace pc = rune::protocol::client;
using SO = rune::protocol::SecurityOperation;
using ST = rune::protocol::SecurityResourceType;
using SP = rune::protocol::SecurityPatternKind;

namespace {

/// Issue #403: latency bounds inside correctness tests are enforced only on an idle host (CLAUDE.md §3), opted in with
/// RUNE_LATENCY_GATES=1 (docs/testing.md, latency gates).  Always measured and printed.
bool latencyGatesEnabled() {
    const char* v = std::getenv("RUNE_LATENCY_GATES");
    return v != nullptr && std::string(v) == "1";
}

/// DefineTopic as `s` (a session on any broker); the catalog leader answers, the others redirect: try every broker
/// the builder has a session on, bounded.  Returns the topic id or -1.  Issue #403: any answer ends an attempt - a
/// follower's Redirect is an ErrorResponse, never a DefineTopicResponse (waiting for the latter burnt 15 s per attempt).
int32_t defineTopicAnywhere(AuthTrio& t, const std::vector<AuthTrio::Session>& sessions, const std::string& name, uint32_t partitions = 1) {
    const auto deadline = Clock3::now() + 60s;
    while (Clock3::now() < deadline) {
        for (const auto& s : sessions) {
            auto& c = s.w->raw();
            const int64_t corr = c.sendDefineTopic(t.channel(s.node), AuthWireClient::kClientStream, s.sid, name, partitions, authRigCharBlob());
            const auto any = c.awaitAnyDdlAnswer(corr, Clock3::now() + 15s);
            if (!any || any->code != 0) continue;
            if (const auto r = c.awaitDefine(corr, Clock3::now() + 15s); r && r->errorCode == 0) return r->topicId;
        }
        std::this_thread::sleep_for(50ms);
    }
    return -1;
}

/// Issue #403: a session of `user` on EVERY broker (bounded retries while the user reaches each broker): a DDL that a
/// broker routes to the catalog leader is retried through the leader's own session, wherever leadership moved.
std::vector<AuthTrio::Session> sessionsOnEveryBroker(AuthTrio& t, const std::string& user) {
    std::vector<AuthTrio::Session> out;
    for (int id : {1, 2, 3}) {
        AuthTrio::Session s;
        if (!AuthTrio::until(30s, [&] { try { s = t.login(id, user); return true; } catch (...) { return false; } })) return {};
        out.push_back(s);
    }
    return out;
}

/// One topic with 3 partitions; part[i] = a partition broker i+1 leads (-1 = none).  A subscription / produce binds only
/// on the partition's leader (a follower answers Redirect / NotLeader).
int ackOrErrorCode(ShardTestClient& c, int64_t corr, std::chrono::milliseconds within);   // defined with the matrix
struct LedTopic { int32_t id = -1; std::array<int32_t, 3> part{-1, -1, -1}; };
LedTopic defineLedTopic(AuthTrio& t, const std::vector<AuthTrio::Session>& bs, const std::string& name) {
    LedTopic out;
    out.id = defineTopicAnywhere(t, bs, name, 3);
    if (out.id < 0) return out;
    const auto deadline = Clock3::now() + 60s;
    while (Clock3::now() < deadline) {
        out.part = {-1, -1, -1};
        for (const PartitionInfo& p : bs[0].w->raw().metadata(t.channel(bs[0].node), AuthWireClient::kClientStream, bs[0].sid))
            if (p.topicId == out.id && p.leaderId >= 1 && p.leaderId <= 3) out.part[static_cast<std::size_t>(p.leaderId - 1)] = p.partition;
        if (out.part[0] >= 0 && out.part[1] >= 0 && out.part[2] >= 0) return out;
        std::this_thread::sleep_for(25ms);
    }
    return out;
}

}  // namespace

// §11: bootstrap via node 1; a role / user / grants via node 2 (the __security leader answers); the user logs in on
// node 3; every broker's SecurityView carries the same digest at the same applied offset.
TEST(AuthClusterRegression, SecurityReplicatesToEveryBrokerAndDigestsAgree) {
    AuthTrio t("repl");
    t.start();
    t.bootstrapAdmin();
    t.mustOk(t.ddl(2, [](AuthWireClient& w, const std::string& ch, int64_t sid) { return w.createRole(ch, sid, "writers"); }), "CREATE ROLE");
    t.mustOk(t.ddl(2, [](AuthWireClient& w, const std::string& ch, int64_t sid) {
                 return w.grant(ch, sid, "writers", true, SO::Write, ST::Topic, SP::Prefix, "world."); }), "GRANT to role");
    t.createUser(2, "alice");
    t.mustOk(t.ddl(2, [](AuthWireClient& w, const std::string& ch, int64_t sid) { return w.grantRole(ch, sid, "alice", "writers"); }), "GRANT ROLE");
    t.grant(2, "alice", SO::Read, ST::Topic, SP::Prefix, "world.");

    // The user logs in on node 3 (retry: the CREATE USER reaches node 3 asynchronously).
    AuthTrio::Session s3;
    ASSERT_TRUE(AuthTrio::until(20s, [&] { try { s3 = t.login(3, "alice"); return true; } catch (...) { return false; } }));
    EXPECT_GT(s3.sid, 0);
    // ... and on every other broker.
    for (int id : {1, 2}) EXPECT_NO_THROW(t.login(id, "alice")) << "login via node " << id;

    uint64_t digest = 0;
    ASSERT_TRUE(t.awaitDigestsAgree(5, 30s, &digest)) << "every broker's SecurityView at one offset, one digest";
    EXPECT_NE(digest, 0u);
    int64_t off = -1;
    for (BrokerNode* n : t.cluster().live()) {
        const auto v = n->securityViewForTest();
        EXPECT_FALSE(v.halted);
        EXPECT_EQ(v.digest, digest) << "node " << n->nodeIdForTest();
        if (off < 0) off = v.appliedOffset;
        EXPECT_EQ(v.appliedOffset, off) << "node " << n->nodeIdForTest();
    }
    // The applied-offset counter (1'000'227) of every broker followed.
    for (int id : {1, 2, 3}) EXPECT_EQ(t.counter(id, kSecurityAppliedOffsetCounterTypeId).value_or(-9), off) << "node " << id;
}

// §12 gate: the user holds one live subscription on each of the 3 brokers; GRANT -> subscribe -> REVOKE x 20; the time
// from the REVOKE's DdlResponse{OK} at the admin client to each subscription's ErrorResponse{45} is <= 100 ms.
TEST(AuthClusterRegression, RevokeEndsSubscriptionsOnEveryBrokerWithin100msRegression) {
    AuthTrio t("revoke");
    t.start();
    t.bootstrapAdmin();
    t.createUser(1, "builder");
    for (SO op : {SO::Create, SO::Alter, SO::Drop, SO::Describe})
        t.mustOk(t.ddl(1, [&](AuthWireClient& w, const std::string& ch, int64_t sid) { return w.grant(ch, sid, "builder", false, op, ST::Cluster, SP::Exact, ""); }), "builder grant");
    t.grant(1, "builder", SO::Read, ST::Topic, SP::Prefix, "");
    t.createUser(2, "reader");
    t.createUser(3, "writer");

    AuthWireClient& bw = t.newWire(1);
    std::vector<AuthTrio::Session> bs;
    for (int id : {1, 2, 3}) {
        AuthTrio::Session s;
        ASSERT_TRUE(AuthTrio::until(30s, [&] { try { s = t.loginOn(bw, id, "builder"); return true; } catch (...) { return false; } }));
        bs.push_back(s);
    }
    const LedTopic led = defineLedTopic(t, bs, "w.orders");
    ASSERT_GE(led.id, 0);
    ASSERT_TRUE(led.part[0] >= 0 && led.part[1] >= 0 && led.part[2] >= 0) << "each broker must lead one partition";

    // The reader holds one wire with a session on each broker.
    AuthWireClient& rw = t.newWire(1);
    std::array<AuthTrio::Session, 3> rs;
    ASSERT_TRUE(AuthTrio::until(30s, [&] {
        try { for (int id : {1, 2, 3}) rs[static_cast<std::size_t>(id - 1)] = t.loginOn(rw, id, "reader"); return true; } catch (...) { return false; }
    }));

    std::vector<double> samples;
    for (int run = 0; run < 20; ++run) {
        t.grant(1 + run % 3, "reader", SO::Read, ST::Topic, SP::Prefix, "w.");
        std::array<int64_t, 3> corr{};
        // Subscribe on each broker: bounded retries while the grant reaches that broker and the topic is placed.
        for (int id : {1, 2, 3}) {
            const auto& s = rs[static_cast<std::size_t>(id - 1)];
            bool bound = false;
            int lastCode = -999;
            const auto deadline = Clock3::now() + 30s;
            while (!bound && Clock3::now() < deadline) {
                const int64_t c = rw.raw().sendSubscribe(t.channel(id), AuthWireClient::kClientStream, s.sid, led.id, led.part[static_cast<std::size_t>(id - 1)]);
                const auto code = rw.raw().awaitSubscribeCode(c, Clock3::now() + 10s);
                if (code && *code == 0) { corr[static_cast<std::size_t>(id - 1)] = c; bound = true; }
                else { lastCode = code ? *code : -1; std::this_thread::sleep_for(20ms); }
            }
            ASSERT_TRUE(bound) << "run " << run << ": subscribe on node " << id << " last code " << lastCode;
        }
        const auto ok = t.revoke(1 + (run + 1) % 3, "reader", SO::Read, ST::Topic, SP::Prefix, "w.");
        const auto t0 = Clock3::now();
        ASSERT_EQ(ok.errorCode, 0) << ok.detail;
        std::array<Clock3::time_point, 3> seen{};
        std::array<bool, 3> got{};
        rw.raw().pollUntil([&] {
            const auto now = Clock3::now();
            for (const auto& e : rw.raw().errors()) {
                for (int i = 0; i < 3; ++i) {
                    if (!got[static_cast<std::size_t>(i)] && e.correlationId == corr[static_cast<std::size_t>(i)] && e.code == 45 &&
                        e.sessionId == rs[static_cast<std::size_t>(i)].sid) {
                        got[static_cast<std::size_t>(i)] = true; seen[static_cast<std::size_t>(i)] = now;
                    }
                }
            }
            return got[0] && got[1] && got[2];
        });
        for (int i = 0; i < 3; ++i) {
            ASSERT_TRUE(got[static_cast<std::size_t>(i)]) << "run " << run << ": no ErrorResponse{45} on node " << i + 1;
            samples.push_back(std::chrono::duration<double, std::milli>(seen[static_cast<std::size_t>(i)] - t0).count());
        }
    }
    std::sort(samples.begin(), samples.end());
    const double p50 = samples[samples.size() / 2], mx = samples.back();
    std::printf("REVOKE-PROPAGATION samples=%zu p50=%.2f ms max=%.2f ms\n", samples.size(), p50, mx);
    EXPECT_EQ(samples.size(), 60u);
    EXPECT_LE(mx, 100.0) << "§12: a REVOKE ends subscriptions on every broker within 100 ms (p50 " << p50 << ")";
}

namespace {

/// The partition-0 leader of `topic` as broker `via`'s metadata (the builder session) says; -1 = none yet.
int32_t leaderOf(AuthTrio& t, const AuthTrio::Session& s, int32_t topic, int32_t partition) {
    for (const PartitionInfo& p : s.w->raw().metadata(t.channel(s.node), AuthWireClient::kClientStream, s.sid))
        if (p.topicId == topic && p.partition == partition) return p.leaderId;
    return -1;
}

/// Counts the log lines containing a needle (the shared ring of LogCapture is too small for a noisy 3-node run).
class NeedleSink final : public spdlog::sinks::base_sink<std::mutex> {
public:
    explicit NeedleSink(std::vector<std::string> needles) : needles_(std::move(needles)), counts_(needles_.size()) {}
    [[nodiscard]] int count(std::size_t i) const { std::lock_guard<std::mutex> g(const_cast<std::mutex&>(mutex_)); return counts_[i]; }
protected:
    void sink_it_(const spdlog::details::log_msg& m) override {
        const std::string text(m.payload.data(), m.payload.size());
        for (std::size_t i = 0; i < needles_.size(); ++i) if (text.find(needles_[i]) != std::string::npos) ++counts_[i];
    }
    void flush_() override {}
private:
    std::vector<std::string> needles_;
    std::vector<int> counts_;
};
struct NeedleCapture {
    std::shared_ptr<NeedleSink> sink;
    std::shared_ptr<spdlog::logger> previous = spdlog::default_logger();
    explicit NeedleCapture(std::vector<std::string> needles) : sink(std::make_shared<NeedleSink>(std::move(needles))) {
        std::vector<spdlog::sink_ptr> sinks{sink};
        // Diagnosing a failure (issue #403): RUNE_DIAG_NEEDLE_FORWARD=1 also forwards every line to the previous logger.
        if (std::getenv("RUNE_DIAG_NEEDLE_FORWARD") != nullptr) for (const auto& s : previous->sinks()) sinks.push_back(s);
        auto l = std::make_shared<spdlog::logger>("needle-capture", sinks.begin(), sinks.end());
        l->set_level(spdlog::level::info);
        spdlog::set_default_logger(l);
    }
    ~NeedleCapture() { spdlog::set_default_logger(previous); }
};

/// A user with the builder grants (CREATE/ALTER/DROP/DESCRIBE on the cluster, READ on every topic).
void createBuilder(AuthTrio& t, int prefer = 1) {
    t.createUser(prefer, "builder");
    for (SO op : {SO::Create, SO::Alter, SO::Drop, SO::Describe})
        t.mustOk(t.ddl(prefer, [&](AuthWireClient& w, const std::string& ch, int64_t sid) { return w.grant(ch, sid, "builder", false, op, ST::Cluster, SP::Exact, ""); }), "builder grant");
    t.grant(prefer, "builder", SO::Read, ST::Topic, SP::Prefix, "");
}

/// The committed ISR of user partition (topic, p) as `n`'s control thread sees it (empty when unknown).
std::vector<int32_t> userIsrOf(BrokerNode& n, int32_t topic, int32_t p) {
    return n.onControlThreadForTest([&n, topic, p]() -> std::vector<int32_t> {
        const MetadataService* ms = n.metadataServiceForTest();
        const auto* pa = ms ? ms->assignment(topic, p) : nullptr;
        return pa ? pa->isr : std::vector<int32_t>{};
    });
}
/// `node` is (in == true) / is not (in == false) in the ISR of __catalog, __security and every partition of `topic`
/// (when topic >= 0), as broker `observer` sees them.
bool isrHas(AuthTrio& t, int observer, int32_t node, bool in, int32_t topic) {
    BrokerNode& o = t.node(observer);
    const auto sec = securityAssignmentOf(o);
    const auto cat = catalogAssignmentOf(o);
    if (!sec || !cat || contains(sec->isr, node) != in || contains(cat->isr, node) != in) return false;
    for (int32_t p = 0; topic >= 0 && p < 3; ++p) {
        const auto isr = userIsrOf(o, topic, p);
        if (isr.empty() || contains(isr, node) != in) return false;
    }
    return true;
}
bool awaitNodeNotInIsr(AuthTrio& t, int observer, int32_t node, std::chrono::milliseconds within, int32_t topic = -1) {
    return AuthTrio::until(within, [&] { return isrHas(t, observer, node, false, topic); });
}
bool awaitNodeInIsr(AuthTrio& t, int observer, int32_t node, std::chrono::milliseconds within, int32_t topic = -1) {
    return AuthTrio::until(within, [&] { return isrHas(t, observer, node, true, topic); });
}
/// {leadershipTerm, leader} of broker `n`'s consensus election (control thread, read-only); {-1, -1} when none.
std::pair<int64_t, int32_t> electionOf(BrokerNode* n) {
    return n->onControlThreadForTest([n]() {
        const auto* e = n->electionForTest();
        return std::pair<int64_t, int32_t>{e ? e->leadershipTermId() : -1, e ? e->leaderMemberId() : -1};
    });
}
int64_t partitionLeo(BrokerNode* n, int32_t topic, int32_t part) {
    return n->onOwnerThreadForTest(topic, part, [](rune::replication::PartitionReplica* r) { return r ? r->logEndOffset() : int64_t{-1}; });
}

}  // namespace

// D34: node 3 runs a different cluster key.  Nodes 1 and 2 log the refusal, node 3 logs "REFUSED by", counter 234 > 0
// everywhere; node 3 leaves every ISR, a client session on node 3 cannot produce to a partition led elsewhere;
// the right key brings it back.
TEST(AuthClusterRegression, WrongKeyBrokerNeverJoinsRegression) {
    NeedleCapture cap({"BrokerNode[1]: peer link to node 3 REFUSED by 3", "BrokerNode[2]: peer link to node 3 REFUSED by 3",
                       "BrokerNode[3]: peer link from node 1 REFUSED", "BrokerNode[3]: peer link from node 2 REFUSED"});
    AuthTrio t("wrongkey");
    t.start();
    t.bootstrapAdmin();
    createBuilder(t);
    t.createUser(1, "writer");
    t.grant(1, "writer", SO::Write, ST::Topic, SP::Prefix, "");
    const std::vector<AuthTrio::Session> bs = sessionsOnEveryBroker(t, "builder");   // bs[0]: node 1
    ASSERT_EQ(bs.size(), 3u) << "a builder session on every broker";
    const LedTopic led = defineLedTopic(t, bs, "wk.orders");
    ASSERT_GE(led.id, 0);
    ASSERT_TRUE(led.part[0] >= 0 && led.part[1] >= 0);
    ASSERT_TRUE(awaitNodeInIsr(t, 1, 3, 60s, led.id)) << "node 3 starts in every ISR";
    ASSERT_TRUE(AuthTrio::until(30s, [&] { return t.counter(3, kPeerLinksEstablishedCounterTypeId).value_or(0) == 2; }));

    const std::string bad = t.makeKeyFile("bad.key", 'X');
    const std::string good = t.makeKeyFile("good.key", 'K');
    t.restart(3, bad);
    ASSERT_TRUE(AuthTrio::until(60s, [&] {
        return cap.sink->count(0) > 0 && cap.sink->count(1) > 0 && cap.sink->count(2) > 0 && cap.sink->count(3) > 0 &&
               t.counter(1, kPeerLinkRefusalsCounterTypeId).value_or(0) > 0 && t.counter(2, kPeerLinkRefusalsCounterTypeId).value_or(0) > 0 &&
               t.counter(3, kPeerLinkRefusalsCounterTypeId).value_or(0) > 0;
    })) << "refusals: " << cap.sink->count(0) << "," << cap.sink->count(1) << "," << cap.sink->count(2) << "," << cap.sink->count(3)
        << " counters " << t.counter(1, kPeerLinkRefusalsCounterTypeId).value_or(-1) << "," << t.counter(2, kPeerLinkRefusalsCounterTypeId).value_or(-1)
        << "," << t.counter(3, kPeerLinkRefusalsCounterTypeId).value_or(-1);
    EXPECT_EQ(t.node(3).peerLinksForTest().established, 0);

    // Node 3 leaves the ISR of the catalog, __security and EVERY partition of the user topic, and stays out.
    ASSERT_TRUE(awaitNodeNotInIsr(t, 1, 3, 60s, led.id)) << "node 3 must leave every ISR";
    ASSERT_TRUE(awaitNodeNotInIsr(t, 2, 3, 60s, led.id));
    for (int i = 0; i < 20; ++i) {
        std::this_thread::sleep_for(100ms);   // observation window of a state that must NOT change
        for (int obs : {1, 2}) EXPECT_TRUE(isrHas(t, obs, 3, false, led.id)) << "node 3 re-entered an ISR seen by node " << obs;
    }
    // Issues #15 / #111: node 3 reaches no peer, so its `__security` state (on its own disk) is not provably current -
    // it serves no login at all (the retriable AuthUnavailable, never an OK on a frozen state), so no session of it
    // can send anything, and nothing reaches the leader's log.
    const int64_t leo0 = partitionLeo(&t.node(1), led.id, led.part[0]);
    {
        AuthWireClient& w = t.wire();
        for (int i = 0; i < 5; ++i) {
            const int64_t sid = w.connect(t.channel(3));
            ASSERT_GT(sid, 0);
            EXPECT_EQ(w.login(t.channel(3), sid, "writer", {AuthTrio::kPw, AuthTrio::kPw}).errorCode(), 48)
                << "attempt " << i << ": a broker cut off its peers answers AuthUnavailable";
        }
        EXPECT_NE(t.node(3).securityServingForTest(), "Serving");
    }
    EXPECT_EQ(partitionLeo(&t.node(1), led.id, led.part[0]), leo0) << "nothing reached the leader's log";

    // Node 3 never votes in a term the others accept.  Its consensus frames do not open on nodes 1 and 2, so with the
    // consensus leader gone the survivor has only its own vote and must NOT win a term - and the control: with the
    // RIGHT key node 3's vote does count and a term is won.
    BrokerNode* n1 = &t.node(1);
    BrokerNode* n2 = &t.node(2);
    const auto [term, consensusLeader] = electionOf(n1);
    ASSERT_TRUE(consensusLeader == 1 || consensusLeader == 2) << "a consensus leader among the linked brokers (term " << term << ", leader " << consensusLeader << ")";
    EXPECT_EQ(electionOf(n2), (std::pair<int64_t, int32_t>{term, consensusLeader})) << "nodes 1 and 2 agree on the term and the leader";
    const int survivor = 3 - consensusLeader;
    BrokerNode* sv = survivor == 1 ? n1 : n2;
    t.stopNode(consensusLeader);
    bool wonATerm = false;
    const auto windowEnd = Clock3::now() + 8s;   // an observation window of an outcome that must NOT happen
    while (Clock3::now() < windowEnd) {
        const auto [tm, ld] = electionOf(sv);
        if (ld == survivor && tm > term) wonATerm = true;
        std::this_thread::sleep_for(50ms);
    }
    EXPECT_FALSE(wonATerm) << "node " << survivor << " must not win a term on node 3's (unopened) vote";
    t.restart(3, good);   // positive control: the same two-broker majority, now with a vote that opens
    BrokerNode* n3 = &t.node(3);
    EXPECT_TRUE(AuthTrio::until(20s, [&] {
        const auto [tm, ld] = electionOf(sv);
        const auto e3 = electionOf(n3);
        return tm > term && (ld == survivor || ld == 3) && e3.second == ld;
    })) << "with the right key node 3's vote counts: a new term is won";
    t.restart(consensusLeader, t.keyContentsPath(consensusLeader));
    EXPECT_TRUE(awaitNodeInIsr(t, 1, 3, 60s, led.id)) << "the right key brings node 3 back into every ISR";
    EXPECT_TRUE(awaitNodeInIsr(t, 2, 3, 60s, led.id));
}

// Issue #403 (harness): a SecurityBootstrap sent to a broker that does not lead `__security` is answered at once - the
// follower routes it with an ErrorResponse{Redirect} - and AuthWireClient::bootstrap returns that answer.  RED before
// the fix: bootstrap() waited for a DdlResponse only, so every attempt on a follower burnt its whole 30 s and
// AuthTrio::bootstrapAdmin (60 s over the three brokers) ran out of attempts whenever leadership moved under load.
TEST(AuthClusterRegression, ASecurityBootstrapThroughAFollowerIsAnsweredRedirectRegression) {
    AuthTrio t("bootredirect");
    t.start();
    const int32_t leader = settledSecurityLeader(t.cluster());
    ASSERT_GT(leader, 0);
    const int follower = leader == 1 ? 2 : 1;
    AuthWireClient& w = t.wire();
    ASSERT_TRUE(AuthTrio::until(30s, [&] {   // the follower's route names the leader (not "no leader known yet": 2)
        const int64_t s0 = w.connect(t.channel(follower));
        if (s0 <= 0) return false;
        const auto b = w.bootstrap(t.channel(follower), s0, t.keyContents(follower), "admin", AuthTrio::kBootstrapPassword);
        (void)w.raw().disconnect(t.channel(follower), AuthWireClient::kClientStream, s0);
        EXPECT_NE(b.errorCode, 0xFFFF) << "the follower's answer never reached bootstrap(): " << b.detail;
        EXPECT_NE(b.errorCode, 0) << "a follower bootstrapped the cluster";
        return b.errorCode == static_cast<uint16_t>(pc::ErrorCode::Redirect);
    })) << "the follower answers Redirect";
    t.bootstrapAdmin();   // and the trio's own bootstrap still lands on the leader
    EXPECT_TRUE(t.node(leader).securityViewForTest().hasSecurityAdmin);
}

// D32: Enabled with no cluster key refuses to start, naming the variable.
TEST(AuthClusterRegression, NoKeyBrokerRefusesToStart) {
    AuthTrio t("nokey");
    t.setKey(1, "");
    try {
        t.cluster().startNode(1);
        FAIL() << "a broker without a cluster key must refuse to start";
    } catch (const std::exception& e) {
        EXPECT_NE(std::string(e.what()).find("RUNE_CLUSTER_KEY_FILE"), std::string::npos) << e.what();
    }
}

// D31: dev mode (RUNE_AUTH=disabled) refuses a multi-broker cluster, on every node.
TEST(AuthClusterRegression, DevModeRefusesMultiNode) {
    AuthTrio t("dev");
    t.setAuthMode(AuthMode::DevDisabled);
    for (int id : {1, 2, 3}) {
        try {
            t.cluster().startNode(id);
            ADD_FAILURE() << "node " << id << " must refuse dev mode in a 3-broker cluster";
        } catch (const std::exception& e) {
            EXPECT_NE(std::string(e.what()).find("multi-broker"), std::string::npos) << e.what();
        }
    }
}

// Task 22 seam: the __security leader appends a CREATE USER and is killed before answering; the client retries the SAME
// request id on the new leader: OK, and exactly one CreateUser for it in every broker's log.
// #205: CREATE USER is answered OK once the `__security` LEADER enforces it (D24).  A follower holds the event in its log
// (acks=all) but its view may not have applied it yet: a login there used to fail with the terminal AuthenticationFailed
// (44), and the client stops for good.  While a broker's view is behind events it holds, a failed proof is answered the
// retriable AuthUnavailable (48); once applied, the login succeeds.
TEST(AuthClusterRegression, AJustCreatedUserCanLogInThroughALaggingFollowerRegression) {
    AuthTrio t("lagfollower");
    t.start();
    t.bootstrapAdmin();
    const int32_t leader = settledSecurityLeader(t.cluster());
    ASSERT_GT(leader, 0);
    const int follower = leader == 1 ? 2 : 1;
    ASSERT_TRUE(t.awaitDigestsAgree(0, 30s));
    t.node(follower).holdSecurityApplyForTest(true);   // it replicates, its view applies nothing
    t.createUser(leader, "lagged");                    // answered OK: the leader enforces it
    AuthWireClient& w = t.wire();
    const int64_t sid = w.connect(t.channel(follower));
    ASSERT_GT(sid, 0);
    EXPECT_EQ(w.login(t.channel(follower), sid, "lagged", {AuthTrio::kPw, AuthTrio::kPw}).errorCode(), 48)
        << "a view behind events this broker holds answers retriable, never the terminal 44";
    t.node(follower).holdSecurityApplyForTest(false);
    ASSERT_TRUE(AuthTrio::until(20s, [&] { try { (void)t.login(follower, "lagged"); return true; } catch (...) { return false; } }))
        << "once applied, the same login succeeds on the follower";
    // A wrong password against a caught-up view is still the terminal 44 (only the lag is retriable).
    const int64_t sid2 = w.connect(t.channel(follower));
    ASSERT_GT(sid2, 0);
    EXPECT_EQ(w.login(t.channel(follower), sid2, "lagged", {"wrong-password-1", "wrong-password-1"}).errorCode(), 44)
        << "a caught-up view's failed proof stays terminal";
}

TEST(AuthClusterRegression, SecurityDdlRetryAfterLeaderKillAppliesOnceRegression) {
    AuthTrio t("leaderkill");
    t.start();
    t.bootstrapAdmin();
    const int32_t leader = settledSecurityLeader(t.cluster());
    ASSERT_GT(leader, 0);
    const rune::protocol::RequestId rid{0xDEAD'BEEF'0000'0001ull, 77};

    ASSERT_TRUE(t.awaitDigestsAgree(0, 30s));
    const int64_t before = t.node(leader).securityViewForTest().appliedOffset;
    t.node(leader).holdSecurityMirrorForTest(true);   // the OK is gated on this broker enforcing the event: held
    const AuthTrio::Session a = t.adminOn(leader);
    const auto verifier = AuthWireClient::verifierOf(AuthTrio::kPw);   // a retry is byte-identical: ONE verifier for both attempts
    t.wire().pinRequestId(rid);
    const auto held = a.w->createUserWithVerifier(t.channel(leader), a.sid, "killvictim", verifier, 2s);
    EXPECT_NE(held.errorCode, 0) << "the held leader never answers";
    // The CreateUser is appended and replicated (committed) while the answer is held.
    ASSERT_TRUE(AuthTrio::until(30s, [&] {
        for (BrokerNode* n : t.cluster().live())
            if (n->nodeIdForTest() != leader && n->securityViewForTest().appliedOffset <= before) return false;
        return true;
    })) << "the event must be committed and applied everywhere before the kill";

    // The id the committed CreateUser gave the user, read on a surviving follower: the retry must answer THE SAME id.
    const int follower = leader == 1 ? 2 : 1;
    const uint64_t committedId = t.node(follower).securityUserIdForTest("killvictim");
    ASSERT_NE(committedId, 0u) << "the user exists on the follower";

    t.node(leader).crashForTest();
    t.stopNode(leader);
    ASSERT_TRUE(AuthTrio::until(60s, [&] { const int32_t l = settledSecurityLeader(t.cluster()); return l > 0 && l != leader; }))
        << "a new __security leader";
    const int32_t other = settledSecurityLeader(t.cluster());
    const auto retry = t.ddl(other, [&](AuthWireClient& w, const std::string& ch, int64_t sid) { return w.createUserWithVerifier(ch, sid, "killvictim", verifier); });
    t.wire().pinRequestId(std::nullopt);
    EXPECT_EQ(retry.errorCode, 0) << retry.detail;
    EXPECT_EQ(retry.objectId, committedId) << "the retry is answered with the id of the ONE committed CreateUser";

    auto countIn = [&](BrokerNode& n) {
        return n.onAdminThreadForTest([&n]() -> int {
            const std::vector<uint8_t> bytes = n.securityLogForTest()->read(0, 1 << 22);
            const std::string_view hay(reinterpret_cast<const char*>(bytes.data()), bytes.size());
            // the request id's high word (little endian) is stored once per event that carries it
            const char needle[8] = {1, 0, 0, 0, static_cast<char>(0xEF), static_cast<char>(0xBE), static_cast<char>(0xAD), static_cast<char>(0xDE)};
            const std::string_view nd(needle, 8);
            int c = 0;
            for (std::size_t p = hay.find(nd); p != std::string_view::npos; p = hay.find(nd, p + 1)) ++c;
            return c;
        });
    };
    ASSERT_TRUE(t.awaitDigestsAgree(before + 1, 30s));
    for (BrokerNode* n : t.cluster().live()) EXPECT_EQ(countIn(*n), 1) << "node " << n->nodeIdForTest() << ": exactly one CreateUser";
    // The killed leader comes back, catches up, and its log holds it once too.
    t.restart(leader, t.keyContentsPath(leader));
    ASSERT_TRUE(t.awaitDigestsAgree(before + 1, 60s));
    EXPECT_EQ(countIn(t.node(leader)), 1);
    EXPECT_EQ(t.node(leader).securityUserIdForTest("killvictim"), committedId) << "the restarted leader replays the same id";
}

namespace {

int ackOrErrorCode(ShardTestClient& c, int64_t corr, std::chrono::milliseconds within);   // defined with the matrix

struct ProducerResult { std::vector<int64_t> acked; std::vector<Clock3::time_point> ackedAt; int attempts = 0; };
/// Issue #403: the producer thread publishes the time of its latest ack (steady-clock ns; 0 = none yet) so the test
/// thread waits for traffic by condition instead of a fixed sleep.  Written by the producer only (release), read by the
/// test (acquire) - test-harness code.
using LastAck = std::atomic<int64_t>;
int64_t steadyNs(Clock3::time_point tp) { return std::chrono::duration_cast<std::chrono::nanoseconds>(tp.time_since_epoch()).count(); }
struct ConsumerResult { std::map<int64_t, int> deliveries; std::set<int64_t> offsets; bool complete = false; int resubscribes = 0; };

/// A producer that owns its client (own Aeron driver) and its thread: it logs in on whichever broker answers, follows
/// Redirects, re-logs-in after a restart, and hands its acked offsets back at join.  Shares nothing with the test.
ProducerResult runProducer(std::stop_token stop, std::array<std::string, 3> channels, std::filesystem::path dir, int32_t topic,
                           LastAck* lastAck) {
    ProducerResult out;
    AuthWireClient w(dir);
    std::array<int64_t, 3> sid{};
    std::size_t node = 0;
    while (!stop.stop_requested()) {
        const std::size_t n = node % 3;
        if (sid[n] == 0) {
            const int64_t s = w.connect(channels[n], pc::kClientVersionAuth, 2s);
            if (s > 0 && w.login(channels[n], s, "prod", {AuthTrio::kPw, AuthTrio::kPw}, 0, true, 5s).ok()) sid[n] = s;
            else { ++node; std::this_thread::sleep_for(20ms); continue; }
        }
        auto& c = w.raw();
        const int64_t corr = c.nextCorrelationId();
        ++out.attempts;
        if (!c.produce(channels[n], AuthWireClient::kClientStream, sid[n], corr, topic, 0, 1, makeBatch(1))) { sid[n] = 0; ++node; continue; }
        const int code = ackOrErrorCode(c, corr, 1s);
        if (code == 0) {
            out.acked.push_back(c.ackFor(corr)->baseOffset);
            out.ackedAt.push_back(Clock3::now());
            lastAck->store(steadyNs(out.ackedAt.back()), std::memory_order_release);
            std::this_thread::sleep_for(10ms);
            continue;
        }
        if (code != 3 && code != 2) sid[n] = 0;   // no answer / revoked / refused: this broker's session is gone
        ++node;
        std::this_thread::sleep_for(10ms);
    }
    return out;
}

/// A consumer with its own client and thread: subscribes from the next missing offset on whichever broker leads,
/// resubscribes elsewhere when its broker restarts.  `target` (a one-shot hand-off from the test: the last acked offset)
/// tells it what "everything" is; it returns every offset it saw.
ConsumerResult runConsumer(std::stop_token stop, std::future<int64_t> target, std::array<std::string, 3> channels,
                           std::filesystem::path dir, int32_t topic, std::size_t batchBytes) {
    ConsumerResult out;
    AuthWireClient w(dir);
    std::array<int64_t, 3> sid{};
    std::size_t node = 0;
    int64_t want = -2;   // -2 = not told yet
    int64_t next = 0;
    const auto hardStop = Clock3::now() + 240s;
    while (!stop.stop_requested() && Clock3::now() < hardStop) {
        if (want == -2 && target.wait_for(0ms) == std::future_status::ready) want = target.get();
        if (want >= -1 && static_cast<int64_t>(out.offsets.size()) > want && *out.offsets.rbegin() >= want) { out.complete = true; break; }
        const std::size_t n = node % 3;
        if (sid[n] == 0) {
            const int64_t s = w.connect(channels[n], pc::kClientVersionAuth, 2s);
            if (s > 0 && w.login(channels[n], s, "cons", {AuthTrio::kPw, AuthTrio::kPw}, 0, true, 5s).ok()) sid[n] = s;
            else { ++node; std::this_thread::sleep_for(20ms); continue; }
        }
        auto& c = w.raw();
        const std::size_t firstRecord = c.records().size();   // subscription ids repeat per broker: only records from now on are ours
        const int64_t corr = c.subscribeIsolated(channels[n], AuthWireClient::kClientStream, sid[n], topic, 0, next, 0);
        const auto code = corr ? c.awaitSubscribeCode(corr, Clock3::now() + 3s) : std::nullopt;
        if (!code || *code != 0) { if (!code || *code != 3) sid[n] = 0; ++node; std::this_thread::sleep_for(20ms); continue; }
        ++out.resubscribes;
        const int64_t sub = c.subscriptionIdOf(corr).value_or(0);
        std::size_t seenRecords = firstRecord;
        auto lastProgress = Clock3::now();
        bool broken = false;
        while (!stop.stop_requested() && !broken && Clock3::now() < hardStop) {
            c.pollFor(50ms, [] { return false; });
            for (; seenRecords < c.records().size(); ++seenRecords) {
                const auto& r = c.records()[seenRecords];
                if (r.subscriptionId != sub) continue;
                const std::size_t k = batchBytes ? r.bytes.size() / batchBytes : 0;
                for (std::size_t i = 0; i < std::max<std::size_t>(k, 1); ++i) {
                    out.offsets.insert(r.baseOffset + static_cast<int64_t>(i));
                    ++out.deliveries[r.baseOffset + static_cast<int64_t>(i)];
                }
                lastProgress = Clock3::now();
            }
            while (out.offsets.count(next)) ++next;
            for (const auto& e : c.errors()) if (e.sessionId == sid[n] && e.code != 0) broken = true;
            if (want == -2 && target.wait_for(0ms) == std::future_status::ready) want = target.get();
            if (want >= -1 && next > want) { out.complete = true; return out; }
            if (Clock3::now() - lastProgress > 4s && want != -2) broken = true;   // told the target but starved: another broker
        }
        sid[n] = 0;
        ++node;
    }
    return out;
}

}  // namespace

// D33/D34: a REAL concurrent producer and consumer (each its own client and thread, nothing shared) run through three
// rolling restarts of the cluster key: (K1 | K2) -> (K2 | K1) -> (K2).  After every restart all links re-establish
// (counter 233 == 2 on each broker); at the end the consumer has seen every acknowledged offset, contiguous from 0.
TEST(AuthClusterRegression, KeyRotationAcrossARollingRestartKeepsTrafficFlowingRegression) {
    AuthTrio t("rotate");
    t.start();
    t.bootstrapAdmin();
    createBuilder(t);
    t.createUser(1, "prod");
    t.grant(1, "prod", SO::Write, ST::Topic, SP::Prefix, "rot.");
    t.createUser(1, "cons");
    t.grant(1, "cons", SO::Read, ST::Topic, SP::Prefix, "rot.");
    const std::vector<AuthTrio::Session> bs = sessionsOnEveryBroker(t, "builder");   // bs[0]: node 1
    ASSERT_EQ(bs.size(), 3u) << "a builder session on every broker";
    const int32_t topic = defineTopicAnywhere(t, bs, "rot.t", 1);
    ASSERT_GE(topic, 0);
    ASSERT_TRUE(AuthTrio::until(60s, [&] { const int32_t l = leaderOf(t, bs[0], topic, 0); return l >= 1 && l <= 3; }));
    const std::size_t batchBytes = makeBatch(1).size();

    const std::string k1 = t.keyContentsPath(1);
    const std::string k2 = t.makeKeyFile("k2.key", 'Q');
    const std::array<std::string, 3> channels{t.channel(1), t.channel(2), t.channel(3)};
    std::promise<int64_t> target;
    std::promise<ProducerResult> pOut;
    std::promise<ConsumerResult> cOut;
    auto pFut = pOut.get_future();
    auto cFut = cOut.get_future();
    // everything the threads need is copied BEFORE they start: they never touch the trio while it restarts nodes
    const std::filesystem::path prodDir = t.cluster().clientsDir->path / "rot_prod", consDir = t.cluster().clientsDir->path / "rot_cons";
    LastAck lastAck{0};
    // PR #604 review: the threads are declared AFTER everything they reference (lastAck, pOut, cOut, target) - an
    // early ASSERT return destroys locals in reverse order, so each jthread is stopped and joined before them.
    std::jthread producer;
    std::jthread consumer;
    producer = std::jthread([&pOut, &lastAck, ch = channels, prodDir, topic](std::stop_token st) {
        try { pOut.set_value(runProducer(st, ch, prodDir, topic, &lastAck)); }
        catch (...) { pOut.set_exception(std::current_exception()); }
    });
    consumer = std::jthread([&cOut, ch = channels, consDir, topic, batchBytes, fut = target.get_future()](std::stop_token st) mutable {
        try { cOut.set_value(runConsumer(st, std::move(fut), ch, consDir, topic, batchBytes)); }
        catch (...) { cOut.set_exception(std::current_exception()); }
    });

    auto allLinked = [&] {
        return AuthTrio::until(60s, [&] {
            for (int id : {1, 2, 3}) if (t.counter(id, kPeerLinksEstablishedCounterTypeId).value_or(0) != 2) return false;
            return true;
        });
    };
    ASSERT_TRUE(allLinked());
    struct Step { const char* name; std::string cur, next; };
    const std::vector<Step> steps = {{"K1|K2", k1, k2}, {"K2|K1", k2, k1}, {"K2", k2, ""}};
    struct Restart { Clock3::time_point start, end; const char* step; int node; };
    std::vector<Restart> restarts;
    for (const Step& st : steps) {
        for (int id : {1, 2, 3}) {
            const auto t0 = Clock3::now();
            t.restart(id, st.cur, st.next);
            const bool linked = allLinked();
            const double ms = std::chrono::duration<double, std::milli>(Clock3::now() - t0).count();
            std::printf("ROTATION step=%s node=%d restart+links-reestablished=%.0f ms\n", st.name, id, ms);
            EXPECT_TRUE(linked) << "step " << st.name << ": links after restarting node " << id;
            EXPECT_TRUE(awaitSecurityLeader(t.cluster(), 60s));
            const auto end = Clock3::now();
            restarts.push_back({t0, end, st.name, id});
            // Issue #403: traffic keeps flowing between restarts (the threads never stop) - the next restart waits, by
            // condition, for an ack that landed after this one ended (a fixed 4 s sleep made the "an ack landed after
            // each restart" check below a race against the load on the host).
            ASSERT_TRUE(AuthTrio::until(60s, [&] { return lastAck.load(std::memory_order_acquire) > steadyNs(end); }))
                << "step " << st.name << ": no ack within 60 s after restarting node " << id;
        }
    }
    const auto loopEnd = Clock3::now();
    producer.request_stop();
    producer.join();
    const ProducerResult prod = pFut.get();
    ASSERT_FALSE(prod.acked.empty());
    const int64_t last = *std::max_element(prod.acked.begin(), prod.acked.end());
    target.set_value(last);
    consumer.join();
    const ConsumerResult cons = cFut.get();
    std::printf("ROTATION traffic: attempts=%d acked=%zu last=%lld consumer-offsets=%zu resubscribes=%d\n", prod.attempts,
                prod.acked.size(), static_cast<long long>(last), cons.offsets.size(), cons.resubscribes);
    EXPECT_TRUE(cons.complete) << "the consumer reached the last acknowledged offset";
    int64_t expect = 0;
    for (const int64_t o : cons.offsets) { if (o > last) break; EXPECT_EQ(o, expect++) << "no gap in the consumed offsets"; }
    for (const int64_t a : prod.acked) EXPECT_TRUE(cons.offsets.count(a)) << "acked offset " << a << " must be consumed";
    // Exactly once: no offset was delivered twice to the consumer.
    int dupes = 0;
    for (const auto& [o, n] : cons.deliveries) if (n > 1) { ++dupes; ADD_FAILURE() << "offset " << o << " delivered " << n << " times"; }
    EXPECT_EQ(dupes, 0);
    std::printf("ROTATION extra-stored-but-unacked=%lld\n", static_cast<long long>(static_cast<int64_t>(cons.offsets.size()) - static_cast<int64_t>(prod.acked.size())));
    // Traffic flowed THROUGH every step: after each restart finished and before the next began (the last: before the
    // producer was stopped) at least one record was acknowledged - including after the final K2-only restart.
    for (std::size_t k = 0; k < restarts.size(); ++k) {
        const auto from = restarts[k].end;
        const auto to = k + 1 < restarts.size() ? restarts[k + 1].start : loopEnd;
        const bool any = std::any_of(prod.ackedAt.begin(), prod.ackedAt.end(), [&](const auto& tp) { return tp > from && tp < to; });
        EXPECT_TRUE(any) << "no ack landed after step " << restarts[k].step << " restart of node " << restarts[k].node;
        // recovery: restart finished -> the first acknowledged record
        double firstMs = -1;
        for (const auto& tp : prod.ackedAt) if (tp > from) { firstMs = std::chrono::duration<double, std::milli>(tp - from).count(); break; }
        std::printf("ROTATION recovery step=%s node=%d restart-end-to-first-ack=%.0f ms\n", restarts[k].step, restarts[k].node, firstMs);
        // Floor: a restarted controller defers user-partition elections for one unservable-report TTL, then the
        // producer's 1 s no-answer timeout + re-login; +1 s margin. Derived, so a TTL change moves the bound with it.
        // Issue #403: a latency bound inside a correctness test - enforced only where latency gates run (CLAUDE.md §3: an
        // idle host, RUNE_LATENCY_GATES=1); always measured and printed.  The correctness of recovery (an ack after every
        // restart) is asserted above unconditionally.
        constexpr double kRecoveryBoundMs = rune::node::MetadataService::kUnservableReportTtlNs / 1e6 + 1000.0 + 1000.0;
        EXPECT_GE(firstMs, 0) << "step " << restarts[k].step << " node " << restarts[k].node << ": no ack after the restart";
        if (latencyGatesEnabled())
            EXPECT_LE(firstMs, kRecoveryBoundMs) << "step " << restarts[k].step << " node " << restarts[k].node << ": first ack after " << firstMs << " ms (bound " << kRecoveryBoundMs << ")";
    }
    if (!latencyGatesEnabled())
        std::printf("ROTATION recovery bound not enforced (set RUNE_LATENCY_GATES=1 on an idle host)\n");
}

// D8: AuthServerFirst for an unknown user carries the same fake salt and iteration count from every broker (derived from
// the replicated fake-salt seed), so a client cannot tell the brokers apart or an absent user from a present one.
TEST(AuthClusterRegression, UnknownUserSaltIsTheSameOnEveryBrokerRegression) {
    AuthTrio t("salt");
    t.start();
    t.bootstrapAdmin();
    t.createUser(1, "alice");
    ASSERT_TRUE(t.awaitDigestsAgree(1, 30s));
    struct Seen { std::string salt; uint32_t iterations = 0; };
    std::vector<Seen> seen;
    for (int id : {1, 2, 3}) {
        AuthWireClient& w = t.newWire(id);
        const int64_t sid = w.connect(t.channel(id));
        ASSERT_GT(sid, 0);
        const auto l = w.login(t.channel(id), sid, "nobody", {"x", "x"}, 0, /*sendFinal=*/false);
        ASSERT_TRUE(l.first.has_value()) << "node " << id;
        ASSERT_GE(l.first->count, 1);
        seen.push_back({std::string(l.first->salts[0].salt), l.first->salts[0].iterations});
    }
    EXPECT_FALSE(seen[0].salt.empty());
    for (int i = 1; i < 3; ++i) {
        EXPECT_EQ(seen[i].salt, seen[0].salt) << "node " << i + 1;
        EXPECT_EQ(seen[i].iterations, seen[0].iterations) << "node " << i + 1;
    }
}

// D28: node 3's applier is damaged (halts on the next event) -> its sessions are revoked and logins refused (fails
// closed); it is stopped, its `__security` directory moved aside, restarted: it refetches from offset 0, the digests
// agree again, logins work.
TEST(AuthClusterRegression, HaltedBrokerRefetchesSecurityAfterItsDirIsMovedAside) {
    AuthTrio t("halted");
    t.start();
    t.bootstrapAdmin();
    t.createUser(1, "alice");
    AuthTrio::Session before;
    ASSERT_TRUE(AuthTrio::until(30s, [&] { try { before = t.login(3, "alice"); return true; } catch (...) { return false; } }));
    ASSERT_TRUE(t.awaitDigestsAgree(1, 30s));

    BrokerNode& n3 = t.node(3);
    n3.onAdminThreadForTest([&n3]() {
        n3.adminWorkerAgentForTest()->worker().securityApplierForTest()->tamperNextFingerprintForTest();
        return 0;
    });
    t.createUser(1, "bob");   // replicated everywhere; node 3's applier fails its fingerprint check and halts
    ASSERT_TRUE(AuthTrio::until(30s, [&] { return n3.securityViewForTest().halted; })) << "node 3 must halt";
    // Fails closed: alice's LIVE session on node 3 is revoked (the session-level SessionRevoked frame) and gone.
    {
        auto& c = before.w->raw();
        bool revoked = false;
        c.pollFor(15s, [&] {
            for (const auto& e : c.errors()) if (e.sessionId == before.sid && e.code == 46) { revoked = true; return true; }
            return false;
        });
        EXPECT_TRUE(revoked) << "alice's session on the halted broker must be revoked (46)";
        EXPECT_FALSE(n3.gatewayHasSessionForTest(before.sid)) << "and removed from the gateway";
    }
    // alice's session on node 3 is revoked, new logins there are refused (44), the other brokers serve.
    AuthWireClient& w = t.newWire(3);
    const int64_t sid = w.connect(t.channel(3));
    ASSERT_GT(sid, 0);
    EXPECT_EQ(w.login(t.channel(3), sid, "alice", {AuthTrio::kPw, AuthTrio::kPw}).errorCode(), 44);
    EXPECT_NO_THROW(t.login(1, "alice"));
    EXPECT_GT(t.counter(3, kSecurityApplyHaltedCounterTypeId).value_or(0), 0);

    t.stopNode(3);
    const fs::path dir = t.cluster().dirs[2]->path / "__security";
    ASSERT_TRUE(fs::is_directory(dir));
    fs::rename(dir, t.cluster().dirs[2]->path / "__security.moved");
    t.restart(3, t.keyContentsPath(3));
    uint64_t digest = 0;
    ASSERT_TRUE(t.awaitDigestsAgree(2, 60s, &digest)) << "node 3 refetched the whole log and agrees";
    EXPECT_FALSE(t.node(3).securityViewForTest().halted);
    EXPECT_TRUE(AuthTrio::until(30s, [&] { try { t.login(3, "alice"); t.login(3, "bob"); return true; } catch (...) { return false; } }))
        << "logins succeed on node 3 again";
}

// §11 3-node slice: op x timing x via (the broker the user's session is on; the admin's DDL always goes through a
// different broker).  45 cells, every code from amatrix::expectedCodes.  Grant Exact, except SecurityDdl (ViaRole).
// Propagation of a change to the user's broker is asynchronous (the 100 ms gate has its own test), so each "after the
// change" observation waits, bounded, for the expected answer; "first" observations wait for the grant to arrive.
namespace {
namespace am = rune::node::test::amatrix;

struct UserSessions {
    std::string name;
    AuthWireClient* w = nullptr;
    std::array<int64_t, 3> sid{};
};

int ackOrErrorCode(ShardTestClient& c, int64_t corr, std::chrono::milliseconds within) {
    int code = kNoAnswer;
    c.pollFor(within, [&] {
        if (const auto a = c.ackFor(corr)) { code = a->errorCode; return true; }
        for (const auto& e : c.errors()) if (e.correlationId == corr) { code = e.code; return true; }
        return false;
    });
    return code;
}

/// Produce (Op::ProduceRouted): the legacy gateway-routed path - a Produce frame to broker `node`'s gateway client channel.
int gatewayProduce(AuthTrio& t, UserSessions& u, int node, int32_t topic, int32_t part) {
    auto& c = u.w->raw();
    const int64_t corr = c.nextCorrelationId();
    if (!c.produce(t.channel(node), AuthWireClient::kClientStream, u.sid[static_cast<std::size_t>(node - 1)], corr, topic, part, 1, makeBatch(1)))
        return kNoAnswer;
    return ackOrErrorCode(c, corr, 5s);
}
/// Produce (Op::Produce): the shard-mode path - straight to the owning worker's ingress (the endpoint / stream the
/// broker advertises in its metadata), bypassing the gateway; the worker's WRITE bitmap decides.
int shardProduce(UserSessions& u, int node, const PartitionInfo& pi, int32_t topic) {
    auto& c = u.w->raw();
    const int64_t corr = c.nextCorrelationId();
    if (!c.produce("aeron:udp?endpoint=" + pi.ingressEndpoint, pi.ingressStreamId, u.sid[static_cast<std::size_t>(node - 1)], corr, topic, pi.partition, 1, makeBatch(1)))
        return kNoAnswer;
    return ackOrErrorCode(c, corr, 5s);
}

/// DDL ops as the user: sent to `via`, the other brokers on Redirect / NotLeader.  `seq` makes every attempt's object new.
int ddlOnce(AuthTrio& t, UserSessions& u, int via, am::Op op, int seq) {
    auto& c = u.w->raw();
    for (int k = 0; k < 3; ++k) {
        const int node = (via - 1 + k) % 3 + 1;
        const int64_t sid = u.sid[static_cast<std::size_t>(node - 1)];
        int code = kNoAnswer;
        if (op == am::Op::CreateTopic) {
            const int64_t corr = c.sendDefineTopic(t.channel(node), AuthWireClient::kClientStream, sid,
                                                   "mx.new." + u.name + "." + std::to_string(seq), 1, authRigCharBlob());
            const auto a = c.awaitAnyDdlAnswer(corr, Clock3::now() + 15s);
            code = a ? a->code : kNoAnswer;
        } else {
            code = u.w->createRole(t.channel(node), sid, "mxr." + u.name + "." + std::to_string(seq)).errorCode;
        }
        if (code != 2 && code != 3) return code;
    }
    return kNoAnswer;
}

}  // namespace

TEST(AuthClusterRegression, MatrixSliceOpByTimingByViaBrokerRegression) {
    AuthTrio t("matrix");
    t.start();
    t.bootstrapAdmin();
    createBuilder(t);
    const std::vector<AuthTrio::Session> bs = sessionsOnEveryBroker(t, "builder");   // bs[0]: node 1
    ASSERT_EQ(bs.size(), 3u) << "a builder session on every broker";
    const LedTopic led = defineLedTopic(t, bs, "mx.t");
    ASSERT_GE(led.id, 0);
    ASSERT_TRUE(led.part[0] >= 0 && led.part[1] >= 0 && led.part[2] >= 0);

    using am::Op; using am::Timing; using am::Grant;
    const std::vector<Op> ops = {Op::ProduceRouted, Op::Produce, Op::Subscribe, Op::CreateTopic, Op::SecurityDdl};
    const std::vector<Timing> timings = {Timing::Steady, Timing::RevokedMid, Timing::UserDisabledMid};
    int cells = 0, n = 0;
    for (const Op op : ops) for (const Timing timing : timings) for (int via = 1; via <= 3; ++via) {
        const Grant grant = op == Op::SecurityDdl ? Grant::ViaRole : Grant::Exact;
        const am::ExpectedCodes want = am::expectedCodes(op, grant, timing);
        ASSERT_NE(am::expect(op, grant, timing), am::Outcome::NotApplicable);
        const int prefer = via % 3 + 1;   // the admin's DDL goes through a different broker
        SCOPED_TRACE(std::string(am::name(op)) + "/" + std::string(am::name(timing)) + "/via" + std::to_string(via));
        ++cells;
        UserSessions u;
        u.name = "mu" + std::to_string(++n);
        t.createUser(prefer, u.name);
        const int32_t part = led.part[static_cast<std::size_t>(via - 1)];   // the partition `via` leads (its worker owns it)
        PartitionInfo ingress;
        if (op == Op::Produce) {
            ASSERT_TRUE(AuthTrio::until(30s, [&] {
                for (const PartitionInfo& pi : bs[0].w->raw().metadata(t.channel(bs[0].node), AuthWireClient::kClientStream, bs[0].sid))
                    if (pi.topicId == led.id && pi.partition == part && pi.leaderId == via && !pi.ingressEndpoint.empty()) { ingress = pi; return true; }
                return false;
            })) << "the worker ingress of the partition via " << via << " leads";
        }
        const std::string newTopicGrant = "mx.new." + u.name + ".";
        const std::string role = "mxrole." + u.name;
        auto doRevoke = [&] {
            switch (op) {
                case Op::Produce: case Op::ProduceRouted: return t.revoke(prefer, u.name, SO::Write, ST::Topic, SP::Exact, "mx.t");
                case Op::Subscribe: return t.revoke(prefer, u.name, SO::Read, ST::Topic, SP::Exact, "mx.t");
                case Op::CreateTopic: return t.revoke(prefer, u.name, SO::Create, ST::Topic, SP::Prefix, newTopicGrant);
                default: return t.ddl(prefer, [&](AuthWireClient& w, const std::string& ch, int64_t sid) { return w.revokeRole(ch, sid, u.name, role); });
            }
        };
        switch (op) {
            case Op::Produce: case Op::ProduceRouted: t.grant(prefer, u.name, SO::Write, ST::Topic, SP::Exact, "mx.t"); break;
            case Op::Subscribe: t.grant(prefer, u.name, SO::Read, ST::Topic, SP::Exact, "mx.t"); break;
            case Op::CreateTopic: t.grant(prefer, u.name, SO::Create, ST::Topic, SP::Prefix, newTopicGrant); break;
            default:
                t.mustOk(t.ddl(prefer, [&](AuthWireClient& w, const std::string& ch, int64_t sid) { return w.createRole(ch, sid, role); }), "role");
                t.grant(prefer, role, SO::Admin, ST::Cluster, SP::Exact, "", /*role=*/true);
                t.mustOk(t.ddl(prefer, [&](AuthWireClient& w, const std::string& ch, int64_t sid) { return w.grantRole(ch, sid, u.name, role); }), "grant role");
        }
        u.w = &t.wire();   // the trio's one driver; sessions are per user and broker
        for (int id : {1, 2, 3}) {
            AuthTrio::Session s;
            ASSERT_TRUE(AuthTrio::until(30s, [&] { try { s = t.loginOn(*u.w, id, u.name); return true; } catch (...) { return false; } }));
            u.sid[static_cast<std::size_t>(id - 1)] = s.sid;
        }
        const int64_t sid = u.sid[static_cast<std::size_t>(via - 1)];
        auto& c = u.w->raw();
        int seq = 0;
        int64_t subCorr = 0;
        auto action = [&]() -> int {
            switch (op) {
                case Op::Produce: return shardProduce(u, via, ingress, led.id);
                case Op::ProduceRouted: return gatewayProduce(t, u, via, led.id, part);
                case Op::Subscribe: {
                    subCorr = c.sendSubscribe(t.channel(via), AuthWireClient::kClientStream, sid, led.id, part);
                    const auto code = c.awaitSubscribeCode(subCorr, Clock3::now() + 10s);
                    return code ? *code : kNoAnswer;
                }
                default: return ddlOnce(t, u, via, op, ++seq);
            }
        };
        int first = kNoAnswer;
        AuthTrio::until(20s, [&] { first = action(); return first == want.first; });
        ASSERT_EQ(first, want.first) << "first answer";
        if (timing == Timing::Steady) continue;

        if (timing == Timing::RevokedMid) {
            const auto r = doRevoke();
            ASSERT_EQ(r.errorCode, 0) << r.detail;
            if (op == Op::Subscribe) {
                EXPECT_EQ(ackOrErrorCode(c, subCorr, 10s), want.second) << "the live subscription ends NotAuthorized";
            } else {
                int second = kNoAnswer;
                AuthTrio::until(10s, [&] { second = action(); return second == want.second; });
                EXPECT_EQ(second, want.second);
            }
        } else {   // UserDisabledMid
            t.mustOk(t.ddl(prefer, [&](AuthWireClient& w, const std::string& ch, int64_t s) {
                return w.alterPassword(ch, s, u.name, pc::UserChangeKind::Disable); }), "disable");
            int revoked = kNoAnswer;
            c.pollFor(10s, [&] {
                for (const auto& e : c.errors()) if (e.sessionId == sid && e.code == want.second) { revoked = e.code; return true; }
                return false;
            });
            EXPECT_EQ(revoked, want.second) << "the session is revoked";
            if (op == Op::Subscribe) {
                int onSub = kNoAnswer;
                c.pollFor(10s, [&] {
                    for (const auto& e : c.errors()) if (e.correlationId == subCorr && e.code == want.second) { onSub = e.code; return true; }
                    return false;
                });
                EXPECT_EQ(onSub, want.second) << "the live subscription itself completes SessionRevoked";
            }
            AuthWireClient& w2 = t.wire();
            const int64_t s2 = w2.connect(t.channel(via));
            ASSERT_GT(s2, 0);
            int reconnect = kNoAnswer;
            AuthTrio::until(10s, [&] { reconnect = w2.login(t.channel(via), s2, u.name, {AuthTrio::kPw, AuthTrio::kPw}).errorCode(); return reconnect == want.reconnect; });
            EXPECT_EQ(reconnect, want.reconnect) << "a reconnect is refused";
        }
    }
    EXPECT_EQ(cells, 45);
}

// Issue #121 / #114 on THREE brokers: the last-admin guard is the leader's dry run over the replicated state, so a NEW
// __security leader (the old one killed) refuses removing the last admin exactly as the old one would; and a user's
// own password change sent through a FOLLOWER broker (redirected session there, its own login nonce) is proven per
// session: unsigned it is refused, signed it applies and replicates.
TEST(AuthClusterRegression, TheLastAdminGuardAndOwnChangeProofsHoldAcrossALeaderKillAdversarialRegression) {
    AuthTrio t("lastadminkill");
    t.start();
    t.bootstrapAdmin();
    t.createUser(1, "ws1", AuthTrio::kPw);
    const int leader = settledSecurityLeader(t.cluster());
    ASSERT_GT(leader, 0);
    t.stopNode(leader);
    ASSERT_TRUE(awaitSecurityLeader(t.cluster(), 60s)) << "a new __security leader";
    const int survivor = leader % 3 + 1;
    auto disable = [](AuthWireClient& w, const std::string& ch, int64_t sid) {
        return w.alterPassword(ch, sid, "admin", pc::UserChangeKind::Disable);
    };
    const auto refused = t.ddl(survivor, disable);
    EXPECT_EQ(refused.errorCode, static_cast<uint16_t>(pc::ErrorCode::InvalidRequest)) << refused.detail;
    EXPECT_NE(refused.detail.find("last enabled security admin"), std::string::npos) << refused.detail;
    // ws1's own password change on the NEW leader (a follower answers Redirect before any check): unsigned it is
    // refused, signed it applies, and both surviving brokers converge on it.
    const int newLeader = settledSecurityLeader(t.cluster());
    ASSERT_GT(newLeader, 0);
    AuthTrio::Session s = t.login(newLeader, "ws1");
    s.w->signOwnCredentialChanges(false);
    const auto unsignedChange = s.w->alterPassword(t.channel(newLeader), s.sid, "ws1", pc::UserChangeKind::AddPassword, "ws1-extra-pw-123");
    EXPECT_EQ(unsignedChange.errorCode, 45u) << unsignedChange.detail;
    s.w->signOwnCredentialChanges(true);
    const auto signedChange = s.w->alterPassword(t.channel(newLeader), s.sid, "ws1", pc::UserChangeKind::AddPassword, "ws1-extra-pw-123");
    ASSERT_EQ(signedChange.errorCode, 0u) << signedChange.detail;
    EXPECT_TRUE(t.awaitDigestsAgree(t.node(newLeader).securityViewForTest().appliedOffset, 30s)) << "both survivors applied it";
    for (int id : {1, 2, 3})
        if (t.cluster().running(id)) EXPECT_NO_THROW((void)t.login(id, "ws1", "ws1-extra-pw-123")) << "node " << id;
    EXPECT_TRUE(t.cluster().running(survivor));
}

// PR #410 review, adversarial pass (#121 at the cluster layer): the last admin removed with FORCE reopens the window on
// EVERY broker; each one still asks for a login (MustAuthenticate) and the other users log in on each; the cluster-key
// holder re-bootstraps through any broker (a follower redirects), after which every broker closes the window again.
TEST(AuthClusterRegression, AForcedLastAdminRemovalKeepsEveryBrokerAskingForALoginAdversarialRegression) {
    AuthTrio t("reopen3");
    t.start();
    t.bootstrapAdmin();
    t.createUser(1, "ws1", AuthTrio::kPw);
    // Sent to the __security leader (a follower answers Redirect); the admin's own session is revoked by it, so the
    // outcome is read from the replicated state, not from an answer.
    const int leader = settledSecurityLeader(t.cluster());
    ASSERT_GT(leader, 0);
    {
        const AuthTrio::Session a = t.adminOn(leader);
        pc::SecurityDdl m; m.sessionId = a.sid; m.kind = pc::SecurityOpKind::AlterUser; m.name = "admin";
        m.change = pc::UserChangeKind::Disable; m.flags = pc::kSecurityDdlFlagForce;
        (void)a.w->securityDdl(t.channel(leader), m, "ALTER USER admin DISABLE FORCE;", std::chrono::seconds{3});
    }
    ASSERT_TRUE(AuthTrio::until(60s, [&] {
        for (int id : {1, 2, 3}) if (t.node(id).securityViewForTest().hasSecurityAdmin) return false;
        return true;
    })) << "every broker sees the window open";
    for (int id : {1, 2, 3}) {
        AuthWireClient& w = t.wire();
        ASSERT_GT(w.connect(t.channel(id)), 0) << id;
        EXPECT_EQ(w.lastConnect()->authState, pc::AuthState::MustAuthenticate) << "node " << id;
        EXPECT_NO_THROW((void)t.login(id, "ws1")) << "ws1 logs in on node " << id;
    }
    // Re-bootstrap through each broker until accepted (a follower answers Redirect): exactly one new admin.
    bool done = false;
    for (int round = 0; round < 100 && !done; ++round)
        for (int id : {1, 2, 3}) {
            AuthWireClient& w = t.wire();
            const int64_t s = w.connect(t.channel(id));
            if (s <= 0) continue;
            if (w.bootstrap(t.channel(id), s, t.keyContents(id), "rescue", "rescue-otp-123456").errorCode == 0) { done = true; break; }
        }
    ASSERT_TRUE(done) << "the cluster-key holder recovers the cluster";
    EXPECT_TRUE(AuthTrio::until(60s, [&] {
        for (int id : {1, 2, 3}) if (!t.node(id).securityViewForTest().hasSecurityAdmin) return false;
        return true;
    })) << "every broker closes the window again";
}

// ═════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
// Issues #15 / #111 (decision M7): a broker whose `__security` state is not provably current - restarted behind the
// cluster, partitioned from its leader, or a partitioned ex-leader (a zombie) - serves no login (the retriable
// AuthUnavailable, 48) and, stale longer than R, ends its authenticated sessions with a session-level 48.
// ═════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
namespace {
constexpr uint16_t kAuthUnavailable = 48;
/// The login answer of `user` on broker `id` (a fresh session on the trio wire); 0xFFFF = none / no connect.
uint16_t loginCode(AuthTrio& t, int id, const std::string& user, const std::string& pw = AuthTrio::kPw) {
    AuthWireClient& w = t.wire();
    const int64_t sid = w.connect(t.channel(id));
    if (sid <= 0) return 0xFFFF;
    return w.login(t.channel(id), sid, user, {pw, pw}).errorCode();
}
/// Shorter R for the tests (F stays the default 1 s: it must exceed the broker liveness + 500 ms).
void withStaleSessionEnd(AuthTrio& t, int64_t ms) {
    auto base = t.cluster().configTweak;
    t.cluster().configTweak = [base, ms](NodeConfig& cfg) { if (base) base(cfg); cfg.securityStaleSessionEndMs = ms; };
}
bool sessionEndedWith(AuthTrio::Session& s, int16_t code, std::chrono::milliseconds within) {
    auto& c = s.w->raw();
    bool seen = false;
    c.pollFor(within, [&] {
        for (const auto& e : c.errors()) if (e.sessionId == s.sid && e.correlationId == 0 && e.code == code) { seen = true; return true; }
        return false;
    });
    if (!seen)
        for (const auto& e : c.errors())
            std::fprintf(stderr, "DIAG error sid=%lld corr=%lld code=%d msg=%s (want sid=%lld)\n", (long long)e.sessionId,
                         (long long)e.correlationId, e.code, e.message.c_str(), (long long)s.sid);
    return seen;
}
}  // namespace

// Issue #15 (RED before the fix: node 3 restarted with its local `__security` prefix - ws1 still enabled there - and,
// its applier behind, accepted ws1's password although the cluster had disabled ws1 while it was down).
TEST(AuthRestartRegression, ARestartedBrokerNeverAcceptsADisabledUsersOldPasswordClusterRegression) {
    AuthTrio t("restartdisabled");
    t.start();
    t.bootstrapAdmin();
    t.createUser(1, "ws1");
    ASSERT_TRUE(AuthTrio::until(30s, [&] { return loginCode(t, 3, "ws1") == 0; })) << "ws1 logs in on node 3";
    t.stopNode(3);
    t.mustOk(t.ddl(1, [](AuthWireClient& w, const std::string& ch, int64_t sid) {
        return w.alterPassword(ch, sid, "ws1", pc::UserChangeKind::Disable, "");
    }), "ALTER USER ws1 DISABLE");
    // Node 3 comes back behind: its applier is held, so its local state (ws1 enabled) never advances on its own.
    t.cluster().beforeStart = [](int id, BrokerNode& n) { if (id == 3) n.holdSecurityApplyAtBootForTest(true); };
    t.restart(3, t.keyContentsPath(3));
    t.cluster().beforeStart = nullptr;
    int unavailable = 0;
    for (int i = 0; i < 10; ++i) {
        const uint16_t code = loginCode(t, 3, "ws1");
        ASSERT_NE(code, 0) << "attempt " << i << ": the disabled user's old password must never log in";
        unavailable += code == kAuthUnavailable ? 1 : 0;
        std::this_thread::sleep_for(100ms);
    }
    EXPECT_GT(unavailable, 0) << "a broker that cannot vouch for its state answers the retriable AuthUnavailable";
    EXPECT_EQ(t.node(3).securityServingForTest(), "CatchingUp");
    t.node(3).holdSecurityApplyForTest(false);
    ASSERT_TRUE(t.awaitDigestsAgree(2, 60s)) << "node 3 applied the DISABLE";
    EXPECT_TRUE(AuthTrio::until(30s, [&] { return loginCode(t, 3, "ws1") == 44; })) << "now judged: disabled (44)";
    EXPECT_EQ(t.node(3).securityServingForTest(), "Serving");
}

// Issue #111 (RED before the fix: a follower cut off its `__security` leader kept serving logins and its sessions
// forever from the frozen state).  Stale after F: logins 48; after R its authenticated sessions end with 48 (their
// clients move); the contact back plus a full catch-up -> Serving again.
TEST(AuthRestartRegression, APartitionedBrokerGoesStaleThenEndsSessionsRetriablyRegression) {
    AuthTrio t("partitionedfollower");
    withStaleSessionEnd(t, 3'000);
    t.start();
    t.bootstrapAdmin();
    t.createUser(1, "ws1");
    const int32_t leader = settledSecurityLeader(t.cluster());
    ASSERT_GT(leader, 0);
    const int cut = leader == 3 ? 2 : 3;   // a follower
    AuthTrio::Session live;
    ASSERT_TRUE(AuthTrio::until(30s, [&] { try { live = t.login(cut, "ws1"); return true; } catch (...) { return false; } }));
    BrokerNode& n = t.node(cut);
    n.onAdminThreadForTest([&n]() {   // its `__security` follower hears its leader no more (no fetch, no answer)
        n.securityReplicaForTest()->holdFetchesUntilForTest(INT64_MAX);
        return 0;
    });
    EXPECT_TRUE(AuthTrio::until(10s, [&] { return n.securityServingForTest() == "Stale"; }));
    EXPECT_EQ(loginCode(t, cut, "ws1"), kAuthUnavailable) << "a stale broker serves no login";
    EXPECT_TRUE(n.gatewayHasSessionForTest(live.sid)) << "within R the live session keeps working";
    EXPECT_TRUE(sessionEndedWith(live, static_cast<int16_t>(kAuthUnavailable), 15s))
        << "stale past R: the session is ended retriably (session-level 48)";
    EXPECT_FALSE(n.gatewayHasSessionForTest(live.sid));
    EXPECT_EQ(loginCode(t, leader, "ws1"), 0) << "a current broker serves";
    n.onAdminThreadForTest([&n]() { n.securityReplicaForTest()->holdFetchesUntilForTest(0); return 0; });
    EXPECT_TRUE(AuthTrio::until(30s, [&] { return loginCode(t, cut, "ws1") == 0; })) << "contact back + caught up: serving";
}

// Issue #111, the zombie leader (RED before the fix: the `__security` leader cut off the consensus kept "leading" -
// nothing tells it otherwise - and served logins and permissions from its frozen state while the majority moved on: a
// user DISABLEd there still logged in on it).  Stale F after its last consensus contact: logins 48, sessions ended
// after R; healed, it follows the new leader and judges the user disabled.
TEST(AuthRestartRegression, APartitionedSecurityLeaderStopsServingWithoutALeaseRegression) {
    AuthTrio t("zombieleader");
    withStaleSessionEnd(t, 3'000);
    t.start();
    t.bootstrapAdmin();
    t.createUser(1, "ws1");
    const int32_t zombie = settledSecurityLeader(t.cluster());
    ASSERT_GT(zombie, 0);
    const int other = zombie == 1 ? 2 : 1;
    AuthTrio::Session live;
    ASSERT_TRUE(AuthTrio::until(30s, [&] { try { live = t.login(zombie, "ws1"); return true; } catch (...) { return false; } }));
    BrokerNode& z = t.node(zombie);
    z.partitionFromPeersForTest(true);
    // Stale first (its admin plane is still up: a DDL reaching it before would still commit through its followers).
    ASSERT_TRUE(AuthTrio::until(10s, [&] { return z.securityServingForTest() == "Stale"; }));
    // The majority moves on: a new `__security` leader, then the DISABLE commits there.
    {   // a retry after a commit timeout may find it applied already (a fresh request id each attempt): either is fine
        const auto a = t.ddl(other, [](AuthWireClient& w, const std::string& ch, int64_t sid) {
            return w.alterPassword(ch, sid, "ws1", pc::UserChangeKind::Disable, "");
        }, 60s);
        ASSERT_TRUE(a.errorCode == 0 || a.detail.find("already disabled") != std::string::npos)
            << "ALTER USER ws1 DISABLE (majority): " << a.errorCode << " " << a.detail;
    }
    EXPECT_TRUE(AuthTrio::until(10s, [&] { return z.securityServingForTest() == "Stale"; }));
    for (int i = 0; i < 5; ++i)
        EXPECT_EQ(loginCode(t, zombie, "ws1"), kAuthUnavailable) << "attempt " << i << ": the zombie serves no login";
    EXPECT_TRUE(sessionEndedWith(live, static_cast<int16_t>(kAuthUnavailable), 15s)) << "its sessions end after R";
    z.partitionFromPeersForTest(false);
    EXPECT_TRUE(AuthTrio::until(60s, [&] { return loginCode(t, zombie, "ws1") == 44; }))
        << "healed: it follows the new leader and the user is disabled";
}

// ═════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
// Adversarial review of PR #459 (issues #15 / #111): try to make a broker serve a login from a `__security` state the
// cluster has moved past - at the moment a zombie leader heals, while it is cut off, right after a restart with no
// seam, and on the survivors of a `__security` leader crash (a normal failover must never make them refuse logins).
// ═════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
namespace {
struct ServingBounds { int64_t fMs = 0; int64_t livenessMs = 0; };
/// Captures F and the broker liveness timeout the brokers apply.
void captureBounds(AuthTrio& t, ServingBounds& b) {
    auto base = t.cluster().configTweak;
    t.cluster().configTweak = [base, &b](NodeConfig& cfg) {
        if (base) base(cfg);
        b.fMs = cfg.effectiveSecurityMaxStalenessMs();
        b.livenessMs = cfg.effectiveBrokerLivenessMs();
    };
}
int64_t msSince(std::chrono::steady_clock::time_point t0) {
    return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - t0).count();
}
}  // namespace

// The healed zombie: it believed it led `__security` through the whole partition, so its admin verdict never lapsed;
// the moment its consensus closes again it must not serve logins from the state it froze (ws1 still enabled there)
// before it has learnt it was replaced and applied the DISABLE it missed.
TEST(AuthRestartAdversarialRegression, AHealedZombieLeaderNeverServesTheDisabledUserAdversarialRegression) {
    AuthTrio t("advzombieheal");
    t.start();
    t.bootstrapAdmin();
    t.createUser(1, "ws1");
    const int32_t zombie = settledSecurityLeader(t.cluster());
    ASSERT_GT(zombie, 0);
    const int other = zombie == 1 ? 2 : 1;
    ASSERT_TRUE(AuthTrio::until(30s, [&] { return loginCode(t, zombie, "ws1") == 0; }));
    BrokerNode& z = t.node(zombie);
    z.partitionFromPeersForTest(true);
    {
        const auto a = t.ddl(other, [](AuthWireClient& w, const std::string& ch, int64_t sid) {
            return w.alterPassword(ch, sid, "ws1", pc::UserChangeKind::Disable, "");
        }, 60s);
        ASSERT_TRUE(a.errorCode == 0 || a.detail.find("already disabled") != std::string::npos)
            << "ALTER USER ws1 DISABLE (majority): " << a.errorCode << " " << a.detail;
    }
    ASSERT_TRUE(AuthTrio::until(15s, [&] { return z.securityServingForTest() == "Stale"; }));
    z.partitionFromPeersForTest(false);
    const auto healed = std::chrono::steady_clock::now();
    int accepted = 0, attempts = 0, judged = 0;
    std::string firstAccept;
    while (msSince(healed) < 60'000 && judged < 5) {
        const uint16_t code = loginCode(t, zombie, "ws1");
        ++attempts;
        if (code == 0) {
            if (accepted++ == 0) firstAccept = std::to_string(msSince(healed)) + " ms after the heal (state " +
                                               z.securityServingForTest() + ")";
        }
        judged = code == 44 ? judged + 1 : 0;
    }
    EXPECT_EQ(accepted, 0) << "the healed ex-leader accepted the DISABLEd user's password " << accepted << " of " << attempts
                           << " times, first " << firstAccept;
    EXPECT_EQ(judged, 5) << "it ends up judging ws1 disabled";
}

// The zombie while cut off: what it serves is never more than about F behind the cluster.  No login of the DISABLEd
// user may succeed on it later than F after the majority committed the DISABLE, nor later than F + the liveness timeout
// after it lost its peers.
TEST(AuthRestartAdversarialRegression, AZombieLeaderStopsServingWithinTheStalenessBoundAdversarialRegression) {
    AuthTrio t("advzombiebound");
    ServingBounds b;
    captureBounds(t, b);
    t.start();
    t.bootstrapAdmin();
    t.createUser(1, "ws1");
    const int32_t zombie = settledSecurityLeader(t.cluster());
    ASSERT_GT(zombie, 0);
    const int other = zombie == 1 ? 2 : 1;
    ASSERT_TRUE(AuthTrio::until(30s, [&] { return loginCode(t, zombie, "ws1") == 0; }));
    BrokerNode& z = t.node(zombie);
    const auto cut = std::chrono::steady_clock::now();
    z.partitionFromPeersForTest(true);
    // Probe the zombie from another thread on its own wire client while the majority DISABLEs ws1.
    std::atomic<bool> stop{false};
    std::atomic<int64_t> lastAcceptMs{-1};
    std::thread probe([&] {
        AuthWireClient w(t.cluster().clientsDir->path / "adv_probe_wire");
        while (!stop.load()) {
            const int64_t sid = w.connect(t.channel(zombie));
            if (sid > 0 && w.login(t.channel(zombie), sid, "ws1", {AuthTrio::kPw, AuthTrio::kPw}).errorCode() == 0)
                lastAcceptMs.store(msSince(cut));
        }
    });
    int64_t disabledMs = -1;
    {
        const auto a = t.ddl(other, [](AuthWireClient& w, const std::string& ch, int64_t sid) {
            return w.alterPassword(ch, sid, "ws1", pc::UserChangeKind::Disable, "");
        }, 60s);
        disabledMs = msSince(cut);
        ASSERT_TRUE(a.errorCode == 0 || a.detail.find("already disabled") != std::string::npos);
    }
    EXPECT_TRUE(AuthTrio::until(20s, [&] { return z.securityServingForTest() == "Stale"; }));
    std::this_thread::sleep_for(500ms);   // observation window: a state that must not change
    stop.store(true);
    probe.join();
    const int64_t last = lastAcceptMs.load();
    std::fprintf(stderr, "ADV zombie bound: F=%lld ms liveness=%lld ms, DISABLE committed at %lld ms, last accepted login at %lld ms\n",
                 (long long)b.fMs, (long long)b.livenessMs, (long long)disabledMs, (long long)last);
    EXPECT_LE(last, disabledMs + b.fMs) << "a login of the DISABLEd user more than F after the cluster committed it";
    EXPECT_LE(last, b.fMs + b.livenessMs + 1'000) << "the zombie served longer than F + liveness after it lost its peers";
}

// A normal failover: the `__security` leader crashes; the survivors must keep serving logins throughout (F exceeds the
// liveness timeout so that this never makes a healthy broker stale - SECURITY.md, NodeConfig).
TEST(AuthRestartAdversarialRegression, ASecurityLeaderCrashNeverMakesTheSurvivorsStaleAdversarialRegression) {
    AuthTrio t("advleadercrash");
    ServingBounds b;
    captureBounds(t, b);
    t.start();
    t.bootstrapAdmin();
    t.createUser(1, "ws1");
    const int32_t leader = settledSecurityLeader(t.cluster());
    ASSERT_GT(leader, 0);
    std::vector<int> survivors;
    for (int id : {1, 2, 3}) if (id != leader) survivors.push_back(id);
    for (int id : survivors) ASSERT_TRUE(AuthTrio::until(30s, [&] { return t.node(id).securityServingForTest() == "Serving"; }));
    t.node(leader).crashForTest();
    t.stopNode(leader);
    const auto crashed = std::chrono::steady_clock::now();
    std::map<int, std::string> lapses;
    bool newLeader = false;
    while (msSince(crashed) < 20'000) {
        for (int id : survivors) {
            const std::string s = t.node(id).securityServingForTest();
            if (s != "Serving" && lapses[id].empty()) lapses[id] = s + " at " + std::to_string(msSince(crashed)) + " ms";
        }
        if (!newLeader) { const int32_t l = settledSecurityLeader(t.cluster()); newLeader = l > 0 && l != leader; }
        std::this_thread::sleep_for(5ms);
    }
    EXPECT_TRUE(newLeader) << "a new __security leader";
    for (int id : survivors)
        EXPECT_TRUE(lapses[id].empty()) << "survivor " << id << " stopped serving logins during a normal failover: " << lapses[id]
                                        << " (F=" << b.fMs << " ms, liveness=" << b.livenessMs << " ms)";
}

// A restart with no seam at all: node 3 comes back after ws1 was DISABLEd while it was down, and is hammered with ws1's
// old password from the first moment its client endpoint answers.  Not one login may succeed.
TEST(AuthRestartAdversarialRegression, ARestartedBrokerHammeredFromBootNeverAcceptsADisabledUserAdversarialRegression) {
    AuthTrio t("advrestarthammer");
    t.start();
    t.bootstrapAdmin();
    t.createUser(1, "ws1");
    ASSERT_TRUE(AuthTrio::until(30s, [&] { return loginCode(t, 3, "ws1") == 0; }));
    t.stopNode(3);
    t.mustOk(t.ddl(1, [](AuthWireClient& w, const std::string& ch, int64_t sid) {
        return w.alterPassword(ch, sid, "ws1", pc::UserChangeKind::Disable, "");
    }), "ALTER USER ws1 DISABLE");
    std::atomic<bool> stop{false};
    std::atomic<int> accepted{0}, answered{0}, unavailable{0};
    std::thread probe;
    t.cluster().beforeStart = [&](int id, BrokerNode&) {
        if (id != 3) return;
        probe = std::thread([&] {   // starts BEFORE node 3's start(): probes from its very first answer
            AuthWireClient w(t.cluster().clientsDir->path / "adv_probe_wire");
            while (!stop.load()) {
                const int64_t sid = w.connect(t.channel(3));
                if (sid <= 0) continue;
                const uint16_t code = w.login(t.channel(3), sid, "ws1", {AuthTrio::kPw, AuthTrio::kPw}).errorCode();
                if (code == 0xFFFF) continue;
                ++answered;
                if (code == 0) ++accepted;
                if (code == 48) ++unavailable;
            }
        });
    };
    t.restart(3, t.keyContentsPath(3));
    t.cluster().beforeStart = nullptr;
    EXPECT_TRUE(AuthTrio::until(60s, [&] { return t.node(3).securityServingForTest() == "Serving"; }));
    std::this_thread::sleep_for(1s);
    stop.store(true);
    probe.join();
    std::fprintf(stderr, "ADV restart hammer: %d answered, %d AuthUnavailable, %d accepted\n", answered.load(),
                 unavailable.load(), accepted.load());
    EXPECT_GT(answered.load(), 0);
    EXPECT_EQ(accepted.load(), 0) << "the restarted broker accepted the DISABLEd user's old password";
}

// The healed zombie again, with its applier slow (held from just before the heal: a long partition leaves it many
// events to fetch and apply).  Demoted, it must re-earn the verdict as a follower - caught up with its new leader and
// every committed record applied - before it serves a login; its leader-era "contact" says nothing about the cluster.
TEST(AuthRestartAdversarialRegression, AHealedZombieWithASlowApplierNeverServesTheDisabledUserAdversarialRegression) {
    AuthTrio t("advzombieslow");
    ServingBounds b;
    captureBounds(t, b);
    t.start();
    t.bootstrapAdmin();
    t.createUser(1, "ws1");
    const int32_t zombie = settledSecurityLeader(t.cluster());
    ASSERT_GT(zombie, 0);
    const int other = zombie == 1 ? 2 : 1;
    ASSERT_TRUE(AuthTrio::until(30s, [&] { return loginCode(t, zombie, "ws1") == 0; }));
    BrokerNode& z = t.node(zombie);
    z.partitionFromPeersForTest(true);
    {
        const auto a = t.ddl(other, [](AuthWireClient& w, const std::string& ch, int64_t sid) {
            return w.alterPassword(ch, sid, "ws1", pc::UserChangeKind::Disable, "");
        }, 60s);
        ASSERT_TRUE(a.errorCode == 0 || a.detail.find("already disabled") != std::string::npos)
            << "ALTER USER ws1 DISABLE (majority): " << a.errorCode << " " << a.detail;
    }
    ASSERT_TRUE(AuthTrio::until(15s, [&] { return z.securityServingForTest() == "Stale"; }));
    z.holdSecurityApplyForTest(true);
    z.partitionFromPeersForTest(false);
    const auto healed = std::chrono::steady_clock::now();
    int accepted = 0, attempts = 0;
    std::string firstAccept;
    while (msSince(healed) < 3 * b.fMs) {
        const uint16_t code = loginCode(t, zombie, "ws1");
        ++attempts;
        if (code == 0 && accepted++ == 0)
            firstAccept = std::to_string(msSince(healed)) + " ms after the heal (state " + z.securityServingForTest() + ")";
    }
    EXPECT_EQ(accepted, 0) << "the healed ex-leader, its DISABLE not applied yet, accepted the DISABLEd user's password "
                           << accepted << " of " << attempts << " times, first " << firstAccept << " (F=" << b.fMs << " ms)";
    z.holdSecurityApplyForTest(false);
    EXPECT_TRUE(AuthTrio::until(60s, [&] { return loginCode(t, zombie, "ws1") == 44; })) << "then judged disabled";
}

// ═════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
// Issues #273 / #136 (follower-not-applied 45; `__security` follower lag).  Node F, a `__security` FOLLOWER, holds its
// applier (it replicates the events, its view applies none of them), then the admin REVOKEs a user's WRITE through the
// `__security` leader - answered OK once the LEADER enforces it (D24).  The user has a live session on F, producing to
// the partition F itself leads (gateway-routed and straight to F's worker ingress).
// ═════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
namespace {
struct LaggingFollower {
    int f = 0;
    int32_t secLeader = 0, part = -1;
    LedTopic led;
    UserSessions u;
    PartitionInfo ingress;
    int64_t appliedBefore = -1;
};
/// The trio, the user "lagw" (WRITE on "lag."), a live session of it on a `__security` follower F producing to the
/// partition F leads (both paths checked OK), then F's applier held and the REVOKE answered OK by the leader.
void lagFollowerAndRevoke(AuthTrio& t, LaggingFollower& s) {
    t.start();
    t.bootstrapAdmin();
    createBuilder(t);
    t.createUser(1, "lagw");
    t.grant(1, "lagw", SO::Write, ST::Topic, SP::Prefix, "lag.");
    const std::vector<AuthTrio::Session> bs = sessionsOnEveryBroker(t, "builder");   // bs[0]: node 1
    ASSERT_EQ(bs.size(), 3u) << "a builder session on every broker";
    s.led = defineLedTopic(t, bs, "lag.t");
    ASSERT_GE(s.led.id, 0);
    ASSERT_TRUE(s.led.part[0] >= 0 && s.led.part[1] >= 0 && s.led.part[2] >= 0) << "each broker must lead one partition";
    s.secLeader = settledSecurityLeader(t.cluster());
    ASSERT_GT(s.secLeader, 0);
    s.f = s.secLeader == 3 ? 2 : 3;   // a `__security` follower
    s.part = s.led.part[static_cast<std::size_t>(s.f - 1)];
    ASSERT_TRUE(t.awaitDigestsAgree(0, 30s));
    s.u.name = "lagw";
    s.u.w = &t.wire();
    ASSERT_TRUE(AuthTrio::until(30s, [&] {
        try { s.u.sid[static_cast<std::size_t>(s.f - 1)] = t.login(s.f, "lagw").sid; return true; } catch (...) { return false; }
    }));
    ASSERT_TRUE(AuthTrio::until(30s, [&] {
        for (const PartitionInfo& pi : bs[0].w->raw().metadata(t.channel(bs[0].node), AuthWireClient::kClientStream, bs[0].sid))
            if (pi.topicId == s.led.id && pi.partition == s.part && pi.leaderId == s.f && !pi.ingressEndpoint.empty()) { s.ingress = pi; return true; }
        return false;
    })) << "the worker ingress of the partition node " << s.f << " leads";
    ASSERT_TRUE(AuthTrio::until(30s, [&] { return gatewayProduce(t, s.u, s.f, s.led.id, s.part) == 0; })) << "the grant works on F (routed)";
    ASSERT_TRUE(AuthTrio::until(30s, [&] { return shardProduce(s.u, s.f, s.ingress, s.led.id) == 0; })) << "the grant works on F (worker)";
    s.appliedBefore = t.node(s.f).securityViewForTest().appliedOffset;
    t.node(s.f).holdSecurityApplyForTest(true);
    const auto ok = t.revoke(s.secLeader, "lagw", SO::Write, ST::Topic, SP::Prefix, "lag.");
    ASSERT_EQ(ok.errorCode, 0) << ok.detail;
    ASSERT_EQ(t.node(s.f).securityViewForTest().appliedOffset, s.appliedBefore) << "precondition: F has not applied the revoke";
}
}  // namespace

// The session-level bound (issues #15 / #111, decision M7: staleness bounded by F, then R), unchanged by #603: F's
// applier stuck behind the REVOKE drops its "caught up" verdict F after it last applied at the high watermark, and R
// later F ends the user's live session with the retriable session-level AuthUnavailable (48); from then on nothing the
// user sends is appended on F, a new login there is refused 48 (never OK), and once F applies the revoke it answers 45.
// (Since #603 every request of that session was already refused 48 from the moment F knew the revoke committed - see
// AFollowerThatHasNotAppliedARevokeNeverServesTheOldGrantRegression; this test pins the session end on top of it.)
TEST(AuthClusterRegression, AFollowerStuckBehindARevokeEndsTheSessionWithinTheStalenessBoundRegression) {
    AuthTrio t("lagbound");
    withStaleSessionEnd(t, 3'000);
    LaggingFollower s;
    lagFollowerAndRevoke(t, s);
    if (HasFatalFailure()) return;
    AuthTrio::Session live{s.u.w, s.u.sid[static_cast<std::size_t>(s.f - 1)], s.f};
    const auto revokedAt = std::chrono::steady_clock::now();
    EXPECT_TRUE(sessionEndedWith(live, static_cast<int16_t>(kAuthUnavailable), 30s))
        << "F, stuck behind the revoke, never ended the user's session (session-level 48)";
    std::printf("LAG-BOUND: F=%d ended the lagging session %lld ms after the revoke's OK\n", s.f,
                static_cast<long long>(std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - revokedAt).count()));
    EXPECT_FALSE(t.node(s.f).gatewayHasSessionForTest(live.sid));
    const int64_t leo = partitionLeo(&t.node(s.f), s.led.id, s.part);
    EXPECT_NE(gatewayProduce(t, s.u, s.f, s.led.id, s.part), 0) << "the ended session's produce (routed)";
    EXPECT_NE(shardProduce(s.u, s.f, s.ingress, s.led.id), 0) << "the ended session's produce (worker ingress)";
    EXPECT_EQ(partitionLeo(&t.node(s.f), s.led.id, s.part), leo) << "F appended for the ended session";
    EXPECT_EQ(loginCode(t, s.f, "lagw"), kAuthUnavailable) << "a lagging F serves no login";
    EXPECT_EQ(t.node(s.f).securityViewForTest().appliedOffset, s.appliedBefore) << "the hold held for the whole window";

    t.node(s.f).holdSecurityApplyForTest(false);
    ASSERT_TRUE(AuthTrio::until(30s, [&] { return t.node(s.f).securityViewForTest().appliedOffset > s.appliedBefore; }));
    ASSERT_TRUE(AuthTrio::until(30s, [&] {
        try { s.u.sid[static_cast<std::size_t>(s.f - 1)] = t.login(s.f, "lagw").sid; return true; } catch (...) { return false; }
    })) << "F, caught up, serves the login again";
    EXPECT_EQ(gatewayProduce(t, s.u, s.f, s.led.id, s.part), 45) << "and refuses the revoked WRITE NotAuthorized";
    EXPECT_EQ(partitionLeo(&t.node(s.f), s.led.id, s.part), leo);
}

// Issue #603 (found by batch M24; RED before the fix: F kept serving the revoked WRITE - OK, appended - on both the
// routed and the worker-ingress path for up to F + R).  F must never serve a grant it holds a committed revoke of:
// from the moment F knows the REVOKE is committed (its leader's HW covers it) every answer is the retriable
// AuthUnavailable (48) and nothing is appended, until F applied it; then the revoke itself answers (45).  The window
// before F learns the HW (one leader->follower message) is the F bound of SecurityServing.h, waited out here by
// condition (F reports itself behind) - never by time.
TEST(AuthClusterRegression, AFollowerThatHasNotAppliedARevokeNeverServesTheOldGrantRegression) {
    AuthTrio t("lagrevoke");
    LaggingFollower s;
    lagFollowerAndRevoke(t, s);
    if (HasFatalFailure()) return;
    ASSERT_TRUE(AuthTrio::until(30s, [&] { return t.node(s.f).securityBehindForTest().gateway; }))
        << "F learns that the revoke it holds is committed";
    const int64_t leo = partitionLeo(&t.node(s.f), s.led.id, s.part);
    for (int i = 0; i < 5; ++i) {
        const int routed = gatewayProduce(t, s.u, s.f, s.led.id, s.part);
        EXPECT_TRUE(routed == 45 || routed == 48) << "attempt " << i << ": F served the revoked grant (routed), answer " << routed;
        const int direct = shardProduce(s.u, s.f, s.ingress, s.led.id);
        EXPECT_TRUE(direct == 45 || direct == 48 || direct == kNoAnswer)
            << "attempt " << i << ": F served the revoked grant (worker ingress), answer " << direct;
    }
    EXPECT_EQ(partitionLeo(&t.node(s.f), s.led.id, s.part), leo) << "F appended under the revoked grant";
    EXPECT_EQ(t.node(s.f).securityViewForTest().appliedOffset, s.appliedBefore) << "the hold held for the whole window";
    t.node(s.f).holdSecurityApplyForTest(false);
    ASSERT_TRUE(AuthTrio::until(30s, [&] { return !t.node(s.f).securityBehindForTest().gateway; })) << "F applied it";
    EXPECT_EQ(gatewayProduce(t, s.u, s.f, s.led.id, s.part), 45) << "then the revoke answers (routed)";
    EXPECT_EQ(shardProduce(s.u, s.f, s.ingress, s.led.id), 45) << "then the revoke answers (worker ingress)";
    EXPECT_EQ(partitionLeo(&t.node(s.f), s.led.id, s.part), leo);
}

// Issue #603, the interaction matrix's cluster slice: node {a `__security` FOLLOWER, the LEADER} x change {REVOKE ROLE,
// GRANT ROLE} (the role carries WRITE and READ on "lag.") x operation {routed produce, worker-ingress produce,
// subscribe} x apply {held, released}, on the partition that node leads.  Held: every operation answers 48 and nothing
// is appended on that node.  Released: the change decides (REVOKE 45, GRANT 0).  The leader-held cells' DDL OK waits for
// the leader's own enforcement (D24), so it is sent from a background thread on the trio wire while the user works on
// a wire of its own.
TEST(AuthClusterRegression, TheBehindRuleAcrossRolesChangesAndOperationsMatrixRegression) {
    AuthTrio t("lagmatrix");
    t.start();
    t.bootstrapAdmin();
    createBuilder(t);
    const std::vector<AuthTrio::Session> bs = sessionsOnEveryBroker(t, "builder");
    ASSERT_EQ(bs.size(), 3u);
    const LedTopic led = defineLedTopic(t, bs, "lag.m");
    ASSERT_GE(led.id, 0);
    ASSERT_TRUE(led.part[0] >= 0 && led.part[1] >= 0 && led.part[2] >= 0) << "each broker must lead one partition";
    const int32_t secLeader = settledSecurityLeader(t.cluster());
    ASSERT_GT(secLeader, 0);
    const int follower = secLeader == 3 ? 2 : 3;
    t.mustOk(t.ddl(1, [](AuthWireClient& w, const std::string& ch, int64_t sid) { return w.createRole(ch, sid, "lagrole"); }), "CREATE ROLE");
    t.grant(1, "lagrole", SO::Write, ST::Topic, SP::Prefix, "lag.", /*role=*/true);
    t.grant(1, "lagrole", SO::Read, ST::Topic, SP::Prefix, "lag.", /*role=*/true);
    AuthWireClient uw(t.cluster().clientsDir->path / "matrix_user_wire");
    int cell = 0;
    for (const int node : {follower, secLeader}) {
        for (const bool revoke : {true, false}) {
            const std::string user = "mx" + std::to_string(cell++);
            SCOPED_TRACE(user + (node == secLeader ? " on the leader " : " on a follower ") + std::to_string(node) +
                         (revoke ? ", REVOKE ROLE" : ", GRANT ROLE"));
            t.createUser(1, user);
            if (revoke)
                t.mustOk(t.ddl(1, [&](AuthWireClient& w, const std::string& ch, int64_t sid) { return w.grantRole(ch, sid, user, "lagrole"); }), "GRANT ROLE");
            ASSERT_TRUE(t.awaitDigestsAgree(0, 30s));
            UserSessions u;
            u.name = user;
            u.w = &uw;
            ASSERT_TRUE(AuthTrio::until(30s, [&] {
                try { u.sid[static_cast<std::size_t>(node - 1)] = t.loginOn(uw, node, user).sid; return true; } catch (...) { return false; }
            }));
            const int32_t part = led.part[static_cast<std::size_t>(node - 1)];
            PartitionInfo ingress;
            ASSERT_TRUE(AuthTrio::until(30s, [&] {
                for (const PartitionInfo& pi : bs[0].w->raw().metadata(t.channel(bs[0].node), AuthWireClient::kClientStream, bs[0].sid))
                    if (pi.topicId == led.id && pi.partition == part && pi.leaderId == node && !pi.ingressEndpoint.empty()) { ingress = pi; return true; }
                return false;
            }));
            const int before = revoke ? 0 : 45;
            auto subscribeCode = [&] {
                auto& c = uw.raw();
                const int64_t corr = c.sendSubscribe(t.channel(node), AuthWireClient::kClientStream,
                                                     u.sid[static_cast<std::size_t>(node - 1)], led.id, part);
                const auto code = c.awaitSubscribeCode(corr, Clock3::now() + 10s);
                return code ? static_cast<int>(*code) : kNoAnswer;
            };
            ASSERT_TRUE(AuthTrio::until(30s, [&] { return gatewayProduce(t, u, node, led.id, part) == before; })) << "routed before";
            ASSERT_TRUE(AuthTrio::until(30s, [&] { return shardProduce(u, node, ingress, led.id) == before; })) << "worker before";
            ASSERT_EQ(subscribeCode(), before) << "subscribe before";

            const int64_t applied = t.node(node).securityViewForTest().appliedOffset;
            t.node(node).holdSecurityApplyForTest(true);
            auto ddl = std::async(std::launch::async, [&]() -> int {
                return t.ddl(secLeader, [&](AuthWireClient& w, const std::string& ch, int64_t sid) {
                    return revoke ? w.revokeRole(ch, sid, user, "lagrole") : w.grantRole(ch, sid, user, "lagrole");
                }, 60s).errorCode;
            });
            ASSERT_TRUE(AuthTrio::until(30s, [&] { return t.node(node).securityBehindForTest().gateway; }))
                << "node " << node << " knows the change is committed";
            const int64_t leo = partitionLeo(&t.node(node), led.id, part);
            for (int i = 0; i < 3; ++i) {
                EXPECT_EQ(gatewayProduce(t, u, node, led.id, part), kAuthUnavailable) << "routed, held, attempt " << i;
                EXPECT_EQ(shardProduce(u, node, ingress, led.id), kAuthUnavailable) << "worker, held, attempt " << i;
                EXPECT_EQ(subscribeCode(), kAuthUnavailable) << "subscribe, held, attempt " << i;
            }
            EXPECT_EQ(partitionLeo(&t.node(node), led.id, part), leo) << "nothing appended while behind";
            EXPECT_EQ(t.node(node).securityViewForTest().appliedOffset, applied) << "the hold held";

            t.node(node).holdSecurityApplyForTest(false);
            ASSERT_EQ(ddl.wait_for(90s), std::future_status::ready);
            EXPECT_EQ(ddl.get(), 0);
            ASSERT_TRUE(AuthTrio::until(30s, [&] { return !t.node(node).securityBehindForTest().gateway; }));
            ASSERT_TRUE(AuthTrio::until(30s, [&] { return !t.node(node).securityBehindForTest().workers; }));
            const int after = revoke ? 45 : 0;
            EXPECT_EQ(gatewayProduce(t, u, node, led.id, part), after) << "routed after";
            EXPECT_EQ(shardProduce(u, node, ingress, led.id), after) << "worker after";
            EXPECT_EQ(subscribeCode(), after) << "subscribe after";
            if (revoke) EXPECT_EQ(partitionLeo(&t.node(node), led.id, part), leo) << "the revoked grant never appended";
        }
    }
}

// ═════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
// Adversarial review of PR #605 (issue #603, the behind rule): try to make a broker serve a grant around a committed
// `__security` event it holds - across a leader crash, a restart, a long hold under live subscriptions - and try to make
// the rule refuse an authorized client in ordinary operation (liveness).
// ═════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
namespace {
/// The partition of `led` that broker `node` leads now (from node 1's metadata as the builder), or nullopt.
std::optional<PartitionInfo> ledBy(AuthTrio& t, const std::vector<AuthTrio::Session>& bs, int32_t topic, int node) {
    for (const auto& s : bs) {
        if (s.node == node) continue;   // ask a broker that is not the one being restarted / crashed
        try {
            for (const PartitionInfo& pi : s.w->raw().metadata(t.channel(s.node), AuthWireClient::kClientStream, s.sid))
                if (pi.topicId == topic && pi.leaderId == node && !pi.ingressEndpoint.empty()) return pi;
        } catch (...) {}
    }
    return std::nullopt;
}
}  // namespace

// Liveness: ordinary security DDL churn (a user and a grant created per round, answered OK by the leader once IT
// enforced it) while an authorized user produces on all three brokers between rounds - routed and straight to the
// worker ingress.  The behind rule must not turn ordinary replication into refusals: the leader never flips; a
// follower flips only for the replication lag of an event, so every produce is served (any 48 is retried and served
// within a bound - the producer retries 48 since this review) and the totals are reported.
TEST(AuthClusterRegression, BehindAdversarialSecurityDdlChurnNeverStarvesAnAuthorizedProducerRegression) {
    AuthTrio t("lagchurn");
    t.start();
    t.bootstrapAdmin();
    createBuilder(t);
    t.createUser(1, "churnw");
    t.grant(1, "churnw", SO::Write, ST::Topic, SP::Prefix, "lag.");
    const std::vector<AuthTrio::Session> bs = sessionsOnEveryBroker(t, "builder");
    ASSERT_EQ(bs.size(), 3u);
    const LedTopic led = defineLedTopic(t, bs, "lag.c");
    ASSERT_GE(led.id, 0);
    ASSERT_TRUE(led.part[0] >= 0 && led.part[1] >= 0 && led.part[2] >= 0);
    const int32_t secLeader = settledSecurityLeader(t.cluster());
    ASSERT_GT(secLeader, 0);
    ASSERT_TRUE(t.awaitDigestsAgree(0, 30s));
    UserSessions u;
    u.name = "churnw";
    u.w = &t.wire();
    std::array<PartitionInfo, 3> ingress{};
    for (int node : {1, 2, 3}) {
        ASSERT_TRUE(AuthTrio::until(30s, [&] {
            try { u.sid[static_cast<std::size_t>(node - 1)] = t.login(node, "churnw").sid; return true; } catch (...) { return false; }
        }));
        ASSERT_TRUE(AuthTrio::until(30s, [&] {
            const auto pi = ledBy(t, bs, led.id, node);
            if (pi && pi->partition == led.part[static_cast<std::size_t>(node - 1)]) { ingress[static_cast<std::size_t>(node - 1)] = *pi; return true; }
            return false;
        }));
        const int32_t part = led.part[static_cast<std::size_t>(node - 1)];
        ASSERT_TRUE(AuthTrio::until(30s, [&] { return gatewayProduce(t, u, node, led.id, part) == 0; }));
        ASSERT_TRUE(AuthTrio::until(30s, [&] { return shardProduce(u, node, ingress[static_cast<std::size_t>(node - 1)], led.id) == 0; }));
    }
    std::array<uint64_t, 3> edges0{};
    for (int node : {1, 2, 3}) edges0[static_cast<std::size_t>(node - 1)] = t.node(node).adminSecurityBehindForTest().edges;
    int answered48 = 0, other = 0;
    constexpr int kRounds = 20;
    for (int round = 0; round < kRounds; ++round) {
        const std::string user = "churn" + std::to_string(round);
        t.createUser(secLeader, user);
        t.grant(secLeader, user, SO::Read, ST::Topic, SP::Prefix, "lag.");
        for (int node : {1, 2, 3}) {
            const int32_t part = led.part[static_cast<std::size_t>(node - 1)];
            for (const bool viaWorker : {false, true}) {
                int code = viaWorker ? shardProduce(u, node, ingress[static_cast<std::size_t>(node - 1)], led.id)
                                     : gatewayProduce(t, u, node, led.id, part);
                for (int retry = 0; code == kAuthUnavailable && retry < 200; ++retry) {   // the client's retry, bounded
                    ++answered48;
                    code = viaWorker ? shardProduce(u, node, ingress[static_cast<std::size_t>(node - 1)], led.id)
                                     : gatewayProduce(t, u, node, led.id, part);
                }
                if (code != 0) ++other;
                EXPECT_EQ(code, 0) << "round " << round << " node " << node << (viaWorker ? " worker" : " routed")
                                   << ": an authorized produce was not served";
            }
        }
    }
    std::string report;
    for (int node : {1, 2, 3}) {
        const auto e = t.node(node).adminSecurityBehindForTest().edges - edges0[static_cast<std::size_t>(node - 1)];
        report += " node " + std::to_string(node) + (node == secLeader ? " (leader)" : "") + ": " + std::to_string(e) + " edges";
        if (node == secLeader) EXPECT_EQ(e, 0u) << "the leader applies every event in the pass it commits";
    }
    std::cout << "[churn] " << kRounds << " rounds x 2 DDL; 48 answers seen (retried): " << answered48 << ";" << report << "\n";
    EXPECT_EQ(other, 0);
}

// Leader change: a follower F stuck behind a committed REVOKE while the `__security` leader CRASHES.  The new leader's
// HW restarts from its own view and F's follower HW reads 0 until it answers - the highest HW F has seen is kept, so
// F stays behind (48, nothing appended) through the failover, and once released it applies the REVOKE and CLEARS
// (never stuck behind a HW from the dead leadership): then the revoke decides (45).
TEST(AuthClusterRegression, BehindAdversarialSecurityLeaderCrashWhileAFollowerIsBehindRegression) {
    AuthTrio t("lagcrash");
    LaggingFollower s;
    lagFollowerAndRevoke(t, s);
    if (HasFatalFailure()) return;
    ASSERT_TRUE(AuthTrio::until(30s, [&] { return t.node(s.f).securityBehindForTest().gateway; }));
    const int64_t leo = partitionLeo(&t.node(s.f), s.led.id, s.part);
    t.node(s.secLeader).crashForTest();
    t.stopNode(s.secLeader);
    ASSERT_TRUE(AuthTrio::until(60s, [&] { const int32_t l = settledSecurityLeader(t.cluster()); return l > 0 && l != s.secLeader; }))
        << "a new __security leader";
    const int32_t newLeader = settledSecurityLeader(t.cluster());
    SCOPED_TRACE("new __security leader " + std::to_string(newLeader) + (newLeader == s.f ? " (F itself)" : ""));
    EXPECT_TRUE(t.node(s.f).securityBehindForTest().gateway) << "F still holds the unapplied REVOKE";
    for (int i = 0; i < 3; ++i) {
        const int routed = gatewayProduce(t, s.u, s.f, s.led.id, s.part);
        EXPECT_TRUE(routed == kAuthUnavailable || routed == 45) << "routed, after the failover, answer " << routed;
        const int direct = shardProduce(s.u, s.f, s.ingress, s.led.id);
        EXPECT_TRUE(direct == kAuthUnavailable || direct == 45) << "worker, after the failover, answer " << direct;
    }
    EXPECT_EQ(partitionLeo(&t.node(s.f), s.led.id, s.part), leo) << "F appended under the revoked grant";
    t.node(s.f).holdSecurityApplyForTest(false);
    ASSERT_TRUE(AuthTrio::until(60s, [&] { return !t.node(s.f).securityBehindForTest().gateway; }))
        << "F cleared after the failover (admin behind " << t.node(s.f).adminSecurityBehindForTest().behind << ")";
    ASSERT_TRUE(AuthTrio::until(30s, [&] { return !t.node(s.f).securityBehindForTest().workers; }));
    EXPECT_EQ(gatewayProduce(t, s.u, s.f, s.led.id, s.part), 45) << "then the revoke answers (routed)";
    EXPECT_EQ(shardProduce(s.u, s.f, s.ingress, s.led.id), 45) << "then the revoke answers (worker ingress)";
    EXPECT_EQ(partitionLeo(&t.node(s.f), s.led.id, s.part), leo);
}

// Restart: F, stuck behind a committed REVOKE, restarts over the same dir (its log holds the REVOKE, its view applied
// none of it).  From its first login on, nothing anywhere serves the revoked WRITE: every produce of the user, on every
// broker, routed and to the worker ingress, answers 45 or 48 - never OK - and nothing is appended.
TEST(AuthClusterRegression, BehindAdversarialAFollowerRestartedBehindARevokeNeverServesTheOldGrantRegression) {
    AuthTrio t("lagrestart");
    LaggingFollower s;
    lagFollowerAndRevoke(t, s);
    if (HasFatalFailure()) return;
    ASSERT_TRUE(AuthTrio::until(30s, [&] { return t.node(s.f).securityBehindForTest().gateway; }));
    std::array<int64_t, 3> leo{};
    for (int node : {1, 2, 3})
        leo[static_cast<std::size_t>(node - 1)] = partitionLeo(&t.node(node), s.led.id, s.led.part[static_cast<std::size_t>(node - 1)]);
    t.restart(s.f, t.keyContentsPath(s.f));   // the hold is a runtime seam: the restarted F applies freely
    const std::vector<AuthTrio::Session> bs = sessionsOnEveryBroker(t, "builder");
    ASSERT_EQ(bs.size(), 3u);
    ASSERT_TRUE(AuthTrio::until(60s, [&] {
        try { s.u.sid[static_cast<std::size_t>(s.f - 1)] = t.login(s.f, "lagw").sid; return true; } catch (...) { return false; }
    })) << "the user logs in on the restarted F";
    int served = 0, revoked = 0;
    for (int node : {1, 2, 3}) {
        SCOPED_TRACE("node " + std::to_string(node));
        if (node != s.f)
            ASSERT_TRUE(AuthTrio::until(30s, [&] {
                try { s.u.sid[static_cast<std::size_t>(node - 1)] = t.login(node, "lagw").sid; return true; } catch (...) { return false; }
            }));
        // Every partition this broker leads now (the restart may have moved some).
        for (const PartitionInfo& pi : bs[0].w->raw().metadata(t.channel(bs[0].node), AuthWireClient::kClientStream, bs[0].sid)) {
            if (pi.topicId != s.led.id || pi.leaderId != node) continue;
            for (int i = 0; i < 3; ++i) {
                const int routed = gatewayProduce(t, s.u, node, s.led.id, pi.partition);
                const int direct = pi.ingressEndpoint.empty() ? 45 : shardProduce(s.u, node, pi, s.led.id);
                for (const int code : {routed, direct}) {
                    if (code == 0) ++served;
                    if (code == 45) ++revoked;
                    EXPECT_NE(code, 0) << "partition " << pi.partition << ": the revoked WRITE was served";
                }
            }
        }
    }
    EXPECT_EQ(served, 0);
    EXPECT_GT(revoked, 0) << "the revoke decides once applied";
    for (int node : {1, 2, 3}) {
        const int32_t part = s.led.part[static_cast<std::size_t>(node - 1)];
        EXPECT_LE(partitionLeo(&t.node(node), s.led.id, part), leo[static_cast<std::size_t>(node - 1)])
            << "partition " << part << " grew under the revoked grant";
    }
}

// Subscriptions living through a long hold: two users subscribed on F to the partition F leads - "keep" (its grants
// unchanged) and "cut" (its READ revoked while F is held).  F is held behind until it goes Stale (well past F, short of
// R), every produce on it answers 48 and nothing is appended.  Released: keep's subscription is still live and receives
// every record exactly once, in order (no loss, no duplicate across the pause); cut's subscription is ended with 45 and
// receives nothing produced after the revoke committed.
TEST(AuthClusterRegression, BehindAdversarialSubscriptionsLiveThroughALongHoldWithoutLossOrDuplicateRegression) {
    AuthTrio t("lagsubs");
    t.start();
    t.bootstrapAdmin();
    createBuilder(t);
    for (const char* user : {"keep", "cut"}) {
        t.createUser(1, user);
        t.grant(1, user, SO::Write, ST::Topic, SP::Prefix, "lag.");
        t.grant(1, user, SO::Read, ST::Topic, SP::Prefix, "lag.");
    }
    const std::vector<AuthTrio::Session> bs = sessionsOnEveryBroker(t, "builder");
    ASSERT_EQ(bs.size(), 3u);
    const LedTopic led = defineLedTopic(t, bs, "lag.s");
    ASSERT_GE(led.id, 0);
    ASSERT_TRUE(led.part[0] >= 0 && led.part[1] >= 0 && led.part[2] >= 0);
    const int32_t secLeader = settledSecurityLeader(t.cluster());
    ASSERT_GT(secLeader, 0);
    const int f = secLeader == 3 ? 2 : 3;
    const int32_t part = led.part[static_cast<std::size_t>(f - 1)];
    ASSERT_TRUE(t.awaitDigestsAgree(0, 30s));
    AuthWireClient keepWire(t.cluster().clientsDir->path / "keep_wire");
    AuthWireClient cutWire(t.cluster().clientsDir->path / "cut_wire");
    UserSessions keep, cut;
    keep.name = "keep"; keep.w = &keepWire;
    cut.name = "cut"; cut.w = &cutWire;
    for (UserSessions* us : {&keep, &cut})
        ASSERT_TRUE(AuthTrio::until(30s, [&] {
            try { us->sid[static_cast<std::size_t>(f - 1)] = t.loginOn(*us->w, f, us->name).sid; return true; } catch (...) { return false; }
        }));
    keepWire.raw().keepSessionsAlive(true);
    cutWire.raw().keepSessionsAlive(true);
    ASSERT_TRUE(AuthTrio::until(30s, [&] { return gatewayProduce(t, keep, f, led.id, part) == 0; }));
    const int64_t start = partitionLeo(&t.node(f), led.id, part);   // the subscriptions start here
    auto subscribe = [&](UserSessions& us) -> int64_t {
        auto& c = us.w->raw();
        const int64_t corr = c.subscribeIsolated(t.channel(f), AuthWireClient::kClientStream, us.sid[static_cast<std::size_t>(f - 1)],
                                                 led.id, part, start, /*read_uncommitted*/ 0);
        const auto code = c.awaitSubscribeCode(corr, Clock3::now() + 10s);
        return code && *code == 0 ? corr : -1;
    };
    const int64_t keepCorr = subscribe(keep), cutCorr = subscribe(cut);
    ASSERT_GT(keepCorr, 0);
    ASSERT_GT(cutCorr, 0);
    const auto keepSub = keepWire.raw().subscriptionIdOf(keepCorr), cutSub = cutWire.raw().subscriptionIdOf(cutCorr);
    ASSERT_TRUE(keepSub && cutSub);
    // The offsets one subscription received (every record of every pushed batch).
    auto offsets = [](ShardTestClient& c, int64_t sub) {
        std::vector<int64_t> out;
        for (const auto& p : c.records()) {
            if (p.subscriptionId != sub) continue;
            std::size_t pos = 0;
            while (pos + static_cast<std::size_t>(rune::protocol::batch::kRecordBatchOverhead) <= p.bytes.size()) {
                rune::protocol::RecordBatchReader r(p.bytes.data() + pos, p.bytes.size() - pos);
                if (!r.ensureValid()) break;
                for (int64_t o = r.baseOffset(); o <= r.baseOffset() + r.lastOffsetDelta(); ++o) out.push_back(o);
                pos += static_cast<std::size_t>(r.totalLength());
            }
        }
        return out;
    };
    for (int i = 0; i < 5; ++i) ASSERT_EQ(gatewayProduce(t, keep, f, led.id, part), 0);
    ASSERT_TRUE(keepWire.raw().pollFor(10s, [&] { return offsets(keepWire.raw(), *keepSub).size() >= 5; }));
    ASSERT_TRUE(cutWire.raw().pollFor(10s, [&] { return offsets(cutWire.raw(), *cutSub).size() >= 5; }));

    t.node(f).holdSecurityApplyForTest(true);
    const auto ok = t.revoke(secLeader, "cut", SO::Read, ST::Topic, SP::Prefix, "lag.");
    ASSERT_EQ(ok.errorCode, 0) << ok.detail;
    const int64_t revokedAt = partitionLeo(&t.node(f), led.id, part);
    ASSERT_TRUE(AuthTrio::until(30s, [&] { return t.node(f).securityBehindForTest().gateway; }));
    ASSERT_TRUE(AuthTrio::until(30s, [&] { return t.node(f).securityServingForTest() == "Stale"; }))
        << "the hold lasts past F (short of R: the sessions live on)";
    for (int i = 0; i < 5; ++i) EXPECT_EQ(gatewayProduce(t, keep, f, led.id, part), kAuthUnavailable) << "held, attempt " << i;
    EXPECT_EQ(partitionLeo(&t.node(f), led.id, part), revokedAt) << "nothing appended while behind";
    t.node(f).holdSecurityApplyForTest(false);
    ASSERT_TRUE(AuthTrio::until(30s, [&] { return !t.node(f).securityBehindForTest().workers; }));

    for (int i = 0; i < 5; ++i)
        ASSERT_TRUE(AuthTrio::until(10s, [&] { return gatewayProduce(t, keep, f, led.id, part) == 0; })) << "keep, after, " << i;
    const int64_t end = partitionLeo(&t.node(f), led.id, part);
    ASSERT_TRUE(keepWire.raw().pollFor(10s, [&] { return static_cast<int64_t>(offsets(keepWire.raw(), *keepSub).size()) >= end - start; }))
        << "keep's subscription lived through the hold";
    const std::vector<int64_t> got = offsets(keepWire.raw(), *keepSub);
    ASSERT_EQ(static_cast<int64_t>(got.size()), end - start) << "loss or duplicate across the pause";
    for (std::size_t i = 0; i < got.size(); ++i) EXPECT_EQ(got[i], start + static_cast<int64_t>(i)) << "out of order / gap at " << i;
    bool cutEnded = false;
    cutWire.raw().pollFor(10s, [&] {
        for (const auto& e : cutWire.raw().errors()) if (e.correlationId == cutCorr && e.code == 45) cutEnded = true;
        return cutEnded;
    });
    EXPECT_TRUE(cutEnded) << "cut's subscription is ended with 45 once F applied the revoke";
    for (const int64_t o : offsets(cutWire.raw(), *cutSub)) EXPECT_LT(o, revokedAt) << "cut received a record produced after its revoke";
}
