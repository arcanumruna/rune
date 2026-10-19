// AuthClusterIntegrationTest.cpp - authentication Task 19 (spec §11 "Brokers", §12): THREE real authenticating
// BrokerNodes over real Aeron UDP (sealed peer links): __security replicates to every broker and the digests agree,
// logins work through every broker, a REVOKE ends live subscriptions on every broker within 100 ms, a leader kill
// mid security DDL applies the retried request once, peer keys right / wrong / none, key rotation across rolling
// restarts.  Every wait is bounded.
#include "support/AuthMatrixExpectations.h"
#include "support/AuthRig.h"
#include "support/AuthTrio.h"

#include <algorithm>
#include <map>
#include <cstdio>
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

/// DefineTopic as `s` (a session on any broker); the catalog leader answers, the others redirect: try every broker
/// the builder has a session on, bounded.  Returns the topic id or -1.
int32_t defineTopicAnywhere(AuthTrio& t, const std::vector<AuthTrio::Session>& sessions, const std::string& name, uint32_t partitions = 1) {
    const auto deadline = Clock3::now() + 60s;
    while (Clock3::now() < deadline) {
        for (const auto& s : sessions) {
            auto& c = s.w->raw();
            const auto r = c.awaitDefine(c.sendDefineTopic(t.channel(s.node), AuthWireClient::kClientStream, s.sid, name, partitions, authRigCharBlob()),
                                         Clock3::now() + 15s);
            if (r && r->errorCode == 0) return r->topicId;
        }
        std::this_thread::sleep_for(50ms);
    }
    return -1;
}

/// One topic with 3 partitions; part[i] = a partition broker i+1 leads (-1 = none).  A subscription / produce binds only
/// on the partition's leader (a follower answers Redirect / NotLeader).
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
        auto l = std::make_shared<spdlog::logger>("needle-capture", sink);
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
    std::vector<AuthTrio::Session> bs{t.login(1, "builder")};
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
    // A client session on node 3 (its security view is on its own disk) cannot produce to a partition led elsewhere:
    // it answers Redirect (3), nothing is stored on the leader, and node 3's frames keep being dropped (never opened).
    AuthTrio::Session w3;
    ASSERT_TRUE(AuthTrio::until(30s, [&] { try { w3 = t.login(3, "writer"); return true; } catch (...) { return false; } }));
    const int64_t leo0 = partitionLeo(&t.node(1), led.id, led.part[0]);
    const uint64_t drops0 = t.node(1).peerLinksForTest().frameDrops + t.node(2).peerLinksForTest().frameDrops;
    const int64_t corr = w3.w->raw().nextCorrelationId();
    ASSERT_TRUE(w3.w->raw().produce(t.channel(3), AuthWireClient::kClientStream, w3.sid, corr, led.id, led.part[0], 1, makeBatch(1)));
    const auto ack = w3.w->raw().awaitProduceResponseUntil(corr, Clock3::now() + 10s);
    // The produce is never acknowledged: node 3 has no link to the leader, so the forwarded frame is dropped unopened
    // (no answer) or refused - and, the real proof, nothing reaches the leader's log.
    EXPECT_TRUE(!ack.has_value() || ack->errorCode != 0) << "a wrong-key broker must never acknowledge a produce";
    EXPECT_EQ(partitionLeo(&t.node(1), led.id, led.part[0]), leo0) << "nothing reached the leader's log";
    EXPECT_GT(drops0, 0u) << "node 3's frames were dropped unopened by nodes 1 and 2";

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
struct ConsumerResult { std::map<int64_t, int> deliveries; std::set<int64_t> offsets; bool complete = false; int resubscribes = 0; };

/// A producer that owns its client (own Aeron driver) and its thread: it logs in on whichever broker answers, follows
/// Redirects, re-logs-in after a restart, and hands its acked offsets back at join.  Shares nothing with the test.
ProducerResult runProducer(std::stop_token stop, std::array<std::string, 3> channels, std::filesystem::path dir, int32_t topic) {
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
        if (code == 0) { out.acked.push_back(c.ackFor(corr)->baseOffset); out.ackedAt.push_back(Clock3::now()); std::this_thread::sleep_for(10ms); continue; }
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
    std::vector<AuthTrio::Session> bs{t.login(1, "builder")};
    const int32_t topic = defineTopicAnywhere(t, bs, "rot.t", 1);
    ASSERT_GE(topic, 0);
    ASSERT_TRUE(AuthTrio::until(60s, [&] { const int32_t l = leaderOf(t, bs[0], topic, 0); return l >= 1 && l <= 3; }));
    const std::size_t batchBytes = makeBatch(1).size();

    const std::string k1 = t.keyContentsPath(1);
    const std::string k2 = t.makeKeyFile("k2.key", 'Q');
    const std::array<std::string, 3> channels{t.channel(1), t.channel(2), t.channel(3)};
    std::promise<int64_t> target;
    std::jthread producer;
    std::jthread consumer;
    std::promise<ProducerResult> pOut;
    std::promise<ConsumerResult> cOut;
    auto pFut = pOut.get_future();
    auto cFut = cOut.get_future();
    // everything the threads need is copied BEFORE they start: they never touch the trio while it restarts nodes
    const std::filesystem::path prodDir = t.cluster().clientsDir->path / "rot_prod", consDir = t.cluster().clientsDir->path / "rot_cons";
    producer = std::jthread([&pOut, ch = channels, prodDir, topic](std::stop_token st) {
        try { pOut.set_value(runProducer(st, ch, prodDir, topic)); }
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
            restarts.push_back({t0, Clock3::now(), st.name, id});
            std::this_thread::sleep_for(4s);   // traffic keeps flowing between restarts (the threads never stop)
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
    }
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
    std::vector<AuthTrio::Session> bs{t.login(1, "builder")};
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
