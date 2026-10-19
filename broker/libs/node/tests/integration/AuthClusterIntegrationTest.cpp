// AuthClusterIntegrationTest.cpp - authentication Task 19 (spec §11 "Brokers", §12): THREE real authenticating
// BrokerNodes over real Aeron UDP (sealed peer links): __security replicates to every broker and the digests agree,
// logins work through every broker, a REVOKE ends live subscriptions on every broker within 100 ms, a leader kill
// mid security DDL applies the retried request once, peer keys right / wrong / none, key rotation across rolling
// restarts.  Every wait is bounded.
#include "support/AuthRig.h"
#include "support/AuthTrio.h"

#include <algorithm>
#include <cstdio>
#include <mutex>
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

/// Produce one record through broker session `s` until acked (NotLeader / UnknownTopic while placement settles).
bool produceOk(const AuthTrio::Session& s, AuthTrio& t, int32_t topic) {
    return s.w->raw().produceUntilOk(t.channel(s.node), AuthWireClient::kClientStream, s.sid, topic, 0, 1, makeBatch(1)).has_value();
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

bool awaitNodeNotInIsr(AuthTrio& t, int observer, int32_t node, std::chrono::milliseconds within) {
    return AuthTrio::until(within, [&] {
        const auto sec = securityAssignmentOf(t.node(observer));
        const auto cat = catalogAssignmentOf(t.node(observer));
        return sec && cat && !contains(sec->isr, node) && !contains(cat->isr, node);
    });
}
bool awaitNodeInIsr(AuthTrio& t, int observer, int32_t node, std::chrono::milliseconds within) {
    return AuthTrio::until(within, [&] {
        const auto sec = securityAssignmentOf(t.node(observer));
        const auto cat = catalogAssignmentOf(t.node(observer));
        return sec && cat && contains(sec->isr, node) && contains(cat->isr, node);
    });
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
    ASSERT_TRUE(awaitNodeInIsr(t, 1, 3, 30s));
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

    // Node 3 leaves the ISR of the catalog and __security and of the user partitions, and stays out.
    ASSERT_TRUE(awaitNodeNotInIsr(t, 1, 3, 60s)) << "node 3 must leave the ISR";
    ASSERT_TRUE(awaitNodeNotInIsr(t, 2, 3, 60s));
    for (int i = 0; i < 20; ++i) {
        std::this_thread::sleep_for(100ms);   // observation window of a state that must NOT change
        for (int obs : {1, 2}) {
            EXPECT_FALSE(contains(securityAssignmentOf(t.node(obs))->isr, 3));
            EXPECT_FALSE(contains(catalogAssignmentOf(t.node(obs))->isr, 3));
        }
    }
    // A client session on node 3 (its security view is on its own disk) cannot produce to a partition led elsewhere:
    // its forwarded frames never open at the leader.
    AuthTrio::Session w3;
    ASSERT_TRUE(AuthTrio::until(30s, [&] { try { w3 = t.login(3, "writer"); return true; } catch (...) { return false; } }));
    const int64_t corr = w3.w->raw().nextCorrelationId();
    ASSERT_TRUE(w3.w->raw().produce(t.channel(3), AuthWireClient::kClientStream, w3.sid, corr, led.id, led.part[0], 1, makeBatch(1)));
    const auto ack = w3.w->raw().awaitProduceResponseUntil(corr, Clock3::now() + 10s);
    EXPECT_FALSE(ack && ack->errorCode == 0) << "a wrong-key broker's forwarded produce must not be acknowledged";

    t.restart(3, good);
    EXPECT_TRUE(awaitNodeInIsr(t, 1, 3, 30s)) << "the right key brings node 3 back into the ISR within 30 s";
    EXPECT_TRUE(awaitNodeInIsr(t, 2, 3, 30s));
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
    a.w->pinRequestId(rid);
    const auto held = a.w->createUser(t.channel(leader), a.sid, "killvictim", AuthTrio::kPw, 2s);
    EXPECT_NE(held.errorCode, 0) << "the held leader never answers";
    // The CreateUser is appended and replicated (committed) while the answer is held.
    ASSERT_TRUE(AuthTrio::until(30s, [&] {
        for (BrokerNode* n : t.cluster().live())
            if (n->nodeIdForTest() != leader && n->securityViewForTest().appliedOffset <= before) return false;
        return true;
    })) << "the event must be committed and applied everywhere before the kill";

    t.node(leader).crashForTest();
    t.stopNode(leader);
    ASSERT_TRUE(AuthTrio::until(60s, [&] { const int32_t l = settledSecurityLeader(t.cluster()); return l > 0 && l != leader; }))
        << "a new __security leader";
    const int32_t other = settledSecurityLeader(t.cluster());
    for (int id : {1, 2, 3}) if (id != leader) t.adminOn(id).w->pinRequestId(rid);
    const auto retry = t.ddl(other, [&](AuthWireClient& w, const std::string& ch, int64_t sid) { return w.createUser(ch, sid, "killvictim", AuthTrio::kPw); });
    EXPECT_EQ(retry.errorCode, 0) << retry.detail;
    EXPECT_NE(retry.objectId, 0u) << "the answer carries the user's object id (the deduped request's)";
    for (int id : {1, 2, 3}) if (id != leader) t.adminOn(id).w->pinRequestId(std::nullopt);

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
}
