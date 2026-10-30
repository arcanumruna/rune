// AuthSecurityBehindTest.cpp - issue #603 against ONE real authenticating broker (shard runtime: the gateway, the worker
// ingress, worker-pushed subscriptions).  The single broker LEADS `__security`: holding its applier while a security DDL
// commits is "a leader with an unapplied tail".  While its view is behind committed events it holds, every grant-checked
// operation answers the retriable AuthUnavailable (48) and appends nothing; once the view applied them the operations
// are judged again - by the NEW grants.  Every wait is bounded; the background DDL runs on the admin's own wire.
#include "support/AuthRig.h"

#include <atomic>
#include <future>
#include <thread>

using namespace rune::node;
using namespace rune::node::test;
using namespace rune::node::test::grants;
namespace pc = rune::protocol::client;
using ST = rune::protocol::SecurityResourceType;
using SO = rune::protocol::SecurityOperation;
using SP = rune::protocol::SecurityPatternKind;
using namespace std::chrono_literals;

namespace {
constexpr int kUnavailable = static_cast<int>(pc::ErrorCode::AuthUnavailable);

AuthBroker::Options shardOptions() { AuthBroker::Options o; o.shard = true; return o; }

struct Behind {
    AuthRig r;
    explicit Behind(const std::string& tag) : r(tag, shardOptions()) { r.c().keepSessionsAlive(true); }

    AuthBroker& b() { return r.broker(); }
    BrokerNode& node() { return b().node(); }
    const std::string& ch() { return r.ch(); }

    /// A security DDL as the admin, in the background: its OK waits for this broker's (held) apply.
    std::future<int> revokeAsync(const std::string& who, SO op, ST t, const std::string& name) {
        return std::async(std::launch::async, [this, who, op, t, name]() -> int {
            return b().admin().revoke(ch(), b().adminSession(), who, false, op, t, SP::Exact, name).errorCode;
        });
    }
    std::future<int> grantAsync(const std::string& who, SO op, ST t, const std::string& name) {
        return std::async(std::launch::async, [this, who, op, t, name]() -> int {
            return b().admin().grant(ch(), b().adminSession(), who, false, op, t, SP::Exact, name).errorCode;
        });
    }
    static int await(std::future<int>& f, std::chrono::seconds within = 30s) {
        return f.wait_for(within) == std::future_status::ready ? f.get() : kNoAnswer;
    }
    bool gatewayBehind(bool want, std::chrono::milliseconds within = 10s) {
        return r.c().pollFor(within, [&] { return node().securityBehindForTest().gateway == want; });
    }
    bool workersBehind(bool want, std::chrono::milliseconds within = 10s) {
        return r.c().pollFor(within, [&] { return node().securityBehindForTest().workers == want; });
    }
    /// One worker-ingress produce (kNoAnswer: none within `seconds`).
    int shardProduce(int64_t sid, int32_t topic, int seconds = 3) {
        const int64_t corr = r.c().nextCorrelationId();
        if (!r.c().produce(b().workerIngressChannel(), AuthRig::stream(), sid, corr, topic, 0, 1, makeCharBatch(1)))
            return kNoAnswer;
        const auto ack = r.c().awaitProduceResponseUntil(corr, AuthRig::dl(seconds));
        return ack ? ack->errorCode : kNoAnswer;
    }
    int shardProduceSettled(int64_t sid, int32_t topic) {
        int code = kNoAnswer;
        for (int a = 0; a < 200; ++a) {
            code = shardProduce(sid, topic);
            if (code != static_cast<int>(pc::ErrorCode::NotLeader) && code != static_cast<int>(pc::ErrorCode::UnknownTopic) &&
                code != kNoAnswer)
                return code;
            std::this_thread::sleep_for(25ms);
        }
        return code;
    }
    uint16_t loginCode(const std::string& user) {
        AuthWireClient& w = r.wire();
        const int64_t sid = w.connect(ch());
        if (sid <= 0) return 0xFFFF;
        return w.login(ch(), sid, user, {AuthRig::kPw, AuthRig::kPw}).errorCode();
    }
};

/// The operations of the matrix, each as "the answer code of one attempt".  `revoked`: the REVOKE takes its grant away;
/// `stateless`: answering it changes nothing, so the answer after the apply equals the one before (group requests move
/// the group's state, so only "not 48" is asserted for them).
struct Op { const char* name; std::function<int()> run; bool revoked; bool stateless; };
}  // namespace

// The interaction matrix's single-broker slice: change {REVOKE, GRANT} x every grant-checked operation x apply {held,
// released}, on the `__security` LEADER (its own unapplied tail).  Held: every operation answers 48 and nothing is
// appended.  Released: each is judged by the view that now holds the change - the revoked ones 45, the rest as before.
// (RED before #603: while held the old grants were served - produce answered 0 and was appended.)
TEST(AuthSecurityBehindRegression, ALeaderBehindItsOwnCommittedEventsRefusesEveryGrantCheckedOperationRegression) {
    for (const bool revoke : {true, false}) {
        SCOPED_TRACE(revoke ? "REVOKE" : "GRANT");
        Behind t(revoke ? "behindrevoke" : "behindgrant");
        std::string writer, reader;
        const int64_t ws = t.r.newUser({exact(SO::Write, ST::Topic, "orders")}, &writer);
        const int64_t rs = t.r.newUser({exact(SO::Read, ST::Topic, "orders"), exact(SO::Read, ST::View, "board"),
                                        exact(SO::Read, ST::Group, "g"), cluster(SO::Describe)}, &reader);
        ASSERT_EQ(t.r.produce(ws, t.r.orders, /*untilOk=*/true), 0);
        ASSERT_EQ(t.shardProduceSettled(ws, t.r.orders), 0);
        const std::vector<Op> ops = {
            {"routed produce", [&] { return t.r.produce(ws, t.r.orders); }, true, true},
            {"worker-ingress produce", [&] { return t.shardProduce(ws, t.r.orders); }, true, true},
            {"subscribe", [&] { return t.r.subscribe(rs, t.r.orders); }, false, true},
            {"view read", [&] { return t.r.getViewRow(rs, t.r.board); }, false, true},
            {"join group", [&] { return t.r.joinGroup(rs, "g", {t.r.orders}); }, false, false},
            {"offset commit", [&] { return t.r.offsetCommit(rs, "g", t.r.orders); }, false, false},
            {"catalog read", [&] { return t.r.catalogSubscribe(rs); }, false, true},
        };
        std::vector<int> baseline;
        for (const Op& op : ops) baseline.push_back(op.run());
        for (std::size_t i = 0; i < ops.size(); ++i) ASSERT_NE(baseline[i], kUnavailable) << ops[i].name;
        ASSERT_EQ(t.node().adminSecurityBehindForTest().edges, 0u) << "normal operation never flipped it";

        t.node().holdSecurityApplyForTest(true);
        auto ddl = revoke ? t.revokeAsync(writer, SO::Write, ST::Topic, "orders")
                          : t.grantAsync(writer, SO::Write, ST::Topic, "secret");
        ASSERT_TRUE(t.gatewayBehind(true)) << "the leader holds a committed event it has not applied";
        ASSERT_TRUE(t.workersBehind(true)) << "the workers are told too";
        const int64_t leo = t.r.logEnd(t.r.orders);
        for (const Op& op : ops) EXPECT_EQ(op.run(), kUnavailable) << op.name << " while the view is behind";
        EXPECT_EQ(t.loginCode(writer), kUnavailable) << "no login is judged against a view behind its own log";
        EXPECT_EQ(t.r.logEnd(t.r.orders), leo) << "nothing appended while behind";
        EXPECT_EQ(t.node().adminSecurityBehindForTest().edges, 1u) << "one edge, however long it is held";
        EXPECT_NE(ddl.wait_for(0s), std::future_status::ready) << "the DDL's OK waits for this broker's enforcement";

        t.node().holdSecurityApplyForTest(false);
        ASSERT_EQ(Behind::await(ddl), 0) << "the DDL is answered once applied";
        ASSERT_TRUE(t.gatewayBehind(false));
        ASSERT_TRUE(t.workersBehind(false)) << "the workers' copy clears once the event's fences were acknowledged";
        EXPECT_EQ(t.node().adminSecurityBehindForTest().edges, 2u);
        for (std::size_t i = 0; i < ops.size(); ++i) {
            const int code = ops[i].run();
            EXPECT_NE(code, kUnavailable) << ops[i].name << " after the apply";
            if (revoke && ops[i].revoked) EXPECT_EQ(code, kNotAuthorized) << ops[i].name << ": the revoked grant";
            else if (ops[i].stateless) EXPECT_EQ(code, baseline[i]) << ops[i].name << ": judged as before";
        }
    }
}

// Normal operation never flips it: DDL after DDL (each committed and applied on the leader in the same pass, the
// SecurityAdmin append included) leaves no edge and refuses nothing.  (RED with the applier's second call removed from
// AdminWorker::doWork: a sole-ISR append commits inside SecurityAdmin's pass, after the first apply.)
TEST(AuthSecurityBehindRegression, SecurityDdlInNormalOperationNeverFlipsItRegression) {
    Behind t("behindnormal");
    std::string writer;
    const int64_t ws = t.r.newUser({exact(SO::Write, ST::Topic, "orders")}, &writer);
    ASSERT_EQ(t.r.produce(ws, t.r.orders, /*untilOk=*/true), 0);
    for (int i = 0; i < 25; ++i) {
        ASSERT_EQ(t.b().admin().grant(t.ch(), t.b().adminSession(), writer, false, SO::Write, ST::Topic, SP::Exact, "secret").errorCode, 0);
        ASSERT_EQ(t.r.produce(ws, t.r.secret), 0) << i;
        ASSERT_EQ(t.b().admin().revoke(t.ch(), t.b().adminSession(), writer, false, SO::Write, ST::Topic, SP::Exact, "secret").errorCode, 0);
        ASSERT_EQ(t.r.produce(ws, t.r.secret), kNotAuthorized) << i;
    }
    EXPECT_EQ(t.node().adminSecurityBehindForTest().edges, 0u) << "an event committed and applied in one pass never flips it";
    EXPECT_EQ(t.node().securityBehindForTest().refusals, 0u);
}

// The way out: the applied view must reach the hot path BEFORE the flag clears.  The control thread's mirror is held
// (its frames - the applied REVOKE and the clearing verdict behind it - wait in order) while the admin applier is
// released: the admin thread is caught up, yet the gateway still refuses 48 - never OK - until the mirror applies the
// REVOKE; then the produce is refused NotAuthorized on both paths.  (RED if the clear could overtake the event.)
TEST(AuthSecurityBehindRegression, TheFlagClearsOnlyAfterTheViewAppliedTheHeldEventsRegression) {
    Behind t("behindorder");
    std::string writer;
    const int64_t ws = t.r.newUser({exact(SO::Write, ST::Topic, "orders")}, &writer);
    ASSERT_EQ(t.r.produce(ws, t.r.orders, /*untilOk=*/true), 0);
    ASSERT_EQ(t.shardProduceSettled(ws, t.r.orders), 0);
    t.node().holdSecurityApplyForTest(true);
    auto ddl = t.revokeAsync(writer, SO::Write, ST::Topic, "orders");
    ASSERT_TRUE(t.gatewayBehind(true));
    const int64_t applied = t.node().securityViewForTest().appliedOffset;
    t.node().holdSecurityMirrorForTest(true);
    t.node().holdSecurityApplyForTest(false);
    ASSERT_TRUE(t.r.c().pollFor(10s, [&] { return !t.node().adminSecurityBehindForTest().behind; }))
        << "the admin thread applied the REVOKE";
    const int64_t leo = t.r.logEnd(t.r.orders);
    for (int i = 0; i < 5; ++i) {
        EXPECT_EQ(t.r.produce(ws, t.r.orders), kUnavailable) << "routed, mirror held, attempt " << i;
        EXPECT_EQ(t.shardProduce(ws, t.r.orders), kUnavailable) << "worker, mirror held, attempt " << i;
    }
    EXPECT_TRUE(t.node().securityBehindForTest().gateway) << "the clearing verdict waits behind the held event";
    EXPECT_EQ(t.node().securityViewForTest().appliedOffset, applied) << "precondition: the view has not applied it";
    EXPECT_EQ(t.r.logEnd(t.r.orders), leo);
    t.node().holdSecurityMirrorForTest(false);
    ASSERT_EQ(Behind::await(ddl), 0);
    ASSERT_TRUE(t.gatewayBehind(false));
    ASSERT_TRUE(t.workersBehind(false));
    EXPECT_EQ(t.r.produce(ws, t.r.orders), kNotAuthorized) << "routed: the revoke, never the old grant";
    EXPECT_EQ(t.shardProduce(ws, t.r.orders), kNotAuthorized) << "worker: the revoke, never the old grant";
    EXPECT_EQ(t.r.logEnd(t.r.orders), leo) << "nothing appended at any point after the revoke committed";
}

// A REVOKE landing mid-stream for an active producer (worker ingress and routed, its own wire and thread): the answers
// go 0 ... 0, then 48 (the view is behind), then 45 (applied) - never an OK after the first refusal, and the log never
// grows after it.  Repeated over rounds (re-granted between them).
TEST(AuthSecurityBehindRegression, ARevokeLandingMidStreamForAnActiveProducerNeverServesTheOldGrantRegression) {
    Behind t("behindstream");
    std::string writer;
    (void)t.r.newUser({exact(SO::Write, ST::Topic, "orders")}, &writer);
    AuthWireClient& pw = t.b().wire();   // a fresh wire of its own, used only by the producer thread
    const int64_t psid = pw.connect(t.ch());
    ASSERT_GT(psid, 0);
    ASSERT_TRUE(pw.login(t.ch(), psid, writer, {AuthRig::kPw, AuthRig::kPw}).ok());
    pw.raw().keepSessionsAlive(true);
    ASSERT_TRUE(pw.raw().produceUntilOk(t.ch(), AuthRig::stream(), psid, t.r.orders, 0, 1, makeCharBatch(1)));
    for (int round = 0; round < 3; ++round) {
        SCOPED_TRACE("round " + std::to_string(round));
        std::vector<int> answers;
        int64_t leoAtFirstRefusal = -1;   // the producer is sequential: nothing of it is in flight at its first refusal
        std::atomic<int> oks{0}, behinds{0};   // test-only progress counters: the waits below are by condition
        std::thread producer([&] {
            auto& c = pw.raw();
            int revokedInARow = 0;
            for (int i = 0; i < 20'000 && revokedInARow < 3; ++i) {
                const int64_t corr = c.nextCorrelationId();
                const std::string& via = (i & 1) ? t.b().workerIngressChannel() : t.ch();
                if (!c.produce(via, AuthRig::stream(), psid, corr, t.r.orders, 0, 1, makeCharBatch(1))) continue;
                const auto a = c.awaitProduceResponseUntil(corr, AuthRig::dl(3));
                answers.push_back(a ? a->errorCode : kNoAnswer);
                if (answers.back() == 0) oks.fetch_add(1);
                if (answers.back() == kUnavailable) behinds.fetch_add(1);
                if (leoAtFirstRefusal < 0 && (answers.back() == kUnavailable || answers.back() == kNotAuthorized))
                    leoAtFirstRefusal = t.r.logEnd(t.r.orders);
                revokedInARow = answers.back() == kNotAuthorized ? revokedInARow + 1 : 0;
            }
        });
        // The stream is running (by condition, PR #605 review: never a sleep): the revoke lands mid-stream.
        EXPECT_TRUE(t.r.c().pollFor(30s, [&] { return oks.load() >= 20; }));
        t.node().holdSecurityApplyForTest(true);
        auto ddl = t.revokeAsync(writer, SO::Write, ST::Topic, "orders");
        EXPECT_TRUE(t.gatewayBehind(true));
        // A held window the producer streams through.
        EXPECT_TRUE(t.r.c().pollFor(30s, [&] { return behinds.load() >= 10; }));
        t.node().holdSecurityApplyForTest(false);
        EXPECT_EQ(Behind::await(ddl), 0);
        producer.join();
        bool refused = false;
        int behindAnswers = 0, oksAfter = 0, oksBefore = 0;
        for (const int code : answers) {
            if (code == kUnavailable || code == kNotAuthorized) refused = true;
            if (code == kUnavailable) ++behindAnswers;
            if (code == 0) ++(refused ? oksAfter : oksBefore);
        }
        EXPECT_GT(oksBefore, 0) << "the stream was live before the revoke";
        EXPECT_GT(behindAnswers, 0) << "the producer streamed through the held window";
        EXPECT_EQ(oksAfter, 0) << "an OK after the first refusal: the old grant was served";
        ASSERT_GE(leoAtFirstRefusal, 0);
        EXPECT_EQ(t.r.logEnd(t.r.orders), leoAtFirstRefusal) << "nothing appended after the first refusal";
        ASSERT_EQ(t.b().admin().grant(t.ch(), t.b().adminSession(), writer, false, SO::Write, ST::Topic, SP::Exact, "orders").errorCode, 0);
        ASSERT_TRUE(pw.raw().produceUntilOk(t.ch(), AuthRig::stream(), psid, t.r.orders, 0, 1, makeCharBatch(1)));
    }
}

// PR #605 review (adversarial, resources): an admin streaming `__security` is sent the held event itself (the read server
// serves up to the HW, not the applier), and the behind Hold pauses that read.  While the broker stays behind it must
// STAY paused.  RED before the fix: every control pass found the session's egress Ready, resumed it, the admin re-read
// the log and the gate held it again - a busy pause / resume loop on the admin thread (the thread that has to catch up)
// for as long as the hold lasted.  Once the view applied the event, the read resumes and delivers it.
TEST(AuthSecurityBehindRegression, ASecurityReadHeldWhileBehindStaysPausedUntilAppliedAdversarialRegression) {
    Behind t("behindsecread");
    std::string writer;
    (void)t.r.newUser({exact(SO::Write, ST::Topic, "orders")}, &writer);
    const int64_t ws = t.r.newUser({exact(SO::Write, ST::Topic, "orders")});
    ASSERT_EQ(t.r.produce(ws, t.r.orders, /*untilOk=*/true), 0);
    // The reader is an admin session on a wire of its own: the background DDL drives the admin's wire (one thread per
    // wire), and this thread polls the reader's.
    AuthWireClient& rw = t.b().wire();
    const int64_t as = rw.connect(t.ch());
    ASSERT_GT(as, 0);
    ASSERT_TRUE(rw.login(t.ch(), as, "admin", {AuthBroker::kAdminPassword, AuthBroker::kAdminPassword}).ok());
    auto& ac = rw.raw();
    ac.keepSessionsAlive(true);
    const int64_t corr = ac.sendSubscribe(t.ch(), AuthRig::stream(), as, pc::kSecurityTopicId, 0);
    ASSERT_EQ(ac.awaitSubscribeCode(corr, AuthRig::dl()).value_or(-1), 0);
    const auto newestPushed = [&] {
        int64_t newest = -1;
        for (const auto& p : ac.records()) newest = std::max(newest, p.baseOffset);
        return newest;
    };
    const int64_t applied = t.node().securityViewForTest().appliedOffset;
    ASSERT_TRUE(ac.pollFor(10s, [&] { return newestPushed() >= 0; })) << "the read streams the log";
    const auto flow = [&] {
        return t.node().onControlThreadForTest([&] {
            const CatalogReadFlow& f = t.node().catalogReadFlowForTest();
            return std::pair<uint64_t, uint64_t>{f.pauses(), f.resumes()};
        });
    };
    const auto before = flow();

    t.node().holdSecurityApplyForTest(true);
    auto ddl = t.grantAsync(writer, SO::Write, ST::Topic, "secret");
    ASSERT_TRUE(t.gatewayBehind(true));
    ASSERT_TRUE(ac.pollFor(10s, [&] { return flow().first > before.first; })) << "the held event's frame was held";
    const auto held = flow();
    // Many control passes while behind (each refused request is a round trip through the control thread).
    for (int i = 0; i < 40; ++i) EXPECT_EQ(t.r.produce(ws, t.r.orders), kUnavailable) << i;
    const auto after = flow();
    EXPECT_LE(after.first - held.first, 1u) << "the held read was resumed and re-held " << (after.first - held.first)
                                             << " times while the broker stayed behind (a busy loop)";
    EXPECT_LE(after.second - held.second, 1u) << "resumed while still behind";
    EXPECT_EQ(t.node().securityViewForTest().appliedOffset, applied) << "the hold held";

    t.node().holdSecurityApplyForTest(false);
    ASSERT_EQ(Behind::await(ddl), 0);
    ASSERT_TRUE(t.gatewayBehind(false));
    EXPECT_TRUE(ac.pollFor(10s, [&] { return newestPushed() > applied; })) << "after the apply the read delivers the event";
}

// PR #605 review (adversarial, halt): a HALTED applier never applies again (D28), so the broker is behind for good - the
// flag is set on its halt and never clears, whatever follows (more security DDL, more passes); a producer's session is
// ended retriably by the halt (D28) and nothing it sends after is appended.
TEST(AuthSecurityBehindRegression, AHaltedApplierStaysBehindForGoodAdversarialRegression) {
    Behind t("behindhalt");
    std::string writer;
    const int64_t ws = t.r.newUser({exact(SO::Write, ST::Topic, "orders")}, &writer);
    ASSERT_EQ(t.r.produce(ws, t.r.orders, /*untilOk=*/true), 0);
    BrokerNode& n = t.node();
    n.onAdminThreadForTest([&n]() {
        n.adminWorkerAgentForTest()->worker().securityApplierForTest()->tamperNextFingerprintForTest();
        return 0;
    });
    auto ddl = t.grantAsync(writer, SO::Write, ST::Topic, "secret");
    ASSERT_TRUE(t.r.c().pollFor(20s, [&] { return n.adminSecurityBehindForTest().behind; })) << "the halt sets it";
    EXPECT_NE(Behind::await(ddl), 0) << "a halted apply never claims enforcement";
    ASSERT_TRUE(t.gatewayBehind(true));
    const int64_t leo = t.r.logEnd(t.r.orders);
    for (int i = 0; i < 5; ++i) {
        EXPECT_NE(t.r.produce(ws, t.r.orders), 0) << "routed, halted, attempt " << i;
        EXPECT_NE(t.shardProduce(ws, t.r.orders), 0) << "worker, halted, attempt " << i;
    }
    auto more = t.grantAsync(writer, SO::Write, ST::Topic, "other");
    (void)Behind::await(more);
    EXPECT_TRUE(n.adminSecurityBehindForTest().behind) << "a halted applier never reads as caught up";
    EXPECT_TRUE(n.securityBehindForTest().gateway);
    EXPECT_EQ(n.adminSecurityBehindForTest().edges, 1u) << "set once, never cleared";
    EXPECT_EQ(t.r.logEnd(t.r.orders), leo) << "nothing appended after the halt";
}
