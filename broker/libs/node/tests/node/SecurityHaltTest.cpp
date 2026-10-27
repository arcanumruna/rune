// SecurityHaltTest.cpp - authentication Task 14 (D28): a halted `__security` apply FAILS CLOSED.  One real authenticating
// broker; the applier's next fingerprint is damaged (tamperNextFingerprintForTest), then any security DDL halts it.
#include "support/AuthRig.h"

#include <rune/node/AdminChannels.h>
#include <rune/node/AdminWorker.h>

#include <functional>

using namespace rune::node;
using namespace rune::node::test;
using namespace rune::node::test::grants;
namespace pc = rune::protocol::client;
using ST = rune::protocol::SecurityResourceType;
using SO = rune::protocol::SecurityOperation;
using namespace std::chrono_literals;

TEST(SecurityHaltRegression, HaltEndsEverySessionRetriablyAndRefusesLoginsRegression) {
    LogCapture log;
    AuthRig r("halt");
    AuthBroker& b = r.broker();
    std::string name;
    const int64_t user = r.newUser({exact(SO::Read, ST::Topic, "orders")}, &name);
    const int64_t corr = r.c().sendSubscribe(r.ch(), r.stream(), user, r.orders, 0);
    ASSERT_EQ(r.c().awaitSubscribeCode(corr, AuthRig::dl()).value_or(-1), 0);

    BrokerNode& n = b.node();
    n.onAdminThreadForTest([&n]() {
        n.adminWorkerAgentForTest()->worker().securityApplierForTest()->tamperNextFingerprintForTest();
        return 0;
    });
    // Any security DDL: it commits, the applier's fingerprint check fails, the applier HALTS.  It must never be answered OK.
    pc::SecurityDdl m;
    m.sessionId = b.adminSession(); m.kind = pc::SecurityOpKind::CreateRole; m.name = "halting";
    const auto ddl = b.admin().securityDdl(r.ch(), m, "CREATE ROLE halting;", 3s);
    EXPECT_NE(ddl.errorCode, 0) << "a halted apply never claims enforcement";

    const auto deadline = std::chrono::steady_clock::now() + 20s;
    while (!n.securityViewForTest().halted && std::chrono::steady_clock::now() < deadline) std::this_thread::sleep_for(10ms);
    ASSERT_TRUE(n.securityViewForTest().halted);

    // #206: every authenticated session is ended RETRIABLY - one session-level AuthUnavailable (48) with the halt detail,
    // no SessionRevoked on its subscription - so its client reconnects to another broker.  Still fail-closed.
    const int unavailable = static_cast<int>(pc::ErrorCode::AuthUnavailable);
    int onSub = -1, onSession = -1;
    std::string detail;
    r.c().pollUntil([&] {
        for (const auto& e : r.c().errors()) {
            if (e.correlationId == corr) onSub = e.code;
            if (e.correlationId == 0 && e.sessionId == user) { onSession = e.code; detail = e.message; }
        }
        return onSession != -1;
    });
    EXPECT_EQ(onSession, unavailable);
    EXPECT_EQ(onSub, -1) << "no SessionRevoked on the subscription's correlation id";
    EXPECT_EQ(detail, "security log halted on this broker - reconnect to another broker");
    EXPECT_TRUE(r.awaitSessionGone(user)) << "#138: the gateway ended the session";

    // New logins are refused retriably (AuthUnavailable: try another broker), the broker itself stays up.
    const int64_t s2 = r.wire().connect(r.ch());
    ASSERT_GT(s2, 0) << "the broker still accepts a Connect";
    EXPECT_EQ(r.wire().login(r.ch(), s2, name, {AuthRig::kPw, AuthRig::kPw}).errorCode(), unavailable);
    const auto halted = b.counter(typeIdOf(RuneMetric::SecurityApplyHalted));
    ASSERT_TRUE(halted.has_value());
    EXPECT_GT(*halted, 0) << "counter 1'000'226 is raised";
    EXPECT_EQ(b.counter(typeIdOf(RuneMetric::SessionsRevoked)).value_or(0), 0) << "a halt revokes nothing (counter 232)";
    EXPECT_GE(log.count("SECURITY APPLY HALTED"), 1);
    EXPECT_GE(log.count("docs/SECURITY.md#halted"), 1);
}

// ── Issue #136: a malformed or skipped kA2cSecurityApplied (the admin -> control `__security` mirror frame) ───────────
// The control thread's SecurityView is what the gateway enforces (the D24 answer gate releases on it).  A frame it
// cannot parse - truncated at every short length, a negative offset - or one whose offset does not follow the view's
// (a REPEAT of an applied offset: the offset moves backwards; a JUMP: an event skipped) carries, or stands in for, an
// event the view would miss.  Whatever the frame, the enforced offset never moves to a wrong place: the view does not
// apply it (its applied offset and the gateway's enforced offset stay where they were) and the broker FAILS CLOSED -
// the view halts and every authenticated session is ended retriably (48), so nothing is served from a state that
// skipped an event.  The out-of-order frames carry the VALID bytes of the next real event (read from `__security`), so
// only the offset check stands between them and the view.  Against ONE real authenticating BrokerNode, the frames handed
// to the control thread's real handler (BrokerNode::injectSecurityFrameOnControlForTest).
// RED: drop the halt on an undecodable frame in BrokerNode::onSecurityApplied_ (truncated rows), or the offset check in
// SecurityView::onApplied (the jump applies; the repeat is applied over the view).
namespace {
/// The value bytes of `__security` record `offset` (read on the admin thread; empty: none).
std::vector<uint8_t> securityEventBytes(BrokerNode& n, int64_t offset) {
    return n.onAdminThreadForTest([&n, offset]() -> std::vector<uint8_t> {
        const std::vector<uint8_t> data = n.securityLogForTest()->read(offset, 1 << 22);
        std::vector<uint8_t> out;
        std::size_t pos = 0;
        while (out.empty() && pos < data.size()) {
            rune::protocol::RecordBatchReader rd(data.data() + pos, data.size() - pos);
            if (!rd.ensureValid()) break;
            rd.forEachRecord([&](std::string_view, std::string_view val, int64_t off, int64_t) {
                if (off == offset && out.empty())
                    out.assign(reinterpret_cast<const uint8_t*>(val.data()), reinterpret_cast<const uint8_t*>(val.data()) + val.size());
            });
            if (rd.totalLength() <= 0) break;
            pos += static_cast<std::size_t>(rd.totalLength());
        }
        return out;
    });
}
/// The code of the first session-level ErrorResponse (correlation id 0) of `sid` (bounded wait; -1: none).
int sessionLevelCode(AuthRig& r, int64_t sid) {
    int code = -1;
    r.c().pollUntil([&] {
        for (const auto& e : r.c().errors()) if (e.correlationId == 0 && e.sessionId == sid) { code = e.code; return true; }
        return false;
    });
    return code;
}
std::vector<uint8_t> appliedFrame(int64_t offset, const std::vector<uint8_t>& event) {
    std::vector<uint8_t> f(kSecurityAppliedFixedBytes + event.size());
    writeSecurityApplied(f.data(), offset, event);
    return f;
}
}  // namespace

TEST(SecurityAppliedRegression, MalformedOrSkippedAppliedFramesNeverAdvanceTheEnforcedOffsetRegression) {
    enum class Kind { Empty, OffsetOnly, ShortOffset, NegativeOffset, Repeat, Jump };
    struct Case { Kind kind; const char* name; };
    const std::vector<Case> cases = {
        {Kind::Empty, "empty frame"}, {Kind::OffsetOnly, "offset and no event (8 bytes)"},
        {Kind::ShortOffset, "truncated inside the offset (5 bytes)"}, {Kind::NegativeOffset, "negative offset"},
        {Kind::Repeat, "an applied offset again (moves backwards), the next event's bytes"},
        {Kind::Jump, "offset applied + 2 (an event skipped), the next event's bytes"},
    };
    int n = 0;
    for (const Case& c : cases) {
        SCOPED_TRACE(c.name);
        AuthRig r("secapplied" + std::to_string(n++));
        BrokerNode& node = r.broker().node();
        const int64_t ws = r.newUser({exact(SO::Write, ST::Topic, "orders")});
        ASSERT_EQ(r.produce(ws, r.orders, /*untilOk=*/true), 0);
        const int64_t before = node.securityAppliedOffsetOnControlForTest();
        ASSERT_GE(before, 1);
        const int64_t enforcedBefore = node.securityGateForTest().gatewayEnforced;
        ASSERT_FALSE(node.securityViewForTest().halted);

        std::vector<uint8_t> frame;
        switch (c.kind) {
            case Kind::Empty: break;
            case Kind::OffsetOnly: frame = appliedFrame(before + 1, {}); break;
            case Kind::ShortOffset: frame = {1, 2, 3, 4, 5}; break;
            case Kind::NegativeOffset: frame = appliedFrame(-5, securityEventBytes(node, before)); break;
            case Kind::Repeat:
            case Kind::Jump: {
                // The next real event: committed and applied by the admin thread, its mirror frame held on the control
                // thread (so the view stays at `before`); its DDL OK is gated on the view and never released here.
                node.holdSecurityMirrorForTest(true);
                pc::SecurityDdl m;
                m.sessionId = r.broker().adminSession(); m.kind = pc::SecurityOpKind::CreateRole; m.name = "next-event";
                (void)r.broker().admin().securityDdl(r.ch(), m, "CREATE ROLE next-event;", 1s);
                std::vector<uint8_t> next;
                ASSERT_TRUE(r.c().pollFor(10s, [&] { next = securityEventBytes(node, before + 1); return !next.empty(); }))
                    << "the next event was not appended";
                ASSERT_EQ(node.securityAppliedOffsetOnControlForTest(), before) << "precondition: the mirror frame is held";
                frame = appliedFrame(c.kind == Kind::Repeat ? before : before + 2, next);
                break;
            }
        }
        node.injectSecurityFrameOnControlForTest(static_cast<uint16_t>(kA2cSecurityApplied), frame);

        const auto v = node.securityViewForTest();
        EXPECT_TRUE(v.halted) << "the control thread's view kept serving after a frame it cannot apply in order";
        EXPECT_EQ(v.appliedOffset, before) << "the view's applied offset moved";
        EXPECT_EQ(node.securityGateForTest().gatewayEnforced, enforcedBefore) << "the gateway's enforced offset moved";
        EXPECT_EQ(sessionLevelCode(r, ws), static_cast<int>(pc::ErrorCode::AuthUnavailable))
            << "a live session was not ended (fail closed, retriably)";
        EXPECT_TRUE(r.awaitSessionGone(ws));
        if (c.kind == Kind::Repeat || c.kind == Kind::Jump) {
            node.holdSecurityMirrorForTest(false);   // the held, in-order frame now meets the halted view
            EXPECT_TRUE(node.securityViewForTest().halted) << "a later in-order frame un-halted the view";
            EXPECT_EQ(node.securityViewForTest().appliedOffset, before);
        }
    }
}

// PR #604 adversarial review (input bounds, the in-order frame): the cases above reach the halt through the frame's offset
// (a decode failure, a gap, a repeat).  Here the offset is exactly the NEXT one - the gap check passes - and the event
// bytes are the real next event's, truncated at a sweep of byte offsets (1, 2, 4, 8, half, all but two, all but one).
// The view can only reject the event itself: it must not apply a prefix of it.  Each: the view halts at the same applied
// offset, the gateway's enforced offset does not move, and the live session is ended retriably (48).
// RED: answer a SecurityView::onApplied error in BrokerNode::onSecurityApplied_ without failSecurityClosed_ (the live
// session is never ended: the broker keeps serving a view that skipped an event).
TEST(SecurityAppliedAdversarialRegression, AnInOrderFrameTruncatedInsideItsEventFailsClosedAdversarialRegression) {
    // The truncation points, resolved against the next event's length once it is known.
    const std::vector<std::pair<const char*, std::function<std::size_t(std::size_t)>>> cuts = {
        {"1 byte", [](std::size_t) { return std::size_t{1}; }},
        {"2 bytes", [](std::size_t) { return std::size_t{2}; }},
        {"4 bytes", [](std::size_t) { return std::size_t{4}; }},
        {"8 bytes", [](std::size_t) { return std::size_t{8}; }},
        {"half", [](std::size_t n) { return n / 2; }},
        {"all but two", [](std::size_t n) { return n - 2; }},
        {"all but one", [](std::size_t n) { return n - 1; }},
    };
    int n = 0;
    for (const auto& [name, cut] : cuts) {
        SCOPED_TRACE(name);
        AuthRig r("secappliedcut" + std::to_string(n++));
        BrokerNode& node = r.broker().node();
        const int64_t ws = r.newUser({exact(SO::Write, ST::Topic, "orders")});
        ASSERT_EQ(r.produce(ws, r.orders, /*untilOk=*/true), 0);
        const int64_t before = node.securityAppliedOffsetOnControlForTest();
        ASSERT_GE(before, 1);
        const int64_t enforcedBefore = node.securityGateForTest().gatewayEnforced;
        node.holdSecurityMirrorForTest(true);
        pc::SecurityDdl m;
        m.sessionId = r.broker().adminSession(); m.kind = pc::SecurityOpKind::CreateRole; m.name = "next-event";
        (void)r.broker().admin().securityDdl(r.ch(), m, "CREATE ROLE next-event;", 1s);
        std::vector<uint8_t> next;
        ASSERT_TRUE(r.c().pollFor(10s, [&] { next = securityEventBytes(node, before + 1); return !next.empty(); }))
            << "the next event was not appended";
        ASSERT_GE(next.size(), 16u) << "an event shorter than the sweep";
        ASSERT_EQ(node.securityAppliedOffsetOnControlForTest(), before) << "precondition: the mirror frame is held";
        next.resize(cut(next.size()));
        node.injectSecurityFrameOnControlForTest(static_cast<uint16_t>(kA2cSecurityApplied), appliedFrame(before + 1, next));

        const auto v = node.securityViewForTest();
        EXPECT_TRUE(v.halted) << "a truncated in-order event was not refused (" << next.size() << " bytes)";
        EXPECT_EQ(v.appliedOffset, before) << "the view applied a truncated event";
        EXPECT_EQ(node.securityGateForTest().gatewayEnforced, enforcedBefore) << "the gateway's enforced offset moved";
        EXPECT_EQ(sessionLevelCode(r, ws), static_cast<int>(pc::ErrorCode::AuthUnavailable))
            << "a live session was not ended (fail closed, retriably)";
        EXPECT_TRUE(r.awaitSessionGone(ws));
        node.holdSecurityMirrorForTest(false);   // the held, intact frame now meets the halted view
        EXPECT_TRUE(node.securityViewForTest().halted) << "the intact frame un-halted the view";
        EXPECT_EQ(node.securityViewForTest().appliedOffset, before);
    }
}
