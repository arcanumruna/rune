// C4 review of issues #461 / #519 / #613: a transient refusal (AuthUnavailable 48, or a rate-limited 44) on a connect.
//
// A fake SessionIo captures every frame; the tests play the broker's answers on an injected clock.
#include <rune/client/session/SessionMachine.h>
#include <rune/protocol/client/AuthMessages.h>
#include <rune/protocol/client/ControlMessages.h>

#include <gtest/gtest.h>

#include <memory>
#include <vector>

namespace pc = rune::protocol::client;
namespace sm = rune::client::session;

namespace {

struct CapturingIo : sm::SessionIo {
    std::vector<std::vector<uint8_t>> frames;
    bool sendFrame(const uint8_t* b, std::size_t n) override { frames.emplace_back(b, b + n); return true; }
    bool retarget(const std::string&) noexcept override { return true; }
    [[nodiscard]] int64_t lastCorrelation() const {
        return pc::MessageHeader::decode(frames.back().data(), frames.back().size())->correlationId;
    }
    [[nodiscard]] int countOf(pc::TemplateId t) const {
        int n = 0;
        for (const auto& f : frames) {
            if (pc::MessageHeader::decode(f.data(), f.size())->templateId == static_cast<uint16_t>(t)) ++n;
        }
        return n;
    }
};

constexpr int16_t kUnavailable = static_cast<int16_t>(pc::ErrorCode::AuthUnavailable);

struct Rig {
    CapturingIo io;
    sm::SessionConfig cfg;
    std::unique_ptr<sm::SessionMachine> m;
    explicit Rig(bool revivedOnDemand = false) {
        cfg.username = "ws1"; cfg.password = "correct horse battery";
        cfg.allowUnauthenticatedBroker = true;   // the Live leg needs no login: the refusals are what is tested
        cfg.revivedOnDemand = revivedOnDemand;
        m = std::make_unique<sm::SessionMachine>(io, cfg);
    }
    /// Answers the pending Connect with a login-required ConnectResponse: the machine goes Authenticating.
    void answerAuthRequired(int64_t now) {
        const int64_t corr = io.lastCorrelation();
        m->onConnectResponse(0, -1, 42, corr, now, 0, {}, pc::AuthMode::Required, pc::AuthState::MustAuthenticate);
    }
};

} // namespace

// Finding 4: the transient-refusal budget used to apply only before the session was ever Live.  A session that was Live,
// dropped, and is connect()ed again by the app is a connect like any other: a 48 or a rate-limited 44 is retried with
// Backoff (honouring the retry-after), never an immediate Disconnected that loses the retry.
TEST(SessionTransientRetryRegression, AReconnectAfterLiveRetriesATransientRefusalRegression) {
    for (const bool rateLimited : {false, true}) {
        SCOPED_TRACE(rateLimited ? "rate-limited 44" : "48 on the ConnectResponse");
        Rig t;
        t.m->connect(0);
        t.m->onConnectResponse(0, -1, 42, t.io.lastCorrelation(), 1);
        ASSERT_EQ(t.m->state(), sm::SessionState::Connected);
        ASSERT_TRUE(t.m->everLive());
        t.m->onErrorResponse(kUnavailable, "stale", /*corr=*/0, 2, /*noticeSessionId=*/42);   // the broker ends it
        ASSERT_EQ(t.m->state(), sm::SessionState::Disconnected) << "a plain session: the app reconnects";
        int64_t now = 1'000'000'000;
        t.m->connect(now);
        ASSERT_EQ(t.m->state(), sm::SessionState::Connecting);
        if (rateLimited) {
            t.answerAuthRequired(++now);
            ASSERT_EQ(t.m->state(), sm::SessionState::Authenticating);
            t.m->onAuthServerFinal(pc::AuthServerFinal{.errorCode = 44, .retryAfterMs = 300}, now);
        } else {
            t.m->onConnectResponse(kUnavailable, -1, 0, t.io.lastCorrelation(), ++now);
        }
        ASSERT_EQ(t.m->state(), sm::SessionState::Backoff) << "a transient refusal on a re-connect is retried";
        EXPECT_TRUE(t.m->retryingTransientRefusal());
        EXPECT_EQ(t.m->rejectCode(), 0);
        if (rateLimited) EXPECT_GE(t.m->nextAttemptNs(), now + 300'000'000LL) << "the retry-after is honoured";
        now = t.m->nextAttemptNs();
        t.m->tick(now);
        ASSERT_EQ(t.m->state(), sm::SessionState::Connecting) << "the retry is a fresh attempt";
        t.m->onConnectResponse(0, -1, 43, t.io.lastCorrelation(), ++now);
        EXPECT_EQ(t.m->state(), sm::SessionState::Connected) << "the retry recovers";
        EXPECT_EQ(t.m->unavailableRetries(), 0) << "Live renews the budget";
    }
}

// Finding 5: a node session (revived on demand: the client reconnects a Disconnected one every duty cycle) answered 48
// past its budget stays in a growing Backoff - never Disconnected, which the per-cycle revive would turn into a
// reconnect storm against the refusing broker.  An app-connected session still ends Disconnected (control).
TEST(SessionTransientRetryRegression, ANodeSessionAnswered48ForeverIsPacedNeverStormsRegression) {
    for (const bool revivedOnDemand : {true, false}) {
        SCOPED_TRACE(revivedOnDemand ? "node session" : "app-connected (control)");
        Rig t(revivedOnDemand);
        int64_t now = 1'000'000'000;
        t.m->connect(now);
        const int rounds = 3 * t.cfg.maxUnavailableRetries + 5;
        int64_t lastDelay = 0;
        bool ended = false;
        for (int i = 0; i < rounds; ++i) {
            ASSERT_EQ(t.m->state(), sm::SessionState::Connecting) << i;
            t.m->onConnectResponse(kUnavailable, -1, 0, t.io.lastCorrelation(), ++now);
            if (t.m->state() == sm::SessionState::Disconnected) { ended = true; break; }
            ASSERT_EQ(t.m->state(), sm::SessionState::Backoff) << i;
            const int64_t delay = t.m->nextAttemptNs() - now;
            EXPECT_GE(delay, lastDelay) << i << ": the backoff never shrinks";
            EXPECT_GT(delay, 0) << i;
            lastDelay = delay;
            // Nothing is sent before the backoff elapses, however often the agent ticks.
            const int sent = t.io.countOf(pc::TemplateId::Connect);
            for (int k = 0; k < 10; ++k) t.m->tick(now + (delay - 1) * k / 10);
            ASSERT_EQ(t.io.countOf(pc::TemplateId::Connect), sent) << i << ": a retry inside the backoff";
            now = t.m->nextAttemptNs();
            t.m->tick(now);
        }
        if (revivedOnDemand) {
            EXPECT_FALSE(ended) << "a node session must never go Disconnected on 48 (the revive would storm)";
            EXPECT_EQ(t.io.countOf(pc::TemplateId::Connect), rounds + 1) << "one ConnectRequest per backoff";
            EXPECT_EQ(t.m->rejectCode(), 0);
        } else {
            EXPECT_TRUE(ended) << "an app-connected session spends its budget and ends";
            EXPECT_EQ(t.m->lastConnectError(), kUnavailable);
        }
    }
}

// Finding 8: an ErrorResponse 48 to the login (its correlation id, while Authenticating) used to be ignored - the login
// waited out the whole handshake timeout.  It is the same transient refusal as an AuthServerFinal{48}: Backoff, retried.
TEST(SessionTransientRetryRegression, AnErrorResponse48WhileAuthenticatingIsRetriedAtOnceRegression) {
    Rig t;
    int64_t now = 1'000'000'000;
    t.m->connect(now);
    t.answerAuthRequired(++now);
    ASSERT_EQ(t.m->state(), sm::SessionState::Authenticating);
    t.m->onErrorResponse(kUnavailable, "security state catching up", t.m->correlationId(), ++now, 42);
    ASSERT_EQ(t.m->state(), sm::SessionState::Backoff) << "the 48 was ignored: the login waits out its timeout";
    EXPECT_EQ(t.m->lastConnectError(), kUnavailable);
    EXPECT_TRUE(t.m->retryingTransientRefusal());
    EXPECT_EQ(t.m->rejectCode(), 0);
    t.m->tick(t.m->nextAttemptNs());
    EXPECT_EQ(t.m->state(), sm::SessionState::Connecting) << "a fresh attempt";
}

// Finding 6 (machine half): a caller that gave up at its deadline stops the paced retry; nothing reconnects behind it.
// A reconnect-forever session that was Live is never stopped this way.
TEST(SessionTransientRetryRegression, AbandonTransientRetryStopsOnlyAConnectsRetryRegression) {
    {
        Rig t;
        int64_t now = 1'000'000'000;
        t.m->connect(now);
        t.m->onConnectResponse(kUnavailable, -1, 0, t.io.lastCorrelation(), ++now);
        ASSERT_EQ(t.m->state(), sm::SessionState::Backoff);
        EXPECT_TRUE(t.m->abandonTransientRetry());
        EXPECT_EQ(t.m->state(), sm::SessionState::Disconnected);
        EXPECT_EQ(t.m->lastConnectError(), kUnavailable) << "the verdict is kept for the report";
        const int sent = t.io.countOf(pc::TemplateId::Connect);
        t.m->tick(now + 60'000'000'000LL);
        EXPECT_EQ(t.io.countOf(pc::TemplateId::Connect), sent) << "no retry behind the failed connect";
    }
    {   // not retrying a transient refusal: nothing to abandon
        Rig t;
        t.m->connect(1);
        EXPECT_FALSE(t.m->abandonTransientRetry());
        EXPECT_EQ(t.m->state(), sm::SessionState::Connecting);
    }
    {   // reconnect-forever after Live: its own unbounded Backoff, never abandoned
        Rig t;
        t.cfg.reconnectForeverAfterLive = true;
        t.m = std::make_unique<sm::SessionMachine>(t.io, t.cfg);
        t.m->connect(0);
        t.m->onConnectResponse(0, -1, 42, t.io.lastCorrelation(), 1);
        t.m->onErrorResponse(kUnavailable, "stale", 0, 2, 42);
        ASSERT_EQ(t.m->state(), sm::SessionState::Backoff);
        EXPECT_FALSE(t.m->abandonTransientRetry());
        EXPECT_EQ(t.m->state(), sm::SessionState::Backoff);
    }
}
