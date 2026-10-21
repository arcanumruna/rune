// SecurityAnswerGateTest.cpp — authentication Task 10 (D24, review focus 4): a security DDL's OK leaves only once THIS
// broker enforces the event.
#include <rune/node/SecurityAnswerGate.h>

#include <gtest/gtest.h>

#include <set>
#include <vector>

namespace pc = rune::protocol::client;
using rune::node::SecurityAnswerGate;

namespace {
struct Sent { int64_t session; std::vector<uint8_t> frame; };

std::vector<uint8_t> okFrame(int64_t session, int64_t corr) {
    pc::DdlResponse r;
    r.sessionId = session; r.correlationId = corr; r.errorCode = 0; r.objectId = 9; r.newVersion = 1;
    std::vector<uint8_t> b(r.encodedSize());
    b.resize(r.encode(b.data()));
    return b;
}
}  // namespace

TEST(SecurityAnswerGateRegression, OkIsReleasedOnlyAfterThisBrokerEnforcesItRegression) {
    SecurityAnswerGate gate;
    std::vector<Sent> sent;
    auto send = [&](int64_t s, const uint8_t* f, std::size_t n) { sent.push_back({s, std::vector<uint8_t>(f, f + n)}); };
    const auto frame = okFrame(7, 42);
    gate.hold(7, /*offset=*/5, frame, send);
    EXPECT_TRUE(sent.empty());
    EXPECT_EQ(gate.release(4, send), 0) << "applied through 4: the event at 5 is not enforced yet";
    EXPECT_TRUE(sent.empty());
    EXPECT_EQ(gate.release(5, send), 1) << "applied-and-refreshed through 5";
    ASSERT_EQ(sent.size(), 1u);
    EXPECT_EQ(sent[0].session, 7);
    EXPECT_EQ(sent[0].frame, frame) << "the OK, byte for byte";
    EXPECT_EQ(gate.release(99, send), 0) << "exactly once";
    EXPECT_EQ(sent.size(), 1u);
    EXPECT_TRUE(gate.empty());
}

TEST(SecurityAnswerGateRegression, AnswersAreReleasedInTheOrderTheyWereHeldAndOnlyTheEnforcedOnes) {
    SecurityAnswerGate gate;
    std::vector<Sent> sent;
    auto send = [&](int64_t s, const uint8_t* f, std::size_t n) { sent.push_back({s, std::vector<uint8_t>(f, f + n)}); };
    gate.hold(1, 3, okFrame(1, 10), send);
    gate.hold(2, 8, okFrame(2, 20), send);
    gate.hold(3, 4, okFrame(3, 30), send);
    EXPECT_EQ(gate.release(4, send), 2);
    ASSERT_EQ(sent.size(), 2u);
    EXPECT_EQ(sent[0].session, 1);
    EXPECT_EQ(sent[1].session, 3);
    EXPECT_EQ(gate.size(), 1u);
    EXPECT_EQ(gate.release(8, send), 1);
    EXPECT_EQ(sent[2].session, 2);
}

TEST(SecurityAnswerGateRegression, FullGateAnswersInternalNeverDrops) {
    SecurityAnswerGate gate;
    std::vector<Sent> sent;
    auto send = [&](int64_t s, const uint8_t* f, std::size_t n) { sent.push_back({s, std::vector<uint8_t>(f, f + n)}); };
    for (std::size_t i = 0; i < SecurityAnswerGate::kSlots; ++i) gate.hold(1, 1000 + static_cast<int64_t>(i), okFrame(1, static_cast<int64_t>(i)), send);
    EXPECT_TRUE(sent.empty());
    gate.hold(5, 2000, okFrame(5, 777), send);   // one too many
    ASSERT_EQ(sent.size(), 1u) << "answered at once - never dropped, never blocking";
    const auto r = pc::DdlResponse::decode(sent[0].frame.data(), sent[0].frame.size());
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(r->errorCode, static_cast<uint16_t>(pc::ErrorCode::Internal));
    EXPECT_EQ(r->correlationId, 777) << "the answer to THAT request";
    EXPECT_EQ(sent[0].session, 5);
    EXPECT_EQ(gate.size(), SecurityAnswerGate::kSlots);
    EXPECT_EQ(gate.internalAnswers(), 1u);
}

TEST(SecurityAnswerGateRegression, AHaltedMirrorAnswersEveryHeldOkInternal) {
    SecurityAnswerGate gate;
    std::vector<Sent> sent;
    auto send = [&](int64_t s, const uint8_t* f, std::size_t n) { sent.push_back({s, std::vector<uint8_t>(f, f + n)}); };
    gate.hold(1, 3, okFrame(1, 10), send);
    gate.hold(2, 8, okFrame(2, 20), send);
    EXPECT_EQ(gate.failAll("security log halted on this broker", send), 2);
    ASSERT_EQ(sent.size(), 2u);
    for (const auto& s : sent) {
        const auto r = pc::DdlResponse::decode(s.frame.data(), s.frame.size());
        ASSERT_TRUE(r.has_value());
        EXPECT_EQ(r->errorCode, static_cast<uint16_t>(pc::ErrorCode::Internal));
        EXPECT_EQ(r->detail, "security log halted on this broker");
    }
    EXPECT_TRUE(gate.empty());
}

// Issue #120 (RED before the fix: no deadline - an OK whose enforcement a worker never acknowledges, e.g. a worker whose
// op ring is paused by a slow consumer's ack backlog, was held forever and the DDL client saw only its own timeout).
// Past the deadline it is answered Internal "committed at offset N; enforcement on this broker not confirmed" on the
// same correlation id (a retry with the same requestId dedupes); a younger OK stays held.
TEST(SecurityAnswerGateRegression, HeldSecurityOkIsAnsweredInDoubtAfterTheDeadlineRegression) {
    SecurityAnswerGate gate;
    std::vector<Sent> sent;
    auto send = [&](int64_t s, const uint8_t* f, std::size_t n) { sent.push_back({s, std::vector<uint8_t>(f, f + n)}); };
    constexpr int64_t kDeadline = 5'000'000'000;
    gate.hold(7, /*offset=*/12, okFrame(7, 42), send, /*nowNs=*/1'000);
    gate.hold(8, /*offset=*/13, okFrame(8, 43), send, /*nowNs=*/3'000'000'000);
    EXPECT_EQ(gate.release(11, send), 0);
    EXPECT_EQ(gate.expire(1'000 + kDeadline, kDeadline, send), 0) << "exactly at the deadline: still held";
    EXPECT_TRUE(sent.empty());
    EXPECT_EQ(gate.expire(1'001 + kDeadline, kDeadline, send), 1) << "past it: answered in doubt";
    ASSERT_EQ(sent.size(), 1u);
    EXPECT_EQ(sent[0].session, 7);
    const auto r = pc::DdlResponse::decode(sent[0].frame.data(), sent[0].frame.size());
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(r->correlationId, 42) << "on the held request's own correlation id";
    EXPECT_EQ(r->errorCode, static_cast<uint16_t>(pc::ErrorCode::Internal));
    EXPECT_NE(std::string(r->detail).find("committed at offset 12"), std::string::npos) << r->detail;
    EXPECT_NE(std::string(r->detail).find("not confirmed"), std::string::npos) << r->detail;
    EXPECT_EQ(gate.size(), 1u) << "the younger OK stays held";
    EXPECT_EQ(gate.expiredAnswers(), 1u);
    EXPECT_EQ(gate.release(13, send), 1) << "and is released normally once enforced";
    EXPECT_TRUE(gate.empty());
}

// Adversarial review of PR #459 (issue #120): an OK answered in doubt by the deadline is never answered a second time
// when its event is enforced later (one answer per request), and many OKs past the deadline at once are each answered
// exactly once, in a gate that keeps the younger ones in order.
TEST(SecurityAnswerGateRegression, AnExpiredOkIsNeverAnsweredTwiceAdversarialRegression) {
    SecurityAnswerGate gate;
    std::vector<Sent> sent;
    auto send = [&](int64_t s, const uint8_t* f, std::size_t n) { sent.push_back({s, std::vector<uint8_t>(f, f + n)}); };
    constexpr int64_t kDeadline = 1'000;
    for (int i = 0; i < 20; ++i) gate.hold(100 + i, /*offset=*/i, okFrame(100 + i, 500 + i), send, /*nowNs=*/i * 100);
    EXPECT_EQ(gate.expire(1'000 + 1'050, kDeadline, send), 11) << "held at 0..1000 ns: past the deadline at 2050";
    EXPECT_EQ(gate.size(), 9u);
    EXPECT_EQ(gate.release(100, send), 9) << "the rest released once enforced";
    ASSERT_EQ(sent.size(), 20u);
    std::set<int64_t> sessions;
    for (const auto& s : sent) EXPECT_TRUE(sessions.insert(s.session).second) << "session " << s.session << " answered twice";
    EXPECT_EQ(gate.release(1'000, send), 0);
    EXPECT_EQ(gate.expire(INT64_MAX / 2, kDeadline, send), 0);
    EXPECT_EQ(sent.size(), 20u);
}
