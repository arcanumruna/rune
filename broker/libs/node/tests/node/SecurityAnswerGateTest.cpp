// SecurityAnswerGateTest.cpp — authentication Task 10 (D24, review focus 4): a security DDL's OK leaves only once THIS
// broker enforces the event.
#include <rune/node/SecurityAnswerGate.h>

#include <gtest/gtest.h>

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
