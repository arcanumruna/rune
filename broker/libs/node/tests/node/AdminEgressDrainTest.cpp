// AdminEgressDrainTest.cpp - issue #209: the admin egress drain routes every frame on its own; a catalog DDL OK held by
// the DdlAnswerGate never delays a gated security OK, catalog answers stay FIFO, and a nearly full gate stops the drain
// (frames stay in the ring) instead of overflowing it.  Real ring (OneToOneByteRing) + real DdlAnswerGate.
#include <rune/node/AdminChannels.h>
#include <rune/node/AdminEgressDrain.h>
#include <rune/node/DdlAnswerGate.h>
#include <rune/node/OneToOneByteRing.h>
#include <rune/protocol/client/ControlMessages.h>
#include <rune/protocol/client/SchemaMessages.h>

#include <gtest/gtest.h>

#include <vector>

namespace pc = rune::protocol::client;
using rune::node::DdlAnswerGate;
using rune::node::OneToOneByteRing;

namespace {

struct Sent { int64_t session; uint16_t tid; int64_t corr; };
struct HeldSec { int64_t session; int64_t offset; std::size_t n; };

std::vector<uint8_t> withSession(int64_t session, const std::vector<uint8_t>& frame, int64_t offset = -1) {
    std::vector<uint8_t> b(8 + (offset >= 0 ? 8 : 0) + frame.size());
    std::memcpy(b.data(), &session, 8);
    std::size_t at = 8;
    if (offset >= 0) { std::memcpy(b.data() + 8, &offset, 8); at = 16; }
    std::memcpy(b.data() + at, frame.data(), frame.size());
    return b;
}
std::vector<uint8_t> defineOk(int64_t session, int64_t corr, int32_t topic) {
    pc::DefineTopicResponse r;
    r.sessionId = session; r.correlationId = corr; r.topicId = topic; r.version = 1; r.errorCode = 0;
    std::vector<uint8_t> b(r.encodedSize()); b.resize(r.encode(b.data())); return b;
}
std::vector<uint8_t> errorAnswer(int64_t session, int64_t corr) {
    pc::ErrorResponse e;
    e.sessionId = session; e.correlationId = corr; e.code = static_cast<int16_t>(pc::ErrorCode::InvalidRequest);
    e.message = "no";
    std::vector<uint8_t> b(256); b.resize(e.encode(b.data())); return b;
}
std::vector<uint8_t> securityOk(int64_t session, int64_t corr) {
    pc::DdlResponse r;
    r.sessionId = session; r.correlationId = corr; r.errorCode = 0; r.objectId = 9; r.newVersion = 1;
    std::vector<uint8_t> b(r.encodedSize()); b.resize(r.encode(b.data())); return b;
}

struct Harness {
    OneToOneByteRing ring;
    DdlAnswerGate gate;
    bool topic7Serves = false;
    std::vector<Sent> sent;
    std::vector<HeldSec> security;
    Harness(int ringBytes, std::size_t gateBytes) : ring(ringBytes), gate(gateBytes, 5'000'000'000LL) {}
    struct SendFn {
        Harness* h;
        void operator()(int64_t s, const uint8_t* f, std::size_t n) const {
            const auto m = pc::MessageHeader::decode(f, n);
            h->sent.push_back({s, m ? m->templateId : uint16_t{0}, m ? m->correlationId : 0});
        }
    };
    [[nodiscard]] SendFn sendFn() { return SendFn{this}; }
    void put(const std::vector<uint8_t>& b, int32_t tid = rune::node::kMsgEgressFrame) {
        ASSERT_TRUE(ring.write(b.data(), b.size(), tid));
    }
    int drain(int64_t now = 0) {
        return rune::node::drainAdminEgress(
            ring, gate, 16, now, [this](int32_t t) { return t != 7 || topic7Serves; }, sendFn(),
            [this](int64_t s, int64_t off, std::span<const uint8_t> f) { security.push_back({s, off, f.size()}); });
    }
    int release(int64_t now = 0) {
        return gate.release(now, [this](int32_t t) { return t != 7 || topic7Serves; }, sendFn(),
                            [](int64_t, int32_t) {});
    }
    [[nodiscard]] int64_t ringBytes() const { return ring.producerPosition() - ring.consumerPosition(); }
};

}  // namespace

TEST(AdminEgressRegression, AHeldCatalogAnswerDoesNotDelayASecurityOkRegression) {
    Harness h(64 * 1024, 64 * 1024);
    h.put(withSession(1, defineOk(1, 10, 7)));
    EXPECT_EQ(h.drain(), 1);
    ASSERT_EQ(h.gate.heldCount(), 1u) << "the OK waits for topic 7";
    const auto sec = securityOk(2, 20);
    h.put(withSession(2, sec, /*offset=*/42), rune::node::kMsgEgressFrameGated);
    EXPECT_EQ(h.drain(), 1) << "the security frame is drained behind a held catalog OK";
    ASSERT_EQ(h.security.size(), 1u);
    EXPECT_EQ(h.security[0].session, 2);
    EXPECT_EQ(h.security[0].offset, 42);
    EXPECT_EQ(h.security[0].n, sec.size());
    EXPECT_EQ(h.gate.heldCount(), 1u);
    EXPECT_TRUE(h.sent.empty());
    EXPECT_EQ(h.ringBytes(), 0);
}

TEST(AdminEgressRegression, CatalogAnswersStayFifoBehindAHeldOneRegression) {
    Harness h(64 * 1024, 64 * 1024);
    h.put(withSession(1, defineOk(1, 10, 7)));
    h.put(withSession(3, errorAnswer(3, 30)));
    EXPECT_EQ(h.drain(), 2);
    EXPECT_EQ(h.gate.heldCount(), 2u) << "the plain answer queues behind the held OK";
    EXPECT_TRUE(h.sent.empty());
    EXPECT_EQ(h.release(), 0) << "topic 7 does not serve yet";
    h.topic7Serves = true;
    EXPECT_EQ(h.release(), 2);
    ASSERT_EQ(h.sent.size(), 2u);
    EXPECT_EQ(h.sent[0].corr, 10);
    EXPECT_EQ(h.sent[0].tid, pc::DefineTopicResponse::TEMPLATE_ID);
    EXPECT_EQ(h.sent[1].corr, 30);
    EXPECT_EQ(h.sent[1].session, 3);
}

TEST(AdminEgressRegression, ANearlyFullGateStopsTheDrainWithoutDroppingRegression) {
    // A 1 KiB ring (maxMsgLength 128): the drain needs 2 * (128 + 32) = 320 free bytes once the gate holds anything.
    // The gate takes one held record (~56 B) and is then below that room.
    constexpr std::size_t kGate = 2 * (128 + 32) + 40;
    Harness h(1024, kGate);
    ASSERT_EQ(h.ring.maxMsgLength(), 128);
    h.put(withSession(1, defineOk(1, 10, 7)));
    for (int i = 0; i < 4; ++i) h.put(withSession(4, errorAnswer(4, 40 + i)));
    const int64_t before = h.ringBytes();
    EXPECT_EQ(h.drain(), 1) << "the held head leaves the gate below the room of one more frame";
    EXPECT_EQ(h.gate.heldCount(), 1u);
    EXPECT_LT(h.gate.freeBytes(), 2u * (128u + 32u));
    EXPECT_GT(h.ringBytes(), 0);
    EXPECT_LT(h.ringBytes(), before);
    const int64_t waiting = h.ringBytes();
    EXPECT_EQ(h.drain(), 0) << "still no room: nothing read";
    EXPECT_EQ(h.ringBytes(), waiting) << "the frames stay in the ring";
    EXPECT_TRUE(h.sent.empty()) << "never the gate's Full path (an ungated send)";
    h.topic7Serves = true;
    EXPECT_EQ(h.release(), 1);
    EXPECT_EQ(h.drain(), 4) << "after the release the next drain delivers them";
    ASSERT_EQ(h.sent.size(), 5u);
    for (int i = 0; i < 5; ++i) EXPECT_EQ(h.sent[static_cast<std::size_t>(i)].corr, i == 0 ? 10 : 39 + i) << i;
    EXPECT_EQ(h.ringBytes(), 0);
}

// ── PR #611 review, adversarial pass (issue #209) ────────────────────────────────────────────────────────────────────

// Resources: a burst of 40 gated security OKs (more than one pass's 16-frame budget) behind a catalog OK held for its
// whole life: every one reaches the security gate within ceil(41 / 16) passes, in ring order, while the catalog OK is
// still held.
TEST(AdminEgressRegression, ABurstOfSecurityOksBehindAHeldCatalogAnswerAllPassAdversarialRegression) {
    Harness h(256 * 1024, 512 * 1024);   // the gate takes 2 x (maxMsgLength 32 KiB + 32) beyond what it holds
    h.put(withSession(1, defineOk(1, 10, 7)));
    for (int i = 0; i < 40; ++i) h.put(withSession(100 + i, securityOk(100 + i, 1000 + i), /*offset=*/i), rune::node::kMsgEgressFrameGated);
    int passes = 0;
    while (h.ringBytes() > 0 && passes < 10) { (void)h.drain(); ++passes; }
    EXPECT_EQ(passes, 3) << "16 frames per pass, never stalled by the held OK";
    ASSERT_EQ(h.security.size(), 40u);
    for (int i = 0; i < 40; ++i) {
        EXPECT_EQ(h.security[static_cast<std::size_t>(i)].session, 100 + i) << i;
        EXPECT_EQ(h.security[static_cast<std::size_t>(i)].offset, i) << i;
    }
    EXPECT_EQ(h.gate.heldCount(), 1u);
    EXPECT_TRUE(h.sent.empty());
}

// Resources + ordering: a security OK that sits behind catalog frames a nearly full gate cannot take stays in the ring
// (never dropped, never sent ungated, never reordered ahead of the catalog frames in front of it); after the release
// the catalog answers leave in FIFO order and the security OK reaches its gate.
TEST(AdminEgressRegression, ASecurityOkBehindAFullGateWaitsInTheRingAndIsNeverLostAdversarialRegression) {
    constexpr std::size_t kGate = 2 * (128 + 32) + 40;
    Harness h(1024, kGate);
    h.put(withSession(1, defineOk(1, 10, 7)));
    h.put(withSession(4, errorAnswer(4, 40)));
    const auto sec = securityOk(5, 50);
    h.put(withSession(5, sec, /*offset=*/77), rune::node::kMsgEgressFrameGated);
    EXPECT_EQ(h.drain(), 1);
    EXPECT_EQ(h.drain(), 0) << "no room: the error answer and the security OK behind it wait";
    EXPECT_TRUE(h.security.empty());
    EXPECT_TRUE(h.sent.empty());
    h.topic7Serves = true;
    EXPECT_EQ(h.release(), 1);
    EXPECT_EQ(h.drain(), 2);
    ASSERT_EQ(h.sent.size(), 2u);
    EXPECT_EQ(h.sent[0].corr, 10);
    EXPECT_EQ(h.sent[1].corr, 40);
    ASSERT_EQ(h.security.size(), 1u);
    EXPECT_EQ(h.security[0].session, 5);
    EXPECT_EQ(h.security[0].offset, 77);
    EXPECT_EQ(h.security[0].n, sec.size());
    EXPECT_EQ(h.ringBytes(), 0);
}

// Input bounds: truncated frames (a catalog frame under the 8-byte session prefix, a gated frame without a whole
// header) are consumed and dropped; they never wedge the ring nor reach either gate, and the frames behind them pass.
TEST(AdminEgressRegression, TruncatedFramesAreDroppedWithoutBlockingTheRingAdversarialRegression) {
    Harness h(64 * 1024, 64 * 1024);
    h.put(withSession(1, defineOk(1, 10, 7)));   // held
    for (std::size_t n = 1; n < 8; ++n) h.put(std::vector<uint8_t>(n, 0xAB));
    const auto sec = securityOk(2, 20);
    for (std::size_t n = 8; n < 16 + pc::MessageHeader::ENCODED_SIZE; ++n) {
        std::vector<uint8_t> b = withSession(2, sec, /*offset=*/1);
        b.resize(n);
        h.put(b, rune::node::kMsgEgressFrameGated);
    }
    h.put(withSession(2, sec, /*offset=*/9), rune::node::kMsgEgressFrameGated);
    int passes = 0;
    while (h.ringBytes() > 0 && passes < 20) { (void)h.drain(); ++passes; }
    EXPECT_EQ(h.ringBytes(), 0);
    ASSERT_EQ(h.security.size(), 1u) << "only the whole gated frame reaches the security gate";
    EXPECT_EQ(h.security[0].offset, 9);
    EXPECT_EQ(h.gate.heldCount(), 1u) << "no truncated frame reached the catalog gate";
    EXPECT_TRUE(h.sent.empty());
}
