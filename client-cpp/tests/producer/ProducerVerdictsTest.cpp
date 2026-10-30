// tests/producer/ProducerVerdictsTest.cpp - checked writes Part 04: a View ack's verdict trailer completes each record
// as it was judged; a retry outside the broker's window is VerdictUnknown for the whole batch.  Bare ProducerImpl: frames
// are captured from the sink and the broker's answers are injected.
#include <rune/client/Client.h>
#include <rune/client/Key.h>
#include <rune/client/codec/DataCodec.h>
#include <rune/client/producer/ProducerImpl.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/protocol/client/Protocol.h>

#include <gtest/gtest.h>

#include <cstdint>
#include <map>
#include <vector>

namespace rc  = rune::client;
namespace rpc = rune::protocol::client;
using rc::Acks;
using rc::OpStatus;
using rc::producer::ProducerImpl;
namespace ackhandle = rune::client::producer::ackhandle;

namespace {
constexpr int64_t kNow = 1'000'000'000;
constexpr uint32_t kCid = 0x00070001;
constexpr uint16_t kViolation = 55;
const std::vector<uint8_t> kVal(16, 0x11);

struct CqCapture {
    std::map<rc::Token, rc::Completion> done;
    std::map<rc::Token, int> times;
    static void deliver(void* ctx, rc::CqRef, const rc::Completion& c) noexcept {
        auto* self = static_cast<CqCapture*>(ctx);
        self->done[c.token] = c;
        ++self->times[c.token];
    }
};

struct Rig {
    ProducerImpl impl;
    CqCapture cq;
    std::vector<int64_t> corrs;
    ProducerImpl::SendFn sink = [this](int32_t, int32_t, int32_t, const uint8_t* b, std::size_t n) {
        const auto v = rc::codec::decodeProduce(b, n);
        if (!v) return false;
        corrs.push_back(v->correlationId);
        return true;
    };
    static rc::ProducerConfig cfg(Acks acks) {
        rc::ProducerConfig c;
        c.acks = acks;
        c.lingerUs = 0;
        c.batchBytes = 1 << 20;
        c.maxFrameBytes = 1 << 20;
        c.maxInFlight = 8;
        c.produceTimeoutMs = 60'000;
        c.deliveryTimeoutMs = 120'000;
        return c;
    }
    explicit Rig(Acks acks = Acks::All | Acks::View, bool tier2 = false) : impl(cfg(acks), 1 << 16) {
        ProducerImpl::Hooks h;
        h.ctx = &cq;
        h.cqDeliver = &CqCapture::deliver;
        impl.setHooks(h);
        (void)tier2;
    }
    void send(int n, bool hold = true) {
        for (int i = 0; i < n; ++i)
            EXPECT_TRUE(impl.submitSendRecord(1, 0, rc::Key(uint64_t(i + 1)), kVal, true, ackhandle::forCq(0, rc::Token(i + 1))));
        (void)hold;
        for (int i = 0; i < 1000 && corrs.empty(); ++i) {
            while (impl.drainRing(kNow) != 0) {}
            (void)impl.flushBatches(kNow, 42, sink);
        }
    }
    static std::vector<uint8_t> trailer(std::initializer_list<uint16_t> idx) {
        std::vector<uint8_t> b;
        for (const uint16_t i : idx) {
            b.push_back(uint8_t(i)); b.push_back(uint8_t(i >> 8));
            b.push_back(uint8_t(kViolation)); b.push_back(uint8_t(kViolation >> 8));
            for (int k = 0; k < 4; ++k) b.push_back(uint8_t(kCid >> (8 * k)));
        }
        return b;
    }
};
}  // namespace

TEST(ProducerVerdicts, CoalescedBatchWithOneBadRecord) {
    Rig r;
    r.send(8);
    ASSERT_EQ(r.corrs.size(), 1u);
    const auto t = Rig::trailer({3});
    r.impl.onProduceResponse(r.corrs[0], 100, 0, -1, true, t);
    for (rc::Token k = 1; k <= 8; ++k) {
        ASSERT_EQ(r.cq.times[k], 1) << k;
        const auto& c = r.cq.done[k];
        if (k == 4) {   // record index 3
            EXPECT_EQ(c.status, OpStatus::BrokerError);
            EXPECT_EQ(c.errorCode, kViolation);
            EXPECT_EQ(rc::constraintIdOf(c), kCid);
            EXPECT_EQ(c.value, -1);
        } else {
            EXPECT_EQ(c.status, OpStatus::Ok);
            EXPECT_EQ(c.value, 100 + int64_t(k) - 1);
        }
    }
    EXPECT_FALSE(r.impl.hasInFlight());
}

TEST(ProducerVerdicts, ReplayedVerdictsCompleteEachRecordAsTheOriginal) {
    Rig r;
    r.send(8);
    ASSERT_EQ(r.corrs.size(), 1u);
    // The first ack is lost; the retry's answer carries the same trailer.
    const auto t = Rig::trailer({3});
    r.impl.onProduceResponse(r.corrs[0], 100, 0, -1, true, t);
    r.impl.onProduceResponse(r.corrs[0], 100, 0, -1, true, t);   // a stale duplicate answer is ignored
    for (rc::Token k = 1; k <= 8; ++k) EXPECT_EQ(r.cq.times[k], 1) << "each record completes exactly once";
    EXPECT_EQ(r.cq.done[4].status, OpStatus::BrokerError);
}

TEST(ProducerVerdicts, EmptyTrailerIsPlainOk) {
    Rig r;
    r.send(3);
    r.impl.onProduceResponse(r.corrs[0], 10, 0, -1, true, {});
    for (rc::Token k = 1; k <= 3; ++k) {
        EXPECT_EQ(r.cq.done[k].status, OpStatus::Ok);
        EXPECT_EQ(r.cq.done[k].value, 10 + int64_t(k) - 1);
    }
}

TEST(ProducerVerdicts, DuplicateSequenceOnAViewProducerIsVerdictUnknown) {
    Rig r;
    r.send(8);
    r.impl.onProduceResponse(r.corrs[0], -1, static_cast<int16_t>(rpc::ErrorCode::DuplicateSequence));
    for (rc::Token k = 1; k <= 8; ++k) {
        ASSERT_EQ(r.cq.times[k], 1);
        EXPECT_EQ(r.cq.done[k].status, OpStatus::VerdictUnknown);
        EXPECT_EQ(r.cq.done[k].errorCode, static_cast<uint16_t>(rpc::ErrorCode::DuplicateSequence));
    }
}

TEST(ProducerVerdicts, DuplicateSequenceOnAnAllProducerIsStillOk) {
    Rig r(Acks::All);
    r.send(4);
    r.impl.onProduceResponse(r.corrs[0], -1, static_cast<int16_t>(rpc::ErrorCode::DuplicateSequence));
    for (rc::Token k = 1; k <= 4; ++k) {
        EXPECT_EQ(r.cq.done[k].status, OpStatus::Ok);
        EXPECT_EQ(r.cq.done[k].value, -1);
    }
}

TEST(ProducerVerdicts, OpStatusThirteenIsReservedAndVerdictUnknownIsFourteen) {
    EXPECT_EQ(static_cast<int>(OpStatus::BrokerTooOld), 12);
    EXPECT_EQ(static_cast<int>(OpStatus::StoredIdUnknown), 13);
    EXPECT_EQ(static_cast<int>(OpStatus::VerdictUnknown), 14);
}
