// tests/producer/ProducerVerdictsTest.cpp - checked writes Part 04: a View ack's verdict trailer completes each record
// as it was judged; a retry outside the broker's window is VerdictUnknown for the whole batch.  Bare ProducerImpl: frames
// are captured from the sink and the broker's answers are injected.
#include "../support/FakeCoordinator.h"

#include <rune/client/Client.h>
#include <rune/client/agent/ClientAgent.h>
#include <rune/client/Key.h>
#include <rune/client/codec/DataCodec.h>
#include <rune/client/producer/ProducerHub.h>
#include <rune/client/producer/ProducerImpl.h>
#include <rune/protocol/client/TxnMessages.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/protocol/client/DataMessages.h>
#include <rune/protocol/client/Protocol.h>

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <string>
#include <cstdint>
#include <map>
#include <thread>
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
    std::map<rc::Token, std::vector<uint8_t>> payloads;   // the inline payload, per token (test-only copy)
    static void deliver(void* ctx, rc::CqRef, const rc::Completion& c, std::span<const uint8_t> payload) noexcept {
        auto* self = static_cast<CqCapture*>(ctx);
        self->done[c.token] = c;
        ++self->times[c.token];
        self->payloads[c.token].assign(payload.begin(), payload.end());
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
    explicit Rig(Acks acks = Acks::All | Acks::View, bool tier2 = false) : Rig(cfg(acks)) { (void)tier2; }
    explicit Rig(const rc::ProducerConfig& c, bool direct = false, std::size_t ringBytes = 1 << 16) : impl(c, ringBytes) {
        ProducerImpl::Hooks h;
        h.ctx = &cq;
        h.cqDeliver = &CqCapture::deliver;
        impl.setHooks(h);
        if (direct) impl.enableDirectIngress({std::this_thread::get_id(), nullptr, nullptr, kNow});
    }
    // Pump until one frame is captured (capped).
    void pump() {
        for (int i = 0; i < 1000 && corrs.empty(); ++i) {
            while (impl.drainRing(kNow) != 0) {}
            (void)impl.flushBatches(kNow, 42, sink);
        }
    }
    // One CLAIM of n records of recLen bytes (the values written through the grant, then committed); no pump.
    void claimNoPump(int n, uint64_t handle, uint32_t recLen = 16) {
        const auto g = impl.claimBatch(1, 0, rc::Key(uint64_t{1}), static_cast<uint32_t>(n), recLen, handle);
        ASSERT_EQ(g.result, rc::SendResult::Ok);
        for (int i = 0; i < n; ++i) {
            uint8_t* d = g.offsets != nullptr ? g.base + g.offsets[i] : g.base + static_cast<std::size_t>(i) * recLen;
            std::fill(d, d + recLen, uint8_t{0x22});
        }
        ASSERT_TRUE(impl.commitClaim(g.token));
    }
    void claim(int n, rc::Token tok, uint32_t recLen = 16) {
        claimNoPump(n, ackhandle::forCq(0, tok), recLen);
        pump();
    }
    // One Tier-2 batch of n records (handle `handle`); pumped until a frame is captured.
    void tier2(int n, uint64_t handle) {
        rune::protocol::RecordBatchBuilder b(0, 1000);
        for (int i = 0; i < n; ++i) b.append(std::string_view("k"), std::string_view("v"), 1000);
        const auto bytes = b.build();
        ASSERT_EQ(impl.submitTier2Bytes(1, 0, handle, n, bytes), rc::producer::RejectReason::None);
        pump();
    }
    void submitOne(rc::Token tok) {
        EXPECT_TRUE(impl.submitSendRecord(1, 0, rc::Key(uint64_t(tok)), kVal, true, ackhandle::forCq(0, tok)));
    }
    std::vector<rc::BatchAck> batchAcks;
    void captureBatchAcks() {
        impl.setOnBatchAck([](void* ctx, const rc::BatchAck& a) noexcept { static_cast<Rig*>(ctx)->batchAcks.push_back(a); }, this);
    }
    void send(int n, bool hold = true, int unhandled = -1) {
        for (int i = 0; i < n; ++i)   // record `unhandled` is sent without an ack handle
            EXPECT_TRUE(impl.submitSendRecord(1, 0, rc::Key(uint64_t(i + 1)), kVal, i != unhandled, ackhandle::forCq(0, rc::Token(i + 1))));
        (void)hold;
        for (int i = 0; i < 1000 && corrs.empty(); ++i) {
            while (impl.drainRing(kNow) != 0) {}
            (void)impl.flushBatches(kNow, 42, sink);
        }
    }
    static std::vector<uint8_t> trailer(std::initializer_list<uint16_t> idx) { return trailer(std::vector<uint16_t>(idx)); }
    static std::vector<uint8_t> trailer(const std::vector<uint16_t>& idx) {
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

TEST(ProducerVerdictsRegression, CoalescedBatchWithOneBadRecordRegression) {
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

// (The Tier-2 first-rejection contract changed with #802: see Tier2BatchReportsEveryRecordVerdictRegression.)

TEST(ProducerVerdictsRegression, VerdictArenaGrowsOnceAndIsCountedRegression) {
    namespace rca = rune::client::agent;
    rca::AgentConfig cfg;
    cfg.transport.ingressChannel = "aeron:udp?endpoint=127.0.0.1:1";
    cfg.transport.ingressStreamId = 1;
    cfg.transport.egressChannel = "aeron:udp?endpoint=127.0.0.1:2";
    cfg.transport.egressStreamId = 2;
    cfg.session.egressChannel = cfg.transport.egressChannel;
    cfg.session.egressStreamId = 2;
    rca::ClientAgent a(cfg);
    const auto deliver = [&](int64_t corr, uint16_t n) {
        std::vector<uint8_t> entries(8u * n, 0);
        for (uint16_t i = 0; i < n; ++i) { entries[8u * i] = uint8_t(i); entries[8u * i + 1] = uint8_t(i >> 8); entries[8u * i + 2] = uint8_t(kViolation); }
        rpc::ProduceResponse m;
        m.sessionId = 1; m.correlationId = corr; m.baseOffset = 5; m.hasVerdicts = true; m.verdictCount = n;
        m.verdictBytes = entries;
        std::vector<uint8_t> f(m.encodedSize(rpc::kClientVersionCheckedWrites));
        EXPECT_EQ(m.encodeFor(f.data(), rpc::kClientVersionCheckedWrites), f.size());
        a.dispatchEgressFrameForTest(kNow, f.data(), f.size());
    };
    EXPECT_EQ(a.verdictArenaGrowths(), 0u);
    deliver(1, 100);                       // the arena is reserved for 65,536 verdicts: nothing allocates
    ASSERT_EQ(a.produceAcks().size(), 1u);
    EXPECT_EQ(a.verdictBytes(a.produceAcks()[0]).size(), 800u);
    a.clearProduceAcks();
    for (int64_t c = 2; c < 18; ++c) deliver(c, 4096);
    ASSERT_EQ(a.produceAcks().size(), 16u);
    EXPECT_EQ(a.verdictArenaGrowths(), 0u) << "a cycle of exactly the reserve";
    deliver(18, 1);
    EXPECT_EQ(a.verdictArenaGrowths(), 1u) << "only a cycle above the reserve grows it";
    a.clearProduceAcks();
    deliver(19, 1);
    EXPECT_EQ(a.verdictArenaGrowths(), 1u);
}

TEST(ProducerVerdictsRegression, ReplayedVerdictsCompleteEachRecordAsTheOriginalRegression) {
    Rig r;
    r.send(4);
    ASSERT_EQ(r.corrs.size(), 1u);
    // The first ack is lost: the batch's attempt deadline passes and the SAME batch (same correlation id) is re-sent.
    ASSERT_EQ(r.impl.retransmitStalled(kNow + 60'000LL * 1'000'000 + 1, r.sink), 1);
    ASSERT_EQ(r.corrs.size(), 2u) << "the retransmit put a second frame on the wire";
    EXPECT_EQ(rc::producer::batchcorr::base(r.corrs[0]), rc::producer::batchcorr::base(r.corrs[1]))
        << "the retransmit is the same batch: the same correlation id up to its attempt tag";
    EXPECT_EQ(rc::producer::batchcorr::attempt(r.corrs[1]), rc::producer::batchcorr::attempt(r.corrs[0]) + 1);
    // The retry's answer carries the original trailer; every record completes as it would have the first time.
    const auto t = Rig::trailer({3});
    r.impl.onProduceResponse(r.corrs[1], 100, 0, -1, true, t);
    r.impl.onProduceResponse(r.corrs[0], 100, 0, -1, true, t);   // the late original answer is ignored
    for (rc::Token k = 1; k <= 4; ++k) EXPECT_EQ(r.cq.times[k], 1) << "each record completes exactly once";
    for (rc::Token k = 1; k <= 3; ++k) {
        EXPECT_EQ(r.cq.done[k].status, OpStatus::Ok) << k;
        EXPECT_EQ(r.cq.done[k].value, 100 + int64_t(k) - 1) << k;
    }
    EXPECT_EQ(r.cq.done[4].status, OpStatus::BrokerError);
    EXPECT_EQ(r.cq.done[4].errorCode, kViolation);
    EXPECT_EQ(rc::constraintIdOf(r.cq.done[4]), kCid);
    EXPECT_FALSE(r.impl.hasInFlight());
}

TEST(ProducerVerdictsRegression, EmptyTrailerIsPlainOkRegression) {
    Rig r;
    r.send(3);
    r.impl.onProduceResponse(r.corrs[0], 10, 0, -1, true, {});
    for (rc::Token k = 1; k <= 3; ++k) {
        EXPECT_EQ(r.cq.done[k].status, OpStatus::Ok);
        EXPECT_EQ(r.cq.done[k].value, 10 + int64_t(k) - 1);
    }
}

TEST(ProducerVerdictsRegression, DuplicateSequenceOnAViewProducerIsVerdictUnknownRegression) {
    Rig r;
    r.send(8);
    r.impl.onProduceResponse(r.corrs[0], -1, static_cast<int16_t>(rpc::ErrorCode::DuplicateSequence));
    for (rc::Token k = 1; k <= 8; ++k) {
        ASSERT_EQ(r.cq.times[k], 1);
        EXPECT_EQ(r.cq.done[k].status, OpStatus::VerdictUnknown);
        EXPECT_EQ(r.cq.done[k].errorCode, static_cast<uint16_t>(rpc::ErrorCode::DuplicateSequence));
    }
}

TEST(ProducerVerdictsRegression, DuplicateSequenceOnAnAllProducerIsStillOkRegression) {
    Rig r(Acks::All);
    r.send(4);
    r.impl.onProduceResponse(r.corrs[0], -1, static_cast<int16_t>(rpc::ErrorCode::DuplicateSequence));
    for (rc::Token k = 1; k <= 4; ++k) {
        EXPECT_EQ(r.cq.done[k].status, OpStatus::Ok);
        EXPECT_EQ(r.cq.done[k].value, -1);
    }
}

TEST(ProducerVerdictsRegression, OpStatusThirteenIsReservedAndVerdictUnknownIsFourteenRegression) {
    EXPECT_EQ(static_cast<int>(OpStatus::BrokerTooOld), 12);
    EXPECT_EQ(static_cast<int>(OpStatus::StoredIdUnknown), 13);
    EXPECT_EQ(static_cast<int>(OpStatus::VerdictUnknown), 14);
    EXPECT_EQ(static_cast<int>(OpStatus::RecordsRejected), 15);
}

TEST(ProducerVerdictsRegression, UnhandledRejectedRecordDoesNotHideALaterRejectionRegression) {
    Rig r;
    r.send(8, true, 2);
    ASSERT_EQ(r.corrs.size(), 1u);
    const auto t = Rig::trailer({2, 5});   // record 2 has no completion handle
    r.impl.onProduceResponse(r.corrs[0], 100, 0, -1, true, t);
    EXPECT_EQ(r.cq.times[3], 0) << "the unhandled record completes nothing";
    for (rc::Token k = 1; k <= 8; ++k) {
        if (k == 3) continue;
        ASSERT_EQ(r.cq.times[k], 1) << k;
        const auto& c = r.cq.done[k];
        if (k == 6) {   // record index 5
            EXPECT_EQ(c.status, OpStatus::BrokerError);
            EXPECT_EQ(c.errorCode, kViolation);
            EXPECT_EQ(rc::constraintIdOf(c), kCid);
            EXPECT_EQ(c.value, -1);
        } else {
            EXPECT_EQ(c.status, OpStatus::Ok) << k;
        }
    }
}

TEST(ProducerVerdictsRegression, BatchAckOnAViewDuplicateIsVerdictUnknownRegression) {
    Rig r;
    r.captureBatchAcks();
    r.send(8);
    ASSERT_EQ(r.corrs.size(), 1u);
    r.impl.onProduceResponse(r.corrs[0], -1, static_cast<int16_t>(rpc::ErrorCode::DuplicateSequence));
    ASSERT_EQ(r.batchAcks.size(), 1u);
    EXPECT_EQ(r.batchAcks[0].errorCode, static_cast<int16_t>(rpc::ErrorCode::DuplicateSequence));
    EXPECT_EQ(r.batchAcks[0].baseOffset, -1);
}

TEST(ProducerVerdictsRegression, TransactionalViewProducerDuplicateIsOkRegression) {
    // A transactional producer never sends the View flag, so its verdicts cannot be lost: a duplicate is plain Ok.
    auto c = Rig::cfg(Acks::All | Acks::View);
    c.transactionalId = "txn-1";
    c.transactionOpTimeoutMs = 1'000;
    c.transactionTimeoutMs = 30'000;
    Rig r(c);
    test_support::FakeCoordinator fake;
    rune::client::producer::ProducerHub hub;
    hub.setCoordinatorSink(fake.sink());
    hub.add(&r.impl);
    int64_t now = kNow;
    const ProducerImpl::SendFn sink = [&r](int32_t, int32_t, int32_t, const uint8_t* b, std::size_t n) {
        const auto v = rc::codec::decodeProduce(b, n);
        if (!v) return false;
        const auto h = rpc::MessageHeader::decode(b, n);
        r.corrs.push_back(h ? h->correlationId : 0);
        return true;
    };
    const auto lastOffered = [&fake](uint16_t tid) -> const test_support::OfferedFrame* {
        for (std::size_t i = fake.offered.size(); i-- > 0;)
            if (fake.offered[i].templateId == tid) return &fake.offered[i];
        return nullptr;
    };
    rune::client::producer::TxnControl init;
    init.kind = rune::client::producer::TxnControl::Kind::Init;
    init.target.handler = rc::CompletionHandler{[](void*, const rc::Completion&, std::span<const uint8_t>) noexcept {}, nullptr};
    init.token = 1;
    r.impl.applyTxnControl(init, now);
    hub.drainAndFlush(now, 42, sink);
    const auto* ip = lastOffered(rpc::InitProducerId::TEMPLATE_ID);
    ASSERT_NE(ip, nullptr);
    rpc::InitProducerIdResponse ir;
    ir.correlationId = ip->corrId;
    ir.producerId = 42;
    ir.producerEpoch = 0;
    ir.errorCode = 0;
    test_support::AnswerFrame ia;
    ia.len = ir.encode(ia.buf.data());
    ia.templateId = rpc::InitProducerIdResponse::TEMPLATE_ID;
    ASSERT_TRUE(hub.onCoordinatorResponse(ia.templateId, ia.data(), ia.len, now));
    rune::client::producer::TxnControl begin;
    begin.kind = rune::client::producer::TxnControl::Kind::Begin;
    r.impl.applyTxnControl(begin, now);
    r.impl.testMarkStarted(rune::client::producer::TxnStarted::Yes);
    r.impl.setTxnGate(ProducerImpl::TxnGate::Open);
    for (int i = 0; i < 4; ++i)
        ASSERT_TRUE(r.impl.submitSendRecord(1, 0, rc::Key(uint64_t(i + 1)), kVal, true, ackhandle::forCq(0, rc::Token(i + 1))));
    while (r.impl.drainRing(now, 16) != 0) {}
    const test_support::OfferedFrame* add = nullptr;
    for (int i = 0; i < 4 && add == nullptr; ++i) {
        hub.drainAndFlush(now, 42, sink);
        add = lastOffered(rpc::AddPartitionsToTxn::TEMPLATE_ID);
    }
    ASSERT_NE(add, nullptr);
    const auto ans = fake.answerAdd(add->corrId, 0);
    ASSERT_TRUE(hub.onCoordinatorResponse(ans.templateId, ans.data(), ans.len, now));
    for (int i = 0; i < 4 && r.corrs.empty(); ++i) hub.drainAndFlush(now, 42, sink);
    ASSERT_EQ(r.corrs.size(), 1u);
    hub.onProduceResponse(r.corrs[0], -1, static_cast<int16_t>(rpc::ErrorCode::DuplicateSequence));
    for (rc::Token k = 1; k <= 4; ++k) {
        ASSERT_EQ(r.cq.times[k], 1) << k;
        EXPECT_EQ(r.cq.done[k].status, OpStatus::Ok);
        EXPECT_EQ(r.cq.done[k].value, -1);
    }
    hub.remove(&r.impl);
}

// ---- #806: Producer::onAck(AckCompletionFn): AckToken sends receive the full Completion a CQ would drain ----
namespace {
struct HandlerCapture {
    std::map<rc::Token, rc::Completion> done;
    std::map<rc::Token, int> times;
    std::map<rc::Token, std::size_t> payloadLen;
    std::map<rc::Token, std::vector<uint8_t>> payload;
    std::map<rc::Token, OpStatus> plain;       // the scalar form
    std::map<rc::Token, int64_t> plainValue;
    std::map<rc::Token, uint16_t> plainError;
    int plainCalls = 0;
    static void full(void* ctx, const rc::Completion& c, std::span<const uint8_t> payload) noexcept {
        auto* self = static_cast<HandlerCapture*>(ctx);
        self->done[c.token] = c;
        ++self->times[c.token];
        self->payloadLen[c.token] = payload.size();
        self->payload[c.token].assign(payload.begin(), payload.end());
    }
    static void scalar(void* ctx, rc::Token t, int64_t v, OpStatus st, uint16_t ec) noexcept {
        auto* self = static_cast<HandlerCapture*>(ctx);
        self->plainValue[t] = v;
        self->plain[t] = st;
        self->plainError[t] = ec;
        ++self->plainCalls;
    }
};
// The Rig's send(), with AckToken handles (ackhandle::forHandler) instead of CQ handles.
void sendToHandler(Rig& r, int n) {
    for (int i = 0; i < n; ++i)
        EXPECT_TRUE(r.impl.submitSendRecord(1, 0, rc::Key(uint64_t(i + 1)), kVal, true, ackhandle::forHandler(rc::Token(i + 1))));
    for (int i = 0; i < 1000 && r.corrs.empty(); ++i) {
        while (r.impl.drainRing(kNow) != 0) {}
        (void)r.impl.flushBatches(kNow, 42, r.sink);
    }
}
}  // namespace

TEST(ProducerVerdictsRegression, OnAckCarriesConstraintIdRegression) {
    Rig r;
    HandlerCapture cap;
    r.impl.setOnAck(&HandlerCapture::full, &cap);
    sendToHandler(r, 3);
    ASSERT_EQ(r.corrs.size(), 1u);
    r.impl.onProduceResponse(r.corrs[0], 100, 0, -1, true, Rig::trailer({1}));
    for (rc::Token k = 1; k <= 3; ++k) ASSERT_EQ(cap.times[k], 1) << k;
    const auto& bad = cap.done[2];   // record index 1
    EXPECT_EQ(bad.status, OpStatus::BrokerError);
    EXPECT_EQ(bad.errorCode, kViolation);
    EXPECT_EQ(rc::constraintIdOf(bad), kCid);
    EXPECT_EQ(bad.op, rc::OpKind::Send);
    EXPECT_EQ(bad.value, -1);
    EXPECT_EQ(cap.done[1].status, OpStatus::Ok);
    EXPECT_EQ(cap.done[1].value, 100);
    EXPECT_EQ(rc::constraintIdOf(cap.done[1]), 0u);
    EXPECT_EQ(cap.done[3].status, OpStatus::Ok);
    EXPECT_EQ(cap.done[3].value, 102);
    for (rc::Token k = 1; k <= 3; ++k) EXPECT_EQ(cap.payloadLen[k], 0u) << "a producer ack carries no inline payload";
    EXPECT_EQ(r.cq.times.size(), 0u) << "the CQ is not called as well";
}

TEST(ProducerVerdictsRegression, OnAckTier2CarriesRecordCountRegression) {
    Rig r;
    HandlerCapture cap;
    r.impl.setOnAck(&HandlerCapture::full, &cap);
    rune::protocol::RecordBatchBuilder b(0, 1000);
    for (int i = 0; i < 5; ++i) b.append(std::string_view("k"), std::string_view("v"), 1000);
    const auto bytes = b.build();
    ASSERT_EQ(r.impl.submitTier2Bytes(1, 0, ackhandle::forHandler(rc::Token(1)), 5, bytes), rc::producer::RejectReason::None);
    for (int i = 0; i < 1000 && r.corrs.empty(); ++i) {
        while (r.impl.drainRing(kNow) != 0) {}
        (void)r.impl.flushBatches(kNow, 42, r.sink);
    }
    ASSERT_EQ(r.corrs.size(), 1u);
    r.impl.onProduceResponse(r.corrs[0], 100, 0, -1, true, {});
    ASSERT_EQ(cap.times[1], 1);
    EXPECT_EQ(cap.done[1].op, rc::OpKind::Tier2Batch);
    EXPECT_EQ(cap.done[1].aux, 5);
    EXPECT_EQ(cap.done[1].status, OpStatus::Ok);
    EXPECT_EQ(cap.done[1].value, 100);
}

TEST(ProducerVerdictsRegression, PlainOnAckStillCalledRegression) {
    Rig r;
    HandlerCapture cap;
    r.impl.setOnAck(&HandlerCapture::scalar, &cap);
    sendToHandler(r, 3);
    ASSERT_EQ(r.corrs.size(), 1u);
    r.impl.onProduceResponse(r.corrs[0], 100, 0, -1, true, Rig::trailer({1}));
    EXPECT_EQ(cap.plainCalls, 3);
    EXPECT_EQ(cap.plain[2], OpStatus::BrokerError);
    EXPECT_EQ(cap.plainError[2], kViolation);
    EXPECT_EQ(cap.plain[1], OpStatus::Ok);
    EXPECT_EQ(cap.plain[3], OpStatus::Ok);
}

TEST(ProducerVerdictsRegression, LastOnAckRegistrationWinsRegression) {
    {   // Plain, then Full: only Full is called
        Rig r;
        HandlerCapture cap;
        r.impl.setOnAck(&HandlerCapture::scalar, &cap);
        r.impl.setOnAck(&HandlerCapture::full, &cap);
        sendToHandler(r, 2);
        ASSERT_EQ(r.corrs.size(), 1u);
        r.impl.onProduceResponse(r.corrs[0], 100, 0, -1, true, {});
        EXPECT_EQ(cap.plainCalls, 0);
        EXPECT_EQ(cap.times[1], 1);
        EXPECT_EQ(cap.times[2], 1);
    }
    {   // Full, then a null scalar handler: neither is called
        Rig r;
        HandlerCapture cap;
        r.impl.setOnAck(&HandlerCapture::full, &cap);
        r.impl.setOnAck(static_cast<rc::AckFn>(nullptr), nullptr);
        sendToHandler(r, 2);
        ASSERT_EQ(r.corrs.size(), 1u);
        r.impl.onProduceResponse(r.corrs[0], 100, 0, -1, true, {});
        EXPECT_EQ(cap.plainCalls, 0);
        EXPECT_TRUE(cap.times.empty());
    }
}

// ---- #802 (claim): a claimBatch reports EVERY record's verdict --------------------------------------------------------
namespace {
std::vector<uint8_t> payloadOf(const Rig& r, rc::Token t) {
    const auto it = r.cq.payloads.find(t);
    return it == r.cq.payloads.end() ? std::vector<uint8_t>{} : it->second;
}
// The rejected-index sets of a claim of 8: first, middle, last, several, none, all.
const std::vector<std::vector<uint16_t>>& claimSets() {
    static const std::vector<std::vector<uint16_t>> s = {{0}, {3}, {7}, {1, 4, 6}, {}, {0, 1, 2, 3, 4, 5, 6, 7}};
    return s;
}
// The claim's whole-batch contract for a claim of `n` at base 100 and rejected index set `set`.
void expectClaimVerdicts(const Rig& r, rc::Token tok, uint32_t n, const std::vector<uint16_t>& set,
                         rc::OpKind op = rc::OpKind::Send) {
    ASSERT_EQ(r.cq.times.count(tok) ? r.cq.times.at(tok) : 0, 1) << "one completion for the claim";
    const rc::Completion& c = r.cq.done.at(tok);
    const auto payload = payloadOf(r, tok);
    EXPECT_EQ(c.op, op);
    EXPECT_EQ(c.reservedBytes, rc::producer::batchCompletionBytes(true, n));
    if (set.empty()) {
        EXPECT_EQ(c.status, OpStatus::Ok);
        EXPECT_EQ(c.value, 100);
        EXPECT_EQ(c.payloadLen, 0u);
        EXPECT_TRUE(payload.empty());
        return;
    }
    EXPECT_EQ(c.status, OpStatus::RecordsRejected);
    EXPECT_EQ(c.value, 100);
    EXPECT_EQ(c.errorCode, kViolation);
    EXPECT_EQ(rc::constraintIdOf(c), kCid);
    ASSERT_EQ(c.payloadLen, 8u * set.size());
    ASSERT_EQ(payload.size(), 8u * set.size());
    const rc::BatchVerdicts v(c, payload);
    ASSERT_EQ(v.rejectedCount(), set.size());
    for (uint32_t i = 0; i < n; ++i) {
        const bool rejected = std::find(set.begin(), set.end(), static_cast<uint16_t>(i)) != set.end();
        EXPECT_EQ(v.accepted(i), !rejected) << i;
        EXPECT_EQ(v.offsetOf(i), rejected ? -1 : 100 + static_cast<int64_t>(i)) << i;
    }
    for (uint32_t k = 0; k < set.size(); ++k) {
        const rc::RecordVerdict rv = v.rejection(k);
        EXPECT_EQ(rv.index, set[k]);
        EXPECT_EQ(rv.errorCode, kViolation);
        EXPECT_EQ(rv.constraintId, kCid);
    }
}
}  // namespace

TEST(ProducerVerdictsRegression, ClaimBatchReportsEveryRecordVerdictRegression) {
    for (const auto& set : claimSets()) {
        SCOPED_TRACE("rejected=" + std::to_string(set.size()) + " first=" + (set.empty() ? "-" : std::to_string(set[0])));
        Rig r;
        r.claim(8, 1);
        ASSERT_EQ(r.corrs.size(), 1u);
        r.impl.onProduceResponse(r.corrs[0], 100, 0, -1, true, Rig::trailer(set));
        expectClaimVerdicts(r, 1, 8, set);
        EXPECT_FALSE(r.impl.hasInFlight());
    }
}

TEST(ProducerVerdictsRegression, DirectClaimReportsEveryRecordVerdictRegression) {
    for (const auto& set : claimSets()) {
        SCOPED_TRACE("Direct rejected=" + std::to_string(set.size()));
        Rig r(Rig::cfg(Acks::All | Acks::View), /*direct=*/true);
        r.claim(8, 1);
        ASSERT_EQ(r.corrs.size(), 1u);
        r.impl.onProduceResponse(r.corrs[0], 100, 0, -1, true, Rig::trailer(set));
        expectClaimVerdicts(r, 1, 8, set);
    }
}

TEST(ProducerVerdictsRegression, ClaimSharingABatchWithSendsRebasesItsIndexesRegression) {
    {
        Rig r;
        r.submitOne(1);
        r.submitOne(2);
        r.claimNoPump(4, ackhandle::forCq(0, 3));
        r.submitOne(4);
        r.pump();
        ASSERT_EQ(r.corrs.size(), 1u) << "one frame holds the two sends, the claim and the last send";
        r.impl.onProduceResponse(r.corrs[0], 100, 0, -1, true, Rig::trailer({1, 3, 5, 6}));
        for (rc::Token t = 1; t <= 4; ++t) ASSERT_EQ(r.cq.times[t], 1) << t;
        EXPECT_EQ(r.cq.done[1].status, OpStatus::Ok);
        EXPECT_EQ(r.cq.done[1].value, 100);
        EXPECT_EQ(r.cq.done[2].status, OpStatus::BrokerError);       // record 1
        EXPECT_EQ(r.cq.done[4].status, OpStatus::BrokerError);       // record 6
        EXPECT_EQ(r.cq.done[4].reservedBytes, rc::producer::kAckRecordBytes);
        const rc::Completion& c = r.cq.done[3];                      // records 2..5: rejected 3 and 5 -> claim 1 and 3
        EXPECT_EQ(c.status, OpStatus::RecordsRejected);
        EXPECT_EQ(c.value, 102);                                     // the claim's own base: record 0 of the claim
        const auto payload = payloadOf(r, 3);
        const rc::BatchVerdicts v(c, payload);
        ASSERT_EQ(v.rejectedCount(), 2u);
        EXPECT_EQ(v.rejection(0).index, 1);
        EXPECT_EQ(v.rejection(1).index, 3);
        EXPECT_TRUE(v.accepted(0));
        EXPECT_FALSE(v.accepted(1));
        EXPECT_TRUE(v.accepted(2));
        EXPECT_FALSE(v.accepted(3));
        EXPECT_EQ(v.offsetOf(2), 104);
        EXPECT_EQ(c.reservedBytes, rc::producer::batchCompletionBytes(true, 4));
    }
}

TEST(ProducerVerdictsRegression, ClaimBatchCompletesOnceOnEveryFailurePathRegression) {
    const uint32_t R = rc::producer::batchCompletionBytes(true, 4);
    const auto check = [&](Rig& r, OpStatus st) {
        ASSERT_EQ(r.cq.times[1], 1);
        EXPECT_EQ(r.cq.done[1].status, st);
        EXPECT_EQ(r.cq.done[1].payloadLen, 0u);
        EXPECT_EQ(r.cq.done[1].reservedBytes, R);
        EXPECT_EQ(r.cq.times[1], 1);
    };
    {   // DuplicateSequence -> VerdictUnknown
        Rig r;
        r.claim(4, 1);
        ASSERT_EQ(r.corrs.size(), 1u);
        r.impl.onProduceResponse(r.corrs[0], -1, static_cast<int16_t>(rpc::ErrorCode::DuplicateSequence));
        check(r, OpStatus::VerdictUnknown);
    }
    {   // the delivery timeout -> DeliveryFailed
        Rig r;
        r.claim(4, 1);
        ASSERT_EQ(r.corrs.size(), 1u);
        (void)r.impl.retransmitStalled(kNow + 130'000LL * 1'000'000, r.sink);
        check(r, OpStatus::DeliveryFailed);
    }
    {   // close -> Closed
        Rig r;
        r.claim(4, 1);
        ASSERT_EQ(r.corrs.size(), 1u);
        EXPECT_EQ(r.impl.closeImmediately(), rc::OpResult::Accepted);
        check(r, OpStatus::Closed);
    }
}

TEST(ProducerVerdictsRegression, ClaimContinuationNeverCollidesRegression) {
    using namespace ackhandle;
    static_assert(forCq(kRefMask, rc::kMaxToken) != kClaimContinuation);
    static_assert(forHandler(rc::kMaxToken) != kClaimContinuation);
    static_assert(!isCq(kClaimContinuation));
    static_assert((kClaimContinuation & kHandlerBit) == 0);
    EXPECT_NE(forCq(0, 0), kClaimContinuation);
    EXPECT_NE(forHandler(0), kClaimContinuation);
    // A claim of the cap (4096 records): its span covers exactly 4096 ack slots, and it completes once.
    Rig r(Rig::cfg(Acks::All | Acks::View), /*direct=*/false, std::size_t{1} << 22);
    r.claimNoPump(4096, ackhandle::forCq(0, 1), 8);
    while (r.impl.drainRing(kNow) != 0) {}
    const auto* ps = r.impl.partitionState(1, 0);
    ASSERT_NE(ps, nullptr);
    EXPECT_EQ(ps->batchAckCount, 4096);
    r.pump();
    ASSERT_EQ(r.corrs.size(), 1u);
    r.impl.onProduceResponse(r.corrs[0], 100, 0, -1, true, Rig::trailer({4095}));
    ASSERT_EQ(r.cq.times[1], 1);
    EXPECT_EQ(r.cq.done[1].status, OpStatus::RecordsRejected);
    EXPECT_EQ(r.cq.done[1].reservedBytes, rc::producer::batchCompletionBytes(true, 4096));
    const auto payload = payloadOf(r, 1);
    const rc::BatchVerdicts v(r.cq.done[1], payload);
    ASSERT_EQ(v.rejectedCount(), 1u);
    EXPECT_EQ(v.rejection(0).index, 4095);
    EXPECT_FALSE(r.impl.hasInFlight());
}

TEST(ProducerVerdictsRegression, OnAckFullHandlerGetsTheBatchVerdictsRegression) {
    {   // Full: the payload holds the one entry
        Rig r;
        HandlerCapture cap;
        r.impl.setOnAck(&HandlerCapture::full, &cap);
        r.claimNoPump(4, ackhandle::forHandler(1));
        r.pump();
        ASSERT_EQ(r.corrs.size(), 1u);
        r.impl.onProduceResponse(r.corrs[0], 100, 0, -1, true, Rig::trailer({2}));
        ASSERT_EQ(cap.times[1], 1);
        EXPECT_EQ(cap.done[1].status, OpStatus::RecordsRejected);
        ASSERT_EQ(cap.payload[1].size(), 8u);
        const rc::BatchVerdicts v(cap.done[1], cap.payload[1]);
        ASSERT_EQ(v.rejectedCount(), 1u);
        EXPECT_EQ(v.rejection(0).index, 2);
        EXPECT_EQ(v.rejection(0).errorCode, kViolation);
        EXPECT_EQ(v.rejection(0).constraintId, kCid);
    }
    {   // Plain: (RecordsRejected, base, first code)
        Rig r;
        HandlerCapture cap;
        r.impl.setOnAck(&HandlerCapture::scalar, &cap);
        r.claimNoPump(4, ackhandle::forHandler(1));
        r.pump();
        ASSERT_EQ(r.corrs.size(), 1u);
        r.impl.onProduceResponse(r.corrs[0], 100, 0, -1, true, Rig::trailer({2}));
        ASSERT_EQ(cap.plainCalls, 1);
        EXPECT_EQ(cap.plain[1], OpStatus::RecordsRejected);
        EXPECT_EQ(cap.plainValue[1], 100);
        EXPECT_EQ(cap.plainError[1], kViolation);
    }
}

// ---- #802 (Tier-2 + BatchAck): one completion reports every record of a Tier-2 batch; a BatchAck reports every rejection ------
TEST(ProducerVerdictsRegression, Tier2BatchReportsEveryRecordVerdictRegression) {
    for (const auto& set : claimSets()) {
        SCOPED_TRACE("rejected=" + std::to_string(set.size()) + " first=" + (set.empty() ? "-" : std::to_string(set[0])));
        Rig r;
        r.tier2(8, ackhandle::forCq(0, 1));
        ASSERT_EQ(r.corrs.size(), 1u);
        r.impl.onProduceResponse(r.corrs[0], 100, 0, -1, true, Rig::trailer(set));
        expectClaimVerdicts(r, 1, 8, set, rc::OpKind::Tier2Batch);
        EXPECT_EQ(r.cq.done[1].aux, 8);
        EXPECT_FALSE(r.impl.hasInFlight());
    }
}

TEST(ProducerVerdictsRegression, OneRecordClaimAndTier2KeepSingleRecordSemanticsRegression) {
    {   // a claim of 1
        Rig r;
        r.claim(1, 1);
        ASSERT_EQ(r.corrs.size(), 1u);
        r.impl.onProduceResponse(r.corrs[0], 100, 0, -1, true, Rig::trailer({0}));
        const auto& c = r.cq.done[1];
        EXPECT_EQ(c.status, OpStatus::BrokerError);
        EXPECT_EQ(c.value, -1);
        EXPECT_EQ(c.errorCode, kViolation);
        EXPECT_EQ(rc::constraintIdOf(c), kCid);
        EXPECT_EQ(c.reservedBytes, rc::producer::kAckRecordBytes);
        EXPECT_EQ(c.payloadLen, 0u);
    }
    {   // a Tier-2 batch of 1
        Rig r;
        r.tier2(1, ackhandle::forCq(0, 1));
        ASSERT_EQ(r.corrs.size(), 1u);
        r.impl.onProduceResponse(r.corrs[0], 100, 0, -1, true, Rig::trailer({0}));
        const auto& c = r.cq.done[1];
        EXPECT_EQ(c.op, rc::OpKind::Tier2Batch);
        EXPECT_EQ(c.status, OpStatus::BrokerError);
        EXPECT_EQ(c.value, -1);
        EXPECT_EQ(c.errorCode, kViolation);
        EXPECT_EQ(rc::constraintIdOf(c), kCid);
        EXPECT_EQ(c.reservedBytes, rc::producer::kAckRecordBytes);
        EXPECT_EQ(c.payloadLen, 0u);
    }
}

TEST(ProducerVerdictsRegression, BatchAckReportsEveryRejectionRegression) {
    {   // handler form
        Rig r;
        r.captureBatchAcks();
        r.send(8);
        ASSERT_EQ(r.corrs.size(), 1u);
        r.impl.onProduceResponse(r.corrs[0], 100, 0, -1, true, Rig::trailer({3, 5}));
        ASSERT_EQ(r.batchAcks.size(), 1u);
        EXPECT_EQ(r.batchAcks[0].baseOffset, 100) << "stored: the real base";
        EXPECT_EQ(r.batchAcks[0].errorCode, static_cast<int16_t>(kViolation));
        EXPECT_EQ(r.batchAcks[0].recordCount, 8);
        EXPECT_EQ(r.batchAcks[0].rejectedCount, 2u);
    }
    {   // the entries themselves (copied during the call)
        Rig r;
        std::vector<uint8_t> seen;
        r.impl.setOnBatchAck([](void* ctx, const rc::BatchAck& a) noexcept {
            auto* v = static_cast<std::vector<uint8_t>*>(ctx);
            if (a.rejectedCount != 0) v->assign(a.rejections, a.rejections + 8u * a.rejectedCount);
        }, &seen);
        r.send(8);
        r.impl.onProduceResponse(r.corrs[0], 100, 0, -1, true, Rig::trailer({3, 5}));
        EXPECT_EQ(seen, Rig::trailer({3, 5}));
    }
    {   // CQ form
        Rig r;
        std::vector<rc::Completion> got;
        std::vector<std::vector<uint8_t>> pl;
        struct Cap { std::vector<rc::Completion>* c; std::vector<std::vector<uint8_t>>* p; } cap{&got, &pl};
        ProducerImpl::Hooks h;
        h.ctx = &cap;
        h.cqDeliver = [](void* ctx, rc::CqRef, const rc::Completion& c, std::span<const uint8_t> p) noexcept {
            auto* k = static_cast<Cap*>(ctx);
            if (c.op == rc::OpKind::BatchAck) { k->c->push_back(c); k->p->emplace_back(p.begin(), p.end()); }
        };
        r.impl.setHooks(h);
        r.impl.setBatchAckCq(nullptr, 7);
        r.send(8);
        r.impl.onProduceResponse(r.corrs[0], 100, 0, -1, true, Rig::trailer({3, 5}));
        ASSERT_EQ(got.size(), 1u);
        EXPECT_EQ(got[0].status, OpStatus::RecordsRejected);
        EXPECT_EQ(got[0].value, 100);
        EXPECT_EQ(pl[0], Rig::trailer({3, 5}));
        EXPECT_EQ(got[0].reservedBytes, 8u * rc::kBatchAckUnitBytes);
    }
    {   // Tier-2 under batchAcksTo: the status, no list (one credit unit)
        Rig r;
        std::vector<rc::Completion> got;
        std::vector<std::size_t> plen;
        struct Cap { std::vector<rc::Completion>* c; std::vector<std::size_t>* p; } cap{&got, &plen};
        ProducerImpl::Hooks h;
        h.ctx = &cap;
        h.cqDeliver = [](void* ctx, rc::CqRef, const rc::Completion& c, std::span<const uint8_t> p) noexcept {
            auto* k = static_cast<Cap*>(ctx);
            if (c.op == rc::OpKind::BatchAck) { k->c->push_back(c); k->p->push_back(p.size()); }
        };
        r.impl.setHooks(h);
        r.impl.setBatchAckCq(nullptr, 7);
        r.tier2(8, 0);
        ASSERT_EQ(r.corrs.size(), 1u);
        r.impl.onProduceResponse(r.corrs[0], 100, 0, -1, true, Rig::trailer({3, 5}));
        ASSERT_EQ(got.size(), 1u);
        EXPECT_EQ(got[0].status, OpStatus::RecordsRejected);
        EXPECT_EQ(plen[0], 0u);
        EXPECT_EQ(got[0].reservedBytes, rc::kBatchAckUnitBytes);
    }
    {   // a 1-record batch's BatchAck keeps the first-rejection form
        Rig r;
        r.captureBatchAcks();
        r.send(1);
        r.impl.onProduceResponse(r.corrs[0], 100, 0, -1, true, Rig::trailer({0}));
        ASSERT_EQ(r.batchAcks.size(), 1u);
        EXPECT_EQ(r.batchAcks[0].baseOffset, -1);
        EXPECT_EQ(r.batchAcks[0].errorCode, static_cast<int16_t>(kViolation));
        EXPECT_EQ(r.batchAcks[0].rejectedCount, 0u);
    }
}

TEST(ProducerVerdictsRegression, OnAckFullHandlerGetsTheTier2VerdictsRegression) {
    Rig r;
    HandlerCapture cap;
    r.impl.setOnAck(&HandlerCapture::full, &cap);
    r.tier2(4, ackhandle::forHandler(1));
    ASSERT_EQ(r.corrs.size(), 1u);
    r.impl.onProduceResponse(r.corrs[0], 100, 0, -1, true, Rig::trailer({1, 3}));
    ASSERT_EQ(cap.times[1], 1);
    EXPECT_EQ(cap.done[1].op, rc::OpKind::Tier2Batch);
    EXPECT_EQ(cap.done[1].status, OpStatus::RecordsRejected);
    ASSERT_EQ(cap.payload[1].size(), 16u);
    const rc::BatchVerdicts v(cap.done[1], cap.payload[1]);
    EXPECT_EQ(v.rejection(0).index, 1);
    EXPECT_EQ(v.rejection(1).index, 3);
}
