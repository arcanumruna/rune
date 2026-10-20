// tests/producer/ProducerTxnDataTest.cpp - Phase 2b F Task 6: transactional data on every produce path -
// AddPartitionsToTxn gating per partition, the TRANSACTIONAL attribute bit, poisoning, the FD2 duty re-check.
//
// A bare transactional ProducerImpl wired to a FakeCoordinator through a ProducerHub (the Client's wiring, minus
// the transport), in Ring (Thread-mode) and Direct (Invoker) ingress.  The test thread IS the duty thread: every
// op is applied through applyTxnControl, every pass is hub.drainAndFlush on an injected duty clock (no sleeps, no
// threads, no atomics).  Every Produce frame the producer offers is decoded (partition, identity, attributes, CRC,
// record values); every coordinator frame is captured by the fake.  Completions of txn ops and of acked records
// are counted per token: exactly once is asserted at teardown.
#include "../support/AckLedger.h"
#include "../support/FakeCoordinator.h"

#include <rune/client/Completion.h>
#include <rune/client/Key.h>
#include <rune/client/codec/DataCodec.h>
#include <rune/client/producer/ProducerHub.h>
#include <rune/client/producer/ProducerImpl.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/protocol/client/DataMessages.h>
#include <rune/protocol/client/TxnMessages.h>

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <utility>
#include <variant>
#include <vector>

namespace rc  = rune::client;
namespace rp  = rune::client::producer;
namespace rpc = rune::protocol::client;
namespace rcc = rune::client::codec;
namespace rb  = rune::protocol::batch;
using rc::Acks;
using rc::Key;
using rc::OpKind;
using rc::OpStatus;
using rc::SendResult;
using rc::Token;
using rc::TxnState;
using rp::ProducerImpl;
using rp::TxnControl;
using test_support::AckLedger;
using test_support::FakeCoordinator;
using test_support::OfferedFrame;

namespace {

constexpr int64_t  kMs  = 1'000'000;
constexpr int64_t  kT0  = 1'000'000'000;
constexpr int32_t  kTopic = 1;
constexpr uint32_t kLen = 16;
constexpr int16_t  ec(rpc::ErrorCode e) { return static_cast<int16_t>(e); }
constexpr uint16_t uec(rpc::ErrorCode e) { return static_cast<uint16_t>(e); }
constexpr uint16_t kInvalidTxnState = uec(rpc::ErrorCode::InvalidTxnState);

rc::ProducerConfig txnCfg(std::string id = "txn-1") {
    rc::ProducerConfig c;
    c.acks                   = Acks::All;
    c.lingerUs               = 0;
    c.batchBytes             = 64 * 1024;
    c.maxInFlight            = 5;
    c.produceTimeoutMs       = 10'000;
    c.deliveryTimeoutMs      = 20'000;
    c.transactionOpTimeoutMs = 1'000;
    c.transactionalId        = std::move(id);
    c.transactionTimeoutMs   = 30'000;
    return c;
}

/// One value: `tag` repeated (so the order of records on the wire is checkable).
std::vector<uint8_t> val(uint8_t tag) { return std::vector<uint8_t>(kLen, tag); }

/// Completions of txn ops per token (exactly once at teardown).
struct OpLedger {
    struct Entry { int times = 0; rc::Completion c{}; };
    std::map<Token, Entry> e;
    static void on(void* ctx, const rc::Completion& c, std::span<const uint8_t>) noexcept {
        auto& x = static_cast<OpLedger*>(ctx)->e[c.token];
        ++x.times;
        x.c = c;
    }
};
struct Op {
    OpLedger* l = nullptr;
    Token     t = 0;
    [[nodiscard]] bool ready() const { return l->e.count(t) != 0; }
    [[nodiscard]] OpStatus status() const { return l->e.at(t).c.status; }
    [[nodiscard]] uint16_t code() const { return l->e.at(t).c.errorCode; }
};

/// CQ-form record completions (the Client's cqDeliver seam): per token.
struct CqLedger {
    std::map<Token, std::pair<int, rc::Completion>> e;
    static void deliver(void* ctx, rc::CqRef, const rc::Completion& c) noexcept {
        if (c.op == OpKind::BatchAck) return;
        auto& x = static_cast<CqLedger*>(ctx)->e[c.token];
        ++x.first;
        x.second = c;
    }
};

/// One Produce frame as the broker would see it.
struct WireBatch {
    int32_t                  partition = 0;
    int64_t                  pid       = 0;
    int16_t                  epoch     = 0;
    int32_t                  baseSeq   = 0;
    int64_t                  corrId    = 0;
    int16_t                  attrs     = 0;
    bool                     crcOk     = false;
    std::vector<uint8_t>     tags;   ///< first byte of every record value, in order
    bool                     acked     = false;
};

enum class Mode : uint8_t { Ring, Direct };

/// A transactional producer wired to a fake coordinator through a hub; the test thread is the duty thread.
struct Rig {
    FakeCoordinator               fake;
    rp::ProducerHub               hub;
    std::unique_ptr<ProducerImpl> impl;
    OpLedger                      ops;
    AckLedger                     acks;
    CqLedger                      cq;
    std::vector<WireBatch>        wire;
    std::vector<int64_t>          answeredAdds;
    ProducerImpl::SendFn          sink;
    int64_t                       now  = kT0;
    Token                         next = 0;
    rc::ProducerConfig            cfg;
    Mode                          mode;

    explicit Rig(Mode m = Mode::Ring, rc::ProducerConfig c = txnCfg()) : cfg(std::move(c)), mode(m) {
        hub.setCoordinatorSink(fake.sink());
        impl = std::make_unique<ProducerImpl>(cfg, 1 << 16);
        if (m == Mode::Direct) impl->enableDirectIngress({std::this_thread::get_id(), nullptr, nullptr});
        acks.attach(*impl);
        ProducerImpl::Hooks h;
        h.ctx       = &cq;
        h.cqDeliver = &CqLedger::deliver;
        impl->setHooks(h);
        hub.add(impl.get());
        sink = [this](int32_t, int32_t p, int32_t, const uint8_t* b, std::size_t n) {
            const auto v = rcc::decodeProduce(b, n);
            if (!v) return false;
            rune::protocol::RecordBatchReader r(v->recordBatch.data(), v->recordBatch.size());
            const auto hdr = rpc::MessageHeader::decode(b, n);
            WireBatch w{p, r.producerId(), r.producerEpoch(), r.baseSequence(), hdr ? hdr->correlationId : 0,
                        r.attributes(), r.ensureValid(), {}, false};
            r.forEachRecord([&](std::string_view, std::string_view value, int64_t, int64_t) {
                w.tags.push_back(value.empty() ? uint8_t{0} : static_cast<uint8_t>(value[0]));
            });
            wire.push_back(std::move(w));
            return true;
        };
    }
    ~Rig() {
        for (const auto& [t, x] : ops.e) EXPECT_EQ(x.times, 1) << "op token " << t << " completed " << x.times;
        EXPECT_TRUE(acks.noneDuplicated()) << "an acked record completed twice";
        for (const auto& [t, x] : cq.e) EXPECT_EQ(x.first, 1) << "CQ token " << t << " completed " << x.first;
        hub.remove(impl.get());
    }

    // ---- txn ops (applied on the duty thread) ----
    Op apply(TxnControl::Kind k) {
        TxnControl c;
        c.kind           = k;
        c.target.handler = rc::CompletionHandler{&OpLedger::on, &ops};
        c.token          = ++next;
        impl->applyTxnControl(c, now);
        return Op{&ops, c.token};
    }
    Op init() { return apply(TxnControl::Kind::Init); }
    Op commit() { return apply(TxnControl::Kind::Commit); }
    Op abort() { return apply(TxnControl::Kind::Abort); }
    /// The void beginTransaction(), and the handle's gate opened as Producer::beginTransaction() does.
    void begin() {
        TxnControl c;
        c.kind = TxnControl::Kind::Begin;
        impl->applyTxnControl(c, now);
        impl->setTxnGate(ProducerImpl::TxnGate::Open);
    }
    void initOk(int64_t pid = 42, int16_t epoch = 0) {
        Op op = init();
        tick();
        const OfferedFrame* f = last(rpc::InitProducerId::TEMPLATE_ID);
        ASSERT_NE(f, nullptr);
        answer(initAnswer(f->corrId, pid, epoch));
        ASSERT_TRUE(op.ready());
        ASSERT_EQ(op.status(), OpStatus::Ok);
    }
    static test_support::AnswerFrame initAnswer(int64_t corr, int64_t pid, int16_t epoch) {
        test_support::AnswerFrame a;
        rpc::InitProducerIdResponse m;
        m.correlationId = corr;
        m.producerId    = pid;
        m.producerEpoch = epoch;
        a.len        = m.encode(a.buf.data());
        a.templateId = rpc::InitProducerIdResponse::TEMPLATE_ID;
        return a;
    }

    // ---- produce paths (ProducerImpl's own entry points, as the Producer handle calls them) ----
    /// Void record.
    SendResult send(int32_t part, uint8_t tag) {
        const auto v = val(tag);
        return impl->trySubmit(kTopic, part, Key(uint64_t{7}), v, 0);
    }
    /// Acked record, the onAck handler form (AckToken).
    SendResult sendAck(int32_t part, uint8_t tag, Token t) {
        const auto v = val(tag);
        return impl->trySubmit(kTopic, part, Key(uint64_t{7}), v, AckLedger::hi(t));
    }
    /// Acked record, the Completions form.
    SendResult sendCq(int32_t part, uint8_t tag, Token t) {
        const auto v = val(tag);
        return impl->trySubmit(kTopic, part, Key(uint64_t{7}), v,
                               static_cast<int64_t>(rp::ackhandle::forCq(rc::CqRef{1}, t)));
    }
    /// A Tier-2 batch of `n` records (tag, tag+1, ...), acked by `t` (0 = void).
    rp::RejectReason batch(int32_t part, uint8_t tag, int n, Token t) {
        rune::protocol::RecordBatchBuilder b(0, 1'700'000'000'000LL);
        for (int i = 0; i < n; ++i) {
            const auto v = val(static_cast<uint8_t>(tag + i));
            b.append(std::string_view("k", 1),
                     std::string_view(reinterpret_cast<const char*>(v.data()), v.size()), 1'700'000'000'000LL);
        }
        std::vector<uint8_t> bytes;
        b.buildInto(bytes);
        return impl->submitTier2Bytes(kTopic, part, t != 0 ? AckLedger::h(t) : 0, n, bytes);
    }
    /// tryClaim + commit (void).
    SendResult claim(int32_t part, uint8_t tag) {
        const auto g = impl->claimRecord(kTopic, part, Key(uint64_t{7}), kLen, 0);
        if (g.result != SendResult::Ok) return g.result;
        std::memset(g.base, tag, kLen);
        return impl->commitClaim(g.token) ? SendResult::Ok : SendResult::Closed;
    }
    /// claimBatch of 3 + commit (records tag, tag+1, tag+2).
    SendResult claimBatch(int32_t part, uint8_t tag) {
        const auto g = impl->claimBatch(kTopic, part, Key(uint64_t{7}), 3, kLen);
        if (g.result != SendResult::Ok) return g.result;
        for (uint32_t i = 0; i < 3; ++i) {
            uint8_t* rec = g.offsets != nullptr ? g.base + g.offsets[i] : g.base + static_cast<std::size_t>(i) * kLen;
            std::memset(rec, static_cast<uint8_t>(tag + i), kLen);
        }
        return impl->commitClaim(g.token) ? SendResult::Ok : SendResult::Closed;
    }

    // ---- duty passes ----
    int tick(int64_t advanceNs = 0) {
        now += advanceNs;
        return hub.drainAndFlush(now, 42, sink);
    }
    void ticks(int n, int64_t advanceNs = 0) { for (int i = 0; i < n; ++i) tick(advanceNs); }
    void answer(const test_support::AnswerFrame& f) {
        ASSERT_TRUE(hub.onCoordinatorResponse(f.templateId, f.data(), f.len, now));
    }
    void sweep() { impl->sweepTimedOut(std::chrono::steady_clock::time_point(std::chrono::nanoseconds(now))); }

    // ---- AddPartitionsToTxn ----
    [[nodiscard]] std::vector<const OfferedFrame*> adds() const {
        std::vector<const OfferedFrame*> v;
        for (const auto& f : fake.offered) if (f.templateId == rpc::AddPartitionsToTxn::TEMPLATE_ID) v.push_back(&f);
        return v;
    }
    [[nodiscard]] const OfferedFrame* pendingAdd() const {
        for (const OfferedFrame* f : adds()) {
            if (std::find(answeredAdds.begin(), answeredAdds.end(), f->corrId) == answeredAdds.end()) return f;
        }
        return nullptr;
    }
    /// The partitions an AddPartitionsToTxn frame names (the fake keeps its first 96 bytes: <= 6 partitions).
    [[nodiscard]] static std::vector<int32_t> named(const OfferedFrame& f) {
        std::vector<int32_t> out;
        const auto m = rpc::AddPartitionsToTxn::decode(f.head.data(), std::min<std::size_t>(f.len, f.head.size()));
        if (!m) return out;
        for (std::size_t i = 0; i < m->partitions.size(); ++i) out.push_back(m->partitions[i].partition);
        return out;
    }
    /// Answers the oldest unanswered AddPartitionsToTxn (after up to 3 passes to let it out) with `code`.
    void answerAdd(int16_t code = 0) {
        for (int i = 0; i < 3 && pendingAdd() == nullptr; ++i) tick();
        const OfferedFrame* f = pendingAdd();
        ASSERT_NE(f, nullptr) << "no AddPartitionsToTxn offered";
        answeredAdds.push_back(f->corrId);
        answer(FakeCoordinator::answer(rp::ReqKind::AddPartitions, f->corrId, code));
    }

    // ---- data ----
    void ackAll() {
        for (auto& w : wire) {
            if (w.acked) continue;
            w.acked = true;
            hub.onProduceResponse(w.corrId, 0, 0);
        }
    }
    [[nodiscard]] std::size_t framesFor(int32_t part) const {
        std::size_t n = 0;
        for (const auto& w : wire) n += w.partition == part ? 1 : 0;
        return n;
    }
    [[nodiscard]] std::vector<uint8_t> tagsFor(int32_t part) const {
        std::vector<uint8_t> t;
        for (const auto& w : wire) if (w.partition == part) t.insert(t.end(), w.tags.begin(), w.tags.end());
        return t;
    }

    // ---- coordinator ----
    [[nodiscard]] const OfferedFrame* last(uint16_t tid) const {
        for (std::size_t i = fake.offered.size(); i-- > 0;) if (fake.offered[i].templateId == tid) return &fake.offered[i];
        return nullptr;
    }
    [[nodiscard]] std::size_t endTxns() const {
        std::size_t n = 0;
        for (const auto& f : fake.offered) n += f.templateId == rpc::EndTxn::TEMPLATE_ID ? 1 : 0;
        return n;
    }
    [[nodiscard]] std::optional<rpc::EndTxn> lastEnd() const {
        const OfferedFrame* f = last(rpc::EndTxn::TEMPLATE_ID);
        if (f == nullptr) return std::nullopt;
        return rpc::EndTxn::decode(f->head.data(), std::min<std::size_t>(f->len, f->head.size()));
    }
    void endAnswer(int16_t code = 0) {
        answer(FakeCoordinator::answer(rp::ReqKind::EndTxn, last(rpc::EndTxn::TEMPLATE_ID)->corrId, code));
    }
    [[nodiscard]] TxnState state() const { return impl->txnStateDuty(); }
    [[nodiscard]] const rp::TxnMembership& membership(int32_t part) const {
        return impl->partitionState(kTopic, part)->txn;
    }
};

// ============================================================================================================
// Review Focus #2: a partition's records never reach the log before its AddPartitionsToTxn is OK - on every path
// ============================================================================================================

enum class Path : uint8_t { Send, TrySendCq, TrySendAck, Batch, TryClaim, ClaimBatch };
const char* pathName(Path p) {
    switch (p) {
    case Path::Send:       return "Send";
    case Path::TrySendCq:  return "TrySendCq";
    case Path::TrySendAck: return "TrySendAck";
    case Path::Batch:      return "Batch";
    case Path::TryClaim:   return "TryClaim";
    case Path::ClaimBatch: return "ClaimBatch";
    }
    return "?";
}

/// Issues op #k of `p` on partition `part`: records tagged 10k, 10k+1, ... (one, or three for the batch forms).
bool issue(Rig& r, Path p, int32_t part, int k) {
    const auto tag = static_cast<uint8_t>(10 * k + 10);
    const Token t  = static_cast<Token>(100 + k);
    switch (p) {
    case Path::Send:       return r.send(part, tag) == SendResult::Ok;
    case Path::TrySendCq:  return r.sendCq(part, tag, t) == SendResult::Ok;
    case Path::TrySendAck: return r.sendAck(part, tag, t) == SendResult::Ok;
    case Path::Batch:      return r.batch(part, tag, 3, t) == rp::RejectReason::None;
    case Path::TryClaim:   return r.claim(part, tag) == SendResult::Ok;
    case Path::ClaimBatch: return r.claimBatch(part, tag) == SendResult::Ok;
    }
    return false;
}
std::vector<uint8_t> expectedTags(Path p, int ops) {
    std::vector<uint8_t> t;
    const int per = (p == Path::Batch || p == Path::ClaimBatch) ? 3 : 1;
    for (int k = 0; k < ops; ++k) for (int i = 0; i < per; ++i) t.push_back(static_cast<uint8_t>(10 * k + 10 + i));
    return t;
}

class NoProduceBeforeAddPartitionsOk : public ::testing::TestWithParam<std::tuple<Path, Mode>> {};

TEST_P(NoProduceBeforeAddPartitionsOk, NoProduceBeforeAddPartitionsOkOnAnyPathRegression) {
    const auto [path, mode] = GetParam();
    Rig r(mode);
    r.initOk();
    r.begin();
    constexpr int kOps = 3;
    for (int k = 0; k < kOps; ++k) {
        ASSERT_TRUE(issue(r, path, 0, k)) << "op " << k;
        r.tick();
    }
    r.ticks(5, kMs);   // the fake delays its answer
    ASSERT_EQ(r.adds().size(), 1u) << "one AddPartitionsToTxn for the new partition";
    EXPECT_EQ(Rig::named(*r.adds()[0]), std::vector<int32_t>{0});
    EXPECT_EQ(r.framesFor(0), 0u) << "a transactional batch reached the log before its AddPartitionsToTxn was OK";
    EXPECT_TRUE(std::holds_alternative<rp::Adding>(r.membership(0)));
    r.answerAdd(0);
    EXPECT_TRUE(std::holds_alternative<rp::Added>(r.membership(0)));
    r.ticks(3);
    ASSERT_GE(r.framesFor(0), 1u) << "the held records never left after the OK";
    EXPECT_EQ(r.tagsFor(0), expectedTags(path, kOps)) << "records out of order";
    for (const auto& w : r.wire) EXPECT_NE(w.attrs & rb::kAttrTransactional, 0) << "a transactional batch without the bit";
    r.ackAll();
    Op commit = r.commit();
    r.tick();
    ASSERT_EQ(r.endTxns(), 1u);
    EXPECT_EQ(r.lastEnd()->commit, uint8_t{1});
    r.endAnswer();
    ASSERT_TRUE(commit.ready());
    EXPECT_EQ(commit.status(), OpStatus::Ok);
    if (path == Path::TrySendAck || path == Path::Batch) {
        for (int k = 0; k < kOps; ++k) EXPECT_EQ(r.acks.status(static_cast<Token>(100 + k)), OpStatus::Ok);
    }
    if (path == Path::TrySendCq) {
        for (int k = 0; k < kOps; ++k) EXPECT_EQ(r.cq.e.at(static_cast<Token>(100 + k)).second.status, OpStatus::Ok);
    }
    EXPECT_TRUE(std::holds_alternative<rp::NotInTxn>(r.membership(0))) << "membership kept past EndTxn's answer";
}

INSTANTIATE_TEST_SUITE_P(
    ProducerTxnDataRegression, NoProduceBeforeAddPartitionsOk,
    ::testing::Combine(::testing::Values(Path::Send, Path::TrySendCq, Path::TrySendAck, Path::Batch, Path::TryClaim,
                                         Path::ClaimBatch),
                       ::testing::Values(Mode::Ring, Mode::Direct)),
    [](const auto& info) {
        return std::string(pathName(std::get<0>(info.param))) +
               (std::get<1>(info.param) == Mode::Ring ? "Ring" : "Direct");
    });

}  // namespace

// ============================================================================================================
// AddPartitionsToTxn: once per new partition per transaction
// ============================================================================================================

TEST(ProducerTxnData, OneAddPartitionsPerNewPartitionPerTransaction) {
    Rig r;
    r.initOk();
    for (int txn = 0; txn < 2; ++txn) {
        SCOPED_TRACE(txn);
        const std::size_t addsBefore = r.adds().size();
        r.begin();
        for (int i = 0; i < 100; ++i) {
            ASSERT_EQ(r.send(i % 4, static_cast<uint8_t>(i)), SendResult::Ok);
            if (i % 10 == 9) {
                r.tick();
                while (r.pendingAdd() != nullptr) r.answerAdd(0);
                r.tick();
                r.ackAll();
            }
        }
        r.ticks(2);
        while (r.pendingAdd() != nullptr) r.answerAdd(0);
        r.ticks(2);
        r.ackAll();
        std::map<int32_t, int> perPartition;
        for (std::size_t i = addsBefore; i < r.adds().size(); ++i) {
            for (const int32_t p : Rig::named(*r.adds()[i])) ++perPartition[p];
        }
        EXPECT_EQ(perPartition, (std::map<int32_t, int>{{0, 1}, {1, 1}, {2, 1}, {3, 1}}))
            << "AddPartitionsToTxn must name every new partition exactly once per transaction";
        EXPECT_EQ(r.impl->txnPartitionCount(), 4u);
        Op commit = r.commit();
        r.ticks(2);
        r.ackAll();
        r.tick();
        ASSERT_EQ(r.endTxns(), static_cast<std::size_t>(txn + 1));
        r.endAnswer();
        ASSERT_TRUE(commit.ready());
        EXPECT_EQ(commit.status(), OpStatus::Ok);
        EXPECT_EQ(r.impl->txnPartitionCount(), 0u) << "the next transaction must add its partitions again";
    }
    std::size_t records = 0;
    for (const auto& w : r.wire) records += w.tags.size();
    EXPECT_EQ(records, 200u);
}

// ============================================================================================================
// The TRANSACTIONAL attribute bit
// ============================================================================================================

TEST(ProducerTxnData, EveryTransactionalBatchHasTheTransactionalBitAndPlainNever) {
    {
        Rig r;
        r.initOk();
        r.begin();
        ASSERT_EQ(r.send(0, 1), SendResult::Ok);
        ASSERT_EQ(r.batch(0, 2, 3, 0), rp::RejectReason::None);   // Tier-2: restamped on the duty side
        ASSERT_EQ(r.claim(1, 5), SendResult::Ok);
        ASSERT_EQ(r.batch(1, 6, 2, 0), rp::RejectReason::None);
        r.tick();
        r.answerAdd(0);
        r.ticks(3);
        ASSERT_EQ(r.wire.size(), 4u);
        for (const auto& w : r.wire) {
            EXPECT_NE(w.attrs & rb::kAttrTransactional, 0) << "partition " << w.partition;
            EXPECT_TRUE(w.crcOk) << "the restamped CRC does not verify (full recompute)";
        }
        r.ackAll();
        Op commit = r.commit();
        r.tick();
        r.endAnswer();
        ASSERT_TRUE(commit.ready());
    }
    {
        // A plain producer (no transactional id, a bare rig): never the bit, on either tier.
        rc::ProducerConfig c = txnCfg("");
        ProducerImpl impl(c, 1 << 16);
        std::vector<WireBatch> wire;
        ProducerImpl::SendFn sink = [&](int32_t, int32_t p, int32_t, const uint8_t* b, std::size_t n) {
            const auto v = rcc::decodeProduce(b, n);
            if (!v) return false;
            rune::protocol::RecordBatchReader rd(v->recordBatch.data(), v->recordBatch.size());
            wire.push_back({p, rd.producerId(), rd.producerEpoch(), rd.baseSequence(), 0, rd.attributes(),
                            rd.ensureValid(), {}, false});
            return true;
        };
        const auto v = val(1);
        ASSERT_EQ(impl.trySubmit(kTopic, 0, Key(uint64_t{1}), v, 0), SendResult::Ok);
        rune::protocol::RecordBatchBuilder b(0, 1'700'000'000'000LL);
        b.append(std::string_view("k", 1), std::string_view(reinterpret_cast<const char*>(v.data()), v.size()),
                 1'700'000'000'000LL);
        std::vector<uint8_t> bytes;
        b.buildInto(bytes);
        ASSERT_EQ(impl.submitTier2Bytes(kTopic, 0, 0, 1, bytes), rp::RejectReason::None);
        for (int i = 0; i < 3; ++i) {
            (void)impl.drainRing(kT0, 64);
            (void)impl.flushBatches(kT0, 42, sink);
        }
        ASSERT_EQ(wire.size(), 2u);
        for (const auto& w : wire) {
            EXPECT_EQ(w.attrs & rb::kAttrTransactional, 0) << "a plain batch carries the TRANSACTIONAL bit";
            EXPECT_TRUE(w.crcOk);
        }
    }
}

// ============================================================================================================
// Commit waits for every Added partition to drain
// ============================================================================================================

TEST(ProducerTxnData, CommitWaitsForEveryAddedPartitionToDrain) {
    rc::ProducerConfig cfg = txnCfg();
    cfg.lingerUs = 10'000'000;   // the commit flushes them (no linger)
    Rig r(Mode::Ring, cfg);
    r.initOk();
    r.begin();
    ASSERT_EQ(r.send(0, 1), SendResult::Ok);
    ASSERT_EQ(r.send(1, 2), SendResult::Ok);
    r.ticks(2);
    ASSERT_EQ(r.adds().size(), 1u);
    EXPECT_EQ(Rig::named(*r.adds()[0]), (std::vector<int32_t>{0, 1})) << "one request carries every new partition";
    Op commit = r.commit();   // issued while AddPartitions is unanswered
    r.ticks(4, kMs);
    EXPECT_TRUE(r.wire.empty());
    EXPECT_EQ(r.endTxns(), 0u) << "EndTxn before the partitions were even added";
    r.answerAdd(0);
    r.tick();
    ASSERT_EQ(r.wire.size(), 2u) << "the commit flushes every Added partition at once";
    EXPECT_EQ(r.endTxns(), 0u);
    r.hub.onProduceResponse(r.wire[0].corrId, 0, 0);
    r.wire[0].acked = true;
    r.ticks(2);
    EXPECT_EQ(r.endTxns(), 0u) << "EndTxn while partition " << r.wire[1].partition << " still has a batch in flight";
    r.ackAll();
    r.tick();
    ASSERT_EQ(r.endTxns(), 1u);
    EXPECT_EQ(r.lastEnd()->commit, uint8_t{1});
    EXPECT_FALSE(commit.ready());
    r.endAnswer();
    ASSERT_TRUE(commit.ready());
    EXPECT_EQ(commit.status(), OpStatus::Ok);
    EXPECT_TRUE(std::holds_alternative<rp::NotInTxn>(r.membership(0)));
    EXPECT_TRUE(std::holds_alternative<rp::NotInTxn>(r.membership(1)));
}

// ============================================================================================================
// Poisoning
// ============================================================================================================

// A non-retriable AddPartitionsToTxn answer: the partition is AddFailed, the transaction AbortableError (commit
// completes with it, sends nothing); the abort discards the partition's records - never produced - and the acked
// ones complete with the transaction's status.  The next transaction adds the partition again.
TEST(ProducerTxnDataRegression, AddPartitionsFailurePoisonsAndAbortDiscardsWithoutProducingRegression) {
    for (const Mode mode : {Mode::Ring, Mode::Direct}) {
        SCOPED_TRACE(mode == Mode::Ring ? "Ring" : "Direct");
        Rig r(mode);
        r.initOk();
        r.begin();
        ASSERT_EQ(r.sendAck(0, 1, 7), SendResult::Ok);
        ASSERT_EQ(r.send(0, 2), SendResult::Ok);
        ASSERT_EQ(r.batch(0, 3, 2, 8), rp::RejectReason::None);
        r.tick();
        r.answerAdd(ec(rpc::ErrorCode::InvalidTxnState));
        EXPECT_EQ(r.state(), TxnState::AbortableError);
        ASSERT_TRUE(std::holds_alternative<rp::AddFailed>(r.membership(0)));
        r.ticks(4, kMs);
        EXPECT_TRUE(r.wire.empty()) << "an AddFailed partition produced";
        Op commit = r.commit();
        ASSERT_TRUE(commit.ready());
        EXPECT_EQ(commit.status(), OpStatus::BrokerError);
        EXPECT_EQ(commit.code(), kInvalidTxnState);
        Op abort = r.abort();
        r.ticks(3);
        EXPECT_TRUE(r.wire.empty()) << "the abort produced the failed partition's records";
        ASSERT_EQ(r.acks.times(7), 1);
        EXPECT_EQ(r.acks.status(7), OpStatus::BrokerError);
        EXPECT_EQ(r.acks.last.at(7).ec, kInvalidTxnState) << "acked records complete with the transaction's status";
        ASSERT_EQ(r.acks.times(8), 1);
        EXPECT_EQ(r.acks.status(8), OpStatus::BrokerError);
        EXPECT_EQ(r.endTxns(), 0u) << "nothing was added: the transaction ends locally";
        ASSERT_TRUE(abort.ready());
        EXPECT_EQ(abort.status(), OpStatus::Ok);
        EXPECT_EQ(r.state(), TxnState::Ready);
        EXPECT_TRUE(std::holds_alternative<rp::NotInTxn>(r.membership(0)));
        // The next transaction: the partition is added again and its records go out.
        r.begin();
        ASSERT_EQ(r.send(0, 9), SendResult::Ok);
        r.answerAdd(0);
        ASSERT_EQ(r.adds().size(), 2u) << "the next transaction adds the partition again";
        r.ticks(2);
        ASSERT_EQ(r.wire.size(), 1u);
        EXPECT_EQ(r.wire[0].tags, std::vector<uint8_t>{9});
        r.ackAll();
        Op c2 = r.commit();
        r.tick();
        r.endAnswer();
        ASSERT_TRUE(c2.ready());
        EXPECT_EQ(c2.status(), OpStatus::Ok);
    }
}

// FD2 duty re-check: the handle's gate is Open but the duty side is no longer InTransaction when the records reach
// it (here forced AbortableError before the drain): they never leave; the acked one completes with the
// transaction's status, the void one is counted (and fails the transaction).  Outside any transaction (Ready) the
// acked form completes BrokerError(19).  Direct (Invoker) ingress decides it synchronously: NotInTransaction.
TEST(ProducerTxnDataRegression, RecordReachingDutyOutsideATransactionIsNeverProducedRegression) {
    constexpr uint16_t kNotAuthorized = uec(rpc::ErrorCode::NotAuthorized);
    {
        Rig r;
        r.initOk();
        r.begin();
        ASSERT_EQ(r.sendAck(0, 1, 7), SendResult::Ok);   // still in the producer ring
        ASSERT_EQ(r.send(0, 2), SendResult::Ok);
        r.impl->testPoison(OpStatus::BrokerError, kNotAuthorized, rp::AbortKind::Plain);
        r.ticks(4, kMs);
        EXPECT_TRUE(r.wire.empty()) << "a record read outside InTransaction was produced";
        EXPECT_TRUE(r.adds().empty()) << "a refused record added its partition";
        ASSERT_EQ(r.acks.times(7), 1);
        EXPECT_EQ(r.acks.status(7), OpStatus::BrokerError);
        EXPECT_EQ(r.acks.last.at(7).ec, kNotAuthorized) << "the acked record completes with the transaction's status";
        EXPECT_EQ(r.impl->txnRecordsRefusedDuty(), 1u);
        Op abort = r.abort();
        r.tick();
        ASSERT_TRUE(abort.ready());
        EXPECT_EQ(abort.status(), OpStatus::Ok);
    }
    {
        Rig r;   // no transaction at all: the gate let a record through after the transaction ended
        r.initOk();
        r.impl->setTxnGate(ProducerImpl::TxnGate::Open);
        ASSERT_EQ(r.sendAck(0, 1, 7), SendResult::Ok);
        ASSERT_EQ(r.batch(0, 2, 2, 0), rp::RejectReason::None);
        r.ticks(2);
        EXPECT_TRUE(r.wire.empty());
        EXPECT_EQ(r.acks.status(7), OpStatus::BrokerError);
        EXPECT_EQ(r.acks.last.at(7).ec, kInvalidTxnState);
        EXPECT_EQ(r.impl->txnRecordsRefusedDuty(), 2u) << "the void Tier-2 batch's records";
        EXPECT_EQ(r.state(), TxnState::Ready);
    }
    {
        Rig r(Mode::Direct);
        r.initOk();
        r.begin();
        r.impl->testPoison(OpStatus::BrokerError, kNotAuthorized, rp::AbortKind::Plain);
        EXPECT_EQ(r.sendAck(0, 1, 7), SendResult::NotInTransaction);
        EXPECT_EQ(r.claim(0, 2), SendResult::NotInTransaction);
        EXPECT_EQ(r.batch(0, 3, 1, 0), rp::RejectReason::NotInTransaction);
        r.ticks(2);
        EXPECT_TRUE(r.wire.empty());
        EXPECT_EQ(r.acks.times(7), 0) << "a refused send has no side effect";
        Op abort = r.abort();
        r.tick();
        ASSERT_TRUE(abort.ready());
    }
}

// Task 5 carry: a record that fails delivery inside a transaction poisons it - a commit never sends EndTxn(commit)
// over lost data.  (a) a sent batch answered with a non-retriable error, (b) a sent batch with no answer by the
// delivery timeout while the commit waits for it, (c) a void record never sent (its partition never added) past
// the delivery timeout.
TEST(ProducerTxnDataRegression, FailedDeliveryInsideATransactionPoisonsItRegression) {
    {
        Rig r;
        r.initOk();
        r.begin();
        ASSERT_EQ(r.send(0, 1), SendResult::Ok);
        r.tick();
        r.answerAdd(0);
        r.tick();
        ASSERT_EQ(r.wire.size(), 1u);
        r.hub.onProduceResponse(r.wire[0].corrId, -1, ec(rpc::ErrorCode::NotAuthorized));
        EXPECT_EQ(r.state(), TxnState::AbortableError) << "a lost record left the transaction committable";
        Op commit = r.commit();
        ASSERT_TRUE(commit.ready());
        EXPECT_EQ(commit.status(), OpStatus::BrokerError);
        EXPECT_EQ(commit.code(), uec(rpc::ErrorCode::NotAuthorized));
        r.ticks(3);
        EXPECT_EQ(r.endTxns(), 0u);
        Op abort = r.abort();
        r.tick();
        ASSERT_EQ(r.endTxns(), 1u);
        EXPECT_EQ(r.lastEnd()->commit, uint8_t{0});
        r.endAnswer();
        r.tick();   // the batch's outcome was unknown: the abort bumps the epoch (KIP-360)
        ASSERT_NE(r.last(rpc::InitProducerId::TEMPLATE_ID), nullptr);
        r.answer(Rig::initAnswer(r.last(rpc::InitProducerId::TEMPLATE_ID)->corrId, 42, 1));
        ASSERT_TRUE(abort.ready());
        EXPECT_EQ(abort.status(), OpStatus::Ok);
    }
    {
        Rig r;
        r.initOk();
        r.begin();
        ASSERT_EQ(r.send(0, 1), SendResult::Ok);
        r.tick();
        r.answerAdd(0);
        r.tick();
        ASSERT_EQ(r.wire.size(), 1u);
        Op commit = r.commit();   // waits for the batch, which is never answered
        r.now += r.cfg.deliveryTimeoutMs * kMs;
        r.sweep();
        ASSERT_TRUE(commit.ready());
        EXPECT_EQ(commit.status(), OpStatus::DeliveryFailed);
        EXPECT_EQ(r.state(), TxnState::AbortableError);
        r.ticks(3);
        EXPECT_EQ(r.endTxns(), 0u) << "EndTxn(commit) over a lost record";
    }
    {
        Rig r;
        r.initOk();
        r.begin();
        ASSERT_EQ(r.send(0, 1), SendResult::Ok);
        r.ticks(2);                               // its AddPartitionsToTxn is never answered
        r.now += r.cfg.deliveryTimeoutMs * kMs;
        r.sweep();
        EXPECT_EQ(r.state(), TxnState::AbortableError) << "a void record lost before it was sent";
        Op commit = r.commit();
        ASSERT_TRUE(commit.ready());
        EXPECT_EQ(commit.status(), OpStatus::DeliveryFailed);
        EXPECT_TRUE(r.wire.empty());
    }
}

// Task 5 carry (re-review Minor 3): ResendEnd re-sends EndTxn(commit) without re-checking the data, which is safe
// only because no record can join a committing transaction.  Pinned: the gate is Closed at commit issue, and a
// record that reaches the duty side while Committing (here: the gate forced Open) is refused - never produced,
// never in the transaction - both while the commit waits for its data and in the in-doubt re-send window.
TEST(ProducerTxnDataRegression, RecordsAfterCommitIssueNeverJoinTheCommittingTransactionRegression) {
    for (const Mode mode : {Mode::Ring, Mode::Direct}) {
        SCOPED_TRACE(mode == Mode::Ring ? "Ring" : "Direct");
        Rig r(mode);
        r.initOk();
        r.begin();
        ASSERT_EQ(r.send(0, 1), SendResult::Ok);
        r.tick();
        r.answerAdd(0);
        r.tick();
        ASSERT_EQ(r.wire.size(), 1u);
        Op commit = r.commit();
        r.impl->setTxnGate(ProducerImpl::TxnGate::Closed);   // Producer::commitTransaction() closes it at issue
        EXPECT_EQ(r.send(0, 2), SendResult::NotInTransaction);
        r.impl->setTxnGate(ProducerImpl::TxnGate::Open);     // a record the gate let through anyway
        const SendResult late = r.sendAck(0, 3, 7);
        r.ticks(3);
        if (mode == Mode::Direct) {
            EXPECT_EQ(late, SendResult::NotInTransaction);
        } else {
            ASSERT_EQ(late, SendResult::Ok);
            ASSERT_EQ(r.acks.times(7), 1);
            EXPECT_EQ(r.acks.status(7), OpStatus::BrokerError);
            EXPECT_EQ(r.acks.last.at(7).ec, kInvalidTxnState);
        }
        r.ackAll();
        r.tick();
        ASSERT_EQ(r.endTxns(), 1u);
        r.endAnswer();
        ASSERT_TRUE(commit.ready());
        EXPECT_EQ(commit.status(), OpStatus::Ok);
        EXPECT_EQ(r.tagsFor(0), std::vector<uint8_t>{1}) << "a record sent after the commit joined it";

        // The in-doubt window: EndTxn(commit) unanswered past the op deadline, a late VOID record reaches the duty
        // side, the retried commit re-sends EndTxn at once (ResendEnd) - the record is never produced and the
        // commit, which may have landed, is not poisoned.
        r.begin();
        ASSERT_EQ(r.send(1, 4), SendResult::Ok);
        r.tick();
        r.answerAdd(0);
        r.tick();
        r.ackAll();
        Op c2 = r.commit();
        r.tick();
        ASSERT_EQ(r.endTxns(), 2u);
        r.tick(r.cfg.transactionOpTimeoutMs * kMs);
        ASSERT_EQ(c2.status(), OpStatus::Timeout);
        const uint64_t refusedBefore = r.impl->txnRecordsRefusedDuty();
        const SendResult late2 = r.send(1, 5);
        r.ticks(2);
        if (mode == Mode::Ring) {
            ASSERT_EQ(late2, SendResult::Ok);
            EXPECT_EQ(r.impl->txnRecordsRefusedDuty(), refusedBefore + 1);
        } else {
            EXPECT_EQ(late2, SendResult::NotInTransaction);
        }
        EXPECT_EQ(r.state(), TxnState::CommittingTransaction) << "a commit that may have landed was poisoned";
        Op retry = r.commit();
        r.tick();
        ASSERT_EQ(r.endTxns(), 3u) << "ResendEnd re-sends EndTxn at once";
        EXPECT_EQ(r.lastEnd()->commit, uint8_t{1});
        r.endAnswer();
        ASSERT_TRUE(retry.ready());
        EXPECT_EQ(retry.status(), OpStatus::Ok);
        r.ticks(2);
        EXPECT_EQ(r.tagsFor(1), std::vector<uint8_t>{4}) << "a record sent after the commit was produced";
    }
}

// An AddPartitionsToTxn with no answer by produceTimeoutMs is sent again (the data still waits for its OK); an abort
// cancels one still on the link and - since its frame went out - treats the transaction as possibly started: it
// sends EndTxn(abort), and 19 there means nothing was open (m15).
TEST(ProducerTxnData, AddPartitionsTimeoutIsResentAndAnAbortTreatsItAsPossiblyStarted) {
    {
        Rig r;
        r.initOk();
        r.begin();
        ASSERT_EQ(r.send(0, 1), SendResult::Ok);
        r.ticks(2);
        ASSERT_EQ(r.adds().size(), 1u);
        r.tick(r.cfg.produceTimeoutMs * kMs);   // expires
        r.ticks(2);
        ASSERT_EQ(r.adds().size(), 2u) << "the expired AddPartitionsToTxn was not sent again";
        EXPECT_EQ(Rig::named(*r.adds()[1]), std::vector<int32_t>{0});
        EXPECT_TRUE(r.wire.empty());
        r.answeredAdds.push_back(r.adds()[0]->corrId);   // the first one's answer never comes
        r.answerAdd(0);
        r.ticks(2);
        ASSERT_EQ(r.wire.size(), 1u);
        r.ackAll();
        Op commit = r.commit();
        r.tick();
        r.endAnswer();
        ASSERT_TRUE(commit.ready());
        EXPECT_EQ(commit.status(), OpStatus::Ok);
    }
    {
        Rig r;
        r.initOk();
        r.begin();
        ASSERT_EQ(r.sendAck(0, 1, 7), SendResult::Ok);
        r.ticks(2);
        ASSERT_EQ(r.adds().size(), 1u);   // on the wire, never answered
        Op abort = r.abort();
        r.ticks(2);
        EXPECT_EQ(r.acks.status(7), OpStatus::BrokerError) << "the abort discards the unsent record";
        ASSERT_EQ(r.endTxns(), 1u) << "a possibly-started transaction must be aborted on the coordinator";
        EXPECT_EQ(r.lastEnd()->commit, uint8_t{0});
        r.endAnswer(ec(rpc::ErrorCode::InvalidTxnState));
        ASSERT_TRUE(abort.ready());
        EXPECT_EQ(abort.status(), OpStatus::Ok);
        EXPECT_EQ(r.state(), TxnState::Ready);
        r.ticks(4, kMs);
        EXPECT_EQ(r.adds().size(), 1u) << "the cancelled AddPartitionsToTxn was re-sent after the abort";
        EXPECT_TRUE(r.wire.empty());
    }
}

// Fix-on-sight (Task 6): a sequence reset of a transactional partition (here an OutOfOrderSequence on the batch next
// in line) must never bump the epoch locally - the coordinator owns it, and data under an epoch no EndTxn ends would
// dangle.  The partition sends nothing new, the transaction can only abort, EndTxn(abort) carries the coordinator's
// epoch and the abort's KIP-360 bump restarts the sequences.
TEST(ProducerTxnDataRegression, TransactionalSequenceResetNeverBumpsTheEpochLocallyRegression) {
    Rig r;
    r.initOk(42, 3);
    r.begin();
    ASSERT_EQ(r.send(0, 1), SendResult::Ok);
    r.tick();
    r.answerAdd(0);
    r.tick();
    ASSERT_EQ(r.wire.size(), 1u);
    ASSERT_EQ(r.wire[0].epoch, 3);
    r.wire[0].acked = true;
    r.hub.onProduceResponse(r.wire[0].corrId, -1, ec(rpc::ErrorCode::OutOfOrderSequence));
    EXPECT_EQ(r.impl->producerEpoch(), 3) << "the epoch was bumped locally";
    EXPECT_EQ(r.state(), TxnState::AbortableError);
    ASSERT_EQ(r.send(0, 2), SendResult::Ok);   // Ring: the gate is still Open - the drain refuses it (FD2)
    r.ticks(4, kMs);
    EXPECT_EQ(r.impl->txnRecordsRefusedDuty(), 1u);
    for (const auto& w : r.wire) EXPECT_EQ(w.epoch, 3) << "data under a locally bumped epoch";
    Op abort = r.abort();
    r.tick();
    ASSERT_EQ(r.endTxns(), 1u);
    EXPECT_EQ(r.lastEnd()->producerEpoch, 3) << "EndTxn(abort) under a locally bumped epoch";
    r.endAnswer();
    r.tick();
    r.answer(Rig::initAnswer(r.last(rpc::InitProducerId::TEMPLATE_ID)->corrId, 42, 4));
    ASSERT_TRUE(abort.ready());
    EXPECT_EQ(abort.status(), OpStatus::Ok);
    r.begin();
    ASSERT_EQ(r.send(0, 3), SendResult::Ok);
    r.tick();
    r.answerAdd(0);
    r.tick();
    ASSERT_EQ(r.wire.size(), 2u);
    EXPECT_EQ(r.wire.back().epoch, 4);
    EXPECT_EQ(r.wire.back().baseSeq, 0);
    r.ackAll();
    Op commit = r.commit();
    r.tick();
    r.endAnswer();
    ASSERT_TRUE(commit.ready());
    EXPECT_EQ(commit.status(), OpStatus::Ok);
}

// Direct (Invoker) ingress: a claim inside a transaction is reserved in the partition builder; its committed record
// stays there until the partition is Added, then leaves in the frame.
TEST(ProducerTxnData, InvokerModeClaimInATransactionWritesTheFrameAfterAdded) {
    Rig r(Mode::Direct);
    r.initOk();
    r.begin();
    const auto g = r.impl->claimRecord(kTopic, 0, Key(uint64_t{3}), kLen, 0);
    ASSERT_EQ(g.result, SendResult::Ok);
    r.tick();
    EXPECT_TRUE(r.adds().empty()) << "an open claim is not a record yet";
    std::memset(g.base, 0xAB, kLen);
    ASSERT_TRUE(r.impl->commitClaim(g.token));
    r.ticks(3, kMs);
    ASSERT_EQ(r.adds().size(), 1u);
    EXPECT_TRUE(r.wire.empty()) << "the claim's frame left before its partition was Added";
    r.answerAdd(0);
    r.tick();
    ASSERT_EQ(r.wire.size(), 1u);
    EXPECT_EQ(r.wire[0].tags, std::vector<uint8_t>{0xAB});
    EXPECT_NE(r.wire[0].attrs & rb::kAttrTransactional, 0);
    r.ackAll();
    Op commit = r.commit();
    r.tick();
    r.endAnswer();
    ASSERT_TRUE(commit.ready());
    EXPECT_EQ(commit.status(), OpStatus::Ok);
}
