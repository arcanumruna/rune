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
    Op flush() { return apply(TxnControl::Kind::Flush); }
    /// The HANDLE side of the ring path: the op is written to the producer ring, FIFO with the records sent before
    /// it, and applied by the next pass's drain (Thread mode).  Direct mode applies at once.
    Op issue(TxnControl::Kind k) {
        TxnControl c;
        c.kind           = k;
        c.target.handler = rc::CompletionHandler{&OpLedger::on, &ops};
        c.token          = ++next;
        const rc::OpResult res = impl->issueTxnControl(c);
        EXPECT_EQ(res, rc::OpResult::Accepted);
        return Op{&ops, c.token};
    }
    /// Producer::close(): the issuer closes the intake, the duty side drains and ends the open transaction.
    Op close() {
        const Token t = ++next;
        rc::detail::OpTarget target;
        target.handler = rc::CompletionHandler{&OpLedger::on, &ops};
        (void)impl->closeIntake();
        impl->applyBeginClose(target, t, now);
        return Op{&ops, t};
    }
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
TEST(ProducerTxnDataRegression, TimedOutAddPartitionsAbortsPossiblyStartedTxnRegression) {
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
        EXPECT_EQ(r.impl->coordinatorLink()->inUse(), 0u) << "the cancelled AddPartitionsToTxn leaked its slot";
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

// Fix round 1 (review I2): a sequence reset of a transactional partition with several batches in flight.  The
// batches behind the failed one carry sequences that can never be restarted in place: they fail AT ONCE with the
// transaction's status (each record exactly once), nothing is re-sent under the old identity, and the abort goes
// out promptly - never held for the delivery timeout.
TEST(ProducerTxnDataRegression, TransactionalResetFailsTheStaleInFlightBatchesAtOnceRegression) {
    Rig r;
    r.initOk(42, 0);
    r.begin();
    ASSERT_EQ(r.sendAck(0, 1, 11), SendResult::Ok);
    r.tick();
    r.answerAdd(0);
    r.tick();
    ASSERT_EQ(r.sendAck(0, 2, 12), SendResult::Ok);
    r.tick();
    ASSERT_EQ(r.sendAck(0, 3, 13), SendResult::Ok);
    r.tick();
    ASSERT_EQ(r.wire.size(), 3u) << "three batches in flight";
    const int64_t t0 = r.now;
    for (auto& w : r.wire) w.acked = true;
    r.hub.onProduceResponse(r.wire[0].corrId, -1, ec(rpc::ErrorCode::OutOfOrderSequence));   // C4: next in line
    EXPECT_EQ(r.acks.times(12), 1) << "a stale batch was left in flight under the old identity";
    EXPECT_EQ(r.acks.times(13), 1);
    EXPECT_EQ(r.acks.status(12), OpStatus::BrokerError);
    r.hub.onProduceResponse(r.wire[1].corrId, -1, ec(rpc::ErrorCode::OutOfOrderSequence));   // late: ignored
    r.hub.onProduceResponse(r.wire[2].corrId, -1, ec(rpc::ErrorCode::OutOfOrderSequence));
    EXPECT_EQ(r.state(), TxnState::AbortableError);
    Op abort = r.abort();
    r.ticks(2, kMs);
    ASSERT_EQ(r.endTxns(), 1u) << "the abort waited for the stale batches";
    EXPECT_EQ(r.lastEnd()->producerEpoch, 0);
    r.endAnswer();
    r.tick(kMs);
    r.answer(Rig::initAnswer(r.last(rpc::InitProducerId::TEMPLATE_ID)->corrId, 42, 1));
    ASSERT_TRUE(abort.ready());
    EXPECT_EQ(abort.status(), OpStatus::Ok);
    EXPECT_LT(r.now - t0, r.cfg.produceTimeoutMs * kMs / 100) << "the abort was not prompt";
    r.ticks(4, kMs);
    EXPECT_EQ(r.wire.size(), 3u) << "a stale batch was re-sent";
    for (const Token t : {Token{11}, Token{12}, Token{13}}) EXPECT_EQ(r.acks.times(t), 1);
}

// Task 8 pre-requisite: the FD2 refusal pass refuses RECORDS only.  A message it does not own (Task 8:
// kMsgTxnControl - e.g. an abort issued in AbortableError) is never swallowed: the refusal pass stops at it, the
// normal drain takes it and ends the pass there, and the records behind it wait for the next pass.  A plain
// producer's drain also ends its pass at such a message.
TEST(ProducerTxnDataRegression, RefusalPassStopsAtAMessageItDoesNotOwnRegression) {
    constexpr int32_t kForeign = 99;
    const std::array<uint8_t, 4> payload{1, 2, 3, 4};
    {
        Rig r;
        r.initOk();
        r.begin();
        r.impl->testPoison(OpStatus::BrokerError, uec(rpc::ErrorCode::NotAuthorized), rp::AbortKind::Plain);
        ASSERT_EQ(r.send(0, 1), SendResult::Ok);
        ASSERT_TRUE(r.impl->testWriteRing(kForeign, payload));
        ASSERT_EQ(r.send(0, 2), SendResult::Ok);
        EXPECT_EQ(r.impl->drainRing(r.now, 16), 2) << "one refused record + the foreign message";
        EXPECT_EQ(r.impl->txnRecordsRefusedDuty(), 1u);
        EXPECT_EQ(r.impl->ringForeignMessages(), 1u) << "the refusal pass swallowed a message it does not own";
        EXPECT_EQ(r.impl->lastDrainStop(), ProducerImpl::DrainStop::FlushToken) << "the pass did not stop at it";
        EXPECT_LT(r.impl->testRingHead(), r.impl->testRingTail()) << "the record behind it was read in the same pass";
        EXPECT_EQ(r.impl->drainRing(r.now, 16), 1);
        EXPECT_EQ(r.impl->txnRecordsRefusedDuty(), 2u);
        Op abort = r.abort();
        r.tick();
        ASSERT_TRUE(abort.ready());
    }
    {
        ProducerImpl impl(txnCfg(""), 1 << 16);   // plain
        const auto v = val(1);
        ASSERT_EQ(impl.trySubmit(kTopic, 0, Key(uint64_t{1}), v, 0), SendResult::Ok);
        ASSERT_TRUE(impl.testWriteRing(kForeign, payload));
        ASSERT_EQ(impl.trySubmit(kTopic, 0, Key(uint64_t{1}), v, 0), SendResult::Ok);
        EXPECT_EQ(impl.drainRing(kT0, 16), 2);
        EXPECT_EQ(impl.recordsSent(), 1u) << "records after the message joined the same pass";
        EXPECT_EQ(impl.lastDrainStop(), ProducerImpl::DrainStop::FlushToken);
        EXPECT_EQ(impl.drainRing(kT0, 16), 1);
        EXPECT_EQ(impl.recordsSent(), 2u);
    }
}

// #322: an off-owner credit abort in the ring of a transactional producer is OWNED by the refusal pass (counted,
// never left blocking the ring, never counted as a foreign message).
TEST(ProducerClaimRegression, OffThreadCreditLostMessageIsSkippedByTheTxnRefusalPassRegression) {
    const std::array<uint8_t, 4> payload{1, 2, 3, 4};
    Rig r;
    r.initOk();
    r.begin();
    r.impl->testPoison(OpStatus::BrokerError, uec(rpc::ErrorCode::NotAuthorized), rp::AbortKind::Plain);
    ASSERT_TRUE(r.impl->testWriteRing(rp::kMsgClaimCreditLost, payload));
    ASSERT_TRUE(r.impl->testWriteRing(rp::kMsgTier2Discard, payload));  // short token: skipped, never blocks
    for (int i = 0; i < 8 && r.impl->testRingHead() < r.impl->testRingTail(); ++i) r.impl->drainRing(r.now, 16);
    EXPECT_EQ(r.impl->testRingHead(), r.impl->testRingTail()) << "the ring did not drain";
    EXPECT_EQ(r.impl->claimCreditLost(), 1u);
    EXPECT_EQ(r.impl->ringForeignMessages(), 0u);
    Op abort = r.abort();
    r.tick();
    ASSERT_TRUE(abort.ready());
}

// FD5: more new partitions than one 4096-byte AddPartitionsToTxn holds are split across request slots, in the same
// pass, each partition named once; every partition's data goes out once all are OK.
TEST(ProducerTxnData, AddPartitionsSplitAcrossSlotsWhenOneFrameIsFull) {
    const std::string longId(255, 't');   // header 24 + body 10 + id 2 + 255 + count 2 = 293: 475 entries per frame
    constexpr std::size_t kFixed = 293;
    constexpr int kParts = 500;
    Rig r(Mode::Ring, txnCfg(longId));
    r.initOk();
    r.begin();
    for (int p = 0; p < kParts; ++p) ASSERT_EQ(r.send(p, static_cast<uint8_t>(p)), SendResult::Ok);
    // Every record drained and every partition queued in ONE flush (the hub drains at most 256 messages a cycle),
    // then one duty pass sends the AddPartitionsToTxn requests.
    while (r.impl->drainRing(r.now, 1024) > 0) {}
    (void)r.impl->flushBatches(r.now, 42, r.sink);
    r.tick();
    const auto adds = r.adds();
    ASSERT_EQ(adds.size(), 2u) << "one frame cannot hold 500 partitions: two requests in one pass";
    std::size_t named = 0;
    for (const OfferedFrame* f : adds) {
        ASSERT_LE(f->len, rp::CoordinatorLink::kCoordFrameCap);
        named += (f->len - kFixed) / 8;
    }
    EXPECT_EQ((adds[0]->len - kFixed) / 8, 475u);
    EXPECT_EQ(named, static_cast<std::size_t>(kParts)) << "a partition missing from (or doubled in) the requests";
    EXPECT_NE(adds[0]->corrId, adds[1]->corrId);
    EXPECT_TRUE(r.wire.empty());
    r.answerAdd(0);
    r.tick();
    EXPECT_EQ(r.wire.size(), 475u) << "the first request's partitions go out";
    r.answerAdd(0);
    r.ticks(2);
    EXPECT_EQ(r.wire.size(), static_cast<std::size_t>(kParts));
    r.ackAll();
    Op commit = r.commit();
    r.tick();
    r.endAnswer();
    ASSERT_TRUE(commit.ready());
    EXPECT_EQ(commit.status(), OpStatus::Ok);
    EXPECT_EQ(r.impl->coordinatorLink()->inUse(), 0u);
}

// ============================================================================================================
// Phase 2b F Task 9: the data / flush / sequencing cases of the removed transactional producer's test file, on the
// unified Producer (old -> new names are in the Task 9 commit message).
// ============================================================================================================
namespace {

constexpr int16_t kNotLeaderCode = static_cast<int16_t>(rpc::ErrorCode::NotLeader);
constexpr int16_t kOosCode       = static_cast<int16_t>(rpc::ErrorCode::OutOfOrderSequence);
constexpr int16_t kUnknownPid    = static_cast<int16_t>(rpc::ErrorCode::UnknownProducerId);

/// begin + `n` one-record batches on `part` (each leaves in its own pass), the partition's AddPartitionsToTxn
/// answered OK first.  Returns with every batch on the wire (r.wire) and none acked.
void sendBatches(Rig& r, int n, int32_t part = 0) {
    r.begin();
    ASSERT_EQ(r.send(part, 1), SendResult::Ok);
    r.tick();
    r.answerAdd(0);
    r.tick();
    for (int i = 1; i < n; ++i) {
        ASSERT_EQ(r.send(part, static_cast<uint8_t>(i + 1)), SendResult::Ok);
        r.tick();
    }
    ASSERT_EQ(r.wire.size(), static_cast<std::size_t>(n)) << "every batch on the wire";
}

/// The answer of wire batch `i`.
void answerWire(Rig& r, std::size_t i, int16_t code, int64_t base = 0, int64_t logStart = -1) {
    ASSERT_LT(i, r.wire.size());
    r.wire[i].acked = true;
    r.hub.onProduceResponse(r.wire[i].corrId, code == 0 ? base : -1, code, logStart);
}

/// A committed transaction whose single record was acked at `off`.
void committedTxnAckedAt(Rig& r, int64_t off) {
    const std::size_t before = r.wire.size();
    r.begin();
    ASSERT_EQ(r.send(0, 1), SendResult::Ok);
    r.tick();
    r.answerAdd(0);
    r.tick();
    ASSERT_EQ(r.wire.size(), before + 1);
    answerWire(r, before, 0, off);
    Op commit = r.commit();
    r.tick();
    r.endAnswer();
    ASSERT_TRUE(commit.ready());
    ASSERT_EQ(commit.status(), OpStatus::Ok);
}

/// The oldest unanswered AddPartitionsToTxn naming `part`.
const OfferedFrame* addFor(const Rig& r, int32_t part) {
    for (const OfferedFrame* f : r.adds()) {
        if (std::find(r.answeredAdds.begin(), r.answeredAdds.end(), f->corrId) != r.answeredAdds.end()) continue;
        const auto parts = Rig::named(*f);
        if (std::find(parts.begin(), parts.end(), part) != parts.end()) return f;
    }
    return nullptr;
}

/// Answers the AddPartitionsToTxn that names `part`.
void answerAddFor(Rig& r, int32_t part, int16_t code) {
    const OfferedFrame* f = addFor(r, part);
    ASSERT_NE(f, nullptr) << "no AddPartitionsToTxn names partition " << part;
    r.answeredAdds.push_back(f->corrId);
    r.answer(FakeCoordinator::answer(rp::ReqKind::AddPartitions, f->corrId, code));
}

}  // namespace

// ---- EOS: the idempotent retry keeps the batch identity --------------------------------------------------------

TEST(ProducerTxnData, IdempotentRetryReusesBaseSequence) {
    Rig r;
    r.initOk(99, 0);
    sendBatches(r, 2);
    EXPECT_EQ(r.wire[0].baseSeq, 0);
    EXPECT_EQ(r.wire[1].baseSeq, 1);
    EXPECT_NE(r.wire[0].corrId, r.wire[1].corrId);
    const int64_t firstCorr = r.wire[0].corrId;
    answerWire(r, 1, 0, 1);
    const std::size_t before = r.wire.size();
    r.hub.onProduceResponse(firstCorr, -1, kNotLeaderCode);
    r.impl->resendRouteParked();   // a NotLeader re-offers after fresh metadata
    r.tick();
    ASSERT_EQ(r.wire.size(), before + 1) << "a retriable error must re-send";
    EXPECT_EQ(r.wire.back().baseSeq, 0) << "the retry MUST reuse baseSequence 0";
    // EOS follow-ups I-2: same bytes and baseSequence, but a NEW attempt id (an answer to the first attempt can
    // never decide the retry).
    EXPECT_NE(r.wire.back().corrId, firstCorr) << "the retry is a new attempt";
    EXPECT_NE(r.wire.back().corrId, r.wire[1].corrId);
    EXPECT_EQ(r.wire.back().pid, 99);
    EXPECT_EQ(r.wire.back().epoch, 0) << "no fencing on a retriable error";
}

// I-1 (Kafka parity): OutOfOrderSequence on a batch BEHIND an unresolved older batch means an earlier frame was
// lost - retried once the older batch resolves, never a poisoned (abortable) transaction.
TEST(ProducerTxnDataRegression, OutOfOrderBehindAnOlderBatchIsRetriedNotPoisonedRegression) {
    Rig r;
    r.initOk(77, 0);
    sendBatches(r, 3);
    answerWire(r, 1, kOosCode);   // seq 0 was lost: seq 1 and 2 meet the gap
    answerWire(r, 2, kOosCode);
    EXPECT_EQ(r.state(), TxnState::InTransaction) << "an OOS behind a live older batch is retried, not poisoned";
    std::size_t n = r.wire.size();
    r.tick();
    // Fast retransmit of the gap frame (seq 0) only; seq 1 and 2 wait for it.
    ASSERT_EQ(r.wire.size(), n + 1);
    EXPECT_EQ(r.wire.back().baseSeq, 0);
    answerWire(r, r.wire.size() - 1, 0, 100);
    n = r.wire.size();
    r.tick();   // the parked batches go out again, in sequence order
    ASSERT_EQ(r.wire.size(), n + 2);
    EXPECT_EQ(r.wire[n].baseSeq, 1);
    EXPECT_EQ(r.wire[n + 1].baseSeq, 2);
    answerWire(r, n, 0, 101);
    answerWire(r, n + 1, 0, 102);
    EXPECT_EQ(r.state(), TxnState::InTransaction);
    EXPECT_FALSE(r.impl->hasInFlight());
}

// M-2: the producer's in-flight batches per partition are clamped to kIdempotentMaxInFlightBatches (the broker's
// dedup window); the rest wait.
TEST(ProducerTxnDataRegression, InFlightPerPartitionClampedToTheBrokerWindowRegression) {
    rc::ProducerConfig cfg = txnCfg();
    cfg.maxInFlight = 64;
    Rig r(Mode::Ring, cfg);
    r.initOk(77, 0);
    constexpr int kWindow = rpc::kIdempotentMaxInFlightBatches;
    r.begin();
    ASSERT_EQ(r.send(0, 1), SendResult::Ok);
    r.tick();
    r.answerAdd(0);
    r.tick();
    for (int i = 1; i < 40; ++i) {
        ASSERT_EQ(r.send(0, static_cast<uint8_t>(i + 1)), SendResult::Ok);
        r.tick();
    }
    ASSERT_EQ(r.wire.size(), static_cast<std::size_t>(kWindow)) << "at most the broker's window in flight per partition";
    EXPECT_EQ(r.impl->partitionState(kTopic, 0)->inflight.live(), static_cast<uint32_t>(kWindow));
    answerWire(r, 0, 0, 100);
    r.tick();
    ASSERT_EQ(r.wire.size(), static_cast<std::size_t>(kWindow) + 1) << "one acked -> one more";
    EXPECT_EQ(r.wire.back().baseSeq, kWindow);
}

// I-2: an error answer of a SUPERSEDED attempt never decides the batch.
TEST(ProducerTxnDataRegression, ErrorOfASupersededAttemptIsIgnoredRegression) {
    Rig r;
    r.initOk(77, 0);
    sendBatches(r, 1);
    const int64_t firstCorr = r.wire[0].corrId;
    r.hub.onProduceResponse(firstCorr, -1, kNotLeaderCode);   // retriable
    r.impl->resendRouteParked();                               // re-offered after fresh metadata
    r.tick();
    ASSERT_EQ(r.wire.size(), 2u);
    r.hub.onProduceResponse(firstCorr, -1, kOosCode);          // the FIRST attempt's late answer
    EXPECT_EQ(r.state(), TxnState::InTransaction) << "a superseded attempt's error is ignored";
    answerWire(r, 1, 0, 100);
    EXPECT_EQ(r.state(), TxnState::InTransaction);
    EXPECT_FALSE(r.impl->hasInFlight());
}

// The per-partition int32 sequence never overflows: before the counter would pass INT32_MAX the transaction
// becomes abortable with an epoch bump (the coordinator owns the epoch); no frame ever carries a wrapped sequence.
TEST(ProducerTxnDataRegression, SequenceCounterNeverOverflowsInt32Regression) {
    Rig r;
    r.initOk(77, 0);
    sendBatches(r, 1);
    answerWire(r, 0, 0, 100);
    r.impl->debugSetNextSequenceForTest(kTopic, 0, INT32_MAX - 1);
    const std::size_t before = r.wire.size();
    for (int i = 0; i < 3; ++i) {   // one record per batch (the old producer sent one batch per send)
        ASSERT_EQ(r.send(0, static_cast<uint8_t>(10 + i)), SendResult::Ok);
        r.tick();
    }
    r.ticks(2);
    for (std::size_t i = before; i < r.wire.size(); ++i) EXPECT_GE(r.wire[i].baseSeq, 0) << "a wrapped sequence went out";
    ASSERT_EQ(r.wire.size(), before + 1) << "only INT32_MAX - 1 fits (the next counter value must stay <= INT32_MAX)";
    EXPECT_EQ(r.wire[before].baseSeq, INT32_MAX - 1);
    // Changed timing: the batches behind it wait for the one in flight (as on the plain producer: nothing is
    // abandoned while it may still be stored); once it is acked the transaction becomes abortable.
    EXPECT_EQ(r.state(), TxnState::InTransaction);
    answerWire(r, before, 0, 900);
    r.ticks(2);
    EXPECT_EQ(r.state(), TxnState::AbortableError) << "abortable with an epoch bump before the overflow";
    for (std::size_t i = before; i < r.wire.size(); ++i) EXPECT_GE(r.wire[i].baseSeq, 0) << "a wrapped sequence went out";
}

// NEW-3: ProduceResponse{NotLeader} is a routine partition move, never a reason to abort the transaction.  The
// batch parks - not re-sent to the old owner, nothing new produced on the partition - until fresh metadata arrived;
// then it is re-offered with the SAME frame identity (producerId, epoch, baseSequence) as a NEW attempt.
TEST(ProducerTxnDataRegression, NotLeaderParksUntilMetadataThenReoffersSameIdentityRegression) {
    Rig r;
    r.initOk(77, 3);
    sendBatches(r, 2);
    std::vector<WireBatch> first(r.wire.begin(), r.wire.end());
    std::vector<WireBatch> cur = first;
    for (int round = 0; round < 8; ++round) {
        SCOPED_TRACE(round);
        for (const auto& f : cur) r.hub.onProduceResponse(f.corrId, -1, kNotLeaderCode);
        if (round == 0) ASSERT_EQ(r.send(0, 9), SendResult::Ok);
        const std::size_t before = r.wire.size();
        r.ticks(5);
        EXPECT_EQ(r.wire.size(), before) << "re-sent (or a new batch sent) before fresh metadata";
        ASSERT_EQ(r.state(), TxnState::InTransaction) << "a partition move aborted the transaction";
        r.impl->resendRouteParked();   // the MetadataResponse arrived (new route installed)
        r.tick();
        // Changed: the record held while parked leaves in the same pass, BEHIND the re-offered batches (in
        // sequence order); it used to wait for the acks.  Nothing new ever leaves before the re-offers.
        ASSERT_GE(r.wire.size(), before + cur.size()) << "the batches were not re-offered";
        const std::vector<WireBatch> prev = cur;  // the previous offer
        cur.assign(r.wire.begin() + static_cast<std::ptrdiff_t>(before), r.wire.end());
        for (std::size_t i = 0; i < cur.size(); ++i) {
            EXPECT_EQ(cur[i].pid, 77);
            EXPECT_EQ(cur[i].epoch, 3);
            EXPECT_EQ(cur[i].baseSeq, static_cast<int32_t>(i)) << "same identity, in order";
            if (i < std::min(prev.size(), cur.size())) EXPECT_NE(cur[i].corrId, prev[i].corrId) << "a new attempt";
        }
        ASSERT_LE(cur.size(), 3u);
    }
    EXPECT_EQ(cur.size(), 3u) << "the held record went out behind the re-offers";
    for (const auto& f : cur) r.hub.onProduceResponse(f.corrId, 100, 0);
    r.tick();
    EXPECT_EQ(r.state(), TxnState::InTransaction);
    EXPECT_FALSE(r.impl->hasInFlight());
}

// NEW-4(a): DuplicateSequence is a stored batch - the transaction neither retries nor aborts.
TEST(ProducerTxnDataRegression, DuplicateSequenceIsAStoredBatchRegression) {
    Rig r;
    r.initOk(77, 0);
    sendBatches(r, 1);
    answerWire(r, 0, static_cast<int16_t>(rpc::ErrorCode::DuplicateSequence));
    EXPECT_FALSE(r.impl->hasInFlight()) << "a DuplicateSequence batch is stored";
    const std::size_t before = r.wire.size();
    r.ticks(2);
    EXPECT_EQ(r.wire.size(), before) << "a stored batch was re-sent";
    EXPECT_EQ(r.state(), TxnState::InTransaction);
}

// D6: UnknownProducerId is handled exactly like OutOfOrderSequence by a transactional producer: behind an
// unresolved older batch it is retried, never poisoned...
TEST(ProducerTxnDataRegression, UnknownProducerIdBehindAnOlderBatchIsRetriedNotPoisonedRegression) {
    Rig r;
    r.initOk(77, 0);
    sendBatches(r, 3);
    answerWire(r, 1, kUnknownPid);
    answerWire(r, 2, kUnknownPid);
    EXPECT_EQ(r.state(), TxnState::InTransaction);
    std::size_t n = r.wire.size();
    r.tick();
    ASSERT_EQ(r.wire.size(), n + 1);
    EXPECT_EQ(r.wire.back().baseSeq, 0) << "fast retransmit of the gap frame";
    answerWire(r, r.wire.size() - 1, 0, 100);
    n = r.wire.size();
    r.tick();
    ASSERT_EQ(r.wire.size(), n + 2);
    answerWire(r, n, 0, 101);
    answerWire(r, n + 1, 0, 102);
    EXPECT_EQ(r.state(), TxnState::InTransaction);
    EXPECT_FALSE(r.impl->hasInFlight());
}

// ... and on the next-in-line batch the transaction becomes abortable, never a fatal error and never silently
// dropped; the app-visible code is 28 (UnknownProducerId), not 20.
TEST(ProducerTxnDataRegression, UnknownProducerIdOnTheHeadMakesTheTxnAbortableRegression) {
    Rig r;
    r.initOk(77, 0);
    sendBatches(r, 1);
    answerWire(r, 0, kUnknownPid);
    EXPECT_EQ(r.state(), TxnState::AbortableError);
    Op commit = r.commit();
    ASSERT_TRUE(commit.ready());
    EXPECT_EQ(commit.code(), uec(rpc::ErrorCode::UnknownProducerId)) << "D6 review M2: the app-visible code is 28, not 20";
}

// D6 review M2 (KIP-360), CHANGED on the unified producer: UnknownProducerId with lastAckedOffset < logStartOffset
// (retention removed every record of ours) used to restart the partition's sequences at 0 and KEEP the transaction.
// A transactional epoch belongs to the coordinator (Task 6), so the sequences cannot restart in place: the
// transaction fails abortable with 28 and the abort's epoch bump restarts them - the NEXT transaction's records go
// out at sequence 0 under the bumped epoch.
TEST(ProducerTxnDataRegression, UnknownProducerIdAfterRetentionRestartsSequencesAndKeepsTheTxnRegression) {
    Rig r;
    r.initOk(77, 0);
    committedTxnAckedAt(r, /*off=*/10);   // lastAckedOffset 10; sequences go on at 1
    r.begin();
    ASSERT_EQ(r.send(0, 1), SendResult::Ok);
    r.tick();
    r.answerAdd(0);
    r.tick();
    const std::size_t first = r.wire.size() - 1;
    ASSERT_EQ(r.wire[first].baseSeq, 1);
    // Retention + failover: the broker forgot us; its log starts at 50 > 10.
    answerWire(r, first, kUnknownPid, 0, /*logStart=*/50);
    EXPECT_EQ(r.state(), TxnState::AbortableError) << "changed: a transactional sequence restart can only abort";
    Op commit = r.commit();
    ASSERT_TRUE(commit.ready());
    EXPECT_EQ(commit.status(), OpStatus::BrokerError);
    EXPECT_EQ(commit.code(), uec(rpc::ErrorCode::UnknownProducerId));
    Op abort = r.abort();
    r.tick();
    r.endAnswer();
    r.tick();   // the abort's epoch bump restarts every sequence
    ASSERT_NE(r.last(rpc::InitProducerId::TEMPLATE_ID), nullptr);
    r.answer(Rig::initAnswer(r.last(rpc::InitProducerId::TEMPLATE_ID)->corrId, 77, 1));
    ASSERT_TRUE(abort.ready());
    EXPECT_EQ(abort.status(), OpStatus::Ok);
    EXPECT_EQ(r.state(), TxnState::Ready);
    // The next transaction continues in the restarted sequence space under the bumped epoch, and commits.
    r.begin();
    ASSERT_EQ(r.send(0, 2), SendResult::Ok);
    r.tick();
    r.answerAdd(0);
    r.tick();
    ASSERT_EQ(r.wire.size(), first + 2);
    EXPECT_EQ(r.wire.back().baseSeq, 0) << "sequences restart at 0";
    EXPECT_EQ(r.wire.back().epoch, 1) << "under the bumped epoch";
    EXPECT_NE(r.wire.back().attrs & rb::kAttrTransactional, 0);
    EXPECT_TRUE(r.wire.back().crcOk) << "re-stamped batch CRC";
    answerWire(r, r.wire.size() - 1, 0, 60);
    Op c2 = r.commit();
    r.tick();
    r.endAnswer();
    ASSERT_TRUE(c2.ready());
    EXPECT_EQ(c2.status(), OpStatus::Ok);
}

TEST(ProducerTxnDataRegression, UnknownProducerIdWithAckedDataAboveTheLogStartIsAbortableWithCode28Regression) {
    Rig r;
    r.initOk(77, 0);
    committedTxnAckedAt(r, /*off=*/10);
    r.begin();
    ASSERT_EQ(r.send(0, 1), SendResult::Ok);
    r.tick();
    r.answerAdd(0);
    r.tick();
    answerWire(r, r.wire.size() - 1, kUnknownPid, 0, /*logStart=*/5);   // acked offset 10 >= 5: not retention
    EXPECT_EQ(r.state(), TxnState::AbortableError);
    Op commit = r.commit();
    ASSERT_TRUE(commit.ready());
    EXPECT_EQ(commit.status(), OpStatus::BrokerError);
    EXPECT_EQ(commit.code(), uec(rpc::ErrorCode::UnknownProducerId)) << "the app sees 28, not 20";
}

// E2 (ruling 2): a batch answered TransientLogFull (42) is re-offered (same identity, a new attempt) after a
// backoff, never spends the retry budget and never aborts the transaction.
TEST(ProducerTxnDataRegression, TransientLogFullBacksOffWithoutSpendingRetriesRegression) {
    Rig r;
    r.initOk(77, 0);
    sendBatches(r, 1);
    const int32_t seq0 = r.wire[0].baseSeq;
    for (int round = 0; round < 3 * 5 /* > kMaxRetries */; ++round) {
        SCOPED_TRACE(round);
        const std::size_t before = r.wire.size();
        r.hub.onProduceResponse(r.wire.back().corrId, -1, static_cast<int16_t>(rpc::ErrorCode::TransientLogFull));
        r.tick();
        ASSERT_EQ(r.wire.size(), before) << "re-sent on the very next cycle (no backoff)";
        int waited = 0;   // the backoff is wall-clock: 10 ms duty-clock steps
        while (r.wire.size() == before) {
            r.tick(10 * kMs);
            ASSERT_LE(++waited, 64);
        }
        EXPECT_EQ(r.wire.back().baseSeq, seq0) << "same identity";
        ASSERT_EQ(r.state(), TxnState::InTransaction) << "42 poisoned the transaction";
    }
    answerWire(r, r.wire.size() - 1, 0, 7);
    EXPECT_EQ(r.state(), TxnState::InTransaction);
    EXPECT_FALSE(r.impl->hasInFlight());
}

// Issue #112 (client version 13): ConcurrentTransactions (27) on a produce = the leader is verifying the
// partition's registration with the coordinator (KIP-890 part 1): nothing was appended.  Re-sent unchanged
// (same identity) after a SHORT backoff that grows per attempt (capped), never spending the retry budget, never
// aborting the transaction.  Before the fix 27 was a broker error: the transaction became abortable.
TEST(ProducerTxnDataRegression, ConcurrentTransactionsIsRetriedWithAShortBackoffNeverAbortingRegression) {
    Rig r;
    r.initOk(77, 0);
    sendBatches(r, 1);
    const int32_t seq0 = r.wire[0].baseSeq;
    const int16_t epoch0 = r.wire[0].epoch;
    int firstWait = -1;
    for (int round = 0; round < 3 * 5 /* > kMaxRetries */; ++round) {
        SCOPED_TRACE(round);
        const std::size_t before = r.wire.size();
        r.hub.onProduceResponse(r.wire.back().corrId, -1, static_cast<int16_t>(rpc::ErrorCode::ConcurrentTransactions));
        ASSERT_EQ(r.state(), TxnState::InTransaction) << "27 poisoned the transaction";
        int waited = 0;   // 1 ms duty-clock steps
        while (r.wire.size() == before) {
            r.tick(kMs);
            ASSERT_LE(++waited, 200) << "never re-sent (the backoff is capped by the retriable backoff)";
        }
        if (round == 0) firstWait = waited;
        EXPECT_EQ(r.wire.back().baseSeq, seq0) << "same identity";
        EXPECT_EQ(r.wire.back().epoch, epoch0);
    }
    EXPECT_LE(firstWait, 2) << "the first retry must follow within a broker pass or a plane round trip";
    answerWire(r, r.wire.size() - 1, 0, 7);
    EXPECT_EQ(r.state(), TxnState::InTransaction);
    EXPECT_FALSE(r.impl->hasInFlight());
    Op c = r.commit();
    r.tick();
    r.endAnswer();
    ASSERT_TRUE(c.ready());
    EXPECT_EQ(c.status(), OpStatus::Ok);
}

// PR #589 adversarial review (#112, concurrency): two pipelined batches of one partition are both answered
// ConcurrentTransactions, the FIRST one twice (its backoff doubles), so the second one's retry reaches the leader
// first.  A leader that has verified the partition then answers the second OutOfOrderSequence (its predecessor is
// not stored yet).  The client must park it behind the first, never fail or abort: both end up stored once, in
// order, and the transaction commits.
TEST(ProducerTxnDataRegression, ConcurrentTransactionsBackoffsReorderPipelinedBatchesWithoutAbortingAdversarialRegression) {
    Rig r;
    r.initOk(77, 0);
    sendBatches(r, 2);
    const int32_t seq0 = r.wire[0].baseSeq, seq1 = r.wire[1].baseSeq;
    ASSERT_LT(seq0, seq1);
    constexpr int16_t k27 = static_cast<int16_t>(rpc::ErrorCode::ConcurrentTransactions);
    answerWire(r, 0, k27);
    // seq0's first retry: answered 27 again (still verifying) -> its next backoff doubles.
    for (int i = 0; i < 200 && r.wire.size() == 2; ++i) r.tick(kMs / 10);
    ASSERT_EQ(r.wire.size(), 3u);
    ASSERT_EQ(r.wire[2].baseSeq, seq0);
    answerWire(r, 2, k27);
    answerWire(r, 1, k27);   // seq1's first refusal: the shorter backoff
    // The verified leader from here on: in-order sequences are stored, a gap is OutOfOrderSequence.
    int32_t expected = seq0;
    int64_t nextOffset = 0;
    std::size_t answered = 3;
    bool sawReorder = false;
    for (int i = 0; i < 4000 && r.impl->hasInFlight(); ++i) {
        r.tick(kMs / 10);
        while (answered < r.wire.size()) {
            const WireBatch& w = r.wire[answered];
            if (w.baseSeq == expected) {
                answerWire(r, answered, 0, nextOffset);
                nextOffset += static_cast<int64_t>(w.tags.size());
                expected += static_cast<int32_t>(w.tags.size());
            } else if (w.baseSeq < expected) {
                answerWire(r, answered, 0, -1);   // a duplicate of a stored batch: OK, offset unknown
            } else {
                sawReorder = true;
                answerWire(r, answered, kOosCode);
            }
            ++answered;
        }
    }
    EXPECT_TRUE(sawReorder) << "the attack did not reorder the retries (test is vacuous)";
    EXPECT_FALSE(r.impl->hasInFlight()) << "a batch never completed";
    EXPECT_EQ(expected, seq1 + 1) << "both batches stored, in order";
    EXPECT_EQ(r.state(), TxnState::InTransaction) << "the reorder poisoned the transaction";
    Op c = r.commit();
    r.tick();
    r.endAnswer();
    ASSERT_TRUE(c.ready());
    EXPECT_EQ(c.status(), OpStatus::Ok);
}

// ---- flush ------------------------------------------------------------------------------------------------------

// E3 (DR tx_read_committed): Kafka flush() completes once every record sent so far is ACKED (produced and durable),
// without ending the transaction.  Without it an abort drops unflushed records, so aborted data never reaches the
// log and read_uncommitted consumers cannot see it.
TEST(ProducerTxnDataRegression, FlushWaitsForEveryRecordThenAbortDropsNothing) {
    for (const Mode mode : {Mode::Ring, Mode::Direct}) {
        SCOPED_TRACE(mode == Mode::Ring ? "Ring" : "Direct");
        Rig r(mode);
        r.initOk();
        r.begin();
        ASSERT_EQ(r.send(0, 1), SendResult::Ok);
        ASSERT_EQ(r.send(0, 2), SendResult::Ok);
        Op flush = r.issue(TxnControl::Kind::Flush);
        r.tick();
        EXPECT_FALSE(flush.ready()) << "AddPartitions not answered: nothing produced yet";
        r.answerAdd(0);
        r.ticks(2);
        ASSERT_GE(r.wire.size(), 1u) << "flush produces the waiting records";
        EXPECT_EQ(r.tagsFor(0), (std::vector<uint8_t>{1, 2}));
        EXPECT_FALSE(flush.ready()) << "records in flight";
        r.ackAll();
        r.tick();
        ASSERT_TRUE(flush.ready());
        EXPECT_EQ(flush.status(), OpStatus::Ok);
        EXPECT_EQ(r.endTxns(), 0u) << "flush never ends the transaction";
        EXPECT_EQ(r.state(), TxnState::InTransaction);
        // The abort now only sends EndTxn(abort): the two aborted records are already in the log.
        const std::size_t framesBefore = r.wire.size();
        Op abort = r.abort();
        r.tick();
        ASSERT_EQ(r.endTxns(), 1u);
        EXPECT_EQ(r.wire.size(), framesBefore) << "the abort produced something";
        EXPECT_EQ(r.lastEnd()->commit, uint8_t{0});
        r.endAnswer();
        r.tick();
        ASSERT_TRUE(abort.ready());
        EXPECT_EQ(abort.status(), OpStatus::Ok);
    }
}

// B-MEDIUM-1: a flush whose records an abort dropped (before they were produced) must not complete Ok.
TEST(ProducerTxnDataRegression, FlushFailsWhenAbortDropsItsRecords) {
    for (const Mode mode : {Mode::Ring, Mode::Direct}) {
        SCOPED_TRACE(mode == Mode::Ring ? "Ring" : "Direct");
        Rig r(mode);
        r.initOk();
        r.begin();
        ASSERT_EQ(r.send(0, 1), SendResult::Ok);
        Op flush = r.issue(TxnControl::Kind::Flush);
        Op abort = r.issue(TxnControl::Kind::Abort);   // same duty pass, before the record's partition was added
        r.ticks(4);
        ASSERT_TRUE(flush.ready());
        EXPECT_NE(flush.status(), OpStatus::Ok) << "the flushed record was dropped, never produced";
        EXPECT_TRUE(r.wire.empty());
        ASSERT_TRUE(abort.ready());
    }
}

// B-MEDIUM-3: a poisoned transaction (delivery timed out) completes the pending flush with the poison status.
TEST(ProducerTxnDataRegression, FlushCompletesWithThePoisonStatus) {
    Rig r;
    r.initOk();
    r.begin();
    ASSERT_EQ(r.send(0, 1), SendResult::Ok);
    r.tick();
    r.answerAdd(0);
    r.tick();
    ASSERT_EQ(r.wire.size(), 1u);
    Op flush = r.flush();
    r.now += r.cfg.deliveryTimeoutMs * kMs;   // the batch expired
    r.sweep();
    r.tick();
    ASSERT_TRUE(flush.ready());
    EXPECT_EQ(flush.status(), OpStatus::DeliveryFailed);
    EXPECT_EQ(r.state(), TxnState::AbortableError);
}

// B-MEDIUM-3: fencing while a flush is pending completes it Fenced; a flush in FatalError is refused Fenced.
TEST(ProducerTxnDataRegression, FlushFencedWhenTheProducerIsFenced) {
    Rig r;
    r.initOk();
    r.begin();
    ASSERT_EQ(r.send(0, 1), SendResult::Ok);
    r.tick();
    r.answerAdd(0);
    r.tick();
    ASSERT_EQ(r.wire.size(), 1u);
    Op flush  = r.flush();
    Op commit = r.commit();
    answerWire(r, 0, static_cast<int16_t>(rpc::ErrorCode::ProducerFenced));
    r.tick();
    ASSERT_TRUE(flush.ready());
    if (r.state() == TxnState::FatalError) {
        EXPECT_EQ(flush.status(), OpStatus::Fenced);
        Op again = r.flush();
        r.tick();
        ASSERT_TRUE(again.ready());
        EXPECT_EQ(again.status(), OpStatus::Fenced);
    } else {
        EXPECT_NE(flush.status(), OpStatus::Ok) << "a fenced produce never counts as flushed";
    }
    EXPECT_TRUE(commit.ready());
}

// B-MEDIUM-3: close while a flush is pending completes it (never left dangling), not Ok for dropped records.
TEST(ProducerTxnDataRegression, FlushCompletesWhenTheProducerCloses) {
    Rig r(Mode::Direct);
    r.initOk();
    r.begin();
    ASSERT_EQ(r.send(0, 1), SendResult::Ok);
    Op flush = r.flush();
    Op close = r.close();
    r.ticks(5);
    ASSERT_TRUE(flush.ready());
    EXPECT_NE(flush.status(), OpStatus::Ok);
}

// TX-6b + fix round 1 (C-1): a batch that times out poisons the transaction: the commit completes DeliveryFailed
// without an EndTxn and the producer is AbortableError - the broker transaction is still open, so it stays open
// until the app aborts and EndTxn(abort) is answered.
TEST(ProducerTxnData, CommitFailsWhenProduceBatchTimesOut) {
    Rig r;
    r.initOk();
    r.begin();
    ASSERT_EQ(r.send(0, 1), SendResult::Ok);
    r.tick();
    r.answerAdd(0);
    r.tick();
    ASSERT_EQ(r.wire.size(), 1u);
    Op commit = r.commit();
    r.now += r.cfg.deliveryTimeoutMs * kMs;   // the batch expired
    r.sweep();
    r.tick();
    EXPECT_EQ(r.endTxns(), 0u) << "commit marker over lost data";
    ASSERT_TRUE(commit.ready());
    EXPECT_EQ(commit.status(), OpStatus::DeliveryFailed);
    EXPECT_EQ(r.state(), TxnState::AbortableError);
    EXPECT_TRUE(r.impl->appTxnActive()) << "the broker transaction is still open";
    Op abort = r.abort();
    r.tick();
    ASSERT_EQ(r.endTxns(), 1u);
    EXPECT_EQ(r.lastEnd()->commit, uint8_t{0});
    EXPECT_TRUE(r.impl->appTxnActive()) << "reset only on EndTxn's final answer";
    r.endAnswer();
    // Fix round 2 (B-3, KIP-360): the lost batch left a sequence gap - the abort completes only after the epoch bump.
    EXPECT_FALSE(abort.ready());
    r.tick();
    ASSERT_NE(r.last(rpc::InitProducerId::TEMPLATE_ID), nullptr);
    r.answer(Rig::initAnswer(r.last(rpc::InitProducerId::TEMPLATE_ID)->corrId, 42, 1));
    ASSERT_TRUE(abort.ready());
    EXPECT_EQ(abort.status(), OpStatus::Ok);
    EXPECT_EQ(r.state(), TxnState::Ready);
    EXPECT_FALSE(r.impl->appTxnActive());
}

// ---- AddPartitions: errors, retries, re-discovery ------------------------------------------------------------------

// Fix round 1 (C-1): after a poisoned commit (AddPartitions refused for one partition while another's record
// landed), NO later commit can carry the orphaned record: the first EndTxn ever sent is an abort.
TEST(ProducerTxnDataRegression, PoisonedCommitNeverCommitsOrphanedDataRegression) {
    Rig r;
    r.initOk();
    r.begin();
    constexpr int32_t kGood = 0;
    constexpr int32_t kBad  = 1;
    ASSERT_EQ(r.send(kGood, 1), SendResult::Ok);
    r.ticks(3);
    ASSERT_EQ(r.send(kBad, 2), SendResult::Ok);
    r.ticks(3);
    Op commit = r.commit();
    ASSERT_EQ(r.adds().size(), 2u);
    answerAddFor(r, kGood, 0);
    r.tick();   // the good partition's record lands
    ASSERT_EQ(r.wire.size(), 1u);
    answerWire(r, 0, 0, 0);
    answerAddFor(r, kBad, ec(rpc::ErrorCode::UnknownTopic));
    ASSERT_TRUE(commit.ready());
    EXPECT_EQ(commit.status(), OpStatus::BrokerError);

    // The app retries the whole thing as a "new" transaction - refused until aborted.
    r.begin();
    (void)r.send(kGood, 3);
    Op commitAgain = r.commit();
    ASSERT_TRUE(commitAgain.ready());
    EXPECT_EQ(commitAgain.status(), OpStatus::BrokerError);
    r.ticks(8);
    EXPECT_EQ(r.endTxns(), 0u);
    Op abort = r.abort();
    r.ticks(4);
    ASSERT_EQ(r.endTxns(), 1u);
    EXPECT_EQ(r.lastEnd()->commit, uint8_t{0}) << "the orphaned record must be ABORTED, never committed";
    r.endAnswer();
    r.tick();
    ASSERT_TRUE(abort.ready());
    EXPECT_EQ(abort.status(), OpStatus::Ok);
    EXPECT_EQ(r.state(), TxnState::Ready);
}

// A non-retriable AddPartitions error: the partition's records are NEVER produced, later records for it are
// refused, and a commit fails (BrokerError with that code) instead of writing a marker over them; other partitions
// are unaffected.  The failed transaction must be aborted (EndTxn(abort), once the in-flight record settled) before
// the next begins.  (Changed: the two partitions are added by two requests - sent in separate passes - because the
// producer batches every newly dirty partition of a pass into one request.)
TEST(ProducerTxnDataRegression, AddPartitionsErrorGatesPartitionDataRegression) {
    Rig r;
    r.initOk();
    r.begin();
    constexpr int32_t kGood = 0;
    constexpr int32_t kBad  = 1;
    ASSERT_EQ(r.send(kBad, 1), SendResult::Ok);
    r.ticks(3);
    ASSERT_EQ(r.send(kGood, 2), SendResult::Ok);
    r.ticks(3);
    ASSERT_EQ(r.adds().size(), 2u);
    EXPECT_TRUE(r.wire.empty()) << "data before AddPartitions OK";
    answerAddFor(r, kGood, 0);
    r.tick();
    ASSERT_EQ(r.wire.size(), 1u) << "only the accepted partition's record";
    EXPECT_EQ(r.wire[0].partition, kGood);
    answerAddFor(r, kBad, ec(rpc::ErrorCode::UnknownTopic));
    EXPECT_EQ(r.state(), TxnState::AbortableError);
    (void)r.send(kBad, 3);   // refused
    Op commit = r.commit();
    ASSERT_TRUE(commit.ready());
    EXPECT_EQ(commit.status(), OpStatus::BrokerError);
    EXPECT_EQ(commit.code(), uec(rpc::ErrorCode::UnknownTopic));
    r.ticks(4);
    EXPECT_EQ(r.wire.size(), 1u);
    EXPECT_EQ(r.endTxns(), 0u) << "a commit marker over a refused partition";
    EXPECT_EQ(r.framesFor(kBad), 0u);

    // Abort it: EndTxn(abort) once the in-flight record settled.
    Op abort = r.abort();
    r.tick();
    EXPECT_EQ(r.endTxns(), 0u) << "the in-flight batch has not settled";
    answerWire(r, 0, 0, 0);
    r.tick();
    ASSERT_EQ(r.endTxns(), 1u);
    EXPECT_EQ(r.lastEnd()->commit, uint8_t{0});
    r.endAnswer();
    r.tick();
    ASSERT_TRUE(abort.ready());
    EXPECT_EQ(abort.status(), OpStatus::Ok);

    // The next transaction starts clean: the partition is added again.
    r.begin();
    ASSERT_EQ(r.send(kBad, 4), SendResult::Ok);
    r.ticks(3);
    EXPECT_EQ(r.adds().size(), 3u);
    answerAddFor(r, kBad, 0);
    r.ticks(2);
    EXPECT_EQ(r.framesFor(kBad), 1u);
}

TEST(ProducerTxnDataRegression, AddPartitionsRetriesConcurrentTransactionsRegression) {
    Rig r;
    r.initOk();
    r.begin();
    ASSERT_EQ(r.send(0, 1), SendResult::Ok);
    r.ticks(3);
    ASSERT_EQ(r.adds().size(), 1u);
    const int64_t corr = r.adds()[0]->corrId;
    const auto busy = FakeCoordinator::answer(rp::ReqKind::AddPartitions, corr, ec(rpc::ErrorCode::ConcurrentTransactions));
    r.answer(busy);
    r.ticks(4);   // within the first backoff step
    EXPECT_TRUE(r.wire.empty()) << "no data while AddPartitions is retried";
    EXPECT_EQ(r.adds().size(), 1u) << "re-sent only after the backoff";
    r.tick(2 * kMs);
    ASSERT_EQ(r.adds().size(), 2u) << "27 re-sent the AddPartitions";
    EXPECT_EQ(r.adds()[1]->corrId, corr) << "the retained frame";
    r.answer(busy);
    r.tick(5 * kMs);
    ASSERT_EQ(r.adds().size(), 3u);
    r.answer(FakeCoordinator::answer(rp::ReqKind::AddPartitions, corr, 0));
    r.tick();
    EXPECT_EQ(r.wire.size(), 1u) << "the record after its AddPartitions OK";
}

// An InvalidRequest answer (the broker could not decode it) poisons the transaction at once - never a silent retry.
TEST(ProducerTxnDataRegression, AddPartitionsInvalidRequestPoisonsNotRetriedRegression) {
    Rig r;
    r.initOk();
    r.begin();
    ASSERT_EQ(r.send(0, 1), SendResult::Ok);
    r.ticks(3);
    ASSERT_EQ(r.adds().size(), 1u);
    r.answerAdd(ec(rpc::ErrorCode::InvalidRequest));
    r.ticks(64, 10 * kMs);
    EXPECT_EQ(r.adds().size(), 1u) << "re-sent a request the broker cannot decode";
    Op commit = r.commit();
    r.tick();
    ASSERT_TRUE(commit.ready()) << "a commit over a poisoned transaction stayed pending";
    EXPECT_NE(commit.status(), OpStatus::Ok);
}

// F14: NotCoordinator never just re-sends into the same (loading / moved) coordinator: it asks to re-discover
// (once per generation), waits for the move, then backs off and re-sends.  (The AddPartitions leg; the init and
// EndTxn legs are in ProducerTxn, the offsets legs in ProducerTxnOffsets.)
TEST(ProducerTxnDataRegression, NotCoordinatorRediscoversThenRetriesRegression) {
    Rig r;
    r.initOk();
    r.begin();
    ASSERT_EQ(r.send(0, 1), SendResult::Ok);
    r.ticks(3);
    ASSERT_EQ(r.adds().size(), 1u);
    const int64_t corr = r.adds()[0]->corrId;
    r.answer(FakeCoordinator::answer(rp::ReqKind::AddPartitions, corr, ec(rpc::ErrorCode::NotCoordinator)));
    EXPECT_EQ(r.fake.rediscovers, 1) << "NotCoordinator must ask for a re-discovery";
    r.ticks(64, 10 * kMs);
    EXPECT_EQ(r.adds().size(), 1u) << "re-sent before the coordinator moved";
    EXPECT_TRUE(r.wire.empty());
    r.hub.onCoordinatorMoved();
    r.tick();
    ASSERT_EQ(r.adds().size(), 2u) << "not re-sent after the re-discovery";
    r.answer(FakeCoordinator::answer(rp::ReqKind::AddPartitions, corr, 0));
    r.tick();
    EXPECT_EQ(r.wire.size(), 1u) << "the record after its AddPartitions OK";
}
