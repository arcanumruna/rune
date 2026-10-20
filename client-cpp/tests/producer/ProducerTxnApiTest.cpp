// tests/producer/ProducerTxnApiTest.cpp - Phase 2b F Task 1: the transactional API surface and its refusals.
//
// Bare ProducerImpl rigs (Ring ingress, hooks naming an Invoker Client that only registers the Completions
// queues) for the gate; Client::producer for the creation validation and the plain-producer lifecycle
// refusals.  Nothing sleeps, no threads, no atomics.
#include "../support/AllocCounter.h"

#include <rune/client/Client.h>
#include <rune/client/Completions.h>
#include <rune/client/Key.h>
#include <rune/client/TopicHandle.h>
#include <rune/client/producer/BatchBuilder.h>
#include <rune/client/producer/Producer.h>
#include <rune/client/producer/ProducerImpl.h>
#include <rune/client/shard/ShardConfig.h>

#include <gtest/gtest.h>

#include <cstdint>
#include <memory>
#include <span>
#include <stdexcept>
#include <thread>
#include <string>
#include <string_view>
#include <vector>
#include <unistd.h>

namespace rc = rune::client;
using rc::Acks;
using rc::Key;
using rc::Producer;
using rc::ProducerConfig;
using rc::SendResult;
using rc::TopicHandle;
using rc::producer::ProducerImpl;

namespace {
std::span<const uint8_t> bytes(std::string_view s) {
    return {reinterpret_cast<const uint8_t*>(s.data()), s.size()};
}
const TopicHandle kTopic{7, 4};

int g_ctr = 0;  // single test thread
rc::ClientConfig regCfg() {
    rc::ClientConfig c;
    c.gatewayChannel  = "aeron:ipc";
    c.gatewayStreamId = 4611;
    c.egressChannel   = "aeron:ipc";
    c.egressStreamId  = 4612;
    c.dataDir = "/tmp/txn_api_test_" + std::to_string(::getpid()) + "_" + std::to_string(g_ctr++);
    c.runnerMode = rc::RunnerMode::Invoker;
    return c;
}
rc::CompletionsConfig smallCq() {
    rc::CompletionsConfig c;
    c.capacityBytes  = 1u << 14;
    c.maxResultBytes = 64;
    c.maxErrorText   = 64;
    return c;
}
ProducerConfig cfgOf(Acks acks, std::string txnId = {}) {
    ProducerConfig c;
    c.acks          = acks;
    c.lingerUs      = 0;
    c.batchBytes    = 16 * 1024;
    c.maxInFlight   = 8;
    c.transactionalId = std::move(txnId);
    return c;
}

// A bare Ring-mode producer; its hooks name the Client so Completions queues are accepted.
struct Rig {
    std::unique_ptr<rc::Client> client = std::make_unique<rc::Client>(regCfg());
    ProducerImpl*               impl;
    Producer                    p;
    explicit Rig(ProducerConfig cfg, std::size_t ring = 1 << 20) : impl(new ProducerImpl(cfg, ring)), p(impl) {
        ProducerImpl::Hooks h;
        h.ctx = client.get();
        impl->setHooks(h);
    }
};
}  // namespace

TEST(ProducerTxnApi, NotInTransactionIsAppendedLast) {
    static_assert(static_cast<int>(SendResult::NotInTransaction) == static_cast<int>(SendResult::ClaimHeld) + 1,
                  "append-only: NotInTransaction follows Part B's ClaimHeld");
    static_assert(static_cast<int>(rc::producer::RejectReason::NotInTransaction) ==
                      static_cast<int>(rc::producer::RejectReason::ClaimHeld) + 1,
                  "append-only: RejectReason::NotInTransaction follows ClaimHeld");
    SUCCEED();
}

TEST(ProducerTxnApi, GateInitialStateFollowsTheConfig) {
    Rig plain(cfgOf(Acks::All));
    EXPECT_EQ(plain.impl->txnGate(), ProducerImpl::TxnGate::NotTransactional);
    EXPECT_FALSE(plain.p.transactional());
    Rig txn(cfgOf(Acks::All, "tx-1"));
    EXPECT_EQ(txn.impl->txnGate(), ProducerImpl::TxnGate::Closed);
    EXPECT_TRUE(txn.p.transactional());
}

TEST(ProducerTxnApi, AnOpenGatePassesEveryPathToTheNormalChecks) {
    Rig r(cfgOf(Acks::All, "tx-open"));
    r.impl->setTxnGate(ProducerImpl::TxnGate::Open);
    EXPECT_EQ(r.p.trySend(kTopic, Key(uint64_t{1}), bytes("v")), SendResult::Ok);
    auto c = r.p.tryClaim(kTopic, Key(uint64_t{1}), 8);
    EXPECT_EQ(c.result(), SendResult::Ok);
    c.abort();
    EXPECT_NO_THROW(r.p.send(kTopic, Key(uint64_t{1}), bytes("v")));
}

TEST(ProducerTxnApiRegression, AcksNoneWithATransactionalIdIsRejectedAtCreationRegression) {
    rc::Client client(regCfg());
    ProducerConfig cfg = cfgOf(Acks::None, "tx-1");
    EXPECT_THROW((void)client.producer(cfg), std::invalid_argument);
    cfg.acks = Acks::All;
    EXPECT_NO_THROW((void)client.producer(cfg));
    cfg.transactionalId = std::string(256, 'x');
    EXPECT_THROW((void)client.producer(cfg), std::invalid_argument);
    cfg.transactionalId = std::string(255, 'x');
    EXPECT_NO_THROW((void)client.producer(cfg));
}

TEST(ProducerTxnApiRegression, AShardRefusesATransactionalIdRegression) {
    rc::shard::ShardGroupConfig g;
    g.producer.acks            = Acks::All;
    g.producer.transactionalId = "tx-shard";
    EXPECT_THROW((void)rc::shard::shardProducerConfig(g), std::invalid_argument);
    EXPECT_THROW(rc::shard::validateShardGroupConfig(g, 1), std::invalid_argument);
    g.producer.transactionalId.clear();
    EXPECT_NO_THROW((void)rc::shard::shardProducerConfig(g));
}

TEST(ProducerTxnApiRegression, LifecycleOnAPlainProducerIsRefusedExplicitlyRegression) {
    rc::Client client(regCfg());
    Producer p = client.producer(cfgOf(Acks::All));
    rc::Completions cq(client, smallCq());
    EXPECT_EQ(p.initTransactions(cq, 1), rc::OpResult::InvalidArgument);
    const rc::OffsetEntry offs[] = {{7, 0, 5}};
    EXPECT_EQ(p.sendOffsetsToTransaction(offs, "g", cq, 4), rc::OpResult::InvalidArgument);
    EXPECT_EQ(p.commitTransaction(cq, 2), rc::OpResult::InvalidArgument);
    EXPECT_EQ(p.abortTransaction(cq, 3), rc::OpResult::InvalidArgument);
    EXPECT_EQ(p.flush(cq, 5), rc::OpResult::InvalidArgument);
    EXPECT_THROW(p.beginTransaction(), std::logic_error);
    EXPECT_THROW(p.initTransactionsAndWait(std::chrono::seconds(1)), std::logic_error);
    EXPECT_THROW(p.sendOffsetsToTransactionAndWait(offs, "g", std::chrono::seconds(1)), std::logic_error);
    EXPECT_THROW(p.commitTransactionAndWait(std::chrono::seconds(1)), std::logic_error);
    EXPECT_THROW(p.abortTransactionAndWait(std::chrono::seconds(1)), std::logic_error);
    EXPECT_THROW(p.flushAndWait(std::chrono::seconds(1)), std::logic_error);
    EXPECT_EQ(cq.poll([](const rc::Completion&, std::span<const uint8_t>) {}), 0) << "no completion for a refusal";
    EXPECT_EQ(cq.reservedBytes(), 0u);
    EXPECT_FALSE(p.transactional());
    EXPECT_FALSE(p.transactionActive());
    EXPECT_EQ(p.producerId(), -1);
}

TEST(ProducerTxnApiRegression, SendOutsideATransactionIsRefusedRegression) {
    Rig r(cfgOf(Acks::All, "tx-gate"));
    Producer& p = r.p;
    rc::Completions cq(*r.client, smallCq());
    EXPECT_EQ(p.trySend(kTopic, Key(uint64_t{1}), bytes("v")), SendResult::NotInTransaction);
    EXPECT_EQ(p.trySendTo(kTopic, 0, Key(uint64_t{1}), bytes("v")), SendResult::NotInTransaction);
    EXPECT_EQ(p.trySend(kTopic, Key(uint64_t{1}), bytes("v"), cq, 9), SendResult::NotInTransaction);
    EXPECT_EQ(cq.reservedBytes(), 0u) << "the refused send keeps no credit";
    EXPECT_EQ(p.trySend(kTopic, Key(uint64_t{1}), bytes("v"), rc::AckToken{9}), SendResult::NotInTransaction);
    EXPECT_EQ(p.tryClaim(kTopic, Key(uint64_t{1}), 8).result(), SendResult::NotInTransaction);
    EXPECT_EQ(p.tryClaim(kTopic, Key(uint64_t{1}), 8, cq, 9).result(), SendResult::NotInTransaction);
    EXPECT_EQ(p.tryClaim(kTopic, Key(uint64_t{1}), 8, rc::AckToken{9}).result(), SendResult::NotInTransaction);
    EXPECT_EQ(cq.reservedBytes(), 0u) << "the refused claim keeps no credit";
    EXPECT_EQ(p.claimBatch(kTopic, Key(uint64_t{1}), 2, 8).result(), SendResult::NotInTransaction);
    EXPECT_THROW(p.send(kTopic, Key(uint64_t{1}), bytes("v")), std::logic_error);
    EXPECT_THROW(p.sendTo(kTopic, 0, Key(uint64_t{1}), bytes("v")), std::logic_error);
    EXPECT_THROW(p.send(kTopic, Key(uint64_t{1}), bytes("v"), cq, 9), std::logic_error);
    EXPECT_THROW(p.send(kTopic, Key(uint64_t{1}), bytes("v"), rc::AckToken{9}), std::logic_error);
    EXPECT_EQ(cq.reservedBytes(), 0u);
    // Tier-2: OpResult has no such value, so it throws (like every other Tier-2 misuse), keeps no credit,
    // and the batch stays intact for a retry once a transaction is open.
    auto b = p.batch(kTopic, Key(uint64_t{1}));
    b.add(bytes("a"));
    EXPECT_THROW((void)b.send(), std::logic_error);
    EXPECT_THROW((void)b.send(cq, 3), std::logic_error);
    EXPECT_THROW((void)b.send(rc::AckToken{3}), std::logic_error);
    EXPECT_EQ(cq.reservedBytes(), 0u);
    // Nothing reached the ring.
    EXPECT_EQ(r.impl->drainRing(1'000'000'000, 256), 0);
    // Opening the gate lets the same producer and the same intact batch through.
    r.impl->setTxnGate(ProducerImpl::TxnGate::Open);
    EXPECT_EQ(b.send(), rc::OpResult::Accepted);
    EXPECT_EQ(p.trySend(kTopic, Key(uint64_t{1}), bytes("v")), SendResult::Ok);
}

TEST(ProducerTxnApiRegression, PlainProducerHotPathIsUnchangedRegression) {
    // The gate is NotTransactional: trySend takes the same path (one predictable branch) and allocates nothing.
    Rig r(cfgOf(Acks::None));
    for (int i = 0; i < 64; ++i) ASSERT_EQ(r.p.trySend(kTopic, Key(uint64_t(i)), bytes("warm")), SendResult::Ok);
    alloc_counter::startThisThread();
    for (int i = 0; i < 1000; ++i) ASSERT_EQ(r.p.trySend(kTopic, Key(uint64_t(i)), bytes("x")), SendResult::Ok);
    EXPECT_EQ(alloc_counter::stopThisThread(), 0);
}

// Task 1 review carry: the Direct-mode foreign-thread decision comes BEFORE the (owner-written) txn gate, so a
// foreign thread on a transactional Invoker producer sees WrongThread and never reads the gate.  The off-thread
// calls run in a joined std::thread; results are plain locals read after join().
TEST(ProducerTxnApiRegression, TransactionalInvokerProducerOffTheOwnerThreadIsWrongThreadRegression) {
    ProducerImpl impl(cfgOf(Acks::All, "tx-direct"), 1 << 20);
    impl.enableDirectIngress({std::this_thread::get_id(), nullptr, nullptr, 1});
    ASSERT_EQ(impl.txnGate(), ProducerImpl::TxnGate::Closed);
    SendResult submit = SendResult::Ok, claim = SendResult::Ok, batch = SendResult::Ok;
    rc::producer::RejectReason why = rc::producer::RejectReason::None, tier2 = rc::producer::RejectReason::None;
    std::vector<uint8_t> bb(8, 1);
    std::thread other([&] {
        submit = impl.trySubmit(7, 0, Key(uint64_t{1}), bytes("v"), 0, &why);
        claim  = impl.claimRecord(7, 0, Key(uint64_t{1}), 8, 0).result;
        batch  = impl.claimBatch(7, 0, Key(uint64_t{1}), 2, 8).result;
        tier2  = impl.submitTier2Bytes(7, 0, 0, 1, bb);
    });
    other.join();
    EXPECT_EQ(submit, SendResult::WrongThread);
    EXPECT_EQ(why, rc::producer::RejectReason::WrongThread);
    EXPECT_EQ(claim, SendResult::WrongThread);
    EXPECT_EQ(batch, SendResult::WrongThread);
    EXPECT_EQ(tier2, rc::producer::RejectReason::WrongThread);
    // On the owner thread the closed gate still refuses.
    EXPECT_EQ(impl.trySubmit(7, 0, Key(uint64_t{1}), bytes("v"), 0, nullptr), SendResult::NotInTransaction);
    EXPECT_EQ(impl.claimRecord(7, 0, Key(uint64_t{1}), 8, 0).result, SendResult::NotInTransaction);
    EXPECT_EQ(impl.claimBatch(7, 0, Key(uint64_t{1}), 2, 8).result, SendResult::NotInTransaction);
    EXPECT_EQ(impl.submitTier2Bytes(7, 0, 0, 1, bb), rc::producer::RejectReason::NotInTransaction);
}

// fix1 I8: an invalid topic is never silently dropped - the void send() throws, in a transaction or outside one.
TEST(ProducerTxnApiRegression, SendOnInvalidTopicThrowsRegression) {
    Rig r(cfgOf(Acks::All, "tx-invalid"));
    r.impl->setTxnGate(ProducerImpl::TxnGate::Open);
    EXPECT_THROW(r.p.send(TopicHandle{}, Key(uint64_t{1}), bytes("v")), std::invalid_argument);
    EXPECT_EQ(r.p.trySend(TopicHandle{}, Key(uint64_t{1}), bytes("v")), SendResult::InvalidTopic);
    r.impl->setTxnGate(ProducerImpl::TxnGate::Closed);
    EXPECT_THROW(r.p.send(TopicHandle{}, Key(uint64_t{1}), bytes("v")), std::exception)
        << "outside a transaction an invalid topic is still refused loudly";
}

// Backlog B6: a transactional record (key + value) over Producer::kMaxRecordBytes is rejected at append
// (std::length_error), never sent.
TEST(ProducerTxnApi, RecordOver16KiBIsRejectedAtAppend) {
    Rig r(cfgOf(Acks::All, "tx-big"));
    r.impl->setTxnGate(ProducerImpl::TxnGate::Open);
    const std::vector<uint8_t> over(Producer::kMaxRecordBytes - 7, 0x09);   // + 8-byte key
    EXPECT_THROW(r.p.send(kTopic, Key(uint64_t{0}), over), std::length_error);
    EXPECT_EQ(r.p.trySend(kTopic, Key(uint64_t{0}), over), SendResult::MessageTooLarge);
    EXPECT_EQ(r.impl->drainRing(1'000'000'000, 256), 0) << "an over-size record reached the ring";
}

// Phase 2b F Task 9: transactions are Producer + ProducerConfig::transactionalId; the separate handle is gone.
// (A requires-expression naming a missing type is a hard error outside a template, so the type is pinned through its
// header and the factory through a dependent concept.)
namespace {
template <class C>
concept HasTransactionalProducerFactory = requires(C& c) { c.transactionalProducer(""); };
template <class C>
concept HasProducerFactory = requires(C& c) { c.producer(); };
}  // namespace

#if __has_include(<rune/client/producer/TransactionalProducer.h>) || __has_include(<rune/client/producer/TransactionalProducerImpl.h>)
#error "the TransactionalProducer headers must stay deleted (Phase 2b F Task 9)"
#endif

TEST(ProducerTxnRegression, TransactionalProducerTypeIsGoneRegression) {
    static_assert(!HasTransactionalProducerFactory<rune::client::Client>,
                  "Client::transactionalProducer must stay deleted");
    static_assert(HasProducerFactory<rune::client::Client>, "the concept itself works");
    SUCCEED();
}
