// NsTxnLimitRegressionTest.cpp - namespaces Phase 2 Task 10 (D24): the open-transaction and transaction-time limits of
// a namespace, in the TransactionCoordinator (InitProducerId / AddPartitions / AddOffsets, and the timeout sweep).
//
// Interaction matrix (limit x {below, at, above} x timing):
//   | limit             | operation                    | timing                      | expected                                      | test
//   |-------------------|------------------------------|-----------------------------|-----------------------------------------------|-----
//   | max-open-txns 2   | AddPartitions opens a txn    | sequential                  | 1st, 2nd ok; 3rd kNamespaceLimit, nothing set | LimitEnforcementTableRegression.OpenTxnsRegression
//   | max-open-txns     | another ns / default         | while a ns is full          | unaffected                                    | ...OpenTxnsRegression
//   | max-open-txns     | a txn ends / limit lowered   | after                       | room back / open ones stay, new refused       | ...OpenTxnsRegression
//   | max-txn-time 10 s | InitProducerId(timeout)      | sequential                  | below, at ok; above refused; 0 = clamped      | LimitEnforcementTableRegression.TxnTimeRegression
//   | max-txn-time      | the timeout sweep            | injected clock, lowered     | min(txn timeout, ns max) aborts the open txn  | ...TxnTimeRegression
#include <rune/coordination/NsScopedId.h>
#include <rune/transaction/TransactionCoordinator.h>

#include <gtest/gtest.h>

#include <string>
#include <vector>

using namespace rune::transaction;
using rune::coordination::scopedId;

namespace {
constexpr int64_t kSec = 1'000'000'000LL;

struct Opened { ProducerId pid; ProducerEpoch epoch; };
Opened init(TransactionCoordinator& c, const std::string& id, int32_t timeoutMs = 0) {
    const auto r = c.initProducerId(id, timeoutMs, 0);
    EXPECT_TRUE(r.ok()) << id << " code " << r.errorCode;
    return {r.producerId, r.producerEpoch};
}
int addPart(TransactionCoordinator& c, const std::string& id, const Opened& o, int64_t now = 0) {
    return c.addPartitions(id, o.pid, o.epoch, std::vector<TopicPartition>{{"t", 0}}, now).errorCode;
}
}  // namespace

TEST(LimitEnforcementTableRegression, OpenTxnsRegression) {
    TransactionCoordinator c;
    c.setNamespaceTxnLimits(1, /*maxOpenTxns=*/2, 0);
    const std::string a = scopedId(1, "a"), b = scopedId(1, "b"), d = scopedId(1, "c");
    const Opened oa = init(c, a), ob = init(c, b), oc = init(c, d);   // InitProducerId never opens a txn
    EXPECT_EQ(addPart(c, a, oa), AddPartitionsResult::kOk) << "below";
    EXPECT_EQ(addPart(c, b, ob), AddPartitionsResult::kOk) << "at: usage == limit";
    EXPECT_EQ(addPart(c, d, oc), AddPartitionsResult::kNamespaceLimit) << "above";
    EXPECT_EQ(c.metadata(d)->state, TxnState::Empty) << "a refused AddPartitions started nothing";
    EXPECT_TRUE(c.metadata(d)->partitions.empty());
    // Adding partitions to a txn that is already open is not a new transaction.
    EXPECT_EQ(addPart(c, a, oa), AddPartitionsResult::kOk);
    // Another namespace and `default` are unaffected by a full namespace.
    const std::string other = scopedId(2, "x"), def = "x";
    const Opened oo = init(c, other), od = init(c, def);
    EXPECT_EQ(addPart(c, other, oo), AddPartitionsResult::kOk);
    EXPECT_EQ(addPart(c, def, od), AddPartitionsResult::kOk);
    // A transaction that leaves Ongoing gives its room back.
    ASSERT_TRUE(c.endTransaction(a, oa.pid, oa.epoch, /*commit=*/false, 0).errorCode == 0);
    EXPECT_EQ(addPart(c, d, oc), AddPartitionsResult::kOk) << "room is back once a left Ongoing";
    // Lowered below usage: the open ones stay, a new one is refused; removing the limit lifts it.
    c.setNamespaceTxnLimits(1, 1, 0);
    EXPECT_EQ(c.metadata(b)->state, TxnState::Ongoing);
    EXPECT_EQ(c.metadata(d)->state, TxnState::Ongoing);
    const std::string e = scopedId(1, "e");
    const Opened oe = init(c, e);
    EXPECT_EQ(addPart(c, e, oe), AddPartitionsResult::kNamespaceLimit);
    c.setNamespaceTxnLimits(1, 0, 0);
    EXPECT_EQ(addPart(c, e, oe), AddPartitionsResult::kOk);
}

TEST(LimitEnforcementTableRegression, TxnTimeRegression) {
    TransactionCoordinator c(ProducerIdAllocator{}, /*defaultTxnTimeoutMs=*/60'000);
    c.setNamespaceTxnLimits(1, 0, /*maxTxnTimeMs=*/10'000);
    // InitProducerId: below, at ok; above refused (nothing created); unspecified = the default clamped to the maximum.
    EXPECT_TRUE(c.initProducerId(scopedId(1, "lo"), 5'000, 0).ok());
    EXPECT_TRUE(c.initProducerId(scopedId(1, "at"), 10'000, 0).ok());
    const auto over = c.initProducerId(scopedId(1, "hi"), 10'001, 0);
    EXPECT_EQ(over.errorCode, InitProducerIdResult::kNamespaceLimit);
    EXPECT_EQ(c.metadata(scopedId(1, "hi")), nullptr) << "a refused InitProducerId created no transactional id";
    EXPECT_TRUE(c.initProducerId(scopedId(1, "dflt"), 0, 0).ok());
    EXPECT_EQ(c.metadata(scopedId(1, "dflt"))->txnTimeoutMs, 10'000) << "the 60 s default is clamped";
    // Another namespace and default keep the global timeout.
    EXPECT_TRUE(c.initProducerId(scopedId(2, "o"), 30'000, 0).ok());
    EXPECT_TRUE(c.initProducerId("plain", 0, 0).ok());
    EXPECT_EQ(c.metadata("plain")->txnTimeoutMs, 60'000);

    // The sweep: a txn opened with a 60 s timeout, then the namespace maximum LOWERED to 10 s: min() applies to it.
    c.setNamespaceTxnLimits(1, 0, 0);
    const std::string t = scopedId(1, "sweep");
    const Opened ot = init(c, t, 60'000);
    ASSERT_EQ(addPart(c, t, ot, /*now=*/0), AddPartitionsResult::kOk);
    const std::string u = scopedId(2, "sweep");
    const Opened ou = init(c, u, 60'000);
    ASSERT_EQ(addPart(c, u, ou, 0), AddPartitionsResult::kOk);
    EXPECT_EQ(c.doWork(11 * kSec), 0) << "no limit: 11 s < 60 s";
    c.setNamespaceTxnLimits(1, 0, 10'000);
    EXPECT_EQ(c.doWork(10 * kSec), 0) << "at the maximum: not yet over";
    EXPECT_EQ(c.doWork(10 * kSec + 1), 1) << "over min(60 s, 10 s): aborted";
    EXPECT_EQ(c.metadata(t)->state, TxnState::PrepareAbort);
    EXPECT_EQ(c.metadata(u)->state, TxnState::Ongoing) << "another namespace keeps its own timeout";
    EXPECT_EQ(c.doWork(59 * kSec), 0);
    EXPECT_EQ(c.doWork(61 * kSec), 1) << "the global timeout still applies";
}
