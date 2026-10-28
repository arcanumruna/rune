// NamespaceCrossRefRegressionTest.cpp - final review U2/U3: group and transaction requests never reference another
// namespace's topics (a foreign topic is "does not exist", for the cluster admin too), and a name is not recreated
// before the old incarnation's grants are purged.
#include "support/NamespaceLifecycleRig.h"

#include <gtest/gtest.h>

#include <string>

using namespace rune::node;
using namespace rune::node::test;
using namespace rune::node::test::nslc;
namespace pc = rune::protocol::client;

namespace {
constexpr int kUnknownTopic = static_cast<int>(pc::ErrorCode::UnknownTopic);

struct Topics {
    int32_t foreign = -1;   // xb/orders
    int32_t local = -1;     // orders (default namespace)
};

// One transactional produce as the cluster admin, sent directly (AuthRig::produceAs re-sends UnknownTopic as "not yet
// known", which would hide the answer under test).  The partition verifies the registration with the coordinator
// asynchronously and answers ConcurrentTransactions (27, retriable) while it is pending: re-sent, bounded, until final.
int txnProduce(LcRig& rig, int32_t topicId, int64_t pid, int16_t epoch) {
    AuthRig& a = rig.auth();
    rune::protocol::RecordBatchBuilder bb(0, 1000);
    bb.setProducerId(pid); bb.setProducerEpoch(epoch); bb.setBaseSequence(0); bb.setTransactional(true);
    bb.append("k", "v", 1000);
    const std::vector<uint8_t> batch = bb.build();
    int code = kNoAnswer;
    rig.until([&] {
        const int64_t corr = a.c().nextCorrelationId();
        if (!a.c().produceIdempotent(a.ch(), AuthRig::stream(), rig.rootSid(), corr, topicId, 0, 1, pid, epoch, 0, batch)) return true;
        const auto ans = a.c().awaitProduceResponseUntil(corr, AuthRig::dl());
        code = ans ? ans->errorCode : kNoAnswer;
        return code != static_cast<int>(pc::ErrorCode::ConcurrentTransactions);
    }, 1000);
    return code;
}

Topics seed(LcRig& rig) {
    rig.createNs("xa");
    rig.createNs("xb");
    Topics t;
    t.local = rig.auth().orders;   // the rig predefines the default-namespace topic "orders"
    t.foreign = rig.auth().defineTopic(rig.rootSid(), "xb/orders");
    EXPECT_GE(t.foreign, 0);
    EXPECT_GE(t.local, 0);
    return t;
}
}  // namespace

TEST(NamespaceCrossRefRegression, CrossNamespaceAddPartitionsIsUnknownTopicRegression) {
    LcRig rig("xrefap");
    const Topics t = seed(rig);
    AuthRig& a = rig.auth();
    const int64_t sid = rig.rootSid();
    const auto ids = a.initProducer(sid, "t");
    ASSERT_EQ(ids.code, 0);
    EXPECT_EQ(a.addPartitions(sid, "t", t.foreign, ids.pid, ids.epoch), kUnknownTopic);
    const int64_t before = a.logEnd(t.foreign);
    const int p = txnProduce(rig, t.foreign, ids.pid, ids.epoch);
    EXPECT_NE(p, 0) << "a transactional produce to a topic that was never registered";
    EXPECT_EQ(a.logEnd(t.foreign), before) << "the foreign topic grew";
}

TEST(NamespaceCrossRefRegression, CrossNamespaceOffsetCommitIsUnknownTopicRegression) {
    LcRig rig("xrefoc");
    const Topics t = seed(rig);
    AuthRig& a = rig.auth();
    const int64_t sid = rig.rootSid();
    const auto [member, gen] = a.joinStable(sid, "g", {t.local});
    ASSERT_GE(gen, 0);
    EXPECT_EQ(a.commitAs(sid, "g", member, gen, t.foreign, 5), kUnknownTopic);
    EXPECT_EQ(a.committedOffset(sid, "g", t.local), -1) << "an offset leaked under the same local name";
    EXPECT_EQ(a.committedOffset(sid, "g", t.foreign), -1);
}

TEST(NamespaceCrossRefRegression, CrossNamespaceVerifyByNameRegression) {
    LcRig rig("xrefvn");
    const Topics t = seed(rig);
    AuthRig& a = rig.auth();
    const int64_t sid = rig.rootSid();
    const auto ids = a.initProducer(sid, "t2");
    ASSERT_EQ(ids.code, 0);
    ASSERT_EQ(a.addPartitions(sid, "t2", t.local, ids.pid, ids.epoch), 0);
    const auto p = txnProduce(rig, t.foreign, ids.pid, ids.epoch);
    EXPECT_EQ(p, kUnknownTopic) << "verify must not match a foreign topic by its local name";
}

// Review U3: grants naming `ra/...` are stripped only when NamespacePurged applies on `__security` (asynchronously).
// A name recreated before that must not match the old incarnation's retained grants.
TEST(NamespaceCrossRefRegression, RecreateBeforeSecurityPurgeIsRefusedRegression) {
    using namespace grants;
    constexpr int kTopicExists = static_cast<int>(pc::ErrorCode::TopicExists);
    LcRig rig("xrefrc");
    AuthRig& a = rig.auth();
    rig.createNs("ra");
    const uint32_t oldId = rig.observe("ra").nsId;
    ASSERT_NE(oldId, 0u);
    // A default-namespace user whose grants name namespace `ra` (a prefix grant on `ra/` topics).
    rig.broker().createUser("bob", AuthRig::kPw,
                            {prefix(SO::Read, ST::Topic, "ra/"), prefix(SO::Write, ST::Topic, "ra/"),
                             prefix(SO::Describe, ST::Topic, "ra/")});
    // The purge is held (holding the security APPLY instead makes every authenticated request answer AuthUnavailable):
    // the namespace is DELETING and its grants are still in __security.
    rig.broker().node().holdSecurityPurgeForTest(true);
    ASSERT_EQ(rig.ns(rig.rootSid(), pc::NamespaceDdlKind::Delete, "ra").code, 0);
    EXPECT_EQ(rig.ns(rig.rootSid(), pc::NamespaceDdlKind::Create, "ra").code, kTopicExists)
        << "recreated while the old incarnation's grants are still in __security";

    rig.broker().node().holdSecurityPurgeForTest(false);
    rig.releaseReaper();
    ASSERT_TRUE(rig.until([&] { return rig.erased(oldId); })) << "the old incarnation is erased once the purge applied";
    ASSERT_EQ(rig.ns(rig.rootSid(), pc::NamespaceDdlKind::Create, "ra").code, 0);
    const int32_t tid = a.defineTopic(rig.rootSid(), "ra/t");
    ASSERT_GE(tid, 0);
    const int64_t bob = a.login("bob", AuthRig::kPw, pc::kClientVersionNamespaces);
    EXPECT_EQ(a.produceDenied(bob, tid), kNotAuthorized) << "bob's old grant on ra/ must not reach the new namespace";
}
