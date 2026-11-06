// Checked writes Part 09 Task 5a (#788): on three real brokers, a leader whose catalog has not applied a DDL change never undoes it.
//
// The catalog replicates to every node, so a cluster cannot hold one node's catalog back by itself.  The test makes the old leader
// log a change computed at a catalog position far above anything the others reach (raiseCatalogThroughForTest), crashes it, and the
// new leader - still on the old plans - must keep the logged set until its own plans and position catch up.
#include "support/CheckedWritesRig.h"
#include "client/ClusterClient.h"

#include <rune/metadata/ShardRouter.h>

#include <gtest/gtest.h>

#include <chrono>
#include <string>
#include <thread>

using namespace rune::node;
using namespace rune::node::cw;
using namespace std::chrono_literals;
using rune::node::test::catalog_cluster::ThreeNodes;
namespace tcc = rune::node::test::catalog_cluster;

namespace {
constexpr uint64_t kObj = 77;
constexpr int64_t kAhead = 1'000'000'000;   // no real catalog event gets here
constexpr int32_t kOld = 0x40000801, kNew = 0x40000802;

TestRejectRule tagRule(uint8_t tag) { TestRejectRule r; r.mode = TestRejectRule::Mode::TagEquals; r.tag = tag; return r; }

void installReject(Worker& wk, int32_t src, const char* name, int32_t viewTopic, uint64_t object, TestRejectRule rule) {
    const auto desc = verdictSourceDescriptor();
    rune::view::ViewPlan v;
    v.viewName = name; v.key = "id"; v.sources = {{src, "id"}};
    v.output = {{"id", rune::codec::PrimType::UInt64, 0, "id", rune::view::MergeRule::Replace}};
    v.persistence = 1;
    const rune::codec::Descriptor* srcs[] = {&desc};
    auto comp = rune::view::compileView(v, srcs, viewTopic);
    ASSERT_TRUE(comp.has_value());
    wk.viewSet()->installTestVerdictViewForTest(comp->plan, comp->rowLayout, desc, false, object, rule);
}

struct Seen {
    std::size_t changes = 0;
    uint8_t     members = 0;
    int64_t     catalogThrough = -1;
    uint64_t    firstObject = 0, firstFingerprint = 0;
    bool        activated = false, waitingForInstall = false;
};

class Cluster {
public:
    explicit Cluster(const std::string& tag)
        : c_(ThreeNodes::startWith(tag, [](NodeConfig& cfg) { cfg.verdictIntervalMs = 20; })) {}

    void setUp() {
        ASSERT_TRUE(c_.awaitCatalogLeader(30s));
        const int32_t ctl = tcc::awaitController(c_);
        ASSERT_GT(ctl, 0);
        auto admin = tcc::connectClient(c_, ctl);
        rune::client::TopicOptions to;
        to.replicationFactor = 3;
        const auto src = admin->defineTopicAndWait("gate", 1, "id", rune::codec::serialize(verdictSourceDescriptor()), rune::client::Persistence::Disk, to, {}, 60s);
        ASSERT_EQ(src.errorCode, 0) << src.detail;
        topic_ = src.topicId;
        admin->close();
        ASSERT_TRUE(tcc::awaitLeaders(c_, topic_, 1, leaders_, 30s));
        for (const int id : c_.runningNodes()) prepare(id);
    }

    /// The node holds the cluster feature level and serves the partition; the old plan (T1, tag 9) is installed on its worker.
    void prepare(int id) {
        bool ready = false;
        for (const auto deadline = std::chrono::steady_clock::now() + 60s; std::chrono::steady_clock::now() < deadline && !ready; std::this_thread::sleep_for(50ms))
            ready = c_.node(id).onWorkerThreadForTest(worker(id), [this](Worker& wk) {
                return wk.clusterFeatureLevelForTest() >= rune::replication::MetadataFeature::kCheckedWrites && wk.writeStateForTest(topic_, 0) != nullptr;
            });
        ASSERT_TRUE(ready) << "node " << id << " never served the partition";
        const int32_t src = topic_;
        c_.node(id).onWorkerThreadForTest(worker(id), [src](Worker& wk) { installReject(wk, src, "gate_old", kOld, kObj, tagRule(9)); return 0; });
    }

    int32_t leader() {
        EXPECT_TRUE(tcc::awaitLeaders(c_, topic_, 1, leaders_, 60s));
        return leaders_[0];
    }

    Seen seen(int id) {
        return c_.node(id).onWorkerThreadForTest(worker(id), [this](Worker& wk) {
            Seen s;
            if (const PartitionWriteState* w = wk.writeStateForTest(topic_, 0)) {
                const ConstraintSet& set = w->constraintSets.latest();
                s.changes = w->constraintSets.size();
                s.members = set.count;
                s.catalogThrough = set.catalogThrough;
                if (set.count != 0) { s.firstObject = set.members[0].viewObjectId; s.firstFingerprint = set.members[0].planFingerprint; }
            }
            if (const VerdictSource* j = wk.viewSet()->verdictSource(topic_, 0)) { s.activated = j->activated(); s.waitingForInstall = j->waitingForInstall(); }
            return s;
        });
    }

    template <class Pred>
    bool await(int id, Pred&& pred, std::chrono::milliseconds within = 30s) {
        for (const auto deadline = std::chrono::steady_clock::now() + within; std::chrono::steady_clock::now() < deadline; std::this_thread::sleep_for(20ms))
            if (pred(seen(id))) return true;
        return false;
    }

    /// The catalog applied a DDL change on this node's worker, which now holds the plans after it.
    void applyDdl(int id, int shape, int64_t catalogPosition) {
        const int32_t src = topic_;
        c_.node(id).onWorkerThreadForTest(worker(id), [=](Worker& wk) {
            wk.viewSet()->uninstallView(kOld);
            if (shape == kReplace) installReject(wk, src, "gate_new", kNew, kObj, tagRule(8));
            if (shape == kCreate) installReject(wk, src, "gate_new", kNew, kObj + 1, tagRule(8));
            wk.viewSet()->raiseCatalogThroughForTest(catalogPosition);
            return 0;
        });
    }

    /// Every running replica's HW has reached the leader's log end: a follower's worker only reads the change records at its leadership scan.
    bool replicated(int leaderId) {
        for (const auto deadline = std::chrono::steady_clock::now() + 30s; std::chrono::steady_clock::now() < deadline; std::this_thread::sleep_for(20ms)) {
            const int64_t leo = c_.node(leaderId).onOwnerThreadForTest(topic_, 0, [](rune::replication::PartitionReplica& r) { return r.logEndOffset(); });
            bool all = true;
            for (const int id : c_.runningNodes())
                all = all && c_.node(id).onOwnerThreadForTest(topic_, 0, [leo](rune::replication::PartitionReplica& r) { return r.highWatermark() >= leo; });
            if (all) return true;
        }
        return false;
    }

    int32_t worker(int id) { return rune::metadata::ShardRouter(c_.node(id).workerCountForTest()).workerForPartition(topic_, 0); }

    enum Shape { kReplace, kDrop, kCreate };

    /// The leader logged a DDL change `shape` at a position the rest of the cluster has not reached; it crashes, and a node still on
    /// the old plan (after `restartFollowers` stopped and started both, their old plans reinstalled and no mark seen) leads.
    void run(Shape shape, bool restartFollowers) {
        setUp();
        ASSERT_FALSE(::testing::Test::HasFatalFailure());
        const int old = leader();
        ASSERT_TRUE(await(old, [](const Seen& s) { return s.changes == 1 && s.members == 1; })) << "the first set was never logged";
        ASSERT_TRUE(replicated(old)) << "the first set never reached every replica";
        applyDdl(old, shape, kAhead);
        ASSERT_TRUE(await(old, [](const Seen& s) { return s.changes == 2 && s.catalogThrough == kAhead; })) << "the old leader never logged the DDL change";
        const Seen logged = seen(old);
        ASSERT_TRUE(replicated(old)) << "the change never reached every replica";
        if (restartFollowers) {
            for (int id = 1; id <= 3; ++id) {
                if (id == old) continue;
                c_.stop(id);
                c_.startNode(id);
                prepare(id);   // the boot replay of the catalog installs the old plan; the DDL change is not in it
            }
        }
        c_.crash(old);
        std::vector<int32_t> now;
        for (const auto deadline = std::chrono::steady_clock::now() + 60s; std::chrono::steady_clock::now() < deadline; std::this_thread::sleep_for(20ms)) {
            now.clear();
            if (tcc::awaitLeaders(c_, topic_, 1, now, 1s) && now[0] != old) break;
        }
        ASSERT_FALSE(now.empty());
        const int lead = now[0];
        ASSERT_NE(lead, old);
        ASSERT_TRUE(await(lead, [](const Seen& s) { return s.activated; })) << "the new leader never activated its judge";
        std::this_thread::sleep_for(1s);   // a stale set is logged within a few judge passes
        const Seen held = seen(lead);
        EXPECT_EQ(held.changes, 2u) << "the new leader logged a change over the newer DDL change (#788)";
        EXPECT_EQ(held.members, logged.members);
        EXPECT_EQ(held.firstFingerprint, logged.firstFingerprint);
        applyDdl(lead, shape, kAhead);   // its catalog and plans catch up
        std::this_thread::sleep_for(1s);
        const Seen after = seen(lead);
        EXPECT_EQ(after.changes, 2u) << "the caught-up leader changed the set again";
        EXPECT_FALSE(after.waitingForInstall) << "the judge still waits for a plan the leader installed";
    }

private:
    ThreeNodes           c_;
    int32_t              topic_ = -1;
    std::vector<int32_t> leaders_;
};
}  // namespace

TEST(CheckedWritesCatalogGateClusterRegression, ReplaceDoneByTheOldLeaderIsNotRevertedByTheNewOneRegression) {
    Cluster c("cw09_gate_replace");
    c.run(Cluster::kReplace, false);
}

TEST(CheckedWritesCatalogGateClusterRegression, DropDoneByTheOldLeaderIsNotRevertedByTheNewOneRegression) {
    Cluster c("cw09_gate_drop");
    c.run(Cluster::kDrop, false);
}

TEST(CheckedWritesCatalogGateClusterRegression, CreateDoneByTheOldLeaderIsNotRevertedByTheNewOneRegression) {
    Cluster c("cw09_gate_create");
    c.run(Cluster::kCreate, false);
}

TEST(CheckedWritesCatalogGateClusterRegression, RestartedFollowersBeforeTheFirstMarkDoNotRevertTheChangeRegression) {
    Cluster c("cw09_gate_restart");
    c.run(Cluster::kReplace, true);
}
