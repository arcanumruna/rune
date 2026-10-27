// NamespaceDiskTest.cpp - namespaces plan Task 6 (D14): the on-disk layout <dataDir>/ns-<nsId>/topics/<name>/.
//
// Pinned: NamespacePaths builds every path; parseNsDir accepts exactly ns-<u32>; a real broker keeps the SAME topic
// name in two namespaces in two dirs (and reopens both after a restart); the boot reconcile works per namespace root
// (a drop in ns 1 never touches ns 0's same-named dir); a dataDir entry that is not ns-<u32> is logged and left alone.
#include <rune/aeronutil/testing/FreePort.h>
#include <rune/catalog/CatalogEvent.h>
#include <rune/catalog/testing/GoldenScript.h>
#include <rune/gateway/ClientGateway.h>
#include <rune/gateway/TopicDirMarker.h>
#include <rune/gateway/TopicRegistry.h>
#include <rune/node/BrokerNode.h>
#include <rune/node/NamespacePaths.h>
#include <rune/node/NodeConfig.h>
#include <rune/node/TopicDirReconcile.h>
#include <rune/protocol/RecordBatch.h>
#include "support/OwnerThreadTestHelpers.h"   // leoOnOwner
#include "support/TestNodeConfig.h"
#include "support/TestWorkerCount.h"

#include <gtest/gtest.h>
#include <spdlog/sinks/ostream_sink.h>
#include <spdlog/spdlog.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <map>
#include <memory>
#include <optional>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

using namespace std::chrono_literals;
using namespace rune::node;
using namespace rune::node::test;
namespace fs  = std::filesystem;
namespace cat = rune::catalog;
namespace rr  = rune::replication;
using Clock = std::chrono::steady_clock;

namespace {

struct TempDir {
    fs::path path;
    explicit TempDir(const std::string& tag) {
        static std::atomic<int> seq{0};
        path = fs::temp_directory_path() / ("rune_nsdisk_" + tag + "_" + std::to_string(::getpid()) + "_" +
                                            std::to_string(seq++));
        fs::remove_all(path);
        fs::create_directories(path);
    }
    ~TempDir() { std::error_code ec; fs::remove_all(path, ec); }
    TempDir(const TempDir&) = delete;
    TempDir& operator=(const TempDir&) = delete;
};

void touch(const fs::path& p, const std::string& content = "x") {
    fs::create_directories(p.parent_path());
    std::ofstream(p, std::ios::binary) << content;
}

std::string slurp(const fs::path& p) {
    std::ifstream in(p, std::ios::binary);
    std::ostringstream ss;
    ss << in.rdbuf();
    return ss.str();
}

/// relative path -> bytes ("<dir>" for a directory) of everything below `root`.
std::map<std::string, std::string> treeOf(const fs::path& root) {
    std::map<std::string, std::string> out;
    std::error_code ec;
    for (fs::recursive_directory_iterator it(root, ec), end; !ec && it != end; it.increment(ec)) {
        const std::string rel = fs::relative(it->path(), root).string();
        out[rel] = fs::is_regular_file(it->symlink_status()) ? slurp(it->path()) : "<dir>";
    }
    return out;
}

/// Every regular file's bytes below `root`, concatenated (a topic dir's record bytes, for "holds X" checks).
std::string allBytesBelow(const fs::path& root) {
    std::string s;
    for (const auto& [rel, bytes] : treeOf(root)) s += bytes;
    return s;
}

TopicSpec spec(uint32_t nsId, const std::string& name, int32_t id) {
    TopicSpec s;
    s.nsId = nsId; s.name = name; s.topicId = id; s.partitionCount = 1;
    return s;
}

constexpr int64_t kNow = 1'700'000'000;

struct LogCapture {
    std::ostringstream os;
    std::shared_ptr<spdlog::logger> old, mine;
    LogCapture() {
        mine = std::make_shared<spdlog::logger>("nsdisk", std::make_shared<spdlog::sinks::ostream_sink_mt>(os));
        mine->set_level(spdlog::level::info);
        mine->flush_on(spdlog::level::info);
        old = spdlog::default_logger();
        spdlog::set_default_logger(mine);
    }
    ~LogCapture() { spdlog::set_default_logger(old); }
    bool has(const std::string& needle) const { return os.str().find(needle) != std::string::npos; }
};

template <class Pred>
bool awaitTrue(Pred p, std::chrono::milliseconds timeout) {
    const auto deadline = Clock::now() + timeout;
    while (Clock::now() < deadline) {
        if (p()) return true;
        std::this_thread::sleep_for(10ms);
    }
    return p();
}

NodeConfig makeSingleNodeConfig(const fs::path& dataDir) {
    NodeConfig cfg;
    pinLegacyRuntime(cfg);
    cfg.nodeId = 0;
    cfg.dataDir = dataDir.string();
    cfg.consensusStreamId = 10;
    PeerConfig self;
    self.id = 0;
    self.host = "127.0.0.1";
    self.consensusPort = rune::aeronutil::testing::allocFreeUdpPort();
    cfg.peers = {self};
    cfg.clientEndpoint.channel = "aeron:udp?endpoint=127.0.0.1:" +
                                 std::to_string(rune::aeronutil::testing::allocFreeUdpPort());
    cfg.clientEndpoint.streamId = 1;
    cfg.workerCount = workerCountFromEnv();
    return cfg;
}

cat::CreateTopicOps oneReplicaTopic() {
    cat::CreateTopicOps o;
    o.partitions = 1;
    o.replicationFactor = 1;
    return o;
}

/// NamespaceCreated alpha (ns 1), then CREATE TOPIC t in ns 0 and CREATE TOPIC t in ns 1 (formatVer 2).
std::vector<cat::CatalogEvent> twoNamespaceScript() {
    cat::testing::golden_detail::ScriptBuilder b(cat::kFormatV2);
    rune::protocol::NamespaceLimits l;
    l.maxTopics = 10;
    b.nsCreate("alpha", l, "CREATE NAMESPACE alpha");
    b.inNs(0);
    b.createTopic("t", oneReplicaTopic(), "CREATE TOPIC t");
    b.inNs(1);
    b.createTopic("t", oneReplicaTopic(), "CREATE TOPIC t");
    return std::move(b).take();
}

/// ONE real single-node broker over a data dir that survives restarts.
class NsRig {
public:
    explicit NsRig(const std::string& tag) : dir_(tag), cfg_(makeSingleNodeConfig(dir_.path)) {}
    ~NsRig() { stop(); }
    const fs::path& dataDir() const { return dir_.path; }

    bool start() {
        node_ = std::make_unique<BrokerNode>(cfg_);
        node_->start();
        if (!node_->awaitLeader(30s)) return false;
        return awaitTrue([&] { return node_->onAdminThreadForTest([&] {
            return node_->adminWorkerAgentForTest()->worker().isLeader(); }); }, 30s);
    }
    void stop() { if (node_) { node_->stop(); node_.reset(); } }
    bool restart() { stop(); return start(); }

    /// Append one catalog event on the (single-node) catalog leader, acks=all; false if refused.
    bool appendCatalog(const cat::CatalogEvent& e) {
        auto bytes = cat::encode(e);
        if (!bytes) return false;
        return node_->onAdminThreadForTest([&]() -> bool {
            rr::PartitionReplica* r = node_->catalogReplicaForTest();
            if (r == nullptr || r->role() != rr::Role::Leader) return false;
            if (r->append(std::span<const uint8_t>(*bytes), /*acksAll=*/true) < 0) return false;
            r->onLeaderLogAppended();
            return true;
        });
    }
    std::optional<int32_t> topicId(uint32_t nsId, const std::string& name) {
        return node_->onControlThreadForTest(
            [&] { return node_->gatewayForTest()->topicRegistry()->topicIdForName(nsId, name); });
    }
    bool awaitServes(int32_t id) { return awaitTrue([&] { return node_->catalogTopicServesForTest(id); }, 15s); }
    /// Append `values` as one batch each to (topicId, 0) on its owner thread (the leader replica).
    bool produce(int32_t id, const std::vector<std::string>& values) {
        for (const std::string& v : values) {
            rune::protocol::RecordBatchBuilder b(0, 0);
            b.append(std::string_view{}, v, 0);
            const std::vector<uint8_t> batch = b.build();
            const bool ok = node_->onOwnerThreadForTest(id, 0, [&](rr::PartitionReplica& r) {
                return r.append(std::span<const uint8_t>(batch), /*acksAll=*/false) >= 0;
            });
            if (!ok) return false;
        }
        return true;
    }
    int64_t leo(int32_t id) { return leoOnOwner(*node_, id, 0); }

private:
    TempDir                     dir_;
    NodeConfig                  cfg_;
    std::unique_ptr<BrokerNode> node_;
};

} // namespace

// ── NamespacePaths ──────────────────────────────────────────────────────────────────────────────────────────────

TEST(NamespaceDiskRegression, PathsRegression) {
    const fs::path d = "/data";
    EXPECT_EQ(nsDirName(0), "ns-0");
    EXPECT_EQ(nsDirName(4294967295u), "ns-4294967295");
    EXPECT_EQ(nsRoot(d, 0), d / "ns-0");
    EXPECT_EQ(topicsRoot(d, 0) / "t" / "0", fs::path("/data/ns-0/topics/t/0"));
    EXPECT_EQ(topicsRoot(d, 7) / "t" / "0", fs::path("/data/ns-7/topics/t/0"));
    EXPECT_EQ(droppedRoot(d, 3), fs::path("/data/ns-3/dropped"));
    EXPECT_EQ(quarantineRoot(d, 3), fs::path("/data/ns-3/quarantine"));
    EXPECT_NE(topicsRoot(d, 0), topicsRoot(d, 1));
}

TEST(NamespaceDiskRegression, ParseNsDirTableRegression) {
    struct Case { const char* name; std::optional<uint32_t> want; };
    const Case cases[] = {
        {"ns-0", 0u},
        {"ns-1", 1u},
        {"ns-42", 42u},
        {"ns-4294967295", 4294967295u},
        {"ns-4294967296", std::nullopt},
        {"ns-99999999999", std::nullopt},
        {"ns-", std::nullopt},
        {"ns-01", std::nullopt},
        {"ns-00", std::nullopt},
        {"ns--1", std::nullopt},
        {"ns-+1", std::nullopt},
        {"ns- 1", std::nullopt},
        {"ns-1a", std::nullopt},
        {"nsx1", std::nullopt},
        {"ns1", std::nullopt},
        {"NS-1", std::nullopt},
        {kTopicsDirName.data(), std::nullopt},
        {"", std::nullopt},
        {"ns-1.deleting", std::nullopt},
        {"ns-0.deleting", std::nullopt},
    };
    for (const Case& c : cases) {
        SCOPED_TRACE(std::string("'") + c.name + "'");
        EXPECT_EQ(parseNsDir(c.name), c.want);
    }
    for (const uint32_t id : {0u, 1u, 9u, 10u, 65535u, 4294967295u}) EXPECT_EQ(parseNsDir(nsDirName(id)), id);
}

// ── a real broker: one name, two namespaces, two dirs ─────────────────────────────────────────────────────────────

TEST(NamespaceDiskRegression, SameNameTwoNamespacesDirsRegression) {
    NsRig r("same_name");
    ASSERT_TRUE(r.start());
    for (const cat::CatalogEvent& e : twoNamespaceScript()) ASSERT_TRUE(r.appendCatalog(e));
    ASSERT_TRUE(awaitTrue([&] { return r.topicId(0, "t").has_value() && r.topicId(1, "t").has_value(); }, 15s));
    const int32_t id0 = *r.topicId(0, "t");
    const int32_t id1 = *r.topicId(1, "t");
    ASSERT_NE(id0, id1);
    ASSERT_TRUE(r.awaitServes(id0));
    ASSERT_TRUE(r.awaitServes(id1));
    ASSERT_TRUE(r.produce(id0, {"zero-ns-zero"}));
    ASSERT_TRUE(r.produce(id1, {"one-ns-one", "two-ns-one"}));
    ASSERT_EQ(r.leo(id0), 1);
    ASSERT_EQ(r.leo(id1), 2);

    const fs::path d0 = topicsRoot(r.dataDir(), 0) / "t";
    const fs::path d1 = topicsRoot(r.dataDir(), 1) / "t";
    auto expectDirs = [&] {
        ASSERT_TRUE(fs::is_directory(d0 / "0")) << d0;
        ASSERT_TRUE(fs::is_directory(d1 / "0")) << d1;
        const auto m0 = rune::gateway::readTopicIdMarker(d0);
        const auto m1 = rune::gateway::readTopicIdMarker(d1);
        ASSERT_EQ(m0.kind, rune::gateway::TopicIdMarker::Kind::Valid);
        ASSERT_EQ(m1.kind, rune::gateway::TopicIdMarker::Kind::Valid);
        EXPECT_EQ(m0.id, id0);
        EXPECT_EQ(m1.id, id1);
        const std::string b0 = allBytesBelow(d0), b1 = allBytesBelow(d1);
        EXPECT_NE(b0.find("zero-ns-zero"), std::string::npos);
        EXPECT_EQ(b0.find("ns-one"), std::string::npos) << "ns 1's records never land in ns 0's dir";
        EXPECT_NE(b1.find("one-ns-one"), std::string::npos);
        EXPECT_NE(b1.find("two-ns-one"), std::string::npos);
        EXPECT_EQ(b1.find("zero-ns-zero"), std::string::npos) << "ns 0's records never land in ns 1's dir";
        EXPECT_FALSE(fs::exists(quarantineRoot(r.dataDir(), 0))) << "nothing quarantined";
        EXPECT_FALSE(fs::exists(quarantineRoot(r.dataDir(), 1))) << "nothing quarantined";
    };
    expectDirs();

    ASSERT_TRUE(r.restart());
    ASSERT_EQ(r.topicId(0, "t"), id0);
    ASSERT_EQ(r.topicId(1, "t"), id1);
    ASSERT_TRUE(r.awaitServes(id0));
    ASSERT_TRUE(r.awaitServes(id1));
    EXPECT_EQ(r.leo(id0), 1) << "ns 0's t reopened its own log";
    EXPECT_EQ(r.leo(id1), 2) << "ns 1's t reopened its own log";
    expectDirs();
}

// ── the boot reconcile, per namespace root ────────────────────────────────────────────────────────────────────────

// (1, t) was dropped while the broker was down (its drop committed, the rename never ran): the boot reconcile
// retires and purges ns-1/topics/t only - ns 0's same-named live dir and ns 2's unrelated dir are untouched.
TEST(NamespaceDiskRegression, ReconcileDropsUnderNsRootRegression) {
    TempDir d("drop");
    const fs::path t0 = topicsRoot(d.path, 0) / "t";
    const fs::path t1 = topicsRoot(d.path, 1) / "t";
    const fs::path u2 = topicsRoot(d.path, 2) / "u";
    touch(t0 / "0" / "seg.log", "ns0-data");
    ASSERT_TRUE(rune::gateway::writeTopicIdMarker(t0, 0));
    touch(t1 / "0" / "seg.log", "ns1-data");
    ASSERT_TRUE(rune::gateway::writeTopicIdMarker(t1, 1));
    touch(u2 / "0" / "seg.log", "ns2-data");
    ASSERT_TRUE(rune::gateway::writeTopicIdMarker(u2, 2));
    const auto before0 = treeOf(nsRoot(d.path, 0));
    const auto before2 = treeOf(nsRoot(d.path, 2));

    const std::vector<int32_t> dropped{1};
    const auto res = reconcileTopicDirs(d.path.string(), {spec(0, "t", 0), spec(2, "u", 2)}, dropped, kNow);

    EXPECT_FALSE(fs::exists(t1)) << "the dropped (1, t) dir is gone";
    ASSERT_EQ(res.retired.size(), 1u);
    EXPECT_EQ(res.retired[0], droppedRoot(d.path, 1) / "1.t") << "retired under its own ns root";
    ASSERT_EQ(res.purged.size(), 1u);
    EXPECT_EQ(res.purged[0], droppedRoot(d.path, 1) / "1.t");
    EXPECT_FALSE(fs::exists(droppedRoot(d.path, 1) / "1.t")) << "purged in the same run";
    EXPECT_TRUE(res.quarantined.empty());
    EXPECT_EQ(treeOf(nsRoot(d.path, 0)), before0) << "ns 0's same-named live dir is untouched";
    EXPECT_EQ(treeOf(nsRoot(d.path, 2)), before2);
    EXPECT_FALSE(fs::exists(droppedRoot(d.path, 0)));
    EXPECT_FALSE(fs::exists(d.path / "dropped")) << "nothing is ever made at the pre-namespace top level";
    EXPECT_FALSE(fs::exists(d.path / "quarantine"));

    // Idempotent: a second run changes nothing.
    const auto all = treeOf(d.path);
    const auto again = reconcileTopicDirs(d.path.string(), {spec(0, "t", 0), spec(2, "u", 2)}, dropped, kNow + 1);
    EXPECT_TRUE(again.retired.empty());
    EXPECT_TRUE(again.purged.empty());
    EXPECT_TRUE(again.quarantined.empty());
    EXPECT_EQ(treeOf(d.path), all);
}

// A dataDir entry that is not ns-<u32> - a stray dir, a pre-namespace topics/ (no migration), a namespace drop in
// progress - is logged and never read, moved or deleted, even when it holds a dir a dropped id marks.
TEST(NamespaceDiskRegression, ReconcileIgnoresForeignDirRegression) {
    TempDir d("foreign");
    touch(d.path / "junk" / "file", "stray");
    touch(d.path / kTopicsDirName / "t" / "0" / "seg.log", "pre-namespace");
    ASSERT_TRUE(rune::gateway::writeTopicIdMarker(d.path / kTopicsDirName / "t", 1));
    touch(d.path / "dropped" / "1.t" / "x.log", "old-dropped");
    touch(d.path / "ns-1.deleting" / kTopicsDirName / "t" / "0" / "seg.log", "deleting");
    touch(topicsRoot(d.path, 0) / "t" / "0" / "seg.log", "live");
    ASSERT_TRUE(rune::gateway::writeTopicIdMarker(topicsRoot(d.path, 0) / "t", 0));
    const auto junk = treeOf(d.path / "junk");
    const auto top = treeOf(d.path / kTopicsDirName);
    const auto oldDropped = treeOf(d.path / "dropped");
    const auto deleting = treeOf(d.path / "ns-1.deleting");
    const auto ns0 = treeOf(nsRoot(d.path, 0));

    LogCapture logs;
    const std::vector<int32_t> dropped{1};
    const auto res = reconcileTopicDirs(d.path.string(), {spec(0, "t", 0)}, dropped, kNow);

    EXPECT_TRUE(res.quarantined.empty());
    EXPECT_TRUE(res.retired.empty());
    EXPECT_TRUE(res.purged.empty());
    EXPECT_EQ(treeOf(d.path / "junk"), junk);
    EXPECT_EQ(treeOf(d.path / kTopicsDirName), top);
    EXPECT_EQ(treeOf(d.path / "dropped"), oldDropped);
    EXPECT_EQ(treeOf(d.path / "ns-1.deleting"), deleting);
    EXPECT_EQ(treeOf(nsRoot(d.path, 0)), ns0);
    EXPECT_FALSE(fs::exists(nsRoot(d.path, 1))) << "ns-1.deleting is not ns 1: nothing made for it";
    for (const std::string_view n : {std::string_view{"junk"}, kTopicsDirName, std::string_view{"dropped"},
                                     std::string_view{"ns-1.deleting"}}) {
        SCOPED_TRACE(n);
        EXPECT_TRUE(logs.has("leaving '" + (d.path / n).string() + "' alone")) << logs.os.str();
    }
    EXPECT_FALSE(logs.has("leaving '" + nsRoot(d.path, 0).string() + "' alone"));
}
