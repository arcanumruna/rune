// SegmentRepairClusterRegressionTest.cpp - #880: a damaged sealed segment is rebuilt from an in-sync peer, on REAL brokers.
// The REAL client against three in-process BrokerNodes (support/CatalogBrokerCluster.h ThreeNodes: shard runtime, real
// disk, real Aeron UDP).  A topic of one partition with a small segment roll is produced to, then one node's copy of the
// lowest segment is damaged (a byte flipped inside its 2nd batch):
//   - a FOLLOWER while its node is down: on restart boot recovery keeps and counts the damage, the node refetches the
//     range from the leader and swaps the repaired file in, and the node's file ends byte-identical to a peer's;
//   - the LEADER while it runs (the file is flipped and the log reloaded on the owner thread): it repairs itself from an
//     in-sync follower with no leadership change; a client that re-subscribes reads every record;
//   - a topic of RF 1: nothing to repair from - it stays damaged exactly as #839 leaves it, counted.
// Every wait has a hard cap.
#include "client/ClusterClient.h"
#include "MetricsTestReader.h"

#include <rune/client/Client.h>
#include <rune/client/Completions.h>
#include <rune/client/Key.h>
#include <rune/client/producer/Producer.h>
#include <rune/client/subscriber/Subscription.h>
#include <rune/protocol/RecordBatch.h>

#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <vector>

using namespace std::chrono_literals;
using namespace rune::node;
using namespace rune::node::test;
using namespace rune::node::test::catalog_cluster;
namespace rc = rune::client;
namespace fs = std::filesystem;

namespace {

constexpr int kRecords = 200;

std::vector<std::size_t> batchStarts(const fs::path& logPath) {
    std::ifstream in(logPath, std::ios::binary);
    std::vector<uint8_t> buf((std::istreambuf_iterator<char>(in)), {});
    std::vector<std::size_t> out;
    std::size_t pos = 0;
    while (pos + static_cast<std::size_t>(rune::protocol::batch::kRecordBatchOverhead) <= buf.size()) {
        rune::protocol::RecordBatchReader r(buf.data() + pos, buf.size() - pos);
        const int32_t total = r.totalLength();
        if (total < rune::protocol::batch::kRecordBatchOverhead || pos + static_cast<std::size_t>(total) > buf.size()) break;
        out.push_back(pos);
        pos += static_cast<std::size_t>(total);
    }
    return out;
}

std::vector<uint8_t> slurp(const fs::path& p) {
    std::ifstream in(p, std::ios::binary);
    return std::vector<uint8_t>((std::istreambuf_iterator<char>(in)), {});
}

/// The directory under `root` that names `topic` and holds >= 3 .log files.
fs::path findPartitionDir(const fs::path& root, std::string_view topic) {
    for (const auto& e : fs::recursive_directory_iterator(root)) {
        if (!e.is_directory() || e.path().string().find(topic) == std::string::npos) continue;
        std::size_t logs = 0;
        for (const auto& f : fs::directory_iterator(e.path())) logs += (f.path().extension() == ".log");
        if (logs >= 3) return e.path();
    }
    return {};
}

fs::path lowestSegment(const fs::path& dataDir, std::string_view topic) {
    const fs::path part = findPartitionDir(dataDir, topic);
    if (part.empty()) return {};
    std::vector<fs::path> segs;
    for (const auto& f : fs::directory_iterator(part))
        if (f.path().extension() == ".log") segs.push_back(f.path());
    std::sort(segs.begin(), segs.end());
    return segs.empty() ? fs::path{} : segs.front();
}

/// Flip one byte inside the 2nd batch of `seg`; returns the first offset of that batch (the damage offset), -1 on failure.
int64_t damage(const fs::path& seg) {
    const auto starts = batchStarts(seg);
    if (starts.size() < 2) return -1;
    const auto bytes = slurp(seg);
    const int64_t d = rune::protocol::RecordBatchReader(bytes.data() + starts[1], bytes.size() - starts[1]).baseOffset();
    std::fstream f(seg, std::ios::in | std::ios::out | std::ios::binary);
    const std::size_t at = starts[1] + static_cast<std::size_t>(rune::protocol::batch::kRecordBatchOverhead);
    f.seekg(static_cast<std::streamoff>(at));
    char c = 0;
    f.read(&c, 1);
    c = static_cast<char>(c ^ 0x5A);
    f.seekp(static_cast<std::streamoff>(at));
    f.write(&c, 1);
    return f.good() ? d : -1;
}

/// A metric of node `id`, polled until it reaches `atLeast` (bounded).
bool awaitMetric(ThreeNodes& c, int id, RuneMetric m, int64_t atLeast, std::chrono::milliseconds within) {
    const auto deadline = std::chrono::steady_clock::now() + within;
    for (int i = 0; i < 100'000 && std::chrono::steady_clock::now() < deadline; ++i) {
        const auto v = readRuneMetric(c.node(id), m);
        if (v && *v >= atLeast) return true;
        std::this_thread::sleep_for(20ms);
    }
    return false;
}

int64_t metricOf(ThreeNodes& c, int id, RuneMetric m) { return readRuneMetric(c.node(id), m).value_or(-1); }

struct Topic {
    rc::TopicSpec spec;
    int32_t       topicId = -1;
};

/// Define `name` (1 partition, `rf` replicas, small segments) and produce kRecords records acked by all in-sync replicas.
bool defineAndProduce(ThreeNodes& c, std::string_view name, int rf, Topic& out, rc::Acks acks = rc::Acks::All) {
    if (!c.awaitCatalogLeader(30s)) return false;
    auto admin = connectClient(c, c.runningNodes().front());
    rc::TopicOptions to;
    to.replicationFactor      = static_cast<int16_t>(rf);
    to.config.segmentRollBytes = 4096;
    const auto def = admin->defineTopicAndWait(std::string(name), 1, "", {}, rc::Persistence::Disk, to, {}, 60s);
    admin->close();
    if (def.errorCode != 0) return false;
    out.topicId = def.topicId;
    out.spec    = rc::TopicSpec{std::string(name), def.topicId, 1};
    std::vector<int32_t> leaders;
    if (!awaitLeaders(c, def.topicId, 1, leaders, 30s)) return false;
    auto cfg = clientConfigFor(c, leaders[0]);
    cfg.topics = {out.spec};
    auto client = connectClient(c, cfg);
    rc::ProducerConfig pc;
    pc.acks     = acks;
    pc.lingerUs = 0;
    rc::Producer prod = client->producer(pc);
    rc::Completions cq(*client);
    const rc::TopicHandle th = client->topic(out.spec.name);
    bool ok = true;
    for (int k = 0; k < kRecords && ok; ++k) {
        const std::string v = std::string(64, static_cast<char>('a' + k % 26));
        rc::SendResult r = rc::SendResult::BackPressured;
        for (int spin = 0; spin < 100'000 && r == rc::SendResult::BackPressured; ++spin) {
            r = prod.trySend(th, rc::Key(static_cast<uint64_t>(k)),
                             std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(v.data()), v.size()), cq,
                             static_cast<rc::Token>(k + 1));
            if (r == rc::SendResult::BackPressured) std::this_thread::yield();
        }
        if (r != rc::SendResult::Ok) { ok = false; break; }
        prod.flush();
        bool got = false;
        for (int i = 0; i < 30'000 && !got; ++i) {
            (void)cq.poll([&](const rc::Completion& d, std::span<const uint8_t>) {
                if (d.token == static_cast<rc::Token>(k + 1)) { got = true; ok = ok && d.status == rc::OpStatus::Ok && d.value == k; }
            });
            if (!got) std::this_thread::sleep_for(1ms);
        }
        ok = ok && got;
    }
    prod.close();
    client->close();
    return ok;
}

/// Subscribe from the earliest offset through a client seeded on `via`; returns the offsets read (until `want` records, the
/// subscription ends, or the cap) and whether the subscription ended by itself.
std::vector<int64_t> readAll(ThreeNodes& c, int via, const rc::TopicSpec& spec, std::size_t want, bool* ended = nullptr) {
    auto cfg = clientConfigFor(c, via);
    cfg.topics = {spec};
    auto client = connectClient(c, cfg);
    std::mutex mu;
    std::vector<int64_t> got;
    rc::SubscribeOptions o;
    o.from = rc::From::earliest();
    rc::Subscription sub = client->subscribe(client->topic(spec.name), 0, o, [&](const rc::Record& r) {
        std::lock_guard<std::mutex> g(mu);
        got.push_back(r.offset);
    });
    for (int i = 0; i < 30'000; ++i) {
        {
            std::lock_guard<std::mutex> g(mu);
            if (got.size() >= want) break;
        }
        if (sub.finished()) break;
        std::this_thread::sleep_for(2ms);
    }
    if (ended) *ended = sub.finished();
    sub.unsubscribe();
    client->close();
    std::lock_guard<std::mutex> g(mu);
    return got;
}

bool isIdentity(const std::vector<int64_t>& v, std::size_t n) {
    if (v.size() != n) return false;
    for (std::size_t i = 0; i < n; ++i) if (v[i] != static_cast<int64_t>(i)) return false;
    return true;
}

}  // namespace

TEST(SegmentRepairClusterRegression, AFollowerDamagedWhileDownIsRepairedFromTheLeaderRegression) {
    ThreeNodes c = ThreeNodes::start("segrepair_follower");
    Topic t;
    ASSERT_TRUE(defineAndProduce(c, "rep.f", 3, t));
    const int32_t leader = leaderOn(c.node(1), t.topicId, 0);
    ASSERT_GT(leader, 0);
    int victim = -1, peer = -1;
    for (int id : c.runningNodes()) {
        if (id == leader) continue;
        (victim < 0 ? victim : peer) = id;
    }
    ASSERT_GT(victim, 0);
    ASSERT_GT(peer, 0);
    const std::string topic = "rep.f";
    const fs::path peerSeg = lowestSegment(c.dirs[static_cast<std::size_t>(peer - 1)]->path, topic);
    ASSERT_FALSE(peerSeg.empty());
    const auto intact = slurp(peerSeg);
    c.stop(victim);
    const fs::path seg = lowestSegment(c.dirs[static_cast<std::size_t>(victim - 1)]->path, topic);
    ASSERT_FALSE(seg.empty());
    ASSERT_EQ(slurp(seg), intact) << "replicas hold byte-identical copies";
    const int64_t d = damage(seg);
    ASSERT_GT(d, 0);
    ASSERT_NE(slurp(seg), intact);
    c.startNode(victim);
    EXPECT_EQ(c.node(victim).bootDamagedSegmentsForTest(), 1) << "boot recovery keeps and counts the damaged segment";
    ASSERT_TRUE(awaitMetric(c, victim, RuneMetric::SegmentRepairs, 1, 90s)) << "the node never repaired its segment";
    EXPECT_EQ(metricOf(c, victim, RuneMetric::SegmentRepairNoPeer), 0);
    // The cleaner thread finishes the renames; the file ends byte-identical to the peers' copy.
    bool same = false;
    for (int i = 0; i < 3000 && !same; ++i) {
        same = slurp(seg) == intact;
        if (!same) std::this_thread::sleep_for(10ms);
    }
    EXPECT_TRUE(same) << "the repaired file differs from the intact copy";
    // Every record is readable through the cluster, in order, each once.
    EXPECT_TRUE(isIdentity(readAll(c, victim, t.spec, kRecords), kRecords));
    // The repaired copy survives a restart with no damage.
    c.stop(victim);
    c.startNode(victim);
    EXPECT_EQ(c.node(victim).bootDamagedSegmentsForTest(), 0) << "no damage after the restart";
    // The old leader goes away: whichever of the two remaining nodes leads now, a consumer gets every record.
    c.stop(leader);
    std::vector<int32_t> leaders;
    ASSERT_TRUE(awaitLeaders(c, t.topicId, 1, leaders, 60s)) << "no leader after the old leader stopped";
    EXPECT_NE(leaders[0], leader);
    EXPECT_TRUE(isIdentity(readAll(c, victim, t.spec, kRecords), kRecords)) << "reads after the failover lost records";
}

TEST(SegmentRepairClusterRegression, ALeaderDamagedWhileRunningRepairsItselfWithoutALeadershipChangeRegression) {
    ThreeNodes c = ThreeNodes::start("segrepair_leader");
    Topic t;
    ASSERT_TRUE(defineAndProduce(c, "rep.l", 3, t));
    const int32_t leader = leaderOn(c.node(1), t.topicId, 0);
    ASSERT_GT(leader, 0);
    const std::string topic = "rep.l";
    const fs::path seg = lowestSegment(c.dirs[static_cast<std::size_t>(leader - 1)]->path, topic);
    ASSERT_FALSE(seg.empty());
    const auto intact = slurp(seg);
    const int64_t d = damage(seg);
    ASSERT_GT(d, 0);
    // The live reload recovers the file again: the damage is found (the test seam the storage tests use).
    ASSERT_TRUE(c.node(leader).onOwnerThreadForTest(t.topicId, 0, [](rune::replication::PartitionReplica& r) {
        return r.log()->reloadSegments() && r.log()->damagedSegmentCount() == 1;
    }, 10s)) << "the live reload did not find the damage";
    // A reader that arrives now fails at the damage or (if the repair already finished) reads on; either way a reader
    // that re-subscribes once the repair completed reads everything.
    ASSERT_TRUE(awaitMetric(c, leader, RuneMetric::SegmentRepairs, 1, 90s)) << "the leader never repaired its segment";
    EXPECT_EQ(leaderOn(c.node(1), t.topicId, 0), leader) << "no leadership change";
    bool same = false;
    for (int i = 0; i < 3000 && !same; ++i) {
        same = slurp(seg) == intact;
        if (!same) std::this_thread::sleep_for(10ms);
    }
    EXPECT_TRUE(same);
    EXPECT_TRUE(isIdentity(readAll(c, leader, t.spec, kRecords), kRecords)) << "a re-subscribed consumer reads every record";
    EXPECT_EQ(leaderOn(c.node(1), t.topicId, 0), leader);
}

TEST(SegmentRepairClusterRegression, ARfOneTopicStaysDamagedAndIsCountedRegression) {
    // A topic of one replica needs a minimum ISR of 1 (the default majority of a 3-node cluster is 2).
    ThreeNodes c = ThreeNodes::startWith("segrepair_rf1", [](NodeConfig& cfg) { cfg.gatewayMinIsr = 1; });
    Topic t;
    ASSERT_TRUE(defineAndProduce(c, "rep.1", 1, t, rc::Acks::Leader));
    const int32_t host = leaderOn(c.node(1), t.topicId, 0);
    ASSERT_GT(host, 0);
    const std::string topic = "rep.1";
    c.stop(host);
    const fs::path seg = lowestSegment(c.dirs[static_cast<std::size_t>(host - 1)]->path, topic);
    ASSERT_FALSE(seg.empty());
    ASSERT_GT(damage(seg), 0);
    const auto damaged = slurp(seg);
    c.startNode(host);
    EXPECT_EQ(c.node(host).bootDamagedSegmentsForTest(), 1);
    ASSERT_TRUE(awaitMetric(c, host, RuneMetric::SegmentRepairNoPeer, 1, 90s)) << "the missing peer was never counted";
    EXPECT_EQ(metricOf(c, host, RuneMetric::SegmentRepairs), 0);
    EXPECT_EQ(slurp(seg), damaged) << "the damaged file is exactly as #839 leaves it";
}
