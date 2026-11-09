// CheckedWritesBrokerRegressionTest.cpp — checked writes Part 02: typed validation against REAL brokers and the REAL
// client on the default (shard) runtime.  Worker state is read only inside onOwnerThreadForTest (a direct read from
// the test thread would race the worker).
#include "client/ClusterClient.h"
#include "client/OneBroker.h"
#include "support/SharedClientDriver.h"
#include "client/CheckedWritesBrokerSupport.h"
#include "client/support/KeyIndexRecords.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <cstring>
#include <atomic>
#include <memory>
#include <future>
#include <mutex>
#include <optional>
#include <set>
#include <thread>

using namespace std::chrono_literals;
using namespace rune::node;
using namespace rune::node::test;
namespace cwb = rune::node::test::cw_broker;
namespace rc  = rune::client;
namespace cw  = rune::node::cw;

namespace {

/// The three-broker cluster of these tests: the short warm-up, fused workers, a 40 ms election and a 150 ms liveness timeout, so a crashed
/// leader's partitions fail over in about 0.2 s; the read lease max is 50 ms for a restart's wait for the old leader's lease.  Its clients
/// share one media driver: a client otherwise starts one of its own (100-180 ms).
struct Cluster : rune::node::test::catalog_cluster::ThreeNodes {
    std::unique_ptr<SharedClientDriver> driver;

    Cluster(ThreeNodes&& nodes, std::unique_ptr<SharedClientDriver> d) : ThreeNodes(std::move(nodes)), driver(std::move(d)) {}
};

Cluster startCluster(const std::string& tag) {
    auto driver = std::async(std::launch::async, [] { return std::make_unique<SharedClientDriver>(); });
    auto nodes  = rune::node::test::catalog_cluster::ThreeNodes::startWith(tag, [](NodeConfig& cfg) {
        cfg.unservableWarmupMs      = 50;
        cfg.runtimePlacement        = NodeConfig::RuntimePlacement::Fused;
        cfg.electionTimeoutMs       = 40;
        cfg.readLeaseMaxMs          = 50;
        cfg.brokerLivenessTimeoutMs = 150;
    });
    return Cluster(std::move(nodes), driver.get());
}

/// A client of broker `id` whose leader's dead session is noticed after 0.5 s, not the 10 s default, and whose DDL retry after a failover waits 5 ms.
rc::ClientConfig quickConfig(Cluster& c, int id) {
    rc::ClientConfig cfg = rune::node::test::catalog_cluster::clientConfigFor(c, id);
    cfg = c.driver->attach(cfg);
    cfg.responseTimeoutNs = std::chrono::nanoseconds(300ms).count();
    cfg.keepaliveIntervalNs = std::chrono::nanoseconds(60ms).count();
    cfg.ddlRetryBackoff = 5ms;
    cfg.produceTimeoutMs = 300;
    return cfg;
}

}  // namespace

TEST(CheckedWritesBrokerRegression, TopicCreateWaitsForItsSpecOnEveryWorkerRegression) {
    OneBroker b("cw-order", OneBroker::Runtime::Shard, [](NodeConfig& cfg) { cfg.testHoldWriteSpecPasses = 300; });
    rune::client::Client& c = *b.connectClient();
    const auto r = cwb::defineTyped(c, "chars", 2);
    ASSERT_EQ(r.errorCode, 0) << r.detail;
    for (int32_t p = 0; p < 2; ++p)
        EXPECT_TRUE(cwb::hasWriteState(b.node(), r.topicId, p)) << "partition " << p << ": the topic served before its spec";
    EXPECT_EQ(cwb::produceRaw(b, "chars", r.topicId, 2, std::string("\x01\x02\x03", 3), rune::client::Key(uint64_t{1})),
              static_cast<uint16_t>(rune::protocol::client::ErrorCode::SchemaValidationFailed));
}

TEST(CheckedWritesBrokerRegression, DroppedWhileAwaitingSpecNeverBuildsRegression) {
    OneBroker b("cw-drop", OneBroker::Runtime::Shard, [](NodeConfig& cfg) { cfg.testHoldWriteSpecPasses = 400000; });
    rune::client::Client& c = *b.connectClient();
    // The create is parked behind the held spec; the define answers only once it served, so drive the drop from a second
    // client while the first is still waiting.
    std::atomic<int32_t> topicId{-1};
    std::thread definer([&] {
        const auto r = cwb::defineTyped(c, "chars", 2);
        topicId = r.errorCode == 0 ? r.topicId : -2;
    });
    // The id is known to the catalog before the topic serves.
    int32_t id = -1;
    for (int i = 0; i < 2000 && id < 0; ++i) {
        id = cwb::catalogTopicId(b.node(), "chars");
        if (id < 0) std::this_thread::sleep_for(5ms);
    }
    ASSERT_GE(id, 0);
    rune::client::Client& c2 = *b.connectClient();
    const auto dropped = c2.dropTopicAndWait("chars", {}, 20s);
    EXPECT_EQ(dropped.errorCode, 0) << dropped.detail;
    definer.join();
    // The create committed, then the drop: the define is answered OK at once.  A parked create that outlived the drop
    // (whose held spec the drop forgot) would be built afterwards - replicas of a dropped topic - and the define held
    // until it answers in doubt.  RED: keep the parked create in dropRuntimeTopic_ (code 6 after 5 s).
    EXPECT_EQ(topicId.load(), id);
    EXPECT_FALSE(b.node().catalogTopicServesForTest(id));
    for (int32_t p = 0; p < 2; ++p) EXPECT_FALSE(cwb::hasWriteState(b.node(), id, p));
    EXPECT_FALSE(cwb::anyWorkerHasValidator(b.node(), id));
}

// ===========================================================================================================
// Task 9: typed validation end to end on real brokers.
// ===========================================================================================================
#include "support/ShardTestClient.h"

#include <rune/cli/Cli.h>
#include <rune/protocol/client/SchemaMessages.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/storage/Log.h>
#include <rune/storage/ProducerStateManager.h>

#include <filesystem>
#include <sstream>

namespace {

constexpr uint16_t kOk = 0, kNotLeader = 2, kSchemaValidationFailed = 24, kViewSourceNotFound = 33, kViewKeyInvalid = 35, kInvalidSchema = 23;

rune::client::Key keyOf(const std::string& bytes) {
    return rune::client::Key(std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(bytes.data()), bytes.size()));
}

/// The values stored in one partition, in offset order (read on the partition's owner thread).
std::vector<std::string> storedValues(BrokerNode& n, int32_t topicId, int32_t partition) {
    return n.onOwnerThreadForTest(topicId, partition, [](rune::replication::PartitionReplica& r) {
        std::vector<std::string> out;
        rune::storage::Log& log = *r.log();
        int64_t next = 0;
        const int64_t leo = log.logEndOffset();
        while (next < leo) {
            const std::vector<uint8_t> data = log.read(next, 1 << 22);
            if (data.empty()) break;
            const int64_t before = next;
            std::size_t pos = 0;
            while (pos < data.size()) {
                rune::protocol::RecordBatchReader reader(data.data() + pos, data.size() - pos);
                if (!reader.ensureValid()) break;
                const auto tl = static_cast<std::size_t>(reader.totalLength());
                if (!reader.isControl())
                    reader.forEachRecord([&](std::string_view, std::string_view v, int64_t off, int64_t) {
                        if (off < next) return;
                        out.emplace_back(v);
                        next = off + 1;
                    });
                if (tl == 0) break;
                pos += tl;
            }
            if (next == before) break;
        }
        return out;
    });
}

uint16_t send(OneBroker& b, const std::string& name, int32_t topicId, int32_t parts, const cw::TypedRecord& r) {
    return cwb::produceRaw(b, name, topicId, parts, r.value, keyOf(r.key));
}

/// A typed topic with a custom descriptor, keyed on `key`.
rc::DefineTopicResult defineWith(rc::Client& c, const std::string& name, const rune::codec::Descriptor& d, const std::string& key,
                                 uint32_t parts = 1) {
    return c.defineTopicAndWait(name, parts, key, rune::codec::serialize(d), rc::Persistence::Disk, {}, {}, 60s);
}

std::string le(uint64_t v, std::size_t n) { return cw::leBytes(v, n); }

/// A raw DefineTopic from an "old client" (validate = false): the topic predates the feature level.
int32_t defineLegacyTopic(OneBroker& b, const std::string& name, const rune::codec::Descriptor& d) {
    namespace fs = std::filesystem;
    const fs::path dir = fs::temp_directory_path() / ("rune_cwlegacy_" + std::to_string(::getpid()) + "_" + name);
    fs::create_directories(dir);
    int32_t topicId = -1;
    {
        rune::node::test::ShardTestClient client(dir / "aeron");
        const int64_t sid = client.connect(b.clientChannel(), 110);
        const auto blob = rune::codec::serialize(d);
        const auto r = client.defineTopicUntil(b.clientChannel(), 110, sid, name, 1, /*validate=*/false, blob,
                                               std::chrono::steady_clock::now() + 30s);
        if (r && r->errorCode == 0) topicId = r->topicId;
    }
    std::error_code ignore;
    fs::remove_all(dir, ignore);
    return topicId;
}

int cli(std::vector<std::string> args, std::string* err = nullptr) {
    std::stringstream out, e;
    const int rc = rune::cli::run(std::move(args), out, e);
    if (err) *err = e.str();
    return rc;
}

/// The stub a generated type presents to openTopicAndWait (the fingerprint of the matrix descriptor).
struct StubTyped {
    static inline const uint64_t kFingerprint = rune::codec::fingerprint(cw::matrixDescriptor());
    static constexpr std::string_view kSchemaName = "m";
    std::size_t encodedSize() const noexcept { return 4; }
    std::size_t encode(uint8_t* buf) const noexcept { std::memset(buf, 0, 4); return 4; }
};

}  // namespace

TEST(CheckedWritesBrokerRegression, RawSendIsRawOnUntypedAndValidatedOnTypedRegression) {
    OneBroker b("cw-raw", OneBroker::Runtime::Shard);
    rc::Client& c = *b.connectClient();
    const auto raw = c.defineTopicAndWait("raw", 1, "", {}, rc::Persistence::Disk, {}, {}, 30s);
    ASSERT_EQ(raw.errorCode, 0) << raw.detail;
    const rc::TopicHandle handle = c.rawTopicAndWait("raw", 20s);
    EXPECT_EQ(handle.topicId, raw.topicId);
    const std::string garbage("\x01\x02\x03", 3);
    EXPECT_EQ(cwb::produceRaw(b, "raw", raw.topicId, 1, garbage, rc::Key()), kOk);
    EXPECT_EQ(storedValues(b.node(), raw.topicId, 0), std::vector<std::string>{garbage});
    const auto typed = cwb::defineTyped(c, "p", 1);
    ASSERT_EQ(typed.errorCode, 0) << typed.detail;
    EXPECT_EQ(cwb::produceRaw(b, "p", typed.topicId, 1, garbage, rc::Key(uint64_t{1})), kSchemaValidationFailed);
    EXPECT_TRUE(storedValues(b.node(), typed.topicId, 0).empty());
}

TEST(CheckedWritesBrokerRegression, UnknownVersionIsRefusedRegression) {
    OneBroker b("cw-ver", OneBroker::Runtime::Shard);
    const auto t = cwb::defineTyped(*b.connectClient(), "p", 1);
    ASSERT_EQ(t.errorCode, 0) << t.detail;
    EXPECT_EQ(send(b, "p", t.topicId, 1, cw::validRecord(1)), kOk);
    cw::Parts zero; zero.version = 0;
    EXPECT_EQ(send(b, "p", t.topicId, 1, cw::encode(zero)), kSchemaValidationFailed);
}

TEST(CheckedWritesBrokerRegression, PreUpgradeTopicValidatesAfterTheLevelRegression) {
    int16_t level = 4;   // the cluster does not yet support kCheckedWrites
    OneBroker b("cw-upgrade", OneBroker::Runtime::Shard, [&level](NodeConfig& cfg) { cfg.testSupportedFeatureLevel = level; });
    const int32_t id = defineLegacyTopic(b, "p", cw::matrixDescriptor());
    ASSERT_GE(id, 0);
    cw::Parts bad; bad.name = std::string("a\x80", 2);
    EXPECT_EQ(send(b, "p", id, 1, cw::encode(bad)), kOk) << "below the level a topic created without validate accepts it";
    const auto atLevel = b.node().onWorkerThreadForTest(0, [](Worker& w) { return w.clusterFeatureLevelForTest(); });
    EXPECT_LE(atLevel, 4);

    level = -1;
    b.restart();
    bool raised = false;
    for (int i = 0; i < 400 && !raised; ++i) {
        raised = b.node().onWorkerThreadForTest(0, [](Worker& w) { return w.clusterFeatureLevelForTest(); }) >= rune::replication::MetadataFeature::kCheckedWrites;
        if (!raised) std::this_thread::sleep_for(25ms);
    }
    ASSERT_TRUE(raised) << "the restarted node raises the level to the binary's own";
    EXPECT_EQ(send(b, "p", id, 1, cw::encode(bad)), kSchemaValidationFailed);
    EXPECT_EQ(storedValues(b.node(), id, 0).size(), 1u) << "the record accepted before the level still reads back";
}

TEST(CheckedWritesBrokerRegression, RawKeyOfTheWrongWidthIsRefusedRegression) {
    using rune::codec::DescriptorBuilder;
    using rune::codec::PrimType;
    OneBroker b("cw-key", OneBroker::Runtime::Shard);
    rc::Client& c = *b.connectClient();
    const auto q = defineWith(c, "q", DescriptorBuilder("q", 1).add("id", PrimType::UInt32).add("v", PrimType::Int64).build(), "id");
    const auto s = defineWith(c, "s", DescriptorBuilder("s", 1).addFixedStr("name", 4).add("v", PrimType::Int64).build(), "name");
    const auto k = c.defineTopicAndWait("k", 1, "", rune::codec::serialize(DescriptorBuilder("k", 1).add("v", PrimType::Int64).build()),
                                        rc::Persistence::Disk, {}, {}, 60s);
    ASSERT_EQ(q.errorCode, 0) << q.detail;
    ASSERT_EQ(s.errorCode, 0) << s.detail;
    ASSERT_EQ(k.errorCode, 0) << k.detail;
    const std::string qv = le(12, 2) + le(1, 2) + le(7, 4) + le(0, 8);
    EXPECT_EQ(cwb::produceRaw(b, "q", q.topicId, 1, qv, rc::Key(uint64_t{7})), kSchemaValidationFailed) << "8-byte key on a uint32 field";
    EXPECT_EQ(cwb::produceRaw(b, "q", q.topicId, 1, qv, rc::Key(uint32_t{7})), kOk);
    const std::string sv = le(12, 2) + le(1, 2) + std::string("ab\0\0", 4) + le(0, 8);
    EXPECT_EQ(cwb::produceRaw(b, "s", s.topicId, 1, sv, keyOf("ab")), kSchemaValidationFailed) << "an unpadded string(4) key";
    EXPECT_EQ(cwb::produceRaw(b, "s", s.topicId, 1, sv, keyOf(std::string("ab\0\0", 4))), kOk);
    const std::string kv = le(8, 2) + le(1, 2) + le(0, 8);
    EXPECT_EQ(cwb::produceRaw(b, "k", k.topicId, 1, kv, rc::Key(uint64_t{99})), kOk) << "a keyless typed topic has no key rule";
}

TEST(CheckedWritesBrokerRegression, TransactionOnTypedTopicCommitsRegression) {
    OneBroker b("cw-txn", OneBroker::Runtime::Shard);
    rc::Client& admin = *b.connectClient();
    const auto t = cwb::defineTyped(admin, "p", 1);
    ASSERT_EQ(t.errorCode, 0) << t.detail;
    rc::ClientConfig cfg = b.clientConfig();
    cfg.topics.push_back(rc::TopicSpec{"p", t.topicId, 1});
    rc::Client client(cfg);
    rc::ProducerConfig pcfg;
    pcfg.acks = rc::Acks::All;
    pcfg.lingerUs = 1;
    pcfg.transactionalId = "cw-txn";
    rc::Producer tp = client.producer(pcfg);
    client.connectAndWait(30s);
    tp.initTransactionsAndWait(30s);
    const rc::TopicHandle th = client.topic("p");
    tp.beginTransaction();
    for (uint64_t id = 1; id <= 3; ++id) {
        const auto r = cw::validRecord(id);
        tp.send(th, keyOf(r.key), std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(r.value.data()), r.value.size()));
    }
    tp.commitTransactionAndWait(30s);
    const auto values = storedValues(b.node(), t.topicId, 0);
    EXPECT_EQ(values.size(), 3u) << "three valid records; the COMMIT marker is a control batch and is not validated";
    tp.close();
    client.close();
}

TEST(CheckedWritesBrokerRegression, Utf8RulesEndToEndRegression) {
    OneBroker b("cw-utf8", OneBroker::Runtime::Shard);
    const auto t = cwb::defineTyped(*b.connectClient(), "p", 1);
    ASSERT_EQ(t.errorCode, 0) << t.detail;
    cw::Parts emoji;  emoji.bio = std::string("\xF0\x9F\x98\x80", 4);
    cw::Parts surr;   surr.bio = std::string("\xED\xA0\x80", 3);
    cw::Parts dirty;  dirty.name = std::string("a\0b", 3);
    cw::Parts bytes;  bytes.blob = std::string("\xFF\xFE", 2);
    EXPECT_EQ(send(b, "p", t.topicId, 1, cw::encode(emoji)), kOk);
    EXPECT_EQ(send(b, "p", t.topicId, 1, cw::encode(surr)), kSchemaValidationFailed);
    EXPECT_EQ(send(b, "p", t.topicId, 1, cw::encode(dirty)), kSchemaValidationFailed);
    EXPECT_EQ(send(b, "p", t.topicId, 1, cw::encode(bytes)), kOk);
}

TEST(CheckedWritesBrokerRegression, TypedTombstoneDeletesAndCompactsRegression) {
    OneBroker b("cw-tomb", OneBroker::Runtime::Shard);
    rc::Client& c = *b.connectClient();
    rc::TopicOptions to;
    to.config.cleanupPolicy = 1;   // compact
    const auto t = c.defineTopicAndWait("p", 1, "id", rune::codec::serialize(cw::matrixDescriptor()), rc::Persistence::Disk, to, {}, 60s);
    ASSERT_EQ(t.errorCode, 0) << t.detail;
    EXPECT_EQ(send(b, "p", t.topicId, 1, cw::validRecord(5)), kOk);
    EXPECT_EQ(cwb::produceRaw(b, "p", t.topicId, 1, "", rc::Key(uint64_t{5})), kOk) << "a tombstone with the key field's width deletes";
    EXPECT_EQ(cwb::produceRaw(b, "p", t.topicId, 1, "", rc::Key(uint32_t{5})), kSchemaValidationFailed) << "a 4-byte key tombstone is refused";
    const auto values = storedValues(b.node(), t.topicId, 0);
    ASSERT_EQ(values.size(), 2u);
    EXPECT_TRUE(values.back().empty()) << "the tombstone is the latest record of key 5";
}

TEST(CheckedWritesBrokerRegression, ViewOnUntypedOrKeylessSourceIsRefusedRegression) {
    OneBroker b("cw-view", OneBroker::Runtime::Shard);
    rc::Client& c = *b.connectClient();
    ASSERT_EQ(c.defineTopicAndWait("raw", 1, "", {}, rc::Persistence::Disk, {}, {}, 30s).errorCode, 0);
    ASSERT_EQ(c.defineTopicAndWait("keyless", 1, "", OneBroker::idDescriptor("keyless"), rc::Persistence::Disk, {}, {}, 30s).errorCode, 0);
    auto over = c.createViewAndWait("v1", "id", 0, b.viewPlanOver("raw"), {}, 30s);
    EXPECT_EQ(over.errorCode, kViewSourceNotFound);
    EXPECT_EQ(over.detail, "view source must be a typed topic");
    over = c.createViewAndWait("v2", "id", 0, b.viewPlanOver("keyless"), {}, 30s);
    EXPECT_EQ(over.errorCode, kViewKeyInvalid);
    EXPECT_EQ(over.detail, "view source needs a key");
}

TEST(CheckedWritesBrokerRegression, TypedOpenOfUntypedTopicIsRefusedRegression) {
    OneBroker b("cw-open", OneBroker::Runtime::Shard);
    rc::Client& c = *b.connectClient();
    ASSERT_EQ(c.defineTopicAndWait("raw", 1, "", {}, rc::Persistence::Disk, {}, {}, 30s).errorCode, 0);
    try {
        (void)c.openTopicAndWait<StubTyped>("raw", 20s);
        FAIL() << "a typed open of an untyped topic must throw";
    } catch (const std::runtime_error& e) {
        EXPECT_NE(std::string(e.what()).find("is an untyped (raw) topic"), std::string::npos) << e.what();
    }
}

TEST(CheckedWritesBrokerRegression, RawOpenOfTypedTopicIsRefusedRegression) {
    OneBroker b("cw-rawopen", OneBroker::Runtime::Shard);
    rc::Client& c = *b.connectClient();
    ASSERT_EQ(cwb::defineTyped(c, "p", 1).errorCode, 0);
    try {
        (void)c.rawTopicAndWait("p", 20s);
        FAIL() << "a raw open of a typed topic must throw";
    } catch (const std::runtime_error& e) {
        EXPECT_NE(std::string(e.what()).find("is a typed topic"), std::string::npos) << e.what();
    }
    EXPECT_THROW((void)c.rawTopicAndWait("missing", 20s), std::runtime_error);
}

TEST(CheckedWritesBrokerRegression, QlTopicWithoutPartitionsHasOneRegression) {
    OneBroker b("cw-ql");   // the CLI speaks the legacy gateway stream
    std::string err;
    ASSERT_EQ(cli({"ql", "-e", "topic solo;", "--broker", b.endpoint()}, &err), 0) << err;
    EXPECT_EQ(b.connectClient()->rawTopicAndWait("solo", 20s).partitionCount, 1);
}

TEST(CheckedWritesBrokerRegression, WideKeyFieldIsRefusedAtDefineRegression) {
    OneBroker b("cw-wide");
    std::string err;
    EXPECT_NE(cli({"ql", "-e", "topic w { k string(17) } key k;", "--broker", b.endpoint()}, &err), 0);
    EXPECT_NE(err.find("key field 'k' is string(17)"), std::string::npos) << err;
}

TEST(CheckedWritesBrokerRegression, LegacyRuntimeValidatesRegression) {
    OneBroker b("cw-legacy", OneBroker::Runtime::Legacy);
    const auto t = cwb::defineTyped(*b.connectClient(), "p", 1);
    ASSERT_EQ(t.errorCode, 0) << t.detail;
    cw::Parts bad; bad.bio = std::string("\xC0\xAF", 2);
    EXPECT_EQ(send(b, "p", t.topicId, 1, cw::encode(bad)), kSchemaValidationFailed);
    EXPECT_EQ(send(b, "p", t.topicId, 1, cw::validRecord(1)), kOk);
}

class CheckedWritesIngressRoutes : public ::testing::TestWithParam<bool> {};

TEST_P(CheckedWritesIngressRoutes, BothIngressRoutesValidateRegression) {
    const bool perWorker = GetParam();
    OneBroker b(perWorker ? "cw-ingress-w" : "cw-ingress-g", OneBroker::Runtime::Shard,
                [perWorker](NodeConfig& cfg) { cfg.perWorkerIngressEnabled = perWorker; });
    const auto t = cwb::defineTyped(*b.connectClient(), "p", 1);
    ASSERT_EQ(t.errorCode, 0) << t.detail;
    cw::Parts bad; bad.bio = std::string("\xC0\xAF", 2);
    EXPECT_EQ(send(b, "p", t.topicId, 1, cw::encode(bad)), kSchemaValidationFailed);
    EXPECT_EQ(send(b, "p", t.topicId, 1, cw::validRecord(1)), kOk);
}
INSTANTIATE_TEST_SUITE_P(CheckedWritesBrokerRegression, CheckedWritesIngressRoutes, ::testing::Values(false, true),
                         [](const ::testing::TestParamInfo<bool>& i) { return i.param ? "PerWorkerIngress" : "GatewayRouted"; });

TEST(CheckedWritesBrokerRegression, RestartValidatesFromTheFirstProduceRegression) {
    OneBroker b("cw-restart", OneBroker::Runtime::Shard);
    const auto t = cwb::defineTyped(*b.connectClient(), "p", 1);
    ASSERT_EQ(t.errorCode, 0) << t.detail;
    b.restart();
    cw::Parts bad; bad.bio = std::string("\xC0\xAF", 2);
    // The first produce after the restart: refused, or (while the topic is rebuilt) retriable - never accepted.
    uint16_t code = kNotLeader;
    for (int i = 0; i < 100 && code == kNotLeader; ++i) {
        code = send(b, "p", t.topicId, 1, cw::encode(bad));
        if (code == kNotLeader) std::this_thread::sleep_for(50ms);
    }
    EXPECT_EQ(code, kSchemaValidationFailed);
    EXPECT_TRUE(storedValues(b.node(), t.topicId, 0).empty());
}

TEST(CheckedWritesBrokerRegression, AdoptionStartsValidationRegression) {
    OneBroker b("cw-adopt", OneBroker::Runtime::Shard);
    rc::Client& c = *b.connectClient();
    const auto a = c.defineTopicAndWait("a", 1, "", {}, rc::Persistence::Disk, {}, {}, 30s);
    ASSERT_EQ(a.errorCode, 0) << a.detail;
    const std::string garbage("\x01\x02\x03", 3);
    EXPECT_EQ(cwb::produceRaw(b, "a", a.topicId, 1, garbage, rc::Key(uint64_t{1})), kOk);
    const auto adopted = c.defineTopicAndWait("a", 1, "id", rune::codec::serialize(cw::matrixDescriptor()), rc::Persistence::Disk, {}, {}, 30s);
    ASSERT_EQ(adopted.errorCode, 0) << adopted.detail;
    for (int i = 0; i < 400 && !cwb::hasWriteState(b.node(), a.topicId, 0); ++i) std::this_thread::sleep_for(25ms);
    ASSERT_TRUE(cwb::hasWriteState(b.node(), a.topicId, 0)) << "the owning worker holds the adopted topic's write state";
    EXPECT_EQ(cwb::produceRaw(b, "a", a.topicId, 1, garbage, rc::Key(uint64_t{1})), kSchemaValidationFailed);
}

TEST(CheckedWritesBrokerRegression, AlterThenProduceNewVersionSucceedsRegression) {
    OneBroker b("cw-alter", OneBroker::Runtime::Shard);
    rc::Client& c = *b.connectClient();
    const auto t = cwb::defineTyped(c, "p", 1);
    ASSERT_EQ(t.errorCode, 0) << t.detail;
    const auto altered = c.alterTopicAndWait("p", 0, OneBroker::addColumnOps("extra"), {}, 30s);
    ASSERT_EQ(altered.errorCode, 0) << altered.detail;
    // A v2 record: the matrix block plus the appended int64 column (blockLength 36), then the two var fields.
    cw::Parts p;
    p.version = 2;
    auto rec = cw::encode(p);
    std::string v2 = le(36, 2) + le(2, 2) + rec.value.substr(4, 28) + le(0, 8) + rec.value.substr(32);
    rec.value = v2;
    uint16_t code = kNotLeader;
    for (int i = 0; i < 200 && code == kNotLeader; ++i) {
        code = cwb::produceRaw(b, "p", t.topicId, 1, rec.value, keyOf(rec.key));
        if (code == kNotLeader) std::this_thread::sleep_for(25ms);
    }
    EXPECT_EQ(code, kOk);
    EXPECT_LE(b.node().onWorkerThreadForTest(0, [](Worker& w) { return w.typedVersionAheadForTest(); }), 10u);
}

TEST(CheckedWritesBrokerRegression, ThreeNodesNewLeaderValidatesRegression) {
    using namespace rune::node::test::catalog_cluster;
    Cluster c = startCluster("cw_failover");
    ASSERT_TRUE(c.awaitCatalogLeader(30s));
    const int32_t ctl = awaitController(c);
    ASSERT_GT(ctl, 0);
    auto admin = connectClient(c, quickConfig(c, ctl));
    rc::TopicOptions to;
    to.replicationFactor = 3;
    const auto t = admin->defineTopicAndWait("p", 1, "id", rune::codec::serialize(cw::matrixDescriptor()), rc::Persistence::Disk, to, {}, 60s);
    ASSERT_EQ(t.errorCode, 0) << t.detail;
    std::vector<int32_t> leaders;
    ASSERT_TRUE(awaitLeaders(c, t.topicId, 1, leaders, 30s));
    const int32_t old = leaders[0];
    int seed = 0;
    for (int id : {1, 2, 3}) if (id != old && id != ctl) { seed = id; break; }
    if (seed == 0) for (int id : {1, 2, 3}) if (id != old) { seed = id; break; }
    admin->close();
    admin.reset();
    c.stop(old);
    for (int i = 0; i < 400; ++i) {   // a new leader serves the partition
        leaders.clear();
        if (awaitLeaders(c, t.topicId, 1, leaders, 100ms) && leaders[0] != old) break;
    }
    ASSERT_NE(leaders.empty() ? old : leaders[0], old) << "a new leader was elected";
    cw::Parts bad; bad.bio = std::string("\xC0\xAF", 2);
    const auto produce = [&](const cw::TypedRecord& r) {
        uint16_t code = kNotLeader;
        for (int i = 0; i < 200 && code == kNotLeader; ++i) {
            code = cwb::produceRaw(quickConfig(c, seed), "p", t.topicId, 1, r.value, keyOf(r.key));
            if (code == kNotLeader) std::this_thread::sleep_for(50ms);
        }
        return code;
    };
    EXPECT_EQ(produce(cw::encode(bad)), kSchemaValidationFailed);
    EXPECT_EQ(produce(cw::validRecord(1)), kOk);
}

// A typed TRANSIENT topic validates like a disk one: its partitions append to an InMemoryLog through the same
// produceCore_, and their RcSlot carries the write state.  (TransientTopicDataPathTest now runs on an untyped topic.)
TEST(CheckedWritesBrokerRegression, TransientTypedTopicValidatesRegression) {
    OneBroker b("cw-transient", OneBroker::Runtime::Shard);
    const auto t = b.connectClient()->defineTopicAndWait("p", 1, "id", rune::codec::serialize(cw::matrixDescriptor()),
                                                        rc::Persistence::Transient, {}, {}, 60s);
    ASSERT_EQ(t.errorCode, 0) << t.detail;
    cw::Parts bad; bad.bio = std::string("\xC0\xAF", 2);
    EXPECT_EQ(send(b, "p", t.topicId, 1, cw::encode(bad)), kSchemaValidationFailed);
    EXPECT_EQ(send(b, "p", t.topicId, 1, cw::validRecord(1)), kOk);
    EXPECT_EQ(cwb::produceRaw(b, "p", t.topicId, 1, "", rc::Key(uint32_t{1})), kSchemaValidationFailed) << "a wrong-width tombstone";
}

// Boot: a restarted broker owns its catalog's typed partitions before any spec reaches a worker, so leadership of a typed
// topic must wait for the topic's validators and write spec (held here forever by the test knob).  While they are held,
// an invalid record is answered NotLeader (retriable: the producer keeps retrying, so no answer arrives) and nothing is
// stored - never OK.  RED (before the boot pre-post and the BecomeLeader hold): the restarted leader serves the partition
// without a write state, answers OK and stores the record.
TEST(CheckedWritesBrokerRegression, RestartServesNoTypedPartitionBeforeItsSpecRegression) {
    uint32_t hold = 0;
    OneBroker b("cw-bootgate", OneBroker::Runtime::Shard, [&hold](NodeConfig& cfg) { cfg.testHoldWriteSpecPasses = hold; });
    const auto t = cwb::defineTyped(*b.connectClient(), "p", 1);
    ASSERT_EQ(t.errorCode, 0) << t.detail;
    hold = 4'000'000'000u;   // the restarted broker never delivers a WriteSpecUpdate
    b.restart();
    cw::Parts bad; bad.bio = std::string("\xC0\xAF", 2);
    const auto rec = cw::encode(bad);
    for (int i = 0; i < 3; ++i) {   // 3 x 4 s spans the 2 s leader-election warm-up of the restart
        const uint16_t code = cwb::produceRaw(b, "p", t.topicId, 1, rec.value, keyOf(rec.key), 4s);
        EXPECT_TRUE(code == kNotLeader || code == kSchemaValidationFailed || code == cwb::kNotOk)
            << "attempt " << i << " answered " << code;
    }
    EXPECT_TRUE(storedValues(b.node(), t.topicId, 0).empty()) << "an invalid record was stored before the spec arrived";
}

// ===========================================================================================================
// Part 03: View acks end to end on real brokers on the default (shard) runtime.
// ===========================================================================================================
namespace {

using Clock = std::chrono::steady_clock;

/// The {id, hp} record of OneBroker::idDescriptor / charBlob (block 16): key = id as 8 little-endian bytes.
std::string idValue(uint64_t id) { return le(16, 2) + le(1, 2) + le(id, 8) + le(id * 10, 8); }

/// One client + producer over one topic; every send carries its own token and is awaited by it.
struct Sender {
    std::unique_ptr<rc::Client>      owned;
    rc::Client*                      client = nullptr;
    std::optional<rc::Producer>      prod;
    std::unique_ptr<rc::Completions> cq;
    std::optional<rc::TopicHandle>   topic;
    uint64_t                         next = 1;

    Sender(rc::ClientConfig cfg, const std::string& name, int32_t topicId, int32_t parts, rc::Acks acks, const std::string& txnId = {}) {
        cfg.topics.push_back(rc::TopicSpec{name, topicId, parts});
        owned  = std::make_unique<rc::Client>(cfg);
        client = owned.get();
        rc::ProducerConfig pc;
        pc.acks            = acks;
        pc.lingerUs        = 1;
        pc.produceTimeoutMs = cfg.produceTimeoutMs;
        pc.transactionalId = txnId;
        prod.emplace(client->producer(pc));
        client->connectAndWait(30s);
        cq = std::make_unique<rc::Completions>(*client);
        topic.emplace(client->topic(name));
    }
    /// A producer on a client that is already connected (the broker fixture's own).
    Sender(rc::Client& shared, int32_t topicId, rc::Acks acks) : client(&shared) {
        rc::ProducerConfig pc;
        pc.acks     = acks;
        pc.lingerUs = 1;
        prod.emplace(client->producer(pc));
        cq = std::make_unique<rc::Completions>(*client);
        topic.emplace(rc::TopicHandle{topicId, 1});
    }
    ~Sender() {
        if (prod) prod->closeImmediately();
        if (owned) owned->close();
    }

    /// The producer holds the id its coordinator assigned: a crash with that request still in flight waits out the client's 5 s coordinator-down fallback.
    void awaitProducerId() {
        for (int i = 0; i < 10'000 && prod->producerId() < 0; ++i) std::this_thread::sleep_for(1ms);
        EXPECT_GE(prod->producerId(), 0) << "the producer id was never assigned";
    }
    /// The token of the queued send of record `id`.
    uint64_t send(uint64_t id) { return sendValue(id, idValue(id)); }
    uint64_t sendValue(uint64_t id, const std::string& v) {
        const uint64_t tok  = next++;
        rc::SendResult r = rc::SendResult::BackPressured;
        while (r == rc::SendResult::BackPressured)
            r = prod->trySend(*topic, rc::Key(id), std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(v.data()), v.size()), *cq, tok);
        EXPECT_EQ(r, rc::SendResult::Ok);
        prod->flush();
        return tok;
    }
    /// The completion of `tok` within `within`; nullopt when none arrived.
    std::optional<rc::Completion> await(uint64_t tok, std::chrono::nanoseconds within) {
        std::optional<rc::Completion> out;
        (void)cq->waitFor(tok, within, [&](const rc::Completion& d, std::span<const uint8_t>) { out = d; });
        return out;
    }
};

bool isOk(const std::optional<rc::Completion>& c) { return c && c->status == rc::OpStatus::Ok; }

void awaitLevel(BrokerNode& n) {
    bool raised = false;
    for (int i = 0; i < 4000 && !raised; ++i) {
        raised = n.onWorkerThreadForTest(0, [](Worker& w) { return w.clusterFeatureLevelForTest(); }) >= rune::replication::MetadataFeature::kCheckedWrites;
        if (!raised) std::this_thread::sleep_for(5ms);
    }
    ASSERT_TRUE(raised) << "the cluster feature level reaches kCheckedWrites";
}

/// A typed {id, hp} topic `acct` with the view `acct_v` over it, on a broker whose level allows View acks.
struct AcctFixture {
    int32_t topicId     = -1;
    int32_t viewTopicId = -1;
};
/// The {id, hp} Replace view over `srcTopicId` (OneBroker::viewPlanOver needs a node to look the id up).
std::vector<uint8_t> acctViewPlan(int32_t srcTopicId) {
    namespace rv = rune::view;
    rv::ViewPlan p;
    p.viewName = "acct_v";
    p.key      = "id";
    p.sources.push_back(rv::SourceSpec{srcTopicId, "id"});
    p.output.push_back(rv::AuthoredField{"id", rune::codec::PrimType::Int64, 0, "id", rv::MergeRule::Replace});
    p.output.push_back(rv::AuthoredField{"hp", rune::codec::PrimType::Int64, 0, "hp", rv::MergeRule::Replace});
    return rv::serializeAuthored(p);
}

AcctFixture defineAcct(OneBroker& b, bool withView = true) {
    AcctFixture f;
    rc::Client& c = *b.connectClient();
    const auto t  = c.defineTopicAndWait("acct", 1, "id", OneBroker::idDescriptor("acct"), rc::Persistence::Disk, {}, {}, 60s);
    EXPECT_EQ(t.errorCode, 0) << t.detail;
    f.topicId = t.topicId;
    if (withView) {
        const auto v = c.createViewAndWait("acct_v", "id", 0, b.viewPlanOver("acct"), {}, 60s);
        EXPECT_EQ(v.errorCode, 0) << v.detail;
        f.viewTopicId = v.viewTopicId;
    }
    awaitLevel(b.node());
    return f;
}

constexpr rc::Acks kAllView    = rc::Acks::All | rc::Acks::View;
constexpr rc::Acks kLeaderView = rc::Acks::Leader | rc::Acks::View;

/// A View send parked behind an open transaction completes only after the commit.
void expectViewSendWaitsForTheTransaction(const char* tag, rc::Acks viewAcks) {
    OneBroker b(tag, OneBroker::Runtime::Shard);
    const AcctFixture f = defineAcct(b);
    Sender tx(b.clientConfig(), "acct", f.topicId, 1, rc::Acks::All, "tx-cw");
    tx.prod->initTransactionsAndWait(30s);
    tx.prod->beginTransaction();
    (void)tx.send(1);
    std::this_thread::sleep_for(300ms);

    Sender plain(b.clientConfig(), "acct", f.topicId, 1, rc::Acks::All);
    EXPECT_TRUE(isOk(plain.await(plain.send(2), 2s))) << "a plain All send does not wait for the open transaction";

    Sender view(b.clientConfig(), "acct", f.topicId, 1, viewAcks);
    const uint64_t tok = view.send(3);
    EXPECT_FALSE(view.await(tok, 1s).has_value()) << "the view has not folded past the open transaction";

    tx.prod->commitTransactionAndWait(30s);
    EXPECT_TRUE(isOk(view.await(tok, 5s)));

    // Read-your-writes is the point of View: the first read the broker serves after the ack sees the row.  Only a read
    // that never reached the leader (client-side kNotLeader) is retried; NotReady or AbsentKey fail.
    rc::ViewRowResult row;
    for (int i = 0; i < 20; ++i) {
        row = view.client->getViewRowAndWait(f.viewTopicId, 3, 0, 10s);
        if (row.status != rc::ViewRowResult::kNotLeader) break;
        std::this_thread::sleep_for(50ms);
    }
    EXPECT_EQ(row.status, rc::ViewRowResult::kOk) << "the acked record is already in the view";
    EXPECT_FALSE(row.rowBytes.empty());
}

}  // namespace

TEST(CheckedWritesBrokerRegression, AllViewAckWaitsBehindAnOpenTransactionShardRegression) {
    expectViewSendWaitsForTheTransaction("cw-viewtxn", kAllView);
}

TEST(CheckedWritesBrokerRegression, LeaderViewBehavesLikeAllViewShardRegression) {
    expectViewSendWaitsForTheTransaction("cw-lviewtxn", kLeaderView);
}

TEST(CheckedWritesBrokerRegression, ViewOnAPartitionWithoutViewsAcksAtCommitShardRegression) {
    OneBroker b("cw-noview", OneBroker::Runtime::Shard);
    const AcctFixture f = defineAcct(b, /*withView=*/false);

    // No view folds this partition, so a View send does not wait for one: it acks at commit even while an open
    // transaction holds back the fold point a view would wait for.
    Sender tx(b.clientConfig(), "acct", f.topicId, 1, rc::Acks::All, "tx-cw");
    tx.prod->initTransactionsAndWait(30s);
    tx.prod->beginTransaction();
    ASSERT_TRUE(isOk(tx.await(tx.send(1), 10s))) << "premise: the open transaction's record is in the log";
    Sender parked(b.clientConfig(), "acct", f.topicId, 1, kAllView);
    EXPECT_TRUE(isOk(parked.await(parked.send(2), 10s))) << "the View send acks before the open transaction commits";
    tx.prod->commitTransactionAndWait(30s);

#if defined(__has_feature)
#if __has_feature(thread_sanitizer) || __has_feature(address_sanitizer)
#define RUNE_CW_SANITIZED 1
#endif
#endif
#if defined(__SANITIZE_THREAD__) || defined(__SANITIZE_ADDRESS__)
#define RUNE_CW_SANITIZED 1
#endif
#ifndef RUNE_CW_SANITIZED
    // The latency ratio is a wall-clock bound: under a sanitizer the time is the sanitizer's (TSan box, -j8: All|View
    // p50 2.07 x All p50), so only the deterministic open-transaction check above runs there.
    const auto p50 = [&](rc::Acks acks, uint64_t firstId) {
        Sender s(b.clientConfig(), "acct", f.topicId, 1, acks);
        std::vector<std::chrono::nanoseconds> lat;
        for (uint64_t i = 0; i < 100; ++i) {
            const auto t0 = Clock::now();
            EXPECT_TRUE(isOk(s.await(s.send(firstId + i), 10s)));
            lat.push_back(Clock::now() - t0);
        }
        std::sort(lat.begin(), lat.end());
        return lat[lat.size() / 2];
    };
    const auto plain = p50(rc::Acks::All, 1000);
    const auto view  = p50(kAllView, 2000);
    EXPECT_LE(view, 2 * plain) << "no view folds this partition, so View adds no wait (All p50 " << plain.count() / 1000 << " us, All|View p50 "
                               << view.count() / 1000 << " us)";
#endif
}

TEST(CheckedWritesBrokerRegression, LegacyRuntimeRefusesTheViewBitRegression) {
    OneBroker b("cw-legacyview", OneBroker::Runtime::Legacy);
    const AcctFixture f = defineAcct(b, /*withView=*/false);
    Sender view(b.clientConfig(), "acct", f.topicId, 1, kAllView);
    const auto refused = view.await(view.send(1), 10s);
    ASSERT_TRUE(refused.has_value());
    EXPECT_EQ(refused->status, rc::OpStatus::BrokerError);
    EXPECT_EQ(refused->errorCode, static_cast<uint16_t>(rune::protocol::client::ErrorCode::InvalidRequest));
    Sender plain(b.clientConfig(), "acct", f.topicId, 1, rc::Acks::All);
    EXPECT_TRUE(isOk(plain.await(plain.send(2), 10s)));
}

TEST(CheckedWritesBrokerRegression, V12ClientAgainstV7BrokerKeepsEveryOldAckBehaviourShardRegression) {
    OneBroker b("cw-oldacks", OneBroker::Runtime::Shard);
    const AcctFixture f = defineAcct(b);
    uint64_t id = 1;
    for (const rc::Acks acks : {rc::Acks::Leader, rc::Acks::All}) {
        Sender s(b.clientConfig(), "acct", f.topicId, 1, acks);
        for (int i = 0; i < 100; ++i) EXPECT_TRUE(isOk(s.await(s.send(id++), 10s))) << "acks " << static_cast<int>(acks) << " record " << i;
    }
    Sender none(b.clientConfig(), "acct", f.topicId, 1, rc::Acks::None);   // stays open: closing it would drop sends still queued
    for (int i = 0; i < 100; ++i) (void)none.send(id++);
    std::size_t stored = 0;
    for (int i = 0; i < 400 && stored < 300; ++i) {
        stored = storedValues(b.node(), f.topicId, 0).size();
        if (stored < 300) std::this_thread::sleep_for(25ms);
    }
    EXPECT_EQ(stored, 300u) << "None, Leader and All records all landed";
}

TEST(CheckedWritesBrokerRegression, AllViewProduceAcrossALeaderCrashThreeNodesRegression) {
    using namespace rune::node::test::catalog_cluster;
    Cluster c = startCluster("cw03");
    ASSERT_TRUE(c.awaitCatalogLeader(30s));
    const int32_t ctl = awaitController(c);
    ASSERT_GT(ctl, 0);
    auto admin = connectClient(c, quickConfig(c, ctl));
    rc::TopicOptions to;
    to.replicationFactor = 3;
    const auto src = admin->defineTopicAndWait("acct", 1, "id", OneBroker::idDescriptor("acct"), rc::Persistence::Disk, to, {}, 60s);
    ASSERT_EQ(src.errorCode, 0) << src.detail;
    const auto view = admin->createViewAndWait("acct_v", "id", 0, acctViewPlan(src.topicId), {}, 60s);
    ASSERT_EQ(view.errorCode, 0) << view.detail;
    admin->close();
    admin.reset();
    std::vector<int32_t> leaders;
    ASSERT_TRUE(awaitLeaders(c, src.topicId, 1, leaders, 30s));
    const int32_t victim = leaders[0];
    int32_t seed = 0;
    for (int id : c.runningNodes()) if (id != victim) { seed = id; break; }
    ASSERT_GT(seed, 0);
    for (int id : c.runningNodes()) awaitLevel(c.node(id));

    rc::ClientConfig cfg = quickConfig(c, seed);
    cfg.connectTimeoutMs = 30'000;
    cfg.pingTimeoutMs    = 30'000;
    Sender s(cfg, "acct", src.topicId, 1, kAllView);
    s.awaitProducerId();
    constexpr int kTotal = 200;
    std::vector<uint64_t> tokens;
    std::thread killer;
    for (int i = 0; i < kTotal; ++i) {
        if (i == kTotal / 2) killer = std::thread([&c, victim] { c.crash(victim); });
        tokens.push_back(s.send(static_cast<uint64_t>(i)));
    }
    killer.join();
    int ok = 0;
    for (const uint64_t tok : tokens) {
        const auto d = s.await(tok, 60s);
        if (isOk(d)) ++ok;
        else ADD_FAILURE() << "token " << tok << (d ? " status " + std::to_string(static_cast<int>(d->status)) + " code " + std::to_string(d->errorCode) : std::string(" no completion"));
    }
    EXPECT_EQ(ok, kTotal);

    rc::ClientConfig rcfg = quickConfig(c, seed);
    rcfg.connectTimeoutMs = 30'000;
    rcfg.pingTimeoutMs    = 30'000;
    auto reader = connectClient(c, rcfg);
    for (const uint64_t id : {uint64_t{0}, uint64_t{kTotal / 2}, uint64_t{kTotal - 1}}) {
        rc::ViewRowResult row;
        for (const auto deadline = Clock::now() + 60s; Clock::now() < deadline;) {
            try { row = reader->getViewRowAndWait(view.viewTopicId, id, 0, 10s); } catch (const std::exception&) {}
            if (row.status == rc::ViewRowResult::kOk) break;
            std::this_thread::sleep_for(100ms);
        }
        EXPECT_EQ(row.status, rc::ViewRowResult::kOk) << "row " << id;
    }
    reader->close();
}

// ===========================================================================================================
// Part 04: a verdict source on a real broker.  TestReject (tag 9) is installed on the partition's worker.
// ===========================================================================================================
#include "support/CheckedWritesRig.h"

#include <rune/view/PlanCompiler.h>
#include <rune/view/Verdict.h>

namespace {

std::string verdictValue(uint64_t id, int64_t v, uint16_t tag) {
    std::string val;
    const auto le16 = [&](uint16_t x) { val.push_back(static_cast<char>(x)); val.push_back(static_cast<char>(x >> 8)); };
    le16(18); le16(1);
    for (int i = 0; i < 8; ++i) val.push_back(static_cast<char>(id >> (8 * i)));
    for (int i = 0; i < 8; ++i) val.push_back(static_cast<char>(static_cast<uint64_t>(v) >> (8 * i)));
    le16(tag);
    return val;
}

/// The leader's judge has logged the constraint set: records written from now on are judged.
void awaitSetLogged(BrokerNode& node, int32_t topicId) {
    const int32_t w = rune::metadata::ShardRouter(node.workerCountForTest()).workerForPartition(topicId, 0);
    bool logged = false;
    for (int i = 0; i < 12000 && !logged; ++i) {
        logged = node.onWorkerThreadForTest(w, [topicId](Worker& wk) {
            const PartitionWriteState* ws = wk.writeStateForTest(topicId, 0);
            return ws != nullptr && ws->constraintSets.latest().count != 0;
        });
        if (!logged) std::this_thread::sleep_for(5ms);
    }
    ASSERT_TRUE(logged) << "the TestReject set change was never logged";
}

/// A typed {id, v, tag} topic `vt` (one partition) with TagEquals{9} (object 7) installed on the worker that serves it.
int32_t defineVerdictSource(OneBroker& b, rc::Client* connected = nullptr) {
    rc::Client& c = connected != nullptr ? *connected : *b.connectClient();
    const auto t  = c.defineTopicAndWait("vt", 1, "id", rune::codec::serialize(cw::verdictSourceDescriptor()), rc::Persistence::Disk, {}, {}, 60s);
    EXPECT_EQ(t.errorCode, 0) << t.detail;
    awaitLevel(b.node());
    const int32_t w = rune::metadata::ShardRouter(b.node().workerCountForTest()).workerForPartition(t.topicId, 0);
    const int32_t src = t.topicId;
    b.node().onWorkerThreadForTest(w, [src](Worker& wk) {
        const auto desc = cw::verdictSourceDescriptor();
        rune::view::ViewPlan v;
        v.viewName = "vt_rej"; v.key = "id"; v.sources = {{src, "id"}};
        v.output = {{"id", rune::codec::PrimType::UInt64, 0, "id", rune::view::MergeRule::Replace}};
        v.persistence = 1;
        const rune::codec::Descriptor* srcs[] = {&desc};
        auto comp = rune::view::compileView(v, srcs, 0x40000701);
        EXPECT_TRUE(comp.has_value());
        TestRejectRule rule;
        rule.mode = TestRejectRule::Mode::TagEquals;
        rule.tag = 9;
        wk.viewSet()->installTestVerdictViewForTest(comp->plan, comp->rowLayout, desc, false, 7, rule);
        return 0;
    });
    awaitSetLogged(b.node(), src);   // the set change is logged before the records it is to judge
    return src;
}

}  // namespace

TEST(CheckedWritesBrokerRegression, VerdictAcksCarryThePerRecordResultShardRegression) {
    OneBroker b("cw-verdict", OneBroker::Runtime::Shard);
    const int32_t src = defineVerdictSource(b);
    Sender s(b.clientConfig(), "vt", src, 1, kAllView);
    std::vector<uint64_t> toks;
    for (uint64_t id = 1; id <= 8; ++id) {
        const std::string v = verdictValue(id, 1, id == 3 ? 9 : 1);
        rc::SendResult r = rc::SendResult::BackPressured;
        const uint64_t tok = s.next++;
        while (r == rc::SendResult::BackPressured)
            r = s.prod->trySend(*s.topic, rc::Key(id), std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(v.data()), v.size()), *s.cq, tok);
        ASSERT_EQ(r, rc::SendResult::Ok);
        toks.push_back(tok);
    }
    s.prod->flush();
    for (std::size_t i = 0; i < toks.size(); ++i) {
        const auto c = s.await(toks[i], 15s);
        ASSERT_TRUE(c) << "record " << i + 1;
        if (i == 2) {
            EXPECT_EQ(c->status, rc::OpStatus::BrokerError);
            EXPECT_EQ(c->errorCode, static_cast<uint16_t>(rune::protocol::client::ErrorCode::ConstraintViolation));
            EXPECT_EQ(rc::constraintIdOf(*c), rune::view::constraintIdOf(7, 1));
        } else {
            EXPECT_EQ(c->status, rc::OpStatus::Ok) << "record " << i + 1;
        }
    }
    const std::string v9 = verdictValue(100, 1, 1);
    rc::SendResult r = rc::SendResult::BackPressured;
    const uint64_t tok = s.next++;
    while (r == rc::SendResult::BackPressured)
        r = s.prod->trySend(*s.topic, rc::Key(uint64_t{100}), std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(v9.data()), v9.size()), *s.cq, tok);
    s.prod->flush();
    EXPECT_TRUE(isOk(s.await(tok, 15s))) << "the next batch is acked normally";
}

// #802: a claimBatch reports EVERY record's verdict (RecordsRejected + the rejection list), and the Raw subscriber receives
// exactly the accepted records.  (The Tier-2 leg of this test is added with the Tier-2 change.)
TEST(CheckedWritesBrokerRegression, ClaimAndTier2ReportEveryRecordVerdictShardRegression) {
    OneBroker b("cw-batchverdict", OneBroker::Runtime::Shard);
    rc::Client& admin = *b.connectClient();
    const int32_t src = defineVerdictSource(b, &admin);
    Sender s(admin, src, kAllView);
    const std::vector<uint16_t> tags = {1, 9, 1, 1, 9, 9};   // records 1, 4 and 5 are rejected (TagEquals 9)
    std::vector<std::string> vals;
    for (std::size_t i = 0; i < tags.size(); ++i) vals.push_back(verdictValue(1, 1, tags[i]));   // the claim has one key: id 1 throughout
    const uint32_t recLen = static_cast<uint32_t>(vals[0].size());

    std::mutex mu;
    std::set<int64_t> got;
    rc::SubscribeOptions o;
    o.from = rc::From::earliest();
    rc::Subscription sub = s.client->subscribe(*s.topic, 0, o, [&](const rc::Record& r) {
        std::lock_guard<std::mutex> g(mu);
        got.insert(r.offset);
    });

    // ---- the claim ----
    const uint64_t tok = s.next++;
    rc::BatchClaim claim;
    for (int i = 0; i < 100000; ++i) {
        claim = s.prod->claimBatch(*s.topic, rc::Key(uint64_t{1}), static_cast<uint32_t>(tags.size()), recLen, *s.cq, tok);
        if (claim.result() != rc::SendResult::BackPressured) break;
    }
    ASSERT_EQ(claim.result(), rc::SendResult::Ok);
    for (uint32_t i = 0; i < tags.size(); ++i) {
        const auto dst = claim.record(i);
        ASSERT_EQ(dst.size(), recLen);
        std::memcpy(dst.data(), vals[i].data(), recLen);
    }
    ASSERT_TRUE(claim.commit());
    s.prod->flush();
    std::optional<rc::Completion> c;
    std::vector<uint8_t> payload;
    (void)s.cq->waitFor(tok, 15s, [&](const rc::Completion& d, std::span<const uint8_t> p) { c = d; payload.assign(p.begin(), p.end()); });
    ASSERT_TRUE(c);
    ASSERT_EQ(c->status, rc::OpStatus::RecordsRejected);
    EXPECT_EQ(c->errorCode, static_cast<uint16_t>(rune::protocol::client::ErrorCode::ConstraintViolation));
    EXPECT_EQ(rc::constraintIdOf(*c), rune::view::constraintIdOf(7, 1));
    const rc::BatchVerdicts v(*c, payload);
    ASSERT_EQ(v.rejectedCount(), 3u);
    EXPECT_EQ(v.rejection(0).index, 1);
    EXPECT_EQ(v.rejection(1).index, 4);
    EXPECT_EQ(v.rejection(2).index, 5);
    const int64_t base = c->value;
    ASSERT_GE(base, 0);
    EXPECT_EQ(v.offsetOf(0), base);
    EXPECT_EQ(v.offsetOf(1), -1);
    EXPECT_EQ(v.offsetOf(3), base + 3);

    // ---- the Tier-2 batch: the same tags, one completion.  A Tier-2 batch is atomic (P11-6): one bad record rejects all of it ----
    const uint64_t tok2 = s.next++;
    rc::BatchBuilder bb = s.prod->batch(*s.topic, rc::Key(uint64_t{1}));
    const std::string recKey = std::string("\x01\0\0\0\0\0\0\0", 8);   // the typed record key: id 1, 8 bytes LE
    for (const auto& val : vals) bb.add(std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(val.data()), val.size()), recKey);
    rc::OpResult sr = rc::OpResult::BackPressured;
    for (int i = 0; i < 100000 && sr == rc::OpResult::BackPressured; ++i) sr = bb.send(*s.cq, tok2);
    ASSERT_EQ(sr, rc::OpResult::Accepted);
    s.prod->flush();
    std::optional<rc::Completion> c2;
    std::vector<uint8_t> payload2;
    (void)s.cq->waitFor(tok2, 15s, [&](const rc::Completion& d, std::span<const uint8_t> p) { c2 = d; payload2.assign(p.begin(), p.end()); });
    ASSERT_TRUE(c2);
    ASSERT_EQ(c2->op, rc::OpKind::Tier2Batch);
    ASSERT_EQ(c2->status, rc::OpStatus::BrokerError) << "err=" << c2->errorCode;
    EXPECT_EQ(c2->errorCode, static_cast<uint16_t>(rune::protocol::client::ErrorCode::ConstraintViolation));
    EXPECT_EQ(rc::constraintIdOf(*c2), rune::view::constraintIdOf(7, 1));
    EXPECT_GE(c2->value, 0) << "the batch's base offset";
    EXPECT_TRUE(payload2.empty());

    // ---- the Raw subscriber gets exactly the accepted records: a capped poll ending on a sentinel accepted send ----
    const std::string sentinelVal = verdictValue(100, 1, 1);
    const uint64_t stok = s.sendValue(100, sentinelVal);
    const auto sc = s.await(stok, 15s);
    ASSERT_TRUE(isOk(sc));
    const int64_t sentinelOff = sc->value;
    bool seen = false;
    for (int i = 0; i < 12000 && !seen; ++i) {
        {
            std::lock_guard<std::mutex> g(mu);
            seen = got.count(sentinelOff) != 0;
        }
        if (!seen) std::this_thread::sleep_for(5ms);
    }
    ASSERT_TRUE(seen) << "the sentinel never reached the subscriber";
    sub.unsubscribe();
    std::lock_guard<std::mutex> g(mu);
    EXPECT_EQ(got, (std::set<int64_t>{base + 0, base + 2, base + 3, sentinelOff})) << "nothing of the rejected atomic batch";
}

// A batch whose every record is rejected is skipped by the group fetch path too: the group consumer sees exactly the accepted
// batches around it, and its position moves past the rejected one.
TEST(CheckedWritesBrokerRegression, FullyRejectedBatchIsSkippedOnTheGroupPathShardRegression) {
    OneBroker b("cw-groupskip", OneBroker::Runtime::Shard);
    const int32_t src = defineVerdictSource(b);
    Sender s(b.clientConfig(), "vt", src, 1, kAllView);   // the group resolves "vt" through this client's topics
    const std::string recKey = std::string("\x01\0\0\0\0\0\0\0", 8);   // the typed record key: id 1
    const auto sendBatch = [&](uint16_t tag, int n) -> std::pair<rc::Completion, std::vector<uint8_t>> {
        const uint64_t tok = s.next++;
        rc::BatchBuilder bb = s.prod->batch(*s.topic, rc::Key(uint64_t{1}));
        for (int i = 0; i < n; ++i) {
            const std::string v = verdictValue(1, i, tag);
            bb.add(std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(v.data()), v.size()), recKey);
        }
        rc::OpResult sr = rc::OpResult::BackPressured;
        for (int i = 0; i < 100000 && sr == rc::OpResult::BackPressured; ++i) sr = bb.send(*s.cq, tok);
        EXPECT_EQ(sr, rc::OpResult::Accepted);
        s.prod->flush();
        std::optional<rc::Completion> c;
        std::vector<uint8_t> payload;
        (void)s.cq->waitFor(tok, 15s, [&](const rc::Completion& d, std::span<const uint8_t> p) { c = d; payload.assign(p.begin(), p.end()); });
        EXPECT_TRUE(c.has_value());
        return {c.value_or(rc::Completion{}), payload};
    };
    const auto [ca, pa] = sendBatch(1, 3);   // A: accepted
    const auto [cb, pb] = sendBatch(9, 3);   // B: every record rejected
    const auto [cc, pc] = sendBatch(1, 3);   // C: accepted
    ASSERT_EQ(ca.status, rc::OpStatus::Ok);
    ASSERT_EQ(cb.status, rc::OpStatus::BrokerError) << "B is fully rejected, answered once";
    ASSERT_TRUE(pb.empty());
    ASSERT_EQ(cc.status, rc::OpStatus::Ok);
    std::set<int64_t> want;
    for (int i = 0; i < 3; ++i) { want.insert(ca.value + i); want.insert(cc.value + i); }

    std::mutex mu;
    std::set<int64_t> got;
    rc::GroupConfig gcfg;
    gcfg.autoCommit = false;
    rc::GroupConsumer group = s.client->subscribeGroup("cw-g", {"vt"}, gcfg);
    group.onRecords([&](const rc::RecordSet& rs) {
        std::lock_guard<std::mutex> g(mu);
        rs.forEachRecord([&](std::string_view, std::string_view, int64_t off, int64_t) { got.insert(off); });
    });
    const int64_t lastC = cc.value + 2;
    bool arrived = false;
    for (int i = 0; i < 12000 && !arrived; ++i) {   // 5 ms each: a 60 s deadline
        {
            std::lock_guard<std::mutex> g(mu);
            arrived = got.count(lastC) != 0;
        }
        if (!arrived) std::this_thread::sleep_for(5ms);
    }
    ASSERT_TRUE(arrived) << "the group never delivered the last accepted record";
    std::lock_guard<std::mutex> g(mu);
    EXPECT_EQ(got, want) << "the group sees A and C, and not one record of the fully rejected B";
}

// ===========================================================================================================
// Part 04: verdict sources on three real brokers.  TestReject (tag 9, object 7) is installed on every node's worker.
// ===========================================================================================================
namespace {

/// The {id, v, tag} projection `vt_v` over the verdict source.
std::vector<uint8_t> vtViewPlan(int32_t srcTopicId) {
    namespace rv = rune::view;
    rv::ViewPlan p;
    p.viewName = "vt_v";
    p.key      = "id";
    p.sources.push_back(rv::SourceSpec{srcTopicId, "id"});
    p.output.push_back(rv::AuthoredField{"id", rune::codec::PrimType::UInt64, 0, "id", rv::MergeRule::Replace});
    p.output.push_back(rv::AuthoredField{"v", rune::codec::PrimType::Int64, 0, "v", rv::MergeRule::Replace});
    p.output.push_back(rv::AuthoredField{"tag", rune::codec::PrimType::UInt16, 0, "tag", rv::MergeRule::Replace});
    return rv::serializeAuthored(p);
}

/// TagEquals{9} (object 7) on the worker that serves the partition of `node`, once it serves it.
void installTagReject(BrokerNode& node, int32_t topicId) {
    const int32_t w = rune::metadata::ShardRouter(node.workerCountForTest()).workerForPartition(topicId, 0);
    bool serves = false;
    for (int i = 0; i < 1200 && !serves; ++i) {
        serves = node.onWorkerThreadForTest(w, [topicId](Worker& wk) {
            return wk.clusterFeatureLevelForTest() >= rune::replication::MetadataFeature::kCheckedWrites && wk.writeStateForTest(topicId, 0) != nullptr;
        });
        if (!serves) std::this_thread::sleep_for(50ms);
    }
    ASSERT_TRUE(serves) << "the node never served the source";
    node.onWorkerThreadForTest(w, [topicId](Worker& wk) {
        const auto desc = cw::verdictSourceDescriptor();
        rune::view::ViewPlan v;
        v.viewName = "vt_rej"; v.key = "id"; v.sources = {{topicId, "id"}};
        v.output = {{"id", rune::codec::PrimType::UInt64, 0, "id", rune::view::MergeRule::Replace}};
        v.persistence = 1;
        const rune::codec::Descriptor* srcs[] = {&desc};
        auto comp = rune::view::compileView(v, srcs, 0x40000701);
        EXPECT_TRUE(comp.has_value());
        TestRejectRule rule;
        rule.mode = TestRejectRule::Mode::TagEquals;
        rule.tag = 9;
        wk.viewSet()->installTestVerdictViewForTest(comp->plan, comp->rowLayout, desc, false, 7, rule);
        return 0;
    });
}

struct NodeVerdicts {
    std::set<int64_t> indexed, rejectedInLog;
    int64_t covered = 0, dataEnd = 0;
    uint64_t mismatches = 0, rejectionsConstraint = 0;
};
/// What node `node` holds: its index (worker thread) and the tag-9 records of its committed log (owner thread).
NodeVerdicts verdictsOf(BrokerNode& node, int32_t topicId) {
    NodeVerdicts out;
    const int32_t w = rune::metadata::ShardRouter(node.workerCountForTest()).workerForPartition(topicId, 0);
    node.onOwnerThreadForTest(topicId, 0, [&out](rune::replication::PartitionReplica& r) {
        const auto log = cw::readLogOf(*r.log(), r.highWatermark());
        for (const auto& d : log.data) {
            out.dataEnd = std::max(out.dataEnd, d.offset + 1);
            if (!d.del && d.tag == 9) out.rejectedInLog.insert(d.offset);
        }
        return 0;
    });
    node.onWorkerThreadForTest(w, [&out, topicId](Worker& wk) {
        if (const PartitionWriteState* ws = wk.writeStateForTest(topicId, 0)) {
            ws->rejected.forEachIn(0, INT64_MAX, [&](const rune::storage::RejectedOffsetIndex::Entry& e) { out.indexed.insert(e.offset); });
            out.covered = ws->rejected.coveredThrough();
            out.mismatches = ws->rejected.mismatches();
        }
        if (const VerdictSourceStats* st = wk.verdictStatsForTest(topicId, 0)) out.rejectionsConstraint = st->rejectionsConstraint;
        return 0;
    });
    return out;
}

/// The leader's index, once it covers the whole committed log.
NodeVerdicts awaitIndex(BrokerNode& node, int32_t topicId) {
    NodeVerdicts v;
    for (int i = 0; i < 1200; ++i) {
        v = verdictsOf(node, topicId);
        if (v.dataEnd != 0 && v.covered >= v.dataEnd) break;
        std::this_thread::sleep_for(50ms);
    }
    return v;
}

/// The view row of `id` as (v, tag); nullopt = absent.  Retries only what a moving leader or a building view answers.
std::optional<std::pair<int64_t, uint16_t>> vtRow(rc::Client& reader, int32_t viewTopicId, uint64_t id, bool* answered) {
    rc::ViewRowResult row;
    *answered = false;
    for (const auto deadline = Clock::now() + 60s; Clock::now() < deadline; std::this_thread::sleep_for(100ms)) {
        try { row = reader.getViewRowAndWait(viewTopicId, id, 0, 10s); } catch (const std::exception&) { continue; }
        if (row.status == rc::ViewRowResult::kOk || row.status == rc::ViewRowResult::kAbsentKey) { *answered = true; break; }
    }
    if (row.status != rc::ViewRowResult::kOk) return std::nullopt;
    int64_t v = 0;
    for (int i = 0; i < 8; ++i) v |= static_cast<int64_t>(row.rowBytes[9 + static_cast<std::size_t>(i)]) << (8 * i);   // [bitmap][id][v][tag]
    return std::make_pair(v, static_cast<uint16_t>(row.rowBytes[17] | (row.rowBytes[18] << 8)));
}

/// Offsets a subscription from 0 delivers: until `atLeast` arrived, plus 500 ms for a rejected record that would follow.
std::set<int64_t> deliveredFromZero(rc::ClientConfig cfg, int32_t topicId, std::size_t atLeast) {
    cfg.topics.push_back(rc::TopicSpec{"vt", topicId, 1});
    rc::Client client(cfg);
    client.connectAndWait(30s);
    std::mutex mu;
    std::set<int64_t> got;
    rc::SubscribeOptions o;
    o.from = rc::From::earliest();
    rc::Subscription sub = client.subscribe(client.topic("vt"), 0, o, [&](const rc::Record& r) {
        std::lock_guard<std::mutex> g(mu);
        got.insert(r.offset);
    });
    for (const auto deadline = Clock::now() + 60s; Clock::now() < deadline; std::this_thread::sleep_for(50ms)) {
        std::lock_guard<std::mutex> g(mu);
        if (got.size() >= atLeast) break;
    }
    std::this_thread::sleep_for(500ms);
    sub.unsubscribe();
    std::set<int64_t> out;
    {
        std::lock_guard<std::mutex> g(mu);
        out = got;
    }
    client.close();
    return out;
}

constexpr uint16_t kConstraintViolation = static_cast<uint16_t>(rune::protocol::client::ErrorCode::ConstraintViolation);

}  // namespace

namespace {
/// `vt_v` over the one-node verdict source.
int32_t createVtView(OneBroker& b, int32_t srcTopicId) {
    const auto view = b.connectClient()->createViewAndWait("vt_v", "id", 0, vtViewPlan(srcTopicId), {}, 60s);
    EXPECT_EQ(view.errorCode, 0) << view.detail;
    return view.viewTopicId;
}
}  // namespace

TEST(CheckedWritesBrokerRegression, CoalescedBatchWithOneBadRecordShardRegression) {
    OneBroker b("cw-verdict-coalesced", OneBroker::Runtime::Shard);
    const int32_t src = defineVerdictSource(b);
    const int32_t viewTopic = createVtView(b, src);
    ASSERT_FALSE(HasFatalFailure());
    Sender s(b.clientConfig(), "vt", src, 1, kAllView);
    const auto sendBatch = [&](uint64_t firstId, int bad) {
        std::vector<uint64_t> toks;
        for (uint64_t k = 0; k < 8; ++k) {
            const std::string v = verdictValue(firstId + k, 1, static_cast<int>(k) + 1 == bad ? 9 : 1);
            rc::SendResult r = rc::SendResult::BackPressured;
            const uint64_t tok = s.next++;
            while (r == rc::SendResult::BackPressured)
                r = s.prod->trySend(*s.topic, rc::Key(firstId + k), std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(v.data()), v.size()), *s.cq, tok);
            EXPECT_EQ(r, rc::SendResult::Ok);
            toks.push_back(tok);
        }
        s.prod->flush();
        return toks;
    };
    const auto first = sendBatch(1, 3);
    for (std::size_t i = 0; i < first.size(); ++i) {
        const auto c = s.await(first[i], 15s);
        ASSERT_TRUE(c) << "record " << i + 1;
        if (i == 2) {
            EXPECT_EQ(c->status, rc::OpStatus::BrokerError);
            EXPECT_EQ(c->errorCode, kConstraintViolation);
            EXPECT_EQ(rc::constraintIdOf(*c), rune::view::constraintIdOf(7, 1));
        } else {
            EXPECT_EQ(c->status, rc::OpStatus::Ok) << "record " << i + 1;
        }
    }
    for (const uint64_t tok : sendBatch(101, 0)) EXPECT_TRUE(isOk(s.await(tok, 15s))) << "the next batch is acked normally";
    auto reader = b.connectClient();
    bool answered = false;
    EXPECT_FALSE(vtRow(*reader, viewTopic, 3, &answered).has_value());
    EXPECT_TRUE(answered) << "id 3 is absent from the view";
    EXPECT_TRUE(vtRow(*reader, viewTopic, 4, &answered).has_value());
    EXPECT_EQ(deliveredFromZero(b.clientConfig(), src, 15).count(3), 0u) << "a consumer from 0 never sees the rejected record (id 3, after the set change at offset 0)";
}

TEST(CheckedWritesBrokerRegression, OldWritersOnAVerdictSourceNeverSeeRejectedRecordsShardRegression) {
    OneBroker b("cw-verdict-old", OneBroker::Runtime::Shard);
    const int32_t src = defineVerdictSource(b);
    const int32_t viewTopic = createVtView(b, src);
    ASSERT_FALSE(HasFatalFailure());
    // Every writer that does not ask for a verdict writes a tag-1 record (odd id) and a tag-9 record (even id).
    struct Writer { rc::Acks acks; int32_t announce; };
    uint64_t id = 1;
    std::vector<std::unique_ptr<Sender>> senders;
    for (const Writer w : {Writer{rc::Acks::Leader, 0}, Writer{rc::Acks::All, 0}, Writer{rc::Acks::None, 0}, Writer{rc::Acks::All, 11}}) {
        rc::ClientConfig cfg = b.clientConfig();
        cfg.testAnnounceClientVersion = w.announce;
        senders.push_back(std::make_unique<Sender>(cfg, "vt", src, 1, w.acks));
        Sender& s = *senders.back();
        for (const uint16_t tag : {uint16_t{1}, uint16_t{9}}) {
            const uint64_t tok = s.sendValue(id, verdictValue(id, 1, tag));
            if (w.acks != rc::Acks::None) EXPECT_TRUE(isOk(s.await(tok, 15s))) << "acks " << static_cast<int>(w.acks) << " announce " << w.announce << " id " << id << ": no verdict reaches a writer that did not ask";
            ++id;
        }
    }
    std::size_t stored = 0;
    for (int i = 0; i < 400 && stored < 8; ++i) {
        stored = storedValues(b.node(), src, 0).size();
        if (stored < 8) std::this_thread::sleep_for(25ms);
    }
    ASSERT_EQ(stored, 8u) << "every record was written";
    auto reader = b.connectClient();
    for (uint64_t k = 1; k < id; ++k) {
        bool answered = false;
        const auto row = vtRow(*reader, viewTopic, k, &answered);
        ASSERT_TRUE(answered) << "row " << k;
        EXPECT_EQ(row.has_value(), k % 2 == 1) << "id " << k << (k % 2 == 1 ? " was accepted" : " was rejected");
    }
    EXPECT_EQ(deliveredFromZero(b.clientConfig(), src, 4).size(), 4u) << "a consumer sees exactly the tag-1 records";
}

TEST(CheckedWritesBrokerRegression, RejectedRecordAcrossALeaderCrashThreeNodesRegression) {
    using namespace rune::node::test::catalog_cluster;
    Cluster c = startCluster("cw04");
    ASSERT_TRUE(c.awaitCatalogLeader(30s));
    const int32_t ctl = awaitController(c);
    ASSERT_GT(ctl, 0);
    auto admin = connectClient(c, quickConfig(c, ctl));
    rc::TopicOptions to;
    to.replicationFactor = 3;
    const auto src = admin->defineTopicAndWait("vt", 1, "id", rune::codec::serialize(cw::verdictSourceDescriptor()), rc::Persistence::Disk, to, {}, 60s);
    ASSERT_EQ(src.errorCode, 0) << src.detail;
    const auto view = admin->createViewAndWait("vt_v", "id", 0, vtViewPlan(src.topicId), {}, 60s);
    ASSERT_EQ(view.errorCode, 0) << view.detail;
    admin->close();
    admin.reset();
    std::vector<int32_t> leaders;
    ASSERT_TRUE(awaitLeaders(c, src.topicId, 1, leaders, 30s));
    const int32_t victim = leaders[0];
    for (const int id : c.runningNodes()) {
        installTagReject(c.node(id), src.topicId);
        ASSERT_FALSE(HasFatalFailure());
    }
    awaitSetLogged(c.node(victim), src.topicId);
    ASSERT_FALSE(HasFatalFailure());
    int32_t seed = 0;
    for (int id : c.runningNodes()) if (id != victim) { seed = id; break; }

    rc::ClientConfig cfg = quickConfig(c, seed);
    cfg.connectTimeoutMs = 30'000;
    cfg.pingTimeoutMs    = 30'000;
    Sender s(cfg, "vt", src.topicId, 1, kAllView);
    s.awaitProducerId();
    constexpr int kTotal = 200;
    const auto tagOf = [](int i) -> uint16_t { return i % 10 == 9 ? 9 : 1; };
    std::vector<uint64_t> tokens;
    std::thread killer;
    for (int i = 0; i < kTotal; ++i) {
        if (i == kTotal / 2) killer = std::thread([&c, victim] { c.crash(victim); });
        tokens.push_back(s.sendValue(static_cast<uint64_t>(i), verdictValue(static_cast<uint64_t>(i), i, tagOf(i))));
    }
    killer.join();
    for (int i = 0; i < kTotal; ++i) {
        const auto d = s.await(tokens[static_cast<std::size_t>(i)], 60s);
        ASSERT_TRUE(d.has_value()) << "record " << i << " never completed";
        if (tagOf(i) == 9) {
            EXPECT_EQ(d->status, rc::OpStatus::BrokerError) << "record " << i;
            EXPECT_EQ(d->errorCode, kConstraintViolation) << "record " << i;
            EXPECT_EQ(rc::constraintIdOf(*d), rune::view::constraintIdOf(7, 1)) << "record " << i;
        } else {
            EXPECT_EQ(d->status, rc::OpStatus::Ok) << "record " << i;
        }
    }

    // Every node that takes the lead holds the same verdicts: the victim comes back and the leader is crashed in turn.
    for (int round = 0; round < 2; ++round) {
        ASSERT_TRUE(awaitLeaders(c, src.topicId, 1, leaders, 60s));
        const int32_t leader = leaders[0];
        const NodeVerdicts v = awaitIndex(c.node(leader), src.topicId);
        EXPECT_FALSE(v.rejectedInLog.empty());
        EXPECT_EQ(v.indexed, v.rejectedInLog) << "round " << round << ": node " << leader << " leads with another set of rejected offsets";
        EXPECT_EQ(v.mismatches, 0u);
        if (round == 0) {
            c.startNode(victim);
            installTagReject(c.node(victim), src.topicId);
            ASSERT_FALSE(HasFatalFailure());
            c.crash(leader);
        }
    }

    // vt_v on the final leader is the accepted set: every id with tag 1 is a row, every tag-9 id is absent.
    ASSERT_TRUE(awaitLeaders(c, src.topicId, 1, leaders, 60s));
    rc::ClientConfig rcfg = quickConfig(c, leaders[0]);
    rcfg.connectTimeoutMs = 30'000;
    rcfg.pingTimeoutMs    = 30'000;
    auto reader = connectClient(c, rcfg);
    for (int i = 0; i < kTotal; ++i) {
        bool answered = false;
        const auto row = vtRow(*reader, view.viewTopicId, static_cast<uint64_t>(i), &answered);
        ASSERT_TRUE(answered) << "row " << i;
        if (tagOf(i) == 9) EXPECT_FALSE(row.has_value()) << "id " << i << " was rejected";
        else EXPECT_TRUE(row.has_value() && row->first == i && row->second == 1) << "id " << i;
    }
    reader->close();
}

TEST(CheckedWritesBrokerRegression, RestartKeepsVerdictsAndViewsThreeNodesRegression) {
    using namespace rune::node::test::catalog_cluster;
    Cluster c = startCluster("cw04r");
    ASSERT_TRUE(c.awaitCatalogLeader(30s));
    const int32_t ctl = awaitController(c);
    ASSERT_GT(ctl, 0);
    auto admin = connectClient(c, quickConfig(c, ctl));
    rc::TopicOptions to;
    to.replicationFactor = 3;
    const auto src = admin->defineTopicAndWait("vt", 1, "id", rune::codec::serialize(cw::verdictSourceDescriptor()), rc::Persistence::Disk, to, {}, 60s);
    ASSERT_EQ(src.errorCode, 0) << src.detail;
    const auto view = admin->createViewAndWait("vt_v", "id", 0, vtViewPlan(src.topicId), {}, 60s);
    ASSERT_EQ(view.errorCode, 0) << view.detail;
    admin->close();
    admin.reset();
    std::vector<int32_t> leaders;
    ASSERT_TRUE(awaitLeaders(c, src.topicId, 1, leaders, 30s));
    for (const int id : c.runningNodes()) {
        installTagReject(c.node(id), src.topicId);
        ASSERT_FALSE(HasFatalFailure());
    }
    awaitSetLogged(c.node(leaders[0]), src.topicId);
    ASSERT_FALSE(HasFatalFailure());

    constexpr int kTotal = 60;
    const auto tagOf = [](int i) -> uint16_t { return i % 10 == 9 ? 9 : 1; };
    {
        rc::ClientConfig cfg = quickConfig(c, 1);
        cfg.connectTimeoutMs = 30'000;
        cfg.pingTimeoutMs    = 30'000;
        Sender s(cfg, "vt", src.topicId, 1, kAllView);
        std::vector<uint64_t> tokens;
        for (int i = 0; i < kTotal; ++i) tokens.push_back(s.sendValue(static_cast<uint64_t>(i), verdictValue(static_cast<uint64_t>(i), i, tagOf(i))));
        for (int i = 0; i < kTotal; ++i) {
            const auto d = s.await(tokens[static_cast<std::size_t>(i)], 60s);
            ASSERT_TRUE(d.has_value()) << "record " << i;
            EXPECT_EQ(d->status == rc::OpStatus::Ok, tagOf(i) != 9) << "record " << i;
        }
    }
    ASSERT_TRUE(awaitLeaders(c, src.topicId, 1, leaders, 30s));
    const NodeVerdicts before = awaitIndex(c.node(leaders[0]), src.topicId);
    ASSERT_FALSE(before.rejectedInLog.empty());
    ASSERT_EQ(before.indexed, before.rejectedInLog);

    // Every node restarts; TestReject (a test-installed member) goes back on each.
    for (int id = 3; id >= 1; --id) c.stop(id);
    for (int id = 1; id <= 3; ++id) c.startNode(id);
    ASSERT_TRUE(c.awaitCatalogLeader(60s));
    ASSERT_TRUE(awaitLeaders(c, src.topicId, 1, leaders, 60s));
    // The leadership scan alone rebuilds the index from the committed markers, before TestReject is back.
    const NodeVerdicts scanned = awaitIndex(c.node(leaders[0]), src.topicId);
    EXPECT_EQ(scanned.indexed, before.rejectedInLog) << "the markers alone restore the index";
    EXPECT_GE(scanned.covered, scanned.dataEnd);
    for (const int id : c.runningNodes()) {
        installTagReject(c.node(id), src.topicId);
        ASSERT_FALSE(HasFatalFailure());
    }
    ASSERT_TRUE(awaitLeaders(c, src.topicId, 1, leaders, 60s));
    const NodeVerdicts after = awaitIndex(c.node(leaders[0]), src.topicId);
    EXPECT_EQ(after.indexed, before.rejectedInLog) << "the same offsets are rejected after the restart";
    EXPECT_EQ(after.mismatches, 0u);
    EXPECT_EQ(after.rejectionsConstraint, 0u) << "nothing was judged again: the verdicts came from the markers";

    rc::ClientConfig rcfg = quickConfig(c, leaders[0]);
    rcfg.connectTimeoutMs = 30'000;
    rcfg.pingTimeoutMs    = 30'000;
    const std::set<int64_t> delivered = deliveredFromZero(rcfg, src.topicId, static_cast<std::size_t>(kTotal) - before.rejectedInLog.size());
    for (const int64_t off : before.rejectedInLog) EXPECT_EQ(delivered.count(off), 0u) << "rejected offset " << off << " reappeared for a consumer";
    EXPECT_EQ(delivered.size(), static_cast<std::size_t>(kTotal) - before.rejectedInLog.size()) << "exactly the accepted records are delivered";

    auto reader = connectClient(c, rcfg);
    for (int i = 0; i < kTotal; ++i) {
        bool answered = false;
        const auto row = vtRow(*reader, view.viewTopicId, static_cast<uint64_t>(i), &answered);
        ASSERT_TRUE(answered) << "row " << i;
        if (tagOf(i) == 9) EXPECT_FALSE(row.has_value()) << "id " << i << " was rejected";
        else EXPECT_TRUE(row.has_value() && row->first == i && row->second == 1) << "id " << i;
    }
    reader->close();
}

// ===========================================================================================================
// Part 05: write intents and the key index on real brokers.
// ===========================================================================================================
namespace {

namespace pc = rune::protocol::client;

constexpr int32_t kKiParts = 4;
constexpr uint16_t kInvalidRequest = 39, kKeyExists = 56, kKeyNotFound = 57;

/// A bounded poll; no test synchronises on a sleep.
template <class Pred>
bool waitFor(Pred&& pred, std::chrono::seconds within) {
    for (const auto deadline = Clock::now() + within; Clock::now() < deadline; std::this_thread::sleep_for(5ms))
        if (pred()) return true;
    return pred();
}

rc::DefineTopicResult defineKi(rc::Client& c, const std::string& name, int16_t replicas = 0, bool keyIndex = true) {
    rc::TopicOptions to;
    to.replicationFactor    = replicas;
    to.config.cleanupPolicy = 1;   // compact
    to.keyFeatures.keyIndex = keyIndex;
    return c.defineTopicAndWait(name, kKiParts, "id", rune::codec::serialize(kiAccountDescriptor()), rc::Persistence::Disk, to, {}, 60s);
}

KiAccount acct(uint64_t id, std::string name = "alice", int64_t n = 0) { return KiAccount{id, std::move(name), n}; }

std::vector<uint8_t> encoded(const KiAccount& a) {
    std::vector<uint8_t> out(a.encodedSize());
    (void)a.encode(out.data());
    return out;
}

/// A producer with `acks` and the typed topic `name` written through it.
struct KiWriter {
    rc::Producer                                   prod;
    rc::TypedTopic<KiAccount, rc::ProducerRef>     topic;

    static rc::ProducerConfig config(rc::Acks acks) {
        rc::ProducerConfig cfg;
        cfg.acks     = acks;
        cfg.lingerUs = 1;
        return cfg;
    }
    KiWriter(rc::Client& c, const std::string& name, rc::Acks acks = kAllView)
        : prod(c.producer(config(acks))), topic(c.openTopicAndWait<KiAccount>(name, prod, 30s)) {}
    ~KiWriter() { prod.closeImmediately(); }
};

struct Seen {
    int32_t     partition;
    int64_t     offset;
    rc::RecordOp op;
    uint8_t     attributes;
    std::string key;
    bool        tombstone;
};

/// Subscriptions from offset 0; the handler runs on the client's thread.
class Collector {
public:
    // The handler may still run after unsubscribe() until finished() (Subscription.h), and it writes into this object.
    ~Collector() {
        for (rc::Subscription& s : subs_) s.unsubscribe();
        EXPECT_TRUE(waitFor([&] { return std::all_of(subs_.begin(), subs_.end(), [](const rc::Subscription& s) { return s.finished(); }); }, 10s))
            << "every subscription finished before the collector is destroyed";
    }
    void subscribe(rc::Client& c, rc::TopicHandle topic, int32_t partition) {
        rc::SubscribeOptions o;
        o.from = rc::From::earliest();
        subs_.push_back(c.subscribe(topic, partition, o, [this](const rc::Record& r) {
            std::lock_guard<std::mutex> g(mu_);
            seen_.push_back({r.partition, r.offset, r.op(), r.attributes, std::string(reinterpret_cast<const char*>(r.key.data()), r.key.size()), r.value.empty()});
        }));
    }
    void subscribeAll(rc::Client& c, rc::TopicHandle topic) {
        for (int32_t p = 0; p < topic.partitionCount; ++p) subscribe(c, topic, p);
    }
    /// The partition is the one the topic's partitioner gives `key`.
    void subscribe(rc::Client& c, rc::TopicHandle topic, const rc::Key& key) {
        rc::SubscribeOptions o;
        o.from = rc::From::earliest();
        subs_.push_back(c.subscribe(topic, key, o, [this](const rc::Record& r) {
            std::lock_guard<std::mutex> g(mu_);
            seen_.push_back({r.partition, r.offset, r.op(), r.attributes, std::string(reinterpret_cast<const char*>(r.key.data()), r.key.size()), r.value.empty()});
        }));
    }
    std::vector<Seen> seen() {
        std::lock_guard<std::mutex> g(mu_);
        return seen_;
    }
    std::size_t count() {
        std::lock_guard<std::mutex> g(mu_);
        return seen_.size();
    }

private:
    std::mutex             mu_;
    std::vector<Seen>      seen_;
    std::vector<rc::Subscription> subs_;
};

/// A scratch directory removed on every exit path.
struct ScratchDir {
    std::filesystem::path path;
    explicit ScratchDir(const std::string& tag) : path(std::filesystem::temp_directory_path() / ("rune_" + tag + "_" + std::to_string(::getpid()))) {
        std::filesystem::create_directories(path);
    }
    ~ScratchDir() {
        std::error_code ignore;
        std::filesystem::remove_all(path, ignore);
    }
};

/// `count` writes issued by `issue(i, cq, token)`, at most 128 in flight; `check(i, result)` sees every answer in the order issued and returns false to stop.
template <class Issue, class Check>
bool writePipelined(rc::Client& client, rc::Producer& prod, uint64_t count, Issue issue, Check check) {
    constexpr uint64_t kWindow = 128;
    rc::Completions cq(client);
    uint64_t issued = 0;
    for (uint64_t i = 0; i < count; ++i) {
        for (; issued < count && issued - i < kWindow; ++issued) issue(issued, cq, issued);
        prod.flush();
        std::optional<rc::Completion> answer;
        (void)cq.waitFor(i, 60s, [&](const rc::Completion& d, std::span<const uint8_t>) { answer = d; });
        if (!answer) { ADD_FAILURE() << "write " << i << " never completed"; return false; }
        if (!check(i, rc::WriteResult::fromCompletion(*answer))) return false;
    }
    return true;
}

/// Everything stored on a topic, over all partitions.
std::size_t storedTotal(BrokerNode& n, int32_t topicId, int32_t parts) {
    std::size_t total = 0;
    for (int32_t p = 0; p < parts; ++p) total += storedValues(n, topicId, p).size();
    return total;
}

}  // namespace

TEST(CheckedWritesBrokerRegression, TypedIntentsEndToEndShardRegression) {
    OneBroker b("cw-ki-e2e", OneBroker::Runtime::Shard);
    awaitLevel(b.node());
    ASSERT_FALSE(HasFatalFailure());
    rc::Client& c = *b.connectClient();
    const auto t  = defineKi(c, "ki");
    ASSERT_EQ(t.errorCode, 0) << t.detail;
    KiWriter w(c, "ki");
    Collector col;
    col.subscribeAll(c, rc::TopicHandle{t.topicId, kKiParts});

    std::set<std::pair<int32_t, int64_t>> accepted;
    const auto accept = [&](const rc::WriteResult& r, uint64_t id) {
        EXPECT_TRUE(r.ok());
        EXPECT_GE(r.offset(), 0);
        accepted.emplace(rc::Key(id).partition(kKiParts), r.offset());
    };
    accept(w.topic.insertAndWait(acct(1, "alice")), 1);
    auto r = w.topic.insertAndWait(acct(1, "alice"));
    EXPECT_EQ(r.status(), rc::OpStatus::BrokerError);
    EXPECT_EQ(r.code(), kKeyExists);
    r = w.topic.updateAndWait(acct(2, "bob"));
    EXPECT_EQ(r.code(), kKeyNotFound);
    accept(w.topic.removeExistingAndWait(uint64_t{1}), 1);
    EXPECT_EQ(w.topic.removeExistingAndWait(uint64_t{1}).code(), kKeyNotFound);
    accept(w.topic.removeAndWait(uint64_t{1}), 1);
    accept(w.topic.insertAndWait(acct(1, "alice2")), 1);
    // Id 2 closes the cell: its accepted insert is delivered after the rejected update of id 2, on the same partition.
    accept(w.topic.insertAndWait(acct(2, "bob")), 2);

    ASSERT_TRUE(waitFor([&] { return col.count() >= accepted.size(); }, 10s)) << "the subscriptions deliver the accepted records";
    const std::vector<Seen> seen = col.seen();
    std::set<std::pair<int32_t, int64_t>> delivered;
    for (const Seen& s : seen) delivered.emplace(s.partition, s.offset);
    EXPECT_EQ(seen.size(), accepted.size());
    EXPECT_TRUE(delivered == accepted) << "exactly the accepted records are delivered, no rejected one";
    std::vector<rc::RecordOp> ops;
    for (const Seen& s : seen)
        if (s.partition == rc::Key(uint64_t{1}).partition(kKiParts) && s.key == le(1, 8)) ops.push_back(s.op);
    const std::vector<rc::RecordOp> want = {rc::RecordOp::Insert, rc::RecordOp::Delete, rc::RecordOp::Delete, rc::RecordOp::Insert};
    EXPECT_TRUE(ops == want);
}

TEST(CheckedWritesBrokerRegression, KeyIndexDdlRefusalsShardRegression) {
    OneBroker b("cw-ki-ddl", OneBroker::Runtime::Shard);
    awaitLevel(b.node());
    ASSERT_FALSE(HasFatalFailure());
    rc::Client& c = *b.connectClient();
    rc::TopicOptions keyIndex;
    keyIndex.keyFeatures.keyIndex = true;
    const auto blob = rune::codec::serialize(kiAccountDescriptor());
    const auto expectRefused = [&](const rc::DefineTopicResult& r, const char* detail) {
        EXPECT_EQ(r.errorCode, kInvalidRequest) << detail;
        EXPECT_EQ(r.detail, detail);
    };
    expectRefused(c.defineTopicAndWait("untyped", 1, "", {}, rc::Persistence::Disk, keyIndex, {}, 30s), "key.index needs a typed topic");
    expectRefused(c.defineTopicAndWait("keyless", 1, "", blob, rc::Persistence::Disk, keyIndex, {}, 30s), "key.index needs a key");
    expectRefused(c.defineTopicAndWait("transient", 1, "id", blob, rc::Persistence::Transient, keyIndex, {}, 30s), "key.index needs a persisted topic");
    rc::TopicOptions autoInc = keyIndex;
    autoInc.keyFeatures.autoIncrement = true;
    expectRefused(c.defineTopicAndWait("autoinc", 1, "id", blob, rc::Persistence::Disk, autoInc, {}, 30s), "autoincrement needs cleanup.policy = compact");

    // A key-feature block with an unknown flag bit does not decode: the gateway rejects the frame as InvalidSchema ("DefineTopic body undecodable" in its log).
    // The ErrorResponse carries no message (encodeRejection has none), so only the code is asserted.
    const ScratchDir dir("cwkiddl");
    {
        ShardTestClient client(dir.path / "aeron");
        const int64_t sid = client.connect(b.clientChannel(), 110);
        ASSERT_GT(sid, 0);
        pc::DefineTopic m;
        m.sessionId      = sid;
        m.correlationId  = client.nextCorrelationId();
        m.topicName      = "flags";
        m.partitionCount = 1;
        m.validate       = true;
        m.descriptorBlob = blob.data();
        m.descriptorLen  = static_cast<uint32_t>(blob.size());
        m.hasExt         = true;
        m.ext.clientName = "cw-ki";
        m.ext.requestId  = rune::protocol::RequestId{1, 1};
        m.hasKeyFeatures = true;
        m.keyFeatures.keyIndex = true;
        std::vector<uint8_t> buf(m.encodedSize());
        const std::size_t len = m.encode(buf.data());
        ASSERT_GT(len, rune::protocol::kTopicKeyFeaturesWireLen);
        buf[len - rune::protocol::kTopicKeyFeaturesWireLen] = 0x04;
        ASSERT_TRUE(client.send(b.clientChannel(), 110, buf.data(), len));
        const auto answer = client.awaitAnyDdlAnswer(m.correlationId, Clock::now() + 10s);
        ASSERT_TRUE(answer.has_value());
        EXPECT_EQ(answer->code, static_cast<int16_t>(pc::ErrorCode::InvalidSchema));
    }
}

namespace {

/// {k, n int64}: one record type per key width.  K is an integer type or std::array<uint8_t, N> for a string(N) key.
template <class K>
struct KeyRec {
    static constexpr bool kIsString = !std::is_arithmetic_v<K>;
    static rune::codec::Descriptor descriptor() {
        using rune::codec::PrimType;
        rune::codec::DescriptorBuilder d("kw", 1);
        if constexpr (kIsString) d.addFixedStr("k", sizeof(K), 1, true);
        else if constexpr (std::is_same_v<K, uint16_t>) d.add("k", PrimType::UInt16, 1, true);
        else if constexpr (std::is_same_v<K, int32_t>) d.add("k", PrimType::Int32, 1, true);
        else if constexpr (std::is_same_v<K, uint32_t>) d.add("k", PrimType::UInt32, 1, true);
        else if constexpr (std::is_same_v<K, int64_t>) d.add("k", PrimType::Int64, 1, true);
        else d.add("k", PrimType::UInt64, 1, true);
        d.add("n", PrimType::Int64);
        return d.build();
    }
    static inline const uint64_t kFingerprint = rune::codec::fingerprint(descriptor());
    static constexpr std::string_view kSchemaName = "kw";
    static constexpr uint16_t kVersion = 1;

    K       k{};
    int64_t n = 0;

    [[nodiscard]] std::size_t encodedSize() const noexcept { return 4 + sizeof(K) + 8; }
    [[nodiscard]] std::size_t encode(uint8_t* b) const noexcept {
        const uint16_t block = static_cast<uint16_t>(sizeof(K) + 8);
        b[0] = block & 0xff; b[1] = block >> 8; b[2] = kVersion & 0xff; b[3] = kVersion >> 8;
        std::memcpy(b + 4, &k, sizeof(K));
        std::memcpy(b + 4 + sizeof(K), &n, 8);
        return encodedSize();
    }
    [[nodiscard]] auto key() const noexcept {
        if constexpr (kIsString) return std::span<const uint8_t>(k.data(), k.size());
        else return k;
    }
    [[nodiscard]] rc::Key routeKey() const { return rc::Key(key()); }
    [[nodiscard]] std::string keyBytes() const { return std::string(reinterpret_cast<const char*>(&k), sizeof(K)); }

    static KeyRec make(int i) {
        KeyRec r;
        r.n = i;
        if constexpr (kIsString) {
            r.k.fill(0);
            r.k[0] = static_cast<uint8_t>('a' + i % 26);
            r.k[1] = static_cast<uint8_t>('A' + i / 26);
        } else {
            r.k = static_cast<K>(static_cast<uint32_t>(i) * 40503u + 1u);
        }
        return r;
    }
};

template <class K>
void expectRemoveRoutesAsSend(OneBroker& b, rc::Client& c, const std::string& name, bool keyIndex) {
    using Rec = KeyRec<K>;
    SCOPED_TRACE(name);
    constexpr int kRecords = 50;
    rc::TopicOptions to;
    to.config.cleanupPolicy             = 1;
    to.config.segmentRollBytes          = 256;
    to.config.minCleanableDirtyRatioPpm = 1;
    to.config.fileDeleteDelayMs         = 50;
    to.keyFeatures.keyIndex             = keyIndex;
    const auto t = c.defineTopicAndWait(name, kKiParts, "k", rune::codec::serialize(Rec::descriptor()), rc::Persistence::Disk, to, {}, 60s);
    ASSERT_EQ(t.errorCode, 0) << t.detail;

    rc::Producer prod = c.producer(KiWriter::config(kAllView));
    rc::TypedTopic<Rec, rc::ProducerRef> topic = c.openTopicAndWait<Rec>(name, prod, 30s);
    Collector col;
    col.subscribeAll(c, topic.handle());

    // The cleaner may drop a value its tombstone supersedes before the collector reads it (a compacted topic's contract): its
    // thread is held until every record was delivered, so the count is exact.
    std::promise<void> entered, release;
    std::shared_future<void> released = release.get_future().share();
    std::thread hold([&b, &entered, released] {
        try { b.node().onCleanerThreadForTest([&entered, released] { entered.set_value(); released.wait(); return 0; }, 60s); }
        catch (...) {}   // the test's own waits report a hold that never ran
    });
    struct Release {
        std::promise<void>& p; std::thread& t; bool done = false;
        void operator()() { if (!done) { done = true; p.set_value(); t.join(); } }
        ~Release() { (*this)(); }
    } releaseHold{release, hold};
    ASSERT_EQ(entered.get_future().wait_for(10s), std::future_status::ready) << "the cleaner thread never ran the hold";

    std::multiset<std::pair<int32_t, std::string>> tombstones;
    for (int i = 0; i < kRecords; ++i) {
        const Rec rec = Rec::make(i);
        tombstones.emplace(rec.routeKey().partition(kKiParts), rec.keyBytes());
        topic.send(rec);
        prod.flush();   // a batch per record, so the small segments roll between them
    }
    for (int i = 0; i < kRecords; ++i) {
        topic.remove(Rec::make(i).key());
        prod.flush();
    }

    // The cleaner may drop a send its remove superseded before a late subscription fetches it; the removes are never dropped.
    std::multiset<std::pair<int32_t, std::string>> got;
    ASSERT_TRUE(waitFor([&] {
        got.clear();
        for (const Seen& s : col.seen())
            if (s.tombstone) got.emplace(s.partition, s.key);
        return got.size() >= kRecords;
    }, 10s)) << "delivered " << got.size() << " of " << kRecords << " removes";
    EXPECT_TRUE(got == tombstones) << "every tombstone sits on the partition of its record with the same key bytes";
    for (const Seen& s : col.seen())
        if (!s.tombstone) EXPECT_EQ(tombstones.count({s.partition, s.key}), 1u) << "a delivered send sits on the partition of its record";
    releaseHold();   // the cleaner passes below need its thread

    // The cleaner skips the active segment: one tombstone per partition for a key that was never written (the segment is
    // past its 256 bytes, so the next append rolls it once the provisioner has a segment ready) seals the real tombstones.
    std::array<Rec, kKiParts> fillers;
    for (int32_t p = 0, i = 1000; p < kKiParts; ++i) {
        const Rec rec = Rec::make(i);
        if (rec.routeKey().partition(kKiParts) == p) fillers[static_cast<std::size_t>(p++)] = rec;
    }
    const auto live = [&] {
        std::size_t values = 0;
        for (int32_t p = 0; p < kKiParts; ++p)
            for (const std::string& v : storedValues(b.node(), t.topicId, p)) values += v.empty() ? 0 : 1;
        return values;
    };
    ASSERT_TRUE(waitFor([&] {
        for (const Rec& f : fillers) topic.remove(f.key());
        prod.flush();
        return live() == 0;
    }, 10s)) << live() << " keys are still live after the cleaner pass";
    prod.closeImmediately();
}

}  // namespace

TEST(CheckedWritesBrokerRegression, TypedRemoveRoutesAsSendForEveryKeyWidthShardRegression) {
    OneBroker b("cw-ki-widths", OneBroker::Runtime::Shard);
    awaitLevel(b.node());
    ASSERT_FALSE(HasFatalFailure());
    rc::Client& c = *b.connectClient();
    expectRemoveRoutesAsSend<uint16_t>(b, c, "w_u16", true);
    expectRemoveRoutesAsSend<int32_t>(b, c, "w_i32", true);
    expectRemoveRoutesAsSend<uint32_t>(b, c, "w_u32", true);
    expectRemoveRoutesAsSend<int64_t>(b, c, "w_i64", true);
    expectRemoveRoutesAsSend<uint64_t>(b, c, "w_u64", true);
    expectRemoveRoutesAsSend<std::array<uint8_t, 4>>(b, c, "w_s4", false);
    expectRemoveRoutesAsSend<std::array<uint8_t, 16>>(b, c, "w_s16", false);
}

TEST(CheckedWritesBrokerRegression, IntentsToAnOlderClusterAreRefusedShardRegression) {
    {
        OneBroker b("cw-ki-lvl4", OneBroker::Runtime::Shard, [](NodeConfig& cfg) { cfg.testSupportedFeatureLevel = 4; });
        rc::Client& c = *b.connectClient();
        ASSERT_TRUE(waitFor([&] { return b.node().onWorkerThreadForTest(0, [](Worker& w) { return w.clusterFeatureLevelForTest(); }) == 4; }, 10s));
        const auto ddl = defineKi(c, "ki_idx");
        EXPECT_EQ(ddl.errorCode, kInvalidRequest);
        EXPECT_EQ(ddl.detail, "key.index needs every broker at feature level 5");
        const auto t = defineKi(c, "ki", 0, /*keyIndex=*/false);
        ASSERT_EQ(t.errorCode, 0) << t.detail;
        KiWriter w(c, "ki");
        const auto r = w.topic.insertAndWait(acct(1));
        EXPECT_EQ(r.status(), rc::OpStatus::BrokerError);
        EXPECT_EQ(r.code(), kInvalidRequest);
        EXPECT_EQ(storedTotal(b.node(), t.topicId, kKiParts), 0u);
    }
    OneBroker old("cw-ki-v9", OneBroker::Runtime::Shard, [](NodeConfig& cfg) { cfg.testAdvertisedBrokerVersion = 9; });
    awaitLevel(old.node());
    ASSERT_FALSE(HasFatalFailure());
    rc::Client& c = *old.connectClient();
    const auto t  = defineKi(c, "ki", 0, /*keyIndex=*/false);
    ASSERT_EQ(t.errorCode, 0) << t.detail;
    KiWriter w(c, "ki");
    EXPECT_EQ(w.topic.insertAndWait(acct(1)).status(), rc::OpStatus::BrokerTooOld);
    EXPECT_EQ(storedTotal(old.node(), t.topicId, kKiParts), 0u) << "nothing was sent";
}

TEST(CheckedWritesKeyIndexRegression, RawIntentShapesAreRefusedOrOpaqueRegression) {
    OneBroker b("cw-ki-raw", OneBroker::Runtime::Shard);
    awaitLevel(b.node());
    ASSERT_FALSE(HasFatalFailure());
    rc::Client& c = *b.connectClient();
    const auto ki = defineKi(c, "ki");
    ASSERT_EQ(ki.errorCode, 0) << ki.detail;
    const auto plain = defineKi(c, "plain", 0, /*keyIndex=*/false);
    ASSERT_EQ(plain.errorCode, 0) << plain.detail;
    ASSERT_EQ(c.defineTopicAndWait("raw", 1, "", {}, rc::Persistence::Disk, {}, {}, 30s).errorCode, 0);
    rc::Producer prod = c.producer(KiWriter::config(kAllView));

    // A handle of kind Unknown carries no local check: the broker judges the hand-built shapes.
    const rc::TopicHandle kiHandle{ki.topicId, kKiParts};
    const std::vector<uint8_t> value = encoded(acct(5));
    const rc::Completion reserved = prod.sendAndWait(kiHandle, rc::Key(uint64_t{5}), value, static_cast<rc::WriteIntent>(3), 10s);
    EXPECT_EQ(reserved.status, rc::OpStatus::BrokerError);
    EXPECT_EQ(reserved.errorCode, kInvalidRequest) << "attributes 3 is a reserved shape";
    {
        const ScratchDir dir("cwkiraw");
        {
            ShardTestClient client(dir.path / "aeron");
            const int64_t sid = client.connect(b.clientChannel(), 110);
            ASSERT_GT(sid, 0);
            // The client refuses an empty insert itself, so the frame is built by hand.
            rune::protocol::RecordBatchBuilder batch(0, 1'000'000);
            batch.append(le(5, 8), "", 1'000'000, /*attributes=*/1);
            const int64_t corr = client.nextCorrelationId();
            ASSERT_TRUE(client.produce(b.clientChannel(), 110, sid, corr, ki.topicId, rc::Key(uint64_t{5}).partition(kKiParts),
                                       static_cast<uint8_t>(kAllView), batch.build()));
            const auto ack = client.awaitProduceResponse(corr);
            ASSERT_TRUE(ack.has_value());
            EXPECT_EQ(ack->errorCode, static_cast<int16_t>(kInvalidRequest)) << "an insert tombstone is a reserved shape";
        }
    }
    EXPECT_EQ(storedTotal(b.node(), ki.topicId, kKiParts), 0u) << "neither refused record was stored";

    KiWriter noIndex(c, "plain");
    const auto r = noIndex.topic.insertAndWait(acct(1));
    EXPECT_EQ(r.status(), rc::OpStatus::BrokerError);
    EXPECT_EQ(r.code(), kInvalidRequest) << "an intent needs a key index";

    const rc::TopicHandle rawHandle = c.rawTopicAndWait("raw");
    const std::vector<uint8_t> bytes{1, 2, 3};
    EXPECT_EQ(prod.trySend(rawHandle, rc::Key(uint64_t{1}), bytes, rc::WriteIntent::Insert), rc::SendResult::WriteNotAllowed);
    EXPECT_EQ(prod.trySend(rawHandle, rc::Key(uint64_t{1}), bytes), rc::SendResult::WriteNotAllowed) << "View needs a typed topic";

    // R03b: on a topic the client does not know as raw, the bits are opaque.
    const rc::TopicHandle unknown{rawHandle.topicId, 1};
    Collector col;
    col.subscribe(c, unknown, 0);
    const rc::Completion opaque = prod.sendAndWait(unknown, rc::Key(uint64_t{2}), bytes, rc::WriteIntent::Update, 10s);
    EXPECT_EQ(opaque.status, rc::OpStatus::Ok);
    ASSERT_TRUE(waitFor([&] { return col.count() >= 1; }, 10s));
    const std::vector<Seen> seen = col.seen();
    EXPECT_EQ(seen[0].attributes, 2);
    EXPECT_EQ(seen[0].op, rc::RecordOp::Update);
    EXPECT_EQ(storedValues(b.node(), rawHandle.topicId, 0).size(), 1u);
    prod.closeImmediately();
}

namespace {

struct Outcome {
    bool         ok = false;
    uint16_t     code = 0;
    rc::OpStatus status = rc::OpStatus::Ok;
    std::string  error;
};

std::vector<Outcome> insertRange(rc::ClientConfig cfg, uint64_t first, uint64_t last) {
    std::vector<Outcome> out;
    try {
        rc::Client client(cfg);
        client.connectAndWait(30s);
        KiWriter w(client, "ki");
        writePipelined(client, w.prod, last - first + 1,
            [&](uint64_t i, rc::Completions& cq, rc::Token t) { w.topic.insert(acct(first + i), cq, t); },
            [&](uint64_t, const rc::WriteResult& r) { out.push_back({r.ok(), r.code(), r.status(), {}}); return true; });
        client.close();
    } catch (const std::exception& e) {
        out.push_back({false, 0, rc::OpStatus::Timeout, e.what()});
    }
    return out;
}

/// Two clients race the same ids: each id is accepted once and refused with KeyExists once.
void raceInserts(Cluster& c, int seedA, int seedB, uint64_t first, uint64_t last) {
    using namespace rune::node::test::catalog_cluster;
    rc::ClientConfig a = quickConfig(c, seedA), b = quickConfig(c, seedB);
    a.connectTimeoutMs = b.connectTimeoutMs = 30'000;
    a.pingTimeoutMs = b.pingTimeoutMs = 30'000;
    std::vector<Outcome> ra, rb;
    std::thread ta([&] { ra = insertRange(a, first, last); });
    std::thread tb([&] { rb = insertRange(b, first, last); });
    ta.join();
    tb.join();
    ASSERT_EQ(ra.size(), last - first + 1) << (ra.empty() ? "" : ra.back().error);
    ASSERT_EQ(rb.size(), last - first + 1) << (rb.empty() ? "" : rb.back().error);
    for (std::size_t i = 0; i < ra.size(); ++i) {
        const uint64_t id = first + i;
        EXPECT_EQ(int{ra[i].ok} + int{rb[i].ok}, 1) << "id " << id << " is accepted exactly once";
        const Outcome& lost = ra[i].ok ? rb[i] : ra[i];
        EXPECT_EQ(lost.code, kKeyExists) << "id " << id << " status " << static_cast<int>(lost.status);
    }
}

/// Every id in [first, last] answers KeyExists to an insert through a client of node `seed`.
void expectKeyExists(Cluster& c, int seed, uint64_t first, uint64_t last) {
    using namespace rune::node::test::catalog_cluster;
    rc::ClientConfig cfg = quickConfig(c, seed);
    cfg.connectTimeoutMs = cfg.pingTimeoutMs = 30'000;
    const std::vector<Outcome> r = insertRange(cfg, first, last);
    ASSERT_EQ(r.size(), last - first + 1) << (r.empty() ? "" : r.back().error);
    for (std::size_t i = 0; i < r.size(); ++i) EXPECT_EQ(r[i].code, kKeyExists) << "id " << first + i << " status " << static_cast<int>(r[i].status);
}

/// A 3-replica `ki` on a started cluster, the level raised on every node.
int32_t defineKiOnCluster(Cluster& c) {
    using namespace rune::node::test::catalog_cluster;
    if (!c.awaitCatalogLeader(30s)) { ADD_FAILURE() << "no catalog leader"; return -1; }
    const int32_t ctl = awaitController(c);
    if (ctl <= 0) { ADD_FAILURE() << "no controller"; return -1; }
    for (const int id : c.runningNodes()) awaitLevel(c.node(id));
    if (::testing::Test::HasFatalFailure()) return -1;
    auto admin = connectClient(c, quickConfig(c, ctl));
    const auto t = defineKi(*admin, "ki", 3);
    admin->close();
    if (t.errorCode != 0) { ADD_FAILURE() << t.detail; return -1; }
    std::vector<int32_t> leaders;
    if (!awaitLeaders(c, t.topicId, kKiParts, leaders, 30s)) { ADD_FAILURE() << "no leaders"; return -1; }
    return t.topicId;
}

}  // namespace

TEST(CheckedWritesBrokerRegression, LoginRaceOnAKeyIndexThreeNodesRegression) {
    using namespace rune::node::test::catalog_cluster;
    Cluster c = startCluster("cw05");
    const int32_t topicId = defineKiOnCluster(c);
    ASSERT_GE(topicId, 0);
    raceInserts(c, 1, 2, 1, 100);
    ASSERT_FALSE(HasFatalFailure());

    std::vector<int32_t> leaders;
    ASSERT_TRUE(awaitLeaders(c, topicId, kKiParts, leaders, 30s));
    const int victim = leaders[0];
    c.crash(victim);
    ASSERT_TRUE(awaitLeaders(c, topicId, kKiParts, leaders, 60s)) << "a new leader after the crash";
    const std::vector<int> alive = c.runningNodes();
    ASSERT_EQ(alive.size(), 2u);
    raceInserts(c, alive[0], alive[1], 101, 200);
    ASSERT_FALSE(HasFatalFailure());
    expectKeyExists(c, alive[0], 1, 200);
}

// Leadership moves off one node: the takeover rebuilds the index from the log.  The cell does not assert which node takes
// each partition over; the three nodes run as separate tests.
static void keyIndexSurvivesTakeoverFrom(int id) {
    using namespace rune::node::test::catalog_cluster;
    Cluster c = startCluster("cw05t" + std::to_string(id));
    const int32_t topicId = defineKiOnCluster(c);
    ASSERT_GE(topicId, 0);
    raceInserts(c, 1, 2, 1, 100);
    ASSERT_FALSE(::testing::Test::HasFatalFailure());
    std::vector<int32_t> leaders;
    ASSERT_TRUE(awaitLeaders(c, topicId, kKiParts, leaders, 60s));
    c.crash(id);
    ASSERT_TRUE(awaitLeaders(c, topicId, kKiParts, leaders, 60s)) << "after crashing node " << id;
    expectKeyExists(c, c.runningNodes().front(), 1, 100);
}

TEST(CheckedWritesBrokerRegression, KeyIndexSurvivesTakeoverFromNode1ThreeNodesRegression) { keyIndexSurvivesTakeoverFrom(1); }
TEST(CheckedWritesBrokerRegression, KeyIndexSurvivesTakeoverFromNode2ThreeNodesRegression) { keyIndexSurvivesTakeoverFrom(2); }
TEST(CheckedWritesBrokerRegression, KeyIndexSurvivesTakeoverFromNode3ThreeNodesRegression) { keyIndexSurvivesTakeoverFrom(3); }

TEST(CheckedWritesBrokerRegression, RestartRebuildsTheKeyIndexThreeNodesRegression) {
    using namespace rune::node::test::catalog_cluster;
    Cluster c = startCluster("cw05r");
    const int32_t topicId = defineKiOnCluster(c);
    ASSERT_GE(topicId, 0);
    {
        rc::ClientConfig cfg = quickConfig(c, 1);
        cfg.connectTimeoutMs = cfg.pingTimeoutMs = 30'000;
        rc::Client client(cfg);
        client.connectAndWait(30s);
        {
            KiWriter w(client, "ki");
            ASSERT_TRUE(writePipelined(client, w.prod, 300, [&](uint64_t i, rc::Completions& cq, rc::Token t) { w.topic.insert(acct(i + 1), cq, t); },
                [](uint64_t i, const rc::WriteResult& r) { EXPECT_TRUE(r.ok()) << "insert " << i + 1; return r.ok(); }));
            ASSERT_TRUE(writePipelined(client, w.prod, 100, [&](uint64_t i, rc::Completions& cq, rc::Token t) { w.topic.removeExisting(i + 1, cq, t); },
                [](uint64_t i, const rc::WriteResult& r) { EXPECT_TRUE(r.ok()) << "remove " << i + 1; return r.ok(); }));
        }
        client.close();
    }
    for (int id = 3; id >= 1; --id) c.stop(id);
    for (int id = 1; id <= 3; ++id) c.startNode(id);
    ASSERT_TRUE(c.awaitCatalogLeader(60s));
    std::vector<int32_t> leaders;
    ASSERT_TRUE(awaitLeaders(c, topicId, kKiParts, leaders, 60s));

    expectKeyExists(c, 1, 101, 300);
    ASSERT_FALSE(HasFatalFailure());
    rc::ClientConfig cfg = quickConfig(c, 1);
    cfg.connectTimeoutMs = cfg.pingTimeoutMs = 30'000;
    const std::vector<Outcome> reinserted = insertRange(cfg, 1, 100);
    ASSERT_EQ(reinserted.size(), 100u) << (reinserted.empty() ? "" : reinserted.back().error);
    for (std::size_t i = 0; i < reinserted.size(); ++i) EXPECT_TRUE(reinserted[i].ok) << "removed id " << i + 1 << " code " << reinserted[i].code;
}

TEST(CheckedWritesBrokerRegression, LegacyRuntimeRefusesIntentsRegression) {
    OneBroker b("cw-ki-legacy", OneBroker::Runtime::Legacy);
    awaitLevel(b.node());
    ASSERT_FALSE(HasFatalFailure());
    rc::Client& c = *b.connectClient();
    const auto t  = defineKi(c, "ki");
    ASSERT_EQ(t.errorCode, 0) << t.detail;
    KiWriter w(c, "ki");
    const auto r = w.topic.insertAndWait(acct(1));
    EXPECT_EQ(r.status(), rc::OpStatus::BrokerError);
    EXPECT_EQ(r.code(), kInvalidRequest);
    KiWriter plain(c, "ki", rc::Acks::All);
    EXPECT_TRUE(plain.topic.upsertAndWait(acct(1)).ok());
}

// Part 06 Task 10 (fix round 1): a restarted broker builds its autoincrement topic's replicas from the catalog with the whole
// producer retry window, the depth the cleaner keeps (a runtime create is CatalogEffectsTest's).
TEST(CheckedWritesBrokerRegression, AutoIncTopicKeepsTheRetryWindowDepthAcrossARestartRegression) {
    OneBroker b("cw-ai-depth", OneBroker::Runtime::Shard);
    awaitLevel(b.node());
    ASSERT_FALSE(HasFatalFailure());
    rc::Client& c = *b.connectClient();
    rc::TopicOptions to;
    to.config.cleanupPolicy = 1;   // compact
    to.keyFeatures.autoIncrement = true;
    const auto t = c.defineTopicAndWait("ai", 1, "id", rune::codec::serialize(kiAccountDescriptor()), rc::Persistence::Disk, to, {}, 60s);
    ASSERT_EQ(t.errorCode, 0) << t.detail;
    const auto depth = [&] {
        return b.node().onOwnerThreadForTest(t.topicId, 0, [](rune::replication::PartitionReplica& r) { return r.producerTailDepth(); });
    };
    EXPECT_EQ(depth(), rune::storage::kRetryWindowSize);
    b.restart();
    int got = 0;
    for (int i = 0; i < 400 && got != rune::storage::kRetryWindowSize; ++i) {
        try { got = depth(); } catch (const std::runtime_error&) {}   // the replica is rebuilt after the boot
        if (got != rune::storage::kRetryWindowSize) std::this_thread::sleep_for(25ms);
    }
    EXPECT_EQ(got, rune::storage::kRetryWindowSize) << "the boot path keeps the retry window";
}

// ===========================================================================================================
// Part 06: autoincrement on real brokers.
// ===========================================================================================================
#include "client/support/AutoIncRecords.h"
#include "support/AuthBroker.h"

namespace {

using SO = rune::protocol::SecurityOperation;
using ST = rune::protocol::SecurityResourceType;
using SP = rune::protocol::SecurityPatternKind;

constexpr uint16_t kNotAuthorized = 45;
constexpr const char* kAiPassword = "ai-user-password-1";
constexpr const char* kRetentionDetail = "autoincrement topics keep every record: retention.ms and retention.bytes must be -1";

rc::DefineTopicResult defineAi(rc::Client& c, const std::string& name, const rune::codec::Descriptor& d, const std::string& key,
                               uint32_t parts = 1, int16_t replicas = 0) {
    rc::TopicOptions to;
    to.replicationFactor         = replicas;
    to.config.cleanupPolicy      = 1;   // compact
    to.keyFeatures.autoIncrement = true;
    return c.defineTopicAndWait(name, parts, key, rune::codec::serialize(d), rc::Persistence::Disk, to, {}, 60s);
}

/// A producer with `acks` and the typed topic `name` of record type T written through it.
template <class T>
struct AiWriter {
    rc::Producer                              prod;
    rc::TypedTopic<T, rc::ProducerRef>        topic;

    AiWriter(rc::Client& c, const std::string& name, rc::Acks acks = kAllView)
        : prod(c.producer(KiWriter::config(acks))), topic(c.openTopicAndWait<T>(name, prod, 30s)) {}
    ~AiWriter() { prod.closeImmediately(); }
};

std::vector<uint8_t> aiViewPlan(int32_t srcTopicId) {
    namespace rv = rune::view;
    rv::ViewPlan p;
    p.viewName = "ai_by_id";
    p.key      = "id";
    p.sources.push_back(rv::SourceSpec{srcTopicId, "id"});
    p.output.push_back(rv::AuthoredField{"id", rune::codec::PrimType::UInt64, 0, "id", rv::MergeRule::Replace});
    p.output.push_back(rv::AuthoredField{"n", rune::codec::PrimType::Int64, 0, "n", rv::MergeRule::Replace});
    return rv::serializeAuthored(p);
}

/// The `n` of view row `id` ([bitmap][id][n]); nullopt = absent.  Retries only what a moving leader or a building view answers.
std::optional<int64_t> aiRowN(rc::Client& reader, int32_t viewTopicId, uint64_t id) {
    rc::ViewRowResult row;
    for (const auto deadline = Clock::now() + 60s; Clock::now() < deadline; std::this_thread::sleep_for(100ms)) {
        try { row = reader.getViewRowAndWait(viewTopicId, id, 0, 10s); } catch (const std::exception&) { continue; }
        if (row.status == rc::ViewRowResult::kOk || row.status == rc::ViewRowResult::kAbsentKey) break;
    }
    if (row.status != rc::ViewRowResult::kOk) {
        EXPECT_EQ(row.status, rc::ViewRowResult::kAbsentKey) << "row " << id;
        return std::nullopt;
    }
    int64_t n = 0;
    for (int i = 0; i < 8; ++i) n |= static_cast<int64_t>(row.rowBytes[9 + static_cast<std::size_t>(i)]) << (8 * i);
    return n;
}

uint64_t idOf(const std::string& keyBytes) {
    uint64_t id = 0;
    std::memcpy(&id, keyBytes.data(), std::min<std::size_t>(sizeof id, keyBytes.size()));
    return id;
}

int64_t leoOf(BrokerNode& n, int32_t topicId, int32_t partition) {
    return n.onOwnerThreadForTest(topicId, partition, [](rune::replication::PartitionReplica& r) { return r.log()->logEndOffset(); });
}

/// A connected admin session on the controller of a started cluster, the level raised on every node (a failure is a gtest failure and null).
std::unique_ptr<rc::Client> clusterAdmin(Cluster& c) {
    using namespace rune::node::test::catalog_cluster;
    if (!c.awaitCatalogLeader(30s)) { ADD_FAILURE() << "no catalog leader"; return nullptr; }
    const int32_t ctl = awaitController(c);
    if (ctl <= 0) { ADD_FAILURE() << "no controller"; return nullptr; }
    for (const int id : c.runningNodes()) awaitLevel(c.node(id));
    if (::testing::Test::HasFatalFailure()) return nullptr;
    return connectClient(c, quickConfig(c, ctl));
}

rc::ClientConfig patientConfig(Cluster& c, int seed) {
    rc::ClientConfig cfg = quickConfig(c, seed);
    cfg.connectTimeoutMs = cfg.pingTimeoutMs = 30'000;
    return cfg;
}

std::optional<rune::node::AutoIncReady> counterOf(BrokerNode& n, int32_t topicId) {
    const int32_t w = rune::metadata::ShardRouter(n.workerCountForTest()).workerForPartition(topicId, 0);
    return n.onWorkerThreadForTest(w, [topicId](Worker& wk) { return wk.autoIncStateForTest(topicId, 0); });
}

/// The `i`-th id of a back-filled history: dense runs with gaps.
uint64_t backfillId(uint64_t i) { return i + i / 7 * 3; }

}  // namespace

TEST(CheckedWritesBrokerRegression, AutoIncEndToEndShardRegression) {
    OneBroker b("cw-ai-e2e", OneBroker::Runtime::Shard);
    awaitLevel(b.node());
    ASSERT_FALSE(HasFatalFailure());
    rc::Client& c = *b.connectClient();
    const auto t  = defineAi(c, "user", aiUserDescriptor(), "userId");
    ASSERT_EQ(t.errorCode, 0) << t.detail;
    AiWriter<AiUser> w(c, "user");
    Collector col;
    col.subscribeAll(c, w.topic.handle());

    for (uint64_t i = 1; i <= 100; ++i) {
        const auto r = w.topic.insertAndWait(AiUser{.provider = 1, .subject = "subject" + std::to_string(i)}, 10s);
        ASSERT_TRUE(r.ok()) << "insert " << i << " code " << r.code();
        ASSERT_EQ(r.key(), i);
    }
    EXPECT_TRUE(w.topic.removeExistingAndWait(uint64_t{50}, 10s).ok());
    const auto after = w.topic.insertAndWait(AiUser{.provider = 1, .subject = "after"}, 10s);
    EXPECT_TRUE(after.ok());
    EXPECT_EQ(after.key(), 101u) << "a deleted id is not reused";

    ASSERT_TRUE(waitFor([&] { return col.count() >= 102; }, 10s)) << "delivered " << col.count();
    const std::vector<Seen> seen = col.seen();
    ASSERT_EQ(seen.size(), 102u);
    for (std::size_t i = 0; i < 100; ++i) {
        EXPECT_EQ(seen[i].op, rc::RecordOp::Insert) << "record " << i;
        EXPECT_EQ(idOf(seen[i].key), i + 1);
    }
    EXPECT_EQ(seen[100].op, rc::RecordOp::Delete);
    EXPECT_EQ(idOf(seen[100].key), 50u);
    EXPECT_EQ(seen[101].op, rc::RecordOp::Insert);
    EXPECT_EQ(idOf(seen[101].key), 101u);

    AiWriter<AiUser> allOnly(c, "user", rc::Acks::All);
    const auto plain = allOnly.topic.insertAndWait(AiUser{.provider = 1, .subject = "plain"}, 10s);
    EXPECT_TRUE(plain.ok());
    EXPECT_EQ(plain.key(), 102u);
}

TEST(CheckedWritesBrokerRegression, ModuloRoutingIsTheSameOnEveryPathShardRegression) {
    constexpr int32_t kParts = 4;
    OneBroker b("cw-ai-mod", OneBroker::Runtime::Shard);
    awaitLevel(b.node());
    ASSERT_FALSE(HasFatalFailure());
    rc::Client& c = *b.connectClient();
    const auto t  = defineAi(c, "ai", aiDescriptor(), "id", kParts);
    ASSERT_EQ(t.errorCode, 0) << t.detail;
    const auto view = c.createViewAndWait("ai_by_id", "id", 0, aiViewPlan(t.topicId), {}, 60s);
    ASSERT_EQ(view.errorCode, 0) << view.detail;
    AiWriter<Ai> w(c, "ai");
    Collector col;
    col.subscribeAll(c, w.topic.handle());

    std::vector<uint64_t> ids;
    for (int64_t i = 1; i <= 400; ++i) {
        const auto r = w.topic.insertAndWait(Ai{.n = i}, 10s);
        ASSERT_TRUE(r.ok()) << "insert " << i << " code " << r.code();
        ids.push_back(r.key());
    }
    ASSERT_TRUE(waitFor([&] { return col.count() >= ids.size(); }, 10s)) << "delivered " << col.count();
    std::map<uint64_t, int32_t> partitionOf;
    std::array<std::vector<uint64_t>, kParts> perPartition;
    for (const Seen& s : col.seen()) {
        partitionOf[idOf(s.key)] = s.partition;
        perPartition[static_cast<std::size_t>(s.partition)].push_back(idOf(s.key));
    }
    for (const uint64_t id : ids) {
        ASSERT_EQ(partitionOf.count(id), 1u) << "id " << id;
        EXPECT_EQ(static_cast<int32_t>(id % kParts), partitionOf[id]) << "id " << id;
    }
    for (const auto& run : perPartition) {
        EXPECT_FALSE(run.empty());
        for (std::size_t i = 1; i < run.size(); ++i) EXPECT_EQ(run[i], run[i - 1] + kParts);
    }

    const std::vector<uint64_t> probes(ids.begin(), ids.begin() + 40);
    for (std::size_t i = 0; i < probes.size(); ++i) {
        const rc::WriteResult up = w.topic.updateAndWait(Ai{.id = probes[i], .n = 7}, 10s);
        EXPECT_TRUE(up.ok()) << "update " << probes[i] << " code " << up.code();
        if (i % 2 == 1) {
            const rc::WriteResult rm = w.topic.removeExistingAndWait(probes[i], 10s);
            EXPECT_TRUE(rm.ok()) << "remove " << probes[i] << " code " << rm.code();
        }
    }
    for (std::size_t i = 0; i < probes.size(); ++i) {
        const std::optional<int64_t> n = aiRowN(c, view.viewTopicId, probes[i]);
        EXPECT_EQ(n.has_value(), i % 2 == 0) << "view row " << probes[i];
        if (n) EXPECT_EQ(*n, 7);
    }

    std::vector<std::unique_ptr<Collector>> keyed;
    for (const uint64_t id : probes) {
        keyed.push_back(std::make_unique<Collector>());
        keyed.back()->subscribe(c, w.topic.handle(), rc::Key(id));
    }
    for (std::size_t i = 0; i < probes.size(); ++i) {
        Collector& k = *keyed[i];
        ASSERT_TRUE(waitFor([&] { return k.count() > 0; }, 10s)) << "keyed subscription of " << probes[i];
        for (const Seen& s : k.seen()) EXPECT_EQ(s.partition, static_cast<int32_t>(probes[i] % kParts)) << "keyed subscription of " << probes[i];
    }

    // The gateway resolves the partition of an explicit update (partitionKnown = 0).
    rc::Client& blind = *b.connectClient();
    blind.producePartitionUnknownForTest(true);
    AiWriter<Ai> viaGateway(blind, "ai");
    for (std::size_t i = 0; i < probes.size(); i += 2) {
        const rc::WriteResult up = viaGateway.topic.updateAndWait(Ai{.id = probes[i], .n = 99}, 10s);
        EXPECT_TRUE(up.ok()) << "update " << probes[i] << " code " << up.code();
        const std::optional<int64_t> n = aiRowN(c, view.viewTopicId, probes[i]);
        ASSERT_TRUE(n.has_value());
        EXPECT_EQ(*n, 99);
    }
}

TEST(CheckedWritesBrokerRegression, ImportNeedsTheGrantShardRegression) {
    AuthBroker b("ai-import", {.shard = true});
    b.bootstrapAdmin();
    b.createUser("app", kAiPassword, {AuthGrant{SO::Write, ST::Topic, SP::Exact, "user"}});
    b.createUser("backfill", kAiPassword, {AuthGrant{SO::Write, ST::Topic, SP::Exact, "user"}, AuthGrant{SO::Import, ST::Topic, SP::Exact, "user"}});
    awaitLevel(b.node());
    ASSERT_FALSE(HasFatalFailure());
    const auto connect = [&](const std::string& user, const std::string& password, bool partitionUnknown) {
        rc::ClientConfig cfg;
        cfg.gatewayChannel  = b.channel();
        cfg.gatewayStreamId = 1;
        cfg.egressChannel   = "aeron:udp?endpoint=127.0.0.1:" + std::to_string(rune::aeronutil::testing::allocFreeUdpPort());
        cfg.egressStreamId  = 2;
        cfg.dataDir         = b.clientDir().string();
        cfg.username        = user;
        cfg.password        = password;
        cfg.offerCrypto     = true;
        cfg.tlsTrustPem     = AuthBroker::tlsCaPem();
        auto client = std::make_unique<rc::Client>(cfg);
        client->connectAndWait(30s);
        client->producePartitionUnknownForTest(partitionUnknown);
        return client;
    };
    auto admin = connect("admin", AuthBroker::kAdminPassword, false);
    const auto t = defineAi(*admin, "user", aiUserDescriptor(), "userId");
    ASSERT_EQ(t.errorCode, 0) << t.detail;
    admin->close();

    // The second round has the gateway resolve the partition; the answers are the same.  Verdict markers are appended on their own
    // schedule, so what a refused import must not change is the data records, not the log end.
    std::size_t stored = storedValues(b.node(), t.topicId, 0).size();
    uint64_t explicitId = 500;
    for (const bool partitionUnknown : {false, true}) {
        auto app      = connect("app", kAiPassword, partitionUnknown);
        auto backfill = connect("backfill", kAiPassword, partitionUnknown);
        AiWriter<AiUser> appW(*app, "user"), backfillW(*backfill, "user");
        const auto denied = appW.topic.insertAndWait(AiUser{.userId = explicitId, .provider = 1, .subject = "denied"}, 10s);
        EXPECT_EQ(denied.status(), rc::OpStatus::BrokerError);
        EXPECT_EQ(denied.code(), kNotAuthorized);
        EXPECT_EQ(storedValues(b.node(), t.topicId, 0).size(), stored) << "a refused import appends nothing";
        const auto granted = backfillW.topic.insertAndWait(AiUser{.userId = explicitId, .provider = 1, .subject = "imported"}, 10s);
        EXPECT_TRUE(granted.ok()) << "code " << granted.code();
        EXPECT_EQ(granted.key(), 0u) << "only a key-0 create is answered with an id (P6-10)";
        EXPECT_EQ(backfillW.topic.insertAndWait(AiUser{.userId = explicitId, .provider = 1, .subject = "again"}, 10s).code(), kKeyExists);
        const auto next = appW.topic.insertAndWait(AiUser{.provider = 1, .subject = "next"}, 10s);
        EXPECT_TRUE(next.ok()) << "code " << next.code();
        EXPECT_EQ(next.key(), explicitId + 1) << "the counter moved past the import";
        stored = storedValues(b.node(), t.topicId, 0).size();
        explicitId = 600;
        app->close();
        backfill->close();
    }

    AuthBroker old("ai-import-lvl4", {.shard = true, .configure = [](NodeConfig& cfg) { cfg.testSupportedFeatureLevel = 4; }});
    old.bootstrapAdmin();
    old.createUser("backfill", kAiPassword);
    const auto grant = old.admin().grant(old.channel(), old.adminSession(), "backfill", false, SO::Import, ST::Topic, SP::Exact, "user");
    EXPECT_EQ(grant.errorCode, kInvalidRequest);
    EXPECT_EQ(grant.detail, "the IMPORT grant needs every broker at feature level 5");
}

TEST(CheckedWritesBrokerRegression, RegressorsPathBackFillThenInsertsNeverCollideThreeNodesRegression) {
    using namespace rune::node::test::catalog_cluster;
    constexpr int kUsers = 3000, kCharacters = 6000;
    Cluster c = startCluster("cw06");
    int32_t userTopic = -1, characterTopic = -1;
    {
        auto admin = clusterAdmin(c);
        ASSERT_TRUE(admin != nullptr);
        const auto u = defineAi(*admin, "user", aiUserDescriptor(), "userId", 1, 3);
        ASSERT_EQ(u.errorCode, 0) << u.detail;
        const auto ch = defineAi(*admin, "character", aiCharacterDescriptor(), "characterId", 1, 3);
        ASSERT_EQ(ch.errorCode, 0) << ch.detail;
        userTopic      = u.topicId;
        characterTopic = ch.topicId;
        admin->close();
    }
    std::vector<int32_t> userLeader, characterLeader;
    ASSERT_TRUE(awaitLeaders(c, userTopic, 1, userLeader, 30s));
    ASSERT_TRUE(awaitLeaders(c, characterTopic, 1, characterLeader, 30s));

    std::set<uint64_t> userIds, characterIds;   // every id any insert was ever answered with
    const auto expectFresh = [](std::set<uint64_t>& answered, const rc::WriteResult& r, const char* topic, uint64_t above) {
        EXPECT_TRUE(r.ok()) << topic << " code " << r.code();
        EXPECT_GT(r.key(), above) << topic;
        EXPECT_NE(r.key(), 52u) << topic;
        EXPECT_TRUE(answered.insert(r.key()).second) << topic << " id " << r.key() << " answered twice";
    };
    {
        auto client = connectClient(c, patientConfig(c, 1));
        AiWriter<AiUser> users(*client, "user");
        ASSERT_TRUE(writePipelined(*client, users.prod, kUsers,
            [&](uint64_t i, rc::Completions& cq, rc::Token t) { users.topic.insert(AiUser{.userId = backfillId(i + 1), .provider = 1, .subject = "user" + std::to_string(i + 1)}, cq, t); },
            [&](uint64_t i, const rc::WriteResult& r) {
                EXPECT_TRUE(r.ok()) << "user " << i + 1 << " code " << r.code();
                userIds.insert(backfillId(i + 1));
                return r.ok();
            }));
        AiWriter<AiCharacter> characters(*client, "character");
        ASSERT_TRUE(writePipelined(*client, characters.prod, kCharacters,
            [&](uint64_t i, rc::Completions& cq, rc::Token t) { characters.topic.insert(AiCharacter{.characterId = backfillId(i + 1), .userId = backfillId((i + 1) % kUsers + 1), .name = "c" + std::to_string(i + 1)}, cq, t); },
            [&](uint64_t i, const rc::WriteResult& r) {
                EXPECT_TRUE(r.ok()) << "character " << i + 1 << " code " << r.code();
                characterIds.insert(backfillId(i + 1));
                return r.ok();
            }));
    }
    const uint64_t userMax = *userIds.rbegin(), characterMax = *characterIds.rbegin();

    const auto insertLive = [&](int seed, int users, int characters, uint64_t userAbove, uint64_t characterAbove) {
        auto client = connectClient(c, patientConfig(c, seed));
        AiWriter<AiUser> u(*client, "user");
        ASSERT_TRUE(writePipelined(*client, u.prod, static_cast<uint64_t>(users),
            [&](uint64_t i, rc::Completions& cq, rc::Token t) { u.topic.insert(AiUser{.provider = 2, .subject = "live" + std::to_string(i)}, cq, t); },
            [&](uint64_t, const rc::WriteResult& r) { expectFresh(userIds, r, "user", userAbove); return r.ok(); }));
        AiWriter<AiCharacter> ch(*client, "character");
        ASSERT_TRUE(writePipelined(*client, ch.prod, static_cast<uint64_t>(characters),
            [&](uint64_t i, rc::Completions& cq, rc::Token t) { ch.topic.insert(AiCharacter{.userId = 1, .name = "live" + std::to_string(i)}, cq, t); },
            [&](uint64_t, const rc::WriteResult& r) { expectFresh(characterIds, r, "character", characterAbove); return r.ok(); }));
    };

    const int first = userLeader[0];
    c.crash(first);
    ASSERT_TRUE(awaitLeaders(c, userTopic, 1, userLeader, 60s));
    ASSERT_TRUE(awaitLeaders(c, characterTopic, 1, characterLeader, 60s));
    insertLive(c.runningNodes().front(), 500, 1000, userMax, characterMax);
    ASSERT_FALSE(HasFatalFailure());
    const auto userCounter = counterOf(c.node(userLeader[0]), userTopic);
    const auto characterCounter = counterOf(c.node(characterLeader[0]), characterTopic);
    ASSERT_TRUE(userCounter && characterCounter);
    EXPECT_GT(userCounter->nextN, userMax);
    EXPECT_GT(characterCounter->nextN, characterMax);

    c.startNode(first);
    ASSERT_TRUE(awaitLeaders(c, userTopic, 1, userLeader, 60s));
    c.crash(userLeader[0]);
    ASSERT_TRUE(awaitLeaders(c, userTopic, 1, userLeader, 60s));
    ASSERT_TRUE(awaitLeaders(c, characterTopic, 1, characterLeader, 60s));
    const uint64_t userSoFar = *userIds.rbegin(), characterSoFar = *characterIds.rbegin();
    insertLive(c.runningNodes().front(), 100, 100, userSoFar, characterSoFar);
    EXPECT_EQ(userIds.size(), static_cast<std::size_t>(kUsers) + 600);
    EXPECT_EQ(characterIds.size(), static_cast<std::size_t>(kCharacters) + 1100);
}

TEST(CheckedWritesBrokerRegression, AutoIncDdlRefusalsShardRegression) {
    OneBroker b("cw-ai-ddl", OneBroker::Runtime::Shard);
    awaitLevel(b.node());
    ASSERT_FALSE(HasFatalFailure());
    rc::Client& c = *b.connectClient();
    const auto blob = rune::codec::serialize(aiUserDescriptor());
    const auto expectRefused = [&](const rc::DefineTopicResult& r, const char* detail) {
        EXPECT_EQ(r.errorCode, kInvalidRequest) << detail;
        EXPECT_EQ(r.detail, detail);
    };
    rc::TopicOptions ai;
    ai.config.cleanupPolicy      = 1;
    ai.keyFeatures.autoIncrement = true;
    const auto define = [&](const char* name, const char* key, std::span<const uint8_t> descriptor, rc::Persistence persistence, const rc::TopicOptions& to) {
        return c.defineTopicAndWait(name, 1, key, descriptor, persistence, to, {}, 30s);
    };
    expectRefused(define("untyped", "", {}, rc::Persistence::Disk, ai), "autoincrement needs a typed topic");
    expectRefused(define("keyless", "", blob, rc::Persistence::Disk, ai), "autoincrement needs a key");
    expectRefused(define("transient", "userId", blob, rc::Persistence::Transient, ai), "autoincrement needs a persisted topic");
    const auto narrow = rune::codec::serialize(KeyRec<uint16_t>::descriptor());
    expectRefused(define("narrow", "k", narrow, rc::Persistence::Disk, ai), "autoincrement needs a uint64, int64 or uint32 key");
    for (const int16_t policy : {0, 2}) {
        rc::TopicOptions to = ai;
        to.config.cleanupPolicy = policy;
        expectRefused(define("policy", "userId", blob, rc::Persistence::Disk, to), "autoincrement needs cleanup.policy = compact");
    }
    rc::TopicOptions retention = ai;
    retention.config.retentionMs = 1000;
    expectRefused(define("retention", "userId", blob, rc::Persistence::Disk, retention), kRetentionDetail);
    ASSERT_EQ(define("adopted", "", {}, rc::Persistence::Disk, {}).errorCode, 0);
    expectRefused(define("adopted", "userId", blob, rc::Persistence::Disk, ai), "autoincrement needs a new topic");

    const auto t = define("user", "userId", blob, rc::Persistence::Disk, ai);
    ASSERT_EQ(t.errorCode, 0) << t.detail;
    const rune::protocol::client::ConfigChangeWire retentionBytes{static_cast<uint8_t>(rune::protocol::ConfigKey::RetentionBytes), 1};
    const auto alter = c.alterTopicConfigAndWait("user", 0, std::span(&retentionBytes, 1), {}, 30s);
    EXPECT_EQ(alter.errorCode, kInvalidRequest);
    EXPECT_EQ(alter.detail, kRetentionDetail);
}

TEST(CheckedWritesBrokerRegression, LegacyRuntimeRefusesKeyZeroCreatesRegression) {
    OneBroker b("cw-ai-legacy", OneBroker::Runtime::Legacy);
    awaitLevel(b.node());
    ASSERT_FALSE(HasFatalFailure());
    rc::Client& c = *b.connectClient();
    const auto t  = defineAi(c, "user", aiUserDescriptor(), "userId");
    ASSERT_EQ(t.errorCode, 0) << t.detail;
    AiWriter<AiUser> w(c, "user", rc::Acks::All);
    const auto create = w.topic.insertAndWait(AiUser{.provider = 1, .subject = "new"}, 10s);
    EXPECT_EQ(create.status(), rc::OpStatus::BrokerError);
    EXPECT_EQ(create.code(), kInvalidRequest);
    const auto remove = w.topic.removeAndWait(uint64_t{1}, 10s);
    EXPECT_TRUE(remove.ok()) << "a plain tombstone is not an intent; code " << remove.code();
}

TEST(CheckedWritesBrokerRegression, KeyZeroCreatesToAnOlderClusterShardRegression) {
    {
        OneBroker b("cw-ai-lvl4", OneBroker::Runtime::Shard, [](NodeConfig& cfg) { cfg.testSupportedFeatureLevel = 4; });
        rc::Client& c = *b.connectClient();
        ASSERT_TRUE(waitFor([&] { return b.node().onWorkerThreadForTest(0, [](Worker& w) { return w.clusterFeatureLevelForTest(); }) == 4; }, 10s));
        const auto ddl = defineAi(c, "user", aiUserDescriptor(), "userId");
        EXPECT_EQ(ddl.errorCode, kInvalidRequest);
        EXPECT_EQ(ddl.detail, "autoincrement needs every broker at feature level 5");
        rc::TopicOptions plain;
        plain.config.cleanupPolicy = 1;
        const auto t = c.defineTopicAndWait("plain", 1, "userId", rune::codec::serialize(aiUserDescriptor()), rc::Persistence::Disk, plain, {}, 30s);
        ASSERT_EQ(t.errorCode, 0) << t.detail;
        // A key-0 create sent as an autoincrement client sends it, to a topic that already exists.
        rc::Producer prod = c.producer(KiWriter::config(rc::Acks::All));
        const rc::TopicHandle handle{t.topicId, 1, rc::TopicKind::Typed, rune::protocol::Partitioner::Modulo};
        const AiUser rec{.provider = 1, .subject = "new"};
        std::vector<uint8_t> bytes(rec.encodedSize());
        (void)rec.encode(bytes.data());
        const rc::Completion r = prod.sendAndWait(handle, rc::Key(uint64_t{0}), bytes, rc::WriteIntent::Insert, 10s);
        EXPECT_EQ(r.status, rc::OpStatus::BrokerError);
        EXPECT_EQ(r.errorCode, kInvalidRequest);
        EXPECT_EQ(leoOf(b.node(), t.topicId, 0), 0);
        prod.closeImmediately();
    }
    int32_t advertised = 0;
    OneBroker old("cw-ai-v9", OneBroker::Runtime::Shard, [&advertised](NodeConfig& cfg) { cfg.testAdvertisedBrokerVersion = advertised; });
    awaitLevel(old.node());
    ASSERT_FALSE(HasFatalFailure());
    const auto t = defineAi(*old.connectClient(), "user", aiUserDescriptor(), "userId");
    ASSERT_EQ(t.errorCode, 0) << t.detail;
    advertised = 9;   // the client cannot define key features below 10, so the topic is made first
    old.restart();
    awaitLevel(old.node());
    ASSERT_FALSE(HasFatalFailure());
    rc::Client& c = *old.connectClient();
    rc::Producer prod = c.producer(KiWriter::config(kAllView));
    const rc::TopicHandle handle{t.topicId, 1, rc::TopicKind::Typed, rune::protocol::Partitioner::Modulo};
    const AiUser rec{.provider = 1, .subject = "new"};
    std::vector<uint8_t> bytes(rec.encodedSize());
    (void)rec.encode(bytes.data());
    EXPECT_EQ(prod.sendAndWait(handle, rc::Key(uint64_t{0}), bytes, rc::WriteIntent::Insert, 10s).status, rc::OpStatus::BrokerTooOld);
    EXPECT_TRUE(storedValues(old.node(), t.topicId, 0).empty()) << "nothing was sent";
    prod.closeImmediately();
}

TEST(CheckedWritesBrokerRegression, LostAckRetryAcrossFailoverReturnsTheSameIdsThreeNodesRegression) {
    using namespace rune::node::test::catalog_cluster;
    Cluster c = startCluster("cw06la");
    int32_t topicId = -1, viewTopic = -1;
    {
        auto admin = clusterAdmin(c);
        ASSERT_TRUE(admin != nullptr);
        const auto t = defineAi(*admin, "ai", aiDescriptor(), "id", 1, 3);
        ASSERT_EQ(t.errorCode, 0) << t.detail;
        const auto v = admin->createViewAndWait("ai_by_id", "id", 0, aiViewPlan(t.topicId), {}, 60s);
        ASSERT_EQ(v.errorCode, 0) << v.detail;
        topicId   = t.topicId;
        viewTopic = v.viewTopicId;
        admin->close();
    }
    std::vector<int32_t> leaders;
    ASSERT_TRUE(awaitLeaders(c, topicId, 1, leaders, 30s));
    const int victim = leaders[0];
    std::vector<int> others;
    for (const int id : c.runningNodes()) if (id != victim) others.push_back(id);
    ASSERT_EQ(others.size(), 2u);
    const int seed = others[0], follower = others[1];

    auto client = connectClient(c, patientConfig(c, seed));
    AiWriter<Ai> w(*client, "ai");
    for (int64_t i = 1; i <= 3; ++i) {
        const auto r = w.topic.insertAndWait(Ai{.n = i}, 60s);
        ASSERT_TRUE(r.ok());
        ASSERT_EQ(r.key(), static_cast<uint64_t>(i));
    }
    client->dropProduceAcksForTest(1);
    constexpr int64_t kMarker = 4242;
    std::optional<rc::WriteResult> retried;
    std::thread inserter([&] { retried.emplace(w.topic.insertAndWait(Ai{.n = kMarker}, 90s)); });

    // The follower holds the record, so it is committed: the ack that was dropped had been sent.
    uint64_t assigned = 0;
    ASSERT_TRUE(waitFor([&] {
        for (const std::string& v : storedValues(c.node(follower), topicId, 0)) {
            int64_t n = 0;
            if (v.size() == 20 && (std::memcpy(&n, v.data() + 12, 8), n == kMarker)) { std::memcpy(&assigned, v.data() + 4, 8); return true; }
        }
        return false;
    }, 10s));
    c.crash(victim);
    inserter.join();
    ASSERT_TRUE(retried.has_value());
    EXPECT_TRUE(retried->ok()) << "status " << static_cast<int>(retried->status()) << " code " << retried->code();
    EXPECT_NE(retried->status(), rc::OpStatus::StoredIdUnknown);
    EXPECT_EQ(retried->key(), assigned) << "the retry on the new leader answers the id the old leader assigned";
    EXPECT_EQ(assigned, 4u);

    ASSERT_TRUE(awaitLeaders(c, topicId, 1, leaders, 60s));
    auto reader = connectClient(c, patientConfig(c, seed));
    const std::optional<int64_t> row = aiRowN(*reader, viewTopic, assigned);
    ASSERT_TRUE(row.has_value());
    EXPECT_EQ(*row, kMarker);
    EXPECT_EQ(storedValues(c.node(leaders[0]), topicId, 0).size(), 4u) << "the retry stored nothing twice";
    reader->close();
}

TEST(CheckedWritesBrokerRegression, WideKeyIndexSurvivesFailoverThreeNodesRegression) {
    using namespace rune::node::test::catalog_cluster;
    using Wide = KeyRec<std::array<uint8_t, 16>>;
    Cluster c = startCluster("cw07wk");
    ASSERT_TRUE(c.awaitCatalogLeader(30s));
    const int32_t ctl = awaitController(c);
    ASSERT_GT(ctl, 0);
    for (const int id : c.runningNodes()) awaitLevel(c.node(id));
    ASSERT_FALSE(HasFatalFailure());
    rc::TopicOptions to;
    to.replicationFactor    = 3;
    to.config.cleanupPolicy = 1;
    to.keyFeatures.keyIndex = true;
    auto admin = connectClient(c, quickConfig(c, ctl));
    const auto t = admin->defineTopicAndWait("wide", kKiParts, "k", rune::codec::serialize(Wide::descriptor()), rc::Persistence::Disk, to, {}, 60s);
    admin->close();
    ASSERT_EQ(t.errorCode, 0) << t.detail;
    std::vector<int32_t> leaders;
    ASSERT_TRUE(awaitLeaders(c, t.topicId, kKiParts, leaders, 30s));

    const auto insertAll = [&](int seed, int first, int last, int existingBelow) {
        rc::ClientConfig cfg = quickConfig(c, seed);
        cfg.connectTimeoutMs = cfg.pingTimeoutMs = 30'000;
        rc::Client client(cfg);
        client.connectAndWait(30s);
        rc::Producer prod = client.producer(KiWriter::config(kAllView));
        rc::TypedTopic<Wide, rc::ProducerRef> topic = client.openTopicAndWait<Wide>("wide", prod, 30s);
        writePipelined(client, prod, last - first + 1, [&](uint64_t i, rc::Completions& cq, rc::Token t) { topic.insert(Wide::make(first + static_cast<int>(i)), cq, t); },
            [&](uint64_t i, const rc::WriteResult& r) {
                if (first + static_cast<int>(i) >= existingBelow) EXPECT_TRUE(r.ok()) << "insert " << first + static_cast<int>(i) << " code " << r.code();
                else EXPECT_EQ(r.code(), kKeyExists) << "insert " << first + static_cast<int>(i);
                return true;
            });
        prod.closeImmediately();
        client.close();
    };
    insertAll(1, 0, 99, 0);
    ASSERT_FALSE(HasFatalFailure());

    c.crash(leaders[0]);
    ASSERT_TRUE(awaitLeaders(c, t.topicId, kKiParts, leaders, 60s)) << "a new leader after the crash";
    const int seed = c.runningNodes().front();
    insertAll(seed, 0, 100, 100);
}

// ===========================================================================================================
// Part 07: wide view keys on a real broker - composite, lower(), pages, restart, failover and routing.
// ===========================================================================================================
#include <rune/client/view/ViewKeyBuilder.h>
#include <rune/codec/ViewKeyEncoding.h>

namespace {

namespace rv = rune::view;
namespace rcv = rune::client::view;
using rune::codec::DescriptorBuilder;
using rune::codec::PrimType;

constexpr uint16_t kViewAbsentKey = rc::ViewRowResult::kAbsentKey, kViewInvalidRequest = rc::ViewRowResult::kInvalidRequest;

rune::codec::Descriptor characterDescriptor() {
    return DescriptorBuilder("character", 1).add("characterId", PrimType::UInt64, 1, true).add("userId", PrimType::UInt64).addFixedStr("name", 16)
        .add("gender", PrimType::UInt16).add("lastPlayed", PrimType::Int64).build();
}

std::string characterValue(uint64_t id, uint64_t user, const std::string& name) {
    std::string padded = name;
    padded.resize(16, '\0');
    return le(42, 2) + le(1, 2) + le(id, 8) + le(user, 8) + padded + le(1, 2) + le(id, 8);
}

rc::DefineTopicResult defineCharacters(rc::Client& c, int16_t replicas = 0) {
    rc::TopicOptions to;
    to.replicationFactor = replicas;
    return c.defineTopicAndWait("character", 1, "characterId", rune::codec::serialize(characterDescriptor()), rc::Persistence::Disk, to, {}, 60s);
}

/// A view over `character` keyed on `key`, carrying the columns `columns`.
rv::ViewPlan characterPlan(const std::string& name, int32_t sourceTopicId, std::vector<rv::KeyComponent> key, std::initializer_list<const char*> columns) {
    rv::ViewPlan p;
    p.viewName = name;
    p.sources.push_back(rv::SourceSpec{sourceTopicId, "characterId"});
    p.keyComponents = std::move(key);
    for (const rv::KeyComponent& k : p.keyComponents)
        if (k.keyFunction == rune::codec::kKeyFunctionLower) p.unicodeVersion = rune::codec::kUnicodeVersion;
    const rune::codec::Descriptor d = characterDescriptor();
    for (const char* n : columns) p.output.push_back(rv::AuthoredField{n, d.find(n)->type, 0, n, rv::MergeRule::Replace});
    return p;
}

int32_t createView(rc::Client& c, const rv::ViewPlan& p, const std::string& keyField) {
    const auto v = c.createViewAndWait(p.viewName, rcv::viewKeyField(p, keyField), 0, rv::serializeAuthored(p), {}, 60s);
    EXPECT_EQ(v.errorCode, 0) << v.detail;
    return v.viewTopicId;
}

rv::ViewPlan accountCharactersPlan(int32_t sourceTopicId) {
    return characterPlan("account_characters", sourceTopicId, {{"userId", 0, "userId"}, {"characterId", 0, "characterId"}}, {"name", "gender"});
}

rv::ViewPlan nameKeyPlan(int32_t sourceTopicId) {
    return characterPlan("name_key", sourceTopicId, {{"name", rune::codec::kKeyFunctionLower, "nameKey"}, {"characterId", 0, "characterId"}}, {"name"});
}

/// The view is in `n`'s applied catalog (a follower applies a create-view entry after the controller acked it).
void awaitViewApplied(BrokerNode& n, const std::string& view) {
    bool applied = false;
    for (int i = 0; i < 1200 && !applied; ++i) {
        applied = n.onAdminThreadForTest([&n, &view] { return n.adminWorkerAgentForTest()->worker().applier().state().viewByName(rune::catalog::kDefaultNsId, view) != nullptr; });
        if (!applied) std::this_thread::sleep_for(25ms);
    }
    ASSERT_TRUE(applied) << "the view reaches the node's catalog: " << view;
}

rcv::ViewKeyBuilder keyBuilder(BrokerNode& n, const std::string& view) {
    auto b = n.onAdminThreadForTest([&n, &view] { return rcv::ViewKeyBuilder::fromCatalog(n.adminWorkerAgentForTest()->worker().applier().state(), view); });
    if (!b) throw std::runtime_error(b.error());
    return std::move(*b);
}

std::vector<uint8_t> bytesOf(std::expected<std::vector<uint8_t>, std::string> k) {
    if (!k) throw std::runtime_error(k.error());
    return std::move(*k);
}

/// The columns of a view's rows by name, from the output descriptor the catalog holds.
struct ViewColumns {
    rcv::RowLayout           layout;
    std::vector<std::string> names;

    ViewColumns(BrokerNode& n, const std::string& view) {
        const auto blob = n.onAdminThreadForTest([&n, &view] {
            return n.adminWorkerAgentForTest()->worker().applier().state().viewByName(rune::catalog::kDefaultNsId, view)->def.outputDescriptorBlob;
        });
        std::vector<PrimType> types;
        std::vector<uint16_t> widths;
        for (const auto& f : rune::codec::deserialize(blob).fields) {
            names.push_back(f.name);
            types.push_back(f.type);
            widths.push_back(f.type == PrimType::FixedStr ? f.fixedLen : uint16_t{0});
        }
        layout = rcv::buildRowLayout(types, widths);
    }
    [[nodiscard]] rcv::FieldValue at(std::span<const uint8_t> row, const std::string& column) const {
        const auto slot = std::find(names.begin(), names.end(), column) - names.begin();
        return rcv::readSlot(row, layout, static_cast<uint16_t>(slot));
    }
    [[nodiscard]] uint64_t u(std::span<const uint8_t> row, const std::string& column) const {
        const rcv::FieldValue f = at(row, column);
        return f.type == PrimType::Int64 ? static_cast<uint64_t>(f.i) : f.u;
    }
    [[nodiscard]] std::string str(std::span<const uint8_t> row, const std::string& column) const {
        std::string s(at(row, column).bytes);
        s.resize(s.find('\0') == std::string::npos ? s.size() : s.find('\0'));
        return s;
    }
};

/// A sender that keys each record itself.
struct KeyedSender : Sender {
    using Sender::Sender;

    std::optional<rc::Completion> write(const rc::Key& key, const std::string& value) { return await(put(key, value), 30s); }
    uint64_t put(const rc::Key& key, const std::string& value) {
        const uint64_t tok  = next++;
        rc::SendResult r = rc::SendResult::BackPressured;
        while (r == rc::SendResult::BackPressured)
            r = prod->trySend(*topic, key, std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(value.data()), value.size()), *cq, tok);
        EXPECT_EQ(r, rc::SendResult::Ok);
        prod->flush();
        return tok;
    }
    bool writeAll(const std::vector<std::pair<rc::Key, std::string>>& records) {
        std::vector<uint64_t> tokens;
        for (const auto& [key, value] : records) tokens.push_back(put(key, value));
        bool ok = true;
        for (const uint64_t tok : tokens) ok = isOk(await(tok, 60s)) && ok;
        return ok;
    }
};

/// The row of `key`, asked again until it is Ok when `within` allows (a view that is still rebuilding or a leader that moved).
rc::ViewRowResult readRow(rc::Client& c, int32_t view, std::span<const uint8_t> key, rc::ViewReadOptions opts = {}, std::chrono::seconds within = 0s) {
    rc::ViewRowResult row;
    const auto deadline = Clock::now() + within;
    do {
        try { row = c.getViewRowAndWait(view, key, opts, 10s); } catch (const std::exception&) {}
        if (row.status == rc::ViewRowResult::kOk) break;
        std::this_thread::sleep_for(50ms);
    } while (Clock::now() < deadline);
    return row;
}

/// Every page of the scan of `prefix`; nullopt when a page does not come back Ok within `within`.
std::optional<std::vector<std::vector<uint8_t>>> scanRows(rc::Client& c, int32_t view, std::span<const uint8_t> prefix, uint16_t limit = 0, std::chrono::seconds within = 0s) {
    const auto deadline = Clock::now() + within;
    do {
        std::vector<std::vector<uint8_t>> rows;
        rc::ViewScanOptions opts;
        bool ok = true;
        for (int page = 0; page < 10'000 && ok; ++page) {
            rc::ViewScanResult r;
            try { r = c.scanViewAndWait(view, prefix, limit, opts, 10s); } catch (const std::exception&) {}
            ok = r.status == rc::ViewRowResult::kOk;
            if (!ok) break;
            for (auto& row : r.rows) rows.push_back(std::move(row));
            if (r.nextToken.empty()) return rows;
            opts.resumeToken = std::move(r.nextToken);
        }
        std::this_thread::sleep_for(50ms);
    } while (Clock::now() < deadline);
    return std::nullopt;
}

std::set<uint64_t> idsOf(const ViewColumns& cols, const std::vector<std::vector<uint8_t>>& rows) {
    std::set<uint64_t> ids;
    for (const auto& r : rows) ids.insert(cols.u(r, "characterId"));
    return ids;
}

}  // namespace

TEST(CheckedWritesBrokerRegression, CompositeViewPointAndPrefixReadsRightAfterTheAckShardRegression) {
    OneBroker b("cw07-comp", OneBroker::Runtime::Shard);
    awaitLevel(b.node());
    ASSERT_FALSE(HasFatalFailure());
    rc::Client& c = *b.connectClient();
    const auto t  = defineCharacters(c);
    ASSERT_EQ(t.errorCode, 0) << t.detail;
    const int32_t view = createView(c, accountCharactersPlan(t.topicId), "characterId");
    ASSERT_GE(view, 0);
    const ViewColumns cols(b.node(), "account_characters");
    rcv::ViewKeyBuilder builder = keyBuilder(b.node(), "account_characters");
    KeyedSender w(b.clientConfig(), "character", t.topicId, 1, kAllView);

    std::set<uint64_t> acked42;
    const std::array<std::array<uint64_t, 2>, 4> sends = {{{1, 42}, {2, 42}, {3, 43}, {4, 42}}};
    for (const auto& [id, user] : sends) {
        const std::string name = "char" + std::to_string(id);
        ASSERT_TRUE(isOk(w.write(rc::Key(id), characterValue(id, user, name))));
        if (user == 42) acked42.insert(id);

        builder.clear();
        const auto row = readRow(c, view, bytesOf(builder.setUInt("userId", user).setUInt("characterId", id).key()));
        ASSERT_EQ(row.status, rc::ViewRowResult::kOk) << "character " << id;
        EXPECT_EQ(cols.str(row.rowBytes, "name"), name);

        builder.clear();
        const auto scan = c.scanViewAndWait(view, bytesOf(builder.setUInt("userId", 42).prefix()), 0, {}, 10s);
        EXPECT_TRUE(scan.complete());
        EXPECT_EQ(idsOf(cols, scan.rows), acked42) << "after character " << id;
        EXPECT_EQ(scan.rows.size(), acked42.size());
    }
}

TEST(CheckedWritesBrokerRegression, LowerKeyReadWithTheKeyBuilderShardRegression) {
    OneBroker b("cw07-lower", OneBroker::Runtime::Shard);
    awaitLevel(b.node());
    ASSERT_FALSE(HasFatalFailure());
    rc::Client& c = *b.connectClient();
    const auto t  = defineCharacters(c);
    ASSERT_EQ(t.errorCode, 0) << t.detail;
    const int32_t view = createView(c, nameKeyPlan(t.topicId), "characterId");
    ASSERT_GE(view, 0);
    const ViewColumns cols(b.node(), "name_key");
    rcv::ViewKeyBuilder builder = keyBuilder(b.node(), "name_key");
    KeyedSender w(b.clientConfig(), "character", t.topicId, 1, kAllView);

    ASSERT_TRUE(isOk(w.write(rc::Key(uint64_t{9}), characterValue(9, 42, "FoO"))));
    const auto row = readRow(c, view, bytesOf(builder.setString("name", "foo").setUInt("characterId", 9).key()));
    ASSERT_EQ(row.status, rc::ViewRowResult::kOk);
    EXPECT_EQ(cols.str(row.rowBytes, "nameKey"), "foo");
    EXPECT_EQ(cols.str(row.rowBytes, "name"), "FoO");
}

TEST(CheckedWritesBrokerRegression, RenameMovesTheRowShardRegression) {
    OneBroker b("cw07-rename", OneBroker::Runtime::Shard);
    awaitLevel(b.node());
    ASSERT_FALSE(HasFatalFailure());
    rc::Client& c = *b.connectClient();
    const auto t  = defineCharacters(c);
    ASSERT_EQ(t.errorCode, 0) << t.detail;
    const int32_t view = createView(c, nameKeyPlan(t.topicId), "characterId");
    ASSERT_GE(view, 0);
    rcv::ViewKeyBuilder builder = keyBuilder(b.node(), "name_key");
    KeyedSender w(b.clientConfig(), "character", t.topicId, 1, kAllView);

    const auto keyOf = [&](const char* name) { builder.clear(); return bytesOf(builder.setString("name", name).setUInt("characterId", 7).key()); };
    ASSERT_TRUE(isOk(w.write(rc::Key(uint64_t{7}), characterValue(7, 42, "Alpha"))));
    EXPECT_EQ(readRow(c, view, keyOf("alpha")).status, rc::ViewRowResult::kOk);
    ASSERT_TRUE(isOk(w.write(rc::Key(uint64_t{7}), characterValue(7, 42, "Bravo"))));
    EXPECT_EQ(readRow(c, view, keyOf("alpha")).status, kViewAbsentKey) << "the old key is gone right after the ack";
    EXPECT_EQ(readRow(c, view, keyOf("bravo")).status, rc::ViewRowResult::kOk);
}

TEST(CheckedWritesBrokerRegression, ScanAcrossPagesThenCursorInvalidatedShardRegression) {
    OneBroker b("cw07-pages", OneBroker::Runtime::Shard);
    awaitLevel(b.node());
    ASSERT_FALSE(HasFatalFailure());
    rc::Client& c = *b.connectClient();
    const auto t  = defineCharacters(c);
    ASSERT_EQ(t.errorCode, 0) << t.detail;
    const int32_t view = createView(c, accountCharactersPlan(t.topicId), "characterId");
    ASSERT_GE(view, 0);
    const ViewColumns cols(b.node(), "account_characters");
    rcv::ViewKeyBuilder builder = keyBuilder(b.node(), "account_characters");
    KeyedSender w(b.clientConfig(), "character", t.topicId, 1, kAllView);

    constexpr uint64_t kTotal = 300;
    std::vector<std::pair<rc::Key, std::string>> records;
    for (uint64_t id = 1; id <= kTotal; ++id) records.emplace_back(rc::Key(id), characterValue(id, 42, "c" + std::to_string(id)));
    ASSERT_TRUE(w.writeAll(records));
    const std::vector<uint8_t> prefix = bytesOf(builder.setUInt("userId", 42).prefix());

    std::vector<std::size_t> sizes;
    std::set<uint64_t> seen;
    rc::ViewScanOptions opts;
    rc::ViewScanResult page;
    for (int i = 0; i < 10; ++i) {
        page = c.scanViewAndWait(view, prefix, 100, opts, 10s);
        ASSERT_EQ(page.status, rc::ViewRowResult::kOk);
        sizes.push_back(page.rows.size());
        for (const uint64_t id : idsOf(cols, page.rows)) seen.insert(id);
        if (page.complete()) break;
        opts.resumeToken = page.nextToken;
    }
    ASSERT_TRUE(page.complete());
    ASSERT_GE(sizes.size(), 3u);
    EXPECT_EQ(sizes[0], 100u);
    EXPECT_EQ(sizes[1], 100u);
    EXPECT_EQ(sizes[2], 100u);
    EXPECT_EQ(seen.size(), kTotal) << "the pages together hold every character once";

    const auto first = c.scanViewAndWait(view, prefix, 100, {}, 10s);
    ASSERT_EQ(first.status, rc::ViewRowResult::kOk);
    ASSERT_FALSE(first.nextToken.empty());
    const uint64_t lastOnPage = cols.u(first.rows.back(), "characterId");
    ASSERT_TRUE(isOk(w.write(rc::Key(lastOnPage), {})));
    rc::ViewScanOptions resume;
    resume.resumeToken = first.nextToken;
    const auto stale = c.scanViewAndWait(view, prefix, 100, resume, 10s);
    EXPECT_EQ(stale.status, rc::ViewRowResult::kCursorInvalidated);

    const auto again = scanRows(c, view, prefix, 100);
    ASSERT_TRUE(again.has_value());
    EXPECT_EQ(again->size(), kTotal - 1);
}

TEST(CheckedWritesBrokerRegression, MinOffsetReadSeesTheWriteShardRegression) {
    OneBroker b("cw07-minoff", OneBroker::Runtime::Shard);
    awaitLevel(b.node());
    ASSERT_FALSE(HasFatalFailure());
    rc::Client& c = *b.connectClient();
    const auto t  = defineCharacters(c);
    ASSERT_EQ(t.errorCode, 0) << t.detail;
    const int32_t view = createView(c, accountCharactersPlan(t.topicId), "characterId");
    ASSERT_GE(view, 0);
    rcv::ViewKeyBuilder builder = keyBuilder(b.node(), "account_characters");
    KeyedSender w(b.clientConfig(), "character", t.topicId, 1, rc::Acks::All);

    for (uint64_t id = 1; id <= 20; ++id) {
        const auto ack = w.write(rc::Key(id), characterValue(id, 42, "c" + std::to_string(id)));
        ASSERT_TRUE(isOk(ack));
        builder.clear();
        const auto row = readRow(c, view, bytesOf(builder.setUInt("userId", 42).setUInt("characterId", id).key()), {.minOffset = ack->value});
        EXPECT_EQ(row.status, rc::ViewRowResult::kOk) << "character " << id << " at offset " << ack->value;
    }
}

TEST(CheckedWritesBrokerRegression, WideViewSurvivesRestartShardRegression) {
    LogCapture log;
    OneBroker b("cw07-restart", OneBroker::Runtime::Shard);
    awaitLevel(b.node());
    ASSERT_FALSE(HasFatalFailure());
    rc::Client* c = b.connectClient();
    const auto t  = defineCharacters(*c);
    ASSERT_EQ(t.errorCode, 0) << t.detail;
    const int32_t view     = createView(*c, nameKeyPlan(t.topicId), "characterId");
    const int32_t byUser   = createView(*c, accountCharactersPlan(t.topicId), "characterId");
    ASSERT_GE(view, 0);
    ASSERT_GE(byUser, 0);
    rcv::ViewKeyBuilder builder = keyBuilder(b.node(), "name_key");
    rcv::ViewKeyBuilder users   = keyBuilder(b.node(), "account_characters");

    constexpr uint64_t kTotal = 1000;
    std::vector<std::string> names(kTotal);
    {
        KeyedSender w(b.clientConfig(), "character", t.topicId, 1, kAllView);
        std::vector<std::pair<rc::Key, std::string>> records;
        for (uint64_t id = 0; id < kTotal; ++id) {
            names[id] = "Char" + std::to_string(id);
            records.emplace_back(rc::Key(id), characterValue(id, id % 10, names[id]));
        }
        ASSERT_TRUE(w.writeAll(records));
        records.clear();
        for (uint64_t id = 0; id < kTotal; id += 5) {
            names[id] = "Moved" + std::to_string(id);
            records.emplace_back(rc::Key(id), characterValue(id, id % 10, names[id]));
        }
        ASSERT_TRUE(w.writeAll(records));
    }

    const auto keyOf = [&](uint64_t id) { builder.clear(); return bytesOf(builder.setString("name", names[id]).setUInt("characterId", id).key()); };
    const auto snapshot = [&](rc::Client& client) {
        std::map<uint64_t, std::vector<uint8_t>> rows;
        for (uint64_t id = 0; id < kTotal; ++id) {
            const auto row = readRow(client, view, keyOf(id), {}, 30s);
            EXPECT_EQ(row.status, rc::ViewRowResult::kOk) << "character " << id;
            rows[id] = row.rowBytes;
        }
        std::vector<std::vector<uint8_t>> scanned;
        for (uint64_t user = 0; user < 10; ++user) {
            users.clear();
            auto page = scanRows(client, byUser, bytesOf(users.setUInt("userId", user).prefix()), 0, 30s);
            EXPECT_TRUE(page.has_value()) << "user " << user;
            for (auto& row : page.value_or(std::vector<std::vector<uint8_t>>{})) scanned.push_back(std::move(row));
        }
        std::sort(scanned.begin(), scanned.end());
        return std::make_pair(rows, scanned);
    };
    const auto before = snapshot(*c);
    EXPECT_EQ(before.second.size(), kTotal);

    b.restart();
    c = b.connectClient();
    const auto after = snapshot(*c);
    EXPECT_TRUE(after.first == before.first) << "every point read is the same after the restart";
    EXPECT_TRUE(after.second == before.second) << "the scan is the same after the restart";

    const std::string text = log.all();
    const std::string needle = "VIEW-RECOVERED view=" + std::to_string(view) + " partition=0";
    const auto at = text.find(needle);
    ASSERT_NE(at, std::string::npos) << "the view was recovered from its checkpoint";
    EXPECT_NE(text.substr(at, text.find('\n', at) - at).find("format=v3"), std::string::npos);
}

TEST(CheckedWritesBrokerRegression, WideViewReadsAfterLeaderCrashThreeNodesRegression) {
    using namespace rune::node::test::catalog_cluster;
    Cluster c = startCluster("cw07");
    ASSERT_TRUE(c.awaitCatalogLeader(30s));
    const int32_t ctl = awaitController(c);
    ASSERT_GT(ctl, 0);
    for (const int id : c.runningNodes()) awaitLevel(c.node(id));
    ASSERT_FALSE(HasFatalFailure());
    auto admin = connectClient(c, quickConfig(c, ctl));
    const auto src = defineCharacters(*admin, 3);
    ASSERT_EQ(src.errorCode, 0) << src.detail;
    const int32_t view = createView(*admin, accountCharactersPlan(src.topicId), "characterId");
    ASSERT_GE(view, 0);
    admin->close();
    admin.reset();
    std::vector<int32_t> leaders;
    ASSERT_TRUE(awaitLeaders(c, src.topicId, 1, leaders, 30s));
    const int32_t victim = leaders[0];
    int32_t seed = 0;
    for (const int id : c.runningNodes()) if (id != victim) { seed = id; break; }
    ASSERT_GT(seed, 0);
    awaitViewApplied(c.node(seed), "account_characters");
    ASSERT_FALSE(HasFatalFailure());
    const ViewColumns cols(c.node(seed), "account_characters");
    rcv::ViewKeyBuilder builder = keyBuilder(c.node(seed), "account_characters");

    constexpr uint64_t kTotal = 200;
    rc::ClientConfig cfg = quickConfig(c, seed);
    cfg.connectTimeoutMs = cfg.pingTimeoutMs = 30'000;
    {
        KeyedSender w(cfg, "character", src.topicId, 1, kAllView);
        std::vector<std::pair<rc::Key, std::string>> records;
        for (uint64_t id = 0; id < kTotal; ++id) records.emplace_back(rc::Key(id), characterValue(id, id % 4, "c" + std::to_string(id)));
        ASSERT_TRUE(w.writeAll(records));
    }

    c.crash(victim);
    ASSERT_TRUE(awaitLeaders(c, src.topicId, 1, leaders, 60s));
    auto reader = connectClient(c, cfg);
    for (uint64_t id = 0; id < kTotal; ++id) {
        builder.clear();
        const auto row = readRow(*reader, view, bytesOf(builder.setUInt("userId", id % 4).setUInt("characterId", id).key()), {}, 60s);
        EXPECT_EQ(row.status, rc::ViewRowResult::kOk) << "character " << id;
    }
    for (uint64_t user = 0; user < 4; ++user) {
        builder.clear();
        const auto rows = scanRows(*reader, view, bytesOf(builder.setUInt("userId", user).prefix()), 0, 60s);
        ASSERT_TRUE(rows.has_value()) << "user " << user;
        EXPECT_EQ(rows->size(), kTotal / 4) << "user " << user;
        EXPECT_EQ(idsOf(cols, *rows).size(), kTotal / 4);
    }
    reader->close();
}

TEST(CheckedWritesBrokerRegression, MultiPartitionNarrowKeyViewReadsWithThePartitionShardRegression) {
    using Rec = KeyRec<uint16_t>;
    OneBroker b("cw07-parts", OneBroker::Runtime::Shard);
    awaitLevel(b.node());
    ASSERT_FALSE(HasFatalFailure());
    rc::Client& c = *b.connectClient();
    const auto t  = c.defineTopicAndWait("narrow", kKiParts, "k", rune::codec::serialize(Rec::descriptor()), rc::Persistence::Disk, {}, {}, 60s);
    ASSERT_EQ(t.errorCode, 0) << t.detail;
    rv::ViewPlan plan;
    plan.viewName = "narrow_v";
    plan.sources.push_back(rv::SourceSpec{t.topicId, "k"});
    plan.keyComponents = {{"k", 0, "k"}};
    plan.output.push_back(rv::AuthoredField{"n", PrimType::Int64, 0, "n", rv::MergeRule::Replace});
    const int32_t view = createView(c, plan, "k");
    ASSERT_GE(view, 0);
    rcv::ViewKeyBuilder builder = keyBuilder(b.node(), "narrow_v");
    KeyedSender w(b.clientConfig(), "narrow", t.topicId, kKiParts, kAllView);

    constexpr int kTotal = 64;
    std::vector<std::pair<rc::Key, std::string>> records;
    std::vector<uint16_t> keys;
    for (int i = 1; i <= kTotal; ++i) {
        const Rec rec = Rec::make(i);
        keys.push_back(rec.k);
        std::string value(rec.encodedSize(), '\0');
        (void)rec.encode(reinterpret_cast<uint8_t*>(value.data()));
        records.emplace_back(rec.routeKey(), std::move(value));
    }
    ASSERT_TRUE(w.writeAll(records));

    for (const uint16_t k : keys) {
        builder.clear();
        const std::vector<uint8_t> key = bytesOf(builder.setUInt("k", k).key());
        const auto row = readRow(c, view, key, {.partition = builder.partition()});
        EXPECT_EQ(row.status, rc::ViewRowResult::kOk) << "key " << k << " on partition " << builder.partition();
        EXPECT_EQ(c.getViewRowAndWait(view, uint64_t{k}, 0, 10s).status, kViewInvalidRequest) << "op 72 on a multi-partition view";
    }
}

TEST(CheckedWritesBrokerRegression, OldGetViewRowOnALegacyViewUnchangedShardRegression) {
    OneBroker b("cw07-legacy", OneBroker::Runtime::Shard);
    awaitLevel(b.node());
    ASSERT_FALSE(HasFatalFailure());
    rc::Client& c = *b.connectClient();
    const auto t  = defineCharacters(c);
    ASSERT_EQ(t.errorCode, 0) << t.detail;
    rv::ViewPlan plan = characterPlan("legacy_v", t.topicId, {}, {"characterId", "userId", "gender", "lastPlayed"});
    plan.key = "characterId";
    const int32_t view = createView(c, plan, "characterId");
    ASSERT_GE(view, 0);
    rcv::ViewKeyBuilder builder = keyBuilder(b.node(), "legacy_v");
    KeyedSender w(b.clientConfig(), "character", t.topicId, 1, kAllView);

    ASSERT_TRUE(isOk(w.write(rc::Key(uint64_t{42}), characterValue(42, 7, "legacy"))));
    const auto old = c.getViewRowAndWait(view, uint64_t{42}, 0, 10s);
    ASSERT_EQ(old.status, rc::ViewRowResult::kOk);
    const auto keyed = readRow(c, view, bytesOf(builder.setUInt("characterId", 42).key()));
    ASSERT_EQ(keyed.status, rc::ViewRowResult::kOk);
    EXPECT_TRUE(keyed.rowBytes == old.rowBytes) << "op 88 and op 72 answer the same row bytes";
}

TEST(CheckedWritesBrokerRegression, DeviceHistoryGroupByPrefixReadShardRegression) {
    OneBroker b("cw07-device", OneBroker::Runtime::Shard);
    awaitLevel(b.node());
    ASSERT_FALSE(HasFatalFailure());
    rc::Client* c = b.connectClient();
    const auto device = DescriptorBuilder("device", 1).add("eventId", PrimType::UInt64, 1, true).add("userId", PrimType::UInt64)
                            .addFixedStr("deviceFp", 32).add("lastSeen", PrimType::Int64).build();
    const auto t = c->defineTopicAndWait("device", 1, "eventId", rune::codec::serialize(device), rc::Persistence::Disk, {}, {}, 60s);
    ASSERT_EQ(t.errorCode, 0) << t.detail;
    rv::ViewPlan plan;
    plan.viewName = "device_history";
    plan.sources.push_back(rv::SourceSpec{t.topicId, "eventId"});
    plan.keyComponents = plan.groupBy = {{"userId", 0, "userId"}, {"deviceFp", 0, "deviceFp"}};
    plan.output.push_back(rv::AuthoredField{"uses", PrimType::Int64, 0, "", rv::MergeRule::CountAll});
    plan.output.push_back(rv::AuthoredField{"lastSeen", PrimType::Int64, 0, "lastSeen", rv::MergeRule::Max});
    const int32_t view = createView(*c, plan, "userId");
    ASSERT_GE(view, 0);
    const ViewColumns cols(b.node(), "device_history");
    rcv::ViewKeyBuilder builder = keyBuilder(b.node(), "device_history");

    const std::array<std::array<uint64_t, 3>, 4> devices = {{{42, 'A', 5}, {42, 'B', 2}, {42, 'C', 1}, {43, 'A', 1}}};
    {
        KeyedSender w(b.clientConfig(), "device", t.topicId, 1, kAllView);
        std::vector<std::pair<rc::Key, std::string>> records;
        uint64_t event = 0;
        for (const auto& [user, fp, events] : devices)
            for (uint64_t i = 0; i < events; ++i) {
                std::string padded = "fp" + std::string(1, static_cast<char>(fp));
                padded.resize(32, '\0');
                ++event;
                records.emplace_back(rc::Key(event), le(56, 2) + le(1, 2) + le(event, 8) + le(user, 8) + padded + le(event, 8));
            }
        ASSERT_TRUE(w.writeAll(records));
    }

    const auto uses = [&](rc::Client& client) {
        builder.clear();
        const auto rows = scanRows(client, view, bytesOf(builder.setUInt("userId", 42).prefix()), 0, 30s);
        std::map<std::string, uint64_t> out;
        if (!rows) { ADD_FAILURE() << "the prefix scan"; return out; }
        for (const auto& r : *rows) out[cols.str(r, "deviceFp")] = cols.u(r, "uses");
        return out;
    };
    const std::map<std::string, uint64_t> want = {{"fpA", 5}, {"fpB", 2}, {"fpC", 1}};
    EXPECT_EQ(uses(*c), want);
    b.restart();
    c = b.connectClient();
    EXPECT_EQ(uses(*c), want) << "after the restart";
}

TEST(CheckedWritesBrokerRegression, KeyIndexOnASixteenByteKeyShardRegression) {
    using Wide = KeyRec<std::array<uint8_t, 16>>;
    OneBroker b("cw07-ki16", OneBroker::Runtime::Shard);
    awaitLevel(b.node());
    ASSERT_FALSE(HasFatalFailure());
    rc::Client& c = *b.connectClient();
    rc::TopicOptions to;
    to.config.cleanupPolicy = 1;
    to.keyFeatures.keyIndex = true;
    const auto t = c.defineTopicAndWait("topic16", kKiParts, "k", rune::codec::serialize(Wide::descriptor()), rc::Persistence::Disk, to, {}, 60s);
    ASSERT_EQ(t.errorCode, 0) << t.detail;
    rc::Producer prod = c.producer(KiWriter::config(kAllView));
    rc::TypedTopic<Wide, rc::ProducerRef> topic = c.openTopicAndWait<Wide>("topic16", prod, 30s);

    EXPECT_TRUE(topic.insertAndWait(Wide::make(1)).ok());
    const auto again = topic.insertAndWait(Wide::make(1));
    EXPECT_EQ(again.status(), rc::OpStatus::BrokerError);
    EXPECT_EQ(again.code(), static_cast<uint16_t>(rune::protocol::client::ErrorCode::KeyExists));
    prod.closeImmediately();
}

// A producer-id request in flight when the node that coordinates it crashes is answered by the new coordinator after about two stale-session
// thresholds, not after the 5 s coordinator-down fallback.  The coordinator is not the seed, so one of the two other nodes is it.
namespace {
void producerIdAcrossCoordinatorCrash(int victim) {
    Cluster c = startCluster("cw_pid" + std::to_string(victim));
    ASSERT_TRUE(c.awaitCatalogLeader(30s));
    rc::ClientConfig cfg = quickConfig(c, 1);
    cfg.connectTimeoutMs = cfg.pingTimeoutMs = 30'000;
    rc::Client client(cfg);
    rc::ProducerConfig pc;
    pc.acks = kAllView;
    rc::Producer prod = client.producer(pc);
    client.connectAndWait(30s);
    c.crash(victim);
    const auto start = std::chrono::steady_clock::now();
    while (prod.producerId() < 0 && std::chrono::steady_clock::now() - start < 10s) std::this_thread::sleep_for(1ms);
    EXPECT_GE(prod.producerId(), 0);
    EXPECT_LT(std::chrono::steady_clock::now() - start, 3s) << "the producer id waited for the coordinator-down fallback";
    prod.closeImmediately();
    client.close();
}
}  // namespace

TEST(CheckedWritesBrokerRegression, ProducerIdAcrossACoordinatorCrashNode2ThreeNodesRegression) { producerIdAcrossCoordinatorCrash(2); }
TEST(CheckedWritesBrokerRegression, ProducerIdAcrossACoordinatorCrashNode3ThreeNodesRegression) { producerIdAcrossCoordinatorCrash(3); }
