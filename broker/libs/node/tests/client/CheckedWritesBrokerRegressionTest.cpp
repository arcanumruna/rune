// CheckedWritesBrokerRegressionTest.cpp — checked writes Part 02: typed validation against REAL brokers and the REAL
// client on the default (shard) runtime.  Worker state is read only inside onOwnerThreadForTest (a direct read from
// the test thread would race the worker).
#include "client/OneBroker.h"
#include "client/CheckedWritesBrokerSupport.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <memory>
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
#include "client/ClusterClient.h"
#include "support/ShardTestClient.h"

#include <rune/cli/Cli.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/storage/Log.h>

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
    ThreeNodes c = ThreeNodes::start("cw_failover");
    ASSERT_TRUE(c.awaitCatalogLeader(30s));
    const int32_t ctl = awaitController(c);
    ASSERT_GT(ctl, 0);
    auto admin = connectClient(c, ctl);
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
            code = cwb::produceRaw(clientConfigFor(c, seed), "p", t.topicId, 1, r.value, keyOf(r.key));
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
    std::unique_ptr<rc::Client>      client;
    std::optional<rc::Producer>      prod;
    std::unique_ptr<rc::Completions> cq;
    std::optional<rc::TopicHandle>   topic;
    uint64_t                         next = 1;

    Sender(rc::ClientConfig cfg, const std::string& name, int32_t topicId, int32_t parts, rc::Acks acks, const std::string& txnId = {}) {
        cfg.topics.push_back(rc::TopicSpec{name, topicId, parts});
        client = std::make_unique<rc::Client>(cfg);
        rc::ProducerConfig pc;
        pc.acks            = acks;
        pc.lingerUs        = 1;
        pc.transactionalId = txnId;
        prod.emplace(client->producer(pc));
        client->connectAndWait(30s);
        cq = std::make_unique<rc::Completions>(*client);
        topic.emplace(client->topic(name));
    }
    ~Sender() {
        if (prod) prod->closeImmediately();
        if (client) client->close();
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
    for (int i = 0; i < 800 && !raised; ++i) {
        raised = n.onWorkerThreadForTest(0, [](Worker& w) { return w.clusterFeatureLevelForTest(); }) >= rune::replication::MetadataFeature::kCheckedWrites;
        if (!raised) std::this_thread::sleep_for(25ms);
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
    ThreeNodes c = ThreeNodes::start("cw03");
    ASSERT_TRUE(c.awaitCatalogLeader(30s));
    const int32_t ctl = awaitController(c);
    ASSERT_GT(ctl, 0);
    auto admin = connectClient(c, ctl);
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

    rc::ClientConfig cfg = clientConfigFor(c, seed);
    cfg.connectTimeoutMs = 30'000;
    cfg.pingTimeoutMs    = 30'000;
    Sender s(cfg, "acct", src.topicId, 1, kAllView);
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

    rc::ClientConfig rcfg = clientConfigFor(c, seed);
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
    for (int i = 0; i < 1200 && !logged; ++i) {
        logged = node.onWorkerThreadForTest(w, [topicId](Worker& wk) {
            const PartitionWriteState* ws = wk.writeStateForTest(topicId, 0);
            return ws != nullptr && ws->constraintSets.latest().count != 0;
        });
        if (!logged) std::this_thread::sleep_for(50ms);
    }
    ASSERT_TRUE(logged) << "the TestReject set change was never logged";
}

/// A typed {id, v, tag} topic `vt` (one partition) with TagEquals{9} (object 7) installed on the worker that serves it.
int32_t defineVerdictSource(OneBroker& b) {
    rc::Client& c = *b.connectClient();
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
    ThreeNodes c = ThreeNodes::start("cw04");
    ASSERT_TRUE(c.awaitCatalogLeader(30s));
    const int32_t ctl = awaitController(c);
    ASSERT_GT(ctl, 0);
    auto admin = connectClient(c, ctl);
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

    rc::ClientConfig cfg = clientConfigFor(c, seed);
    cfg.connectTimeoutMs = 30'000;
    cfg.pingTimeoutMs    = 30'000;
    Sender s(cfg, "vt", src.topicId, 1, kAllView);
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
    rc::ClientConfig rcfg = clientConfigFor(c, leaders[0]);
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
    ThreeNodes c = ThreeNodes::start("cw04r");
    ASSERT_TRUE(c.awaitCatalogLeader(30s));
    const int32_t ctl = awaitController(c);
    ASSERT_GT(ctl, 0);
    auto admin = connectClient(c, ctl);
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
        rc::ClientConfig cfg = clientConfigFor(c, 1);
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

    rc::ClientConfig rcfg = clientConfigFor(c, leaders[0]);
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
