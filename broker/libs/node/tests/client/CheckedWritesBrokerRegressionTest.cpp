// CheckedWritesBrokerRegressionTest.cpp — checked writes Part 02: typed validation against REAL brokers and the REAL
// client on the default (shard) runtime.  Worker state is read only inside onOwnerThreadForTest (a direct read from
// the test thread would race the worker).
#include "client/OneBroker.h"
#include "client/CheckedWritesBrokerSupport.h"

#include <gtest/gtest.h>

#include <atomic>
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
