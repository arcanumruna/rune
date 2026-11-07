// CheckedWritesQlBrokerRegressionTest.cpp - checked writes Part 08 Task 10: the REAL `rune` CLI (rune::cli::run) against REAL
// brokers on the shard runtime.  The RegressorsPath DDL (unique and check views included), select, show, stats and
// lower() literals.  Every wait is bounded.
#include "client/ClusterClient.h"
#include "client/OneBroker.h"
#include "client/support/RegressorsPathRecords.h"
#include "support/CatalogBrokerCluster.h"

#include <QlEngine.h>
#include <rune/cli/Cli.h>
#include <rune/client/Client.h>
#include <rune/client/producer/Producer.h>
#include <rune/ql/Backends.h>
#include <rune/ql/Parser.h>

#include <gtest/gtest.h>

#include <chrono>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <map>
#include <set>
#include <sstream>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

using namespace std::chrono_literals;
using namespace rune::node;
using namespace rune::node::test;
namespace rc = rune::client;
namespace fs = std::filesystem;

namespace {

constexpr uint16_t kKeyExists = 56;
constexpr rc::Acks kAllView   = rc::Acks::All | rc::Acks::View;

/// Every broker here runs the shard runtime: its client stream is catalog_cluster::kClientStream.
int runCli(std::vector<std::string> args, std::string* out = nullptr, std::string* err = nullptr) {
    args.insert(args.end(), {"--cli-stream-in", std::to_string(catalog_cluster::kClientStream)});
    std::stringstream o, e;
    const int code = rune::cli::run(std::move(args), o, e);
    if (out) *out = o.str();
    if (err) *err = e.str();
    return code;
}

/// `rune ql -e <text> --broker <endpoint>`
int ql(const std::string& endpoint, const std::string& text, std::string* out = nullptr, std::string* err = nullptr) {
    return runCli({"ql", "-e", text, "--broker", endpoint}, out, err);
}

struct TmpDir {
    fs::path path = fs::temp_directory_path() / ("rune_cwql_broker_test_" + std::to_string(::getpid()));
    TmpDir() { fs::create_directories(path); }
    ~TmpDir() {
        std::error_code ec;
        fs::remove_all(path, ec);
    }
};

fs::path tmpDir() {
    static const TmpDir d;
    return d.path;
}

std::string writeTemp(const std::string& name, const std::string& content) {
    const fs::path p = tmpDir() / name;
    std::ofstream o(p, std::ios::binary);
    o << content;
    return p.string();
}

template <class Pred>
bool waitFor(Pred&& pred, std::chrono::seconds within) {
    for (const auto deadline = std::chrono::steady_clock::now() + within; std::chrono::steady_clock::now() < deadline; std::this_thread::sleep_for(20ms))
        if (pred()) return true;
    return pred();
}

void awaitLevel(BrokerNode& n) {
    bool raised = false;
    for (int i = 0; i < 800 && !raised; ++i) {
        raised = n.onWorkerThreadForTest(0, [](Worker& w) { return w.clusterFeatureLevelForTest(); }) >= rune::replication::MetadataFeature::kCheckedWrites;
        if (!raised) std::this_thread::sleep_for(25ms);
    }
    ASSERT_TRUE(raised) << "the cluster feature level reaches kCheckedWrites";
}

std::string clusterEndpoint(catalog_cluster::ThreeNodes& c, int id) {
    return "127.0.0.1:" + std::to_string(c.ports.client[static_cast<std::size_t>(id - 1)]);
}

// ---- record types of the QL-created topics (what `rune schema generate` would emit; wire = the codec's fixed block) ----

void putLe(uint8_t* b, std::size_t off, uint64_t v, int bytes) {
    for (int i = 0; i < bytes; ++i) b[off + static_cast<std::size_t>(i)] = static_cast<uint8_t>(v >> (8 * i));
}
void putStr(uint8_t* b, std::size_t off, const std::string& s, std::size_t width) {
    std::memset(b + off, 0, width);
    std::memcpy(b + off, s.data(), s.size() < width ? s.size() : width);
}

// topic device_login { loginId uint64 autoincrement, userId uint64, deviceFp string(64), platform uint16, ts int64 } key loginId
rune::codec::Descriptor deviceLoginDescriptor() {
    return rune::codec::DescriptorBuilder("device_login", 1)
        .add("loginId", rune::codec::PrimType::UInt64, 1, /*isKey=*/true)
        .add("userId", rune::codec::PrimType::UInt64)
        .addFixedStr("deviceFp", 64)
        .add("platform", rune::codec::PrimType::UInt16)
        .add("ts", rune::codec::PrimType::Int64)
        .build();
}
struct DeviceLogin {
    static const uint64_t kFingerprint;
    static constexpr std::string_view kSchemaName = "device_login";
    static constexpr uint16_t kVersion = 1, kBlockLength = 90;
    uint64_t    loginId = 0;
    uint64_t    userId = 0;
    std::string deviceFp;
    uint16_t    platform = 0;
    int64_t     ts = 0;
    [[nodiscard]] std::size_t encodedSize() const noexcept { return 4 + kBlockLength; }
    [[nodiscard]] std::size_t encode(uint8_t* b) const noexcept {
        putLe(b, 0, kBlockLength, 2);
        putLe(b, 2, kVersion, 2);
        putLe(b, 4, loginId, 8);
        putLe(b, 12, userId, 8);
        putStr(b, 20, deviceFp, 64);
        putLe(b, 84, platform, 2);
        putLe(b, 86, static_cast<uint64_t>(ts), 8);
        return encodedSize();
    }
    [[nodiscard]] uint64_t key() const noexcept { return loginId; }
};
const uint64_t DeviceLogin::kFingerprint = rune::codec::fingerprint(deviceLoginDescriptor());

// topic character { characterId uint64 autoincrement, userId uint64, name string(16), createdAt int64 } key characterId
rune::codec::Descriptor characterDescriptor() {
    return rune::codec::DescriptorBuilder("character", 1)
        .add("characterId", rune::codec::PrimType::UInt64, 1, /*isKey=*/true)
        .add("userId", rune::codec::PrimType::UInt64)
        .addFixedStr("name", 16)
        .add("createdAt", rune::codec::PrimType::Int64)
        .build();
}
struct Character {
    static const uint64_t kFingerprint;
    static constexpr std::string_view kSchemaName = "character";
    static constexpr uint16_t kVersion = 1, kBlockLength = 40;
    uint64_t    characterId = 0;
    uint64_t    userId = 0;
    std::string name;
    int64_t     createdAt = 0;
    [[nodiscard]] std::size_t encodedSize() const noexcept { return 4 + kBlockLength; }
    [[nodiscard]] std::size_t encode(uint8_t* b) const noexcept {
        putLe(b, 0, kBlockLength, 2);
        putLe(b, 2, kVersion, 2);
        putLe(b, 4, characterId, 8);
        putLe(b, 12, userId, 8);
        putStr(b, 20, name, 16);
        putLe(b, 36, static_cast<uint64_t>(createdAt), 8);
        return encodedSize();
    }
    [[nodiscard]] uint64_t key() const noexcept { return characterId; }
};
const uint64_t Character::kFingerprint = rune::codec::fingerprint(characterDescriptor());

// topic ki { id uint64, name string(16), n int64 } key id
rune::codec::Descriptor kiDescriptor() {
    return rune::codec::DescriptorBuilder("ki", 1)
        .add("id", rune::codec::PrimType::UInt64, 1, /*isKey=*/true)
        .addFixedStr("name", 16)
        .add("n", rune::codec::PrimType::Int64)
        .build();
}
struct Ki {
    static const uint64_t kFingerprint;
    static constexpr std::string_view kSchemaName = "ki";
    static constexpr uint16_t kVersion = 1, kBlockLength = 32;
    uint64_t    id = 0;
    std::string name;
    int64_t     n = 0;
    [[nodiscard]] std::size_t encodedSize() const noexcept { return 4 + kBlockLength; }
    [[nodiscard]] std::size_t encode(uint8_t* b) const noexcept {
        putLe(b, 0, kBlockLength, 2);
        putLe(b, 2, kVersion, 2);
        putLe(b, 4, id, 8);
        putStr(b, 12, name, 16);
        putLe(b, 28, static_cast<uint64_t>(n), 8);
        return encodedSize();
    }
    [[nodiscard]] uint64_t key() const noexcept { return id; }
};
const uint64_t Ki::kFingerprint = rune::codec::fingerprint(kiDescriptor());

/// A producer (acks All | View) and the typed topic `name` of record type T written through it.
template <class T>
struct Writer {
    rc::Producer                       prod;
    rc::TypedTopic<T, rc::ProducerRef> topic;

    static rc::ProducerConfig config() {
        rc::ProducerConfig cfg;
        cfg.acks     = kAllView;
        cfg.lingerUs = 1;
        return cfg;
    }
    Writer(rc::Client& c, const std::string& name) : prod(c.producer(config())), topic(c.openTopicAndWait<T>(name, prod, 30s)) {}
    ~Writer() { prod.closeImmediately(); }
};

/// The table rows of a select's stdout: header, rows, "(n rows)" - each line split on runs of blanks.
std::vector<std::vector<std::string>> cells(const std::string& out) {
    std::vector<std::vector<std::string>> lines;
    std::istringstream in(out);
    for (std::string line; std::getline(in, line);) {
        std::istringstream words(line);
        std::vector<std::string> w;
        for (std::string t; words >> t;) w.push_back(t);
        lines.push_back(std::move(w));
    }
    return lines;
}

/// `ql` until it exits 0 (a view that is still being built answers a refusal), bounded.
bool qlEventually(const std::string& endpoint, const std::string& text, std::string& out, std::string& err) {
    return waitFor([&] { return ql(endpoint, text, &out, &err) == 0; }, 30s);
}

}  // namespace

// The acceptance case: every statement of the RegressorsPath script, one `rune ql -e` each, then the whole script with `-f` on a fresh cluster.
TEST(CheckedWritesQlBrokerRegression, RegressorsPathOutcomesThreeNodesRegression) {
    using namespace rune::node::test::catalog_cluster;
    const std::string golden = std::string(RUNE_QL_GOLDEN_DIR) + "/regressorspath.rql";
    std::ifstream in(golden);
    ASSERT_TRUE(in.good());
    std::stringstream script;
    script << in.rdbuf();
    const std::vector<std::string> statements = rune::cli::engine::splitStatements(script.str());
    ASSERT_EQ(statements.size(), 7u);

    {
        ThreeNodes c = ThreeNodes::start("cw09_ql");
        ASSERT_TRUE(c.awaitCatalogLeader(30s));
        for (const int id : c.runningNodes()) awaitLevel(c.node(id));
        ASSERT_FALSE(HasFatalFailure());
        const std::string ep = clusterEndpoint(c, 1);
        for (std::size_t i = 0; i < statements.size(); ++i) {
            std::string out, err;
            EXPECT_EQ(ql(ep, statements[i], &out, &err), 0) << "statement " << i << ": " << statements[i] << "\n" << err;
            EXPECT_TRUE(err.empty()) << "statement " << i << ": " << err;
        }
        std::string out, err;
        ASSERT_EQ(ql(ep, "show topic user;", &out, &err), 0) << err;
        EXPECT_NE(out.find("partitions 1"), std::string::npos) << out;
        EXPECT_NE(out.find("key.index on"), std::string::npos) << out;
        EXPECT_NE(out.find("autoincrement on (reserve 4096, max 9007199254740991)"), std::string::npos) << out;
        EXPECT_NE(out.find("partitioner modulo"), std::string::npos) << out;
        EXPECT_NE(out.find("autoIncrementCounter 1"), std::string::npos) << out;
        ASSERT_EQ(ql(ep, "show topic character;", &out, &err), 0) << err;
        EXPECT_NE(out.find("partitions 1"), std::string::npos) << out;
        ASSERT_EQ(ql(ep, "show view device_history;", &out, &err), 0) << err;
        EXPECT_NE(out.find("keyspec (userId, deviceFp)"), std::string::npos) << out;
        EXPECT_NE(out.find("group by (userId, deviceFp)"), std::string::npos) << out;
    }

    ThreeNodes c = ThreeNodes::start("cw09_ql_f");
    ASSERT_TRUE(c.awaitCatalogLeader(30s));
    for (const int id : c.runningNodes()) awaitLevel(c.node(id));
    ASSERT_FALSE(HasFatalFailure());
    std::string out, err;
    EXPECT_EQ(runCli({"ql", "-f", golden, "--broker", clusterEndpoint(c, 1)}, &out, &err), 0) << err;
}

// The record types of the RegressorsPath tests are the golden script's topics: the fingerprints match, without a broker.
TEST(CheckedWritesQlBrokerRegression, RegressorsPathRecordFingerprintsMatchTheGoldenScript) {
    std::ifstream in(std::string(RUNE_QL_GOLDEN_DIR) + "/regressorspath.rql");
    ASSERT_TRUE(in.good());
    std::stringstream script;
    script << in.rdbuf();
    std::map<std::string, uint64_t> golden;
    for (const std::string& statement : rune::cli::engine::splitStatements(script.str())) {
        const auto parsed = rune::ql::parse(statement);
        ASSERT_TRUE(parsed.has_value()) << statement;
        if (const auto* topic = std::get_if<rune::ql::TopicDef>(&*parsed))
            golden[topic->name] = rune::codec::fingerprint(rune::ql::toDescriptor(*topic));
    }
    ASSERT_TRUE(golden.contains("user"));
    ASSERT_TRUE(golden.contains("character"));
    EXPECT_EQ(golden["user"], RpUser::kFingerprint);
    EXPECT_EQ(golden["character"], RpCharacter::kFingerprint);
}

// R08a: an aggregate over a per-login key counts every login.
TEST(CheckedWritesQlBrokerRegression, DeviceHistoryCountsLoginsShardRegression) {
    OneBroker b("cw08_dh", OneBroker::Runtime::Shard);
    awaitLevel(b.node());
    ASSERT_FALSE(HasFatalFailure());
    std::string out, err;
    ASSERT_EQ(ql(b.endpoint(),
                 "topic device_login { loginId uint64 autoincrement, userId uint64, deviceFp string(64), platform uint16, ts int64 } "
                 "partitions 1 key loginId with (cleanup.policy = compact);", &out, &err), 0) << err;
    ASSERT_EQ(ql(b.endpoint(),
                 "create view device_history key (userId, deviceFp) as select min(ts) as firstSeen, max(ts) as lastSeen, count(*) as logins, "
                 "last(platform) as platform from device_login group by userId, deviceFp;", &out, &err), 0) << err;

    rc::Client& c = *b.connectClient();
    Writer<DeviceLogin> w(c, "device_login");
    std::set<uint64_t> ids;
    const auto login = [&](uint64_t user, const char* fp, uint16_t platform, int64_t ts) {
        const auto r = w.topic.insertAndWait(DeviceLogin{.userId = user, .deviceFp = fp, .platform = platform, .ts = ts}, 10s);
        ASSERT_TRUE(r.ok()) << "code " << r.code();
        EXPECT_NE(r.key(), 0u);
        EXPECT_TRUE(ids.insert(r.key()).second) << "a fresh loginId each time: " << r.key();
    };
    login(7, "a", 1, 300);
    login(7, "a", 1, 100);
    login(7, "a", 2, 200);
    login(7, "b", 3, 400);
    login(8, "a", 1, 500);
    ASSERT_FALSE(HasFatalFailure());

    ASSERT_TRUE(qlEventually(b.endpoint(), "select * from device_history where userId = 7 and deviceFp = 'a';", out, err)) << err;
    auto rows = cells(out);
    ASSERT_EQ(rows.size(), 3u) << out;
    EXPECT_EQ(rows[0], (std::vector<std::string>{"userId", "deviceFp", "firstSeen", "lastSeen", "logins", "platform"}));
    EXPECT_EQ(rows[1], (std::vector<std::string>{"7", "a", "100", "300", "3", "2"}));
    EXPECT_EQ(rows[2], (std::vector<std::string>{"(1", "rows)"}));

    ASSERT_EQ(ql(b.endpoint(), "select * from device_history where userId = 7;", &out, &err), 0) << err;
    rows = cells(out);
    ASSERT_EQ(rows.size(), 4u) << out;
    const std::set<std::vector<std::string>> got{rows[1], rows[2]};
    const std::set<std::vector<std::string>> want{{"7", "a", "100", "300", "3", "2"}, {"7", "b", "400", "400", "1", "3"}};
    EXPECT_EQ(got, want) << out;
    EXPECT_EQ(rows[3], (std::vector<std::string>{"(2", "rows)"}));
    EXPECT_EQ(rows[1][0], "7") << "user 8 never appears: " << out;
    EXPECT_EQ(rows[2][0], "7") << "user 8 never appears: " << out;
}

// §15, R07f: lower('FOO') reads the row stored under the lowered key of 'Foo'.
TEST(CheckedWritesQlBrokerRegression, LowerLiteralReadsTheRowShardRegression) {
    OneBroker b("cw08_lower", OneBroker::Runtime::Shard);
    awaitLevel(b.node());
    ASSERT_FALSE(HasFatalFailure());
    std::string out, err;
    ASSERT_EQ(ql(b.endpoint(), "topic character { characterId uint64 autoincrement, userId uint64, name string(16), createdAt int64 } "
                               "key characterId with (cleanup.policy = compact);", &out, &err), 0) << err;
    ASSERT_EQ(ql(b.endpoint(), "create view name_index key (lower(name) as nameKey, characterId) as select userId from character;", &out, &err), 0) << err;
    rc::Client& c = *b.connectClient();
    Writer<Character> w(c, "character");
    ASSERT_TRUE(w.topic.insertAndWait(Character{.userId = 9, .name = "Foo"}, 10s).ok());

    ASSERT_TRUE(qlEventually(b.endpoint(), "select userId, nameKey from name_index where nameKey = lower('FOO');", out, err)) << err;
    const auto rows = cells(out);
    ASSERT_EQ(rows.size(), 3u) << out;
    EXPECT_EQ(rows[0], (std::vector<std::string>{"userId", "nameKey"}));
    EXPECT_EQ(rows[1], (std::vector<std::string>{"9", "foo"}));
}

// Spec §1: account_characters and character_by_id, point and prefix reads.
TEST(CheckedWritesQlBrokerRegression, PointAndPrefixSelectShardRegression) {
    OneBroker b("cw08_pp", OneBroker::Runtime::Shard);
    awaitLevel(b.node());
    ASSERT_FALSE(HasFatalFailure());
    std::string out, err;
    ASSERT_EQ(ql(b.endpoint(), "topic character { characterId uint64 autoincrement, userId uint64, name string(16), createdAt int64 } "
                               "key characterId with (cleanup.policy = compact);", &out, &err), 0) << err;
    ASSERT_EQ(ql(b.endpoint(), "create view character_by_id key characterId as select * from character;", &out, &err), 0) << err;
    ASSERT_EQ(ql(b.endpoint(), "create view account_characters key (userId, characterId) as select name from character;", &out, &err), 0) << err;
    rc::Client& c = *b.connectClient();
    Writer<Character> w(c, "character");
    std::vector<uint64_t> ids;
    for (const char* name : {"Ann", "Bea", "Cy"}) {
        const auto r = w.topic.insertAndWait(Character{.userId = 42, .name = name}, 10s);
        ASSERT_TRUE(r.ok()) << "code " << r.code();
        ids.push_back(r.key());
    }

    ASSERT_TRUE(qlEventually(b.endpoint(), "select * from account_characters where userId = 42;", out, err)) << err;
    EXPECT_NE(out.find("(3 rows)"), std::string::npos) << out;
    ASSERT_EQ(ql(b.endpoint(), "select name from character_by_id where characterId = " + std::to_string(ids[1]) + ";", &out, &err), 0) << err;
    const auto one = cells(out);
    ASSERT_EQ(one.size(), 3u) << out;
    EXPECT_EQ(one[1], (std::vector<std::string>{"Bea"}));
    EXPECT_EQ(one[2], (std::vector<std::string>{"(1", "rows)"}));
    ASSERT_EQ(ql(b.endpoint(), "select name from character_by_id where characterId = 999999;", &out, &err), 0) << err;
    EXPECT_EQ(out, "(no row)\n");
    ASSERT_EQ(ql(b.endpoint(), "select * from account_characters where userId = 42 limit 2;", &out, &err), 0) << err;
    EXPECT_NE(out.find("(2 rows)"), std::string::npos) << out;
}

// Review Focus 4: a script creates the topic, then the view over it.
TEST(CheckedWritesQlBrokerRegression, ScriptCreatesTopicThenViewShardRegression) {
    OneBroker b("cw08_script", OneBroker::Runtime::Shard);
    const std::string file = writeTemp("topic_then_view.rql", "topic s { id uint64, n int64 } key id;\ncreate view v key id as select n from s;\n");
    std::string out, err;
    ASSERT_EQ(runCli({"ql", "-f", file, "--broker", b.endpoint()}, &out, &err), 0) << err;
    ASSERT_EQ(ql(b.endpoint(), "show view v;", &out, &err), 0) << err;
    EXPECT_NE(out.find("  sources s\n"), std::string::npos) << out;
}

// R02d, R10b: a keyless typed topic is legal; a view over it and key.index on it are the broker's refusals.
TEST(CheckedWritesQlBrokerRegression, KeylessTypedTopicFromQlShardRegression) {
    OneBroker b("cw08_keyless", OneBroker::Runtime::Shard);
    std::string out, err;
    ASSERT_EQ(ql(b.endpoint(), "topic k { a uint64, b int64 };", &out, &err), 0) << err;
    ASSERT_EQ(ql(b.endpoint(), "show topic k;", &out, &err), 0) << err;
    EXPECT_NE(out.find("kind typed"), std::string::npos) << out;
    EXPECT_NE(out.find("keyField -"), std::string::npos) << out;
    EXPECT_EQ(ql(b.endpoint(), "create view kv key a as select b from k;", &out, &err), 1);
    EXPECT_EQ(err, "rune: ViewKeyInvalid (35) view source needs a key\n");
    EXPECT_EQ(ql(b.endpoint(), "topic k2 { a uint64 } with (key.index = on);", &out, &err), 1);
    EXPECT_EQ(err, "rune: InvalidRequest (39) key.index needs a key\n");
}

TEST(CheckedWritesQlBrokerRegression, CreateOrReplaceSwapsThePlanShardRegression) {
    OneBroker b("cw08_replace", OneBroker::Runtime::Shard);
    std::string out, err;
    ASSERT_EQ(ql(b.endpoint(), "topic s { id uint64, n int64, m int64 } key id;", &out, &err), 0) << err;
    ASSERT_EQ(ql(b.endpoint(), "create or replace view v key id as select n from s;", &out, &err), 0) << err;
    EXPECT_NE(out.find("created view 'v'"), std::string::npos) << out;
    ASSERT_EQ(ql(b.endpoint(), "show view v;", &out, &err), 0) << err;
    EXPECT_NE(out.find("version 1)"), std::string::npos) << out;
    ASSERT_EQ(ql(b.endpoint(), "create or replace view v key id as select n, m from s;", &out, &err), 0) << err;
    EXPECT_NE(out.find("altered view 'v' version=2"), std::string::npos) << out;
    ASSERT_EQ(ql(b.endpoint(), "show view v;", &out, &err), 0) << err;
    EXPECT_NE(out.find("version 2)"), std::string::npos) << out;
    EXPECT_NE(out.find(" m "), std::string::npos) << "the new select list is the plan: " << out;
}

// Part 05's key.index topic through QL, then `rune stats --json`.
TEST(CheckedWritesQlBrokerRegression, StatsJsonOverAKeyIndexTopicShardRegression) {
    OneBroker b("cw08_stats", OneBroker::Runtime::Shard);
    awaitLevel(b.node());
    ASSERT_FALSE(HasFatalFailure());
    std::string out, err;
    ASSERT_EQ(ql(b.endpoint(), "topic ki { id uint64, name string(16), n int64 } key id with (key.index = on, cleanup.policy = compact);", &out, &err), 0) << err;
    rc::Client& c = *b.connectClient();
    Writer<Ki> w(c, "ki");
    ASSERT_TRUE(w.topic.insertAndWait(Ki{.id = 1, .name = "a"}, 10s).ok());
    const auto again = w.topic.insertAndWait(Ki{.id = 1, .name = "b"}, 10s);
    EXPECT_EQ(again.status(), rc::OpStatus::BrokerError);
    EXPECT_EQ(again.code(), kKeyExists);

    EXPECT_TRUE(waitFor([&] {
        return runCli({"stats", "topic", "ki", "--json", "--broker", b.endpoint()}, &out, &err) == 0 &&
               out.find("\"rejectionsKeyExists\": 1") != std::string::npos && out.find("\"verdictLag\": 0") != std::string::npos;
    }, 10s)) << out << err;
    EXPECT_EQ(runCli({"stats", "view", "nope", "--broker", b.endpoint()}, &out, &err), 1);
    EXPECT_EQ(err, "rune: no view 'nope'\n");
}

// Review of Task 9: the CLI live paths on a source with 4 partitions - the point read goes to the row's own partition.
TEST(CheckedWritesQlBrokerRegression, PointSelectRoutesToTheRowsPartitionShardRegression) {
    OneBroker b("cw08_route", OneBroker::Runtime::Shard);
    awaitLevel(b.node());
    ASSERT_FALSE(HasFatalFailure());
    std::string out, err;
    ASSERT_EQ(ql(b.endpoint(), "topic ki { id uint64, name string(16), n int64 } partitions 4 key id with (key.index = on, cleanup.policy = compact);", &out, &err), 0) << err;
    ASSERT_EQ(ql(b.endpoint(), "create view ki_by_id key id as select name, n from ki;", &out, &err), 0) << err;
    rc::Client& c = *b.connectClient();
    Writer<Ki> w(c, "ki");
    std::set<int32_t> partitions;
    for (uint64_t id = 1; id <= 8; ++id) {
        const auto r = w.topic.insertAndWait(Ki{.id = id, .name = "k" + std::to_string(id), .n = static_cast<int64_t>(id * 10)}, 10s);
        ASSERT_TRUE(r.ok()) << "id " << id << " code " << r.code();
        partitions.insert(rc::Key(id).partition(4));
    }
    ASSERT_EQ(partitions.size(), 4u) << "the eight ids land on all four partitions";
    for (uint64_t id = 1; id <= 8; ++id) {
        ASSERT_TRUE(qlEventually(b.endpoint(), "select name, n from ki_by_id where id = " + std::to_string(id) + ";", out, err)) << "id " << id << ": " << err;
        const auto rows = cells(out);
        ASSERT_EQ(rows.size(), 3u) << "id " << id << ": " << out;
        EXPECT_EQ(rows[1], (std::vector<std::string>{"k" + std::to_string(id), std::to_string(id * 10)})) << "id " << id;
    }
    ASSERT_EQ(ql(b.endpoint(), "select name from ki_by_id where id = 99;", &out, &err), 0) << err;
    EXPECT_EQ(out, "(no row)\n");
}

// Review of Task 9: a scan that spans several pages prints every row and the right count; limit cuts inside a page boundary.
TEST(CheckedWritesQlBrokerRegression, ScanSelectPagesShardRegression) {
    constexpr int kRows = 300;
    static_assert(kRows > rune::cli::engine::kScanPageRows, "the scan spans more than one page");
    OneBroker b("cw08_pages", OneBroker::Runtime::Shard);
    awaitLevel(b.node());
    ASSERT_FALSE(HasFatalFailure());
    std::string out, err;
    ASSERT_EQ(ql(b.endpoint(), "topic character { characterId uint64 autoincrement, userId uint64, name string(16), createdAt int64 } "
                               "key characterId with (cleanup.policy = compact);", &out, &err), 0) << err;
    ASSERT_EQ(ql(b.endpoint(), "create view account_characters key (userId, characterId) as select name from character;", &out, &err), 0) << err;
    rc::Client& c = *b.connectClient();
    Writer<Character> w(c, "character");
    std::set<uint64_t> ids;
    for (int i = 0; i < kRows; ++i) {
        const auto r = w.topic.insertAndWait(Character{.userId = 42, .name = "c" + std::to_string(i)}, 10s);
        ASSERT_TRUE(r.ok()) << "insert " << i << " code " << r.code();
        ids.insert(r.key());
    }
    ASSERT_TRUE(w.topic.insertAndWait(Character{.userId = 43, .name = "other"}, 10s).ok());

    ASSERT_TRUE(qlEventually(b.endpoint(), "select characterId from account_characters where userId = 42;", out, err)) << err;
    const auto rows = cells(out);
    ASSERT_EQ(rows.size(), static_cast<std::size_t>(kRows) + 2) << "header, every row, the count";
    EXPECT_EQ(rows.back(), (std::vector<std::string>{"(300", "rows)"}));
    std::set<uint64_t> printed;
    for (std::size_t i = 1; i + 1 < rows.size(); ++i) printed.insert(std::stoull(rows[i][0]));
    EXPECT_EQ(printed, ids) << "no row missing or repeated across the page boundary";

    ASSERT_EQ(ql(b.endpoint(), "select characterId from account_characters where userId = 42 limit 260;", &out, &err), 0) << err;
    EXPECT_NE(out.find("(260 rows)"), std::string::npos) << out;
}

// A follower seed: the client follows the Redirect to the partition leader and prints the leader's counters.
TEST(CheckedWritesQlBrokerRegression, StatsFromAFollowerRedirectsThreeNodesRegression) {
    using namespace rune::node::test::catalog_cluster;
    ThreeNodes c = ThreeNodes::start("cw08_stf");
    ASSERT_TRUE(c.awaitCatalogLeader(30s));
    for (const int id : c.runningNodes()) awaitLevel(c.node(id));
    ASSERT_FALSE(HasFatalFailure());
    const int32_t ctl = awaitController(c);
    ASSERT_GT(ctl, 0);
    std::string out, err;
    ASSERT_EQ(ql(clusterEndpoint(c, ctl), "topic ki { id uint64, name string(16), n int64 } partitions 1 replicas 3 key id with (key.index = on, cleanup.policy = compact);", &out, &err), 0) << err;
    int32_t topicId = -1;
    for (const auto deadline = std::chrono::steady_clock::now() + 10s; topicId < 0 && std::chrono::steady_clock::now() < deadline; std::this_thread::sleep_for(20ms))
        topicId = c.node(ctl).onAdminThreadForTest([&]() -> int32_t {
            const auto* t = c.node(ctl).adminWorkerAgentForTest()->worker().applier().state().topicByName(rune::catalog::kDefaultNsId, "ki");
            return t != nullptr ? t->topicId : -1;
        });
    ASSERT_GE(topicId, 0);
    std::vector<int32_t> leaders;
    ASSERT_TRUE(awaitLeaders(c, topicId, 1, leaders, 30s));
    {
        auto client = connectClient(c, ctl);
        Writer<Ki> w(*client, "ki");
        ASSERT_TRUE(w.topic.insertAndWait(Ki{.id = 1, .name = "a"}, 10s).ok());
        EXPECT_EQ(w.topic.insertAndWait(Ki{.id = 1, .name = "b"}, 10s).code(), kKeyExists);
    }
    int follower = -1;
    for (const int id : c.runningNodes())
        if (id != leaders[0]) { follower = id; break; }
    ASSERT_GT(follower, 0);
    EXPECT_TRUE(waitFor([&] {
        return runCli({"stats", "topic", "ki", "--json", "--broker", clusterEndpoint(c, follower)}, &out, &err) == 0 &&
               out.find("\"rejectionsKeyExists\": 1") != std::string::npos && out.find("\"verdictLag\": 0") != std::string::npos;
    }, 20s)) << "follower " << follower << " leader " << leaders[0] << ": " << out << err;
}
