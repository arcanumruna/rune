// CheckedWritesRegressorsPathRegressionTest.cpp - checked writes Part 09 Task 10: the RegressorsPath scenario (ql/tests/golden/regressorspath.rql)
// end to end on real brokers - the login race, the Foo / foo names, the 13th slot, delete and reuse, a failover, a restart of every node and
// the compaction of the rejected records - through the real client and the real `rune` CLI.  Every wait is a bounded waitFor.
#include "client/ClusterClient.h"
#include "client/OneBroker.h"
#include "client/support/RegressorsPathRecords.h"
#include "support/CatalogBrokerCluster.h"
#include "support/CheckedWritesRig.h"

#include <QlEngine.h>
#include <rune/cli/Cli.h>
#include <rune/client/Client.h>
#include <rune/client/Completions.h>
#include <rune/client/TypedTopic.h>
#include <rune/client/producer/Producer.h>
#include <rune/client/subscriber/Subscription.h>
#include <rune/client/view/ViewKeyBuilder.h>
#include <rune/client/view/ViewRowCodec.h>
#include <rune/metadata/ShardRouter.h>

#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <sstream>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

using namespace std::chrono_literals;
using namespace rune::node;
using namespace rune::node::test;
namespace rc  = rune::client;
namespace rcv = rune::client::view;
namespace cw  = rune::node::cw;
namespace fs  = std::filesystem;
using rune::codec::PrimType;

namespace {

constexpr uint16_t kConstraintViolation = static_cast<uint16_t>(rune::protocol::client::ErrorCode::ConstraintViolation);
constexpr uint16_t kKeyNotFound         = static_cast<uint16_t>(rune::protocol::client::ErrorCode::KeyNotFound);
constexpr rc::Acks kAllView             = rc::Acks::All | rc::Acks::View;

template <class Pred>
bool waitFor(Pred&& pred, std::chrono::seconds within) {
    for (const auto deadline = std::chrono::steady_clock::now() + within; std::chrono::steady_clock::now() < deadline; std::this_thread::sleep_for(20ms))
        if (pred()) return true;
    return pred();
}

/// Every broker here runs the shard runtime: its client stream is catalog_cluster::kClientStream.
int runCli(std::vector<std::string> args, std::string* out = nullptr, std::string* err = nullptr) {
    args.insert(args.end(), {"--cli-stream-in", std::to_string(catalog_cluster::kClientStream)});
    std::stringstream o, e;
    const int code = rune::cli::run(std::move(args), o, e);
    if (out) *out = o.str();
    if (err) *err = e.str();
    return code;
}

int ql(const std::string& endpoint, const std::string& text, std::string* out = nullptr, std::string* err = nullptr) {
    return runCli({"ql", "-e", text, "--broker", endpoint}, out, err);
}

struct TmpDir {
    fs::path path = fs::temp_directory_path() / ("rune_cw09_rp_test_" + std::to_string(::getpid()));
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

void awaitLevel(BrokerNode& n) {
    ASSERT_TRUE(waitFor([&] { return n.onWorkerThreadForTest(0, [](Worker& w) { return w.clusterFeatureLevelForTest(); }) >= rune::replication::MetadataFeature::kCheckedWrites; }, 10s))
        << "the cluster feature level reaches kCheckedWrites";
}

std::string endpointOf(catalog_cluster::ThreeNodes& c, int id) {
    return "127.0.0.1:" + std::to_string(c.ports.client[static_cast<std::size_t>(id - 1)]);
}

/// The golden script with the user and character topics on small segments and an eager cleaner (tombstones leave at once).
std::string eagerScript() {
    std::ifstream in(std::string(RUNE_QL_GOLDEN_DIR) + "/regressorspath.rql");
    if (!in.good()) throw std::runtime_error("the golden script is missing");
    std::stringstream text;
    text << in.rdbuf();
    std::string script = text.str();
    const std::string plain = "with (cleanup.policy = compact)";
    const std::string eager = "with (cleanup.policy = compact, segment.bytes = 65536, delete.retention.ms = 0, min.compaction.lag.ms = 50, "
                              "min.cleanable.dirty.ratio = 0.000001, file.delete.delay.ms = 50)";
    int rewritten = 0;
    for (auto at = script.find(plain); at != std::string::npos; at = script.find(plain, at + eager.size()), ++rewritten) script.replace(at, plain.size(), eager);
    if (rewritten != 3) throw std::runtime_error("expected the user, device_login and character topics to be compacted, found " + std::to_string(rewritten));
    const fs::path path = tmpDir() / "regressorspath.rql";
    std::ofstream(path, std::ios::binary) << script;
    return path.string();
}

/// The statement of the golden script that defines topic `name`, on one broker.
std::string goldenTopic(const std::string& name) {
    std::ifstream in(std::string(RUNE_QL_GOLDEN_DIR) + "/regressorspath.rql");
    std::stringstream text;
    text << in.rdbuf();
    for (std::string st : rune::cli::engine::splitStatements(text.str()))
        if (st.find("topic " + name + " {") != std::string::npos) return st.replace(st.find(" replicas 3"), std::string_view(" replicas 3").size(), "");
    return {};
}

void fastWarmup(NodeConfig& cfg) { cfg.unservableWarmupMs = 50; }

catalog_cluster::ThreeNodes startCluster(const std::string& tag) {
    return catalog_cluster::ThreeNodes::startWith(tag, [](NodeConfig& cfg) {
        cfg.verdictIntervalMs      = 20;
        cfg.unservableWarmupMs     = 50;
        cfg.producerIdExpirationMs = 2'000;   // a stopped writer's last batches leave the cleaner something to compact
    });
}

/// Every running broker agrees on a live leader of partition 0 of `topicId`.  An election after a crash or a restart of every node can
/// outlast 10 s on a loaded host (the failure detector alone takes several seconds), so this wait is the one 10 s waitFor that is repeated,
/// at most 3 times; the rounds beyond the first are counted and printed.
bool leadersWithin(catalog_cluster::ThreeNodes& c, int32_t topicId, std::vector<int32_t>& leaders) {
    for (int round = 0; round < 3; ++round) {
        if (waitFor([&] { return catalog_cluster::awaitLeaders(c, topicId, 1, leaders, 1ms); }, 10s)) return true;
        std::printf("leader wait for topic %d: round %d of 3 timed out\n", topicId, round + 1);
    }
    return false;
}

void ready(catalog_cluster::ThreeNodes& c) {
    ASSERT_TRUE(waitFor([&] { return c.awaitCatalogLeader(1ms); }, 10s));
    for (const int id : c.runningNodes()) awaitLevel(c.node(id));
}

// ---- views: topic id, columns and key builder, read from a node's catalog ----

rcv::ViewKeyBuilder keyBuilder(BrokerNode& n, const std::string& view) {
    auto b = n.onAdminThreadForTest([&n, &view] { return rcv::ViewKeyBuilder::fromCatalog(n.adminWorkerAgentForTest()->worker().applier().state(), view); });
    if (!b) throw std::runtime_error(b.error());
    return std::move(*b);
}

std::vector<uint8_t> bytesOf(std::expected<std::vector<uint8_t>, std::string> k) {
    if (!k) throw std::runtime_error(k.error());
    return std::move(*k);
}

struct ViewHandle {
    int32_t                  topic = -1;
    rcv::RowLayout           layout;
    std::vector<std::string> names;
    rcv::ViewKeyBuilder      builder;

    ViewHandle(BrokerNode& n, const std::string& view) : builder(keyBuilder(n, view)) {
        const auto [topicId, blob] = n.onAdminThreadForTest([&n, &view] {
            const auto* v = n.adminWorkerAgentForTest()->worker().applier().state().viewByName(rune::catalog::kDefaultNsId, view);
            return std::make_pair(v->def.viewTopicId, v->def.outputDescriptorBlob);
        });
        topic = topicId;
        std::vector<PrimType> types;
        std::vector<uint16_t> widths;
        for (const auto& f : rune::codec::deserialize(blob).fields) {
            names.push_back(f.name);
            types.push_back(f.type);
            widths.push_back(f.type == PrimType::FixedStr ? f.fixedLen : uint16_t{0});
        }
        layout = rcv::buildRowLayout(types, widths);
    }
    [[nodiscard]] uint64_t u(std::span<const uint8_t> row, const std::string& column) const {
        const auto slot = static_cast<uint16_t>(std::find(names.begin(), names.end(), column) - names.begin());
        const rcv::FieldValue f = rcv::readSlot(row, layout, slot);
        return f.type == PrimType::Int64 ? static_cast<uint64_t>(f.i) : f.u;
    }
    [[nodiscard]] std::vector<uint8_t> keyOf(uint64_t id) { builder.clear(); return bytesOf(builder.setUInt(names[0], id).key()); }
};

int32_t topicIdOf(BrokerNode& n, const std::string& name) {
    return n.onAdminThreadForTest([&n, &name] {
        const auto* t = n.adminWorkerAgentForTest()->worker().applier().state().topicByName(rune::catalog::kDefaultNsId, name);
        return t != nullptr ? t->topicId : -1;
    });
}

/// One try of a point read; a view that is not ready or a leader that moved throws or answers a non-Ok status.
rc::ViewRowResult readRow(rc::Client& c, const ViewHandle& v, std::span<const uint8_t> key, int64_t minOffset = -1) {
    rc::ViewReadOptions o;
    o.minOffset = minOffset;
    return c.getViewRowAndWait(v.topic, key, o, 10s);
}

/// The row of `key` once it reads Ok (a rebuilt view answers a refusal until it is back).
std::optional<std::vector<uint8_t>> rowWhenReady(rc::Client& c, const ViewHandle& v, std::span<const uint8_t> key) {
    std::optional<std::vector<uint8_t>> row;
    waitFor([&] {
        try {
            auto r = readRow(c, v, key);
            if (r.status == rc::ViewRowResult::kOk) row = std::move(r.rowBytes);
        } catch (const std::exception&) {}
        return row.has_value();
    }, 10s);
    return row;
}

std::vector<std::vector<uint8_t>> scanAll(rc::Client& c, const ViewHandle& v, std::span<const uint8_t> prefix) {
    std::vector<std::vector<uint8_t>> rows;
    rc::ViewScanOptions opts;
    for (int page = 0; page < 1000; ++page) {
        auto r = c.scanViewAndWait(v.topic, prefix, 0, opts, 10s);
        if (r.status != rc::ViewRowResult::kOk) throw std::runtime_error("scan status " + std::to_string(r.status));
        for (auto& row : r.rows) rows.push_back(std::move(row));
        if (r.nextToken.empty()) break;
        opts.resumeToken = std::move(r.nextToken);
    }
    return rows;
}

// ---- writers: a client with an All|View producer and the user and character topics, writes issued before they are awaited ----

struct Writer {
    std::unique_ptr<rc::Client>                                     client;
    std::optional<rc::Producer>                                     prod;
    std::optional<rc::TypedTopic<RpUser, rc::ProducerRef>>          users;
    std::optional<rc::TypedTopic<RpCharacter, rc::ProducerRef>>     characters;
    std::optional<rc::Completions>                                  cq;
    rc::Token                                                       next = 1;

    explicit Writer(rc::ClientConfig cfg, bool withUsers = true, bool withCharacters = true) {
        client = std::make_unique<rc::Client>(cfg);
        rc::ProducerConfig pc;
        pc.acks     = kAllView;
        pc.lingerUs = 1;
        prod.emplace(client->producer(pc));
        client->connectAndWait(10s);
        if (withUsers) users.emplace(client->openTopicAndWait<RpUser>("user", *prod, 10s));
        if (withCharacters) characters.emplace(client->openTopicAndWait<RpCharacter>("character", *prod, 10s));
        cq.emplace(*client);
    }
    ~Writer() {
        cq.reset();
        if (prod) prod->closeImmediately();
        if (client) client->close();
    }

    rc::Token issued() { prod->flush(); return next++; }
    rc::Token insert(const RpUser& r) { users->insert(r, *cq, next); return issued(); }
    rc::Token update(const RpUser& r) { users->update(r, *cq, next); return issued(); }
    rc::Token insert(const RpCharacter& r) { characters->insert(r, *cq, next); return issued(); }
    rc::Token update(const RpCharacter& r) { characters->update(r, *cq, next); return issued(); }
    rc::Token removeExisting(uint64_t id) { characters->removeExisting(id, *cq, next); return issued(); }

    rc::WriteResult await(rc::Token t, std::chrono::nanoseconds within = 10s) {
        std::optional<rc::WriteResult> r;
        if (cq->waitFor(t, within, [&](const rc::Completion& c, std::span<const uint8_t>) { r = rc::WriteResult::fromCompletion(c); }) != rc::Completions::Wait::Done)
            throw std::runtime_error("a write never completed within 10 s");
        return *r;
    }
    rc::WriteResult insertAndAwait(const RpUser& r) { return await(insert(r)); }
    rc::WriteResult insertAndAwait(const RpCharacter& r) { return await(insert(r)); }
    rc::WriteResult updateAndAwait(const RpCharacter& r) { return await(update(r)); }
    rc::WriteResult removeExistingAndAwait(uint64_t id) { return await(removeExisting(id)); }
};

using Fleet = std::vector<std::unique_ptr<Writer>>;

/// `n` writers, each seeded at the next running node.
Fleet makeFleet(catalog_cluster::ThreeNodes& c, std::size_t n) {
    Fleet f;
    const auto nodes = c.runningNodes();
    for (std::size_t i = 0; i < n; ++i) f.push_back(std::make_unique<Writer>(catalog_cluster::clientConfigFor(c, nodes[i % nodes.size()])));
    return f;
}

/// What the run was told per topic: the offsets of the accepted and of the rejected writes, the newest accepted offset of each key and
/// the highest id any answer carried.
struct Ledger {
    std::map<uint64_t, int64_t> latest;
    std::set<int64_t>           accepted, rejected;
    std::set<uint64_t>          removed;   ///< keys whose newest accepted write is a tombstone
    uint64_t                    highestId = 0;

    /// `id` is the key written, 0 for a create (the answer carries the assigned id).
    const rc::WriteResult& note(uint64_t id, const rc::WriteResult& r, bool tombstone = false) {
        highestId = std::max({highestId, id, r.key()});
        if (r.ok()) {
            accepted.insert(r.offset());
            const uint64_t key = id != 0 ? id : r.key();
            latest[key] = r.offset();
            if (tombstone) removed.insert(key);
            else removed.erase(key);
        } else if (r.offset() >= 0 && (r.code() == kConstraintViolation || r.code() == kKeyNotFound)) {
            rejected.insert(r.offset());
        }
        return r;
    }
};

RpCharacter character(uint64_t user, const std::string& name, uint64_t id = 0, uint16_t gender = 1) {
    return RpCharacter{.characterId = id, .userId = user, .name = name, .gender = gender};
}

bool isConstraint(const rc::WriteResult& r, rc::Client& as, const std::string& view, const std::string& constraint) {
    return r.code() == kConstraintViolation && as.constraintName(r.constraintId()) == std::optional(rc::ConstraintName{view, constraint});
}

/// The newest record of topic `name` from the leader of its partition 0, read below the high watermark on the replica's thread.
struct LeaderLog {
    int                  node = -1;
    int64_t              cleanedThrough = 0;
    cw::VerdictRig::RigLog log;
};

LeaderLog readLeader(catalog_cluster::ThreeNodes& c, int32_t topicId) {
    std::vector<int32_t> leaders;
    LeaderLog out;
    if (!leadersWithin(c, topicId, leaders)) return out;
    out.node = leaders[0];
    c.node(out.node).onOwnerThreadForTest(topicId, 0, [&out](rune::replication::PartitionReplica& r) {
        out.cleanedThrough = r.lastCleanOffset();
        out.log            = cw::readLogOf(*r.log(), r.highWatermark());
        return 0;
    });
    return out;
}

/// Accepted updates through `w` until the leader's cleaner has passed `through`: the segment that holds the rejected records is sealed and compacted.
template <class Rec, class Send>
void compactPast(catalog_cluster::ThreeNodes& c, Writer& w, int32_t topicId, int64_t through, Ledger& ledger, uint64_t id, const Rec& rec, Send&& send) {
    const auto passed = [&] { return readLeader(c, topicId).cleanedThrough > through; };
    // 800 records of about 125 bytes roll the 64 KiB segment that holds the rejected ones.  The cleaner makes no headway while the log is
    // being written, so the writes stop before the 10 s wait for it.
    for (int chunk = 0; chunk < 25; ++chunk) {
        std::vector<rc::Token> tokens;
        for (int i = 0; i < 32; ++i) tokens.push_back(send(w, rec));
        for (const rc::Token t : tokens) ledger.note(id, w.await(t));
    }
    EXPECT_TRUE(waitFor(passed, 10s)) << "the leader's cleaner never passed offset " << through << " of topic " << topicId;
}

}  // namespace

// The acceptance case: the scenario of the spec, step by step.
TEST(CheckedWritesRegressorsPathRegression, TheWholeScenarioThreeNodesRegression) {
    using namespace rune::node::test::catalog_cluster;
    ThreeNodes c = startCluster("cw09");
    ready(c);
    ASSERT_FALSE(HasFailure());
    const std::string ep = endpointOf(c, 1);
    Ledger users, chars;

    // 1. DDL
    std::string out, err;
    ASSERT_EQ(runCli({"ql", "-f", eagerScript(), "--broker", ep}, &out, &err), 0) << err;
    ASSERT_EQ(ql(ep, "show view character_name;", &out, &err), 0) << err;
    EXPECT_NE(out.find("unique"), std::string::npos) << out;
    EXPECT_NE(out.find("keyspec lower(name) as nameKey"), std::string::npos) << out;
    ASSERT_EQ(ql(ep, "show view account_stats;", &out, &err), 0) << err;
    EXPECT_NE(out.find("constraint max_characters check (characters <= 12)"), std::string::npos) << out;
    ASSERT_EQ(ql(ep, "create view character_by_id key characterId as select userId, name from character;", &out, &err), 0) << err;
    ASSERT_EQ(ql(ep, "create view account_characters key (userId, characterId) as select name from character;", &out, &err), 0) << err;
    const int32_t ctl = awaitController(c);
    ASSERT_GT(ctl, 0);
    BrokerNode& admin = c.node(ctl);
    ViewHandle userIdentity(admin, "user_identity"), characterName(admin, "character_name"), accountStats(admin, "account_stats"),
        accountCharacters(admin, "account_characters"), characterById(admin, "character_by_id");
    const int32_t userTopic = topicIdOf(admin, "user"), characterTopic = topicIdOf(admin, "character");
    ASSERT_GE(userTopic, 0);
    ASSERT_GE(characterTopic, 0);
    const auto loginKey = [&](const std::string& subject) {
        userIdentity.builder.clear();
        return bytesOf(userIdentity.builder.setUInt("provider", 1).setString("subject", subject).key());
    };
    const auto nameKey = [&](const std::string& name) {
        characterName.builder.clear();
        return bytesOf(characterName.builder.setString("name", name).key());
    };
    const auto statsOf = [&](rc::Client& cl, uint64_t user) -> std::optional<uint64_t> {
        const auto row = rowWhenReady(cl, accountStats, accountStats.keyOf(user));
        return row ? std::optional(accountStats.u(*row, "characters")) : std::nullopt;
    };

    // 2. Login race
    {
        Fleet ab = makeFleet(c, 2);
        const auto key = loginKey("steam:42");
        for (auto& w : ab) {
            ASSERT_TRUE(waitFor([&] { return readRow(*w->client, userIdentity, key).status == rc::ViewRowResult::kAbsentKey; }, 10s)) << "no login row yet";
        }
        const rc::Token ta = ab[0]->insert(RpUser{.provider = 1, .subject = "steam:42"});
        const rc::Token tb = ab[1]->insert(RpUser{.provider = 1, .subject = "steam:42"});
        const rc::WriteResult ra = users.note(0, ab[0]->await(ta)), rb = users.note(0, ab[1]->await(tb));
        ASSERT_NE(ra.ok(), rb.ok()) << "exactly one login wins: " << ra.code() << " / " << rb.code();
        const rc::WriteResult& win = ra.ok() ? ra : rb;
        const rc::WriteResult& lose = ra.ok() ? rb : ra;
        rc::Client& loser = *ab[ra.ok() ? 1 : 0]->client;
        ASSERT_GE(win.key(), 1u);
        EXPECT_TRUE(isConstraint(lose, loser, "user_identity", "unique")) << "code " << lose.code();
        const auto row = readRow(loser, userIdentity, key, lose.offset());
        ASSERT_EQ(row.status, rc::ViewRowResult::kOk) << "the loser reads the winner's row on the first try";
        EXPECT_EQ(userIdentity.u(row.rowBytes, "userId"), win.key());
    }
    const uint64_t k = users.latest.begin()->first;
    ASSERT_GE(k, 1u);

    // 3. Failover mid-scenario
    std::vector<int32_t> leaders;
    ASSERT_TRUE(leadersWithin(c, characterTopic, leaders));
    const int32_t crashed = leaders[0];
    c.crash(crashed);
    ASSERT_TRUE(leadersWithin(c, characterTopic, leaders));
    ASSERT_NE(leaders[0], crashed);
    ASSERT_TRUE(leadersWithin(c, userTopic, leaders));

    // 4. Create returns its id
    Fleet fleet = makeFleet(c, 4);
    const auto create = [&](std::size_t w, uint64_t user, const std::string& name) { return chars.note(0, fleet[w]->insertAndAwait(character(user, name))); };
    const rc::WriteResult foo = create(0, k, "Foo");
    ASSERT_TRUE(foo.ok()) << "code " << foo.code();
    const uint64_t c1 = foo.key();
    ASSERT_GE(c1, 1u);
    {
        const auto row = readRow(*fleet[0]->client, characterById, characterById.keyOf(c1), foo.offset());
        ASSERT_EQ(row.status, rc::ViewRowResult::kOk);
        EXPECT_EQ(characterById.u(row.rowBytes, "userId"), k);
    }

    // 5. Case-insensitive name
    const rc::WriteResult login2 = users.note(0, fleet[0]->insertAndAwait(RpUser{.provider = 1, .subject = "steam:43"}));
    ASSERT_TRUE(login2.ok());
    const uint64_t k2 = login2.key();
    EXPECT_TRUE(isConstraint(create(1, k2, "foo"), *fleet[1]->client, "character_name", "unique"));

    // 6. The 13th slot
    const rc::WriteResult login3 = users.note(0, fleet[0]->insertAndAwait(RpUser{.provider = 1, .subject = "steam:44"}));
    ASSERT_TRUE(login3.ok());
    const uint64_t k3 = login3.key();
    std::vector<std::pair<std::size_t, rc::Token>> pending;
    for (std::size_t i = 0; i < 13; ++i) pending.emplace_back(i % fleet.size(), fleet[i % fleet.size()]->insert(character(k3, "k3c" + std::to_string(i))));
    std::vector<uint64_t> k3Ids;
    std::vector<std::string> k3Names;
    int accepted = 0, refused = 0;
    int64_t refusedOffset = -1;
    for (std::size_t i = 0; i < pending.size(); ++i) {
        const rc::WriteResult r = chars.note(0, fleet[pending[i].first]->await(pending[i].second));
        if (r.ok()) { ++accepted; k3Ids.push_back(r.key()); k3Names.push_back("k3c" + std::to_string(i)); }
        else if (isConstraint(r, *fleet[pending[i].first]->client, "account_stats", "max_characters")) { ++refused; refusedOffset = r.offset(); }
        else ADD_FAILURE() << "unexpected answer " << r.code() << " status " << static_cast<int>(r.status());
    }
    EXPECT_EQ(accepted, 12);
    EXPECT_EQ(refused, 1);
    EXPECT_EQ(statsOf(*fleet[0]->client, k3), std::optional<uint64_t>(12));
    accountCharacters.builder.clear();
    EXPECT_EQ(scanAll(*fleet[0]->client, accountCharacters, bytesOf(accountCharacters.builder.setUInt("userId", k3).prefix())).size(), 12u);
    // A refused create's answer carries no id: its id is the one of its record in the leader's log.
    uint64_t refusedId = 0;
    for (const auto& rec : readLeader(c, characterTopic).log.data)
        if (rec.offset == refusedOffset) refusedId = rec.id;
    ASSERT_GE(refusedOffset, 0);
    ASSERT_NE(refusedId, 0u) << "the refused create's record is in the log at offset " << refusedOffset;
    const rc::WriteResult after = create(2, k2, "Bar");
    ASSERT_TRUE(after.ok()) << "code " << after.code();
    EXPECT_GT(after.key(), refusedId) << "the refused create's id is burned";
    ASSERT_FALSE(HasFailure());

    // 7. Restart mid-scenario
    fleet.clear();
    for (const int id : c.runningNodes()) c.stop(id);
    c.startNodes({1, 2, 3});
    ready(c);
    ASSERT_FALSE(HasFailure());
    ASSERT_TRUE(leadersWithin(c, characterTopic, leaders));
    ASSERT_TRUE(leadersWithin(c, userTopic, leaders));
    fleet = makeFleet(c, 4);
    EXPECT_EQ(statsOf(*fleet[0]->client, k3), std::optional<uint64_t>(12)) << "k3 still has 12";
    EXPECT_TRUE(isConstraint(create(0, k3, "k3c13"), *fleet[0]->client, "account_stats", "max_characters"));
    EXPECT_TRUE(isConstraint(create(1, k2, "foo"), *fleet[1]->client, "character_name", "unique"));
    {
        const auto row = rowWhenReady(*fleet[2]->client, characterName, nameKey("foo"));
        ASSERT_TRUE(row.has_value());
        EXPECT_EQ(characterName.u(*row, "characterId"), c1);
        const auto login = rowWhenReady(*fleet[2]->client, userIdentity, loginKey("steam:42"));
        ASSERT_TRUE(login.has_value());
        EXPECT_EQ(userIdentity.u(*login, "userId"), k);
    }
    ASSERT_FALSE(HasFailure());

    // 8. Delete, reuse, count
    const uint64_t cx = k3Ids.front();
    const std::string cxName = k3Names.front();
    const rc::WriteResult removed = chars.note(cx, fleet[0]->removeExistingAndAwait(cx), true);
    ASSERT_TRUE(removed.ok()) << "code " << removed.code();
    EXPECT_EQ(statsOf(*fleet[0]->client, k3), std::optional<uint64_t>(11));
    EXPECT_TRUE(create(1, k2, cxName).ok()) << "the name is free again";
    EXPECT_TRUE(create(2, k3, "k3again").ok());
    EXPECT_EQ(statsOf(*fleet[0]->client, k3), std::optional<uint64_t>(12));
    const rc::WriteResult twice = chars.note(cx, fleet[0]->removeExistingAndAwait(cx), true);
    EXPECT_EQ(twice.code(), kKeyNotFound);
    ASSERT_FALSE(HasFailure());

    // 9. Never delivered: a raw subscription from 0 on user and on character
    const rc::WriteResult sentinelUser = users.note(0, fleet[0]->insertAndAwait(RpUser{.provider = 1, .subject = "steam:99"}));
    const rc::WriteResult sentinelChar = create(0, 5, "Sentinel");
    ASSERT_TRUE(sentinelUser.ok());
    ASSERT_TRUE(sentinelChar.ok());
    const auto delivered = [&](const std::string& topic, int32_t topicId, const Ledger& ledger, int64_t sentinel) {
        ASSERT_TRUE(leadersWithin(c, topicId, leaders));
        rc::ClientConfig cfg = clientConfigFor(c, leaders[0]);
        cfg.topics.push_back(rc::TopicSpec{topic, topicId, 1});
        rc::Client reader(cfg);
        reader.connectAndWait(10s);
        std::mutex mu;
        std::set<int64_t> got;
        rc::SubscribeOptions o;
        o.from = rc::From::earliest();
        rc::Subscription sub = reader.subscribe(reader.topic(topic), 0, o, [&](const rc::Record& r) {
            std::lock_guard<std::mutex> g(mu);
            got.insert(r.offset);
        });
        const bool arrived = waitFor([&] { std::lock_guard<std::mutex> g(mu); return got.contains(sentinel); }, 10s);
        sub.unsubscribe();
        reader.close();
        ASSERT_TRUE(arrived) << topic << ": the newest accepted record never arrived";
        std::lock_guard<std::mutex> g(mu);
        for (const int64_t off : ledger.rejected) EXPECT_FALSE(got.contains(off)) << topic << ": the rejected record at offset " << off << " was delivered";
        for (const int64_t off : ledger.accepted) EXPECT_TRUE(got.contains(off)) << topic << ": the accepted record at offset " << off << " was not delivered";
    };
    delivered("character", characterTopic, chars, sentinelChar.offset());
    delivered("user", userTopic, users, sentinelUser.offset());
    ASSERT_FALSE(HasFailure());
    EXPECT_FALSE(chars.rejected.empty());
    EXPECT_FALSE(users.rejected.empty());

    // 10. Gone after compaction
    compactPast(c, *fleet[0], characterTopic, *chars.rejected.rbegin(), chars, c1, character(k, "Foo", c1),
                [](Writer& w, const RpCharacter& r) { return w.update(r); });
    compactPast(c, *fleet[0], userTopic, *users.rejected.rbegin(), users, k, RpUser{.userId = k, .provider = 1, .subject = "steam:42"},
                [](Writer& w, const RpUser& r) { return w.update(r); });
    ASSERT_FALSE(HasFailure());
    // Only a partition's leader compacts; a follower keeps the records it fetched.
    const auto judge = [&](const std::string& topic, int32_t topicId, const Ledger& ledger) {
        const LeaderLog l = readLeader(c, topicId);
        ASSERT_GE(l.node, 1);
        std::map<uint64_t, int64_t> last;
        for (const auto& rec : l.log.data) {
            EXPECT_FALSE(ledger.rejected.contains(rec.offset)) << topic << ": a record at the rejected offset " << rec.offset << " is still in node " << l.node << "'s log";
            last[rec.id] = rec.offset;
        }
        for (const auto& [id, off] : last)
            if (!ledger.latest.contains(id)) ADD_FAILURE() << topic << ": key " << id << " has a record but no accepted write";
        for (const auto& [id, off] : ledger.latest) {
            const auto it = last.find(id);
            if (ledger.removed.contains(id)) {   // its newest accepted write is a tombstone: the key is gone, or only the tombstone is left
                if (it != last.end()) EXPECT_EQ(it->second, off) << topic << ": deleted key " << id << " has a record newer or older than its tombstone";
            } else {
                ASSERT_NE(it, last.end()) << topic << ": key " << id << " has no record left, its newest accepted write is at offset " << off;
                EXPECT_EQ(it->second, off) << topic << ": key " << id << "'s newest record is not its newest accepted write";
            }
        }
        EXPECT_TRUE(waitFor([&] {
            return runCli({"stats", "topic", topic, "--json", "--broker", endpointOf(c, l.node)}, &out, &err) == 0 && out.find("\"rejectedNotCompacted\": 0") != std::string::npos;
        }, 10s)) << topic << ": " << out << err;
    };
    judge("character", characterTopic, chars);
    judge("user", userTopic, users);
}

// R04a on real brokers: the leader fails right after `drop view`; the drop finishes on the new leader and nothing hangs.
TEST(CheckedWritesRegressorsPathRegression, FailoverDuringDropFinishesTheDropThreeNodesRegression) {
    using namespace rune::node::test::catalog_cluster;
    ThreeNodes c = startCluster("cw09_drop");
    ready(c);
    ASSERT_FALSE(HasFatalFailure());
    std::string out, err;
    ASSERT_EQ(runCli({"ql", "-f", eagerScript(), "--broker", endpointOf(c, 1)}, &out, &err), 0) << err;
    const int32_t characterTopic = topicIdOf(c.node(awaitController(c)), "character");
    ASSERT_GE(characterTopic, 0);
    std::vector<int32_t> leaders;
    ASSERT_TRUE(leadersWithin(c, characterTopic, leaders));
    const int32_t leader = leaders[0];
    const int survivor = leader == 1 ? 2 : 1;

    auto w = std::make_unique<Writer>(clientConfigFor(c, survivor));
    ASSERT_TRUE(w->insertAndAwait(character(1, "Foo")).ok());
    std::vector<rc::Token> inFlight;
    for (int i = 0; i < 12; ++i) inFlight.push_back(w->insert(character(2 + static_cast<uint64_t>(i % 3), "f" + std::to_string(i))));
    ASSERT_EQ(ql(endpointOf(c, survivor), "drop view character_name;", &out, &err), 0) << err;
    c.crash(leader);
    c.startNode(leader);
    const auto inFlightDeadline = std::chrono::steady_clock::now() + 10s;
    for (const rc::Token t : inFlight) {
        const rc::WriteResult r = w->await(t, std::max(1ms, std::chrono::duration_cast<std::chrono::milliseconds>(inFlightDeadline - std::chrono::steady_clock::now())));
        EXPECT_NE(r.status(), rc::OpStatus::Timeout);
        EXPECT_NE(r.status(), rc::OpStatus::Closed);
    }

    ready(c);
    ASSERT_FALSE(HasFatalFailure());
    ASSERT_TRUE(leadersWithin(c, characterTopic, leaders));
    EXPECT_TRUE(waitFor([&] {
        for (const int id : c.runningNodes()) {
            const int32_t worker = rune::metadata::ShardRouter(c.node(id).workerCountForTest()).workerForPartition(characterTopic, 0);
            const bool waiting = c.node(id).onWorkerThreadForTest(worker, [characterTopic](Worker& wk) {
                const VerdictSource* j = wk.viewSet()->verdictSource(characterTopic, 0);
                return j != nullptr && j->waitingForInstall();
            });
            if (waiting) return false;
        }
        return true;
    }, 10s)) << "a verdict source still waits for an install";
    w.reset();
    w = std::make_unique<Writer>(clientConfigFor(c, survivor));
    const rc::WriteResult dup = w->insertAndAwait(character(9, "foo"));
    EXPECT_TRUE(dup.ok()) << "the dropped unique view no longer refuses a duplicate: code " << dup.code();
}

namespace {

// topic pc { id uint64 key, userId uint64, name string(16) }
rune::codec::Descriptor pcDescriptor() {
    return rune::codec::DescriptorBuilder("pc", 1)
        .add("id", PrimType::UInt64, 1, /*isKey=*/true)
        .add("userId", PrimType::UInt64)
        .addFixedStr("name", 16)
        .build();
}
struct Pc {
    static const uint64_t kFingerprint;
    static constexpr std::string_view kSchemaName = "pc";
    static constexpr uint16_t kVersion = 1, kBlockLength = 32;
    uint64_t    id = 0;
    uint64_t    userId = 0;
    std::string name;
    [[nodiscard]] std::size_t encodedSize() const noexcept { return 4 + kBlockLength; }
    [[nodiscard]] std::size_t encode(uint8_t* b) const noexcept {
        rpdetail::putHeader(b, kBlockLength, kVersion);
        rpdetail::putLe(b, 4, id, 8);
        rpdetail::putLe(b, 12, userId, 8);
        rpdetail::putStr(b, 20, name, 16);
        return encodedSize();
    }
    [[nodiscard]] uint64_t key() const noexcept { return id; }
};
const uint64_t Pc::kFingerprint = rune::codec::fingerprint(pcDescriptor());

/// A writer of `pc` that does not ask for a verdict (or, as the first case, does): plain upserts.
struct PcWriter {
    std::unique_ptr<rc::Client>                              client;
    std::optional<rc::Producer>                              prod;
    std::optional<rc::TypedTopic<Pc, rc::ProducerRef>>       topic;
    std::optional<rc::Completions>                           cq;
    rc::Token                                                next = 1;

    PcWriter(rc::ClientConfig cfg, rc::Acks acks) {
        client = std::make_unique<rc::Client>(cfg);
        rc::ProducerConfig pc;
        pc.acks     = acks;
        pc.lingerUs = 1;
        prod.emplace(client->producer(pc));
        client->connectAndWait(10s);
        topic.emplace(client->openTopicAndWait<Pc>("pc", *prod, 10s));
        cq.emplace(*client);
    }
    ~PcWriter() {
        cq.reset();
        if (prod) prod->closeImmediately();
        if (client) client->close();
    }
    rc::Token upsert(const Pc& r) {
        topic->upsert(r, *cq, next);
        prod->flush();
        return next++;
    }
    std::optional<rc::WriteResult> await(rc::Token t) {
        std::optional<rc::WriteResult> r;
        if (cq->waitFor(t, 10s, [&](const rc::Completion& c, std::span<const uint8_t>) { r = rc::WriteResult::fromCompletion(c); }) != rc::Completions::Wait::Done) return std::nullopt;
        return r;
    }
};

}  // namespace

// §11.2: a write that asks for no verdict is judged all the same; its answer is today's and a rejected record is simply never visible.
TEST(CheckedWritesRegressorsPathRegression, OldWritersOnAConstrainedSourceShardRegression) {
    OneBroker b("cw09_old", OneBroker::Runtime::Shard, fastWarmup);
    awaitLevel(b.node());
    ASSERT_FALSE(HasFatalFailure());
    std::string out, err;
    ASSERT_EQ(ql(b.endpoint(), "topic pc { id uint64, userId uint64, name string(16) } key id with (cleanup.policy = compact);", &out, &err), 0) << err;
    ASSERT_EQ(ql(b.endpoint(), "create unique view pc_name key lower(name) as nameKey as select id from pc;", &out, &err), 0) << err;
    ViewHandle view(b.node(), "pc_name");

    // The client has no idempotence switch: an acks=none batch is its non-idempotent wire path (producer id -1) and gets no completion, so
    // "today's completion" is asserted for acks=leader, acks=all and the v11 session only; the pid -1 check path has model-level tests.
    struct Kind { rc::Acks acks; int32_t announce; };
    uint64_t id = 1;
    std::optional<int64_t> firstFoo;
    std::vector<std::unique_ptr<PcWriter>> writers;   // an acks=none writer has nothing to await: it lives until the broker has judged its records
    for (const Kind kind : {Kind{rc::Acks::Leader, 0}, Kind{rc::Acks::All, 0}, Kind{rc::Acks::None, 0}, Kind{rc::Acks::All, 11}}) {
        rc::ClientConfig cfg = b.clientConfig();
        cfg.testAnnounceClientVersion = kind.announce;
        PcWriter& w = *writers.emplace_back(std::make_unique<PcWriter>(cfg, kind.acks));
        for (const char* name : {"Foo", "foo"}) {
            const rc::Token t = w.upsert(Pc{.id = id, .userId = 7, .name = name});
            ++id;
            if (kind.acks == rc::Acks::None) continue;
            const auto r = w.await(t);
            ASSERT_TRUE(r.has_value()) << "acks " << static_cast<int>(kind.acks) << " announce " << kind.announce;
            EXPECT_EQ(r->status(), rc::OpStatus::Ok) << "acks " << static_cast<int>(kind.acks) << " announce " << kind.announce << " " << name << ": no verdict reaches a writer that did not ask";
            if (!firstFoo) firstFoo = r->offset();
        }
    }
    ASSERT_TRUE(firstFoo.has_value());
    ASSERT_TRUE(waitFor([&] {
        return runCli({"stats", "topic", "pc", "--json", "--broker", b.endpoint()}, &out, &err) == 0 && out.find("\"rejectionsConstraint\": 7,") != std::string::npos;
    }, 10s)) << out << err;

    rc::Client& reader = *b.connectClient();
    auto sentinel = PcWriter(b.clientConfig(), kAllView);
    const auto last = sentinel.await(sentinel.upsert(Pc{.id = 100, .userId = 7, .name = "Sentinel"}));
    ASSERT_TRUE(last.has_value());
    ASSERT_TRUE(last->ok());

    // The one lower(name) row of "foo" is the first writer's.
    rcv::ViewKeyBuilder foo = keyBuilder(b.node(), "pc_name");
    const auto row = rowWhenReady(reader, view, bytesOf(foo.setString("name", "foo").key()));
    ASSERT_TRUE(row.has_value());
    EXPECT_EQ(view.u(*row, "id"), 1u);

    // A consumer from 0 sees the accepted records: the first "Foo" and the sentinel.
    rc::ClientConfig cfg = b.clientConfig();
    const int32_t topicId = topicIdOf(b.node(), "pc");
    cfg.topics.push_back(rc::TopicSpec{"pc", topicId, 1});
    rc::Client consumer(cfg);
    consumer.connectAndWait(10s);
    std::mutex mu;
    std::set<int64_t> got;
    rc::SubscribeOptions o;
    o.from = rc::From::earliest();
    rc::Subscription sub = consumer.subscribe(consumer.topic("pc"), 0, o, [&](const rc::Record& r) {
        std::lock_guard<std::mutex> g(mu);
        got.insert(r.offset);
    });
    EXPECT_TRUE(waitFor([&] { std::lock_guard<std::mutex> g(mu); return got.contains(last->offset()); }, 10s));
    sub.unsubscribe();
    consumer.close();
    std::lock_guard<std::mutex> g(mu);
    EXPECT_EQ(got, (std::set<int64_t>{*firstFoo, last->offset()}));
}

// R07c: a composite group-by check.
TEST(CheckedWritesRegressorsPathRegression, CompositeGroupByCheckShardRegression) {
    OneBroker b("cw09_pg", OneBroker::Runtime::Shard, fastWarmup);
    awaitLevel(b.node());
    ASSERT_FALSE(HasFatalFailure());
    std::string out, err;
    ASSERT_EQ(ql(b.endpoint(), goldenTopic("character"), &out, &err), 0) << err;
    ASSERT_EQ(ql(b.endpoint(),
                 "create view per_gender key (userId, gender) as select count(*) as n from character group by userId, gender constraint two_per_gender check (n <= 2);",
                 &out, &err), 0) << err;
    Writer w(b.clientConfig(), false);
    ASSERT_TRUE(w.insertAndAwait(character(7, "a", 0, 1)).ok());
    ASSERT_TRUE(w.insertAndAwait(character(7, "b", 0, 1)).ok());
    EXPECT_TRUE(isConstraint(w.insertAndAwait(character(7, "c", 0, 1)), *w.client, "per_gender", "two_per_gender"));
    EXPECT_TRUE(w.insertAndAwait(character(7, "c", 0, 2)).ok()) << "another gender has its own count";
    EXPECT_TRUE(w.insertAndAwait(character(8, "d", 0, 1)).ok()) << "another user has its own count";
}

// A replace of the check view while creates are running: nothing times out, the old cap holds up to the change record, the new one after it.
TEST(CheckedWritesRegressorsPathRegression, ReplaceWhileWritingShardRegression) {
    constexpr uint64_t kUsers = 20;
    OneBroker b("cw09_rep", OneBroker::Runtime::Shard, fastWarmup);
    awaitLevel(b.node());
    ASSERT_FALSE(HasFatalFailure());
    std::string out, err;
    ASSERT_EQ(ql(b.endpoint(), goldenTopic("character"), &out, &err), 0) << err;
    ASSERT_EQ(ql(b.endpoint(), "create view account_stats key userId as select count(*) as characters from character group by userId constraint max_characters check (characters <= 12);",
                 &out, &err), 0) << err;
    const int32_t characterTopic = topicIdOf(b.node(), "character");

    struct Done { uint64_t user; rc::OpStatus status; uint16_t code; int64_t offset; };
    std::mutex mu;
    std::vector<Done> done;
    std::atomic<bool> stop{false};
    std::string writerError;
    std::thread producer([&] {
        try {
            Writer w(b.clientConfig(), false);
            for (uint64_t n = 0; !stop.load(); ++n) {
                const uint64_t user = 1 + n % kUsers;
                const rc::WriteResult r = w.insertAndAwait(character(user, "c" + std::to_string(n)));
                std::lock_guard<std::mutex> g(mu);
                done.push_back({user, r.status(), r.code(), r.offset()});
            }
        } catch (const std::exception& e) {
            writerError = e.what();
        }
    });
    const auto okPerUser = [&](int64_t below) {
        std::map<uint64_t, int> n;
        std::lock_guard<std::mutex> g(mu);
        for (const Done& d : done)
            if (d.status == rc::OpStatus::Ok && d.offset < below) ++n[d.user];
        return n;
    };
    const auto everyUserHas = [&](int want) {
        const auto n = okPerUser(INT64_MAX);
        return n.size() == kUsers && std::all_of(n.begin(), n.end(), [want](const auto& kv) { return kv.second >= want; });
    };
    const bool atCap = waitFor([&] { return everyUserHas(12); }, 10s);
    const int replaced = atCap ? ql(b.endpoint(), "create or replace view account_stats key userId as select count(*) as characters from character group by userId constraint max_characters check (characters <= 13);",
                                    &out, &err) : -1;
    const bool atNewCap = replaced == 0 && waitFor([&] { return everyUserHas(13); }, 10s);
    stop = true;
    producer.join();
    ASSERT_TRUE(writerError.empty()) << writerError;
    ASSERT_TRUE(atCap) << "every user reached 12";
    ASSERT_EQ(replaced, 0) << err;
    ASSERT_TRUE(atNewCap) << "every user reached 13 after the replace";

    int64_t changeOffset = -1;
    b.node().onOwnerThreadForTest(characterTopic, 0, [&](rune::replication::PartitionReplica& r) {
        const auto log = cw::readLogOf(*r.log(), r.highWatermark());
        if (!log.changes.empty()) changeOffset = log.changes.back().offset;
        return 0;
    });
    ASSERT_GE(changeOffset, 0);
    for (const auto& [user, n] : okPerUser(changeOffset)) EXPECT_LE(n, 12) << "user " << user << " before the replace's change record";
    for (const auto& [user, n] : okPerUser(INT64_MAX)) EXPECT_LE(n, 13) << "user " << user;
    std::lock_guard<std::mutex> g(mu);
    for (const Done& d : done) {
        EXPECT_NE(d.status, rc::OpStatus::Timeout);
        if (d.status != rc::OpStatus::Ok) EXPECT_EQ(d.code, kConstraintViolation) << "user " << d.user;
    }
}

// ============================================================================
// The default (non-stress) cuts of TheWholeScenarioThreeNodesRegression and FailoverDuringDropFinishesTheDropThreeNodesRegression, which
// stay below under the stress label.  Each cut is one property: a single broker where the property is not cross-node, the smallest
// three-broker cluster where it is.  ASSERTION MAP: cleanup-report.md.
// ============================================================================
namespace {

/// `st` as a statement of a script.
std::string stmt(std::string st) {
    if (st.empty() || st.back() != ';') st += ';';
    return st + "\n";
}

const std::string kUserIdentity   = "create unique view user_identity key (provider, subject) as select userId from user;\n";
const std::string kCharacterName  = "create unique view character_name key lower(name) as nameKey as select characterId from character;\n";
const std::string kCharacterById  = "create view character_by_id key characterId as select userId, name from character;\n";
const std::string kAccountChars   = "create view account_characters key (userId, characterId) as select name from character;\n";
std::string accountStats(int cap) {
    return "create view account_stats key userId as select count(*) as characters from character group by userId constraint max_characters check (characters <= " +
           std::to_string(cap) + ");\n";
}

/// The golden topic `name` on one broker; `eager`: small segments and a cleaner that drops tombstones and old records at once.
std::string topicDdl(const std::string& name, bool eager = false) {
    std::string st = goldenTopic(name);
    if (eager) {
        const std::string plain = "with (cleanup.policy = compact)";
        st.replace(st.find(plain), plain.size(),
                   "with (cleanup.policy = compact, segment.bytes = 4096, delete.retention.ms = 0, min.compaction.lag.ms = 50, "
                   "min.cleanable.dirty.ratio = 0.000001, file.delete.delay.ms = 50)");
    }
    return stmt(st);
}

/// A script through one `rune ql -f` (one client and driver for all its statements); its output is every statement's.
int qlScript(const std::string& endpoint, const std::string& text, std::string* out, std::string* err) {
    static int seq = 0;
    const fs::path path = tmpDir() / ("cut" + std::to_string(seq++) + ".rql");
    std::ofstream(path, std::ios::binary) << text;
    return runCli({"ql", "-f", path.string(), "--broker", endpoint}, out, err);
}

/// The newest records of topic `topicId` on the single broker, read below the high watermark on the replica's thread.
LeaderLog readOwner(BrokerNode& n, int32_t topicId) {
    LeaderLog out;
    out.node = 1;
    n.onOwnerThreadForTest(topicId, 0, [&out](rune::replication::PartitionReplica& r) {
        out.cleanedThrough = r.lastCleanOffset();
        out.log            = cw::readLogOf(*r.log(), r.highWatermark());
        return 0;
    });
    return out;
}

/// The topic's accepted updates until its cleaner has passed `through` (32 records per chunk).
template <class Rec, class Send>
void compactOwner(BrokerNode& n, Writer& w, int32_t topicId, int64_t through, Ledger& ledger, uint64_t id, const Rec& rec, Send&& send) {
    for (int chunk = 0; chunk < 4; ++chunk) {
        std::vector<rc::Token> tokens;
        for (int i = 0; i < 32; ++i) tokens.push_back(send(w, rec));
        for (const rc::Token t : tokens) ledger.note(id, w.await(t));
    }
    EXPECT_TRUE(waitFor([&] { return readOwner(n, topicId).cleanedThrough > through; }, 10s)) << "the cleaner never passed offset " << through << " of topic " << topicId;
}

/// What compaction must have left of `topic`: no rejected record, the newest accepted record of every key, and the stats saying so.
void judgeOwner(BrokerNode& n, const std::string& endpoint, const std::string& topic, int32_t topicId, const Ledger& ledger) {
    const LeaderLog l = readOwner(n, topicId);
    std::map<uint64_t, int64_t> last;
    for (const auto& rec : l.log.data) {
        EXPECT_FALSE(ledger.rejected.contains(rec.offset)) << topic << ": a record at the rejected offset " << rec.offset << " is still in the log";
        last[rec.id] = rec.offset;
    }
    for (const auto& [id, off] : last)
        if (!ledger.latest.contains(id)) ADD_FAILURE() << topic << ": key " << id << " has a record but no accepted write";
    for (const auto& [id, off] : ledger.latest) {
        const auto it = last.find(id);
        if (ledger.removed.contains(id)) {
            if (it != last.end()) EXPECT_EQ(it->second, off) << topic << ": deleted key " << id << " has a record newer or older than its tombstone";
        } else {
            ASSERT_NE(it, last.end()) << topic << ": key " << id << " has no record left, its newest accepted write is at offset " << off;
            EXPECT_EQ(it->second, off) << topic << ": key " << id << "'s newest record is not its newest accepted write";
        }
    }
    std::string out, err;
    EXPECT_TRUE(waitFor([&] {
        return runCli({"stats", "topic", topic, "--json", "--broker", endpoint}, &out, &err) == 0 && out.find("\"rejectedNotCompacted\": 0") != std::string::npos;
    }, 10s)) << topic << ": " << out << err;
}

/// A raw subscription from 0 on `topic`: the rejected records of `ledger` are never delivered, the accepted ones always are.
void expectDelivery(rc::ClientConfig cfg, const std::string& topic, int32_t topicId, const Ledger& ledger, int64_t sentinel) {
    cfg.topics.push_back(rc::TopicSpec{topic, topicId, 1});
    rc::Client reader(cfg);
    reader.connectAndWait(10s);
    std::mutex mu;
    std::set<int64_t> got;
    rc::SubscribeOptions o;
    o.from = rc::From::earliest();
    rc::Subscription sub = reader.subscribe(reader.topic(topic), 0, o, [&](const rc::Record& r) {
        std::lock_guard<std::mutex> g(mu);
        got.insert(r.offset);
    });
    const bool arrived = waitFor([&] { std::lock_guard<std::mutex> g(mu); return got.contains(sentinel); }, 10s);
    sub.unsubscribe();
    reader.close();
    ASSERT_TRUE(arrived) << topic << ": the newest accepted record never arrived";
    std::lock_guard<std::mutex> g(mu);
    for (const int64_t off : ledger.rejected) EXPECT_FALSE(got.contains(off)) << topic << ": the rejected record at offset " << off << " was delivered";
    for (const int64_t off : ledger.accepted) EXPECT_TRUE(got.contains(off)) << topic << ": the accepted record at offset " << off << " was not delivered";
}

}  // namespace

// Whole step 2: two logins of one subject race; the unique view lets exactly one win.
TEST(CheckedWritesRegressorsPathRegression, LoginRaceAdmitsOneWinnerShardRegression) {
    OneBroker b("cw09_race", OneBroker::Runtime::Shard, fastWarmup);
    awaitLevel(b.node());
    ASSERT_FALSE(HasFatalFailure());
    std::string out, err;
    ASSERT_EQ(qlScript(b.endpoint(), topicDdl("user") + kUserIdentity, &out, &err), 0) << err;
    ViewHandle userIdentity(b.node(), "user_identity");
    Writer a(b.clientConfig(), true, false), c(b.clientConfig(), true, false);
    Ledger users;
    userIdentity.builder.clear();
    const auto key = bytesOf(userIdentity.builder.setUInt("provider", 1).setString("subject", "steam:42").key());
    for (Writer* w : {&a, &c})
        ASSERT_TRUE(waitFor([&] { return readRow(*w->client, userIdentity, key).status == rc::ViewRowResult::kAbsentKey; }, 10s)) << "no login row yet";
    const rc::Token ta = a.insert(RpUser{.provider = 1, .subject = "steam:42"});
    const rc::Token tb = c.insert(RpUser{.provider = 1, .subject = "steam:42"});
    const rc::WriteResult ra = users.note(0, a.await(ta)), rb = users.note(0, c.await(tb));
    ASSERT_NE(ra.ok(), rb.ok()) << "exactly one login wins: " << ra.code() << " / " << rb.code();
    const rc::WriteResult& win = ra.ok() ? ra : rb;
    const rc::WriteResult& lose = ra.ok() ? rb : ra;
    rc::Client& loser = *(ra.ok() ? c : a).client;
    ASSERT_GE(win.key(), 1u);
    EXPECT_TRUE(isConstraint(lose, loser, "user_identity", "unique")) << "code " << lose.code();
    const auto row = readRow(loser, userIdentity, key, lose.offset());
    ASSERT_EQ(row.status, rc::ViewRowResult::kOk) << "the loser reads the winner's row on the first try";
    EXPECT_EQ(userIdentity.u(row.rowBytes, "userId"), win.key());
}

// Whole steps 1 (the shown views), 4 and 5.
TEST(CheckedWritesRegressorsPathRegression, ViewsAreShownAndCreatesReturnTheirIdsShardRegression) {
    OneBroker b("cw09_names", OneBroker::Runtime::Shard, fastWarmup);
    awaitLevel(b.node());
    ASSERT_FALSE(HasFatalFailure());
    std::string out, err;
    ASSERT_EQ(qlScript(b.endpoint(), topicDdl("character") + kCharacterName + accountStats(12) + kCharacterById + "show view character_name;\nshow view account_stats;\n",
                       &out, &err), 0) << err;
    EXPECT_NE(out.find("unique"), std::string::npos) << out;
    EXPECT_NE(out.find("keyspec lower(name) as nameKey"), std::string::npos) << out;
    EXPECT_NE(out.find("constraint max_characters check (characters <= 12)"), std::string::npos) << out;
    ViewHandle characterById(b.node(), "character_by_id");
    Writer w(b.clientConfig(), false);
    const rc::WriteResult foo = w.insertAndAwait(character(7, "Foo"));
    ASSERT_TRUE(foo.ok()) << "code " << foo.code();
    const uint64_t c1 = foo.key();
    ASSERT_GE(c1, 1u);
    const auto row = readRow(*w.client, characterById, characterById.keyOf(c1), foo.offset());
    ASSERT_EQ(row.status, rc::ViewRowResult::kOk);
    EXPECT_EQ(characterById.u(row.rowBytes, "userId"), 7u);
    EXPECT_TRUE(isConstraint(w.insertAndAwait(character(8, "foo")), *w.client, "character_name", "unique"));
}

// Whole step 6: the 13th character of an account is refused, its id is burned.
TEST(CheckedWritesRegressorsPathRegression, ThirteenthCharacterIsRefusedAndItsIdBurnedShardRegression) {
    OneBroker b("cw09_slot", OneBroker::Runtime::Shard, fastWarmup);
    awaitLevel(b.node());
    ASSERT_FALSE(HasFatalFailure());
    std::string out, err;
    ASSERT_EQ(qlScript(b.endpoint(), topicDdl("character") + accountStats(12) + kAccountChars, &out, &err), 0) << err;
    ViewHandle accountStats(b.node(), "account_stats"), accountCharacters(b.node(), "account_characters");
    const int32_t characterTopic = topicIdOf(b.node(), "character");
    ASSERT_GE(characterTopic, 0);
    Writer w0(b.clientConfig(), false), w1(b.clientConfig(), false);
    Ledger chars;
    constexpr uint64_t k3 = 3;
    std::vector<std::pair<Writer*, rc::Token>> pending;
    for (std::size_t i = 0; i < 13; ++i) {
        Writer& w = i % 2 == 0 ? w0 : w1;
        pending.emplace_back(&w, w.insert(character(k3, "k3c" + std::to_string(i))));
    }
    int accepted = 0, refused = 0;
    int64_t refusedOffset = -1;
    for (auto& [w, token] : pending) {
        const rc::WriteResult r = chars.note(0, w->await(token));
        if (r.ok()) ++accepted;
        else if (isConstraint(r, *w->client, "account_stats", "max_characters")) { ++refused; refusedOffset = r.offset(); }
        else ADD_FAILURE() << "unexpected answer " << r.code() << " status " << static_cast<int>(r.status());
    }
    EXPECT_EQ(accepted, 12);
    EXPECT_EQ(refused, 1);
    const auto stats = rowWhenReady(*w0.client, accountStats, accountStats.keyOf(k3));
    ASSERT_TRUE(stats.has_value());
    EXPECT_EQ(accountStats.u(*stats, "characters"), 12u);
    accountCharacters.builder.clear();
    EXPECT_EQ(scanAll(*w0.client, accountCharacters, bytesOf(accountCharacters.builder.setUInt("userId", k3).prefix())).size(), 12u);
    // A refused create's answer carries no id: its id is the one of its record in the log.
    uint64_t refusedId = 0;
    for (const auto& rec : readOwner(b.node(), characterTopic).log.data)
        if (rec.offset == refusedOffset) refusedId = rec.id;
    ASSERT_GE(refusedOffset, 0);
    ASSERT_NE(refusedId, 0u) << "the refused create's record is in the log at offset " << refusedOffset;
    const rc::WriteResult after = w0.insertAndAwait(character(4, "Bar"));
    ASSERT_TRUE(after.ok()) << "code " << after.code();
    EXPECT_GT(after.key(), refusedId) << "the refused create's id is burned";
}

// Whole step 7 on one broker: a restart keeps the rows, the limit and the names.
TEST(CheckedWritesRegressorsPathRegression, RestartKeepsTheLimitsAndTheNamesShardRegression) {
    OneBroker b("cw09_restart", OneBroker::Runtime::Shard, fastWarmup);
    awaitLevel(b.node());
    ASSERT_FALSE(HasFatalFailure());
    std::string out, err;
    ASSERT_EQ(qlScript(b.endpoint(), topicDdl("user") + kUserIdentity + topicDdl("character") + kCharacterName + accountStats(2), &out, &err), 0) << err;
    uint64_t k = 0, c1 = 0;
    constexpr uint64_t kFull = 3;
    {
        Writer w(b.clientConfig());
        const rc::WriteResult login = w.insertAndAwait(RpUser{.provider = 1, .subject = "steam:42"});
        ASSERT_TRUE(login.ok());
        k = login.key();
        const rc::WriteResult foo = w.insertAndAwait(character(kFull, "Foo"));
        ASSERT_TRUE(foo.ok());
        c1 = foo.key();
        ASSERT_TRUE(w.insertAndAwait(character(kFull, "Bar")).ok());
    }
    b.restart();
    awaitLevel(b.node());
    ASSERT_FALSE(HasFatalFailure());
    ViewHandle userIdentity(b.node(), "user_identity"), characterName(b.node(), "character_name"), accountStats(b.node(), "account_stats");
    Writer w(b.clientConfig());
    const auto stats = rowWhenReady(*w.client, accountStats, accountStats.keyOf(kFull));
    ASSERT_TRUE(stats.has_value());
    EXPECT_EQ(accountStats.u(*stats, "characters"), 2u) << "the account still has 2";
    EXPECT_TRUE(isConstraint(w.insertAndAwait(character(kFull, "Baz")), *w.client, "account_stats", "max_characters"));
    EXPECT_TRUE(isConstraint(w.insertAndAwait(character(9, "foo")), *w.client, "character_name", "unique"));
    characterName.builder.clear();
    const auto name = rowWhenReady(*w.client, characterName, bytesOf(characterName.builder.setString("name", "foo").key()));
    ASSERT_TRUE(name.has_value());
    EXPECT_EQ(characterName.u(*name, "characterId"), c1);
    userIdentity.builder.clear();
    const auto login = rowWhenReady(*w.client, userIdentity, bytesOf(userIdentity.builder.setUInt("provider", 1).setString("subject", "steam:42").key()));
    ASSERT_TRUE(login.has_value());
    EXPECT_EQ(userIdentity.u(*login, "userId"), k);
}

// Whole step 8: a delete frees the name and the slot; deleting twice is KeyNotFound.
TEST(CheckedWritesRegressorsPathRegression, DeleteFreesTheNameAndTheSlotShardRegression) {
    OneBroker b("cw09_delete", OneBroker::Runtime::Shard, fastWarmup);
    awaitLevel(b.node());
    ASSERT_FALSE(HasFatalFailure());
    std::string out, err;
    ASSERT_EQ(qlScript(b.endpoint(), topicDdl("character") + kCharacterName + accountStats(2), &out, &err), 0) << err;
    ViewHandle accountStats(b.node(), "account_stats");
    Writer w(b.clientConfig(), false);
    Ledger chars;
    constexpr uint64_t k3 = 3;
    const rc::WriteResult first = chars.note(0, w.insertAndAwait(character(k3, "k3c0")));
    ASSERT_TRUE(first.ok());
    ASSERT_TRUE(chars.note(0, w.insertAndAwait(character(k3, "k3c1"))).ok());
    const auto statsOf = [&] {
        const auto row = rowWhenReady(*w.client, accountStats, accountStats.keyOf(k3));
        return row ? std::optional(accountStats.u(*row, "characters")) : std::nullopt;
    };
    const uint64_t cx = first.key();
    const rc::WriteResult removed = chars.note(cx, w.removeExistingAndAwait(cx), true);
    ASSERT_TRUE(removed.ok()) << "code " << removed.code();
    EXPECT_EQ(statsOf(), std::optional<uint64_t>(1));
    EXPECT_TRUE(chars.note(0, w.insertAndAwait(character(8, "k3c0"))).ok()) << "the name is free again";
    EXPECT_TRUE(chars.note(0, w.insertAndAwait(character(k3, "k3again"))).ok());
    EXPECT_EQ(statsOf(), std::optional<uint64_t>(2));
    const rc::WriteResult twice = chars.note(cx, w.removeExistingAndAwait(cx), true);
    EXPECT_EQ(twice.code(), kKeyNotFound);
}

// Whole steps 9 and 10: a rejected record is never delivered, and compaction removes it from the log.
TEST(CheckedWritesRegressorsPathRegression, RejectedRecordsAreNeverDeliveredAndCompactedAwayShardRegression) {
    OneBroker b("cw09_gone", OneBroker::Runtime::Shard, fastWarmup);
    awaitLevel(b.node());
    ASSERT_FALSE(HasFatalFailure());
    std::string out, err;
    ASSERT_EQ(qlScript(b.endpoint(), topicDdl("user", true) + kUserIdentity + topicDdl("character", true) + kCharacterName, &out, &err), 0) << err;
    const int32_t userTopic = topicIdOf(b.node(), "user"), characterTopic = topicIdOf(b.node(), "character");
    ASSERT_GE(userTopic, 0);
    ASSERT_GE(characterTopic, 0);
    Writer w(b.clientConfig());
    Ledger users, chars;
    const rc::WriteResult login = users.note(0, w.insertAndAwait(RpUser{.provider = 1, .subject = "steam:42"}));
    ASSERT_TRUE(login.ok());
    users.note(0, w.insertAndAwait(RpUser{.provider = 1, .subject = "steam:42"}));
    const rc::WriteResult foo = chars.note(0, w.insertAndAwait(character(login.key(), "Foo")));
    ASSERT_TRUE(foo.ok());
    chars.note(0, w.insertAndAwait(character(login.key(), "foo")));
    const rc::WriteResult sentinelUser = users.note(0, w.insertAndAwait(RpUser{.provider = 1, .subject = "steam:99"}));
    const rc::WriteResult sentinelChar = chars.note(0, w.insertAndAwait(character(5, "Sentinel")));
    ASSERT_TRUE(sentinelUser.ok());
    ASSERT_TRUE(sentinelChar.ok());
    EXPECT_FALSE(chars.rejected.empty());
    EXPECT_FALSE(users.rejected.empty());
    expectDelivery(b.clientConfig(), "character", characterTopic, chars, sentinelChar.offset());
    expectDelivery(b.clientConfig(), "user", userTopic, users, sentinelUser.offset());
    ASSERT_FALSE(HasFailure());
    compactOwner(b.node(), w, characterTopic, *chars.rejected.rbegin(), chars, foo.key(), character(login.key(), "Foo", foo.key()),
                 [](Writer& wr, const RpCharacter& r) { return wr.update(r); });
    compactOwner(b.node(), w, userTopic, *users.rejected.rbegin(), users, login.key(), RpUser{.userId = login.key(), .provider = 1, .subject = "steam:42"},
                 [](Writer& wr, const RpUser& r) { return wr.update(r); });
    ASSERT_FALSE(HasFailure());
    judgeOwner(b.node(), b.endpoint(), "character", characterTopic, chars);
    judgeOwner(b.node(), b.endpoint(), "user", userTopic, users);
}

// Whole step 3: the leader of a partition crashes and another node takes over.
TEST(CheckedWritesRegressorsPathRegression, ACrashedLeaderIsReplacedThreeNodesRegression) {
    using namespace rune::node::test::catalog_cluster;
    ThreeNodes c = startCluster("cw09_lead");
    ready(c);
    ASSERT_FALSE(HasFatalFailure());
    std::string out, err;
    ASSERT_EQ(qlScript(endpointOf(c, 1), stmt(goldenTopic("user")) + stmt(goldenTopic("character")), &out, &err), 0) << err;
    BrokerNode& admin = c.node(awaitController(c));
    const int32_t userTopic = topicIdOf(admin, "user"), characterTopic = topicIdOf(admin, "character");
    ASSERT_GE(userTopic, 0);
    ASSERT_GE(characterTopic, 0);
    std::vector<int32_t> leaders;
    ASSERT_TRUE(leadersWithin(c, characterTopic, leaders));
    const int32_t crashed = leaders[0];
    c.crash(crashed);
    ASSERT_TRUE(leadersWithin(c, characterTopic, leaders));
    ASSERT_NE(leaders[0], crashed);
    ASSERT_TRUE(leadersWithin(c, userTopic, leaders));
}

// Whole step 7 across nodes: every broker restarts and the limit still holds.
TEST(CheckedWritesRegressorsPathRegression, RestartOfEveryNodeKeepsTheLimitThreeNodesRegression) {
    using namespace rune::node::test::catalog_cluster;
    ThreeNodes c = startCluster("cw09_all");
    ready(c);
    ASSERT_FALSE(HasFatalFailure());
    std::string out, err;
    ASSERT_EQ(qlScript(endpointOf(c, 1), stmt(goldenTopic("character")) + kCharacterName + accountStats(2), &out, &err), 0) << err;
    const int32_t characterTopic = topicIdOf(c.node(awaitController(c)), "character");
    ASSERT_GE(characterTopic, 0);
    constexpr uint64_t kFull = 3;
    {
        Writer w(clientConfigFor(c, 1), false);
        ASSERT_TRUE(w.insertAndAwait(character(kFull, "Foo")).ok());
        ASSERT_TRUE(w.insertAndAwait(character(kFull, "Bar")).ok());
    }
    for (const int id : c.runningNodes()) c.stop(id);
    c.startNodes({1, 2, 3});
    ready(c);
    ASSERT_FALSE(HasFatalFailure());
    std::vector<int32_t> leaders;
    ASSERT_TRUE(leadersWithin(c, characterTopic, leaders));
    BrokerNode& admin = c.node(awaitController(c));
    ViewHandle accountStats(admin, "account_stats");
    Writer w(clientConfigFor(c, 1), false);
    const auto stats = rowWhenReady(*w.client, accountStats, accountStats.keyOf(kFull));
    ASSERT_TRUE(stats.has_value());
    EXPECT_EQ(accountStats.u(*stats, "characters"), 2u) << "the account still has 2";
    EXPECT_TRUE(isConstraint(w.insertAndAwait(character(kFull, "Baz")), *w.client, "account_stats", "max_characters"));
    EXPECT_TRUE(isConstraint(w.insertAndAwait(character(9, "foo")), *w.client, "character_name", "unique"));
}

// FailoverDuringDrop with the fewest records: the drop finishes on the new leader and the dropped unique view no longer refuses.
TEST(CheckedWritesRegressorsPathRegression, FailoverDuringDropFinishesTheDropMinimalThreeNodesRegression) {
    using namespace rune::node::test::catalog_cluster;
    ThreeNodes c = startCluster("cw09_dropmin");
    ready(c);
    ASSERT_FALSE(HasFatalFailure());
    std::string out, err;
    ASSERT_EQ(qlScript(endpointOf(c, 1), stmt(goldenTopic("character")) + kCharacterName, &out, &err), 0) << err;
    const int32_t characterTopic = topicIdOf(c.node(awaitController(c)), "character");
    ASSERT_GE(characterTopic, 0);
    std::vector<int32_t> leaders;
    ASSERT_TRUE(leadersWithin(c, characterTopic, leaders));
    const int32_t leader = leaders[0];
    const int survivor = leader == 1 ? 2 : 1;
    auto w = std::make_unique<Writer>(clientConfigFor(c, survivor), false);
    ASSERT_TRUE(w->insertAndAwait(character(1, "Foo")).ok());
    std::vector<rc::Token> inFlight;
    for (int i = 0; i < 3; ++i) inFlight.push_back(w->insert(character(2 + static_cast<uint64_t>(i), "f" + std::to_string(i))));
    ASSERT_EQ(ql(endpointOf(c, survivor), "drop view character_name;", &out, &err), 0) << err;
    c.crash(leader);
    c.startNode(leader);
    for (const rc::Token t : inFlight) {
        const rc::WriteResult r = w->await(t);
        EXPECT_NE(r.status(), rc::OpStatus::Timeout);
        EXPECT_NE(r.status(), rc::OpStatus::Closed);
    }
    ready(c);
    ASSERT_FALSE(HasFatalFailure());
    ASSERT_TRUE(leadersWithin(c, characterTopic, leaders));
    EXPECT_TRUE(waitFor([&] {
        for (const int id : c.runningNodes()) {
            const int32_t worker = rune::metadata::ShardRouter(c.node(id).workerCountForTest()).workerForPartition(characterTopic, 0);
            const bool waiting = c.node(id).onWorkerThreadForTest(worker, [characterTopic](Worker& wk) {
                const VerdictSource* j = wk.viewSet()->verdictSource(characterTopic, 0);
                return j != nullptr && j->waitingForInstall();
            });
            if (waiting) return false;
        }
        return true;
    }, 10s)) << "a verdict source still waits for an install";
    w.reset();
    w = std::make_unique<Writer>(clientConfigFor(c, survivor), false);
    const rc::WriteResult dup = w->insertAndAwait(character(9, "foo"));
    EXPECT_TRUE(dup.ok()) << "the dropped unique view no longer refuses a duplicate: code " << dup.code();
}
