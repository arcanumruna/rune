// CheckedWritesTxnBrokerRegressionTest.cpp - checked writes Part 11 Task 14: transactions on constrained sources end to end on real brokers.
// The character topic and its views are the RegressorsPath golden script's; the ledger is {entryId, account, amount, memo} with the view
// `balances` (balance >= 0).  Real clients, transactional producers with acks=All, and the real `rune` CLI for the DDL.
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

#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <fstream>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

using namespace std::chrono_literals;
using namespace rune::node;
using namespace rune::node::test;
namespace rc  = rune::client;
namespace rcv = rune::client::view;
namespace cw  = rune::node::cw;
using rune::codec::PrimType;

namespace {

constexpr uint16_t kConstraintViolation = static_cast<uint16_t>(rune::protocol::client::ErrorCode::ConstraintViolation);
constexpr uint16_t kTxnTooLarge         = static_cast<uint16_t>(rune::protocol::client::ErrorCode::TxnTooLarge);
constexpr uint16_t kInvalidRequest      = static_cast<uint16_t>(rune::protocol::client::ErrorCode::InvalidRequest);
constexpr rc::Acks kAllView             = rc::Acks::All | rc::Acks::View;

template <class Pred>
bool waitFor(Pred&& pred, std::chrono::milliseconds within) {
    for (const auto deadline = std::chrono::steady_clock::now() + within; std::chrono::steady_clock::now() < deadline; std::this_thread::sleep_for(5ms))
        if (pred()) return true;
    return pred();
}

std::string stmt(std::string st) {
    if (st.empty() || st.back() != ';') st += ';';
    return st + "\n";
}

/// The statement of the golden script that holds `marker`.
std::string goldenStatement(const std::string& marker) {
    std::ifstream in(std::string(RUNE_QL_GOLDEN_DIR) + "/regressorspath.rql");
    std::stringstream text;
    text << in.rdbuf();
    for (std::string st : rune::cli::engine::splitStatements(text.str()))
        if (st.find(marker) != std::string::npos) return st;
    throw std::runtime_error("regressorspath.rql: no statement holding '" + marker + "'");
}

/// The character topic with character_name and account_stats, and the ledger with balances; `replicas` keeps the golden `replicas 3`.
std::string schema(bool replicas) {
    std::string character = goldenStatement("topic character {");
    const std::string three = " replicas 3";
    const std::string ledger = "topic ledger { entryId uint64, account uint64, amount int64, memo string(16) } partitions 1" + std::string(replicas ? three : "") +
                               " key entryId with (cleanup.policy = compact)";
    if (!replicas) character.replace(character.find(three), three.size(), "");
    return stmt(character) + stmt(goldenStatement("view character_name key")) + stmt(goldenStatement("view account_stats key")) +
           stmt("create view character_by_id key characterId as select * from character") + stmt(ledger) +
           stmt("create view balances key account as select sum(amount) as balance from ledger constraint non_negative check (balance >= 0)");
}

int qlScript(const std::string& endpoint, const std::string& text, std::string* err) {
    static int seq = 0;
    const std::filesystem::path path = std::filesystem::temp_directory_path() / ("rune_cw11_txn_" + std::to_string(::getpid()) + "_" + std::to_string(seq++) + ".rql");
    std::ofstream(path, std::ios::binary) << text;
    std::stringstream o, e;
    const int code = rune::cli::run({"ql", "-f", path.string(), "--broker", endpoint, "--cli-stream-in", std::to_string(catalog_cluster::kClientStream)}, o, e);
    *err = e.str();
    std::error_code ec;
    std::filesystem::remove(path, ec);
    return code;
}

void awaitLevel(BrokerNode& n) {
    ASSERT_TRUE(waitFor([&] { return n.onWorkerThreadForTest(0, [](Worker& w) { return w.clusterFeatureLevelForTest(); }) >= rune::replication::MetadataFeature::kCheckedWrites; }, 10s))
        << "the cluster feature level reaches kCheckedWrites";
}

int32_t topicIdOf(BrokerNode& n, const std::string& name) {
    return n.onAdminThreadForTest([&n, &name] {
        const auto* t = n.adminWorkerAgentForTest()->worker().applier().state().topicByName(rune::catalog::kDefaultNsId, name);
        return t != nullptr ? t->topicId : -1;
    });
}

// ---- views: a node's catalog gives the topic id, the columns and the key builder ----

struct ViewHandle {
    int32_t                  topic = -1;
    rcv::RowLayout           layout;
    std::vector<std::string> names;
    rcv::ViewKeyBuilder      builder;

    ViewHandle(BrokerNode& n, const std::string& view)
        : builder(n.onAdminThreadForTest([&n, &view] {
              auto b = rcv::ViewKeyBuilder::fromCatalog(n.adminWorkerAgentForTest()->worker().applier().state(), view);
              if (!b) throw std::runtime_error(b.error());
              return std::move(*b);
          })) {
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
    [[nodiscard]] int64_t i(std::span<const uint8_t> row, const std::string& column) const {
        const auto slot = static_cast<uint16_t>(std::find(names.begin(), names.end(), column) - names.begin());
        const rcv::FieldValue f = rcv::readSlot(row, layout, slot);
        return f.type == PrimType::Int64 ? f.i : static_cast<int64_t>(f.u);
    }
    [[nodiscard]] std::vector<uint8_t> keyOf(uint64_t id) {
        builder.clear();
        auto k = builder.setUInt(names[0], id).key();
        if (!k) throw std::runtime_error(k.error());
        return std::move(*k);
    }
    [[nodiscard]] std::vector<uint8_t> keyOf(const std::string& name) {
        builder.clear();
        auto k = builder.setString("name", name).key();
        if (!k) throw std::runtime_error(k.error());
        return std::move(*k);
    }
};

/// The row of `key`: nullopt when the view has none.  A view that is not ready or a leader that moved is retried within `within`.
std::optional<std::vector<uint8_t>> readRow(rc::Client& c, const ViewHandle& v, std::span<const uint8_t> key, std::chrono::milliseconds within = 10s) {
    std::optional<std::vector<uint8_t>> row;
    bool answered = false;
    waitFor([&] {
        try {
            const rc::ViewRowResult r = c.getViewRowAndWait(v.topic, key, {}, 10s);
            if (r.status == rc::ViewRowResult::kOk) { row = r.rowBytes; answered = true; }
            else if (r.status == rc::ViewRowResult::kAbsentKey) answered = true;
        } catch (const std::exception&) {}
        return answered;
    }, within);
    if (!answered) throw std::runtime_error("a view row never answered");
    return row;
}

// ---- writers ----

RpCharacter character(uint64_t user, const std::string& name, uint64_t id = 0) { return RpCharacter{.characterId = id, .userId = user, .name = name, .gender = 1}; }
RpLedger entry(uint64_t id, uint64_t account, int64_t amount) { return RpLedger{.entryId = id, .account = account, .amount = amount, .memo = "t"}; }

/// A transactional producer (acks=All) over the character and ledger topics.
struct TxnClient {
    std::unique_ptr<rc::Client>                                  client;
    std::optional<rc::Producer>                                  prod;
    std::optional<rc::TypedTopic<RpCharacter, rc::ProducerRef>>  characters;
    std::optional<rc::TypedTopic<RpLedger, rc::ProducerRef>>     ledger;

    TxnClient(rc::ClientConfig cfg, const std::string& txnId, int32_t transactionTimeoutMs = 60'000, int64_t opTimeoutMs = 0) {
        client = std::make_unique<rc::Client>(cfg);
        rc::ProducerConfig pc;
        pc.acks                 = rc::Acks::All;
        pc.lingerUs             = 1;
        pc.transactionalId      = txnId;
        pc.transactionTimeoutMs = transactionTimeoutMs;
        if (opTimeoutMs > 0) pc.transactionOpTimeoutMs = opTimeoutMs;
        prod.emplace(client->producer(pc));
        client->connectAndWait(30s);
        prod->initTransactionsAndWait(30s);
        characters.emplace(client->openTopicAndWait<RpCharacter>("character", *prod, 30s));
        ledger.emplace(client->openTopicAndWait<RpLedger>("ledger", *prod, 30s));
    }
    ~TxnClient() {
        if (prod) prod->closeImmediately();
        if (client) client->close();
    }
    void begin() { prod->beginTransaction(); }
    rc::WriteResult create(uint64_t user, const std::string& name) { return characters->insertAndWait(character(user, name), 10s); }
    rc::WriteResult rename(uint64_t id, uint64_t user, const std::string& name) { return characters->updateAndWait(character(user, name, id), 10s); }
    rc::WriteResult put(uint64_t id, uint64_t account, int64_t amount) { return ledger->upsertAndWait(entry(id, account, amount), 10s); }
    rc::TxnCommitResult commit(std::chrono::nanoseconds within = 30s) { return prod->commitTransactionAndWait(within); }
};

/// An All|View producer: plain writes and atomic ledger batches.
struct PlainClient {
    std::unique_ptr<rc::Client>                                  client;
    std::optional<rc::Producer>                                  prod;
    std::optional<rc::TypedTopic<RpCharacter, rc::ProducerRef>>  characters;
    std::optional<rc::TypedTopic<RpLedger, rc::ProducerRef>>     ledger;
    std::optional<rc::Completions>                               cq;
    std::optional<rc::TopicHandle>                               ledgerHandle;

    PlainClient(rc::ClientConfig cfg, int32_t ledgerTopic) {
        cfg.topics.push_back(rc::TopicSpec{"ledger", ledgerTopic, 1});
        client = std::make_unique<rc::Client>(cfg);
        rc::ProducerConfig pc;
        pc.acks     = kAllView;
        pc.lingerUs = 1;
        prod.emplace(client->producer(pc));
        client->connectAndWait(30s);
        characters.emplace(client->openTopicAndWait<RpCharacter>("character", *prod, 30s));
        ledger.emplace(client->openTopicAndWait<RpLedger>("ledger", *prod, 30s));
        cq.emplace(*client);
        ledgerHandle.emplace(client->topic("ledger"));
    }
    ~PlainClient() {
        cq.reset();
        if (prod) prod->closeImmediately();
        if (client) client->close();
    }
    rc::WriteResult create(uint64_t user, const std::string& name) { return characters->insertAndWait(character(user, name), 10s); }
    rc::WriteResult put(uint64_t id, uint64_t account, int64_t amount) { return ledger->upsertAndWait(entry(id, account, amount), 10s); }
    /// The records of `entries` in one atomic batch: its one completion.
    rc::WriteResult batch(const std::vector<RpLedger>& entries) {
        rc::BatchBuilder b = prod->batch(*ledgerHandle, rc::Key(entries.at(0).account));
        for (const RpLedger& e : entries) {
            std::vector<uint8_t> value(e.encodedSize());
            (void)e.encode(value.data());
            char key[8];
            std::memcpy(key, &e.entryId, sizeof key);   // the record's key is its key field
            b.add(value, std::string_view(key, sizeof key));
        }
        if (b.send(*cq, 1) != rc::OpResult::Accepted) throw std::runtime_error("the batch was refused");
        std::optional<rc::WriteResult> r;
        if (cq->waitFor(1, 10s, [&](const rc::Completion& c, std::span<const uint8_t>) { r = rc::WriteResult::fromCompletion(c); }) != rc::Completions::Wait::Done)
            throw std::runtime_error("the batch never completed");
        return *r;
    }
};

bool isConstraint(const rc::WriteResult& r, rc::Client& as, const std::string& view, const std::string& constraint) {
    return r.code() == kConstraintViolation && as.constraintName(r.constraintId()) == std::optional(rc::ConstraintName{view, constraint});
}
bool isConstraint(const rc::TxnCommitResult& r, rc::Client& as, const std::string& view, const std::string& constraint) {
    return !r.ok && r.code == kConstraintViolation && as.constraintName(r.constraintId) == std::optional(rc::ConstraintName{view, constraint});
}

/// A read_committed subscription from 0 on both topics: the character names and the ledger entry ids it delivers.
struct Reader {
    std::unique_ptr<rc::Client> client;
    std::mutex                  mu;
    std::set<std::string>       names;
    std::set<uint64_t>          entries;
    std::vector<rc::Subscription> subs;

    Reader(rc::ClientConfig cfg, int32_t characterTopic, int32_t ledgerTopic) {
        cfg.topics.push_back(rc::TopicSpec{"character", characterTopic, 1});
        cfg.topics.push_back(rc::TopicSpec{"ledger", ledgerTopic, 1});
        client = std::make_unique<rc::Client>(cfg);
        client->connectAndWait(30s);
        rc::SubscribeOptions o;
        o.from      = rc::From::earliest();
        o.isolation = rc::Isolation::ReadCommitted;
        subs.push_back(client->subscribe(client->topic("character"), 0, o, [this](const rc::Record& r) {
            if (r.value.size() < 36) return;
            std::lock_guard<std::mutex> g(mu);
            names.insert(std::string(reinterpret_cast<const char*>(r.value.data()) + 20, strnlen(reinterpret_cast<const char*>(r.value.data()) + 20, 16)));
        }));
        subs.push_back(client->subscribe(client->topic("ledger"), 0, o, [this](const rc::Record& r) {
            if (r.value.size() < 12) return;
            uint64_t id = 0;
            std::memcpy(&id, r.value.data() + 4, sizeof id);
            std::lock_guard<std::mutex> g(mu);
            entries.insert(id);
        }));
    }
    ~Reader() {
        for (auto& s : subs) s.unsubscribe();
        client->close();
    }
    bool hasName(const std::string& n) { std::lock_guard<std::mutex> g(mu); return names.contains(n); }
    bool hasEntry(uint64_t id) { std::lock_guard<std::mutex> g(mu); return entries.contains(id); }
};

void fastBroker(NodeConfig& cfg) { cfg.unservableWarmupMs = 50; cfg.runtimePlacement = NodeConfig::RuntimePlacement::Fused; }

/// One broker with the schema installed.
struct Shard {
    OneBroker b;
    int32_t   characterTopic = -1, ledgerTopic = -1;

    explicit Shard(const char* tag, std::function<void(NodeConfig&)> tweak = fastBroker, std::function<void(BrokerNode&)> beforeStart = {})
        : b(tag, OneBroker::Runtime::Shard, std::move(tweak), std::move(beforeStart)) {}
    void setUp() {
        awaitLevel(b.node());
        ASSERT_FALSE(::testing::Test::HasFatalFailure());
        std::string err;
        ASSERT_EQ(qlScript(b.endpoint(), schema(false), &err), 0) << err;
        characterTopic = topicIdOf(b.node(), "character");
        ledgerTopic    = topicIdOf(b.node(), "ledger");
        ASSERT_GE(characterTopic, 0);
        ASSERT_GE(ledgerTopic, 0);
    }
};

/// Every character created in a transaction of `txns` two-topic transactions (two characters and two ledger entries of 1 for a user, six
/// transactions per user, so the 12th slot fills last) is visible on each partition whole at once: a reader of account_stats and balances
/// sees only even counts, and character_by_id shows both characters of a transaction or neither.
void expectEachPartitionVisibleInOneStep(const char* tag, int txns) {
    Shard s(tag);
    s.setUp();
    ASSERT_FALSE(::testing::Test::HasFatalFailure());
    ViewHandle stats(s.b.node(), "account_stats"), balances(s.b.node(), "balances"), byId(s.b.node(), "character_by_id");
    TxnClient writer(s.b.clientConfig(), "tx-allyes");
    rc::Client& reader = *s.b.connectClient();
    std::atomic<uint64_t> user{1};
    std::atomic<bool> done{false};
    std::atomic<int> reads{0};
    std::mutex mu;
    std::string torn;
    std::array<uint64_t, 2> ids{};   // the characters of the transaction being committed, published before its commit
    std::jthread watcher([&] {
        while (!done) {
            const uint64_t u = user;
            std::array<uint64_t, 2> pair;
            {
                std::lock_guard<std::mutex> g(mu);
                pair = ids;
            }
            try {
                if (pair[0] != 0) {   // a row seen means the other must be seen by a later read
                    for (const bool reversed : {false, true}) {
                        const bool first = readRow(reader, byId, byId.keyOf(pair[reversed ? 1 : 0]), 1s).has_value();
                        const bool second = readRow(reader, byId, byId.keyOf(pair[reversed ? 0 : 1]), 1s).has_value();
                        if (first && !second) {
                            std::lock_guard<std::mutex> g(mu);
                            torn = "character_by_id shows " + std::to_string(pair[reversed ? 1 : 0]) + " without " + std::to_string(pair[reversed ? 0 : 1]);
                        }
                    }
                }
                const auto characters = readRow(reader, stats, stats.keyOf(u), 1s);
                const auto balance    = readRow(reader, balances, balances.keyOf(u), 1s);
                const int64_t n = characters ? stats.i(*characters, "characters") : 0, sum = balance ? balances.i(*balance, "balance") : 0;
                ++reads;
                if (n % 2 != 0 || sum % 2 != 0) {
                    std::lock_guard<std::mutex> g(mu);
                    torn = "user " + std::to_string(u) + ": account_stats counts " + std::to_string(n) + ", balances sums " + std::to_string(sum);
                }
            } catch (const std::exception&) {}
        }
    });
    uint64_t entryId = 0;
    for (int i = 0; i < txns; ++i) {
        const uint64_t u = 1 + static_cast<uint64_t>(i) / 6;
        user = u;
        writer.begin();
        const rc::WriteResult a = writer.create(u, "a" + std::to_string(i)), b = writer.create(u, "b" + std::to_string(i));
        ASSERT_TRUE(a.ok());
        ASSERT_TRUE(b.ok());
        ASSERT_TRUE(writer.put(++entryId, u, 1).ok());
        ASSERT_TRUE(writer.put(++entryId, u, 1).ok());
        {
            std::lock_guard<std::mutex> g(mu);
            ids = {a.key(), b.key()};
        }
        const rc::TxnCommitResult r = writer.commit();
        ASSERT_TRUE(r.ok) << "transaction " << i << " voted " << r.code;
    }
    done = true;
    watcher.join();
    EXPECT_GT(reads.load(), 0);
    EXPECT_EQ(torn, "");
    const uint64_t last = 1 + static_cast<uint64_t>(txns - 1) / 6;
    const int64_t want = 2 * (1 + (txns - 1) % 6);
    const auto characters = readRow(reader, stats, stats.keyOf(last));
    const auto balance    = readRow(reader, balances, balances.keyOf(last));
    ASSERT_TRUE(characters.has_value());
    ASSERT_TRUE(balance.has_value());
    EXPECT_EQ(stats.i(*characters, "characters"), want);
    EXPECT_EQ(balances.i(*balance, "balance"), want);
}

}  // namespace

TEST(CheckedWritesTxnBrokerRegression, TwoPartitionsOneNoNothingCommitsShardRegression) {
    Shard s("cw11_no");
    s.setUp();
    ASSERT_FALSE(HasFatalFailure());
    ViewHandle names(s.b.node(), "character_name"), balances(s.b.node(), "balances");
    PlainClient plain(s.b.clientConfig(), s.ledgerTopic);
    ASSERT_TRUE(plain.put(1, 1, 10).ok());
    Reader reader(s.b.clientConfig(), s.characterTopic, s.ledgerTopic);
    TxnClient t(s.b.clientConfig(), "tx-no");

    t.begin();
    ASSERT_TRUE(t.create(7, "Zed").ok());
    ASSERT_TRUE(t.put(2, 1, -50).ok());
    const rc::TxnCommitResult no = t.commit();
    EXPECT_TRUE(isConstraint(no, *t.client, "balances", "non_negative")) << "ok " << no.ok << " code " << no.code;

    t.begin();
    ASSERT_TRUE(t.create(7, "Yes").ok());
    ASSERT_TRUE(t.put(3, 1, 5).ok());
    const rc::TxnCommitResult yes = t.commit();
    EXPECT_TRUE(yes.ok) << "the next transaction voted " << yes.code;

    // The later transaction is delivered on both topics: whatever the first left would be delivered by now.
    ASSERT_TRUE(waitFor([&] { return reader.hasName("Yes") && reader.hasEntry(3); }, 10s));
    EXPECT_FALSE(reader.hasName("Zed"));
    EXPECT_FALSE(reader.hasEntry(2));
    EXPECT_FALSE(readRow(*plain.client, names, names.keyOf("zed")).has_value());
    EXPECT_TRUE(readRow(*plain.client, names, names.keyOf("yes")).has_value());
    const auto balance = readRow(*plain.client, balances, balances.keyOf(1));
    ASSERT_TRUE(balance.has_value());
    EXPECT_EQ(balances.i(*balance, "balance"), 15);
}

// The e2e verifier's shape: a client with no static topics reads both topics read_committed from earliest, by the handles it opened, and
// sees a committed transaction whole by its characters' assigned ids and its entry ids.  A name it never opened has no handle to read.
TEST(CheckedWritesTxnBrokerRegression, VerifierReadsBothTopicsByOpenedHandlesShardRegression) {
    Shard s("cw11_verify");
    s.setUp();
    ASSERT_FALSE(HasFatalFailure());
    TxnClient t(s.b.clientConfig(), "tx-verify");
    t.begin();
    const rc::WriteResult a = t.create(9, "Vera"), b = t.create(9, "Vic");
    ASSERT_TRUE(a.ok());
    ASSERT_TRUE(b.ok());
    ASSERT_TRUE(t.put(41, 9, 1).ok());
    ASSERT_TRUE(t.put(42, 9, 1).ok());
    ASSERT_TRUE(t.commit().ok);

    rc::Client& c = *s.b.connectClient();
    rc::Producer p = c.producer(rc::ProducerConfig{});
    const auto characters = c.openTopicAndWait<RpCharacter>("character", p, 10s);
    const auto ledger = c.openTopicAndWait<RpLedger>("ledger", p, 10s);
    rc::SubscribeOptions so;
    so.from      = rc::From::earliest();
    so.isolation = rc::Isolation::ReadCommitted;
    std::mutex mu;
    std::set<uint64_t> ids, entries;
    const auto idAt4 = [&](std::set<uint64_t>& into) {
        return [&](const rc::Record& r) {
            uint64_t id = 0;
            std::memcpy(&id, r.value.data() + 4, sizeof id);
            std::lock_guard<std::mutex> g(mu);
            into.insert(id);
        };
    };
    EXPECT_THROW((void)c.subscribe(c.topic("ledger"), rc::AllPartitions{}, so, idAt4(entries)), std::invalid_argument) << "a name absent from ClientConfig::topics";
    rc::SubscriptionSet cs = c.subscribe(characters.handle(), rc::AllPartitions{}, so, idAt4(ids));
    rc::SubscriptionSet ls = c.subscribe(ledger.handle(), rc::AllPartitions{}, so, idAt4(entries));
    EXPECT_TRUE(waitFor([&] {
        std::lock_guard<std::mutex> g(mu);
        return ids.contains(a.key()) && ids.contains(b.key()) && entries.contains(41) && entries.contains(42);
    }, 5s)) << "the committed transaction is whole";
    p.closeImmediately();
}

TEST(CheckedWritesTxnBrokerRegression, AllYesEachPartitionVisibleInOneStepShardRegression) {
    expectEachPartitionVisibleInOneStep("cw11_step", 60);
}

TEST(CheckedWritesTxnBrokerRegression, AllYesEachPartitionVisibleInOneStepFullLengthShardRegression) {
    expectEachPartitionVisibleInOneStep("cw11_step_full", 1000);
}

TEST(CheckedWritesTxnBrokerRegression, RacingTransactionsForTheLastSlotShardRegression) {
    Shard s("cw11_slot");
    s.setUp();
    ASSERT_FALSE(HasFatalFailure());
    constexpr uint64_t user = 9;
    PlainClient plain(s.b.clientConfig(), s.ledgerTopic);
    // The eight producers start up and write while the eleven plain creates fill the slots; they commit once the slots are filled.
    std::vector<std::unique_ptr<TxnClient>> clients(8);
    std::vector<rc::TxnCommitResult> results(clients.size());
    std::vector<char> isFull(clients.size(), 0);
    std::atomic<bool> filled{false};
    std::atomic<int> refused{0};
    std::vector<rc::ClientConfig> configs;   // a config takes the next data dir of the broker rig: made here, not in the threads
    for (std::size_t i = 0; i < clients.size(); ++i) configs.push_back(s.b.clientConfig());
    {
        std::vector<std::jthread> threads;
        for (std::size_t i = 0; i < clients.size(); ++i)
            threads.emplace_back([&, i] {
                clients[i] = std::make_unique<TxnClient>(configs[i], "tx-slot-" + std::to_string(i));
                clients[i]->begin();
                if (!clients[i]->create(user, "r" + std::to_string(i)).ok()) ++refused;
                while (!filled) std::this_thread::sleep_for(1ms);
                results[i] = clients[i]->commit();
                isFull[i]  = !results[i].ok && isConstraint(results[i], *clients[i]->client, "account_stats", "max_characters");   // a Client is read on its own thread
                clients[i].reset();   // and closed on it: the eight closes overlap
            });
        for (int i = 0; i < 11; ++i)
            if (!plain.create(user, "p" + std::to_string(i)).ok()) { ADD_FAILURE() << "plain create " << i << " was refused"; break; }
        filled = true;
    }
    ASSERT_EQ(refused.load(), 0) << "a transaction write was refused";
    int ok = 0, full = 0;
    for (std::size_t i = 0; i < results.size(); ++i) {
        if (results[i].ok) ++ok;
        else if (isFull[i]) ++full;
        else ADD_FAILURE() << "transaction " << i << " voted " << results[i].code;
    }
    EXPECT_EQ(ok, 1);
    EXPECT_EQ(full, 7);
}

TEST(CheckedWritesTxnBrokerRegression, TxnTooLargeShardRegression) {
    Shard s("cw11_big", [](NodeConfig& cfg) { fastBroker(cfg); cfg.txnJudgedRecordsMax = 16; });
    s.setUp();
    ASSERT_FALSE(HasFatalFailure());
    TxnClient t(s.b.clientConfig(), "tx-big");
    t.begin();
    uint64_t highest = 0;
    for (int i = 0; i < 16; ++i) {
        const rc::WriteResult w = t.create(3, "big" + std::to_string(i));
        ASSERT_TRUE(w.ok()) << "write " << i << " answered " << w.code();
        highest = std::max(highest, w.key());
    }
    const rc::WriteResult over = t.create(3, "big16");
    EXPECT_EQ(over.code(), kTxnTooLarge) << "the 17th record is refused when it is written";
    const rc::TxnCommitResult r = t.commit();
    EXPECT_FALSE(r.ok);
    EXPECT_EQ(r.code, kTxnTooLarge);
    EXPECT_NO_THROW(t.prod->abortTransactionAndWait(30s));
    PlainClient plain(s.b.clientConfig(), s.ledgerTopic);
    const rc::WriteResult after = plain.create(3, "after");
    ASSERT_TRUE(after.ok()) << after.code();
    EXPECT_GT(after.key(), highest) << "the refused transaction's ids are burned";
}

TEST(CheckedWritesTxnBrokerRegression, AtomicBatchOnAConstrainedSourceShardRegression) {
    Shard s("cw11_batch");
    s.setUp();
    ASSERT_FALSE(HasFatalFailure());
    ViewHandle balances(s.b.node(), "balances");
    PlainClient plain(s.b.clientConfig(), s.ledgerTopic);
    constexpr uint64_t account = 4;
    ASSERT_TRUE(plain.put(1, account, 15).ok());
    Reader reader(s.b.clientConfig(), s.characterTopic, s.ledgerTopic);

    const rc::WriteResult refused = plain.batch({entry(2, account, -10), entry(3, account, -10)});
    EXPECT_TRUE(isConstraint(refused, *plain.client, "balances", "non_negative")) << "code " << refused.code();
    int64_t first = -1;
    s.b.node().onOwnerThreadForTest(s.ledgerTopic, 0, [&](rune::replication::PartitionReplica& r) {
        for (const auto& rec : cw::readLogOf(*r.log(), r.highWatermark()).data)
            if (rec.id == 2) first = rec.offset;
        return 0;
    });
    ASSERT_GE(first, 0);
    EXPECT_EQ(refused.offset(), first) << "the answer is the batch's first record: a rejected batch has one trailer entry at its base";
    auto balance = readRow(*plain.client, balances, balances.keyOf(account));
    ASSERT_TRUE(balance.has_value());
    EXPECT_EQ(balances.i(*balance, "balance"), 15);

    const rc::WriteResult accepted = plain.batch({entry(4, account, -10)});
    EXPECT_TRUE(accepted.ok()) << "code " << accepted.code();
    balance = readRow(*plain.client, balances, balances.keyOf(account));
    ASSERT_TRUE(balance.has_value());
    EXPECT_EQ(balances.i(*balance, "balance"), 5);
    ASSERT_TRUE(waitFor([&] { return reader.hasEntry(4); }, 10s));
    EXPECT_FALSE(reader.hasEntry(2));
    EXPECT_FALSE(reader.hasEntry(3));
}

/// The coordinator's votes, seen on its control thread: a vote of (topic, producer) in `held` is not sent until it is released.
struct VoteGate {
    std::mutex                                   mu;
    std::set<std::pair<std::string, int64_t>>    held, seen;   // seen: the votes that were let through
    bool holds(std::string_view topic, int64_t pid) {
        std::lock_guard<std::mutex> g(mu);
        const std::pair<std::string, int64_t> key{std::string(topic), pid};
        if (held.contains(key)) return true;
        seen.insert(key);
        return false;
    }
    bool saw(const std::string& topic, int64_t pid) { std::lock_guard<std::mutex> g(mu); return seen.contains({topic, pid}); }
    void release() { std::lock_guard<std::mutex> g(mu); held.clear(); }
    void release(const std::string& topic, int64_t pid) { std::lock_guard<std::mutex> g(mu); held.erase({topic, pid}); }
};

/// T1 and T2 rewrite the same character and the same ledger entry and commit at once, with the votes held so that T1's reaches `character` first
/// and T2's `ledger` first: each partition judges one and blocks the other.  T1 has the transaction timeout `timeoutMs` and aborts by it within
/// one pass; T2's timeout is kSurvivorExtra longer, so it outlives T1's locks on any host (#1006: with equal timeouts a slow runner let both
/// expire) and commits with its rows whole.  No commit hangs.
void expectOppositeOrderResolvesByTimeout(const char* tag, int32_t timeoutMs) {
    auto gate = std::make_shared<VoteGate>();
    Shard s(tag, fastBroker, [gate](BrokerNode& n) {
        n.setTxnVoteHoldForTest([gate](std::string_view topic, int32_t, int64_t pid, int16_t) { return gate->holds(topic, pid); });
    });
    s.setUp();
    ASSERT_FALSE(::testing::Test::HasFatalFailure());
    ViewHandle names(s.b.node(), "character_name"), balances(s.b.node(), "balances");
    PlainClient plain(s.b.clientConfig(), s.ledgerTopic);
    const rc::WriteResult c = plain.create(5, "base");
    ASSERT_TRUE(c.ok());
    ASSERT_TRUE(plain.put(1, 5, 5).ok());
    std::array<std::unique_ptr<TxnClient>, 2> t;
    constexpr int32_t kSurvivorExtra = 10'000;
    for (std::size_t i = 0; i < t.size(); ++i)   // producerId() is read on this thread
        t[i] = std::make_unique<TxnClient>(s.b.clientConfig(), "tx-order-" + std::to_string(i), i == 0 ? timeoutMs : timeoutMs + kSurvivorExtra);
    const int64_t pid[2] = {t[0]->prod->producerId(), t[1]->prod->producerId()};
    ASSERT_GE(pid[0], 0);
    ASSERT_GE(pid[1], 0);
    {
        std::lock_guard<std::mutex> g(gate->mu);
        gate->held = {{"ledger", pid[0]}, {"character", pid[1]}};
    }
    for (std::size_t i = 0; i < t.size(); ++i) {
        t[i]->begin();
        ASSERT_TRUE(t[i]->rename(c.key(), 5, "n" + std::to_string(i + 1)).ok());
        ASSERT_TRUE(t[i]->put(1, 5, 6 + static_cast<int64_t>(i)).ok());
    }
    std::array<std::optional<rc::TxnCommitResult>, 2> r;
    const auto t0 = std::chrono::steady_clock::now();
    std::chrono::steady_clock::time_point done[2];
    {
        std::vector<std::jthread> threads;
        for (std::size_t i = 0; i < t.size(); ++i)
            threads.emplace_back([&, i] {
                try { r[i] = t[i]->commit(15s); } catch (const std::exception&) {}
                done[i] = std::chrono::steady_clock::now();
            });
        // T1's vote has reached character and T2's ledger: each partition judges its first; then the held votes go out and meet the locks.
        ASSERT_TRUE(waitFor([&] { return gate->saw("character", pid[0]) && gate->saw("ledger", pid[1]); }, 5s));
        std::this_thread::sleep_for(20ms);
        gate->release();
    }
    const auto limit = std::chrono::milliseconds(timeoutMs) + 1500ms;   // the timeout and one pass of the coordinator and the vote retry
    for (std::size_t i = 0; i < t.size(); ++i) EXPECT_LT(done[i] - t0, limit) << "commit " << i << " waited past the transaction timeout and a pass";
    const bool first = r[0] && r[0]->ok, second = r[1] && r[1]->ok;
    EXPECT_FALSE(first) << "T1 (the short timeout) aborts by its timeout";
    EXPECT_TRUE(second) << "T2 outlives T1's locks and commits";
    const auto balance = readRow(*plain.client, balances, balances.keyOf(5));
    ASSERT_TRUE(balance.has_value());
    const bool one = readRow(*plain.client, names, names.keyOf("n1")).has_value(), two = readRow(*plain.client, names, names.keyOf("n2")).has_value();
    EXPECT_NE(one, two) << "exactly one of the two names is the character's";
    EXPECT_EQ(balances.i(*balance, "balance"), one ? 6 : 7) << "the rows are one transaction's";
    EXPECT_EQ(one ? first : second, true) << "the visible rows are a committed transaction's";
}

TEST(CheckedWritesTxnBrokerRegression, OppositeOrderLocksResolveByTimeoutShardRegression) { expectOppositeOrderResolvesByTimeout("cw11_order", 200); }

TEST(CheckedWritesTxnBrokerRegression, OppositeOrderLocksResolveByTimeoutFullLengthShardRegression) { expectOppositeOrderResolvesByTimeout("cw11_orderfl", 2'000); }

/// Eight two-topic transactions on unrelated users and accounts, with every character vote held until every ledger vote was judged yes:
/// each yes holds a transaction overlay until its marker, and the markers wait for the character votes.  No row is shared, so no
/// judgement may wait on another transaction: every commit answers OK within 1 s of the release (the pool grows instead of making a judge wait).
TEST(CheckedWritesTxnBrokerRegression, UnrelatedTransactionsNeverWaitForEachOtherShardRegression) {
    auto gate = std::make_shared<VoteGate>();
    Shard s("cw11_unrelated", fastBroker, [gate](BrokerNode& n) {
        n.setTxnVoteHoldForTest([gate](std::string_view topic, int32_t, int64_t pid, int16_t) { return gate->holds(topic, pid); });
    });
    s.setUp();
    ASSERT_FALSE(HasFatalFailure());
    constexpr std::size_t n = 8;   // the worker's base overlays (kTxnOverlaysPerWorker): all held by ledger yeses
    std::vector<rc::ClientConfig> configs;
    for (std::size_t i = 0; i < n; ++i) configs.push_back(s.b.clientConfig());
    std::vector<std::unique_ptr<TxnClient>> t(n);
    std::vector<int64_t> pid(n);
    {
        std::vector<std::jthread> threads;
        for (std::size_t i = 0; i < n; ++i)
            threads.emplace_back([&, i] {
                t[i] = std::make_unique<TxnClient>(configs[i], "tx-unrelated-" + std::to_string(i), 3'000);
                pid[i] = t[i]->prod->producerId();
                t[i]->begin();
                EXPECT_TRUE(t[i]->create(300 + i, "u" + std::to_string(i)).ok());
                EXPECT_TRUE(t[i]->put(10 + i, 700 + i, 1).ok());
            });
    }
    {   // no vote is sent before the commit
        std::lock_guard<std::mutex> g(gate->mu);
        for (std::size_t i = 0; i < n; ++i) gate->held.insert({"character", pid[i]});
    }
    const auto ledgerYes = [&] {
        return s.b.node().onWorkerThreadForTest(0, [&](Worker& w) {
            const TxnJudgeStats* st = w.txnStatsForTest(s.ledgerTopic, 0);
            return st != nullptr ? st->votesYes : 0;
        });
    };
    std::vector<std::optional<rc::TxnCommitResult>> r(n);
    std::vector<std::string> error(n);
    std::vector<std::chrono::steady_clock::time_point> done(n);
    std::chrono::steady_clock::time_point released;
    {
        std::vector<std::jthread> threads;
        for (std::size_t i = 0; i < n; ++i)
            threads.emplace_back([&, i] {
                try { r[i] = t[i]->commit(15s); } catch (const std::exception& e) { error[i] = e.what(); }
                done[i] = std::chrono::steady_clock::now();
            });
        ASSERT_TRUE(waitFor([&] { return ledgerYes() >= n; }, 5s)) << "every ledger vote is judged yes and holds its overlay";
        released = std::chrono::steady_clock::now();
        for (std::size_t i = 0; i < n; ++i) gate->release("character", pid[i]);
    }
    for (std::size_t i = 0; i < n; ++i) {
        EXPECT_TRUE(r[i] && r[i]->ok) << "commit " << i << " answered " << (r[i] ? r[i]->code : -1) << " " << error[i];
        EXPECT_LT(done[i] - released, 1s) << "commit " << i << " waited " << std::chrono::duration_cast<std::chrono::milliseconds>(done[i] - released).count() << " ms";
    }
}

/// The e2e leg 9b in small (checked_writes_probe txn-run): `workers` workers of `perWorker` producers race `users` users for the same two character names per
/// transaction, each crediting its own ledger account (two transactions racing on both topics resolve only by the transaction timeout, R11d).
/// Per user six commit (the account_stats cap is 12 characters), every other answer is 55, an overdraw of the worker's account is refused by
/// balances; none fails, none waits for the transaction timeout.  Differences from the probe: the ids are the Shard's, and an `od` character is named per
/// worker (the probe's is shared).
void expectRacingTwoTopicTransactions(const char* tag, int workers, int perWorker, int users) {
    Shard s(tag);
    s.setUp();
    ASSERT_FALSE(::testing::Test::HasFatalFailure());
    constexpr int rounds = 7;
    std::vector<rc::ClientConfig> configs;   // a config takes the next data dir of the broker rig: made here, not in the threads
    for (int i = 0; i < workers * perWorker; ++i) configs.push_back(s.b.clientConfig());
    std::vector<std::atomic<int>> ok(static_cast<std::size_t>(users));
    std::atomic<int> refused{0}, overdrawn{0}, other{0}, slow{0};
    std::mutex mu;
    std::string failure;
    {
        std::vector<std::jthread> threads;
        for (int w = 0; w < workers * perWorker; ++w)
            threads.emplace_back([&, w] {
                try {
                    TxnClient t(configs[w], "tx-race-" + std::to_string(w), 5'000);
                    const int worker = w / perWorker;
                    for (int u = w % perWorker; u < users; u += perWorker) {
                        const uint64_t user = 100 + static_cast<uint64_t>(u), account = user * 1000 + static_cast<uint64_t>(worker);
                        for (int k = 0; k <= rounds; ++k) {   // k0-k6 fit account_stats in all; the od debits the account's whole balance and more
                            const bool od = k == rounds;
                            const std::string tag = "u" + std::to_string(u) + (od ? "od" : "k" + std::to_string(k));
                            const uint64_t entryId = 1 + 2 * (static_cast<uint64_t>(u * (rounds + 1) + k) * workers * perWorker + static_cast<uint64_t>(w));
                            t.begin();
                            std::vector<rc::WriteResult> writes;
                            if (od) {
                                writes = {t.create(user + 1000, tag + "w" + std::to_string(worker)), t.put(entryId, account, -1000)};
                            } else {
                                writes = {t.create(user, tag + "a"), t.create(user, tag + "b"), t.put(entryId, account, 1), t.put(entryId + 1, account, 1)};
                            }
                            const auto bad = std::find_if(writes.begin(), writes.end(), [](const rc::WriteResult& r) { return !r.ok(); });
                            if (bad != writes.end()) {   // the name race lost at the write: a refusal, the transaction is abandoned
                                t.prod->abortTransactionAndWait(30s);
                                ++(bad->code() == kConstraintViolation ? refused : other);
                                continue;
                            }
                            const auto t0 = std::chrono::steady_clock::now();
                            const rc::TxnCommitResult r = t.commit();
                            if (std::chrono::steady_clock::now() - t0 > 1s) ++slow;
                            if (od) ++(isConstraint(r, *t.client, "balances", "non_negative") ? overdrawn : other);
                            else ++(r.ok ? ok[static_cast<std::size_t>(u)] : r.code == kConstraintViolation ? refused : other);
                        }
                    }
                } catch (const std::exception& e) {
                    std::lock_guard<std::mutex> g(mu);
                    failure = e.what();
                }
            });
    }
    EXPECT_EQ(failure, "");
    EXPECT_EQ(other.load(), 0) << "an answer other than ok, 55 or the overdraw's (a 19 among them)";
    EXPECT_EQ(slow.load(), 0) << "a commit took over 1 s";
    EXPECT_EQ(overdrawn.load(), users * workers) << "every overdraw is refused by balances";
    for (int u = 0; u < users; ++u) EXPECT_EQ(ok[static_cast<std::size_t>(u)].load(), 6) << "user " << u;
}

TEST(CheckedWritesTxnBrokerRegression, RacingTwoTopicTransactionsShardRegression) { expectRacingTwoTopicTransactions("cw11_race", 10, 1, 1); }

TEST(CheckedWritesTxnBrokerRegression, RacingTwoTopicTransactionsFullLengthShardRegression) { expectRacingTwoTopicTransactions("cw11_racefl", 4, 4, 16); }

TEST(CheckedWritesTxnBrokerRegression, OldClientTransactionOnAConstrainedSourceShardRegression) {
    Shard s("cw11_old");
    s.setUp();
    ASSERT_FALSE(HasFatalFailure());
    rc::ClientConfig cfg = s.b.clientConfig();
    cfg.testAnnounceClientVersion = 16;   // a session that does not know the vote answers
    TxnClient t(cfg, "tx-old");
    t.begin();   // a v16 session gets no id back for a key-0 create: the transaction is ledger entries
    ASSERT_TRUE(t.put(1, 1, -5).ok());
    std::optional<rc::TxnCommitResult> r;
    std::string thrown;
    try { r = t.commit(); } catch (const std::exception& e) { thrown = e.what(); }
    if (r) {
        EXPECT_FALSE(r->ok);
        EXPECT_EQ(r->code, kInvalidRequest);
    } else {
        EXPECT_NE(thrown.find("InvalidRequest"), std::string::npos) << thrown;
    }
    EXPECT_NO_THROW(t.prod->abortTransactionAndWait(30s));
    t.begin();
    ASSERT_TRUE(t.put(2, 1, 5).ok());
    const rc::TxnCommitResult next = t.commit();
    EXPECT_TRUE(next.ok) << "the next transaction voted " << next.code;
}

// ---- three nodes: a crash while a transaction is voting ----

/// Three brokers with the schema (3 replicas), short elections and a client config that notices a dead node fast.
struct Cluster {
    std::shared_ptr<VoteGate>   votes = std::make_shared<VoteGate>();   // sees every vote a coordinator sends, holds none
    catalog_cluster::ThreeNodes c;
    int32_t                     characterTopic = -1, ledgerTopic = -1, ctl = -1;

    explicit Cluster(const char* tag)
        : c(catalog_cluster::ThreeNodes::startWith(
              tag,
              [](NodeConfig& cfg) {
                  cfg.unservableWarmupMs      = 50;
                  cfg.runtimePlacement        = NodeConfig::RuntimePlacement::Fused;
                  cfg.electionTimeoutMs       = 40;
                  cfg.readLeaseMaxMs          = 50;
                  cfg.brokerLivenessTimeoutMs = 150;
                  cfg.verdictIntervalMs       = 20;
              },
              [gate = votes](int, BrokerNode& n) {
                  n.setTxnVoteHoldForTest([gate](std::string_view topic, int32_t, int64_t pid, int16_t) { return gate->holds(topic, pid); });
              })) {}
    void setUp() {
        ASSERT_TRUE(waitFor([&] { return c.awaitCatalogLeader(1ms); }, 10s));
        for (const int id : c.runningNodes()) awaitLevel(c.node(id));
        ASSERT_FALSE(::testing::Test::HasFatalFailure());
        ctl = awaitController(c);
        ASSERT_GT(ctl, 0);
        std::string err;
        ASSERT_EQ(qlScript("127.0.0.1:" + std::to_string(c.ports.client[static_cast<std::size_t>(ctl - 1)]), schema(true), &err), 0) << err;
        characterTopic = topicIdOf(c.node(ctl), "character");
        ledgerTopic    = topicIdOf(c.node(ctl), "ledger");
        ASSERT_GE(characterTopic, 0);
        ASSERT_GE(ledgerTopic, 0);
        std::vector<int32_t> leaders;
        ASSERT_TRUE(catalog_cluster::awaitLeaders(c, characterTopic, 1, leaders, 30s));
        ASSERT_TRUE(catalog_cluster::awaitLeaders(c, ledgerTopic, 1, leaders, 30s));
    }
    rc::ClientConfig config(int seed) {
        rc::ClientConfig cfg = catalog_cluster::clientConfigFor(c, seed);
        cfg.responseTimeoutNs   = std::chrono::nanoseconds(500ms).count();
        cfg.keepaliveIntervalNs = std::chrono::nanoseconds(100ms).count();
        return cfg;
    }
    /// Makes the controller resign until it is not `node`; ctl is the new one.
    void moveControllerOff(int node) {
        for (int tries = 0; ctl == node && tries < 10; ++tries) {
            const int old = ctl;
            const auto moved = [&] {
                for (BrokerNode* n : c.live())
                    if (n->isLeader() && n->nodeIdForTest() != old) { ctl = n->nodeIdForTest(); return true; }
                return false;
            };
            // A resign is refused until a follower caught up (under load, for a while): retry it until it is taken or the lead moved.
            if (!waitFor([&] { return moved() || c.node(old).resignConsensusForTest(); }, 5s)) continue;
            waitFor(moved, 5s);
        }
    }
    int32_t leaderOf(int32_t topic) {
        std::vector<int32_t> leaders;
        return catalog_cluster::awaitLeaders(c, topic, 1, leaders, 30s) ? leaders[0] : -1;
    }
};

/// `victim` is crashed once the transaction's vote has gone out to `character` and the one to `ledger` is held, so the commit cannot have
/// answered; the held vote is released after the crash.  A transaction of one character and one ledger entry
/// (ids `entryId`) commits through a client seeded on `seed`.  The commit still answers yes within the transaction timeout and a pass, then a
/// transaction of a fresh producer commits on the survivors.  The crashed one's rows are both visible or neither, to a read_committed reader.
void expectCrashMidVoteLeavesAllOrNothing(Cluster& k, int victim, int seed, const char* name, uint64_t entryId) {
    constexpr int32_t timeoutMs = 5'000;
    TxnClient t(k.config(seed), std::string("tx-") + name, timeoutMs);
    const int64_t pid = t.prod->producerId();
    ASSERT_GE(pid, 0);
    {
        std::lock_guard<std::mutex> g(k.votes->mu);
        k.votes->held = {{"ledger", pid}};
    }
    t.begin();
    ASSERT_TRUE(t.create(1, name).ok());
    ASSERT_TRUE(t.put(entryId, 1, 5).ok());
    std::optional<rc::TxnCommitResult> r;
    std::string thrown;
    std::atomic<bool> answered{false};
    const auto t0 = std::chrono::steady_clock::now();
    std::chrono::steady_clock::time_point done;
    std::chrono::steady_clock::time_point crashedAt;
    std::thread committer([&] {
        try { r = t.commit(30s); } catch (const std::exception& e) { thrown = e.what(); }
        done = std::chrono::steady_clock::now();
        answered = true;
    });
    const bool voted = waitFor([&] { return k.votes->saw("character", pid); }, 10s);
    if (voted) {
        EXPECT_FALSE(answered) << "the commit answered before the crash";
        k.c.crash(victim);
        crashedAt = std::chrono::steady_clock::now();
    }
    k.votes->release();
    committer.join();
    ASSERT_TRUE(voted) << "the vote never went out";
    ASSERT_TRUE(r.has_value()) << "the commit threw: " << thrown;
    EXPECT_TRUE(r->ok) << "the commit answered " << r->code;
    EXPECT_LT(done - t0, std::chrono::milliseconds(timeoutMs) + 1500ms) << "the commit waited past the transaction timeout and a pass";
    if (victim == k.ctl) EXPECT_LT(done - crashedAt, std::chrono::milliseconds(timeoutMs / 2)) << "rediscovered at the stale edge, not after the 5 s grace";

    const int survivor = k.c.runningNodes().front();
    ASSERT_NE(survivor, victim);
    ASSERT_GE(k.leaderOf(k.characterTopic), 0);
    ASSERT_GE(k.leaderOf(k.ledgerTopic), 0);
    TxnClient next(k.config(survivor), std::string("tx-after-") + name, 5'000);
    next.begin();
    ASSERT_TRUE(next.create(2, "After").ok());
    ASSERT_TRUE(next.put(entryId + 1, 2, 5).ok());
    const rc::TxnCommitResult n = next.commit(30s);
    EXPECT_TRUE(n.ok) << "the transaction after the crash voted " << n.code;

    Reader reader(k.config(survivor), k.characterTopic, k.ledgerTopic);
    ASSERT_TRUE(waitFor([&] { return reader.hasName("After") && reader.hasEntry(entryId + 1); }, 10s)) << "the later transaction is visible";
    EXPECT_EQ(reader.hasName(name), reader.hasEntry(entryId)) << "the crashed transaction is whole or absent";
    if (r && r->ok) EXPECT_TRUE(reader.hasName(name)) << "an acknowledged commit is visible";
}

TEST(CheckedWritesTxnBrokerRegression, VoterLeaderCrashMidVoteThreeNodesRegression) {
    Cluster k("cw11_voter");
    k.setUp();
    ASSERT_FALSE(HasFatalFailure());
    const int victim = k.leaderOf(k.characterTopic);
    k.moveControllerOff(victim);
    ASSERT_EQ(k.leaderOf(k.characterTopic), victim) << "character moved with the controller";
    ASSERT_TRUE(victim > 0 && victim != k.ctl) << "character is led by " << victim << ", the controller is " << k.ctl;
    expectCrashMidVoteLeavesAllOrNothing(k, victim, k.ctl, "Voter", 1);
}

TEST(CheckedWritesTxnBrokerRegression, CoordinatorCrashMidVoteThreeNodesRegression) {
    Cluster k("cw11_coord");
    k.setUp();
    ASSERT_FALSE(HasFatalFailure());
    expectCrashMidVoteLeavesAllOrNothing(k, k.ctl, k.ctl % 3 + 1, "Coord", 1);
}
