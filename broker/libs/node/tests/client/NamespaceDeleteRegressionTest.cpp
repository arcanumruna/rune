// NamespaceDeleteRegressionTest.cpp - namespaces Phase 2 Task 7 (D17, D18): a REAL client against a REAL authenticating
// BrokerNode (default config).  A namespace is deleted; the reaper (the catalog leader's admin thread) drives the delete
// to the end; the name is free at once and nothing of the old namespace is seen by its re-creation.
//
// Interaction matrix (event x state x timing):
//   | event                          | namespace state            | timing                         | expected                            | test
//   |--------------------------------|----------------------------|--------------------------------|-------------------------------------|-----
//   | delete, then re-create         | a with a topic, group      | sequential, reaper to the end  | the old id gone, nothing old seen   | RecreateSeesNothingOld
//   |                                | offsets, a txn, a user     |                                |                                     |
//   | login of a deleted ns's user   | DELETING, then erased      | during, after                  | AuthenticationFailed (44)           | LoginDuringDeletingAuthFailed
//   | ns-<id>.deleting dir, erased id| erased                     | restart                        | removed                             | DeletingDirRemovedAtBoot
//   | ns-<id> dir, erased id         | erased (crash before rename)| restart                       | removed                             | DeletingDirRemovedAtBoot
//   | ns-<id>.deleting, live id      | live                       | restart                        | refuses to start                    | LiveNsDeletingDirIsFatal
//   | the four dir states, the pure  | erased / DELETING / live /  | -                              | removed / kept / fatal / untouched  | ReconcileNsDirsByState
//   | reconcile                      | never issued               |                                |                                     |
#include "support/AuthRig.h"

#include <rune/client/Client.h>
#include <rune/client/Completions.h>
#include <rune/client/group/GroupConsumer.h>
#include <rune/client/producer/Producer.h>
#include <rune/client/subscriber/Subscription.h>
#include <rune/node/NamespacePaths.h>
#include <rune/node/TopicDirReconcile.h>

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

using namespace rune::node;
using namespace rune::node::test;
using namespace rune::node::test::grants;
namespace pc = rune::protocol::client;
namespace rc = rune::client;
namespace fs = std::filesystem;
using namespace std::chrono_literals;

namespace {

constexpr const char* kAdminPw = "namespace-admin-password-1";
constexpr const char* kUserPw = "namespace-user-password-12";

std::unique_ptr<rc::Client> newClient(AuthBroker& b, const std::string& user, std::vector<rc::TopicSpec> topics = {}) {
    rc::ClientConfig c;
    c.gatewayChannel  = b.channel();
    c.gatewayStreamId = 1;
    c.egressChannel   = "aeron:udp?endpoint=127.0.0.1:" + std::to_string(rune::aeronutil::testing::allocFreeUdpPort());
    c.egressStreamId  = 2;
    c.dataDir         = b.clientDir().string();
    c.username        = user;
    c.password        = user == "admin" ? kAdminPw : kUserPw;
    c.offerCrypto     = true;
    c.tlsTrustPem     = AuthBroker::tlsCaPem();
    c.topics          = std::move(topics);
    return std::make_unique<rc::Client>(c);
}
std::unique_ptr<rc::Client> connected(AuthBroker& b, const std::string& user, std::vector<rc::TopicSpec> topics = {}) {
    auto c = newClient(b, user, std::move(topics));
    c->connectAndWait(20s);
    return c;
}
std::string connectError(rc::Client& c) {
    try { c.connectAndWait(20s); } catch (const std::runtime_error& e) { return e.what(); }
    return {};
}

std::vector<AuthGrant> userGrants() {
    return {prefix(SO::Create, ST::Topic, ""), prefix(SO::Describe, ST::Topic, ""), prefix(SO::Write, ST::Topic, ""),
            prefix(SO::Read, ST::Topic, ""), prefix(SO::Read, ST::Group, ""), prefix(SO::Describe, ST::Group, ""),
            prefix(SO::Write, ST::TransactionalId, ""), prefix(SO::Describe, ST::TransactionalId, "")};
}

bool waitFor(const std::function<bool()>& pred, std::chrono::seconds limit = 60s) {
    const auto deadline = std::chrono::steady_clock::now() + limit;
    while (std::chrono::steady_clock::now() < deadline) {
        if (pred()) return true;
        std::this_thread::sleep_for(20ms);
    }
    return pred();
}

std::span<const uint8_t> bytesOf(const std::string& s) { return {reinterpret_cast<const uint8_t*>(s.data()), s.size()}; }

/// The listing of namespaces by the cluster admin.
std::vector<pc::NamespaceInfo> listAll(rc::Client& admin) {
    auto r = admin.listNamespaces();
    return r ? *r : std::vector<pc::NamespaceInfo>{};
}
bool hasNs(rc::Client& admin, uint32_t nsId) {
    for (const auto& n : listAll(admin)) if (n.nsId == nsId) return true;
    return false;
}
uint32_t idOf(rc::Client& admin, const std::string& name) {
    for (const auto& n : listAll(admin)) if (n.name == name && n.state != 4) return n.nsId;
    return 0;
}

/// Namespaces a and b, a user in each, a topic `t` in each.
struct Fix {
    AuthBroker b;
    std::unique_ptr<rc::Client> admin;
    rc::TopicSpec tA, tB;
    uint32_t aId = 0;
    explicit Fix(const std::string& tag) : b(tag) {
        b.bootstrapAdmin(kAdminPw);
        admin = connected(b, "admin");
        for (const char* ns : {"a", "b"}) {
            auto r = admin->createNamespace(ns);
            if (!r) throw std::runtime_error(std::string("createNamespace ") + ns + ": " + r.error().detail);
        }
        for (const char* u : {"a/ua", "b/ub"}) b.createUser(u, kUserPw, userGrants());
        tA = defineT("a/ua");
        tB = defineT("b/ub");
        aId = idOf(*admin, "a");
    }
    rc::TopicSpec defineT(const std::string& user) {
        auto c = connected(b, user);
        const auto r = c->defineTopicAndWait("t", 1, "", false, {}, 20s);
        if (r.errorCode != 0) throw std::runtime_error("define t: " + r.detail);
        c->close();
        return rc::TopicSpec{"t", r.topicId, 1};
    }
    [[nodiscard]] fs::path dataDir() const { return b.dir() / "broker"; }
};

}  // namespace

// Delete `a` (a topic with records, a committed group offset, a committed transaction, a user): the old nsId leaves the
// listing, its directories are gone, the name is free at once; the namespace created under the same name sees nothing of
// the old one - not its user, not its topics, not its committed offset.
TEST(NamespaceDeleteRegression, RecreateSeesNothingOld) {
    Fix f("nsdel_recreate");
    // Data of every kind in `a`.
    {
        auto ua = connected(f.b, "a/ua", {f.tA});
        rc::ProducerConfig pcfg;
        pcfg.acks = rc::Acks::All;
        rc::Producer p = ua->producer(pcfg);
        for (int i = 0; i < 5; ++i) p.send(ua->topic("t"), rc::Key(int64_t{i}), bytesOf("old" + std::to_string(i)));
        ASSERT_EQ(p.closeAndWait(30s), rc::OpStatus::Ok);
        rc::GroupConfig gcfg;
        gcfg.autoCommit = false;
        gcfg.autoOffsetReset = rc::OffsetReset::Earliest;
        gcfg.sessionTimeoutMs = 10'000;
        rc::GroupConsumer gc = ua->subscribeGroup("g", {"t"}, gcfg);
        std::atomic<size_t> n{0};
        gc.onRecords([&](const rc::RecordSet& rs) { rs.forEachRecord([&](std::string_view, std::string_view, int64_t, int64_t) { ++n; }); });
        ASSERT_TRUE(waitFor([&] { return n.load() >= 5; }, 40s));
        gc.commitSyncAndWait(20s);
        gc.close();
        rc::ProducerConfig tcfg;
        tcfg.acks = rc::Acks::All;
        tcfg.transactionalId = "tx";
        rc::Producer tp = ua->producer(tcfg);
        tp.initTransactionsAndWait(30s);
        tp.beginTransaction();
        tp.send(ua->topic("t"), rc::Key(int64_t{9}), bytesOf("oldtx"));
        tp.commitTransactionAndWait(30s);
        tp.close();
        ua->close();
    }
    const uint32_t oldId = f.aId;
    ASSERT_NE(oldId, 0u);
    const fs::path oldDir = nsRoot(f.dataDir(), oldId);
    ASSERT_TRUE(fs::exists(oldDir)) << "a's data dir exists before the delete";

    const auto del = f.admin->deleteNamespace("a");
    ASSERT_TRUE(del.has_value()) << del.error().errorCode << " " << del.error().detail;
    // The name is free at once (the record is DELETING, reachable by id only).
    auto again = f.admin->createNamespace("a");
    ASSERT_TRUE(again.has_value()) << again.error().detail;
    EXPECT_NE(*again, oldId) << "a namespace id is never reused";
    // The reaper finishes: the old id leaves the listing, its directories go.
    ASSERT_TRUE(waitFor([&] { return !hasNs(*f.admin, oldId); }, 90s)) << "the old namespace was never erased";
    ASSERT_TRUE(waitFor([&] {
        return !fs::exists(oldDir) && !fs::exists(fs::path(oldDir.string() + ".deleting"));
    }, 60s)) << "the old ns-<id> directory (or its .deleting) is still on disk";
    EXPECT_TRUE(hasNs(*f.admin, *again));

    // The old user is gone (a new login of it fails); a user of the new `a` sees an empty namespace.
    auto old = newClient(f.b, "a/ua");
    EXPECT_NE(connectError(*old).find("44"), std::string::npos) << "the deleted namespace's user logged in";
    f.b.createUser("a/ua", kUserPw, userGrants());   // same name, new namespace id: allowed
    auto ua = connected(f.b, "a/ua");
    EXPECT_TRUE(ua->listTopics().empty()) << "the re-created namespace lists a topic of the old one";
    const auto nt = ua->defineTopicAndWait("t", 1, "", false, {}, 20s);
    ASSERT_EQ(nt.errorCode, 0) << nt.detail;
    rc::TopicSpec newT{"t", nt.topicId, 1};
    ua->close();
    auto ua2 = connected(f.b, "a/ua", {newT});
    {
        rc::ProducerConfig pcfg;
        pcfg.acks = rc::Acks::All;
        rc::Producer p = ua2->producer(pcfg);
        for (int i = 0; i < 3; ++i) p.send(ua2->topic("t"), rc::Key(int64_t{i}), bytesOf("new" + std::to_string(i)));
        ASSERT_EQ(p.closeAndWait(30s), rc::OpStatus::Ok);
    }
    // The same group id in the new namespace starts at offset 0 (no committed offset of the old one) and the topic holds
    // only the 3 new records.
    rc::GroupConfig gcfg;
    gcfg.autoCommit = false;
    gcfg.autoOffsetReset = rc::OffsetReset::Earliest;
    gcfg.sessionTimeoutMs = 10'000;
    rc::GroupConsumer gc = ua2->subscribeGroup("g", {"t"}, gcfg);
    std::mutex mu;
    std::vector<std::string> got;
    gc.onRecords([&](const rc::RecordSet& rs) {
        std::lock_guard<std::mutex> lk(mu);
        rs.forEachRecord([&](std::string_view, std::string_view v, int64_t, int64_t) { got.emplace_back(v); });
    });
    ASSERT_TRUE(waitFor([&] { std::lock_guard<std::mutex> lk(mu); return got.size() >= 3; }, 40s));
    std::this_thread::sleep_for(300ms);
    {
        std::lock_guard<std::mutex> lk(mu);
        EXPECT_EQ(got, (std::vector<std::string>{"new0", "new1", "new2"})) << "an old record or offset leaked into the re-created namespace";
    }
    gc.close();
    ua2->close();
    // b is untouched.
    auto ub = connected(f.b, "b/ub", {f.tB});
    EXPECT_EQ(ub->topic("t").topicId, f.tB.topicId);
}

// A login of a user of a DELETING namespace gets AuthenticationFailed (the name is gone from the index at once), and so
// does one after the erase.
TEST(NamespaceDeleteRegression, LoginDuringDeletingAuthFailedRegression) {
    Fix f("nsdel_login");
    auto live = connected(f.b, "a/ua");
    const uint32_t oldId = f.aId;
    ASSERT_TRUE(f.admin->deleteNamespace("a").has_value());
    {
        auto c = newClient(f.b, "a/ua");
        const std::string err = connectError(*c);
        EXPECT_NE(err.find("44"), std::string::npos) << "during DELETING: " << err;
    }
    ASSERT_TRUE(waitFor([&] { return !hasNs(*f.admin, oldId); }, 90s));
    {
        auto c = newClient(f.b, "a/ua");
        const std::string err = connectError(*c);
        EXPECT_NE(err.find("44"), std::string::npos) << "after the erase: " << err;
    }
}

// Leftovers of an erased namespace are removed at boot: ns-<id>.deleting (a purge the process did not finish) and ns-<id>
// itself (a crash between the erase and the rename).
TEST(NamespaceDeleteRegression, DeletingDirRemovedAtBootRegression) {
    Fix f("nsdel_boot");
    const uint32_t id = f.aId;
    ASSERT_TRUE(f.admin->deleteNamespace("a").has_value());
    ASSERT_TRUE(waitFor([&] { return !hasNs(*f.admin, id); }, 90s));
    ASSERT_TRUE(waitFor([&] { return !fs::exists(nsRoot(f.dataDir(), id)); }, 60s));
    const fs::path deleting = fs::path(nsRoot(f.dataDir(), id).string() + ".deleting");
    const fs::path plain = nsRoot(f.dataDir(), id);
    for (const fs::path& p : {deleting, plain}) {
        fs::create_directories(p / "topics" / "t" / "0");
        std::ofstream(p / "topics" / "t" / "0" / "seg.log") << "data";
    }
    f.admin->close();
    f.b.restart(/*reloginAdmin=*/false);
    EXPECT_FALSE(fs::exists(deleting)) << "the .deleting dir of an erased namespace survived the boot";
    EXPECT_FALSE(fs::exists(plain)) << "the ns dir of an erased namespace survived the boot";
}

// A ns-<id>.deleting beside a LIVE namespace of that id is a fatal mismatch: logged, counted, the broker refuses to start.
TEST(NamespaceDeleteRegression, LiveNsDeletingDirIsFatalRegression) {
    Fix f("nsdel_fatal");
    const fs::path deleting = fs::path(nsRoot(f.dataDir(), f.aId).string() + ".deleting");
    fs::create_directories(deleting / "topics");
    f.admin->close();
    EXPECT_THROW(f.b.restart(/*reloginAdmin=*/false), std::exception);
    EXPECT_TRUE(fs::exists(deleting)) << "nothing is removed beside a live namespace";
}

// The pure reconcile over the four states a namespace dir can be in.
TEST(NamespaceDeleteRegression, ReconcileNsDirsByStateRegression) {
    const fs::path root = fs::temp_directory_path() / ("rune_nsdel_pure_" + std::to_string(::getpid()));
    fs::remove_all(root);
    const auto touch = [&](const std::string& dir) {
        fs::create_directories(root / dir / "topics");
        std::ofstream(root / dir / "topics" / "x") << "d";
    };
    // facts: 0 default (live), 1 live, 2 DELETING, 3 erased (id < nextNsId = 7), 5 never issued here (>= 7 is 9)
    touch("ns-0"); touch("ns-1"); touch("ns-2"); touch("ns-2.deleting"); touch("ns-1.deleting");
    touch("ns-3"); touch("ns-3.deleting"); touch("ns-4.deleting"); touch("ns-9"); touch("ns-9.deleting");
    touch("ns-01.deleting"); touch("junk.deleting");
    const std::vector<NsDirFact> facts{{0, false}, {1, false}, {2, true}};
    const auto res = reconcileNsDeletingDirs(root.string(), facts, /*nextNsId=*/7);
    EXPECT_FALSE(fs::exists(root / "ns-3")) << "an erased namespace's dir";
    EXPECT_FALSE(fs::exists(root / "ns-3.deleting"));
    EXPECT_FALSE(fs::exists(root / "ns-4.deleting")) << "an erased id with no dir of its own";
    EXPECT_TRUE(fs::exists(root / "ns-2.deleting")) << "a DELETING namespace's leftover is kept";
    EXPECT_TRUE(fs::exists(root / "ns-2"));
    EXPECT_TRUE(fs::exists(root / "ns-1")) << "a live namespace's own dir";
    EXPECT_TRUE(fs::exists(root / "ns-1.deleting")) << "the fatal mismatch is reported, not repaired";
    EXPECT_TRUE(fs::exists(root / "ns-0"));
    EXPECT_TRUE(fs::exists(root / "ns-9")) << "an id this catalog never issued is not ours to judge";
    EXPECT_TRUE(fs::exists(root / "ns-9.deleting"));
    EXPECT_TRUE(fs::exists(root / "ns-01.deleting")) << "not a ns-<u32> name";
    EXPECT_TRUE(fs::exists(root / "junk.deleting"));
    ASSERT_EQ(res.liveMismatch.size(), 1u);
    EXPECT_EQ(res.liveMismatch[0].filename().string(), "ns-1.deleting");
    EXPECT_EQ(res.keptDeleting.size(), 1u);
    EXPECT_EQ(res.removed.size(), 1u);      // ns-4.deleting (ns-3.deleting went with ns-3, below)
    EXPECT_EQ(res.erasedDirs.size(), 1u);   // ns-3 (renamed over its stale .deleting, then removed)
    fs::remove_all(root);
}
