// NamespaceClientTest.cpp - namespaces Task 11: the REAL client API (createNamespace / listNamespaces /
// setNamespaceLimits) and the REAL `rune namespace` / `rune ql` CLI against a REAL authenticating BrokerNode.
// Also the Task 8 rows 1, 6 and 8 (a namespace admin's security DDL) through QL text and the CLI.
#include "support/AuthBroker.h"
#include "support/AuthRig.h"

#include <rune/cli/Cli.h>
#include <rune/cli/NamespaceCommands.h>
#include <rune/client/Client.h>
#include <rune/client/NamespaceLimitFields.h>

#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <thread>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

using namespace rune::node;
using namespace rune::node::test;
namespace grants = rune::node::test::grants;
namespace pc = rune::protocol::client;
namespace rc = rune::client;
namespace fs = std::filesystem;
using namespace std::chrono_literals;

namespace {

constexpr const char* kAdminPw = "namespace-admin-password-1";
constexpr const char* kNsAdminPw = "ns-a-admin-password-1234";

std::unique_ptr<rc::Client> makeClient(AuthBroker& b, const std::string& user, const std::string& password,
                                       int32_t announceVersion = 0) {
    rc::ClientConfig c;
    c.gatewayChannel  = b.channel();
    c.gatewayStreamId = 1;
    c.egressChannel   = "aeron:udp?endpoint=127.0.0.1:" + std::to_string(rune::aeronutil::testing::allocFreeUdpPort());
    c.egressStreamId  = 2;
    c.dataDir         = b.clientDir().string();
    c.username        = user;
    c.password        = password;
    c.offerCrypto     = true;
    c.tlsTrustPem     = AuthBroker::tlsCaPem();
    c.testAnnounceClientVersion = announceVersion;
    auto client = std::make_unique<rc::Client>(c);
    client->connectAndWait(20s);
    return client;
}

const pc::NamespaceInfo* findNs(const std::vector<pc::NamespaceInfo>& v, const std::string& name) {
    const auto it = std::find_if(v.begin(), v.end(), [&](const pc::NamespaceInfo& n) { return n.name == name; });
    return it == v.end() ? nullptr : &*it;
}

fs::path scratchDir() {
    static const fs::path d = fs::temp_directory_path() / ("rune_ns_cli_test_" + std::to_string(::getpid()));
    fs::create_directories(d);   // every call: each test's ScratchCleanup removes it on exit
    return d;
}
std::string writeTemp(const std::string& name, const std::string& content) {
    const fs::path p = scratchDir() / name;
    std::ofstream o(p, std::ios::binary);
    o << content;
    fs::permissions(p, fs::perms::owner_read | fs::perms::owner_write);
    return p.string();
}
int runCli(std::vector<std::string> args, std::string* out = nullptr, std::string* err = nullptr) {
    args.push_back("--tls-ca");
    args.push_back(AuthBroker::tlsCaFile().string());
    std::stringstream o, e;
    const int rc = rune::cli::run(std::move(args), o, e);
    if (out) *out = o.str();
    if (err) *err = e.str();
    return rc;
}
/// `rune ql -f <statement file>` as `user` (a password literal never goes in argv).
int ql(AuthBroker& b, const std::string& user, const std::string& pwFile, const std::string& statement,
       std::string* err = nullptr) {
    static int n = 0;
    const std::string file = writeTemp("ns_stmt" + std::to_string(++n) + ".rql", statement + "\n");
    std::string out, e;
    const int rc = runCli({"ql", "--broker", b.endpoint(), "--user", user, "--password-file", pwFile, "-f", file}, &out, &e);
    if (err) *err = e;
    return rc;
}

std::string squeeze(const std::string& s) {   // table padding -> single spaces
    std::string out;
    bool space = false;
    for (const char c : s) {
        if (c == ' ') { if (!space) out += c; space = true; }
        else { out += c; space = false; }
    }
    return out;
}

struct ScratchCleanup {
    ~ScratchCleanup() { std::error_code ec; fs::remove_all(scratchDir(), ec); }
};

}  // namespace

// The client API: create (with a limits patch: the named fields, every other one the broker default), list (pages
// internally; `default` and the new namespace), setNamespaceLimits (the full record, visible in the next list), the
// cluster's maxNamespaces refusal (NamespaceLimitExceeded on v14), and a v13 client answered InvalidRequest.
TEST(NamespaceClientRegression, CreateListLimitsRegression) {
    AuthBroker::Options o;
    o.configure = [](NodeConfig& cfg) { cfg.maxNamespaces = 3; };   // default + a + b; a 4th is refused
    AuthBroker b("nsclient", o);
    b.bootstrapAdmin(kAdminPw);
    auto c = makeClient(b, "admin", kAdminPw);

    rc::NamespaceLimitsPatch patch;
    ASSERT_TRUE(patch.set(*rc::namespaceLimitIndex("max-topics"), 7));
    const auto a = c->createNamespace("a", patch);
    ASSERT_TRUE(a.has_value()) << a.error().errorCode << " " << a.error().detail;
    EXPECT_EQ(*a, 1u);
    const auto again = c->createNamespace("a");
    ASSERT_FALSE(again.has_value());
    EXPECT_NE(again.error().errorCode, 0);

    auto list = c->listNamespaces();
    ASSERT_TRUE(list.has_value()) << list.error().errorCode;
    const auto* def = findNs(*list, "default");
    const auto* na = findNs(*list, "a");
    ASSERT_NE(def, nullptr);
    ASSERT_NE(na, nullptr);
    EXPECT_EQ(def->nsId, 0u);
    EXPECT_EQ(na->nsId, 1u);
    EXPECT_EQ(na->state, 0);
    EXPECT_EQ(na->limits.maxTopics, 7u) << "the patched field";
    EXPECT_EQ(na->limits.maxViews, def->limits.maxViews) << "an unnamed field takes the broker default";

    rune::protocol::NamespaceLimits full = na->limits;
    full.maxTopics = 11;
    full.featureBits = 1;
    const auto set = c->setNamespaceLimits("a", full);
    ASSERT_TRUE(set.has_value()) << set.error().errorCode << " " << set.error().detail;
    list = c->listNamespaces();
    ASSERT_TRUE(list.has_value());
    na = findNs(*list, "a");
    ASSERT_NE(na, nullptr);
    EXPECT_EQ(na->limits, full);
    const auto noSuch = c->setNamespaceLimits("zz", full);
    ASSERT_FALSE(noSuch.has_value());
    EXPECT_NE(noSuch.error().errorCode, 0);

    // A v13 client never reaches namespace DDL: InvalidRequest from the gateway, while there is still room (so only
    // the version gate can refuse it), and nothing is created.
    {
        auto old = makeClient(b, "admin", kAdminPw, 13);
        const auto oc = old->createNamespace("old");
        ASSERT_FALSE(oc.has_value());
        EXPECT_EQ(oc.error().errorCode, static_cast<uint16_t>(pc::ErrorCode::InvalidRequest));
        list = c->listNamespaces();
        ASSERT_TRUE(list.has_value());
        EXPECT_EQ(findNs(*list, "old"), nullptr);
        EXPECT_EQ(list->size(), 2u);
    }

    // maxNamespaces: b fits, c does not.
    const auto nb = c->createNamespace("b");
    ASSERT_TRUE(nb.has_value()) << nb.error().errorCode;
    const auto nc = c->createNamespace("c");
    ASSERT_FALSE(nc.has_value());
    EXPECT_EQ(nc.error().errorCode, static_cast<uint16_t>(pc::ErrorCode::NamespaceLimitExceeded));

    list = c->listNamespaces();
    ASSERT_TRUE(list.has_value());
    EXPECT_EQ(list->size(), 3u);
    EXPECT_EQ(findNs(*list, "c"), nullptr);
}

// Issue #641: `limits --set` sends only the named fields and the broker merges them onto the CURRENT record.  Two
// CLI runs for two fields both persist; and a Client holding a STALE read cannot overwrite a field it did not name.
TEST(NamespaceCliRegression, NamespaceLimitSetsOfDifferentFieldsBothPersistRegression) {
    AuthBroker b("nslimits641");
    b.bootstrapAdmin(kAdminPw);
    const std::string adminPw = writeTemp("admin-pw641", std::string(kAdminPw) + "\n");
    const std::vector<std::string> as = {"--broker", b.endpoint(), "--user", "admin", "--password-file", adminPw};
    auto with = [&](std::vector<std::string> args) { args.insert(args.end(), as.begin(), as.end()); return args; };
    std::string out, err;
    ASSERT_EQ(runCli(with({"namespace", "create", "a", "--max-topics", "5", "--max-views", "6"}), &out, &err), 0) << err;
    ASSERT_EQ(runCli(with({"namespace", "limits", "a", "--set", "max-topics=9"}), &out, &err), 0) << err;
    ASSERT_EQ(runCli(with({"namespace", "limits", "a", "--set", "max-views=11"}), &out, &err), 0) << err;
    ASSERT_EQ(runCli(with({"namespace", "list", "--json"}), &out, &err), 0) << err;
    EXPECT_NE(out.find("\"max-topics\":9,"), std::string::npos) << out;
    EXPECT_NE(out.find("\"max-views\":11,"), std::string::npos) << out;

    // The Client level: `stale` is read once; another writer changes max-topics; a patch naming only max-views,
    // built next to that stale read, must leave the other writer's max-topics alone.
    auto c = makeClient(b, "admin", kAdminPw);
    const auto stale = c->listNamespaces();
    ASSERT_TRUE(stale.has_value());
    const auto* sa = findNs(*stale, "a");
    ASSERT_NE(sa, nullptr);
    EXPECT_EQ(sa->limits.maxTopics, 9u);
    rc::NamespaceLimitsPatch other;
    ASSERT_TRUE(other.set(*rc::namespaceLimitIndex("max-topics"), 21));
    ASSERT_TRUE(c->setNamespaceLimits("a", other).has_value());
    rc::NamespaceLimitsPatch mine;
    ASSERT_TRUE(mine.set(*rc::namespaceLimitIndex("max-views"), 33));
    ASSERT_TRUE(c->setNamespaceLimits("a", mine).has_value());
    const auto now = c->listNamespaces();
    ASSERT_TRUE(now.has_value());
    const auto* na = findNs(*now, "a");
    ASSERT_NE(na, nullptr);
    EXPECT_EQ(na->limits.maxTopics, 21u) << "the other writer's field survives a patch that did not name it";
    EXPECT_EQ(na->limits.maxViews, 33u);
    EXPECT_THROW((void)c->setNamespaceLimits("a", rc::NamespaceLimitsPatch{}), std::invalid_argument);
    c->close();
}

// The CLI: `rune namespace create|list|limits` end to end, then the Task 8 rows 1, 6 and 8 as a namespace admin of
// `a` through `rune ql`: GRANT ADMIN ON CLUSTER is NotAuthorized (45), CREATE USER 'b/u2' answers like a missing
// namespace (39, "no namespace '"), CREATE ROLE is allowed.
TEST(NamespaceCliRegression, CreateListLimitsAndEscalationRowsRegression) {
    ScratchCleanup cleanup;
    AuthBroker b("nscli");
    b.bootstrapAdmin(kAdminPw);
    const std::string adminPw = writeTemp("admin-pw", std::string(kAdminPw) + "\n");
    const std::vector<std::string> as = {"--broker", b.endpoint(), "--user", "admin", "--password-file", adminPw};
    auto with = [&](std::vector<std::string> args) { args.insert(args.end(), as.begin(), as.end()); return args; };
    std::string out, err;

    ASSERT_EQ(runCli(with({"namespace", "create", "a", "--max-topics", "5", "--max-views", "6"}), &out, &err), 0) << err;
    EXPECT_NE(out.find("created namespace a (nsId 1)"), std::string::npos) << out;
    EXPECT_EQ(runCli(with({"namespace", "create", "a"}), &out, &err), 1) << "already exists";

    ASSERT_EQ(runCli(with({"namespace", "list"}), &out, &err), 0) << err;
    EXPECT_NE(out.find("default"), std::string::npos) << out;
    EXPECT_NE(squeeze(out).find("\n1 a active\n"), std::string::npos) << out;
    ASSERT_EQ(runCli(with({"namespace", "list", "--json"}), &out, &err), 0) << err;
    EXPECT_NE(out.find("{\"nsId\":1,\"name\":\"a\",\"state\":\"active\",\"limits\":{\"max-topics\":5,"), std::string::npos) << out;
    EXPECT_NE(out.find("\"max-views\":6,"), std::string::npos) << out;

    // limits: print, then read-modify-write one field (the other keeps its value).
    ASSERT_EQ(runCli(with({"namespace", "limits", "a"}), &out, &err), 0) << err;
    EXPECT_NE(out.find("max-topics"), std::string::npos) << out;
    ASSERT_EQ(runCli(with({"namespace", "limits", "a", "--set", "max-topics=9"}), &out, &err), 0) << err;
    EXPECT_NE(out.find("updated limits of namespace a"), std::string::npos) << out;
    ASSERT_EQ(runCli(with({"namespace", "list", "--json"}), &out, &err), 0) << err;
    EXPECT_NE(out.find("\"limits\":{\"max-topics\":9,"), std::string::npos) << out;
    EXPECT_NE(out.find("\"max-views\":6,"), std::string::npos) << "--set kept the other fields: " << out;
    EXPECT_EQ(runCli(with({"namespace", "limits", "zz"}), &out, &err), 1);
    EXPECT_NE(err.find("no namespace 'zz'"), std::string::npos) << err;

    // Task 8 rows 1, 6, 8 through QL text: a namespace admin of `a`.
    ASSERT_EQ(ql(b, "admin", adminPw, "create user 'a/nsadmin' with password '" + std::string(kNsAdminPw) + "';", &err), 0) << err;
    ASSERT_EQ(ql(b, "admin", adminPw, "grant admin on namespace 'a' to user 'a/nsadmin';", &err), 0) << err;
    const std::string nsPw = writeTemp("nsadmin-pw", std::string(kNsAdminPw) + "\n");
    EXPECT_NE(ql(b, "a/nsadmin", nsPw, "grant admin on cluster to user 'a/nsadmin';", &err), 0);   // row 1
    EXPECT_NE(err.find("(45)"), std::string::npos) << err;
    EXPECT_NE(ql(b, "a/nsadmin", nsPw, "create user 'b/u2' with password 'u2-password-12345678';", &err), 0);   // row 6
    EXPECT_NE(err.find("(39)"), std::string::npos) << err;
    EXPECT_NE(err.find("no namespace '"), std::string::npos) << err;
    EXPECT_EQ(ql(b, "a/nsadmin", nsPw, "create role x;", &err), 0) << err;   // row 8
    // ... and a namespace admin cannot run namespace DDL (cluster admin only).
    EXPECT_EQ(runCli({"namespace", "create", "n2", "--broker", b.endpoint(), "--user", "a/nsadmin", "--password-file", nsPw},
                     &out, &err), 1);
}

// ── Phase 2 Task 16 (D29): `rune namespace suspend | resume | delete | usage` end to end ───────────────────────────────────────
// Interaction matrix (verb x namespace state x TTY):
//   | verb                         | state before            | input                      | expected                                          | test
//   |------------------------------|-------------------------|----------------------------|---------------------------------------------------|-----
//   | suspend --read-only / --reject-all | active            | exactly one mode           | state follows in `list`; no / both modes = usage 2 | NamespaceCliRegression.SuspendResumeDeleteRegression
//   | resume                       | suspended               | -                          | active again                                      | NamespaceCliRegression.SuspendResumeDeleteRegression
//   | delete --yes                 | active                  | -                          | the namespace leaves `list`                       | NamespaceCliRegression.SuspendResumeDeleteRegression
//   | delete (no --yes)            | active                  | no TTY / TTY "n" / TTY "y" | refused (1) / aborted (1) / deleted (0)           | NamespaceCliRegression.DeleteWithoutYesPromptsRegression
//   | usage                        | 1 topic of 2 partitions | table / --json             | `limit value usage` rows with the counts          | NamespaceCliRegression.UsageTableRegression
//   | suspend / resume / delete / usage | unknown name       | -                          | exit 1 with the broker's refusal                  | NamespaceCliRegression.UnknownNamespaceRegression
namespace {
std::string stateOf(AuthBroker& b, const std::string& adminPw, const std::string& name) {
    std::string out, err;
    if (runCli({"namespace", "list", "--json", "--broker", b.endpoint(), "--user", "admin", "--password-file", adminPw}, &out, &err) != 0)
        return "?";
    const std::string key = "\"name\":\"" + name + "\",\"state\":\"";
    const auto at = out.find(key);
    if (at == std::string::npos) return "";   // gone
    const auto from = at + key.size();
    return out.substr(from, out.find('"', from) - from);
}
bool waitState(AuthBroker& b, const std::string& pw, const std::string& name, const std::string& want) {
    const auto deadline = std::chrono::steady_clock::now() + 30s;
    while (std::chrono::steady_clock::now() < deadline) {
        if (stateOf(b, pw, name) == want) return true;
        std::this_thread::sleep_for(50ms);
    }
    return stateOf(b, pw, name) == want;
}
}  // namespace

TEST(NamespaceCliRegression, SuspendResumeDeleteRegression) {
    ScratchCleanup cleanup;
    AuthBroker b("nsclisrd");
    b.bootstrapAdmin(kAdminPw);
    const std::string pw = writeTemp("admin-pw", std::string(kAdminPw) + "\n");
    const std::vector<std::string> as = {"--broker", b.endpoint(), "--user", "admin", "--password-file", pw};
    auto with = [&](std::vector<std::string> args) { args.insert(args.end(), as.begin(), as.end()); return args; };
    std::string out, err;
    ASSERT_EQ(runCli(with({"namespace", "create", "s"}), &out, &err), 0) << err;

    EXPECT_EQ(runCli(with({"namespace", "suspend", "s"}), &out, &err), 2) << "a mode is required";
    EXPECT_NE(err.find("exactly one of --read-only or --reject-all"), std::string::npos) << err;
    EXPECT_EQ(runCli(with({"namespace", "suspend", "s", "--read-only", "--reject-all"}), &out, &err), 2) << "both modes";
    EXPECT_EQ(stateOf(b, pw, "s"), "active") << "a usage error changed nothing";

    ASSERT_EQ(runCli(with({"namespace", "suspend", "s", "--read-only"}), &out, &err), 0) << err;
    EXPECT_NE(out.find("suspended namespace s (read-only)"), std::string::npos) << out;
    EXPECT_EQ(stateOf(b, pw, "s"), "suspended-read-only");
    ASSERT_EQ(runCli(with({"namespace", "suspend", "s", "--reject-all"}), &out, &err), 0) << err;
    EXPECT_EQ(stateOf(b, pw, "s"), "suspended-reject-all");
    ASSERT_EQ(runCli(with({"namespace", "resume", "s"}), &out, &err), 0) << err;
    EXPECT_NE(out.find("resumed namespace s"), std::string::npos) << out;
    EXPECT_EQ(stateOf(b, pw, "s"), "active");

    ASSERT_EQ(runCli(with({"namespace", "delete", "s", "--yes"}), &out, &err), 0) << err;
    EXPECT_NE(out.find("deleting namespace s"), std::string::npos) << out;
    EXPECT_TRUE(waitState(b, pw, "s", "")) << "the namespace is erased (the reaper drives the delete to completion)";
}

TEST(NamespaceCliRegression, DeleteWithoutYesPromptsRegression) {
    ScratchCleanup cleanup;
    AuthBroker b("nsclidel");
    b.bootstrapAdmin(kAdminPw);
    const std::string pw = writeTemp("admin-pw", std::string(kAdminPw) + "\n");
    ASSERT_EQ(runCli({"namespace", "create", "d", "--broker", b.endpoint(), "--user", "admin", "--password-file", pw}), 0);
    const std::vector<std::string> args = {"delete", "d", "--broker", b.endpoint(), "--user", "admin", "--password-file", pw,
                                           "--tls-ca", AuthBroker::tlsCaFile().string()};
    auto del = [&](const std::string& input, bool tty, std::string& out, std::string& err) {
        std::istringstream in(input);
        std::ostringstream o, e;
        const int rc = rune::cli::cmdNamespace(args, in, tty, o, e);
        out = o.str();
        err = e.str();
        return rc;
    };
    std::string out, err;
    // Not a terminal: refused, nothing asked, nothing deleted - even when stdin happens to hold a "y".
    EXPECT_EQ(del("y\n", false, out, err), 1);
    EXPECT_NE(err.find("without --yes"), std::string::npos) << err;
    EXPECT_EQ(err.find("[y/N]"), std::string::npos) << "no prompt without a terminal: " << err;
    EXPECT_EQ(stateOf(b, pw, "d"), "active");
    // A terminal: the prompt names the namespace; anything but yes aborts.
    for (const char* no : {"n\n", "\n", "nope\n", ""}) {
        EXPECT_EQ(del(no, true, out, err), 1) << "[" << no << "]";
        EXPECT_NE(err.find("Delete namespace 'd'"), std::string::npos) << err;
        EXPECT_NE(err.find("[y/N]"), std::string::npos) << err;
        EXPECT_NE(err.find("aborted"), std::string::npos) << err;
        EXPECT_EQ(stateOf(b, pw, "d"), "active") << "an unconfirmed delete deletes nothing";
    }
    // A terminal and "y": deleted.
    EXPECT_EQ(del("y\n", true, out, err), 0) << err;
    EXPECT_TRUE(waitState(b, pw, "d", "")) << "confirmed: the namespace is gone";
}

TEST(NamespaceCliRegression, UsageTableRegression) {
    ScratchCleanup cleanup;
    AuthBroker b("nscliusage");
    b.bootstrapAdmin(kAdminPw);
    const std::string pw = writeTemp("admin-pw", std::string(kAdminPw) + "\n");
    const std::vector<std::string> as = {"--broker", b.endpoint(), "--user", "admin", "--password-file", pw};
    auto with = [&](std::vector<std::string> args) { args.insert(args.end(), as.begin(), as.end()); return args; };
    std::string out, err;
    ASSERT_EQ(runCli(with({"namespace", "create", "u", "--max-topics", "5"}), &out, &err), 0) << err;
    b.createUser("u/w", "namespace-user-password-12",
                 {grants::prefix(grants::SO::Create, grants::ST::Topic, ""), grants::prefix(grants::SO::Describe, grants::ST::Topic, "")});
    auto w = makeClient(b, "u/w", "namespace-user-password-12");
    ASSERT_EQ(w->defineTopicAndWait("t", 2, "", false, {}, 60s).errorCode, 0);

    // The table: header, state line, `limit  value  usage` rows; the topic count is next to its limit.
    bool seen = false;
    for (int i = 0; i < 200 && !seen; ++i) {
        ASSERT_EQ(runCli(with({"namespace", "usage", "u"}), &out, &err), 0) << err;
        seen = squeeze(out).find("\nmax-topics 5 1\n") != std::string::npos;
        if (!seen) std::this_thread::sleep_for(50ms);
    }
    EXPECT_TRUE(seen) << out;
    EXPECT_EQ(out.rfind("namespace u (active)\n", 0), 0u) << out;
    EXPECT_NE(squeeze(out).find("\nlimit value usage\n"), std::string::npos) << out;
    EXPECT_NE(squeeze(out).find("\nmax-partitions-total 0 2\n"), std::string::npos) << "partitions total: " << out;
    EXPECT_NE(squeeze(out).find("\nmax-schemas 0 -\n"), std::string::npos) << "no usage meaning prints '-': " << out;
    // JSON.
    ASSERT_EQ(runCli(with({"namespace", "usage", "u", "--json"}), &out, &err), 0) << err;
    EXPECT_NE(out.find("\"name\":\"u\",\"state\":\"active\",\"limits\":{\"max-topics\":5,"), std::string::npos) << out;
    EXPECT_NE(out.find("\"usage\":{\"max-topics\":1,"), std::string::npos) << out;
    // A suspended namespace reports its state.
    ASSERT_EQ(runCli(with({"namespace", "suspend", "u", "--read-only"}), &out, &err), 0) << err;
    ASSERT_EQ(runCli(with({"namespace", "usage", "u"}), &out, &err), 0) << err;
    EXPECT_EQ(out.rfind("namespace u (suspended-read-only)\n", 0), 0u) << out;
}

TEST(NamespaceCliRegression, UnknownNamespaceRegression) {
    ScratchCleanup cleanup;
    AuthBroker b("nscliunk");
    b.bootstrapAdmin(kAdminPw);
    const std::string pw = writeTemp("admin-pw", std::string(kAdminPw) + "\n");
    const std::vector<std::string> as = {"--broker", b.endpoint(), "--user", "admin", "--password-file", pw};
    auto with = [&](std::vector<std::string> args) { args.insert(args.end(), as.begin(), as.end()); return args; };
    std::string out, err;
    for (const auto& cmd : std::vector<std::vector<std::string>>{
             {"namespace", "suspend", "zz", "--read-only"}, {"namespace", "resume", "zz"}, {"namespace", "delete", "zz", "--yes"},
             {"namespace", "usage", "zz"}}) {
        EXPECT_EQ(runCli(with(cmd), &out, &err), 1) << cmd[1];
        EXPECT_NE(err.find("rune namespace " + cmd[1] + ":"), std::string::npos) << err;
        EXPECT_TRUE(out.empty()) << cmd[1] << " printed on stdout: " << out;
    }
    EXPECT_NE(err.find("no such namespace"), std::string::npos) << "usage names the problem: " << err;
    // A subcommand without a name, and an unknown subcommand: usage errors.
    for (const char* verb : {"suspend", "resume", "delete", "usage"})
        EXPECT_EQ(runCli(with({"namespace", verb}), &out, &err), 2) << verb;
    EXPECT_EQ(runCli(with({"namespace", "frobnicate"}), &out, &err), 2);
    EXPECT_NE(err.find("unknown subcommand 'frobnicate'"), std::string::npos) << err;
    // `default` cannot be suspended or deleted (the broker refuses; the namespace stays active).
    EXPECT_EQ(runCli(with({"namespace", "delete", "default", "--yes"}), &out, &err), 1);
    EXPECT_EQ(stateOf(b, pw, "default"), "active");
}

// ── Adversarial review of client batch C8 (#641): limit patches through the real client against a real broker ──────

namespace {
uint64_t limitMaxOf(std::size_t i) {
    return i == rc::kNamespaceFeatureBitsField ? uint64_t{UINT32_MAX} : UINT64_MAX;
}
std::vector<uint64_t> limitsOf(rc::Client& c, const std::string& ns) {
    const auto all = c.listNamespaces();
    if (!all) return {};
    const auto* n = findNs(*all, ns);
    if (n == nullptr) return {};
    std::vector<uint64_t> v;
    for (std::size_t i = 0; i < rc::kNamespaceLimitFields.size(); ++i) v.push_back(rc::namespaceLimitGet(n->limits, i));
    return v;
}
} // namespace

TEST(NamespaceClientRegression, LimitPatchEachFieldAloneAndUnknownBitsAdversarialRegression) {
    AuthBroker b("nslimits641adv");
    b.bootstrapAdmin(kAdminPw);
    auto c = makeClient(b, "admin", kAdminPw);
    const std::size_t n = rc::kNamespaceLimitFields.size();
    rc::NamespaceLimitsPatch all;
    std::vector<uint64_t> want;
    for (std::size_t i = 0; i < n; ++i) {
        ASSERT_TRUE(all.set(i, 100 + i));
        want.push_back(100 + i);
    }
    ASSERT_TRUE(c->createNamespace("p", all).has_value());
    ASSERT_EQ(limitsOf(*c, "p"), want);
    // Every field alone, at its max: exactly that field changes.
    for (std::size_t i = 0; i < n; ++i) {
        rc::NamespaceLimitsPatch one;
        ASSERT_TRUE(one.set(i, limitMaxOf(i)));
        const auto r = c->setNamespaceLimits("p", one);
        ASSERT_TRUE(r.has_value()) << rc::kNamespaceLimitFields[i].name << ": " << r.error().errorCode << " " << r.error().detail;
        want[i] = limitMaxOf(i);
        ASSERT_EQ(limitsOf(*c, "p"), want) << "after setting only " << rc::kNamespaceLimitFields[i].name;
    }
    // A patch whose mask carries the reserved bits 29..31 is refused by the encoder: it never reaches the broker.
    rc::NamespaceLimitsPatch odd;
    ASSERT_TRUE(odd.set(0, 4242));
    odd.present[3] = static_cast<uint8_t>(odd.present[3] | 0xE0u);
    EXPECT_ANY_THROW((void)c->setNamespaceLimits("p", odd));
    EXPECT_EQ(limitsOf(*c, "p"), want);
    // A value without its mask bit is never applied.
    rc::NamespaceLimitsPatch partial;
    ASSERT_TRUE(partial.set(0, 4242));
    partial.values.featureBits = 7;
    partial.values.maxViews    = 7;
    ASSERT_TRUE(c->setNamespaceLimits("p", partial).has_value());
    want[0] = 4242;
    EXPECT_EQ(limitsOf(*c, "p"), want);
    // An empty patch never reaches the broker.
    EXPECT_THROW((void)c->setNamespaceLimits("p", rc::NamespaceLimitsPatch{}), std::invalid_argument);
    EXPECT_EQ(limitsOf(*c, "p"), want);
    // A patch on a namespace that does not exist is refused with a code, and creates nothing.
    rc::NamespaceLimitsPatch some;
    ASSERT_TRUE(some.set(0, 1));
    const auto missing = c->setNamespaceLimits("nope", some);
    ASSERT_FALSE(missing.has_value());
    EXPECT_NE(missing.error().errorCode, 0u);
    EXPECT_TRUE(limitsOf(*c, "nope").empty());
    c->close();
}

TEST(NamespaceClientRegression, ConcurrentLimitWritersNeverLoseAFieldAdversarialRegression) {
    // #641: two admins patch DIFFERENT fields of one namespace at the same time, 25 times each.  Each field ends at its
    // own writer's last value; the fields neither writer named keep their values.
    AuthBroker b("nslimits641race");
    b.bootstrapAdmin(kAdminPw);
    auto setup = makeClient(b, "admin", kAdminPw);
    const std::size_t n = rc::kNamespaceLimitFields.size();
    rc::NamespaceLimitsPatch all;
    for (std::size_t i = 0; i < n; ++i) ASSERT_TRUE(all.set(i, 1000 + i));
    ASSERT_TRUE(setup->createNamespace("r", all).has_value());
    auto c1 = makeClient(b, "admin", kAdminPw);
    auto c2 = makeClient(b, "admin", kAdminPw);
    const std::size_t f1 = *rc::namespaceLimitIndex("max-topics"), f2 = *rc::namespaceLimitIndex("max-views");
    constexpr int kRounds = 25;
    std::atomic<int> failures{0};
    auto writer = [&](rc::Client& c, std::size_t field, uint64_t base) {
        for (int k = 1; k <= kRounds; ++k) {
            rc::NamespaceLimitsPatch p;
            if (!p.set(field, base + static_cast<uint64_t>(k))) { ++failures; return; }
            if (!c.setNamespaceLimits("r", p).has_value()) ++failures;
        }
    };
    std::thread t1(writer, std::ref(*c1), f1, 10'000);
    std::thread t2(writer, std::ref(*c2), f2, 20'000);
    t1.join();
    t2.join();
    EXPECT_EQ(failures.load(), 0);
    const auto got = limitsOf(*setup, "r");
    ASSERT_EQ(got.size(), n);
    for (std::size_t i = 0; i < n; ++i) {
        const uint64_t want = i == f1 ? 10'000 + kRounds : i == f2 ? 20'000 + kRounds : 1000 + i;
        EXPECT_EQ(got[i], want) << rc::kNamespaceLimitFields[i].name;
    }
    c1->close();
    c2->close();
    setup->close();
}

TEST(NamespaceClientRegression, LimitPatchWithoutClusterAdminIsRefusedAdversarialRegression) {
    // Auth: a user with no grants, and a namespace admin of `a` (ADMIN ON NAMESPACE a, not ON CLUSTER), may not change
    // limits - their own namespace's included (a tenant raising its own quota is an escalation).
    AuthBroker b("nslimits641auth");
    b.bootstrapAdmin(kAdminPw);
    auto admin = makeClient(b, "admin", kAdminPw);
    rc::NamespaceLimitsPatch init;
    ASSERT_TRUE(init.set(*rc::namespaceLimitIndex("max-topics"), 5));
    ASSERT_TRUE(admin->createNamespace("a", init).has_value());
    const auto before = limitsOf(*admin, "a");
    b.createUser("nobody", kNsAdminPw, {});
    auto nobody = makeClient(b, "nobody", kNsAdminPw);
    rc::NamespaceLimitsPatch raise;
    ASSERT_TRUE(raise.set(*rc::namespaceLimitIndex("max-topics"), 1'000'000));
    for (const char* ns : {"a", "default"}) {
        const auto r = nobody->setNamespaceLimits(ns, raise);
        ASSERT_FALSE(r.has_value()) << ns;
        EXPECT_EQ(r.error().errorCode, static_cast<uint16_t>(pc::ErrorCode::NotAuthorized)) << ns << ": " << r.error().detail;
    }
    EXPECT_EQ(limitsOf(*admin, "a"), before) << "a refused patch changes nothing";
    // The CLI as the same user: refused (non-zero), nothing stored.
    fs::create_directories(scratchDir());
    const std::string pw = writeTemp("nobody-pw641", std::string(kNsAdminPw) + "\n");
    std::string out, err;
    const int code = runCli({"namespace", "limits", "a", "--set", "max-topics=1000000", "--broker", b.endpoint(), "--user",
                             "nobody", "--password-file", pw}, &out, &err);
    EXPECT_NE(code, 0) << out;
    EXPECT_EQ(limitsOf(*admin, "a"), before);
    nobody->close();
    admin->close();
}

// Finding (C8 adversarial review, minor): `rune namespace limits <ns> --set` pre-lists the namespaces, so a user without
// ADMIN ON CLUSTER is told "no namespace '<ns>'" (rc 1) instead of the broker's NotAuthorized (45): the patch is never
// sent.  The CLI sends the patch and reports the broker's code.
TEST(NamespaceClientRegression, LimitsCliRefusalNamesNotAuthorizedAdversarialRegression) {
    AuthBroker b("nslimits641authcli");
    b.bootstrapAdmin(kAdminPw);
    auto admin = makeClient(b, "admin", kAdminPw);
    ASSERT_TRUE(admin->createNamespace("a").has_value());
    b.createUser("nobody", kNsAdminPw, {});
    fs::create_directories(scratchDir());
    const std::string pw = writeTemp("nobody-pw641cli", std::string(kNsAdminPw) + "\n");
    std::string out, err;
    const int code = runCli({"namespace", "limits", "a", "--set", "max-topics=1000000", "--broker", b.endpoint(), "--user",
                             "nobody", "--password-file", pw}, &out, &err);
    EXPECT_NE(code, 0) << out;
    EXPECT_NE(err.find("NotAuthorized"), std::string::npos) << "a refusal reads as one, not as a missing namespace: " << err;
    admin->close();
}

// C8 fix round: `--set` on a namespace that does not exist reads as "no namespace" (rc 1), not as a bare broker code.
TEST(NamespaceClientRegression, LimitsCliSetOnAMissingNamespaceNamesItAdversarialRegression) {
    AuthBroker b("nslimits641missingcli");
    b.bootstrapAdmin(kAdminPw);
    fs::create_directories(scratchDir());
    const std::string pw = writeTemp("admin-pw641missing", std::string(kAdminPw) + "\n");
    std::string out, err;
    const int code = runCli({"namespace", "limits", "ghost", "--set", "max-topics=5", "--broker", b.endpoint(), "--user",
                             "admin", "--password-file", pw}, &out, &err);
    EXPECT_EQ(code, 1) << out << err;
    EXPECT_NE(err.find("no namespace 'ghost'"), std::string::npos) << err;
    EXPECT_EQ(out.find("updated limits"), std::string::npos) << out;
}
