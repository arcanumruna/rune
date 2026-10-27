// NamespaceClientTest.cpp - namespaces Task 11: the REAL client API (createNamespace / listNamespaces /
// setNamespaceLimits) and the REAL `rune namespace` / `rune ql` CLI against a REAL authenticating BrokerNode.
// Also the Task 8 rows 1, 6 and 8 (a namespace admin's security DDL) through QL text and the CLI.
#include "support/AuthBroker.h"

#include <rune/cli/Cli.h>
#include <rune/client/Client.h>
#include <rune/client/NamespaceLimitFields.h>

#include <gtest/gtest.h>

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <string>
#include <unistd.h>
#include <vector>

using namespace rune::node;
using namespace rune::node::test;
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
    static const fs::path d = [] {
        fs::path p = fs::temp_directory_path() / ("rune_ns_cli_test_" + std::to_string(::getpid()));
        fs::create_directories(p);
        return p;
    }();
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
