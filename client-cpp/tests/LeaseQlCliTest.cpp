// tests/LeaseQlCliTest.cpp
//
// Lease topics Task 4 - the CLI half of QL, OFFLINE: the dry-run plans of `lease topic` and the three lease statements, the
// local "not available yet" refusal of the statements until Task 22 executes them, and the lease view line of `show view`.
#include "QlEngine.h"

#include "rune/cli/Cli.h"
#include "rune/cli/CatalogCommands.h"

#include <rune/catalog/CatalogState.h>
#include <rune/ql/Parser.h>

#include <gtest/gtest.h>

#include <sstream>
#include <string>
#include <vector>

namespace ql  = rune::ql;
namespace eng = rune::cli::engine;
namespace cat = rune::catalog;

namespace {

int runCli(std::vector<std::string> args, std::string* out, std::string* err) {
    std::stringstream o, e;
    const int rc = rune::cli::run(std::move(args), o, e);
    *out = o.str();
    *err = e.str();
    return rc;
}

std::string dryRun(const std::string& statement) {
    std::string out, err;
    EXPECT_EQ(runCli({"ql", "--dry-run", "-e", statement}, &out, &err), 0) << statement << " -> " << err;
    return out;
}

eng::Prepared prepared(const std::string& text) {
    auto st = ql::parse(text);
    EXPECT_TRUE(st.has_value()) << text << (st ? "" : " -> " + st.error().message);
    std::string why;
    auto p = eng::prepare(*st, {}, why);
    EXPECT_TRUE(p.has_value()) << text << " -> " << why;
    return p ? std::move(*p) : eng::Prepared{};
}

}  // namespace

TEST(LeaseQlCliRegression, DefinePlanNamesTheLeaseRegression) {
    const std::string out = dryRun("lease topic inst { id uint64, addr string(64) } partitions 4 replicas 3 key id;");
    const std::string head =
        "topic inst partitions 4 replicas 3 persistence disk key id lease lease.ttl 30000 lease.min_ttl 1000 "
        "lease.max_ttl 9000000000000 lease.max_keys 1048576 lease.expiry_batch 256 lease.checkpoint on "
        "lease.checkpoint_interval 300000 lease.checkpoint_rate 1000\n";
    EXPECT_EQ(out.substr(0, head.size()), head) << out;

    const std::string custom = dryRun(
        "transient lease topic inst { id uint64 } key id with (lease.ttl = 5000, lease.min_ttl = 2000, lease.max_ttl = 60000, "
        "lease.max_keys = 10, lease.expiry_batch = 8, lease.checkpoint = off, lease.checkpoint_interval = 2000, lease.checkpoint_rate = 5);");
    const std::string want =
        "topic inst partitions 1 replicas 0 persistence transient key id lease lease.ttl 5000 lease.min_ttl 2000 "
        "lease.max_ttl 60000 lease.max_keys 10 lease.expiry_batch 8 lease.checkpoint off lease.checkpoint_interval 2000 "
        "lease.checkpoint_rate 5\n";
    EXPECT_EQ(custom.substr(0, want.size()), want) << custom;

    // A plain topic prints no lease words.
    EXPECT_EQ(dryRun("topic t { id uint64 } key id;").find("lease"), std::string::npos);

    // The statement is refused before anything is sent.
    std::string out2, err;
    EXPECT_EQ(runCli({"ql", "--dry-run", "-e", "lease topic t;"}, &out2, &err), 1);
    EXPECT_NE(err.find("a lease topic needs a typed, keyed topic"), std::string::npos) << err;
}

TEST(LeaseQlCliRegression, LeaseStatementPlansRegression) {
    EXPECT_EQ(dryRun("show leases inst;"), "show leases inst limit 100\n");
    EXPECT_EQ(dryRun("show leases 'acme/claims' limit 5;"), "show leases acme/claims limit 5\n");
    EXPECT_EQ(dryRun("show leases inst where path like 'herald/eu/_shared/%' limit 7;"),
              "show leases inst where path like 'herald/eu/_shared/%' limit 7\n");
    EXPECT_EQ(dryRun("show leases inst where k like '%';"), "show leases inst where k like '%' limit 100\n");
    EXPECT_EQ(dryRun("show lease 'acme/claims' key 7;"), "show lease acme/claims key 7\n");
    EXPECT_EQ(dryRun("show lease inst key -3;"), "show lease inst key -3\n");
    EXPECT_EQ(dryRun("show lease inst key 'eu-1';"), "show lease inst key 'eu-1'\n");
    EXPECT_EQ(dryRun("revoke lease t key 7;"), "revoke lease t key 7\n");
    EXPECT_EQ(dryRun("revoke lease t key 'x';"), "revoke lease t key 'x'\n");
    // They are never routed to the security code: `revoke read …` still is.
    EXPECT_EQ(dryRun("revoke read on topic t from user u;"), "REVOKE READ ON TOPIC t FROM USER u;\n");

    // A malformed key is refused before anything runs.
    std::string out, err;
    ASSERT_EQ(runCli({"ql", "--dry-run", "-e", "show lease t key"}, &out, &err), 1);
    EXPECT_NE(err.find("a lease key is an integer or a string"), std::string::npos) << err;
}

TEST(LeaseQlCliRegression, LeaseStatementsAreNotAvailableYetRegression) {
    struct Row { const char* text; const char* message; };
    const Row rows[] = {
        {"show leases t;", "rune: 'show leases' is not available yet\n"},
        {"show lease t key 1;", "rune: 'show lease' is not available yet\n"},
        {"revoke lease t key 1;", "rune: 'revoke lease' is not available yet\n"},
    };
    for (const auto& row : rows) {
        // An unopened broker proves nothing is sent: the statement fails locally, without touching it.
        eng::LiveBroker broker;
        std::stringstream out, err;
        eng::QlRun run{broker, out, err, false, 1};
        EXPECT_FALSE(run.exec(prepared(row.text), row.text, std::nullopt)) << row.text;
        EXPECT_EQ(err.str(), row.message) << row.text;
        EXPECT_TRUE(out.str().empty());
        EXPECT_EQ(run.lastErrorCode, 0u) << "a local refusal carries no broker code";
    }
    // In a script the failure names its statement.
    eng::LiveBroker broker;
    std::stringstream out, err;
    eng::QlRun run{broker, out, err, true, 3};
    EXPECT_FALSE(run.exec(prepared("revoke lease t key 1;"), "revoke lease t key 1;", std::nullopt));
    EXPECT_EQ(err.str(), "statement 3: 'revoke lease' is not available yet\n");
}

TEST(LeaseQlCliRegression, LeaseViewPrintsItsTopicRegression) {
    cat::ViewObject v;
    v.objectId = 9;
    v.name = "inst.leases";
    v.version = 1;
    v.kind = cat::ViewKind::LeaseIndex;
    v.leaseTopicObjectId = 4;
    const std::string text = rune::cli::formatViewDefinition(v);
    EXPECT_EQ(text, "view inst.leases (objectId 9, version 1)\n  lease view of topic inst\n");
    EXPECT_EQ(text.find("undecodable"), std::string::npos);

    // A plan view with an empty plan blob still reports it (the lease branch does not swallow ordinary views).
    cat::ViewObject plan;
    plan.name = "v";
    EXPECT_NE(rune::cli::formatViewDefinition(plan).find("(undecodable plan)"), std::string::npos);
}
