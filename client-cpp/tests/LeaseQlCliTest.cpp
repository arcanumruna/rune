// tests/LeaseQlCliTest.cpp
//
// Lease topics Task 4 - the CLI half of QL, OFFLINE: the dry-run plans of `lease topic` and the three lease statements, the
// refusal at prepare of the statements until Task 22 executes them, and the lease view line of `show view`.
#include "QlEngine.h"

#include "rune/cli/Cli.h"
#include "rune/cli/CatalogCommands.h"

#include <rune/catalog/CatalogState.h>
#include <rune/ql/Parser.h>

#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>
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

std::string writeScript(const std::string& name, const std::string& text) {
    const auto path = std::filesystem::temp_directory_path() / ("lease_ql_cli_" + name);
    std::ofstream(path) << text;
    return path.string();
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

// The three lease statements are refused at prepare (until Task 22), so their plans are tested by building the Prepared
// directly from the parsed statement.
std::string planOf(const std::string& text) {
    auto st = ql::parse(text);
    EXPECT_TRUE(st.has_value()) << text << (st ? "" : " -> " + st.error().message);
    eng::Prepared p;
    if (const auto* d = st ? std::get_if<ql::ShowLeasesDef>(&*st) : nullptr) p = eng::PShowLeases{*d};
    else if (const auto* d1 = st ? std::get_if<ql::ShowLeaseDef>(&*st) : nullptr) p = eng::PShowLease{*d1};
    else if (const auto* d2 = st ? std::get_if<ql::RevokeLeaseDef>(&*st) : nullptr) p = eng::PRevokeLease{*d2};
    else ADD_FAILURE() << text << " is not a lease statement";
    std::ostringstream out;
    eng::printPlan(p, std::nullopt, out);
    return out.str();
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

TEST(LeaseQlCliRegression, CreateLeaseTopicCliRegression) {
    const std::string head =
        "topic inst partitions 4 replicas 3 persistence disk key id lease lease.ttl 30000 lease.min_ttl 1000 "
        "lease.max_ttl 9000000000000 lease.max_keys 1048576 lease.expiry_batch 256 lease.checkpoint on "
        "lease.checkpoint_interval 300000 lease.checkpoint_rate 1000\n";
    const std::string out = dryRun("create lease topic inst { id uint64, addr string(64) } partitions 4 replicas 3 key id;");
    EXPECT_EQ(out.substr(0, head.size()), head) << out;

    const std::string custom = dryRun(
        "create transient lease topic inst { id uint64 } key id with (lease.ttl = 5000, lease.min_ttl = 2000, lease.max_ttl = 60000, "
        "lease.max_keys = 10, lease.expiry_batch = 8, lease.checkpoint = off, lease.checkpoint_interval = 2000, lease.checkpoint_rate = 5);");
    const std::string want =
        "topic inst partitions 1 replicas 0 persistence transient key id lease lease.ttl 5000 lease.min_ttl 2000 "
        "lease.max_ttl 60000 lease.max_keys 10 lease.expiry_batch 8 lease.checkpoint off lease.checkpoint_interval 2000 "
        "lease.checkpoint_rate 5\n";
    EXPECT_EQ(custom.substr(0, want.size()), want) << custom;

    EXPECT_EQ(dryRun("create topic t { id uint64 } key id;").find("lease"), std::string::npos);

    std::string out2, err;
    EXPECT_EQ(runCli({"ql", "--dry-run", "-e", "create lease topic t;"}, &out2, &err), 1);
    EXPECT_NE(err.find("a lease topic needs a typed, keyed topic"), std::string::npos) << err;

    std::string out3, err3;
    EXPECT_EQ(runCli({"ql", "--dry-run", "-f", writeScript("create_topic.rql",
                      "create topic a { id uint64 } key id;\ncreate view v key id as select id from a;\n")}, &out3, &err3), 0) << err3;
}

TEST(LeaseQlCliRegression, LeaseStatementPlansRegression) {
    EXPECT_EQ(planOf("show leases inst;"), "show leases inst limit 100\n");
    EXPECT_EQ(planOf("show leases 'acme/claims' limit 5;"), "show leases acme/claims limit 5\n");
    EXPECT_EQ(planOf("show leases inst where path like 'herald/eu/_shared/%' limit 7;"),
              "show leases inst where path like 'herald/eu/_shared/%' limit 7\n");
    EXPECT_EQ(planOf("show leases inst where k like '%';"), "show leases inst where k like '%' limit 100\n");
    EXPECT_EQ(planOf("show lease 'acme/claims' key 7;"), "show lease acme/claims key 7\n");
    EXPECT_EQ(planOf("show lease inst key -3;"), "show lease inst key -3\n");
    EXPECT_EQ(planOf("show lease inst key 'eu-1';"), "show lease inst key 'eu-1'\n");
    EXPECT_EQ(planOf("revoke lease t key 7;"), "revoke lease t key 7\n");
    EXPECT_EQ(planOf("revoke lease t key 'x';"), "revoke lease t key 'x'\n");
    // They are never routed to the security code: `revoke read …` still is.
    EXPECT_EQ(dryRun("revoke read on topic t from user u;"), "REVOKE READ ON TOPIC t FROM USER u;\n");

    // A malformed key is refused before anything runs.
    std::string out, err;
    ASSERT_EQ(runCli({"ql", "--dry-run", "-e", "show lease t key"}, &out, &err), 1);
    EXPECT_NE(err.find("a lease key is an integer or a string"), std::string::npos) << err;
}

TEST(LeaseQlCliRegression, LeaseStatementsAreRefusedAtPrepareRegression) {
    struct Row { const char* statement; const char* message; };
    const Row rows[] = {
        {"show leases x;", "'show leases' is not available yet"},
        {"show lease x key 1;", "'show lease' is not available yet"},
        {"revoke lease x key 1;", "'revoke lease' is not available yet"},
    };
    for (const auto& row : rows) {
        // A script whose second statement is a lease statement stops at prepare: nothing is printed or issued, and no
        // broker is contacted (a dry run and a live run both prepare the whole script first).
        const std::string script = std::string("lease topic x { id uint64 } key id; ") + row.statement;
        std::string out, err;
        EXPECT_EQ(runCli({"ql", "--dry-run", "-f", writeScript("refused.rql", script)}, &out, &err), 1) << script;
        EXPECT_EQ(err, std::string("statement 2: ") + row.message + "\n") << script;
        EXPECT_TRUE(out.empty()) << script << " printed " << out;
    }
}

TEST(LeaseQlCliRegression, ScriptViewOverLeaseColumnsRegression) {
    std::string out, err;
    ASSERT_EQ(runCli({"ql", "--dry-run", "-f", writeScript("view.rql",
        "lease topic inst { id uint64, addr string(64) } key id;\n"
        "create view v key id as select id, lease_holder from inst;\n")}, &out, &err), 0) << err;
    EXPECT_NE(out.find("topic inst "), std::string::npos) << out;
    EXPECT_NE(out.find("create view v source inst key id"), std::string::npos) << out;
    EXPECT_NE(out.find("lease_holder"), std::string::npos) << out;
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
