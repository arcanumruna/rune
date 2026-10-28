// Namespaces Task 11 — `rune namespace create | list | limits`, OFFLINE: the limit flags, `--set k=v`
// read-modify-write, the list output and the usage errors.  Nothing here talks to a broker (the CLI against a real
// broker is the broker repo's NamespaceClientTest).
#include "rune/cli/Cli.h"
#include "rune/cli/NamespaceCommands.h"

#include <gtest/gtest.h>

#include <set>
#include <sstream>
#include <string>
#include <vector>

namespace rcl = rune::client;
namespace rp  = rune::protocol;
namespace rpc = rune::protocol::client;

namespace {
struct CliRun { int code; std::string out, err; };
CliRun runCli(std::vector<std::string> args) {
    std::ostringstream out, err;
    const int code = rune::cli::run(std::move(args), out, err);
    return {code, out.str(), err.str()};
}
} // namespace

// Every one of the 29 mask fields has a distinct flag, `--<name> N` sets exactly that field and its mask bit.
TEST(NamespaceCliRegression, FlagsCoverEveryLimitRegression) {
    ASSERT_EQ(rcl::kNamespaceLimitFields.size(), rp::kNamespaceLimitFieldCount);
    ASSERT_EQ(rcl::kNamespaceLimitFields.size(), 29u);
    std::set<std::string_view> names;
    for (const auto& f : rcl::kNamespaceLimitFields) EXPECT_TRUE(names.insert(f.name).second) << f.name;
    // Distinct member pointers: no two flags write the same field.
    for (std::size_t i = 0; i < rcl::kNamespaceLimitFields.size(); ++i) {
        std::vector<std::string> args = {"--" + std::string(rcl::kNamespaceLimitFields[i].name),
                                         std::to_string(1000 + i), "--broker", "x:1"};
        std::vector<std::string> rest;
        const auto p = rune::cli::parseNamespaceLimitFlags(args, rest);
        ASSERT_TRUE(p.has_value()) << rcl::kNamespaceLimitFields[i].name << ": " << p.error();
        EXPECT_EQ(rest, (std::vector<std::string>{"--broker", "x:1"}));
        for (std::size_t j = 0; j < rcl::kNamespaceLimitFields.size(); ++j) {
            EXPECT_EQ(p->has(j), i == j) << i << "/" << j;
            EXPECT_EQ(rcl::namespaceLimitGet(p->values, j), i == j ? 1000 + i : 0u) << i << "/" << j;
        }
    }
    // All 29 at once: the full mask.
    std::vector<std::string> all, rest;
    for (std::size_t i = 0; i < rcl::kNamespaceLimitFields.size(); ++i) {
        all.push_back("--" + std::string(rcl::kNamespaceLimitFields[i].name));
        all.push_back(std::to_string(i + 1));
    }
    const auto p = rune::cli::parseNamespaceLimitFlags(all, rest);
    ASSERT_TRUE(p.has_value());
    EXPECT_TRUE(rest.empty());
    EXPECT_EQ(p->present, rcl::kNamespaceFullMask);
    EXPECT_EQ(p->values.maxTopics, 1u);
    EXPECT_EQ(p->values.lowestMinInsync, 28u);
    EXPECT_EQ(p->values.featureBits, 29u);
    // Refusals: a missing / non-numeric / negative / over-u32 feature-bits value, the same flag twice.
    for (const std::vector<std::string>& bad : std::vector<std::vector<std::string>>{
             {"--max-topics"}, {"--max-topics", "x"}, {"--max-topics", "-1"}, {"--max-topics", "1", "--max-topics", "2"},
             {"--feature-bits", "4294967296"}, {"--max-views", "18446744073709551616"}}) {
        std::vector<std::string> r;
        EXPECT_FALSE(rune::cli::parseNamespaceLimitFlags(bad, r).has_value()) << bad[0];
    }
    // The usage lists every flag.
    std::ostringstream u;
    rune::cli::printNamespaceUsage(u);
    for (const auto& f : rcl::kNamespaceLimitFields) EXPECT_NE(u.str().find(f.name), std::string::npos) << f.name;
}

// Issue #641: the patch built from `--set` flags names exactly the given fields (the wire mask), so the write
// never carries a stale copy of any other limit.
TEST(NamespaceCliRegression, LimitSetsBuildAPatchOfOnlyTheNamedFieldsRegression) {
    const auto patch = rune::cli::namespaceLimitSetsPatch({"max-topics=7", "max-users=0"});
    ASSERT_TRUE(patch.has_value()) << patch.error();
    for (std::size_t i = 0; i < rcl::kNamespaceLimitFields.size(); ++i) {
        const std::string_view name = rcl::kNamespaceLimitFields[i].name;
        const bool named = name == "max-topics" || name == "max-users";
        EXPECT_EQ(patch->has(i), named) << name;
        EXPECT_EQ(rcl::namespaceLimitGet(patch->values, i), name == "max-topics" ? 7u : 0u) << name;
    }
    EXPECT_FALSE(rune::cli::namespaceLimitSetsPatch({"max-topics=1", "max-topics=2"}).has_value());
    EXPECT_FALSE(rune::cli::namespaceLimitSetsPatch({"nope=1"}).has_value());
}

// `limits <ns> --set k=v`: only the named fields change, every other keeps the value read from the broker.
TEST(NamespaceCliRegression, LimitsSetReadModifyWriteRegression) {
    rp::NamespaceLimits base{};
    for (std::size_t i = 0; i < rcl::kNamespaceLimitFields.size(); ++i)
        ASSERT_TRUE(rcl::namespaceLimitSet(base, i, 100 + i));
    const auto r = rune::cli::applyNamespaceLimitSets(base, {"max-topics=7", "feature-bits=3", "max-users=0"});
    ASSERT_TRUE(r.has_value()) << r.error();
    for (std::size_t i = 0; i < rcl::kNamespaceLimitFields.size(); ++i) {
        const auto name = rcl::kNamespaceLimitFields[i].name;
        const uint64_t want = name == "max-topics" ? 7 : name == "feature-bits" ? 3 : name == "max-users" ? 0 : 100 + i;
        EXPECT_EQ(rcl::namespaceLimitGet(*r, i), want) << name;
    }
    EXPECT_EQ(rune::cli::applyNamespaceLimitSets(base, {}).value(), base);
    for (const char* bad : {"max-topics", "=1", "max-topics=", "nope=1", "max-topics=x", "feature-bits=4294967296"})
        EXPECT_FALSE(rune::cli::applyNamespaceLimitSets(base, {bad}).has_value()) << bad;
    EXPECT_FALSE(rune::cli::applyNamespaceLimitSets(base, {"max-topics=1", "max-topics=2"}).has_value());

    // The printed limits and the list output carry the values.
    const std::string lim = rune::cli::formatNamespaceLimits(*r);
    EXPECT_NE(lim.find("max-topics"), std::string::npos);
    EXPECT_EQ(std::count(lim.begin(), lim.end(), '\n'), 29);
    rpc::NamespaceInfo a{1, "a", 0, *r}, b{2, "b\"q", 2, base};
    const std::string table = rune::cli::formatNamespacesTable({a, b});
    EXPECT_EQ(table.substr(0, table.find('\n')), "nsId  name  state");
    EXPECT_NE(table.find("1     a     active\n"), std::string::npos) << table;
    EXPECT_NE(table.find("suspended-reject-all"), std::string::npos);
    const std::string json = rune::cli::formatNamespacesJson({a, b});
    EXPECT_EQ(json.rfind("[{\"nsId\":1,\"name\":\"a\",\"state\":\"active\",\"limits\":{\"max-topics\":7,", 0), 0u) << json;
    EXPECT_NE(json.find("\"name\":\"b\\\"q\""), std::string::npos) << json;
    EXPECT_NE(json.find("\"feature-bits\":3}"), std::string::npos) << json;
    EXPECT_EQ(json.back(), '\n');
}

TEST(NamespaceCliRegression, UsageRegression) {
    struct Case { std::vector<std::string> args; const char* contains; } cases[] = {
        {{"namespace"}, "a subcommand is required"},
        {{"namespace", "frob"}, "unknown subcommand 'frob'"},
        {{"namespace", "create"}, "a namespace <name> is required"},
        {{"namespace", "create", "--max-topics", "1"}, "a namespace <name> is required"},
        {{"namespace", "create", "a", "--max-topics", "x", "--broker", "h:1"}, "--max-topics"},
        {{"namespace", "create", "a", "--max-topic", "1", "--broker", "h:1"}, "unknown option '--max-topic'"},
        {{"namespace", "create", "a", "extra", "--broker", "h:1"}, "unexpected argument 'extra'"},
        {{"namespace", "list", "--frob"}, "unknown option '--frob'"},
        {{"namespace", "limits"}, "a namespace <name> is required"},
        {{"namespace", "limits", "a", "--set"}, "--set needs"},
        {{"namespace", "limits", "a", "--set", "nope=1", "--broker", "h:1"}, "unknown limit 'nope'"},
        {{"namespace", "limits", "a", "--set", "max-topics", "--broker", "h:1"}, "expected <limit>=N"},
    };
    for (const auto& c : cases) {
        const auto r = runCli(c.args);
        EXPECT_EQ(r.code, 2) << c.args.back() << ": " << r.err;
        EXPECT_NE(r.err.find(c.contains), std::string::npos) << c.args.back() << " -> " << r.err;
        EXPECT_NE(r.err.find("rune namespace create <name>"), std::string::npos) << r.err;   // the usage follows
    }
    // No broker named (and none in RUNE_BROKER): a usage error before anything connects.
    if (std::getenv("RUNE_BROKER") == nullptr) {
        for (std::vector<std::string> a : std::vector<std::vector<std::string>>{
                 {"namespace", "create", "a"}, {"namespace", "list"}, {"namespace", "limits", "a"}}) {
            const auto r = runCli(a);
            EXPECT_EQ(r.code, 2) << r.err;
            EXPECT_NE(r.err.find("--broker host:port is required"), std::string::npos) << r.err;
        }
    }
    // The top-level usage lists the namespace commands.
    const auto top = runCli({"help"});
    EXPECT_NE((top.out + top.err).find("rune namespace list"), std::string::npos);
}
