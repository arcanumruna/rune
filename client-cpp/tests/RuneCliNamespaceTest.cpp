// Namespaces Task 11 — `rune namespace create | list | limits`, OFFLINE: the limit flags, `--set k=v`
// read-modify-write, the list output and the usage errors.  Nothing here talks to a broker (the CLI against a real
// broker is the broker repo's NamespaceClientTest).
#include "rune/cli/Cli.h"
#include "rune/cli/NamespaceCommands.h"

#include <gtest/gtest.h>

#include <algorithm>
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
        // Phase 2 Task 16 (D29): suspend / resume / delete / usage.
        {{"namespace", "suspend"}, "a namespace <name> is required"},
        {{"namespace", "suspend", "a", "--broker", "h:1"}, "exactly one of --read-only or --reject-all"},
        {{"namespace", "suspend", "a", "--read-only", "--reject-all", "--broker", "h:1"}, "exactly one of --read-only or --reject-all"},
        {{"namespace", "suspend", "a", "--readonly", "--broker", "h:1"}, "unknown option '--readonly'"},
        {{"namespace", "resume"}, "a namespace <name> is required"},
        {{"namespace", "resume", "a", "--yes", "--broker", "h:1"}, "unknown option '--yes'"},
        {{"namespace", "delete"}, "a namespace <name> is required"},
        {{"namespace", "delete", "a", "--force", "--broker", "h:1"}, "unknown option '--force'"},
        {{"namespace", "usage"}, "a namespace <name> is required"},
        {{"namespace", "usage", "a", "--csv", "--broker", "h:1"}, "unknown option '--csv'"},
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
                 {"namespace", "create", "a"}, {"namespace", "list"}, {"namespace", "limits", "a"},
                 {"namespace", "suspend", "a", "--read-only"}, {"namespace", "resume", "a"}, {"namespace", "delete", "a", "--yes"},
                 {"namespace", "usage", "a"}}) {
            const auto r = runCli(a);
            EXPECT_EQ(r.code, 2) << r.err;
            EXPECT_NE(r.err.find("--broker host:port is required"), std::string::npos) << r.err;
        }
    }
    // The top-level usage lists the namespace commands.
    const auto top = runCli({"help"});
    EXPECT_NE((top.out + top.err).find("rune namespace list"), std::string::npos);
}

// Phase 2 Task 16: `delete` without --yes never reaches a broker unless a person confirmed on a terminal.  OFFLINE: every case
// ends before any connection (the broker endpoint is unreachable; a connect would hang or fail differently).
TEST(NamespaceCliRegression, DeletePromptOfflineRegression) {
    const std::vector<std::string> args = {"delete", "a", "--broker", "127.0.0.1:1"};
    auto del = [&](const std::string& input, bool tty) {
        std::istringstream in(input);
        std::ostringstream o, e;
        const int rc = rune::cli::cmdNamespace(args, in, tty, o, e);
        return CliRun{rc, o.str(), e.str()};
    };
    const auto noTty = del("y\n", false);
    EXPECT_EQ(noTty.code, 1);
    EXPECT_NE(noTty.err.find("refusing to delete namespace 'a' without --yes"), std::string::npos) << noTty.err;
    EXPECT_EQ(noTty.err.find("[y/N]"), std::string::npos) << "no prompt without a terminal";
    for (const char* no : {"n\n", "\n", "nope\n", "", "yess\n"}) {
        const auto r = del(no, true);
        EXPECT_EQ(r.code, 1) << "[" << no << "]";
        EXPECT_NE(r.err.find("Delete namespace 'a'"), std::string::npos) << r.err;
        EXPECT_NE(r.err.find("[y/N]"), std::string::npos) << r.err;
        EXPECT_NE(r.err.find("aborted"), std::string::npos) << r.err;
        EXPECT_TRUE(r.out.empty());
    }
    // The 3-argument form (what `run` calls) with a non-terminal stdin in a test process: refused.
    const auto viaRun = runCli({"namespace", "delete", "a", "--broker", "127.0.0.1:1"});
    EXPECT_EQ(viaRun.code, 1);
    EXPECT_NE(viaRun.err.find("without --yes"), std::string::npos) << viaRun.err;
}

TEST(NamespaceCliRegression, UsageFormatRegression) {
    rpc::NamespaceUsageResponse r;
    r.state = 1;
    r.limits.maxTopics = 5;
    r.limits.maxDiskBytes = 1000;
    r.usage[0] = 3;       // max-topics
    r.usage[2] = 7;       // max-partitions-total
    r.usage[8] = 400;     // max-disk-bytes
    r.usage[4] = 99;      // max-schemas: no usage meaning in this build -> printed as '-', whatever the wire carries
    const std::string t = rune::cli::formatNamespaceUsage("acme", r);
    EXPECT_EQ(t.rfind("namespace acme (suspended-read-only)\n", 0), 0u) << t;
    std::string squeezed;
    for (bool sp = false; const char c : t) { if (c == ' ') { if (!sp) squeezed += c; sp = true; } else { squeezed += c; sp = false; } }
    EXPECT_NE(squeezed.find("\nlimit value usage\n"), std::string::npos) << t;
    EXPECT_NE(squeezed.find("\nmax-topics 5 3\n"), std::string::npos) << t;
    EXPECT_NE(squeezed.find("\nmax-partitions-total 0 7\n"), std::string::npos) << t;
    EXPECT_NE(squeezed.find("\nmax-disk-bytes 1000 400\n"), std::string::npos) << t;
    EXPECT_NE(squeezed.find("\nmax-schemas 0 -\n"), std::string::npos) << t;
    EXPECT_EQ(std::count(t.begin(), t.end(), '\n'), 1 + 1 + 29) << "state line + header + one row per limit";
    const std::string j = rune::cli::formatNamespaceUsageJson("ac\"me", r);
    EXPECT_EQ(j.rfind("{\"name\":\"ac\\\"me\",\"state\":\"suspended-read-only\",\"limits\":{\"max-topics\":5,", 0), 0u) << j;
    EXPECT_NE(j.find("\"usage\":{\"max-topics\":3,"), std::string::npos) << j;
    EXPECT_EQ(j.find("max-schemas\":99"), std::string::npos) << "no usage for a field without a meaning: " << j;
    EXPECT_EQ(j.back(), '\n');
// ── Adversarial review of client batch C8 (#641): the --set patch over every field combination and malformed input ──

namespace {
void expectPatchExactly(const rcl::NamespaceLimitsPatch& p, const std::vector<std::pair<std::size_t, uint64_t>>& want,
                        const std::string& ctx) {
    std::vector<bool> named(rcl::kNamespaceLimitFields.size(), false);
    for (const auto& [i, v] : want) {
        named[i] = true;
        EXPECT_TRUE(p.has(i)) << ctx << " field " << rcl::kNamespaceLimitFields[i].name;
        EXPECT_EQ(rcl::namespaceLimitGet(p.values, i), v) << ctx << " field " << rcl::kNamespaceLimitFields[i].name;
    }
    for (std::size_t i = 0; i < named.size(); ++i)
        if (!named[i]) EXPECT_FALSE(p.has(i)) << ctx << ": unnamed field " << rcl::kNamespaceLimitFields[i].name << " is in the mask";
    // Bits 29..31 of the 4-byte mask name no field: never sent.
    EXPECT_EQ(p.present[3] & 0xE0u, 0u) << ctx;
}
} // namespace

TEST(NamespaceCliRegression, LimitSetsPatchEveryFieldCombinationAdversarialRegression) {
    const std::size_t n = rcl::kNamespaceLimitFields.size();
    const auto maxOf = [](std::size_t i) -> uint64_t {
        return i == rcl::kNamespaceFeatureBitsField ? uint64_t{UINT32_MAX} : UINT64_MAX;
    };
    // Each field alone at 0, 1 and its max; max+1 is refused for the u32 field.
    for (std::size_t i = 0; i < n; ++i) {
        const std::string name(rcl::kNamespaceLimitFields[i].name);
        for (const uint64_t v : {uint64_t{0}, uint64_t{1}, maxOf(i)}) {
            const auto p = rune::cli::namespaceLimitSetsPatch({name + "=" + std::to_string(v)});
            ASSERT_TRUE(p.has_value()) << name << "=" << v << ": " << p.error();
            expectPatchExactly(*p, {{i, v}}, name + "=" + std::to_string(v));
        }
        if (i == rcl::kNamespaceFeatureBitsField)
            EXPECT_FALSE(rune::cli::namespaceLimitSetsPatch({name + "=4294967296"}).has_value());
        EXPECT_FALSE(rune::cli::namespaceLimitSetsPatch({name + "=18446744073709551616"}).has_value()) << name;
    }
    // All 29 at once, in reverse order.
    std::vector<std::string> all;
    std::vector<std::pair<std::size_t, uint64_t>> want;
    for (std::size_t i = n; i-- > 0;) {
        all.push_back(std::string(rcl::kNamespaceLimitFields[i].name) + "=" + std::to_string(500 + i));
        want.emplace_back(i, 500 + i);
    }
    const auto full = rune::cli::namespaceLimitSetsPatch(all);
    ASSERT_TRUE(full.has_value()) << full.error();
    expectPatchExactly(*full, want, "all");
    EXPECT_EQ(full->present, rcl::kNamespaceFullMask);
    // Seeded random subsets (every pair of fields is covered many times over 400 draws).
    uint64_t seed = 0x9E3779B97F4A7C15ull;
    const auto rnd = [&] { seed ^= seed << 13; seed ^= seed >> 7; seed ^= seed << 17; return seed; };
    for (int k = 0; k < 400; ++k) {
        std::vector<std::string> sets;
        std::vector<std::pair<std::size_t, uint64_t>> w;
        const uint64_t mask = rnd();
        for (std::size_t i = 0; i < n; ++i) {
            if (((mask >> i) & 1u) == 0) continue;
            const uint64_t v = rnd() % (maxOf(i) == UINT64_MAX ? UINT64_MAX : maxOf(i) + 1);
            sets.push_back(std::string(rcl::kNamespaceLimitFields[i].name) + "=" + std::to_string(v));
            w.emplace_back(i, v);
        }
        const auto p = rune::cli::namespaceLimitSetsPatch(sets);
        ASSERT_TRUE(p.has_value()) << "seed draw " << k << ": " << p.error();
        expectPatchExactly(*p, w, "draw " + std::to_string(k));
    }
    // An empty list is an empty patch (cmdLimits then only prints; it never sends one).
    const auto none = rune::cli::namespaceLimitSetsPatch({});
    ASSERT_TRUE(none.has_value());
    expectPatchExactly(*none, {}, "empty");
}

TEST(NamespaceCliRegression, LimitSetsPatchRefusesMalformedAdversarialRegression) {
    for (const char* bad : {"", "=", "k=", "=v", "max-topics=1=2", "max-topics==1", "max-topics=-1", "max-topics=+1",
                            "max-topics= 1", "max-topics=1 ", " max-topics=1", "MAX-TOPICS=1", "max_topics=1",
                            "max-topics=0x10", "max-topics=1.0", "--max-topics=1", "feature-bits=-0"}) {
        const auto p = rune::cli::namespaceLimitSetsPatch({bad});
        EXPECT_FALSE(p.has_value()) << "'" << bad << "' was accepted";
        // A bad entry after good ones refuses the whole list: nothing partial is ever sent.
        EXPECT_FALSE(rune::cli::namespaceLimitSetsPatch({"max-users=3", bad}).has_value()) << bad;
    }
    EXPECT_FALSE(rune::cli::namespaceLimitSetsPatch({"max-users=3", "max-topics=1", "max-users=3"}).has_value())
        << "a repeat is refused even with the same value";
    // The CLI refuses a malformed --set before any broker contact (usage, rc 2), even with no --broker at all.
    for (const char* bad : {"max-topics", "nope=1", "max-topics=x"}) {
        const auto r = runCli({"namespace", "limits", "ns1", "--set", bad});
        EXPECT_EQ(r.code, 2) << bad << ": " << r.err;
        const std::string first = r.err.substr(0, r.err.find('\n'));
        EXPECT_NE(first.find(std::string("--set '") + bad + "'"), std::string::npos) << first;
        EXPECT_EQ(first.find("--broker"), std::string::npos) << "the bad --set is named, not the missing broker: " << first;
    }
    const auto dangling = runCli({"namespace", "limits", "ns1", "--broker", "h:1", "--set"});
    EXPECT_EQ(dangling.code, 2) << dangling.err;
}
