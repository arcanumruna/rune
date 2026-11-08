// Lease topics Task 4: `lease topic` DDL, the lease.* settings, `show leases` / `show lease` / `revoke lease`
// (parse and plan only - nothing executes here).
#include "ConfigKeys.h"

#include <rune/protocol/LeaseSpec.h>
#include <rune/ql/Backends.h>
#include <rune/ql/Parser.h>

#include <gtest/gtest.h>

#include <limits>
#include <optional>
#include <string>
#include <string_view>
#include <variant>
#include <utility>
#include <vector>

namespace ql = rune::ql;
namespace rp = rune::protocol;

namespace {

ql::TopicDef topicOf(const std::string& text) {
    auto r = ql::parse(text);
    EXPECT_TRUE(r.has_value()) << text << (r ? "" : " -> " + r.error().message);
    const auto* t = r ? std::get_if<ql::TopicDef>(&*r) : nullptr;
    EXPECT_NE(t, nullptr) << text;
    return t ? *t : ql::TopicDef{};
}

std::string refusal(const std::string& text) {
    auto r = ql::parse(text);
    return r ? std::string("<accepted>") : r.error().message;
}

template <class T>
T parseAs(const std::string& text) {
    auto r = ql::parse(text);
    EXPECT_TRUE(r.has_value()) << text << (r ? "" : " -> " + r.error().message);
    const T* t = r ? std::get_if<T>(&*r) : nullptr;
    EXPECT_NE(t, nullptr) << text;
    return t ? *t : T{};
}

const std::string kBase = "lease topic inst { id uint64, addr string(64) } partitions 4 replicas 3 key id";
std::string withClause(const std::string& settings) { return kBase + " with (" + settings + ");"; }

}  // namespace

TEST(LeaseQlRegression, ParsesLeaseTopicRegression) {
    const auto td = topicOf(kBase + ";");
    EXPECT_TRUE(td.lease);
    EXPECT_EQ(td.name, "inst");
    EXPECT_EQ(td.partitions, 4u);
    EXPECT_EQ(td.replicas, 3);
    EXPECT_EQ(td.keyField, "id");
    ASSERT_EQ(td.fields.size(), 2u);
    EXPECT_EQ(td.persistence, ql::Persistence::Disk);

    const auto plan = ql::applyPlan(td);
    ASSERT_TRUE(plan.lease.has_value());
    EXPECT_EQ(*plan.lease, rp::LeaseSpec{});
    EXPECT_EQ(plan.config.cleanupPolicy, 1);
    EXPECT_EQ(plan.keyFeatures, rp::TopicKeyFeatures{});

    const auto all = ql::applyPlan(topicOf(withClause(
        "lease.ttl = 5000, lease.min_ttl = 2000, lease.max_ttl = 60000, lease.max_keys = 10, lease.expiry_batch = 8, "
        "lease.checkpoint = off, lease.checkpoint_interval = 2000, lease.checkpoint_rate = 5")));
    ASSERT_TRUE(all.lease.has_value());
    EXPECT_EQ(all.lease->ttlMs, 5000u);
    EXPECT_EQ(all.lease->minTtlMs, 2000u);
    EXPECT_EQ(all.lease->maxTtlMs, 60000u);
    EXPECT_EQ(all.lease->maxKeys, 10u);
    EXPECT_EQ(all.lease->expiryBatch, 8u);
    EXPECT_EQ(all.lease->checkpoint, 0u);
    EXPECT_EQ(all.lease->checkpointIntervalMs, 2000u);
    EXPECT_EQ(all.lease->checkpointRate, 5u);

    const auto on = ql::applyPlan(topicOf(withClause("lease.checkpoint = on")));
    EXPECT_EQ(on.lease->checkpoint, 1u);

    // Restating the compaction the topic already has is accepted, as is key.index = off.
    const auto restated = ql::applyPlan(topicOf(withClause("cleanup.policy = compact, retention.ms = -1, retention.bytes = -1, key.index = off")));
    EXPECT_EQ(restated.config.cleanupPolicy, 1);
    EXPECT_EQ(restated.config.retentionMs, -1);
    EXPECT_FALSE(restated.keyFeatures.keyIndex);

    const auto transient = topicOf("transient lease topic inst { id uint64 } key id;");
    EXPECT_TRUE(transient.lease);
    EXPECT_EQ(transient.persistence, ql::Persistence::Transient);

    for (const char* text : {"LEASE TOPIC inst { id uint64 } key id;", "Lease Topic inst { id uint64 } key id;",
                             "TRANSIENT LEASE TOPIC inst { id uint64 } key id;"})
        EXPECT_TRUE(topicOf(text).lease) << text;

    // A plain topic carries no lease and plans none.
    const auto plain = topicOf("topic t { id uint64 } key id;");
    EXPECT_FALSE(plain.lease);
    EXPECT_FALSE(ql::applyPlan(plain).lease.has_value());
}

TEST(LeaseQlRegression, LeaseTopicRefusalRegression) {
    struct Row { std::string text, message; };
    const std::vector<Row> rows = {
        {"lease topic t;", "a lease topic needs a typed, keyed topic"},
        {"lease topic t partitions 2;", "a lease topic needs a typed, keyed topic"},
        {"lease topic t { id uint64 };", "a lease topic needs a typed, keyed topic"},
        {"lease topic t { id uint64 } partitions 2;", "a lease topic needs a typed, keyed topic"},
        {"transient lease topic t { id uint64 };", "a lease topic needs a typed, keyed topic"},
        {"lease topic t { id uint64 autoincrement } key id;", "a lease topic cannot use autoincrement"},
        {"lease topic t { id uint64, lease_x int32 } key id;", "lease_ field names are reserved"},
        {"lease topic t { lease_owner uint64 } key lease_owner;", "lease_ field names are reserved"},
        {"lease topic t { id uint64, name string } key id;", "a lease topic's fields are fixed-width: 'name' is string; use string(N)"},
        {"lease topic t { id uint64, blob bytes } key id;", "a lease topic's fields are fixed-width: 'blob' is bytes; use string(N)"},
        {"lease topic t { id uint64, a string(8), b string } key id;", "a lease topic's fields are fixed-width: 'b' is string; use string(N)"},
        {withClause("key.index = on"), "a lease topic's key index is its lease view"},
        {withClause("autoincrement.reserve = 10"), "autoincrement.max and autoincrement.reserve need autoincrement"},
        {withClause("autoincrement.max = 10"), "autoincrement.max and autoincrement.reserve need autoincrement"},
        {withClause("cleanup.policy = delete"), "a lease topic is compacted and keeps every live lease"},
        {withClause("cleanup.policy = compact_delete"), "a lease topic is compacted and keeps every live lease"},
        {withClause("retention.ms = 1000"), "a lease topic is compacted and keeps every live lease"},
        {withClause("retention.bytes = 1000"), "a lease topic is compacted and keeps every live lease"},
        {"topic t { id uint64 } key id with (lease.ttl = 5000);", "lease.* settings need a lease topic"},
        {"topic t with (lease.checkpoint = off);", "lease.* settings need a lease topic"},
        {"topic t { id uint64 } key id with (retention.ms = 5, lease.max_keys = 5);", "lease.* settings need a lease topic"},
        {"lease view v;", "expected 'topic' after 'lease', got 'view'"},
        {"lease;", "expected 'topic' after 'lease', got ';'"},
        {"transient lease view v;", "expected 'topic' after 'transient lease', got 'view'"},
    };
    for (const auto& row : rows) EXPECT_EQ(refusal(row.text), row.message) << row.text;

    // The error is positioned at the offending token where the parser has it, else the statement start.
    const auto colOf = [](const std::string& text, const std::string& token) { return text.find(token) + 1; };
    const std::string a = "lease topic t { id uint64 autoincrement, other int32 } key id;";
    auto r = ql::parse(a);
    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error().line, 1u);
    EXPECT_EQ(r.error().col, colOf(a, "id"));
    auto s = ql::parse("lease topic t;");
    ASSERT_FALSE(s.has_value());
    EXPECT_EQ(s.error().col, 1u);
    const std::string v = "lease topic t { id uint64, name string } key id;";
    auto vr = ql::parse(v);
    ASSERT_FALSE(vr.has_value());
    EXPECT_EQ(vr.error().col, colOf(v, "name"));
    EXPECT_EQ(refusal("topic t { id uint64, name string } key id;"), "<accepted>");   // a plain topic keeps string
    EXPECT_EQ(refusal("lease topic t { id uint64, name string(32) } key id;"), "<accepted>");
    const std::string w = "lease topic t { id uint64 } key id with (retention.ms = 1);";
    auto wr = ql::parse(w);
    ASSERT_FALSE(wr.has_value());
    EXPECT_EQ(wr.error().col, colOf(w, "retention.ms"));
    const std::string t = "transient lease topic t;";
    auto tr = ql::parse(t);
    ASSERT_FALSE(tr.has_value());
    EXPECT_EQ(tr.error().col, 1u);
}

TEST(LeaseQlRegression, ExpectedListNamesLeaseRegression) {
    const std::string m = refusal("foo;");
    EXPECT_NE(m.find("'lease'"), std::string::npos) << m;
    const std::string sm = refusal("show foo;");
    EXPECT_NE(sm.find("'leases'"), std::string::npos) << sm;
    EXPECT_NE(sm.find("'lease'"), std::string::npos) << sm;
}

TEST(LeaseQlRegression, LeaseSettingRangeRegression) {
    using namespace rp;
    struct Row { std::string setting; std::string_view message; };   // message empty = accepted
    // Expected texts follow validateLeaseSpec's order over the defaults (ttl 30000, min 1000, max 9e12): min, max, ttl, ...
    const std::vector<Row> rows = {
        {"lease.ttl = 1000", {}}, {"lease.ttl = 30000", {}}, {"lease.ttl = 9000000000000", {}},
        {"lease.ttl = 999", kLeaseTtlRangeText}, {"lease.ttl = 0", kLeaseTtlRangeText}, {"lease.ttl = -1", kLeaseTtlRangeText},
        {"lease.ttl = 9000000000001", kLeaseTtlRangeText},
        {"lease.min_ttl = 1000", {}}, {"lease.min_ttl = 30000", {}},
        {"lease.min_ttl = 999", kLeaseMinTtlRangeText}, {"lease.min_ttl = -1", kLeaseMinTtlRangeText},
        {"lease.min_ttl = 30001", kLeaseTtlRangeText},                // ttl 30000 now below the floor
        {"lease.min_ttl = 9000000000000", kLeaseTtlRangeText},
        {"lease.max_ttl = 30000", {}}, {"lease.max_ttl = 9000000000000", {}},
        {"lease.max_ttl = 9000000000001", kLeaseMaxTtlRangeText}, {"lease.max_ttl = -1", kLeaseMaxTtlRangeText},
        {"lease.max_ttl = 29999", kLeaseTtlRangeText},                 // below the default ttl
        {"lease.max_ttl = 999", kLeaseMinTtlRangeText},                // below the default min_ttl
        {"lease.max_keys = 1", {}}, {"lease.max_keys = 16777216", {}},
        {"lease.max_keys = 0", kLeaseMaxKeysRangeText}, {"lease.max_keys = 16777217", kLeaseMaxKeysRangeText},
        {"lease.max_keys = 5000000000", kLeaseMaxKeysRangeText}, {"lease.max_keys = 4294967297", kLeaseMaxKeysRangeText},
        {"lease.max_keys = -1", kLeaseMaxKeysRangeText},
        {"lease.expiry_batch = 1", {}}, {"lease.expiry_batch = 4096", {}},
        {"lease.expiry_batch = 0", kLeaseExpiryBatchRangeText}, {"lease.expiry_batch = 4097", kLeaseExpiryBatchRangeText},
        {"lease.expiry_batch = 70000", kLeaseExpiryBatchRangeText}, {"lease.expiry_batch = 65537", kLeaseExpiryBatchRangeText},
        {"lease.expiry_batch = -1", kLeaseExpiryBatchRangeText},
        {"lease.checkpoint_interval = 1000", {}}, {"lease.checkpoint_interval = 86400000", {}},
        {"lease.checkpoint_interval = 999", kLeaseCheckpointIntervalRangeText},
        {"lease.checkpoint_interval = 86400001", kLeaseCheckpointIntervalRangeText},
        {"lease.checkpoint_interval = 4294968296", kLeaseCheckpointIntervalRangeText},   // 2^32 + 1000 wraps to 1000
        {"lease.checkpoint_interval = -1", kLeaseCheckpointIntervalRangeText},
        {"lease.checkpoint_rate = 1", {}}, {"lease.checkpoint_rate = 100000", {}},
        {"lease.checkpoint_rate = 0", kLeaseCheckpointRateRangeText}, {"lease.checkpoint_rate = 100001", kLeaseCheckpointRateRangeText},
        {"lease.checkpoint_rate = 4294967297", kLeaseCheckpointRateRangeText}, {"lease.checkpoint_rate = -1", kLeaseCheckpointRateRangeText},
        {"lease.checkpoint = on", {}}, {"lease.checkpoint = off", {}},
        // cross rules, first refusal in validateLeaseSpec's order
        {"lease.ttl = 500, lease.min_ttl = 2000", kLeaseTtlRangeText},
        {"lease.ttl = 5000, lease.min_ttl = 5000, lease.max_ttl = 5000", {}},
        {"lease.min_ttl = 5000, lease.max_ttl = 4000", kLeaseMinTtlRangeText},
        {"lease.min_ttl = 2000, lease.max_ttl = 3000, lease.ttl = 4000", kLeaseTtlRangeText},
        {"lease.min_ttl = 2000, lease.max_ttl = 9000000000001", kLeaseMaxTtlRangeText},
    };
    for (const auto& row : rows) {
        const std::string text = withClause(row.setting);
        if (row.message.empty()) {
            auto r = ql::parse(text);
            EXPECT_TRUE(r.has_value()) << text << (r ? "" : " -> " + r.error().message);
        } else {
            EXPECT_EQ(refusal(text), std::string(row.message)) << text;
        }
    }
    // The same texts on `transient lease topic`.
    EXPECT_EQ(refusal("transient lease topic t { id uint64 } key id with (lease.ttl = 1);"), std::string(kLeaseTtlRangeText));
}

TEST(LeaseQlRegression, ApplyLeaseSettingBoundsRegression) {
    using F = ql::detail::Field;
    using ql::detail::applyLeaseSetting;
    rp::LeaseSpec s{};
    // A checkpoint byte above 1 is unreachable from the parser (OnOff gives 0/1) but the helper still refuses it.
    EXPECT_EQ(applyLeaseSetting(s, F::LeaseCheckpoint, 256), std::optional<std::string_view>(rp::kLeaseCheckpointText));
    EXPECT_EQ(applyLeaseSetting(s, F::LeaseCheckpoint, -1), std::optional<std::string_view>(rp::kLeaseCheckpointText));
    EXPECT_EQ(s.checkpoint, 1u);   // refused values are not stored
    EXPECT_EQ(applyLeaseSetting(s, F::LeaseExpiryBatch, 65536), std::optional<std::string_view>(rp::kLeaseExpiryBatchRangeText));
    EXPECT_EQ(s.expiryBatch, 256u);
    EXPECT_EQ(applyLeaseSetting(s, F::LeaseExpiryBatch, 65535), std::nullopt);
    EXPECT_EQ(s.expiryBatch, 65535u);
    EXPECT_EQ(applyLeaseSetting(s, F::LeaseMaxKeys, 4294967296), std::optional<std::string_view>(rp::kLeaseMaxKeysRangeText));
    EXPECT_EQ(applyLeaseSetting(s, F::LeaseMaxKeys, 4294967295), std::nullopt);
    EXPECT_EQ(applyLeaseSetting(s, F::LeaseTtl, std::numeric_limits<int64_t>::max()), std::nullopt);
    EXPECT_EQ(s.ttlMs, static_cast<uint64_t>(std::numeric_limits<int64_t>::max()));
    EXPECT_EQ(applyLeaseSetting(s, F::LeaseTtl, std::numeric_limits<int64_t>::min()), std::optional<std::string_view>(rp::kLeaseTtlRangeText));
    EXPECT_FALSE(ql::detail::isLeaseField(F::KeyIndex));
    EXPECT_FALSE(ql::detail::isLeaseField(F::AutoIncrementMax));
    EXPECT_TRUE(ql::detail::isLeaseField(F::LeaseTtl));
    EXPECT_TRUE(ql::detail::isLeaseField(F::LeaseCheckpointRate));
    // The protocol texts are byte-identical to what validateLeaseSpec returned before they were named.
    EXPECT_EQ(rp::validateLeaseSpec(rp::LeaseSpec{.checkpoint = 2}), std::optional<std::string_view>("lease.checkpoint must be 0 or 1"));
    EXPECT_EQ(std::string(rp::kLeaseExpiryBatchRangeText), "lease.expiry_batch must be between 1 and 4096");
}

TEST(LeaseQlRegression, AlterLeaseSettingRegression) {
    using Changes = std::vector<std::pair<uint8_t, int64_t>>;
    const auto plan = ql::applyConfigPlan(parseAs<ql::AlterTopicConfigDef>("alter topic t set lease.ttl = 5000, lease.checkpoint = off;"));
    ASSERT_TRUE(plan.has_value());
    EXPECT_EQ(plan->changes, (Changes{{7, 5000}, {10, 0}}));
    const auto more = ql::applyConfigPlan(parseAs<ql::AlterTopicConfigDef>(
        "alter topic t set lease.min_ttl = 2000, lease.max_ttl = 60000, lease.checkpoint = on, lease.checkpoint_interval = 4000, lease.checkpoint_rate = 9;"));
    ASSERT_TRUE(more.has_value());
    EXPECT_EQ(more->changes, (Changes{{8, 2000}, {9, 60000}, {10, 1}, {11, 4000}, {12, 9}}));
    for (const char* key : {"lease.max_keys", "lease.expiry_batch"}) {
        const std::string text = std::string("alter topic t set ") + key + " = 5;";
        EXPECT_EQ(refusal(text), std::string("unknown or immutable topic setting '") + key + "'") << text;
    }
}

TEST(LeaseQlRegression, ShowLeasesRegression) {
    const auto plain = parseAs<ql::ShowLeasesDef>("show leases inst;");
    EXPECT_EQ(plain.topic, "inst");
    EXPECT_EQ(plain.keyColumn, "");
    EXPECT_FALSE(plain.prefix.has_value());
    EXPECT_EQ(plain.limit, 100u);

    EXPECT_EQ(parseAs<ql::ShowLeasesDef>("show leases 'acme/claims';").topic, "acme/claims");
    EXPECT_EQ(parseAs<ql::ShowLeasesDef>("SHOW LEASES inst LIMIT 5;").limit, 5u);
    EXPECT_EQ(parseAs<ql::ShowLeasesDef>("show leases inst limit 4294967295;").limit, 4294967295u);

    const auto path = parseAs<ql::ShowLeasesDef>("show leases inst where path like 'herald/eu/_shared/COORDINATOR_SERVICE/%' limit 7;");
    EXPECT_EQ(path.keyColumn, "path");
    ASSERT_TRUE(path.prefix.has_value());
    EXPECT_EQ(*path.prefix, "herald/eu/_shared/COORDINATOR_SERVICE/");   // '_' is literal
    EXPECT_EQ(path.limit, 7u);

    const auto all = parseAs<ql::ShowLeasesDef>("show leases inst where k like '%';");
    ASSERT_TRUE(all.prefix.has_value());
    EXPECT_EQ(*all.prefix, "");
    EXPECT_EQ(parseAs<ql::ShowLeasesDef>("show leases inst where k LIKE 'a%';").prefix, std::optional<std::string>("a"));
    EXPECT_EQ(parseAs<ql::ShowLeasesDef>("show leases inst where k like 'a_b%';").prefix, std::optional<std::string>("a_b"));
    EXPECT_EQ(*parseAs<ql::ShowLeasesDef>("show leases inst where k like '" + std::string(127, 'x') + "%';").prefix, std::string(127, 'x'));

    constexpr const char* kPrefixOnly = "show leases filters by key prefix only";
    const std::vector<std::string> prefixOnly = {
        "show leases t where k like '%x';",
        "show leases t where k like 'a%b%';",
        "show leases t where k like 'a%b';",
        "show leases t where k like '%%';",
        "show leases t where k like 'abc';",
        "show leases t where k like '';",
        "show leases t where uri = 'x';",
        "show leases t where k like 'a%' and j like 'b%';",
        "show leases t where k like 'a%' or j like 'b%';",
        "show leases t where k like 5;",
        "show leases t where k > 'a';",
        "show leases t where k in ('a');",
    };
    for (const auto& text : prefixOnly) EXPECT_EQ(refusal(text), kPrefixOnly) << text;
    EXPECT_EQ(refusal("show leases t where k like '" + std::string(128, 'x') + "%';"), "a lease key prefix is at most 127 bytes");

    EXPECT_EQ(refusal("show leases t limit 0;"), "limit must be >= 1");
    EXPECT_EQ(refusal("show leases t limit 4294967296;"), "limit must be <= 4294967295");
    EXPECT_EQ(refusal("show leases t limit x;"), "expected integer for limit, got 'x'");
    EXPECT_EQ(refusal("show leases;"), "expected topic name (an identifier or a quoted string), got ';'");
    EXPECT_EQ(refusal("show leases t extra;"), "expected ;, got 'extra'");
    EXPECT_EQ(refusal("show leases t where k like 'a%'"), "expected ;, got end of input");   // no ';'
    EXPECT_EQ(refusal("show leases t where k like 'abc;"), "unterminated string literal");

    // The prefix limit is in BYTES: 63 two-byte characters + 'x' = 127 accepted, 64 two-byte characters = 128 refused.
    std::string two;
    for (int i = 0; i < 63; ++i) two += "\xC3\xA9";
    const auto ok127 = parseAs<ql::ShowLeasesDef>("show leases t where k like '" + two + "x%';");
    ASSERT_TRUE(ok127.prefix.has_value());
    EXPECT_EQ(ok127.prefix->size(), 127u);
    EXPECT_EQ(refusal("show leases t where k like '" + two + "\xC3\xA9%';"), "a lease key prefix is at most 127 bytes");

    // Positions: the refusals of the `like` term are at the pattern token.
    const std::string like = "show leases t where k like 'abc';";
    auto lr = ql::parse(like);
    ASSERT_FALSE(lr.has_value());
    EXPECT_EQ(lr.error().line, 1u);
    EXPECT_EQ(lr.error().col, like.find("'abc'") + 1);
}

TEST(LeaseQlRegression, ShowAndRevokeLeaseRegression) {
    const auto i = parseAs<ql::ShowLeaseDef>("show lease 'acme/claims' key 7;");
    EXPECT_EQ(i.topic, "acme/claims");
    EXPECT_EQ(i.key.kind, ql::Literal::Kind::Integer);
    EXPECT_FALSE(i.key.negative);
    EXPECT_EQ(i.key.magnitude, 7u);

    const auto n = parseAs<ql::ShowLeaseDef>("show lease inst key -3;");
    EXPECT_EQ(n.key.kind, ql::Literal::Kind::Integer);
    EXPECT_TRUE(n.key.negative);
    EXPECT_EQ(n.key.magnitude, 3u);

    const auto sKey = parseAs<ql::ShowLeaseDef>("show lease inst key 'eu-1';");
    EXPECT_EQ(sKey.key.kind, ql::Literal::Kind::String);
    EXPECT_EQ(sKey.key.text, "eu-1");

    const auto big = parseAs<ql::ShowLeaseDef>("show lease inst key 18446744073709551615;");
    EXPECT_EQ(big.key.magnitude, 18446744073709551615ull);

    const auto r = parseAs<ql::RevokeLeaseDef>("revoke lease t key 7;");
    EXPECT_EQ(r.topic, "t");
    EXPECT_EQ(r.key.magnitude, 7u);
    EXPECT_EQ(parseAs<ql::RevokeLeaseDef>("REVOKE LEASE t KEY 'x';").key.text, "x");

    for (const char* text : {"show lease t key true;", "show lease t key false;", "revoke lease t key true;", "show lease t key x;",
                             "revoke lease t key;", "show lease t key ;"})
        EXPECT_EQ(refusal(text), "a lease key is an integer or a string") << text;
    EXPECT_EQ(refusal("show lease t key - 3;"), "expected digits right after '-', got '3'");
    EXPECT_EQ(refusal("show lease t 7;"), "expected 'key', got '7'");
    EXPECT_EQ(refusal("revoke lease t;"), "expected 'key', got ';'");
    EXPECT_EQ(refusal("show lease t key 7 extra;"), "expected ;, got 'extra'");
    EXPECT_EQ(refusal("show lease t key 'abc;"), "unterminated string literal");
    EXPECT_EQ(refusal("revoke lease t key 'abc;"), "unterminated string literal");

    // The security grammar of `revoke` is unchanged.
    const auto rv = parseAs<ql::RevokeDef>("revoke read on topic t from user u;");
    EXPECT_EQ(rv.granteeName, "u");
    EXPECT_EQ(parseAs<ql::RevokeRoleDef>("revoke role r from user u;").role, "r");
    EXPECT_TRUE(ql::isSecurityStatement(ql::Statement(rv)));

    // New statements sit at the END of the variant (indices never move), and are none of the security statements.
    EXPECT_EQ(ql::parse("show leases t;")->index(), 25u);
    EXPECT_EQ(ql::parse("show lease t key 1;")->index(), 26u);
    EXPECT_EQ(ql::parse("revoke lease t key 1;")->index(), 27u);
    EXPECT_EQ(std::variant_size_v<ql::Statement>, 28u);
    for (const char* text : {"show leases t;", "show lease t key 1;", "revoke lease t key 1;", "lease topic t { id uint64 } key id;"}) {
        const auto st = ql::parse(text);
        ASSERT_TRUE(st.has_value()) << text;
        EXPECT_FALSE(ql::isSecurityStatement(*st)) << text;
        EXPECT_FALSE(ql::carriesPassword(*st)) << text;
        EXPECT_EQ(ql::redactedText(*st), "") << text;
    }
    // A script mixes them.
    const auto script = ql::parseScript("lease topic t { id uint64 } key id; show leases t; revoke lease t key 1; revoke read on topic t from user u;");
    ASSERT_TRUE(script.has_value());
    ASSERT_EQ(script->size(), 4u);
    EXPECT_EQ((*script)[0].index(), 0u);
    EXPECT_EQ((*script)[1].index(), 25u);
    EXPECT_EQ((*script)[2].index(), 27u);
    EXPECT_EQ((*script)[3].index(), 14u);
}

TEST(LeaseQlRegression, LeaseErrorPositionsAndDuplicatesRegression) {
    struct Row { std::string text, message; std::size_t col; };
    const std::vector<Row> rows = {
        {"lease topic t { id uint64, lease_x int32 } key id;", "lease_ field names are reserved", 28},
        {"lease topic t { id uint64 } key id with (lease.ttl = 100, lease.min_ttl = 2000);",
         std::string(rp::kLeaseTtlRangeText), 1},
        {"lease topic t { id uint64 } key id with (lease.ttl = -1);", std::string(rp::kLeaseTtlRangeText), 42},
        {"lease topic t { id uint64 } key id with (lease.ttl = 5000, lease.ttl = 6000);", "duplicate setting 'lease.ttl'", 60},
    };
    for (const auto& row : rows) {
        auto r = ql::parse(row.text);
        ASSERT_FALSE(r.has_value()) << row.text;
        EXPECT_EQ(r.error().message, row.message) << row.text;
        EXPECT_EQ(r.error().line, 1u) << row.text;
        EXPECT_EQ(r.error().col, row.col) << row.text;
    }
}

TEST(LeaseQlRegression, LeaseIsAnOrdinaryNameRegression) {
    EXPECT_EQ(parseAs<ql::TopicDef>("topic lease {id int32} key id;").name, "lease");
    EXPECT_FALSE(parseAs<ql::TopicDef>("topic lease {id int32} key id;").lease);
    const auto lt = parseAs<ql::TopicDef>("lease topic lease {id int32} key id;");
    EXPECT_EQ(lt.name, "lease");
    EXPECT_TRUE(lt.lease);
    EXPECT_EQ(parseAs<ql::ShowTopicDef>("show topic lease;").name, "lease");
    EXPECT_EQ(parseAs<ql::AlterTopicConfigDef>("alter topic lease set retention.ms = 1;").name, "lease");
    EXPECT_EQ(parseAs<ql::SelectDef>("select * from lease where id = 1;").view, "lease");
    EXPECT_EQ(parseAs<ql::RevokeDef>("revoke read on topic lease from user u;").granteeName, "u");
    EXPECT_EQ(parseAs<ql::RevokeRoleDef>("revoke role lease from user u;").role, "lease");
}

TEST(LeaseQlRegression, RuleOrderRegression) {
    // Pairs of violations: the EARLIER rule in the broker's leaseDefineRefusal order wins.
    struct Row { std::string text, message; };
    const std::string ttl = std::string(rp::kLeaseTtlRangeText);
    const std::vector<Row> rows = {
        // spec -> typed+keyed
        {"lease topic t partitions 2 with (lease.ttl = 5);", ttl},
        // typed+keyed -> every later rule (autoincrement without a key is the generic "autoincrement needs a key")
        {"lease topic t { id uint64 } with (key.index = on, retention.ms = 5);", "a lease topic needs a typed, keyed topic"},
        {"lease topic t { id uint64, lease_x int32 } with (autoincrement.max = 5);", "a lease topic needs a typed, keyed topic"},
        // autoincrement -> key.index
        {"lease topic t { id uint64 autoincrement } key id with (key.index = on);", "a lease topic cannot use autoincrement"},
        // key.index -> cleanup/retention
        {"lease topic t { id uint64 } key id with (key.index = on, retention.ms = 5);", "a lease topic's key index is its lease view"},
        // cleanup/retention -> lease_ fields
        {"lease topic t { id uint64, lease_x int32 } key id with (retention.ms = 5);",
         "a lease topic is compacted and keeps every live lease"},
        // lease_ fields -> autoincrement.reserve/max
        {"lease topic t { id uint64, lease_x int32 } key id with (autoincrement.max = 5);", "lease_ field names are reserved"},
        // last: autoincrement.reserve / max alone
        {"lease topic t { id uint64 } key id with (autoincrement.max = 5);",
         "autoincrement.max and autoincrement.reserve need autoincrement"},
        {"lease topic t { id uint64 } key id with (autoincrement.reserve = 5);",
         "autoincrement.max and autoincrement.reserve need autoincrement"},
        // each rule is a whole pass: a later setting of an EARLIER rule beats an earlier setting of a later rule
        {"lease topic t { id uint64 } key id with (retention.ms = 5, key.index = on);", "a lease topic's key index is its lease view"},
        {"lease topic t { id uint64 } key id with (autoincrement.max = 5, retention.ms = 5);",
         "a lease topic is compacted and keeps every live lease"},
        {"lease topic t { id uint64 } key id with (retention.ms = 5, lease.ttl = 1);", ttl},
    };
    for (const auto& row : rows) EXPECT_EQ(refusal(row.text), row.message) << row.text;

    // The protocol defaults are accepted (the broker only refuses a value that differs from them).
    EXPECT_TRUE(ql::parse("lease topic t { id uint64 } key id with (autoincrement.reserve = " +
                          std::to_string(rp::kAutoIncrementReserveDefault) + ");").has_value());
    EXPECT_TRUE(ql::parse("lease topic t { id uint64 } key id with (autoincrement.max = " +
                          std::to_string(rp::kAutoIncrementMaxDefault) + ");").has_value());
}

TEST(LeaseQlRegression, ScriptViewOverLeaseColumnsRegression) {
    const auto td = topicOf("lease topic inst { id uint64, addr string(64) } key id;");
    const ql::ViewSource src = ql::viewSourceOf(td);
    ASSERT_EQ(src.descriptor.fields.size(), 6u);
    const char* names[] = {"lease_version", "lease_ttl_ms", "lease_holder", "lease_remaining_ms", "id", "addr"};
    for (std::size_t i = 0; i < 6; ++i) {
        EXPECT_EQ(src.descriptor.fields[i].name, names[i]);
        EXPECT_EQ(src.descriptor.fields[i].id, i);
    }
    // A plain topic's source is unchanged.
    EXPECT_EQ(ql::viewSourceOf(topicOf("topic p { id uint64 } key id;")).descriptor.fields.size(), 1u);

    auto v = ql::parse("create view v key id as select id, lease_holder from inst;");
    ASSERT_TRUE(v.has_value()) << v.error().message;
    const auto* cv = std::get_if<ql::CreateViewDef>(&*v);
    ASSERT_NE(cv, nullptr);
    auto plan = ql::applyViewPlan(*cv, src);
    ASSERT_TRUE(plan.has_value()) << plan.error().message;
    EXPECT_EQ(plan->output.size(), 2u);
}
