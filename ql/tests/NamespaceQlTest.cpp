// Namespaces Task 11 (spec §10, D6/D7): QL accepts namespace-qualified names, NAMESPACE resources and TRUNCATE.
#include <rune/protocol/SecurityTypes.h>
#include <rune/protocol/client/Protocol.h>
#include <rune/ql/Backends.h>
#include <rune/ql/Parser.h>
#include <rune/security/SecurityStatementText.h>
#include <gtest/gtest.h>
#include <string>
namespace ql = rune::ql;
namespace pc = rune::protocol::client;
namespace rp = rune::protocol;

namespace {
ql::SecurityPlan planOf(const char* text) {
    auto st = ql::parse(text);
    EXPECT_TRUE(st.has_value()) << text << ": " << (st ? "" : st.error().message);
    if (!st) return {};
    auto p = ql::applySecurityPlan(*st);
    EXPECT_TRUE(p.has_value()) << text;
    if (!p) return {};
    // The leader renders the recorded statement from the op (#228): byte-identical to the client's redaction.
    EXPECT_EQ(rune::security::securityStatementText(static_cast<uint8_t>(p->kind), p->name,
                                                    static_cast<uint8_t>(p->change), p->passwordIndex,
                                                    p->terminateSessions, p->force ? 1 : 0, p->granteeKind, p->op,
                                                    p->resourceType, p->patternKind, p->pattern, p->roleName),
              p->statement) << text;
    return *p;
}
constexpr uint8_t u8(auto e) { return static_cast<uint8_t>(e); }
} // namespace

TEST(NamespaceQlRegression, QualifiedNamesRegression) {
    {
        const auto p = planOf("CREATE USER 'a/alice' WITH PASSWORD 'correct horse battery';");
        EXPECT_EQ(p.kind, pc::SecurityOpKind::CreateUser);
        EXPECT_EQ(p.name, "a/alice");
        EXPECT_EQ(p.statement, "CREATE USER 'a/alice' WITH PASSWORD '<redacted>';");
    }
    {
        const auto p = planOf("GRANT READ ON TOPIC 'a/t' TO ROLE 'a/r';");
        EXPECT_EQ(p.kind, pc::SecurityOpKind::Grant);
        EXPECT_EQ(p.name, "a/r");
        EXPECT_EQ(p.granteeKind, u8(rp::SecurityGranteeKind::Role));
        EXPECT_EQ(p.op, u8(rp::SecurityOperation::Read));
        EXPECT_EQ(p.resourceType, u8(rp::SecurityResourceType::Topic));
        EXPECT_EQ(p.patternKind, u8(rp::SecurityPatternKind::Exact));
        EXPECT_EQ(p.pattern, "a/t");
        EXPECT_EQ(p.statement, "GRANT READ ON TOPIC 'a/t' TO ROLE 'a/r';");
    }
    {
        const auto p = planOf("GRANT ADMIN ON NAMESPACE 'a' TO ROLE 'a/admins';");
        EXPECT_EQ(p.kind, pc::SecurityOpKind::Grant);
        EXPECT_EQ(p.name, "a/admins");
        EXPECT_EQ(p.op, u8(rp::SecurityOperation::Admin));
        EXPECT_EQ(p.resourceType, u8(rp::SecurityResourceType::Namespace));
        EXPECT_EQ(p.patternKind, u8(rp::SecurityPatternKind::Exact));
        EXPECT_EQ(p.pattern, "a");
        EXPECT_EQ(p.statement, "GRANT ADMIN ON NAMESPACE a TO ROLE 'a/admins';");
    }
    {
        const auto p = planOf("GRANT TRUNCATE ON TOPIC t TO USER u;");
        EXPECT_EQ(p.op, u8(rp::SecurityOperation::Truncate));
        EXPECT_EQ(p.resourceType, u8(rp::SecurityResourceType::Topic));
        EXPECT_EQ(p.pattern, "t");
        EXPECT_EQ(p.statement, "GRANT TRUNCATE ON TOPIC t TO USER u;");
    }
    {
        const auto p = planOf("REVOKE TRUNCATE ON TOPIC 'a/t' FROM ROLE 'a/r';");
        EXPECT_EQ(p.kind, pc::SecurityOpKind::Revoke);
        EXPECT_EQ(p.op, u8(rp::SecurityOperation::Truncate));
        EXPECT_EQ(p.pattern, "a/t");
    }
    {
        const auto p = planOf("GRANT ROLE 'a/r' TO USER 'a/alice';");
        EXPECT_EQ(p.kind, pc::SecurityOpKind::GrantRole);
        EXPECT_EQ(p.name, "a/alice");
        EXPECT_EQ(p.roleName, "a/r");
    }
    // A PREFIX pattern is a prefix string, not a qualified name: never shape-checked.
    EXPECT_EQ(planOf("GRANT READ ON TOPIC PREFIX 'a/' TO ROLE r;").pattern, "a/");
}

TEST(NamespaceQlRegression, BareIdentifiersUnchangedRegression) {
    const auto p = planOf("GRANT READ ON TOPIC player_events TO ROLE world_server;");
    EXPECT_EQ(p.name, "world_server");
    EXPECT_EQ(p.pattern, "player_events");
    EXPECT_EQ(p.statement, "GRANT READ ON TOPIC player_events TO ROLE world_server;");
    EXPECT_EQ(planOf("CREATE USER ws1 WITH PASSWORD 'correct horse battery';").name, "ws1");
    EXPECT_EQ(planOf("GRANT DESCRIBE ON NAMESPACE a TO USER u;").pattern, "a");
}

TEST(NamespaceQlRegression, MalformedQualifiedNamesAreParseErrorsRegression) {
    struct Case { const char* src; const char* contains; } cases[] = {
        {"CREATE USER 'a/b/c' WITH PASSWORD 'correct horse battery';", "one '/'"},
        {"CREATE USER 'a//x' WITH PASSWORD 'correct horse battery';", "one '/'"},
        {"CREATE USER '/x' WITH PASSWORD 'correct horse battery';", "one '/'"},
        {"CREATE USER 'a/' WITH PASSWORD 'correct horse battery';", "one '/'"},
        {"CREATE ROLE 'a/b/c';", "one '/'"},
        {"DROP USER 'a/b/c';", "one '/'"},
        {"GRANT READ ON TOPIC 'a/b/c' TO ROLE r;", "one '/'"},
        {"GRANT READ ON TOPIC t TO ROLE 'a/b/c';", "one '/'"},
        {"GRANT ROLE 'a//r' TO USER u;", "one '/'"},
        {"GRANT ADMIN ON NAMESPACE 'a/b' TO ROLE r;", "a namespace name has no '/'"},
        {"GRANT ADMIN ON NAMESPACE PREFIX 'a' TO ROLE r;", "a namespace name"},
        {"GRANT FLY ON TOPIC t TO USER u;", "'admin' or 'truncate'"},
    };
    for (const auto& c : cases) {
        auto r = ql::parse(c.src);
        ASSERT_FALSE(r.has_value()) << c.src;
        EXPECT_NE(r.error().message.find(c.contains), std::string::npos) << c.src << " -> " << r.error().message;
    }
}
