#include <rune/protocol/SecurityTypes.h>
#include <type_traits>
#include <rune/ql/Backends.h>
#include <rune/ql/Parser.h>
#include <gtest/gtest.h>
namespace ql = rune::ql;
namespace pc = rune::protocol::client;

TEST(SecurityStatementsRegression, EverySpecStatementParses) {
    const char* spec[] = {
        "CREATE USER ws1 WITH PASSWORD 'correct horse battery';",
        "ALTER  USER ws1 ADD PASSWORD 'second password 123';",
        "ALTER  USER ws1 DROP PASSWORD 1;",
        "ALTER  USER ws1 SET PASSWORD 'new password 1234' TERMINATE SESSIONS;",
        "ALTER  USER ws1 SET PASSWORD 'new password 1234';",
        "ALTER  USER ws1 DISABLE;", "ALTER USER ws1 ENABLE;", "DROP   USER ws1;",
        "CREATE ROLE world_server;",
        "GRANT READ  ON TOPIC player_events   TO ROLE world_server;",
        "GRANT WRITE ON TOPIC PREFIX 'world.' TO ROLE world_server;",
        "GRANT READ  ON GROUP PREFIX 'world-' TO ROLE world_server;",
        "GRANT ROLE world_server TO USER ws1;",
        "REVOKE WRITE ON TOPIC PREFIX 'world.' FROM ROLE world_server;",
        "DROP ROLE world_server;",
        "SHOW USERS;", "SHOW ROLES;", "SHOW GRANTS FOR USER ws1;", "SHOW GRANTS FOR ROLE world_server;",
        "GRANT CREATE ON CLUSTER TO USER ops;", "GRANT WRITE ON TRANSACTIONAL_ID 'ws-1' TO USER ws1;",
        "SHOW GRANTS FOR USER ws1 AS OF OFFSET 12;", "SHOW SECURITY HISTORY USER ws1 SINCE '2026-10-13T00:00:00Z';",
        "REVOKE ROLE world_server FROM USER ws1;", "GRANT READ ON TOPIC 'world.zone-1' TO USER 'svc@eu-1';",
    };
    for (const char* s : spec) { auto r = ql::parse(s); EXPECT_TRUE(r.has_value()) << s << ": " << (r ? "" : r.error().message); }
}

TEST(SecurityStatementsRegression, PasswordsAreRedactedInTheStatementSentRegression) {
    auto st = ql::parse("create user ws1 with password 'correct horse battery';").value();
    const std::string text = ql::redactedText(st);
    EXPECT_EQ(text, "CREATE USER ws1 WITH PASSWORD '<redacted>';");
    EXPECT_EQ(text.find("correct"), std::string::npos);
    auto plan = ql::applySecurityPlan(st).value();
    EXPECT_EQ(plan.statement, text);
    EXPECT_EQ(plan.password, "correct horse battery") << "kept in memory for the verifier, never in the statement";
    auto set = ql::parse("ALTER USER ws1 SET PASSWORD 'p4ssword-long' TERMINATE SESSIONS;").value();
    EXPECT_EQ(ql::redactedText(set), "ALTER USER ws1 SET PASSWORD '<redacted>' TERMINATE SESSIONS;");
    EXPECT_FALSE(ql::carriesPassword(ql::parse("DROP USER ws1;").value()));
}

TEST(SecurityStatementsRegression, BadStatementsAreLocatedErrors) {
    struct Case { const char* src; const char* contains; } cases[] = {
        {"ALTER USER ws1 DROP PASSWORD 3;", "password index is 1 (oldest) or 2"},
        {"GRANT READ ON CLUSTERS TO USER u;", "expected 'cluster', 'topic', 'view', 'group' or 'transactional_id'"},
        {"GRANT FLY ON TOPIC t TO USER u;", "expected an operation"},
        {"CREATE USER ws1 WITH PASSWORD '';", "password must not be empty"},
        {"GRANT READ ON TOPIC t TO u;", "expected 'user' or 'role'"},
        {"SHOW GRANTS FOR ws1;", "expected 'user' or 'role'"},
        {"CREATE USER ws1;", "expected 'with'"},
    };
    for (const auto& c : cases) {
        auto r = ql::parse(c.src);
        ASSERT_FALSE(r.has_value()) << c.src;
        EXPECT_NE(r.error().message.find(c.contains), std::string::npos) << c.src << " -> " << r.error().message;
    }
}

TEST(SecurityStatementsRegression, ExistingStatementsKeepTheirVariantIndices) {
    EXPECT_EQ(ql::parse("topic t partitions 1;")->index(), 0u);
    EXPECT_EQ(ql::parse("diff catalog offset 1 to 2;")->index(), 7u);
    EXPECT_EQ(ql::parse("CREATE ROLE r;")->index(), 11u);
}

TEST(SecurityStatementsRegression, ScriptsMixCatalogAndSecurityStatements) {
    auto s = ql::parseScript("topic zone1 partitions 3;\nCREATE ROLE r; GRANT WRITE ON TOPIC PREFIX 'world.' TO ROLE r;");
    ASSERT_TRUE(s.has_value());
    EXPECT_EQ(s->size(), 3u);
}

TEST(SecurityStatementsTest, VariantIndicesOfEveryNewStatementArePinned) {
    const std::pair<const char*, std::size_t> idx[] = {
        {"CREATE USER u WITH PASSWORD 'x';", 8}, {"ALTER USER u DISABLE;", 9}, {"DROP USER u;", 10},
        {"CREATE ROLE r;", 11}, {"DROP ROLE r;", 12}, {"GRANT READ ON TOPIC t TO USER u;", 13},
        {"REVOKE READ ON TOPIC t FROM USER u;", 14}, {"GRANT ROLE r TO USER u;", 15},
        {"REVOKE ROLE r FROM USER u;", 16}, {"SHOW USERS;", 17}, {"SHOW ROLES;", 18},
        {"SHOW GRANTS FOR USER u;", 19}, {"SHOW SECURITY HISTORY;", 20},
    };
    for (const auto& [src, i] : idx) EXPECT_EQ(ql::parse(src).value().index(), i) << src;
}

TEST(SecurityStatementsTest, ParsedFieldsAreExact) {
    auto g = std::get<ql::GrantDef>(ql::parse("grant Write on topic prefix 'world.' to role 'w@1';").value());
    EXPECT_EQ(g.op, ql::SecOp::Write);
    EXPECT_EQ(g.resource.type, ql::SecResource::Topic);
    EXPECT_TRUE(g.resource.prefix);
    EXPECT_EQ(g.resource.name, "world.");
    EXPECT_EQ(g.grantee, ql::Grantee::Role);
    EXPECT_EQ(g.granteeName, "w@1");
    auto c = std::get<ql::GrantDef>(ql::parse("grant admin on cluster to user ops;").value());
    EXPECT_EQ(c.op, ql::SecOp::Admin);
    EXPECT_EQ(c.resource.type, ql::SecResource::Cluster);
    auto n = std::get<ql::GrantDef>(ql::parse("grant read on topic prefix to user u;").value());
    EXPECT_FALSE(n.resource.prefix) << "a topic literally named prefix";
    EXPECT_EQ(n.resource.name, "prefix");
    auto a = std::get<ql::AlterUserDef>(ql::parse("alter user u drop password 2;").value());
    EXPECT_EQ(a.change, ql::AlterUserDef::Change::DropPassword);
    EXPECT_EQ(a.index, 2);
    auto sg = std::get<ql::ShowGrantsDef>(ql::parse("show grants for role r as of time '2026-10-13T00:00:00Z';").value());
    EXPECT_EQ(sg.who, ql::Grantee::Role);
    ASSERT_TRUE(sg.at.has_value());
    EXPECT_EQ(sg.at->by, ql::AsOfPoint::By::Time);
    auto h = std::get<ql::ShowSecurityHistoryDef>(ql::parse("show security history role r since '2026-10-13T00:00:00Z';").value());
    ASSERT_TRUE(h.who.has_value());
    EXPECT_EQ(*h.who, ql::Grantee::Role);
    EXPECT_EQ(h.name, "r");
    EXPECT_TRUE(h.sinceIso.has_value());
}

TEST(SecurityStatementsTest, MoreBadStatements) {
    for (const char* s : {"ALTER USER u DROP PASSWORD 0;", "GRANT ROLE r TO ROLE x;", "ALTER USER u SET PASSWORD '';",
                          "SHOW SECURITY;", "CREATE TOPIC x;", "SHOW GRANTS FOR USER u AS OF VERSION 1;",
                          "DROP USER;", "GRANT READ ON TOPIC t FROM USER u;", "REVOKE READ ON TOPIC t TO USER u;",
                          "ALTER USER u FROB;", "ALTER TOPIC;"})
        EXPECT_FALSE(ql::parse(s).has_value()) << s;
    // existing messages grow, they do not break
    EXPECT_NE(ql::parse("drop frob x;").error().message.find("'topic'"), std::string::npos);
}

TEST(SecurityStatementsTest, RedactedTextIsCanonicalAndReparses) {
    const char* in[] = {
        "create user 'svc@eu-1' with password 'a b c d e f g h';", "alter user u add password 'zzzzzzzzzzzz';",
        "alter user u drop password 1;", "alter user u set password 'zzzzzzzzzzzz';", "alter user u disable;",
        "alter user u enable;", "drop user u;", "create role r;", "drop role r;",
        "grant read on topic 'a.b' to user u;", "revoke create on cluster from role r;",
        "grant describe on view prefix '' to role r;", "grant role r to user u;", "revoke role r from user u;",
        "show users;", "show roles;", "show grants for user u;", "show grants for user u as of offset 5;",
        "show grants for role r as of time '2026-10-13T00:00:00Z';", "show security history;",
        "show security history user u since '2026-10-13T00:00:00Z';",
    };
    for (const char* s : in) {
        auto st = ql::parse(s).value();
        const std::string t = ql::redactedText(st);
        EXPECT_EQ(t.find("zzzz"), std::string::npos) << t;
        EXPECT_EQ(t.find("a b c"), std::string::npos) << t;
        if (ql::carriesPassword(st)) {
            EXPECT_NE(t.find("'<redacted>'"), std::string::npos) << t;
            EXPECT_TRUE(ql::isSecurityStatement(st));
        }
        auto again = ql::parse(t);
        ASSERT_TRUE(again.has_value()) << t;
        EXPECT_EQ(ql::redactedText(*again), t) << "idempotent";
    }
    EXPECT_EQ(ql::redactedText(ql::parse("create user 'svc@eu-1' with password 'abcdefghijkl';").value()),
              "CREATE USER 'svc@eu-1' WITH PASSWORD '<redacted>';");
    EXPECT_EQ(ql::redactedText(ql::parse("grant read on topic prefix 'w.' to role r;").value()),
              "GRANT READ ON TOPIC PREFIX 'w.' TO ROLE r;");
    EXPECT_EQ(ql::redactedText(ql::parse("revoke admin on cluster from user u;").value()),
              "REVOKE ADMIN ON CLUSTER FROM USER u;");
    EXPECT_TRUE(ql::redactedText(ql::parse("topic t partitions 1;").value()).empty());
    EXPECT_FALSE(ql::isSecurityStatement(ql::parse("topic t partitions 1;").value()));
    EXPECT_TRUE(ql::isSecurityStatement(ql::parse("show users;").value()));
}

TEST(SecurityStatementsTest, PlansCarryTheWireFields) {
    auto p = ql::applySecurityPlan(ql::parse("alter user u set password 'zzzzzzzzzzzz' terminate sessions;").value()).value();
    EXPECT_EQ(p.kind, pc::SecurityOpKind::AlterUser);
    EXPECT_EQ(p.change, pc::UserChangeKind::SetPassword);
    EXPECT_EQ(p.name, "u");
    EXPECT_EQ(p.terminateSessions, 1);
    EXPECT_EQ(p.password, "zzzzzzzzzzzz");
    auto d = ql::applySecurityPlan(ql::parse("alter user u drop password 2;").value()).value();
    EXPECT_EQ(d.change, pc::UserChangeKind::DropPassword);
    EXPECT_EQ(d.passwordIndex, 2);
    EXPECT_TRUE(d.password.empty());
    auto g = ql::applySecurityPlan(ql::parse("grant write on topic prefix 'w.' to role r;").value()).value();
    EXPECT_EQ(g.kind, pc::SecurityOpKind::Grant);
    EXPECT_EQ(g.name, "r");
    EXPECT_EQ(g.granteeKind, 1);
    EXPECT_EQ(g.op, 1);
    EXPECT_EQ(g.resourceType, 1);
    EXPECT_EQ(g.patternKind, 1);
    EXPECT_EQ(g.pattern, "w.");
    auto c = ql::applySecurityPlan(ql::parse("revoke create on cluster from user u;").value()).value();
    EXPECT_EQ(c.kind, pc::SecurityOpKind::Revoke);
    EXPECT_EQ(c.resourceType, 0);
    EXPECT_EQ(c.granteeKind, 0);
    auto gr = ql::applySecurityPlan(ql::parse("grant role r to user u;").value()).value();
    EXPECT_EQ(gr.kind, pc::SecurityOpKind::GrantRole);
    EXPECT_EQ(gr.name, "u");
    EXPECT_EQ(gr.roleName, "r");
    EXPECT_EQ(ql::applySecurityPlan(ql::parse("create role r;").value())->kind, pc::SecurityOpKind::CreateRole);
    EXPECT_EQ(ql::applySecurityPlan(ql::parse("drop user r;").value())->kind, pc::SecurityOpKind::DropUser);
    // SHOW statements and catalog statements are not plans
    EXPECT_FALSE(ql::applySecurityPlan(ql::parse("show users;").value()).has_value());
    EXPECT_FALSE(ql::applySecurityPlan(ql::parse("topic t partitions 1;").value()).has_value());
}

TEST(SecurityStatementsRegression, ParseErrorsNeverEchoAPasswordRegression) {
    struct Case { const char* src; const char* secret; } cases[] = {
        {"CREATE USER ws1 WITH PASSWORD hunter2hunter2;", "hunter2"},                // unquoted password
        {"CREATE USER ws1 WITH PASSWORD 'abc'def123456';", "def123456"},             // a quote inside: the tail is junk after the password
        {"ALTER USER u ADD PASSWORD secretsecret12;", "secretsecret12"},
        {"ALTER USER u SET PASSWORD 'pw-long-enough' junk77secret;", "junk77secret"},
        {"CREATE USER ws1 WITH PASSWORD 'abcdefghijklm' 9999secret;", "9999secret"},
        {"CREATE USER ws1 WITH PASSWORD ;", "zzz"},
        // the keyword is missing or misplaced: the literal is the current token when the error is raised
        {"CREATE USER ws1 WITH 'hunter2hunter2';", "hunter2hunter2"},
        {"CREATE USER ws1 'hunter2hunter2';", "hunter2hunter2"},
        {"ALTER USER ws1 SET 'hunter2hunter2';", "hunter2hunter2"},
        {"ALTER USER ws1 ADD 'hunter2hunter2';", "hunter2hunter2"},
        {"ALTER USER ws1 'hunter2hunter2';", "hunter2hunter2"},
        {"CREATE USER ws1 WITH 'hunter2hunter2", "hunter2hunter2"},                  // unterminated: the tail runs on
        {"ALTER USER ws1 SET PASSWORD 'hunter2hunter2", "hunter2hunter2"},
        // the whole class: a quoted string in ANY statement is never echoed
        {"CREATE USR ws1 WITH PASSWORD 'hunter2hunter2';", "hunter2hunter2"},
        {"GRANT 'hunter2hunter2' ON TOPIC t TO ROLE r;", "hunter2hunter2"},
        {"ALTER TOPIC t 'hunter2hunter2';", "hunter2hunter2"},
        {"ALTER TOPIC t SET COMPATIBILITY 'hunter2hunter2';", "hunter2hunter2"},
        {"DROP 'hunter2hunter2';", "hunter2hunter2"},
    };
    for (const auto& c : cases) {
        auto r = ql::parse(c.src);
        ASSERT_FALSE(r.has_value()) << c.src;
        EXPECT_EQ(r.error().message.find(c.secret), std::string::npos) << c.src << " -> " << r.error().message;
        EXPECT_EQ(r.error().message.find("abcdefghijklm"), std::string::npos) << r.error().message;
        EXPECT_EQ(r.error().message.find("pw-long-enough"), std::string::npos) << r.error().message;
        EXPECT_FALSE(r.error().message.empty());
    }
    // parseScript: the failing statement's error carries no literal either
    for (const char* script : {"create role r;\nCREATE USER ws1 WITH 'hunter2hunter2';",
                               "create role r;\nALTER USER ws1 SET 'hunter2hunter2",
                               "create role r;\nCREATE USER ws1 'hunter2hunter2';"}) {
        auto sr = ql::parseScript(script);
        ASSERT_FALSE(sr.has_value()) << script;
        EXPECT_EQ(sr.error().statementIndex, 2u);
        EXPECT_EQ(sr.error().error.message.find("hunter2"), std::string::npos) << sr.error().error.message;
        EXPECT_FALSE(sr.error().error.message.empty());
    }
    // errors before the password keyword still name the offending token (the user's own text)
    auto early = ql::parse("CREATE USER ws1 WITHOUT PASSWORD 'x';");
    ASSERT_FALSE(early.has_value());
    EXPECT_NE(early.error().message.find("WITHOUT"), std::string::npos) << early.error().message;
}

TEST(SecurityStatementsRegression, PlanBytesAreTheProtocolEnumsRegression) {
    namespace rp = rune::protocol;
    static_assert(std::is_same_v<ql::SecOp, rp::SecurityOperation>);
    static_assert(std::is_same_v<ql::SecResource, rp::SecurityResourceType>);
    static_assert(std::is_same_v<ql::Grantee, rp::SecurityGranteeKind>);
    auto plan = ql::applySecurityPlan(ql::parse("GRANT WRITE ON TOPIC PREFIX 'world.' TO ROLE r;").value()).value();
    EXPECT_EQ(plan.op, static_cast<uint8_t>(rp::SecurityOperation::Write));
    EXPECT_EQ(plan.resourceType, static_cast<uint8_t>(rp::SecurityResourceType::Topic));
    EXPECT_EQ(plan.patternKind, static_cast<uint8_t>(rp::SecurityPatternKind::Prefix));
    EXPECT_EQ(plan.granteeKind, static_cast<uint8_t>(rp::SecurityGranteeKind::Role));
    EXPECT_TRUE(rp::validSecurityOperation(plan.op) && rp::validSecurityResourceType(plan.resourceType));
}

// Issue #121: FORCE (the statement may remove the last enabled security admin) on every statement that can remove
// one; it round-trips through the redacted text and reaches the plan.  GRANT never takes it.
TEST(SecurityStatementsRegression, ForceParsesRendersAndReachesThePlanRegression) {
    const std::pair<const char*, const char*> cases[] = {
        {"alter user admin disable force;", "ALTER USER admin DISABLE FORCE;"},
        {"drop user admin force;", "DROP USER admin FORCE;"},
        {"drop role ops force;", "DROP ROLE ops FORCE;"},
        {"revoke admin on cluster from user admin force;", "REVOKE ADMIN ON CLUSTER FROM USER admin FORCE;"},
        {"revoke role ops from user admin force;", "REVOKE ROLE ops FROM USER admin FORCE;"},
    };
    for (const auto& [src, text] : cases) {
        auto st = ql::parse(src);
        ASSERT_TRUE(st.has_value()) << src << ": " << st.error().message;
        EXPECT_EQ(ql::redactedText(*st), text);
        auto again = ql::parse(ql::redactedText(*st));
        ASSERT_TRUE(again.has_value()) << text;
        auto plan = ql::applySecurityPlan(*again);
        ASSERT_TRUE(plan.has_value()) << text;
        EXPECT_TRUE(plan->force) << text;
    }
    EXPECT_FALSE(ql::applySecurityPlan(ql::parse("drop user admin;").value())->force);
    EXPECT_FALSE(ql::applySecurityPlan(ql::parse("alter user admin disable;").value())->force);
    EXPECT_FALSE(ql::parse("grant admin on cluster to user admin force;").has_value()) << "GRANT takes no FORCE";
    EXPECT_FALSE(ql::parse("alter user admin enable force;").has_value());
    EXPECT_EQ(std::get<ql::DropUserDef>(ql::parse("drop user force;").value()).name, "force") << "a user named force";
}

// PR #410 review, adversarial pass: FORCE at the edges of the grammar - case-insensitive, at most once, only at the
// end, never on a statement that cannot remove an admin, and a name spelled "force" stays a name.
TEST(SecurityStatementsRegression, ForceAtTheEdgesOfTheGrammarAdversarialRegression) {
    EXPECT_TRUE(ql::applySecurityPlan(ql::parse("ALTER USER admin DISABLE FoRcE ;").value())->force);
    EXPECT_FALSE(ql::parse("drop user admin force force;").has_value()) << "FORCE twice";
    EXPECT_FALSE(ql::parse("drop user force admin;").has_value()) << "FORCE before the name";
    EXPECT_FALSE(ql::parse("create user u with password 'abcdefghijkl' force;").has_value());
    EXPECT_FALSE(ql::parse("create role r force;").has_value());
    EXPECT_FALSE(ql::parse("grant role ops to user admin force;").has_value());
    EXPECT_FALSE(ql::parse("alter user admin drop password 1 force;").has_value());
    EXPECT_FALSE(ql::parse("alter user admin set password 'abcdefghijkl' force;").has_value());
    const auto r = ql::parse("revoke read on topic t from user force force;");
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(std::get<ql::RevokeDef>(*r).granteeName, "force");
    EXPECT_TRUE(std::get<ql::RevokeDef>(*r).force);
    const auto rr = ql::parse("revoke role force from user force;");
    ASSERT_TRUE(rr.has_value()) << "a role and a user named force";
    EXPECT_FALSE(std::get<ql::RevokeRoleDef>(*rr).force);
}
