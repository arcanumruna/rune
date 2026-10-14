#include <rune/ql/Parser.h>
#include <rune/ql/Backends.h>
#include <gtest/gtest.h>
#include <limits>
#include <type_traits>

namespace ql = rune::ql;

namespace {
bool mentions(const std::string& s, const char* what) { return s.find(what) != std::string::npos; }
}

TEST(QlCatalogStatementsRegression, UntypedTopicWithReplicas) {
    auto r = ql::parse("topic events partitions 3 replicas 2;");
    ASSERT_TRUE(r.has_value()) << r.error().message;
    const auto& t = std::get<ql::TopicDef>(*r);
    EXPECT_TRUE(t.fields.empty()); EXPECT_EQ(t.partitions, 3u); EXPECT_EQ(t.replicas, 2);
    EXPECT_TRUE(ql::applyPlan(t).descriptorBlob.empty());
}
TEST(QlCatalogStatementsRegression, WithClauseCarriesEveryCreationSetting) {
    auto r = ql::parse("topic c partitions 1 with (cleanup.policy = compact, segment.bytes = 1048576, "
                       "retention.ms = 1000, tier = leader, durability = all-in-sync, copartition.group = 'g');");
    ASSERT_TRUE(r.has_value()) << r.error().message;
    const auto plan = ql::applyPlan(std::get<ql::TopicDef>(*r));
    EXPECT_EQ(plan.config.cleanupPolicy, 1);
    EXPECT_EQ(plan.config.segmentRollBytes, 1'048'576);
    EXPECT_EQ(plan.config.retentionMs, 1000);
    EXPECT_EQ(plan.config.durabilityTier, 0);
    EXPECT_EQ(plan.config.durability, 1);
    EXPECT_EQ(plan.config.coPartitionGroup, "g");
    EXPECT_FALSE(ql::parse("topic c partitions 1 with (tier = fast);").has_value());
}
TEST(QlCatalogStatementsRegression, TypedTopicStillRequiresKey) {
    EXPECT_FALSE(ql::parse("topic t { id int64 } partitions 1;").has_value());
    EXPECT_TRUE(ql::parse("topic t { id int64 } partitions 1 replicas 3 key id;").has_value());
}
TEST(QlCatalogStatementsRegression, DropTopicAndView) {
    EXPECT_EQ(std::get<ql::DropTopicDef>(*ql::parse("drop topic users;")).name, "users");
    EXPECT_EQ(std::get<ql::DropViewDef>(*ql::parse("DROP VIEW top;")).name, "top");
}
TEST(QlCatalogStatementsRegression, AlterTopicSetSettings) {
    auto r = ql::parse("alter topic t set retention.ms = 60000, min.cleanable.dirty.ratio = 0.25;");
    ASSERT_TRUE(r.has_value()) << r.error().message;
    auto plan = ql::applyConfigPlan(std::get<ql::AlterTopicConfigDef>(*r));
    ASSERT_TRUE(plan.has_value());
    ASSERT_EQ(plan->changes.size(), 2u);
    EXPECT_EQ(plan->changes[0], (std::pair<uint8_t, int64_t>{1, 60000}));
    EXPECT_EQ(plan->changes[1], (std::pair<uint8_t, int64_t>{4, 250000}));
}
TEST(QlCatalogStatementsRegression, ImmutableSettingIsAParseErrorNamingIt) {
    auto r = ql::parse("alter topic t set partitions = 4;");
    ASSERT_FALSE(r.has_value());
    EXPECT_NE(r.error().message.find("partitions"), std::string::npos);
}
TEST(QlCatalogStatementsRegression, ShowHistoryForms) {
    auto all = std::get<ql::ShowHistoryDef>(*ql::parse("show history;"));
    EXPECT_FALSE(all.kind.has_value());
    auto one = std::get<ql::ShowHistoryDef>(*ql::parse("show history schema players since '2026-10-01T00:00:00Z';"));
    EXPECT_EQ(*one.kind, ql::ObjectKind::Schema); EXPECT_EQ(one.name, "players");
    EXPECT_EQ(*one.sinceIso, "2026-10-01T00:00:00Z");
}
TEST(QlCatalogStatementsRegression, ShowAsOfForms) {
    auto v = std::get<ql::ShowAsOfDef>(*ql::parse("show schema players as of version 3;"));
    EXPECT_EQ(v.at.by, ql::AsOfPoint::By::Version); EXPECT_EQ(v.at.number, 3);
    auto o = std::get<ql::ShowAsOfDef>(*ql::parse("show view top as of offset 17;"));
    EXPECT_EQ(o.kind, ql::ObjectKind::View); EXPECT_EQ(o.at.by, ql::AsOfPoint::By::Offset);
    auto t = std::get<ql::ShowAsOfDef>(*ql::parse("show topic t as of time '2026-10-13T12:00:00Z';"));
    EXPECT_EQ(t.at.by, ql::AsOfPoint::By::Time);
    EXPECT_FALSE(ql::parse("show topic t as of time 'yesterday';").has_value());
}
TEST(QlCatalogStatementsRegression, DiffForms) {
    auto s = std::get<ql::DiffDef>(*ql::parse("diff schema players version 1 to 3;"));
    EXPECT_EQ(s.scope, ql::DiffDef::Scope::Schema); EXPECT_EQ(s.from, 1); EXPECT_EQ(s.to, 3);
    auto c = std::get<ql::DiffDef>(*ql::parse("diff catalog offset 0 to 42;"));
    EXPECT_EQ(c.scope, ql::DiffDef::Scope::Catalog); EXPECT_EQ(c.to, 42);
}
TEST(QlCatalogStatementsRegression, ExistingStatementsAreUnchanged) {
    EXPECT_TRUE(std::holds_alternative<ql::TopicDef>(*ql::parse("topic t { id int64 } partitions 1 key id;")));
    EXPECT_TRUE(std::holds_alternative<ql::AlterTopicDef>(*ql::parse("alter topic t add column x int32;")));
    EXPECT_TRUE(std::holds_alternative<ql::TopicDef>(*ql::parse("transient topic t { id int64 } partitions 1 key id;")));
}
TEST(QlCatalogStatementsRegression, ReplicasIsInt16WithAParseBound) {
    // F31: int16_t like the wire, the client and the catalog.
    static_assert(std::is_same_v<decltype(ql::TopicDef{}.replicas), int16_t>);
    EXPECT_EQ(std::get<ql::TopicDef>(*ql::parse("topic t partitions 1 replicas 32767;")).replicas, 32767);
    auto r = ql::parse("topic t partitions 1 replicas 32768;");
    ASSERT_FALSE(r.has_value());
    EXPECT_NE(r.error().message.find("replicas"), std::string::npos);
}
TEST(QlCatalogStatementsRegression, TopicCreateConfigIsTheOneProtocolDefinition) {
    static_assert(std::is_same_v<ql::TopicCreateConfig, rune::protocol::TopicConfig>);   // F32
    EXPECT_EQ(ql::applyPlan(std::get<ql::TopicDef>(*ql::parse("topic t partitions 1;"))).config,
              rune::protocol::TopicConfig{}) << "no `with`: exactly the shared defaults";
}
TEST(QlCatalogStatementsRegression, ParseRefusesTrailingStatementsRegression) {
    // F8: a second statement is never silently dropped.
    auto r = ql::parse("topic a partitions 1; topic b partitions 1;");
    ASSERT_FALSE(r.has_value());
    EXPECT_NE(r.error().message.find("end of input"), std::string::npos);
    EXPECT_TRUE(ql::parse("topic a partitions 1;  \n").has_value()) << "trailing blanks are fine";
}
TEST(QlCatalogStatementsRegression, ParseScriptReturnsEveryStatementInOrder) {
    auto s = ql::parseScript("topic events partitions 1;\ntopic multi partitions 3;\ndrop topic events;\n");
    ASSERT_TRUE(s.has_value()) << s.error().error.message;
    ASSERT_EQ(s->size(), 3u);
    EXPECT_EQ(std::get<ql::TopicDef>((*s)[1]).name, "multi");
    EXPECT_TRUE(std::holds_alternative<ql::DropTopicDef>((*s)[2]));
}
TEST(QlCatalogStatementsRegression, ParseScriptErrorNamesTheStatementIndexRegression) {
    auto s = ql::parseScript("topic a partitions 1;\ntopic b partitions;\ntopic c partitions 1;");
    ASSERT_FALSE(s.has_value());
    EXPECT_EQ(s.error().statementIndex, 2u);
    auto t = ql::parseScript("topic a partitions 1; topic b");   // an unterminated tail is an error, not ignored
    ASSERT_FALSE(t.has_value());
    EXPECT_EQ(t.error().statementIndex, 2u);
}

// ── Edge cases ───────────────────────────────────────────────────────────────

// 1. single statement
TEST(QlCatalogStatementsRegression, ParseTrailingGarbageIsTheExactError) {
    for (const char* src : {"drop topic a; drop topic b;", "drop topic a; x", "drop topic a; ;", "drop topic a; 5",
                            "drop topic a; 'str'", "drop topic a; 'unterminated"}) {
        auto r = ql::parse(src);
        ASSERT_FALSE(r.has_value()) << src;
        EXPECT_EQ(r.error().message, "expected end of input after statement 1; use a script (rune ql -f) for several statements") << src;
        EXPECT_GT(r.error().line, 0u);
    }
}
TEST(QlCatalogStatementsRegression, ParseAllowsWhitespaceAndCommentsAfterSemicolon) {
    EXPECT_TRUE(ql::parse("drop topic a; \n\t -- done\n  -- more\n").has_value());
    EXPECT_FALSE(ql::parse("").has_value());
    EXPECT_FALSE(ql::parse("-- only a comment\n").has_value());
}

// 2. parseScript
TEST(QlCatalogStatementsRegression, ParseScriptEmptyAndCommentsOnlyAreEmpty) {
    auto a = ql::parseScript("");
    ASSERT_TRUE(a.has_value()); EXPECT_TRUE(a->empty());
    auto b = ql::parseScript("  \n\n-- nothing here\n   -- at all\n");
    ASSERT_TRUE(b.has_value()); EXPECT_TRUE(b->empty());
}
TEST(QlCatalogStatementsRegression, ParseScriptBlankLinesAndCommentsBetweenStatements) {
    auto s = ql::parseScript("-- first\ndrop topic a;\n\n\n-- second\n\ndrop view b; -- tail\n");
    ASSERT_TRUE(s.has_value()) << s.error().error.message;
    ASSERT_EQ(s->size(), 2u);
    EXPECT_EQ(std::get<ql::DropTopicDef>((*s)[0]).name, "a");
    EXPECT_EQ(std::get<ql::DropViewDef>((*s)[1]).name, "b");
}
TEST(QlCatalogStatementsRegression, ParseScriptMalformedSecondReturnsNoSuccess) {
    auto s = ql::parseScript("drop topic a;\n\ndrop topic;\n");
    ASSERT_FALSE(s.has_value()) << "the valid first statement must not be returned as a success";
    EXPECT_EQ(s.error().statementIndex, 2u);
    EXPECT_EQ(s.error().error.line, 3u);
}
TEST(QlCatalogStatementsRegression, ParseScriptFirstStatementFailingIsIndexOne) {
    auto s = ql::parseScript("drop table t; drop topic a;");
    ASSERT_FALSE(s.has_value());
    EXPECT_EQ(s.error().statementIndex, 1u);
}
TEST(QlCatalogStatementsRegression, ParseScriptTrailingStatementWithoutSemicolonNamesItsIndex) {
    auto s = ql::parseScript("drop topic a; drop topic b; drop topic c");
    ASSERT_FALSE(s.has_value());
    EXPECT_EQ(s.error().statementIndex, 3u);
    auto lone = ql::parseScript(";");   // an empty statement is not a statement
    ASSERT_FALSE(lone.has_value());
    EXPECT_EQ(lone.error().statementIndex, 1u);
}

// 3. replicas
TEST(QlCatalogStatementsRegression, ReplicasBounds) {
    EXPECT_EQ(std::get<ql::TopicDef>(*ql::parse("topic t partitions 1 replicas 0;")).replicas, 0);
    EXPECT_EQ(std::get<ql::TopicDef>(*ql::parse("topic t partitions 1 replicas 32767;")).replicas, 32767);
    for (const char* src : {"topic t partitions 1 replicas -1;", "topic t partitions 1 replicas 32768;",
                            "topic t partitions 1 replicas 99999999999999999999;", "topic t partitions 1 replicas;",
                            "topic t partitions 1 replicas x;"}) {
        auto r = ql::parse(src);
        ASSERT_FALSE(r.has_value()) << src;
        EXPECT_TRUE(mentions(r.error().message, "replicas")) << src << " -> " << r.error().message;
    }
}

// 4. min.cleanable.dirty.ratio
TEST(QlCatalogStatementsRegression, DirtyRatioDecimalToPpm) {
    auto ppm = [](const char* v) -> int64_t {
        auto r = ql::parse(std::string("alter topic t set min.cleanable.dirty.ratio = ") + v + ";");
        if (!r) { ADD_FAILURE() << v << ": " << r.error().message; return std::numeric_limits<int64_t>::min(); }
        return std::get<ql::AlterTopicConfigDef>(*r).settings[0].value;
    };
    EXPECT_EQ(ppm("0"), 0); EXPECT_EQ(ppm("1"), 1'000'000); EXPECT_EQ(ppm("0.25"), 250'000);
    EXPECT_EQ(ppm("0.000001"), 1); EXPECT_EQ(ppm("1.0"), 1'000'000); EXPECT_EQ(ppm("0.5"), 500'000);
}
TEST(QlCatalogStatementsRegression, DirtyRatioOutOfRangeOrMalformedIsRefused) {
    for (const char* v : {"1.5", "-0.1", "abc", "2", "1.000001", "0.0000001", "0.", "'0.5'", ""}) {
        auto r = ql::parse(std::string("alter topic t set min.cleanable.dirty.ratio = ") + v + ";");
        EXPECT_FALSE(r.has_value()) << v;
    }
    // with a space around the dot it is not a decimal
    EXPECT_FALSE(ql::parse("alter topic t set min.cleanable.dirty.ratio = 0 . 5;").has_value());
}

// 5. unknown settings
TEST(QlCatalogStatementsRegression, UnknownOrImmutableSettingMessage) {
    // creation-only key in alter
    auto a = ql::parse("alter topic t set segment.bytes = 1;");
    ASSERT_FALSE(a.has_value());
    EXPECT_EQ(a.error().message, "unknown or immutable topic setting 'segment.bytes'");
    // misspelled key
    auto b = ql::parse("alter topic t set retention.mss = 1;");
    ASSERT_FALSE(b.has_value());
    EXPECT_EQ(b.error().message, "unknown or immutable topic setting 'retention.mss'");
    // `with` also refuses unknown keys, and accepts the creation-only one
    EXPECT_FALSE(ql::parse("topic t partitions 1 with (bogus = 1);").has_value());
    EXPECT_TRUE(ql::parse("topic t partitions 1 with (segment.bytes = 1);").has_value());
}
TEST(QlCatalogStatementsRegression, DuplicateSettingIsRefused) {
    auto a = ql::parse("alter topic t set retention.ms = 1, retention.ms = 2;");
    ASSERT_FALSE(a.has_value());
    EXPECT_TRUE(mentions(a.error().message, "duplicate"));
    EXPECT_FALSE(ql::parse("topic t partitions 1 with (tier = leader, tier = strict);").has_value());
}
TEST(QlCatalogStatementsRegression, SetSettingErrorPaths) {
    EXPECT_FALSE(ql::parse("alter topic t set;").has_value());
    EXPECT_FALSE(ql::parse("alter topic t set retention.ms;").has_value());
    EXPECT_FALSE(ql::parse("alter topic t set retention.ms = ;").has_value());
    EXPECT_FALSE(ql::parse("alter topic t set retention.ms = 99999999999999999999;").has_value());
    EXPECT_FALSE(ql::parse("alter topic t set retention.ms = 1,;").has_value());
    EXPECT_FALSE(ql::parse("alter topic t set retention.ms = 1").has_value());
    // set compatibility / persistence keep their old meaning
    EXPECT_TRUE(std::holds_alternative<ql::AlterTopicDef>(*ql::parse("alter topic t set compatibility forward;")));
    auto pers = ql::parse("alter topic t set persistence = 1;");
    ASSERT_FALSE(pers.has_value());
    EXPECT_TRUE(mentions(pers.error().message, "persistence"));
}

// 6. dotted keys
TEST(QlCatalogStatementsRegression, DottedKeyWithSpacesIsNotTheDottedKey) {
    // The tokenizer splits at blanks; a key is IDENT('.'IDENT)* with NO blanks.
    auto r = ql::parse("alter topic t set retention . ms = 1;");
    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error().message, "unknown or immutable topic setting 'retention'");   // "retention" alone, then " . ms"
    EXPECT_FALSE(ql::parse("alter topic t set retention .ms = 1;").has_value());
    EXPECT_FALSE(ql::parse("alter topic t set retention. ms = 1;").has_value());
    EXPECT_FALSE(ql::parse("alter topic t set retention. = 1;").has_value());
    EXPECT_FALSE(ql::parse("topic t partitions 1 with (durability = all - in - sync);").has_value());
    EXPECT_TRUE(ql::parse("alter topic t set retention.ms = 1;").has_value());
}

// 7. untyped vs typed
TEST(QlCatalogStatementsRegression, TypedNeedsBothBracesAndKey) {
    auto braceNoKey = ql::parse("topic t { id int64 } partitions 1 replicas 2;");
    ASSERT_FALSE(braceNoKey.has_value());
    EXPECT_TRUE(mentions(braceNoKey.error().message, "key"));
    auto keyNoBrace = ql::parse("topic t partitions 1 key id;");
    ASSERT_FALSE(keyNoBrace.has_value());
    EXPECT_TRUE(mentions(keyNoBrace.error().message, "key"));
    EXPECT_FALSE(ql::parse("topic t { } partitions 1 key id;").has_value());
    auto u = ql::parse("topic t partitions 1;");
    ASSERT_TRUE(u.has_value());
    EXPECT_TRUE(std::get<ql::TopicDef>(*u).keyField.empty());
    EXPECT_TRUE(ql::parse("transient topic t partitions 1;").has_value());
    EXPECT_TRUE(ql::parse("topic t { id int64 } partitions 2 replicas 3 key id with (retention.ms = 5);").has_value());
    EXPECT_FALSE(ql::parse("topic t partitions 0;").has_value());
    EXPECT_FALSE(ql::parse("topic t;").has_value());
}

// 8. error paths of each new statement
TEST(QlCatalogStatementsRegression, NewStatementErrorPaths) {
    for (const char* bad : {
             "show history since;", "show history since 5;", "show history since 'nope';", "show history topic;",
             "show history bogus t;", "show history",
             "show schema t as of version;", "show schema t as of version x;", "show schema t as of;",
             "show schema t as of foo 3;", "show schema t of version 3;", "show schema t as of version -1;",
             "show schema as of version 1;", "show schema t as of version 3", "show;", "show table t as of version 1;",
             "show topic t as of time 123;", "show topic t as of time;", "show topic t as of time '2026-13-01T00:00:00Z';",
             "show topic t as of time '2026-10-13 12:00:00';", "show topic t as of time '2026-10-13T12:00:00';",
             "show topic t as of time '2026-10-13T12:00:00",
             "diff schema t version 3;", "diff schema t version 3 to;", "diff schema t 3 to 4;",
             "diff schema version 1 to 2;", "diff catalog offset -1 to 5;", "diff catalog offset 1 to -5;",
             "diff catalog offset 1;", "diff catalog 1 to 2;", "diff table t;", "diff;", "diff schema t version 1 to 2",
             "drop topic;", "drop table t;", "drop view;", "drop topic a b;", "drop topic a", "drop;", "drop column x;"}) {
        auto r = ql::parse(bad);
        EXPECT_FALSE(r.has_value()) << bad;
        if (!r) EXPECT_FALSE(r.error().message.empty()) << bad;
    }
    auto t = ql::parse("show topic t as of time 123;");
    ASSERT_FALSE(t.has_value());
    EXPECT_TRUE(mentions(t.error().message, "quoted string"));
}

// 9. keyword case
TEST(QlCatalogStatementsRegression, KeywordsAreCaseInsensitiveNamesKeepCase) {
    auto h = ql::parse("SHOW HISTORY Topic MyTopic SINCE '2026-10-01T00:00:00Z';");
    ASSERT_TRUE(h.has_value()) << h.error().message;
    EXPECT_EQ(std::get<ql::ShowHistoryDef>(*h).name, "MyTopic");
    EXPECT_EQ(std::get<ql::DropTopicDef>(*ql::parse("Drop Topic MyT;")).name, "MyT");
    auto s = ql::parse("Alter Topic T SET Retention.MS = 5, MIN.Cleanable.Dirty.Ratio = 0.5;");
    ASSERT_TRUE(s.has_value()) << s.error().message;
    EXPECT_EQ(std::get<ql::AlterTopicConfigDef>(*s).name, "T");
    EXPECT_EQ(std::get<ql::AlterTopicConfigDef>(*s).settings[0].key, "retention.ms");
    auto c = ql::parse("TOPIC Ev PARTITIONS 1 REPLICAS 2 WITH (Tier = STRICT, DURABILITY = All-In-Sync);");
    ASSERT_TRUE(c.has_value()) << c.error().message;
    EXPECT_EQ(std::get<ql::TopicDef>(*c).name, "Ev");
    EXPECT_TRUE(ql::parse("DIFF CATALOG OFFSET 1 TO 2;").has_value());
    EXPECT_TRUE(ql::parse("Show Schema S As Of Version 1;").has_value());
    EXPECT_EQ(std::get<ql::TopicDef>(*ql::parse("TRANSIENT TOPIC t { Id int64 } PARTITIONS 1 KEY Id;")).keyField, "Id");
}

// 10. existing behaviour that the refactor could have moved
TEST(QlCatalogStatementsRegression, ExistingGrammarEdgeBehaviourUnchanged) {
    EXPECT_FALSE(ql::parse("alter transient topic t add column x int32;").has_value());
    EXPECT_FALSE(ql::parse("alter topic t add column x int32; alter topic t drop column x;").has_value());
    EXPECT_FALSE(ql::parse("topic t { a int32 } partitions 1 key a").has_value());   // no ';'
    EXPECT_EQ(std::get<ql::AlterTopicDef>(*ql::parse("alter topic t add column a int32, drop column b;")).ops.size(), 2u);
}

// 12. no silent acceptance / errors carry positions
TEST(QlCatalogStatementsRegression, ErrorsCarryPositions) {
    auto r = ql::parse("drop topic a;\nx");
    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error().line, 2u); EXPECT_EQ(r.error().col, 1u);
    auto q = ql::parse("show topic t as of time 'x';");
    ASSERT_FALSE(q.has_value());
    EXPECT_EQ(q.error().line, 1u); EXPECT_GT(q.error().col, 1u);
    auto e = ql::parse("topic t partitions 1 with (retention.ms = 1");   // unclosed paren, EOF
    ASSERT_FALSE(e.has_value());
    EXPECT_TRUE(mentions(e.error().message, "end of input"));
    EXPECT_FALSE(ql::parse("topic t partitions 1 with ();").has_value());
    EXPECT_FALSE(ql::parse("topic t partitions 1 with (cleanup.policy = 1);").has_value());
    EXPECT_FALSE(ql::parse("topic t partitions 1 with (copartition.group = g);").has_value());
    EXPECT_FALSE(ql::parse("topic t partitions 1 with (copartition.group = 'g);").has_value());
}

// ── Fix round 1 ──────────────────────────────────────────────────────────────

// R1: every integer setting is a full int64; ranges belong to the catalog library.
TEST(QlCatalogStatementsRegression, IntegerSettingsAreSignedAndUnnarrowedRegression) {
    auto a = ql::parse("alter topic t set retention.ms = -1, retention.bytes = -1, max.compaction.lag.ms = -1;");
    ASSERT_TRUE(a.has_value()) << a.error().message;
    auto plan = ql::applyConfigPlan(std::get<ql::AlterTopicConfigDef>(*a));
    ASSERT_TRUE(plan.has_value());
    const std::vector<std::pair<uint8_t, int64_t>> want{{1, -1}, {2, -1}, {6, -1}};
    EXPECT_EQ(plan->changes, want);

    auto c = ql::parse("topic t partitions 1 with (retention.ms = -1, segment.bytes = -5);");
    ASSERT_TRUE(c.has_value()) << c.error().message;
    const auto cfg = ql::applyPlan(std::get<ql::TopicDef>(*c)).config;
    EXPECT_EQ(cfg.retentionMs, -1);
    EXPECT_EQ(cfg.segmentRollBytes, -5);

    auto n = ql::parse("alter topic t set delete.retention.ms = -7;");
    ASSERT_TRUE(n.has_value()) << n.error().message;
    EXPECT_EQ(std::get<ql::AlterTopicConfigDef>(*n).settings[0].value, -7);

    auto lo = ql::parse("alter topic t set retention.ms = -9223372036854775808;");
    ASSERT_TRUE(lo.has_value()) << lo.error().message;
    EXPECT_EQ(std::get<ql::AlterTopicConfigDef>(*lo).settings[0].value, std::numeric_limits<int64_t>::min());
    auto hi = ql::parse("alter topic t set retention.ms = 9223372036854775807;");
    ASSERT_TRUE(hi.has_value()) << hi.error().message;
    EXPECT_EQ(std::get<ql::AlterTopicConfigDef>(*hi).settings[0].value, std::numeric_limits<int64_t>::max());

    for (const char* v : {"-9223372036854775809", "9223372036854775808", "- 1", "-", "-x"}) {
        auto r = ql::parse(std::string("alter topic t set retention.ms = ") + v + ";");
        ASSERT_FALSE(r.has_value()) << v;
        EXPECT_TRUE(mentions(r.error().message, "retention.ms")) << v << " -> " << r.error().message;
    }
    EXPECT_FALSE(ql::parse("alter topic t set retention.ms = --1;").has_value()) << "-- starts a comment";
}

// R2: an accepted time names one real UTC second.
TEST(QlCatalogStatementsRegression, TimeIsARealCalendarInstantRegression) {
    auto stmt = [](const char* t) { return std::string("show topic t as of time '") + t + "';"; };
    for (const char* ok : {"2028-02-29T00:00:00Z", "2000-02-29T00:00:00Z", "2026-04-30T23:59:59Z",
                           "2026-12-31T23:59:59Z", "2026-01-31T00:00:00Z"})
        EXPECT_TRUE(ql::parse(stmt(ok)).has_value()) << ok;
    for (const char* bad : {"2026-02-29T00:00:00Z", "2100-02-29T00:00:00Z", "2026-02-30T00:00:00Z",
                            "2026-02-31T00:00:00Z", "2026-04-31T00:00:00Z", "2026-06-31T00:00:00Z",
                            "2026-09-31T00:00:00Z", "2026-11-31T00:00:00Z", "2026-00-10T00:00:00Z"}) {
        auto r = ql::parse(stmt(bad));
        ASSERT_FALSE(r.has_value()) << bad;
        EXPECT_TRUE(mentions(r.error().message, "invalid time")) << bad;
    }
    EXPECT_FALSE(ql::parse("show history since '2026-02-30T00:00:00Z';").has_value());
}

// R3: every row of the settings table.
TEST(QlCatalogStatementsRegression, EveryAlterableKeyMapsToItsWireKeyRegression) {
    static_assert(static_cast<uint8_t>(rune::protocol::ConfigKey::MaxCompactionLagMs) == 6);
    auto r = ql::parse("alter topic t set retention.ms = 11, retention.bytes = 22, delete.retention.ms = 33, "
                       "min.cleanable.dirty.ratio = 0.125, min.compaction.lag.ms = 55, max.compaction.lag.ms = 66;");
    ASSERT_TRUE(r.has_value()) << r.error().message;
    auto plan = ql::applyConfigPlan(std::get<ql::AlterTopicConfigDef>(*r));
    ASSERT_TRUE(plan.has_value());
    const std::vector<std::pair<uint8_t, int64_t>> want{{1, 11}, {2, 22}, {3, 33}, {4, 125000}, {5, 55}, {6, 66}};
    EXPECT_EQ(plan->changes, want);
}
TEST(QlCatalogStatementsRegression, EveryCreationSettingLandsInItsConfigFieldRegression) {
    auto r = ql::parse("topic t partitions 1 with (retention.ms = 11, retention.bytes = 22, delete.retention.ms = 33, "
                       "min.cleanable.dirty.ratio = 0.125, min.compaction.lag.ms = 55, max.compaction.lag.ms = 66, "
                       "cleanup.policy = compact_delete, segment.bytes = 77, retention.check.ms = 88, "
                       "file.delete.delay.ms = 99, durability = all-in-sync, tier = strict, "
                       "copartition.group = 'grp', partition.key = 'pk');");
    ASSERT_TRUE(r.has_value()) << r.error().message;
    const auto c = ql::applyPlan(std::get<ql::TopicDef>(*r)).config;
    EXPECT_EQ(c.retentionMs, 11); EXPECT_EQ(c.retentionBytes, 22); EXPECT_EQ(c.deleteRetentionMs, 33);
    EXPECT_EQ(c.minCleanableDirtyRatioPpm, 125000); EXPECT_EQ(c.minCompactionLagMs, 55);
    EXPECT_EQ(c.maxCompactionLagMs, 66); EXPECT_EQ(c.cleanupPolicy, 2); EXPECT_EQ(c.segmentRollBytes, 77);
    EXPECT_EQ(c.retentionCheckMs, 88); EXPECT_EQ(c.fileDeleteDelayMs, 99); EXPECT_EQ(c.durability, 1);
    EXPECT_EQ(c.durabilityTier, 2); EXPECT_EQ(c.coPartitionGroup, "grp"); EXPECT_EQ(c.partitionKey, "pk");
}
TEST(QlCatalogStatementsRegression, EveryEnumWordHasItsWireByteRegression) {
    auto a = ql::parse("topic t partitions 1 with (cleanup.policy = delete, durability = majority, tier = majority);");
    ASSERT_TRUE(a.has_value()) << a.error().message;
    const auto& w = std::get<ql::TopicDef>(*a).with;
    ASSERT_EQ(w.size(), 3u);
    EXPECT_EQ(w[0].value, 0); EXPECT_EQ(w[1].value, 0); EXPECT_EQ(w[2].value, 1);
    EXPECT_EQ(w[0].text, "delete"); EXPECT_EQ(w[1].text, "majority"); EXPECT_EQ(w[2].text, "majority");
    auto b = ql::parse("topic t partitions 1 with (cleanup.policy = compact, tier = leader);");
    ASSERT_TRUE(b.has_value()) << b.error().message;
    EXPECT_EQ(std::get<ql::TopicDef>(*b).with[0].value, 1);
    EXPECT_EQ(std::get<ql::TopicDef>(*b).with[1].value, 0);
    auto c = ql::parse("topic t partitions 1 with (durability = ALL-IN-SYNC);");
    ASSERT_TRUE(c.has_value()) << c.error().message;
    EXPECT_EQ(std::get<ql::TopicDef>(*c).with[0].value, 1);
    EXPECT_EQ(std::get<ql::TopicDef>(*c).with[0].text, "all-in-sync");
}
TEST(QlCatalogStatementsRegression, ApplyConfigPlanRefusesHandBuiltBadKeysRegression) {
    ql::AlterTopicConfigDef d{"t", {{"segment.bytes", 1, ""}}};
    auto r = ql::applyConfigPlan(d);
    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error().message, "unknown or immutable topic setting 'segment.bytes'");
    ql::AlterTopicConfigDef e{"t", {{"bogus", 1, ""}}};
    auto q = ql::applyConfigPlan(e);
    ASSERT_FALSE(q.has_value());
    EXPECT_EQ(q.error().message, "unknown or immutable topic setting 'bogus'");
}

// R4: characters no token starts with are never names; digits glued to letters are one bad token.
TEST(QlCatalogStatementsRegression, NonIdentifierCharactersAreNeverNamesRegression) {
    auto at = ql::parse("drop topic @;");
    ASSERT_FALSE(at.has_value());
    EXPECT_TRUE(mentions(at.error().message, "'@'")) << at.error().message;
    for (const char* bad : {"drop topic $;", "drop view #;", "topic t { @ int64 } partitions 1 key @;",
                            "topic t { id int64 } partitions 1 key $;", "alter topic t drop column @;",
                            "show topic % as of version 1;", "show history topic !;",
                            "diff schema & version 1 to 2;", "diff catalog offset 1to 2;",
                            "topic t partitions 1 replicas 2key;"})
        EXPECT_FALSE(ql::parse(bad).has_value()) << bad;
    auto e = ql::parse("drop topic \xC3\xA9;");
    ASSERT_FALSE(e.has_value());
    EXPECT_TRUE(mentions(e.error().message, "0xC3")) << e.error().message;
    auto g = ql::parse("topic t partitions 3replicas 2;");
    ASSERT_FALSE(g.has_value());
    EXPECT_TRUE(mentions(g.error().message, "3replicas")) << g.error().message;
    for (const char* ok : {"drop topic a_1;", "topic t { x1 int64, _y string(10) } partitions 1 key x1;",
                           "alter topic t set min.cleanable.dirty.ratio = 0.25;", "topic t partitions 10;"})
        EXPECT_TRUE(ql::parse(ok).has_value()) << ok;
}
