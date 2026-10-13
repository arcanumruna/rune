// Catalog history (spec §7): history / as-of / diff over a recorded log, and the .rcat file.
#include <rune/catalog/CatalogFile.h>
#include <rune/catalog/CatalogFingerprint.h>
#include <rune/catalog/CatalogQueries.h>
#include <rune/catalog/CatalogReplayer.h>
#include <rune/catalog/testing/GoldenScript.h>
#include <rune/codec/Descriptor.h>
#include <rune/codec/Serialize.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <set>
#include <sstream>
#include <string>
#include <string_view>
#include <vector>

namespace rc = rune::catalog;
namespace cd = rune::codec;

namespace {
std::vector<rc::RecordedEvent> recorded() {
    std::vector<rc::RecordedEvent> out;
    int64_t o = 0;
    for (auto& e : rc::testing::goldenScriptV1()) out.push_back({o++, std::move(e)});
    return out;
}
std::vector<int64_t> offsetsOf(const std::vector<const rc::RecordedEvent*>& h) {
    std::vector<int64_t> out;
    for (auto* r : h) out.push_back(r->offset);
    return out;
}
} // namespace

// ── history ──────────────────────────────────────────────────────────────────
TEST(CatalogQueriesRegression, HistoryOfAReusedNameShowsBothObjectsDistinctly) {
    const auto log = recorded();
    auto h = rc::history(log, {.type = rc::ObjectType::Topic, .name = "players"});
    ASSERT_FALSE(h.empty());
    std::set<uint64_t> ids;
    for (auto* r : h) ids.insert(r->event.objectId);
    EXPECT_EQ(ids.size(), 2u) << "drop + reuse = two objects, never conflated";
    EXPECT_EQ(h.front()->event.kind, rc::EventKind::CreateTopic);
    // CreateTopic, AlterSchema x2, SetCompatibility, DropTopic — then the new CreateTopic.
    EXPECT_EQ(offsetsOf(h), (std::vector<int64_t>{0, 4, 5, 6, 16, 17}));
}

TEST(CatalogQueriesRegression, SchemaHistoryIsTheSchemaKindsOfThatTopic) {
    const auto log = recorded();
    auto h = rc::history(log, {.type = rc::ObjectType::Schema, .name = "players"});
    // CreateTopic(with schema), AlterSchema x2, SetCompatibility — of the FIRST players.
    ASSERT_EQ(h.size(), 4u);
    EXPECT_EQ(h[3]->event.kind, rc::EventKind::SetCompatibility);
    // A CreateSchema on an untyped topic starts that topic's schema history.
    auto p = rc::history(log, {.type = rc::ObjectType::Schema, .name = "presence"});
    ASSERT_FALSE(p.empty());
    EXPECT_EQ(p.front()->event.kind, rc::EventKind::CreateSchema);
    for (auto* r : p) EXPECT_NE(r->event.kind, rc::EventKind::CreateTopic) << "both presence topics were created untyped";
}

TEST(CatalogQueriesRegression, ViewHistoryIsTheViewKindsAndSeesAReusedViewName) {
    const auto log = recorded();
    auto h = rc::history(log, {.type = rc::ObjectType::View, .name = "ev_view"});
    std::set<uint64_t> ids;
    for (auto* r : h) {
        ids.insert(r->event.objectId);
        EXPECT_TRUE(r->event.kind == rc::EventKind::CreateView || r->event.kind == rc::EventKind::AlterView ||
                    r->event.kind == rc::EventKind::DropView);
    }
    EXPECT_EQ(ids.size(), 2u);
    EXPECT_TRUE(rc::history(log, {.type = rc::ObjectType::Topic, .name = "ev_view"}).empty()) << "a view is not a topic";
}

TEST(CatalogQueriesRegression, AnUnfilteredHistoryIsTheWholeLogInOffsetOrder) {
    const auto log = recorded();
    auto h = rc::history(log, {});
    ASSERT_EQ(h.size(), log.size());
    for (std::size_t i = 0; i < h.size(); ++i) EXPECT_EQ(h[i], &log[i]);
}

TEST(CatalogQueriesRegression, SinceFiltersOnTimestamp) {
    const auto log = recorded();
    auto h = rc::history(log, {.sinceMs = log[10].event.timestampMs});
    EXPECT_EQ(h.size(), log.size() - 10);
}

// ── as-of ────────────────────────────────────────────────────────────────────
TEST(CatalogQueriesRegression, AsOfOffsetTimeAndVersionAgree) {
    const auto log = recorded();
    auto byOffset = rc::stateAsOf(log, {.by = rc::AsOf::By::Offset, .value = 5});
    auto byTime   = rc::stateAsOf(log, {.by = rc::AsOf::By::TimeMs, .value = log[5].event.timestampMs});
    auto byVer    = rc::stateAsOf(log, {.by = rc::AsOf::By::Version, .value = 3,
                                        .type = rc::ObjectType::Schema, .name = "players"});
    ASSERT_TRUE(byOffset && byTime && byVer);
    EXPECT_EQ(byOffset->digest(), byTime->digest());
    EXPECT_EQ(byOffset->digest(), byVer->digest()) << "event 5 produced players schema version 3";
    EXPECT_EQ(byOffset->topicByName("players")->schema->schemaVersion, 3);
    EXPECT_EQ(byOffset->appliedOffset(), 5);
}

TEST(CatalogQueriesRegression, AsOfTimeBetweenEventsStopsAtTheLastEarlierOne) {
    const auto log = recorded();
    auto s = rc::stateAsOf(log, {.by = rc::AsOf::By::TimeMs, .value = log[7].event.timestampMs + 999});
    ASSERT_TRUE(s.has_value());
    EXPECT_EQ(s->appliedOffset(), 7);
    auto before = rc::stateAsOf(log, {.by = rc::AsOf::By::TimeMs, .value = log[0].event.timestampMs - 1});
    ASSERT_TRUE(before.has_value());
    EXPECT_EQ(before->appliedOffset(), -1) << "as of before the first event: the empty catalog";
}

TEST(CatalogQueriesRegression, AsOfTimeStopsAtTheFirstLaterEventEvenIfAnEarlierOneFollows) {
    // Time is informational, the offset is the order: a later event with an EARLIER
    // timestamp (a clock step back on a new leader) is not reached past a later one.
    auto log = recorded();
    log[3].event.timestampMs = log[0].event.timestampMs;   // only the timestamp: fingerprints don't cover it
    auto s = rc::stateAsOf(log, {.by = rc::AsOf::By::TimeMs, .value = log[1].event.timestampMs});
    ASSERT_TRUE(s.has_value());
    EXPECT_EQ(s->appliedOffset(), 1);
}

TEST(CatalogQueriesRegression, AsOfTopicAndViewVersionsSelectTheLatestObjectOfThatName) {
    const auto log = recorded();
    auto t = rc::stateAsOf(log, {.by = rc::AsOf::By::Version, .value = 1, .type = rc::ObjectType::Topic, .name = "players"});
    ASSERT_TRUE(t.has_value()) << t.error().detail;
    EXPECT_EQ(t->appliedOffset(), 17) << "version 1 of the LATEST 'players' (the reuse at offset 17)";
    auto ev = rc::stateAsOf(log, {.by = rc::AsOf::By::Version, .value = 3, .type = rc::ObjectType::Topic, .name = "events"});
    ASSERT_TRUE(ev.has_value()) << ev.error().detail;
    EXPECT_EQ(ev->appliedOffset(), 7);
    EXPECT_EQ(ev->topicByName("events")->config.retentionMs, 60'000);
    auto v = rc::stateAsOf(log, {.by = rc::AsOf::By::Version, .value = 2, .type = rc::ObjectType::View, .name = "top_players"});
    ASSERT_TRUE(v.has_value()) << v.error().detail;
    EXPECT_EQ(v->appliedOffset(), 10);
    EXPECT_EQ(v->viewByName("top_players")->def.viewTopicId, 4);
}

TEST(CatalogQueriesRegression, AsOfAMissingVersionIsAnExplicitError) {
    const auto log = recorded();
    auto s = rc::stateAsOf(log, {.by = rc::AsOf::By::Version, .value = 99, .type = rc::ObjectType::Topic, .name = "events"});
    ASSERT_FALSE(s.has_value());
    EXPECT_EQ(s.error().code, rc::ReplayError::Code::Apply);
    EXPECT_NE(s.error().detail.find("no version 99"), std::string::npos);
    auto none = rc::stateAsOf(log, {.by = rc::AsOf::By::Version, .value = 1, .type = rc::ObjectType::View, .name = "nope"});
    ASSERT_FALSE(none.has_value());
    EXPECT_NE(none.error().detail.find("no version 1 of view 'nope'"), std::string::npos) << none.error().detail;
}

TEST(CatalogQueriesRegression, AsOfPastABadEventSurfacesThatEventsError) {
    auto log = recorded();
    log[4].event.fingerprint ^= 1;
    auto before = rc::stateAsOf(log, {.by = rc::AsOf::By::Offset, .value = 3});
    ASSERT_TRUE(before.has_value()) << "as of a point before the bad event replays fine";
    auto past = rc::stateAsOf(log, {.by = rc::AsOf::By::Offset, .value = 9});
    ASSERT_FALSE(past.has_value());
    EXPECT_EQ(past.error().offset, 4);
    EXPECT_EQ(past.error().apply->code, rc::ApplyError::Code::FingerprintMismatch);
}

TEST(CatalogQueriesRegression, AsOfARecordedLogWithAGapIsAnError) {
    auto log = recorded();
    log.erase(log.begin() + 2);
    auto s = rc::stateAsOf(log, {.by = rc::AsOf::By::Offset, .value = 5});
    ASSERT_FALSE(s.has_value());
    EXPECT_EQ(s.error().code, rc::ReplayError::Code::Gap);
    EXPECT_EQ(s.error().offset, 3);
}

// ── diff ─────────────────────────────────────────────────────────────────────
TEST(CatalogQueriesRegression, DiffReportsAddedRemovedAndChangedFields) {
    const auto log = recorded();
    auto a = rc::stateAsOf(log, {.by = rc::AsOf::By::Offset, .value = 6});
    auto b = rc::stateAsOf(log, {.by = rc::AsOf::By::Offset, .value = 17});
    ASSERT_TRUE(a && b);
    const auto d = rc::diff(*a, *b);
    auto find = [&](std::string_view n) { for (auto& e : d) if (e.name == n) return &e; return (const rc::DiffEntry*)nullptr; };
    ASSERT_NE(find("events"), nullptr);
    EXPECT_EQ(find("events")->change, rc::DiffEntry::Change::Changed);
    EXPECT_NE(std::find(find("events")->fields.begin(), find("events")->fields.end(), "config.retentionMs"),
              find("events")->fields.end());
    EXPECT_TRUE(rc::diff(*b, *b).empty());
}

TEST(CatalogQueriesRegression, DiffPairsByTypeAndNameAndNamesEveryChange) {
    const auto log = recorded();
    auto a = rc::stateAsOf(log, {.by = rc::AsOf::By::Offset, .value = 9});    // top_players v1 live
    auto b = rc::stateAsOf(log, {.by = rc::AsOf::By::Offset, .value = 17});
    ASSERT_TRUE(a && b);
    const auto d = rc::diff(*a, *b);
    auto find = [&](rc::ObjectType t, std::string_view n) {
        for (auto& e : d) if (e.type == t && e.name == n) return &e;
        return (const rc::DiffEntry*)nullptr;
    };
    const auto* tp = find(rc::ObjectType::View, "top_players");
    ASSERT_NE(tp, nullptr);
    EXPECT_EQ(tp->change, rc::DiffEntry::Change::Removed);
    EXPECT_NE(tp->fingerprintA, 0u);
    EXPECT_EQ(tp->fingerprintB, 0u);
    const auto* players = find(rc::ObjectType::Topic, "players");
    ASSERT_NE(players, nullptr);
    EXPECT_EQ(players->change, rc::DiffEntry::Change::Changed) << "same (type, name), a different definition";
    EXPECT_NE(std::find(players->fields.begin(), players->fields.end(), "schema"), players->fields.end());
    EXPECT_NE(std::find(players->fields.begin(), players->fields.end(), "partitions"), players->fields.end());
    EXPECT_EQ(find(rc::ObjectType::Topic, "presence"), nullptr)
        << "the reused presence has the same definition: no change (fingerprints are id-free)";
    auto back = rc::diff(*b, *a);
    const rc::DiffEntry* added = nullptr;
    for (auto& e : back) if (e.name == "top_players") added = &e;
    ASSERT_NE(added, nullptr);
    EXPECT_EQ(added->change, rc::DiffEntry::Change::Added);
    for (const auto& e : d) EXPECT_TRUE(e.change != rc::DiffEntry::Change::Changed || !e.fields.empty()) << e.name;
    for (std::size_t i = 1; i < d.size(); ++i)
        EXPECT_TRUE(d[i - 1].type < d[i].type || (d[i - 1].type == d[i].type && d[i - 1].name < d[i].name))
            << "deterministic (type, name) order";
}

namespace {
std::vector<uint8_t> desc(std::initializer_list<std::pair<const char*, cd::PrimType>> fields, uint16_t version = 1) {
    cd::DescriptorBuilder b("d", version);
    for (auto& [n, t] : fields) b.add(n, t);
    return cd::serialize(b.build());
}
template <class T>
struct FieldRow { const char* expected; std::function<void(T&)> mutate; };

template <class T>
void expectEveryFingerprintedFieldIsNamed(const T& base, const std::vector<FieldRow<T>>& rows) {
    EXPECT_TRUE(rc::changedFields(base, base).empty());
    for (const auto& r : rows) {
        T m = base;
        r.mutate(m);
        ASSERT_NE(rc::fingerprintOf(m), rc::fingerprintOf(base)) << r.expected << ": the row must change the definition";
        const auto f = rc::changedFields(base, m);
        EXPECT_EQ(f, std::vector<std::string>{r.expected}) << r.expected;
    }
}
} // namespace

TEST(CatalogQueriesRegression, EveryTopicDefinitionFieldHasADiffName) {
    rc::TopicObject t{.objectId = 1, .name = "t", .version = 2, .topicId = 0, .partitions = 2, .replicationFactor = 3,
                      .persistence = rc::Persistence::Disk, .config = {},
                      .schema = rc::SchemaState{.keyField = "id", .validate = true,
                                                .evolvingBlob = desc({{"id", cd::PrimType::Int64}, {"hp", cd::PrimType::Int32}}),
                                                .schemaVersion = 1, .nextTag = 2, .compat = cd::CompatMode::Backward}};
    using T = rc::TopicObject;
    expectEveryFingerprintedFieldIsNamed<T>(t, {
        {"version", [](T& x) { x.version = 9; }},
        {"partitions", [](T& x) { x.partitions = 9; }},
        {"replicationFactor", [](T& x) { x.replicationFactor = 1; }},
        {"persistence", [](T& x) { x.persistence = rc::Persistence::Transient; }},
        {"config.cleanupPolicy", [](T& x) { x.config.cleanupPolicy = 1; }},
        {"config.retentionMs", [](T& x) { x.config.retentionMs = 5; }},
        {"config.retentionBytes", [](T& x) { x.config.retentionBytes = 5; }},
        {"config.deleteRetentionMs", [](T& x) { x.config.deleteRetentionMs = 5; }},
        {"config.minCleanableDirtyRatioPpm", [](T& x) { x.config.minCleanableDirtyRatioPpm = 5; }},
        {"config.segmentRollBytes", [](T& x) { x.config.segmentRollBytes = 5; }},
        {"config.minCompactionLagMs", [](T& x) { x.config.minCompactionLagMs = 5; }},
        {"config.maxCompactionLagMs", [](T& x) { x.config.maxCompactionLagMs = 5; }},
        {"config.retentionCheckMs", [](T& x) { x.config.retentionCheckMs = 5; }},
        {"config.fileDeleteDelayMs", [](T& x) { x.config.fileDeleteDelayMs = 5; }},
        {"config.durability", [](T& x) { x.config.durability = 1; }},
        {"config.durabilityTier", [](T& x) { x.config.durabilityTier = 2; }},
        {"config.coPartitionGroup", [](T& x) { x.config.coPartitionGroup = "g"; }},
        {"config.partitionKey", [](T& x) { x.config.partitionKey = "k"; }},
        {"schema", [](T& x) { x.schema.reset(); }},
        {"schema.keyField", [](T& x) { x.schema->keyField = "hp"; }},
        {"schema.validate", [](T& x) { x.schema->validate = false; }},
        {"schema.fields", [](T& x) { x.schema->evolvingBlob = desc({{"id", cd::PrimType::Int64}, {"mp", cd::PrimType::Int32}}); }},
        {"schema.fields", [](T& x) { x.schema->nextTag = 7; }},
        {"schema.version", [](T& x) { x.schema->schemaVersion = 2; }},
        {"schema.compat", [](T& x) { x.schema->compat = cd::CompatMode::Forward; }},
        {"schema.descriptor", [](T& x) {
             x.schema->evolvingBlob = desc({{"id", cd::PrimType::Int64}, {"hp", cd::PrimType::Int32}}, /*version=*/5); }},
    });
}

TEST(CatalogQueriesRegression, EveryViewDefinitionFieldHasADiffName) {
    rc::ViewObject v{.objectId = 4, .name = "v", .version = 1,
                     .def = rc::ViewDef{.viewTopicId = 3, .sinkPartitions = 1, .persistence = rc::Persistence::Disk,
                                        .keyField = "id", .sourceNames = {"src"}, .normalizedPlanBlob = {1, 2},
                                        .compiledPlanBlob = {9},
                                        .outputDescriptorBlob = desc({{"id", cd::PrimType::Int64}})}};
    using V = rc::ViewObject;
    expectEveryFingerprintedFieldIsNamed<V>(v, {
        {"version", [](V& x) { x.version = 2; }},
        {"view.sinkPartitions", [](V& x) { x.def.sinkPartitions = 4; }},
        {"view.persistence", [](V& x) { x.def.persistence = rc::Persistence::Transient; }},
        {"view.keyField", [](V& x) { x.def.keyField = ""; }},
        {"view.sources", [](V& x) { x.def.sourceNames = {"src", "other"}; }},
        {"view.plan", [](V& x) { x.def.normalizedPlanBlob = {3}; }},
        {"view.output", [](V& x) { x.def.outputDescriptorBlob = desc({{"id", cd::PrimType::Int64}, {"n", cd::PrimType::Int32}}); }},
    });
    // Ids are not definitions (ruling 11): a different sink or compiled plan is no change.
    V other = v;
    other.def.viewTopicId = 77;
    other.def.compiledPlanBlob = {1, 2, 3};
    other.objectId = 99;
    EXPECT_TRUE(rc::changedFields(v, other).empty());
}

// ── .rcat ────────────────────────────────────────────────────────────────────
TEST(CatalogQueriesRegression, RcatRoundTripsAndRefusesGarbage) {
    const auto log = recorded();
    std::stringstream ss;
    rc::writeRcatHeader(ss);
    for (const auto& r : log) rc::writeRcatRecord(ss, r.offset, rc::encode(r.event).value());
    auto back = rc::readRcat(ss);
    ASSERT_TRUE(back.has_value()) << back.error();
    ASSERT_EQ(back->size(), log.size());
    EXPECT_EQ((*back)[7].first, 7);
    for (std::size_t i = 0; i < log.size(); ++i) EXPECT_EQ((*back)[i].second, rc::encode(log[i].event).value());
    std::stringstream bad("NOPE");
    EXPECT_FALSE(rc::readRcat(bad).has_value());
}

namespace {
std::string rcatBytes(const std::vector<std::pair<int64_t, std::vector<uint8_t>>>& records) {
    std::stringstream ss;
    rc::writeRcatHeader(ss);
    for (const auto& [o, b] : records) rc::writeRcatRecord(ss, o, b);
    return ss.str();
}
std::string readError(const std::string& bytes) {
    std::stringstream in(bytes);
    auto r = rc::readRcat(in);
    return r.has_value() ? std::string("<accepted>") : r.error();
}
} // namespace

TEST(CatalogQueriesRegression, RcatLayoutIsFrozen) {
    const std::string b = rcatBytes({{0x0102, {0xAA, 0xBB}}});
    const std::string expected = std::string("RCAT") + '\x01' + '\x00' +                     // magic, u16 version 1
                                 std::string("\x02\x01\x00\x00\x00\x00\x00\x00", 8) +       // i64 offset LE
                                 std::string("\x02\x00\x00\x00", 4) + "\xAA\xBB";            // u32 len LE, bytes
    EXPECT_EQ(b, expected);
    std::stringstream empty;
    rc::writeRcatHeader(empty);
    auto r = rc::readRcat(empty);
    ASSERT_TRUE(r.has_value()) << r.error();
    EXPECT_TRUE(r->empty()) << "a header alone is an empty log";
}

TEST(CatalogQueriesRegression, RcatRefusalsNameTheBytePosition) {
    const std::string good = rcatBytes({{0, {1, 2, 3}}, {1, {4}}});
    EXPECT_NE(readError("").find("byte 0"), std::string::npos) << readError("");
    EXPECT_NE(readError("RCA").find("byte 0"), std::string::npos) << readError("RCA");
    EXPECT_NE(readError(std::string("XCAT\x01\x00", 6)).find("bad magic at byte 0"), std::string::npos);
    {
        std::string v = good; v[4] = 2;
        const auto e = readError(v);
        EXPECT_NE(e.find("file version 2"), std::string::npos) << e;
        EXPECT_NE(e.find("byte 4"), std::string::npos) << e;
    }
    {
        const auto e = readError(good.substr(0, 6 + 5));   // record header cut inside the offset
        EXPECT_NE(e.find("truncated"), std::string::npos) << e;
        EXPECT_NE(e.find("byte 6"), std::string::npos) << e;
    }
    {
        const auto e = readError(good.substr(0, good.size() - 1));   // last record's body short by one
        EXPECT_NE(e.find("truncated"), std::string::npos) << e;
        EXPECT_NE(e.find("byte " + std::to_string(6 + 12 + 3 + 12)), std::string::npos) << e;   // where the byte is missing
    }
    {
        const auto e = readError(rcatBytes({{5, {1}}, {5, {2}}}));
        EXPECT_NE(e.find("offset 5 does not follow offset 5"), std::string::npos) << e;
        EXPECT_NE(e.find("byte " + std::to_string(6 + 12 + 1)), std::string::npos) << e;
    }
    EXPECT_NE(readError(rcatBytes({{5, {1}}, {4, {2}}})).find("does not follow"), std::string::npos);
    EXPECT_EQ(readError(rcatBytes({{3, {1}}, {9, {2}}})), "<accepted>") << "increasing is enough; replay judges gaps";
}

TEST(CatalogQueriesRegression, RcatHugeDeclaredLengthIsTruncatedNotAnAllocation) {
    std::string b = rcatBytes({{0, {}}});
    b[6 + 8 + 3] = '\x7F';   // declared length ~2 GiB, no bytes follow
    const auto e = readError(b);
    EXPECT_NE(e.find("truncated"), std::string::npos) << e;
}
