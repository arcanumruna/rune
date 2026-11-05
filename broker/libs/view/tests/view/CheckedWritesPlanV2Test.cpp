// libs/view/tests/view/CheckedWritesPlanV2Test.cpp
//
// Compiled plan v2 (key components, unique, constraints, Unicode pin) and the view compiler's key modes and identity rule.
#include <gtest/gtest.h>
#include <rune/codec/Descriptor.h>
#include <rune/codec/UnicodeLower.h>
#include <rune/view/PlanCompiler.h>

namespace {

using rune::codec::PrimType;
using namespace rune::view;
using Kind = CompileErrorKind;

constexpr int32_t kViewTopic = 0x40000007;

rune::codec::Descriptor characterSource() {
    return rune::codec::DescriptorBuilder("character", 1)
        .add("characterId", PrimType::UInt64, 1, true).add("userId", PrimType::UInt64)
        .addFixedStr("name", 16).addFixedStr("tag", 80).add("provider", PrimType::UInt16)
        .addFixedStr("subject", 64).add("gender", PrimType::UInt16).add("lastPlayed", PrimType::Int64).build();
}

rune::codec::Descriptor deviceSource() {
    return rune::codec::DescriptorBuilder("device", 1)
        .add("eventId", PrimType::UInt64, 1, true).add("userId", PrimType::UInt64).addFixedStr("deviceFp", 32)
        .add("platform", PrimType::Enum).add("lastSeen", PrimType::Int64).add("bytes", PrimType::UInt64).build();
}

AuthoredField replaceOf(const rune::codec::Descriptor& d, const std::string& name) {
    return {name, d.find(name)->type, 0, name, MergeRule::Replace};
}

ViewPlan projection(const rune::codec::Descriptor& d, std::vector<KeyComponent> key, std::initializer_list<const char*> select,
                    const char* sourceKey = "characterId") {
    ViewPlan v;
    v.viewName = "char_view";
    v.sources = {{5, sourceKey}};
    v.keyComponents = std::move(key);
    for (const char* s : select) v.output.push_back(replaceOf(d, s));
    return v;
}

ViewPlan aggregate(std::vector<KeyComponent> key, const char* sourceKey = "userId") {
    ViewPlan v;
    v.viewName = "device_history";
    v.sources = {{5, sourceKey}};
    v.groupBy = key;
    v.keyComponents = std::move(key);
    v.output = {{"uses", PrimType::Int64, 0, "", MergeRule::CountAll}, {"lastSeen", PrimType::Int64, 0, "lastSeen", MergeRule::Max}};
    return v;
}

std::expected<CompileResult, CompileError> compileOver(const ViewPlan& v, const rune::codec::Descriptor& d) {
    const rune::codec::Descriptor* srcs[] = {&d};
    return compileView(v, srcs, kViewTopic);
}

std::string slotName(const CompileResult& r, size_t slot) { return r.outputDescriptor.fields[slot].name; }

void expectRefused(const std::expected<CompileResult, CompileError>& r, Kind kind, const std::string& text) {
    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error().kind, kind) << r.error().message;
    EXPECT_NE(r.error().message.find(text), std::string::npos) << r.error().message;
}

// Compiled bytes captured from the code before the v2 change (a v1 plan keeps its exact bytes).
const std::vector<uint8_t> kLegacyShapeGolden = {
    0x01, 0x08, 0x00, 0x00, 0x00, 0x63, 0x68, 0x61, 0x72, 0x5f, 0x61, 0x6c, 0x6c, 0x07, 0x00, 0x00, 0x40, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x01, 0x00, 0x00, 0x00, 0x05, 0x00, 0x00, 0x00, 0x8a, 0x4d, 0x0f, 0x6c, 0xcb, 0x8c, 0x47, 0x06, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x04, 0x00, 0x00, 0x01, 0x00, 0x00, 0x02, 0x00, 0x0a, 0x00, 0x00, 0x02, 0x00, 0x00, 0x03, 0x00,
    0x0a, 0x00, 0x00, 0x03, 0x00, 0x00, 0x04, 0x00, 0x02, 0x00, 0x00, 0x04, 0x00, 0x00, 0x05, 0x00, 0x0a, 0x00, 0x00, 0x05, 0x00, 0x00, 0x06,
    0x00, 0x02, 0x00, 0x00, 0x06, 0x00, 0x00, 0x07, 0x00, 0x01, 0x00, 0x00, 0x07, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00};
const std::vector<uint8_t> kGroupKeyGolden = {
    0x01, 0x0c, 0x00, 0x00, 0x00, 0x64, 0x65, 0x76, 0x69, 0x63, 0x65, 0x5f, 0x63, 0x6f, 0x75, 0x6e, 0x74, 0x08, 0x00, 0x00, 0x40, 0x01, 0x00,
    0x00, 0x00, 0x01, 0x00, 0x01, 0x00, 0x00, 0x00, 0x05, 0x00, 0x00, 0x00, 0x3d, 0xfe, 0x90, 0xa9, 0x2c, 0xdc, 0xd9, 0x12, 0x02, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x01, 0x00, 0x00, 0x01, 0x00, 0x01, 0x00, 0x00, 0xff, 0xff, 0x07, 0x00, 0x00, 0x00, 0x00, 0x02, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00};

PredicateNode column(const char* c) { return {PredicateOp::Column, 0, 0, c}; }
PredicateNode constant(PrimType t, uint8_t first) {
    PredicateNode n{PredicateOp::Const, 0, 0, ""};
    n.constType = t;
    n.constBytes[0] = first;
    return n;
}

} // namespace

TEST(CheckedWritesPlanCompile, LegacyShapeStaysV1) {
    const auto d = characterSource();
    ViewPlan v;
    v.viewName = "char_all";
    v.key = "characterId";
    v.sources = {{5, "characterId"}};
    for (const auto& f : d.fields) v.output.push_back(replaceOf(d, f.name));
    const auto r = compileOver(v, d);
    ASSERT_TRUE(r.has_value()) << r.error().message;
    EXPECT_EQ(keyModeOf(r->plan), ViewKeyMode::LegacyU64);
    EXPECT_FALSE(ownerMappedOf(r->plan));
    EXPECT_EQ(prefixWidthOf(r->plan), 0u);
    EXPECT_EQ(r->plan.keyWidth, 8u);
    EXPECT_FALSE(needsV2(r->plan));
    const auto bytes = serialize(r->plan);
    EXPECT_EQ(bytes[0], 0x01);
    EXPECT_EQ(bytes, kLegacyShapeGolden);
}

TEST(CheckedWritesPlanCompile, CompositeAccountCharacters) {
    const auto d = characterSource();
    const auto r = compileOver(projection(d, {{"userId", 0, "userId"}, {"characterId", 0, "characterId"}}, {"name", "gender", "lastPlayed"}), d);
    ASSERT_TRUE(r.has_value()) << r.error().message;
    EXPECT_EQ(keyModeOf(r->plan), ViewKeyMode::Keyed);
    EXPECT_TRUE(ownerMappedOf(r->plan));
    EXPECT_EQ(r->plan.keyWidth, 16u);
    EXPECT_EQ(prefixWidthOf(r->plan), 8u);
    ASSERT_EQ(r->plan.output.size(), 5u);
    EXPECT_EQ(slotName(*r, 0), "userId");
    EXPECT_EQ(slotName(*r, 1), "characterId");
    EXPECT_EQ(slotName(*r, 2), "name");
    EXPECT_EQ(slotName(*r, 3), "gender");
    EXPECT_EQ(slotName(*r, 4), "lastPlayed");
    EXPECT_EQ(r->plan.output[0].rule, MergeRule::Replace);
    EXPECT_EQ(r->plan.keyComponents[0].outSlot, 0u);
    EXPECT_EQ(r->plan.keyComponents[1].outSlot, 1u);
    EXPECT_TRUE(needsV2(r->plan));
    EXPECT_EQ(serialize(r->plan)[0], 0x02);
}

TEST(CheckedWritesPlanCompile, LowerKeyColumnIsDerived) {
    const auto d = characterSource();
    auto v = projection(d, {{"name", 1, "nameKey"}, {"characterId", 0, "characterId"}}, {"gender"});
    v.unicodeVersion = rune::codec::kUnicodeVersion;
    const auto r = compileOver(v, d);
    ASSERT_TRUE(r.has_value()) << r.error().message;
    EXPECT_EQ(slotName(*r, 0), "nameKey");
    EXPECT_EQ(r->plan.output[0].type, PrimType::FixedStr);
    EXPECT_EQ(r->outputDescriptor.fields[0].fixedLen, 24u);
    EXPECT_EQ(r->plan.output[0].srcFieldId, kDerivedKeyField);
    EXPECT_EQ(r->plan.keyComponents[0].width, 24u);
    EXPECT_EQ(r->plan.keyWidth, 32u);
    EXPECT_EQ(prefixWidthOf(r->plan), 24u);
    EXPECT_EQ(r->plan.unicodeVersion, rune::codec::kUnicodeVersion);
}

TEST(CheckedWritesPlanCompile, MaximumWidthIs128) {
    const auto d = characterSource();
    auto v = projection(d, {{"tag", 1, "tagKey"}, {"characterId", 0, "characterId"}}, {"gender"});
    v.unicodeVersion = rune::codec::kUnicodeVersion;
    const auto ok = compileOver(v, d);
    ASSERT_TRUE(ok.has_value()) << ok.error().message;
    EXPECT_EQ(ok->plan.keyWidth, 128u);

    v.keyComponents.push_back({"gender", 0, "genderKey"});
    expectRefused(compileOver(v, d), Kind::KeyExpressionInvalid, "view key is 130 bytes; the limit is 128");
}

TEST(CheckedWritesPlanCompile, IdentityRule) {
    const auto d = characterSource();
    auto v = projection(d, {{"userId", 0, "userId"}}, {"name"});
    expectRefused(compileOver(v, d), Kind::KeyExpressionInvalid,
                  "view 'char_view': key does not identify the source record; declare the view unique or include characterId");
    v.unique = true;
    const auto r = compileOver(v, d);
    ASSERT_TRUE(r.has_value()) << r.error().message;
    EXPECT_TRUE(ownerMappedOf(r->plan));
    EXPECT_TRUE(r->plan.unique);
}

TEST(CheckedWritesPlanCompile, StringSourceKeyIsKeyedWithoutOwnerMap) {
    const auto d = rune::codec::DescriptorBuilder("topic", 1).addFixedStr("name", 16, 1, true).add("hp", PrimType::Int32).build();
    ViewPlan v;
    v.viewName = "by_name";
    v.key = "name";
    v.sources = {{5, "name"}};
    v.output = {replaceOf(d, "name"), replaceOf(d, "hp")};
    const auto r = compileOver(v, d);
    ASSERT_TRUE(r.has_value()) << r.error().message;
    EXPECT_EQ(keyModeOf(r->plan), ViewKeyMode::Keyed);
    EXPECT_FALSE(ownerMappedOf(r->plan));
    EXPECT_EQ(r->plan.keyWidth, 16u);
}

TEST(CheckedWritesPlanCompile, NarrowSourceKeyIsLegacy) {
    const auto d = rune::codec::DescriptorBuilder("topic", 1).add("id", PrimType::UInt16, 1, true).add("hp", PrimType::Int32).build();
    ViewPlan v;
    v.viewName = "by_id";
    v.key = "id";
    v.sources = {{5, "id"}};
    v.output = {replaceOf(d, "id"), replaceOf(d, "hp")};
    const auto r = compileOver(v, d);
    ASSERT_TRUE(r.has_value()) << r.error().message;
    EXPECT_EQ(keyModeOf(r->plan), ViewKeyMode::LegacyU64);
    EXPECT_EQ(r->plan.keyWidth, 2u);
    EXPECT_FALSE(needsV2(r->plan));
}

TEST(CheckedWritesPlanCompile, UnknownUnicodeVersionIsRefused) {
    const auto d = characterSource();
    auto v = projection(d, {{"name", 1, "nameKey"}, {"characterId", 0, "characterId"}}, {"gender"});
    v.unicodeVersion = rune::codec::packUnicodeVersion(17, 0, 0);
    expectRefused(compileOver(v, d), Kind::KeyExpressionInvalid, "lower() is pinned to Unicode 17.0.0; this broker has 16.0.0");
    v.unicodeVersion = 0;
    expectRefused(compileOver(v, d), Kind::KeyExpressionInvalid, "lower() needs a pinned Unicode version");
    auto plain = projection(d, {{"characterId", 0, "characterId"}, {"userId", 0, "userId"}}, {"gender"});
    plain.unicodeVersion = rune::codec::kUnicodeVersion;
    expectRefused(compileOver(plain, d), Kind::KeyExpressionInvalid, "unicodeVersion is set but no key component uses lower()");
}

TEST(CheckedWritesPlanCompile, KeyTypeRefusals) {
    const auto d = rune::codec::DescriptorBuilder("character", 1)
        .add("characterId", PrimType::UInt64, 1, true).add("gender", PrimType::UInt16).add("ratio", PrimType::Float64).build();
    auto with = [&](KeyComponent c) {
        ViewPlan v = projection(d, {c, {"characterId", 0, "characterId"}}, {"gender"});
        v.unicodeVersion = rune::codec::kUnicodeVersion;
        return compileOver(v, d);
    };
    expectRefused(with({"gender", 1, "g"}), Kind::KeyExpressionInvalid, "lower() needs a string(N) column; 'gender' is UInt16");
    expectRefused(with({"ratio", 0, "ratio"}), Kind::KeyExpressionInvalid, "key column 'ratio' must be an integer, enum, bool or string(N)");
    expectRefused(with({"gender", 2, "g"}), Kind::KeyExpressionInvalid, "casefold() is reserved");
    expectRefused(with({"gender", 3, "g"}), Kind::KeyExpressionInvalid, "unknown key function 3");
    expectRefused(with({"ghost", 0, "ghost"}), Kind::FieldUnresolved, "key column 'ghost' is not a field of the source");
}

TEST(CheckedWritesPlanCompile, DuplicateKeyColumnIsRefused) {
    const auto d = characterSource();
    expectRefused(compileOver(projection(d, {{"characterId", 0, "k"}, {"userId", 0, "k"}}, {"name"}), d),
                  Kind::KeyExpressionInvalid, "duplicate key column 'k'");
}

TEST(CheckedWritesPlanCompile, SelectedKeyColumnMustBeUnchanged) {
    const auto d = characterSource();
    auto v = projection(d, {{"characterId", 0, "characterId"}}, {"characterId", "name"});
    v.output[0].srcField = "userId";
    expectRefused(compileOver(v, d), Kind::KeyExpressionInvalid, "key column 'characterId' must project source field 'characterId' unchanged");
}

TEST(CheckedWritesPlanCompile, SingleEightByteGroupKeyStaysLegacy) {
    const auto d = deviceSource();
    ViewPlan v;
    v.viewName = "device_count";
    v.key = "userId";
    v.sources = {{5, "userId"}};
    v.output = {replaceOf(d, "userId"), {"n", PrimType::Int64, 0, "", MergeRule::CountAll}};
    const auto legacy = compileView(v, std::array<const rune::codec::Descriptor*, 1>{&d}, 0x40000008);
    ASSERT_TRUE(legacy.has_value()) << legacy.error().message;
    EXPECT_EQ(keyModeOf(legacy->plan), ViewKeyMode::LegacyU64);
    EXPECT_EQ(serialize(legacy->plan), kGroupKeyGolden);

    ViewPlan w;
    w.viewName = "device_count";
    w.sources = {{5, "userId"}};
    w.keyComponents = w.groupBy = {{"userId", 0, "userId"}};
    w.output = {{"n", PrimType::Int64, 0, "", MergeRule::CountAll}};
    const auto v2 = compileView(w, std::array<const rune::codec::Descriptor*, 1>{&d}, 0x40000008);
    ASSERT_TRUE(v2.has_value()) << v2.error().message;
    EXPECT_EQ(keyModeOf(v2->plan), ViewKeyMode::LegacyU64);
    EXPECT_EQ(serialize(v2->plan), kGroupKeyGolden);
}

TEST(CheckedWritesPlanCompile, CompositeGroupByIsKeyed) {
    const auto d = deviceSource();
    const auto r = compileOver(aggregate({{"userId", 0, "userId"}, {"deviceFp", 0, "deviceFp"}}), d);
    ASSERT_TRUE(r.has_value()) << r.error().message;
    EXPECT_EQ(keyModeOf(r->plan), ViewKeyMode::Keyed);
    EXPECT_EQ(r->plan.keyWidth, 40u);
    EXPECT_EQ(prefixWidthOf(r->plan), 8u);
    EXPECT_FALSE(ownerMappedOf(r->plan));
    ASSERT_EQ(r->plan.output.size(), 4u);
    EXPECT_EQ(slotName(*r, 0), "userId");
    EXPECT_EQ(slotName(*r, 1), "deviceFp");
    EXPECT_EQ(slotName(*r, 2), "uses");
    EXPECT_EQ(slotName(*r, 3), "lastSeen");

    auto lowered = aggregate({{"userId", 0, "userId"}, {"deviceFp", 1, "fpKey"}});
    lowered.unicodeVersion = rune::codec::kUnicodeVersion;
    const auto rl = compileOver(lowered, d);
    ASSERT_TRUE(rl.has_value()) << rl.error().message;
    EXPECT_EQ(rl->plan.keyWidth, 56u);
    EXPECT_EQ(rl->plan.output[1].type, PrimType::FixedStr);
    EXPECT_EQ(rl->outputDescriptor.fields[1].fixedLen, 48u);
    EXPECT_EQ(rl->plan.output[1].srcFieldId, kDerivedKeyField);
}

TEST(CheckedWritesPlanCompile, NarrowAndEnumGroupKeysAreKeyed) {
    const auto d = deviceSource();
    const auto en = compileOver(aggregate({{"platform", 0, "platform"}}, "platform"), d);
    ASSERT_TRUE(en.has_value()) << en.error().message;
    EXPECT_EQ(keyModeOf(en->plan), ViewKeyMode::Keyed);
    EXPECT_EQ(en->plan.keyWidth, 4u);

    const auto narrow = rune::codec::DescriptorBuilder("device", 1)
        .add("eventId", PrimType::UInt64, 1, true).add("region", PrimType::UInt16).add("lastSeen", PrimType::Int64).build();
    const auto u16 = compileOver(aggregate({{"region", 0, "region"}}, "region"), narrow);
    ASSERT_TRUE(u16.has_value()) << u16.error().message;
    EXPECT_EQ(keyModeOf(u16->plan), ViewKeyMode::Keyed);
    EXPECT_EQ(u16->plan.keyWidth, 2u);
}

TEST(CheckedWritesPlanCompile, GroupByMustEqualTheKey) {
    const auto d = deviceSource();
    auto v = aggregate({{"userId", 0, "userId"}, {"deviceFp", 0, "deviceFp"}});
    v.groupBy = {{"deviceFp", 0, "deviceFp"}, {"userId", 0, "userId"}};
    expectRefused(compileOver(v, d), Kind::KeyInvalid, "group by (deviceFp, userId) must equal the key (userId, deviceFp)");
    v.groupBy.clear();
    expectRefused(compileOver(v, d), Kind::KeyInvalid, "group by () must equal the key (userId, deviceFp)");
    v.groupBy = {{"userId", 0, "userId"}, {"deviceFp", 1, "deviceFp"}};
    expectRefused(compileOver(v, d), Kind::KeyInvalid, "group by (userId, lower(deviceFp)) must equal the key (userId, deviceFp)");
}

TEST(CheckedWritesPlanCompile, KeyedAggregateNeedsAnIntegerSourceKey) {
    const auto d = rune::codec::DescriptorBuilder("device", 1)
        .addFixedStr("eventId", 16, 1, true).add("userId", PrimType::UInt64).addFixedStr("deviceFp", 32).add("lastSeen", PrimType::Int64).build();
    expectRefused(compileOver(aggregate({{"userId", 0, "userId"}, {"deviceFp", 0, "deviceFp"}}), d), Kind::KeyExpressionInvalid,
                  "aggregate views need an integer source key of at most 8 bytes");

    auto v = aggregate({{"userId", 0, "userId"}, {"deviceFp", 0, "deviceFp"}});
    v.unique = true;
    expectRefused(compileOver(v, deviceSource()), Kind::UniqueInvalid, "unique needs a projection view");
}

TEST(CheckedWritesPlanCompile, ConstraintsCompileStructurally) {
    const auto d = characterSource();
    auto base = [&] { return projection(d, {{"characterId", 0, "characterId"}}, {"gender", "lastPlayed"}); };
    PredicateNode ge{PredicateOp::Ge, 0, 1, ""};
    PredicateNode eq{PredicateOp::Eq, 0, 1, ""};

    auto v = base();
    v.constraints = {{"nonNegative", {column("lastPlayed"), constant(PrimType::Int64, 0), ge}},
                     {"male", {column("gender"), constant(PrimType::UInt16, 1), eq}}};
    const auto r = compileOver(v, d);
    ASSERT_TRUE(r.has_value()) << r.error().message;
    ASSERT_EQ(r->plan.constraints.size(), 2u);
    EXPECT_EQ(r->plan.constraints[0].ordinal, 1u);
    EXPECT_EQ(r->plan.constraints[0].name, "nonNegative");
    EXPECT_EQ(r->plan.constraints[0].predicate[0].outSlot, 2u);   // characterId, gender, lastPlayed
    EXPECT_EQ(r->plan.constraints[0].predicate[2].op, PredicateOp::Ge);
    EXPECT_EQ(r->plan.constraints[1].ordinal, 2u);
    EXPECT_EQ(r->plan.constraints[1].predicate[0].outSlot, 1u);
    EXPECT_TRUE(needsV2(r->plan));

    auto refuse = [&](AuthoredConstraint c, const std::string& text) {
        auto w = base();
        w.constraints = {std::move(c)};
        expectRefused(compileOver(w, d), Kind::ConstraintInvalid, text);
    };
    refuse({"dangling", {column("lastPlayed"), constant(PrimType::Int64, 0), PredicateNode{PredicateOp::Ge, 0, 5, ""}}}, "constraint 'dangling'");
    refuse({"mismatch", {column("lastPlayed"), constant(PrimType::UInt16, 0), ge}}, "constraint 'mismatch'");
    refuse({"ghost", {column("ghost"), constant(PrimType::Int64, 0), ge}}, "constraint 'ghost'");
    refuse({"", {column("lastPlayed"), constant(PrimType::Int64, 0), ge}}, "constraint ''");
    refuse({"shape", {column("lastPlayed"), constant(PrimType::Int64, 0), ge, PredicateNode{PredicateOp::Eq, 0, 2, ""}}}, "constraint 'shape'");
    refuse({"empty", {}}, "constraint 'empty'");

    auto dup = base();
    dup.constraints = {{"same", {column("lastPlayed"), constant(PrimType::Int64, 0), ge}}, {"same", {column("lastPlayed"), constant(PrimType::Int64, 0), ge}}};
    expectRefused(compileOver(dup, d), Kind::ConstraintInvalid, "constraint 'same'");
}

TEST(CheckedWritesPlanV2, CompiledRoundTripAndStrictness) {
    CompiledViewPlan p;
    p.viewName = "v";
    p.viewTopicId = 9;
    p.keyFieldId = {1};
    p.sourceTopicId = {5};
    p.outputFingerprint = 0x1122334455667788ull;
    p.output = {{0, PrimType::UInt64, 0, 1, MergeRule::Replace}, {1, PrimType::FixedStr, 0, kDerivedKeyField, MergeRule::Replace},
                {2, PrimType::Int64, 0, 4, MergeRule::Replace}};
    p.keyComponents = {{1, 0, 8, 0, PrimType::UInt64}, {2, 1, 24, 1, PrimType::FixedStr}};
    p.keyWidth = 32;
    p.unique = true;
    CompiledPredicateNode col{PredicateOp::Column, 0, 0, 2};
    CompiledPredicateNode k{PredicateOp::Const, 0, 0, 0, PrimType::Int64};
    k.constBytes[3] = 0x7f;
    CompiledPredicateNode ge{PredicateOp::Ge, 0, 1, 0};
    p.constraints = {{1, "nonNegative", {col, k, ge}}, {2, "other", {col, k, ge}}};
    p.unicodeVersion = rune::codec::kUnicodeVersion;
    p.viewObjectId = 77;

    const auto bytes = serialize(p);
    EXPECT_EQ(bytes[0], 0x02);
    const auto back = deserializeCompiledViewPlan(bytes);
    ASSERT_TRUE(back.has_value());
    EXPECT_EQ(back->keyComponents.size(), 2u);
    EXPECT_EQ(back->keyComponents[1].keyFunction, 1);
    EXPECT_EQ(back->keyComponents[1].width, 24u);
    EXPECT_EQ(back->keyComponents[1].outSlot, 1u);
    EXPECT_EQ(back->keyComponents[1].type, PrimType::FixedStr);
    EXPECT_EQ(back->keyWidth, 32u);
    EXPECT_TRUE(back->unique);
    ASSERT_EQ(back->constraints.size(), 2u);
    EXPECT_EQ(back->constraints[1].ordinal, 2u);
    EXPECT_EQ(back->constraints[0].name, "nonNegative");
    EXPECT_EQ(back->constraints[0].predicate[0].outSlot, 2u);
    EXPECT_EQ(back->constraints[0].predicate[1].constType, PrimType::Int64);
    EXPECT_EQ(back->constraints[0].predicate[1].constBytes[3], 0x7f);
    EXPECT_EQ(back->constraints[0].predicate[2].b, 1u);
    EXPECT_EQ(back->unicodeVersion, rune::codec::kUnicodeVersion);
    EXPECT_EQ(back->viewObjectId, 77u);
    EXPECT_EQ(serialize(*back), bytes);

    for (size_t n = 0; n < bytes.size(); ++n)
        EXPECT_FALSE(deserializeCompiledViewPlan(std::span<const uint8_t>(bytes.data(), n)).has_value()) << "prefix " << n;
    auto trailing = bytes;
    trailing.push_back(0);
    EXPECT_FALSE(deserializeCompiledViewPlan(trailing).has_value());
}

TEST(CheckedWritesPlanV2, V1BlobDecodesWithSynthesizedComponent) {
    CompiledViewPlan p;
    p.viewName = "v";
    p.keyFieldId = {3};
    p.sourceTopicId = {5};
    p.output = {{0, PrimType::UInt16, 0, 3, MergeRule::Replace}};
    EXPECT_FALSE(needsV2(p));
    const auto bytes = serialize(p);
    EXPECT_EQ(bytes[0], 0x01);
    const auto back = deserializeCompiledViewPlan(bytes);
    ASSERT_TRUE(back.has_value());
    ASSERT_EQ(back->keyComponents.size(), 1u);
    EXPECT_EQ(back->keyComponents[0].srcFieldId, 3u);
    EXPECT_EQ(back->keyComponents[0].keyFunction, 0);
    EXPECT_EQ(back->keyComponents[0].width, 2u);
    EXPECT_EQ(back->keyComponents[0].outSlot, 0u);
    EXPECT_EQ(back->keyComponents[0].type, PrimType::UInt16);
    EXPECT_EQ(back->keyWidth, 2u);
    EXPECT_FALSE(needsV2(*back));
    EXPECT_EQ(serialize(*back), bytes);

    auto trailing = bytes;
    trailing.push_back(0);
    EXPECT_FALSE(deserializeCompiledViewPlan(trailing).has_value());
}

TEST(CheckedWritesPlanV2, CompiledDecodeRefusesAnUnknownKeyFunctionRegression) {
    CompiledViewPlan p;
    p.viewName = "v";
    p.keyFieldId = {1};
    p.sourceTopicId = {5};
    p.output = {{0, PrimType::UInt64, 0, 1, MergeRule::Replace}, {1, PrimType::UInt64, 0, 2, MergeRule::Replace}};
    p.keyComponents = {{1, 0, 8, 0, PrimType::UInt64}, {2, 0, 8, 1, PrimType::UInt64}};
    p.keyWidth = 16;
    p.unique = true;
    auto bytes = serialize(p);
    ASSERT_EQ(bytes[0], 0x02);
    ASSERT_TRUE(deserializeCompiledViewPlan(bytes).has_value());

    // tail after the components: keyWidth u16, unique u8, constraint count u32, unicodeVersion u32, viewObjectId u64
    const size_t firstComponent = bytes.size() - (2 + 1 + 4 + 4 + 8) - 2 * 8;
    for (uint8_t fn : {uint8_t{2}, uint8_t{3}, uint8_t{255}}) {
        auto patched = bytes;
        patched[firstComponent + 2] = fn;
        EXPECT_FALSE(deserializeCompiledViewPlan(patched).has_value()) << "keyFunction " << int(fn);
    }
    auto zeroWidth = bytes;
    zeroWidth[firstComponent + 3] = 0;
    zeroWidth[firstComponent + 4] = 0;
    EXPECT_FALSE(deserializeCompiledViewPlan(zeroWidth).has_value());
    auto wrongWidth = bytes;
    wrongWidth[firstComponent + 3] = 4;
    EXPECT_FALSE(deserializeCompiledViewPlan(wrongWidth).has_value());
}

TEST(CheckedWritesPlanCompile, GroupByWithoutAnAggregateColumnRegression) {
    const auto d = deviceSource();
    auto v = aggregate({{"userId", 0, "userId"}});
    v.output = {replaceOf(d, "lastSeen")};
    expectRefused(compileOver(v, d), Kind::KeyInvalid, "group by needs an aggregate column");
}
