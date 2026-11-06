// tests/client/CheckedWritesViewKeyBuilderTest.cpp - checked writes Part 07: the client's view key bytes, scan payload decode.
#include <gtest/gtest.h>

#include <rune/catalog/testing/GoldenScript.h>
#include <rune/client/Key.h>
#include <rune/client/view/ViewKeyBuilder.h>
#include <rune/client/view/ViewRowCodec.h>
#include <rune/codec/ViewKeyEncoding.h>
#include <rune/protocol/PartitionHash.h>

#include <cstring>

namespace rcv = rune::client::view;
namespace cd  = rune::codec;
using rune::protocol::Partitioner;

namespace {

rcv::ViewPlan planOf(std::vector<rcv::KeyComponent> components, std::string sourceKey) {
    rcv::ViewPlan p;
    p.viewName = "v";
    p.key = components.empty() ? sourceKey : components[0].column;
    p.sources.push_back(rcv::SourceSpec{0, std::move(sourceKey)});
    p.output.push_back(rcv::AuthoredField{"n", cd::PrimType::UInt64, 0, "characterId", rcv::MergeRule::Replace});
    p.keyComponents = std::move(components);
    return p;
}

cd::Descriptor characterDescriptor() {
    cd::DescriptorBuilder d("character");
    d.add("characterId", cd::PrimType::UInt64, 1, true).add("userId", cd::PrimType::UInt64).add("provider", cd::PrimType::UInt16);
    d.addFixedStr("name", 16);
    return d.build();
}

std::vector<uint8_t> le(uint64_t v, std::size_t n) {
    std::vector<uint8_t> out(n);
    for (std::size_t i = 0; i < n; ++i) out[i] = uint8_t(v >> (8 * i));
    return out;
}

} // namespace

TEST(CheckedWritesViewKeyBuilder, CompositeKeyAndPrefix) {
    auto b = rcv::ViewKeyBuilder::fromPlan(planOf({{"userId", 0, "userId"}, {"characterId", 0, "characterId"}}, "characterId"),
                                           characterDescriptor());
    ASSERT_TRUE(b.has_value()) << b.error();
    EXPECT_EQ(b->keyWidth(), 16u);
    b->setUInt("userId", 42);
    const auto prefix = b->prefix();
    ASSERT_TRUE(prefix.has_value());
    EXPECT_EQ(*prefix, (std::vector<uint8_t>{0, 0, 0, 0, 0, 0, 0, 0x2A}));
    const auto missing = b->key();
    ASSERT_FALSE(missing.has_value());
    EXPECT_EQ(missing.error(), "characterId is not set");

    b->setUInt("characterId", 7);
    const auto key = b->key();
    ASSERT_TRUE(key.has_value());
    EXPECT_EQ(*key, (std::vector<uint8_t>{0, 0, 0, 0, 0, 0, 0, 0x2A, 0, 0, 0, 0, 0, 0, 0, 7}));
    EXPECT_FALSE(b->prefix().has_value());   // every component set: that is a key
}

TEST(CheckedWritesViewKeyBuilder, LowerMatchesTheBrokerFoldKey) {
    auto b = rcv::ViewKeyBuilder::fromPlan(
        planOf({{"name", cd::kKeyFunctionLower, "nameKey"}, {"characterId", 0, "characterId"}}, "characterId"), characterDescriptor());
    ASSERT_TRUE(b.has_value()) << b.error();
    std::vector<uint8_t> golden = {0x66, 0x6F, 0x6F};
    golden.resize(24, 0);
    for (uint8_t v : {0, 0, 0, 0, 0, 0, 0, 7}) golden.push_back(v);

    b->setString("name", "FoO").setUInt("characterId", 7);
    auto key = b->key();
    ASSERT_TRUE(key.has_value()) << key.error();
    EXPECT_EQ(key->size(), 32u);
    EXPECT_EQ(*key, golden);

    b->clear();
    b->setString("nameKey", "FOO").setUInt("characterId", 7);
    key = b->key();
    ASSERT_TRUE(key.has_value());
    EXPECT_EQ(*key, golden);
}

TEST(CheckedWritesViewKeyBuilder, RangeAndLengthAreChecked) {
    auto small = rcv::ViewKeyBuilder::fromPlan(planOf({{"provider", 0, "provider"}}, "provider"), characterDescriptor());
    ASSERT_TRUE(small.has_value()) << small.error();
    small->setInt("provider", -1);
    EXPECT_FALSE(small->key().has_value());
    small->setUInt("provider", 70000);
    EXPECT_FALSE(small->key().has_value());
    small->setUInt("provider", 513);
    ASSERT_TRUE(small->key().has_value());
    EXPECT_EQ(*small->key(), (std::vector<uint8_t>{0x02, 0x01}));

    auto str = rcv::ViewKeyBuilder::fromPlan(planOf({{"name", cd::kKeyFunctionLower, "nameKey"}}, "characterId"), characterDescriptor());
    ASSERT_TRUE(str.has_value()) << str.error();
    str->setString("name", std::string(17, 'a'));
    EXPECT_FALSE(str->key().has_value());
    str->setString("name", std::string("\xC3\x28", 2));
    EXPECT_FALSE(str->key().has_value());
    str->setString("name", "ok");
    EXPECT_TRUE(str->key().has_value());

    str->setString("nope", "x");
    EXPECT_FALSE(str->key().has_value());   // a set* that named no component is not silently dropped
}

TEST(CheckedWritesViewKeyBuilder, PartitionFollowsTheProducer) {
    cd::DescriptorBuilder d16("s16");
    d16.add("id", cd::PrimType::UInt16, 1, true).add("v", cd::PrimType::Int32);
    auto fnv = rcv::ViewKeyBuilder::fromPlan(planOf({}, "id"), d16.build(), 4, Partitioner::Fnv1a);
    ASSERT_TRUE(fnv.has_value()) << fnv.error();
    fnv->setUInt("id", 513);
    const auto wire16 = le(513, 2);
    EXPECT_EQ(fnv->partition(), rune::protocol::partitionFor(wire16, 4, Partitioner::Fnv1a));

    cd::DescriptorBuilder d64("s64");
    d64.add("id", cd::PrimType::UInt64, 1, true);
    auto mod = rcv::ViewKeyBuilder::fromPlan(planOf({}, "id"), d64.build(), 4, Partitioner::Modulo);
    ASSERT_TRUE(mod.has_value()) << mod.error();
    mod->setUInt("id", 4097);
    EXPECT_EQ(mod->partition(), rune::protocol::partitionFor(le(4097, 8), 4, Partitioner::Modulo));
    EXPECT_EQ(mod->partition(), 1);

    auto one = rcv::ViewKeyBuilder::fromPlan(planOf({}, "id"), d64.build(), 1, Partitioner::Fnv1a);
    ASSERT_TRUE(one.has_value());
    one->setUInt("id", 4097);
    EXPECT_EQ(one->partition(), -1);
}

TEST(CheckedWritesViewKeyBuilder, FromCatalogFindsTheView) {
    namespace gd = rune::catalog::testing::golden_detail;
    namespace cat = rune::catalog;
    gd::ScriptBuilder script;
    cat::CreateTopicOps topic;
    topic.partitions = 1;
    topic.replicationFactor = 1;
    topic.schema = cat::SchemaDef{.keyField = "characterId", .validate = true, .descriptorBlob = cd::serialize(characterDescriptor()), .nextTag = 4};
    script.createTopic("character", std::move(topic), "CREATE TOPIC character");

    const auto plan = planOf({{"userId", 0, "userId"}, {"characterId", 0, "characterId"}}, "characterId");
    cd::DescriptorBuilder out("account_characters");
    out.add("userId", cd::PrimType::UInt64, 1, true).add("characterId", cd::PrimType::UInt64);
    cat::ViewDef view;
    view.sinkPartitions = 1;
    view.keyField = rcv::viewKeyField(plan, "");   // as createView sends it; "userId,characterId" fails apply
    view.sourceNames = {"character"};
    view.normalizedPlanBlob = rune::view::serializeAuthored(plan);
    view.compiledPlanBlob = {0xC0};
    view.outputDescriptorBlob = cd::serialize(out.build());
    script.createView("account_characters", std::move(view), "CREATE VIEW account_characters");

    auto b = rcv::ViewKeyBuilder::fromCatalog(script.state(), "account_characters");
    ASSERT_TRUE(b.has_value()) << b.error();
    EXPECT_EQ(b->components().size(), 2u);
    EXPECT_EQ(b->components()[0].column, "userId");
    EXPECT_EQ(b->keyWidth(), 16u);
    EXPECT_EQ(rcv::viewKeyField(plan, ""), "userId");
    EXPECT_FALSE(rcv::ViewKeyBuilder::fromCatalog(script.state(), "nope").has_value());
}

TEST(CheckedWritesViewKeyBuilder, DecodeScanPayload) {
    const auto put32 = [](std::vector<uint8_t>& v, uint32_t x) { for (int i = 0; i < 4; ++i) v.push_back(uint8_t(x >> (8 * i))); };
    std::vector<uint8_t> payload;
    put32(payload, 12);
    for (uint8_t i = 0; i < 12; ++i) payload.push_back(0xA0 + i);
    const std::vector<std::vector<uint8_t>> rows = {{1, 2, 3}, {}, {9, 9}};
    std::size_t rowsLen = 0;
    for (const auto& r : rows) rowsLen += 4 + r.size();
    put32(payload, uint32_t(rowsLen));
    for (const auto& r : rows) {
        put32(payload, uint32_t(r.size()));
        payload.insert(payload.end(), r.begin(), r.end());
    }

    rune::client::ViewScanResult page;
    ASSERT_TRUE(rcv::decodeScanPayload(payload, page));
    EXPECT_EQ(page.rows, rows);
    ASSERT_EQ(page.nextToken.size(), 12u);
    EXPECT_EQ(page.nextToken[0], 0xA0);

    for (std::size_t cut = 0; cut < payload.size(); ++cut) {
        rune::client::ViewScanResult truncated;
        EXPECT_FALSE(rcv::decodeScanPayload(std::span<const uint8_t>(payload.data(), cut), truncated)) << "cut " << cut;
    }

    std::vector<uint8_t> last = {0, 0, 0, 0, 0, 0, 0, 0};   // no token, no rows: the last, empty page
    rune::client::ViewScanResult empty;
    ASSERT_TRUE(rcv::decodeScanPayload(last, empty));
    EXPECT_TRUE(empty.nextToken.empty());
    empty.status = 0;   // ViewRowResult::kOk
    EXPECT_TRUE(empty.complete());
}

TEST(CheckedWritesViewKeyBuilderRegression, LongStringKeyRoutesLikeTheProducerRegression) {
    cd::DescriptorBuilder d("s32");
    d.addFixedStr("name", 32, 1, true);
    auto b = rcv::ViewKeyBuilder::fromPlan(planOf({}, "name"), d.build(), 4, Partitioner::Fnv1a);
    ASSERT_TRUE(b.has_value()) << b.error();
    for (const std::string_view v : {"a", "abc", "alice", "bob", "carol", "dave", "eve"}) {
        b->setString("name", v);
        // TypedTopic::claim keys a string field wider than Key::kMaxInline with the value, trailing NULs stripped.
        const rune::client::Key producer(std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(v.data()), v.size()));
        EXPECT_EQ(b->partition(), producer.partition(4)) << v;
    }
}
