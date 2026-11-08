// CheckedWritesWriteStateTest.cpp — checked writes Part 02 Task 3: TopicWriteSpec, the wire form of its facts, and
// checkTypedRecord / checkTypedBatch outside the matrices (bounded framing, version rules, the model-checked fuzz).
#include <rune/node/WriteState.h>
#include <rune/catalog/CatalogState.h>
#include <rune/codec/Serialize.h>

#include "support/CheckedWritesBatches.h"
#include "support/CheckedWritesValidationModel.h"

#include <gtest/gtest.h>

#include <random>

using namespace rune::node;
using namespace rune::node::cw;
using rune::codec::PrimType;

namespace {

rune::catalog::TopicObject topicOf(const rune::codec::Descriptor& d, std::string keyField, bool validate) {
    rune::catalog::TopicObject t;
    t.topicId = 9;
    t.name = "t";
    rune::catalog::SchemaState s;
    s.keyField = std::move(keyField);
    s.validate = validate;
    s.evolvingBlob = rune::codec::serialize(d);
    t.schema = std::move(s);
    return t;
}

TopicWriteSpec specOf(Entity e) {
    TopicWriteSpec s;
    specFor(e, s);
    return s;
}

}  // namespace

TEST(CheckedWritesWriteState, WireRoundTripsAndRefusesShortInput) {
    TopicWriteSpecWire w;
    w.topicId = -5; w.keyFieldId = 3; w.keyType = PrimType::FixedStr; w.keyWidth = 16; w.legacyValidate = true;
    uint8_t buf[kTopicWriteSpecWireLen];
    ASSERT_EQ(encodeTopicWriteSpec(w, buf), kTopicWriteSpecWireLen);
    EXPECT_EQ(kTopicWriteSpecWireLen, 9u);
    const auto back = decodeTopicWriteSpec(buf);
    ASSERT_TRUE(back);
    EXPECT_EQ(*back, w);
    EXPECT_FALSE(decodeTopicWriteSpec(std::span<const uint8_t>(buf, 8)));
    EXPECT_EQ(encodeTopicWriteSpec(w, std::span<uint8_t>(buf, 8)), 0u);
}

TEST(CheckedWritesWriteState, KeyIndexFlagRoundTrips) {
    TopicWriteSpecWire w;
    w.topicId = 4; w.keyFieldId = 1; w.keyType = PrimType::UInt64; w.keyWidth = 8; w.keyIndex = true;
    uint8_t buf[kTopicWriteSpecWireLen];
    ASSERT_EQ(encodeTopicWriteSpec(w, buf), 9u);
    EXPECT_EQ(buf[8], 2);
    const auto back = decodeTopicWriteSpec(buf);
    ASSERT_TRUE(back);
    EXPECT_EQ(*back, w);
    for (int bit = 2; bit < 8; ++bit) {
        buf[8] = static_cast<uint8_t>(1 << bit);
        EXPECT_FALSE(decodeTopicWriteSpec(buf)) << "flag bit " << bit;
    }
}

TEST(CheckedWritesWriteState, AutoIncFieldsRoundTrip) {
    TopicWriteSpecWire w;
    w.topicId = 4; w.keyFieldId = 1; w.keyType = PrimType::UInt64; w.keyWidth = 8; w.keyIndex = true;
    w.autoIncrement = true; w.partitioner = rune::protocol::Partitioner::Modulo;
    w.autoIncrementMax = 0x0102030405060708ull; w.autoIncrementReserve = 77; w.partitionCount = 3;
    uint8_t buf[kTopicWriteSpecAutoIncWireLen];
    ASSERT_EQ(topicWriteSpecWireLen(w), 26u);
    ASSERT_EQ(encodeTopicWriteSpec(w, buf), 26u);
    EXPECT_EQ(buf[8], 6) << "keyIndex | autoIncrement";
    const auto back = decodeTopicWriteSpec(buf);
    ASSERT_TRUE(back);
    EXPECT_EQ(*back, w);
    EXPECT_EQ(encodeTopicWriteSpec(w, std::span<uint8_t>(buf, 25)), 0u);
    EXPECT_FALSE(decodeTopicWriteSpec(std::span<const uint8_t>(buf, 25))) << "a 25-byte tail";
    EXPECT_FALSE(decodeTopicWriteSpec(std::span<const uint8_t>(buf, 9))) << "autoincrement needs the long form";
    uint8_t bad[26];
    std::memcpy(bad, buf, 26);
    bad[9] = 2;
    EXPECT_FALSE(decodeTopicWriteSpec(bad)) << "partitioner 2";
    std::memcpy(bad, buf, 26);
    bad[8] = 2;
    EXPECT_FALSE(decodeTopicWriteSpec(bad)) << "the long form without autoincrement";
    std::memcpy(bad, buf, 26);
    bad[8] = 0x0E;
    EXPECT_FALSE(decodeTopicWriteSpec(bad)) << "flag bit 3";
}

TEST(CheckedWritesWriteState, TheNineByteFormStillRoundTrips) {
    TopicWriteSpecWire w;
    w.topicId = 4; w.keyFieldId = 1; w.keyType = PrimType::UInt64; w.keyWidth = 8; w.keyIndex = true;
    uint8_t buf[kTopicWriteSpecAutoIncWireLen];
    ASSERT_EQ(topicWriteSpecWireLen(w), 9u);
    ASSERT_EQ(encodeTopicWriteSpec(w, buf), 9u);
    const auto back = decodeTopicWriteSpec(std::span<const uint8_t>(buf, 9));
    ASSERT_TRUE(back);
    EXPECT_EQ(*back, w);
    EXPECT_FALSE(back->autoIncrement);
    EXPECT_EQ(back->partitioner, rune::protocol::Partitioner::Fnv1a);
}

TEST(CheckedWritesWriteState, SetFactsCopiesTheAutoIncrementFacts) {
    TopicWriteSpecWire w;
    w.topicId = 4; w.keyIndex = true; w.autoIncrement = true; w.partitioner = rune::protocol::Partitioner::Modulo;
    w.autoIncrementMax = 99; w.autoIncrementReserve = 5; w.partitionCount = 2;
    TopicWriteSpec s;
    EXPECT_FALSE(s.autoIncrement);
    s.setFacts(w);
    EXPECT_TRUE(s.autoIncrement);
    EXPECT_EQ(s.partitioner, rune::protocol::Partitioner::Modulo);
    EXPECT_EQ(s.autoIncrementMax, 99u);
    EXPECT_EQ(s.autoIncrementReserve, 5u);
    EXPECT_EQ(s.partitionCount, 2);
}

TEST(CheckedWritesWriteState, SpecOfAnAutoIncrementCatalogTopic) {
    auto t = topicOf(matrixDescriptor(), "id", true);
    t.partitions = 4;
    t.schema->keyFeatures = {.keyIndex = true, .autoIncrement = true, .partitioner = rune::protocol::Partitioner::Modulo,
                             .autoIncrementMax = 1000, .autoIncrementReserve = 128};
    const auto w = writeSpecWireOf(t);
    ASSERT_TRUE(w);
    EXPECT_TRUE(w->autoIncrement);
    EXPECT_TRUE(w->keyIndex);
    EXPECT_EQ(w->partitioner, rune::protocol::Partitioner::Modulo);
    EXPECT_EQ(w->autoIncrementMax, 1000u);
    EXPECT_EQ(w->autoIncrementReserve, 128u);
    EXPECT_EQ(w->partitionCount, 4);
    EXPECT_EQ(topicWriteSpecWireLen(*w), 26u);
}

TEST(CheckedWritesWriteState, SetFactsCopiesTheKeyIndex) {
    TopicWriteSpecWire w;
    w.topicId = 4; w.keyIndex = true;
    TopicWriteSpec s;
    EXPECT_FALSE(s.keyIndex);
    s.setFacts(w);
    EXPECT_TRUE(s.keyIndex);
    w.keyIndex = false;
    s.setFacts(w);
    EXPECT_FALSE(s.keyIndex);
}

TEST(CheckedWritesWriteState, SpecOfACatalogTopic) {
    rune::catalog::TopicObject untyped;
    EXPECT_FALSE(writeSpecWireOf(untyped));

    const auto d = matrixDescriptor();
    auto w = writeSpecWireOf(topicOf(d, "id", true));
    ASSERT_TRUE(w);
    EXPECT_EQ(w->keyFieldId, d.find("id")->id);
    EXPECT_EQ(w->keyType, PrimType::UInt64);
    EXPECT_EQ(w->keyWidth, 8);
    EXPECT_TRUE(w->legacyValidate);
    EXPECT_EQ(w->topicId, 9);

    w = writeSpecWireOf(topicOf(d, "name", false));
    ASSERT_TRUE(w);
    EXPECT_EQ(w->keyWidth, 16);
    EXPECT_FALSE(w->legacyValidate);

    w = writeSpecWireOf(topicOf(d, "bio", true));
    ASSERT_TRUE(w);
    EXPECT_EQ(w->keyFieldId, d.find("bio")->id);
    EXPECT_EQ(w->keyWidth, 0) << "a string key cannot be checked (P4: the topic predates the key rule)";

    w = writeSpecWireOf(topicOf(d, "", true));
    ASSERT_TRUE(w);
    EXPECT_EQ(w->keyFieldId, kNoKeyField);

    auto renamed = d;
    renamed.fields[0].name = "ident";   // ALTER RENAME keeps the tag
    w = writeSpecWireOf(topicOf(renamed, "ident", true));
    ASSERT_TRUE(w);
    EXPECT_EQ(w->keyFieldId, d.find("id")->id);
}

TEST(CheckedWritesWriteState, VersionsAreDenseAndLookupIsExact) {
    TopicWriteSpec s;
    s.installVersion(1, matrixDescriptor(1));
    s.installVersion(3, matrixDescriptor(3));
    EXPECT_EQ(s.versionFor(2), nullptr);
    EXPECT_EQ(s.versionFor(0), nullptr);
    EXPECT_EQ(s.maxVersion(), 3);
    EXPECT_NE(s.validatorFor(1), nullptr);
    EXPECT_EQ(s.validatorFor(2), nullptr);
    EXPECT_EQ(s.descriptorFor(3)->version, 3);
    EXPECT_EQ(s.versionFor(4000), nullptr);
}

TEST(CheckedWritesWriteState, KeyOffsetFollowsEachVersionsLayout) {
    using rune::codec::DescriptorBuilder;
    const auto v1 = DescriptorBuilder("t", 1).add("a", PrimType::Int32).add("id", PrimType::UInt64).build();
    const auto v2 = DescriptorBuilder("t", 2).add("a", PrimType::Int32).add("id", PrimType::UInt64).add("b", PrimType::Int64, 2).build();
    const auto noKey = DescriptorBuilder("t", 3).add("a", PrimType::Int32).build();
    TopicWriteSpec s;
    s.installVersion(1, v1);
    s.installVersion(2, v2);
    s.installVersion(3, noKey);
    TopicWriteSpecWire w;
    w.topicId = 1; w.keyFieldId = 1; w.keyType = PrimType::UInt64; w.keyWidth = 8; w.legacyValidate = true;
    s.setFacts(w);
    EXPECT_EQ(s.versionFor(1)->keyValueOffset, 4u + 4u);
    EXPECT_EQ(s.versionFor(2)->keyValueOffset, 4u + 4u);
    EXPECT_EQ(s.versionFor(3)->keyValueOffset, kNoKeyOffset);
    // A v3 record has no key field: it is refused at the checked level.
    const std::string value = leBytes(4, 2) + leBytes(3, 2) + leBytes(1, 4);
    const auto batch = batchOf({{leBytes(7, 8), value}});
    EXPECT_EQ(checkTypedBatch(s, batch, 5), TypedCheck::Reject);
    EXPECT_EQ(checkTypedBatch(s, batch, 4), TypedCheck::Accept);
}

TEST(CheckedWritesWriteState, FactsAfterVersionsRecomputeOffsets) {
    TopicWriteSpecWire w;
    w.topicId = 1; w.keyFieldId = kIdTag; w.keyType = PrimType::UInt64; w.keyWidth = 8; w.legacyValidate = true;
    TopicWriteSpec a, b;
    a.installVersion(1, matrixDescriptor());
    a.setFacts(w);
    b.setFacts(w);
    b.installVersion(1, matrixDescriptor());
    EXPECT_EQ(a.versionFor(1)->keyValueOffset, 4u);
    EXPECT_EQ(b.versionFor(1)->keyValueOffset, 4u);
    EXPECT_TRUE(a.hasFacts());
    TopicWriteSpec c;
    c.installVersion(1, matrixDescriptor());
    EXPECT_FALSE(c.hasFacts());
    EXPECT_EQ(c.versionFor(1)->keyValueOffset, kNoKeyOffset);
}

TEST(CheckedWritesWriteStateRegression, LyingFramingIsRejectedInBoundsRegression) {
    const TopicWriteSpec spec = specOf(Entity::TypedLegacy);
    // a key length past the end of the record
    EXPECT_EQ(checkTypedBatch(spec, withLyingKeyLength(batchOf({validRecord()}), 0), 5), TypedCheck::Reject);
    // a value length past the end: patch the value-length varint (after the 8-byte key) of the one record
    {
        auto b = batchOf({validRecord()});
        std::size_t pos = 61;
        while (b[pos] & 0x80) ++pos;
        ++pos;                                           // length varint
        ++pos;                                           // attributes
        while (b[pos] & 0x80) ++pos;
        ++pos;                                           // tsDelta
        while (b[pos] & 0x80) ++pos;
        ++pos;                                           // offsetDelta
        ++pos;                                           // keyLen
        pos += 8;                                        // key
        b[pos] = 0x7E;                                   // valueLen = 63 < the real value, but patched below to overrun
        b[pos] = 0xFE; b.insert(b.begin() + static_cast<std::ptrdiff_t>(pos) + 1, 0x7F);   // a two-byte varint far past the end
        EXPECT_EQ(checkTypedBatch(spec, b, 5), TypedCheck::Reject);
    }
    // recordCount one larger than the records present
    {
        auto b = batchOf({validRecord()});
        b[57] = 2;
        EXPECT_EQ(checkTypedBatch(spec, b, 5), TypedCheck::Reject);
    }
    // a record length of 0
    {
        auto b = batchOf({validRecord()});
        b[61] = 0;
        EXPECT_EQ(checkTypedBatch(spec, b, 5), TypedCheck::Reject);
    }
    // a batch shorter than its header, and an empty one
    EXPECT_EQ(checkTypedBatch(spec, std::span<const uint8_t>{}, 5), TypedCheck::Reject);
    const std::vector<uint8_t> shortBatch(40, 0);
    EXPECT_EQ(checkTypedBatch(spec, shortBatch, 5), TypedCheck::Reject);
}

TEST(CheckedWritesWriteState, FirstNonAcceptedRecordDecides) {
    const TopicWriteSpec spec = specOf(Entity::TypedLegacy);
    Parts ahead; ahead.version = 2;
    Parts bad;   bad.kind = 9;
    EXPECT_EQ(checkTypedBatch(spec, batchOf({encode(ahead), encode(bad)}), 5), TypedCheck::VersionAhead);
    EXPECT_EQ(checkTypedBatch(spec, batchOf({encode(bad), encode(ahead)}), 5), TypedCheck::Reject);
}

TEST(CheckedWritesWriteState, BelowTheLevelWithoutLegacyAcceptsAnything) {
    const TopicWriteSpec spec = specOf(Entity::TypedPreUpgrade);
    const std::vector<uint8_t> garbage(100, 0xAB);
    EXPECT_EQ(checkTypedBatch(spec, garbage, 4), TypedCheck::Accept) << "not even walked";
    EXPECT_EQ(checkTypedBatch(spec, garbage, 5), TypedCheck::Reject);
}

TEST(CheckedWritesWriteState, RecordCheckIsTheBatchCheckPerRecord) {
    for (int entity = 0; entity < 4; ++entity) {
        const TopicWriteSpec spec = specOf(static_cast<Entity>(entity));
        for (int op = 1; op <= 14; ++op) {
            Parts p;
            switch (op) {
            case 2: p.version = 0; break;
            case 3: p.version = 2; break;
            case 4: p.blockLength = 29; break;
            case 5: p.truncateBy = 1; break;
            case 6: p.kind = 9; break;
            case 7: p.bio = std::string("\xC0\xAF", 2); break;
            case 8: p.name = std::string("\x80", 1); break;
            case 9: p.name = std::string("a\0b", 3); break;
            case 10: p.key = leBytes(p.id + 1, 8); break;
            case 11: p.key = leBytes(p.id, 4); break;
            case 12: p.tombstone = true; break;
            case 13: p.tombstone = true; p.key = leBytes(p.id, 4); break;
            default: break;
            }
            const auto batch = batchOf({encode(p)});
            for (int16_t level : {int16_t{4}, int16_t{5}}) {
                TypedCheck viaRecord = TypedCheck::Accept;
                ASSERT_TRUE(rune::protocol::forEachRecordSpan(batch.data(), batch.size(), [&](const rune::protocol::RecordSpan& r) {
                    viaRecord = checkTypedRecord(spec, r, level);
                }));
                EXPECT_EQ(viaRecord, checkTypedBatch(spec, batch, level)) << "entity " << entity << " op " << op << " level " << level;
            }
        }
    }
}

TEST(CheckedWritesValidationFuzzRegression, RandomMutationsAgreeWithTheModel) {
    const Entity entities[] = {Entity::TypedLegacy, Entity::TypedPreUpgrade, Entity::TypedKeyless, Entity::TypedUncheckableKey};
    for (uint32_t seed = 1; seed <= 64; ++seed) {
        std::mt19937 rng(seed);
        for (int iter = 0; iter < 2000; ++iter) {
            std::vector<TypedRecord> recs;
            const std::size_t n = 1 + rng() % 8;
            for (std::size_t i = 0; i < n; ++i) {
                Parts p;
                p.id = rng() % 50;
                p.bio = std::string(rng() % 6, 'a');
                recs.push_back(encode(p));
            }
            auto batch = batchOf(recs);
            switch (rng() % 6) {
            case 0: batch[61 + rng() % (batch.size() - 61)] ^= static_cast<uint8_t>(1u << (rng() % 8)); break;   // flip
            case 1: batch.resize(61 + rng() % (batch.size() - 60)); break;                                       // truncate
            case 2: for (int k = 0; k < 4; ++k) batch.push_back(static_cast<uint8_t>(rng())); break;           // extend
            case 3: batch[61 + rng() % (batch.size() - 61)] = static_cast<uint8_t>(rng()); break;               // byte rewrite
            case 4: batch[57] = static_cast<uint8_t>(rng() % 10); break;                                         // record count
            default: break;                                                                                      // untouched
            }
            for (Entity e : entities) {
                TopicWriteSpec spec;
                specFor(e, spec);
                for (int16_t level : {int16_t{4}, int16_t{5}}) {
                    const TypedCheck got = checkTypedBatch(spec, batch, level), want = referenceCheck(spec, batch, level);
                    if (got != want) {
                        std::string hex;
                        for (uint8_t b : batch) { char c[4]; std::snprintf(c, sizeof c, "%02x", b); hex += c; }
                        FAIL() << "seed " << seed << " iter " << iter << " entity " << int(e) << " level " << level << " got " << int(got)
                               << " want " << int(want) << " batch " << hex;
                    }
                }
            }
        }
    }
}

// ═══ Lease topics: the write spec's lease block and the system-column layout guard ═══

namespace {

rune::codec::Descriptor leaseDescriptor(uint16_t keyWidth = 64) {
    rune::codec::DescriptorBuilder d("inst");
    d.add("lease_version", PrimType::Int64).add("lease_ttl_ms", PrimType::UInt64)
        .add("lease_holder", PrimType::UInt64).add("lease_remaining_ms", PrimType::UInt64)
        .addFixedStr("id", keyWidth, 1, /*isKey=*/true);
    return d.build();
}

}  // namespace

TEST(LeaseWriteSpecRegression, WireRoundTrip) {
    rune::protocol::LeaseSpec ls;
    ls.ttlMs = 4242; ls.maxKeys = 9; ls.checkpoint = 0;
    TopicWriteSpecWire w;
    w.topicId = 6; w.keyFieldId = 4; w.keyType = PrimType::FixedStr; w.keyWidth = 128; w.keyIndex = true;
    w.lease = ls;
    EXPECT_EQ(kTopicWriteSpecLeaseWireLen, 48u);
    ASSERT_EQ(topicWriteSpecWireLen(w), 48u);
    std::vector<uint8_t> buf(48);
    ASSERT_EQ(encodeTopicWriteSpec(w, buf), 48u);
    EXPECT_EQ(buf[8] & 8, 8);
    const auto back = decodeTopicWriteSpec(buf);
    ASSERT_TRUE(back);
    EXPECT_EQ(*back, w);
    ASSERT_TRUE(back->lease.has_value());
    EXPECT_EQ(*back->lease, ls);

    // the 9 and 26 byte forms are unchanged
    TopicWriteSpecWire plain;
    plain.topicId = 6;
    uint8_t nine[kTopicWriteSpecWireLen];
    ASSERT_EQ(encodeTopicWriteSpec(plain, nine), 9u);
    EXPECT_TRUE(decodeTopicWriteSpec(nine));
    TopicWriteSpecWire ai;
    ai.topicId = 6; ai.autoIncrement = true; ai.partitionCount = 2;
    uint8_t tw[kTopicWriteSpecAutoIncWireLen];
    ASSERT_EQ(encodeTopicWriteSpec(ai, tw), 26u);
    EXPECT_TRUE(decodeTopicWriteSpec(tw));

    // lease + autoincrement together: refused on decode
    auto both = buf;
    both[8] |= 4;
    EXPECT_FALSE(decodeTopicWriteSpec(both));
    // a 48-byte frame without bit 3
    auto noBit = buf;
    noBit[8] &= static_cast<uint8_t>(~8u);
    EXPECT_FALSE(decodeTopicWriteSpec(noBit));
    // bit 3 on a 9-byte frame
    auto shortBit = std::vector<uint8_t>(buf.begin(), buf.begin() + 9);
    EXPECT_FALSE(decodeTopicWriteSpec(shortBit));
    // bits 4-7
    for (int bit = 4; bit < 8; ++bit) {
        auto b = buf;
        b[8] |= static_cast<uint8_t>(1u << bit);
        EXPECT_FALSE(decodeTopicWriteSpec(b)) << "bit " << bit;
    }
    // cut at every length
    for (std::size_t n = 0; n < 48; ++n) {
        const bool good = n == 9 || n == 26;
        const auto cut = decodeTopicWriteSpec(std::span<const uint8_t>(buf.data(), n));
        EXPECT_FALSE(cut.has_value()) << "cut to " << n << (good ? " (a legal length, but bit 3 is set)" : "");
    }
}

TEST(LeaseWriteSpecRegression, KeyWidthRealRegression) {
    rune::catalog::TopicObject lease = topicOf(leaseDescriptor(128), "id", true);
    lease.lease = rune::protocol::LeaseSpec{};
    const auto w = writeSpecWireOf(lease);
    ASSERT_TRUE(w);
    EXPECT_EQ(w->keyWidth, 128) << "a lease topic's key may be a string(128)";
    ASSERT_TRUE(w->lease.has_value());
    EXPECT_EQ(*w->lease, rune::protocol::LeaseSpec{});

    rune::catalog::TopicObject plain = topicOf(leaseDescriptor(128), "id", true);
    const auto p = writeSpecWireOf(plain);
    ASSERT_TRUE(p);
    EXPECT_EQ(p->keyWidth, 0) << "the same key on a non-lease topic is not a topic key";
    EXPECT_FALSE(p->lease.has_value());
}

TEST(LeaseWriteSpecRegression, LayoutBreakRefusedRegression) {
    TopicWriteSpecWire facts;
    facts.topicId = 9; facts.lease = rune::protocol::LeaseSpec{};

    auto bad = leaseDescriptor();
    bad.fields[0].offset = 8;   // lease_version moved off offset 0
    ASSERT_FALSE(leaseLayoutOk(bad));
    ASSERT_TRUE(leaseLayoutOk(leaseDescriptor()));

    TopicWriteSpec spec;
    EXPECT_EQ(spec.setFacts(facts), 0u);
    EXPECT_FALSE(spec.installVersion(1, bad));
    EXPECT_EQ(spec.descriptorFor(1), nullptr);
    EXPECT_TRUE(spec.installVersion(1, leaseDescriptor()));
    EXPECT_NE(spec.descriptorFor(1), nullptr);

    // the spec arrives AFTER the version (the production order: SchemaUpdate, then WriteSpecUpdate): the bad one is erased
    TopicWriteSpec late;
    EXPECT_TRUE(late.installVersion(1, bad));   // no lease known yet
    EXPECT_TRUE(late.installVersion(2, leaseDescriptor()));
    EXPECT_EQ(late.setFacts(facts), 1u);
    EXPECT_EQ(late.descriptorFor(1), nullptr);
    EXPECT_NE(late.descriptorFor(2), nullptr);

    // a non-lease spec guards nothing
    TopicWriteSpec nolease;
    EXPECT_TRUE(nolease.installVersion(1, bad));
    TopicWriteSpecWire plainFacts;
    plainFacts.topicId = 9;
    EXPECT_EQ(nolease.setFacts(plainFacts), 0u);
    EXPECT_NE(nolease.descriptorFor(1), nullptr);
}
