// CheckedWritesValidationMatrixTest.cpp — checked writes Part 02 Task 3: the validation matrices M1 (Op x Entity x
// Level x Position, 360 cells) and M2 (key type x variant x level, 84 cells) against checkTypedBatch.
#include <rune/node/WriteState.h>

#include "support/CheckedWritesBatches.h"

#include <gtest/gtest.h>

#include <tuple>

using namespace rune::node;
using namespace rune::node::cw;

namespace {

constexpr int kOps = 15, kEntities = 4, kLevels = 2, kPositions = 3;
constexpr int kKeyTypes = 7, kVariants = 6;

const char* const kEntityNames[] = {"TypedLegacy", "TypedPreUpgrade", "TypedKeyless", "TypedUncheckableKey"};
const char* const kPositionNames[] = {"Only", "First", "Last"};

Parts opParts(int op) {
    Parts p;
    switch (op + 1) {
    case 1:  break;                                                         // V01 valid
    case 2:  p.version = 0; break;                                          // V02 version 0
    case 3:  p.version = 2; break;                                          // V03 version = max + 1
    case 4:  p.blockLength = 29; break;                                     // V04 blockLength mismatch
    case 5:  p.truncateBy = 1; break;                                       // V05 truncated var field
    case 6:  p.kind = 9; break;                                             // V06 enum outside its whitelist
    case 7:  p.bio = std::string("\xC0\xAF", 2); break;                     // V07 overlong UTF-8 in a string
    case 8:  p.name = std::string("\x80", 1); break;                        // V08 stray continuation in string(16)
    case 9:  p.name = std::string("a\0b", 3); break;                        // V09 dirty string(16) padding
    case 10: p.key = leBytes(p.id + 1, 8); break;                           // V10 right width, other bytes
    case 11: p.key = leBytes(p.id, 4); break;                               // V11 4-byte key on a uint64 key
    case 12: p.tombstone = true; break;                                     // V12 tombstone with an 8-byte key
    case 13: p.tombstone = true; p.key = leBytes(p.id, 4); break;           // V13 tombstone with a 4-byte key
    case 14: p.blob = std::string("\xFF\xFE\x80", 3); break;                // V14 invalid UTF-8 in a bytes field
    case 15: break;                                                         // V15 lying key length (patched after the batch is built)
    }
    return p;
}

TypedCheck expected(int op1, int entity, int level, int /*position*/) {
    const bool l4 = level == 1;
    const auto e = static_cast<Entity>(entity);
    if (!l4 && e == Entity::TypedPreUpgrade) return TypedCheck::Accept;                                       // 1
    if (op1 == 15) return TypedCheck::Reject;                                                                 // 2
    if (op1 == 12 || op1 == 13) {                                                                             // 3
        if (e == Entity::TypedKeyless) return TypedCheck::Reject;
        if (e == Entity::TypedUncheckableKey || !l4) return TypedCheck::Accept;
        return op1 == 12 ? TypedCheck::Accept : TypedCheck::Reject;
    }
    if (op1 == 2) return TypedCheck::Reject;                                                                  // 4
    if (op1 == 3) return TypedCheck::VersionAhead;
    if (op1 == 4 || op1 == 5 || op1 == 6) return TypedCheck::Reject;                                          // 5
    if (op1 == 7 || op1 == 8 || op1 == 9) return l4 ? TypedCheck::Reject : TypedCheck::Accept;                // 6
    if (op1 == 10 || op1 == 11)                                                                               // 7
        return (l4 && (e == Entity::TypedLegacy || e == Entity::TypedPreUpgrade)) ? TypedCheck::Reject : TypedCheck::Accept;
    return TypedCheck::Accept;                                                                                // 8
}

std::vector<uint8_t> cellBatch(int op, int position) {
    std::vector<TypedRecord> recs;
    const std::size_t at = position == 0 ? 0 : position == 1 ? 0 : 2;
    const std::size_t n  = position == 0 ? 1 : 3;
    for (std::size_t i = 0; i < n; ++i) recs.push_back(i == at ? encode(opParts(op)) : validRecord(100 + i));
    auto batch = batchOf(recs);
    if (op + 1 == 15) batch = withLyingKeyLength(std::move(batch), at);
    return batch;
}

}  // namespace

using MatrixParam = std::tuple<int, int, int, int>;
class CheckedWritesValidationMatrixCell : public ::testing::TestWithParam<MatrixParam> {};

TEST_P(CheckedWritesValidationMatrixCell, CheckTypedBatchAgreesWithTheTable) {
    const auto [op, entity, level, position] = GetParam();
    TopicWriteSpec spec;
    specFor(static_cast<Entity>(entity), spec);
    const auto batch = cellBatch(op, position);
    const int16_t lv = level == 0 ? 4 : 5;
    EXPECT_EQ(checkTypedBatch(spec, batch, lv), expected(op + 1, entity, level, position));
}

namespace {
std::string cellName(const ::testing::TestParamInfo<MatrixParam>& i) {
    const auto [op, entity, level, position] = i.param;
    char buf[16];
    std::snprintf(buf, sizeof buf, "V%02d", op + 1);
    return std::string(buf) + "_" + kEntityNames[entity] + (level == 0 ? "_L3_" : "_L4_") + kPositionNames[position];
}
}  // namespace

INSTANTIATE_TEST_SUITE_P(CheckedWritesValidationMatrix, CheckedWritesValidationMatrixCell,
    ::testing::Combine(::testing::Range(0, kOps), ::testing::Range(0, kEntities), ::testing::Range(0, kLevels), ::testing::Range(0, kPositions)),
    cellName);

// ---- M2: the wire key ----------------------------------------------------------------------------------------------

namespace {

struct KeyType { const char* name; rune::codec::PrimType type; uint16_t width; };
const KeyType kKeyTypeTable[kKeyTypes] = {
    {"uint16", rune::codec::PrimType::UInt16, 2}, {"int32", rune::codec::PrimType::Int32, 4}, {"uint32", rune::codec::PrimType::UInt32, 4},
    {"int64", rune::codec::PrimType::Int64, 8},   {"uint64", rune::codec::PrimType::UInt64, 8}, {"string1", rune::codec::PrimType::FixedStr, 1},
    {"string16", rune::codec::PrimType::FixedStr, 16}};
const char* const kVariantNames[kVariants] = {"Exact", "OneShort", "OneLong", "SameWidthOtherBytes", "ZeroPlaceholderBoth", "ZeroKeyNonZeroField"};

rune::codec::Descriptor keyDescriptor(const KeyType& t) {
    rune::codec::DescriptorBuilder b("w");
    if (t.type == rune::codec::PrimType::FixedStr) b.addFixedStr("k", t.width);
    else b.add("k", t.type);
    return b.add("pad", rune::codec::PrimType::Int32).build();
}

}  // namespace

using WireKeyParam = std::tuple<int, int, int>;
class CheckedWritesWireKeyMatrixCell : public ::testing::TestWithParam<WireKeyParam> {};

TEST_P(CheckedWritesWireKeyMatrixCell, WireKeyMustEqualTheKeyField) {
    const auto [kt, variant, level] = GetParam();
    const KeyType& t = kKeyTypeTable[kt];
    TopicWriteSpec spec;
    TopicWriteSpecWire w;
    w.topicId = 1; w.keyFieldId = 0; w.keyType = t.type; w.keyWidth = static_cast<uint8_t>(t.width); w.legacyValidate = true;
    spec.installVersion(1, keyDescriptor(t));
    spec.setFacts(w);
    const uint32_t blockLength = t.width + 4u;
    std::string field(t.width, '\0');
    for (uint16_t i = 0; i < t.width; ++i) field[i] = static_cast<char>('A' + i);   // non-zero, valid UTF-8 for the string types
    if (variant == 4) field.assign(t.width, '\0');
    std::string key = field;
    switch (variant) {
    case 1: key.pop_back(); break;
    case 2: key.push_back('x'); break;
    case 3: key[0] = static_cast<char>(key[0] ^ 0x01); break;
    case 5: key.assign(t.width, '\0'); break;
    default: break;
    }
    std::string value = leBytes(blockLength, 2) + leBytes(1, 2) + field + std::string(4, '\0');
    const auto batch = batchOf({{key, value}});
    const bool l4 = level == 1;
    const bool accept = !l4 || variant == 0 || variant == 4;
    EXPECT_EQ(checkTypedBatch(spec, batch, l4 ? 5 : 4), accept ? TypedCheck::Accept : TypedCheck::Reject);
}

namespace {
std::string wireKeyCellName(const ::testing::TestParamInfo<WireKeyParam>& i) {
    const auto [kt, variant, level] = i.param;
    return std::string(kKeyTypeTable[kt].name) + "_" + kVariantNames[variant] + (level == 0 ? "_L3" : "_L4");
}
}  // namespace

INSTANTIATE_TEST_SUITE_P(CheckedWritesWireKeyMatrix, CheckedWritesWireKeyMatrixCell,
    ::testing::Combine(::testing::Range(0, kKeyTypes), ::testing::Range(0, kVariants), ::testing::Range(0, kLevels)),
    wireKeyCellName);

TEST(CheckedWritesValidationMatrixCountRegression, CellCountsArePinned) {
    int m1 = 0, m2 = 0;
    const auto& ut = *::testing::UnitTest::GetInstance();
    for (int i = 0; i < ut.total_test_suite_count(); ++i) {
        const auto* s = ut.GetTestSuite(i);
        if (std::string_view(s->name()) == "CheckedWritesValidationMatrix/CheckedWritesValidationMatrixCell") m1 = s->total_test_count();
        if (std::string_view(s->name()) == "CheckedWritesWireKeyMatrix/CheckedWritesWireKeyMatrixCell") m2 = s->total_test_count();
    }
    EXPECT_EQ(m1, 360);
    EXPECT_EQ(m2, 84);
}
