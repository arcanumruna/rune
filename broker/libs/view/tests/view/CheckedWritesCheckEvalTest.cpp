// libs/view/tests/view/CheckedWritesCheckEvalTest.cpp
//
// Three-valued check-predicate evaluation on row images, padded slot constants, the changed-column test and the
// constraint shape rules.
#include <gtest/gtest.h>
#include <AllocWindow.h>
#include <cmath>
#include <cstring>
#include <limits>
#include <tuple>
#include <rune/codec/Descriptor.h>
#include <rune/view/CheckEval.h>
#include <rune/view/PlanCompiler.h>

namespace {

using rune::codec::FieldValue;
using rune::codec::PrimType;
using namespace rune::view;

enum Col : uint16_t { Characters, Balance, Total, Ratio, Ok, Plat, Tag, Small };

rune::codec::Descriptor rowDescriptor() {
    return rune::codec::DescriptorBuilder("check_row", 1)
        .add("characters", PrimType::Int64).add("balance", PrimType::Int64).add("total", PrimType::UInt64)
        .add("ratio", PrimType::Float64).add("ok", PrimType::Bool).add("plat", PrimType::Enum)
        .addFixedStr("tag", 8).add("small", PrimType::UInt16).build();
}

const RowLayout& layout() {
    static const RowLayout l = buildRowLayout(rowDescriptor());
    return l;
}

FieldValue intValue(int64_t v) { FieldValue f; f.present = true; f.i = v; return f; }
FieldValue uintValue(uint64_t v) { FieldValue f; f.present = true; f.u = v; return f; }
FieldValue floatValue(double v) { FieldValue f; f.present = true; f.f = v; return f; }
FieldValue boolValue(bool v) { FieldValue f; f.present = true; f.b = v; return f; }
FieldValue strValue(std::string_view v) { FieldValue f; f.present = true; f.bytes = v; return f; }

struct Row {
    std::vector<uint8_t> bytes = std::vector<uint8_t>(layout().rowLen, 0);
    Row& set(Col c, FieldValue v) { writeSlot(bytes.data(), layout(), c, v); return *this; }
    Row& null(Col c) { setNull(bytes.data(), layout(), c); return *this; }
    const uint8_t* data() const { return bytes.data(); }
};

CompiledPredicateNode column(Col c) { CompiledPredicateNode n{PredicateOp::Column}; n.outSlot = c; return n; }
CompiledPredicateNode constant(Col c, FieldValue v) {
    CompiledPredicateNode n{PredicateOp::Const};
    n.constType = layout().slotType[c];
    encodeSlotValue(n.constBytes.data(), n.constType, layout().slotWidth[c], v);
    return n;
}
CompiledPredicateNode node(PredicateOp op, uint16_t a, uint16_t b = 0) { CompiledPredicateNode n{op}; n.a = a; n.b = b; return n; }

PredicateResult eval(const std::vector<CompiledPredicateNode>& nodes, const Row& row) {
    return evaluatePredicate(nodes, layout(), row.data());
}

// [Column c, Const v, op(0, 1)]
std::vector<CompiledPredicateNode> compareNodes(PredicateOp op, Col c, FieldValue v) {
    return {column(c), constant(c, v), node(op, 0, 1)};
}

constexpr PredicateOp kOps[] = {PredicateOp::Eq, PredicateOp::Ne, PredicateOp::Lt, PredicateOp::Le, PredicateOp::Gt, PredicateOp::Ge};

struct TypeCase {
    Col col;
    FieldValue constant, below, equal, above;
};

TypeCase typeCase(int i) {
    switch (i) {
        case 0: return {Characters, intValue(0), intValue(-1), intValue(0), intValue(1)};
        case 1: return {Balance, intValue(-5), intValue(std::numeric_limits<int64_t>::min()), intValue(-5), intValue(std::numeric_limits<int64_t>::max())};
        case 2: return {Total, uintValue(1), uintValue(0), uintValue(1), uintValue(0xFFFF'FFFF'FFFF'FFFFull)};
        case 3: return {Ratio, floatValue(1.5), floatValue(-2.0), floatValue(1.5), floatValue(1e300)};
        case 4: return {Ok, boolValue(true), boolValue(false), boolValue(true), boolValue(true)};
        case 5: return {Plat, uintValue(3), uintValue(2), uintValue(3), uintValue(4)};
        case 6: return {Tag, strValue("abd"), strValue("abc"), strValue("abd"), strValue("abe")};
        default: return {Small, uintValue(300), uintValue(299), uintValue(300), uintValue(65535)};
    }
}

bool holds(PredicateOp op, int c) {
    switch (op) {
        case PredicateOp::Eq: return c == 0;
        case PredicateOp::Ne: return c != 0;
        case PredicateOp::Lt: return c < 0;
        case PredicateOp::Le: return c <= 0;
        case PredicateOp::Gt: return c > 0;
        default: return c >= 0;
    }
}

class CheckedWritesCheckEvalMatrix : public ::testing::TestWithParam<std::tuple<int, int>> {};

} // namespace

TEST_P(CheckedWritesCheckEvalMatrix, EveryComparisonOnEveryType) {
    const PredicateOp op = kOps[std::get<0>(GetParam())];
    const TypeCase t = typeCase(std::get<1>(GetParam()));
    const auto nodes = compareNodes(op, t.col, t.constant);
    const FieldValue values[] = {t.below, t.equal, t.above};
    for (int pos = 0; pos < 3; ++pos) {
        if (t.col == Ok && pos == 2) continue;   // Bool has no value above true
        Row row;
        row.set(t.col, values[pos]);
        EXPECT_EQ(eval(nodes, row), holds(op, pos - 1) ? PredicateResult::True : PredicateResult::False)
            << "op " << static_cast<int>(op) << " type " << std::get<1>(GetParam()) << " position " << pos;
    }
}

INSTANTIATE_TEST_SUITE_P(Matrix, CheckedWritesCheckEvalMatrix, ::testing::Combine(::testing::Range(0, 6), ::testing::Range(0, 8)));

TEST(CheckedWritesCheckEval, CountLimitOnTwelveAndThirteen) {
    const auto nodes = compareNodes(PredicateOp::Le, Characters, intValue(12));
    EXPECT_EQ(eval(nodes, Row().set(Characters, intValue(12))), PredicateResult::True);
    EXPECT_EQ(eval(nodes, Row().set(Characters, intValue(13))), PredicateResult::False);
}

TEST(CheckedWritesCheckEval, ConstOnTheLeftMirrorsTheOperator) {
    const std::vector<CompiledPredicateNode> nodes = {constant(Small, uintValue(5)), column(Small), node(PredicateOp::Lt, 0, 1)};
    EXPECT_EQ(eval(nodes, Row().set(Small, uintValue(6))), PredicateResult::True);
    EXPECT_EQ(eval(nodes, Row().set(Small, uintValue(5))), PredicateResult::False);
}

TEST(CheckedWritesCheckEval, KleeneAndOrNot) {
    // Three comparisons over `small` produce the three truth values: True (= 1), False (= 2), Unknown (null column).
    const PredicateResult all[] = {PredicateResult::False, PredicateResult::True, PredicateResult::Unknown};
    auto truthNodes = [](PredicateResult r) -> std::vector<CompiledPredicateNode> {
        const Col c = r == PredicateResult::Unknown ? Total : Small;
        return {column(c), constant(c, uintValue(r == PredicateResult::False ? 2 : 1)), node(PredicateOp::Eq, 0, 1)};
    };
    Row row;
    row.set(Small, uintValue(1)).null(Total);
    auto combine = [&](PredicateOp op, PredicateResult x, PredicateResult y) {
        auto nodes = truthNodes(x);
        const auto rhs = truthNodes(y);
        for (auto n : rhs) {
            if (n.op == PredicateOp::Eq) { n.a += 3; n.b += 3; }
            nodes.push_back(n);
        }
        nodes.push_back(node(op, 2, 5));
        return eval(nodes, row);
    };
    for (const auto x : all) {
        for (const auto y : all) {
            const bool anyFalse = x == PredicateResult::False || y == PredicateResult::False;
            const bool anyTrue = x == PredicateResult::True || y == PredicateResult::True;
            const bool anyUnknown = x == PredicateResult::Unknown || y == PredicateResult::Unknown;
            EXPECT_EQ(combine(PredicateOp::And, x, y), anyFalse ? PredicateResult::False : anyUnknown ? PredicateResult::Unknown : PredicateResult::True);
            EXPECT_EQ(combine(PredicateOp::Or, x, y), anyTrue ? PredicateResult::True : anyUnknown ? PredicateResult::Unknown : PredicateResult::False);
        }
        auto nodes = truthNodes(x);
        nodes.push_back(node(PredicateOp::Not, 2));
        EXPECT_EQ(eval(nodes, row), x == PredicateResult::Unknown ? x : x == PredicateResult::True ? PredicateResult::False : PredicateResult::True);
    }
}

TEST(CheckedWritesCheckEvalRegression, NullAndNonFiniteAreUnknownAndPassRegression) {
    const auto ge = compareNodes(PredicateOp::Ge, Balance, intValue(0));
    EXPECT_EQ(eval(ge, Row().null(Balance)), PredicateResult::Unknown);
    auto negated = ge;
    negated.push_back(node(PredicateOp::Not, 2));
    EXPECT_EQ(eval(negated, Row().null(Balance)), PredicateResult::Unknown);

    const auto le = compareNodes(PredicateOp::Le, Ratio, floatValue(1.5));
    EXPECT_EQ(eval(le, Row().set(Ratio, floatValue(std::nan("")))), PredicateResult::Unknown);
    EXPECT_EQ(eval(le, Row().set(Ratio, floatValue(std::numeric_limits<double>::infinity()))), PredicateResult::Unknown);
    EXPECT_EQ(eval(le, Row().set(Ratio, floatValue(2.0))), PredicateResult::False);
}

TEST(CheckedWritesCheckEval, ConstantsDecodeFromThePaddedSlotEncoding) {
    auto withBytes = [](Col c, std::initializer_list<uint8_t> bytes) {
        CompiledPredicateNode n{PredicateOp::Const};
        n.constType = layout().slotType[c];
        std::copy(bytes.begin(), bytes.end(), n.constBytes.begin());
        return n;
    };
    auto equalTo = [&](Col c, CompiledPredicateNode k, const Row& row) {
        return eval({column(c), k, node(PredicateOp::Eq, 0, 1)}, row);
    };
    EXPECT_EQ(equalTo(Characters, withBytes(Characters, {0x0C}), Row().set(Characters, intValue(12))), PredicateResult::True);
    EXPECT_EQ(equalTo(Small, withBytes(Small, {0xFF, 0xFF}), Row().set(Small, uintValue(65535))), PredicateResult::True);
    EXPECT_EQ(equalTo(Ok, withBytes(Ok, {0x01}), Row().set(Ok, boolValue(true))), PredicateResult::True);
    EXPECT_EQ(equalTo(Tag, withBytes(Tag, {0x61, 0x62, 0x63}), Row().set(Tag, strValue("abc"))), PredicateResult::True);

    auto junk = withBytes(Tag, {0x61, 0x62, 0x63});
    junk.constBytes[8] = 0x7F;   // past the 8-byte slot width
    junk.constBytes[15] = 0x7F;
    EXPECT_EQ(equalTo(Tag, junk, Row().set(Tag, strValue("abc"))), PredicateResult::True);
    auto wide = withBytes(Small, {0x2C, 0x01, 0xAA, 0xBB});   // 300 then bytes past the 2-byte width
    EXPECT_EQ(equalTo(Small, wide, Row().set(Small, uintValue(300))), PredicateResult::True);
}

TEST(CheckedWritesCheckEval, ReadsChangedComparesOnlyTheReadColumns) {
    const auto nodes = compareNodes(PredicateOp::Le, Characters, intValue(12));
    const Row base = Row().set(Characters, intValue(5)).set(Tag, strValue("aa"));
    EXPECT_FALSE(predicateReadsChanged(nodes, layout(), base.data(), Row(base).set(Tag, strValue("bb")).data()));
    EXPECT_TRUE(predicateReadsChanged(nodes, layout(), base.data(), Row(base).set(Characters, intValue(6)).data()));
    EXPECT_TRUE(predicateReadsChanged(nodes, layout(), nullptr, base.data()));
    EXPECT_TRUE(predicateReadsChanged(nodes, layout(), base.data(), Row(base).null(Characters).data()));
    EXPECT_FALSE(predicateReadsChanged(nodes, layout(), Row().null(Characters).data(), Row().null(Characters).data()));
}

TEST(CheckedWritesCheckEval, DeepTreeEvaluatesWithoutAllocation) {
    // 2 leaves, 31 comparisons and 30 ands = 63 nodes, folded as a balanced tree.
    std::vector<CompiledPredicateNode> nodes = {column(Characters), constant(Characters, intValue(12))};
    std::vector<uint16_t> level;
    for (int i = 0; i < 31; ++i) {
        nodes.push_back(node(PredicateOp::Le, 0, 1));
        level.push_back(static_cast<uint16_t>(nodes.size() - 1));
    }
    while (level.size() > 1) {
        std::vector<uint16_t> next;
        for (std::size_t i = 0; i + 1 < level.size(); i += 2) {
            nodes.push_back(node(PredicateOp::And, level[i], level[i + 1]));
            next.push_back(static_cast<uint16_t>(nodes.size() - 1));
        }
        if (level.size() % 2) next.push_back(level.back());
        level = std::move(next);
    }
    ASSERT_EQ(nodes.size(), 63u);
    const Row small = Row().set(Characters, intValue(12));
    const Row big = Row().set(Characters, intValue(13));
    EXPECT_EQ(eval(nodes, small), PredicateResult::True);
    rune::testing::AllocWindow w;
    const PredicateResult a = eval(nodes, small);
    const PredicateResult b = eval(nodes, big);
    EXPECT_EQ(w.count(), 0u);
    EXPECT_EQ(a, PredicateResult::True);
    EXPECT_EQ(b, PredicateResult::False);
}

namespace {

PredicateNode authoredColumn(const char* c) { return {PredicateOp::Column, 0, 0, c}; }
PredicateNode authoredConst(PrimType t, uint8_t first) {
    PredicateNode n{PredicateOp::Const, 0, 0, ""};
    n.constType = t;
    n.constBytes[0] = first;
    return n;
}

std::expected<CompileResult, CompileError> compileConstraint(std::vector<PredicateNode> predicate) {
    const auto source = rune::codec::DescriptorBuilder("wide", 1)
                            .add("id", PrimType::UInt64, 1, true).addFixedStr("subject", 64).add("n", PrimType::Int64).build();
    ViewPlan v;
    v.viewName = "wide_view";
    v.sources = {{5, "id"}};
    v.keyComponents = {{"id", 0, "id"}};
    v.output = {{"subject", PrimType::FixedStr, 0, "subject", MergeRule::Replace}, {"n", PrimType::Int64, 0, "n", MergeRule::Replace}};
    v.constraints = {{"c1", std::move(predicate)}};
    const rune::codec::Descriptor* srcs[] = {&source};
    return compileView(v, srcs, 0x40000007);
}

} // namespace

TEST(CheckedWritesPlanCompile, CheckOnAWideColumnIsConstraintInvalid) {
    const auto r = compileConstraint({authoredColumn("subject"), authoredConst(PrimType::FixedStr, 'a'), PredicateNode{PredicateOp::Eq, 0, 1, ""}});
    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error().kind, CompileErrorKind::ConstraintInvalid);
    const std::string tail = "constraint 'c1': check compares fixed-width columns of at most 16 bytes; 'subject' is string(64)";
    ASSERT_GE(r.error().message.size(), tail.size()) << r.error().message;
    EXPECT_EQ(r.error().message.substr(r.error().message.size() - tail.size()), tail);
}

TEST(CheckedWritesPlanCompile, SixtyFivePredicateNodesAreRefused) {
    auto chain = [](std::size_t count) {
        std::vector<PredicateNode> nodes = {authoredColumn("n"), authoredConst(PrimType::Int64, 0), PredicateNode{PredicateOp::Ge, 0, 1, ""}};
        while (nodes.size() < count) nodes.push_back(PredicateNode{PredicateOp::And, static_cast<uint16_t>(nodes.size() - 1), 2, ""});
        return nodes;
    };
    EXPECT_TRUE(compileConstraint(chain(64)).has_value());
    const auto r = compileConstraint(chain(65));
    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error().kind, CompileErrorKind::ConstraintInvalid);
    EXPECT_NE(r.error().message.find("constraint 'c1': at most 64 predicate nodes"), std::string::npos) << r.error().message;
}
