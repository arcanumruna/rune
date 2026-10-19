// Catalog history: THE topic / view name rule (rune/catalog/TopicName.h), moved from the
// broker's rune/node/TopicName.h into the frozen formatVer-1 apply rules.  The library
// function is pinned byte-for-byte to today's broker rule (every byte, every position),
// and apply (CreateTopic, CreateView) refuses every name class it rejects.
#include <rune/catalog/CatalogState.h>
#include <rune/catalog/CatalogFingerprint.h>
#include <rune/catalog/TopicName.h>
#include <rune/codec/Descriptor.h>
#include <rune/codec/Serialize.h>
#include <gtest/gtest.h>

#include <cctype>
#include <string>
#include <vector>

namespace rc = rune::catalog;
using rc::checkTopicName;
using rc::isValidTopicName;
using rc::topicNamesFoldEqual;
using rc::TopicNameError;

static_assert(isValidTopicName("orders"));
static_assert(!isValidTopicName(""));
static_assert(!isValidTopicName("__views"));
static_assert(!isValidTopicName("__catalog"));
static_assert(topicNamesFoldEqual("Orders", "oRDERS"));
static_assert(rc::kMaxTopicNameBytes == 249);

// ── the library function: today's broker rule, exactly ────────────────────────────
TEST(CatalogTopicNameRegression, AcceptsSafeSegments) {
    for (const char* n : {"orders", "Orders.v2", "a", "_x", "-", ".hidden", "a-b_c.D9"})
        EXPECT_EQ(checkTopicName(n), TopicNameError::None) << n;
    EXPECT_EQ(checkTopicName(std::string(249, 'x')), TopicNameError::None) << "249 bytes is the max";
}

TEST(CatalogTopicNameRegression, RejectsEmptyTooLongAndDotSegments) {
    EXPECT_EQ(checkTopicName(""), TopicNameError::Empty);
    EXPECT_EQ(checkTopicName(std::string(250, 'x')), TopicNameError::TooLong);
    EXPECT_EQ(checkTopicName("."), TopicNameError::DotSegment);
    EXPECT_EQ(checkTopicName(".."), TopicNameError::DotSegment);
}

TEST(CatalogTopicNameRegression, RejectsPathControlAndNonAsciiBytes) {
    for (const std::string& n : {std::string("a/b"), std::string("../x"), std::string("/abs"),
                                std::string("a\\b"), std::string("has space"), std::string("tab\tx"),
                                std::string("caf\xC3\xA9"), std::string("nul\0b", 5), std::string("colon:x")})
        EXPECT_EQ(checkTopicName(n), TopicNameError::BadChar) << rc::escapeNameForLog(n);
}

TEST(CatalogTopicNameRegression, ReservesTheDoubleUnderscorePrefix) {
    for (const char* n : {"__catalog", "__topic_schemas", "__views", "__consumer_offsets", "__transaction_state",
                          "__cluster_metadata", "__view_snapshots", "__orphan.3", "__"})
        EXPECT_EQ(checkTopicName(n), TopicNameError::Reserved) << n;
    EXPECT_EQ(checkTopicName("_single"), TopicNameError::None);
}

TEST(CatalogTopicNameRegression, RuleExceptionsStillApplyToAllowedBytes) {
    EXPECT_EQ(checkTopicName("..."), TopicNameError::None);
    EXPECT_EQ(checkTopicName("__x"), TopicNameError::Reserved);
    EXPECT_EQ(checkTopicName("_"), TopicNameError::None);
    EXPECT_EQ(checkTopicName("a__"), TopicNameError::None);
    EXPECT_EQ(checkTopicName(std::string(250, '_')), TopicNameError::TooLong) << "length is checked first";
    EXPECT_EQ(checkTopicName("__a/b"), TopicNameError::BadChar) << "bytes are checked before the prefix";
}

TEST(CatalogTopicNameRegression, EveryErrorHasAHumanReadableText) {
    EXPECT_EQ(rc::topicNameErrorText(TopicNameError::Empty), "empty");
    EXPECT_EQ(rc::topicNameErrorText(TopicNameError::TooLong), "longer than 249 bytes");
    EXPECT_EQ(rc::topicNameErrorText(TopicNameError::DotSegment), "'.' or '..'");
    EXPECT_EQ(rc::topicNameErrorText(TopicNameError::BadChar), "a byte outside [A-Za-z0-9._-]");
    EXPECT_EQ(rc::topicNameErrorText(TopicNameError::Reserved), "the '__' prefix is reserved for internal topics");
    EXPECT_EQ(rc::topicNameErrorText(TopicNameError::None), "ok");
}

TEST(CatalogTopicNameRegression, EscapeForLogKeepsPrintableAsciiOnly) {
    EXPECT_EQ(rc::escapeNameForLog("ok.name"), "ok.name");
    EXPECT_EQ(rc::escapeNameForLog(std::string("a\nb\0\x1b\\", 6)), "a\\x0ab\\x00\\x1b\\\\");
    EXPECT_EQ(rc::escapeNameForLog("caf\xC3\xA9"), "caf\\xc3\\xa9");
}

namespace {
bool byteIsAllowed(int b) {
    return (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z') || (b >= '0' && b <= '9') || b == '.' || b == '_' || b == '-';
}
enum class Pos { Alone, Start, Middle, End };
std::string nameWithByte(int b, Pos pos) {
    const char c = static_cast<char>(b);
    switch (pos) {
        case Pos::Alone:  return std::string(1, c);
        case Pos::Start:  return std::string(1, c) + "abc";
        case Pos::Middle: return std::string("ab") + c + "cd";
        case Pos::End:    return std::string("abc") + c;
    }
    return {};
}
struct ByteCase { int byte; Pos pos; };
std::vector<ByteCase> allByteCases() {
    std::vector<ByteCase> v;
    for (int b = 0; b < 256; ++b)
        for (Pos p : {Pos::Alone, Pos::Start, Pos::Middle, Pos::End}) v.push_back({b, p});
    return v;
}
TopicNameError expectedFor(int b, Pos pos) {
    if (!byteIsAllowed(b)) return TopicNameError::BadChar;
    if (pos == Pos::Alone && b == '.') return TopicNameError::DotSegment;
    return TopicNameError::None;
}
} // namespace

TEST(CatalogTopicNameRegression, EveryByteInEveryPositionIsAcceptedExactlyWhenAllowed) {
    for (const auto& [b, pos] : allByteCases())
        EXPECT_EQ(checkTopicName(nameWithByte(b, pos)), expectedFor(b, pos)) << "byte " << b << " pos " << static_cast<int>(pos);
}

TEST(CatalogTopicNameRegression, FoldEqualityIsAsciiLetterCaseOnly) {
    EXPECT_TRUE(topicNamesFoldEqual("Orders", "orders"));
    EXPECT_FALSE(topicNamesFoldEqual("orders", "orders "));
    EXPECT_FALSE(topicNamesFoldEqual("caf\xC3\xA9", "CAF\xC3\x89")) << "non-ASCII bytes are never folded";
    for (int a = 0; a < 256; ++a)
        for (int b = 0; b < 256; ++b) {
            const bool letters = a < 128 && b < 128 && std::isalpha(a) && std::isalpha(b);
            const bool want = a == b || (letters && std::tolower(a) == std::tolower(b));
            EXPECT_EQ(topicNamesFoldEqual(std::string(1, static_cast<char>(a)), std::string(1, static_cast<char>(b))), want)
                << a << " vs " << b;
        }
}

// ── the apply rules refuse every name class ───────────────────────────────────────
namespace {
std::vector<uint8_t> outBlob() {
    rune::codec::DescriptorBuilder b("out");
    b.add("id", rune::codec::PrimType::Int64, 1, true);
    return rune::codec::serialize(b.build());
}
rc::CatalogEvent createTopicEvent(const rc::CatalogState& s, std::string name) {
    rc::CreateTopicOps o; o.topicId = s.nextTopicId();
    rc::CatalogEvent e; e.kind = rc::EventKind::CreateTopic; e.ops = o; e.objectName = std::move(name);
    e.objectId = s.nextObjectId(); e.newVersion = 1;
    return e;
}
rc::CatalogEvent createViewEvent(const rc::CatalogState& s, std::string name) {
    rc::ViewDef v; v.viewTopicId = s.nextTopicId(); v.keyField = "id"; v.sourceNames = {"Orders"};
    v.outputDescriptorBlob = outBlob(); v.normalizedPlanBlob = {1};
    rc::CatalogEvent e; e.kind = rc::EventKind::CreateView; e.ops = rc::CreateViewOps{v}; e.objectName = std::move(name);
    e.objectId = s.nextObjectId(); e.newVersion = 1;
    return e;
}
rc::CatalogState withOrders() {   // a live topic "Orders" (and a live view "Totals" over it)
    rc::CatalogState s;
    auto t = createTopicEvent(s, "Orders");
    t.fingerprint = rc::previewFingerprint(s, t).value();
    EXPECT_TRUE(rc::apply(s, t, 0));
    auto v = createViewEvent(s, "Totals");
    v.fingerprint = rc::previewFingerprint(s, v).value();
    EXPECT_TRUE(rc::apply(s, v, 1));
    return s;
}
struct NameCase { const char* what; std::string name; const char* detail; };
std::vector<NameCase> invalidNames() {
    return {
        {"empty", "", "(empty)"},
        {"too long", std::string(250, 'x'), "(longer than 249 bytes)"},
        {"dot", ".", "('.' or '..')"},
        {"dot dot", "..", "('.' or '..')"},
        {"slash", "a/b", "(a byte outside"},
        {"traversal", "../x", "(a byte outside"},
        {"backslash", "a\\b", "(a byte outside"},
        {"space", "has space", "(a byte outside"},
        {"control", "line\nbreak", "'line\\x0abreak'"},   // the detail escapes it
        {"nul", std::string("nul\0b", 5), "(a byte outside"},
        {"non-ascii", "caf\xC3\xA9", "(a byte outside"},
        {"reserved", "__catalog", "(the '__' prefix is reserved"},
        {"reserved bare", "__", "(the '__' prefix is reserved"},
        {"folds onto a topic", "orders", "case-folds onto the live name 'Orders'"},
        {"folds onto a view", "TOTALS", "case-folds onto the live name 'Totals'"},
    };
}
} // namespace

TEST(CatalogTopicNameRegression, CreateTopicAndCreateViewRefuseEveryInvalidName) {
    for (const auto& c : invalidNames()) {
        for (const bool view : {false, true}) {
            rc::CatalogState s = withOrders();
            const uint64_t before = s.digest();
            const rc::CatalogEvent e = view ? createViewEvent(s, c.name) : createTopicEvent(s, c.name);
            auto p = rc::previewFingerprint(s, e);
            ASSERT_FALSE(p.has_value()) << c.what;
            auto a = rc::apply(s, e, 2);
            ASSERT_FALSE(a.has_value()) << c.what << (view ? " (view)" : " (topic)");
            EXPECT_EQ(a.error().code, rc::ApplyError::Code::Precondition) << c.what;
            EXPECT_NE(a.error().detail.find(c.detail), std::string::npos) << c.what << ": " << a.error().detail;
            EXPECT_EQ(a.error().detail.find('\n'), std::string::npos) << "a rejected name never reaches a detail raw";
            EXPECT_EQ(s.digest(), before) << c.what;
        }
    }
}

TEST(CatalogTopicNameRegression, ValidNamesAtTheEdgesAreAccepted) {
    for (const std::string& n : {std::string(249, 'y'), std::string("_x"), std::string("..."), std::string("a-b_c.D9"),
                                std::string("Orders2")}) {
        for (const bool view : {false, true}) {
            rc::CatalogState s = withOrders();
            rc::CatalogEvent e = view ? createViewEvent(s, n) : createTopicEvent(s, n);
            auto fp = rc::previewFingerprint(s, e);
            ASSERT_TRUE(fp.has_value()) << n << ": " << fp.error().detail;
            e.fingerprint = *fp;
            EXPECT_TRUE(rc::apply(s, e, 2)) << n;
        }
    }
}

TEST(CatalogTopicNameRegression, ADroppedNamesCaseVariantIsFreeAgain) {
    rc::CatalogState s;
    auto t = createTopicEvent(s, "Orders");
    t.fingerprint = rc::previewFingerprint(s, t).value();
    ASSERT_TRUE(rc::apply(s, t, 0));
    rc::CatalogEvent d; d.kind = rc::EventKind::DropTopic; d.ops = rc::DropTopicOps{0}; d.objectName = "Orders";
    d.objectId = 1; d.prevVersion = 1; d.newVersion = 2;
    d.fingerprint = rc::previewFingerprint(s, d).value();
    ASSERT_TRUE(rc::apply(s, d, 1));
    auto again = createTopicEvent(s, "orders");
    auto fp = rc::previewFingerprint(s, again);
    ASSERT_TRUE(fp.has_value()) << fp.error().detail;
    again.fingerprint = *fp;
    EXPECT_TRUE(rc::apply(s, again, 2)) << "the fold rule looks at LIVE names only";
}
