// Part 07 Task 14: GetViewRowK and ScanViewRows through a real Worker + WorkerViewSet (the GetViewRow pipeline).
#include <gtest/gtest.h>

#include <set>
#include <vector>

#include <rune/protocol/client/DataMessages.h>
#include <rune/view/ViewRow.h>

#include "support/CheckedWritesViewReadRig.h"

namespace {

using namespace cwkeyed;
using rune::node::WorkerOp;
using rune::node::WorkerReturn;
using rune::node::test::CwViewReadWorkerRig;
using rune::node::test::appendRecords;
using Status = rune::protocol::client::ViewRowResponse;

constexpr int16_t kOk = Status::kOk;

std::vector<Rec> userRows(uint64_t user, uint64_t firstChar, int n) {
    std::vector<Rec> rs;
    for (int i = 0; i < n; ++i) rs.push_back(Rec{firstChar + static_cast<uint64_t>(i), user, "n", static_cast<uint16_t>(100 + i)});
    return rs;
}

WorkerReturn getViewRow72(CwViewReadWorkerRig& r, int32_t view, uint64_t key) {
    WorkerOp op;
    op.type = WorkerOp::Type::GetViewRow; op.topicId = view; op.partition = 0; op.sessionId = 1; op.correlationId = 7;
    op.nextOffset = static_cast<int64_t>(key);
    EXPECT_TRUE(r.w.tryEnqueueOp(op));
    auto answers = r.pass(r.now += 1'000'000);
    EXPECT_EQ(answers.size(), 1u);
    return answers.empty() ? WorkerReturn{} : std::move(answers.front());
}

}  // namespace

TEST(CheckedWritesViewRead, PointKeyReadServesTheRow) {
    CwViewReadWorkerRig r;
    const auto c = accountCharacters(r.src);
    r.install(c);
    r.append({{1, 42, "a", 10}});
    r.append({{2, 42, "b", 20}});
    r.append({{3, 43, "c", 30}});

    const WorkerReturn ans = r.getK(beKey({42, 2}));
    EXPECT_EQ(ans.type, WorkerReturn::Type::ViewRowResult);
    ASSERT_EQ(ans.errorCode, kOk);
    EXPECT_EQ(ans.recordCount, static_cast<int32_t>(c.rowLayout.rowLen));
    ASSERT_EQ(ans.chunk.size(), c.rowLayout.rowLen);
    EXPECT_EQ(rune::view::readSlot(ans.chunk.data(), c.rowLayout, 2).u, 20u);
    EXPECT_EQ(r.getK(beKey({42, 9})).errorCode, Status::kAbsentKey);
    EXPECT_EQ(r.w.getViewRowKsForTest(), 2u);
}

TEST(CheckedWritesViewRead, ScanServesOnePageInOneStep) {
    CwViewReadWorkerRig r;
    const auto c = accountCharacters(r.src);
    r.install(c);
    r.append(userRows(42, 1, 12));
    r.append(userRows(43, 100, 3));

    ASSERT_TRUE(rune::node::test::postViewRead(r.w, WorkerOp::Type::ScanViewRows, beKey({42}), {}, kViewTopic));
    const auto answers = r.pass(r.now += 1'000'000);   // one doWork pass
    ASSERT_EQ(answers.size(), 1u);
    EXPECT_EQ(answers[0].type, WorkerReturn::Type::ViewScanResult);
    const auto page = rune::node::test::parseScanReturn(answers[0]);
    EXPECT_EQ(page.code, kOk);
    EXPECT_EQ(page.rowCount, 12);
    ASSERT_EQ(page.rows.size(), 12u);
    EXPECT_TRUE(page.token.empty());
    std::set<uint64_t> chars;   // a page is in table order, not key order
    for (const auto& row : page.rows) {
        EXPECT_EQ(rune::view::readSlot(row.data(), c.rowLayout, 0).u, 42u);
        chars.insert(rune::view::readSlot(row.data(), c.rowLayout, 1).u);
    }
    EXPECT_EQ(chars.size(), 12u);
    EXPECT_EQ(*chars.begin(), 1u);
    EXPECT_EQ(*chars.rbegin(), 12u);
    EXPECT_EQ(r.w.scanViewRowsForTest(), 1u);
}

TEST(CheckedWritesViewRead, EmptyPrefixPageIsOkWithNoRowsAndNoToken) {
    CwViewReadWorkerRig r;
    const auto c = accountCharacters(r.src);
    r.install(c);
    r.append(userRows(42, 1, 3));

    const auto page = r.scan(beKey({99}));
    EXPECT_EQ(page.code, kOk);
    EXPECT_EQ(page.rowCount, 0);
    EXPECT_TRUE(page.rows.empty());
    EXPECT_TRUE(page.token.empty());
}

TEST(CheckedWritesViewReadRegression, ErasedTokenRowAnswersCursorInvalidatedRegression) {
    CwViewReadWorkerRig r;
    const auto c = accountCharacters(r.src);
    r.install(c);
    r.append(userRows(42, 1, 12));

    const auto page1 = r.scan(beKey({42}), {}, 5);
    ASSERT_EQ(page1.code, kOk);
    ASSERT_EQ(page1.rows.size(), 5u);
    ASSERT_EQ(page1.token.size(), 12u);

    r.append({Rec{5, 42, "", 0, true}});   // the token's row (the fifth) is erased
    const auto page2 = r.scan(beKey({42}), page1.token, 5);
    EXPECT_EQ(page2.code, Status::kCursorInvalidated);
    EXPECT_TRUE(page2.rows.empty());
}

TEST(CheckedWritesViewReadRegression, ScanNeverCrossesIntoAnotherPrefixRegression) {
    CwViewReadWorkerRig r;
    const auto c = accountCharacters(r.src);
    r.vs.setHashMaskForTest(0);   // every key and every prefix collides
    r.install(c);
    r.append(userRows(42, 1, 12));
    r.append(userRows(43, 100, 12));

    std::vector<uint8_t> token;
    std::size_t total = 0;
    for (int pages = 0; pages < 10; ++pages) {
        const auto page = r.scan(beKey({42}), token, 5);
        ASSERT_EQ(page.code, kOk);
        for (const auto& row : page.rows) EXPECT_EQ(rune::view::readSlot(row.data(), c.rowLayout, 0).u, 42u);
        total += page.rows.size();
        token = page.token;
        if (token.empty()) break;
    }
    EXPECT_EQ(total, 12u);
}

TEST(CheckedWritesViewReadRegression, MinOffsetHoldsTheReadUntilFoldedRegression) {
    CwViewReadWorkerRig r;
    const auto c = accountCharacters(r.src);
    r.install(c);
    r.append(userRows(42, 1, 9));   // source offsets 0..8 folded

    ASSERT_TRUE(rune::node::test::postViewRead(r.w, WorkerOp::Type::GetViewRowK, beKey({42, 10}), {}, kViewTopic, 0, /*minOffset*/ 9));
    EXPECT_TRUE(r.pass(2'000'000'000).empty());
    EXPECT_EQ(r.w.heldViewReadsForTest(), 1u) << "the fold is at 9: a read after offset 9 waits";

    r.append({Rec{10, 42, "j", 77}});   // offset 9 folds
    const auto served = r.pass(2'001'000'000);
    ASSERT_EQ(served.size(), 1u);
    ASSERT_EQ(served[0].errorCode, kOk);
    EXPECT_EQ(rune::view::readSlot(served[0].chunk.data(), c.rowLayout, 2).u, 77u);
    EXPECT_EQ(r.w.heldViewReadsForTest(), 0u);

    // Offset 50 is never written: NotReady at the 100 ms deadline, never earlier.
    ASSERT_TRUE(rune::node::test::postViewRead(r.w, WorkerOp::Type::GetViewRowK, beKey({42, 10}), {}, kViewTopic, 0, /*minOffset*/ 50));
    const int64_t t0 = 3'000'000'000;
    EXPECT_TRUE(r.pass(t0).empty());
    EXPECT_TRUE(r.pass(t0 + 50'000'000).empty());
    EXPECT_TRUE(r.pass(t0 + 99'000'000).empty());
    EXPECT_EQ(r.w.heldViewReadsForTest(), 1u);
    const auto expired = r.pass(t0 + 101'000'000);
    ASSERT_EQ(expired.size(), 1u);
    EXPECT_EQ(expired[0].errorCode, Status::kNotReady);
    EXPECT_EQ(r.w.heldViewReadsForTest(), 0u);
}

TEST(CheckedWritesViewReadRegression, GetViewRowOnAKeyedViewIsInvalidRequestRegression) {
    CwViewReadWorkerRig r;
    constexpr int32_t kLower = kViewTopic + 1, kNarrow = kViewTopic + 2, kLegacy = kViewTopic + 3;
    r.install(accountCharacters(r.src));
    r.install(compileKeyed(r.src, {{"name", 1, "nameKey"}, {"characterId", 0, "characterId"}}, {"gender"}, kLower));
    const rune::codec::Descriptor* srcs[] = {&r.src};
    rune::view::ViewPlan narrow;   // key = gender (u16, unique): a two-byte key
    narrow.viewName      = "by_gender";
    narrow.sources       = {{kSrcTopic, "characterId"}};
    narrow.keyComponents = {{"gender", 0, "gender"}};
    narrow.unique        = true;
    narrow.output        = {{"gender", PrimType::UInt16, 0, "gender", rune::view::MergeRule::Replace}};
    r.install(rune::view::compileView(narrow, srcs, kNarrow).value());
    rune::view::ViewPlan v;
    v.viewName = "by_character";
    v.key      = "characterId";
    v.sources  = {{kSrcTopic, "characterId"}};
    v.output   = {{"characterId", PrimType::UInt64, 0, "characterId", rune::view::MergeRule::Replace},
                  {"gender", PrimType::UInt16, 0, "gender", rune::view::MergeRule::Replace}};
    const auto legacy = rune::view::compileView(v, srcs, kLegacy).value();
    r.install(legacy);
    r.append({{7, 42, "a", 5}});

    EXPECT_EQ(getViewRow72(r, kViewTopic, 42).errorCode, Status::kInvalidRequest) << "account_characters";
    EXPECT_EQ(getViewRow72(r, kLower, 42).errorCode, Status::kInvalidRequest) << "a lower() view";
    EXPECT_EQ(getViewRow72(r, kNarrow, 5).errorCode, Status::kInvalidRequest) << "a u16 view";
    const WorkerReturn ok = getViewRow72(r, kLegacy, 7);
    ASSERT_EQ(ok.errorCode, kOk) << "a u64 legacy view answers as before";
    EXPECT_EQ(rune::view::readSlot(ok.chunk.data(), legacy.rowLayout, 1).u, 5u);
}

TEST(CheckedWritesViewReadRegression, WrongKeyLengthIsInvalidRequestRegression) {
    CwViewReadWorkerRig r;
    r.install(accountCharacters(r.src));
    r.append({{1, 42, "a", 10}});
    std::vector<uint8_t> key = beKey({42, 1});
    key.pop_back();
    ASSERT_EQ(key.size(), 15u);
    EXPECT_EQ(r.getK(key).errorCode, Status::kInvalidRequest);
}

TEST(CheckedWritesViewReadRegression, MaxMinOffsetIsHeldNotServedRegression) {
    CwViewReadWorkerRig r;
    r.install(accountCharacters(r.src));
    r.append({{1, 42, "a", 10}});

    ASSERT_TRUE(rune::node::test::postViewRead(r.w, WorkerOp::Type::GetViewRowK, beKey({42, 1}), {}, kViewTopic, 0, INT64_MAX));
    const int64_t t0 = 4'000'000'000;
    EXPECT_TRUE(r.pass(t0).empty()) << "minOffset + 1 must not wrap into a served read";
    EXPECT_EQ(r.w.heldViewReadsForTest(), 1u);
    EXPECT_TRUE(r.pass(t0 + 99'000'000).empty());
    const auto expired = r.pass(t0 + 101'000'000);
    ASSERT_EQ(expired.size(), 1u);
    EXPECT_EQ(expired[0].errorCode, Status::kNotReady);
}

TEST(CheckedWritesViewReadRegression, ATableNotCaughtUpAnswersNotReadyAtOnceDespiteMinOffsetRegression) {
    CwViewReadWorkerRig r;
    r.install(accountCharacters(r.src));
    appendRecords(*r.srcRep.replica, r.src, userRows(42, 1, 3));   // no pass yet: the table is not hosted
    ASSERT_TRUE(rune::node::test::postViewRead(r.w, WorkerOp::Type::GetViewRowK, beKey({42, 1}), {}, kViewTopic, 0, 1000));
    const auto answers = r.pass(r.now += 1'000'000);
    ASSERT_EQ(answers.size(), 1u);
    EXPECT_EQ(answers[0].errorCode, Status::kNotReady);
    EXPECT_EQ(r.w.heldViewReadsForTest(), 0u);
}
