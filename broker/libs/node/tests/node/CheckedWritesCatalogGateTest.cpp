// Checked writes Part 09 Task 5a (#788): a constraint-set change is asked for only once this worker's catalog has applied the
// position the logged set was computed at.  A leader behind the catalog never undoes a newer DDL change.
#include "CheckedWritesRig.h"

using namespace rune::node;
using namespace rune::node::cw;

namespace {
constexpr int32_t kRej = 0x40000601, kRej2 = 0x40000603;
constexpr uint64_t kObj = 77;
constexpr int64_t kAhead = 5;   // the catalog position of the DDL change this node has not applied yet

TestRejectRule tagRule(uint8_t tag) { TestRejectRule r; r.mode = TestRejectRule::Mode::TagEquals; r.tag = tag; return r; }
rune::storage::control_record::ChangeMember member(uint64_t object, uint8_t tag) {
    return {object, static_cast<uint8_t>(rune::view::VerdictViewKind::TestReject), tagRule(tag).fingerprint()};
}

/// A leader that applied a DDL change at kAhead logged `logged`; this node is promoted before its catalog or its installs got there.
struct LaggingLeader {
    VerdictRig r;
    LaggingLeader(std::vector<rune::storage::control_record::ChangeMember> logged, bool restartInstead) {
        r.typed();
        r.installRejectView(kRej, kObj, tagRule(9));
        r.passes(50);
        EXPECT_EQ(r.readLog().changes.size(), 1u);
        r.demote();
        r.replicate({VerdictRig::changeBatch(r.writeState()->constraintSets.latest().id + 1, std::move(logged), kAhead)});
        if (restartInstead) r.restart();
        else r.promote();
        r.passes(200);
    }
    /// Every change record in the log: the one the other leader logged, and any this node added.
    [[nodiscard]] std::size_t changes() const { return r.readLog().changes.size(); }
};
}  // namespace

TEST(CheckedWritesCatalogGateRegression, ReplaceBehindTheCatalogIsNotRevertedRegression) {
    LaggingLeader l({member(kObj, 8)}, false);
    EXPECT_EQ(l.changes(), 2u) << "the lagging leader logged a stale set over the newer replace (#788)";
    l.r.replaceRejectView(kRej, kRej2, kObj, tagRule(8));   // the catalog applies the replace: install, then mark
    l.r.catalogMark(kAhead);
    l.r.passes(100);
    EXPECT_EQ(l.changes(), 2u);
    ASSERT_EQ(l.r.produce({{1, 10, 8}, {2, 20, 9}}), 0);
    ASSERT_TRUE(l.r.driveUntilJudged());
    EXPECT_EQ(l.r.stats()->rejectionsConstraint, 1u) << "the judge activates on the replaced member";
}

TEST(CheckedWritesCatalogGateRegression, DropBehindTheCatalogIsNotRevertedRegression) {
    LaggingLeader l({}, false);
    EXPECT_EQ(l.changes(), 2u) << "the lagging leader logged a stale set over the newer drop (#788)";
    l.r.dropRejectView(kRej);
    l.r.catalogMark(kAhead);
    l.r.passes(100);
    EXPECT_EQ(l.changes(), 2u);
    ASSERT_EQ(l.r.produce({{1, 10, 9}}), 0);   // no member judges any more
    l.r.passes(100);
    ASSERT_NE(l.r.stats(), nullptr);
    EXPECT_EQ(l.r.stats()->rejectionsConstraint, 0u);
    EXPECT_EQ(l.changes(), 2u);
}

TEST(CheckedWritesCatalogGateRegression, CreateBehindTheCatalogIsNotRevertedRegression) {
    // The leader created another object's member after dropping this one's: the logged set is the new object alone.
    LaggingLeader l({member(kObj + 1, 8)}, false);
    EXPECT_EQ(l.changes(), 2u) << "the lagging leader logged a stale set over the newer create (#788)";
    l.r.dropRejectView(kRej);
    l.r.installRejectView(kRej2, kObj + 1, tagRule(8), "rej2");
    l.r.catalogMark(kAhead);
    l.r.passes(100);
    EXPECT_EQ(l.changes(), 2u);
    ASSERT_EQ(l.r.produce({{1, 10, 8}, {2, 20, 9}}), 0);
    ASSERT_TRUE(l.r.driveUntilJudged());
    EXPECT_EQ(l.r.stats()->rejectionsConstraint, 1u);
}

TEST(CheckedWritesCatalogGateRegression, RestartBeforeTheFirstMarkKeepsTheLoggedSetRegression) {
    LaggingLeader l({member(kObj, 8)}, true);   // the restart reinstalls the old plan; no mark has arrived
    ASSERT_EQ(l.r.vs->catalogThroughForTest(), -1);
    EXPECT_EQ(l.changes(), 2u) << "a restarted leader with no mark logged a stale set (#788)";
    l.r.replaceRejectView(kRej, kRej2, kObj, tagRule(8));
    l.r.catalogMark(kAhead);
    l.r.passes(100);
    EXPECT_EQ(l.changes(), 2u);
    ASSERT_EQ(l.r.produce({{1, 10, 8}, {2, 20, 9}}), 0);
    ASSERT_TRUE(l.r.driveUntilJudged());
    EXPECT_EQ(l.r.stats()->rejectionsConstraint, 1u);
}

TEST(CheckedWritesCatalogGate, MarkOpensTheGateAtTheChangesPositionAndNotBefore) {
    // The catalog applied the replace and one more create, but the mark of the replace is the last one: the leader asks for the
    // set only when the mark covers the logged change, and it stamps its own position on what it logs.
    LaggingLeader l({member(kObj, 8)}, false);
    l.r.replaceRejectView(kRej, kRej2, kObj, tagRule(8));
    l.r.installRejectView(0x40000604, kObj + 1, tagRule(7), "rej3");
    l.r.catalogMark(kAhead - 1);
    l.r.passes(100);
    EXPECT_EQ(l.changes(), 2u) << "a mark below the change's position opened the gate";
    l.r.catalogMark(kAhead + 3);
    l.r.passes(100);
    const auto changes = l.r.readLog().changes;
    ASSERT_EQ(changes.size(), 3u) << "the mark did not re-run the sync";
    EXPECT_EQ(changes.back().members.size(), 2u);
    EXPECT_EQ(changes.back().catalogThrough, kAhead + 3);
}

TEST(CheckedWritesCatalogGateRegression, UninstallDrainedBeforeItsMarkIsNotStampedBelowItsEffectsRegression) {
    // The worker drains the uninstall of event X+1 while its position is still X: the set computed without the member must not be
    // stamped X, or a leader at exactly X (gate open, template still live) would put the member back.
    VerdictRig r;
    r.typed();
    r.installRejectView(kRej, kObj, tagRule(9));
    r.passes(50);
    r.catalogMark(kAhead);
    ASSERT_EQ(r.readLog().changes.size(), 1u);
    r.uninstallFrame(kRej);
    r.passes(100);
    EXPECT_EQ(r.readLog().changes.size(), 1u) << "a set was stamped before the mark of the uninstall that shaped it";
    r.catalogMark(kAhead + 1);
    r.passes(100);
    const auto changes = r.readLog().changes;
    ASSERT_EQ(changes.size(), 2u) << "the mark did not reopen the gate";
    EXPECT_TRUE(changes.back().members.empty());
    EXPECT_EQ(changes.back().catalogThrough, kAhead + 1);
}

TEST(CheckedWritesCatalogGate, MarkNeverMovesBackAndStartsBelowEveryPosition) {
    VerdictRig r;
    EXPECT_EQ(r.vs->catalogThroughForTest(), -1);
    r.catalogMark(9);
    r.catalogMark(4);
    EXPECT_EQ(r.vs->catalogThroughForTest(), 9);
}
