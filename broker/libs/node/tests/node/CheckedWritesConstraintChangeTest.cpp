// Checked writes Part 09 Task 6: the two-phase drop and the retained plan record (R04a).  A dropped or replaced plan keeps judging
// until W passes its change record; the plan record (type 20) in the log lets a leader whose catalog no longer has the plan judge
// [K, L) all the same.  A rig node's catalog is its recorded installs, so a view dropped or replaced is not installed again by a
// restart or a failover (what a node whose catalog dropped the plan models: failover()/restart() reinstall only live views).
#include "CheckedWritesRig.h"

#include <rune/codec/Serialize.h>
#include <rune/ql/Backends.h>
#include <rune/view/Verdict.h>

#include <algorithm>
#include <functional>
#include <map>
#include <memory>
#include <set>
#include <tuple>

using namespace rune::node;
using namespace rune::node::cw;
namespace cr = rune::storage::control_record;
namespace rv = rune::view;

namespace {
using CV = ConstrainedView;
using Char = VerdictRig::Char;
constexpr uint8_t kLeader = 1;
constexpr int kCap = 4000;
const uint64_t kNameObj = VerdictRig::objectIdOf(CV::CharacterName);
const uint64_t kStatsObj = VerdictRig::objectIdOf(CV::AccountStats);

/// A rig with a follower (the HW moves only on followerFetch) over the character topic, explicit keys.
struct Cw {
    VerdictRig r{true};
    explicit Cw(int64_t intervalNs = 1'000'000) {
        r.worker->setVerdictIntervalNs(intervalNs);
        r.createCharacterTopic(false);
        r.enableAcks();
        r.passes(20);
    }
    bool passesUntil(const std::function<bool()>& done) {
        for (int i = 0; i < kCap; ++i) {
            if (done()) return true;
            r.pass();
        }
        return done();
    }
    void append(std::vector<Char> chars) {
        (void)r.appendCharacters(chars, kLeader);
        r.passes(3);
    }
    [[nodiscard]] static bool names(const VerdictRig::RigChange& c, uint64_t obj) {
        return std::any_of(c.members.begin(), c.members.end(), [&](const cr::ChangeMember& m) { return m.viewObjectId == obj; });
    }
    /// The offset of the latest change that no longer names `obj` after one that did (-1 = none).
    [[nodiscard]] int64_t dropOf(uint64_t obj) const {
        const auto l = r.readLog();
        bool seen = false;
        int64_t d = -1;
        for (const auto& c : l.changes) {
            if (names(c, obj)) { seen = true; d = -1; }
            else if (seen && d < 0) d = c.offset;
        }
        return d;
    }
    [[nodiscard]] int64_t joinOf(uint64_t obj) const {
        for (const auto& c : r.readLog().changes) if (names(c, obj)) return c.offset;
        return -1;
    }
    void create(CV v, int64_t cap = 12) {
        (void)r.createConstrainedView(v, cap);
        ASSERT_TRUE(passesUntil([&] { return joinOf(VerdictRig::objectIdOf(v)) >= 0; })) << "the change naming the view was never logged";
    }
    void drop(CV v) {
        r.dropConstrainedView(v);
        ASSERT_TRUE(passesUntil([&] { return dropOf(VerdictRig::objectIdOf(v)) >= 0; })) << "the drop was never logged";
    }
    [[nodiscard]] const VerdictSource* judge() const { return r.vs->verdictSource(VerdictRig::kSrc, r.partition()); }
    [[nodiscard]] int64_t committed() const { return r.writeState()->markers.committedThrough(); }
};

Char ch(uint64_t id, uint64_t user, const std::string& name) { return Char{.id = id, .userId = user, .name = name}; }
const std::vector<Char> kFirst{ch(1, 1, "a"), ch(2, 1, "b"), ch(3, 1, "c"), ch(4, 2, "A")};   // 4 repeats 1's name
const std::vector<Char> kSecond{ch(5, 2, "e"), ch(6, 2, "f"), ch(7, 3, "E"), ch(8, 3, "a")};   // 7 and 8 repeat names
const std::vector<Char> kTail{ch(9, 4, "z")};

/// Each rejected record of the log's markers as (ordinal of the data record, code, constraintId): independent of where markers fell.
std::vector<std::tuple<std::size_t, uint16_t, uint32_t>> verdictsOf(const VerdictRig& r) {
    const auto l = r.readLog();
    std::map<int64_t, std::size_t> ordinal;
    for (std::size_t i = 0; i < l.data.size(); ++i) ordinal[l.data[i].offset] = i;
    std::set<std::tuple<std::size_t, uint16_t, uint32_t>> out;
    for (const auto& m : l.markers)
        for (const auto& [off, code, cid] : m.entries) out.emplace(ordinal.at(off), code, cid);
    return {out.begin(), out.end()};
}

/// The offsets a raw consumer from 0 receives, once the HW and the judge allow everything.
std::vector<int64_t> deliveredOffsets(VerdictRig& r) {
    const int64_t sub = r.subscribe(SubKind::Raw, 0);
    std::vector<int64_t> got;
    for (const auto& d : r.collect(sub).records) got.push_back(d.offset);
    std::sort(got.begin(), got.end());
    return got;
}

/// The data records of the log the verdicts accept.
std::vector<int64_t> acceptedOffsets(const VerdictRig& r) {
    const auto l = r.readLog();
    std::set<int64_t> rejected;
    for (const auto& m : l.markers) for (const auto& e : m.entries) rejected.insert(std::get<0>(e));
    std::vector<int64_t> out;
    for (const auto& d : l.data) if (!rejected.count(d.offset)) out.push_back(d.offset);
    return out;
}

/// The state of 'a drop is in flight': records judged by character_name up to the drop D, D committed, and the marker covering them
/// above the HW (W < D).  `hold` keeps the judge from judging the second batch before the follower fetched D.
void dropInFlight(Cw& c, int32_t* nameTopic = nullptr) {
    c.create(CV::CharacterName);
    if (nameTopic != nullptr) *nameTopic = c.r.constrained.at(CV::CharacterName).topicId;
    c.append(kFirst);
    ASSERT_TRUE(c.r.driveUntilJudged());
    c.r.holdJudge(true);
    c.append(kSecond);
    c.drop(CV::CharacterName);
    c.append(kTail);
    c.r.followerFetch();
    c.r.holdJudge(false);
    const int64_t d = c.dropOf(kNameObj);
    ASSERT_TRUE(c.passesUntil([&] { return c.judge()->judgedThrough() > d + 1; })) << "the judge never passed the drop";
    ASSERT_LE(c.committed(), d) << "W must be below the drop";
}
}  // namespace

TEST(CheckedWritesConstraintChange, CreateAppendsThePlanBeforeTheChange) {
    Cw c;
    c.create(CV::CharacterName);
    auto l = c.r.readLog();
    ASSERT_EQ(l.plans.size(), 1u);
    const int64_t nameChange = c.joinOf(kNameObj);
    EXPECT_EQ(l.plans[0].offset, nameChange - 1) << "the plan record sits right below the change";
    EXPECT_EQ(l.plans[0].header.viewObjectId, kNameObj);
    EXPECT_EQ(l.plans[0].header.planFingerprint, c.r.fingerprintOf(CV::CharacterName, false));
    EXPECT_EQ(l.plans[0].header.kind, static_cast<uint8_t>(rv::VerdictViewKind::Unique));
    const auto& live = c.r.constrained.at(CV::CharacterName);
    EXPECT_EQ(l.plans[0].plan, rv::serialize(live.comp.plan));
    EXPECT_EQ(l.plans[0].outputDescriptor, rune::codec::serialize(live.comp.outputDescriptor));
    EXPECT_EQ(l.plans[0].sourceDescriptor, rune::codec::serialize(rune::ql::toDescriptor(VerdictRig::parseTopic(VerdictRig::kCharacterTopic))));

    c.create(CV::AccountStats);
    l = c.r.readLog();
    ASSERT_EQ(l.plans.size(), 2u) << "one more for the joining check member";
    EXPECT_EQ(l.plans[1].offset, c.joinOf(kStatsObj) - 1);
    EXPECT_EQ(l.plans[1].header.kind, static_cast<uint8_t>(rv::VerdictViewKind::Check));
    EXPECT_EQ(l.plans[1].plan, rv::serialize(c.r.constrained.at(CV::AccountStats).comp.plan));
    for (const auto& p : l.plans) EXPECT_NE(p.header.kind, static_cast<uint8_t>(rv::VerdictViewKind::KeyIndex)) << "the key index member has none";
    ASSERT_GE(l.changes.size(), 3u);
    EXPECT_FALSE(std::any_of(l.plans.begin(), l.plans.end(), [&](const auto& p) { return p.offset == l.changes.front().offset - 1; })) << "the key index change has no plan record";
}

// R04a phase 2: the dropped plan stays installed, and judging, until W passes the drop.
TEST(CheckedWritesConstraintChange, RetiringTemplateStaysUntilWPassesTheDrop) {
    Cw c;
    int32_t topic = 0;
    dropInFlight(c, &topic);
    const int64_t d = c.dropOf(kNameObj);
    EXPECT_EQ(c.r.vs->retiringVerdictTemplatesForTest(), 1u);
    EXPECT_NE(c.r.vs->instanceForTest(topic, c.r.partition()), nullptr) << "its instance is alive";
    c.r.followerFetch();
    ASSERT_TRUE(c.r.driveUntilJudged());
    EXPECT_GT(c.committed(), d);
    c.r.passes(10);
    EXPECT_EQ(c.r.vs->retiringVerdictTemplatesForTest(), 0u);
    EXPECT_EQ(c.r.vs->instanceForTest(topic, c.r.partition()), nullptr);
}

// R04a, Review Focus 2: the node that leads next has no character_name in its catalog.  It judges [K, D) with the plan from its log,
// finishes the drop and nothing waits forever.
TEST(CheckedWritesConstraintChangeRegression, FailoverDuringDropFinishesTheDropRegression) {
    std::vector<std::tuple<std::size_t, uint16_t, uint32_t>> expected;
    {
        Cw undisturbed;
        dropInFlight(undisturbed);
        undisturbed.r.followerFetch();
        ASSERT_TRUE(undisturbed.r.driveUntilJudged(undisturbed.r.dataEnd()));
        expected = verdictsOf(undisturbed.r);
    }
    Cw c;
    dropInFlight(c);
    c.r.failover();
    const int64_t end = c.r.dataEnd();
    ASSERT_TRUE(c.r.driveUntilJudgeReaches(end));
    EXPECT_EQ(c.r.stats()->retainedPlanInstalls, 1u);
    ASSERT_TRUE(c.r.driveUntilJudged(end)) << "the judge finishes";
    EXPECT_FALSE(c.judge()->waitingForInstall());
    EXPECT_EQ(verdictsOf(c.r), expected) << "the retained plan judges as the installed one did";
    EXPECT_FALSE(verdictsOf(c.r).empty());
    c.r.followerFetch();
    c.r.passes(10);
    EXPECT_GT(c.committed(), c.dropOf(kNameObj));
    EXPECT_EQ(c.r.vs->retiringVerdictTemplatesForTest(), 0u) << "W passed the drop: the template is gone";
    EXPECT_FALSE(c.judge()->waitingForInstall());
    const auto got = deliveredOffsets(c.r);
    EXPECT_EQ(got, acceptedOffsets(c.r)) << "a consumer gets every accepted record and no rejected one";
    EXPECT_EQ(got.size(), 9u - verdictsOf(c.r).size());
}

// The drop record D was appended above the HW and cut by the failover: the new leader judges with the retained plan, finds the desired
// set without it and logs its own drop.
TEST(CheckedWritesConstraintChangeRegression, FailoverAfterTheDropRecordWasCutRegression) {
    Cw c;
    c.create(CV::CharacterName);
    c.append(kFirst);
    ASSERT_TRUE(c.r.driveUntilJudged());
    c.append(kSecond);
    c.drop(CV::CharacterName);
    const int64_t cut = c.dropOf(kNameObj);
    ASSERT_GE(cut, c.r.replica->highWatermark()) << "the drop is above the HW";
    c.r.failover();
    ASSERT_TRUE(c.passesUntil([&] { return c.dropOf(kNameObj) >= 0; })) << "the new leader never logged its own drop";
    EXPECT_EQ(c.r.stats()->retainedPlanInstalls, 1u);
    const int64_t d2 = c.dropOf(kNameObj);
    c.append(kTail);
    ASSERT_TRUE(c.r.driveUntilJudged(c.r.dataEnd()));
    EXPECT_GT(c.committed(), d2);
    c.r.passes(10);
    EXPECT_EQ(c.r.vs->retiringVerdictTemplatesForTest(), 0u) << "uninstalled once W passed D'";
    EXPECT_FALSE(c.judge()->waitingForInstall());
}

// A replace whose swap S committed while W < S: the restarted node installs only the new plan; the old one comes from the log.
TEST(CheckedWritesConstraintChangeRegression, RestartDuringReplaceUsesTheRetainedOldPlanRegression) {
    using Verdicts = std::vector<std::tuple<std::size_t, uint16_t, uint32_t>>;
    const auto run = [](bool disturbed) -> Verdicts {
        auto c = std::make_unique<Cw>();
        c->create(CV::AccountStats, 12);
        const std::vector<Char> userSix{ch(1, 6, "a"), ch(2, 6, "b"), ch(3, 6, "c"), ch(4, 6, "d")};
        const std::vector<Char> userSeven{ch(5, 7, "e"), ch(6, 7, "f"), ch(7, 7, "g"), ch(8, 7, "h")};
        c->append({ch(9, 5, "x")});
        EXPECT_TRUE(c->r.driveUntilJudged());
        c->r.worker->setVerdictIntervalNs(1'000'000'000'000);   // markers only for rejections: the first batch stays unmarked
        c->append(userSix);
        c->r.replaceConstrainedView(CV::AccountStats, 2);
        const auto swapped = [&] {
            const auto l = c->r.readLog();
            return !l.changes.empty() && l.changes.back().members.size() >= 2 &&
                   std::any_of(l.changes.back().members.begin(), l.changes.back().members.end(), [&](const cr::ChangeMember& m) { return m.viewObjectId == kStatsObj && m.planFingerprint == c->r.fingerprintOf(CV::AccountStats, false); });
        };
        EXPECT_TRUE(c->passesUntil(swapped)) << "the swap was never logged";
        c->append(userSeven);
        c->r.followerFetch(c->r.dataEnd());
        EXPECT_TRUE(c->passesUntil([&] { return c->judge()->judgedThrough() >= c->r.dataEnd(); }));
        if (disturbed) c->r.failover();
        const int64_t end = c->r.dataEnd();
        EXPECT_TRUE(c->r.driveUntilJudged(end));
        EXPECT_FALSE(c->judge()->waitingForInstall());
        if (disturbed) EXPECT_EQ(c->r.stats()->retainedPlanInstalls, 1u);
        c->r.followerFetch();
        c->r.passes(10);
        EXPECT_EQ(c->r.vs->retiringVerdictTemplatesForTest(), 0u) << "the old plan is gone once W passed the swap";
        return verdictsOf(c->r);
    };
    const Verdicts before = run(false);
    const Verdicts after = run(true);
    EXPECT_EQ(after, before);
    EXPECT_FALSE(after.empty()) << "the new cap rejects the second batch";
}

enum class Timing { DNotCommitted, DCommittedJudgeBelow, JudgePastDWBelow, WPastD };
enum class Mode { Failover, Restart };

class CheckedWritesConstraintChangeDropTiming : public ::testing::TestWithParam<std::tuple<Timing, Mode>> {};

TEST_P(CheckedWritesConstraintChangeDropTiming, NothingWaitsForeverAfterAnyDropTimingRegression) {
    const auto [timing, mode] = GetParam();
    Cw c;
    c.create(CV::CharacterName);
    c.append(kFirst);
    ASSERT_TRUE(c.r.driveUntilJudged());
    const bool held = timing == Timing::DCommittedJudgeBelow || timing == Timing::JudgePastDWBelow;
    if (held) c.r.holdJudge(true);
    c.append(kSecond);
    c.drop(CV::CharacterName);
    c.append(kTail);
    switch (timing) {
    case Timing::DNotCommitted:
        c.r.passes(30);
        break;
    case Timing::DCommittedJudgeBelow:
        c.r.followerFetch();
        c.r.holdJudge(false);
        break;
    case Timing::JudgePastDWBelow:
        c.r.followerFetch();
        c.r.holdJudge(false);
        ASSERT_TRUE(c.passesUntil([&] { return c.judge()->judgedThrough() > c.dropOf(kNameObj) + 1; }));
        break;
    case Timing::WPastD:
        ASSERT_TRUE(c.r.driveUntilJudged());
        c.r.passes(10);
        EXPECT_EQ(c.r.vs->retiringVerdictTemplatesForTest(), 0u);
        break;
    }
    if (mode == Mode::Failover) c.r.failover();
    else c.r.restart();
    ASSERT_TRUE(c.passesUntil([&] { return c.dropOf(kNameObj) >= 0; })) << "the drop is never in the log";
    c.append({ch(10, 5, "y")});   // a record above the drop: the next marker covers it, W passes it
    const int64_t end = c.r.dataEnd();
    ASSERT_TRUE(c.r.driveUntilJudged(end));
    c.r.followerFetch();
    c.r.passes(10);
    EXPECT_FALSE(c.judge()->waitingForInstall());
    EXPECT_GT(c.committed(), c.dropOf(kNameObj));
    EXPECT_EQ(c.r.vs->retiringVerdictTemplatesForTest(), 0u);
    EXPECT_EQ(c.r.writeState()->rejected.mismatches(), 0u);
}

INSTANTIATE_TEST_SUITE_P(Timings, CheckedWritesConstraintChangeDropTiming,
                         ::testing::Combine(::testing::Values(Timing::DNotCommitted, Timing::DCommittedJudgeBelow, Timing::JudgePastDWBelow, Timing::WPastD),
                                            ::testing::Values(Mode::Failover, Mode::Restart)));

// The scan feeds the plan records; the activation drops the ones whose member left below W.  The log keeps them.
TEST(CheckedWritesConstraintChange, RetainedPlansArePrunedAfterW) {
    Cw c;
    c.create(CV::CharacterName);
    c.create(CV::AccountStats);
    const uint64_t nameFingerprint = c.r.fingerprintOf(CV::CharacterName, false);
    c.append(kFirst);
    c.drop(CV::CharacterName);
    c.append(kTail);
    ASSERT_TRUE(c.r.driveUntilJudged(c.r.dataEnd()));
    c.r.passes(10);
    ASSERT_GT(c.committed(), c.dropOf(kNameObj));
    c.r.demotePromote();
    c.r.passes(50);
    const PartitionWriteState* w = c.r.writeState();
    EXPECT_EQ(w->retainedPlans.size(), 1u) << "only the member still logged";
    EXPECT_EQ(w->retainedPlans.find(kNameObj, nameFingerprint), nullptr);
    EXPECT_NE(w->retainedPlans.find(kStatsObj, c.r.fingerprintOf(CV::AccountStats, false)), nullptr);
    EXPECT_EQ(c.r.readLog().plans.size(), 2u) << "the log still holds the record";
}

// P9-8: a replacement plan joins at the swap, and its record is in the log for a later drop or failover.
TEST(CheckedWritesConstraintChange, ReplaceAppendsTheNewPlanBeforeTheSwap) {
    Cw c;
    c.create(CV::AccountStats, 12);
    c.r.replaceConstrainedView(CV::AccountStats, 2);
    const uint64_t fp2 = c.r.fingerprintOf(CV::AccountStats, false);
    const auto swapped = [&] {
        const auto l = c.r.readLog();
        return std::any_of(l.plans.begin(), l.plans.end(), [&](const auto& p) { return p.header.planFingerprint == fp2; });
    };
    ASSERT_TRUE(c.passesUntil(swapped)) << "no plan record for the replacement";
    const auto l = c.r.readLog();
    const auto plan = std::find_if(l.plans.begin(), l.plans.end(), [&](const auto& p) { return p.header.planFingerprint == fp2; });
    const auto change = std::find_if(l.changes.begin(), l.changes.end(), [&](const auto& ch) { return ch.offset == plan->offset + 1; });
    ASSERT_NE(change, l.changes.end()) << "the record sits right below the swap";
    EXPECT_EQ(plan->plan, rv::serialize(c.r.constrained.at(CV::AccountStats).comp.plan));
}

// A view that was replaced and then dropped: the node that leads next has neither plan in its catalog.
TEST(CheckedWritesConstraintChangeRegression, FailoverDuringDropAfterReplaceRegression) {
    Cw c;
    c.create(CV::AccountStats, 12);
    c.append({ch(1, 6, "a"), ch(2, 6, "b")});
    ASSERT_TRUE(c.r.driveUntilJudged());
    c.r.replaceConstrainedView(CV::AccountStats, 2);
    const uint64_t fp2 = c.r.fingerprintOf(CV::AccountStats, false);
    ASSERT_TRUE(c.passesUntil([&] {
        const auto l = c.r.readLog();
        return std::any_of(l.plans.begin(), l.plans.end(), [&](const auto& p) { return p.header.planFingerprint == fp2; });
    }));
    c.r.holdJudge(true);
    c.append({ch(3, 6, "c")});
    c.drop(CV::AccountStats);
    c.append(kTail);
    c.r.followerFetch();
    c.r.holdJudge(false);
    c.r.failover();
    const int64_t end = c.r.dataEnd();
    ASSERT_TRUE(c.r.driveUntilJudged(end)) << "the judge waits for a plan only the log has";
    EXPECT_GE(c.r.stats()->retainedPlanInstalls, 1u);
    EXPECT_FALSE(c.judge()->waitingForInstall());
}

// A plan this node refused to install is never installed from the log instead: the judge waits and no change drops the member.
TEST(CheckedWritesConstraintChangeRegression, RefusedInstallIsNeverOverruledByTheLogRegression) {
    Cw c;
    c.create(CV::CharacterName);
    c.append(kFirst);
    ASSERT_TRUE(c.r.driveUntilJudged());
    const std::size_t changes = c.r.readLog().changes.size();
    c.r.refuseInstalls = true;
    c.r.failover();
    c.r.passes(200);
    EXPECT_EQ(c.r.stats()->retainedPlanInstalls, 0u);
    EXPECT_TRUE(c.judge()->waitingForInstall());
    EXPECT_EQ(c.r.readLog().changes.size(), changes) << "no change may drop a live constraint this node cannot judge with";
    EXPECT_EQ(c.dropOf(kNameObj), -1);
}

// P9-6: a judge that goes away takes the retired views it held with it.
TEST(CheckedWritesConstraintChangeRegression, DroppingTheJudgeErasesItsRetainedTemplatesRegression) {
    Cw c;
    dropInFlight(c);
    c.r.failover();
    ASSERT_TRUE(c.r.driveUntilJudgeReaches(c.r.dataEnd()));
    ASSERT_EQ(c.r.vs->retiringVerdictTemplatesForTest(), 1u);
    c.r.vs->dropVerdictSource(VerdictRig::kSrc, c.r.partition());
    c.r.passes(3);
    EXPECT_EQ(c.r.vs->retiringVerdictTemplatesForTest(), 0u);
}

// A catalog that lags the logged set keeps the judge waiting for its own install: the log's plan does not stand in for it (#788).
TEST(CheckedWritesConstraintChangeRegression, LaggingCatalogDoesNotInstallFromTheLogRegression) {
    constexpr int64_t kAhead = 1000;
    Cw c;
    c.r.vs->raiseCatalogThroughForTest(kAhead);
    c.create(CV::CharacterName);
    c.append(kFirst);
    ASSERT_TRUE(c.r.driveUntilJudged());
    c.r.failover(/*reinstall=*/false);
    c.r.passes(100);
    EXPECT_EQ(c.r.stats()->retainedPlanInstalls, 0u);
    EXPECT_TRUE(c.judge()->waitingForInstall());
    c.r.reinstallAll();
    c.r.catalogMark(kAhead);
    ASSERT_TRUE(c.r.driveUntilJudged(c.r.dataEnd()));
    EXPECT_EQ(c.r.vs->retiringVerdictTemplatesForTest(), 0u) << "the catalog's template judges, not a retiring one from the log";
    EXPECT_FALSE(c.judge()->waitingForInstall());
}

// A replacement this node refuses leaves the old plan retiring and no new template: the logged member stays.
TEST(CheckedWritesConstraintChangeRegression, RefusedReplacementKeepsTheLoggedMemberRegression) {
    Cw c;
    c.create(CV::AccountStats, 12);
    c.append({ch(1, 6, "a")});
    ASSERT_TRUE(c.r.driveUntilJudged());
    const std::size_t changes = c.r.readLog().changes.size();
    c.r.refuseInstalls = true;
    c.r.replaceConstrainedView(CV::AccountStats, 2);
    c.r.passes(200);
    EXPECT_EQ(c.r.readLog().changes.size(), changes) << "no change may drop a constraint this node cannot replace";
    EXPECT_EQ(c.dropOf(kStatsObj), -1);
}

// #940: a later good install of the object ends the keep its earlier refusal gave: fp2 refused, fp3 installs, the drop is logged.
TEST(CheckedWritesConstraintChangeRegression, AGoodInstallClearsTheObjectsEarlierRefusalRegression) {
    Cw c;
    c.create(CV::AccountStats, 12);
    c.append({ch(1, 6, "a")});
    ASSERT_TRUE(c.r.driveUntilJudged());
    c.r.refuseInstalls = true;
    c.r.replaceConstrainedView(CV::AccountStats, 2);
    c.r.passes(200);
    ASSERT_EQ(c.dropOf(kStatsObj), -1) << "the refusal keeps the member";
    c.r.refuseInstalls = false;
    c.r.replaceConstrainedView(CV::AccountStats, 3);
    const uint64_t fp3 = c.r.fingerprintOf(CV::AccountStats, false);
    ASSERT_TRUE(c.passesUntil([&] {
        c.r.followerFetch();
        const auto changes = c.r.readLog().changes;
        return std::any_of(changes.back().members.begin(), changes.back().members.end(), [&](const cr::ChangeMember& m) { return m.viewObjectId == kStatsObj && m.planFingerprint == fp3; });
    })) << "the replacement never swapped in";
    c.drop(CV::AccountStats);
    c.r.passes(200);
    EXPECT_NE(c.dropOf(kStatsObj), -1) << "the drop must be logged: no stale refusal keeps the dropped member";
}

// A restarted node installs its catalog's views for recovery, and the recovery pass materialized the instance of one no judge had attached
// yet: re-registering the template wiped its verdict identity, the object had no plan to swap to, and the member left the set.  In
// production that is a restart while a replacement candidate still builds (see RestartWhileACandidateBuilds...); this test reaches the same
// path only because the rig keeps sending recovery installs after restart() (#954).
TEST(CheckedWritesConstraintChangeRegression, ReplaceAfterRestartSwapsAndKeepsJudgingRegression) {
    constexpr uint8_t kAllView = rune::protocol::client::kAcksWireView | rune::protocol::client::kAcksWireAll;
    Cw c;
    c.create(CV::AccountStats, 3);
    int32_t seq = 0;
    const auto insert = [&](uint64_t id, const char* name) {
        const std::vector<Char> one{Char{.id = id, .userId = 1, .name = name, .intent = 1}};
        return c.r.ackFetching(c.r.appendCharacters(one, kAllView, 41, seq++), 6000);
    };
    for (const auto& [id, name] : {std::pair<uint64_t, const char*>{1, "a"}, {2, "b"}, {3, "c"}}) {
        const auto a = insert(id, name);
        ASSERT_TRUE(a && a->code == 0 && a->verdicts.empty());
    }
    c.r.restart();
    ASSERT_TRUE(c.r.driveUntilJudged());
    c.r.replaceConstrainedView(CV::AccountStats, 4);
    const uint64_t fp4 = c.r.fingerprintOf(CV::AccountStats, false);
    ASSERT_TRUE(c.passesUntil([&] {
        c.r.followerFetch();
        const auto changes = c.r.readLog().changes;
        return std::any_of(changes.back().members.begin(), changes.back().members.end(), [&](const cr::ChangeMember& m) { return m.viewObjectId == kStatsObj && m.planFingerprint == fp4; });
    })) << "the replacement never swapped in";
    EXPECT_EQ(c.r.stats()->candidateSwaps, 1u);
    const auto fourth = insert(4, "d");
    ASSERT_TRUE(fourth);
    EXPECT_TRUE(fourth->verdicts.empty()) << "the new cap allows a fourth";
    const auto fifth = insert(5, "e");
    ASSERT_TRUE(fifth);
    EXPECT_EQ(fifth->verdicts, (std::vector<std::tuple<uint16_t, uint16_t, uint32_t>>{{0, 55, static_cast<uint32_t>(kStatsObj << 16) | 1}})) << "the check still judges after the restart";
}

// The production trigger: CREATE OR REPLACE applied live (no recovery install), then a restart before the swap is logged.  The boot
// installs only the new plan, for recovery; its candidate must stay the object's replacement through the recovery pass and swap in.
TEST(CheckedWritesConstraintChangeRegression, RestartWhileACandidateBuildsKeepsTheReplacementRegression) {
    constexpr uint8_t kAllView = rune::protocol::client::kAcksWireView | rune::protocol::client::kAcksWireAll;
    Cw c;
    c.create(CV::AccountStats, 3);
    int32_t seq = 0;
    const auto insert = [&](uint64_t id, const char* name) {
        const std::vector<Char> one{Char{.id = id, .userId = 1, .name = name, .intent = 1}};
        return c.r.ackFetching(c.r.appendCharacters(one, kAllView, 41, seq++), 6000);
    };
    for (const auto& [id, name] : {std::pair<uint64_t, const char*>{1, "a"}, {2, "b"}, {3, "c"}}) {
        const auto a = insert(id, name);
        ASSERT_TRUE(a && a->code == 0 && a->verdicts.empty());
    }
    c.r.holdJudge(true);
    c.r.replaceConstrainedView(CV::AccountStats, 4);
    const uint64_t fp4 = c.r.fingerprintOf(CV::AccountStats, false);
    const auto swapped = [&] {
        const auto changes = c.r.readLog().changes;
        return std::any_of(changes.back().members.begin(), changes.back().members.end(), [&](const cr::ChangeMember& m) { return m.viewObjectId == kStatsObj && m.planFingerprint == fp4; });
    };
    c.r.passes(20);
    ASSERT_FALSE(swapped()) << "the held judge must not swap before the restart";
    c.r.restart();
    c.r.holdJudge(false);
    ASSERT_TRUE(c.passesUntil([&] {
        c.r.followerFetch();
        return swapped();
    })) << "the candidate lost its identity in the recovery pass and never swapped in";
    EXPECT_EQ(c.dropOf(kStatsObj), -1) << "the check left the set";
    const auto fourth = insert(4, "d");
    ASSERT_TRUE(fourth);
    EXPECT_TRUE(fourth->verdicts.empty()) << "the new cap allows a fourth";
    const auto fifth = insert(5, "e");
    ASSERT_TRUE(fifth);
    EXPECT_EQ(fifth->verdicts, (std::vector<std::tuple<uint16_t, uint16_t, uint32_t>>{{0, 55, static_cast<uint32_t>(kStatsObj << 16) | 1}})) << "the new plan judges after the restart";
}
