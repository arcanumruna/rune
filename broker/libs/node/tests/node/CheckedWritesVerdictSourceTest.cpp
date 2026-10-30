// Part 04 Task 5: the judge of one source partition (unit level: no worker, no replica).
#include <gtest/gtest.h>

#include <memory>
#include <vector>

#include <rune/codec/Descriptor.h>
#include <rune/node/VerdictSource.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/view/PlanCompiler.h>

using namespace rune::node;
namespace cr = rune::storage::control_record;
using P = rune::codec::PrimType;
using rune::view::VerdictViewKind;

namespace {

constexpr uint16_t kViolation = 55;

rune::codec::Descriptor tagSource() {
    return rune::codec::DescriptorBuilder("rec", 1).add("id", P::Int64, 1, true).add("tag", P::Int32).build();
}
rune::view::ViewPlan tagView() {
    rune::view::ViewPlan v;
    v.viewName = "rec_tag";
    v.key      = "id";
    v.sources  = {{5, "id"}};
    v.output   = {{"id", P::Int64, 0, "id", rune::view::MergeRule::Replace}, {"tag", P::Int32, 0, "tag", rune::view::MergeRule::Replace}};
    return v;
}

void le16(std::vector<uint8_t>& b, uint16_t v) { b.push_back(uint8_t(v)); b.push_back(uint8_t(v >> 8)); }
void le32(std::vector<uint8_t>& b, uint32_t v) { for (int i = 0; i < 4; ++i) b.push_back(uint8_t(v >> (8 * i))); }
void le64(std::vector<uint8_t>& b, uint64_t v) { for (int i = 0; i < 8; ++i) b.push_back(uint8_t(v >> (8 * i))); }
std::string_view sv(const std::vector<uint8_t>& b) { return {reinterpret_cast<const char*>(b.data()), b.size()}; }

struct Rec { int64_t id; int32_t tag; bool tombstone = false; };

// A committed log of whole batches; `read(from)` starts at the batch holding `from`.
struct Log {
    std::vector<uint8_t> bytes;
    struct Span { std::size_t pos; int64_t base, last; };
    std::vector<Span>    batches;
    int64_t              next = 0;

    void data(const std::vector<Rec>& recs) {
        rune::protocol::RecordBatchBuilder b(next, 0);
        std::vector<std::vector<uint8_t>> keep;
        for (const Rec& r : recs) {
            std::vector<uint8_t> k, v;
            le64(k, uint64_t(r.id));
            if (!r.tombstone) { le16(v, 12); le16(v, 1); le64(v, uint64_t(r.id)); le32(v, uint32_t(r.tag)); }
            b.append(sv(k), sv(v), 0);
        }
        push(b.build(), int64_t(recs.size()));
    }
    void control() {   // one control batch (an empty verdict marker) at the next offset
        std::vector<uint8_t> c(cr::markerBatchBytes(0));
        (void)cr::buildVerdictMarker(c, cr::MarkerHeader{0, 0, 0, 0}, {});
        for (int i = 0; i < 8; ++i) c[std::size_t(i)] = uint8_t(uint64_t(next) >> (8 * i));   // baseOffset
        push(std::move(c), 1);
    }
    void push(std::vector<uint8_t> batch, int64_t count) {
        batches.push_back({bytes.size(), next, next + count - 1});
        bytes.insert(bytes.end(), batch.begin(), batch.end());
        next += count;
    }
    rune::storage::LogReadView read(int64_t from) const {
        for (const Span& s : batches)
            if (s.last >= from) return {bytes.data() + s.pos, bytes.size() - s.pos, s.base, next};
        return {};
    }
};

ConstraintSet setOf(uint32_t id, std::vector<std::pair<uint64_t, uint64_t>> members) {   // {objectId, fingerprint}
    ConstraintSet s;
    s.id = id;
    for (const auto& [o, fp] : members) s.members[s.count++] = {o, VerdictViewKind::TestReject, fp};
    return s;
}

TestRejectRule tagEquals(uint8_t tag) { TestRejectRule r; r.mode = TestRejectRule::Mode::TagEquals; r.tag = tag; return r; }
TestRejectRule capLive(uint32_t cap) { TestRejectRule r; r.mode = TestRejectRule::Mode::CapLiveKeys; r.cap = cap; return r; }

struct Member {
    rune::codec::Descriptor src = tagSource();
    rune::view::CompileResult comp;
    std::unique_ptr<ViewInstance> inst;
    TestRejectRule rule;
    uint64_t objectId;
    Member(uint64_t id, TestRejectRule r) : rule(r), objectId(id) {
        const rune::codec::Descriptor* s[] = {&src};
        comp = rune::view::compileView(tagView(), s, uint32_t(id)).value();
        inst = std::make_unique<ViewInstance>(comp.plan, comp.rowLayout, src, false, 0, 256, 1u << 16);
    }
    [[nodiscard]] std::pair<uint64_t, uint64_t> ident() const { return {objectId, rule.fingerprint()}; }
    [[nodiscard]] bool has(int64_t key) const { return inst->table.get(uint64_t(key)) != nullptr; }
};

struct Rig {
    Log log;
    PartitionWriteState st;
    VerdictSource vs{5, 0};
    std::vector<std::unique_ptr<Member>> members;

    Member& install(uint64_t id, TestRejectRule r) {
        members.push_back(std::make_unique<Member>(id, r));
        vs.installTestVerdictViewForTest(members.back()->inst.get(), id, r);
        return *members.back();
    }
    void start() {
        vs.bindState(&st);
        vs.bindLeader(nullptr, 1, 0);
    }
    int drive(int64_t now = 0, int64_t bound = INT64_MAX) {
        return vs.driveJudge([&](int64_t from, int64_t) { return log.read(from); }, std::min(bound, log.next), now, st);
    }
};

}  // namespace

TEST(CheckedWritesVerdictSource, TagEqualsRejectsAndRecordsTheVerdict) {
    Rig r;
    auto& m = r.install(7, tagEquals(9));
    r.st.constraintSets.onChange(0, setOf(1, {m.ident()}));
    r.log.control();
    r.log.data({{1, 1}, {2, 9}, {3, 1}});
    r.start();
    r.drive();
    EXPECT_EQ(r.vs.judgedThrough(), 4);
    EXPECT_TRUE(m.has(1));
    EXPECT_FALSE(m.has(2));
    EXPECT_TRUE(m.has(3));
    EXPECT_EQ(m.inst->table.size(), 2u);
    ASSERT_EQ(r.st.rejected.size(), 1u);
    const auto* e = r.st.rejected.find(2);
    ASSERT_NE(e, nullptr);
    EXPECT_EQ(e->code, kViolation);
    EXPECT_EQ(e->constraintId, 0x00070001u);
    EXPECT_EQ(r.vs.stats().rejectionsConstraint, 1u);
}

TEST(CheckedWritesVerdictSource, OneOfTwoRejectsNoViewChanges) {
    Rig r;
    auto& a = r.install(7, tagEquals(9));
    auto& b = r.install(8, capLive(1));
    r.st.constraintSets.onChange(0, setOf(1, {a.ident(), b.ident()}));
    r.log.control();
    r.log.data({{1, 1}, {2, 1}});
    r.start();
    r.drive();
    EXPECT_TRUE(a.has(1));
    EXPECT_TRUE(b.has(1));
    EXPECT_FALSE(a.has(2));   // 8 rejected the second new key: 7 applied nothing either
    EXPECT_FALSE(b.has(2));
    EXPECT_EQ(r.st.rejected.find(2)->constraintId, 0x00080001u);
}

TEST(CheckedWritesVerdictSource, FirstFailingMemberInSetOrderWins) {
    Rig r;
    auto& b = r.install(8, tagEquals(9));
    auto& a = r.install(7, tagEquals(9));
    r.st.constraintSets.onChange(0, setOf(1, {a.ident(), b.ident()}));
    r.log.control();
    r.log.data({{1, 9}});
    r.start();
    r.drive();
    EXPECT_EQ(r.st.rejected.find(1)->constraintId, 0x00070001u);
}

TEST(CheckedWritesVerdictSource, TombstonesAreNeverRejected) {
    Rig r;
    auto& m = r.install(7, capLive(0));
    r.st.constraintSets.onChange(0, setOf(1, {m.ident()}));
    r.log.control();
    r.log.data({{1, 0, true}});
    r.log.data({{2, 0}});
    r.start();
    r.drive();
    EXPECT_FALSE(r.st.rejected.isRejected(1));
    EXPECT_TRUE(r.st.rejected.isRejected(2));
}

TEST(CheckedWritesVerdictSource, CapLiveKeysCountsAcceptedOnly) {
    Rig r;
    auto& m = r.install(7, capLive(2));
    r.st.constraintSets.onChange(0, setOf(1, {m.ident()}));
    r.log.control();
    r.log.data({{1, 0}, {2, 0}, {3, 0}, {1, 0, true}, {3, 0}});   // offsets 1..5
    r.start();
    r.drive();
    EXPECT_TRUE(r.st.rejected.isRejected(3));
    EXPECT_FALSE(r.st.rejected.isRejected(5));
    EXPECT_EQ(r.st.rejected.size(), 1u);
    EXPECT_TRUE(m.has(3));
}

TEST(CheckedWritesVerdictSourceRegression, CapacityStallIsNeverAVerdictRegression) {
    Rig r;
    auto& m = r.install(7, tagEquals(9));
    r.st.constraintSets.onChange(0, setOf(1, {m.ident()}));
    r.st.rejected.reserve(1);
    r.log.control();
    r.log.data({{1, 9}, {2, 9}, {3, 1}});
    r.start();
    r.drive();
    EXPECT_TRUE(r.vs.stalled());
    EXPECT_EQ(r.vs.judgedThrough(), 2);
    EXPECT_EQ(r.st.rejected.size(), 1u);
    EXPECT_EQ(m.inst->table.size(), 0u);
    EXPECT_EQ(r.vs.stats().verdictStalls, 1u);
    EXPECT_EQ(r.drive(), 0);   // stalled: no work, no verdict
    EXPECT_EQ(r.vs.judgedThrough(), 2);
}

TEST(CheckedWritesVerdictSource, MarkerTriggers) {
    Rig r;
    auto& m = r.install(7, tagEquals(9));
    r.st.constraintSets.onChange(0, setOf(1, {m.ident()}));
    r.log.control();
    r.log.data({{1, 1}, {2, 1}});   // offsets 1, 2
    r.start();
    r.drive(50'000'000);
    EXPECT_TRUE(r.vs.appendedForTest().empty());   // 50 ms, nothing rejected
    r.drive(100'000'000);
    ASSERT_EQ(r.vs.appendedForTest().size(), 1u);   // the interval passed
    auto h = cr::parseMarkerHeader(r.vs.appendedForTest()[0].data(), r.vs.appendedForTest()[0].size());
    ASSERT_TRUE(h);
    EXPECT_EQ(h->fromOffset, 1);
    EXPECT_EQ(h->finalThrough, 3);
    EXPECT_EQ(h->count, 0u);
    r.log.data({{3, 9}});
    r.drive(101'000'000);
    ASSERT_EQ(r.vs.appendedForTest().size(), 2u);   // a rejection: at once
    h = cr::parseMarkerHeader(r.vs.appendedForTest()[1].data(), r.vs.appendedForTest()[1].size());
    EXPECT_EQ(h->fromOffset, 3);
    EXPECT_EQ(h->count, 1u);
}

TEST(CheckedWritesVerdictSource, MarkerSplitsAtTheEntryBeyondTheLimit) {
    Rig r;
    auto& m = r.install(7, tagEquals(9));
    r.st.constraintSets.onChange(0, setOf(1, {m.ident()}));
    r.log.control();
    std::vector<Rec> recs;
    for (int i = 1; i <= 4097; ++i) recs.push_back({i, 9});
    r.log.data(recs);   // offsets 1..4097
    r.start();
    r.drive();
    ASSERT_EQ(r.vs.appendedForTest().size(), 2u);
    const auto h0 = cr::parseMarkerHeader(r.vs.appendedForTest()[0].data(), r.vs.appendedForTest()[0].size());
    const auto h1 = cr::parseMarkerHeader(r.vs.appendedForTest()[1].data(), r.vs.appendedForTest()[1].size());
    EXPECT_EQ(h0->count, cr::kMaxMarkerEntries);
    EXPECT_EQ(h0->finalThrough, 4097);   // the 4097th entry's offset
    EXPECT_EQ(h1->fromOffset, 4097);
    EXPECT_EQ(h1->count, 1u);
    EXPECT_EQ(h1->finalThrough, 4098);
}

TEST(CheckedWritesVerdictSource, JoiningMemberBuildsUnjudgedToTheChangeOffset) {
    Rig r;
    auto& x = r.install(7, tagEquals(9));
    auto& y = r.install(8, tagEquals(77));
    r.st.constraintSets.onChange(0, setOf(1, {x.ident()}));
    r.st.constraintSets.onChange(20, setOf(2, {x.ident(), y.ident()}));
    r.log.control();                                                   // offset 0
    std::vector<Rec> early;
    for (int i = 1; i <= 19; ++i) early.push_back({i, i == 5 ? 9 : (i == 6 ? 77 : 1)});
    r.log.data(early);                                                 // offsets 1..19
    r.log.control();                                                   // offset 20: the change
    r.log.data({{21, 1}, {22, 77}, {23, 9}});                          // offsets 21..23
    r.start();
    r.drive();
    EXPECT_EQ(r.vs.judgedThrough(), 24);
    EXPECT_FALSE(x.has(5));                  // rejected by X
    EXPECT_FALSE(y.has(5));                  // Y skips the index entry while building
    EXPECT_TRUE(y.has(6));                   // 77 was not judged by Y before it joined
    EXPECT_TRUE(x.has(6));
    EXPECT_FALSE(y.has(22));                 // judged by both: Y rejects
    EXPECT_FALSE(x.has(22));
    EXPECT_TRUE(y.has(21));
    EXPECT_EQ(y.inst->foldCursor, 24);
    EXPECT_EQ(r.st.rejected.find(22)->constraintId, 0x00080001u);
}

TEST(CheckedWritesVerdictSource, LeavingMemberJudgesUntilItsLeaveOffset) {
    Rig r;
    auto& x = r.install(7, tagEquals(9));
    auto& y = r.install(8, tagEquals(77));
    r.st.constraintSets.onChange(0, setOf(1, {x.ident(), y.ident()}));
    r.st.constraintSets.onChange(30, setOf(2, {x.ident()}));
    r.log.control();
    std::vector<Rec> a, b;
    for (int i = 1; i <= 29; ++i) a.push_back({i, i == 25 ? 77 : 1});
    r.log.data(a);                                                     // 1..29
    r.log.control();                                                   // 30: the drop
    for (int i = 31; i <= 40; ++i) b.push_back({i, i == 35 ? 77 : 1});
    r.log.data(b);                                                     // 31..40
    r.start();
    r.drive(200'000'000);
    EXPECT_TRUE(r.st.rejected.isRejected(25));     // Y judged before the drop
    EXPECT_FALSE(r.st.rejected.isRejected(35));    // and not after
    EXPECT_TRUE(r.vs.retiringForTest(8));
    EXPECT_EQ(r.vs.memberCountForTest(), 2u);      // W <= 30: Y stays registered
    verdictOnHw(r.st, 1'000'000);                  // the markers the pass appended are committed
    EXPECT_GT(r.st.markers.committedThrough(), 30);
    r.drive(300'000'000);
    EXPECT_EQ(r.vs.memberCountForTest(), 1u);
}

TEST(CheckedWritesVerdictSourceRegression, MissingMemberWaitsNeverGuessesRegression) {
    Rig r;
    auto& x = r.install(9, tagEquals(9));
    r.st.constraintSets.onChange(0, setOf(1, {{9, x.rule.fingerprint() + 1}}));   // no such (id, fingerprint)
    r.log.control();
    r.log.data({{1, 9}});
    r.start();
    r.drive();
    EXPECT_TRUE(r.vs.waitingForInstall());
    EXPECT_EQ(r.vs.judgedThrough(), 1);
    EXPECT_EQ(r.vs.stats().judgeWaitsForInstall, 1u);
    r.drive();
    EXPECT_EQ(r.vs.stats().judgeWaitsForInstall, 1u);   // logged once per wait
    EXPECT_TRUE(r.st.rejected.size() == 0);
    r.vs.addVerdictView(x.inst.get(), VerdictViewKind::TestReject, 9, x.rule.fingerprint() + 1);
    r.drive();
    EXPECT_FALSE(r.vs.waitingForInstall());
    EXPECT_EQ(r.vs.judgedThrough(), 2);
}

TEST(CheckedWritesVerdictSource, FeedMergesMarkersAndChanges) {
    PartitionWriteState st;
    std::vector<uint8_t> change(cr::changeBatchBytes(1));
    const cr::ChangeMember cm{7, uint8_t(VerdictViewKind::TestReject), 1234};
    ASSERT_NE(cr::buildConstraintSetChange(change, 1, std::span<const cr::ChangeMember>(&cm, 1)), 0u);
    verdictFeedBatch(&st, change.data(), change.size(), 10);
    std::vector<uint8_t> marker(cr::markerBatchBytes(1));
    const cr::MarkerEntry me{2, kViolation, 0x00070001};
    ASSERT_NE(cr::buildVerdictMarker(marker, cr::MarkerHeader{10, 20, 1, 1}, std::span<const cr::MarkerEntry>(&me, 1)), 0u);
    verdictFeedBatch(&st, marker.data(), marker.size(), 25);
    EXPECT_EQ(st.constraintSets.setAt(11)->id, 1u);
    EXPECT_EQ(st.constraintSets.setAt(11)->members[0].planFingerprint, 1234u);
    EXPECT_TRUE(st.rejected.isRejected(12));
    EXPECT_EQ(st.rejected.coveredThrough(), 20);
    EXPECT_EQ(st.markers.appendedThrough(), 20);
    verdictFeedBegin(&st, 0, 11);   // a failover cut at 11
    EXPECT_EQ(st.constraintSets.size(), 1u);
    EXPECT_FALSE(st.rejected.isRejected(12));
}

TEST(CheckedWritesVerdictSource, ActivationExtendsCoverageOverEmptySets) {
    Rig r;
    auto& x = r.install(7, tagEquals(9));
    r.st.constraintSets.onChange(10, setOf(1, {x.ident()}));
    r.st.constraintSets.onChange(30, setOf(2, {}));
    r.st.constraintSets.onChange(50, setOf(3, {x.ident()}));
    r.st.rejected.onMarker(10, 31, {});
    r.start();
    r.drive();   // an empty log
    EXPECT_EQ(r.vs.judgedThrough(), 51);
}

TEST(CheckedWritesVerdictSource, KnownThroughFailsClosedWithoutAJudge) {
    PartitionWriteState st;
    EXPECT_EQ(st.verdictKnownThrough(), INT64_MAX);
    EXPECT_FALSE(st.isVerdictSource());
    st.constraintSets.onChange(10, setOf(1, {{7, 1}}));
    st.rejected.noteCovered(15);
    EXPECT_TRUE(st.isVerdictSource());
    EXPECT_TRUE(st.isConstrainedSource());
    EXPECT_EQ(st.verdictKnownThrough(), 15);
}
