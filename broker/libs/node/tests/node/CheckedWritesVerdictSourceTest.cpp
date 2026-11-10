// Part 04 Task 5: the judge of one source partition (unit level: no worker, no replica).
#include <gtest/gtest.h>

#include <filesystem>
#include <memory>
#include <vector>

#include <unistd.h>

#include <rune/codec/Descriptor.h>
#include <rune/node/AdminChannels.h>
#include <rune/node/VerdictSource.h>
#include <rune/node/WorkerViewSet.h>
#include <rune/node/RcState.h>
#include <rune/protocol/BatchRebuild.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/storage/Log.h>
#include <rune/storage/LogCleaner.h>
#include <rune/storage/ProducerStateManager.h>
#include <rune/transaction/ControlMarker.h>
#include <rune/transaction/ReadCommittedView.h>
#include <rune/view/AggFold.h>
#include <rune/view/PlanCompiler.h>
#include <rune/view/ViewFold.h>
#include <spdlog/sinks/ostream_sink.h>
#include <spdlog/spdlog.h>
#include <sstream>

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

struct Rec { int64_t id; int32_t tag; bool tombstone = false; uint8_t intent = 0; };

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
            b.append(sv(k), sv(v), 0, r.intent);
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
    TxnOverlayPool pool;
    VerdictSource vs{5, 0};
    std::vector<std::unique_ptr<Member>> members;

    Member& install(uint64_t id, TestRejectRule r) {
        members.push_back(std::make_unique<Member>(id, r));
        vs.installTestVerdictViewForTest(members.back()->inst.get(), id, r);
        return *members.back();
    }
    void start() {
        vs.bindTxn(&pool, {}, 0);   // replay folds against the pool's supersede keys; production always binds one
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
    std::ostringstream out;
    const auto saved = spdlog::default_logger();
    spdlog::set_default_logger(std::make_shared<spdlog::logger>("cw-wait", std::make_shared<spdlog::sinks::ostream_sink_mt>(out)));
    struct Restore { std::shared_ptr<spdlog::logger> l; ~Restore() { spdlog::set_default_logger(l); } } restore{saved};
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
    const std::string logged = out.str();
    EXPECT_NE(logged.find("waits for a view install"), std::string::npos);
    EXPECT_EQ(logged.find("waits for a view install"), logged.rfind("waits for a view install")) << "one line for the whole wait";
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
    ASSERT_NE(cr::buildConstraintSetChange(change, 1, -1, std::span<const cr::ChangeMember>(&cm, 1)), 0u);
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

TEST(CheckedWritesVerdictSource, JudgeSeesTheRecordIntent) {
    Rig r;
    auto& m = r.install(7, tagEquals(9));
    r.st.constraintSets.onChange(0, setOf(1, {m.ident()}));
    r.log.control();
    r.log.data({{1, 1, false, 1}});   // Intent::Insert
    r.start();
    r.drive();
    EXPECT_EQ(r.vs.lastIntentForTest(), 1);
}

// ---- the readers sweep (Review Focus 2): no log reader mistakes a checked-writes record for a txn marker ---------

namespace {

// Types 16 and 19 have no builder: a value of exactly six bytes ([version][5 bytes]) is what the old txn-marker
// reader accepted, so a reader without the hardening mistakes it for a marker.
std::vector<uint8_t> controlOfType(int16_t type, int64_t base) {
    std::vector<uint8_t> b;
    if (type == cr::kVerdictMarker) {
        b.resize(cr::markerBatchBytes(0));
        (void)cr::buildVerdictMarker(b, cr::MarkerHeader{0, 0, 0, 0}, {});
    } else if (type == cr::kConstraintSetChange) {
        b.resize(cr::changeBatchBytes(0));
        (void)cr::buildConstraintSetChange(b, 1, -1, {});
    } else if (type == cr::kVerdictPlan) {
        const std::vector<uint8_t> blob(24, 0x42);
        b.resize(cr::verdictPlanBatchBytes(cr::verdictPlanPayloadBytes(blob.size(), blob.size(), blob.size())));
        (void)cr::buildVerdictPlan(b, cr::VerdictPlanHeader{7, 0xF00D, 2}, blob, blob, blob);
    } else {
        b.resize(cr::detail::batchBytes(6));
        std::size_t p = cr::detail::beginBatch(b.data(), type, 6);
        b[p++] = cr::kPayloadVersion;
        for (int i = 0; i < 5; ++i) b[p++] = 0;
        cr::detail::sealBatch(b.data(), p);
    }
    for (int i = 0; i < 8; ++i) b[std::size_t(i)] = uint8_t(uint64_t(base) >> (8 * i));
    return b;
}

// A batch in the txn marker's own framing (no record-length varint) with a chosen producer id and key type, CRC resealed:
// what a txn-marker reader without the producerId >= 0 and type 0/1 checks would take for a marker.
std::vector<uint8_t> txnFramedLookalike(int64_t pid, int16_t type) {
    auto b = rune::transaction::ControlMarker::build(0, 0, rune::transaction::ControlType::Commit, 0);
    for (int i = 0; i < 8; ++i) b[43 + std::size_t(i)] = uint8_t(uint64_t(pid) >> (8 * i));
    b[65] = uint8_t(type);
    b[66] = uint8_t(uint16_t(type) >> 8);
    const uint32_t crc = rune::protocol::crc32c(b.data() + 21, b.size() - 21);
    for (int i = 0; i < 4; ++i) b[17 + std::size_t(i)] = uint8_t(crc >> (8 * i));
    return b;
}

std::vector<uint8_t> txnData(int64_t base, int64_t pid) {
    rune::protocol::RecordBatchBuilder b(base, 0);
    b.setProducerId(pid); b.setProducerEpoch(0); b.setBaseSequence(0);
    b.setTransactional(true);
    b.append("k", "v", 0);
    return b.build();
}
std::vector<uint8_t> commitMarker(int64_t base, int64_t pid) {
    auto b = rune::transaction::ControlMarker::build(pid, 0, rune::transaction::ControlType::Commit, 0);
    for (int i = 0; i < 8; ++i) b[std::size_t(i)] = uint8_t(uint64_t(base) >> (8 * i));
    return b;
}
std::vector<uint8_t> plainData(int64_t base, std::string_view key) {
    rune::protocol::RecordBatchBuilder b(base, 0);
    b.append(key, "v", 0);
    return b.build();
}

struct TmpDir {
    std::filesystem::path path;
    explicit TmpDir(const char* name) : path(std::filesystem::temp_directory_path() / (std::string(name) + "-" + std::to_string(::getpid()))) {
        std::filesystem::remove_all(path);
        std::filesystem::create_directories(path);
    }
    ~TmpDir() { std::error_code ec; std::filesystem::remove_all(path, ec); }
};

}  // namespace

TEST(CheckedWritesControlRecordRegression, EveryLogReaderSkipsTypes16To20Regression) {
    namespace tx = rune::transaction;
    for (const auto& lookalike : {txnFramedLookalike(-1, 0), txnFramedLookalike(5, cr::kVerdictMarker), txnFramedLookalike(-1, cr::kTxnJudge), txnFramedLookalike(-1, cr::kVerdictPlan)}) {
        tx::LsoTracker tracker(8);
        tx::AbortedTxnIndex aborted;
        tx::RcBuilder builder(&tracker, &aborted, false);
        EXPECT_EQ(builder.step(lookalike.data(), lookalike.size()).outcome, tx::RcBuilder::Outcome::Ignored);
        tx::ControlMarkerPayload payload{};
        EXPECT_FALSE(tx::ControlMarker::extract(lookalike.data(), 0, payload, lookalike.size()));
    }
    for (const int16_t type : {cr::kReservation, cr::kVerdictMarker, cr::kConstraintSetChange, cr::kTxnJudge, cr::kVerdictPlan}) {
        SCOPED_TRACE("control record type " + std::to_string(type));
        const std::vector<uint8_t> ctl = controlOfType(type, 0);

        {   // (1) RcBuilder::step
            tx::LsoTracker tracker(8);
            tx::AbortedTxnIndex aborted;
            tx::RcBuilder builder(&tracker, &aborted, false);
            const auto res = builder.step(ctl.data(), ctl.size());
            EXPECT_EQ(res.outcome, tx::RcBuilder::Outcome::Ignored);
            EXPECT_EQ(tracker.lso(100), 100);
            EXPECT_TRUE(aborted.ranges().empty());
        }
        {   // (2) ControlMarker::extract
            tx::ControlMarkerPayload payload{};
            EXPECT_FALSE(tx::ControlMarker::extract(ctl.data(), 0, payload, ctl.size()));
        }
        {   // (7) forEachRecordSpan parses it as one record
            int n = 0;
            EXPECT_TRUE(rune::protocol::forEachRecordSpan(ctl.data(), ctl.size(), [&](const rune::protocol::RecordSpan& s) {
                ++n;
                EXPECT_EQ(s.key.size(), 2u);
            }));
            EXPECT_EQ(n, 1);
        }
        {   // (5) both folds count it as control and touch no table
            const rune::storage::LogReadView lv{ctl.data(), ctl.size(), 0, 1};
            Member m(7, tagEquals(9));
            int64_t cursor = 0;
            const auto st = rune::view::foldBatch(m.inst->plan, m.inst->layout, m.inst->vdc, m.inst->table, lv, 1, cursor, rune::view::kNoSkip);
            EXPECT_EQ(st.skippedControl, 1u);
            EXPECT_EQ(m.inst->table.size(), 0u);

            const rune::codec::Descriptor src = rune::codec::DescriptorBuilder("order", 1).add("orderId", P::Int64, 1, true).add("userId", P::Int64, 1).add("amount", P::Int64, 1).build();
            rune::view::ViewPlan plan;
            plan.viewName = "totals"; plan.key = "userId"; plan.sources = {{5, "userId"}};
            plan.output = {{"userId", P::Int64, 0, "userId", rune::view::MergeRule::Replace}, {"total", P::Int64, 0, "amount", rune::view::MergeRule::Sum}};
            const rune::codec::Descriptor* srcs[] = {&src};
            const auto comp = rune::view::compileView(plan, srcs, 9).value();
            const auto agg = rune::view::buildAggLayout(comp.plan, src).value();
            rune::view::VersionDescriptorCache vdc{src};
            rune::view::ViewTable view(comp.rowLayout, 256, 1u << 16), contributors(agg.contributorRow, 256, 1u << 16), groups(agg.groupRow, 256, 1u << 16);
            cursor = 0;
            const auto sa = rune::view::foldAggregateBatch(comp.plan, comp.rowLayout, agg, vdc, view, contributors, groups, lv, 1, cursor, rune::view::kNoSkip);
            EXPECT_EQ(sa.skippedControl, 1u);
            EXPECT_EQ(view.size() + contributors.size() + groups.size(), 0u);
        }
        {   // (4) + (9): RcState over a log with a committed txn around it builds Ready, with no open txn, no aborted
            //     range, and a producer-state manager that learned only the real producer.
            TmpDir dir("cwsweep-rc");
            rune::storage::Log log(dir.path.string());
            log.recover();
            log.append(txnData(0, 77), 0);
            log.append(commitMarker(1, 77), 1);
            log.append(controlOfType(type, 2), 2);
            log.append(plainData(3, "after"), 3);
            rune::storage::ProducerStateManager psm;
            rune::node::RcState rc;
            rc.reserve(8);
            rc.bindProducerState(&psm, 5, 0);
            const rune::node::RcFacts facts{.leader = true, .logGeneration = 1, .leadershipGeneration = 1, .epoch = 1};
            for (int i = 0; i < 64 && !rc.ready(); ++i) (void)rc.advance(facts, &log, 1 << 20);
            ASSERT_TRUE(rc.ready());
            EXPECT_EQ(rc.lso(4), 4);
            EXPECT_TRUE(rc.aborted().ranges().empty());
            EXPECT_EQ(psm.size(), 1u);
        }
        {   // (3) the cleaner's pre-pass: a log with the record compacts exactly like the log without it
            const auto cleanedKeys = [&](bool withControl) {
                TmpDir dir("cwsweep-clean");
                rune::storage::Log log(dir.path.string(), /*segmentBytes=*/1);
                log.recover();
                int64_t off = 0;
                log.append(plainData(off, "k"), off); ++off;
                if (withControl) { log.append(controlOfType(type, off), off); ++off; }
                log.append(plainData(off, "k"), off); ++off;
                log.append(plainData(off, "pad"), off); ++off;
                log.append(plainData(off, "pad2"), off); ++off;
                rune::storage::LogCleaner cleaner;
                cleaner.clean(log, 0, log.segmentBaseOffsets().back(), 0, 1'000'000);
                log.reloadSegments();
                std::vector<std::string> keys;
                for (int64_t o = 0; o < off; ++o) {
                    const auto bytes = log.read(o, 1 << 20);
                    if (bytes.empty()) continue;
                    rune::protocol::RecordBatchReader r(bytes.data(), bytes.size());
                    if (r.baseOffset() != o || !r.ensureValid() || r.isControl()) continue;
                    r.forEachRecord([&](std::string_view k, std::string_view, int64_t, int64_t) { keys.emplace_back(k); });
                }
                return keys;
            };
            // A marker with no set change before it is held by the cleaner (fail closed): only the other types compact like no record.
            if (type != cr::kVerdictMarker) EXPECT_EQ(cleanedKeys(true), cleanedKeys(false));
        }
    }
}

// The type-17 marker through the cleaner, in context (the sweep above skips it because a lone marker is held): a log
// [set change, data, marker A, data, marker B, pads].  The cleaner keeps marker A while the replica bound (C11) has not passed
// it, and drops it two passes after the bound passes it; the last marker (B) is the coverage and is always kept.
TEST(CheckedWritesControlRecordRegression, VerdictMarkerInContextFollowsTheReplicaBoundThroughTheCleanerRegression) {
    const auto markerOf = [](int64_t base, int64_t from, int64_t ft) {
        std::vector<uint8_t> b(cr::markerBatchBytes(0));
        (void)cr::buildVerdictMarker(b, cr::MarkerHeader{from, ft, 1, 0}, {});
        for (int i = 0; i < 8; ++i) b[std::size_t(i)] = uint8_t(uint64_t(base) >> (8 * i));
        return b;
    };
    const auto hasBatchAt = [](rune::storage::Log& log, int64_t off) {
        const auto bytes = log.read(off, 1 << 20);
        if (bytes.empty()) return false;
        rune::protocol::RecordBatchReader r(bytes.data(), bytes.size());
        return r.baseOffset() == off;
    };
    const auto run = [&](int64_t bound, bool& aKept, bool& bKept, bool& changeKept) {
        TmpDir dir("cwsweep-ctx");
        rune::storage::Log log(dir.path.string(), /*segmentBytes=*/1);
        log.recover();
        log.append(controlOfType(cr::kConstraintSetChange, 0), 0);
        log.append(plainData(1, "k"), 1);
        log.append(markerOf(2, 1, 2), 2);
        log.append(plainData(3, "k"), 3);
        log.append(markerOf(4, 3, 4), 4);
        log.append(plainData(5, "pad"), 5);
        log.append(plainData(6, "pad2"), 6);
        const int64_t t0 = 1'000'000;
        for (const int64_t now : {t0, t0 + 5000}) {
            rune::storage::LogCleaner cleaner;
            cleaner.setMarkerRemovalBound(bound);
            cleaner.clean(log, 0, log.segmentBaseOffsets().back(), 0, 1000, now);
            log.reloadSegments();
        }
        aKept = hasBatchAt(log, 2);
        bKept = hasBatchAt(log, 4);
        changeKept = hasBatchAt(log, 0);
    };
    bool aKept = false, bKept = false, changeKept = false;
    run(/*bound=*/0, aKept, bKept, changeKept);
    EXPECT_TRUE(aKept) << "bound 0: no replica has fetched marker A";
    EXPECT_TRUE(bKept);
    EXPECT_TRUE(changeKept);
    run(/*bound=*/5, aKept, bKept, changeKept);
    EXPECT_FALSE(aKept) << "every replica passed marker A: it is dropped after the horizon";
    EXPECT_TRUE(bKept) << "the last marker is the coverage";
    EXPECT_TRUE(changeKept) << "set changes are always kept";
}

// The drop of one verdict view retires it per (template, partition): partition 0 finishing its drop must not take the member from
// partition 1's judge, which is still judging records below its own drop.
TEST(CheckedWritesVerdictSourceRegression, RetireIsPerPartitionRegression) {
    ViewSnapshotChannel channel;
    const std::string dir = (std::filesystem::temp_directory_path() / ("rune-cw-retire-" + std::to_string(::getpid()))).string();
    struct Cleanup { std::string d; ~Cleanup() { std::error_code ec; std::filesystem::remove_all(d, ec); } } cleanup{dir};
    WorkerViewSet vs(channel, dir);
    const rune::codec::Descriptor src = tagSource();
    const rune::codec::Descriptor* srcs[] = {&src};
    const TestRejectRule rule = tagEquals(9);
    constexpr int32_t kViewTopic = 0x40000001;
    constexpr uint64_t kObject = 7;
    auto comp = rune::view::compileView(tagView(), srcs, kViewTopic).value();
    vs.installTestVerdictViewForTest(comp.plan, comp.rowLayout, src, false, kObject, rule);

    Log log[2];
    TopicWriteSpec spec;
    PartitionWriteState st[2];
    for (int p = 0; p < 2; ++p) {
        st[p].spec = &spec;
        log[p].control();                                                             // 0: the set
        std::vector<Rec> a, b;
        for (int i = 1; i <= 9; ++i) a.push_back({i, i == 3 ? 9 : 1});
        log[p].data(a);                                                               // 1..9
        log[p].control();                                                             // 10: the drop
        for (int i = 11; i <= 15; ++i) b.push_back({i, 1});
        log[p].data(b);                                                               // 11..15
        st[p].constraintSets.onChange(0, setOf(1, {{kObject, rule.fingerprint()}}));
        st[p].constraintSets.onChange(10, setOf(2, {}));
    }
    const auto drive = [&](int p) {
        auto readRange = [&](int64_t from, int64_t) { return log[p].read(from); };
        vs.driveFolds(5, p, readRange, log[p].next, rune::view::kNoSkip, 200'000'000, nullptr, 1, true, {}, &st[p], vs.ensureVerdictSource(5, p, st[p]));
        verdictOnHw(st[p], 1'000'000);
    };
    for (int p = 0; p < 2; ++p) vs.ensureVerdictSource(5, p, st[p])->bindLeader(nullptr, 1, 0);
    vs.verdictSource(5, 1)->holdJudgeForTest(true);
    ASSERT_TRUE(vs.uninstallView(kViewTopic));   // judged by both: it retires
    for (int i = 0; i < 4; ++i) drive(0);
    EXPECT_EQ(vs.verdictSource(5, 0) == nullptr ? 0u : vs.verdictSource(5, 0)->memberCountForTest(), 0u);
    ASSERT_NE(vs.verdictSource(5, 1), nullptr);
    EXPECT_EQ(vs.verdictSource(5, 1)->memberCountForTest(), 1u) << "partition 0 finishing its drop leaves partition 1's member alone";
    vs.verdictSource(5, 1)->holdJudgeForTest(false);
    for (int i = 0; i < 4; ++i) drive(1);
    EXPECT_TRUE(st[1].rejected.isRejected(3)) << "partition 1 judged below its drop with the member";
    EXPECT_GE(st[1].markers.committedThrough(), 11);
    EXPECT_TRUE(vs.verdictSource(5, 1) == nullptr || vs.verdictSource(5, 1)->judgedThrough() >= log[1].next);
}

// A retiring view is attached only by a judge that is activated: partition 1's fresh judge is already past the drop and would never retire it.
TEST(CheckedWritesVerdictSourceRegression, FreshJudgePastTheDropDoesNotAttachRetiringViewRegression) {
    ViewSnapshotChannel channel;
    const std::string dir = (std::filesystem::temp_directory_path() / ("rune-cw-retire-fresh-" + std::to_string(::getpid()))).string();
    struct Cleanup { std::string d; ~Cleanup() { std::error_code ec; std::filesystem::remove_all(d, ec); } } cleanup{dir};
    WorkerViewSet vs(channel, dir);
    const rune::codec::Descriptor src = tagSource();
    const rune::codec::Descriptor* srcs[] = {&src};
    const TestRejectRule rule = tagEquals(9);
    constexpr int32_t kViewTopic = 0x40000001;
    constexpr uint64_t kObject = 7;
    auto comp = rune::view::compileView(tagView(), srcs, kViewTopic).value();
    vs.installTestVerdictViewForTest(comp.plan, comp.rowLayout, src, false, kObject, rule);

    Log log[2];
    TopicWriteSpec spec;
    PartitionWriteState st[2];
    for (int p = 0; p < 2; ++p) {
        st[p].spec = &spec;
        log[p].control();                                                             // 0: the set
        std::vector<Rec> a, b;
        for (int i = 1; i <= 9; ++i) a.push_back({i, i == 3 ? 9 : 1});
        log[p].data(a);                                                               // 1..9
        log[p].control();                                                             // 10: the drop
        for (int i = 11; i <= 15; ++i) b.push_back({i, 1});
        log[p].data(b);                                                               // 11..15
        st[p].constraintSets.onChange(0, setOf(1, {{kObject, rule.fingerprint()}}));
        st[p].constraintSets.onChange(10, setOf(2, {}));
    }
    const auto drive = [&](int p) {
        auto readRange = [&](int64_t from, int64_t) { return log[p].read(from); };
        vs.driveFolds(5, p, readRange, log[p].next, rune::view::kNoSkip, 200'000'000, nullptr, 1, true, {}, &st[p], vs.ensureVerdictSource(5, p, st[p]));
        verdictOnHw(st[p], 1'000'000);
    };
    vs.ensureVerdictSource(5, 0, st[0])->bindLeader(nullptr, 1, 0);
    ASSERT_TRUE(vs.uninstallView(kViewTopic));   // partition 0 judges it: it retires
    st[1].rejected.reserve(kIndexReserve);
    st[1].rejected.noteCovered(16);              // partition 1's previous leader finished the drop
    VerdictSource* fresh = vs.ensureVerdictSource(5, 1, st[1]);
    ASSERT_NE(fresh, nullptr);
    fresh->bindLeader(nullptr, 1, 0);
    EXPECT_EQ(fresh->memberCountForTest(), 0u) << "not activated: it holds nothing it could retire";
    for (int i = 0; i < 4; ++i) drive(1);
    EXPECT_EQ(vs.verdictSource(5, 1)->memberCountForTest(), 0u) << "past the drop, it never attaches the view";
    for (int i = 0; i < 4; ++i) drive(0);
    EXPECT_EQ(vs.retiringVerdictTemplatesForTest(), 0u) << "partition 0 passed its drop and nothing else holds the view";
}

// A View ack waits for the views of the source to fold past it; a verdict view that was dropped and is only judged up to its
// drop is not one of them.
TEST(CheckedWritesVerdictSourceRegression, RetiringMemberDoesNotHoldViewAcksRegression) {
    ViewSnapshotChannel channel;
    const std::string dir = (std::filesystem::temp_directory_path() / ("rune-cw-retire-ack-" + std::to_string(::getpid()))).string();
    struct Cleanup { std::string d; ~Cleanup() { std::error_code ec; std::filesystem::remove_all(d, ec); } } cleanup{dir};
    WorkerViewSet vs(channel, dir);
    const rune::codec::Descriptor src = tagSource();
    const rune::codec::Descriptor* srcs[] = {&src};
    const TestRejectRule rule = tagEquals(9);
    constexpr int32_t kViewTopic = 0x40000001;
    auto comp = rune::view::compileView(tagView(), srcs, kViewTopic).value();
    vs.installTestVerdictViewForTest(comp.plan, comp.rowLayout, src, false, 7, rule);
    TopicWriteSpec spec;
    PartitionWriteState st;
    st.spec = &spec;
    st.constraintSets.onChange(0, setOf(1, {{7, rule.fingerprint()}}));
    vs.ensureVerdictSource(5, 0, st)->bindLeader(nullptr, 1, 0);
    ASSERT_EQ(vs.minFoldCursor(5, 0), 0) << "the verdict view has not folded";
    ASSERT_TRUE(vs.uninstallView(kViewTopic));
    EXPECT_EQ(vs.minFoldCursor(5, 0), INT64_MAX) << "dropped: the ack no longer waits for it";
}

// The constraint set is reconciled with the templates only when a template was installed, retired or erased, or the logged set
// changed: a steady pass skips it.
TEST(CheckedWritesVerdictSourceRegression, ConstraintSetIsReconciledOnlyOnAChangeRegression) {
    ViewSnapshotChannel channel;
    const std::string dir = (std::filesystem::temp_directory_path() / ("rune-cw-sync-" + std::to_string(::getpid()))).string();
    struct Cleanup { std::string d; ~Cleanup() { std::error_code ec; std::filesystem::remove_all(d, ec); } } cleanup{dir};
    WorkerViewSet vs(channel, dir);
    const rune::codec::Descriptor src = tagSource();
    const rune::codec::Descriptor* srcs[] = {&src};
    constexpr int32_t kViewTopic = 0x40000001;
    auto comp = rune::view::compileView(tagView(), srcs, kViewTopic).value();
    vs.installTestVerdictViewForTest(comp.plan, comp.rowLayout, src, false, 7, tagEquals(9));
    Log log;
    TopicWriteSpec spec;
    PartitionWriteState st;
    st.spec = &spec;
    const auto drive = [&] {
        auto readRange = [&](int64_t from, int64_t) { return log.read(from); };
        vs.driveFolds(5, 0, readRange, log.next, rune::view::kNoSkip, 200'000'000, nullptr, 1, true, {}, &st, vs.ensureVerdictSource(5, 0, st));
    };
    vs.ensureVerdictSource(5, 0, st)->bindLeader(nullptr, 1, 0);
    drive();
    drive();
    VerdictSource* judge = vs.verdictSource(5, 0);
    ASSERT_NE(judge, nullptr);
    const uint64_t synced = judge->constraintSyncEpoch;
    ASSERT_NE(synced, UINT64_MAX);
    drive();
    EXPECT_EQ(judge->constraintSyncEpoch, synced) << "a steady pass does not reconcile";
    ASSERT_TRUE(vs.uninstallView(kViewTopic));
    drive();
    EXPECT_NE(vs.verdictSource(5, 0) == nullptr ? UINT64_MAX : vs.verdictSource(5, 0)->constraintSyncEpoch, synced) << "a retired template does";
}

// A view added before the first bindLeader has no judge cursor to compare its checkpoint with: it waits for the bind.
TEST(CheckedWritesVerdictSourceRegression, AddVerdictViewBeforeBindDoesNotDiscardCheckpointRegression) {
    struct Discards {
        int n = 0;
        ViewDiscardReason reason{};
        static void count(void* ctx, ViewInstance&, ViewDiscardReason why) { ++static_cast<Discards*>(ctx)->n; static_cast<Discards*>(ctx)->reason = why; }
    } discards;
    Rig r;
    r.vs.setViewHooks({&discards, nullptr, &Discards::count, nullptr});
    const TestRejectRule rule = tagEquals(9);
    r.members.push_back(std::make_unique<Member>(7, rule));
    ViewInstance& inst = *r.members.back()->inst;
    inst.persisted  = true;
    inst.foldCursor = 5;
    r.st.constraintSets.onChange(0, setOf(1, {r.members.back()->ident()}));
    r.st.rejected.noteCovered(10);
    r.log.control();
    std::vector<Rec> recs;
    for (int i = 1; i <= 9; ++i) recs.push_back({i, 1});
    r.log.data(recs);
    r.vs.installTestVerdictViewForTest(&inst, 7, rule);   // before any bindLeader
    EXPECT_EQ(discards.n, 0);
    EXPECT_EQ(inst.foldCursor, 5);
    r.start();
    r.drive();
    EXPECT_EQ(discards.n, 0) << "the checkpoint is behind the markers this node holds";
    EXPECT_EQ(r.vs.stats().verdictCheckpointsDiscarded, 0u);
}

// V6 (view backups): a persisted member whose checkpoint outran the markers is handed to the owner's discard hook with the
// AheadOfVerdicts reason; the judge itself no longer touches the member's cursor (the owner backs the file up and rebuilds).
TEST(CheckedWritesVerdictSourceRegression, DiscardHookReceivesAheadOfVerdictsReasonRegression) {
    struct Discards {
        int n = 0;
        ViewDiscardReason reason{};
        static void count(void* ctx, ViewInstance&, ViewDiscardReason why) { ++static_cast<Discards*>(ctx)->n; static_cast<Discards*>(ctx)->reason = why; }
    } discards;
    Rig r;
    r.vs.setViewHooks({&discards, nullptr, &Discards::count, nullptr});
    const TestRejectRule rule = tagEquals(9);
    r.members.push_back(std::make_unique<Member>(7, rule));
    ViewInstance& inst = *r.members.back()->inst;
    inst.persisted  = true;
    inst.foldCursor = 50;   // far ahead of every marker this node holds (none)
    r.st.rejected.noteCovered(10);
    r.log.control();
    std::vector<Rec> recs;
    for (int i = 1; i <= 9; ++i) recs.push_back({i, 1});
    r.log.data(recs);
    r.vs.installTestVerdictViewForTest(&inst, 7, rule);
    r.start();
    r.drive();
    EXPECT_EQ(discards.n, 1);
    EXPECT_EQ(discards.reason, ViewDiscardReason::AheadOfVerdicts);
    EXPECT_EQ(inst.foldCursor, 50) << "the judge leaves the cursor to the owner";
    EXPECT_EQ(r.vs.stats().verdictCheckpointsDiscarded, 1u);
}

// A leader cut that loses the uncommitted marker leaves judged records without cover; an idle partition still covers them.
TEST(CheckedWritesVerdictSourceRegression, LeaderCutLosingMarkerStillCoversJudgedRecordsRegression) {
    Rig r;
    auto& m = r.install(7, tagEquals(9));
    r.st.constraintSets.onChange(0, setOf(1, {m.ident()}));
    r.log.control();
    r.log.data({{1, 1}, {2, 1}});
    r.start();
    r.drive(100'000'000);
    ASSERT_EQ(r.vs.appendedForTest().size(), 1u);
    ASSERT_EQ(r.vs.judgedThrough(), 3);
    r.st.markers.truncateFrom(0);   // the marker never committed
    r.vs.bindLeader(nullptr, 2, 0);
    for (int i = 1; i <= 4; ++i) {
        r.drive(100'000'000 + i * 100'000'000);
        verdictOnHw(r.st, 1'000'000);
    }
    EXPECT_EQ(r.st.markers.committedThrough(), 3);
    EXPECT_EQ(r.vs.appendedForTest().size(), 2u);
}

// A new leader whose HW lags its log's markers (a restart, retention 0): it re-marks nothing those markers cover (spec §8.5,
// it writes markers only for what it judges after them).  The re-mark came from its own index, which compaction had thinned.
TEST(CheckedWritesVerdictSourceRegression, NewLeaderWithALaggingHwDoesNotRemarkBelowItsLogsMarkersRegression) {
    Rig r;
    auto& m = r.install(7, tagEquals(9));
    r.st.constraintSets.onChange(0, setOf(1, {m.ident()}));
    r.log.control();
    std::vector<Rec> recs;
    for (int64_t id = 1; id <= 13; ++id) recs.push_back({id, id == 5 ? 9 : 1});
    r.log.data(recs);
    r.log.control();   // the marker at 14 covering [1,14)
    std::vector<uint8_t> marker(cr::markerBatchBytes(1));
    const cr::MarkerEntry me{4, kViolation, rune::view::constraintIdOf(7, 1)};
    ASSERT_NE(cr::buildVerdictMarker(marker, cr::MarkerHeader{1, 14, 1, 1}, std::span<const cr::MarkerEntry>(&me, 1)), 0u);
    verdictFeedBatch(&r.st, marker.data(), marker.size(), 14);
    ASSERT_EQ(r.st.markers.committedThrough(), 0) << "no HW yet";
    r.start();
    r.drive(1'000'000'000);
    EXPECT_TRUE(r.vs.appendedForTest().empty()) << "re-marked what the marker at 14 covers";
    EXPECT_EQ(r.vs.judgedThrough(), 15);
    r.log.data({{20, 9}});
    r.drive(2'000'000'000);
    ASSERT_EQ(r.vs.appendedForTest().size(), 1u);
    const auto h = cr::parseMarkerHeader(r.vs.appendedForTest()[0].data(), r.vs.appendedForTest()[0].size());
    ASSERT_TRUE(h.has_value());
    EXPECT_EQ(h->fromOffset, 14) << "marks from the coverage of the marker at 14, no gap";
    EXPECT_EQ(h->finalThrough, 16);
    verdictOnHw(r.st, 1'000'000);
    EXPECT_EQ(r.st.markers.committedThrough(), 16);
    EXPECT_EQ(r.st.rejected.mismatches(), 0u);
}

// The leadership scan meets more markers above its HW than the ledger holds.  They are in the log all the same, so the new leader
// re-marks nothing they cover.  RED: a marker the full ledger left out did not raise appendedThrough, and the leader re-marked
// [the 1024th marker's coverage, judgeCursor) from its own index.
TEST(CheckedWritesVerdictSourceRegression, NewLeaderPastAFullMarkerLedgerDoesNotRemarkRegression) {
    Rig r;
    auto& m = r.install(7, tagEquals(9));
    r.st.constraintSets.onChange(0, setOf(1, {m.ident()}));
    r.log.control();
    int64_t from = 1;
    for (int64_t k = 1; k <= int64_t(MarkerLedger::kCapacity) + 8; ++k) {
        r.log.data({{k, 1}});
        const int64_t at = r.log.next;
        r.log.control();   // the marker at `at` covering [from, at)
        std::vector<uint8_t> marker(cr::markerBatchBytes(0));
        ASSERT_NE(cr::buildVerdictMarker(marker, cr::MarkerHeader{from, at, 1, 0}, {}), 0u);
        verdictFeedBatch(&r.st, marker.data(), marker.size(), at);
        from = at;
    }
    ASSERT_TRUE(r.st.markers.full()) << "no HW yet: every marker is above it";
    EXPECT_EQ(r.st.markers.appendedThrough(), from);
    r.start();
    r.drive(1'000'000'000);
    verdictOnHw(r.st, r.log.next);   // the scanned markers commit: the ledger drains and emission may go on
    r.drive(2'000'000'000);
    EXPECT_TRUE(r.vs.appendedForTest().empty()) << "re-marked what the markers past the full ledger cover";
    EXPECT_EQ(r.st.rejected.mismatches(), 0u);
}

TEST(CheckedWritesVerdictSourceRegression, RequestChangeAboveTwoToTheThirtyOneReturnsTheOffsetRegression) {
    constexpr int64_t kOffset = (int64_t{1} << 31) + 5;
    Rig r;
    auto& m = r.install(7, tagEquals(9));
    r.log.next = kOffset - 1;
    r.log.control();
    r.st.constraintSets.onChange(kOffset - 1, setOf(1, {}));
    r.start();
    r.drive();
    ASSERT_EQ(r.vs.judgedThrough(), kOffset);
    const int64_t at = r.vs.requestChange(setOf(2, {m.ident()}));
    EXPECT_EQ(at, kOffset);
    EXPECT_EQ(r.st.constraintSets.lastChangeOffset(), kOffset);
}

// The last record before the committed marker was rejected and compaction removed it, at the end of its segment: after a
// restart a member rebuilding to W reads [cursor, W) from the gap, the next segment starts at W, so the read is empty.  The
// member must still reach W, or the judge never moves again (the View acks of every later write time out).
TEST(CheckedWritesVerdictSourceRegression, RebuildAcrossACompactionGapBeforeTheMarkerRegression) {
    TmpDir dir("cw-rebuild-gap");
    rune::storage::Log log(dir.path.string(), /*segmentBytes=*/1);   // one batch per segment
    log.recover();
    Member x(7, tagEquals(9));
    const auto put = [&](std::vector<uint8_t> b, int64_t base, int64_t count) {
        for (int i = 0; i < 8; ++i) b[std::size_t(i)] = uint8_t(uint64_t(base) >> (8 * i));
        log.append(b, base + count - 1);
    };
    const auto data = [&](int64_t base, const std::vector<Rec>& recs) {
        Log b;
        b.next = base;
        b.data(recs);
        put(b.bytes, base, int64_t(recs.size()));
    };
    std::vector<uint8_t> change(cr::changeBatchBytes(1));
    const cr::ChangeMember cm{x.objectId, uint8_t(VerdictViewKind::TestReject), x.rule.fingerprint()};
    (void)cr::buildConstraintSetChange(change, 1, -1, std::span<const cr::ChangeMember>(&cm, 1));
    put(change, 0, 1);                                                  // 0: the set {x}
    data(1, {{1, 1}});                                                  // 1
    data(2, {{2, 9}});                                                  // 2: rejected, compacted away
    std::vector<uint8_t> marker(cr::markerBatchBytes(1));
    const cr::MarkerEntry rejected{1, kViolation, rune::view::constraintIdOf(x.objectId, 1)};
    (void)cr::buildVerdictMarker(marker, cr::MarkerHeader{1, 3, 1, 1}, std::span<const cr::MarkerEntry>(&rejected, 1));
    put(marker, 3, 1);                                                  // 3: W = 3, lists 2
    data(4, {{2, 2}});                                                  // 4: written after W
    data(5, {{3, 1}});                                                  // 5: the active segment
    rune::storage::LogCleaner cleaner;
    cleaner.clean(log, 0, log.segmentBaseOffsets().back(), 0, 1'000'000);
    log.reloadSegments();
    std::vector<uint8_t> scratch(1 << 20);
    const auto readRange = [&](int64_t from, int64_t to) {   // the worker's readNoFault_
        rune::storage::ReadIntoStatus rs;
        return log.readInto(from, 1 << 16, scratch.data(), scratch.size(), rs, to);
    };
    const auto segs = log.segmentBaseOffsets();
    ASSERT_NE(std::find(segs.begin(), segs.end(), 3), segs.end());   // the marker starts its segment
    ASSERT_TRUE(readRange(2, 3).empty());                             // the gap: nothing at 2 below W
    ASSERT_FALSE(readRange(2, 6).empty());

    PartitionWriteState st;
    verdictFeedBegin(&st, 0, 0);   // the restarted leader's scan of what is left
    for (int64_t o = 0; o < log.logEndOffset(); ++o) {
        const auto b = log.read(o, 1 << 20);
        if (b.empty()) continue;
        rune::protocol::RecordBatchReader r(b.data(), b.size());
        if (r.baseOffset() != o) continue;
        verdictFeedBatch(&st, b.data(), std::size_t(r.totalLength()), o);
    }
    st.scanHw = log.logEndOffset();
    verdictOnHw(st, log.logEndOffset());
    ASSERT_EQ(st.rejected.coveredThrough(), 3);

    TxnOverlayPool pool;
    VerdictSource vs{5, 0};
    vs.bindTxn(&pool, {}, 0);
    vs.installTestVerdictViewForTest(x.inst.get(), x.objectId, x.rule);
    vs.bindState(&st);
    vs.bindLeader(nullptr, 1, log.logStartOffset());
    for (int i = 0; i < 4; ++i) (void)vs.driveJudge(readRange, log.logEndOffset(), 0, st);
    EXPECT_EQ(x.inst->foldCursor, 6);
    EXPECT_EQ(vs.judgedThrough(), 6);
    EXPECT_TRUE(x.has(1));
    EXPECT_TRUE(x.has(2));   // from 4
    EXPECT_TRUE(x.has(3));
}

namespace {
// 0 change {x}, 1 data, 2 rejected (compacted away, so its segment's tail is a gap), 3 data (an open transaction's first
// record: the LSO, the bound, stays at 3), 4 the committed marker (W = 3, lists 2), 5 data, 6 a marker above the HW
// (K = 6) or data (K = 3), 7 the active segment.  HW = 5.  Drives the judge of a new leader at bound 3; the member must
// reach judgeCursor_ = K.
void catchUpAcrossTheGapAtTheBound(const char* name, bool markerAboveHw) {
    TmpDir dir(name);
    rune::storage::Log log(dir.path.string(), /*segmentBytes=*/1);   // one batch per segment
    log.recover();
    Member x(7, tagEquals(9));
    const auto put = [&](std::vector<uint8_t> b, int64_t base, int64_t count) {
        for (int i = 0; i < 8; ++i) b[std::size_t(i)] = uint8_t(uint64_t(base) >> (8 * i));
        log.append(b, base + count - 1);
    };
    const auto data = [&](int64_t base, const std::vector<Rec>& recs) {
        Log b;
        b.next = base;
        b.data(recs);
        put(b.bytes, base, int64_t(recs.size()));
    };
    const auto marker = [&](int64_t at, int64_t from, int64_t through, std::span<const cr::MarkerEntry> entries) {
        std::vector<uint8_t> m(cr::markerBatchBytes(entries.size()));
        (void)cr::buildVerdictMarker(m, cr::MarkerHeader{from, through, 1, uint32_t(entries.size())}, entries);
        put(m, at, 1);
    };
    std::vector<uint8_t> change(cr::changeBatchBytes(1));
    const cr::ChangeMember cm{x.objectId, uint8_t(VerdictViewKind::TestReject), x.rule.fingerprint()};
    (void)cr::buildConstraintSetChange(change, 1, -1, std::span<const cr::ChangeMember>(&cm, 1));
    put(change, 0, 1);
    data(1, {{1, 1}});
    data(2, {{2, 9}});
    data(3, {{3, 1}});
    const cr::MarkerEntry rejected{1, kViolation, rune::view::constraintIdOf(x.objectId, 1)};   // 2, from 1
    marker(4, 1, 3, std::span<const cr::MarkerEntry>(&rejected, 1));
    data(5, {{4, 1}});
    if (markerAboveHw) marker(6, 3, 6, {});
    else data(6, {{6, 1}});
    data(7, {{5, 1}});
    rune::storage::LogCleaner cleaner;
    cleaner.setVerdictBounds(3, 5);
    cleaner.clean(log, 0, log.segmentBaseOffsets().back(), 0, 1'000'000);
    log.reloadSegments();
    std::vector<uint8_t> scratch(1 << 20);
    const auto readRange = [&](int64_t from, int64_t to) {   // the worker's readNoFault_
        rune::storage::ReadIntoStatus rs;
        return log.readInto(from, 1 << 16, scratch.data(), scratch.size(), rs, to);
    };
    ASSERT_TRUE(readRange(2, 3).empty());   // the gap: the next segment starts at the bound
    ASSERT_FALSE(readRange(2, 4).empty());

    PartitionWriteState st;
    verdictFeedBegin(&st, 0, 0);   // the new leader's scan, to its LEO
    for (int64_t o = 0; o < log.logEndOffset(); ++o) {
        const auto b = log.read(o, 1 << 20);
        if (b.empty()) continue;
        rune::protocol::RecordBatchReader r(b.data(), b.size());
        if (r.baseOffset() != o) continue;
        verdictFeedBatch(&st, b.data(), std::size_t(r.totalLength()), o);
    }
    st.scanHw = 5;
    verdictOnHw(st, 5);
    const int64_t K = markerAboveHw ? 6 : 3;
    ASSERT_EQ(st.markers.committedThrough(), 3);
    ASSERT_EQ(st.rejected.coveredThrough(), K);

    TxnOverlayPool pool;
    VerdictSource vs{5, 0};
    vs.bindTxn(&pool, {}, 0);
    vs.installTestVerdictViewForTest(x.inst.get(), x.objectId, x.rule);
    vs.bindState(&st);
    vs.bindLeader(nullptr, 1, log.logStartOffset());
    for (int i = 0; i < 6; ++i) (void)vs.driveJudge(readRange, /*bound=*/3, 0, st);
    EXPECT_EQ(vs.judgedThrough(), K);
    EXPECT_EQ(x.inst->foldCursor, K);
    EXPECT_TRUE(x.has(1));
    EXPECT_FALSE(x.has(2));
    EXPECT_EQ(x.has(3), markerAboveHw);
    EXPECT_EQ(x.has(4), markerAboveHw);
}
}  // namespace

// A new leader activates at the coverage K of a marker its scan read above its lagging HW, so judgeCursor_ > bound.  The
// gap's next segment starts at the bound: a member read that stops at the bound is empty until the LSO moves, so the
// member's catch-up must read past judgeCursor_ as it did before the bound fix.
TEST(CheckedWritesVerdictSourceRegression, MemberCatchUpReadsPastTheJudgeCursorAboveTheBoundRegression) {
    catchUpAcrossTheGapAtTheBound("cw-judge-above-bound", /*markerAboveHw=*/true);
}

// judgeCursor_ == bound == the gap's next segment base: a read to max(bound, judgeCursor_) stops at that base and is
// empty, so the member stays behind and the judge held until a write raises the bound.  The read must end past it.
TEST(CheckedWritesVerdictSourceRegression, MemberCatchUpReadsPastAJudgeCursorAtTheBoundRegression) {
    catchUpAcrossTheGapAtTheBound("cw-judge-at-bound", /*markerAboveHw=*/false);
}
