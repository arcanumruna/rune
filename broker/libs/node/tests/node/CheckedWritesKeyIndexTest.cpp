// Part 05 Task 6: the key index as a verdict member (unit level: no worker, no replica).
#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <random>
#include <set>
#include <string>
#include <vector>

#include <rune/codec/Descriptor.h>
#include <rune/node/VerdictSource.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/storage/RejectedOffsetIndex.h>
#include <rune/transaction/AbortedTxnIndex.h>
#include <rune/view/PlanCompiler.h>

using namespace rune::node;
namespace cr = rune::storage::control_record;
using P = rune::codec::PrimType;
using rune::view::VerdictViewKind;

namespace {

constexpr uint16_t kExists = 56, kNotFound = 57, kViolation = 55;
constexpr uint8_t kUpsert = 0, kInsert = 1, kExisting = 2;

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

struct Rec {
    int64_t id;
    int32_t tag = 1;
    bool    tombstone = false;
    uint8_t intent = kUpsert;
    size_t  keyBytes = 8;
};
Rec put(int64_t id, uint8_t intent = kUpsert, int32_t tag = 1) { return {id, tag, false, intent}; }
Rec del(int64_t id, uint8_t intent = kUpsert) { return {id, 1, true, intent}; }

// A committed log of whole batches; `read(from)` starts at the batch holding `from`.
struct Log {
    std::vector<uint8_t> bytes;
    struct Span { std::size_t pos; int64_t base, last; };
    std::vector<Span>    batches;
    int64_t              next = 0;

    void data(const std::vector<Rec>& recs, int64_t abortedPid = -1) {
        rune::protocol::RecordBatchBuilder b(next, 0);
        if (abortedPid >= 0) {
            b.setTransactional(true);
            b.setProducerId(abortedPid);
        }
        for (const Rec& r : recs) {
            std::vector<uint8_t> k, v;
            le64(k, uint64_t(r.id));
            k.resize(r.keyBytes);
            if (!r.tombstone) { le16(v, 12); le16(v, 1); le64(v, uint64_t(r.id)); le32(v, uint32_t(r.tag)); }
            b.append(sv(k), sv(v), 0, r.intent);
        }
        push(b.build(), int64_t(recs.size()));
    }
    void control() {
        std::vector<uint8_t> c(cr::markerBatchBytes(0));
        (void)cr::buildVerdictMarker(c, cr::MarkerHeader{0, 0, 0, 0}, {});
        for (int i = 0; i < 8; ++i) c[std::size_t(i)] = uint8_t(uint64_t(next) >> (8 * i));
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

TestRejectRule tagEquals(uint8_t tag) { TestRejectRule r; r.mode = TestRejectRule::Mode::TagEquals; r.tag = tag; return r; }
TestRejectRule capLive(uint32_t cap) { TestRejectRule r; r.mode = TestRejectRule::Mode::CapLiveKeys; r.cap = cap; return r; }

struct Rig {
    rune::codec::Descriptor src = tagSource();
    Log log;
    PartitionWriteState st;
    VerdictSource vs{5, 0};
    std::unique_ptr<ViewInstance> index;
    std::unique_ptr<ViewInstance> reject;
    rune::view::CompileResult comp;
    ConstraintSet set;

    explicit Rig(bool withIndex = true) {
        set.id = 1;
        if (!withIndex) return;
        index = std::make_unique<ViewInstance>(keyIndexPlan(5, src.find("id")->id), keyIndexRowLayout(), src, false, 0, 4096, kKeyIndexChunkBytes);
        vs.addVerdictView(index.get(), VerdictViewKind::KeyIndex, 0, kKeyIndexPlanFingerprint);
        set.members[set.count++] = {0, VerdictViewKind::KeyIndex, kKeyIndexPlanFingerprint};
    }
    void withReject(TestRejectRule rule) {
        const rune::codec::Descriptor* s[] = {&src};
        comp   = rune::view::compileView(tagView(), s, 7).value();
        reject = std::make_unique<ViewInstance>(comp.plan, comp.rowLayout, src, false, 0, 256, 1u << 16);
        vs.installTestVerdictViewForTest(reject.get(), 7, rule);
        set.members[set.count++] = {7, VerdictViewKind::TestReject, rule.fingerprint()};
    }
    void start() {
        st.constraintSets.onChange(0, set);
        log.control();
        vs.bindState(&st);
        vs.bindLeader(nullptr, 1, 0);
        drive();
    }
    int drive() {
        return vs.driveJudge([&](int64_t from, int64_t) { return log.read(from); }, log.next, 0, st);
    }
    [[nodiscard]] bool contains(int64_t id) const { return index->table.get(uint64_t(id)) != nullptr; }
    [[nodiscard]] std::pair<uint16_t, uint32_t> verdictAt(int64_t offset) const {
        const auto* e = st.rejected.find(offset);
        return e == nullptr ? std::pair<uint16_t, uint32_t>{0, 0} : std::pair<uint16_t, uint32_t>{e->code, e->constraintId};
    }
};

using V = std::pair<uint16_t, uint32_t>;

struct Cell { const char* name; bool tombstone; uint8_t intent; bool exists; uint16_t code; };

}  // namespace

class CheckedWritesKeyIndexIntent : public ::testing::TestWithParam<Cell> {};

TEST_P(CheckedWritesKeyIndexIntent, IntentTable) {
    const Cell c = GetParam();
    Rig r;
    r.start();
    int64_t offset = r.log.next;
    const std::string key("\x05\0\0\0\0\0\0\0", 8);
    if (c.exists) EXPECT_EQ(r.vs.judge({key, "v", offset++, kUpsert, nullptr}).code, 0);
    ASSERT_EQ(r.index->table.size(), c.exists ? 1u : 0u);
    const auto v = r.vs.judge({key, c.tombstone ? "" : "v", offset, c.intent, nullptr});
    EXPECT_EQ(v.code, c.code);
    EXPECT_EQ(v.constraintId, 0u);
    const size_t before = c.exists ? 1u : 0u;
    EXPECT_EQ(r.index->table.size(), v.code != 0 ? before : (c.tombstone ? 0u : 1u));
}

INSTANTIATE_TEST_SUITE_P(CheckedWritesKeyIndex, CheckedWritesKeyIndexIntent, ::testing::Values(
    Cell{"UpsertExists", false, kUpsert, true, 0},   Cell{"UpsertAbsent", false, kUpsert, false, 0},
    Cell{"InsertExists", false, kInsert, true, kExists}, Cell{"InsertAbsent", false, kInsert, false, 0},
    Cell{"UpdateExists", false, kExisting, true, 0}, Cell{"UpdateAbsent", false, kExisting, false, kNotFound},
    Cell{"RemoveExists", true, kUpsert, true, 0},    Cell{"RemoveAbsent", true, kUpsert, false, 0},
    Cell{"RemoveExistingExists", true, kExisting, true, 0}, Cell{"RemoveExistingAbsent", true, kExisting, false, kNotFound}),
    [](const auto& i) { return std::string(i.param.name); });

TEST(CheckedWritesKeyIndex, MixedIntentAndPlainRecordsInOneBatchJudgeInOrder) {
    Rig r;
    r.log.control();
    r.log.data({put(5, kInsert), put(5), put(5, kInsert), del(5), del(5, kExisting), put(5, kExisting)});
    r.start();
    const int64_t o = 1;   // the batch sits at offsets 1..6
    EXPECT_EQ(r.verdictAt(o), V(0, 0));
    EXPECT_EQ(r.verdictAt(o + 1), V(0, 0));
    EXPECT_EQ(r.verdictAt(o + 2), V(kExists, 0));
    EXPECT_EQ(r.verdictAt(o + 3), V(0, 0));
    EXPECT_EQ(r.verdictAt(o + 4), V(kNotFound, 0));
    EXPECT_EQ(r.verdictAt(o + 5), V(kNotFound, 0));
    EXPECT_FALSE(r.contains(5));
    EXPECT_EQ(r.vs.stats().rejectionsKeyExists, 1u);
    EXPECT_EQ(r.vs.stats().rejectionsKeyNotFound, 2u);
    EXPECT_EQ(r.vs.stats().rejectionsConstraint, 0u);
}

TEST(CheckedWritesKeyIndex, KeyIndexIsFirstInSetOrder) {
    Rig r;
    r.withReject(tagEquals(9));
    r.log.control();
    r.log.data({put(5)});                                           // offset 1
    r.log.data({put(5, kInsert, 9), put(6, kInsert, 9), put(6, kExisting, 1)});   // 2, 3, 4
    r.start();
    EXPECT_EQ(r.verdictAt(1), V(0, 0));
    EXPECT_EQ(r.verdictAt(2), V(kExists, 0));
    EXPECT_EQ(r.verdictAt(3), V(kViolation, 0x00070001u));
    EXPECT_FALSE(r.contains(6));
    EXPECT_EQ(r.verdictAt(4), V(kNotFound, 0));
}

TEST(CheckedWritesKeyIndex, TombstonesAreNeverRejectedExceptRemoveExistingOnAbsent) {
    Rig r;
    r.withReject(capLive(0));
    r.log.control();
    r.log.data({del(5), del(6, kExisting)});   // 1, 2
    r.start();
    EXPECT_EQ(r.verdictAt(1), V(0, 0));
    EXPECT_EQ(r.verdictAt(2), V(kNotFound, 0));
    EXPECT_EQ(r.index->table.size(), 0u);
    EXPECT_EQ(r.vs.stats().rejectionsConstraint, 0u);
}

namespace {


}  // namespace

TEST(CheckedWritesKeyIndex, FoldReplaysUnjudgedAndSkips) {
    Log log;
    log.data({put(1), put(2), put(3)});                     // 0..2
    log.data({put(4)});                                     // 3 (rejected)
    log.data({put(20), put(21)}, 77);                       // 4..5 (aborted transaction)
    log.control();                                          // 6
    log.data({put(6)});                                     // 7 (rejected)
    log.data({put(7), del(2), put(8), del(1)});             // 8..11
    ASSERT_EQ(log.next, 12);
    rune::storage::RejectedOffsetIndex rejected;
    rejected.reserve(16);
    ASSERT_TRUE(rejected.add(3, kViolation, 0));
    ASSERT_TRUE(rejected.add(7, kViolation, 0));
    rune::transaction::AbortedTxnIndex aborted;
    aborted.onAbortMarker(/*pid*/ 77, /*firstDataOffset*/ 4, /*abortMarkerOffset*/ 6);
    const rune::view::FoldSkip skip{.aborted = {&aborted}, .rejected = &rejected};
    const std::set<int64_t> live{3, 7, 8};
    const auto liveSet = [&](rune::view::ViewTable& t) {
        std::set<int64_t> out;
        for (int64_t id : {1, 2, 3, 4, 6, 7, 8, 20, 21})
            if (t.get(uint64_t(id)) != nullptr) out.insert(id);
        return out;
    };

    rune::view::ViewTable whole(keyIndexRowLayout(), 4096, kKeyIndexChunkBytes);
    KeyIndexFold wf{whole, 8};
    int64_t wc = 0;
    const auto st = foldKeyIndexBatch(wf, log.read(0), 12, wc, skip);
    EXPECT_EQ(liveSet(whole), live);
    EXPECT_EQ(st.skippedRejected, 2u);
    EXPECT_EQ(st.skippedAborted, 1u);
    EXPECT_EQ(st.skippedControl, 1u);
    EXPECT_EQ(wc, 12);

    rune::view::ViewTable stepped(keyIndexRowLayout(), 4096, kKeyIndexChunkBytes);
    KeyIndexFold sf{stepped, 8};
    int64_t cursor = 0;
    for (int64_t bound = 1; bound <= 12; ++bound) {
        (void)foldKeyIndexBatch(sf, log.read(cursor), bound, cursor, skip);
        EXPECT_LE(cursor, bound);
    }
    EXPECT_EQ(cursor, 12);
    EXPECT_EQ(liveSet(stepped), live);

    // A torn final batch (the one at 8..11): the cursor stops after the last whole batch (offset 8) and nothing of it is applied.
    rune::view::ViewTable torn(keyIndexRowLayout(), 4096, kKeyIndexChunkBytes);
    KeyIndexFold tf{torn, 8};
    int64_t tornCursor = 0;
    auto view = log.read(0);
    view.len -= 5;
    (void)foldKeyIndexBatch(tf, view, 12, tornCursor, skip);
    EXPECT_EQ(tornCursor, 8);
    EXPECT_EQ(liveSet(torn), (std::set<int64_t>{1, 2, 3}));
}

TEST(CheckedWritesTxnOverlay, ChainedKeyIndexInsertThenUpdate) {
    rune::view::ViewTable table(keyIndexRowLayout(), 4096, kKeyIndexChunkBytes);
    rune::view::Prospect p;
    p.reserve(keyIndexRowLayout(), nullptr);
    rune::view::TxnOverlay chain;
    chain.reserve(8, 1);
    const std::string k(8, '\x07'), v(4, 'x');
    const KeyIndexFold f{table, 8};
    const auto prepare = [&](uint8_t intent, rune::view::TxnOverlay* ch, const rune::view::SourceRecord& base) {
        rune::view::SourceRecord rec = base;
        rec.intent = intent;
        return prepareKeyIndex(f, rec, p, ch);
    };
    const rune::view::SourceRecord base{k, v, 0, 0, nullptr};

    ASSERT_EQ(prepare(kInsert, &chain, base), rune::view::PrepareStatus::Ok);
    EXPECT_FALSE(p.rows[0].existed);
    EXPECT_TRUE(evaluateIntent(base, p).accepted());
    ASSERT_EQ(prepare(kExisting, &chain, base), rune::view::PrepareStatus::Ok);   // the key exists in the chain
    EXPECT_TRUE(p.rows[0].existed);
    {
        rune::view::SourceRecord rec = base;
        rec.intent = kExisting;
        EXPECT_TRUE(evaluateIntent(rec, p).accepted());
    }
    EXPECT_EQ(table.size(), 0u);

    // Without the chain the key is absent; a second insert in the chain is KeyExists.
    ASSERT_EQ(prepare(kExisting, nullptr, base), rune::view::PrepareStatus::Ok);
    EXPECT_FALSE(p.rows[0].existed);
    ASSERT_EQ(prepare(kInsert, &chain, base), rune::view::PrepareStatus::Ok);
    EXPECT_TRUE(p.rows[0].existed);

    // A held overlay locks the key for everyone else.
    rune::view::TxnLockSet locks;
    locks.locked[0] = &chain;
    locks.count = 1;
    const KeyIndexFold locked{table, 8, 0, &locks};
    EXPECT_EQ(prepareKeyIndex(locked, base, p), rune::view::PrepareStatus::Locked);
    // A chained delete of an absent key locks it too, so an outside insert cannot overtake the transaction's delete.
    const std::string gone(8, '\x09');
    ASSERT_EQ(prepareKeyIndex(f, {gone, {}, 0, 0, nullptr}, p, &chain), rune::view::PrepareStatus::Ok);
    EXPECT_EQ(prepareKeyIndex(locked, {gone, v, 0, 0, nullptr}, p), rune::view::PrepareStatus::Locked);
    const std::string other(8, '\x08');
    EXPECT_EQ(prepareKeyIndex(locked, {other, v, 0, 0, nullptr}, p), rune::view::PrepareStatus::Ok);
}

TEST(CheckedWritesKeyIndex, TombstoneIsNeverRejectedByAnOffsetRule) {
    Rig r(false);
    TestRejectRule rule;
    rule.mode = TestRejectRule::Mode::OffsetSet;
    rule.offsets[0] = 1;
    rule.offsets[1] = 2;
    rule.offsetCount = 2;
    r.withReject(rule);
    r.log.control();
    r.log.data({put(1), del(1)});   // 1 (live, listed), 2 (tombstone, listed)
    r.start();
    EXPECT_EQ(r.verdictAt(1), V(kViolation, rune::view::constraintIdOf(7, 1)));
    EXPECT_EQ(r.verdictAt(2), V(0, 0));
}

TEST(CheckedWritesKeyIndex, ActivationPendingUntilTheFirstDrive) {
    Rig r;
    r.st.constraintSets.onChange(0, r.set);
    r.log.control();
    r.vs.bindState(&r.st);
    EXPECT_FALSE(r.vs.activationPending());
    r.vs.bindLeader(nullptr, 1, 0);
    EXPECT_TRUE(r.vs.activationPending());
    r.drive();
    EXPECT_FALSE(r.vs.activationPending());
}

TEST(CheckedWritesKeyIndex, AMemberWithoutAFixedKeyOfOneToSixteenBytesIsRefused) {
    const auto build = [](rune::codec::Descriptor src, uint16_t keyField) {
        ViewInstance inst(keyIndexPlan(5, keyField), keyIndexRowLayout(), std::move(src), false, 0, 4096, kKeyIndexChunkBytes);
        VerdictSource vs(5, 0);
        vs.addVerdictView(&inst, VerdictViewKind::KeyIndex, 0, kKeyIndexPlanFingerprint);
    };
    EXPECT_NO_THROW(build(tagSource(), 0));
    EXPECT_THROW(build(tagSource(), 99), std::invalid_argument);   // no such field
    const auto wide = rune::codec::DescriptorBuilder("w", 1).addFixedStr("k", 16, 1, true).build();
    EXPECT_NO_THROW(build(wide, 0));
    const auto tooWide = rune::codec::DescriptorBuilder("w", 1).addFixedStr("k", 17, 1, true).build();
    EXPECT_THROW(build(tooWide, 0), std::invalid_argument);
    const auto text = rune::codec::DescriptorBuilder("t", 1).add("k", P::String, 1, true).build();
    EXPECT_THROW(build(text, 0), std::invalid_argument);
}

TEST(CheckedWritesKeyIndex, ReplayOfAMalformedRecordDoesNotCountItAgain) {
    Rig r;
    Rec bad = put(5);
    bad.keyBytes = 7;
    r.log.control();
    r.log.data({bad, put(6)});
    r.start();
    ASSERT_EQ(r.vs.stats().keyIndexSkippedMalformed, 1u);
    r.index->foldCursor = 0;   // a rebuild: the member replays the log
    r.drive();
    EXPECT_EQ(r.index->foldCursor, r.vs.judgedThrough());
    EXPECT_EQ(r.vs.stats().keyIndexSkippedMalformed, 1u);
    EXPECT_TRUE(r.contains(6));
}

TEST(CheckedWritesKeyIndex, WordPacksEveryWidthInjectively) {
    EXPECT_EQ(keyIndexWord(std::string_view("\x01\x02", 2)), 0x0201u);
    std::mt19937_64 rng(5);
    for (size_t width : {1, 2, 3, 4, 5, 6, 7, 8}) {
        const size_t want = width == 1 ? 256 : 10000;
        std::set<std::string> keys;
        std::set<uint64_t> words;
        while (keys.size() < want) {
            std::string k(width, '\0');
            for (char& c : k) c = char(rng());
            if (keys.insert(k).second) words.insert(keyIndexWord(k));
        }
        EXPECT_EQ(words.size(), keys.size()) << "width " << width;
    }
}

TEST(CheckedWritesKeyIndex, MalformedKeyAppliesNothingAndCounts) {
    Rig r;
    Rec bad = put(5);
    bad.keyBytes = 7;
    r.log.control();
    r.log.data({bad});
    r.start();
    EXPECT_EQ(r.verdictAt(1), V(0, 0));
    EXPECT_EQ(r.index->table.size(), 0u);
    EXPECT_EQ(r.vs.stats().keyIndexSkippedMalformed, 1u);
}

TEST(CheckedWritesKeyIndex, DirtyTrackingNeverGrows) {
    Rig r;
    r.start();
    std::mt19937_64 rng(11);
    for (int pass = 0; pass < 100; ++pass) {
        std::vector<Rec> recs;
        for (int i = 0; i < 100; ++i) {
            const int64_t id = int64_t(rng() % 100);
            const unsigned pick = unsigned(rng() % 4);
            recs.push_back(pick == 0 ? del(id) : put(id, pick == 1 ? kInsert : kUpsert));
        }
        r.log.data(recs);
        r.drive();
        EXPECT_EQ(r.index->table.dirtyRids().size(), 0u) << "pass " << pass;
        EXPECT_EQ(r.index->table.pendingDeleteCount(), 0u) << "pass " << pass;
    }
    EXPECT_EQ(r.vs.judgedThrough(), r.log.next);
}

TEST(CheckedWritesKeyIndexRegression, TestRejectVerdictsAreUnchangedByTheRestructureRegression) {
    Rig r(false);
    r.withReject(tagEquals(9));
    r.log.control();
    r.log.data({put(1, kUpsert, 9), del(1), put(2, kUpsert, 1)});   // 1..3
    r.start();
    EXPECT_EQ(r.verdictAt(1), V(kViolation, rune::view::constraintIdOf(7, 1)));
    EXPECT_EQ(r.verdictAt(2), V(0, 0));
    EXPECT_EQ(r.verdictAt(3), V(0, 0));
    EXPECT_EQ(r.vs.stats().rejectionsConstraint, 1u);
    EXPECT_EQ(r.vs.stats().rejectionsKeyExists + r.vs.stats().rejectionsKeyNotFound, 0u);
}
