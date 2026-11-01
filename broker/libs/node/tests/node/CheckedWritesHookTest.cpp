// CheckedWritesHookTest.cpp - checked writes Part 03 Task 6: the section 11.2 requirement table (pinned rows and the
// sweep invariants) and CheckedWriteHook::check on real record bytes.
#include <rune/node/CheckedWriteHook.h>
#include <rune/node/NamespaceLimitCheck.h>

#include "support/CheckedWritesBatches.h"

#include <gtest/gtest.h>

#include <string>
#include <vector>

using namespace rune::node;
using namespace rune::node::cw;
using rune::protocol::client::ErrorCode;

namespace {

constexpr ErrorCode OK  = ErrorCode::OK;
constexpr ErrorCode IR  = ErrorCode::InvalidRequest;
using H = HookRefusal;

enum Shape { Plain, Reserved, InsertExplicit, Update, RemoveExisting, KeyZero, KeyZeroMixed, ManyPlain, ManyPlainOver };

constexpr BatchIntents intentsOf(Shape s) {
    BatchIntents b;
    b.records = 1;
    switch (s) {
    case Plain: break;
    case Reserved: b.reservedBits = true; break;
    case InsertExplicit:
    case Update:
    case RemoveExisting: b.anyIntent = b.explicitKeyIntent = true; break;
    case KeyZero: b.anyIntent = b.keyZeroCreate = true; break;
    case KeyZeroMixed: b.anyIntent = b.keyZeroCreate = b.mixedKeyZero = true; b.records = 2; break;
    case ManyPlain: b.records = 4096; break;
    case ManyPlainOver: b.records = 4097; break;
    }
    return b;
}

constexpr WriteFacts F(uint8_t acks, bool idem, bool txn, bool atomic, bool legacy, bool level) {
    WriteFacts f;
    f.acks = acks; f.idempotent = idem; f.transactional = txn; f.atomicBatch = atomic; f.legacyMode = legacy; f.levelCheckedWrites = level;
    return f;
}
constexpr TopicHookFacts T(bool keyIndex, bool autoInc, bool constrained, bool verdict) { return {keyIndex, autoInc, constrained, verdict}; }

struct Row { const char* why; Shape shape; WriteFacts f; TopicHookFacts t; ErrorCode code; H refusal; };

constexpr WriteFacts kIdemView = F(3, true, false, false, false, true);

const std::vector<Row>& rows() {
    static const std::vector<Row> r = {
        {"1 plain upsert, acks 0 (11.2 row 1)", Plain, F(0, false, false, false, false, false), T(0,0,0,0), OK, H::None},
        {"2 plain tombstone, legacy: plain writes are never refused (SD10)", Plain, F(1, false, false, false, true, false), T(0,0,0,0), OK, H::None},
        {"3 plain acks 3, 4096 records", ManyPlain, kIdemView, T(0,0,0,1), OK, H::None},
        {"4 plain acks 3, 4097 records (SD4)", ManyPlainOver, kIdemView, T(0,0,0,1), IR, H::ViewBatchTooLarge},
        {"5 plain acks 3, 4097 records, not a verdict source", ManyPlainOver, kIdemView, T(0,0,0,0), OK, H::None},
        {"6 plain acks 2 in legacy mode", Plain, F(2, false, false, false, true, true), T(0,0,0,0), IR, H::LegacyMode},
        {"7 plain acks 3 below the level", Plain, F(3, true, false, false, false, false), T(0,0,0,0), IR, H::LevelTooLow},
        {"8 reserved bits, rule 1 before every other", Reserved, F(0, false, false, false, true, false), T(0,0,0,0), IR, H::ReservedBits},
        {"9 insert explicit, no key index", InsertExplicit, kIdemView, T(0,0,0,0), IR, H::NoKeyIndex},
        {"10 insert explicit, acks 1 (11.2 row 2)", InsertExplicit, F(1, true, false, false, false, true), T(1,0,0,0), IR, H::ExplicitKeyNeedsIdempotentAndView},
        {"11 update, not idempotent", Update, F(3, false, false, false, false, true), T(1,0,0,0), IR, H::ExplicitKeyNeedsIdempotentAndView},
        {"12 removeExisting, acks 2 qualifies", RemoveExisting, F(2, true, false, false, false, true), T(1,0,0,0), OK, H::None},
        {"13 update in a transaction (11.2 row 6)", Update, F(3, true, true, false, false, true), T(1,0,0,0), IR, H::IntentInTxnOrAtomicBatch},
        {"14 plain in a transaction on a constrained source (11.2 row 5)", Plain, F(1, false, true, false, false, true), T(0,0,1,0), IR, H::TxnOnConstrainedSource},
        {"15 plain atomic batch on a constrained source", Plain, F(1, false, false, true, false, true), T(0,0,1,0), IR, H::TxnOnConstrainedSource},
        {"16 plain transaction, key index only", Plain, F(1, false, true, false, false, true), T(1,0,0,0), OK, H::None},
        {"17 key-0 creates, acks 1 (11.2 row 3)", KeyZero, F(1, true, false, false, false, true), T(1,1,0,0), OK, H::None},
        {"18 key-0 creates, acks 0", KeyZero, F(0, true, false, false, false, true), T(1,1,0,0), IR, H::KeyZeroCreateNeedsIdempotentAll},
        {"19 key-0 creates, not idempotent", KeyZero, F(1, false, false, false, false, true), T(1,1,0,0), IR, H::KeyZeroCreateNeedsIdempotentAll},
        {"20 key-0 creates on a constrained source, acks 1 (11.2 row 4)", KeyZero, F(1, true, false, false, false, true), T(1,1,1,0), IR, H::KeyZeroCreateNeedsView},
        {"21 key-0 creates on a constrained source, acks 3", KeyZero, kIdemView, T(1,1,1,0), OK, H::None},
        {"22 key-0 mixed with plain records (6.3)", KeyZeroMixed, F(1, true, false, false, false, true), T(1,1,0,0), IR, H::MixedKeyZeroBatch},
        {"23 key-0 creates in a transaction on a key-index-only topic", KeyZero, F(1, true, true, false, false, true), T(1,1,0,0), OK, H::None},
        {"24 key-0 creates in legacy mode", KeyZero, F(1, true, false, false, true, true), T(1,1,0,0), IR, H::LegacyMode},
        // each rule's boundary, one condition flipped at a time
        {"25 reserved bits with a valid everything else", Reserved, kIdemView, T(1,0,0,0), IR, H::ReservedBits},
        {"26 acks 1 in legacy mode is a plain write", Plain, F(1, true, false, false, true, true), T(0,0,0,0), OK, H::None},
        {"27 acks 3 in legacy mode", Plain, F(3, true, false, false, true, true), T(0,0,0,0), IR, H::LegacyMode},
        {"28 insert explicit in legacy mode", InsertExplicit, F(3, true, false, false, true, true), T(1,0,0,0), IR, H::LegacyMode},
        {"29 insert explicit below the level", InsertExplicit, F(3, true, false, false, false, false), T(1,0,0,0), IR, H::LevelTooLow},
        {"30 acks 2 below the level", Plain, F(2, false, false, false, false, false), T(0,0,0,0), IR, H::LevelTooLow},
        {"31 acks 0 below the level", Plain, F(0, false, false, false, false, false), T(1,0,0,0), OK, H::None},
        {"32 insert explicit with a key index, acks 3, idempotent", InsertExplicit, kIdemView, T(1,0,0,0), OK, H::None},
        {"33 update, acks 2, idempotent", Update, F(2, true, false, false, false, true), T(1,0,0,0), OK, H::None},
        {"34 update, acks 1 (All without View)", Update, F(1, true, false, false, false, true), T(1,0,0,0), IR, H::ExplicitKeyNeedsIdempotentAndView},
        {"35 update, acks 0", Update, F(0, true, false, false, false, true), T(1,0,0,0), IR, H::ExplicitKeyNeedsIdempotentAndView},
        {"36 removeExisting, not idempotent, acks 2", RemoveExisting, F(2, false, false, false, false, true), T(1,0,0,0), IR, H::ExplicitKeyNeedsIdempotentAndView},
        {"37 update in an atomic batch", Update, F(3, true, false, true, false, true), T(1,0,0,0), IR, H::IntentInTxnOrAtomicBatch},
        {"38 insert explicit in a transaction, no key index", InsertExplicit, F(3, true, true, false, false, true), T(0,0,0,0), IR, H::NoKeyIndex},
        {"39 update, transactional on a constrained source: rule 6 first", Update, F(3, true, true, false, false, true), T(1,0,1,0), IR, H::IntentInTxnOrAtomicBatch},
        {"40 plain, neither txn nor atomic, constrained source", Plain, F(1, false, false, false, false, true), T(0,0,1,0), OK, H::None},
        {"41 plain transaction, constrained source, below the level", Plain, F(1, false, true, false, false, false), T(0,0,1,0), IR, H::TxnOnConstrainedSource},
        {"42 key-0 creates, acks 2 (View without All)", KeyZero, F(2, true, false, false, false, true), T(1,1,0,0), IR, H::KeyZeroCreateNeedsIdempotentAll},
        {"43 key-0 creates, acks 3, idempotent, unconstrained", KeyZero, kIdemView, T(1,1,0,0), OK, H::None},
        {"44 key-0 creates with no key index", KeyZero, kIdemView, T(0,1,0,0), IR, H::NoKeyIndex},
        {"45 key-0 creates below the level", KeyZero, F(1, true, false, false, false, false), T(1,1,0,0), IR, H::LevelTooLow},
        {"46 key-0 mixed, not idempotent: mixed first", KeyZeroMixed, F(0, false, false, false, false, true), T(1,1,0,0), IR, H::MixedKeyZeroBatch},
        {"47 key-0 creates in an atomic batch on a constrained source", KeyZero, kIdemView, T(1,1,1,0), OK, H::None},
        {"48 key-0 creates in a transaction on a constrained source", KeyZero, F(3, true, true, false, false, true), T(1,1,1,0), IR, H::TxnOnConstrainedSource},
        {"49 4096 records, acks 3, verdict source", ManyPlain, kIdemView, T(0,0,0,1), OK, H::None},
        {"50 4097 records, acks 2, verdict source", ManyPlainOver, F(2, false, false, false, false, true), T(0,0,0,1), IR, H::ViewBatchTooLarge},
        {"51 4097 records, acks 1, verdict source", ManyPlainOver, F(1, true, false, false, false, true), T(0,0,0,1), OK, H::None},
        {"52 4097 records, acks 3, legacy mode: legacy first", ManyPlainOver, F(3, true, false, false, true, true), T(0,0,0,1), IR, H::LegacyMode},
        {"53 4097 records, acks 3 below the level", ManyPlainOver, F(3, true, false, false, false, false), T(0,0,0,1), IR, H::LevelTooLow},
        {"54 4097 records, acks 0", ManyPlainOver, F(0, false, false, false, false, true), T(0,0,0,1), OK, H::None},
        {"55 import granted changes nothing", Plain, [] { auto f = F(1, false, false, false, false, true); f.importGranted = true; return f; }(), T(0,0,0,0), OK, H::None},
        {"56 import granted, update still needs View", Update, [] { auto f = F(1, true, false, false, false, true); f.importGranted = true; return f; }(), T(1,0,0,0), IR, H::ExplicitKeyNeedsIdempotentAndView},
        {"57 auto-increment flag alone does not refuse", Plain, F(1, true, false, false, false, true), T(1,1,0,0), OK, H::None},
        {"58 verdict source flag alone does not refuse", Plain, F(1, false, false, false, false, true), T(0,0,0,1), OK, H::None},
        {"59 plain transaction on a verdict source", Plain, F(3, true, true, false, false, true), T(0,0,0,1), OK, H::None},
        {"60 plain acks 3 atomic, unconstrained", Plain, F(3, true, false, true, false, true), T(0,0,0,0), OK, H::None},
        {"61 removeExisting in an atomic batch", RemoveExisting, F(3, true, false, true, false, true), T(1,0,0,0), IR, H::IntentInTxnOrAtomicBatch},
        {"62 insert explicit, acks 3, idempotent, all topic facts", InsertExplicit, kIdemView, T(1,1,1,1), OK, H::None},
        {"63 reserved bits under legacy and no level", Reserved, F(3, true, true, true, true, false), T(1,1,1,1), IR, H::ReservedBits},
        {"64 plain acks 1 below the level", Plain, F(1, true, false, false, false, false), T(0,0,0,0), OK, H::None},
    };
    return r;
}

constexpr bool isRow(Shape s, WriteFacts f, TopicHookFacts t, ErrorCode code, H why) {
    const auto v = evaluateRequirements(intentsOf(s), f, t);
    return v.code == code && v.why == why;
}
static_assert(isRow(Plain, F(0, false, false, false, false, false), T(0,0,0,0), OK, H::None));
static_assert(isRow(ManyPlainOver, kIdemView, T(0,0,0,1), IR, H::ViewBatchTooLarge));
static_assert(isRow(Reserved, F(0, false, false, false, true, false), T(0,0,0,0), IR, H::ReservedBits));
static_assert(isRow(KeyZero, F(1, true, false, false, false, true), T(1,1,0,0), OK, H::None));
static_assert(isRow(KeyZero, F(1, true, false, false, false, true), T(1,1,1,0), IR, H::KeyZeroCreateNeedsView));

}  // namespace

TEST(CheckedWritesRequirementTable, PinnedRows) {
    ASSERT_EQ(rows().size(), 64u);
    for (const Row& r : rows()) {
        const auto v = evaluateRequirements(intentsOf(r.shape), r.f, r.t);
        EXPECT_EQ(v.code, r.code) << r.why;
        EXPECT_EQ(v.why, r.refusal) << r.why;
    }
}

TEST(CheckedWritesRequirementTable, SweepInvariants) {
    // Which refusals a fact can introduce or remove; a change of result caused by one flipped fact must involve one.
    const auto feeds = [](int fact, H h) {
        switch (fact) {
        case 0: return h == H::LegacyMode || h == H::LevelTooLow || h == H::ExplicitKeyNeedsIdempotentAndView ||
                       h == H::KeyZeroCreateNeedsIdempotentAll || h == H::KeyZeroCreateNeedsView || h == H::ViewBatchTooLarge;   // acks
        case 1: return h == H::ExplicitKeyNeedsIdempotentAndView || h == H::KeyZeroCreateNeedsIdempotentAll;                      // idempotent
        case 2: return h == H::IntentInTxnOrAtomicBatch || h == H::TxnOnConstrainedSource;                                         // transactional
        case 3: return h == H::IntentInTxnOrAtomicBatch || h == H::TxnOnConstrainedSource;                                         // atomic
        case 4: return false;                                                                                                      // import
        case 5: return h == H::LegacyMode;                                                                                         // legacy
        case 6: return h == H::LevelTooLow;                                                                                        // level
        case 7: return h == H::NoKeyIndex;                                                                                         // keyIndex
        case 8: return false;                                                                                                      // autoIncrement
        case 9: return h == H::TxnOnConstrainedSource || h == H::KeyZeroCreateNeedsView;                                           // constrained
        default: return h == H::ViewBatchTooLarge;                                                                                 // verdict source
        }
    };
    const auto build = [](uint32_t m, WriteFacts& f, TopicHookFacts& t) {
        f = {};
        f.acks = m & 3; f.idempotent = m & 4; f.transactional = m & 8; f.atomicBatch = m & 16; f.importGranted = m & 32;
        f.legacyMode = m & 64; f.levelCheckedWrites = m & 128;
        t = {};
        t.keyIndex = m & 256; t.autoIncrement = m & 512; t.constrainedSource = m & 1024; t.verdictSource = m & 2048;
    };
    // Matrix B: every intent shape x reserved bits on / off x records {1, 4096, 4097} x the 12 facts (import included).
    const Shape shapes[] = {Plain, InsertExplicit, Update, RemoveExisting, KeyZero, KeyZeroMixed};
    const auto flip = [](uint32_t m, int fact) {
        switch (fact) {
        case 0: return m ^ 1u;
        case 1: return m ^ 4u;
        case 2: return m ^ 8u;
        case 3: return m ^ 16u;
        case 4: return m ^ 32u;
        case 5: return m ^ 64u;
        case 6: return m ^ 128u;
        case 7: return m ^ 256u;
        case 8: return m ^ 512u;
        case 9: return m ^ 1024u;
        default: return m ^ 2048u;
        }
    };
    int cells = 0;
    for (Shape s : shapes)
    for (const bool reserved : {false, true})
    for (const int32_t records : {1, 4096, 4097}) {
        BatchIntents b = intentsOf(s);
        b.reservedBits = reserved;
        b.records = records;
        for (uint32_t m = 0; m < 4096; ++m) {
            WriteFacts f; TopicHookFacts t;
            build(m, f, t);
            const auto v = evaluateRequirements(b, f, t);
            ++cells;
            const bool view = f.acks & 2;
            if (!b.anyIntent && !b.reservedBits && !view && !((f.transactional || f.atomicBatch) && t.constrainedSource)) {
                ASSERT_EQ(v.code, OK) << "(i) shape " << s << " records " << records << " m " << m;
            }
            if (b.reservedBits) ASSERT_EQ(v.why, H::ReservedBits) << "(ii)";
            if (!b.reservedBits && f.legacyMode && (b.anyIntent || view)) ASSERT_EQ(v.why, H::LegacyMode) << "(iii)";
            if (v.code != OK) ASSERT_EQ(v.code, IR) << "(iv)";
            for (int fact = 0; fact < 11; ++fact) {
                WriteFacts f2; TopicHookFacts t2;
                build(flip(m, fact), f2, t2);
                const auto w = evaluateRequirements(b, f2, t2);
                if (w.why != v.why) ASSERT_TRUE(feeds(fact, v.why) || feeds(fact, w.why)) << "(v) fact " << fact << " shape " << s << " reserved " << reserved << " records " << records << " m " << m;
            }
        }
    }
    EXPECT_EQ(cells, 6 * 2 * 3 * 4096);
}

namespace {

struct Hooked {
    TopicWriteSpec       spec;
    PartitionWriteState  state;
    WriteFacts           facts;
    explicit Hooked(Entity e = Entity::TypedLegacy, bool level = true) {
        specFor(e, spec);
        state.spec = &spec;
        facts.levelCheckedWrites = level;
        facts.idempotent = true;
        facts.acks = level ? 3 : 1;
    }
    ErrorCode run(const std::vector<uint8_t>& batch, HookRefusal* why = nullptr, int32_t count = -1) {
        CheckedWriteHook hook(state, facts, {});
        int32_t n = count;
        if (n < 0) {
            n = 0;
            (void)rune::protocol::forEachRecordSpan(batch.data(), batch.size(), [&](const rune::protocol::RecordSpan&) { ++n; });
        }
        const ErrorCode e = hook.check(batch.data(), batch.size(), n);
        if (why) *why = hook.refusal();
        EXPECT_TRUE(hook.checked());
        return e;
    }
};

std::vector<uint8_t> withAttrs(const std::vector<std::pair<TypedRecord, uint8_t>>& records) {
    rune::protocol::RecordBatchBuilder b(0, 1'000'000);
    for (const auto& [r, a] : records) b.append(r.key, r.value, 1'000'000, a);
    return b.build();
}

}  // namespace

TEST(CheckedWritesHook, ValidUpsertBatchPasses) {
    Hooked h;
    HookRefusal why{};
    EXPECT_EQ(h.run(batchOf({validRecord(1), validRecord(2), validRecord(3)}), &why), OK);
    EXPECT_EQ(why, H::None);
}

TEST(CheckedWritesHook, UnknownVersionIsSchemaValidationFailed) {
    Hooked h;
    h.spec.installVersion(3, matrixDescriptor(3));
    HookRefusal why{};
    EXPECT_EQ(h.run(batchOf({validRecord(1, 0)}), &why), ErrorCode::SchemaValidationFailed);
    EXPECT_EQ(why, H::ValidationFailed);
    EXPECT_EQ(h.run(batchOf({validRecord(1, 2)}), &why), ErrorCode::SchemaValidationFailed) << "below the highest installed, no entry";
    EXPECT_EQ(why, H::ValidationFailed);
}

TEST(CheckedWritesHookRegression, VersionAboveTheInstalledMaxIsRetriableNotLeaderRegression) {
    Hooked h;
    HookRefusal why{};
    const auto batch = batchOf({validRecord(1, 2)});
    EXPECT_EQ(h.run(batch, &why), ErrorCode::NotLeader);
    EXPECT_EQ(why, H::VersionAhead);
    h.spec.installVersion(2, matrixDescriptor(2));
    EXPECT_EQ(h.run(batch, &why), OK);
    EXPECT_EQ(why, H::None);
}

TEST(CheckedWritesHook, WireKeyMismatchIsSchemaValidationFailed) {
    Hooked h;
    Parts p;
    p.key = leBytes(99, 8);
    HookRefusal why{};
    EXPECT_EQ(h.run(batchOf({encode(p)}), &why), ErrorCode::SchemaValidationFailed);
    EXPECT_EQ(why, H::ValidationFailed);
}

TEST(CheckedWritesHook, TombstoneSkipsValueValidation) {
    Hooked h;
    Parts p;
    p.tombstone = true;
    EXPECT_EQ(h.run(batchOf({encode(p)})), OK);
    p.key = std::string(7, 'k');
    EXPECT_EQ(h.run(batchOf({encode(p)})), ErrorCode::SchemaValidationFailed);
    Hooked keyless(Entity::TypedKeyless);
    Parts q;
    q.tombstone = true;
    EXPECT_EQ(keyless.run(batchOf({encode(q)})), ErrorCode::SchemaValidationFailed);
}

TEST(CheckedWritesHookRegression, BelowTheLevelKeepsPartTwosLegacyRuleRegression) {
    Parts bad;
    bad.bio = "\xff\xfe";
    {
        Hooked pre(Entity::TypedPreUpgrade, false);
        EXPECT_EQ(pre.run(batchOf({encode(bad)})), OK) << "no validate flag, below the level: not checked";
    }
    Hooked legacy(Entity::TypedLegacy, false);
    EXPECT_EQ(legacy.run(batchOf({validRecord(1)})), OK);
    EXPECT_EQ(legacy.run(batchOf({validRecord(1, 0)})), ErrorCode::SchemaValidationFailed);
}

TEST(CheckedWritesHook, ReservedAttributeBitIsInvalidRequest) {
    Hooked h;
    HookRefusal why{};
    for (uint8_t attrs : {uint8_t{0x04}, uint8_t{0x03}}) {
        EXPECT_EQ(h.run(withAttrs({{validRecord(1), 0}, {validRecord(2), attrs}}), &why), IR) << int(attrs);
        EXPECT_EQ(why, H::ReservedBits);
    }
}

TEST(CheckedWritesHookRegression, InvalidRecordBeforeAReservedBitAnswersSchemaValidationFailedRegression) {
    Hooked h;
    EXPECT_EQ(h.run(withAttrs({{validRecord(1, 0), 0}, {validRecord(2), 0x04}})), ErrorCode::SchemaValidationFailed);
    EXPECT_EQ(h.run(withAttrs({{validRecord(2), 0x04}, {validRecord(1, 0), 0}})), IR);
}

TEST(CheckedWritesHook, AnyIntentIsRefusedUntilAKeyIndexExists) {
    Hooked h;
    HookRefusal why{};
    EXPECT_EQ(h.run(withAttrs({{validRecord(1), 1}}), &why), IR);
    EXPECT_EQ(why, H::NoKeyIndex);
}

TEST(CheckedWritesHook, StageIsIdentityAndAssignedBaseIsZero) {
    Hooked h;
    CheckedWriteHook hook(h.state, h.facts, {});
    const uint8_t bytes[4] = {1, 2, 3, 4};
    int log = 0;
    const auto s = hook.stage(log, bytes, sizeof bytes);
    EXPECT_EQ(s.error, OK);
    EXPECT_EQ(s.batch, bytes);
    EXPECT_EQ(s.len, sizeof bytes);
    EXPECT_EQ(hook.assignedBase(), 0u);
    EXPECT_FALSE(hook.checked());
}

// RB5 (SD12): a namespace whose lowest acks is All admits only frames with the All bit.  Wire 2 (Leader | View) waits for
// the commit but is not All, so it must not pass; 3 (All | View) does.  RED: `wireAcks >= 1` (the pre-checked-writes
// test) lets wire 2 through.
TEST(CheckedWritesNamespaceRegression, LeaderViewNeverPassesANamespaceThatRequiresAllRegression) {
    rune::protocol::NamespaceLimits all;
    all.lowestAcks = 2;
    EXPECT_FALSE(nsAcksAllowed(all, 0));
    EXPECT_TRUE(nsAcksAllowed(all, 1));
    EXPECT_FALSE(nsAcksAllowed(all, 2)) << "Leader | View is not All";
    EXPECT_TRUE(nsAcksAllowed(all, 3));
    rune::protocol::NamespaceLimits leader;
    leader.lowestAcks = 1;
    for (const uint8_t wire : {uint8_t{0}, uint8_t{1}, uint8_t{2}, uint8_t{3}}) EXPECT_TRUE(nsAcksAllowed(leader, wire)) << int(wire);
}
