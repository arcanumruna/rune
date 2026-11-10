// CheckedWritesAutoIncHookTest.cpp - checked writes Part 06 Task 8: autoincrement in the write hook (check, the
// reservation record, the id patch on a scratch copy, the assigned-base window) over an InMemoryLog.
#include <rune/node/CheckedWriteHook.h>
#include <rune/node/VerdictSource.h>
#include <rune/storage/ControlRecordLayout.h>

#include <gtest/gtest.h>

#include <cstring>
#include <optional>
#include <stdexcept>
#include <string>
#include <vector>

using namespace rune::node;
using rune::codec::PrimType;
using rune::protocol::client::ErrorCode;
namespace cr = rune::storage::control_record;

namespace {

constexpr uint64_t kMax53 = (uint64_t{1} << 53) - 1;

std::string le(uint64_t v, std::size_t n) {
    std::string s(n, '\0');
    for (std::size_t i = 0; i < n; ++i) s[i] = static_cast<char>((v >> (8 * i)) & 0xFF);
    return s;
}

/// {id key, v int64, tag uint16}: the value is [blockLength u16][version u16][id][v][tag].
std::string valueOf(uint64_t id, std::size_t keyWidth) {
    return le(keyWidth + 10, 2) + le(1, 2) + le(id, keyWidth) + le(7, 8) + le(3, 2);
}

struct Rec {
    uint64_t id;
    uint8_t  attrs = 1;
    std::optional<uint64_t> valueId = std::nullopt;
};

/// An InMemoryLog that can be told to throw on its next append.
struct TestLog {
    rune::storage::InMemoryLog log{1 << 20, 256};
    bool                       failNext = false;
    int64_t logEndOffset() const noexcept { return log.logEndOffset(); }
    int64_t appendNoSyncRestamp(const uint8_t* b, std::size_t len, int32_t lastOffsetDelta, int64_t& base) {
        if (failNext) { failNext = false; throw std::runtime_error("append"); }
        return log.appendNoSyncRestamp(b, len, lastOffsetDelta, base);
    }
};

ConstraintSet keyIndexSet() {
    ConstraintSet set;
    set.id = 1;
    set.count = 1;
    set.members[0] = ConstraintSetMember{0, rune::view::VerdictViewKind::KeyIndex, kKeyIndexPlanFingerprint};
    return set;
}

struct Outcome {
    ErrorCode    code = ErrorCode::OK;
    HookRefusal  why  = HookRefusal::None;
    uint64_t     assignedBase = 0;
    int64_t      base = -1;
};

struct Rig {
    TopicWriteSpec       spec;
    PartitionWriteState  state;
    WriteFacts           facts;
    std::vector<uint8_t> scratch = std::vector<uint8_t>(64 * 1024);
    TestLog              tlog;
    PrimType             keyType;
    std::size_t          keyWidth;

    explicit Rig(PrimType type = PrimType::UInt64, int32_t partitions = 1, int32_t partition = 0, uint64_t max = kMax53,
                 uint32_t reserve = 4096, bool autoIncrement = true)
        : keyType(type), keyWidth(rune::codec::fixedSize(type)) {
        spec.installVersion(1, rune::codec::DescriptorBuilder("a", 1).add("id", type).add("v", PrimType::Int64).add("tag", PrimType::UInt16).build());
        TopicWriteSpecWire w;
        w.topicId        = 1;
        w.keyFieldId     = 0;
        w.keyType        = type;
        w.keyWidth       = static_cast<uint8_t>(keyWidth);
        w.keyIndex       = true;
        w.autoIncrement  = autoIncrement;
        w.autoIncrementMax = effectiveAutoIncMax(type, max);
        w.autoIncrementReserve = reserve;
        w.partitionCount = partitions;
        spec.setFacts(w);
        state.spec = &spec;
        state.constraintSets.onChange(7, keyIndexSet());
        state.assignedBases.reserve(64);
        state.autoInc.configure(partition, partitions, spec.autoIncrementMax, reserve);
        state.autoInc.beginScan();
        (void)state.autoInc.finishScan();
        facts.acks = 3;
        facts.idempotent = true;
        facts.levelCheckedWrites = true;
        facts.importGranted = true;
    }

    std::vector<uint8_t> batchOf(const std::vector<Rec>& recs) const {
        rune::protocol::RecordBatchBuilder b(0, 1'000'000);
        for (const Rec& r : recs) b.append(le(r.id, keyWidth), valueOf(r.valueId.value_or(r.id), keyWidth), 1'000'000, r.attrs);
        return b.build();
    }
    std::vector<uint8_t> creates(int n) const { return batchOf(std::vector<Rec>(static_cast<std::size_t>(n), Rec{0})); }

    Outcome write(const std::vector<uint8_t>& batch, int64_t pid = -1) {
        facts.producerId = pid;
        CheckedWriteHook hook(state, facts, scratch);
        int32_t n = 0;
        (void)rune::protocol::forEachRecordSpan(batch.data(), batch.size(), [&](const rune::protocol::RecordSpan&) { ++n; });
        Outcome o;
        o.code = hook.check(batch.data(), batch.size(), n);
        o.why = hook.refusal();
        if (o.code != ErrorCode::OK) return o;
        const auto staged = hook.stage(tlog, batch.data(), batch.size());
        o.code = staged.error;
        if (o.code != ErrorCode::OK) return o;
        if (tlog.appendNoSyncRestamp(staged.batch, staged.len, n - 1, o.base) < 0) { o.code = ErrorCode::TransientLogFull; return o; }
        hook.onAppended(o.base, o.base + n - 1);
        o.assignedBase = hook.assignedBase();
        return o;
    }

    /// The first batch stored at or after `offset`.
    std::pair<const uint8_t*, std::size_t> batchAt(int64_t offset) {
        const auto view = tlog.log.readView(offset, 1 << 20);
        return {view.data, static_cast<std::size_t>(rune::protocol::RecordBatchReader(view.data, view.len).totalLength())};
    }
    /// The stored keys of the data batch at `offset`.
    std::vector<uint64_t> keysAt(int64_t offset, std::vector<uint64_t>* valueIds = nullptr) {
        const auto [data, len] = batchAt(offset);
        std::vector<uint64_t> keys;
        (void)rune::protocol::forEachRecordSpan(data, len, [&](const rune::protocol::RecordSpan& r) {
            uint64_t k = 0, v = 0;
            std::memcpy(&k, r.key.data(), r.key.size());
            std::memcpy(&v, r.value.data() + 4, keyWidth);
            keys.push_back(k);
            if (valueIds) valueIds->push_back(v);
        });
        return keys;
    }
    bool validAt(int64_t offset) {
        const auto [data, len] = batchAt(offset);
        return rune::protocol::RecordBatchReader(data, len).ensureValid();
    }
    std::optional<uint64_t> reservationAt(int64_t offset) {
        const auto [data, len] = batchAt(offset);
        return cr::parseReservation(data, len);
    }
};

}  // namespace

TEST(CheckedWritesAutoIncHook, KeyZeroBatchIsPatchedInPlaceOfTheCopy) {
    Rig rig;
    const auto input = rig.creates(3);
    const auto before = input;
    const Outcome o = rig.write(input);
    ASSERT_EQ(o.code, ErrorCode::OK);
    EXPECT_EQ(o.base, 1);
    EXPECT_EQ(rig.reservationAt(0), 4096u);
    std::vector<uint64_t> valueIds;
    EXPECT_EQ(rig.keysAt(1, &valueIds), (std::vector<uint64_t>{1, 2, 3}));
    EXPECT_EQ(valueIds, (std::vector<uint64_t>{1, 2, 3}));
    EXPECT_TRUE(rig.validAt(1));
    EXPECT_EQ(o.assignedBase, 1u);
    EXPECT_EQ(input, before) << "the caller's buffer is never mutated";
}

TEST(CheckedWritesAutoIncHook, SecondBatchReservesNothing) {
    Rig rig;
    ASSERT_EQ(rig.write(rig.creates(3)).code, ErrorCode::OK);
    const Outcome o = rig.write(rig.creates(2));
    ASSERT_EQ(o.code, ErrorCode::OK);
    EXPECT_EQ(o.base, 4) << "no second reservation record";
    EXPECT_EQ(rig.keysAt(4), (std::vector<uint64_t>{4, 5}));
    EXPECT_EQ(o.assignedBase, 4u);
    EXPECT_EQ(rig.state.autoIncStats.reservationsAppended, 1u);
}

TEST(CheckedWritesAutoIncHook, Uint32KeysPatchFourBytes) {
    Rig rig(PrimType::UInt32);
    const Outcome o = rig.write(rig.creates(2));
    ASSERT_EQ(o.code, ErrorCode::OK);
    const auto [data, len] = rig.batchAt(o.base);
    std::vector<std::string> keys;
    (void)rune::protocol::forEachRecordSpan(data, len, [&](const rune::protocol::RecordSpan& r) { keys.emplace_back(r.key); });
    EXPECT_EQ(keys, (std::vector<std::string>{le(1, 4), le(2, 4)}));
    EXPECT_TRUE(rig.validAt(o.base));
}

TEST(CheckedWritesAutoIncHook, StrideAndPartition) {
    Rig rig(PrimType::UInt64, 4, 2);
    const Outcome o = rig.write(rig.creates(3));
    ASSERT_EQ(o.code, ErrorCode::OK);
    EXPECT_EQ(rig.keysAt(o.base), (std::vector<uint64_t>{6, 10, 14}));
    EXPECT_EQ(o.assignedBase, 6u);
}

TEST(CheckedWritesAutoIncHook, ExhaustedBatchAppendsNothing) {
    Rig rig(PrimType::UInt64, 1, 0, 2);
    const Outcome o = rig.write(rig.creates(3));
    EXPECT_EQ(o.code, ErrorCode::AutoIncrementExhausted);
    EXPECT_EQ(o.why, HookRefusal::AutoIncExhausted);
    EXPECT_EQ(rig.tlog.log.logEndOffset(), 0);
    EXPECT_EQ(rig.state.autoInc.nextN(), 1u);
    EXPECT_EQ(rig.state.autoInc.reservedThroughN(), 0u);
}

TEST(CheckedWritesAutoIncHook, ExplicitInsertWithoutImportIsNotAuthorized) {
    Rig rig;
    rig.facts.importGranted = false;
    const Outcome o = rig.write(rig.batchOf({Rec{5}}));
    EXPECT_EQ(o.code, ErrorCode::NotAuthorized);
    EXPECT_EQ(o.why, HookRefusal::ImportNotGranted);
    EXPECT_EQ(rig.tlog.log.logEndOffset(), 0);
}

TEST(CheckedWritesAutoIncHook, ImportAboveTheReservationAppendsItFirst) {
    Rig rig;
    rig.state.autoInc = AutoIncState{};
    rig.state.autoInc.configure(0, 1, kMax53, 4096);
    rig.state.autoInc.beginScan();
    rig.state.autoInc.onReservation(4096);
    rig.state.autoInc.onAssignedThrough(4096);
    (void)rig.state.autoInc.finishScan();
    const Outcome o = rig.write(rig.batchOf({Rec{5000}}));
    ASSERT_EQ(o.code, ErrorCode::OK);
    EXPECT_EQ(o.base, 1);
    const auto through = rig.reservationAt(0);
    ASSERT_TRUE(through.has_value());
    EXPECT_GE(*through, 5000u);
    EXPECT_EQ(rig.state.autoInc.nextN(), 5001u);
    EXPECT_EQ(o.assignedBase, 0u);
    EXPECT_EQ(rig.keysAt(1), (std::vector<uint64_t>{5000})) << "an import is stored as sent";
}

TEST(CheckedWritesAutoIncHook, ImportBadKeys) {
    Rig even(PrimType::UInt64, 2, 0);
    EXPECT_EQ(even.write(even.batchOf({Rec{3}})).why, HookRefusal::ImportOutOfRange) << "not this partition's id";
    EXPECT_EQ(even.write(even.batchOf({Rec{4}})).code, ErrorCode::OK);
    Rig negative(PrimType::Int64);
    const Outcome neg = negative.write(negative.batchOf({Rec{static_cast<uint64_t>(-5)}}));
    EXPECT_EQ(neg.code, ErrorCode::InvalidRequest);
    EXPECT_EQ(neg.why, HookRefusal::ImportOutOfRange);
    Rig big;
    EXPECT_EQ(big.write(big.batchOf({Rec{uint64_t{1} << 53}})).why, HookRefusal::ImportOutOfRange);
    EXPECT_EQ(big.write(big.batchOf({Rec{kMax53}})).code, ErrorCode::OK);
    EXPECT_EQ(big.state.autoInc.nextN(), kMax53 + 1);
}

TEST(CheckedWritesAutoIncHook, KeyZeroWithANonZeroValueKeyIsSchemaValidationFailed) {
    Rig rig;
    const Outcome o = rig.write(rig.batchOf({Rec{0, 1, 7}}));
    EXPECT_EQ(o.code, ErrorCode::SchemaValidationFailed);
    EXPECT_EQ(rig.tlog.log.logEndOffset(), 0);
}

TEST(CheckedWritesAutoIncHook, WindowRecordsTheBase) {
    Rig rig;
    const Outcome o = rig.write(rig.creates(2), 11);
    ASSERT_EQ(o.code, ErrorCode::OK);
    EXPECT_EQ(rig.state.assignedBases.find(11, o.base), 1u);
    EXPECT_TRUE(rig.state.assignedBases.isKeyZeroBatch(11, o.base));
    EXPECT_EQ(rig.state.autoIncStats.keyZeroBatches, 1u);
}

TEST(CheckedWritesAutoIncHook, ReservationAppendFailureIsInternalAndAssignsNothing) {
    Rig rig;
    rig.tlog.failNext = true;
    const Outcome o = rig.write(rig.creates(2));
    EXPECT_EQ(o.code, ErrorCode::Internal);
    EXPECT_EQ(rig.state.autoInc.nextN(), 1u);
    EXPECT_EQ(rig.state.autoInc.reservedThroughN(), 0u);
    EXPECT_EQ(rig.tlog.log.logEndOffset(), 0);
}

TEST(CheckedWritesAutoIncHook, ACounterThatIsNotReadyWaits) {
    Rig rig;
    rig.state.autoInc.deactivate();
    for (const auto& batch : {rig.creates(1), rig.batchOf({Rec{5}})}) {
        const Outcome o = rig.write(batch);
        EXPECT_EQ(o.code, ErrorCode::NotLeader);
        EXPECT_EQ(o.why, HookRefusal::AutoIncActivating);
    }
}

TEST(CheckedWritesAutoIncHook, PlainUpsertIsRefusedAndTombstoneIsNot) {
    Rig rig;
    const Outcome up = rig.write(rig.batchOf({Rec{9, 0}}));
    EXPECT_EQ(up.code, ErrorCode::InvalidRequest);
    EXPECT_EQ(up.why, HookRefusal::UpsertOnAutoIncrement);
    rune::protocol::RecordBatchBuilder b(0, 1'000'000);
    b.append(le(9, 8), std::string_view{}, 1'000'000, 0);
    EXPECT_EQ(rig.write(b.build()).code, ErrorCode::OK);
}

TEST(CheckedWritesAutoIncHookRegression, ImportBatchIsNeverRecordedRegression) {
    Rig rig;
    rig.state.assignedBases = AssignedBaseWindow{};
    rig.state.assignedBases.reserve(4);
    for (int64_t pid = 1; pid <= 4; ++pid) ASSERT_EQ(rig.write(rig.creates(1), pid).code, ErrorCode::OK);
    ASSERT_EQ(rig.state.assignedBases.producers(), 4u);
    ASSERT_EQ(rig.state.assignedBases.capacity(), 4u);
    const Outcome imp = rig.write(rig.batchOf({Rec{500}}), 5);
    ASSERT_EQ(imp.code, ErrorCode::OK);
    EXPECT_EQ(imp.assignedBase, 0u);
    EXPECT_EQ(rig.state.assignedBases.find(5, imp.base), 0u);
    EXPECT_EQ(rig.state.assignedBases.producers(), 4u);
    const Outcome zero = rig.write(rig.creates(1), 5);
    EXPECT_EQ(zero.code, ErrorCode::NotLeader);
    EXPECT_EQ(zero.why, HookRefusal::AutoIncWindowFull);
}

TEST(CheckedWritesAutoIncHookRegression, NonAutoIncSpecIsUntouchedRegression) {
    Rig rig(PrimType::UInt64, 1, 0, kMax53, 4096, false);
    const auto batch = rig.batchOf({Rec{7, 0}, Rec{8, 0}});
    CheckedWriteHook hook(rig.state, rig.facts, rig.scratch);
    ASSERT_EQ(hook.check(batch.data(), batch.size(), 2), ErrorCode::OK);
    const auto staged = hook.stage(rig.tlog, batch.data(), batch.size());
    EXPECT_EQ(staged.error, ErrorCode::OK);
    EXPECT_EQ(staged.batch, batch.data());
    EXPECT_EQ(staged.len, batch.size());
    hook.onAppended(0, 1);
    EXPECT_EQ(hook.assignedBase(), 0u);
}
