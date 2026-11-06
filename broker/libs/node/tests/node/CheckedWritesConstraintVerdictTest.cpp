// Part 09 Task 4: unique and check members of the verdict source (unit level: no worker, no replica).  The views are the QL
// statements of the spec, compiled by the real compiler; the members are ViewInstances built as WorkerViewSet::getOrCreate_ builds them.
#include <gtest/gtest.h>

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <limits>
#include <memory>
#include <cstring>
#include <stdexcept>
#include <string>
#include <vector>

#include <unistd.h>

#include <rune/codec/Descriptor.h>
#include <rune/node/AdminChannels.h>
#include <rune/node/ViewCheckpointScan.h>
#include <rune/node/ViewSnapshotChannel.h>
#include <rune/node/WorkerViewSet.h>
#include <rune/node/VerdictSource.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/ql/Backends.h>
#include <rune/ql/Parser.h>
#include <rune/view/PlanCompiler.h>

using namespace rune::node;
namespace cr = rune::storage::control_record;
namespace ql = rune::ql;
namespace rv = rune::view;
using rune::codec::FieldValue;
using rune::codec::PrimType;
using rv::VerdictViewKind;

namespace {

constexpr uint16_t kViolation = 55, kExists = 56, kNotFound = 57;
constexpr uint8_t kUpsert = 0, kInsert = 1, kExisting = 2;
constexpr int32_t kSrc = 5;

const char* kCharacter = "topic character { characterId uint64, userId uint64, name string(16), gender uint16, lastPlayed int64 } partitions 1 key characterId;";
const char* kEntry = "topic entry { entryId uint64, account uint64, amount int64, memo string(16) } partitions 1 key entryId;";

const char* kUniqueName = "create unique view character_name key lower(name) as nameKey as select characterId, userId from character;";
const char* kAccountStats = "create view account_stats key userId as select count(*) as characters from character group by userId "
                            "constraint max_characters check (characters <= 12);";

struct Rec {
    std::string          key;
    std::vector<uint8_t> value;   // empty = tombstone
    uint8_t              intent = kUpsert;
};

void putInt(std::vector<uint8_t>& rec, const rune::codec::Descriptor& d, const char* field, uint64_t v) {
    const auto* f = d.find(field);
    for (uint32_t i = 0; i < rune::codec::fieldWidth(*f); ++i) rec[4 + f->offset + i] = uint8_t(v >> (8 * i));
}
void putStr(std::vector<uint8_t>& rec, const rune::codec::Descriptor& d, const char* field, const std::string& s) {
    const auto* f = d.find(field);
    for (size_t i = 0; i < s.size(); ++i) rec[4 + f->offset + i] = uint8_t(s[i]);
}
std::string wireOf(uint64_t id) {
    std::string s(8, '\0');
    for (int i = 0; i < 8; ++i) s[std::size_t(i)] = char(id >> (8 * i));
    return s;
}
std::string_view sv(const std::vector<uint8_t>& b) { return {reinterpret_cast<const char*>(b.data()), b.size()}; }

// A committed log of whole batches; `read(from)` starts at the batch holding `from`.
struct Log {
    std::vector<uint8_t> bytes;
    struct Span { std::size_t pos; int64_t base, last; };
    std::vector<Span>    batches;
    int64_t              next = 0;

    int64_t data(const std::vector<Rec>& recs) {
        const int64_t base = next;
        rune::protocol::RecordBatchBuilder b(next, 0);
        for (const Rec& r : recs) b.append(r.key, sv(r.value), 0, r.intent);
        push(b.build(), int64_t(recs.size()));
        return base;
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

struct Mem {
    rv::CompileResult             comp;
    std::unique_ptr<ViewInstance> inst;
    uint64_t                      objectId = 0;

    [[nodiscard]] uint16_t slot(const char* field) const {
        for (std::size_t i = 0; i < comp.outputDescriptor.fields.size(); ++i)
            if (comp.outputDescriptor.fields[i].name == field) return uint16_t(i);
        throw std::runtime_error(std::string("no column ") + field);
    }
    [[nodiscard]] static rv::ViewKey u64Key(uint64_t v) {
        FieldValue f;
        f.type = PrimType::UInt64;
        f.present = true;
        f.u = v;
        rv::ViewKey k;
        EXPECT_TRUE(rune::codec::encodeKeyComponent(PrimType::UInt64, 0, f, std::span<uint8_t>(k.bytes.data(), 8)));
        k.len = 8;
        return k;
    }
    [[nodiscard]] const uint8_t* row(uint64_t userKey) const {
        return rv::keyModeOf(comp.plan) == rv::ViewKeyMode::LegacyU64 ? inst->table.get(userKey) : inst->table.get(u64Key(userKey));
    }
    [[nodiscard]] int64_t value(uint64_t userKey, const char* col) const {
        const uint8_t* r = row(userKey);
        return r == nullptr ? -1 : rv::readSlot(r, comp.rowLayout, slot(col)).i;
    }
};

using V = std::pair<uint16_t, uint32_t>;

struct Rig {
    rune::codec::Descriptor        src;
    ql::TopicDef                   topic;
    Log                            log;
    PartitionWriteState            st;
    VerdictSource                  vs{kSrc, 0};
    std::vector<std::unique_ptr<Mem>> mems;
    std::unique_ptr<ViewInstance>  index;
    ConstraintSet                  set;
    bool                           started = false;

    explicit Rig(const char* topicText = kCharacter) {
        auto parsed = ql::parse(topicText);
        if (!parsed) throw std::runtime_error(parsed.error().message);
        topic = std::get<ql::TopicDef>(*parsed);
        src   = ql::toDescriptor(topic);
        set.id = 1;
    }
    void keyIndex() {
        index = std::make_unique<ViewInstance>(keyIndexPlan(kSrc, src.find(topic.keyField.c_str())->id), keyIndexRowLayout(), src, false, 0, 4096, kKeyIndexChunkBytes);
        vs.addVerdictView(index.get(), VerdictViewKind::KeyIndex, 0, kKeyIndexPlanFingerprint);
        set.members[set.count++] = {0, VerdictViewKind::KeyIndex, kKeyIndexPlanFingerprint};
    }
    Mem& view(const std::string& text, uint64_t objectId) {
        auto parsed = ql::parse(text);
        if (!parsed) throw std::runtime_error(text + ": " + parsed.error().message);
        auto authored = ql::applyViewPlan(std::get<ql::CreateViewDef>(*parsed), ql::viewSourceOf(topic));
        if (!authored) throw std::runtime_error(text + ": " + authored.error().message);
        authored->sources[0].topicId = kSrc;
        const rune::codec::Descriptor* srcs[] = {&src};
        auto compiled = rv::compileView(*authored, srcs, int32_t(0x40000000 + objectId));
        if (!compiled) throw std::runtime_error(text + ": " + compiled.error().message);
        auto m = std::make_unique<Mem>();
        m->comp = std::move(*compiled);
        m->comp.plan.viewObjectId = objectId;
        m->objectId = objectId;
        rv::ViewTableKeys keys;
        if (const auto spec = rv::buildKeyedFoldSpec(m->comp.plan, src)) {
            keys.keyWidth = uint8_t(spec->keyWidth);
            keys.prefixWidth = uint8_t(spec->prefixWidth);
        }
        m->inst = std::make_unique<ViewInstance>(m->comp.plan, m->comp.rowLayout, src, false, 0, 256, 1u << 16, keys);
        if (rv::isAggregateView(m->inst->plan)) m->inst->agg.emplace(rv::buildAggLayout(m->inst->plan, src).value(), 256, 1u << 16, m->inst->tableKeys);
        if (const auto spec = rv::buildKeyedFoldSpec(m->inst->plan, src)) {
            m->inst->keyed = spec;
            buildKeyedTables(*m->inst, 256, 1u << 16);
        }
        const VerdictViewKind kind = rv::verdictKindOf(m->inst->plan).value();
        vs.addVerdictView(m->inst.get(), kind, objectId, objectId);
        set.members[set.count++] = {objectId, kind, objectId};
        std::sort(set.members.begin(), set.members.begin() + set.count, [](const auto& a, const auto& b) { return a.viewObjectId < b.viewObjectId; });
        mems.push_back(std::move(m));
        return *mems.back();
    }
    // The members join at a constraint-set change after everything appended so far (which they replay unjudged).
    void start() {
        log.control();
        st.constraintSets.onChange(log.next - 1, set);
        vs.bindState(&st);
        vs.bindLeader(nullptr, 1, 0);
        started = true;
        drive();
    }
    int drive() { return vs.driveJudge([&](int64_t from, int64_t) { return log.read(from); }, log.next, 0, st); }
    // Appends one batch and judges it; returns the batch's base offset.
    int64_t put(const std::vector<Rec>& recs) {
        const int64_t base = log.data(recs);
        if (started) drive();
        return base;
    }
    [[nodiscard]] V verdictAt(int64_t offset) const {
        const auto* e = st.rejected.find(offset);
        return e == nullptr ? V{0, 0} : V{e->code, e->constraintId};
    }
    [[nodiscard]] V put1(const Rec& r) { return verdictAt(put({r})); }
    [[nodiscard]] Rec character(uint64_t id, uint64_t user, const std::string& name, uint64_t gender = 0, int64_t lastPlayed = 0, uint8_t intent = kUpsert) const {
        std::vector<uint8_t> v(4 + src.blockLength, 0);
        v[0] = uint8_t(src.blockLength); v[1] = uint8_t(src.blockLength >> 8);
        v[2] = uint8_t(src.version); v[3] = uint8_t(src.version >> 8);
        putInt(v, src, "characterId", id);
        putInt(v, src, "userId", user);
        putStr(v, src, "name", name);
        putInt(v, src, "gender", gender);
        putInt(v, src, "lastPlayed", uint64_t(lastPlayed));
        return {wireOf(id), v, intent};
    }
    [[nodiscard]] Rec entry(uint64_t id, uint64_t account, int64_t amount, const std::string& memo = "") const {
        std::vector<uint8_t> v(4 + src.blockLength, 0);
        v[0] = uint8_t(src.blockLength); v[1] = uint8_t(src.blockLength >> 8);
        v[2] = uint8_t(src.version); v[3] = uint8_t(src.version >> 8);
        putInt(v, src, "entryId", id);
        putInt(v, src, "account", account);
        putInt(v, src, "amount", uint64_t(amount));
        putStr(v, src, "memo", memo);
        return {wireOf(id), v, kUpsert};
    }
    [[nodiscard]] static Rec tombstone(uint64_t id, uint8_t intent = kUpsert) { return {wireOf(id), {}, intent}; }
    [[nodiscard]] bool indexHas(uint64_t id) const { return index->table.get(id) != nullptr; }
};

constexpr uint32_t kUniqueId = 0x00070000, kStatsId = 0x00080001, kBalanceId = 0x00090001;

// `n` creates for `user` with ids from `firstId` and distinct names.
std::vector<Rec> creates(const Rig& r, uint64_t user, int n, uint64_t firstId) {
    std::vector<Rec> out;
    for (int i = 0; i < n; ++i) out.push_back(r.character(firstId + uint64_t(i), user, "u" + std::to_string(user) + "n" + std::to_string(i)));
    return out;
}

}  // namespace

TEST(CheckedWritesConstraintVerdict, ConstraintFoldOnAProspectRejectsTheTakenKeyAndSkipsTombstones) {
    Rig r;
    Mem& m = r.view(kUniqueName, 7);
    rv::Prospect p;
    p.reserve(m.inst->layout, nullptr);
    p.op = rv::ProspectOp::Upsert;
    p.rowCount = 1;
    p.rows[0].ownedByOther = true;
    const ConstraintFold fold{m.inst->plan, m.inst->layout, m.inst->table};
    const Rec live = r.character(1, 1, "Foo");
    const rv::SourceRecord rec{live.key, sv(live.value), 3, 0, nullptr};
    const rv::Verdict taken = evaluateConstraints(fold, rec, p);
    EXPECT_EQ(taken.code, kViolation);
    EXPECT_EQ(taken.constraintId, kUniqueId);
    const rv::SourceRecord tomb{live.key, {}, 4, 0, nullptr};
    EXPECT_TRUE(evaluateConstraints(fold, tomb, p).accepted());
}

TEST(CheckedWritesConstraintVerdict, FooThenLowerFooIsRejectedByTheUniqueView) {
    Rig r;
    Mem& names = r.view(kUniqueName, 7);
    Mem& stats = r.view(kAccountStats, 8);
    r.start();
    EXPECT_EQ(r.put1(r.character(1, 1, "Foo")), (V{0, 0}));
    const int64_t at = r.log.next;
    EXPECT_EQ(r.put1(r.character(2, 2, "foo")), (V{kViolation, kUniqueId}));
    EXPECT_EQ(names.inst->table.size(), 1u);
    EXPECT_EQ(stats.row(2), nullptr);
    EXPECT_EQ(stats.value(1, "characters"), 1);
    ASSERT_EQ(r.vs.stats().perConstraintCount, 1u);
    EXPECT_EQ(r.vs.stats().perConstraint[0].constraintId, kUniqueId);
    EXPECT_EQ(r.vs.stats().perConstraint[0].rejections, 1u);
    EXPECT_EQ(r.vs.stats().rejectionsConstraint, 1u);
    EXPECT_TRUE(r.st.rejected.isRejected(at));
}

TEST(CheckedWritesConstraintVerdict, RenameToTheOwnNameInAnotherCasePasses) {
    Rig r;
    Mem& names = r.view(kUniqueName, 7);
    r.start();
    EXPECT_EQ(r.put1(r.character(1, 1, "Foo")), (V{0, 0}));
    EXPECT_EQ(r.put1(r.character(1, 1, "FOO")), (V{0, 0}));
    EXPECT_EQ(names.inst->table.size(), 1u);
    EXPECT_EQ(names.inst->counters.ownerMoves, 0u);
}

TEST(CheckedWritesConstraintVerdict, TwelfthPassesThirteenthIsRejected) {
    Rig r;
    Mem& stats = r.view(kAccountStats, 8);
    r.start();
    int64_t base = 0;
    for (int i = 0; i < 13; ++i) {
        const Rec c = r.character(100 + uint64_t(i), 7, "n" + std::to_string(i));
        base = r.put({c});
        EXPECT_EQ(r.verdictAt(base), (i < 12 ? V{0, 0} : V{kViolation, kStatsId})) << i;
    }
    EXPECT_EQ(stats.value(7, "characters"), 12);
}

TEST(CheckedWritesConstraintVerdict, OverLimitGroupRenamePassesCreateFailsDeletePasses) {
    Rig r;
    Mem& stats = r.view(kAccountStats, 8);
    r.put(creates(r, 7, 14, 100));   // folded before the members joined: unjudged
    r.start();
    ASSERT_EQ(stats.value(7, "characters"), 14);
    EXPECT_EQ(r.put1(r.character(100, 7, "renamed")), (V{0, 0}));
    EXPECT_EQ(r.put1(r.character(200, 7, "new1")), (V{kViolation, kStatsId}));
    EXPECT_EQ(r.put1(r.tombstone(100)), (V{0, 0}));
    EXPECT_EQ(stats.value(7, "characters"), 13);
    EXPECT_EQ(r.put1(r.character(201, 7, "new2")), (V{kViolation, kStatsId}));
    EXPECT_EQ(r.put1(r.tombstone(101)), (V{0, 0}));
    EXPECT_EQ(r.put1(r.tombstone(102)), (V{0, 0}));
    EXPECT_EQ(stats.value(7, "characters"), 11);
    EXPECT_EQ(r.put1(r.character(202, 7, "new3")), (V{0, 0}));
    EXPECT_EQ(stats.value(7, "characters"), 12);
}

TEST(CheckedWritesConstraintVerdict, DeleteAlwaysPassesBalanceLedger) {
    Rig r(kEntry);
    Mem& ledger = r.view("create view ledger_balance key account as select sum(amount) as balance from entry group by account "
                         "constraint solvent check (balance >= 0);", 9);
    r.start();
    EXPECT_EQ(r.put1(r.entry(1, 1, 10)), (V{0, 0}));
    EXPECT_EQ(r.put1(r.entry(2, 1, -4)), (V{0, 0}));
    EXPECT_EQ(ledger.value(1, "balance"), 6);
    EXPECT_EQ(r.put1(Rig::tombstone(1)), (V{0, 0}));
    EXPECT_EQ(ledger.value(1, "balance"), -4);
    EXPECT_EQ(r.put1(r.entry(3, 1, -1)), (V{kViolation, kBalanceId}));
    EXPECT_EQ(r.put1(r.entry(4, 1, 2)), (V{kViolation, kBalanceId}));   // -2 still fails and the balance changed
    EXPECT_EQ(r.put1(r.entry(5, 1, 5)), (V{0, 0}));
    EXPECT_EQ(ledger.value(1, "balance"), 1);
    EXPECT_EQ(r.put1(r.entry(2, 1, -4, "memo")), (V{0, 0}));
}

TEST(CheckedWritesConstraintVerdictRegression, MoveOutOfAnOverLimitGroupIsRejectedRegression) {
    Rig r;
    Mem& stats = r.view(kAccountStats, 8);
    r.put(creates(r, 7, 14, 100));
    r.put(creates(r, 8, 3, 200));
    r.put(creates(r, 9, 11, 300));
    r.start();
    EXPECT_EQ(r.put1(r.character(100, 8, "moved")), (V{kViolation, kStatsId}));   // 7 stays over the limit and its count changed
    EXPECT_EQ(stats.value(7, "characters"), 14);
    EXPECT_EQ(stats.value(8, "characters"), 3);
    EXPECT_EQ(r.put1(r.character(200, 9, "moved8")), (V{0, 0}));                  // 9 reaches 12
    EXPECT_EQ(stats.value(9, "characters"), 12);
    EXPECT_EQ(r.put1(r.character(201, 9, "moved9")), (V{kViolation, kStatsId}));  // 9 would reach 13
}

TEST(CheckedWritesConstraintVerdict, DeleteMakesRoomInTheSameBatch) {
    Rig r;
    r.view(kUniqueName, 7);
    Mem& stats = r.view(kAccountStats, 8);
    r.put(creates(r, 7, 12, 100));
    r.put({r.character(500, 1, "Foo")});
    r.start();
    const int64_t a = r.put({Rig::tombstone(100), r.character(600, 7, "extra")});
    EXPECT_EQ(r.verdictAt(a), (V{0, 0}));
    EXPECT_EQ(r.verdictAt(a + 1), (V{0, 0}));
    EXPECT_EQ(stats.value(7, "characters"), 12);
    const int64_t b = r.put({Rig::tombstone(500), r.character(700, 2, "foo")});
    EXPECT_EQ(r.verdictAt(b), (V{0, 0}));
    EXPECT_EQ(r.verdictAt(b + 1), (V{0, 0}));
}

TEST(CheckedWritesConstraintVerdict, SelfCollidingBatchJudgesInOrder) {
    Rig r;
    r.view(kUniqueName, 7);
    r.start();
    const int64_t a = r.put({r.character(1, 1, "Bar"), r.character(2, 2, "bar")});
    EXPECT_EQ(r.verdictAt(a), (V{0, 0}));
    EXPECT_EQ(r.verdictAt(a + 1), (V{kViolation, kUniqueId}));
}

TEST(CheckedWritesConstraintVerdict, SeveralVerdictViewsOneRejectsNoViewChanges) {
    Rig r;
    r.keyIndex();
    Mem& names = r.view(kUniqueName, 7);
    Mem& stats = r.view(kAccountStats, 8);
    r.put(creates(r, 7, 11, 100));
    r.put({r.character(50, 1, "Foo")});
    r.start();
    const auto nameKeyOf = [&](const char* n) {
        rv::ViewKey k;
        std::copy(n, n + std::strlen(n), k.bytes.begin());
        k.len = uint8_t(names.inst->keyed->keyWidth);
        return k;
    };
    const auto bytesOf = [](const rv::ViewTable& t, const rv::ViewKey& k, std::size_t len) {
        const uint8_t* p = t.get(k);
        return p == nullptr ? std::string("-") : std::string(reinterpret_cast<const char*>(p), len);
    };
    // The unique view's rows, owner map and both claims tables, sizes and the affected keys.
    const auto dump = [&](std::initializer_list<const char*> taken, std::initializer_list<uint64_t> ids) {
        const rv::UniqueClaims& cl = *names.inst->claims;
        const std::size_t K = names.inst->keyed->keyWidth, W = names.inst->keyed->wireKeyWidth;
        std::string s;
        for (const char* n : {"foo", "u7n0", "u7n10"}) s += bytesOf(names.inst->table, nameKeyOf(n), names.comp.rowLayout.rowLen);
        for (const char* n : taken) s += bytesOf(cl.indexTable(), nameKeyOf(n), 2 * W + 5);
        for (const uint64_t id : ids) {
            const std::string wire = wireOf(id);
            const auto wk = rv::ViewKey::fromBytes({reinterpret_cast<const uint8_t*>(wire.data()), wire.size()});
            s += bytesOf(*names.inst->owners, wk, K) + bytesOf(cl.extrasTable(), wk, K + 2 * W + 1 + names.comp.rowLayout.rowLen);
        }
        return s + std::to_string(names.inst->table.size()) + "/" + std::to_string(names.inst->owners->size()) + "/" +
               std::to_string(cl.indexTable().size()) + "/" + std::to_string(cl.extrasTable().size());
    };
    const std::string before = dump({"foo", "fresh"}, {50, 900, 901});
    EXPECT_EQ(r.put1(r.character(900, 7, "Foo")), (V{kViolation, kUniqueId}));
    EXPECT_EQ(stats.value(7, "characters"), 11);
    EXPECT_FALSE(r.indexHas(900));
    EXPECT_EQ(dump({"foo", "fresh"}, {50, 900, 901}), before);
    EXPECT_EQ(r.put1(r.character(50, 1, "other", 0, 0, kInsert)), (V{kExists, 0}));   // the key index is first

    // The unique member prepares a new name before the stats member rejects the 13th create of user 7: that name stays unclaimed.
    EXPECT_EQ(r.put1(r.character(901, 7, "fresh")), (V{0, 0}));   // the 12th
    const std::string full = dump({"foo", "fresh"}, {50, 901, 902});
    EXPECT_EQ(r.put1(r.character(902, 7, "Fresh2")), (V{kViolation, kStatsId}));
    EXPECT_EQ(dump({"foo", "fresh"}, {50, 901, 902}), full);
    EXPECT_EQ(names.inst->claims->ownerOf(nameKeyOf("fresh2")), nullptr);
    EXPECT_EQ(names.inst->table.get(nameKeyOf("fresh2")), nullptr);
    EXPECT_FALSE(r.indexHas(902));
}

TEST(CheckedWritesConstraintVerdict, UniqueBeforeItsChecksInsideOneMember) {
    Rig r;
    r.view("create unique view checked_name key lower(name) as nameKey as select characterId, gender from character "
           "constraint small_gender check (gender <= 3);", 7);
    r.start();
    EXPECT_EQ(r.put1(r.character(1, 1, "Foo", 1)), (V{0, 0}));
    EXPECT_EQ(r.put1(r.character(2, 2, "foo", 5)), (V{kViolation, 0x00070000}));
    EXPECT_EQ(r.put1(r.character(3, 3, "bar", 5)), (V{kViolation, 0x00070001}));
}

TEST(CheckedWritesConstraintVerdict, TombstonesAreNeverRejected) {
    Rig r;
    r.keyIndex();
    r.view(kUniqueName, 7);
    r.view(kAccountStats, 8);
    r.put(creates(r, 7, 14, 100));
    r.start();
    EXPECT_EQ(r.put1(Rig::tombstone(100)), (V{0, 0}));
    EXPECT_EQ(r.put1(Rig::tombstone(9999)), (V{0, 0}));
    EXPECT_EQ(r.put1(Rig::tombstone(9998, kExisting)), (V{kNotFound, 0}));
}

TEST(CheckedWritesConstraintVerdict, CheckOverACompositeGroupBy) {
    Rig r;
    Mem& per = r.view("create view per_gender key (userId, gender) as select count(*) as n from character group by userId, gender "
                      "constraint at_most_two check (n <= 2);", 10);
    (void)per;
    r.start();
    EXPECT_EQ(r.put1(r.character(1, 7, "a", 1)), (V{0, 0}));
    EXPECT_EQ(r.put1(r.character(2, 7, "b", 1)), (V{0, 0}));
    EXPECT_EQ(r.put1(r.character(3, 7, "c", 1)), (V{kViolation, 0x000A0001}));
    EXPECT_EQ(r.put1(r.character(4, 7, "d", 2)), (V{0, 0}));
}

TEST(CheckedWritesConstraintVerdict, CheckOnAProjectionColumn) {
    Rig r;
    r.view("create view lp key characterId as select lastPlayed from character constraint nonneg check (lastPlayed >= 0);", 11);
    r.start();
    EXPECT_EQ(r.put1(r.character(1, 1, "a", 0, 5)), (V{0, 0}));
    EXPECT_EQ(r.put1(r.character(1, 1, "a", 0, -1)), (V{kViolation, 0x000B0001}));
    EXPECT_EQ(r.put1(Rig::tombstone(1)), (V{0, 0}));
}

TEST(CheckedWritesConstraintVerdictRegression, SumOverflowNullNeverRejectsRegression) {
    Rig r;
    Mem& ov = r.view("create view ov key userId as select sum(lastPlayed) as total from character group by userId "
                     "constraint cap check (total <= 5);", 12);
    r.put({r.character(1, 1, "a", 0, std::numeric_limits<int64_t>::max())});   // unjudged: over the cap already
    r.start();
    EXPECT_EQ(r.put1(r.character(2, 1, "b", 0, std::numeric_limits<int64_t>::max())), (V{0, 0}));   // the sum overflows: NULL, Unknown passes
    EXPECT_GE(ov.inst->counters.aggOverflow, 1u);
}

TEST(CheckedWritesConstraintVerdict, UnjudgedReplayBelowKNeverRejects) {
    Rig r;
    Mem& names = r.view(kUniqueName, 7);
    Mem& stats = r.view(kAccountStats, 8);
    std::vector<Rec> recs;
    for (uint64_t i = 1; i <= 30; ++i) recs.push_back(r.character(i, i <= 14 ? 7 : 20 + i, "n" + std::to_string(i % 10)));   // 10 names, 20 duplicates
    r.put(recs);
    r.start();
    EXPECT_EQ(r.st.rejected.size(), 0u);
    EXPECT_EQ(names.inst->table.size(), 10u);
    ASSERT_TRUE(names.inst->claims.has_value());
    EXPECT_EQ(names.inst->claims->extras(), 20u);
    EXPECT_EQ(names.inst->counters.uniqueConflicts, 20u);
    EXPECT_EQ(stats.value(7, "characters"), 14);
}

// A judged unique member (the verdict-source path: the judge folds it and the member checkpoint hook pushes it) keeps its claims
// through a restart: tables 0, 1, 6 and 7 of the recovered instance equal the judged one's.
TEST(CheckedWritesConstraintVerdictRegression, JudgedUniqueClaimsSurviveRestartRegression) {
    Rig r;
    const rv::CompileResult comp = r.view(kUniqueName, 7).comp;
    const std::string dir = (std::filesystem::temp_directory_path() / ("rune-cw-judged-unique-" + std::to_string(::getpid()))).string();
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);
    struct Cleanup { std::string d; ~Cleanup() { std::error_code ec; std::filesystem::remove_all(d, ec); } } cleanup{dir};
    const auto installOn = [&](ViewSnapshotChannel& ch, WorkerViewSet& vs, bool recovery) {
        ViewPlanUpdateMsg m;
        m.viewTopicId = comp.plan.viewTopicId;
        m.recovery    = recovery;
        m.planBlob    = rv::serialize(comp.plan);
        m.outDescBlob = rune::codec::serialize(comp.outputDescriptor);
        m.srcDescBlob = rune::codec::serialize(r.src);
        EXPECT_TRUE(encodeViewPlanUpdate(ch.install, ViewSnapshotChannel::kInstallMsgType, m));
        EXPECT_EQ(vs.drainInstalls(), 1);
        return m.planBlob;
    };
    ViewSnapshotChannel ch;
    WorkerViewSet vs(ch, dir, 12345);
    const auto blob = installOn(ch, vs, false);

    // 30 characters over 10 names, folded before the member joined: 20 extras.
    std::vector<Rec> early;
    for (uint64_t id = 1; id <= 30; ++id) early.push_back(r.character(id, 1, "n" + std::to_string(id % 10)));
    r.log.data(early);
    r.log.control();
    ConstraintSet set;
    set.id = 1;
    set.members[set.count++] = {7, VerdictViewKind::Unique, rv::verdictPlanFingerprint(blob)};
    PartitionWriteState st;
    TopicWriteSpec spec;
    st.spec = &spec;
    st.constraintSets.onChange(r.log.next - 1, set);
    vs.ensureVerdictSource(kSrc, 0, st)->bindLeader(nullptr, 1, 0);

    std::vector<uint8_t> file;
    const auto drive = [&] {
        for (int i = 0; i < 6; ++i) {
            vs.driveFolds(kSrc, 0, [&](int64_t from, int64_t) { return r.log.read(from); }, r.log.next, rv::kNoSkip, 200'000'000, nullptr, 1, true, {}, &st,
                          vs.ensureVerdictSource(kSrc, 0, st));
            verdictOnHw(st, 1'000'000);
            ch.deltas.drain([&](int32_t type, aeron::concurrent::AtomicBuffer& b, OneToOneByteRing::index_t off, OneToOneByteRing::index_t len) {
                if (type != ViewSnapshotChannel::kDeltaMsgType) return;
                const uint8_t* p = b.buffer() + off + ViewSnapshotChannel::kDeltaHeaderBytes;
                file.insert(file.end(), p, p + len - ViewSnapshotChannel::kDeltaHeaderBytes);
            });
        }
    };
    drive();
    // Judged: owners 1..3 leave (promotions), a duplicate name is refused, a new name and a rename are taken.
    const int64_t refused = r.log.data({Rig::tombstone(1), Rig::tombstone(2), Rig::tombstone(3), r.character(31, 2, "N5"), r.character(32, 2, "fresh"),
                                         r.character(14, 1, "renamed")}) + 3;
    drive();
    ASSERT_TRUE(st.rejected.isRejected(refused));
    const ViewInstance* judged = vs.instanceForTest(comp.plan.viewTopicId, 0);
    ASSERT_NE(judged, nullptr);
    ASSERT_TRUE(judged->claims.has_value());
    ASSERT_GE(judged->counters.uniquePromotions, 3u);
    ASSERT_GT(judged->claims->extras(), 0u);
    ASSERT_FALSE(file.empty()) << "the member checkpoint hook pushed nothing";
    {
        std::ofstream f(viewCheckpointFilePath(dir, comp.plan.viewTopicId, 0), std::ios::binary);
        f.write(reinterpret_cast<const char*>(file.data()), std::streamsize(file.size()));
    }

    ViewSnapshotChannel ch2;
    WorkerViewSet vs2(ch2, dir, 99);
    (void)installOn(ch2, vs2, true);
    vs2.materializeRecoveryInstancesFor(kSrc, 0, r.log.next);
    const ViewInstance* back = vs2.instanceForTest(comp.plan.viewTopicId, 0);
    ASSERT_NE(back, nullptr);
    ASSERT_TRUE(back->claims.has_value());

    const std::size_t K = judged->keyed->keyWidth, W = judged->keyed->wireKeyWidth, rowLen = judged->layout.rowLen;
    const auto bytesAt = [](const rv::ViewTable& t, const rv::ViewKey& k, std::size_t len) {
        const uint8_t* p = t.get(k);
        return p == nullptr ? std::vector<uint8_t>{} : std::vector<uint8_t>(p, p + len);
    };
    EXPECT_EQ(back->table.size(), judged->table.size());
    EXPECT_EQ(back->owners->size(), judged->owners->size());
    EXPECT_EQ(back->claims->indexTable().size(), judged->claims->indexTable().size());
    EXPECT_EQ(back->claims->extrasTable().size(), judged->claims->extrasTable().size());
    EXPECT_EQ(back->claims->extras(), judged->claims->extras());
    EXPECT_EQ(back->counters.uniqueConflicts, judged->counters.uniqueConflicts);
    std::vector<std::string> names{"fresh", "renamed", "n5"};
    for (int i = 0; i < 10; ++i) names.push_back("n" + std::to_string(i));
    for (const std::string& n : names) {
        rv::ViewKey k;
        std::copy(n.begin(), n.end(), k.bytes.begin());
        k.len = uint8_t(K);
        EXPECT_EQ(bytesAt(back->table, k, rowLen), bytesAt(judged->table, k, rowLen)) << n;
        EXPECT_EQ(bytesAt(back->claims->indexTable(), k, 2 * W + 5), bytesAt(judged->claims->indexTable(), k, 2 * W + 5)) << n;
    }
    for (uint64_t id = 1; id <= 40; ++id) {
        const std::string wire = wireOf(id);
        const auto wk = rv::ViewKey::fromBytes({reinterpret_cast<const uint8_t*>(wire.data()), wire.size()});
        EXPECT_EQ(bytesAt(*back->owners, wk, K), bytesAt(*judged->owners, wk, K)) << id;
        EXPECT_EQ(bytesAt(back->claims->extrasTable(), wk, K + 2 * W + 1 + rowLen), bytesAt(judged->claims->extrasTable(), wk, K + 2 * W + 1 + rowLen)) << id;
    }
}
