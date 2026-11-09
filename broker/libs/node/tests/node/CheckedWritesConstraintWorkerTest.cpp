// Checked writes Part 09 Task 5b: unique and check views on a real Worker - created, replaced and dropped while writes flow,
// candidates that swap at S, plain writes judged, stat 32.  The views are the QL statements of the spec, authored and compiled as
// the AdminWorker does and installed through the control thread's install frames (then the catalog mark of the event).
#include "CheckedWritesRig.h"

#include <rune/ql/Backends.h>
#include <rune/ql/Parser.h>
#include <rune/view/Verdict.h>

#include <algorithm>
#include <map>
#include <stdexcept>

using namespace rune::node;
using namespace rune::node::cw;
namespace ql = rune::ql;
namespace rv = rune::view;
namespace pc = rune::protocol::client;

namespace {
constexpr uint8_t kLeader = 1, kView = pc::kAcksWireView, kAll = pc::kAcksWireAll, kAllView = pc::kAcksWireView | pc::kAcksWireAll;
constexpr uint16_t kViolation = 55;
constexpr int16_t kInvalidRequest = static_cast<int16_t>(pc::ErrorCode::InvalidRequest);
constexpr uint8_t kInsert = 1;
using V = std::vector<std::tuple<uint16_t, uint16_t, uint32_t>>;

const char* kUserTopic = "topic user { userId uint64, provider uint16, subject string(64), createdAt int64 } partitions 1 key userId;";
const char* kPcTopic = "topic pc { id uint64, userId uint64, name string(16) } partitions 1 key id;";

using CV = ConstrainedView;
uint64_t objectIdOf(CV v) { return VerdictRig::objectIdOf(v); }
constexpr uint32_t kNameId = 7u << 16, kStatsId = (8u << 16) | 1;

/// One rig over a source topic and the constrained views created on it.
struct Cw {
    VerdictRig r;
    const char* topicText;
    std::map<CV, VerdictRig::ConstrainedLive>& live = r.constrained;

    explicit Cw(const char* text = VerdictRig::kCharacterTopic, bool follower = false, int64_t rollBytes = 64LL * 1024 * 1024) : r(follower, rollBytes), topicText(text) {
        r.worker->setVerdictIntervalNs(1'000'000);
    }
    /// The judge and every live view caught up with the log.
    void settle() {
        EXPECT_TRUE(r.driveUntilJudgeReaches());
        for (const auto& [v, l] : live) r.driveUntilFolded(l.topicId, r.dataEnd());
    }
    void createTopic(bool autoIncrement) { r.createQlTopic(topicText, autoIncrement); r.enableAcks(); r.passes(20); }
    int32_t createConstrainedView(CV v, int64_t cap = 12, bool persisted = false) { return r.createConstrainedView(v, cap, persisted); }
    void replaceConstrainedView(CV v, int64_t cap, bool persisted = false) { r.replaceConstrainedView(v, cap, persisted); }
    void dropConstrainedView(CV v) { r.dropConstrainedView(v); }
    [[nodiscard]] const ViewInstance* instance(CV v) const { return r.constrainedInstance(v); }
    [[nodiscard]] size_t uniqueConflicts(CV v) const { return r.uniqueConflicts(v); }
    [[nodiscard]] static rv::ViewKey bytesKey(std::vector<uint8_t> b) { return rv::ViewKey::fromBytes(b); }
    [[nodiscard]] static std::vector<uint8_t> nameKey(const std::string& lowered, size_t width = 24) {
        std::vector<uint8_t> k(width, 0);
        std::copy(lowered.begin(), lowered.end(), k.begin());
        return k;
    }
    [[nodiscard]] const uint8_t* rowOf(CV v, const std::vector<uint8_t>& key) const {
        const ViewInstance* i = instance(v);
        if (i == nullptr) return nullptr;
        return i->table.get(bytesKey(key));
    }
    [[nodiscard]] int64_t column(CV v, const uint8_t* row, const char* col, int64_t absent = -1) const {
        if (row == nullptr) return absent;
        const auto& fs = live.at(v).comp.outputDescriptor.fields;
        for (size_t i = 0; i < fs.size(); ++i)
            if (fs[i].name == col) {
                const auto f = rv::readSlot(row, live.at(v).comp.rowLayout, static_cast<uint16_t>(i));
                return f.type == rune::codec::PrimType::Int64 ? f.i : static_cast<int64_t>(f.u);
            }
        throw std::runtime_error(std::string("no column ") + col);
    }
    [[nodiscard]] int64_t owner(const std::string& lowered) const { return column(CV::CharacterName, rowOf(CV::CharacterName, nameKey(lowered)), "characterId"); }
    [[nodiscard]] int64_t characters(uint64_t user) const {
        const ViewInstance* i = instance(CV::AccountStats);
        return column(CV::AccountStats, i == nullptr ? nullptr : i->table.get(user), "characters");
    }
};

using Char = VerdictRig::Char;
Char create(uint64_t user, const std::string& name) { return Char{.userId = user, .name = name, .intent = kInsert}; }
Char upsert(uint64_t id, uint64_t user, const std::string& name) { return Char{.id = id, .userId = user, .name = name}; }

/// An idempotent producer: every frame continues its sequence.
struct Prod {
    VerdictRig& r;
    int64_t pid;
    uint8_t acks = kAllView;
    int32_t seq = 0;
    int64_t send(const std::vector<Char>& chars) {
        const int64_t c = r.appendCharacters(chars, acks, pid, seq);
        seq += static_cast<int32_t>(chars.size());
        return c;
    }
    std::optional<VerdictRig::Ack> sendAck(const std::vector<Char>& chars) { return r.ack(send(chars)); }
};

/// `n` key-0 creates of `user` with distinct names.
std::vector<Char> creates(uint64_t user, int n, const std::string& prefix = "u") {
    std::vector<Char> out;
    for (int i = 0; i < n; ++i) out.push_back(create(user, prefix + std::to_string(user) + "n" + std::to_string(i)));
    return out;
}
}  // namespace

// §15: two logins race for the same identity.  The loser's rejection names the winner's record.
TEST(CheckedWritesConstraintWorker, LoginRaceLoserReadsTheWinner) {
    Cw c(kUserTopic);
    c.createTopic(true);
    const int32_t identity = c.createConstrainedView(CV::UserIdentity);
    c.r.passes(20);
    const auto login = [](const char* subject) {
        return std::vector<VerdictRig::SrcRecord>{{0, kInsert, false, {{"provider", 1}, {"createdAt", 5}}, {{"subject", subject}}}};
    };
    const int64_t c1 = c.r.appendRecords(login("steam:42"), kAllView, 11, 0);
    const int64_t c2 = c.r.appendRecords(login("steam:42"), kAllView, 12, 0);
    const auto a1 = c.r.ack(c1), a2 = c.r.ack(c2);
    ASSERT_TRUE(a1 && a2);
    const auto& win = a1->verdicts.empty() ? *a1 : *a2;
    const auto& lose = a1->verdicts.empty() ? *a2 : *a1;
    EXPECT_EQ(win.code, 0);
    EXPECT_NE(win.assignedBase, 0u);
    EXPECT_EQ(lose.code, 0);
    EXPECT_EQ(lose.verdicts, (V{{0, kViolation, static_cast<uint32_t>(objectIdOf(CV::UserIdentity) << 16)}}));
    std::vector<uint8_t> key{0, 1};
    std::string subject = "steam:42";
    subject.resize(64, '\0');
    key.insert(key.end(), subject.begin(), subject.end());
    const auto row = c.r.getViewRowK(identity, key, lose.baseOffset);
    ASSERT_EQ(row.code, pc::ViewRowResponse::kOk);
    const auto& comp = c.live.at(CV::UserIdentity).comp;
    uint16_t slot = 0;
    for (size_t i = 0; i < comp.outputDescriptor.fields.size(); ++i)
        if (comp.outputDescriptor.fields[i].name == "userId") slot = static_cast<uint16_t>(i);
    EXPECT_EQ(rv::readSlot(row.row.data(), comp.rowLayout, slot).u, win.assignedBase) << "the loser reads the winner's record on its first serve";
}

// §8.7 create, §15: the members of a view created over 500 records replay them unjudged; the writes of the build are never refused.
TEST(CheckedWritesConstraintWorker, CreateOnANonEmptyTopicJudgesFromTheChangeRecord) {
    Cw c;
    c.createTopic(false);
    for (uint64_t at = 1; at <= 500; at += 50) {
        std::vector<Char> chunk;
        for (uint64_t id = at; id < at + 50; ++id) {
            const uint64_t user = id >= 20 && id < 34 ? 7 : 100 + id;
            chunk.push_back(upsert(id, user, id == 5 ? "Dup" : id == 9 ? "dup" : "n" + std::to_string(id)));
        }
        const auto a = c.r.ack(c.r.appendCharacters(chunk, kLeader));
        ASSERT_TRUE(a && a->code == 0);
    }
    ASSERT_TRUE(c.r.driveUntilJudgeReaches());
    const int64_t sub = c.r.subscribe(SubKind::Raw, 0);
    const size_t changesBefore = c.r.readLog().changes.size();
    c.r.holdJudge(true);
    c.createConstrainedView(CV::CharacterName);
    c.createConstrainedView(CV::AccountStats);
    c.r.passes(30);
    const auto changes = c.r.readLog().changes;
    ASSERT_EQ(changes.size(), changesBefore + 2) << "one change per view";
    const int64_t A = changes.back().offset;
    for (uint64_t i = 0; i < 3; ++i) {
        const auto a = c.r.ack(c.r.appendCharacters(std::vector{upsert(501 + i, 900 + i, "w" + std::to_string(i))}, kLeader));
        ASSERT_TRUE(a);
        EXPECT_EQ(a->code, 0) << "a plain write during the build is acked and never refused";
    }
    int64_t above = 0, below = 0;
    for (const auto& d : c.r.collect(sub).records) (d.offset > A ? above : below)++;
    EXPECT_GT(below, 0) << "records below the change are delivered as soon as they commit";
    EXPECT_EQ(above, 0) << "records above it wait for the members";
    c.r.holdJudge(false);
    c.settle();
    for (const CV v : {CV::CharacterName, CV::AccountStats}) EXPECT_GE(c.instance(v)->foldCursor, A + 1) << "both members reached A + 1";
    for (const auto& d : c.r.collect(sub).records) above += d.offset > A;
    EXPECT_EQ(above, 3) << "the three writes of the build are delivered once both members judged them";
    EXPECT_EQ(c.owner("dup"), 5);
    EXPECT_EQ(c.uniqueConflicts(CV::CharacterName), 1u);
    EXPECT_EQ(c.characters(7), 14);
    Prod p{c.r, 41};
    const auto a = p.sendAck({upsert(600, 7, "extra")});
    ASSERT_TRUE(a);
    EXPECT_EQ(a->verdicts, (V{{0, kViolation, kStatsId}}));
}

// §8.7 replace, §15: the candidate builds while the old plan judges; the swap is one change record at S.  User 1 holds 12 characters,
// so a 13th create is refused below S (the old cap) and the first one above S is allowed (the new cap).
TEST(CheckedWritesConstraintWorker, ReplaceSwapsAtSWithoutAGap) {
    Cw c;
    c.createTopic(true);
    Prod seed{c.r, 40};
    for (uint64_t u = 1; u <= 600; u += 10) {
        std::vector<Char> batch;
        for (uint64_t user = u; user < u + 10; ++user)
            for (const Char& ch : creates(user, user == 1 ? 12 : 10)) batch.push_back(ch);
        ASSERT_TRUE(seed.sendAck(batch));
    }
    c.createConstrainedView(CV::AccountStats, 12, /*persisted=*/true);
    ASSERT_TRUE(c.r.driveUntilJudgeReaches());
    c.settle();
    ASSERT_EQ(c.characters(1), 12);
    const uint64_t obj = objectIdOf(CV::AccountStats);
    const auto changesBefore = c.r.readLog().changes.size();
    Prod writer{c.r, 42};
    c.replaceConstrainedView(CV::AccountStats, 13, /*persisted=*/true);
    int stall = 0, maxStall = 0, building = 0;
    int64_t last = c.r.vs->verdictSource(VerdictRig::kSrc, 0)->judgedThrough();
    std::vector<int64_t> probes;
    for (uint64_t i = 0; i < 4000 && c.r.stats()->candidateSwaps == 0; ++i) {
        probes.push_back(writer.send(creates(1, 1, "probe" + std::to_string(i) + "x")));
        c.r.pass();
        building += c.r.vs->candidateForTest(obj) ? 1 : 0;
        const int64_t j = c.r.vs->verdictSource(VerdictRig::kSrc, 0)->judgedThrough();
        stall = j > last || c.r.leo() <= j ? 0 : stall + 1;
        maxStall = std::max(maxStall, stall);
        last = j;
    }
    ASSERT_EQ(c.r.stats()->candidateSwaps, 1u);
    for (int i = 0; i < 3; ++i) probes.push_back(writer.send(creates(1, 1, "later" + std::to_string(i) + "x")));
    EXPECT_GE(building, 2) << "the replacement built as a candidate over several passes";
    EXPECT_LE(maxStall, 3) << "the judge never stalls for the build";
    EXPECT_TRUE(c.r.vs->verdictSource(VerdictRig::kSrc, 0)->retiringForTest(obj)) << "the old plan retires right after the swap";
    ASSERT_TRUE(c.r.driveUntilJudgeReaches());
    const auto changes = c.r.readLog().changes;
    ASSERT_EQ(changes.size(), changesBefore + 1) << "exactly one change record names the new fingerprint";
    EXPECT_EQ(changes.back().members.size(), 2u);
    const auto fp13 = c.r.fingerprintOf(CV::AccountStats, true);
    EXPECT_EQ(std::count_if(changes.back().members.begin(), changes.back().members.end(), [&](const auto& m) { return m.planFingerprint == fp13; }), 1);
    const int64_t S = changes.back().offset;
    int accepted = 0;
    for (const int64_t corr : probes) {
        const auto a = c.r.ack(corr);
        ASSERT_TRUE(a);
        if (a->verdicts.empty()) {
            ++accepted;
            EXPECT_GT(a->baseOffset, S) << "a 13th character below S was accepted";
        } else {
            EXPECT_EQ(a->verdicts, (V{{0, kViolation, kStatsId}}));
        }
    }
    EXPECT_EQ(accepted, 1) << "13 characters above S, then the cap holds again";
    c.settle();
    EXPECT_EQ(c.characters(1), 13);
}

// The 13th create rejected under 12 is in no member: the candidate never folds a rejected record.
TEST(CheckedWritesConstraintWorker, ReplaceCandidateNeverHoldsARejectedRecord) {
    Cw c;
    c.createTopic(true);
    c.createConstrainedView(CV::AccountStats, 12);
    Prod p{c.r, 41};
    ASSERT_TRUE(p.sendAck(creates(5, 12)));
    const auto a = p.sendAck(creates(5, 1, "thirteenth"));
    ASSERT_TRUE(a);
    ASSERT_EQ(a->verdicts, (V{{0, kViolation, kStatsId}}));
    c.settle();
    ASSERT_EQ(c.characters(5), 12);
    c.replaceConstrainedView(CV::AccountStats, 13);
    for (int i = 0; i < 200 && c.r.stats()->candidateSwaps == 0; ++i) c.r.pass();
    ASSERT_EQ(c.r.stats()->candidateSwaps, 1u);
    c.settle();
    EXPECT_EQ(c.characters(5), 12) << "the swapped-in member never folded the rejected record";
    ASSERT_TRUE(p.sendAck(creates(6, 1)));
    c.settle();
    EXPECT_EQ(c.characters(5), 12);
    const auto b = p.sendAck(creates(5, 1, "fresh"));
    ASSERT_TRUE(b);
    EXPECT_TRUE(b->verdicts.empty()) << "13 is allowed now";
    c.settle();
    EXPECT_EQ(c.characters(5), 13);
}

// §8.7 drop, §15: a drop stops judging above D; what was rejected below stays rejected.
TEST(CheckedWritesConstraintWorker, DropStopsJudgingAboveD) {
    Cw c;
    c.createTopic(true);
    c.createConstrainedView(CV::CharacterName);
    Prod p{c.r, 41};
    ASSERT_TRUE(p.sendAck({create(1, "Foo")}));
    const auto dup = p.sendAck({create(2, "foo")});
    ASSERT_TRUE(dup);
    ASSERT_EQ(dup->verdicts, (V{{0, kViolation, kNameId}}));
    const int64_t rejectedAt = dup->baseOffset;
    for (int i = 0; i < 5; ++i) (void)p.send(creates(3, 1, "bg" + std::to_string(i)));
    c.dropConstrainedView(CV::CharacterName);
    ASSERT_TRUE(c.r.driveUntilJudgeReaches());
    const auto after = p.sendAck({create(4, "foo")});
    ASSERT_TRUE(after);
    EXPECT_TRUE(after->verdicts.empty()) << "above D nothing judges the name";
    ASSERT_TRUE(c.r.driveUntilJudgeReaches());
    EXPECT_NE(c.r.writeState()->rejected.find(rejectedAt), nullptr) << "below D the rejection stays";
    const int64_t sub = c.r.subscribe(SubKind::Raw, 0);
    for (const auto& d : c.r.collect(sub).records) EXPECT_NE(d.offset, rejectedAt);
}

TEST(CheckedWritesConstraintWorker, DropThenCreateLeavesAGap) {
    Cw c;
    c.createTopic(true);
    c.createConstrainedView(CV::CharacterName);
    Prod p{c.r, 41};
    ASSERT_TRUE(p.sendAck({create(1, "Foo")}));
    c.dropConstrainedView(CV::CharacterName);
    const auto dup = p.sendAck({create(2, "foo")});
    ASSERT_TRUE(dup);
    EXPECT_TRUE(dup->verdicts.empty()) << "no view judges it";
    ASSERT_TRUE(c.r.driveUntilJudgeReaches());
    c.createConstrainedView(CV::CharacterName);
    c.settle();
    EXPECT_EQ(c.owner("foo"), 1) << "the smaller id owns the name";
    EXPECT_EQ(c.uniqueConflicts(CV::CharacterName), 1u);
    const auto again = p.sendAck({create(3, "FOO")});
    ASSERT_TRUE(again);
    EXPECT_EQ(again->verdicts, (V{{0, kViolation, kNameId}}));
}

// §15, P11-14: a constrained source takes a transaction; the vote judges it and the commit applies it.
TEST(CheckedWritesConstraintWorker, ConstrainedSourceAcceptsTransactions) {
    {
        Cw c;
        c.createTopic(true);
        c.createConstrainedView(CV::CharacterName);
        const auto a = c.r.ack(c.r.appendRecords(std::vector{VerdictRig::srcOf(create(1, "a"))}, kAllView, 77, 0, /*txn=*/true));
        ASSERT_TRUE(a);
        EXPECT_EQ(a->code, 0);
        const auto end = c.r.commitTxn(77);
        EXPECT_TRUE(end.committed);
        ASSERT_EQ(end.votes.size(), 1u);
        EXPECT_EQ(end.votes.front().second.code, 0);
        c.settle();
        EXPECT_EQ(c.owner("a"), static_cast<int64_t>(a->assignedBase));
        EXPECT_EQ(c.r.writeState()->txn.stats.markerWithoutVote, 0u);
    }
    Cw plain;
    plain.createTopic(true);
    const auto a = plain.r.ack(plain.r.appendRecords(std::vector{VerdictRig::srcOf(create(1, "a"))}, kAllView, 77, 0, /*txn=*/true));
    ASSERT_TRUE(a);
    EXPECT_EQ(a->code, 0) << "an unconstrained typed topic takes it as today";
}

// 03's KeyZeroCreateNeedsView for real: a key-0 create on a constrained source needs a View ack.
TEST(CheckedWritesConstraintWorker, KeyZeroCreateNeedsViewOnAConstrainedSource) {
    Cw c;
    c.createTopic(true);
    c.createConstrainedView(CV::CharacterName);
    const auto refused = c.r.ack(c.r.appendRecords(std::vector{VerdictRig::srcOf(create(1, "a"))}, kLeader, 41, 0));
    ASSERT_TRUE(refused);
    EXPECT_EQ(refused->code, kInvalidRequest);
    EXPECT_EQ(c.r.worker->hookRefusalsForTest(HookRefusal::KeyZeroCreateNeedsView), 1u);
    Prod p{c.r, 41};
    const auto ok = p.sendAck({create(1, "a")});
    ASSERT_TRUE(ok);
    EXPECT_EQ(ok->code, 0);
    EXPECT_EQ(ok->assignedBase, 1u);
}

// §11.2, P5-12: any producer's write is judged; the acks are today's.
TEST(CheckedWritesConstraintWorker, PlainWritesFromAnyProducerAreJudged) {
    Cw c(kPcTopic);
    c.createTopic(false);
    c.createConstrainedView(CV::PcName);
    c.r.passes(20);
    const auto write = [&](std::vector<VerdictRig::SrcRecord> recs) { return c.r.ack(c.r.appendRecords(recs, kLeader)); };
    const auto rec = [](uint64_t id, const char* name) { return VerdictRig::SrcRecord{id, 0, false, {{"userId", 1}}, {{"name", name}}}; };
    for (const auto& batch : {std::vector{rec(1, "Foo")}, std::vector{rec(2, "foo")}}) {
        const auto a = write(batch);
        ASSERT_TRUE(a);
        EXPECT_EQ(a->code, 0);
        EXPECT_FALSE(a->hasVerdicts) << "no trailer on an ack without View";
    }
    c.r.enableAcks(16);
    for (const auto& batch : {std::vector{rec(3, "Foo")}, std::vector{rec(4, "foo")}}) {
        const auto a = write(batch);
        ASSERT_TRUE(a);
        EXPECT_EQ(a->code, 0) << "a v11 session sees today's acks";
        EXPECT_FALSE(a->hasVerdicts);
    }
    ASSERT_TRUE(c.r.driveUntilJudgeReaches());
    EXPECT_EQ(c.r.stats()->rejectionsConstraint, 3u) << "every duplicate of the first Foo is judged, whoever wrote it";
    const int64_t sub = c.r.subscribe(SubKind::Raw, 0);
    std::vector<uint64_t> ids;
    for (const auto& d : c.r.collect(sub).records) ids.push_back(d.id);
    EXPECT_EQ(ids, std::vector<uint64_t>{1}) << "only the first Foo reaches a consumer";
    EXPECT_EQ(c.instance(CV::PcName)->table.size(), 1u);
}

TEST(CheckedWritesConstraintWorker, ViewAckCarriesPerRecordVerdicts) {
    Cw c;
    c.createTopic(true);
    c.createConstrainedView(CV::CharacterName);
    c.createConstrainedView(CV::AccountStats);
    Prod p{c.r, 41};
    ASSERT_TRUE(p.sendAck(creates(7, 12)));
    ASSERT_TRUE(p.sendAck({create(1, "taken")}));
    std::vector<Char> batch;
    for (int i = 0; i < 8; ++i) batch.push_back(create(100 + static_cast<uint64_t>(i), "b" + std::to_string(i)));
    batch[2].name = "Taken";
    batch[5].userId = 7;
    const auto a = p.sendAck(batch);
    ASSERT_TRUE(a);
    EXPECT_EQ(a->code, 0);
    EXPECT_EQ(a->verdicts, (V{{2, kViolation, kNameId}, {5, kViolation, kStatsId}}));
    const int64_t sub = c.r.subscribe(SubKind::Raw, 0);
    size_t visible = 0;
    for (const auto& d : c.r.collect(sub).records) visible += d.offset >= a->baseOffset && d.offset < a->baseOffset + 8;
    EXPECT_EQ(visible, 6u);
}

TEST(CheckedWritesConstraintWorkerRegression, ViewRetryReplaysTheOriginalConstraintIdsAfterAReplaceRegression) {
    Cw c;
    c.createTopic(true);
    c.createConstrainedView(CV::CharacterName);
    c.createConstrainedView(CV::AccountStats);
    Prod p{c.r, 41};
    ASSERT_TRUE(p.sendAck(creates(7, 12)));
    ASSERT_TRUE(p.sendAck({create(1, "taken")}));
    std::vector<Char> batch;
    for (int i = 0; i < 8; ++i) batch.push_back(create(100 + static_cast<uint64_t>(i), "b" + std::to_string(i)));
    batch[2].name = "Taken";
    batch[5].userId = 7;
    const int32_t seq = p.seq;
    const auto first = p.sendAck(batch);
    ASSERT_TRUE(first);
    ASSERT_EQ(first->verdicts, (V{{2, kViolation, kNameId}, {5, kViolation, kStatsId}}));
    c.replaceConstrainedView(CV::AccountStats, 13);
    for (int i = 0; i < 200 && c.r.stats()->candidateSwaps == 0; ++i) c.r.pass();
    ASSERT_EQ(c.r.stats()->candidateSwaps, 1u);
    ASSERT_TRUE(c.r.driveUntilJudgeReaches());
    const int64_t leo = c.r.leo();
    const uint64_t replays = c.r.stats()->verdictReplays;
    const auto retry = c.r.ack(c.r.appendCharacters(batch, kAllView, 41, seq));
    ASSERT_TRUE(retry);
    EXPECT_EQ(retry->baseOffset, first->baseOffset);
    EXPECT_EQ(retry->verdicts, first->verdicts) << "the original ids, not the replacement's verdicts";
    EXPECT_GT(c.r.writeState()->markers.committedThrough(), first->baseOffset + 7) << "released only once W is past the batch's last offset";
    EXPECT_EQ(c.r.stats()->verdictReplays, replays + 1);
    EXPECT_EQ(c.r.leo(), leo);
}

TEST(CheckedWritesConstraintWorkerRegression, PreExistingDuplicatesSurviveCompactionAndRefoldRegression) {
    Cw c(VerdictRig::kCharacterTopic, false, /*rollBytes=*/1);
    c.createTopic(true);
    Prod p{c.r, 41};
    std::vector<Char> dups;
    for (int id = 1; id <= 12; ++id) dups.push_back(create(static_cast<uint64_t>(id), id == 5 ? "Dup" : id == 9 ? "dup" : id == 12 ? "DUP" : "n" + std::to_string(id)));
    for (const Char& ch : dups) ASSERT_TRUE(p.sendAck({ch}));
    c.createConstrainedView(CV::CharacterName, 12, /*persisted=*/true);
    c.settle();
    EXPECT_EQ(c.owner("dup"), 5);
    EXPECT_EQ(c.uniqueConflicts(CV::CharacterName), 2u);
    const auto del = p.sendAck({Char{.id = 5, .tombstone = true, .intent = 0}});
    ASSERT_TRUE(del);
    c.settle();
    EXPECT_EQ(c.owner("dup"), 9);
    EXPECT_EQ(c.uniqueConflicts(CV::CharacterName), 1u);
    for (int i = 0; i < 8; ++i) (void)c.r.compact(1'000'000 + i * 10'000);
    const std::string file = c.r.snapDir() + "/view-" + std::to_string(c.live.at(CV::CharacterName).topicId) + "-p0.committed.delta";
    std::error_code ec;
    std::filesystem::remove(file, ec);
    c.r.restart();
    for (int i = 0; i < 400; ++i) c.r.pass();
    c.settle();
    EXPECT_EQ(c.owner("dup"), 9) << "the full refold of the compacted log";
    EXPECT_EQ(c.uniqueConflicts(CV::CharacterName), 1u);
    c.r.failover();
    for (int i = 0; i < 400; ++i) c.r.pass();
    c.settle();
    EXPECT_EQ(c.owner("dup"), 9);
    EXPECT_EQ(c.uniqueConflicts(CV::CharacterName), 1u);
}

// §15 through the worker: a delete never fails, an over-limit balance keeps failing until it repairs.
TEST(CheckedWritesConstraintWorker, LedgerDeleteLeavesItFailingThenOnlyRepairsPass) {
    Cw c(VerdictRig::kLedgerTopic);
    c.r.createLedgerTopic();
    c.r.enableAcks();
    c.r.passes(20);
    c.createConstrainedView(CV::Ledger);
    int32_t seq = 0;
    using Entry = VerdictRig::Entry;
    const auto put = [&](Entry e) {
        const int64_t k = c.r.appendEntries(std::vector{std::move(e)}, kAllView, 41, seq++);
        return c.r.ack(k);
    };
    const auto entry = [](uint64_t id, int64_t amount) { return Entry{.id = id, .account = 1, .amount = amount}; };
    const uint32_t cid = (objectIdOf(CV::Ledger) << 16) | 1;
    const V ok{}, bad{{0, kViolation, cid}};
    const auto verdicts = [&](Entry r) { const auto a = put(std::move(r)); EXPECT_TRUE(a); return a ? a->verdicts : V{{9, 9, 9}}; };
    EXPECT_EQ(verdicts(entry(1, 10)), ok);
    EXPECT_EQ(verdicts(entry(2, -4)), ok);
    EXPECT_EQ(verdicts(Entry{.id = 1, .tombstone = true}), ok) << "a delete always passes";
    EXPECT_EQ(verdicts(entry(3, -1)), bad);
    EXPECT_EQ(verdicts(entry(4, 2)), bad) << "-2 still fails and the balance changed";
    EXPECT_EQ(verdicts(entry(5, 5)), ok);
    EXPECT_EQ(verdicts(Entry{.id = 2, .account = 1, .amount = -4, .memo = "memo"}), ok);
    ASSERT_TRUE(c.r.driveUntilJudgeReaches());
    const int64_t sub = c.r.subscribe(SubKind::Raw, 0);
    std::vector<uint64_t> ids;
    for (const auto& d : c.r.collect(sub).records) ids.push_back(d.id);
    EXPECT_EQ(std::count(ids.begin(), ids.end(), 3u), 0) << "a consumer never sees the rejected entries";
    EXPECT_EQ(std::count(ids.begin(), ids.end(), 4u), 0);
}

// R08f: stat 32 is the unique view's gauge and is served first.
TEST(CheckedWritesConstraintWorker, UniqueConflictsStatIsServed) {
    Cw c;
    c.createTopic(false);
    const auto put = [&](uint64_t id, const char* n, bool tomb = false) {
        const int64_t k = tomb ? c.r.appendCharacters(std::vector{Char{.id = id, .tombstone = true}}, kAllView, 50, 0)
                               : c.r.appendCharacters(std::vector{Char{.id = id, .userId = id, .name = n}}, kLeader);
        EXPECT_TRUE(c.r.ack(k));
    };
    put(5, "Dup");
    put(9, "dup");
    const int32_t name = c.createConstrainedView(CV::CharacterName);
    const int32_t stats = c.createConstrainedView(CV::AccountStats);
    c.settle();
    const auto ids = [](const VerdictRig::StatsReply& s) { std::vector<uint16_t> out; for (const auto& e : s.stats) out.push_back(e.first); return out; };
    const auto before = c.r.getStats(pc::StatsScope::ViewPartition, name, 0);
    ASSERT_EQ(before.status, 0);
    EXPECT_EQ(ids(before), (std::vector<uint16_t>{32, 33, 34, 35, 36}));
    EXPECT_EQ(before.stats.front().second, 1);
    put(5, "Dup", true);
    c.settle();
    EXPECT_EQ(c.r.getStats(pc::StatsScope::ViewPartition, name, 0).stats.front().second, 0);
    const auto other = ids(c.r.getStats(pc::StatsScope::ViewPartition, stats, 0));
    EXPECT_EQ(std::count(other.begin(), other.end(), 32), 0) << "a view without claims has no stat 32";
}

TEST(CheckedWritesConstraintWorker, TopicStatsListEveryConstraint) {
    Cw c;
    c.createTopic(true);
    c.createConstrainedView(CV::CharacterName);
    c.createConstrainedView(CV::AccountStats);
    Prod p{c.r, 41};
    ASSERT_TRUE(p.sendAck(creates(7, 12)));
    ASSERT_TRUE(p.sendAck({create(8, "u7n0")}));   // the name is taken
    ASSERT_TRUE(p.sendAck({create(7, "fresh")}));   // the 13th character of user 7
    ASSERT_TRUE(c.r.driveUntilJudgeReaches());
    const auto s = c.r.getStats(pc::StatsScope::TopicPartition, VerdictRig::kSrc, 0);
    ASSERT_EQ(s.status, 0);
    const auto rejections = std::find_if(s.stats.begin(), s.stats.end(), [](const auto& e) { return e.first == 3; });
    ASSERT_NE(rejections, s.stats.end());
    EXPECT_EQ(rejections->second, 2);
    auto constraints = s.constraints;
    std::sort(constraints.begin(), constraints.end());
    EXPECT_EQ(constraints, (std::vector<std::pair<uint32_t, uint64_t>>{{kNameId, 1}, {kStatsId, 1}}));
}

// P9-5 (#788 on the candidate path): a leader whose catalog is behind the logged change builds no candidate and swaps none.
TEST(CheckedWritesConstraintWorkerRegression, ReplaceBehindTheCatalogBuildsNoCandidateRegression) {
    constexpr int64_t kAhead = 50;   // the catalog position of the change this node has not applied yet
    Cw c;
    c.createTopic(true);
    c.createConstrainedView(CV::AccountStats, 12);
    c.settle();
    const uint64_t obj = objectIdOf(CV::AccountStats);
    c.r.demote();
    c.r.replicate({VerdictRig::changeBatch(c.r.writeState()->constraintSets.latest().id + 1, c.r.readLog().changes.back().members, kAhead)});
    c.r.promote();
    c.r.passes(100);
    const size_t changes = c.r.readLog().changes.size();
    c.replaceConstrainedView(CV::AccountStats, 13);
    c.r.passes(300);
    EXPECT_FALSE(c.r.vs->candidateForTest(obj)) << "a candidate was built behind the catalog";
    EXPECT_EQ(c.r.stats()->candidateSwaps, 0u);
    EXPECT_EQ(c.r.readLog().changes.size(), changes) << "a set was logged over a change the catalog has not applied";
    c.r.catalogMark(kAhead);
    for (int i = 0; i < 400 && c.r.stats()->candidateSwaps == 0; ++i) c.r.pass();
    EXPECT_EQ(c.r.stats()->candidateSwaps, 1u) << "the mark opened the gate and the replacement swapped in";
    EXPECT_EQ(c.r.readLog().changes.size(), changes + 1);
}

/// AccountStats 12 -> 13 with the judge stopped the pass after its candidate caught up: the swap is logged at S, which the judge has not
/// passed.  Returns S.
int64_t replaceUntilSwapIsLogged(Cw& c) {
    const uint64_t obj = objectIdOf(CV::AccountStats);
    const VerdictSource* judge = c.r.vs->verdictSource(VerdictRig::kSrc, 0);
    const size_t changes = c.r.readLog().changes.size();
    c.r.holdJudge(true);
    c.replaceConstrainedView(CV::AccountStats, 13);
    const uint64_t fp13 = c.r.fingerprintOf(CV::AccountStats, false);
    c.r.holdJudge(false);
    for (int i = 0; i < 400 && !judge->candidateCaughtUp(obj, fp13); ++i) c.r.pass();
    EXPECT_TRUE(judge->candidateCaughtUp(obj, fp13));
    c.r.holdJudge(true);
    for (int i = 0; i < 20 && c.r.readLog().changes.size() == changes; ++i) c.r.pass();
    EXPECT_EQ(c.r.readLog().changes.size(), changes + 1) << "the swap was logged";
    const int64_t S = c.r.readLog().changes.back().offset;
    EXPECT_LE(judge->judgedThrough(), S) << "the judge has not passed it";
    return S;
}

// A drop that reaches the worker after the swap was logged but before the judge passed it: the judge still switches to the
// replacement at S (the dropped view judges until D) and then to the empty set at D.
TEST(CheckedWritesConstraintWorkerRegression, DropBeforeTheJudgePassesTheSwapKeepsJudgingRegression) {
    Cw c;
    c.createTopic(true);
    c.createConstrainedView(CV::AccountStats, 12);
    Prod p{c.r, 41};
    ASSERT_TRUE(p.sendAck(creates(5, 12)));
    c.settle();
    replaceUntilSwapIsLogged(c);
    c.dropConstrainedView(CV::AccountStats);
    c.r.passes(20);
    c.r.holdJudge(false);
    EXPECT_TRUE(c.r.driveUntilJudgeReaches()) << "the judge passed S and D";
    EXPECT_FALSE(c.r.vs->verdictSource(VerdictRig::kSrc, 0)->waitingForInstall()) << "the judge waits for the replacement the drop removed";
    const auto a = p.sendAck(creates(5, 1, "after"));
    ASSERT_TRUE(a);
    EXPECT_TRUE(a->verdicts.empty()) << "no member judges above D";
}

// The swap was logged, another leader judged past it, and this node leads again: the replacement is then the member the replicated
// markers say it is, not a candidate, so a later drop removes the view like any other.
TEST(CheckedWritesConstraintWorkerRegression, CandidateIsAMemberWhenTheNodeLeadsPastItsSwapRegression) {
    Cw c;
    c.createTopic(true);
    c.createConstrainedView(CV::AccountStats, 12);
    Prod p{c.r, 41};
    ASSERT_TRUE(p.sendAck(creates(5, 3)));
    c.settle();
    const uint64_t obj = objectIdOf(CV::AccountStats);
    const int64_t S = replaceUntilSwapIsLogged(c);
    const uint32_t setId = c.r.writeState()->constraintSets.latest().id;
    const int64_t covered = c.r.writeState()->rejected.coveredThrough();
    c.r.demote();
    c.r.replicate({VerdictRig::markerBatch(covered, S + 1, setId)});
    c.r.holdJudge(false);
    c.r.promote();
    c.r.passes(100);
    EXPECT_FALSE(c.r.vs->candidateForTest(obj)) << "the replacement judges since the markers covered its change";
    c.dropConstrainedView(CV::AccountStats);
    c.r.passes(50);
    EXPECT_TRUE(c.r.driveUntilJudgeReaches()) << "the drop removed an active member the judge could not do without";
    EXPECT_FALSE(c.r.vs->verdictSource(VerdictRig::kSrc, 0)->waitingForInstall());
}
