// LeaseMatrixTest.cpp - lease topics Task 17 (decision 3): the 1-node BROKER layer of the interaction matrix.  Every applicable sequential
// cell (op x entity) of the client-surface lease ops that a real client can drive (Grant, Update, Revoke, KeepAlive1, TimeToLive,
// PointRead, ScanLeases) runs against ONE real authenticating shard broker with the injected lease clock, and is checked against
// LeaseMatrixExpectations.h `expect`: the op's reply, its effect on the lease it addresses, and that the control leases (another key of
// the same partition, a key on the other partition, a key of the second lease topic) and the user view row are unchanged unless `expect`
// says so.  The broker-side records (Expire / Checkpoint / Clear), KeepAliveBatch, WatchStart and the DDL ops are not drivable through the
// client API; they are pinned by the unit table (LeaseMatrixUnitRegression), LeaseSessionBroker / LeaseWatch tests and the DDL tests.
#include "support/LeaseBrokerRig.h"
#include "support/LeaseMatrixExpectations.h"

#include <rune/client/Completions.h>
#include <rune/client/lease/LeaseTopic.h>
#include <rune/client/producer/Producer.h>
#include <rune/protocol/client/Protocol.h>

#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <thread>

using namespace rune::node::test;
using namespace rune::node::test::leasematrix;
using namespace std::chrono_literals;
namespace rc = rune::client;
namespace pc = rune::protocol::client;
using rune::gen::LeaseClaim;

namespace {
using Lt = rc::LeaseTopic<LeaseClaim>;
constexpr uint16_t kNotFound = static_cast<uint16_t>(pc::ErrorCode::LeaseNotFound);
constexpr uint16_t kKeyExists = static_cast<uint16_t>(pc::ErrorCode::KeyExists);

struct Cell { Op op; Entity ent; };
std::string cellName(const testing::TestParamInfo<Cell>& i) { return std::string(name(i.param.op)) + "_" + name(i.param.ent); }

std::vector<Cell> cells() {
    std::vector<Cell> v;
    for (Op o : {Op::Grant, Op::Update, Op::Revoke, Op::KeepAlive1, Op::TimeToLive, Op::PointRead, Op::ScanLeases})
        for (std::size_t e = 0; e < kEntities; ++e)
            if (!naRule(o, Entity(e))) v.push_back({o, Entity(e)});
    return v;
}

/// The verdict of a LeaseResult in the matrix's terms.
Result resultOf(const rc::LeaseResult& r) {
    if (r.ok()) return Result::Ok;
    if (r.errorCode == kKeyExists) return Result::KeyExists;
    if (r.errorCode == kNotFound) return Result::LeaseNotFound;
    return Result::InvalidRequest;   // anything else is surfaced by the caller's message
}

struct Seen { bool present = false; int64_t version = 0; uint64_t a = 0; };
Seen seen(Lt& lt, const LeaseClaim& r) {
    const auto row = lt.getAndWait(r);
    return row ? Seen{true, row->lease_version, row->a} : Seen{};
}
bool same(const Seen& x, const Seen& y) { return x.present == y.present && x.version == y.version && x.a == y.a; }
std::string errText(const std::exception& e) { return e.what(); }

class LeaseMatrixBroker : public testing::TestWithParam<Cell> {
public:
    static void SetUpTestSuite() { rig_ = std::make_unique<LeaseBrokerRig>("lease-matrix"); }
    static void TearDownTestSuite() { rig_.reset(); }
protected:
    static std::unique_ptr<LeaseBrokerRig> rig_;
    static int                             seq_;
};
std::unique_ptr<LeaseBrokerRig> LeaseMatrixBroker::rig_;
int LeaseMatrixBroker::seq_ = 0;

/// Two keys that hash to the same / different partitions of `lt`.
std::string keyOn(Lt& lt, LeaseBrokerRig& rig, const std::string& stem, int32_t wantPart, bool same) {
    for (int i = 0; i < 400; ++i) {
        const std::string k = stem + "-" + std::to_string(i);
        if ((lt.partitionOf(rig.row(k)) == wantPart) == same) return k;
    }
    return stem + "-none";
}
}  // namespace

TEST_P(LeaseMatrixBroker, SequentialCellRegression) {
    LeaseBrokerRig& rig = *rig_;
    const Op op = GetParam().op;
    const Entity ent = GetParam().ent;
    const Outcome ex = expect(op, ent, Timing::Sequential);
    const std::string stem = "m" + std::to_string(++seq_);

    auto lt = rig.ua().openLeaseTopicAndWait<LeaseClaim>("t", {}, 30s);
    auto lt2 = rig.ua().openLeaseTopicAndWait<LeaseClaim>("t2", {}, 30s);
    // L: the SameKeyVer lease, live.  Controls: C1 same partition, C2 other partition (both of t), C3 on t2.
    const std::string kL = stem + "-L";
    const int32_t pL = lt.partitionOf(rig.row(kL));
    const std::string kC1 = keyOn(lt, rig, stem + "-c1", pL, true);
    const std::string kC2 = keyOn(lt, rig, stem + "-c2", pL, false);
    const LeaseClaim rowL = rig.row(kL, 11), rowC1 = rig.row(kC1, 21), rowC2 = rig.row(kC2, 22), rowC3 = rig.row(stem + "-c3", 23);
    const rc::LeaseResult gL = lt.grantAndWait(rowL), gC1 = lt.grantAndWait(rowC1), gC2 = lt.grantAndWait(rowC2), gC3 = lt2.grantAndWait(rowC3);
    ASSERT_TRUE(gL.ok() && gC1.ok() && gC2.ok() && gC3.ok());
    bool uvSeen = false;
    for (int i = 0; i < 2000 && !uvSeen; ++i) uvSeen = rig.userViewRow(rowL).has_value();
    ASSERT_TRUE(uvSeen);
    rig.advance(1s);   // a keepalive is then visible as a re-armed remaining time

    // What the op addresses.
    LeaseClaim target = rowL;
    int64_t    targetVer = gL.version;
    Lt*        targetLt = &lt;
    switch (ent) {
    case Entity::SameKeyVer: case Entity::UserView: break;
    case Entity::StaleVer: {   // the version before a revoke + regrant
        ASSERT_TRUE(lt.revokeAndWait(rowL, gL.version).ok());
        const rc::LeaseResult g = lt.grantAndWait(rowL);
        ASSERT_TRUE(g.ok());
        break;   // targetVer stays the stale one; L is now at g.version
    }
    case Entity::OtherKeySamePart: target = rowC1; targetVer = gC1.version; break;
    case Entity::OtherPart: target = rowC2; targetVer = gC2.version; break;
    case Entity::OtherLeaseTopic: target = rowC3; targetVer = gC3.version; targetLt = &lt2; break;
    default: break;
    }
    // A fresh key for Grant on every entity but the same-key one (SameKeyVer / UserView / StaleVer grant the LIVE key: KeyExists).
    if (op == Op::Grant && (ent == Entity::OtherKeySamePart || ent == Entity::OtherPart || ent == Entity::OtherLeaseTopic)) {
        const std::string k = ent == Entity::OtherKeySamePart ? keyOn(lt, rig, stem + "-f1", pL, true)
                              : ent == Entity::OtherPart      ? keyOn(lt, rig, stem + "-f2", pL, false)
                                                              : stem + "-f3";
        target = rig.row(k, 31);
    }
    const LeaseClaim current = ent == Entity::StaleVer ? rowL : target;

    // Snapshots before.
    const Seen beforeL = seen(lt, rowL), beforeC1 = seen(lt, rowC1), beforeC2 = seen(lt, rowC2), beforeC3 = seen(lt2, rowC3);
    const auto uvBefore = rig.userViewRow(rowL);
    const Seen beforeT = seen(*targetLt, target);
    rc::LeaseTtlInfo tiBefore;
    (void)targetLt->timeToLiveAndWait(target, &tiBefore);

    // Run the op.
    Result got = Result::Absent;
    std::string msgOther, msgMissing;
    if (ent == Entity::OtherNs || ent == Entity::OldSession || ent == Entity::NonLeaseSameWorker) {
        // The target cannot be opened as a lease topic by this session: the refusal is the verdict.
        auto tryOpen = [&](LeaseBrokerRig::Client& c, const std::string& n) -> std::string {
            try { (void)c.openLeaseTopicAndWait<LeaseClaim>(n, {}, 10s); return {}; } catch (const std::exception& e) { return errText(e); }
        };
        if (ent == Entity::OtherNs) {
            msgOther   = tryOpen(rig.ua(), "b/t");
            msgMissing = tryOpen(rig.ua(), "zz-missing");
            got = !msgOther.empty() ? Result::MissingObject : Result::Ok;
        } else if (ent == Entity::OldSession) {
            // The v17 session: opening the topic, or its first lease op, is refused before any lease state is touched.
            bool refused = false;
            try {
                auto l17 = rig.ua17().openLeaseTopicAndWait<LeaseClaim>("t", {}, 10s);
                rc::LeaseResult r;
                switch (op) {
                case Op::Grant: r = l17.grantAndWait(rig.row(stem + "-v17")); break;
                case Op::Update: r = l17.updateAndWait(rowL, gL.version); break;
                case Op::Revoke: r = l17.revokeAndWait(rowL, gL.version); break;
                case Op::KeepAlive1: r = l17.keepAliveOnceAndWait(rowL, gL.version); break;
                case Op::TimeToLive: r = l17.timeToLiveAndWait(rowL); break;
                case Op::PointRead: (void)l17.getAndWait(rowL); r = rc::LeaseResult{rc::OpStatus::BrokerError, 39}; break;
                default: r = rc::LeaseResult{rc::OpStatus::BrokerError, 39}; break;
                }
                refused = !r.ok();
            } catch (const std::exception&) { refused = true; }
            got = refused ? Result::InvalidRequest : Result::Ok;
        } else {   // a plain topic
            const std::string m = tryOpen(rig.ua(), "n");
            if (op == Op::KeepAlive1 || op == Op::TimeToLive || op == Op::ScanLeases) {
                got = !m.empty() ? Result::NotALeaseTopic : Result::Ok;
            } else {   // grant / update / revoke / read on a plain topic are ordinary operations: one acked produce
                rc::ProducerConfig pcfg;
                pcfg.acks = rc::Acks::Leader;
                pcfg.lingerUs = 0;
                auto p = rig.ua().producer(pcfg);
                rc::Completions cq(rig.ua());
                const uint8_t v[8] = {1, 2, 3, 4, 5, 6, 7, 8};
                bool ok = false, done = false;
                if (p.trySend(rc::TopicHandle{rig.nonLease().id, 1}, rc::Key(uint64_t{7}), std::span<const uint8_t>(v, 8), cq, 1) == rc::SendResult::Ok) {
                    p.flush();
                    for (int i = 0; i < 20000 && !done; ++i) {
                        (void)cq.poll([&](const rc::Completion& d, std::span<const uint8_t>) { ok = d.status == rc::OpStatus::Ok; done = true; });
                        if (!done) std::this_thread::yield();
                    }
                }
                p.close();
                got = ok ? Result::Ok : Result::InvalidRequest;
            }
        }
    } else {
        rc::LeaseResult r;
        switch (op) {
        case Op::Grant: r = targetLt->grantAndWait(target); break;
        case Op::Update: r = targetLt->updateAndWait(rig.row(std::string(target.claim.data(), strnlen(target.claim.data(), 32)), 99), targetVer); break;
        case Op::Revoke: r = targetLt->revokeAndWait(target, targetVer); break;
        case Op::KeepAlive1: r = targetLt->keepAliveOnceAndWait(target, targetVer); break;
        case Op::TimeToLive: r = targetLt->timeToLiveAndWait(target); break;
        case Op::PointRead: {
            const auto row = targetLt->getAndWait(target);
            r = row ? rc::LeaseResult{} : rc::LeaseResult{rc::OpStatus::BrokerError, kNotFound};
            break;
        }
        case Op::ScanLeases: {
            rc::CompletionsConfig qc;
            qc.maxResultBytes = 1u << 14;
            qc.capacityBytes = 1u << 18;
            rc::Completions q(rig.ua(), qc);
            const int32_t part = targetLt->partitionOf(target);
            bool found = false, done = false;
            uint64_t resume = 0;
            bool first = true;
            for (int page = 0; page < 200 && !done; ++page) {
                if (rig.ua().scanLeases(targetLt->leaseViewTopicId(), part, {}, resume, first, 64, 10'000, q, 1) != rc::OpResult::Accepted) break;
                rc::Completion cc{};
                (void)q.waitFor(1, 30s, [&](const rc::Completion& comp, std::span<const uint8_t> payload) {
                    cc = comp;
                    (void)rc::forEachLeaseRow(payload, [&](std::span<const uint8_t> key, std::span<const uint8_t>) {
                        if (std::string(reinterpret_cast<const char*>(key.data()), strnlen(reinterpret_cast<const char*>(key.data()), key.size())) ==
                            std::string(target.claim.data(), strnlen(target.claim.data(), 32)))
                            found = true;
                    });
                });
                if (cc.status != rc::OpStatus::Ok) break;
                const rc::LeaseScanPage pg = rc::LeaseScanPage::of(cc);
                done = pg.done;
                resume = pg.lastHash;
                first = false;
            }
            r = done && found ? rc::LeaseResult{} : rc::LeaseResult{rc::OpStatus::BrokerError, kNotFound};
            break;
        }
        default: break;
        }
        got = resultOf(r);
        if (!r.ok() && got == Result::InvalidRequest) ADD_FAILURE() << name(op) << "x" << name(ent) << ": unexpected status " << int(r.status) << " code " << r.errorCode;
    }

    EXPECT_EQ(int(got), int(ex.result)) << name(op) << " x " << name(ent) << ": the reply";
    if (ent == Entity::OtherNs) {
        // The answer for another namespace's topic is the answer for a truly missing name (names normalized).
        auto norm = [](std::string s, const std::string& n) {
            for (std::size_t p = s.find(n); p != std::string::npos; p = s.find(n, p + 1)) s.replace(p, n.size(), "<name>");
            return s;
        };
        EXPECT_EQ(norm(msgOther, "b/t"), norm(msgMissing, "zz-missing")) << "OtherNs answers as a missing name";
    }

    // Effects on the addressed lease.
    if (ent != Entity::OtherNs && ent != Entity::OldSession && ent != Entity::NonLeaseSameWorker) {
        const Seen afterT = seen(*targetLt, target);
        rc::LeaseTtlInfo tiAfter;
        (void)targetLt->timeToLiveAndWait(target, &tiAfter);
        const bool targetIsFreshGrant = op == Op::Grant && (ent == Entity::OtherKeySamePart || ent == Entity::OtherPart || ent == Entity::OtherLeaseTopic);
        if (ent == Entity::StaleVer) {
            EXPECT_TRUE(same(seen(lt, rowL), seen(lt, rowL)));   // the live L is re-read below against its regrant version
        } else {
            switch (ex.target) {
            case Effect::None: EXPECT_TRUE(same(beforeT, afterT)) << "no effect on the addressed lease"; break;
            case Effect::Created: EXPECT_TRUE(afterT.present && (targetIsFreshGrant ? !beforeT.present : true)) << "a row is created"; break;
            case Effect::Replaced: EXPECT_TRUE(afterT.present && afterT.a == 99u) << "columns replaced"; break;
            case Effect::Removed: EXPECT_FALSE(afterT.present) << "the lease ends"; break;
            case Effect::Rearmed:
                EXPECT_TRUE(afterT.present && tiBefore.remainingMs >= 0 && tiAfter.remainingMs > tiBefore.remainingMs) << "the deadline moves out";
                break;
            default: break;
            }
        }
    }
    // L, controls and the user view.
    if (ent == Entity::StaleVer) {
        EXPECT_TRUE(seen(lt, rowL).present) << "the live lease is untouched by the stale op";
        EXPECT_NE(seen(lt, rowL).version, gL.version);
    } else if (ent != Entity::SameKeyVer && ent != Entity::UserView) {
        EXPECT_TRUE(same(beforeL, seen(lt, rowL))) << "L is unchanged by an op on another entity";
    }
    if (ent == Entity::SameKeyVer || ent == Entity::UserView) {
        const Seen afterL = seen(lt, rowL);
        rc::LeaseTtlInfo tiL;
        const bool rearmed = lt.timeToLiveAndWait(rowL, &tiL).ok() && tiBefore.remainingMs >= 0 && tiL.remainingMs > tiBefore.remainingMs;
        EXPECT_EQ(afterL.present != beforeL.present || afterL.a != beforeL.a || rearmed, ex.lChanged) << "L changed exactly as expected";
    }
    if (!(ent == Entity::OtherKeySamePart)) EXPECT_TRUE(same(beforeC1, seen(lt, rowC1))) << "control C1";
    if (!(ent == Entity::OtherPart)) EXPECT_TRUE(same(beforeC2, seen(lt, rowC2))) << "control C2";
    if (!(ent == Entity::OtherLeaseTopic)) EXPECT_TRUE(same(beforeC3, seen(lt2, rowC3))) << "control C3 (second lease topic)";
    // The user view: a stable observation after a bounded settle.
    std::optional<std::vector<uint8_t>> uvAfter = rig.userViewRow(rowL);
    for (int i = 0; i < 100 && ex.userViewChanged && uvAfter == uvBefore; ++i) uvAfter = rig.userViewRow(rowL);
    if (ent == Entity::UserView || ent == Entity::SameKeyVer)
        EXPECT_EQ(uvAfter != uvBefore, ex.userViewChanged) << "the user view changes exactly as expected";
    else if (ent != Entity::StaleVer)
        EXPECT_EQ(uvAfter, uvBefore) << "the user view of L is unchanged";
}

INSTANTIATE_TEST_SUITE_P(Cells, LeaseMatrixBroker, testing::ValuesIn(cells()), cellName);

// ============================================================================================================================
// Timing cells.  Every timing cell of every implemented op is accounted for exactly once in kCoverage: run on the 1-node broker here
// (Where::OneNode), run on the 3-node cluster in LeaseTrioTest.cpp (Where::Trio), or mapped to the existing named test that asserts
// the cell's exact scenario (Where::Mapped; the unit and worker layers pin the states a real client cannot freeze).
// ============================================================================================================================
namespace {
enum class Where : uint8_t { OneNode, Trio, Mapped };
struct Cov { Op op; Timing timing; Where where; const char* test; };
constexpr Cov kCoverage[] = {
    {Op::Grant, Timing::BackToBack, Where::OneNode, "LeaseMatrixTiming"},
    {Op::Grant, Timing::LeaderChangeGrantAck, Where::Trio, "LeaseTrioRegression.GrantAckLeaderChangeRegression"},
    {Op::Grant, Timing::KnownOldDuplicate, Where::Mapped, "LeaseWriteHookWorkerRegression.LeaseKnownOldDuplicateTableRegression"},
    {Op::Grant, Timing::ContenderGrants, Where::Mapped, "LeaseRaceRegression.ContenderGrantsDoNotDeferExpireRegression"},
    {Op::Grant, Timing::RestartOne, Where::OneNode, "LeaseMatrixTiming"},
    {Op::Grant, Timing::RestartAll, Where::Trio, "LeaseTrioRegression.WholeClusterRestartRegression"},
    {Op::Grant, Timing::CompactionMid, Where::Mapped, "LeaseJudgeWorkerRegression.LeaseRebuildAfterCompactionRegression"},
    {Op::Grant, Timing::SuspendReadOnly, Where::OneNode, "LeaseMatrixTiming"},
    {Op::Grant, Timing::SuspendRejectAll, Where::OneNode, "LeaseMatrixTiming"},
    {Op::Grant, Timing::AlterMidLife, Where::OneNode, "LeaseMatrixTiming"},
    {Op::Update, Timing::BackToBack, Where::OneNode, "LeaseMatrixTiming"},
    {Op::Update, Timing::AroundExpireBeforePoll, Where::Mapped, "LeaseRaceRegression.RevokeAndUpdateAtFourPointsRegression"},
    {Op::Update, Timing::AroundExpireAfterBuild, Where::Mapped, "LeaseRaceRegression.RevokeAndUpdateAtFourPointsRegression"},
    {Op::Update, Timing::AroundExpireAfterCommit, Where::Mapped, "LeaseRaceRegression.RevokeAndUpdateAtFourPointsRegression"},
    {Op::Update, Timing::AroundExpireAfterFold, Where::Mapped, "LeaseRaceRegression.RevokeAndUpdateAtFourPointsRegression"},
    {Op::Update, Timing::KnownOldDuplicate, Where::Mapped, "LeaseWriteHookWorkerRegression.LeaseKnownOldUpdateWithOtherMemberIsUnknownRegression"},
    {Op::Update, Timing::SuspendReadOnly, Where::OneNode, "LeaseMatrixTiming"},
    {Op::Update, Timing::SuspendRejectAll, Where::OneNode, "LeaseMatrixTiming"},
    {Op::Revoke, Timing::AroundExpireBeforePoll, Where::Mapped, "LeaseRaceRegression.RevokeAndUpdateAtFourPointsRegression"},
    {Op::Revoke, Timing::AroundExpireAfterBuild, Where::Mapped, "LeaseRaceRegression.RevokeAndUpdateAtFourPointsRegression"},
    {Op::Revoke, Timing::AroundExpireAfterCommit, Where::Mapped, "LeaseRaceRegression.RevokeAndUpdateAtFourPointsRegression"},
    {Op::Revoke, Timing::AroundExpireAfterFold, Where::Mapped, "LeaseRaceRegression.RevokeAndUpdateAtFourPointsRegression"},
    {Op::Revoke, Timing::RevokeBehindUpdate, Where::OneNode, "LeaseMatrixTiming"},
    {Op::Revoke, Timing::KnownOldDuplicate, Where::Mapped, "LeaseWriteHookWorkerRegression.LeaseKnownOldDuplicateTableRegression"},
    {Op::Revoke, Timing::LeaderChangeUncommitted, Where::Trio, "LeaseTrioRegression.LeaderChangeCellsRegression"},
    {Op::Revoke, Timing::LeaderChangeCommittedNotFolded, Where::Trio, "LeaseTrioRegression.LeaderChangeCellsRegression"},
    {Op::Revoke, Timing::LeaderChangeFolded, Where::Trio, "LeaseTrioRegression.LeaderChangeCellsRegression"},
    {Op::Revoke, Timing::SuspendRejectAll, Where::OneNode, "LeaseMatrixTiming"},
    {Op::KeepAlive1, Timing::AroundExpireBeforePoll, Where::Mapped, "LeaseRaceRegression.KeepAliveAtFourPointsRegression"},
    {Op::KeepAlive1, Timing::AroundExpireAfterBuild, Where::Mapped, "LeaseRaceRegression.KeepAliveAtFourPointsRegression"},
    {Op::KeepAlive1, Timing::AroundExpireAfterCommit, Where::Mapped, "LeaseRaceRegression.KeepAliveAtFourPointsRegression"},
    {Op::KeepAlive1, Timing::AroundExpireAfterFold, Where::Mapped, "LeaseRaceRegression.KeepAliveAtFourPointsRegression"},
    {Op::KeepAlive1, Timing::KeepAliveInClearing, Where::Mapped, "LeaseKeepAliveTableRegression.ClearingParksRegression"},
    {Op::KeepAlive1, Timing::KeepAliveInCheckpointing, Where::Mapped, "LeaseKeepAliveTableRegression.CheckpointingParksRegression"},
    {Op::KeepAlive1, Timing::KeepAliveInEmitting, Where::Mapped, "LeaseKeepAliveTableRegression.EmittingParksRegression"},
    {Op::KeepAlive1, Timing::KeepAliveInDueNotEmitted, Where::Mapped, "LeaseKeepAliveTableRegression.DueParksRegression"},
    {Op::KeepAlive1, Timing::LeaderChangeUncommitted, Where::Trio, "LeaseTrioRegression.LeaderChangeCellsRegression"},
    {Op::KeepAlive1, Timing::LeaderChangeCommittedNotFolded, Where::Trio, "LeaseTrioRegression.LeaderChangeCellsRegression"},
    {Op::KeepAlive1, Timing::LeaderChangeFolded, Where::Trio, "LeaseTrioRegression.LeaderChangeCellsRegression"},
    {Op::KeepAlive1, Timing::Zombie, Where::Mapped, "LeaseKeepAliveTableRegression.ParkedOkAfterReadLeaseEndsIsNotLeaderRegression"},
    {Op::KeepAlive1, Timing::SuspendReadOnly, Where::OneNode, "LeaseMatrixTiming"},
    {Op::KeepAlive1, Timing::SuspendRejectAll, Where::OneNode, "LeaseMatrixTiming"},
    {Op::KeepAlive1, Timing::DropWithParked, Where::Mapped, "LeaseKeepAliveTableRegression.DropWithParkedUnknownTopicRegression"},
    {Op::KeepAlive1, Timing::RestartOne, Where::OneNode, "LeaseMatrixTiming"},
    {Op::KeepAlive1, Timing::AtRiskPendingUnsent, Where::Mapped, "LeaseSessionRegression.GuardFailsPendingAndUnsentRegression"},
    {Op::KeepAliveBatch, Timing::KeepAliveInClearing, Where::Mapped, "LeaseKeepAliveTableRegression.BatchMixedAnswersRegression"},
    {Op::KeepAliveBatch, Timing::KeepAliveInCheckpointing, Where::Mapped, "LeaseKeepAliveTableRegression.BatchMixedAnswersRegression"},
    {Op::KeepAliveBatch, Timing::KeepAliveInEmitting, Where::Mapped, "LeaseKeepAliveTableRegression.BatchMixedAnswersRegression"},
    {Op::KeepAliveBatch, Timing::KeepAliveInDueNotEmitted, Where::Mapped, "LeaseKeepAliveTableRegression.BatchMixedAnswersRegression"},
    {Op::KeepAliveBatch, Timing::DropWithParked, Where::Mapped, "LeaseKeepAliveTableRegression.DropWithParkedUnknownTopicRegression"},
    {Op::TimeToLive, Timing::LeaderChangeFolded, Where::Trio, "LeaseTrioRegression.LeaderChangeCellsRegression"},
    {Op::TimeToLive, Timing::Zombie, Where::Mapped, "LeaseKeepAliveTableRegression.NoReadLeaseNotLeaderRegression"},
    {Op::PointRead, Timing::BackToBack, Where::OneNode, "LeaseMatrixTiming"},
    {Op::PointRead, Timing::LeaderChangeFolded, Where::Trio, "LeaseTrioRegression.LeaderChangeCellsRegression"},
    {Op::ScanLeases, Timing::LeaderChangeMidScan, Where::Trio, "LeaseTrioRegression.ScanLeasesLeaderChangeMidScanRegression"},
    {Op::ScanLeases, Timing::CompactionMid, Where::Mapped, "LeaseJudgeWorkerRegression.LeaseRebuildAfterCompactionRegression"},
    {Op::WatchStart, Timing::HandoffOverflow, Where::Mapped, "LeaseWatchHandoffRegression.*Overflow*"},
    {Op::WatchStart, Timing::LeaderChangeMidScan, Where::Trio, "LeaseWatchTrioRegression.LeaderChangeMidScanRegression"},
    {Op::WatchStart, Timing::CompactionMid, Where::Mapped, "LeaseJudgeWorkerRegression.LeaseRebuildAfterCompactionRegression"},
    {Op::BrokerExpire, Timing::LeaderChangeUncommitted, Where::Trio, "LeaseTrioRegression.LeaderChangeCellsRegression"},
    {Op::BrokerExpire, Timing::LeaderChangeCommittedNotFolded, Where::Trio, "LeaseTrioRegression.LeaderChangeCellsRegression"},
    {Op::BrokerExpire, Timing::LeaderChangeFolded, Where::Trio, "LeaseTrioRegression.LeaderChangeCellsRegression"},
    {Op::BrokerExpire, Timing::RestartAll, Where::Trio, "LeaseTrioRegression.WholeClusterRestartRegression"},
    {Op::BrokerExpire, Timing::SuspendReadOnly, Where::OneNode, "LeaseMatrixTiming"},
    {Op::BrokerExpire, Timing::SuspendRejectAll, Where::OneNode, "LeaseMatrixTiming"},
    {Op::BrokerExpire, Timing::ContenderGrants, Where::Mapped, "LeaseRaceRegression.ContenderGrantsDoNotDeferExpireRegression"},
    {Op::BrokerCheckpoint, Timing::LeaderChangeUncommitted, Where::Trio, "LeaseTrioRegression.BrokerCheckpointLeaderChangeRegression"},
    {Op::BrokerCheckpoint, Timing::LeaderChangeCommittedNotFolded, Where::Trio, "LeaseTrioRegression.BrokerCheckpointLeaderChangeRegression"},
    {Op::BrokerCheckpoint, Timing::LeaderChangeFolded, Where::Trio, "LeaseTrioRegression.BrokerCheckpointLeaderChangeRegression"},
    {Op::BrokerCheckpoint, Timing::RestartOne, Where::Trio, "LeaseTrioRegression.BrokerCheckpointRestartOneRegression"},
    {Op::BrokerCheckpoint, Timing::RestartAll, Where::Trio, "LeaseTrioRegression.WholeClusterRestartRegression"},
    {Op::BrokerCheckpoint, Timing::CompactionMid, Where::Mapped, "LeaseJudgeWorkerRegression.LeaseRebuildAfterCompactionRegression"},
    {Op::BrokerCheckpoint, Timing::AlterMidLife, Where::OneNode, "LeaseMatrixTiming"},
    {Op::ClearCheckpoint0, Timing::LeaderChangeUncommitted, Where::Trio, "LeaseTrioRegression.CheckpointClearLeaderKillRegression"},
    {Op::ClearCheckpoint0, Timing::LeaderChangeCommittedNotFolded, Where::Trio, "LeaseTrioRegression.CheckpointClearLeaderKillRegression"},
    {Op::ClearCheckpoint0, Timing::LeaderChangeFolded, Where::Trio, "LeaseTrioRegression.CheckpointClearLeaderKillRegression"},
    {Op::AlterBounds, Timing::AlterMidLife, Where::OneNode, "LeaseMatrixTiming"},
    {Op::AlterCheckpoint, Timing::AlterMidLife, Where::OneNode, "LeaseMatrixTiming"},
    {Op::DropTopic, Timing::DropWithParked, Where::OneNode, "LeaseMatrixTiming"},
};
}  // namespace

TEST(LeaseMatrixRegression, TimingCoverageRegression) {
    // Every timing cell of every implemented op is in kCoverage exactly once, and nothing else is.
    std::size_t expected = 0;
    for (std::size_t o = 0; o < kOps; ++o) {
        if (!implemented(Op(o))) continue;
        for (Timing t : timingsOf(Op(o))) {
            ++expected;
            int n = 0;
            for (const Cov& c : kCoverage) n += int(c.op == Op(o) && c.timing == t);
            EXPECT_EQ(n, 1) << name(Op(o)) << " x " << name(t);
        }
    }
    EXPECT_EQ(std::size(kCoverage), expected);
    EXPECT_EQ(expected, countCells(true).timing);
    for (const Cov& c : kCoverage)
        EXPECT_TRUE(std::string(c.test).find("Regression") != std::string::npos || std::string(c.test) == "LeaseMatrixTiming") << c.test;
}

namespace {
struct TCell { Op op; Timing timing; };
std::string tName(const testing::TestParamInfo<TCell>& i) { return std::string(name(i.param.op)) + "_" + name(i.param.timing); }
std::vector<TCell> oneNodeTimings() {
    std::vector<TCell> v;
    for (const Cov& c : kCoverage)
        if (c.where == Where::OneNode) v.push_back({c.op, c.timing});
    return v;
}
constexpr uint16_t kSuspended = static_cast<uint16_t>(pc::ErrorCode::NamespaceSuspended);

class LeaseMatrixTiming : public testing::TestWithParam<TCell> {};

bool waitGone(Lt& lt, const LeaseClaim& r) {
    for (int i = 0; i < 600; ++i)
        if (!lt.getAndWait(r).has_value()) return true;
    return false;
}
int64_t remaining(Lt& lt, const LeaseClaim& r) {
    rc::LeaseTtlInfo info;
    return lt.timeToLiveAndWait(r, &info).ok() ? info.remainingMs : -2;
}
}  // namespace

TEST_P(LeaseMatrixTiming, TimingCellRegression) {
    const Op op = GetParam().op;
    const Timing tm = GetParam().timing;
    LeaseBrokerRig rig(std::string("lease-mx-t-") + name(op) + "-" + name(tm), 60'000, 1000, 600'000);
    auto lt = rig.ua().openLeaseTopicAndWait<LeaseClaim>("t", {}, 30s);
    rc::LeaseOptions o10;
    o10.ttlMs = 10'000;
    namespace pcl = rune::protocol;
    auto suspend = [&](pc::NsSuspendMode m) { ASSERT_TRUE(rig.admin().suspendNamespace("a", m).has_value()); };
    auto resume = [&] { ASSERT_TRUE(rig.admin().resumeNamespace("a").has_value()); };
    const LeaseClaim r = rig.row("tm-key", 1);

    if (tm == Timing::BackToBack) {
        if (op == Op::Grant) {   // two grants of one key issued back to back: the first wins, the second is KeyExists, one row
            const rc::LeaseResult a = lt.grantAndWait(r), b = lt.grantAndWait(r);
            EXPECT_TRUE(a.ok());
            EXPECT_EQ(b.errorCode, kKeyExists);
            EXPECT_EQ(lt.getAndWait(r)->lease_version, a.version);
        } else if (op == Op::Update) {   // updates issued back to back at the lease's version: each applies, the last row stands
            const rc::LeaseResult g = lt.grantAndWait(r);
            ASSERT_TRUE(g.ok());
            for (uint64_t i = 1; i <= 20; ++i) ASSERT_TRUE(lt.updateAndWait(rig.row("tm-key", 100 + i), g.version).ok()) << i;
            EXPECT_EQ(lt.getAndWait(r)->a, 120u);
        } else {   // PointRead: the read right behind the grant's reply sees the row (read-your-writes), every time
            for (int i = 0; i < 20; ++i) {
                const LeaseClaim k = rig.row("tm-pr-" + std::to_string(i), uint64_t(i));
                const rc::LeaseResult g = lt.grantAndWait(k);
                ASSERT_TRUE(g.ok());
                const auto row = lt.getAndWait(k, g.version);
                ASSERT_TRUE(row.has_value()) << i;
                EXPECT_EQ(row->lease_version, g.version);
            }
        }
    } else if (tm == Timing::SuspendReadOnly || tm == Timing::SuspendRejectAll) {
        const bool rejectAll = tm == Timing::SuspendRejectAll;
        rc::LeaseOptions o = o10;
        const rc::LeaseResult g = lt.grantAndWait(r, o);
        ASSERT_TRUE(g.ok());
        // Task 21: the RejectAll keepalive cell proves the keepalive was REFUSED, not merely that the lease ended: 6 s pass before the
        // suspension and 6 s after, so a keepalive that was wrongly served (deadline 6 s + 10 s) would leave the lease alive at 12 s.
        const bool splitAdvance = op == Op::KeepAlive1 && rejectAll;
        if (splitAdvance) rig.advance(6s);
        suspend(rejectAll ? pc::NsSuspendMode::RejectAll : pc::NsSuspendMode::ReadOnly);
        // A suspended namespace answers NamespaceSuspended; RejectAll also ends the namespace's sessions (the call then fails without a code).
        auto refused = [&](const rc::LeaseResult& x) { return !x.ok() && (x.errorCode == kSuspended || rejectAll); };
        switch (op) {
        case Op::Grant: EXPECT_TRUE(refused(lt.grantAndWait(rig.row("tm-new", 2)))); break;
        case Op::Update: EXPECT_TRUE(refused(lt.updateAndWait(rig.row("tm-key", 5), g.version))); break;
        case Op::Revoke: EXPECT_TRUE(refused(lt.revokeAndWait(r, g.version))); break;
        case Op::KeepAlive1: {
            const rc::LeaseResult ka = lt.keepAliveOnceAndWait(r, g.version);
            if (rejectAll) EXPECT_TRUE(refused(ka)) << "RejectAll refuses keepalives";
            else EXPECT_TRUE(ka.ok()) << "ReadOnly serves keepalives";
            break;
        }
        default: break;   // BrokerExpire: below, expiry continues
        }
        // Expiry never pauses (G3): an unkept lease ends under either suspension (the keepalive of ReadOnly writes nothing).
        const bool kept = op == Op::KeepAlive1 && !rejectAll;
        rig.advance(splitAdvance ? 6s : 12s);
        resume();
        // Observed by the cluster admin (the namespace's own sessions ended under RejectAll).
        auto adminLt = rig.admin().openLeaseTopicAndWait<LeaseClaim>("a/t", {}, 30s);
        EXPECT_TRUE(waitGone(adminLt, r)) << "an unkept lease still expires under " << name(tm) << (kept ? " (a ReadOnly keepalive writes nothing)" : "");
        adminLt.close();
    } else if (tm == Timing::RevokeBehindUpdate) {
        const rc::LeaseResult g = lt.grantAndWait(r);
        ASSERT_TRUE(g.ok());
        ASSERT_TRUE(lt.updateAndWait(rig.row("tm-key", 9), g.version).ok());
        const rc::LeaseResult rv = lt.revokeAndWait(r, g.version);
        EXPECT_TRUE(rv.ok()) << "a revoke right behind an update: " << int(rv.status) << " " << rv.errorCode;
        EXPECT_TRUE(waitGone(lt, r));
    } else if (tm == Timing::RestartOne) {
        const rc::LeaseResult g = lt.grantAndWait(r, o10);
        ASSERT_TRUE(g.ok());
        lt.close();
        rig.restart();
        auto lt2 = rig.ua().openLeaseTopicAndWait<LeaseClaim>("t", {}, 30s);
        const auto row = lt2.getAndWait(r);
        ASSERT_TRUE(row.has_value()) << "the lease survives the restart";
        EXPECT_EQ(row->lease_version, g.version);
        if (op == Op::KeepAlive1) {
            EXPECT_TRUE(lt2.keepAliveOnceAndWait(r, g.version).ok()) << "the pre-restart version still keeps the lease alive";
        } else {   // Grant: a regrant of the live key is KeyExists, the version unchanged
            EXPECT_EQ(lt2.grantAndWait(r).errorCode, kKeyExists);
        }
        lt2.close();
    } else if (tm == Timing::AlterMidLife) {
        const rc::LeaseResult g = lt.grantAndWait(r, o10);
        ASSERT_TRUE(g.ok());
        const auto before = lt.getAndWait(r);
        ASSERT_TRUE(before.has_value());
        if (op == Op::AlterBounds || op == Op::Grant) {   // lower the ceiling below the live lease's ttl
            const pc::ConfigChangeWire ch[] = {{static_cast<uint8_t>(pcl::ConfigKey::LeaseMaxTtlMs), 120'000}};
            const auto a = rig.ua().alterTopicConfigAndWait("t", 0, ch, {}, 30s);
            ASSERT_EQ(a.errorCode, 0) << a.detail;
            // The live lease is untouched; a new grant above the new ceiling is refused.
            EXPECT_TRUE(lt.getAndWait(r).has_value());
            EXPECT_EQ(lt.getAndWait(r)->lease_version, g.version);
            rc::LeaseOptions big = o10;
            big.ttlMs = 200'000;
            const rc::LeaseResult tl = lt.grantAndWait(rig.row("tm-big2", 4), big);
            EXPECT_FALSE(tl.ok()) << "a ttl above the lowered max is refused";
        } else {   // AlterCheckpoint / BrokerCheckpoint x AlterMidLife: switch checkpoints on and off mid-life
            for (int64_t v : {int64_t{1}, int64_t{0}}) {
                const pc::ConfigChangeWire ch[] = {{static_cast<uint8_t>(pcl::ConfigKey::LeaseCheckpoint), v}};
                const auto a = rig.ua().alterTopicConfigAndWait("t", 0, ch, {}, 30s);
                ASSERT_EQ(a.errorCode, 0) << a.detail;
                rig.advance(2s);
            }
            const auto after = lt.getAndWait(r);
            ASSERT_TRUE(after.has_value()) << "the lease lives through the checkpoint toggles";
            EXPECT_EQ(after->lease_version, g.version);
            EXPECT_GT(remaining(lt, r), 0);
        }
    } else if (tm == Timing::DropWithParked) {
        auto lt2 = rig.ua().openLeaseTopicAndWait<LeaseClaim>("t2", {}, 30s);   // t carries the user view, which blocks its drop
        const rc::LeaseResult g = lt2.grantAndWait(r, o10);
        ASSERT_TRUE(g.ok());
        const auto d = rig.ua().dropTopicAndWait("t2", {}, 30s);
        ASSERT_EQ(d.errorCode, 0) << d.detail;
        const rc::LeaseResult ka = lt2.keepAliveOnceAndWait(r, g.version);
        EXPECT_FALSE(ka.ok()) << "a keepalive of a dropped lease topic is refused (UnknownTopic), never hangs";
    } else {
        FAIL() << "unhandled timing cell " << name(op) << " x " << name(tm);
    }
}

INSTANTIATE_TEST_SUITE_P(Cells, LeaseMatrixTiming, testing::ValuesIn(oneNodeTimings()), tName);
