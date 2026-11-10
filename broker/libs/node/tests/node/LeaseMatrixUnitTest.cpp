// LeaseMatrixUnitTest.cpp - lease topics Task 17a: the interaction matrix on the in-process lease worker (spec §21, plan "Interaction matrix").
//
//   LeaseMatrixRegression.MatrixCountRegression        pins 198 / 55 / 143 / 89 / 232 over all ops (and the implemented-only counts)
//   LeaseMatrixUnitRegression.SequentialCellRegression  one TEST_P instance per applicable sequential cell of an entity the single-topic
//                                                       rig can host (SameKeyVer, StaleVer, OtherKeySamePart, UserView) whose op is
//                                                       implemented.  Each cell asserts the op's outcome AND that every other lease row,
//                                                       its remaining time and the user view row are unchanged (or changed exactly as
//                                                       LeaseMatrixExpectations.h says).
//
// The other five entities (OtherPart, OtherLeaseTopic, NonLeaseSameWorker, OtherNs, OldSession) need a second partition, a second topic, a
// namespace or a gateway: the rig (VerdictRig) hosts one lease topic and no gateway.  They run on the real 1-node broker (LeaseMatrixTest.cpp),
// and the gateway ones are also pinned by LeaseGatewayRouteTest (VersionGateRegression, ForeignNamespaceIsMissingRegression).
#include "LeaseRig.h"
#include "LeaseMatrixExpectations.h"

#include <rune/codec/Validator.h>
#include <rune/view/KeyedFold.h>
#include <rune/view/ViewFold.h>

#include <map>
#include <optional>
#include <set>
#include <sstream>

using namespace rune::node;
using namespace rune::node::cw;
using namespace rune::node::test::leasematrix;
using ltr::Lk;
namespace pc = rune::protocol::client;
namespace rv = rune::view;

// ---- the counts -----------------------------------------------------------------------------------------------------------------
// The timing cells that run on the unit layer's sibling layers, and the existing tests that already assert the exact scenario (decision 4).
namespace rune::node::test::leasematrix {
struct MappedTiming { Op op; Timing timing; const char* test; };
inline constexpr MappedTiming kMappedTimings[] = {
    {Op::WatchStart, Timing::HandoffOverflow, "LeaseWatchHandoffRegression.*Overflow* (client-cpp/tests/lease/LeaseWatchTest.cpp)"},
    {Op::KeepAlive1, Timing::AtRiskPendingUnsent, "LeaseSessionRegression.GuardFailsPendingAndUnsentRegression (client-cpp/tests/lease/LeaseSessionTest.cpp)"},
};
}  // namespace rune::node::test::leasematrix

TEST(LeaseMatrixRegression, MatrixCountRegression) {
    const Counts all = countCells(false);
    EXPECT_EQ(all.sequential, 198u);
    EXPECT_EQ(all.na, 55u) << "48 + the 7 recipe ops x OtherPart (a recipe needs a lease topic of ONE partition, spec 12.3)";
    EXPECT_EQ(all.applicable, 143u);
    EXPECT_EQ(all.timing, 89u);
    EXPECT_EQ(all.executed, 232u);
    const Counts impl = countCells(true);   // Task 20 built the Own* / Elect* ops: every op is implemented
    EXPECT_EQ(impl.sequential, all.sequential);
    EXPECT_EQ(impl.applicable, all.applicable);
    EXPECT_EQ(impl.na, all.na);
    EXPECT_EQ(impl.timing, all.timing);
    EXPECT_EQ(impl.executed, all.executed);
    for (std::size_t oi = 0; oi < kOps; ++oi) EXPECT_TRUE(implemented(Op(oi))) << name(Op(oi));
    EXPECT_EQ(expect(Op::OwnTryClaim, Entity::SameKeyVer, Timing::Sequential).result, Result::Claimed);
    EXPECT_EQ(expect(Op::OwnClaim, Entity::NonLeaseSameWorker, Timing::Sequential).result, Result::NotALeaseTopic);
    EXPECT_TRUE(naRule(Op::ElectCampaign, Entity::OtherPart));
    // Every timing cell is run by a test of this task or mapped to a named existing test.
    for (const MappedTiming& m : kMappedTimings) {
        bool listed = false;
        for (Timing t : timingsOf(m.op)) listed = listed || t == m.timing;
        EXPECT_TRUE(listed) << name(m.op) << " x " << name(m.timing) << " is not a timing cell of that op";
        EXPECT_NE(std::string(m.test).find("Regression"), std::string::npos);
    }
    // Spot checks of the rules, against the plan's table.
    EXPECT_TRUE(naRule(Op::Grant, Entity::StaleVer));
    EXPECT_FALSE(naRule(Op::Update, Entity::StaleVer));
    EXPECT_TRUE(naRule(Op::TimeToLive, Entity::UserView));
    EXPECT_FALSE(naRule(Op::DropTopic, Entity::UserView));
    EXPECT_TRUE(naRule(Op::BrokerExpire, Entity::OtherNs));
    EXPECT_FALSE(naRule(Op::AlterBounds, Entity::OtherNs));
    EXPECT_TRUE(naRule(Op::AlterBounds, Entity::OldSession));
    EXPECT_EQ(expect(Op::Grant, Entity::OldSession, Timing::Sequential).result, Result::InvalidRequest);
    EXPECT_EQ(expect(Op::Update, Entity::StaleVer, Timing::Sequential).result, Result::LeaseNotFound);
    EXPECT_EQ(expect(Op::BrokerExpire, Entity::StaleVer, Timing::Sequential).result, Result::StaleVerdict);
    EXPECT_EQ(expect(Op::KeepAlive1, Entity::NonLeaseSameWorker, Timing::Sequential).result, Result::NotALeaseTopic);
    EXPECT_EQ(expect(Op::BrokerCheckpoint, Entity::NonLeaseSameWorker, Timing::Sequential).result, Result::NoEffect);
    EXPECT_TRUE(expect(Op::BrokerExpire, Entity::UserView, Timing::Sequential).userViewChanged);
    EXPECT_FALSE(expect(Op::BrokerCheckpoint, Entity::UserView, Timing::Sequential).userViewChanged);
    EXPECT_FALSE(expect(Op::KeepAlive1, Entity::UserView, Timing::Sequential).userViewChanged);
}

namespace {

constexpr int32_t kLeaseView = VerdictRig::kLeaseView;
constexpr int32_t kProj = 40;
constexpr int64_t kStaleDelta = 1'000'000;
constexpr uint8_t kExpireAttr = uint8_t(rune::protocol::record::LeaseOp::Expire) << rune::protocol::record::kAttrLeaseOpShift;

rv::ViewPlan projPlan() {
    rv::ViewPlan v;
    v.viewName = "proj"; v.key = "id"; v.sources = {{VerdictRig::kSrc, "id"}};
    v.output = {{"id", rune::codec::PrimType::UInt64, 0, "id", rv::MergeRule::Replace},
                {"addr", rune::codec::PrimType::FixedStr, 0, "addr", rv::MergeRule::Replace}};
    v.persistence = 1;
    return v;
}
rv::CompileResult compileProj() {
    static const rune::codec::Descriptor src = VerdictRig::leaseSourceDescriptor();
    const rune::codec::Descriptor* srcs[] = {&src};
    auto c = rv::compileView(projPlan(), srcs, kProj);
    EXPECT_TRUE(c.has_value());
    return std::move(c).value();
}
std::vector<uint8_t> idKeyBE(uint64_t id) {   // a view key: a uint64 component is big-endian
    std::vector<uint8_t> k;
    for (int i = 7; i >= 0; --i) k.push_back(uint8_t(id >> (8 * i)));
    return k;
}
std::vector<uint8_t> keyBytes(uint64_t id) {
    const std::string s = ltr::keyOf(id);
    return std::vector<uint8_t>(s.begin(), s.end());
}

using Row = std::optional<std::vector<uint8_t>>;
struct KeyState {
    Row     lease;       ///< the lease view row
    Row     user;        ///< the user view row (nullopt: absent)
    int64_t remaining = -1;   ///< TimeToLive remaining ms (-1: not live)
    bool operator==(const KeyState&) const = default;
};
struct Snap {
    std::map<uint64_t, KeyState> keys;
    uint64_t staleBroker = 0;
};

struct Cell {
    Op op;
    Entity ent;
    std::string label() const { return std::string(name(op)) + "_" + name(ent); }
};

/// The rig: one lease topic (keys 1, 2, 4 live, 3 fresh), the lease clock injected, a user view over it.
struct UnitCell {
    Lk k;
    int64_t v1 = -1, v2 = -1, v4 = -1;
    explicit UnitCell(bool userView) : k(ltr::specWith(1000, 0, 1000, 1)) {
        if (userView) {
            k.r.installKeyedView(compileProj());
            k.r.passes(30);
        }
        hasUser = userView;
    }
    bool hasUser = false;

    bool setup() {
        if (!k.produce({ltr::grantRec(1), ltr::grantRec(2), ltr::grantRec(4)})) return false;
        if (!k.r.driveUntilJudged()) return false;
        if (!k.waitRuntime()) return false;
        v1 = k.versionOf(1); v2 = k.versionOf(2); v4 = k.versionOf(4);
        k.advanceMs(1000);   // every lease has 4000 ms left: a keepalive (-> 5000) is visible, an update (no renewal) is not
        k.r.passes(5);
        return v1 >= 0 && v2 >= 0 && v4 >= 0;
    }
    struct Ttl { int16_t code = -99; int64_t remainingMs = -1; };
    Ttl ttlOf(uint64_t key) {
        k.r.leaseTtlAnswers.clear();
        const std::string kb = ltr::keyOf(key);
        WorkerOp op;
        op.type = WorkerOp::Type::LeaseTimeToLive; op.topicId = VerdictRig::kSrc; op.partition = 0;
        op.sessionId = 5; op.correlationId = ++k.corr;
        op.leaseTail = std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(kb.data()), kb.size());
        EXPECT_TRUE(k.r.worker->tryEnqueueOp(std::move(op)));
        for (int i = 0; i < 8 && k.r.leaseTtlAnswers.empty(); ++i) k.r.pass();
        Ttl t;
        if (k.r.leaseTtlAnswers.size() == 1) { t.code = k.r.leaseTtlAnswers[0].errorCode; t.remainingMs = k.r.leaseTtlAnswers[0].baseOffset; }
        k.r.leaseTtlAnswers.clear();
        return t;
    }
    Row rowOf(int32_t view, const std::vector<uint8_t>& key) {
        const auto got = k.r.getViewRowK(view, key);
        if (got.code != pc::ViewRowResponse::kOk) return std::nullopt;
        return got.row;
    }
    Snap snap() {
        Snap s;
        for (uint64_t key : {1, 2, 3, 4}) {
            KeyState ks;
            ks.lease = rowOf(kLeaseView, keyBytes(key));
            if (hasUser) ks.user = rowOf(kProj, idKeyBE(key));
            const Ttl t = ttlOf(key);
            ks.remaining = t.code == 0 ? t.remainingMs : -1;
            s.keys[key] = std::move(ks);
        }
        s.staleBroker = k.r.stats() != nullptr ? k.r.stats()->leaseRejectedStaleBroker : 0;
        return s;
    }
    /// A broker record (Expire / Checkpoint) appended by the test, as the emitter would.
    void appendBroker(rune::protocol::record::LeaseOp op, uint64_t key, int64_t version, uint64_t remainingMs = 0) {
        rune::protocol::RecordBatchBuilder b(0, 0);
        const auto v = ltr::leaseValue(key, version, "a", 5000, remainingMs);
        b.append(ltr::keyOf(key), std::string_view(reinterpret_cast<const char*>(v.data()), v.size()), 0, rune::protocol::record::withLeaseOp(0, op));
        const auto batch = b.build();
        const int64_t base = k.r.replica->appendLeaseBatch(batch);
        EXPECT_GE(base, 0);
    }
    /// The verdict a produce got: 0 accepted, else the rejection code of its single record (or the produce-level error).
    int produceVerdict(const RawRec& rec) {
        const int64_t c = k.send({rec});
        const auto ack = k.r.ack(c, 600);
        if (!ack) return -1;
        if (ack->code != 0) return ack->code;
        EXPECT_TRUE(k.r.driveUntilJudged());
        k.r.passes(10);
        for (const auto& v : ack->verdicts) return std::get<1>(v);
        const PartitionWriteState* w = k.r.writeState();
        if (w != nullptr) {
            const int64_t off = w->rejected.nextRejectedAtOrAfter(ack->baseOffset);
            if (off == ack->baseOffset) return w->rejected.find(off)->code;
        }
        return 0;
    }
    int keepAliveCode(const std::vector<std::pair<std::string, int64_t>>& entries, std::size_t at = 0) {
        const auto fs = k.keepAlive(entries);
        if (fs.empty()) return -1;
        for (const auto& f : fs) for (const auto& e : f.es) if (e.index == at) return e.code;
        return -2;
    }
};

Result fromCode(int code) {
    switch (code) {
    case 0: return Result::Ok;
    case int(pc::ErrorCode::KeyExists): return Result::KeyExists;
    case int(pc::ErrorCode::LeaseNotFound): return Result::LeaseNotFound;
    default: return Result::Absent;   // a code the matrix does not name: the comparison fails and prints it
    }
}

void runCell(const Cell& c) {
    const Outcome want = expect(c.op, c.ent, Timing::Sequential);
    ASSERT_NE(want.result, Result::Absent);
    UnitCell u(/*userView=*/c.ent == Entity::UserView);
    ASSERT_TRUE(u.setup());

    // The addressed lease.  Grant addresses a fresh key (3) unless the cell is the live key itself.
    const bool other = c.ent == Entity::OtherKeySamePart;
    const uint64_t key = other ? (c.op == Op::Grant ? 3 : 2) : 1;
    const int64_t live = other ? u.v2 : u.v1;
    const int64_t version = c.ent == Entity::StaleVer ? live + kStaleDelta : live;
    // A clear needs a checkpointed lease first (the keepalive's Checkpoint(0) clears one).
    if (c.op == Op::ClearCheckpoint0) {
        u.appendBroker(rune::protocol::record::LeaseOp::Checkpoint, key, live, 2000);
        ASSERT_TRUE(u.k.r.driveUntilJudged());
        u.k.r.passes(10);
    }
    const Snap before = u.snap();
    bool dropped = false;
    Result got = Result::Ok;
    switch (c.op) {
    case Op::Grant: got = fromCode(u.produceVerdict(ltr::grantRec(key))); break;
    case Op::Update: got = fromCode(u.produceVerdict(ltr::updateRec(key, version))); break;
    case Op::Revoke: got = fromCode(u.produceVerdict(ltr::revokeRec(key, version))); break;
    case Op::KeepAlive1: got = fromCode(u.keepAliveCode({ltr::E(key, version)})); break;
    case Op::KeepAliveBatch: {   // [target, key 4]: key 4 is the companion (always live, always answered Ok, excluded below)
        const auto fs = u.k.keepAlive({ltr::E(key, version), ltr::E(4, u.v4)});
        int code = -1, companion = -1;
        for (const auto& f : fs) for (const auto& e : f.es) { if (e.index == 0) code = e.code; if (e.index == 1) companion = e.code; }
        EXPECT_EQ(companion, 0) << "the companion entry is answered Ok";
        got = fromCode(code);
        break;
    }
    case Op::TimeToLive: { const auto t = u.ttlOf(key); got = fromCode(t.code); EXPECT_EQ(t.remainingMs, 4000); break; }
    case Op::PointRead: got = u.rowOf(kLeaseView, keyBytes(key)).has_value() ? Result::Ok : Result::LeaseNotFound; break;
    case Op::ScanLeases: case Op::WatchStart: {
        const auto p = u.k.scan({}, 64, 0, /*first=*/c.op == Op::WatchStart);
        const auto ids = p.ids();
        got = p.code == 0 && std::find(ids.begin(), ids.end(), key) != ids.end() ? Result::Ok : Result::LeaseNotFound;
        if (c.op == Op::WatchStart) { EXPECT_GE(p.asOf, 0); EXPECT_TRUE(p.done); }
        break;
    }
    case Op::BrokerExpire:
        u.appendBroker(rune::protocol::record::LeaseOp::Expire, key, version);
        break;
    case Op::BrokerCheckpoint:
        u.appendBroker(rune::protocol::record::LeaseOp::Checkpoint, key, version, 2000);
        break;
    case Op::ClearCheckpoint0:
        u.appendBroker(rune::protocol::record::LeaseOp::Checkpoint, key, version, 0);
        break;
    case Op::AlterBounds: {
        auto spec = ltr::specWith(1000, 0, 1000, 1);
        spec.maxTtlMs = 3000;   // below the live leases' 5000
        u.k.r.alterLeaseSpec(spec);
        break;
    }
    case Op::AlterCheckpoint: u.k.r.alterLeaseSpec(ltr::specWith(1000, 1, 2000, 5)); break;
    case Op::DropTopic: {
        u.k.r.worker->setDroppedTopicsDirect({VerdictRig::kSrc});
        for (int i = 0; i < 40; ++i) u.k.r.pass();
        dropped = true;
        break;
    }
    default: FAIL() << "op not implemented";
    }
    if (isBrokerOp(c.op)) {
        ASSERT_TRUE(u.k.r.driveUntilJudged());
        u.k.r.passes(20);
        got = u.k.r.stats()->leaseRejectedStaleBroker > before.staleBroker ? Result::StaleVerdict : Result::Ok;
    } else if (!dropped) {
        u.k.r.passes(20);
    }
    EXPECT_EQ(int(got), int(want.result)) << c.label() << ": op result";

    if (dropped) {   // the topic-wide cell: the runtime is gone and a keepalive is UnknownTopic
        EXPECT_EQ(u.k.rt(), nullptr);
        const auto fs = u.k.keepAlive({ltr::E(1, u.v1)});
        ASSERT_FALSE(fs.empty());
        ASSERT_FALSE(fs[0].es.empty());
        EXPECT_EQ(fs[0].es[0].code, uint16_t(pc::ErrorCode::UnknownTopic));
        return;
    }
    const Snap after = u.snap();
    if (isBrokerOp(c.op)) EXPECT_EQ(after.staleBroker, before.staleBroker + (want.result == Result::StaleVerdict ? 1u : 0u));
    else EXPECT_EQ(after.staleBroker, before.staleBroker) << "stale_broker moves only for an injected stale broker record";

    for (uint64_t kk : {1, 2, 3}) {   // key 4 is the batch companion
        const KeyState& b = before.keys.at(kk);
        const KeyState& a = after.keys.at(kk);
        const bool isTarget = kk == key;
        const Effect eff = isTarget && want.result == Result::Ok ? want.target : Effect::None;
        // L (key 1) may change only when `expect` says it does.
        if (kk == 1 && c.ent != Entity::SameKeyVer && c.ent != Entity::UserView && c.ent != Entity::StaleVer)
            EXPECT_FALSE(want.lChanged);
        const std::string at = c.label() + ": key " + std::to_string(kk);
        switch (eff) {
        case Effect::None:
            EXPECT_TRUE(a == b) << at << " must be unchanged (lease row, remaining and user row)";
            break;
        case Effect::Created:
            EXPECT_FALSE(b.lease.has_value()) << at;
            EXPECT_TRUE(a.lease.has_value()) << at << " the grant created the row";
            if (u.hasUser || c.op == Op::Grant) { /* U is fed on a later fold; a user view row of a new lease: present when installed */ }
            break;
        case Effect::Replaced:
            ASSERT_TRUE(a.lease.has_value()) << at;
            EXPECT_NE(*a.lease, *b.lease) << at << " columns replaced";
            EXPECT_EQ(a.remaining, b.remaining) << at << " an update never renews";
            if (u.hasUser) { ASSERT_TRUE(a.user.has_value()); EXPECT_NE(*a.user, *b.user) << at << " U upserts the new columns"; }
            break;
        case Effect::Removed:
            EXPECT_FALSE(a.lease.has_value()) << at << " the lease ended";
            EXPECT_EQ(a.remaining, -1) << at;
            EXPECT_FALSE(a.user.has_value()) << at << " U loses the row with the lease (same fold batch)";
            break;
        case Effect::Rearmed:
            EXPECT_EQ(a.lease, b.lease) << at << " a keepalive leaves the row alone";
            EXPECT_EQ(a.remaining, 5000) << at << " the deadline is now + ttl";
            EXPECT_EQ(a.user, b.user) << at << " U byte-identical";
            break;
        case Effect::Checkpointed:
            ASSERT_TRUE(a.lease.has_value()) << at;
            EXPECT_NE(*a.lease, *b.lease) << at << " the lease row took the checkpoint's remaining";
            EXPECT_EQ(a.user, b.user) << at << " U byte-identical (a Checkpoint is invisible to user views)";
            break;
        }
    }
    if (c.op == Op::KeepAliveBatch) EXPECT_EQ(after.keys.at(4).remaining, 5000);
    if (c.op == Op::AlterBounds) {   // new grants are checked against the new max; the live leases keep their TTL
        EXPECT_EQ(u.k.rowTtl(1), 5000u);
        const int64_t cc = u.k.send({ltr::grantRec(77, 5000)});
        const auto ack = u.k.r.ack(cc, 600);
        ASSERT_TRUE(ack.has_value());
        EXPECT_EQ(int(ack->code), int(pc::ErrorCode::LeaseTtlTooLarge));
    }
}

std::vector<Cell> unitCells() {
    std::vector<Cell> out;
    for (std::size_t oi = 0; oi < kOps; ++oi) {
        const Op o = Op(oi);
        // The recipes are client-side compositions of Grant / Revoke / Update over a real client: their sequential cells run on the real
        // 1-node broker (LeaseMatrixTest.cpp, LeaseMatrixRecipeRegression.SequentialCellRegression); their expectations are pinned above.
        if (!implemented(o) || isRecipe(o)) continue;
        for (Entity e : {Entity::SameKeyVer, Entity::StaleVer, Entity::OtherKeySamePart, Entity::UserView})
            if (!naRule(o, e)) out.push_back({o, e});
    }
    return out;
}

class LeaseMatrixUnitRegression : public ::testing::TestWithParam<Cell> {};

}  // namespace

TEST_P(LeaseMatrixUnitRegression, SequentialCellRegression) { runCell(GetParam()); }

INSTANTIATE_TEST_SUITE_P(Cells, LeaseMatrixUnitRegression, ::testing::ValuesIn(unitCells()),
                         [](const ::testing::TestParamInfo<Cell>& i) { return i.param.label(); });
