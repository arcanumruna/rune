// Lease topics Task 13a (spec 7.1): point reads of the lease view `<topic>.leases` (GetViewRow / GetViewRowK) on a real worker over a real
// lease index and judge.  The lease clock and the pass clock are the test's; every drive loop is capped; no sleeps.
//
// Interaction matrix (read x state; every cell asserts the exact status):
//   | read                    | state                                         | expected                         | test
//   | GetViewRowK / GetViewRow| granted key / absent key / wrong key width    | Ok + row / AbsentKey / Invalid   | PointReadServesRowRegression
//   | GetViewRowK minOffset   | the fold behind the offset / released / > 100 ms | held / Ok / NotReady          | PointReadReadYourWritesRegression
//   | GetViewRowK             | index not ready (update with no row, judge held) | NotReady, never the old row   | NotReadyNeverStaleRegression
//   | GetViewRowK             | follower / read lease ended                   | the not-leader sentinel          | NotLeaderAndLeaseRegression
//   | GetViewRowK pinned      | the current fingerprint / another / 0         | Ok / SchemaMismatch / Ok         | FingerprintPinRegression
//   | ScanViewRows            | any                                           | UnknownView                      | ScanViewRowsOnLeaseViewUnknownRegression
//   | GetViewRowK pinned      | AlterSchema adds a user column                | fingerprint changes; old pin SchemaMismatch, 0 wider row | LeaseViewFingerprintEvolvesRegression
//   | fingerprint             | demote+promote / rename / FilterSchema order  | unchanged / unchanged / equal    | FingerprintStableAcrossRebuildRegression, LeaseViewFingerprintGoldenRegression
//   | GetViewRowK pinned      | judge held after ADD / demoted + wrong pin    | NotReady / not-leader sentinel   | PinCheckedAfterReadinessAndLeadershipRegression
//   | GetViewRow u64          | uint32 / uint16 / int32 key topic             | Ok in range, InvalidRequest out  | NarrowIntegerKeyU64ReadRegression
//   | held GetViewRowK        | demote / drop / ADD rebuild while held        | sentinel / UnknownView / NotReady | HeldReadReServeCellsRegression
#include "LeaseRig.h"

#include <rune/view/ViewRow.h>

#include <cstring>
#include <optional>

using namespace rune::node;
using namespace rune::node::cw;
using ltr::Lk;
namespace pc = rune::protocol::client;
using RS = pc::ViewRowResponse;

namespace {

constexpr int32_t kView = VerdictRig::kLeaseView;
constexpr int32_t kSrc  = VerdictRig::kSrc;

rune::protocol::LeaseSpec noCheckpoints() { return ltr::specWith(1000, 0, 300'000, 1000); }

std::vector<uint8_t> bytesOf(uint64_t id) {
    const std::string s = ltr::keyOf(id);
    return std::vector<uint8_t>(s.begin(), s.end());
}

/// One read through the worker's op path: the first answer within `passes` passes, nullopt when it is held (no answer).
std::optional<WorkerReturn> readOnce(Lk& k, WorkerOp::Type type, const std::vector<uint8_t>& key, int64_t minOffset = -1, uint64_t fp = 0,
                                     int passes = 4, int32_t view = kView) {
    (void)k.r.takeViewAnswers();
    k.r.postViewReadOp(type, view, key, minOffset, fp);
    for (int i = 0; i < passes; ++i) {
        k.r.pass();
        auto a = k.r.takeViewAnswers();
        if (!a.empty()) {
            EXPECT_EQ(a.size(), 1u);
            return std::move(a.front());
        }
    }
    return std::nullopt;
}
int16_t statusOf(const std::optional<WorkerReturn>& r) { return r ? r->errorCode : int16_t(-99); }

/// A granted, judged, ready lease topic with key 1 (ttl 5000, holder 1, addr "a").
void grantKey1(Lk& k) {
    ASSERT_TRUE(k.produce({ltr::grantRec(1)}));
    ASSERT_TRUE(k.r.driveUntilJudged());
    for (int i = 0; i < ltr::kCap && !k.r.leaseReady(); ++i) k.r.pass();
    ASSERT_TRUE(k.r.leaseReady());
}
// The raw little-endian 8 bytes of an Int64 / UInt64 slot (FieldValue keeps signed and unsigned in different members).
uint64_t colU64(const WorkerReturn& r, const rune::view::RowLayout& l, uint16_t slot) {
    uint64_t v = 0;
    std::memcpy(&v, r.chunk.data() + l.slotOffset[slot], sizeof v);
    return v;
}

}  // namespace

// 1. A GetViewRowK / GetViewRow of a granted key is served from the lease index: the row carries the grant's columns.  An absent key is
// AbsentKey; a key of the wrong width is InvalidRequest (BadKey, as every keyed view).
// Mutation: drop the lease branch in serveViewReadImpl_ -> UnknownView: red.
TEST(LeaseViewReadRegression, PointReadServesRowRegression) {
    Lk k(noCheckpoints());
    grantKey1(k);
    const auto lk = k.r.vs->leaseIndexFor(kSrc, 0, k.r.replica->leadershipGeneration(), true, true);
    ASSERT_NE(lk.ref.table, nullptr);
    const rune::view::RowLayout& l = *lk.ref.layout;
    const int64_t version = k.versionOf(1);
    ASSERT_GE(version, 0);

    const auto a = readOnce(k, WorkerOp::Type::GetViewRowK, bytesOf(1));
    ASSERT_TRUE(a);
    ASSERT_EQ(a->errorCode, int16_t(RS::kOk));
    EXPECT_EQ(a->recordCount, int32_t(l.rowLen));
    ASSERT_EQ(a->chunk.size(), l.rowLen);
    EXPECT_EQ(int64_t(colU64(*a, l, kLeaseVersionSlot)), version) << "lease_version = the grant offset";
    EXPECT_EQ(colU64(*a, l, 1), 5000u) << "lease_ttl_ms";
    EXPECT_EQ(colU64(*a, l, 2), 1u) << "lease_holder";
    EXPECT_EQ(colU64(*a, l, 3), 0u) << "lease_remaining_ms";
    EXPECT_EQ(colU64(*a, l, 4), 1u) << "the user key column";
    EXPECT_EQ(a->chunk[l.slotOffset[5]], uint8_t('a')) << "the user addr column";
    EXPECT_EQ(int64_t(colU64(*a, l, uint16_t(l.slotType.size() - 2))), version) << "lease_granted_offset";

    // GetViewRow: the u64 as the 8-byte key.
    const auto u = readOnce(k, WorkerOp::Type::GetViewRow, bytesOf(1));
    ASSERT_TRUE(u);
    EXPECT_EQ(u->errorCode, int16_t(RS::kOk));
    EXPECT_EQ(u->chunk, a->chunk);

    EXPECT_EQ(statusOf(readOnce(k, WorkerOp::Type::GetViewRowK, bytesOf(99))), int16_t(RS::kAbsentKey));
    EXPECT_EQ(statusOf(readOnce(k, WorkerOp::Type::GetViewRow, bytesOf(99))), int16_t(RS::kAbsentKey));
    EXPECT_EQ(statusOf(readOnce(k, WorkerOp::Type::GetViewRowK, std::vector<uint8_t>(4, 1))), int16_t(RS::kInvalidRequest)) << "4-byte key";
    EXPECT_EQ(statusOf(readOnce(k, WorkerOp::Type::GetViewRowK, std::vector<uint8_t>(9, 1))), int16_t(RS::kInvalidRequest)) << "9-byte key";
    EXPECT_EQ(statusOf(readOnce(k, WorkerOp::Type::GetViewRowK, {})), int16_t(RS::kInvalidRequest)) << "empty key";
    EXPECT_EQ(statusOf(readOnce(k, WorkerOp::Type::GetViewRowK, bytesOf(1), -1, 0, 4, kView + 1)), int16_t(RS::kUnknownView)) << "another view id";
}

// 2. Read-your-writes: a read with minOffset names a record the fold has not reached is HELD (no answer, no stale row), served the pass
// the fold reaches it, and answered NotReady once 100 ms of passes went by.  The unjudged record is a broker Checkpoint(remaining) appended
// with the judge held.
// Mutation: ignore minOffset in the lease branch -> the read is answered at once with the old row: red.
TEST(LeaseViewReadRegression, PointReadReadYourWritesRegression) {
    Lk k(noCheckpoints());
    grantKey1(k);
    const int64_t v = k.versionOf(1);
    k.r.holdJudge(true);
    const int64_t at = k.appendCheckpoint(1, v, 3000);
    ASSERT_GE(at, 0);
    k.r.passes(5);
    ASSERT_TRUE(k.r.leaseReady()) << "the index is ready (the checkpoint is merely not judged yet)";
    ASSERT_EQ(k.rowRemaining(1), 0u);

    EXPECT_FALSE(readOnce(k, WorkerOp::Type::GetViewRowK, bytesOf(1), at, 0, 10)) << "held: no answer while the fold is behind";
    EXPECT_EQ(k.r.worker->heldViewReadsForTest(), 1u);
    k.r.holdJudge(false);
    for (int i = 0; i < ltr::kCap && k.rowRemaining(1) != 3000; ++i) k.r.pass();
    auto served = k.r.takeViewAnswers();
    ASSERT_EQ(served.size(), 1u) << "served the pass the fold reached the offset";
    ASSERT_EQ(served[0].errorCode, int16_t(RS::kOk));
    const auto lk = k.r.vs->leaseIndexFor(kSrc, 0, k.r.replica->leadershipGeneration(), true, true);
    EXPECT_EQ(colU64(served[0], *lk.ref.layout, 3), 3000u) << "the row after the checkpoint";
    EXPECT_EQ(k.r.worker->heldViewReadsForTest(), 0u);

    // A deadline: a record that is never judged (the judge stays held) -> NotReady at 100 ms, never earlier.
    k.r.holdJudge(true);
    const int64_t at2 = k.appendCheckpoint(1, v, 2000);
    k.r.passes(5);
    (void)k.r.takeViewAnswers();
    k.r.postViewReadOp(WorkerOp::Type::GetViewRowK, kView, bytesOf(1), at2);
    for (int i = 0; i < 90; ++i) {
        k.r.pass();
        ASSERT_TRUE(k.r.takeViewAnswers().empty()) << "held at pass " << i;
    }
    std::optional<WorkerReturn> expired;
    for (int i = 0; i < 30 && !expired; ++i) {
        k.r.pass();
        auto a = k.r.takeViewAnswers();
        if (!a.empty()) expired = std::move(a.front());
    }
    ASSERT_TRUE(expired);
    EXPECT_EQ(expired->errorCode, int16_t(RS::kNotReady));
    EXPECT_EQ(k.r.worker->heldViewReadsForTest(), 0u);
    k.r.holdJudge(false);

    // minOffset alone (the fold is at the committed bound): an offset nobody wrote is held, then NotReady - never the row now.  This
    // leg is the one that fails when minOffset is ignored (the legs above are also held by the unfolded committed tail).
    ASSERT_TRUE(k.r.driveUntilJudged());
    for (int i = 0; i < 20; ++i) k.r.pass();
    ASSERT_NE(k.rowRemaining(1), UINT64_MAX);
    const int64_t never = k.r.leo() + 1000;
    (void)k.r.takeViewAnswers();
    k.r.postViewReadOp(WorkerOp::Type::GetViewRowK, kView, bytesOf(1), never);
    for (int i = 0; i < 20; ++i) {
        k.r.pass();
        ASSERT_TRUE(k.r.takeViewAnswers().empty()) << "held at pass " << i;
    }
    EXPECT_EQ(k.r.worker->heldViewReadsForTest(), 1u);
    std::optional<WorkerReturn> late;
    for (int i = 0; i < 200 && !late; ++i) {
        k.r.pass();
        auto a = k.r.takeViewAnswers();
        if (!a.empty()) late = std::move(a.front());
    }
    ASSERT_TRUE(late);
    EXPECT_EQ(late->errorCode, int16_t(RS::kNotReady));
}

// 3. An index that is not ready never serves the row it holds, and serves Ok once it is ready again.  Not ready: an update of a key with
// no row, appended with the judge held (the hook cannot track it), as the emitter's NotReadyGate cell.  The index still serves this leadership
// (only the judge is behind), so the read is HELD like the read-your-writes barrier, and answers NotReady at the hold deadline (100 ms).
// Mutation: drop `|| !lk.ready` in the lease branch -> the stale row is served: red.  Mutation: answer NotReady at once for CatchingUp ->
// the read is answered inside the hold: red (a read racing the judge by one pass failed: Cells/LeaseMatrixRecipeRegression *_StaleVer).
TEST(LeaseViewReadRegression, NotReadyNeverStaleRegression) {
    Lk k(noCheckpoints());
    grantKey1(k);
    ASSERT_EQ(statusOf(readOnce(k, WorkerOp::Type::GetViewRowK, bytesOf(1))), int16_t(RS::kOk));
    k.r.holdJudge(true);
    (void)k.send({ltr::updateRec(99, 0)});   // no row for key 99
    k.r.passes(30);
    ASSERT_FALSE(k.r.leaseReady());
    const auto heldThenNotReady = [&](uint64_t id) {
        (void)k.r.takeViewAnswers();
        k.r.postViewReadOp(WorkerOp::Type::GetViewRowK, kView, bytesOf(id));
        for (int i = 0; i < 90; ++i) {
            k.r.pass();
            if (!k.r.takeViewAnswers().empty()) return std::string("answered inside the hold at pass ") + std::to_string(i);
        }
        if (k.r.worker->heldViewReadsForTest() != 1u) return std::string("not held");
        for (int i = 0; i < 30; ++i) {
            k.r.pass();
            auto a = k.r.takeViewAnswers();
            if (a.empty()) continue;
            return a.front().errorCode == int16_t(RS::kNotReady) ? std::string() : "answered " + std::to_string(a.front().errorCode);
        }
        return std::string("never answered");
    };
    EXPECT_EQ(heldThenNotReady(1), "") << "key 1 has a row, but the index is not ready";
    EXPECT_EQ(heldThenNotReady(99), "");
    k.r.holdJudge(false);
    ASSERT_TRUE(k.r.driveUntilJudged());
    for (int i = 0; i < ltr::kCap && !k.r.leaseReady(); ++i) k.r.pass();
    EXPECT_EQ(statusOf(readOnce(k, WorkerOp::Type::GetViewRowK, bytesOf(1))), int16_t(RS::kOk));
}

// 4. Only the leader of the source partition, under a read lease, serves: a follower and a lapsed lease answer the internal not-leader
// sentinel (the gateway redirects), as a plan view does.
// Mutation: drop the role check -> the follower serves its (frozen) index or NotReady instead of the sentinel: red.
TEST(LeaseViewReadRegression, NotLeaderAndLeaseRegression) {
    {
        Lk k(noCheckpoints());
        grantKey1(k);
        k.r.demote();
        EXPECT_EQ(statusOf(readOnce(k, WorkerOp::Type::GetViewRowK, bytesOf(1))), kViewRowNotLeaderInternal) << "an ex-leader";
    }
    Lk k(noCheckpoints());
    grantKey1(k);
    k.r.worker->requireViewReadLease();
    EXPECT_EQ(statusOf(readOnce(k, WorkerOp::Type::GetViewRowK, bytesOf(1))), kViewRowNotLeaderInternal) << "required, none held yet";
    WorkerOp lease;
    lease.type = WorkerOp::Type::ReadLease;
    lease.nextOffset = k.leaseNow + 100'000'000;
    ASSERT_TRUE(k.r.worker->tryEnqueueOp(std::move(lease)));
    EXPECT_EQ(statusOf(readOnce(k, WorkerOp::Type::GetViewRowK, bytesOf(1))), int16_t(RS::kOk)) << "under the lease";
    k.leaseNow += 100'000'000;
    EXPECT_EQ(statusOf(readOnce(k, WorkerOp::Type::GetViewRowK, bytesOf(1))), kViewRowNotLeaderInternal) << "at the end of the lease";
    EXPECT_GE(k.r.worker->viewReadsWithoutLeaseForTest(), 2u);
}

// 5. The fingerprint pin: the lease view's current fingerprint (WorkerViewSet::leaseViewFingerprint) serves, another non-zero one is
// SchemaMismatch, 0 is unpinned.
// Mutation: drop the pin check -> the wrong pin is served: red.
TEST(LeaseViewReadRegression, FingerprintPinRegression) {
    Lk k(noCheckpoints());
    grantKey1(k);
    const uint64_t fp = k.r.vs->leaseViewFingerprint(kSrc, 0);
    ASSERT_NE(fp, 0u);
    EXPECT_EQ(statusOf(readOnce(k, WorkerOp::Type::GetViewRowK, bytesOf(1), -1, fp)), int16_t(RS::kOk));
    EXPECT_EQ(statusOf(readOnce(k, WorkerOp::Type::GetViewRowK, bytesOf(1), -1, fp + 1)), int16_t(RS::kSchemaMismatch));
    EXPECT_EQ(statusOf(readOnce(k, WorkerOp::Type::GetViewRowK, bytesOf(1), -1, 0)), int16_t(RS::kOk)) << "0 = unpinned";
    EXPECT_EQ(statusOf(readOnce(k, WorkerOp::Type::GetViewRow, bytesOf(1), -1, fp)), int16_t(RS::kOk));
    EXPECT_EQ(statusOf(readOnce(k, WorkerOp::Type::GetViewRow, bytesOf(1), -1, ~fp)), int16_t(RS::kSchemaMismatch));
    EXPECT_EQ(k.r.vs->leaseViewFingerprint(kSrc, 7), 0u) << "no index on another partition";
}

// 6. ScanViewRows of a lease view is not served (Task 13b's ScanLeases is the scan): UnknownView.
// Mutation: serve the scan through scanPage in the lease branch -> not UnknownView: red.
TEST(LeaseViewReadRegression, ScanViewRowsOnLeaseViewUnknownRegression) {
    Lk k(noCheckpoints());
    grantKey1(k);
    EXPECT_EQ(statusOf(readOnce(k, WorkerOp::Type::ScanViewRows, bytesOf(1))), int16_t(RS::kUnknownView));
    EXPECT_EQ(statusOf(readOnce(k, WorkerOp::Type::ScanViewRows, {})), int16_t(RS::kUnknownView));
}

// 10. AlterSchema adds a user column: the lease index rebuilds over the wider layout, so the lease view's fingerprint changes; a read pinned to
// the old fingerprint is SchemaMismatch, an unpinned one gets the wider row.
// Mutation: compute the fingerprint once per instance from the spec's key field only (not the layout source) -> the fingerprint stays: red.
TEST(LeaseViewReadRegression, LeaseViewFingerprintEvolvesRegression) {
    Lk k(noCheckpoints());
    grantKey1(k);
    const uint64_t before = k.r.vs->leaseViewFingerprint(kSrc, 0);
    const auto a0 = readOnce(k, WorkerOp::Type::GetViewRowK, bytesOf(1), -1, before);
    ASSERT_TRUE(a0);
    ASSERT_EQ(a0->errorCode, int16_t(RS::kOk));
    const std::size_t narrow = a0->chunk.size();

    k.r.addLeasePort();
    std::optional<WorkerReturn> wide;
    uint64_t after = before;
    for (int i = 0; i < 400; ++i) {   // the rebuild replays the log under the new layout; NotReady until it is done
        wide = readOnce(k, WorkerOp::Type::GetViewRowK, bytesOf(1), -1, 0, 2);
        after = k.r.vs->leaseViewFingerprint(kSrc, 0);
        if (wide && wide->errorCode == int16_t(RS::kOk) && after != before) break;
    }
    ASSERT_TRUE(wide);
    ASSERT_EQ(wide->errorCode, int16_t(RS::kOk));
    EXPECT_NE(after, before) << "the fingerprint follows the layout";
    EXPECT_GT(wide->chunk.size(), narrow) << "the wider row";
    EXPECT_EQ(statusOf(readOnce(k, WorkerOp::Type::GetViewRowK, bytesOf(1), -1, before)), int16_t(RS::kSchemaMismatch)) << "the old pin";
    EXPECT_EQ(statusOf(readOnce(k, WorkerOp::Type::GetViewRowK, bytesOf(1), -1, after)), int16_t(RS::kOk)) << "the new pin";
}


// ---- Task 13a review fixes ------------------------------------------------------------------------------------------------------

namespace {

/// The lease source descriptor of the rig with the key field's type and the user column's / schema's names chosen.
rune::codec::Descriptor leaseSrc(uint16_t latest, const char* schemaName = "inst", const char* addrName = "addr",
                                 rune::codec::PrimType keyType = rune::codec::PrimType::UInt64) {
    using rune::codec::PrimType;
    rune::codec::DescriptorBuilder d(schemaName, latest);
    d.add("lease_version", PrimType::Int64).add("lease_ttl_ms", PrimType::UInt64).add("lease_holder", PrimType::UInt64)
        .add("lease_remaining_ms", PrimType::UInt64).add("id", keyType, 1, true).addFixedStr(addrName, 16);
    if (latest >= 2) d.add("port", PrimType::UInt32, 2);
    return d.build();
}

void waitReady(Lk& k) {
    for (int i = 0; i < ltr::kCap && !k.r.leaseReady(); ++i) k.r.pass();
    ASSERT_TRUE(k.r.leaseReady());
}
/// Passes until the index is ready AND its fingerprint differs from `old` (a rebuild over a new layout), hard-capped.
uint64_t waitNewFingerprint(Lk& k, uint64_t old) {
    for (int i = 0; i < 2000; ++i) {
        k.r.pass();
        const uint64_t fp = k.r.vs->leaseViewFingerprint(kSrc, 0);
        if (fp != 0 && fp != old && k.r.leaseReady()) return fp;
    }
    return 0;
}

}  // namespace

// 11. M2: the lease view fingerprint is a pure function of the row LAYOUT (field ids, column types, widths, offsets of the lease index row):
// it survives a demote + promote (a rebuild of the index), a rename of a user column with a forced rebuild, and it is the same whichever
// of the two schema installs (SchemaUpdate = the writer version, FilterSchema = the evolving blob) reaches the worker first.  A pinned
// read keeps working through each of them.
// Mutation: hash a field NAME into leaseViewLayoutFingerprint -> the rename leg turns red.
TEST(LeaseViewReadRegression, FingerprintStableAcrossRebuildRegression) {
    Lk k(noCheckpoints());
    grantKey1(k);
    const uint64_t fp0 = k.r.vs->leaseViewFingerprint(kSrc, 0);
    ASSERT_NE(fp0, 0u);
    EXPECT_EQ(fp0, leaseViewFingerprintOf(VerdictRig::leaseSourceDescriptor(1))) << "the worker's fingerprint is the layout function";

    k.r.demotePromote();
    waitReady(k);
    EXPECT_EQ(k.r.vs->leaseViewFingerprint(kSrc, 0), fp0) << "a demote + promote rebuilds the index over the same layout";
    EXPECT_EQ(statusOf(readOnce(k, WorkerOp::Type::GetViewRowK, bytesOf(1), -1, fp0)), int16_t(RS::kOk)) << "the pin from before survives";

    // RENAME a user column and the schema; the evolving blob arrives, a new leadership forces the rebuild.
    k.r.leaseFilterSchema(leaseSrc(1, "inst_renamed", "address"), 1);
    k.r.passes(10);
    k.r.demotePromote();
    waitReady(k);
    EXPECT_EQ(k.r.vs->leaseViewFingerprint(kSrc, 0), fp0) << "names are not part of the fingerprint";
    EXPECT_EQ(statusOf(readOnce(k, WorkerOp::Type::GetViewRowK, bytesOf(1), -1, fp0)), int16_t(RS::kOk));

    // FilterSchema before vs after the versions: two rigs, one ADD of `port`, the two installs in either order.
    const rune::codec::Descriptor v2 = leaseSrc(2);
    uint64_t fpA = 0, fpB = 0;
    {
        Lk a(noCheckpoints());
        grantKey1(a);
        a.r.leaseFilterSchema(v2, 2);
        a.r.leaseSchemaUpdate(v2, 2);
        fpA = waitNewFingerprint(a, fp0);
    }
    {
        Lk b(noCheckpoints());
        grantKey1(b);
        b.r.leaseSchemaUpdate(v2, 2);
        b.r.leaseFilterSchema(v2, 2);
        fpB = waitNewFingerprint(b, fp0);
    }
    ASSERT_NE(fpA, 0u);
    ASSERT_NE(fpB, 0u);
    EXPECT_EQ(fpA, fpB) << "the install order does not move the fingerprint";
    EXPECT_EQ(fpA, leaseViewFingerprintOf(v2));
    EXPECT_NE(fpA, fp0) << "an ADD does";
}

// 12. The fingerprint is pinned by a golden value: the layout of the rig's lease topic at version 1 and 2 (the four system columns, id, addr
// [, port] and the two offset columns).  A change of the hashed bytes (the encoding below) or of the lease system columns moves it.
// Mutation: hash the slot index instead of the field id / drop the offset from the bytes -> red.
TEST(LeaseViewReadRegression, LeaseViewFingerprintGoldenRegression) {
    EXPECT_EQ(leaseViewFingerprintOf(leaseSrc(1)), 0x6141a01b41ea80bcull) << "v1 layout";
    EXPECT_EQ(leaseViewFingerprintOf(leaseSrc(2)), 0xff8cf5f8f7c7dbebull) << "v2 layout (port added)";
    // names, the schema name and sinceVersion never matter
    EXPECT_EQ(leaseViewFingerprintOf(leaseSrc(1, "other", "other_name")), leaseViewFingerprintOf(leaseSrc(1)));
    // a different key type changes the column type of the id slot
    EXPECT_NE(leaseViewFingerprintOf(leaseSrc(1, "inst", "addr", rune::codec::PrimType::UInt32)), leaseViewFingerprintOf(leaseSrc(1)));
}

// 13. M3: the fingerprint pin is checked AFTER leadership, the read lease and readiness, against the READY index's layout.  After an AlterSchema
// ADD whose rebuild cannot finish (the judge is held), a read pinned to the NEW fingerprint is NotReady (retriable), not SchemaMismatch; a
// demoted partition answers the not-leader sentinel whatever the pin; once the rebuild is done the new pin is served and the old one is
// SchemaMismatch.
// Mutation: check the pin first (against the stale index's fingerprint) -> the first leg answers SchemaMismatch and the demoted one too: red.
TEST(LeaseViewReadRegression, PinCheckedAfterReadinessAndLeadershipRegression) {
    Lk k(noCheckpoints());
    grantKey1(k);
    const uint64_t oldFp = k.r.vs->leaseViewFingerprint(kSrc, 0);
    const uint64_t newFp = leaseViewFingerprintOf(leaseSrc(2));
    ASSERT_NE(oldFp, newFp);
    k.r.holdJudge(true);
    k.r.addLeasePort();
    k.r.passes(30);
    ASSERT_FALSE(k.r.leaseReady()) << "the rebuild for the wider layout waits for the judge";
    EXPECT_EQ(statusOf(readOnce(k, WorkerOp::Type::GetViewRowK, bytesOf(1), -1, newFp)), int16_t(RS::kNotReady)) << "pinned to the new layout";
    EXPECT_EQ(statusOf(readOnce(k, WorkerOp::Type::GetViewRowK, bytesOf(1), -1, oldFp)), int16_t(RS::kNotReady)) << "pinned to the old layout";
    k.r.holdJudge(false);
    ASSERT_TRUE(k.r.driveUntilJudged());
    ASSERT_EQ(waitNewFingerprint(k, oldFp), newFp);
    EXPECT_EQ(statusOf(readOnce(k, WorkerOp::Type::GetViewRowK, bytesOf(1), -1, newFp)), int16_t(RS::kOk));
    EXPECT_EQ(statusOf(readOnce(k, WorkerOp::Type::GetViewRowK, bytesOf(1), -1, oldFp)), int16_t(RS::kSchemaMismatch));

    k.r.demote();
    EXPECT_EQ(statusOf(readOnce(k, WorkerOp::Type::GetViewRowK, bytesOf(1), -1, newFp + 1)), kViewRowNotLeaderInternal) << "demoted, wrong pin";
    EXPECT_EQ(statusOf(readOnce(k, WorkerOp::Type::GetViewRowK, bytesOf(1), -1, oldFp)), kViewRowNotLeaderInternal) << "demoted, stale pin";
}

// 14. L3: a GetViewRow (u64 key) of a lease topic whose key is a fixed-width integer NARROWER than 8 bytes encodes the u64 to the key's width
// (little-endian: its wire form); a value that does not fit the key's type is InvalidRequest.  UInt32 / UInt16: the unsigned range;
// Int32: the sign-extension of the int32 range, as a plan view's LegacyU64 key reads it.  A GetViewRowK of the key's own width is served;
// of 8 bytes it is InvalidRequest (a key of another width than the table's).
// Mutation: always send the 8 bytes (drop the narrowing) -> every in-range read is InvalidRequest: red.
TEST(LeaseViewReadRegression, NarrowIntegerKeyU64ReadRegression) {
    using rune::codec::PrimType;
    struct Case { PrimType type; uint32_t width; std::vector<uint64_t> granted; std::vector<uint64_t> absent; std::vector<uint64_t> bad; };
    const std::vector<Case> cases = {
        {PrimType::UInt32, 4, {7, 0xFFFFFFFFull}, {8}, {0x1'0000'0000ull, 0x1'0000'0007ull, UINT64_MAX}},
        {PrimType::UInt16, 2, {7, 0xFFFFull}, {8}, {0x1'0000ull, 0x1'0007ull, UINT64_MAX}},
        {PrimType::Int32, 4, {7, 0xFFFFFFFFFFFFFFFBull /* -5 */, 0x7FFFFFFFull}, {8}, {0x1'0000'0000ull, 0xFFFFFFFFull /* 4294967295 */, 0x8000'0000ull}},
    };
    for (const Case& c : cases) {
        SCOPED_TRACE(testing::Message() << "key width " << c.width << " type " << int(c.type));
        Lk k(noCheckpoints(), false, {}, 64LL * 1024 * 1024, KeyShape{c.type}, leaseSrc(1, "inst", "addr", c.type));
        std::vector<RawRec> grants;
        for (uint64_t id : c.granted) {
            std::string key(c.width, '\0');
            std::vector<uint8_t> v(4u + 32u + c.width + 16u, 0);
            ltr::putLe(v, 0, 32u + c.width + 16u, 2);
            ltr::putLe(v, 2, 1, 2);
            ltr::putLe(v, 12, 5000, 8);
            ltr::putLe(v, 20, 1, 8);
            ltr::putLe(v, 36, id, c.width);
            v[36 + c.width] = 'a';
            for (uint32_t i = 0; i < c.width; ++i) key[i] = char(id >> (8 * i));
            grants.push_back({key, v, 1});
        }
        ASSERT_TRUE(k.produce(grants));
        ASSERT_TRUE(k.r.driveUntilJudged());
        waitReady(k);
        const auto lk = k.r.vs->leaseIndexFor(kSrc, 0, k.r.replica->leadershipGeneration(), true, true);
        ASSERT_NE(lk.ref.table, nullptr);
        ASSERT_EQ(lk.ref.table->keyWidth(), c.width);
        const auto u64Bytes = [](uint64_t v) { std::vector<uint8_t> b(8); for (int i = 0; i < 8; ++i) b[std::size_t(i)] = uint8_t(v >> (8 * i)); return b; };
        for (uint64_t id : c.granted) {
            const auto a = readOnce(k, WorkerOp::Type::GetViewRow, u64Bytes(id));
            ASSERT_TRUE(a) << id;
            EXPECT_EQ(a->errorCode, int16_t(RS::kOk)) << "u64 " << id;
            EXPECT_EQ(a->recordCount, int32_t(lk.ref.layout->rowLen));
            std::vector<uint8_t> wire(c.width);
            for (uint32_t i = 0; i < c.width; ++i) wire[i] = uint8_t(id >> (8 * i));
            const auto kk = readOnce(k, WorkerOp::Type::GetViewRowK, wire);
            ASSERT_TRUE(kk) << id;
            EXPECT_EQ(kk->errorCode, int16_t(RS::kOk)) << "the key's own wire form";
            EXPECT_EQ(kk->chunk, a->chunk) << "the same row";
        }
        for (uint64_t id : c.absent) EXPECT_EQ(statusOf(readOnce(k, WorkerOp::Type::GetViewRow, u64Bytes(id))), int16_t(RS::kAbsentKey)) << "u64 " << id;
        for (uint64_t id : c.bad) EXPECT_EQ(statusOf(readOnce(k, WorkerOp::Type::GetViewRow, u64Bytes(id))), int16_t(RS::kInvalidRequest)) << "u64 " << id;
        EXPECT_EQ(statusOf(readOnce(k, WorkerOp::Type::GetViewRowK, std::vector<uint8_t>(8, 0))), int16_t(RS::kInvalidRequest)) << "8-byte wire key";
    }
}

// 15. M4 (worker cells): a read HELD behind the fold is re-served every pass with every check re-run.  Demoted while held -> the not-leader
// sentinel; the topic dropped while held -> UnknownView (the lease view mapping is gone with the topic); an AlterSchema ADD whose rebuild
// cannot finish while held -> NotReady at once (not at the 100 ms deadline).
// Mutation: skip the readiness / role check on a re-serve (answer the held read from the old index) -> red.
TEST(LeaseViewReadRegression, HeldReadReServeCellsRegression) {
    const auto holdOne = [](Lk& k) {
        const int64_t never = k.r.leo() + 1000;
        (void)k.r.takeViewAnswers();
        k.r.postViewReadOp(WorkerOp::Type::GetViewRowK, kView, bytesOf(1), never);
        for (int i = 0; i < 5; ++i) {
            k.r.pass();
            ASSERT_TRUE(k.r.takeViewAnswers().empty());
        }
        ASSERT_EQ(k.r.worker->heldViewReadsForTest(), 1u);
    };
    const auto answerWithin = [](Lk& k, int passes) -> std::optional<WorkerReturn> {
        for (int i = 0; i < passes; ++i) {
            k.r.pass();
            auto a = k.r.takeViewAnswers();
            if (!a.empty()) return std::move(a.front());
        }
        return std::nullopt;
    };
    {   // demoted while held
        Lk k(noCheckpoints());
        grantKey1(k);
        holdOne(k);
        k.r.demote();
        const auto a = answerWithin(k, 20);
        ASSERT_TRUE(a);
        EXPECT_EQ(a->errorCode, kViewRowNotLeaderInternal);
        EXPECT_EQ(k.r.worker->heldViewReadsForTest(), 0u);
    }
    {   // dropped while held
        Lk k(noCheckpoints());
        grantKey1(k);
        holdOne(k);
        WorkerOp drop;
        drop.type = WorkerOp::Type::TopicDropped; drop.topicId = kSrc;
        ASSERT_TRUE(k.r.worker->tryEnqueueOp(std::move(drop)));
        const auto a = answerWithin(k, 20);
        ASSERT_TRUE(a);
        EXPECT_EQ(a->errorCode, int16_t(RS::kUnknownView));
        EXPECT_EQ(k.r.worker->heldViewReadsForTest(), 0u);
        EXPECT_EQ(statusOf(readOnce(k, WorkerOp::Type::GetViewRowK, bytesOf(1))), int16_t(RS::kUnknownView)) << "a new read too";
    }
    {   // an AlterSchema ADD while held, the rebuild blocked
        Lk k(noCheckpoints());
        grantKey1(k);
        holdOne(k);
        k.r.holdJudge(true);
        k.r.addLeasePort();
        const auto a = answerWithin(k, 40);
        ASSERT_TRUE(a);
        EXPECT_EQ(a->errorCode, int16_t(RS::kNotReady));
        EXPECT_EQ(k.r.worker->heldViewReadsForTest(), 0u);
        k.r.holdJudge(false);
    }
}
