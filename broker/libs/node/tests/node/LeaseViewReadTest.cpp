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

// 3. An index that is not ready answers NotReady - never the row it holds - and Ok once it is ready again.  Not ready: an update of a key with
// no row, appended with the judge held (the hook cannot track it), as the emitter's NotReadyGate cell.
// Mutation: drop `|| !lk.ready` in the lease branch -> the stale row is served: red.
TEST(LeaseViewReadRegression, NotReadyNeverStaleRegression) {
    Lk k(noCheckpoints());
    grantKey1(k);
    ASSERT_EQ(statusOf(readOnce(k, WorkerOp::Type::GetViewRowK, bytesOf(1))), int16_t(RS::kOk));
    k.r.holdJudge(true);
    (void)k.send({ltr::updateRec(99, 0)});   // no row for key 99
    k.r.passes(30);
    ASSERT_FALSE(k.r.leaseReady());
    EXPECT_EQ(statusOf(readOnce(k, WorkerOp::Type::GetViewRowK, bytesOf(1))), int16_t(RS::kNotReady)) << "key 1 has a row, but the index is not ready";
    EXPECT_EQ(statusOf(readOnce(k, WorkerOp::Type::GetViewRowK, bytesOf(99))), int16_t(RS::kNotReady));
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
