// Lease topics Task 13b: ScanLeases (worker op 38) - the keyset scan of a lease topic's index in 64-bit hash order.
//
// Real worker, real lease index, real judge; the lease clock is driven by hand and every drive loop is capped.  The order hash is
// scanHash(key, sourceTopicId) (LeaseHash.h): independent of the table's own hash seed, so a resume token (lastHash) is valid on any
// replica after a leader change.
//
// Interaction matrix (ScanLeases x ...):
//   | scan input              | other state                          | outcome                                    | test |
//   | all pages, maxRows 64   | 300 keys                             | ascending hash, every key once             | HashOrderPagesRegression
//   | equal hashes (seam)     | tie group at the page boundary       | the whole group joins the page             | HashTiesInOnePageRegression
//   | resume token            | other leader (other table seed)      | the union is every key once                | ResumeAcrossLeaderChangeRegression
//   | resume token            | table grows between pages            | original keys once, in order               | ResumeAcrossTableGrowRegression
//   | key prefix              | 1 / 2 / 9-byte prefixes              | filtered on the broker                     | PrefixFilteredOnBrokerRegression
//   | asOfOffset              | after a single-record batch          | = the batch offset + 1 (exclusive)         | AsOfExclusiveRegression
//   | resume token            | revoke / grant between pages         | no duplicate, no unchanged key lost        | ChurnBetweenPagesRegression
//   | maxRows                 | 0 / 1 / 1024 / 1025                  | InvalidRequest / ok / ok / InvalidRequest  | MaxRowsBoundsRegression
//   | byte cap                | cap inside a tie group / group > cap | page ends before it / MessageTooLarge      | ByteCapStopsOnHashBoundaryRegression
//   | gates                   | dropped / follower / lease ended / not ready | UnknownTopic / NotLeader x3        | ScanGatesRegression
//   | steady state            | warm                                 | zero allocations                           | ScanNoAllocRegression
//   | the hash                | fixed keys                           | pinned values                              | ScanHashGoldenRegression
#include "LeaseRig.h"
#include "AllocWindow.h"

#include <rune/node/lease/LeaseHash.h>
#include <rune/protocol/client/LeaseMessages.h>

#include <algorithm>
#include <map>
#include <set>

using namespace rune::node;
using namespace rune::node::cw;
using ltr::Lk;
namespace pc = rune::protocol::client;

namespace {

constexpr int32_t kSrc = VerdictRig::kSrc;
constexpr int16_t kNotLeader = int16_t(pc::ErrorCode::NotLeader);
constexpr int16_t kUnknownTopic = int16_t(pc::ErrorCode::UnknownTopic);
constexpr int16_t kInvalid = int16_t(pc::ErrorCode::InvalidRequest);
constexpr int16_t kTooLarge = int16_t(pc::ErrorCode::MessageTooLarge);
constexpr int16_t kInternal = int16_t(pc::ErrorCode::Internal);

rune::protocol::LeaseSpec spec(uint32_t maxKeys = 2000) { return ltr::specWith(maxKeys, 0, 300'000, 1000); }

std::vector<uint8_t> kb(uint64_t id) {
    const std::string s = ltr::keyOf(id);
    return std::vector<uint8_t>(s.begin(), s.end());
}
uint64_t hashOf(uint64_t id) { return lease::scanHash(kb(id), kSrc); }

void ready(Lk& k) {
    ASSERT_TRUE(k.r.driveUntilJudged());
    for (int i = 0; i < ltr::kCap && !k.r.leaseReady(); ++i) k.r.pass();
    ASSERT_TRUE(k.r.leaseReady());
}
/// Grants ids [first, first + n) in one go, then waits for the ready index.
void fill(Lk& k, uint64_t n, uint64_t first = 1) {
    ASSERT_TRUE(k.grantMany(n, 5000, first));
    ready(k);
}
std::vector<uint64_t> byHash(std::vector<uint64_t> ids) {
    std::sort(ids.begin(), ids.end(), [](uint64_t a, uint64_t b) { return hashOf(a) < hashOf(b); });
    return ids;
}
std::vector<uint64_t> range(uint64_t first, uint64_t n) {
    std::vector<uint64_t> v;
    for (uint64_t i = 0; i < n; ++i) v.push_back(first + i);
    return v;
}

}  // namespace

// 1. 300 keys in pages of 64: five pages, ascending by hash across and within pages, every key exactly once, lastHash = the page's last hash,
// done only on the last page.
// Mutation: order the walk by rid instead of by hash (Worker::executeScanLeases_) -> the union order is not the hash order: red.
TEST(ScanLeasesRegression, HashOrderPagesRegression) {
    Lk k(spec(1000));
    fill(k, 300);
    std::vector<Lk::ScanPage> pages;
    const std::vector<uint64_t> got = k.scanAll(64, &pages);
    ASSERT_EQ(pages.size(), 5u);
    for (std::size_t i = 0; i < pages.size(); ++i) {
        EXPECT_EQ(pages[i].rows.size(), i < 4 ? 64u : 44u) << "page " << i;
        EXPECT_EQ(pages[i].done, i == 4) << "page " << i;
        ASSERT_FALSE(pages[i].rows.empty());
        EXPECT_EQ(pages[i].lastHash, hashOf(pages[i].ids().back())) << "page " << i;
    }
    EXPECT_EQ(got, byHash(range(1, 300)));
    EXPECT_EQ(std::set<uint64_t>(got.begin(), got.end()).size(), 300u);
    for (std::size_t i = 1; i < pages.size(); ++i) EXPECT_EQ(pages[i].asOf, pages[0].asOf) << "no writes: one as-of";
    EXPECT_EQ(k.r.worker->scanLeasesForTest(), 5u);
}

namespace {
// The seam hash: ids 1, 2 -> 10, 20; ids 3..(2 + kTies) -> 100; ids above -> 1000 + id.  Keys are the 8-byte LE ids.
uint64_t gTieFrom = 3, gTieTo = 7;
uint64_t tieHash(std::span<const uint8_t> key, int32_t) {
    uint64_t id = 0;
    std::memcpy(&id, key.data(), std::min<std::size_t>(8, key.size()));
    if (id == 1) return 10;
    if (id == 2) return 20;
    if (id >= gTieFrom && id <= gTieTo) return 100;
    return 1000 + id;
}
}  // namespace

// 2. Equal hashes (through the seam; the real hash is 64-bit): five keys share a hash, maxRows 3.  The page that reaches the tie group takes
// the WHOLE group (7 rows), whatever the insertion order, and the next page starts after it; nothing is lost or repeated.
// Over the tie headroom (> 64 beyond maxRows) the page is refused Internal and counted - never a wrong page.
// Mutation: drop the tie list (Worker::executeScanLeases_ addTie) -> the group is cut at maxRows and the rest is lost on resume: red.
TEST(ScanLeasesRegression, HashTiesInOnePageRegression) {
    gTieFrom = 3; gTieTo = 7;
    const std::vector<std::vector<uint64_t>> orders = {{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, {10, 9, 8, 7, 6, 5, 4, 3, 2, 1}, {5, 1, 7, 9, 3, 10, 2, 6, 8, 4}};
    for (const auto& order : orders) {
        Lk k(spec(100));
        k.r.worker->setScanHashForTest(&tieHash);
        std::vector<RawRec> recs;
        for (uint64_t id : order) recs.push_back(ltr::grantRec(id));
        ASSERT_TRUE(k.produce(recs));
        ready(k);
        const Lk::ScanPage p1 = k.scan({}, 3, 0, true);
        ASSERT_EQ(p1.code, 0);
        std::vector<uint64_t> ids = p1.ids();
        ASSERT_EQ(ids.size(), 7u) << "1, 2 and the whole tie group";
        EXPECT_EQ(ids[0], 1u);
        EXPECT_EQ(ids[1], 2u);
        EXPECT_EQ(std::set<uint64_t>(ids.begin() + 2, ids.end()), (std::set<uint64_t>{3, 4, 5, 6, 7}));
        EXPECT_EQ(p1.lastHash, 100u);
        EXPECT_FALSE(p1.done);
        const Lk::ScanPage p2 = k.scan({}, 3, p1.lastHash, false);
        EXPECT_EQ(p2.ids(), (std::vector<uint64_t>{8, 9, 10}));
        EXPECT_TRUE(p2.done);
        EXPECT_EQ(k.r.worker->scanTieOverflowForTest(), 0u);
    }
    {   // 67 equal hashes: maxRows 3 + the 64-entry tie list - served whole; 68: refused
        for (const uint64_t n : {67u, 68u}) {
            gTieFrom = 1000; gTieTo = 1000 + n - 1;
            Lk k(spec(200));
            k.r.worker->setScanHashForTest(&tieHash);
            ASSERT_TRUE(k.grantMany(n, 5000, 1000));
            ready(k);
            const Lk::ScanPage p = k.scan({}, 3, 0, true);
            if (n == 67) {
                EXPECT_EQ(p.code, 0);
                EXPECT_EQ(p.rows.size(), 67u);
                EXPECT_TRUE(p.done);
            } else {
                EXPECT_EQ(p.code, kInternal);
                EXPECT_TRUE(p.done) << "every refusal carries done";
                EXPECT_EQ(k.r.worker->scanTieOverflowForTest(), 1u);
            }
        }
    }
}

// 3. A resume token is the last hash: two leaders with DIFFERENT table seeds (so different rids and probe orders) serve alternate pages of
// one scan, and the union is every key once, in hash order.
// Mutation: hash with the table's seed instead of the topic id (Worker::executeScanLeases_) -> the order differs per replica: red.
TEST(ScanLeasesRegression, ResumeAcrossLeaderChangeRegression) {
    RigTuning ta, tb;
    ta.hashSeed = 0;
    tb.hashSeed = 0x1234abcdULL;
    Lk a(spec(1000), false, ta), b(spec(1000), false, tb);
    EXPECT_NE(a.r.vs->hashSeedForTest(), b.r.vs->hashSeedForTest());
    fill(a, 200);
    fill(b, 200);
    std::vector<uint64_t> got;
    uint64_t resume = 0;
    bool first = true, done = false;
    for (int i = 0; i < 64 && !done; ++i) {
        Lk& k = (i % 2 == 0) ? a : b;
        const Lk::ScanPage p = k.scan({}, 16, resume, first);
        ASSERT_EQ(p.code, 0);
        for (uint64_t id : p.ids()) got.push_back(id);
        resume = p.lastHash; first = false; done = p.done;
    }
    ASSERT_TRUE(done);
    EXPECT_EQ(got, byHash(range(1, 200)));
}

// 4. The table grows (and rehashes) between pages: every key of the original set appears exactly once across the pages, in hash order, and
// no key repeats.  New keys above the resume hash may appear; none below.
// Mutation: resume by rid position (a table-order cursor) -> the rehash reorders the walk and keys repeat or vanish: red.
TEST(ScanLeasesRegression, ResumeAcrossTableGrowRegression) {
    Lk k(spec(9000));
    fill(k, 100);
    Lk::ScanPage p1 = k.scan({}, 32, 0, true);
    ASSERT_EQ(p1.code, 0);
    std::vector<uint64_t> got = p1.ids();
    ASSERT_TRUE(k.grantMany(5000, 5000, 1000));   // far past the initial table capacity
    ready(k);
    uint64_t resume = p1.lastHash;
    bool done = p1.done;
    for (int i = 0; i < 400 && !done; ++i) {
        const Lk::ScanPage p = k.scan({}, 256, resume, false);
        ASSERT_EQ(p.code, 0);
        for (uint64_t id : p.ids()) got.push_back(id);
        resume = p.lastHash; done = p.done;
    }
    ASSERT_TRUE(done);
    std::map<uint64_t, int> seen;
    for (uint64_t id : got) ++seen[id];
    for (uint64_t id = 1; id <= 100; ++id) EXPECT_EQ(seen[id], 1) << "original key " << id;
    for (const auto& [id, n] : seen) EXPECT_EQ(n, 1) << "key " << id << " repeated";
    for (std::size_t i = 1; i < got.size(); ++i) EXPECT_LT(hashOf(got[i - 1]), hashOf(got[i])) << "at " << i;
}

// 5. The prefix filters on the broker: keys are 8-byte LE ids, so a 1-byte prefix is the low byte.  ids 5 and 261 share low byte 5; 261 alone
// has the 2-byte prefix {5, 1}; a prefix longer than the key matches nothing; an empty result is done with lastHash = the resume hash.
// Mutation: ignore the prefix (Worker::executeScanLeases_) -> every key is returned: red.
TEST(ScanLeasesRegression, PrefixFilteredOnBrokerRegression) {
    Lk k(spec(1000));
    fill(k, 300);
    const auto ids = [&](std::vector<uint8_t> prefix) {
        std::vector<uint64_t> v = k.scanAll(64, nullptr, prefix);
        std::sort(v.begin(), v.end());
        return v;
    };
    EXPECT_EQ(ids({5}), (std::vector<uint64_t>{5, 261}));
    EXPECT_EQ(ids({5, 1}), (std::vector<uint64_t>{261}));
    EXPECT_EQ(ids({0xFF}), (std::vector<uint64_t>{255}));
    EXPECT_EQ(ids({0, 0, 0, 0, 0, 0, 0, 0, 0}), std::vector<uint64_t>{}) << "a 9-byte prefix is longer than any 8-byte key";
    const std::vector<uint8_t> none = {0xEE, 0x05};   // id 238 has low byte 0xEE; none has second byte 5
    const Lk::ScanPage p = k.scan(none, 64, 12345, false);
    EXPECT_EQ(p.code, 0);
    EXPECT_TRUE(p.rows.empty());
    EXPECT_TRUE(p.done);
    EXPECT_EQ(p.lastHash, 12345u);
}

// 6. asOfOffset is exclusive: after a single-record batch at offset X the scan's as-of is X + 1 (the fold cursor); the row is visible.
// Mutation: report the last folded offset instead of the cursor (Worker::executeScanLeases_) -> X, not X + 1: red.
TEST(ScanLeasesRegression, AsOfExclusiveRegression) {
    Lk k(spec(1000));
    fill(k, 10);
    const Lk::ScanPage before = k.scan({}, 64, 0, true);
    ASSERT_EQ(before.code, 0);
    ASSERT_TRUE(k.produce({ltr::grantRec(5000)}));
    ready(k);
    const int64_t x = k.versionOf(5000);
    ASSERT_GE(x, 0);
    const Lk::ScanPage after = k.scan({}, 64, 0, true);
    ASSERT_EQ(after.code, 0);
    EXPECT_GT(after.asOf, x) << "the page reflects the record at x";
    EXPECT_EQ(after.asOf, k.r.leo()) << "the fold cursor: the first offset not reflected";
    EXPECT_GT(after.asOf, before.asOf);
    const auto ids = after.ids();
    EXPECT_NE(std::find(ids.begin(), ids.end(), 5000u), ids.end());
}

// 7. Churn between pages: a revoke of a key beyond the resume, a revoke of a key already returned, and a new grant between pages.  Nothing
// repeats; every key that was untouched and beyond the resume is returned; the revoked key beyond the resume is not.
// Mutation: skip the liveness test of a rid (leaseRowLive) -> the revoked key is still returned: red.
TEST(ScanLeasesRegression, ChurnBetweenPagesRegression) {
    Lk k(spec(1000));
    fill(k, 300);
    const Lk::ScanPage p1 = k.scan({}, 64, 0, true);
    ASSERT_EQ(p1.code, 0);
    const std::vector<uint64_t> sorted = byHash(range(1, 300));
    const uint64_t seenKey = sorted[10];      // already returned
    const uint64_t goneKey = sorted[200];     // beyond the resume
    const uint64_t goneKey2 = sorted[250];    // (two, so that one erased row id with a stale key above the resume survives the one grant below)
    ASSERT_TRUE(k.produce({ltr::revokeRec(seenKey, k.versionOf(seenKey)), ltr::revokeRec(goneKey, k.versionOf(goneKey)),
                           ltr::revokeRec(goneKey2, k.versionOf(goneKey2))}));
    ASSERT_TRUE(k.produce({ltr::grantRec(7000)}));
    ready(k);
    {   // the scenario really leaves an erased row id behind (the walk must skip it)
        const auto lk = k.r.vs->leaseIndexFor(kSrc, 0, k.r.replica->leadershipGeneration(), true, true);
        ASSERT_NE(lk.ref.table, nullptr);
        uint32_t erased = 0;
        for (uint32_t rid = 0; rid < lk.ref.table->ridHighWater(); ++rid) erased += leaseRowLive(*lk.ref.table, rid) ? 0 : 1;
        ASSERT_GE(erased, 1u);
    }
    std::vector<uint64_t> got = p1.ids();
    uint64_t resume = p1.lastHash;
    bool done = p1.done;
    for (int i = 0; i < 64 && !done; ++i) {
        const Lk::ScanPage p = k.scan({}, 64, resume, false);
        ASSERT_EQ(p.code, 0);
        for (uint64_t id : p.ids()) got.push_back(id);
        resume = p.lastHash; done = p.done;
    }
    ASSERT_TRUE(done);
    std::map<uint64_t, int> seen;
    for (uint64_t id : got) ++seen[id];
    for (const auto& [id, n] : seen) EXPECT_EQ(n, 1) << "key " << id << " repeated";
    EXPECT_EQ(seen.count(goneKey), 0u);
    EXPECT_EQ(seen.count(goneKey2), 0u);
    EXPECT_EQ(got.size(), 300u - 2u + (hashOf(7000) > p1.lastHash ? 1u : 0u)) << "the revoked rows are not rows (no erased rid is returned)";
    for (std::size_t i = 64; i < sorted.size(); ++i)
        if (sorted[i] != goneKey && sorted[i] != goneKey2) EXPECT_EQ(seen[sorted[i]], 1) << "untouched key " << sorted[i];
}

// 8. maxRows bounds on the worker: 1 -> one row per page (a full scan of 40 keys takes 40 pages), 1024 -> one page, 0 and 1025 -> InvalidRequest
// (done), as is a first flag above 1 and a 128-byte prefix.
// Mutation: widen the bound check (maxRows >= 0 / <= 1025) -> 0 or 1025 is served: red.
TEST(ScanLeasesRegression, MaxRowsBoundsRegression) {
    Lk k(spec(1000));
    fill(k, 40);
    std::vector<Lk::ScanPage> pages;
    EXPECT_EQ(k.scanAll(1, &pages), byHash(range(1, 40)));
    EXPECT_EQ(pages.size(), 40u);
    pages.clear();
    EXPECT_EQ(k.scanAll(1024, &pages), byHash(range(1, 40)));
    EXPECT_EQ(pages.size(), 1u);
    for (const int32_t bad : {0, -1, 1025, 100000}) {
        const Lk::ScanPage p = k.scan({}, bad, 0, true);
        EXPECT_EQ(p.code, kInvalid) << "maxRows " << bad;
        EXPECT_TRUE(p.done);
    }
    const std::vector<uint8_t> longPrefix(pc::kScanLeasesMaxPrefixBytes + 1, 1);
    EXPECT_EQ(k.scan(longPrefix, 10, 0, true).code, kInvalid);
    const std::vector<uint8_t> okPrefix(pc::kScanLeasesMaxPrefixBytes, 1);
    EXPECT_EQ(k.scan(okPrefix, 10, 0, true).code, 0);
}

// 9. The byte cap closes a page on a hash boundary: group A (hash 50) fits, A + B (hash 100) does not, B alone does.  The page is A only,
// done 0, lastHash 50; resumed, it is B (done).  A group over the cap on its own is MessageTooLarge (never a split group).
// Mutation: cut the page at the byte cap row by row -> a page ends inside B: red.
namespace {
uint64_t gCapA = 0, gCapB = 0;
uint64_t capHash(std::span<const uint8_t> key, int32_t) {
    uint64_t id = 0;
    std::memcpy(&id, key.data(), std::min<std::size_t>(8, key.size()));
    return id <= gCapA ? 50 : (id <= gCapA + gCapB ? 100 : 1000 + id);
}
}  // namespace
TEST(ScanLeasesRegression, ByteCapStopsOnHashBoundaryRegression) {
    const auto rowBytesOf = [](Lk& k) {
        const auto lk = k.r.vs->leaseIndexFor(kSrc, 0, k.r.replica->leadershipGeneration(), true, true);
        return std::size_t(1 + lk.ref.table->keyWidth() + 4 + lk.ref.layout->rowLen);
    };
    std::size_t rb = 0;
    {
        Lk probe(spec(10));
        fill(probe, 1);
        rb = rowBytesOf(probe);
        const std::size_t perPage = pc::kScanLeasesMaxPageBytes / rb;
        gCapA = gCapB = perPage * 6 / 10;
        ASSERT_LE(gCapA + gCapB, 1000u) << "the sample must fit maxRows 1024 and the tie headroom";
        ASSERT_GT((gCapA + gCapB) * rb, std::size_t(pc::kScanLeasesMaxPageBytes));
        ASSERT_LE(gCapB * rb, std::size_t(pc::kScanLeasesMaxPageBytes));
    }
    {
        Lk k(spec(2000));
        k.r.worker->setScanHashForTest(&capHash);
        fill(k, gCapA + gCapB);
        const Lk::ScanPage p1 = k.scan({}, 1024, 0, true);
        ASSERT_EQ(p1.code, 0);
        EXPECT_EQ(p1.rows.size(), gCapA);
        EXPECT_FALSE(p1.done);
        EXPECT_EQ(p1.lastHash, 50u);
        for (uint64_t id : p1.ids()) EXPECT_LE(id, gCapA);
        const Lk::ScanPage p2 = k.scan({}, 1024, p1.lastHash, false);
        ASSERT_EQ(p2.code, 0);
        EXPECT_EQ(p2.rows.size(), gCapB);
        EXPECT_TRUE(p2.done);
    }
    {   // one group over the cap
        gCapA = (pc::kScanLeasesMaxPageBytes / rb) + 2;
        gCapB = 0;
        Lk k(spec(2000));
        k.r.worker->setScanHashForTest(&capHash);
        fill(k, gCapA);
        const Lk::ScanPage p = k.scan({}, 1024, 0, true);
        EXPECT_EQ(p.code, kTooLarge);
        EXPECT_TRUE(p.done);
        EXPECT_TRUE(p.rows.empty());
    }
}

// 10. The gates, in order: a dropped topic is UnknownTopic; a follower, a lapsed read lease and an index that is not ready are NotLeader (the
// client code space has no NotReady; a retry reaches the same verdict as the lease reads).  Every refusal is done.
// Mutation: drop the leaseServeable_ gate in executeScanLeases_ -> a follower or unready index answers rows: red.
TEST(ScanLeasesRegression, ScanGatesRegression) {
    {   // demoted (a follower)
        Lk k(spec());
        fill(k, 5);
        k.r.demote();
        const Lk::ScanPage p = k.scan({}, 10, 0, true);
        EXPECT_EQ(p.code, kNotLeader);
        EXPECT_TRUE(p.done);
    }
    {   // dropped
        Lk k(spec());
        fill(k, 5);
        WorkerOp drop;
        drop.type = WorkerOp::Type::TopicDropped; drop.topicId = kSrc;
        ASSERT_TRUE(k.r.worker->tryEnqueueOp(std::move(drop)));
        k.r.passes(3);
        const Lk::ScanPage p = k.scan({}, 10, 0, true);
        EXPECT_EQ(p.code, kUnknownTopic);
        EXPECT_TRUE(p.done);
    }
    {   // read lease ended
        Lk k(spec());
        fill(k, 5);
        k.r.worker->requireViewReadLease();
        Lk::ScanPage p = k.scan({}, 10, 0, true);
        EXPECT_EQ(p.code, kNotLeader) << "required, none held yet";
        EXPECT_TRUE(p.done);
        WorkerOp lease;
        lease.type = WorkerOp::Type::ReadLease;
        lease.nextOffset = k.leaseNow + 100'000'000;
        ASSERT_TRUE(k.r.worker->tryEnqueueOp(std::move(lease)));
        EXPECT_EQ(k.scan({}, 10, 0, true).code, 0) << "under the lease";
        k.leaseNow += 100'000'000;
        EXPECT_EQ(k.scan({}, 10, 0, true).code, kNotLeader) << "at the end of the lease";
    }
    {   // index not ready
        Lk k(spec());
        fill(k, 5);
        k.r.holdJudge(true);
        (void)k.send({ltr::updateRec(99, 0)});
        k.r.passes(30);
        ASSERT_FALSE(k.r.leaseReady());
        const Lk::ScanPage p = k.scan({}, 10, 0, true);
        EXPECT_EQ(p.code, kNotLeader);
        EXPECT_TRUE(p.done);
        k.r.holdJudge(false);
        ready(k);
        EXPECT_EQ(k.scan({}, 10, 0, true).code, 0);
    }
}

// 11. Hard rule 4: a warm scan allocates nothing on the worker thread (selection, heapsort and encode run in the preallocated scratch).
// Mutation: build the page in a std::vector -> allocations in the window: red.
TEST(ScanLeasesRegression, ScanNoAllocRegression) {
    Lk k(spec(1000));
    fill(k, 300);
    const auto post = [&](uint64_t resume, bool first) {
        WorkerOp op;
        op.type = WorkerOp::Type::ScanLeases; op.topicId = kSrc; op.partition = 0;
        op.sessionId = 1; op.correlationId = ++k.corr;
        op.nextOffset = int64_t(resume); op.recordCount = 64; op.isolation = first ? 1 : 0;
        return k.r.worker->tryEnqueueOp(std::move(op));
    };
    uint64_t rows = 0;
    uint64_t workerAllocs = 0;
    const auto run = [&](uint64_t resume, bool first) {
        bool ok = post(resume, first);
        k.r.now += 1'000'000;
        {   // only the worker's own pass is measured: the drain below builds the test's owning WorkerReturn
            rune::testing::AllocWindow window;
            k.r.worker->doWork(k.r.now);
            workerAllocs += window.count();
        }
        k.r.worker->drainReturn([&](WorkerReturn r) {
            if (r.type == WorkerReturn::Type::ScanLeasesResult) rows += uint64_t(r.recordCount);
        }, [](const TxnReturn&) {});
        return ok;
    };
    for (int i = 0; i < 8; ++i) ASSERT_TRUE(run(0, true));   // warm up (the scratch is allocated on the first scan)
    ASSERT_GT(rows, 0u);
    rows = 0;
    uint64_t failures = 0;
    workerAllocs = 0;
    for (int i = 0; i < 500; ++i) failures += run(0, true) ? 0 : 1;
    EXPECT_EQ(workerAllocs, 0u);
    EXPECT_EQ(failures, 0u);
    EXPECT_EQ(rows, 500u * 64u);
}

// 12. The order hash is pinned (wyhash, seed = the source topic id): a change of the function would reorder every scan across a rolling upgrade.
// Mutation: change the seed or the function in scanHash (LeaseHash.h) -> the constants differ: red.
TEST(ScanLeasesRegression, ScanHashGoldenRegression) {
    const std::vector<uint8_t> a = {1, 0, 0, 0, 0, 0, 0, 0};
    const std::vector<uint8_t> b = {'a', 'b', 'c'};
    const std::vector<uint8_t> c(16, 0xAB);
    EXPECT_EQ(lease::scanHash(a, 6), 17450080259330277671ULL) << "golden 1";
    EXPECT_EQ(lease::scanHash(b, 7), 10202830245010477438ULL) << "golden 2";
    EXPECT_EQ(lease::scanHash(c, 1000), 16726008355911138569ULL) << "golden 3";
    EXPECT_NE(lease::scanHash(a, 6), lease::scanHash(a, 7)) << "the topic id is the seed";
}
