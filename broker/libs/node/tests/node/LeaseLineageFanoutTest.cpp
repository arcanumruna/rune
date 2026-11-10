// Lease topics Task 27 (spec A.6): LeaseLineageFanout - when the control thread posts a led transient lease partition's
// lineage to its worker.  Pure unit tests with lambdas.
#include <gtest/gtest.h>

#include <rune/metadata/PartitionAssignment.h>
#include <rune/node/LeaseLineageFanout.h>

#include <vector>

using rune::metadata::TransientLineage;
using rune::node::LeaseLineageFanout;

namespace {

struct Led { int32_t t, p, epoch; };
struct Posted { int32_t w, t, p, epoch; uint64_t lineage; uint32_t generation; };

struct Rig {
    LeaseLineageFanout fan;
    std::vector<Led>   led{{1, 0, 3}};
    TransientLineage   rec{};
    bool               trustedNow = true;
    bool               pending    = false;
    bool               ringOk     = true;
    int                scans      = 0;
    std::vector<Posted> sent;

    Rig() { rec.lineage = 0xAA; rec.generation = 2; }

    int step(bool changed) {
        return fan.step(
            changed,
            [&](auto&& fn) { ++scans; for (const auto& l : led) fn(l.t, l.p, l.epoch); },
            [&](int32_t, int32_t) -> const TransientLineage* { return trustedNow ? &rec : nullptr; },
            [&](int32_t t, int32_t) { return t + 10; },
            [&](int32_t) { return pending; },
            [&](int32_t w, int32_t t, int32_t p, int32_t e, uint64_t l, uint32_t g) {
                if (!ringOk) return false;
                sent.push_back({w, t, p, e, l, g});
                return true;
            });
    }
};

}  // namespace

TEST(LeaseLineageFanoutRegression, PostsOncePerLeadershipRegression) {
    Rig r;
    EXPECT_EQ(r.step(true), 1);
    ASSERT_EQ(r.sent.size(), 1u);
    EXPECT_EQ(r.sent[0].w, 11);
    EXPECT_EQ(r.sent[0].epoch, 3);
    EXPECT_EQ(r.sent[0].lineage, 0xAAu);
    EXPECT_EQ(r.sent[0].generation, 2u);
    EXPECT_EQ(r.step(true), 0) << "the same (epoch, lineage) is not posted again";
    r.led[0].epoch = 4;   // a new leadership
    EXPECT_EQ(r.step(true), 1);
    r.rec.lineage = 0xBB; // a new lineage at the same epoch
    EXPECT_EQ(r.step(true), 1);
    EXPECT_EQ(r.sent.size(), 3u);
    EXPECT_EQ(r.sent[2].lineage, 0xBBu);
    EXPECT_EQ(r.fan.postedForTest(), 3u);
}

TEST(LeaseLineageFanoutRegression, HeldBehindPendingRoleOpsRegression) {
    Rig r;
    r.pending = true;
    EXPECT_EQ(r.step(true), 0);
    EXPECT_TRUE(r.sent.empty());
    EXPECT_TRUE(r.fan.retryingForTest());
    EXPECT_EQ(r.fan.heldForTest(), 1u);
    r.pending = false;
    EXPECT_EQ(r.step(false), 1) << "the retry needs no change";
    EXPECT_EQ(r.sent.size(), 1u);
    EXPECT_FALSE(r.fan.retryingForTest());
}

TEST(LeaseLineageFanoutRegression, RetriesAFullRingRegression) {
    Rig r;
    r.ringOk = false;
    EXPECT_EQ(r.step(true), 0);
    EXPECT_TRUE(r.fan.retryingForTest());
    EXPECT_EQ(r.step(false), 0);
    EXPECT_TRUE(r.fan.retryingForTest());
    r.ringOk = true;
    EXPECT_EQ(r.step(false), 1);
    EXPECT_EQ(r.sent.size(), 1u);
}

TEST(LeaseLineageFanoutRegression, UntrustedLineageRetriedRegression) {
    Rig r;
    r.trustedNow = false;
    EXPECT_EQ(r.step(true), 0);
    EXPECT_EQ(r.step(false), 0);
    EXPECT_TRUE(r.fan.retryingForTest());
    r.trustedNow = true;
    EXPECT_EQ(r.step(false), 1);
    EXPECT_FALSE(r.fan.retryingForTest());
}

TEST(LeaseLineageFanoutRegression, NoScanWhenUnchangedRegression) {
    Rig r;
    EXPECT_EQ(r.step(true), 1);
    const int scans = r.scans;
    EXPECT_EQ(r.step(false), 0);
    EXPECT_EQ(r.scans, scans) << "nothing changed and nothing to retry: the map is not walked";
}

TEST(LeaseLineageFanoutRegression, SweepsUnledPartitionsRegression) {
    Rig r;
    r.led = {{1, 0, 3}, {1, 1, 3}};
    EXPECT_EQ(r.step(true), 2);
    EXPECT_EQ(r.fan.trackedForTest(), 2u);
    r.led = {{1, 0, 3}};   // p1 is no longer led here
    EXPECT_EQ(r.step(true), 0) << "p0 is not re-posted";
    EXPECT_EQ(r.fan.trackedForTest(), 1u);
    r.led = {{1, 0, 3}, {1, 1, 3}};   // p1 led again; its entry was swept, so it is posted again
    EXPECT_EQ(r.step(true), 1);
    EXPECT_EQ(r.sent.back().p, 1);
    EXPECT_EQ(r.fan.trackedForTest(), 2u);
    r.led = {{1, 0, 3}, {1, 1, 4}};   // p1 at a NEW epoch: posted once
    EXPECT_EQ(r.step(true), 1);
    EXPECT_EQ(r.sent.back().epoch, 4);
    EXPECT_EQ(r.step(true), 0);
    EXPECT_EQ(r.fan.trackedForTest(), 2u);
}

TEST(LeaseLineageFanoutRegression, UntrustedLineageKeepsPartitionTrackedRegression) {
    Rig r;
    EXPECT_EQ(r.step(true), 1);
    EXPECT_EQ(r.fan.trackedForTest(), 1u);
    r.trustedNow = false;   // p0 is still led but its lineage is untrusted this pass
    EXPECT_EQ(r.step(true), 0);
    EXPECT_EQ(r.fan.trackedForTest(), 1u) << "a still-led partition is never swept";
    r.trustedNow = true;
    EXPECT_EQ(r.step(true), 0) << "same (epoch, lineage): not re-posted after the untrusted pass";
}
