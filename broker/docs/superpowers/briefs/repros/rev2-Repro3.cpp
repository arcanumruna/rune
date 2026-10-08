#include "DrHarness.h"
using namespace rune::replication;
using namespace rune::replication::dr;
namespace { constexpr int kCap = 3000; }
// maxFetchBytes = 1 → every answer = exactly one (oversize-exempt) batch, i.e.
// the regime of any batch >= maxFetchBytes (1 MiB default; produce allows 8 MiB).
TEST(Review3, OversizeBoundaryBatch_ReconcileLivelock) {
    DrCluster c(3);
    c.start();
    ASSERT_EQ(c.produceAcked(10), 10);
    c.isolate(3);
    ASSERT_EQ(c.produceAcked(20), 20);
    c.run(20);
    c.moveLeader(3);
    c.healAll();
    const bool w = c.waitWritable(kCap);
    std::cerr << "writable=" << w << " leo3=" << c.rep(3).logEndOffset() << "\n";
    EXPECT_TRUE(w) << "reconcile from LEO-1 makes no progress when the boundary batch fills the budget";
}
