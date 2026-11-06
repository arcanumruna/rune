// Part 07 Task 14 (hard rule): the held keyed view reads and the gateway's keyed read path allocate nothing.  Part of
// the alloc-regression binary (AllocWindow replaces the global operator new, binary-wide).
#include <gtest/gtest.h>

#include <rune/protocol/client/DataMessages.h>

#include "support/AllocWindow.h"
#include "support/CheckedWritesViewReadRig.h"

namespace {

using namespace cwkeyed;
using rune::node::WorkerOp;
using rune::node::WorkerReturn;
namespace pc = rune::protocol::client;

std::vector<Rec> userRows(uint64_t user, uint64_t firstChar, int n) {
    std::vector<Rec> rs;
    for (int i = 0; i < n; ++i) rs.push_back(Rec{firstChar + static_cast<uint64_t>(i), user, "n", static_cast<uint16_t>(100 + i)});
    return rs;
}

}  // namespace

TEST(CheckedWritesViewReadRegression, HeldScanAllocatesNothingRegression) {
    rune::node::test::CwViewReadWorkerRig r;
    r.install(accountCharacters(r.src));
    r.append(userRows(42, 1, 12));
    (void)r.scan(beKey({42}));   // warm up

    constexpr std::size_t kHeld = 256;
    const auto prefix = beKey({42});
    int64_t now = 5'000'000'000;
    bool posted = true;
    {
        rune::testing::AllocWindow window;
        for (std::size_t i = 0; i < kHeld; i += 32) {
            for (std::size_t j = 0; j < 32; ++j)
                posted &= rune::node::test::postViewRead(r.w, WorkerOp::Type::ScanViewRows, prefix, {}, kViewTopic, 0, /*minOffset*/ 1000);
            r.w.doWork(now += 1'000'000);
        }
        r.w.doWork(now += 1'000'000);   // every held read is re-served once more
        EXPECT_EQ(window.count(), 0u) << "256 held scans";
    }
    ASSERT_TRUE(posted);
    EXPECT_EQ(r.w.heldViewReadsForTest(), kHeld);

    ASSERT_TRUE(rune::node::test::postViewRead(r.w, WorkerOp::Type::ScanViewRows, prefix, {}, kViewTopic, 0, 1000));
    bool refused = false;
    r.w.doWork(now += 1'000'000);
    r.w.drainReturn([&](WorkerReturn ret) {
        if (ret.type == WorkerReturn::Type::ViewScanResult) refused = ret.errorCode == pc::ViewRowResponse::kNotReady;
    });
    EXPECT_TRUE(refused) << "the 257th is answered NotReady at once";
    EXPECT_EQ(r.w.heldViewReadsForTest(), kHeld);
}

TEST(CheckedWritesViewReadGateway, GatewayAllocatesNothingPerRead) {
    rune::node::test::CwViewReadGatewayRig rig;
    rig.append(userRows(42, 1, 12));
    const auto getK  = rig.getKFrame(1, beKey({42, 2}));
    const auto scan  = rig.scanFrame(2, beKey({42}), {}, 5);
    for (int i = 0; i < 4; ++i) {   // warm up (frame buffers, ring state)
        rig.sendK(getK);
        ASSERT_TRUE(rig.awaitFrame());
        rig.sendScan(scan);
        ASSERT_TRUE(rig.awaitFrame());
    }
    EXPECT_EQ(rig.lastStatus(), pc::ViewRowResponse::kOk);

    uint64_t answered = 0;
    {
        rune::testing::AllocWindow window;
        for (int i = 0; i < 1000; ++i) {
            rig.sendK(getK);
            answered += rig.awaitFrame() ? 1 : 0;
        }
        EXPECT_EQ(window.count(), 0u) << "1000 GetViewRowK";
    }
    EXPECT_EQ(answered, 1000u);
    {
        rune::testing::AllocWindow window;
        for (int i = 0; i < 1000; ++i) {
            rig.sendScan(scan);
            answered += rig.awaitFrame() ? 1 : 0;
        }
        EXPECT_EQ(window.count(), 0u) << "1000 ScanViewRows";
    }
    EXPECT_EQ(answered, 2000u);
}
