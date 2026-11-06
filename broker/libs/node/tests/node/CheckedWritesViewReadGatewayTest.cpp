// Part 07 Task 14: GetViewRowK and ScanViewRows through a real ClientGateway + WorkerPool.
#include <gtest/gtest.h>

#include <algorithm>

#include <rune/gateway/ObjectIndex.h>
#include <rune/gateway/PermissionTable.h>
#include <rune/gateway/SecurityView.h>
#include <rune/protocol/client/DataMessages.h>
#include <rune/view/ViewRow.h>

#include "support/CheckedWritesViewReadRig.h"

namespace {

using namespace cwkeyed;
using rune::node::test::CwViewReadGatewayRig;
namespace pc = rune::protocol::client;

constexpr int32_t kInvalidRequest = pc::ViewRowResponse::kInvalidRequest;

std::vector<Rec> userRows(uint64_t user, uint64_t firstChar, int n) {
    std::vector<Rec> rs;
    for (int i = 0; i < n; ++i) rs.push_back(Rec{firstChar + static_cast<uint64_t>(i), user, "n", static_cast<uint16_t>(100 + i)});
    return rs;
}

}  // namespace

TEST(CheckedWritesViewReadGateway, RoutesToTheSourceOwner) {
    CwViewReadGatewayRig rig(/*sinkPartitions=*/1, /*workers=*/4);
    rig.append(userRows(42, 1, 3));
    const int32_t owner = rig.pool.ownerWorkerId(CwViewReadGatewayRig::kSrcTopic, 0);

    rig.sendK(rig.getKFrame(1, beKey({42, 2})));
    ASSERT_TRUE(rig.awaitFrame());
    EXPECT_EQ(rig.lastStatus(), pc::ViewRowResponse::kOk);
    uint64_t total = 0;
    for (int32_t w = 0; w < rig.pool.workerCount(); ++w) total += rig.workerGetKs(w);
    EXPECT_EQ(total, 1u);
    EXPECT_EQ(rig.workerGetKs(owner), 1u) << "the source owner's worker served it";
}

TEST(CheckedWritesViewReadGatewayRegression, MultiPartitionSinkNeedsThePartitionRegression) {
    CwViewReadGatewayRig wide(/*sinkPartitions=*/4, /*workers=*/4);
    const auto key = beKey({42, 2});

    wide.sendK(wide.getKFrame(1, key, /*partition=*/2));
    ASSERT_TRUE(wide.awaitFrame());   // the worker of (source, 2) holds no replica of it: a bare NotLeader
    const int32_t owner2 = wide.pool.ownerWorkerId(CwViewReadGatewayRig::kSrcTopic, 2);
    uint64_t total = 0;
    for (int32_t w = 0; w < wide.pool.workerCount(); ++w) total += wide.workerGetKs(w);
    EXPECT_EQ(total, 1u);
    EXPECT_EQ(wide.workerGetKs(owner2), 1u) << "routed to (source, 2)";

    for (const int32_t bad : {-1, 4}) {
        const uint64_t before = wide.frames;
        wide.sendK(wide.getKFrame(2, key, bad));
        EXPECT_EQ(wide.frames, before + 1) << "partition " << bad << " is answered at once";
        EXPECT_EQ(wide.lastStatus(), kInvalidRequest) << "partition " << bad;
    }
    uint64_t after = 0;
    for (int32_t w = 0; w < wide.pool.workerCount(); ++w) after += wide.workerGetKs(w);
    EXPECT_EQ(after, 1u) << "a refused read never reaches a worker";

    CwViewReadGatewayRig narrow(/*sinkPartitions=*/1, /*workers=*/1);
    narrow.sendK(narrow.getKFrame(3, key, /*partition=*/1));
    EXPECT_EQ(narrow.lastStatus(), kInvalidRequest) << "a one-partition sink has no partition 1";
    narrow.append(userRows(42, 1, 3));
    narrow.sendK(narrow.getKFrame(4, key, /*partition=*/0));
    ASSERT_TRUE(narrow.awaitFrame());
    EXPECT_EQ(narrow.lastStatus(), pc::ViewRowResponse::kOk) << "partition 0 names the only partition";
}

TEST(CheckedWritesViewReadGateway, OldSessionIsInvalidRequest) {
    CwViewReadGatewayRig rig;
    rig.append(userRows(42, 1, 3));
    const int64_t old = rig.gw.injectTestSession(0, pc::kClientVersionCheckedWrites - 1);
    rig.sendK(rig.getKFrame(1, beKey({42, 2}), -1, -1, old));
    EXPECT_EQ(rig.lastStatus(), kInvalidRequest);
    rig.sendScan(rig.scanFrame(2, beKey({42}), {}, 0, old));
    EXPECT_EQ(rig.lastStatus(), kInvalidRequest);
    EXPECT_EQ(rig.workerGetKs(0), 0u);
    EXPECT_EQ(rig.pool.worker(0).scanViewRowsForTest(), 0u);
}

TEST(CheckedWritesViewReadGateway, NotAuthorizedLikeGetViewRow) {
    CwViewReadGatewayRig rig;
    rig.append(userRows(42, 1, 3));
    rune::gateway::SecurityView view{rune::security::SecurityState{}};
    rig.gw.setAuthMode(rune::gateway::GatewayAuthMode::Required, &view);
    rune::gateway::ObjectIndex idx;
    idx.onTopic(CwViewReadGatewayRig::kSrcTopic, "character");
    idx.onView(CwViewReadGatewayRig::kViewTopic, "account_characters");
    rig.gw.setSessionAuthForTest(rig.sid, rune::gateway::AuthAuthenticated{1, "nobody", rune::gateway::PermissionTable::compile({}, idx)});

    rig.sendK(rig.getKFrame(1, beKey({42, 2})));
    EXPECT_EQ(rig.lastStatus(), static_cast<int32_t>(pc::ViewRowResponse::kNotAuthorized));
    rig.sendScan(rig.scanFrame(2, beKey({42})));
    EXPECT_EQ(rig.lastStatus(), static_cast<int32_t>(pc::ViewRowResponse::kNotAuthorized));
}

TEST(CheckedWritesViewReadGateway, ScanResultEncodesTheFrame) {
    CwViewReadGatewayRig rig;
    rig.append(userRows(42, 1, 12));
    std::vector<uint8_t> token;
    std::vector<uint64_t> seen;
    for (int page = 0; page < 5; ++page) {
        rig.sendScan(rig.scanFrame(10 + page, beKey({42}), token, 5));
        ASSERT_TRUE(rig.awaitFrame());
        const auto resp = pc::ScanViewRowsResponse::decode(rig.lastFrame.data(), rig.lastFrame.size());
        ASSERT_TRUE(resp.has_value());
        EXPECT_EQ(resp->status, pc::ViewRowResponse::kOk);
        EXPECT_EQ(resp->correlationId, 10 + page);
        EXPECT_TRUE(resp->forEachRow([&](std::span<const uint8_t> row) { seen.push_back(rune::view::readSlot(row.data(), rig.layout, 1).u); }));
        token.assign(resp->nextToken.begin(), resp->nextToken.end());
        if (token.empty()) break;
        EXPECT_EQ(token.size(), 12u);
    }
    ASSERT_EQ(seen.size(), 12u);
    std::sort(seen.begin(), seen.end());   // pages come in table order
    for (std::size_t i = 0; i < seen.size(); ++i) EXPECT_EQ(seen[i], i + 1);
}
