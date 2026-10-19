// AdminIsrChannelTest.cpp — catalog history Task 6: the admin↔control frames of
// the __catalog replica's controller-ISR path.
//   - AdminIsrChannel::sendAlterIsr encodes exactly ONE kA2cCatalogAlterIsr
//     frame that decodes to the message; a full ring refuses (false, counted) —
//     the replica re-sends on its cadence, never assumes delivery.
//   - The catalog role (kMsgCatalogRole) carries leader / epoch / leaderId /
//     ISR / partitionEpoch; a malformed one is refused, never applied.
//   - The controller's AlterIsrResponse relay is verbatim — leaderId included
//     (the E2 shard-mode lesson: a relay that dropped it left -1, and the replica
//     never matched its own acks).
#include <rune/node/AdminChannels.h>
#include <rune/node/AdminIsrChannel.h>
#include <rune/node/CatalogPartition.h>
#include <rune/node/NodeConfig.h>

#include <gtest/gtest.h>

#include <cstdint>
#include <cstring>
#include <vector>

using namespace rune::node;
namespace rr = rune::replication;

namespace {

struct Frame {
    int32_t type = 0;
    std::vector<uint8_t> bytes;
};

std::vector<Frame> drainAll(OneToOneByteRing& ring) {
    std::vector<Frame> out;
    while (ring.drain([&](int32_t tid, aeron::concurrent::AtomicBuffer& b, aeron::util::index_t off,
                          aeron::util::index_t len) {
        const auto* p = reinterpret_cast<const uint8_t*>(b.buffer() + off);
        out.push_back(Frame{tid, std::vector<uint8_t>(p, p + len)});
    }, 64) > 0) {}
    return out;
}

rr::AlterIsrMsg sampleAlterIsr() {
    rr::AlterIsrMsg m{};
    m.topicId        = kCatalogTopicId;
    m.partition      = kCatalogPartition;
    m.leaderId       = 2;
    m.leaderEpoch    = 7;
    m.isrCount       = 3;
    m.isr[0] = 2; m.isr[1] = 1; m.isr[2] = 3;
    m.partitionEpoch = 11;
    return m;
}

} // namespace

TEST(AdminIsrChannel, SendAlterIsrEncodesExactlyOneFrameThatDecodesToTheMessage) {
    OneToOneByteRing ring(1 << 12);
    AdminIsrChannel ch(ring);
    const rr::AlterIsrMsg m = sampleAlterIsr();
    ASSERT_TRUE(ch.sendAlterIsr(m));
    const auto frames = drainAll(ring);
    ASSERT_EQ(frames.size(), 1u);
    EXPECT_EQ(frames[0].type, kA2cCatalogAlterIsr);
    rr::AlterIsrMsg d{};
    ASSERT_TRUE(decodeCatalogAlterIsr(frames[0].bytes.data(), frames[0].bytes.size(), d));
    EXPECT_EQ(d.topicId, m.topicId);
    EXPECT_EQ(d.partition, m.partition);
    EXPECT_EQ(d.leaderId, m.leaderId);
    EXPECT_EQ(d.leaderEpoch, m.leaderEpoch);
    EXPECT_EQ(d.isrCount, m.isrCount);
    EXPECT_EQ(d.isr, m.isr);
    EXPECT_EQ(d.partitionEpoch, m.partitionEpoch);
    EXPECT_EQ(ch.sent(), 1u);
    EXPECT_EQ(ch.refused(), 0u);
}

TEST(AdminIsrChannel, FullRingRefusesAndCountsNeverAssumedDeliveredRegression) {
    OneToOneByteRing ring(4096);   // max message 512 B: a handful of AlterIsr frames (v4: 240 B each) at most
    AdminIsrChannel ch(ring);
    const rr::AlterIsrMsg m = sampleAlterIsr();
    int accepted = 0;
    for (int i = 0; i < 64 && ch.sendAlterIsr(m); ++i) ++accepted;
    ASSERT_GT(accepted, 0);
    ASSERT_LT(accepted, 64) << "the ring never filled";
    EXPECT_FALSE(ch.sendAlterIsr(m)) << "a full ring must refuse (the replica re-sends)";
    EXPECT_EQ(ch.refused(), 2u);
    EXPECT_EQ(drainAll(ring).size(), static_cast<std::size_t>(accepted)) << "no partial / phantom frame";
    EXPECT_TRUE(ch.sendAlterIsr(m)) << "space again → accepted";

    // A ring whose max message cannot hold one AlterIsr refuses — never a throw
    // out of the replica's doWork.
    OneToOneByteRing tiny(256);   // max message 32 B
    AdminIsrChannel tch(tiny);
    EXPECT_NO_THROW(EXPECT_FALSE(tch.sendAlterIsr(m)));
    EXPECT_EQ(tch.refused(), 1u);
}

TEST(AdminIsrChannel, DataSendsAreRefusedOnTheIsrChannel) {
    OneToOneByteRing ring(1 << 12);
    AdminIsrChannel ch(ring);
    EXPECT_FALSE(ch.sendFetchRequest(rr::FetchRequestMsg{}));
    EXPECT_FALSE(ch.sendFetchResponse(1, rr::FetchResponseMsg{}));
    EXPECT_TRUE(drainAll(ring).empty());
}

TEST(AdminIsrChannel, CatalogRoleRoundTripsLeaderEpochIsrAndPartitionEpoch) {
    OneToOneByteRing ring(1 << 12);
    AdminRoleMsg m;
    m.leader = true; m.epoch = 9; m.leaderId = 3; m.partitionEpoch = 4;
    m.isrCount = 2; m.isr[0] = 3; m.isr[1] = 1;
    ASSERT_TRUE(encodeAdminRole(ring, m, kMsgCatalogRole));
    const auto frames = drainAll(ring);
    ASSERT_EQ(frames.size(), 1u);
    EXPECT_EQ(frames[0].type, kMsgCatalogRole);
    AdminRoleMsg d;
    ASSERT_TRUE(decodeAdminRole(frames[0].bytes.data(), frames[0].bytes.size(), d));
    EXPECT_TRUE(d.leader);
    EXPECT_EQ(d.epoch, 9);
    EXPECT_EQ(d.leaderId, 3);
    EXPECT_EQ(d.partitionEpoch, 4);
    ASSERT_EQ(d.isrCount, 2);
    EXPECT_EQ(d.isr[0], 3);
    EXPECT_EQ(d.isr[1], 1);
}

TEST(AdminIsrChannel, MalformedCatalogRoleIsRefusedNeverAppliedRegression) {
    OneToOneByteRing ring(1 << 12);
    AdminRoleMsg tooBig;
    tooBig.isrCount = AdminRoleMsg::kMaxIsr + 1;
    EXPECT_FALSE(encodeAdminRole(ring, tooBig, kMsgCatalogRole)) << "an ISR beyond the bound is never encoded";
    // A short frame and a frame whose ISR count exceeds its bytes both refuse.
    AdminRoleMsg ok;
    ok.leader = true; ok.isrCount = 3; ok.isr[0] = 1; ok.isr[1] = 2; ok.isr[2] = 3;
    ASSERT_TRUE(encodeAdminRole(ring, ok, kMsgCatalogRole));
    const auto frames = drainAll(ring);
    ASSERT_EQ(frames.size(), 1u);
    AdminRoleMsg d;
    EXPECT_FALSE(decodeAdminRole(frames[0].bytes.data(), kAdminRoleFixedBytes - 1, d));
    EXPECT_FALSE(decodeAdminRole(frames[0].bytes.data(), frames[0].bytes.size() - 4, d))
        << "an ISR count beyond the frame's bytes";
    EXPECT_EQ(d.isrCount, 0) << "a refused decode leaves no partial role";
    std::vector<uint8_t> neg = frames[0].bytes;
    const int32_t bad = -1;
    std::memcpy(neg.data() + 13, &bad, 4);
    EXPECT_FALSE(decodeAdminRole(neg.data(), neg.size(), d)) << "a negative ISR count";
}

TEST(AdminIsrChannel, SchemaRoleStillDecodesWithAnEmptyIsr) {
    OneToOneByteRing ring(1 << 12);
    AdminRoleMsg m;
    m.leader = false; m.epoch = 5; m.leaderId = 2;
    ASSERT_TRUE(encodeAdminRole(ring, m, kMsgCatalogRole));
    const auto frames = drainAll(ring);
    ASSERT_EQ(frames.size(), 1u);
    EXPECT_EQ(frames[0].type, kMsgCatalogRole);
    const AdminRoleMsg d = decodeAdminRole(frames[0].bytes.data(), frames[0].bytes.size());
    EXPECT_FALSE(d.leader);
    EXPECT_EQ(d.epoch, 5);
    EXPECT_EQ(d.leaderId, 2);
    EXPECT_EQ(d.isrCount, 0);
    EXPECT_EQ(d.partitionEpoch, -1);
}

// E2 shard-mode lesson: the relay must keep leaderId (and every other field).
TEST(CatalogAlterIsrRelayRegression, ControllerAnswerIsRelayedVerbatimWithItsLeaderIdRegression) {
    OneToOneByteRing ring(1 << 12);
    rr::AlterIsrResponseMsg r{};
    r.topicId = kCatalogTopicId; r.partition = kCatalogPartition;
    r.leaderId = 2; r.leaderEpoch = 13; r.partitionEpoch = 6; r.errorCode = rr::kAlterIsrIneligibleReplica;
    ASSERT_TRUE(encodeCatalogAlterIsrResponse(ring, r));
    const auto frames = drainAll(ring);
    ASSERT_EQ(frames.size(), 1u);
    EXPECT_EQ(frames[0].type, kMsgCatalogAlterIsrResponse);
    rr::AlterIsrResponseMsg d{};
    ASSERT_TRUE(decodeCatalogAlterIsrResponse(frames[0].bytes.data(), frames[0].bytes.size(), d));
    EXPECT_EQ(d.topicId, r.topicId);
    EXPECT_EQ(d.partition, r.partition);
    EXPECT_EQ(d.leaderId, 2) << "leaderId must survive the relay (E2: it stayed -1 for worker-owned replicas)";
    EXPECT_EQ(d.leaderEpoch, r.leaderEpoch);
    EXPECT_EQ(d.partitionEpoch, r.partitionEpoch);
    EXPECT_EQ(d.errorCode, r.errorCode);
}

TEST(CatalogPartition, ConfigIsMajorityAcksAllNeverCompactedNeverDeleted) {
    NodeConfig nc;
    nc.nodeId = 4;
    const auto c = makeCatalogPartitionConfig(nc, 5);
    EXPECT_EQ(c.topicId, kCatalogTopicId);
    EXPECT_EQ(c.partitionId, 0u);
    EXPECT_EQ(c.selfId, 4);
    EXPECT_EQ(c.minIsr, 3);
    EXPECT_EQ(c.durability, rr::DurabilityMode::Majority);
    EXPECT_EQ(c.cleanupPolicy, rr::CleanupPolicy::Delete);
    EXPECT_EQ(c.retentionMs, -1);
    EXPECT_EQ(c.retentionBytes, -1);
    EXPECT_EQ(c.logStartOffset, 0);
    EXPECT_EQ(catalogMinIsr(1), 1);
    EXPECT_EQ(catalogMinIsr(2), 2);
    EXPECT_EQ(catalogMinIsr(3), 2);
    EXPECT_EQ(catalogMinIsr(4), 3);
}
