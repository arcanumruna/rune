// Pure GSO grouping / GRO split math (no sockets): spec 2026-10-03-aeron-udp-gso-gro-design.md §4.1.
#include "rune/protocol/net/rune_udp_gso.h"

#include <gtest/gtest.h>

#include <vector>

namespace {
std::vector<iovec> lens(std::initializer_list<std::size_t> ls) {
    static char buf[1 << 17];
    std::vector<iovec> v;
    for (auto l : ls) v.push_back(iovec{buf, l});
    return v;
}
std::size_t group(const std::vector<iovec>& v, std::size_t segs = RUNE_GSO_MAX_SEGMENTS,
                  std::size_t bytes = RUNE_GSO_MAX_BYTES) {
    return rune_gso_group(v.data(), v.size(), segs, bytes);
}
} // namespace

TEST(UdpGsoPlan, EmptyRunIsZero) { EXPECT_EQ(rune_gso_group(nullptr, 0, 64, 65507), 0u); }
TEST(UdpGsoPlan, SingleDatagramIsAGroupOfOne) { EXPECT_EQ(group(lens({1408})), 1u); }
TEST(UdpGsoPlan, EqualRunGroupsWhole) { EXPECT_EQ(group(lens({1408, 1408, 1408, 1408})), 4u); }
TEST(UdpGsoPlan, ShorterLastJoinsTheGroup) { EXPECT_EQ(group(lens({1408, 1408, 700})), 3u); }
TEST(UdpGsoPlan, ShorterSegmentEndsTheGroup) { EXPECT_EQ(group(lens({1408, 700, 1408})), 2u); }
TEST(UdpGsoPlan, LongerSegmentBreaksTheRun) { EXPECT_EQ(group(lens({700, 1408, 1408})), 1u); }
TEST(UdpGsoPlan, ZeroLengthNeverGroups) {
    EXPECT_EQ(group(lens({0, 0})), 1u);
    EXPECT_EQ(group(lens({1408, 0})), 1u);
}
TEST(UdpGsoPlan, SegmentCapHolds) {
    std::vector<iovec> v = lens({});
    for (int i = 0; i < 100; ++i) v.push_back(lens({1000})[0]);
    EXPECT_EQ(group(v, 64, 1 << 20), 64u);
}
TEST(UdpGsoPlan, ByteCapHolds) {
    // 46 x 1408 = 64,768 <= 65,507; a 47th would be 66,176 > cap.
    std::vector<iovec> v;
    for (int i = 0; i < 60; ++i) v.push_back(lens({1408})[0]);
    EXPECT_EQ(group(v), 46u);
}
TEST(UdpGsoPlan, OversizeFirstIsAGroupOfOne) { EXPECT_EQ(group(lens({65504, 65504})), 1u); }
TEST(UdpGsoPlan, GroCountAndLengths) {
    EXPECT_EQ(rune_gro_count(0, 0), 1u);         // no GRO cmsg: deliver as-is
    EXPECT_EQ(rune_gro_count(1408, 0), 1u);
    EXPECT_EQ(rune_gro_count(1408, 1408), 1u);   // exactly one segment
    EXPECT_EQ(rune_gro_count(1000, 1408), 1u);   // seg > len
    EXPECT_EQ(rune_gro_count(4224, 1408), 3u);   // exact multiple
    EXPECT_EQ(rune_gro_count(4225, 1408), 4u);   // 1-byte last
    EXPECT_EQ(rune_gro_segment_len(4225, 1408, 0), 1408u);
    EXPECT_EQ(rune_gro_segment_len(4225, 1408, 3), 1u);
    EXPECT_EQ(rune_gro_segment_len(3500, 1408, 2), 684u);
}

// Fix round 1 (review M2): raise Aeron's msgs-per-send only when a GSO group of >= 2 MTU datagrams can exist.
TEST(UdpGsoPlan, MsgsPerSendRaisedOnlyWhenGsoCanGroup) {
    EXPECT_EQ(rune_gso_msgs_per_send(1, 1408), 16u);
    EXPECT_EQ(rune_gso_msgs_per_send(1, 8192), 16u);
    EXPECT_EQ(rune_gso_msgs_per_send(1, 32736), 16u);   // 2 x 32736 = 65472 <= 65507
    EXPECT_EQ(rune_gso_msgs_per_send(1, 32768), 0u);    // 2 x 32768 > 65507: GSO can never group
    EXPECT_EQ(rune_gso_msgs_per_send(1, 65504), 0u);    // loopback-derived MTU
    EXPECT_EQ(rune_gso_msgs_per_send(0, 1408), 0u);     // no UDP_SEGMENT (GRO-only bindings, or none)
}
