// Issue #122: the bound producer-id format (rune/protocol/ProducerIdBinding.h).
#include <rune/protocol/ProducerIdBinding.h>

#include <gtest/gtest.h>

#include <set>

namespace rp = rune::protocol;

TEST(ProducerIdBindingRegression, BoundIdsCarryTheirOwnerAndKeepTheCounterOrderRegression) {
    for (const uint64_t owner : {uint64_t{0}, uint64_t{1}, uint64_t{7}, (uint64_t{1} << 23) - 1}) {
        const int64_t pid = rp::bindProducerId(12345, owner);
        EXPECT_GT(pid, 0);
        EXPECT_TRUE(rp::isBoundProducerId(pid));
        EXPECT_EQ(rp::producerIdCounter(pid), 12345);
        EXPECT_TRUE(rp::producerIdOwnedBy(pid, owner));
        EXPECT_TRUE(rp::producerIdUsableBy(pid, owner));
        EXPECT_FALSE(rp::producerIdUsableBy(pid, owner + 1)) << "owner " << owner;
        EXPECT_EQ(static_cast<uint64_t>(pid & rp::kPidOwnerMask), owner) << "small ids are the tag itself";
    }
    // Allocation order is numeric order whatever the owners: a recovered maximum reseeds past every counter.
    EXPECT_LT(rp::bindProducerId(10, (uint64_t{1} << 23) - 1), rp::bindProducerId(11, 0));
    EXPECT_GT(rp::bindProducerId(0, 0), int64_t{1} << 61) << "every bound id is above every legacy id in use";
    EXPECT_GT(rp::bindProducerId(rp::kMaxPidCounter, uint64_t{1} << 40), 0);
}

TEST(ProducerIdBindingRegression, LegacyAndAbsentIdsAreNotRefusedRegression) {
    EXPECT_FALSE(rp::isBoundProducerId(42));
    EXPECT_TRUE(rp::producerIdUsableBy(42, 7)) << "version skew: an id issued before binding stays usable";
    EXPECT_FALSE(rp::producerIdOwnedBy(42, 7)) << "... but it is owned by nobody (a txn id holding it is rebound)";
    EXPECT_EQ(rp::producerIdCounter(42), 42);
    EXPECT_TRUE(rp::producerIdUsableBy(-1, 7)) << "no producer id";
    EXPECT_FALSE(rp::isBoundProducerId(-1));
}

TEST(ProducerIdBindingRegression, LargePrincipalIdsFoldIntoTheUpperTagHalfRegression) {
    std::set<uint32_t> tags;
    for (uint64_t p = uint64_t{1} << 23; p < (uint64_t{1} << 23) + 1000; ++p) {
        const uint32_t t = rp::pidOwnerTag(p);
        EXPECT_GE(t, uint32_t{1} << 23) << "a folded tag never equals an exact (small) one";
        EXPECT_LT(t, uint32_t{1} << 24);
        tags.insert(t);
    }
    EXPECT_GT(tags.size(), 990u) << "the fold spreads";
}
