// WorkerOpWireRegressionTest.cpp — Phase 2c Task 5: the internal-ring wire values are append-only.
// OpType::FILTER_SCHEMA = 25 (control -> every worker: install a typed topic's filter schema) and
// TailKind::FILTER = 11 (the schema bytes / a Subscribe op's filter bytes) must never move.
#include <rune_protocol_internal_sbe/OpType.h>
#include <rune_protocol_internal_sbe/TailKind.h>

#include <rune/node/WorkerOp.h>

#include <gtest/gtest.h>

namespace sbe = rune::protocol::internal::sbe;

TEST(WorkerOpWireRegression, FilterOpAndTailValuesAreAppendedRegression) {
    EXPECT_EQ(static_cast<int>(sbe::OpType::Value::FILTER_SCHEMA), 25);
    EXPECT_EQ(static_cast<int>(sbe::TailKind::Value::FILTER), 11);
    // The values before them are unchanged (append-only).
    EXPECT_EQ(static_cast<int>(sbe::OpType::Value::PEER_LINK_KEYS), 22);
    EXPECT_EQ(static_cast<int>(sbe::OpType::Value::READ_LEASE), 24);
    EXPECT_EQ(static_cast<int>(sbe::TailKind::Value::PEER_KEYS), 9);
}

TEST(WorkerOpWireRegression, ClusterFeatureLevelRoundTripsRegression) {
    EXPECT_EQ(static_cast<int>(sbe::OpType::Value::CLUSTER_FEATURE_LEVEL), 31);
    EXPECT_EQ(static_cast<int>(rune::node::WorkerOp::Type::ClusterFeatureLevel), 31);
    EXPECT_EQ(static_cast<int>(sbe::OpType::Value::NS_VIEW_MEM_OVER), 30);
}

TEST(WorkerOpWireRegression, WriteSpecUpdateRoundTripsRegression) {
    EXPECT_EQ(static_cast<int>(sbe::OpType::Value::WRITE_SPEC_UPDATE), 32);
    EXPECT_EQ(static_cast<int>(sbe::TailKind::Value::WRITE_SPEC), 12);
    EXPECT_EQ(static_cast<int>(rune::node::WorkerOp::Type::WriteSpecUpdate), 32);
    EXPECT_EQ(static_cast<int>(sbe::TailKind::Value::FILTER), 11);
}
