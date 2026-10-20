// WorkerOpWireRegressionTest.cpp — Phase 2c Task 5: the internal-ring wire values are append-only.
// OpType::FILTER_SCHEMA = 23 (control -> every worker: install a typed topic's filter schema) and
// TailKind::FILTER = 10 (the schema bytes / a Subscribe op's filter bytes) must never move.
#include <rune_protocol_internal_sbe/OpType.h>
#include <rune_protocol_internal_sbe/TailKind.h>

#include <gtest/gtest.h>

namespace sbe = rune::protocol::internal::sbe;

TEST(WorkerOpWireRegression, FilterOpAndTailValuesAreAppendedRegression) {
    EXPECT_EQ(static_cast<int>(sbe::OpType::Value::FILTER_SCHEMA), 23);
    EXPECT_EQ(static_cast<int>(sbe::TailKind::Value::FILTER), 10);
    // The values before them are unchanged (append-only).
    EXPECT_EQ(static_cast<int>(sbe::OpType::Value::PEER_LINK_KEYS), 22);
    EXPECT_EQ(static_cast<int>(sbe::TailKind::Value::PEER_KEYS), 9);
}
