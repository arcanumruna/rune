#pragma once
// DdlHarnessTestHelpers.h — the small drivers the DdlHarness regression files
// share (catalog history Task 10; split out of DdlHarnessRegressionTest.cpp in fix
// round 1, review M8).  Test-only.
#include "DdlHarness.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <string>

namespace rune::node::test {

inline void createOk(DdlHarness& h, Op op, const std::string& name, const std::string& src = "src") {
    const int64_t c = h.submit(op, name, src);
    h.settle();
    ASSERT_TRUE(h.response(c).has_value());
    ASSERT_EQ(h.response(c)->err, kOk) << opName(op) << " '" << name << "'";
}
/// An UNTYPED topic (an empty descriptor) created by DDL — the former static
/// schema-less static topic.
inline int64_t submitUntyped(DdlHarness& h, const std::string& name, uint8_t persistence = 0) {
    DefineTopicMsg m;
    m.name = name; m.partitionCount = 1; m.persistence = persistence;
    return h.submitRawDefine(std::move(m));
}
/// `op` on `name` while the ISR is below the catalog's minIsr: its append is
/// refused (NotLeader, nothing appended) — the catalog counterpart of a create
/// that never committed.  Returns its answer code.
inline uint16_t refusedAtAppend(DdlHarness& h, Op op, const std::string& name) {
    h.shrinkIsr();
    const int64_t c = h.submit(op, name);
    h.settle();
    h.restoreIsr();
    h.settle();
    return h.response(c) ? h.response(c)->err : uint16_t{0xFFFF};
}
/// Tick until `corr` is answered; releases held commits only when asked.
inline bool tickUntilAnswered(DdlHarness& h, int64_t corr, bool releaseCommits) {
    for (int i = 0; i < 50 && !h.response(corr).has_value(); ++i) {
        if (releaseCommits) h.releaseCommits();
        h.tick();
    }
    return h.response(corr).has_value();
}
inline void expectIdle(DdlHarness& h, const char* where) {
    EXPECT_EQ(h.worker().inFlightStateName(), "Idle") << where;
    EXPECT_FALSE(h.worker().hasInFlight()) << where;
}
inline DdlHarnessOptions heldCommits() {
    DdlHarnessOptions o;
    o.holdCommits = true;
    return o;
}

} // namespace rune::node::test
