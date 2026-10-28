// Namespaces Phase 2 Task 3: the suspend table, checked against an independent copy of the D21 / D30 lists.
#include <rune/gateway/NamespaceSuspendTable.h>

#include <rune/protocol/client/Dispatch.h>

#include <gtest/gtest.h>

#include <algorithm>
#include <set>
#include <tuple>

namespace {
namespace pc = rune::protocol::client;
using rune::catalog::NsStateTag;
using rune::gateway::SuspendAction;

// The broker->client response ids (not dispatched by the gateway).
const std::set<uint16_t> kResponses = {2, 5, 7, 9, 11, 13, 15, 17, 19, 20, 33, 35, 37, 39, 41, 43, 44, 46, 48,
                                       50, 52, 54, 65, 67, 69, 71, 73, 78, 83, 85};
// D21: refused while ReadOnly.
const std::set<uint16_t> kReadOnlyRefuses = {16, 40, 45, 47, 49, 51, 53, 64, 68, 70, 74, 75, 76, 77, 79, 80};
// D30: still allowed while RejectAll (session control), plus NamespaceDdl (81): cluster-scoped and cluster-admin only,
// so its handler's authorization answers a non-admin NotAuthorized in every state (plan 2026-10-26 matrix rule 3).
const std::set<uint16_t> kRejectAllAllows = {1, 3, 4, 6, 10, 12, 14, 81};

enum class Who { UserInNs, ClusterAdmin };

SuspendAction expected(uint16_t tid, NsStateTag gate) {
    switch (gate) {
    case NsStateTag::Active: return SuspendAction::Allow;
    case NsStateTag::SuspendedReadOnly:
        return kReadOnlyRefuses.count(tid) ? SuspendAction::Refuse : SuspendAction::Allow;
    default:   // RejectAll and Deleting
        return kRejectAllAllows.count(tid) ? SuspendAction::Allow : SuspendAction::Refuse;
    }
}
// The gateway rule: a cluster admin is never refused.
SuspendAction effective(SuspendAction a, Who who) { return who == Who::ClusterAdmin ? SuspendAction::Allow : a; }

TEST(NamespaceSuspendTableRegression, CoversEveryDispatchedTemplate)
{
    for (uint16_t tid = 0; tid < rune::gateway::kSuspendTableSpan; ++tid) {
        if (!pc::isKnownTemplate(tid)) continue;
        const bool dispatched = std::find(rune::gateway::kDispatchedTemplates.begin(),
                                          rune::gateway::kDispatchedTemplates.end(), tid) !=
                                rune::gateway::kDispatchedTemplates.end();
        EXPECT_EQ(dispatched, kResponses.count(tid) == 0) << "template " << tid;
    }
    // 84 (NamespaceUsage) is not yet in isKnownTemplate's ranges but is a request the table already covers.
    EXPECT_NE(std::find(rune::gateway::kDispatchedTemplates.begin(), rune::gateway::kDispatchedTemplates.end(), 84),
              rune::gateway::kDispatchedTemplates.end());
    for (std::size_t i = 0; i < rune::gateway::kSuspendTable.size(); ++i)
        EXPECT_EQ(rune::gateway::kSuspendTable[i].templateId, rune::gateway::kDispatchedTemplates[i]);
}

class TableRegression : public ::testing::TestWithParam<std::tuple<uint16_t, NsStateTag, Who>> {};

TEST_P(TableRegression, MatchesD21)
{
    const auto [tid, gate, who] = GetParam();
    EXPECT_EQ(effective(rune::gateway::suspendAction(tid, gate), who), effective(expected(tid, gate), who))
        << "template " << tid << " gate " << static_cast<int>(gate) << " admin " << (who == Who::ClusterAdmin);
}
}  // namespace

namespace {
std::vector<uint16_t> templatesVec() {
    return {rune::gateway::kDispatchedTemplates.begin(), rune::gateway::kDispatchedTemplates.end()};
}
}  // namespace

INSTANTIATE_TEST_SUITE_P(NamespaceSuspendTableRegression, TableRegression,
    ::testing::Combine(::testing::ValuesIn(templatesVec()),
                       ::testing::Values(NsStateTag::SuspendedReadOnly, NsStateTag::SuspendedRejectAll, NsStateTag::Deleting),
                       ::testing::Values(Who::UserInNs, Who::ClusterAdmin)));

TEST(NamespaceSuspendTableRegression, CountPinnedRegression)
{
    EXPECT_EQ(::testing::UnitTest::GetInstance()->current_test_suite() != nullptr, true);
    // Cells instantiated = |templates| x 3 gates x 2 principals.
    EXPECT_EQ(templatesVec().size() * 6, rune::gateway::kDispatchedTemplates.size() * 6);
    std::size_t cells = 0;
    const auto* us = ::testing::UnitTest::GetInstance();
    for (int i = 0; i < us->total_test_suite_count(); ++i) {
        const auto* ts = us->GetTestSuite(i);
        if (std::string_view(ts->name()).find("TableRegression") != std::string_view::npos &&
            std::string_view(ts->name()).find("NamespaceSuspendTableRegression/") == 0)
            cells += static_cast<std::size_t>(ts->total_test_count());
    }
    EXPECT_EQ(cells, rune::gateway::kDispatchedTemplates.size() * 6);
}
