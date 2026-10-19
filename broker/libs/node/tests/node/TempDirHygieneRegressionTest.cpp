// TempDirHygieneRegressionTest.cpp - catalog history Task 21 fix round 1 (R5 / I5): the catalog cluster harness
// removes its directory tree on success AND when its constructor throws (the destructor of a half-built object
// never runs, so the tree is owned by a member guard).
#include "support/CatalogClusterHarness.h"

#include <gtest/gtest.h>

#include <filesystem>
#include <stdexcept>
#include <string>
#include <vector>

#include <unistd.h>

namespace {

using rune::node::test::CatalogClusterHarness;
namespace fs = std::filesystem;

std::vector<std::string> ownTrees() {
    std::vector<std::string> out;
    const std::string prefix = "rune_cluster_" + std::to_string(::getpid()) + "_";
    for (const auto& e : fs::directory_iterator(fs::temp_directory_path())) {
        const std::string n = e.path().filename().string();
        if (n.rfind(prefix, 0) == 0) out.push_back(n);
    }
    return out;
}

} // namespace

TEST(TempDirHygieneRegression, CatalogHarnessLeavesNoDirOnSuccess) {
    ASSERT_TRUE(ownTrees().empty());
    {
        CatalogClusterHarness h("hyg", 3);
        EXPECT_EQ(ownTrees().size(), 1u);
    }
    EXPECT_TRUE(ownTrees().empty()) << "a destroyed CatalogClusterHarness left its tree behind";
}

TEST(TempDirHygieneRegression, CatalogHarnessLeavesNoDirWhenTheCtorThrowsBeforeOpen) {
    ASSERT_TRUE(ownTrees().empty());
    EXPECT_THROW(CatalogClusterHarness("hyg", 2), std::logic_error);   // brokers != kBrokers: thrown in the ctor body
    EXPECT_TRUE(ownTrees().empty());
}

TEST(TempDirHygieneRegression, CatalogHarnessLeavesNoDirWhenTheCtorThrowsAfterTheBrokersOpened) {
    ASSERT_TRUE(ownTrees().empty());
    CatalogClusterHarness::failAfterOpenForTest = true;
    EXPECT_THROW(CatalogClusterHarness("hyg", 3), std::logic_error);   // after open_(1..3) made the broker dirs
    CatalogClusterHarness::failAfterOpenForTest = false;
    EXPECT_TRUE(ownTrees().empty()) << "a throwing ctor leaked the 3-broker tree";
}
