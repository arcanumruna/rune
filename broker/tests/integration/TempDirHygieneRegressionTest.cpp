// TempDirHygieneRegressionTest.cpp - catalog history Task 21 fix round 1 (R5 / I5): the consensus ClusterHarness
// leaked one empty rune_cluster_* directory per node per harness (17 474 on the reviewer's disk): NodeBundle's
// placeholder TempDir was default-constructed (creating a directory) and the ctor then move-assigned over it,
// and TempDir's defaulted move-assignment overwrote `dir` without removing the old one.
#include "ClusterHarness.h"

#include <gtest/gtest.h>

#include <string>
#include <vector>

#include <unistd.h>

namespace {

using rune::consensus::test::ClusterHarness;
using rune::consensus::test::TempDir;
namespace fs = std::filesystem;

/// The temp entries this process created for the cluster harnesses: rune_cluster_* / rune_node* with "_<pid>_".
std::vector<std::string> ownEntries() {
    std::vector<std::string> out;
    const std::string pid = "_" + std::to_string(::getpid()) + "_";
    for (const auto& e : fs::directory_iterator(fs::temp_directory_path())) {
        const std::string n = e.path().filename().string();
        if ((n.rfind("rune_cluster_", 0) == 0 || n.rfind("rune_node", 0) == 0) && n.find(pid) != std::string::npos) out.push_back(n);
    }
    return out;
}

} // namespace

TEST(TempDirHygieneRegression, ClusterHarnessLeavesNoDir) {
    ASSERT_TRUE(ownEntries().empty());
    {
        ClusterHarness h(3);
        EXPECT_EQ(ownEntries().size(), 3u) << "one live directory per node while the harness lives";
    }
    EXPECT_TRUE(ownEntries().empty()) << "a destroyed ClusterHarness left directories behind";
}

TEST(TempDirHygieneRegression, TempDirMoveAssignRemovesTheOverwrittenDir) {
    fs::path old, kept;
    {
        TempDir a("rune_cluster_t_");
        old = a.dir;
        ASSERT_TRUE(fs::exists(old));
        a = TempDir("rune_cluster_u_");
        EXPECT_FALSE(fs::exists(old)) << "the overwritten directory leaked";
        kept = a.dir;
        EXPECT_TRUE(fs::exists(kept));
        EXPECT_NE(old, kept);
    }
    EXPECT_FALSE(fs::exists(kept)) << "the live directory outlived its TempDir";
}

TEST(TempDirHygieneRegression, PlaceholderTempDirOwnsNothing) {
    const auto before = ownEntries().size();
    {
        TempDir none(nullptr);
        EXPECT_TRUE(none.dir.empty());
        TempDir moved(TempDir("rune_cluster_m_"));
        EXPECT_FALSE(moved.dir.empty());
    }
    EXPECT_EQ(ownEntries().size(), before);
}
