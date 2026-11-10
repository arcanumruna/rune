// NamespacePathsLayoutRegressionTest.cpp - the one place a test spells the on-disk directory names (#884).
//
// scripts/check_no_topics_literal.py forbids the "topics" literal in every libs/ source except NamespacePaths, so
// tests build paths through rune/node/NamespacePaths.h.  This file pins what those names ARE: renaming the dir would
// orphan every existing data directory, so the spelling itself is a compatibility contract.
#include <rune/node/NamespacePaths.h>

#include <gtest/gtest.h>

#include <filesystem>

namespace fs = std::filesystem;
using namespace rune::node;

TEST(NamespacePathsLayoutRegression, OnDiskNamesArePinned) {
    EXPECT_EQ(kTopicsDirName, "topics");
    EXPECT_EQ(kNsDirPrefix, "ns-");
    EXPECT_EQ(nsDirName(1), "ns-1");
}

TEST(NamespacePathsLayoutRegression, TopicsRootIsUnderTheNamespaceRoot) {
    const fs::path d{"/data"};
    EXPECT_EQ(nsRoot(d, 1), fs::path("/data/ns-1"));
    EXPECT_EQ(topicsRoot(d, 1), fs::path("/data/ns-1/topics"));
    EXPECT_EQ(topicsRoot(d, 0), fs::path("/data/ns-0/topics"));
    EXPECT_EQ(topicsRoot(d, 4294967295u), fs::path("/data/ns-4294967295/topics"));
}
