// Spec §6 golden logs: a recorded formatVer-1 log, checked in, must replay to the
// SAME pinned digests forever.  A failure here means a v1 rule changed — ship the
// change under formatVer 2 instead (Global Constraints).
#include <rune/catalog/CatalogFile.h>
#include <rune/catalog/CatalogReplayer.h>
#include <rune/catalog/testing/GoldenScript.h>
#include "CatalogGoldenPins.h"
#include <fstream>
#include <iterator>
#include <gtest/gtest.h>

TEST(CatalogGoldenRegression, V1LogsReplayToPinnedFingerprints) {
    std::ifstream in(RUNE_CATALOG_GOLDEN_DIR "/v1/basic.rcat", std::ios::binary);
    ASSERT_TRUE(in) << "missing golden log";
    auto records = rune::catalog::readRcat(in);
    ASSERT_TRUE(records.has_value()) << records.error();
    ASSERT_EQ(records->size(), std::size(rune::catalog::golden::kBasicV1Digests));
    rune::catalog::CatalogReplayer r;
    for (std::size_t i = 0; i < records->size(); ++i) {
        auto a = r.feed((*records)[i].first, (*records)[i].second);
        ASSERT_TRUE(a.has_value()) << rune::catalog::describe(a.error());
        EXPECT_EQ(r.state().digest(), rune::catalog::golden::kBasicV1Digests[i]) << "offset " << i;
    }
}

TEST(CatalogGoldenRegression, TheGoldenScriptStillProducesTheCheckedInBytes) {
    // Guards the generator itself: re-encoding the script must equal the file.
    std::ifstream in(RUNE_CATALOG_GOLDEN_DIR "/v1/basic.rcat", std::ios::binary);
    auto records = rune::catalog::readRcat(in);
    ASSERT_TRUE(records.has_value());
    const auto script = rune::catalog::testing::goldenScriptV1();
    ASSERT_EQ(records->size(), script.size());
    for (std::size_t i = 0; i < script.size(); ++i) {
        EXPECT_EQ((*records)[i].first, static_cast<int64_t>(i));
        EXPECT_EQ((*records)[i].second, rune::catalog::encode(script[i]).value()) << "offset " << i;
    }
}

TEST(CatalogGoldenRegression, TheGoldenLogDecodesToTheScriptEventByEvent) {
    // Both directions: the frozen bytes decode to exactly the scripted events (every field,
    // replayedFrom present and absent), so the decoder is pinned as well as the encoder.
    std::ifstream in(RUNE_CATALOG_GOLDEN_DIR "/v1/basic.rcat", std::ios::binary);
    auto records = rune::catalog::readRcat(in);
    ASSERT_TRUE(records.has_value());
    const auto script = rune::catalog::testing::goldenScriptV1();
    ASSERT_EQ(records->size(), script.size());
    for (std::size_t i = 0; i < script.size(); ++i) {
        auto e = rune::catalog::decode((*records)[i].second);
        ASSERT_TRUE(e.has_value()) << "offset " << i;
        EXPECT_EQ(*e, script[i]) << "offset " << i;
    }
}
