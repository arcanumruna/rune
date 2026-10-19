// Authentication spec §11 golden logs: a recorded formatVer-1 __security log, checked in, must replay
// to the SAME pinned digests forever.  A failure here means a v1 rule changed — ship the change under
// formatVer 2 instead (Global Constraints).
#include <rune/catalog/CatalogFile.h>
#include <rune/catalog/RecordFile.h>
#include <rune/security/SecurityFile.h>
#include <rune/security/SecurityReplayer.h>
#include <rune/security/testing/SecurityGoldenScript.h>
#include "SecurityGoldenPins.h"
#include <gtest/gtest.h>

#include <fstream>
#include <iterator>
#include <sstream>

TEST(SecurityGoldenRegression, V1LogsReplayToPinnedFingerprints) {
    std::ifstream in(RUNE_SECURITY_GOLDEN_DIR "/v1/basic.rsec", std::ios::binary);
    ASSERT_TRUE(in) << "missing golden log";
    auto records = rune::security::readRsec(in);
    ASSERT_TRUE(records.has_value()) << records.error();
    ASSERT_EQ(records->size(), std::size(rune::security::golden::kBasicV1Digests));
    rune::security::SecurityReplayer r;
    for (std::size_t i = 0; i < records->size(); ++i) {
        auto a = r.feed((*records)[i].first, (*records)[i].second);
        ASSERT_TRUE(a.has_value()) << rune::security::describe(a.error());
        EXPECT_EQ(r.state().digest(), rune::security::golden::kBasicV1Digests[i]) << "offset " << i;
    }
}

TEST(SecurityGoldenRegression, TheGoldenScriptStillProducesTheCheckedInBytes) {
    // Guards the generator itself: re-encoding the script must equal the file.
    std::ifstream in(RUNE_SECURITY_GOLDEN_DIR "/v1/basic.rsec", std::ios::binary);
    auto records = rune::security::readRsec(in);
    ASSERT_TRUE(records.has_value());
    const auto script = rune::security::testing::goldenScriptV1();
    ASSERT_EQ(records->size(), script.size());
    for (std::size_t i = 0; i < script.size(); ++i) {
        EXPECT_EQ((*records)[i].first, static_cast<int64_t>(i));
        EXPECT_EQ((*records)[i].second, rune::security::encode(script[i]).value()) << "offset " << i;
    }
}

TEST(SecurityGoldenRegression, TheGoldenLogDecodesToTheScriptEventByEvent) {
    // Both directions: the frozen bytes decode to exactly the scripted events (every field), so the
    // decoder is pinned as well as the encoder.
    std::ifstream in(RUNE_SECURITY_GOLDEN_DIR "/v1/basic.rsec", std::ios::binary);
    auto records = rune::security::readRsec(in);
    ASSERT_TRUE(records.has_value());
    const auto script = rune::security::testing::goldenScriptV1();
    ASSERT_EQ(records->size(), script.size());
    for (std::size_t i = 0; i < script.size(); ++i) {
        auto e = rune::security::decode((*records)[i].second);
        ASSERT_TRUE(e.has_value()) << "offset " << i;
        EXPECT_EQ(*e, script[i]) << "offset " << i;
    }
}

TEST(SecurityGoldenRegression, TheGoldenScriptCoversEveryKindAndNeverCarriesAPassword) {
    bool seen[11] = {};
    for (const auto& e : rune::security::testing::goldenScriptV1()) {
        seen[static_cast<int>(e.kind)] = true;
        if (const auto p = e.statement.find("PASSWORD '"); p != std::string::npos)
            EXPECT_EQ(e.statement.compare(p, 22, "PASSWORD '<redacted>';"), 0) << e.statement;
    }
    for (int k = 1; k <= 10; ++k) EXPECT_TRUE(seen[k]) << "kind " << k;
}

TEST(RecordFileRegression, RcatBytesAreUnchangedByTheGeneralization) {
    // Re-encode the checked-in catalog golden through the generalized container with magic RCAT:
    // the bytes must equal the file the pre-generalization writer produced.
    std::ifstream in(RUNE_CATALOG_GOLDEN_DIR "/v1/basic.rcat", std::ios::binary);
    ASSERT_TRUE(in);
    const std::string original((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
    std::istringstream again(original);
    auto records = rune::catalog::readRecordFile(again, rune::catalog::kRcatMagic);
    ASSERT_TRUE(records.has_value()) << records.error();
    std::ostringstream out(std::ios::binary);
    rune::catalog::writeRecordFileHeader(out, rune::catalog::kRcatMagic);
    for (const auto& [off, bytes] : *records) rune::catalog::writeRecordFileRecord(out, off, bytes);
    EXPECT_EQ(out.str(), original);
    // and through the .rcat-named functions
    std::istringstream third(original);
    auto viaRcat = rune::catalog::readRcat(third);
    ASSERT_TRUE(viaRcat.has_value());
    EXPECT_EQ(*viaRcat, *records);
}

TEST(RecordFileRegression, RsecAndRcatAreDistinctByMagicOnly) {
    std::ostringstream out(std::ios::binary);
    rune::security::writeRsecHeader(out);
    rune::security::writeRsecRecord(out, 0, std::vector<uint8_t>{1, 2, 3});
    std::istringstream asRsec(out.str());
    auto ok = rune::security::readRsec(asRsec);
    ASSERT_TRUE(ok.has_value()) << ok.error();
    EXPECT_EQ(ok->size(), 1u);
    std::istringstream asRcat(out.str());
    auto bad = rune::catalog::readRcat(asRcat);
    ASSERT_FALSE(bad.has_value());
    EXPECT_NE(bad.error().find("rcat: bad magic"), std::string::npos) << bad.error();
    std::istringstream rcatFile(std::string("RCAT") + std::string(2, '\0'));
    auto wrong = rune::security::readRsec(rcatFile);
    ASSERT_FALSE(wrong.has_value());
    EXPECT_NE(wrong.error().find("rsec: bad magic"), std::string::npos) << wrong.error();
}
