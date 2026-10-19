// ClusterKeysTest — the cluster key files (authentication plan D32).
#include <rune/peerlink/ClusterKeys.h>

#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>
#include <string>

#include <unistd.h>

namespace {

namespace fs = std::filesystem;

struct TmpDir {
    fs::path path;
    TmpDir() {
        static int seq = 0;
        path = fs::temp_directory_path() / ("rune_ckeys_" + std::to_string(::getpid()) + "_" + std::to_string(seq++));
        fs::create_directories(path);
    }
    ~TmpDir() { std::error_code ec; fs::remove_all(path, ec); }
    std::string write(const std::string& name, const std::string& body) const {
        const fs::path p = path / name;
        std::ofstream(p, std::ios::binary) << body;
        return p.string();
    }
};

const std::string kKeyA(40, 'a');
const std::string kKeyB(40, 'b');

} // namespace

TEST(ClusterKeysRegression, KeyIdIsStableAndDiffersPerKey) {
    TmpDir d;
    const auto a1 = rune::peerlink::loadClusterKeys(d.write("a1", kKeyA), "");
    const auto a2 = rune::peerlink::loadClusterKeys(d.write("a2", kKeyA), "");
    const auto b  = rune::peerlink::loadClusterKeys(d.write("b", kKeyB), "");
    ASSERT_TRUE(a1.has_value()) << a1.error();
    ASSERT_TRUE(a2.has_value());
    ASSERT_TRUE(b.has_value());
    EXPECT_EQ(a1->current.keyId, a2->current.keyId);
    EXPECT_EQ(a1->current.master, a2->current.master);
    EXPECT_NE(a1->current.keyId, b->current.keyId);
    EXPECT_FALSE(a1->next.has_value());
}

TEST(ClusterKeysRegression, TrailingWhitespaceIsIgnored) {
    TmpDir d;
    const auto bare = rune::peerlink::loadClusterKeys(d.write("bare", kKeyA), "");
    const auto nl   = rune::peerlink::loadClusterKeys(d.write("nl", kKeyA + "\n"), "");
    const auto crlf = rune::peerlink::loadClusterKeys(d.write("crlf", kKeyA + " \r\n\t\n"), "");
    ASSERT_TRUE(bare.has_value());
    ASSERT_TRUE(nl.has_value());
    ASSERT_TRUE(crlf.has_value());
    EXPECT_EQ(bare->current.keyId, nl->current.keyId);
    EXPECT_EQ(bare->current.keyId, crlf->current.keyId);
}

TEST(ClusterKeysRegression, NextKeyIsFoundById) {
    TmpDir d;
    const auto k = rune::peerlink::loadClusterKeys(d.write("cur", kKeyA), d.write("next", kKeyB));
    ASSERT_TRUE(k.has_value()) << k.error();
    ASSERT_TRUE(k->next.has_value());
    ASSERT_NE(k->byId(k->current.keyId), nullptr);
    ASSERT_NE(k->byId(k->next->keyId), nullptr);
    EXPECT_EQ(k->byId(k->current.keyId), &k->current);
    EXPECT_EQ(k->byId(k->next->keyId), &*k->next);
    EXPECT_EQ(k->byId(k->current.keyId ^ 1), nullptr);
}

TEST(ClusterKeysRegression, MissingUnreadableAndShortFilesAreRefusedNamingVariableAndPath) {
    TmpDir d;
    const std::string missing = (d.path / "nope").string();
    auto r = rune::peerlink::loadClusterKeys(missing, "");
    ASSERT_FALSE(r.has_value());
    EXPECT_NE(r.error().find("RUNE_CLUSTER_KEY_FILE"), std::string::npos) << r.error();
    EXPECT_NE(r.error().find(missing), std::string::npos) << r.error();

    const std::string shortFile = d.write("short", std::string(31, 'x') + "\n");   // 31 bytes after trimming
    r = rune::peerlink::loadClusterKeys(shortFile, "");
    ASSERT_FALSE(r.has_value());
    EXPECT_NE(r.error().find("32"), std::string::npos) << r.error();
    EXPECT_NE(r.error().find(shortFile), std::string::npos) << r.error();

    // The NEXT key follows the same rules and names its own variable.
    const std::string ok = d.write("ok", kKeyA);
    r = rune::peerlink::loadClusterKeys(ok, shortFile);
    ASSERT_FALSE(r.has_value());
    EXPECT_NE(r.error().find("RUNE_CLUSTER_KEY_FILE_NEXT"), std::string::npos) << r.error();

    r = rune::peerlink::loadClusterKeys("", "");
    ASSERT_FALSE(r.has_value());
    EXPECT_NE(r.error().find("RUNE_CLUSTER_KEY_FILE"), std::string::npos) << r.error();
}
