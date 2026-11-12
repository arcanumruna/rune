// tests/SchemaGenToolTest.cpp
//
// Task 12 — rune_schema_gen tool unit tests.
//
// Tests the --from-file code path via rune::tools::generateFromFile() which
// does not require a broker connection.  Links rune_codec + filesystem only.
//
#include <gtest/gtest.h>
#include <filesystem>
#include <fstream>
#include <string>
#include <unistd.h>
#include "rune/tools/SchemaGen.h"
#include <rune/codec/Descriptor.h>
#include <rune/codec/Fingerprint.h>
#include <rune/codec/LeaseColumns.h>
#include <rune/codec/Serialize.h>

namespace fs = std::filesystem;

// ---------------------------------------------------------------------------
// Regression test suite — runs as part of SchemaGenTool.* ctest filter.
// ---------------------------------------------------------------------------

TEST(SchemaGenTool, FromFileEmitsHeader) {
    using namespace rune::codec;
    auto blob = serialize(DescriptorBuilder("characters", 1)
        .add("id",   PrimType::Int64)
        .add("name", PrimType::String)
        .build());

    fs::path dir = fs::temp_directory_path() / "rune_gen_tool_test";
    fs::remove_all(dir);
    fs::create_directories(dir);

    fs::path blobPath = dir / "characters.blob";
    {
        std::ofstream o(blobPath, std::ios::binary);
        o.write((const char*)blob.data(), (std::streamsize)blob.size());
    }

    auto genPath = rune::tools::generateFromFile(blobPath.string(), dir.string());

    ASSERT_TRUE(fs::exists(genPath));

    std::ifstream in(genPath);
    std::string s((std::istreambuf_iterator<char>(in)), {});

    EXPECT_NE(s.find("struct Characters"), std::string::npos)
        << "generated header must contain 'struct Characters'";
    EXPECT_NE(s.find("kFingerprint"), std::string::npos)
        << "generated header must contain 'kFingerprint'";
}

// Regression: generateFromFile must return the path with the descriptor's
// schema name (not the input blob filename).
TEST(SchemaGenToolRegression, OutputFilenameMatchesSchemaName) {
    using namespace rune::codec;
    auto blob = serialize(DescriptorBuilder("orders", 1)
        .add("orderId", PrimType::Int64)
        .build());

    fs::path dir = fs::temp_directory_path() / "rune_gen_tool_regression";
    fs::remove_all(dir);
    fs::create_directories(dir);

    // intentionally name the blob differently from the schema
    fs::path blobPath = dir / "some_blob.bin";
    {
        std::ofstream o(blobPath, std::ios::binary);
        o.write((const char*)blob.data(), (std::streamsize)blob.size());
    }

    auto genPath = rune::tools::generateFromFile(blobPath.string(), dir.string());

    // Output filename must be derived from the descriptor name, not the blob file.
    fs::path expected = dir / "orders.gen.h";
    EXPECT_EQ(genPath, expected.string())
        << "output path must be outDir/orders.gen.h (schema name), not blob filename";
    ASSERT_TRUE(fs::exists(genPath));
}

// Regression: missing blob file must throw, not crash silently.
TEST(SchemaGenToolRegression, MissingBlobThrows) {
    EXPECT_THROW(
        (void)rune::tools::generateFromFile("/nonexistent/path/blob.bin", std::filesystem::temp_directory_path().string()),
        std::runtime_error);
}

// Regression: malformed blob must throw std::runtime_error (from deserialize).
TEST(SchemaGenToolRegression, MalformedBlobThrows) {
    fs::path dir = fs::temp_directory_path() / "rune_gen_tool_malformed";
    fs::remove_all(dir);
    fs::create_directories(dir);

    fs::path blobPath = dir / "bad.blob";
    {
        std::ofstream o(blobPath, std::ios::binary);
        const uint8_t garbage[] = {0x00, 0x01, 0x02, 0x03};
        o.write((const char*)garbage, sizeof(garbage));
    }

    EXPECT_THROW(
        (void)rune::tools::generateFromFile(blobPath.string(), dir.string()),
        std::runtime_error);
}

// ---------------------------------------------------------------------------
// Lease form offline (--lease): the blob carries no lease block, so the caller says it.
// ---------------------------------------------------------------------------
namespace {

fs::path leaseTestDir(const char* tag) {
    fs::path dir = fs::temp_directory_path() /
                   ("rune_gen_tool_lease_" + std::string(tag) + "_" + std::to_string(::getpid()));
    fs::remove_all(dir);
    fs::create_directories(dir);
    return dir;
}

fs::path writeBlob(const fs::path& dir, rune::codec::Descriptor d) {
    auto blob = rune::codec::serialize(d);
    fs::path p = dir / "in.blob";
    std::ofstream o(p, std::ios::binary);
    o.write((const char*)blob.data(), (std::streamsize)blob.size());
    return p;
}

rune::codec::Descriptor leaseShaped() {
    using namespace rune::codec;
    auto d = DescriptorBuilder("lease_claim")
                 .addFixedStr("claim", 32, 1, true)
                 .add("a", PrimType::UInt64)
                 .build();
    EXPECT_TRUE(prependLeaseColumns(d));
    return d;
}

std::string slurp(const std::string& path) {
    std::ifstream in(path);
    return std::string((std::istreambuf_iterator<char>(in)), {});
}

} // namespace

TEST(SchemaGenToolRegression, FromFileWithLeaseEmitsLeasesRow) {
    fs::path dir = leaseTestDir("on");
    auto gen = rune::tools::generateFromFile(writeBlob(dir, leaseShaped()).string(), dir.string(), true);
    const std::string s = slurp(gen);
    EXPECT_NE(s.find("LeasesRow"), std::string::npos);
    EXPECT_NE(s.find("withLease"), std::string::npos);
    fs::remove_all(dir);
}

TEST(SchemaGenToolRegression, FromFileWithoutLeaseHasNoLeasesRow) {
    fs::path dir = leaseTestDir("off");
    const std::string blob = writeBlob(dir, leaseShaped()).string();
    const std::string a = slurp(rune::tools::generateFromFile(blob, dir.string(), false));
    EXPECT_EQ(a.find("LeasesRow"), std::string::npos);
    EXPECT_EQ(a.find("withLease"), std::string::npos);
    const std::string b = slurp(rune::tools::generateFromFile(blob, dir.string()));
    EXPECT_EQ(b.find("LeasesRow"), std::string::npos);
    EXPECT_EQ(a, b);
    fs::remove_all(dir);
}

// F-6: the lease form must not change the schema fingerprint (a generated lease header that disagreed with the stored schema would be
// refused at runtime).  Both headers carry the same kFingerprint literal, and it is the descriptor's fingerprint.
TEST(SchemaGenToolRegression, LeaseFlagDoesNotChangeTheFingerprint) {
    fs::path dir = leaseTestDir("fp");
    const auto d = leaseShaped();
    const std::string blob = writeBlob(dir, d).string();
    auto literalOf = [](const std::string& h) {
        const std::string key = "kFingerprint = ";
        const auto at = h.find(key);
        EXPECT_NE(at, std::string::npos);
        return at == std::string::npos ? std::string{} : h.substr(at + key.size(), h.find("ull;", at) - at - key.size());
    };
    const std::string off = literalOf(slurp(rune::tools::generateFromFile(blob, dir.string(), false)));
    const std::string on  = literalOf(slurp(rune::tools::generateFromFile(blob, dir.string(), true)));
    EXPECT_FALSE(on.empty());
    EXPECT_EQ(on, off) << "--lease must not fold into the fingerprint";
    EXPECT_EQ(on, std::to_string(rune::codec::fingerprint(d)));
    fs::remove_all(dir);
}

TEST(SchemaGenToolRegression, FromFileLeaseOnNonLeaseSchemaThrows) {
    using namespace rune::codec;
    fs::path dir = leaseTestDir("bad");
    fs::path blob = writeBlob(dir, DescriptorBuilder("plain", 1).add("id", PrimType::Int64).build());
    EXPECT_THROW((void)rune::tools::generateFromFile(blob.string(), dir.string(), true), std::invalid_argument);
    EXPECT_FALSE(fs::exists(dir / "plain.gen.h"));
    fs::remove_all(dir);
}
