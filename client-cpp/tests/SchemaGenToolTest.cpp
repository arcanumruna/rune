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
#include "rune/tools/SchemaGen.h"
#include <rune/codec/Descriptor.h>
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
