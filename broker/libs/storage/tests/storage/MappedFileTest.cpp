#include <gtest/gtest.h>
#include <rune/storage/MappedFile.h>
#include <filesystem>
using rune::storage::MappedFile;

TEST(MappedFile, WriteReadPersistAcrossReopen) {
    auto p = std::filesystem::temp_directory_path() / "rune_mf.bin";
    std::filesystem::remove(p);
    { MappedFile mf(p.string(), 4096, true); mf.data()[0] = 42; mf.data()[4095] = 7; mf.sync(); }
    { MappedFile mf(p.string(), 4096, false); EXPECT_EQ(mf.data()[0], 42); EXPECT_EQ(mf.data()[4095], 7); }
    std::filesystem::remove(p);
}
