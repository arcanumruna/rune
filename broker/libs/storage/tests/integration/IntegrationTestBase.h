#pragma once

// ============================================================================
// IntegrationTestBase.h
//
// Base fixture for rune-broker storage integration tests.
//
// Responsibilities:
//   - Create a unique temporary directory per test (SetUp/TearDown).
//   - Provide produce/consume helpers that operate on a real PartitionApi.
//   - Provide filesystem manipulation helpers for crash/corruption scenarios.
//   - Provide segment counting helpers.
//
// Usage:
//   class MyTest : public IntegrationTestBase {};
//   TEST_F(MyTest, SomeScenario) { ... }
//
// No Docker, no external processes — all I/O is on the real local filesystem.
// ============================================================================

#include <gtest/gtest.h>
#include <rune/storage/PartitionApi.h>

#include <algorithm>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <random>
#include <string>
#include <vector>

namespace fs = std::filesystem;

// ---------------------------------------------------------------------------
// Produced record descriptor (key, value, timestamp) — lets tests verify
// round-trips without relying on ConsumedRecord's field ordering.
// ---------------------------------------------------------------------------
struct ProducedRecord {
    std::string key;
    std::string value;
    int64_t     timestamp;
};

// ---------------------------------------------------------------------------
// IntegrationTestBase
// ---------------------------------------------------------------------------
class IntegrationTestBase : public ::testing::Test {
protected:
    // The unique per-test temp directory — created in SetUp, removed in TearDown.
    fs::path testDir_;

    // -----------------------------------------------------------------------
    // SetUp / TearDown
    // -----------------------------------------------------------------------
    void SetUp() override {
        // Generate a unique name so parallel test runs don't collide.
        std::mt19937_64 rng(std::random_device{}());
        std::string suffix = std::to_string(rng());
        testDir_ = fs::temp_directory_path() / ("rune_it_" + suffix);
        fs::create_directories(testDir_);
    }

    void TearDown() override {
        std::error_code ec;
        fs::remove_all(testDir_, ec);
        // Ignore errors — if a test already cleaned up manually that's fine.
    }

    // -----------------------------------------------------------------------
    // Helper: makeValue
    //   Produces a deterministic byte string of `vsize` bytes for record `i`.
    //   Includes high bytes so we catch encoding bugs (not just ASCII round-trips).
    // -----------------------------------------------------------------------
    static std::string makeValue(int i, int vsize = 32) {
        std::string v;
        v.reserve(static_cast<size_t>(vsize));
        for (int b = 0; b < vsize; ++b) {
            // Mix of low/high bytes, NUL bytes at every 7th position.
            uint8_t byte;
            if (b % 7 == 0)
                byte = 0x00;
            else
                byte = static_cast<uint8_t>((i * 31 + b * 17 + 0xAB) & 0xFF);
            v.push_back(static_cast<char>(byte));
        }
        return v;
    }

    // -----------------------------------------------------------------------
    // Helper: makeKey
    // -----------------------------------------------------------------------
    static std::string makeKey(int i) {
        return "key-" + std::to_string(i);
    }

    // -----------------------------------------------------------------------
    // Helper: produceN
    //   Produces `n` records into `api` starting from ts=1000+0, 1000+1, ...
    //   Returns a vector of ProducedRecord so the caller can verify round-trips.
    // -----------------------------------------------------------------------
    static std::vector<ProducedRecord> produceN(
            rune::storage::PartitionApi& api,
            int n,
            int vsize = 32) {
        std::vector<ProducedRecord> produced;
        produced.reserve(static_cast<size_t>(n));
        for (int i = 0; i < n; ++i) {
            ProducedRecord rec;
            rec.key       = makeKey(i);
            rec.value     = makeValue(i, vsize);
            rec.timestamp = static_cast<int64_t>(1000 + i);
            api.produce(rec.key, rec.value, rec.timestamp);
            produced.push_back(std::move(rec));
        }
        return produced;
    }

    // -----------------------------------------------------------------------
    // Helper: consumeAll
    //   Consumes everything from offset 0.
    // -----------------------------------------------------------------------
    static std::vector<rune::storage::ConsumedRecord> consumeAll(
            rune::storage::PartitionApi& api) {
        return api.consume(0);
    }

    // -----------------------------------------------------------------------
    // Helper: corruptFileByte
    //   Flips one byte at `pos` in the file at `path`.
    //   pos == -1  → last byte of the file (default).
    //   Any non-negative value targets that exact byte offset.
    // -----------------------------------------------------------------------
    static void corruptFileByte(const fs::path& path, std::streamoff pos = -1) {
        std::fstream f(path, std::ios::in | std::ios::out | std::ios::binary);
        ASSERT_TRUE(f.is_open()) << "corruptFileByte: cannot open " << path;

        std::streampos seekPos;
        if (pos < 0) {
            f.seekg(0, std::ios::end);
            std::streampos size = f.tellg();
            ASSERT_GT(size, 0) << "corruptFileByte: file is empty";
            seekPos = size - std::streampos(1);
        } else {
            seekPos = static_cast<std::streampos>(pos);
        }
        f.seekg(seekPos);
        char byte;
        f.get(byte);
        byte = static_cast<char>(~static_cast<uint8_t>(byte)); // flip all bits
        f.seekp(seekPos);
        f.put(byte);
    }

    // -----------------------------------------------------------------------
    // Helper: appendJunk
    //   Appends `nbytes` of non-zero junk bytes to the file at `path`.
    // -----------------------------------------------------------------------
    static void appendJunk(const fs::path& path, int nbytes) {
        std::ofstream f(path, std::ios::binary | std::ios::app);
        ASSERT_TRUE(f.is_open()) << "appendJunk: cannot open " << path;
        std::vector<uint8_t> junk(static_cast<size_t>(nbytes));
        // Fill with recognisably non-zero junk so it can't accidentally be
        // interpreted as a valid zero-length batch.
        for (int i = 0; i < nbytes; ++i)
            junk[static_cast<size_t>(i)] = static_cast<uint8_t>(0xAA + (i & 0x0F));
        f.write(reinterpret_cast<const char*>(junk.data()),
                static_cast<std::streamsize>(nbytes));
    }

    // -----------------------------------------------------------------------
    // Helper: truncateFileTo
    //   Truncates `path` to `newSize` bytes.
    // -----------------------------------------------------------------------
    static void truncateFileTo(const fs::path& path, uintmax_t newSize) {
        std::error_code ec;
        fs::resize_file(path, newSize, ec);
        ASSERT_FALSE(ec) << "truncateFileTo: " << ec.message();
    }

    // -----------------------------------------------------------------------
    // Helper: countLogSegments
    //   Returns the number of *.log files under `dir`.
    // -----------------------------------------------------------------------
    static int countLogSegments(const fs::path& dir) {
        int count = 0;
        for (auto& entry : fs::directory_iterator(dir))
            if (entry.path().extension() == ".log")
                ++count;
        return count;
    }

    // -----------------------------------------------------------------------
    // Helper: logFilePaths
    //   Returns a sorted list of *.log file paths under `dir`.
    // -----------------------------------------------------------------------
    static std::vector<fs::path> logFilePaths(const fs::path& dir) {
        std::vector<fs::path> paths;
        for (auto& entry : fs::directory_iterator(dir))
            if (entry.path().extension() == ".log")
                paths.push_back(entry.path());
        std::sort(paths.begin(), paths.end());
        return paths;
    }

    // -----------------------------------------------------------------------
    // Helper: activeLogPath
    //   Returns the path to the active (last) .log segment under `dir`.
    // -----------------------------------------------------------------------
    static fs::path activeLogPath(const fs::path& dir) {
        auto paths = logFilePaths(dir);
        EXPECT_FALSE(paths.empty()) << "activeLogPath: no .log files in " << dir;
        return paths.back();
    }

    // -----------------------------------------------------------------------
    // Assertion helper: verifyContiguous
    //   Checks that `records` has exactly `expectedCount` entries with
    //   offsets [0, expectedCount), and that values match `produced[i].value`.
    // -----------------------------------------------------------------------
    static void verifyContiguous(
            const std::vector<rune::storage::ConsumedRecord>& records,
            const std::vector<ProducedRecord>& produced,
            int expectedCount,
            const char* context = "") {
        ASSERT_EQ(static_cast<int>(records.size()), expectedCount)
            << context << ": record count mismatch";
        for (int i = 0; i < expectedCount; ++i) {
            EXPECT_EQ(records[static_cast<size_t>(i)].offset, static_cast<int64_t>(i))
                << context << ": offset mismatch at i=" << i;
            EXPECT_EQ(records[static_cast<size_t>(i)].value,
                      produced[static_cast<size_t>(i)].value)
                << context << ": value mismatch at i=" << i;
            EXPECT_EQ(records[static_cast<size_t>(i)].key,
                      produced[static_cast<size_t>(i)].key)
                << context << ": key mismatch at i=" << i;
            EXPECT_EQ(records[static_cast<size_t>(i)].timestamp,
                      produced[static_cast<size_t>(i)].timestamp)
                << context << ": timestamp mismatch at i=" << i;
        }
    }
};
