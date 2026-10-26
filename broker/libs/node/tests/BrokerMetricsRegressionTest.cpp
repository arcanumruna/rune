// arcanum-metrics plan Task 7 (D60): the broker consumes arcanum-metrics like the flight
// recorder (ARCANUM_METRICS_DIR or FetchContent at the pinned commit).  This test pins the
// layout version Rune is built against: a library bump that changes the file layout must
// fail here at compile time, and a created file must read back that version at run time.
#include <metrics/MetricsFile.h>
#include <metrics/MetricsLayout.h>

#include <gtest/gtest.h>

#include <cstdint>
#include <cstring>
#include <filesystem>
#include <string>
#include <unistd.h>

static_assert(metrics::kLayoutVersion == 2, "Rune is built against arcanum-metrics layout version 2");

TEST(BrokerMetricsRegression, LibraryVersionRegression) {
    // Per-binary TMPDIR (cmake/RuneTestTmpRoot.cmake) keeps this off other test binaries' files.
    const std::string path =
        (std::filesystem::temp_directory_path() / ("rune-broker-metrics-" + std::to_string(::getpid()) + ".bin"))
            .string();
    std::filesystem::remove(path);

    metrics::MetricsFile writer;
    const metrics::CreateParams params{path.c_str(), 16, 16, static_cast<int64_t>(::getpid()), 1, {}};
    ASSERT_EQ(metrics::MetricsFile::create(params, writer), metrics::FileStatus::Ok);

    int32_t written = 0;
    std::memcpy(&written, static_cast<const std::byte*>(writer.base()) + metrics::kHdrLayoutVersionOffset,
                sizeof(written));
    EXPECT_EQ(written, 2);

    // A reader (as a separate process would) accepts the file only at the expected version.
    metrics::MetricsFile reader;
    ASSERT_EQ(metrics::MetricsFile::open(path.c_str(), 4, reader), metrics::FileStatus::Ok);
    int32_t readBack = 0;
    std::memcpy(&readBack, static_cast<const std::byte*>(reader.base()) + metrics::kHdrLayoutVersionOffset,
                sizeof(readBack));
    EXPECT_EQ(readBack, metrics::kLayoutVersion);

    std::filesystem::remove(path);
}
