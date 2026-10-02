#include <gtest/gtest.h>

#include "rune/replication/DiagFlags.h"
#include <rune/replication/PartitionReplica.h>
#include <rune/storage/Log.h>

#include <spdlog/spdlog.h>
#include <spdlog/sinks/ostream_sink.h>

#include <cstdlib>
#include <filesystem>
#include <memory>
#include <sstream>
#include <string>
#include <unistd.h>

using rune::replication::diagFlagFromEnv;

// A default deployment (env unset) must not run the per-fetch FollowerSync
// trace: it formats an info line on every follower fetch emission.
TEST(DiagFlagsRegression, FollowerSyncTraceIsOffByDefaultRegression) {
    EXPECT_FALSE(diagFlagFromEnv(nullptr)) << "unset = off";
    EXPECT_FALSE(diagFlagFromEnv("")) << "empty = off";
    EXPECT_FALSE(diagFlagFromEnv("0")) << "\"0\" = off";
    EXPECT_TRUE(diagFlagFromEnv("1"));
    EXPECT_TRUE(diagFlagFromEnv("yes"));
    EXPECT_TRUE(diagFlagFromEnv("01"));
}

// Review (eos-dup-fix-review §2.1): the parser test above cannot catch a call
// site reverted to its own default-ON getenv lambda.  This drives the REAL
// FollowerSync trace call site (PartitionReplica::becomeFollower logs
// "FollowerSync[...] reset" when the trace is on) under the process default
// env and captures the default logger: nothing may be logged.
TEST(DiagFlagsRegression, FollowerSyncTraceCallSiteSilentByDefaultRegression) {
    if (std::getenv("RUNE_FS_LOG") != nullptr)
        GTEST_SKIP() << "RUNE_FS_LOG is set in this environment; the default is not under test";

    std::ostringstream captured;
    auto sink   = std::make_shared<spdlog::sinks::ostream_sink_st>(captured);
    auto prev   = spdlog::default_logger();
    auto logger = std::make_shared<spdlog::logger>("diagflags-capture", sink);
    logger->set_level(spdlog::level::trace);
    spdlog::set_default_logger(logger);

    {
        const auto dir = std::filesystem::temp_directory_path() /
                         ("rune_diagflags_" + std::to_string(::getpid()));
        std::filesystem::create_directories(dir);
        {
            rune::storage::Log log(dir.string());
            rune::replication::InMemoryTransport transport;
            rune::replication::PartitionConfig cfg;
            cfg.partitionId = 0;
            cfg.selfId      = 2;
            rune::replication::PartitionReplica replica(
                cfg, rune::replication::Role::Leader, /*epoch=*/1,
                {1, 2}, {1, 2}, &log, &transport);
            replica.becomeFollower(/*leaderEpoch=*/2, /*leaderId=*/1);   // trace call site
            spdlog::info("diagflags-probe");                               // capture control
        }
        std::error_code ec;
        std::filesystem::remove_all(dir, ec);
    }

    spdlog::set_default_logger(prev);
    const std::string out = captured.str();
    ASSERT_NE(out.find("diagflags-probe"), std::string::npos) << "capture sink not wired";
    EXPECT_EQ(out.find("FollowerSync["), std::string::npos)
        << "FollowerSync trace logged with RUNE_FS_LOG unset:\n" << out;
}
