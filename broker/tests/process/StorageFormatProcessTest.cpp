// StorageFormatProcessTest.cpp — the REAL rune_broker binary on an unformatted data dir, and the
// `rune_broker storage format` CLI (ISR design v3 §3.2, Step 5): exact exit codes and texts.
#include "SpawnBroker.h"

#include <gtest/gtest.h>

#include <csignal>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <string>

#include <sys/wait.h>
#include <unistd.h>

namespace fs = std::filesystem;
using namespace std::chrono_literals;
using namespace rune::processtest;

namespace {

std::size_t entryCount(const fs::path& d) {
    std::size_t n = 0;
    for ([[maybe_unused]] const auto& e : fs::directory_iterator(d)) ++n;
    return n;
}

std::string slurp(const fs::path& p) {
    std::ifstream f(p, std::ios::binary);
    std::stringstream ss;
    ss << f.rdbuf();
    return ss.str();
}

/// Run `rune_broker <args>` with no RUNE_* env; returns the exit code (-1: hang or signal death).
int runCli(const fs::path& dir, int seq, std::vector<std::string> args, std::string& out) {
    SpawnOptions o;
    o.brokerEnv = false;
    o.args = std::move(args);
    Broker b = spawnBrokerIn(dir, seq, o);
    if (b.pid <= 0) return -1;
    const int st = awaitExit(b, 30s);
    out = b.log();
    if (st == -1 || !WIFEXITED(st)) return -1;
    return WEXITSTATUS(st);
}

} // namespace

// No RUNE_STORAGE_FORMAT on an empty data dir: exit 78 within 10 s, the R-1 text names the command, and the
// dir stays empty.  RED: format on unset (the broker starts and never exits).
TEST(StorageFormatProcessRegression, EmptyDirWithoutFormatExits78AndNamesTheCommandRegression) {
    const fs::path dir = scratchDir("rune_storage_proc", 0);
    fs::create_directories(dir / "data");   // an existing, empty (mounted) volume
    SpawnOptions o;
    Broker b = spawnBrokerIn(dir, 0, o);
    ASSERT_GT(b.pid, 0) << "posix_spawn failed for " << RUNE_BROKER_BIN;
    const int st = awaitExit(b, 10s);
    ASSERT_NE(st, -1) << "the broker did not refuse within 10 s:\n" << b.log();
    ASSERT_TRUE(WIFEXITED(st)) << b.log();
    EXPECT_EQ(WEXITSTATUS(st), 78) << b.log();
    const std::string log = b.log();
    const std::string d = b.dataDir().string();
    EXPECT_NE(log.find("rune-broker: refusing to start — data dir " + d +
                       " is empty and not formatted (no storage.meta); nothing was written."), std::string::npos) << log;
    EXPECT_NE(log.find("rune_broker storage format --new --data-dir " + d), std::string::npos) << log;
    EXPECT_NE(log.find("rune_broker storage format --join --data-dir " + d), std::string::npos) << log;
    EXPECT_EQ(entryCount(b.dataDir()), 0u) << "the refusal wrote into the data dir";
    std::error_code ec;
    fs::remove_all(dir, ec);
}

// The CLI rows: --join formats once, again refuses (1), --ignore-formatted is 0, bad arguments are 64, and
// --reassign-join on an empty dir is 1.  RED: overwrite an already formatted dir.
TEST(StorageFormatProcessRegression, CliFormatsOnceThenRefusesRegression) {
    const fs::path dir = scratchDir("rune_storage_proc", 1);
    const std::string d = (dir / "data").string();   // missing: the CLI creates it
    std::string out;
    ASSERT_EQ(runCli(dir, 0, {"storage", "format", "--join", "--data-dir", d}, out), 0) << out;
    EXPECT_NE(out.find("formatted " + d + ": storageId="), std::string::npos) << out;
    EXPECT_NE(out.find("origin=joiner (joins the running cluster as a new or replaced disk)"), std::string::npos) << out;
    const std::string meta = slurp(fs::path(d) / "storage.meta");
    EXPECT_NE(meta.find("origin=joiner\n"), std::string::npos) << meta;

    ASSERT_EQ(runCli(dir, 1, {"storage", "format", "--join", "--data-dir", d}, out), 1) << out;
    EXPECT_NE(out.find("refusing: " + d + " is already formatted (storageId="), std::string::npos) << out;
    EXPECT_NE(out.find("origin=joiner); nothing changed."), std::string::npos) << out;
    EXPECT_EQ(slurp(fs::path(d) / "storage.meta"), meta) << "a refused format changed storage.meta";

    ASSERT_EQ(runCli(dir, 2, {"storage", "format", "--join", "--ignore-formatted", "--data-dir", d}, out), 0) << out;
    EXPECT_NE(out.find("is already formatted"), std::string::npos) << out;
    EXPECT_EQ(slurp(fs::path(d) / "storage.meta"), meta);

    EXPECT_EQ(runCli(dir, 3, {"storage", "format"}, out), 64) << out;
    EXPECT_NE(out.find("usage: rune_broker storage format (--new|--join|--reassign-join)"), std::string::npos) << out;
    EXPECT_EQ(runCli(dir, 4, {"storage", "format", "--new", "--join", "--data-dir", d}, out), 64) << out;
    EXPECT_EQ(runCli(dir, 5, {"storage", "format", "--new"}, out), 64) << "neither --data-dir nor RUNE_DATA_DIR: " << out;
    EXPECT_EQ(runCli(dir, 6, {"bogus"}, out), 64) << out;
    EXPECT_NE(out.find("usage: rune_broker [storage format"), std::string::npos) << out;

    const std::string e = (dir / "empty").string();
    ASSERT_EQ(runCli(dir, 7, {"storage", "format", "--reassign-join", "--data-dir", e}, out), 1) << out;
    EXPECT_NE(out.find("refusing: " + e + " is not formatted; --reassign-join re-identifies a restored data dir. "
                       "For an empty dir use --join."), std::string::npos) << out;
    EXPECT_EQ(entryCount(e), 0u);
    std::error_code ec;
    fs::remove_all(dir, ec);
}

// `format --new` writes undecided (the CLI needs no RUNE_PEERS); a single-node start with no
// RUNE_STORAGE_FORMAT persists founder and serves.  RED: keep undecided on a single node.
TEST(StorageFormatProcessRegression, CliNewThenSingleNodeStartServesRegression) {
    const fs::path dir = scratchDir("rune_storage_proc", 2);
    const fs::path data = dir / "data";
    std::string out;
    ASSERT_EQ(runCli(dir, 0, {"storage", "format", "--new", "--data-dir", data.string()}, out), 0) << out;
    EXPECT_NE(out.find("origin=undecided (new cluster; founds once a majority of the configured members has started)"),
              std::string::npos) << out;
    ASSERT_NE(slurp(data / "storage.meta").find("origin=undecided\nmode=new\n"), std::string::npos);

    SpawnOptions o;   // no RUNE_STORAGE_FORMAT
    Broker b = spawnBrokerIn(dir, 1, o);
    ASSERT_GT(b.pid, 0);
    ASSERT_TRUE(awaitLog(b, "is LEADER", 30s)) << "the formatted single node never led:\n" << b.log();
    EXPECT_NE(slurp(data / "storage.meta").find("origin=founder\n"), std::string::npos) << slurp(data / "storage.meta");
    EXPECT_NE(b.log().find("storage: dataDir=" + data.string() + " storageId="), std::string::npos) << b.log();
    EXPECT_NE(b.log().find(" origin=founder state=CONFIRMED"), std::string::npos) << b.log();
    ASSERT_EQ(::kill(b.pid, SIGTERM), 0);
    const int st = awaitExit(b, 30s);
    ASSERT_NE(st, -1) << b.log();
    ASSERT_TRUE(WIFEXITED(st)) << b.log();
    EXPECT_EQ(WEXITSTATUS(st), 0) << b.log();
    std::error_code ec;
    fs::remove_all(dir, ec);
}

// CP1 review M4: a storage failure while reading the identity at boot exits 74 (EX_IOERR), like any storage failure at
// startup, not 1 and not the R-2 "wipe the dir" refusal (78).  RED: no StorageIoError catch in main (exit 1).
TEST(StorageFormatProcessRegression, UnreadableStorageMetaExits74Regression) {
    if (::geteuid() == 0) GTEST_SKIP() << "root reads any file";
    const fs::path dir = scratchDir("rune_storage_proc", 20);
    fs::create_directories(dir / "data");
    {
        std::ofstream f(dir / "data" / "storage.meta", std::ios::binary);
        f << "version=1\n";
    }
    fs::permissions(dir / "data" / "storage.meta", fs::perms::none);
    SpawnOptions o;
    Broker b = spawnBrokerIn(dir, 0, o);
    ASSERT_GT(b.pid, 0);
    const int st = awaitExit(b, 10s);
    ASSERT_NE(st, -1) << "the broker did not exit within 10 s:\n" << b.log();
    ASSERT_TRUE(WIFEXITED(st)) << b.log();
    EXPECT_EQ(WEXITSTATUS(st), 74) << b.log();
    EXPECT_NE(b.log().find("storage failure at startup"), std::string::npos) << b.log();
    std::error_code ec;
    fs::permissions(dir / "data" / "storage.meta", fs::perms::owner_all, ec);
    fs::remove_all(dir, ec);
}
