// rune_role reads the arcanum-metrics file (plan 2026-10-26 Task 10).
//
// Drives the REAL rune_role binary (RUNE_ROLE_BIN) against real metrics files written with the real MetricsWriter, in
// the same layout BrokerNode uses (every kRuneMetrics row under the 4-byte LE node-id key, label "<prefix> node=<id>").
//
//   GoldenOutputTable           stdout is byte-identical to the goldens captured from the previous (cnc.dat) binary,
//                               run against a real Aeron driver's counters holding the same values.
//   ExitCodesTable              no file -> 1, version word 0 -> 1 (NotReady on stderr), no role metric -> 2, full
//                               file -> 0; plus the path resolution order (argv, RUNE_METRICS_FILE, RUNE_DATA_DIR).
//   ParsersStillAgreeRegression the dr-e2e harness parsers (_ROLE_RE, parse_rune_role_storage in
//                               dr-e2e/harness/cluster.py) give the same result on the new output as on the golden.
//
// Mutation: print `term` before `role` in rune_role.cpp -> GoldenOutputTable (and ParsersStillAgree) turn red.
#include <rune/node/RuneMetrics.h>

#include <metrics/MetricsFile.h>
#include <metrics/MetricsLayout.h>
#include <metrics/MetricsWriter.h>

#include <gtest/gtest.h>

#include <sys/stat.h>
#include <sys/wait.h>
#include <fcntl.h>
#include <unistd.h>

#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <limits>
#include <span>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#ifndef RUNE_ROLE_BIN
#error "RUNE_ROLE_BIN must name the rune_role binary"
#endif
#ifndef RUNE_DR_E2E_DIR
#error "RUNE_DR_E2E_DIR must name broker/dr-e2e"
#endif

extern char** environ;

namespace {

using rune::node::RuneMetric;
using rune::node::kRuneMetrics;
using rune::node::kNodeMetricCount;
using rune::node::kRoleKeyLength;
using rune::node::encodeRoleKey;
using rune::node::typeIdOf;

constexpr std::int64_t kI64Max = std::numeric_limits<std::int64_t>::max();

// One golden row: the values a node publishes.  storage=false: only the Role and Term metrics exist (a file written
// before the storage gate's metrics were allocated -> every STORAGE value prints -1, the state UNKNOWN).
struct Row {
    std::int32_t nodeId;
    std::int64_t role;
    std::int64_t term;
    bool storage;
    std::int64_t gate, held, pending, oos, lost;
};

// The rows and stdout below were captured from the previous binary (it read the same values out of a real Aeron
// driver's cnc.dat counters).  Do not regenerate them from the new binary: they ARE the compatibility contract.
const Row kRows[] = {
    {1, -1, -1, true, 0, 0, 0, 0, 0},
    {1, -1, -1, true, 1, 0, 3, 2, 0},
    {1, -1, -1, true, 2, 0, 0, -1, 0},
    {1, -1, -1, false, 0, 0, 0, 0, 0},
    {2, 0, 3, true, 0, 0, 0, 0, 0},
    {2, 0, 3, true, 1, 0, 1, 4, 0},
    {2, 0, 3, true, 2, 0, 2, 0, 1},
    {2, 0, 3, false, 0, 0, 0, 0, 0},
    {3, 1, 4, true, 0, 0, 0, 0, 0},
    {3, 1, 4, true, 1, 0, 2, 7, 0},
    {3, 1, 4, true, 2, 0, 2, 0, 0},
    {3, 1, 4, false, 0, 0, 0, 0, 0},
    {7, 2, 9, true, 0, 12, 0, 0, 0},
    {7, 2, 9, true, 1, 12, 1, 3, 2},
    {7, 2, 9, true, 2, 12, 2, 0, 0},
    {7, 2, 9, false, 0, 0, 0, 0, 0},
    {0, 2, 123456789012, true, 7, 1, 0, 0, 0},
    {5, 99, 0, true, -1, -1, -1, -1, -1},
    {2147483647, 1, -1, true, 1, kI64Max, 2, 1000000, 42},
};

const char* const kGolden[] = {
    "ROLE nodeId=1 role=NONE term=-1\nSTORAGE nodeId=1 state=CONFIRMED heldLeaderRoles=0 pendingNodes=0 outOfSync=0 lostCommittedReports=0\n",
    "ROLE nodeId=1 role=NONE term=-1\nSTORAGE nodeId=1 state=UNCONFIRMED heldLeaderRoles=0 pendingNodes=3 outOfSync=2 lostCommittedReports=0\n",
    "ROLE nodeId=1 role=NONE term=-1\nSTORAGE nodeId=1 state=UNDECIDED heldLeaderRoles=0 pendingNodes=0 outOfSync=-1 lostCommittedReports=0\n",
    "ROLE nodeId=1 role=NONE term=-1\nSTORAGE nodeId=1 state=UNKNOWN heldLeaderRoles=-1 pendingNodes=-1 outOfSync=-1 lostCommittedReports=-1\n",
    "ROLE nodeId=2 role=FOLLOWER term=3\nSTORAGE nodeId=2 state=CONFIRMED heldLeaderRoles=0 pendingNodes=0 outOfSync=0 lostCommittedReports=0\n",
    "ROLE nodeId=2 role=FOLLOWER term=3\nSTORAGE nodeId=2 state=UNCONFIRMED heldLeaderRoles=0 pendingNodes=1 outOfSync=4 lostCommittedReports=0\n",
    "ROLE nodeId=2 role=FOLLOWER term=3\nSTORAGE nodeId=2 state=UNDECIDED heldLeaderRoles=0 pendingNodes=2 outOfSync=0 lostCommittedReports=1\n",
    "ROLE nodeId=2 role=FOLLOWER term=3\nSTORAGE nodeId=2 state=UNKNOWN heldLeaderRoles=-1 pendingNodes=-1 outOfSync=-1 lostCommittedReports=-1\n",
    "ROLE nodeId=3 role=CANDIDATE term=4\nSTORAGE nodeId=3 state=CONFIRMED heldLeaderRoles=0 pendingNodes=0 outOfSync=0 lostCommittedReports=0\n",
    "ROLE nodeId=3 role=CANDIDATE term=4\nSTORAGE nodeId=3 state=UNCONFIRMED heldLeaderRoles=0 pendingNodes=2 outOfSync=7 lostCommittedReports=0\n",
    "ROLE nodeId=3 role=CANDIDATE term=4\nSTORAGE nodeId=3 state=UNDECIDED heldLeaderRoles=0 pendingNodes=2 outOfSync=0 lostCommittedReports=0\n",
    "ROLE nodeId=3 role=CANDIDATE term=4\nSTORAGE nodeId=3 state=UNKNOWN heldLeaderRoles=-1 pendingNodes=-1 outOfSync=-1 lostCommittedReports=-1\n",
    "ROLE nodeId=7 role=LEADER term=9\nSTORAGE nodeId=7 state=CONFIRMED heldLeaderRoles=12 pendingNodes=0 outOfSync=0 lostCommittedReports=0\n",
    "ROLE nodeId=7 role=LEADER term=9\nSTORAGE nodeId=7 state=UNCONFIRMED heldLeaderRoles=12 pendingNodes=1 outOfSync=3 lostCommittedReports=2\n",
    "ROLE nodeId=7 role=LEADER term=9\nSTORAGE nodeId=7 state=UNDECIDED heldLeaderRoles=12 pendingNodes=2 outOfSync=0 lostCommittedReports=0\n",
    "ROLE nodeId=7 role=LEADER term=9\nSTORAGE nodeId=7 state=UNKNOWN heldLeaderRoles=-1 pendingNodes=-1 outOfSync=-1 lostCommittedReports=-1\n",
    "ROLE nodeId=0 role=LEADER term=123456789012\nSTORAGE nodeId=0 state=UNKNOWN heldLeaderRoles=1 pendingNodes=0 outOfSync=0 lostCommittedReports=0\n",
    "ROLE nodeId=5 role=NONE term=0\nSTORAGE nodeId=5 state=UNKNOWN heldLeaderRoles=-1 pendingNodes=-1 outOfSync=-1 lostCommittedReports=-1\n",
    "ROLE nodeId=2147483647 role=CANDIDATE term=-1\nSTORAGE nodeId=2147483647 state=UNCONFIRMED heldLeaderRoles=9223372036854775807 pendingNodes=2 outOfSync=1000000 lostCommittedReports=42\n",
};
static_assert(sizeof(kRows) / sizeof(kRows[0]) == sizeof(kGolden) / sizeof(kGolden[0]));

// A pid-scoped scratch dir, removed (files first, then the dir, each by exact path) at the end of the test.
class TempDir {
public:
    TempDir() {
        path_ = "/tmp/rune_t" + std::to_string(::getpid()) + "_role";
        ::mkdir(path_.c_str(), 0700);
    }
    ~TempDir() {
        for (const std::string& f : files_) ::unlink(f.c_str());
        for (auto it = dirs_.rbegin(); it != dirs_.rend(); ++it) ::rmdir(it->c_str());
        ::rmdir(path_.c_str());
    }
    TempDir(const TempDir&) = delete;
    TempDir& operator=(const TempDir&) = delete;

    [[nodiscard]] const std::string& path() const noexcept { return path_; }
    std::string file(const std::string& name) {
        files_.push_back(path_ + "/" + name);
        return files_.back();
    }
    std::string dir(const std::string& name) {
        dirs_.push_back(path_ + "/" + name);
        ::mkdir(dirs_.back().c_str(), 0700);
        return dirs_.back();
    }

private:
    std::string path_;
    std::vector<std::string> files_;
    std::vector<std::string> dirs_;
};

std::int64_t valueFor(const Row& r, RuneMetric m) {
    switch (m) {
        case RuneMetric::Role:                 return r.role;
        case RuneMetric::Term:                 return r.term;
        case RuneMetric::StorageGateState:     return r.gate;
        case RuneMetric::LeaderRolesHeld:      return r.held;
        case RuneMetric::StoragePendingNodes:  return r.pending;
        case RuneMetric::OutOfSyncPartitions:  return r.oos;
        case RuneMetric::LostCommittedReports: return r.lost;
        default:                               return 0;
    }
}

// Writes a metrics file the way BrokerNode does (D55 capacity, every kRuneMetrics row under the node key, label
// "<prefix> node=<id>").  storage=false allocates only Role and Term; withRole=false leaves the Role metric out.
void writeMetricsFile(const std::string& path, const Row& r, bool withRole = true) {
    const std::int64_t required = metrics::requiredSlots({kNodeMetricCount, 0, 0, 0, 0, 16});
    const auto maxSlots = static_cast<std::int32_t>(required);
    metrics::MetricsFile file;
    ASSERT_EQ(metrics::MetricsFile::create(metrics::CreateParams{path.c_str(), maxSlots, required,
                                                                 static_cast<std::int64_t>(::getpid()), 1, {}},
                                           file),
              metrics::FileStatus::Ok);
    std::vector<std::int32_t> freeStack(static_cast<std::size_t>(maxSlots), 0);
    metrics::MetricsWriter writer;
    ASSERT_EQ(metrics::MetricsWriter::create(file, freeStack, writer), metrics::WriterStatus::Ok);

    std::uint8_t keyBytes[kRoleKeyLength];
    encodeRoleKey(keyBytes, r.nodeId);
    const std::span<const std::byte> key{reinterpret_cast<const std::byte*>(keyBytes), kRoleKeyLength};
    const std::string suffix = " node=" + std::to_string(r.nodeId);
    for (const auto& d : kRuneMetrics) {
        if (d.id == RuneMetric::Role && !withRole) continue;
        if (!r.storage && d.id != RuneMetric::Role && d.id != RuneMetric::Term) continue;
        const metrics::AllocResult a =
            writer.allocate(typeIdOf(d.id), d.kind, -1, key, std::string{d.labelPrefix} + suffix);
        ASSERT_EQ(a.status, metrics::AllocStatus::Ok) << d.labelPrefix;
        a.writer.set(valueFor(r, d.id));
    }
}

struct RunResult {
    int exitCode = -1;
    std::string out;
    std::string err;
};

std::string drain(int fd) {
    std::string s;
    char buf[4096];
    for (;;) {
        const ssize_t n = ::read(fd, buf, sizeof buf);
        if (n > 0) {
            s.append(buf, static_cast<std::size_t>(n));
        } else if (n == 0 || errno != EINTR) {
            break;
        }
    }
    return s;
}

// Runs `argv` with exactly `env` (no inherited RUNE_* variables), stdout and stderr captured separately (stderr goes
// to a file so the two pipes can never deadlock each other).
RunResult runProcess(const std::vector<std::string>& args, const std::vector<std::string>& env,
                     const std::string& errPath) {
    int outPipe[2];
    if (::pipe(outPipe) != 0) return {};
    const pid_t pid = ::fork();
    if (pid == 0) {
        const int errFd = ::open(errPath.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0600);
        ::dup2(outPipe[1], STDOUT_FILENO);
        if (errFd >= 0) ::dup2(errFd, STDERR_FILENO);
        ::close(outPipe[0]);
        ::close(outPipe[1]);
        std::vector<char*> av;
        for (const std::string& a : args) av.push_back(const_cast<char*>(a.c_str()));
        av.push_back(nullptr);
        std::vector<char*> ev;
        for (const std::string& e : env) ev.push_back(const_cast<char*>(e.c_str()));
        ev.push_back(nullptr);
        ::execve(av[0], av.data(), ev.data());
        ::_exit(127);
    }
    ::close(outPipe[1]);
    RunResult r;
    r.out = drain(outPipe[0]);
    ::close(outPipe[0]);
    int status = 0;
    while (::waitpid(pid, &status, 0) < 0 && errno == EINTR) {}
    r.exitCode = WIFEXITED(status) ? WEXITSTATUS(status) : 128 + WTERMSIG(status);
    std::ifstream in(errPath);
    std::stringstream ss;
    ss << in.rdbuf();
    r.err = ss.str();
    return r;
}

RunResult runRole(TempDir& tmp, const std::vector<std::string>& extraArgs, const std::vector<std::string>& env) {
    std::vector<std::string> args{RUNE_ROLE_BIN};
    args.insert(args.end(), extraArgs.begin(), extraArgs.end());
    static int seq = 0;
    return runProcess(args, env, tmp.file("stderr." + std::to_string(seq++)));
}

TEST(RuneRoleRegression, GoldenOutputTable) {
    TempDir tmp;
    for (std::size_t i = 0; i < sizeof(kRows) / sizeof(kRows[0]); ++i) {
        SCOPED_TRACE("row " + std::to_string(i));
        const std::string path = tmp.file("golden" + std::to_string(i) + ".dat");
        writeMetricsFile(path, kRows[i]);
        const RunResult r = runRole(tmp, {path}, {});
        EXPECT_EQ(r.exitCode, 0) << r.err;
        EXPECT_EQ(r.out, kGolden[i]);
        EXPECT_EQ(r.err, "");
    }
}

TEST(RuneRoleRegression, ExitCodesTable) {
    TempDir tmp;
    const Row full{7, 2, 9, true, 1, 12, 1, 3, 2};
    const std::string fullOut = kGolden[13];

    const std::string fullPath = tmp.file("full.dat");
    writeMetricsFile(fullPath, full);

    const std::string zeroPath = tmp.file("zero.dat");   // version word 0 (a creator that never published)
    {
        std::ofstream z(zeroPath, std::ios::binary);
        const std::string zeros(64 * 1024, '\0');
        z.write(zeros.data(), static_cast<std::streamsize>(zeros.size()));
    }
    const std::string noRolePath = tmp.file("norole.dat");
    writeMetricsFile(noRolePath, full, /*withRole=*/false);

    // ${RUNE_DATA_DIR}/metrics/rune-metrics.dat
    const std::string dataDir = tmp.dir("data");
    tmp.dir("data/metrics");
    const std::string dataDirFile = tmp.file("data/metrics/rune-metrics.dat");
    writeMetricsFile(dataDirFile, full);

    const std::string missing = tmp.path() + "/does-not-exist.dat";

    struct Case {
        const char* name;
        std::vector<std::string> args;
        std::vector<std::string> env;
        int exitCode;
        std::string out;
        const char* errHas;   // substring of stderr ("" = stderr must be empty)
    };
    const std::vector<Case> cases = {
        {"no file (argv)", {missing}, {}, 1, "", "IoError"},
        {"no file (RUNE_METRICS_FILE)", {}, {"RUNE_METRICS_FILE=" + missing}, 1, "", "IoError"},
        {"no file (RUNE_DATA_DIR)", {}, {"RUNE_DATA_DIR=" + tmp.path() + "/nodata"}, 1, "", "nodata/metrics/rune-metrics.dat"},
        {"version word 0", {zeroPath}, {}, 1, "", "NotReady"},
        {"no role metric", {noRolePath}, {}, 2, "", "not present"},
        {"full file (argv)", {fullPath}, {}, 0, fullOut, ""},
        {"full file (RUNE_METRICS_FILE)", {}, {"RUNE_METRICS_FILE=" + fullPath}, 0, fullOut, ""},
        {"full file (RUNE_DATA_DIR)", {}, {"RUNE_DATA_DIR=" + dataDir}, 0, fullOut, ""},
        {"argv beats RUNE_METRICS_FILE", {fullPath}, {"RUNE_METRICS_FILE=" + missing}, 0, fullOut, ""},
        {"RUNE_METRICS_FILE beats RUNE_DATA_DIR", {}, {"RUNE_METRICS_FILE=" + fullPath, "RUNE_DATA_DIR=" + tmp.path() + "/nodata"}, 0, fullOut, ""},
        {"empty RUNE_METRICS_FILE falls back", {}, {"RUNE_METRICS_FILE=", "RUNE_DATA_DIR=" + dataDir}, 0, fullOut, ""},
    };
    for (const Case& c : cases) {
        SCOPED_TRACE(c.name);
        const RunResult r = runRole(tmp, c.args, c.env);
        EXPECT_EQ(r.exitCode, c.exitCode) << r.err;
        EXPECT_EQ(r.out, c.out);
        if (c.errHas[0] == '\0') {
            EXPECT_EQ(r.err, "");
        } else {
            EXPECT_NE(r.err.find(c.errHas), std::string::npos) << r.err;
        }
    }
}

// The dr-e2e harness's own parsers, on a file of rune_role output: one canonical line,
// "<role> <nodeId> <term> | <parse_rune_role_storage dict, sorted keys>" (role fields "None" when unmatched).
const char* const kParserScript =
    "import sys\n"
    "sys.path.insert(0, sys.argv[1])\n"
    "from harness.cluster import parse_rune_role_storage, _ROLE_RE\n"
    "out = open(sys.argv[2]).read()\n"
    "m = _ROLE_RE.search(out)\n"
    "role = (m.group(2), int(m.group(1)), int(m.group(3))) if m else None\n"
    "d = parse_rune_role_storage(out)\n"
    "print(repr(role), '|', repr(sorted(d.items())) if d is not None else 'None')\n";

std::string parseWithHarness(TempDir& tmp, const std::string& text, const std::string& tag) {
    const std::string in = tmp.file(tag + ".out");
    {
        std::ofstream f(in, std::ios::binary);
        f << text;
    }
    const RunResult r = runProcess({"/usr/bin/env", "python3", "-I", "-c", kParserScript, RUNE_DR_E2E_DIR, in},
                                   {"PATH=/usr/local/bin:/usr/bin:/bin"}, tmp.file(tag + ".err"));
    EXPECT_EQ(r.exitCode, 0) << r.err;
    return r.out;
}

std::string expectedParse(const Row& r, const std::string& role, const std::string& state) {
    const bool s = r.storage;
    std::ostringstream o;
    o << "('" << role << "', " << r.nodeId << ", " << r.term << ") | [('held_leader_roles', " << (s ? r.held : -1)
      << "), ('lost_committed_reports', " << (s ? r.lost : -1) << "), ('node_id', " << r.nodeId
      << "), ('out_of_sync', " << (s ? r.oos : -1) << "), ('pending_nodes', " << (s ? r.pending : -1)
      << "), ('state', '" << state << "')]\n";
    return o.str();
}

TEST(RuneRoleRegression, ParsersStillAgreeRegression) {
    TempDir tmp;
    const char* const roleNames[] = {"FOLLOWER", "CANDIDATE", "LEADER"};
    const char* const stateNames[] = {"CONFIRMED", "UNCONFIRMED", "UNDECIDED"};
    for (std::size_t i = 0; i < sizeof(kRows) / sizeof(kRows[0]); ++i) {
        SCOPED_TRACE("row " + std::to_string(i));
        const Row& row = kRows[i];
        const std::string path = tmp.file("parse" + std::to_string(i) + ".dat");
        writeMetricsFile(path, row);
        const RunResult r = runRole(tmp, {path}, {});
        ASSERT_EQ(r.exitCode, 0) << r.err;

        const std::string fromNew = parseWithHarness(tmp, r.out, "new" + std::to_string(i));
        const std::string fromGolden = parseWithHarness(tmp, kGolden[i], "golden" + std::to_string(i));
        EXPECT_EQ(fromNew, fromGolden);

        const std::string role = (row.role >= 0 && row.role <= 2) ? roleNames[row.role] : "NONE";
        const std::string state = (row.storage && row.gate >= 0 && row.gate <= 2) ? stateNames[row.gate] : "UNKNOWN";
        EXPECT_EQ(fromNew, expectedParse(row, role, state));
    }
}

}  // namespace
