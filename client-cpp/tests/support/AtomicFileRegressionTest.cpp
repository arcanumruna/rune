// AtomicFileRegressionTest.cpp — issue 44: a file another process polls (RUNE_EXPECT_FILE and friends) is published
// with tmp + rename, so a reader sees the previous file, none, or the complete new one — never a prefix.
// A forked reader opens + reads the file in a loop while the parent replaces it N times.  No sleeps: the reader
// runs until the parent closes the "done" pipe, with a hard iteration cap (it fails rather than hangs).
#include "../../examples/AtomicFile.h"

#include <gtest/gtest.h>

#include <poll.h>
#include <sys/wait.h>
#include <unistd.h>

#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <string>

namespace {

constexpr std::size_t   kPayloadBytes = 4u << 20;   // large enough that an in-place write is never one syscall
constexpr int           kWrites       = 150;
constexpr std::uint64_t kReaderCap    = 20'000'000;

enum ReaderExit : int { kOk = 0, kPartial = 1, kCapHit = 2, kNeverSawFile = 3 };

std::string payloadFor(int generation) {
    return std::string(kPayloadBytes, static_cast<char>('A' + generation % 26));
}

/// Child: read `path` until `doneFd` hits EOF.  Every successful open must read exactly one complete payload
/// (right size, one repeated byte).  Returns the exit code.
int readerLoop(const std::string& path, int doneFd) {
    std::uint64_t present = 0;
    for (std::uint64_t i = 0; i < kReaderCap; ++i) {
        pollfd pf{doneFd, POLLIN, 0};
        if (::poll(&pf, 1, 0) > 0 && (pf.revents & (POLLHUP | POLLIN)) != 0) {
            return present > 0 ? kOk : kNeverSawFile;
        }
        std::ifstream f(path, std::ios::binary);
        if (!f) continue;   // absent: allowed
        const std::string got((std::istreambuf_iterator<char>(f)), std::istreambuf_iterator<char>());
        ++present;
        if (got.size() != kPayloadBytes) return kPartial;
        for (const char ch : got)
            if (ch != got[0]) return kPartial;
    }
    return kCapHit;
}

} // namespace

TEST(AtomicFileRegression, AReaderNeverSeesAPartialFileRegression) {
    const std::filesystem::path dir =
        std::filesystem::temp_directory_path() / ("rune_atomic_file_" + std::to_string(::getpid()));
    std::filesystem::create_directories(dir);
    const std::string path = (dir / "expect.txt").string();

    // Seed generation 0 so the reader has something to read from its first pass.
    ASSERT_TRUE(rune::examples::writeFileAtomically(path, payloadFor(0)));

    int done[2];
    ASSERT_EQ(::pipe(done), 0);
    const pid_t pid = ::fork();
    ASSERT_GE(pid, 0);
    if (pid == 0) {
        ::close(done[1]);
        ::_exit(readerLoop(path, done[0]));
    }
    ::close(done[0]);

    for (int g = 1; g <= kWrites; ++g)
        ASSERT_TRUE(rune::examples::writeFileAtomically(path, payloadFor(g))) << "write " << g;
    ::close(done[1]);   // EOF: tell the reader to finish

    int st = 0;
    ASSERT_EQ(::waitpid(pid, &st, 0), pid);
    ASSERT_TRUE(WIFEXITED(st));
    EXPECT_EQ(WEXITSTATUS(st), kOk) << "1 = the reader saw a partial/short file, 2 = cap hit, 3 = never saw the file";

    // The final file is the last payload and no .tmp is left behind.
    std::ifstream f(path, std::ios::binary);
    const std::string last((std::istreambuf_iterator<char>(f)), std::istreambuf_iterator<char>());
    EXPECT_EQ(last, payloadFor(kWrites));
    EXPECT_FALSE(std::filesystem::exists(path + ".tmp"));
    std::filesystem::remove_all(dir);
}

TEST(AtomicFileRegression, AFailedWriteLeavesThePreviousFileRegression) {
    const std::filesystem::path dir =
        std::filesystem::temp_directory_path() / ("rune_atomic_file_fail_" + std::to_string(::getpid()));
    std::filesystem::create_directories(dir);
    const std::string path = (dir / "expect.txt").string();
    ASSERT_TRUE(rune::examples::writeFileAtomically(path, "old\n"));
    // The tmp name is taken by a directory: the write must fail and leave `path` untouched.
    std::filesystem::create_directory(path + ".tmp");
    EXPECT_FALSE(rune::examples::writeFileAtomically(path, "new\n"));
    std::ifstream f(path);
    std::string line;
    std::getline(f, line);
    EXPECT_EQ(line, "old");
    std::filesystem::remove_all(dir);
}
