// FreePortRegressionTest.cpp — 2026-10-15 (regression-speed): the test UDP port allocator is collision-proof
// across concurrently running test processes, which is what lets the real-BrokerNode suites drop their
// `RESOURCE_LOCK rune_udp_ports` and run in parallel.  Before: a pid-scattered walk + bind probe — two
// processes could both see a port free before either bound it ("Address already in use" under ctest -j).
#include <rune/aeronutil/testing/FreePort.h>

#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <set>
#include <vector>

#include <poll.h>
#include <sys/wait.h>
#include <unistd.h>

namespace {

namespace fp = rune::aeronutil::testing;

constexpr int kChildren = 8;
constexpr int kSingles  = 40;
constexpr int kRanges   = 6;
constexpr int kWidth    = 4;

/// Read exactly n bytes from fd within the deadline (poll); false on EOF / timeout.
bool readAll(int fd, void* buf, std::size_t n, std::chrono::steady_clock::time_point deadline) {
    auto* p = static_cast<char*>(buf);
    std::size_t got = 0;
    while (got < n) {
        const auto left = std::chrono::duration_cast<std::chrono::milliseconds>(deadline - std::chrono::steady_clock::now());
        if (left.count() <= 0) return false;
        pollfd pf{fd, POLLIN, 0};
        if (::poll(&pf, 1, static_cast<int>(left.count())) <= 0) return false;
        const ssize_t r = ::read(fd, p + got, n - got);
        if (r <= 0) return false;
        got += static_cast<std::size_t>(r);
    }
    return true;
}

/// Fork `kChildren` processes that allocate AT THE SAME TIME (they start together on a pipe barrier and stay
/// alive until the parent read every one, so no block is released mid-test); returns every child's ports.
/// Each child answers on its own pipe and every read is bounded: a child that dies (or aborts in the
/// allocator) is a failure of this test, never a hang (2026-10-15: one shared answer pipe hung the gate
/// when a child aborted — the other children held its write end open).
std::vector<std::vector<uint16_t>> allocateConcurrently() {
    int go[2];
    if (::pipe(go) != 0) return {};
    int rel[2];   // release: the parent closes its write end once it read every child
    if (::pipe(rel) != 0) return {};
    std::vector<pid_t> kids;
    std::vector<int>   answers;
    for (int k = 0; k < kChildren; ++k) {
        int out[2];
        if (::pipe(out) != 0) break;
        const pid_t pid = ::fork();
        if (pid == 0) {
            ::close(go[1]);
            ::close(out[0]);
            for (int a : answers) ::close(a);
            char c;
            (void)::read(go[0], &c, 1);   // barrier: every child allocates concurrently
            std::vector<uint16_t> mine;
            for (int i = 0; i < kSingles; ++i) mine.push_back(fp::allocFreeUdpPort());
            for (int i = 0; i < kRanges; ++i) {
                const uint16_t b = fp::allocFreeUdpPortRange(kWidth);
                for (int w = 0; w < kWidth; ++w) mine.push_back(static_cast<uint16_t>(b + w));
            }
            uint32_t n = static_cast<uint32_t>(mine.size());
            (void)::write(out[1], &n, sizeof n);
            (void)::write(out[1], mine.data(), n * sizeof(uint16_t));
            // Hold the blocks until the parent read everyone: EOF on the RELEASE pipe (nothing is ever written to
            // it), bounded.  Holding on the barrier pipe was wrong: poll(POLLIN) returned at once while a slower
            // sibling's barrier byte was still unread, so a fast child exited - releasing its blocks - while the
            // others were still claiming, and they took the same ports (seen on Linux).
            ::close(rel[1]);
            pollfd pf{rel[0], POLLIN, 0};
            if (::poll(&pf, 1, 30'000) > 0) { char eof; (void)::read(rel[0], &eof, 1); }
            ::_exit(0);
        }
        ::close(out[1]);
        kids.push_back(pid);
        answers.push_back(out[0]);
    }
    ::close(go[0]);
    const char start[kChildren] = {};
    (void)::write(go[1], start, kChildren);
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(20);
    std::vector<std::vector<uint16_t>> all;
    for (int fd : answers) {
        uint32_t n = 0;
        if (!readAll(fd, &n, sizeof n, deadline) || n > 4096) break;
        std::vector<uint16_t> v(n);
        if (!readAll(fd, v.data(), n * sizeof(uint16_t), deadline)) break;
        all.push_back(std::move(v));
    }
    ::close(go[1]);
    ::close(rel[1]);  // release the children (EOF)
    ::close(rel[0]);
    for (int fd : answers) ::close(fd);
    for (pid_t p : kids) { int st = 0; ::waitpid(p, &st, 0); }
    return all;
}

} // namespace

TEST(FreePortRegression, ConcurrentProcessesAreNeverHandedTheSamePort) {
    for (int round = 0; round < 3; ++round) {
        const auto all = allocateConcurrently();
        ASSERT_EQ(all.size(), static_cast<std::size_t>(kChildren)) << "round " << round;
        std::set<uint16_t> seen;
        for (std::size_t k = 0; k < all.size(); ++k) {
            ASSERT_EQ(all[k].size(), static_cast<std::size_t>(kSingles + kRanges * kWidth));
            for (uint16_t p : all[k]) {
                EXPECT_GE(p, fp::kPortRangeBase);
                EXPECT_LT(p, fp::kPortRangeBase + fp::kPortRangeSpan);
                EXPECT_TRUE(seen.insert(p).second) << "round " << round << ": port " << p
                                                   << " handed to two processes (or twice to one)";
            }
        }
    }
}

TEST(FreePortRegression, ARangeIsConsecutiveAndDisjointFromEverySinglePort) {
    std::set<uint16_t> seen;
    // 30 x 9 ports stays inside one owned window (kMaxOwnedBlocks * kBlockPorts), so no port may repeat.
    static_assert(30 * 9 <= fp::kMaxOwnedBlocks * fp::kBlockPorts);
    for (int i = 0; i < 30; ++i) {
        const uint16_t b = fp::allocFreeUdpPortRange(8);
        for (uint16_t w = 0; w < 8; ++w) EXPECT_TRUE(seen.insert(static_cast<uint16_t>(b + w)).second);
        EXPECT_TRUE(seen.insert(fp::allocFreeUdpPort()).second);
    }
}

TEST(FreePortRegression, ALongRunningProcessNeverExhaustsTheRangeOthersNeed) {
    // 3000 allocations (more than this process may own) never abort and never repeat within the owned
    // window: a process rewinds only its OWN blocks.
    std::vector<uint16_t> v;
    for (int i = 0; i < 3000; ++i) v.push_back(fp::allocFreeUdpPort());
    constexpr std::size_t kWindow = fp::kMaxOwnedBlocks * fp::kBlockPorts / 2;
    for (std::size_t i = 0; i + kWindow <= v.size(); i += kWindow) {
        std::set<uint16_t> w(v.begin() + static_cast<std::ptrdiff_t>(i),
                             v.begin() + static_cast<std::ptrdiff_t>(i + kWindow));
        EXPECT_EQ(w.size(), kWindow) << "a port repeated within " << kWindow << " allocations";
    }
    // A child process still gets ports none of ours (the blocks this process owns stay owned).
    const std::set<uint16_t> mine(v.begin(), v.end());
    int out[2];
    ASSERT_EQ(::pipe(out), 0);
    const pid_t pid = ::fork();
    if (pid == 0) {
        ::close(out[0]);
        uint16_t p[20];
        for (auto& x : p) x = fp::allocFreeUdpPort();
        (void)::write(out[1], p, sizeof p);
        ::_exit(0);
    }
    ::close(out[1]);
    uint16_t p[20] = {};
    ASSERT_EQ(::read(out[0], p, sizeof p), static_cast<ssize_t>(sizeof p));
    ::close(out[0]);
    int st = 0;
    ::waitpid(pid, &st, 0);
    for (uint16_t x : p) EXPECT_EQ(mine.count(x), 0u) << "port " << x << " is in a block this process owns";
}
